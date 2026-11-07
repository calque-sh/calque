//! The direct-columnar build: one exact point-in-time artifact encoded straight
//! from PostgreSQL binary COPY.
//!
//! The cursor is bound only once the [`CanonicalCutProof`] exists, because it
//! is the proof's position; until then the artifact has no bytes on disk.

use std::{
    collections::BTreeMap,
    fs::{File, OpenOptions},
    io::{BufReader, BufWriter, Write},
    path::{Path, PathBuf},
    time::Duration,
};

use artifact_contract::{
    ArtifactChunks, ArtifactCompatibility, ArtifactManifest, ArtifactMemory,
    ArtifactRepresentation, ArtifactTable, DecodedArtifact, DecodedArtifactFormat,
    DirectColumnarIdentity, DirectColumnarSchema, ManifestError, PointInTimeMetadata,
    ValidatedArtifactManifest, DIRECT_COLUMNAR_MEDIA_TYPE,
};
use chrono::DateTime;
use direct_columnar::{
    browser_key_index_bytes, read_header_summary, schema_hash, ArtifactIdentity, EncodeError,
    EncodeReport, Encoder, EncoderLimits, SchemaContract, WorkspaceSchema, FORMAT_VERSION,
};
use pgoutput_decoder::DecoderLimits;
use postgres_types::PgLsn;
use snapshot_coordinator::{
    CanonicalCutProof, CutEmission, ExportedSnapshot, SnapshotCoordinator, SnapshotCopyCompletion,
};
use source_control::{ActivatedSource, SchemaRegistry};
use sync_types::{ArtifactId, ObjectId, OpaqueCursor, SourceEpoch, TableId};
use thiserror::Error;
use tokio_util::sync::CancellationToken;

use crate::{
    build_state::{
        copy_progress, drive_copy_workers, revalidate_registry_after_cut, validate_copy_assignment,
        CopyEvent,
    },
    direct_overlay::DirectOverlay,
    roll_forward::roll_forward_overlay_to_cut_controlled,
    BoundedCopyLimits, BoundedCopyProgress, CopyLoadStats, DirectColumnarCopyError,
    DirectColumnarTableBinding, SnapshotBuildPlan, SnapshotBuildStateError, SnapshotCopyWorkload,
    SnapshotRollForwardStats, COLLATION_GENERATION_V1, NO_INDEX_PROFILE_HASH,
    PUBLICATION_GENERATION_V1, SOURCE_WIDE_SHARD_PROFILE_HASH, TYPE_CODEC_GENERATION_V1,
};

const PROTOCOL_VERSION: u32 = 1;
const IO_BUFFER_BYTES: usize = 1024 * 1024;

/// The direct-columnar workspace schema for a plan. Collections are named
/// after the PostgreSQL table, not the plan's hashed local table, because the
/// collection name is the application-facing name the generated bindings and
/// the change contract both use.
pub fn direct_columnar_workspace_schema(
    plan: &SnapshotBuildPlan,
) -> Result<WorkspaceSchema, DirectColumnarCopyError> {
    let tables = plan
        .tables()
        .iter()
        .map(|table| {
            crate::direct_columnar_table_schema(
                table,
                &DirectColumnarTableBinding {
                    collection: table.source_table().to_owned(),
                    dictionary_columns: Default::default(),
                },
            )
        })
        .collect::<Result<Vec<_>, _>>()?;
    Ok(WorkspaceSchema { tables })
}

/// The header identity the encoder writes and the browser reproduces from its
/// bootstrap plan and generated bindings. Every field derives from the plan:
/// `manifestVersion` is the schema generation and `authorizationScopeVersion`
/// the authorization generation, both as decimal text.
pub fn direct_columnar_artifact_identity(
    plan: &SnapshotBuildPlan,
    source_epoch: &SourceEpoch,
    schema_hash: &str,
    cursor: &str,
) -> ArtifactIdentity {
    let authorization = plan.authorization_identity();
    ArtifactIdentity {
        source_epoch: source_epoch.as_str().to_owned(),
        manifest_version: plan.schema_generation().to_string(),
        collection_set_hash: plan.table_set_hash().to_owned(),
        schema_hash: schema_hash.to_owned(),
        authorization_scope_version: authorization.generation().to_string(),
        authorization_scope_hash: authorization.scope_hash().to_string(),
        cursor: cursor.to_owned(),
    }
}

/// The manifest's copy of the physical schema: the document `schema_hash`
/// digests, in its key order, read back through the contract type so the
/// manifest can only say what the encoder was given.
pub(crate) fn direct_columnar_manifest_schema(
    schema: &WorkspaceSchema,
) -> Result<DirectColumnarSchema, serde_json::Error> {
    serde_json::from_value(serde_json::to_value(SchemaContract::new(schema))?)
}

/// What the caller decides about a direct artifact before it is built. Row
/// counts, the cut and the bytes are facts of the build and come from it.
#[derive(Debug, Clone)]
pub struct DirectArtifactRequest {
    pub artifact_id: ArtifactId,
    pub created_at: String,
    pub compatibility: ArtifactCompatibility,
}

/// Builder-owned state of one direct-columnar build, from the export to the
/// cut proof. The encoder and the overlay are never exposed. A failure at any
/// step taints the session, and a tainted session cannot produce an artifact.
pub struct DirectBuildSession {
    path: PathBuf,
    /// The artifact's path, reserved empty at creation and written only once
    /// the cursor is bound.
    file: File,
    plan: SnapshotBuildPlan,
    snapshot: ExportedSnapshot,
    activated_source: ActivatedSource,
    encoder: Encoder,
    identity: ArtifactIdentity,
    schema: WorkspaceSchema,
    max_header_bytes: u64,
    overlay: DirectOverlay,
    cut: Option<SnapshotRollForwardStats>,
    loaded: BTreeMap<TableId, CopyLoadStats>,
    tainted: bool,
}

impl DirectBuildSession {
    pub fn create(
        path: impl AsRef<Path>,
        plan: SnapshotBuildPlan,
        snapshot: &ExportedSnapshot,
        activated_source: &ActivatedSource,
        limits: EncoderLimits,
    ) -> Result<Self, SnapshotBuildStateError> {
        if plan.registry_binding() != snapshot.registry_binding()
            || plan.registry_binding() != activated_source.registry_binding()
        {
            return Err(SnapshotBuildStateError::RegistryBindingMismatch);
        }
        let schema = direct_columnar_workspace_schema(&plan)?;
        let schema_hash = schema_hash(&schema)?;
        let identity = direct_columnar_artifact_identity(
            &plan,
            activated_source.source_epoch(),
            &schema_hash,
            "",
        );
        let encoder = Encoder::new_awaiting_cursor(identity.clone(), schema.clone(), limits)?;
        let overlay = DirectOverlay::new(
            &plan.tables,
            activated_source.source_epoch().clone(),
            snapshot.consistent_point,
        )?;
        let file = OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(path.as_ref())
            .map_err(|error| {
                if error.kind() == std::io::ErrorKind::AlreadyExists {
                    SnapshotBuildStateError::BuildPathAlreadyExists
                } else {
                    SnapshotBuildStateError::Io(error)
                }
            })?;
        Ok(Self {
            path: path.as_ref().to_path_buf(),
            file,
            plan,
            snapshot: snapshot.clone(),
            activated_source: activated_source.clone(),
            encoder,
            identity,
            schema,
            max_header_bytes: limits.max_header_bytes,
            overlay,
            cut: None,
            loaded: BTreeMap::new(),
            tainted: false,
        })
    }

    /// Rolls the temporary stream forward from the exported snapshot to the
    /// cut marker into the overlay, then revalidates the registry. Must run
    /// before any row is copied: COPY output is final once it is encoded.
    #[allow(clippy::too_many_arguments)]
    pub async fn roll_forward_to_cut_controlled(
        &mut self,
        coordinator: &mut SnapshotCoordinator,
        expected: &CutEmission,
        timeout: Duration,
        decoder_limits: DecoderLimits,
        registry: &SchemaRegistry,
        registry_revalidation: postgres_connect::Endpoint,
        cancellation: &CancellationToken,
    ) -> Result<SnapshotRollForwardStats, SnapshotBuildStateError> {
        if self.tainted {
            return Err(SnapshotBuildStateError::BuildTainted);
        }
        if self.cut.is_some() || !self.loaded.is_empty() {
            self.tainted = true;
            return Err(SnapshotBuildStateError::CutAlreadyRolledForward);
        }
        if registry.binding() != *self.plan.registry_binding()
            || registry.binding() != *self.snapshot.registry_binding()
        {
            return Err(SnapshotBuildStateError::RegistryBindingMismatch);
        }
        let result = async {
            let stats = roll_forward_overlay_to_cut_controlled(
                coordinator,
                expected,
                timeout,
                decoder_limits,
                registry,
                &self.plan,
                &mut self.overlay,
                self.activated_source.source_epoch(),
                cancellation,
            )
            .await?;
            revalidate_registry_after_cut(registry, registry_revalidation, timeout, cancellation)
                .await?;
            Ok::<_, SnapshotBuildStateError>(stats)
        }
        .await;
        match result {
            Ok(stats) => {
                self.cut = Some(stats.clone());
                Ok(stats)
            }
            Err(error) => {
                self.tainted = true;
                Err(error)
            }
        }
    }

    /// Copies every planned table from a bounded set of imported workers
    /// through the overlay into the encoder. Producers decode at most one
    /// unacknowledged batch each; this task merges and encodes.
    pub async fn load_copy_workers_bounded_controlled(
        &mut self,
        workloads: Vec<SnapshotCopyWorkload<'_>>,
        limits: BoundedCopyLimits,
        cleanup_timeout: Duration,
        cancellation: &CancellationToken,
        mut on_progress: impl FnMut(BoundedCopyProgress),
    ) -> Result<Vec<SnapshotCopyCompletion>, SnapshotBuildStateError> {
        let admitted = if self.tainted {
            Err(SnapshotBuildStateError::BuildTainted)
        } else if self.cut.is_none() {
            Err(SnapshotBuildStateError::CutNotRolledForward)
        } else if cleanup_timeout.is_zero() {
            Err(SnapshotBuildStateError::InvalidParallelCopyLimits)
        } else {
            validate_copy_assignment(&self.plan, &workloads, limits, self.loaded.is_empty())
        };
        if let Err(error) = admitted {
            self.tainted = true;
            return Err(error);
        }
        let workloads = workloads
            .into_iter()
            .map(|workload| {
                let tables = workload
                    .table_indices
                    .iter()
                    .map(|index| (*index, self.plan.tables[*index].clone()))
                    .collect::<Vec<_>>();
                (workload.worker, tables)
            })
            .collect();
        let mut in_progress = BTreeMap::<TableId, CopyLoadStats>::new();
        let result = drive_copy_workers(
            workloads,
            limits.table,
            cleanup_timeout,
            cancellation,
            |event, active_workers| {
                self.apply_copy_event(event, &mut in_progress, active_workers, &mut on_progress)
            },
        )
        .await;
        let completions = match result {
            Ok(completions) => completions,
            Err(error) => {
                self.tainted = true;
                return Err(error);
            }
        };
        if self.loaded.len() != self.plan.tables.len() || !in_progress.is_empty() {
            self.tainted = true;
            return Err(SnapshotBuildStateError::ParallelCopyIncomplete);
        }
        Ok(completions)
    }

    fn apply_copy_event(
        &mut self,
        event: CopyEvent,
        in_progress: &mut BTreeMap<TableId, CopyLoadStats>,
        active_workers: usize,
        on_progress: &mut impl FnMut(BoundedCopyProgress),
    ) -> Result<(), SnapshotBuildStateError> {
        let (table_index, stats, acknowledgement, finished) = match event {
            CopyEvent::Batch {
                table_index,
                rows,
                stats,
                acknowledgement,
            } => {
                let table = self
                    .plan
                    .tables
                    .get(table_index)
                    .ok_or(SnapshotBuildStateError::UnknownTableIndex(table_index))?;
                for row in rows {
                    if let Some(row) = self.overlay.merge_copy_row(table_index, row)? {
                        self.encoder.push_row(&table.source_table, &row)?;
                    }
                }
                (table_index, stats, acknowledgement, false)
            }
            CopyEvent::Finished {
                table_index,
                stats,
                acknowledgement,
            } => {
                let table = self
                    .plan
                    .tables
                    .get(table_index)
                    .ok_or(SnapshotBuildStateError::UnknownTableIndex(table_index))?;
                for row in self.overlay.finish_table(table_index)? {
                    self.encoder.push_row(&table.source_table, &row)?;
                }
                (table_index, stats, acknowledgement, true)
            }
        };
        let table_id = self.plan.tables[table_index].table_id.clone();
        if finished {
            in_progress.remove(&table_id);
            self.loaded.insert(table_id.clone(), stats);
        } else {
            in_progress.insert(table_id.clone(), stats);
        }
        acknowledgement
            .send(())
            .map_err(|_| SnapshotBuildStateError::ParallelCopyChannelClosed)?;
        on_progress(copy_progress(
            self.plan.tables.len(),
            &self.loaded,
            in_progress,
            table_id,
            stats,
            active_workers,
        )?);
        Ok(())
    }

    /// The path the artifact is written to, in the build's private work
    /// directory.
    pub fn artifact_path(&self) -> &Path {
        &self.path
    }

    /// Binds the cursor the proof names, writes the artifact and checks what
    /// was written. `tee` is told the artifact's length before its first byte
    /// and is handed every byte written to the file, in order. Blocking; run
    /// it off the async runtime.
    pub fn finish_at_cut(
        self,
        proof: &CanonicalCutProof,
        request: DirectArtifactRequest,
        tee: &mut impl ArtifactTee,
    ) -> Result<DirectArtifactAtCut, DirectFinalizationError> {
        if self.tainted {
            return Err(DirectFinalizationError::BuildTainted);
        }
        let cut = self
            .cut
            .as_ref()
            .ok_or(DirectFinalizationError::CutNotRolledForward)?;
        if self.loaded.len() != self.plan.tables.len() {
            return Err(DirectFinalizationError::TablesNotLoaded);
        }
        let observation = &cut.cut;
        if self.activated_source.source_epoch() != proof.source_epoch()
            || observation.identity != *proof.identity()
            || observation.transaction_id != proof.transaction_id()
            || observation.message_lsn != proof.message_lsn()
            || observation.commit_lsn != proof.commit_lsn()
            || observation.end_lsn != proof.end_lsn()
        {
            return Err(DirectFinalizationError::CutProofMismatch);
        }
        let copy_consistent_lsn = self.snapshot.consistent_point;
        if copy_consistent_lsn > proof.message_lsn()
            || proof.message_lsn() > proof.commit_lsn()
            || proof.commit_lsn() > proof.end_lsn()
        {
            return Err(DirectFinalizationError::InvalidCutOrdering);
        }
        validate_request(&self.plan, &request)?;

        let mut tables = Vec::with_capacity(self.plan.tables.len());
        let mut table_rows = Vec::with_capacity(self.plan.tables.len());
        for table in &self.plan.tables {
            let rows = self
                .encoder
                .table_rows(&table.source_table)
                .ok_or(DirectFinalizationError::TablesNotLoaded)?;
            tables.push(table.artifact_table(rows));
            table_rows.push((table.source_table.clone(), rows));
        }
        let snapshot_cursor = self
            .activated_source
            .cursor_codec()
            .encode(proof.position());
        let mut identity = self.identity;
        snapshot_cursor.as_str().clone_into(&mut identity.cursor);
        let mut encoder = self.encoder;
        encoder.bind_cursor(snapshot_cursor.as_str())?;
        if self.file.metadata()?.len() != 0 {
            return Err(DirectFinalizationError::Verification(
                "build file is not empty",
            ));
        }
        let artifact = encoder.lay_out()?;
        tee.begin(artifact.bytes())?;
        let mut output = BufWriter::with_capacity(
            IO_BUFFER_BYTES,
            TeeToFile {
                file: self.file,
                tee,
            },
        );
        let report = artifact.write(&mut output)?;
        output
            .into_inner()
            .map_err(|error| DirectFinalizationError::Io(error.into_error()))?
            .file
            .sync_all()?;
        verify_written_artifact(
            &self.path,
            &identity,
            &table_rows,
            &report,
            self.max_header_bytes,
        )?;

        let schema = direct_columnar_manifest_schema(&self.schema)
            .map_err(DirectFinalizationError::ManifestEncode)?;
        Ok(DirectArtifactAtCut {
            path: self.path,
            request,
            snapshot_cursor,
            source_epoch: proof.source_epoch().clone(),
            copy_consistent_lsn,
            cut_message_lsn: proof.message_lsn(),
            cut_marker_id: proof.identity().build_id.clone(),
            report,
            format: WrittenFormat {
                schema_hash: identity.schema_hash.clone(),
                collection_set_hash: identity.collection_set_hash.clone(),
                identity: DirectColumnarIdentity {
                    manifest_version: identity.manifest_version.clone(),
                    authorization_scope_version: identity.authorization_scope_version.clone(),
                },
                schema,
            },
            tables,
        })
    }
}

/// Receives a copy of an artifact as [`DirectBuildSession::finish_at_cut`]
/// writes it: `begin` once with the artifact's length, before its first byte,
/// then every byte in order through `Write`. It is never flushed: the caller
/// that owns it finishes it once the artifact is written.
pub trait ArtifactTee: Write {
    fn begin(&mut self, artifact_bytes: u64) -> std::io::Result<()>;
}

/// The artifact's file, then its tee.
struct TeeToFile<'a, T> {
    file: File,
    tee: &'a mut T,
}

impl<T: Write> Write for TeeToFile<'_, T> {
    fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
        self.file.write_all(bytes)?;
        self.tee.write_all(bytes)?;
        Ok(bytes.len())
    }

    // Only the file: a tee that compresses would end a block here, which
    // changes its bytes.
    fn flush(&mut self) -> std::io::Result<()> {
        self.file.flush()
    }
}

/// Checks the written file against what the encoder reported and what the
/// build meant to write: its length, and a header that names this build's
/// identity, the proof's cursor and each table's row count. The file's payload
/// digest is checked when it is uploaded: a blob store hashes the bytes it
/// stores against the decoded SHA-256 before the object becomes visible.
pub(crate) fn verify_written_artifact(
    path: &Path,
    identity: &ArtifactIdentity,
    table_rows: &[(String, u64)],
    report: &EncodeReport,
    max_header_bytes: u64,
) -> Result<(), DirectFinalizationError> {
    let file = File::open(path)?;
    if file.metadata()?.len() != report.bytes {
        return Err(DirectFinalizationError::Verification(
            "artifact length differs from the encoder's report",
        ));
    }
    let header =
        read_header_summary(BufReader::new(file), max_header_bytes).map_err(
            |error| match error {
                EncodeError::Io(_) | EncodeError::Json(_) | EncodeError::MalformedHeader(_) => {
                    DirectFinalizationError::Verification("artifact header cannot be read back")
                }
                error => DirectFinalizationError::Encode(error),
            },
        )?;
    if header.identity != *identity {
        return Err(DirectFinalizationError::Verification(
            "artifact header names another identity or cursor",
        ));
    }
    if header.table_rows != table_rows {
        return Err(DirectFinalizationError::Verification(
            "artifact header row counts differ from the build",
        ));
    }
    Ok(())
}

fn validate_request(
    plan: &SnapshotBuildPlan,
    request: &DirectArtifactRequest,
) -> Result<(), DirectFinalizationError> {
    DateTime::parse_from_rfc3339(&request.created_at)
        .map_err(|_| DirectFinalizationError::InvalidCreatedAt)?;
    let compatibility = &request.compatibility;
    let authorization = plan.authorization_identity();
    if compatibility.schema_generation != plan.schema_generation()
        || compatibility.table_set_hash != plan.table_set_hash()
        || compatibility.authorization_generation != authorization.generation()
        || compatibility.authorization_policy_hash != authorization.policy_hash().to_string()
        || compatibility.authorization_scope_hash != authorization.scope_hash().to_string()
    {
        return Err(DirectFinalizationError::CompatibilityMismatch);
    }
    if compatibility.publication_generation != PUBLICATION_GENERATION_V1
        || compatibility.shard_profile_hash != SOURCE_WIDE_SHARD_PROFILE_HASH
        || compatibility.index_profile_hash != NO_INDEX_PROFILE_HASH
        || compatibility.type_codec_generation != TYPE_CODEC_GENERATION_V1
        || compatibility.collation_generation != COLLATION_GENERATION_V1
    {
        return Err(DirectFinalizationError::UnsupportedCompatibilityProfile);
    }
    Ok(())
}

/// A written, header-verified direct-columnar artifact at one proven cut.
#[derive(Debug, Clone)]
pub struct DirectArtifactAtCut {
    path: PathBuf,
    request: DirectArtifactRequest,
    snapshot_cursor: OpaqueCursor,
    source_epoch: SourceEpoch,
    copy_consistent_lsn: PgLsn,
    cut_message_lsn: PgLsn,
    cut_marker_id: String,
    report: EncodeReport,
    format: WrittenFormat,
    tables: Vec<ArtifactTable>,
}

/// The direct-columnar format facts known once the artifact is written. The
/// chunk digests join them when the representations have been verified.
#[derive(Debug, Clone)]
struct WrittenFormat {
    schema_hash: String,
    collection_set_hash: String,
    identity: DirectColumnarIdentity,
    schema: DirectColumnarSchema,
}

impl WrittenFormat {
    fn with_chunks(&self, chunks: ArtifactChunks) -> DecodedArtifactFormat {
        DecodedArtifactFormat::DirectColumnar {
            schema_hash: self.schema_hash.clone(),
            collection_set_hash: self.collection_set_hash.clone(),
            format_version: FORMAT_VERSION,
            identity: self.identity.clone(),
            schema: self.schema.clone(),
            chunks,
        }
    }
}

impl DirectArtifactAtCut {
    pub fn path(&self) -> &Path {
        &self.path
    }

    pub fn decoded_sha256(&self) -> &str {
        &self.report.sha256
    }

    pub fn decoded_bytes(&self) -> u64 {
        self.report.bytes
    }

    /// The immutable manifest for these bytes, with the chunk digests the
    /// representations' verification read. The memory estimate is the
    /// measured bytes, the primary-key indexes the browser builds beside them,
    /// and the growth reserve.
    pub fn validated_manifest(
        &self,
        decoded_object_id: ObjectId,
        representations: Vec<ArtifactRepresentation>,
        chunks: ArtifactChunks,
        growth_reserve_bytes: u64,
    ) -> Result<ValidatedArtifactManifest, DirectFinalizationError> {
        let manifest = ArtifactManifest {
            protocol_version: PROTOCOL_VERSION,
            artifact_id: self.request.artifact_id.clone(),
            created_at: self.request.created_at.clone(),
            decoded: DecodedArtifact {
                object_id: decoded_object_id,
                sha256: self.report.sha256.clone(),
                bytes: self.report.bytes,
                media_type: DIRECT_COLUMNAR_MEDIA_TYPE.to_owned(),
                format: self.format.with_chunks(chunks),
            },
            pit: PointInTimeMetadata {
                source_epoch: self.source_epoch.clone(),
                snapshot_cursor: self.snapshot_cursor.clone(),
                copy_consistent_lsn: self.copy_consistent_lsn.to_string(),
                cut_message_lsn: self.cut_message_lsn.to_string(),
                cut_marker_id: self.cut_marker_id.clone(),
            },
            compatibility: self.request.compatibility.clone(),
            tables: self.tables.clone(),
            representations,
            memory: ArtifactMemory {
                estimated_peak_bytes: self
                    .report
                    .bytes
                    .checked_add(browser_key_index_bytes(
                        self.tables.iter().map(|table| table.row_count),
                    ))
                    .and_then(|bytes| bytes.checked_add(growth_reserve_bytes))
                    .ok_or(DirectFinalizationError::MemoryOverflow)?,
                recommended_growth_reserve_bytes: growth_reserve_bytes,
            },
            signature: None,
        };
        let bytes =
            serde_json::to_vec(&manifest).map_err(DirectFinalizationError::ManifestEncode)?;
        ValidatedArtifactManifest::parse(&bytes).map_err(DirectFinalizationError::Manifest)
    }
}

#[derive(Debug, Error)]
pub enum DirectFinalizationError {
    #[error("direct build is tainted by a prior failure")]
    BuildTainted,
    #[error("direct build has not rolled forward to its cut")]
    CutNotRolledForward,
    #[error("direct build has not copied every planned table")]
    TablesNotLoaded,
    #[error("cut proof does not match the cut this build rolled forward to")]
    CutProofMismatch,
    #[error("cut proof requires copy_consistent <= message <= commit <= end")]
    InvalidCutOrdering,
    #[error("artifact created_at is not RFC 3339")]
    InvalidCreatedAt,
    #[error("artifact compatibility does not describe this build's plan")]
    CompatibilityMismatch,
    #[error("artifact compatibility names an unsupported profile")]
    UnsupportedCompatibilityProfile,
    #[error("written direct-columnar artifact is refused: {0}")]
    Verification(&'static str),
    #[error("artifact memory estimate overflowed")]
    MemoryOverflow,
    #[error(transparent)]
    Encode(#[from] EncodeError),
    #[error("direct-columnar manifest encoding failed: {0}")]
    ManifestEncode(#[source] serde_json::Error),
    #[error(transparent)]
    Manifest(ManifestError),
    #[error(transparent)]
    Io(#[from] std::io::Error),
}

#[cfg(test)]
mod tests {
    use direct_columnar::{ColumnKind, ColumnSchema, TableSchema, Value};

    use super::*;

    fn written(directory: &Path) -> (PathBuf, ArtifactIdentity, Vec<(String, u64)>, EncodeReport) {
        let schema = WorkspaceSchema {
            tables: vec![TableSchema {
                name: "tasks".to_owned(),
                primary_key: vec!["id".to_owned()],
                columns: vec![
                    ColumnSchema {
                        name: "id".to_owned(),
                        kind: ColumnKind::Int64,
                        nullable: false,
                    },
                    ColumnSchema {
                        name: "title".to_owned(),
                        kind: ColumnKind::Utf8,
                        nullable: false,
                    },
                ],
            }],
        };
        let identity = ArtifactIdentity {
            source_epoch: "epoch-1".to_owned(),
            manifest_version: "3".to_owned(),
            collection_set_hash: format!("sha256:{}", "1".repeat(64)),
            schema_hash: schema_hash(&schema).unwrap(),
            authorization_scope_version: "1".to_owned(),
            authorization_scope_hash: format!("sha256:{}", "2".repeat(64)),
            cursor: String::new(),
        };
        let mut encoder =
            Encoder::new_awaiting_cursor(identity.clone(), schema, EncoderLimits::default())
                .unwrap();
        for id in 0..64 {
            encoder
                .push_row(
                    "tasks",
                    &[Value::Int64(id), Value::Text(format!("task {id}"))],
                )
                .unwrap();
        }
        encoder.bind_cursor("cursor-at-the-cut").unwrap();
        let path = directory.join("artifact.calque");
        let report = encoder.finish(File::create(&path).unwrap()).unwrap();
        let identity = ArtifactIdentity {
            cursor: "cursor-at-the-cut".to_owned(),
            ..identity
        };
        (path, identity, vec![("tasks".to_owned(), 64)], report)
    }

    const MAX_HEADER: u64 = 1024 * 1024;

    #[test]
    fn the_written_artifact_is_checked_against_the_build() {
        let directory = tempfile::tempdir().unwrap();
        let (path, identity, rows, report) = written(directory.path());
        verify_written_artifact(&path, &identity, &rows, &report, MAX_HEADER).unwrap();

        let other_cursor = ArtifactIdentity {
            cursor: "another-cursor".to_owned(),
            ..identity.clone()
        };
        assert!(matches!(
            verify_written_artifact(&path, &other_cursor, &rows, &report, MAX_HEADER),
            Err(DirectFinalizationError::Verification(
                "artifact header names another identity or cursor"
            ))
        ));
        assert!(matches!(
            verify_written_artifact(
                &path,
                &identity,
                &[("tasks".to_owned(), 63)],
                &report,
                MAX_HEADER
            ),
            Err(DirectFinalizationError::Verification(
                "artifact header row counts differ from the build"
            ))
        ));
    }

    #[test]
    fn a_short_artifact_is_refused() {
        let directory = tempfile::tempdir().unwrap();
        let (path, identity, rows, report) = written(directory.path());
        let bytes = std::fs::read(&path).unwrap();

        std::fs::write(&path, &bytes[..bytes.len() - 1]).unwrap();
        assert!(matches!(
            verify_written_artifact(&path, &identity, &rows, &report, MAX_HEADER),
            Err(DirectFinalizationError::Verification(
                "artifact length differs from the encoder's report"
            ))
        ));

        // Short enough that the header itself is cut, and reported as such.
        std::fs::write(&path, &bytes[..20]).unwrap();
        let short = EncodeReport {
            bytes: 20,
            ..report
        };
        assert!(matches!(
            verify_written_artifact(&path, &identity, &rows, &short, MAX_HEADER),
            Err(DirectFinalizationError::Verification(
                "artifact header cannot be read back"
            ))
        ));
    }

    #[test]
    fn a_corrupt_header_is_refused() {
        let directory = tempfile::tempdir().unwrap();
        let (path, identity, rows, report) = written(directory.path());
        let mut bytes = std::fs::read(&path).unwrap();
        bytes[0] ^= 0xff;
        std::fs::write(&path, &bytes).unwrap();
        assert!(matches!(
            verify_written_artifact(&path, &identity, &rows, &report, MAX_HEADER),
            Err(DirectFinalizationError::Verification(
                "artifact header cannot be read back"
            ))
        ));

        bytes[0] ^= 0xff;
        let cursor = bytes
            .windows(b"cursor-at-the-cut".len())
            .position(|window| window == b"cursor-at-the-cut")
            .unwrap();
        bytes[cursor] = b'k';
        std::fs::write(&path, &bytes).unwrap();
        assert!(matches!(
            verify_written_artifact(&path, &identity, &rows, &report, MAX_HEADER),
            Err(DirectFinalizationError::Verification(
                "artifact header names another identity or cursor"
            ))
        ));
    }
}
