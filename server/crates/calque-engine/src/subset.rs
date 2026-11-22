//! Artifacts of some of the served tables.
//!
//! A page whose bindings name fewer tables than its server serves is planned
//! the whole artifact at first. [`copy_subset`] then writes the artifact of
//! only its tables at the same cut, by copying their parts out of the decoded
//! object already published there, and publishes it. [`encode_subset`] gives
//! it the re-encodes every publication gets, so the next such page downloads
//! only its tables.
//! A subset is an artifact of its cut like any other, and is retired with it.

use std::{
    collections::BTreeSet,
    fs::File,
    io::{BufReader, BufWriter},
    time::Instant,
};

use artifact_catalog::{CatalogError, PublishedArtifactQuery, SqliteArtifactCatalog};
use artifact_contract::{
    DecodedArtifact, DecodedArtifactFormat, RepresentationEncoding, ValidatedArtifactManifest,
};
use direct_columnar::{
    browser_key_index_bytes, read_header_summary, schema_hash, write_subset, EncoderLimits,
};
use snapshot_builder::{
    direct_columnar_artifact_identity, direct_columnar_manifest_schema,
    direct_columnar_workspace_schema, SnapshotBuildPlan,
};
use snapshot_pipeline::PreparedRepresentations;
use sync_types::{ArtifactId, ObjectId, TableId};
use uuid::Uuid;

use crate::{
    reencode::{
        abandon, cancelled_or, download, now_ms, published_at, reencode_leaving_replaced,
        representation, upload_and_publish, ReencodeError, ReencodeOutcome, ReencodeOutcomes,
        ReencodeRequest, ReencodeStage, WorkDirectory, REENCODE_LEASE_MS,
    },
    state::snapshot_workspace_root,
};

/// The most table sets a source-wide scope publishes subsets of.
pub(crate) const MAX_SUBSETS: usize = 8;

/// What one [`derive_subset`] did.
#[cfg(test)]
#[derive(Debug)]
pub(crate) struct SubsetOutcomes {
    /// `NothingToReencode` when `tables` is the whole set or nothing is
    /// published at the cut to derive from.
    pub(crate) subset: ReencodeOutcome,
    /// The subset's own re-encodes, run once it is published.
    pub(crate) encodes: Option<ReencodeOutcomes>,
}

/// [`copy_subset`], then [`encode_subset`] once the copy is published.
#[cfg(test)]
pub(crate) async fn derive_subset(
    request: ReencodeRequest<'_>,
    plan: &SnapshotBuildPlan,
    tables: &BTreeSet<TableId>,
    admit: impl Fn() -> bool,
    mut on_stage: impl FnMut(ReencodeStage),
) -> Result<(SubsetOutcomes, Vec<ArtifactId>), ReencodeError> {
    let subset = copy_subset(request, plan, tables, &admit, &mut on_stage).await?;
    if !matches!(
        subset,
        ReencodeOutcome::Published { .. } | ReencodeOutcome::AlreadyPublished
    ) {
        return Ok((
            SubsetOutcomes {
                subset,
                encodes: None,
            },
            Vec::new(),
        ));
    }
    let (encodes, replaced) = encode_subset(request, plan, tables, admit, on_stage).await?;
    Ok((
        SubsetOutcomes {
            subset,
            encodes: Some(encodes),
        },
        replaced,
    ))
}

/// Publishes the artifact of only `tables` at `request.cut`, copied from the
/// artifact `request.query` publishes there for every table of `plan`, with
/// the representations a new cut is published with. It is admitted as a
/// re-encode is: only if `admit` holds when the catalog asks, under the fence
/// lock.
pub(crate) async fn copy_subset(
    request: ReencodeRequest<'_>,
    plan: &SnapshotBuildPlan,
    tables: &BTreeSet<TableId>,
    admit: impl Fn() -> bool,
    mut on_stage: impl FnMut(ReencodeStage),
) -> Result<ReencodeOutcome, ReencodeError> {
    let narrowed = plan.narrowed_to(tables)?;
    if narrowed.table_set_hash() == plan.table_set_hash() {
        return Ok(ReencodeOutcome::NothingToReencode);
    }
    let catalog = SqliteArtifactCatalog::open(request.catalog_path)?;
    let copied =
        !published_at(&catalog, &subset_query(&request, &narrowed), request.cut)?.is_empty();
    drop(catalog);
    if copied {
        return Ok(ReencodeOutcome::AlreadyPublished);
    }
    publish_subset(request, plan, &narrowed, &admit, &mut on_stage).await
}

/// Re-encodes the artifact of only `tables` published at `request.cut` as a
/// publication is, and returns the artifacts it replaces, for the caller to
/// retire.
pub(crate) async fn encode_subset(
    request: ReencodeRequest<'_>,
    plan: &SnapshotBuildPlan,
    tables: &BTreeSet<TableId>,
    admit: impl Fn() -> bool,
    on_stage: impl FnMut(ReencodeStage),
) -> Result<(ReencodeOutcomes, Vec<ArtifactId>), ReencodeError> {
    let query = subset_query(&request, &plan.narrowed_to(tables)?);
    reencode_leaving_replaced(
        ReencodeRequest {
            query: &query,
            ..request
        },
        admit,
        on_stage,
    )
    .await
}

fn subset_query(
    request: &ReencodeRequest<'_>,
    narrowed: &SnapshotBuildPlan,
) -> PublishedArtifactQuery {
    PublishedArtifactQuery {
        cache_key: request.query.cache_key.clone(),
        table_set_hash: narrowed.table_set_hash().to_owned(),
    }
}

async fn publish_subset(
    request: ReencodeRequest<'_>,
    plan: &SnapshotBuildPlan,
    narrowed: &SnapshotBuildPlan,
    admit: &impl Fn() -> bool,
    on_stage: &mut impl FnMut(ReencodeStage),
) -> Result<ReencodeOutcome, ReencodeError> {
    let ReencodeRequest {
        state_root,
        catalog_path,
        store,
        query,
        cut,
        fence,
        cancellation,
    } = request;
    let mut catalog = SqliteArtifactCatalog::open(catalog_path)?;
    let published = published_at(&catalog, query, cut)?;
    // Its manifest is rewritten, which only the key of a signed one could do.
    let Some(parent) = published
        .iter()
        .find(|manifest| manifest.manifest().signature.is_none())
    else {
        return Ok(ReencodeOutcome::NothingToReencode);
    };
    let source = parent.manifest();
    let identity_object = representation(parent, RepresentationEncoding::Identity)?;

    let work = WorkDirectory::create(
        &snapshot_workspace_root(state_root).join(format!("subset-{}", Uuid::new_v4().simple())),
    )?;
    let parent_path = work.path.join("parent");
    download(
        store,
        &identity_object.object_id,
        &parent_path,
        cancellation,
    )
    .await?;
    on_stage(ReencodeStage::Subsetting);
    let started = Instant::now();
    let limits = EncoderLimits {
        max_artifact_bytes: source.decoded.bytes,
        ..EncoderLimits::default()
    };
    // The parent was written for `plan` at this cut, so the subset's header
    // is the one an encoder given the narrowed plan writes.
    let header = read_header_summary(
        BufReader::new(File::open(&parent_path)?),
        limits.max_header_bytes,
    )?;
    let whole_schema = direct_columnar_workspace_schema(plan)?;
    if header.identity
        != direct_columnar_artifact_identity(
            plan,
            &source.pit.source_epoch,
            &schema_hash(&whole_schema)?,
            source.pit.snapshot_cursor.as_str(),
        )
    {
        return Err(ReencodeError::ParentIdentity);
    }
    let schema = direct_columnar_workspace_schema(narrowed)?;
    let identity = direct_columnar_artifact_identity(
        narrowed,
        &source.pit.source_epoch,
        &schema_hash(&schema)?,
        source.pit.snapshot_cursor.as_str(),
    );
    let names: Vec<&str> = narrowed
        .tables()
        .iter()
        .map(|table| table.source_table())
        .collect();
    let decoded_path = work.path.join("decoded");
    let report = {
        let mut output = BufWriter::new(File::create(&decoded_path)?);
        let report = write_subset(
            BufReader::new(File::open(&parent_path)?),
            &names,
            &identity,
            limits,
            &mut output,
        )?;
        output.into_inner().map_err(|error| error.into_error())?;
        report
    };
    std::fs::remove_file(&parent_path)?;
    let prepared = PreparedRepresentations::prepare_controlled(
        &decoded_path,
        report.bytes,
        &report.sha256,
        report.bytes.saturating_add(1024 * 1024),
        cancellation,
    )
    .map_err(|error| cancelled_or(cancellation, error.into()))?;
    let encode_ms = started.elapsed().as_millis();

    let mut manifest = source.clone();
    manifest.artifact_id = ArtifactId::new(format!(
        "subset-{}-{}",
        short_hash(narrowed.table_set_hash()),
        Uuid::new_v4().simple()
    ))?;
    let DecodedArtifactFormat::DirectColumnar {
        format_version,
        identity: decoded_identity,
        ..
    } = &source.decoded.format;
    manifest.decoded = DecodedArtifact {
        object_id: ObjectId::new(format!("sha256:{}", report.sha256))?,
        sha256: report.sha256.clone(),
        bytes: report.bytes,
        media_type: source.decoded.media_type.clone(),
        format: DecodedArtifactFormat::DirectColumnar {
            schema_hash: identity.schema_hash.clone(),
            collection_set_hash: identity.collection_set_hash.clone(),
            format_version: *format_version,
            identity: decoded_identity.clone(),
            schema: direct_columnar_manifest_schema(&schema)?,
            chunks: prepared.decoded_chunks().clone(),
        },
    };
    narrowed
        .table_set_hash()
        .clone_into(&mut manifest.compatibility.table_set_hash);
    manifest.tables.retain(|table| {
        narrowed
            .tables()
            .iter()
            .any(|kept| *kept.table_id() == table.table_id)
    });
    manifest.representations = prepared.manifests();
    manifest.memory.estimated_peak_bytes = report
        .bytes
        .saturating_add(browser_key_index_bytes(
            manifest.tables.iter().map(|table| table.row_count),
        ))
        .saturating_add(manifest.memory.recommended_growth_reserve_bytes);
    manifest.signature = None;
    let manifest = ValidatedArtifactManifest::parse(&serde_json::to_vec(&manifest)?)?;
    if cancellation.is_cancelled() {
        return Err(ReencodeError::Cancelled);
    }

    let owner = format!("subset-{}", Uuid::new_v4().simple());
    let lease = catalog.register_build(&manifest, &owner, now_ms()?, REENCODE_LEASE_MS)?;
    let objects: Vec<_> = prepared
        .entries()
        .map(|entry| (entry.manifest(), entry.path()))
        .collect();
    let published = upload_and_publish(
        &mut catalog,
        store,
        &lease,
        &objects,
        fence,
        admit,
        on_stage,
        ReencodeStage::SubsetAdmitting,
    )
    .await;
    if let Err(error) = published {
        abandon(&mut catalog, store, &lease, &owner).await?;
        return match error {
            ReencodeError::Catalog(CatalogError::PublicationRefused(_)) => {
                Ok(ReencodeOutcome::Refused)
            }
            error => Err(cancelled_or(cancellation, error)),
        };
    }
    tracing::info!(
        target: "calque::preparation",
        artifact = %manifest.manifest().artifact_id,
        parent = %source.artifact_id,
        tables = manifest.manifest().tables.len(),
        parent_tables = source.tables.len(),
        decoded_bytes = report.bytes,
        parent_decoded_bytes = source.decoded.bytes,
        encode_ms = encode_ms as u64,
        "subset artifact published"
    );
    Ok(ReencodeOutcome::Published {
        artifact: manifest.manifest().artifact_id.clone(),
    })
}

/// The first 16 hex digits of a `sha256:` digest.
fn short_hash(digest: &str) -> &str {
    let hex = digest.strip_prefix("sha256:").unwrap_or(digest);
    &hex[..hex.len().min(16)]
}

#[cfg(test)]
mod tests {
    use std::cell::Cell;

    use artifact_catalog::ArtifactState;
    use artifact_contract::{
        ArtifactManifest, ArtifactMemory, PointInTimeMetadata, DIRECT_COLUMNAR_MEDIA_TYPE,
    };
    use blob_store::{BlobStore, LocalFilesystemBlobStore, ObjectMetadata};
    use direct_columnar::{ArtifactIdentity, Encoder, Value};
    use sha2::{Digest, Sha256};
    use sync_types::{CursorPosition, SourceEpoch};
    use tokio::{io::AsyncReadExt, sync::RwLock};
    use tokio_util::sync::CancellationToken;

    use super::*;
    use crate::{
        reencode::retire_replaced,
        snapshot::{published_query_for, tests::prepared_source_for},
        test_support::{column, inspection, registry_for},
        PreparedLocalSource,
    };

    const TASKS: usize = 300;
    const PROJECTS: usize = 40;

    struct Fixture {
        directory: tempfile::TempDir,
        store: LocalFilesystemBlobStore,
        prepared: PreparedLocalSource,
        plan: SnapshotBuildPlan,
        query: PublishedArtifactQuery,
        parent: ValidatedArtifactManifest,
    }

    impl Fixture {
        fn catalog_path(&self) -> std::path::PathBuf {
            self.directory.path().join("catalog.sqlite3")
        }

        fn catalog(&self) -> SqliteArtifactCatalog {
            SqliteArtifactCatalog::open(self.catalog_path()).unwrap()
        }

        fn table(&self, name: &str) -> TableId {
            self.plan
                .tables()
                .iter()
                .find(|table| table.source_table() == name)
                .unwrap()
                .table_id()
                .clone()
        }

        fn subset_query(&self, tables: &BTreeSet<TableId>) -> PublishedArtifactQuery {
            PublishedArtifactQuery {
                cache_key: self.query.cache_key.clone(),
                table_set_hash: self
                    .plan
                    .narrowed_to(tables)
                    .unwrap()
                    .table_set_hash()
                    .to_owned(),
            }
        }

        async fn derive(
            &self,
            tables: &BTreeSet<TableId>,
            admit: impl Fn() -> bool,
            on_stage: impl FnMut(ReencodeStage),
        ) -> Result<(SubsetOutcomes, Vec<ArtifactId>), ReencodeError> {
            let catalog_path = self.catalog_path();
            let fence = RwLock::new(());
            let cancellation = CancellationToken::new();
            derive_subset(
                ReencodeRequest {
                    state_root: self.directory.path(),
                    catalog_path: &catalog_path,
                    store: &self.store,
                    query: &self.query,
                    cut: &self.parent.manifest().pit.snapshot_cursor,
                    fence: &fence,
                    cancellation: &cancellation,
                },
                &self.plan,
                tables,
                admit,
                on_stage,
            )
            .await
        }

        async fn object(&self, object: &ObjectId) -> Vec<u8> {
            let mut bytes = Vec::new();
            self.store
                .get(object)
                .await
                .unwrap()
                .unwrap()
                .read_to_end(&mut bytes)
                .await
                .unwrap();
            bytes
        }
    }

    fn uuid(seed: usize) -> [u8; 16] {
        let mut value = [0_u8; 16];
        value[..8].copy_from_slice(
            &(seed as u64)
                .wrapping_mul(0x9e37_79b9_7f4a_7c15)
                .to_le_bytes(),
        );
        value[8..].copy_from_slice(&(seed as u64 + 1).to_le_bytes());
        value
    }

    /// The rows of one table of the fixture: a key and a title.
    fn rows(table: &str) -> Vec<Vec<Value>> {
        let (count, offset) = match table {
            "tasks" => (TASKS, 0),
            _ => (PROJECTS, 10_000),
        };
        (0..count)
            .map(|row| {
                vec![
                    Value::Uuid(uuid(offset + row)),
                    Value::Text(format!("{table} {row} of the fixture")),
                ]
            })
            .collect()
    }

    /// The artifact an encoder writes for `plan` at `cursor` under
    /// `identity`, with the fixture's rows of each of its tables.
    fn encoded(plan: &SnapshotBuildPlan, identity: ArtifactIdentity) -> Vec<u8> {
        let schema = direct_columnar_workspace_schema(plan).unwrap();
        let mut encoder = Encoder::new(identity, schema, EncoderLimits::default()).unwrap();
        for table in plan.tables() {
            for row in rows(table.source_table()) {
                encoder.push_row(table.source_table(), &row).unwrap();
            }
        }
        let mut bytes = Vec::new();
        encoder.finish(&mut bytes).unwrap();
        bytes
    }

    fn identity_for(
        plan: &SnapshotBuildPlan,
        epoch: &SourceEpoch,
        cursor: &str,
    ) -> ArtifactIdentity {
        let schema = direct_columnar_workspace_schema(plan).unwrap();
        direct_columnar_artifact_identity(plan, epoch, &schema_hash(&schema).unwrap(), cursor)
    }

    /// Two tables published at one cut as a publication publishes them, its
    /// header written under `identity` when given and else the plan's own.
    async fn published(identity: Option<ArtifactIdentity>) -> Fixture {
        let directory = tempfile::tempdir().unwrap();
        let epoch = SourceEpoch::new("subset-epoch").unwrap();
        let registry = registry_for(vec![
            inspection(
                "tasks",
                16_384,
                vec![
                    column(1, "id", 2_950, "uuid", false, Some(1)),
                    column(2, "title", 25, "text", false, None),
                ],
            ),
            inspection(
                "projects",
                16_385,
                vec![
                    column(1, "id", 2_950, "uuid", false, Some(1)),
                    column(2, "title", 25, "text", false, None),
                ],
            ),
        ]);
        let prepared = prepared_source_for(epoch.clone(), registry);
        let plan = SnapshotBuildPlan::from_registry(&prepared.registry, &prepared.scope).unwrap();
        let cursor = prepared.activated.cursor_codec().encode(&CursorPosition {
            source_epoch: epoch.clone(),
            sequence: 5,
        });
        let decoded = encoded(
            &plan,
            identity.unwrap_or_else(|| identity_for(&plan, &epoch, cursor.as_str())),
        );
        let decoded_path = directory.path().join("decoded");
        std::fs::write(&decoded_path, &decoded).unwrap();
        let digest = format!("{:x}", Sha256::digest(&decoded));
        let representations = PreparedRepresentations::prepare_controlled(
            &decoded_path,
            decoded.len() as u64,
            &digest,
            decoded.len() as u64 + 1024 * 1024,
            &CancellationToken::new(),
        )
        .unwrap();
        let query = published_query_for(&prepared).unwrap();
        let schema = direct_columnar_workspace_schema(&plan).unwrap();
        let authorization = plan.authorization_identity();
        let tables: Vec<_> = plan
            .tables()
            .iter()
            .map(|table| table.artifact_table(rows(table.source_table()).len() as u64))
            .collect();
        let manifest = ArtifactManifest {
            protocol_version: 1,
            artifact_id: ArtifactId::new("local-columnar-subset-parent").unwrap(),
            created_at: "2026-11-21T09:00:00Z".to_owned(),
            decoded: DecodedArtifact {
                object_id: ObjectId::new(format!("sha256:{digest}")).unwrap(),
                sha256: digest.clone(),
                bytes: decoded.len() as u64,
                media_type: DIRECT_COLUMNAR_MEDIA_TYPE.to_owned(),
                format: DecodedArtifactFormat::DirectColumnar {
                    schema_hash: schema_hash(&schema).unwrap(),
                    collection_set_hash: plan.table_set_hash().to_owned(),
                    format_version: direct_columnar::FORMAT_VERSION,
                    identity: artifact_contract::DirectColumnarIdentity {
                        manifest_version: plan.schema_generation().to_string(),
                        authorization_scope_version: authorization.generation().to_string(),
                    },
                    schema: direct_columnar_manifest_schema(&schema).unwrap(),
                    chunks: representations.decoded_chunks().clone(),
                },
            },
            pit: PointInTimeMetadata {
                source_epoch: epoch,
                snapshot_cursor: cursor,
                copy_consistent_lsn: "0/A".to_owned(),
                cut_message_lsn: "0/B".to_owned(),
                cut_marker_id: "build-1".to_owned(),
            },
            compatibility: artifact_contract::ArtifactCompatibility {
                publication_generation: snapshot_builder::PUBLICATION_GENERATION_V1,
                schema_generation: plan.schema_generation(),
                authorization_generation: authorization.generation(),
                authorization_policy_hash: authorization.policy_hash().to_string(),
                authorization_scope_hash: authorization.scope_hash().to_string(),
                table_set_hash: plan.table_set_hash().to_owned(),
                shard_profile_hash: snapshot_builder::SOURCE_WIDE_SHARD_PROFILE_HASH.to_owned(),
                index_profile_hash: snapshot_builder::NO_INDEX_PROFILE_HASH.to_owned(),
                type_codec_generation: snapshot_builder::TYPE_CODEC_GENERATION_V1,
                collation_generation: snapshot_builder::COLLATION_GENERATION_V1,
            },
            memory: ArtifactMemory {
                estimated_peak_bytes: decoded.len() as u64
                    + browser_key_index_bytes(tables.iter().map(|table| table.row_count))
                    + 4096,
                recommended_growth_reserve_bytes: 4096,
            },
            tables,
            representations: representations.manifests(),
            signature: None,
        };
        let parent =
            ValidatedArtifactManifest::parse(&serde_json::to_vec(&manifest).unwrap()).unwrap();
        assert_eq!(parent.cache_key(), query.cache_key);
        let store =
            LocalFilesystemBlobStore::open(directory.path().join("objects"), 64 * 1024 * 1024)
                .unwrap();
        let mut catalog =
            SqliteArtifactCatalog::open(directory.path().join("catalog.sqlite3")).unwrap();
        let lease = catalog
            .register_build(&parent, "builder", now_ms().unwrap(), 60_000)
            .unwrap();
        catalog.mark_uploading(&lease, now_ms().unwrap()).unwrap();
        for entry in representations.entries() {
            let representation = entry.manifest();
            let metadata = ObjectMetadata::new(
                representation.encoded_bytes,
                representation.encoded_sha256.clone(),
            )
            .unwrap();
            store
                .put_file_if_absent(&representation.object_id, entry.path(), &metadata)
                .await
                .unwrap();
        }
        catalog.mark_uploaded(&lease, now_ms().unwrap()).unwrap();
        catalog
            .verify_and_publish(&lease, &store, now_ms().unwrap())
            .await
            .unwrap();
        Fixture {
            directory,
            store,
            prepared,
            plan,
            query,
            parent,
        }
    }

    /// The subset of `projects` is the artifact an encoder writes for that
    /// table alone at the parent's cut, it is published with its re-encodes
    /// under the narrowed table set, the parent is untouched, and a second
    /// run finds the work done.
    #[tokio::test]
    async fn a_subset_is_the_artifact_of_its_tables_alone_at_the_parent_s_cut() {
        let fixture = published(None).await;
        let projects = BTreeSet::from([fixture.table("projects")]);
        let mut stages = Vec::new();
        let (outcomes, replaced) = fixture
            .derive(&projects, || true, |stage| stages.push(stage))
            .await
            .unwrap();
        let ReencodeOutcome::Published { artifact: derived } = &outcomes.subset else {
            panic!("{outcomes:?}");
        };
        let encodes = outcomes.encodes.as_ref().unwrap();
        assert!(matches!(encodes.zstd, ReencodeOutcome::Published { .. }));
        assert!(matches!(
            encodes.brotli,
            Some(Ok(ReencodeOutcome::Published { .. }))
        ));
        assert_eq!(
            stages[..2],
            [ReencodeStage::Subsetting, ReencodeStage::SubsetAdmitting]
        );
        assert_eq!(replaced.len(), 2, "{replaced:?}");
        assert!(replaced.contains(derived));
        retire_replaced(
            &fixture.catalog_path(),
            &replaced,
            &CancellationToken::new(),
        )
        .await
        .unwrap();

        let catalog = fixture.catalog();
        let query = fixture.subset_query(&projects);
        let [subset] = <[_; 1]>::try_from(catalog.published(&query).unwrap()).unwrap();
        let manifest = subset.manifest();
        assert_eq!(
            manifest
                .tables
                .iter()
                .map(|table| table.table_id.clone())
                .collect::<BTreeSet<_>>(),
            projects
        );
        assert_eq!(manifest.pit, fixture.parent.manifest().pit);
        assert_eq!(subset.cache_key(), fixture.query.cache_key);
        let narrowed = fixture.plan.narrowed_to(&projects).unwrap();
        let expected = encoded(
            &narrowed,
            identity_for(
                &narrowed,
                fixture.prepared.activated.source_epoch(),
                manifest.pit.snapshot_cursor.as_str(),
            ),
        );
        let identity = representation(&subset, RepresentationEncoding::Identity).unwrap();
        let bytes = fixture.object(&identity.object_id).await;
        assert!(bytes == expected, "the subset is the encoder's artifact");
        subset.verify_decoded_bytes(&bytes).unwrap();
        assert!(manifest.decoded.bytes < fixture.parent.manifest().decoded.bytes);
        assert!(
            manifest.memory.estimated_peak_bytes
                < fixture.parent.manifest().memory.estimated_peak_bytes
        );
        for artifact in &replaced {
            assert_eq!(
                catalog.state(artifact).unwrap(),
                Some(ArtifactState::Retired)
            );
        }
        assert_eq!(
            catalog.published(&fixture.query).unwrap(),
            std::slice::from_ref(&fixture.parent)
        );

        let (again, replaced) = fixture.derive(&projects, || true, |_| {}).await.unwrap();
        assert_eq!(again.subset, ReencodeOutcome::AlreadyPublished);
        let encodes = again.encodes.unwrap();
        assert_eq!(encodes.zstd, ReencodeOutcome::AlreadyPublished);
        assert!(matches!(
            encodes.brotli,
            Some(Ok(ReencodeOutcome::AlreadyPublished))
        ));
        assert!(replaced.is_empty());
    }

    /// A fence between the subset's upload and its publication leaves it
    /// unpublished, and the catalog asks under the fence lock.
    #[tokio::test]
    async fn a_fence_during_the_derivation_keeps_the_subset_unpublished() {
        let fixture = published(None).await;
        let projects = BTreeSet::from([fixture.table("projects")]);
        let catalog_path = fixture.catalog_path();
        let fence = RwLock::new(());
        let cancellation = CancellationToken::new();
        let served = Cell::new(true);
        let asked_under_fence = Cell::new(None);
        let (outcomes, replaced) = derive_subset(
            ReencodeRequest {
                state_root: fixture.directory.path(),
                catalog_path: &catalog_path,
                store: &fixture.store,
                query: &fixture.query,
                cut: &fixture.parent.manifest().pit.snapshot_cursor,
                fence: &fence,
                cancellation: &cancellation,
            },
            &fixture.plan,
            &projects,
            || {
                asked_under_fence.set(Some(fence.try_write().is_err()));
                served.get()
            },
            |stage| {
                if stage == ReencodeStage::SubsetAdmitting {
                    let _fence = fence.try_write().expect("nothing else holds the fence");
                    served.set(false);
                }
            },
        )
        .await
        .unwrap();
        assert_eq!(outcomes.subset, ReencodeOutcome::Refused);
        assert!(outcomes.encodes.is_none());
        assert!(replaced.is_empty());
        assert_eq!(asked_under_fence.get(), Some(true));
        let catalog = fixture.catalog();
        assert!(catalog
            .published(&fixture.subset_query(&projects))
            .unwrap()
            .is_empty());
        assert_eq!(
            catalog.published(&fixture.query).unwrap(),
            std::slice::from_ref(&fixture.parent)
        );
    }

    /// Nothing is derived for the whole set, nor from a parent whose header
    /// names another plan, and a table the plan does not hold is refused.
    #[tokio::test]
    async fn a_subset_is_derived_only_of_part_of_the_plan_s_tables() {
        let fixture = published(None).await;
        let every: BTreeSet<_> = fixture
            .plan
            .tables()
            .iter()
            .map(|table| table.table_id().clone())
            .collect();
        let (outcomes, _) = fixture.derive(&every, || true, |_| {}).await.unwrap();
        assert_eq!(outcomes.subset, ReencodeOutcome::NothingToReencode);
        let elsewhere = registry_for(vec![inspection(
            "comments",
            16_386,
            vec![column(1, "id", 2_950, "uuid", false, Some(1))],
        )])
        .tables()[0]
            .table_id
            .clone();
        assert!(matches!(
            fixture
                .derive(&BTreeSet::from([elsewhere]), || true, |_| {})
                .await,
            Err(ReencodeError::Plan(
                snapshot_builder::SnapshotPlanError::TableOutsidePlan
            ))
        ));

        let mut other = identity_for(
            &fixture.plan,
            fixture.prepared.activated.source_epoch(),
            fixture.parent.manifest().pit.snapshot_cursor.as_str(),
        );
        other.cursor = "another-cut".to_owned();
        let fixture = published(Some(other)).await;
        let projects = BTreeSet::from([fixture.table("projects")]);
        assert!(matches!(
            fixture.derive(&projects, || true, |_| {}).await,
            Err(ReencodeError::ParentIdentity)
        ));
        assert!(fixture
            .catalog()
            .published(&fixture.subset_query(&projects))
            .unwrap()
            .is_empty());
    }
}
