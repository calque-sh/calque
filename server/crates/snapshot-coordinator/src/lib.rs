//! Exported logical-snapshot and transactional cut-marker coordination.

mod pressure;

pub use pressure::{
    sample_snapshot_pressure, SnapshotPressureError, SnapshotPressureEvaluation,
    SnapshotPressureLevel, SnapshotPressurePolicy, SnapshotPressureReason, SnapshotPressureSample,
    SnapshotPressureTarget, SnapshotPressureThresholds, SnapshotRemainingWalThresholds,
    SnapshotWalStatus,
};

use canonical_log::{CanonicalEntry, CanonicalRecord};
use pg_walstream::{
    parse_lsn, CancellationToken, PgReplicationConnection, PgReplicationShutdownHandle,
    ReplicationSlotOptions, SlotType,
};
use pgoutput_decoder::{
    DecodeError, DecodedChange, DecodedTransaction, Decoder, DecoderEvent, DecoderLimits,
    LogicalMessageKind, ReplicationCopyData,
};
use serde::{Deserialize, Serialize};
use source_control::{
    is_valid_slot_name, quote_identifier, PgoutputLaunchProfile, PgoutputProfileError,
    SchemaRegistry, SchemaRegistryBinding,
};
use std::collections::{BTreeMap, BTreeSet};
use std::error::Error;
use std::time::{Duration, Instant};
use sync_types::{CursorPosition, SourceEpoch, TableId};
use thiserror::Error;
use tokio_postgres::error::SqlState;
use tokio_postgres::types::PgLsn;
use tokio_postgres::{Client, CopyOutStream, IsolationLevel, Transaction};
use uuid::Uuid;

/// Reserved transactional logical-message prefix used by snapshot builds.
pub const CUT_MARKER_PREFIX: &str = "sync.snapshot_cut.v1";
const MAX_CUT_ID_COMPONENT_BYTES: usize = 64;
const MAX_REPLICATION_CONNECT_TIMEOUT_SECS: u64 = 30;

pub type Result<T> = std::result::Result<T, CoordinatorError>;

#[derive(Debug, Error)]
pub enum RollForwardError<E: Error + 'static> {
    #[error(transparent)]
    Coordinator(#[from] CoordinatorError),
    #[error("pre-cut transaction apply failed: {0}")]
    Apply(#[source] E),
}

/// Startup configuration for one temporary snapshot stream.
///
/// Its profile is the same closed launch type carried by source enrollment and
/// persistent CDC startup. An invalid slot or publication name is refused
/// while constructing this value, before a replication connection or slot can
/// be consumed.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SnapshotStreamConfig {
    pub slot_name: String,
    pub publication_name: String,
    pgoutput_profile: PgoutputLaunchProfile,
}

impl SnapshotStreamConfig {
    pub fn new(slot_name: impl Into<String>, publication_name: impl Into<String>) -> Result<Self> {
        let config = Self {
            slot_name: slot_name.into(),
            publication_name: publication_name.into(),
            pgoutput_profile: PgoutputLaunchProfile::V1,
        };
        validate_slot_name(&config.slot_name)?;
        if config.publication_name.is_empty() {
            return Err(CoordinatorError::InvalidConfiguration(
                "publication name must not be empty".to_owned(),
            ));
        }
        Ok(config)
    }
}

/// Snapshot exported by a session-bound temporary logical slot.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ExportedSnapshot {
    pub slot_name: String,
    pub snapshot_name: String,
    pub consistent_point: PgLsn,
    quoted_snapshot_name: String,
    registry_binding: SchemaRegistryBinding,
    pgoutput_profile: PgoutputLaunchProfile,
}

impl ExportedSnapshot {
    pub fn registry_binding(&self) -> &SchemaRegistryBinding {
        &self.registry_binding
    }
}

/// The exact identity serialized into the dedicated marker transaction.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CutIdentity {
    pub build_id: String,
    pub nonce: String,
}

impl CutIdentity {
    /// Mints a random build id and nonce from OS-backed randomness.
    pub fn generate() -> Self {
        Self {
            build_id: Uuid::new_v4().to_string(),
            nonce: Uuid::new_v4().simple().to_string(),
        }
    }

    /// An identity from a given build id and nonce, each 1 to 64 bytes of ASCII
    /// letters, digits, `-` and `_`. A snapshot build pairs its own build id
    /// with a generated nonce.
    pub fn new(build_id: impl Into<String>, nonce: impl Into<String>) -> Result<Self> {
        let identity = Self {
            build_id: build_id.into(),
            nonce: nonce.into(),
        };
        if !valid_cut_component(&identity.build_id) || !valid_cut_component(&identity.nonce) {
            return Err(CoordinatorError::InvalidCutIdentity);
        }
        Ok(identity)
    }

    fn bytes(&self) -> Result<Vec<u8>> {
        serde_json::to_vec(self).map_err(CoordinatorError::MarkerJson)
    }
}

fn valid_cut_component(value: &str) -> bool {
    !value.is_empty()
        && value.len() <= MAX_CUT_ID_COMPONENT_BYTES
        && value
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_'))
}

/// LSN returned by `pg_logical_emit_message` before its transaction commits.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CutEmission {
    pub identity: CutIdentity,
    pub message_lsn: PgLsn,
}

/// Complete marker transaction observed on the temporary stream.
///
/// This is a barrier candidate, not an engine cursor. The persistent CDC path
/// must observe the same identity and marker LSN, durably append its transaction,
/// and only then assign the artifact's opaque snapshot cursor.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CutObservation {
    pub identity: CutIdentity,
    pub transaction_id: u32,
    pub message_lsn: PgLsn,
    pub commit_lsn: PgLsn,
    pub end_lsn: PgLsn,
    pub committed_application_transactions_before_cut: usize,
}

/// Durable proof that the temporary snapshot cut is the same transaction as a
/// retained barrier in the persistent canonical log.
///
/// `position` is server-trusted cursor `P`. It may be exposed to clients only
/// after encoding it with the source's opaque cursor codec.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CanonicalCutProof {
    position: CursorPosition,
    source_epoch: SourceEpoch,
    identity: CutIdentity,
    transaction_id: u32,
    message_lsn: PgLsn,
    commit_lsn: PgLsn,
    end_lsn: PgLsn,
}

impl CanonicalCutProof {
    pub fn position(&self) -> &CursorPosition {
        &self.position
    }

    pub fn source_epoch(&self) -> &SourceEpoch {
        &self.source_epoch
    }

    pub fn identity(&self) -> &CutIdentity {
        &self.identity
    }

    pub fn transaction_id(&self) -> u32 {
        self.transaction_id
    }

    pub fn message_lsn(&self) -> PgLsn {
        self.message_lsn
    }

    pub fn commit_lsn(&self) -> PgLsn {
        self.commit_lsn
    }

    pub fn end_lsn(&self) -> PgLsn {
        self.end_lsn
    }
}

/// Stateful join between a temporary-stream cut and persistent canonical-log
/// records. Unrelated records leave the mapper unresolved. Any record that
/// partially collides with the expected marker or PostgreSQL transaction fails
/// closed instead of being skipped.
#[derive(Debug)]
pub struct PersistentBarrierMapper {
    expected_source_epoch: SourceEpoch,
    observation: CutObservation,
    resolved: bool,
}

impl PersistentBarrierMapper {
    pub fn new(expected_source_epoch: SourceEpoch, observation: CutObservation) -> Self {
        Self {
            expected_source_epoch,
            observation,
            resolved: false,
        }
    }

    pub fn observe(&mut self, record: &CanonicalRecord) -> Result<Option<CanonicalCutProof>> {
        if self.resolved {
            return Err(CoordinatorError::PersistentBarrierAlreadyResolved);
        }
        if record.position.source_epoch != self.expected_source_epoch
            || record.entry.key().source_epoch != self.expected_source_epoch
        {
            return Err(CoordinatorError::PersistentBarrierEpochMismatch);
        }

        let expected_end_lsn = self.observation.end_lsn.to_string();
        let same_postgres_transaction = record.entry.key().postgres_xid
            == self.observation.transaction_id
            && record.entry.key().end_lsn == expected_end_lsn;
        let is_candidate = match &record.entry {
            CanonicalEntry::Transaction(_) => same_postgres_transaction,
            CanonicalEntry::Barrier(barrier) => {
                let build_id_matches = barrier.build_id == self.observation.identity.build_id;
                let nonce_matches = barrier.nonce == self.observation.identity.nonce;
                if build_id_matches != nonce_matches {
                    return Err(CoordinatorError::PersistentBarrierCollision);
                }
                same_postgres_transaction || (build_id_matches && nonce_matches)
            }
        };
        if !is_candidate {
            return Ok(None);
        }

        let proof = prove_persistent_cut(&self.expected_source_epoch, &self.observation, record)?;
        self.resolved = true;
        Ok(Some(proof))
    }
}

/// Verifies every field joining temporary and persistent observation of a cut.
pub fn prove_persistent_cut(
    expected_source_epoch: &SourceEpoch,
    observation: &CutObservation,
    record: &CanonicalRecord,
) -> Result<CanonicalCutProof> {
    if record.position.sequence == 0 {
        return Err(CoordinatorError::InvalidPersistentBarrierPosition);
    }
    if &record.position.source_epoch != expected_source_epoch
        || &record.entry.key().source_epoch != expected_source_epoch
    {
        return Err(CoordinatorError::PersistentBarrierEpochMismatch);
    }
    let CanonicalEntry::Barrier(barrier) = &record.entry else {
        return Err(CoordinatorError::PersistentCutIsNotBarrier);
    };

    if barrier.build_id != observation.identity.build_id {
        return Err(CoordinatorError::PersistentBarrierMismatch("build_id"));
    }
    if barrier.nonce != observation.identity.nonce {
        return Err(CoordinatorError::PersistentBarrierMismatch("nonce"));
    }
    if barrier.key.postgres_xid != observation.transaction_id {
        return Err(CoordinatorError::PersistentBarrierMismatch("postgres_xid"));
    }

    let message_lsn = parse_canonical_lsn("message_lsn", &barrier.message_lsn)?;
    let commit_lsn = parse_canonical_lsn("commit_lsn", &barrier.commit_lsn)?;
    let end_lsn = parse_canonical_lsn("end_lsn", &barrier.key.end_lsn)?;
    if message_lsn != observation.message_lsn {
        return Err(CoordinatorError::PersistentBarrierMismatch("message_lsn"));
    }
    if commit_lsn != observation.commit_lsn {
        return Err(CoordinatorError::PersistentBarrierMismatch("commit_lsn"));
    }
    if end_lsn != observation.end_lsn {
        return Err(CoordinatorError::PersistentBarrierMismatch("end_lsn"));
    }

    Ok(CanonicalCutProof {
        position: record.position.clone(),
        source_epoch: expected_source_epoch.clone(),
        identity: observation.identity.clone(),
        transaction_id: observation.transaction_id,
        message_lsn,
        commit_lsn,
        end_lsn,
    })
}

fn parse_canonical_lsn(field: &'static str, value: &str) -> Result<PgLsn> {
    value
        .parse::<PgLsn>()
        .map_err(|_| CoordinatorError::InvalidPersistentBarrierLsn {
            field,
            value: value.to_owned(),
        })
}

/// Tracks the all-workers-imported barrier for one exported snapshot.
#[derive(Debug)]
pub struct ImportBarrier {
    snapshot: ExportedSnapshot,
    expected_workers: BTreeMap<String, BTreeSet<TableId>>,
    imported_workers: BTreeSet<String>,
    completed_workers: BTreeSet<String>,
}

impl ImportBarrier {
    pub fn new<I, S, T>(snapshot: &ExportedSnapshot, assignments: I) -> Result<Self>
    where
        I: IntoIterator<Item = (S, T)>,
        S: Into<String>,
        T: IntoIterator<Item = TableId>,
    {
        let mut expected_workers = BTreeMap::new();
        let valid_tables = snapshot
            .registry_binding
            .tables()
            .iter()
            .map(|(table_id, _)| table_id.clone())
            .collect::<BTreeSet<_>>();
        let mut assigned_once = BTreeSet::new();
        for (worker, tables) in assignments {
            let worker = worker.into();
            let tables = tables.into_iter().collect::<BTreeSet<_>>();
            if worker.is_empty()
                || tables.is_empty()
                || tables.iter().any(|table| !valid_tables.contains(table))
                || tables
                    .iter()
                    .any(|table| !assigned_once.insert(table.clone()))
                || expected_workers.insert(worker, tables).is_some()
            {
                return Err(CoordinatorError::InvalidConfiguration(
                    "COPY assignments require unique non-empty worker IDs and unique enrolled tables"
                        .to_owned(),
                ));
            }
        }
        if expected_workers.is_empty() {
            return Err(CoordinatorError::InvalidConfiguration(
                "at least one COPY worker assignment is required".to_owned(),
            ));
        }
        Ok(Self {
            snapshot: snapshot.clone(),
            expected_workers,
            imported_workers: BTreeSet::new(),
            completed_workers: BTreeSet::new(),
        })
    }

    pub fn missing_workers(&self) -> Vec<&str> {
        self.expected_workers
            .keys()
            .filter(|worker| !self.completed_workers.contains(*worker))
            .map(String::as_str)
            .collect()
    }

    fn record_import(
        &mut self,
        worker_id: &str,
        snapshot: &ExportedSnapshot,
    ) -> Result<BTreeSet<TableId>> {
        if snapshot != &self.snapshot {
            return Err(CoordinatorError::SnapshotMismatch);
        }
        let assigned_tables = self
            .expected_workers
            .get(worker_id)
            .cloned()
            .ok_or_else(|| CoordinatorError::UnknownWorker(worker_id.to_owned()))?;
        if !self.imported_workers.insert(worker_id.to_owned()) {
            return Err(CoordinatorError::DuplicateWorker(worker_id.to_owned()));
        }
        Ok(assigned_tables)
    }

    /// Records that one imported snapshot transaction finished all of its COPY
    /// work and committed successfully.
    pub fn record_completion(&mut self, completion: SnapshotCopyCompletion) -> Result<()> {
        if completion.snapshot != self.snapshot {
            return Err(CoordinatorError::SnapshotMismatch);
        }
        let expected_tables = self
            .expected_workers
            .get(&completion.worker_id)
            .ok_or_else(|| CoordinatorError::UnknownWorker(completion.worker_id.clone()))?;
        if !self.imported_workers.contains(&completion.worker_id) {
            return Err(CoordinatorError::WorkerWasNotImported(completion.worker_id));
        }
        if &completion.copied_tables != expected_tables {
            return Err(CoordinatorError::WorkerCopySetMismatch(
                completion.worker_id,
            ));
        }
        if !self.completed_workers.insert(completion.worker_id.clone()) {
            return Err(CoordinatorError::DuplicateWorkerCompletion(
                completion.worker_id,
            ));
        }
        Ok(())
    }

    fn ensure_imported(&self) -> Result<()> {
        let missing = self
            .expected_workers
            .keys()
            .filter(|worker| !self.imported_workers.contains(*worker))
            .cloned()
            .collect::<Vec<_>>();
        if missing.is_empty() {
            Ok(())
        } else {
            Err(CoordinatorError::ImportBarrierIncomplete(missing))
        }
    }

    /// Fails unless every assigned worker recorded its exact completion.
    pub fn ensure_complete(&self) -> Result<()> {
        let missing = self.missing_workers();
        if missing.is_empty() {
            Ok(())
        } else {
            Err(CoordinatorError::ImportBarrierIncomplete(
                missing.into_iter().map(str::to_owned).collect(),
            ))
        }
    }
}

/// An open read-only COPY transaction pinned to the exported snapshot.
pub struct SnapshotCopyWorker<'a> {
    transaction: Transaction<'a>,
    snapshot: ExportedSnapshot,
    worker_id: String,
    assigned_tables: BTreeSet<TableId>,
    copied_tables: BTreeSet<TableId>,
    walks_key_index: bool,
}

/// How one table's COPY reads its rows.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CopyRead {
    /// By whatever plan PostgreSQL chooses.
    Planned,
    /// Along the primary-key index, whatever the planner would prefer: a
    /// sequential or bitmap scan and a sort read every row they keep before
    /// the first leaves PostgreSQL. JIT is off too: in a large shared table
    /// the walk's estimate crosses JIT's inlining and optimization thresholds,
    /// and a read that only streams rows ran faster without it.
    KeyIndex,
}

/// Read-only transaction that has locked every enrolled table but has not yet
/// imported the logical slot's exported snapshot.
///
/// Reservations are created while [`SnapshotDdlGuard`] excludes queued
/// `ACCESS EXCLUSIVE` DDL. This prevents PostgreSQL lock-queue fairness from
/// placing a concurrent `TRUNCATE` ahead of a later COPY worker.
pub struct SnapshotCopyReservation<'a> {
    transaction: Transaction<'a>,
    registry_binding: SchemaRegistryBinding,
}

impl SnapshotCopyReservation<'_> {
    pub async fn abort(self) -> Result<()> {
        self.transaction.rollback().await?;
        Ok(())
    }
}

/// Opaque COPY stream bound to one imported exported snapshot, logical table,
/// and exact server-compiled statement.
pub struct SnapshotTableCopy {
    stream: CopyOutStream,
    snapshot: ExportedSnapshot,
    table_id: TableId,
    copy_sql: String,
}

/// Holds `ACCESS SHARE` locks for every enrolled table across exported-snapshot
/// creation and COPY import.
///
/// PostgreSQL documents `TRUNCATE` and table-rewriting DDL as not MVCC-safe for
/// snapshots that have not touched the relation; these locks exclude them
/// without blocking row changes. They are taken before the slot exports `S`
/// and released when the temporary stream starts, which
/// [`SnapshotCoordinator::start_stream_after_import_controlled`] does once
/// every worker has imported `S`. Each worker's transaction holds its own
/// `ACCESS SHARE` from [`SnapshotDdlGuard::reserve_copy`] until it commits, so
/// the exclusion lasts until the last worker commits.
pub struct SnapshotDdlGuard {
    client: Option<Client>,
    registry_binding: SchemaRegistryBinding,
}

impl SnapshotDdlGuard {
    pub async fn acquire(client: Client, registry: &SchemaRegistry) -> Result<Self> {
        let registry_binding = registry.binding();
        if registry_binding.tables().is_empty() {
            return Err(CoordinatorError::InvalidConfiguration(
                "snapshot DDL guard requires at least one enrolled table".to_owned(),
            ));
        }

        validate_registry_connection(&client, &registry_binding, "snapshot DDL guard connection")
            .await?;
        let relations = quote_registry_relations(&registry_binding);

        client.batch_execute("BEGIN READ ONLY").await?;
        let lock_result = client
            .batch_execute(&format!(
                "LOCK TABLE {} IN ACCESS SHARE MODE",
                relations.join(", ")
            ))
            .await;
        if let Err(error) = lock_result {
            let _ = client.batch_execute("ROLLBACK").await;
            return Err(error.into());
        }

        Ok(Self {
            client: Some(client),
            registry_binding,
        })
    }

    /// Reserves one COPY worker while this guard still owns the pre-export DDL
    /// exclusion. Moving the guard into
    /// [`SnapshotCoordinator::export_controlled`] makes later reservations
    /// impossible. PostgreSQL permits the later `SET TRANSACTION SNAPSHOT`
    /// because `LOCK TABLE` does not take an MVCC snapshot.
    pub async fn reserve_copy<'a>(
        &self,
        client: &'a mut Client,
    ) -> Result<SnapshotCopyReservation<'a>> {
        validate_registry_connection(client, &self.registry_binding, "snapshot COPY connection")
            .await?;
        let relations = quote_registry_relations(&self.registry_binding);
        let transaction = client
            .build_transaction()
            .isolation_level(IsolationLevel::RepeatableRead)
            .read_only(true)
            .start()
            .await?;
        let lock_result = transaction
            .batch_execute(&format!(
                "LOCK TABLE {} IN ACCESS SHARE MODE NOWAIT",
                relations.join(", ")
            ))
            .await;
        if let Err(error) = lock_result {
            let contended = error.code() == Some(&SqlState::LOCK_NOT_AVAILABLE);
            let _ = transaction.rollback().await;
            if contended {
                return Err(CoordinatorError::SnapshotCopyReservationContended);
            }
            return Err(error.into());
        }
        Ok(SnapshotCopyReservation {
            transaction,
            registry_binding: self.registry_binding.clone(),
        })
    }

    async fn release(&mut self) -> Result<()> {
        let client = self
            .client
            .take()
            .ok_or(CoordinatorError::SnapshotDdlGuardMissing)?;
        client.batch_execute("COMMIT").await?;
        Ok(())
    }

    /// Aborts this build's pre-export DDL exclusion transaction. A contended
    /// COPY reservation must take this path and restart from a new exported
    /// snapshot; retrying against the existing guard would preserve the lock
    /// queue cycle.
    pub async fn abort(mut self) -> Result<()> {
        let client = self
            .client
            .take()
            .ok_or(CoordinatorError::SnapshotDdlGuardMissing)?;
        client.batch_execute("ROLLBACK").await?;
        Ok(())
    }
}

async fn validate_registry_connection(
    client: &Client,
    registry_binding: &SchemaRegistryBinding,
    context: &'static str,
) -> Result<()> {
    let expected_source = registry_binding.source_identity();
    let identity = client
        .query_one(
            "SELECT current_database(), control.system_identifier::text, \
                    checkpoint.timeline_id, checkpoint.prev_timeline_id \
               FROM pg_catalog.pg_control_system() AS control \
              CROSS JOIN pg_catalog.pg_control_checkpoint() AS checkpoint",
            &[],
        )
        .await?;
    let database: String = identity.get(0);
    let system_identifier: String = identity.get(1);
    let timeline_id: i32 = identity.get(2);
    let previous_timeline_id: i32 = identity.get(3);
    if database != expected_source.database_name
        || system_identifier != expected_source.system_identifier
        || u32::try_from(timeline_id).ok() != Some(expected_source.timeline_id)
        || u32::try_from(previous_timeline_id).ok() != Some(expected_source.previous_timeline_id)
    {
        return Err(CoordinatorError::SourceIdentityMismatch(context));
    }
    Ok(())
}

/// The enrolled relations as quoted `schema.table` names, sorted. Quoted the
/// way snapshot-builder quotes them in each table's COPY statement, so no
/// connection spends a round trip per table on it.
fn quote_registry_relations(registry_binding: &SchemaRegistryBinding) -> Vec<String> {
    let mut relations: Vec<String> = registry_binding
        .tables()
        .iter()
        .map(|(_, table)| {
            format!(
                "{}.{}",
                quote_identifier(&table.schema),
                quote_identifier(&table.table)
            )
        })
        .collect();
    relations.sort_unstable();
    relations
}

impl SnapshotTableCopy {
    pub fn into_stream_for(
        self,
        expected_snapshot: &ExportedSnapshot,
        expected_table: &TableId,
        expected_copy_sql: &str,
    ) -> Result<CopyOutStream> {
        if &self.snapshot != expected_snapshot {
            return Err(CoordinatorError::SnapshotMismatch);
        }
        if &self.table_id != expected_table || self.copy_sql != expected_copy_sql {
            return Err(CoordinatorError::CopyCapabilityMismatch);
        }
        Ok(self.stream)
    }
}

/// Opaque evidence that one imported read-only COPY transaction committed.
#[derive(Debug)]
pub struct SnapshotCopyCompletion {
    snapshot: ExportedSnapshot,
    worker_id: String,
    copied_tables: BTreeSet<TableId>,
}

impl SnapshotCopyWorker<'_> {
    pub fn exported_snapshot(&self) -> &ExportedSnapshot {
        &self.snapshot
    }

    /// Starts the exact compiler-owned binary COPY for one logical table and
    /// binds the resulting stream to this worker's imported snapshot.
    pub async fn copy_table(
        &mut self,
        table_id: TableId,
        copy_sql: &str,
        read: CopyRead,
    ) -> Result<SnapshotTableCopy> {
        if !self.assigned_tables.contains(&table_id) || self.copied_tables.contains(&table_id) {
            return Err(CoordinatorError::UnassignedOrDuplicateTableCopy(table_id));
        }
        let stream = self.copy_out(copy_sql, read).await?;
        self.copied_tables.insert(table_id.clone());
        Ok(SnapshotTableCopy {
            stream,
            snapshot: self.snapshot.clone(),
            table_id,
            copy_sql: copy_sql.to_owned(),
        })
    }

    /// Starts a maintained, streaming PostgreSQL COPY reader. The statement is
    /// restricted to an explicit server-compiled `COPY (SELECT ...)` shape.
    async fn copy_out(&mut self, copy_sql: &str, read: CopyRead) -> Result<CopyOutStream> {
        validate_copy_statement(copy_sql)?;
        let walks_key_index = read == CopyRead::KeyIndex;
        if walks_key_index != self.walks_key_index {
            let value = if walks_key_index { "off" } else { "DEFAULT" };
            self.transaction
                .batch_execute(&format!(
                    "SET LOCAL enable_seqscan TO {value};
                     SET LOCAL enable_bitmapscan TO {value};
                     SET LOCAL enable_sort TO {value};
                     SET LOCAL jit TO {value};"
                ))
                .await?;
            self.walks_key_index = walks_key_index;
        }
        Ok(self.transaction.copy_out(copy_sql).await?)
    }

    pub async fn commit(self) -> Result<SnapshotCopyCompletion> {
        if self.copied_tables != self.assigned_tables {
            let worker_id = self.worker_id.clone();
            self.transaction.rollback().await?;
            return Err(CoordinatorError::WorkerCopySetMismatch(worker_id));
        }
        self.transaction.commit().await?;
        Ok(SnapshotCopyCompletion {
            snapshot: self.snapshot,
            worker_id: self.worker_id,
            copied_tables: self.copied_tables,
        })
    }

    /// Rolls back one imported COPY transaction during build cancellation.
    /// Every active [`SnapshotTableCopy`] from this worker must be dropped
    /// before calling this method so PostgreSQL can leave COPY mode cleanly.
    pub async fn abort(self) -> Result<()> {
        self.transaction.rollback().await?;
        Ok(())
    }
}

fn validate_copy_statement(copy_sql: &str) -> Result<()> {
    let trimmed = copy_sql.trim_start();
    if trimmed
        .as_bytes()
        .get(..6)
        .is_some_and(|prefix| prefix.eq_ignore_ascii_case(b"COPY ("))
        && trimmed.to_ascii_uppercase().contains(" TO STDOUT")
    {
        Ok(())
    } else {
        Err(CoordinatorError::InvalidCopyStatement)
    }
}

/// Imports an exported logical-slot snapshot into a pre-locked COPY worker.
pub async fn import_snapshot<'a>(
    reservation: SnapshotCopyReservation<'a>,
    snapshot: &ExportedSnapshot,
    barrier: &mut ImportBarrier,
    worker_id: &str,
) -> Result<SnapshotCopyWorker<'a>> {
    if reservation.registry_binding != snapshot.registry_binding {
        return Err(CoordinatorError::SnapshotCopyReservationMismatch);
    }
    reservation
        .transaction
        .batch_execute(&format!(
            "SET TRANSACTION SNAPSHOT {}",
            snapshot.quoted_snapshot_name
        ))
        .await?;
    let assigned_tables = barrier.record_import(worker_id, snapshot)?;
    Ok(SnapshotCopyWorker {
        transaction: reservation.transaction,
        snapshot: snapshot.clone(),
        worker_id: worker_id.to_owned(),
        assigned_tables,
        copied_tables: BTreeSet::new(),
        walks_key_index: false,
    })
}

/// Owns the temporary replication session from slot creation through cut.
pub struct SnapshotCoordinator {
    connection: Option<PgReplicationConnection>,
    replication_backend_pid: i32,
    exported: ExportedSnapshot,
    ddl_guard: Option<SnapshotDdlGuard>,
    publication_option: String,
    pgoutput_profile: PgoutputLaunchProfile,
    started: bool,
    cut_observed: bool,
    received_lsn: u64,
    applied_lsn: u64,
}

impl SnapshotCoordinator {
    /// Creates a temporary `pgoutput` slot and retains its exporting session.
    /// Connection establishment is bounded by the required libpq
    /// `connect_timeout`. Once the backend PID is known, cancellation or
    /// timeout revokes that exact PostgreSQL backend and joins the blocking
    /// task before returning, so no detached task can retain the temporary
    /// slot.
    #[allow(clippy::too_many_arguments)]
    pub async fn export_controlled(
        replication_connection: &str,
        regular_client: &mut Client,
        registry: &SchemaRegistry,
        ddl_guard: SnapshotDdlGuard,
        config: SnapshotStreamConfig,
        cancellation: &CancellationToken,
        operation_timeout: Duration,
    ) -> Result<Self> {
        validate_replication_connect_timeout(replication_connection)?;
        if operation_timeout.is_zero() {
            return Err(CoordinatorError::InvalidTimeout);
        }
        let registry_binding = registry.binding();
        if ddl_guard.registry_binding != registry_binding {
            return Err(CoordinatorError::SnapshotDdlGuardMismatch);
        }
        let expected_source = registry_binding.source_identity();
        if config.publication_name != registry_binding.publication().name {
            return Err(CoordinatorError::RegistryPublicationMismatch);
        }
        let publication_option: String = regular_client
            .query_one("SELECT quote_ident($1::text)", &[&config.publication_name])
            .await?
            .get(0);
        let connection_string = replication_connection.to_owned();
        let (mut connection, replication_backend_pid, replication_shutdown) =
            tokio::task::spawn_blocking(move || -> Result<_> {
                let mut connection = PgReplicationConnection::connect(&connection_string)?;
                let backend_pid = replication_backend_pid(&mut connection)?;
                let shutdown = connection.shutdown_handle()?;
                Ok((connection, backend_pid, shutdown))
            })
            .await
            .map_err(CoordinatorError::ReplicationTask)??;
        if cancellation.is_cancelled() {
            let drop_task = tokio::task::spawn_blocking(move || -> Result<()> {
                drop(connection);
                Ok(())
            });
            join_revoked_replication_task(
                drop_task,
                replication_shutdown,
                Some(regular_client),
                replication_backend_pid,
                operation_timeout,
            )
            .await?;
            return Err(CoordinatorError::Cancelled);
        }

        let slot_name = config.slot_name.clone();
        let two_phase = config.pgoutput_profile.two_phase();
        let mut replication_task = tokio::task::spawn_blocking(move || -> Result<_> {
            let replication_identity = connection.identify_system()?;
            let replication_system = replication_identity
                .get_value(0, 0)
                .ok_or(CoordinatorError::MissingSourceIdentity)?;
            let replication_timeline = replication_identity
                .get_value(0, 1)
                .ok_or(CoordinatorError::MissingSourceIdentity)?;
            let replication_database = replication_identity
                .get_value(0, 3)
                .ok_or(CoordinatorError::MissingSourceIdentity)?;
            let slot_result = connection.create_replication_slot_with_options(
                &slot_name,
                SlotType::Logical,
                Some("pgoutput"),
                &ReplicationSlotOptions {
                    temporary: true,
                    two_phase,
                    snapshot: Some("export".to_owned()),
                    ..Default::default()
                },
            )?;
            let snapshot_name = slot_result
                .get_value(0, 2)
                .filter(|value| !value.is_empty())
                .ok_or(CoordinatorError::MissingExportedSnapshot)?;
            let created_consistent_point = slot_result
                .get_value(0, 1)
                .filter(|value| !value.is_empty())
                .ok_or(CoordinatorError::MissingConsistentPoint)
                .and_then(|value| parse_lsn(&value).map_err(CoordinatorError::Replication))?;
            Ok((
                connection,
                replication_system,
                replication_timeline,
                replication_database,
                snapshot_name,
                created_consistent_point,
            ))
        });
        let operation_deadline = tokio::time::sleep(operation_timeout);
        tokio::pin!(operation_deadline);
        let replication = tokio::select! {
            biased;
            () = cancellation.cancelled() => {
                join_revoked_replication_task(
                    replication_task,
                    replication_shutdown,
                    Some(regular_client),
                    replication_backend_pid,
                    operation_timeout,
                ).await?;
                return Err(CoordinatorError::Cancelled);
            }
            () = &mut operation_deadline => {
                join_revoked_replication_task(
                    replication_task,
                    replication_shutdown,
                    Some(regular_client),
                    replication_backend_pid,
                    operation_timeout,
                ).await?;
                return Err(CoordinatorError::ReplicationOperationTimedOut);
            }
            result = &mut replication_task => {
                result.map_err(CoordinatorError::ReplicationTask)??
            }
        };
        let (
            connection,
            replication_system,
            replication_timeline,
            replication_database,
            snapshot_name,
            created_consistent_point,
        ) = replication;
        if replication_system != expected_source.system_identifier
            || replication_timeline != expected_source.timeline_id.to_string()
            || replication_database != expected_source.database_name
        {
            return Err(CoordinatorError::SourceIdentityMismatch(
                "replication connection",
            ));
        }

        let regular_identity = regular_client
            .query_one(
                "SELECT current_database(), control.system_identifier::text, \
                        checkpoint.timeline_id, checkpoint.prev_timeline_id, \
                        EXISTS (SELECT 1 FROM pg_catalog.pg_publication WHERE pubname = $1) \
                   FROM pg_catalog.pg_control_system() AS control \
                  CROSS JOIN pg_catalog.pg_control_checkpoint() AS checkpoint",
                &[&config.publication_name],
            )
            .await?;
        let regular_database: String = regular_identity.get(0);
        let regular_system: String = regular_identity.get(1);
        let regular_timeline: i32 = regular_identity.get(2);
        let regular_previous_timeline: i32 = regular_identity.get(3);
        let publication_exists: bool = regular_identity.get(4);
        if regular_database != expected_source.database_name
            || regular_system != expected_source.system_identifier
            || u32::try_from(regular_timeline).ok() != Some(expected_source.timeline_id)
            || u32::try_from(regular_previous_timeline).ok()
                != Some(expected_source.previous_timeline_id)
        {
            return Err(CoordinatorError::SourceIdentityMismatch(
                "regular connection",
            ));
        }
        if !publication_exists {
            return Err(CoordinatorError::PublicationMissing);
        }
        let quoted_snapshot_name: String = regular_client
            .query_one("SELECT quote_literal($1::text)", &[&snapshot_name])
            .await?
            .get(0);
        // Verify the creation result against the typed catalog value instead of
        // trusting either representation alone.
        let row = regular_client
            .query_opt(
                "SELECT confirmed_flush_lsn, temporary, plugin, database
               FROM pg_replication_slots
              WHERE slot_name = $1",
                &[&config.slot_name],
            )
            .await?;
        let row = row.ok_or(CoordinatorError::SlotNotVisible)?;
        let consistent_point: Option<PgLsn> = row.get(0);
        let temporary: bool = row.get(1);
        let plugin: String = row.get(2);
        let slot_database: String = row.get(3);
        if !temporary || plugin != "pgoutput" || slot_database != expected_source.database_name {
            return Err(CoordinatorError::UnexpectedSlotMetadata);
        }
        let consistent_point = consistent_point.ok_or(CoordinatorError::MissingConsistentPoint)?;
        if u64::from(consistent_point) != created_consistent_point {
            return Err(CoordinatorError::ConsistentPointMismatch {
                created: created_consistent_point,
                catalog: u64::from(consistent_point),
            });
        }

        Ok(Self {
            connection: Some(connection),
            replication_backend_pid,
            exported: ExportedSnapshot {
                slot_name: config.slot_name,
                snapshot_name,
                consistent_point,
                quoted_snapshot_name,
                registry_binding,
                pgoutput_profile: config.pgoutput_profile,
            },
            ddl_guard: Some(ddl_guard),
            publication_option,
            pgoutput_profile: config.pgoutput_profile,
            started: false,
            cut_observed: false,
            received_lsn: 0,
            applied_lsn: 0,
        })
    }

    pub fn exported_snapshot(&self) -> &ExportedSnapshot {
        &self.exported
    }

    /// Starts the temporary stream once every COPY worker has imported `S`,
    /// before any of them copies. A build that copies after its cut uses this:
    /// the workers' snapshots are already fixed, so rolling the stream forward
    /// first cannot change what they read. See [`SnapshotDdlGuard`] for why the
    /// guard may be released here. The blocking libpq owner is never detached;
    /// a caller supplying a control connection gets exact backend revocation on
    /// cancellation or timeout.
    pub async fn start_stream_after_import_controlled(
        &mut self,
        barrier: &ImportBarrier,
        regular_client: Option<&Client>,
        cancellation: &CancellationToken,
        operation_timeout: Duration,
    ) -> Result<()> {
        self.ready_to_start(barrier, operation_timeout)?;
        barrier.ensure_imported()?;
        self.start_replication(regular_client, cancellation, operation_timeout)
            .await
    }

    fn ready_to_start(&self, barrier: &ImportBarrier, operation_timeout: Duration) -> Result<()> {
        if operation_timeout.is_zero() {
            return Err(CoordinatorError::InvalidTimeout);
        }
        if self.started {
            return Err(CoordinatorError::StreamAlreadyStarted);
        }
        if barrier.snapshot != self.exported {
            return Err(CoordinatorError::SnapshotMismatch);
        }
        Ok(())
    }

    async fn start_replication(
        &mut self,
        regular_client: Option<&Client>,
        cancellation: &CancellationToken,
        operation_timeout: Duration,
    ) -> Result<()> {
        self.ddl_guard
            .as_mut()
            .ok_or(CoordinatorError::SnapshotDdlGuardMissing)?
            .release()
            .await?;
        self.ddl_guard = None;
        let pgoutput_profile = self.pgoutput_profile;
        let publication_option = self.publication_option.clone();
        let mut connection = self
            .connection
            .take()
            .ok_or(CoordinatorError::ReplicationSessionMissing)?;
        let replication_shutdown = connection.shutdown_handle()?;
        let slot_name = self.exported.slot_name.clone();
        let consistent_point = u64::from(self.exported.consistent_point);
        let mut replication_task = tokio::task::spawn_blocking(move || -> Result<_> {
            let options =
                pgoutput_profile.start_replication_options(publication_option.as_str())?;
            connection.start_replication(&slot_name, consistent_point, &options)?;
            Ok(connection)
        });
        let operation_deadline = tokio::time::sleep(operation_timeout);
        tokio::pin!(operation_deadline);
        self.connection = Some(tokio::select! {
            biased;
            () = cancellation.cancelled() => {
                join_revoked_replication_task(
                    replication_task,
                    replication_shutdown,
                    regular_client,
                    self.replication_backend_pid,
                    operation_timeout,
                ).await?;
                return Err(CoordinatorError::Cancelled);
            }
            () = &mut operation_deadline => {
                join_revoked_replication_task(
                    replication_task,
                    replication_shutdown,
                    regular_client,
                    self.replication_backend_pid,
                    operation_timeout,
                ).await?;
                return Err(CoordinatorError::ReplicationOperationTimedOut);
            }
            result = &mut replication_task => {
                result.map_err(CoordinatorError::ReplicationTask)??
            }
        });
        self.started = true;
        Ok(())
    }

    /// Applies complete committed transactions in order until the exact
    /// dedicated cut-marker transaction commits.
    ///
    /// The callback runs once per pre-cut transaction. Advancing replication
    /// feedback is coupled to a successful callback, so a failed apply
    /// cannot be acknowledged by the temporary stream.
    ///
    /// Cancellation is caller-owned. It closes the read loop without minting
    /// a cut observation; the lifecycle owner must consume this coordinator
    /// through [`SnapshotCoordinator::abort`] so its temporary slot
    /// disappears.
    pub async fn roll_forward_to_cut_controlled<F, E>(
        &mut self,
        expected: &CutEmission,
        timeout: Duration,
        limits: DecoderLimits,
        cancellation: &CancellationToken,
        mut apply: F,
    ) -> std::result::Result<CutObservation, RollForwardError<E>>
    where
        F: FnMut(&DecodedTransaction) -> std::result::Result<(), E>,
        E: Error + 'static,
    {
        if !self.started {
            return Err(CoordinatorError::StreamNotStarted.into());
        }
        if cancellation.is_cancelled() {
            return Err(CoordinatorError::Cancelled.into());
        }
        let deadline = Instant::now()
            .checked_add(timeout)
            .ok_or(CoordinatorError::InvalidTimeout)?;
        let io_cancellation = cancellation.child_token();
        let mut decoder =
            Decoder::for_launch(self.pgoutput_profile, limits).map_err(CoordinatorError::from)?;
        let mut tracker = DecodedCutTracker::new(expected.clone())?;

        loop {
            let remaining = deadline
                .checked_duration_since(Instant::now())
                .ok_or(CoordinatorError::CutTimeout)?;
            let data = match tokio::time::timeout(
                remaining,
                self.connection
                    .as_mut()
                    .ok_or(CoordinatorError::ReplicationSessionMissing)?
                    .get_copy_data_async(&io_cancellation),
            )
            .await
            {
                Ok(result) => match result {
                    Ok(data) => data,
                    Err(_) if cancellation.is_cancelled() => {
                        return Err(CoordinatorError::Cancelled.into());
                    }
                    Err(error) => return Err(CoordinatorError::from(error).into()),
                },
                Err(_) => {
                    io_cancellation.cancel();
                    return Err(CoordinatorError::CutTimeout.into());
                }
            };
            match decoder
                .decode_copy_data(data)
                .map_err(CoordinatorError::from)?
            {
                ReplicationCopyData::Wal {
                    wal_end, events, ..
                } => {
                    self.received_lsn = self.received_lsn.max(wal_end.get());
                    for event in events {
                        let DecoderEvent::TransactionCommitted(transaction) = event else {
                            continue;
                        };
                        match tracker.classify(&transaction)? {
                            CutDisposition::Cut(observation) => {
                                self.applied_lsn = self
                                    .applied_lsn
                                    .max(transaction.end_lsn.get().min(self.received_lsn));
                                self.cut_observed = true;
                                return Ok(observation);
                            }
                            CutDisposition::Apply {
                                has_application_changes,
                            } => {
                                apply(&transaction).map_err(RollForwardError::Apply)?;
                                tracker.record_applied(has_application_changes);
                            }
                        }
                        self.applied_lsn = self
                            .applied_lsn
                            .max(transaction.end_lsn.get().min(self.received_lsn));
                    }
                }
                ReplicationCopyData::Keepalive {
                    wal_end,
                    reply_requested,
                    ..
                } => {
                    self.received_lsn = self.received_lsn.max(wal_end.get());
                    if reply_requested {
                        self.connection
                            .as_mut()
                            .ok_or(CoordinatorError::ReplicationSessionMissing)?
                            .send_standby_status_update(
                                self.received_lsn,
                                self.applied_lsn,
                                self.applied_lsn,
                                false,
                            )
                            .await
                            .map_err(CoordinatorError::from)?;
                    }
                }
            }
        }
    }

    /// Keeps the temporary stream alive after its cut until `stop` fires.
    ///
    /// A build that copies after its cut still needs the temporary slot while
    /// COPY runs: snapshot pressure is sampled against it, and PostgreSQL ends
    /// a walsender whose client stays silent past `wal_sender_timeout`. This
    /// reads only each frame's envelope, answers keepalives, and reports the
    /// cut as applied, so nothing after the cut is decoded or acknowledged.
    pub async fn hold_after_cut_controlled(
        &mut self,
        stop: &CancellationToken,
        cancellation: &CancellationToken,
    ) -> Result<()> {
        if !self.cut_observed {
            return Err(CoordinatorError::CutNotObserved);
        }
        let never = CancellationToken::new();
        loop {
            if cancellation.is_cancelled() {
                return Err(CoordinatorError::Cancelled);
            }
            if stop.is_cancelled() {
                return Ok(());
            }
            let connection = self
                .connection
                .as_mut()
                .ok_or(CoordinatorError::ReplicationSessionMissing)?;
            // Dropping the read at either signal is safe: it only awaits
            // socket readiness, and frames already read stay queued.
            let data = tokio::select! {
                biased;
                () = cancellation.cancelled() => return Err(CoordinatorError::Cancelled),
                () = stop.cancelled() => return Ok(()),
                data = connection.get_copy_data_async(&never) => data?,
            };
            let (wal_end, reply_requested) = copy_data_envelope(&data)?;
            self.received_lsn = self.received_lsn.max(wal_end);
            if reply_requested {
                connection
                    .send_standby_status_update(
                        self.received_lsn,
                        self.applied_lsn,
                        self.applied_lsn,
                        false,
                    )
                    .await?;
            }
        }
    }

    /// Closes a snapshot build without preserving its temporary slot. If the
    /// DDL guard has not yet been released it is rolled back first; consuming
    /// the coordinator then closes its replication connection, which is the
    /// owner of the PostgreSQL temporary logical slot.
    pub async fn abort(mut self) -> Result<()> {
        if let Some(guard) = self.ddl_guard.take() {
            guard.abort().await?;
        }
        drop(self);
        Ok(())
    }
}

/// Reads a replication frame's `wal_end` and reply flag without decoding its
/// payload. The frame shapes are the ones [`Decoder::decode_copy_data`] checks.
fn copy_data_envelope(data: &[u8]) -> Result<(u64, bool)> {
    let wal_end = |bytes: &[u8]| {
        let mut lsn = [0_u8; 8];
        lsn.copy_from_slice(bytes);
        u64::from_be_bytes(lsn)
    };
    match data.first() {
        None => Err(DecodeError::EmptyCopyData.into()),
        Some(b'w') if data.len() >= 25 => Ok((wal_end(&data[9..17]), false)),
        Some(b'w') => Err(DecodeError::XLogDataFrameTooShort { actual: data.len() }.into()),
        Some(b'k') if data.len() == 18 => Ok((wal_end(&data[1..9]), data[17] != 0)),
        Some(b'k') => Err(DecodeError::KeepaliveFrameLength { actual: data.len() }.into()),
        Some(&tag) => Err(DecodeError::UnknownCopyDataTag(tag).into()),
    }
}

/// Emits a unique marker in a dedicated transaction with no application writes.
pub async fn emit_cut_marker(client: &mut Client, identity: CutIdentity) -> Result<CutEmission> {
    let content = identity.bytes()?;
    let transaction = client.transaction().await?;
    let message_lsn: PgLsn = transaction
        .query_one(
            "SELECT pg_logical_emit_message(true, $1::text, $2::bytea)",
            &[&CUT_MARKER_PREFIX, &content],
        )
        .await?
        .get(0);
    transaction.commit().await?;
    Ok(CutEmission {
        identity,
        message_lsn,
    })
}

#[derive(Debug)]
struct DecodedCutTracker {
    expected: CutEmission,
    expected_content: Vec<u8>,
    committed_application_transactions: usize,
}

enum CutDisposition {
    Apply { has_application_changes: bool },
    Cut(CutObservation),
}

impl DecodedCutTracker {
    fn new(expected: CutEmission) -> Result<Self> {
        if expected.identity.build_id.is_empty() || expected.identity.nonce.is_empty() {
            return Err(CoordinatorError::InvalidCutIdentity);
        }
        let expected_content = expected.identity.bytes()?;
        Ok(Self {
            expected,
            expected_content,
            committed_application_transactions: 0,
        })
    }

    fn classify(&self, transaction: &DecodedTransaction) -> Result<CutDisposition> {
        let has_application_changes = transaction.changes.iter().any(|change| {
            matches!(
                change,
                DecodedChange::Insert { .. }
                    | DecodedChange::Update { .. }
                    | DecodedChange::Delete { .. }
                    | DecodedChange::Truncate { .. }
            )
        });
        let mut matching_marker = None;
        for change in &transaction.changes {
            let DecodedChange::LogicalMessage(message) = change else {
                continue;
            };
            if message.kind != LogicalMessageKind::CutMarker {
                continue;
            }
            let identity: CutIdentity = serde_json::from_slice(&message.content)?;
            if identity != self.expected.identity {
                if identity.build_id == self.expected.identity.build_id
                    || identity.nonce == self.expected.identity.nonce
                {
                    return Err(CoordinatorError::CutMarkerIdentityCollision);
                }
                continue;
            }
            if matching_marker.is_some() {
                return Err(CoordinatorError::DuplicateCutMarker);
            }
            if message.content != self.expected_content {
                return Err(CoordinatorError::NonCanonicalCutMarker);
            }
            let observed_lsn = PgLsn::from(message.message_lsn.get());
            if observed_lsn != self.expected.message_lsn {
                return Err(CoordinatorError::MarkerLsnMismatch {
                    emitted: self.expected.message_lsn,
                    observed: observed_lsn,
                });
            }
            matching_marker = Some((identity, observed_lsn));
        }

        if let Some((identity, message_lsn)) = matching_marker {
            if has_application_changes || transaction.changes.len() != 1 {
                return Err(CoordinatorError::CutTransactionNotDedicated);
            }
            return Ok(CutDisposition::Cut(CutObservation {
                identity,
                transaction_id: transaction.xid,
                message_lsn,
                commit_lsn: PgLsn::from(transaction.commit_lsn.get()),
                end_lsn: PgLsn::from(transaction.end_lsn.get()),
                committed_application_transactions_before_cut: self
                    .committed_application_transactions,
            }));
        }

        Ok(CutDisposition::Apply {
            has_application_changes,
        })
    }

    fn record_applied(&mut self, has_application_changes: bool) {
        if has_application_changes {
            self.committed_application_transactions += 1;
        }
    }
}

fn validate_slot_name(slot_name: &str) -> Result<()> {
    if is_valid_slot_name(slot_name) {
        Ok(())
    } else {
        Err(CoordinatorError::InvalidConfiguration(
            "slot name must match PostgreSQL's lowercase [a-z0-9_] slot-name grammar and be at most 63 bytes"
                .to_owned(),
        ))
    }
}

fn replication_backend_pid(connection: &mut PgReplicationConnection) -> Result<i32> {
    connection
        .exec("SELECT pg_backend_pid()")?
        .get_value(0, 0)
        .ok_or(CoordinatorError::ReplicationBackendPidMissing)?
        .parse()
        .map_err(|_| CoordinatorError::ReplicationBackendPidMissing)
}

/// Revokes both sides of a blocking replication operation and joins its sole
/// connection owner before returning. The local abort command is independent
/// of the PostgreSQL control connection, while the bounded server-side check
/// proves that the exact walsender backend is gone when control is available.
async fn join_revoked_replication_task<T>(
    task: tokio::task::JoinHandle<Result<T>>,
    shutdown: PgReplicationShutdownHandle,
    regular_client: Option<&Client>,
    backend_pid: i32,
    operation_timeout: Duration,
) -> Result<()> {
    // Revoke the local socket on the blocking pool while the independent
    // server-side backend revocation proceeds concurrently.
    let shutdown_task = tokio::task::spawn_blocking(move || shutdown.request_shutdown());
    let revocation = if let Some(client) = regular_client {
        Some(
            match tokio::time::timeout(
                operation_timeout,
                terminate_replication_backend(client, backend_pid),
            )
            .await
            {
                Ok(result) => result,
                Err(_) => Err(CoordinatorError::ReplicationOperationTimedOut),
            },
        )
    } else {
        None
    };
    let shutdown_result = shutdown_task.await;
    let joined = task.await;
    // Drop any successful connection-bearing operation output only after its
    // owner has joined. The emergency worker abort makes this drop local and
    // non-blocking even if the PostgreSQL control path failed.
    match joined {
        Ok(output) => drop(output),
        Err(error) => return Err(CoordinatorError::ReplicationTask(error)),
    }
    if let Some(result) = revocation {
        result?;
    }
    shutdown_result.map_err(CoordinatorError::ReplicationTask)??;
    Ok(())
}

async fn terminate_replication_backend(client: &Client, backend_pid: i32) -> Result<()> {
    let terminated: bool = client
        .query_one(
            "SELECT CASE
                       WHEN EXISTS (
                           SELECT 1 FROM pg_catalog.pg_stat_activity WHERE pid = $1
                       ) THEN pg_catalog.pg_terminate_backend($1, 1000)
                       ELSE true
                    END",
            &[&backend_pid],
        )
        .await?
        .get(0);
    if !terminated {
        return Err(CoordinatorError::ReplicationBackendRevocationFailed(
            backend_pid,
        ));
    }
    for _ in 0..20 {
        let remains: bool = client
            .query_one(
                "SELECT EXISTS (
                     SELECT 1 FROM pg_catalog.pg_stat_activity WHERE pid = $1
                 )",
                &[&backend_pid],
            )
            .await?
            .get(0);
        if !remains {
            return Ok(());
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    Err(CoordinatorError::ReplicationBackendRevocationFailed(
        backend_pid,
    ))
}

fn validate_replication_connect_timeout(connection: &str) -> Result<()> {
    let timeout = if let Some((_, query)) = connection.split_once('?') {
        query.split('&').find_map(|parameter| {
            parameter
                .split_once('=')
                .filter(|(key, _)| *key == "connect_timeout")
                .and_then(|(_, value)| value.parse::<u64>().ok())
        })
    } else {
        connection.split_whitespace().find_map(|parameter| {
            parameter
                .split_once('=')
                .filter(|(key, _)| *key == "connect_timeout")
                .and_then(|(_, value)| value.parse::<u64>().ok())
        })
    };
    if timeout.is_some_and(|seconds| (1..=MAX_REPLICATION_CONNECT_TIMEOUT_SECS).contains(&seconds))
    {
        Ok(())
    } else {
        Err(CoordinatorError::InvalidConfiguration(
            "replication connection requires connect_timeout=1..30 seconds".to_owned(),
        ))
    }
}

#[derive(Debug, Error)]
pub enum CoordinatorError {
    #[error("invalid configuration: {0}")]
    InvalidConfiguration(String),
    #[error(transparent)]
    PgoutputProfile(#[from] PgoutputProfileError),
    #[error("cut identity components must be 1..=64 ASCII alphanumeric, '-' or '_'")]
    InvalidCutIdentity,
    #[error("failed to encode or decode cut marker JSON: {0}")]
    MarkerJson(#[from] serde_json::Error),
    #[error("PostgreSQL operation failed: {0}")]
    Postgres(#[from] tokio_postgres::Error),
    #[error("replication operation failed: {0}")]
    Replication(#[from] pg_walstream::ReplicationError),
    #[error("blocking replication task failed")]
    ReplicationTask(#[source] tokio::task::JoinError),
    #[error("blocking replication operation exceeded its lifecycle deadline")]
    ReplicationOperationTimedOut,
    #[error("replication backend PID is missing or invalid")]
    ReplicationBackendPidMissing,
    #[error("replication backend {0} could not be revoked")]
    ReplicationBackendRevocationFailed(i32),
    #[error("snapshot replication session is missing")]
    ReplicationSessionMissing,
    #[error("pgoutput decoding failed: {0}")]
    Decode(#[from] DecodeError),
    #[error("temporary slot did not return an exported snapshot")]
    MissingExportedSnapshot,
    #[error("PostgreSQL connection did not return a complete source identity")]
    MissingSourceIdentity,
    #[error("{0} does not match the enrolled PostgreSQL source identity")]
    SourceIdentityMismatch(&'static str),
    #[error("configured publication does not exist on the enrolled source")]
    PublicationMissing,
    #[error("temporary stream publication differs from the activated schema registry")]
    RegistryPublicationMismatch,
    #[error("temporary slot was not visible in pg_replication_slots")]
    SlotNotVisible,
    #[error("temporary slot did not expose a consistent point")]
    MissingConsistentPoint,
    #[error(
        "temporary slot consistent point differs between creation result ({created}) and catalog ({catalog})"
    )]
    ConsistentPointMismatch { created: u64, catalog: u64 },
    #[error("temporary slot metadata was not temporary pgoutput")]
    UnexpectedSlotMetadata,
    #[error("COPY worker used a different exported snapshot")]
    SnapshotMismatch,
    #[error("unknown COPY worker {0}")]
    UnknownWorker(String),
    #[error("COPY worker {0} imported the snapshot more than once")]
    DuplicateWorker(String),
    #[error("COPY worker {0} completed without importing this snapshot")]
    WorkerWasNotImported(String),
    #[error("COPY worker {0} completed more than once")]
    DuplicateWorkerCompletion(String),
    #[error("COPY worker {0} did not complete its exact assigned table set")]
    WorkerCopySetMismatch(String),
    #[error("COPY workers have not imported the snapshot: {0:?}")]
    ImportBarrierIncomplete(Vec<String>),
    #[error("only explicit COPY (SELECT ...) TO STDOUT statements are accepted")]
    InvalidCopyStatement,
    #[error("table COPY capability does not match the build snapshot, table, or compiled SQL")]
    CopyCapabilityMismatch,
    #[error("table {0} was not assigned to this COPY worker or was copied more than once")]
    UnassignedOrDuplicateTableCopy(TableId),
    #[error("snapshot DDL guard belongs to a different activated schema registry")]
    SnapshotDdlGuardMismatch,
    #[error("snapshot DDL guard is missing or was already released")]
    SnapshotDdlGuardMissing,
    #[error("snapshot COPY reservation belongs to a different activated schema registry")]
    SnapshotCopyReservationMismatch,
    #[error("snapshot COPY reservation was queued behind incompatible DDL; abort and retry the whole build")]
    SnapshotCopyReservationContended,
    #[error("temporary stream was already started")]
    StreamAlreadyStarted,
    #[error("temporary stream has not started")]
    StreamNotStarted,
    #[error("temporary stream has not reached its cut marker")]
    CutNotObserved,
    #[error("snapshot coordination was cancelled")]
    Cancelled,
    #[error("cut observation timeout is not representable")]
    InvalidTimeout,
    #[error("timed out before observing the cut-marker commit")]
    CutTimeout,
    #[error("cut marker appeared more than once in its transaction")]
    DuplicateCutMarker,
    #[error("cut marker partially collides with the expected build identity")]
    CutMarkerIdentityCollision,
    #[error("cut marker payload did not use the canonical JSON encoding")]
    NonCanonicalCutMarker,
    #[error("cut marker LSN mismatch: emitted {emitted}, observed {observed}")]
    MarkerLsnMismatch { emitted: PgLsn, observed: PgLsn },
    #[error("cut-marker transaction was not dedicated to exactly one marker")]
    CutTransactionNotDedicated,
    #[error("persistent canonical barrier belongs to a different source epoch")]
    PersistentBarrierEpochMismatch,
    #[error("persistent cut candidate is a data transaction rather than a barrier")]
    PersistentCutIsNotBarrier,
    #[error("persistent cut barrier differs at {0}")]
    PersistentBarrierMismatch(&'static str),
    #[error("persistent barrier partially collides with the expected marker identity")]
    PersistentBarrierCollision,
    #[error("persistent barrier has invalid {field} value {value}")]
    InvalidPersistentBarrierLsn { field: &'static str, value: String },
    #[error("persistent canonical barrier position must be nonzero")]
    InvalidPersistentBarrierPosition,
    #[error("persistent barrier mapper has already resolved cursor P")]
    PersistentBarrierAlreadyResolved,
}

#[cfg(test)]
mod tests {
    use super::*;
    use canonical_log::{CanonicalBarrier, CanonicalTransaction, FileCanonicalLog, TransactionKey};
    use sync_types::SourceId;

    fn emission() -> CutEmission {
        CutEmission {
            identity: CutIdentity::new("build-1", "nonce-1").unwrap(),
            message_lsn: PgLsn::from(20),
        }
    }

    fn source_epoch() -> SourceEpoch {
        SourceEpoch::new("epoch-1").unwrap()
    }

    fn cut_observation() -> CutObservation {
        CutObservation {
            identity: emission().identity,
            transaction_id: 8,
            message_lsn: PgLsn::from(20),
            commit_lsn: PgLsn::from(21),
            end_lsn: PgLsn::from(22),
            committed_application_transactions_before_cut: 1,
        }
    }

    fn barrier_record() -> CanonicalRecord {
        CanonicalRecord {
            position: CursorPosition {
                source_epoch: source_epoch(),
                sequence: 42,
            },
            entry: CanonicalEntry::Barrier(CanonicalBarrier {
                key: TransactionKey {
                    source_epoch: source_epoch(),
                    end_lsn: PgLsn::from(22).to_string(),
                    postgres_xid: 8,
                },
                build_id: "build-1".to_owned(),
                nonce: "nonce-1".to_owned(),
                message_lsn: PgLsn::from(20).to_string(),
                commit_lsn: PgLsn::from(21).to_string(),
                commit_time_unix_micros: 1,
            }),
        }
    }

    fn decoded_transaction(xid: u32, changes: Vec<DecodedChange>) -> DecodedTransaction {
        DecodedTransaction {
            xid,
            mode: pgoutput_decoder::TransactionMode::Normal,
            first_observed_wal_lsn: pgoutput_decoder::PgLsn::new(18),
            final_lsn: Some(pgoutput_decoder::PgLsn::new(21)),
            commit_lsn: pgoutput_decoder::PgLsn::new(21),
            end_lsn: pgoutput_decoder::PgLsn::new(22),
            commit_time_postgres_micros: 5,
            changes,
        }
    }

    fn marker_change(identity: &CutIdentity, message_lsn: u64) -> DecodedChange {
        DecodedChange::LogicalMessage(pgoutput_decoder::LogicalMessage {
            message_lsn: pgoutput_decoder::PgLsn::new(message_lsn),
            prefix: CUT_MARKER_PREFIX.to_owned(),
            content: identity.bytes().unwrap(),
            kind: LogicalMessageKind::CutMarker,
        })
    }

    #[test]
    fn exact_marker_json_and_prefix_are_stable() {
        assert_eq!(CUT_MARKER_PREFIX, "sync.snapshot_cut.v1");
        assert_eq!(
            emission().identity.bytes().unwrap(),
            br#"{"build_id":"build-1","nonce":"nonce-1"}"#
        );
    }

    #[test]
    fn production_cut_identity_is_random_and_fixture_components_are_bounded() {
        let first = CutIdentity::generate();
        let second = CutIdentity::generate();
        assert_ne!(first, second);
        assert!(CutIdentity::new("a".repeat(64), "b_1").is_ok());
        assert!(matches!(
            CutIdentity::new("a".repeat(65), "b"),
            Err(CoordinatorError::InvalidCutIdentity)
        ));
        assert!(matches!(
            CutIdentity::new("contains space", "b"),
            Err(CoordinatorError::InvalidCutIdentity)
        ));
    }

    #[test]
    fn persistent_mapper_emits_p_only_for_the_exact_durable_barrier() {
        let observation = cut_observation();
        let mut mapper = PersistentBarrierMapper::new(source_epoch(), observation.clone());
        let mut unrelated = barrier_record();
        let CanonicalEntry::Barrier(barrier) = &mut unrelated.entry else {
            unreachable!();
        };
        barrier.key.postgres_xid = 99;
        barrier.key.end_lsn = PgLsn::from(99).to_string();
        barrier.build_id = "other-build".to_owned();
        barrier.nonce = "other-nonce".to_owned();
        assert_eq!(mapper.observe(&unrelated).unwrap(), None);

        let directory = tempfile::tempdir().unwrap();
        let source_id = SourceId::new("source-1").unwrap();
        let mut log =
            FileCanonicalLog::open(directory.path(), source_id.clone(), source_epoch()).unwrap();
        let CanonicalEntry::Barrier(barrier) = barrier_record().entry else {
            unreachable!();
        };
        log.append_barrier(barrier).unwrap();
        drop(log);
        let reopened = FileCanonicalLog::open(directory.path(), source_id, source_epoch()).unwrap();
        let records = reopened.read_after_bounded(0, None, 1, 64 * 1024).unwrap();

        let proof = mapper.observe(&records[0]).unwrap().unwrap();
        assert_eq!(proof.position.sequence, 1);
        assert_eq!(proof.source_epoch, source_epoch());
        assert_eq!(proof.identity, observation.identity);
        assert_eq!(proof.transaction_id, 8);
        assert_eq!(proof.message_lsn, PgLsn::from(20));
        assert_eq!(proof.commit_lsn, PgLsn::from(21));
        assert_eq!(proof.end_lsn, PgLsn::from(22));
        assert!(matches!(
            mapper.observe(&records[0]),
            Err(CoordinatorError::PersistentBarrierAlreadyResolved)
        ));
    }

    #[test]
    fn persistent_mapper_rejects_partial_identity_collision() {
        let mut record = barrier_record();
        let CanonicalEntry::Barrier(barrier) = &mut record.entry else {
            unreachable!();
        };
        barrier.key.postgres_xid = 99;
        barrier.key.end_lsn = PgLsn::from(99).to_string();
        barrier.nonce = "wrong-nonce".to_owned();
        let mut mapper = PersistentBarrierMapper::new(source_epoch(), cut_observation());
        assert!(matches!(
            mapper.observe(&record),
            Err(CoordinatorError::PersistentBarrierCollision)
        ));
    }

    #[test]
    fn persistent_mapper_rejects_transaction_at_the_cut_identity() {
        let mut record = barrier_record();
        record.entry = CanonicalEntry::Transaction(CanonicalTransaction {
            key: TransactionKey {
                source_epoch: source_epoch(),
                end_lsn: PgLsn::from(22).to_string(),
                postgres_xid: 8,
            },
            transaction_id: "transaction-8".to_owned(),
            begin_lsn: PgLsn::from(19).to_string(),
            commit_lsn: PgLsn::from(21).to_string(),
            commit_time_unix_micros: 1,
            operations: Vec::new(),
            mutation_ids: Vec::new(),
        });
        let mut mapper = PersistentBarrierMapper::new(source_epoch(), cut_observation());
        assert!(matches!(
            mapper.observe(&record),
            Err(CoordinatorError::PersistentCutIsNotBarrier)
        ));
    }

    #[test]
    fn persistent_cut_proof_rejects_every_provenance_mismatch() {
        for field in [
            "build_id",
            "nonce",
            "postgres_xid",
            "message_lsn",
            "commit_lsn",
            "end_lsn",
        ] {
            let mut record = barrier_record();
            let CanonicalEntry::Barrier(barrier) = &mut record.entry else {
                unreachable!();
            };
            match field {
                "build_id" => barrier.build_id = "wrong-build".to_owned(),
                "nonce" => barrier.nonce = "wrong-nonce".to_owned(),
                "postgres_xid" => barrier.key.postgres_xid = 9,
                "message_lsn" => barrier.message_lsn = PgLsn::from(30).to_string(),
                "commit_lsn" => barrier.commit_lsn = PgLsn::from(31).to_string(),
                "end_lsn" => barrier.key.end_lsn = PgLsn::from(32).to_string(),
                _ => unreachable!(),
            }
            assert!(matches!(
                prove_persistent_cut(&source_epoch(), &cut_observation(), &record),
                Err(CoordinatorError::PersistentBarrierMismatch(actual)) if actual == field
            ));
        }
    }

    #[test]
    fn persistent_cut_proof_rejects_bad_lsn_epoch_and_zero_position() {
        let mut bad_lsn = barrier_record();
        let CanonicalEntry::Barrier(barrier) = &mut bad_lsn.entry else {
            unreachable!();
        };
        barrier.message_lsn = "not-an-lsn".to_owned();
        assert!(matches!(
            prove_persistent_cut(&source_epoch(), &cut_observation(), &bad_lsn),
            Err(CoordinatorError::InvalidPersistentBarrierLsn {
                field: "message_lsn",
                ..
            })
        ));

        let mut wrong_epoch = barrier_record();
        wrong_epoch.position.source_epoch = SourceEpoch::new("epoch-2").unwrap();
        assert!(matches!(
            prove_persistent_cut(&source_epoch(), &cut_observation(), &wrong_epoch),
            Err(CoordinatorError::PersistentBarrierEpochMismatch)
        ));

        let mut zero_position = barrier_record();
        zero_position.position.sequence = 0;
        assert!(matches!(
            prove_persistent_cut(&source_epoch(), &cut_observation(), &zero_position),
            Err(CoordinatorError::InvalidPersistentBarrierPosition)
        ));
    }

    #[test]
    fn tracker_exposes_only_the_marker_commit_boundary() {
        let expected = emission();
        let mut tracker = DecodedCutTracker::new(expected.clone()).unwrap();
        let application = decoded_transaction(
            7,
            vec![DecodedChange::Truncate {
                relations: Vec::new(),
                cascade: false,
                restart_identity: false,
            }],
        );
        let mut applied = Vec::new();
        let CutDisposition::Apply {
            has_application_changes,
        } = tracker.classify(&application).unwrap()
        else {
            panic!("application transaction must be applied")
        };
        applied.push(application.xid);
        tracker.record_applied(has_application_changes);

        let marker = decoded_transaction(8, vec![marker_change(&expected.identity, 20)]);
        let CutDisposition::Cut(observed) = tracker.classify(&marker).unwrap() else {
            panic!("cut transaction must not be applied")
        };

        assert_eq!(applied, [7]);
        assert_eq!(observed.transaction_id, 8);
        assert_eq!(observed.committed_application_transactions_before_cut, 1);
        assert_eq!(observed.message_lsn, PgLsn::from(20));
        assert_eq!(observed.end_lsn, PgLsn::from(22));
    }

    #[test]
    fn tracker_rejects_writes_in_dedicated_marker_transaction() {
        let expected = emission();
        let tracker = DecodedCutTracker::new(expected.clone()).unwrap();
        let transaction = decoded_transaction(
            8,
            vec![
                DecodedChange::Truncate {
                    relations: Vec::new(),
                    cascade: false,
                    restart_identity: false,
                },
                marker_change(&expected.identity, 20),
            ],
        );
        assert!(matches!(
            tracker.classify(&transaction),
            Err(CoordinatorError::CutTransactionNotDedicated)
        ));
    }

    #[test]
    fn tracker_fails_closed_on_partial_marker_identity_collision() {
        let expected = emission();
        let colliding = CutIdentity::new(&expected.identity.build_id, "other-nonce").unwrap();
        let transaction = decoded_transaction(8, vec![marker_change(&colliding, 20)]);
        let tracker = DecodedCutTracker::new(expected).unwrap();
        assert!(matches!(
            tracker.classify(&transaction),
            Err(CoordinatorError::CutMarkerIdentityCollision)
        ));
    }

    #[test]
    fn tracker_refuses_a_repeated_reencoded_or_moved_marker() {
        let expected = emission();
        let tracker = DecodedCutTracker::new(expected.clone()).unwrap();
        let twice = decoded_transaction(
            8,
            vec![
                marker_change(&expected.identity, 20),
                marker_change(&expected.identity, 20),
            ],
        );
        assert!(matches!(
            tracker.classify(&twice),
            Err(CoordinatorError::DuplicateCutMarker)
        ));

        // The same identity in other bytes: a marker this build did not write.
        let mut reencoded = marker_change(&expected.identity, 20);
        let DecodedChange::LogicalMessage(message) = &mut reencoded else {
            unreachable!();
        };
        message.content = br#"{"nonce":"nonce-1","build_id":"build-1"}"#.to_vec();
        assert!(matches!(
            tracker.classify(&decoded_transaction(8, vec![reencoded])),
            Err(CoordinatorError::NonCanonicalCutMarker)
        ));

        let moved = decoded_transaction(8, vec![marker_change(&expected.identity, 19)]);
        assert!(matches!(
            tracker.classify(&moved),
            Err(CoordinatorError::MarkerLsnMismatch { emitted, observed })
                if emitted == PgLsn::from(20) && observed == PgLsn::from(19)
        ));
    }

    /// A registry of `public.tasks` and `public.users`, as enrolment would
    /// build it from an inspection.
    fn two_table_registry() -> SchemaRegistry {
        use source_control::{
            ColumnInspection, EnrollmentRequirements, EnrollmentSnapshot, PublicationSettings,
            QualifiedTable, SourceIdentity, SourceSettings, TableInspection,
        };

        let identity = SourceIdentity {
            system_identifier: "7612345678901234567".to_owned(),
            database_name: "app".to_owned(),
            timeline_id: 1,
            previous_timeline_id: 1,
        };
        let inspection = |table: &str, relation_oid: u32| TableInspection {
            table: QualifiedTable {
                schema: "public".to_owned(),
                table: table.to_owned(),
            },
            relation_oid,
            in_publication: true,
            relation_kind: 'r',
            persistence: 'p',
            replica_identity: 'd',
            is_partition: false,
            has_publication_row_filter: false,
            has_publication_column_list: false,
            unlisted_columns: Vec::new(),
            columns: vec![ColumnInspection {
                attribute_number: 1,
                name: "id".to_owned(),
                type_oid: 20,
                type_modifier: -1,
                type_schema: "pg_catalog".to_owned(),
                type_name: "int8".to_owned(),
                nullable: false,
                primary_key_ordinal: Some(1),
                generated: false,
                nondeterministic_collation: false,
                carried_as: None,
            }],
        };
        let tables = vec![inspection("tasks", 16_384), inspection("users", 16_385)];
        let snapshot = EnrollmentSnapshot {
            identity: identity.clone(),
            settings: SourceSettings {
                server_version_num: 180_004,
                server_encoding: "UTF8".to_owned(),
                date_style: "ISO, MDY".to_owned(),
                time_zone: "UTC".to_owned(),
                bytea_output: "hex".to_owned(),
                wal_level: "logical".to_owned(),
                max_replication_slots: 10,
                max_wal_senders: 10,
                current_user: "sync".to_owned(),
                current_user_is_superuser: false,
                current_user_has_replication: true,
            },
            publication: PublicationSettings {
                name: "sync_publication".to_owned(),
                exists: true,
                publishes_insert: true,
                publishes_update: true,
                publishes_delete: true,
                publishes_truncate: true,
                publishes_via_partition_root: false,
            },
            tables: tables.clone(),
        };
        let requirements = EnrollmentRequirements {
            expected_identity: identity,
            publication_name: "sync_publication".to_owned(),
            tables: tables.into_iter().map(|table| table.table).collect(),
            pgoutput_profile: PgoutputLaunchProfile::V1,
        };
        SchemaRegistry::initialize(&snapshot, &requirements).unwrap()
    }

    #[test]
    fn the_import_barrier_admits_each_assigned_worker_once_and_completes_it_once() {
        let registry = two_table_registry();
        let snapshot = ExportedSnapshot {
            slot_name: "snapshot_build_1".to_owned(),
            snapshot_name: "00000003-00000002-1".to_owned(),
            consistent_point: PgLsn::from(10),
            quoted_snapshot_name: "'00000003-00000002-1'".to_owned(),
            registry_binding: registry.binding(),
            pgoutput_profile: PgoutputLaunchProfile::V1,
        };
        let tasks = registry.tables()[0].table_id.clone();
        let users = registry.tables()[1].table_id.clone();
        let mut barrier = ImportBarrier::new(
            &snapshot,
            [("copy-0", [tasks.clone()]), ("copy-1", [users.clone()])],
        )
        .unwrap();
        let completion = |worker: &str, table: &TableId| SnapshotCopyCompletion {
            snapshot: snapshot.clone(),
            worker_id: worker.to_owned(),
            copied_tables: BTreeSet::from([table.clone()]),
        };

        assert!(matches!(
            barrier.record_import("copy-9", &snapshot),
            Err(CoordinatorError::UnknownWorker(worker)) if worker == "copy-9"
        ));
        assert_eq!(
            barrier.record_import("copy-0", &snapshot).unwrap(),
            BTreeSet::from([tasks.clone()])
        );
        assert!(matches!(
            barrier.record_import("copy-0", &snapshot),
            Err(CoordinatorError::DuplicateWorker(worker)) if worker == "copy-0"
        ));
        assert!(matches!(
            barrier.ensure_imported(),
            Err(CoordinatorError::ImportBarrierIncomplete(missing)) if missing == ["copy-1"]
        ));

        assert!(matches!(
            barrier.record_completion(completion("copy-9", &users)),
            Err(CoordinatorError::UnknownWorker(worker)) if worker == "copy-9"
        ));
        assert!(matches!(
            barrier.record_completion(completion("copy-1", &users)),
            Err(CoordinatorError::WorkerWasNotImported(worker)) if worker == "copy-1"
        ));
        barrier
            .record_completion(completion("copy-0", &tasks))
            .unwrap();
        assert!(matches!(
            barrier.record_completion(completion("copy-0", &tasks)),
            Err(CoordinatorError::DuplicateWorkerCompletion(worker)) if worker == "copy-0"
        ));
        assert_eq!(barrier.missing_workers(), ["copy-1"]);
    }

    #[test]
    fn only_a_copy_of_a_query_to_stdout_is_started() {
        for accepted in [
            "COPY (SELECT id FROM tasks) TO STDOUT",
            "  copy (select id from tasks) to stdout with (format binary)",
        ] {
            validate_copy_statement(accepted).unwrap();
        }
        for refused in [
            "",
            "SELECT id FROM tasks",
            "COPY tasks TO STDOUT",
            "COPY(SELECT id FROM tasks) TO STDOUT",
            "COPY (SELECT id FROM tasks) TO '/tmp/tasks'",
            "COPY tasks FROM STDIN",
        ] {
            assert!(
                matches!(
                    validate_copy_statement(refused),
                    Err(CoordinatorError::InvalidCopyStatement)
                ),
                "{refused:?}"
            );
        }
    }

    #[test]
    fn a_held_stream_reads_only_frame_envelopes() {
        let mut xlog = vec![b'w'];
        xlog.extend_from_slice(&7_u64.to_be_bytes());
        xlog.extend_from_slice(&42_u64.to_be_bytes());
        xlog.extend_from_slice(&0_i64.to_be_bytes());
        assert_eq!(copy_data_envelope(&xlog).unwrap(), (42, false));
        xlog.extend_from_slice(b"any payload is left undecoded");
        assert_eq!(copy_data_envelope(&xlog).unwrap(), (42, false));

        let mut keepalive = vec![b'k'];
        keepalive.extend_from_slice(&99_u64.to_be_bytes());
        keepalive.extend_from_slice(&0_i64.to_be_bytes());
        keepalive.push(1);
        assert_eq!(copy_data_envelope(&keepalive).unwrap(), (99, true));
        keepalive[17] = 0;
        assert_eq!(copy_data_envelope(&keepalive).unwrap(), (99, false));

        for frame in [&b""[..], &xlog[..24], &keepalive[..17], b"x"] {
            assert!(matches!(
                copy_data_envelope(frame),
                Err(CoordinatorError::Decode(_))
            ));
        }
    }

    #[test]
    fn slot_names_follow_postgres_replication_slot_grammar() {
        let config = SnapshotStreamConfig::new("snapshot_build_1", "pub1").unwrap();
        assert_eq!(config.pgoutput_profile, PgoutputLaunchProfile::V1);
        assert!(SnapshotStreamConfig::new("UPPER", "pub1").is_err());
        assert!(SnapshotStreamConfig::new("bad-hyphen", "pub1").is_err());
        assert!(SnapshotStreamConfig::new("", "pub1").is_err());
    }

    #[test]
    fn replication_connection_requires_a_finite_connect_deadline() {
        assert!(validate_replication_connect_timeout(
            "postgresql://localhost/db?replication=database&connect_timeout=5"
        )
        .is_ok());
        assert!(validate_replication_connect_timeout(
            "host=localhost dbname=db replication=database connect_timeout=30"
        )
        .is_ok());
        for connection in [
            "postgresql://localhost/db?replication=database",
            "postgresql://localhost/db?connect_timeout=0&replication=database",
            "postgresql://localhost/db?connect_timeout=31&replication=database",
            "host=localhost dbname=db replication=database",
        ] {
            assert!(matches!(
                validate_replication_connect_timeout(connection),
                Err(CoordinatorError::InvalidConfiguration(_))
            ));
        }
    }
}
