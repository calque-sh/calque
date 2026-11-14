//! Durable source-wide canonical transaction log.

use std::{
    collections::{BTreeMap, HashSet},
    io,
    ops::Range,
    path::{Path, PathBuf},
    sync::{Arc, Mutex, OnceLock},
    time::Duration,
};

use postgres::{types::PgLsn, Client, GenericClient, Row, Statement, Transaction};
use postgres_connect::DropOffRuntime;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use sync_protocol::{decode_primary_key, PrimaryKeyCodecError};
use sync_types::{
    ColumnId, CursorPosition, IdentifierError, MutationId, PlanId, SourceEpoch, SourceId, TableId,
};
use thiserror::Error;

#[cfg(any(test, feature = "test-support"))]
mod file_log;
mod stored_entry;
#[cfg(any(test, feature = "test-support"))]
pub use file_log::FileCanonicalLog;
pub use stored_entry::EntryDecodeError;

const MAX_RECORD_BYTES: u64 = 64 * 1024 * 1024;
/// Admission budget that keeps one source transaction inside a 16 MiB change
/// response after protocol framing. A stored entry's operations are the change
/// stream's own messages, so its bytes bound what a response sends of it.
const MAX_DELIVERABLE_TRANSACTION_ENTRY_BYTES: usize = 12 * 1024 * 1024;
const MAX_DELIVERABLE_TRANSACTION_OPERATIONS: usize = 100_000;
/// PostgreSQL `DataRow` bytes outside the entry and checksum values: message
/// tag/length, field count, three field lengths, and the 8-byte sequence.
const POSTGRES_READ_DATA_ROW_FIXED_BYTES: u64 = 1 + 4 + 2 + (3 * 4) + 8;
/// Most entry bytes one append statement carries. A larger group runs as
/// several statements inside one transaction.
const MAX_APPEND_STATEMENT_BYTES: usize = 8 * 1024 * 1024;
const POSTGRES_READ_MINIMUM_CHARGED_ROW_BYTES: u64 = POSTGRES_READ_DATA_ROW_FIXED_BYTES + 64;

/// Stable, validated identity of one snapshot build across process restarts.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub struct SnapshotBuildId(PlanId);

impl SnapshotBuildId {
    pub fn new(value: impl Into<String>) -> Result<Self, IdentifierError> {
        PlanId::new(value).map(Self)
    }

    pub fn as_str(&self) -> &str {
        self.0.as_str()
    }
}

/// Stable, non-secret identity of the process or job renewing a build lease.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub struct SnapshotBuildOwnerId(PlanId);

impl SnapshotBuildOwnerId {
    pub fn new(value: impl Into<String>) -> Result<Self, IdentifierError> {
        PlanId::new(value).map(Self)
    }

    pub fn as_str(&self) -> &str {
        self.0.as_str()
    }
}

/// Exact authority to renew or release one snapshot-build retention floor.
///
/// Fields are private so stale callers cannot manufacture a newer token or
/// substitute another source epoch, build, or owner.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SnapshotBuildRetentionLease {
    source_id: SourceId,
    source_epoch: SourceEpoch,
    build_id: SnapshotBuildId,
    owner_id: SnapshotBuildOwnerId,
    token: i64,
    minimum_sequence: u64,
    expires_at_unix_millis: i64,
}

impl SnapshotBuildRetentionLease {
    pub fn source_epoch(&self) -> &SourceEpoch {
        &self.source_epoch
    }

    pub const fn token(&self) -> i64 {
        self.token
    }

    pub const fn minimum_sequence(&self) -> u64 {
        self.minimum_sequence
    }

    pub const fn expires_at_unix_millis(&self) -> i64 {
        self.expires_at_unix_millis
    }
}

/// Provider-neutral snapshot-build retention capability.
///
/// Acquisition must happen before snapshot export. The provider atomically
/// chooses the current complete canonical head plus one as the retained floor.
pub trait SnapshotBuildRetention {
    /// Hard upper bound enforced by the provider itself for every retention
    /// operation. Callers use it for lease admission; implementations must
    /// configure their storage/network deadlines rather than relying on a
    /// detached outer thread timeout.
    fn maximum_retention_operation_duration(&self) -> Duration;

    fn acquire_snapshot_build_retention(
        &mut self,
        build_id: SnapshotBuildId,
        owner_id: SnapshotBuildOwnerId,
        ttl: Duration,
    ) -> Result<SnapshotBuildRetentionLease, LogError>;

    fn renew_snapshot_build_retention(
        &mut self,
        lease: &SnapshotBuildRetentionLease,
        ttl: Duration,
    ) -> Result<SnapshotBuildRetentionLease, LogError>;

    fn release_snapshot_build_retention(
        &mut self,
        lease: &SnapshotBuildRetentionLease,
    ) -> Result<(), LogError>;
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct TransactionKey {
    pub source_epoch: SourceEpoch,
    pub end_lsn: String,
    pub postgres_xid: u32,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct CanonicalTransaction {
    pub key: TransactionKey,
    pub transaction_id: String,
    pub begin_lsn: String,
    pub commit_lsn: String,
    pub commit_time_unix_micros: u64,
    pub operations: Vec<CanonicalOperation>,
    pub mutation_ids: Vec<MutationId>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct CanonicalBarrier {
    pub key: TransactionKey,
    pub build_id: String,
    pub nonce: String,
    pub message_lsn: String,
    pub commit_lsn: String,
    pub commit_time_unix_micros: u64,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(
    tag = "recordKind",
    content = "record",
    rename_all = "SCREAMING_SNAKE_CASE"
)]
pub enum CanonicalEntry {
    Transaction(CanonicalTransaction),
    Barrier(CanonicalBarrier),
}

impl CanonicalEntry {
    pub fn key(&self) -> &TransactionKey {
        match self {
            Self::Transaction(transaction) => &transaction.key,
            Self::Barrier(barrier) => &barrier.key,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", try_from = "StoredOperation")]
pub struct CanonicalOperation {
    pub table_id: TableId,
    pub primary_key: Vec<u8>,
    #[serde(flatten)]
    pub kind: CanonicalOperationKind,
}

/// The JSON form of a [`CanonicalOperation`] in the filesystem log that tests
/// use, with the kind's fields beside `kind`.
///
/// The fields are read directly rather than through `#[serde(flatten)]`: with
/// serde_json's `arbitrary_precision`, flatten's intermediate tree holds a
/// number with a fraction or an exponent as a map no float can be read from.
/// A well-formed field another kind owns is ignored, as flatten ignores it.
#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct StoredOperation {
    table_id: TableId,
    primary_key: Vec<u8>,
    kind: StoredOperationKind,
    columns: Option<Vec<ColumnValue>>,
    #[serde(rename = "changed_columns")]
    changed_columns: Option<Vec<ColumnValue>>,
    reason: Option<String>,
}

#[derive(Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
enum StoredOperationKind {
    InsertFullRow,
    UpdatePatch,
    DeleteByKey,
    TruncateAuthorizedTable,
}

impl TryFrom<StoredOperation> for CanonicalOperation {
    type Error = &'static str;

    fn try_from(stored: StoredOperation) -> Result<Self, Self::Error> {
        let kind = match stored.kind {
            StoredOperationKind::InsertFullRow => CanonicalOperationKind::InsertFullRow {
                columns: stored.columns.ok_or("missing field `columns`")?,
            },
            StoredOperationKind::UpdatePatch => CanonicalOperationKind::UpdatePatch {
                changed_columns: stored
                    .changed_columns
                    .ok_or("missing field `changed_columns`")?,
            },
            StoredOperationKind::DeleteByKey => CanonicalOperationKind::DeleteByKey,
            StoredOperationKind::TruncateAuthorizedTable => {
                CanonicalOperationKind::TruncateAuthorizedTable {
                    reason: stored.reason.ok_or("missing field `reason`")?,
                }
            }
        };
        Ok(Self {
            table_id: stored.table_id,
            primary_key: stored.primary_key,
            kind,
        })
    }
}

/// The SHA-256 a record is stored beside, in lowercase hex.
fn sha256_hex(bytes: &[u8]) -> String {
    hex::encode(Sha256::digest(bytes))
}

/// Whether `stored` is exactly the lowercase hex SHA-256 of `bytes`.
fn sha256_hex_matches(bytes: &[u8], stored: &str) -> bool {
    let mut actual = [0_u8; 64];
    hex::encode_to_slice(Sha256::digest(bytes), &mut actual)
        .is_ok_and(|()| actual == stored.as_bytes())
}

/// Decodes one stored entry, naming an entry an earlier release wrote as
/// [`LogError::EarlierEntryFormat`] for the state directory `state_dir`.
fn decode_stored_entry(bytes: &[u8], state_dir: &Path) -> Result<CanonicalEntry, LogError> {
    CanonicalEntry::decode(bytes).map_err(|error| match error {
        EntryDecodeError::EarlierFormat => LogError::EarlierEntryFormat {
            state_dir: state_dir.to_path_buf(),
        },
        error => LogError::EntryDecode(error),
    })
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "SCREAMING_SNAKE_CASE")]
pub enum CanonicalOperationKind {
    InsertFullRow { columns: Vec<ColumnValue> },
    UpdatePatch { changed_columns: Vec<ColumnValue> },
    DeleteByKey,
    TruncateAuthorizedTable { reason: String },
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ColumnValue {
    pub column_id: ColumnId,
    pub value: CellValue,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "kind", content = "value", rename_all = "SCREAMING_SNAKE_CASE")]
pub enum CellValue {
    Null,
    Int64(i64),
    Uint64(u64),
    Float64(f64),
    Text(String),
    Blob(Vec<u8>),
    Bool(bool),
    DecimalText(String),
    TimestampUnixMicros(i64),
    Uuid16([u8; 16]),
    JsonText(String),
    DateIso8601(String),
    TimeIso8601(String),
    IntervalIso8601(String),
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct CanonicalRecord {
    pub position: CursorPosition,
    pub entry: CanonicalEntry,
}

#[derive(Debug, Clone, PartialEq)]
pub struct CanonicalPage {
    pub records: Vec<CanonicalRecord>,
    /// Checksummed provider bytes consumed by this page.
    pub stored_bytes: u64,
}

/// Read-only access to one complete, retained canonical source history.
///
/// Implementations must return only contiguous complete records, validate
/// provider integrity before returning them, and enforce both page limits.
/// This capability grants no append, fencing, or retention authority.
pub trait CanonicalLogRead: Send + Sync {
    fn source_id(&self) -> &SourceId;

    fn source_epoch(&self) -> &SourceEpoch;

    fn head(&self) -> Result<CursorPosition, LogError>;

    fn minimum_retained(&self) -> Result<CursorPosition, LogError>;

    /// The head and the retention floor together. A log that can read both
    /// from one snapshot in one statement does so.
    fn bounds(&self) -> Result<LogBounds, LogError> {
        Ok(LogBounds {
            head: self.head()?,
            minimum_retained: self.minimum_retained()?,
        })
    }

    fn read_page_after_bounded(
        &self,
        from_sequence_exclusive: u64,
        through_sequence_inclusive: Option<u64>,
        max_records: usize,
        max_bytes: u64,
    ) -> Result<CanonicalPage, LogError>;

    /// The bounds, and the page [`Self::read_page_after_bounded`] returns for
    /// the same arguments. The page is a result of its own, so a page the log
    /// refuses still leaves the bounds to check first. A log that can read
    /// both from one snapshot in one round trip does so.
    fn read_bounds_and_page_after_bounded(
        &self,
        from_sequence_exclusive: u64,
        through_sequence_inclusive: Option<u64>,
        max_records: usize,
        max_bytes: u64,
    ) -> Result<(LogBounds, Result<CanonicalPage, LogError>), LogError> {
        let bounds = self.bounds()?;
        let page = self.read_page_after_bounded(
            from_sequence_exclusive,
            through_sequence_inclusive,
            max_records,
            max_bytes,
        );
        Ok((bounds, page))
    }
}

/// A log's head and its retention floor, read together.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LogBounds {
    pub head: CursorPosition,
    pub minimum_retained: CursorPosition,
}

/// Durable retention authority required while issuing a bootstrap plan.
///
/// This remains separate from [`CanonicalLogRead`] so consumers that only
/// serve canonical history cannot accidentally gain retention-write access.
pub trait CanonicalLogPlanRetention {
    fn acquire_plan_retention_lease(
        &mut self,
        plan_id: PlanId,
        minimum_sequence: u64,
        expires_ms: i64,
        now_ms: i64,
    ) -> Result<(), LogError>;
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum AppendOutcome {
    Appended(CursorPosition),
    AlreadyPresent(CursorPosition),
}

impl AppendOutcome {
    pub fn position(&self) -> &CursorPosition {
        match self {
            Self::Appended(position) | Self::AlreadyPresent(position) => position,
        }
    }
}

#[derive(Debug, Error)]
pub enum LogError {
    #[error("canonical log I/O failed: {0}")]
    Io(#[from] io::Error),
    #[error("canonical log serialization failed: {0}")]
    Serialization(#[from] serde_json::Error),
    #[error("canonical entry does not decode: {0}")]
    EntryDecode(EntryDecodeError),
    #[error("canonical log manifest is corrupt: {0}")]
    ManifestCorrupt(String),
    #[error("canonical segment {sequence} is corrupt: {reason}")]
    SegmentCorrupt { sequence: u64, reason: String },
    #[error("transaction belongs to source epoch {actual}, expected {expected}")]
    EpochMismatch {
        expected: SourceEpoch,
        actual: SourceEpoch,
    },
    #[error("canonical entry identity already exists with different contents")]
    DuplicateTransactionConflict,
    #[error(
        "PostgreSQL sent a transaction again that ends at or before the LSN through which the log forgot retired transactions"
    )]
    ReplayBehindCompaction,
    #[error("canonical entry is invalid: {0}")]
    InvalidEntry(&'static str),
    #[error("canonical operation primary key is invalid: {0}")]
    InvalidPrimaryKey(#[from] PrimaryKeyCodecError),
    #[error("requested sequence {requested} is before retention floor {minimum}")]
    BeforeRetention { requested: u64, minimum: u64 },
    #[error("canonical sequence gap: expected {expected}, found {actual}")]
    SequenceGap { expected: u64, actual: u64 },
    #[error("canonical sequence space is exhausted")]
    SequenceExhausted,
    #[error("record exceeds the 64 MiB canonical segment limit")]
    RecordTooLarge,
    #[error(
        "canonical transaction has {actual} operations, exceeding the deliverable limit {maximum}"
    )]
    TransactionOperationLimitExceeded { actual: usize, maximum: usize },
    #[error("canonical transaction encoding is {actual} bytes, exceeding the deliverable limit {maximum}")]
    TransactionDeliveryLimitExceeded { actual: usize, maximum: usize },
    #[error("bootstrap retention lease expiry or range is invalid")]
    InvalidRetentionLease,
    #[error("bootstrap retention lease ID was reused for a different range or expiry")]
    RetentionLeaseConflict,
    #[error("snapshot-build retention lease duration or floor is invalid")]
    InvalidSnapshotBuildRetentionLease,
    #[error("snapshot-build retention lease is owned by another active capability")]
    SnapshotBuildRetentionLeaseConflict,
    #[error("snapshot-build retention lease is expired, stale, or belongs to another identity")]
    SnapshotBuildRetentionLeaseRejected,
    #[error("snapshot-build retention lease token space is exhausted")]
    SnapshotBuildRetentionTokenExhausted,
    #[error("system clock is before the Unix epoch or exceeds signed milliseconds")]
    SystemClockInvalid,
    #[error("manifest exceeds the 64 MiB durable file limit")]
    ManifestTooLarge,
    #[error("canonical read limits must allow at least one record and one byte")]
    InvalidReadLimit,
    #[error("fenced PostgreSQL canonical log is unavailable or returned an uncertain result")]
    PostgresUnavailable,
    #[error("fenced PostgreSQL canonical log connected to a different control datastore")]
    DatastoreIdentityMismatch,
    #[error("canonical append fence is stale, expired, inactive, or belongs to another epoch")]
    FencedWriterRejected,
    #[error("canonical append fence token must be positive")]
    InvalidFenceToken,
    #[error("PostgreSQL canonical head is corrupt or belongs to another source epoch")]
    PostgresHeadCorrupt,
    #[error("{state_dir} serves a canonical log an earlier release of calque wrote, and calque does not migrate it.", state_dir = state_dir.display())]
    EarlierEntryFormat { state_dir: PathBuf },
    #[error(
        "next canonical record is {next_record_bytes} bytes, exceeding the {max_bytes}-byte read limit"
    )]
    ReadLimitTooSmall {
        next_record_bytes: u64,
        max_bytes: u64,
    },
}

impl LogError {
    /// Whether only `calque destroy` gets past this refusal. The command needs
    /// the application's database URL variable, which the log does not know,
    /// so the caller names it.
    pub fn requires_destroying_state(&self) -> bool {
        matches!(self, Self::EarlierEntryFormat { .. })
    }
}

/// Stable, non-secret identity for one PostgreSQL database inside one cluster.
///
/// A connection string is routing configuration, not identity. The source
/// fence claim carries this value so a canonical writer cannot be redirected
/// to a copied or otherwise unrelated control database that happens to contain
/// matching fence-token rows.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PostgresDatastoreIdentity {
    system_identifier: String,
    database_oid: u32,
}

impl PostgresDatastoreIdentity {
    pub fn inspect(client: &mut Client) -> Result<Self, LogError> {
        let row = client
            .query_one(
                "SELECT control.system_identifier::text,
                        database.oid::bigint
                   FROM pg_catalog.pg_control_system() AS control
                  CROSS JOIN pg_catalog.pg_database AS database
                  WHERE database.datname = current_database()",
                &[],
            )
            .map_err(postgres_log_error)?;
        let database_oid: i64 = row.get(1);
        Ok(Self {
            system_identifier: row.get(0),
            database_oid: u32::try_from(database_oid)
                .map_err(|_| LogError::DatastoreIdentityMismatch)?,
        })
    }
}

/// Database-verified identity for one externally fenced canonical writer.
///
/// Constructing this value grants no authority: every append locks and checks
/// the active lease and durable writer-fence rows in the same PostgreSQL
/// transaction that assigns and commits the canonical sequence.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FencedWriterIdentity {
    source_id: SourceId,
    source_epoch: SourceEpoch,
    fence_token: i64,
    datastore: PostgresDatastoreIdentity,
}

impl FencedWriterIdentity {
    pub fn new(
        source_id: SourceId,
        source_epoch: SourceEpoch,
        fence_token: i64,
        datastore: PostgresDatastoreIdentity,
    ) -> Result<Self, LogError> {
        if fence_token <= 0 {
            return Err(LogError::InvalidFenceToken);
        }
        Ok(Self {
            source_id,
            source_epoch,
            fence_token,
            datastore,
        })
    }

    pub fn source_id(&self) -> &SourceId {
        &self.source_id
    }

    pub fn source_epoch(&self) -> &SourceEpoch {
        &self.source_epoch
    }
}

/// Transaction-scoped advisory lock guarding every `CREATE ... IF NOT EXISTS`
/// that installs into the `calque_control` schema.
///
/// `IF NOT EXISTS` is check-then-create: two sessions can both see the object
/// absent, and one then fails with a unique violation on a catalog index.
/// Every crate that installs into `calque_control` must take this key, since
/// they all run `CREATE SCHEMA IF NOT EXISTS calque_control`.
pub const CONTROL_SCHEMA_BOOTSTRAP_LOCK_KEY: i64 = 7_249_927_482_239_849_732;

const POSTGRES_LOG_INSTALL_SQL: &str = r#"
CREATE SCHEMA IF NOT EXISTS calque_control;
CREATE TABLE IF NOT EXISTS calque_control.canonical_log_head (
    source_id text PRIMARY KEY,
    source_epoch text NOT NULL,
    head_sequence bigint NOT NULL CHECK (head_sequence >= 0)
);
CREATE TABLE IF NOT EXISTS calque_control.canonical_log_record (
    source_id text NOT NULL,
    source_epoch text NOT NULL,
    sequence bigint NOT NULL CHECK (sequence > 0),
    transaction_identity text NOT NULL,
    entry_bytes bytea NOT NULL,
    entry_sha256 text NOT NULL,
    PRIMARY KEY (source_id, sequence),
    UNIQUE (source_id, source_epoch, transaction_identity)
);
CREATE TABLE IF NOT EXISTS calque_control.canonical_log_retention (
    source_id text PRIMARY KEY,
    source_epoch text NOT NULL,
    minimum_retained_sequence bigint NOT NULL DEFAULT 1
        CHECK (minimum_retained_sequence > 0),
    next_build_lease_token bigint NOT NULL DEFAULT 0
        CHECK (next_build_lease_token >= 0)
);
ALTER TABLE calque_control.canonical_log_retention
    ADD COLUMN IF NOT EXISTS retired_compacted_through_lsn pg_lsn;
CREATE TABLE IF NOT EXISTS calque_control.canonical_log_plan_retention_lease (
    source_id text NOT NULL,
    source_epoch text NOT NULL,
    plan_id text NOT NULL,
    minimum_sequence bigint NOT NULL CHECK (minimum_sequence > 0),
    expires_at timestamptz NOT NULL,
    PRIMARY KEY (source_id, source_epoch, plan_id)
);
CREATE TABLE IF NOT EXISTS calque_control.canonical_log_build_retention_lease (
    source_id text NOT NULL,
    source_epoch text NOT NULL,
    build_id text NOT NULL,
    owner_id text NOT NULL,
    lease_token bigint NOT NULL CHECK (lease_token > 0),
    minimum_sequence bigint NOT NULL CHECK (minimum_sequence > 0),
    expires_at timestamptz NOT NULL,
    PRIMARY KEY (source_id, source_epoch, build_id),
    UNIQUE (source_id, source_epoch, lease_token)
);
CREATE TABLE IF NOT EXISTS calque_control.canonical_log_retired_record (
    source_id text NOT NULL,
    source_epoch text NOT NULL,
    original_sequence bigint NOT NULL CHECK (original_sequence > 0),
    transaction_identity text NOT NULL,
    entry_sha256 text NOT NULL,
    PRIMARY KEY (source_id, source_epoch, transaction_identity),
    UNIQUE (source_id, source_epoch, original_sequence)
);
"#;

/// Appends a group of canonical entries under the writer fence, in array order.
///
/// `$4`, `$5`, `$6` and `$8` hold each entry's transaction key, bytes, SHA-256
/// and end LSN in group order, and `$7` is the durable checkpoint LSN or NULL.
/// The statement returns the group's status, then each entry's status and
/// sequence in group order, and it writes only when the group's status is
/// `ok`. New entries take consecutive sequences after the head in group order;
/// an entry already durable, live or retired, keeps its sequence. An entry
/// that is neither and ends at or before the LSN through which the log forgot
/// retired identities was appended before and forgotten, and its group is
/// refused as `compacted`.
const APPEND_GROUP_SQL: &str = "WITH
 authorized AS MATERIALIZED (
    SELECT lease.fence_token
      FROM calque_control.source_fence_lease AS lease
      JOIN calque_control.canonical_log_writer_fence AS writer
        ON writer.source_id = lease.source_id
     WHERE lease.source_id = $1
       AND lease.source_epoch = $2
       AND lease.fence_token = $3::bigint
       AND lease.lease_phase = 'active'
       AND lease.expires_at > clock_timestamp()
       AND writer.source_epoch = lease.source_epoch
       AND writer.fence_token = lease.fence_token
     FOR UPDATE OF lease, writer
 ),
 locked_head AS MATERIALIZED (
    SELECT head.source_epoch, head.head_sequence
      FROM calque_control.canonical_log_head AS head
      CROSS JOIN authorized
     WHERE head.source_id = $1
     FOR UPDATE OF head
 ),
 locked_enrollment AS MATERIALIZED (
    SELECT enrollment.source_id
      FROM calque_control.source_slot_enrollment AS enrollment
      CROSS JOIN authorized
     WHERE $7::text IS NOT NULL
       AND enrollment.source_id = $1
       AND enrollment.source_epoch = $2
     FOR UPDATE OF enrollment
 ),
 input AS MATERIALIZED (
    SELECT item.ordinal, item.transaction_identity, item.entry_bytes, item.entry_sha256,
           item.end_lsn
      FROM unnest($4::text[], $5::bytea[], $6::text[], $8::pg_lsn[])
           WITH ORDINALITY AS item (transaction_identity, entry_bytes, entry_sha256, end_lsn, ordinal)
 ),
 compaction AS MATERIALIZED (
    SELECT retention.retired_compacted_through_lsn AS through_lsn
      FROM calque_control.canonical_log_retention AS retention
      CROSS JOIN authorized
     WHERE retention.source_id = $1
       AND retention.source_epoch = $2
 ),
 existing AS MATERIALIZED (
    SELECT input.ordinal, record.sequence, record.entry_bytes
      FROM input
      JOIN calque_control.canonical_log_record AS record
        ON record.source_id = $1 AND record.source_epoch = $2
       AND record.transaction_identity = input.transaction_identity
      CROSS JOIN authorized
 ),
 retired AS MATERIALIZED (
    SELECT input.ordinal, record.original_sequence, record.entry_sha256
      FROM input
      JOIN calque_control.canonical_log_retired_record AS record
        ON record.source_id = $1 AND record.source_epoch = $2
       AND record.transaction_identity = input.transaction_identity
      CROSS JOIN authorized
 ),
 entry_status AS MATERIALIZED (
    SELECT input.ordinal,
           CASE
               WHEN existing.ordinal IS NOT NULL
                AND (existing.sequence <= 0 OR existing.entry_bytes <> input.entry_bytes)
                 THEN 'duplicate_conflict'
               WHEN existing.ordinal IS NOT NULL THEN 'already'
               WHEN retired.ordinal IS NOT NULL
                AND (retired.original_sequence <= 0
                     OR retired.entry_sha256 <> input.entry_sha256)
                 THEN 'duplicate_conflict'
               WHEN retired.ordinal IS NOT NULL THEN 'retired'
               WHEN input.end_lsn <= (SELECT through_lsn FROM compaction) THEN 'compacted'
               ELSE 'new'
           END AS status,
           COALESCE(existing.sequence, retired.original_sequence) AS present_sequence
      FROM input
      LEFT JOIN existing ON existing.ordinal = input.ordinal
      LEFT JOIN retired ON retired.ordinal = input.ordinal
 ),
 numbered AS MATERIALIZED (
    SELECT entry_status.ordinal, entry_status.status,
           CASE
               WHEN entry_status.status <> 'new' THEN entry_status.present_sequence
               WHEN COALESCE((SELECT head_sequence FROM locked_head), 0)
                    > 9223372036854775807
                      - count(*) FILTER (WHERE entry_status.status = 'new')
                        OVER (ORDER BY entry_status.ordinal)
                 THEN NULL
               ELSE COALESCE((SELECT head_sequence FROM locked_head), 0)
                    + count(*) FILTER (WHERE entry_status.status = 'new')
                      OVER (ORDER BY entry_status.ordinal)
           END AS sequence
      FROM entry_status
 ),
 classified AS MATERIALIZED (
    SELECT CASE
        WHEN NOT EXISTS (SELECT 1 FROM authorized) THEN 'fenced'
        WHEN EXISTS (SELECT 1 FROM locked_head)
         AND ((SELECT source_epoch FROM locked_head) <> $2
          OR (SELECT head_sequence FROM locked_head) < 0) THEN 'corrupt'
        WHEN $7::text IS NOT NULL
         AND NOT EXISTS (SELECT 1 FROM locked_enrollment) THEN 'checkpoint_missing'
        WHEN EXISTS (SELECT 1 FROM numbered WHERE status = 'compacted') THEN 'compacted'
        WHEN EXISTS (SELECT 1 FROM numbered WHERE status = 'duplicate_conflict')
          THEN 'duplicate_conflict'
        WHEN EXISTS (SELECT 1 FROM numbered WHERE status = 'new' AND sequence IS NULL)
          THEN 'exhausted'
        ELSE 'ok'
    END AS status,
    (SELECT count(*) FROM numbered WHERE status = 'new') AS appended,
    (SELECT max(sequence) FROM numbered WHERE status = 'new') AS head_sequence
 ),
 published_head AS (
    INSERT INTO calque_control.canonical_log_head
        (source_id, source_epoch, head_sequence)
    SELECT $1, $2, classified.head_sequence
      FROM classified
     WHERE classified.status = 'ok'
       AND classified.appended > 0
       AND ($7::text IS NULL OR EXISTS (SELECT 1 FROM locked_enrollment))
    ON CONFLICT (source_id) DO UPDATE
       SET head_sequence = EXCLUDED.head_sequence
     WHERE calque_control.canonical_log_head.source_epoch = EXCLUDED.source_epoch
       AND calque_control.canonical_log_head.head_sequence
           = EXCLUDED.head_sequence - (SELECT appended FROM classified)
    RETURNING head_sequence
 ),
 inserted AS (
    INSERT INTO calque_control.canonical_log_record (
        source_id, source_epoch, sequence, transaction_identity,
        entry_bytes, entry_sha256
    )
    SELECT $1, $2, numbered.sequence, input.transaction_identity,
           input.entry_bytes, input.entry_sha256
      FROM numbered
      JOIN input ON input.ordinal = numbered.ordinal
     WHERE numbered.status = 'new'
       AND EXISTS (SELECT 1 FROM published_head)
    RETURNING sequence
 ),
 checkpointed AS (
    UPDATE calque_control.source_slot_enrollment AS enrollment
       SET durable_canonical_lsn = GREATEST(
           enrollment.durable_canonical_lsn::pg_lsn,
           $7::text::pg_lsn
       )::text
      FROM classified
     WHERE $7::text IS NOT NULL
       AND classified.status = 'ok'
       AND (classified.appended = 0 OR EXISTS (SELECT 1 FROM inserted))
       AND enrollment.source_id = $1
       AND enrollment.source_epoch = $2
    RETURNING enrollment.source_id
 )
 SELECT CASE
    WHEN classified.status = 'ok'
     AND (SELECT count(*) FROM inserted) <> classified.appended THEN 'corrupt'
    WHEN $7::text IS NOT NULL
     AND classified.status = 'ok'
     AND NOT EXISTS (SELECT 1 FROM checkpointed) THEN 'checkpoint_missing'
    ELSE classified.status
 END,
 ARRAY(SELECT numbered.status FROM numbered ORDER BY numbered.ordinal),
 ARRAY(SELECT numbered.sequence FROM numbered ORDER BY numbered.ordinal)
   FROM classified";

/// PostgreSQL-backed canonical append boundary for an HA source runtime.
///
/// Unlike the test-support `FileCanonicalLog`, this adapter provides
/// cross-process writer exclusion. It also owns the durable PostgreSQL
/// retention floor and snapshot-build leases; artifact-provider retention
/// remains a separate boundary.
pub struct PostgresFencedCanonicalLog {
    client: Mutex<Client>,
    append_statement: OnceLock<Statement>,
    read_statements: OnceLock<ReadStatements>,
    writer: FencedWriterIdentity,
    state_dir: PathBuf,
    observer: Arc<dyn PostgresAppendObserver>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PostgresAppendBoundary {
    BeforeCheckpoint,
    FenceAuthorized,
    SequenceAllocated,
    RecordInserted,
    HeadUpdated,
    AfterCheckpoint,
    TransactionCommitted,
}

/// Trusted synchronous instrumentation for deterministic crash-boundary proof.
pub trait PostgresAppendObserver: std::fmt::Debug + Send + Sync + 'static {
    fn observe(&self, boundary: PostgresAppendBoundary);
}

#[derive(Debug)]
struct NoopPostgresAppendObserver;

impl PostgresAppendObserver for NoopPostgresAppendObserver {
    fn observe(&self, _boundary: PostgresAppendBoundary) {}
}

impl PostgresFencedCanonicalLog {
    /// Connects `writer` to its log. `state_dir` is the state directory of
    /// the calque process the log serves: refusing a log an earlier release
    /// wrote, at connect or on a later read, names it.
    pub fn connect(
        connection_string: &str,
        writer: FencedWriterIdentity,
        state_dir: &Path,
    ) -> Result<Self, LogError> {
        Self::connect_with_observer(
            connection_string,
            writer,
            state_dir,
            Arc::new(NoopPostgresAppendObserver),
        )
    }

    pub fn connect_with_observer(
        connection_string: &str,
        writer: FencedWriterIdentity,
        state_dir: &Path,
        observer: Arc<dyn PostgresAppendObserver>,
    ) -> Result<Self, LogError> {
        let mut client = connect(connection_string)?;
        if PostgresDatastoreIdentity::inspect(&mut client)? != writer.datastore {
            return Err(LogError::DatastoreIdentityMismatch);
        }
        client
            .batch_execute(&format!(
                "SET statement_timeout = {};
                 SET lock_timeout = '1s';
                 SET synchronous_commit = on;",
                LOG_STATEMENT_TIMEOUT.as_millis()
            ))
            .map_err(postgres_log_error)?;
        Self::install_schema(&mut client)?;
        let log = Self {
            client: Mutex::new(client),
            append_statement: OnceLock::new(),
            read_statements: OnceLock::new(),
            writer,
            state_dir: state_dir.to_path_buf(),
            observer,
        };
        log.refuse_earlier_entry_format()?;
        Ok(log)
    }

    /// Installs the control-schema tables the log keeps, under
    /// [`CONTROL_SCHEMA_BOOTSTRAP_LOCK_KEY`], without opening a writer or
    /// reading any source's log. The session's own settings bound the wait.
    pub fn install_schema(client: &mut Client) -> Result<(), LogError> {
        let mut transaction = client.transaction().map_err(postgres_log_error)?;
        // PostgreSQL reports each object `IF NOT EXISTS` skips as a NOTICE,
        // which the driver logs at INFO on every start.
        transaction
            .batch_execute(
                "SET LOCAL synchronous_commit = on; SET LOCAL client_min_messages = warning",
            )
            .map_err(postgres_log_error)?;
        transaction
            .query_one(
                "SELECT pg_advisory_xact_lock($1)",
                &[&CONTROL_SCHEMA_BOOTSTRAP_LOCK_KEY],
            )
            .map_err(postgres_log_error)?;
        transaction
            .batch_execute(POSTGRES_LOG_INSTALL_SQL)
            .map_err(postgres_log_error)?;
        transaction.commit().map_err(postgres_log_error)
    }

    /// Refuses a log an earlier release of calque wrote.
    ///
    /// Connecting refuses such a log before anything appends to it, so one
    /// release wrote every entry in a log, and the newest entry decides.
    fn refuse_earlier_entry_format(&self) -> Result<(), LogError> {
        let mut client = self
            .client
            .lock()
            .map_err(|_| LogError::PostgresUnavailable)?;
        let Some(row) = client
            .query_opt(
                "SELECT entry_bytes
                   FROM calque_control.canonical_log_record
                  WHERE source_id = $1 AND source_epoch = $2
                  ORDER BY sequence DESC
                  LIMIT 1",
                &[
                    &self.writer.source_id.as_str(),
                    &self.writer.source_epoch.as_str(),
                ],
            )
            .map_err(postgres_log_error)?
        else {
            return Ok(());
        };
        decode_stored_entry(row.get(0), &self.state_dir).map(|_| ())
    }

    pub fn writer(&self) -> &FencedWriterIdentity {
        &self.writer
    }

    pub fn source_id(&self) -> &SourceId {
        self.writer.source_id()
    }

    pub fn source_epoch(&self) -> &SourceEpoch {
        self.writer.source_epoch()
    }

    fn log_source(&self) -> PostgresLogSource<'_> {
        PostgresLogSource {
            source_id: &self.writer.source_id,
            source_epoch: &self.writer.source_epoch,
            state_dir: &self.state_dir,
            statements: &self.read_statements,
        }
    }

    pub fn head(&self) -> Result<CursorPosition, LogError> {
        let mut client = self
            .client
            .lock()
            .map_err(|_| LogError::PostgresUnavailable)?;
        self.log_source().head(&mut client)
    }

    /// Proves that this process still owns the current active writer fence and
    /// can commit through the canonical-log datastore without advancing the
    /// canonical cursor.
    ///
    /// Stronger than a read-only `SELECT 1`, so readiness closes when the lease
    /// is stale or the record and head tables are not writable. The
    /// self-assignment preserves the head and still requires PostgreSQL to
    /// authorize and commit a write transaction.
    pub fn probe_writable_current_fence(&self) -> Result<(), LogError> {
        let mut client = self
            .client
            .lock()
            .map_err(|_| LogError::PostgresUnavailable)?;
        let mut transaction = client.transaction().map_err(postgres_log_error)?;
        transaction
            .batch_execute("SET LOCAL synchronous_commit = on")
            .map_err(postgres_log_error)?;
        let authorized = transaction
            .query_opt(
                "SELECT lease.fence_token
                   FROM calque_control.source_fence_lease AS lease
                   JOIN calque_control.canonical_log_writer_fence AS writer
                     ON writer.source_id = lease.source_id
                  WHERE lease.source_id = $1
                    AND lease.source_epoch = $2
                    AND lease.fence_token = $3::bigint
                    AND lease.lease_phase = 'active'
                    AND lease.expires_at > clock_timestamp()
                    AND writer.source_epoch = lease.source_epoch
                    AND writer.fence_token = lease.fence_token
                  FOR UPDATE OF lease, writer",
                &[
                    &self.writer.source_id.as_str(),
                    &self.writer.source_epoch.as_str(),
                    &self.writer.fence_token,
                ],
            )
            .map_err(postgres_log_error)?
            .is_some();
        if !authorized {
            return Err(LogError::FencedWriterRejected);
        }
        let head_rows = transaction
            .execute(
                "INSERT INTO calque_control.canonical_log_head
                    (source_id, source_epoch, head_sequence)
                 VALUES ($1, $2, 0)
                 ON CONFLICT (source_id) DO UPDATE
                   SET head_sequence = calque_control.canonical_log_head.head_sequence
                 WHERE calque_control.canonical_log_head.source_epoch = EXCLUDED.source_epoch",
                &[
                    &self.writer.source_id.as_str(),
                    &self.writer.source_epoch.as_str(),
                ],
            )
            .map_err(postgres_log_error)?;
        if head_rows != 1 {
            return Err(LogError::PostgresHeadCorrupt);
        }
        transaction
            .batch_execute("LOCK TABLE calque_control.canonical_log_record IN ROW EXCLUSIVE MODE")
            .map_err(postgres_log_error)?;
        transaction.commit().map_err(postgres_log_error)
    }

    pub fn minimum_retained(&self) -> Result<CursorPosition, LogError> {
        let mut client = self
            .client
            .lock()
            .map_err(|_| LogError::PostgresUnavailable)?;
        self.log_source().minimum_retained(&mut client)
    }

    pub fn bounds(&self) -> Result<LogBounds, LogError> {
        let mut client = self
            .client
            .lock()
            .map_err(|_| LogError::PostgresUnavailable)?;
        self.log_source().bounds(&mut client)
    }

    fn acquire_plan_retention_lease_inner(
        &self,
        plan_id: PlanId,
        minimum_sequence: u64,
        expires_ms: i64,
    ) -> Result<(), LogError> {
        let minimum =
            i64::try_from(minimum_sequence).map_err(|_| LogError::InvalidRetentionLease)?;
        if minimum <= 0 || expires_ms <= 0 {
            return Err(LogError::InvalidRetentionLease);
        }
        let mut client = self
            .client
            .lock()
            .map_err(|_| LogError::PostgresUnavailable)?;
        let mut transaction = client.transaction().map_err(postgres_log_error)?;
        transaction
            .batch_execute("SET LOCAL synchronous_commit = on")
            .map_err(postgres_log_error)?;
        ensure_postgres_retention_state(&mut transaction, &self.writer)?;
        let retention = transaction
            .query_one(
                "SELECT source_epoch, minimum_retained_sequence,
                        floor(extract(epoch FROM clock_timestamp()) * 1000)::bigint
                   FROM calque_control.canonical_log_retention
                  WHERE source_id = $1
                  FOR UPDATE",
                &[&self.writer.source_id.as_str()],
            )
            .map_err(postgres_log_error)?;
        let stored_epoch: String = retention.get(0);
        let stored_minimum: i64 = retention.get(1);
        let database_now_ms: i64 = retention.get(2);
        if stored_epoch != self.writer.source_epoch.as_str()
            || stored_minimum <= 0
            || expires_ms <= database_now_ms
        {
            return Err(LogError::InvalidRetentionLease);
        }
        let head = transaction
            .query_opt(
                "SELECT source_epoch, head_sequence
                   FROM calque_control.canonical_log_head
                  WHERE source_id = $1
                  FOR UPDATE",
                &[&self.writer.source_id.as_str()],
            )
            .map_err(postgres_log_error)?;
        let head_sequence = match head {
            None => 0_i64,
            Some(row) => {
                let source_epoch: String = row.get(0);
                let sequence: i64 = row.get(1);
                if source_epoch != self.writer.source_epoch.as_str() || sequence < 0 {
                    return Err(LogError::PostgresHeadCorrupt);
                }
                sequence
            }
        };
        if minimum < stored_minimum
            || minimum
                > head_sequence
                    .checked_add(1)
                    .ok_or(LogError::SequenceExhausted)?
        {
            return Err(LogError::InvalidRetentionLease);
        }
        transaction
            .execute(
                "DELETE FROM calque_control.canonical_log_plan_retention_lease
                  WHERE source_id = $1 AND source_epoch = $2
                    AND expires_at <= clock_timestamp()",
                &[
                    &self.writer.source_id.as_str(),
                    &self.writer.source_epoch.as_str(),
                ],
            )
            .map_err(postgres_log_error)?;
        if let Some(existing) = transaction
            .query_opt(
                "SELECT minimum_sequence,
                        floor(extract(epoch FROM expires_at) * 1000)::bigint
                   FROM calque_control.canonical_log_plan_retention_lease
                  WHERE source_id = $1 AND source_epoch = $2 AND plan_id = $3
                  FOR UPDATE",
                &[
                    &self.writer.source_id.as_str(),
                    &self.writer.source_epoch.as_str(),
                    &plan_id.as_str(),
                ],
            )
            .map_err(postgres_log_error)?
        {
            let existing_minimum: i64 = existing.get(0);
            let existing_expires_ms: i64 = existing.get(1);
            if existing_minimum != minimum || existing_expires_ms != expires_ms {
                return Err(LogError::RetentionLeaseConflict);
            }
            transaction.commit().map_err(postgres_log_error)?;
            return Ok(());
        }
        transaction
            .execute(
                "INSERT INTO calque_control.canonical_log_plan_retention_lease
                    (source_id, source_epoch, plan_id, minimum_sequence, expires_at)
                 VALUES ($1, $2, $3, $4::bigint,
                         TIMESTAMPTZ 'epoch' + ($5::bigint * interval '1 millisecond'))",
                &[
                    &self.writer.source_id.as_str(),
                    &self.writer.source_epoch.as_str(),
                    &plan_id.as_str(),
                    &minimum,
                    &expires_ms,
                ],
            )
            .map_err(postgres_log_error)?;
        transaction.commit().map_err(postgres_log_error)
    }

    /// Advances the durable PostgreSQL retention floor and removes obsolete
    /// active records in one synchronous transaction. Retired identities keep
    /// duplicate replay idempotent after the active bytes are removed.
    pub fn retain_from(&mut self, minimum_sequence: u64) -> Result<(), LogError> {
        let mut client = self
            .client
            .lock()
            .map_err(|_| LogError::PostgresUnavailable)?;
        let mut transaction = client.transaction().map_err(postgres_log_error)?;
        transaction
            .batch_execute("SET LOCAL synchronous_commit = on")
            .map_err(postgres_log_error)?;
        // Lock the fence rows first, in the order an append locks them.
        let compaction = compaction_floor(&mut transaction, &self.writer)?;
        ensure_postgres_retention_state(&mut transaction, &self.writer)?;
        let state = transaction
            .query_one(
                "SELECT source_epoch, minimum_retained_sequence
                   FROM calque_control.canonical_log_retention
                  WHERE source_id = $1
                  FOR UPDATE",
                &[&self.writer.source_id.as_str()],
            )
            .map_err(postgres_log_error)?;
        let stored_epoch: String = state.get(0);
        let stored_minimum: i64 = state.get(1);
        if stored_epoch != self.writer.source_epoch.as_str() || stored_minimum <= 0 {
            return Err(LogError::PostgresHeadCorrupt);
        }
        let head = transaction
            .query_opt(
                "SELECT source_epoch, head_sequence
                   FROM calque_control.canonical_log_head
                  WHERE source_id = $1
                  FOR UPDATE",
                &[&self.writer.source_id.as_str()],
            )
            .map_err(postgres_log_error)?;
        let head_sequence = match head {
            None => 0_i64,
            Some(row) => {
                let source_epoch: String = row.get(0);
                let sequence: i64 = row.get(1);
                if source_epoch != self.writer.source_epoch.as_str() || sequence < 0 {
                    return Err(LogError::PostgresHeadCorrupt);
                }
                sequence
            }
        };
        transaction
            .execute(
                "DELETE FROM calque_control.canonical_log_plan_retention_lease
                  WHERE source_id = $1 AND source_epoch = $2
                    AND expires_at <= clock_timestamp()",
                &[
                    &self.writer.source_id.as_str(),
                    &self.writer.source_epoch.as_str(),
                ],
            )
            .map_err(postgres_log_error)?;
        transaction
            .execute(
                "DELETE FROM calque_control.canonical_log_build_retention_lease
                  WHERE source_id = $1 AND source_epoch = $2
                    AND expires_at <= clock_timestamp()",
                &[
                    &self.writer.source_id.as_str(),
                    &self.writer.source_epoch.as_str(),
                ],
            )
            .map_err(postgres_log_error)?;
        let lease_floor = transaction
            .query_one(
                "SELECT min(minimum_sequence)
                   FROM (
                       SELECT minimum_sequence
                         FROM calque_control.canonical_log_plan_retention_lease
                        WHERE source_id = $1 AND source_epoch = $2
                          AND expires_at > clock_timestamp()
                       UNION ALL
                       SELECT minimum_sequence
                         FROM calque_control.canonical_log_build_retention_lease
                        WHERE source_id = $1 AND source_epoch = $2
                          AND expires_at > clock_timestamp()
                   ) AS active_retention_floor",
                &[
                    &self.writer.source_id.as_str(),
                    &self.writer.source_epoch.as_str(),
                ],
            )
            .map_err(postgres_log_error)?
            .get::<_, Option<i64>>(0);
        let requested = i64::try_from(minimum_sequence).unwrap_or(i64::MAX);
        let mut retained = requested.max(stored_minimum).min(
            head_sequence
                .checked_add(1)
                .ok_or(LogError::SequenceExhausted)?,
        );
        if let Some(lease_floor) = lease_floor {
            if lease_floor <= 0 {
                return Err(LogError::PostgresHeadCorrupt);
            }
            retained = retained.min(lease_floor);
        }
        // A trimmed entry PostgreSQL can still send again keeps its identity;
        // one that ends at or before the compaction floor leaves none.
        transaction
            .execute(
                "INSERT INTO calque_control.canonical_log_retired_record
                    (source_id, source_epoch, original_sequence,
                     transaction_identity, entry_sha256)
                 SELECT source_id, source_epoch, sequence,
                        transaction_identity, entry_sha256
                   FROM calque_control.canonical_log_record
                  WHERE source_id = $1 AND source_epoch = $2
                    AND sequence < $3::bigint
                    AND ($4::pg_lsn IS NULL OR $4::pg_lsn < (transaction_identity::jsonb ->> 'endLsn')::pg_lsn)",
                &[
                    &self.writer.source_id.as_str(),
                    &self.writer.source_epoch.as_str(),
                    &retained,
                    &compaction,
                ],
            )
            .map_err(postgres_log_error)?;
        if let Some(floor) = compaction {
            compact_retired_records(&mut transaction, &self.writer, floor)?;
        }
        transaction
            .execute(
                "DELETE FROM calque_control.canonical_log_record
                  WHERE source_id = $1 AND source_epoch = $2
                    AND sequence < $3::bigint",
                &[
                    &self.writer.source_id.as_str(),
                    &self.writer.source_epoch.as_str(),
                    &retained,
                ],
            )
            .map_err(postgres_log_error)?;
        let updated = transaction
            .execute(
                "UPDATE calque_control.canonical_log_retention
                    SET minimum_retained_sequence = $2::bigint
                  WHERE source_id = $1 AND source_epoch = $3",
                &[
                    &self.writer.source_id.as_str(),
                    &retained,
                    &self.writer.source_epoch.as_str(),
                ],
            )
            .map_err(postgres_log_error)?;
        if updated != 1 {
            return Err(LogError::PostgresHeadCorrupt);
        }
        transaction.commit().map_err(postgres_log_error)
    }

    /// The stored bytes of the entries after `after_sequence` through
    /// `through_sequence`.
    pub fn entry_bytes_between(
        &self,
        after_sequence: u64,
        through_sequence: u64,
    ) -> Result<u64, LogError> {
        let after = i64::try_from(after_sequence).map_err(|_| LogError::SequenceExhausted)?;
        let through = i64::try_from(through_sequence).map_err(|_| LogError::SequenceExhausted)?;
        let mut client = self
            .client
            .lock()
            .map_err(|_| LogError::PostgresUnavailable)?;
        let bytes: i64 = client
            .query_one(
                "SELECT coalesce(sum(octet_length(entry_bytes)), 0)::bigint
                   FROM calque_control.canonical_log_record
                  WHERE source_id = $1 AND source_epoch = $2
                    AND sequence > $3::bigint AND sequence <= $4::bigint",
                &[
                    &self.writer.source_id.as_str(),
                    &self.writer.source_epoch.as_str(),
                    &after,
                    &through,
                ],
            )
            .map_err(postgres_log_error)?
            .get(0);
        u64::try_from(bytes).map_err(|_| LogError::PostgresHeadCorrupt)
    }

    fn acquire_snapshot_build_retention_inner(
        &self,
        build_id: SnapshotBuildId,
        owner_id: SnapshotBuildOwnerId,
        ttl: Duration,
    ) -> Result<SnapshotBuildRetentionLease, LogError> {
        let ttl_ms = snapshot_build_lease_ttl_millis(ttl)?;
        let mut client = self
            .client
            .lock()
            .map_err(|_| LogError::PostgresUnavailable)?;
        let mut transaction = client.transaction().map_err(postgres_log_error)?;
        transaction
            .batch_execute("SET LOCAL synchronous_commit = on")
            .map_err(postgres_log_error)?;
        ensure_postgres_retention_state(&mut transaction, &self.writer)?;
        let state = transaction
            .query_one(
                "SELECT source_epoch, minimum_retained_sequence,
                        next_build_lease_token
                   FROM calque_control.canonical_log_retention
                  WHERE source_id = $1
                  FOR UPDATE",
                &[&self.writer.source_id.as_str()],
            )
            .map_err(postgres_log_error)?;
        let stored_epoch: String = state.get(0);
        let retained_floor: i64 = state.get(1);
        let next_token: i64 = state.get(2);
        if stored_epoch != self.writer.source_epoch.as_str()
            || retained_floor <= 0
            || next_token < 0
        {
            return Err(LogError::PostgresHeadCorrupt);
        }
        if let Some(existing) = transaction
            .query_opt(
                "SELECT owner_id, lease_token, minimum_sequence,
                        floor(extract(epoch FROM expires_at) * 1000)::bigint,
                        expires_at > clock_timestamp()
                   FROM calque_control.canonical_log_build_retention_lease
                  WHERE source_id = $1 AND source_epoch = $2 AND build_id = $3
                  FOR UPDATE",
                &[
                    &self.writer.source_id.as_str(),
                    &self.writer.source_epoch.as_str(),
                    &build_id.as_str(),
                ],
            )
            .map_err(postgres_log_error)?
        {
            let stored_owner: String = existing.get(0);
            let token: i64 = existing.get(1);
            let minimum: i64 = existing.get(2);
            let _expires_at: i64 = existing.get(3);
            let active: bool = existing.get(4);
            if active {
                if stored_owner != owner_id.as_str() || token <= 0 || minimum <= 0 {
                    return Err(LogError::SnapshotBuildRetentionLeaseConflict);
                }
                let replacement_token = next_token
                    .checked_add(1)
                    .filter(|token| *token > 0)
                    .ok_or(LogError::SnapshotBuildRetentionTokenExhausted)?;
                let replacement = transaction
                    .query_one(
                        "UPDATE calque_control.canonical_log_build_retention_lease
                            SET lease_token = $4::bigint,
                                expires_at = clock_timestamp()
                                    + ($5::bigint * interval '1 millisecond')
                          WHERE source_id = $1 AND source_epoch = $2 AND build_id = $3
                          RETURNING floor(extract(epoch FROM expires_at) * 1000)::bigint",
                        &[
                            &self.writer.source_id.as_str(),
                            &self.writer.source_epoch.as_str(),
                            &build_id.as_str(),
                            &replacement_token,
                            &ttl_ms,
                        ],
                    )
                    .map_err(postgres_log_error)?;
                let updated = transaction
                    .execute(
                        "UPDATE calque_control.canonical_log_retention
                            SET next_build_lease_token = $2::bigint
                          WHERE source_id = $1 AND source_epoch = $3
                            AND next_build_lease_token = $2::bigint - 1",
                        &[
                            &self.writer.source_id.as_str(),
                            &replacement_token,
                            &self.writer.source_epoch.as_str(),
                        ],
                    )
                    .map_err(postgres_log_error)?;
                if updated != 1 {
                    return Err(LogError::PostgresHeadCorrupt);
                }
                transaction.commit().map_err(postgres_log_error)?;
                return Ok(SnapshotBuildRetentionLease {
                    source_id: self.writer.source_id.clone(),
                    source_epoch: self.writer.source_epoch.clone(),
                    build_id,
                    owner_id,
                    token: replacement_token,
                    minimum_sequence: u64::try_from(minimum)
                        .map_err(|_| LogError::PostgresHeadCorrupt)?,
                    expires_at_unix_millis: replacement.get(0),
                });
            }
            transaction
                .execute(
                    "DELETE FROM calque_control.canonical_log_build_retention_lease
                      WHERE source_id = $1 AND source_epoch = $2 AND build_id = $3",
                    &[
                        &self.writer.source_id.as_str(),
                        &self.writer.source_epoch.as_str(),
                        &build_id.as_str(),
                    ],
                )
                .map_err(postgres_log_error)?;
        }
        let head = transaction
            .query_opt(
                "SELECT source_epoch, head_sequence
                   FROM calque_control.canonical_log_head
                  WHERE source_id = $1
                  FOR SHARE",
                &[&self.writer.source_id.as_str()],
            )
            .map_err(postgres_log_error)?;
        let head_sequence = match head {
            None => 0_i64,
            Some(row) => {
                let source_epoch: String = row.get(0);
                let sequence: i64 = row.get(1);
                if source_epoch != self.writer.source_epoch.as_str() || sequence < 0 {
                    return Err(LogError::PostgresHeadCorrupt);
                }
                sequence
            }
        };
        let minimum_sequence = head_sequence
            .checked_add(1)
            .filter(|minimum| *minimum >= retained_floor)
            .ok_or(LogError::InvalidSnapshotBuildRetentionLease)?;
        let token = next_token
            .checked_add(1)
            .filter(|token| *token > 0)
            .ok_or(LogError::SnapshotBuildRetentionTokenExhausted)?;
        let row = transaction
            .query_one(
                "INSERT INTO calque_control.canonical_log_build_retention_lease
                    (source_id, source_epoch, build_id, owner_id, lease_token,
                     minimum_sequence, expires_at)
                 VALUES ($1, $2, $3, $4, $5::bigint, $6::bigint,
                         clock_timestamp() + ($7::bigint * interval '1 millisecond'))
                 RETURNING floor(extract(epoch FROM expires_at) * 1000)::bigint",
                &[
                    &self.writer.source_id.as_str(),
                    &self.writer.source_epoch.as_str(),
                    &build_id.as_str(),
                    &owner_id.as_str(),
                    &token,
                    &minimum_sequence,
                    &ttl_ms,
                ],
            )
            .map_err(postgres_log_error)?;
        let expires_at_unix_millis: i64 = row.get(0);
        let updated = transaction
            .execute(
                "UPDATE calque_control.canonical_log_retention
                    SET next_build_lease_token = $2::bigint
                  WHERE source_id = $1 AND source_epoch = $3
                    AND next_build_lease_token = $2::bigint - 1",
                &[
                    &self.writer.source_id.as_str(),
                    &token,
                    &self.writer.source_epoch.as_str(),
                ],
            )
            .map_err(postgres_log_error)?;
        if updated != 1 {
            return Err(LogError::PostgresHeadCorrupt);
        }
        transaction.commit().map_err(postgres_log_error)?;
        Ok(SnapshotBuildRetentionLease {
            source_id: self.writer.source_id.clone(),
            source_epoch: self.writer.source_epoch.clone(),
            build_id,
            owner_id,
            token,
            minimum_sequence: u64::try_from(minimum_sequence)
                .map_err(|_| LogError::PostgresHeadCorrupt)?,
            expires_at_unix_millis,
        })
    }

    fn renew_snapshot_build_retention_inner(
        &self,
        lease: &SnapshotBuildRetentionLease,
        ttl: Duration,
    ) -> Result<SnapshotBuildRetentionLease, LogError> {
        self.validate_snapshot_build_lease_identity(lease)?;
        let ttl_ms = snapshot_build_lease_ttl_millis(ttl)?;
        let mut client = self
            .client
            .lock()
            .map_err(|_| LogError::PostgresUnavailable)?;
        let mut transaction = client.transaction().map_err(postgres_log_error)?;
        transaction
            .batch_execute("SET LOCAL synchronous_commit = on")
            .map_err(postgres_log_error)?;
        let row = transaction
            .query_opt(
                "UPDATE calque_control.canonical_log_build_retention_lease
                    SET expires_at = clock_timestamp()
                                   + ($7::bigint * interval '1 millisecond')
                  WHERE source_id = $1 AND source_epoch = $2 AND build_id = $3
                    AND owner_id = $4 AND lease_token = $5::bigint
                    AND minimum_sequence = $6::bigint
                    AND expires_at > clock_timestamp()
                 RETURNING floor(extract(epoch FROM expires_at) * 1000)::bigint",
                &[
                    &lease.source_id.as_str(),
                    &lease.source_epoch.as_str(),
                    &lease.build_id.as_str(),
                    &lease.owner_id.as_str(),
                    &lease.token,
                    &i64::try_from(lease.minimum_sequence)
                        .map_err(|_| LogError::SnapshotBuildRetentionLeaseRejected)?,
                    &ttl_ms,
                ],
            )
            .map_err(postgres_log_error)?;
        let Some(row) = row else {
            return Err(LogError::SnapshotBuildRetentionLeaseRejected);
        };
        let expires_at_unix_millis: i64 = row.get(0);
        transaction.commit().map_err(postgres_log_error)?;
        let mut renewed = lease.clone();
        renewed.expires_at_unix_millis = expires_at_unix_millis;
        Ok(renewed)
    }

    fn release_snapshot_build_retention_inner(
        &self,
        lease: &SnapshotBuildRetentionLease,
    ) -> Result<(), LogError> {
        self.validate_snapshot_build_lease_identity(lease)?;
        let mut client = self
            .client
            .lock()
            .map_err(|_| LogError::PostgresUnavailable)?;
        let mut transaction = client.transaction().map_err(postgres_log_error)?;
        transaction
            .batch_execute("SET LOCAL synchronous_commit = on")
            .map_err(postgres_log_error)?;
        let deleted = transaction
            .execute(
                "DELETE FROM calque_control.canonical_log_build_retention_lease
                  WHERE source_id = $1 AND source_epoch = $2 AND build_id = $3
                    AND owner_id = $4 AND lease_token = $5::bigint
                    AND minimum_sequence = $6::bigint
                    AND expires_at > clock_timestamp()",
                &[
                    &lease.source_id.as_str(),
                    &lease.source_epoch.as_str(),
                    &lease.build_id.as_str(),
                    &lease.owner_id.as_str(),
                    &lease.token,
                    &i64::try_from(lease.minimum_sequence)
                        .map_err(|_| LogError::SnapshotBuildRetentionLeaseRejected)?,
                ],
            )
            .map_err(postgres_log_error)?;
        if deleted != 1 {
            return Err(LogError::SnapshotBuildRetentionLeaseRejected);
        }
        transaction.commit().map_err(postgres_log_error)
    }

    fn validate_snapshot_build_lease_identity(
        &self,
        lease: &SnapshotBuildRetentionLease,
    ) -> Result<(), LogError> {
        if lease.source_id != self.writer.source_id
            || lease.source_epoch != self.writer.source_epoch
            || lease.token <= 0
            || lease.minimum_sequence == 0
        {
            return Err(LogError::SnapshotBuildRetentionLeaseRejected);
        }
        Ok(())
    }

    /// Reads a bounded, contiguous page from the PostgreSQL canonical store.
    ///
    /// This is the independent convergence/readback boundary used by source
    /// takeover proof. Deployment consumers read through [`CanonicalLogRead`];
    /// this method does not weaken writer fencing.
    pub fn read_after_bounded(
        &self,
        from_sequence_exclusive: u64,
        through_sequence_inclusive: Option<u64>,
        max_records: usize,
        max_bytes: u64,
    ) -> Result<Vec<CanonicalRecord>, LogError> {
        Ok(self
            .read_page_after_bounded(
                from_sequence_exclusive,
                through_sequence_inclusive,
                max_records,
                max_bytes,
            )?
            .records)
    }

    /// Reads a bounded page and reports the exact checksummed PostgreSQL entry
    /// bytes charged against the caller's cross-page scan budget.
    pub fn read_page_after_bounded(
        &self,
        from_sequence_exclusive: u64,
        through_sequence_inclusive: Option<u64>,
        max_records: usize,
        max_bytes: u64,
    ) -> Result<CanonicalPage, LogError> {
        let mut client = self
            .client
            .lock()
            .map_err(|_| LogError::PostgresUnavailable)?;
        self.log_source().read_page_after_bounded(
            &mut client,
            from_sequence_exclusive,
            through_sequence_inclusive,
            max_records,
            max_bytes,
        )
    }

    /// The bounds and the first page from one statement; see
    /// [`CanonicalLogRead::read_bounds_and_page_after_bounded`].
    pub fn read_bounds_and_page_after_bounded(
        &self,
        from_sequence_exclusive: u64,
        through_sequence_inclusive: Option<u64>,
        max_records: usize,
        max_bytes: u64,
    ) -> Result<(LogBounds, Result<CanonicalPage, LogError>), LogError> {
        let mut client = self
            .client
            .lock()
            .map_err(|_| LogError::PostgresUnavailable)?;
        self.log_source().read_bounds_and_page_after_bounded(
            &mut client,
            from_sequence_exclusive,
            through_sequence_inclusive,
            max_records,
            max_bytes,
        )
    }

    pub fn append(&self, transaction: CanonicalTransaction) -> Result<AppendOutcome, LogError> {
        self.append_one(CanonicalEntry::Transaction(transaction), None)
    }

    pub fn append_barrier(&self, barrier: CanonicalBarrier) -> Result<AppendOutcome, LogError> {
        self.append_one(CanonicalEntry::Barrier(barrier), None)
    }

    /// Atomically appends or deduplicates one canonical entry and advances the
    /// durable source checkpoint in the same PostgreSQL statement commit.
    ///
    /// The caller may permit WAL feedback only after this returns. The
    /// checkpoint remains monotonic on replay.
    pub fn append_with_checkpoint(
        &self,
        entry: CanonicalEntry,
        durable_lsn: u64,
    ) -> Result<AppendOutcome, LogError> {
        self.append_one(entry, Some(durable_lsn))
    }

    fn append_one(
        &self,
        entry: CanonicalEntry,
        durable_lsn: Option<u64>,
    ) -> Result<AppendOutcome, LogError> {
        let mut outcomes = self.append_group(vec![entry], durable_lsn)?;
        match (outcomes.pop(), outcomes.is_empty()) {
            (Some(outcome), true) => Ok(outcome),
            _ => Err(LogError::PostgresHeadCorrupt),
        }
    }

    /// Appends `entries` in order in one PostgreSQL commit and returns one
    /// outcome per entry, in the same order.
    ///
    /// New entries take consecutive sequences in the order given. An entry
    /// whose transaction key is already durable, or repeats an earlier entry's
    /// in the group, is reported present. `durable_lsn`, when given, advances
    /// the durable source checkpoint in the same commit. Every entry and the
    /// checkpoint commit or none do, and the writer fence stays locked from the
    /// check until that commit.
    pub fn append_group(
        &self,
        entries: Vec<CanonicalEntry>,
        durable_lsn: Option<u64>,
    ) -> Result<Vec<AppendOutcome>, LogError> {
        if entries.is_empty() {
            return Err(LogError::InvalidEntry("an append group holds no entries"));
        }
        let encoded = entries
            .iter()
            .map(|entry| self.encode_append(entry))
            .collect::<Result<Vec<_>, _>>()?;
        let statements = append_statements(&encoded);
        let durable_lsn =
            durable_lsn.map(|lsn| format!("{:X}/{:08X}", lsn >> 32, lsn & 0xffff_ffff));
        let mut client = self
            .client
            .lock()
            .map_err(|_| LogError::PostgresUnavailable)?;
        let statement = self.append_statement(&mut client)?;
        if durable_lsn.is_some() {
            self.observer
                .observe(PostgresAppendBoundary::BeforeCheckpoint);
        }
        let mut outcomes = Vec::with_capacity(encoded.len());
        if let [only] = statements.as_slice() {
            outcomes.extend(self.run_append_statement(
                &mut *client,
                &statement,
                &encoded[only.clone()],
                durable_lsn.as_deref(),
            )?);
        } else {
            let mut transaction = client.transaction().map_err(postgres_log_error)?;
            let last = statements.len() - 1;
            for (index, range) in statements.into_iter().enumerate() {
                let checkpoint = if index == last {
                    durable_lsn.as_deref()
                } else {
                    None
                };
                outcomes.extend(self.run_append_statement(
                    &mut transaction,
                    &statement,
                    &encoded[range],
                    checkpoint,
                )?);
            }
            transaction.commit().map_err(postgres_log_error)?;
        }
        self.observer
            .observe(PostgresAppendBoundary::FenceAuthorized);
        if outcomes
            .iter()
            .any(|outcome| matches!(outcome, AppendOutcome::Appended(_)))
        {
            self.observer
                .observe(PostgresAppendBoundary::SequenceAllocated);
            self.observer
                .observe(PostgresAppendBoundary::RecordInserted);
            self.observer.observe(PostgresAppendBoundary::HeadUpdated);
        }
        if durable_lsn.is_some() {
            self.observer
                .observe(PostgresAppendBoundary::AfterCheckpoint);
        }
        self.observer
            .observe(PostgresAppendBoundary::TransactionCommitted);
        Ok(outcomes)
    }

    fn encode_append(&self, entry: &CanonicalEntry) -> Result<EncodedAppend, LogError> {
        if entry.key().source_epoch != self.writer.source_epoch {
            return Err(LogError::EpochMismatch {
                expected: self.writer.source_epoch.clone(),
                actual: entry.key().source_epoch.clone(),
            });
        }
        let entry_bytes = entry.encode();
        validate_encoded_entry(entry, entry_bytes.len())?;
        if entry_bytes.len() as u64 > MAX_RECORD_BYTES {
            return Err(LogError::RecordTooLarge);
        }
        let end_lsn = entry
            .key()
            .end_lsn
            .parse::<PgLsn>()
            .map_err(|_| LogError::InvalidEntry("end LSN is not a PostgreSQL LSN"))?;
        Ok(EncodedAppend {
            transaction_identity: transaction_key_string(entry.key())?,
            entry_sha256: sha256_hex(&entry_bytes),
            entry_bytes,
            end_lsn,
        })
    }

    /// The append statement, prepared once on this connection.
    fn append_statement(&self, client: &mut Client) -> Result<Statement, LogError> {
        if let Some(statement) = self.append_statement.get() {
            return Ok(statement.clone());
        }
        let statement = client
            .prepare(APPEND_GROUP_SQL)
            .map_err(postgres_log_error)?;
        Ok(self.append_statement.get_or_init(|| statement).clone())
    }

    fn run_append_statement(
        &self,
        client: &mut impl GenericClient,
        statement: &Statement,
        entries: &[EncodedAppend],
        durable_lsn: Option<&str>,
    ) -> Result<Vec<AppendOutcome>, LogError> {
        let identities: Vec<&str> = entries
            .iter()
            .map(|entry| entry.transaction_identity.as_str())
            .collect();
        let bytes: Vec<&[u8]> = entries
            .iter()
            .map(|entry| entry.entry_bytes.as_slice())
            .collect();
        let checksums: Vec<&str> = entries
            .iter()
            .map(|entry| entry.entry_sha256.as_str())
            .collect();
        let end_lsns: Vec<PgLsn> = entries.iter().map(|entry| entry.end_lsn).collect();
        let row = client
            .query_one(
                statement,
                &[
                    &self.writer.source_id.as_str(),
                    &self.writer.source_epoch.as_str(),
                    &self.writer.fence_token,
                    &identities,
                    &bytes,
                    &checksums,
                    &durable_lsn,
                    &end_lsns,
                ],
            )
            .map_err(postgres_log_error)?;
        let status: String = row.get(0);
        match status.as_str() {
            "fenced" => return Err(LogError::FencedWriterRejected),
            "corrupt" | "checkpoint_missing" => return Err(LogError::PostgresHeadCorrupt),
            "duplicate_conflict" => return Err(LogError::DuplicateTransactionConflict),
            "compacted" => return Err(LogError::ReplayBehindCompaction),
            "exhausted" => return Err(LogError::SequenceExhausted),
            "ok" => {}
            _ => return Err(LogError::PostgresHeadCorrupt),
        }
        let statuses: Vec<String> = row.get(1);
        let sequences: Vec<Option<i64>> = row.get(2);
        if statuses.len() != entries.len() || sequences.len() != entries.len() {
            return Err(LogError::PostgresHeadCorrupt);
        }
        statuses
            .into_iter()
            .zip(sequences)
            .map(|(status, sequence)| {
                let position = CursorPosition {
                    source_epoch: self.writer.source_epoch.clone(),
                    sequence: sequence
                        .and_then(|sequence| u64::try_from(sequence).ok())
                        .filter(|sequence| *sequence > 0)
                        .ok_or(LogError::PostgresHeadCorrupt)?,
                };
                match status.as_str() {
                    "new" => Ok(AppendOutcome::Appended(position)),
                    "already" | "retired" => Ok(AppendOutcome::AlreadyPresent(position)),
                    _ => Err(LogError::PostgresHeadCorrupt),
                }
            })
            .collect()
    }
}

/// One canonical entry encoded for the append statement.
struct EncodedAppend {
    transaction_identity: String,
    entry_bytes: Vec<u8>,
    entry_sha256: String,
    end_lsn: PgLsn,
}

/// Splits an append group into statements. A statement ends before the entry
/// that would take it past `MAX_APPEND_STATEMENT_BYTES`, and before an entry
/// whose transaction key it already holds. Every statement holds at least one
/// entry, and the statements cover the group in order.
fn append_statements(entries: &[EncodedAppend]) -> Vec<Range<usize>> {
    let mut statements = Vec::new();
    let mut start = 0;
    let mut bytes = 0_usize;
    let mut identities = HashSet::new();
    for (index, entry) in entries.iter().enumerate() {
        let size = entry.entry_bytes.len();
        if index > start
            && (bytes.saturating_add(size) > MAX_APPEND_STATEMENT_BYTES
                || identities.contains(entry.transaction_identity.as_str()))
        {
            statements.push(start..index);
            start = index;
            bytes = 0;
            identities.clear();
        }
        bytes = bytes.saturating_add(size);
        identities.insert(entry.transaction_identity.as_str());
    }
    statements.push(start..entries.len());
    statements
}

/// The retention-operation deadline of `PostgresFencedCanonicalLog`, which the
/// test `FileCanonicalLog` declares too.
const RETENTION_OPERATION_DEADLINE: Duration = Duration::from_secs(3);

/// One source's log as a read sees it: what every statement filters on, and
/// the state directory a refused entry format names. The writer and a reader
/// run the same reads through it, each on its own connection.
struct PostgresLogSource<'a> {
    source_id: &'a SourceId,
    source_epoch: &'a SourceEpoch,
    state_dir: &'a Path,
    statements: &'a OnceLock<ReadStatements>,
}

/// The head and the retention floor from one snapshot: one row whatever the
/// log holds, with a side's columns null where that side has no row yet.
const READ_BOUNDS_SQL: &str = "SELECT head.source_epoch, head.head_sequence,
        retention.source_epoch, retention.minimum_retained_sequence
   FROM (SELECT $1::text AS source_id) AS source
   LEFT JOIN calque_control.canonical_log_head AS head
     ON head.source_id = source.source_id
   LEFT JOIN calque_control.canonical_log_retention AS retention
     ON retention.source_id = source.source_id";

/// A page from one snapshot: the bounds row of [`READ_BOUNDS_SQL`] on every
/// row, then one row per candidate record after `$3` up to the lesser of `$4`
/// and the head, at most `$5` of them, none when `$3` is before the floor.
/// Each carries its sizes, and its payload only while the running charge of
/// `$7` bytes a row plus its sizes is within `$6`, so a record past the byte
/// limit is never sent merely to be refused. With no candidate there is one
/// row, the bounds, with the record columns null.
const READ_PAGE_SQL: &str = "SELECT bounds.head_epoch, bounds.head_sequence,
        bounds.retention_epoch, bounds.minimum_retained,
        page.sequence, page.entry_length, page.checksum_length,
        CASE WHEN page.charged <= $6::bigint THEN page.entry_bytes END,
        CASE WHEN page.charged <= $6::bigint THEN page.entry_sha256 END
   FROM (SELECT head.source_epoch AS head_epoch, head.head_sequence,
                retention.source_epoch AS retention_epoch,
                retention.minimum_retained_sequence AS minimum_retained
           FROM (SELECT $1::text AS source_id) AS source
           LEFT JOIN calque_control.canonical_log_head AS head
             ON head.source_id = source.source_id
           LEFT JOIN calque_control.canonical_log_retention AS retention
             ON retention.source_id = source.source_id) AS bounds
   LEFT JOIN LATERAL (
        SELECT candidate.sequence, candidate.entry_bytes, candidate.entry_sha256,
               candidate.entry_length, candidate.checksum_length,
               sum(candidate.entry_length + candidate.checksum_length + $7::bigint)
                 OVER (ORDER BY candidate.sequence) AS charged
          FROM (SELECT record.sequence, record.entry_bytes, record.entry_sha256,
                       octet_length(record.entry_bytes)::bigint AS entry_length,
                       octet_length(record.entry_sha256)::bigint AS checksum_length
                  FROM calque_control.canonical_log_record AS record
                 WHERE record.source_id = $1 AND record.source_epoch = $2
                   AND record.sequence > $3::bigint
                   AND record.sequence <= LEAST($4::bigint, COALESCE(bounds.head_sequence, 0))
                   AND $3::bigint >= COALESCE(bounds.minimum_retained, 1) - 1
                 ORDER BY record.sequence
                 LIMIT $5::bigint) AS candidate) AS page ON true
  ORDER BY page.sequence";

/// The log's read statements, prepared once on the connection that runs them.
///
/// A query passed as a string costs a prepare round trip on every run, and a
/// change build reads on every commit for every connected browser. Each read
/// is one statement, so it sees one snapshot without a transaction around it.
struct ReadStatements {
    bounds: Statement,
    page: Statement,
}

impl ReadStatements {
    fn prepare(client: &mut Client) -> Result<Self, LogError> {
        Ok(Self {
            bounds: client
                .prepare(READ_BOUNDS_SQL)
                .map_err(postgres_log_error)?,
            page: client.prepare(READ_PAGE_SQL).map_err(postgres_log_error)?,
        })
    }
}

impl<'a> PostgresLogSource<'a> {
    /// The read statements on `client`, prepared on its first read. The
    /// caller holds `client`'s lock, so two reads never prepare them at once.
    fn statements(&self, client: &mut Client) -> Result<&'a ReadStatements, LogError> {
        if let Some(statements) = self.statements.get() {
            return Ok(statements);
        }
        let statements = ReadStatements::prepare(client)?;
        Ok(self.statements.get_or_init(|| statements))
    }

    fn bounds_row(&self, client: &mut Client) -> Result<Row, LogError> {
        let statement = &self.statements(client)?.bounds;
        client
            .query_one(statement, &[&self.source_id.as_str()])
            .map_err(postgres_log_error)
    }

    /// The head from a bounds row's first two columns: sequence 0 before the
    /// log has one.
    fn head_from(&self, row: &Row) -> Result<CursorPosition, LogError> {
        let sequence = match (
            row.get::<_, Option<String>>(0),
            row.get::<_, Option<i64>>(1),
        ) {
            (None, None) => 0,
            (Some(epoch), Some(sequence))
                if epoch == self.source_epoch.as_str() && sequence >= 0 =>
            {
                u64::try_from(sequence).map_err(|_| LogError::PostgresHeadCorrupt)?
            }
            _ => return Err(LogError::PostgresHeadCorrupt),
        };
        Ok(CursorPosition {
            source_epoch: self.source_epoch.clone(),
            sequence,
        })
    }

    /// The retention floor from a bounds row's third and fourth columns:
    /// sequence 1 before anything was retired.
    fn floor_from(&self, row: &Row) -> Result<CursorPosition, LogError> {
        let sequence = match (
            row.get::<_, Option<String>>(2),
            row.get::<_, Option<i64>>(3),
        ) {
            (None, None) => 1,
            (Some(epoch), Some(minimum)) if epoch == self.source_epoch.as_str() && minimum > 0 => {
                u64::try_from(minimum).map_err(|_| LogError::PostgresHeadCorrupt)?
            }
            _ => return Err(LogError::PostgresHeadCorrupt),
        };
        Ok(CursorPosition {
            source_epoch: self.source_epoch.clone(),
            sequence,
        })
    }

    fn head(&self, client: &mut Client) -> Result<CursorPosition, LogError> {
        self.head_from(&self.bounds_row(client)?)
    }

    fn minimum_retained(&self, client: &mut Client) -> Result<CursorPosition, LogError> {
        self.floor_from(&self.bounds_row(client)?)
    }

    fn bounds(&self, client: &mut Client) -> Result<LogBounds, LogError> {
        let row = self.bounds_row(client)?;
        Ok(LogBounds {
            head: self.head_from(&row)?,
            minimum_retained: self.floor_from(&row)?,
        })
    }

    fn read_page_after_bounded(
        &self,
        client: &mut Client,
        from_sequence_exclusive: u64,
        through_sequence_inclusive: Option<u64>,
        max_records: usize,
        max_bytes: u64,
    ) -> Result<CanonicalPage, LogError> {
        let rows = self.page_rows(
            client,
            from_sequence_exclusive,
            through_sequence_inclusive,
            max_records,
            max_bytes,
        )?;
        self.page_from_rows(
            rows,
            from_sequence_exclusive,
            through_sequence_inclusive,
            max_bytes,
        )
    }

    /// The bounds and the page from one run of the page statement: the bounds
    /// row it always returns, and the page those rows make.
    fn read_bounds_and_page_after_bounded(
        &self,
        client: &mut Client,
        from_sequence_exclusive: u64,
        through_sequence_inclusive: Option<u64>,
        max_records: usize,
        max_bytes: u64,
    ) -> Result<(LogBounds, Result<CanonicalPage, LogError>), LogError> {
        let rows = self.page_rows(
            client,
            from_sequence_exclusive,
            through_sequence_inclusive,
            max_records,
            max_bytes,
        )?;
        let first = rows.first().ok_or(LogError::PostgresHeadCorrupt)?;
        let bounds = LogBounds {
            head: self.head_from(first)?,
            minimum_retained: self.floor_from(first)?,
        };
        let page = self.page_from_rows(
            rows,
            from_sequence_exclusive,
            through_sequence_inclusive,
            max_bytes,
        );
        Ok((bounds, page))
    }

    /// Runs the page statement: the bounds on every row, then the candidates.
    fn page_rows(
        &self,
        client: &mut Client,
        from_sequence_exclusive: u64,
        through_sequence_inclusive: Option<u64>,
        max_records: usize,
        max_bytes: u64,
    ) -> Result<Vec<Row>, LogError> {
        if max_records == 0 || max_bytes == 0 {
            return Err(LogError::InvalidReadLimit);
        }
        let from =
            i64::try_from(from_sequence_exclusive).map_err(|_| LogError::PostgresHeadCorrupt)?;
        // The statement reads no further than the head, so a fence past what a
        // bigint holds reads as the head.
        let through =
            through_sequence_inclusive.map(|through| i64::try_from(through).unwrap_or(i64::MAX));
        let metadata_limit_from_bytes = max_bytes
            .checked_div(POSTGRES_READ_MINIMUM_CHARGED_ROW_BYTES)
            .unwrap_or(0)
            .saturating_add(1);
        let metadata_limit =
            max_records.min(usize::try_from(metadata_limit_from_bytes).unwrap_or(usize::MAX));
        let limit = i64::try_from(metadata_limit).unwrap_or(i64::MAX);
        let payload_limit = i64::try_from(max_bytes).unwrap_or(i64::MAX);
        let row_fixed_bytes = i64::try_from(POSTGRES_READ_DATA_ROW_FIXED_BYTES)
            .map_err(|_| LogError::PostgresHeadCorrupt)?;
        let statement = &self.statements(client)?.page;
        client
            .query(
                statement,
                &[
                    &self.source_id.as_str(),
                    &self.source_epoch.as_str(),
                    &from,
                    &through,
                    &limit,
                    &payload_limit,
                    &row_fixed_bytes,
                ],
            )
            .map_err(postgres_log_error)
    }

    /// The page the page statement's rows make, checked record by record.
    fn page_from_rows(
        &self,
        rows: Vec<Row>,
        from_sequence_exclusive: u64,
        through_sequence_inclusive: Option<u64>,
        max_bytes: u64,
    ) -> Result<CanonicalPage, LogError> {
        let bounds = rows.first().ok_or(LogError::PostgresHeadCorrupt)?;
        let minimum_retained = self.floor_from(bounds)?.sequence;
        if from_sequence_exclusive.saturating_add(1) < minimum_retained {
            return Err(LogError::BeforeRetention {
                requested: from_sequence_exclusive,
                minimum: minimum_retained,
            });
        }
        let head = self.head_from(bounds)?;
        let through = through_sequence_inclusive
            .unwrap_or(head.sequence)
            .min(head.sequence);
        if from_sequence_exclusive >= through {
            return Ok(CanonicalPage {
                records: Vec::new(),
                stored_bytes: 0,
            });
        }
        // Check the sizes first so a tiny byte limit is refused from them
        // alone; the statement sent no payload past the limit.
        let mut expected = from_sequence_exclusive
            .checked_add(1)
            .ok_or(LogError::SequenceExhausted)?;
        let mut charged = 0_u64;
        let mut selected = Vec::with_capacity(rows.len());
        for row in &rows {
            let Some(stored_sequence) = row.get::<_, Option<i64>>(4) else {
                break;
            };
            let sequence =
                u64::try_from(stored_sequence).map_err(|_| LogError::PostgresHeadCorrupt)?;
            if sequence != expected {
                return Err(LogError::SequenceGap {
                    expected,
                    actual: sequence,
                });
            }
            let entry_bytes: i64 = row.get(5);
            let checksum_bytes: i64 = row.get(6);
            if entry_bytes < 0 || entry_bytes as u64 > MAX_RECORD_BYTES || checksum_bytes != 64 {
                return Err(LogError::PostgresHeadCorrupt);
            }
            let row_bytes = u64::try_from(entry_bytes)
                .map_err(|_| LogError::PostgresHeadCorrupt)?
                .checked_add(
                    u64::try_from(checksum_bytes).map_err(|_| LogError::PostgresHeadCorrupt)?,
                )
                .and_then(|bytes| bytes.checked_add(POSTGRES_READ_DATA_ROW_FIXED_BYTES))
                .ok_or(LogError::RecordTooLarge)?;
            let next = charged
                .checked_add(row_bytes)
                .ok_or(LogError::RecordTooLarge)?;
            if next > max_bytes {
                if selected.is_empty() {
                    return Err(LogError::ReadLimitTooSmall {
                        next_record_bytes: row_bytes,
                        max_bytes,
                    });
                }
                break;
            }
            selected.push((sequence, row_bytes, row));
            charged = next;
            expected = expected.checked_add(1).ok_or(LogError::SequenceExhausted)?;
        }
        if selected.is_empty() {
            return Err(LogError::SequenceGap {
                expected,
                actual: head.sequence,
            });
        }
        let mut records = Vec::with_capacity(selected.len());
        for (sequence, selected_bytes, row) in selected {
            let (Some(entry_bytes), Some(stored_sha256)) = (
                row.get::<_, Option<&[u8]>>(7),
                row.get::<_, Option<&str>>(8),
            ) else {
                return Err(LogError::PostgresHeadCorrupt);
            };
            let actual_bytes = u64::try_from(entry_bytes.len())
                .map_err(|_| LogError::PostgresHeadCorrupt)?
                .checked_add(
                    u64::try_from(stored_sha256.len())
                        .map_err(|_| LogError::PostgresHeadCorrupt)?,
                )
                .and_then(|bytes| bytes.checked_add(POSTGRES_READ_DATA_ROW_FIXED_BYTES))
                .ok_or(LogError::RecordTooLarge)?;
            if actual_bytes != selected_bytes {
                return Err(LogError::PostgresHeadCorrupt);
            }
            if !sha256_hex_matches(entry_bytes, stored_sha256) {
                return Err(LogError::PostgresHeadCorrupt);
            }
            let entry = decode_stored_entry(entry_bytes, self.state_dir)?;
            if entry.key().source_epoch != *self.source_epoch {
                return Err(LogError::PostgresHeadCorrupt);
            }
            validate_encoded_entry(&entry, entry_bytes.len())
                .map_err(|_| LogError::PostgresHeadCorrupt)?;
            records.push(CanonicalRecord {
                position: CursorPosition {
                    source_epoch: self.source_epoch.clone(),
                    sequence,
                },
                entry,
            });
        }
        Ok(CanonicalPage {
            records,
            stored_bytes: charged,
        })
    }
}

/// A read-only connection to one source's log in PostgreSQL.
///
/// It runs the writer's reads, with the writer's statement and lock
/// timeouts, on a connection of its own, so a read need not wait for the
/// writer's connection or for another reader's. Its session is read-only by
/// default and it keeps no fence token: it neither appends nor takes a
/// retention lease.
pub struct PostgresCanonicalLogReader {
    /// A pool can drop a reader on a runtime thread.
    client: Mutex<DropOffRuntime<Client>>,
    read_statements: OnceLock<ReadStatements>,
    source_id: SourceId,
    source_epoch: SourceEpoch,
    state_dir: PathBuf,
}

impl PostgresCanonicalLogReader {
    /// Connects a reader of `writer`'s log. The connection must reach the
    /// datastore `writer` was verified against, as the writer's own must.
    pub fn connect(
        connection_string: &str,
        writer: &FencedWriterIdentity,
        state_dir: &Path,
    ) -> Result<Self, LogError> {
        let mut client = connect(connection_string)?;
        if PostgresDatastoreIdentity::inspect(&mut client)? != writer.datastore {
            return Err(LogError::DatastoreIdentityMismatch);
        }
        client
            .batch_execute(&format!(
                "SET statement_timeout = {};
                 SET lock_timeout = '1s';
                 SET default_transaction_read_only = on;",
                LOG_STATEMENT_TIMEOUT.as_millis()
            ))
            .map_err(postgres_log_error)?;
        Ok(Self {
            client: Mutex::new(DropOffRuntime::new(client)),
            read_statements: OnceLock::new(),
            source_id: writer.source_id.clone(),
            source_epoch: writer.source_epoch.clone(),
            state_dir: state_dir.to_path_buf(),
        })
    }

    /// Whether the connection has closed, after which every read fails.
    pub fn is_closed(&self) -> bool {
        self.client.lock().map_or(true, |client| client.is_closed())
    }

    fn log_source(&self) -> PostgresLogSource<'_> {
        PostgresLogSource {
            source_id: &self.source_id,
            source_epoch: &self.source_epoch,
            state_dir: &self.state_dir,
            statements: &self.read_statements,
        }
    }

    fn client(&self) -> Result<std::sync::MutexGuard<'_, DropOffRuntime<Client>>, LogError> {
        self.client
            .lock()
            .map_err(|_| LogError::PostgresUnavailable)
    }
}

impl CanonicalLogRead for PostgresCanonicalLogReader {
    fn source_id(&self) -> &SourceId {
        &self.source_id
    }

    fn source_epoch(&self) -> &SourceEpoch {
        &self.source_epoch
    }

    fn head(&self) -> Result<CursorPosition, LogError> {
        self.log_source().head(&mut *self.client()?)
    }

    fn minimum_retained(&self) -> Result<CursorPosition, LogError> {
        self.log_source().minimum_retained(&mut *self.client()?)
    }

    fn bounds(&self) -> Result<LogBounds, LogError> {
        self.log_source().bounds(&mut *self.client()?)
    }

    fn read_page_after_bounded(
        &self,
        from_sequence_exclusive: u64,
        through_sequence_inclusive: Option<u64>,
        max_records: usize,
        max_bytes: u64,
    ) -> Result<CanonicalPage, LogError> {
        self.log_source().read_page_after_bounded(
            &mut *self.client()?,
            from_sequence_exclusive,
            through_sequence_inclusive,
            max_records,
            max_bytes,
        )
    }

    fn read_bounds_and_page_after_bounded(
        &self,
        from_sequence_exclusive: u64,
        through_sequence_inclusive: Option<u64>,
        max_records: usize,
        max_bytes: u64,
    ) -> Result<(LogBounds, Result<CanonicalPage, LogError>), LogError> {
        self.log_source().read_bounds_and_page_after_bounded(
            &mut *self.client()?,
            from_sequence_exclusive,
            through_sequence_inclusive,
            max_records,
            max_bytes,
        )
    }
}

impl SnapshotBuildRetention for PostgresFencedCanonicalLog {
    fn maximum_retention_operation_duration(&self) -> Duration {
        RETENTION_OPERATION_DEADLINE
    }

    fn acquire_snapshot_build_retention(
        &mut self,
        build_id: SnapshotBuildId,
        owner_id: SnapshotBuildOwnerId,
        ttl: Duration,
    ) -> Result<SnapshotBuildRetentionLease, LogError> {
        self.acquire_snapshot_build_retention_inner(build_id, owner_id, ttl)
    }

    fn renew_snapshot_build_retention(
        &mut self,
        lease: &SnapshotBuildRetentionLease,
        ttl: Duration,
    ) -> Result<SnapshotBuildRetentionLease, LogError> {
        self.renew_snapshot_build_retention_inner(lease, ttl)
    }

    fn release_snapshot_build_retention(
        &mut self,
        lease: &SnapshotBuildRetentionLease,
    ) -> Result<(), LogError> {
        self.release_snapshot_build_retention_inner(lease)
    }
}

impl CanonicalLogRead for PostgresFencedCanonicalLog {
    fn source_id(&self) -> &SourceId {
        PostgresFencedCanonicalLog::source_id(self)
    }

    fn source_epoch(&self) -> &SourceEpoch {
        PostgresFencedCanonicalLog::source_epoch(self)
    }

    fn head(&self) -> Result<CursorPosition, LogError> {
        PostgresFencedCanonicalLog::head(self)
    }

    fn minimum_retained(&self) -> Result<CursorPosition, LogError> {
        PostgresFencedCanonicalLog::minimum_retained(self)
    }

    fn bounds(&self) -> Result<LogBounds, LogError> {
        PostgresFencedCanonicalLog::bounds(self)
    }

    fn read_page_after_bounded(
        &self,
        from_sequence_exclusive: u64,
        through_sequence_inclusive: Option<u64>,
        max_records: usize,
        max_bytes: u64,
    ) -> Result<CanonicalPage, LogError> {
        PostgresFencedCanonicalLog::read_page_after_bounded(
            self,
            from_sequence_exclusive,
            through_sequence_inclusive,
            max_records,
            max_bytes,
        )
    }

    fn read_bounds_and_page_after_bounded(
        &self,
        from_sequence_exclusive: u64,
        through_sequence_inclusive: Option<u64>,
        max_records: usize,
        max_bytes: u64,
    ) -> Result<(LogBounds, Result<CanonicalPage, LogError>), LogError> {
        PostgresFencedCanonicalLog::read_bounds_and_page_after_bounded(
            self,
            from_sequence_exclusive,
            through_sequence_inclusive,
            max_records,
            max_bytes,
        )
    }
}

impl CanonicalLogPlanRetention for PostgresFencedCanonicalLog {
    fn acquire_plan_retention_lease(
        &mut self,
        plan_id: PlanId,
        minimum_sequence: u64,
        expires_ms: i64,
        _now_ms: i64,
    ) -> Result<(), LogError> {
        self.acquire_plan_retention_lease_inner(plan_id, minimum_sequence, expires_ms)
    }
}

/// How many retired identities one trim forgets at most. A trim holds back
/// appends while it runs, so it forgets a backlog a few thousand rows at a
/// time; twice the trim's own 1,024 entries, so a backlog shrinks with every
/// trim.
const COMPACTION_BATCH_ROWS: i64 = 2_048;

/// The LSN at or before which PostgreSQL sends no transaction to this source
/// again, while `writer` holds the source's active fence: the least of the
/// log's durable checkpoint and the confirmed flush of the source's own slot
/// in this cluster. PostgreSQL starts a logical slot no earlier than its
/// confirmed flush, and calque confirms only what its log holds. `None` when
/// the fence is not this writer's or the slot is not here, and nothing is
/// forgotten then.
fn compaction_floor(
    transaction: &mut Transaction<'_>,
    writer: &FencedWriterIdentity,
) -> Result<Option<PgLsn>, LogError> {
    let row = transaction
        .query_opt(
            "SELECT least(enrollment.durable_canonical_lsn::pg_lsn, slot.confirmed_flush_lsn)
               FROM calque_control.source_fence_lease AS lease
               JOIN calque_control.canonical_log_writer_fence AS writer
                 ON writer.source_id = lease.source_id
               JOIN calque_control.source_slot_enrollment AS enrollment
                 ON enrollment.source_id = lease.source_id
                AND enrollment.source_epoch = lease.source_epoch
               JOIN pg_catalog.pg_replication_slots AS slot
                 ON slot.slot_name = enrollment.slot_name
                AND slot.database = enrollment.database_name
                AND slot.slot_type = 'logical'
              WHERE lease.source_id = $1
                AND lease.source_epoch = $2
                AND lease.fence_token = $3::bigint
                AND lease.lease_phase = 'active'
                AND lease.expires_at > clock_timestamp()
                AND writer.source_epoch = lease.source_epoch
                AND writer.fence_token = lease.fence_token
                AND enrollment.system_identifier
                    = (SELECT system_identifier::text FROM pg_catalog.pg_control_system())
                AND slot.confirmed_flush_lsn IS NOT NULL
              FOR UPDATE OF lease, writer",
            &[
                &writer.source_id.as_str(),
                &writer.source_epoch.as_str(),
                &writer.fence_token,
            ],
        )
        .map_err(postgres_log_error)?;
    Ok(row.and_then(|row| row.get::<_, Option<PgLsn>>(0)))
}

/// Forgets the oldest retired identities that end at or before `floor`, at
/// most [`COMPACTION_BATCH_ROWS`] of them, and records that the log forgot
/// every identity through `floor`: the append refuses an entry that ends
/// there and is neither live nor retired. Sequences follow WAL order, so the
/// oldest retired identities are the ones that end earliest.
fn compact_retired_records(
    transaction: &mut Transaction<'_>,
    writer: &FencedWriterIdentity,
    floor: PgLsn,
) -> Result<(), LogError> {
    transaction
        .execute(
            "DELETE FROM calque_control.canonical_log_retired_record AS record
              USING (
                  SELECT original_sequence
                    FROM calque_control.canonical_log_retired_record
                   WHERE source_id = $1 AND source_epoch = $2
                   ORDER BY original_sequence
                   LIMIT $4
              ) AS oldest
              WHERE record.source_id = $1
                AND record.source_epoch = $2
                AND record.original_sequence = oldest.original_sequence
                AND (record.transaction_identity::jsonb ->> 'endLsn')::pg_lsn <= $3::pg_lsn",
            &[
                &writer.source_id.as_str(),
                &writer.source_epoch.as_str(),
                &floor,
                &COMPACTION_BATCH_ROWS,
            ],
        )
        .map_err(postgres_log_error)?;
    let updated = transaction
        .execute(
            "UPDATE calque_control.canonical_log_retention
                SET retired_compacted_through_lsn
                    = greatest(retired_compacted_through_lsn, $3::pg_lsn)
              WHERE source_id = $1 AND source_epoch = $2",
            &[
                &writer.source_id.as_str(),
                &writer.source_epoch.as_str(),
                &floor,
            ],
        )
        .map_err(postgres_log_error)?;
    if updated != 1 {
        return Err(LogError::PostgresHeadCorrupt);
    }
    Ok(())
}

fn ensure_postgres_retention_state(
    transaction: &mut Transaction<'_>,
    writer: &FencedWriterIdentity,
) -> Result<(), LogError> {
    transaction
        .execute(
            "INSERT INTO calque_control.canonical_log_retention
                (source_id, source_epoch, minimum_retained_sequence,
                 next_build_lease_token)
             VALUES ($1, $2, 1, 0)
             ON CONFLICT (source_id) DO NOTHING",
            &[&writer.source_id.as_str(), &writer.source_epoch.as_str()],
        )
        .map_err(postgres_log_error)?;
    Ok(())
}

/// Every log connection's `statement_timeout`. A request that hears nothing
/// from the server for this long plus `postgres_connect::REPLY_MARGIN` fails
/// the connection, so an append whose server stops answering returns.
const LOG_STATEMENT_TIMEOUT: Duration = Duration::from_secs(2);

fn connect(connection_string: &str) -> Result<Client, LogError> {
    postgres_connect::Endpoint::parse(connection_string)
        .map_err(|_| LogError::PostgresUnavailable)?
        .bound_replies(LOG_STATEMENT_TIMEOUT)
        .connect()
        .map_err(postgres_log_error)
}

fn postgres_log_error(_error: postgres::Error) -> LogError {
    LogError::PostgresUnavailable
}

fn snapshot_build_lease_ttl_millis(ttl: Duration) -> Result<i64, LogError> {
    i64::try_from(ttl.as_millis())
        .ok()
        .filter(|ttl_ms| *ttl_ms > 0)
        .ok_or(LogError::InvalidSnapshotBuildRetentionLease)
}

fn transaction_key_string(key: &TransactionKey) -> Result<String, serde_json::Error> {
    serde_json::to_string(key)
}

/// Checks an entry against what the log may hold and deliver, given the
/// length of its JSON encoding: the caller already has that encoding in hand,
/// so the size a transaction's delivery is bounded by is not found by
/// encoding it again.
fn validate_encoded_entry(entry: &CanonicalEntry, encoded_bytes: usize) -> Result<(), LogError> {
    if entry.key().end_lsn.is_empty() {
        return Err(LogError::InvalidEntry("end LSN is empty"));
    }
    match entry {
        CanonicalEntry::Transaction(transaction) => {
            if transaction.transaction_id.is_empty() {
                return Err(LogError::InvalidEntry("transaction ID is empty"));
            }
            if transaction.begin_lsn.is_empty() || transaction.commit_lsn.is_empty() {
                return Err(LogError::InvalidEntry(
                    "transaction LSN provenance is empty",
                ));
            }
            if transaction.operations.len() > MAX_DELIVERABLE_TRANSACTION_OPERATIONS {
                return Err(LogError::TransactionOperationLimitExceeded {
                    actual: transaction.operations.len(),
                    maximum: MAX_DELIVERABLE_TRANSACTION_OPERATIONS,
                });
            }
            let mut mutation_ids = BTreeMap::new();
            for mutation_id in &transaction.mutation_ids {
                if mutation_ids.insert(mutation_id, ()).is_some() {
                    return Err(LogError::InvalidEntry("duplicate mutation ID"));
                }
            }
            for operation in &transaction.operations {
                validate_operation(operation)?;
            }
            if encoded_bytes > MAX_DELIVERABLE_TRANSACTION_ENTRY_BYTES {
                return Err(LogError::TransactionDeliveryLimitExceeded {
                    actual: encoded_bytes,
                    maximum: MAX_DELIVERABLE_TRANSACTION_ENTRY_BYTES,
                });
            }
        }
        CanonicalEntry::Barrier(barrier) => {
            if barrier.build_id.is_empty() || barrier.nonce.is_empty() {
                return Err(LogError::InvalidEntry("barrier identity is empty"));
            }
            if barrier.message_lsn.is_empty() || barrier.commit_lsn.is_empty() {
                return Err(LogError::InvalidEntry("barrier LSN provenance is empty"));
            }
        }
    }
    Ok(())
}

fn validate_operation(operation: &CanonicalOperation) -> Result<(), LogError> {
    match &operation.kind {
        CanonicalOperationKind::InsertFullRow { columns } => {
            if columns.is_empty() {
                return Err(LogError::InvalidEntry("insert row is empty"));
            }
            decode_primary_key(&operation.primary_key)?;
            validate_columns(columns)?;
        }
        CanonicalOperationKind::UpdatePatch { changed_columns } => {
            if changed_columns.is_empty() {
                return Err(LogError::InvalidEntry("update patch is empty"));
            }
            decode_primary_key(&operation.primary_key)?;
            validate_columns(changed_columns)?;
        }
        CanonicalOperationKind::DeleteByKey => {
            decode_primary_key(&operation.primary_key)?;
        }
        CanonicalOperationKind::TruncateAuthorizedTable { reason } => {
            if !operation.primary_key.is_empty() {
                return Err(LogError::InvalidEntry("truncate primary key is not empty"));
            }
            if reason.is_empty() {
                return Err(LogError::InvalidEntry("truncate reason is empty"));
            }
        }
    }
    Ok(())
}

fn validate_columns(columns: &[ColumnValue]) -> Result<(), LogError> {
    // Columns usually arrive in ascending ID order, which rules out a
    // duplicate without sorting; any other order is sorted and checked.
    let mut ascending = true;
    let mut previous = 0;
    for column in columns {
        let column_id = column.column_id.get();
        if column_id == 0 {
            return Err(LogError::InvalidEntry("column ID is zero"));
        }
        ascending &= column_id > previous;
        previous = column_id;
        match &column.value {
            CellValue::Float64(value) if !value.is_finite() => {
                return Err(LogError::InvalidEntry("floating-point value is not finite"));
            }
            _ => {}
        }
    }
    if !ascending {
        let mut column_ids = columns
            .iter()
            .map(|column| column.column_id)
            .collect::<Vec<_>>();
        column_ids.sort_unstable();
        if column_ids.windows(2).any(|pair| pair[0] == pair[1]) {
            return Err(LogError::InvalidEntry("duplicate column ID"));
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use std::fs;

    use super::*;
    use crate::file_log::{
        segment_file_name, write_manifest, DurabilityPoint, FORMAT_VERSION, SEGMENTS_DIRECTORY,
    };
    use sync_protocol::{
        change::{
            cell_value, CellValue as WireCellValue, ColumnValue as WireColumnValue, PrimaryKey,
        },
        encode_primary_key, PRIMARY_KEY_CODEC_VERSION,
    };
    use tempfile::TempDir;

    fn source_id() -> SourceId {
        SourceId::new("source-main").unwrap()
    }

    fn encoded(identity: &str, bytes: usize) -> EncodedAppend {
        EncodedAppend {
            transaction_identity: identity.to_owned(),
            entry_bytes: vec![0; bytes],
            entry_sha256: String::new(),
            end_lsn: PgLsn::from(0),
        }
    }

    /// Stored records carry this digest, so the hasher that checks it on a
    /// read must compute the same one. The expected value is from Python's
    /// `hashlib`, over a record long enough to span several SHA-256 blocks.
    #[test]
    fn a_record_digest_is_the_standard_sha256_in_lowercase_hex() {
        assert_eq!(
            sha256_hex(&b"calque canonical log entry ".repeat(40)),
            "5439e27816a1008ad05989808eca52a7f2e6d0cb72ef02efe45b641f4713160b"
        );
    }

    #[test]
    #[ignore = "requires CALQUE_CONTROL_DATABASE_URL"]
    fn a_readers_session_refuses_a_write_and_another_datastore() {
        let url =
            std::env::var("CALQUE_CONTROL_DATABASE_URL").expect("live control-store database URL");
        let mut control = Client::connect(&url, postgres::NoTls).unwrap();
        PostgresFencedCanonicalLog::install_schema(&mut control).unwrap();
        let datastore = PostgresDatastoreIdentity::inspect(&mut control).unwrap();
        let source = SourceId::new(format!("log-reader-{}", std::process::id())).unwrap();
        let epoch = SourceEpoch::new("epoch-1").unwrap();
        let writer =
            FencedWriterIdentity::new(source.clone(), epoch.clone(), 1, datastore).unwrap();
        let reader =
            PostgresCanonicalLogReader::connect(&url, &writer, Path::new("/srv/calque/state"))
                .unwrap();
        let refused = reader
            .client
            .lock()
            .unwrap()
            .execute(
                "INSERT INTO calque_control.canonical_log_head
                    (source_id, source_epoch, head_sequence)
                 VALUES ($1, $2, 0)",
                &[&source.as_str(), &epoch.as_str()],
            )
            .unwrap_err();
        assert_eq!(
            refused.code(),
            Some(&postgres::error::SqlState::READ_ONLY_SQL_TRANSACTION)
        );
        assert!(control
            .query_opt(
                "SELECT 1 FROM calque_control.canonical_log_head WHERE source_id = $1",
                &[&source.as_str()],
            )
            .unwrap()
            .is_none());

        let elsewhere = FencedWriterIdentity::new(
            source,
            epoch,
            1,
            PostgresDatastoreIdentity {
                system_identifier: "0".to_owned(),
                database_oid: 1,
            },
        )
        .unwrap();
        assert!(matches!(
            PostgresCanonicalLogReader::connect(&url, &elsewhere, Path::new("/srv/calque/state")),
            Err(LogError::DatastoreIdentityMismatch)
        ));
    }

    /// The statements a session holds, when each was prepared and how often
    /// it ran, read over the simple protocol so the query leaves no statement
    /// of its own.
    fn prepared_statements(client: &mut Client) -> Vec<(String, String, i64)> {
        client
            .simple_query(
                "SELECT statement, prepare_time::text,
                        (generic_plans + custom_plans)::text
                   FROM pg_prepared_statements ORDER BY statement",
            )
            .unwrap()
            .into_iter()
            .filter_map(|message| match message {
                postgres::SimpleQueryMessage::Row(row) => Some((
                    row.get(0).unwrap().to_owned(),
                    row.get(1).unwrap().to_owned(),
                    row.get(2).unwrap().parse().unwrap(),
                )),
                _ => None,
            })
            .collect()
    }

    #[test]
    #[ignore = "requires CALQUE_CONTROL_DATABASE_URL"]
    fn a_reader_prepares_its_read_statements_once_and_keeps_no_other() {
        let url =
            std::env::var("CALQUE_CONTROL_DATABASE_URL").expect("live control-store database URL");
        let mut control = Client::connect(&url, postgres::NoTls).unwrap();
        PostgresFencedCanonicalLog::install_schema(&mut control).unwrap();
        let datastore = PostgresDatastoreIdentity::inspect(&mut control).unwrap();
        let source = SourceId::new(format!("log-prepared-{}", std::process::id())).unwrap();
        let writer = FencedWriterIdentity::new(source.clone(), epoch(), 1, datastore).unwrap();
        let cleanup = |control: &mut Client| {
            for table in ["canonical_log_record", "canonical_log_head"] {
                control
                    .execute(
                        &format!("DELETE FROM calque_control.{table} WHERE source_id = $1"),
                        &[&source.as_str()],
                    )
                    .unwrap();
            }
        };
        cleanup(&mut control);
        // Three records, so a page read runs every statement.
        control
            .execute(
                "INSERT INTO calque_control.canonical_log_head
                    (source_id, source_epoch, head_sequence)
                 VALUES ($1, $2, 3)",
                &[&source.as_str(), &epoch().as_str()],
            )
            .unwrap();
        for sequence in 1..=3_u64 {
            let entry = CanonicalEntry::Transaction(transaction(sequence));
            let entry_bytes = entry.encode();
            control
                .execute(
                    "INSERT INTO calque_control.canonical_log_record
                        (source_id, source_epoch, sequence, transaction_identity,
                         entry_bytes, entry_sha256)
                     VALUES ($1, $2, $3, $4, $5, $6)",
                    &[
                        &source.as_str(),
                        &epoch().as_str(),
                        &i64::try_from(sequence).unwrap(),
                        &transaction_key_string(entry.key()).unwrap(),
                        &entry_bytes,
                        &sha256_hex(&entry_bytes),
                    ],
                )
                .unwrap();
        }
        let reader =
            PostgresCanonicalLogReader::connect(&url, &writer, Path::new("/srv/calque/state"))
                .unwrap();
        let read = || {
            assert_eq!(reader.head().unwrap().sequence, 3);
            reader.minimum_retained().unwrap();
            assert_eq!(
                reader.bounds().unwrap(),
                LogBounds {
                    head: reader.head().unwrap(),
                    minimum_retained: reader.minimum_retained().unwrap(),
                }
            );
            let page = reader
                .read_page_after_bounded(0, None, 10, 1 << 20)
                .unwrap();
            assert_eq!(page.records.len(), 3);
        };

        read();
        let first = prepared_statements(&mut reader.client.lock().unwrap());
        let mut expected = [READ_BOUNDS_SQL, READ_PAGE_SQL];
        expected.sort_unstable();
        assert_eq!(
            first
                .iter()
                .map(|(statement, _, _)| statement.as_str())
                .collect::<Vec<_>>(),
            expected
        );

        for _ in 0..20 {
            read();
        }
        let later = prepared_statements(&mut reader.client.lock().unwrap());
        assert_eq!(
            later
                .iter()
                .map(|(statement, prepared, _)| (statement.as_str(), prepared.as_str()))
                .collect::<Vec<_>>(),
            first
                .iter()
                .map(|(statement, prepared, _)| (statement.as_str(), prepared.as_str()))
                .collect::<Vec<_>>(),
            "later reads reuse the statements the first prepared"
        );
        // Each read runs the bounds statement five times, for the head and
        // the floor, then for the bounds and the two again to compare them
        // with, and the page statement once: a statement sent as text instead
        // would leave its count short.
        let runs = |statements: &[(String, String, i64)], sql: &str| {
            statements
                .iter()
                .find(|(statement, _, _)| statement == sql)
                .map(|(_, _, runs)| *runs)
                .unwrap()
        };
        assert_eq!(runs(&later, READ_BOUNDS_SQL), 105);
        assert_eq!(runs(&later, READ_PAGE_SQL), 21);

        // A combined read is one run of the page statement, and the head and
        // floor it returns are that run's: the bounds statement does not run.
        let (bounds, page) = reader
            .read_bounds_and_page_after_bounded(0, None, 10, 1 << 20)
            .unwrap();
        let combined = prepared_statements(&mut reader.client.lock().unwrap());
        assert_eq!(runs(&combined, READ_BOUNDS_SQL), 105);
        assert_eq!(runs(&combined, READ_PAGE_SQL), 22);
        assert_eq!(bounds, reader.bounds().unwrap());
        assert_eq!(page.unwrap().records.len(), 3);

        // A byte limit that holds the first record and not the second: the
        // page statement sends the second's sizes, so the read can stop on
        // them, and not its payload.
        let first_bytes: i64 = control
            .query_one(
                "SELECT octet_length(entry_bytes)::bigint + 64
                   FROM calque_control.canonical_log_record
                  WHERE source_id = $1 AND sequence = 1",
                &[&source.as_str()],
            )
            .unwrap()
            .get(0);
        let fixed = i64::try_from(POSTGRES_READ_DATA_ROW_FIXED_BYTES).unwrap();
        let rows = control
            .query(
                READ_PAGE_SQL,
                &[
                    &source.as_str(),
                    &epoch().as_str(),
                    &0_i64,
                    &None::<i64>,
                    &10_i64,
                    &(first_bytes + fixed),
                    &fixed,
                ],
            )
            .unwrap();
        let sent = rows
            .iter()
            .map(|row| {
                (
                    row.get::<_, i64>(4),
                    row.get::<_, Option<Vec<u8>>>(7).is_some(),
                    row.get::<_, Option<String>>(8).is_some(),
                )
            })
            .collect::<Vec<_>>();
        assert_eq!(
            sent,
            [(1, true, true), (2, false, false), (3, false, false)]
        );
        let page = reader
            .read_page_after_bounded(0, None, 10, u64::try_from(first_bytes + fixed).unwrap())
            .unwrap();
        assert_eq!(page.records.len(), 1);
        cleanup(&mut control);
    }

    #[test]
    fn an_append_group_splits_before_a_repeated_key_and_at_the_byte_cap() {
        let distinct = [encoded("a", 10), encoded("b", 10), encoded("c", 10)];
        assert_eq!(
            append_statements(&distinct),
            vec![Range { start: 0, end: 3 }]
        );

        let repeated = [
            encoded("a", 10),
            encoded("b", 10),
            encoded("a", 10),
            encoded("c", 10),
            encoded("b", 10),
        ];
        assert_eq!(append_statements(&repeated), [0..2, 2..5]);

        let half = MAX_APPEND_STATEMENT_BYTES / 2;
        let large = [
            encoded("a", half),
            encoded("b", MAX_APPEND_STATEMENT_BYTES - half),
            encoded("c", 1),
            encoded("d", MAX_APPEND_STATEMENT_BYTES + 1),
            encoded("e", 1),
        ];
        assert_eq!(append_statements(&large), [0..2, 2..3, 3..4, 4..5]);
    }

    fn epoch() -> SourceEpoch {
        SourceEpoch::new("epoch-1").unwrap()
    }

    fn transaction(sequence: u64) -> CanonicalTransaction {
        CanonicalTransaction {
            key: TransactionKey {
                source_epoch: epoch(),
                end_lsn: format!("0/{:X}", sequence + 1),
                postgres_xid: sequence as u32,
            },
            transaction_id: format!("tx-{sequence}"),
            begin_lsn: format!("0/{:X}", sequence.saturating_sub(1)),
            commit_lsn: format!("0/{sequence:X}"),
            commit_time_unix_micros: sequence,
            operations: vec![CanonicalOperation {
                table_id: TableId::new("public.tasks").unwrap(),
                primary_key: encode_primary_key(&PrimaryKey {
                    codec_version: PRIMARY_KEY_CODEC_VERSION,
                    parts: vec![WireColumnValue {
                        column_id: 1,
                        value: Some(WireCellValue {
                            kind: Some(cell_value::Kind::Int64Value(
                                i64::try_from(sequence).unwrap(),
                            )),
                        }),
                    }],
                })
                .unwrap(),
                kind: CanonicalOperationKind::UpdatePatch {
                    changed_columns: vec![ColumnValue {
                        column_id: ColumnId::new(1),
                        value: CellValue::Text(format!("task {sequence}")),
                    }],
                },
            }],
            mutation_ids: Vec::new(),
        }
    }

    fn barrier(sequence: u64) -> CanonicalBarrier {
        CanonicalBarrier {
            key: TransactionKey {
                source_epoch: epoch(),
                end_lsn: format!("0/{:X}", sequence + 1),
                postgres_xid: sequence as u32,
            },
            build_id: format!("build-{sequence}"),
            nonce: format!("nonce-{sequence}"),
            message_lsn: format!("0/{:X}", sequence.saturating_sub(1)),
            commit_lsn: format!("0/{sequence:X}"),
            commit_time_unix_micros: sequence,
        }
    }

    #[test]
    fn append_is_durable_contiguous_and_reopenable() {
        let directory = TempDir::new().unwrap();
        let mut log = FileCanonicalLog::open(directory.path(), source_id(), epoch()).unwrap();

        assert_eq!(
            log.append(transaction(1)).unwrap(),
            AppendOutcome::Appended(CursorPosition {
                source_epoch: epoch(),
                sequence: 1,
            })
        );
        log.append(transaction(2)).unwrap();
        drop(log);

        let log = FileCanonicalLog::open(directory.path(), source_id(), epoch()).unwrap();
        let records = log.read_after(0, None).unwrap();
        assert_eq!(records.len(), 2);
        assert_eq!(records[0].position.sequence, 1);
        assert_eq!(records[1].position.sequence, 2);
        assert_eq!(log.head().sequence, 2);
    }

    #[test]
    fn reads_are_bounded_by_record_count_and_encoded_bytes() {
        let directory = TempDir::new().unwrap();
        let source_id = source_id();
        let epoch = epoch();
        let mut log = FileCanonicalLog::open(directory.path(), source_id, epoch).unwrap();
        log.append(transaction(1)).unwrap();
        log.append(transaction(2)).unwrap();

        let one = log
            .read_after_bounded(0, None, 1, MAX_RECORD_BYTES)
            .unwrap();
        assert_eq!(one.len(), 1);
        assert_eq!(one[0].position.sequence, 1);
        let after_first = log
            .read_after_bounded(1, None, 1, MAX_RECORD_BYTES)
            .unwrap();
        assert_eq!(after_first[0].position.sequence, 2);

        assert!(matches!(
            log.read_after_bounded(0, None, 1, 1),
            Err(LogError::ReadLimitTooSmall { max_bytes: 1, .. })
        ));
        assert!(matches!(
            log.read_after_bounded(0, None, 0, MAX_RECORD_BYTES),
            Err(LogError::InvalidReadLimit)
        ));

        fn read_through_provider<L: CanonicalLogRead + ?Sized>(
            log: &L,
        ) -> Result<CanonicalPage, LogError> {
            assert_eq!(log.source_id().as_str(), "source-main");
            assert_eq!(log.source_epoch().as_str(), "epoch-1");
            assert_eq!(log.head()?.sequence, 2);
            assert_eq!(log.minimum_retained()?.sequence, 1);
            log.read_page_after_bounded(0, None, 1, MAX_RECORD_BYTES)
        }

        let page = read_through_provider(&log).unwrap();
        assert_eq!(page.records.len(), 1);
        assert!(page.stored_bytes > 0);

        fn assert_send_sync<T: Send + Sync>() {}
        assert_send_sync::<FileCanonicalLog>();
        assert_send_sync::<PostgresFencedCanonicalLog>();
    }

    #[test]
    fn rejects_transactions_that_cannot_be_delivered_by_the_change_protocol() {
        let directory = TempDir::new().unwrap();
        let mut log = FileCanonicalLog::open(directory.path(), source_id(), epoch()).unwrap();
        let mut too_many_operations = transaction(1);
        too_many_operations.operations = vec![
            too_many_operations.operations[0].clone();
            MAX_DELIVERABLE_TRANSACTION_OPERATIONS + 1
        ];
        assert!(matches!(
            log.append(too_many_operations),
            Err(LogError::TransactionOperationLimitExceeded { .. })
        ));

        let mut too_large = transaction(2);
        too_large.operations = vec![CanonicalOperation {
            table_id: TableId::new("public.tasks").unwrap(),
            primary_key: Vec::new(),
            kind: CanonicalOperationKind::TruncateAuthorizedTable {
                reason: "x".repeat(MAX_DELIVERABLE_TRANSACTION_ENTRY_BYTES),
            },
        }];
        assert!(matches!(
            log.append(too_large),
            Err(LogError::TransactionDeliveryLimitExceeded { .. })
        ));
        assert_eq!(log.head().sequence, 0);
    }

    #[test]
    fn operation_column_ids_are_unique() {
        let directory = TempDir::new().unwrap();
        let mut log = FileCanonicalLog::open(directory.path(), source_id(), epoch()).unwrap();
        let mut duplicate = transaction(1);
        let CanonicalOperationKind::UpdatePatch { changed_columns } =
            &mut duplicate.operations[0].kind
        else {
            panic!("expected update patch")
        };
        changed_columns.push(changed_columns[0].clone());
        assert!(matches!(
            log.append(duplicate),
            Err(LogError::InvalidEntry("duplicate column ID"))
        ));
        assert_eq!(log.head().sequence, 0);
    }

    #[test]
    fn column_ids_in_any_order_are_checked_for_duplicates() {
        let columns = |ids: &[u32]| {
            ids.iter()
                .map(|&id| ColumnValue {
                    column_id: ColumnId::new(id),
                    value: CellValue::Null,
                })
                .collect::<Vec<_>>()
        };
        assert!(validate_columns(&columns(&[1, 2, 5])).is_ok());
        assert!(validate_columns(&columns(&[5, 1, 2])).is_ok());
        for duplicated in [&[1, 2, 1][..], &[3, 1, 3], &[2, 2]] {
            assert!(matches!(
                validate_columns(&columns(duplicated)),
                Err(LogError::InvalidEntry("duplicate column ID"))
            ));
        }
        assert!(matches!(
            validate_columns(&columns(&[2, 0])),
            Err(LogError::InvalidEntry("column ID is zero"))
        ));
    }

    /// An entry stored as JSON, as every earlier release stored it, with or
    /// without the `routingIdentity` one of them wrote, is refused as that
    /// release's log.
    #[test]
    fn an_entry_an_earlier_release_wrote_is_refused_not_read() {
        let current = CanonicalEntry::Transaction(transaction(1));
        let mut routing = serde_json::to_value(&current).unwrap();
        routing["record"]["operations"][0]
            .as_object_mut()
            .unwrap()
            .insert("routingIdentity".to_owned(), serde_json::json!([]));
        let state_dir = Path::new(".calque-dev");
        for earlier in [
            serde_json::to_vec(&current).unwrap(),
            serde_json::to_vec(&routing).unwrap(),
            serde_json::to_vec(&CanonicalEntry::Barrier(barrier(1))).unwrap(),
        ] {
            let refused = decode_stored_entry(&earlier, state_dir);
            assert!(matches!(
                &refused,
                Err(LogError::EarlierEntryFormat { state_dir: named }) if named == state_dir
            ));
            let refused = refused.unwrap_err();
            assert!(refused.requires_destroying_state());
            assert_eq!(
                refused.to_string(),
                ".calque-dev serves a canonical log an earlier release of calque wrote, and calque does not migrate it."
            );
        }
        assert_eq!(
            decode_stored_entry(&current.encode(), state_dir).unwrap(),
            current
        );
    }

    /// In JSON, as the filesystem log stores it, an operation is written with
    /// its kind's fields beside `kind`, and decoding reads every kind and
    /// every cell back as it was written, a float among them, whatever the
    /// order of the operation's fields.
    #[test]
    fn a_stored_operation_decodes_to_the_operation_written() {
        let operation = CanonicalOperation {
            table_id: TableId::new("public.tasks").unwrap(),
            primary_key: vec![1, 2],
            kind: CanonicalOperationKind::UpdatePatch {
                changed_columns: vec![ColumnValue {
                    column_id: ColumnId::new(1),
                    value: CellValue::Text("a".to_owned()),
                }],
            },
        };
        let written = r#"{"tableId":"public.tasks","primaryKey":[1,2],"kind":"UPDATE_PATCH","changed_columns":[{"columnId":1,"value":{"kind":"TEXT","value":"a"}}]}"#;
        assert_eq!(serde_json::to_string(&operation).unwrap(), written);
        assert_eq!(
            serde_json::from_str::<CanonicalOperation>(written).unwrap(),
            operation
        );

        let cells = [
            CellValue::Null,
            CellValue::Int64(-7),
            CellValue::Uint64(u64::MAX),
            CellValue::Float64(0.5),
            CellValue::Text("text".to_owned()),
            CellValue::Blob(vec![0, 255]),
            CellValue::Bool(true),
            CellValue::DecimalText("1.50".to_owned()),
            CellValue::TimestampUnixMicros(1_700_000_000_000_000),
            CellValue::Uuid16([9; 16]),
            CellValue::JsonText("{\"a\":1}".to_owned()),
            CellValue::DateIso8601("2026-10-16".to_owned()),
            CellValue::TimeIso8601("12:00:00".to_owned()),
            CellValue::IntervalIso8601("P1D".to_owned()),
        ];
        let columns = (1_u32..)
            .zip(cells)
            .map(|(column_id, value)| ColumnValue {
                column_id: ColumnId::new(column_id),
                value,
            })
            .collect::<Vec<_>>();
        let mut every_kind = transaction(1);
        every_kind.operations = [
            CanonicalOperationKind::InsertFullRow {
                columns: columns.clone(),
            },
            CanonicalOperationKind::UpdatePatch {
                changed_columns: columns,
            },
            CanonicalOperationKind::DeleteByKey,
            CanonicalOperationKind::TruncateAuthorizedTable {
                reason: "table truncated".to_owned(),
            },
        ]
        .into_iter()
        .map(|kind| CanonicalOperation {
            kind,
            ..operation.clone()
        })
        .collect();
        let entry = CanonicalEntry::Transaction(every_kind);
        let decode = |bytes: &[u8]| serde_json::from_slice::<CanonicalEntry>(bytes).unwrap();
        assert_eq!(decode(&serde_json::to_vec(&entry).unwrap()), entry);
        // `serde_json::Value` sorts an object's keys, which puts `columns` and
        // `changed_columns` before `kind`. The entry's tag stays first, as
        // every entry is written.
        let CanonicalEntry::Transaction(sorted) = &entry else {
            unreachable!()
        };
        let sorted = format!(
            r#"{{"recordKind":"TRANSACTION","record":{}}}"#,
            serde_json::to_value(sorted).unwrap()
        )
        .into_bytes();
        let at = |needle: &[u8]| {
            sorted
                .windows(needle.len())
                .position(|window| window == needle)
                .unwrap()
        };
        assert!(at(br#""columns""#) < at(br#""kind":"INSERT_FULL_ROW""#));
        assert_eq!(decode(&sorted), entry);
    }

    /// An operation without the field its kind owns, or of a kind this release
    /// does not know, is refused; a well-formed field another kind owns is
    /// ignored.
    #[test]
    fn a_stored_operation_missing_its_kinds_field_is_refused() {
        let decode = |operation: &str| {
            serde_json::from_str::<CanonicalOperation>(&format!(
                r#"{{"tableId":"public.tasks","primaryKey":[1],{operation}}}"#
            ))
        };
        for refused in [
            r#""kind":"INSERT_FULL_ROW""#,
            r#""kind":"INSERT_FULL_ROW","changed_columns":[]"#,
            r#""kind":"UPDATE_PATCH""#,
            r#""kind":"UPDATE_PATCH","columns":[]"#,
            r#""kind":"TRUNCATE_AUTHORIZED_TABLE""#,
            r#""kind":"UPSERT","columns":[]"#,
            r#""columns":[]"#,
        ] {
            assert!(decode(refused).is_err(), "{refused}");
        }
        for (ignored, kind) in [
            (
                r#""kind":"DELETE_BY_KEY","columns":[],"reason":"x""#,
                CanonicalOperationKind::DeleteByKey,
            ),
            (
                r#""kind":"INSERT_FULL_ROW","columns":[],"changed_columns":[]"#,
                CanonicalOperationKind::InsertFullRow {
                    columns: Vec::new(),
                },
            ),
        ] {
            assert_eq!(decode(ignored).unwrap().kind, kind, "{ignored}");
        }
    }

    /// A damaged current-format entry and an unreachable PostgreSQL log leave
    /// the state directory as it is: neither asks for `calque destroy`.
    #[test]
    fn only_an_earlier_entry_format_requires_destroying_state() {
        let mut damaged = CanonicalEntry::Transaction(transaction(1)).encode();
        damaged.truncate(damaged.len() - 1);
        let damaged = decode_stored_entry(&damaged, Path::new(".")).unwrap_err();
        assert!(matches!(damaged, LogError::EntryDecode(_)), "{damaged}");
        for error in [damaged, LogError::PostgresUnavailable] {
            assert!(!error.requires_destroying_state(), "{error}");
        }
    }

    #[test]
    fn file_log_rejects_the_previous_format_version() {
        let directory = TempDir::new().unwrap();
        let log = FileCanonicalLog::open(directory.path(), source_id(), epoch()).unwrap();
        let mut manifest = log.manifest.clone();
        manifest.format_version = FORMAT_VERSION - 1;
        write_manifest(directory.path(), &manifest).unwrap();
        drop(log);

        assert!(matches!(
            FileCanonicalLog::open(directory.path(), source_id(), epoch()),
            Err(LogError::ManifestCorrupt(reason)) if reason.contains("unsupported format version")
        ));
    }

    #[test]
    fn duplicate_delivery_has_one_canonical_effect() {
        let directory = TempDir::new().unwrap();
        let mut log = FileCanonicalLog::open(directory.path(), source_id(), epoch()).unwrap();
        let transaction = transaction(7);
        let first = log.append(transaction.clone()).unwrap();
        let duplicate = log.append(transaction).unwrap();

        assert!(matches!(first, AppendOutcome::Appended(_)));
        assert!(matches!(duplicate, AppendOutcome::AlreadyPresent(_)));
        assert_eq!(first.position(), duplicate.position());
        assert_eq!(log.head().sequence, 1);
    }

    #[test]
    fn rejects_noncanonical_primary_keys_before_allocating_a_sequence() {
        let directory = TempDir::new().unwrap();
        let mut log = FileCanonicalLog::open(directory.path(), source_id(), epoch()).unwrap();
        let mut invalid = transaction(1);
        invalid.operations[0].primary_key = vec![1];
        assert!(matches!(
            log.append(invalid),
            Err(LogError::InvalidPrimaryKey(_))
        ));
        assert_eq!(log.head().sequence, 0);
    }

    #[test]
    fn barrier_is_explicit_durable_and_idempotent() {
        let directory = TempDir::new().unwrap();
        let mut log = FileCanonicalLog::open(directory.path(), source_id(), epoch()).unwrap();
        let barrier = barrier(7);
        let first = log.append_barrier(barrier.clone()).unwrap();
        let duplicate = log.append_barrier(barrier.clone()).unwrap();
        assert_eq!(first.position(), duplicate.position());
        drop(log);

        let log = FileCanonicalLog::open(directory.path(), source_id(), epoch()).unwrap();
        assert_eq!(
            log.read_after(0, None).unwrap()[0].entry,
            CanonicalEntry::Barrier(barrier)
        );
    }

    #[test]
    fn transaction_and_barrier_cannot_share_one_postgres_identity() {
        let directory = TempDir::new().unwrap();
        let mut log = FileCanonicalLog::open(directory.path(), source_id(), epoch()).unwrap();
        log.append(transaction(7)).unwrap();
        assert!(matches!(
            log.append_barrier(barrier(7)),
            Err(LogError::DuplicateTransactionConflict)
        ));
    }

    #[test]
    fn startup_recovers_a_durable_segment_left_before_manifest_publish() {
        let directory = TempDir::new().unwrap();
        let log = FileCanonicalLog::open(directory.path(), source_id(), epoch()).unwrap();
        let record = CanonicalRecord {
            position: CursorPosition {
                source_epoch: epoch(),
                sequence: 1,
            },
            entry: CanonicalEntry::Transaction(transaction(1)),
        };
        log.write_segment_with_hook(&record, &mut |_| Ok(()))
            .unwrap();
        drop(log);

        let log = FileCanonicalLog::open(directory.path(), source_id(), epoch()).unwrap();
        assert_eq!(log.head().sequence, 1);
        assert_eq!(log.read_after(0, None).unwrap(), vec![record]);
    }

    #[test]
    fn duplicate_key_with_different_contents_fails_closed() {
        let directory = TempDir::new().unwrap();
        let mut log = FileCanonicalLog::open(directory.path(), source_id(), epoch()).unwrap();
        let original = transaction(7);
        log.append(original.clone()).unwrap();
        let mut conflicting = original;
        conflicting.transaction_id = "different".to_owned();

        assert!(matches!(
            log.append(conflicting),
            Err(LogError::DuplicateTransactionConflict)
        ));
    }

    #[test]
    fn corrupt_segment_is_rejected_on_read_and_reopen() {
        let directory = TempDir::new().unwrap();
        let mut log = FileCanonicalLog::open(directory.path(), source_id(), epoch()).unwrap();
        log.append(transaction(1)).unwrap();
        let segment = directory
            .path()
            .join(SEGMENTS_DIRECTORY)
            .join(segment_file_name(1));
        fs::write(segment, b"corrupt").unwrap();

        assert!(matches!(
            log.read_after(0, None),
            Err(LogError::SegmentCorrupt { sequence: 1, .. })
        ));
        assert!(matches!(
            FileCanonicalLog::open(directory.path(), source_id(), epoch()),
            Err(LogError::SegmentCorrupt { sequence: 1, .. })
        ));
    }

    #[test]
    fn retention_floor_rejects_missing_history_after_reopen() {
        let directory = TempDir::new().unwrap();
        let mut log = FileCanonicalLog::open(directory.path(), source_id(), epoch()).unwrap();
        log.append(transaction(1)).unwrap();
        log.append(transaction(2)).unwrap();
        log.retain_from(2).unwrap();
        drop(log);

        let log = FileCanonicalLog::open(directory.path(), source_id(), epoch()).unwrap();
        assert!(matches!(
            log.read_after(0, None),
            Err(LogError::BeforeRetention {
                requested: 0,
                minimum: 2
            })
        ));
        assert_eq!(log.read_after(1, None).unwrap().len(), 1);
    }

    #[test]
    fn bootstrap_lease_durably_pins_history_until_expiry() {
        let directory = TempDir::new().unwrap();
        let mut log = FileCanonicalLog::open(directory.path(), source_id(), epoch()).unwrap();
        for sequence in 1..=3 {
            log.append(transaction(sequence)).unwrap();
        }
        let plan = PlanId::new("plan-retention-test").unwrap();
        log.acquire_retention_lease(plan.clone(), 2, 2_000, 1_000)
            .unwrap();
        log.acquire_retention_lease(plan, 2, 2_000, 1_001).unwrap();
        log.retain_from_at(4, 1_500).unwrap();
        assert_eq!(log.minimum_retained().sequence, 2);
        assert!(log.read_after(1, Some(3)).is_ok());

        log.retain_from_at(4, 2_000).unwrap();
        assert_eq!(log.minimum_retained().sequence, 4);
        assert!(matches!(
            log.read_after(1, Some(3)),
            Err(LogError::BeforeRetention { minimum: 4, .. })
        ));
    }

    #[test]
    fn snapshot_build_lease_durably_pins_head_plus_one_and_fails_stale_capabilities() {
        let directory = TempDir::new().unwrap();
        let source_id = source_id();
        let epoch = epoch();
        let mut log =
            FileCanonicalLog::open(directory.path(), source_id.clone(), epoch.clone()).unwrap();
        for sequence in 1..=3 {
            log.append(transaction(sequence)).unwrap();
        }
        let build_id = SnapshotBuildId::new("snapshot-build-1").unwrap();
        let owner_id = SnapshotBuildOwnerId::new("snapshot-worker-a").unwrap();
        let lease = log
            .acquire_snapshot_build_retention_at(
                build_id.clone(),
                owner_id.clone(),
                Duration::from_millis(1_000),
                1_000,
            )
            .unwrap();
        assert_eq!(lease.source_id, source_id);
        assert_eq!(lease.source_epoch(), &epoch);
        assert_eq!(lease.build_id, build_id);
        assert_eq!(lease.owner_id, owner_id);
        assert_eq!(lease.token(), 1);
        assert_eq!(lease.minimum_sequence(), 4);
        assert_eq!(lease.expires_at_unix_millis(), 2_000);

        let replacement = log
            .acquire_snapshot_build_retention_at(
                build_id.clone(),
                owner_id.clone(),
                Duration::from_millis(5_000),
                1_100,
            )
            .unwrap();
        assert!(replacement.token() > lease.token());
        assert_eq!(replacement.minimum_sequence(), lease.minimum_sequence());
        assert_eq!(replacement.expires_at_unix_millis(), 6_100);
        assert!(matches!(
            log.renew_snapshot_build_retention_at(&lease, Duration::from_millis(2_000), 1_500,),
            Err(LogError::SnapshotBuildRetentionLeaseRejected)
        ));
        assert!(matches!(
            log.release_snapshot_build_retention_at(&lease, 1_500),
            Err(LogError::SnapshotBuildRetentionLeaseRejected)
        ));
        assert!(matches!(
            log.acquire_snapshot_build_retention_at(
                build_id.clone(),
                SnapshotBuildOwnerId::new("snapshot-worker-b").unwrap(),
                Duration::from_millis(1_000),
                1_100,
            ),
            Err(LogError::SnapshotBuildRetentionLeaseConflict)
        ));

        log.append(transaction(4)).unwrap();
        log.retain_from_at(5, 1_500).unwrap();
        assert_eq!(log.minimum_retained().sequence, 4);
        assert_eq!(log.read_after(3, None).unwrap().len(), 1);
        let renewed = log
            .renew_snapshot_build_retention_at(&replacement, Duration::from_millis(2_000), 1_500)
            .unwrap();
        assert_eq!(renewed.token(), replacement.token());
        assert_eq!(renewed.expires_at_unix_millis(), 3_500);
        drop(log);

        let mut reopened = FileCanonicalLog::open(directory.path(), source_id, epoch).unwrap();
        let mut forged = renewed.clone();
        forged.token += 1;
        assert!(matches!(
            reopened.release_snapshot_build_retention_at(&forged, 1_600),
            Err(LogError::SnapshotBuildRetentionLeaseRejected)
        ));
        let mut wrong_owner = renewed.clone();
        wrong_owner.owner_id = SnapshotBuildOwnerId::new("snapshot-worker-b").unwrap();
        assert!(matches!(
            reopened.renew_snapshot_build_retention_at(
                &wrong_owner,
                Duration::from_millis(100),
                1_600,
            ),
            Err(LogError::SnapshotBuildRetentionLeaseRejected)
        ));
        let mut wrong_epoch = renewed.clone();
        wrong_epoch.source_epoch = SourceEpoch::new("epoch-other").unwrap();
        assert!(matches!(
            reopened.release_snapshot_build_retention_at(&wrong_epoch, 1_600),
            Err(LogError::SnapshotBuildRetentionLeaseRejected)
        ));
        reopened
            .release_snapshot_build_retention_at(&renewed, 1_600)
            .unwrap();
        assert!(matches!(
            reopened.release_snapshot_build_retention_at(&renewed, 1_600),
            Err(LogError::SnapshotBuildRetentionLeaseRejected)
        ));
        reopened.retain_from_at(5, 1_600).unwrap();
        assert_eq!(reopened.minimum_retained().sequence, 5);
    }

    #[test]
    fn expired_snapshot_build_lease_cannot_renew_or_release_and_gets_a_new_token() {
        let directory = TempDir::new().unwrap();
        let mut log = FileCanonicalLog::open(directory.path(), source_id(), epoch()).unwrap();
        log.append(transaction(1)).unwrap();
        let build_id = SnapshotBuildId::new("snapshot-build-expiry").unwrap();
        let owner_id = SnapshotBuildOwnerId::new("snapshot-worker-a").unwrap();
        let expired = log
            .acquire_snapshot_build_retention_at(
                build_id.clone(),
                owner_id.clone(),
                Duration::from_millis(100),
                1_000,
            )
            .unwrap();
        assert!(matches!(
            log.renew_snapshot_build_retention_at(&expired, Duration::from_millis(100), 1_100,),
            Err(LogError::SnapshotBuildRetentionLeaseRejected)
        ));
        assert!(matches!(
            log.release_snapshot_build_retention_at(&expired, 1_100),
            Err(LogError::SnapshotBuildRetentionLeaseRejected)
        ));
        let replacement = log
            .acquire_snapshot_build_retention_at(
                build_id,
                owner_id,
                Duration::from_millis(100),
                1_100,
            )
            .unwrap();
        assert!(replacement.token() > expired.token());
        assert!(matches!(
            log.release_snapshot_build_retention_at(&expired, 1_101),
            Err(LogError::SnapshotBuildRetentionLeaseRejected)
        ));
        assert!(matches!(
            log.acquire_snapshot_build_retention_at(
                SnapshotBuildId::new("zero-ttl").unwrap(),
                SnapshotBuildOwnerId::new("worker").unwrap(),
                Duration::ZERO,
                1_100,
            ),
            Err(LogError::InvalidSnapshotBuildRetentionLease)
        ));
    }

    #[test]
    fn replay_of_retired_transaction_keeps_its_original_canonical_effect() {
        let directory = TempDir::new().unwrap();
        let mut log = FileCanonicalLog::open(directory.path(), source_id(), epoch()).unwrap();
        let original = transaction(1);
        let first = log.append(original.clone()).unwrap();
        log.append(transaction(2)).unwrap();
        log.retain_from(2).unwrap();
        drop(log);

        let mut log = FileCanonicalLog::open(directory.path(), source_id(), epoch()).unwrap();
        let replay = log.append(original.clone()).unwrap();
        assert_eq!(
            replay,
            AppendOutcome::AlreadyPresent(first.position().clone())
        );
        assert_eq!(log.head().sequence, 2);

        let mut conflict = original;
        conflict.transaction_id = "conflict".to_owned();
        assert!(matches!(
            log.append(conflict),
            Err(LogError::DuplicateTransactionConflict)
        ));
    }

    #[test]
    fn source_epoch_change_requires_a_new_log() {
        let directory = TempDir::new().unwrap();
        FileCanonicalLog::open(directory.path(), source_id(), epoch()).unwrap();

        assert!(matches!(
            FileCanonicalLog::open(
                directory.path(),
                source_id(),
                SourceEpoch::new("epoch-2").unwrap()
            ),
            Err(LogError::EpochMismatch { .. })
        ));
    }

    #[test]
    fn append_crash_matrix_recovers_exactly_one_contiguous_effect() {
        let crash_points = [
            DurabilityPoint::SegmentFileSynced,
            DurabilityPoint::SegmentPublished,
            DurabilityPoint::SegmentDirectorySynced,
            DurabilityPoint::ManifestFileSynced,
            DurabilityPoint::ManifestPublished,
            DurabilityPoint::ManifestDirectorySynced,
        ];

        for crash_point in crash_points {
            let directory = TempDir::new().unwrap();
            let expected_transaction = transaction(1);
            let mut log = FileCanonicalLog::open(directory.path(), source_id(), epoch()).unwrap();
            let mut failed = false;
            let result = log.append_with_hook(expected_transaction.clone(), &mut |point| {
                if point == crash_point && !failed {
                    failed = true;
                    return Err(LogError::Io(io::Error::other(format!(
                        "injected crash at {point:?}"
                    ))));
                }
                Ok(())
            });
            assert!(matches!(result, Err(LogError::Io(_))), "{crash_point:?}");
            assert!(failed, "failpoint {crash_point:?} was not reached");
            drop(log);

            let mut reopened =
                FileCanonicalLog::open(directory.path(), source_id(), epoch()).unwrap();
            let segment_was_published = crash_point != DurabilityPoint::SegmentFileSynced;
            assert_eq!(
                reopened.head().sequence,
                u64::from(segment_was_published),
                "{crash_point:?}"
            );

            let retry = reopened.append(expected_transaction.clone()).unwrap();
            assert_eq!(
                matches!(retry, AppendOutcome::AlreadyPresent(_)),
                segment_was_published,
                "{crash_point:?}"
            );
            assert_eq!(reopened.head().sequence, 1, "{crash_point:?}");
            assert_eq!(
                reopened.read_after(0, None).unwrap()[0].entry,
                CanonicalEntry::Transaction(expected_transaction),
                "{crash_point:?}"
            );
        }
    }

    #[test]
    fn failed_append_can_be_retried_without_reopening() {
        for crash_point in [
            DurabilityPoint::SegmentFileSynced,
            DurabilityPoint::ManifestFileSynced,
            DurabilityPoint::ManifestPublished,
            DurabilityPoint::ManifestDirectorySynced,
        ] {
            let directory = TempDir::new().unwrap();
            let expected_transaction = transaction(1);
            let mut log = FileCanonicalLog::open(directory.path(), source_id(), epoch()).unwrap();
            let result = log.append_with_hook(expected_transaction.clone(), &mut |point| {
                if point == crash_point {
                    return Err(LogError::Io(io::Error::other("injected append failure")));
                }
                Ok(())
            });
            assert!(matches!(result, Err(LogError::Io(_))), "{crash_point:?}");

            log.append(expected_transaction.clone()).unwrap();
            assert_eq!(log.head().sequence, 1, "{crash_point:?}");
            assert_eq!(log.read_after(0, None).unwrap().len(), 1);
        }
    }

    #[test]
    fn retention_crash_matrix_never_exposes_deleted_history() {
        for crash_point in [
            DurabilityPoint::RetentionManifestCommitted,
            DurabilityPoint::RetentionFileDeleted(1),
        ] {
            let directory = TempDir::new().unwrap();
            let mut log = FileCanonicalLog::open(directory.path(), source_id(), epoch()).unwrap();
            for sequence in 1..=3 {
                log.append(transaction(sequence)).unwrap();
            }

            let result = log.retain_from_with_hook(3, &mut |point| {
                if point == crash_point {
                    return Err(LogError::Io(io::Error::other("injected retention crash")));
                }
                Ok(())
            });
            assert!(matches!(result, Err(LogError::Io(_))), "{crash_point:?}");
            assert_eq!(log.minimum_retained().sequence, 3);
            assert!(matches!(
                log.append(transaction(3)),
                Ok(AppendOutcome::AlreadyPresent(_))
            ));
            drop(log);

            let reopened = FileCanonicalLog::open(directory.path(), source_id(), epoch()).unwrap();
            assert_eq!(reopened.minimum_retained().sequence, 3);
            assert!(matches!(
                reopened.read_after(1, None),
                Err(LogError::BeforeRetention {
                    requested: 1,
                    minimum: 3
                })
            ));
            let records = reopened.read_after(2, None).unwrap();
            assert_eq!(records.len(), 1);
            assert_eq!(records[0].position.sequence, 3);
        }
    }

    #[test]
    fn retention_manifest_crash_reopens_an_old_or_new_complete_index() {
        for crash_point in [
            DurabilityPoint::ManifestFileSynced,
            DurabilityPoint::ManifestPublished,
            DurabilityPoint::ManifestDirectorySynced,
        ] {
            let directory = TempDir::new().unwrap();
            let mut log = FileCanonicalLog::open(directory.path(), source_id(), epoch()).unwrap();
            for sequence in 1..=3 {
                log.append(transaction(sequence)).unwrap();
            }

            let result = log.retain_from_with_hook(3, &mut |point| {
                if point == crash_point {
                    return Err(LogError::Io(io::Error::other(
                        "injected retention manifest crash",
                    )));
                }
                Ok(())
            });
            assert!(matches!(result, Err(LogError::Io(_))), "{crash_point:?}");
            drop(log);

            let reopened = FileCanonicalLog::open(directory.path(), source_id(), epoch()).unwrap();
            let manifest_was_published = crash_point != DurabilityPoint::ManifestFileSynced;
            assert_eq!(
                reopened.minimum_retained().sequence,
                if manifest_was_published { 3 } else { 1 },
                "{crash_point:?}"
            );
            assert_eq!(
                reopened
                    .read_after(if manifest_was_published { 2 } else { 0 }, None)
                    .unwrap()
                    .len(),
                if manifest_was_published { 1 } else { 3 },
                "{crash_point:?}"
            );
        }
    }

    #[test]
    fn failed_retention_manifest_write_keeps_live_index_unchanged() {
        let directory = TempDir::new().unwrap();
        let mut log = FileCanonicalLog::open(directory.path(), source_id(), epoch()).unwrap();
        for sequence in 1..=3 {
            log.append(transaction(sequence)).unwrap();
        }

        let result = log.retain_from_with_hook(3, &mut |point| {
            if point == DurabilityPoint::ManifestFileSynced {
                return Err(LogError::Io(io::Error::other(
                    "injected manifest publication failure",
                )));
            }
            Ok(())
        });
        assert!(matches!(result, Err(LogError::Io(_))));
        assert_eq!(log.minimum_retained().sequence, 1);
        assert!(matches!(
            log.append(transaction(1)),
            Ok(AppendOutcome::AlreadyPresent(_))
        ));

        log.retain_from(3).unwrap();
        assert_eq!(log.minimum_retained().sequence, 3);
    }

    #[test]
    fn startup_rejects_an_unindexed_sequence_gap() {
        let directory = TempDir::new().unwrap();
        let mut log = FileCanonicalLog::open(directory.path(), source_id(), epoch()).unwrap();
        log.append(transaction(1)).unwrap();
        let record = CanonicalRecord {
            position: CursorPosition {
                source_epoch: epoch(),
                sequence: 3,
            },
            entry: CanonicalEntry::Transaction(transaction(3)),
        };
        log.write_segment_with_hook(&record, &mut |_| Ok(()))
            .unwrap();
        drop(log);

        assert!(matches!(
            FileCanonicalLog::open(directory.path(), source_id(), epoch()),
            Err(LogError::SequenceGap {
                expected: 2,
                actual: 3
            })
        ));
    }

    #[test]
    fn startup_rejects_noncanonical_manifest_paths() {
        let directory = TempDir::new().unwrap();
        let mut log = FileCanonicalLog::open(directory.path(), source_id(), epoch()).unwrap();
        log.append(transaction(1)).unwrap();
        let mut manifest = log.manifest.clone();
        manifest.segments[0].file_name = "../manifest.json".to_owned();
        write_manifest(directory.path(), &manifest).unwrap();
        drop(log);

        assert!(matches!(
            FileCanonicalLog::open(directory.path(), source_id(), epoch()),
            Err(LogError::ManifestCorrupt(reason)) if reason.contains("non-canonical file name")
        ));
    }

    /// A byte budget that runs out *mid-page* must end the page at the last
    /// record that fits, not refuse the read and not overrun the budget.
    #[test]
    fn a_bounded_read_ends_the_page_at_the_last_record_that_fits() {
        let directory = TempDir::new().unwrap();
        let mut log = FileCanonicalLog::open(directory.path(), source_id(), epoch()).unwrap();
        log.append(transaction(1)).unwrap();
        log.append(transaction(2)).unwrap();

        // A one-byte budget cannot hold the first record, and the refusal
        // reports its cost: the budget that leaves only the second outside.
        let first_record_bytes = match log.read_after_bounded(0, None, 2, 1) {
            Err(LogError::ReadLimitTooSmall {
                next_record_bytes, ..
            }) => next_record_bytes,
            other => panic!("expected ReadLimitTooSmall, got {other:?}"),
        };

        let page = log
            .read_after_bounded(0, None, 2, first_record_bytes)
            .unwrap();
        assert_eq!(page.len(), 1);
        assert_eq!(page[0].position.sequence, 1);

        // The short page is a resumable fence, not a truncation.
        let rest = log
            .read_after_bounded(1, None, 2, MAX_RECORD_BYTES)
            .unwrap();
        assert_eq!(rest.len(), 1);
        assert_eq!(rest[0].position.sequence, 2);
    }

    /// Retention is a promise about what has already been destroyed, so the
    /// floor may not walk backwards: the segments below it are gone, and a
    /// floor that claims otherwise offers history the log cannot produce.
    #[test]
    fn a_retention_floor_never_walks_backwards() {
        let directory = TempDir::new().unwrap();
        let mut log = FileCanonicalLog::open(directory.path(), source_id(), epoch()).unwrap();
        log.append(transaction(1)).unwrap();
        log.append(transaction(2)).unwrap();
        log.append(transaction(3)).unwrap();
        log.retain_from(3).unwrap();

        // Accepted, but it must not lower the floor.
        log.retain_from(2).unwrap();

        assert!(matches!(
            log.read_after(1, None),
            Err(LogError::BeforeRetention {
                requested: 1,
                minimum: 3
            })
        ));
    }

    /// A read must stop at the fence it was given, not at the head. A bootstrap
    /// plan freezes its fence at issuance while ingest keeps appending, so
    /// reading to the head would hand a client transactions from beyond its
    /// activation fence.
    #[test]
    fn a_read_stops_at_its_fence_rather_than_the_head() {
        let directory = TempDir::new().unwrap();
        let mut log = FileCanonicalLog::open(directory.path(), source_id(), epoch()).unwrap();
        log.append(transaction(1)).unwrap();
        log.append(transaction(2)).unwrap();
        log.append(transaction(3)).unwrap();

        let fenced = log
            .read_after_bounded(0, Some(2), 10, MAX_RECORD_BYTES)
            .unwrap();
        assert_eq!(fenced.len(), 2);
        assert_eq!(fenced[0].position.sequence, 1);
        assert_eq!(fenced[1].position.sequence, 2);

        // The fence bounds the page; it does not retire what lies beyond it.
        let unfenced = log
            .read_after_bounded(0, None, 10, MAX_RECORD_BYTES)
            .unwrap();
        assert_eq!(unfenced.len(), 3);
        assert_eq!(unfenced[2].position.sequence, 3);
    }

    /// A lease may not pin history that retention has already destroyed.
    ///
    /// One open log with no reopen: `validate_and_index` re-checks leases on
    /// open and would mask a missing guard.
    #[test]
    fn a_retention_lease_may_not_pin_history_below_the_floor() {
        let directory = TempDir::new().unwrap();
        let mut log = FileCanonicalLog::open(directory.path(), source_id(), epoch()).unwrap();
        for sequence in 1..=3 {
            log.append(transaction(sequence)).unwrap();
        }
        log.retain_from(3).unwrap();

        let plan = PlanId::new("plan-below-floor").unwrap();
        assert!(matches!(
            log.acquire_retention_lease(plan, 1, 2_000, 1_000),
            Err(LogError::InvalidRetentionLease)
        ));

        // The floor itself is still a legal pin, so the guard is a floor and
        // not an outright refusal.
        let at_floor = PlanId::new("plan-at-floor").unwrap();
        log.acquire_retention_lease(at_floor, 3, 2_000, 1_000)
            .unwrap();
    }
}
