use std::sync::Arc;

use serde::{Deserialize, Serialize};
use thiserror::Error;

pub const CUT_MARKER_PREFIX: &str = "sync.snapshot_cut.v1";
pub const MUTATION_MARKER_PREFIX: &str = "sync.mutation.v1";

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct PgLsn(u64);

impl PgLsn {
    pub const fn new(value: u64) -> Self {
        Self(value)
    }

    pub const fn get(self) -> u64 {
        self.0
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DecoderLimits {
    pub max_message_bytes: usize,
    pub max_relations: usize,
    pub max_types: usize,
    pub max_columns_per_relation: usize,
    pub max_identifier_bytes: usize,
    pub max_tuple_bytes: usize,
    pub max_logical_message_bytes: usize,
    pub max_changes_per_transaction: usize,
    pub max_transaction_bytes: usize,
    /// Transactions remain in memory only through this estimated byte count.
    /// Larger transactions spill decoded changes into an anonymous local file.
    pub max_in_memory_transaction_bytes: usize,
    /// Hard upper bound for the local binary encoding of one spilled transaction.
    pub max_transaction_spool_bytes: usize,
    pub max_active_streamed_transactions: usize,
    pub max_total_buffered_transaction_bytes: usize,
    /// Source-wide steady-state logical charge for active spill files.
    ///
    /// Rewriting one transaction after a streamed-subtransaction abort keeps
    /// the old anonymous file until the replacement is synchronized. Physical
    /// disk use is therefore bounded by this value plus
    /// `max_transaction_spool_bytes` during that operation.
    pub max_total_spool_bytes: usize,
}

impl Default for DecoderLimits {
    fn default() -> Self {
        Self {
            max_message_bytes: 16 * 1024 * 1024,
            max_relations: 16_384,
            max_types: 16_384,
            max_columns_per_relation: 1_600,
            max_identifier_bytes: 63,
            max_tuple_bytes: 8 * 1024 * 1024,
            max_logical_message_bytes: 1024 * 1024,
            max_changes_per_transaction: 100_000,
            max_transaction_bytes: 64 * 1024 * 1024,
            max_in_memory_transaction_bytes: 1024 * 1024,
            max_transaction_spool_bytes: 128 * 1024 * 1024,
            max_active_streamed_transactions: 8,
            max_total_buffered_transaction_bytes: 128 * 1024 * 1024,
            max_total_spool_bytes: 256 * 1024 * 1024,
        }
    }
}

impl DecoderLimits {
    pub(crate) fn validate(&self) -> Result<(), DecodeError> {
        if self.max_message_bytes == 0
            || self.max_relations == 0
            || self.max_types == 0
            || self.max_columns_per_relation == 0
            || self.max_identifier_bytes == 0
            || self.max_tuple_bytes == 0
            || self.max_logical_message_bytes == 0
            || self.max_changes_per_transaction == 0
            || self.max_transaction_bytes == 0
            || self.max_in_memory_transaction_bytes == 0
            || self.max_in_memory_transaction_bytes > self.max_transaction_bytes
            || self.max_transaction_spool_bytes < self.max_transaction_bytes
            || self.max_active_streamed_transactions == 0
            || self.max_total_buffered_transaction_bytes < self.max_transaction_bytes
            || self.max_total_spool_bytes < self.max_transaction_spool_bytes
        {
            return Err(DecodeError::InvalidLimits);
        }
        Ok(())
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum ReplicaIdentity {
    Default,
    Nothing,
    Full,
    Index,
}

impl ReplicaIdentity {
    pub(crate) fn from_pgoutput(value: u8) -> Result<Self, DecodeError> {
        match value {
            b'd' => Ok(Self::Default),
            b'n' => Ok(Self::Nothing),
            b'f' => Ok(Self::Full),
            b'i' => Ok(Self::Index),
            other => Err(DecodeError::UnknownReplicaIdentity(other)),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RelationColumn {
    pub name: String,
    pub type_oid: u32,
    pub type_modifier: i32,
    pub is_key: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RelationMetadata {
    pub relation_oid: u32,
    pub namespace: String,
    pub relation_name: String,
    pub replica_identity: ReplicaIdentity,
    pub columns: Vec<RelationColumn>,
    pub layout_generation: u64,
    pub first_observed_wal_lsn: PgLsn,
    pub last_observed_wal_lsn: PgLsn,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TypeMetadata {
    pub type_oid: u32,
    pub namespace: String,
    pub type_name: String,
    pub first_observed_wal_lsn: PgLsn,
    pub last_observed_wal_lsn: PgLsn,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum CellValue {
    Null,
    Text(Vec<u8>),
    Binary(Vec<u8>),
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum TupleCell {
    /// PostgreSQL's `u` tuple tag: the prior value is unchanged and absent.
    Absent,
    Value(CellValue),
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RowImage {
    pub cells: Vec<CellValue>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TupleImage {
    pub cells: Vec<TupleCell>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RowPatch {
    /// One entry per relation column. `Absent` is distinct from explicit null.
    pub cells: Vec<TupleCell>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum OldTupleKind {
    Key,
    Full,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum LogicalMessageKind {
    CutMarker,
    MutationMarker,
    Other,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct LogicalMessage {
    pub message_lsn: PgLsn,
    pub prefix: String,
    pub content: Vec<u8>,
    pub kind: LogicalMessageKind,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum DecodedChange {
    Insert {
        relation: Arc<RelationMetadata>,
        row: RowImage,
    },
    Update {
        relation: Arc<RelationMetadata>,
        old_tuple: Option<TupleImage>,
        old_tuple_kind: Option<OldTupleKind>,
        patch: RowPatch,
    },
    Delete {
        relation: Arc<RelationMetadata>,
        old_tuple: TupleImage,
        old_tuple_kind: OldTupleKind,
    },
    Truncate {
        relations: Vec<Arc<RelationMetadata>>,
        cascade: bool,
        restart_identity: bool,
    },
    LogicalMessage(LogicalMessage),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TransactionMode {
    Normal,
    Streamed,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DecodedTransaction {
    pub xid: u32,
    pub mode: TransactionMode,
    pub first_observed_wal_lsn: PgLsn,
    /// The final LSN from `Begin`; absent for protocol-v2 streamed transactions.
    pub final_lsn: Option<PgLsn>,
    pub commit_lsn: PgLsn,
    pub end_lsn: PgLsn,
    /// PostgreSQL microseconds since 2000-01-01, preserved without conversion.
    pub commit_time_postgres_micros: i64,
    pub changes: Vec<DecodedChange>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum DecoderEvent {
    RelationObserved {
        relation: Arc<RelationMetadata>,
        layout_changed: bool,
    },
    TypeObserved(Arc<TypeMetadata>),
    Origin {
        origin_lsn: PgLsn,
        origin_name: String,
    },
    StandaloneLogicalMessage(LogicalMessage),
    TransactionCommitted(DecodedTransaction),
    StreamAborted {
        xid: u32,
        subtransaction_xid: u32,
    },
}

/// One server-to-client `CopyData` frame from logical replication.
///
/// The outer replication envelope is retained separately from the decoded
/// `pgoutput` events so callers can report received WAL and answer primary
/// keepalives without deriving positions from transaction contents.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ReplicationCopyData {
    Wal {
        start_lsn: PgLsn,
        wal_end: PgLsn,
        send_time_postgres_micros: i64,
        events: Vec<DecoderEvent>,
    },
    Keepalive {
        wal_end: PgLsn,
        send_time_postgres_micros: i64,
        reply_requested: bool,
    },
}

#[derive(Debug, Error, PartialEq, Eq)]
pub enum DecodeError {
    #[error("decoder limits must all be nonzero")]
    InvalidLimits,
    #[error("pgoutput protocol version {0} is unsupported; use version 1 or 2")]
    UnsupportedProtocolVersion(u32),
    #[error("decoder is poisoned after an earlier protocol or limit failure")]
    Poisoned,
    #[error("pg_walstream rejected the message: {0}")]
    PgWalstream(String),
    #[error("pgoutput message is not the canonical byte sequence represented by pg_walstream")]
    NonCanonicalMessage,
    #[error("message is {actual} bytes, limit is {limit}")]
    MessageTooLarge { actual: usize, limit: usize },
    #[error("logical replication CopyData frame is {actual} bytes, limit is {limit}")]
    CopyDataTooLarge { actual: usize, limit: usize },
    #[error("logical replication CopyData frame is empty")]
    EmptyCopyData,
    #[error("logical replication CopyData uses unknown tag {0:#04x}")]
    UnknownCopyDataTag(u8),
    #[error("XLogData frame is {actual} bytes, minimum is 25")]
    XLogDataFrameTooShort { actual: usize },
    #[error("primary keepalive frame must be exactly 18 bytes, got {actual}")]
    KeepaliveFrameLength { actual: usize },
    #[error("relation registry limit {limit} exceeded")]
    TooManyRelations { limit: usize },
    #[error("type registry limit {limit} exceeded")]
    TooManyTypes { limit: usize },
    #[error("relation {relation_oid} has {actual} columns, limit is {limit}")]
    TooManyColumns {
        relation_oid: u32,
        actual: usize,
        limit: usize,
    },
    #[error("{field} exceeds identifier limit {limit} bytes")]
    IdentifierTooLong { field: &'static str, limit: usize },
    #[error("relation {relation_oid} column {column_index} uses unknown flags {flags:#04x}")]
    UnknownColumnFlags {
        relation_oid: u32,
        column_index: usize,
        flags: u8,
    },
    #[error("unknown replica identity byte {0:#04x}")]
    UnknownReplicaIdentity(u8),
    #[error("relation {0} has not been observed")]
    UnknownRelation(u32),
    #[error("relation {relation_oid} tuple has {actual} columns, expected {expected}")]
    TupleLayoutMismatch {
        relation_oid: u32,
        expected: usize,
        actual: usize,
    },
    #[error("relation {relation_oid} tuple is {actual} bytes, limit is {limit}")]
    TupleTooLarge {
        relation_oid: u32,
        actual: usize,
        limit: usize,
    },
    #[error("insert for relation {relation_oid} contains an absent/unchanged column")]
    IncompleteInsert { relation_oid: u32 },
    #[error("unknown tuple data tag {0:#04x}")]
    UnknownTupleDataTag(u8),
    #[error("unknown old tuple discriminator {0:?}")]
    UnknownOldTupleKind(Option<char>),
    #[error("truncate uses unknown flags {0:#04x}")]
    UnknownTruncateFlags(u8),
    #[error("commit uses unsupported flags {0:#04x}")]
    UnknownCommitFlags(u8),
    #[error("logical message uses unsupported flags {0:#04x}")]
    UnknownLogicalMessageFlags(u8),
    #[error("logical message is {actual} bytes, limit is {limit}")]
    LogicalMessageTooLarge { actual: usize, limit: usize },
    #[error("reserved marker prefix {0} must be transactional")]
    NonTransactionalReservedMarker(String),
    #[error("transactional message arrived outside a transaction")]
    TransactionalMessageOutsideTransaction,
    #[error("normal transaction {0} began while another normal transaction is active")]
    NormalTransactionAlreadyActive(u32),
    #[error("normal commit arrived without a begin")]
    CommitWithoutBegin,
    #[error("normal transaction begin/commit timestamps differ")]
    CommitTimestampMismatch,
    #[error("transactional change arrived outside a transaction")]
    ChangeOutsideTransaction,
    #[error("streamed message xid/context is inconsistent")]
    InvalidStreamingContext,
    #[error("stream segment started while xid {0} is still active")]
    StreamSegmentAlreadyActive(u32),
    #[error("streamed transaction {0} started more than once")]
    DuplicateStreamedTransaction(u32),
    #[error("streamed transaction {0} was not previously started")]
    UnknownStreamedTransaction(u32),
    #[error("active streamed transaction limit {limit} exceeded")]
    TooManyActiveStreamedTransactions { limit: usize },
    #[error("stream stop arrived without an active segment")]
    StreamStopWithoutStart,
    #[error("stream commit arrived before the active segment stopped")]
    StreamCommitDuringSegment,
    #[error("stream abort arrived while a streamed segment is active")]
    StreamAbortDuringSegment,
    #[error(
        "streamed subtransaction {subtransaction_xid} inside transaction {xid} aborted more than once"
    )]
    DuplicateStreamedSubtransactionAbort { xid: u32, subtransaction_xid: u32 },
    #[error(
        "streamed data for aborted subtransaction {subtransaction_xid} inside transaction {xid}"
    )]
    DataForAbortedStreamedSubtransaction { xid: u32, subtransaction_xid: u32 },
    #[error("interleaved streamed {kind} metadata for oid {oid} conflicts at commit")]
    StreamedMetadataConflict { kind: &'static str, oid: u32 },
    #[error("committed change references relation {relation_oid} metadata that was aborted")]
    AbortedStreamedMetadataReferenced { relation_oid: u32 },
    #[error("transaction {xid} exceeds change limit {limit}")]
    TooManyTransactionChanges { xid: u32, limit: usize },
    #[error("transaction {xid} exceeds byte limit {limit}")]
    TransactionTooLarge { xid: u32, limit: usize },
    #[error("decoder buffered-transaction bytes exceed source-wide limit {limit}")]
    TotalBufferedTransactionsTooLarge { limit: usize },
    #[error("transaction {xid} spill encoding exceeds byte limit {limit}")]
    TransactionSpoolTooLarge { xid: u32, limit: usize },
    #[error("decoder spill files exceed source-wide byte limit {limit}")]
    TotalSpoolTooLarge { limit: usize },
    #[error("transaction spill I/O failed: {0}")]
    SpoolIo(String),
    #[error("transaction spill encoding failed: {0}")]
    SpoolEncode(String),
    #[error("transaction spill decoding failed: {0}")]
    SpoolDecode(String),
    #[error("transaction spill contains trailing bytes")]
    SpoolTrailingBytes,
    #[error("two-phase pgoutput messages are disabled in version 1")]
    TwoPhaseUnsupported,
    #[error("unexpected streaming envelope for this pgoutput message")]
    UnexpectedStreamingEnvelope,
}
