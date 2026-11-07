use std::{
    collections::BTreeMap,
    fs::File,
    io::{Read, Seek, SeekFrom, Write},
    sync::Arc,
};

use bytes::Bytes;
use pg_walstream::{
    encode_streaming_message_to_bytes, parse_keepalive_message, BufferReader, ColumnData,
    ColumnInfo, LogicalReplicationMessage, LogicalReplicationParser, StreamingReplicationMessage,
    TupleData,
};
use serde::{Deserialize, Serialize};

use crate::model::{
    CellValue, DecodeError, DecodedChange, DecodedTransaction, DecoderEvent, DecoderLimits,
    LogicalMessage, LogicalMessageKind, OldTupleKind, PgLsn, RelationColumn, RelationMetadata,
    ReplicaIdentity, ReplicationCopyData, RowImage, RowPatch, TransactionMode, TupleCell,
    TupleImage, TypeMetadata, CUT_MARKER_PREFIX, MUTATION_MARKER_PREFIX,
};

const TRUNCATE_CASCADE: u8 = 1;
const TRUNCATE_RESTART_IDENTITY: u8 = 2;
const MAX_SPOOL_DECODE_BYTES: usize = 256 * 1024 * 1024;
const SPOOL_LENGTH_BYTES: usize = 4;
const ABORTED_SUBTRANSACTION_ESTIMATED_BYTES: usize = std::mem::size_of::<u32>();

struct PendingTransaction {
    xid: u32,
    mode: TransactionMode,
    first_observed_wal_lsn: PgLsn,
    final_lsn: Option<PgLsn>,
    begin_timestamp: Option<i64>,
    changes: ChangeStore,
    metadata: Vec<OwnedMetadata>,
    aborted_subtransactions: BTreeMap<u32, ()>,
    estimated_bytes: usize,
}

enum OwnedMetadata {
    Relation {
        owner_xid: u32,
        estimated_bytes: usize,
        predecessor: Option<Arc<RelationMetadata>>,
        relation: Arc<RelationMetadata>,
        layout_changed: bool,
    },
    Type {
        owner_xid: u32,
        estimated_bytes: usize,
        predecessor: Option<Arc<TypeMetadata>>,
        metadata: Arc<TypeMetadata>,
    },
    Origin {
        owner_xid: u32,
        estimated_bytes: usize,
        origin_lsn: PgLsn,
        origin_name: String,
    },
}

impl OwnedMetadata {
    fn owner_xid(&self) -> u32 {
        match self {
            Self::Relation { owner_xid, .. }
            | Self::Type { owner_xid, .. }
            | Self::Origin { owner_xid, .. } => *owner_xid,
        }
    }

    fn estimated_bytes(&self) -> usize {
        match self {
            Self::Relation {
                estimated_bytes, ..
            }
            | Self::Type {
                estimated_bytes, ..
            }
            | Self::Origin {
                estimated_bytes, ..
            } => *estimated_bytes,
        }
    }
}

#[derive(Serialize, Deserialize)]
struct BufferedChange {
    owner_xid: u32,
    estimated_bytes: usize,
    change: DecodedChange,
}

enum ChangeStore {
    Memory(Vec<BufferedChange>),
    Spool {
        file: File,
        changes: usize,
        bytes: usize,
        owners: BTreeMap<u32, OwnerStats>,
    },
}

#[derive(Clone, Copy, Default)]
struct OwnerStats {
    changes: usize,
    estimated_bytes: usize,
}

impl OwnerStats {
    fn add(&mut self, entry: &BufferedChange) {
        self.changes = self.changes.saturating_add(1);
        self.estimated_bytes = self.estimated_bytes.saturating_add(entry.estimated_bytes);
    }
}

impl ChangeStore {
    fn new() -> Self {
        Self::Memory(Vec::new())
    }

    fn spool_bytes(&self) -> usize {
        match self {
            Self::Memory(_) => 0,
            Self::Spool { bytes, .. } => *bytes,
        }
    }

    fn len(&self) -> usize {
        match self {
            Self::Memory(changes) => changes.len(),
            Self::Spool { changes, .. } => *changes,
        }
    }

    fn owner_stats(&self, owner_xid: u32) -> OwnerStats {
        match self {
            Self::Memory(changes) => {
                let mut stats = OwnerStats::default();
                for entry in changes.iter().filter(|entry| entry.owner_xid == owner_xid) {
                    stats.add(entry);
                }
                stats
            }
            Self::Spool { owners, .. } => owners.get(&owner_xid).copied().unwrap_or_default(),
        }
    }

    fn push(
        &mut self,
        xid: u32,
        entry: BufferedChange,
        next_estimated_bytes: usize,
        source_spool_remaining: usize,
        limits: &DecoderLimits,
    ) -> Result<usize, DecodeError> {
        match self {
            Self::Memory(changes)
                if next_estimated_bytes <= limits.max_in_memory_transaction_bytes =>
            {
                changes.push(entry);
                Ok(0)
            }
            Self::Memory(changes) => {
                let prior = std::mem::take(changes);
                let mut file = tempfile::tempfile()
                    .map_err(|error| DecodeError::SpoolIo(error.to_string()))?;
                let mut spool_bytes = 0_usize;
                let mut spool_remaining = source_spool_remaining;
                let change_count = prior.len().saturating_add(1);
                let mut owners = BTreeMap::<u32, OwnerStats>::new();
                for item in prior.iter().chain(std::iter::once(&entry)) {
                    let previous = spool_bytes;
                    spool_bytes = encode_spooled_change(
                        &mut file,
                        item,
                        xid,
                        spool_bytes,
                        spool_remaining,
                        limits,
                    )?;
                    spool_remaining = spool_remaining.saturating_sub(spool_bytes - previous);
                    owners.entry(item.owner_xid).or_default().add(item);
                }
                *self = Self::Spool {
                    file,
                    changes: change_count,
                    bytes: spool_bytes,
                    owners,
                };
                Ok(spool_bytes)
            }
            Self::Spool {
                file,
                changes,
                bytes,
                owners,
            } => {
                let previous = *bytes;
                *bytes = encode_spooled_change(
                    file,
                    &entry,
                    xid,
                    previous,
                    source_spool_remaining,
                    limits,
                )?;
                *changes = changes.saturating_add(1);
                owners.entry(entry.owner_xid).or_default().add(&entry);
                Ok(*bytes - previous)
            }
        }
    }

    fn into_changes(self) -> Result<Vec<DecodedChange>, DecodeError> {
        match self {
            Self::Memory(changes) => Ok(changes.into_iter().map(|entry| entry.change).collect()),
            Self::Spool {
                mut file, changes, ..
            } => {
                file.sync_data()
                    .map_err(|error| DecodeError::SpoolIo(error.to_string()))?;
                file.seek(SeekFrom::Start(0))
                    .map_err(|error| DecodeError::SpoolIo(error.to_string()))?;
                let mut decoded = Vec::with_capacity(changes);
                for _ in 0..changes {
                    let mut length = [0_u8; SPOOL_LENGTH_BYTES];
                    file.read_exact(&mut length)
                        .map_err(|error| DecodeError::SpoolIo(error.to_string()))?;
                    let length = usize::try_from(u32::from_be_bytes(length))
                        .map_err(|error| DecodeError::SpoolDecode(error.to_string()))?;
                    if length == 0 || length > MAX_SPOOL_DECODE_BYTES {
                        return Err(DecodeError::SpoolDecode(format!(
                            "framed change length {length} exceeds 1..={MAX_SPOOL_DECODE_BYTES}"
                        )));
                    }
                    let mut encoded = vec![0_u8; length];
                    file.read_exact(&mut encoded)
                        .map_err(|error| DecodeError::SpoolIo(error.to_string()))?;
                    let entry: BufferedChange = postcard::from_bytes(&encoded)
                        .map_err(|error| DecodeError::SpoolDecode(error.to_string()))?;
                    decoded.push(entry.change);
                }
                let mut trailing = [0_u8; 1];
                if file
                    .read(&mut trailing)
                    .map_err(|error| DecodeError::SpoolIo(error.to_string()))?
                    != 0
                {
                    return Err(DecodeError::SpoolTrailingBytes);
                }
                Ok(decoded)
            }
        }
    }

    fn rollback_owner(
        &mut self,
        xid: u32,
        owner_xid: u32,
        limits: &DecoderLimits,
    ) -> Result<RollbackStats, DecodeError> {
        match self {
            Self::Memory(changes) => {
                let mut removed = RollbackStats::default();
                changes.retain(|entry| {
                    if entry.owner_xid == owner_xid {
                        removed.changes = removed.changes.saturating_add(1);
                        removed.estimated_bytes = removed
                            .estimated_bytes
                            .saturating_add(entry.estimated_bytes);
                        false
                    } else {
                        true
                    }
                });
                Ok(removed)
            }
            Self::Spool {
                file,
                changes,
                bytes,
                owners,
            } => {
                if !owners.contains_key(&owner_xid) {
                    return Ok(RollbackStats::default());
                }
                file.sync_data()
                    .map_err(|error| DecodeError::SpoolIo(error.to_string()))?;
                file.seek(SeekFrom::Start(0))
                    .map_err(|error| DecodeError::SpoolIo(error.to_string()))?;
                let mut replacement = tempfile::tempfile()
                    .map_err(|error| DecodeError::SpoolIo(error.to_string()))?;
                let mut retained_changes = 0_usize;
                let mut retained_bytes = 0_usize;
                let mut removed = RollbackStats::default();

                for _ in 0..*changes {
                    let entry = read_spooled_change(file)?;
                    if entry.owner_xid == owner_xid {
                        removed.changes = removed.changes.saturating_add(1);
                        removed.estimated_bytes = removed
                            .estimated_bytes
                            .saturating_add(entry.estimated_bytes);
                    } else {
                        retained_bytes = encode_spooled_change(
                            &mut replacement,
                            &entry,
                            xid,
                            retained_bytes,
                            limits.max_transaction_spool_bytes,
                            limits,
                        )?;
                        retained_changes = retained_changes.saturating_add(1);
                    }
                }
                let mut trailing = [0_u8; 1];
                if file
                    .read(&mut trailing)
                    .map_err(|error| DecodeError::SpoolIo(error.to_string()))?
                    != 0
                {
                    return Err(DecodeError::SpoolTrailingBytes);
                }
                replacement
                    .sync_data()
                    .map_err(|error| DecodeError::SpoolIo(error.to_string()))?;
                removed.spool_bytes = bytes.saturating_sub(retained_bytes);
                *file = replacement;
                *changes = retained_changes;
                *bytes = retained_bytes;
                owners.remove(&owner_xid);
                Ok(removed)
            }
        }
    }
}

#[derive(Default)]
struct RollbackStats {
    changes: usize,
    estimated_bytes: usize,
    spool_bytes: usize,
    added_estimated_bytes: usize,
}

fn read_spooled_change(file: &mut File) -> Result<BufferedChange, DecodeError> {
    let mut length = [0_u8; SPOOL_LENGTH_BYTES];
    file.read_exact(&mut length)
        .map_err(|error| DecodeError::SpoolIo(error.to_string()))?;
    let length = usize::try_from(u32::from_be_bytes(length))
        .map_err(|error| DecodeError::SpoolDecode(error.to_string()))?;
    if length == 0 || length > MAX_SPOOL_DECODE_BYTES {
        return Err(DecodeError::SpoolDecode(format!(
            "framed change length {length} exceeds 1..={MAX_SPOOL_DECODE_BYTES}"
        )));
    }
    let mut encoded = vec![0_u8; length];
    file.read_exact(&mut encoded)
        .map_err(|error| DecodeError::SpoolIo(error.to_string()))?;
    postcard::from_bytes(&encoded).map_err(|error| DecodeError::SpoolDecode(error.to_string()))
}

fn encode_spooled_change(
    file: &mut File,
    change: &BufferedChange,
    xid: u32,
    current_bytes: usize,
    source_spool_remaining: usize,
    limits: &DecoderLimits,
) -> Result<usize, DecodeError> {
    let encoded =
        postcard::to_stdvec(change).map_err(|error| DecodeError::SpoolEncode(error.to_string()))?;
    let encoded_length =
        u32::try_from(encoded.len()).map_err(|_| DecodeError::TransactionSpoolTooLarge {
            xid,
            limit: limits.max_transaction_spool_bytes,
        })?;
    let framed_length = SPOOL_LENGTH_BYTES.checked_add(encoded.len()).ok_or(
        DecodeError::TransactionSpoolTooLarge {
            xid,
            limit: limits.max_transaction_spool_bytes,
        },
    )?;
    let next =
        current_bytes
            .checked_add(framed_length)
            .ok_or(DecodeError::TransactionSpoolTooLarge {
                xid,
                limit: limits.max_transaction_spool_bytes,
            })?;
    if next > limits.max_transaction_spool_bytes {
        return Err(DecodeError::TransactionSpoolTooLarge {
            xid,
            limit: limits.max_transaction_spool_bytes,
        });
    }
    if framed_length > source_spool_remaining {
        return Err(DecodeError::TotalSpoolTooLarge {
            limit: limits.max_total_spool_bytes,
        });
    }
    file.write_all(&encoded_length.to_be_bytes())
        .and_then(|()| file.write_all(&encoded))
        .map_err(|error| DecodeError::SpoolIo(error.to_string()))?;
    Ok(next)
}

impl PendingTransaction {
    fn push(
        &mut self,
        owner_xid: u32,
        change: DecodedChange,
        estimated_bytes: usize,
        source_spool_remaining: usize,
        limits: &DecoderLimits,
    ) -> Result<usize, DecodeError> {
        if self
            .changes
            .len()
            .saturating_add(self.metadata.len())
            .saturating_add(self.aborted_subtransactions.len())
            >= limits.max_changes_per_transaction
        {
            return Err(DecodeError::TooManyTransactionChanges {
                xid: self.xid,
                limit: limits.max_changes_per_transaction,
            });
        }
        let next = self.estimated_bytes.checked_add(estimated_bytes).ok_or(
            DecodeError::TransactionTooLarge {
                xid: self.xid,
                limit: limits.max_transaction_bytes,
            },
        )?;
        if next > limits.max_transaction_bytes {
            return Err(DecodeError::TransactionTooLarge {
                xid: self.xid,
                limit: limits.max_transaction_bytes,
            });
        }
        let spool_delta = self.changes.push(
            self.xid,
            BufferedChange {
                owner_xid,
                estimated_bytes,
                change,
            },
            next,
            source_spool_remaining,
            limits,
        )?;
        self.estimated_bytes = next;
        Ok(spool_delta)
    }

    fn push_metadata(
        &mut self,
        metadata: OwnedMetadata,
        limits: &DecoderLimits,
    ) -> Result<(), DecodeError> {
        if self
            .changes
            .len()
            .saturating_add(self.metadata.len())
            .saturating_add(self.aborted_subtransactions.len())
            >= limits.max_changes_per_transaction
        {
            return Err(DecodeError::TooManyTransactionChanges {
                xid: self.xid,
                limit: limits.max_changes_per_transaction,
            });
        }
        let next = self
            .estimated_bytes
            .checked_add(metadata.estimated_bytes())
            .ok_or(DecodeError::TransactionTooLarge {
                xid: self.xid,
                limit: limits.max_transaction_bytes,
            })?;
        if next > limits.max_transaction_bytes {
            return Err(DecodeError::TransactionTooLarge {
                xid: self.xid,
                limit: limits.max_transaction_bytes,
            });
        }
        self.metadata.push(metadata);
        self.estimated_bytes = next;
        Ok(())
    }

    fn rollback_subtransaction(
        &mut self,
        owner_xid: u32,
        source_buffer_remaining: usize,
        limits: &DecoderLimits,
    ) -> Result<RollbackStats, DecodeError> {
        if self.aborted_subtransactions.contains_key(&owner_xid) {
            return Err(DecodeError::DuplicateStreamedSubtransactionAbort {
                xid: self.xid,
                subtransaction_xid: owner_xid,
            });
        }
        let change_stats = self.changes.owner_stats(owner_xid);
        let metadata_stats = self
            .metadata
            .iter()
            .filter(|metadata| metadata.owner_xid() == owner_xid)
            .fold(OwnerStats::default(), |mut stats, metadata| {
                stats.changes = stats.changes.saturating_add(1);
                stats.estimated_bytes = stats
                    .estimated_bytes
                    .saturating_add(metadata.estimated_bytes());
                stats
            });
        let removed_changes = change_stats.changes.saturating_add(metadata_stats.changes);
        let removed_estimated_bytes = change_stats
            .estimated_bytes
            .saturating_add(metadata_stats.estimated_bytes);
        let retained_items = self
            .changes
            .len()
            .saturating_add(self.metadata.len())
            .saturating_add(self.aborted_subtransactions.len())
            .saturating_sub(removed_changes);
        if retained_items >= limits.max_changes_per_transaction {
            return Err(DecodeError::TooManyTransactionChanges {
                xid: self.xid,
                limit: limits.max_changes_per_transaction,
            });
        }
        let retained_estimated_bytes = self.estimated_bytes.saturating_sub(removed_estimated_bytes);
        let next_estimated_bytes = retained_estimated_bytes
            .checked_add(ABORTED_SUBTRANSACTION_ESTIMATED_BYTES)
            .ok_or(DecodeError::TransactionTooLarge {
                xid: self.xid,
                limit: limits.max_transaction_bytes,
            })?;
        if next_estimated_bytes > limits.max_transaction_bytes {
            return Err(DecodeError::TransactionTooLarge {
                xid: self.xid,
                limit: limits.max_transaction_bytes,
            });
        }
        if ABORTED_SUBTRANSACTION_ESTIMATED_BYTES
            > source_buffer_remaining.saturating_add(removed_estimated_bytes)
        {
            return Err(DecodeError::TotalBufferedTransactionsTooLarge {
                limit: limits.max_total_buffered_transaction_bytes,
            });
        }

        let mut removed = self.changes.rollback_owner(self.xid, owner_xid, limits)?;
        self.metadata.retain(|metadata| {
            if metadata.owner_xid() == owner_xid {
                removed.estimated_bytes = removed
                    .estimated_bytes
                    .saturating_add(metadata.estimated_bytes());
                removed.changes = removed.changes.saturating_add(1);
                false
            } else {
                true
            }
        });
        // pgoutput can filter every change owned by a subtransaction while
        // PostgreSQL still emits its Stream Abort. Recording the abort as a
        // safe no-op prevents later data for that XID without treating a
        // publication-filtered transaction as malformed.
        debug_assert_eq!(removed.changes, removed_changes);
        debug_assert_eq!(removed.estimated_bytes, removed_estimated_bytes);
        self.estimated_bytes = next_estimated_bytes;
        self.aborted_subtransactions.insert(owner_xid, ());
        removed.added_estimated_bytes = ABORTED_SUBTRANSACTION_ESTIMATED_BYTES;
        Ok(removed)
    }

    fn finish(
        self,
        commit_lsn: u64,
        end_lsn: u64,
        timestamp: i64,
    ) -> Result<(DecodedTransaction, Vec<OwnedMetadata>), DecodeError> {
        Ok((
            DecodedTransaction {
                xid: self.xid,
                mode: self.mode,
                first_observed_wal_lsn: self.first_observed_wal_lsn,
                final_lsn: self.final_lsn,
                commit_lsn: PgLsn::new(commit_lsn),
                end_lsn: PgLsn::new(end_lsn),
                commit_time_postgres_micros: timestamp,
                changes: self.changes.into_changes()?,
            },
            self.metadata,
        ))
    }
}

#[derive(Clone, Copy)]
enum Target {
    Normal,
    Streamed { xid: u32, owner_xid: u32 },
}

/// Stateful decoder. Any error poisons the instance; restart it from the last
/// durable canonical cursor instead of trying to infer a safe parser state.
pub struct Decoder {
    protocol_version: u32,
    parser: LogicalReplicationParser,
    limits: DecoderLimits,
    poisoned: bool,
    relations: BTreeMap<u32, Arc<RelationMetadata>>,
    types: BTreeMap<u32, Arc<TypeMetadata>>,
    normal: Option<PendingTransaction>,
    streamed: BTreeMap<u32, PendingTransaction>,
    active_stream: Option<u32>,
    total_buffered_transaction_bytes: usize,
    total_spool_bytes: usize,
}

impl Decoder {
    /// Constructs the production decoder from the closed launch profile.
    pub fn for_launch(
        profile: crate::PgoutputLaunchProfile,
        limits: DecoderLimits,
    ) -> Result<Self, DecodeError> {
        Self::new(profile.protocol_version(), limits)
    }

    /// A decoder for `pgoutput` protocol 1 or 2. Outside this crate a decoder
    /// is built through [`Self::for_launch`], which passes protocol 1.
    pub(crate) fn new(protocol_version: u32, limits: DecoderLimits) -> Result<Self, DecodeError> {
        limits.validate()?;
        if !matches!(protocol_version, 1 | 2) {
            return Err(DecodeError::UnsupportedProtocolVersion(protocol_version));
        }
        Ok(Self {
            protocol_version,
            parser: LogicalReplicationParser::with_protocol_version(protocol_version),
            limits,
            poisoned: false,
            relations: BTreeMap::new(),
            types: BTreeMap::new(),
            normal: None,
            streamed: BTreeMap::new(),
            active_stream: None,
            total_buffered_transaction_bytes: 0,
            total_spool_bytes: 0,
        })
    }

    pub fn total_spool_bytes(&self) -> usize {
        self.total_spool_bytes
    }

    /// Decode bytes only through `pg_walstream`. Re-encoding rejects trailing or
    /// otherwise non-canonical bytes without locally interpreting the protocol.
    pub fn decode(
        &mut self,
        wal_lsn: PgLsn,
        bytes: &[u8],
    ) -> Result<Vec<DecoderEvent>, DecodeError> {
        if self.poisoned {
            return Err(DecodeError::Poisoned);
        }
        if bytes.len() > self.limits.max_message_bytes {
            return self.fail(DecodeError::MessageTooLarge {
                actual: bytes.len(),
                limit: self.limits.max_message_bytes,
            });
        }
        let decoded = match self.parser.parse_wal_message(bytes) {
            Ok(decoded) => decoded,
            Err(error) => return self.fail(DecodeError::PgWalstream(error.to_string())),
        };
        if encode_streaming_message_to_bytes(&decoded, self.protocol_version as u8).as_ref()
            != bytes
        {
            return self.fail(DecodeError::NonCanonicalMessage);
        }
        self.accept_decoded(wal_lsn, decoded)
    }

    /// Decodes the maintained PostgreSQL logical-replication `CopyData`
    /// envelope and delegates the contained `pgoutput` payload to
    /// `pg_walstream` through [`Self::decode`].
    ///
    /// PostgreSQL defines only two server tags here: XLogData (`w`) and the
    /// primary keepalive (`k`). The fixed envelope fields are framing, not
    /// `pgoutput`; row and transaction messages remain exclusively decoded by
    /// `pg_walstream`.
    pub fn decode_copy_data(&mut self, data: Bytes) -> Result<ReplicationCopyData, DecodeError> {
        if self.poisoned {
            return Err(DecodeError::Poisoned);
        }
        let frame_limit = match self.limits.max_message_bytes.checked_add(25) {
            Some(limit) => limit,
            None => return self.fail(DecodeError::InvalidLimits),
        };
        if data.len() > frame_limit {
            return self.fail(DecodeError::CopyDataTooLarge {
                actual: data.len(),
                limit: frame_limit,
            });
        }
        if data.is_empty() {
            return self.fail(DecodeError::EmptyCopyData);
        }

        match data[0] {
            b'w' => self.decode_xlog_data(data),
            b'k' => self.decode_keepalive(data),
            tag => self.fail(DecodeError::UnknownCopyDataTag(tag)),
        }
    }

    fn decode_xlog_data(&mut self, data: Bytes) -> Result<ReplicationCopyData, DecodeError> {
        if data.len() < 25 {
            return self.fail(DecodeError::XLogDataFrameTooShort { actual: data.len() });
        }
        let mut reader = BufferReader::from_bytes(data);
        let tag = reader
            .read_u8()
            .map_err(|error| DecodeError::PgWalstream(error.to_string()))?;
        debug_assert_eq!(tag, b'w');
        let start_lsn = reader
            .read_u64()
            .map_err(|error| DecodeError::PgWalstream(error.to_string()))?;
        let wal_end = reader
            .read_u64()
            .map_err(|error| DecodeError::PgWalstream(error.to_string()))?;
        let send_time_postgres_micros = reader
            .read_i64()
            .map_err(|error| DecodeError::PgWalstream(error.to_string()))?;
        let payload = reader
            .read_bytes_buf(reader.remaining())
            .map_err(|error| DecodeError::PgWalstream(error.to_string()))?;
        let events = if payload.is_empty() {
            Vec::new()
        } else {
            self.decode(PgLsn::new(start_lsn), payload.as_ref())?
        };
        Ok(ReplicationCopyData::Wal {
            start_lsn: PgLsn::new(start_lsn),
            wal_end: PgLsn::new(wal_end),
            send_time_postgres_micros,
            events,
        })
    }

    fn decode_keepalive(&mut self, data: Bytes) -> Result<ReplicationCopyData, DecodeError> {
        if data.len() != 18 {
            return self.fail(DecodeError::KeepaliveFrameLength { actual: data.len() });
        }
        let keepalive = match parse_keepalive_message(data.as_ref()) {
            Ok(keepalive) => keepalive,
            Err(error) => return self.fail(DecodeError::PgWalstream(error.to_string())),
        };
        Ok(ReplicationCopyData::Keepalive {
            wal_end: PgLsn::new(keepalive.wal_end),
            send_time_postgres_micros: keepalive.timestamp,
            reply_requested: keepalive.reply_requested,
        })
    }

    pub fn accept_decoded(
        &mut self,
        wal_lsn: PgLsn,
        decoded: StreamingReplicationMessage,
    ) -> Result<Vec<DecoderEvent>, DecodeError> {
        if self.poisoned {
            return Err(DecodeError::Poisoned);
        }
        match self.process(wal_lsn, decoded) {
            Ok(events) => Ok(events),
            Err(error) => self.fail(error),
        }
    }

    fn fail<T>(&mut self, error: DecodeError) -> Result<T, DecodeError> {
        self.poisoned = true;
        Err(error)
    }

    fn process(
        &mut self,
        wal_lsn: PgLsn,
        decoded: StreamingReplicationMessage,
    ) -> Result<Vec<DecoderEvent>, DecodeError> {
        use LogicalReplicationMessage as M;
        let StreamingReplicationMessage {
            message,
            is_streaming,
            xid: envelope_xid,
            data_xid,
        } = decoded;
        if data_xid.is_some() && !is_streaming {
            return Err(DecodeError::InvalidStreamingContext);
        }
        match message {
            M::Begin {
                final_lsn,
                timestamp,
                xid,
            } => {
                self.require_normal(is_streaming, envelope_xid)?;
                if let Some(active) = &self.normal {
                    return Err(DecodeError::NormalTransactionAlreadyActive(active.xid));
                }
                self.normal = Some(PendingTransaction {
                    xid,
                    mode: TransactionMode::Normal,
                    first_observed_wal_lsn: wal_lsn,
                    final_lsn: Some(PgLsn::new(final_lsn)),
                    begin_timestamp: Some(timestamp),
                    changes: ChangeStore::new(),
                    metadata: Vec::new(),
                    aborted_subtransactions: BTreeMap::new(),
                    estimated_bytes: 0,
                });
                Ok(Vec::new())
            }
            M::Commit {
                flags,
                commit_lsn,
                end_lsn,
                timestamp,
            } => {
                self.require_normal(is_streaming, envelope_xid)?;
                if flags != 0 {
                    return Err(DecodeError::UnknownCommitFlags(flags));
                }
                let pending = self
                    .normal
                    .as_ref()
                    .ok_or(DecodeError::CommitWithoutBegin)?;
                if pending.begin_timestamp != Some(timestamp) {
                    return Err(DecodeError::CommitTimestampMismatch);
                }
                let tx = self.normal.take().expect("normal transaction checked");
                self.total_buffered_transaction_bytes = self
                    .total_buffered_transaction_bytes
                    .saturating_sub(tx.estimated_bytes);
                self.total_spool_bytes = self
                    .total_spool_bytes
                    .saturating_sub(tx.changes.spool_bytes());
                self.commit_pending(tx, commit_lsn, end_lsn, timestamp)
            }
            M::Relation {
                relation_id,
                namespace,
                relation_name,
                replica_identity,
                columns,
            } => {
                if is_streaming {
                    let (xid, owner_xid) = self.stream_owner(envelope_xid, data_xid)?;
                    self.observe_stream_relation(
                        wal_lsn,
                        xid,
                        owner_xid,
                        relation_id,
                        namespace.as_ref(),
                        relation_name.as_ref(),
                        replica_identity,
                        columns,
                    )?;
                    Ok(Vec::new())
                } else {
                    self.require_data(false, envelope_xid, data_xid)?;
                    let (relation, layout_changed) = self.observe_relation(
                        wal_lsn,
                        relation_id,
                        namespace.as_ref(),
                        relation_name.as_ref(),
                        replica_identity,
                        columns,
                    )?;
                    Ok(vec![DecoderEvent::RelationObserved {
                        relation,
                        layout_changed,
                    }])
                }
            }
            M::Type {
                type_id,
                namespace,
                type_name,
            } => {
                if is_streaming {
                    let (xid, owner_xid) = self.stream_owner(envelope_xid, data_xid)?;
                    self.observe_stream_type(
                        wal_lsn, xid, owner_xid, type_id, namespace, type_name,
                    )?;
                    Ok(Vec::new())
                } else {
                    self.require_data(false, envelope_xid, data_xid)?;
                    let metadata = self.observe_type(wal_lsn, type_id, namespace, type_name)?;
                    Ok(vec![DecoderEvent::TypeObserved(metadata)])
                }
            }
            M::Insert { relation_id, tuple } => {
                let target = self.target(is_streaming, envelope_xid, data_xid)?;
                let relation = self.relation_required_for_target(target, relation_id)?;
                let (row, bytes) = self.full_row(&relation, tuple)?;
                self.push(target, DecodedChange::Insert { relation, row }, bytes)?;
                Ok(Vec::new())
            }
            M::Update {
                relation_id,
                old_tuple,
                new_tuple,
                key_type,
            } => {
                let target = self.target(is_streaming, envelope_xid, data_xid)?;
                let relation = self.relation_required_for_target(target, relation_id)?;
                let (patch, new_bytes) = self.patch(&relation, new_tuple)?;
                let (old_tuple, old_bytes) = if let Some(tuple) = old_tuple {
                    let (tuple, bytes) = self.tuple_image(&relation, tuple)?;
                    (Some(tuple), bytes)
                } else {
                    (None, 0)
                };
                let old_tuple_kind = match key_type {
                    Some('K') => Some(OldTupleKind::Key),
                    Some('O') => Some(OldTupleKind::Full),
                    None if old_tuple.is_none() => None,
                    other => return Err(DecodeError::UnknownOldTupleKind(other)),
                };
                self.push(
                    target,
                    DecodedChange::Update {
                        relation,
                        old_tuple,
                        old_tuple_kind,
                        patch,
                    },
                    new_bytes.saturating_add(old_bytes),
                )?;
                Ok(Vec::new())
            }
            M::Delete {
                relation_id,
                old_tuple,
                key_type,
            } => {
                let target = self.target(is_streaming, envelope_xid, data_xid)?;
                let relation = self.relation_required_for_target(target, relation_id)?;
                let (old_tuple, bytes) = self.tuple_image(&relation, old_tuple)?;
                let old_tuple_kind = match key_type {
                    'K' => OldTupleKind::Key,
                    'O' => OldTupleKind::Full,
                    other => return Err(DecodeError::UnknownOldTupleKind(Some(other))),
                };
                self.push(
                    target,
                    DecodedChange::Delete {
                        relation,
                        old_tuple,
                        old_tuple_kind,
                    },
                    bytes,
                )?;
                Ok(Vec::new())
            }
            M::Truncate {
                relation_ids,
                flags,
            } => {
                let target = self.target(is_streaming, envelope_xid, data_xid)?;
                if flags & !(TRUNCATE_CASCADE | TRUNCATE_RESTART_IDENTITY) != 0 {
                    return Err(DecodeError::UnknownTruncateFlags(flags));
                }
                let relations = relation_ids
                    .into_iter()
                    .map(|oid| self.relation_required_for_target(target, oid))
                    .collect::<Result<Vec<_>, _>>()?;
                let bytes = relations.len().saturating_mul(4);
                self.push(
                    target,
                    DecodedChange::Truncate {
                        relations,
                        cascade: flags & TRUNCATE_CASCADE != 0,
                        restart_identity: flags & TRUNCATE_RESTART_IDENTITY != 0,
                    },
                    bytes,
                )?;
                Ok(Vec::new())
            }
            M::Message {
                flags,
                lsn,
                prefix,
                content,
            } => {
                self.require_data(is_streaming, envelope_xid, data_xid)?;
                if content.len() > self.limits.max_logical_message_bytes {
                    return Err(DecodeError::LogicalMessageTooLarge {
                        actual: content.len(),
                        limit: self.limits.max_logical_message_bytes,
                    });
                }
                self.validate_identifier("logical message prefix", &prefix)?;
                let message = LogicalMessage {
                    message_lsn: PgLsn::new(lsn),
                    kind: marker_kind(&prefix),
                    prefix,
                    content: content.to_vec(),
                };
                let bytes = message.prefix.len().saturating_add(message.content.len());
                match flags {
                    0 if message.kind == LogicalMessageKind::Other => {
                        Ok(vec![DecoderEvent::StandaloneLogicalMessage(message)])
                    }
                    0 => Err(DecodeError::NonTransactionalReservedMarker(message.prefix)),
                    1 => {
                        let target =
                            self.target(is_streaming, envelope_xid, data_xid)
                                .map_err(|error| {
                                    if error == DecodeError::ChangeOutsideTransaction {
                                        DecodeError::TransactionalMessageOutsideTransaction
                                    } else {
                                        error
                                    }
                                })?;
                        self.push(target, DecodedChange::LogicalMessage(message), bytes)?;
                        Ok(Vec::new())
                    }
                    other => Err(DecodeError::UnknownLogicalMessageFlags(other)),
                }
            }
            M::Origin {
                origin_lsn,
                origin_name,
            } => {
                self.validate_identifier("origin name", &origin_name)?;
                if is_streaming {
                    let xid = self.stream_control_xid(envelope_xid, data_xid)?;
                    let estimated_bytes = origin_name.len().saturating_add(8);
                    self.push_metadata(
                        xid,
                        OwnedMetadata::Origin {
                            owner_xid: xid,
                            estimated_bytes,
                            origin_lsn: PgLsn::new(origin_lsn),
                            origin_name,
                        },
                    )?;
                    Ok(Vec::new())
                } else {
                    self.require_normal(false, envelope_xid)?;
                    Ok(vec![DecoderEvent::Origin {
                        origin_lsn: PgLsn::new(origin_lsn),
                        origin_name,
                    }])
                }
            }
            M::StreamStart { xid, first_segment } => {
                if self.protocol_version < 2
                    || !is_streaming
                    || envelope_xid != Some(xid)
                    || data_xid.is_some()
                {
                    return Err(DecodeError::InvalidStreamingContext);
                }
                if let Some(active) = self.active_stream {
                    return Err(DecodeError::StreamSegmentAlreadyActive(active));
                }
                if first_segment {
                    if self.streamed.contains_key(&xid) {
                        return Err(DecodeError::DuplicateStreamedTransaction(xid));
                    }
                    if self.streamed.len() >= self.limits.max_active_streamed_transactions {
                        return Err(DecodeError::TooManyActiveStreamedTransactions {
                            limit: self.limits.max_active_streamed_transactions,
                        });
                    }
                    self.streamed.insert(
                        xid,
                        PendingTransaction {
                            xid,
                            mode: TransactionMode::Streamed,
                            first_observed_wal_lsn: wal_lsn,
                            final_lsn: None,
                            begin_timestamp: None,
                            changes: ChangeStore::new(),
                            metadata: Vec::new(),
                            aborted_subtransactions: BTreeMap::new(),
                            estimated_bytes: 0,
                        },
                    );
                } else if !self.streamed.contains_key(&xid) {
                    return Err(DecodeError::UnknownStreamedTransaction(xid));
                }
                self.active_stream = Some(xid);
                Ok(Vec::new())
            }
            M::StreamStop => {
                self.require_normal(is_streaming, envelope_xid)?;
                self.active_stream
                    .take()
                    .ok_or(DecodeError::StreamStopWithoutStart)?;
                Ok(Vec::new())
            }
            M::StreamCommit {
                xid,
                flags,
                commit_lsn,
                end_lsn,
                timestamp,
            } => {
                self.require_normal(is_streaming, envelope_xid)?;
                if flags != 0 {
                    return Err(DecodeError::UnknownCommitFlags(flags));
                }
                if self.active_stream.is_some() {
                    return Err(DecodeError::StreamCommitDuringSegment);
                }
                let tx = self
                    .streamed
                    .remove(&xid)
                    .ok_or(DecodeError::UnknownStreamedTransaction(xid))?;
                self.total_buffered_transaction_bytes = self
                    .total_buffered_transaction_bytes
                    .saturating_sub(tx.estimated_bytes);
                self.total_spool_bytes = self
                    .total_spool_bytes
                    .saturating_sub(tx.changes.spool_bytes());
                self.commit_pending(tx, commit_lsn, end_lsn, timestamp)
            }
            M::StreamAbort {
                xid,
                subtransaction_xid,
                ..
            } => {
                self.require_normal(is_streaming, envelope_xid)?;
                if self.active_stream.is_some() {
                    return Err(DecodeError::StreamAbortDuringSegment);
                }
                if subtransaction_xid == xid {
                    let pending = self
                        .streamed
                        .remove(&xid)
                        .ok_or(DecodeError::UnknownStreamedTransaction(xid))?;
                    self.total_buffered_transaction_bytes = self
                        .total_buffered_transaction_bytes
                        .saturating_sub(pending.estimated_bytes);
                    self.total_spool_bytes = self
                        .total_spool_bytes
                        .saturating_sub(pending.changes.spool_bytes());
                } else {
                    let source_buffer_remaining = self
                        .limits
                        .max_total_buffered_transaction_bytes
                        .saturating_sub(self.total_buffered_transaction_bytes);
                    let pending = self
                        .streamed
                        .get_mut(&xid)
                        .ok_or(DecodeError::UnknownStreamedTransaction(xid))?;
                    let removed = pending.rollback_subtransaction(
                        subtransaction_xid,
                        source_buffer_remaining,
                        &self.limits,
                    )?;
                    self.total_buffered_transaction_bytes = self
                        .total_buffered_transaction_bytes
                        .saturating_sub(removed.estimated_bytes)
                        .saturating_add(removed.added_estimated_bytes);
                    self.total_spool_bytes =
                        self.total_spool_bytes.saturating_sub(removed.spool_bytes);
                }
                Ok(vec![DecoderEvent::StreamAborted {
                    xid,
                    subtransaction_xid,
                }])
            }
            M::BeginPrepare { .. }
            | M::Prepare { .. }
            | M::CommitPrepared { .. }
            | M::RollbackPrepared { .. }
            | M::StreamPrepare { .. } => Err(DecodeError::TwoPhaseUnsupported),
        }
    }

    fn require_normal(
        &self,
        is_streaming: bool,
        envelope_xid: Option<u32>,
    ) -> Result<(), DecodeError> {
        if is_streaming || envelope_xid.is_some() {
            return Err(DecodeError::UnexpectedStreamingEnvelope);
        }
        Ok(())
    }

    fn require_data(
        &self,
        is_streaming: bool,
        envelope_xid: Option<u32>,
        data_xid: Option<u32>,
    ) -> Result<(), DecodeError> {
        if is_streaming {
            let xid = envelope_xid.ok_or(DecodeError::InvalidStreamingContext)?;
            if data_xid.is_none()
                || self.active_stream != Some(xid)
                || !self.streamed.contains_key(&xid)
            {
                return Err(DecodeError::InvalidStreamingContext);
            }
        } else if envelope_xid.is_some() || data_xid.is_some() {
            return Err(DecodeError::InvalidStreamingContext);
        }
        Ok(())
    }

    fn stream_owner(
        &self,
        envelope_xid: Option<u32>,
        data_xid: Option<u32>,
    ) -> Result<(u32, u32), DecodeError> {
        self.require_data(true, envelope_xid, data_xid)?;
        let xid = envelope_xid.expect("validated top-level xid");
        let owner_xid = data_xid.expect("validated data xid");
        let pending = self
            .streamed
            .get(&xid)
            .expect("validated streamed transaction");
        if pending.aborted_subtransactions.contains_key(&owner_xid) {
            return Err(DecodeError::DataForAbortedStreamedSubtransaction {
                xid,
                subtransaction_xid: owner_xid,
            });
        }
        Ok((xid, owner_xid))
    }

    fn stream_control_xid(
        &self,
        envelope_xid: Option<u32>,
        data_xid: Option<u32>,
    ) -> Result<u32, DecodeError> {
        if data_xid.is_some() {
            return Err(DecodeError::InvalidStreamingContext);
        }
        let xid = envelope_xid.ok_or(DecodeError::InvalidStreamingContext)?;
        if self.active_stream != Some(xid) || !self.streamed.contains_key(&xid) {
            return Err(DecodeError::InvalidStreamingContext);
        }
        Ok(xid)
    }

    fn target(
        &self,
        is_streaming: bool,
        envelope_xid: Option<u32>,
        data_xid: Option<u32>,
    ) -> Result<Target, DecodeError> {
        if is_streaming {
            let (xid, owner_xid) = self.stream_owner(envelope_xid, data_xid)?;
            Ok(Target::Streamed { xid, owner_xid })
        } else if self.normal.is_some() {
            self.require_data(false, envelope_xid, data_xid)?;
            Ok(Target::Normal)
        } else {
            Err(DecodeError::ChangeOutsideTransaction)
        }
    }

    fn push(
        &mut self,
        target: Target,
        change: DecodedChange,
        bytes: usize,
    ) -> Result<(), DecodeError> {
        let next_total = self
            .total_buffered_transaction_bytes
            .checked_add(bytes)
            .ok_or(DecodeError::TotalBufferedTransactionsTooLarge {
                limit: self.limits.max_total_buffered_transaction_bytes,
            })?;
        if next_total > self.limits.max_total_buffered_transaction_bytes {
            return Err(DecodeError::TotalBufferedTransactionsTooLarge {
                limit: self.limits.max_total_buffered_transaction_bytes,
            });
        }
        let source_spool_remaining = self
            .limits
            .max_total_spool_bytes
            .saturating_sub(self.total_spool_bytes);
        let spool_delta = match target {
            Target::Normal => {
                let pending = self.normal.as_mut().expect("validated transaction");
                pending.push(
                    pending.xid,
                    change,
                    bytes,
                    source_spool_remaining,
                    &self.limits,
                )?
            }
            Target::Streamed { xid, owner_xid } => self
                .streamed
                .get_mut(&xid)
                .expect("validated transaction")
                .push(
                    owner_xid,
                    change,
                    bytes,
                    source_spool_remaining,
                    &self.limits,
                )?,
        };
        self.total_buffered_transaction_bytes = next_total;
        self.total_spool_bytes = self.total_spool_bytes.checked_add(spool_delta).ok_or(
            DecodeError::TotalSpoolTooLarge {
                limit: self.limits.max_total_spool_bytes,
            },
        )?;
        Ok(())
    }

    fn push_metadata(&mut self, xid: u32, metadata: OwnedMetadata) -> Result<(), DecodeError> {
        let bytes = metadata.estimated_bytes();
        let next_total = self
            .total_buffered_transaction_bytes
            .checked_add(bytes)
            .ok_or(DecodeError::TotalBufferedTransactionsTooLarge {
                limit: self.limits.max_total_buffered_transaction_bytes,
            })?;
        if next_total > self.limits.max_total_buffered_transaction_bytes {
            return Err(DecodeError::TotalBufferedTransactionsTooLarge {
                limit: self.limits.max_total_buffered_transaction_bytes,
            });
        }
        self.streamed
            .get_mut(&xid)
            .ok_or(DecodeError::UnknownStreamedTransaction(xid))?
            .push_metadata(metadata, &self.limits)?;
        self.total_buffered_transaction_bytes = next_total;
        Ok(())
    }

    fn relation_required_for_target(
        &self,
        target: Target,
        oid: u32,
    ) -> Result<Arc<RelationMetadata>, DecodeError> {
        if let Target::Streamed { xid, .. } = target {
            if let Some(relation) = self.streamed.get(&xid).and_then(|pending| {
                pending
                    .metadata
                    .iter()
                    .rev()
                    .find_map(|metadata| match metadata {
                        OwnedMetadata::Relation { relation, .. }
                            if relation.relation_oid == oid =>
                        {
                            Some(Arc::clone(relation))
                        }
                        _ => None,
                    })
            }) {
                return Ok(relation);
            }
        }
        self.relation_required(oid)
    }

    fn relation_required(&self, oid: u32) -> Result<Arc<RelationMetadata>, DecodeError> {
        self.relations
            .get(&oid)
            .cloned()
            .ok_or(DecodeError::UnknownRelation(oid))
    }

    #[allow(clippy::too_many_arguments)]
    fn observe_stream_relation(
        &mut self,
        wal_lsn: PgLsn,
        xid: u32,
        owner_xid: u32,
        oid: u32,
        namespace: &str,
        name: &str,
        identity: u8,
        columns: Vec<ColumnInfo>,
    ) -> Result<(), DecodeError> {
        self.validate_identifier("relation namespace", namespace)?;
        self.validate_identifier("relation name", name)?;
        if columns.len() > self.limits.max_columns_per_relation {
            return Err(DecodeError::TooManyColumns {
                relation_oid: oid,
                actual: columns.len(),
                limit: self.limits.max_columns_per_relation,
            });
        }

        let mut known = self
            .relations
            .keys()
            .copied()
            .map(|oid| (oid, ()))
            .collect::<BTreeMap<_, _>>();
        for pending in self.streamed.values() {
            for metadata in &pending.metadata {
                if let OwnedMetadata::Relation { relation, .. } = metadata {
                    known.insert(relation.relation_oid, ());
                }
            }
        }
        if !known.contains_key(&oid) && known.len() >= self.limits.max_relations {
            return Err(DecodeError::TooManyRelations {
                limit: self.limits.max_relations,
            });
        }

        let identity = ReplicaIdentity::from_pgoutput(identity)?;
        let columns = columns
            .into_iter()
            .enumerate()
            .map(|(index, column)| {
                self.validate_identifier("column name", column.name.as_ref())?;
                if column.flags & !1 != 0 {
                    return Err(DecodeError::UnknownColumnFlags {
                        relation_oid: oid,
                        column_index: index,
                        flags: column.flags,
                    });
                }
                Ok(RelationColumn {
                    name: column.name.to_string(),
                    type_oid: column.type_id,
                    type_modifier: column.type_modifier,
                    is_key: column.flags & 1 != 0,
                })
            })
            .collect::<Result<Vec<_>, DecodeError>>()?;
        let predecessor = self
            .streamed
            .get(&xid)
            .and_then(|pending| {
                pending
                    .metadata
                    .iter()
                    .rev()
                    .find_map(|metadata| match metadata {
                        OwnedMetadata::Relation { relation, .. }
                            if relation.relation_oid == oid =>
                        {
                            Some(Arc::clone(relation))
                        }
                        _ => None,
                    })
            })
            .or_else(|| self.relations.get(&oid).cloned());
        let layout_changed = predecessor.as_ref().is_none_or(|old| {
            old.namespace != namespace
                || old.relation_name != name
                || old.replica_identity != identity
                || old.columns != columns
        });
        let generation = predecessor.as_ref().map_or(1, |old| {
            if layout_changed {
                old.layout_generation.saturating_add(1)
            } else {
                old.layout_generation
            }
        });
        let first = predecessor
            .as_ref()
            .map_or(wal_lsn, |old| old.first_observed_wal_lsn);
        let estimated_bytes = namespace
            .len()
            .saturating_add(name.len())
            .saturating_add(
                columns
                    .iter()
                    .map(|column| column.name.len().saturating_add(9))
                    .sum::<usize>(),
            )
            .saturating_add(32);
        let relation = Arc::new(RelationMetadata {
            relation_oid: oid,
            namespace: namespace.to_owned(),
            relation_name: name.to_owned(),
            replica_identity: identity,
            columns,
            layout_generation: generation,
            first_observed_wal_lsn: first,
            last_observed_wal_lsn: wal_lsn,
        });
        self.push_metadata(
            xid,
            OwnedMetadata::Relation {
                owner_xid,
                estimated_bytes,
                predecessor,
                relation,
                layout_changed,
            },
        )
    }

    fn observe_stream_type(
        &mut self,
        wal_lsn: PgLsn,
        xid: u32,
        owner_xid: u32,
        oid: u32,
        namespace: String,
        name: String,
    ) -> Result<(), DecodeError> {
        self.validate_identifier("type namespace", &namespace)?;
        self.validate_identifier("type name", &name)?;
        let mut known = self
            .types
            .keys()
            .copied()
            .map(|oid| (oid, ()))
            .collect::<BTreeMap<_, _>>();
        for pending in self.streamed.values() {
            for metadata in &pending.metadata {
                if let OwnedMetadata::Type { metadata, .. } = metadata {
                    known.insert(metadata.type_oid, ());
                }
            }
        }
        if !known.contains_key(&oid) && known.len() >= self.limits.max_types {
            return Err(DecodeError::TooManyTypes {
                limit: self.limits.max_types,
            });
        }
        let predecessor = self
            .streamed
            .get(&xid)
            .and_then(|pending| {
                pending
                    .metadata
                    .iter()
                    .rev()
                    .find_map(|metadata| match metadata {
                        OwnedMetadata::Type { metadata, .. } if metadata.type_oid == oid => {
                            Some(Arc::clone(metadata))
                        }
                        _ => None,
                    })
            })
            .or_else(|| self.types.get(&oid).cloned());
        let first = predecessor
            .as_ref()
            .map_or(wal_lsn, |old| old.first_observed_wal_lsn);
        let estimated_bytes = namespace
            .len()
            .saturating_add(name.len())
            .saturating_add(24);
        let metadata = Arc::new(TypeMetadata {
            type_oid: oid,
            namespace,
            type_name: name,
            first_observed_wal_lsn: first,
            last_observed_wal_lsn: wal_lsn,
        });
        self.push_metadata(
            xid,
            OwnedMetadata::Type {
                owner_xid,
                estimated_bytes,
                predecessor,
                metadata,
            },
        )
    }

    fn commit_pending(
        &mut self,
        pending: PendingTransaction,
        commit_lsn: u64,
        end_lsn: u64,
        timestamp: i64,
    ) -> Result<Vec<DecoderEvent>, DecodeError> {
        let (transaction, metadata) = pending.finish(commit_lsn, end_lsn, timestamp)?;
        let mut next_relations = self.relations.clone();
        let mut next_types = self.types.clone();
        let mut events = Vec::with_capacity(metadata.len().saturating_add(1));
        let mut valid_relation_versions = Vec::new();

        for metadata in metadata {
            match metadata {
                OwnedMetadata::Relation {
                    predecessor,
                    relation,
                    layout_changed,
                    ..
                } => {
                    let current = next_relations.get(&relation.relation_oid).cloned();
                    let predecessor_matches =
                        relation_option_same_version(current.as_ref(), predecessor.as_ref());
                    let already_observed = current
                        .as_ref()
                        .is_some_and(|current| relation_same_version(current, &relation));
                    if !predecessor_matches && !already_observed {
                        return Err(DecodeError::StreamedMetadataConflict {
                            kind: "relation",
                            oid: relation.relation_oid,
                        });
                    }
                    let published = current
                        .filter(|current| relation_same_version(current, &relation))
                        .map_or_else(
                            || Arc::clone(&relation),
                            |current| Arc::new(merge_relation_observation(&current, &relation)),
                        );
                    valid_relation_versions.push(Arc::clone(&relation));
                    valid_relation_versions.push(Arc::clone(&published));
                    next_relations.insert(relation.relation_oid, Arc::clone(&published));
                    events.push(DecoderEvent::RelationObserved {
                        relation: published,
                        layout_changed,
                    });
                }
                OwnedMetadata::Type {
                    predecessor,
                    metadata,
                    ..
                } => {
                    let current = next_types.get(&metadata.type_oid).cloned();
                    let predecessor_matches =
                        type_option_same_identity(current.as_ref(), predecessor.as_ref());
                    let already_observed = current
                        .as_ref()
                        .is_some_and(|current| type_same_identity(current, &metadata));
                    if !predecessor_matches && !already_observed {
                        return Err(DecodeError::StreamedMetadataConflict {
                            kind: "type",
                            oid: metadata.type_oid,
                        });
                    }
                    let published = current
                        .filter(|current| type_same_identity(current, &metadata))
                        .map_or_else(
                            || Arc::clone(&metadata),
                            |current| Arc::new(merge_type_observation(&current, &metadata)),
                        );
                    next_types.insert(metadata.type_oid, Arc::clone(&published));
                    events.push(DecoderEvent::TypeObserved(published));
                }
                OwnedMetadata::Origin {
                    origin_lsn,
                    origin_name,
                    ..
                } => events.push(DecoderEvent::Origin {
                    origin_lsn,
                    origin_name,
                }),
            }
        }

        if transaction.mode == TransactionMode::Streamed {
            validate_transaction_relation_versions(
                &transaction,
                &self.relations,
                &valid_relation_versions,
            )?;
        }
        self.relations = next_relations;
        self.types = next_types;
        events.push(DecoderEvent::TransactionCommitted(transaction));
        Ok(events)
    }

    #[allow(clippy::too_many_arguments)]
    fn observe_relation(
        &mut self,
        wal_lsn: PgLsn,
        oid: u32,
        namespace: &str,
        name: &str,
        identity: u8,
        columns: Vec<ColumnInfo>,
    ) -> Result<(Arc<RelationMetadata>, bool), DecodeError> {
        self.validate_identifier("relation namespace", namespace)?;
        self.validate_identifier("relation name", name)?;
        if columns.len() > self.limits.max_columns_per_relation {
            return Err(DecodeError::TooManyColumns {
                relation_oid: oid,
                actual: columns.len(),
                limit: self.limits.max_columns_per_relation,
            });
        }
        if !self.relations.contains_key(&oid) && self.relations.len() >= self.limits.max_relations {
            return Err(DecodeError::TooManyRelations {
                limit: self.limits.max_relations,
            });
        }
        let identity = ReplicaIdentity::from_pgoutput(identity)?;
        let columns = columns
            .into_iter()
            .enumerate()
            .map(|(index, column)| {
                self.validate_identifier("column name", column.name.as_ref())?;
                if column.flags & !1 != 0 {
                    return Err(DecodeError::UnknownColumnFlags {
                        relation_oid: oid,
                        column_index: index,
                        flags: column.flags,
                    });
                }
                Ok(RelationColumn {
                    name: column.name.to_string(),
                    type_oid: column.type_id,
                    type_modifier: column.type_modifier,
                    is_key: column.flags & 1 != 0,
                })
            })
            .collect::<Result<Vec<_>, DecodeError>>()?;
        let previous = self.relations.get(&oid);
        let layout_changed = match previous {
            None => true,
            Some(old) => {
                old.namespace != namespace
                    || old.relation_name != name
                    || old.replica_identity != identity
                    || old.columns != columns
            }
        };
        let generation = previous.map_or(1, |old| {
            if layout_changed {
                old.layout_generation.saturating_add(1)
            } else {
                old.layout_generation
            }
        });
        let first = previous.map_or(wal_lsn, |old| old.first_observed_wal_lsn);
        let relation = Arc::new(RelationMetadata {
            relation_oid: oid,
            namespace: namespace.to_owned(),
            relation_name: name.to_owned(),
            replica_identity: identity,
            columns,
            layout_generation: generation,
            first_observed_wal_lsn: first,
            last_observed_wal_lsn: wal_lsn,
        });
        self.relations.insert(oid, Arc::clone(&relation));
        Ok((relation, layout_changed))
    }

    fn observe_type(
        &mut self,
        wal_lsn: PgLsn,
        oid: u32,
        namespace: String,
        name: String,
    ) -> Result<Arc<TypeMetadata>, DecodeError> {
        self.validate_identifier("type namespace", &namespace)?;
        self.validate_identifier("type name", &name)?;
        if !self.types.contains_key(&oid) && self.types.len() >= self.limits.max_types {
            return Err(DecodeError::TooManyTypes {
                limit: self.limits.max_types,
            });
        }
        let first = self
            .types
            .get(&oid)
            .map_or(wal_lsn, |old| old.first_observed_wal_lsn);
        let metadata = Arc::new(TypeMetadata {
            type_oid: oid,
            namespace,
            type_name: name,
            first_observed_wal_lsn: first,
            last_observed_wal_lsn: wal_lsn,
        });
        self.types.insert(oid, Arc::clone(&metadata));
        Ok(metadata)
    }

    fn full_row(
        &self,
        relation: &Arc<RelationMetadata>,
        tuple: TupleData,
    ) -> Result<(RowImage, usize), DecodeError> {
        let (cells, bytes) = self.tuple_cells(relation, tuple)?;
        let cells = cells
            .into_iter()
            .map(|cell| match cell {
                TupleCell::Value(value) => Ok(value),
                TupleCell::Absent => Err(DecodeError::IncompleteInsert {
                    relation_oid: relation.relation_oid,
                }),
            })
            .collect::<Result<Vec<_>, _>>()?;
        Ok((RowImage { cells }, bytes))
    }

    fn tuple_image(
        &self,
        relation: &Arc<RelationMetadata>,
        tuple: TupleData,
    ) -> Result<(TupleImage, usize), DecodeError> {
        let (cells, bytes) = self.tuple_cells(relation, tuple)?;
        Ok((TupleImage { cells }, bytes))
    }

    fn patch(
        &self,
        relation: &Arc<RelationMetadata>,
        tuple: TupleData,
    ) -> Result<(RowPatch, usize), DecodeError> {
        let (cells, bytes) = self.tuple_cells(relation, tuple)?;
        Ok((RowPatch { cells }, bytes))
    }

    fn tuple_cells(
        &self,
        relation: &Arc<RelationMetadata>,
        tuple: TupleData,
    ) -> Result<(Vec<TupleCell>, usize), DecodeError> {
        if tuple.columns.len() != relation.columns.len() {
            return Err(DecodeError::TupleLayoutMismatch {
                relation_oid: relation.relation_oid,
                expected: relation.columns.len(),
                actual: tuple.columns.len(),
            });
        }
        let bytes = tuple.columns.iter().try_fold(0usize, |total, column| {
            total
                .checked_add(column.as_bytes().len())
                .ok_or(DecodeError::TupleTooLarge {
                    relation_oid: relation.relation_oid,
                    actual: usize::MAX,
                    limit: self.limits.max_tuple_bytes,
                })
        })?;
        if bytes > self.limits.max_tuple_bytes {
            return Err(DecodeError::TupleTooLarge {
                relation_oid: relation.relation_oid,
                actual: bytes,
                limit: self.limits.max_tuple_bytes,
            });
        }
        let cells = tuple
            .columns
            .into_iter()
            .map(tuple_cell)
            .collect::<Result<Vec<_>, _>>()?;
        Ok((cells, bytes))
    }

    fn validate_identifier(&self, field: &'static str, value: &str) -> Result<(), DecodeError> {
        if value.len() > self.limits.max_identifier_bytes {
            return Err(DecodeError::IdentifierTooLong {
                field,
                limit: self.limits.max_identifier_bytes,
            });
        }
        Ok(())
    }
}

fn tuple_cell(column: ColumnData) -> Result<TupleCell, DecodeError> {
    match column.data_type {
        b'u' => Ok(TupleCell::Absent),
        b'n' => Ok(TupleCell::Value(CellValue::Null)),
        b't' => Ok(TupleCell::Value(CellValue::Text(
            column.as_bytes().to_vec(),
        ))),
        b'b' => Ok(TupleCell::Value(CellValue::Binary(
            column.as_bytes().to_vec(),
        ))),
        other => Err(DecodeError::UnknownTupleDataTag(other)),
    }
}

fn marker_kind(prefix: &str) -> LogicalMessageKind {
    match prefix {
        CUT_MARKER_PREFIX => LogicalMessageKind::CutMarker,
        MUTATION_MARKER_PREFIX => LogicalMessageKind::MutationMarker,
        _ => LogicalMessageKind::Other,
    }
}

fn relation_same_version(left: &RelationMetadata, right: &RelationMetadata) -> bool {
    left.relation_oid == right.relation_oid
        && left.namespace == right.namespace
        && left.relation_name == right.relation_name
        && left.replica_identity == right.replica_identity
        && left.columns == right.columns
        && left.layout_generation == right.layout_generation
}

fn relation_option_same_version(
    left: Option<&Arc<RelationMetadata>>,
    right: Option<&Arc<RelationMetadata>>,
) -> bool {
    match (left, right) {
        (None, None) => true,
        (Some(left), Some(right)) => relation_same_version(left, right),
        _ => false,
    }
}

fn merge_relation_observation(
    left: &RelationMetadata,
    right: &RelationMetadata,
) -> RelationMetadata {
    debug_assert!(relation_same_version(left, right));
    let mut merged = right.clone();
    merged.first_observed_wal_lsn = left
        .first_observed_wal_lsn
        .min(right.first_observed_wal_lsn);
    merged.last_observed_wal_lsn = left.last_observed_wal_lsn.max(right.last_observed_wal_lsn);
    merged
}

fn type_same_identity(left: &TypeMetadata, right: &TypeMetadata) -> bool {
    left.type_oid == right.type_oid
        && left.namespace == right.namespace
        && left.type_name == right.type_name
}

fn type_option_same_identity(
    left: Option<&Arc<TypeMetadata>>,
    right: Option<&Arc<TypeMetadata>>,
) -> bool {
    match (left, right) {
        (None, None) => true,
        (Some(left), Some(right)) => type_same_identity(left, right),
        _ => false,
    }
}

fn merge_type_observation(left: &TypeMetadata, right: &TypeMetadata) -> TypeMetadata {
    debug_assert!(type_same_identity(left, right));
    let mut merged = right.clone();
    merged.first_observed_wal_lsn = left
        .first_observed_wal_lsn
        .min(right.first_observed_wal_lsn);
    merged.last_observed_wal_lsn = left.last_observed_wal_lsn.max(right.last_observed_wal_lsn);
    merged
}

fn validate_transaction_relation_versions(
    transaction: &DecodedTransaction,
    committed_relations: &BTreeMap<u32, Arc<RelationMetadata>>,
    staged_relations: &[Arc<RelationMetadata>],
) -> Result<(), DecodeError> {
    let validate = |relation: &Arc<RelationMetadata>| {
        let committed = committed_relations
            .get(&relation.relation_oid)
            .is_some_and(|candidate| relation_same_version(candidate, relation));
        let staged = staged_relations
            .iter()
            .any(|candidate| relation_same_version(candidate, relation));
        if committed || staged {
            Ok(())
        } else {
            Err(DecodeError::AbortedStreamedMetadataReferenced {
                relation_oid: relation.relation_oid,
            })
        }
    };

    for change in &transaction.changes {
        match change {
            DecodedChange::Insert { relation, .. }
            | DecodedChange::Update { relation, .. }
            | DecodedChange::Delete { relation, .. } => validate(relation)?,
            DecodedChange::Truncate { relations, .. } => {
                for relation in relations {
                    validate(relation)?;
                }
            }
            DecodedChange::LogicalMessage(_) => {}
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use pg_walstream::{ColumnData, ColumnInfo, LogicalReplicationMessage as M, TupleData};

    use super::*;

    /// Transactions, normal or streamed, that currently hold spilled changes.
    fn spooled_transaction_count(decoder: &Decoder) -> usize {
        usize::from(
            decoder
                .normal
                .as_ref()
                .is_some_and(|transaction| transaction.changes.spool_bytes() > 0),
        ) + decoder
            .streamed
            .values()
            .filter(|transaction| transaction.changes.spool_bytes() > 0)
            .count()
    }

    fn decoder() -> Decoder {
        Decoder::new(2, DecoderLimits::default()).unwrap()
    }

    #[test]
    #[ignore = "requires TEST_DATABASE_URL and disposable PostgreSQL 18 with wal_level=logical"]
    fn live_pg18_streamed_large_transaction_matches_postgres_after_nested_abort() {
        use std::{
            collections::BTreeMap,
            sync::mpsc,
            thread,
            time::{Duration, SystemTime, UNIX_EPOCH},
        };

        use pg_walstream::{parse_lsn, PgReplicationConnection};
        use postgres::{Client, NoTls};
        use tokio_util::sync::CancellationToken;

        let database_url = std::env::var("TEST_DATABASE_URL")
            .expect("TEST_DATABASE_URL must name a disposable PostgreSQL 18 database");
        let suffix = format!(
            "{}_{}",
            std::process::id(),
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        );
        let table = format!("rm_streamed_{suffix}");
        let publication = format!("rm_streamed_pub_{suffix}");
        let slot = format!("rm_streamed_slot_{suffix}");
        let mut source = Client::connect(&database_url, NoTls).unwrap();
        let version: i32 = source
            .query_one("SELECT current_setting('server_version_num')::int", &[])
            .unwrap()
            .get(0);
        assert!((180_000..190_000).contains(&version));
        source
            .batch_execute(&format!(
                "CREATE TABLE public.{table} (id bigint PRIMARY KEY, body text NOT NULL);
                 CREATE PUBLICATION {publication} FOR TABLE public.{table};"
            ))
            .unwrap();
        let start_lsn: String = source
            .query_one(
                "SELECT lsn::text FROM pg_catalog.pg_create_logical_replication_slot($1, 'pgoutput')",
                &[&slot],
            )
            .unwrap()
            .get(0);

        let separator = if database_url.contains('?') { '&' } else { '?' };
        let replication_url =
            format!("{database_url}{separator}replication=database&connect_timeout=5");
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_io()
            .enable_time()
            .build()
            .unwrap();
        let mut replication = {
            let _runtime_guard = runtime.enter();
            let mut replication = PgReplicationConnection::connect(&replication_url).unwrap();
            replication
                .exec("SET logical_decoding_work_mem = '64kB'")
                .unwrap();
            replication
                .start_replication(
                    &slot,
                    parse_lsn(&start_lsn).unwrap(),
                    &[
                        ("proto_version", "2"),
                        ("publication_names", publication.as_str()),
                        ("messages", "true"),
                        ("streaming", "on"),
                        ("binary", "false"),
                        ("two_phase", "false"),
                    ],
                )
                .unwrap();
            replication
        };

        let writer_url = database_url.clone();
        let writer_table = table.clone();
        let (started_tx, transaction_started) = mpsc::sync_channel(0);
        let writer = thread::spawn(move || {
            let mut writer = Client::connect(&writer_url, NoTls).unwrap();
            let mut transaction = writer.transaction().unwrap();
            started_tx.send(()).unwrap();
            transaction
                .execute(
                    &format!(
                        "INSERT INTO public.{writer_table} (id, body)
                         SELECT value, repeat('row-' || value::text || '-', 96)
                           FROM generate_series(1, 3000) AS value"
                    ),
                    &[],
                )
                .unwrap();
            transaction
                .batch_execute("SAVEPOINT aborted_child")
                .unwrap();
            transaction
                .execute(
                    &format!(
                        "INSERT INTO public.{writer_table} VALUES (999999, 'must-not-survive')"
                    ),
                    &[],
                )
                .unwrap();
            transaction
                .batch_execute("ROLLBACK TO SAVEPOINT aborted_child")
                .unwrap();
            transaction.commit().unwrap();
        });
        transaction_started.recv().unwrap();

        let limits = DecoderLimits {
            max_changes_per_transaction: 4_000,
            max_transaction_bytes: 16 * 1024 * 1024,
            max_transaction_spool_bytes: 32 * 1024 * 1024,
            max_total_buffered_transaction_bytes: 16 * 1024 * 1024,
            max_total_spool_bytes: 32 * 1024 * 1024,
            max_in_memory_transaction_bytes: 64 * 1024,
            ..DecoderLimits::default()
        };
        let mut decoder = Decoder::new(2, limits).unwrap();
        let (transaction, saw_bounded_spool) = runtime
            .block_on(async {
                tokio::time::timeout(Duration::from_secs(30), async {
                    let cancellation = CancellationToken::new();
                    let mut saw_bounded_spool = false;
                    loop {
                        let frame = replication
                            .get_copy_data_async(&cancellation)
                            .await
                            .unwrap();
                        let ReplicationCopyData::Wal { events, .. } =
                            decoder.decode_copy_data(frame).unwrap()
                        else {
                            continue;
                        };
                        saw_bounded_spool |= spooled_transaction_count(&decoder) > 0;
                        if let Some(transaction) =
                            events.into_iter().find_map(|event| match event {
                                DecoderEvent::TransactionCommitted(transaction) => {
                                    Some(transaction)
                                }
                                _ => None,
                            })
                        {
                            break (transaction, saw_bounded_spool);
                        }
                    }
                })
                .await
            })
            .expect("streamed transaction must commit before the deadline");
        writer.join().unwrap();

        assert_eq!(transaction.mode, TransactionMode::Streamed);
        assert!(
            saw_bounded_spool,
            "large streamed transaction must cross the bounded spool"
        );
        assert_eq!(transaction.changes.len(), 3_000);
        let decoded = transaction
            .changes
            .iter()
            .map(|change| {
                let DecodedChange::Insert { row, .. } = change else {
                    panic!("fixture emits inserts only");
                };
                let [CellValue::Text(id), CellValue::Text(body)] = row.cells.as_slice() else {
                    panic!("fixture values must use text pgoutput cells");
                };
                (
                    std::str::from_utf8(id).unwrap().parse::<i64>().unwrap(),
                    std::str::from_utf8(body).unwrap().to_owned(),
                )
            })
            .collect::<BTreeMap<_, _>>();
        let reference = source
            .query(
                &format!("SELECT id, body FROM public.{table} ORDER BY id"),
                &[],
            )
            .unwrap()
            .into_iter()
            .map(|row| (row.get::<_, i64>(0), row.get::<_, String>(1)))
            .collect::<BTreeMap<_, _>>();
        assert_eq!(decoded, reference);
        assert!(!decoded.contains_key(&999_999));
        assert_eq!(decoder.total_spool_bytes(), 0);
        assert_eq!(spooled_transaction_count(&decoder), 0);

        drop(replication);
        source
            .query_one("SELECT pg_catalog.pg_drop_replication_slot($1)", &[&slot])
            .unwrap();
        source
            .batch_execute(&format!(
                "DROP PUBLICATION {publication}; DROP TABLE public.{table};"
            ))
            .unwrap();
    }

    fn relation(columns: Vec<ColumnInfo>) -> M {
        M::Relation {
            relation_id: 7,
            namespace: Arc::from("public"),
            relation_name: Arc::from("tasks"),
            replica_identity: b'd',
            columns,
        }
    }

    fn columns() -> Vec<ColumnInfo> {
        vec![
            ColumnInfo::new(1, "id".to_owned(), 23, -1),
            ColumnInfo::new(0, "body".to_owned(), 25, -1),
        ]
    }

    fn wire(
        decoder: &mut Decoder,
        wal_lsn: u64,
        message: StreamingReplicationMessage,
    ) -> Result<Vec<DecoderEvent>, DecodeError> {
        let bytes = encode_streaming_message_to_bytes(&message, 2);
        decoder.decode(PgLsn::new(wal_lsn), &bytes)
    }

    fn copy_data(start_lsn: u64, wal_end: u64, send_time: i64, message: M) -> Bytes {
        let payload =
            encode_streaming_message_to_bytes(&StreamingReplicationMessage::new(message), 2);
        let mut frame = Vec::with_capacity(25 + payload.len());
        frame.push(b'w');
        frame.extend_from_slice(&start_lsn.to_be_bytes());
        frame.extend_from_slice(&wal_end.to_be_bytes());
        frame.extend_from_slice(&send_time.to_be_bytes());
        frame.extend_from_slice(&payload);
        Bytes::from(frame)
    }

    fn normal(message: M) -> StreamingReplicationMessage {
        StreamingReplicationMessage::new(message)
    }

    fn streamed(xid: u32, message: M) -> StreamingReplicationMessage {
        StreamingReplicationMessage::new_streaming(message, xid)
    }

    fn streamed_data(xid: u32, owner_xid: u32, message: M) -> StreamingReplicationMessage {
        StreamingReplicationMessage::new_streaming_data(message, xid, owner_xid)
    }

    fn insert(id: &str, body: &str) -> M {
        M::Insert {
            relation_id: 7,
            tuple: TupleData::new(vec![
                ColumnData::text(id.as_bytes().to_vec()),
                ColumnData::text(body.as_bytes().to_vec()),
            ]),
        }
    }

    #[test]
    fn normal_fixture_emits_one_complete_patch_aware_transaction() {
        let mut decoder = decoder();
        let relation_events = wire(&mut decoder, 1, normal(relation(columns()))).unwrap();
        assert!(matches!(
            relation_events.as_slice(),
            [DecoderEvent::RelationObserved {
                layout_changed: true,
                ..
            }]
        ));
        assert!(wire(
            &mut decoder,
            2,
            normal(M::Begin {
                final_lsn: 20,
                timestamp: 99,
                xid: 42,
            }),
        )
        .unwrap()
        .is_empty());
        assert!(wire(
            &mut decoder,
            3,
            normal(M::Update {
                relation_id: 7,
                old_tuple: None,
                new_tuple: TupleData::new(vec![ColumnData::unchanged(), ColumnData::null()]),
                key_type: None,
            }),
        )
        .unwrap()
        .is_empty());
        assert!(wire(
            &mut decoder,
            4,
            normal(M::Message {
                flags: 1,
                lsn: 21,
                prefix: MUTATION_MARKER_PREFIX.to_owned(),
                content: vec![1, 2, 3].into(),
            }),
        )
        .unwrap()
        .is_empty());
        assert!(wire(
            &mut decoder,
            5,
            normal(M::Truncate {
                relation_ids: vec![7],
                flags: TRUNCATE_CASCADE | TRUNCATE_RESTART_IDENTITY,
            }),
        )
        .unwrap()
        .is_empty());

        let events = wire(
            &mut decoder,
            6,
            normal(M::Commit {
                flags: 0,
                commit_lsn: 22,
                end_lsn: 23,
                timestamp: 99,
            }),
        )
        .unwrap();
        let [DecoderEvent::TransactionCommitted(transaction)] = events.as_slice() else {
            panic!("expected one committed transaction")
        };
        assert_eq!(transaction.xid, 42);
        assert_eq!(transaction.mode, TransactionMode::Normal);
        assert_eq!(transaction.changes.len(), 3);
        let DecodedChange::Update { patch, .. } = &transaction.changes[0] else {
            panic!("expected update")
        };
        assert_eq!(
            patch.cells,
            vec![TupleCell::Absent, TupleCell::Value(CellValue::Null)]
        );
        let DecodedChange::LogicalMessage(marker) = &transaction.changes[1] else {
            panic!("expected marker")
        };
        assert_eq!(marker.kind, LogicalMessageKind::MutationMarker);
        assert!(matches!(
            &transaction.changes[2],
            DecodedChange::Truncate {
                cascade: true,
                restart_identity: true,
                ..
            }
        ));
    }

    #[test]
    fn streamed_fixture_stays_hidden_across_segments_until_stream_commit() {
        assert_eq!(CUT_MARKER_PREFIX, "sync.snapshot_cut.v1");

        let mut decoder = decoder();
        wire(&mut decoder, 1, normal(relation(columns()))).unwrap();
        assert!(wire(
            &mut decoder,
            2,
            streamed(
                77,
                M::StreamStart {
                    xid: 77,
                    first_segment: true,
                }
            ),
        )
        .unwrap()
        .is_empty());
        assert!(wire(
            &mut decoder,
            3,
            streamed(
                77,
                M::Insert {
                    relation_id: 7,
                    tuple: TupleData::new(vec![
                        ColumnData::text(b"1".to_vec()),
                        ColumnData::text(b"first".to_vec()),
                    ]),
                }
            ),
        )
        .unwrap()
        .is_empty());
        assert!(wire(&mut decoder, 4, normal(M::StreamStop))
            .unwrap()
            .is_empty());
        assert!(wire(
            &mut decoder,
            5,
            streamed(
                77,
                M::StreamStart {
                    xid: 77,
                    first_segment: false,
                }
            ),
        )
        .unwrap()
        .is_empty());
        assert!(wire(
            &mut decoder,
            6,
            streamed(
                77,
                M::Message {
                    flags: 1,
                    lsn: 30,
                    prefix: CUT_MARKER_PREFIX.to_owned(),
                    content: b"cut-1".to_vec().into(),
                }
            ),
        )
        .unwrap()
        .is_empty());
        wire(&mut decoder, 7, normal(M::StreamStop)).unwrap();
        let events = wire(
            &mut decoder,
            8,
            normal(M::StreamCommit {
                xid: 77,
                flags: 0,
                commit_lsn: 31,
                end_lsn: 32,
                timestamp: 100,
            }),
        )
        .unwrap();
        let [DecoderEvent::TransactionCommitted(transaction)] = events.as_slice() else {
            panic!("expected streamed commit")
        };
        assert_eq!(transaction.mode, TransactionMode::Streamed);
        assert_eq!(transaction.final_lsn, None);
        assert_eq!(transaction.changes.len(), 2);
        assert!(matches!(
            &transaction.changes[1],
            DecodedChange::LogicalMessage(LogicalMessage {
                kind: LogicalMessageKind::CutMarker,
                ..
            })
        ));
    }

    #[test]
    fn streamed_subtransaction_abort_discards_only_its_owned_changes() {
        let mut decoder = decoder();
        wire(&mut decoder, 1, normal(relation(columns()))).unwrap();
        wire(
            &mut decoder,
            2,
            streamed(
                77,
                M::StreamStart {
                    xid: 77,
                    first_segment: true,
                },
            ),
        )
        .unwrap();
        wire(
            &mut decoder,
            3,
            streamed_data(
                77,
                88,
                M::Insert {
                    relation_id: 7,
                    tuple: TupleData::new(vec![
                        ColumnData::text(b"1".to_vec()),
                        ColumnData::text(b"aborted".to_vec()),
                    ]),
                },
            ),
        )
        .unwrap();
        wire(&mut decoder, 4, normal(M::StreamStop)).unwrap();
        let events = wire(
            &mut decoder,
            5,
            normal(M::StreamAbort {
                xid: 77,
                subtransaction_xid: 88,
                abort_lsn: None,
                abort_timestamp: None,
            }),
        )
        .unwrap();
        assert_eq!(
            events,
            vec![DecoderEvent::StreamAborted {
                xid: 77,
                subtransaction_xid: 88,
            }]
        );
        wire(
            &mut decoder,
            6,
            streamed(
                77,
                M::StreamStart {
                    xid: 77,
                    first_segment: false,
                },
            ),
        )
        .unwrap();
        wire(
            &mut decoder,
            7,
            streamed_data(
                77,
                77,
                M::Insert {
                    relation_id: 7,
                    tuple: TupleData::new(vec![
                        ColumnData::text(b"2".to_vec()),
                        ColumnData::text(b"parent".to_vec()),
                    ]),
                },
            ),
        )
        .unwrap();
        wire(&mut decoder, 8, normal(M::StreamStop)).unwrap();
        let events = wire(
            &mut decoder,
            9,
            normal(M::StreamCommit {
                xid: 77,
                flags: 0,
                commit_lsn: 10,
                end_lsn: 11,
                timestamp: 12,
            }),
        )
        .unwrap();
        let [DecoderEvent::TransactionCommitted(transaction)] = events.as_slice() else {
            panic!("expected parent commit")
        };
        assert_eq!(transaction.changes.len(), 1);
        let DecodedChange::Insert { row, .. } = &transaction.changes[0] else {
            panic!("expected retained parent insert")
        };
        assert_eq!(row.cells[1], CellValue::Text(b"parent".to_vec()));
    }

    #[test]
    fn streamed_wire_preserves_distinct_top_and_data_xids_byte_for_byte() {
        let top_xid = 77_u32;
        let subtransaction_xid = 88_u32;
        let message = streamed_data(top_xid, subtransaction_xid, insert("1", "child"));
        let encoded = encode_streaming_message_to_bytes(&message, 2);
        assert_eq!(&encoded[1..5], &subtransaction_xid.to_be_bytes());

        let mut decoder = decoder();
        wire(&mut decoder, 1, normal(relation(columns()))).unwrap();
        wire(
            &mut decoder,
            2,
            streamed(
                top_xid,
                M::StreamStart {
                    xid: top_xid,
                    first_segment: true,
                },
            ),
        )
        .unwrap();
        decoder.decode(PgLsn::new(3), &encoded).unwrap();
        wire(&mut decoder, 4, normal(M::StreamStop)).unwrap();
        wire(
            &mut decoder,
            5,
            normal(M::StreamAbort {
                xid: top_xid,
                subtransaction_xid,
                abort_lsn: None,
                abort_timestamp: None,
            }),
        )
        .unwrap();
        let events = wire(
            &mut decoder,
            6,
            normal(M::StreamCommit {
                xid: top_xid,
                flags: 0,
                commit_lsn: 7,
                end_lsn: 8,
                timestamp: 9,
            }),
        )
        .unwrap();
        let [DecoderEvent::TransactionCommitted(transaction)] = events.as_slice() else {
            panic!("expected empty parent commit")
        };
        assert!(transaction.changes.is_empty());
    }

    #[test]
    fn nested_savepoint_abort_sequence_converges_to_parent_only_reference_state() {
        let mut decoder = decoder();
        wire(&mut decoder, 1, normal(relation(columns()))).unwrap();
        wire(
            &mut decoder,
            2,
            streamed(
                100,
                M::StreamStart {
                    xid: 100,
                    first_segment: true,
                },
            ),
        )
        .unwrap();
        for (lsn, owner_xid, id, body) in [
            (3, 100, "1", "top-before"),
            (4, 110, "2", "parent-savepoint"),
            (5, 120, "3", "nested-savepoint"),
        ] {
            wire(
                &mut decoder,
                lsn,
                streamed_data(100, owner_xid, insert(id, body)),
            )
            .unwrap();
        }
        wire(&mut decoder, 6, normal(M::StreamStop)).unwrap();
        wire(
            &mut decoder,
            7,
            normal(M::StreamAbort {
                xid: 100,
                subtransaction_xid: 120,
                abort_lsn: None,
                abort_timestamp: None,
            }),
        )
        .unwrap();
        wire(
            &mut decoder,
            8,
            streamed(
                100,
                M::StreamStart {
                    xid: 100,
                    first_segment: false,
                },
            ),
        )
        .unwrap();
        wire(
            &mut decoder,
            9,
            streamed_data(100, 110, insert("4", "parent-after-child-abort")),
        )
        .unwrap();
        wire(
            &mut decoder,
            10,
            streamed_data(100, 100, insert("5", "top-middle")),
        )
        .unwrap();
        wire(&mut decoder, 11, normal(M::StreamStop)).unwrap();
        wire(
            &mut decoder,
            12,
            normal(M::StreamAbort {
                xid: 100,
                subtransaction_xid: 110,
                abort_lsn: None,
                abort_timestamp: None,
            }),
        )
        .unwrap();
        wire(
            &mut decoder,
            13,
            streamed(
                100,
                M::StreamStart {
                    xid: 100,
                    first_segment: false,
                },
            ),
        )
        .unwrap();
        wire(
            &mut decoder,
            14,
            streamed_data(100, 100, insert("6", "top-after")),
        )
        .unwrap();
        wire(&mut decoder, 15, normal(M::StreamStop)).unwrap();
        let events = wire(
            &mut decoder,
            16,
            normal(M::StreamCommit {
                xid: 100,
                flags: 0,
                commit_lsn: 17,
                end_lsn: 18,
                timestamp: 19,
            }),
        )
        .unwrap();
        let [DecoderEvent::TransactionCommitted(transaction)] = events.as_slice() else {
            panic!("expected complete parent transaction")
        };
        let rows = transaction
            .changes
            .iter()
            .map(|change| match change {
                DecodedChange::Insert { row, .. } => (&row.cells[0], &row.cells[1]),
                _ => panic!("expected insert-only reference fixture"),
            })
            .collect::<Vec<_>>();
        assert_eq!(
            rows,
            [
                (
                    &CellValue::Text(b"1".to_vec()),
                    &CellValue::Text(b"top-before".to_vec())
                ),
                (
                    &CellValue::Text(b"5".to_vec()),
                    &CellValue::Text(b"top-middle".to_vec())
                ),
                (
                    &CellValue::Text(b"6".to_vec()),
                    &CellValue::Text(b"top-after".to_vec())
                ),
            ]
        );
    }

    #[test]
    fn top_abort_discards_streamed_relation_type_and_origin_metadata() {
        let mut decoder = decoder();
        wire(
            &mut decoder,
            1,
            streamed(
                77,
                M::StreamStart {
                    xid: 77,
                    first_segment: true,
                },
            ),
        )
        .unwrap();
        assert!(
            wire(&mut decoder, 2, streamed_data(77, 88, relation(columns())),)
                .unwrap()
                .is_empty()
        );
        assert!(wire(
            &mut decoder,
            3,
            streamed_data(
                77,
                88,
                M::Type {
                    type_id: 8_001,
                    namespace: "public".to_owned(),
                    type_name: "private_type".to_owned(),
                },
            ),
        )
        .unwrap()
        .is_empty());
        assert!(wire(
            &mut decoder,
            4,
            streamed(
                77,
                M::Origin {
                    origin_lsn: 4,
                    origin_name: "remote".to_owned(),
                },
            ),
        )
        .unwrap()
        .is_empty());
        assert!(!decoder.relations.contains_key(&7));
        assert!(!decoder.types.contains_key(&8_001));
        wire(&mut decoder, 5, normal(M::StreamStop)).unwrap();
        wire(
            &mut decoder,
            6,
            normal(M::StreamAbort {
                xid: 77,
                subtransaction_xid: 77,
                abort_lsn: None,
                abort_timestamp: None,
            }),
        )
        .unwrap();
        assert!(!decoder.relations.contains_key(&7));
        assert!(!decoder.types.contains_key(&8_001));
    }

    #[test]
    fn interleaved_streams_publish_compatible_metadata_only_at_each_commit() {
        let mut decoder = decoder();
        for (xid, lsn) in [(77, 1), (88, 4)] {
            wire(
                &mut decoder,
                lsn,
                streamed(
                    xid,
                    M::StreamStart {
                        xid,
                        first_segment: true,
                    },
                ),
            )
            .unwrap();
            wire(
                &mut decoder,
                lsn + 1,
                streamed_data(xid, xid, relation(columns())),
            )
            .unwrap();
            wire(&mut decoder, lsn + 2, normal(M::StreamStop)).unwrap();
        }
        assert!(!decoder.relations.contains_key(&7));

        for (xid, lsn) in [(88, 7), (77, 11)] {
            let events = wire(
                &mut decoder,
                lsn,
                normal(M::StreamCommit {
                    xid,
                    flags: 0,
                    commit_lsn: lsn + 1,
                    end_lsn: lsn + 2,
                    timestamp: i64::from(xid),
                }),
            )
            .unwrap();
            assert!(matches!(
                events.as_slice(),
                [
                    DecoderEvent::RelationObserved { .. },
                    DecoderEvent::TransactionCommitted(_)
                ]
            ));
        }
        let relation = decoder
            .relations
            .get(&7)
            .expect("committed relation metadata");
        assert_eq!(relation.layout_generation, 1);
        assert_eq!(relation.first_observed_wal_lsn, PgLsn::new(2));
        assert_eq!(relation.last_observed_wal_lsn, PgLsn::new(5));
    }

    #[test]
    fn subabort_rewrites_bounded_spool_and_reclaims_exact_accounting() {
        let limits = DecoderLimits {
            max_transaction_bytes: 2_048,
            max_in_memory_transaction_bytes: 8,
            max_transaction_spool_bytes: 8_192,
            max_total_buffered_transaction_bytes: 4_096,
            max_total_spool_bytes: 16_384,
            ..DecoderLimits::default()
        };
        let mut decoder = Decoder::new(2, limits).unwrap();
        wire(&mut decoder, 1, normal(relation(columns()))).unwrap();
        wire(
            &mut decoder,
            2,
            streamed(
                77,
                M::StreamStart {
                    xid: 77,
                    first_segment: true,
                },
            ),
        )
        .unwrap();
        for (lsn, owner_xid, id, fill) in [(3, 88, "1", b'x'), (4, 77, "2", b'y')] {
            wire(
                &mut decoder,
                lsn,
                streamed_data(
                    77,
                    owner_xid,
                    M::Insert {
                        relation_id: 7,
                        tuple: TupleData::new(vec![
                            ColumnData::text(id.as_bytes().to_vec()),
                            ColumnData::text(vec![fill; 96]),
                        ]),
                    },
                ),
            )
            .unwrap();
        }
        wire(&mut decoder, 5, normal(M::StreamStop)).unwrap();
        let before_abort = decoder.total_spool_bytes();
        assert!(before_abort > 192);
        wire(
            &mut decoder,
            6,
            normal(M::StreamAbort {
                xid: 77,
                subtransaction_xid: 88,
                abort_lsn: None,
                abort_timestamp: None,
            }),
        )
        .unwrap();
        let after_abort = decoder.total_spool_bytes();
        assert!(after_abort > 96);
        assert!(after_abort < before_abort);

        let events = wire(
            &mut decoder,
            7,
            normal(M::StreamCommit {
                xid: 77,
                flags: 0,
                commit_lsn: 8,
                end_lsn: 9,
                timestamp: 10,
            }),
        )
        .unwrap();
        let [DecoderEvent::TransactionCommitted(transaction)] = events.as_slice() else {
            panic!("expected retained top-level spool entry")
        };
        assert_eq!(transaction.changes.len(), 1);
        assert_eq!(decoder.total_spool_bytes(), 0);
    }

    #[test]
    fn subabort_removes_private_layout_before_parent_continues() {
        let mut decoder = decoder();
        wire(&mut decoder, 1, normal(relation(columns()))).unwrap();
        wire(
            &mut decoder,
            2,
            streamed(
                77,
                M::StreamStart {
                    xid: 77,
                    first_segment: true,
                },
            ),
        )
        .unwrap();
        let mut private_columns = columns();
        private_columns.push(ColumnInfo::new(0, "private".to_owned(), 25, -1));
        wire(
            &mut decoder,
            3,
            streamed_data(77, 88, relation(private_columns)),
        )
        .unwrap();
        wire(
            &mut decoder,
            4,
            streamed_data(
                77,
                88,
                M::Insert {
                    relation_id: 7,
                    tuple: TupleData::new(vec![
                        ColumnData::text(b"1".to_vec()),
                        ColumnData::text(b"child".to_vec()),
                        ColumnData::text(b"secret".to_vec()),
                    ]),
                },
            ),
        )
        .unwrap();
        wire(&mut decoder, 5, normal(M::StreamStop)).unwrap();
        wire(
            &mut decoder,
            6,
            normal(M::StreamAbort {
                xid: 77,
                subtransaction_xid: 88,
                abort_lsn: None,
                abort_timestamp: None,
            }),
        )
        .unwrap();
        wire(
            &mut decoder,
            7,
            streamed(
                77,
                M::StreamStart {
                    xid: 77,
                    first_segment: false,
                },
            ),
        )
        .unwrap();
        wire(
            &mut decoder,
            8,
            streamed_data(77, 77, insert("2", "parent")),
        )
        .unwrap();
        wire(&mut decoder, 9, normal(M::StreamStop)).unwrap();
        let events = wire(
            &mut decoder,
            10,
            normal(M::StreamCommit {
                xid: 77,
                flags: 0,
                commit_lsn: 11,
                end_lsn: 12,
                timestamp: 13,
            }),
        )
        .unwrap();
        let [DecoderEvent::TransactionCommitted(transaction)] = events.as_slice() else {
            panic!("expected parent commit without aborted metadata")
        };
        assert_eq!(transaction.changes.len(), 1);
        assert_eq!(decoder.relations.get(&7).unwrap().layout_generation, 1);
        assert_eq!(decoder.relations.get(&7).unwrap().columns.len(), 2);
    }

    #[test]
    fn malformed_stream_contexts_and_reuse_after_abort_poison() {
        let mut missing_data_xid = decoder();
        wire(&mut missing_data_xid, 1, normal(relation(columns()))).unwrap();
        missing_data_xid
            .accept_decoded(
                PgLsn::new(2),
                streamed(
                    77,
                    M::StreamStart {
                        xid: 77,
                        first_segment: true,
                    },
                ),
            )
            .unwrap();
        let malformed = StreamingReplicationMessage {
            message: insert("1", "missing-owner"),
            is_streaming: true,
            xid: Some(77),
            data_xid: None,
        };
        assert_eq!(
            missing_data_xid.accept_decoded(PgLsn::new(3), malformed),
            Err(DecodeError::InvalidStreamingContext)
        );
        assert!(missing_data_xid.poisoned);

        let mut reused = decoder();
        wire(&mut reused, 1, normal(relation(columns()))).unwrap();
        wire(
            &mut reused,
            2,
            streamed(
                77,
                M::StreamStart {
                    xid: 77,
                    first_segment: true,
                },
            ),
        )
        .unwrap();
        wire(
            &mut reused,
            3,
            streamed_data(77, 88, insert("1", "aborted")),
        )
        .unwrap();
        wire(&mut reused, 4, normal(M::StreamStop)).unwrap();
        wire(
            &mut reused,
            5,
            normal(M::StreamAbort {
                xid: 77,
                subtransaction_xid: 88,
                abort_lsn: None,
                abort_timestamp: None,
            }),
        )
        .unwrap();
        wire(
            &mut reused,
            6,
            streamed(
                77,
                M::StreamStart {
                    xid: 77,
                    first_segment: false,
                },
            ),
        )
        .unwrap();
        assert_eq!(
            wire(
                &mut reused,
                7,
                streamed_data(77, 88, insert("2", "invalid-reuse")),
            ),
            Err(DecodeError::DataForAbortedStreamedSubtransaction {
                xid: 77,
                subtransaction_xid: 88,
            })
        );
        assert!(reused.poisoned);
    }

    #[test]
    fn publication_filtered_subtransaction_abort_is_a_safe_noop() {
        let mut decoder = decoder();
        wire(&mut decoder, 1, normal(relation(columns()))).unwrap();
        wire(
            &mut decoder,
            2,
            streamed(
                77,
                M::StreamStart {
                    xid: 77,
                    first_segment: true,
                },
            ),
        )
        .unwrap();
        wire(
            &mut decoder,
            3,
            streamed_data(77, 77, insert("1", "parent")),
        )
        .unwrap();
        wire(&mut decoder, 4, normal(M::StreamStop)).unwrap();

        assert_eq!(
            wire(
                &mut decoder,
                5,
                normal(M::StreamAbort {
                    xid: 77,
                    subtransaction_xid: 88,
                    abort_lsn: None,
                    abort_timestamp: None,
                }),
            )
            .unwrap(),
            vec![DecoderEvent::StreamAborted {
                xid: 77,
                subtransaction_xid: 88,
            }]
        );

        wire(
            &mut decoder,
            6,
            streamed(
                77,
                M::StreamStart {
                    xid: 77,
                    first_segment: false,
                },
            ),
        )
        .unwrap();
        wire(
            &mut decoder,
            7,
            streamed_data(77, 77, insert("2", "parent-continued")),
        )
        .unwrap();
        wire(&mut decoder, 8, normal(M::StreamStop)).unwrap();
        let events = wire(
            &mut decoder,
            9,
            normal(M::StreamCommit {
                xid: 77,
                flags: 0,
                commit_lsn: 10,
                end_lsn: 11,
                timestamp: 12,
            }),
        )
        .unwrap();
        let [DecoderEvent::TransactionCommitted(transaction)] = events.as_slice() else {
            panic!("expected parent commit after filtered subtransaction abort")
        };
        assert_eq!(transaction.changes.len(), 2);
    }

    #[test]
    fn filtered_subtransaction_aborts_are_charged_and_do_not_rewrite_an_unowned_spool() {
        let limits = DecoderLimits {
            max_changes_per_transaction: 16,
            max_transaction_bytes: 4_096,
            max_in_memory_transaction_bytes: 8,
            max_transaction_spool_bytes: 8_192,
            max_total_buffered_transaction_bytes: 8_192,
            max_total_spool_bytes: 16_384,
            ..DecoderLimits::default()
        };
        let mut decoder = Decoder::new(2, limits).unwrap();
        wire(&mut decoder, 1, normal(relation(columns()))).unwrap();
        wire(
            &mut decoder,
            2,
            streamed(
                77,
                M::StreamStart {
                    xid: 77,
                    first_segment: true,
                },
            ),
        )
        .unwrap();
        wire(
            &mut decoder,
            3,
            streamed_data(77, 77, insert("1", &"x".repeat(1_500))),
        )
        .unwrap();
        wire(&mut decoder, 4, normal(M::StreamStop)).unwrap();

        let initial_spool_bytes = decoder.total_spool_bytes;
        let initial_buffered_bytes = decoder.total_buffered_transaction_bytes;
        let pending = decoder.streamed.get(&77).unwrap();
        let ChangeStore::Spool { owners, .. } = &pending.changes else {
            panic!("near-limit transaction should be spooled")
        };
        assert_eq!(owners.len(), 1);
        assert!(owners.contains_key(&77));

        for (offset, owner_xid) in (88_u32..103).enumerate() {
            wire(
                &mut decoder,
                5 + u64::try_from(offset).unwrap(),
                normal(M::StreamAbort {
                    xid: 77,
                    subtransaction_xid: owner_xid,
                    abort_lsn: None,
                    abort_timestamp: None,
                }),
            )
            .unwrap();
        }
        assert_eq!(decoder.total_spool_bytes, initial_spool_bytes);
        assert_eq!(
            decoder.total_buffered_transaction_bytes,
            initial_buffered_bytes + 15 * ABORTED_SUBTRANSACTION_ESTIMATED_BYTES
        );
        let pending = decoder.streamed.get(&77).unwrap();
        assert_eq!(pending.aborted_subtransactions.len(), 15);
        let ChangeStore::Spool { owners, .. } = &pending.changes else {
            panic!("transaction should remain spooled")
        };
        assert_eq!(owners.len(), 1);
        assert!(owners.contains_key(&77));

        assert_eq!(
            wire(
                &mut decoder,
                20,
                normal(M::StreamAbort {
                    xid: 77,
                    subtransaction_xid: 103,
                    abort_lsn: None,
                    abort_timestamp: None,
                }),
            ),
            Err(DecodeError::TooManyTransactionChanges { xid: 77, limit: 16 })
        );
        assert!(decoder.poisoned);
    }

    #[test]
    fn streamed_transactions_share_one_source_wide_memory_quota() {
        let limits = DecoderLimits {
            max_transaction_bytes: 32,
            max_in_memory_transaction_bytes: 16,
            max_total_buffered_transaction_bytes: 40,
            ..DecoderLimits::default()
        };
        let mut decoder = Decoder::new(2, limits).unwrap();
        wire(&mut decoder, 1, normal(relation(columns()))).unwrap();

        for (xid, wal_lsn) in [(77, 2), (88, 5)] {
            wire(
                &mut decoder,
                wal_lsn,
                streamed(
                    xid,
                    M::StreamStart {
                        xid,
                        first_segment: true,
                    },
                ),
            )
            .unwrap();
            let result = wire(
                &mut decoder,
                wal_lsn + 1,
                streamed(
                    xid,
                    M::Insert {
                        relation_id: 7,
                        tuple: TupleData::new(vec![
                            ColumnData::text(b"1".to_vec()),
                            ColumnData::text(vec![b'x'; 24]),
                        ]),
                    },
                ),
            );
            if xid == 77 {
                result.unwrap();
                wire(&mut decoder, wal_lsn + 2, normal(M::StreamStop)).unwrap();
            } else {
                assert!(matches!(
                    result,
                    Err(DecodeError::TotalBufferedTransactionsTooLarge { limit: 40 })
                ));
            }
        }
        assert!(decoder.poisoned);
    }

    #[test]
    fn spool_length_framing_rejects_oversize_truncation_and_trailing_bytes() {
        let mut oversized = tempfile::tempfile().unwrap();
        oversized
            .write_all(
                &u32::try_from(MAX_SPOOL_DECODE_BYTES + 1)
                    .unwrap()
                    .to_be_bytes(),
            )
            .unwrap();
        assert!(matches!(
            (ChangeStore::Spool {
                file: oversized,
                changes: 1,
                bytes: SPOOL_LENGTH_BYTES,
                owners: BTreeMap::new(),
            })
            .into_changes(),
            Err(DecodeError::SpoolDecode(_))
        ));

        let mut truncated = tempfile::tempfile().unwrap();
        truncated.write_all(&4_u32.to_be_bytes()).unwrap();
        truncated.write_all(&[1]).unwrap();
        assert!(matches!(
            (ChangeStore::Spool {
                file: truncated,
                changes: 1,
                bytes: SPOOL_LENGTH_BYTES + 1,
                owners: BTreeMap::new(),
            })
            .into_changes(),
            Err(DecodeError::SpoolIo(_))
        ));

        let mut trailing = tempfile::tempfile().unwrap();
        trailing.write_all(&[1]).unwrap();
        assert_eq!(
            (ChangeStore::Spool {
                file: trailing,
                changes: 0,
                bytes: 1,
                owners: BTreeMap::new(),
            })
            .into_changes(),
            Err(DecodeError::SpoolTrailingBytes)
        );
    }

    #[test]
    fn large_normal_transaction_spills_then_rehydrates_only_at_commit() {
        let limits = DecoderLimits {
            max_transaction_bytes: 1024,
            max_in_memory_transaction_bytes: 8,
            max_transaction_spool_bytes: 4096,
            max_total_buffered_transaction_bytes: 2048,
            max_total_spool_bytes: 8192,
            ..DecoderLimits::default()
        };
        let mut decoder = Decoder::new(2, limits).unwrap();
        wire(&mut decoder, 1, normal(relation(columns()))).unwrap();
        wire(
            &mut decoder,
            2,
            normal(M::Begin {
                final_lsn: 10,
                timestamp: 5,
                xid: 9,
            }),
        )
        .unwrap();
        wire(
            &mut decoder,
            3,
            normal(M::Insert {
                relation_id: 7,
                tuple: TupleData::new(vec![
                    ColumnData::text(b"1".to_vec()),
                    ColumnData::text(vec![b'x'; 64]),
                ]),
            }),
        )
        .unwrap();
        assert_eq!(spooled_transaction_count(&decoder), 1);
        assert!(decoder.total_spool_bytes() > 64);

        let events = wire(
            &mut decoder,
            4,
            normal(M::Commit {
                flags: 0,
                commit_lsn: 11,
                end_lsn: 12,
                timestamp: 5,
            }),
        )
        .unwrap();
        let [DecoderEvent::TransactionCommitted(transaction)] = events.as_slice() else {
            panic!("expected complete committed transaction")
        };
        assert_eq!(transaction.changes.len(), 1);
        let DecodedChange::Insert { row, .. } = &transaction.changes[0] else {
            panic!("expected insert")
        };
        assert_eq!(row.cells[1], CellValue::Text(vec![b'x'; 64]));
        assert_eq!(spooled_transaction_count(&decoder), 0);
        assert_eq!(decoder.total_spool_bytes(), 0);
    }

    #[test]
    fn spill_encoding_limit_poisoning_occurs_before_commit_visibility() {
        let limits = DecoderLimits {
            max_transaction_bytes: 80,
            max_in_memory_transaction_bytes: 8,
            max_transaction_spool_bytes: 80,
            max_total_buffered_transaction_bytes: 160,
            max_total_spool_bytes: 160,
            ..DecoderLimits::default()
        };
        let mut decoder = Decoder::new(2, limits).unwrap();
        wire(&mut decoder, 1, normal(relation(columns()))).unwrap();
        wire(
            &mut decoder,
            2,
            normal(M::Begin {
                final_lsn: 10,
                timestamp: 5,
                xid: 9,
            }),
        )
        .unwrap();
        let error = wire(
            &mut decoder,
            3,
            normal(M::Insert {
                relation_id: 7,
                tuple: TupleData::new(vec![
                    ColumnData::text(b"1".to_vec()),
                    ColumnData::text(vec![b'x'; 64]),
                ]),
            }),
        )
        .unwrap_err();
        assert!(matches!(
            error,
            DecodeError::TransactionSpoolTooLarge { xid: 9, limit: 80 }
        ));
        assert!(decoder.poisoned);
    }

    #[test]
    fn streamed_spill_survives_segments_and_is_released_on_abort() {
        let limits = DecoderLimits {
            max_transaction_bytes: 1024,
            max_in_memory_transaction_bytes: 8,
            max_transaction_spool_bytes: 4096,
            max_total_buffered_transaction_bytes: 2048,
            max_total_spool_bytes: 8192,
            ..DecoderLimits::default()
        };
        let mut decoder = Decoder::new(2, limits).unwrap();
        wire(&mut decoder, 1, normal(relation(columns()))).unwrap();
        wire(
            &mut decoder,
            2,
            streamed(
                77,
                M::StreamStart {
                    xid: 77,
                    first_segment: true,
                },
            ),
        )
        .unwrap();
        wire(
            &mut decoder,
            3,
            streamed(
                77,
                M::Insert {
                    relation_id: 7,
                    tuple: TupleData::new(vec![
                        ColumnData::text(b"1".to_vec()),
                        ColumnData::text(vec![b'x'; 64]),
                    ]),
                },
            ),
        )
        .unwrap();
        wire(&mut decoder, 4, normal(M::StreamStop)).unwrap();
        let spilled_bytes = decoder.total_spool_bytes();
        assert_eq!(spooled_transaction_count(&decoder), 1);
        assert!(spilled_bytes > 64);

        wire(
            &mut decoder,
            5,
            streamed(
                77,
                M::StreamStart {
                    xid: 77,
                    first_segment: false,
                },
            ),
        )
        .unwrap();
        wire(
            &mut decoder,
            6,
            streamed(
                77,
                M::Message {
                    flags: 1,
                    lsn: 30,
                    prefix: CUT_MARKER_PREFIX.to_owned(),
                    content: b"cut".to_vec().into(),
                },
            ),
        )
        .unwrap();
        wire(&mut decoder, 7, normal(M::StreamStop)).unwrap();
        assert!(decoder.total_spool_bytes() > spilled_bytes);

        let events = wire(
            &mut decoder,
            8,
            normal(M::StreamAbort {
                xid: 77,
                subtransaction_xid: 77,
                abort_lsn: Some(31),
                abort_timestamp: Some(5),
            }),
        )
        .unwrap();
        assert!(matches!(
            events.as_slice(),
            [DecoderEvent::StreamAborted { xid: 77, .. }]
        ));
        assert_eq!(spooled_transaction_count(&decoder), 0);
        assert_eq!(decoder.total_spool_bytes(), 0);
    }

    #[test]
    fn relation_layout_version_is_captured_by_each_change() {
        let mut decoder = decoder();
        wire(&mut decoder, 1, normal(relation(columns()))).unwrap();
        wire(
            &mut decoder,
            2,
            normal(M::Begin {
                final_lsn: 10,
                timestamp: 5,
                xid: 9,
            }),
        )
        .unwrap();
        wire(
            &mut decoder,
            3,
            normal(M::Insert {
                relation_id: 7,
                tuple: TupleData::new(vec![
                    ColumnData::text(b"1".to_vec()),
                    ColumnData::text(b"old".to_vec()),
                ]),
            }),
        )
        .unwrap();
        let mut changed = columns();
        changed.push(ColumnInfo::new(0, "done".to_owned(), 16, -1));
        wire(&mut decoder, 4, normal(relation(changed))).unwrap();
        wire(
            &mut decoder,
            5,
            normal(M::Insert {
                relation_id: 7,
                tuple: TupleData::new(vec![
                    ColumnData::text(b"2".to_vec()),
                    ColumnData::text(b"new".to_vec()),
                    ColumnData::text(b"f".to_vec()),
                ]),
            }),
        )
        .unwrap();
        let events = wire(
            &mut decoder,
            6,
            normal(M::Commit {
                flags: 0,
                commit_lsn: 11,
                end_lsn: 12,
                timestamp: 5,
            }),
        )
        .unwrap();
        let [DecoderEvent::TransactionCommitted(transaction)] = events.as_slice() else {
            panic!("expected commit")
        };
        let generations: Vec<_> = transaction
            .changes
            .iter()
            .map(|change| match change {
                DecodedChange::Insert { relation, .. } => relation.layout_generation,
                _ => unreachable!(),
            })
            .collect();
        assert_eq!(generations, [1, 2]);
    }

    #[test]
    fn reserved_nontransactional_marker_and_limits_fail_closed() {
        let mut decoder = decoder();
        let error = wire(
            &mut decoder,
            1,
            normal(M::Message {
                flags: 0,
                lsn: 1,
                prefix: CUT_MARKER_PREFIX.to_owned(),
                content: b"cut".to_vec().into(),
            }),
        )
        .unwrap_err();
        assert!(matches!(
            error,
            DecodeError::NonTransactionalReservedMarker(_)
        ));
        assert_eq!(
            decoder.accept_decoded(PgLsn::new(2), normal(M::StreamStop)),
            Err(DecodeError::Poisoned)
        );

        let limits = DecoderLimits {
            max_message_bytes: 1,
            ..DecoderLimits::default()
        };
        let mut decoder = Decoder::new(2, limits).unwrap();
        assert!(matches!(
            decoder.decode(PgLsn::new(1), &[0, 1]),
            Err(DecodeError::MessageTooLarge { .. })
        ));
        assert!(decoder.poisoned);
    }

    #[test]
    fn malformed_or_trailing_bytes_are_rejected_by_crate_backed_wire_boundary() {
        let mut decoder = decoder();
        let mut bytes = encode_streaming_message_to_bytes(
            &normal(M::Begin {
                final_lsn: 1,
                timestamp: 2,
                xid: 3,
            }),
            2,
        )
        .to_vec();
        bytes.push(0);
        assert_eq!(
            decoder.decode(PgLsn::new(1), &bytes),
            Err(DecodeError::NonCanonicalMessage)
        );
        assert!(decoder.poisoned);
    }

    #[test]
    fn copy_data_envelope_preserves_wal_positions_and_complete_transactions() {
        let mut decoder = decoder();
        let relation_frame = decoder
            .decode_copy_data(copy_data(10, 11, 12, relation(columns())))
            .unwrap();
        let ReplicationCopyData::Wal {
            start_lsn,
            wal_end,
            send_time_postgres_micros,
            events,
        } = relation_frame
        else {
            panic!("expected XLogData")
        };
        assert_eq!(start_lsn, PgLsn::new(10));
        assert_eq!(wal_end, PgLsn::new(11));
        assert_eq!(send_time_postgres_micros, 12);
        assert!(matches!(
            events.as_slice(),
            [DecoderEvent::RelationObserved { .. }]
        ));

        decoder
            .decode_copy_data(copy_data(
                13,
                14,
                15,
                M::Begin {
                    final_lsn: 20,
                    timestamp: 5,
                    xid: 9,
                },
            ))
            .unwrap();
        decoder
            .decode_copy_data(copy_data(
                16,
                17,
                18,
                M::Insert {
                    relation_id: 7,
                    tuple: TupleData::new(vec![
                        ColumnData::text(b"1".to_vec()),
                        ColumnData::text(b"task".to_vec()),
                    ]),
                },
            ))
            .unwrap();
        let committed = decoder
            .decode_copy_data(copy_data(
                20,
                22,
                23,
                M::Commit {
                    flags: 0,
                    commit_lsn: 20,
                    end_lsn: 21,
                    timestamp: 5,
                },
            ))
            .unwrap();
        let ReplicationCopyData::Wal { events, .. } = committed else {
            panic!("expected XLogData")
        };
        let [DecoderEvent::TransactionCommitted(transaction)] = events.as_slice() else {
            panic!("expected complete transaction")
        };
        assert_eq!(transaction.xid, 9);
        assert_eq!(transaction.first_observed_wal_lsn, PgLsn::new(13));
        assert_eq!(transaction.changes.len(), 1);
    }

    #[test]
    fn copy_data_keepalive_is_exact_and_malformed_frames_poison() {
        let mut keepalive = Vec::with_capacity(18);
        keepalive.push(b'k');
        keepalive.extend_from_slice(&44_u64.to_be_bytes());
        keepalive.extend_from_slice(&55_i64.to_be_bytes());
        keepalive.push(1);
        assert_eq!(
            decoder()
                .decode_copy_data(Bytes::from(keepalive.clone()))
                .unwrap(),
            ReplicationCopyData::Keepalive {
                wal_end: PgLsn::new(44),
                send_time_postgres_micros: 55,
                reply_requested: true,
            }
        );

        keepalive.push(0);
        let mut malformed_decoder = decoder();
        assert_eq!(
            malformed_decoder.decode_copy_data(Bytes::from(keepalive)),
            Err(DecodeError::KeepaliveFrameLength { actual: 19 })
        );
        assert!(malformed_decoder.poisoned);

        let mut unknown_decoder = decoder();
        assert_eq!(
            unknown_decoder.decode_copy_data(Bytes::from_static(b"x")),
            Err(DecodeError::UnknownCopyDataTag(b'x'))
        );
        assert!(unknown_decoder.poisoned);
    }
}
