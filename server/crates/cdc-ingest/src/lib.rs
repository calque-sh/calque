//! Fenced CDC append/acknowledgement state machine.
//!
//! It accepts only complete transactions emitted by `pgoutput-decoder`,
//! normalizes them through a source-specific codec, durably appends them, and
//! only then creates an ACK intent. The replication transport must revalidate
//! that intent immediately before sending PostgreSQL feedback.

use std::{error::Error, fmt, sync::Arc};

use canonical_log::{AppendOutcome, CanonicalEntry, LogError, PostgresFencedCanonicalLog};
use pgoutput_decoder::DecodedTransaction;
use postgres_types::PgLsn;
use sync_types::{CursorPosition, SourceEpoch, SourceId};
use thiserror::Error;

mod normalizer;

pub use normalizer::{
    ColumnCodec, PostgresTextCodec, SchemaNormalizer, TableCodec, POSTGRES_EPOCH_UNIX_MICROS,
};

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub struct LeaseEpoch(u64);

impl LeaseEpoch {
    pub fn new(value: u64) -> Result<Self, FenceError> {
        if value == 0 {
            return Err(FenceError::InvalidLeaseEpoch);
        }
        Ok(Self(value))
    }

    pub const fn get(self) -> u64 {
        self.0
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SourceLease {
    pub source_id: SourceId,
    pub source_epoch: SourceEpoch,
    pub lease_epoch: LeaseEpoch,
}

#[derive(Debug, Error, Clone, PartialEq, Eq)]
pub enum FenceError {
    #[error("lease epoch must be nonzero")]
    InvalidLeaseEpoch,
    #[error("no active lease exists for source {0}")]
    NoActiveLease(SourceId),
    #[error("lease {actual} is stale; active lease is {expected}")]
    StaleLease { expected: u64, actual: u64 },
    #[error("source epoch changed from {actual} to {expected}")]
    SourceEpochChanged {
        expected: SourceEpoch,
        actual: SourceEpoch,
    },
    #[error("fence authority lock is poisoned")]
    AuthorityPoisoned,
}

pub trait FenceAuthority: Send + Sync {
    fn validate(&self, lease: &SourceLease) -> Result<(), FenceError>;
}

#[derive(Debug, Error, Clone, PartialEq, Eq)]
#[error("canonical normalization failed: {message}")]
pub struct NormalizationError {
    message: String,
}

impl NormalizationError {
    pub fn new(message: impl Into<String>) -> Self {
        Self {
            message: message.into(),
        }
    }
}

pub trait TransactionNormalizer {
    fn normalize(
        &mut self,
        transaction: DecodedTransaction,
        source_epoch: &SourceEpoch,
    ) -> Result<CanonicalEntry, NormalizationError>;
}

#[derive(Debug, Error)]
pub enum AppendFailure {
    #[error(transparent)]
    CanonicalLog(#[from] LogError),
    #[error("durable append failed: {0}")]
    Other(Box<dyn Error + Send + Sync>),
}

/// One normalized source transaction and the WAL end that acknowledges it.
#[derive(Debug, Clone, PartialEq)]
pub struct DurableAppend {
    pub entry: CanonicalEntry,
    pub wal_end: PgLsn,
}

/// The outcome of one durable group: one append outcome per entry, in the
/// group's order.
#[derive(Debug, PartialEq, Eq)]
pub struct DurableAppendOutcome {
    outcomes: Vec<AppendOutcome>,
    checkpoint_is_durable: bool,
}

impl DurableAppendOutcome {
    pub const fn new(outcomes: Vec<AppendOutcome>, checkpoint_is_durable: bool) -> Self {
        Self {
            outcomes,
            checkpoint_is_durable,
        }
    }

    pub fn outcomes(&self) -> &[AppendOutcome] {
        &self.outcomes
    }

    /// True when the durable source checkpoint already covers the group's last
    /// WAL end.
    pub const fn checkpoint_is_durable(&self) -> bool {
        self.checkpoint_is_durable
    }
}

pub trait DurableAppender {
    /// Durably appends `group` in order, each entry as its own canonical
    /// record, and returns one outcome per entry in the same order.
    ///
    /// `group` is never empty. An error means the caller holds no
    /// acknowledgement for any entry of the group, whatever the appender
    /// managed to write before it failed; replay deduplicates what it did.
    fn append_durable(
        &mut self,
        group: Vec<DurableAppend>,
    ) -> Result<DurableAppendOutcome, AppendFailure>;
}

/// Appends the group in one PostgreSQL commit. The checkpoint is left to the
/// caller.
impl DurableAppender for PostgresFencedCanonicalLog {
    fn append_durable(
        &mut self,
        group: Vec<DurableAppend>,
    ) -> Result<DurableAppendOutcome, AppendFailure> {
        let entries = group.into_iter().map(|append| append.entry).collect();
        let outcomes = self.append_group(entries, None)?;
        Ok(DurableAppendOutcome::new(outcomes, false))
    }
}

/// An ACK candidate that can exist only after durable canonical append.
#[derive(Debug, PartialEq, Eq)]
pub struct PendingAck {
    lease: SourceLease,
    wal_end: PgLsn,
    canonical_position: CursorPosition,
    checkpoint_is_durable: bool,
}

/// Revalidated feedback fields. Construct this immediately before the
/// replication transport writes a status update.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AuthorizedAck {
    wal_end: PgLsn,
    canonical_position: CursorPosition,
    checkpoint_is_durable: bool,
}

impl AuthorizedAck {
    pub fn wal_end(&self) -> PgLsn {
        self.wal_end
    }

    pub fn canonical_position(&self) -> &CursorPosition {
        &self.canonical_position
    }

    pub const fn checkpoint_is_durable(&self) -> bool {
        self.checkpoint_is_durable
    }
}

#[derive(Debug, Error)]
pub enum IngestError {
    #[error(transparent)]
    Fence(#[from] FenceError),
    #[error(transparent)]
    Normalize(#[from] NormalizationError),
    #[error(transparent)]
    Append(#[from] AppendFailure),
    #[error("canonical transaction source epoch differs from the active lease")]
    CanonicalEpochMismatch,
    #[error("canonical transaction xid {canonical} differs from decoded xid {decoded}")]
    XidMismatch { decoded: u32, canonical: u32 },
    #[error("canonical commit LSN is invalid: {0}")]
    InvalidCommitLsn(String),
    #[error("canonical end LSN is invalid: {0}")]
    InvalidEndLsn(String),
    #[error("canonical commit LSN {canonical} differs from decoded commit LSN {decoded}")]
    CommitLsnMismatch { decoded: PgLsn, canonical: PgLsn },
    #[error("canonical end LSN {canonical} differs from decoded WAL end {decoded}")]
    EndLsnMismatch { decoded: PgLsn, canonical: PgLsn },
    #[error("transaction WAL end {wal_end} precedes commit LSN {commit_lsn}")]
    WalEndBeforeCommit { commit_lsn: PgLsn, wal_end: PgLsn },
    #[error("ACK WAL position regressed from {previous} to {next}")]
    AckRegression { previous: PgLsn, next: PgLsn },
    #[error("an ingest group holds no transactions")]
    EmptyGroup,
    #[error("the appender returned {actual} outcomes for a group of {expected}")]
    GroupOutcomeMismatch { expected: usize, actual: usize },
    #[error("ACK intent belongs to a different lease")]
    AckLeaseMismatch,
    #[error("ACK intent for {pending} was superseded by durable position {latest}")]
    AckSuperseded { pending: PgLsn, latest: PgLsn },
}

pub struct IngestSession<A, F, N> {
    appender: A,
    authority: Arc<F>,
    normalizer: N,
    lease: SourceLease,
    last_ack: Option<PgLsn>,
}

impl<A, F, N> fmt::Debug for IngestSession<A, F, N>
where
    A: fmt::Debug,
    F: fmt::Debug,
    N: fmt::Debug,
{
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("IngestSession")
            .field("appender", &self.appender)
            .field("authority", &self.authority)
            .field("normalizer", &self.normalizer)
            .field("lease", &self.lease)
            .field("last_ack", &self.last_ack)
            .finish()
    }
}

impl<A, F, N> IngestSession<A, F, N>
where
    A: DurableAppender,
    F: FenceAuthority,
    N: TransactionNormalizer,
{
    pub fn new(
        appender: A,
        authority: Arc<F>,
        normalizer: N,
        lease: SourceLease,
    ) -> Result<Self, IngestError> {
        authority.validate(&lease)?;
        Ok(Self {
            appender,
            authority,
            normalizer,
            lease,
            last_ack: None,
        })
    }

    /// Normalizes `group` and appends it as one durable group, in the order
    /// given, and returns the capability to acknowledge its last WAL end.
    ///
    /// The lease is validated before each transaction is normalized, so a
    /// fence lost partway through the group stops it before anything is
    /// appended, and again after the append. Every WAL end is checked against
    /// the one before it, the previous group's included, before the append.
    pub fn ingest_group(
        &mut self,
        group: Vec<DecodedTransaction>,
    ) -> Result<PendingAck, IngestError> {
        if group.is_empty() {
            return Err(IngestError::EmptyGroup);
        }
        let expected = group.len();
        let mut previous = self.last_ack;
        let mut appends = Vec::with_capacity(expected);
        for decoded in group {
            self.authority.validate(&self.lease)?;
            let decoded_xid = decoded.xid;
            let decoded_commit = PgLsn::from(decoded.commit_lsn.get());
            let wal_end = PgLsn::from(decoded.end_lsn.get());
            if wal_end < decoded_commit {
                return Err(IngestError::WalEndBeforeCommit {
                    commit_lsn: decoded_commit,
                    wal_end,
                });
            }
            if let Some(previous) = previous {
                if wal_end < previous {
                    return Err(IngestError::AckRegression {
                        previous,
                        next: wal_end,
                    });
                }
            }
            let canonical = self
                .normalizer
                .normalize(decoded, &self.lease.source_epoch)?;
            validate_identity(
                &canonical,
                &self.lease,
                decoded_xid,
                decoded_commit,
                wal_end,
            )?;
            appends.push(DurableAppend {
                entry: canonical,
                wal_end,
            });
            previous = Some(wal_end);
        }
        let wal_end = previous.ok_or(IngestError::EmptyGroup)?;
        let append = self.appender.append_durable(appends)?;
        let canonical_position = match append.outcomes() {
            [.., last] if append.outcomes().len() == expected => last.position().clone(),
            outcomes => {
                return Err(IngestError::GroupOutcomeMismatch {
                    expected,
                    actual: outcomes.len(),
                })
            }
        };

        // Revocation after append is safe: the durable records remain eligible
        // for idempotent replay, but this session receives no ACK capability.
        self.authority.validate(&self.lease)?;
        self.last_ack = Some(wal_end);
        Ok(PendingAck {
            lease: self.lease.clone(),
            wal_end,
            canonical_position,
            checkpoint_is_durable: append.checkpoint_is_durable(),
        })
    }

    pub fn authorize_ack(&self, pending: PendingAck) -> Result<AuthorizedAck, IngestError> {
        if pending.lease != self.lease {
            return Err(IngestError::AckLeaseMismatch);
        }
        self.authority.validate(&self.lease)?;
        if let Some(latest) = self.last_ack {
            if pending.wal_end != latest {
                return Err(IngestError::AckSuperseded {
                    pending: pending.wal_end,
                    latest,
                });
            }
        }
        Ok(AuthorizedAck {
            wal_end: pending.wal_end,
            canonical_position: pending.canonical_position,
            checkpoint_is_durable: pending.checkpoint_is_durable,
        })
    }
}

fn validate_identity(
    entry: &CanonicalEntry,
    lease: &SourceLease,
    decoded_xid: u32,
    decoded_commit: PgLsn,
    decoded_end: PgLsn,
) -> Result<(), IngestError> {
    let key = entry.key();
    if key.source_epoch != lease.source_epoch {
        return Err(IngestError::CanonicalEpochMismatch);
    }
    if key.postgres_xid != decoded_xid {
        return Err(IngestError::XidMismatch {
            decoded: decoded_xid,
            canonical: key.postgres_xid,
        });
    }
    let canonical_end = key
        .end_lsn
        .parse::<PgLsn>()
        .map_err(|_| IngestError::InvalidEndLsn(key.end_lsn.clone()))?;
    if canonical_end != decoded_end {
        return Err(IngestError::EndLsnMismatch {
            decoded: decoded_end,
            canonical: canonical_end,
        });
    }
    let commit_lsn = match entry {
        CanonicalEntry::Transaction(transaction) => &transaction.commit_lsn,
        CanonicalEntry::Barrier(barrier) => &barrier.commit_lsn,
    };
    let canonical_commit = commit_lsn
        .parse::<PgLsn>()
        .map_err(|_| IngestError::InvalidCommitLsn(commit_lsn.clone()))?;
    if canonical_commit != decoded_commit {
        return Err(IngestError::CommitLsnMismatch {
            decoded: decoded_commit,
            canonical: canonical_commit,
        });
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use canonical_log::{CanonicalBarrier, CanonicalTransaction, FileCanonicalLog, TransactionKey};
    use pgoutput_decoder::{PgLsn as DecoderLsn, TransactionMode};
    use std::{collections::BTreeMap, sync::Mutex};
    use tempfile::TempDir;

    /// Appends one entry at a time, so a failure can leave a prefix of the group
    /// durable. That prefix is never acknowledged, and replay deduplicates it.
    impl DurableAppender for FileCanonicalLog {
        fn append_durable(
            &mut self,
            group: Vec<DurableAppend>,
        ) -> Result<DurableAppendOutcome, AppendFailure> {
            let mut outcomes = Vec::with_capacity(group.len());
            for append in group {
                outcomes.push(match append.entry {
                    CanonicalEntry::Transaction(transaction) => self.append(transaction),
                    CanonicalEntry::Barrier(barrier) => self.append_barrier(barrier),
                }?);
            }
            Ok(DurableAppendOutcome::new(outcomes, false))
        }
    }

    /// Single-process fence authority: each acquire supersedes the previous
    /// lease for its source.
    #[derive(Debug, Default)]
    struct MemoryFenceAuthority {
        leases: Mutex<BTreeMap<SourceId, (SourceEpoch, LeaseEpoch)>>,
    }

    impl MemoryFenceAuthority {
        fn acquire(
            &self,
            source_id: SourceId,
            source_epoch: SourceEpoch,
        ) -> Result<SourceLease, FenceError> {
            let mut leases = self
                .leases
                .lock()
                .map_err(|_| FenceError::AuthorityPoisoned)?;
            let next = leases
                .get(&source_id)
                .map_or(1, |(_, lease)| lease.get().saturating_add(1));
            let lease_epoch = LeaseEpoch::new(next)?;
            leases.insert(source_id.clone(), (source_epoch.clone(), lease_epoch));
            Ok(SourceLease {
                source_id,
                source_epoch,
                lease_epoch,
            })
        }
    }

    impl FenceAuthority for MemoryFenceAuthority {
        fn validate(&self, lease: &SourceLease) -> Result<(), FenceError> {
            let leases = self
                .leases
                .lock()
                .map_err(|_| FenceError::AuthorityPoisoned)?;
            let (source_epoch, lease_epoch) = leases
                .get(&lease.source_id)
                .ok_or_else(|| FenceError::NoActiveLease(lease.source_id.clone()))?;
            if source_epoch != &lease.source_epoch {
                return Err(FenceError::SourceEpochChanged {
                    expected: source_epoch.clone(),
                    actual: lease.source_epoch.clone(),
                });
            }
            if lease_epoch != &lease.lease_epoch {
                return Err(FenceError::StaleLease {
                    expected: lease_epoch.get(),
                    actual: lease.lease_epoch.get(),
                });
            }
            Ok(())
        }
    }

    #[derive(Debug, Default)]
    struct PassthroughNormalizer;

    impl TransactionNormalizer for PassthroughNormalizer {
        fn normalize(
            &mut self,
            transaction: DecodedTransaction,
            source_epoch: &SourceEpoch,
        ) -> Result<CanonicalEntry, NormalizationError> {
            Ok(CanonicalEntry::Transaction(CanonicalTransaction {
                key: TransactionKey {
                    source_epoch: source_epoch.clone(),
                    end_lsn: PgLsn::from(transaction.end_lsn.get()).to_string(),
                    postgres_xid: transaction.xid,
                },
                transaction_id: format!("tx-{}", transaction.xid),
                begin_lsn: PgLsn::from(transaction.first_observed_wal_lsn.get()).to_string(),
                commit_lsn: PgLsn::from(transaction.commit_lsn.get()).to_string(),
                commit_time_unix_micros: 0,
                operations: Vec::new(),
                mutation_ids: Vec::new(),
            }))
        }
    }

    fn source_id() -> SourceId {
        SourceId::new("source-main").unwrap()
    }

    fn epoch() -> SourceEpoch {
        SourceEpoch::new("epoch-1").unwrap()
    }

    fn decoded(xid: u32, commit: u64, end: u64) -> DecodedTransaction {
        DecodedTransaction {
            xid,
            mode: TransactionMode::Normal,
            first_observed_wal_lsn: DecoderLsn::new(commit.saturating_sub(1)),
            final_lsn: Some(DecoderLsn::new(commit)),
            commit_lsn: DecoderLsn::new(commit),
            end_lsn: DecoderLsn::new(end),
            commit_time_postgres_micros: 0,
            changes: Vec::new(),
        }
    }

    fn session(
        directory: &TempDir,
        authority: Arc<MemoryFenceAuthority>,
    ) -> IngestSession<FileCanonicalLog, MemoryFenceAuthority, PassthroughNormalizer> {
        let lease = authority.acquire(source_id(), epoch()).unwrap();
        let log = FileCanonicalLog::open(directory.path(), source_id(), epoch()).unwrap();
        IngestSession::new(log, authority, PassthroughNormalizer, lease).unwrap()
    }

    #[test]
    fn ack_capability_exists_only_after_durable_append() {
        let directory = TempDir::new().unwrap();
        let authority = Arc::new(MemoryFenceAuthority::default());
        let mut session = session(&directory, authority);

        let pending = session.ingest_group(vec![decoded(7, 16, 20)]).unwrap();
        let ack = session.authorize_ack(pending).unwrap();
        assert_eq!(ack.wal_end(), PgLsn::from(20));
        assert_eq!(ack.canonical_position().sequence, 1);

        let log = session.appender;
        assert_eq!(
            log.read_after_bounded(0, None, 16, 1024 * 1024)
                .unwrap()
                .len(),
            1
        );
    }

    #[test]
    fn replayed_transaction_deduplicates_and_can_be_acknowledged() {
        let directory = TempDir::new().unwrap();
        let authority = Arc::new(MemoryFenceAuthority::default());
        let mut session = session(&directory, authority);
        let first = session.ingest_group(vec![decoded(7, 16, 20)]).unwrap();
        let replay = session.ingest_group(vec![decoded(7, 16, 20)]).unwrap();

        assert_eq!(first.canonical_position, replay.canonical_position);
        let log = session.appender;
        assert_eq!(log.head().sequence, 1);
    }

    #[test]
    fn stale_leader_cannot_append_or_authorize_an_existing_ack() {
        let directory = TempDir::new().unwrap();
        let authority = Arc::new(MemoryFenceAuthority::default());
        let mut session = session(&directory, Arc::clone(&authority));
        let pending = session.ingest_group(vec![decoded(7, 16, 20)]).unwrap();
        authority.acquire(source_id(), epoch()).unwrap();

        assert!(matches!(
            session.authorize_ack(pending),
            Err(IngestError::Fence(FenceError::StaleLease { .. }))
        ));
        assert!(matches!(
            session.ingest_group(vec![decoded(8, 21, 24)]),
            Err(IngestError::Fence(FenceError::StaleLease { .. }))
        ));
        let log = session.appender;
        assert_eq!(log.head().sequence, 1);
    }

    struct RevokingAppender {
        inner: FileCanonicalLog,
        authority: Arc<MemoryFenceAuthority>,
    }

    impl DurableAppender for RevokingAppender {
        fn append_durable(
            &mut self,
            group: Vec<DurableAppend>,
        ) -> Result<DurableAppendOutcome, AppendFailure> {
            let outcome = self.inner.append_durable(group)?;
            self.authority.acquire(source_id(), epoch()).unwrap();
            Ok(outcome)
        }
    }

    #[test]
    fn revocation_after_append_returns_no_ack_capability() {
        let directory = TempDir::new().unwrap();
        let authority = Arc::new(MemoryFenceAuthority::default());
        let lease = authority.acquire(source_id(), epoch()).unwrap();
        let log = FileCanonicalLog::open(directory.path(), source_id(), epoch()).unwrap();
        let appender = RevokingAppender {
            inner: log,
            authority: Arc::clone(&authority),
        };
        let mut session =
            IngestSession::new(appender, authority, PassthroughNormalizer, lease).unwrap();

        assert!(matches!(
            session.ingest_group(vec![decoded(7, 16, 20)]),
            Err(IngestError::Fence(FenceError::StaleLease { .. }))
        ));
        let appender = session.appender;
        assert_eq!(appender.inner.head().sequence, 1);
    }

    #[test]
    fn identity_and_ack_regressions_fail_before_append() {
        let directory = TempDir::new().unwrap();
        let authority = Arc::new(MemoryFenceAuthority::default());
        let mut session = session(&directory, authority);
        session.ingest_group(vec![decoded(7, 16, 20)]).unwrap();

        assert!(matches!(
            session.ingest_group(vec![decoded(8, 18, 19)]),
            Err(IngestError::AckRegression { .. })
        ));
        let log = session.appender;
        assert_eq!(log.head().sequence, 1);
    }

    fn logged_xids(log: &FileCanonicalLog) -> Vec<u32> {
        log.read_after_bounded(0, None, 16, 1024 * 1024)
            .unwrap()
            .iter()
            .map(|record| record.entry.key().postgres_xid)
            .collect()
    }

    #[test]
    fn a_group_appends_in_order_and_acknowledges_its_last_wal_end() {
        let directory = TempDir::new().unwrap();
        let authority = Arc::new(MemoryFenceAuthority::default());
        let mut session = session(&directory, authority);

        let pending = session
            .ingest_group(vec![
                decoded(7, 16, 20),
                decoded(8, 21, 24),
                decoded(9, 25, 28),
            ])
            .unwrap();
        assert_eq!(pending.wal_end, PgLsn::from(28));
        assert_eq!(pending.canonical_position.sequence, 3);
        assert_eq!(
            session.authorize_ack(pending).unwrap().wal_end(),
            PgLsn::from(28)
        );

        let log = session.appender;
        assert_eq!(logged_xids(&log), [7, 8, 9]);
    }

    #[test]
    fn a_replayed_group_deduplicates_and_extends_in_order() {
        let directory = TempDir::new().unwrap();
        let authority = Arc::new(MemoryFenceAuthority::default());
        let mut first = session(&directory, Arc::clone(&authority));
        first
            .ingest_group(vec![decoded(7, 16, 20), decoded(8, 21, 24)])
            .unwrap();
        drop(first);

        // A new owner replays from the slot's confirmed position, which never
        // passed the first group's acknowledgement.
        let mut replaying = session(&directory, authority);
        let replay = replaying
            .ingest_group(vec![
                decoded(7, 16, 20),
                decoded(8, 21, 24),
                decoded(9, 25, 28),
            ])
            .unwrap();
        assert_eq!(replay.wal_end, PgLsn::from(28));
        assert_eq!(replay.canonical_position.sequence, 3);
        let log = replaying.appender;
        assert_eq!(logged_xids(&log), [7, 8, 9]);
    }

    /// Takes the lease over after normalizing `revoke_after` transactions: the
    /// fence is lost partway through a group, before its append.
    struct RevokingNormalizer {
        inner: PassthroughNormalizer,
        authority: Arc<MemoryFenceAuthority>,
        revoke_after: usize,
        normalized: usize,
    }

    impl TransactionNormalizer for RevokingNormalizer {
        fn normalize(
            &mut self,
            transaction: DecodedTransaction,
            source_epoch: &SourceEpoch,
        ) -> Result<CanonicalEntry, NormalizationError> {
            let entry = self.inner.normalize(transaction, source_epoch)?;
            self.normalized += 1;
            if self.normalized == self.revoke_after {
                self.authority.acquire(source_id(), epoch()).unwrap();
            }
            Ok(entry)
        }
    }

    #[test]
    fn a_fence_lost_partway_through_a_group_appends_none_of_it() {
        let directory = TempDir::new().unwrap();
        let authority = Arc::new(MemoryFenceAuthority::default());
        let lease = authority.acquire(source_id(), epoch()).unwrap();
        let log = FileCanonicalLog::open(directory.path(), source_id(), epoch()).unwrap();
        let normalizer = RevokingNormalizer {
            inner: PassthroughNormalizer,
            authority: Arc::clone(&authority),
            revoke_after: 1,
            normalized: 0,
        };
        let mut session = IngestSession::new(log, authority, normalizer, lease).unwrap();

        assert!(matches!(
            session.ingest_group(vec![
                decoded(7, 16, 20),
                decoded(8, 21, 24),
                decoded(9, 25, 28),
            ]),
            Err(IngestError::Fence(FenceError::StaleLease { .. }))
        ));
        let IngestSession {
            appender: log,
            normalizer,
            ..
        } = session;
        assert_eq!(log.head().sequence, 0, "the group appended");
        assert_eq!(
            normalizer.normalized, 1,
            "the fence check before the second transaction did not stop the group"
        );
    }

    /// Appends the first `fail_at` entries of a group, then fails: a commit
    /// that fails partway through a group.
    struct PartialAppender {
        inner: FileCanonicalLog,
        fail_at: usize,
    }

    impl DurableAppender for PartialAppender {
        fn append_durable(
            &mut self,
            mut group: Vec<DurableAppend>,
        ) -> Result<DurableAppendOutcome, AppendFailure> {
            if group.len() > self.fail_at {
                group.truncate(self.fail_at);
                self.inner.append_durable(group)?;
                return Err(AppendFailure::Other(Box::new(std::io::Error::other(
                    "injected failure partway through a group",
                ))));
            }
            self.inner.append_durable(group)
        }
    }

    #[test]
    fn a_group_that_fails_partway_returns_no_ack_and_keeps_the_previous_one() {
        let directory = TempDir::new().unwrap();
        let authority = Arc::new(MemoryFenceAuthority::default());
        let lease = authority.acquire(source_id(), epoch()).unwrap();
        let appender = PartialAppender {
            inner: FileCanonicalLog::open(directory.path(), source_id(), epoch()).unwrap(),
            fail_at: 1,
        };
        let mut session =
            IngestSession::new(appender, authority, PassthroughNormalizer, lease).unwrap();
        let acknowledged = session.ingest_group(vec![decoded(7, 16, 20)]).unwrap();

        assert!(matches!(
            session.ingest_group(vec![decoded(8, 21, 24), decoded(9, 25, 28)]),
            Err(IngestError::Append(AppendFailure::Other(_)))
        ));
        // The failed group took no acknowledgement: the one before it is still
        // the latest.
        assert_eq!(
            session.authorize_ack(acknowledged).unwrap().wal_end(),
            PgLsn::from(20)
        );
        let appender = session.appender;
        assert_eq!(appender.inner.head().sequence, 2);
    }

    #[test]
    fn a_wal_end_regression_inside_a_group_fails_before_append() {
        let directory = TempDir::new().unwrap();
        let authority = Arc::new(MemoryFenceAuthority::default());
        let mut session = session(&directory, authority);

        assert!(matches!(
            session.ingest_group(vec![decoded(8, 21, 24), decoded(9, 18, 19)]),
            Err(IngestError::AckRegression { .. })
        ));
        assert!(matches!(
            session.ingest_group(Vec::new()),
            Err(IngestError::EmptyGroup)
        ));
        let log = session.appender;
        assert_eq!(log.head().sequence, 0);
    }

    struct FailingAppender;

    impl DurableAppender for FailingAppender {
        fn append_durable(
            &mut self,
            _group: Vec<DurableAppend>,
        ) -> Result<DurableAppendOutcome, AppendFailure> {
            Err(AppendFailure::Other(Box::new(std::io::Error::other(
                "injected durable append failure",
            ))))
        }
    }

    #[test]
    fn append_failure_returns_no_ack_capability() {
        let authority = Arc::new(MemoryFenceAuthority::default());
        let lease = authority.acquire(source_id(), epoch()).unwrap();
        let mut session =
            IngestSession::new(FailingAppender, authority, PassthroughNormalizer, lease).unwrap();

        assert!(matches!(
            session.ingest_group(vec![decoded(7, 16, 20)]),
            Err(IngestError::Append(AppendFailure::Other(_)))
        ));
    }

    #[derive(Default)]
    struct WrongXidNormalizer(PassthroughNormalizer);

    impl TransactionNormalizer for WrongXidNormalizer {
        fn normalize(
            &mut self,
            transaction: DecodedTransaction,
            source_epoch: &SourceEpoch,
        ) -> Result<CanonicalEntry, NormalizationError> {
            let mut entry = self.0.normalize(transaction, source_epoch)?;
            match &mut entry {
                CanonicalEntry::Transaction(transaction) => {
                    transaction.key.postgres_xid = transaction.key.postgres_xid.saturating_add(1);
                }
                CanonicalEntry::Barrier(barrier) => {
                    barrier.key.postgres_xid = barrier.key.postgres_xid.saturating_add(1);
                }
            }
            Ok(entry)
        }
    }

    #[test]
    fn normalized_identity_mismatch_fails_before_append() {
        let directory = TempDir::new().unwrap();
        let authority = Arc::new(MemoryFenceAuthority::default());
        let lease = authority.acquire(source_id(), epoch()).unwrap();
        let log = FileCanonicalLog::open(directory.path(), source_id(), epoch()).unwrap();
        let mut session =
            IngestSession::new(log, authority, WrongXidNormalizer::default(), lease).unwrap();

        assert!(matches!(
            session.ingest_group(vec![decoded(7, 16, 20)]),
            Err(IngestError::XidMismatch {
                decoded: 7,
                canonical: 8
            })
        ));
        let log = session.appender;
        assert_eq!(log.head().sequence, 0);
    }

    #[test]
    fn older_pending_ack_is_superseded_by_a_newer_durable_append() {
        let directory = TempDir::new().unwrap();
        let authority = Arc::new(MemoryFenceAuthority::default());
        let mut session = session(&directory, authority);
        let older = session.ingest_group(vec![decoded(7, 16, 20)]).unwrap();
        let newer = session.ingest_group(vec![decoded(8, 21, 24)]).unwrap();

        assert!(matches!(
            session.authorize_ack(older),
            Err(IngestError::AckSuperseded { pending, latest })
                if pending == PgLsn::from(20) && latest == PgLsn::from(24)
        ));
        assert_eq!(
            session.authorize_ack(newer).unwrap().wal_end(),
            PgLsn::from(24)
        );
    }

    struct BarrierNormalizer;

    impl TransactionNormalizer for BarrierNormalizer {
        fn normalize(
            &mut self,
            transaction: DecodedTransaction,
            source_epoch: &SourceEpoch,
        ) -> Result<CanonicalEntry, NormalizationError> {
            Ok(CanonicalEntry::Barrier(CanonicalBarrier {
                key: TransactionKey {
                    source_epoch: source_epoch.clone(),
                    end_lsn: PgLsn::from(transaction.end_lsn.get()).to_string(),
                    postgres_xid: transaction.xid,
                },
                build_id: "build-1".to_owned(),
                nonce: "nonce-1".to_owned(),
                message_lsn: "0/F".to_owned(),
                commit_lsn: PgLsn::from(transaction.commit_lsn.get()).to_string(),
                commit_time_unix_micros: 0,
            }))
        }
    }

    #[test]
    fn cut_marker_normalization_can_map_to_one_durable_barrier_cursor() {
        let directory = TempDir::new().unwrap();
        let authority = Arc::new(MemoryFenceAuthority::default());
        let lease = authority.acquire(source_id(), epoch()).unwrap();
        let log = FileCanonicalLog::open(directory.path(), source_id(), epoch()).unwrap();
        let mut session = IngestSession::new(log, authority, BarrierNormalizer, lease).unwrap();

        let pending = session.ingest_group(vec![decoded(7, 16, 20)]).unwrap();
        assert_eq!(pending.canonical_position.sequence, 1);
        let log = session.appender;
        assert!(matches!(
            &log.read_after_bounded(0, None, 16, 1024 * 1024)
                .unwrap()[0]
                .entry,
            CanonicalEntry::Barrier(CanonicalBarrier {
                build_id,
                nonce,
                ..
            }) if build_id == "build-1" && nonce == "nonce-1"
        ));
    }

    /// Rewrites one field of the entry the passthrough normalizer produces.
    struct TamperingNormalizer(fn(&mut CanonicalTransaction));

    impl TransactionNormalizer for TamperingNormalizer {
        fn normalize(
            &mut self,
            transaction: DecodedTransaction,
            source_epoch: &SourceEpoch,
        ) -> Result<CanonicalEntry, NormalizationError> {
            let mut entry = PassthroughNormalizer.normalize(transaction, source_epoch)?;
            if let CanonicalEntry::Transaction(transaction) = &mut entry {
                (self.0)(transaction);
            }
            Ok(entry)
        }
    }

    #[test]
    fn a_wal_position_the_entry_does_not_carry_fails_before_append() {
        type Tamper = fn(&mut CanonicalTransaction);
        type Refusal = fn(&IngestError) -> bool;
        // decoded(7, 16, 20) commits at 0/10 and ends at 0/14.
        let cases: [(&str, DecodedTransaction, Tamper, Refusal); 6] = [
            (
                "a WAL end before its commit",
                decoded(7, 20, 16),
                |_| {},
                |error| {
                    matches!(error, IngestError::WalEndBeforeCommit { commit_lsn, wal_end }
                        if *commit_lsn == PgLsn::from(20) && *wal_end == PgLsn::from(16))
                },
            ),
            (
                "another source epoch",
                decoded(7, 16, 20),
                |transaction| transaction.key.source_epoch = SourceEpoch::new("epoch-2").unwrap(),
                |error| matches!(error, IngestError::CanonicalEpochMismatch),
            ),
            (
                "an end LSN that does not parse",
                decoded(7, 16, 20),
                |transaction| transaction.key.end_lsn = "end".to_owned(),
                |error| matches!(error, IngestError::InvalidEndLsn(lsn) if lsn == "end"),
            ),
            (
                "another end LSN",
                decoded(7, 16, 20),
                |transaction| transaction.key.end_lsn = "0/15".to_owned(),
                |error| {
                    matches!(error, IngestError::EndLsnMismatch { decoded, canonical }
                        if *decoded == PgLsn::from(20) && *canonical == PgLsn::from(21))
                },
            ),
            (
                "a commit LSN that does not parse",
                decoded(7, 16, 20),
                |transaction| transaction.commit_lsn = "commit".to_owned(),
                |error| matches!(error, IngestError::InvalidCommitLsn(lsn) if lsn == "commit"),
            ),
            (
                "another commit LSN",
                decoded(7, 16, 20),
                |transaction| transaction.commit_lsn = "0/11".to_owned(),
                |error| {
                    matches!(error, IngestError::CommitLsnMismatch { decoded, canonical }
                        if *decoded == PgLsn::from(16) && *canonical == PgLsn::from(17))
                },
            ),
        ];
        for (case, transaction, tamper, refused) in cases {
            let directory = TempDir::new().unwrap();
            let authority = Arc::new(MemoryFenceAuthority::default());
            let lease = authority.acquire(source_id(), epoch()).unwrap();
            let log = FileCanonicalLog::open(directory.path(), source_id(), epoch()).unwrap();
            let mut session =
                IngestSession::new(log, authority, TamperingNormalizer(tamper), lease).unwrap();

            let error = session.ingest_group(vec![transaction]).expect_err(case);
            assert!(refused(&error), "{case}: {error}");
            assert_eq!(session.last_ack, None, "{case}: an ACK position was kept");
            assert_eq!(session.appender.head().sequence, 0, "{case}: appended");
        }
    }

    #[test]
    fn a_pending_ack_from_the_previous_lease_is_refused_by_the_next_owner() {
        let directory = TempDir::new().unwrap();
        let authority = Arc::new(MemoryFenceAuthority::default());
        let mut previous = session(&directory, Arc::clone(&authority));
        let pending = previous.ingest_group(vec![decoded(7, 16, 20)]).unwrap();
        drop(previous);

        let next = session(&directory, authority);
        assert!(matches!(
            next.authorize_ack(pending),
            Err(IngestError::AckLeaseMismatch)
        ));
    }

    #[test]
    fn a_zero_lease_epoch_is_refused() {
        assert_eq!(LeaseEpoch::new(0), Err(FenceError::InvalidLeaseEpoch));
        assert_eq!(LeaseEpoch::new(1).map(LeaseEpoch::get), Ok(1));
    }
}
