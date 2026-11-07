use std::{
    collections::{BTreeSet, VecDeque},
    panic::{catch_unwind, resume_unwind, AssertUnwindSafe},
    path::{Path, PathBuf},
    sync::{Arc, Mutex},
    time::{Duration, Instant},
};

use authz::{AuthorizationPolicy, PolicyHash};
use canonical_log::{
    AppendOutcome, CanonicalEntry, FencedWriterIdentity, LogError, PostgresAppendBoundary,
    PostgresAppendObserver, PostgresFencedCanonicalLog,
};
use cdc_ingest::{
    AppendFailure, DurableAppend, DurableAppendOutcome, DurableAppender,
    FenceAuthority as IngestFenceAuthority, FenceError as IngestFenceError, IngestError,
    IngestSession, LeaseEpoch, SchemaNormalizer, SourceLease,
};
use mutation_api::{mark_canonical_receipts, MutationBatchError};
use pg_walstream::{
    CancellationToken, PgReplicationConnection, ReplicationError, StandbyStatusUpdateBoundary,
    StandbyStatusUpdateObserver,
};
use pgoutput_decoder::{
    DecodeError, DecodedTransaction, Decoder, DecoderEvent, DecoderLimits, ReplicationCopyData,
};
use postgres::Client;
use source_control::SchemaRegistry;
use sync_types::{CursorPosition, MutationId, SourceEpoch, SourceId};
use thiserror::Error;

use crate::{ActiveLease, ActiveSourceSession, FenceAuthority, FenceError, PostgresFenceStore};

const MAX_BOUNDED_TRANSACTIONS: usize = 10_000;
const MAX_INGEST_DURATION: Duration = Duration::from_secs(300);
const FEEDBACK_FLUSH_TIMEOUT: Duration = Duration::from_secs(2);
/// The most source transactions one append group holds.
const MAX_GROUP_TRANSACTIONS: usize = 256;
/// The most CopyData bytes read to fill one append group after its first
/// transaction.
const MAX_GROUP_READ_BYTES: usize = 8 * 1024 * 1024;

pub(crate) struct PersistentIngestState {
    decoder: Option<Decoder>,
    decoder_limits: Option<DecoderLimits>,
    pending_events: VecDeque<DecoderEvent>,
    received_lsn: u64,
    /// The WAL position PostgreSQL had flushed when this session started its
    /// stream, until the stream has reached it.
    start_flushed_lsn: Option<u64>,
    ingest: Option<RuntimeIngestSession>,
    authorization_policy: Option<IngestAuthorizationPolicy>,
    observer: Arc<SessionObserver>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct IngestAuthorizationPolicy {
    generation: u64,
    hash: PolicyHash,
}

impl From<&AuthorizationPolicy> for IngestAuthorizationPolicy {
    fn from(policy: &AuthorizationPolicy) -> Self {
        Self {
            generation: policy.generation(),
            hash: *policy.hash(),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum PersistentIngestBoundary {
    TransactionSpilled,
    BeforeNormalize,
    AfterNormalize,
    BeforeCanonicalAppend,
    CanonicalFenceAuthorized,
    CanonicalSequenceAllocated,
    CanonicalRecordInserted,
    CanonicalHeadUpdated,
    CanonicalTransactionCommitted,
    AfterCanonicalAppend,
    BeforeReceiptMark,
    AfterReceiptMark,
    BeforeCheckpoint,
    AfterCheckpoint,
    BeforeFinalFence,
    AfterFinalFence,
    BeforeFeedbackEncode,
    AfterFeedbackEncode,
    BeforeFeedbackCopyData,
    FeedbackCopyDataQueued,
    BeforeFeedbackTransportFlush,
    FeedbackTransportFlushed,
}

impl PersistentIngestBoundary {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::TransactionSpilled => "TransactionSpilled",
            Self::BeforeNormalize => "BeforeNormalize",
            Self::AfterNormalize => "AfterNormalize",
            Self::BeforeCanonicalAppend => "BeforeCanonicalAppend",
            Self::CanonicalFenceAuthorized => "CanonicalFenceAuthorized",
            Self::CanonicalSequenceAllocated => "CanonicalSequenceAllocated",
            Self::CanonicalRecordInserted => "CanonicalRecordInserted",
            Self::CanonicalHeadUpdated => "CanonicalHeadUpdated",
            Self::CanonicalTransactionCommitted => "CanonicalTransactionCommitted",
            Self::AfterCanonicalAppend => "AfterCanonicalAppend",
            Self::BeforeReceiptMark => "BeforeReceiptMark",
            Self::AfterReceiptMark => "AfterReceiptMark",
            Self::BeforeCheckpoint => "BeforeCheckpoint",
            Self::AfterCheckpoint => "AfterCheckpoint",
            Self::BeforeFinalFence => "BeforeFinalFence",
            Self::AfterFinalFence => "AfterFinalFence",
            Self::BeforeFeedbackEncode => "BeforeFeedbackEncode",
            Self::AfterFeedbackEncode => "AfterFeedbackEncode",
            Self::BeforeFeedbackCopyData => "BeforeFeedbackCopyData",
            Self::FeedbackCopyDataQueued => "FeedbackCopyDataQueued",
            Self::BeforeFeedbackTransportFlush => "BeforeFeedbackTransportFlush",
            Self::FeedbackTransportFlushed => "FeedbackTransportFlushed",
        }
    }

    pub fn parse(value: &str) -> Option<Self> {
        Some(match value {
            "TransactionSpilled" => Self::TransactionSpilled,
            "BeforeNormalize" => Self::BeforeNormalize,
            "AfterNormalize" => Self::AfterNormalize,
            "BeforeCanonicalAppend" => Self::BeforeCanonicalAppend,
            "CanonicalFenceAuthorized" => Self::CanonicalFenceAuthorized,
            "CanonicalSequenceAllocated" => Self::CanonicalSequenceAllocated,
            "CanonicalRecordInserted" => Self::CanonicalRecordInserted,
            "CanonicalHeadUpdated" => Self::CanonicalHeadUpdated,
            "CanonicalTransactionCommitted" => Self::CanonicalTransactionCommitted,
            "AfterCanonicalAppend" => Self::AfterCanonicalAppend,
            "BeforeReceiptMark" => Self::BeforeReceiptMark,
            "AfterReceiptMark" => Self::AfterReceiptMark,
            "BeforeCheckpoint" => Self::BeforeCheckpoint,
            "AfterCheckpoint" => Self::AfterCheckpoint,
            "BeforeFinalFence" => Self::BeforeFinalFence,
            "AfterFinalFence" => Self::AfterFinalFence,
            "BeforeFeedbackEncode" => Self::BeforeFeedbackEncode,
            "AfterFeedbackEncode" => Self::AfterFeedbackEncode,
            "BeforeFeedbackCopyData" => Self::BeforeFeedbackCopyData,
            "FeedbackCopyDataQueued" => Self::FeedbackCopyDataQueued,
            "BeforeFeedbackTransportFlush" => Self::BeforeFeedbackTransportFlush,
            "FeedbackTransportFlushed" => Self::FeedbackTransportFlushed,
            _ => return None,
        })
    }

    pub const fn is_feedback_path(self) -> bool {
        matches!(
            self,
            Self::BeforeFinalFence
                | Self::AfterFinalFence
                | Self::BeforeFeedbackEncode
                | Self::AfterFeedbackEncode
                | Self::BeforeFeedbackCopyData
                | Self::FeedbackCopyDataQueued
                | Self::BeforeFeedbackTransportFlush
                | Self::FeedbackTransportFlushed
        )
    }
}

impl std::fmt::Display for PersistentIngestBoundary {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str(self.as_str())
    }
}

/// Trusted runtime callbacks invoked synchronously at correctness boundaries.
/// The default is a no-op; production may publish durable-head hints, while
/// deterministic process tests may block before takeover or kill.
pub trait PersistentIngestObserver: std::fmt::Debug + Send + Sync + 'static {
    fn observe(&self, boundary: PersistentIngestBoundary);

    /// Reports a newly committed canonical-log position before ingest proceeds
    /// to receipt marking, checkpoint advancement, or PostgreSQL feedback.
    fn observe_durable_append(&self, _position: &CursorPosition) {}

    /// Reports how many entries one canonical-log commit appended. Entries the
    /// log already held, which a replay deduplicates, are not counted.
    fn observe_appended_entries(&self, _entries: usize) {}
}

#[derive(Debug)]
struct NoopIngestObserver;

impl PersistentIngestObserver for NoopIngestObserver {
    fn observe(&self, _boundary: PersistentIngestBoundary) {}
}

#[derive(Debug)]
struct SessionObserver(Mutex<Arc<dyn PersistentIngestObserver>>);

impl SessionObserver {
    fn new() -> Self {
        Self(Mutex::new(Arc::new(NoopIngestObserver)))
    }

    fn replace(&self, observer: Arc<dyn PersistentIngestObserver>) {
        *self.0.lock().expect("ingest observer mutex poisoned") = observer;
    }
}

impl PersistentIngestObserver for SessionObserver {
    fn observe(&self, boundary: PersistentIngestBoundary) {
        let observer = Arc::clone(&self.0.lock().expect("ingest observer mutex poisoned"));
        observer.observe(boundary);
    }

    fn observe_durable_append(&self, position: &CursorPosition) {
        let observer = Arc::clone(&self.0.lock().expect("ingest observer mutex poisoned"));
        observer.observe_durable_append(position);
    }

    fn observe_appended_entries(&self, entries: usize) {
        let observer = Arc::clone(&self.0.lock().expect("ingest observer mutex poisoned"));
        observer.observe_appended_entries(entries);
    }
}

impl PersistentIngestState {
    pub(crate) fn new(initial_lsn: u64, start_flushed_lsn: u64) -> Self {
        Self {
            decoder: None,
            decoder_limits: None,
            pending_events: VecDeque::new(),
            received_lsn: initial_lsn,
            start_flushed_lsn: Some(start_flushed_lsn),
            ingest: None,
            authorization_policy: None,
            observer: Arc::new(SessionObserver::new()),
        }
    }

    pub(crate) fn reset_terminal(&mut self) {
        self.decoder = None;
        self.decoder_limits = None;
        self.pending_events.clear();
        self.ingest = None;
        self.authorization_policy = None;
        self.observer.replace(Arc::new(NoopIngestObserver));
    }

    fn initialize_decoder(
        &mut self,
        profile: source_control::PgoutputLaunchProfile,
        limits: &DecoderLimits,
    ) -> Result<(), PersistentIngestError> {
        if self
            .decoder_limits
            .as_ref()
            .is_some_and(|set| set != limits)
        {
            return Err(PersistentIngestError::InvalidConfig);
        }
        if self.decoder.is_none() {
            self.decoder = Some(Decoder::for_launch(profile, limits.clone())?);
            self.decoder_limits = Some(limits.clone());
        }
        Ok(())
    }
}

#[derive(Debug, Clone)]
pub struct PersistentIngestConfig {
    decoder_limits: DecoderLimits,
    max_transactions: usize,
    timeout: Duration,
    ends_once_current: bool,
    observer: Arc<dyn PersistentIngestObserver>,
    /// The state directory of the calque process the session serves, which
    /// the canonical log names when it refuses a log an earlier release wrote.
    state_dir: PathBuf,
}

impl PersistentIngestConfig {
    pub fn new(state_dir: &Path) -> Self {
        Self {
            decoder_limits: DecoderLimits::default(),
            max_transactions: 1,
            timeout: Duration::from_secs(30),
            ends_once_current: false,
            observer: Arc::new(NoopIngestObserver),
            state_dir: state_dir.to_path_buf(),
        }
    }

    pub fn with_bounds(
        mut self,
        max_transactions: usize,
        timeout: Duration,
    ) -> Result<Self, PersistentIngestError> {
        if max_transactions == 0
            || max_transactions > MAX_BOUNDED_TRANSACTIONS
            || timeout.is_zero()
            || timeout > MAX_INGEST_DURATION
        {
            return Err(PersistentIngestError::InvalidConfig);
        }
        self.max_transactions = max_transactions;
        self.timeout = timeout;
        Ok(self)
    }

    /// Ends a cycle before its timeout once the session is current: its stream
    /// has reached the WAL position PostgreSQL had flushed when the session
    /// started it, every transaction received is appended, and nothing more
    /// is waiting. A session is current that way once; its later cycles wait
    /// out their timeout.
    pub fn ending_once_current(mut self) -> Self {
        self.ends_once_current = true;
        self
    }

    pub fn with_decoder_limits(mut self, limits: DecoderLimits) -> Self {
        self.decoder_limits = limits;
        self
    }

    pub fn with_observer(mut self, observer: Arc<dyn PersistentIngestObserver>) -> Self {
        self.observer = observer;
        self
    }

    fn validate(&self) -> Result<(), PersistentIngestError> {
        if self.max_transactions == 0
            || self.max_transactions > MAX_BOUNDED_TRANSACTIONS
            || self.timeout.is_zero()
            || self.timeout > MAX_INGEST_DURATION
        {
            return Err(PersistentIngestError::InvalidConfig);
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PersistentIngestStats {
    pub committed_transactions: usize,
    pub last_canonical_position: Option<CursorPosition>,
    pub received_lsn: u64,
    pub flushed_lsn: u64,
    pub feedback_writes: usize,
    /// Whether the cycle once held every complete message that had reached
    /// the replication connection: a group read until none was waiting, or
    /// the cycle ended with none waiting. A cycle that never did had messages
    /// waiting throughout.
    pub caught_up: bool,
    /// The longest any transaction the cycle decoded waited between its
    /// commit and the send of the message that carried the commit, both on
    /// the source's clock; zero when it decoded none. A walsender that is
    /// behind sends late even while the connection keeps draining.
    pub longest_send_delay: Duration,
}

/// Operator-facing recovery category for one failed persistent ingest attempt.
///
/// Callers must still preserve the concrete error for diagnostics. This value
/// only selects the safe next action; it never permits feedback after failure.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PersistentIngestDisposition {
    Retry,
    OperatorAction,
    Resnapshot,
    EpochReseed,
}

#[derive(Debug, Error)]
pub enum PersistentIngestError {
    #[error("persistent ingest configuration is empty or outside bounded limits")]
    InvalidConfig,
    #[error("persistent source session is closed or locally revoked")]
    SessionClosed,
    #[error("schema registry does not match the activated source")]
    RegistryBindingMismatch,
    #[error("authorization policy changed while the persistent source session was active")]
    AuthorizationPolicyChanged,
    #[error("mutation receipt database is unavailable or returned an uncertain result")]
    MutationReceiptUnavailable,
    #[error(transparent)]
    CanonicalLog(#[from] LogError),
    #[error(transparent)]
    Decoder(#[from] DecodeError),
    #[error(transparent)]
    Ingest(#[from] IngestError),
    #[error(transparent)]
    Fence(#[from] FenceError),
    #[error("PostgreSQL replication transport failed or returned an uncertain result")]
    ReplicationUnavailable,
}

impl PersistentIngestError {
    pub fn disposition(&self) -> PersistentIngestDisposition {
        match self {
            Self::InvalidConfig | Self::AuthorizationPolicyChanged => {
                PersistentIngestDisposition::OperatorAction
            }
            Self::SessionClosed
            | Self::MutationReceiptUnavailable
            | Self::ReplicationUnavailable => PersistentIngestDisposition::Retry,
            Self::RegistryBindingMismatch => PersistentIngestDisposition::Resnapshot,
            Self::CanonicalLog(error) => log_disposition(error),
            Self::Decoder(_) => PersistentIngestDisposition::OperatorAction,
            Self::Ingest(error) => ingest_disposition(error),
            Self::Fence(error) => fence_disposition(error),
        }
    }
}

fn fence_disposition(error: &FenceError) -> PersistentIngestDisposition {
    match error {
        FenceError::EpochMismatch { .. } | FenceError::SlotEnrollmentMismatch => {
            PersistentIngestDisposition::EpochReseed
        }
        FenceError::LeaseHeld { .. }
        | FenceError::LeaseLost
        | FenceError::LeaseRowLocked
        | FenceError::DurableLogFenceRejected
        | FenceError::ControlStoreUnavailable => PersistentIngestDisposition::Retry,
        _ => PersistentIngestDisposition::OperatorAction,
    }
}

fn log_disposition(error: &LogError) -> PersistentIngestDisposition {
    match error {
        LogError::PostgresUnavailable | LogError::FencedWriterRejected => {
            PersistentIngestDisposition::Retry
        }
        LogError::ManifestCorrupt(_)
        | LogError::SegmentCorrupt { .. }
        | LogError::EpochMismatch { .. }
        | LogError::DuplicateTransactionConflict
        | LogError::SequenceGap { .. }
        | LogError::PostgresHeadCorrupt => PersistentIngestDisposition::EpochReseed,
        LogError::BeforeRetention { .. } => PersistentIngestDisposition::Resnapshot,
        _ => PersistentIngestDisposition::OperatorAction,
    }
}

fn ingest_disposition(error: &IngestError) -> PersistentIngestDisposition {
    match error {
        IngestError::Fence(IngestFenceError::SourceEpochChanged { .. })
        | IngestError::CanonicalEpochMismatch => PersistentIngestDisposition::EpochReseed,
        IngestError::Fence(_) => PersistentIngestDisposition::Retry,
        IngestError::Normalize(_) => PersistentIngestDisposition::Resnapshot,
        IngestError::Append(AppendFailure::CanonicalLog(error)) => log_disposition(error),
        IngestError::Append(AppendFailure::Other(error)) => {
            error.downcast_ref::<ReceiptMarkError>().map_or(
                PersistentIngestDisposition::OperatorAction,
                |error| match error {
                    ReceiptMarkError::State(MutationBatchError::CanonicalReceiptState(_)) => {
                        PersistentIngestDisposition::Retry
                    }
                    ReceiptMarkError::InvalidMarker | ReceiptMarkError::State(_) => {
                        PersistentIngestDisposition::OperatorAction
                    }
                },
            )
        }
        _ => PersistentIngestDisposition::OperatorAction,
    }
}

struct RuntimeFenceAdapter {
    lease: ActiveLease,
    source_lease: SourceLease,
}

impl IngestFenceAuthority for RuntimeFenceAdapter {
    fn validate(&self, lease: &SourceLease) -> Result<(), IngestFenceError> {
        if lease.source_id != self.source_lease.source_id {
            return Err(IngestFenceError::NoActiveLease(lease.source_id.clone()));
        }
        if lease.source_epoch != self.source_lease.source_epoch {
            return Err(IngestFenceError::SourceEpochChanged {
                expected: self.source_lease.source_epoch.clone(),
                actual: lease.source_epoch.clone(),
            });
        }
        if lease.lease_epoch != self.source_lease.lease_epoch {
            return Err(IngestFenceError::StaleLease {
                expected: self.source_lease.lease_epoch.get(),
                actual: lease.lease_epoch.get(),
            });
        }
        // Canonical append validates and locks the exact durable writer fence;
        // feedback later validates and locks the exact active owner row around
        // the transport flush. A control-store transaction here would not
        // authorize either side effect and would only duplicate those fences.
        if !self.lease.is_locally_valid() {
            return Err(IngestFenceError::NoActiveLease(lease.source_id.clone()));
        }
        Ok(())
    }
}

struct ReceiptMarkingAppender {
    log: PostgresFencedCanonicalLog,
    receipts: Client,
    cursor_codec: sync_types::CursorCodec,
    source_id: SourceId,
    source_epoch: SourceEpoch,
    observer: Arc<dyn PersistentIngestObserver>,
}

#[derive(Debug)]
struct CanonicalObserverAdapter {
    observer: Arc<dyn PersistentIngestObserver>,
}

impl PostgresAppendObserver for CanonicalObserverAdapter {
    fn observe(&self, boundary: PostgresAppendBoundary) {
        self.observer.observe(match boundary {
            PostgresAppendBoundary::BeforeCheckpoint => PersistentIngestBoundary::BeforeCheckpoint,
            PostgresAppendBoundary::FenceAuthorized => {
                PersistentIngestBoundary::CanonicalFenceAuthorized
            }
            PostgresAppendBoundary::SequenceAllocated => {
                PersistentIngestBoundary::CanonicalSequenceAllocated
            }
            PostgresAppendBoundary::RecordInserted => {
                PersistentIngestBoundary::CanonicalRecordInserted
            }
            PostgresAppendBoundary::HeadUpdated => PersistentIngestBoundary::CanonicalHeadUpdated,
            PostgresAppendBoundary::AfterCheckpoint => PersistentIngestBoundary::AfterCheckpoint,
            PostgresAppendBoundary::TransactionCommitted => {
                PersistentIngestBoundary::CanonicalTransactionCommitted
            }
        });
    }
}

#[derive(Debug)]
struct FeedbackObserverAdapter {
    observer: Arc<dyn PersistentIngestObserver>,
}

impl StandbyStatusUpdateObserver for FeedbackObserverAdapter {
    fn observe(&self, boundary: StandbyStatusUpdateBoundary) {
        self.observer.observe(match boundary {
            StandbyStatusUpdateBoundary::Encoded => PersistentIngestBoundary::AfterFeedbackEncode,
            StandbyStatusUpdateBoundary::BeforeCopyDataQueue => {
                PersistentIngestBoundary::BeforeFeedbackCopyData
            }
            StandbyStatusUpdateBoundary::CopyDataQueued => {
                PersistentIngestBoundary::FeedbackCopyDataQueued
            }
            StandbyStatusUpdateBoundary::BeforeTransportFlush => {
                PersistentIngestBoundary::BeforeFeedbackTransportFlush
            }
            StandbyStatusUpdateBoundary::TransportFlushed => {
                PersistentIngestBoundary::FeedbackTransportFlushed
            }
        });
    }
}

struct ObservedNormalizer<N> {
    inner: N,
    observer: Arc<dyn PersistentIngestObserver>,
}

impl<N> cdc_ingest::TransactionNormalizer for ObservedNormalizer<N>
where
    N: cdc_ingest::TransactionNormalizer,
{
    fn normalize(
        &mut self,
        transaction: pgoutput_decoder::DecodedTransaction,
        source_epoch: &SourceEpoch,
    ) -> Result<CanonicalEntry, cdc_ingest::NormalizationError> {
        self.observer
            .observe(PersistentIngestBoundary::BeforeNormalize);
        let entry = self.inner.normalize(transaction, source_epoch)?;
        self.observer
            .observe(PersistentIngestBoundary::AfterNormalize);
        Ok(entry)
    }
}

type RuntimeIngestSession = IngestSession<
    ReceiptMarkingAppender,
    RuntimeFenceAdapter,
    ObservedNormalizer<SchemaNormalizer>,
>;

struct CanonicalMutationMarker {
    server_transaction_id: [u8; 16],
    mutation_ids: BTreeSet<MutationId>,
}

impl DurableAppender for ReceiptMarkingAppender {
    /// Appends the group in one canonical-log commit, then marks mutation
    /// receipts in group order.
    ///
    /// A group with no mutation receipts advances the durable checkpoint to
    /// its last WAL end in that same commit. A group with receipts leaves the
    /// checkpoint where it was, and the caller advances it only after every
    /// receipt is marked, so a failure between the append and the last receipt
    /// replays the group and marks what was missed.
    fn append_durable(
        &mut self,
        group: Vec<DurableAppend>,
    ) -> Result<DurableAppendOutcome, AppendFailure> {
        let mut entries = Vec::with_capacity(group.len());
        let mut receipts = Vec::with_capacity(group.len());
        let mut checkpoint = None;
        for DurableAppend { entry, wal_end } in group {
            receipts.push(
                mutation_receipt_marker(&entry)
                    .map_err(|error| AppendFailure::Other(Box::new(error)))?,
            );
            entries.push(entry);
            checkpoint = Some(u64::from(wal_end));
        }
        let checkpoint_is_durable = receipts.iter().all(Option::is_none);
        let checkpoint = checkpoint.filter(|_| checkpoint_is_durable);
        self.observer
            .observe(PersistentIngestBoundary::BeforeCanonicalAppend);
        let outcomes = self.log.append_group(entries, checkpoint)?;
        if outcomes.len() != receipts.len() {
            return Err(AppendFailure::CanonicalLog(LogError::PostgresHeadCorrupt));
        }
        if let Some(AppendOutcome::Appended(position)) = outcomes
            .iter()
            .rev()
            .find(|outcome| matches!(outcome, AppendOutcome::Appended(_)))
        {
            self.observer.observe_durable_append(position);
        }
        self.observer.observe_appended_entries(
            outcomes
                .iter()
                .filter(|outcome| matches!(outcome, AppendOutcome::Appended(_)))
                .count(),
        );
        self.observer
            .observe(PersistentIngestBoundary::AfterCanonicalAppend);
        for (outcome, marker) in outcomes.iter().zip(&receipts) {
            let Some(marker) = marker else {
                continue;
            };
            self.observer
                .observe(PersistentIngestBoundary::BeforeReceiptMark);
            let cursor = self.cursor_codec.encode(outcome.position());
            mark_canonical_receipts(
                &mut self.receipts,
                &self.cursor_codec,
                &self.source_id,
                &self.source_epoch,
                &marker.server_transaction_id,
                &marker.mutation_ids,
                &cursor,
            )
            .map_err(ReceiptMarkError::State)
            .map_err(|error| AppendFailure::Other(Box::new(error)))?;
            self.observer
                .observe(PersistentIngestBoundary::AfterReceiptMark);
        }
        Ok(DurableAppendOutcome::new(outcomes, checkpoint_is_durable))
    }
}

#[derive(Debug, Error)]
enum ReceiptMarkError {
    #[error("canonical mutation marker identity is malformed")]
    InvalidMarker,
    #[error("canonical mutation receipt marking failed")]
    State(#[source] MutationBatchError),
}

fn mutation_receipt_marker(
    entry: &CanonicalEntry,
) -> Result<Option<CanonicalMutationMarker>, ReceiptMarkError> {
    let CanonicalEntry::Transaction(transaction) = entry else {
        return Ok(None);
    };
    if transaction.mutation_ids.is_empty() {
        return Ok(None);
    }
    let encoded = transaction
        .transaction_id
        .strip_prefix("mutation:")
        .ok_or(ReceiptMarkError::InvalidMarker)?;
    let mut server_transaction_id = [0_u8; 16];
    hex::decode_to_slice(encoded, &mut server_transaction_id)
        .map_err(|_| ReceiptMarkError::InvalidMarker)?;
    let mutation_ids = transaction.mutation_ids.iter().cloned().collect();
    Ok(Some(CanonicalMutationMarker {
        server_transaction_id,
        mutation_ids,
    }))
}

impl ActiveSourceSession {
    /// Processes a bounded number of complete persistent CDC transactions.
    ///
    /// The only feedback path is inside this method. It is reached after the
    /// fenced canonical append, canonical mutation-receipt marking, durable
    /// source checkpoint update, and acquisition of the active feedback fence.
    pub fn ingest_bounded(
        &mut self,
        registry: &SchemaRegistry,
        authorization_policy: &AuthorizationPolicy,
        config: PersistentIngestConfig,
    ) -> Result<PersistentIngestStats, PersistentIngestError> {
        config.validate()?;
        if !self.socket_is_open() {
            return Err(PersistentIngestError::SessionClosed);
        }
        let outcome = catch_unwind(AssertUnwindSafe(|| {
            if registry.binding() != *self.activated_source.registry_binding() {
                return Err(PersistentIngestError::RegistryBindingMismatch);
            }
            if authorization_policy.registry_binding()
                != Some(self.activated_source.registry_binding())
            {
                return Err(PersistentIngestError::RegistryBindingMismatch);
            }
            let policy_identity = IngestAuthorizationPolicy::from(authorization_policy);
            if self
                .ingest_state
                .authorization_policy
                .is_some_and(|active| active != policy_identity)
            {
                return Err(PersistentIngestError::AuthorizationPolicyChanged);
            }
            self.ingest_state
                .observer
                .replace(Arc::clone(&config.observer));
            let observer: Arc<dyn PersistentIngestObserver> = self.ingest_state.observer.clone();
            if self.ingest_state.ingest.is_none() {
                let source_id = self.lease.identity().source_id().clone();
                let source_epoch = self.lease.identity().source_epoch().clone();
                let fence_token = self.lease.identity().token().get();
                let lease_epoch = u64::try_from(fence_token)
                    .ok()
                    .and_then(|value| LeaseEpoch::new(value).ok())
                    .ok_or(PersistentIngestError::SessionClosed)?;
                let source_lease = SourceLease {
                    source_id: source_id.clone(),
                    source_epoch: source_epoch.clone(),
                    lease_epoch,
                };
                let fence = Arc::new(RuntimeFenceAdapter {
                    lease: self.lease.clone(),
                    source_lease: source_lease.clone(),
                });
                let log = PostgresFencedCanonicalLog::connect_with_observer(
                    &self.canonical_log_connection,
                    FencedWriterIdentity::new(
                        source_id.clone(),
                        source_epoch.clone(),
                        fence_token,
                        self.canonical_datastore.clone(),
                    )?,
                    &config.state_dir,
                    Arc::new(CanonicalObserverAdapter {
                        observer: Arc::clone(&observer),
                    }),
                )?;
                let mut receipts = postgres_connect::Endpoint::parse(&self.source_connection)
                    .map_err(|_| PersistentIngestError::MutationReceiptUnavailable)?
                    .connect()
                    .map_err(|_| PersistentIngestError::MutationReceiptUnavailable)?;
                receipts
                    .batch_execute("SET statement_timeout = '2s'; SET lock_timeout = '1s';")
                    .map_err(|_| PersistentIngestError::MutationReceiptUnavailable)?;
                let appender = ReceiptMarkingAppender {
                    log,
                    receipts,
                    cursor_codec: self.activated_source.cursor_codec().clone(),
                    source_id,
                    source_epoch,
                    observer: Arc::clone(&observer),
                };
                let normalizer = ObservedNormalizer {
                    inner: SchemaNormalizer::from_registry_dynamic(registry)
                        .map_err(IngestError::from)?,
                    observer: Arc::clone(&observer),
                };
                self.ingest_state.ingest = Some(IngestSession::new(
                    appender,
                    fence,
                    normalizer,
                    source_lease,
                )?);
                self.ingest_state.authorization_policy = Some(policy_identity);
            }
            self.ingest_state.initialize_decoder(
                self.activated_source.pgoutput_profile(),
                &config.decoder_limits,
            )?;
            let initial_lsn = self.slot.confirmed_flush_lsn;
            let runtime = self
                .io_runtime
                .as_mut()
                .ok_or(PersistentIngestError::SessionClosed)?;
            let replication = self
                .replication
                .as_mut()
                .ok_or(PersistentIngestError::SessionClosed)?;
            let decoder = self
                .ingest_state
                .decoder
                .as_mut()
                .ok_or(PersistentIngestError::SessionClosed)?;
            let ingest = self
                .ingest_state
                .ingest
                .as_mut()
                .ok_or(PersistentIngestError::SessionClosed)?;
            run_ingest_loop(
                runtime,
                replication,
                decoder,
                &mut self.ingest_state.pending_events,
                ingest,
                &self.authority,
                &self.lease,
                &observer,
                initial_lsn,
                self.ingest_state.received_lsn,
                self.ingest_state
                    .start_flushed_lsn
                    .filter(|_| config.ends_once_current),
                config.max_transactions,
                config.timeout,
            )
        }));
        let result = match outcome {
            Ok(result) => result,
            Err(payload) => {
                self.close_source_socket();
                resume_unwind(payload);
            }
        };
        match &result {
            Ok(stats) => {
                self.slot.confirmed_flush_lsn = stats.flushed_lsn;
                self.ingest_state.received_lsn = stats.received_lsn;
                if has_reached(self.ingest_state.start_flushed_lsn, stats.received_lsn) {
                    self.ingest_state.start_flushed_lsn = None;
                }
            }
            Err(_) => self.close_source_socket(),
        }
        result
    }
}

/// Ingests up to `max_transactions` source transactions, in append groups.
///
/// A group starts with the first complete transaction decoded and takes
/// every further one that has already reached the replication connection,
/// without waiting for more, up to `MAX_GROUP_TRANSACTIONS` or
/// `MAX_GROUP_READ_BYTES` of CopyData. The group appends in one canonical-log
/// commit, and one fenced status update then acknowledges its last WAL end.
#[allow(clippy::too_many_arguments)]
fn run_ingest_loop<A, F, N>(
    runtime: &mut tokio::runtime::Runtime,
    replication: &mut PgReplicationConnection,
    decoder: &mut Decoder,
    pending_events: &mut VecDeque<DecoderEvent>,
    ingest: &mut IngestSession<A, F, N>,
    authority: &Arc<FenceAuthority<PostgresFenceStore>>,
    lease: &ActiveLease,
    observer: &Arc<dyn PersistentIngestObserver>,
    initial_lsn: u64,
    initial_received_lsn: u64,
    current_at: Option<u64>,
    max_transactions: usize,
    timeout: Duration,
) -> Result<PersistentIngestStats, PersistentIngestError>
where
    A: DurableAppender,
    F: IngestFenceAuthority,
    N: cdc_ingest::TransactionNormalizer,
{
    let deadline = Instant::now()
        .checked_add(timeout)
        .ok_or(PersistentIngestError::InvalidConfig)?;
    let cancellation = CancellationToken::new();
    let mut stats = PersistentIngestStats {
        committed_transactions: 0,
        last_canonical_position: None,
        received_lsn: initial_received_lsn.max(initial_lsn),
        flushed_lsn: initial_lsn,
        feedback_writes: 0,
        caught_up: false,
        longest_send_delay: Duration::ZERO,
    };
    while stats.committed_transactions < max_transactions {
        let limit = (max_transactions - stats.committed_transactions).min(MAX_GROUP_TRANSACTIONS);
        let mut group = take_group(pending_events, limit);
        if !group.is_empty() {
            // A keepalive read here needs no reply of its own: the group's
            // status update below answers it.
            let mut read_bytes = 0_usize;
            while group.len() < limit && read_bytes < MAX_GROUP_READ_BYTES {
                let Some(data) = replication
                    .try_get_copy_data()
                    .map_err(|_| PersistentIngestError::ReplicationUnavailable)?
                else {
                    stats.caught_up = true;
                    break;
                };
                read_bytes = read_bytes.saturating_add(data.len());
                let copy_data = decoder.decode_copy_data(data)?;
                if decoder.total_spool_bytes() > 0 {
                    observer.observe(PersistentIngestBoundary::TransactionSpilled);
                }
                receive_copy_data(copy_data, pending_events, &mut stats);
                group.extend(take_group(pending_events, limit - group.len()));
            }
            let transactions = group.len();
            let pending = ingest.ingest_group(group)?;
            let authorized = ingest.authorize_ack(pending)?;
            let ack_lsn: u64 = authorized.wal_end().into();
            if !authorized.checkpoint_is_durable() {
                observer.observe(PersistentIngestBoundary::BeforeCheckpoint);
                authority.advance_durable_canonical_lsn(lease, ack_lsn)?;
                observer.observe(PersistentIngestBoundary::AfterCheckpoint);
            }
            send_feedback_fenced(
                runtime,
                replication,
                authority,
                lease,
                observer,
                stats.received_lsn,
                ack_lsn,
            )?;
            stats.flushed_lsn = ack_lsn;
            stats.feedback_writes = stats
                .feedback_writes
                .checked_add(1)
                .ok_or(PersistentIngestError::InvalidConfig)?;
            stats.committed_transactions = stats
                .committed_transactions
                .checked_add(transactions)
                .ok_or(PersistentIngestError::InvalidConfig)?;
            stats.last_canonical_position = Some(authorized.canonical_position().clone());
            continue;
        }
        let Some(remaining) = deadline.checked_duration_since(Instant::now()) else {
            // The deadline passed while the loop took a message. Taking one
            // more that has already arrived tells whether any was left
            // waiting; a transaction it completes waits for the next cycle.
            match replication
                .try_get_copy_data()
                .map_err(|_| PersistentIngestError::ReplicationUnavailable)?
            {
                Some(data) => {
                    let copy_data = decoder.decode_copy_data(data)?;
                    take_copy_data(
                        runtime,
                        replication,
                        decoder,
                        copy_data,
                        pending_events,
                        &mut stats,
                        authority,
                        lease,
                        observer,
                    )?;
                }
                None => stats.caught_up = true,
            }
            return Ok(stats);
        };
        if has_reached(current_at, stats.received_lsn) {
            // The walsender decodes WAL in order and sends a transaction as it
            // decodes its commit, so a message whose WAL end is at or past
            // `current_at` follows every transaction committed by then, and
            // the empty group says each is appended. The session is current
            // unless more is waiting.
            match replication
                .try_get_copy_data()
                .map_err(|_| PersistentIngestError::ReplicationUnavailable)?
            {
                Some(data) => {
                    let copy_data = decoder.decode_copy_data(data)?;
                    take_copy_data(
                        runtime,
                        replication,
                        decoder,
                        copy_data,
                        pending_events,
                        &mut stats,
                        authority,
                        lease,
                        observer,
                    )?;
                    continue;
                }
                None => {
                    stats.caught_up = true;
                    return Ok(stats);
                }
            }
        }
        let data = runtime.block_on(async {
            let read = replication.get_copy_data_async(&cancellation);
            tokio::pin!(read);
            tokio::select! {
                result = &mut read => result
                    .map(Some)
                    .map_err(|_| PersistentIngestError::ReplicationUnavailable),
                () = tokio::time::sleep(remaining) => {
                    cancellation.cancel();
                    settle_cancelled_read(
                        tokio::time::timeout(FEEDBACK_FLUSH_TIMEOUT, &mut read).await.ok(),
                    )
                }
            }
        })?;
        let Some(data) = data else {
            // Nothing reached the connection before the deadline.
            stats.caught_up = true;
            return Ok(stats);
        };
        let copy_data = decoder.decode_copy_data(data)?;
        take_copy_data(
            runtime,
            replication,
            decoder,
            copy_data,
            pending_events,
            &mut stats,
            authority,
            lease,
            observer,
        )?;
    }
    Ok(stats)
}

/// What a read cancelled at the deadline settled to, or `None` if it did not
/// settle in time.
///
/// A cancelled read still returns a message that reached the connection
/// before the cancellation took effect, and that message is kept: dropping it
/// would lose WAL the slot has sent, with nothing downstream to notice the
/// gap. Only a read that ended as cancelled holds nothing.
fn settle_cancelled_read<T>(
    settled: Option<Result<T, ReplicationError>>,
) -> Result<Option<T>, PersistentIngestError> {
    match settled {
        Some(Ok(data)) => Ok(Some(data)),
        Some(Err(ReplicationError::Cancelled(_))) => Ok(None),
        Some(Err(_)) | None => Err(PersistentIngestError::ReplicationUnavailable),
    }
}

fn has_reached(target: Option<u64>, received_lsn: u64) -> bool {
    target.is_some_and(|lsn| received_lsn >= lsn)
}

/// Queues one decoded CopyData message read outside a group, and answers the
/// server when it asked for a reply.
#[allow(clippy::too_many_arguments)]
fn take_copy_data(
    runtime: &mut tokio::runtime::Runtime,
    replication: &mut PgReplicationConnection,
    decoder: &Decoder,
    copy_data: ReplicationCopyData,
    pending_events: &mut VecDeque<DecoderEvent>,
    stats: &mut PersistentIngestStats,
    authority: &FenceAuthority<PostgresFenceStore>,
    lease: &ActiveLease,
    observer: &Arc<dyn PersistentIngestObserver>,
) -> Result<(), PersistentIngestError> {
    if decoder.total_spool_bytes() > 0 {
        observer.observe(PersistentIngestBoundary::TransactionSpilled);
    }
    if receive_copy_data(copy_data, pending_events, stats) {
        send_feedback_fenced(
            runtime,
            replication,
            authority,
            lease,
            observer,
            stats.received_lsn,
            stats.flushed_lsn,
        )?;
        stats.feedback_writes = stats
            .feedback_writes
            .checked_add(1)
            .ok_or(PersistentIngestError::InvalidConfig)?;
    }
    Ok(())
}

/// Takes committed transactions from the front of `events`, in order, until
/// it holds `limit` or `events` is empty. Every other event carries nothing to
/// append and is dropped.
fn take_group(events: &mut VecDeque<DecoderEvent>, limit: usize) -> Vec<DecodedTransaction> {
    let mut group = Vec::new();
    while group.len() < limit {
        match events.pop_front() {
            Some(DecoderEvent::TransactionCommitted(transaction)) => group.push(transaction),
            Some(
                DecoderEvent::RelationObserved { .. }
                | DecoderEvent::TypeObserved(_)
                | DecoderEvent::Origin { .. }
                | DecoderEvent::StandaloneLogicalMessage(_)
                | DecoderEvent::StreamAborted { .. },
            ) => {}
            None => break,
        }
    }
    group
}

/// Queues the events of one CopyData message, records its WAL end and how
/// late it carried any commit, and returns whether the server asked for a
/// reply.
fn receive_copy_data(
    copy_data: ReplicationCopyData,
    pending_events: &mut VecDeque<DecoderEvent>,
    stats: &mut PersistentIngestStats,
) -> bool {
    match copy_data {
        ReplicationCopyData::Wal {
            wal_end,
            send_time_postgres_micros,
            events,
            ..
        } => {
            stats.received_lsn = stats.received_lsn.max(wal_end.get());
            for event in &events {
                if let DecoderEvent::TransactionCommitted(transaction) = event {
                    stats.longest_send_delay = stats.longest_send_delay.max(send_delay(
                        transaction.commit_time_postgres_micros,
                        send_time_postgres_micros,
                    ));
                }
            }
            pending_events.extend(events);
            false
        }
        ReplicationCopyData::Keepalive {
            wal_end,
            reply_requested,
            ..
        } => {
            stats.received_lsn = stats.received_lsn.max(wal_end.get());
            reply_requested
        }
    }
}

/// How long after a commit at `commit_micros` the message carrying it was
/// sent at `send_micros`, both PostgreSQL microseconds on the source's clock;
/// zero when that clock ran backwards between them.
fn send_delay(commit_micros: i64, send_micros: i64) -> Duration {
    Duration::from_micros(u64::try_from(send_micros.saturating_sub(commit_micros)).unwrap_or(0))
}

fn send_feedback_fenced(
    runtime: &mut tokio::runtime::Runtime,
    replication: &mut PgReplicationConnection,
    authority: &FenceAuthority<PostgresFenceStore>,
    lease: &ActiveLease,
    observer: &Arc<dyn PersistentIngestObserver>,
    received_lsn: u64,
    flushed_lsn: u64,
) -> Result<(), PersistentIngestError> {
    observer.observe(PersistentIngestBoundary::BeforeFinalFence);
    authority.with_active_feedback_fence(lease, || {
        observer.observe(PersistentIngestBoundary::AfterFinalFence);
        observer.observe(PersistentIngestBoundary::BeforeFeedbackEncode);
        let feedback_observer: Arc<dyn StandbyStatusUpdateObserver> =
            Arc::new(FeedbackObserverAdapter {
                observer: Arc::clone(observer),
            });
        let sent = runtime.block_on(async {
            tokio::time::timeout(
                FEEDBACK_FLUSH_TIMEOUT,
                replication.send_standby_status_update_observed(
                    received_lsn,
                    flushed_lsn,
                    flushed_lsn,
                    false,
                    feedback_observer,
                ),
            )
            .await
        });
        match sent {
            Ok(Ok(())) => Ok(()),
            Ok(Err(_)) | Err(_) => Err(PersistentIngestError::ReplicationUnavailable),
        }
    })??;
    Ok(())
}

#[cfg(test)]
mod tests {
    use pgoutput_decoder::{PgLsn as DecodedLsn, TransactionMode};

    use super::*;

    fn committed(xid: u32) -> DecoderEvent {
        let lsn = DecodedLsn::new(u64::from(xid) * 16);
        DecoderEvent::TransactionCommitted(DecodedTransaction {
            xid,
            mode: TransactionMode::Normal,
            first_observed_wal_lsn: lsn,
            final_lsn: Some(lsn),
            commit_lsn: lsn,
            end_lsn: DecodedLsn::new(lsn.get() + 8),
            commit_time_postgres_micros: 0,
            changes: Vec::new(),
        })
    }

    fn xids(group: &[DecodedTransaction]) -> Vec<u32> {
        group.iter().map(|transaction| transaction.xid).collect()
    }

    #[test]
    fn a_read_cancelled_at_the_deadline_keeps_a_message_it_returns() {
        assert!(matches!(
            settle_cancelled_read(Some(Ok(b"wal".to_vec()))),
            Ok(Some(data)) if data == b"wal"
        ));
        assert!(matches!(
            settle_cancelled_read::<Vec<u8>>(Some(Err(ReplicationError::Cancelled(
                "cancelled".to_owned()
            )))),
            Ok(None)
        ));
        assert!(matches!(
            settle_cancelled_read::<Vec<u8>>(Some(Err(ReplicationError::protocol(
                "malformed".to_owned()
            )))),
            Err(PersistentIngestError::ReplicationUnavailable)
        ));
        assert!(matches!(
            settle_cancelled_read::<Vec<u8>>(None),
            Err(PersistentIngestError::ReplicationUnavailable)
        ));
    }

    /// A stream has reached a position once it has received WAL up to it, and
    /// never reaches one it was not given.
    #[test]
    fn a_stream_reaches_a_position_once_it_has_received_wal_up_to_it() {
        assert!(!has_reached(Some(32), 31));
        assert!(has_reached(Some(32), 32));
        assert!(has_reached(Some(32), 33));
        assert!(!has_reached(None, u64::MAX));
    }

    #[test]
    fn a_group_takes_committed_transactions_in_order_up_to_its_limit() {
        let mut events = VecDeque::from([
            DecoderEvent::Origin {
                origin_lsn: DecodedLsn::new(1),
                origin_name: "origin".to_owned(),
            },
            committed(7),
            DecoderEvent::StreamAborted {
                xid: 9,
                subtransaction_xid: 10,
            },
            committed(8),
            committed(11),
            committed(12),
        ]);

        assert_eq!(xids(&take_group(&mut events, 3)), [7, 8, 11]);
        assert_eq!(events, [committed(12)]);
        assert_eq!(xids(&take_group(&mut events, 3)), [12]);
        assert!(events.is_empty());
        assert!(take_group(&mut events, 3).is_empty());
    }

    fn committed_at(xid: u32, commit_time_postgres_micros: i64) -> DecoderEvent {
        let DecoderEvent::TransactionCommitted(mut transaction) = committed(xid) else {
            unreachable!("`committed` builds a committed transaction");
        };
        transaction.commit_time_postgres_micros = commit_time_postgres_micros;
        DecoderEvent::TransactionCommitted(transaction)
    }

    fn wal(send_time_postgres_micros: i64, events: Vec<DecoderEvent>) -> ReplicationCopyData {
        ReplicationCopyData::Wal {
            start_lsn: DecodedLsn::new(16),
            wal_end: DecodedLsn::new(32),
            send_time_postgres_micros,
            events,
        }
    }

    /// Each commit's delay is measured against the message that carried it,
    /// and the cycle keeps the longest. A source clock that ran backwards
    /// reads as no delay, and messages that carry no commit change nothing.
    #[test]
    fn a_cycle_keeps_the_longest_any_commit_waited_to_be_sent() {
        let mut pending = VecDeque::new();
        let mut stats = PersistentIngestStats {
            committed_transactions: 0,
            last_canonical_position: None,
            received_lsn: 0,
            flushed_lsn: 0,
            feedback_writes: 0,
            caught_up: false,
            longest_send_delay: Duration::ZERO,
        };

        let early = wal(
            1_000_000,
            vec![committed_at(1, 700_000), committed_at(2, 950_000)],
        );
        assert!(!receive_copy_data(early, &mut pending, &mut stats));
        assert_eq!(stats.longest_send_delay, Duration::from_millis(300));

        for quiet in [
            wal(2_000_000, vec![committed_at(3, 1_900_000)]),
            wal(3_000_000, vec![committed_at(4, 3_500_000)]),
            wal(
                9_000_000,
                vec![DecoderEvent::StreamAborted {
                    xid: 9,
                    subtransaction_xid: 10,
                }],
            ),
        ] {
            receive_copy_data(quiet, &mut pending, &mut stats);
        }
        let keepalive = ReplicationCopyData::Keepalive {
            wal_end: DecodedLsn::new(64),
            send_time_postgres_micros: 9_000_000,
            reply_requested: true,
        };
        assert!(receive_copy_data(keepalive, &mut pending, &mut stats));
        assert_eq!(stats.longest_send_delay, Duration::from_millis(300));

        let late = wal(5_000_000, vec![committed_at(5, 3_800_000)]);
        receive_copy_data(late, &mut pending, &mut stats);
        assert_eq!(stats.longest_send_delay, Duration::from_millis(1_200));
        assert_eq!(pending.len(), 6);
        assert_eq!(stats.received_lsn, 64);
    }

    #[test]
    fn events_that_append_nothing_never_form_a_group() {
        let mut events = VecDeque::from([
            DecoderEvent::Origin {
                origin_lsn: DecodedLsn::new(1),
                origin_name: "origin".to_owned(),
            },
            DecoderEvent::StreamAborted {
                xid: 9,
                subtransaction_xid: 10,
            },
        ]);

        assert!(take_group(&mut events, 256).is_empty());
        assert!(events.is_empty());
    }

    #[test]
    fn failures_have_safe_recovery_dispositions() {
        assert_eq!(
            PersistentIngestError::RegistryBindingMismatch.disposition(),
            PersistentIngestDisposition::Resnapshot
        );
        assert_eq!(
            PersistentIngestError::AuthorizationPolicyChanged.disposition(),
            PersistentIngestDisposition::OperatorAction
        );
        assert_eq!(
            PersistentIngestError::Fence(FenceError::EpochMismatch {
                expected: SourceEpoch::new("epoch-a").unwrap(),
                actual: SourceEpoch::new("epoch-b").unwrap(),
            })
            .disposition(),
            PersistentIngestDisposition::EpochReseed
        );
        assert_eq!(
            PersistentIngestError::Fence(FenceError::LeaseRowLocked).disposition(),
            PersistentIngestDisposition::Retry
        );
        assert_eq!(
            PersistentIngestError::Fence(FenceError::ControlStoreRefused(
                crate::PostgresRefusal::InsufficientPrivilege
            ))
            .disposition(),
            PersistentIngestDisposition::OperatorAction
        );
        assert_eq!(
            PersistentIngestError::Decoder(DecodeError::InvalidLimits).disposition(),
            PersistentIngestDisposition::OperatorAction
        );
        assert_eq!(
            PersistentIngestError::CanonicalLog(LogError::BeforeRetention {
                requested: 1,
                minimum: 2,
            })
            .disposition(),
            PersistentIngestDisposition::Resnapshot
        );
    }
}
