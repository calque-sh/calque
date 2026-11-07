use std::{
    sync::{Arc, Mutex, MutexGuard},
    time::{Duration, Instant},
};

use canonical_log::{
    CanonicalPage, LogError, PostgresFencedCanonicalLog, SnapshotBuildRetentionLease,
};
use serde_json::to_vec;
use snapshot_coordinator::{
    CanonicalCutProof, CoordinatorError, CutObservation, PersistentBarrierMapper,
};
use sync_types::SourceEpoch;
use thiserror::Error;
use tokio::sync::watch;
use tokio_util::sync::CancellationToken;

const PAGE_RECORDS: usize = 256;
const PAGE_BYTES: u64 = 8 * 1024 * 1024;

/// Provider facts needed to map one temporary cut to independently durable
/// canonical truth. Implementations must return complete contiguous records.
pub trait CanonicalBarrierRead: Send + Sync {
    /// Provider-enforced upper bound for each synchronous read operation.
    fn maximum_barrier_read_duration(&self) -> Duration;

    fn source_epoch(&self) -> Result<SourceEpoch, LogError>;
    fn minimum_retained_sequence(&self) -> Result<u64, LogError>;
    fn durable_head_sequence(&self) -> Result<u64, LogError>;
    fn read_page_after_bounded(
        &self,
        after: u64,
        through: u64,
        max_records: usize,
        max_bytes: u64,
    ) -> Result<CanonicalPage, LogError>;
}

#[cfg(any(test, feature = "test-support"))]
impl CanonicalBarrierRead for canonical_log::FileCanonicalLog {
    fn maximum_barrier_read_duration(&self) -> Duration {
        Duration::from_millis(50)
    }

    fn source_epoch(&self) -> Result<SourceEpoch, LogError> {
        Ok(self.source_epoch().clone())
    }

    fn minimum_retained_sequence(&self) -> Result<u64, LogError> {
        Ok(self.minimum_retained().sequence)
    }

    fn durable_head_sequence(&self) -> Result<u64, LogError> {
        Ok(self.head().sequence)
    }

    fn read_page_after_bounded(
        &self,
        after: u64,
        through: u64,
        max_records: usize,
        max_bytes: u64,
    ) -> Result<CanonicalPage, LogError> {
        canonical_log::FileCanonicalLog::read_page_after_bounded(
            self,
            after,
            Some(through),
            max_records,
            max_bytes,
        )
    }
}

impl<P: CanonicalBarrierRead> CanonicalBarrierRead for Mutex<P> {
    fn maximum_barrier_read_duration(&self) -> Duration {
        lock_provider(self)
            .map(|provider| provider.maximum_barrier_read_duration())
            .unwrap_or(Duration::MAX)
    }

    fn source_epoch(&self) -> Result<SourceEpoch, LogError> {
        lock_provider(self)?.source_epoch()
    }

    fn minimum_retained_sequence(&self) -> Result<u64, LogError> {
        lock_provider(self)?.minimum_retained_sequence()
    }

    fn durable_head_sequence(&self) -> Result<u64, LogError> {
        lock_provider(self)?.durable_head_sequence()
    }

    fn read_page_after_bounded(
        &self,
        after: u64,
        through: u64,
        max_records: usize,
        max_bytes: u64,
    ) -> Result<CanonicalPage, LogError> {
        lock_provider(self)?.read_page_after_bounded(after, through, max_records, max_bytes)
    }
}

fn lock_provider<P>(provider: &Mutex<P>) -> Result<MutexGuard<'_, P>, LogError> {
    provider.lock().map_err(|_| LogError::PostgresUnavailable)
}

impl CanonicalBarrierRead for PostgresFencedCanonicalLog {
    fn maximum_barrier_read_duration(&self) -> Duration {
        Duration::from_secs(3)
    }

    fn source_epoch(&self) -> Result<SourceEpoch, LogError> {
        Ok(self.writer().source_epoch().clone())
    }

    fn minimum_retained_sequence(&self) -> Result<u64, LogError> {
        Ok(self.minimum_retained()?.sequence)
    }

    fn durable_head_sequence(&self) -> Result<u64, LogError> {
        Ok(self.head()?.sequence)
    }

    fn read_page_after_bounded(
        &self,
        after: u64,
        through: u64,
        max_records: usize,
        max_bytes: u64,
    ) -> Result<CanonicalPage, LogError> {
        let records = self.read_after_bounded(after, Some(through), max_records, max_bytes)?;
        let stored_bytes = records.iter().try_fold(0_u64, |charged, record| {
            let bytes = to_vec(&record.entry).map_err(LogError::Serialization)?;
            charged
                .checked_add(bytes.len() as u64)
                .ok_or(LogError::RecordTooLarge)
        })?;
        Ok(CanonicalPage {
            records,
            stored_bytes,
        })
    }
}

/// Hint channel shared by a durable append owner and snapshot barrier waiters.
/// Correctness never depends on a notification: waiters periodically re-read
/// the provider's durable head, and the watch generation closes wake-before-
/// registration and registration-before-append races.
#[derive(Debug, Clone)]
pub struct CanonicalHeadSignal {
    sender: watch::Sender<u64>,
}

impl Default for CanonicalHeadSignal {
    fn default() -> Self {
        Self::new()
    }
}

impl CanonicalHeadSignal {
    pub fn new() -> Self {
        let (sender, _) = watch::channel(0);
        Self { sender }
    }

    pub fn notify_head_advanced(&self) {
        self.sender.send_modify(|generation| {
            *generation = generation.wrapping_add(1);
        });
    }

    /// Subscribes to durable-head hints. Consumers must always re-read their
    /// durable provider after waking; the generation is not a cursor.
    pub fn subscribe(&self) -> watch::Receiver<u64> {
        self.sender.subscribe()
    }
}

#[derive(Debug, Clone, Copy)]
pub struct BarrierWaitRequest {
    pub timeout: Duration,
    pub poll_interval: Duration,
    pub max_records: usize,
    pub max_bytes: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct BarrierWaitProgress {
    pub minimum_sequence: u64,
    pub durable_head_sequence: u64,
    pub records_scanned: usize,
    pub bytes_scanned: u64,
}

#[derive(Debug)]
pub struct BarrierWaitResult {
    pub proof: CanonicalCutProof,
    pub progress: BarrierWaitProgress,
}

/// Waits for an exact persistent cut barrier while the caller holds the
/// supplied durable build-retention capability.
pub async fn wait_for_persistent_barrier<P: CanonicalBarrierRead + Send + 'static>(
    provider: Arc<Mutex<P>>,
    signal: &CanonicalHeadSignal,
    lease: &SnapshotBuildRetentionLease,
    cut: &CutObservation,
    request: BarrierWaitRequest,
    cancellation: &CancellationToken,
    mut on_progress: impl FnMut(BarrierWaitProgress),
) -> Result<BarrierWaitResult, BarrierWaitError> {
    validate_request(request)?;
    let provider_timeout = request
        .timeout
        .min(request.poll_interval.max(Duration::from_millis(250)));
    let provider_epoch = read_provider(
        Arc::clone(&provider),
        provider_timeout,
        cancellation,
        CanonicalBarrierRead::source_epoch,
    )
    .await?;
    if provider_epoch != *lease.source_epoch() {
        return Err(BarrierWaitError::SourceEpochChanged);
    }
    let minimum_sequence = lease.minimum_sequence();
    if minimum_sequence == 0 {
        return Err(BarrierWaitError::InvalidRequest);
    }
    let deadline = Instant::now()
        .checked_add(request.timeout)
        .ok_or(BarrierWaitError::InvalidRequest)?;
    let mut receiver = signal.subscribe();
    let mut after = minimum_sequence - 1;
    let mut records_scanned = 0_usize;
    let mut bytes_scanned = 0_u64;
    let mut mapper = PersistentBarrierMapper::new(provider_epoch, cut.clone());

    loop {
        if cancellation.is_cancelled() {
            return Err(BarrierWaitError::Cancelled);
        }
        let retained = read_provider(
            Arc::clone(&provider),
            provider_timeout,
            cancellation,
            CanonicalBarrierRead::minimum_retained_sequence,
        )
        .await?;
        if retained > minimum_sequence {
            return Err(BarrierWaitError::RetentionCrossed {
                leased: minimum_sequence,
                retained,
            });
        }
        let durable_head_sequence = read_provider(
            Arc::clone(&provider),
            provider_timeout,
            cancellation,
            CanonicalBarrierRead::durable_head_sequence,
        )
        .await?;
        while after < durable_head_sequence {
            let remaining_records = request
                .max_records
                .checked_sub(records_scanned)
                .ok_or(BarrierWaitError::ScanLimitExceeded)?;
            let remaining_bytes = request
                .max_bytes
                .checked_sub(bytes_scanned)
                .ok_or(BarrierWaitError::ScanLimitExceeded)?;
            if remaining_records == 0 || remaining_bytes == 0 {
                return Err(BarrierWaitError::ScanLimitExceeded);
            }
            let page = read_provider(
                Arc::clone(&provider),
                provider_timeout,
                cancellation,
                move |provider| {
                    provider.read_page_after_bounded(
                        after,
                        durable_head_sequence,
                        PAGE_RECORDS.min(remaining_records),
                        PAGE_BYTES.min(remaining_bytes),
                    )
                },
            )
            .await?;
            if page.records.is_empty() {
                return Err(BarrierWaitError::ScanStalled {
                    after,
                    durable_head: durable_head_sequence,
                });
            }
            records_scanned = records_scanned
                .checked_add(page.records.len())
                .ok_or(BarrierWaitError::ScanLimitExceeded)?;
            bytes_scanned = bytes_scanned
                .checked_add(page.stored_bytes)
                .ok_or(BarrierWaitError::ScanLimitExceeded)?;
            for record in &page.records {
                if let Some(proof) = mapper.observe(record)? {
                    let progress = BarrierWaitProgress {
                        minimum_sequence,
                        durable_head_sequence,
                        records_scanned,
                        bytes_scanned,
                    };
                    on_progress(progress);
                    return Ok(BarrierWaitResult { proof, progress });
                }
            }
            after = page
                .records
                .last()
                .expect("nonempty canonical page")
                .position
                .sequence;
            on_progress(BarrierWaitProgress {
                minimum_sequence,
                durable_head_sequence,
                records_scanned,
                bytes_scanned,
            });
        }

        let remaining = deadline
            .checked_duration_since(Instant::now())
            .ok_or(BarrierWaitError::Timeout)?;
        let wait = request.poll_interval.min(remaining);
        tokio::select! {
            () = cancellation.cancelled() => return Err(BarrierWaitError::Cancelled),
            changed = receiver.changed() => {
                if changed.is_err() {
                    return Err(BarrierWaitError::SignalClosed);
                }
            }
            () = tokio::time::sleep(wait) => {}
        }
    }
}

async fn read_provider<P, T, F>(
    provider: Arc<Mutex<P>>,
    timeout: Duration,
    cancellation: &CancellationToken,
    operation: F,
) -> Result<T, BarrierWaitError>
where
    P: CanonicalBarrierRead + Send + 'static,
    T: Send + 'static,
    F: FnOnce(&P) -> Result<T, LogError> + Send + 'static,
{
    if cancellation.is_cancelled() {
        return Err(BarrierWaitError::Cancelled);
    }
    let maximum = provider
        .lock()
        .map_err(|_| BarrierWaitError::ProviderTaskFailed)?
        .maximum_barrier_read_duration();
    if maximum.is_zero() || maximum > timeout {
        return Err(BarrierWaitError::ProviderTimeout);
    }
    let started = Instant::now();
    let task = tokio::task::spawn_blocking(move || {
        let provider = provider.lock().map_err(|_| LogError::PostgresUnavailable)?;
        operation(&provider)
    });
    let result = task
        .await
        .map_err(|_| BarrierWaitError::ProviderTaskFailed)??;
    if started.elapsed() > timeout {
        Err(BarrierWaitError::ProviderTimeout)
    } else if cancellation.is_cancelled() {
        Err(BarrierWaitError::Cancelled)
    } else {
        Ok(result)
    }
}

fn validate_request(request: BarrierWaitRequest) -> Result<(), BarrierWaitError> {
    if request.timeout.is_zero()
        || request.poll_interval.is_zero()
        || request.max_records == 0
        || request.max_bytes == 0
    {
        Err(BarrierWaitError::InvalidRequest)
    } else {
        Ok(())
    }
}

#[derive(Debug, Error)]
pub enum BarrierWaitError {
    #[error("persistent barrier wait request is invalid")]
    InvalidRequest,
    #[error("snapshot build was cancelled while waiting for persistent barrier")]
    Cancelled,
    #[error("canonical barrier provider operation timed out")]
    ProviderTimeout,
    #[error("canonical barrier provider task failed")]
    ProviderTaskFailed,
    #[error("persistent barrier wait timed out")]
    Timeout,
    #[error("canonical head signal closed")]
    SignalClosed,
    #[error("canonical source epoch changed while waiting for persistent barrier")]
    SourceEpochChanged,
    #[error("canonical retention crossed leased floor {leased}, reaching {retained}")]
    RetentionCrossed { leased: u64, retained: u64 },
    #[error("persistent barrier scan exceeded its total record or byte budget")]
    ScanLimitExceeded,
    #[error("persistent barrier scan stalled after {after} while durable head was {durable_head}")]
    ScanStalled { after: u64, durable_head: u64 },
    #[error(transparent)]
    CanonicalLog(#[from] LogError),
    #[error(transparent)]
    Coordinator(#[from] CoordinatorError),
}

#[cfg(test)]
mod tests {
    use std::sync::{
        atomic::{AtomicUsize, Ordering},
        Arc,
    };

    use canonical_log::{
        CanonicalBarrier, FileCanonicalLog, SnapshotBuildId, SnapshotBuildOwnerId,
        SnapshotBuildRetention, TransactionKey,
    };
    use snapshot_coordinator::{CutIdentity, CutObservation};
    use sync_types::{CursorPosition, SourceId};
    use tokio_postgres::types::PgLsn;

    use super::*;

    fn append_barrier(log: &Mutex<FileCanonicalLog>, epoch: &SourceEpoch, identity: &CutIdentity) {
        log.lock()
            .unwrap()
            .append_barrier(CanonicalBarrier {
                key: TransactionKey {
                    source_epoch: epoch.clone(),
                    end_lsn: PgLsn::from(30_u64).to_string(),
                    postgres_xid: 7,
                },
                build_id: identity.build_id.clone(),
                nonce: identity.nonce.clone(),
                message_lsn: PgLsn::from(20_u64).to_string(),
                commit_lsn: PgLsn::from(25_u64).to_string(),
                commit_time_unix_micros: 1,
            })
            .unwrap();
    }

    /// Where the barrier's append lands among the waiter's reads.
    #[derive(Clone, Copy, Debug)]
    enum Landing {
        /// Inside the source-epoch read, before the waiter registers.
        BeforeRegistration,
        /// Inside the second durable-head read, once it has read the head. The
        /// first head read signals with nothing appended, a spurious wakeup.
        AfterSpuriousWakeup,
    }

    /// The file log, appending the barrier and signalling from inside one of
    /// the waiter's reads once that read has its value. A concurrently spawned
    /// append lands wherever the scheduler puts it, and with a short poll a
    /// missed signal costs nothing a test can see.
    struct StagedAppend {
        log: Mutex<FileCanonicalLog>,
        signal: CanonicalHeadSignal,
        epoch: SourceEpoch,
        identity: CutIdentity,
        landing: Landing,
        head_reads: AtomicUsize,
    }

    impl StagedAppend {
        fn land(&self) {
            append_barrier(&self.log, &self.epoch, &self.identity);
            self.signal.notify_head_advanced();
        }
    }

    impl CanonicalBarrierRead for StagedAppend {
        fn maximum_barrier_read_duration(&self) -> Duration {
            self.log.maximum_barrier_read_duration()
        }

        fn source_epoch(&self) -> Result<SourceEpoch, LogError> {
            let epoch = self.log.source_epoch()?;
            if let Landing::BeforeRegistration = self.landing {
                self.land();
            }
            Ok(epoch)
        }

        fn minimum_retained_sequence(&self) -> Result<u64, LogError> {
            self.log.minimum_retained_sequence()
        }

        fn durable_head_sequence(&self) -> Result<u64, LogError> {
            let head = self.log.durable_head_sequence()?;
            if let Landing::AfterSpuriousWakeup = self.landing {
                match self.head_reads.fetch_add(1, Ordering::SeqCst) {
                    0 => self.signal.notify_head_advanced(),
                    1 => self.land(),
                    _ => {}
                }
            }
            Ok(head)
        }

        fn read_page_after_bounded(
            &self,
            after: u64,
            through: u64,
            max_records: usize,
            max_bytes: u64,
        ) -> Result<CanonicalPage, LogError> {
            self.log
                .read_page_after_bounded(after, through, max_records, max_bytes)
        }
    }

    fn fixture() -> (
        tempfile::TempDir,
        FileCanonicalLog,
        SnapshotBuildRetentionLease,
        CutObservation,
    ) {
        let root = tempfile::tempdir().unwrap();
        let source = SourceId::new("source-barrier-wait").unwrap();
        let epoch = SourceEpoch::new("epoch-barrier-wait").unwrap();
        let mut log = FileCanonicalLog::open(root.path(), source, epoch.clone()).unwrap();
        let lease = log
            .acquire_snapshot_build_retention(
                SnapshotBuildId::new("build-barrier-wait").unwrap(),
                SnapshotBuildOwnerId::new("owner-barrier-wait").unwrap(),
                Duration::from_secs(30),
            )
            .unwrap();
        let identity = CutIdentity::new("build-barrier-wait", "nonce-barrier-wait").unwrap();
        let cut = CutObservation {
            identity,
            transaction_id: 7,
            message_lsn: PgLsn::from(20_u64),
            commit_lsn: PgLsn::from(25_u64),
            end_lsn: PgLsn::from(30_u64),
            committed_application_transactions_before_cut: 0,
        };
        (root, log, lease, cut)
    }

    /// The engine's request. Its 3 s poll also bounds each provider read, so a
    /// read waiting on an append's fsync stays inside it, and a waiter that
    /// misses a signal sleeps long enough to tell.
    fn request() -> BarrierWaitRequest {
        BarrierWaitRequest {
            timeout: Duration::from_secs(30),
            poll_interval: Duration::from_secs(3),
            max_records: 10,
            max_bytes: 1024 * 1024,
        }
    }

    #[tokio::test]
    async fn closes_registration_before_append_and_spurious_wakeup_races() {
        for landing in [Landing::BeforeRegistration, Landing::AfterSpuriousWakeup] {
            let (_root, log, lease, cut) = fixture();
            let signal = CanonicalHeadSignal::new();
            signal.notify_head_advanced();
            let provider = Arc::new(Mutex::new(StagedAppend {
                log: Mutex::new(log),
                signal: signal.clone(),
                epoch: lease.source_epoch().clone(),
                identity: cut.identity.clone(),
                landing,
                head_reads: AtomicUsize::new(0),
            }));
            let started = Instant::now();
            let result = wait_for_persistent_barrier(
                provider,
                &signal,
                &lease,
                &cut,
                request(),
                &CancellationToken::new(),
                |_| {},
            )
            .await
            .unwrap();
            assert!(
                started.elapsed() < request().poll_interval,
                "{landing:?}: the waiter slept through a signal"
            );
            assert_eq!(result.proof.position().sequence, 1, "{landing:?}");
            assert_eq!(result.progress.records_scanned, 1, "{landing:?}");
        }
    }

    #[tokio::test]
    async fn cancellation_never_mints_a_cut() {
        let (_root, log, lease, cut) = fixture();
        let log = Arc::new(Mutex::new(log));
        let cancellation = CancellationToken::new();
        cancellation.cancel();
        assert!(matches!(
            wait_for_persistent_barrier(
                Arc::clone(&log),
                &CanonicalHeadSignal::new(),
                &lease,
                &cut,
                request(),
                &cancellation,
                |_| {},
            )
            .await,
            Err(BarrierWaitError::Cancelled)
        ));
        assert_eq!(
            log.lock().unwrap().head(),
            CursorPosition {
                source_epoch: lease.source_epoch().clone(),
                sequence: 0,
            }
        );
    }

    /// A barrier for another build, in a transaction other than the cut's.
    fn append_other_barrier(log: &mut FileCanonicalLog, epoch: &SourceEpoch) {
        log.append_barrier(CanonicalBarrier {
            key: TransactionKey {
                source_epoch: epoch.clone(),
                end_lsn: PgLsn::from(14_u64).to_string(),
                postgres_xid: 3,
            },
            build_id: "build-other".to_owned(),
            nonce: "nonce-other".to_owned(),
            message_lsn: PgLsn::from(12_u64).to_string(),
            commit_lsn: PgLsn::from(13_u64).to_string(),
            commit_time_unix_micros: 1,
        })
        .unwrap();
    }

    /// The file log with one fact it reports replaced.
    struct Overridden {
        log: Mutex<FileCanonicalLog>,
        source_epoch: Option<SourceEpoch>,
        minimum_retained_sequence: Option<u64>,
        empty_pages: bool,
    }

    impl Overridden {
        fn new(log: FileCanonicalLog) -> Self {
            Self {
                log: Mutex::new(log),
                source_epoch: None,
                minimum_retained_sequence: None,
                empty_pages: false,
            }
        }
    }

    impl CanonicalBarrierRead for Overridden {
        fn maximum_barrier_read_duration(&self) -> Duration {
            self.log.maximum_barrier_read_duration()
        }

        fn source_epoch(&self) -> Result<SourceEpoch, LogError> {
            match &self.source_epoch {
                Some(epoch) => Ok(epoch.clone()),
                None => self.log.source_epoch(),
            }
        }

        fn minimum_retained_sequence(&self) -> Result<u64, LogError> {
            match self.minimum_retained_sequence {
                Some(sequence) => Ok(sequence),
                None => self.log.minimum_retained_sequence(),
            }
        }

        fn durable_head_sequence(&self) -> Result<u64, LogError> {
            self.log.durable_head_sequence()
        }

        fn read_page_after_bounded(
            &self,
            after: u64,
            through: u64,
            max_records: usize,
            max_bytes: u64,
        ) -> Result<CanonicalPage, LogError> {
            if self.empty_pages {
                return Ok(CanonicalPage {
                    records: Vec::new(),
                    stored_bytes: 0,
                });
            }
            self.log
                .read_page_after_bounded(after, through, max_records, max_bytes)
        }
    }

    async fn wait(
        provider: Overridden,
        lease: &SnapshotBuildRetentionLease,
        cut: &CutObservation,
        request: BarrierWaitRequest,
    ) -> Result<BarrierWaitResult, BarrierWaitError> {
        wait_for_persistent_barrier(
            Arc::new(Mutex::new(provider)),
            &CanonicalHeadSignal::new(),
            lease,
            cut,
            request,
            &CancellationToken::new(),
            |_| {},
        )
        .await
    }

    // Each refusal below holds with the cut's barrier already durable, so a
    // wait that skipped the check would return its proof.

    #[tokio::test]
    async fn a_provider_on_another_source_epoch_mints_no_cut() {
        let (_root, log, lease, cut) = fixture();
        let provider = Overridden {
            source_epoch: Some(SourceEpoch::new("epoch-after-reset").unwrap()),
            ..Overridden::new(log)
        };
        append_barrier(&provider.log, lease.source_epoch(), &cut.identity);

        assert!(matches!(
            wait(provider, &lease, &cut, request()).await,
            Err(BarrierWaitError::SourceEpochChanged)
        ));
    }

    #[tokio::test]
    async fn retention_past_the_leased_floor_mints_no_cut() {
        let (_root, log, lease, cut) = fixture();
        assert_eq!(lease.minimum_sequence(), 1);
        let provider = Overridden {
            minimum_retained_sequence: Some(2),
            ..Overridden::new(log)
        };
        append_barrier(&provider.log, lease.source_epoch(), &cut.identity);

        assert!(matches!(
            wait(provider, &lease, &cut, request()).await,
            Err(BarrierWaitError::RetentionCrossed {
                leased: 1,
                retained: 2
            })
        ));
    }

    #[tokio::test]
    async fn a_scan_past_its_record_or_byte_budget_mints_no_cut() {
        let first_record_bytes = {
            let (_root, mut log, lease, _cut) = fixture();
            append_other_barrier(&mut log, lease.source_epoch());
            log.read_page_after_bounded(0, Some(1), 1, u64::MAX)
                .unwrap()
                .stored_bytes
        };
        // The cut's barrier is the second record, so a budget of one record,
        // or of the first record's bytes, ends the scan before it.
        let budgets = [
            ("records", 1, request().max_bytes, false),
            ("bytes", request().max_records, first_record_bytes, false),
            ("both enough", 2, 2 * first_record_bytes + 1024, true),
        ];
        for (case, max_records, max_bytes, finds_the_cut) in budgets {
            let (_root, mut log, lease, cut) = fixture();
            append_other_barrier(&mut log, lease.source_epoch());
            let provider = Overridden::new(log);
            append_barrier(&provider.log, lease.source_epoch(), &cut.identity);
            let request = BarrierWaitRequest {
                max_records,
                max_bytes,
                ..request()
            };

            let result = wait(provider, &lease, &cut, request).await;
            if finds_the_cut {
                assert_eq!(result.unwrap().proof.position().sequence, 2, "{case}");
            } else {
                assert!(
                    matches!(result, Err(BarrierWaitError::ScanLimitExceeded)),
                    "{case}: {result:?}"
                );
            }
        }
    }

    #[tokio::test]
    async fn a_head_whose_records_cannot_be_read_stalls_without_a_cut() {
        let (_root, log, lease, cut) = fixture();
        let provider = Overridden {
            empty_pages: true,
            ..Overridden::new(log)
        };
        append_barrier(&provider.log, lease.source_epoch(), &cut.identity);

        assert!(matches!(
            wait(provider, &lease, &cut, request()).await,
            Err(BarrierWaitError::ScanStalled {
                after: 0,
                durable_head: 1
            })
        ));
    }
}
