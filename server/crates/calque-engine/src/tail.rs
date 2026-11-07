//! The canonical log's newest records, read once for every change build.
//!
//! One follower per process reads each committed range from the log once and
//! holds it in memory, bounded in bytes. A build whose whole range the tail
//! holds reads it from here, and sends each transaction as its scope's one
//! shared encoding of it. A build the tail cannot serve reads the log, as
//! every build did before.

use std::{
    collections::{HashMap, VecDeque},
    sync::{
        atomic::{AtomicU64, AtomicUsize, Ordering},
        Arc, PoisonError, RwLock as StdRwLock, Weak,
    },
    time::{Duration, Instant},
};

use canonical_log::{CanonicalLogRead, CanonicalPage, CanonicalRecord, LogBounds, LogError};
use change_api::{
    ChangeRecords, PageRecord, RecordPage, ScopeEncoding, ScopeEncodingKey, MAX_SCAN_RECORDS,
    MAX_SCAN_STORED_BYTES,
};
use sync_types::{CursorPosition, SourceEpoch, SourceId};
use tokio::sync::watch;

use crate::log_readers::{LogReaders, PooledLogReader};

/// How often the follower reads the log's head when no commit wakes it.
const FOLLOW_INTERVAL: Duration = Duration::from_millis(250);
/// How long after the follower's last read the tail still serves builds. A
/// tail older than this may be missing commits the log has, so builds read
/// the log instead.
const STALE_AFTER: Duration = Duration::from_secs(2);
const FOLLOW_PAGE_RECORDS: usize = 1_024;
/// A build over at most this much of the tail runs where it is awaited rather
/// than on the blocking pool.
const INLINE_RECORDS: usize = 32;
const INLINE_STORED_BYTES: u64 = 256 * 1024;
/// The watches builds wait on, each woken by a task of its own, so a commit's
/// wakes spread over the worker threads instead of one taking every waiter
/// list in turn.
const WAKE_SHARDS: usize = 64;

pub(crate) struct ChangeTail {
    source_id: SourceId,
    source_epoch: SourceEpoch,
    budget_bytes: u64,
    state: StdRwLock<TailState>,
    encodings: StdRwLock<HashMap<ScopeEncodingKey, Arc<ScopeEncoding>>>,
    /// The bytes of every scope's frames not yet dropped, those an encoding
    /// no longer holds but a batch still carries included.
    encoded_bytes: Arc<AtomicU64>,
    /// Bumped after the tail's head moves, when the follower fails and when a
    /// revocation lands, so a waiting build looks again. Each build waits on
    /// one of them.
    advanced: Arc<[watch::Sender<u64>]>,
    next_waiter: AtomicUsize,
}

#[derive(Default)]
struct TailState {
    /// Every record after `floor`, through `head`.
    records: VecDeque<TailRecord>,
    floor: u64,
    head: u64,
    /// The log's retention floor at the follower's last read.
    minimum_retained: u64,
    record_bytes: u64,
    started: bool,
    /// When the follower last read the log's bounds. `None` before its first
    /// read and after a failed one.
    read_at: Option<Instant>,
}

impl TailState {
    fn fresh(&self, now: Instant) -> bool {
        self.read_at
            .is_some_and(|read_at| now.saturating_duration_since(read_at) <= STALE_AFTER)
    }

    fn restart_at(&mut self, sequence: u64) {
        self.records.clear();
        self.floor = sequence;
        self.head = sequence;
        self.record_bytes = 0;
    }

    /// The log's floor only rises, so a read that began before a trim cannot
    /// lower it again.
    fn retained_from(&mut self, minimum: u64) {
        self.minimum_retained = self.minimum_retained.max(minimum);
    }
}

#[derive(Clone)]
struct TailRecord {
    record: Arc<CanonicalRecord>,
    stored_bytes: u64,
}

impl TailRecord {
    fn sequence(&self) -> u64 {
        self.record.position.sequence
    }
}

impl ChangeTail {
    pub(crate) fn new(source_id: SourceId, source_epoch: SourceEpoch, budget_bytes: u64) -> Self {
        Self {
            source_id,
            source_epoch,
            budget_bytes,
            state: StdRwLock::new(TailState::default()),
            encodings: StdRwLock::new(HashMap::new()),
            encoded_bytes: Arc::default(),
            advanced: (0..WAKE_SHARDS).map(|_| watch::Sender::new(0)).collect(),
            next_waiter: AtomicUsize::new(0),
        }
    }

    pub(crate) fn subscribe(&self) -> watch::Receiver<u64> {
        let shard = self.next_waiter.fetch_add(1, Ordering::Relaxed) % self.advanced.len();
        self.advanced[shard].subscribe()
    }

    pub(crate) fn is_fresh(&self, now: Instant) -> bool {
        read(&self.state).fresh(now)
    }

    /// The last sequence the follower has read, once it has started. The log
    /// is never trimmed past it, so the follower reads every record.
    pub(crate) fn head(&self) -> Option<u64> {
        let state = read(&self.state);
        state.started.then_some(state.head)
    }

    /// Drops the records the log has trimmed below `minimum`, so the tail
    /// serves no build the log would answer with `BeforeRetention`.
    pub(crate) fn trimmed(&self, minimum: u64) {
        let floor = {
            let mut state = write(&self.state);
            state.retained_from(minimum);
            let floor = minimum.saturating_sub(1);
            if floor <= state.floor {
                return;
            }
            if floor > state.head {
                state.restart_at(floor);
            } else {
                while state
                    .records
                    .front()
                    .is_some_and(|record| record.sequence() <= floor)
                {
                    if let Some(oldest) = state.records.pop_front() {
                        state.record_bytes = state.record_bytes.saturating_sub(oldest.stored_bytes);
                    }
                }
                state.floor = floor;
            }
            floor
        };
        for encoding in self.encodings_held() {
            encoding.evict_through(floor);
        }
    }

    /// When the tail turns stale if the follower reads nothing before then.
    fn stale_at(&self) -> Option<Instant> {
        read(&self.state)
            .read_at
            .map(|read_at| read_at + STALE_AFTER)
    }

    /// Marks the tail stale and wakes every build waiting on it, so each
    /// looks again and reads the log.
    fn turn_stale(&self) {
        write(&self.state).read_at = None;
        self.notify();
    }

    /// The records a build after `from` reads, pinned for its life, when the
    /// tail is fresh and holds all of them: `from` is held or is the floor, and
    /// the fence and the ceiling are at or below the head. A ceiling caps the
    /// head the build sees. The pin stops at the most one build reads.
    pub(crate) fn view(
        &self,
        from: u64,
        fence: Option<u64>,
        ceiling: Option<u64>,
        now: Instant,
    ) -> Option<TailView> {
        let state = read(&self.state);
        if !state.fresh(now) || from < state.floor || from > state.head {
            return None;
        }
        let head = match ceiling {
            Some(ceiling) if ceiling > state.head => return None,
            Some(ceiling) => ceiling,
            None => state.head,
        };
        if from > head || fence.is_some_and(|fence| fence > head) {
            return None;
        }
        let skip = usize::try_from(from - state.floor).ok()?;
        let mut records = Vec::new();
        let mut stored_bytes = 0_u64;
        for record in state.records.range(skip..) {
            if record.sequence() > head || records.len() == MAX_SCAN_RECORDS {
                break;
            }
            let next = stored_bytes.saturating_add(record.stored_bytes);
            if next > MAX_SCAN_STORED_BYTES {
                break;
            }
            stored_bytes = next;
            records.push(record.clone());
        }
        Some(TailView {
            source_id: self.source_id.clone(),
            source_epoch: self.source_epoch.clone(),
            after: from,
            records,
            stored_bytes,
            head,
            minimum_retained: state.minimum_retained,
        })
    }

    /// The one encoding every build of `key`'s scope shares.
    pub(crate) fn encoding(&self, key: ScopeEncodingKey) -> Arc<ScopeEncoding> {
        if let Some(encoding) = read(&self.encodings).get(&key) {
            return Arc::clone(encoding);
        }
        Arc::clone(write(&self.encodings).entry(key).or_insert_with_key(|key| {
            Arc::new(ScopeEncoding::counted_in(
                key.clone(),
                Arc::clone(&self.encoded_bytes),
            ))
        }))
    }

    /// Reads every record the log has committed past the tail's head, and
    /// says whether the head moved. The first read starts the tail at the
    /// log's head: what was committed before the process started is read from
    /// the log.
    pub(crate) fn catch_up<L: CanonicalLogRead + ?Sized>(
        &self,
        log: &L,
        now: Instant,
    ) -> Result<bool, LogError> {
        if !read(&self.state).started {
            let bounds = log.bounds()?;
            let mut state = write(&self.state);
            state.restart_at(bounds.head.sequence);
            state.retained_from(bounds.minimum_retained.sequence);
            state.started = true;
        }
        let mut moved = false;
        loop {
            let after = read(&self.state).head;
            let (bounds, page) = log.read_bounds_and_page_after_bounded(
                after,
                None,
                FOLLOW_PAGE_RECORDS,
                MAX_SCAN_STORED_BYTES,
            )?;
            if bounds.head.sequence < after {
                return Err(LogError::SequenceGap {
                    expected: after,
                    actual: bounds.head.sequence,
                });
            }
            // The log no longer holds what follows the tail: it starts again
            // where the log does.
            if after.saturating_add(1) < bounds.minimum_retained.sequence {
                self.restart(bounds.minimum_retained.sequence - 1, &bounds);
                moved = true;
                continue;
            }
            let page = match page {
                Ok(page) => page,
                // One record larger than a whole build reads: the tail starts
                // after it, and builds that need it read the log.
                Err(LogError::ReadLimitTooSmall { .. }) => {
                    self.restart(after.saturating_add(1), &bounds);
                    moved = true;
                    continue;
                }
                Err(error) => return Err(error),
            };
            if page.records.is_empty() {
                if after < bounds.head.sequence {
                    return Err(LogError::SequenceGap {
                        expected: after.saturating_add(1),
                        actual: bounds.head.sequence,
                    });
                }
                break;
            }
            self.append(page, &bounds)?;
            moved = true;
            if read(&self.state).head >= bounds.head.sequence {
                break;
            }
        }
        write(&self.state).read_at = Some(now);
        if moved {
            self.evict();
        }
        Ok(moved)
    }

    fn append(&self, page: CanonicalPage, bounds: &LogBounds) -> Result<(), LogError> {
        let count = u64::try_from(page.records.len()).unwrap_or(u64::MAX).max(1);
        // The log charges a page, not its records, so each record carries an
        // even share of what its page cost.
        let share = page.stored_bytes / count;
        let mut remainder = page.stored_bytes % count;
        let mut state = write(&self.state);
        for record in page.records {
            let expected = state.head.saturating_add(1);
            if record.position.sequence != expected
                || record.position.source_epoch != self.source_epoch
            {
                return Err(LogError::SequenceGap {
                    expected,
                    actual: record.position.sequence,
                });
            }
            let stored_bytes = share + std::mem::take(&mut remainder);
            state.head = expected;
            state.record_bytes = state.record_bytes.saturating_add(stored_bytes);
            state.records.push_back(TailRecord {
                record: Arc::new(record),
                stored_bytes,
            });
        }
        state.retained_from(bounds.minimum_retained.sequence);
        Ok(())
    }

    fn restart(&self, sequence: u64, bounds: &LogBounds) {
        {
            let mut state = write(&self.state);
            state.restart_at(sequence);
            state.retained_from(bounds.minimum_retained.sequence);
        }
        for encoding in self.encodings_held() {
            encoding.evict_through(sequence);
        }
    }

    /// Drops the oldest records, and every scope's encoding of them, until
    /// the tail is within its bound. A frame a slow client's batch still
    /// carries counts until that batch is written, so while it does the tail
    /// keeps fewer records, and none once such frames alone fill the bound.
    fn evict(&self) {
        let encodings = self.encodings_held();
        loop {
            let encoded = self.encoded_bytes.load(Ordering::Relaxed);
            let floor = {
                let mut state = write(&self.state);
                // The encodings are taken to shrink with the records they
                // encode; what does not is caught by the next pass.
                let held = state.record_bytes.saturating_add(encoded);
                if held <= self.budget_bytes || state.records.is_empty() {
                    break;
                }
                let target = u128::from(self.budget_bytes) * u128::from(state.record_bytes)
                    / u128::from(held);
                while let Some(oldest) = state.records.pop_front() {
                    state.floor = oldest.sequence();
                    state.record_bytes = state.record_bytes.saturating_sub(oldest.stored_bytes);
                    if u128::from(state.record_bytes) <= target {
                        break;
                    }
                }
                state.floor
            };
            for encoding in &encodings {
                encoding.evict_through(floor);
            }
        }
        drop(encodings);
        write(&self.encodings)
            .retain(|_, encoding| Arc::strong_count(encoding) > 1 || !encoding.is_empty());
    }

    fn encodings_held(&self) -> Vec<Arc<ScopeEncoding>> {
        read(&self.encodings).values().cloned().collect()
    }

    /// Wakes every build waiting on the tail.
    pub(crate) fn notify(&self) {
        let Ok(runtime) = tokio::runtime::Handle::try_current() else {
            self.advanced.iter().for_each(bump);
            return;
        };
        for shard in 0..self.advanced.len() {
            let advanced = Arc::clone(&self.advanced);
            runtime.spawn(async move { bump(&advanced[shard]) });
        }
    }

    #[cfg(test)]
    fn held(&self) -> (u64, u64, usize) {
        let state = read(&self.state);
        (state.floor, state.head, state.records.len())
    }
}

fn bump(advanced: &watch::Sender<u64>) {
    advanced.send_modify(|count| *count = count.wrapping_add(1));
}

/// Follows the log's head into `tail` for as long as anything holds the tail,
/// reading when a commit is signalled and at least every `FOLLOW_INTERVAL`.
///
/// A build waiting on a fresh tail waits for the tail alone. A read that hangs
/// rather than fails would leave it waiting on a tail that no longer moves,
/// so a read still running when the tail turns stale wakes every such build.
pub(crate) async fn follow<R>(
    tail: Weak<ChangeTail>,
    readers: Arc<LogReaders<R>>,
    mut head: watch::Receiver<u64>,
) where
    R: CanonicalLogRead + PooledLogReader,
{
    loop {
        let Some(held) = tail.upgrade() else {
            return;
        };
        let read = {
            let reading = async {
                let reader = readers.acquire().await?;
                let follower = Arc::clone(&held);
                tokio::task::spawn_blocking(move || follower.catch_up(&*reader, Instant::now()))
                    .await
                    .unwrap_or(Err(LogError::PostgresUnavailable))
            };
            tokio::pin!(reading);
            match held.stale_at() {
                Some(stale_at) => tokio::select! {
                    read = &mut reading => read,
                    () = tokio::time::sleep_until(stale_at.into()) => {
                        held.turn_stale();
                        reading.await
                    }
                },
                None => reading.await,
            }
        };
        match read {
            Ok(true) => held.notify(),
            Ok(false) => {}
            Err(error) => {
                tracing::debug!(%error, "the change tail could not read the canonical log");
                held.turn_stale();
            }
        }
        drop(held);
        tokio::select! {
            changed = head.changed() => if changed.is_err() { break },
            () = tokio::time::sleep(FOLLOW_INTERVAL) => {}
        }
    }
    if let Some(held) = tail.upgrade() {
        held.turn_stale();
    }
}

/// What one build reads of the tail: the records after its cursor, held for
/// as long as it runs, so the tail can drop them meanwhile.
pub(crate) struct TailView {
    source_id: SourceId,
    source_epoch: SourceEpoch,
    after: u64,
    records: Vec<TailRecord>,
    stored_bytes: u64,
    head: u64,
    minimum_retained: u64,
}

impl TailView {
    /// Whether the build is small enough to run where it is awaited.
    pub(crate) fn runs_inline(&self) -> bool {
        self.records.len() <= INLINE_RECORDS && self.stored_bytes <= INLINE_STORED_BYTES
    }

    fn bounds(&self) -> LogBounds {
        LogBounds {
            head: CursorPosition {
                source_epoch: self.source_epoch.clone(),
                sequence: self.head,
            },
            minimum_retained: CursorPosition {
                source_epoch: self.source_epoch.clone(),
                sequence: self.minimum_retained,
            },
        }
    }
}

impl ChangeRecords for TailView {
    fn source_id(&self) -> &SourceId {
        &self.source_id
    }

    fn source_epoch(&self) -> &SourceEpoch {
        &self.source_epoch
    }

    /// The page the log would return, under its limits.
    fn read_page_after(
        &self,
        from_sequence_exclusive: u64,
        through_sequence_inclusive: Option<u64>,
        max_records: usize,
        max_bytes: u64,
    ) -> Result<RecordPage, LogError> {
        if max_records == 0 || max_bytes == 0 {
            return Err(LogError::InvalidReadLimit);
        }
        if from_sequence_exclusive.saturating_add(1) < self.minimum_retained {
            return Err(LogError::BeforeRetention {
                requested: from_sequence_exclusive,
                minimum: self.minimum_retained,
            });
        }
        let through = through_sequence_inclusive
            .unwrap_or(self.head)
            .min(self.head);
        if from_sequence_exclusive >= through {
            return Ok(RecordPage {
                records: Vec::new(),
                stored_bytes: 0,
            });
        }
        let skip = from_sequence_exclusive
            .checked_sub(self.after)
            .and_then(|skip| usize::try_from(skip).ok())
            .ok_or(LogError::SequenceGap {
                expected: self.after.saturating_add(1),
                actual: from_sequence_exclusive.saturating_add(1),
            })?;
        let mut records = Vec::new();
        let mut charged = 0_u64;
        for record in self.records.iter().skip(skip) {
            if record.sequence() > through || records.len() == max_records {
                break;
            }
            let next = charged.saturating_add(record.stored_bytes);
            if next > max_bytes {
                if records.is_empty() {
                    return Err(LogError::ReadLimitTooSmall {
                        next_record_bytes: record.stored_bytes,
                        max_bytes,
                    });
                }
                break;
            }
            charged = next;
            records.push(PageRecord::Shared(Arc::clone(&record.record)));
        }
        // The pin ended before `through`: it holds what one build reads, so
        // only a build past its own limits gets here.
        if records.is_empty() {
            return Err(LogError::ReadLimitTooSmall {
                next_record_bytes: 0,
                max_bytes,
            });
        }
        Ok(RecordPage {
            records,
            stored_bytes: charged,
        })
    }

    fn read_bounds_and_page_after(
        &self,
        from_sequence_exclusive: u64,
        through_sequence_inclusive: Option<u64>,
        max_records: usize,
        max_bytes: u64,
    ) -> Result<(LogBounds, Result<RecordPage, LogError>), LogError> {
        Ok((
            self.bounds(),
            self.read_page_after(
                from_sequence_exclusive,
                through_sequence_inclusive,
                max_records,
                max_bytes,
            ),
        ))
    }
}

fn read<T>(lock: &StdRwLock<T>) -> std::sync::RwLockReadGuard<'_, T> {
    lock.read().unwrap_or_else(PoisonError::into_inner)
}

fn write<T>(lock: &StdRwLock<T>) -> std::sync::RwLockWriteGuard<'_, T> {
    lock.write().unwrap_or_else(PoisonError::into_inner)
}

#[cfg(test)]
mod tests {
    use std::sync::{Condvar, Mutex};

    use canonical_log::{
        CanonicalEntry, CanonicalOperation, CanonicalOperationKind, CanonicalTransaction,
        TransactionKey,
    };
    use change_api::{
        parse_change_request, ActivatedChangeScope, ChangeAuthorization, ChangeBatch,
        ChangeBatchBuilder, ChangeBatchRequest, IncomingChangeRequest, ScopedChangePlan,
    };
    use sync_protocol::{
        change::{self, change_stream_frame, ChangeStreamFrame},
        FrameDecoder,
    };
    use sync_types::{PlanId, TableId};

    use super::*;
    use crate::{
        snapshot::tests::prepared_source_for,
        test_support::{column, inspection, registry_for},
        PreparedLocalSource,
    };

    const RECORD_BYTES: u64 = 100;
    const NOW: i64 = 1_000_000;

    /// A canonical log in memory, which can trim, fail, and hang.
    struct FakeLog {
        source_id: SourceId,
        source_epoch: SourceEpoch,
        state: Mutex<FakeState>,
        released: Condvar,
    }

    struct FakeState {
        records: Vec<CanonicalRecord>,
        /// The lowest sequence still retained.
        minimum: u64,
        fails: bool,
        /// Every read blocks until this is cleared.
        hangs: bool,
    }

    impl FakeLog {
        fn head_sequence(state: &FakeState) -> u64 {
            state
                .records
                .last()
                .map_or(state.minimum.saturating_sub(1), |record| {
                    record.position.sequence
                })
        }

        fn append(&self, tables: &[&TableId]) -> u64 {
            let mut state = self.state.lock().unwrap();
            let sequence = Self::head_sequence(&state) + 1;
            state
                .records
                .push(record(&self.source_epoch, sequence, tables));
            sequence
        }

        fn trim_below(&self, minimum: u64) {
            let mut state = self.state.lock().unwrap();
            state.minimum = minimum;
            state
                .records
                .retain(|record| record.position.sequence >= minimum);
        }

        fn fail(&self, fails: bool) {
            self.state.lock().unwrap().fails = fails;
        }

        /// Every read blocks until the returned guard is dropped, which a
        /// failing test does as it unwinds.
        fn hang(self: &Arc<Self>) -> Hung {
            self.state.lock().unwrap().hangs = true;
            Hung(Arc::clone(self))
        }

        fn position(&self, sequence: u64) -> CursorPosition {
            CursorPosition {
                source_epoch: self.source_epoch.clone(),
                sequence,
            }
        }

        fn state(&self) -> Result<std::sync::MutexGuard<'_, FakeState>, LogError> {
            let state = self
                .released
                .wait_while(self.state.lock().unwrap(), |state| state.hangs)
                .unwrap();
            if state.fails {
                return Err(LogError::PostgresUnavailable);
            }
            Ok(state)
        }
    }

    struct Hung(Arc<FakeLog>);

    impl Drop for Hung {
        fn drop(&mut self) {
            self.0.state.lock().unwrap().hangs = false;
            self.0.released.notify_all();
        }
    }

    impl CanonicalLogRead for FakeLog {
        fn source_id(&self) -> &SourceId {
            &self.source_id
        }

        fn source_epoch(&self) -> &SourceEpoch {
            &self.source_epoch
        }

        fn head(&self) -> Result<CursorPosition, LogError> {
            let state = self.state()?;
            Ok(self.position(Self::head_sequence(&state)))
        }

        fn minimum_retained(&self) -> Result<CursorPosition, LogError> {
            Ok(self.position(self.state()?.minimum))
        }

        fn read_page_after_bounded(
            &self,
            from_sequence_exclusive: u64,
            through_sequence_inclusive: Option<u64>,
            max_records: usize,
            max_bytes: u64,
        ) -> Result<CanonicalPage, LogError> {
            let state = self.state()?;
            if max_records == 0 || max_bytes == 0 {
                return Err(LogError::InvalidReadLimit);
            }
            if from_sequence_exclusive.saturating_add(1) < state.minimum {
                return Err(LogError::BeforeRetention {
                    requested: from_sequence_exclusive,
                    minimum: state.minimum,
                });
            }
            let through = through_sequence_inclusive.unwrap_or(u64::MAX);
            let records = state
                .records
                .iter()
                .filter(|record| {
                    (from_sequence_exclusive + 1..=through).contains(&record.position.sequence)
                })
                .take(max_records)
                .cloned()
                .collect::<Vec<_>>();
            let fits = usize::try_from(max_bytes / RECORD_BYTES).unwrap_or(usize::MAX);
            if !records.is_empty() && fits == 0 {
                return Err(LogError::ReadLimitTooSmall {
                    next_record_bytes: RECORD_BYTES,
                    max_bytes,
                });
            }
            let records = records.into_iter().take(fits).collect::<Vec<_>>();
            Ok(CanonicalPage {
                stored_bytes: RECORD_BYTES * records.len() as u64,
                records,
            })
        }
    }

    /// The log as the follower's pool hands it out.
    struct FakeReader(Arc<FakeLog>);

    impl PooledLogReader for FakeReader {
        fn is_closed(&self) -> bool {
            false
        }
    }

    impl CanonicalLogRead for FakeReader {
        fn source_id(&self) -> &SourceId {
            CanonicalLogRead::source_id(&*self.0)
        }

        fn source_epoch(&self) -> &SourceEpoch {
            CanonicalLogRead::source_epoch(&*self.0)
        }

        fn head(&self) -> Result<CursorPosition, LogError> {
            self.0.head()
        }

        fn minimum_retained(&self) -> Result<CursorPosition, LogError> {
            self.0.minimum_retained()
        }

        fn read_page_after_bounded(
            &self,
            from_sequence_exclusive: u64,
            through_sequence_inclusive: Option<u64>,
            max_records: usize,
            max_bytes: u64,
        ) -> Result<CanonicalPage, LogError> {
            self.0.read_page_after_bounded(
                from_sequence_exclusive,
                through_sequence_inclusive,
                max_records,
                max_bytes,
            )
        }
    }

    fn record(epoch: &SourceEpoch, sequence: u64, tables: &[&TableId]) -> CanonicalRecord {
        CanonicalRecord {
            position: CursorPosition {
                source_epoch: epoch.clone(),
                sequence,
            },
            entry: CanonicalEntry::Transaction(CanonicalTransaction {
                key: TransactionKey {
                    source_epoch: epoch.clone(),
                    end_lsn: format!("0/{sequence:X}"),
                    postgres_xid: u32::try_from(sequence).unwrap(),
                },
                transaction_id: format!("tx-{sequence}"),
                begin_lsn: format!("0/{sequence:X}"),
                commit_lsn: format!("0/{sequence:X}"),
                commit_time_unix_micros: sequence,
                operations: tables
                    .iter()
                    .map(|table| CanonicalOperation {
                        table_id: (*table).clone(),
                        primary_key: Vec::new(),
                        kind: CanonicalOperationKind::TruncateAuthorizedTable {
                            reason: format!("truncated at {sequence}"),
                        },
                    })
                    .collect(),
                mutation_ids: Vec::new(),
            }),
        }
    }

    /// Two tables, `tasks` and `projects`, under one policy.
    struct Fixture {
        prepared: PreparedLocalSource,
        log: Arc<FakeLog>,
        tasks: TableId,
        projects: TableId,
    }

    impl Fixture {
        fn new() -> Self {
            let columns = || {
                vec![
                    column(1, "id", 2_950, "uuid", false, Some(1)),
                    column(2, "title", 25, "text", false, None),
                ]
            };
            let prepared = prepared_source_for(
                SourceEpoch::new("tail-epoch").unwrap(),
                registry_for(vec![
                    inspection("tasks", 16_384, columns()),
                    inspection("projects", 16_385, columns()),
                ]),
            );
            let table = |name: &str| {
                prepared
                    .registry
                    .tables()
                    .iter()
                    .find(|table| table.inspection.table.table == name)
                    .unwrap()
                    .table_id
                    .clone()
            };
            let (tasks, projects) = (table("tasks"), table("projects"));
            let log = Arc::new(FakeLog {
                source_id: SourceId::new("source-1").unwrap(),
                source_epoch: prepared.activated.source_epoch().clone(),
                state: Mutex::new(FakeState {
                    records: Vec::new(),
                    minimum: 1,
                    fails: false,
                    hangs: false,
                }),
                released: Condvar::new(),
            });
            Self {
                prepared,
                log,
                tasks,
                projects,
            }
        }

        fn tail(&self, budget_bytes: u64) -> ChangeTail {
            ChangeTail::new(
                self.log.source_id.clone(),
                self.log.source_epoch.clone(),
                budget_bytes,
            )
        }

        /// A tail started on the empty log, then caught up with `records`
        /// commits alternating between the two tables and both.
        fn followed(&self, budget_bytes: u64, records: u64) -> ChangeTail {
            let tail = self.tail(budget_bytes);
            assert!(!tail.catch_up(&*self.log, Instant::now()).unwrap());
            for index in 0..records {
                match index % 3 {
                    0 => self.log.append(&[&self.tasks]),
                    1 => self.log.append(&[&self.projects]),
                    _ => self.log.append(&[&self.tasks, &self.projects]),
                };
            }
            tail.catch_up(&*self.log, Instant::now()).unwrap();
            tail
        }

        fn cursor(&self, sequence: u64) -> sync_types::OpaqueCursor {
            self.prepared
                .activated
                .cursor_codec()
                .encode(&self.log.position(sequence))
        }

        fn sequence(&self, cursor: &sync_types::OpaqueCursor) -> u64 {
            self.prepared
                .activated
                .cursor_codec()
                .decode(cursor)
                .unwrap()
                .sequence
        }

        fn authorization(&self, tables: &[&TableId]) -> ChangeAuthorization {
            self.fenced_authorization(tables, 0)
        }

        /// A plan whose catch-up ends at `fence`.
        fn fenced_authorization(&self, tables: &[&TableId], fence: u64) -> ChangeAuthorization {
            ChangeAuthorization::new(ScopedChangePlan {
                plan_id: PlanId::new("plan-1").unwrap(),
                activated_scope: ActivatedChangeScope::new(
                    self.log.source_id.clone(),
                    &self.prepared.activated,
                    self.prepared.scope.clone(),
                )
                .unwrap(),
                expires_at_unix_micros: NOW * 1_000,
                from_cursor_exclusive: self.cursor(0),
                through_cursor_inclusive: self.cursor(fence),
                table_starts: tables
                    .iter()
                    .map(|table| ((*table).clone(), None))
                    .collect(),
                acknowledgeable_mutation_ids: Default::default(),
                revocation_barrier: None,
            })
            .unwrap()
        }

        fn request(&self, from: u64, tables: &[&TableId], max_transactions: u32) -> Incoming {
            self.fenced_request(from, None, tables, max_transactions)
        }

        fn fenced_request(
            &self,
            from: u64,
            fence: Option<u64>,
            tables: &[&TableId],
            max_transactions: u32,
        ) -> Incoming {
            Incoming(
                parse_change_request(change::ChangeRequest {
                    protocol_version: 1,
                    from_cursor_exclusive: Some(change::Cursor {
                        token: self.cursor(from).as_str().as_bytes().to_vec(),
                    }),
                    through_cursor_inclusive: fence.map(|fence| change::Cursor {
                        token: self.cursor(fence).as_str().as_bytes().to_vec(),
                    }),
                    tables: tables
                        .iter()
                        .map(|table| change::TableSubscription {
                            table_id: table.to_string(),
                            shard_snapshot_cursor: None,
                        })
                        .collect(),
                    authorization_scope_token: b"scope-secret".to_vec(),
                    max_batch_bytes: 1024 * 1024,
                    max_transactions,
                    long_poll_timeout_ms: 0,
                    client_instance_id: "client-1".to_owned(),
                    device_id: "device-1".to_owned(),
                    pending_mutation_ids: Vec::new(),
                    accept_transaction_fragmentation: true,
                    accept_revocation_barrier: true,
                    plan_id: "plan-1".to_owned(),
                    stream: false,
                })
                .unwrap(),
            )
        }

        /// One build from the tail, sharing the scope's encoding.
        fn built_from_tail(
            &self,
            tail: &ChangeTail,
            authorization: &ChangeAuthorization,
            request: &Incoming,
            ceiling: Option<u64>,
        ) -> ChangeBatch {
            let from = self.sequence(&request.0.request().from_cursor_exclusive);
            let fence = request
                .0
                .request()
                .through_cursor_inclusive
                .as_ref()
                .map(|fence| self.sequence(fence));
            let view = tail
                .view(from, fence, ceiling, Instant::now())
                .expect("the tail holds the range");
            let encoding = tail.encoding(ScopeEncodingKey::of(authorization));
            ChangeBatchBuilder::new(&view, authorization)
                .unwrap()
                .with_shared_encoding(&encoding)
                .unwrap()
                .build_at(request.0.request(), NOW)
                .unwrap()
        }

        fn built_from_log(
            &self,
            authorization: &ChangeAuthorization,
            request: &Incoming,
        ) -> ChangeBatch {
            ChangeBatchBuilder::new(&*self.log, authorization)
                .unwrap()
                .build_at(request.0.request(), NOW)
                .unwrap()
        }
    }

    struct Incoming(IncomingChangeRequest);

    impl Incoming {
        fn request(&self) -> &ChangeBatchRequest {
            self.0.request()
        }
    }

    fn frames(batch: &ChangeBatch) -> Vec<ChangeStreamFrame> {
        let bytes = batch.to_bytes();
        let mut decoder =
            FrameDecoder::with_max_payload_bytes(std::io::Cursor::new(bytes), 8 << 20);
        let mut frames = Vec::new();
        while let Some(frame) = decoder.read_message::<ChangeStreamFrame>().unwrap() {
            frames.push(frame);
        }
        frames
    }

    /// Every transaction frame byte for byte, and the batch end but for the
    /// digest that covers each batch's own stream id.
    fn assert_same_delivery(shared: &ChangeBatch, direct: &ChangeBatch) {
        let (shared, direct) = (frames(shared), frames(direct));
        assert_eq!(shared.len(), direct.len());
        assert_eq!(shared[1..shared.len() - 1], direct[1..direct.len() - 1]);
        let end = |frames: &[ChangeStreamFrame]| match frames.last().unwrap().body.clone() {
            Some(change_stream_frame::Body::BatchEnd(mut end)) => {
                end.frames_sha256.clear();
                end
            }
            other => panic!("the batch ends on {other:?}"),
        };
        assert_eq!(end(&shared), end(&direct));
    }

    fn operation_tables(batch: &ChangeBatch) -> Vec<String> {
        frames(batch)
            .into_iter()
            .filter_map(|frame| match frame.body {
                Some(change_stream_frame::Body::TransactionFragment(fragment)) => {
                    Some(fragment.operations)
                }
                _ => None,
            })
            .flatten()
            .map(|operation| operation.table_id)
            .collect()
    }

    #[test]
    fn a_build_from_the_tail_sends_what_a_build_from_the_log_sends() {
        let fixture = Fixture::new();
        let tail = fixture.followed(u64::MAX, 7);
        assert_eq!(tail.held(), (0, 7, 7));
        let tables = [&fixture.tasks, &fixture.projects];
        let authorization = fixture.authorization(&tables);
        let mut from = 0;
        let mut batches = 0;
        loop {
            let request = fixture.request(from, &tables, 2);
            let shared = fixture.built_from_tail(&tail, &authorization, &request, None);
            assert_same_delivery(&shared, &fixture.built_from_log(&authorization, &request));
            let through = fixture.sequence(&shared.through_cursor_inclusive);
            assert!(through > from, "a batch from the tail moved nothing");
            from = through;
            batches += 1;
            if shared.currently_up_to_date {
                break;
            }
        }
        assert_eq!((from, batches), (7, 4));
    }

    #[test]
    fn two_scopes_on_one_cursor_each_get_their_own_tables() {
        let fixture = Fixture::new();
        let tail = fixture.followed(u64::MAX, 6);
        let both = [&fixture.tasks, &fixture.projects];
        let tasks = [&fixture.tasks];
        let (wide, narrow) = (fixture.authorization(&both), fixture.authorization(&tasks));
        assert!(!Arc::ptr_eq(
            &tail.encoding(ScopeEncodingKey::of(&wide)),
            &tail.encoding(ScopeEncodingKey::of(&narrow))
        ));
        for (authorization, tables) in [(&wide, &both[..]), (&narrow, &tasks[..])] {
            let request = fixture.request(0, tables, 100);
            let shared = fixture.built_from_tail(&tail, authorization, &request, None);
            assert_same_delivery(&shared, &fixture.built_from_log(authorization, &request));
        }
        let narrow_batch =
            fixture.built_from_tail(&tail, &narrow, &fixture.request(0, &tasks, 100), None);
        let delivered = operation_tables(&narrow_batch);
        assert_eq!(delivered.len(), 4);
        assert!(delivered
            .iter()
            .all(|table| *table == fixture.tasks.to_string()));
    }

    #[test]
    fn a_range_the_tail_does_not_hold_is_left_to_the_log() {
        let fixture = Fixture::new();
        let tail = fixture.followed(5 * RECORD_BYTES, 9);
        let (floor, head, held) = tail.held();
        assert_eq!((floor, head, held), (4, 9, 5));
        let now = Instant::now();
        assert!(tail.view(floor - 1, None, None, now).is_none());
        assert!(tail.view(floor, None, None, now).is_some());
        assert!(tail.view(head, None, None, now).is_some());
        assert!(tail.view(head + 1, None, None, now).is_none());
        assert!(tail.view(floor, Some(head), None, now).is_some());
        assert!(tail.view(floor, Some(head + 1), None, now).is_none());
        assert!(tail.view(floor, None, Some(head + 1), now).is_none());
        assert!(tail.view(floor, Some(7), Some(6), now).is_none());
        tail.turn_stale();
        assert!(!tail.is_fresh(now));
        assert!(tail.view(floor, None, None, now).is_none());
    }

    #[test]
    fn a_ceiling_caps_what_a_build_from_the_tail_reads() {
        let fixture = Fixture::new();
        let tail = fixture.followed(u64::MAX, 6);
        let tables = [&fixture.tasks, &fixture.projects];
        let authorization = fixture.authorization(&tables);
        let capped = fixture.built_from_tail(
            &tail,
            &authorization,
            &fixture.request(1, &tables, 100),
            Some(4),
        );
        assert_eq!(fixture.sequence(&capped.through_cursor_inclusive), 4);
        assert!(capped.currently_up_to_date);
        assert_eq!(capped.relevant_transactions, 3);
        let at_ceiling = fixture.built_from_tail(
            &tail,
            &authorization,
            &fixture.request(4, &tables, 100),
            Some(4),
        );
        assert_eq!(fixture.sequence(&at_ceiling.through_cursor_inclusive), 4);
        assert_eq!(at_ceiling.relevant_transactions, 0);
    }

    #[test]
    fn a_fenced_build_from_the_tail_stops_at_its_fence() {
        let fixture = Fixture::new();
        let tail = fixture.followed(u64::MAX, 6);
        let tables = [&fixture.tasks, &fixture.projects];
        let authorization = fixture.fenced_authorization(&tables, 4);
        let request = fixture.fenced_request(1, Some(4), &tables, 100);
        let shared = fixture.built_from_tail(&tail, &authorization, &request, None);
        assert_same_delivery(&shared, &fixture.built_from_log(&authorization, &request));
        assert_eq!(fixture.sequence(&shared.through_cursor_inclusive), 4);
        assert!(shared.reached_requested_fence);
        assert_eq!(shared.relevant_transactions, 3);
    }

    #[test]
    fn a_build_keeps_the_records_the_tail_evicts_while_it_reads() {
        let fixture = Fixture::new();
        let tail = fixture.followed(4 * RECORD_BYTES, 4);
        let tables = [&fixture.tasks, &fixture.projects];
        let authorization = fixture.authorization(&tables);
        let view = tail.view(0, None, None, Instant::now()).unwrap();
        let encoding = tail.encoding(ScopeEncodingKey::of(&authorization));
        for _ in 0..8 {
            fixture.log.append(&[&fixture.tasks]);
        }
        tail.catch_up(&*fixture.log, Instant::now()).unwrap();
        assert_eq!(tail.held().0, 8);
        let shared = ChangeBatchBuilder::new(&view, &authorization)
            .unwrap()
            .with_shared_encoding(&encoding)
            .unwrap()
            .build_at(fixture.request(0, &tables, 100).request(), NOW)
            .unwrap();
        // The log has moved on, so the reference reads as many transactions
        // as the view held.
        let direct = fixture.built_from_log(&authorization, &fixture.request(0, &tables, 4));
        assert_eq!(
            shared.through_cursor_inclusive,
            direct.through_cursor_inclusive
        );
        let (shared, direct) = (frames(&shared), frames(&direct));
        assert_eq!(shared.len(), 6);
        assert_eq!(shared[1..5], direct[1..5]);
    }

    #[test]
    fn the_tail_holds_its_bound_with_every_encoding_of_its_records() {
        let fixture = Fixture::new();
        let budget = 6 * RECORD_BYTES;
        let tail = fixture.followed(budget, 6);
        let tables = [&fixture.tasks, &fixture.projects];
        let authorization = fixture.authorization(&tables);
        fixture.built_from_tail(
            &tail,
            &authorization,
            &fixture.request(0, &tables, 100),
            None,
        );
        let encoding = tail.encoding(ScopeEncodingKey::of(&authorization));
        assert!(encoding.encoded_bytes() > 0);
        fixture.log.append(&[&fixture.tasks]);
        tail.catch_up(&*fixture.log, Instant::now()).unwrap();
        let (floor, head, held) = tail.held();
        assert_eq!(head, 7);
        assert!(
            u64::try_from(held).unwrap() * RECORD_BYTES + encoding.encoded_bytes() <= budget,
            "the tail holds {held} records and {} encoded bytes over a {budget}-byte bound",
            encoding.encoded_bytes()
        );
        assert!(floor > 1);
    }

    #[test]
    fn a_frame_a_batch_still_carries_counts_against_the_bound_until_it_is_dropped() {
        let fixture = Fixture::new();
        let mut tail = fixture.followed(u64::MAX, 0);
        for _ in 0..3 {
            fixture.log.append(&[&fixture.tasks]);
        }
        tail.catch_up(&*fixture.log, Instant::now()).unwrap();
        let tables = [&fixture.tasks, &fixture.projects];
        let authorization = fixture.authorization(&tables);
        let carried = fixture.built_from_tail(
            &tail,
            &authorization,
            &fixture.request(0, &tables, 100),
            None,
        );
        let encoding = tail.encoding(ScopeEncodingKey::of(&authorization));
        let carried_bytes = encoding.encoded_bytes();
        assert!(carried_bytes > 0);
        let budget = carried_bytes + 4 * RECORD_BYTES;
        tail.budget_bytes = budget;

        // The tail drops the records the batch carries, and keeps their
        // frames counted for as long as the batch is held.
        for _ in 0..3 {
            fixture.log.append(&[&fixture.tasks]);
        }
        tail.catch_up(&*fixture.log, Instant::now()).unwrap();
        let (floor, head, held) = tail.held();
        assert_eq!((floor, head, held), (2, 6, 4));
        assert_eq!(encoding.encoded_bytes(), carried_bytes);
        assert!(u64::try_from(held).unwrap() * RECORD_BYTES + encoding.encoded_bytes() <= budget);

        drop(carried);
        assert!(encoding.encoded_bytes() < carried_bytes);
        fixture.log.append(&[&fixture.tasks]);
        tail.catch_up(&*fixture.log, Instant::now()).unwrap();
        assert_eq!(tail.held(), (2, 7, 5));
    }

    #[test]
    fn the_tail_starts_at_the_log_head_and_again_where_the_log_trims() {
        let fixture = Fixture::new();
        for _ in 0..3 {
            fixture.log.append(&[&fixture.tasks]);
        }
        let tail = fixture.tail(u64::MAX);
        assert!(!tail.catch_up(&*fixture.log, Instant::now()).unwrap());
        assert_eq!(tail.held(), (3, 3, 0));
        for _ in 0..9 {
            fixture.log.append(&[&fixture.tasks]);
        }
        fixture.log.trim_below(10);
        assert!(tail.catch_up(&*fixture.log, Instant::now()).unwrap());
        assert_eq!(tail.held(), (9, 12, 3));
    }

    #[test]
    fn a_trimmed_range_leaves_the_tail_and_its_encodings() {
        let fixture = Fixture::new();
        let tail = fixture.tail(u64::MAX);
        assert_eq!(tail.head(), None);
        let tail = fixture.followed(u64::MAX, 9);
        assert_eq!(tail.head(), Some(9));
        let tables = [&fixture.tasks, &fixture.projects];
        let authorization = fixture.authorization(&tables);
        fixture.built_from_tail(
            &tail,
            &authorization,
            &fixture.request(0, &tables, 100),
            None,
        );
        let encoding = tail.encoding(ScopeEncodingKey::of(&authorization));
        let encoded = encoding.encoded_bytes();

        fixture.log.trim_below(5);
        tail.trimmed(5);
        assert_eq!(tail.held(), (4, 9, 5));
        assert!(encoding.encoded_bytes() < encoded);
        let now = Instant::now();
        assert!(tail.view(3, None, None, now).is_none());
        let request = fixture.request(4, &tables, 100);
        assert_same_delivery(
            &fixture.built_from_tail(&tail, &authorization, &request, None),
            &fixture.built_from_log(&authorization, &request),
        );
        tail.trimmed(2);
        assert_eq!(tail.held(), (4, 9, 5));
    }

    #[test]
    fn a_read_from_before_a_trim_keeps_the_trimmed_floor() {
        let fixture = Fixture::new();
        let tail = fixture.followed(u64::MAX, 9);
        tail.trimmed(5);
        fixture.log.append(&[&fixture.tasks]);
        assert!(tail.catch_up(&*fixture.log, Instant::now()).unwrap());
        let view = tail.view(4, None, None, Instant::now()).unwrap();
        let (bounds, page) = view
            .read_bounds_and_page_after(3, None, 100, u64::MAX)
            .unwrap();
        assert_eq!(bounds.minimum_retained.sequence, 5);
        assert!(matches!(
            page,
            Err(LogError::BeforeRetention { minimum: 5, .. })
        ));
    }

    #[test]
    fn a_log_behind_the_tail_is_a_gap_not_a_restart() {
        let fixture = Fixture::new();
        let tail = fixture.followed(u64::MAX, 3);
        let behind = Fixture::new();
        behind.log.append(&[&behind.tasks]);
        assert!(matches!(
            tail.catch_up(&*behind.log, Instant::now()),
            Err(LogError::SequenceGap { .. })
        ));
        assert_eq!(tail.held(), (0, 3, 3));
    }

    #[test]
    fn a_page_missing_a_record_is_a_gap() {
        let fixture = Fixture::new();
        let tail = fixture.followed(u64::MAX, 2);
        for _ in 0..3 {
            fixture.log.append(&[&fixture.tasks]);
        }
        fixture
            .log
            .state
            .lock()
            .unwrap()
            .records
            .retain(|record| record.position.sequence != 4);
        assert!(matches!(
            tail.catch_up(&*fixture.log, Instant::now()),
            Err(LogError::SequenceGap {
                expected: 4,
                actual: 5
            })
        ));
        assert_eq!(tail.held().1, 3);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn a_notify_wakes_every_build_waiting_on_the_tail() {
        let tail = Fixture::new().tail(u64::MAX);
        let mut waiting = (0..3 * WAKE_SHARDS)
            .map(|_| tail.subscribe())
            .collect::<Vec<_>>();
        tail.notify();
        for advanced in &mut waiting {
            tokio::time::timeout(Duration::from_secs(5), advanced.changed())
                .await
                .expect("every waiting build wakes")
                .unwrap();
        }
        // Outside a runtime too.
        let tail = Arc::new(tail);
        let mut waiting = (0..WAKE_SHARDS + 1)
            .map(|_| tail.subscribe())
            .collect::<Vec<_>>();
        std::thread::spawn({
            let tail = Arc::clone(&tail);
            move || tail.notify()
        })
        .join()
        .unwrap();
        assert!(waiting
            .iter_mut()
            .all(|advanced| advanced.has_changed().unwrap()));
    }

    #[tokio::test]
    async fn the_follower_wakes_builds_when_it_reads_and_when_it_fails() {
        let fixture = Fixture::new();
        let tail = Arc::new(fixture.tail(u64::MAX));
        let log = Arc::clone(&fixture.log);
        let readers = LogReaders::new(1, move || Ok(FakeReader(Arc::clone(&log))));
        let head = watch::Sender::new(0_u64);
        let mut advanced = tail.subscribe();
        let follower = tokio::spawn(follow(Arc::downgrade(&tail), readers, head.subscribe()));
        let wait = |advanced: &mut watch::Receiver<u64>| {
            let mut advanced = advanced.clone();
            async move {
                tokio::time::timeout(Duration::from_secs(5), advanced.changed())
                    .await
                    .expect("the follower wakes waiting builds")
                    .unwrap();
            }
        };
        tokio::time::timeout(Duration::from_secs(5), async {
            while !tail.is_fresh(Instant::now()) {
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .unwrap();
        advanced.borrow_and_update();

        fixture.log.append(&[&fixture.tasks]);
        head.send_modify(|count| *count += 1);
        wait(&mut advanced).await;
        assert_eq!(tail.held(), (0, 1, 1));
        assert!(tail.view(0, None, None, Instant::now()).is_some());
        advanced.borrow_and_update();

        fixture.log.fail(true);
        head.send_modify(|count| *count += 1);
        wait(&mut advanced).await;
        assert!(!tail.is_fresh(Instant::now()));
        assert!(tail.view(0, None, None, Instant::now()).is_none());

        drop(tail);
        head.send_modify(|count| *count += 1);
        tokio::time::timeout(Duration::from_secs(5), follower)
            .await
            .expect("the follower ends with the tail")
            .unwrap();
    }

    #[tokio::test]
    async fn a_follower_whose_read_hangs_wakes_builds_when_the_tail_turns_stale() {
        let fixture = Fixture::new();
        let tail = Arc::new(fixture.tail(u64::MAX));
        let log = Arc::clone(&fixture.log);
        let readers = LogReaders::new(1, move || Ok(FakeReader(Arc::clone(&log))));
        let head = watch::Sender::new(0_u64);
        let mut advanced = tail.subscribe();
        let follower = tokio::spawn(follow(Arc::downgrade(&tail), readers, head.subscribe()));
        tokio::time::timeout(Duration::from_secs(5), async {
            while !tail.is_fresh(Instant::now()) {
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .unwrap();
        advanced.borrow_and_update();

        // The read the commit signals blocks without failing.
        let hung = fixture.log.hang();
        let committed = Instant::now();
        fixture.log.append(&[&fixture.tasks]);
        head.send_modify(|count| *count += 1);
        tokio::time::timeout(STALE_AFTER * 2, advanced.changed())
            .await
            .expect("a hung read wakes waiting builds once the tail is stale")
            .unwrap();
        assert!(committed.elapsed() <= STALE_AFTER + FOLLOW_INTERVAL);
        assert!(!tail.is_fresh(Instant::now()));
        assert!(tail.view(0, None, None, Instant::now()).is_none());
        advanced.borrow_and_update();

        drop(hung);
        tokio::time::timeout(Duration::from_secs(5), advanced.changed())
            .await
            .expect("the read that hung wakes builds when it lands")
            .unwrap();
        assert_eq!(tail.held(), (0, 1, 1));

        drop(tail);
        head.send_modify(|count| *count += 1);
        tokio::time::timeout(Duration::from_secs(5), follower)
            .await
            .expect("the follower ends with the tail")
            .unwrap();
    }
}
