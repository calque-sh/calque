//! The canonical-log connections change builds, bootstrap planners and the
//! change tail read through.
//!
//! A change build takes a reader of its own for as long as it runs, so builds
//! for different browsers run side by side up to the pool's bound instead of
//! one after another behind the writer connection's lock. Every reader runs
//! the writer's statements over the same log.

use std::{
    ops::Deref,
    sync::{Arc, Mutex as StdMutex, PoisonError},
};

use canonical_log::{LogError, PostgresCanonicalLogReader};
use tokio::sync::{OwnedSemaphorePermit, Semaphore};

/// The most canonical-log readers one process opens for change builds that
/// the change tail cannot serve.
pub(crate) const MAX_LOG_READERS: usize = 8;
/// One for each bootstrap planner that may run at once.
const PLANNER_LOG_READERS: usize = crate::backend::MAX_CONCURRENT_PLANNERS;

/// A connection the pool hands out again only while it is open.
pub trait PooledLogReader: Send + Sync + 'static {
    fn is_closed(&self) -> bool;
}

impl PooledLogReader for PostgresCanonicalLogReader {
    fn is_closed(&self) -> bool {
        PostgresCanonicalLogReader::is_closed(self)
    }
}

type Connect<R> = Arc<dyn Fn() -> Result<R, LogError> + Send + Sync>;

/// One source's readers, in a pool for each purpose, so that change builds
/// never hold up a bootstrap's planner or the change tail's follower.
#[derive(Clone)]
pub struct SourceLogReaders {
    pub(crate) changes: Arc<LogReaders>,
    pub(crate) planners: Arc<LogReaders>,
    /// The change tail's follower holds its one reader only while it reads.
    pub(crate) tail: Arc<LogReaders>,
    /// So does a partitioned scope's tenant follower.
    pub(crate) tenants: Arc<LogReaders>,
}

impl SourceLogReaders {
    pub(crate) fn new(
        connect: impl Fn() -> Result<PostgresCanonicalLogReader, LogError> + Send + Sync + 'static,
    ) -> Self {
        let connect: Connect<PostgresCanonicalLogReader> = Arc::new(connect);
        let pool = |capacity| {
            let connect = Arc::clone(&connect);
            LogReaders::new(capacity, move || connect())
        };
        Self {
            changes: pool(MAX_LOG_READERS),
            planners: pool(PLANNER_LOG_READERS),
            tail: pool(1),
            tenants: pool(1),
        }
    }
}

/// At most a fixed number of readers of one source's canonical log.
///
/// A request takes an idle reader, or opens one while fewer than the bound
/// exist, and otherwise waits for one to be put back. A reader whose
/// connection has closed is dropped rather than put back, so the next request
/// opens a fresh one.
pub struct LogReaders<R: PooledLogReader = PostgresCanonicalLogReader> {
    connect: Connect<R>,
    idle: StdMutex<Vec<R>>,
    permits: Arc<Semaphore>,
}

impl<R: PooledLogReader> LogReaders<R> {
    pub(crate) fn new(
        capacity: usize,
        connect: impl Fn() -> Result<R, LogError> + Send + Sync + 'static,
    ) -> Arc<Self> {
        Arc::new(Self {
            connect: Arc::new(connect),
            idle: StdMutex::new(Vec::with_capacity(capacity)),
            permits: Arc::new(Semaphore::new(capacity)),
        })
    }

    /// A reader for one build, held until it is dropped. Opening one blocks,
    /// so it runs on the blocking pool.
    pub(crate) async fn acquire(self: &Arc<Self>) -> Result<LogReader<R>, LogError> {
        let permit = Arc::clone(&self.permits)
            .acquire_owned()
            .await
            .map_err(|_| LogError::PostgresUnavailable)?;
        let reader = match self.take_idle() {
            Some(reader) => reader,
            None => {
                let connect = Arc::clone(&self.connect);
                tokio::task::spawn_blocking(move || connect())
                    .await
                    .map_err(|_| LogError::PostgresUnavailable)??
            }
        };
        Ok(LogReader {
            reader: Some(reader),
            readers: Arc::clone(self),
            _permit: permit,
        })
    }

    fn take_idle(&self) -> Option<R> {
        let mut idle = self.idle.lock().unwrap_or_else(PoisonError::into_inner);
        while let Some(reader) = idle.pop() {
            if !reader.is_closed() {
                return Some(reader);
            }
        }
        None
    }
}

/// One reader, taken from its pool and put back when dropped.
pub(crate) struct LogReader<R: PooledLogReader = PostgresCanonicalLogReader> {
    reader: Option<R>,
    readers: Arc<LogReaders<R>>,
    _permit: OwnedSemaphorePermit,
}

impl<R: PooledLogReader> Deref for LogReader<R> {
    type Target = R;

    fn deref(&self) -> &R {
        self.reader
            .as_ref()
            .expect("a reader is held until it is dropped")
    }
}

impl<R: PooledLogReader> Drop for LogReader<R> {
    fn drop(&mut self) {
        // This runs before the permit is released, so the request the permit
        // admits next finds this reader idle instead of opening another.
        if let Some(reader) = self.reader.take() {
            if !reader.is_closed() {
                self.readers
                    .idle
                    .lock()
                    .unwrap_or_else(PoisonError::into_inner)
                    .push(reader);
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use std::{
        sync::atomic::{AtomicBool, AtomicUsize, Ordering},
        time::Duration,
    };

    use super::*;

    struct FakeReader {
        id: usize,
        closed: Arc<AtomicBool>,
    }

    impl PooledLogReader for FakeReader {
        fn is_closed(&self) -> bool {
            self.closed.load(Ordering::Acquire)
        }
    }

    struct Opened {
        count: Arc<AtomicUsize>,
        closed: Arc<StdMutex<Vec<Arc<AtomicBool>>>>,
    }

    fn pool(capacity: usize) -> (Arc<LogReaders<FakeReader>>, Opened) {
        let count = Arc::new(AtomicUsize::new(0));
        let closed = Arc::new(StdMutex::new(Vec::new()));
        let opened = Opened {
            count: Arc::clone(&count),
            closed: Arc::clone(&closed),
        };
        let readers = LogReaders::new(capacity, move || {
            let flag = Arc::new(AtomicBool::new(false));
            closed.lock().unwrap().push(Arc::clone(&flag));
            Ok(FakeReader {
                id: count.fetch_add(1, Ordering::AcqRel),
                closed: flag,
            })
        });
        (readers, opened)
    }

    #[tokio::test]
    async fn a_build_does_not_wait_for_another_build_holding_a_reader() {
        let (readers, opened) = pool(MAX_LOG_READERS);
        let held = readers.acquire().await.unwrap();
        let second = tokio::time::timeout(Duration::from_secs(5), readers.acquire())
            .await
            .expect("a second build gets a reader while the first holds one")
            .unwrap();
        assert_ne!(held.id, second.id);
        assert_eq!(opened.count.load(Ordering::Acquire), 2);
    }

    #[tokio::test]
    async fn a_reader_put_back_is_handed_out_again_rather_than_another_opened() {
        let (readers, opened) = pool(MAX_LOG_READERS);
        let first = readers.acquire().await.unwrap().id;
        let again = readers.acquire().await.unwrap().id;
        assert_eq!(first, again);
        assert_eq!(opened.count.load(Ordering::Acquire), 1);
    }

    #[tokio::test]
    async fn a_reader_whose_connection_closed_is_not_handed_out_again() {
        let (readers, opened) = pool(MAX_LOG_READERS);
        let first = readers.acquire().await.unwrap();
        let first_id = first.id;
        drop(first);
        // It closes while idle, as a connection PostgreSQL ended would.
        opened.closed.lock().unwrap()[0].store(true, Ordering::Release);
        let next = readers.acquire().await.unwrap();
        assert_ne!(next.id, first_id);
        assert_eq!(opened.count.load(Ordering::Acquire), 2);

        // And one that closed while held is not put back.
        let next_id = next.id;
        opened.closed.lock().unwrap()[1].store(true, Ordering::Release);
        drop(next);
        let last = readers.acquire().await.unwrap();
        assert_ne!(last.id, next_id);
        assert_eq!(opened.count.load(Ordering::Acquire), 3);
    }

    #[tokio::test]
    async fn with_every_reader_taken_the_next_request_waits_rather_than_opens_another() {
        let (readers, opened) = pool(MAX_LOG_READERS);
        let mut held = Vec::new();
        for _ in 0..MAX_LOG_READERS {
            held.push(readers.acquire().await.unwrap());
        }
        let waiting = tokio::spawn({
            let readers = Arc::clone(&readers);
            async move { readers.acquire().await.map(|reader| reader.id) }
        });
        tokio::time::sleep(Duration::from_millis(50)).await;
        assert!(!waiting.is_finished());
        assert_eq!(opened.count.load(Ordering::Acquire), MAX_LOG_READERS);

        let released = held.pop().unwrap().id;
        let got = tokio::time::timeout(Duration::from_secs(5), waiting)
            .await
            .expect("a reader put back admits the waiting request")
            .unwrap()
            .unwrap();
        assert_eq!(got, released);
        assert_eq!(opened.count.load(Ordering::Acquire), MAX_LOG_READERS);
    }

    #[tokio::test]
    async fn a_reader_that_fails_to_open_fails_the_request_and_frees_its_place() {
        let attempts = Arc::new(AtomicUsize::new(0));
        let readers = LogReaders::new(1, {
            let attempts = Arc::clone(&attempts);
            move || {
                if attempts.fetch_add(1, Ordering::AcqRel) == 0 {
                    Err(LogError::PostgresUnavailable)
                } else {
                    Ok(FakeReader {
                        id: 7,
                        closed: Arc::new(AtomicBool::new(false)),
                    })
                }
            }
        });
        assert!(matches!(
            readers.acquire().await,
            Err(LogError::PostgresUnavailable)
        ));
        let reader = tokio::time::timeout(Duration::from_secs(5), readers.acquire())
            .await
            .expect("a failed open does not keep its place")
            .unwrap();
        assert_eq!(reader.id, 7);
    }
}
