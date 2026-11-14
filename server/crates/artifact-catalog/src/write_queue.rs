//! One queue of writers per catalog file in this process.
//!
//! SQLite's busy handler retries on a timer, so a connection that commits and
//! begins its next write at once keeps the write lock however long another
//! connection has waited. Every catalog connection in the process takes a
//! turn here before it begins a write, and turns go in arrival order.

use std::{
    collections::{HashMap, VecDeque},
    path::{Path, PathBuf},
    sync::{Arc, Condvar, Mutex, MutexGuard, OnceLock, PoisonError, Weak},
    time::{Duration, Instant},
};

use background_thread::Class;

pub(crate) struct WriteQueue {
    state: Mutex<QueueState>,
    turn_ended: Condvar,
}

#[derive(Default)]
struct QueueState {
    writing: bool,
    waiting: VecDeque<u64>,
    next_ticket: u64,
}

/// The right to write until it drops. A background thread holds it in the
/// normal class, so the writers behind it do not wait on a throttled disk.
pub(crate) struct Turn {
    queue: Arc<WriteQueue>,
    raised_from: Option<Class>,
}

impl WriteQueue {
    /// The queue of the catalog at `path`, which must exist.
    pub(crate) fn for_path(path: &Path) -> std::io::Result<Arc<Self>> {
        static QUEUES: OnceLock<Mutex<HashMap<PathBuf, Weak<WriteQueue>>>> = OnceLock::new();
        let key = std::fs::canonicalize(path)?;
        let mut queues = QUEUES
            .get_or_init(Mutex::default)
            .lock()
            .unwrap_or_else(PoisonError::into_inner);
        if let Some(queue) = queues.get(&key).and_then(Weak::upgrade) {
            return Ok(queue);
        }
        queues.retain(|_, queue| queue.strong_count() > 0);
        let queue = Arc::new(Self {
            state: Mutex::default(),
            turn_ended: Condvar::new(),
        });
        queues.insert(key, Arc::downgrade(&queue));
        Ok(queue)
    }

    /// Waits for every writer that asked earlier, or gives up after
    /// `timeout` and returns `None`.
    pub(crate) fn take_turn(self: &Arc<Self>, timeout: Duration) -> Option<Turn> {
        let deadline = Instant::now() + timeout;
        let mut state = self.lock();
        let ticket = state.next_ticket;
        state.next_ticket += 1;
        state.waiting.push_back(ticket);
        loop {
            if !state.writing && state.waiting.front() == Some(&ticket) {
                state.waiting.pop_front();
                state.writing = true;
                drop(state);
                return Some(Turn {
                    queue: Arc::clone(self),
                    raised_from: raise_for_the_turn(),
                });
            }
            let now = Instant::now();
            // Only a held turn keeps the front waiter here past its
            // deadline, and that turn's drop wakes the next one.
            if now >= deadline {
                state.waiting.retain(|waiting| *waiting != ticket);
                return None;
            }
            state = self
                .turn_ended
                .wait_timeout(state, deadline - now)
                .unwrap_or_else(PoisonError::into_inner)
                .0;
        }
    }

    fn lock(&self) -> MutexGuard<'_, QueueState> {
        self.state.lock().unwrap_or_else(PoisonError::into_inner)
    }
}

/// Moves a thread that runs below live delivery to the normal class, where
/// the platform lets it, and returns the class it left.
fn raise_for_the_turn() -> Option<Class> {
    let class = background_thread::current_thread_class();
    (class != Class::Normal && background_thread::set_current_thread_class(Class::Normal).is_ok())
        .then_some(class)
}

impl Drop for Turn {
    fn drop(&mut self) {
        self.queue.lock().writing = false;
        self.queue.turn_ended.notify_all();
        if let Some(class) = self.raised_from {
            let _ = background_thread::set_current_thread_class(class);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn turns_go_in_arrival_order_and_a_waiter_gives_up_at_its_timeout() {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("catalog");
        std::fs::write(&path, b"").unwrap();
        let queue = WriteQueue::for_path(&path).unwrap();
        assert!(Arc::ptr_eq(
            &queue,
            &WriteQueue::for_path(&directory.path().join(".").join("catalog")).unwrap()
        ));

        let first = queue.take_turn(Duration::from_secs(5)).unwrap();
        assert!(queue.take_turn(Duration::from_millis(20)).is_none());
        let order = Mutex::new(Vec::new());
        std::thread::scope(|scope| {
            for writer in 0..4 {
                let queue = &queue;
                let order = &order;
                scope.spawn(move || {
                    let _turn = queue.take_turn(Duration::from_secs(5)).unwrap();
                    order.lock().unwrap().push(writer);
                });
                while queue.lock().waiting.len() <= writer {
                    std::thread::yield_now();
                }
            }
            drop(first);
        });
        assert_eq!(*order.lock().unwrap(), [0, 1, 2, 3]);
    }

    #[test]
    fn a_writer_below_delivery_holds_its_turn_in_the_normal_class_and_goes_back() {
        if !background_thread::lowered_threads_can_rise() {
            return;
        }
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("catalog");
        std::fs::write(&path, b"").unwrap();
        let queue = WriteQueue::for_path(&path).unwrap();
        std::thread::spawn(move || {
            background_thread::set_current_thread_class(Class::Below).unwrap();
            let turn = queue.take_turn(Duration::from_secs(5)).unwrap();
            assert_eq!(background_thread::current_thread_class(), Class::Normal);
            drop(turn);
            assert_eq!(background_thread::current_thread_class(), Class::Below);
        })
        .join()
        .unwrap();
    }

    #[test]
    fn a_background_writer_holds_its_turn_in_the_normal_class() {
        if !background_thread::lowered_threads_can_rise() {
            return;
        }
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("catalog");
        std::fs::write(&path, b"").unwrap();
        let queue = WriteQueue::for_path(&path).unwrap();
        std::thread::spawn(move || {
            background_thread::lower_current_thread().unwrap();
            let turn = queue.take_turn(Duration::from_secs(5)).unwrap();
            assert!(!background_thread::current_thread_is_lowered());
            drop(turn);
            assert!(background_thread::current_thread_is_lowered());
        })
        .join()
        .unwrap();
    }
}
