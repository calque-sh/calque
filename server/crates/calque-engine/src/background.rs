//! Work that runs below live delivery: a re-cut's build and its encodes, and
//! a tenant's build until a page waits on it, in the background class, and
//! tenant builds a page waits on that run beside each other, in the below
//! class.

use std::{
    io,
    sync::{
        atomic::{AtomicBool, AtomicU8, Ordering},
        Arc,
    },
    time::Duration,
};

use background_thread::Class;

/// Runs `work` on a thread of its own named `name`, in the background class,
/// and waits for it.
pub(crate) fn run_in_background<T: Send>(
    name: &str,
    work: impl FnOnce() -> T + Send,
) -> io::Result<T> {
    std::thread::scope(|scope| {
        let thread = std::thread::Builder::new()
            .name(name.to_owned())
            .spawn_scoped(scope, || {
                lower_current_thread(name);
                work()
            })?;
        Ok(thread
            .join()
            .unwrap_or_else(|panic| std::panic::resume_unwind(panic)))
    })
}

/// Lowers the calling thread, or says that `work` runs at normal priority.
pub(crate) fn lower_current_thread(work: &str) {
    if let Err(error) = background_thread::lower_current_thread() {
        tracing::warn!(work, %error, "runs at normal priority");
    }
}

/// A current-thread runtime whose blocking tasks run in the class of the
/// thread that builds it.
pub(crate) fn current_thread_runtime() -> io::Result<tokio::runtime::Runtime> {
    WorkClass::new(background_thread::current_thread_class()).runtime()
}

const BUILD_RUNTIME_WORKERS: usize = 2;

/// The class one piece of work runs in, shared by every thread it runs on.
/// Each thread follows a change at its next [`Self::enter`].
#[derive(Clone)]
pub(crate) struct WorkClass(Arc<Shared>);

struct Shared {
    class: AtomicU8,
    /// Whether a thread failed to enter the class, which is said once.
    refused: AtomicBool,
}

impl WorkClass {
    pub(crate) fn new(class: Class) -> Self {
        Self(Arc::new(Shared {
            class: AtomicU8::new(class as u8),
            refused: AtomicBool::new(false),
        }))
    }

    pub(crate) fn get(&self) -> Class {
        match self.0.class.load(Ordering::Relaxed) {
            0 => Class::Normal,
            1 => Class::Below,
            _ => Class::Background,
        }
    }

    /// Moves the work to `class`.
    pub(crate) fn set(&self, class: Class) {
        self.0.class.store(class as u8, Ordering::Relaxed);
    }

    /// Moves the work up to `class`, and says whether it ran below it.
    pub(crate) fn raise(&self, class: Class) -> bool {
        self.0.class.fetch_min(class as u8, Ordering::Relaxed) > class as u8
    }

    /// Moves the calling thread to the work's class. Linux refuses to raise
    /// an unprivileged thread, which then stays where it is.
    pub(crate) fn enter(&self, work: &str) {
        let class = self.get();
        if background_thread::current_thread_class() == class {
            return;
        }
        if let Err(error) = background_thread::set_current_thread_class(class) {
            if !self.0.refused.swap(true, Ordering::Relaxed) {
                tracing::warn!(work, ?class, %error, "runs outside its class");
            }
        }
    }

    /// A current-thread runtime whose blocking threads enter the work's class
    /// when they start. A blocking thread ends as soon as it is idle, so a task
    /// usually starts on a new thread, but one spawned as another task ends
    /// can run on that task's thread, in the class it entered.
    pub(crate) fn runtime(&self) -> io::Result<tokio::runtime::Runtime> {
        let class = self.clone();
        tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .thread_keep_alive(Duration::ZERO)
            .on_thread_start(move || class.enter("a blocking task"))
            .build()
    }

    /// A runtime for one build. The calling thread encodes rows while
    /// `BUILD_RUNTIME_WORKERS` threads read its PostgreSQL connections, as
    /// they do on the process's runtime in a process that serves one tenant.
    /// Each thread enters the class in force when it wakes.
    pub(crate) fn build_runtime(&self) -> io::Result<tokio::runtime::Runtime> {
        let started = self.clone();
        let woken = self.clone();
        tokio::runtime::Builder::new_multi_thread()
            .worker_threads(BUILD_RUNTIME_WORKERS)
            .enable_all()
            .thread_keep_alive(Duration::ZERO)
            .on_thread_start(move || started.enter("a build's connections"))
            .on_thread_unpark(move || woken.enter("a build's connections"))
            .build()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_builds_connection_threads_follow_the_class_it_is_raised_to() {
        std::thread::spawn(|| {
            if !background_thread::lowered_threads_can_rise() {
                return;
            }
            let class = WorkClass::new(Class::Background);
            let runtime = class.build_runtime().unwrap();
            let on_worker = |runtime: &tokio::runtime::Runtime| {
                runtime
                    .block_on(runtime.spawn(async {
                        tokio::task::yield_now().await;
                        background_thread::current_thread_class()
                    }))
                    .unwrap()
            };
            assert_eq!(on_worker(&runtime), Class::Background);
            assert!(class.raise(Class::Normal));
            std::thread::sleep(Duration::from_millis(20));
            assert_eq!(on_worker(&runtime), Class::Normal);
        })
        .join()
        .unwrap();
    }

    #[test]
    fn work_raised_while_it_runs_moves_its_next_threads_and_never_moves_down() {
        std::thread::spawn(|| {
            let class = WorkClass::new(Class::Background);
            class.enter("a test");
            let on_a_new_blocking_thread = || {
                class.runtime().unwrap().block_on(async {
                    tokio::task::spawn_blocking(background_thread::current_thread_class)
                        .await
                        .unwrap()
                })
            };
            assert_eq!(background_thread::current_thread_class(), Class::Background);
            assert_eq!(on_a_new_blocking_thread(), Class::Background);
            assert!(!class.raise(Class::Background));
            assert!(class.raise(Class::Normal));
            assert!(!class.raise(Class::Background));
            assert_eq!(class.get(), Class::Normal);
            class.enter("a test");
            let raised = if cfg!(target_os = "linux") {
                background_thread::current_thread_class()
            } else {
                Class::Normal
            };
            assert_eq!(background_thread::current_thread_class(), raised);
            assert_eq!(on_a_new_blocking_thread(), raised);
        })
        .join()
        .unwrap();
    }
}
