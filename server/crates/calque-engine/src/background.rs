//! Work that runs in the scheduler's background class beside live delivery:
//! a re-cut's build and its encodes. A tenant's artifact can be built and
//! re-encoded through the same helpers.

use std::io;

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

/// A current-thread runtime whose blocking tasks run lowered when the thread
/// that builds it is.
pub(crate) fn current_thread_runtime() -> io::Result<tokio::runtime::Runtime> {
    let lowered = background_thread::current_thread_is_lowered();
    tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .on_thread_start(move || {
            if lowered {
                lower_current_thread("a blocking task");
            }
        })
        .build()
}
