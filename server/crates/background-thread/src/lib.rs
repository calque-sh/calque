//! Moves a thread to the scheduler's background class, for work that can
//! wait while live delivery and the browser cannot: a re-cut's build and the
//! encodes that follow a publication.
//!
//! On macOS this is the background QoS class: the thread runs on the slower
//! cores, and gets what other work leaves idle. On Linux it is nice 19 for the
//! thread alone. A thread macOS starts does not take its creator's class, so
//! work that starts threads starts them through [`spawn`] or [`spawn_scoped`],
//! which carry the class across. A library that starts its own threads, as
//! libzstd's workers do, starts them at normal priority.

use std::{io, thread};

/// Lowers the calling thread. Nothing raises it again.
pub fn lower_current_thread() -> io::Result<()> {
    platform::lower()
}

/// Whether the calling thread runs in the background class.
pub fn current_thread_is_lowered() -> bool {
    platform::is_lowered()
}

/// Starts `work` on a thread `builder` describes, lowered if the calling
/// thread is.
pub fn spawn<T: Send + 'static>(
    builder: thread::Builder,
    work: impl FnOnce() -> T + Send + 'static,
) -> io::Result<thread::JoinHandle<T>> {
    let lowered = current_thread_is_lowered();
    builder.spawn(move || {
        enter(lowered);
        work()
    })
}

/// Starts `work` on a thread of `scope`, lowered if the calling thread is.
pub fn spawn_scoped<'scope, T: Send + 'scope>(
    scope: &'scope thread::Scope<'scope, '_>,
    work: impl FnOnce() -> T + Send + 'scope,
) -> thread::ScopedJoinHandle<'scope, T> {
    let lowered = current_thread_is_lowered();
    scope.spawn(move || {
        enter(lowered);
        work()
    })
}

fn enter(lowered: bool) {
    if lowered {
        if let Err(error) = lower_current_thread() {
            tracing::warn!(%error, "a background thread runs at normal priority");
        }
    }
}

#[cfg(target_os = "macos")]
mod platform {
    use std::io;

    use libc::qos_class_t::{QOS_CLASS_BACKGROUND, QOS_CLASS_UNSPECIFIED};

    #[allow(unsafe_code)]
    pub(super) fn lower() -> io::Result<()> {
        // SAFETY: the call takes two integers and changes only the calling
        // thread's QoS class.
        let status = unsafe { libc::pthread_set_qos_class_self_np(QOS_CLASS_BACKGROUND, 0) };
        if status == 0 {
            Ok(())
        } else {
            Err(io::Error::from_raw_os_error(status))
        }
    }

    #[allow(unsafe_code)]
    pub(super) fn is_lowered() -> bool {
        let mut class = QOS_CLASS_UNSPECIFIED;
        let mut priority = 0;
        // SAFETY: `pthread_self` names the calling thread, which outlives the
        // call, and both out-pointers are to locals that do.
        let status = unsafe {
            libc::pthread_get_qos_class_np(libc::pthread_self(), &mut class, &mut priority)
        };
        status == 0 && matches!(class, QOS_CLASS_BACKGROUND)
    }
}

#[cfg(target_os = "linux")]
mod platform {
    use std::io;

    use rustix::{
        process::{getpriority_process, setpriority_process},
        thread::gettid,
    };

    /// The lowest priority `setpriority` takes. Linux applies it to the one
    /// thread `gettid` names.
    const NICE: i32 = 19;

    pub(super) fn lower() -> io::Result<()> {
        setpriority_process(Some(gettid()), NICE).map_err(io::Error::from)
    }

    pub(super) fn is_lowered() -> bool {
        getpriority_process(Some(gettid())).is_ok_and(|priority| priority == NICE)
    }
}

#[cfg(not(any(target_os = "macos", target_os = "linux")))]
mod platform {
    use std::io;

    pub(super) fn lower() -> io::Result<()> {
        Err(io::Error::from(io::ErrorKind::Unsupported))
    }

    pub(super) fn is_lowered() -> bool {
        false
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_lowered_thread_starts_its_threads_lowered_and_only_it_is_lowered() {
        thread::spawn(|| {
            assert!(!current_thread_is_lowered());
            assert!(!spawn(thread::Builder::new(), current_thread_is_lowered)
                .unwrap()
                .join()
                .unwrap());
            lower_current_thread().unwrap();
            assert!(current_thread_is_lowered());
            assert!(spawn(thread::Builder::new(), current_thread_is_lowered)
                .unwrap()
                .join()
                .unwrap());
            assert!(thread::scope(|scope| spawn_scoped(
                scope,
                current_thread_is_lowered
            )
            .join()
            .unwrap()));
        })
        .join()
        .unwrap();
        assert!(!current_thread_is_lowered());
    }
}
