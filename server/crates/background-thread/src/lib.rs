//! Moves a thread below live delivery and the browser. Two classes sit there.
//!
//! The background class is for work nobody waits on: a re-cut's build, the
//! encodes that follow a publication, a tenant warmed ahead of its users. On
//! macOS it is the background QoS class: a background thread runs on the
//! slower cores, gets what other work leaves idle, and its disk writes give
//! way to other work's. On Linux it is nice 19 for the thread alone.
//!
//! The below class is for builds a page waits on that run beside each other.
//! It keeps the cores and the disk priority of normal work and gives way when
//! normal work wants the core: on macOS the default QoS class at its lowest
//! relative priority, on Linux nice 10.
//!
//! A thread macOS starts does not take its creator's class, so work that
//! starts threads starts them through [`spawn`] or [`spawn_scoped`], which
//! carry the class across. A library that starts its own threads, as
//! libzstd's workers do, starts them at normal priority.
//!
//! macOS lets a thread raise its own class again. Linux lets a thread lower
//! its niceness only within `RLIMIT_NICE`, which is 0 unless configured, so
//! there a lowered thread stays lowered unless the limit is 20 or the process
//! holds `CAP_SYS_NICE` ([`lowered_threads_can_rise`]).

use std::{io, thread};

/// How far below live delivery a thread runs.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum Class {
    Normal,
    Below,
    Background,
}

/// Moves the calling thread to `class`.
pub fn set_current_thread_class(class: Class) -> io::Result<()> {
    platform::set(class)
}

/// The class the calling thread runs in.
pub fn current_thread_class() -> Class {
    platform::current()
}

/// Moves the calling thread to the background class.
pub fn lower_current_thread() -> io::Result<()> {
    set_current_thread_class(Class::Background)
}

/// Whether the calling thread runs in the background class.
pub fn current_thread_is_lowered() -> bool {
    current_thread_class() == Class::Background
}

/// Whether a thread moved to the background class can move back to normal
/// priority. macOS lets it. Linux lets it when `RLIMIT_NICE` is 20 or more, or
/// when the process holds `CAP_SYS_NICE`, which a container's non-root user
/// does not even when the container adds it.
pub fn lowered_threads_can_rise() -> bool {
    thread::spawn(|| match lower_current_thread() {
        Ok(()) => set_current_thread_class(Class::Normal).is_ok(),
        Err(_) => true,
    })
    .join()
    .unwrap_or(false)
}

/// Starts `work` on a thread `builder` describes, in the calling thread's
/// class.
pub fn spawn<T: Send + 'static>(
    builder: thread::Builder,
    work: impl FnOnce() -> T + Send + 'static,
) -> io::Result<thread::JoinHandle<T>> {
    let class = current_thread_class();
    builder.spawn(move || {
        enter(class);
        work()
    })
}

/// Starts `work` on a thread of `scope`, in the calling thread's class.
pub fn spawn_scoped<'scope, T: Send + 'scope>(
    scope: &'scope thread::Scope<'scope, '_>,
    work: impl FnOnce() -> T + Send + 'scope,
) -> thread::ScopedJoinHandle<'scope, T> {
    let class = current_thread_class();
    scope.spawn(move || {
        enter(class);
        work()
    })
}

fn enter(class: Class) {
    if class == current_thread_class() {
        return;
    }
    if let Err(error) = set_current_thread_class(class) {
        tracing::warn!(?class, %error, "a thread runs outside the class of the thread that started it");
    }
}

#[cfg(target_os = "macos")]
mod platform {
    use std::io;

    use libc::qos_class_t::{QOS_CLASS_BACKGROUND, QOS_CLASS_DEFAULT, QOS_CLASS_UNSPECIFIED};

    use super::Class;

    /// The lowest priority a thread can take within its QoS class.
    const LOWEST_RELATIVE_PRIORITY: i32 = -15;

    #[allow(unsafe_code)]
    pub(super) fn set(class: Class) -> io::Result<()> {
        let (qos, relative_priority) = match class {
            Class::Normal => (QOS_CLASS_DEFAULT, 0),
            Class::Below => (QOS_CLASS_DEFAULT, LOWEST_RELATIVE_PRIORITY),
            Class::Background => (QOS_CLASS_BACKGROUND, 0),
        };
        // SAFETY: the call takes two integers and changes only the calling
        // thread's QoS class.
        let status = unsafe { libc::pthread_set_qos_class_self_np(qos, relative_priority) };
        if status == 0 {
            Ok(())
        } else {
            Err(io::Error::from_raw_os_error(status))
        }
    }

    #[allow(unsafe_code)]
    pub(super) fn current() -> Class {
        let mut class = QOS_CLASS_UNSPECIFIED;
        let mut priority = 0;
        // SAFETY: `pthread_self` names the calling thread, which outlives the
        // call, and both out-pointers are to locals that do.
        let status = unsafe {
            libc::pthread_get_qos_class_np(libc::pthread_self(), &mut class, &mut priority)
        };
        match class {
            _ if status != 0 => Class::Normal,
            QOS_CLASS_BACKGROUND => Class::Background,
            QOS_CLASS_DEFAULT if priority < 0 => Class::Below,
            _ => Class::Normal,
        }
    }
}

#[cfg(target_os = "linux")]
mod platform {
    use std::io;

    use rustix::{
        process::{getpriority_process, setpriority_process},
        thread::gettid,
    };

    use super::Class;

    /// The niceness of each class. Under contention the kernel gives a nice
    /// 19 thread about a seventieth of the CPU a nice 0 thread gets, and a
    /// nice 10 thread about a tenth. Linux applies it to the one thread
    /// `gettid` names.
    const fn nice(class: Class) -> i32 {
        match class {
            Class::Normal => 0,
            Class::Below => 10,
            Class::Background => 19,
        }
    }

    pub(super) fn set(class: Class) -> io::Result<()> {
        setpriority_process(Some(gettid()), nice(class)).map_err(io::Error::from)
    }

    pub(super) fn current() -> Class {
        match getpriority_process(Some(gettid())) {
            Ok(niceness) if niceness >= nice(Class::Background) => Class::Background,
            Ok(niceness) if niceness >= nice(Class::Below) => Class::Below,
            _ => Class::Normal,
        }
    }
}

#[cfg(not(any(target_os = "macos", target_os = "linux")))]
mod platform {
    use std::io;

    use super::Class;

    pub(super) fn set(class: Class) -> io::Result<()> {
        if class == Class::Normal {
            Ok(())
        } else {
            Err(io::Error::from(io::ErrorKind::Unsupported))
        }
    }

    pub(super) fn current() -> Class {
        Class::Normal
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn class_of_a_spawned_thread() -> Class {
        spawn(thread::Builder::new(), current_thread_class)
            .unwrap()
            .join()
            .unwrap()
    }

    #[test]
    fn a_thread_starts_its_threads_in_its_class_and_only_it_moves() {
        thread::spawn(|| {
            assert_eq!(current_thread_class(), Class::Normal);
            assert_eq!(class_of_a_spawned_thread(), Class::Normal);
            lower_current_thread().unwrap();
            assert!(current_thread_is_lowered());
            assert_eq!(class_of_a_spawned_thread(), Class::Background);
            assert_eq!(
                thread::scope(|scope| spawn_scoped(scope, current_thread_class).join().unwrap()),
                Class::Background
            );
        })
        .join()
        .unwrap();
        assert_eq!(current_thread_class(), Class::Normal);
    }

    #[test]
    fn a_thread_below_delivery_is_not_a_lowered_thread_and_carries_its_class() {
        thread::spawn(|| {
            set_current_thread_class(Class::Below).unwrap();
            assert_eq!(current_thread_class(), Class::Below);
            assert!(!current_thread_is_lowered());
            assert_eq!(class_of_a_spawned_thread(), Class::Below);
        })
        .join()
        .unwrap();
    }

    #[test]
    fn a_background_thread_moves_up_to_normal_where_the_platform_allows_it() {
        let can_rise = lowered_threads_can_rise();
        assert!(can_rise || cfg!(target_os = "linux"));
        thread::spawn(move || {
            lower_current_thread().unwrap();
            match set_current_thread_class(Class::Normal) {
                Ok(()) => {
                    assert!(can_rise);
                    assert_eq!(current_thread_class(), Class::Normal);
                }
                Err(error) => {
                    assert!(!can_rise, "{error}");
                    assert_eq!(current_thread_class(), Class::Background, "{error}");
                }
            }
        })
        .join()
        .unwrap();
    }
}
