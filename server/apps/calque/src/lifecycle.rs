//! The startup and shutdown steps `calque dev` and `calque serve` share: how
//! a stop is requested and bounded, what stopping the runtime after a failed
//! startup adds to the startup error, and which remedy a failure ends with.

use std::{future::Future, path::Path, time::Duration};

use calque_engine::{LocalSourceError, LocalSourceRuntime};

/// What the shared steps need from a command's error type.
pub(crate) trait CommandError: From<LocalSourceError> + Send + Sized + 'static {
    fn blocking_task(message: String) -> Self;
    fn source(&self) -> Option<&LocalSourceError>;
    fn into_source(self) -> Result<LocalSourceError, Self>;
    /// Whether this is the error of a runtime whose CDC loop stopped before
    /// readiness.
    fn runtime_stopped(&self) -> bool;
    /// Whether this failure names its own remedy, so no other is appended.
    fn names_its_remedy(&self) -> bool;
    /// Whether this is a refusal only `calque destroy` gets past.
    fn requires_destroying_state(&self) -> bool;
    fn runtime_cleanup(startup: Self, cleanup: Self) -> Self;
    fn into_runtime_cleanup(self) -> Result<(Self, Self), Self>;
    fn state_must_be_destroyed(refusal: Self, state_dir: &Path, database_url_env: &str) -> Self;
    fn source_changed(refusal: Self, state_dir: &Path, database_url_env: &str) -> Self;
    fn publication_drifted(refusal: Self, state_dir: &Path, database_url_env: &str) -> Self;
}

/// Whether this is the engine's refusal of a drifted publication.
fn drifted<E: CommandError>(error: &E) -> bool {
    error
        .source()
        .is_some_and(LocalSourceError::publication_drifted)
}

pub(crate) async fn run_blocking<T, E>(
    operation: impl FnOnce() -> Result<T, E> + Send + 'static,
) -> Result<T, E>
where
    T: Send + 'static,
    E: CommandError,
{
    tokio::task::spawn_blocking(operation)
        .await
        .map_err(|error| E::blocking_task(error.to_string()))?
}

pub(crate) async fn shutdown_runtime<E: CommandError>(
    runtime: LocalSourceRuntime,
) -> Result<(), E> {
    run_blocking(move || runtime.shutdown().map_err(E::from)).await
}

pub(crate) async fn shutdown_after<E: CommandError>(runtime: LocalSourceRuntime, startup: E) -> E {
    stop_after(move || runtime.shutdown(), startup).await
}

/// Stops what a failed startup started and returns the startup error, with
/// the stop's own failure beside it when stopping failed too. When the CDC
/// loop had already stopped before readiness, stopping the runtime returns
/// the same cause the stopped runtime's error reports, so that cause is not
/// added a second time.
pub(crate) async fn stop_after<E: CommandError>(
    stop: impl FnOnce() -> Result<(), LocalSourceError> + Send + 'static,
    startup: E,
) -> E {
    let cleanup = match run_blocking(move || stop().map_err(E::from)).await {
        Ok(()) => return startup,
        Err(cleanup) => cleanup,
    };
    if !startup.runtime_stopped() {
        return E::runtime_cleanup(startup, cleanup);
    }
    match cleanup.into_source() {
        Ok(error) => match added_by_the_stop(error) {
            Some(cleanup) => E::runtime_cleanup(startup, E::from(cleanup)),
            None => startup,
        },
        Err(cleanup) => E::runtime_cleanup(startup, cleanup),
    }
}

/// The part of an error from stopping a runtime, whose CDC loop had already
/// stopped, that its health text does not already report.
///
/// - The error the loop stopped on, and `IngestThreadPanicked` for the loop's
///   own panic (`cdc-runtime-panic` in the health text), add nothing.
/// - A failed session cleanup, and `IngestThreadPanickedAfterLoop` for a
///   panic while the thread closed its session, are the stop's own failures
///   and are returned.
/// - `SessionCleanupFailedAfter` carries the loop's cause beside the cleanup
///   failure; only the cleanup failure is returned.
fn added_by_the_stop(error: LocalSourceError) -> Option<LocalSourceError> {
    match error {
        LocalSourceError::SessionCleanupFailedAfter(_) => {
            Some(LocalSourceError::SessionCleanupFailed)
        }
        error @ (LocalSourceError::SessionCleanupFailed
        | LocalSourceError::IngestThreadPanickedAfterLoop) => Some(error),
        _ => None,
    }
}

/// Appends, once, the remedy the engine leaves the command to name: `calque
/// destroy` for a refusal only it gets past, restoring a drifted publication,
/// or reconnecting a changed source. When stopping the runtime also failed, a
/// startup failure that needs destroying takes the remedy; otherwise the
/// stop's refusal does, unless the startup failure names its own.
pub(crate) fn with_state_remedy<E: CommandError>(
    error: E,
    state_dir: &Path,
    database_url_env: &str,
) -> E {
    let error = match error.into_runtime_cleanup() {
        Ok((startup, cleanup)) => {
            return if startup.requires_destroying_state() {
                E::runtime_cleanup(
                    E::state_must_be_destroyed(startup, state_dir, database_url_env),
                    cleanup,
                )
            } else if startup.names_its_remedy() {
                E::runtime_cleanup(startup, cleanup)
            } else if cleanup.requires_destroying_state() {
                E::state_must_be_destroyed(
                    E::runtime_cleanup(startup, cleanup),
                    state_dir,
                    database_url_env,
                )
            } else if drifted(&cleanup) {
                E::publication_drifted(
                    E::runtime_cleanup(startup, cleanup),
                    state_dir,
                    database_url_env,
                )
            } else {
                E::runtime_cleanup(startup, cleanup)
            };
        }
        Err(error) => error,
    };
    if matches!(
        error.source(),
        Some(LocalSourceError::SourceIdentityChanged)
    ) {
        E::source_changed(error, state_dir, database_url_env)
    } else if drifted(&error) {
        E::publication_drifted(error, state_dir, database_url_env)
    } else if error.requires_destroying_state() {
        E::state_must_be_destroyed(error, state_dir, database_url_env)
    } else {
        error
    }
}

/// Waits for whichever OS shutdown signal arrives first. `docker stop` and
/// most orchestrators send SIGTERM rather than the SIGINT `ctrl_c()` observes;
/// without SIGTERM the process is killed after the grace period and leaves its
/// source lease held until it expires, which the next start waits out.
#[cfg(unix)]
pub(crate) async fn shutdown_signal() {
    use tokio::signal::unix::{signal, SignalKind};

    let ctrl_c = tokio::signal::ctrl_c();
    let mut terminate = match signal(SignalKind::terminate()) {
        Ok(terminate) => terminate,
        Err(error) => {
            tracing::warn!(%error, "SIGTERM cannot be observed; only ctrl-c stops calque cleanly");
            let _ = ctrl_c.await;
            return;
        }
    };
    tokio::select! {
        _ = ctrl_c => tracing::info!("stopping: received SIGINT"),
        _ = terminate.recv() => tracing::info!("stopping: received SIGTERM"),
    }
}

#[cfg(not(unix))]
pub(crate) async fn shutdown_signal() {
    let _ = tokio::signal::ctrl_c().await;
    tracing::info!("stopping: received ctrl-c");
}

/// How long a stop may take once it is requested. One that took longer than a
/// source lease's 10 s TTL would release the lease no sooner than letting it
/// expire does, and `docker stop` and the launchers send SIGKILL after 10 s.
const STOP_BOUND: Duration = Duration::from_secs(10);

/// Resolves when `requested` does, and bounds the stop that follows. A
/// PostgreSQL that stops answering can hold a query, and the stop with it,
/// forever; a process still running `STOP_BOUND` after the request exits
/// without finishing the stop.
pub(crate) async fn bounded_stop(requested: impl Future<Output = ()>) {
    requested.await;
    let watchdog = std::thread::Builder::new()
        .name("calque-stop-bound".to_owned())
        .spawn(|| {
            use std::io::Write as _;

            std::thread::sleep(STOP_BOUND);
            let _ = writeln!(
                std::io::stderr(),
                "calque: the stop did not finish within {} s, so calque exited without finishing it; its source lease expires on its own",
                STOP_BOUND.as_secs()
            );
            std::process::exit(1);
        });
    if let Err(error) = watchdog {
        tracing::warn!(%error, "the stop runs without its {} s bound", STOP_BOUND.as_secs());
    }
}
