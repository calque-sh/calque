use std::{
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc, Mutex,
    },
    time::Duration,
};

use canonical_log::{
    LogError, SnapshotBuildId, SnapshotBuildOwnerId, SnapshotBuildRetention,
    SnapshotBuildRetentionLease,
};
use thiserror::Error;
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;

/// Owns renewal and exact release of one durable snapshot-build retention
/// capability. Caller cancellation and renewal uncertainty cancel the child
/// work token immediately, but renewal itself continues until the lifecycle
/// owner explicitly joins this guard after publication or cleanup. This keeps
/// retention authoritative while cancellation cleanup is still unresolved.
pub struct SnapshotBuildLeaseGuard<P: SnapshotBuildRetention + Send + 'static> {
    provider: Arc<Mutex<P>>,
    lease: Arc<Mutex<SnapshotBuildRetentionLease>>,
    cancellation: CancellationToken,
    stop: CancellationToken,
    healthy: Arc<AtomicBool>,
    renewal_task: Option<JoinHandle<Result<(), LogError>>>,
}

impl<P: SnapshotBuildRetention + Send + 'static> SnapshotBuildLeaseGuard<P> {
    pub async fn acquire(
        provider: Arc<Mutex<P>>,
        build_id: SnapshotBuildId,
        owner_id: SnapshotBuildOwnerId,
        ttl: Duration,
        renewal_interval: Duration,
        parent_cancellation: &CancellationToken,
    ) -> Result<Self, SnapshotBuildLeaseGuardError> {
        let operation_timeout = provider
            .lock()
            .map_err(|_| SnapshotBuildLeaseGuardError::ProviderMutexPoisoned)?
            .maximum_retention_operation_duration();
        validate_timing(ttl, renewal_interval, operation_timeout)?;
        let acquisition_provider = Arc::clone(&provider);
        let initial = tokio::task::spawn_blocking(move || {
            let started = std::time::Instant::now();
            let mut provider = acquisition_provider
                .lock()
                .map_err(|_| SnapshotBuildLeaseGuardError::ProviderMutexPoisoned)?;
            let lease = provider
                .acquire_snapshot_build_retention(build_id, owner_id, ttl)
                .map_err(SnapshotBuildLeaseGuardError::CanonicalLog)?;
            if started.elapsed() > operation_timeout {
                let _ = provider.release_snapshot_build_retention(&lease);
                return Err(SnapshotBuildLeaseGuardError::OperationTimedOut);
            }
            Ok(lease)
        })
        .await
        .map_err(SnapshotBuildLeaseGuardError::RenewalTask)??;
        let lease = Arc::new(Mutex::new(initial));
        let cancellation = parent_cancellation.child_token();
        let stop = CancellationToken::new();
        let healthy = Arc::new(AtomicBool::new(true));
        let renewal_task = tokio::spawn(renewal_loop(
            Arc::clone(&provider),
            Arc::clone(&lease),
            ttl,
            renewal_interval,
            cancellation.clone(),
            stop.clone(),
            Arc::clone(&healthy),
            operation_timeout,
        ));
        Ok(Self {
            provider,
            lease,
            cancellation,
            stop,
            healthy,
            renewal_task: Some(renewal_task),
        })
    }

    pub fn cancellation_token(&self) -> CancellationToken {
        self.cancellation.clone()
    }

    pub fn is_healthy(&self) -> bool {
        self.healthy.load(Ordering::Acquire)
    }

    pub fn current_lease(
        &self,
    ) -> Result<SnapshotBuildRetentionLease, SnapshotBuildLeaseGuardError> {
        self.lease
            .lock()
            .map(|lease| lease.clone())
            .map_err(|_| SnapshotBuildLeaseGuardError::LeaseMutexPoisoned)
    }

    /// Stops renewal and releases the exact latest capability. This must run
    /// only after publication or complete terminal cleanup. A renewal failure
    /// is returned before release because the stale capability is no longer
    /// authoritative; provider expiry performs the safe eventual cleanup.
    pub async fn release(mut self) -> Result<(), SnapshotBuildLeaseGuardError> {
        self.stop.cancel();
        let task = self
            .renewal_task
            .take()
            .ok_or(SnapshotBuildLeaseGuardError::RenewalTaskMissing)?;
        match task.await? {
            Ok(()) => {}
            Err(error) => return Err(SnapshotBuildLeaseGuardError::RenewalFailed(error)),
        }
        if !self.is_healthy() {
            return Err(SnapshotBuildLeaseGuardError::LeaseUnhealthy);
        }
        let lease = self.current_lease()?;
        let provider = Arc::clone(&self.provider);
        tokio::task::spawn_blocking(move || {
            provider
                .lock()
                .map_err(|_| SnapshotBuildLeaseGuardError::ProviderMutexPoisoned)?
                .release_snapshot_build_retention(&lease)
                .map_err(SnapshotBuildLeaseGuardError::CanonicalLog)
        })
        .await
        .map_err(SnapshotBuildLeaseGuardError::RenewalTask)??;
        Ok(())
    }

    /// Joins the renewal owner without releasing its last capability. This is
    /// required after renewal uncertainty and publication uncertainty: expiry,
    /// not a possibly stale release, is the safe authority boundary.
    pub async fn stop_without_release(mut self) -> Result<(), SnapshotBuildLeaseGuardError> {
        self.stop.cancel();
        let task = self
            .renewal_task
            .take()
            .ok_or(SnapshotBuildLeaseGuardError::RenewalTaskMissing)?;
        match task.await? {
            Ok(()) if self.is_healthy() => Ok(()),
            Ok(()) => Err(SnapshotBuildLeaseGuardError::LeaseUnhealthy),
            Err(error) => Err(SnapshotBuildLeaseGuardError::RenewalFailed(error)),
        }
    }
}

impl<P: SnapshotBuildRetention + Send + 'static> Drop for SnapshotBuildLeaseGuard<P> {
    fn drop(&mut self) {
        self.stop.cancel();
    }
}

#[allow(clippy::too_many_arguments)]
async fn renewal_loop<P: SnapshotBuildRetention + Send + 'static>(
    provider: Arc<Mutex<P>>,
    lease: Arc<Mutex<SnapshotBuildRetentionLease>>,
    ttl: Duration,
    interval: Duration,
    work_cancellation: CancellationToken,
    stop: CancellationToken,
    healthy: Arc<AtomicBool>,
    operation_timeout: Duration,
) -> Result<(), LogError> {
    loop {
        tokio::select! {
            () = stop.cancelled() => return Ok(()),
            () = tokio::time::sleep(interval) => {}
        }
        let provider = Arc::clone(&provider);
        let renewal_lease = Arc::clone(&lease);
        let expires_at = lease
            .lock()
            .map_err(|_| LogError::PostgresUnavailable)?
            .expires_at_unix_millis();
        let remaining = expires_at.saturating_sub(current_time_millis()?);
        let safe_remaining = u64::try_from(remaining)
            .ok()
            .and_then(|millis| millis.checked_div(2))
            .filter(|millis| *millis > 0)
            .map(Duration::from_millis)
            .ok_or(LogError::PostgresUnavailable)?;
        if operation_timeout > (ttl / 3).min(safe_remaining) {
            healthy.store(false, Ordering::Release);
            work_cancellation.cancel();
            return Err(LogError::PostgresUnavailable);
        }
        let renewal = tokio::task::spawn_blocking(move || {
            let started = std::time::Instant::now();
            let current = renewal_lease
                .lock()
                .map_err(|_| LogError::PostgresUnavailable)?
                .clone();
            let mut provider = provider.lock().map_err(|_| LogError::PostgresUnavailable)?;
            let renewed = provider.renew_snapshot_build_retention(&current, ttl)?;
            Ok((renewed, started.elapsed() > operation_timeout))
        });
        let renewed = renewal.await.map_err(|_| LogError::PostgresUnavailable)?;
        match renewed {
            Ok((renewed, exceeded_operation_timeout)) => {
                // A slow successful renewal is still the provider's newest
                // authority. Record it before declaring this owner unhealthy,
                // and leave it to expire so unresolved build state remains
                // retained for the full authoritative grace period.
                *lease.lock().map_err(|_| LogError::PostgresUnavailable)? = renewed;
                if exceeded_operation_timeout {
                    healthy.store(false, Ordering::Release);
                    work_cancellation.cancel();
                    return Err(LogError::PostgresUnavailable);
                }
            }
            Err(error) => {
                healthy.store(false, Ordering::Release);
                work_cancellation.cancel();
                return Err(error);
            }
        }
    }
}

fn current_time_millis() -> Result<i64, LogError> {
    let millis = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_err(|_| LogError::PostgresUnavailable)?
        .as_millis();
    i64::try_from(millis).map_err(|_| LogError::PostgresUnavailable)
}

fn validate_timing(
    ttl: Duration,
    renewal_interval: Duration,
    operation_timeout: Duration,
) -> Result<(), SnapshotBuildLeaseGuardError> {
    if ttl.is_zero()
        || renewal_interval.is_zero()
        || renewal_interval >= ttl
        || renewal_interval > ttl / 2
        || operation_timeout.is_zero()
        || operation_timeout > ttl / 3
    {
        Err(SnapshotBuildLeaseGuardError::InvalidTiming)
    } else {
        Ok(())
    }
}

#[derive(Debug, Error)]
pub enum SnapshotBuildLeaseGuardError {
    #[error("snapshot build lease TTL and renewal interval are invalid")]
    InvalidTiming,
    #[error("snapshot build retention provider mutex is poisoned")]
    ProviderMutexPoisoned,
    #[error("snapshot build lease mutex is poisoned")]
    LeaseMutexPoisoned,
    #[error("snapshot build lease renewal task is missing")]
    RenewalTaskMissing,
    #[error("snapshot build lease renewal task failed: {0}")]
    RenewalTask(#[from] tokio::task::JoinError),
    #[error("snapshot build retention lease became unhealthy")]
    LeaseUnhealthy,
    #[error("snapshot build retention provider operation exceeded its safety deadline")]
    OperationTimedOut,
    #[error("snapshot build retention lease renewal failed: {0}")]
    RenewalFailed(LogError),
    #[error(transparent)]
    CanonicalLog(#[from] LogError),
}

#[cfg(test)]
mod tests {
    use super::*;
    use canonical_log::FileCanonicalLog;
    use sync_types::{SourceEpoch, SourceId};

    struct FailRenew {
        log: FileCanonicalLog,
    }

    struct SlowRenew {
        log: FileCanonicalLog,
        delay: Duration,
    }

    /// The file log, under its own deadline, with each acquire `delay` slower.
    struct SlowAcquire {
        log: FileCanonicalLog,
        delay: Duration,
    }

    impl SnapshotBuildRetention for SlowAcquire {
        fn maximum_retention_operation_duration(&self) -> Duration {
            self.log.maximum_retention_operation_duration()
        }

        fn acquire_snapshot_build_retention(
            &mut self,
            build_id: SnapshotBuildId,
            owner_id: SnapshotBuildOwnerId,
            ttl: Duration,
        ) -> Result<SnapshotBuildRetentionLease, LogError> {
            std::thread::sleep(self.delay);
            self.log
                .acquire_snapshot_build_retention(build_id, owner_id, ttl)
        }

        fn renew_snapshot_build_retention(
            &mut self,
            lease: &SnapshotBuildRetentionLease,
            ttl: Duration,
        ) -> Result<SnapshotBuildRetentionLease, LogError> {
            self.log.renew_snapshot_build_retention(lease, ttl)
        }

        fn release_snapshot_build_retention(
            &mut self,
            lease: &SnapshotBuildRetentionLease,
        ) -> Result<(), LogError> {
            self.log.release_snapshot_build_retention(lease)
        }
    }

    impl SnapshotBuildRetention for FailRenew {
        fn maximum_retention_operation_duration(&self) -> Duration {
            self.log.maximum_retention_operation_duration()
        }

        fn acquire_snapshot_build_retention(
            &mut self,
            build_id: SnapshotBuildId,
            owner_id: SnapshotBuildOwnerId,
            ttl: Duration,
        ) -> Result<SnapshotBuildRetentionLease, LogError> {
            self.log
                .acquire_snapshot_build_retention(build_id, owner_id, ttl)
        }

        fn renew_snapshot_build_retention(
            &mut self,
            _lease: &SnapshotBuildRetentionLease,
            _ttl: Duration,
        ) -> Result<SnapshotBuildRetentionLease, LogError> {
            Err(LogError::PostgresUnavailable)
        }

        fn release_snapshot_build_retention(
            &mut self,
            lease: &SnapshotBuildRetentionLease,
        ) -> Result<(), LogError> {
            self.log.release_snapshot_build_retention(lease)
        }
    }

    impl SnapshotBuildRetention for SlowRenew {
        /// Short enough for a test to outlive a TTL of four deadlines, and long
        /// enough for the file log's acquire, which waits on fsync.
        fn maximum_retention_operation_duration(&self) -> Duration {
            Duration::from_millis(500)
        }

        fn acquire_snapshot_build_retention(
            &mut self,
            build_id: SnapshotBuildId,
            owner_id: SnapshotBuildOwnerId,
            ttl: Duration,
        ) -> Result<SnapshotBuildRetentionLease, LogError> {
            self.log
                .acquire_snapshot_build_retention(build_id, owner_id, ttl)
        }

        fn renew_snapshot_build_retention(
            &mut self,
            lease: &SnapshotBuildRetentionLease,
            ttl: Duration,
        ) -> Result<SnapshotBuildRetentionLease, LogError> {
            std::thread::sleep(self.delay);
            self.log.renew_snapshot_build_retention(lease, ttl)
        }

        fn release_snapshot_build_retention(
            &mut self,
            lease: &SnapshotBuildRetentionLease,
        ) -> Result<(), LogError> {
            self.log.release_snapshot_build_retention(lease)
        }
    }

    fn file_log(root: &std::path::Path) -> FileCanonicalLog {
        FileCanonicalLog::open(
            root,
            SourceId::new("source-lease-guard").unwrap(),
            SourceEpoch::new("epoch-lease-guard").unwrap(),
        )
        .unwrap()
    }

    #[tokio::test]
    async fn renews_and_releases_the_latest_exact_capability() {
        let root = tempfile::tempdir().unwrap();
        let provider = Arc::new(Mutex::new(file_log(root.path())));
        let deadline = provider
            .lock()
            .unwrap()
            .maximum_retention_operation_duration();
        let interval = Duration::from_millis(50);
        // `ttl / 3` has to admit the file log's 3 s operation deadline.
        let guard = SnapshotBuildLeaseGuard::acquire(
            Arc::clone(&provider),
            SnapshotBuildId::new("build-lease-guard").unwrap(),
            SnapshotBuildOwnerId::new("owner-lease-guard").unwrap(),
            Duration::from_secs(10),
            interval,
            &CancellationToken::new(),
        )
        .await
        .unwrap();
        let initial_expiry = guard.current_lease().unwrap().expires_at_unix_millis();
        // The first renewal starts one interval in and may take the deadline.
        tokio::time::timeout(interval + deadline, async {
            loop {
                if guard.current_lease().unwrap().expires_at_unix_millis() > initial_expiry {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .unwrap();
        guard.release().await.unwrap();
        provider
            .lock()
            .unwrap()
            .acquire_snapshot_build_retention(
                SnapshotBuildId::new("build-lease-guard").unwrap(),
                SnapshotBuildOwnerId::new("owner-lease-guard-2").unwrap(),
                Duration::from_secs(1),
            )
            .unwrap();
    }

    /// A local build retains for 30 s and renews every 5 s. That timing admits
    /// the file log's deadline, and the deadline covers a 400 ms acquire.
    #[tokio::test]
    async fn a_slow_acquire_is_admitted_under_the_file_logs_deadline_at_local_build_timing() {
        let root = tempfile::tempdir().unwrap();
        let provider = Arc::new(Mutex::new(SlowAcquire {
            log: file_log(root.path()),
            delay: Duration::from_millis(400),
        }));
        let guard = SnapshotBuildLeaseGuard::acquire(
            provider,
            SnapshotBuildId::new("build-lease-slow-acquire").unwrap(),
            SnapshotBuildOwnerId::new("owner-lease-slow-acquire").unwrap(),
            Duration::from_secs(30),
            Duration::from_secs(5),
            &CancellationToken::new(),
        )
        .await
        .unwrap();
        assert!(guard.is_healthy());
        guard.release().await.unwrap();
    }

    #[tokio::test]
    async fn renewal_uncertainty_cancels_work_and_never_releases_stale_authority() {
        let root = tempfile::tempdir().unwrap();
        let provider = Arc::new(Mutex::new(FailRenew {
            log: file_log(root.path()),
        }));
        // `ttl / 3` has to admit the file log's 3 s operation deadline.
        let guard = SnapshotBuildLeaseGuard::acquire(
            provider,
            SnapshotBuildId::new("build-lease-fail").unwrap(),
            SnapshotBuildOwnerId::new("owner-lease-fail").unwrap(),
            Duration::from_secs(10),
            Duration::from_millis(50),
            &CancellationToken::new(),
        )
        .await
        .unwrap();
        let cancellation = guard.cancellation_token();
        tokio::time::timeout(Duration::from_secs(1), cancellation.cancelled())
            .await
            .unwrap();
        assert!(!guard.is_healthy());
        assert!(matches!(
            guard.release().await,
            Err(SnapshotBuildLeaseGuardError::RenewalFailed(
                LogError::PostgresUnavailable
            ))
        ));
    }

    #[tokio::test]
    async fn stalled_renewal_cancels_before_authoritative_expiry() {
        // The renewal overruns SlowRenew's 500 ms deadline by a fifth, and the
        // TTL is four deadlines.
        let ttl = Duration::from_secs(2);
        let root = tempfile::tempdir().unwrap();
        let provider = Arc::new(Mutex::new(SlowRenew {
            log: file_log(root.path()),
            delay: Duration::from_millis(600),
        }));
        let build_id = SnapshotBuildId::new("build-lease-stalled").unwrap();
        let guard = SnapshotBuildLeaseGuard::acquire(
            Arc::clone(&provider),
            build_id.clone(),
            SnapshotBuildOwnerId::new("owner-lease-stalled").unwrap(),
            ttl,
            Duration::from_millis(50),
            &CancellationToken::new(),
        )
        .await
        .unwrap();
        let initial = guard.current_lease().unwrap();
        tokio::time::timeout(ttl, guard.cancellation_token().cancelled())
            .await
            .unwrap();
        assert!(current_time_millis().unwrap() < initial.expires_at_unix_millis());
        assert!(!guard.is_healthy());
        let authoritative = guard.current_lease().unwrap();
        assert!(matches!(
            provider.lock().unwrap().acquire_snapshot_build_retention(
                build_id.clone(),
                SnapshotBuildOwnerId::new("owner-lease-stalled-2").unwrap(),
                ttl,
            ),
            Err(LogError::SnapshotBuildRetentionLeaseConflict)
        ));
        assert!(matches!(
            guard.stop_without_release().await,
            Err(SnapshotBuildLeaseGuardError::RenewalFailed(
                LogError::PostgresUnavailable
            ))
        ));
        let remaining = authoritative
            .expires_at_unix_millis()
            .saturating_sub(current_time_millis().unwrap());
        tokio::time::sleep(Duration::from_millis(
            u64::try_from(remaining).unwrap_or(0).saturating_add(20),
        ))
        .await;
        provider
            .lock()
            .unwrap()
            .acquire_snapshot_build_retention(
                build_id,
                SnapshotBuildOwnerId::new("owner-lease-stalled-2").unwrap(),
                ttl,
            )
            .unwrap();
    }
}
