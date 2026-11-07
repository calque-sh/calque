use serde::{Deserialize, Serialize};
use snapshot_coordinator::{
    sample_snapshot_pressure, SnapshotPressureError, SnapshotPressureEvaluation,
    SnapshotPressureLevel, SnapshotPressurePolicy, SnapshotPressureReason, SnapshotPressureSample,
    SnapshotPressureTarget,
};
use std::{
    future::{pending, Future},
    pin::Pin,
    sync::atomic::{AtomicBool, Ordering},
    time::Duration,
};
use thiserror::Error;
use tokio::{
    sync::{mpsc, oneshot, watch},
    task::JoinHandle,
    time::{self, Instant, MissedTickBehavior},
};
use tokio_postgres::Client;
use tokio_util::sync::CancellationToken;

/// Configuration for the build-wide duration and forward-progress limits.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SnapshotWatchdogConfig {
    max_total_duration: Duration,
    no_progress_timeout: Duration,
}

impl SnapshotWatchdogConfig {
    pub fn new(
        max_total_duration: Duration,
        no_progress_timeout: Duration,
    ) -> Result<Self, SnapshotWatchdogError> {
        if max_total_duration.is_zero() || no_progress_timeout.is_zero() {
            return Err(SnapshotWatchdogError::InvalidConfiguration);
        }
        Ok(Self {
            max_total_duration,
            no_progress_timeout,
        })
    }

    pub(crate) const fn no_progress_timeout(self) -> Duration {
        self.no_progress_timeout
    }
}

/// Boxed async sampler boundary used by the production PostgreSQL adapter and
/// deterministic unit-test fakes. Failures must already be reduced to a stable,
/// non-secret category before crossing this boundary.
pub(crate) trait SnapshotPressureSampler: Send + 'static {
    fn sample(
        &mut self,
    ) -> Pin<
        Box<
            dyn Future<Output = Result<SnapshotPressureSample, SnapshotPressureSamplingFailure>>
                + Send
                + '_,
        >,
    >;
}

/// Production pressure sampler. It exclusively owns the observation client so
/// a stalled or cancelled sample cannot interfere with build-control queries.
pub(crate) struct PostgresSnapshotPressureSampler {
    client: Client,
    target: SnapshotPressureTarget,
}

impl PostgresSnapshotPressureSampler {
    pub fn new(client: Client, target: SnapshotPressureTarget) -> Self {
        Self { client, target }
    }
}

impl SnapshotPressureSampler for PostgresSnapshotPressureSampler {
    fn sample(
        &mut self,
    ) -> Pin<
        Box<
            dyn Future<Output = Result<SnapshotPressureSample, SnapshotPressureSamplingFailure>>
                + Send
                + '_,
        >,
    > {
        Box::pin(async {
            sample_snapshot_pressure(&self.client, &self.target)
                .await
                .map_err(SnapshotPressureSamplingFailure::from)
        })
    }
}

/// Optional periodic pressure monitor attached to a watchdog.
pub(crate) struct SnapshotPressureMonitor {
    interval: Duration,
    sample_timeout: Duration,
    policy: SnapshotPressurePolicy,
    sampler: Box<dyn SnapshotPressureSampler>,
}

impl SnapshotPressureMonitor {
    pub fn new(
        interval: Duration,
        sample_timeout: Duration,
        policy: SnapshotPressurePolicy,
        sampler: impl SnapshotPressureSampler,
    ) -> Result<Self, SnapshotWatchdogError> {
        if interval.is_zero() || sample_timeout.is_zero() {
            return Err(SnapshotWatchdogError::InvalidConfiguration);
        }
        Ok(Self {
            interval,
            sample_timeout,
            policy,
            sampler: Box::new(sampler),
        })
    }

    pub fn postgres(
        interval: Duration,
        sample_timeout: Duration,
        client: Client,
        target: SnapshotPressureTarget,
        policy: SnapshotPressurePolicy,
    ) -> Result<Self, SnapshotWatchdogError> {
        Self::new(
            interval,
            sample_timeout,
            policy,
            PostgresSnapshotPressureSampler::new(client, target),
        )
    }
}

/// Stable, payload-free pressure sampling failure categories.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum SnapshotPressureSamplingFailure {
    SourceUnavailable,
    TemporarySlotMissing,
    UnexpectedSlotMetadata,
    TemporarySlotInactive,
    MetricUnavailable,
    UnknownWalStatus,
    BackendMissing,
    InvalidConfiguration,
    SampleTimeout,
    SamplerStopped,
}

impl From<SnapshotPressureError> for SnapshotPressureSamplingFailure {
    fn from(error: SnapshotPressureError) -> Self {
        match error {
            SnapshotPressureError::Postgres(_) => Self::SourceUnavailable,
            SnapshotPressureError::TemporarySlotMissing => Self::TemporarySlotMissing,
            SnapshotPressureError::UnexpectedSlotMetadata => Self::UnexpectedSlotMetadata,
            SnapshotPressureError::TemporarySlotInactive => Self::TemporarySlotInactive,
            SnapshotPressureError::MissingMetric(_)
            | SnapshotPressureError::InvalidMetric(_)
            | SnapshotPressureError::DuplicateBackendActivity => Self::MetricUnavailable,
            SnapshotPressureError::UnknownWalStatus => Self::UnknownWalStatus,
            SnapshotPressureError::BackendMissing { .. } => Self::BackendMissing,
            SnapshotPressureError::InvalidSlotName
            | SnapshotPressureError::InvalidBackendPid
            | SnapshotPressureError::InvalidThresholds
            | SnapshotPressureError::DurationOverflow
            | SnapshotPressureError::TooManyBackendPids => Self::InvalidConfiguration,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum SnapshotPressureLimitKind {
    WalPressure,
    CatalogXidAge,
    CopyTransactionAge,
}

/// Exact stable terminal reason retained by the watchdog. The latest pressure
/// observation carries the complete sanitized hard-limit evaluation.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "code", rename_all = "SCREAMING_SNAKE_CASE")]
pub enum SnapshotWatchdogFailureReason {
    MaximumBuildDuration,
    NoProgress,
    PressureHardLimit {
        kind: SnapshotPressureLimitKind,
    },
    PressureSamplingUncertain {
        failure: SnapshotPressureSamplingFailure,
    },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub(crate) enum SnapshotWatchdogStatus {
    Running,
    Stopped,
    ChildCancelled,
    Failed,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub(crate) struct SnapshotPressureObservation {
    pub revision: u64,
    pub sample: SnapshotPressureSample,
    pub evaluation: SnapshotPressureEvaluation,
}

/// Latest in-memory watchdog state. This is exposed through a watch channel;
/// no observation is logged by this module.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub(crate) struct SnapshotWatchdogState {
    pub status: SnapshotWatchdogStatus,
    pub heartbeat_count: u64,
    pub latest_pressure: Option<SnapshotPressureObservation>,
    pub failure_reason: Option<SnapshotWatchdogFailureReason>,
}

impl SnapshotWatchdogState {
    fn running() -> Self {
        Self {
            status: SnapshotWatchdogStatus::Running,
            heartbeat_count: 0,
            latest_pressure: None,
            failure_reason: None,
        }
    }
}

/// Stoppable and joinable lifecycle watchdog for one snapshot build.
pub(crate) struct SnapshotLifecycleWatchdog {
    heartbeat_tx: watch::Sender<u64>,
    state_rx: watch::Receiver<SnapshotWatchdogState>,
    pressure_attach_tx: mpsc::Sender<PressureAttachRequest>,
    pressure_detach_tx: mpsc::Sender<oneshot::Sender<()>>,
    pressure_claimed: AtomicBool,
    pressure_detached: AtomicBool,
    stop: CancellationToken,
    task: Option<JoinHandle<SnapshotWatchdogState>>,
}

impl SnapshotLifecycleWatchdog {
    pub fn spawn(
        config: SnapshotWatchdogConfig,
        child_cancellation: CancellationToken,
        pressure: Option<SnapshotPressureMonitor>,
    ) -> Self {
        let (heartbeat_tx, heartbeat_rx) = watch::channel(0_u64);
        let (pressure_attach_tx, pressure_attach_rx) = mpsc::channel(1);
        let (pressure_detach_tx, pressure_detach_rx) = mpsc::channel(1);
        let pressure_claimed = AtomicBool::new(pressure.is_some());
        let admitted_at = Instant::now();
        let initial_state = SnapshotWatchdogState::running();
        let (state_tx, state_rx) = watch::channel(initial_state.clone());
        let stop = CancellationToken::new();
        let task_stop = stop.clone();
        let task = tokio::spawn(run_watchdog(WatchdogTask {
            config,
            child_cancellation,
            pressure,
            heartbeat_rx,
            pressure_attach_rx,
            pressure_detach_rx,
            state_tx,
            stop: task_stop,
            state: initial_state,
            admitted_at,
        }));
        Self {
            heartbeat_tx,
            state_rx,
            pressure_attach_tx,
            pressure_detach_tx,
            pressure_claimed,
            pressure_detached: AtomicBool::new(false),
            stop,
            task: Some(task),
        }
    }

    /// Records application-visible forward progress and resets the no-progress
    /// deadline. Repeated heartbeats are retained as a saturating counter.
    pub fn heartbeat(&self) {
        self.heartbeat_tx.send_modify(|count| {
            *count = count.saturating_add(1);
        });
    }

    pub fn latest(&self) -> SnapshotWatchdogState {
        self.state_rx.borrow().clone()
    }

    /// One-shot deferred pressure attachment. This allows build duration and
    /// no-progress clocks to begin at admission while PostgreSQL sampling is
    /// armed only after the temporary slot and COPY sessions exist.
    pub(crate) async fn attach_pressure(
        &self,
        monitor: SnapshotPressureMonitor,
    ) -> Result<(), SnapshotWatchdogError> {
        if self.latest().status != SnapshotWatchdogStatus::Running {
            return Err(SnapshotWatchdogError::PressureAttachTooLate);
        }
        if self
            .pressure_claimed
            .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
            .is_err()
        {
            return Err(SnapshotWatchdogError::PressureAlreadyAttached);
        }

        let (accepted_tx, accepted_rx) = oneshot::channel();
        self.pressure_attach_tx
            .send(PressureAttachRequest {
                monitor,
                accepted_tx,
            })
            .await
            .map_err(|_| SnapshotWatchdogError::PressureAttachTooLate)?;
        accepted_rx
            .await
            .map_err(|_| SnapshotWatchdogError::PressureAttachTooLate)
    }

    /// Stops and joins the attached pressure sampler while leaving the build
    /// duration and no-progress watchdog active. Snapshot orchestration calls
    /// this before it closes the temporary slot.
    pub(crate) async fn detach_pressure(&self) -> Result<(), SnapshotWatchdogError> {
        if self.latest().status != SnapshotWatchdogStatus::Running {
            return Err(SnapshotWatchdogError::PressureDetachTooLate);
        }
        if !self.pressure_claimed.load(Ordering::Acquire) {
            return Err(SnapshotWatchdogError::PressureNotAttached);
        }
        if self
            .pressure_detached
            .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
            .is_err()
        {
            return Err(SnapshotWatchdogError::PressureAlreadyDetached);
        }
        let (detached_tx, detached_rx) = oneshot::channel();
        self.pressure_detach_tx
            .send(detached_tx)
            .await
            .map_err(|_| SnapshotWatchdogError::PressureDetachTooLate)?;
        detached_rx
            .await
            .map_err(|_| SnapshotWatchdogError::PressureDetachTooLate)
    }

    /// Stops observation without cancelling child work, then joins every task.
    pub async fn stop(mut self) -> Result<SnapshotWatchdogState, SnapshotWatchdogError> {
        self.stop.cancel();
        self.join_task().await
    }

    async fn join_task(&mut self) -> Result<SnapshotWatchdogState, SnapshotWatchdogError> {
        self.task
            .take()
            .ok_or(SnapshotWatchdogError::AlreadyJoined)?
            .await
            .map_err(SnapshotWatchdogError::Join)
    }
}

impl Drop for SnapshotLifecycleWatchdog {
    fn drop(&mut self) {
        self.stop.cancel();
    }
}

#[derive(Debug, Error)]
pub enum SnapshotWatchdogError {
    #[error("snapshot watchdog configuration is invalid")]
    InvalidConfiguration,
    #[error("snapshot watchdog was already joined")]
    AlreadyJoined,
    #[error("snapshot pressure monitor was already attached")]
    PressureAlreadyAttached,
    #[error("snapshot pressure monitor cannot be attached after watchdog termination")]
    PressureAttachTooLate,
    #[error("snapshot pressure monitor was not attached")]
    PressureNotAttached,
    #[error("snapshot pressure monitor was already detached")]
    PressureAlreadyDetached,
    #[error("snapshot pressure monitor cannot be detached after watchdog termination")]
    PressureDetachTooLate,
    #[error("snapshot watchdog task failed")]
    Join(#[source] tokio::task::JoinError),
}

struct PressureAttachRequest {
    monitor: SnapshotPressureMonitor,
    accepted_tx: oneshot::Sender<()>,
}

struct WatchdogTask {
    config: SnapshotWatchdogConfig,
    child_cancellation: CancellationToken,
    pressure: Option<SnapshotPressureMonitor>,
    heartbeat_rx: watch::Receiver<u64>,
    pressure_attach_rx: mpsc::Receiver<PressureAttachRequest>,
    pressure_detach_rx: mpsc::Receiver<oneshot::Sender<()>>,
    state_tx: watch::Sender<SnapshotWatchdogState>,
    stop: CancellationToken,
    state: SnapshotWatchdogState,
    admitted_at: Instant,
}

async fn run_watchdog(task: WatchdogTask) -> SnapshotWatchdogState {
    let WatchdogTask {
        config,
        child_cancellation,
        pressure,
        mut heartbeat_rx,
        mut pressure_attach_rx,
        mut pressure_detach_rx,
        state_tx,
        stop,
        mut state,
        admitted_at,
    } = task;
    let total_sleep = time::sleep_until(admitted_at + config.max_total_duration);
    let no_progress_sleep = time::sleep_until(admitted_at + config.no_progress_timeout);
    tokio::pin!(total_sleep);
    tokio::pin!(no_progress_sleep);

    let pressure_stop = CancellationToken::new();
    let (mut pressure_rx, mut pressure_task) = if let Some(pressure) = pressure {
        let (rx, policy, task) =
            start_pressure_sampler(pressure, pressure_stop.clone(), child_cancellation.clone());
        (Some((rx, policy)), Some(task))
    } else {
        (None, None)
    };
    let mut pressure_attach_open = true;

    loop {
        tokio::select! {
            biased;
            _ = stop.cancelled() => {
                finish(&state_tx, &mut state, SnapshotWatchdogStatus::Stopped, None);
                break;
            }
            _ = child_cancellation.cancelled() => {
                finish(
                    &state_tx,
                    &mut state,
                    SnapshotWatchdogStatus::ChildCancelled,
                    None,
                );
                break;
            }
            changed = heartbeat_rx.changed() => {
                if changed.is_err() {
                    fail(
                        &child_cancellation,
                        &state_tx,
                        &mut state,
                        SnapshotWatchdogFailureReason::NoProgress,
                    );
                    break;
                }
                state.heartbeat_count = *heartbeat_rx.borrow_and_update();
                no_progress_sleep
                    .as_mut()
                    .reset(Instant::now() + config.no_progress_timeout);
                let _ = state_tx.send_replace(state.clone());
            }
            _ = &mut total_sleep => {
                fail(
                    &child_cancellation,
                    &state_tx,
                    &mut state,
                    SnapshotWatchdogFailureReason::MaximumBuildDuration,
                );
                break;
            }
            _ = &mut no_progress_sleep => {
                fail(
                    &child_cancellation,
                    &state_tx,
                    &mut state,
                    SnapshotWatchdogFailureReason::NoProgress,
                );
                break;
            }
            attachment = pressure_attach_rx.recv(), if pressure_rx.is_none() && pressure_attach_open => {
                let Some(attachment) = attachment else {
                    pressure_attach_open = false;
                    continue;
                };
                let (rx, policy, task) = start_pressure_sampler(
                    attachment.monitor,
                    pressure_stop.clone(),
                    child_cancellation.clone(),
                );
                pressure_rx = Some((rx, policy));
                pressure_task = Some(task);
                let _ = attachment.accepted_tx.send(());
            }
            detachment = pressure_detach_rx.recv(), if pressure_rx.is_some() => {
                let Some(detached_tx) = detachment else {
                    continue;
                };
                pressure_stop.cancel();
                if let Some(task) = pressure_task.take() {
                    let _ = task.await;
                }
                pressure_rx = None;
                let _ = detached_tx.send(());
            }
            pressure_result = next_pressure(&mut pressure_rx) => {
                let Some(result) = pressure_result else {
                    fail(
                        &child_cancellation,
                        &state_tx,
                        &mut state,
                        SnapshotWatchdogFailureReason::PressureSamplingUncertain {
                            failure: SnapshotPressureSamplingFailure::SamplerStopped,
                        },
                    );
                    break;
                };
                let policy = pressure_rx
                    .as_ref()
                    .expect("pressure result requires a configured monitor")
                    .1;
                match result {
                    Ok(sample) => {
                        let evaluation = policy.evaluate(&sample);
                        let revision = state
                            .latest_pressure
                            .as_ref()
                            .map_or(1, |observation| observation.revision.saturating_add(1));
                        state.latest_pressure = Some(SnapshotPressureObservation {
                            revision,
                            sample,
                            evaluation: evaluation.clone(),
                        });
                        let _ = state_tx.send_replace(state.clone());
                        if evaluation.level == SnapshotPressureLevel::HardLimit {
                            fail(
                                &child_cancellation,
                                &state_tx,
                                &mut state,
                                SnapshotWatchdogFailureReason::PressureHardLimit {
                                    kind: primary_hard_limit(&evaluation),
                                },
                            );
                            break;
                        }
                    }
                    Err(failure) => {
                        fail(
                            &child_cancellation,
                            &state_tx,
                            &mut state,
                            SnapshotWatchdogFailureReason::PressureSamplingUncertain { failure },
                        );
                        break;
                    }
                }
            }
        }
    }

    pressure_stop.cancel();
    if let Some(pressure_task) = pressure_task {
        let _ = pressure_task.await;
    }
    state
}

fn start_pressure_sampler(
    pressure: SnapshotPressureMonitor,
    stop: CancellationToken,
    child_cancellation: CancellationToken,
) -> (
    mpsc::Receiver<Result<SnapshotPressureSample, SnapshotPressureSamplingFailure>>,
    SnapshotPressurePolicy,
    JoinHandle<()>,
) {
    let (tx, rx) = mpsc::channel(1);
    let policy = pressure.policy;
    let task = tokio::spawn(run_pressure_sampler(pressure, tx, stop, child_cancellation));
    (rx, policy, task)
}

async fn run_pressure_sampler(
    mut monitor: SnapshotPressureMonitor,
    tx: mpsc::Sender<Result<SnapshotPressureSample, SnapshotPressureSamplingFailure>>,
    stop: CancellationToken,
    child_cancellation: CancellationToken,
) {
    let mut interval = time::interval(monitor.interval);
    interval.set_missed_tick_behavior(MissedTickBehavior::Skip);
    loop {
        tokio::select! {
            biased;
            _ = stop.cancelled() => return,
            _ = child_cancellation.cancelled() => return,
            _ = interval.tick() => {}
        }

        let result = tokio::select! {
            biased;
            _ = stop.cancelled() => return,
            _ = child_cancellation.cancelled() => return,
            result = time::timeout(monitor.sample_timeout, monitor.sampler.sample()) => {
                result.unwrap_or(Err(SnapshotPressureSamplingFailure::SampleTimeout))
            }
        };
        let terminal = result.is_err();
        let sent = tokio::select! {
            biased;
            _ = stop.cancelled() => return,
            _ = child_cancellation.cancelled() => return,
            sent = tx.send(result) => sent,
        };
        if sent.is_err() || terminal {
            return;
        }
    }
}

async fn next_pressure(
    pressure: &mut Option<(
        mpsc::Receiver<Result<SnapshotPressureSample, SnapshotPressureSamplingFailure>>,
        SnapshotPressurePolicy,
    )>,
) -> Option<Result<SnapshotPressureSample, SnapshotPressureSamplingFailure>> {
    match pressure {
        Some((rx, _)) => rx.recv().await,
        None => pending().await,
    }
}

fn fail(
    child_cancellation: &CancellationToken,
    state_tx: &watch::Sender<SnapshotWatchdogState>,
    state: &mut SnapshotWatchdogState,
    reason: SnapshotWatchdogFailureReason,
) {
    child_cancellation.cancel();
    finish(
        state_tx,
        state,
        SnapshotWatchdogStatus::Failed,
        Some(reason),
    );
}

fn finish(
    state_tx: &watch::Sender<SnapshotWatchdogState>,
    state: &mut SnapshotWatchdogState,
    status: SnapshotWatchdogStatus,
    reason: Option<SnapshotWatchdogFailureReason>,
) {
    state.status = status;
    state.failure_reason = reason;
    let _ = state_tx.send_replace(state.clone());
}

fn primary_hard_limit(evaluation: &SnapshotPressureEvaluation) -> SnapshotPressureLimitKind {
    if evaluation.reasons.iter().any(|reason| {
        reason.level() == SnapshotPressureLevel::HardLimit
            && matches!(
                reason,
                SnapshotPressureReason::RetainedWalBytes { .. }
                    | SnapshotPressureReason::RemainingSafeWalBytes { .. }
                    | SnapshotPressureReason::WalStatus { .. }
            )
    }) {
        SnapshotPressureLimitKind::WalPressure
    } else if evaluation.reasons.iter().any(|reason| {
        reason.level() == SnapshotPressureLevel::HardLimit
            && matches!(reason, SnapshotPressureReason::CatalogXidAge { .. })
    }) {
        SnapshotPressureLimitKind::CatalogXidAge
    } else {
        SnapshotPressureLimitKind::CopyTransactionAge
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use snapshot_coordinator::{
        SnapshotPressureThresholds, SnapshotRemainingWalThresholds, SnapshotWalStatus,
    };
    use std::{
        collections::VecDeque,
        sync::{Arc, Mutex},
    };

    #[derive(Clone)]
    struct FakeSampler {
        results:
            Arc<Mutex<VecDeque<Result<SnapshotPressureSample, SnapshotPressureSamplingFailure>>>>,
    }

    impl FakeSampler {
        fn new(
            results: impl IntoIterator<
                Item = Result<SnapshotPressureSample, SnapshotPressureSamplingFailure>,
            >,
        ) -> Self {
            Self {
                results: Arc::new(Mutex::new(results.into_iter().collect())),
            }
        }
    }

    impl SnapshotPressureSampler for FakeSampler {
        fn sample(
            &mut self,
        ) -> Pin<
            Box<
                dyn Future<Output = Result<SnapshotPressureSample, SnapshotPressureSamplingFailure>>
                    + Send
                    + '_,
            >,
        > {
            let result = self
                .results
                .lock()
                .unwrap()
                .pop_front()
                .unwrap_or_else(|| Ok(healthy_sample()));
            Box::pin(async move { result })
        }
    }

    struct PendingSampler;

    impl SnapshotPressureSampler for PendingSampler {
        fn sample(
            &mut self,
        ) -> Pin<
            Box<
                dyn Future<Output = Result<SnapshotPressureSample, SnapshotPressureSamplingFailure>>
                    + Send
                    + '_,
            >,
        > {
            Box::pin(pending())
        }
    }

    fn config(max_total_secs: u64, no_progress_secs: u64) -> SnapshotWatchdogConfig {
        SnapshotWatchdogConfig::new(
            Duration::from_secs(max_total_secs),
            Duration::from_secs(no_progress_secs),
        )
        .unwrap()
    }

    fn pressure_policy() -> SnapshotPressurePolicy {
        SnapshotPressurePolicy::new(
            SnapshotPressureThresholds::new(1_000, 2_000).unwrap(),
            SnapshotPressureThresholds::new(100, 200).unwrap(),
            Duration::from_secs(5),
            Duration::from_secs(10),
            SnapshotRemainingWalThresholds::new(500, 100).unwrap(),
        )
        .unwrap()
    }

    fn healthy_sample() -> SnapshotPressureSample {
        SnapshotPressureSample {
            retained_wal_bytes: 999,
            catalog_xid_age: 99,
            oldest_copy_or_export_transaction_millis: Some(4_999),
            wal_status: SnapshotWalStatus::Reserved,
            safe_wal_size_bytes: Some(501),
            exporter_backend_pid: 10,
            copy_backend_count: 1,
        }
    }

    fn monitor(sampler: FakeSampler) -> Result<SnapshotPressureMonitor, SnapshotWatchdogError> {
        SnapshotPressureMonitor::new(
            Duration::from_secs(1),
            Duration::from_millis(500),
            pressure_policy(),
            sampler,
        )
    }

    #[tokio::test(start_paused = true)]
    async fn maximum_total_duration_cancels_and_joins() {
        let child = CancellationToken::new();
        let mut watchdog = SnapshotLifecycleWatchdog::spawn(config(10, 20), child.clone(), None);

        time::advance(Duration::from_secs(10)).await;
        let state = watchdog.join_task().await.unwrap();
        assert!(child.is_cancelled());
        assert_eq!(state.status, SnapshotWatchdogStatus::Failed);
        assert_eq!(
            state.failure_reason,
            Some(SnapshotWatchdogFailureReason::MaximumBuildDuration)
        );
    }

    #[tokio::test(start_paused = true)]
    async fn no_progress_timeout_cancels_and_joins() {
        let child = CancellationToken::new();
        let mut watchdog = SnapshotLifecycleWatchdog::spawn(config(60, 10), child.clone(), None);

        time::advance(Duration::from_secs(10)).await;
        let state = watchdog.join_task().await.unwrap();
        assert!(child.is_cancelled());
        assert_eq!(
            state.failure_reason,
            Some(SnapshotWatchdogFailureReason::NoProgress)
        );
    }

    #[tokio::test(start_paused = true)]
    async fn heartbeat_resets_only_the_no_progress_deadline() {
        let child = CancellationToken::new();
        let mut watchdog = SnapshotLifecycleWatchdog::spawn(config(60, 10), child.clone(), None);

        time::advance(Duration::from_secs(9)).await;
        watchdog.heartbeat();
        tokio::task::yield_now().await;
        time::advance(Duration::from_secs(9)).await;
        assert!(!child.is_cancelled());
        assert_eq!(watchdog.latest().heartbeat_count, 1);
        time::advance(Duration::from_secs(1)).await;

        let state = watchdog.join_task().await.unwrap();
        assert_eq!(
            state.failure_reason,
            Some(SnapshotWatchdogFailureReason::NoProgress)
        );
    }

    #[tokio::test(start_paused = true)]
    async fn stop_is_joinable_and_does_not_cancel_child_work() {
        let child = CancellationToken::new();
        let watchdog = SnapshotLifecycleWatchdog::spawn(config(60, 10), child.clone(), None);

        let state = watchdog.stop().await.unwrap();
        assert!(!child.is_cancelled());
        assert_eq!(state.status, SnapshotWatchdogStatus::Stopped);
        assert_eq!(state.failure_reason, None);
    }

    #[tokio::test(start_paused = true)]
    async fn warning_observation_is_exposed_without_cancelling_work() {
        let child = CancellationToken::new();
        let mut warning = healthy_sample();
        warning.retained_wal_bytes = 1_000;
        let watchdog = SnapshotLifecycleWatchdog::spawn(config(60, 10), child.clone(), None);
        let mut state_rx = watchdog.state_rx.clone();
        watchdog
            .attach_pressure(monitor(FakeSampler::new([Ok(warning)])).unwrap())
            .await
            .unwrap();

        state_rx.changed().await.unwrap();
        let state = state_rx.borrow().clone();
        assert_eq!(
            state.latest_pressure.as_ref().unwrap().evaluation.level,
            SnapshotPressureLevel::Warning
        );
        assert!(!child.is_cancelled());

        let stopped = watchdog.stop().await.unwrap();
        assert_eq!(stopped.status, SnapshotWatchdogStatus::Stopped);
    }

    #[tokio::test(start_paused = true)]
    async fn pressure_attachment_is_one_shot() {
        let child = CancellationToken::new();
        let watchdog = SnapshotLifecycleWatchdog::spawn(config(60, 10), child.clone(), None);

        watchdog
            .attach_pressure(monitor(FakeSampler::new([Ok(healthy_sample())])).unwrap())
            .await
            .unwrap();
        assert!(matches!(
            watchdog
                .attach_pressure(monitor(FakeSampler::new([Ok(healthy_sample())])).unwrap())
                .await,
            Err(SnapshotWatchdogError::PressureAlreadyAttached)
        ));

        watchdog.stop().await.unwrap();
        assert!(!child.is_cancelled());
    }

    #[tokio::test(start_paused = true)]
    async fn pressure_detach_joins_sampler_and_is_one_shot() {
        let child = CancellationToken::new();
        let watchdog = SnapshotLifecycleWatchdog::spawn(config(60, 10), child.clone(), None);

        assert!(matches!(
            watchdog.detach_pressure().await,
            Err(SnapshotWatchdogError::PressureNotAttached)
        ));
        watchdog
            .attach_pressure(monitor(FakeSampler::new([Ok(healthy_sample())])).unwrap())
            .await
            .unwrap();
        watchdog.detach_pressure().await.unwrap();
        assert!(matches!(
            watchdog.detach_pressure().await,
            Err(SnapshotWatchdogError::PressureAlreadyDetached)
        ));
        assert_eq!(watchdog.latest().status, SnapshotWatchdogStatus::Running);

        let state = watchdog.stop().await.unwrap();
        assert_eq!(state.status, SnapshotWatchdogStatus::Stopped);
        assert!(!child.is_cancelled());
    }

    #[tokio::test(start_paused = true)]
    async fn pressure_attachment_after_terminal_state_is_rejected() {
        let child = CancellationToken::new();
        let mut watchdog = SnapshotLifecycleWatchdog::spawn(config(60, 10), child.clone(), None);
        let mut state_rx = watchdog.state_rx.clone();
        child.cancel();
        state_rx.changed().await.unwrap();
        assert_eq!(
            state_rx.borrow().status,
            SnapshotWatchdogStatus::ChildCancelled
        );

        assert!(matches!(
            watchdog
                .attach_pressure(monitor(FakeSampler::new([Ok(healthy_sample())])).unwrap())
                .await,
            Err(SnapshotWatchdogError::PressureAttachTooLate)
        ));
        assert_eq!(
            watchdog.join_task().await.unwrap().status,
            SnapshotWatchdogStatus::ChildCancelled
        );
    }

    #[tokio::test(start_paused = true)]
    async fn hard_pressure_retains_evaluation_and_cancels_work() {
        let child = CancellationToken::new();
        let mut hard = healthy_sample();
        hard.catalog_xid_age = 200;
        let mut watchdog = SnapshotLifecycleWatchdog::spawn(
            config(60, 10),
            child.clone(),
            Some(monitor(FakeSampler::new([Ok(hard)])).unwrap()),
        );

        let state = watchdog.join_task().await.unwrap();
        assert!(child.is_cancelled());
        assert_eq!(
            state.failure_reason,
            Some(SnapshotWatchdogFailureReason::PressureHardLimit {
                kind: SnapshotPressureLimitKind::CatalogXidAge,
            })
        );
        assert_eq!(
            state.latest_pressure.unwrap().evaluation.level,
            SnapshotPressureLevel::HardLimit
        );
    }

    #[tokio::test(start_paused = true)]
    async fn sampler_uncertainty_fails_closed_with_stable_reason() {
        let child = CancellationToken::new();
        let mut watchdog = SnapshotLifecycleWatchdog::spawn(
            config(60, 10),
            child.clone(),
            Some(
                monitor(FakeSampler::new([Err(
                    SnapshotPressureSamplingFailure::MetricUnavailable,
                )]))
                .unwrap(),
            ),
        );

        let state = watchdog.join_task().await.unwrap();
        assert!(child.is_cancelled());
        assert_eq!(
            state.failure_reason,
            Some(SnapshotWatchdogFailureReason::PressureSamplingUncertain {
                failure: SnapshotPressureSamplingFailure::MetricUnavailable,
            })
        );
    }

    #[tokio::test(start_paused = true)]
    async fn stalled_sampler_times_out_and_remains_joinable() {
        let child = CancellationToken::new();
        let pressure = SnapshotPressureMonitor::new(
            Duration::from_secs(1),
            Duration::from_millis(500),
            pressure_policy(),
            PendingSampler,
        )
        .unwrap();
        let mut watchdog =
            SnapshotLifecycleWatchdog::spawn(config(60, 10), child.clone(), Some(pressure));

        let state = watchdog.join_task().await.unwrap();
        assert!(child.is_cancelled());
        assert_eq!(
            state.failure_reason,
            Some(SnapshotWatchdogFailureReason::PressureSamplingUncertain {
                failure: SnapshotPressureSamplingFailure::SampleTimeout,
            })
        );
    }
}
