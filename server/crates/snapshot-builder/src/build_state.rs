use std::{
    collections::{BTreeMap, BTreeSet},
    time::Duration,
};

use direct_columnar::Value;
use futures_util::{stream::FuturesUnordered, StreamExt, TryStreamExt};
use snapshot_coordinator::{CoordinatorError, SnapshotCopyCompletion, SnapshotCopyWorker};
use source_control::{SchemaRegistry, SchemaRegistryError};
use sync_types::TableId;
use thiserror::Error;
use tokio::sync::{mpsc, oneshot};
use tokio_postgres::binary_copy::BinaryCopyOutStream;
use tokio_util::sync::CancellationToken;

use crate::{
    copy_loader::validate_limits, direct_copy_loader::decode_direct_columnar_row, CopyLoadError,
    CopyLoadLimits, CopyLoadStats, SnapshotBuildPlan, SnapshotPlanError, SnapshotRollForwardError,
    SnapshotTablePlan,
};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct BoundedCopyLimits {
    pub table: CopyLoadLimits,
    pub max_workers: usize,
    /// Maximum aggregate retained decoded batch bytes across all producers.
    pub max_inflight_batch_bytes: u64,
}

pub struct SnapshotCopyWorkload<'a> {
    pub(crate) worker: SnapshotCopyWorker<'a>,
    pub(crate) table_indices: Vec<usize>,
}

impl<'a> SnapshotCopyWorkload<'a> {
    pub fn new(worker: SnapshotCopyWorker<'a>, table_indices: Vec<usize>) -> Self {
        Self {
            worker,
            table_indices,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BoundedCopyProgress {
    pub table_id: TableId,
    pub table: CopyLoadStats,
    pub tables_total: usize,
    pub tables_completed: usize,
    pub active_workers: usize,
    pub queued_tables: usize,
    pub total_rows: u64,
    pub total_decoded_value_bytes: u64,
    pub total_committed_batches: u64,
}

pub const PUBLICATION_GENERATION_V1: u64 = 1;
pub const SOURCE_WIDE_SHARD_PROFILE_HASH: &str =
    "sha256:afbb4ffe2274f634e4c0f9bb6acea218bf45676d23d388d05843f533cba4892b";
pub const NO_INDEX_PROFILE_HASH: &str =
    "sha256:5710a9a8eecddb6d17e8f28b54ad49214dbbcdb79fb03febe69c0974eea09c17";
pub const TYPE_CODEC_GENERATION_V1: u64 = 1;
pub const COLLATION_GENERATION_V1: u64 = 1;

pub(crate) enum CopyEvent {
    Batch {
        table_index: usize,
        rows: Vec<Vec<Value>>,
        stats: CopyLoadStats,
        acknowledgement: oneshot::Sender<()>,
    },
    Finished {
        table_index: usize,
        stats: CopyLoadStats,
        acknowledgement: oneshot::Sender<()>,
    },
}

/// One imported worker and the planned tables it copies, in order.
pub(crate) type CopyWorkload<'a> = (SnapshotCopyWorker<'a>, Vec<(usize, SnapshotTablePlan)>);

/// Runs every worker's COPY and hands each decoded batch, and each table's
/// end, to `apply` on this task. Producers decode at most one unacknowledged
/// batch each, so retained memory stays bounded by the workers' batch limit.
/// The first failure cancels the remaining workers, which roll back.
pub(crate) async fn drive_copy_workers<F>(
    workloads: Vec<CopyWorkload<'_>>,
    limits: CopyLoadLimits,
    cleanup_timeout: Duration,
    cancellation: &CancellationToken,
    mut apply: F,
) -> Result<Vec<SnapshotCopyCompletion>, SnapshotBuildStateError>
where
    F: FnMut(CopyEvent, usize) -> Result<(), SnapshotBuildStateError>,
{
    let operation_cancellation = cancellation.child_token();
    let worker_count = workloads.len();
    let (sender, mut receiver) = mpsc::channel(worker_count);
    let mut workers = FuturesUnordered::new();
    for (worker, tables) in workloads {
        workers.push(run_copy_workload(
            worker,
            tables,
            limits,
            cleanup_timeout,
            sender.clone(),
            operation_cancellation.clone(),
        ));
    }
    drop(sender);

    let mut first_error = None;
    let mut completions = Vec::with_capacity(worker_count);
    while !workers.is_empty() {
        tokio::select! {
            () = operation_cancellation.cancelled(), if first_error.is_none() => {
                first_error = Some(SnapshotBuildStateError::Copy(CopyLoadError::Cancelled));
            }
            event = receiver.recv() => {
                let Some(event) = event else {
                    if first_error.is_none() {
                        first_error = Some(SnapshotBuildStateError::ParallelCopyChannelClosed);
                        operation_cancellation.cancel();
                    }
                    continue;
                };
                if first_error.is_some() {
                    drop(event);
                    continue;
                }
                if let Err(error) = apply(event, workers.len()) {
                    first_error = Some(error);
                    operation_cancellation.cancel();
                }
            }
            result = workers.next() => {
                match result.expect("copy worker set is nonempty") {
                    Ok(completion) => completions.push(completion),
                    Err(error) => {
                        if first_error.is_none() {
                            first_error = Some(error);
                        }
                        operation_cancellation.cancel();
                    }
                }
            }
        }
    }
    match first_error {
        Some(error) => Err(error),
        None => Ok(completions),
    }
}

/// The admission checks both build forms make before a bounded copy: every
/// planned table is assigned to exactly one worker, and the workers' admitted
/// batch memory fits the in-flight limit.
pub(crate) fn validate_copy_assignment(
    plan: &SnapshotBuildPlan,
    workloads: &[SnapshotCopyWorkload<'_>],
    limits: BoundedCopyLimits,
    nothing_loaded: bool,
) -> Result<(), SnapshotBuildStateError> {
    validate_limits(limits.table)?;
    if workloads.is_empty()
        || limits.max_workers == 0
        || workloads.len() > limits.max_workers
        || !nothing_loaded
    {
        return Err(SnapshotBuildStateError::InvalidParallelCopyLimits);
    }
    let admitted_batch_bytes = u64::try_from(workloads.len())
        .ok()
        .and_then(|workers| workers.checked_mul(limits.table.max_batch_bytes))
        .ok_or(SnapshotBuildStateError::InvalidParallelCopyLimits)?;
    if admitted_batch_bytes > limits.max_inflight_batch_bytes {
        return Err(SnapshotBuildStateError::InvalidParallelCopyLimits);
    }
    let mut table_indices = BTreeSet::new();
    for workload in workloads {
        if workload.table_indices.is_empty() {
            return Err(SnapshotBuildStateError::EmptyCopyWorkload);
        }
        for &index in &workload.table_indices {
            if index >= plan.tables.len() {
                return Err(SnapshotBuildStateError::UnknownTableIndex(index));
            }
            if !table_indices.insert(index) {
                return Err(SnapshotBuildStateError::DuplicateTableIndex(index));
            }
        }
    }
    if table_indices.len() != plan.tables.len() {
        return Err(SnapshotBuildStateError::ParallelCopyIncomplete);
    }
    Ok(())
}

pub(crate) fn copy_progress(
    tables_total: usize,
    loaded: &BTreeMap<TableId, CopyLoadStats>,
    in_progress: &BTreeMap<TableId, CopyLoadStats>,
    table_id: TableId,
    table: CopyLoadStats,
    active_workers: usize,
) -> Result<BoundedCopyProgress, SnapshotBuildStateError> {
    let totals = loaded.values().chain(in_progress.values()).try_fold(
        (0_u64, 0_u64, 0_u64),
        |totals, stats| {
            Ok::<_, SnapshotBuildStateError>((
                totals
                    .0
                    .checked_add(stats.rows)
                    .ok_or(SnapshotBuildStateError::CopyProgressOverflow)?,
                totals
                    .1
                    .checked_add(stats.decoded_value_bytes)
                    .ok_or(SnapshotBuildStateError::CopyProgressOverflow)?,
                totals
                    .2
                    .checked_add(stats.committed_batches)
                    .ok_or(SnapshotBuildStateError::CopyProgressOverflow)?,
            ))
        },
    )?;
    let active_tables = in_progress.len();
    let queued_tables = tables_total
        .checked_sub(loaded.len())
        .and_then(|remaining| remaining.checked_sub(active_tables))
        .ok_or(SnapshotBuildStateError::CopyProgressOverflow)?;
    Ok(BoundedCopyProgress {
        table_id,
        table,
        tables_total,
        tables_completed: loaded.len(),
        active_workers,
        queued_tables,
        total_rows: totals.0,
        total_decoded_value_bytes: totals.1,
        total_committed_batches: totals.2,
    })
}

/// Re-reads the source catalog after the cut commit was observed. Any DDL or
/// publication drift committed at or before the cut invalidates the old
/// schema generation instead of producing a stale artifact.
pub(crate) async fn revalidate_registry_after_cut(
    registry: &SchemaRegistry,
    endpoint: postgres_connect::Endpoint,
    timeout: Duration,
    cancellation: &CancellationToken,
) -> Result<(), SnapshotBuildStateError> {
    let operation_timeout = timeout / 2;
    if operation_timeout.is_zero() {
        return Err(SnapshotBuildStateError::RegistryRevalidationTimeout);
    }
    let endpoint = revalidation_endpoint(endpoint, operation_timeout);
    let revalidation_registry = registry.clone();
    let revalidation = tokio::task::spawn_blocking(move || {
        let mut client = endpoint
            .connect()
            .map_err(SnapshotBuildStateError::RegistryRevalidationConnection)?;
        revalidation_registry
            .verify_live(&mut client)
            .map_err(SnapshotBuildStateError::Registry)
    });
    let started = std::time::Instant::now();
    revalidation
        .await
        .map_err(SnapshotBuildStateError::RegistryRevalidationTask)??;
    if started.elapsed() > timeout {
        return Err(SnapshotBuildStateError::RegistryRevalidationTimeout);
    }
    if cancellation.is_cancelled() {
        return Err(SnapshotBuildStateError::RegistryRevalidationCancelled);
    }
    Ok(())
}

/// `endpoint` as the revalidation connects: every wait ends at
/// `operation_timeout`, and a server that stops answering fails the connection
/// a reply margin later instead of holding the build.
fn revalidation_endpoint(
    mut endpoint: postgres_connect::Endpoint,
    operation_timeout: Duration,
) -> postgres_connect::Endpoint {
    endpoint.config_mut().connect_timeout(operation_timeout);
    endpoint.bound_replies(operation_timeout);
    endpoint.append_options(&format!(
        "-c statement_timeout={} -c lock_timeout={}",
        operation_timeout.as_millis().min(i32::MAX as u128),
        operation_timeout.as_millis().min(i32::MAX as u128),
    ));
    endpoint
}

async fn run_copy_workload(
    mut worker: SnapshotCopyWorker<'_>,
    tables: Vec<(usize, SnapshotTablePlan)>,
    limits: CopyLoadLimits,
    cleanup_timeout: Duration,
    sender: mpsc::Sender<CopyEvent>,
    cancellation: CancellationToken,
) -> Result<SnapshotCopyCompletion, SnapshotBuildStateError> {
    for (table_index, table) in tables {
        if cancellation.is_cancelled() {
            abort_copy_worker(worker, cleanup_timeout).await?;
            return Err(SnapshotBuildStateError::Copy(CopyLoadError::Cancelled));
        }
        let copy = match worker
            .copy_table(table.table_id.clone(), table.copy_sql(), table.copy_read())
            .await
        {
            Ok(copy) => copy,
            Err(error) => {
                abort_copy_worker(worker, cleanup_timeout).await?;
                return Err(error.into());
            }
        };
        let stream = match copy.into_stream_for(
            worker.exported_snapshot(),
            &table.table_id,
            table.copy_sql(),
        ) {
            Ok(stream) => stream,
            Err(error) => {
                abort_copy_worker(worker, cleanup_timeout).await?;
                return Err(error.into());
            }
        };
        if let Err(error) =
            decode_copy_batches(table_index, &table, stream, limits, &sender, &cancellation).await
        {
            abort_copy_worker(worker, cleanup_timeout).await?;
            return Err(error.into());
        }
    }
    worker.commit().await.map_err(Into::into)
}

async fn abort_copy_worker(
    worker: SnapshotCopyWorker<'_>,
    cleanup_timeout: Duration,
) -> Result<(), SnapshotBuildStateError> {
    tokio::time::timeout(cleanup_timeout, worker.abort())
        .await
        .map_err(|_| SnapshotBuildStateError::CopyCleanupTimedOut)??;
    Ok(())
}

async fn decode_copy_batches(
    table_index: usize,
    table: &SnapshotTablePlan,
    stream: tokio_postgres::CopyOutStream,
    limits: CopyLoadLimits,
    sender: &mpsc::Sender<CopyEvent>,
    cancellation: &CancellationToken,
) -> Result<CopyLoadStats, CopyLoadError> {
    validate_limits(limits)?;
    let types = table.copy_types();
    let rows = BinaryCopyOutStream::new(stream, &types);
    futures_util::pin_mut!(rows);
    let mut batch = Vec::with_capacity(limits.batch_rows);
    let mut batch_bytes = 0_u64;
    let mut stats = CopyLoadStats {
        rows: 0,
        decoded_value_bytes: 0,
        committed_batches: 0,
        peak_batch_bytes: 0,
    };

    loop {
        let row = tokio::select! {
            () = cancellation.cancelled() => return Err(CopyLoadError::Cancelled),
            result = rows.try_next() => result?,
        };
        let Some(row) = row else {
            break;
        };
        let next_rows = stats
            .rows
            .checked_add(1)
            .ok_or(CopyLoadError::RowOverflow)?;
        if next_rows > limits.max_rows {
            return Err(CopyLoadError::RowLimitExceeded {
                maximum: limits.max_rows,
            });
        }
        let (values, decoded_bytes) = decode_direct_columnar_row(table, &row, limits)?;
        let value_vector_bytes = u64::try_from(values.len())
            .ok()
            .and_then(|count| count.checked_mul(std::mem::size_of::<Value>() as u64))
            .ok_or(CopyLoadError::DecodedByteOverflow)?;
        let retained_row_bytes = decoded_bytes
            .checked_add(value_vector_bytes)
            .ok_or(CopyLoadError::DecodedByteOverflow)?;
        if retained_row_bytes > limits.max_batch_bytes {
            return Err(CopyLoadError::BatchByteLimitExceeded {
                maximum: limits.max_batch_bytes,
            });
        }
        let next_batch_bytes = batch_bytes
            .checked_add(retained_row_bytes)
            .ok_or(CopyLoadError::DecodedByteOverflow)?;
        if !batch.is_empty() && next_batch_bytes > limits.max_batch_bytes {
            send_copy_batch(
                table_index,
                std::mem::take(&mut batch),
                &mut stats,
                sender,
                cancellation,
            )
            .await?;
            batch_bytes = 0;
        }
        let next_decoded_bytes = stats
            .decoded_value_bytes
            .checked_add(decoded_bytes)
            .ok_or(CopyLoadError::DecodedByteOverflow)?;
        if next_decoded_bytes > limits.max_decoded_value_bytes {
            return Err(CopyLoadError::DecodedByteLimitExceeded {
                maximum: limits.max_decoded_value_bytes,
            });
        }
        stats.rows = next_rows;
        stats.decoded_value_bytes = next_decoded_bytes;
        batch_bytes = batch_bytes
            .checked_add(retained_row_bytes)
            .ok_or(CopyLoadError::DecodedByteOverflow)?;
        stats.peak_batch_bytes = stats.peak_batch_bytes.max(batch_bytes);
        batch.push(values);
        if batch.len() == limits.batch_rows {
            send_copy_batch(
                table_index,
                std::mem::take(&mut batch),
                &mut stats,
                sender,
                cancellation,
            )
            .await?;
            batch_bytes = 0;
        }
    }
    if !batch.is_empty() {
        send_copy_batch(table_index, batch, &mut stats, sender, cancellation).await?;
    }
    let (acknowledgement, acknowledged) = oneshot::channel();
    send_copy_event(
        sender,
        CopyEvent::Finished {
            table_index,
            stats,
            acknowledgement,
        },
        cancellation,
    )
    .await?;
    await_copy_ack(acknowledged, cancellation).await?;
    Ok(stats)
}

async fn send_copy_batch(
    table_index: usize,
    rows: Vec<Vec<Value>>,
    stats: &mut CopyLoadStats,
    sender: &mpsc::Sender<CopyEvent>,
    cancellation: &CancellationToken,
) -> Result<(), CopyLoadError> {
    stats.committed_batches = stats
        .committed_batches
        .checked_add(1)
        .ok_or(CopyLoadError::DecodedByteOverflow)?;
    let (acknowledgement, acknowledged) = oneshot::channel();
    send_copy_event(
        sender,
        CopyEvent::Batch {
            table_index,
            rows,
            stats: *stats,
            acknowledgement,
        },
        cancellation,
    )
    .await?;
    await_copy_ack(acknowledged, cancellation).await
}

async fn send_copy_event(
    sender: &mpsc::Sender<CopyEvent>,
    event: CopyEvent,
    cancellation: &CancellationToken,
) -> Result<(), CopyLoadError> {
    tokio::select! {
        () = cancellation.cancelled() => Err(CopyLoadError::Cancelled),
        result = sender.send(event) => result.map_err(|_| CopyLoadError::MaterializationChannelClosed),
    }
}

async fn await_copy_ack(
    acknowledgement: oneshot::Receiver<()>,
    cancellation: &CancellationToken,
) -> Result<(), CopyLoadError> {
    tokio::select! {
        () = cancellation.cancelled() => Err(CopyLoadError::Cancelled),
        result = acknowledgement => result.map_err(|_| CopyLoadError::MaterializationChannelClosed),
    }
}

#[derive(Debug, Error)]
pub enum SnapshotBuildStateError {
    #[error("snapshot build path already exists")]
    BuildPathAlreadyExists,
    #[error("snapshot plan and exported stream belong to different source registries")]
    RegistryBindingMismatch,
    #[error("snapshot build is tainted by a prior COPY failure")]
    BuildTainted,
    #[error("snapshot build table index {0} does not exist")]
    UnknownTableIndex(usize),
    #[error("bounded parallel COPY limits or worker admission are invalid")]
    InvalidParallelCopyLimits,
    #[error("bounded COPY worker has no assigned tables")]
    EmptyCopyWorkload,
    #[error("snapshot table index {0} was assigned to more than one COPY worker")]
    DuplicateTableIndex(usize),
    #[error("bounded COPY did not cover every planned table exactly once")]
    ParallelCopyIncomplete,
    #[error("bounded COPY materialization channel closed unexpectedly")]
    ParallelCopyChannelClosed,
    #[error("bounded COPY progress counters overflowed")]
    CopyProgressOverflow,
    #[error("bounded COPY worker rollback exceeded its lifecycle deadline")]
    CopyCleanupTimedOut,
    #[error("direct build must roll forward to its cut before COPY")]
    CutNotRolledForward,
    #[error("direct build already rolled forward or copied rows")]
    CutAlreadyRolledForward,
    #[error(transparent)]
    Copy(#[from] CopyLoadError),
    #[error(transparent)]
    Overlay(#[from] crate::DirectOverlayError),
    #[error(transparent)]
    DirectSchema(#[from] crate::DirectColumnarCopyError),
    #[error("direct-columnar encoding failed: {0}")]
    Encode(#[from] direct_columnar::EncodeError),
    #[error(transparent)]
    Coordinator(#[from] CoordinatorError),
    #[error(transparent)]
    Plan(#[from] SnapshotPlanError),
    #[error(transparent)]
    Registry(#[from] SchemaRegistryError),
    #[error("registry revalidation worker failed: {0}")]
    RegistryRevalidationTask(tokio::task::JoinError),
    #[error("registry revalidation exceeded its lifecycle deadline")]
    RegistryRevalidationTimeout,
    #[error("registry revalidation was cancelled")]
    RegistryRevalidationCancelled,
    #[error("registry revalidation connection failed: {0}")]
    RegistryRevalidationConnection(#[source] postgres::Error),
    #[error(transparent)]
    RollForward(#[from] SnapshotRollForwardError),
    #[error(transparent)]
    Io(#[from] std::io::Error),
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_revalidation_connection_bounds_its_replies_past_its_statement_timeout() {
        let endpoint =
            postgres_connect::Endpoint::parse("postgresql://user@127.0.0.1/app").unwrap();
        let endpoint = revalidation_endpoint(endpoint, Duration::from_secs(15));
        assert_eq!(
            endpoint.config().get_reply_timeout(),
            Some(&(Duration::from_secs(15) + postgres_connect::REPLY_MARGIN))
        );
        assert!(endpoint
            .config()
            .get_options()
            .unwrap()
            .contains("-c statement_timeout=15000 -c lock_timeout=15000"));
    }
}
