//! The planner's synchronous view of the canonical log: each read goes over a
//! channel to a thread of its own that holds the log.

use super::*;

pub(super) enum CanonicalLogCommand {
    Head(mpsc::SyncSender<Result<CursorPosition, LogError>>),
    MinimumRetained(mpsc::SyncSender<Result<CursorPosition, LogError>>),
    ReadPage {
        from_sequence_exclusive: u64,
        through_sequence_inclusive: Option<u64>,
        max_records: usize,
        max_bytes: u64,
        reply: mpsc::SyncSender<Result<CanonicalPage, LogError>>,
    },
    AcquirePlanRetention {
        plan_id: PlanId,
        minimum_sequence: u64,
        expires_ms: i64,
        now_ms: i64,
        reply: mpsc::SyncSender<Result<(), LogError>>,
    },
}

pub(super) struct BlockingCanonicalLogProxy {
    pub(super) source_id: SourceId,
    pub(super) source_epoch: SourceEpoch,
    pub(super) commands: mpsc::SyncSender<CanonicalLogCommand>,
}

pub(super) type CanonicalLogWorker = Box<dyn FnOnce() + Send + 'static>;

pub(super) struct BlockingCanonicalLogBridge {
    pub(super) proxy: Option<BlockingCanonicalLogProxy>,
    pub(super) worker: Option<JoinHandle<()>>,
}

impl BlockingCanonicalLogBridge {
    pub(super) fn proxy_mut(&mut self) -> &mut BlockingCanonicalLogProxy {
        self.proxy
            .as_mut()
            .expect("canonical-log proxy is available until bridge shutdown")
    }

    pub(super) fn finish(mut self) -> Result<(), LogError> {
        self.disconnect_and_join()
    }

    pub(super) fn disconnect_and_join(&mut self) -> Result<(), LogError> {
        // Disconnect first so an idle worker leaves recv(), then always consume
        // the handle. Join failure is reported on the normal path and ignored by
        // Drop so cleanup cannot cause a second panic during unwinding.
        self.proxy.take();
        if let Some(worker) = self.worker.take() {
            worker.join().map_err(|_| LogError::PostgresUnavailable)?;
        }
        Ok(())
    }
}

impl Drop for BlockingCanonicalLogBridge {
    fn drop(&mut self) {
        let _ = self.disconnect_and_join();
    }
}

impl BlockingCanonicalLogProxy {
    pub(super) fn call<T>(
        &self,
        command: impl FnOnce(mpsc::SyncSender<Result<T, LogError>>) -> CanonicalLogCommand,
    ) -> Result<T, LogError> {
        let (reply, result) = mpsc::sync_channel(0);
        self.commands
            .send(command(reply))
            .map_err(|_| LogError::PostgresUnavailable)?;
        result.recv().map_err(|_| LogError::PostgresUnavailable)?
    }
}

impl CanonicalLogRead for BlockingCanonicalLogProxy {
    fn source_id(&self) -> &SourceId {
        &self.source_id
    }

    fn source_epoch(&self) -> &SourceEpoch {
        &self.source_epoch
    }

    fn head(&self) -> Result<CursorPosition, LogError> {
        self.call(CanonicalLogCommand::Head)
    }

    fn minimum_retained(&self) -> Result<CursorPosition, LogError> {
        self.call(CanonicalLogCommand::MinimumRetained)
    }

    fn read_page_after_bounded(
        &self,
        from_sequence_exclusive: u64,
        through_sequence_inclusive: Option<u64>,
        max_records: usize,
        max_bytes: u64,
    ) -> Result<CanonicalPage, LogError> {
        self.call(|reply| CanonicalLogCommand::ReadPage {
            from_sequence_exclusive,
            through_sequence_inclusive,
            max_records,
            max_bytes,
            reply,
        })
    }
}

impl CanonicalLogPlanRetention for BlockingCanonicalLogProxy {
    fn acquire_plan_retention_lease(
        &mut self,
        plan_id: PlanId,
        minimum_sequence: u64,
        expires_ms: i64,
        now_ms: i64,
    ) -> Result<(), LogError> {
        self.call(|reply| CanonicalLogCommand::AcquirePlanRetention {
            plan_id,
            minimum_sequence,
            expires_ms,
            now_ms,
            reply,
        })
    }
}

pub(super) fn blocking_canonical_log_proxy<L>(
    source_id: SourceId,
    source_epoch: SourceEpoch,
    log: Arc<Mutex<L>>,
) -> io::Result<BlockingCanonicalLogBridge>
where
    L: CanonicalLogRead + CanonicalLogPlanRetention + 'static,
{
    blocking_canonical_log_proxy_with_spawner(source_id, source_epoch, log, |worker| {
        std::thread::Builder::new()
            .name("calque-canonical-log".to_owned())
            .spawn(worker)
    })
}

pub(super) fn blocking_canonical_log_proxy_with_spawner<L>(
    source_id: SourceId,
    source_epoch: SourceEpoch,
    log: Arc<Mutex<L>>,
    spawn: impl FnOnce(CanonicalLogWorker) -> io::Result<JoinHandle<()>>,
) -> io::Result<BlockingCanonicalLogBridge>
where
    L: CanonicalLogRead + CanonicalLogPlanRetention + 'static,
{
    let (commands, receiver) = mpsc::sync_channel(1);
    let worker = spawn(Box::new(move || {
        while let Ok(command) = receiver.recv() {
            let mut log = log.blocking_lock();
            match command {
                CanonicalLogCommand::Head(reply) => {
                    let _ = reply.send(log.head());
                }
                CanonicalLogCommand::MinimumRetained(reply) => {
                    let _ = reply.send(log.minimum_retained());
                }
                CanonicalLogCommand::ReadPage {
                    from_sequence_exclusive,
                    through_sequence_inclusive,
                    max_records,
                    max_bytes,
                    reply,
                } => {
                    let _ = reply.send(log.read_page_after_bounded(
                        from_sequence_exclusive,
                        through_sequence_inclusive,
                        max_records,
                        max_bytes,
                    ));
                }
                CanonicalLogCommand::AcquirePlanRetention {
                    plan_id,
                    minimum_sequence,
                    expires_ms,
                    now_ms,
                    reply,
                } => {
                    let _ = reply.send(log.acquire_plan_retention_lease(
                        plan_id,
                        minimum_sequence,
                        expires_ms,
                        now_ms,
                    ));
                }
            }
        }
    }))?;
    Ok(BlockingCanonicalLogBridge {
        proxy: Some(BlockingCanonicalLogProxy {
            source_id,
            source_epoch,
            commands,
        }),
        worker: Some(worker),
    })
}
