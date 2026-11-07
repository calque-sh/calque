//! Delivering changes after a bootstrap: long polls, change streams, and the
//! batches they are built from under the fence lock.

use super::*;

/// What one build under the fence lock made: a batch and its grant's expiry,
/// or, for a build from the log that found no room, the plan's room to wait
/// for.
pub(super) enum Built {
    Batch(ChangeBatch, i64),
    NoRoom(PlanRoom),
}

/// What a change build reads: the change tail where the deployment keeps
/// one, and the log's readers.
pub(super) struct ChangeReads<'a, R: PooledLogReader = PostgresCanonicalLogReader> {
    pub(super) activated: &'a ActivatedSource,
    pub(super) tail: Option<&'a ChangeTail>,
    pub(super) readers: &'a Arc<LogReaders<R>>,
    pub(super) room: &'a LogBatchRoom,
}

impl LocalBackendInner {
    pub(super) fn change_reads(&self) -> ChangeReads<'_> {
        ChangeReads {
            activated: &self.activated,
            tail: self.tail.as_deref(),
            readers: &self.log_readers.changes,
            room: &self.log_batch_room,
        }
    }
}

/// Builds from the change tail when it holds the request's whole range,
/// sending each transaction as its scope's one shared encoding, and otherwise
/// from a reader of the log, in the room it reserved or in the plan's room
/// free now. `None` is a build from the log that found no room.
///
/// `acknowledgements` resolves the request's pending ids, and runs only once
/// the build has fixed the head it reads to. A write commits its receipt with
/// its source change, so every echo at or below that head belongs to a
/// receipt the proof can see. A write that commits after the proof is read
/// lands past that head and is acknowledged by the next poll, not passed over
/// unacknowledged by this one.
#[allow(clippy::too_many_arguments)]
pub(super) async fn build_change_batch<R: PooledLogReader + CanonicalLogRead>(
    reads: ChangeReads<'_, R>,
    plan_without_proof: ScopedChangePlan,
    acknowledgements: Option<
        impl Future<Output = Result<ResolvedMutationAcknowledgements, ChangeBatchError>>,
    >,
    request: change_api::ChangeBatchRequest,
    now_unix_micros: i64,
    ceiling: Option<u64>,
    plan: &PlanRoom,
    room: &mut Option<BatchRoom>,
) -> Result<Option<ChangeBatch>, ChangeBatchError> {
    if let Some((tail, view)) = reads.tail.and_then(|tail| {
        tail_view(tail, reads.activated, &request, ceiling).map(|view| (tail, view))
    }) {
        // The view's records stop at the head it was taken at, so the proof
        // read now covers every echo it holds.
        let authorization = authorize(plan_without_proof, acknowledgements).await?;
        let encoding = tail.encoding(ScopeEncodingKey::of(&authorization));
        let inline = view.runs_inline();
        let build = move || {
            ChangeBatchBuilder::new(&view, &authorization)?
                .with_shared_encoding(&encoding)?
                .build_at(&request, now_unix_micros)
        };
        // Most live builds read one commit, and run where they are awaited.
        return if inline {
            build().map(Some)
        } else {
            tokio::task::spawn_blocking(build)
                .await
                .map_err(|_| ChangeBatchError::Log(LogError::PostgresUnavailable))?
                .map(Some)
        };
    }
    let room = match room.take() {
        Some(room) => room,
        None => match reads.room.try_reserve(plan, request.max_batch_bytes)? {
            Some(room) => room,
            None => return Ok(None),
        },
    };
    // A build holds a reader of its own from its first read to its last frame,
    // so builds for different browsers run side by side up to the pool's
    // bound. The reader is awaited here rather than on a blocking thread: a
    // commit wakes every parked poll at once, and each would otherwise hold a
    // thread of the blocking pool while it waited for one.
    let mut reader = reads
        .readers
        .acquire()
        .await
        .map_err(ChangeBatchError::Log)?;
    let mut ceiling = ceiling;
    if acknowledgements.is_some() {
        let (held, head) = tokio::task::spawn_blocking(move || {
            let head = reader.head();
            (reader, head)
        })
        .await
        .map_err(|_| ChangeBatchError::Log(LogError::PostgresUnavailable))?;
        reader = held;
        let head = head.map_err(ChangeBatchError::Log)?.sequence;
        ceiling = Some(ceiling.map_or(head, |ceiling| ceiling.min(head)));
    }
    let authorization = authorize(plan_without_proof, acknowledgements).await?;
    // The room moves into the build, so a build whose client has left keeps
    // it until the build ends.
    tokio::task::spawn_blocking(move || {
        let mut batch = match ceiling {
            None => ChangeBatchBuilder::new(&*reader, &authorization)?
                .build_at(&request, now_unix_micros)?,
            Some(ceiling) => {
                let capped = CeilingLog {
                    log: &*reader,
                    ceiling,
                };
                ChangeBatchBuilder::new(&capped, &authorization)?
                    .build_at(&request, now_unix_micros)?
            }
        };
        room.hold(&mut batch.chunks);
        Ok(Some(batch))
    })
    .await
    .map_err(|_| ChangeBatchError::Log(canonical_log::LogError::PostgresUnavailable))?
}

async fn authorize(
    plan_without_proof: ScopedChangePlan,
    acknowledgements: Option<
        impl Future<Output = Result<ResolvedMutationAcknowledgements, ChangeBatchError>>,
    >,
) -> Result<ChangeAuthorization, ChangeBatchError> {
    let acknowledgeable_mutation_ids = match acknowledgements {
        Some(resolve) => resolve.await?,
        None => ResolvedMutationAcknowledgements::default(),
    };
    Ok(ChangeAuthorization::new(ScopedChangePlan {
        acknowledgeable_mutation_ids,
        ..plan_without_proof
    })?)
}

/// The tail's records for `request`, when it holds the whole range. A cursor
/// that does not decode is left to the log's build, which refuses it.
pub(super) fn tail_view(
    tail: &ChangeTail,
    activated: &ActivatedSource,
    request: &change_api::ChangeBatchRequest,
    ceiling: Option<u64>,
) -> Option<tail::TailView> {
    let codec = activated.cursor_codec();
    let from = codec.decode(&request.from_cursor_exclusive).ok()?.sequence;
    let fence = match &request.through_cursor_inclusive {
        Some(fence) => Some(codec.decode(fence).ok()?.sequence),
        None => None,
    };
    tail.view(from, fence, ceiling, std::time::Instant::now())
}

/// The canonical log as a plan of a fenced generation may see it: nothing past
/// the head its fence stood on.
///
/// The cap is on the log rather than on the batch, so it holds whatever the
/// request asks for -- a live poll's fence is the head this reports, and a
/// read can never return a record past it.
pub(super) struct CeilingLog<'a, L: CanonicalLogRead + ?Sized> {
    pub(super) log: &'a L,
    pub(super) ceiling: u64,
}

impl<L: CanonicalLogRead + ?Sized> CanonicalLogRead for CeilingLog<'_, L> {
    fn source_id(&self) -> &SourceId {
        self.log.source_id()
    }

    fn source_epoch(&self) -> &SourceEpoch {
        self.log.source_epoch()
    }

    fn head(&self) -> Result<CursorPosition, LogError> {
        let mut head = self.log.head()?;
        head.sequence = head.sequence.min(self.ceiling);
        Ok(head)
    }

    fn minimum_retained(&self) -> Result<CursorPosition, LogError> {
        self.log.minimum_retained()
    }

    fn bounds(&self) -> Result<LogBounds, LogError> {
        let mut bounds = self.log.bounds()?;
        bounds.head.sequence = bounds.head.sequence.min(self.ceiling);
        Ok(bounds)
    }

    fn read_page_after_bounded(
        &self,
        from_sequence_exclusive: u64,
        through_sequence_inclusive: Option<u64>,
        max_records: usize,
        max_bytes: u64,
    ) -> Result<CanonicalPage, LogError> {
        if from_sequence_exclusive >= self.ceiling {
            return Ok(CanonicalPage {
                records: Vec::new(),
                stored_bytes: 0,
            });
        }
        let through =
            through_sequence_inclusive.map_or(self.ceiling, |through| through.min(self.ceiling));
        self.log.read_page_after_bounded(
            from_sequence_exclusive,
            Some(through),
            max_records,
            max_bytes,
        )
    }

    fn read_bounds_and_page_after_bounded(
        &self,
        from_sequence_exclusive: u64,
        through_sequence_inclusive: Option<u64>,
        max_records: usize,
        max_bytes: u64,
    ) -> Result<(LogBounds, Result<CanonicalPage, LogError>), LogError> {
        if from_sequence_exclusive >= self.ceiling {
            return Ok((
                self.bounds()?,
                Ok(CanonicalPage {
                    records: Vec::new(),
                    stored_bytes: 0,
                }),
            ));
        }
        let through =
            through_sequence_inclusive.map_or(self.ceiling, |through| through.min(self.ceiling));
        let (mut bounds, page) = self.log.read_bounds_and_page_after_bounded(
            from_sequence_exclusive,
            Some(through),
            max_records,
            max_bytes,
        )?;
        bounds.head.sequence = bounds.head.sequence.min(self.ceiling);
        Ok((bounds, page))
    }
}

/// What a change build can wait on.
pub(super) struct ChangeWakes {
    pub(super) head: watch::Receiver<u64>,
    pub(super) tail: Option<watch::Receiver<u64>>,
    pub(super) revocations: watch::Receiver<u64>,
}

impl ChangeWakes {
    /// Resolves once a build may find something new, and says whether it
    /// can wait again. A commit reaches a build from a fresh tail only once
    /// the tail has read it, so then it waits for the tail to move, and
    /// otherwise for the head. A revocation wakes it either way.
    pub(super) async fn changed(&mut self, tail_fresh: bool) -> bool {
        let Self {
            head,
            tail,
            revocations,
        } = self;
        match tail.as_mut().filter(|_| tail_fresh) {
            // A revocation moves the tail's watches too.
            Some(tail) => tail.changed().await.is_ok(),
            None => tokio::select! {
                changed = head.changed() => changed.is_ok(),
                changed = revocations.changed() => changed.is_ok(),
            },
        }
    }
}

/// A streamed response's state between batches.
pub(super) struct StreamedChanges {
    pub(super) incoming: IncomingChangeRequest,
    /// The request as the next build reads it: its `from_cursor_exclusive`
    /// is where the last written batch ended.
    pub(super) request: change_api::ChangeBatchRequest,
    pub(super) wakes: ChangeWakes,
    pub(super) parked: ParkedPoll,
    pub(super) deadline: tokio::time::Instant,
    /// The last batch stopped short of the head it read.
    pub(super) behind: bool,
    /// The first build's batch, which moved nothing, until a batch is sent.
    pub(super) unsent: Option<ChangeBatch>,
    pub(super) _open_stream: OwnedSemaphorePermit,
}

/// How long the grant still has at `now`, never negative.
pub(super) fn remaining_authorization(grant_expiry: i64, now: i64) -> Duration {
    Duration::from_micros(u64::try_from(grant_expiry.saturating_sub(now)).unwrap_or_default())
}

/// Whether a request of a fenced plan starts past its fence's ceiling, where
/// this deployment never served it.
pub(super) fn starts_past(activated: &ActivatedSource, from: &OpaqueCursor, ceiling: u64) -> bool {
    activated
        .cursor_codec()
        .decode(from)
        .is_ok_and(|from| from.sequence > ceiling)
}

/// The live long-polls parked on this process, at most one per plan.
///
/// A client keeps one poll of a plan in flight, so a second poll of the same
/// plan means the client gave up on the first: a nudge aborts it to send new
/// pending mutation ids, or a proxy holds it open after the browser abandoned
/// it. The client waits on the newer poll, so parking it wakes the older one,
/// which answers at once with what it would have answered when its wait ran
/// out. Parked polls are therefore bounded by the requests in flight, and a
/// client that holds a plan is never refused a wait.
#[derive(Default)]
pub(super) struct ParkedPolls {
    pub(super) next_ticket: u64,
    /// Each plan's parked poll, by ticket. Dropping the sender wakes it.
    pub(super) by_plan: BTreeMap<PlanId, (u64, oneshot::Sender<()>)>,
}

/// One parked poll's place. Dropping it frees the place, unless a newer poll
/// of the same plan has taken it.
pub(super) struct ParkedPoll {
    pub(super) polls: Arc<StdMutex<ParkedPolls>>,
    pub(super) plan_id: PlanId,
    pub(super) ticket: u64,
    /// `None` once a newer poll has taken the place: a resolved receiver
    /// panics if it is polled again, and a stream asks more than once.
    pub(super) superseded: Option<oneshot::Receiver<()>>,
}

impl ParkedPoll {
    pub(super) fn park(polls: &Arc<StdMutex<ParkedPolls>>, plan_id: &PlanId) -> Self {
        let (wake, superseded) = oneshot::channel();
        let mut parked = polls
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let ticket = parked.next_ticket;
        parked.next_ticket = ticket.wrapping_add(1);
        // The entry this replaces drops the older poll's sender, which wakes it.
        parked.by_plan.insert(plan_id.clone(), (ticket, wake));
        drop(parked);
        Self {
            polls: Arc::clone(polls),
            plan_id: plan_id.clone(),
            ticket,
            superseded: Some(superseded),
        }
    }

    /// Resolves once a newer poll of the same plan has parked, and at once
    /// on every call after that.
    pub(super) async fn superseded(&mut self) {
        // Nothing is ever sent: the receiver resolves, with an error, when the
        // newer poll's entry drops its sender.
        if let Some(superseded) = self.superseded.as_mut() {
            let _ = superseded.await;
            self.superseded = None;
        }
    }
}

impl Drop for ParkedPoll {
    fn drop(&mut self) {
        let mut parked = self
            .polls
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if parked
            .by_plan
            .get(&self.plan_id)
            .is_some_and(|(ticket, _)| *ticket == self.ticket)
        {
            parked.by_plan.remove(&self.plan_id);
        }
    }
}

impl LocalBackend {
    pub(super) async fn changes(
        &self,
        incoming: IncomingChangeRequest,
    ) -> Result<ChangeBatch, ChangeBatchError> {
        let mut wakes = self.change_wakes();
        let (batch, _) = self
            .long_poll(&incoming, incoming.request(), &mut wakes)
            .await?;
        Ok(batch)
    }

    /// Every subscription precedes the first build it guards, so a head or
    /// tail move or a barrier that lands while it runs still wakes the wait
    /// after it.
    pub(super) fn change_wakes(&self) -> ChangeWakes {
        ChangeWakes {
            head: self.inner.head_signal.subscribe(),
            tail: self.inner.tail.as_ref().map(|tail| tail.subscribe()),
            revocations: self.inner.revocations.subscribe(),
        }
    }

    /// Wakes every waiting change build to resolve its grant again: those
    /// waiting on a fresh tail through the tail's watches, the others through
    /// the revocation count.
    pub(super) fn wake_builds_for_revocation(&self) {
        self.inner
            .revocations
            .send_modify(|count| *count = count.wrapping_add(1));
        if let Some(tail) = &self.inner.tail {
            tail.notify();
        }
    }

    pub(super) fn tail_is_fresh(&self) -> bool {
        self.inner
            .tail
            .as_ref()
            .is_some_and(|tail| tail.is_fresh(std::time::Instant::now()))
    }

    /// One batch, and the grant's expiry. A live request whose first build
    /// finds nothing waits once for the head to move, at most its timeout and
    /// at most the grant, and builds again.
    pub(super) async fn long_poll(
        &self,
        incoming: &IncomingChangeRequest,
        request: &change_api::ChangeBatchRequest,
        wakes: &mut ChangeWakes,
    ) -> Result<(ChangeBatch, i64), ChangeBatchError> {
        let now = now_unix_micros().map_err(|_| ChangeBatchError::InvalidServerTime)?;
        let live = request.through_cursor_inclusive.is_none();
        let wait_timeout = Duration::from_millis(u64::from(request.long_poll_timeout_ms));
        let (first, grant_expiry) = self.build_under_fence(incoming, request, now).await?;
        // A batch that ends on a revocation barrier is the answer even when it
        // does not move the cursor: a client standing on the barrier's cursor
        // would otherwise wait out its poll for a barrier already signed.
        if !live
            || wait_timeout.is_zero()
            || first.through_cursor_inclusive != request.from_cursor_exclusive
            || first.revocation_barrier_emitted
        {
            return Ok((first, grant_expiry));
        }

        let bounded_wait = wait_timeout.min(remaining_authorization(grant_expiry, now));
        if !bounded_wait.is_zero() {
            let mut parked = ParkedPoll::park(&self.inner.parked_polls, &request.plan_id);
            let tail_fresh = self.tail_is_fresh();
            tokio::select! {
                _ = wakes.changed(tail_fresh) => {}
                () = parked.superseded() => {}
                () = tokio::time::sleep(bounded_wait) => {}
            }
        }
        // After the wait, never before it: a receipt that became canonical
        // while the poll was parked is what woke it, so resolving earlier
        // would delay its acknowledgement by one more poll. The grant is
        // resolved again for the same reason: a fence or a barrier that
        // arrived while it was parked applies to this build.
        let after_wait = now_unix_micros().map_err(|_| ChangeBatchError::InvalidServerTime)?;
        self.build_under_fence(incoming, request, after_wait).await
    }

    /// Answers a request that set `stream`: a batch each time the head moves,
    /// or for a fenced request its batches up to the fence, from a task that
    /// holds the plan's place and one of the process's open streams for the
    /// stream's life.
    ///
    /// The first build's batch opens the response when it moves the cursor.
    /// One that does not is held back: the response opens without it, and it
    /// is sent only if the stream would otherwise end having sent nothing, so
    /// every response carries a batch.
    pub(super) async fn change_stream(
        &self,
        incoming: IncomingChangeRequest,
    ) -> Result<ChangeStream, ChangeBatchError> {
        let opened = tokio::time::Instant::now();
        let open_stream = Arc::clone(&self.inner.open_streams)
            .try_acquire_owned()
            .map_err(|_| ChangeBatchError::OpenStreamLimit)?;
        let wakes = self.change_wakes();
        // Parked before the first build, so a newer request of the plan ends
        // this stream whether or not either of them ever waits.
        let parked = ParkedPoll::park(&self.inner.parked_polls, &incoming.request().plan_id);
        let now = now_unix_micros().map_err(|_| ChangeBatchError::InvalidServerTime)?;
        let (first, grant_expiry) = self
            .build_under_fence(&incoming, incoming.request(), now)
            .await?;
        // A batch that ends on a revocation barrier is the answer even when it
        // does not move the cursor: a client standing on the barrier's cursor
        // would otherwise wait out its stream for a barrier already signed.
        if first.revocation_barrier_emitted || first.reached_requested_fence {
            return Ok(ChangeStream {
                first: Some(first),
                rest: None,
            });
        }
        let lifetime = Duration::from_millis(u64::from(incoming.request().long_poll_timeout_ms));
        let deadline = (opened + lifetime)
            .min(tokio::time::Instant::now() + remaining_authorization(grant_expiry, now));
        let moved = first.through_cursor_inclusive != incoming.request().from_cursor_exclusive;
        let behind = moved && !first.currently_up_to_date;
        let mut request = incoming.request().clone();
        request.from_cursor_exclusive = first.through_cursor_inclusive.clone();
        let (first, unsent) = if moved {
            (Some(first), None)
        } else {
            (None, Some(first))
        };
        let stream = StreamedChanges {
            incoming,
            request,
            wakes,
            parked,
            deadline,
            behind,
            unsent,
            _open_stream: open_stream,
        };
        let (sender, receiver) = tokio::sync::mpsc::channel(1);
        tokio::spawn(
            self.clone()
                .stream_changes(sender, stream)
                .instrument(tracing::Span::current()),
        );
        Ok(ChangeStream {
            first,
            rest: Some(receiver),
        })
    }

    /// Writes a batch each time the head moves, or for a fenced stream each
    /// batch at once, until the stream's deadline, a newer request of its
    /// plan, a batch that ends on a revocation barrier, a fenced batch that
    /// reaches the fence, a refused build, or the client leaving. The client's
    /// next request meets a refusal with its status, so the stream ends after
    /// its last complete batch rather than carrying an error.
    ///
    /// `stream` is dropped before `sender`, so the stream's place among the
    /// open streams is free by the time its body ends.
    pub(super) async fn stream_changes(
        self,
        sender: tokio::sync::mpsc::Sender<Vec<Bytes>>,
        mut stream: StreamedChanges,
    ) {
        let deadline = tokio::time::sleep_until(stream.deadline);
        tokio::pin!(deadline);
        loop {
            if !stream.behind {
                let tail_fresh = self.tail_is_fresh();
                tokio::select! {
                    biased;
                    () = sender.closed() => return,
                    () = stream.parked.superseded() => break,
                    () = &mut deadline => break,
                    woke = stream.wakes.changed(tail_fresh) => if !woke { break },
                }
            }
            // Each build resolves the grant, the revocation ceiling and the
            // acknowledgeable receipts again, as a long-poll's second build
            // does: nothing resolved for one batch is reused for the next.
            let Ok(now) = now_unix_micros() else { break };
            let built = tokio::select! {
                biased;
                () = sender.closed() => return,
                () = stream.parked.superseded() => break,
                () = &mut deadline => break,
                built = self.build_under_fence(&stream.incoming, &stream.request, now) => built,
            };
            let batch = match built {
                Ok((batch, _)) => batch,
                Err(error) => {
                    tracing::debug!(%error, "a streamed change build was refused");
                    break;
                }
            };
            let moved = batch.through_cursor_inclusive != stream.request.from_cursor_exclusive;
            // A batch its budget cut short is followed at once, without
            // waiting for the head to move again.
            stream.behind = moved && !batch.currently_up_to_date;
            if !moved && !batch.revocation_barrier_emitted {
                continue;
            }
            let last = batch.revocation_barrier_emitted || batch.reached_requested_fence;
            stream.request.from_cursor_exclusive = batch.through_cursor_inclusive;
            tokio::select! {
                biased;
                () = stream.parked.superseded() => break,
                () = &mut deadline => break,
                sent = sender.send(batch.chunks) => if sent.is_err() { return },
            }
            stream.unsent = None;
            if last {
                return;
            }
        }
        if let Some(unsent) = stream.unsent.take() {
            let _ = sender.send(unsent.chunks).await;
        }
    }

    /// One change batch for the grant as it stands now, built under the
    /// shared fence lock, and the grant's expiry.
    ///
    /// A plan whose generation is fenced reads the canonical log only up to
    /// the head its fence stood on. Until the application signs its barrier it
    /// is served empty batches there; once it has, the batch ends on the
    /// barrier. A request that claims a position past the fence was never
    /// served by this deployment and is refused as expired.
    ///
    /// A build from the log that finds no room for its batch waits for it
    /// after giving the fence lock back, so a slow client's unwritten batches
    /// never hold up a fence, and then resolves the grant again.
    pub(super) async fn build_under_fence(
        &self,
        incoming: &IncomingChangeRequest,
        request: &change_api::ChangeBatchRequest,
        now: i64,
    ) -> Result<(ChangeBatch, i64), ChangeBatchError> {
        let mut room = None;
        let mut now = now;
        loop {
            match self
                .build_once_under_fence(incoming, request, now, &mut room)
                .await?
            {
                Built::Batch(batch, grant_expiry) => return Ok((batch, grant_expiry)),
                Built::NoRoom(plan) => {
                    room = Some(
                        self.inner
                            .log_batch_room
                            .reserve(&plan, request.max_batch_bytes)
                            .await?,
                    );
                    now = now_unix_micros().map_err(|_| ChangeBatchError::InvalidServerTime)?;
                }
            }
        }
    }

    pub(super) async fn build_once_under_fence(
        &self,
        incoming: &IncomingChangeRequest,
        request: &change_api::ChangeBatchRequest,
        now: i64,
        room: &mut Option<BatchRoom>,
    ) -> Result<Built, ChangeBatchError> {
        let _fence = self.inner.build_fence.build().await;
        let grant = self
            .inner
            .plans
            .resolve(&self.serving(), incoming, request, now)?;
        let ceiling = grant
            .revocation
            .as_ref()
            .map(|revocation| revocation.ceiling);
        if ceiling.is_some_and(|ceiling| {
            starts_past(
                &self.inner.activated,
                &request.from_cursor_exclusive,
                ceiling,
            )
        }) {
            return Err(ChangeBatchError::AuthorizationExpired);
        }
        let grant_expiry = grant.claims.expires_at_unix_micros;
        let plan_without_proof = ScopedChangePlan {
            plan_id: request.plan_id.clone(),
            activated_scope: grant.plan.activated_scope.clone(),
            expires_at_unix_micros: grant_expiry,
            from_cursor_exclusive: grant.claims.start_cursor()?,
            through_cursor_inclusive: grant.claims.through_cursor()?,
            table_starts: grant.table_starts,
            acknowledgeable_mutation_ids: ResolvedMutationAcknowledgements::default(),
            revocation_barrier: grant.revocation.and_then(|revocation| revocation.signed),
        };
        let acknowledgements = (!incoming.pending_mutation_ids().is_empty())
            .then(|| self.resolve_acknowledgements(incoming, &grant.plan.scope));
        Ok(
            match build_change_batch(
                self.inner.change_reads(),
                plan_without_proof,
                acknowledgements,
                request.clone(),
                now,
                ceiling,
                &grant.plan.room,
                room,
            )
            .await?
            {
                Some(batch) => Built::Batch(batch, grant_expiry),
                None => Built::NoRoom(grant.plan.room.clone()),
            },
        )
    }
}
