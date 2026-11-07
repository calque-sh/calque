use std::collections::{BTreeMap, BTreeSet};

use crate::{
    change::{
        change_stream_frame, ChangeStreamFrame, RevocationBarrier, SourceGeneration,
        TransactionFragment,
    },
    validate_change_stream_frame, verify_change_frame_body_sha256, verify_revocation_barrier,
    BatchFramesDigest, ProtocolValidationError, RevocationBarrierVerificationKey,
    MAX_CHANGE_TRANSACTIONS, MAX_REQUEST_TABLES, MAX_TRANSACTION_OPERATIONS,
};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ChangeStreamExpectation {
    pub plan_id: String,
    pub from_cursor_exclusive: Vec<u8>,
    pub requested_fence: Option<Vec<u8>>,
    pub table_starts: Vec<ExpectedTableStart>,
    pub generation: ExpectedSourceGeneration,
    /// The Ed25519 key this receiver accepts revocation barriers from. `None`
    /// refuses every barrier rather than skipping the check.
    pub revocation_barrier_key: Option<RevocationBarrierVerificationKey>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ExpectedTableStart {
    pub table_id: String,
    pub snapshot_cursor: Option<Vec<u8>>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ExpectedSourceGeneration {
    pub source_epoch: String,
    pub publication_generation: u64,
    pub schema_generation: u64,
    pub authorization_generation: u64,
    pub authorization_scope_hash: Vec<u8>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CompletedBatch {
    pub through_cursor_inclusive: Vec<u8>,
    pub relevant_transaction_count: u64,
    pub relevant_operation_count: u64,
    pub reached_requested_fence: bool,
    /// The request's pending mutations this batch acknowledged.
    pub acknowledged_mutation_ids: Vec<String>,
    /// The authenticated barrier this batch ended on, when it ended on one.
    pub revocation_barrier: Option<RevocationBarrier>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Phase {
    AwaitingStart,
    Batch,
    Complete,
    Error,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct ActiveTransaction {
    transaction_id: Vec<u8>,
    transaction_cursor: Vec<u8>,
    postgres_xid: u64,
    begin_lsn: String,
    commit_lsn: String,
    commit_time_unix_micros: u64,
    next_fragment_index: u32,
}

/// Fail-closed v1 stream-order validator above protobuf and body integrity.
pub struct ChangeStreamState {
    expectation: ChangeStreamExpectation,
    expected_table_starts: BTreeMap<String, Option<Vec<u8>>>,
    phase: Phase,
    /// The body digests of the batch's frames so far, which `BatchEnd`'s
    /// count and digest must match.
    frames: BatchFramesDigest,
    active_transaction: Option<ActiveTransaction>,
    transaction_ids: BTreeSet<Vec<u8>>,
    transaction_cursors: BTreeSet<Vec<u8>>,
    transaction_count: u64,
    operation_count: u64,
    revocation_barrier: Option<RevocationBarrier>,
}

impl ChangeStreamState {
    pub fn new(expectation: ChangeStreamExpectation) -> Result<Self, ProtocolValidationError> {
        if expectation.plan_id.is_empty() {
            return Err(fail("changeStreamExpectation.planId", "must not be empty"));
        }
        if expectation.from_cursor_exclusive.is_empty() {
            return Err(fail(
                "changeStreamExpectation.fromCursorExclusive",
                "must not be empty",
            ));
        }
        if expectation
            .requested_fence
            .as_ref()
            .is_some_and(Vec::is_empty)
        {
            return Err(fail(
                "changeStreamExpectation.requestedFence",
                "must not be empty when present",
            ));
        }
        if expectation.table_starts.is_empty()
            || expectation.table_starts.len() > MAX_REQUEST_TABLES
        {
            return Err(fail(
                "changeStreamExpectation.tableStarts",
                format!("must contain 1..={MAX_REQUEST_TABLES} tables"),
            ));
        }
        if expectation.generation.source_epoch.is_empty()
            || expectation.generation.authorization_scope_hash.len() != 32
        {
            return Err(fail(
                "changeStreamExpectation.generation",
                "contains an invalid source epoch or authorization scope hash",
            ));
        }
        let mut expected_table_starts = BTreeMap::new();
        for (index, start) in expectation.table_starts.iter().enumerate() {
            if start.table_id.is_empty()
                || start.snapshot_cursor.as_ref().is_some_and(Vec::is_empty)
            {
                return Err(fail(
                    format!("changeStreamExpectation.tableStarts[{index}]"),
                    "contains an invalid table start",
                ));
            }
            if expected_table_starts
                .insert(start.table_id.clone(), start.snapshot_cursor.clone())
                .is_some()
            {
                return Err(fail(
                    format!("changeStreamExpectation.tableStarts[{index}].tableId"),
                    "must be unique",
                ));
            }
        }
        Ok(Self {
            expectation,
            expected_table_starts,
            phase: Phase::AwaitingStart,
            frames: BatchFramesDigest::new(),
            active_transaction: None,
            transaction_ids: BTreeSet::new(),
            transaction_cursors: BTreeSet::new(),
            transaction_count: 0,
            operation_count: 0,
            revocation_barrier: None,
        })
    }

    pub fn terminal(&self) -> bool {
        matches!(self.phase, Phase::Complete | Phase::Error)
    }

    pub fn accept(
        &mut self,
        frame: &ChangeStreamFrame,
    ) -> Result<Option<CompletedBatch>, ProtocolValidationError> {
        if self.terminal() {
            return Err(fail("changeStream", "is already terminal"));
        }
        let result = self.accept_inner(frame);
        if result.is_err() {
            self.phase = Phase::Error;
        }
        result
    }

    fn accept_inner(
        &mut self,
        frame: &ChangeStreamFrame,
    ) -> Result<Option<CompletedBatch>, ProtocolValidationError> {
        validate_change_stream_frame(frame)?;
        verify_change_frame_body_sha256(frame).map_err(|error| {
            fail(
                "changeStreamFrame.bodySha256",
                format!("failed integrity verification: {error}"),
            )
        })?;
        let body_sha256: [u8; 32] = frame
            .body_sha256
            .as_slice()
            .try_into()
            .expect("verified digests are 32 bytes");

        match frame
            .body
            .as_ref()
            .expect("stateless validation requires body")
        {
            change_stream_frame::Body::BatchStart(batch) => {
                if self.phase != Phase::AwaitingStart {
                    return Err(fail(
                        "changeStreamFrame.batchStart",
                        "must be the first frame",
                    ));
                }
                if batch
                    .from_cursor_exclusive
                    .as_ref()
                    .map(|cursor| cursor.token.as_slice())
                    != Some(self.expectation.from_cursor_exclusive.as_slice())
                {
                    return Err(fail(
                        "changeStreamFrame.batchStart.fromCursorExclusive",
                        "does not equal the requested cursor",
                    ));
                }
                if batch
                    .requested_fence
                    .as_ref()
                    .map(|cursor| cursor.token.as_slice())
                    != self.expectation.requested_fence.as_deref()
                {
                    return Err(fail(
                        "changeStreamFrame.batchStart.requestedFence",
                        "does not equal the requested fence",
                    ));
                }
                let actual_ids = batch.table_ids.iter().collect::<BTreeSet<_>>();
                let expected_ids = self.expected_table_starts.keys().collect::<BTreeSet<_>>();
                if actual_ids != expected_ids {
                    return Err(fail(
                        "changeStreamFrame.batchStart.tableIds",
                        "does not equal the requested table set",
                    ));
                }
                if batch.plan_id != self.expectation.plan_id {
                    return Err(fail(
                        "changeStreamFrame.batchStart.planId",
                        "does not equal the authenticated bootstrap plan",
                    ));
                }
                let actual_starts = batch
                    .table_starts
                    .iter()
                    .map(|start| {
                        (
                            start.table_id.clone(),
                            start
                                .shard_snapshot_cursor
                                .as_ref()
                                .map(|cursor| cursor.token.clone()),
                        )
                    })
                    .collect::<BTreeMap<_, _>>();
                if actual_starts != self.expected_table_starts {
                    return Err(fail(
                        "changeStreamFrame.batchStart.tableStarts",
                        "does not equal the authenticated per-table starts",
                    ));
                }
                validate_generation(
                    batch.generation.as_ref().expect("validated generation"),
                    &self.expectation.generation,
                )?;
                self.phase = Phase::Batch;
                self.frames.push(&body_sha256);
                Ok(None)
            }
            change_stream_frame::Body::TransactionFragment(fragment) => {
                self.accept_fragment(fragment)?;
                self.frames.push(&body_sha256);
                Ok(None)
            }
            change_stream_frame::Body::Heartbeat(_) => {
                self.require_batch_without_fragment("heartbeat")?;
                self.frames.push(&body_sha256);
                Ok(None)
            }
            change_stream_frame::Body::BatchEnd(batch) => {
                self.require_batch_without_fragment("batchEnd")?;
                if batch.frame_count != self.frames.frames() {
                    return Err(fail(
                        "changeStreamFrame.batchEnd.frameCount",
                        format!(
                            "is {}, observed {}",
                            batch.frame_count,
                            self.frames.frames()
                        ),
                    ));
                }
                if batch.frames_sha256.as_slice()
                    != std::mem::take(&mut self.frames).finish().as_slice()
                {
                    return Err(fail(
                        "changeStreamFrame.batchEnd.framesSha256",
                        "does not cover the frames this batch carried, in their order",
                    ));
                }
                if batch.relevant_transaction_count != self.transaction_count {
                    return Err(fail(
                        "changeStreamFrame.batchEnd.relevantTransactionCount",
                        format!(
                            "is {}, observed {}",
                            batch.relevant_transaction_count, self.transaction_count
                        ),
                    ));
                }
                if batch.relevant_operation_count != self.operation_count {
                    return Err(fail(
                        "changeStreamFrame.batchEnd.relevantOperationCount",
                        format!(
                            "is {}, observed {}",
                            batch.relevant_operation_count, self.operation_count
                        ),
                    ));
                }
                let through = batch
                    .through_cursor_inclusive
                    .as_ref()
                    .expect("validated cursor")
                    .token
                    .clone();
                if let Some(barrier) = self.revocation_barrier.as_ref() {
                    let barrier_cursor = barrier
                        .barrier_cursor
                        .as_ref()
                        .expect("validated barrier cursor")
                        .token
                        .as_slice();
                    if barrier_cursor != through.as_slice() {
                        return Err(fail(
                            "changeStreamFrame.batchEnd.throughCursorInclusive",
                            "must equal the revocation barrier's cursor",
                        ));
                    }
                }
                let reached_expected_fence = self
                    .expectation
                    .requested_fence
                    .as_ref()
                    .is_some_and(|fence| *fence == through);
                if batch.reached_requested_fence != reached_expected_fence {
                    return Err(fail(
                        "changeStreamFrame.batchEnd.reachedRequestedFence",
                        "must exactly match requested-fence cursor equality",
                    ));
                }
                self.phase = Phase::Complete;
                Ok(Some(CompletedBatch {
                    through_cursor_inclusive: through,
                    relevant_transaction_count: self.transaction_count,
                    relevant_operation_count: self.operation_count,
                    reached_requested_fence: batch.reached_requested_fence,
                    acknowledged_mutation_ids: batch.acknowledged_mutation_ids.clone(),
                    revocation_barrier: self.revocation_barrier.clone(),
                }))
            }
            change_stream_frame::Body::RevocationBarrier(barrier) => {
                self.accept_revocation_barrier(barrier)?;
                self.frames.push(&body_sha256);
                Ok(None)
            }
            change_stream_frame::Body::Error(_) => {
                // A batch that carries a barrier and then a stream error never
                // completes, so the revocation would reach the caller as a
                // transport failure it may retry. A barrier batch either ends
                // on the barrier or is refused whole.
                if self.revocation_barrier.is_some() {
                    return Err(fail(
                        "changeStreamFrame.error",
                        "must not follow a revocation barrier in the same batch",
                    ));
                }
                self.phase = Phase::Error;
                Ok(None)
            }
        }
    }

    /// The ordered authenticated revocation barrier.
    ///
    /// It may not interleave with an open transaction, it must be the last
    /// body before `BatchEnd` (the error arm above enforces part of that), and
    /// `BatchEnd.throughCursorInclusive` must equal its cursor, so no
    /// transaction straddles it and the batch ends on it. Before any of that is
    /// remembered, its signature must verify against the configured key, and
    /// it must retire the generation and scope this stream is bound to and be
    /// addressed to this stream's plan.
    fn accept_revocation_barrier(
        &mut self,
        barrier: &RevocationBarrier,
    ) -> Result<(), ProtocolValidationError> {
        // `require_batch_without_fragment` refuses every body but `batchEnd`
        // once one barrier has been seen, a second barrier included.
        self.require_batch_without_fragment("revocationBarrier")?;
        let key = self
            .expectation
            .revocation_barrier_key
            .as_ref()
            .ok_or_else(|| {
                fail(
                    "changeStreamFrame.revocationBarrier",
                    "cannot be authenticated: this receiver has no revocation barrier key",
                )
            })?;
        verify_revocation_barrier(barrier, key).map_err(|error| {
            fail(
                "changeStreamFrame.revocationBarrier.signature",
                format!("failed authorization-authority verification: {error}"),
            )
        })?;
        if barrier.revoked_authorization_generation
            != self.expectation.generation.authorization_generation
        {
            return Err(fail(
                "changeStreamFrame.revocationBarrier.revokedAuthorizationGeneration",
                format!(
                    "is {}, but this stream runs authorization generation {}",
                    barrier.revoked_authorization_generation,
                    self.expectation.generation.authorization_generation
                ),
            ));
        }
        if barrier.revoked_authorization_scope_hash
            != self.expectation.generation.authorization_scope_hash
        {
            return Err(fail(
                "changeStreamFrame.revocationBarrier.revokedAuthorizationScopeHash",
                "does not equal this stream's authorization scope",
            ));
        }
        self.revocation_barrier = Some(barrier.clone());
        Ok(())
    }

    fn accept_fragment(
        &mut self,
        fragment: &TransactionFragment,
    ) -> Result<(), ProtocolValidationError> {
        if self.revocation_barrier.is_some() {
            return Err(fail(
                "changeStreamFrame.transactionFragment",
                "must not follow a revocation barrier in the same batch",
            ));
        }
        if self.phase != Phase::Batch {
            return Err(fail(
                "changeStreamFrame.transactionFragment",
                "requires a preceding batch start",
            ));
        }
        if let Some(operation) = fragment
            .operations
            .iter()
            .find(|operation| !self.expected_table_starts.contains_key(&operation.table_id))
        {
            return Err(fail(
                "changeStreamFrame.transactionFragment.operations.tableId",
                format!("is not subscribed: {}", operation.table_id),
            ));
        }
        let cursor = fragment
            .transaction_cursor
            .as_ref()
            .expect("validated transaction cursor")
            .token
            .clone();
        match &mut self.active_transaction {
            None => {
                if !fragment.first_fragment || fragment.fragment_index != 0 {
                    return Err(fail(
                        "changeStreamFrame.transactionFragment",
                        "a transaction must begin at fragment zero",
                    ));
                }
                if !self.transaction_ids.insert(fragment.transaction_id.clone()) {
                    return Err(fail(
                        "changeStreamFrame.transactionFragment.transactionId",
                        "was already completed in this batch",
                    ));
                }
                if !self.transaction_cursors.insert(cursor.clone()) {
                    return Err(fail(
                        "changeStreamFrame.transactionFragment.transactionCursor",
                        "was already completed in this batch",
                    ));
                }
                self.active_transaction = Some(ActiveTransaction {
                    transaction_id: fragment.transaction_id.clone(),
                    transaction_cursor: cursor,
                    postgres_xid: fragment.postgres_xid,
                    begin_lsn: fragment.begin_lsn.clone(),
                    commit_lsn: fragment.commit_lsn.clone(),
                    commit_time_unix_micros: fragment.commit_time_unix_micros,
                    next_fragment_index: 1,
                });
            }
            Some(active) => {
                if fragment.first_fragment || fragment.fragment_index != active.next_fragment_index
                {
                    return Err(fail(
                        "changeStreamFrame.transactionFragment.fragmentIndex",
                        format!(
                            "expected consecutive fragment {}",
                            active.next_fragment_index
                        ),
                    ));
                }
                if fragment.transaction_id != active.transaction_id
                    || cursor != active.transaction_cursor
                    || fragment.postgres_xid != active.postgres_xid
                    || fragment.begin_lsn != active.begin_lsn
                    || fragment.commit_lsn != active.commit_lsn
                    || fragment.commit_time_unix_micros != active.commit_time_unix_micros
                {
                    return Err(fail(
                        "changeStreamFrame.transactionFragment",
                        "changed transaction identity or PostgreSQL provenance mid-fragment",
                    ));
                }
                active.next_fragment_index =
                    active.next_fragment_index.checked_add(1).ok_or_else(|| {
                        fail(
                            "changeStreamFrame.transactionFragment.fragmentIndex",
                            "fragment index overflow",
                        )
                    })?;
            }
        }
        self.operation_count = self
            .operation_count
            .checked_add(u64::try_from(fragment.operations.len()).map_err(|_| {
                fail(
                    "changeStreamFrame.transactionFragment.operations",
                    "operation count overflow",
                )
            })?)
            .ok_or_else(|| {
                fail(
                    "changeStreamFrame.transactionFragment.operations",
                    "operation count overflow",
                )
            })?;
        if self.operation_count > MAX_TRANSACTION_OPERATIONS as u64 {
            return Err(fail(
                "changeStreamFrame.transactionFragment.operations",
                format!("batch exceeds {MAX_TRANSACTION_OPERATIONS} operations"),
            ));
        }
        if fragment.final_fragment {
            self.transaction_count = self.transaction_count.checked_add(1).ok_or_else(|| {
                fail(
                    "changeStreamFrame.transactionFragment",
                    "transaction count overflow",
                )
            })?;
            if self.transaction_count > u64::from(MAX_CHANGE_TRANSACTIONS) {
                return Err(fail(
                    "changeStreamFrame.transactionFragment",
                    format!("batch exceeds {MAX_CHANGE_TRANSACTIONS} transactions"),
                ));
            }
            self.active_transaction = None;
        }
        Ok(())
    }

    fn require_batch_without_fragment(&self, body: &str) -> Result<(), ProtocolValidationError> {
        if self.revocation_barrier.is_some() && body != "batchEnd" {
            return Err(fail(
                format!("changeStreamFrame.{body}"),
                "must not follow a revocation barrier in the same batch",
            ));
        }
        if self.phase != Phase::Batch {
            return Err(fail(
                format!("changeStreamFrame.{body}"),
                "requires a preceding batch start",
            ));
        }
        if self.active_transaction.is_some() {
            return Err(fail(
                format!("changeStreamFrame.{body}"),
                "cannot interleave with transaction fragments",
            ));
        }
        Ok(())
    }
}

fn validate_generation(
    actual: &SourceGeneration,
    expected: &ExpectedSourceGeneration,
) -> Result<(), ProtocolValidationError> {
    if actual.source_epoch != expected.source_epoch
        || actual.publication_generation != expected.publication_generation
        || actual.schema_generation != expected.schema_generation
        || actual.authorization_generation != expected.authorization_generation
        || actual.authorization_scope_hash != expected.authorization_scope_hash
    {
        return Err(fail(
            "changeStreamFrame.batchStart.generation",
            "does not equal the expected source/authorization generation",
        ));
    }
    Ok(())
}

fn fail(path: impl Into<String>, message: impl Into<String>) -> ProtocolValidationError {
    ProtocolValidationError {
        path: path.into(),
        message: message.into(),
    }
}

#[cfg(test)]
mod tests {
    use crate::{
        change::{
            change_stream_frame, operation, BatchEnd, BatchStart, ChangeStreamFrame, Cursor,
            Heartbeat, Operation, OperationKind, RevocationBarrier, SourceGeneration, StreamError,
            StreamErrorCode, TableSubscription, TransactionFragment, Truncate,
        },
        change_frame_body_sha256,
    };

    use super::*;

    fn cursor(value: u8) -> Cursor {
        Cursor { token: vec![value] }
    }

    fn expectation() -> ChangeStreamExpectation {
        ChangeStreamExpectation {
            plan_id: "plan-1".to_owned(),
            from_cursor_exclusive: vec![0],
            requested_fence: Some(vec![9]),
            table_starts: vec![ExpectedTableStart {
                table_id: "public.tasks".to_owned(),
                snapshot_cursor: Some(vec![0]),
            }],
            generation: ExpectedSourceGeneration {
                source_epoch: "epoch-1".to_owned(),
                publication_generation: 2,
                schema_generation: 3,
                authorization_generation: 4,
                authorization_scope_hash: vec![5; 32],
            },
            revocation_barrier_key: Some(RevocationBarrierVerificationKey::new(
                TEST_BARRIER_KEY_ID,
                test_barrier_public_key(),
            )),
        }
    }

    /// The fixed Ed25519 key pair every revocation-barrier vector in this
    /// repository is signed with, in Rust and in TypeScript alike.
    const TEST_BARRIER_SEED: [u8; 32] = [
        0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24,
        25, 26, 27, 28, 29, 30, 31,
    ];
    const TEST_BARRIER_KEY_ID: &str = "revocation-key-1";

    fn test_barrier_key_pair() -> ring::signature::Ed25519KeyPair {
        ring::signature::Ed25519KeyPair::from_seed_unchecked(&TEST_BARRIER_SEED)
            .expect("fixed test seed is a valid Ed25519 seed")
    }

    fn test_barrier_public_key() -> [u8; 32] {
        use ring::signature::KeyPair;
        let pair = test_barrier_key_pair();
        pair.public_key()
            .as_ref()
            .try_into()
            .expect("Ed25519 public keys are 32 bytes")
    }

    fn sign_barrier(barrier: &mut RevocationBarrier) {
        let preimage =
            crate::revocation_barrier_signing_preimage(barrier).expect("barrier has a cursor");
        barrier.signature = test_barrier_key_pair().sign(&preimage).as_ref().to_vec();
    }

    fn barrier(cursor_token: u8) -> RevocationBarrier {
        let mut barrier = RevocationBarrier {
            barrier_cursor: Some(cursor(cursor_token)),
            revoked_authorization_generation: 4,
            effective_authorization_generation: 5,
            revoked_authorization_scope_hash: vec![5; 32],
            barrier_id: "barrier-1".to_owned(),
            issued_at_unix_micros: 1_700_000_000_000_000,
            signing_key_id: TEST_BARRIER_KEY_ID.to_owned(),
            signature: Vec::new(),
        };
        sign_barrier(&mut barrier);
        barrier
    }

    #[test]
    fn accepts_one_authenticated_barrier_that_ends_the_batch_at_its_own_cursor() {
        let mut state = ChangeStreamState::new(expectation()).unwrap();
        let frames = [
            start(),
            fragment(0, true),
            seal(change_stream_frame::Body::RevocationBarrier(barrier(9))),
        ];
        for frame in &frames {
            state.accept(frame).unwrap();
        }
        let completed = state
            .accept(&ended(
                &frames.iter().collect::<Vec<_>>(),
                BatchEnd {
                    through_cursor_inclusive: Some(cursor(9)),
                    reached_requested_fence: true,
                    relevant_transaction_count: 1,
                    relevant_operation_count: 1,
                    ..BatchEnd::default()
                },
            ))
            .unwrap()
            .unwrap();
        let observed = completed.revocation_barrier.expect("barrier is reported");
        assert_eq!(observed.effective_authorization_generation, 5);
        assert_eq!(observed.barrier_id, "barrier-1");
    }

    #[test]
    fn refuses_a_barrier_that_is_forged_replayed_misplaced_or_unauthenticated() {
        // Forged: one flipped signature byte.
        let mut forged_state = ChangeStreamState::new(expectation()).unwrap();
        forged_state.accept(&start()).unwrap();
        let mut forged = barrier(9);
        forged.signature[0] ^= 0xff;
        assert!(forged_state
            .accept(&seal(change_stream_frame::Body::RevocationBarrier(forged)))
            .is_err());

        // Replayed from an earlier authorization generation.
        let mut replayed_state = ChangeStreamState::new(expectation()).unwrap();
        replayed_state.accept(&start()).unwrap();
        let mut replayed = barrier(9);
        replayed.revoked_authorization_generation = 3;
        replayed.effective_authorization_generation = 4;
        sign_barrier(&mut replayed);
        assert!(replayed_state
            .accept(&seal(change_stream_frame::Body::RevocationBarrier(
                replayed
            )))
            .is_err());

        // Naming a scope this stream does not run.
        let mut other_scope_state = ChangeStreamState::new(expectation()).unwrap();
        other_scope_state.accept(&start()).unwrap();
        let mut other_scope = barrier(9);
        other_scope.revoked_authorization_scope_hash = vec![6; 32];
        sign_barrier(&mut other_scope);
        assert!(other_scope_state
            .accept(&seal(change_stream_frame::Body::RevocationBarrier(
                other_scope
            )))
            .is_err());

        // No configured key: every barrier is refused.
        let mut keyless = expectation();
        keyless.revocation_barrier_key = None;
        let mut keyless_state = ChangeStreamState::new(keyless).unwrap();
        keyless_state.accept(&start()).unwrap();
        assert!(keyless_state
            .accept(&seal(change_stream_frame::Body::RevocationBarrier(
                barrier(9)
            )))
            .is_err());
    }

    #[test]
    fn nothing_may_be_delivered_after_a_barrier_in_the_same_batch() {
        for (index, following) in [
            change_stream_frame::Body::TransactionFragment(match fragment(0, true).body {
                Some(change_stream_frame::Body::TransactionFragment(value)) => value,
                _ => unreachable!(),
            }),
            change_stream_frame::Body::Heartbeat(Heartbeat {
                durable_head: Some(cursor(9)),
                server_time_unix_micros: 1,
            }),
            change_stream_frame::Body::RevocationBarrier(barrier(9)),
            // A stream error is not exempt from the rule: a batch that carries
            // a barrier and then an error never completes, so the caller would
            // be handed a retryable-looking transport failure in place of the
            // revocation.
            change_stream_frame::Body::Error(StreamError {
                code: StreamErrorCode::InternalRetryable as i32,
                message: "internal".to_owned(),
                resnapshot_required: false,
                minimum_retained_cursor: None,
                durable_head: None,
                retry_after_ms: 0,
            }),
        ]
        .into_iter()
        .enumerate()
        {
            let mut state = ChangeStreamState::new(expectation()).unwrap();
            state.accept(&start()).unwrap();
            state
                .accept(&seal(change_stream_frame::Body::RevocationBarrier(
                    barrier(9),
                )))
                .unwrap();
            assert!(
                state.accept(&seal(following)).is_err(),
                "body {index} was accepted after a barrier"
            );
        }
    }

    #[test]
    fn a_batch_end_that_does_not_stand_on_the_barrier_is_refused() {
        let mut state = ChangeStreamState::new(expectation()).unwrap();
        let frames = [
            start(),
            seal(change_stream_frame::Body::RevocationBarrier(barrier(8))),
        ];
        for frame in &frames {
            state.accept(frame).unwrap();
        }
        assert!(state
            .accept(&ended(
                &frames.iter().collect::<Vec<_>>(),
                BatchEnd {
                    through_cursor_inclusive: Some(cursor(9)),
                    reached_requested_fence: true,
                    ..BatchEnd::default()
                },
            ))
            .is_err());
    }

    fn seal(body: change_stream_frame::Body) -> ChangeStreamFrame {
        let mut frame = ChangeStreamFrame {
            protocol_version: 1,
            body: Some(body),
            body_sha256: Vec::new(),
        };
        frame.body_sha256 = change_frame_body_sha256(&frame).unwrap().to_vec();
        frame
    }

    /// The `BatchEnd` that closes a batch of exactly `previous`, in order.
    fn ended(previous: &[&ChangeStreamFrame], batch: BatchEnd) -> ChangeStreamFrame {
        let mut frames = BatchFramesDigest::new();
        for frame in previous {
            frames.push(frame.body_sha256.as_slice().try_into().unwrap());
        }
        seal(change_stream_frame::Body::BatchEnd(BatchEnd {
            frame_count: frames.frames(),
            frames_sha256: frames.finish().to_vec(),
            ..batch
        }))
    }

    fn start() -> ChangeStreamFrame {
        seal(change_stream_frame::Body::BatchStart(BatchStart {
            generation: Some(SourceGeneration {
                source_epoch: "epoch-1".to_owned(),
                publication_generation: 2,
                schema_generation: 3,
                authorization_generation: 4,
                authorization_scope_hash: vec![5; 32],
            }),
            from_cursor_exclusive: Some(cursor(0)),
            requested_fence: Some(cursor(9)),
            table_ids: vec!["public.tasks".to_owned()],
            server_time_unix_micros: 1,
            plan_id: "plan-1".to_owned(),
            table_starts: vec![TableSubscription {
                table_id: "public.tasks".to_owned(),
                shard_snapshot_cursor: Some(cursor(0)),
            }],
            stream_id: vec![1; 16],
        }))
    }

    fn fragment(index: u32, final_fragment: bool) -> ChangeStreamFrame {
        seal(change_stream_frame::Body::TransactionFragment(
            TransactionFragment {
                transaction_id: vec![1],
                transaction_cursor: Some(cursor(1)),
                fragment_index: index,
                first_fragment: index == 0,
                final_fragment,
                postgres_xid: 7,
                begin_lsn: "0/1".to_owned(),
                commit_lsn: "0/2".to_owned(),
                commit_time_unix_micros: 1,
                operations: vec![Operation {
                    table_id: "public.tasks".to_owned(),
                    kind: OperationKind::TruncateAuthorizedTable as i32,
                    primary_key: Vec::new(),
                    payload: Some(operation::Payload::Truncate(Truncate {
                        reason: "authorized reset".to_owned(),
                    })),
                }],
            },
        ))
    }

    fn whole_batch(previous: &[&ChangeStreamFrame], operations: u64) -> ChangeStreamFrame {
        ended(
            previous,
            BatchEnd {
                through_cursor_inclusive: Some(cursor(9)),
                reached_requested_fence: true,
                relevant_transaction_count: 1,
                relevant_operation_count: operations,
                ..BatchEnd::default()
            },
        )
    }

    #[test]
    fn accepts_contiguous_fragments_and_exact_fixed_fence() {
        let mut state = ChangeStreamState::new(expectation()).unwrap();
        let frames = [start(), fragment(0, false), fragment(1, true)];
        for frame in &frames {
            state.accept(frame).unwrap();
        }
        let completed = state
            .accept(&whole_batch(&frames.iter().collect::<Vec<_>>(), 2))
            .unwrap()
            .unwrap();
        assert!(completed.reached_requested_fence);
        assert!(completed.acknowledged_mutation_ids.is_empty());
        assert!(state.terminal());
    }

    #[test]
    fn reports_the_acknowledgements_its_batch_end_carries() {
        let mut state = ChangeStreamState::new(expectation()).unwrap();
        let frames = [start(), fragment(0, true)];
        for frame in &frames {
            state.accept(frame).unwrap();
        }
        let mut end = whole_batch(&frames.iter().collect::<Vec<_>>(), 1);
        if let Some(change_stream_frame::Body::BatchEnd(batch)) = &mut end.body {
            batch.acknowledged_mutation_ids = vec!["m-1".to_owned(), "m-2".to_owned()];
        }
        end.body_sha256 = change_frame_body_sha256(&end).unwrap().to_vec();
        let completed = state.accept(&end).unwrap().unwrap();
        assert_eq!(completed.acknowledged_mutation_ids, ["m-1", "m-2"]);
    }

    #[test]
    fn refuses_a_batch_end_repeating_an_acknowledgement() {
        let mut state = ChangeStreamState::new(expectation()).unwrap();
        let frames = [start(), fragment(0, true)];
        for frame in &frames {
            state.accept(frame).unwrap();
        }
        let mut end = whole_batch(&frames.iter().collect::<Vec<_>>(), 1);
        if let Some(change_stream_frame::Body::BatchEnd(batch)) = &mut end.body {
            batch.acknowledged_mutation_ids = vec!["m-1".to_owned(), "m-1".to_owned()];
        }
        end.body_sha256 = change_frame_body_sha256(&end).unwrap().to_vec();
        assert!(state.accept(&end).is_err());
        assert!(state.terminal());
    }

    #[test]
    fn refuses_a_batch_whose_frames_were_lost_reordered_or_counted_wrong() {
        let start = start();
        let first = fragment(0, false);
        let last = fragment(1, true);

        let mut lost = ChangeStreamState::new(expectation()).unwrap();
        lost.accept(&start).unwrap();
        lost.accept(&first).unwrap();
        lost.accept(&last).unwrap();
        assert!(lost.accept(&whole_batch(&[&start, &last], 2)).is_err());
        assert!(lost.terminal());
        assert!(lost.accept(&fragment(0, true)).is_err());

        let mut reordered = ChangeStreamState::new(expectation()).unwrap();
        reordered.accept(&start).unwrap();
        reordered.accept(&first).unwrap();
        reordered.accept(&last).unwrap();
        assert!(reordered
            .accept(&whole_batch(&[&start, &last, &first], 2))
            .is_err());

        let mut miscounted = ChangeStreamState::new(expectation()).unwrap();
        miscounted.accept(&start).unwrap();
        miscounted.accept(&first).unwrap();
        miscounted.accept(&last).unwrap();
        let mut end = whole_batch(&[&start, &first, &last], 2);
        if let Some(change_stream_frame::Body::BatchEnd(batch)) = &mut end.body {
            batch.frame_count = 4;
        }
        end.body_sha256 = change_frame_body_sha256(&end).unwrap().to_vec();
        assert!(miscounted.accept(&end).is_err());
    }

    #[test]
    fn poisons_on_fragment_loss_identity_change_and_interleaving() {
        let mut skipped = ChangeStreamState::new(expectation()).unwrap();
        skipped.accept(&start()).unwrap();
        assert!(skipped.accept(&fragment(1, true)).is_err());
        assert!(skipped.terminal());
        assert!(skipped.accept(&fragment(0, true)).is_err());

        let mut changed = ChangeStreamState::new(expectation()).unwrap();
        changed.accept(&start()).unwrap();
        changed.accept(&fragment(0, false)).unwrap();
        let mut second = fragment(1, true);
        if let Some(change_stream_frame::Body::TransactionFragment(fragment)) = &mut second.body {
            fragment.transaction_id = vec![2];
        }
        second.body_sha256 = change_frame_body_sha256(&second).unwrap().to_vec();
        assert!(changed.accept(&second).is_err());

        let mut interleaved = ChangeStreamState::new(expectation()).unwrap();
        interleaved.accept(&start()).unwrap();
        interleaved.accept(&fragment(0, false)).unwrap();
        assert!(interleaved
            .accept(&seal(change_stream_frame::Body::Heartbeat(Heartbeat {
                durable_head: Some(cursor(1)),
                server_time_unix_micros: 1,
            })))
            .is_err());
    }
}
