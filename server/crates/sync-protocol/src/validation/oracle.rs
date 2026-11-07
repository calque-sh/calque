//! Validators the `calque` binary never calls: change requests, change-stream
//! frames, mutation responses and logical markers. The protocol fixture tests use
//! them as oracles.

use std::collections::BTreeSet;

use super::{
    fail, require_bounded_bytes, require_client_identifier, require_nonempty_bytes,
    require_protocol_identifier, require_version, validate_mutation_cursor,
    validate_protocol_identifiers, ProtocolValidationError, MAX_BARRIER_IDENTIFIER_BYTES,
    MAX_CHANGE_AUTHORIZATION_TOKEN_BYTES, MAX_CHANGE_BATCH_BYTES, MAX_CHANGE_TRANSACTIONS,
    MAX_CLIENT_INSTANCE_ID_BYTES, MAX_CONFLICT_KIND_BYTES, MAX_CONFLICT_PAYLOAD_BYTES,
    MAX_CURSOR_TOKEN_BYTES, MAX_DEVICE_ID_BYTES, MAX_LONG_POLL_TIMEOUT_MS, MAX_MUTATION_BATCH_SIZE,
    MAX_MUTATION_FOOTPRINT_ROWS, MAX_PENDING_MUTATION_IDS, MAX_REQUEST_TABLES,
    MAX_TRANSACTION_OPERATIONS, MIN_CHANGE_BATCH_BYTES, MIN_LONG_POLL_TIMEOUT_MS,
};
use crate::{
    change::{
        self, cell_value, change_stream_frame, operation, BatchEnd, BatchStart, CellValue,
        ChangeRequest, ChangeStreamFrame, ColumnValue, Cursor as ChangeCursor, Operation,
        OperationKind, RevocationBarrier, SourceGeneration, StreamError, StreamErrorCode,
        TransactionFragment,
    },
    decode_primary_key,
    mutation::{
        MutationBatchResponse, MutationLogicalMarker, MutationOutcome, MutationOutcomeCode,
    },
    revocation::REVOCATION_BARRIER_SIGNATURE_BYTES,
};

/// A batch's stream id is a per-response nonce, not an identifier anything
/// looks up; the bound only keeps a hostile one small.
const MAX_STREAM_ID_BYTES: usize = 64;

fn require_exact_bytes(
    value: &[u8],
    expected: usize,
    path: &str,
) -> Result<(), ProtocolValidationError> {
    if value.len() != expected {
        return Err(fail(path, format!("must contain exactly {expected} bytes")));
    }
    Ok(())
}

fn validate_change_cursor(
    cursor: Option<&ChangeCursor>,
    path: &str,
) -> Result<(), ProtocolValidationError> {
    let cursor = cursor.ok_or_else(|| fail(path, "is required"))?;
    require_bounded_bytes(
        &cursor.token,
        MAX_CURSOR_TOKEN_BYTES,
        &format!("{path}.token"),
    )
}

fn validate_generation(
    generation: Option<&SourceGeneration>,
    path: &str,
) -> Result<(), ProtocolValidationError> {
    let generation = generation.ok_or_else(|| fail(path, "is required"))?;
    require_protocol_identifier(&generation.source_epoch, &format!("{path}.sourceEpoch"))?;
    require_exact_bytes(
        &generation.authorization_scope_hash,
        32,
        &format!("{path}.authorizationScopeHash"),
    )
}

fn validate_cell(cell: &CellValue, path: &str) -> Result<(), ProtocolValidationError> {
    let kind = cell
        .kind
        .as_ref()
        .ok_or_else(|| fail(format!("{path}.kind"), "is required"))?;
    match kind {
        cell_value::Kind::NullValue(value) => {
            if change::NullValue::try_from(*value) != Ok(change::NullValue::NullValue) {
                return Err(fail(
                    format!("{path}.kind.nullValue"),
                    "contains an unknown enum value",
                ));
            }
        }
        cell_value::Kind::Uuid16(value) => {
            require_exact_bytes(value, 16, &format!("{path}.kind.uuid16"))?;
        }
        cell_value::Kind::Float64Value(value) if !value.is_finite() => {
            return Err(fail(
                format!("{path}.kind.float64Value"),
                "must be finite until special float semantics are specified",
            ));
        }
        cell_value::Kind::Int64Value(_)
        | cell_value::Kind::Uint64Value(_)
        | cell_value::Kind::Float64Value(_)
        | cell_value::Kind::TextValue(_)
        | cell_value::Kind::BlobValue(_)
        | cell_value::Kind::BoolValue(_)
        | cell_value::Kind::DecimalText(_)
        | cell_value::Kind::TimestampUnixMicros(_)
        | cell_value::Kind::JsonText(_)
        | cell_value::Kind::DateIso8601(_)
        | cell_value::Kind::TimeIso8601(_)
        | cell_value::Kind::IntervalIso8601(_) => {}
    }
    Ok(())
}

fn validate_columns(columns: &[ColumnValue], path: &str) -> Result<(), ProtocolValidationError> {
    let mut column_ids = BTreeSet::new();
    for (index, column) in columns.iter().enumerate() {
        let column_path = format!("{path}[{index}]");
        if column.column_id == 0 {
            return Err(fail(format!("{column_path}.columnId"), "must not be zero"));
        }
        if !column_ids.insert(column.column_id) {
            return Err(fail(
                format!("{column_path}.columnId"),
                "must be unique in a row",
            ));
        }
        validate_cell(
            column
                .value
                .as_ref()
                .ok_or_else(|| fail(format!("{column_path}.value"), "is required"))?,
            &format!("{column_path}.value"),
        )?;
    }
    Ok(())
}

fn validate_operation(operation: &Operation, path: &str) -> Result<(), ProtocolValidationError> {
    require_protocol_identifier(&operation.table_id, &format!("{path}.tableId"))?;
    let kind = OperationKind::try_from(operation.kind).map_err(|_| {
        fail(
            format!("{path}.kind"),
            format!("unknown or unspecified value {}", operation.kind),
        )
    })?;
    if kind == OperationKind::Unspecified {
        return Err(fail(
            format!("{path}.kind"),
            "unknown or unspecified value 0",
        ));
    }
    let payload = operation
        .payload
        .as_ref()
        .ok_or_else(|| fail(format!("{path}.payload"), "is required"))?;
    let matches = matches!(
        (kind, payload),
        (OperationKind::InsertFullRow, operation::Payload::Insert(_))
            | (OperationKind::UpdatePatch, operation::Payload::Update(_))
            | (OperationKind::DeleteByKey, operation::Payload::Delete(_))
            | (
                OperationKind::TruncateAuthorizedTable,
                operation::Payload::Truncate(_)
            )
    );
    if !matches {
        return Err(fail(
            format!("{path}.payload"),
            "does not match the operation kind",
        ));
    }

    if kind == OperationKind::TruncateAuthorizedTable {
        if !operation.primary_key.is_empty() {
            return Err(fail(
                format!("{path}.primaryKey"),
                "must be empty for truncate",
            ));
        }
    } else {
        decode_primary_key(&operation.primary_key)
            .map_err(|error| fail(format!("{path}.primaryKey"), error.to_string()))?;
    }

    match payload {
        operation::Payload::Insert(insert) => {
            validate_columns(&insert.columns, &format!("{path}.insert.columns"))?;
        }
        operation::Payload::Update(update) => {
            if update.changed_columns.is_empty() {
                return Err(fail(
                    format!("{path}.update.changedColumns"),
                    "must contain at least one changed column",
                ));
            }
            validate_columns(
                &update.changed_columns,
                &format!("{path}.update.changedColumns"),
            )?;
        }
        operation::Payload::Delete(_) | operation::Payload::Truncate(_) => {}
    }
    Ok(())
}

fn validate_table_subscriptions(
    tables: &[change::TableSubscription],
    path: &str,
) -> Result<BTreeSet<String>, ProtocolValidationError> {
    if tables.is_empty() || tables.len() > MAX_REQUEST_TABLES {
        return Err(fail(
            path,
            format!("must contain 1..={MAX_REQUEST_TABLES} tables"),
        ));
    }
    let mut table_ids = BTreeSet::new();
    for (index, table) in tables.iter().enumerate() {
        let table_path = format!("{path}[{index}]");
        require_protocol_identifier(&table.table_id, &format!("{table_path}.tableId"))?;
        if !table_ids.insert(table.table_id.clone()) {
            return Err(fail(format!("{table_path}.tableId"), "must be unique"));
        }
        if table.shard_snapshot_cursor.is_some() {
            validate_change_cursor(
                table.shard_snapshot_cursor.as_ref(),
                &format!("{table_path}.shardSnapshotCursor"),
            )?;
        }
    }
    Ok(table_ids)
}

pub fn validate_change_request(request: &ChangeRequest) -> Result<(), ProtocolValidationError> {
    require_version(request.protocol_version, "changeRequest.protocolVersion")?;
    require_protocol_identifier(&request.plan_id, "changeRequest.planId")?;
    validate_change_cursor(
        request.from_cursor_exclusive.as_ref(),
        "changeRequest.fromCursorExclusive",
    )?;
    if request.through_cursor_inclusive.is_some() {
        validate_change_cursor(
            request.through_cursor_inclusive.as_ref(),
            "changeRequest.throughCursorInclusive",
        )?;
    }
    validate_table_subscriptions(&request.tables, "changeRequest.tables")?;
    require_bounded_bytes(
        &request.authorization_scope_token,
        MAX_CHANGE_AUTHORIZATION_TOKEN_BYTES,
        "changeRequest.authorizationScopeToken",
    )?;
    if !(MIN_CHANGE_BATCH_BYTES..=MAX_CHANGE_BATCH_BYTES).contains(&request.max_batch_bytes) {
        return Err(fail(
            "changeRequest.maxBatchBytes",
            format!("must be between {MIN_CHANGE_BATCH_BYTES} and {MAX_CHANGE_BATCH_BYTES}"),
        ));
    }
    if request.max_transactions == 0 || request.max_transactions > MAX_CHANGE_TRANSACTIONS {
        return Err(fail(
            "changeRequest.maxTransactions",
            format!("must be between 1 and {MAX_CHANGE_TRANSACTIONS}"),
        ));
    }
    if request.long_poll_timeout_ms != 0
        && !(MIN_LONG_POLL_TIMEOUT_MS..=MAX_LONG_POLL_TIMEOUT_MS)
            .contains(&request.long_poll_timeout_ms)
    {
        return Err(fail(
            "changeRequest.longPollTimeoutMs",
            format!(
                "must be zero or between {MIN_LONG_POLL_TIMEOUT_MS} and {MAX_LONG_POLL_TIMEOUT_MS}"
            ),
        ));
    }
    // A response stays open only as long as its wait, so a zero wait is no
    // lifetime.
    if request.stream && request.long_poll_timeout_ms == 0 {
        return Err(fail(
            "changeRequest.stream",
            "requires a non-zero longPollTimeoutMs",
        ));
    }
    require_client_identifier(
        &request.client_instance_id,
        MAX_CLIENT_INSTANCE_ID_BYTES,
        "changeRequest.clientInstanceId",
    )?;
    require_client_identifier(
        &request.device_id,
        MAX_DEVICE_ID_BYTES,
        "changeRequest.deviceId",
    )?;
    validate_protocol_identifiers(
        &request.pending_mutation_ids,
        MAX_PENDING_MUTATION_IDS,
        "changeRequest.pendingMutationIds",
    )?;
    Ok(())
}

fn validate_batch_start(batch: &BatchStart, path: &str) -> Result<(), ProtocolValidationError> {
    validate_generation(batch.generation.as_ref(), &format!("{path}.generation"))?;
    validate_change_cursor(
        batch.from_cursor_exclusive.as_ref(),
        &format!("{path}.fromCursorExclusive"),
    )?;
    if batch.requested_fence.is_some() {
        validate_change_cursor(
            batch.requested_fence.as_ref(),
            &format!("{path}.requestedFence"),
        )?;
    }
    let table_ids = validate_protocol_identifiers(
        &batch.table_ids,
        MAX_REQUEST_TABLES,
        &format!("{path}.tableIds"),
    )?;
    require_protocol_identifier(&batch.plan_id, &format!("{path}.planId"))?;
    require_bounded_bytes(
        &batch.stream_id,
        MAX_STREAM_ID_BYTES,
        &format!("{path}.streamId"),
    )?;
    let starts = validate_table_subscriptions(&batch.table_starts, &format!("{path}.tableStarts"))?;
    if table_ids != starts {
        return Err(fail(
            format!("{path}.tableStarts"),
            "must cover exactly tableIds",
        ));
    }
    Ok(())
}

fn validate_transaction_fragment(
    fragment: &TransactionFragment,
    path: &str,
) -> Result<(), ProtocolValidationError> {
    require_nonempty_bytes(&fragment.transaction_id, &format!("{path}.transactionId"))?;
    validate_change_cursor(
        fragment.transaction_cursor.as_ref(),
        &format!("{path}.transactionCursor"),
    )?;
    if fragment.postgres_xid == 0 {
        return Err(fail(format!("{path}.postgresXid"), "must not be zero"));
    }
    if fragment.begin_lsn.is_empty() {
        return Err(fail(format!("{path}.beginLsn"), "must not be empty"));
    }
    if fragment.commit_lsn.is_empty() {
        return Err(fail(format!("{path}.commitLsn"), "must not be empty"));
    }
    if fragment.first_fragment != (fragment.fragment_index == 0) {
        return Err(fail(
            format!("{path}.firstFragment"),
            "must be true exactly when fragmentIndex is zero",
        ));
    }
    if fragment.operations.len() > MAX_TRANSACTION_OPERATIONS {
        return Err(fail(
            format!("{path}.operations"),
            format!("must contain at most {MAX_TRANSACTION_OPERATIONS} operations"),
        ));
    }
    for (index, operation) in fragment.operations.iter().enumerate() {
        validate_operation(operation, &format!("{path}.operations[{index}]"))?;
    }
    if fragment.operations.is_empty() {
        return Err(fail(format!("{path}.operations"), "must not be empty"));
    }
    Ok(())
}

fn validate_batch_end(batch: &BatchEnd, path: &str) -> Result<(), ProtocolValidationError> {
    validate_change_cursor(
        batch.through_cursor_inclusive.as_ref(),
        &format!("{path}.throughCursorInclusive"),
    )?;
    validate_protocol_identifiers(
        &batch.acknowledged_mutation_ids,
        MAX_PENDING_MUTATION_IDS,
        &format!("{path}.acknowledgedMutationIds"),
    )?;
    if batch.frame_count == 0 {
        return Err(fail(
            format!("{path}.frameCount"),
            "must count at least the batch start",
        ));
    }
    require_exact_bytes(&batch.frames_sha256, 32, &format!("{path}.framesSha256"))
}

fn validate_stream_error(error: &StreamError, path: &str) -> Result<(), ProtocolValidationError> {
    let code = StreamErrorCode::try_from(error.code).map_err(|_| {
        fail(
            format!("{path}.code"),
            format!("unknown or unspecified value {}", error.code),
        )
    })?;
    if code == StreamErrorCode::Unspecified {
        return Err(fail(
            format!("{path}.code"),
            "unknown or unspecified value 0",
        ));
    }
    if error.message.is_empty() {
        return Err(fail(format!("{path}.message"), "must not be empty"));
    }
    if error.minimum_retained_cursor.is_some() {
        validate_change_cursor(
            error.minimum_retained_cursor.as_ref(),
            &format!("{path}.minimumRetainedCursor"),
        )?;
    }
    if error.durable_head.is_some() {
        validate_change_cursor(error.durable_head.as_ref(), &format!("{path}.durableHead"))?;
    }
    Ok(())
}

/// Stateless shape of one revocation barrier, before any signature check.
///
/// Nothing here is authorization: it bounds the untrusted fields so the
/// preimage the signature is checked against can be built at all, and refuses
/// the two orderings that can never be true — a barrier that retires nothing
/// (`revoked` is zero) and one whose replacement does not follow the
/// generation it retires.
fn validate_revocation_barrier(
    barrier: &RevocationBarrier,
    path: &str,
) -> Result<(), ProtocolValidationError> {
    validate_change_cursor(
        barrier.barrier_cursor.as_ref(),
        &format!("{path}.barrierCursor"),
    )?;
    require_client_identifier(
        &barrier.barrier_id,
        MAX_BARRIER_IDENTIFIER_BYTES,
        &format!("{path}.barrierId"),
    )?;
    require_client_identifier(
        &barrier.signing_key_id,
        MAX_BARRIER_IDENTIFIER_BYTES,
        &format!("{path}.signingKeyId"),
    )?;
    require_exact_bytes(
        &barrier.revoked_authorization_scope_hash,
        32,
        &format!("{path}.revokedAuthorizationScopeHash"),
    )?;
    require_exact_bytes(
        &barrier.signature,
        REVOCATION_BARRIER_SIGNATURE_BYTES,
        &format!("{path}.signature"),
    )?;
    if barrier.revoked_authorization_generation == 0 {
        return Err(fail(
            format!("{path}.revokedAuthorizationGeneration"),
            "must be greater than zero",
        ));
    }
    if barrier.effective_authorization_generation <= barrier.revoked_authorization_generation {
        return Err(fail(
            format!("{path}.effectiveAuthorizationGeneration"),
            format!(
                "is {}, which does not follow the revoked generation {}",
                barrier.effective_authorization_generation,
                barrier.revoked_authorization_generation
            ),
        ));
    }
    if barrier.issued_at_unix_micros == 0 {
        return Err(fail(
            format!("{path}.issuedAtUnixMicros"),
            "must be greater than zero",
        ));
    }
    Ok(())
}

pub fn validate_change_stream_frame(
    frame: &ChangeStreamFrame,
) -> Result<(), ProtocolValidationError> {
    require_version(frame.protocol_version, "changeStreamFrame.protocolVersion")?;
    require_exact_bytes(&frame.body_sha256, 32, "changeStreamFrame.bodySha256")?;
    match frame
        .body
        .as_ref()
        .ok_or_else(|| fail("changeStreamFrame.body", "is required"))?
    {
        change_stream_frame::Body::BatchStart(batch) => {
            validate_batch_start(batch, "changeStreamFrame.batchStart")
        }
        change_stream_frame::Body::TransactionFragment(fragment) => {
            validate_transaction_fragment(fragment, "changeStreamFrame.transactionFragment")
        }
        change_stream_frame::Body::BatchEnd(batch) => {
            validate_batch_end(batch, "changeStreamFrame.batchEnd")
        }
        change_stream_frame::Body::Heartbeat(heartbeat) => validate_change_cursor(
            heartbeat.durable_head.as_ref(),
            "changeStreamFrame.heartbeat.durableHead",
        ),
        change_stream_frame::Body::Error(error) => {
            validate_stream_error(error, "changeStreamFrame.error")
        }
        change_stream_frame::Body::RevocationBarrier(barrier) => {
            validate_revocation_barrier(barrier, "changeStreamFrame.revocationBarrier")
        }
    }
}

fn validate_mutation_outcome(
    outcome: &MutationOutcome,
    path: &str,
) -> Result<(), ProtocolValidationError> {
    require_protocol_identifier(&outcome.mutation_id, &format!("{path}.mutationId"))?;
    let code = MutationOutcomeCode::try_from(outcome.code).map_err(|_| {
        fail(
            format!("{path}.code"),
            format!("unknown or unspecified value {}", outcome.code),
        )
    })?;
    if code == MutationOutcomeCode::Unspecified {
        return Err(fail(
            format!("{path}.code"),
            "unknown or unspecified value 0",
        ));
    }
    if outcome.message.is_empty() {
        return Err(fail(format!("{path}.message"), "must not be empty"));
    }
    let accepted = matches!(
        code,
        MutationOutcomeCode::AcceptedAwaitingCanonicalEcho
            | MutationOutcomeCode::AlreadyAcceptedAwaitingCanonicalEcho
            | MutationOutcomeCode::AlreadyCanonical
    );
    if accepted {
        require_exact_bytes(
            &outcome.server_transaction_id,
            16,
            &format!("{path}.serverTransactionId"),
        )?;
    } else if !outcome.server_transaction_id.is_empty() {
        return Err(fail(
            format!("{path}.serverTransactionId"),
            "is only legal for an accepted outcome",
        ));
    }
    if code == MutationOutcomeCode::AlreadyCanonical {
        validate_mutation_cursor(
            outcome.observed_canonical_cursor.as_ref(),
            &format!("{path}.observedCanonicalCursor"),
        )?;
    } else if outcome.observed_canonical_cursor.is_some() {
        return Err(fail(
            format!("{path}.observedCanonicalCursor"),
            "is only legal when already canonical",
        ));
    }
    if code == MutationOutcomeCode::Conflict {
        let conflict = outcome
            .conflict
            .as_ref()
            .ok_or_else(|| fail(format!("{path}.conflict"), "is required"))?;
        require_client_identifier(
            &conflict.kind,
            MAX_CONFLICT_KIND_BYTES,
            &format!("{path}.conflict.kind"),
        )?;
        if conflict.application_payload.len() > MAX_CONFLICT_PAYLOAD_BYTES {
            return Err(fail(
                format!("{path}.conflict.applicationPayload"),
                format!("exceeds limit {MAX_CONFLICT_PAYLOAD_BYTES}"),
            ));
        }
        if conflict.rows.len() > MAX_MUTATION_FOOTPRINT_ROWS {
            return Err(fail(
                format!("{path}.conflict.rows"),
                format!("must contain at most {MAX_MUTATION_FOOTPRINT_ROWS} rows"),
            ));
        }
        let mut rows = BTreeSet::new();
        for (index, row) in conflict.rows.iter().enumerate() {
            let row_path = format!("{path}.conflict.rows[{index}]");
            require_protocol_identifier(&row.table_id, &format!("{row_path}.tableId"))?;
            decode_primary_key(&row.primary_key)
                .map_err(|error| fail(format!("{row_path}.primaryKey"), error.to_string()))?;
            require_nonempty_bytes(&row.encoded_full_row, &format!("{row_path}.encodedFullRow"))?;
            require_nonempty_bytes(&row.version, &format!("{row_path}.version"))?;
            if !rows.insert((row.table_id.clone(), row.primary_key.clone())) {
                return Err(fail(row_path, "must be unique by table and primary key"));
            }
        }
    } else if outcome.conflict.is_some() {
        return Err(fail(
            format!("{path}.conflict"),
            "is only legal for a conflict outcome",
        ));
    }
    if code == MutationOutcomeCode::RetryableFailure {
        if outcome.retry_after_ms == 0 {
            return Err(fail(
                format!("{path}.retryAfterMs"),
                "must be greater than zero when retryable",
            ));
        }
    } else if outcome.retry_after_ms != 0 {
        return Err(fail(
            format!("{path}.retryAfterMs"),
            "is only legal for a retryable failure",
        ));
    }
    Ok(())
}

pub fn validate_mutation_batch_response(
    response: &MutationBatchResponse,
) -> Result<(), ProtocolValidationError> {
    require_version(
        response.protocol_version,
        "mutationBatchResponse.protocolVersion",
    )?;
    if response.outcomes.is_empty() || response.outcomes.len() > MAX_MUTATION_BATCH_SIZE {
        return Err(fail(
            "mutationBatchResponse.outcomes",
            format!("must contain 1..={MAX_MUTATION_BATCH_SIZE} outcomes"),
        ));
    }
    if response.server_time_unix_micros == 0 {
        return Err(fail(
            "mutationBatchResponse.serverTimeUnixMicros",
            "must be greater than zero",
        ));
    }
    let mut mutation_ids = BTreeSet::new();
    for (index, outcome) in response.outcomes.iter().enumerate() {
        let path = format!("mutationBatchResponse.outcomes[{index}]");
        validate_mutation_outcome(outcome, &path)?;
        if !mutation_ids.insert(outcome.mutation_id.clone()) {
            return Err(fail(format!("{path}.mutationId"), "must be unique"));
        }
    }
    Ok(())
}

pub fn validate_mutation_logical_marker(
    marker: &MutationLogicalMarker,
) -> Result<(), ProtocolValidationError> {
    require_version(
        marker.protocol_version,
        "mutationLogicalMarker.protocolVersion",
    )?;
    if marker.mutation_ids.is_empty() {
        return Err(fail(
            "mutationLogicalMarker.mutationIds",
            "must not be empty",
        ));
    }
    validate_protocol_identifiers(
        &marker.mutation_ids,
        MAX_PENDING_MUTATION_IDS,
        "mutationLogicalMarker.mutationIds",
    )?;
    require_exact_bytes(
        &marker.server_transaction_id,
        16,
        "mutationLogicalMarker.serverTransactionId",
    )
}
