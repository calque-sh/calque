use std::collections::BTreeSet;

use thiserror::Error;

use crate::{
    decode_primary_key,
    mutation::{Cursor as MutationCursor, MutationBatchRequest, MutationEnvelope},
};

pub const PROTOCOL_VERSION: u32 = 1;
pub(crate) const MAX_PROTOCOL_IDENTIFIER_BYTES: usize = 512;
pub const MAX_CURSOR_TOKEN_BYTES: usize = 2_048;
pub const MAX_REQUEST_TABLES: usize = 1_024;
pub const MAX_PENDING_MUTATION_IDS: usize = 256;
pub const MIN_CHANGE_BATCH_BYTES: u64 = 1_024;
pub const MAX_CHANGE_BATCH_BYTES: u64 = 16 * 1_024 * 1_024;
pub const MAX_CHANGE_TRANSACTIONS: u32 = 10_000;
pub const MIN_LONG_POLL_TIMEOUT_MS: u32 = 1_000;
pub const MAX_LONG_POLL_TIMEOUT_MS: u32 = 120_000;
pub const MAX_CHANGE_AUTHORIZATION_TOKEN_BYTES: usize = 8 * 1_024;
pub const MAX_CLIENT_INSTANCE_ID_BYTES: usize = 1_024;
pub const MAX_DEVICE_ID_BYTES: usize = 1_024;
pub const MAX_TRANSACTION_OPERATIONS: usize = 100_000;
pub const MAX_MUTATION_BATCH_SIZE: usize = 100;
pub const MAX_MUTATION_CANONICAL_INPUT_BYTES: usize = 1_024 * 1_024;
pub const MAX_MUTATION_BATCH_INPUT_BYTES: usize = 4 * 1_024 * 1_024;
pub const MAX_MUTATION_SCOPE_TOKEN_BYTES: usize = 1_024 * 1_024;
pub const MAX_EXPECTED_VERSIONS: usize = 1_024;
pub const MAX_MUTATION_FOOTPRINT_ROWS: usize = 1_024;
pub const MAX_MUTATION_FOOTPRINT_TABLES: usize = 256;
pub const MAX_COMMAND_NAME_BYTES: usize = 128;
pub const MAX_CONFLICT_KIND_BYTES: usize = 128;
pub const MAX_CONFLICT_PAYLOAD_BYTES: usize = 8 * 1_024;
pub const MAX_BARRIER_IDENTIFIER_BYTES: usize = 128;

#[cfg(any(test, feature = "test-support"))]
mod oracle;
#[cfg(any(test, feature = "test-support"))]
pub use oracle::{
    validate_change_request, validate_change_stream_frame, validate_mutation_batch_response,
    validate_mutation_logical_marker,
};

#[derive(Debug, Clone, PartialEq, Eq, Error)]
#[error("{path}: {message}")]
pub struct ProtocolValidationError {
    pub path: String,
    pub message: String,
}

fn fail(path: impl Into<String>, message: impl Into<String>) -> ProtocolValidationError {
    ProtocolValidationError {
        path: path.into(),
        message: message.into(),
    }
}

fn require_version(value: u32, path: &str) -> Result<(), ProtocolValidationError> {
    if value == PROTOCOL_VERSION {
        Ok(())
    } else {
        Err(fail(path, format!("unsupported protocol version {value}")))
    }
}

fn require_nonempty_bytes(value: &[u8], path: &str) -> Result<(), ProtocolValidationError> {
    if value.is_empty() {
        Err(fail(path, "must not be empty"))
    } else {
        Ok(())
    }
}

fn require_bounded_bytes(
    value: &[u8],
    maximum: usize,
    path: &str,
) -> Result<(), ProtocolValidationError> {
    require_nonempty_bytes(value, path)?;
    if value.len() > maximum {
        return Err(fail(
            path,
            format!("has {} bytes, exceeding limit {maximum}", value.len()),
        ));
    }
    Ok(())
}

fn require_client_identifier(
    value: &str,
    maximum: usize,
    path: &str,
) -> Result<(), ProtocolValidationError> {
    if value.is_empty() {
        return Err(fail(path, "must not be empty"));
    }
    if value.len() > maximum {
        return Err(fail(
            path,
            format!("has {} bytes, exceeding limit {maximum}", value.len()),
        ));
    }
    if value.chars().any(char::is_control) {
        return Err(fail(path, "must not contain control characters"));
    }
    Ok(())
}

fn require_protocol_identifier(value: &str, path: &str) -> Result<(), ProtocolValidationError> {
    if value.is_empty() {
        return Err(fail(path, "must not be empty"));
    }
    if value.len() > MAX_PROTOCOL_IDENTIFIER_BYTES {
        return Err(fail(
            path,
            format!(
                "has {} bytes, exceeding limit {MAX_PROTOCOL_IDENTIFIER_BYTES}",
                value.len()
            ),
        ));
    }
    if !value.as_bytes()[0].is_ascii_alphanumeric() {
        return Err(fail(
            path,
            "must begin with an ASCII alphanumeric character",
        ));
    }
    if value.bytes().any(|byte| {
        !(byte.is_ascii_alphanumeric() || matches!(byte, b'.' | b'_' | b':' | b'/' | b'-'))
    }) {
        return Err(fail(path, "contains an unsupported character"));
    }
    Ok(())
}

fn require_command_name(value: &str, path: &str) -> Result<(), ProtocolValidationError> {
    if value.is_empty() || value.len() > MAX_COMMAND_NAME_BYTES {
        return Err(fail(
            path,
            format!("must contain 1..={MAX_COMMAND_NAME_BYTES} bytes"),
        ));
    }
    if !value.as_bytes()[0].is_ascii_alphanumeric()
        || value.bytes().any(|byte| {
            !(byte.is_ascii_alphanumeric() || matches!(byte, b'.' | b'_' | b':' | b'/' | b'-'))
        })
    {
        return Err(fail(path, "contains an unsupported command-name character"));
    }
    Ok(())
}

fn validate_mutation_cursor(
    cursor: Option<&MutationCursor>,
    path: &str,
) -> Result<(), ProtocolValidationError> {
    let cursor = cursor.ok_or_else(|| fail(path, "is required"))?;
    require_bounded_bytes(
        &cursor.token,
        MAX_CURSOR_TOKEN_BYTES,
        &format!("{path}.token"),
    )
}

fn validate_protocol_identifiers(
    values: &[String],
    maximum: usize,
    path: &str,
) -> Result<BTreeSet<String>, ProtocolValidationError> {
    if values.len() > maximum {
        return Err(fail(path, format!("must contain at most {maximum} values")));
    }
    let mut seen = BTreeSet::new();
    for (index, value) in values.iter().enumerate() {
        require_protocol_identifier(value, &format!("{path}[{index}]"))?;
        if !seen.insert(value.clone()) {
            return Err(fail(format!("{path}[{index}]"), "must be unique"));
        }
    }
    Ok(seen)
}

fn validate_mutation_envelope(
    mutation: &MutationEnvelope,
    path: &str,
) -> Result<(), ProtocolValidationError> {
    require_protocol_identifier(&mutation.mutation_id, &format!("{path}.mutationId"))?;
    require_command_name(&mutation.command_name, &format!("{path}.commandName"))?;
    if mutation.command_version == 0 {
        return Err(fail(
            format!("{path}.commandVersion"),
            "must be greater than zero",
        ));
    }
    require_bounded_bytes(
        &mutation.canonical_input,
        MAX_MUTATION_CANONICAL_INPUT_BYTES,
        &format!("{path}.canonicalInput"),
    )?;
    validate_mutation_cursor(mutation.base_cursor.as_ref(), &format!("{path}.baseCursor"))?;
    if mutation.created_at_unix_micros == 0 {
        return Err(fail(
            format!("{path}.createdAtUnixMicros"),
            "must be greater than zero",
        ));
    }
    if mutation.expected_versions.len() > MAX_EXPECTED_VERSIONS {
        return Err(fail(
            format!("{path}.expectedVersions"),
            format!("must contain at most {MAX_EXPECTED_VERSIONS} values"),
        ));
    }
    let mut expected_keys = BTreeSet::new();
    for (index, expected) in mutation.expected_versions.iter().enumerate() {
        let expected_path = format!("{path}.expectedVersions[{index}]");
        require_protocol_identifier(&expected.table_id, &format!("{expected_path}.tableId"))?;
        decode_primary_key(&expected.primary_key)
            .map_err(|error| fail(format!("{expected_path}.primaryKey"), error.to_string()))?;
        require_nonempty_bytes(
            &expected.expected_version,
            &format!("{expected_path}.expectedVersion"),
        )?;
        if !expected_keys.insert((expected.table_id.clone(), expected.primary_key.clone())) {
            return Err(fail(
                expected_path,
                "must be unique by table and primary key",
            ));
        }
    }

    let footprint = mutation
        .declared_footprint
        .as_ref()
        .ok_or_else(|| fail(format!("{path}.declaredFootprint"), "is required"))?;
    if footprint.rows.len() > MAX_MUTATION_FOOTPRINT_ROWS {
        return Err(fail(
            format!("{path}.declaredFootprint.rows"),
            format!("must contain at most {MAX_MUTATION_FOOTPRINT_ROWS} rows"),
        ));
    }
    if footprint.table_ids.is_empty() || footprint.table_ids.len() > MAX_MUTATION_FOOTPRINT_TABLES {
        return Err(fail(
            format!("{path}.declaredFootprint.tableIds"),
            format!("must contain 1..={MAX_MUTATION_FOOTPRINT_TABLES} tables"),
        ));
    }
    let table_ids = validate_protocol_identifiers(
        &footprint.table_ids,
        MAX_MUTATION_FOOTPRINT_TABLES,
        &format!("{path}.declaredFootprint.tableIds"),
    )?;
    let mut rows = BTreeSet::new();
    for (index, row) in footprint.rows.iter().enumerate() {
        let row_path = format!("{path}.declaredFootprint.rows[{index}]");
        require_protocol_identifier(&row.table_id, &format!("{row_path}.tableId"))?;
        if !table_ids.contains(&row.table_id) {
            return Err(fail(
                format!("{row_path}.tableId"),
                "must be included in declaredFootprint.tableIds",
            ));
        }
        decode_primary_key(&row.primary_key)
            .map_err(|error| fail(format!("{row_path}.primaryKey"), error.to_string()))?;
        if !rows.insert((row.table_id.clone(), row.primary_key.clone())) {
            return Err(fail(row_path, "must be unique by table and primary key"));
        }
    }
    Ok(())
}

pub fn validate_mutation_batch_request(
    request: &MutationBatchRequest,
) -> Result<(), ProtocolValidationError> {
    require_version(
        request.protocol_version,
        "mutationBatchRequest.protocolVersion",
    )?;
    require_client_identifier(
        &request.client_instance_id,
        MAX_CLIENT_INSTANCE_ID_BYTES,
        "mutationBatchRequest.clientInstanceId",
    )?;
    require_client_identifier(
        &request.device_id,
        MAX_DEVICE_ID_BYTES,
        "mutationBatchRequest.deviceId",
    )?;
    require_bounded_bytes(
        &request.authorization_scope_token,
        MAX_MUTATION_SCOPE_TOKEN_BYTES,
        "mutationBatchRequest.authorizationScopeToken",
    )?;
    if request.mutations.is_empty() || request.mutations.len() > MAX_MUTATION_BATCH_SIZE {
        return Err(fail(
            "mutationBatchRequest.mutations",
            format!("must contain 1..={MAX_MUTATION_BATCH_SIZE} mutations"),
        ));
    }
    let mut total_input_bytes = 0_usize;
    let mut mutation_ids = BTreeSet::new();
    for (index, mutation) in request.mutations.iter().enumerate() {
        let path = format!("mutationBatchRequest.mutations[{index}]");
        validate_mutation_envelope(mutation, &path)?;
        total_input_bytes = total_input_bytes
            .checked_add(mutation.canonical_input.len())
            .ok_or_else(|| fail("mutationBatchRequest.mutations", "input size overflow"))?;
        if total_input_bytes > MAX_MUTATION_BATCH_INPUT_BYTES {
            return Err(fail(
                "mutationBatchRequest.mutations",
                format!("canonical input exceeds aggregate limit {MAX_MUTATION_BATCH_INPUT_BYTES}"),
            ));
        }
        if !mutation_ids.insert(mutation.mutation_id.clone()) {
            return Err(fail(format!("{path}.mutationId"), "must be unique"));
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        change::{
            self, cell_value, change_stream_frame, operation, BatchEnd, BatchStart, CellValue,
            ChangeRequest, ChangeStreamFrame, ColumnValue, Cursor as ChangeCursor, FullRow,
            Heartbeat, Operation, OperationKind, PrimaryKey, RowPatch, SourceGeneration,
            StreamError, TableSubscription, TransactionFragment,
        },
        encode_primary_key,
        mutation::{
            AffectedRow, CanonicalRow, Conflict, ExpectedVersion, MutationBatchResponse,
            MutationFootprint, MutationLogicalMarker, MutationOutcome,
        },
    };

    fn change_cursor() -> ChangeCursor {
        ChangeCursor { token: vec![1] }
    }

    fn mutation_cursor() -> MutationCursor {
        MutationCursor { token: vec![1] }
    }

    fn primary_key() -> Vec<u8> {
        encode_primary_key(&PrimaryKey {
            codec_version: 1,
            parts: vec![ColumnValue {
                column_id: 1,
                value: Some(CellValue {
                    kind: Some(cell_value::Kind::Int64Value(1)),
                }),
            }],
        })
        .unwrap()
    }

    fn frame(body: change_stream_frame::Body) -> ChangeStreamFrame {
        ChangeStreamFrame {
            protocol_version: 1,
            body: Some(body),
            body_sha256: vec![0; 32],
        }
    }

    fn generation() -> SourceGeneration {
        SourceGeneration {
            source_epoch: "epoch-1".to_owned(),
            publication_generation: 1,
            schema_generation: 1,
            authorization_generation: 0,
            authorization_scope_hash: vec![0; 32],
        }
    }

    fn column(column_id: u32, kind: cell_value::Kind) -> ColumnValue {
        ColumnValue {
            column_id,
            value: Some(CellValue { kind: Some(kind) }),
        }
    }

    fn operation(kind: OperationKind, payload: operation::Payload) -> Operation {
        Operation {
            table_id: "public.tasks".to_owned(),
            kind: kind as i32,
            primary_key: if kind == OperationKind::TruncateAuthorizedTable {
                Vec::new()
            } else {
                primary_key()
            },
            payload: Some(payload),
        }
    }

    fn fragment(operations: Vec<Operation>) -> TransactionFragment {
        TransactionFragment {
            transaction_id: vec![1],
            transaction_cursor: Some(change_cursor()),
            fragment_index: 0,
            first_fragment: true,
            final_fragment: true,
            postgres_xid: 1,
            begin_lsn: "0/1".to_owned(),
            commit_lsn: "0/2".to_owned(),
            commit_time_unix_micros: 1,
            operations,
        }
    }

    #[test]
    fn accepts_every_change_body_operation_and_cell_kind() {
        let cells = vec![
            column(1, cell_value::Kind::NullValue(0)),
            column(2, cell_value::Kind::Int64Value(-1)),
            column(3, cell_value::Kind::Uint64Value(1)),
            column(4, cell_value::Kind::Float64Value(1.5)),
            column(5, cell_value::Kind::TextValue("text".to_owned())),
            column(6, cell_value::Kind::BlobValue(vec![1])),
            column(7, cell_value::Kind::BoolValue(true)),
            column(8, cell_value::Kind::DecimalText("1.0".to_owned())),
            column(9, cell_value::Kind::TimestampUnixMicros(1)),
            column(10, cell_value::Kind::Uuid16(vec![1; 16])),
            column(11, cell_value::Kind::JsonText("{}".to_owned())),
            column(12, cell_value::Kind::DateIso8601("2026-07-14".to_owned())),
            column(13, cell_value::Kind::TimeIso8601("12:00:00".to_owned())),
            column(14, cell_value::Kind::IntervalIso8601("PT1S".to_owned())),
        ];
        let operations = vec![
            operation(
                OperationKind::InsertFullRow,
                operation::Payload::Insert(FullRow { columns: cells }),
            ),
            operation(
                OperationKind::UpdatePatch,
                operation::Payload::Update(RowPatch {
                    changed_columns: vec![column(1, cell_value::Kind::TextValue("next".into()))],
                }),
            ),
            operation(
                OperationKind::DeleteByKey,
                operation::Payload::Delete(change::Delete {}),
            ),
            operation(
                OperationKind::TruncateAuthorizedTable,
                operation::Payload::Truncate(change::Truncate {
                    reason: "authorized reset".to_owned(),
                }),
            ),
        ];
        validate_change_stream_frame(&frame(change_stream_frame::Body::TransactionFragment(
            fragment(operations),
        )))
        .unwrap();

        let start = BatchStart {
            generation: Some(generation()),
            from_cursor_exclusive: Some(change_cursor()),
            requested_fence: None,
            table_ids: vec!["public.tasks".to_owned()],
            server_time_unix_micros: 1,
            plan_id: "plan-1".to_owned(),
            table_starts: vec![TableSubscription {
                table_id: "public.tasks".to_owned(),
                shard_snapshot_cursor: None,
            }],
            stream_id: vec![1; 16],
        };
        validate_change_stream_frame(&frame(change_stream_frame::Body::BatchStart(start))).unwrap();
        validate_change_stream_frame(&frame(change_stream_frame::Body::BatchEnd(BatchEnd {
            through_cursor_inclusive: Some(change_cursor()),
            acknowledged_mutation_ids: vec!["mutation-1".to_owned()],
            frame_count: 1,
            frames_sha256: vec![0; 32],
            ..BatchEnd::default()
        })))
        .unwrap();
        validate_change_stream_frame(&frame(change_stream_frame::Body::Heartbeat(Heartbeat {
            durable_head: Some(change_cursor()),
            server_time_unix_micros: 1,
        })))
        .unwrap();

        for code in 1..=9 {
            validate_change_stream_frame(&frame(change_stream_frame::Body::Error(StreamError {
                code,
                message: "error".to_owned(),
                ..StreamError::default()
            })))
            .unwrap();
        }
    }

    #[test]
    fn rejects_unknown_enums_missing_fields_duplicates_and_bounds() {
        let mut unknown_operation = operation(
            OperationKind::DeleteByKey,
            operation::Payload::Delete(change::Delete {}),
        );
        unknown_operation.kind = 99;
        assert!(validate_change_stream_frame(&frame(
            change_stream_frame::Body::TransactionFragment(fragment(vec![unknown_operation]))
        ))
        .unwrap_err()
        .path
        .ends_with("kind"));

        let unknown_error = frame(change_stream_frame::Body::Error(StreamError {
            code: 99,
            message: "error".to_owned(),
            ..StreamError::default()
        }));
        assert!(validate_change_stream_frame(&unknown_error).is_err());

        let bad_null = operation(
            OperationKind::InsertFullRow,
            operation::Payload::Insert(FullRow {
                columns: vec![column(1, cell_value::Kind::NullValue(99))],
            }),
        );
        assert!(validate_change_stream_frame(&frame(
            change_stream_frame::Body::TransactionFragment(fragment(vec![bad_null]))
        ))
        .is_err());

        assert!(validate_change_stream_frame(&ChangeStreamFrame {
            protocol_version: 1,
            body_sha256: vec![0; 32],
            ..ChangeStreamFrame::default()
        })
        .is_err());

        assert_eq!(
            validate_change_stream_frame(&frame(change_stream_frame::Body::TransactionFragment(
                fragment(Vec::new())
            )))
            .unwrap_err()
            .path,
            "changeStreamFrame.transactionFragment.operations"
        );
        let end = BatchEnd {
            through_cursor_inclusive: Some(change_cursor()),
            frame_count: 1,
            frames_sha256: vec![0; 32],
            ..BatchEnd::default()
        };
        for (batch, path) in [
            (
                BatchEnd {
                    acknowledged_mutation_ids: vec!["m-1".to_owned(), "m-1".to_owned()],
                    ..end.clone()
                },
                "changeStreamFrame.batchEnd.acknowledgedMutationIds[1]",
            ),
            (
                BatchEnd {
                    frame_count: 0,
                    ..end.clone()
                },
                "changeStreamFrame.batchEnd.frameCount",
            ),
            (
                BatchEnd {
                    frames_sha256: vec![0; 31],
                    ..end.clone()
                },
                "changeStreamFrame.batchEnd.framesSha256",
            ),
        ] {
            assert_eq!(
                validate_change_stream_frame(&frame(change_stream_frame::Body::BatchEnd(batch)))
                    .unwrap_err()
                    .path,
                path
            );
        }

        let duplicate_columns = operation(
            OperationKind::InsertFullRow,
            operation::Payload::Insert(FullRow {
                columns: vec![
                    column(1, cell_value::Kind::BoolValue(true)),
                    column(1, cell_value::Kind::BoolValue(false)),
                ],
            }),
        );
        assert!(validate_change_stream_frame(&frame(
            change_stream_frame::Body::TransactionFragment(fragment(vec![duplicate_columns]))
        ))
        .is_err());

        let request = ChangeRequest {
            protocol_version: 1,
            from_cursor_exclusive: Some(change_cursor()),
            tables: (0..=MAX_REQUEST_TABLES)
                .map(|index| TableSubscription {
                    table_id: format!("public.table_{index}"),
                    shard_snapshot_cursor: None,
                })
                .collect(),
            authorization_scope_token: vec![1],
            max_batch_bytes: MIN_CHANGE_BATCH_BYTES,
            max_transactions: 1,
            client_instance_id: "client-1".to_owned(),
            plan_id: "plan-1".to_owned(),
            device_id: "device-1".to_owned(),
            ..ChangeRequest::default()
        };
        assert!(validate_change_request(&request).is_err());
    }

    #[test]
    fn a_stream_is_admitted_only_with_a_wait() {
        let live = ChangeRequest {
            protocol_version: 1,
            from_cursor_exclusive: Some(change_cursor()),
            tables: vec![TableSubscription {
                table_id: "public.tasks".to_owned(),
                shard_snapshot_cursor: None,
            }],
            authorization_scope_token: vec![1],
            max_batch_bytes: MIN_CHANGE_BATCH_BYTES,
            max_transactions: 1,
            long_poll_timeout_ms: MIN_LONG_POLL_TIMEOUT_MS,
            client_instance_id: "client-1".to_owned(),
            plan_id: "plan-1".to_owned(),
            device_id: "device-1".to_owned(),
            stream: true,
            ..ChangeRequest::default()
        };
        validate_change_request(&live).unwrap();
        validate_change_request(&ChangeRequest {
            through_cursor_inclusive: Some(change_cursor()),
            ..live.clone()
        })
        .unwrap();

        let immediate = ChangeRequest {
            long_poll_timeout_ms: 0,
            ..live
        };
        let error = validate_change_request(&immediate).unwrap_err();
        assert_eq!(error.path, "changeRequest.stream");
    }

    fn mutation_envelope(id: &str) -> MutationEnvelope {
        MutationEnvelope {
            mutation_id: id.to_owned(),
            command_name: "tasks.rename".to_owned(),
            command_version: 1,
            canonical_input: vec![1],
            base_cursor: Some(mutation_cursor()),
            created_at_unix_micros: 1,
            expected_versions: vec![ExpectedVersion {
                table_id: "public.tasks".to_owned(),
                primary_key: primary_key(),
                expected_version: vec![1],
            }],
            declared_footprint: Some(MutationFootprint {
                rows: vec![AffectedRow {
                    table_id: "public.tasks".to_owned(),
                    primary_key: primary_key(),
                }],
                table_ids: vec!["public.tasks".to_owned()],
            }),
        }
    }

    #[test]
    fn accepts_mutation_request_marker_and_every_outcome() {
        validate_mutation_batch_request(&MutationBatchRequest {
            protocol_version: 1,
            client_instance_id: "client-1".to_owned(),
            device_id: "device-1".to_owned(),
            authorization_scope_token: vec![1],
            mutations: vec![mutation_envelope("mutation-1")],
        })
        .unwrap();
        validate_mutation_logical_marker(&MutationLogicalMarker {
            protocol_version: 1,
            mutation_ids: vec!["mutation-1".to_owned()],
            server_transaction_id: vec![1; 16],
        })
        .unwrap();

        let outcomes = (1..=8)
            .map(|code| MutationOutcome {
                mutation_id: format!("mutation-{code}"),
                code,
                message: "outcome".to_owned(),
                server_transaction_id: if code <= 3 { vec![1; 16] } else { Vec::new() },
                observed_canonical_cursor: (code == 3).then(mutation_cursor),
                conflict: (code == 6).then(|| Conflict {
                    kind: "version".to_owned(),
                    rows: vec![CanonicalRow {
                        table_id: "public.tasks".to_owned(),
                        primary_key: primary_key(),
                        encoded_full_row: vec![1],
                        version: vec![1],
                    }],
                    application_payload: vec![1],
                }),
                retry_after_ms: u64::from(code == 8),
            })
            .collect();
        validate_mutation_batch_response(&MutationBatchResponse {
            protocol_version: 1,
            outcomes,
            server_time_unix_micros: 1,
        })
        .unwrap();
    }

    #[test]
    fn rejects_mutation_unknown_outcomes_required_fields_duplicates_and_bounds() {
        let mut missing_footprint = mutation_envelope("mutation-1");
        missing_footprint.declared_footprint = None;
        assert!(validate_mutation_batch_request(&MutationBatchRequest {
            protocol_version: 1,
            client_instance_id: "client-1".to_owned(),
            device_id: "device-1".to_owned(),
            authorization_scope_token: vec![1],
            mutations: vec![missing_footprint],
        })
        .is_err());

        let duplicate = mutation_envelope("mutation-1");
        assert!(validate_mutation_batch_request(&MutationBatchRequest {
            protocol_version: 1,
            client_instance_id: "client-1".to_owned(),
            device_id: "device-1".to_owned(),
            authorization_scope_token: vec![1],
            mutations: vec![duplicate.clone(), duplicate],
        })
        .is_err());

        let invalid = MutationOutcome {
            mutation_id: "mutation-1".to_owned(),
            code: 99,
            message: "invalid".to_owned(),
            ..MutationOutcome::default()
        };
        assert!(validate_mutation_batch_response(&MutationBatchResponse {
            protocol_version: 1,
            outcomes: vec![invalid],
            server_time_unix_micros: 1,
        })
        .is_err());

        let too_many = (0..=MAX_MUTATION_BATCH_SIZE)
            .map(|index| mutation_envelope(&format!("mutation-{index}")))
            .collect();
        assert!(validate_mutation_batch_request(&MutationBatchRequest {
            protocol_version: 1,
            client_instance_id: "client-1".to_owned(),
            device_id: "device-1".to_owned(),
            authorization_scope_token: vec![1],
            mutations: too_many,
        })
        .is_err());
    }
}
