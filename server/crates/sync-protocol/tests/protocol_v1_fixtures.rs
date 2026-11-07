use std::io::Cursor;

use prost::Message;
use sync_protocol::{
    change::{change_stream_frame, operation, ChangeRequest, ChangeStreamFrame, OperationKind},
    decode_primary_key,
    mutation::{MutationBatchRequest, MutationBatchResponse, MutationLogicalMarker},
    validate_change_request, validate_change_stream_frame, validate_mutation_batch_request,
    validate_mutation_batch_response, validate_mutation_logical_marker,
    verify_change_frame_body_sha256, ChangeStreamExpectation, ChangeStreamState,
    ExpectedSourceGeneration, ExpectedTableStart, FrameDecodeError, FrameDecoder,
};

fn fixture(name: &str) -> Vec<u8> {
    let text = match name {
        "change-request.frame.hex" => include_str!(
            "../../../../spec/fixtures/protocol-v1/typescript/change-request.frame.hex"
        ),
        "change-stream.frame.hex" => {
            include_str!("../../../../spec/fixtures/protocol-v1/typescript/change-stream.frame.hex")
        }
        "mutation-request.frame.hex" => include_str!(
            "../../../../spec/fixtures/protocol-v1/typescript/mutation-request.frame.hex"
        ),
        "mutation-response.frame.hex" => include_str!(
            "../../../../spec/fixtures/protocol-v1/typescript/mutation-response.frame.hex"
        ),
        "mutation-marker.frame.hex" => include_str!(
            "../../../../spec/fixtures/protocol-v1/typescript/mutation-marker.frame.hex"
        ),
        "primary-key.hex" => {
            include_str!("../../../../spec/fixtures/protocol-v1/typescript/primary-key.hex")
        }
        "primary-key-text.hex" => {
            include_str!("../../../../spec/fixtures/protocol-v1/typescript/primary-key-text.hex")
        }
        _ => panic!("unknown fixture {name}"),
    };
    let text = text.trim();
    assert!(!text.is_empty());
    assert_eq!(text.len() % 2, 0);
    assert!(text
        .bytes()
        .all(|byte| byte.is_ascii_hexdigit() && !byte.is_ascii_uppercase()));
    hex::decode(text).expect("fixture contains valid canonical hex")
}

fn decode_all<T>(name: &str) -> Vec<T>
where
    T: Message + Default,
{
    let mut decoder = FrameDecoder::new(Cursor::new(fixture(name)));
    let mut messages = Vec::new();
    while let Some(message) = decoder.read_message::<T>().expect("fixture frame decodes") {
        messages.push(message);
    }
    messages
}

fn invalid_fixture(name: &str) -> Vec<u8> {
    let text = match name {
        "framing-truncated-prefix.hex" => include_str!(
            "../../../../spec/fixtures/protocol-v1/typescript/invalid/framing-truncated-prefix.hex"
        ),
        "framing-oversized.hex" => include_str!(
            "../../../../spec/fixtures/protocol-v1/typescript/invalid/framing-oversized.hex"
        ),
        "framing-malformed-protobuf.hex" => include_str!(
            "../../../../spec/fixtures/protocol-v1/typescript/invalid/framing-malformed-protobuf.hex"
        ),
        "primary-key-noncanonical.hex" => include_str!(
            "../../../../spec/fixtures/protocol-v1/typescript/invalid/primary-key-noncanonical.hex"
        ),
        "primary-key-text-invalid-utf8.hex" => include_str!(
            "../../../../spec/fixtures/protocol-v1/typescript/invalid/primary-key-text-invalid-utf8.hex"
        ),
        "wrong-checksum.frame.hex" => include_str!(
            "../../../../spec/fixtures/protocol-v1/typescript/invalid/wrong-checksum.frame.hex"
        ),
        "unknown-enum.frame.hex" => include_str!(
            "../../../../spec/fixtures/protocol-v1/typescript/invalid/unknown-enum.frame.hex"
        ),
        "missing-oneof.frame.hex" => include_str!(
            "../../../../spec/fixtures/protocol-v1/typescript/invalid/missing-oneof.frame.hex"
        ),
        "duplicate-change-request.frame.hex" => include_str!(
            "../../../../spec/fixtures/protocol-v1/typescript/invalid/duplicate-change-request.frame.hex"
        ),
        "order-before-start.frame.hex" => include_str!(
            "../../../../spec/fixtures/protocol-v1/typescript/invalid/order-before-start.frame.hex"
        ),
        "fragment-gap.frame.hex" => include_str!(
            "../../../../spec/fixtures/protocol-v1/typescript/invalid/fragment-gap.frame.hex"
        ),
        "exact-limit-change-request.frame.hex" => include_str!(
            "../../../../spec/fixtures/protocol-v1/typescript/invalid/exact-limit-change-request.frame.hex"
        ),
        "over-limit-change-request.frame.hex" => include_str!(
            "../../../../spec/fixtures/protocol-v1/typescript/invalid/over-limit-change-request.frame.hex"
        ),
        _ => panic!("unknown invalid fixture {name}"),
    };
    hex::decode(text.trim()).expect("invalid fixture contains canonical hex")
}

fn decode_invalid_all<T>(name: &str) -> Vec<T>
where
    T: Message + Default,
{
    let mut decoder = FrameDecoder::new(Cursor::new(invalid_fixture(name)));
    let mut messages = Vec::new();
    while let Some(message) = decoder
        .read_message::<T>()
        .expect("semantic-invalid fixture remains structurally framed")
    {
        messages.push(message);
    }
    messages
}

fn stream_expectation() -> ChangeStreamExpectation {
    ChangeStreamExpectation {
        plan_id: "plan-1".into(),
        from_cursor_exclusive: b"cursor-0".to_vec(),
        requested_fence: Some(b"cursor-h".to_vec()),
        table_starts: vec![ExpectedTableStart {
            table_id: "public.tasks".into(),
            snapshot_cursor: Some(b"cursor-p".to_vec()),
        }],
        generation: ExpectedSourceGeneration {
            source_epoch: "epoch-1".into(),
            publication_generation: 2,
            schema_generation: 3,
            authorization_generation: 4,
            authorization_scope_hash: vec![5; 32],
        },
        revocation_barrier_key: None,
    }
}

#[test]
fn consumes_typescript_change_requests_with_both_optional_states_and_a_stream() {
    let requests = decode_all::<ChangeRequest>("change-request.frame.hex");
    assert_eq!(requests.len(), 3);
    for request in &requests {
        validate_change_request(request).expect("TypeScript request is semantically valid");
    }
    assert!(requests[0].through_cursor_inclusive.is_none());
    assert!(requests[1].through_cursor_inclusive.is_some());
    assert!(requests[1].accept_transaction_fragmentation);
    // Both sides of the revocation-barrier negotiation are on the wire: the
    // minimal request leaves it unset, the full one sets it.
    assert!(!requests[0].accept_revocation_barrier);
    assert!(requests[1].accept_revocation_barrier);
    assert_eq!(requests[1].max_batch_bytes, 16 * 1024 * 1024);
    assert_eq!(requests[1].max_transactions, 10_000);
    assert_eq!(requests[1].long_poll_timeout_ms, 120_000);
    // Only the live request with a wait asks for a stream.
    assert!(!requests[0].stream && !requests[1].stream);
    assert!(requests[2].stream);
    assert!(requests[2].through_cursor_inclusive.is_none());
    assert_eq!(requests[2].long_poll_timeout_ms, 1_000);
}

#[test]
fn consumes_every_typescript_frame_body_operation_cell_and_error() {
    let frames = decode_all::<ChangeStreamFrame>("change-stream.frame.hex");
    assert_eq!(frames.len(), 15);
    let mut body_kinds = std::collections::BTreeSet::new();
    let mut error_codes = std::collections::BTreeSet::new();
    let mut operation_kinds = Vec::new();
    let mut cell_kind_tags = Vec::new();

    for frame in &frames {
        validate_change_stream_frame(frame).expect("TypeScript frame is semantically valid");
        verify_change_frame_body_sha256(frame).expect("TypeScript frame digest is valid");
        match frame.body.as_ref().expect("validated body") {
            change_stream_frame::Body::BatchStart(_) => {
                body_kinds.insert("batch-start");
            }
            change_stream_frame::Body::TransactionFragment(fragment) => {
                body_kinds.insert("transaction-fragment");
                operation_kinds.extend(fragment.operations.iter().map(|operation| operation.kind));
                let insert = fragment.operations.first().expect("insert operation");
                let Some(operation::Payload::Insert(row)) = insert.payload.as_ref() else {
                    panic!("first operation must be insert");
                };
                cell_kind_tags.extend(row.columns.iter().map(|column| {
                    column
                        .value
                        .as_ref()
                        .and_then(|value| value.kind.as_ref())
                        .map(std::mem::discriminant)
                        .expect("validated cell kind")
                }));
            }
            change_stream_frame::Body::BatchEnd(end) => {
                body_kinds.insert("batch-end");
                assert_eq!(end.acknowledged_mutation_ids, ["mutation-1", "mutation-2"]);
                assert_eq!(end.frame_count, 3);
            }
            change_stream_frame::Body::Heartbeat(_) => {
                body_kinds.insert("heartbeat");
            }
            change_stream_frame::Body::Error(error) => {
                body_kinds.insert("error");
                error_codes.insert(error.code);
            }
            change_stream_frame::Body::RevocationBarrier(barrier) => {
                body_kinds.insert("revocation-barrier");
                assert_eq!(barrier.signature.len(), 64);
            }
        }
    }

    assert_eq!(
        body_kinds,
        [
            "batch-end",
            "batch-start",
            "error",
            "heartbeat",
            "revocation-barrier",
            "transaction-fragment"
        ]
        .into_iter()
        .collect()
    );
    assert_eq!(
        operation_kinds,
        [
            OperationKind::InsertFullRow as i32,
            OperationKind::UpdatePatch as i32,
            OperationKind::DeleteByKey as i32,
            OperationKind::TruncateAuthorizedTable as i32,
        ]
    );
    assert_eq!(cell_kind_tags.len(), 14);
    assert_eq!(error_codes, (1_i32..=9).collect());
}

#[test]
fn consumes_typescript_mutation_requests_all_outcomes_and_marker() {
    let requests = decode_all::<MutationBatchRequest>("mutation-request.frame.hex");
    assert_eq!(requests.len(), 2);
    for request in &requests {
        validate_mutation_batch_request(request)
            .expect("TypeScript mutation request is semantically valid");
    }
    assert_eq!(
        requests[1].mutations[0].created_at_unix_micros,
        9_007_199_254_740_993
    );

    let responses = decode_all::<MutationBatchResponse>("mutation-response.frame.hex");
    assert_eq!(responses.len(), 1);
    validate_mutation_batch_response(&responses[0])
        .expect("TypeScript mutation response is semantically valid");
    assert_eq!(
        responses[0]
            .outcomes
            .iter()
            .map(|outcome| outcome.code)
            .collect::<std::collections::BTreeSet<_>>(),
        (1_i32..=8).collect()
    );

    let markers = decode_all::<MutationLogicalMarker>("mutation-marker.frame.hex");
    assert_eq!(markers.len(), 1);
    validate_mutation_logical_marker(&markers[0])
        .expect("TypeScript mutation marker is semantically valid");
}

#[test]
fn consumes_typescript_v1_key_whitelist() {
    let key = decode_primary_key(&fixture("primary-key.hex"))
        .expect("TypeScript primary key is canonical and valid");
    assert_eq!(key.parts.len(), 4);
}

#[test]
fn consumes_typescript_text_key() {
    let key = decode_primary_key(&fixture("primary-key-text.hex"))
        .expect("TypeScript text primary key is canonical and valid");
    let text = key.parts[1]
        .value
        .as_ref()
        .and_then(|value| value.kind.as_ref());
    assert_eq!(
        text,
        Some(&sync_protocol::change::cell_value::Kind::TextValue(
            "é-\u{1d11e}".to_owned()
        ))
    );
    assert!(decode_primary_key(&invalid_fixture("primary-key-text-invalid-utf8.hex")).is_err());
}

#[test]
fn rejects_typescript_origin_malformed_truncated_and_oversized_framing() {
    for (name, expected) in [
        ("framing-truncated-prefix.hex", "length prefix is truncated"),
        ("framing-oversized.hex", "exceeds configured maximum"),
        ("framing-malformed-protobuf.hex", "malformed protobuf"),
    ] {
        let mut decoder = FrameDecoder::new(Cursor::new(invalid_fixture(name)));
        let error = decoder
            .read_message::<ChangeRequest>()
            .expect_err("TypeScript-origin invalid framing must fail");
        assert!(error.to_string().contains(expected), "{name}: {error}");
        assert!(matches!(
            decoder.read_message::<ChangeRequest>(),
            Err(FrameDecodeError::DecoderPoisoned)
        ));
    }
}

#[test]
fn rejects_typescript_origin_noncanonical_key_checksum_enum_oneof_and_duplicate() {
    assert!(decode_primary_key(&invalid_fixture("primary-key-noncanonical.hex")).is_err());

    let checksum = &decode_invalid_all::<ChangeStreamFrame>("wrong-checksum.frame.hex")[0];
    validate_change_stream_frame(checksum).expect("only integrity is invalid");
    assert!(verify_change_frame_body_sha256(checksum).is_err());

    let unknown = &decode_invalid_all::<ChangeStreamFrame>("unknown-enum.frame.hex")[0];
    assert!(validate_change_stream_frame(unknown).is_err());

    let missing = &decode_invalid_all::<ChangeStreamFrame>("missing-oneof.frame.hex")[0];
    assert!(validate_change_stream_frame(missing).is_err());

    let duplicate = &decode_invalid_all::<ChangeRequest>("duplicate-change-request.frame.hex")[0];
    assert!(validate_change_request(duplicate).is_err());
}

#[test]
fn rejects_typescript_origin_stream_order_and_fragment_gap() {
    let mut before_start = ChangeStreamState::new(stream_expectation()).unwrap();
    let frame = &decode_invalid_all::<ChangeStreamFrame>("order-before-start.frame.hex")[0];
    assert!(before_start.accept(frame).is_err());

    let frames = decode_invalid_all::<ChangeStreamFrame>("fragment-gap.frame.hex");
    let mut gap = ChangeStreamState::new(stream_expectation()).unwrap();
    gap.accept(&frames[0]).expect("batch start is valid");
    assert!(gap.accept(&frames[1]).is_err());
}

#[test]
fn accepts_typescript_origin_exact_public_limits_and_rejects_plus_one() {
    let exact = &decode_invalid_all::<ChangeRequest>("exact-limit-change-request.frame.hex")[0];
    validate_change_request(exact).expect("every populated public limit is exact");
    assert_eq!(exact.tables.len(), 1_024);
    assert_eq!(exact.pending_mutation_ids.len(), 256);
    assert_eq!(exact.authorization_scope_token.len(), 8 * 1_024);
    assert_eq!(exact.client_instance_id.len(), 1_024);
    assert_eq!(exact.device_id.len(), 1_024);
    assert_eq!(exact.plan_id.len(), 512);
    assert_eq!(
        exact.from_cursor_exclusive.as_ref().unwrap().token.len(),
        2_048
    );

    let over = &decode_invalid_all::<ChangeRequest>("over-limit-change-request.frame.hex")[0];
    assert!(validate_change_request(over).is_err());
}
