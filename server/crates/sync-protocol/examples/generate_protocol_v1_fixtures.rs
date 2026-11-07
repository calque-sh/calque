use std::{env, fs, path::Path};

use sync_protocol::{
    change::{
        cell_value, change_stream_frame, operation, BatchEnd, BatchStart, CellValue, ChangeRequest,
        ChangeStreamFrame, ColumnValue, Cursor as ChangeCursor, Delete, FullRow, Heartbeat,
        NullValue, Operation, OperationKind, PrimaryKey, RevocationBarrier, RowPatch,
        SourceGeneration, StreamError, StreamErrorCode, TableSubscription, TransactionFragment,
        Truncate,
    },
    change_frame_body_sha256, encode_frame, encode_primary_key,
    mutation::{
        AffectedRow, CanonicalRow, Conflict, Cursor as MutationCursor, ExpectedVersion,
        MutationBatchRequest, MutationBatchResponse, MutationEnvelope, MutationFootprint,
        MutationLogicalMarker, MutationOutcome, MutationOutcomeCode,
    },
    revocation_barrier_signing_preimage, BatchFramesDigest,
};

fn main() {
    let output = env::args()
        .nth(1)
        .expect("usage: generate_protocol_v1_fixtures OUTPUT_DIR");
    let output = Path::new(&output);
    fs::create_dir_all(output).expect("create fixture output directory");
    fs::create_dir_all(output.join("invalid")).expect("create invalid fixture output directory");

    write_hex(output, "change-request.frame.hex", &change_requests());
    write_hex(output, "change-stream.frame.hex", &change_stream_frames());
    write_hex(
        output,
        "revocation-barrier.frame.hex",
        &revocation_barrier_stream(),
    );
    write_hex(output, "mutation-request.frame.hex", &mutation_requests());
    write_hex(output, "mutation-response.frame.hex", &mutation_response());
    write_hex(output, "mutation-marker.frame.hex", &mutation_marker());
    write_hex(output, "primary-key.hex", &primary_key());
    write_hex(output, "primary-key-text.hex", &text_primary_key());

    for (name, bytes) in negative_fixtures() {
        write_hex(&output.join("invalid"), name, &bytes);
    }
}

fn write_hex(output: &Path, name: &str, bytes: &[u8]) {
    fs::write(output.join(name), format!("{}\n", hex::encode(bytes))).expect("write fixture");
}

fn change_cursor(token: &str) -> ChangeCursor {
    ChangeCursor {
        token: token.as_bytes().to_vec(),
    }
}

fn mutation_cursor(token: &str) -> MutationCursor {
    MutationCursor {
        token: token.as_bytes().to_vec(),
    }
}

fn column(column_id: u32, kind: cell_value::Kind) -> ColumnValue {
    ColumnValue {
        column_id,
        value: Some(CellValue { kind: Some(kind) }),
    }
}

fn primary_key_message() -> PrimaryKey {
    PrimaryKey {
        codec_version: 1,
        parts: vec![
            column(1, cell_value::Kind::Int64Value(-42)),
            column(2, cell_value::Kind::BoolValue(true)),
            column(3, cell_value::Kind::BlobValue(vec![0, 1, 2, 255])),
            column(4, cell_value::Kind::Uuid16((0_u8..16).collect())),
        ],
    }
}

fn primary_key() -> Vec<u8> {
    encode_primary_key(&primary_key_message()).expect("canonical primary key")
}

fn text_primary_key() -> Vec<u8> {
    encode_primary_key(&PrimaryKey {
        codec_version: 1,
        parts: vec![
            column(1, cell_value::Kind::Int64Value(-42)),
            column(2, cell_value::Kind::TextValue("é-\u{1d11e}".to_owned())),
        ],
    })
    .expect("canonical text primary key")
}

fn append_frame<T: prost::Message>(bytes: &mut Vec<u8>, message: &T) {
    bytes.extend(encode_frame(message).expect("fixture frame encodes"));
}

fn change_requests() -> Vec<u8> {
    let mut bytes = Vec::new();
    append_frame(
        &mut bytes,
        &ChangeRequest {
            protocol_version: 1,
            from_cursor_exclusive: Some(change_cursor("cursor-0")),
            through_cursor_inclusive: None,
            tables: vec![TableSubscription {
                table_id: "public.tasks".into(),
                shard_snapshot_cursor: None,
            }],
            authorization_scope_token: b"scope-token".to_vec(),
            max_batch_bytes: 1_024,
            max_transactions: 1,
            long_poll_timeout_ms: 0,
            client_instance_id: "client-1".into(),
            accept_transaction_fragmentation: false,
            accept_revocation_barrier: false,
            plan_id: "plan-1".into(),
            device_id: "device-1".into(),
            pending_mutation_ids: Vec::new(),
            stream: false,
        },
    );
    append_frame(
        &mut bytes,
        &ChangeRequest {
            protocol_version: 1,
            from_cursor_exclusive: Some(change_cursor("cursor-0")),
            through_cursor_inclusive: Some(change_cursor("cursor-h")),
            tables: vec![
                TableSubscription {
                    table_id: "public.tasks".into(),
                    shard_snapshot_cursor: Some(change_cursor("cursor-p")),
                },
                TableSubscription {
                    table_id: "public.users".into(),
                    shard_snapshot_cursor: None,
                },
            ],
            authorization_scope_token: b"scope-token".to_vec(),
            max_batch_bytes: 16 * 1024 * 1024,
            max_transactions: 10_000,
            long_poll_timeout_ms: 120_000,
            client_instance_id: "client-1".into(),
            accept_transaction_fragmentation: true,
            accept_revocation_barrier: true,
            plan_id: "plan-1".into(),
            device_id: "device-1".into(),
            pending_mutation_ids: vec!["mutation-1".into(), "mutation-2".into()],
            stream: false,
        },
    );
    append_frame(
        &mut bytes,
        &ChangeRequest {
            protocol_version: 1,
            from_cursor_exclusive: Some(change_cursor("cursor-h")),
            through_cursor_inclusive: None,
            tables: vec![TableSubscription {
                table_id: "public.tasks".into(),
                shard_snapshot_cursor: None,
            }],
            authorization_scope_token: b"scope-token".to_vec(),
            max_batch_bytes: 1_024,
            max_transactions: 1_000,
            long_poll_timeout_ms: 1_000,
            client_instance_id: "client-1".into(),
            accept_transaction_fragmentation: true,
            accept_revocation_barrier: false,
            plan_id: "plan-1".into(),
            device_id: "device-1".into(),
            pending_mutation_ids: Vec::new(),
            stream: true,
        },
    );
    bytes
}

fn limit_change_request(authorization_token_bytes: usize) -> ChangeRequest {
    ChangeRequest {
        protocol_version: 1,
        from_cursor_exclusive: Some(ChangeCursor {
            token: vec![b'c'; 2_048],
        }),
        through_cursor_inclusive: Some(ChangeCursor {
            token: vec![b'f'; 2_048],
        }),
        tables: (0..1_024)
            .map(|index| TableSubscription {
                table_id: format!("t{index}"),
                shard_snapshot_cursor: None,
            })
            .collect(),
        authorization_scope_token: vec![1; authorization_token_bytes],
        max_batch_bytes: 16 * 1_024 * 1_024,
        max_transactions: 10_000,
        long_poll_timeout_ms: 120_000,
        client_instance_id: "c".repeat(1_024),
        accept_transaction_fragmentation: true,
        accept_revocation_barrier: true,
        stream: false,
        plan_id: "p".repeat(512),
        device_id: "d".repeat(1_024),
        pending_mutation_ids: (0..256).map(|index| format!("m{index}")).collect(),
    }
}

fn signed_frame(body: change_stream_frame::Body) -> ChangeStreamFrame {
    let mut frame = ChangeStreamFrame {
        protocol_version: 1,
        body: Some(body),
        body_sha256: Vec::new(),
    };
    frame.body_sha256 = change_frame_body_sha256(&frame)
        .expect("known body")
        .to_vec();
    frame
}

/// The `BatchEnd` closing a batch of exactly `previous`, in order.
fn batch_end(previous: &[ChangeStreamFrame], end: BatchEnd) -> ChangeStreamFrame {
    let mut frames = BatchFramesDigest::new();
    for frame in previous {
        frames.push(
            frame
                .body_sha256
                .as_slice()
                .try_into()
                .expect("signed frames carry a digest"),
        );
    }
    signed_frame(change_stream_frame::Body::BatchEnd(BatchEnd {
        frame_count: frames.frames(),
        frames_sha256: frames.finish().to_vec(),
        ..end
    }))
}

fn all_row_values() -> Vec<ColumnValue> {
    vec![
        column(1, cell_value::Kind::NullValue(NullValue::NullValue as i32)),
        column(2, cell_value::Kind::Int64Value(-9)),
        column(3, cell_value::Kind::Uint64Value(u64::MAX - 1)),
        column(4, cell_value::Kind::Float64Value(1.5)),
        column(5, cell_value::Kind::TextValue("café".into())),
        column(6, cell_value::Kind::BlobValue(vec![0, 255])),
        column(7, cell_value::Kind::BoolValue(true)),
        column(8, cell_value::Kind::DecimalText("123.4500".into())),
        column(9, cell_value::Kind::TimestampUnixMicros(-1_234_567)),
        column(10, cell_value::Kind::Uuid16((16_u8..32).collect())),
        column(11, cell_value::Kind::JsonText("{\"a\":1}".into())),
        column(12, cell_value::Kind::DateIso8601("2026-07-14".into())),
        column(13, cell_value::Kind::TimeIso8601("21:30:00Z".into())),
        column(14, cell_value::Kind::IntervalIso8601("P1DT2H".into())),
    ]
}

fn change_stream_frames() -> Vec<u8> {
    let generation = SourceGeneration {
        source_epoch: "epoch-1".into(),
        publication_generation: 2,
        schema_generation: 3,
        authorization_generation: 4,
        authorization_scope_hash: vec![5; 32],
    };
    let key = primary_key();
    let bodies = vec![
        change_stream_frame::Body::BatchStart(BatchStart {
            generation: Some(generation.clone()),
            from_cursor_exclusive: Some(change_cursor("cursor-0")),
            requested_fence: Some(change_cursor("cursor-h")),
            table_ids: vec!["public.tasks".into(), "public.users".into()],
            server_time_unix_micros: 1,
            plan_id: "plan-1".into(),
            table_starts: vec![
                TableSubscription {
                    table_id: "public.tasks".into(),
                    shard_snapshot_cursor: Some(change_cursor("cursor-p")),
                },
                TableSubscription {
                    table_id: "public.users".into(),
                    shard_snapshot_cursor: None,
                },
            ],
            stream_id: b"stream-1".to_vec(),
        }),
        change_stream_frame::Body::BatchStart(BatchStart {
            generation: Some(generation),
            from_cursor_exclusive: Some(change_cursor("cursor-0")),
            requested_fence: None,
            table_ids: vec!["public.tasks".into()],
            server_time_unix_micros: 0,
            plan_id: "plan-1".into(),
            table_starts: vec![TableSubscription {
                table_id: "public.tasks".into(),
                shard_snapshot_cursor: None,
            }],
            stream_id: b"stream-2".to_vec(),
        }),
        change_stream_frame::Body::TransactionFragment(TransactionFragment {
            transaction_id: (32_u8..48).collect(),
            transaction_cursor: Some(change_cursor("cursor-1")),
            fragment_index: 0,
            first_fragment: true,
            final_fragment: true,
            postgres_xid: 7,
            begin_lsn: "0/10".into(),
            commit_lsn: "0/20".into(),
            commit_time_unix_micros: 9,
            operations: vec![
                Operation {
                    table_id: "public.tasks".into(),
                    kind: OperationKind::InsertFullRow as i32,
                    primary_key: key.clone(),
                    payload: Some(operation::Payload::Insert(FullRow {
                        columns: all_row_values(),
                    })),
                },
                Operation {
                    table_id: "public.tasks".into(),
                    kind: OperationKind::UpdatePatch as i32,
                    primary_key: key.clone(),
                    payload: Some(operation::Payload::Update(RowPatch {
                        changed_columns: vec![column(
                            5,
                            cell_value::Kind::TextValue("updated".into()),
                        )],
                    })),
                },
                Operation {
                    table_id: "public.tasks".into(),
                    kind: OperationKind::DeleteByKey as i32,
                    primary_key: key,
                    payload: Some(operation::Payload::Delete(Delete {})),
                },
                Operation {
                    table_id: "public.users".into(),
                    kind: OperationKind::TruncateAuthorizedTable as i32,
                    primary_key: Vec::new(),
                    payload: Some(operation::Payload::Truncate(Truncate {
                        reason: "authorized reset".into(),
                    })),
                },
            ],
        }),
    ];
    let mut frames = bodies.into_iter().map(signed_frame).collect::<Vec<_>>();
    frames.push(batch_end(
        &frames,
        BatchEnd {
            through_cursor_inclusive: Some(change_cursor("cursor-h")),
            reached_requested_fence: true,
            currently_up_to_date: false,
            relevant_transaction_count: 1,
            relevant_operation_count: 4,
            server_time_unix_micros: 10,
            acknowledged_mutation_ids: vec!["mutation-1".into(), "mutation-2".into()],
            ..BatchEnd::default()
        },
    ));
    let bodies = [
        change_stream_frame::Body::Heartbeat(Heartbeat {
            durable_head: Some(change_cursor("cursor-h")),
            server_time_unix_micros: 11,
        }),
        change_stream_frame::Body::RevocationBarrier(revocation_barrier(4, vec![5; 32])),
    ];

    frames.extend(bodies.into_iter().map(signed_frame));
    let mut bytes = Vec::new();
    for frame in &frames {
        append_frame(&mut bytes, frame);
    }
    for code in [
        StreamErrorCode::CursorEpochMismatch,
        StreamErrorCode::CursorBeforeRetention,
        StreamErrorCode::SchemaGenerationMismatch,
        StreamErrorCode::AuthorizationChanged,
        StreamErrorCode::TableSetInvalid,
        StreamErrorCode::ProtocolVersionUnsupported,
        StreamErrorCode::RateLimited,
        StreamErrorCode::InternalRetryable,
        StreamErrorCode::RequestInvalid,
    ] {
        append_frame(
            &mut bytes,
            &signed_frame(change_stream_frame::Body::Error(StreamError {
                code: code as i32,
                message: format!("error-{}", code as i32),
                resnapshot_required: matches!(
                    code,
                    StreamErrorCode::CursorEpochMismatch
                        | StreamErrorCode::CursorBeforeRetention
                        | StreamErrorCode::SchemaGenerationMismatch
                        | StreamErrorCode::AuthorizationChanged
                ),
                minimum_retained_cursor: Some(change_cursor("cursor-min")),
                durable_head: Some(change_cursor("cursor-h")),
                retry_after_ms: 25,
            })),
        );
    }
    bytes
}

/// The fixed Ed25519 key pair every revocation-barrier vector in this
/// repository is signed with, in Rust and in TypeScript alike. A test key,
/// never a deployment key.
const BARRIER_SEED: [u8; 32] = [
    0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24, 25,
    26, 27, 28, 29, 30, 31,
];
const BARRIER_KEY_ID: &str = "revocation-key-1";

fn sign_barrier(mut barrier: RevocationBarrier) -> RevocationBarrier {
    let preimage = revocation_barrier_signing_preimage(&barrier).expect("barrier carries a cursor");
    let pair = ring::signature::Ed25519KeyPair::from_seed_unchecked(&BARRIER_SEED)
        .expect("fixed fixture seed is a valid Ed25519 seed");
    barrier.signature = pair.sign(&preimage).as_ref().to_vec();
    barrier
}

fn revocation_barrier(revoked_generation: u64, scope_hash: Vec<u8>) -> RevocationBarrier {
    sign_barrier(RevocationBarrier {
        barrier_cursor: Some(change_cursor("cursor-h")),
        revoked_authorization_generation: revoked_generation,
        effective_authorization_generation: revoked_generation + 1,
        revoked_authorization_scope_hash: scope_hash,
        barrier_id: "barrier-1".into(),
        issued_at_unix_micros: 1_700_000_000_000_000,
        signing_key_id: BARRIER_KEY_ID.into(),
        signature: Vec::new(),
    })
}

fn barrier_batch_start() -> change_stream_frame::Body {
    change_stream_frame::Body::BatchStart(BatchStart {
        generation: Some(SourceGeneration {
            source_epoch: "epoch-1".into(),
            publication_generation: 2,
            schema_generation: 3,
            authorization_generation: 4,
            authorization_scope_hash: vec![5; 32],
        }),
        from_cursor_exclusive: Some(change_cursor("cursor-0")),
        requested_fence: Some(change_cursor("cursor-h")),
        table_ids: vec!["public.tasks".into()],
        server_time_unix_micros: 1,
        plan_id: "plan-1".into(),
        table_starts: vec![TableSubscription {
            table_id: "public.tasks".into(),
            shard_snapshot_cursor: Some(change_cursor("cursor-p")),
        }],
        stream_id: b"stream-1".to_vec(),
    })
}

fn barrier_fragment() -> change_stream_frame::Body {
    change_stream_frame::Body::TransactionFragment(TransactionFragment {
        transaction_id: (48_u8..64).collect(),
        transaction_cursor: Some(change_cursor("cursor-1")),
        fragment_index: 0,
        first_fragment: true,
        final_fragment: true,
        postgres_xid: 11,
        begin_lsn: "0/30".into(),
        commit_lsn: "0/40".into(),
        commit_time_unix_micros: 12,
        operations: vec![Operation {
            table_id: "public.tasks".into(),
            kind: OperationKind::TruncateAuthorizedTable as i32,
            primary_key: Vec::new(),
            payload: Some(operation::Payload::Truncate(Truncate {
                reason: "authorized reset".into(),
            })),
        }],
    })
}

fn barrier_batch_end(previous: &[ChangeStreamFrame]) -> ChangeStreamFrame {
    batch_end(
        previous,
        BatchEnd {
            through_cursor_inclusive: Some(change_cursor("cursor-h")),
            reached_requested_fence: true,
            currently_up_to_date: true,
            relevant_transaction_count: 1,
            relevant_operation_count: 1,
            server_time_unix_micros: 13,
            ..BatchEnd::default()
        },
    )
}

/// The positive barrier vector: one batch that delivers a transaction, then
/// the barrier, then ends exactly on the barrier's own cursor.
fn revocation_barrier_stream() -> Vec<u8> {
    let mut frames = [
        barrier_batch_start(),
        barrier_fragment(),
        change_stream_frame::Body::RevocationBarrier(revocation_barrier(4, vec![5; 32])),
    ]
    .into_iter()
    .map(signed_frame)
    .collect::<Vec<_>>();
    frames.push(barrier_batch_end(&frames));
    let mut bytes = Vec::new();
    for frame in &frames {
        append_frame(&mut bytes, frame);
    }
    bytes
}

fn barrier_negative_fixtures() -> Vec<(&'static str, Vec<u8>)> {
    let mut malformed = revocation_barrier(4, vec![5; 32]);
    malformed.signature.pop();

    let mut reordered = Vec::new();
    append_frame(&mut reordered, &signed_frame(barrier_batch_start()));
    append_frame(
        &mut reordered,
        &signed_frame(change_stream_frame::Body::RevocationBarrier(
            revocation_barrier(4, vec![5; 32]),
        )),
    );
    append_frame(&mut reordered, &signed_frame(barrier_fragment()));

    let mut duplicated = Vec::new();
    append_frame(&mut duplicated, &signed_frame(barrier_batch_start()));
    for _ in 0..2 {
        append_frame(
            &mut duplicated,
            &signed_frame(change_stream_frame::Body::RevocationBarrier(
                revocation_barrier(4, vec![5; 32]),
            )),
        );
    }

    let mut cross_epoch = Vec::new();
    append_frame(&mut cross_epoch, &signed_frame(barrier_batch_start()));
    append_frame(
        &mut cross_epoch,
        &signed_frame(change_stream_frame::Body::RevocationBarrier(
            revocation_barrier(3, vec![5; 32]),
        )),
    );

    // The valid barrier frame, cut short of its declared payload, and the same
    // payload behind a 4 MiB-plus-one length prefix. Both are refused by the
    // frame decoder before any barrier field is looked at.
    let whole = encode_frame(&signed_frame(change_stream_frame::Body::RevocationBarrier(
        revocation_barrier(4, vec![5; 32]),
    )))
    .expect("barrier frame encodes");
    let truncated = whole[..whole.len() - 8].to_vec();
    let mut oversized = vec![0x00, 0x40, 0x00, 0x01];
    oversized.extend_from_slice(&whole[4..]);

    vec![
        (
            "revocation-barrier-malformed.frame.hex",
            encode_frame(&signed_frame(change_stream_frame::Body::RevocationBarrier(
                malformed,
            )))
            .expect("semantically invalid barrier still frames"),
        ),
        ("revocation-barrier-truncated.hex", truncated),
        ("revocation-barrier-oversized.hex", oversized),
        ("revocation-barrier-reordered.frame.hex", reordered),
        ("revocation-barrier-duplicated.frame.hex", duplicated),
        ("revocation-barrier-cross-epoch.frame.hex", cross_epoch),
    ]
}

fn mutation_requests() -> Vec<u8> {
    let key = primary_key();
    let mut bytes = Vec::new();
    append_frame(
        &mut bytes,
        &MutationBatchRequest {
            protocol_version: 1,
            client_instance_id: "client-1".into(),
            device_id: "device-1".into(),
            authorization_scope_token: b"scope-token".to_vec(),
            mutations: vec![MutationEnvelope {
                mutation_id: "mutation-1".into(),
                command_name: "tasks.rename".into(),
                command_version: 1,
                canonical_input: br#"{"title":"next"}"#.to_vec(),
                base_cursor: Some(mutation_cursor("cursor-0")),
                created_at_unix_micros: 1,
                expected_versions: Vec::new(),
                declared_footprint: Some(MutationFootprint {
                    rows: Vec::new(),
                    table_ids: vec!["public.tasks".into()],
                }),
            }],
        },
    );
    append_frame(
        &mut bytes,
        &MutationBatchRequest {
            protocol_version: 1,
            client_instance_id: "client-1".into(),
            device_id: "device-1".into(),
            authorization_scope_token: b"scope-token".to_vec(),
            mutations: vec![MutationEnvelope {
                mutation_id: "mutation-2".into(),
                command_name: "tasks.complete".into(),
                command_version: 2,
                canonical_input: vec![0, 1, 2, 255],
                base_cursor: Some(mutation_cursor("cursor-1")),
                created_at_unix_micros: 9_007_199_254_740_993,
                expected_versions: vec![ExpectedVersion {
                    table_id: "public.tasks".into(),
                    primary_key: key.clone(),
                    expected_version: b"version-1".to_vec(),
                }],
                declared_footprint: Some(MutationFootprint {
                    rows: vec![AffectedRow {
                        table_id: "public.tasks".into(),
                        primary_key: key,
                    }],
                    table_ids: vec!["public.tasks".into(), "public.audit".into()],
                }),
            }],
        },
    );
    bytes
}

fn mutation_response() -> Vec<u8> {
    let key = primary_key();
    let accepted_transaction = (64_u8..80).collect::<Vec<_>>();
    let outcomes = vec![
        MutationOutcome {
            mutation_id: "mutation-1".into(),
            code: MutationOutcomeCode::AcceptedAwaitingCanonicalEcho as i32,
            message: "accepted".into(),
            server_transaction_id: accepted_transaction.clone(),
            observed_canonical_cursor: None,
            conflict: None,
            retry_after_ms: 0,
        },
        MutationOutcome {
            mutation_id: "mutation-2".into(),
            code: MutationOutcomeCode::AlreadyAcceptedAwaitingCanonicalEcho as i32,
            message: "already accepted".into(),
            server_transaction_id: accepted_transaction.clone(),
            observed_canonical_cursor: None,
            conflict: None,
            retry_after_ms: 0,
        },
        MutationOutcome {
            mutation_id: "mutation-3".into(),
            code: MutationOutcomeCode::AlreadyCanonical as i32,
            message: "canonical".into(),
            server_transaction_id: accepted_transaction,
            observed_canonical_cursor: Some(mutation_cursor("cursor-3")),
            conflict: None,
            retry_after_ms: 0,
        },
        rejected("mutation-4", MutationOutcomeCode::RejectedValidation),
        rejected("mutation-5", MutationOutcomeCode::RejectedAuthorization),
        MutationOutcome {
            mutation_id: "mutation-6".into(),
            code: MutationOutcomeCode::Conflict as i32,
            message: "conflict".into(),
            server_transaction_id: Vec::new(),
            observed_canonical_cursor: None,
            conflict: Some(Conflict {
                kind: "version".into(),
                rows: vec![CanonicalRow {
                    table_id: "public.tasks".into(),
                    primary_key: key,
                    encoded_full_row: b"row".to_vec(),
                    version: b"version-2".to_vec(),
                }],
                application_payload: b"details".to_vec(),
            }),
            retry_after_ms: 0,
        },
        rejected("mutation-7", MutationOutcomeCode::CommandVersionUnsupported),
        MutationOutcome {
            retry_after_ms: 25,
            ..rejected("mutation-8", MutationOutcomeCode::RetryableFailure)
        },
    ];
    encode_frame(&MutationBatchResponse {
        protocol_version: 1,
        outcomes,
        server_time_unix_micros: 99,
    })
    .expect("mutation response encodes")
}

fn rejected(mutation_id: &str, code: MutationOutcomeCode) -> MutationOutcome {
    MutationOutcome {
        mutation_id: mutation_id.into(),
        code: code as i32,
        message: "rejected".into(),
        server_transaction_id: Vec::new(),
        observed_canonical_cursor: None,
        conflict: None,
        retry_after_ms: 0,
    }
}

fn mutation_marker() -> Vec<u8> {
    encode_frame(&MutationLogicalMarker {
        protocol_version: 1,
        mutation_ids: vec!["mutation-1".into(), "mutation-2".into()],
        server_transaction_id: (80_u8..96).collect(),
    })
    .expect("mutation marker encodes")
}

fn negative_fixtures() -> Vec<(&'static str, Vec<u8>)> {
    let mut wrong_checksum = signed_frame(change_stream_frame::Body::Heartbeat(Heartbeat {
        durable_head: Some(change_cursor("cursor-h")),
        server_time_unix_micros: 1,
    }));
    wrong_checksum.body_sha256 = vec![0; 32];

    let unknown_enum = signed_frame(change_stream_frame::Body::TransactionFragment(
        TransactionFragment {
            transaction_id: vec![1],
            transaction_cursor: Some(change_cursor("cursor-1")),
            fragment_index: 0,
            first_fragment: true,
            final_fragment: true,
            postgres_xid: 1,
            begin_lsn: "0/1".into(),
            commit_lsn: "0/2".into(),
            commit_time_unix_micros: 1,
            operations: vec![Operation {
                table_id: "public.tasks".into(),
                kind: 99,
                primary_key: Vec::new(),
                payload: Some(operation::Payload::Truncate(Truncate {
                    reason: "invalid enum".into(),
                })),
            }],
        },
    ));

    let missing_oneof = ChangeStreamFrame {
        protocol_version: 1,
        body: None,
        body_sha256: vec![0; 32],
    };

    let mut duplicate_request = limit_change_request(8 * 1_024);
    duplicate_request.from_cursor_exclusive = Some(change_cursor("cursor-0"));
    duplicate_request.through_cursor_inclusive = None;
    duplicate_request.authorization_scope_token = b"scope-token".to_vec();
    duplicate_request.max_batch_bytes = 1_024;
    duplicate_request.max_transactions = 1;
    duplicate_request.long_poll_timeout_ms = 0;
    duplicate_request.client_instance_id = "client-1".into();
    duplicate_request.plan_id = "plan-1".into();
    duplicate_request.device_id = "device-1".into();
    duplicate_request.pending_mutation_ids.clear();
    duplicate_request.tables = vec![
        TableSubscription {
            table_id: "public.tasks".into(),
            shard_snapshot_cursor: None,
        },
        TableSubscription {
            table_id: "public.tasks".into(),
            shard_snapshot_cursor: None,
        },
    ];

    let before_start = signed_frame(change_stream_frame::Body::Heartbeat(Heartbeat {
        durable_head: Some(change_cursor("cursor-h")),
        server_time_unix_micros: 1,
    }));

    let start = signed_frame(change_stream_frame::Body::BatchStart(BatchStart {
        generation: Some(SourceGeneration {
            source_epoch: "epoch-1".into(),
            publication_generation: 2,
            schema_generation: 3,
            authorization_generation: 4,
            authorization_scope_hash: vec![5; 32],
        }),
        from_cursor_exclusive: Some(change_cursor("cursor-0")),
        requested_fence: Some(change_cursor("cursor-h")),
        table_ids: vec!["public.tasks".into()],
        server_time_unix_micros: 1,
        plan_id: "plan-1".into(),
        table_starts: vec![TableSubscription {
            table_id: "public.tasks".into(),
            shard_snapshot_cursor: Some(change_cursor("cursor-p")),
        }],
        stream_id: b"stream-1".to_vec(),
    }));
    let fragment_gap = signed_frame(change_stream_frame::Body::TransactionFragment(
        TransactionFragment {
            transaction_id: vec![1],
            transaction_cursor: Some(change_cursor("cursor-1")),
            fragment_index: 1,
            first_fragment: false,
            final_fragment: true,
            postgres_xid: 1,
            begin_lsn: "0/1".into(),
            commit_lsn: "0/2".into(),
            commit_time_unix_micros: 1,
            operations: vec![Operation {
                table_id: "public.tasks".into(),
                kind: OperationKind::TruncateAuthorizedTable as i32,
                primary_key: Vec::new(),
                payload: Some(operation::Payload::Truncate(Truncate {
                    reason: "authorized reset".into(),
                })),
            }],
        },
    ));
    let mut fragment_gap_stream = Vec::new();
    append_frame(&mut fragment_gap_stream, &start);
    append_frame(&mut fragment_gap_stream, &fragment_gap);

    let mut noncanonical_key = primary_key();
    noncanonical_key.extend_from_slice(&[0x98, 0x06, 0x01]);

    let mut fixtures = vec![
        ("framing-truncated-prefix.hex", vec![0, 0, 0]),
        ("framing-oversized.hex", vec![0, 64, 0, 1]),
        ("framing-malformed-protobuf.hex", vec![0, 0, 0, 1, 0x0f]),
        ("primary-key-noncanonical.hex", noncanonical_key),
        // One text part whose protobuf string is the lone byte 0xFF.
        (
            "primary-key-text-invalid-utf8.hex",
            vec![
                0x08, 0x01, 0x12, 0x07, 0x08, 0x02, 0x12, 0x03, 0x2a, 0x01, 0xff,
            ],
        ),
        (
            "wrong-checksum.frame.hex",
            encode_frame(&wrong_checksum).expect("invalid semantic fixture still frames"),
        ),
        (
            "unknown-enum.frame.hex",
            encode_frame(&unknown_enum).expect("invalid semantic fixture still frames"),
        ),
        (
            "missing-oneof.frame.hex",
            encode_frame(&missing_oneof).expect("invalid semantic fixture still frames"),
        ),
        (
            "duplicate-change-request.frame.hex",
            encode_frame(&duplicate_request).expect("invalid semantic fixture still frames"),
        ),
        (
            "order-before-start.frame.hex",
            encode_frame(&before_start).expect("invalid stream-order fixture frames"),
        ),
        ("fragment-gap.frame.hex", fragment_gap_stream),
        (
            "exact-limit-change-request.frame.hex",
            encode_frame(&limit_change_request(8 * 1_024)).expect("exact-limit fixture frames"),
        ),
        (
            "over-limit-change-request.frame.hex",
            encode_frame(&limit_change_request(8 * 1_024 + 1))
                .expect("semantic over-limit fixture remains below frame limit"),
        ),
    ];
    fixtures.extend(barrier_negative_fixtures());
    fixtures
}
