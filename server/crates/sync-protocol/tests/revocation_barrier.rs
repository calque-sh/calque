//! The ordered authenticated revocation barrier, against the cross-language
//! golden vectors.
//!
//! Every vector here was produced by the TypeScript generator, and
//! `pnpm --filter @calque/protocol fixtures:check` proves the two generators
//! emit the same bytes, so these tests run against the other language's
//! encoder.

use std::io::Cursor;

use prost::Message;
use sync_protocol::{
    change::{change_stream_frame, ChangeStreamFrame, RevocationBarrier},
    validate_change_stream_frame, ChangeStreamExpectation, ChangeStreamState,
    ExpectedSourceGeneration, ExpectedTableStart, FrameDecodeError, FrameDecoder,
    RevocationBarrierVerificationKey,
};

/// The public half of the fixed key pair the fixture generators sign with.
const BARRIER_PUBLIC_KEY: [u8; 32] = [
    0x03, 0xa1, 0x07, 0xbf, 0xf3, 0xce, 0x10, 0xbe, 0x1d, 0x70, 0xdd, 0x18, 0xe7, 0x4b, 0xc0, 0x99,
    0x67, 0xe4, 0xd6, 0x30, 0x9b, 0xa5, 0x0d, 0x5f, 0x1d, 0xdc, 0x86, 0x64, 0x12, 0x55, 0x31, 0xb8,
];

fn hex_fixture(text: &str) -> Vec<u8> {
    hex::decode(text.trim()).expect("fixture contains canonical lowercase hex")
}

fn fixture(name: &str) -> Vec<u8> {
    hex_fixture(match name {
        "revocation-barrier.frame.hex" => include_str!(
            "../../../../spec/fixtures/protocol-v1/typescript/revocation-barrier.frame.hex"
        ),
        _ => panic!("unknown fixture {name}"),
    })
}

fn invalid_fixture(name: &str) -> Vec<u8> {
    hex_fixture(match name {
        "revocation-barrier-malformed.frame.hex" => include_str!(
            "../../../../spec/fixtures/protocol-v1/typescript/invalid/revocation-barrier-malformed.frame.hex"
        ),
        "revocation-barrier-truncated.hex" => include_str!(
            "../../../../spec/fixtures/protocol-v1/typescript/invalid/revocation-barrier-truncated.hex"
        ),
        "revocation-barrier-oversized.hex" => include_str!(
            "../../../../spec/fixtures/protocol-v1/typescript/invalid/revocation-barrier-oversized.hex"
        ),
        "revocation-barrier-reordered.frame.hex" => include_str!(
            "../../../../spec/fixtures/protocol-v1/typescript/invalid/revocation-barrier-reordered.frame.hex"
        ),
        "revocation-barrier-duplicated.frame.hex" => include_str!(
            "../../../../spec/fixtures/protocol-v1/typescript/invalid/revocation-barrier-duplicated.frame.hex"
        ),
        "revocation-barrier-cross-epoch.frame.hex" => include_str!(
            "../../../../spec/fixtures/protocol-v1/typescript/invalid/revocation-barrier-cross-epoch.frame.hex"
        ),
        _ => panic!("unknown invalid fixture {name}"),
    })
}

fn frames(bytes: Vec<u8>) -> Vec<ChangeStreamFrame> {
    let mut decoder = FrameDecoder::new(Cursor::new(bytes));
    let mut messages = Vec::new();
    while let Some(frame) = decoder
        .read_message::<ChangeStreamFrame>()
        .expect("fixture frame decodes")
    {
        messages.push(frame);
    }
    messages
}

fn expectation() -> ChangeStreamExpectation {
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
        revocation_barrier_key: Some(RevocationBarrierVerificationKey::new(
            "revocation-key-1",
            BARRIER_PUBLIC_KEY,
        )),
    }
}

#[test]
fn consumes_the_typescript_barrier_and_reports_it_on_the_completed_batch() {
    let mut state = ChangeStreamState::new(expectation()).unwrap();
    let mut completed = None;
    for frame in frames(fixture("revocation-barrier.frame.hex")) {
        completed = state
            .accept(&frame)
            .expect("TypeScript barrier stream is valid")
            .or(completed);
    }
    let completed = completed.expect("the batch completed");
    let barrier = completed
        .revocation_barrier
        .expect("the completed batch reports its barrier");
    assert_eq!(barrier.revoked_authorization_generation, 4);
    assert_eq!(barrier.effective_authorization_generation, 5);
    assert_eq!(barrier.signing_key_id, "revocation-key-1");
    assert_eq!(barrier.signature.len(), 64);
    assert_eq!(
        completed.through_cursor_inclusive,
        barrier.barrier_cursor.expect("barrier cursor").token
    );
    // Everything the batch delivered is before the barrier, and there is
    // exactly one transaction's worth of it.
    assert_eq!(completed.relevant_transaction_count, 1);
}

#[test]
fn a_forged_signature_is_refused_even_though_the_frame_digest_is_correct() {
    let mut forged = frames(fixture("revocation-barrier.frame.hex"));
    let Some(change_stream_frame::Body::RevocationBarrier(barrier)) = forged[2].body.as_mut()
    else {
        panic!("the third frame is the barrier");
    };
    barrier.signature[0] ^= 0xff;
    forged[2].body_sha256 = sync_protocol::change_frame_body_sha256(&forged[2])
        .expect("known body")
        .to_vec();
    // The frame is structurally perfect: the digest is recomputed over the
    // forged body, so only the authorization authority's signature can catch
    // it.
    validate_change_stream_frame(&forged[2]).expect("shape is valid");

    let mut state = ChangeStreamState::new(expectation()).unwrap();
    state.accept(&forged[0]).unwrap();
    state.accept(&forged[1]).unwrap();
    let error = state
        .accept(&forged[2])
        .expect_err("a forged barrier must be refused");
    assert_eq!(error.path, "changeStreamFrame.revocationBarrier.signature");
    assert!(state.terminal());
}

#[test]
fn a_receiver_with_no_configured_key_refuses_every_barrier() {
    let mut keyless = expectation();
    keyless.revocation_barrier_key = None;
    let mut state = ChangeStreamState::new(keyless).unwrap();
    let frames = frames(fixture("revocation-barrier.frame.hex"));
    state.accept(&frames[0]).unwrap();
    state.accept(&frames[1]).unwrap();
    let error = state
        .accept(&frames[2])
        .expect_err("an unauthenticatable barrier must be refused");
    assert_eq!(error.path, "changeStreamFrame.revocationBarrier");
}

#[test]
fn rejects_malformed_reordered_duplicated_and_cross_epoch_barriers() {
    let malformed = &frames(invalid_fixture("revocation-barrier-malformed.frame.hex"))[0];
    assert_eq!(
        validate_change_stream_frame(malformed)
            .expect_err("a 63-byte signature is refused before any crypto")
            .path,
        "changeStreamFrame.revocationBarrier.signature"
    );

    for (name, path) in [
        (
            "revocation-barrier-reordered.frame.hex",
            "changeStreamFrame.transactionFragment",
        ),
        (
            "revocation-barrier-duplicated.frame.hex",
            "changeStreamFrame.revocationBarrier",
        ),
        (
            "revocation-barrier-cross-epoch.frame.hex",
            "changeStreamFrame.revocationBarrier.revokedAuthorizationGeneration",
        ),
    ] {
        let stream = frames(invalid_fixture(name));
        let mut state = ChangeStreamState::new(expectation()).unwrap();
        let mut error = None;
        for frame in &stream {
            if let Err(failure) = state.accept(frame) {
                error = Some(failure);
                break;
            }
        }
        let error = error.unwrap_or_else(|| panic!("{name} must be refused"));
        assert_eq!(error.path, path, "{name}");
        assert!(state.terminal(), "{name}");
    }
}

#[test]
fn rejects_truncated_and_oversized_barrier_frames_before_any_field_is_read() {
    let mut truncated = FrameDecoder::new(Cursor::new(invalid_fixture(
        "revocation-barrier-truncated.hex",
    )));
    let error = truncated
        .read_message::<ChangeStreamFrame>()
        .expect_err("a truncated barrier frame must fail");
    assert!(error.to_string().contains("truncated"), "{error}");

    let mut oversized = FrameDecoder::new(Cursor::new(invalid_fixture(
        "revocation-barrier-oversized.hex",
    )));
    let error = oversized
        .read_message::<ChangeStreamFrame>()
        .expect_err("an oversized barrier frame must fail");
    assert!(
        error.to_string().contains("exceeds configured maximum"),
        "{error}"
    );
    assert!(matches!(
        oversized.read_message::<ChangeStreamFrame>(),
        Err(FrameDecodeError::DecoderPoisoned)
    ));
}

/// The `sync.change.v1.ChangeStreamFrame` without the barrier body: the same
/// fields and numbers, and a `body` oneof that stops at 14.
///
/// A reader that does not know the barrier must fail closed on one: prost
/// keeps no unknown fields, so field 15 vanishes, `body` is `None`, and the
/// stateless validator refuses the frame before any stream state moves.
#[derive(Clone, PartialEq, prost::Message)]
struct LegacyChangeStreamFrame {
    #[prost(uint32, tag = "1")]
    protocol_version: u32,
    #[prost(oneof = "LegacyBody", tags = "10, 11, 12, 13, 14")]
    body: Option<LegacyBody>,
    #[prost(bytes = "vec", tag = "20")]
    body_sha256: Vec<u8>,
}

#[derive(Clone, PartialEq, prost::Oneof)]
enum LegacyBody {
    #[prost(message, tag = "10")]
    BatchStart(sync_protocol::change::BatchStart),
    #[prost(message, tag = "11")]
    TransactionFragment(sync_protocol::change::TransactionFragment),
    #[prost(message, tag = "12")]
    BatchEnd(sync_protocol::change::BatchEnd),
    #[prost(message, tag = "13")]
    Heartbeat(sync_protocol::change::Heartbeat),
    #[prost(message, tag = "14")]
    Error(sync_protocol::change::StreamError),
}

#[test]
fn a_pre_change_v1_reader_fails_closed_on_the_barrier_instead_of_ignoring_it() {
    let stream = fixture("revocation-barrier.frame.hex");
    let mut decoder = FrameDecoder::new(Cursor::new(stream));
    let mut legacy = Vec::new();
    while let Some(frame) = decoder
        .read_message::<LegacyChangeStreamFrame>()
        .expect("the outer framing is unchanged, so a v1 reader still frames it")
    {
        legacy.push(frame);
    }
    assert_eq!(legacy.len(), 4);

    // The three bodies it does know still decode.
    assert!(matches!(legacy[0].body, Some(LegacyBody::BatchStart(_))));
    assert!(matches!(
        legacy[1].body,
        Some(LegacyBody::TransactionFragment(_))
    ));
    assert!(matches!(legacy[3].body, Some(LegacyBody::BatchEnd(_))));

    // The barrier does not. It is not silently skipped into some other body,
    // and it is not carried as an unknown field it might later ignore: the
    // frame arrives with no body at all.
    assert!(legacy[2].body.is_none());

    // And that is refused, not tolerated. The same bytes re-encoded through
    // the v1 shape are what a v1 build would hand its own validator.
    let reencoded = ChangeStreamFrame::decode(legacy[2].encode_to_vec().as_slice())
        .expect("a bodyless frame is still structurally a frame");
    assert!(reencoded.body.is_none());
    let error = validate_change_stream_frame(&reencoded)
        .expect_err("a v1 reader must refuse a frame whose body it does not recognize");
    assert_eq!(error.path, "changeStreamFrame.body");

    // The integrity check reaches the same verdict independently: there is no
    // digest a bodyless frame can be given that verifies.
    assert!(sync_protocol::verify_change_frame_body_sha256(&reencoded).is_err());
}

/// A barrier that is only an unknown field to a reader that does not know it,
/// with no other body in the frame, never decodes as a known body.
#[test]
fn an_unknown_body_never_becomes_a_known_one() {
    let barrier = RevocationBarrier {
        barrier_cursor: Some(sync_protocol::change::Cursor {
            token: b"cursor-h".to_vec(),
        }),
        revoked_authorization_generation: 4,
        effective_authorization_generation: 5,
        revoked_authorization_scope_hash: vec![5; 32],
        barrier_id: "barrier-1".into(),
        issued_at_unix_micros: 1,
        signing_key_id: "revocation-key-1".into(),
        signature: vec![7; 64],
    };
    let frame = ChangeStreamFrame {
        protocol_version: 1,
        body: Some(change_stream_frame::Body::RevocationBarrier(barrier)),
        body_sha256: vec![0; 32],
    };
    let legacy = LegacyChangeStreamFrame::decode(frame.encode_to_vec().as_slice())
        .expect("a v1 reader decodes the envelope");
    assert!(legacy.body.is_none());
    assert_eq!(legacy.protocol_version, 1);
    assert_eq!(legacy.body_sha256, vec![0; 32]);
}
