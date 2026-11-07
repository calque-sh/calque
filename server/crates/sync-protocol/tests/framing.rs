use std::io::Cursor;

use sync_protocol::{
    change::{change_stream_frame, ChangeStreamFrame, Cursor as ChangeCursor, Heartbeat},
    encode_frame, encode_frame_with_limit,
    mutation::{Cursor as MutationCursor, MutationBatchRequest, MutationEnvelope},
    FrameDecodeError, FrameDecoder, FrameEncodeError, DEFAULT_MAX_FRAME_PAYLOAD_BYTES,
};

fn heartbeat_frame() -> ChangeStreamFrame {
    ChangeStreamFrame {
        protocol_version: 1,
        body: Some(change_stream_frame::Body::Heartbeat(Heartbeat {
            durable_head: Some(ChangeCursor { token: vec![0xaa] }),
            server_time_unix_micros: 9,
        })),
        body_sha256: Vec::new(),
    }
}

fn golden_bytes() -> Vec<u8> {
    include_str!("fixtures/heartbeat.frame.hex")
        .split_ascii_whitespace()
        .map(|byte| u8::from_str_radix(byte, 16).expect("fixture contains valid hex bytes"))
        .collect()
}

#[test]
fn change_frame_matches_golden_big_endian_fixture() {
    let expected = golden_bytes();
    let encoded = encode_frame(&heartbeat_frame()).expect("heartbeat frame encodes");

    assert_eq!(encoded, expected);
    assert_eq!(&encoded[..4], &[0, 0, 0, 11]);

    let mut decoder = FrameDecoder::new(Cursor::new(expected));
    assert_eq!(
        decoder
            .read_message::<ChangeStreamFrame>()
            .expect("golden frame decodes"),
        Some(heartbeat_frame())
    );
    assert_eq!(
        decoder
            .read_message::<ChangeStreamFrame>()
            .expect("clean EOF is valid"),
        None
    );
}

#[test]
fn mutation_contract_round_trips_through_the_same_codec() {
    let request = MutationBatchRequest {
        protocol_version: 1,
        client_instance_id: "client-1".into(),
        device_id: "device-1".into(),
        authorization_scope_token: vec![1, 2, 3],
        mutations: vec![MutationEnvelope {
            mutation_id: "mutation-1".into(),
            command_name: "tasks.rename".into(),
            command_version: 1,
            canonical_input: br#"{"title":"next"}"#.to_vec(),
            base_cursor: Some(MutationCursor {
                token: vec![4, 5, 6],
            }),
            created_at_unix_micros: 42,
            expected_versions: Vec::new(),
            declared_footprint: None,
        }],
    };
    let encoded = encode_frame(&request).expect("mutation request encodes");
    let mut decoder = FrameDecoder::new(Cursor::new(encoded));

    assert_eq!(
        decoder
            .read_message::<MutationBatchRequest>()
            .expect("mutation request decodes"),
        Some(request)
    );
}

#[test]
fn clean_empty_stream_is_not_a_truncated_frame() {
    let mut decoder = FrameDecoder::new(Cursor::new(Vec::<u8>::new()));
    assert_eq!(
        decoder
            .read_message::<ChangeStreamFrame>()
            .expect("empty stream is clean EOF"),
        None
    );
}

#[test]
fn rejects_every_truncated_length_prefix() {
    for received in 1..4 {
        let mut decoder = FrameDecoder::new(Cursor::new(vec![0_u8; received]));
        let error = decoder
            .read_message::<ChangeStreamFrame>()
            .expect_err("partial prefix must fail");
        assert!(matches!(
            error,
            FrameDecodeError::TruncatedLengthPrefix { received: actual } if actual == received
        ));
    }
}

#[test]
fn rejects_zero_length_frame() {
    let mut decoder = FrameDecoder::new(Cursor::new([0_u8; 4]));
    assert!(matches!(
        decoder.read_message::<ChangeStreamFrame>(),
        Err(FrameDecodeError::ZeroLength)
    ));
}

#[test]
fn rejects_oversize_before_reading_or_allocating_payload() {
    let bytes = [0, 0, 0, 5, 0xaa, 0xbb, 0xcc, 0xdd, 0xee];
    let mut decoder = FrameDecoder::with_max_payload_bytes(Cursor::new(bytes), 4);
    assert!(matches!(
        decoder.read_message::<ChangeStreamFrame>(),
        Err(FrameDecodeError::PayloadTooLarge {
            declared: 5,
            maximum: 4
        })
    ));

    let reader = decoder.into_inner();
    assert_eq!(reader.position(), 4, "oversize payload must remain unread");
}

#[test]
fn default_decoder_enforces_the_four_mebibyte_limit() {
    let declared = u32::try_from(DEFAULT_MAX_FRAME_PAYLOAD_BYTES + 1)
        .expect("default frame limit fits the wire prefix");
    let mut decoder = FrameDecoder::new(Cursor::new(declared.to_be_bytes()));

    assert!(matches!(
        decoder.read_message::<ChangeStreamFrame>(),
        Err(FrameDecodeError::PayloadTooLarge {
            declared,
            maximum: DEFAULT_MAX_FRAME_PAYLOAD_BYTES
        }) if declared == DEFAULT_MAX_FRAME_PAYLOAD_BYTES + 1
    ));
}

#[test]
fn accepts_payload_exactly_at_configured_limit() {
    let frame = heartbeat_frame();
    let payload_len = prost::Message::encoded_len(&frame);
    let encoded = encode_frame_with_limit(&frame, payload_len).expect("exact limit encodes");
    let mut decoder = FrameDecoder::with_max_payload_bytes(Cursor::new(encoded), payload_len);

    assert_eq!(
        decoder
            .read_message::<ChangeStreamFrame>()
            .expect("exact limit decodes"),
        Some(frame)
    );
}

#[test]
fn rejects_truncated_payload_and_poisoned_decoder_cannot_resume() {
    let mut decoder = FrameDecoder::new(Cursor::new([0, 0, 0, 3, 0x08, 0x01]));
    assert!(matches!(
        decoder.read_message::<ChangeStreamFrame>(),
        Err(FrameDecodeError::TruncatedPayload {
            declared: 3,
            received: 2
        })
    ));
    assert!(matches!(
        decoder.read_message::<ChangeStreamFrame>(),
        Err(FrameDecodeError::DecoderPoisoned)
    ));
}

#[test]
fn rejects_malformed_protobuf() {
    let mut decoder = FrameDecoder::new(Cursor::new([0, 0, 0, 1, 0xff]));
    assert!(matches!(
        decoder.read_message::<ChangeStreamFrame>(),
        Err(FrameDecodeError::MalformedProtobuf(_))
    ));
}

#[test]
fn encoder_enforces_zero_and_oversize_limits() {
    assert!(matches!(
        encode_frame(&ChangeStreamFrame::default()),
        Err(FrameEncodeError::ZeroLength)
    ));

    let frame = heartbeat_frame();
    let payload_len = prost::Message::encoded_len(&frame);
    assert!(matches!(
        encode_frame_with_limit(&frame, payload_len - 1),
        Err(FrameEncodeError::PayloadTooLarge {
            declared,
            maximum
        }) if declared == payload_len && maximum == payload_len - 1
    ));
}
