use prost::{
    encoding::{encode_key, encode_varint, encoded_len_varint, key_len, WireType},
    Message,
};
use sha2::{Digest, Sha256};
use subtle::ConstantTimeEq;
use thiserror::Error;

use crate::{
    change::{change_stream_frame, ChangeStreamFrame},
    FrameEncodeError, FRAME_LENGTH_PREFIX_BYTES,
};

const CHECKSUM_DOMAIN: &[u8] = b"calque.change.body-sha256.v2\0";
const BATCH_FRAMES_DOMAIN: &[u8] = b"calque.change.batch-frames-sha256.v1\0";
/// `ChangeStreamFrame.body_sha256`'s field number.
const BODY_SHA256_FIELD: u32 = 20;

#[derive(Debug, Error, PartialEq, Eq)]
pub enum ChangeFrameIntegrityError {
    #[error("change stream frame body is missing")]
    MissingBody,
    #[error("encoded change stream body exceeds the u32 checksum length field")]
    BodyTooLarge,
    #[error("change stream frame body SHA-256 must contain exactly 32 bytes")]
    InvalidDigestLength,
    #[error("change stream frame body SHA-256 mismatch")]
    DigestMismatch,
}

/// Hashes the preimage
/// domain || protobuf_body_field_tag_u8 || body_length_be_u32 ||
/// deterministic known-field protobuf body bytes.
///
/// Nothing of the response a frame travels in is hashed, so one sealed frame
/// is the same bytes in every response that carries it. The tag byte is the
/// selected body's protobuf field number, so an additive body alternative
/// widens the legal tag set without changing the formula. A build that does
/// not know a body sees no `body` at all -- prost drops the unknown field --
/// and returns [`ChangeFrameIntegrityError::MissingBody`] rather than a
/// digest, so an unrecognized body fails closed.
pub fn change_frame_body_sha256(
    frame: &ChangeStreamFrame,
) -> Result<[u8; 32], ChangeFrameIntegrityError> {
    let body = frame
        .body
        .as_ref()
        .ok_or(ChangeFrameIntegrityError::MissingBody)?;
    let mut body_bytes = Vec::with_capacity(body_encoded_len(body));
    encode_body(body, &mut body_bytes);
    body_sha256(body_tag(body), &body_bytes)
}

/// One length-prefixed frame and its body digest.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SealedChangeFrame {
    pub bytes: Vec<u8>,
    pub body_sha256: [u8; 32],
}

/// Seals `frame` and encodes it as one length-prefixed frame of at most
/// `max_payload_bytes`: the bytes [`encode_frame_with_limit`] gives once
/// `body_sha256` holds [`change_frame_body_sha256`], with the body
/// serialized once, into the frame, and hashed where it lies. Any
/// `body_sha256` the frame carries is replaced.
///
/// [`encode_frame_with_limit`]: crate::encode_frame_with_limit
pub fn encode_sealed_change_frame(
    mut frame: ChangeStreamFrame,
    max_payload_bytes: usize,
) -> Result<SealedChangeFrame, SealedChangeFrameError> {
    let body = frame
        .body
        .take()
        .ok_or(ChangeFrameIntegrityError::MissingBody)?;
    frame.body_sha256 = Vec::new();
    let tag = body_tag(&body);
    let body_len = body_encoded_len(&body);
    let body_length =
        u32::try_from(body_len).map_err(|_| ChangeFrameIntegrityError::BodyTooLarge)?;
    // Field 1, then the body (10 to 15), then its digest (20): the
    // field-number order prost encodes the whole frame in.
    let payload_len = frame.encoded_len()
        + key_len(u32::from(tag))
        + encoded_len_varint(u64::from(body_length))
        + body_len
        + key_len(BODY_SHA256_FIELD)
        + encoded_len_varint(32)
        + 32;
    if payload_len > max_payload_bytes {
        return Err(FrameEncodeError::PayloadTooLarge {
            declared: payload_len,
            maximum: max_payload_bytes,
        }
        .into());
    }
    let payload_len_u32 =
        u32::try_from(payload_len).map_err(|_| FrameEncodeError::LengthOverflow { payload_len })?;
    let mut bytes = Vec::with_capacity(FRAME_LENGTH_PREFIX_BYTES + payload_len);
    bytes.extend_from_slice(&payload_len_u32.to_be_bytes());
    frame
        .encode(&mut bytes)
        .map_err(FrameEncodeError::Protobuf)?;
    encode_key(u32::from(tag), WireType::LengthDelimited, &mut bytes);
    encode_varint(u64::from(body_length), &mut bytes);
    let body_start = bytes.len();
    encode_body(&body, &mut bytes);
    let digest = body_sha256(tag, &bytes[body_start..])?;
    encode_key(BODY_SHA256_FIELD, WireType::LengthDelimited, &mut bytes);
    encode_varint(32, &mut bytes);
    bytes.extend_from_slice(&digest);
    debug_assert_eq!(bytes.len(), FRAME_LENGTH_PREFIX_BYTES + payload_len);
    Ok(SealedChangeFrame {
        bytes,
        body_sha256: digest,
    })
}

#[derive(Debug, Error)]
pub enum SealedChangeFrameError {
    #[error(transparent)]
    Integrity(#[from] ChangeFrameIntegrityError),
    #[error(transparent)]
    Frame(#[from] FrameEncodeError),
}

/// `BatchEnd.frames_sha256` over the frames that precede it, fed their body
/// digests in order.
#[derive(Clone)]
pub struct BatchFramesDigest {
    hasher: Sha256,
    frames: u64,
}

impl Default for BatchFramesDigest {
    fn default() -> Self {
        Self::new()
    }
}

impl BatchFramesDigest {
    pub fn new() -> Self {
        let mut hasher = Sha256::new();
        hasher.update(BATCH_FRAMES_DOMAIN);
        Self { hasher, frames: 0 }
    }

    pub fn push(&mut self, body_sha256: &[u8; 32]) {
        self.hasher.update(body_sha256);
        self.frames += 1;
    }

    /// How many digests have been pushed.
    pub fn frames(&self) -> u64 {
        self.frames
    }

    pub fn finish(self) -> [u8; 32] {
        self.hasher.finalize().into()
    }
}

/// The selected body's protobuf field number.
const fn body_tag(body: &change_stream_frame::Body) -> u8 {
    match body {
        change_stream_frame::Body::BatchStart(_) => 10,
        change_stream_frame::Body::TransactionFragment(_) => 11,
        change_stream_frame::Body::BatchEnd(_) => 12,
        change_stream_frame::Body::Heartbeat(_) => 13,
        change_stream_frame::Body::Error(_) => 14,
        change_stream_frame::Body::RevocationBarrier(_) => 15,
    }
}

fn body_encoded_len(body: &change_stream_frame::Body) -> usize {
    match body {
        change_stream_frame::Body::BatchStart(body) => body.encoded_len(),
        change_stream_frame::Body::TransactionFragment(body) => body.encoded_len(),
        change_stream_frame::Body::BatchEnd(body) => body.encoded_len(),
        change_stream_frame::Body::Heartbeat(body) => body.encoded_len(),
        change_stream_frame::Body::Error(body) => body.encoded_len(),
        change_stream_frame::Body::RevocationBarrier(body) => body.encoded_len(),
    }
}

/// Appends the body's deterministic known-field serialization.
fn encode_body(body: &change_stream_frame::Body, bytes: &mut Vec<u8>) {
    match body {
        change_stream_frame::Body::BatchStart(body) => body.encode_raw(bytes),
        change_stream_frame::Body::TransactionFragment(body) => body.encode_raw(bytes),
        change_stream_frame::Body::BatchEnd(body) => body.encode_raw(bytes),
        change_stream_frame::Body::Heartbeat(body) => body.encode_raw(bytes),
        change_stream_frame::Body::Error(body) => body.encode_raw(bytes),
        change_stream_frame::Body::RevocationBarrier(body) => body.encode_raw(bytes),
    }
}

fn body_sha256(body_tag: u8, body_bytes: &[u8]) -> Result<[u8; 32], ChangeFrameIntegrityError> {
    let body_length =
        u32::try_from(body_bytes.len()).map_err(|_| ChangeFrameIntegrityError::BodyTooLarge)?;
    let mut hasher = Sha256::new();
    hasher.update(CHECKSUM_DOMAIN);
    hasher.update([body_tag]);
    hasher.update(body_length.to_be_bytes());
    hasher.update(body_bytes);
    Ok(hasher.finalize().into())
}

pub fn verify_change_frame_body_sha256(
    frame: &ChangeStreamFrame,
) -> Result<(), ChangeFrameIntegrityError> {
    let actual: &[u8; 32] = frame
        .body_sha256
        .as_slice()
        .try_into()
        .map_err(|_| ChangeFrameIntegrityError::InvalidDigestLength)?;
    let expected = change_frame_body_sha256(frame)?;
    if bool::from(actual.ct_eq(&expected)) {
        Ok(())
    } else {
        Err(ChangeFrameIntegrityError::DigestMismatch)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        change::{
            change_stream_frame, BatchEnd, BatchStart, Cursor, Heartbeat, RevocationBarrier,
            StreamError, TransactionFragment,
        },
        encode_frame_with_limit,
    };

    fn heartbeat_frame() -> ChangeStreamFrame {
        ChangeStreamFrame {
            protocol_version: 1,
            body: Some(change_stream_frame::Body::Heartbeat(Heartbeat {
                durable_head: Some(Cursor {
                    token: b"cursor-9".to_vec(),
                }),
                server_time_unix_micros: 42,
            })),
            body_sha256: Vec::new(),
        }
    }

    #[test]
    fn heartbeat_checksum_matches_cross_language_golden_vector() {
        let digest = change_frame_body_sha256(&heartbeat_frame()).unwrap();
        assert_eq!(
            hex::encode(digest),
            "3354e0618a1b1bc8398e9b660a5f6af1f02c6a2702f8f2dc4f4e4a67edab420e"
        );
    }

    #[test]
    fn batch_frames_digest_matches_cross_language_golden_vector() {
        let digest = change_frame_body_sha256(&heartbeat_frame()).unwrap();
        let mut frames = BatchFramesDigest::new();
        frames.push(&digest);
        frames.push(&digest);
        assert_eq!(frames.frames(), 2);
        assert_eq!(
            hex::encode(frames.finish()),
            "4e3e251e441747e7acca24728d1d2b71b04638551ebcb611bbc1c3541c9a24ca"
        );
        assert_eq!(
            hex::encode(BatchFramesDigest::new().finish()),
            "0038f268dd7e088a6f73143de821a658ecb6fcf0238f50f837e85c4ca73d8ba2"
        );
    }

    #[test]
    fn verification_rejects_tampering_and_invalid_width() {
        let mut frame = heartbeat_frame();
        frame.body_sha256 = change_frame_body_sha256(&frame).unwrap().to_vec();
        verify_change_frame_body_sha256(&frame).unwrap();

        let Some(change_stream_frame::Body::Heartbeat(heartbeat)) = frame.body.as_mut() else {
            unreachable!("a heartbeat frame");
        };
        heartbeat.server_time_unix_micros += 1;
        assert_eq!(
            verify_change_frame_body_sha256(&frame),
            Err(ChangeFrameIntegrityError::DigestMismatch)
        );
        frame.body_sha256.pop();
        assert_eq!(
            verify_change_frame_body_sha256(&frame),
            Err(ChangeFrameIntegrityError::InvalidDigestLength)
        );
    }

    /// The frame sealed and encoded the way the encoder's one-pass path
    /// must reproduce: digest set, then prost's encoding of the whole frame.
    fn sealed_by_prost(mut frame: ChangeStreamFrame, max: usize) -> Result<Vec<u8>, String> {
        frame.body_sha256 = change_frame_body_sha256(&frame)
            .map_err(|error| error.to_string())?
            .to_vec();
        encode_frame_with_limit(&frame, max).map_err(|error| error.to_string())
    }

    #[test]
    fn one_pass_sealing_gives_the_bytes_of_sealing_then_encoding() {
        let cursor = || Cursor {
            token: b"cursor-9".to_vec(),
        };
        let bodies = [
            change_stream_frame::Body::BatchStart(BatchStart::default()),
            change_stream_frame::Body::BatchStart(BatchStart {
                from_cursor_exclusive: Some(cursor()),
                table_ids: vec!["public.tasks".to_owned(); 3],
                server_time_unix_micros: 9,
                plan_id: "plan-1".to_owned(),
                ..BatchStart::default()
            }),
            change_stream_frame::Body::TransactionFragment(TransactionFragment::default()),
            // Bodies whose lengths take one, two and three varint bytes.
            change_stream_frame::Body::TransactionFragment(TransactionFragment {
                transaction_id: vec![7; 100],
                transaction_cursor: Some(cursor()),
                final_fragment: true,
                ..TransactionFragment::default()
            }),
            change_stream_frame::Body::TransactionFragment(TransactionFragment {
                transaction_id: vec![7; 1_000],
                ..TransactionFragment::default()
            }),
            change_stream_frame::Body::TransactionFragment(TransactionFragment {
                transaction_id: vec![7; 20_000],
                ..TransactionFragment::default()
            }),
            change_stream_frame::Body::BatchEnd(BatchEnd {
                through_cursor_inclusive: Some(cursor()),
                currently_up_to_date: true,
                ..BatchEnd::default()
            }),
            change_stream_frame::Body::Heartbeat(Heartbeat::default()),
            change_stream_frame::Body::Error(StreamError {
                code: 2,
                message: "expired".to_owned(),
                ..StreamError::default()
            }),
            change_stream_frame::Body::RevocationBarrier(RevocationBarrier {
                barrier_cursor: Some(cursor()),
                revoked_authorization_generation: 1,
                effective_authorization_generation: 2,
                ..RevocationBarrier::default()
            }),
        ];
        for body in bodies {
            for protocol_version in [1, 0, u32::MAX] {
                let frame = ChangeStreamFrame {
                    protocol_version,
                    body: Some(body.clone()),
                    // Replaced, not kept.
                    body_sha256: vec![1; 32],
                };
                let expected = sealed_by_prost(frame.clone(), usize::MAX).unwrap();
                let payload = expected.len() - FRAME_LENGTH_PREFIX_BYTES;
                let sealed = encode_sealed_change_frame(frame.clone(), payload).unwrap();
                assert_eq!(sealed.bytes, expected);
                assert_eq!(
                    sealed.body_sha256,
                    change_frame_body_sha256(&frame).unwrap()
                );
                assert_eq!(
                    encode_sealed_change_frame(frame.clone(), payload - 1)
                        .unwrap_err()
                        .to_string(),
                    sealed_by_prost(frame, payload - 1).unwrap_err()
                );
            }
        }

        let mut bodiless = heartbeat_frame();
        bodiless.body = None;
        assert!(matches!(
            encode_sealed_change_frame(bodiless, usize::MAX),
            Err(SealedChangeFrameError::Integrity(
                ChangeFrameIntegrityError::MissingBody
            ))
        ));
    }
}
