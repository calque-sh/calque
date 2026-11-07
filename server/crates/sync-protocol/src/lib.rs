//! Generated sync wire messages and bounded outer-frame codecs.
//!
//! This crate provides bounded frame encoding, canonical key decoding, body
//! integrity and mutation-request validation. The
//! `test-support` feature adds the frame decoder, the other message validators
//! and fail-closed stream-order validation, which tests use as oracles.

#[cfg(any(test, feature = "test-support"))]
mod frame_decoder;
mod framing;
mod integrity;
mod primary_key;
mod revocation;
#[cfg(any(test, feature = "test-support"))]
mod stream_state;
mod validation;

#[cfg(any(test, feature = "test-support"))]
pub use frame_decoder::{FrameDecodeError, FrameDecoder};
pub use framing::{
    encode_frame, encode_frame_with_limit, FrameEncodeError, DEFAULT_MAX_FRAME_PAYLOAD_BYTES,
    FRAME_LENGTH_PREFIX_BYTES,
};
pub use integrity::{
    change_frame_body_sha256, encode_sealed_change_frame, verify_change_frame_body_sha256,
    BatchFramesDigest, ChangeFrameIntegrityError, SealedChangeFrame, SealedChangeFrameError,
};
pub use primary_key::{
    decode_primary_key, encode_primary_key, PrimaryKeyCodecError, PRIMARY_KEY_CODEC_VERSION,
    PRIMARY_KEY_MAX_BYTES,
};
pub use revocation::{
    revocation_barrier_signing_preimage, verify_revocation_barrier, RevocationBarrierError,
    RevocationBarrierVerificationKey,
};
#[cfg(any(test, feature = "test-support"))]
pub use stream_state::{
    ChangeStreamExpectation, ChangeStreamState, CompletedBatch, ExpectedSourceGeneration,
    ExpectedTableStart,
};
#[cfg(any(test, feature = "test-support"))]
pub use validation::{
    validate_change_request, validate_change_stream_frame, validate_mutation_batch_response,
    validate_mutation_logical_marker,
};
pub use validation::{
    validate_mutation_batch_request, ProtocolValidationError, MAX_BARRIER_IDENTIFIER_BYTES,
    MAX_CHANGE_AUTHORIZATION_TOKEN_BYTES, MAX_CHANGE_BATCH_BYTES, MAX_CHANGE_TRANSACTIONS,
    MAX_CLIENT_INSTANCE_ID_BYTES, MAX_COMMAND_NAME_BYTES, MAX_CONFLICT_KIND_BYTES,
    MAX_CONFLICT_PAYLOAD_BYTES, MAX_CURSOR_TOKEN_BYTES, MAX_DEVICE_ID_BYTES, MAX_EXPECTED_VERSIONS,
    MAX_LONG_POLL_TIMEOUT_MS, MAX_MUTATION_BATCH_INPUT_BYTES, MAX_MUTATION_BATCH_SIZE,
    MAX_MUTATION_CANONICAL_INPUT_BYTES, MAX_MUTATION_FOOTPRINT_ROWS, MAX_MUTATION_FOOTPRINT_TABLES,
    MAX_MUTATION_SCOPE_TOKEN_BYTES, MAX_PENDING_MUTATION_IDS, MAX_REQUEST_TABLES,
    MAX_TRANSACTION_OPERATIONS, MIN_CHANGE_BATCH_BYTES, MIN_LONG_POLL_TIMEOUT_MS, PROTOCOL_VERSION,
};

/// Messages generated from `spec/change_stream.proto`.
pub mod change {
    include!(concat!(env!("OUT_DIR"), "/sync.change.v1.rs"));
}

/// Messages generated from `spec/mutation.proto`.
pub mod mutation {
    include!(concat!(env!("OUT_DIR"), "/sync.mutation.v1.rs"));
}
