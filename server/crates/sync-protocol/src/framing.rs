use std::{error::Error, fmt};

use prost::Message;

/// Width of the unsigned, big-endian payload-length prefix.
pub const FRAME_LENGTH_PREFIX_BYTES: usize = 4;

/// Default maximum encoded protobuf payload: 4 MiB.
pub const DEFAULT_MAX_FRAME_PAYLOAD_BYTES: usize = 4 * 1024 * 1024;

/// Encodes one protobuf message with the default outer-frame limit.
pub fn encode_frame<M: Message>(message: &M) -> Result<Vec<u8>, FrameEncodeError> {
    encode_frame_with_limit(message, DEFAULT_MAX_FRAME_PAYLOAD_BYTES)
}

/// Encodes one protobuf message with an unsigned, big-endian length prefix.
pub fn encode_frame_with_limit<M: Message>(
    message: &M,
    max_payload_bytes: usize,
) -> Result<Vec<u8>, FrameEncodeError> {
    let payload_len = message.encoded_len();
    if payload_len == 0 {
        return Err(FrameEncodeError::ZeroLength);
    }
    if payload_len > max_payload_bytes {
        return Err(FrameEncodeError::PayloadTooLarge {
            declared: payload_len,
            maximum: max_payload_bytes,
        });
    }

    let payload_len_u32 =
        u32::try_from(payload_len).map_err(|_| FrameEncodeError::LengthOverflow { payload_len })?;
    let mut frame = Vec::with_capacity(FRAME_LENGTH_PREFIX_BYTES + payload_len);
    frame.extend_from_slice(&payload_len_u32.to_be_bytes());
    message
        .encode(&mut frame)
        .map_err(FrameEncodeError::Protobuf)?;
    Ok(frame)
}

#[derive(Debug)]
pub enum FrameEncodeError {
    ZeroLength,
    PayloadTooLarge { declared: usize, maximum: usize },
    LengthOverflow { payload_len: usize },
    Protobuf(prost::EncodeError),
}

impl fmt::Display for FrameEncodeError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::ZeroLength => formatter.write_str("zero-length frames are invalid"),
            Self::PayloadTooLarge { declared, maximum } => write!(
                formatter,
                "frame payload length {declared} exceeds configured maximum {maximum}"
            ),
            Self::LengthOverflow { payload_len } => write!(
                formatter,
                "frame payload length {payload_len} exceeds the unsigned 32-bit prefix"
            ),
            Self::Protobuf(error) => write!(formatter, "protobuf encoding failed: {error}"),
        }
    }
}

impl Error for FrameEncodeError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::Protobuf(error) => Some(error),
            _ => None,
        }
    }
}
