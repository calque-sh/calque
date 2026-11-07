use std::{error::Error, fmt, io};

use prost::Message;

use crate::{DEFAULT_MAX_FRAME_PAYLOAD_BYTES, FRAME_LENGTH_PREFIX_BYTES};

/// Stateful decoder for the framed byte stream.
///
/// A framing or protobuf error poisons the decoder because continuing after a
/// rejected length or malformed payload could reinterpret payload bytes as a
/// new frame boundary. The declared length is checked before payload allocation.
pub struct FrameDecoder<R> {
    reader: R,
    max_payload_bytes: usize,
    failed: bool,
}

impl<R: io::Read> FrameDecoder<R> {
    /// Creates a decoder with the 4 MiB default payload limit.
    pub fn new(reader: R) -> Self {
        Self::with_max_payload_bytes(reader, DEFAULT_MAX_FRAME_PAYLOAD_BYTES)
    }

    /// Creates a decoder with a caller-selected payload limit.
    pub fn with_max_payload_bytes(reader: R, max_payload_bytes: usize) -> Self {
        Self {
            reader,
            max_payload_bytes,
            failed: false,
        }
    }

    /// Reads and decodes the next message, or returns `None` at a clean EOF.
    pub fn read_message<M: Message + Default>(&mut self) -> Result<Option<M>, FrameDecodeError> {
        let Some(payload) = self.read_payload()? else {
            return Ok(None);
        };

        match M::decode(payload.as_slice()) {
            Ok(message) => Ok(Some(message)),
            Err(error) => {
                self.failed = true;
                Err(FrameDecodeError::MalformedProtobuf(error))
            }
        }
    }

    /// Reads the next bounded payload without interpreting protobuf fields.
    pub(crate) fn read_payload(&mut self) -> Result<Option<Vec<u8>>, FrameDecodeError> {
        if self.failed {
            return Err(FrameDecodeError::DecoderPoisoned);
        }

        let result = self.read_payload_inner();
        if result.is_err() {
            self.failed = true;
        }
        result
    }

    /// Returns the wrapped reader, including after a terminal failure.
    pub fn into_inner(self) -> R {
        self.reader
    }

    fn read_payload_inner(&mut self) -> Result<Option<Vec<u8>>, FrameDecodeError> {
        let mut prefix = [0_u8; FRAME_LENGTH_PREFIX_BYTES];
        let prefix_bytes =
            read_up_to(&mut self.reader, &mut prefix).map_err(FrameDecodeError::Io)?;
        if prefix_bytes == 0 {
            return Ok(None);
        }
        if prefix_bytes != FRAME_LENGTH_PREFIX_BYTES {
            return Err(FrameDecodeError::TruncatedLengthPrefix {
                received: prefix_bytes,
            });
        }

        let declared = u32::from_be_bytes(prefix) as usize;
        if declared == 0 {
            return Err(FrameDecodeError::ZeroLength);
        }
        if declared > self.max_payload_bytes {
            return Err(FrameDecodeError::PayloadTooLarge {
                declared,
                maximum: self.max_payload_bytes,
            });
        }

        let mut payload = vec![0_u8; declared];
        let received = read_up_to(&mut self.reader, &mut payload).map_err(FrameDecodeError::Io)?;
        if received != declared {
            return Err(FrameDecodeError::TruncatedPayload { declared, received });
        }
        Ok(Some(payload))
    }
}

fn read_up_to(reader: &mut impl io::Read, buffer: &mut [u8]) -> io::Result<usize> {
    let mut received = 0;
    while received < buffer.len() {
        match reader.read(&mut buffer[received..]) {
            Ok(0) => break,
            Ok(count) => received += count,
            Err(error) if error.kind() == io::ErrorKind::Interrupted => continue,
            Err(error) => return Err(error),
        }
    }
    Ok(received)
}

#[derive(Debug)]
pub enum FrameDecodeError {
    Io(io::Error),
    DecoderPoisoned,
    TruncatedLengthPrefix { received: usize },
    ZeroLength,
    PayloadTooLarge { declared: usize, maximum: usize },
    TruncatedPayload { declared: usize, received: usize },
    MalformedProtobuf(prost::DecodeError),
}

impl fmt::Display for FrameDecodeError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Io(error) => write!(formatter, "frame transport read failed: {error}"),
            Self::DecoderPoisoned => {
                formatter.write_str("frame decoder cannot continue after a terminal failure")
            }
            Self::TruncatedLengthPrefix { received } => write!(
                formatter,
                "frame length prefix is truncated: received {received} of {FRAME_LENGTH_PREFIX_BYTES} bytes"
            ),
            Self::ZeroLength => formatter.write_str("zero-length frames are invalid"),
            Self::PayloadTooLarge { declared, maximum } => write!(
                formatter,
                "frame payload length {declared} exceeds configured maximum {maximum}"
            ),
            Self::TruncatedPayload { declared, received } => write!(
                formatter,
                "frame payload is truncated: received {received} of {declared} bytes"
            ),
            Self::MalformedProtobuf(error) => {
                write!(formatter, "frame contains malformed protobuf: {error}")
            }
        }
    }
}

impl Error for FrameDecodeError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::Io(error) => Some(error),
            Self::MalformedProtobuf(error) => Some(error),
            _ => None,
        }
    }
}
