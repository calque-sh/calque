//! Shared, language-neutral identities, cursors, generations, and errors.
//!
//! This crate contains no PostgreSQL, HTTP, storage, or SQLite behavior. It is
//! the dependency root for the server data plane.

use std::{fmt, str::FromStr};

use base64::{engine::general_purpose::URL_SAFE_NO_PAD, Engine as _};
use hmac::{Hmac, KeyInit, Mac};
use serde::{de, Deserialize, Deserializer, Serialize};
use sha2::Sha256;
use thiserror::Error;

const CURSOR_VERSION: u8 = 1;
/// The longest cursor token [`OpaqueCursor`] parses. It equals the change
/// protocol's bound on a wire cursor, which change-api asserts at compile time.
pub const MAX_CURSOR_TOKEN_BYTES: usize = 2_048;
const MIN_CURSOR_SECRET_BYTES: usize = 32;

type HmacSha256 = Hmac<Sha256>;

#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub enum IdentifierError {
    #[error("identifier cannot be empty")]
    Empty,
    #[error("identifier exceeds 512 bytes")]
    TooLong,
    #[error("identifier must begin with an ASCII alphanumeric character")]
    InvalidStart,
    #[error("identifier contains an unsupported character at byte {index}")]
    InvalidCharacter { index: usize },
}

fn validate_identifier(value: &str) -> Result<(), IdentifierError> {
    if value.is_empty() {
        return Err(IdentifierError::Empty);
    }
    if value.len() > 512 {
        return Err(IdentifierError::TooLong);
    }
    if !value.as_bytes()[0].is_ascii_alphanumeric() {
        return Err(IdentifierError::InvalidStart);
    }
    if let Some((index, _)) = value.bytes().enumerate().find(|(_, byte)| {
        !(byte.is_ascii_alphanumeric() || matches!(byte, b'.' | b'_' | b':' | b'/' | b'-'))
    }) {
        return Err(IdentifierError::InvalidCharacter { index });
    }
    Ok(())
}

macro_rules! identifier {
    ($name:ident) => {
        #[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
        #[serde(try_from = "String", into = "String")]
        pub struct $name(String);

        impl $name {
            pub fn new(value: impl Into<String>) -> Result<Self, IdentifierError> {
                let value = value.into();
                validate_identifier(&value)?;
                Ok(Self(value))
            }

            pub fn as_str(&self) -> &str {
                &self.0
            }
        }

        impl fmt::Display for $name {
            fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
                formatter.write_str(&self.0)
            }
        }

        impl FromStr for $name {
            type Err = IdentifierError;

            fn from_str(value: &str) -> Result<Self, Self::Err> {
                Self::new(value)
            }
        }

        impl TryFrom<String> for $name {
            type Error = IdentifierError;

            fn try_from(value: String) -> Result<Self, Self::Error> {
                Self::new(value)
            }
        }

        impl From<$name> for String {
            fn from(value: $name) -> Self {
                value.0
            }
        }
    };
}

identifier!(SourceId);
identifier!(SourceEpoch);
identifier!(TableId);
identifier!(ArtifactId);
identifier!(ObjectId);
identifier!(MutationId);
identifier!(PlanId);

/// Stable schema-generation-scoped column identifier used by the binary wire
/// protocol. PostgreSQL attribute numbers are never exposed as this value.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub struct ColumnId(u32);

impl ColumnId {
    pub const fn new(value: u32) -> Self {
        Self(value)
    }

    pub const fn get(self) -> u32 {
        self.0
    }
}

/// Server-trusted position at a complete canonical transaction or barrier.
///
/// The monotonically allocated sequence has meaning only within `source_epoch`.
/// Clients receive an [`OpaqueCursor`] and never compare this value directly.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CursorPosition {
    pub source_epoch: SourceEpoch,
    pub sequence: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(transparent)]
pub struct OpaqueCursor(String);

impl OpaqueCursor {
    pub fn parse(value: impl Into<String>) -> Result<Self, CursorError> {
        let value = value.into();
        if value.is_empty() {
            return Err(CursorError::Empty);
        }
        if value.len() > MAX_CURSOR_TOKEN_BYTES {
            return Err(CursorError::TooLong);
        }
        Ok(Self(value))
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl fmt::Display for OpaqueCursor {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(&self.0)
    }
}

impl FromStr for OpaqueCursor {
    type Err = CursorError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        Self::parse(value)
    }
}

impl<'de> Deserialize<'de> for OpaqueCursor {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let value = String::deserialize(deserializer)?;
        Self::parse(value).map_err(de::Error::custom)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub enum CursorError {
    #[error("cursor token cannot be empty")]
    Empty,
    #[error("cursor token exceeds 2048 bytes")]
    TooLong,
    #[error("cursor signing secret must contain at least 32 bytes")]
    WeakSecret,
    #[error("cursor token has invalid framing")]
    InvalidFraming,
    #[error("cursor token has invalid base64url encoding")]
    InvalidEncoding,
    #[error("cursor signature verification failed")]
    InvalidSignature,
    #[error("cursor payload is malformed")]
    MalformedPayload,
    #[error("cursor version {0} is unsupported")]
    UnsupportedVersion(u8),
    #[error("cursor belongs to source epoch {actual}, expected {expected}")]
    EpochMismatch {
        expected: SourceEpoch,
        actual: SourceEpoch,
    },
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct CursorPayloadV1 {
    version: u8,
    source_epoch: SourceEpoch,
    sequence: u64,
}

/// Encodes and authenticates opaque cursor tokens using HMAC-SHA-256.
///
/// The token is `base64url(payload-json).base64url(mac)`. JSON is an internal
/// server codec, not a client contract. Verification happens before parsing.
#[derive(Clone)]
pub struct CursorCodec {
    /// Keyed once: every cursor a batch signs or checks starts from a copy.
    keyed: HmacSha256,
}

impl CursorCodec {
    pub fn new(secret: impl AsRef<[u8]>) -> Result<Self, CursorError> {
        let secret = secret.as_ref();
        if secret.len() < MIN_CURSOR_SECRET_BYTES {
            return Err(CursorError::WeakSecret);
        }
        Ok(Self {
            keyed: HmacSha256::new_from_slice(secret)
                .expect("HMAC accepts keys of any non-empty length"),
        })
    }

    pub fn encode(&self, position: &CursorPosition) -> OpaqueCursor {
        let payload = serde_json::to_vec(&CursorPayloadV1 {
            version: CURSOR_VERSION,
            source_epoch: position.source_epoch.clone(),
            sequence: position.sequence,
        })
        .expect("CursorPayloadV1 serialization cannot fail");
        let signature = self.sign(&payload);
        OpaqueCursor(format!(
            "{}.{}",
            URL_SAFE_NO_PAD.encode(payload),
            URL_SAFE_NO_PAD.encode(signature)
        ))
    }

    pub fn decode(&self, cursor: &OpaqueCursor) -> Result<CursorPosition, CursorError> {
        let (encoded_payload, encoded_signature) = cursor
            .as_str()
            .split_once('.')
            .ok_or(CursorError::InvalidFraming)?;
        if encoded_signature.contains('.') {
            return Err(CursorError::InvalidFraming);
        }
        let payload = URL_SAFE_NO_PAD
            .decode(encoded_payload)
            .map_err(|_| CursorError::InvalidEncoding)?;
        let signature = URL_SAFE_NO_PAD
            .decode(encoded_signature)
            .map_err(|_| CursorError::InvalidEncoding)?;

        let mut mac = self.keyed.clone();
        mac.update(&payload);
        mac.verify_slice(&signature)
            .map_err(|_| CursorError::InvalidSignature)?;

        let payload: CursorPayloadV1 =
            serde_json::from_slice(&payload).map_err(|_| CursorError::MalformedPayload)?;
        if payload.version != CURSOR_VERSION {
            return Err(CursorError::UnsupportedVersion(payload.version));
        }
        Ok(CursorPosition {
            source_epoch: payload.source_epoch,
            sequence: payload.sequence,
        })
    }

    pub fn decode_for_epoch(
        &self,
        cursor: &OpaqueCursor,
        expected_epoch: &SourceEpoch,
    ) -> Result<CursorPosition, CursorError> {
        let position = self.decode(cursor)?;
        if &position.source_epoch != expected_epoch {
            return Err(CursorError::EpochMismatch {
                expected: expected_epoch.clone(),
                actual: position.source_epoch,
            });
        }
        Ok(position)
    }

    fn sign(&self, payload: &[u8]) -> Vec<u8> {
        let mut mac = self.keyed.clone();
        mac.update(payload);
        mac.finalize().into_bytes().to_vec()
    }
}

impl fmt::Debug for CursorCodec {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("CursorCodec")
            .field("secret", &"[REDACTED]")
            .finish()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn codec() -> CursorCodec {
        CursorCodec::new(b"test-only-secret-with-at-least-32-bytes")
            .expect("test key is sufficiently long")
    }

    fn epoch(value: &str) -> SourceEpoch {
        SourceEpoch::new(value).expect("valid test source epoch")
    }

    #[test]
    fn identifiers_have_one_shared_validation_contract() {
        assert_eq!(
            TableId::new("public.tasks").unwrap().as_str(),
            "public.tasks"
        );
        assert_eq!(TableId::new("_tasks"), Err(IdentifierError::InvalidStart));
        assert_eq!(
            TableId::new("tasks with spaces"),
            Err(IdentifierError::InvalidCharacter { index: 5 })
        );
    }

    #[test]
    fn cursor_round_trips_without_exposing_secret_in_debug() {
        let codec = codec();
        let position = CursorPosition {
            source_epoch: epoch("source-epoch-7"),
            sequence: 42,
        };
        let cursor = codec.encode(&position);

        assert_eq!(codec.decode(&cursor).unwrap(), position);
        assert!(cursor.as_str().contains('.'));
        assert_eq!(
            format!("{codec:?}"),
            "CursorCodec { secret: \"[REDACTED]\" }"
        );
        assert_eq!(
            cursor.as_str(),
            "eyJ2ZXJzaW9uIjoxLCJzb3VyY2VfZXBvY2giOiJzb3VyY2UtZXBvY2gtNyIsInNlcXVlbmNlIjo0Mn0.fiOSBKjNMT93yqMXD5K_R-c6-mgrZz_zjgL3M4CwjGM"
        );
    }

    #[test]
    fn cursor_tampering_fails_closed() {
        let codec = codec();
        let cursor = codec.encode(&CursorPosition {
            source_epoch: epoch("source-epoch-7"),
            sequence: 42,
        });
        let mut bytes = cursor.to_string().into_bytes();
        bytes[3] = if bytes[3] == b'A' { b'B' } else { b'A' };
        let tampered = OpaqueCursor::parse(String::from_utf8(bytes).unwrap()).unwrap();

        assert_eq!(codec.decode(&tampered), Err(CursorError::InvalidSignature));
    }

    /// The token is HMAC-SHA-256 under the codec's secret, so a cursor a
    /// client already holds stays valid when the codec's implementation
    /// changes. The expected token was computed independently of this crate.
    #[test]
    fn cursor_signature_is_hmac_sha256_under_the_secret() {
        let position = CursorPosition {
            source_epoch: epoch("source-epoch-7"),
            sequence: 42,
        };
        assert_eq!(
            codec().encode(&position).to_string(),
            "eyJ2ZXJzaW9uIjoxLCJzb3VyY2VfZXBvY2giOiJzb3VyY2UtZXBvY2gtNyIsInNlcXVlbmNlIjo0Mn0\
             .fiOSBKjNMT93yqMXD5K_R-c6-mgrZz_zjgL3M4CwjGM"
        );

        let other = CursorCodec::new(b"another-test-secret-with-32-or-more-bytes").unwrap();
        assert_eq!(
            other.decode(&codec().encode(&position)),
            Err(CursorError::InvalidSignature)
        );
    }

    /// A token signed over `payload` by `codec`, framed as `encode` frames one.
    fn signed_token(codec: &CursorCodec, payload: &[u8]) -> OpaqueCursor {
        OpaqueCursor::parse(format!(
            "{}.{}",
            URL_SAFE_NO_PAD.encode(payload),
            URL_SAFE_NO_PAD.encode(codec.sign(payload))
        ))
        .unwrap()
    }

    #[test]
    fn a_token_that_is_not_one_signed_v1_payload_is_refused() {
        let codec = codec();
        let valid = codec
            .encode(&CursorPosition {
                source_epoch: epoch("source-epoch-7"),
                sequence: 42,
            })
            .to_string();
        let (payload, signature) = valid.split_once('.').unwrap();
        let cases = [
            (
                "no separator",
                payload.to_owned(),
                CursorError::InvalidFraming,
            ),
            (
                "a third part",
                format!("{valid}.{signature}"),
                CursorError::InvalidFraming,
            ),
            (
                "a payload that is not base64url",
                format!("{payload}+.{signature}"),
                CursorError::InvalidEncoding,
            ),
            (
                "a signature that is not base64url",
                format!("{payload}.{signature}="),
                CursorError::InvalidEncoding,
            ),
        ];
        for (case, token, expected) in cases {
            let token = OpaqueCursor::parse(token).unwrap();
            assert_eq!(codec.decode(&token), Err(expected), "{case}");
        }

        // Signed, so only the payload's own checks can refuse these.
        assert_eq!(
            codec.decode(&signed_token(&codec, br#"{"version":1,"sequence":42}"#)),
            Err(CursorError::MalformedPayload)
        );
        assert_eq!(
            codec.decode(&signed_token(
                &codec,
                br#"{"version":1,"source_epoch":"source-epoch-7","sequence":42,"extra":0}"#
            )),
            Err(CursorError::MalformedPayload)
        );
        assert_eq!(
            codec.decode(&signed_token(
                &codec,
                br#"{"version":2,"source_epoch":"source-epoch-7","sequence":42}"#
            )),
            Err(CursorError::UnsupportedVersion(2))
        );
        assert_eq!(
            codec
                .decode(&signed_token(
                    &codec,
                    br#"{"version":1,"source_epoch":"source-epoch-7","sequence":42}"#
                ))
                .unwrap()
                .sequence,
            42
        );
    }

    #[test]
    fn foreign_source_epoch_is_a_structured_rebuild_boundary() {
        let codec = codec();
        let cursor = codec.encode(&CursorPosition {
            source_epoch: epoch("old-history"),
            sequence: 8,
        });

        assert_eq!(
            codec.decode_for_epoch(&cursor, &epoch("new-history")),
            Err(CursorError::EpochMismatch {
                expected: epoch("new-history"),
                actual: epoch("old-history"),
            })
        );
    }

    #[test]
    fn short_cursor_secrets_are_rejected() {
        assert_eq!(
            CursorCodec::new(b"short").unwrap_err(),
            CursorError::WeakSecret
        );
    }

    #[test]
    fn cursor_json_cannot_bypass_token_limits() {
        let oversized = format!("\"{}\"", "x".repeat(MAX_CURSOR_TOKEN_BYTES + 1));
        assert!(serde_json::from_str::<OpaqueCursor>(&oversized).is_err());
        assert!(serde_json::from_str::<OpaqueCursor>("\"\"").is_err());
    }
}
