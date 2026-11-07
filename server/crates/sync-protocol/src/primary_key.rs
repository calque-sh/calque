use std::collections::BTreeSet;

use prost::Message;
use thiserror::Error;

use crate::change::{cell_value, PrimaryKey};

pub const PRIMARY_KEY_CODEC_VERSION: u32 = 1;
pub const PRIMARY_KEY_MAX_BYTES: usize = 8 * 1024;
pub(crate) const PRIMARY_KEY_MAX_PARTS: usize = 32;

#[derive(Debug, Error, PartialEq, Eq)]
pub enum PrimaryKeyCodecError {
    #[error("primary key encoding is empty")]
    Empty,
    #[error("primary key encoding is {actual} bytes, exceeding limit {maximum}")]
    TooLarge { actual: usize, maximum: usize },
    #[error("primary key protobuf is malformed")]
    Malformed,
    #[error("primary key encoding contains unknown fields or is not canonical protobuf")]
    NonCanonical,
    #[error("primary key codec version {0} is unsupported")]
    UnsupportedVersion(u32),
    #[error("primary key must contain at least one part")]
    MissingParts,
    #[error("primary key has {actual} parts, exceeding limit {maximum}")]
    TooManyParts { actual: usize, maximum: usize },
    #[error("primary key part {index} has column ID zero")]
    ZeroColumnId { index: usize },
    #[error("primary key contains duplicate column ID {column_id}")]
    DuplicateColumnId { column_id: u32 },
    #[error("primary key part {index} has no value")]
    MissingValue { index: usize },
    #[error("primary key part {index} has no value kind")]
    MissingValueKind { index: usize },
    #[error("primary key part {index} is null")]
    NullPart { index: usize },
    #[error("primary key part {index} uses a floating-point value, which is not a v1 key type")]
    FloatingPointPart { index: usize },
    #[error("primary key part {index} uses a value kind that is not supported by codec v1")]
    UnsupportedValueKind { index: usize },
    #[error("primary key part {index} UUID has {actual} bytes, expected 16")]
    InvalidUuidWidth { index: usize, actual: usize },
}

/// Encodes a validated v1 primary key using Prost's generated protobuf codec.
///
/// Composite part order is preserved and must already match the owning table's
/// schema-generation primary-key ordinal. [`decode_primary_key`] performs the
/// same structural checks on untrusted bytes.
pub fn encode_primary_key(key: &PrimaryKey) -> Result<Vec<u8>, PrimaryKeyCodecError> {
    validate_primary_key(key)?;
    let bytes = key.encode_to_vec();
    validate_encoded_size(&bytes)?;
    Ok(bytes)
}

/// Decodes only the unique known-field protobuf representation accepted by v1.
/// Re-encoding equality rejects unknown fields, alternate field order, duplicate
/// scalar fields, and overlong/non-minimal varints that could otherwise give one
/// logical key multiple byte identities.
pub fn decode_primary_key(bytes: &[u8]) -> Result<PrimaryKey, PrimaryKeyCodecError> {
    validate_encoded_size(bytes)?;
    let key = PrimaryKey::decode(bytes).map_err(|_| PrimaryKeyCodecError::Malformed)?;
    validate_primary_key(&key)?;
    if key.encode_to_vec() != bytes {
        return Err(PrimaryKeyCodecError::NonCanonical);
    }
    Ok(key)
}

fn validate_encoded_size(bytes: &[u8]) -> Result<(), PrimaryKeyCodecError> {
    if bytes.is_empty() {
        return Err(PrimaryKeyCodecError::Empty);
    }
    if bytes.len() > PRIMARY_KEY_MAX_BYTES {
        return Err(PrimaryKeyCodecError::TooLarge {
            actual: bytes.len(),
            maximum: PRIMARY_KEY_MAX_BYTES,
        });
    }
    Ok(())
}

fn validate_primary_key(key: &PrimaryKey) -> Result<(), PrimaryKeyCodecError> {
    if key.codec_version != PRIMARY_KEY_CODEC_VERSION {
        return Err(PrimaryKeyCodecError::UnsupportedVersion(key.codec_version));
    }
    if key.parts.is_empty() {
        return Err(PrimaryKeyCodecError::MissingParts);
    }
    if key.parts.len() > PRIMARY_KEY_MAX_PARTS {
        return Err(PrimaryKeyCodecError::TooManyParts {
            actual: key.parts.len(),
            maximum: PRIMARY_KEY_MAX_PARTS,
        });
    }

    let mut column_ids = BTreeSet::new();
    for (index, part) in key.parts.iter().enumerate() {
        if part.column_id == 0 {
            return Err(PrimaryKeyCodecError::ZeroColumnId { index });
        }
        if !column_ids.insert(part.column_id) {
            return Err(PrimaryKeyCodecError::DuplicateColumnId {
                column_id: part.column_id,
            });
        }
        let value = part
            .value
            .as_ref()
            .ok_or(PrimaryKeyCodecError::MissingValue { index })?;
        match value
            .kind
            .as_ref()
            .ok_or(PrimaryKeyCodecError::MissingValueKind { index })?
        {
            cell_value::Kind::NullValue(_) => {
                return Err(PrimaryKeyCodecError::NullPart { index });
            }
            cell_value::Kind::Float64Value(_) => {
                return Err(PrimaryKeyCodecError::FloatingPointPart { index });
            }
            cell_value::Kind::Uuid16(value) if value.len() != 16 => {
                return Err(PrimaryKeyCodecError::InvalidUuidWidth {
                    index,
                    actual: value.len(),
                });
            }
            cell_value::Kind::Int64Value(_)
            | cell_value::Kind::BoolValue(_)
            | cell_value::Kind::TextValue(_)
            | cell_value::Kind::BlobValue(_)
            | cell_value::Kind::Uuid16(_) => {}
            _ => return Err(PrimaryKeyCodecError::UnsupportedValueKind { index }),
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::change::{cell_value, CellValue, ColumnValue};

    fn part(column_id: u32, kind: cell_value::Kind) -> ColumnValue {
        ColumnValue {
            column_id,
            value: Some(CellValue { kind: Some(kind) }),
        }
    }

    fn composite_key() -> PrimaryKey {
        PrimaryKey {
            codec_version: PRIMARY_KEY_CODEC_VERSION,
            parts: vec![
                part(7, cell_value::Kind::Int64Value(-42)),
                part(2, cell_value::Kind::BoolValue(true)),
                part(9, cell_value::Kind::Uuid16(vec![0xAB; 16])),
            ],
        }
    }

    #[test]
    fn composite_key_matches_cross_language_golden_vector() {
        let encoded = encode_primary_key(&composite_key()).unwrap();
        assert_eq!(
            hex::encode(&encoded),
            "0801120608071202105312060802120238011216080912125210abababababababababababababababab"
        );
        assert_eq!(decode_primary_key(&encoded).unwrap(), composite_key());
    }

    /// A text part is protobuf `text_value` (field 5), so its bytes are the
    /// string's UTF-8. `encodeTextPrimaryKey` in the client protocol package
    /// pins the same vector.
    #[test]
    fn text_key_matches_cross_language_golden_vector() {
        let key = PrimaryKey {
            codec_version: PRIMARY_KEY_CODEC_VERSION,
            parts: vec![
                part(7, cell_value::Kind::Int64Value(-42)),
                part(3, cell_value::Kind::TextValue("é-1".to_owned())),
            ],
        };
        let encoded = encode_primary_key(&key).unwrap();
        assert_eq!(
            hex::encode(&encoded),
            "08011206080712021053120a080312062a04c3a92d31"
        );
        assert_eq!(decode_primary_key(&encoded).unwrap(), key);
    }

    #[test]
    fn rejects_noncanonical_and_malformed_encodings() {
        let canonical = encode_primary_key(&composite_key()).unwrap();
        let mut unknown_field = canonical.clone();
        unknown_field.extend_from_slice(&[0x98, 0x06, 0x01]);
        assert_eq!(
            decode_primary_key(&unknown_field),
            Err(PrimaryKeyCodecError::NonCanonical)
        );
        assert_eq!(
            decode_primary_key(&[0x08]),
            Err(PrimaryKeyCodecError::Malformed)
        );
        assert_eq!(decode_primary_key(&[]), Err(PrimaryKeyCodecError::Empty));
    }

    #[test]
    fn rejects_null_float_duplicate_and_bad_uuid_parts() {
        let cases = [
            (
                PrimaryKey {
                    codec_version: 1,
                    parts: vec![part(1, cell_value::Kind::NullValue(0))],
                },
                PrimaryKeyCodecError::NullPart { index: 0 },
            ),
            (
                PrimaryKey {
                    codec_version: 1,
                    parts: vec![part(1, cell_value::Kind::Float64Value(1.0))],
                },
                PrimaryKeyCodecError::FloatingPointPart { index: 0 },
            ),
            (
                PrimaryKey {
                    codec_version: 1,
                    parts: vec![
                        part(1, cell_value::Kind::BoolValue(false)),
                        part(1, cell_value::Kind::BoolValue(true)),
                    ],
                },
                PrimaryKeyCodecError::DuplicateColumnId { column_id: 1 },
            ),
            (
                PrimaryKey {
                    codec_version: 1,
                    parts: vec![part(1, cell_value::Kind::Uuid16(vec![0; 15]))],
                },
                PrimaryKeyCodecError::InvalidUuidWidth {
                    index: 0,
                    actual: 15,
                },
            ),
        ];
        for (key, expected) in cases {
            assert_eq!(encode_primary_key(&key), Err(expected));
        }
    }

    #[test]
    fn rejects_every_value_kind_outside_the_v1_key_whitelist() {
        let unsupported = [
            cell_value::Kind::Uint64Value(1),
            cell_value::Kind::DecimalText("1.0".to_owned()),
            cell_value::Kind::TimestampUnixMicros(1),
            cell_value::Kind::JsonText("{}".to_owned()),
            cell_value::Kind::DateIso8601("2026-07-14".to_owned()),
            cell_value::Kind::TimeIso8601("12:00:00".to_owned()),
            cell_value::Kind::IntervalIso8601("PT1S".to_owned()),
        ];

        for kind in unsupported {
            assert_eq!(
                encode_primary_key(&PrimaryKey {
                    codec_version: PRIMARY_KEY_CODEC_VERSION,
                    parts: vec![part(1, kind)],
                }),
                Err(PrimaryKeyCodecError::UnsupportedValueKind { index: 0 })
            );
        }
    }

    #[test]
    fn enforces_version_part_and_byte_limits() {
        assert_eq!(
            encode_primary_key(&PrimaryKey {
                codec_version: 2,
                parts: vec![part(1, cell_value::Kind::BoolValue(true))],
            }),
            Err(PrimaryKeyCodecError::UnsupportedVersion(2))
        );
        assert_eq!(
            decode_primary_key(&vec![0; PRIMARY_KEY_MAX_BYTES + 1]),
            Err(PrimaryKeyCodecError::TooLarge {
                actual: PRIMARY_KEY_MAX_BYTES + 1,
                maximum: PRIMARY_KEY_MAX_BYTES,
            })
        );
    }
}
