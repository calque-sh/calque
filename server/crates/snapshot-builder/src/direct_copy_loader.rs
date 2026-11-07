use std::collections::BTreeSet;

use bigdecimal::BigDecimal;
use chrono::{DateTime, Datelike, NaiveDate, NaiveDateTime, NaiveTime, Utc};
use direct_columnar::{ColumnKind, ColumnSchema, EncodeError, TableSchema, Value};
use source_control::PostgresTypeCodec;
use thiserror::Error;
use uuid::Uuid;

use crate::{CopyLoadLimits, SnapshotTablePlan};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DirectColumnarTableBinding {
    pub collection: String,
    pub dictionary_columns: BTreeSet<String>,
}

pub fn direct_columnar_table_schema(
    table: &SnapshotTablePlan,
    binding: &DirectColumnarTableBinding,
) -> Result<TableSchema, DirectColumnarCopyError> {
    compile_table_schema(&table.columns, &table.primary_key, binding)
}

fn compile_table_schema(
    columns: &[crate::SnapshotColumnPlan],
    primary_key_ids: &[sync_types::ColumnId],
    binding: &DirectColumnarTableBinding,
) -> Result<TableSchema, DirectColumnarCopyError> {
    if binding.collection.is_empty()
        || binding.collection.len() > 255
        || binding.collection.contains('\0')
    {
        return Err(DirectColumnarCopyError::InvalidBinding(
            "collection name is empty, too long, or contains NUL",
        ));
    }
    let selected = columns
        .iter()
        .map(|column| column.local_name.as_str())
        .collect::<BTreeSet<_>>();
    if binding
        .dictionary_columns
        .iter()
        .any(|column| !selected.contains(column.as_str()))
    {
        return Err(DirectColumnarCopyError::InvalidBinding(
            "dictionary column is not selected",
        ));
    }
    let encoded_columns = columns
        .iter()
        .map(|column| {
            let dictionary = binding.dictionary_columns.contains(&column.local_name);
            Ok(ColumnSchema {
                name: column.local_name.clone(),
                kind: direct_kind(column.copy.codec, dictionary)?,
                nullable: column.nullable,
            })
        })
        .collect::<Result<Vec<_>, DirectColumnarCopyError>>()?;
    let primary_key = primary_key_ids
        .iter()
        .map(|column_id| {
            columns
                .iter()
                .find(|column| column.column_id == *column_id)
                .map(|column| column.local_name.clone())
                .ok_or(DirectColumnarCopyError::InvalidBinding(
                    "primary key references an unselected column",
                ))
        })
        .collect::<Result<Vec<_>, _>>()?;
    Ok(TableSchema {
        name: binding.collection.clone(),
        primary_key,
        columns: encoded_columns,
    })
}

fn direct_kind(
    codec: PostgresTypeCodec,
    dictionary: bool,
) -> Result<ColumnKind, DirectColumnarCopyError> {
    let kind = match codec {
        PostgresTypeCodec::Bool => ColumnKind::Bool,
        PostgresTypeCodec::Int2 | PostgresTypeCodec::Int4 | PostgresTypeCodec::Int8 => {
            ColumnKind::Int64
        }
        PostgresTypeCodec::Float4 | PostgresTypeCodec::Float8 => ColumnKind::F64,
        PostgresTypeCodec::Numeric => ColumnKind::Decimal,
        PostgresTypeCodec::Text | PostgresTypeCodec::Varchar | PostgresTypeCodec::Bpchar => {
            if dictionary {
                ColumnKind::Dict
            } else {
                ColumnKind::Utf8
            }
        }
        PostgresTypeCodec::Bytea => ColumnKind::Bytes,
        PostgresTypeCodec::Uuid => ColumnKind::Uuid,
        PostgresTypeCodec::Json | PostgresTypeCodec::Jsonb => ColumnKind::Json,
        PostgresTypeCodec::Date => ColumnKind::Date,
        PostgresTypeCodec::Time => ColumnKind::Time,
        PostgresTypeCodec::Timestamp | PostgresTypeCodec::Timestamptz => ColumnKind::Timestamp,
    };
    if dictionary && kind != ColumnKind::Dict {
        return Err(DirectColumnarCopyError::InvalidBinding(
            "dictionary encoding is valid only for text columns",
        ));
    }
    Ok(kind)
}

/// Decodes one binary COPY row of `table` into the encoder's values and their
/// decoded size, refusing a value or a row over `limits`.
pub fn decode_direct_columnar_row(
    table: &SnapshotTablePlan,
    row: &tokio_postgres::binary_copy::BinaryCopyOutRow,
    limits: CopyLoadLimits,
) -> Result<(Vec<Value>, u64), DirectColumnarCopyError> {
    let mut values = Vec::with_capacity(table.columns.len());
    let mut decoded_bytes = 0_u64;
    for (index, column) in table.columns.iter().enumerate() {
        let (value, value_bytes) = match column.copy.codec {
            PostgresTypeCodec::Bool => fixed(row.try_get::<Option<bool>>(index)?, 1, Value::Bool),
            PostgresTypeCodec::Int2 => fixed(row.try_get::<Option<i16>>(index)?, 8, |value| {
                Value::Int64(i64::from(value))
            }),
            PostgresTypeCodec::Int4 => fixed(row.try_get::<Option<i32>>(index)?, 8, |value| {
                Value::Int64(i64::from(value))
            }),
            PostgresTypeCodec::Int8 => fixed(row.try_get::<Option<i64>>(index)?, 8, Value::Int64),
            PostgresTypeCodec::Float4 => finite_float(
                row.try_get::<Option<f32>>(index)?.map(f64::from),
                &column.source_name,
            )?,
            PostgresTypeCodec::Float8 => {
                finite_float(row.try_get::<Option<f64>>(index)?, &column.source_name)?
            }
            PostgresTypeCodec::Numeric => match row.try_get::<Option<&str>>(index)? {
                None => (Value::Null, 0),
                Some(value) => {
                    enforce_value(&column.source_name, value.len(), limits)?;
                    let normalized = canonical_decimal(value, &column.source_name)?;
                    let bytes = enforce_value(&column.source_name, normalized.len(), limits)?;
                    (Value::DecimalText(normalized), bytes)
                }
            },
            PostgresTypeCodec::Text | PostgresTypeCodec::Varchar | PostgresTypeCodec::Bpchar => {
                match row.try_get::<Option<&str>>(index)? {
                    None => (Value::Null, 0),
                    Some(value) => {
                        let bytes = enforce_value(&column.source_name, value.len(), limits)?;
                        (Value::Text(value.to_owned()), bytes)
                    }
                }
            }
            PostgresTypeCodec::Bytea => match row.try_get::<Option<&[u8]>>(index)? {
                None => (Value::Null, 0),
                Some(value) => {
                    let bytes = enforce_value(&column.source_name, value.len(), limits)?;
                    (Value::Bytes(value.to_vec()), bytes)
                }
            },
            PostgresTypeCodec::Uuid => fixed(row.try_get::<Option<Uuid>>(index)?, 16, |value| {
                Value::Uuid(*value.as_bytes())
            }),
            PostgresTypeCodec::Json | PostgresTypeCodec::Jsonb => {
                match row.try_get::<Option<&str>>(index)? {
                    None => (Value::Null, 0),
                    Some(value) => {
                        enforce_value(&column.source_name, value.len(), limits)?;
                        let canonical = canonical_json(value, &column.source_name)?;
                        let bytes = enforce_value(&column.source_name, canonical.len(), limits)?;
                        (Value::Json(canonical), bytes)
                    }
                }
            }
            PostgresTypeCodec::Date => match row.try_get::<Option<NaiveDate>>(index)? {
                None => (Value::Null, 0),
                Some(value) => {
                    validate_year(&column.source_name, value.year())?;
                    let encoded = value.format("%Y-%m-%d").to_string();
                    let bytes = enforce_value(&column.source_name, encoded.len(), limits)?;
                    (Value::DateText(encoded), bytes)
                }
            },
            PostgresTypeCodec::Time => match row.try_get::<Option<NaiveTime>>(index)? {
                None => (Value::Null, 0),
                Some(value) => {
                    let encoded = value.format("%H:%M:%S%.f").to_string();
                    let bytes = enforce_value(&column.source_name, encoded.len(), limits)?;
                    (Value::TimeText(encoded), bytes)
                }
            },
            PostgresTypeCodec::Timestamp => match row.try_get::<Option<NaiveDateTime>>(index)? {
                None => (Value::Null, 0),
                Some(value) => {
                    validate_year(&column.source_name, value.year())?;
                    (
                        Value::TimestampMicros(value.and_utc().timestamp_micros()),
                        8,
                    )
                }
            },
            PostgresTypeCodec::Timestamptz => match row.try_get::<Option<DateTime<Utc>>>(index)? {
                None => (Value::Null, 0),
                Some(value) => {
                    validate_year(&column.source_name, value.year())?;
                    (Value::TimestampMicros(value.timestamp_micros()), 8)
                }
            },
        };
        if value_bytes > limits.max_value_bytes {
            return Err(DirectColumnarCopyError::ValueByteLimitExceeded {
                column: column.source_name.clone(),
                actual: value_bytes,
                maximum: limits.max_value_bytes,
            });
        }
        decoded_bytes = decoded_bytes
            .checked_add(value_bytes)
            .ok_or(DirectColumnarCopyError::DecodedByteOverflow)?;
        if decoded_bytes > limits.max_row_bytes {
            return Err(DirectColumnarCopyError::RowByteLimitExceeded {
                actual: decoded_bytes,
                maximum: limits.max_row_bytes,
            });
        }
        values.push(value);
    }
    Ok((values, decoded_bytes))
}

fn fixed<T>(value: Option<T>, bytes: u64, map: impl FnOnce(T) -> Value) -> (Value, u64) {
    value.map_or((Value::Null, 0), |value| (map(value), bytes))
}

fn finite_float(value: Option<f64>, column: &str) -> Result<(Value, u64), DirectColumnarCopyError> {
    match value {
        None => Ok((Value::Null, 0)),
        Some(value) if value.is_finite() => Ok((Value::F64(value), 8)),
        Some(_) => Err(DirectColumnarCopyError::InvalidValue {
            column: column.to_owned(),
            reason: "non-finite floating-point values are outside codec v1",
        }),
    }
}

pub(crate) fn canonical_decimal(
    value: &str,
    column: &str,
) -> Result<String, DirectColumnarCopyError> {
    value
        .parse::<BigDecimal>()
        .map(|value| value.normalized().to_plain_string())
        .map_err(|_| DirectColumnarCopyError::InvalidValue {
            column: column.to_owned(),
            reason: "invalid numeric text fallback",
        })
}

pub(crate) fn canonical_json(
    value: &str,
    column: &str,
) -> Result<Vec<u8>, DirectColumnarCopyError> {
    let parsed: serde_json::Value =
        serde_json::from_str(value).map_err(|_| DirectColumnarCopyError::InvalidValue {
            column: column.to_owned(),
            reason: "invalid JSON text fallback",
        })?;
    serde_json::to_vec(&parsed).map_err(DirectColumnarCopyError::Json)
}

pub(crate) fn validate_year(column: &str, year: i32) -> Result<(), DirectColumnarCopyError> {
    if (1..=9_999).contains(&year) {
        Ok(())
    } else {
        Err(DirectColumnarCopyError::InvalidValue {
            column: column.to_owned(),
            reason: "temporal value is outside codec-v1 years 1..=9999",
        })
    }
}

fn enforce_value(
    column: &str,
    size: usize,
    limits: CopyLoadLimits,
) -> Result<u64, DirectColumnarCopyError> {
    let actual = u64::try_from(size).map_err(|_| DirectColumnarCopyError::DecodedByteOverflow)?;
    if actual > limits.max_value_bytes {
        return Err(DirectColumnarCopyError::ValueByteLimitExceeded {
            column: column.to_owned(),
            actual,
            maximum: limits.max_value_bytes,
        });
    }
    Ok(actual)
}

#[derive(Debug, Error)]
pub enum DirectColumnarCopyError {
    #[error("direct-columnar COPY binding is invalid: {0}")]
    InvalidBinding(&'static str),
    #[error("PostgreSQL binary COPY decoding failed: {0}")]
    Postgres(#[from] tokio_postgres::Error),
    #[error("direct-columnar encoding failed: {0}")]
    Encode(#[from] EncodeError),
    #[error("JSON canonicalization failed: {0}")]
    Json(serde_json::Error),
    #[error("COPY decoded byte count overflowed u64")]
    DecodedByteOverflow,
    #[error(
        "COPY column {column} decoded to {actual} bytes, exceeding the per-value limit {maximum}"
    )]
    ValueByteLimitExceeded {
        column: String,
        actual: u64,
        maximum: u64,
    },
    #[error("COPY row decoded to {actual} bytes, exceeding the per-row limit {maximum}")]
    RowByteLimitExceeded { actual: u64, maximum: u64 },
    #[error("COPY column {column} is invalid: {reason}")]
    InvalidValue {
        column: String,
        reason: &'static str,
    },
}

#[cfg(test)]
mod tests {
    use postgres_types::Type;
    use sync_types::ColumnId;

    use super::*;
    use crate::{CopyColumnPlan, SnapshotColumnPlan};

    fn column(id: u32, name: &str, codec: PostgresTypeCodec, nullable: bool) -> SnapshotColumnPlan {
        let postgres_type = match codec {
            PostgresTypeCodec::Int8 => Type::INT8,
            PostgresTypeCodec::Text => Type::TEXT,
            PostgresTypeCodec::Jsonb => Type::TEXT,
            _ => panic!("test codec is not mapped"),
        };
        SnapshotColumnPlan {
            column_id: ColumnId::new(id),
            source_name: name.to_owned(),
            local_name: name.to_owned(),
            nullable,
            copy: CopyColumnPlan {
                codec,
                postgres_type,
                cast_to_text: codec == PostgresTypeCodec::Jsonb,
            },
        }
    }

    fn columns() -> Vec<SnapshotColumnPlan> {
        vec![
            column(1, "id", PostgresTypeCodec::Int8, false),
            column(2, "status", PostgresTypeCodec::Text, false),
            column(3, "payload", PostgresTypeCodec::Jsonb, true),
        ]
    }

    #[test]
    fn explicit_binding_preserves_logical_name_and_physical_text_choice() {
        let binding = DirectColumnarTableBinding {
            collection: "events".to_owned(),
            dictionary_columns: BTreeSet::from(["status".to_owned()]),
        };
        let schema = compile_table_schema(&columns(), &[ColumnId::new(1)], &binding).unwrap();
        assert_eq!(schema.name, "events");
        assert_eq!(schema.primary_key, ["id"]);
        assert_eq!(schema.columns[0].kind, ColumnKind::Int64);
        assert_eq!(schema.columns[1].kind, ColumnKind::Dict);
        assert_eq!(schema.columns[2].kind, ColumnKind::Json);
    }

    #[test]
    fn binding_cannot_request_dictionary_encoding_for_non_text() {
        let binding = DirectColumnarTableBinding {
            collection: "events".to_owned(),
            dictionary_columns: BTreeSet::from(["id".to_owned()]),
        };
        assert!(matches!(
            compile_table_schema(&columns(), &[ColumnId::new(1)], &binding),
            Err(DirectColumnarCopyError::InvalidBinding(
                "dictionary encoding is valid only for text columns"
            ))
        ));
    }

    #[test]
    fn canonical_fallbacks_match_the_encoder_contract() {
        assert_eq!(canonical_decimal("10.5000", "amount").unwrap(), "10.5");
        assert_eq!(
            canonical_json("{ \"b\": 2, \"a\": 1 }", "payload").unwrap(),
            br#"{"a":1,"b":2}"#
        );
        assert!(canonical_json("{", "payload").is_err());
    }
}
