//! Partitioned scopes: one scope per value of the column that leads every
//! partitioned table's primary key.
//!
//! A row belongs to the partition its key's leading part names. The router
//! reads that part from the canonical key, so a row of one partition never
//! reaches another's scope, and an update that changes it is refused as a
//! rebuild boundary for both partitions it touches: neither side can express
//! the move as a patch.

use std::fmt;

use canonical_log::{CanonicalOperation, CanonicalOperationKind, CellValue};
use source_control::PostgresTypeCodec;
use sync_protocol::{change::cell_value::Kind, decode_primary_key};
use sync_types::ColumnId;

use crate::{CanonicalAuthorizationError, PrincipalValue};

/// The value one partitioned scope is bound to, typed by the partition column.
#[derive(Clone, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub enum PartitionValue {
    Int(i64),
    Uuid([u8; 16]),
}

impl PartitionValue {
    /// The value a principal attribute carries for a partition column of
    /// `codec`, or `None` when the attribute's type is not exactly the
    /// column's.
    pub fn from_principal(value: &PrincipalValue, codec: PostgresTypeCodec) -> Option<Self> {
        match (codec, value) {
            (PostgresTypeCodec::Int2, PrincipalValue::Int2(value)) => {
                Some(Self::Int(i64::from(*value)))
            }
            (PostgresTypeCodec::Int4, PrincipalValue::Int4(value)) => {
                Some(Self::Int(i64::from(*value)))
            }
            (PostgresTypeCodec::Int8, PrincipalValue::Int8(value)) => Some(Self::Int(*value)),
            (PostgresTypeCodec::Uuid, PrincipalValue::Uuid(value)) => Some(Self::Uuid(*value)),
            _ => None,
        }
    }

    /// Whether a partition column may be of `codec`.
    pub const fn supports(codec: PostgresTypeCodec) -> bool {
        matches!(
            codec,
            PostgresTypeCodec::Int2
                | PostgresTypeCodec::Int4
                | PostgresTypeCodec::Int8
                | PostgresTypeCodec::Uuid
        )
    }

    /// The value as a SQL literal of `codec`. It is rendered from the typed
    /// value alone, digits or hexadecimal, so no input text reaches SQL.
    pub fn sql_literal(&self, codec: PostgresTypeCodec) -> String {
        format!("'{self}'::{}", codec.catalog_name())
    }

    pub(crate) fn hash_frame(&self) -> Vec<u8> {
        match self {
            Self::Int(value) => {
                let mut bytes = vec![1];
                bytes.extend_from_slice(&value.to_be_bytes());
                bytes
            }
            Self::Uuid(value) => {
                let mut bytes = vec![2];
                bytes.extend_from_slice(value);
                bytes
            }
        }
    }

    fn matches_key_part(&self, kind: &Kind) -> Result<bool, CanonicalAuthorizationError> {
        match (self, kind) {
            (Self::Int(expected), Kind::Int64Value(actual)) => Ok(expected == actual),
            (Self::Uuid(expected), Kind::Uuid16(actual)) => Ok(expected.as_slice() == actual),
            _ => Err(CanonicalAuthorizationError::InvalidPartitionKey),
        }
    }

    fn matches_cell(&self, value: &CellValue) -> Result<bool, CanonicalAuthorizationError> {
        match (self, value) {
            (Self::Int(expected), CellValue::Int64(actual)) => Ok(expected == actual),
            (Self::Uuid(expected), CellValue::Uuid16(actual)) => Ok(expected == actual),
            _ => Err(CanonicalAuthorizationError::InvalidPartitionKey),
        }
    }

    fn from_key_part(kind: &Kind) -> Result<Self, CanonicalAuthorizationError> {
        match kind {
            Kind::Int64Value(value) => Ok(Self::Int(*value)),
            Kind::Uuid16(value) => <[u8; 16]>::try_from(value.as_slice())
                .map(Self::Uuid)
                .map_err(|_| CanonicalAuthorizationError::InvalidPartitionKey),
            _ => Err(CanonicalAuthorizationError::InvalidPartitionKey),
        }
    }

    fn from_cell(value: &CellValue) -> Result<Self, CanonicalAuthorizationError> {
        match value {
            CellValue::Int64(value) => Ok(Self::Int(*value)),
            CellValue::Uuid16(value) => Ok(Self::Uuid(*value)),
            _ => Err(CanonicalAuthorizationError::InvalidPartitionKey),
        }
    }
}

impl fmt::Display for PartitionValue {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Int(value) => write!(formatter, "{value}"),
            Self::Uuid(bytes) => {
                for (index, byte) in bytes.iter().enumerate() {
                    if matches!(index, 4 | 6 | 8 | 10) {
                        formatter.write_str("-")?;
                    }
                    write!(formatter, "{byte:02x}")?;
                }
                Ok(())
            }
        }
    }
}

impl fmt::Debug for PartitionValue {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "PartitionValue({self})")
    }
}

/// What one operation does to the partitions of a partitioned table.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PartitionTouch {
    /// A row of one partition.
    Row(PartitionValue),
    /// An update that moves a row from one partition to another.
    Moved {
        from: PartitionValue,
        to: PartitionValue,
    },
    /// A truncate, which clears the table in every partition.
    Every,
}

/// Reads the partition one operation of a partitioned table touches from its
/// canonical key, whose leading part must be `column`, and from an update's
/// new value of that column.
pub(crate) fn partition_touch(
    operation: &CanonicalOperation,
    column: ColumnId,
) -> Result<PartitionTouch, CanonicalAuthorizationError> {
    if matches!(
        operation.kind,
        CanonicalOperationKind::TruncateAuthorizedTable { .. }
    ) {
        return Ok(PartitionTouch::Every);
    }
    let kind = leading_key_part(&operation.primary_key, column)?;
    let from = PartitionValue::from_key_part(&kind)?;
    if let Some(new) = moved_value(operation, column) {
        let to = PartitionValue::from_cell(new)?;
        if to != from {
            return Ok(PartitionTouch::Moved { from, to });
        }
    }
    Ok(PartitionTouch::Row(from))
}

/// Whether one operation of a partitioned table belongs to the partition of
/// `value`: `Ok(true)` to deliver it, `Ok(false)` to omit it, and
/// [`CanonicalAuthorizationError::PartitionMoved`] for a move into or out of
/// it.
pub(crate) fn routes_to(
    operation: &CanonicalOperation,
    column: ColumnId,
    value: &PartitionValue,
) -> Result<bool, CanonicalAuthorizationError> {
    if matches!(
        operation.kind,
        CanonicalOperationKind::TruncateAuthorizedTable { .. }
    ) {
        return Ok(true);
    }
    let kind = leading_key_part(&operation.primary_key, column)?;
    let owned = value.matches_key_part(&kind)?;
    if let Some(new) = moved_value(operation, column) {
        if value.matches_cell(new)? != owned {
            return Err(CanonicalAuthorizationError::PartitionMoved);
        }
    }
    Ok(owned)
}

/// Whether a row key of a partitioned table names the partition of `value`.
pub(crate) fn key_in_partition(
    primary_key: &[u8],
    column: ColumnId,
    value: &PartitionValue,
) -> Result<bool, CanonicalAuthorizationError> {
    value.matches_key_part(&leading_key_part(primary_key, column)?)
}

fn leading_key_part(
    primary_key: &[u8],
    column: ColumnId,
) -> Result<Kind, CanonicalAuthorizationError> {
    let key = decode_primary_key(primary_key)
        .map_err(|_| CanonicalAuthorizationError::InvalidPartitionKey)?;
    let first = key
        .parts
        .into_iter()
        .next()
        .ok_or(CanonicalAuthorizationError::InvalidPartitionKey)?;
    if first.column_id != column.get() {
        return Err(CanonicalAuthorizationError::InvalidPartitionKey);
    }
    first
        .value
        .and_then(|value| value.kind)
        .ok_or(CanonicalAuthorizationError::InvalidPartitionKey)
}

fn moved_value(operation: &CanonicalOperation, column: ColumnId) -> Option<&CellValue> {
    let CanonicalOperationKind::UpdatePatch { changed_columns } = &operation.kind else {
        return None;
    };
    changed_columns
        .iter()
        .find(|changed| changed.column_id == column)
        .map(|changed| &changed.value)
}
