//! The rows the temporary stream changed between the exported snapshot and the
//! cut, held by primary key until COPY reaches them.
//!
//! The direct-columnar encoder is append-only, so a direct build rolls the
//! temporary stream forward before it copies, into this overlay, and streams
//! each table's COPY through it. COPY returns each row as the snapshot held it;
//! the overlay replaces, drops or keeps it, and appends the rows the
//! roll-forward inserted once the table's COPY ends.
//!
//! Each touched key gets one slot: `base` records whether the snapshot held a
//! row there, and `image` is the row the key holds at the cut, either a patch
//! against a snapshot row not yet copied (`FromBase`) or a row the roll-forward
//! inserted (`Full`). A primary-key change moves the image to its new key, so
//! the patch is applied when COPY returns the row at its old key.
//!
//! Every inconsistency fails closed: an insert over a live row, a change to a
//! missing row, a COPY row at a key the roll-forward inserted, a snapshot row
//! the roll-forward changed that COPY never returned, and a truncate.

use std::collections::HashMap;

use canonical_log::{CanonicalOperationKind, CanonicalTransaction, CellValue, ColumnValue};
use chrono::{DateTime, Datelike, NaiveDate};
use direct_columnar::Value;
use postgres_types::PgLsn;
use source_control::PostgresTypeCodec;
use sync_protocol::{change::cell_value, decode_primary_key, PrimaryKeyCodecError};
use sync_types::{ColumnId, SourceEpoch, TableId};
use thiserror::Error;

use crate::{
    direct_copy_loader::{canonical_decimal, canonical_json, validate_year},
    DirectColumnarCopyError, SnapshotTablePlan,
};

/// One roll-forward transaction's effect on the overlay.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct OverlayApplyStats {
    pub(crate) operations: u64,
    pub(crate) end_lsn: PgLsn,
}

pub(crate) struct DirectOverlay {
    source_epoch: SourceEpoch,
    last_end_lsn: PgLsn,
    tables: Vec<OverlayTable>,
    table_indexes: HashMap<TableId, usize>,
}

struct OverlayTable {
    table_id: TableId,
    columns: Vec<OverlayColumn>,
    positions: HashMap<ColumnId, usize>,
    /// Plan positions of the primary-key columns, in key ordinal order.
    key_positions: Vec<usize>,
    slots: HashMap<Vec<u8>, Slot>,
    /// Snapshot key -> the key whose slot holds the `FromBase` image derived
    /// from that snapshot row. Exactly one entry per live `FromBase` image.
    base_refs: HashMap<Vec<u8>, Vec<u8>>,
    key_buffer: Vec<u8>,
    finished: bool,
}

struct OverlayColumn {
    column_id: ColumnId,
    name: String,
    codec: PostgresTypeCodec,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Base {
    Present,
    Absent,
}

struct Slot {
    base: Base,
    base_seen: bool,
    image: Image,
}

enum Image {
    None,
    Full(Vec<Value>),
    FromBase {
        base_key: Vec<u8>,
        patch: Vec<Option<Value>>,
    },
    /// A `FromBase` image already merged into the COPY row it patched.
    Emitted,
}

impl DirectOverlay {
    pub(crate) fn new(
        tables: &[SnapshotTablePlan],
        source_epoch: SourceEpoch,
        copy_consistent_lsn: PgLsn,
    ) -> Result<Self, DirectOverlayError> {
        let mut compiled = Vec::with_capacity(tables.len());
        let mut table_indexes = HashMap::with_capacity(tables.len());
        for (index, table) in tables.iter().enumerate() {
            if table_indexes
                .insert(table.table_id.clone(), index)
                .is_some()
            {
                return Err(DirectOverlayError::DuplicateTable(table.table_id.clone()));
            }
            let positions = table
                .columns
                .iter()
                .enumerate()
                .map(|(position, column)| (column.column_id, position))
                .collect::<HashMap<_, _>>();
            let key_positions = table
                .primary_key
                .iter()
                .map(|column_id| {
                    positions.get(column_id).copied().ok_or_else(|| {
                        DirectOverlayError::UnknownColumn {
                            table: table.table_id.clone(),
                            column_id: *column_id,
                        }
                    })
                })
                .collect::<Result<Vec<_>, _>>()?;
            if key_positions.is_empty() {
                return Err(DirectOverlayError::EmptyPrimaryKey(table.table_id.clone()));
            }
            compiled.push(OverlayTable {
                table_id: table.table_id.clone(),
                columns: table
                    .columns
                    .iter()
                    .map(|column| OverlayColumn {
                        column_id: column.column_id,
                        name: column.source_name.clone(),
                        codec: column.copy.codec,
                    })
                    .collect(),
                positions,
                key_positions,
                slots: HashMap::new(),
                base_refs: HashMap::new(),
                key_buffer: Vec::new(),
                finished: false,
            });
        }
        Ok(Self {
            source_epoch,
            last_end_lsn: copy_consistent_lsn,
            tables: compiled,
            table_indexes,
        })
    }

    pub(crate) fn last_end_lsn(&self) -> PgLsn {
        self.last_end_lsn
    }

    /// Applies one committed, authorization-routed transaction. Transactions
    /// arrive in commit order. A failed transaction may leave the overlay
    /// partly applied; the caller must abandon the build.
    pub(crate) fn apply_transaction(
        &mut self,
        transaction: CanonicalTransaction,
    ) -> Result<OverlayApplyStats, DirectOverlayError> {
        if transaction.key.source_epoch != self.source_epoch {
            return Err(DirectOverlayError::EpochMismatch {
                expected: self.source_epoch.clone(),
                actual: transaction.key.source_epoch,
            });
        }
        let begin_lsn = parse_lsn(&transaction.begin_lsn, "begin_lsn")?;
        let commit_lsn = parse_lsn(&transaction.commit_lsn, "commit_lsn")?;
        let end_lsn = parse_lsn(&transaction.key.end_lsn, "end_lsn")?;
        if begin_lsn > commit_lsn || commit_lsn > end_lsn {
            return Err(DirectOverlayError::InvalidTransactionLsnOrder);
        }
        if end_lsn <= self.last_end_lsn {
            return Err(DirectOverlayError::NonMonotonicEndLsn {
                previous: self.last_end_lsn,
                actual: end_lsn,
            });
        }
        let operations = u64::try_from(transaction.operations.len())
            .map_err(|_| DirectOverlayError::Overflow)?;
        for operation in transaction.operations {
            let index = *self
                .table_indexes
                .get(&operation.table_id)
                .ok_or_else(|| DirectOverlayError::UnknownTable(operation.table_id.clone()))?;
            let table = &mut self.tables[index];
            match operation.kind {
                CanonicalOperationKind::InsertFullRow { columns } => {
                    table.insert(&operation.primary_key, columns)?;
                }
                CanonicalOperationKind::UpdatePatch { changed_columns } => {
                    table.update(&operation.primary_key, changed_columns)?;
                }
                CanonicalOperationKind::DeleteByKey => table.delete(&operation.primary_key)?,
                CanonicalOperationKind::TruncateAuthorizedTable { .. } => {
                    return Err(DirectOverlayError::Truncate(operation.table_id));
                }
            }
        }
        self.last_end_lsn = end_lsn;
        Ok(OverlayApplyStats {
            operations,
            end_lsn,
        })
    }

    /// Passes one snapshot row of `table_index` through the overlay: the row
    /// itself, its patched image, or nothing when the roll-forward deleted it.
    pub(crate) fn merge_copy_row(
        &mut self,
        table_index: usize,
        row: Vec<Value>,
    ) -> Result<Option<Vec<Value>>, DirectOverlayError> {
        let table = self
            .tables
            .get_mut(table_index)
            .ok_or(DirectOverlayError::UnknownTableIndex(table_index))?;
        if table.finished {
            return Err(DirectOverlayError::TableFinished(table.table_id.clone()));
        }
        if table.slots.is_empty() {
            return Ok(Some(row));
        }
        table.key_buffer.clear();
        encode_key_into(&row, &table.key_positions, &mut table.key_buffer)
            .map_err(|column| table.invalid_key(column))?;
        let Some(slot) = table.slots.get_mut(&table.key_buffer) else {
            return Ok(Some(row));
        };
        if slot.base == Base::Absent {
            return Err(DirectOverlayError::CopiedRowWasInserted(
                table.table_id.clone(),
            ));
        }
        if slot.base_seen {
            return Err(DirectOverlayError::DuplicateCopiedRow(
                table.table_id.clone(),
            ));
        }
        slot.base_seen = true;
        let Some(holder) = table.base_refs.remove(&table.key_buffer) else {
            // The roll-forward deleted this snapshot row (possibly after
            // moving it); whatever the key holds now is a `Full` image.
            return Ok(None);
        };
        let holder_slot = table
            .slots
            .get_mut(&holder)
            .ok_or_else(|| DirectOverlayError::OverlayCorrupt(table.table_id.clone()))?;
        let Image::FromBase { base_key, patch } =
            std::mem::replace(&mut holder_slot.image, Image::Emitted)
        else {
            return Err(DirectOverlayError::OverlayCorrupt(table.table_id.clone()));
        };
        if base_key != table.key_buffer {
            return Err(DirectOverlayError::OverlayCorrupt(table.table_id.clone()));
        }
        let mut row = row;
        for (value, replacement) in row.iter_mut().zip(patch) {
            if let Some(replacement) = replacement {
                *value = replacement;
            }
        }
        table.key_buffer.clear();
        encode_key_into(&row, &table.key_positions, &mut table.key_buffer)
            .map_err(|column| table.invalid_key(column))?;
        if table.key_buffer != holder {
            return Err(DirectOverlayError::MergedKeyMismatch(
                table.table_id.clone(),
            ));
        }
        Ok(Some(row))
    }

    /// Ends `table_index`'s COPY: proves every snapshot row the roll-forward
    /// changed was copied, and returns the rows it inserted, in key order.
    pub(crate) fn finish_table(
        &mut self,
        table_index: usize,
    ) -> Result<Vec<Vec<Value>>, DirectOverlayError> {
        let table = self
            .tables
            .get_mut(table_index)
            .ok_or(DirectOverlayError::UnknownTableIndex(table_index))?;
        if table.finished {
            return Err(DirectOverlayError::TableFinished(table.table_id.clone()));
        }
        table.finished = true;
        if !table.base_refs.is_empty() {
            return Err(DirectOverlayError::MissingCopiedRow(table.table_id.clone()));
        }
        let mut inserted = Vec::new();
        for (key, slot) in std::mem::take(&mut table.slots) {
            if slot.base == Base::Present && !slot.base_seen {
                return Err(DirectOverlayError::MissingCopiedRow(table.table_id.clone()));
            }
            match slot.image {
                Image::Full(row) => inserted.push((key, row)),
                Image::FromBase { .. } => {
                    return Err(DirectOverlayError::MissingCopiedRow(table.table_id.clone()));
                }
                Image::None | Image::Emitted => {}
            }
        }
        inserted.sort_unstable_by(|left, right| left.0.cmp(&right.0));
        Ok(inserted.into_iter().map(|(_, row)| row).collect())
    }
}

impl OverlayTable {
    fn insert(
        &mut self,
        encoded_key: &[u8],
        columns: Vec<ColumnValue>,
    ) -> Result<(), DirectOverlayError> {
        let key = self.decode_key(encoded_key)?;
        let row = self.full_row(columns)?;
        let mut row_key = Vec::new();
        encode_key_into(&row, &self.key_positions, &mut row_key)
            .map_err(|column| self.invalid_key(column))?;
        if row_key != key {
            return Err(DirectOverlayError::InsertPrimaryKeyMismatch(
                self.table_id.clone(),
            ));
        }
        let slot = self.slots.entry(key).or_insert(Slot {
            base: Base::Absent,
            base_seen: false,
            image: Image::None,
        });
        if !matches!(slot.image, Image::None) {
            return Err(DirectOverlayError::InsertOverRow(self.table_id.clone()));
        }
        slot.image = Image::Full(row);
        Ok(())
    }

    fn update(
        &mut self,
        encoded_key: &[u8],
        changed_columns: Vec<ColumnValue>,
    ) -> Result<(), DirectOverlayError> {
        let key_values = self.decode_key_values(encoded_key)?;
        let key = self.key_bytes(&key_values)?;
        let patch = self.patch(changed_columns)?;
        let mut image = self.take_image(&key)?;
        match &mut image {
            Image::Full(row) => {
                for (value, replacement) in row.iter_mut().zip(&patch) {
                    if let Some(replacement) = replacement {
                        value.clone_from(replacement);
                    }
                }
            }
            Image::FromBase { patch: pending, .. } => {
                for (value, replacement) in pending.iter_mut().zip(&patch) {
                    if replacement.is_some() {
                        value.clone_from(replacement);
                    }
                }
            }
            Image::None | Image::Emitted => unreachable!("take_image returns a live image"),
        }
        // Key columns absent from the patch keep their value; the operation's
        // key is where the image lives now.
        let new_key_values = self
            .key_positions
            .iter()
            .zip(key_values)
            .map(|(position, old)| patch[*position].clone().unwrap_or(old))
            .collect::<Vec<_>>();
        let new_key = self.key_bytes(&new_key_values)?;
        if new_key == key {
            self.slots
                .get_mut(&key)
                .expect("take_image leaves the slot in place")
                .image = image;
            return Ok(());
        }
        if let Image::FromBase { base_key, .. } = &image {
            let holder = self
                .base_refs
                .get_mut(base_key)
                .ok_or_else(|| DirectOverlayError::OverlayCorrupt(self.table_id.clone()))?;
            if *holder != key {
                return Err(DirectOverlayError::OverlayCorrupt(self.table_id.clone()));
            }
            holder.clone_from(&new_key);
        }
        let target = self.slots.entry(new_key).or_insert(Slot {
            base: Base::Absent,
            base_seen: false,
            image: Image::None,
        });
        if !matches!(target.image, Image::None) {
            return Err(DirectOverlayError::KeyCollision(self.table_id.clone()));
        }
        target.image = image;
        Ok(())
    }

    fn delete(&mut self, encoded_key: &[u8]) -> Result<(), DirectOverlayError> {
        let key = self.decode_key(encoded_key)?;
        if let Image::FromBase { base_key, .. } = self.take_image(&key)? {
            if self.base_refs.remove(&base_key).as_ref() != Some(&key) {
                return Err(DirectOverlayError::OverlayCorrupt(self.table_id.clone()));
            }
        }
        Ok(())
    }

    /// Removes the live image at `key`, leaving `Image::None` in its slot. A
    /// key the roll-forward has not touched yet held a snapshot row, so its
    /// first update or delete opens a slot with a `FromBase` image of it.
    fn take_image(&mut self, key: &[u8]) -> Result<Image, DirectOverlayError> {
        if !self.slots.contains_key(key) {
            self.base_refs.insert(key.to_vec(), key.to_vec());
            self.slots.insert(
                key.to_vec(),
                Slot {
                    base: Base::Present,
                    base_seen: false,
                    image: Image::FromBase {
                        base_key: key.to_vec(),
                        patch: vec![None; self.columns.len()],
                    },
                },
            );
        }
        let slot = self.slots.get_mut(key).expect("slot inserted above");
        match std::mem::replace(&mut slot.image, Image::None) {
            Image::None | Image::Emitted => {
                Err(DirectOverlayError::MissingRow(self.table_id.clone()))
            }
            image => Ok(image),
        }
    }

    fn full_row(&self, columns: Vec<ColumnValue>) -> Result<Vec<Value>, DirectOverlayError> {
        let mut row = vec![None; self.columns.len()];
        for column in columns {
            let position = self.position(column.column_id)?;
            if row[position].is_some() {
                return Err(self.duplicate_column(column.column_id));
            }
            row[position] = Some(self.value(position, &column.value)?);
        }
        row.into_iter()
            .collect::<Option<Vec<_>>>()
            .ok_or_else(|| DirectOverlayError::IncompleteInsertRow(self.table_id.clone()))
    }

    fn patch(
        &self,
        changed_columns: Vec<ColumnValue>,
    ) -> Result<Vec<Option<Value>>, DirectOverlayError> {
        if changed_columns.is_empty() {
            return Err(DirectOverlayError::EmptyUpdatePatch(self.table_id.clone()));
        }
        let mut patch = vec![None; self.columns.len()];
        for column in changed_columns {
            let position = self.position(column.column_id)?;
            if patch[position].is_some() {
                return Err(self.duplicate_column(column.column_id));
            }
            patch[position] = Some(self.value(position, &column.value)?);
        }
        Ok(patch)
    }

    fn decode_key(&self, encoded: &[u8]) -> Result<Vec<u8>, DirectOverlayError> {
        let values = self.decode_key_values(encoded)?;
        self.key_bytes(&values)
    }

    /// The operation's primary key as direct values, in key ordinal order.
    fn decode_key_values(&self, encoded: &[u8]) -> Result<Vec<Value>, DirectOverlayError> {
        let key = decode_primary_key(encoded)?;
        let actual = key
            .parts
            .iter()
            .map(|part| ColumnId::new(part.column_id))
            .collect::<Vec<_>>();
        let expected = self
            .key_positions
            .iter()
            .map(|position| self.columns[*position].column_id)
            .collect::<Vec<_>>();
        if actual != expected {
            return Err(DirectOverlayError::PrimaryKeyLayoutMismatch {
                table: self.table_id.clone(),
                expected,
                actual,
            });
        }
        key.parts
            .iter()
            .zip(&self.key_positions)
            .map(|(part, position)| {
                let cell = part
                    .value
                    .as_ref()
                    .and_then(|value| value.kind.as_ref())
                    .map(wire_cell)
                    .transpose()
                    .map_err(|()| self.invalid_key(*position))?
                    .ok_or_else(|| self.invalid_key(*position))?;
                self.value(*position, &cell)
            })
            .collect()
    }

    fn key_bytes(&self, values: &[Value]) -> Result<Vec<u8>, DirectOverlayError> {
        let mut key = Vec::new();
        for (index, value) in values.iter().enumerate() {
            push_key_value(value, &mut key)
                .map_err(|()| self.invalid_key(self.key_positions[index]))?;
        }
        Ok(key)
    }

    fn value(&self, position: usize, value: &CellValue) -> Result<Value, DirectOverlayError> {
        let column = &self.columns[position];
        direct_value(column.codec, value, &column.name).map_err(|reason| match reason {
            ValueRefusal::Shape => DirectOverlayError::ValueShape {
                table: self.table_id.clone(),
                column: column.name.clone(),
            },
            ValueRefusal::Invalid(error) => DirectOverlayError::InvalidValue(error),
        })
    }

    fn position(&self, column_id: ColumnId) -> Result<usize, DirectOverlayError> {
        self.positions
            .get(&column_id)
            .copied()
            .ok_or_else(|| DirectOverlayError::UnknownColumn {
                table: self.table_id.clone(),
                column_id,
            })
    }

    fn duplicate_column(&self, column_id: ColumnId) -> DirectOverlayError {
        DirectOverlayError::DuplicateColumn {
            table: self.table_id.clone(),
            column_id,
        }
    }

    fn invalid_key(&self, position: usize) -> DirectOverlayError {
        DirectOverlayError::InvalidKeyValue {
            table: self.table_id.clone(),
            column: self.columns[position].name.clone(),
        }
    }
}

/// Appends the key columns of `row` to `key`; the error is the offending
/// column's plan position.
fn encode_key_into(row: &[Value], positions: &[usize], key: &mut Vec<u8>) -> Result<(), usize> {
    for &position in positions {
        let value = row.get(position).ok_or(position)?;
        push_key_value(value, key).map_err(|()| position)?;
    }
    Ok(())
}

/// A type-tagged, length-prefixed key part. Only the overlay compares these
/// bytes, and only with bytes it made itself, so all it needs is that equal
/// direct values produce equal bytes and unequal ones do not.
fn push_key_value(value: &Value, key: &mut Vec<u8>) -> Result<(), ()> {
    let variable = |tag: u8, bytes: &[u8], key: &mut Vec<u8>| {
        let length = u32::try_from(bytes.len()).map_err(|_| ())?;
        key.push(tag);
        key.extend_from_slice(&length.to_le_bytes());
        key.extend_from_slice(bytes);
        Ok::<(), ()>(())
    };
    match value {
        Value::Int64(value) => {
            key.push(1);
            key.extend_from_slice(&value.to_le_bytes());
        }
        Value::Uint64(value) => {
            key.push(2);
            key.extend_from_slice(&value.to_le_bytes());
        }
        Value::TimestampMicros(value) => {
            key.push(3);
            key.extend_from_slice(&value.to_le_bytes());
        }
        Value::Bool(value) => key.extend_from_slice(&[4, u8::from(*value)]),
        Value::Text(value) => variable(5, value.as_bytes(), key)?,
        Value::Bytes(value) => variable(6, value, key)?,
        Value::Uuid(value) => {
            key.push(7);
            key.extend_from_slice(value);
        }
        Value::DecimalText(value) => variable(8, value.as_bytes(), key)?,
        Value::DateText(value) => variable(9, value.as_bytes(), key)?,
        Value::TimeText(value) => variable(10, value.as_bytes(), key)?,
        Value::IntervalText(value) => variable(11, value.as_bytes(), key)?,
        Value::Null | Value::F64(_) | Value::Json(_) => return Err(()),
    }
    Ok(())
}

fn wire_cell(kind: &cell_value::Kind) -> Result<CellValue, ()> {
    Ok(match kind {
        cell_value::Kind::NullValue(_) => CellValue::Null,
        cell_value::Kind::Int64Value(value) => CellValue::Int64(*value),
        cell_value::Kind::Uint64Value(value) => CellValue::Uint64(*value),
        cell_value::Kind::Float64Value(value) => CellValue::Float64(*value),
        cell_value::Kind::TextValue(value) => CellValue::Text(value.clone()),
        cell_value::Kind::DecimalText(value) => CellValue::DecimalText(value.clone()),
        cell_value::Kind::JsonText(value) => CellValue::JsonText(value.clone()),
        cell_value::Kind::DateIso8601(value) => CellValue::DateIso8601(value.clone()),
        cell_value::Kind::TimeIso8601(value) => CellValue::TimeIso8601(value.clone()),
        cell_value::Kind::IntervalIso8601(value) => CellValue::IntervalIso8601(value.clone()),
        cell_value::Kind::BlobValue(value) => CellValue::Blob(value.clone()),
        cell_value::Kind::BoolValue(value) => CellValue::Bool(*value),
        cell_value::Kind::TimestampUnixMicros(value) => CellValue::TimestampUnixMicros(*value),
        cell_value::Kind::Uuid16(value) => {
            CellValue::Uuid16(<[u8; 16]>::try_from(value.as_slice()).map_err(|_| ())?)
        }
    })
}

enum ValueRefusal {
    Shape,
    Invalid(DirectColumnarCopyError),
}

impl From<DirectColumnarCopyError> for ValueRefusal {
    fn from(error: DirectColumnarCopyError) -> Self {
        Self::Invalid(error)
    }
}

/// The direct value binary COPY would have decoded for the PostgreSQL value
/// the change stream carried. Both halves of one artifact go through the same
/// canonical forms: normalized decimal text, canonical JSON bytes, ISO dates,
/// microsecond timestamps and the codec-v1 year range. The pairing of codec and
/// canonical kind is strict; a mismatch is refused, never coerced.
fn direct_value(
    codec: PostgresTypeCodec,
    value: &CellValue,
    column: &str,
) -> Result<Value, ValueRefusal> {
    Ok(match (codec, value) {
        (_, CellValue::Null) => Value::Null,
        (PostgresTypeCodec::Bool, CellValue::Bool(value)) => Value::Bool(*value),
        (
            PostgresTypeCodec::Int2 | PostgresTypeCodec::Int4 | PostgresTypeCodec::Int8,
            CellValue::Int64(value),
        ) => Value::Int64(*value),
        (PostgresTypeCodec::Float4 | PostgresTypeCodec::Float8, CellValue::Float64(value)) => {
            if !value.is_finite() {
                return Err(ValueRefusal::Invalid(
                    DirectColumnarCopyError::InvalidValue {
                        column: column.to_owned(),
                        reason: "non-finite floating-point values are outside codec v1",
                    },
                ));
            }
            Value::F64(*value)
        }
        (PostgresTypeCodec::Numeric, CellValue::DecimalText(value)) => {
            Value::DecimalText(canonical_decimal(value, column)?)
        }
        (
            PostgresTypeCodec::Text | PostgresTypeCodec::Varchar | PostgresTypeCodec::Bpchar,
            CellValue::Text(value),
        ) => Value::Text(value.clone()),
        (PostgresTypeCodec::Bytea, CellValue::Blob(value)) => Value::Bytes(value.clone()),
        (PostgresTypeCodec::Uuid, CellValue::Uuid16(value)) => Value::Uuid(*value),
        (PostgresTypeCodec::Json | PostgresTypeCodec::Jsonb, CellValue::JsonText(value)) => {
            Value::Json(canonical_json(value, column)?)
        }
        (PostgresTypeCodec::Date, CellValue::DateIso8601(value)) => {
            let date = NaiveDate::parse_from_str(value, "%Y-%m-%d").map_err(|_| {
                DirectColumnarCopyError::InvalidValue {
                    column: column.to_owned(),
                    reason: "date is not canonical ISO text",
                }
            })?;
            validate_year(column, date.year())?;
            Value::DateText(value.clone())
        }
        (PostgresTypeCodec::Time, CellValue::TimeIso8601(value)) => Value::TimeText(value.clone()),
        (
            PostgresTypeCodec::Timestamp | PostgresTypeCodec::Timestamptz,
            CellValue::TimestampUnixMicros(value),
        ) => {
            let year = DateTime::from_timestamp_micros(*value)
                .map(|timestamp| timestamp.year())
                .ok_or_else(|| DirectColumnarCopyError::InvalidValue {
                    column: column.to_owned(),
                    reason: "timestamp is outside the representable range",
                })?;
            validate_year(column, year)?;
            Value::TimestampMicros(*value)
        }
        _ => return Err(ValueRefusal::Shape),
    })
}

fn parse_lsn(value: &str, field: &'static str) -> Result<PgLsn, DirectOverlayError> {
    value
        .parse()
        .map_err(|_| DirectOverlayError::InvalidLsn { field })
}

#[derive(Debug, Error)]
pub enum DirectOverlayError {
    #[error("canonical primary-key codec rejected an operation: {0}")]
    PrimaryKey(#[from] PrimaryKeyCodecError),
    #[error("direct build plans table {0} twice")]
    DuplicateTable(TableId),
    #[error("table {0} has no primary key")]
    EmptyPrimaryKey(TableId),
    #[error("transaction belongs to source epoch {actual}, expected {expected}")]
    EpochMismatch {
        expected: SourceEpoch,
        actual: SourceEpoch,
    },
    #[error("transaction {field} is not a PostgreSQL LSN")]
    InvalidLsn { field: &'static str },
    #[error("transaction requires begin_lsn <= commit_lsn <= end_lsn")]
    InvalidTransactionLsnOrder,
    #[error("transaction end LSN {actual} does not follow applied LSN {previous}")]
    NonMonotonicEndLsn { previous: PgLsn, actual: PgLsn },
    #[error("canonical operation targets table {0}, which the build does not copy")]
    UnknownTable(TableId),
    #[error("direct build has no table at index {0}")]
    UnknownTableIndex(usize),
    #[error("COPY of {0} already ended")]
    TableFinished(TableId),
    #[error("operation for {table} references unknown column {column_id:?}")]
    UnknownColumn { table: TableId, column_id: ColumnId },
    #[error("operation for {table} repeats column {column_id:?}")]
    DuplicateColumn { table: TableId, column_id: ColumnId },
    #[error("insert for {0} is not a full row")]
    IncompleteInsertRow(TableId),
    #[error("insert for {0} carries a primary key its row does not")]
    InsertPrimaryKeyMismatch(TableId),
    #[error("update patch for {0} is empty")]
    EmptyUpdatePatch(TableId),
    #[error("primary-key layout for {table} differs from schema: expected {expected:?}, found {actual:?}")]
    PrimaryKeyLayoutMismatch {
        table: TableId,
        expected: Vec<ColumnId>,
        actual: Vec<ColumnId>,
    },
    #[error("column {column} of {table} does not carry its PostgreSQL type")]
    ValueShape { table: TableId, column: String },
    #[error(transparent)]
    InvalidValue(DirectColumnarCopyError),
    #[error("primary-key column {column} of {table} holds a value that cannot key a row")]
    InvalidKeyValue { table: TableId, column: String },
    #[error("roll-forward inserted a {0} key that already holds a row")]
    InsertOverRow(TableId),
    #[error("roll-forward changed a {0} row that does not exist")]
    MissingRow(TableId),
    #[error("roll-forward moved a {0} row onto a key that already holds one")]
    KeyCollision(TableId),
    #[error("{0} was truncated during the build; the build must start again")]
    Truncate(TableId),
    #[error("COPY of {0} returned a row at a key the roll-forward inserted")]
    CopiedRowWasInserted(TableId),
    #[error("COPY of {0} returned one key twice")]
    DuplicateCopiedRow(TableId),
    #[error("COPY of {0} did not return a row the roll-forward changed")]
    MissingCopiedRow(TableId),
    #[error("patched {0} row does not carry the key the roll-forward moved it to")]
    MergedKeyMismatch(TableId),
    #[error("overlay bookkeeping for {0} is inconsistent")]
    OverlayCorrupt(TableId),
    #[error("overlay statistic overflowed")]
    Overflow,
}

#[cfg(test)]
mod tests {
    use canonical_log::{CanonicalOperation, TransactionKey};
    use postgres_types::Type;
    use sync_protocol::{
        change::{CellValue as WireCellValue, ColumnValue as WireColumnValue, PrimaryKey},
        encode_primary_key, PRIMARY_KEY_CODEC_VERSION,
    };

    use super::*;
    use crate::{CopyColumnPlan, SnapshotColumnPlan};

    fn epoch() -> SourceEpoch {
        SourceEpoch::new("epoch-1").unwrap()
    }

    fn table_id() -> TableId {
        TableId::new("public.tasks").unwrap()
    }

    fn column(id: u32, name: &str, codec: PostgresTypeCodec, nullable: bool) -> SnapshotColumnPlan {
        let postgres_type = match codec {
            PostgresTypeCodec::Int8 => Type::INT8,
            PostgresTypeCodec::Text => Type::TEXT,
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
                cast_to_text: false,
            },
        }
    }

    /// `tasks(id int8 primary key, title text not null, note text null)`,
    /// with column ids out of column order.
    fn overlay() -> DirectOverlay {
        let plan = SnapshotTablePlan::for_tests(
            table_id(),
            "tasks",
            vec![
                column(4, "id", PostgresTypeCodec::Int8, false),
                column(2, "title", PostgresTypeCodec::Text, false),
                column(9, "note", PostgresTypeCodec::Text, true),
            ],
            vec![ColumnId::new(4)],
        );
        DirectOverlay::new(&[plan], epoch(), PgLsn::from(100)).unwrap()
    }

    fn key(id: i64) -> Vec<u8> {
        encode_primary_key(&PrimaryKey {
            codec_version: PRIMARY_KEY_CODEC_VERSION,
            parts: vec![WireColumnValue {
                column_id: 4,
                value: Some(WireCellValue {
                    kind: Some(cell_value::Kind::Int64Value(id)),
                }),
            }],
        })
        .unwrap()
    }

    fn value(column_id: u32, value: CellValue) -> ColumnValue {
        ColumnValue {
            column_id: ColumnId::new(column_id),
            value,
        }
    }

    fn text(value: &str) -> CellValue {
        CellValue::Text(value.to_owned())
    }

    fn operation(id: i64, kind: CanonicalOperationKind) -> CanonicalOperation {
        CanonicalOperation {
            table_id: table_id(),
            primary_key: key(id),
            kind,
        }
    }

    fn insert(id: i64, title: &str, note: Option<&str>) -> CanonicalOperation {
        operation(
            id,
            CanonicalOperationKind::InsertFullRow {
                columns: vec![
                    value(4, CellValue::Int64(id)),
                    value(2, text(title)),
                    value(9, note.map_or(CellValue::Null, text)),
                ],
            },
        )
    }

    fn update(id: i64, changed_columns: Vec<ColumnValue>) -> CanonicalOperation {
        operation(id, CanonicalOperationKind::UpdatePatch { changed_columns })
    }

    fn delete(id: i64) -> CanonicalOperation {
        operation(id, CanonicalOperationKind::DeleteByKey)
    }

    fn transaction(sequence: u64, operations: Vec<CanonicalOperation>) -> CanonicalTransaction {
        let end = 100 + sequence * 3;
        CanonicalTransaction {
            key: TransactionKey {
                source_epoch: epoch(),
                end_lsn: PgLsn::from(end).to_string(),
                postgres_xid: sequence as u32,
            },
            transaction_id: format!("tx-{sequence}"),
            begin_lsn: PgLsn::from(end - 2).to_string(),
            commit_lsn: PgLsn::from(end - 1).to_string(),
            commit_time_unix_micros: sequence,
            operations,
            mutation_ids: Vec::new(),
        }
    }

    fn row(id: i64, title: &str, note: Option<&str>) -> Vec<Value> {
        vec![
            Value::Int64(id),
            Value::Text(title.to_owned()),
            note.map_or(Value::Null, |note| Value::Text(note.to_owned())),
        ]
    }

    /// The snapshot COPY returns, in heap order: ids 5, 1, 2, 3, 4.
    fn snapshot() -> Vec<Vec<Value>> {
        vec![
            row(5, "five", Some("n5")),
            row(1, "one", Some("n1")),
            row(2, "two", Some("n2")),
            row(3, "three", None),
            row(4, "four", Some("n4")),
        ]
    }

    /// What the artifact receives for the table: every COPY row through the
    /// overlay, then the rows the roll-forward inserted.
    fn copy_through(overlay: &mut DirectOverlay, rows: Vec<Vec<Value>>) -> Vec<Vec<Value>> {
        let mut emitted = Vec::new();
        for row in rows {
            if let Some(row) = overlay.merge_copy_row(0, row).unwrap() {
                emitted.push(row);
            }
        }
        emitted.extend(overlay.finish_table(0).unwrap());
        emitted
    }

    fn apply(overlay: &mut DirectOverlay, transactions: Vec<Vec<CanonicalOperation>>) {
        for (index, operations) in transactions.into_iter().enumerate() {
            overlay
                .apply_transaction(transaction(index as u64 + 1, operations))
                .unwrap();
        }
    }

    #[test]
    fn an_update_a_delete_and_an_insert_in_the_copy_window_reach_the_artifact() {
        let mut overlay = overlay();
        apply(
            &mut overlay,
            vec![
                vec![update(2, vec![value(2, text("two, edited"))])],
                vec![delete(3)],
                vec![insert(9, "nine", None)],
            ],
        );
        assert_eq!(overlay.last_end_lsn(), PgLsn::from(109));
        assert_eq!(
            copy_through(&mut overlay, snapshot()),
            vec![
                row(5, "five", Some("n5")),
                row(1, "one", Some("n1")),
                row(2, "two, edited", Some("n2")),
                row(4, "four", Some("n4")),
                row(9, "nine", None),
            ]
        );
    }

    #[test]
    fn a_transaction_committed_between_export_and_cut_applies_whole() {
        // One transaction does everything the oracle's writer does: update a
        // row, insert a row, delete a row, change a primary key, and delete
        // then re-insert a key. The next one edits the moved row at its new
        // key and inserts at the key the move freed.
        let mut overlay = overlay();
        apply(
            &mut overlay,
            vec![
                vec![
                    update(1, vec![value(9, text("n1, edited"))]),
                    insert(7, "seven", Some("n7")),
                    delete(5),
                    update(2, vec![value(4, CellValue::Int64(20))]),
                    delete(4),
                    insert(4, "four again", None),
                ],
                vec![
                    update(20, vec![value(2, text("twenty"))]),
                    insert(2, "two again", Some("fresh")),
                ],
            ],
        );
        assert_eq!(
            copy_through(&mut overlay, snapshot()),
            vec![
                row(1, "one", Some("n1, edited")),
                row(20, "twenty", Some("n2")),
                row(3, "three", None),
                row(2, "two again", Some("fresh")),
                row(4, "four again", None),
                row(7, "seven", Some("n7")),
            ]
        );
    }

    #[test]
    fn a_patch_that_omits_an_unchanged_toast_column_keeps_the_copied_value() {
        // pgoutput leaves an unchanged TOASTed value out of the new tuple, so
        // the patch names only the title. The note comes from COPY.
        let mut overlay = overlay();
        apply(
            &mut overlay,
            vec![vec![update(1, vec![value(2, text("one, edited"))])]],
        );
        let merged = overlay
            .merge_copy_row(0, row(1, "one", Some("large toasted note")))
            .unwrap();
        assert_eq!(
            merged,
            Some(row(1, "one, edited", Some("large toasted note")))
        );
    }

    #[test]
    fn keys_can_trade_places_and_chain_moves() {
        let mut overlay = overlay();
        apply(
            &mut overlay,
            vec![
                vec![update(1, vec![value(4, CellValue::Int64(10))])],
                vec![update(2, vec![value(4, CellValue::Int64(1))])],
                vec![update(10, vec![value(4, CellValue::Int64(2))])],
                vec![update(3, vec![value(4, CellValue::Int64(30))])],
                vec![update(
                    30,
                    vec![value(4, CellValue::Int64(31)), value(2, text("t"))],
                )],
            ],
        );
        assert_eq!(
            copy_through(&mut overlay, snapshot()),
            vec![
                row(5, "five", Some("n5")),
                row(2, "one", Some("n1")),
                row(1, "two", Some("n2")),
                row(31, "t", None),
                row(4, "four", Some("n4")),
            ]
        );
    }

    #[test]
    fn a_row_inserted_then_changed_then_deleted_leaves_nothing() {
        let mut overlay = overlay();
        apply(
            &mut overlay,
            vec![
                vec![insert(8, "eight", None)],
                vec![update(8, vec![value(4, CellValue::Int64(80))])],
                vec![delete(80)],
            ],
        );
        assert_eq!(copy_through(&mut overlay, snapshot()), snapshot());
    }

    #[test]
    fn an_insert_over_a_row_the_snapshot_holds_is_refused_when_copy_returns_it() {
        let mut overlay = overlay();
        apply(&mut overlay, vec![vec![insert(3, "impossible", None)]]);
        assert!(matches!(
            overlay.merge_copy_row(0, row(3, "three", None)),
            Err(DirectOverlayError::CopiedRowWasInserted(_))
        ));
    }

    #[test]
    fn a_changed_row_copy_never_returns_is_refused_at_the_end_of_the_table() {
        for operations in [
            vec![update(42, vec![value(2, text("ghost"))])],
            vec![delete(42)],
            vec![update(42, vec![value(4, CellValue::Int64(43))])],
        ] {
            let mut overlay = overlay();
            apply(&mut overlay, vec![operations]);
            for row in snapshot() {
                overlay.merge_copy_row(0, row).unwrap();
            }
            assert!(matches!(
                overlay.finish_table(0),
                Err(DirectOverlayError::MissingCopiedRow(_))
            ));
        }
    }

    #[test]
    fn a_key_copy_returns_twice_is_refused() {
        let mut overlay = overlay();
        apply(&mut overlay, vec![vec![delete(1)]]);
        overlay.merge_copy_row(0, row(1, "one", None)).unwrap();
        assert!(matches!(
            overlay.merge_copy_row(0, row(1, "one", None)),
            Err(DirectOverlayError::DuplicateCopiedRow(_))
        ));
    }

    #[test]
    fn changes_to_rows_that_are_gone_or_taken_are_refused() {
        let mut overlay = self::overlay();
        apply(&mut overlay, vec![vec![delete(1)]]);
        assert!(matches!(
            overlay.apply_transaction(transaction(2, vec![update(1, vec![value(2, text("x"))])])),
            Err(DirectOverlayError::MissingRow(_))
        ));

        let mut overlay = self::overlay();
        apply(&mut overlay, vec![vec![insert(9, "nine", None)]]);
        assert!(matches!(
            overlay.apply_transaction(transaction(2, vec![insert(9, "again", None)])),
            Err(DirectOverlayError::InsertOverRow(_))
        ));

        let mut overlay = self::overlay();
        apply(
            &mut overlay,
            vec![vec![insert(9, "nine", None), insert(8, "eight", None)]],
        );
        assert!(matches!(
            overlay.apply_transaction(transaction(
                2,
                vec![update(9, vec![value(4, CellValue::Int64(8))])]
            )),
            Err(DirectOverlayError::KeyCollision(_))
        ));
    }

    #[test]
    fn a_truncate_ends_the_build() {
        let mut overlay = overlay();
        let truncate = CanonicalOperation {
            table_id: table_id(),
            primary_key: Vec::new(),
            kind: CanonicalOperationKind::TruncateAuthorizedTable {
                reason: "truncate".to_owned(),
            },
        };
        assert!(matches!(
            overlay.apply_transaction(transaction(1, vec![truncate])),
            Err(DirectOverlayError::Truncate(_))
        ));
    }

    #[test]
    fn transactions_must_follow_the_export_in_commit_order_and_epoch() {
        let mut overlay = overlay();
        let mut early = transaction(1, vec![delete(1)]);
        early.begin_lsn = PgLsn::from(98).to_string();
        early.commit_lsn = PgLsn::from(99).to_string();
        early.key.end_lsn = PgLsn::from(100).to_string();
        assert!(matches!(
            overlay.apply_transaction(early),
            Err(DirectOverlayError::NonMonotonicEndLsn { .. })
        ));

        let mut foreign = transaction(1, vec![delete(1)]);
        foreign.key.source_epoch = SourceEpoch::new("epoch-2").unwrap();
        assert!(matches!(
            overlay.apply_transaction(foreign),
            Err(DirectOverlayError::EpochMismatch { .. })
        ));

        apply(&mut overlay, vec![vec![delete(1)], vec![delete(2)]]);
        assert!(matches!(
            overlay.apply_transaction(transaction(2, vec![delete(3)])),
            Err(DirectOverlayError::NonMonotonicEndLsn { .. })
        ));
    }

    #[test]
    fn a_value_that_does_not_carry_its_column_type_is_refused() {
        let mut overlay = overlay();
        assert!(matches!(
            overlay.apply_transaction(transaction(
                1,
                vec![update(1, vec![value(2, CellValue::Int64(7))])]
            )),
            Err(DirectOverlayError::ValueShape { .. })
        ));
    }

    #[test]
    fn an_untouched_table_passes_copy_rows_through() {
        let mut overlay = overlay();
        assert_eq!(copy_through(&mut overlay, snapshot()), snapshot());
        assert!(matches!(
            overlay.merge_copy_row(0, row(1, "one", None)),
            Err(DirectOverlayError::TableFinished(_))
        ));
    }
}
