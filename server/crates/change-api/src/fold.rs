//! Folding the transactions of a fenced batch into one that changes each row
//! once, as the batch's end leaves it.
//!
//! A row's first operation in the fold says whether it stood before the fold
//! began: an insert says it did not, a patch or a delete that it did. What the
//! fold sends for it follows from that and from how the fold leaves it:
//!
//! | before | after                 | sent                      |
//! |--------|-----------------------|---------------------------|
//! | absent | present               | INSERT_FULL_ROW           |
//! | absent | absent                | nothing                   |
//! | stood  | patched               | UPDATE_PATCH              |
//! | stood  | deleted               | DELETE_BY_KEY             |
//! | stood  | deleted, then present | DELETE_BY_KEY, INSERT     |
//!
//! A sequence a receiver would refuse, such as an insert of a row the fold
//! holds, is not folded: the batch is built as committed instead, and the
//! receiver refuses it as it would have.

use std::collections::HashMap;

use prost::Message;
use sync_protocol::{
    change::{operation::Payload, ColumnValue as WireColumnValue, Operation},
    decode_primary_key,
};

#[derive(Default)]
pub(crate) struct Fold {
    rows: Vec<FoldedRow>,
    /// Each row's index in `rows`, by table and primary key.
    index: HashMap<String, HashMap<Vec<u8>, usize>>,
    operation_bytes: usize,
    operation_count: u64,
    largest_operation: usize,
}

#[derive(Default)]
struct FoldedRow {
    /// Whether the fold has met the row yet.
    met: bool,
    /// Set once a row that stood before the fold is deleted, and sent first.
    delete: Option<Operation>,
    /// The row as the fold leaves it, an insert or a patch, or `None` while
    /// it is deleted.
    current: Option<Operation>,
}

/// An operation sequence a receiver would refuse.
#[derive(Debug, PartialEq, Eq)]
pub(crate) struct Unfoldable;

impl Fold {
    /// What the operations the fold sends take inside their fragments.
    pub(crate) fn operation_bytes(&self) -> usize {
        self.operation_bytes
    }

    pub(crate) fn operation_count(&self) -> u64 {
        self.operation_count
    }

    /// The largest operation the fold has sent at any point.
    pub(crate) fn largest_operation(&self) -> usize {
        self.largest_operation
    }

    /// Folds one transaction's operations in, in order. After `Unfoldable`
    /// the fold is part-applied, and the batch is built as committed instead.
    pub(crate) fn apply(&mut self, operations: Vec<Operation>) -> Result<(), Unfoldable> {
        operations
            .into_iter()
            .try_for_each(|operation| self.apply_one(operation))
    }

    fn apply_one(&mut self, operation: Operation) -> Result<(), Unfoldable> {
        let slot = self
            .index
            .get(&operation.table_id)
            .and_then(|rows| rows.get(&operation.primary_key))
            .copied();
        let slot = match slot {
            Some(slot) => slot,
            None => {
                self.index
                    .entry(operation.table_id.clone())
                    .or_default()
                    .insert(operation.primary_key.clone(), self.rows.len());
                self.rows.push(FoldedRow::default());
                self.rows.len() - 1
            }
        };
        let mut row = std::mem::take(&mut self.rows[slot]);
        self.count(&row, false);
        let folded = row.fold(operation);
        self.count(&row, true);
        self.rows[slot] = row;
        folded
    }

    fn count(&mut self, row: &FoldedRow, add: bool) {
        for operation in row.sent() {
            let bytes = embedded_operation_len(operation);
            if add {
                self.operation_bytes += bytes;
                self.operation_count += 1;
                self.largest_operation = self.largest_operation.max(bytes);
            } else {
                self.operation_bytes -= bytes;
                self.operation_count -= 1;
            }
        }
    }

    /// The operations the fold sends, rows in the order the fold first met
    /// them.
    pub(crate) fn into_operations(self) -> Vec<Operation> {
        let mut operations = Vec::with_capacity(self.rows.len());
        for row in self.rows {
            operations.extend(row.delete);
            operations.extend(row.current);
        }
        operations
    }
}

impl FoldedRow {
    fn sent(&self) -> impl Iterator<Item = &Operation> {
        self.delete.iter().chain(self.current.iter())
    }

    /// A row's first operation decides whether it stood before the fold.
    fn fold(&mut self, operation: Operation) -> Result<(), Unfoldable> {
        let first = !std::mem::replace(&mut self.met, true);
        let Operation {
            table_id,
            kind,
            primary_key,
            payload,
        } = operation;
        let rebuild = move |payload: Payload| Operation {
            table_id,
            kind,
            primary_key,
            payload: Some(payload),
        };
        match payload {
            Some(Payload::Update(patch)) => {
                if first {
                    self.current = Some(rebuild(Payload::Update(patch)));
                    return Ok(());
                }
                let columns = match self
                    .current
                    .as_mut()
                    .and_then(|current| current.payload.as_mut())
                {
                    Some(Payload::Insert(row)) => &mut row.columns,
                    Some(Payload::Update(earlier)) => &mut earlier.changed_columns,
                    _ => return Err(Unfoldable),
                };
                merge(columns, patch.changed_columns);
            }
            Some(Payload::Delete(delete)) => match self.current.take() {
                // A row the fold inserted, or inserted again after deleting
                // it, leaves as it was before the insert.
                Some(Operation {
                    payload: Some(Payload::Insert(_)),
                    ..
                }) => {}
                // A row that stood before the fold is deleted once.
                Some(Operation {
                    payload: Some(Payload::Update(_)),
                    ..
                }) => self.delete = Some(rebuild(Payload::Delete(delete))),
                None if first => self.delete = Some(rebuild(Payload::Delete(delete))),
                _ => return Err(Unfoldable),
            },
            Some(Payload::Insert(row)) if self.current.is_none() => {
                self.current = Some(rebuild(Payload::Insert(row)));
            }
            _ => return Err(Unfoldable),
        }
        Ok(())
    }
}

/// Sets each patched column's value, adding the columns `columns` lacks.
fn merge(columns: &mut Vec<WireColumnValue>, patch: Vec<WireColumnValue>) {
    for column in patch {
        match columns
            .iter_mut()
            .find(|existing| existing.column_id == column.column_id)
        {
            Some(existing) => *existing = column,
            None => columns.push(column),
        }
    }
}

/// Whether a batch may fold `operations`: a truncate, or an update that moves
/// a row to another primary key, ends the fold before its transaction.
pub(crate) fn foldable(operations: &[Operation]) -> bool {
    operations.iter().all(|operation| match &operation.payload {
        Some(Payload::Insert(_) | Payload::Delete(_)) => true,
        Some(Payload::Update(patch)) => !moves_primary_key(operation, &patch.changed_columns),
        Some(Payload::Truncate(_)) | None => false,
    })
}

fn moves_primary_key(operation: &Operation, changed: &[WireColumnValue]) -> bool {
    let Ok(key) = decode_primary_key(&operation.primary_key) else {
        return true;
    };
    key.parts.iter().any(|part| {
        changed
            .iter()
            .any(|column| column.column_id == part.column_id && column.value != part.value)
    })
}

/// What one operation takes inside a transaction fragment.
pub(crate) fn embedded_operation_len(operation: &Operation) -> usize {
    crate::embedded_message_len(operation.encoded_len())
}

#[cfg(test)]
pub(crate) mod tests {
    use sync_protocol::{
        change::{cell_value, CellValue, Delete, FullRow, OperationKind, PrimaryKey, RowPatch},
        encode_primary_key, PRIMARY_KEY_CODEC_VERSION,
    };

    use super::*;
    use crate::tests::receiver::{receive, Tables};

    const TABLE: &str = "public.tasks";

    fn value(kind: cell_value::Kind) -> Option<CellValue> {
        Some(CellValue { kind: Some(kind) })
    }

    fn column(column_id: u32, number: i64) -> WireColumnValue {
        WireColumnValue {
            column_id,
            value: value(cell_value::Kind::Int64Value(number)),
        }
    }

    fn key(id: i64) -> Vec<u8> {
        encode_primary_key(&PrimaryKey {
            codec_version: PRIMARY_KEY_CODEC_VERSION,
            parts: vec![column(1, id)],
        })
        .unwrap()
    }

    fn insert(id: i64, title: i64, done: i64) -> Operation {
        Operation {
            table_id: TABLE.to_owned(),
            kind: OperationKind::InsertFullRow as i32,
            primary_key: key(id),
            payload: Some(Payload::Insert(FullRow {
                columns: vec![column(1, id), column(2, title), column(3, done)],
            })),
        }
    }

    fn patch(id: i64, columns: Vec<WireColumnValue>) -> Operation {
        Operation {
            table_id: TABLE.to_owned(),
            kind: OperationKind::UpdatePatch as i32,
            primary_key: key(id),
            payload: Some(Payload::Update(RowPatch {
                changed_columns: columns,
            })),
        }
    }

    fn delete(id: i64) -> Operation {
        Operation {
            table_id: TABLE.to_owned(),
            kind: OperationKind::DeleteByKey as i32,
            primary_key: key(id),
            payload: Some(Payload::Delete(Delete {})),
        }
    }

    fn folded(operations: Vec<Operation>) -> Result<Vec<Operation>, Unfoldable> {
        let mut fold = Fold::default();
        fold.apply(operations)?;
        let (bytes, count) = (fold.operation_bytes(), fold.operation_count());
        let operations = fold.into_operations();
        assert_eq!(count, operations.len() as u64);
        assert_eq!(
            bytes,
            operations.iter().map(embedded_operation_len).sum::<usize>()
        );
        Ok(operations)
    }

    #[test]
    fn each_row_is_sent_once_as_the_fold_leaves_it() {
        // Inserted, then patched twice: one insert of its last values.
        assert_eq!(
            folded(vec![
                insert(1, 10, 0),
                patch(1, vec![column(2, 11)]),
                patch(1, vec![column(3, 1), column(2, 12)]),
            ]),
            Ok(vec![insert(1, 12, 1)])
        );
        // Patched where it stood: one patch of every column changed.
        assert_eq!(
            folded(vec![
                patch(1, vec![column(2, 11)]),
                patch(1, vec![column(3, 1)]),
                patch(1, vec![column(2, 12)]),
            ]),
            Ok(vec![patch(1, vec![column(2, 12), column(3, 1)])])
        );
        // Deleted where it stood, after a patch: one delete.
        assert_eq!(
            folded(vec![patch(1, vec![column(2, 11)]), delete(1)]),
            Ok(vec![delete(1)])
        );
        // Inserted and deleted: nothing.
        assert_eq!(folded(vec![insert(1, 10, 0), delete(1)]), Ok(vec![]));
        // Deleted where it stood and inserted again: a delete, then the insert
        // with its last values.
        assert_eq!(
            folded(vec![
                delete(1),
                insert(1, 20, 0),
                patch(1, vec![column(3, 1)]),
                delete(1),
                insert(1, 30, 1),
            ]),
            Ok(vec![delete(1), insert(1, 30, 1)])
        );
        // Inserted, deleted and inserted again: the last insert.
        assert_eq!(
            folded(vec![insert(1, 10, 0), delete(1), insert(1, 20, 1)]),
            Ok(vec![insert(1, 20, 1)])
        );
        // Rows keep the order the fold first met them in.
        assert_eq!(
            folded(vec![
                patch(2, vec![column(2, 1)]),
                insert(1, 10, 0),
                patch(2, vec![column(2, 2)]),
            ]),
            Ok(vec![patch(2, vec![column(2, 2)]), insert(1, 10, 0)])
        );
    }

    #[test]
    fn a_sequence_a_receiver_refuses_is_not_folded() {
        for operations in [
            vec![insert(1, 10, 0), insert(1, 11, 0)],
            vec![patch(1, vec![column(2, 1)]), insert(1, 11, 0)],
            vec![delete(1), delete(1)],
            vec![delete(1), patch(1, vec![column(2, 1)])],
            vec![insert(1, 10, 0), delete(1), delete(1)],
            vec![insert(1, 10, 0), delete(1), patch(1, vec![column(2, 1)])],
        ] {
            assert_eq!(folded(operations), Err(Unfoldable));
        }
    }

    #[test]
    fn a_truncate_or_a_moved_key_ends_the_fold() {
        assert!(foldable(&[
            insert(1, 10, 0),
            // The key's own value, as a full-row patch carries it.
            patch(1, vec![column(1, 1), column(2, 11)]),
            delete(1),
        ]));
        assert!(!foldable(&[patch(1, vec![column(1, 2)])]));
        assert!(!foldable(&[Operation {
            table_id: TABLE.to_owned(),
            kind: OperationKind::TruncateAuthorizedTable as i32,
            primary_key: Vec::new(),
            payload: Some(Payload::Truncate(sync_protocol::change::Truncate {
                reason: "truncate".to_owned(),
            })),
        }]));
    }

    /// A small deterministic generator, so a failing seed replays.
    pub(crate) struct Draw(pub(crate) u64);

    impl Draw {
        pub(crate) fn below(&mut self, bound: u64) -> u64 {
            self.0 ^= self.0 << 13;
            self.0 ^= self.0 >> 7;
            self.0 ^= self.0 << 17;
            self.0 % bound
        }
    }

    /// Valid operations drawn against a table the receiver holds: inserts of
    /// absent rows, patches of some columns of present ones, and deletes.
    fn workload(draw: &mut Draw, start: &Tables, length: usize) -> Vec<Operation> {
        let mut rows = start.clone();
        let mut operations = Vec::new();
        while operations.len() < length {
            let id = draw.below(6) as i64;
            let operation = if rows
                .get(TABLE)
                .is_some_and(|table| table.contains_key(&key(id)))
            {
                match draw.below(4) {
                    0 => delete(id),
                    _ => {
                        let mut columns = Vec::new();
                        for column_id in [3, 1, 2] {
                            if draw.below(2) == 0 {
                                let number = if column_id == 1 {
                                    id
                                } else {
                                    draw.below(100) as i64
                                };
                                columns.push(column(column_id, number));
                            }
                        }
                        if columns.is_empty() {
                            columns.push(column(2, draw.below(100) as i64));
                        }
                        patch(id, columns)
                    }
                }
            } else {
                insert(id, draw.below(100) as i64, draw.below(2) as i64)
            };
            receive(&mut rows, &operation).unwrap();
            operations.push(operation);
        }
        operations
    }

    #[test]
    fn a_receiver_ends_where_the_folded_operations_leave_it() {
        for seed in 1..=2_000_u64 {
            let mut draw = Draw(seed.wrapping_mul(0x9E37_79B9_7F4A_7C15) | 1);
            let mut start = Tables::new();
            for id in 0..3 {
                if draw.below(2) == 0 {
                    receive(&mut start, &insert(id, 1, 0)).unwrap();
                }
            }
            let length = 1 + draw.below(24) as usize;
            let operations = workload(&mut draw, &start, length);
            let mut committed = start.clone();
            for operation in &operations {
                receive(&mut committed, operation).unwrap();
            }
            let mut received = start.clone();
            for operation in folded(operations.clone()).unwrap() {
                receive(&mut received, &operation)
                    .unwrap_or_else(|()| panic!("seed {seed} refused {operation:?}"));
            }
            assert_eq!(received, committed, "seed {seed}: {operations:?}");
        }
    }
}
