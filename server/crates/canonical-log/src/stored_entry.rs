//! The bytes the PostgreSQL log stores for one entry: a protobuf message whose
//! operations are the change stream's `Operation`, the message a change batch
//! sends them in.
//!
//! An earlier release stored each entry as JSON. Its first byte is `{`, which
//! no entry this module writes begins with: that byte would open field 15 as a
//! group, and the message has neither.

use prost::Message;
use sync_protocol::change::{self, cell_value::Kind, operation::Payload, OperationKind};
use sync_types::{ColumnId, IdentifierError, MutationId, SourceEpoch, TableId};
use thiserror::Error;

use crate::{
    CanonicalBarrier, CanonicalEntry, CanonicalOperation, CanonicalOperationKind,
    CanonicalTransaction, CellValue, ColumnValue, TransactionKey,
};

#[derive(Debug, Error)]
pub enum EntryDecodeError {
    #[error("the entry is JSON, which an earlier release stored")]
    EarlierFormat,
    #[error(transparent)]
    Protobuf(#[from] prost::DecodeError),
    #[error(transparent)]
    Identifier(#[from] IdentifierError),
    #[error("{0}")]
    Invalid(&'static str),
}

#[derive(Clone, PartialEq, Message)]
struct StoredEntry {
    #[prost(oneof = "StoredRecord", tags = "1, 2")]
    record: Option<StoredRecord>,
}

#[derive(Clone, PartialEq, prost::Oneof)]
enum StoredRecord {
    #[prost(message, tag = "1")]
    Transaction(StoredTransaction),
    #[prost(message, tag = "2")]
    Barrier(StoredBarrier),
}

#[derive(Clone, PartialEq, Message)]
struct StoredKey {
    #[prost(string, tag = "1")]
    source_epoch: String,
    #[prost(string, tag = "2")]
    end_lsn: String,
    #[prost(uint32, tag = "3")]
    postgres_xid: u32,
}

#[derive(Clone, PartialEq, Message)]
struct StoredTransaction {
    #[prost(message, optional, tag = "1")]
    key: Option<StoredKey>,
    #[prost(string, tag = "2")]
    transaction_id: String,
    #[prost(string, tag = "3")]
    begin_lsn: String,
    #[prost(string, tag = "4")]
    commit_lsn: String,
    #[prost(uint64, tag = "5")]
    commit_time_unix_micros: u64,
    #[prost(message, repeated, tag = "6")]
    operations: Vec<change::Operation>,
    #[prost(string, repeated, tag = "7")]
    mutation_ids: Vec<String>,
}

#[derive(Clone, PartialEq, Message)]
struct StoredBarrier {
    #[prost(message, optional, tag = "1")]
    key: Option<StoredKey>,
    #[prost(string, tag = "2")]
    build_id: String,
    #[prost(string, tag = "3")]
    nonce: String,
    #[prost(string, tag = "4")]
    message_lsn: String,
    #[prost(string, tag = "5")]
    commit_lsn: String,
    #[prost(uint64, tag = "6")]
    commit_time_unix_micros: u64,
}

impl CanonicalEntry {
    /// The bytes the PostgreSQL log stores for this entry. The same entry
    /// always encodes to the same bytes, which a duplicate identity is
    /// matched on.
    pub fn encode(&self) -> Vec<u8> {
        let record = match self {
            Self::Transaction(transaction) => StoredRecord::Transaction(StoredTransaction {
                key: Some(stored_key(&transaction.key)),
                transaction_id: transaction.transaction_id.clone(),
                begin_lsn: transaction.begin_lsn.clone(),
                commit_lsn: transaction.commit_lsn.clone(),
                commit_time_unix_micros: transaction.commit_time_unix_micros,
                operations: transaction
                    .operations
                    .iter()
                    .map(stored_operation)
                    .collect(),
                mutation_ids: transaction
                    .mutation_ids
                    .iter()
                    .map(|mutation_id| mutation_id.as_str().to_owned())
                    .collect(),
            }),
            Self::Barrier(barrier) => StoredRecord::Barrier(StoredBarrier {
                key: Some(stored_key(&barrier.key)),
                build_id: barrier.build_id.clone(),
                nonce: barrier.nonce.clone(),
                message_lsn: barrier.message_lsn.clone(),
                commit_lsn: barrier.commit_lsn.clone(),
                commit_time_unix_micros: barrier.commit_time_unix_micros,
            }),
        };
        StoredEntry {
            record: Some(record),
        }
        .encode_to_vec()
    }

    /// Decodes the bytes [`Self::encode`] stores.
    pub fn decode(bytes: &[u8]) -> Result<Self, EntryDecodeError> {
        if bytes.first() == Some(&b'{') {
            return Err(EntryDecodeError::EarlierFormat);
        }
        match StoredEntry::decode(bytes)?.record {
            Some(StoredRecord::Transaction(transaction)) => {
                Ok(Self::Transaction(CanonicalTransaction {
                    key: transaction_key(transaction.key)?,
                    transaction_id: transaction.transaction_id,
                    begin_lsn: transaction.begin_lsn,
                    commit_lsn: transaction.commit_lsn,
                    commit_time_unix_micros: transaction.commit_time_unix_micros,
                    operations: transaction
                        .operations
                        .into_iter()
                        .map(canonical_operation)
                        .collect::<Result<_, _>>()?,
                    mutation_ids: transaction
                        .mutation_ids
                        .into_iter()
                        .map(MutationId::new)
                        .collect::<Result<_, _>>()?,
                }))
            }
            Some(StoredRecord::Barrier(barrier)) => Ok(Self::Barrier(CanonicalBarrier {
                key: transaction_key(barrier.key)?,
                build_id: barrier.build_id,
                nonce: barrier.nonce,
                message_lsn: barrier.message_lsn,
                commit_lsn: barrier.commit_lsn,
                commit_time_unix_micros: barrier.commit_time_unix_micros,
            })),
            None => Err(EntryDecodeError::Invalid("entry holds no record")),
        }
    }
}

impl CanonicalOperation {
    /// The bytes this operation adds to the stored entry that holds it.
    pub fn stored_len(&self) -> u64 {
        let len = stored_operation(self).encoded_len();
        (1 + prost::length_delimiter_len(len) + len) as u64
    }
}

fn stored_key(key: &TransactionKey) -> StoredKey {
    StoredKey {
        source_epoch: key.source_epoch.as_str().to_owned(),
        end_lsn: key.end_lsn.clone(),
        postgres_xid: key.postgres_xid,
    }
}

fn transaction_key(key: Option<StoredKey>) -> Result<TransactionKey, EntryDecodeError> {
    let key = key.ok_or(EntryDecodeError::Invalid("entry has no key"))?;
    Ok(TransactionKey {
        source_epoch: SourceEpoch::new(key.source_epoch)?,
        end_lsn: key.end_lsn,
        postgres_xid: key.postgres_xid,
    })
}

fn stored_operation(operation: &CanonicalOperation) -> change::Operation {
    let (kind, payload) = match &operation.kind {
        CanonicalOperationKind::InsertFullRow { columns } => (
            OperationKind::InsertFullRow,
            Payload::Insert(change::FullRow {
                columns: stored_columns(columns),
            }),
        ),
        CanonicalOperationKind::UpdatePatch { changed_columns } => (
            OperationKind::UpdatePatch,
            Payload::Update(change::RowPatch {
                changed_columns: stored_columns(changed_columns),
            }),
        ),
        CanonicalOperationKind::DeleteByKey => (
            OperationKind::DeleteByKey,
            Payload::Delete(change::Delete {}),
        ),
        CanonicalOperationKind::TruncateAuthorizedTable { reason } => (
            OperationKind::TruncateAuthorizedTable,
            Payload::Truncate(change::Truncate {
                reason: reason.clone(),
            }),
        ),
    };
    change::Operation {
        table_id: operation.table_id.as_str().to_owned(),
        kind: kind as i32,
        primary_key: operation.primary_key.clone(),
        payload: Some(payload),
    }
}

fn stored_columns(columns: &[ColumnValue]) -> Vec<change::ColumnValue> {
    columns
        .iter()
        .map(|column| change::ColumnValue {
            column_id: column.column_id.get(),
            value: Some(change::CellValue {
                kind: Some(match &column.value {
                    CellValue::Null => Kind::NullValue(change::NullValue::NullValue as i32),
                    CellValue::Int64(value) => Kind::Int64Value(*value),
                    CellValue::Uint64(value) => Kind::Uint64Value(*value),
                    CellValue::Float64(value) => Kind::Float64Value(*value),
                    CellValue::Text(value) => Kind::TextValue(value.clone()),
                    CellValue::Blob(value) => Kind::BlobValue(value.clone()),
                    CellValue::Bool(value) => Kind::BoolValue(*value),
                    CellValue::DecimalText(value) => Kind::DecimalText(value.clone()),
                    CellValue::TimestampUnixMicros(value) => Kind::TimestampUnixMicros(*value),
                    CellValue::Uuid16(value) => Kind::Uuid16(value.to_vec()),
                    CellValue::JsonText(value) => Kind::JsonText(value.clone()),
                    CellValue::DateIso8601(value) => Kind::DateIso8601(value.clone()),
                    CellValue::TimeIso8601(value) => Kind::TimeIso8601(value.clone()),
                    CellValue::IntervalIso8601(value) => Kind::IntervalIso8601(value.clone()),
                }),
            }),
        })
        .collect()
}

fn canonical_operation(
    operation: change::Operation,
) -> Result<CanonicalOperation, EntryDecodeError> {
    let kind = match (OperationKind::try_from(operation.kind), operation.payload) {
        (Ok(OperationKind::InsertFullRow), Some(Payload::Insert(row))) => {
            CanonicalOperationKind::InsertFullRow {
                columns: canonical_columns(row.columns)?,
            }
        }
        (Ok(OperationKind::UpdatePatch), Some(Payload::Update(patch))) => {
            CanonicalOperationKind::UpdatePatch {
                changed_columns: canonical_columns(patch.changed_columns)?,
            }
        }
        (Ok(OperationKind::DeleteByKey), Some(Payload::Delete(_))) => {
            CanonicalOperationKind::DeleteByKey
        }
        (Ok(OperationKind::TruncateAuthorizedTable), Some(Payload::Truncate(truncate))) => {
            CanonicalOperationKind::TruncateAuthorizedTable {
                reason: truncate.reason,
            }
        }
        _ => {
            return Err(EntryDecodeError::Invalid(
                "operation's kind and payload disagree",
            ))
        }
    };
    Ok(CanonicalOperation {
        table_id: TableId::new(operation.table_id)?,
        primary_key: operation.primary_key,
        kind,
    })
}

fn canonical_columns(
    columns: Vec<change::ColumnValue>,
) -> Result<Vec<ColumnValue>, EntryDecodeError> {
    columns
        .into_iter()
        .map(|column| {
            let kind = column
                .value
                .and_then(|value| value.kind)
                .ok_or(EntryDecodeError::Invalid("column has no value"))?;
            Ok(ColumnValue {
                column_id: ColumnId::new(column.column_id),
                value: canonical_cell(kind)?,
            })
        })
        .collect()
}

fn canonical_cell(kind: Kind) -> Result<CellValue, EntryDecodeError> {
    Ok(match kind {
        Kind::NullValue(null) if null == change::NullValue::NullValue as i32 => CellValue::Null,
        Kind::NullValue(_) => return Err(EntryDecodeError::Invalid("null value is unknown")),
        Kind::Int64Value(value) => CellValue::Int64(value),
        Kind::Uint64Value(value) => CellValue::Uint64(value),
        Kind::Float64Value(value) => CellValue::Float64(value),
        Kind::TextValue(value) => CellValue::Text(value),
        Kind::BlobValue(value) => CellValue::Blob(value),
        Kind::BoolValue(value) => CellValue::Bool(value),
        Kind::DecimalText(value) => CellValue::DecimalText(value),
        Kind::TimestampUnixMicros(value) => CellValue::TimestampUnixMicros(value),
        Kind::Uuid16(value) => CellValue::Uuid16(
            value
                .try_into()
                .map_err(|_| EntryDecodeError::Invalid("uuid is not 16 bytes"))?,
        ),
        Kind::JsonText(value) => CellValue::JsonText(value),
        Kind::DateIso8601(value) => CellValue::DateIso8601(value),
        Kind::TimeIso8601(value) => CellValue::TimeIso8601(value),
        Kind::IntervalIso8601(value) => CellValue::IntervalIso8601(value),
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn key() -> TransactionKey {
        TransactionKey {
            source_epoch: SourceEpoch::new("epoch-1").unwrap(),
            end_lsn: "0/2".to_owned(),
            postgres_xid: 7,
        }
    }

    fn columns(values: impl IntoIterator<Item = (u32, CellValue)>) -> Vec<ColumnValue> {
        values
            .into_iter()
            .map(|(column_id, value)| ColumnValue {
                column_id: ColumnId::new(column_id),
                value,
            })
            .collect()
    }

    fn transaction(operations: Vec<CanonicalOperationKind>) -> CanonicalTransaction {
        CanonicalTransaction {
            key: key(),
            transaction_id: "tx-1".to_owned(),
            begin_lsn: "0/1".to_owned(),
            commit_lsn: "0/2".to_owned(),
            commit_time_unix_micros: 1_700_000_000_000_000,
            operations: operations
                .into_iter()
                .map(|kind| CanonicalOperation {
                    table_id: TableId::new("table:tasks:g1").unwrap(),
                    primary_key: vec![8, 1, 18, 4],
                    kind,
                })
                .collect(),
            mutation_ids: Vec::new(),
        }
    }

    fn every_cell() -> Vec<ColumnValue> {
        columns((1..).zip([
            CellValue::Null,
            CellValue::Int64(i64::MIN),
            CellValue::Uint64(u64::MAX),
            CellValue::Float64(0.5),
            CellValue::Text("text".to_owned()),
            CellValue::Blob(vec![0, 255]),
            CellValue::Bool(false),
            CellValue::DecimalText("1.50".to_owned()),
            CellValue::TimestampUnixMicros(-1),
            CellValue::Uuid16([9; 16]),
            CellValue::JsonText("{\"a\":1}".to_owned()),
            CellValue::DateIso8601("2026-10-16".to_owned()),
            CellValue::TimeIso8601("12:00:00".to_owned()),
            CellValue::IntervalIso8601("P1D".to_owned()),
        ]))
    }

    /// An entry encodes to the same bytes each time and decodes to itself:
    /// every operation kind and every cell kind, floats bit for bit, an
    /// explicit null beside a column a patch leaves out, mutation ids, a
    /// transaction without operations, and a barrier.
    #[test]
    fn an_entry_encodes_and_decodes_to_itself() {
        let floats = [0.1, -2.5e-300, 6.02e23, -0.0, f64::MIN_POSITIVE, f64::MAX];
        let mut with_mutations = transaction(Vec::new());
        with_mutations.mutation_ids = vec![
            MutationId::new("mutation-1").unwrap(),
            MutationId::new("mutation-2").unwrap(),
        ];
        let entries = [
            CanonicalEntry::Transaction(transaction(vec![
                CanonicalOperationKind::InsertFullRow {
                    columns: every_cell(),
                },
                CanonicalOperationKind::UpdatePatch {
                    changed_columns: every_cell(),
                },
                CanonicalOperationKind::DeleteByKey,
                CanonicalOperationKind::TruncateAuthorizedTable {
                    reason: "table truncated".to_owned(),
                },
            ])),
            CanonicalEntry::Transaction(transaction(vec![CanonicalOperationKind::UpdatePatch {
                changed_columns: columns((1..).zip(floats.map(CellValue::Float64))),
            }])),
            CanonicalEntry::Transaction(transaction(vec![CanonicalOperationKind::UpdatePatch {
                changed_columns: columns([
                    (1, CellValue::Null),
                    (3, CellValue::Text("kept".to_owned())),
                ]),
            }])),
            CanonicalEntry::Transaction(transaction(Vec::new())),
            CanonicalEntry::Transaction(with_mutations),
            CanonicalEntry::Barrier(CanonicalBarrier {
                key: key(),
                build_id: "build-1".to_owned(),
                nonce: "nonce-1".to_owned(),
                message_lsn: "0/1".to_owned(),
                commit_lsn: "0/2".to_owned(),
                commit_time_unix_micros: 1,
            }),
        ];
        for entry in entries {
            let bytes = entry.encode();
            assert_eq!(bytes, entry.encode());
            assert_ne!(bytes.first(), Some(&b'{'));
            let decoded = CanonicalEntry::decode(&bytes).unwrap();
            assert_eq!(decoded, entry);
            // `f64`'s equality takes -0.0 for 0.0; the bits must survive too.
            let cells = |entry: &CanonicalEntry| match entry {
                CanonicalEntry::Transaction(transaction) => transaction
                    .operations
                    .iter()
                    .flat_map(|operation| match &operation.kind {
                        CanonicalOperationKind::InsertFullRow { columns }
                        | CanonicalOperationKind::UpdatePatch {
                            changed_columns: columns,
                        } => columns.clone(),
                        _ => Vec::new(),
                    })
                    .filter_map(|column| match column.value {
                        CellValue::Float64(value) => Some(value.to_bits()),
                        _ => None,
                    })
                    .collect::<Vec<_>>(),
                CanonicalEntry::Barrier(_) => Vec::new(),
            };
            assert_eq!(cells(&decoded), cells(&entry));
        }
    }

    fn stored(damage: impl FnOnce(&mut StoredTransaction)) -> Vec<u8> {
        let entry =
            CanonicalEntry::Transaction(transaction(vec![CanonicalOperationKind::InsertFullRow {
                columns: columns([(1, CellValue::Uuid16([1; 16]))]),
            }]));
        let Some(StoredRecord::Transaction(mut stored)) =
            StoredEntry::decode(&*entry.encode()).unwrap().record
        else {
            unreachable!()
        };
        damage(&mut stored);
        StoredEntry {
            record: Some(StoredRecord::Transaction(stored)),
        }
        .encode_to_vec()
    }

    fn cell(stored: &mut StoredTransaction) -> &mut change::ColumnValue {
        let Some(Payload::Insert(row)) = &mut stored.operations[0].payload else {
            unreachable!()
        };
        &mut row.columns[0]
    }

    /// Bytes that are not an entry, an entry without its record or key, an
    /// operation whose kind and payload disagree, a cell without a value, a
    /// uuid of other than 16 bytes and an invalid table id are refused, and
    /// none of them as an earlier release's entry.
    #[test]
    fn a_damaged_entry_is_refused() {
        let undamaged = stored(|_| {});
        assert!(CanonicalEntry::decode(&undamaged).is_ok());
        let damaged = [
            undamaged[..undamaged.len() - 1].to_vec(),
            vec![0xff, 0xff, 0xff],
            Vec::new(),
            stored(|stored| stored.key = None),
            stored(|stored| stored.operations[0].payload = None),
            stored(|stored| {
                stored.operations[0].payload = Some(Payload::Delete(change::Delete {}));
            }),
            stored(|stored| stored.operations[0].kind = 9),
            stored(|stored| cell(stored).value = None),
            stored(|stored| cell(stored).value = Some(change::CellValue { kind: None })),
            stored(|stored| {
                cell(stored).value = Some(change::CellValue {
                    kind: Some(Kind::Uuid16(vec![1; 15])),
                });
            }),
            stored(|stored| {
                cell(stored).value = Some(change::CellValue {
                    kind: Some(Kind::NullValue(1)),
                });
            }),
            stored(|stored| stored.operations[0].table_id = String::new()),
        ];
        for (index, bytes) in damaged.iter().enumerate() {
            let refused = CanonicalEntry::decode(bytes);
            assert!(
                matches!(
                    refused,
                    Err(EntryDecodeError::Protobuf(_)
                        | EntryDecodeError::Identifier(_)
                        | EntryDecodeError::Invalid(_))
                ),
                "damage {index}: {refused:?}"
            );
        }
        assert!(matches!(
            CanonicalEntry::decode(br#"{"recordKind":"TRANSACTION","record":{}}"#),
            Err(EntryDecodeError::EarlierFormat)
        ));
    }
}
