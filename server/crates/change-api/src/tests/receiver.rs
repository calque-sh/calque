//! A receiver that applies change operations as a client does, so a test can
//! compare where two deliveries leave it.

use std::collections::BTreeMap;

use sync_protocol::{
    change::{operation::Payload, CellValue, ColumnValue, Operation, PrimaryKey},
    decode_primary_key, encode_primary_key,
};

/// Rows by table and primary key, each a value per column.
pub(crate) type Tables = BTreeMap<String, BTreeMap<Vec<u8>, BTreeMap<u32, Option<CellValue>>>>;

/// Applies one operation, or refuses it as a receiver does: an insert of a
/// row it holds, a patch or a delete of one it lacks, or a patch that moves a
/// row onto the key of another.
pub(crate) fn receive(tables: &mut Tables, operation: &Operation) -> Result<(), ()> {
    let table = tables.entry(operation.table_id.clone()).or_default();
    let received = match &operation.payload {
        Some(Payload::Insert(row)) => {
            let values = row
                .columns
                .iter()
                .map(|column| (column.column_id, column.value.clone()))
                .collect();
            match table.insert(operation.primary_key.clone(), values) {
                None => Ok(()),
                Some(_) => Err(()),
            }
        }
        Some(Payload::Update(patch)) => {
            let mut row = table.remove(&operation.primary_key).ok_or(())?;
            for column in &patch.changed_columns {
                row.insert(column.column_id, column.value.clone());
            }
            let key = decode_primary_key(&operation.primary_key).map_err(|_| ())?;
            let parts = key
                .parts
                .iter()
                .map(|part| ColumnValue {
                    column_id: part.column_id,
                    value: row
                        .get(&part.column_id)
                        .cloned()
                        .unwrap_or_else(|| part.value.clone()),
                })
                .collect();
            let moved = encode_primary_key(&PrimaryKey {
                codec_version: key.codec_version,
                parts,
            })
            .map_err(|_| ())?;
            match table.insert(moved, row) {
                None => Ok(()),
                Some(_) => Err(()),
            }
        }
        Some(Payload::Delete(_)) => table.remove(&operation.primary_key).map(|_| ()).ok_or(()),
        Some(Payload::Truncate(_)) => {
            table.clear();
            Ok(())
        }
        None => Err(()),
    };
    if table.is_empty() {
        tables.remove(&operation.table_id);
    }
    received
}
