use std::collections::{BTreeMap, BTreeSet};

use bigdecimal::BigDecimal;
use canonical_log::{
    CanonicalBarrier, CanonicalEntry, CanonicalOperation, CanonicalOperationKind,
    CanonicalTransaction, CellValue as CanonicalCellValue, ColumnValue as CanonicalColumnValue,
    TransactionKey,
};
use chrono::{DateTime, NaiveDate, NaiveDateTime, NaiveTime};
use pgoutput_decoder::{
    CellValue as PgCellValue, DecodedChange, DecodedTransaction, LogicalMessage,
    LogicalMessageKind, OldTupleKind, RelationMetadata, ReplicaIdentity, RowPatch, TupleImage,
};
use postgres_types::PgLsn;
use prost::Message;
use serde::Deserialize;
use source_control::{PostgresTypeCodec, RegisteredTable, SchemaRegistry};
use sync_protocol::{
    change::{cell_value, CellValue as WireCellValue, ColumnValue as WireColumnValue, PrimaryKey},
    encode_primary_key,
    mutation::MutationLogicalMarker,
    PRIMARY_KEY_CODEC_VERSION,
};
use sync_types::{ColumnId, MutationId, SourceEpoch, TableId};
use uuid::Uuid;

use crate::{NormalizationError, TransactionNormalizer};

pub const POSTGRES_EPOCH_UNIX_MICROS: i64 = 946_684_800_000_000;
const MAX_MARKER_ID_BYTES: usize = 512;
const MAX_MUTATION_IDS: usize = 1_024;
const SERVER_TRANSACTION_ID_BYTES: usize = 16;

/// Version-1 source text codecs. The replication connection must use pgoutput
/// `binary=false`; binary tuple cells fail closed.
pub type PostgresTextCodec = PostgresTypeCodec;

trait DecodePostgresText {
    fn decode(self, input: &PgCellValue) -> Result<CanonicalCellValue, NormalizationError>;
    fn decode_text(self, bytes: &[u8]) -> Result<CanonicalCellValue, NormalizationError>;
}

impl DecodePostgresText for PostgresTypeCodec {
    fn decode(self, input: &PgCellValue) -> Result<CanonicalCellValue, NormalizationError> {
        match input {
            PgCellValue::Null => Ok(CanonicalCellValue::Null),
            PgCellValue::Binary(_) => Err(fail(
                "binary pgoutput tuple value received while binary=false is required",
            )),
            PgCellValue::Text(bytes) => self.decode_text(bytes),
        }
    }

    fn decode_text(self, bytes: &[u8]) -> Result<CanonicalCellValue, NormalizationError> {
        let text = std::str::from_utf8(bytes)
            .map_err(|_| fail("PostgreSQL text output is not valid UTF-8"))?;
        match self {
            Self::Bool => match text {
                "t" => Ok(CanonicalCellValue::Bool(true)),
                "f" => Ok(CanonicalCellValue::Bool(false)),
                _ => Err(fail(format!("invalid PostgreSQL bool output {text:?}"))),
            },
            Self::Int2 => parse_integer::<i16>(text),
            Self::Int4 => parse_integer::<i32>(text),
            Self::Int8 => parse_integer::<i64>(text),
            Self::Float4 => parse_float::<f32>(text),
            Self::Float8 => parse_float::<f64>(text),
            Self::Numeric => {
                let value = text
                    .parse::<BigDecimal>()
                    .map_err(|_| fail(format!("invalid PostgreSQL numeric output {text:?}")))?;
                Ok(CanonicalCellValue::DecimalText(
                    value.normalized().to_plain_string(),
                ))
            }
            Self::Text | Self::Varchar | Self::Bpchar => {
                Ok(CanonicalCellValue::Text(text.to_owned()))
            }
            Self::Bytea => {
                let encoded = text
                    .strip_prefix("\\x")
                    .ok_or_else(|| fail("PostgreSQL bytea_output must be hex"))?;
                let value = hex::decode(encoded)
                    .map_err(|_| fail(format!("invalid PostgreSQL bytea output {text:?}")))?;
                Ok(CanonicalCellValue::Blob(value))
            }
            Self::Uuid => {
                let value = Uuid::parse_str(text)
                    .map_err(|_| fail(format!("invalid PostgreSQL UUID output {text:?}")))?;
                Ok(CanonicalCellValue::Uuid16(*value.as_bytes()))
            }
            Self::Json | Self::Jsonb => {
                let value: serde_json::Value = serde_json::from_str(text)
                    .map_err(|_| fail("invalid PostgreSQL JSON output"))?;
                let canonical = serde_json::to_string(&value)
                    .map_err(|error| fail(format!("JSON normalization failed: {error}")))?;
                Ok(CanonicalCellValue::JsonText(canonical))
            }
            Self::Date => {
                let date = NaiveDate::parse_from_str(text, "%Y-%m-%d")
                    .map_err(|_| fail(format!("invalid ISO PostgreSQL date output {text:?}")))?;
                Ok(CanonicalCellValue::DateIso8601(
                    date.format("%Y-%m-%d").to_string(),
                ))
            }
            Self::Time => {
                let time = NaiveTime::parse_from_str(text, "%H:%M:%S%.f")
                    .map_err(|_| fail(format!("invalid ISO PostgreSQL time output {text:?}")))?;
                Ok(CanonicalCellValue::TimeIso8601(
                    time.format("%H:%M:%S%.f").to_string(),
                ))
            }
            Self::Timestamp => {
                let timestamp = NaiveDateTime::parse_from_str(text, "%Y-%m-%d %H:%M:%S%.f")
                    .map_err(|_| {
                        fail(format!("invalid ISO PostgreSQL timestamp output {text:?}"))
                    })?;
                Ok(CanonicalCellValue::TimestampUnixMicros(
                    timestamp.and_utc().timestamp_micros(),
                ))
            }
            Self::Timestamptz => {
                let timestamp = [
                    "%Y-%m-%d %H:%M:%S%.f%#z",
                    "%Y-%m-%d %H:%M:%S%.f%:z",
                    "%Y-%m-%d %H:%M:%S%.f%::z",
                ]
                .into_iter()
                .find_map(|format| DateTime::parse_from_str(text, format).ok())
                .ok_or_else(|| {
                    fail(format!(
                        "invalid ISO PostgreSQL timestamptz output {text:?}"
                    ))
                })?;
                Ok(CanonicalCellValue::TimestampUnixMicros(
                    timestamp.timestamp_micros(),
                ))
            }
        }
    }
}

fn parse_integer<T>(text: &str) -> Result<CanonicalCellValue, NormalizationError>
where
    T: std::str::FromStr + Into<i64>,
{
    text.parse::<T>()
        .map(Into::into)
        .map(CanonicalCellValue::Int64)
        .map_err(|_| fail(format!("invalid PostgreSQL integer output {text:?}")))
}

fn parse_float<T>(text: &str) -> Result<CanonicalCellValue, NormalizationError>
where
    T: std::str::FromStr + Into<f64>,
{
    let value = text
        .parse::<T>()
        .map(Into::into)
        .map_err(|_| fail(format!("invalid PostgreSQL float output {text:?}")))?;
    if !value.is_finite() {
        return Err(fail(
            "non-finite floating-point values are outside codec v1",
        ));
    }
    Ok(CanonicalCellValue::Float64(value))
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ColumnCodec {
    pub column_id: ColumnId,
    pub name: String,
    pub type_oid: u32,
    pub type_modifier: i32,
    /// Whether pgoutput's relation metadata marks this column as part of the
    /// configured replica identity. This is distinct from the application PK.
    pub is_replica_identity: bool,
    pub codec: PostgresTextCodec,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TableCodec {
    pub relation_oid: u32,
    pub namespace: String,
    pub relation_name: String,
    pub table_id: TableId,
    pub replica_identity: ReplicaIdentity,
    pub layout_generation: u64,
    pub columns: Vec<ColumnCodec>,
    /// Zero-based column indexes in primary-key ordinal order.
    pub primary_key_columns: Vec<usize>,
}

#[derive(Debug)]
pub struct SchemaNormalizer {
    schema_generation: u64,
    tables: BTreeMap<u32, TableCodec>,
    registered: BTreeMap<u32, RegisteredTable>,
}

impl SchemaNormalizer {
    /// Compiles a persisted schema-generation registry against the exact
    /// relation layouts observed by the active pgoutput decoder.
    pub fn from_registry(
        registry: &SchemaRegistry,
        relations: impl IntoIterator<Item = std::sync::Arc<RelationMetadata>>,
    ) -> Result<Self, NormalizationError> {
        let mut observed = BTreeMap::new();
        for relation in relations {
            let oid = relation.relation_oid;
            if observed.insert(oid, relation).is_some() {
                return Err(fail(format!("duplicate observed relation OID {oid}")));
            }
        }
        if observed.len() != registry.tables().len() {
            return Err(fail(format!(
                "observed {} relations, registry requires {}",
                observed.len(),
                registry.tables().len()
            )));
        }
        let tables = registry
            .tables()
            .iter()
            .map(|registered| {
                let relation = observed
                    .remove(&registered.inspection.relation_oid)
                    .ok_or_else(|| {
                        fail(format!(
                            "registry relation OID {} was not observed",
                            registered.inspection.relation_oid
                        ))
                    })?;
                table_codec_from_registry(registered, &relation)
            })
            .collect::<Result<Vec<_>, _>>()?;
        if !observed.is_empty() {
            return Err(fail("observed relation set contains unregistered OIDs"));
        }
        let mut normalizer = Self::new(registry.schema_generation(), tables)?;
        normalizer.registered = registered_tables_by_oid(registry)?;
        Ok(normalizer)
    }

    /// Creates a registry-backed normalizer that validates and caches relation
    /// layouts on first committed use. Logical replication sends Relation
    /// messages lazily per changed table, so snapshot roll-forward cannot
    /// require every publication table to appear before the first transaction.
    pub fn from_registry_dynamic(registry: &SchemaRegistry) -> Result<Self, NormalizationError> {
        if registry.schema_generation() == 0 || registry.tables().is_empty() {
            return Err(fail(
                "schema registry must contain a nonzero generation and tables",
            ));
        }
        Ok(Self {
            schema_generation: registry.schema_generation(),
            tables: BTreeMap::new(),
            registered: registered_tables_by_oid(registry)?,
        })
    }

    pub fn new(
        schema_generation: u64,
        tables: impl IntoIterator<Item = TableCodec>,
    ) -> Result<Self, NormalizationError> {
        if schema_generation == 0 {
            return Err(fail("schema generation must be nonzero"));
        }
        let mut by_relation = BTreeMap::new();
        let mut table_ids = BTreeSet::new();
        for table in tables {
            validate_table_codec(&table)?;
            if !table_ids.insert(table.table_id.clone()) {
                return Err(fail(format!("duplicate table ID {}", table.table_id)));
            }
            let relation_oid = table.relation_oid;
            if by_relation.insert(relation_oid, table).is_some() {
                return Err(fail(format!("duplicate relation OID {relation_oid}")));
            }
        }
        if by_relation.is_empty() {
            return Err(fail("schema normalizer requires at least one table"));
        }
        Ok(Self {
            schema_generation,
            tables: by_relation,
            registered: BTreeMap::new(),
        })
    }

    /// Canonicalizes a complete committed transaction without taking ownership.
    /// Snapshot roll-forward uses this same registry-checked codec as persistent
    /// CDC so the two paths cannot drift on values, keys, or table identity.
    pub fn normalize_committed(
        &mut self,
        transaction: &DecodedTransaction,
        source_epoch: &SourceEpoch,
    ) -> Result<CanonicalEntry, NormalizationError> {
        let commit_time_unix_micros = transaction
            .commit_time_postgres_micros
            .checked_add(POSTGRES_EPOCH_UNIX_MICROS)
            .and_then(|value| u64::try_from(value).ok())
            .ok_or_else(|| fail("PostgreSQL commit timestamp is outside Unix microsecond range"))?;

        let cut_markers: Vec<_> = transaction
            .changes
            .iter()
            .filter_map(|change| match change {
                DecodedChange::LogicalMessage(message)
                    if message.kind == LogicalMessageKind::CutMarker =>
                {
                    Some(message)
                }
                _ => None,
            })
            .collect();
        if !cut_markers.is_empty() {
            if cut_markers.len() != 1 || transaction.changes.len() != 1 {
                return Err(fail(
                    "snapshot cut marker must be the only change in its transaction",
                ));
            }
            return normalize_cut_marker(
                transaction,
                source_epoch,
                cut_markers[0],
                commit_time_unix_micros,
            );
        }

        let mut operations = Vec::new();
        let mut mutation_marker = None;
        for change in &transaction.changes {
            match change {
                DecodedChange::Insert { relation, row } => {
                    let table = self.table_for(relation)?;
                    require_cell_count(table, row.cells.len())?;
                    let columns = table
                        .columns
                        .iter()
                        .zip(&row.cells)
                        .map(|(column, value)| canonical_column(column, value))
                        .collect::<Result<Vec<_>, _>>()?;
                    operations.push(CanonicalOperation {
                        table_id: table.table_id.clone(),
                        primary_key: encode_key_from_row(table, &row.cells)?,
                        kind: CanonicalOperationKind::InsertFullRow { columns },
                    });
                }
                DecodedChange::Update {
                    relation,
                    old_tuple,
                    old_tuple_kind,
                    patch,
                } => {
                    let table = self.table_for(relation)?;
                    require_cell_count(table, patch.cells.len())?;
                    if let Some(old_tuple) = old_tuple {
                        require_cell_count(table, old_tuple.cells.len())?;
                    }
                    let changed_columns = normalize_patch(table, patch)?;
                    if changed_columns.is_empty() {
                        return Err(fail("pgoutput update contains no materialized values"));
                    }
                    match (old_tuple, old_tuple_kind) {
                        (Some(_), Some(old_tuple_kind)) => {
                            validate_old_tuple_kind(table, *old_tuple_kind)?
                        }
                        (None, None) => {}
                        _ => {
                            return Err(fail(
                                "pgoutput update old tuple and old tuple kind disagree",
                            ))
                        }
                    }
                    operations.push(CanonicalOperation {
                        table_id: table.table_id.clone(),
                        primary_key: encode_update_key(table, old_tuple.as_ref(), patch)?,
                        kind: CanonicalOperationKind::UpdatePatch { changed_columns },
                    });
                }
                DecodedChange::Delete {
                    relation,
                    old_tuple,
                    old_tuple_kind,
                } => {
                    let table = self.table_for(relation)?;
                    require_cell_count(table, old_tuple.cells.len())?;
                    validate_old_tuple_kind(table, *old_tuple_kind)?;
                    operations.push(CanonicalOperation {
                        table_id: table.table_id.clone(),
                        primary_key: encode_key_from_tuple(table, old_tuple)?,
                        kind: CanonicalOperationKind::DeleteByKey,
                    });
                }
                DecodedChange::Truncate {
                    relations,
                    cascade,
                    restart_identity,
                } => {
                    let mut seen = BTreeSet::new();
                    for relation in relations {
                        let table = self.table_for(relation)?;
                        if !seen.insert(table.relation_oid) {
                            return Err(fail("truncate contains a duplicate relation"));
                        }
                        operations.push(CanonicalOperation {
                            table_id: table.table_id.clone(),
                            primary_key: Vec::new(),
                            kind: CanonicalOperationKind::TruncateAuthorizedTable {
                                reason: format!(
                                    "postgres-truncate:cascade={cascade},restart-identity={restart_identity}"
                                ),
                            },
                        });
                    }
                }
                DecodedChange::LogicalMessage(message)
                    if message.kind == LogicalMessageKind::MutationMarker =>
                {
                    if mutation_marker.is_some() {
                        return Err(fail("transaction contains multiple mutation markers"));
                    }
                    mutation_marker = Some(decode_mutation_marker(message)?);
                }
                DecodedChange::LogicalMessage(_) => {}
            }
        }

        let (transaction_id, mutation_ids) = mutation_marker.map_or_else(
            || {
                Ok((
                    format!(
                        "pg:{}:{}:{}",
                        source_epoch,
                        transaction.xid,
                        pg_lsn(transaction.end_lsn.get())
                    ),
                    Vec::new(),
                ))
            },
            |marker| Ok((format!("mutation:{}", hex::encode(marker.0)), marker.1)),
        )?;

        Ok(CanonicalEntry::Transaction(CanonicalTransaction {
            key: transaction_key(transaction, source_epoch),
            transaction_id,
            begin_lsn: pg_lsn(transaction.first_observed_wal_lsn.get()),
            commit_lsn: pg_lsn(transaction.commit_lsn.get()),
            commit_time_unix_micros,
            operations,
            mutation_ids,
        }))
    }

    fn table_for(
        &mut self,
        relation: &RelationMetadata,
    ) -> Result<&TableCodec, NormalizationError> {
        if !self.tables.contains_key(&relation.relation_oid) {
            let registered = self.registered.get(&relation.relation_oid).ok_or_else(|| {
                fail(format!(
                    "relation OID {} is outside schema generation {}",
                    relation.relation_oid, self.schema_generation
                ))
            })?;
            let table = table_codec_from_registry(registered, relation)?;
            self.tables.insert(relation.relation_oid, table);
        }
        let table = self.tables.get(&relation.relation_oid).ok_or_else(|| {
            fail(format!(
                "relation OID {} is outside schema generation {}",
                relation.relation_oid, self.schema_generation
            ))
        })?;
        validate_relation(table, relation)?;
        Ok(table)
    }
}

fn registered_tables_by_oid(
    registry: &SchemaRegistry,
) -> Result<BTreeMap<u32, RegisteredTable>, NormalizationError> {
    let mut registered = BTreeMap::new();
    for table in registry.tables() {
        let oid = table.inspection.relation_oid;
        if registered.insert(oid, table.clone()).is_some() {
            return Err(fail(format!("duplicate registered relation OID {oid}")));
        }
    }
    Ok(registered)
}

fn table_codec_from_registry(
    registered: &RegisteredTable,
    relation: &RelationMetadata,
) -> Result<TableCodec, NormalizationError> {
    let inspection = &registered.inspection;
    if relation.relation_oid != inspection.relation_oid
        || relation.namespace != inspection.table.schema
        || relation.relation_name != inspection.table.table
        || relation.columns.len() != inspection.columns.len()
        || registered.column_ids.len() != inspection.columns.len()
    {
        return Err(fail(format!(
            "observed relation {}.{} does not match registered table {}",
            relation.namespace, relation.relation_name, registered.table_id
        )));
    }
    let replica_identity = match inspection.replica_identity {
        'd' => ReplicaIdentity::Default,
        'f' => ReplicaIdentity::Full,
        _ => {
            return Err(fail(format!(
                "registered table {} has unsupported replica identity",
                registered.table_id
            )))
        }
    };
    if relation.replica_identity != replica_identity {
        return Err(fail(format!(
            "observed relation {} replica identity differs from registry",
            registered.table_id
        )));
    }

    let mut primary_key = inspection
        .columns
        .iter()
        .enumerate()
        .filter_map(|(index, column)| column.primary_key_ordinal.map(|ordinal| (ordinal, index)))
        .collect::<Vec<_>>();
    primary_key.sort_by_key(|(ordinal, _)| *ordinal);
    if primary_key
        .iter()
        .enumerate()
        .any(|(index, (ordinal, _))| *ordinal != (index as u32) + 1)
    {
        return Err(fail(format!(
            "registered table {} primary-key ordinals are not contiguous",
            registered.table_id
        )));
    }

    let columns = inspection
        .columns
        .iter()
        .zip(&registered.column_ids)
        .zip(&relation.columns)
        .map(|((column, column_id), observed)| {
            if column.name != observed.name
                || column.type_oid != observed.type_oid
                || column.type_modifier != observed.type_modifier
            {
                return Err(fail(format!(
                    "observed relation {} column {} differs from registry",
                    registered.table_id, column.name
                )));
            }
            let expected_replica_identity = match replica_identity {
                ReplicaIdentity::Default => column.primary_key_ordinal.is_some(),
                ReplicaIdentity::Full => true,
                ReplicaIdentity::Nothing | ReplicaIdentity::Index => false,
            };
            if observed.is_key != expected_replica_identity {
                return Err(fail(format!(
                    "observed relation {} key flag differs at column {}",
                    registered.table_id, column.name
                )));
            }
            Ok(ColumnCodec {
                column_id: *column_id,
                name: column.name.clone(),
                type_oid: column.type_oid,
                type_modifier: column.type_modifier,
                is_replica_identity: observed.is_key,
                codec: column.codec().ok_or_else(|| {
                    fail(format!(
                        "{}.{} has no version-1 text codec",
                        column.type_schema, column.type_name
                    ))
                })?,
            })
        })
        .collect::<Result<Vec<_>, _>>()?;

    Ok(TableCodec {
        relation_oid: relation.relation_oid,
        namespace: relation.namespace.clone(),
        relation_name: relation.relation_name.clone(),
        table_id: registered.table_id.clone(),
        replica_identity,
        layout_generation: relation.layout_generation,
        columns,
        primary_key_columns: primary_key.into_iter().map(|(_, index)| index).collect(),
    })
}

impl TransactionNormalizer for SchemaNormalizer {
    fn normalize(
        &mut self,
        transaction: DecodedTransaction,
        source_epoch: &SourceEpoch,
    ) -> Result<CanonicalEntry, NormalizationError> {
        self.normalize_committed(&transaction, source_epoch)
    }
}

fn validate_table_codec(table: &TableCodec) -> Result<(), NormalizationError> {
    if table.relation_oid == 0
        || table.namespace.is_empty()
        || table.relation_name.is_empty()
        || table.layout_generation == 0
        || table.columns.is_empty()
    {
        return Err(fail(format!(
            "table {} codec is incomplete",
            table.table_id
        )));
    }
    let mut column_ids = BTreeSet::new();
    let mut names = BTreeSet::new();
    for column in &table.columns {
        if column.column_id.get() == 0
            || column.name.is_empty()
            || column.type_oid == 0
            || !column_ids.insert(column.column_id)
            || !names.insert(column.name.as_str())
        {
            return Err(fail(format!(
                "table {} has an invalid or duplicate column codec",
                table.table_id
            )));
        }
    }
    if table.columns.len() > 1_600 {
        return Err(fail(format!(
            "table {} exceeds PostgreSQL's 1600-column limit",
            table.table_id
        )));
    }
    if table.primary_key_columns.is_empty() || table.primary_key_columns.len() > 32 {
        return Err(fail(format!(
            "table {} requires 1-32 primary-key columns",
            table.table_id
        )));
    }
    let mut indexes = BTreeSet::new();
    for &index in &table.primary_key_columns {
        let column = table.columns.get(index).ok_or_else(|| {
            fail(format!(
                "table {} primary-key index {index} is out of range",
                table.table_id
            ))
        })?;
        if !indexes.insert(index) || !column.codec.is_primary_key_safe() {
            return Err(fail(format!(
                "table {} primary-key column {} is duplicated or lacks a safe v1 codec",
                table.table_id, column.name
            )));
        }
    }
    let replica_identity_columns = table
        .columns
        .iter()
        .enumerate()
        .filter_map(|(index, column)| column.is_replica_identity.then_some(index))
        .collect::<BTreeSet<_>>();
    let expected_identity_columns = match table.replica_identity {
        ReplicaIdentity::Default => indexes,
        ReplicaIdentity::Full => (0..table.columns.len()).collect(),
        ReplicaIdentity::Nothing | ReplicaIdentity::Index => {
            return Err(fail(format!(
                "table {} has an unsupported replica identity",
                table.table_id
            )))
        }
    };
    if replica_identity_columns != expected_identity_columns {
        return Err(fail(format!(
            "table {} replica-identity columns do not match its replica identity",
            table.table_id
        )));
    }
    Ok(())
}

fn validate_relation(
    table: &TableCodec,
    relation: &RelationMetadata,
) -> Result<(), NormalizationError> {
    if relation.namespace != table.namespace
        || relation.relation_name != table.relation_name
        || relation.replica_identity != table.replica_identity
        || relation.layout_generation != table.layout_generation
        || relation.columns.len() != table.columns.len()
    {
        return Err(fail(format!(
            "relation {}.{} layout differs from pinned table {} schema",
            relation.namespace, relation.relation_name, table.table_id
        )));
    }
    for (observed, expected) in relation.columns.iter().zip(&table.columns) {
        if observed.name != expected.name
            || observed.type_oid != expected.type_oid
            || observed.type_modifier != expected.type_modifier
            || observed.is_key != expected.is_replica_identity
        {
            return Err(fail(format!(
                "relation {}.{} column layout differs at {}",
                relation.namespace, relation.relation_name, expected.name
            )));
        }
    }
    Ok(())
}

fn require_cell_count(table: &TableCodec, actual: usize) -> Result<(), NormalizationError> {
    if actual != table.columns.len() {
        return Err(fail(format!(
            "table {} tuple has {actual} cells, expected {}",
            table.table_id,
            table.columns.len()
        )));
    }
    Ok(())
}

fn canonical_column(
    column: &ColumnCodec,
    value: &PgCellValue,
) -> Result<CanonicalColumnValue, NormalizationError> {
    Ok(CanonicalColumnValue {
        column_id: column.column_id,
        value: column.codec.decode(value)?,
    })
}

fn normalize_patch(
    table: &TableCodec,
    patch: &RowPatch,
) -> Result<Vec<CanonicalColumnValue>, NormalizationError> {
    table
        .columns
        .iter()
        .zip(&patch.cells)
        .filter_map(|(column, cell)| match cell {
            pgoutput_decoder::TupleCell::Absent => None,
            pgoutput_decoder::TupleCell::Value(value) => Some(canonical_column(column, value)),
        })
        .collect()
}

fn validate_old_tuple_kind(
    table: &TableCodec,
    actual: OldTupleKind,
) -> Result<(), NormalizationError> {
    let expected = match table.replica_identity {
        ReplicaIdentity::Default => OldTupleKind::Key,
        ReplicaIdentity::Full => OldTupleKind::Full,
        ReplicaIdentity::Nothing | ReplicaIdentity::Index => {
            return Err(fail(format!(
                "table {} has an unsupported replica identity",
                table.table_id
            )))
        }
    };
    if actual != expected {
        return Err(fail(format!(
            "table {} old tuple kind {actual:?} differs from replica identity {expected:?}",
            table.table_id
        )));
    }
    Ok(())
}

fn encode_key_from_row(
    table: &TableCodec,
    row: &[PgCellValue],
) -> Result<Vec<u8>, NormalizationError> {
    let parts = table
        .primary_key_columns
        .iter()
        .map(|&index| wire_key_part(&table.columns[index], &row[index]))
        .collect::<Result<Vec<_>, _>>()?;
    encode_key(parts)
}

fn encode_key_from_tuple(
    table: &TableCodec,
    tuple: &TupleImage,
) -> Result<Vec<u8>, NormalizationError> {
    let parts = table
        .primary_key_columns
        .iter()
        .map(|&index| match &tuple.cells[index] {
            pgoutput_decoder::TupleCell::Absent => Err(fail(format!(
                "primary-key column {} is absent from old tuple",
                table.columns[index].name
            ))),
            pgoutput_decoder::TupleCell::Value(value) => {
                wire_key_part(&table.columns[index], value)
            }
        })
        .collect::<Result<Vec<_>, _>>()?;
    encode_key(parts)
}

fn encode_update_key(
    table: &TableCodec,
    old_tuple: Option<&TupleImage>,
    patch: &RowPatch,
) -> Result<Vec<u8>, NormalizationError> {
    if let Some(old_tuple) = old_tuple {
        return encode_key_from_tuple(table, old_tuple);
    }
    let parts = table
        .primary_key_columns
        .iter()
        .map(|&index| match &patch.cells[index] {
            pgoutput_decoder::TupleCell::Absent => Err(fail(format!(
                "update has no old tuple and primary-key column {} is absent from new tuple",
                table.columns[index].name
            ))),
            pgoutput_decoder::TupleCell::Value(value) => {
                wire_key_part(&table.columns[index], value)
            }
        })
        .collect::<Result<Vec<_>, _>>()?;
    encode_key(parts)
}

fn wire_key_part(
    column: &ColumnCodec,
    value: &PgCellValue,
) -> Result<WireColumnValue, NormalizationError> {
    let canonical = column.codec.decode(value)?;
    if canonical == CanonicalCellValue::Null {
        return Err(fail(format!(
            "primary-key column {} decoded as null",
            column.name
        )));
    }
    Ok(WireColumnValue {
        column_id: column.column_id.get(),
        value: Some(wire_cell(&canonical)),
    })
}

fn encode_key(parts: Vec<WireColumnValue>) -> Result<Vec<u8>, NormalizationError> {
    encode_primary_key(&PrimaryKey {
        codec_version: PRIMARY_KEY_CODEC_VERSION,
        parts,
    })
    .map_err(|error| fail(format!("primary-key encoding failed: {error}")))
}

fn wire_cell(value: &CanonicalCellValue) -> WireCellValue {
    let kind = match value {
        CanonicalCellValue::Null => cell_value::Kind::NullValue(0),
        CanonicalCellValue::Int64(value) => cell_value::Kind::Int64Value(*value),
        CanonicalCellValue::Uint64(value) => cell_value::Kind::Uint64Value(*value),
        CanonicalCellValue::Float64(value) => cell_value::Kind::Float64Value(*value),
        CanonicalCellValue::Text(value) => cell_value::Kind::TextValue(value.clone()),
        CanonicalCellValue::Blob(value) => cell_value::Kind::BlobValue(value.clone()),
        CanonicalCellValue::Bool(value) => cell_value::Kind::BoolValue(*value),
        CanonicalCellValue::DecimalText(value) => cell_value::Kind::DecimalText(value.clone()),
        CanonicalCellValue::TimestampUnixMicros(value) => {
            cell_value::Kind::TimestampUnixMicros(*value)
        }
        CanonicalCellValue::Uuid16(value) => cell_value::Kind::Uuid16(value.to_vec()),
        CanonicalCellValue::JsonText(value) => cell_value::Kind::JsonText(value.clone()),
        CanonicalCellValue::DateIso8601(value) => cell_value::Kind::DateIso8601(value.clone()),
        CanonicalCellValue::TimeIso8601(value) => cell_value::Kind::TimeIso8601(value.clone()),
        CanonicalCellValue::IntervalIso8601(value) => {
            cell_value::Kind::IntervalIso8601(value.clone())
        }
    };
    WireCellValue { kind: Some(kind) }
}

fn decode_mutation_marker(
    message: &LogicalMessage,
) -> Result<(Vec<u8>, Vec<MutationId>), NormalizationError> {
    let marker = MutationLogicalMarker::decode(message.content.as_slice())
        .map_err(|_| fail("mutation logical marker protobuf is malformed"))?;
    if marker.encode_to_vec() != message.content {
        return Err(fail("mutation logical marker protobuf is not canonical"));
    }
    if marker.protocol_version != 1 {
        return Err(fail(format!(
            "mutation logical marker version {} is unsupported",
            marker.protocol_version
        )));
    }
    if marker.mutation_ids.is_empty() || marker.mutation_ids.len() > MAX_MUTATION_IDS {
        return Err(fail("mutation marker must contain 1-1024 mutation IDs"));
    }
    if marker.server_transaction_id.len() != SERVER_TRANSACTION_ID_BYTES {
        return Err(fail(
            "mutation marker server transaction ID must contain exactly 16 bytes",
        ));
    }
    let mut seen = BTreeSet::new();
    let mut ids = Vec::with_capacity(marker.mutation_ids.len());
    for value in marker.mutation_ids {
        let id = MutationId::new(value)
            .map_err(|error| fail(format!("invalid mutation ID in marker: {error}")))?;
        if !seen.insert(id.clone()) {
            return Err(fail("mutation marker contains a duplicate mutation ID"));
        }
        ids.push(id);
    }
    Ok((marker.server_transaction_id, ids))
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct CutIdentity {
    build_id: String,
    nonce: String,
}

fn normalize_cut_marker(
    transaction: &DecodedTransaction,
    source_epoch: &SourceEpoch,
    marker: &LogicalMessage,
    commit_time_unix_micros: u64,
) -> Result<CanonicalEntry, NormalizationError> {
    let identity: CutIdentity = serde_json::from_slice(&marker.content)
        .map_err(|_| fail("snapshot cut marker JSON is malformed"))?;
    if identity.build_id.is_empty()
        || identity.nonce.is_empty()
        || identity.build_id.len() > MAX_MARKER_ID_BYTES
        || identity.nonce.len() > MAX_MARKER_ID_BYTES
    {
        return Err(fail(
            "snapshot cut identity fields must contain 1-512 bytes",
        ));
    }
    if marker.message_lsn.get() > transaction.commit_lsn.get() {
        return Err(fail("snapshot cut marker LSN follows its commit LSN"));
    }
    Ok(CanonicalEntry::Barrier(CanonicalBarrier {
        key: transaction_key(transaction, source_epoch),
        build_id: identity.build_id,
        nonce: identity.nonce,
        message_lsn: pg_lsn(marker.message_lsn.get()),
        commit_lsn: pg_lsn(transaction.commit_lsn.get()),
        commit_time_unix_micros,
    }))
}

fn transaction_key(transaction: &DecodedTransaction, source_epoch: &SourceEpoch) -> TransactionKey {
    TransactionKey {
        source_epoch: source_epoch.clone(),
        end_lsn: pg_lsn(transaction.end_lsn.get()),
        postgres_xid: transaction.xid,
    }
}

fn pg_lsn(value: u64) -> String {
    PgLsn::from(value).to_string()
}

fn fail(message: impl Into<String>) -> NormalizationError {
    NormalizationError::new(message)
}

#[cfg(test)]
mod tests {
    use std::{collections::BTreeSet, sync::Arc};

    use canonical_log::CanonicalOperationKind;
    use pgoutput_decoder::{
        OldTupleKind, PgLsn as DecoderLsn, RelationColumn, ReplicaIdentity, RowImage,
        TransactionMode, TupleCell,
    };
    use source_control::{
        ColumnInspection, EnrollmentRequirements, EnrollmentSnapshot, PublicationSettings,
        QualifiedTable, SchemaRegistry, SourceIdentity, SourceSettings, TableInspection,
    };
    use sync_protocol::decode_primary_key;

    use super::*;

    const UUID_TEXT: &str = "123e4567-e89b-12d3-a456-426614174000";

    fn epoch() -> SourceEpoch {
        SourceEpoch::new("epoch-1").unwrap()
    }

    fn table_codec() -> TableCodec {
        TableCodec {
            relation_oid: 16_384,
            namespace: "public".to_owned(),
            relation_name: "tasks".to_owned(),
            table_id: TableId::new("tasks-v7").unwrap(),
            replica_identity: ReplicaIdentity::Default,
            layout_generation: 3,
            columns: vec![
                ColumnCodec {
                    column_id: ColumnId::new(5),
                    name: "tenant_id".to_owned(),
                    type_oid: 2_950,
                    type_modifier: -1,
                    is_replica_identity: true,
                    codec: PostgresTextCodec::Uuid,
                },
                ColumnCodec {
                    column_id: ColumnId::new(1),
                    name: "title".to_owned(),
                    type_oid: 25,
                    type_modifier: -1,
                    is_replica_identity: false,
                    codec: PostgresTextCodec::Text,
                },
                ColumnCodec {
                    column_id: ColumnId::new(9),
                    name: "sequence".to_owned(),
                    type_oid: 20,
                    type_modifier: -1,
                    is_replica_identity: true,
                    codec: PostgresTextCodec::Int8,
                },
            ],
            primary_key_columns: vec![2, 0],
        }
    }

    fn relation() -> Arc<RelationMetadata> {
        let table = table_codec();
        Arc::new(RelationMetadata {
            relation_oid: table.relation_oid,
            namespace: table.namespace,
            relation_name: table.relation_name,
            replica_identity: ReplicaIdentity::Default,
            columns: table
                .columns
                .into_iter()
                .map(|column| RelationColumn {
                    name: column.name,
                    type_oid: column.type_oid,
                    type_modifier: column.type_modifier,
                    is_key: column.is_replica_identity,
                })
                .collect(),
            layout_generation: table.layout_generation,
            first_observed_wal_lsn: DecoderLsn::new(10),
            last_observed_wal_lsn: DecoderLsn::new(20),
        })
    }

    fn transaction(changes: Vec<DecodedChange>) -> DecodedTransaction {
        DecodedTransaction {
            xid: 42,
            mode: TransactionMode::Normal,
            first_observed_wal_lsn: DecoderLsn::new(16),
            final_lsn: Some(DecoderLsn::new(24)),
            commit_lsn: DecoderLsn::new(24),
            end_lsn: DecoderLsn::new(32),
            commit_time_postgres_micros: 123,
            changes,
        }
    }

    fn text(value: &str) -> PgCellValue {
        PgCellValue::Text(value.as_bytes().to_vec())
    }

    fn normalizer() -> SchemaNormalizer {
        SchemaNormalizer::new(7, [table_codec()]).unwrap()
    }

    fn enrolled_registry(replica_identity: char) -> SchemaRegistry {
        let identity = SourceIdentity {
            system_identifier: "7612345678901234567".to_owned(),
            database_name: "app".to_owned(),
            timeline_id: 1,
            previous_timeline_id: 1,
        };
        let table = QualifiedTable {
            schema: "public".to_owned(),
            table: "tasks".to_owned(),
        };
        let snapshot = EnrollmentSnapshot {
            identity: identity.clone(),
            settings: SourceSettings {
                server_version_num: 180_004,
                server_encoding: "UTF8".to_owned(),
                date_style: "ISO, MDY".to_owned(),
                time_zone: "UTC".to_owned(),
                bytea_output: "hex".to_owned(),
                wal_level: "logical".to_owned(),
                max_replication_slots: 10,
                max_wal_senders: 10,
                current_user: "sync".to_owned(),
                current_user_is_superuser: false,
                current_user_has_replication: true,
            },
            publication: PublicationSettings {
                name: "sync_publication".to_owned(),
                exists: true,
                publishes_insert: true,
                publishes_update: true,
                publishes_delete: true,
                publishes_truncate: true,
                publishes_via_partition_root: false,
            },
            tables: vec![TableInspection {
                table: table.clone(),
                relation_oid: 16_384,
                in_publication: true,
                relation_kind: 'r',
                persistence: 'p',
                replica_identity,
                is_partition: false,
                has_publication_row_filter: false,
                has_publication_column_list: false,
                unlisted_columns: Vec::new(),
                columns: vec![
                    ColumnInspection {
                        attribute_number: 1,
                        name: "tenant_id".to_owned(),
                        type_oid: 2_950,
                        type_modifier: -1,
                        type_schema: "pg_catalog".to_owned(),
                        type_name: "uuid".to_owned(),
                        nullable: false,
                        primary_key_ordinal: Some(2),
                        generated: false,
                        nondeterministic_collation: false,
                        carried_as: None,
                    },
                    ColumnInspection {
                        attribute_number: 2,
                        name: "title".to_owned(),
                        type_oid: 25,
                        type_modifier: -1,
                        type_schema: "pg_catalog".to_owned(),
                        type_name: "text".to_owned(),
                        nullable: false,
                        primary_key_ordinal: None,
                        generated: false,
                        nondeterministic_collation: false,
                        carried_as: None,
                    },
                    ColumnInspection {
                        attribute_number: 3,
                        name: "sequence".to_owned(),
                        type_oid: 20,
                        type_modifier: -1,
                        type_schema: "pg_catalog".to_owned(),
                        type_name: "int8".to_owned(),
                        nullable: false,
                        primary_key_ordinal: Some(1),
                        generated: false,
                        nondeterministic_collation: false,
                        carried_as: None,
                    },
                ],
            }],
        };
        let requirements = EnrollmentRequirements {
            expected_identity: identity,
            publication_name: "sync_publication".to_owned(),
            tables: BTreeSet::from([table]),
            pgoutput_profile: source_control::PgoutputLaunchProfile::V1,
        };
        SchemaRegistry::initialize(&snapshot, &requirements).unwrap()
    }

    #[test]
    fn persisted_registry_compiles_exact_default_and_full_relation_layouts() {
        let registry = enrolled_registry('d');
        let mut compiled = SchemaNormalizer::from_registry(&registry, [relation()]).unwrap();
        assert_eq!(compiled.schema_generation, 1);
        let decoded = transaction(vec![DecodedChange::Insert {
            relation: relation(),
            row: RowImage {
                cells: vec![text(UUID_TEXT), text("first"), text("7")],
            },
        }]);
        let entry = compiled.normalize_committed(&decoded, &epoch()).unwrap();
        let CanonicalEntry::Transaction(transaction) = entry else {
            panic!("expected transaction")
        };
        let key = decode_primary_key(&transaction.operations[0].primary_key).unwrap();
        assert_eq!(
            key.parts
                .iter()
                .map(|part| part.column_id)
                .collect::<Vec<_>>(),
            [3, 1]
        );

        let mut full_relation = (*relation()).clone();
        full_relation.replica_identity = ReplicaIdentity::Full;
        for column in &mut full_relation.columns {
            column.is_key = true;
        }
        assert!(SchemaNormalizer::from_registry(
            &enrolled_registry('f'),
            [Arc::new(full_relation)]
        )
        .is_ok());
    }

    #[test]
    fn normalizes_crud_truncate_and_mutation_echo_in_decoded_order() {
        let relation = relation();
        let marker = MutationLogicalMarker {
            protocol_version: 1,
            mutation_ids: vec!["mutation-2".to_owned(), "mutation-1".to_owned()],
            server_transaction_id: vec![0xAB; SERVER_TRANSACTION_ID_BYTES],
        };
        let changes = vec![
            DecodedChange::Insert {
                relation: Arc::clone(&relation),
                row: RowImage {
                    cells: vec![text(UUID_TEXT), text("first"), text("7")],
                },
            },
            DecodedChange::Update {
                relation: Arc::clone(&relation),
                old_tuple: Some(TupleImage {
                    cells: vec![
                        TupleCell::Value(text(UUID_TEXT)),
                        TupleCell::Absent,
                        TupleCell::Value(text("7")),
                    ],
                }),
                old_tuple_kind: Some(OldTupleKind::Key),
                patch: RowPatch {
                    cells: vec![
                        TupleCell::Value(text(UUID_TEXT)),
                        TupleCell::Value(PgCellValue::Null),
                        TupleCell::Value(text("8")),
                    ],
                },
            },
            DecodedChange::Delete {
                relation: Arc::clone(&relation),
                old_tuple: TupleImage {
                    cells: vec![
                        TupleCell::Value(text(UUID_TEXT)),
                        TupleCell::Absent,
                        TupleCell::Value(text("8")),
                    ],
                },
                old_tuple_kind: OldTupleKind::Key,
            },
            DecodedChange::LogicalMessage(LogicalMessage {
                message_lsn: DecoderLsn::new(23),
                prefix: pgoutput_decoder::MUTATION_MARKER_PREFIX.to_owned(),
                content: marker.encode_to_vec(),
                kind: LogicalMessageKind::MutationMarker,
            }),
            DecodedChange::Truncate {
                relations: vec![relation],
                cascade: false,
                restart_identity: true,
            },
        ];

        let entry = normalizer()
            .normalize(transaction(changes), &epoch())
            .unwrap();
        let CanonicalEntry::Transaction(transaction) = entry else {
            panic!("expected canonical transaction")
        };
        assert_eq!(
            transaction.transaction_id,
            "mutation:abababababababababababababababab"
        );
        assert_eq!(transaction.commit_time_unix_micros, 946_684_800_000_123);
        assert_eq!(
            transaction
                .mutation_ids
                .iter()
                .map(MutationId::as_str)
                .collect::<Vec<_>>(),
            ["mutation-2", "mutation-1"]
        );
        assert_eq!(transaction.operations.len(), 4);

        let insert_key = decode_primary_key(&transaction.operations[0].primary_key).unwrap();
        assert_eq!(
            insert_key
                .parts
                .iter()
                .map(|part| part.column_id)
                .collect::<Vec<_>>(),
            [9, 5]
        );
        assert!(matches!(
            &transaction.operations[0].kind,
            CanonicalOperationKind::InsertFullRow { columns }
                if columns.len() == 3 && columns[1].value == CanonicalCellValue::Text("first".to_owned())
        ));
        assert!(matches!(
            &transaction.operations[1].kind,
            CanonicalOperationKind::UpdatePatch { changed_columns }
                if changed_columns.len() == 3
                    && changed_columns[1].value == CanonicalCellValue::Null
        ));
        let old_update_key = decode_primary_key(&transaction.operations[1].primary_key).unwrap();
        assert!(matches!(
            old_update_key.parts[0]
                .value
                .as_ref()
                .and_then(|value| value.kind.as_ref()),
            Some(cell_value::Kind::Int64Value(7))
        ));
        assert!(matches!(
            transaction.operations[2].kind,
            CanonicalOperationKind::DeleteByKey
        ));
        assert_eq!(key_sequence(&transaction.operations[2].primary_key), 8);
        assert!(matches!(
            &transaction.operations[3].kind,
            CanonicalOperationKind::TruncateAuthorizedTable { reason }
                if reason == "postgres-truncate:cascade=false,restart-identity=true"
        ));
        assert!(transaction.operations[3].primary_key.is_empty());
    }

    /// The `sequence` part of an encoded `tasks` primary key, its first part.
    fn key_sequence(primary_key: &[u8]) -> i64 {
        let key = decode_primary_key(primary_key).unwrap();
        match key.parts[0]
            .value
            .as_ref()
            .and_then(|value| value.kind.as_ref())
        {
            Some(cell_value::Kind::Int64Value(sequence)) => *sequence,
            other => panic!("expected an int64 key part, found {other:?}"),
        }
    }

    /// The `tasks` codec and relation under `REPLICA IDENTITY FULL`.
    fn full_identity() -> (TableCodec, Arc<RelationMetadata>) {
        let mut table = table_codec();
        table.replica_identity = ReplicaIdentity::Full;
        for column in &mut table.columns {
            column.is_replica_identity = true;
        }
        let mut relation = (*relation()).clone();
        relation.replica_identity = ReplicaIdentity::Full;
        for column in &mut relation.columns {
            column.is_key = true;
        }
        (table, Arc::new(relation))
    }

    #[test]
    fn update_without_old_tuple_uses_new_key_but_requires_every_key_part() {
        let relation = relation();
        let update = |sequence| DecodedChange::Update {
            relation: Arc::clone(&relation),
            old_tuple: None,
            old_tuple_kind: None,
            patch: RowPatch {
                cells: vec![
                    TupleCell::Value(text(UUID_TEXT)),
                    TupleCell::Value(text("changed")),
                    sequence,
                ],
            },
        };
        let entry = normalizer()
            .normalize(
                transaction(vec![update(TupleCell::Value(text("7")))]),
                &epoch(),
            )
            .unwrap();
        assert!(matches!(entry, CanonicalEntry::Transaction(_)));

        let error = normalizer()
            .normalize(transaction(vec![update(TupleCell::Absent)]), &epoch())
            .unwrap_err();
        assert!(error.to_string().contains(
            "update has no old tuple and primary-key column sequence is absent from new tuple"
        ));
    }

    #[test]
    fn full_replica_identity_keys_each_change_by_its_primary_key_columns() {
        let (table, relation) = full_identity();
        let row = |title: &str, sequence: &str| {
            vec![
                TupleCell::Value(text(UUID_TEXT)),
                TupleCell::Value(text(title)),
                TupleCell::Value(text(sequence)),
            ]
        };
        let changes = vec![
            DecodedChange::Update {
                relation: Arc::clone(&relation),
                old_tuple: Some(TupleImage {
                    cells: row("old-title", "7"),
                }),
                old_tuple_kind: Some(OldTupleKind::Full),
                patch: RowPatch {
                    cells: row("new-title", "8"),
                },
            },
            DecodedChange::Delete {
                relation,
                old_tuple: TupleImage {
                    cells: row("new-title", "8"),
                },
                old_tuple_kind: OldTupleKind::Full,
            },
        ];
        let CanonicalEntry::Transaction(transaction) = SchemaNormalizer::new(7, [table])
            .unwrap()
            .normalize(transaction(changes), &epoch())
            .unwrap()
        else {
            panic!("expected canonical transaction")
        };

        // The update applies to the row at its old key and moves it to the new one.
        assert_eq!(key_sequence(&transaction.operations[0].primary_key), 7);
        assert!(matches!(
            &transaction.operations[0].kind,
            CanonicalOperationKind::UpdatePatch { changed_columns }
                if changed_columns[2].value == CanonicalCellValue::Int64(8)
        ));
        assert_eq!(key_sequence(&transaction.operations[1].primary_key), 8);
        let key = decode_primary_key(&transaction.operations[1].primary_key).unwrap();
        assert_eq!(
            key.parts
                .iter()
                .map(|part| part.column_id)
                .collect::<Vec<_>>(),
            [9, 5]
        );
    }

    #[test]
    fn an_old_tuple_without_every_primary_key_part_fails_closed() {
        let (table, relation) = full_identity();
        let old_tuple = || TupleImage {
            cells: vec![
                TupleCell::Value(text(UUID_TEXT)),
                TupleCell::Value(text("title")),
                TupleCell::Absent,
            ],
        };
        let update = DecodedChange::Update {
            relation: Arc::clone(&relation),
            old_tuple: Some(old_tuple()),
            old_tuple_kind: Some(OldTupleKind::Full),
            patch: RowPatch {
                cells: vec![
                    TupleCell::Value(text(UUID_TEXT)),
                    TupleCell::Value(text("changed")),
                    TupleCell::Value(text("7")),
                ],
            },
        };
        let delete = DecodedChange::Delete {
            relation,
            old_tuple: old_tuple(),
            old_tuple_kind: OldTupleKind::Full,
        };
        for change in [update, delete] {
            let error = SchemaNormalizer::new(7, [table.clone()])
                .unwrap()
                .normalize(transaction(vec![change]), &epoch())
                .unwrap_err();
            assert!(error
                .to_string()
                .contains("primary-key column sequence is absent from old tuple"));
        }
    }

    #[test]
    fn cut_marker_becomes_a_barrier_only_when_transaction_is_dedicated() {
        let marker = LogicalMessage {
            message_lsn: DecoderLsn::new(20),
            prefix: pgoutput_decoder::CUT_MARKER_PREFIX.to_owned(),
            content: br#"{"build_id":"build-1","nonce":"nonce-1"}"#.to_vec(),
            kind: LogicalMessageKind::CutMarker,
        };
        let entry = normalizer()
            .normalize(
                transaction(vec![DecodedChange::LogicalMessage(marker.clone())]),
                &epoch(),
            )
            .unwrap();
        assert!(matches!(
            entry,
            CanonicalEntry::Barrier(CanonicalBarrier {
                build_id,
                nonce,
                message_lsn,
                ..
            }) if build_id == "build-1" && nonce == "nonce-1" && message_lsn == "0/14"
        ));

        let mixed = transaction(vec![
            DecodedChange::LogicalMessage(marker),
            DecodedChange::Insert {
                relation: relation(),
                row: RowImage {
                    cells: vec![text(UUID_TEXT), text("bad"), text("1")],
                },
            },
        ]);
        assert!(normalizer()
            .normalize(mixed, &epoch())
            .unwrap_err()
            .to_string()
            .contains("only change"));
    }

    #[test]
    fn relation_layout_binary_values_and_noncanonical_markers_fail_closed() {
        let mut changed = (*relation()).clone();
        changed.layout_generation += 1;
        let layout_error = normalizer()
            .normalize(
                transaction(vec![DecodedChange::Insert {
                    relation: Arc::new(changed),
                    row: RowImage {
                        cells: vec![text(UUID_TEXT), text("x"), text("1")],
                    },
                }]),
                &epoch(),
            )
            .unwrap_err();
        assert!(layout_error.to_string().contains("layout differs"));

        let binary_error = normalizer()
            .normalize(
                transaction(vec![DecodedChange::Insert {
                    relation: relation(),
                    row: RowImage {
                        cells: vec![PgCellValue::Binary(vec![0; 16]), text("x"), text("1")],
                    },
                }]),
                &epoch(),
            )
            .unwrap_err();
        assert!(binary_error.to_string().contains("binary=false"));

        let marker = MutationLogicalMarker {
            protocol_version: 1,
            mutation_ids: vec!["mutation-1".to_owned()],
            server_transaction_id: vec![1],
        };
        let mut content = marker.encode_to_vec();
        content.extend_from_slice(&[0x98, 0x06, 0x01]);
        let marker_error = normalizer()
            .normalize(
                transaction(vec![DecodedChange::LogicalMessage(LogicalMessage {
                    message_lsn: DecoderLsn::new(20),
                    prefix: pgoutput_decoder::MUTATION_MARKER_PREFIX.to_owned(),
                    content,
                    kind: LogicalMessageKind::MutationMarker,
                })]),
                &epoch(),
            )
            .unwrap_err();
        assert!(marker_error.to_string().contains("not canonical"));

        let short_marker = MutationLogicalMarker {
            protocol_version: 1,
            mutation_ids: vec!["mutation-1".to_owned()],
            server_transaction_id: vec![1],
        };
        let marker_error = normalizer()
            .normalize(
                transaction(vec![DecodedChange::LogicalMessage(LogicalMessage {
                    message_lsn: DecoderLsn::new(20),
                    prefix: pgoutput_decoder::MUTATION_MARKER_PREFIX.to_owned(),
                    content: short_marker.encode_to_vec(),
                    kind: LogicalMessageKind::MutationMarker,
                })]),
                &epoch(),
            )
            .unwrap_err();
        assert!(marker_error.to_string().contains("exactly 16 bytes"));
    }

    #[test]
    fn maintained_text_codecs_normalize_values_and_reject_ambiguous_forms() {
        let cases = [
            (PostgresTextCodec::Bool, "t", CanonicalCellValue::Bool(true)),
            (
                PostgresTextCodec::Int2,
                "-12",
                CanonicalCellValue::Int64(-12),
            ),
            (PostgresTextCodec::Int4, "42", CanonicalCellValue::Int64(42)),
            (
                PostgresTextCodec::Numeric,
                "001.2300",
                CanonicalCellValue::DecimalText("1.23".to_owned()),
            ),
            (
                PostgresTextCodec::Bytea,
                "\\x00abff",
                CanonicalCellValue::Blob(vec![0, 0xAB, 0xFF]),
            ),
            (
                PostgresTextCodec::Uuid,
                UUID_TEXT,
                CanonicalCellValue::Uuid16([
                    0x12, 0x3E, 0x45, 0x67, 0xE8, 0x9B, 0x12, 0xD3, 0xA4, 0x56, 0x42, 0x66, 0x14,
                    0x17, 0x40, 0x00,
                ]),
            ),
            (
                PostgresTextCodec::Jsonb,
                r#"{"z":1.2300,"a":true}"#,
                CanonicalCellValue::JsonText(r#"{"a":true,"z":1.2300}"#.to_owned()),
            ),
            (
                PostgresTextCodec::Date,
                "2026-07-14",
                CanonicalCellValue::DateIso8601("2026-07-14".to_owned()),
            ),
            (
                PostgresTextCodec::Time,
                "12:34:56.123400",
                CanonicalCellValue::TimeIso8601("12:34:56.123400".to_owned()),
            ),
            (
                PostgresTextCodec::Timestamp,
                "1970-01-01 00:00:00.000001",
                CanonicalCellValue::TimestampUnixMicros(1),
            ),
            (
                PostgresTextCodec::Timestamptz,
                "1970-01-01 01:00:00.000001+01",
                CanonicalCellValue::TimestampUnixMicros(1),
            ),
        ];
        for (codec, input, expected) in cases {
            assert_eq!(codec.decode(&text(input)).unwrap(), expected, "{codec:?}");
        }

        for (codec, input) in [
            (PostgresTextCodec::Bool, "true"),
            (PostgresTextCodec::Float8, "NaN"),
            (PostgresTextCodec::Float4, "Infinity"),
            (PostgresTextCodec::Bytea, "abc"),
            (PostgresTextCodec::Json, "{bad"),
        ] {
            assert!(codec.decode(&text(input)).is_err(), "{codec:?} {input}");
        }
    }

    /// A text key part is `text_value`, the string PostgreSQL sent, in an
    /// insert and in an update keyed by its old tuple, where the new tuple's
    /// key is an unchanged-TOAST marker.
    #[test]
    fn a_text_key_part_is_the_text_postgres_sent() {
        let mut table = table_codec();
        table.columns[0].is_replica_identity = false;
        table.columns[1].is_replica_identity = true;
        table.primary_key_columns = vec![1, 2];
        let mut relation = (*relation()).clone();
        relation.columns[0].is_key = false;
        relation.columns[1].is_key = true;
        let relation = Arc::new(relation);
        let key_text = "é-\u{1d11e}";
        let changes = vec![
            DecodedChange::Insert {
                relation: Arc::clone(&relation),
                row: RowImage {
                    cells: vec![text(UUID_TEXT), text(key_text), text("7")],
                },
            },
            DecodedChange::Update {
                relation: Arc::clone(&relation),
                old_tuple: Some(TupleImage {
                    cells: vec![
                        TupleCell::Absent,
                        TupleCell::Value(text(key_text)),
                        TupleCell::Value(text("7")),
                    ],
                }),
                old_tuple_kind: Some(OldTupleKind::Key),
                patch: RowPatch {
                    cells: vec![
                        TupleCell::Value(text(UUID_TEXT)),
                        TupleCell::Absent,
                        TupleCell::Value(text("7")),
                    ],
                },
            },
        ];
        let entry = SchemaNormalizer::new(7, [table])
            .unwrap()
            .normalize(transaction(changes), &epoch())
            .unwrap();
        let CanonicalEntry::Transaction(transaction) = entry else {
            panic!("expected canonical transaction")
        };
        for operation in &transaction.operations {
            let key = decode_primary_key(&operation.primary_key).unwrap();
            let kinds: Vec<_> = key
                .parts
                .iter()
                .map(|part| {
                    (
                        part.column_id,
                        part.value.as_ref().and_then(|v| v.kind.clone()),
                    )
                })
                .collect();
            assert_eq!(
                kinds,
                [
                    (1, Some(cell_value::Kind::TextValue(key_text.to_owned()))),
                    (9, Some(cell_value::Kind::Int64Value(7))),
                ]
            );
        }
        assert_eq!(transaction.operations.len(), 2);
    }

    #[test]
    fn invalid_schema_registry_mappings_are_rejected_at_construction() {
        let mut table = table_codec();
        table.columns[0].codec = PostgresTextCodec::Bpchar;
        assert!(SchemaNormalizer::new(7, [table])
            .unwrap_err()
            .to_string()
            .contains("lacks a safe v1 codec"));
        let mut table = table_codec();
        table.columns[0].codec = PostgresTextCodec::Text;
        assert!(SchemaNormalizer::new(7, [table]).is_ok());

        let table = table_codec();
        assert!(SchemaNormalizer::new(0, [table]).is_err());
        assert!(SchemaNormalizer::new(1, Vec::<TableCodec>::new()).is_err());

        let mut missing_identity = table_codec();
        missing_identity.columns[0].is_replica_identity = false;
        assert!(SchemaNormalizer::new(7, [missing_identity])
            .unwrap_err()
            .to_string()
            .contains("replica-identity columns do not match"));
    }
}
