//! One generic CRUD command per enrolled table.
//!
//! The command's input is the change stream's own `Operation` grammar, so the
//! bytes a client applies optimistically are the bytes the server validates,
//! and the CDC echo of the committed row confirms them. The command writes the
//! row the operation names inside the transaction `mutation-api` opened. The
//! scope must permit the table and every column the operation touches.

use std::{
    collections::{BTreeMap, BTreeSet},
    fmt::Write as _,
};

use authz::AuthorizedScope;
use mutation_api::{
    AffectedKey, CommandContext, CommandFailure, CommandFootprint, CommandOperation,
    MutationCommand,
};
use postgres::{types::ToSql, Transaction};
use prost::Message;
use source_control::{quote_identifier, ColumnInspection, PostgresTypeCodec, RegisteredTable};
use sync_protocol::{
    change::{cell_value::Kind, operation::Payload, ColumnValue, Operation, OperationKind},
    decode_primary_key,
};
use sync_types::{ColumnId, TableId};
use thiserror::Error;
use time::{format_description::well_known::Rfc3339, OffsetDateTime};

pub(crate) const CRUD_COMMAND_VERSION: u32 = 1;
const CRUD_COMMAND_PREFIX: &str = "crud:";

/// Which operations a table's command admits.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct CrudOperations {
    pub insert: bool,
    pub update: bool,
    pub delete: bool,
}

impl CrudOperations {
    pub const ALL: Self = Self {
        insert: true,
        update: true,
        delete: true,
    };

    fn permits(self, kind: OperationKind) -> bool {
        match kind {
            OperationKind::InsertFullRow => self.insert,
            OperationKind::UpdatePatch => self.update,
            OperationKind::DeleteByKey => self.delete,
            OperationKind::Unspecified | OperationKind::TruncateAuthorizedTable => false,
        }
    }
}

/// What one column is to the CRUD command over its table: part of the key, or
/// writable by a patch, or neither. The registry and `calque init`'s manifest
/// writer both read [`admitted_operations`] from it, so they cannot disagree.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct CrudColumnRole {
    pub key: bool,
    pub writable: bool,
}

impl CrudColumnRole {
    /// The role of a column with these catalog facts.
    ///
    /// A key column is never writable: the key is immutable. A generated
    /// column cannot be written at all. A column whose PostgreSQL type has no
    /// version-1 canonical codec cannot be bound as a parameter, so it is not
    /// writable either.
    pub fn of(type_schema: &str, type_name: &str, generated: bool, key: bool) -> Self {
        Self {
            key,
            writable: !key
                && !generated
                && PostgresTypeCodec::from_catalog(type_schema, type_name).is_some(),
        }
    }
}

/// The operations a CRUD command over `columns` admits, in facade order.
///
/// `insert` appears only when the command can carry every column the table
/// has, so an `insert` method is never generated for a row the command could
/// not write; `update` only when there is something to patch; `delete`
/// whenever the table's command permits it, since a delete names only the key.
pub fn admitted_operations(
    columns: impl IntoIterator<Item = CrudColumnRole>,
    operations: CrudOperations,
) -> Vec<CrudOperationName> {
    let mut every_column_writable_or_key = true;
    let mut any_writable = false;
    for column in columns {
        every_column_writable_or_key &= column.key || column.writable;
        any_writable |= column.writable;
    }
    let mut admitted = Vec::with_capacity(3);
    if operations.insert && every_column_writable_or_key {
        admitted.push(CrudOperationName::Insert);
    }
    if operations.update && any_writable {
        admitted.push(CrudOperationName::Update);
    }
    if operations.delete {
        admitted.push(CrudOperationName::Delete);
    }
    admitted
}

#[derive(Debug, Error)]
pub(crate) enum CrudCommandError {
    #[error("table has no primary key")]
    NoPrimaryKey,
    #[error("primary-key column {column} has a type the v1 key codec does not carry")]
    UnsupportedKeyType { column: String },
    #[error("registered column ids do not match the inspected columns")]
    ColumnMismatch,
}

#[derive(Debug, Clone)]
struct CrudColumn {
    column_id: u32,
    name: String,
    codec: Option<PostgresTypeCodec>,
    /// `"schema"."type"`, the cast every bound parameter goes through.
    sql_type: String,
    nullable: bool,
    key: bool,
    writable: bool,
}

/// The command `crud:<table id>` for one registered table.
pub struct CrudCommand {
    name: &'static str,
    table_id: TableId,
    qualified_table: String,
    columns: Vec<CrudColumn>,
    by_column_id: BTreeMap<u32, usize>,
    /// Column ids in the table's key order.
    key: Vec<u32>,
    operations: CrudOperations,
}

impl CrudCommand {
    /// The command name a table's mutations are submitted under.
    pub fn name_for(table_id: &TableId) -> String {
        format!("{CRUD_COMMAND_PREFIX}{}", table_id.as_str())
    }

    pub(crate) fn for_table(
        table: &RegisteredTable,
        operations: CrudOperations,
    ) -> Result<Self, CrudCommandError> {
        if table.column_ids.len() != table.inspection.columns.len() {
            return Err(CrudCommandError::ColumnMismatch);
        }
        let mut columns = Vec::with_capacity(table.column_ids.len());
        let mut key_ordinals = Vec::new();
        for (column_id, inspection) in table.column_ids.iter().zip(&table.inspection.columns) {
            let codec =
                PostgresTypeCodec::from_catalog(&inspection.type_schema, &inspection.type_name);
            let key = inspection.primary_key_ordinal.is_some();
            if key && !codec.is_some_and(PostgresTypeCodec::is_primary_key_safe) {
                return Err(CrudCommandError::UnsupportedKeyType {
                    column: inspection.name.clone(),
                });
            }
            if let Some(ordinal) = inspection.primary_key_ordinal {
                key_ordinals.push((ordinal, column_id.get()));
            }
            let role = CrudColumnRole::of(
                &inspection.type_schema,
                &inspection.type_name,
                inspection.generated,
                key,
            );
            columns.push(CrudColumn {
                column_id: column_id.get(),
                name: inspection.name.clone(),
                codec,
                sql_type: sql_type(inspection),
                nullable: inspection.nullable,
                key,
                writable: role.writable,
            });
        }
        if key_ordinals.is_empty() {
            return Err(CrudCommandError::NoPrimaryKey);
        }
        key_ordinals.sort_unstable();
        let by_column_id = columns
            .iter()
            .enumerate()
            .map(|(index, column)| (column.column_id, index))
            .collect();
        Ok(Self {
            // Commands live for the process; the registry holds them as
            // `'static` names, and one small leak per enrolled table is the
            // price of that signature.
            name: Box::leak(Self::name_for(&table.table_id).into_boxed_str()),
            table_id: table.table_id.clone(),
            qualified_table: format!(
                "{}.{}",
                quote_identifier(&table.inspection.table.schema),
                quote_identifier(&table.inspection.table.table)
            ),
            columns,
            by_column_id,
            key: key_ordinals.into_iter().map(|(_, id)| id).collect(),
            operations,
        })
    }

    /// Column names a patch may change, in catalog order.
    pub fn writable_columns(&self) -> impl Iterator<Item = &str> {
        self.columns
            .iter()
            .filter(|column| column.writable)
            .map(|column| column.name.as_str())
    }

    /// What a generated collection may declare for this command.
    pub fn summary(&self) -> CrudCommandSummary {
        CrudCommandSummary {
            table_id: self.table_id.clone(),
            name: self.name.to_owned(),
            writable_columns: self.writable_columns().map(str::to_owned).collect(),
            operations: admitted_operations(self.column_roles(), self.operations),
        }
    }

    /// This command's columns as [`admitted_operations`] reads them.
    fn column_roles(&self) -> impl Iterator<Item = CrudColumnRole> + '_ {
        self.columns.iter().map(|column| CrudColumnRole {
            key: column.key,
            writable: column.writable,
        })
    }

    fn column(&self, column_id: u32) -> Option<&CrudColumn> {
        self.by_column_id
            .get(&column_id)
            .and_then(|index| self.columns.get(*index))
    }

    /// Decodes and validates one operation against the table's shape. Every
    /// refusal here is `Validation` except an operation the table's command
    /// does not admit, which is `Authorization`.
    fn parse(&self, canonical_input: &[u8]) -> Result<ParsedOperation, CommandFailure> {
        let operation =
            Operation::decode(canonical_input).map_err(|_| CommandFailure::Validation)?;
        if operation.encode_to_vec() != canonical_input {
            return Err(CommandFailure::Validation);
        }
        if operation.table_id != self.table_id.as_str() {
            return Err(CommandFailure::Validation);
        }
        let kind =
            OperationKind::try_from(operation.kind).map_err(|_| CommandFailure::Validation)?;
        if matches!(
            kind,
            OperationKind::Unspecified | OperationKind::TruncateAuthorizedTable
        ) {
            return Err(CommandFailure::Validation);
        }
        if !self.operations.permits(kind) {
            return Err(CommandFailure::Authorization);
        }
        let key = self.parse_key(&operation.primary_key)?;
        let (columns, written) = match (kind, operation.payload) {
            (OperationKind::InsertFullRow, Some(Payload::Insert(row))) => {
                let columns = self.parse_columns(&row.columns, ColumnRule::FullRow(&key))?;
                let written = columns.iter().map(|column| column.column_id).collect();
                (columns, written)
            }
            (OperationKind::UpdatePatch, Some(Payload::Update(patch))) => {
                let columns = self.parse_columns(&patch.changed_columns, ColumnRule::Patch)?;
                let written = columns.iter().map(|column| column.column_id).collect();
                (columns, written)
            }
            (OperationKind::DeleteByKey, Some(Payload::Delete(_))) => (Vec::new(), BTreeSet::new()),
            _ => return Err(CommandFailure::Validation),
        };
        Ok(ParsedOperation {
            kind,
            primary_key: operation.primary_key,
            key,
            columns,
            written,
        })
    }

    fn parse_key(&self, encoded: &[u8]) -> Result<Vec<BoundCell>, CommandFailure> {
        let key = decode_primary_key(encoded).map_err(|_| CommandFailure::Validation)?;
        if key.parts.len() != self.key.len()
            || key
                .parts
                .iter()
                .zip(&self.key)
                .any(|(part, expected)| part.column_id != *expected)
        {
            return Err(CommandFailure::Validation);
        }
        key.parts
            .iter()
            .map(|part| {
                let column = self
                    .column(part.column_id)
                    .ok_or(CommandFailure::Validation)?;
                let kind = cell_kind(part).ok_or(CommandFailure::Validation)?;
                let text = key_part_text(column, kind)?;
                Ok(BoundCell {
                    column_id: column.column_id,
                    text: Some(text),
                })
            })
            .collect()
    }

    fn parse_columns(
        &self,
        wire: &[ColumnValue],
        rule: ColumnRule<'_>,
    ) -> Result<Vec<BoundCell>, CommandFailure> {
        let mut seen = BTreeSet::new();
        let mut cells = Vec::with_capacity(wire.len());
        for value in wire {
            let column = self
                .column(value.column_id)
                .ok_or(CommandFailure::Validation)?;
            if !seen.insert(column.column_id) {
                return Err(CommandFailure::Validation);
            }
            let kind = cell_kind(value).ok_or(CommandFailure::Validation)?;
            let text = match rule {
                ColumnRule::Patch => {
                    if column.key {
                        return Err(CommandFailure::Validation);
                    }
                    if !column.writable {
                        return Err(CommandFailure::Authorization);
                    }
                    cell_text(column, kind)?
                }
                ColumnRule::FullRow(key) => {
                    if column.key {
                        let text = cell_text(column, kind)?;
                        let matches = key
                            .iter()
                            .any(|part| part.column_id == column.column_id && part.text == text);
                        if !matches {
                            return Err(CommandFailure::Validation);
                        }
                        text
                    } else {
                        if !column.writable {
                            return Err(CommandFailure::Authorization);
                        }
                        cell_text(column, kind)?
                    }
                }
            };
            cells.push(BoundCell {
                column_id: column.column_id,
                text,
            });
        }
        match rule {
            ColumnRule::Patch if cells.is_empty() => Err(CommandFailure::Validation),
            ColumnRule::FullRow(_) if seen.len() != self.columns.len() => {
                Err(CommandFailure::Validation)
            }
            _ => Ok(cells),
        }
    }

    /// The scope's verdict on one parsed operation: the table, the row's
    /// partition when the table is partitioned, and every column the
    /// operation touches. A patch cannot change a key column, so a write
    /// never moves a row out of its partition.
    fn authorize(
        &self,
        parsed: &ParsedOperation,
        scope: &AuthorizedScope,
    ) -> Result<(), CommandFailure> {
        let permit = scope
            .table_permit(&self.table_id)
            .ok_or(CommandFailure::Authorization)?;
        if !matches!(
            scope.admits_row_key(&self.table_id, &parsed.primary_key),
            Ok(true)
        ) {
            return Err(CommandFailure::Authorization);
        }
        let touched = self.key.iter().chain(parsed.written.iter());
        for column_id in touched {
            if !permit.permits_column(ColumnId::new(*column_id)) {
                return Err(CommandFailure::Authorization);
            }
        }
        Ok(())
    }

    /// The statement for one parsed, authorized operation and its parameters
    /// in `$n` order. Every value is bound as text and cast to the column's
    /// declared type, so PostgreSQL parses each literal exactly as it would
    /// in a client's own SQL.
    fn statement(&self, parsed: &ParsedOperation) -> Statement {
        let mut sql = String::new();
        let mut parameters: Vec<Option<String>> = Vec::new();
        let mut bind = |cell: &BoundCell, parameters: &mut Vec<Option<String>>| -> String {
            parameters.push(cell.text.clone());
            let column = self
                .column(cell.column_id)
                .expect("parsed cells name known columns");
            format!("(${}::text)::{}", parameters.len(), column.sql_type)
        };
        let column_name = |column_id: u32| {
            quote_identifier(
                &self
                    .column(column_id)
                    .expect("parsed cells name known columns")
                    .name,
            )
        };
        match parsed.kind {
            OperationKind::InsertFullRow => {
                let names: Vec<String> = parsed
                    .columns
                    .iter()
                    .map(|cell| column_name(cell.column_id))
                    .collect();
                let values: Vec<String> = parsed
                    .columns
                    .iter()
                    .map(|cell| bind(cell, &mut parameters))
                    .collect();
                let _ = write!(
                    sql,
                    "INSERT INTO {} ({}) VALUES ({})",
                    self.qualified_table,
                    names.join(", "),
                    values.join(", ")
                );
            }
            OperationKind::UpdatePatch => {
                let assignments: Vec<String> = parsed
                    .columns
                    .iter()
                    .map(|cell| {
                        format!(
                            "{} = {}",
                            column_name(cell.column_id),
                            bind(cell, &mut parameters)
                        )
                    })
                    .collect();
                let _ = write!(
                    sql,
                    "UPDATE {} SET {} WHERE ",
                    self.qualified_table,
                    assignments.join(", ")
                );
                let predicate = self.predicate(&parsed.key, &mut bind, &mut parameters);
                sql.push_str(&predicate);
            }
            OperationKind::DeleteByKey => {
                let _ = write!(sql, "DELETE FROM {} WHERE ", self.qualified_table);
                let predicate = self.predicate(&parsed.key, &mut bind, &mut parameters);
                sql.push_str(&predicate);
            }
            OperationKind::Unspecified | OperationKind::TruncateAuthorizedTable => {
                unreachable!("parse admits only row operations")
            }
        }
        Statement { sql, parameters }
    }

    fn predicate(
        &self,
        key: &[BoundCell],
        bind: &mut impl FnMut(&BoundCell, &mut Vec<Option<String>>) -> String,
        parameters: &mut Vec<Option<String>>,
    ) -> String {
        key.iter()
            .map(|cell| {
                let name = quote_identifier(
                    &self
                        .column(cell.column_id)
                        .expect("parsed cells name known columns")
                        .name,
                );
                format!("{name} = {}", bind(cell, parameters))
            })
            .collect::<Vec<_>>()
            .join(" AND ")
    }
}

impl MutationCommand for CrudCommand {
    fn name(&self) -> &'static str {
        self.name
    }

    fn version(&self) -> u32 {
        CRUD_COMMAND_VERSION
    }

    /// The capability operation one input performs, read from the same parse
    /// the execution uses.
    fn operation(&self, canonical_input: &[u8]) -> Result<CommandOperation, CommandFailure> {
        Ok(match self.parse(canonical_input)?.kind {
            OperationKind::InsertFullRow => CommandOperation::Insert,
            OperationKind::UpdatePatch => CommandOperation::Update,
            OperationKind::DeleteByKey => CommandOperation::Delete,
            // `parse` refuses both before returning.
            OperationKind::Unspecified | OperationKind::TruncateAuthorizedTable => {
                return Err(CommandFailure::Validation)
            }
        })
    }

    fn footprint(&self, canonical_input: &[u8]) -> Result<CommandFootprint, CommandFailure> {
        let parsed = self.parse(canonical_input)?;
        Ok(CommandFootprint {
            rows: BTreeSet::from([AffectedKey {
                table_id: self.table_id.clone(),
                primary_key: parsed.primary_key,
            }]),
            table_ids: BTreeSet::from([self.table_id.clone()]),
        })
    }

    fn execute(
        &self,
        transaction: &mut Transaction<'_>,
        canonical_input: &[u8],
        context: CommandContext<'_>,
    ) -> Result<(), CommandFailure> {
        let parsed = self.parse(canonical_input)?;
        self.authorize(&parsed, context.authorized_scope)?;
        let statement = self.statement(&parsed);
        let parameters: Vec<&(dyn ToSql + Sync)> = statement
            .parameters
            .iter()
            .map(|parameter| parameter as &(dyn ToSql + Sync))
            .collect();
        let affected = transaction
            .execute(statement.sql.as_str(), &parameters)
            .map_err(classify_postgres_error)?;
        if affected == 0 && parsed.kind != OperationKind::InsertFullRow {
            return Err(CommandFailure::Conflict {
                kind: "row-missing".to_owned(),
                payload: Vec::new(),
            });
        }
        Ok(())
    }
}

/// The command of every table whose key the v1 key codec carries. A table it
/// cannot key is skipped rather than served without a command.
fn crud_commands(
    tables: &[RegisteredTable],
    operations: CrudOperations,
) -> impl Iterator<Item = CrudCommand> + '_ {
    tables
        .iter()
        .filter_map(move |table| CrudCommand::for_table(table, operations).ok())
}

/// Which operations each table's command admits.
///
/// `ByTable` narrows the surface per collection inside the command as well as
/// above it. A table the map does not name registers no command, so the map
/// fails closed rather than falling back to the whole surface.
#[derive(Debug, Clone)]
pub enum PerTableOperations {
    Uniform(CrudOperations),
    ByTable(BTreeMap<TableId, CrudOperations>),
}

impl PerTableOperations {
    /// The operations this table's command admits, or `None` when the
    /// deployment mounts no command for it.
    pub(crate) fn for_table(&self, table_id: &TableId) -> Option<CrudOperations> {
        match self {
            Self::Uniform(operations) => Some(*operations),
            Self::ByTable(by_table) => by_table.get(table_id).copied(),
        }
    }
}

/// Registers `crud:<table>` for every table the deployment mounts one for,
/// admitting exactly the operations that table is mounted with.
///
/// This is the second enforcement point after the grant's narrowed
/// capability: a collection mounted for `update` registers a command that
/// refuses a `DELETE` even if every gate above it were removed.
pub fn register_crud_commands_per_table(
    registry: &mut mutation_api::CommandRegistry,
    tables: &[RegisteredTable],
    operations: &PerTableOperations,
) -> Result<(), mutation_api::MutationBatchError> {
    for command in crud_commands_per_table(tables, operations) {
        registry.register(command)?;
    }
    Ok(())
}

fn crud_commands_per_table<'a>(
    tables: &'a [RegisteredTable],
    operations: &'a PerTableOperations,
) -> impl Iterator<Item = CrudCommand> + 'a {
    tables.iter().filter_map(move |table| {
        CrudCommand::for_table(table, operations.for_table(&table.table_id)?).ok()
    })
}

/// What the commands [`register_crud_commands_per_table`] registers under
/// [`PerTableOperations::Uniform`] let a client do, without a registry and
/// without a database connection.
pub fn crud_command_summaries(
    tables: &[RegisteredTable],
    operations: CrudOperations,
) -> Vec<CrudCommandSummary> {
    crud_commands(tables, operations)
        .map(|command| command.summary())
        .collect()
}

/// [`crud_command_summaries`] for a deployment that mounts a different
/// operation set per table. Same walk, same order, so what a summary declares
/// and what [`register_crud_commands_per_table`] registers cannot drift.
pub fn crud_command_summaries_per_table(
    tables: &[RegisteredTable],
    operations: &PerTableOperations,
) -> Vec<CrudCommandSummary> {
    crud_commands_per_table(tables, operations)
        .map(|command| command.summary())
        .collect()
}

/// What one registered command lets a client do; the input to a generated
/// schema's `commands` entry.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CrudCommandSummary {
    pub table_id: TableId,
    pub name: String,
    pub writable_columns: Vec<String>,
    /// The operations a generated facade may offer, from
    /// [`admitted_operations`].
    pub operations: Vec<CrudOperationName>,
}

/// One operation name as a generated collection command spells it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CrudOperationName {
    Insert,
    Update,
    Delete,
}

impl CrudOperationName {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Insert => "insert",
            Self::Update => "update",
            Self::Delete => "delete",
        }
    }
}

struct ParsedOperation {
    kind: OperationKind,
    primary_key: Vec<u8>,
    key: Vec<BoundCell>,
    columns: Vec<BoundCell>,
    written: BTreeSet<u32>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct BoundCell {
    column_id: u32,
    text: Option<String>,
}

#[derive(Clone, Copy)]
enum ColumnRule<'a> {
    Patch,
    FullRow(&'a [BoundCell]),
}

struct Statement {
    sql: String,
    parameters: Vec<Option<String>>,
}

/// The value kind one wire cell carries, or `None` when it carries none.
fn cell_kind(cell: &ColumnValue) -> Option<&Kind> {
    cell.value.as_ref().and_then(|value| value.kind.as_ref())
}

fn sql_type(column: &ColumnInspection) -> String {
    format!(
        "{}.{}",
        quote_identifier(&column.type_schema),
        quote_identifier(&column.type_name)
    )
}

/// The text PostgreSQL parses for one value cell of one column, or the
/// validation failure when the wire kind is not the column's.
fn cell_text(column: &CrudColumn, kind: &Kind) -> Result<Option<String>, CommandFailure> {
    if let Kind::NullValue(_) = kind {
        return if column.nullable {
            Ok(None)
        } else {
            Err(CommandFailure::Validation)
        };
    }
    let codec = column.codec.ok_or(CommandFailure::Validation)?;
    let text = match (codec, kind) {
        (PostgresTypeCodec::Bool, Kind::BoolValue(value)) => bool_text(*value),
        (PostgresTypeCodec::Int2, Kind::Int64Value(value)) => {
            i16::try_from(*value).map_err(|_| CommandFailure::Validation)?;
            value.to_string()
        }
        (PostgresTypeCodec::Int4, Kind::Int64Value(value)) => {
            i32::try_from(*value).map_err(|_| CommandFailure::Validation)?;
            value.to_string()
        }
        (PostgresTypeCodec::Int8, Kind::Int64Value(value)) => value.to_string(),
        (PostgresTypeCodec::Float4 | PostgresTypeCodec::Float8, Kind::Float64Value(value)) => {
            float_text(*value)
        }
        (PostgresTypeCodec::Numeric, Kind::DecimalText(value)) => sql_text(value)?,
        (
            PostgresTypeCodec::Text | PostgresTypeCodec::Varchar | PostgresTypeCodec::Bpchar,
            Kind::TextValue(value),
        ) => sql_text(value)?,
        (PostgresTypeCodec::Bytea, Kind::BlobValue(value)) => bytea_text(value),
        (PostgresTypeCodec::Uuid, Kind::Uuid16(value)) => uuid_text(value)?,
        (PostgresTypeCodec::Json | PostgresTypeCodec::Jsonb, Kind::JsonText(value)) => {
            sql_text(value)?
        }
        (PostgresTypeCodec::Date, Kind::DateIso8601(value)) => sql_text(value)?,
        (PostgresTypeCodec::Time, Kind::TimeIso8601(value)) => sql_text(value)?,
        (
            PostgresTypeCodec::Timestamp | PostgresTypeCodec::Timestamptz,
            Kind::TimestampUnixMicros(micros),
        ) => timestamp_text(*micros)?,
        _ => return Err(CommandFailure::Validation),
    };
    Ok(Some(text))
}

/// A key part in the v1 key grammar: integers, booleans, text, bytes and
/// UUIDs, never null.
fn key_part_text(column: &CrudColumn, kind: &Kind) -> Result<String, CommandFailure> {
    let codec = column.codec.ok_or(CommandFailure::Validation)?;
    match (codec, kind) {
        (PostgresTypeCodec::Bool, Kind::BoolValue(_))
        | (
            PostgresTypeCodec::Int2 | PostgresTypeCodec::Int4 | PostgresTypeCodec::Int8,
            Kind::Int64Value(_),
        )
        | (PostgresTypeCodec::Text | PostgresTypeCodec::Varchar, Kind::TextValue(_))
        | (PostgresTypeCodec::Bytea, Kind::BlobValue(_))
        | (PostgresTypeCodec::Uuid, Kind::Uuid16(_)) => {
            cell_text(column, kind)?.ok_or(CommandFailure::Validation)
        }
        _ => Err(CommandFailure::Validation),
    }
}

fn bool_text(value: bool) -> String {
    if value { "true" } else { "false" }.to_owned()
}

fn float_text(value: f64) -> String {
    if value.is_nan() {
        "NaN".to_owned()
    } else if value.is_infinite() {
        if value.is_sign_negative() {
            "-Infinity"
        } else {
            "Infinity"
        }
        .to_owned()
    } else {
        format!("{value:?}")
    }
}

/// PostgreSQL text never carries NUL; refusing it here keeps the failure a
/// validation rather than a retryable server error.
fn sql_text(value: &str) -> Result<String, CommandFailure> {
    if value.contains('\0') {
        return Err(CommandFailure::Validation);
    }
    Ok(value.to_owned())
}

fn bytea_text(value: &[u8]) -> String {
    let mut text = String::with_capacity(2 + value.len() * 2);
    text.push_str("\\x");
    for byte in value {
        let _ = write!(text, "{byte:02x}");
    }
    text
}

fn uuid_text(value: &[u8]) -> Result<String, CommandFailure> {
    if value.len() != 16 {
        return Err(CommandFailure::Validation);
    }
    let mut text = String::with_capacity(36);
    for (index, byte) in value.iter().enumerate() {
        if matches!(index, 4 | 6 | 8 | 10) {
            text.push('-');
        }
        let _ = write!(text, "{byte:02x}");
    }
    Ok(text)
}

fn timestamp_text(micros: i64) -> Result<String, CommandFailure> {
    OffsetDateTime::from_unix_timestamp_nanos(i128::from(micros) * 1_000)
        .map_err(|_| CommandFailure::Validation)?
        .format(&Rfc3339)
        .map_err(|_| CommandFailure::Validation)
}

/// SQLSTATE classes decide the outcome: integrity violations are conflicts
/// the application can act on, data exceptions are the input's fault, a
/// privilege refusal is authorization, and everything else is worth a retry.
fn classify_postgres_error(error: postgres::Error) -> CommandFailure {
    let Some(state) = error.code() else {
        return CommandFailure::Retryable;
    };
    let code = state.code();
    if code.starts_with("23") {
        let constraint = error
            .as_db_error()
            .and_then(|db| db.constraint())
            .unwrap_or_default();
        return CommandFailure::Conflict {
            kind: "constraint".to_owned(),
            payload: constraint.as_bytes().to_vec(),
        };
    }
    if code.starts_with("22") {
        return CommandFailure::Validation;
    }
    if code == "42501" {
        return CommandFailure::Authorization;
    }
    CommandFailure::Retryable
}

#[cfg(test)]
mod tests {
    use authz::{AuthorizationPolicy, PolicyDefinition, TablePermitDefinition};
    use source_control::{QualifiedTable, TableInspection};
    use sync_protocol::{
        change::{CellValue, Delete, FullRow, PrimaryKey, RowPatch},
        encode_primary_key,
    };

    use super::*;

    fn column(
        attribute_number: i16,
        name: &str,
        type_name: &str,
        nullable: bool,
        key: Option<u32>,
        generated: bool,
    ) -> ColumnInspection {
        ColumnInspection {
            attribute_number,
            name: name.to_owned(),
            type_oid: 0,
            type_modifier: -1,
            type_schema: "pg_catalog".to_owned(),
            type_name: type_name.to_owned(),
            nullable,
            primary_key_ordinal: key,
            generated,
            nondeterministic_collation: false,
        }
    }

    fn table() -> RegisteredTable {
        RegisteredTable {
            table_id: TableId::new("public.tasks").unwrap(),
            column_ids: (1..=5).map(ColumnId::new).collect(),
            inspection: TableInspection {
                table: QualifiedTable {
                    schema: "public".to_owned(),
                    table: "tasks".to_owned(),
                },
                relation_oid: 16_384,
                in_publication: true,
                relation_kind: 'r',
                persistence: 'p',
                replica_identity: 'd',
                is_partition: false,
                has_publication_row_filter: false,
                has_publication_column_list: false,
                columns: vec![
                    column(1, "id", "int8", false, Some(1), false),
                    column(2, "title", "text", false, None, false),
                    column(3, "done", "bool", false, None, false),
                    column(4, "due", "timestamptz", true, None, false),
                    column(5, "search", "text", true, None, true),
                ],
            },
        }
    }

    fn command() -> CrudCommand {
        CrudCommand::for_table(&table(), CrudOperations::ALL).unwrap()
    }

    fn cell(column_id: u32, kind: Kind) -> ColumnValue {
        ColumnValue {
            column_id,
            value: Some(CellValue { kind: Some(kind) }),
        }
    }

    fn key(id: i64) -> Vec<u8> {
        encode_primary_key(&PrimaryKey {
            codec_version: 1,
            parts: vec![cell(1, Kind::Int64Value(id))],
        })
        .unwrap()
    }

    fn operation(kind: OperationKind, primary_key: Vec<u8>, payload: Payload) -> Vec<u8> {
        Operation {
            table_id: "public.tasks".to_owned(),
            kind: kind as i32,
            primary_key,
            payload: Some(payload),
        }
        .encode_to_vec()
    }

    fn patch(columns: Vec<ColumnValue>) -> Vec<u8> {
        operation(
            OperationKind::UpdatePatch,
            key(7),
            Payload::Update(RowPatch {
                changed_columns: columns,
            }),
        )
    }

    fn scope(permitted: Vec<u32>) -> AuthorizedScope {
        AuthorizationPolicy::compile(
            PolicyDefinition {
                generation: 1,
                tables: vec![TablePermitDefinition {
                    table_id: TableId::new("public.tasks").unwrap(),
                    permitted_columns: permitted.into_iter().map(ColumnId::new).collect(),
                }],
            },
            &[table()],
        )
        .unwrap()
        .scope()
    }

    #[test]
    fn derives_the_command_from_the_registered_table() {
        let command = command();
        assert_eq!(command.name(), "crud:public.tasks");
        assert_eq!(command.version(), 1);
        assert_eq!(
            command.writable_columns().collect::<Vec<_>>(),
            vec!["title", "done", "due"]
        );
        // A generated column can never be carried, so no full row can.
        assert!(!command
            .summary()
            .operations
            .contains(&CrudOperationName::Insert));
    }

    #[test]
    fn footprint_is_the_operation_row_and_table() {
        let footprint = command()
            .footprint(&patch(vec![cell(2, Kind::TextValue("ship".to_owned()))]))
            .unwrap();
        assert_eq!(
            footprint.rows,
            BTreeSet::from([AffectedKey {
                table_id: TableId::new("public.tasks").unwrap(),
                primary_key: key(7),
            }])
        );
        assert_eq!(
            footprint.table_ids,
            BTreeSet::from([TableId::new("public.tasks").unwrap()])
        );
    }

    #[test]
    fn refuses_non_canonical_bytes_and_foreign_tables_and_empty_patches() {
        let command = command();
        let mut padded = patch(vec![cell(2, Kind::TextValue("x".to_owned()))]);
        // An unknown field survives decode but not re-encode equality.
        padded.extend_from_slice(&[0xf8, 0x7f, 0x01]);
        assert_eq!(command.footprint(&padded), Err(CommandFailure::Validation));
        let mut foreign = Operation::decode(patch(vec![]).as_slice()).unwrap();
        foreign.table_id = "public.other".to_owned();
        assert_eq!(
            command.footprint(&foreign.encode_to_vec()),
            Err(CommandFailure::Validation)
        );
        assert_eq!(
            command.footprint(&patch(vec![])),
            Err(CommandFailure::Validation)
        );
        let truncate = Operation {
            table_id: "public.tasks".to_owned(),
            kind: OperationKind::TruncateAuthorizedTable as i32,
            primary_key: key(1),
            payload: Some(Payload::Truncate(sync_protocol::change::Truncate {
                reason: String::new(),
            })),
        }
        .encode_to_vec();
        assert_eq!(
            command.footprint(&truncate),
            Err(CommandFailure::Validation)
        );
    }

    #[test]
    fn a_patch_may_not_name_the_key_or_a_column_the_command_does_not_write() {
        let command = command();
        assert_eq!(
            command.footprint(&patch(vec![cell(1, Kind::Int64Value(9))])),
            Err(CommandFailure::Validation)
        );
        assert_eq!(
            command.footprint(&patch(vec![cell(5, Kind::TextValue("x".to_owned()))])),
            Err(CommandFailure::Authorization)
        );
        assert_eq!(
            command.footprint(&patch(vec![cell(9, Kind::TextValue("x".to_owned()))])),
            Err(CommandFailure::Validation)
        );
        assert_eq!(
            command.footprint(&patch(vec![
                cell(2, Kind::TextValue("a".to_owned())),
                cell(2, Kind::TextValue("b".to_owned())),
            ])),
            Err(CommandFailure::Validation)
        );
    }

    #[test]
    fn values_must_match_the_column_type_and_nullability() {
        let command = command();
        assert_eq!(
            command.footprint(&patch(vec![cell(2, Kind::Int64Value(1))])),
            Err(CommandFailure::Validation)
        );
        assert_eq!(
            command.footprint(&patch(vec![cell(2, Kind::NullValue(0))])),
            Err(CommandFailure::Validation)
        );
        assert!(command
            .footprint(&patch(vec![cell(4, Kind::NullValue(0))]))
            .is_ok());
        assert!(command
            .footprint(&patch(vec![cell(
                4,
                Kind::TimestampUnixMicros(1_700_000_000_250_000)
            )]))
            .is_ok());
        let text_key = encode_primary_key(&PrimaryKey {
            codec_version: 1,
            parts: vec![cell(1, Kind::BlobValue(b"7".to_vec()))],
        })
        .unwrap();
        assert_eq!(
            command.footprint(&operation(
                OperationKind::DeleteByKey,
                text_key,
                Payload::Delete(Delete {})
            )),
            Err(CommandFailure::Validation)
        );
    }

    /// A `text` key part binds only from `text_value`, and never with a NUL
    /// PostgreSQL text cannot hold; `bpchar` keys are refused at registration.
    #[test]
    fn a_text_key_part_binds_only_from_text() {
        let mut keyed = table();
        keyed.inspection.columns[1].primary_key_ordinal = Some(2);
        let command = CrudCommand::for_table(&keyed, CrudOperations::ALL).unwrap();
        let delete = |title: Kind| {
            let key = encode_primary_key(&PrimaryKey {
                codec_version: 1,
                parts: vec![cell(1, Kind::Int64Value(7)), cell(2, title)],
            })
            .unwrap();
            command.footprint(&operation(
                OperationKind::DeleteByKey,
                key,
                Payload::Delete(Delete {}),
            ))
        };
        assert!(delete(Kind::TextValue("é-\u{1d11e}".to_owned())).is_ok());
        assert!(delete(Kind::TextValue(String::new())).is_ok());
        assert_eq!(
            delete(Kind::BlobValue(b"x".to_vec())),
            Err(CommandFailure::Validation)
        );
        assert_eq!(
            delete(Kind::TextValue("a\0b".to_owned())),
            Err(CommandFailure::Validation)
        );

        keyed.inspection.columns[1].type_name = "bpchar".to_owned();
        assert!(matches!(
            CrudCommand::for_table(&keyed, CrudOperations::ALL),
            Err(CrudCommandError::UnsupportedKeyType { column }) if column == "title"
        ));
    }

    /// The capability gate asks the command which operation an input performs,
    /// and it must answer from the same parse the execution uses. An answer
    /// that drifted from the SQL the command then runs would let a grant that
    /// authorizes `update` reach a `DELETE`.
    #[test]
    fn the_capability_operation_is_the_one_the_statement_performs() {
        let mut inspection = table();
        inspection.inspection.columns.pop();
        inspection.column_ids.pop();
        let command = CrudCommand::for_table(&inspection, CrudOperations::ALL).unwrap();

        let update = patch(vec![cell(3, Kind::BoolValue(true))]);
        assert_eq!(command.operation(&update), Ok(CommandOperation::Update));
        assert!(command
            .statement(&command.parse(&update).unwrap())
            .sql
            .starts_with("UPDATE "));

        let delete = operation(
            OperationKind::DeleteByKey,
            key(7),
            Payload::Delete(Delete {}),
        );
        assert_eq!(command.operation(&delete), Ok(CommandOperation::Delete));
        assert!(command
            .statement(&command.parse(&delete).unwrap())
            .sql
            .starts_with("DELETE "));

        let insert = operation(
            OperationKind::InsertFullRow,
            key(7),
            Payload::Insert(FullRow {
                columns: vec![
                    cell(1, Kind::Int64Value(7)),
                    cell(2, Kind::TextValue("t".to_owned())),
                    cell(3, Kind::BoolValue(false)),
                    cell(4, Kind::NullValue(0)),
                ],
            }),
        );
        assert_eq!(command.operation(&insert), Ok(CommandOperation::Insert));
        assert!(command
            .statement(&command.parse(&insert).unwrap())
            .sql
            .starts_with("INSERT "));

        // An input the command cannot parse names no operation, so the gate
        // above it refuses rather than defaulting to one.
        assert_eq!(
            command.operation(b"not-an-operation"),
            Err(CommandFailure::Validation)
        );
        assert_eq!(
            command.operation(&operation(
                OperationKind::TruncateAuthorizedTable,
                key(7),
                Payload::Delete(Delete {})
            )),
            Err(CommandFailure::Validation)
        );
    }

    #[test]
    fn an_operation_the_command_does_not_admit_is_unauthorized() {
        let command = CrudCommand::for_table(
            &table(),
            CrudOperations {
                insert: false,
                update: true,
                delete: false,
            },
        )
        .unwrap();
        assert_eq!(
            command.footprint(&operation(
                OperationKind::DeleteByKey,
                key(1),
                Payload::Delete(Delete {})
            )),
            Err(CommandFailure::Authorization)
        );
    }

    #[test]
    fn the_scope_must_permit_the_table_and_every_touched_column() {
        let command = command();
        let parsed = command
            .parse(&patch(vec![cell(3, Kind::BoolValue(true))]))
            .unwrap();
        assert!(command.authorize(&parsed, &scope(vec![1, 2, 3])).is_ok());
        // Column 3 is written but not permitted. (A scope always permits the
        // key: the policy compiler refuses one that does not.)
        assert_eq!(
            command.authorize(&parsed, &scope(vec![1, 2])),
            Err(CommandFailure::Authorization)
        );
        // A scope that permits another table, and not this one, refuses
        // before any column is looked at.
        let mut other = table();
        other.table_id = TableId::new("public.other").unwrap();
        other.inspection.table.table = "other".to_owned();
        other.inspection.relation_oid += 1;
        let no_table = AuthorizationPolicy::compile(
            PolicyDefinition {
                generation: 1,
                tables: vec![TablePermitDefinition {
                    table_id: other.table_id.clone(),
                    permitted_columns: (1..=5).map(ColumnId::new).collect(),
                }],
            },
            &[table(), other],
        )
        .unwrap()
        .scope();
        assert_eq!(
            command.authorize(&parsed, &no_table),
            Err(CommandFailure::Authorization)
        );
    }

    /// `public.issues (tenant_id int8, id int8, title text)` keyed by
    /// `(tenant_id, id)` and partitioned on `tenant_id`.
    fn issues() -> RegisteredTable {
        let mut issues = table();
        issues.table_id = TableId::new("public.issues").unwrap();
        issues.column_ids = (1..=3).map(ColumnId::new).collect();
        issues.inspection.table.table = "issues".to_owned();
        issues.inspection.columns = vec![
            column(1, "tenant_id", "int8", false, Some(1), false),
            column(2, "id", "int8", false, Some(2), false),
            column(3, "title", "text", false, None, false),
        ];
        issues
    }

    fn tenant_scope(tenant: i64) -> AuthorizedScope {
        let table_id = TableId::new("public.issues").unwrap();
        AuthorizationPolicy::compile_partitioned_detached(
            PolicyDefinition {
                generation: 1,
                tables: vec![TablePermitDefinition {
                    table_id: table_id.clone(),
                    permitted_columns: (1..=3).map(ColumnId::new).collect(),
                }],
            },
            authz::PartitionDefinition {
                attribute: "tenant_id".to_owned(),
                tables: [(table_id, ColumnId::new(1))].into(),
            },
            &[issues()],
        )
        .unwrap()
        .partition_scope(authz::PartitionValue::Int(tenant))
        .unwrap()
    }

    #[test]
    fn a_write_to_a_partitioned_table_names_a_row_of_the_grants_tenant() {
        let command = CrudCommand::for_table(&issues(), CrudOperations::ALL).unwrap();
        let issue_key = |tenant: i64| {
            encode_primary_key(&PrimaryKey {
                codec_version: 1,
                parts: vec![
                    cell(1, Kind::Int64Value(tenant)),
                    cell(2, Kind::Int64Value(1)),
                ],
            })
            .unwrap()
        };
        let issue = |kind: OperationKind, tenant: i64, payload: Payload| {
            Operation {
                table_id: "public.issues".to_owned(),
                kind: kind as i32,
                primary_key: issue_key(tenant),
                payload: Some(payload),
            }
            .encode_to_vec()
        };
        // Tenants 7 and 8 each have an issue 1: only the leading key part
        // tells them apart.
        for tenant in [7, 8] {
            let writes = [
                issue(
                    OperationKind::InsertFullRow,
                    tenant,
                    Payload::Insert(FullRow {
                        columns: vec![
                            cell(1, Kind::Int64Value(tenant)),
                            cell(2, Kind::Int64Value(1)),
                            cell(3, Kind::TextValue("ship".to_owned())),
                        ],
                    }),
                ),
                issue(
                    OperationKind::UpdatePatch,
                    tenant,
                    Payload::Update(RowPatch {
                        changed_columns: vec![cell(3, Kind::TextValue("ship".to_owned()))],
                    }),
                ),
                issue(
                    OperationKind::DeleteByKey,
                    tenant,
                    Payload::Delete(Delete {}),
                ),
            ];
            for write in writes {
                let parsed = command.parse(&write).unwrap();
                let verdict = |scope| command.authorize(&parsed, &scope);
                assert_eq!(verdict(tenant_scope(tenant)), Ok(()));
                assert_eq!(
                    verdict(tenant_scope(15 - tenant)),
                    Err(CommandFailure::Authorization)
                );
            }
        }
        // A patch cannot move a row to another tenant: the key is not
        // writable.
        let move_tenant = issue(
            OperationKind::UpdatePatch,
            7,
            Payload::Update(RowPatch {
                changed_columns: vec![cell(1, Kind::Int64Value(8))],
            }),
        );
        assert_eq!(
            command.parse(&move_tenant).err(),
            Some(CommandFailure::Validation)
        );
    }

    #[test]
    fn renders_parameterized_statements_with_typed_casts() {
        let command = command();
        let parsed = command
            .parse(&patch(vec![
                cell(2, Kind::TextValue("ship".to_owned())),
                cell(4, Kind::NullValue(0)),
            ]))
            .unwrap();
        let statement = command.statement(&parsed);
        assert_eq!(
            statement.sql,
            "UPDATE \"public\".\"tasks\" SET \"title\" = ($1::text)::\"pg_catalog\".\"text\", \
             \"due\" = ($2::text)::\"pg_catalog\".\"timestamptz\" \
             WHERE \"id\" = ($3::text)::\"pg_catalog\".\"int8\""
        );
        assert_eq!(
            statement.parameters,
            vec![Some("ship".to_owned()), None, Some("7".to_owned())]
        );
        let delete = command
            .parse(&operation(
                OperationKind::DeleteByKey,
                key(7),
                Payload::Delete(Delete {}),
            ))
            .unwrap();
        assert_eq!(
            command.statement(&delete).sql,
            "DELETE FROM \"public\".\"tasks\" WHERE \"id\" = ($1::text)::\"pg_catalog\".\"int8\""
        );
    }

    #[test]
    fn a_full_row_carries_every_column_with_the_key_matching_the_key_parts() {
        let mut inspection = table();
        inspection.inspection.columns.pop();
        inspection.column_ids.pop();
        let command = CrudCommand::for_table(&inspection, CrudOperations::ALL).unwrap();
        assert!(command
            .summary()
            .operations
            .contains(&CrudOperationName::Insert));
        let row = |columns: Vec<ColumnValue>| {
            operation(
                OperationKind::InsertFullRow,
                key(7),
                Payload::Insert(FullRow { columns }),
            )
        };
        let full = row(vec![
            cell(1, Kind::Int64Value(7)),
            cell(2, Kind::TextValue("t".to_owned())),
            cell(3, Kind::BoolValue(false)),
            cell(4, Kind::NullValue(0)),
        ]);
        let parsed = command.parse(&full).unwrap();
        assert_eq!(
            command.statement(&parsed).sql,
            "INSERT INTO \"public\".\"tasks\" (\"id\", \"title\", \"done\", \"due\") VALUES \
             (($1::text)::\"pg_catalog\".\"int8\", ($2::text)::\"pg_catalog\".\"text\", \
             ($3::text)::\"pg_catalog\".\"bool\", ($4::text)::\"pg_catalog\".\"timestamptz\")"
        );
        assert_eq!(
            command.footprint(&row(vec![
                cell(1, Kind::Int64Value(8)),
                cell(2, Kind::TextValue("t".to_owned())),
                cell(3, Kind::BoolValue(false)),
                cell(4, Kind::NullValue(0)),
            ])),
            Err(CommandFailure::Validation)
        );
        assert_eq!(
            command.footprint(&row(vec![
                cell(1, Kind::Int64Value(7)),
                cell(2, Kind::TextValue("t".to_owned())),
            ])),
            Err(CommandFailure::Validation)
        );
    }

    #[test]
    fn renders_literals_the_way_postgres_reads_them() {
        assert_eq!(bytea_text(&[0x00, 0xab]), "\\x00ab");
        assert_eq!(
            uuid_text(&[
                0x12, 0x34, 0x56, 0x78, 0x9a, 0xbc, 0xde, 0xf0, 0x12, 0x34, 0x56, 0x78, 0x9a, 0xbc,
                0xde, 0xf0
            ])
            .unwrap(),
            "12345678-9abc-def0-1234-56789abcdef0"
        );
        assert_eq!(
            timestamp_text(1_700_000_000_250_000).unwrap(),
            "2023-11-14T22:13:20.25Z"
        );
        assert_eq!(float_text(0.1), "0.1");
        assert_eq!(float_text(f64::INFINITY), "Infinity");
        assert_eq!(quote_identifier("we\"ird"), "\"we\"\"ird\"");
    }
}
