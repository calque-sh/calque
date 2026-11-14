//! Bounded, store-specific artifact encoder for the browser direct-columnar engine.
//!
//! Rows are written once into private per-column spools. Finishing assembles the
//! immutable artifact without retaining a second row image or a whole encoded
//! artifact in memory.
//!
//! `MAGIC` tags the container and never moves; [`FORMAT_VERSION`] in the JSON
//! header is the artifact's format version, and a decoder dispatches on it.
//!
//! # Dictionary ordering
//!
//! Every [`ColumnKind::Dict`] column writes its dictionary entries in ascending
//! order of their UTF-8 bytes (unsigned `[u8]` lexicographic order, not a
//! locale collation). Dictionary code order is therefore value order, so a
//! decoder sorts a dictionary column by sorting its packed codes and never
//! touches the dictionary bytes.
//!
//! # Variable-width columns
//!
//! A utf8, bytes, json, decimal, date, time or interval column carries an
//! optional validity bitmap, a lengths part and a values part. The lengths part
//! holds `rows + 1` little-endian `u32`s: a 0, then each row's value length in
//! bytes, a null row's being 0. A decoder rebuilds the offsets with one prefix
//! sum and must refuse the column when that sum passes the values part's length
//! at any row, or ends short of it. Lengths compress where offsets do not: a
//! column of similar values repeats a few lengths, while its offsets never
//! repeat. A dictionary's offsets part stays cumulative.
//!
//! # Primary keys
//!
//! A table names its primary-key columns and carries no index over them. A
//! receiver builds its own index after every digest matches, and must refuse a
//! table in which two rows share a key: the encoder refuses to write one, so
//! such a table is damaged or forged.

pub mod pack;

// The packer's unit tests share `tests/support` with the integration tests,
// which name this crate from outside.
#[cfg(test)]
extern crate self as direct_columnar;

use std::{
    collections::{HashMap, HashSet},
    fs::File,
    io::{self, BufReader, BufWriter, Read, Write},
    str::FromStr,
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc,
    },
};

use bigdecimal::BigDecimal;
use chrono::{NaiveDate, NaiveTime};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use tempfile::NamedTempFile;
use thiserror::Error;

/// Container tag for the framing (magic, header length, JSON header,
/// 8-byte-aligned payload), independent of [`FORMAT_VERSION`].
const MAGIC: &[u8; 8] = b"CALQART1";
const FORMAT_NAME: &str = "calque-direct-columnar";

/// Artifact format version, written into the header and mirrored by the
/// manifest's `decoded.formatVersion`.
pub const FORMAT_VERSION: u32 = 4;

/// The peak browser memory of the primary-key indexes the engine worker builds
/// for tables with these row counts, which the artifact's bytes do not include.
/// It mirrors the client's `keyIndexBytes`: every table's four-byte slots, the
/// power of two at or above twice its rows, and the largest table's four-byte
/// row hashes, since the worker builds one table at a time.
pub fn browser_key_index_bytes(table_rows: impl IntoIterator<Item = u64>) -> u64 {
    let mut slots = 0_u64;
    let mut hashes = 0_u64;
    for rows in table_rows {
        let capacity = rows
            .saturating_mul(2)
            .max(1)
            .checked_next_power_of_two()
            .unwrap_or(u64::MAX);
        slots = slots.saturating_add(capacity.saturating_mul(4));
        hashes = hashes.max(rows.saturating_mul(4));
    }
    slots.saturating_add(hashes)
}

const NUMERIC_BLOCK_ROWS: usize = 1_024;
const NUMERIC_BLOCK_HEADER_BYTES: usize = 16;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "lowercase")]
pub enum ColumnKind {
    Int64,
    Uint64,
    F64,
    Timestamp,
    Bool,
    Utf8,
    Bytes,
    Uuid,
    Json,
    Decimal,
    Date,
    Time,
    Interval,
    Dict,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ColumnSchema {
    pub name: String,
    #[serde(rename = "type")]
    pub kind: ColumnKind,
    pub nullable: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct TableSchema {
    pub name: String,
    pub primary_key: Vec<String>,
    pub columns: Vec<ColumnSchema>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct WorkspaceSchema {
    pub tables: Vec<TableSchema>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ArtifactIdentity {
    pub source_epoch: String,
    pub manifest_version: String,
    pub collection_set_hash: String,
    pub schema_hash: String,
    pub authorization_scope_version: String,
    /// The scope this artifact was built for. A version alone cannot separate
    /// two scopes published at the same authorization generation, as per-scope
    /// builds are.
    pub authorization_scope_hash: String,
    pub cursor: String,
}

#[derive(Debug, Clone, PartialEq)]
pub enum Value {
    Null,
    Int64(i64),
    Uint64(u64),
    F64(f64),
    TimestampMicros(i64),
    Bool(bool),
    Text(String),
    Bytes(Vec<u8>),
    Uuid([u8; 16]),
    DecimalText(String),
    DateText(String),
    TimeText(String),
    IntervalText(String),
    Json(Vec<u8>),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct EncoderLimits {
    pub max_tables: usize,
    pub max_columns_per_table: usize,
    pub max_rows: u64,
    pub max_variable_bytes_per_column: u64,
    pub max_header_bytes: u64,
    pub max_artifact_bytes: u64,
}

impl Default for EncoderLimits {
    fn default() -> Self {
        Self {
            max_tables: 256,
            max_columns_per_table: 1_024,
            max_rows: 10_000_000,
            max_variable_bytes_per_column: u64::from(u32::MAX),
            max_header_bytes: 1_048_576,
            max_artifact_bytes: 8 * 1024 * 1024 * 1024,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EncodeReport {
    pub bytes: u64,
    pub sha256: String,
    pub rows: u64,
}

pub struct Encoder {
    identity: ArtifactIdentity,
    tables: Vec<TableSpool>,
    table_indexes: HashMap<String, usize>,
    limits: EncoderLimits,
    rows: u64,
    poisoned: bool,
}

impl Encoder {
    pub fn new(
        identity: ArtifactIdentity,
        schema: WorkspaceSchema,
        limits: EncoderLimits,
    ) -> Result<Self, EncodeError> {
        Self::with_spool_memory(identity, schema, limits, SPOOL_MEMORY_BYTES)
    }

    fn with_spool_memory(
        identity: ArtifactIdentity,
        schema: WorkspaceSchema,
        limits: EncoderLimits,
        spool_memory_bytes: usize,
    ) -> Result<Self, EncodeError> {
        validate_identity(&identity)?;
        validate_schema(&schema, limits)?;
        if identity.schema_hash != schema_hash(&schema)? {
            return Err(EncodeError::SchemaIdentityMismatch);
        }
        let budget = SpoolBudget::new(spool_memory_bytes);
        let mut table_indexes = HashMap::with_capacity(schema.tables.len());
        let mut tables = Vec::with_capacity(schema.tables.len());
        for (index, table) in schema.tables.into_iter().enumerate() {
            table_indexes.insert(table.name.clone(), index);
            tables.push(TableSpool::new(table, &budget)?);
        }
        Ok(Self {
            identity,
            tables,
            table_indexes,
            limits,
            rows: 0,
            poisoned: false,
        })
    }

    /// Starts an encoder for an artifact whose cursor is not known until the
    /// last row is pushed. The identity's `cursor` must be empty;
    /// [`Encoder::bind_cursor`] supplies it, and [`Encoder::lay_out`] refuses
    /// an unbound cursor.
    pub fn new_awaiting_cursor(
        identity: ArtifactIdentity,
        schema: WorkspaceSchema,
        limits: EncoderLimits,
    ) -> Result<Self, EncodeError> {
        if !identity.cursor.is_empty() {
            return Err(EncodeError::CursorAlreadyBound);
        }
        let mut encoder = Self::new(
            ArtifactIdentity {
                cursor: "unbound".to_owned(),
                ..identity
            },
            schema,
            limits,
        )?;
        encoder.identity.cursor.clear();
        Ok(encoder)
    }

    /// Names the cursor the pushed rows are exact at. Allowed once, and only
    /// on an encoder started by [`Encoder::new_awaiting_cursor`].
    pub fn bind_cursor(&mut self, cursor: &str) -> Result<(), EncodeError> {
        if !self.identity.cursor.is_empty() {
            return Err(EncodeError::CursorAlreadyBound);
        }
        if !valid_name(cursor) {
            return Err(EncodeError::InvalidIdentity);
        }
        cursor.clone_into(&mut self.identity.cursor);
        Ok(())
    }

    /// Rows pushed into `table` so far.
    pub fn table_rows(&self, table: &str) -> Option<u64> {
        self.table_indexes
            .get(table)
            .map(|&index| u64::from(self.tables[index].rows))
    }

    pub fn push_row(&mut self, table: &str, values: &[Value]) -> Result<(), EncodeError> {
        if self.poisoned {
            return Err(EncodeError::Poisoned);
        }
        let Some(&index) = self.table_indexes.get(table) else {
            return Err(EncodeError::UnknownTable(table.to_owned()));
        };
        if self.rows >= self.limits.max_rows {
            return Err(EncodeError::RowLimit(self.limits.max_rows));
        }
        let result = self.tables[index].push_row(values, self.limits.max_variable_bytes_per_column);
        match result {
            Ok(()) => {
                self.rows += 1;
                Ok(())
            }
            Err(error) => {
                self.poisoned = true;
                Err(error)
            }
        }
    }

    pub fn finish<W: Write>(self, output: W) -> Result<EncodeReport, EncodeError> {
        self.lay_out()?.write(output)
    }

    /// Finishes every column and fixes the artifact's layout without writing
    /// it, so a caller knows the artifact's exact length before its first
    /// byte.
    pub fn lay_out(mut self) -> Result<LaidOutArtifact, EncodeError> {
        if self.poisoned {
            return Err(EncodeError::Poisoned);
        }
        if self.identity.cursor.is_empty() {
            return Err(EncodeError::CursorUnbound);
        }
        let mut finished = Vec::with_capacity(self.tables.len());
        for table in self.tables.drain(..) {
            finished.push(table.finish_columns()?);
        }

        let mut payload_bytes = 0_u64;
        for table in &mut finished {
            for column in &mut table.columns {
                for part in &mut column.parts {
                    payload_bytes = align8(payload_bytes)?;
                    part.offset = payload_bytes;
                    payload_bytes = payload_bytes
                        .checked_add(part.length)
                        .ok_or(EncodeError::SizeOverflow)?;
                }
            }
        }

        let mut table_headers = Vec::with_capacity(finished.len());
        for table in &finished {
            let mut column_headers = Vec::with_capacity(table.columns.len());
            for column in &table.columns {
                let mut parts = Vec::with_capacity(column.parts.len());
                for part in &column.parts {
                    parts.push(Part {
                        offset: part.offset,
                        length: part.length,
                    });
                }
                column_headers.push(ColumnHeader {
                    name: &column.schema.name,
                    kind: column.schema.kind,
                    nullable: column.schema.nullable,
                    dictionary_count: column.dictionary_count,
                    bit_width: column.bit_width,
                    encoding: column.encoding,
                    parts,
                });
            }
            table_headers.push(TableHeader {
                name: &table.schema.name,
                rows: table.rows,
                primary_key: &table.schema.primary_key,
                columns: column_headers,
            });
        }
        let header = serde_json::to_vec(&Header {
            format: FORMAT_NAME,
            version: FORMAT_VERSION,
            identity: &self.identity,
            tables: table_headers,
        })?;
        if header.is_empty() || header.len() as u64 > self.limits.max_header_bytes {
            return Err(EncodeError::HeaderLimit(self.limits.max_header_bytes));
        }
        let header_length = u32::try_from(header.len()).map_err(|_| EncodeError::SizeOverflow)?;
        let payload_start = align8(
            u64::try_from(MAGIC.len() + 4 + header.len()).map_err(|_| EncodeError::SizeOverflow)?,
        )?;
        let expected_bytes = payload_start
            .checked_add(payload_bytes)
            .ok_or(EncodeError::SizeOverflow)?;
        if expected_bytes > self.limits.max_artifact_bytes {
            return Err(EncodeError::ArtifactLimit(self.limits.max_artifact_bytes));
        }
        Ok(LaidOutArtifact {
            tables: finished,
            header,
            header_length,
            payload_start,
            expected_bytes,
            rows: self.rows,
            max_artifact_bytes: self.limits.max_artifact_bytes,
        })
    }
}

/// An artifact whose columns are finished and whose layout is fixed, from
/// [`Encoder::lay_out`]. [`LaidOutArtifact::write`] writes exactly
/// [`LaidOutArtifact::bytes`] bytes or fails.
pub struct LaidOutArtifact {
    tables: Vec<FinishedColumns>,
    header: Vec<u8>,
    header_length: u32,
    payload_start: u64,
    expected_bytes: u64,
    rows: u64,
    max_artifact_bytes: u64,
}

impl LaidOutArtifact {
    /// The artifact's length in bytes.
    pub fn bytes(&self) -> u64 {
        self.expected_bytes
    }

    pub fn write<W: Write>(mut self, output: W) -> Result<EncodeReport, EncodeError> {
        let payload_start = self.payload_start;
        let mut output = HashingWriter::new(output, self.max_artifact_bytes);
        output.write_all(MAGIC)?;
        output.write_all(&self.header_length.to_le_bytes())?;
        output.write_all(&self.header)?;
        output.pad_to(payload_start)?;
        for table in &mut self.tables {
            for column in &mut table.columns {
                for part in &mut column.parts {
                    output.pad_to(payload_start + part.offset)?;
                    let mut reader = part.file.reader()?;
                    if part.value_lengths {
                        copy_value_lengths(&mut reader, &mut output)?;
                    } else {
                        io::copy(&mut reader, &mut output)?;
                    }
                }
            }
        }
        if output.bytes != self.expected_bytes {
            return Err(EncodeError::EncodedLength {
                expected: self.expected_bytes,
                actual: output.bytes,
            });
        }
        let (bytes, sha256) = output.finish();
        Ok(EncodeReport {
            bytes,
            sha256,
            rows: self.rows,
        })
    }
}

/// The document `schema_hash` digests. A client that rebuilds it from a
/// generated schema and hashes it the same way binds that schema to the plan's
/// `schemaHash`, so its field order is part of the contract.
#[derive(Serialize)]
pub struct SchemaContract<'a> {
    format: &'static str,
    version: u32,
    tables: &'a [TableSchema],
}

impl<'a> SchemaContract<'a> {
    pub fn new(schema: &'a WorkspaceSchema) -> Self {
        Self {
            format: FORMAT_NAME,
            version: FORMAT_VERSION,
            tables: &schema.tables,
        }
    }
}

pub fn schema_hash(schema: &WorkspaceSchema) -> Result<String, EncodeError> {
    let bytes = serde_json::to_vec(&SchemaContract::new(schema))?;
    Ok(format!("sha256:{}", hex::encode(Sha256::digest(bytes))))
}

struct TableSpool {
    schema: TableSchema,
    columns: Vec<ColumnSpool>,
    primary_key_indexes: Vec<usize>,
    keys: HashSet<Vec<u8>>,
    rows: u32,
}

impl TableSpool {
    fn new(schema: TableSchema, budget: &SpoolBudget) -> Result<Self, EncodeError> {
        let by_name: HashMap<_, _> = schema
            .columns
            .iter()
            .enumerate()
            .map(|(index, column)| (column.name.as_str(), index))
            .collect();
        let primary_key_indexes = schema
            .primary_key
            .iter()
            .map(|name| by_name[name.as_str()])
            .collect();
        let columns = schema
            .columns
            .iter()
            .cloned()
            .map(|column| ColumnSpool::new(column, budget))
            .collect::<Result<_, _>>()?;
        Ok(Self {
            schema,
            columns,
            primary_key_indexes,
            keys: HashSet::new(),
            rows: 0,
        })
    }

    fn push_row(&mut self, values: &[Value], variable_limit: u64) -> Result<(), EncodeError> {
        if values.len() != self.schema.columns.len() {
            return Err(EncodeError::ColumnCount {
                table: self.schema.name.clone(),
                expected: self.schema.columns.len(),
                actual: values.len(),
            });
        }
        if self.rows == u32::MAX {
            return Err(EncodeError::TableRowLimit(self.schema.name.clone()));
        }
        for (column, value) in self.schema.columns.iter().zip(values) {
            validate_value(column, value)?;
        }
        let key = canonical_key(
            &self.schema.name,
            self.primary_key_indexes
                .iter()
                .map(|&index| (&self.schema.columns[index], &values[index])),
        )?;
        if !self.keys.insert(key) {
            return Err(EncodeError::DuplicatePrimaryKey(self.schema.name.clone()));
        }
        for (column, value) in self.columns.iter_mut().zip(values) {
            column.push(value, variable_limit)?;
        }
        self.rows += 1;
        Ok(())
    }

    fn finish_columns(mut self) -> Result<FinishedColumns, EncodeError> {
        let columns = self
            .columns
            .drain(..)
            .map(|column| column.finish(self.rows))
            .collect::<Result<Vec<_>, _>>()?;
        Ok(FinishedColumns {
            schema: self.schema,
            columns,
            rows: self.rows,
        })
    }
}

/// What one encoder's spools hold in memory before the rest move to temporary
/// files. A 100k-row artifact fits, and spends no build time on the hundred
/// files, and the `write(2)`s behind them, that made spooling the cost of a
/// build once several ran at once.
const SPOOL_MEMORY_BYTES: usize = 64 * 1024 * 1024;
const SPOOL_FIRST_ALLOCATION_BYTES: usize = 8 * 1024;
const SPOOL_FILE_BUFFER_BYTES: usize = 64 * 1024;

/// The memory left for one encoder's spools.
#[derive(Clone)]
struct SpoolBudget(Arc<AtomicUsize>);

impl SpoolBudget {
    fn new(bytes: usize) -> Self {
        Self(Arc::new(AtomicUsize::new(bytes)))
    }

    fn take(&self, bytes: usize) -> bool {
        self.0
            .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |left| {
                left.checked_sub(bytes)
            })
            .is_ok()
    }

    fn give_back(&self, bytes: usize) {
        self.0.fetch_add(bytes, Ordering::Relaxed);
    }
}

/// Spooled bytes in memory, charged to their encoder's budget until dropped.
struct SpoolMemory {
    bytes: Vec<u8>,
    budget: SpoolBudget,
    charged: usize,
}

impl SpoolMemory {
    /// Grows the allocation to hold `extra` more bytes, or says the budget
    /// cannot pay for it.
    fn make_room(&mut self, extra: usize) -> bool {
        let needed = self.bytes.len().saturating_add(extra);
        if needed <= self.charged {
            return true;
        }
        let capacity = needed
            .max(self.charged.saturating_mul(2))
            .max(SPOOL_FIRST_ALLOCATION_BYTES);
        if !self.budget.take(capacity - self.charged) {
            return false;
        }
        self.bytes.reserve_exact(capacity - self.bytes.len());
        self.charged = capacity;
        true
    }
}

impl Drop for SpoolMemory {
    fn drop(&mut self) {
        self.budget.give_back(self.charged);
    }
}

/// One part of a column as rows arrive: in memory while its encoder's budget
/// lasts, then in a temporary file.
enum SpoolFile {
    Memory(SpoolMemory),
    File(BufWriter<NamedTempFile>),
}

fn spool_file(budget: &SpoolBudget) -> SpoolFile {
    SpoolFile::Memory(SpoolMemory {
        bytes: Vec::new(),
        budget: budget.clone(),
        charged: 0,
    })
}

impl Write for SpoolFile {
    fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
        if let Self::Memory(memory) = self {
            if memory.make_room(bytes.len()) {
                memory.bytes.extend_from_slice(bytes);
                return Ok(bytes.len());
            }
            let mut file = BufWriter::with_capacity(SPOOL_FILE_BUFFER_BYTES, NamedTempFile::new()?);
            file.write_all(&memory.bytes)?;
            *self = Self::File(file);
        }
        match self {
            Self::File(file) => file.write(bytes),
            Self::Memory(_) => Err(io::Error::other("the spool did not move to its file")),
        }
    }

    fn flush(&mut self) -> io::Result<()> {
        match self {
            Self::File(file) => file.flush(),
            Self::Memory(_) => Ok(()),
        }
    }
}

/// A finished spool, read back as often as its column needs.
enum Spooled {
    Memory(SpoolMemory),
    File(NamedTempFile),
}

fn finish_spool(spool: SpoolFile) -> Result<Spooled, EncodeError> {
    Ok(match spool {
        SpoolFile::Memory(memory) => Spooled::Memory(memory),
        SpoolFile::File(file) => Spooled::File(
            file.into_inner()
                .map_err(|error| EncodeError::Io(error.into_error()))?,
        ),
    })
}

impl Spooled {
    fn length(&self) -> Result<u64, EncodeError> {
        Ok(match self {
            Self::Memory(memory) => memory.bytes.len() as u64,
            Self::File(file) => file.as_file().metadata()?.len(),
        })
    }

    fn reader(&self) -> Result<SpooledReader<'_>, EncodeError> {
        Ok(match self {
            Self::Memory(memory) => SpooledReader::Memory(&memory.bytes),
            Self::File(file) => SpooledReader::File(BufReader::new(file.reopen()?)),
        })
    }
}

enum SpooledReader<'a> {
    Memory(&'a [u8]),
    File(BufReader<File>),
}

impl Read for SpooledReader<'_> {
    fn read(&mut self, bytes: &mut [u8]) -> io::Result<usize> {
        match self {
            Self::Memory(memory) => memory.read(bytes),
            Self::File(file) => file.read(bytes),
        }
    }
}

struct ColumnSpool {
    schema: ColumnSchema,
    validity: Option<ValiditySpool>,
    data: SpoolFile,
    offsets: Option<SpoolFile>,
    variable_bytes: u64,
    dictionary: Option<DictionarySpool>,
    budget: SpoolBudget,
}

impl ColumnSpool {
    fn new(schema: ColumnSchema, budget: &SpoolBudget) -> Result<Self, EncodeError> {
        let mut offsets = matches!(
            schema.kind,
            ColumnKind::Utf8
                | ColumnKind::Bytes
                | ColumnKind::Json
                | ColumnKind::Decimal
                | ColumnKind::Date
                | ColumnKind::Time
                | ColumnKind::Interval
        )
        .then(|| spool_file(budget));
        if let Some(offsets) = &mut offsets {
            offsets.write_all(&0_u32.to_le_bytes())?;
        }
        let dictionary =
            (schema.kind == ColumnKind::Dict).then(|| DictionarySpool::new(budget.clone()));
        Ok(Self {
            validity: schema.nullable.then(|| ValiditySpool::new(budget.clone())),
            schema,
            data: spool_file(budget),
            offsets,
            variable_bytes: 0,
            dictionary,
            budget: budget.clone(),
        })
    }

    fn push(&mut self, value: &Value, variable_limit: u64) -> Result<(), EncodeError> {
        let present = !matches!(value, Value::Null);
        if let Some(validity) = &mut self.validity {
            validity.push(present)?;
        }
        match (&self.schema.kind, value) {
            (ColumnKind::Int64, Value::Int64(value)) => {
                self.data.write_all(&value.to_le_bytes())?;
            }
            (ColumnKind::Uint64, Value::Uint64(value)) => {
                self.data.write_all(&value.to_le_bytes())?;
            }
            (ColumnKind::F64, Value::F64(value)) => self.data.write_all(&value.to_le_bytes())?,
            (ColumnKind::Timestamp, Value::TimestampMicros(value)) => {
                self.data.write_all(&value.to_le_bytes())?;
            }
            (ColumnKind::Bool, Value::Bool(value)) => self.data.write_all(&[u8::from(*value)])?,
            (ColumnKind::Utf8, Value::Text(value)) => {
                self.push_variable(value.as_bytes(), variable_limit)?;
            }
            (ColumnKind::Bytes, Value::Bytes(value)) => {
                self.push_variable(value, variable_limit)?;
            }
            (ColumnKind::Uuid, Value::Uuid(value)) => self.data.write_all(value)?,
            (ColumnKind::Json, Value::Json(value)) => {
                self.push_variable(value, variable_limit)?;
            }
            (ColumnKind::Decimal, Value::DecimalText(value))
            | (ColumnKind::Date, Value::DateText(value))
            | (ColumnKind::Time, Value::TimeText(value))
            | (ColumnKind::Interval, Value::IntervalText(value)) => {
                self.push_variable(value.as_bytes(), variable_limit)?;
            }
            (ColumnKind::Dict, Value::Text(value)) => {
                let code = self
                    .dictionary
                    .as_mut()
                    .ok_or(EncodeError::Internal("dictionary spool missing"))?
                    .code(value, variable_limit)?;
                self.data.write_all(&code.to_le_bytes())?;
            }
            (_, Value::Null) => match self.schema.kind {
                ColumnKind::Int64 | ColumnKind::Timestamp => {
                    self.data.write_all(&0_i64.to_le_bytes())?
                }
                ColumnKind::Uint64 => self.data.write_all(&0_u64.to_le_bytes())?,
                ColumnKind::F64 => self.data.write_all(&0_f64.to_le_bytes())?,
                ColumnKind::Bool => self.data.write_all(&[0])?,
                ColumnKind::Uuid => self.data.write_all(&[0; 16])?,
                ColumnKind::Utf8
                | ColumnKind::Bytes
                | ColumnKind::Json
                | ColumnKind::Decimal
                | ColumnKind::Date
                | ColumnKind::Time
                | ColumnKind::Interval => self.push_variable(&[], variable_limit)?,
                // Interns nothing. `remap_codes` turns the sentinel into code
                // 0, so a null row's code does not depend on intern order and
                // stays legal when the dictionary is empty.
                ColumnKind::Dict => self.data.write_all(&u32::MAX.to_le_bytes())?,
            },
            _ => return Err(EncodeError::Internal("validated value changed kind")),
        }
        Ok(())
    }

    fn push_variable(&mut self, bytes: &[u8], limit: u64) -> Result<(), EncodeError> {
        self.variable_bytes = self
            .variable_bytes
            .checked_add(bytes.len() as u64)
            .ok_or(EncodeError::SizeOverflow)?;
        if self.variable_bytes > limit || self.variable_bytes > u64::from(u32::MAX) {
            return Err(EncodeError::VariableColumnLimit {
                column: self.schema.name.clone(),
                maximum: limit.min(u64::from(u32::MAX)),
            });
        }
        self.data.write_all(bytes)?;
        self.offsets
            .as_mut()
            .ok_or(EncodeError::Internal("offset spool missing"))?
            .write_all(&(self.variable_bytes as u32).to_le_bytes())?;
        Ok(())
    }

    fn finish(mut self, rows: u32) -> Result<FinishedColumn, EncodeError> {
        let mut parts = Vec::new();
        if let Some(validity) = self.validity {
            parts.push(validity.finish(rows)?);
        }
        let values = finish_spool(self.data)?;
        match self.schema.kind {
            ColumnKind::Int64 | ColumnKind::Uint64 | ColumnKind::Timestamp => {
                let validity = self.schema.nullable.then(|| &parts[0]);
                let (numeric_parts, numeric_encoding) =
                    encode_numeric(values, self.schema.kind, validity, &self.budget)?;
                parts.extend(numeric_parts);
                return Ok(FinishedColumn {
                    schema: self.schema,
                    parts,
                    dictionary_count: None,
                    bit_width: None,
                    encoding: numeric_encoding,
                });
            }
            ColumnKind::F64 | ColumnKind::Bool | ColumnKind::Uuid => {
                parts.push(FinishedPart::new(values)?);
            }
            ColumnKind::Utf8
            | ColumnKind::Bytes
            | ColumnKind::Json
            | ColumnKind::Decimal
            | ColumnKind::Date
            | ColumnKind::Time
            | ColumnKind::Interval => {
                let mut offsets = FinishedPart::new(finish_spool(
                    self.offsets
                        .take()
                        .ok_or(EncodeError::Internal("offset spool missing"))?,
                )?)?;
                offsets.value_lengths = true;
                parts.push(offsets);
                parts.push(FinishedPart::new(values)?);
            }
            ColumnKind::Dict => {
                let dictionary = self
                    .dictionary
                    .take()
                    .ok_or(EncodeError::Internal("dictionary spool missing"))?;
                let (count, offsets, data, remap) = dictionary.finish()?;
                let width = bit_width(u64::from(count.saturating_sub(1)));
                let codes = remap_codes(values, &remap, width, &self.budget)?;
                parts.push(FinishedPart::new(codes)?);
                parts.push(offsets);
                parts.push(data);
                return Ok(FinishedColumn {
                    schema: self.schema,
                    parts,
                    dictionary_count: Some(count),
                    bit_width: Some(width),
                    encoding: None,
                });
            }
        }
        Ok(FinishedColumn {
            schema: self.schema,
            parts,
            dictionary_count: None,
            bit_width: None,
            encoding: None,
        })
    }
}

struct ValiditySpool {
    file: Option<SpoolFile>,
    budget: SpoolBudget,
    byte: u8,
    bit: u8,
}

impl ValiditySpool {
    fn new(budget: SpoolBudget) -> Self {
        Self {
            file: None,
            budget,
            byte: 0,
            bit: 0,
        }
    }

    fn push(&mut self, present: bool) -> Result<(), EncodeError> {
        if present {
            self.byte |= 1 << self.bit;
        }
        self.bit += 1;
        if self.bit == 8 {
            let file = match &mut self.file {
                Some(file) => file,
                None => self.file.insert(spool_file(&self.budget)),
            };
            file.write_all(&[self.byte])?;
            self.byte = 0;
            self.bit = 0;
        }
        Ok(())
    }

    fn finish(mut self, rows: u32) -> Result<FinishedPart, EncodeError> {
        let mut file = match self.file.take() {
            Some(file) => file,
            None => spool_file(&self.budget),
        };
        if self.bit != 0 {
            file.write_all(&[self.byte])?;
        }
        let expected = u64::from(rows).div_ceil(8);
        let part = FinishedPart::new(finish_spool(file)?)?;
        if part.length != expected {
            return Err(EncodeError::Internal("validity length mismatch"));
        }
        Ok(part)
    }
}

/// Interns the distinct values of one dictionary column.
///
/// Codes are handed out in first-seen order while rows stream, then remapped at
/// [`DictionarySpool::finish`] so the emitted dictionary is sorted. The
/// distinct values are the only thing held in memory, which the intern table
/// already required.
struct DictionarySpool {
    values: HashMap<String, u32>,
    bytes: u64,
    budget: SpoolBudget,
}

impl DictionarySpool {
    fn new(budget: SpoolBudget) -> Self {
        Self {
            values: HashMap::new(),
            bytes: 0,
            budget,
        }
    }

    fn code(&mut self, value: &str, limit: u64) -> Result<u32, EncodeError> {
        if let Some(code) = self.values.get(value) {
            return Ok(*code);
        }
        let code = u32::try_from(self.values.len()).map_err(|_| EncodeError::SizeOverflow)?;
        self.bytes = self
            .bytes
            .checked_add(value.len() as u64)
            .ok_or(EncodeError::SizeOverflow)?;
        if self.bytes > limit || self.bytes > u64::from(u32::MAX) {
            return Err(EncodeError::VariableColumnLimit {
                column: "dictionary".to_owned(),
                maximum: limit.min(u64::from(u32::MAX)),
            });
        }
        self.values.insert(value.to_owned(), code);
        Ok(code)
    }

    /// Writes the dictionary in ascending UTF-8 byte order and returns the
    /// entry count, the offsets and data parts, and `first_seen -> sorted` code
    /// remapping the column's codes must be rewritten through.
    fn finish(self) -> Result<(u32, FinishedPart, FinishedPart, Vec<u32>), EncodeError> {
        let count = u32::try_from(self.values.len()).map_err(|_| EncodeError::SizeOverflow)?;
        let mut sorted: Vec<(&str, u32)> = self
            .values
            .iter()
            .map(|(value, code)| (value.as_str(), *code))
            .collect();
        sorted.sort_unstable_by(|left, right| left.0.as_bytes().cmp(right.0.as_bytes()));

        let mut remap = vec![0_u32; sorted.len()];
        let mut offsets = spool_file(&self.budget);
        let mut data = spool_file(&self.budget);
        offsets.write_all(&0_u32.to_le_bytes())?;
        let mut bytes = 0_u32;
        for (sorted_code, (value, first_seen)) in sorted.into_iter().enumerate() {
            let sorted_code = u32::try_from(sorted_code).map_err(|_| EncodeError::SizeOverflow)?;
            remap[first_seen as usize] = sorted_code;
            data.write_all(value.as_bytes())?;
            bytes = bytes
                .checked_add(u32::try_from(value.len()).map_err(|_| EncodeError::SizeOverflow)?)
                .ok_or(EncodeError::SizeOverflow)?;
            offsets.write_all(&bytes.to_le_bytes())?;
        }
        Ok((
            count,
            FinishedPart::new(finish_spool(offsets)?)?,
            FinishedPart::new(finish_spool(data)?)?,
            remap,
        ))
    }
}

/// A table as an artifact carries it: columns and no primary-key index.
struct FinishedColumns {
    schema: TableSchema,
    columns: Vec<FinishedColumn>,
    rows: u32,
}

struct FinishedColumn {
    schema: ColumnSchema,
    parts: Vec<FinishedPart>,
    dictionary_count: Option<u32>,
    bit_width: Option<u8>,
    encoding: Option<NumericEncoding>,
}

struct FinishedPart {
    file: Spooled,
    length: u64,
    offset: u64,
    /// A variable-width column's cumulative offsets, which
    /// [`LaidOutArtifact::write`] writes as value lengths.
    value_lengths: bool,
}

impl FinishedPart {
    fn new(file: Spooled) -> Result<Self, EncodeError> {
        let length = file.length()?;
        Ok(Self {
            file,
            length,
            offset: 0,
            value_lengths: false,
        })
    }
}

/// Copies a cumulative offsets spool as value lengths: the leading 0, then each
/// row's `offsets[row + 1] - offsets[row]`, one `u32` apiece, so the part keeps
/// the length and position the header gives it.
fn copy_value_lengths(offsets: &mut impl Read, output: &mut impl Write) -> Result<(), EncodeError> {
    let mut buffer = [0_u8; 64 * 1024];
    let mut previous = 0_u32;
    loop {
        let read = fill(offsets, &mut buffer)?;
        if read == 0 {
            return Ok(());
        }
        if !read.is_multiple_of(4) {
            return Err(EncodeError::Internal("offset spool is misaligned"));
        }
        for chunk in buffer[..read].chunks_exact_mut(4) {
            let offset = u32::from_le_bytes((&*chunk).try_into().expect("chunks_exact_mut(4)"));
            let length = offset
                .checked_sub(previous)
                .ok_or(EncodeError::Internal("offset spool regressed"))?;
            chunk.copy_from_slice(&length.to_le_bytes());
            previous = offset;
        }
        output.write_all(&buffer[..read])?;
    }
}

#[derive(Serialize)]
struct Header<'a> {
    format: &'static str,
    version: u32,
    identity: &'a ArtifactIdentity,
    tables: Vec<TableHeader<'a>>,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct TableHeader<'a> {
    name: &'a str,
    rows: u32,
    primary_key: &'a [String],
    columns: Vec<ColumnHeader<'a>>,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct ColumnHeader<'a> {
    name: &'a str,
    #[serde(rename = "type")]
    kind: ColumnKind,
    nullable: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    dictionary_count: Option<u32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    bit_width: Option<u8>,
    #[serde(skip_serializing_if = "Option::is_none")]
    encoding: Option<NumericEncoding>,
    parts: Vec<Part>,
}

#[derive(Debug, Clone, Copy, Serialize)]
#[serde(rename_all = "camelCase")]
struct NumericEncoding {
    kind: &'static str,
    block_rows: u32,
}

#[derive(Debug, Clone, Copy, Serialize)]
struct Part {
    offset: u64,
    length: u64,
}

struct HashingWriter<W> {
    inner: W,
    hash: Sha256,
    bytes: u64,
    maximum: u64,
}

impl<W: Write> HashingWriter<W> {
    fn new(inner: W, maximum: u64) -> Self {
        Self {
            inner,
            hash: Sha256::new(),
            bytes: 0,
            maximum,
        }
    }

    fn pad_to(&mut self, target: u64) -> Result<(), EncodeError> {
        if target < self.bytes {
            return Err(EncodeError::Internal("segment order regressed"));
        }
        let mut remaining = target - self.bytes;
        const ZEROES: [u8; 8] = [0; 8];
        while remaining > 0 {
            let count = usize::try_from(remaining.min(ZEROES.len() as u64))
                .map_err(|_| EncodeError::SizeOverflow)?;
            self.write_all(&ZEROES[..count])?;
            remaining -= count as u64;
        }
        Ok(())
    }

    fn finish(self) -> (u64, String) {
        (self.bytes, hex::encode(self.hash.finalize()))
    }
}

impl<W: Write> Write for HashingWriter<W> {
    fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
        let next = self
            .bytes
            .checked_add(bytes.len() as u64)
            .ok_or_else(|| std::io::Error::other("artifact size overflow"))?;
        if next > self.maximum {
            return Err(std::io::Error::other("artifact size limit exceeded"));
        }
        self.inner.write_all(bytes)?;
        self.hash.update(bytes);
        self.bytes = next;
        Ok(bytes.len())
    }

    fn flush(&mut self) -> std::io::Result<()> {
        self.inner.flush()
    }
}

fn validate_schema(schema: &WorkspaceSchema, limits: EncoderLimits) -> Result<(), EncodeError> {
    if schema.tables.is_empty() || schema.tables.len() > limits.max_tables {
        return Err(EncodeError::TableLimit(limits.max_tables));
    }
    let mut table_names = HashSet::new();
    for table in &schema.tables {
        if !valid_name(&table.name) || !table_names.insert(table.name.as_str()) {
            return Err(EncodeError::InvalidSchema(
                "table names must be unique and bounded",
            ));
        }
        if table.columns.is_empty() || table.columns.len() > limits.max_columns_per_table {
            return Err(EncodeError::ColumnLimit {
                table: table.name.clone(),
                maximum: limits.max_columns_per_table,
            });
        }
        let mut columns = HashSet::new();
        for column in &table.columns {
            if !valid_name(&column.name) || !columns.insert(column.name.as_str()) {
                return Err(EncodeError::InvalidSchema(
                    "column names must be unique and bounded",
                ));
            }
        }
        if table.primary_key.is_empty()
            || table.primary_key.iter().collect::<HashSet<_>>().len() != table.primary_key.len()
            || table
                .primary_key
                .iter()
                .any(|name| !columns.contains(name.as_str()))
        {
            return Err(EncodeError::InvalidSchema(
                "primary key must be selected and unique",
            ));
        }
        for key in &table.primary_key {
            let column = table
                .columns
                .iter()
                .find(|column| &column.name == key)
                .expect("every primary key column was found among the columns above");
            if column.nullable || matches!(column.kind, ColumnKind::F64 | ColumnKind::Json) {
                return Err(EncodeError::InvalidSchema(
                    "primary keys require a non-null exact key type",
                ));
            }
        }
    }
    Ok(())
}

fn validate_identity(identity: &ArtifactIdentity) -> Result<(), EncodeError> {
    let ordinary = [
        identity.source_epoch.as_str(),
        identity.manifest_version.as_str(),
        identity.authorization_scope_version.as_str(),
        identity.cursor.as_str(),
    ];
    if ordinary.into_iter().any(|value| !valid_name(value))
        || !valid_sha256(&identity.collection_set_hash)
        || !valid_sha256(&identity.schema_hash)
        || !valid_sha256(&identity.authorization_scope_hash)
    {
        return Err(EncodeError::InvalidIdentity);
    }
    Ok(())
}

fn valid_sha256(value: &str) -> bool {
    value.len() == 71
        && value.starts_with("sha256:")
        && value[7..]
            .bytes()
            .all(|byte| byte.is_ascii_hexdigit() && !byte.is_ascii_uppercase())
}

fn valid_name(value: &str) -> bool {
    !value.is_empty() && value.len() <= 255 && !value.contains('\0')
}

fn validate_value(schema: &ColumnSchema, value: &Value) -> Result<(), EncodeError> {
    if matches!(value, Value::Null) {
        return if schema.nullable {
            Ok(())
        } else {
            Err(EncodeError::NullInRequiredColumn(schema.name.clone()))
        };
    }
    let matches = matches!(
        (schema.kind, value),
        (ColumnKind::Int64, Value::Int64(_))
            | (ColumnKind::Uint64, Value::Uint64(_))
            | (ColumnKind::F64, Value::F64(_))
            | (ColumnKind::Timestamp, Value::TimestampMicros(_))
            | (ColumnKind::Bool, Value::Bool(_))
            | (ColumnKind::Utf8 | ColumnKind::Dict, Value::Text(_))
            | (ColumnKind::Bytes, Value::Bytes(_))
            | (ColumnKind::Uuid, Value::Uuid(_))
            | (ColumnKind::Json, Value::Json(_))
            | (ColumnKind::Decimal, Value::DecimalText(_))
            | (ColumnKind::Date, Value::DateText(_))
            | (ColumnKind::Time, Value::TimeText(_))
            | (ColumnKind::Interval, Value::IntervalText(_))
    );
    if !matches {
        return Err(EncodeError::ValueKind(schema.name.clone()));
    }
    match value {
        Value::F64(value) if !value.is_finite() => Err(EncodeError::NonFinite(schema.name.clone())),
        Value::Json(bytes) => {
            let parsed: serde_json::Value = serde_json::from_slice(bytes)
                .map_err(|_| EncodeError::InvalidJson(schema.name.clone()))?;
            if serde_json::to_vec(&parsed)
                .map_err(|_| EncodeError::InvalidJson(schema.name.clone()))?
                != *bytes
            {
                return Err(EncodeError::NonCanonicalValue(schema.name.clone()));
            }
            Ok(())
        }
        Value::DecimalText(value) => {
            let parsed = BigDecimal::from_str(value)
                .map_err(|_| EncodeError::NonCanonicalValue(schema.name.clone()))?;
            if parsed.normalized().to_plain_string() != *value {
                return Err(EncodeError::NonCanonicalValue(schema.name.clone()));
            }
            Ok(())
        }
        Value::DateText(value) => {
            let parsed = NaiveDate::parse_from_str(value, "%Y-%m-%d")
                .map_err(|_| EncodeError::NonCanonicalValue(schema.name.clone()))?;
            if parsed.format("%Y-%m-%d").to_string() != *value {
                return Err(EncodeError::NonCanonicalValue(schema.name.clone()));
            }
            Ok(())
        }
        Value::TimeText(value) => {
            let parsed = NaiveTime::parse_from_str(value, "%H:%M:%S%.f")
                .map_err(|_| EncodeError::NonCanonicalValue(schema.name.clone()))?;
            if parsed.format("%H:%M:%S%.f").to_string() != *value {
                return Err(EncodeError::NonCanonicalValue(schema.name.clone()));
            }
            Ok(())
        }
        Value::IntervalText(value)
            if value.is_empty()
                || value.contains('\0')
                || !(value.starts_with('P') || value.starts_with("-P")) =>
        {
            Err(EncodeError::NonCanonicalValue(schema.name.clone()))
        }
        _ => Ok(()),
    }
}

fn canonical_key<'a>(
    table: &str,
    values: impl Iterator<Item = (&'a ColumnSchema, &'a Value)>,
) -> Result<Vec<u8>, EncodeError> {
    let mut result = Vec::new();
    for (schema, value) in values {
        match (schema.kind, value) {
            (ColumnKind::Int64, Value::Int64(value)) => {
                result.push(1);
                result.extend_from_slice(&value.to_le_bytes());
            }
            (ColumnKind::Uint64, Value::Uint64(value)) => {
                result.push(2);
                result.extend_from_slice(&value.to_le_bytes());
            }
            (ColumnKind::Timestamp, Value::TimestampMicros(value)) => {
                result.push(3);
                result.extend_from_slice(&value.to_le_bytes());
            }
            (ColumnKind::Bool, Value::Bool(value)) => {
                result.extend_from_slice(&[4, u8::from(*value)]);
            }
            (ColumnKind::Utf8 | ColumnKind::Dict, Value::Text(value)) => {
                push_key_variable(&mut result, 5, value.as_bytes())?;
            }
            (ColumnKind::Bytes, Value::Bytes(value)) => {
                push_key_variable(&mut result, 6, value)?;
            }
            (ColumnKind::Uuid, Value::Uuid(value)) => {
                result.push(7);
                result.extend_from_slice(value);
            }
            (ColumnKind::Decimal, Value::DecimalText(value)) => {
                push_key_variable(&mut result, 8, value.as_bytes())?;
            }
            (ColumnKind::Date, Value::DateText(value)) => {
                push_key_variable(&mut result, 9, value.as_bytes())?;
            }
            (ColumnKind::Time, Value::TimeText(value)) => {
                push_key_variable(&mut result, 10, value.as_bytes())?;
            }
            (ColumnKind::Interval, Value::IntervalText(value)) => {
                push_key_variable(&mut result, 11, value.as_bytes())?;
            }
            _ => return Err(EncodeError::PrimaryKeyValue(table.to_owned())),
        }
    }
    Ok(result)
}

fn push_key_variable(result: &mut Vec<u8>, tag: u8, value: &[u8]) -> Result<(), EncodeError> {
    let length = u32::try_from(value.len()).map_err(|_| EncodeError::SizeOverflow)?;
    result.push(tag);
    result.extend_from_slice(&length.to_le_bytes());
    result.extend_from_slice(value);
    Ok(())
}

/// Rewrites a spooled `u32[rows]` code column through `remap`, which carries the
/// sorted code for every first-seen code. A code outside `remap` is the null
/// sentinel, or any code at all when the dictionary is empty, and becomes 0.
/// Streams in bounded chunks; the code column is the only dictionary part whose
/// size scales with rows.
fn remap_codes(
    codes: Spooled,
    remap: &[u32],
    width: u8,
    budget: &SpoolBudget,
) -> Result<Spooled, EncodeError> {
    let mut source = codes.reader()?;
    let mut values = Vec::new();
    let mut buffer = [0_u8; 8 * 1024];
    loop {
        let read = fill(&mut source, &mut buffer)?;
        if read == 0 {
            break;
        }
        if !read.is_multiple_of(4) {
            return Err(EncodeError::Internal(
                "dictionary code column is misaligned",
            ));
        }
        for chunk in buffer[..read].chunks_exact(4) {
            let code = u32::from_le_bytes(chunk.try_into().expect("chunks_exact(4)"));
            let sorted = remap.get(code as usize).copied().unwrap_or(0);
            values.push(u64::from(sorted));
        }
    }
    pack_values(values, width, budget)
}

fn bit_width(maximum: u64) -> u8 {
    (u64::BITS - maximum.leading_zeros()) as u8
}

fn packed_bytes(count: usize, width: u8) -> Result<usize, EncodeError> {
    count
        .checked_mul(usize::from(width))
        .and_then(|bits| bits.checked_add(7))
        .map(|bits| bits / 8)
        .ok_or(EncodeError::SizeOverflow)
}

fn pack_values(
    values: impl IntoIterator<Item = u64>,
    width: u8,
    budget: &SpoolBudget,
) -> Result<Spooled, EncodeError> {
    let mut packed = Vec::new();
    pack_into(values, width, &mut packed)?;
    let mut output = spool_file(budget);
    output.write_all(&packed)?;
    finish_spool(output)
}

/// Appends `values` to `output`, `width` bits apiece, least significant bit
/// first.
fn pack_into(
    values: impl IntoIterator<Item = u64>,
    width: u8,
    output: &mut Vec<u8>,
) -> Result<(), EncodeError> {
    if width > 64 {
        return Err(EncodeError::Internal("packed width exceeds 64 bits"));
    }
    let mut accumulator = 0_u128;
    let mut bits = 0_u8;
    for value in values {
        if width < 64 && value >= (1_u64 << width) {
            return Err(EncodeError::Internal("packed value exceeds width"));
        }
        accumulator |= u128::from(value) << bits;
        bits += width;
        while bits >= 8 {
            output.push(accumulator as u8);
            accumulator >>= 8;
            bits -= 8;
        }
    }
    if bits != 0 {
        output.push(accumulator as u8);
    }
    Ok(())
}

#[cfg(test)]
fn unpack_bits(bytes: &[u8], width: u8, count: usize) -> Result<Vec<u64>, EncodeError> {
    if width > 64 || bytes.len() != packed_bytes(count, width)? {
        return Err(EncodeError::Internal("invalid packed input"));
    }
    let mut values = Vec::with_capacity(count);
    let mut accumulator = 0_u128;
    let mut bits = 0_u8;
    let mut cursor = 0;
    for _ in 0..count {
        while bits < width {
            let byte = *bytes
                .get(cursor)
                .ok_or(EncodeError::Internal("truncated packed input"))?;
            accumulator |= u128::from(byte) << bits;
            bits += 8;
            cursor += 1;
        }
        let mask = if width == 64 {
            u128::from(u64::MAX)
        } else if width == 0 {
            0
        } else {
            (1_u128 << width) - 1
        };
        values.push((accumulator & mask) as u64);
        accumulator >>= width;
        bits -= width;
    }
    Ok(values)
}

fn encode_numeric(
    source: Spooled,
    kind: ColumnKind,
    validity: Option<&FinishedPart>,
    budget: &SpoolBudget,
) -> Result<(Vec<FinishedPart>, Option<NumericEncoding>), EncodeError> {
    let raw_length = source.length()?;
    if !raw_length.is_multiple_of(8) {
        return Err(EncodeError::Internal("numeric column is misaligned"));
    }
    let rows = usize::try_from(raw_length / 8).map_err(|_| EncodeError::SizeOverflow)?;
    if rows == 0 {
        return Ok((vec![FinishedPart::new(source)?], None));
    }

    let mut reader = source.reader()?;
    let validity = validity
        .map(|part| {
            let mut bytes = Vec::with_capacity(part.length as usize);
            part.file.reader()?.read_to_end(&mut bytes)?;
            Ok::<_, EncodeError>(bytes)
        })
        .transpose()?;
    let mut metadata = spool_file(budget);
    let mut data = spool_file(budget);
    let mut packed = Vec::with_capacity(NUMERIC_BLOCK_ROWS * 8);
    let mut data_length = 0_usize;
    for start in (0..rows).step_by(NUMERIC_BLOCK_ROWS) {
        let count = (rows - start).min(NUMERIC_BLOCK_ROWS);
        let mut raw = vec![0_u8; count * 8];
        reader.read_exact(&mut raw)?;

        let present = |within: usize| {
            validity.as_ref().is_none_or(|bitmap| {
                let row = start + within;
                bitmap[row / 8] & (1 << (row % 8)) != 0
            })
        };
        let (base_bits, scale_log10, deltas) = match kind {
            ColumnKind::Int64 | ColumnKind::Timestamp => {
                let values = raw
                    .chunks_exact(8)
                    .map(|chunk| i64::from_le_bytes(chunk.try_into().expect("8-byte chunk")))
                    .collect::<Vec<_>>();
                let scale = if kind == ColumnKind::Timestamp
                    && values
                        .iter()
                        .enumerate()
                        .all(|(index, value)| !present(index) || value % 1_000 == 0)
                {
                    1_000_i64
                } else {
                    1_i64
                };
                let scaled = values.iter().map(|value| value / scale).collect::<Vec<_>>();
                let base = scaled
                    .iter()
                    .enumerate()
                    .filter_map(|(index, value)| present(index).then_some(*value))
                    .min()
                    .unwrap_or(0);
                let deltas = scaled
                    .into_iter()
                    .enumerate()
                    .map(|(index, value)| {
                        if present(index) {
                            u64::try_from(i128::from(value) - i128::from(base))
                                .map_err(|_| EncodeError::SizeOverflow)
                        } else {
                            Ok(0)
                        }
                    })
                    .collect::<Result<Vec<_>, _>>()?;
                (base as u64, if scale == 1_000 { 3 } else { 0 }, deltas)
            }
            ColumnKind::Uint64 => {
                let values = raw
                    .chunks_exact(8)
                    .map(|chunk| u64::from_le_bytes(chunk.try_into().expect("8-byte chunk")))
                    .collect::<Vec<_>>();
                let base = values
                    .iter()
                    .enumerate()
                    .filter_map(|(index, value)| present(index).then_some(*value))
                    .min()
                    .unwrap_or(0);
                let deltas = values
                    .into_iter()
                    .enumerate()
                    .map(|(index, value)| if present(index) { value - base } else { 0 })
                    .collect();
                (base, 0, deltas)
            }
            _ => return Err(EncodeError::Internal("non-exact numeric encoding")),
        };
        let maximum = deltas.iter().copied().max().unwrap_or(0);
        let candidate_width = bit_width(maximum);
        let candidate_length = packed_bytes(count, candidate_width)?;
        let (width, base, scale) = if candidate_length < raw.len() {
            packed.clear();
            pack_into(deltas, candidate_width, &mut packed)?;
            data.write_all(&packed)?;
            (candidate_width, base_bits, scale_log10)
        } else {
            data.write_all(&raw)?;
            (64, 0, 0)
        };
        let offset = u32::try_from(data_length).map_err(|_| EncodeError::SizeOverflow)?;
        metadata.write_all(&offset.to_le_bytes())?;
        metadata.write_all(&base.to_le_bytes())?;
        metadata.write_all(&[width, scale, 0, 0])?;
        data_length = data_length
            .checked_add(if width == 64 {
                raw.len()
            } else {
                candidate_length
            })
            .ok_or(EncodeError::SizeOverflow)?;
    }

    let metadata = finish_spool(metadata)?;
    let data = finish_spool(data)?;
    let encoded_length = metadata
        .length()?
        .checked_add(data.length()?)
        .ok_or(EncodeError::SizeOverflow)?;
    let expected_metadata = rows
        .div_ceil(NUMERIC_BLOCK_ROWS)
        .checked_mul(NUMERIC_BLOCK_HEADER_BYTES)
        .ok_or(EncodeError::SizeOverflow)? as u64;
    if metadata.length()? != expected_metadata {
        return Err(EncodeError::Internal("numeric metadata length mismatch"));
    }
    if encoded_length >= raw_length {
        return Ok((vec![FinishedPart::new(source)?], None));
    }
    Ok((
        vec![FinishedPart::new(metadata)?, FinishedPart::new(data)?],
        Some(NumericEncoding {
            kind: "for",
            block_rows: NUMERIC_BLOCK_ROWS as u32,
        }),
    ))
}

/// Reads until `buffer` is full or the source ends, so a short read never
/// splits a code across two chunks.
fn fill(source: &mut impl Read, buffer: &mut [u8]) -> Result<usize, EncodeError> {
    let mut filled = 0;
    while filled < buffer.len() {
        let read = source.read(&mut buffer[filled..])?;
        if read == 0 {
            break;
        }
        filled += read;
    }
    Ok(filled)
}

fn align8(value: u64) -> Result<u64, EncodeError> {
    value
        .checked_add(7)
        .map(|value| value & !7)
        .ok_or(EncodeError::SizeOverflow)
}

/// What a written artifact's header says it is: the identity, including the
/// cursor, and each table's row count.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ArtifactHeaderSummary {
    pub identity: ArtifactIdentity,
    pub table_rows: Vec<(String, u64)>,
}

#[derive(Deserialize)]
struct HeaderSummary {
    format: String,
    version: u32,
    identity: ArtifactIdentity,
    tables: Vec<TableSummary>,
}

#[derive(Deserialize)]
struct TableSummary {
    name: String,
    rows: u64,
}

/// Reads the framing and header of an artifact back from its bytes, so a
/// publisher checks what was written rather than what it asked the encoder to
/// write. The payload is not read.
pub fn read_header_summary<R: Read>(
    mut input: R,
    max_header_bytes: u64,
) -> Result<ArtifactHeaderSummary, EncodeError> {
    let mut magic = [0_u8; 8];
    input.read_exact(&mut magic)?;
    if &magic != MAGIC {
        return Err(EncodeError::MalformedHeader("magic"));
    }
    let mut length = [0_u8; 4];
    input.read_exact(&mut length)?;
    let length = u64::from(u32::from_le_bytes(length));
    if length == 0 || length > max_header_bytes {
        return Err(EncodeError::MalformedHeader("header length"));
    }
    let mut header = vec![0_u8; usize::try_from(length).map_err(|_| EncodeError::SizeOverflow)?];
    input.read_exact(&mut header)?;
    let header: HeaderSummary = serde_json::from_slice(&header)?;
    if header.format != FORMAT_NAME || header.version != FORMAT_VERSION {
        return Err(EncodeError::MalformedHeader("format"));
    }
    Ok(ArtifactHeaderSummary {
        identity: header.identity,
        table_rows: header
            .tables
            .into_iter()
            .map(|table| (table.name, table.rows))
            .collect(),
    })
}

#[derive(Debug, Error)]
pub enum EncodeError {
    #[error("direct-columnar encoder is poisoned after a failed row")]
    Poisoned,
    #[error("unknown direct-columnar table {0}")]
    UnknownTable(String),
    #[error("invalid direct-columnar schema: {0}")]
    InvalidSchema(&'static str),
    #[error("invalid direct-columnar artifact identity")]
    InvalidIdentity,
    #[error("direct-columnar schema hash does not match the artifact identity")]
    SchemaIdentityMismatch,
    #[error("direct-columnar artifact cursor was never bound")]
    CursorUnbound,
    #[error("direct-columnar artifact cursor is already bound")]
    CursorAlreadyBound,
    #[error("direct-columnar artifact header is malformed: {0}")]
    MalformedHeader(&'static str),
    #[error("direct-columnar table limit exceeds {0}")]
    TableLimit(usize),
    #[error("table {table} exceeds {maximum} columns")]
    ColumnLimit { table: String, maximum: usize },
    #[error("direct-columnar workspace exceeds {0} rows")]
    RowLimit(u64),
    #[error("table {0} exceeds the format's u32 row limit")]
    TableRowLimit(String),
    #[error("table {table} expected {expected} columns, got {actual}")]
    ColumnCount {
        table: String,
        expected: usize,
        actual: usize,
    },
    #[error("required column {0} contains null")]
    NullInRequiredColumn(String),
    #[error("column {0} contains the wrong value kind")]
    ValueKind(String),
    #[error("column {0} contains a non-finite number")]
    NonFinite(String),
    #[error("column {0} contains invalid JSON")]
    InvalidJson(String),
    #[error("column {0} contains a non-canonical value")]
    NonCanonicalValue(String),
    #[error("table {0} contains an unsupported primary-key value")]
    PrimaryKeyValue(String),
    #[error("table {0} contains a duplicate primary key")]
    DuplicatePrimaryKey(String),
    #[error("variable column {column} exceeds {maximum} bytes")]
    VariableColumnLimit { column: String, maximum: u64 },
    #[error("direct-columnar header exceeds {0} bytes")]
    HeaderLimit(u64),
    #[error("direct-columnar artifact exceeds {0} bytes")]
    ArtifactLimit(u64),
    #[error("direct-columnar size arithmetic overflow")]
    SizeOverflow,
    #[error("encoded length mismatch: expected {expected}, got {actual}")]
    EncodedLength { expected: u64, actual: u64 },
    #[error("internal direct-columnar invariant failed: {0}")]
    Internal(&'static str),
    #[error(transparent)]
    Io(#[from] std::io::Error),
    #[error(transparent)]
    Json(#[from] serde_json::Error),
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn browser_key_index_bytes_counts_every_tables_slots_and_the_largest_tables_hashes() {
        // No table: nothing to build.
        assert_eq!(browser_key_index_bytes([0_u64; 0]), 0);
        // An empty table still gets one slot.
        assert_eq!(browser_key_index_bytes([0_u64]), 4);
        // Four rows fill eight slots exactly; five round up to sixteen.
        assert_eq!(browser_key_index_bytes([4_u64]), 8 * 4 + 4 * 4);
        assert_eq!(browser_key_index_bytes([5_u64]), 16 * 4 + 5 * 4);
        // The core artifact test's seven one-row tables: 60 bytes there too.
        assert_eq!(browser_key_index_bytes([1_u64; 7]), 60);
        // One million rows take 2^21 slots; only the largest table's hashes
        // count.
        assert_eq!(
            browser_key_index_bytes([3_u64, 1_000_000]),
            8 * 4 + 2_097_152 * 4 + 1_000_000 * 4
        );
    }

    /// One read of the spool holds 16,384 offsets, so a longer column crosses
    /// reads, and each length after a crossing is still taken from the offset
    /// before it, not from zero.
    #[test]
    fn value_lengths_carry_the_previous_offset_across_spool_reads() {
        let lengths: Vec<u32> = std::iter::once(0)
            .chain((0..40_000_u32).map(|row| row % 7 + 1))
            .collect();
        let mut offsets = Vec::with_capacity(lengths.len() * 4);
        let mut offset = 0_u32;
        for length in &lengths {
            offset += length;
            offsets.extend_from_slice(&offset.to_le_bytes());
        }
        let mut written = Vec::new();
        copy_value_lengths(&mut offsets.as_slice(), &mut written).unwrap();
        let expected: Vec<u8> = lengths
            .iter()
            .flat_map(|length| length.to_le_bytes())
            .collect();
        assert_eq!(written, expected);
    }

    fn identity() -> ArtifactIdentity {
        ArtifactIdentity {
            source_epoch: "source-epoch-1".to_owned(),
            manifest_version: "manifest-1".to_owned(),
            collection_set_hash: format!("sha256:{}", "1".repeat(64)),
            schema_hash: schema_hash(&schema()).unwrap(),
            authorization_scope_version: "scope-1".to_owned(),
            authorization_scope_hash: format!("sha256:{}", "2".repeat(64)),
            cursor: "cursor-1".to_owned(),
        }
    }

    fn schema() -> WorkspaceSchema {
        WorkspaceSchema {
            tables: vec![TableSchema {
                name: "items".to_owned(),
                primary_key: vec!["id".to_owned()],
                columns: vec![
                    ColumnSchema {
                        name: "id".to_owned(),
                        kind: ColumnKind::Dict,
                        nullable: false,
                    },
                    ColumnSchema {
                        name: "score".to_owned(),
                        kind: ColumnKind::F64,
                        nullable: true,
                    },
                    ColumnSchema {
                        name: "active".to_owned(),
                        kind: ColumnKind::Bool,
                        nullable: false,
                    },
                    ColumnSchema {
                        name: "label".to_owned(),
                        kind: ColumnKind::Utf8,
                        nullable: true,
                    },
                    ColumnSchema {
                        name: "payload".to_owned(),
                        kind: ColumnKind::Json,
                        nullable: false,
                    },
                ],
            }],
        }
    }

    fn row(id: &str, score: Value, label: Value) -> Vec<Value> {
        vec![
            Value::Text(id.to_owned()),
            score,
            Value::Bool(true),
            label,
            Value::Json(br#"{"ok":true}"#.to_vec()),
        ]
    }

    #[test]
    fn streams_deterministic_nullable_columns_without_a_key_index() {
        let encode = || {
            let mut encoder = Encoder::new(identity(), schema(), EncoderLimits::default()).unwrap();
            encoder
                .push_row("items", &row("a", Value::F64(4.5), Value::Null))
                .unwrap();
            encoder
                .push_row("items", &row("b", Value::Null, Value::Text("B".to_owned())))
                .unwrap();
            // The artifact's length is known before its first byte is written.
            let artifact = encoder.lay_out().unwrap();
            let length = artifact.bytes();
            let mut bytes = Vec::new();
            let report = artifact.write(&mut bytes).unwrap();
            assert_eq!(length as usize, bytes.len());
            (bytes, report)
        };
        let (first, report) = encode();
        let (second, second_report) = encode();
        assert_eq!(first, second);
        assert_eq!(report, second_report);
        assert_eq!(report.rows, 2);
        assert_eq!(report.bytes as usize, first.len());
        assert_eq!(&first[..8], MAGIC);
        assert_eq!(report.sha256, hex::encode(Sha256::digest(&first)));

        let header_length = u32::from_le_bytes(first[8..12].try_into().unwrap()) as usize;
        let header: serde_json::Value =
            serde_json::from_slice(&first[12..12 + header_length]).unwrap();
        assert_eq!(header["tables"][0]["rows"], 2);
        assert_eq!(header["version"], 4);
        assert_eq!(header["tables"][0]["columns"][1]["nullable"], true);
        assert_eq!(header["tables"][0]["columns"][1]["parts"][0]["length"], 1);
        // No primary-key index: a receiver builds its own.
        let table = header["tables"][0].as_object().unwrap();
        assert_eq!(
            table
                .keys()
                .map(String::as_str)
                .collect::<std::collections::BTreeSet<_>>(),
            std::collections::BTreeSet::from(["columns", "name", "primaryKey", "rows"])
        );
    }

    #[test]
    fn duplicate_type_null_and_bound_failures_poison_without_output() {
        let mut duplicate = Encoder::new(identity(), schema(), EncoderLimits::default()).unwrap();
        duplicate
            .push_row("items", &row("a", Value::F64(1.0), Value::Null))
            .unwrap();
        assert!(matches!(
            duplicate.push_row("items", &row("a", Value::F64(2.0), Value::Null)),
            Err(EncodeError::DuplicatePrimaryKey(_))
        ));
        assert!(matches!(
            duplicate.finish(Vec::new()),
            Err(EncodeError::Poisoned)
        ));

        let mut wrong = Encoder::new(identity(), schema(), EncoderLimits::default()).unwrap();
        let mut bad = row("b", Value::Null, Value::Null);
        bad[2] = Value::Null;
        assert!(matches!(
            wrong.push_row("items", &bad),
            Err(EncodeError::NullInRequiredColumn(_))
        ));

        let mut noncanonical =
            Encoder::new(identity(), schema(), EncoderLimits::default()).unwrap();
        let mut bad_json = row("json", Value::Null, Value::Null);
        bad_json[4] = Value::Json(br#"{ "ok": true }"#.to_vec());
        assert!(matches!(
            noncanonical.push_row("items", &bad_json),
            Err(EncodeError::NonCanonicalValue(column)) if column == "payload"
        ));

        let limits = EncoderLimits {
            max_variable_bytes_per_column: 1,
            ..EncoderLimits::default()
        };
        let mut bounded = Encoder::new(identity(), schema(), limits).unwrap();
        assert!(matches!(
            bounded.push_row("items", &row("long", Value::Null, Value::Null)),
            Err(EncodeError::VariableColumnLimit { .. })
        ));
    }

    #[test]
    fn dictionary_entries_are_sorted_and_codes_are_remapped() {
        let schema = WorkspaceSchema {
            tables: vec![TableSchema {
                name: "items".to_owned(),
                primary_key: vec!["id".to_owned()],
                columns: vec![
                    ColumnSchema {
                        name: "id".to_owned(),
                        kind: ColumnKind::Int64,
                        nullable: false,
                    },
                    ColumnSchema {
                        name: "tag".to_owned(),
                        kind: ColumnKind::Dict,
                        nullable: true,
                    },
                ],
            }],
        };
        let identity = ArtifactIdentity {
            schema_hash: schema_hash(&schema).unwrap(),
            ..identity()
        };
        let mut encoder = Encoder::new(identity, schema, EncoderLimits::default()).unwrap();
        // Interned first-seen: delta=0, alpha=1, charlie=2, bravo=3.
        for (id, tag) in [
            (0, Some("delta")),
            (1, Some("alpha")),
            (2, Some("charlie")),
            (3, None),
            (4, Some("bravo")),
            (5, Some("alpha")),
        ] {
            let tag = tag.map_or(Value::Null, |tag| Value::Text(tag.to_owned()));
            encoder.push_row("items", &[Value::Int64(id), tag]).unwrap();
        }
        let mut artifact = Vec::new();
        encoder.finish(&mut artifact).unwrap();

        let header_length = u32::from_le_bytes(artifact[8..12].try_into().unwrap()) as usize;
        let header: serde_json::Value =
            serde_json::from_slice(&artifact[12..12 + header_length]).unwrap();
        let column = &header["tables"][0]["columns"][1];
        assert_eq!(column["dictionaryCount"], 4);
        assert_eq!(column["bitWidth"], 2);
        let payload_start = align8(12 + header_length as u64).unwrap() as usize;
        let part = |index: usize| {
            let part = &column["parts"][index];
            let offset = payload_start + part["offset"].as_u64().unwrap() as usize;
            &artifact[offset..offset + part["length"].as_u64().unwrap() as usize]
        };

        // parts are validity, codes, dictionary offsets, dictionary data.
        let codes = unpack_bits(part(1), 2, 6).unwrap();
        let offsets: Vec<u32> = part(2)
            .chunks_exact(4)
            .map(|chunk| u32::from_le_bytes(chunk.try_into().unwrap()))
            .collect();
        let data = part(3);
        assert_eq!(data, b"alphabravocharliedelta");
        assert_eq!(offsets, vec![0, 5, 10, 17, 22]);
        let entries: Vec<&[u8]> = offsets
            .windows(2)
            .map(|window| &data[window[0] as usize..window[1] as usize])
            .collect();
        assert!(entries.windows(2).all(|pair| pair[0] < pair[1]));
        assert_eq!(codes, vec![3, 0, 2, 0, 1, 0]);
    }

    /// The v4 golden vector for variable-width columns: the lengths part holds
    /// a 0 and then each row's byte length, a null row's being 0. The artifact
    /// digest pins the rest of the encoding for this input, so any change to
    /// what this encoder writes shows up here and must be made on purpose.
    #[test]
    fn variable_width_columns_carry_value_lengths() {
        let schema = WorkspaceSchema {
            tables: vec![TableSchema {
                name: "items".to_owned(),
                primary_key: vec!["id".to_owned()],
                columns: vec![
                    ColumnSchema {
                        name: "id".to_owned(),
                        kind: ColumnKind::Int64,
                        nullable: false,
                    },
                    ColumnSchema {
                        name: "note".to_owned(),
                        kind: ColumnKind::Utf8,
                        nullable: true,
                    },
                    ColumnSchema {
                        name: "raw".to_owned(),
                        kind: ColumnKind::Bytes,
                        nullable: false,
                    },
                ],
            }],
        };
        let identity = ArtifactIdentity {
            schema_hash: schema_hash(&schema).unwrap(),
            ..identity()
        };
        let mut encoder = Encoder::new(identity, schema, EncoderLimits::default()).unwrap();
        for (id, note, raw) in [
            (1, Some("déjà"), vec![0xff]),
            (2, None, vec![]),
            (3, Some(""), vec![1, 2, 3]),
            (4, Some("vu"), vec![]),
        ] {
            let note = note.map_or(Value::Null, |note| Value::Text(note.to_owned()));
            encoder
                .push_row("items", &[Value::Int64(id), note, Value::Bytes(raw)])
                .unwrap();
        }
        let mut artifact = Vec::new();
        let report = encoder.finish(&mut artifact).unwrap();

        let header_length = u32::from_le_bytes(artifact[8..12].try_into().unwrap()) as usize;
        let header: serde_json::Value =
            serde_json::from_slice(&artifact[12..12 + header_length]).unwrap();
        assert_eq!(header["version"], 4);
        let payload_start = align8(12 + header_length as u64).unwrap() as usize;
        let part = |column: usize, index: usize| {
            let part = &header["tables"][0]["columns"][column]["parts"][index];
            let offset = payload_start + part["offset"].as_u64().unwrap() as usize;
            &artifact[offset..offset + part["length"].as_u64().unwrap() as usize]
        };

        // `note` is nullable, so its parts are validity, lengths, values.
        // "déjà" is six bytes of UTF-8.
        assert_eq!(part(1, 0), [0b1101]);
        assert_eq!(
            hex::encode(part(1, 1)),
            "0000000006000000000000000000000002000000"
        );
        assert_eq!(part(1, 2), "déjàvu".as_bytes());
        assert_eq!(
            hex::encode(part(2, 0)),
            "0000000001000000000000000300000000000000"
        );
        assert_eq!(part(2, 1), [0xff, 1, 2, 3]);
        assert_eq!(report.sha256, GOLDEN_VARIABLE_WIDTH_SHA256);
    }

    const GOLDEN_VARIABLE_WIDTH_SHA256: &str =
        "413e7051c0fa04d60e9b97203e6aff545145b9235c4f518df04de13622f6ef39";

    #[test]
    fn exact_numeric_blocks_pack_and_arbitrary_values_fall_back_without_loss() {
        let schema = WorkspaceSchema {
            tables: vec![TableSchema {
                name: "items".to_owned(),
                primary_key: vec!["id".to_owned()],
                columns: vec![
                    ColumnSchema {
                        name: "id".to_owned(),
                        kind: ColumnKind::Int64,
                        nullable: false,
                    },
                    ColumnSchema {
                        name: "occurred_at".to_owned(),
                        kind: ColumnKind::Timestamp,
                        nullable: false,
                    },
                    ColumnSchema {
                        name: "arbitrary".to_owned(),
                        kind: ColumnKind::Int64,
                        nullable: false,
                    },
                ],
            }],
        };
        let identity = ArtifactIdentity {
            schema_hash: schema_hash(&schema).unwrap(),
            ..identity()
        };
        let mut encoder = Encoder::new(identity, schema, EncoderLimits::default()).unwrap();
        for row in 0..2_048_i64 {
            encoder
                .push_row(
                    "items",
                    &[
                        Value::Int64(row),
                        Value::TimestampMicros(1_700_000_000_000_000 + row * 1_000),
                        Value::Int64(if row & 1 == 0 { i64::MIN } else { i64::MAX }),
                    ],
                )
                .unwrap();
        }
        let mut artifact = Vec::new();
        encoder.finish(&mut artifact).unwrap();
        let header_length = u32::from_le_bytes(artifact[8..12].try_into().unwrap()) as usize;
        let header: serde_json::Value =
            serde_json::from_slice(&artifact[12..12 + header_length]).unwrap();
        let columns = header["tables"][0]["columns"].as_array().unwrap();
        assert_eq!(columns[0]["encoding"]["kind"], "for");
        assert_eq!(columns[1]["encoding"]["kind"], "for");
        assert_eq!(columns[2].get("encoding"), None);
    }

    #[test]
    fn packed_values_round_trip_at_every_width() {
        for width in [0_u8, 1, 3, 7, 8, 12, 17, 32, 63, 64] {
            let limit = if width == 64 {
                u64::MAX
            } else if width == 0 {
                0
            } else {
                (1_u64 << width) - 1
            };
            let values = (0..37_u64)
                .map(|index| index.wrapping_mul(0x9e37_79b9_7f4a_7c15) & limit)
                .collect::<Vec<_>>();
            let packed = pack_values(values.clone(), width, &SpoolBudget::new(usize::MAX)).unwrap();
            let mut bytes = Vec::new();
            packed.reader().unwrap().read_to_end(&mut bytes).unwrap();
            assert_eq!(bytes.len(), packed_bytes(values.len(), width).unwrap());
            assert_eq!(unpack_bits(&bytes, width, values.len()).unwrap(), values);
        }
    }

    #[test]
    fn an_artifact_is_the_same_whether_its_spools_stay_in_memory_or_spill() {
        let encode = |spool_memory_bytes| {
            let mut encoder = Encoder::with_spool_memory(
                identity(),
                schema(),
                EncoderLimits::default(),
                spool_memory_bytes,
            )
            .unwrap();
            for index in 0..5_000 {
                let label = if index % 7 == 0 {
                    Value::Null
                } else {
                    Value::Text(format!("label {index}"))
                };
                encoder
                    .push_row(
                        "items",
                        &row(&format!("id-{index}"), Value::F64(f64::from(index)), label),
                    )
                    .unwrap();
            }
            let mut bytes = Vec::new();
            let report = encoder.finish(&mut bytes).unwrap();
            (bytes, report.sha256)
        };
        let in_memory = encode(SPOOL_MEMORY_BYTES);
        assert_eq!(encode(0), in_memory);
        assert_eq!(encode(20 * 1024), in_memory);
    }

    #[test]
    fn a_spool_moves_to_a_file_past_its_budget_and_gives_its_memory_back() {
        let budget = SpoolBudget::new(16 * 1024);
        let mut spool = spool_file(&budget);
        spool.write_all(&[7; 8 * 1024]).unwrap();
        assert!(matches!(spool, SpoolFile::Memory(_)));
        spool.write_all(&[8; 64 * 1024]).unwrap();
        assert!(matches!(spool, SpoolFile::File(_)));
        assert_eq!(budget.0.load(Ordering::Relaxed), 16 * 1024);

        let finished = finish_spool(spool).unwrap();
        assert_eq!(finished.length().unwrap(), 72 * 1024);
        let mut read = Vec::new();
        finished.reader().unwrap().read_to_end(&mut read).unwrap();
        assert_eq!(read[..8 * 1024], [7; 8 * 1024]);
        assert_eq!(read[8 * 1024..], [8; 64 * 1024]);
    }

    #[test]
    fn invalid_schema_rejects_nullable_or_json_primary_keys() {
        let mut invalid = schema();
        invalid.tables[0].primary_key = vec!["score".to_owned()];
        assert!(matches!(
            Encoder::new(identity(), invalid, EncoderLimits::default()),
            Err(EncodeError::InvalidSchema(_))
        ));
    }

    #[test]
    fn an_artifact_built_before_its_cut_is_proven_names_the_cursor_bound_afterwards() {
        let unbound = || ArtifactIdentity {
            cursor: String::new(),
            ..identity()
        };
        let mut encoder =
            Encoder::new_awaiting_cursor(unbound(), schema(), EncoderLimits::default()).unwrap();
        encoder
            .push_row("items", &row("a", Value::F64(1.0), Value::Null))
            .unwrap();
        assert_eq!(encoder.table_rows("items"), Some(1));
        assert!(matches!(
            encoder.finish(Vec::new()),
            Err(EncodeError::CursorUnbound)
        ));

        let mut encoder =
            Encoder::new_awaiting_cursor(unbound(), schema(), EncoderLimits::default()).unwrap();
        encoder
            .push_row("items", &row("a", Value::F64(1.0), Value::Null))
            .unwrap();
        encoder.bind_cursor("cursor-at-cut").unwrap();
        assert!(matches!(
            encoder.bind_cursor("another-cursor"),
            Err(EncodeError::CursorAlreadyBound)
        ));
        let mut bytes = Vec::new();
        encoder.finish(&mut bytes).unwrap();
        let summary = read_header_summary(bytes.as_slice(), 1 << 20).unwrap();
        assert_eq!(summary.identity.cursor, "cursor-at-cut");
        assert_eq!(
            summary.identity,
            ArtifactIdentity {
                cursor: "cursor-at-cut".to_owned(),
                ..identity()
            }
        );
        assert_eq!(summary.table_rows, vec![("items".to_owned(), 1)]);

        assert!(matches!(
            Encoder::new_awaiting_cursor(identity(), schema(), EncoderLimits::default()),
            Err(EncodeError::CursorAlreadyBound)
        ));
    }

    #[test]
    fn a_short_or_foreign_artifact_header_is_refused() {
        let mut encoder = Encoder::new(identity(), schema(), EncoderLimits::default()).unwrap();
        encoder
            .push_row("items", &row("a", Value::F64(1.0), Value::Null))
            .unwrap();
        let mut bytes = Vec::new();
        encoder.finish(&mut bytes).unwrap();
        let header_length = u32::from_le_bytes(bytes[8..12].try_into().unwrap()) as usize;

        assert!(matches!(
            read_header_summary(&bytes[..12 + header_length - 1], 1 << 20),
            Err(EncodeError::Io(_))
        ));
        let mut foreign = bytes.clone();
        foreign[0] = b'X';
        assert!(matches!(
            read_header_summary(foreign.as_slice(), 1 << 20),
            Err(EncodeError::MalformedHeader("magic"))
        ));
        assert!(matches!(
            read_header_summary(bytes.as_slice(), header_length as u64 - 1),
            Err(EncodeError::MalformedHeader("header length"))
        ));
    }
}
