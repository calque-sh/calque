//! The packed artifact body of `spec/packed-artifact-v1.md`: [`pack`] and its inverse [`unpack`].

use std::{
    cell::Cell,
    collections::HashMap,
    io::{Read, Write},
    ops::Range,
};

use brotli::enc::BrotliEncoderParams;
use serde::{Deserialize, Serialize};
use thiserror::Error;

use super::{FORMAT_NAME, FORMAT_VERSION, MAGIC, NUMERIC_BLOCK_HEADER_BYTES, NUMERIC_BLOCK_ROWS};

const BODY_MAGIC: &[u8; 8] = b"CALQPAK1";
const BODY_VERSION: u64 = 1;
const UUID_BYTES: usize = 16;
const SEPARATOR: u8 = 0xff;
/// An `int` column's values lie strictly between `-INT_LIMIT` and `INT_LIMIT`.
const INT_LIMIT: i64 = 1 << 51;
/// The most bytes a zigzag delta of two such values takes.
const MAX_INT_WIDTH: u8 = 7;
/// The size test's brotli parameters.
const SIZE_TEST_QUALITY: i32 = 9;
const SIZE_TEST_LGWIN: i32 = 24;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum Transform {
    Fk,
    Int,
    Sep,
}

/// What [`pack`] made of an artifact.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Packed {
    /// The packed body, or the artifact itself when `stored`.
    pub body: Vec<u8>,
    /// Whether packing would not have shortened the artifact, so the body is
    /// the artifact and no column is transformed.
    pub stored: bool,
    pub hoisted: Vec<HoistedPart>,
    /// One entry per column of every table, in header order.
    pub columns: Vec<ColumnChoice>,
    /// The body offsets at which its sections end, ascending: the preamble
    /// with the artifact's header and the hoisted parts, then each stored
    /// part and each stream in body order. The last is the body's length.
    /// Empty when `stored`.
    pub part_ends: Vec<usize>,
}

/// A part the body carries ahead of the payload.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct HoistedPart {
    pub table: String,
    pub column: String,
    /// The part's index among its column's parts.
    pub part: usize,
    pub bytes: u64,
}

/// What the packer did with one column.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ColumnChoice {
    pub table: String,
    pub column: String,
    #[serde(rename = "type")]
    pub kind: String,
    /// The transform in the body.
    pub transform: Option<Transform>,
    /// The transform the column was eligible for.
    pub considered: Option<Transform>,
    /// The column's parts in the artifact, padding left out.
    pub stored_bytes: u64,
    /// The stream of the transform considered.
    pub stream_bytes: Option<u64>,
    /// The two sizes the `int` and `sep` rule compared: the parts the stream
    /// would replace, and the stream.
    pub stored_q9_bytes: Option<u64>,
    pub stream_q9_bytes: Option<u64>,
    /// `fk`: bytes per row index. `int`: bytes per delta.
    pub width: Option<u8>,
    /// `fk`: values not found in the referenced table.
    pub escapes: Option<u64>,
    /// `fk`: the table the column is coded against.
    pub referenced_table: Option<String>,
}

#[derive(Debug, Error)]
pub enum PackError {
    #[error("not a direct-columnar v4 artifact the packer reads: {0}")]
    Artifact(&'static str),
    #[error("the packed body does not restore the artifact: {0}")]
    Inverse(#[from] UnpackError),
    #[error("the packed body restores other bytes than the artifact's")]
    InverseBytes,
    #[error("the preamble exceeds the format's u32 length")]
    PreambleLength,
    #[error("the size test's encoder failed: {0}")]
    Brotli(#[source] std::io::Error),
    #[error("the pack was cancelled")]
    Cancelled,
}

/// Why [`unpack`] refuses a body.
#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub enum UnpackError {
    #[error("the body begins with neither a packed nor a stored artifact's magic")]
    Magic,
    #[error("the body's preamble cannot be read: {0}")]
    Preamble(&'static str),
    #[error("the body is version {0}, not 1")]
    Version(u64),
    #[error("the body's artifact header cannot be read: {0}")]
    Header(&'static str),
    #[error("the body restores {actual} bytes, not the {expected} expected")]
    DecodedLength { expected: u64, actual: u64 },
    #[error("the body ends before its last part")]
    Truncated,
    #[error("bytes follow the body's last part")]
    TrailingBytes,
    #[error("the transformed column {column} of table {table}: {defect}")]
    Column {
        /// Indexes into the header's tables and that table's columns.
        table: u32,
        column: u32,
        defect: Defect,
    },
    #[error("{0} bytes of output could not be allocated")]
    Allocation(u64),
}

/// What is wrong with a transformed column.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Error)]
pub enum Defect {
    #[error("the transform does not apply to its type")]
    Type,
    #[error("the header's parts are not the ones the transform replaces")]
    Parts,
    #[error("the stream's length is not the one its rows give")]
    Rows,
    #[error("the width is not the one the transform writes")]
    Width,
    #[error("the referenced table has no key part in the body before the stream")]
    References,
    #[error("a row index past its table")]
    RowIndex,
    #[error("a null row with an index other than 0")]
    NullRow,
    #[error("an escape past the escapes")]
    EscapePastEscapes,
    #[error("escapes no row uses")]
    UnusedEscapes,
    #[error("a value 2^51 or more from zero")]
    Value,
    #[error("a frame-of-reference block that cannot hold its rows")]
    Block,
    #[error("a separator count other than the present rows")]
    Separators,
    #[error("a part rebuilt to a length other than the header's")]
    PartLength,
}

/// Packs `artifact`. The body is never longer than the artifact and depends
/// on its bytes alone.
pub fn pack(artifact: &[u8]) -> Result<Packed, PackError> {
    pack_unless(artifact, &|| false)
}

/// Packs `artifact` as [`pack`] does, and ends with [`PackError::Cancelled`]
/// once `cancelled` has answered true. It is asked before each table's key
/// part is indexed, before each column, before each table a foreign key is
/// tried against, at every read of a size test's input, and before the body
/// is checked against the artifact.
pub fn pack_unless(artifact: &[u8], cancelled: &dyn Fn() -> bool) -> Result<Packed, PackError> {
    pack_with(artifact, Rule::Frozen, &Cancel::new(cancelled))
}

/// Restores the artifact a body stands for, which must be `decoded_bytes`
/// long. Refuses every body `spec/packed-artifact-v1.md` lists.
pub fn unpack(body: &[u8], decoded_bytes: u64) -> Result<Vec<u8>, UnpackError> {
    let decoded =
        usize::try_from(decoded_bytes).map_err(|_| UnpackError::Allocation(decoded_bytes))?;
    let magic = body.get(..8);
    if magic == Some(MAGIC.as_slice()) {
        if body.len() != decoded {
            return Err(UnpackError::DecodedLength {
                expected: decoded_bytes,
                actual: body.len() as u64,
            });
        }
        return Ok(body.to_vec());
    }
    if magic != Some(BODY_MAGIC.as_slice()) {
        return Err(UnpackError::Magic);
    }
    let preamble_bytes = body
        .get(8..12)
        .map(|length| u32::from_le_bytes(length.try_into().expect("4 bytes")) as usize)
        .and_then(|length| body.get(12..length.checked_add(12)?))
        .ok_or(UnpackError::Preamble("it runs past the body"))?;
    let preamble = Preamble::read(preamble_bytes)?;
    let artifact_at = 12 + preamble_bytes.len();
    let layout = Layout::read(&body[artifact_at..]).map_err(UnpackError::Header)?;
    if layout.bytes != decoded {
        return Err(UnpackError::DecodedLength {
            expected: decoded_bytes,
            actual: layout.bytes as u64,
        });
    }
    let plan = Plan::new(&layout, &preamble, artifact_at)?;
    if plan.body_bytes > body.len() {
        return Err(UnpackError::Truncated);
    }
    if plan.body_bytes < body.len() {
        return Err(UnpackError::TrailingBytes);
    }

    let mut output = Vec::new();
    output
        .try_reserve_exact(decoded)
        .map_err(|_| UnpackError::Allocation(decoded_bytes))?;
    output.resize(decoded, 0);
    output[..layout.payload_start]
        .copy_from_slice(&body[artifact_at..artifact_at + layout.payload_start]);
    for &index in &layout.order {
        match plan.slots[index] {
            Slot::Stored | Slot::Hoisted => output[layout.range(index)].copy_from_slice(
                &body[plan.at[index]..plan.at[index] + layout.parts[index].length],
            ),
            Slot::Dropped => {}
            Slot::Stream(entry) => {
                let source = Source {
                    body,
                    layout: &layout,
                    plan: &plan,
                };
                let column = &preamble.columns[entry];
                source
                    .rebuild(&mut output, entry, column)
                    .map_err(|defect| UnpackError::Column {
                        table: column.table,
                        column: column.column,
                        defect,
                    })?;
            }
        }
    }
    Ok(output)
}

/// Which eligible columns the packer transforms.
#[derive(Clone, Copy, PartialEq, Eq)]
enum Rule {
    /// `fk` everywhere, `int` and `sep` by the size test, and the artifact
    /// itself when packing does not shorten it.
    Frozen,
    /// Every eligible column and never a stored body, so a test reaches each
    /// rebuilding path whatever the size test would say.
    #[cfg(test)]
    Every,
}

/// A caller's cancel, latched: once it has answered true the pack is over,
/// whatever it answers afterwards.
struct Cancel<'a> {
    asked: &'a dyn Fn() -> bool,
    hit: Cell<bool>,
}

impl<'a> Cancel<'a> {
    fn new(asked: &'a dyn Fn() -> bool) -> Self {
        Self {
            asked,
            hit: Cell::new(false),
        }
    }

    /// Asks the caller, unless it has already answered true.
    fn hit(&self) -> bool {
        if !self.hit.get() && (self.asked)() {
            self.hit.set(true);
        }
        self.hit.get()
    }

    fn check(&self) -> Result<(), PackError> {
        if self.hit() {
            Err(PackError::Cancelled)
        } else {
            Ok(())
        }
    }

    /// What the caller has answered so far, without asking again.
    fn latched(&self) -> Result<(), PackError> {
        if self.hit.get() {
            Err(PackError::Cancelled)
        } else {
            Ok(())
        }
    }
}

#[cfg(test)]
fn pack_under(artifact: &[u8], rule: Rule) -> Result<Packed, PackError> {
    pack_with(artifact, rule, &Cancel::new(&|| false))
}

fn pack_with(artifact: &[u8], rule: Rule, cancel: &Cancel) -> Result<Packed, PackError> {
    let layout = Layout::read(artifact).map_err(PackError::Artifact)?;
    if layout.bytes != artifact.len() {
        return Err(PackError::Artifact(
            "it does not end where its last part ends",
        ));
    }
    let mut end = layout.payload_start;
    for &index in &layout.order {
        let range = layout.range(index);
        if artifact[end..range.start].iter().any(|&byte| byte != 0) {
            return Err(PackError::Artifact("the padding between parts is not zero"));
        }
        end = range.end;
    }
    let bytes_of = |part: usize| &artifact[layout.range(part)];

    let mut keys: Vec<Option<Key>> = Vec::with_capacity(layout.tables.len());
    for table in 0..layout.tables.len() {
        cancel.check()?;
        keys.push(layout.key_part(table).map(|part| Key {
            part,
            bytes: bytes_of(part),
            rows: key_rows(bytes_of(part)),
        }));
    }

    let mut choices = Vec::new();
    let mut entries = Vec::new();
    let mut streams = Vec::new();
    let mut hoisted = Vec::new();
    for (table_index, table) in layout.tables.iter().enumerate() {
        for (column_index, column) in table.columns.iter().enumerate() {
            cancel.check()?;
            let candidate =
                Candidate::of(artifact, &layout, &keys, table_index, column_index, cancel);
            // A search for a foreign key's table that a cancel cut short found
            // none, and nothing is made of that.
            cancel.latched()?;
            let mut choice = ColumnChoice {
                table: table.name.clone(),
                column: column.name.clone(),
                kind: column.kind.clone(),
                transform: None,
                considered: None,
                stored_bytes: layout
                    .parts_of(table_index, column_index)
                    .map(|part| layout.parts[part].length as u64)
                    .sum(),
                stream_bytes: None,
                stored_q9_bytes: None,
                stream_q9_bytes: None,
                width: None,
                escapes: None,
                referenced_table: None,
            };
            let Some(candidate) = candidate else {
                choices.push(choice);
                continue;
            };
            choice.considered = Some(candidate.transform);
            choice.stream_bytes = Some(candidate.stream.len() as u64);
            match candidate.transform {
                Transform::Fk => {
                    choice.width = Some(candidate.width);
                    choice.escapes = Some(candidate.escapes);
                    choice.referenced_table = candidate
                        .references
                        .map(|table| layout.tables[table].name.clone());
                }
                Transform::Int => choice.width = Some(candidate.width),
                Transform::Sep => {}
            }
            let applied = match (rule, candidate.transform) {
                (Rule::Frozen, Transform::Int | Transform::Sep) => {
                    let replaced = candidate.replaced.clone().map(bytes_of);
                    let stored = quality_9_bytes(replaced, cancel)?;
                    let stream = quality_9_bytes([candidate.stream.as_slice()], cancel)?;
                    choice.stored_q9_bytes = Some(stored);
                    choice.stream_q9_bytes = Some(stream);
                    stream < stored
                }
                _ => true,
            };
            if applied {
                choice.transform = Some(candidate.transform);
                if let Some(key) = candidate
                    .references
                    .and_then(|table| keys[table].as_ref())
                    .filter(|key| {
                        layout.position[key.part] > layout.position[candidate.replaced.start]
                    })
                {
                    hoisted.push(key.part);
                }
                entries.push(ColumnEntry {
                    table: table_index as u32,
                    column: column_index as u32,
                    transform: candidate.transform,
                    references: candidate.references.map(|table| table as u32),
                    width: candidate.width,
                    escapes: candidate.escapes,
                    stream_bytes: candidate.stream.len() as u64,
                });
                streams.push(candidate.stream);
            }
            choices.push(choice);
        }
    }
    // The key maps are read by the search above alone.
    drop(keys);
    hoisted.sort_unstable();
    hoisted.dedup();

    let preamble = Preamble {
        version: BODY_VERSION,
        hoisted: hoisted
            .iter()
            .map(|&part| {
                let located = &layout.parts[part];
                HoistedEntry {
                    table: located.table as u32,
                    column: located.column as u32,
                    part: (part - layout.first[located.table][located.column]) as u32,
                }
            })
            .collect(),
        columns: entries,
    };
    let preamble_bytes = preamble.spelled();
    let preamble_length =
        u32::try_from(preamble_bytes.len()).map_err(|_| PackError::PreambleLength)?;
    let plan = Plan::new(&layout, &preamble, 12 + preamble_bytes.len())?;
    // The layout gives the body's length before a byte of it is written.
    if rule == Rule::Frozen && plan.body_bytes >= artifact.len() {
        for choice in &mut choices {
            choice.transform = None;
        }
        return Ok(Packed {
            body: artifact.to_vec(),
            stored: true,
            hoisted: Vec::new(),
            columns: choices,
            part_ends: Vec::new(),
        });
    }
    let mut body = Vec::with_capacity(plan.body_bytes);
    body.extend_from_slice(BODY_MAGIC);
    body.extend_from_slice(&preamble_length.to_le_bytes());
    body.extend_from_slice(&preamble_bytes);
    body.extend_from_slice(&artifact[..layout.payload_start]);
    for &part in &hoisted {
        body.extend_from_slice(bytes_of(part));
    }
    let mut part_ends = vec![body.len()];
    for &index in &layout.order {
        match plan.slots[index] {
            Slot::Stored => body.extend_from_slice(bytes_of(index)),
            // Each stream is freed once the body holds it.
            Slot::Stream(entry) => body.extend_from_slice(&std::mem::take(&mut streams[entry])),
            Slot::Hoisted | Slot::Dropped => continue,
        }
        part_ends.push(body.len());
    }
    debug_assert_eq!(body.len(), plan.body_bytes);

    cancel.check()?;
    // The body is published only if this module's own inverse restores the
    // artifact from it.
    if unpack(&body, artifact.len() as u64)? != artifact {
        return Err(PackError::InverseBytes);
    }
    Ok(Packed {
        body,
        stored: false,
        hoisted: hoisted
            .iter()
            .map(|&part| {
                let located = &layout.parts[part];
                HoistedPart {
                    table: layout.tables[located.table].name.clone(),
                    column: layout.tables[located.table].columns[located.column]
                        .name
                        .clone(),
                    part: part - layout.first[located.table][located.column],
                    bytes: located.length as u64,
                }
            })
            .collect(),
        columns: choices,
        part_ends,
    })
}

/// The artifact header's members that packing reads.
#[derive(Deserialize)]
struct Header {
    format: String,
    version: u32,
    tables: Vec<Table>,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct Table {
    name: String,
    rows: u32,
    primary_key: Vec<String>,
    columns: Vec<Column>,
}

#[derive(Deserialize)]
struct Column {
    name: String,
    #[serde(rename = "type")]
    kind: String,
    nullable: bool,
    encoding: Option<Encoding>,
    parts: Vec<Extent>,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct Encoding {
    kind: String,
    block_rows: u64,
}

#[derive(Deserialize)]
struct Extent {
    offset: u64,
    length: u64,
}

/// One part of an artifact.
struct Part {
    table: usize,
    column: usize,
    /// From the payload's start.
    offset: usize,
    length: usize,
}

/// An artifact's tables and where its parts lie.
struct Layout {
    tables: Vec<Table>,
    payload_start: usize,
    /// Every part in header order.
    parts: Vec<Part>,
    /// Per table and column, the index in `parts` of the column's first part.
    first: Vec<Vec<usize>>,
    /// The indexes into `parts` in payload order.
    order: Vec<usize>,
    /// Per part, its place in `order`.
    position: Vec<usize>,
    /// The artifact's length: the payload's start plus its last part's end.
    bytes: usize,
}

/// A numeric column's data parts, as indexes into [`Layout::parts`].
#[derive(Clone, Copy)]
enum IntData {
    Raw { values: usize },
    For { headers: usize, packed: usize },
}

impl Layout {
    /// Reads an artifact's framing and header from `bytes`, which begins at
    /// the artifact's first byte and holds at least all that precedes its
    /// payload.
    fn read(bytes: &[u8]) -> Result<Self, &'static str> {
        if bytes.get(..8) != Some(MAGIC.as_slice()) {
            return Err("it does not begin with the artifact's magic");
        }
        let header_end = bytes
            .get(8..12)
            .map(|length| u32::from_le_bytes(length.try_into().expect("4 bytes")) as usize)
            .and_then(|length| length.checked_add(12));
        let payload_start = header_end.and_then(align8);
        let (Some(header_end), Some(payload_start)) = (header_end, payload_start) else {
            return Err("it runs past the body");
        };
        if payload_start > bytes.len() {
            return Err("it runs past the body");
        }
        let header: Header = serde_json::from_slice(&bytes[12..header_end])
            .map_err(|_| "it is not a direct-columnar header")?;
        if header.format != FORMAT_NAME || header.version != FORMAT_VERSION {
            return Err("it is not a version 4 header");
        }
        let mut parts = Vec::new();
        let mut first = Vec::with_capacity(header.tables.len());
        for (table_index, table) in header.tables.iter().enumerate() {
            let mut table_first = Vec::with_capacity(table.columns.len());
            for (column_index, column) in table.columns.iter().enumerate() {
                table_first.push(parts.len());
                for extent in &column.parts {
                    let (Ok(offset), Ok(length)) = (
                        usize::try_from(extent.offset),
                        usize::try_from(extent.length),
                    ) else {
                        return Err("a part's extent overflows");
                    };
                    if offset.checked_add(length).is_none() {
                        return Err("a part's extent overflows");
                    }
                    parts.push(Part {
                        table: table_index,
                        column: column_index,
                        offset,
                        length,
                    });
                }
            }
            first.push(table_first);
        }
        let mut order: Vec<usize> = (0..parts.len()).collect();
        order.sort_unstable_by_key(|&part| (parts[part].offset, parts[part].length, part));
        let mut position = vec![0; parts.len()];
        let mut end = 0_usize;
        for (place, &part) in order.iter().enumerate() {
            position[part] = place;
            if Some(parts[part].offset) != align8(end) {
                return Err("a part does not follow the part before it");
            }
            end = parts[part].offset + parts[part].length;
        }
        let bytes = payload_start
            .checked_add(end)
            .ok_or("a part's extent overflows")?;
        Ok(Self {
            tables: header.tables,
            payload_start,
            parts,
            first,
            order,
            position,
            bytes,
        })
    }

    /// Where part `index` lies in the artifact.
    fn range(&self, index: usize) -> Range<usize> {
        let start = self.payload_start + self.parts[index].offset;
        start..start + self.parts[index].length
    }

    fn parts_of(&self, table: usize, column: usize) -> Range<usize> {
        let first = self.first[table][column];
        first..first + self.tables[table].columns[column].parts.len()
    }

    fn rows(&self, table: usize) -> usize {
        self.tables[table].rows as usize
    }

    /// A column's validity part and data parts, or `None` when its parts are
    /// not in payload order or a nullable column's first part is not a bitmap
    /// of its rows.
    fn shape(&self, table: usize, column: usize) -> Option<(Option<usize>, Range<usize>)> {
        let parts = self.parts_of(table, column);
        if (parts.start + 1..parts.end).any(|part| self.position[part - 1] > self.position[part]) {
            return None;
        }
        if !self.tables[table].columns[column].nullable {
            return Some((None, parts));
        }
        let validity = parts.start;
        (!parts.is_empty() && self.parts[validity].length == self.rows(table).div_ceil(8))
            .then_some((Some(validity), validity + 1..parts.end))
    }

    /// A uuid column's validity and values parts.
    fn uuid_parts(&self, table: usize, column: usize) -> Option<(Option<usize>, usize)> {
        let (validity, data) = self.shape(table, column)?;
        let values = self.rows(table).checked_mul(UUID_BYTES)?;
        (data.len() == 1 && self.parts[data.start].length == values)
            .then_some((validity, data.start))
    }

    /// An int64 or timestamp column's validity and data parts.
    fn int_parts(&self, table: usize, column: usize) -> Option<(Option<usize>, IntData)> {
        let (validity, data) = self.shape(table, column)?;
        let rows = self.rows(table);
        let data = match &self.tables[table].columns[column].encoding {
            None => (data.len() == 1 && self.parts[data.start].length == rows.checked_mul(8)?)
                .then_some(IntData::Raw { values: data.start }),
            Some(encoding) => (encoding.kind == "for"
                && encoding.block_rows == NUMERIC_BLOCK_ROWS as u64
                && data.len() == 2
                && self.parts[data.start].length
                    == rows
                        .div_ceil(NUMERIC_BLOCK_ROWS)
                        .checked_mul(NUMERIC_BLOCK_HEADER_BYTES)?)
            .then_some(IntData::For {
                headers: data.start,
                packed: data.start + 1,
            }),
        }?;
        Some((validity, data))
    }

    /// A utf8 or json column's validity, lengths and values parts.
    fn text_parts(&self, table: usize, column: usize) -> Option<(Option<usize>, usize, usize)> {
        let (validity, data) = self.shape(table, column)?;
        let lengths = self.rows(table).checked_add(1)?.checked_mul(4)?;
        (data.len() == 2 && self.parts[data.start].length == lengths).then_some((
            validity,
            data.start,
            data.start + 1,
        ))
    }

    /// The key part of a table whose primary key is one uuid column that is
    /// not nullable.
    fn key_part(&self, table: usize) -> Option<usize> {
        let [name] = self.tables[table].primary_key.as_slice() else {
            return None;
        };
        let column = self.tables[table]
            .columns
            .iter()
            .position(|column| &column.name == name)?;
        let found = &self.tables[table].columns[column];
        if found.kind != "uuid" || found.nullable {
            return None;
        }
        self.uuid_parts(table, column).map(|(_, values)| values)
    }
}

fn align8(value: usize) -> Option<usize> {
    value.checked_add(7).map(|value| value & !7)
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
struct Preamble {
    version: u64,
    hoisted: Vec<HoistedEntry>,
    columns: Vec<ColumnEntry>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
struct HoistedEntry {
    table: u32,
    column: u32,
    part: u32,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
struct ColumnEntry {
    table: u32,
    column: u32,
    transform: Transform,
    references: Option<u32>,
    width: u8,
    escapes: u64,
    stream_bytes: u64,
}

impl Preamble {
    fn read(bytes: &[u8]) -> Result<Self, UnpackError> {
        #[derive(Deserialize)]
        struct Versioned {
            version: u64,
        }
        let versioned: Versioned = serde_json::from_slice(bytes)
            .map_err(|_| UnpackError::Preamble("it is not JSON with a version"))?;
        if versioned.version != BODY_VERSION {
            return Err(UnpackError::Version(versioned.version));
        }
        let preamble: Self = serde_json::from_slice(bytes)
            .map_err(|_| UnpackError::Preamble("a member is missing or of the wrong type"))?;
        if preamble.spelled() != bytes {
            return Err(UnpackError::Preamble(
                "it is not spelled as the packer writes it",
            ));
        }
        Ok(preamble)
    }

    /// The preamble's bytes: no whitespace, members in declaration order.
    fn spelled(&self) -> Vec<u8> {
        serde_json::to_vec(self).expect("a preamble holds only integers and fixed strings")
    }
}

/// What the body holds for one part.
#[derive(Clone, Copy, PartialEq, Eq)]
enum Slot {
    /// The part, where payload order puts it.
    Stored,
    /// The part, ahead of the payload.
    Hoisted,
    /// The stream of this entry of the preamble's columns, which replaces it.
    Stream(usize),
    /// Nothing: the stream standing at an earlier part replaces it too.
    Dropped,
}

/// The parts one preamble entry's rebuilding reads and writes, as indexes into
/// [`Layout::parts`].
#[derive(Clone, Copy)]
enum Resolved {
    Fk {
        validity: Option<usize>,
        values: usize,
        key: usize,
    },
    Int {
        validity: Option<usize>,
        data: IntData,
    },
    Sep {
        validity: Option<usize>,
        lengths: usize,
        values: usize,
    },
}

/// A body's layout: what it holds for each part and where.
struct Plan {
    slots: Vec<Slot>,
    /// Where each part's stored bytes, or the stream standing for it, begin in
    /// the body. Unused for a dropped part.
    at: Vec<usize>,
    /// Per preamble column.
    columns: Vec<Resolved>,
    body_bytes: usize,
}

impl Plan {
    /// Checks the preamble against the header and lays the body out, the
    /// artifact's first bytes being at `artifact_at`.
    fn new(layout: &Layout, preamble: &Preamble, artifact_at: usize) -> Result<Self, UnpackError> {
        let mut slots = vec![Slot::Stored; layout.parts.len()];
        let mut columns = Vec::with_capacity(preamble.columns.len());
        let mut previous = None;
        for (index, entry) in preamble.columns.iter().enumerate() {
            let (table, column) = (entry.table as usize, entry.column as usize);
            if previous.is_some_and(|previous| previous >= (table, column)) {
                return Err(UnpackError::Preamble("its columns are not in header order"));
            }
            previous = Some((table, column));
            let Some(header) = layout
                .tables
                .get(table)
                .and_then(|table| table.columns.get(column))
            else {
                return Err(UnpackError::Preamble(
                    "it names a column the artifact does not have",
                ));
            };
            let unused = match entry.transform {
                Transform::Fk => false,
                Transform::Int => entry.references.is_some() || entry.escapes != 0,
                Transform::Sep => {
                    entry.references.is_some() || entry.escapes != 0 || entry.width != 0
                }
            };
            if unused {
                return Err(UnpackError::Preamble(
                    "it sets a member its transform does not use",
                ));
            }
            let defect = |defect| UnpackError::Column {
                table: entry.table,
                column: entry.column,
                defect,
            };
            let kind = header.kind.as_str();
            let resolved = match entry.transform {
                Transform::Fk => {
                    if kind != "uuid" {
                        return Err(defect(Defect::Type));
                    }
                    let (validity, values) = layout
                        .uuid_parts(table, column)
                        .ok_or(defect(Defect::Parts))?;
                    let references = entry
                        .references
                        .map(|references| references as usize)
                        .filter(|&references| {
                            references != table && references < layout.tables.len()
                        })
                        .ok_or(defect(Defect::References))?;
                    let key = layout
                        .key_part(references)
                        .ok_or(defect(Defect::References))?;
                    if entry.width != index_width(layout.tables[references].rows) {
                        return Err(defect(Defect::Width));
                    }
                    let stream = (layout.rows(table) as u64)
                        .checked_mul(u64::from(entry.width))
                        .zip(entry.escapes.checked_mul(UUID_BYTES as u64))
                        .and_then(|(planes, escapes)| planes.checked_add(escapes));
                    if stream != Some(entry.stream_bytes) {
                        return Err(defect(Defect::Rows));
                    }
                    slots[values] = Slot::Stream(index);
                    Resolved::Fk {
                        validity,
                        values,
                        key,
                    }
                }
                Transform::Int => {
                    if kind != "int64" && kind != "timestamp" {
                        return Err(defect(Defect::Type));
                    }
                    let (validity, data) = layout
                        .int_parts(table, column)
                        .ok_or(defect(Defect::Parts))?;
                    if entry.width > MAX_INT_WIDTH {
                        return Err(defect(Defect::Width));
                    }
                    let (IntData::Raw { values: replaced }
                    | IntData::For {
                        packed: replaced, ..
                    }) = data;
                    slots[replaced] = Slot::Stream(index);
                    Resolved::Int { validity, data }
                }
                Transform::Sep => {
                    if kind != "utf8" && kind != "json" {
                        return Err(defect(Defect::Type));
                    }
                    let (validity, lengths, values) = layout
                        .text_parts(table, column)
                        .ok_or(defect(Defect::Parts))?;
                    slots[lengths] = Slot::Stream(index);
                    slots[values] = Slot::Dropped;
                    Resolved::Sep {
                        validity,
                        lengths,
                        values,
                    }
                }
            };
            columns.push(resolved);
        }

        let mut hoisted = Vec::with_capacity(preamble.hoisted.len());
        let mut previous = None;
        for entry in &preamble.hoisted {
            let named = (entry.table, entry.column, entry.part);
            if previous.is_some_and(|previous| previous >= named) {
                return Err(UnpackError::Preamble(
                    "its hoisted parts are not in header order",
                ));
            }
            previous = Some(named);
            let (table, column) = (entry.table as usize, entry.column as usize);
            let part = layout
                .tables
                .get(table)
                .and_then(|table| table.columns.get(column))
                .filter(|column| (entry.part as usize) < column.parts.len())
                .map(|_| layout.first[table][column] + entry.part as usize)
                .ok_or(UnpackError::Preamble(
                    "it hoists a part the artifact does not have",
                ))?;
            if slots[part] != Slot::Stored {
                return Err(UnpackError::Preamble("it hoists a part a stream replaces"));
            }
            slots[part] = Slot::Hoisted;
            hoisted.push(part);
        }

        // A row index is resolved as its stream is read, so the key part must
        // already have been read: hoisted, or stored ahead of the stream.
        for (entry, resolved) in preamble.columns.iter().zip(&columns) {
            if let Resolved::Fk { key, values, .. } = *resolved {
                let before = match slots[key] {
                    Slot::Hoisted => true,
                    Slot::Stored => layout.position[key] < layout.position[values],
                    Slot::Stream(_) | Slot::Dropped => false,
                };
                if !before {
                    return Err(UnpackError::Column {
                        table: entry.table,
                        column: entry.column,
                        defect: Defect::References,
                    });
                }
            }
        }

        let mut at = vec![0; layout.parts.len()];
        let mut cursor = artifact_at.checked_add(layout.payload_start);
        let mut place = |part: usize, length: Option<usize>| {
            at[part] = cursor.unwrap_or(usize::MAX);
            cursor = cursor
                .zip(length)
                .and_then(|(cursor, length)| cursor.checked_add(length));
        };
        for &part in &hoisted {
            place(part, Some(layout.parts[part].length));
        }
        for &part in &layout.order {
            match slots[part] {
                Slot::Stored => place(part, Some(layout.parts[part].length)),
                Slot::Stream(entry) => place(
                    part,
                    usize::try_from(preamble.columns[entry].stream_bytes).ok(),
                ),
                Slot::Hoisted | Slot::Dropped => {}
            }
        }
        Ok(Self {
            slots,
            at,
            columns,
            // A length past `usize` is past any body.
            body_bytes: cursor.ok_or(UnpackError::Truncated)?,
        })
    }
}

/// The fewest bytes, at least one, that hold a table's row count.
fn index_width(rows: u32) -> u8 {
    match rows {
        0..=0xff => 1,
        0x100..=0xffff => 2,
        0x1_0000..=0xff_ffff => 3,
        _ => 4,
    }
}

/// A validity bitmap, or none for a column that is not nullable.
#[derive(Clone, Copy)]
struct Validity<'a>(Option<&'a [u8]>);

impl Validity<'_> {
    fn present(self, row: usize) -> bool {
        self.0
            .is_none_or(|bits| bits[row >> 3] & (1 << (row & 7)) != 0)
    }

    /// The present rows among the first `rows`.
    fn count(self, rows: usize) -> usize {
        let Some(bits) = self.0 else {
            return rows;
        };
        let whole = rows / 8;
        let mut count = bits[..whole]
            .iter()
            .map(|byte| byte.count_ones() as usize)
            .sum();
        if !rows.is_multiple_of(8) {
            count += (bits[whole] & ((1 << (rows % 8)) - 1)).count_ones() as usize;
        }
        count
    }

    /// Whether any of the `count` rows from `start`, a multiple of 8, is
    /// present.
    fn any(self, start: usize, count: usize) -> bool {
        let Some(bits) = self.0 else {
            return count != 0;
        };
        let (from, whole) = (start / 8, count / 8);
        bits[from..from + whole].iter().any(|&byte| byte != 0)
            || (!count.is_multiple_of(8) && bits[from + whole] & ((1 << (count % 8)) - 1) != 0)
    }
}

/// A body whose layout has been checked.
struct Source<'a> {
    body: &'a [u8],
    layout: &'a Layout,
    plan: &'a Plan,
}

impl<'a> Source<'a> {
    /// A stored or hoisted part's bytes in the body.
    fn stored(&self, part: usize) -> &'a [u8] {
        &self.body[self.plan.at[part]..self.plan.at[part] + self.layout.parts[part].length]
    }

    /// Writes the parts that entry `index` of the preamble's columns replaces.
    fn rebuild(&self, output: &mut [u8], index: usize, entry: &ColumnEntry) -> Result<(), Defect> {
        let layout = self.layout;
        let rows = layout.rows(entry.table as usize);
        let validity = |part: Option<usize>| Validity(part.map(|part| self.stored(part)));
        let stream = |part: usize| {
            &self.body[self.plan.at[part]..self.plan.at[part] + entry.stream_bytes as usize]
        };
        let width = usize::from(entry.width);
        match self.plan.columns[index] {
            Resolved::Fk {
                validity: bitmap,
                values,
                key,
            } => rebuild_fk(
                &mut output[layout.range(values)],
                stream(values),
                validity(bitmap),
                self.stored(key),
                width,
            ),
            Resolved::Int {
                validity: bitmap,
                data,
            } => {
                let validity = validity(bitmap);
                match data {
                    IntData::Raw { values } => rebuild_raw(
                        &mut output[layout.range(values)],
                        Deltas::new(stream(values), validity.count(rows), width)?,
                        validity,
                    ),
                    IntData::For { headers, packed } => rebuild_for(
                        &mut output[layout.range(packed)],
                        Deltas::new(stream(packed), validity.count(rows), width)?,
                        validity,
                        self.stored(headers),
                        rows,
                    ),
                }
            }
            Resolved::Sep {
                validity: bitmap,
                lengths,
                values,
            } => {
                // The lengths part precedes the values part.
                let (head, tail) = output.split_at_mut(layout.range(values).start);
                rebuild_sep(
                    &mut head[layout.range(lengths)],
                    &mut tail[..layout.parts[values].length],
                    stream(lengths),
                    validity(bitmap),
                )
            }
        }
    }
}

/// Writes a uuid column's values part, `16 * rows` bytes, from its `fk`
/// stream, `rows * width` bytes of planes and then the escapes.
fn rebuild_fk(
    values: &mut [u8],
    stream: &[u8],
    validity: Validity,
    key: &[u8],
    width: usize,
) -> Result<(), Defect> {
    let rows = values.len() / UUID_BYTES;
    let key_rows = key.len() / UUID_BYTES;
    let (planes, mut escapes) = stream.split_at(rows * width);
    for (row, value) in values.chunks_exact_mut(UUID_BYTES).enumerate() {
        let code = (0..width).fold(0_usize, |code, plane| {
            code | usize::from(planes[plane * rows + row]) << (8 * plane)
        });
        if !validity.present(row) {
            if code != 0 {
                return Err(Defect::NullRow);
            }
            value.fill(0);
        } else if code < key_rows {
            value.copy_from_slice(&key[code * UUID_BYTES..(code + 1) * UUID_BYTES]);
        } else if code == key_rows {
            let Some((escape, rest)) = escapes.split_at_checked(UUID_BYTES) else {
                return Err(Defect::EscapePastEscapes);
            };
            value.copy_from_slice(escape);
            escapes = rest;
        } else {
            return Err(Defect::RowIndex);
        }
    }
    if !escapes.is_empty() {
        return Err(Defect::UnusedEscapes);
    }
    Ok(())
}

/// The present rows' values of an `int` stream, in row order.
struct Deltas<'a> {
    planes: &'a [u8],
    count: usize,
    width: usize,
    next: usize,
    previous: i64,
}

impl<'a> Deltas<'a> {
    /// `stream` must be `count` deltas of `width` bytes, `width` at most 7.
    fn new(stream: &'a [u8], count: usize, width: usize) -> Result<Self, Defect> {
        if count.checked_mul(width) != Some(stream.len()) {
            return Err(Defect::Rows);
        }
        Ok(Self {
            planes: stream,
            count,
            width,
            next: 0,
            previous: 0,
        })
    }

    /// The next present row's value. Called once per present row, except
    /// that a stream without planes may be left unread: its values are all 0.
    fn value(&mut self) -> Result<i64, Defect> {
        let zigzag = (0..self.width).fold(0_u64, |zigzag, plane| {
            zigzag | u64::from(self.planes[plane * self.count + self.next]) << (8 * plane)
        });
        self.next += 1;
        let delta = (zigzag >> 1) as i64 ^ -((zigzag & 1) as i64);
        // Below 2^51 plus below 2^56: no overflow.
        let value = self.previous + delta;
        if value <= -INT_LIMIT || value >= INT_LIMIT {
            return Err(Defect::Value);
        }
        self.previous = value;
        Ok(value)
    }
}

/// Writes the values part of a numeric column without an encoding: an `i64`
/// per row.
fn rebuild_raw(values: &mut [u8], mut deltas: Deltas, validity: Validity) -> Result<(), Defect> {
    for (row, bytes) in values.chunks_exact_mut(8).enumerate() {
        let value = if validity.present(row) {
            deltas.value()?
        } else {
            0
        };
        bytes.copy_from_slice(&value.to_le_bytes());
    }
    Ok(())
}

/// Writes a frame-of-reference column's packed part, each block at the offset,
/// base, width and scale its stored header gives.
fn rebuild_for(
    packed: &mut [u8],
    mut deltas: Deltas,
    validity: Validity,
    headers: &[u8],
    rows: usize,
) -> Result<(), Defect> {
    let mut at = 0_usize;
    for (block, header) in headers.chunks_exact(NUMERIC_BLOCK_HEADER_BYTES).enumerate() {
        let start = block * NUMERIC_BLOCK_ROWS;
        let count = (rows - start).min(NUMERIC_BLOCK_ROWS);
        let offset = u32::from_le_bytes(header[..4].try_into().expect("4 bytes"));
        let base = i64::from_le_bytes(header[4..12].try_into().expect("8 bytes"));
        let bits = u32::from(header[12]);
        if offset as usize != at {
            return Err(Defect::Block);
        }
        if bits == 64 {
            let block = packed
                .get_mut(at..at + count * 8)
                .ok_or(Defect::PartLength)?;
            for (row, bytes) in block.chunks_exact_mut(8).enumerate() {
                let value = if validity.present(start + row) {
                    deltas.value()?
                } else {
                    0
                };
                bytes.copy_from_slice(&value.to_le_bytes());
            }
            at += count * 8;
            continue;
        }
        let unit = match header[13] {
            0 => 1,
            3 => 1_000,
            _ => return Err(Defect::Block),
        };
        if bits > 64 || base <= -INT_LIMIT || base >= INT_LIMIT {
            return Err(Defect::Block);
        }
        if bits == 0 && deltas.width == 0 {
            // No bits and no planes: every present row's value is 0, the
            // block reads nothing and writes nothing, and its base is all
            // there is to hold to its rows. A body can name 2^32 - 1 such
            // rows in a few bytes, so they are not walked.
            if base != 0 && validity.any(start, count) {
                return Err(Defect::Block);
            }
            continue;
        }
        let length = (count * bits as usize).div_ceil(8);
        let block = packed.get_mut(at..at + length).ok_or(Defect::PartLength)?;
        let mut bytes = block.iter_mut();
        let mut accumulator = 0_u128;
        let mut filled = 0_u32;
        for row in start..start + count {
            if validity.present(row) {
                let value = deltas.value()?;
                // Both are below 2^51 from zero, so the difference does not
                // overflow, and one below the base is too wide as unsigned.
                let offset = (value / unit - base) as u64;
                if value % unit != 0 || offset >> bits != 0 {
                    return Err(Defect::Block);
                }
                accumulator |= u128::from(offset) << filled;
            }
            filled += bits;
            while filled >= 8 {
                *bytes.next().expect("the block's length") = accumulator as u8;
                accumulator >>= 8;
                filled -= 8;
            }
        }
        if filled != 0 {
            *bytes.next().expect("the block's length") = accumulator as u8;
        }
        at += length;
    }
    if at != packed.len() {
        return Err(Defect::PartLength);
    }
    Ok(())
}

/// Writes a text column's lengths part, `4 * (rows + 1)` bytes, and values
/// part from its `sep` stream.
fn rebuild_sep(
    lengths: &mut [u8],
    values: &mut [u8],
    stream: &[u8],
    validity: Validity,
) -> Result<(), Defect> {
    let rows = lengths.len() / 4 - 1;
    let separators = stream.iter().filter(|&&byte| byte == SEPARATOR).count();
    if separators != validity.count(rows) {
        return Err(Defect::Separators);
    }
    if stream.len() - separators != values.len() {
        return Err(Defect::PartLength);
    }
    lengths[..4].fill(0);
    let mut rest = stream;
    let mut written = 0_usize;
    for (row, length) in lengths[4..].chunks_exact_mut(4).enumerate() {
        let mut bytes = 0_u32;
        if validity.present(row) {
            let end = rest
                .iter()
                .position(|&byte| byte == SEPARATOR)
                .expect("a separator per present row");
            bytes = u32::try_from(end).map_err(|_| Defect::PartLength)?;
            values[written..written + end].copy_from_slice(&rest[..end]);
            written += end;
            rest = &rest[end + 1..];
        }
        length.copy_from_slice(&bytes.to_le_bytes());
    }
    // Bytes after the last separator belong to no row.
    if !rest.is_empty() {
        return Err(Defect::PartLength);
    }
    Ok(())
}

/// A table's key part and the first row holding each key.
struct Key<'a> {
    part: usize,
    bytes: &'a [u8],
    rows: HashMap<[u8; UUID_BYTES], u32>,
}

fn key_rows(key: &[u8]) -> HashMap<[u8; UUID_BYTES], u32> {
    let mut rows = HashMap::with_capacity(key.len() / UUID_BYTES);
    for (row, value) in key.chunks_exact(UUID_BYTES).enumerate() {
        rows.entry(value.try_into().expect("16 bytes"))
            .or_insert(row as u32);
    }
    rows
}

/// A transform a column is eligible for, with the stream it would put in the
/// body.
struct Candidate {
    transform: Transform,
    references: Option<usize>,
    width: u8,
    escapes: u64,
    stream: Vec<u8>,
    /// The parts the stream replaces.
    replaced: Range<usize>,
}

impl Candidate {
    /// The transform column `column` of table `table` qualifies for, if its
    /// stream rebuilds the parts it replaces exactly.
    fn of(
        artifact: &[u8],
        layout: &Layout,
        keys: &[Option<Key>],
        table: usize,
        column: usize,
        cancel: &Cancel,
    ) -> Option<Self> {
        let header = &layout.tables[table].columns[column];
        let rows = layout.rows(table);
        let bytes_of = |part: usize| &artifact[layout.range(part)];
        let validity = |part: Option<usize>| Validity(part.map(bytes_of));
        match header.kind.as_str() {
            "uuid" => {
                if layout.tables[table].primary_key.as_slice() == [header.name.as_str()] {
                    return None;
                }
                let (bitmap, values) = layout.uuid_parts(table, column)?;
                let (validity, stored) = (validity(bitmap), bytes_of(values));
                let present = validity.count(rows);
                if present == 0 {
                    return None;
                }
                // `100 * found >= 99 * present` once this many are found, and
                // never once more than the rest are missed.
                let needed = (present as u128 * 99).div_ceil(100) as usize;
                let allowed = present - needed;
                let (references, key) = keys.iter().enumerate().find_map(|(references, key)| {
                    let key = key.as_ref().filter(|_| references != table)?;
                    if cancel.hit() {
                        return None;
                    }
                    let (mut found, mut missed) = (0_usize, 0_usize);
                    for (row, value) in stored.chunks_exact(UUID_BYTES).enumerate() {
                        if !validity.present(row) {
                            continue;
                        }
                        if key.rows.contains_key(value) {
                            found += 1;
                            if found >= needed {
                                return Some((references, key));
                            }
                        } else {
                            missed += 1;
                            if missed > allowed {
                                return None;
                            }
                        }
                    }
                    None
                })?;
                let key_rows = layout.tables[references].rows;
                let width = index_width(key_rows);
                let mut stream = vec![0; rows * usize::from(width)];
                let mut escapes = Vec::new();
                for (row, value) in stored.chunks_exact(UUID_BYTES).enumerate() {
                    if !validity.present(row) {
                        continue;
                    }
                    let code = key.rows.get(value).copied().unwrap_or_else(|| {
                        escapes.extend_from_slice(value);
                        key_rows
                    });
                    for plane in 0..usize::from(width) {
                        stream[plane * rows + row] = (code >> (8 * plane)) as u8;
                    }
                }
                let escaped = (escapes.len() / UUID_BYTES) as u64;
                stream.extend_from_slice(&escapes);
                let mut rebuilt = vec![0; stored.len()];
                rebuild_fk(
                    &mut rebuilt,
                    &stream,
                    validity,
                    key.bytes,
                    usize::from(width),
                )
                .ok()?;
                (rebuilt == stored).then_some(Self {
                    transform: Transform::Fk,
                    references: Some(references),
                    width,
                    escapes: escaped,
                    stream,
                    replaced: values..values + 1,
                })
            }
            "int64" | "timestamp" => {
                let (bitmap, data) = layout.int_parts(table, column)?;
                let validity = validity(bitmap);
                let (values, replaced) = match data {
                    IntData::Raw { values } => (read_raw(bytes_of(values), validity)?, values),
                    IntData::For { headers, packed } => (
                        read_for(bytes_of(headers), bytes_of(packed), validity, rows)?,
                        packed,
                    ),
                };
                let (stream, width) = int_stream(&values);
                let deltas = Deltas::new(&stream, values.len(), usize::from(width)).ok()?;
                let stored = bytes_of(replaced);
                let mut rebuilt = vec![0; stored.len()];
                match data {
                    IntData::Raw { .. } => rebuild_raw(&mut rebuilt, deltas, validity),
                    IntData::For { headers, .. } => {
                        rebuild_for(&mut rebuilt, deltas, validity, bytes_of(headers), rows)
                    }
                }
                .ok()?;
                (rebuilt == stored).then_some(Self {
                    transform: Transform::Int,
                    references: None,
                    width,
                    escapes: 0,
                    stream,
                    replaced: replaced..replaced + 1,
                })
            }
            "utf8" | "json" => {
                let (bitmap, lengths, values) = layout.text_parts(table, column)?;
                let validity = validity(bitmap);
                let (stored_lengths, stored_values) = (bytes_of(lengths), bytes_of(values));
                if stored_values.contains(&SEPARATOR) {
                    return None;
                }
                let mut stream = Vec::with_capacity(stored_values.len() + validity.count(rows));
                let mut rest = stored_values;
                for (row, length) in stored_lengths[4..].chunks_exact(4).enumerate() {
                    let length = u32::from_le_bytes(length.try_into().expect("4 bytes")) as usize;
                    let (value, after) = rest.split_at_checked(length)?;
                    rest = after;
                    if validity.present(row) {
                        stream.extend_from_slice(value);
                        stream.push(SEPARATOR);
                    }
                }
                let mut rebuilt_lengths = vec![0; stored_lengths.len()];
                let mut rebuilt_values = vec![0; stored_values.len()];
                rebuild_sep(&mut rebuilt_lengths, &mut rebuilt_values, &stream, validity).ok()?;
                (rebuilt_lengths == stored_lengths && rebuilt_values == stored_values).then_some(
                    Self {
                        transform: Transform::Sep,
                        references: None,
                        width: 0,
                        escapes: 0,
                        stream,
                        replaced: lengths..values + 1,
                    },
                )
            }
            _ => None,
        }
    }
}

fn within_limit(value: i128) -> Option<i64> {
    (value > -i128::from(INT_LIMIT) && value < i128::from(INT_LIMIT)).then_some(value as i64)
}

/// The present rows' values of a numeric column without an encoding, or
/// `None` when one is 2^51 or more from zero.
fn read_raw(values: &[u8], validity: Validity) -> Option<Vec<i64>> {
    values
        .chunks_exact(8)
        .enumerate()
        .filter(|(row, _)| validity.present(*row))
        .map(|(_, bytes)| {
            within_limit(i64::from_le_bytes(bytes.try_into().expect("8 bytes")).into())
        })
        .collect()
}

/// The present rows' values of a frame-of-reference column, or `None` when
/// one is 2^51 or more from zero or a block cannot be read.
fn read_for(headers: &[u8], packed: &[u8], validity: Validity, rows: usize) -> Option<Vec<i64>> {
    let mut values = Vec::with_capacity(validity.count(rows));
    for (block, header) in headers.chunks_exact(NUMERIC_BLOCK_HEADER_BYTES).enumerate() {
        let start = block * NUMERIC_BLOCK_ROWS;
        let count = (rows - start).min(NUMERIC_BLOCK_ROWS);
        let offset = u32::from_le_bytes(header[..4].try_into().expect("4 bytes")) as usize;
        let base = i64::from_le_bytes(header[4..12].try_into().expect("8 bytes"));
        let bits = u32::from(header[12]);
        let mut bytes = packed.get(offset..)?.iter();
        if bits == 64 {
            for row in start..start + count {
                let mut value = [0; 8];
                for byte in &mut value {
                    *byte = *bytes.next()?;
                }
                if validity.present(row) {
                    values.push(within_limit(i64::from_le_bytes(value).into())?);
                }
            }
            continue;
        }
        let unit = match header[13] {
            0 => 1,
            3 => 1_000,
            _ => return None,
        };
        if bits > 64 {
            return None;
        }
        let mut accumulator = 0_u128;
        let mut filled = 0_u32;
        for row in start..start + count {
            while filled < bits {
                accumulator |= u128::from(*bytes.next()?) << filled;
                filled += 8;
            }
            let offset = accumulator & ((1 << bits) - 1);
            accumulator >>= bits;
            filled -= bits;
            if validity.present(row) {
                values.push(within_limit((i128::from(base) + offset as i128) * unit)?);
            }
        }
    }
    Some(values)
}

/// The `int` stream of the present rows' values, and its width.
fn int_stream(values: &[i64]) -> (Vec<u8>, u8) {
    let mut previous = 0;
    let zigzags: Vec<u64> = values
        .iter()
        .map(|&value| {
            let delta = value - previous;
            previous = value;
            ((delta << 1) ^ (delta >> 63)) as u64
        })
        .collect();
    let largest = zigzags.iter().copied().max().unwrap_or(0);
    let width = (u64::BITS - largest.leading_zeros()).div_ceil(8) as usize;
    let mut stream = vec![0; zigzags.len() * width];
    for (plane, bytes) in stream.chunks_exact_mut(zigzags.len().max(1)).enumerate() {
        for (byte, zigzag) in bytes.iter_mut().zip(&zigzags) {
            *byte = (zigzag >> (8 * plane)) as u8;
        }
    }
    (stream, width as u8)
}

/// The length of `pieces`, end to end, as the size test's brotli stream.
fn quality_9_bytes<'a>(
    pieces: impl IntoIterator<Item = &'a [u8]>,
    cancel: &Cancel,
) -> Result<u64, PackError> {
    struct Count(u64);
    impl Write for Count {
        fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
            self.0 += bytes.len() as u64;
            Ok(bytes.len())
        }

        fn flush(&mut self) -> std::io::Result<()> {
            Ok(())
        }
    }
    struct Pieces<'a, 'c>(Vec<&'a [u8]>, &'a Cancel<'c>);
    impl Read for Pieces<'_, '_> {
        fn read(&mut self, buffer: &mut [u8]) -> std::io::Result<usize> {
            // The encoder reads a few kilobytes at a time, so a cancel ends a
            // size test within one of its blocks.
            if self.1.hit() {
                return Err(std::io::Error::other("the pack was cancelled"));
            }
            while let Some(piece) = self.0.first_mut() {
                if piece.is_empty() {
                    self.0.remove(0);
                    continue;
                }
                return piece.read(buffer);
            }
            Ok(0)
        }
    }
    let pieces: Vec<&[u8]> = pieces.into_iter().collect();
    let params = BrotliEncoderParams {
        quality: SIZE_TEST_QUALITY,
        lgwin: SIZE_TEST_LGWIN,
        size_hint: pieces.iter().map(|piece| piece.len()).sum(),
        ..BrotliEncoderParams::default()
    };
    let mut count = Count(0);
    brotli::BrotliCompress(&mut Pieces(pieces, cancel), &mut count, &params)
        .map_err(|error| cancel.latched().err().unwrap_or(PackError::Brotli(error)))?;
    Ok(count.0)
}

#[cfg(test)]
mod tests;
