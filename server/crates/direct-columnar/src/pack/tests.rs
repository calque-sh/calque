//! The packer and its inverse: each transform's round trip with the size test
//! set aside, and one test for each body [`unpack`] refuses.

use std::{
    cell::Cell,
    sync::OnceLock,
    time::{Duration, Instant},
};

use direct_columnar::{ColumnKind, Value};
use serde_json::json;

use super::*;

#[path = "../../tests/support/mod.rs"]
mod support;

use support::{encode, table, Random};

fn restores(body: &[u8], artifact: &[u8]) {
    assert!(unpack(body, artifact.len() as u64).unwrap() == artifact);
}

/// Packs `artifact` under the frozen rule and with every eligible column
/// transformed, checks that both bodies restore it, and returns the second.
fn round_trip(artifact: &[u8]) -> Packed {
    let frozen = pack(artifact).unwrap();
    assert!(frozen.body.len() <= artifact.len());
    assert_eq!(frozen.stored, frozen.body == artifact);
    restores(&frozen.body, artifact);
    assert!(pack(artifact).unwrap() == frozen);

    let every = pack_under(artifact, Rule::Every).unwrap();
    assert!(every.body.starts_with(BODY_MAGIC));
    restores(&every.body, artifact);
    for column in &every.columns {
        assert_eq!(column.transform, column.considered);
    }
    assert_eq!(
        every
            .columns
            .iter()
            .map(|column| column.considered)
            .collect::<Vec<_>>(),
        frozen
            .columns
            .iter()
            .map(|column| column.considered)
            .collect::<Vec<_>>()
    );
    every
}

fn choice<'a>(packed: &'a Packed, table: &str, column: &str) -> &'a ColumnChoice {
    packed
        .columns
        .iter()
        .find(|choice| choice.table == table && choice.column == column)
        .unwrap()
}

fn hoisted(packed: &Packed) -> Vec<(&str, &str, usize)> {
    packed
        .hoisted
        .iter()
        .map(|part| (part.table.as_str(), part.column.as_str(), part.part))
        .collect()
}

/// A column written by hand: its parts as the payload holds them.
struct ForgedColumn {
    name: &'static str,
    kind: &'static str,
    nullable: bool,
    encoded: bool,
    parts: Vec<Vec<u8>>,
    /// The order the header lists the parts in, when not the payload's.
    listed: Option<Vec<usize>>,
}

struct ForgedTable {
    name: &'static str,
    rows: u32,
    primary_key: &'static [&'static str],
    columns: Vec<ForgedColumn>,
}

fn forged(name: &'static str, kind: &'static str, parts: Vec<Vec<u8>>) -> ForgedColumn {
    ForgedColumn {
        name,
        kind,
        nullable: false,
        encoded: false,
        parts,
        listed: None,
    }
}

fn forged_nullable(name: &'static str, kind: &'static str, parts: Vec<Vec<u8>>) -> ForgedColumn {
    ForgedColumn {
        nullable: true,
        ..forged(name, kind, parts)
    }
}

fn encoded(column: ForgedColumn) -> ForgedColumn {
    ForgedColumn {
        encoded: true,
        ..column
    }
}

/// An artifact holding `tables` in the order given, part after part.
fn forge(tables: &[ForgedTable]) -> Vec<u8> {
    let mut payload = Vec::new();
    let tables: Vec<serde_json::Value> = tables
        .iter()
        .map(|table| {
            let columns: Vec<serde_json::Value> = table
                .columns
                .iter()
                .map(|column| {
                    let extents: Vec<serde_json::Value> = column
                        .parts
                        .iter()
                        .map(|part| {
                            payload.resize(payload.len().next_multiple_of(8), 0);
                            let offset = payload.len();
                            payload.extend_from_slice(part);
                            json!({"offset": offset, "length": part.len()})
                        })
                        .collect();
                    let parts: Vec<serde_json::Value> = match &column.listed {
                        Some(listed) => listed.iter().map(|&part| extents[part].clone()).collect(),
                        None => extents,
                    };
                    let mut header = json!({
                        "name": column.name,
                        "type": column.kind,
                        "nullable": column.nullable,
                        "parts": parts,
                    });
                    if column.encoded {
                        header["encoding"] = json!({"kind": "for", "blockRows": 1024});
                    }
                    header
                })
                .collect();
            json!({
                "name": table.name,
                "rows": table.rows,
                "primaryKey": table.primary_key,
                "columns": columns,
            })
        })
        .collect();
    frame(
        &json!({"format": "calque-direct-columnar", "version": 4, "tables": tables}),
        &payload,
    )
}

/// An artifact of this header and payload.
fn frame(header: &serde_json::Value, payload: &[u8]) -> Vec<u8> {
    let header = serde_json::to_vec(header).unwrap();
    let mut artifact = MAGIC.to_vec();
    artifact.extend_from_slice(&(header.len() as u32).to_le_bytes());
    artifact.extend_from_slice(&header);
    artifact.resize(artifact.len().next_multiple_of(8), 0);
    artifact.extend_from_slice(payload);
    artifact
}

fn bitmap(present: &[bool]) -> Vec<u8> {
    let mut bytes = vec![0; present.len().div_ceil(8)];
    for (row, _) in present.iter().enumerate().filter(|(_, present)| **present) {
        bytes[row / 8] |= 1 << (row % 8);
    }
    bytes
}

fn int64s(values: &[i64]) -> Vec<u8> {
    values
        .iter()
        .flat_map(|value| value.to_le_bytes())
        .collect()
}

fn uuids(values: &[[u8; 16]]) -> Vec<u8> {
    values.concat()
}

/// A text column's lengths and values parts.
fn texts(values: &[&[u8]]) -> [Vec<u8>; 2] {
    let mut lengths = vec![0; 4];
    for value in values {
        lengths.extend_from_slice(&(value.len() as u32).to_le_bytes());
    }
    [lengths, values.concat()]
}

/// One frame-of-reference block: its header's base, width and scale, and each
/// row's number, the value itself at width 64.
struct Block {
    base: i64,
    bits: u8,
    scale: u8,
    numbers: Vec<u64>,
}

/// A frame-of-reference column's block headers and packed parts, packed a bit
/// at a time.
fn blocks(blocks: &[Block]) -> [Vec<u8>; 2] {
    let (mut headers, mut packed) = (Vec::new(), Vec::new());
    for block in blocks {
        headers.extend_from_slice(&(packed.len() as u32).to_le_bytes());
        headers.extend_from_slice(&block.base.to_le_bytes());
        headers.extend_from_slice(&[block.bits, block.scale, 0, 0]);
        if block.bits == 64 {
            for number in &block.numbers {
                packed.extend_from_slice(&number.to_le_bytes());
            }
            continue;
        }
        let bits = usize::from(block.bits);
        let start = packed.len();
        packed.resize(start + (block.numbers.len() * bits).div_ceil(8), 0);
        for (row, number) in block.numbers.iter().enumerate() {
            for bit in (0..bits).filter(|bit| number >> bit & 1 == 1) {
                let at = row * bits + bit;
                packed[start + at / 8] |= 1 << (at % 8);
            }
        }
    }
    [headers, packed]
}

/// The header as JSON and the payload's start, read without the module's
/// checks.
fn header_of(artifact: &[u8]) -> (serde_json::Value, usize) {
    let length = u32::from_le_bytes(artifact[8..12].try_into().unwrap()) as usize;
    (
        serde_json::from_slice(&artifact[12..12 + length]).unwrap(),
        (12 + length).next_multiple_of(8),
    )
}

/// A packed body's first pieces: magic, preamble length and preamble.
fn framed(preamble: &[u8], rest: &[u8]) -> Vec<u8> {
    let mut body = BODY_MAGIC.to_vec();
    body.extend_from_slice(&(preamble.len() as u32).to_le_bytes());
    body.extend_from_slice(preamble);
    body.extend_from_slice(rest);
    body
}

const NO_COLUMNS: &[u8] = br#"{"version":1,"hoisted":[],"columns":[]}"#;

/// The body `spec/packed-artifact-v1.md` describes for these pieces, put together
/// with none of the module's checks: the hoisted parts, then every other part
/// in payload order, a stream standing for its column's last part, or for a
/// `sep` column's last two.
fn assemble(artifact: &[u8], preamble: &Preamble, streams: &[Vec<u8>]) -> Vec<u8> {
    let (header, payload_start) = header_of(artifact);
    let mut parts = Vec::new();
    for (table, listed) in header["tables"].as_array().unwrap().iter().enumerate() {
        for (column, listed) in listed["columns"].as_array().unwrap().iter().enumerate() {
            let listed = listed["parts"].as_array().unwrap();
            for (part, extent) in listed.iter().enumerate() {
                parts.push((
                    (table, column, part),
                    extent["offset"].as_u64().unwrap() as usize,
                    extent["length"].as_u64().unwrap() as usize,
                    listed.len(),
                ));
            }
        }
    }
    let stored = |offset: usize, length: usize| &artifact[payload_start + offset..][..length];
    let hoisted: Vec<(usize, usize, usize)> = preamble
        .hoisted
        .iter()
        .map(|part| {
            (
                part.table as usize,
                part.column as usize,
                part.part as usize,
            )
        })
        .collect();

    let mut body = framed(&preamble.spelled(), &artifact[..payload_start]);
    for named in &hoisted {
        if let Some(&(_, offset, length, _)) = parts.iter().find(|part| part.0 == *named) {
            body.extend_from_slice(stored(offset, length));
        }
    }
    parts.sort_by_key(|&(index, offset, length, _)| (offset, length, index));
    for (index, offset, length, count) in parts {
        if hoisted.contains(&index) {
            continue;
        }
        let entry = preamble
            .columns
            .iter()
            .position(|entry| (entry.table as usize, entry.column as usize) == (index.0, index.1));
        let Some(entry) = entry else {
            body.extend_from_slice(stored(offset, length));
            continue;
        };
        let replaced = match preamble.columns[entry].transform {
            Transform::Sep => count.saturating_sub(2),
            Transform::Fk | Transform::Int => count.saturating_sub(1),
        };
        if index.2 < replaced {
            body.extend_from_slice(stored(offset, length));
        } else if index.2 == replaced {
            body.extend_from_slice(&streams[entry]);
        }
    }
    body
}

/// A packed body taken apart, to be changed and put together again.
#[derive(Clone)]
struct Taken {
    artifact: Vec<u8>,
    preamble: Preamble,
    /// Per entry of the preamble's columns.
    streams: Vec<Vec<u8>>,
}

impl Taken {
    /// The body of `artifact` with every eligible column transformed.
    fn of(artifact: &[u8]) -> Self {
        let body = pack_under(artifact, Rule::Every).unwrap().body;
        let length = u32::from_le_bytes(body[8..12].try_into().unwrap()) as usize;
        let preamble = Preamble::read(&body[12..12 + length]).unwrap();
        let layout = Layout::read(artifact).unwrap();
        let plan = Plan::new(&layout, &preamble, 12 + length).unwrap();
        let streams = preamble
            .columns
            .iter()
            .enumerate()
            .map(|(entry, column)| {
                let part = plan
                    .slots
                    .iter()
                    .position(|slot| *slot == Slot::Stream(entry))
                    .unwrap();
                body[plan.at[part]..][..column.stream_bytes as usize].to_vec()
            })
            .collect();
        let taken = Self {
            artifact: artifact.to_vec(),
            preamble,
            streams,
        };
        // The documented layout, put together by the test, is the packer's.
        assert!(taken.body() == body);
        taken
    }

    fn body(&self) -> Vec<u8> {
        assemble(&self.artifact, &self.preamble, &self.streams)
    }

    fn unpacked(&self) -> Result<Vec<u8>, UnpackError> {
        unpack(&self.body(), self.artifact.len() as u64)
    }

    fn refusal(&self) -> UnpackError {
        refusal(&self.body(), self.artifact.len() as u64)
    }

    /// The indexes of a table and one of its columns.
    fn named(&self, table: &str, column: &str) -> (u32, u32) {
        let (header, _) = header_of(&self.artifact);
        let tables = header["tables"].as_array().unwrap();
        let at = tables.iter().position(|listed| listed["name"] == table);
        let columns = tables[at.unwrap()]["columns"].as_array().unwrap();
        let column = columns.iter().position(|listed| listed["name"] == column);
        (at.unwrap() as u32, column.unwrap() as u32)
    }

    /// The index of a column's entry among the preamble's columns.
    fn entry(&self, table: &str, column: &str) -> usize {
        let named = self.named(table, column);
        self.preamble
            .columns
            .iter()
            .position(|entry| (entry.table, entry.column) == named)
            .unwrap()
    }

    /// Where one of a column's parts lies in the artifact.
    fn part(&self, table: &str, column: &str, part: usize) -> Range<usize> {
        let (table, column) = self.named(table, column);
        let (header, payload_start) = header_of(&self.artifact);
        let extent = &header["tables"][table as usize]["columns"][column as usize]["parts"][part];
        let start = payload_start + extent["offset"].as_u64().unwrap() as usize;
        start..start + extent["length"].as_u64().unwrap() as usize
    }

    /// Replaces a column's stream and the length the preamble gives it.
    fn restream(&mut self, entry: usize, stream: Vec<u8>) {
        self.preamble.columns[entry].stream_bytes = stream.len() as u64;
        self.streams[entry] = stream;
    }

    fn defect(&self, table: &str, column: &str, defect: Defect) -> UnpackError {
        let (table, column) = self.named(table, column);
        UnpackError::Column {
            table,
            column,
            defect,
        }
    }
}

/// The error `unpack` gives, without printing the artifact when it gives
/// none.
fn refusal(body: &[u8], decoded_bytes: u64) -> UnpackError {
    match unpack(body, decoded_bytes) {
        Err(error) => error,
        Ok(bytes) => panic!("unpack restored {} bytes", bytes.len()),
    }
}

/// `support::every_kind`'s body with every eligible column transformed. Its
/// tables are items, links, nothing, owners, pair, ticks and zones.
fn sample() -> Taken {
    static SAMPLE: OnceLock<Taken> = OnceLock::new();
    SAMPLE
        .get_or_init(|| Taken::of(&support::every_kind()))
        .clone()
}

/// The transform of `from`'s entry, on `to` in its place.
fn moved(taken: &Taken, from: (&str, &str), to: (&str, &str)) -> Taken {
    let mut moved = taken.clone();
    let entry = taken.entry(from.0, from.1);
    let (table, column) = taken.named(to.0, to.1);
    let mut column_entry = moved.preamble.columns.remove(entry);
    let stream = moved.streams.remove(entry);
    (column_entry.table, column_entry.column) = (table, column);
    let at = moved
        .preamble
        .columns
        .iter()
        .position(|entry| (entry.table, entry.column) >= (table, column))
        .unwrap_or(moved.preamble.columns.len());
    if moved
        .preamble
        .columns
        .get(at)
        .is_some_and(|entry| (entry.table, entry.column) == (table, column))
    {
        moved.preamble.columns.remove(at);
        moved.streams.remove(at);
    }
    moved.preamble.columns.insert(at, column_entry);
    moved.streams.insert(at, stream);
    moved
}

#[test]
fn every_kind_round_trips_in_both_table_orders() {
    let artifact = support::every_kind();
    let packed = round_trip(&artifact);
    assert_eq!(hoisted(&packed), [("owners", "id", 0), ("zones", "id", 0)]);
    // With the size test set aside, the columns it leaves stored are streams.
    for (table, column, transform) in [
        ("items", "owner_id", Transform::Fk),
        ("items", "zone_id", Transform::Fk),
        ("items", "quantity", Transform::Int),
        ("items", "created_at", Transform::Int),
        ("items", "ghost", Transform::Sep),
        ("items", "meta", Transform::Sep),
        ("pair", "at", Transform::Int),
        ("pair", "n", Transform::Int),
        ("ticks", "at", Transform::Int),
        ("ticks", "jitter", Transform::Int),
        ("nothing", "label", Transform::Sep),
        ("nothing", "n", Transform::Int),
        ("nothing", "at", Transform::Int),
    ] {
        assert_eq!(
            choice(&packed, table, column).transform,
            Some(transform),
            "{table}.{column}"
        );
    }
    // An empty table's stream is empty, and so is an all-null text column's.
    let empty = choice(&packed, "nothing", "n");
    assert_eq!((empty.width, empty.stream_bytes), (Some(0), Some(0)));
    assert_eq!(choice(&packed, "items", "ghost").stream_bytes, Some(0));
    for (table, column) in [
        ("items", "id"),
        ("items", "total"),
        ("items", "score"),
        ("items", "active"),
        ("items", "blob"),
        ("items", "amount"),
        ("items", "day"),
        ("items", "clock"),
        ("items", "span"),
        ("items", "status"),
        ("pair", "big"),
        ("owners", "id"),
        ("nothing", "id"),
    ] {
        assert_eq!(
            choice(&packed, table, column).considered,
            None,
            "{table}.{column}"
        );
    }

    // The by-size order: the header is unchanged but for offsets,
    // zones now precedes every column coded against it and owners follows
    // links.
    let relaid = support::smallest_first(&artifact);
    let relaid_packed = round_trip(&relaid);
    assert_eq!(hoisted(&relaid_packed), [("owners", "id", 0)]);
    assert!(relaid_packed.columns == packed.columns);
    let layout = Layout::read(&relaid).unwrap();
    assert_ne!(layout.order, (0..layout.parts.len()).collect::<Vec<_>>());
    let schema_order = Layout::read(&artifact).unwrap();
    assert_eq!(
        schema_order.order,
        (0..schema_order.parts.len()).collect::<Vec<_>>()
    );
}

#[test]
fn a_foreign_key_is_coded_at_99_percent_found_and_not_below() {
    // Per column: its present rows, the first of them in no table.
    const COLUMNS: [(&str, usize, usize); 7] = [
        ("one_of_100", 100, 1),
        ("two_of_100", 100, 2),
        ("two_of_200", 200, 2),
        ("three_of_200", 200, 3),
        ("all_null", 0, 0),
        ("one_found", 1, 0),
        ("one_lost", 1, 1),
    ];
    let mut random = Random(11);
    let people: Vec<[u8; 16]> = (0..150).map(|_| random.uuid()).collect();
    let claims = (0..200)
        .map(|row| {
            let mut values = vec![Value::Int64(row as i64)];
            for (_, present, lost) in COLUMNS {
                values.push(if row >= present {
                    Value::Null
                } else if row < lost {
                    Value::Uuid(random.uuid())
                } else {
                    Value::Uuid(people[random.below(people.len())])
                });
            }
            values
        })
        .collect();
    let mut columns = vec![("id", ColumnKind::Int64, false)];
    columns.extend(COLUMNS.map(|(name, ..)| (name, ColumnKind::Uuid, true)));
    let artifact = encode(vec![
        table("claims", &["id"], &columns, claims),
        table(
            "people",
            &["id"],
            &[("id", ColumnKind::Uuid, false)],
            people.iter().map(|&id| vec![Value::Uuid(id)]).collect(),
        ),
    ]);

    let packed = round_trip(&artifact);
    for (column, escapes) in [
        ("one_of_100", Some(1)),
        ("two_of_100", None),
        ("two_of_200", Some(2)),
        ("three_of_200", None),
        ("all_null", None),
        ("one_found", Some(0)),
        ("one_lost", None),
    ] {
        let choice = choice(&packed, "claims", column);
        assert_eq!(choice.escapes, escapes, "{column}");
        assert_eq!(choice.transform, escapes.map(|_| Transform::Fk), "{column}");
        assert_eq!(
            choice.stream_bytes,
            escapes.map(|escapes| 200 + 16 * escapes),
            "{column}"
        );
    }
    assert_eq!(hoisted(&packed), [("people", "id", 0)]);
    // The frozen rule applies fk wherever it qualifies, with no size test.
    let frozen = pack(&artifact).unwrap();
    let one = choice(&frozen, "claims", "one_of_100");
    assert_eq!(one.transform, Some(Transform::Fk));
    assert_eq!((one.stored_q9_bytes, one.stream_q9_bytes), (None, None));
}

#[test]
fn a_foreign_key_s_table_is_ruled_in_or_out_wherever_its_misses_fall() {
    // The search leaves a table at the miss that rules it out and takes it at
    // the find that rules it in, so each is set at the column's end here, and
    // in the test above at its start. Per column: its present rows, the last
    // of them in no table.
    const COLUMNS: [(&str, usize, usize); 7] = [
        ("last_of_100", 100, 1),
        ("last_two_of_100", 100, 2),
        // 99% of 150 is 148.5 rows, so 149 are needed.
        ("last_of_150", 150, 1),
        ("last_two_of_150", 150, 2),
        ("last_two_of_200", 200, 2),
        ("last_three_of_200", 200, 3),
        ("last_of_one", 1, 1),
    ];
    let mut random = Random(12);
    let people: Vec<[u8; 16]> = (0..150).map(|_| random.uuid()).collect();
    let strangers: Vec<[u8; 16]> = (0..150).map(|_| random.uuid()).collect();
    let claims = (0..200)
        .map(|row| {
            let mut values = vec![Value::Int64(row as i64)];
            for (_, present, lost) in COLUMNS {
                values.push(if row >= present {
                    Value::Null
                } else if row >= present - lost {
                    Value::Uuid(random.uuid())
                } else {
                    Value::Uuid(people[random.below(people.len())])
                });
            }
            // Half of these are in the first table tried, and all but the
            // last in the second.
            values.push(Value::Uuid(match row {
                199 => random.uuid(),
                row if row % 2 == 0 => strangers[row % strangers.len()],
                row => people[row % people.len()],
            }));
            values
        })
        .collect();
    let keys = |ids: &[[u8; 16]]| ids.iter().map(|&id| vec![Value::Uuid(id)]).collect();
    let mut columns = vec![("id", ColumnKind::Int64, false)];
    columns.extend(COLUMNS.map(|(name, ..)| (name, ColumnKind::Uuid, true)));
    columns.push(("either", ColumnKind::Uuid, false));
    let id = [("id", ColumnKind::Uuid, false)];
    let mut both = strangers.clone();
    both.extend(&people);
    let artifact = encode(vec![
        table("claims", &["id"], &columns, claims),
        // None of a claim's people, so every column leaves this table first.
        table("strangers", &["id"], &id, keys(&strangers)),
        table("people", &["id"], &id, keys(&people)),
        table("both", &["id"], &id, keys(&both)),
    ]);

    let packed = round_trip(&artifact);
    for (column, escapes) in [
        ("last_of_100", Some(1)),
        ("last_two_of_100", None),
        ("last_of_150", Some(1)),
        ("last_two_of_150", None),
        ("last_two_of_200", Some(2)),
        ("last_three_of_200", None),
        ("last_of_one", None),
    ] {
        let choice = choice(&packed, "claims", column);
        assert_eq!(choice.escapes, escapes, "{column}");
        assert_eq!(choice.transform, escapes.map(|_| Transform::Fk), "{column}");
        assert_eq!(
            choice.referenced_table.as_deref(),
            escapes.map(|_| "people"),
            "{column}"
        );
    }
    // Neither of the first two tables holds 99% of this column, and the first
    // that does is the table it is coded against, with the one row it lacks.
    let either = choice(&packed, "claims", "either");
    assert_eq!(either.referenced_table.as_deref(), Some("both"));
    assert_eq!(either.escapes, Some(1));
}

#[test]
fn a_row_index_takes_the_fewest_bytes_that_hold_the_row_count() {
    assert_eq!(
        [
            0,
            255,
            256,
            65_535,
            65_536,
            16_777_215,
            16_777_216,
            u32::MAX
        ]
        .map(index_width),
        [1, 1, 2, 2, 3, 3, 4, 4]
    );
}

#[test]
fn a_key_is_hoisted_only_when_it_follows_the_column_coded_against_it() {
    let mut random = Random(3);
    // 255 rows leave one byte for the escape code; 256 need a second.
    let first: Vec<[u8; 16]> = (0..255).map(|_| random.uuid()).collect();
    let last: Vec<[u8; 16]> = (0..256).map(|_| random.uuid()).collect();
    let middle = (0..300)
        .map(|row| {
            vec![
                Value::Int64(row as i64),
                Value::Uuid(if row == 0 {
                    random.uuid()
                } else {
                    first[row % first.len()]
                }),
                Value::Uuid(if row == 299 {
                    random.uuid()
                } else {
                    last[row % last.len()]
                }),
            ]
        })
        .collect();
    let keys = |ids: &[[u8; 16]]| ids.iter().map(|&id| vec![Value::Uuid(id)]).collect();
    let artifact = encode(vec![
        table(
            "first",
            &["id"],
            &[("id", ColumnKind::Uuid, false)],
            keys(&first),
        ),
        table(
            "middle",
            &["id"],
            &[
                ("id", ColumnKind::Int64, false),
                ("to_first", ColumnKind::Uuid, false),
                ("to_last", ColumnKind::Uuid, false),
            ],
            middle,
        ),
        table(
            "last",
            &["id"],
            &[("id", ColumnKind::Uuid, false)],
            keys(&last),
        ),
    ]);

    let packed = round_trip(&artifact);
    assert_eq!(hoisted(&packed), [("last", "id", 0)]);
    let to_first = choice(&packed, "middle", "to_first");
    assert_eq!(to_first.referenced_table.as_deref(), Some("first"));
    assert_eq!((to_first.width, to_first.escapes), (Some(1), Some(1)));
    let to_last = choice(&packed, "middle", "to_last");
    assert_eq!(to_last.referenced_table.as_deref(), Some("last"));
    assert_eq!((to_last.width, to_last.escapes), (Some(2), Some(1)));

    // Row 0 of to_first holds the escape code, 255, and row 1 key row 1.
    let taken = Taken::of(&artifact);
    let stream = &taken.streams[taken.entry("middle", "to_first")];
    assert_eq!(stream.len(), 300 + 16);
    assert_eq!(stream[..3], [255, 1, 2]);
    // Row 299 of to_last holds 256 across its two planes, low byte first.
    let stream = &taken.streams[taken.entry("middle", "to_last")];
    assert_eq!(stream.len(), 2 * 300 + 16);
    assert_eq!((stream[299], stream[300 + 299]), (0, 1));
    // Row 298 holds key row 298 % 256.
    assert_eq!((stream[298], stream[300 + 298]), (42, 0));
}

#[test]
fn a_repeated_key_is_coded_as_its_first_row() {
    let (a, b) = ([0xaa; 16], [0xbb; 16]);
    let artifact = forge(&[
        ForgedTable {
            name: "keys",
            rows: 3,
            primary_key: &["id"],
            columns: vec![forged("id", "uuid", vec![uuids(&[a, b, a])])],
        },
        ForgedTable {
            name: "uses",
            rows: 4,
            primary_key: &[],
            columns: vec![forged("key", "uuid", vec![uuids(&[a, b, a, b])])],
        },
    ]);
    round_trip(&artifact);
    let taken = Taken::of(&artifact);
    assert_eq!(taken.streams, [[0, 1, 0, 1]]);
}

#[test]
fn empty_tables_and_an_artifact_without_tables_round_trip() {
    use ColumnKind::{
        Bool, Bytes, Date, Decimal, Dict, Int64, Interval, Json, Time, Timestamp, Uint64, Utf8,
        Uuid, F64,
    };
    let kinds = [
        ("int64", Int64),
        ("uint64", Uint64),
        ("f64", F64),
        ("timestamp", Timestamp),
        ("bool", Bool),
        ("utf8", Utf8),
        ("bytes", Bytes),
        ("uuid", Uuid),
        ("json", Json),
        ("decimal", Decimal),
        ("date", Date),
        ("time", Time),
        ("interval", Interval),
        ("dict", Dict),
    ];
    let mut columns = vec![("id", Uuid, false)];
    columns.extend(kinds.map(|(name, kind)| (name, kind, true)));
    let artifact = encode(vec![
        table("void", &["id"], &columns, Vec::new()),
        table(
            "pointer",
            &["id"],
            &[("id", Int64, false), ("void_id", Uuid, true)],
            vec![vec![Value::Int64(1), Value::Null]],
        ),
    ]);
    let packed = round_trip(&artifact);
    for (column, transform) in [
        ("int64", Transform::Int),
        ("timestamp", Transform::Int),
        ("utf8", Transform::Sep),
        ("json", Transform::Sep),
    ] {
        let choice = choice(&packed, "void", column);
        assert_eq!(choice.transform, Some(transform), "{column}");
        assert_eq!(choice.stream_bytes, Some(0), "{column}");
    }
    // No row is found in an empty table, and no uuid column of one is coded.
    assert_eq!(choice(&packed, "void", "uuid").considered, None);
    assert_eq!(choice(&packed, "pointer", "void_id").considered, None);

    let none = forge(&[]);
    assert!(pack(&none).unwrap().stored);
    let body = pack_under(&none, Rule::Every).unwrap().body;
    assert_eq!(body, framed(NO_COLUMNS, &none));
    restores(&body, &none);
}

#[test]
fn a_value_2_51_from_zero_leaves_its_column_stored() {
    const LIMIT: i64 = 1 << 51;
    for (value, eligible) in [
        (LIMIT - 1, true),
        (LIMIT, false),
        (LIMIT + 1, false),
        (1 - LIMIT, true),
        (-LIMIT, false),
        (-LIMIT - 1, false),
        (i64::MAX, false),
        (i64::MIN, false),
    ] {
        let rows = |count: i64| {
            (0..count)
                .map(|row| {
                    let number = if row == count / 2 { value } else { row };
                    vec![
                        Value::Int64(row),
                        Value::Int64(number),
                        Value::TimestampMicros(number),
                    ]
                })
                .collect()
        };
        let columns = [
            ("id", ColumnKind::Int64, false),
            ("number", ColumnKind::Int64, false),
            ("at", ColumnKind::Timestamp, false),
        ];
        let artifact = encode(vec![
            table("two", &["id"], &columns, rows(2)),
            table("many", &["id"], &columns, rows(1_500)),
        ]);
        let (header, _) = header_of(&artifact);
        // Two rows are stored without an encoding.
        assert!(header["tables"][0]["columns"][1]["encoding"].is_null());
        if eligible {
            assert_eq!(header["tables"][1]["columns"][1]["encoding"]["kind"], "for");
        }

        let packed = round_trip(&artifact);
        for (table, column) in [
            ("two", "number"),
            ("two", "at"),
            ("many", "number"),
            ("many", "at"),
        ] {
            assert_eq!(
                choice(&packed, table, column).considered,
                eligible.then_some(Transform::Int),
                "{value} in {table}.{column}"
            );
        }
    }
}

#[test]
fn text_holding_0xff_stays_stored() {
    let artifact = forge(&[ForgedTable {
        name: "texts",
        rows: 3,
        primary_key: &[],
        columns: vec![
            forged("clean", "utf8", texts(&[b"a", b"", b"\xc3\xa9"]).into()),
            forged("dirty", "utf8", texts(&[b"a", b"b\xffc", b""]).into()),
            forged("last_byte", "json", texts(&[b"{}", b"1", b"\xff"]).into()),
            forged("bytes", "bytes", texts(&[b"a", b"", b"c"]).into()),
        ],
    }]);
    let packed = round_trip(&artifact);
    assert_eq!(
        choice(&packed, "texts", "clean").transform,
        Some(Transform::Sep)
    );
    assert_eq!(choice(&packed, "texts", "clean").stream_bytes, Some(3 + 3));
    for column in ["dirty", "last_byte", "bytes"] {
        assert_eq!(
            choice(&packed, "texts", column).considered,
            None,
            "{column}"
        );
    }
    let taken = Taken::of(&artifact);
    assert_eq!(taken.streams, [b"a\xff\xff\xc3\xa9\xff"]);
}

#[test]
fn text_streams_keep_null_rows_out_and_empty_values_in() {
    let present = [true, false, true, true, false];
    let artifact = forge(&[ForgedTable {
        name: "texts",
        rows: 5,
        primary_key: &[],
        columns: vec![
            forged_nullable(
                "note",
                "utf8",
                [
                    vec![bitmap(&present)],
                    texts(&[b"ab", b"", b"", b"c", b""]).into(),
                ]
                .concat(),
            ),
            forged_nullable(
                "ghost",
                "json",
                [
                    vec![bitmap(&[false; 5])],
                    texts(&[b"", b"", b"", b"", b""]).into(),
                ]
                .concat(),
            ),
        ],
    }]);
    round_trip(&artifact);
    let taken = Taken::of(&artifact);
    // A null row adds nothing; a present empty value adds its separator.
    assert_eq!(taken.streams, [&b"ab\xff\xffc\xff"[..], b""]);
}

#[test]
fn numeric_streams_are_zigzag_deltas_of_the_present_rows_in_byte_planes() {
    let present = [true, false, true, true, true];
    let artifact = forge(&[ForgedTable {
        name: "numbers",
        rows: 5,
        primary_key: &[],
        columns: vec![
            // 300, then -1 (delta -301), then 0x10203 (delta 0x10204).
            forged_nullable(
                "holes",
                "int64",
                vec![bitmap(&present), int64s(&[300, 0, -1, 0x1_0203, 0x1_0203])],
            ),
            forged("flat", "timestamp", vec![int64s(&[0; 5])]),
        ],
    }]);
    round_trip(&artifact);
    let taken = Taken::of(&artifact);
    // Zigzag: 600, 601, 0x20408, 0. Three planes of four rows, low bytes
    // first.
    assert_eq!(
        taken.streams[0],
        [0x58, 0x59, 0x08, 0, 0x02, 0x02, 0x04, 0, 0, 0, 0x02, 0]
    );
    assert_eq!(taken.preamble.columns[0].width, 3);
    // Every delta is zero, so the stream has no plane.
    assert_eq!(taken.streams[1], [0_u8; 0]);
    assert_eq!(taken.preamble.columns[1].width, 0);
}

/// A table of one frame-of-reference column over these blocks, `present`
/// giving its validity when it is nullable.
fn block_table(kind: &'static str, present: Option<&[bool]>, of: &[Block]) -> Vec<u8> {
    let rows = of.iter().map(|block| block.numbers.len()).sum::<usize>();
    let parts: Vec<Vec<u8>> = present.map(bitmap).into_iter().chain(blocks(of)).collect();
    forge(&[ForgedTable {
        name: "blocks",
        rows: rows as u32,
        primary_key: &[],
        columns: vec![encoded(ForgedColumn {
            nullable: present.is_some(),
            ..forged("value", kind, parts)
        })],
    }])
}

#[test]
fn numeric_blocks_of_every_width_round_trip() {
    const LIMIT: i64 = 1 << 51;
    let mut random = Random(64);
    let mut numbers = |bits: u32| -> Vec<u64> {
        (0..NUMERIC_BLOCK_ROWS)
            .map(|_| random.next() >> (64 - bits))
            .collect()
    };
    let wide: Vec<u64> = numbers(52)
        .into_iter()
        .map(|number| number.min((1 << 52) - 2))
        .collect();
    let raw: Vec<u64> = numbers(40)
        .into_iter()
        .enumerate()
        .map(|(row, number)| (number as i64 * if row % 2 == 0 { 1 } else { -1 }) as u64)
        .collect();
    let of = [
        Block {
            base: 77,
            bits: 0,
            scale: 0,
            numbers: vec![0; NUMERIC_BLOCK_ROWS],
        },
        Block {
            base: -5,
            bits: 1,
            scale: 0,
            numbers: numbers(1),
        },
        Block {
            base: -1_000,
            bits: 13,
            scale: 3,
            numbers: numbers(13),
        },
        // The widest block of values inside the limit: 1 - 2^51 to 2^51 - 1.
        Block {
            base: 1 - LIMIT,
            bits: 52,
            scale: 0,
            numbers: wide,
        },
        Block {
            base: 0,
            bits: 64,
            scale: 0,
            numbers: raw.clone(),
        },
        // A raw block's base and scale are not read, and stay as stored.
        Block {
            base: i64::MIN,
            bits: 64,
            scale: 9,
            numbers: raw,
        },
        // Wider than its values need, as the encoder would not write it.
        Block {
            base: 3,
            bits: 9,
            scale: 0,
            numbers: vec![1, 0, 2, 1, 2],
        },
    ];
    for kind in ["int64", "timestamp"] {
        let artifact = block_table(kind, None, &of);
        let packed = round_trip(&artifact);
        let choice = choice(&packed, "blocks", "value");
        assert_eq!(choice.transform, Some(Transform::Int));
        assert_eq!(choice.width, Some(7));
    }

    // Null rows hold zero bits, in a packed block and in a raw one, and a
    // block may hold nothing else.
    let present: Vec<bool> = (0..3 * NUMERIC_BLOCK_ROWS + 9)
        .map(|row| row % 3 != 1 && !(NUMERIC_BLOCK_ROWS..2 * NUMERIC_BLOCK_ROWS).contains(&row))
        .collect();
    let holes = |numbers: Vec<u64>, start: usize| -> Vec<u64> {
        numbers
            .into_iter()
            .enumerate()
            .map(|(row, number)| if present[start + row] { number } else { 0 })
            .collect()
    };
    let of = [
        Block {
            base: -40,
            bits: 11,
            scale: 3,
            numbers: holes(numbers(11), 0),
        },
        Block {
            base: 0,
            bits: 0,
            scale: 0,
            numbers: vec![0; NUMERIC_BLOCK_ROWS],
        },
        Block {
            base: 0,
            bits: 64,
            scale: 0,
            numbers: holes(numbers(30), 2 * NUMERIC_BLOCK_ROWS),
        },
        Block {
            base: 1_000_000,
            bits: 3,
            scale: 0,
            numbers: holes(vec![7, 7, 0, 5, 7, 1, 2, 7, 3], 3 * NUMERIC_BLOCK_ROWS),
        },
    ];
    let artifact = block_table("timestamp", Some(&present), &of);
    let packed = round_trip(&artifact);
    assert_eq!(
        choice(&packed, "blocks", "value").transform,
        Some(Transform::Int)
    );
    let taken = Taken::of(&artifact);
    let kept = present.iter().filter(|present| **present).count();
    assert_eq!(
        taken.streams[0].len(),
        kept * usize::from(taken.preamble.columns[0].width)
    );
}

#[test]
fn a_column_without_an_encoding_round_trips_at_any_row_count() {
    let mut random = Random(8);
    let present: Vec<bool> = (0..3_000).map(|row| row % 7 != 2).collect();
    let values: Vec<i64> = present
        .iter()
        .map(|present| {
            if *present {
                random.below(1 << 40) as i64 - (1 << 39)
            } else {
                0
            }
        })
        .collect();
    let artifact = forge(&[ForgedTable {
        name: "plain",
        rows: 3_000,
        primary_key: &[],
        columns: vec![
            forged("full", "int64", vec![int64s(&values)]),
            forged_nullable(
                "holes",
                "timestamp",
                vec![bitmap(&present), int64s(&values)],
            ),
        ],
    }]);
    let packed = round_trip(&artifact);
    for column in ["full", "holes"] {
        assert_eq!(
            choice(&packed, "plain", column).transform,
            Some(Transform::Int),
            "{column}"
        );
    }
}

#[test]
fn parts_a_stream_would_not_rebuild_leave_their_column_stored() {
    let (a, b) = ([0xaa; 16], [0xbb; 16]);
    let present = [true, false, true];
    let keys = ForgedTable {
        name: "keys",
        rows: 2,
        primary_key: &["id"],
        columns: vec![forged("id", "uuid", vec![uuids(&[a, b])])],
    };
    let packed_block = |numbers: Vec<u64>| {
        blocks(&[Block {
            base: 10,
            bits: 5,
            scale: 0,
            numbers,
        }])
    };
    let [headers, mut loose] = packed_block(vec![1, 0, 3]);
    // The 15 bits of three rows leave the second byte's top bit unused.
    loose[1] |= 0x80;
    let [gap_headers, gap_packed] = {
        let [mut headers, mut packed] = blocks(&[
            Block {
                base: 0,
                bits: 8,
                scale: 0,
                numbers: vec![1; NUMERIC_BLOCK_ROWS],
            },
            Block {
                base: 0,
                bits: 8,
                scale: 0,
                numbers: vec![2, 3],
            },
        ]);
        // The second block starts one byte late.
        headers[16..20].copy_from_slice(&(NUMERIC_BLOCK_ROWS as u32 + 1).to_le_bytes());
        packed.insert(NUMERIC_BLOCK_ROWS, 0);
        [headers, packed]
    };
    let [clean_lengths, clean_values] = texts(&[b"ab", b"", b"c"]);
    let mut first_length = clean_lengths.clone();
    first_length[0] = 1;

    // Each column, and whether a transform still fits it.
    let columns: Vec<(ForgedColumn, bool)> = vec![
        (
            forged_nullable(
                "uuid",
                "uuid",
                vec![bitmap(&present), uuids(&[a, [0; 16], b])],
            ),
            true,
        ),
        (
            forged_nullable(
                "uuid_under_null",
                "uuid",
                vec![bitmap(&present), uuids(&[a, b, b])],
            ),
            false,
        ),
        (
            forged_nullable("raw", "int64", vec![bitmap(&present), int64s(&[5, 0, 6])]),
            true,
        ),
        (
            forged_nullable(
                "raw_under_null",
                "int64",
                vec![bitmap(&present), int64s(&[5, 9, 6])],
            ),
            false,
        ),
        (
            encoded(forged_nullable(
                "block",
                "int64",
                [vec![bitmap(&present)], packed_block(vec![1, 0, 3]).into()].concat(),
            )),
            true,
        ),
        (
            encoded(forged_nullable(
                "block_under_null",
                "int64",
                [vec![bitmap(&present)], packed_block(vec![1, 2, 3]).into()].concat(),
            )),
            false,
        ),
        (
            encoded(forged_nullable(
                "block_unused_bit",
                "int64",
                vec![bitmap(&present), headers, loose],
            )),
            false,
        ),
        (
            forged_nullable(
                "text",
                "utf8",
                vec![
                    bitmap(&present),
                    clean_lengths.clone(),
                    clean_values.clone(),
                ],
            ),
            true,
        ),
        (
            forged_nullable(
                "text_under_null",
                "utf8",
                [vec![bitmap(&present)], texts(&[b"ab", b"x", b"c"]).into()].concat(),
            ),
            false,
        ),
        (
            forged(
                "text_first_length",
                "utf8",
                vec![first_length, clean_values.clone()],
            ),
            false,
        ),
        (
            forged(
                "text_short_values",
                "json",
                vec![clean_lengths.clone(), b"ab".to_vec()],
            ),
            false,
        ),
        (
            forged(
                "text_long_values",
                "json",
                vec![clean_lengths.clone(), b"abcd".to_vec()],
            ),
            false,
        ),
        (
            ForgedColumn {
                listed: Some(vec![1, 0]),
                ..forged(
                    "text_values_first",
                    "utf8",
                    vec![clean_values.clone(), clean_lengths.clone()],
                )
            },
            false,
        ),
        (
            forged("text_clean", "utf8", vec![clean_lengths, clean_values]),
            true,
        ),
    ];
    let expected: Vec<(&str, bool)> = columns
        .iter()
        .map(|(column, fits)| (column.name, *fits))
        .collect();
    let artifact = forge(&[
        keys,
        ForgedTable {
            name: "rows",
            rows: 3,
            primary_key: &[],
            columns: columns.into_iter().map(|(column, _)| column).collect(),
        },
        ForgedTable {
            name: "gapped",
            rows: NUMERIC_BLOCK_ROWS as u32 + 2,
            primary_key: &[],
            columns: vec![encoded(forged(
                "value",
                "int64",
                vec![gap_headers, gap_packed],
            ))],
        },
    ]);
    let packed = round_trip(&artifact);
    for (column, fits) in expected {
        assert_eq!(
            choice(&packed, "rows", column).considered.is_some(),
            fits,
            "{column}"
        );
    }
    assert_eq!(choice(&packed, "gapped", "value").considered, None);
}

#[test]
fn pack_refuses_what_is_not_a_v4_artifact_laid_out_part_after_part() {
    let reason = |artifact: &[u8]| match pack(artifact) {
        Err(PackError::Artifact(reason)) => reason,
        other => panic!("{:?}", other.map(|packed| packed.body.len())),
    };
    let good = support::stored();
    assert!(pack(&good).is_ok());
    assert_eq!(reason(b""), "it does not begin with the artifact's magic");
    assert_eq!(
        reason(b"CALQPAK1"),
        "it does not begin with the artifact's magic"
    );
    assert_eq!(reason(&good[..11]), "it runs past the body");
    assert_eq!(reason(&good[..40]), "it runs past the body");
    assert_eq!(
        reason(&good[..good.len() - 1]),
        "it does not end where its last part ends"
    );
    assert_eq!(
        reason(&[good.as_slice(), &[0]].concat()),
        "it does not end where its last part ends"
    );

    let column = |parts: serde_json::Value| json!({"name": "c", "type": "bool", "nullable": false, "parts": parts});
    let header = |version: u32, format: &str, column: serde_json::Value| {
        json!({
            "format": format,
            "version": version,
            "tables": [{"name": "t", "rows": 1, "primaryKey": [], "columns": [column]}],
        })
    };
    let one = column(json!([{"offset": 0, "length": 1}]));
    assert!(pack(&frame(&header(4, FORMAT_NAME, one.clone()), &[1])).is_ok());
    assert_eq!(
        reason(&frame(&header(5, FORMAT_NAME, one.clone()), &[1])),
        "it is not a version 4 header"
    );
    assert_eq!(
        reason(&frame(&header(4, "calque", one), &[1])),
        "it is not a version 4 header"
    );
    assert_eq!(
        reason(&frame(&json!([]), &[])),
        "it is not a direct-columnar header"
    );
    // The encoder writes `nullable` on every column.
    assert_eq!(
        reason(&frame(
            &header(
                4,
                FORMAT_NAME,
                json!({"name": "c", "type": "bool", "parts": [{"offset": 0, "length": 1}]})
            ),
            &[1]
        )),
        "it is not a direct-columnar header"
    );
    let two = |second: u64| {
        frame(
            &header(
                4,
                FORMAT_NAME,
                column(json!([{"offset": 0, "length": 1}, {"offset": second, "length": 1}])),
            ),
            &[1, 0, 0, 0, 0, 0, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0, 1]
                [..if second == 16 { 17 } else { 9 }],
        )
    };
    assert!(pack(&two(8)).is_ok());
    for second in [0, 1, 7, 16] {
        assert_eq!(
            reason(&two(second)),
            "a part does not follow the part before it",
            "{second}"
        );
    }
    assert_eq!(
        reason(&frame(
            &header(
                4,
                FORMAT_NAME,
                column(json!([{"offset": u64::MAX, "length": 1}]))
            ),
            &[1]
        )),
        "a part's extent overflows"
    );
    assert_eq!(
        reason(&frame(
            &header(
                4,
                FORMAT_NAME,
                column(json!([{"offset": 0, "length": u64::MAX}]))
            ),
            &[1]
        )),
        "a part's extent overflows"
    );
    let mut padded = two(8);
    let last = padded.len() - 2;
    padded[last] = 1;
    assert_eq!(reason(&padded), "the padding between parts is not zero");
}

#[test]
fn a_stored_body_is_the_artifact_and_restores_only_at_its_own_length() {
    let artifact = support::stored();
    let packed = pack(&artifact).unwrap();
    assert!(packed.stored && packed.body == artifact);
    restores(&artifact, &artifact);
    let length = artifact.len() as u64;
    for expected in [length - 1, length + 1, 0] {
        assert_eq!(
            refusal(&artifact, expected),
            UnpackError::DecodedLength {
                expected,
                actual: length
            }
        );
    }
}

#[test]
fn refuses_a_body_that_begins_with_neither_magic() {
    let body = sample().body();
    let length = sample().artifact.len() as u64;
    for bad in [&b""[..], b"CALQPAK", b"CALQPAQ1", b"calqpak1", &body[1..]] {
        assert_eq!(refusal(bad, length), UnpackError::Magic);
    }
    for bit in 0..64 {
        let mut flipped = body.clone();
        flipped[bit / 8] ^= 1 << (bit % 8);
        assert_eq!(refusal(&flipped, length), UnpackError::Magic);
    }
}

#[test]
fn refuses_a_preamble_that_runs_past_the_body() {
    let body = sample().body();
    let past = UnpackError::Preamble("it runs past the body");
    assert_eq!(refusal(b"CALQPAK1", 0), past);
    assert_eq!(refusal(b"CALQPAK1\x01\0\0", 0), past);
    assert_eq!(refusal(b"CALQPAK1\x05\0\0\0abcd", 0), past);
    let mut long = body.clone();
    long[8..12].copy_from_slice(&u32::MAX.to_le_bytes());
    assert_eq!(refusal(&long, 0), past);
    let exact = (body.len() as u32 - 12 + 1).to_le_bytes();
    long[8..12].copy_from_slice(&exact);
    assert_eq!(refusal(&long, 0), past);
}

/// The sample's body under another preamble.
fn with_preamble(preamble: &[u8]) -> UnpackError {
    let taken = sample();
    let body = taken.body();
    let length = u32::from_le_bytes(body[8..12].try_into().unwrap()) as usize;
    refusal(
        &framed(preamble, &body[12 + length..]),
        taken.artifact.len() as u64,
    )
}

#[test]
fn refuses_a_preamble_that_is_not_json_with_a_version() {
    for preamble in [
        &b""[..],
        b"version 1",
        b"[]",
        b"{}",
        br#"{"version":"1"}"#,
        br#"{"version":-1}"#,
        br#"{"version":1.5}"#,
        br#"{"version":1"#,
        b"\xff\xfe",
    ] {
        assert_eq!(
            with_preamble(preamble),
            UnpackError::Preamble("it is not JSON with a version"),
            "{}",
            String::from_utf8_lossy(preamble)
        );
    }
}

#[test]
fn refuses_a_version_other_than_1() {
    assert_eq!(
        with_preamble(br#"{"version":2,"hoisted":[],"columns":[]}"#),
        UnpackError::Version(2)
    );
    assert_eq!(
        with_preamble(br#"{"version":0,"hoisted":[],"columns":[]}"#),
        UnpackError::Version(0)
    );
    // Whatever else a later version's preamble holds.
    assert_eq!(
        with_preamble(br#"{"version":7,"streams":{}}"#),
        UnpackError::Version(7)
    );
}

#[test]
fn refuses_a_preamble_that_lacks_a_member_or_holds_one_of_another_type() {
    let entry =
        |members: &str| format!(r#"{{"version":1,"hoisted":[],"columns":[{{{members}}}]}}"#);
    for preamble in [
        r#"{"version":1}"#.to_owned(),
        r#"{"version":1,"hoisted":[]}"#.to_owned(),
        r#"{"version":1,"columns":[]}"#.to_owned(),
        r#"{"version":1,"hoisted":{},"columns":[]}"#.to_owned(),
        r#"{"version":1,"hoisted":[{"table":0,"column":0}],"columns":[]}"#.to_owned(),
        entry(r#""table":0,"column":1,"transform":"fk","references":3,"width":2,"escapes":1"#),
        entry(
            r#""table":0,"transform":"fk","references":3,"width":2,"escapes":1,"streamBytes":416"#,
        ),
        entry(
            r#""table":0,"column":1,"transform":"fk","references":3,"width":2e0,"escapes":1,"streamBytes":416"#,
        ),
        entry(
            r#""table":0,"column":1,"transform":"zip","references":3,"width":2,"escapes":1,"streamBytes":416"#,
        ),
        entry(
            r#""table":0,"column":1,"transform":"fk","references":3,"width":"2","escapes":1,"streamBytes":416"#,
        ),
        entry(
            r#""table":0,"column":1,"transform":"fk","references":3,"width":256,"escapes":1,"streamBytes":416"#,
        ),
        entry(
            r#""table":-1,"column":1,"transform":"fk","references":3,"width":2,"escapes":1,"streamBytes":416"#,
        ),
        entry(
            r#""table":0,"column":4294967296,"transform":"fk","references":3,"width":2,"escapes":1,"streamBytes":416"#,
        ),
        entry(
            r#""table":0,"column":1,"transform":"fk","references":3,"width":2,"escapes":1.0,"streamBytes":416"#,
        ),
    ] {
        assert_eq!(
            with_preamble(preamble.as_bytes()),
            UnpackError::Preamble("a member is missing or of the wrong type"),
            "{preamble}"
        );
    }
}

#[test]
fn refuses_a_preamble_spelled_otherwise_than_the_packer_writes_it() {
    let spelled = String::from_utf8(sample().preamble.spelled()).unwrap();
    assert!(spelled.starts_with(r#"{"version":1,"hoisted":[{"table":3,"column":0,"part":0},"#));
    for preamble in [
        spelled.replacen('{', "{ ", 1),
        format!("{spelled}\n"),
        format!(" {spelled}"),
        spelled.replacen(r#"{"version":1,"#, r#"{"version":1,"extra":0,"#, 1),
        spelled.replacen(r#""width":2,"escapes":1"#, r#""escapes":1,"width":2"#, 1),
        // The letter f as a JSON escape.
        spelled.replacen(
            r#""transform":"fk""#,
            &format!(r#""transform":"{}u0066k""#, '\\'),
            1,
        ),
        spelled.replacen(r#""references":null"#, r#""references":null,"note":"x""#, 1),
        // A reference left out reads as none, which the packer writes.
        spelled.replacen(r#""references":null,"#, "", 1),
        // The members in another order, with every value as the packer's.
        {
            let (_, members) = spelled.split_once(r#"{"version":1,"#).unwrap();
            format!(r#"{{{},"version":1}}"#, members.strip_suffix('}').unwrap())
        },
    ] {
        assert_ne!(preamble, spelled);
        assert_eq!(
            with_preamble(preamble.as_bytes()),
            UnpackError::Preamble("it is not spelled as the packer writes it"),
            "{preamble}"
        );
    }
}

/// A body of no transformed column over this artifact, which must then hold
/// no padding between parts.
fn bare(artifact: &[u8]) -> Vec<u8> {
    framed(NO_COLUMNS, artifact)
}

#[test]
fn refuses_an_artifact_header_it_cannot_read() {
    let header = |reason| UnpackError::Header(reason);
    let mut taken = sample();
    taken.artifact[3] ^= 0x20;
    assert_eq!(
        taken.refusal(),
        header("it does not begin with the artifact's magic")
    );

    let good = support::stored();
    let length = good.len() as u64;
    assert_eq!(
        refusal(&bare(&good[..7]), length),
        header("it does not begin with the artifact's magic")
    );
    assert_eq!(
        refusal(&bare(&good[..11]), length),
        header("it runs past the body")
    );
    assert_eq!(
        refusal(&bare(&good[..100]), length),
        header("it runs past the body")
    );
    let mut long = good.clone();
    long[8..12].copy_from_slice(&u32::MAX.to_le_bytes());
    assert_eq!(
        refusal(&bare(&long), length),
        header("it runs past the body")
    );
    let mut not_json = good.clone();
    not_json[12] = b'[';
    assert_eq!(
        refusal(&bare(&not_json), length),
        header("it is not a direct-columnar header")
    );

    let column = |parts: serde_json::Value| json!({"name": "c", "type": "bool", "nullable": false, "parts": parts});
    let table = |version: u32, format: &str, column: serde_json::Value| {
        json!({
            "format": format,
            "version": version,
            "tables": [{"name": "t", "rows": 1, "primaryKey": [], "columns": [column]}],
        })
    };
    let one = column(json!([{"offset": 0, "length": 1}]));
    let good = frame(&table(4, FORMAT_NAME, one.clone()), &[1]);
    restores(&bare(&good), &good);
    for (artifact, reason) in [
        (
            frame(&table(3, FORMAT_NAME, one.clone()), &[1]),
            "it is not a version 4 header",
        ),
        (
            frame(&table(4, "calque-direct", one), &[1]),
            "it is not a version 4 header",
        ),
        (
            frame(
                &table(
                    4,
                    FORMAT_NAME,
                    json!({"name": "c", "type": "bool", "parts": [{"offset": 0, "length": 1}]}),
                ),
                &[1],
            ),
            "it is not a direct-columnar header",
        ),
        (
            frame(
                &table(4, FORMAT_NAME, column(json!([{"offset": 8, "length": 1}]))),
                &[0, 0, 0, 0, 0, 0, 0, 0, 1],
            ),
            "a part does not follow the part before it",
        ),
        (
            frame(
                &table(
                    4,
                    FORMAT_NAME,
                    column(json!([{"offset": 0, "length": 1}, {"offset": 1, "length": 1}])),
                ),
                &[1, 1],
            ),
            "a part does not follow the part before it",
        ),
        (
            frame(
                &table(
                    4,
                    FORMAT_NAME,
                    column(json!([{"offset": 0, "length": u64::MAX}, {"offset": 8, "length": 1}])),
                ),
                &[1],
            ),
            "a part does not follow the part before it",
        ),
        (
            frame(
                &table(
                    4,
                    FORMAT_NAME,
                    column(json!([{"offset": 8, "length": u64::MAX}])),
                ),
                &[1],
            ),
            "a part's extent overflows",
        ),
        // The part ends inside the address space and the artifact past it.
        (
            frame(
                &table(
                    4,
                    FORMAT_NAME,
                    column(json!([{"offset": 0, "length": u64::MAX}])),
                ),
                &[1],
            ),
            "a part's extent overflows",
        ),
    ] {
        assert_eq!(
            refusal(&bare(&artifact), artifact.len() as u64),
            header(reason)
        );
    }
}

#[test]
fn refuses_a_decoded_length_other_than_the_header_s() {
    let taken = sample();
    let (body, length) = (taken.body(), taken.artifact.len() as u64);
    restores(&body, &taken.artifact);
    for expected in [length - 1, length + 1, 0, u64::MAX] {
        assert_eq!(
            refusal(&body, expected),
            UnpackError::DecodedLength {
                expected,
                actual: length
            }
        );
    }
}

#[test]
fn refuses_a_body_that_ends_before_its_last_part_or_after_it() {
    let taken = sample();
    let (body, length) = (taken.body(), taken.artifact.len() as u64);
    for kept in [body.len() - 1, body.len() - 5_000] {
        assert_eq!(refusal(&body[..kept], length), UnpackError::Truncated);
    }
    assert_eq!(
        refusal(&[body.as_slice(), &[0]].concat(), length),
        UnpackError::TrailingBytes
    );

    // A stream the preamble makes longer than the body has bytes for.
    let title = taken.entry("items", "title");
    for stream_bytes in [
        taken.streams[title].len() as u64 + 1,
        u64::MAX / 2,
        u64::MAX,
    ] {
        let mut long = taken.clone();
        long.preamble.columns[title].stream_bytes = stream_bytes;
        assert_eq!(long.refusal(), UnpackError::Truncated, "{stream_bytes}");
    }
    let mut short = taken.clone();
    short.preamble.columns[title].stream_bytes -= 1;
    assert_eq!(short.refusal(), UnpackError::TrailingBytes);
}

#[test]
fn refuses_columns_out_of_header_order_or_listed_twice() {
    let out_of_order = UnpackError::Preamble("its columns are not in header order");
    let mut swapped = sample();
    swapped.preamble.columns.swap(0, 1);
    swapped.streams.swap(0, 1);
    assert_eq!(swapped.refusal(), out_of_order);

    let mut twice = sample();
    let (entry, stream) = (twice.preamble.columns[4].clone(), twice.streams[4].clone());
    twice.preamble.columns.insert(4, entry);
    twice.streams.insert(4, stream);
    assert_eq!(twice.refusal(), out_of_order);

    // Table order comes before column order: items.blob after ticks.jitter.
    let mut earlier_table = sample();
    let last = earlier_table.preamble.columns.len() - 1;
    assert_eq!(
        (
            earlier_table.preamble.columns[last - 1].table,
            earlier_table.preamble.columns[last - 1].column
        ),
        (5, 3)
    );
    (
        earlier_table.preamble.columns[last].table,
        earlier_table.preamble.columns[last].column,
    ) = (0, 13);
    assert_eq!(earlier_table.refusal(), out_of_order);
}

#[test]
fn refuses_a_column_the_artifact_does_not_have() {
    for (table, column) in [(6, 2), (7, 0), (u32::MAX, u32::MAX)] {
        let mut taken = sample();
        let last = taken.preamble.columns.len() - 1;
        (
            taken.preamble.columns[last].table,
            taken.preamble.columns[last].column,
        ) = (table, column);
        assert_eq!(
            taken.refusal(),
            UnpackError::Preamble("it names a column the artifact does not have")
        );
    }
}

#[test]
fn refuses_a_member_set_on_a_transform_that_does_not_use_it() {
    let unused = UnpackError::Preamble("it sets a member its transform does not use");
    let taken = sample();
    let (position, title) = (
        taken.entry("items", "position"),
        taken.entry("items", "title"),
    );
    type Change = fn(&mut ColumnEntry);
    let changes: [(usize, Change); 5] = [
        (position, |entry| entry.references = Some(3)),
        (position, |entry| entry.escapes = 1),
        (title, |entry| entry.references = Some(3)),
        (title, |entry| entry.escapes = 1),
        (title, |entry| entry.width = 1),
    ];
    for (entry, change) in changes {
        let mut changed = taken.clone();
        change(&mut changed.preamble.columns[entry]);
        assert_eq!(changed.refusal(), unused);
    }
}

#[test]
fn refuses_a_transform_on_a_column_of_another_type() {
    let taken = sample();
    // fk on an int64 column, with a table to refer to.
    let mut fk = taken.clone();
    let position = taken.entry("items", "position");
    fk.preamble.columns[position].transform = Transform::Fk;
    fk.preamble.columns[position].references = Some(3);
    assert_eq!(
        fk.refusal(),
        taken.defect("items", "position", Defect::Type)
    );
    for (table, column, transform) in [
        ("items", "title", Transform::Int),
        ("items", "meta", Transform::Int),
        ("items", "position", Transform::Sep),
        ("ticks", "at", Transform::Sep),
    ] {
        let mut changed = taken.clone();
        let entry = taken.entry(table, column);
        changed.preamble.columns[entry].transform = transform;
        changed.preamble.columns[entry].width = 0;
        assert_eq!(
            changed.refusal(),
            taken.defect(table, column, Defect::Type),
            "{table}.{column}"
        );
    }
    // On the columns no transform fits: a uuid one for int and sep, and
    // others for each.
    for (to, from) in [
        (("items", "id"), ("items", "position")),
        (("items", "id"), ("items", "title")),
        (("items", "total"), ("items", "position")),
        (("items", "blob"), ("items", "title")),
        (("items", "status"), ("items", "title")),
        (("items", "score"), ("items", "owner_id")),
        (("items", "day"), ("items", "owner_id")),
        (("items", "span"), ("items", "owner_id")),
    ] {
        assert_eq!(
            moved(&taken, from, to).refusal(),
            taken.defect(to.0, to.1, Defect::Type),
            "{to:?}"
        );
    }
}

/// A body whose preamble puts one transform on column 1 of table 1 of this
/// artifact, over this stream.
fn one_stream(
    artifact: &[u8],
    transform: Transform,
    references: Option<u32>,
    width: u8,
    stream: &[u8],
) -> Taken {
    Taken {
        artifact: artifact.to_vec(),
        preamble: Preamble {
            version: 1,
            hoisted: Vec::new(),
            columns: vec![ColumnEntry {
                table: 1,
                column: 1,
                transform,
                references,
                width,
                escapes: 0,
                stream_bytes: stream.len() as u64,
            }],
        },
        streams: vec![stream.to_vec()],
    }
}

/// A table of two key rows, then a table of three rows whose second column is
/// `column`.
fn keyed(column: ForgedColumn) -> Vec<u8> {
    forge(&[
        ForgedTable {
            name: "keys",
            rows: 2,
            primary_key: &["id"],
            columns: vec![forged("id", "uuid", vec![uuids(&[[0xaa; 16], [0xbb; 16]])])],
        },
        ForgedTable {
            name: "rows",
            rows: 3,
            primary_key: &[],
            columns: vec![forged("flag", "bool", vec![vec![1, 0, 1]]), column],
        },
    ])
}

#[test]
fn refuses_a_transformed_column_whose_parts_are_not_its_type_s() {
    let parts = UnpackError::Column {
        table: 1,
        column: 1,
        defect: Defect::Parts,
    };
    let present = bitmap(&[true, true, true]);
    let [lengths, values] = texts(&[b"a", b"b", b"c"]);
    let [headers, packed] = blocks(&[Block {
        base: 0,
        bits: 8,
        scale: 0,
        numbers: vec![1, 2, 3],
    }]);
    let fk = |column| one_stream(&keyed(column), Transform::Fk, Some(0), 1, &[0, 1, 0]);
    let int = |column| one_stream(&keyed(column), Transform::Int, None, 1, &[2, 2, 2]);
    let sep = |column| one_stream(&keyed(column), Transform::Sep, None, 0, b"a\xffb\xffc\xff");
    let values_of = || uuids(&[[0xaa; 16], [0xbb; 16], [0xaa; 16]]);
    let raw = || int64s(&[1, 2, 3]);

    // Each transform restores the column its parts are right for.
    let good = [
        fk(forged("c", "uuid", vec![values_of()])),
        fk(forged_nullable(
            "c",
            "uuid",
            vec![present.clone(), values_of()],
        )),
        int(forged("c", "int64", vec![raw()])),
        int(encoded(forged(
            "c",
            "timestamp",
            vec![headers.clone(), packed.clone()],
        ))),
        sep(forged("c", "utf8", vec![lengths.clone(), values.clone()])),
    ];
    for taken in &good {
        assert!(taken.unpacked().unwrap() == taken.artifact);
    }

    let bad = [
        // A row short, a row long, and a part too many.
        fk(forged("c", "uuid", vec![values_of()[..47].to_vec()])),
        fk(forged(
            "c",
            "uuid",
            vec![[values_of(), vec![0; 16]].concat()],
        )),
        fk(forged("c", "uuid", vec![values_of(), Vec::new()])),
        fk(forged("c", "uuid", Vec::new())),
        // A nullable column without a bitmap of its rows.
        fk(forged_nullable("c", "uuid", vec![values_of()])),
        fk(forged_nullable("c", "uuid", vec![vec![7, 0], values_of()])),
        fk(forged_nullable("c", "uuid", Vec::new())),
        // A bitmap on a column that is not nullable.
        fk(forged("c", "uuid", vec![present.clone(), values_of()])),
        int(forged("c", "int64", vec![raw()[..23].to_vec()])),
        int(forged("c", "int64", vec![raw(), Vec::new()])),
        int(forged("c", "int64", vec![headers.clone(), packed.clone()])),
        int(encoded(forged("c", "int64", vec![raw()]))),
        int(encoded(forged(
            "c",
            "int64",
            vec![headers[..15].to_vec(), packed.clone()],
        ))),
        int(encoded(forged(
            "c",
            "int64",
            vec![[headers.clone(), headers.clone()].concat(), packed.clone()],
        ))),
        int(encoded(forged(
            "c",
            "int64",
            vec![headers.clone(), packed.clone(), Vec::new()],
        ))),
        sep(forged(
            "c",
            "utf8",
            vec![lengths[..12].to_vec(), values.clone()],
        )),
        sep(forged("c", "utf8", vec![lengths.clone()])),
        sep(forged(
            "c",
            "json",
            vec![lengths.clone(), values.clone(), Vec::new()],
        )),
        sep(forged_nullable(
            "c",
            "json",
            vec![lengths.clone(), values.clone()],
        )),
        // The header lists the values part before the lengths part, which
        // the payload holds first.
        sep(ForgedColumn {
            listed: Some(vec![1, 0]),
            ..forged("c", "utf8", vec![values.clone(), lengths.clone()])
        }),
        int(encoded(ForgedColumn {
            listed: Some(vec![1, 0]),
            ..forged("c", "int64", vec![packed.clone(), headers.clone()])
        })),
        fk(ForgedColumn {
            listed: Some(vec![1, 0]),
            ..forged_nullable("c", "uuid", vec![values_of(), present.clone()])
        }),
    ];
    for (case, taken) in bad.iter().enumerate() {
        assert_eq!(taken.refusal(), parts, "case {case}");
    }

    // An encoding other than frame of reference in blocks of 1,024 rows.
    for encoding in [
        json!({"kind": "rle", "blockRows": 1024}),
        json!({"kind": "for", "blockRows": 512}),
    ] {
        let good = keyed(encoded(forged(
            "c",
            "int64",
            vec![headers.clone(), packed.clone()],
        )));
        let (mut header, payload_start) = header_of(&good);
        header["tables"][1]["columns"][1]["encoding"] = encoding;
        let artifact = frame(&header, &good[payload_start..]);
        assert_eq!(
            one_stream(&artifact, Transform::Int, None, 1, &[2, 2, 2]).refusal(),
            parts
        );
    }
}

#[test]
fn refuses_a_stream_length_other_than_its_rows_give() {
    let taken = sample();
    let owner = taken.entry("items", "owner_id");
    let rows = taken.defect("items", "owner_id", Defect::Rows);
    // 200 rows of two bytes and one escape are 416 bytes.
    assert_eq!(taken.streams[owner].len(), 416);
    for (escapes, stream_bytes) in [
        (1, 415),
        (1, 417),
        (0, 416),
        (2, 416),
        (1, 0),
        (u64::MAX, 416),
        (u64::MAX / 16 + 1, 416),
    ] {
        let mut changed = taken.clone();
        changed.preamble.columns[owner].escapes = escapes;
        changed.preamble.columns[owner].stream_bytes = stream_bytes;
        assert_eq!(changed.refusal(), rows, "{escapes} {stream_bytes}");
    }

    // An int stream is its present rows at its width: 978 of 1,100 at 7.
    let at = taken.entry("ticks", "at");
    assert_eq!(taken.streams[at].len(), 978 * 7);
    let rows = taken.defect("ticks", "at", Defect::Rows);
    for width in [0, 1, 6] {
        let mut changed = taken.clone();
        changed.preamble.columns[at].width = width;
        assert_eq!(changed.refusal(), rows, "width {width}");
    }
    for stream in [
        taken.streams[at][..978 * 7 - 1].to_vec(),
        [taken.streams[at].as_slice(), &[0]].concat(),
        // One delta for each row, null rows too.
        vec![0; 1_100 * 7],
        Vec::new(),
    ] {
        let mut changed = taken.clone();
        changed.restream(at, stream);
        assert_eq!(changed.refusal(), rows);
    }
}

#[test]
fn refuses_a_width_the_transform_does_not_write() {
    let taken = sample();
    // 300 owners take two bytes and 12 zones one.
    for (column, widths) in [("owner_id", [0, 1, 3, 4]), ("zone_id", [0, 2, 3, 255])] {
        let entry = taken.entry("items", column);
        for width in widths {
            let mut changed = taken.clone();
            changed.preamble.columns[entry].width = width;
            changed.preamble.columns[entry].stream_bytes =
                200 * u64::from(width) + 16 * changed.preamble.columns[entry].escapes;
            assert_eq!(
                changed.refusal(),
                taken.defect("items", column, Defect::Width),
                "{column} at {width}"
            );
        }
    }
    let position = taken.entry("items", "position");
    for width in [8, 9, 255] {
        let mut changed = taken.clone();
        changed.preamble.columns[position].width = width;
        changed.restream(position, vec![0; 200 * usize::from(width)]);
        assert_eq!(
            changed.refusal(),
            taken.defect("items", "position", Defect::Width),
            "{width}"
        );
    }
}

#[test]
fn refuses_a_reference_to_no_key_part_read_before_the_stream() {
    let taken = sample();
    let owner = taken.entry("items", "owner_id");
    let references = taken.defect("items", "owner_id", Defect::References);
    // None, the column's own table, no table, and two tables keyed on an
    // int64.
    for table in [None, Some(0), Some(7), Some(u32::MAX), Some(1), Some(4)] {
        let mut changed = taken.clone();
        changed.preamble.columns[owner].references = table;
        assert_eq!(changed.refusal(), references, "{table:?}");
    }

    // owners and zones follow items, so their keys must be hoisted.
    for (kept, column) in [(1, "owner_id"), (0, "zone_id")] {
        let mut changed = taken.clone();
        changed.preamble.hoisted = vec![taken.preamble.hoisted[kept].clone()];
        assert_eq!(
            changed.refusal(),
            taken.defect("items", column, Defect::References)
        );
    }

    // A key that is itself a stream cannot be read as rows.
    let (a, b) = ([0xaa; 16], [0xbb; 16]);
    let key = |name| ForgedTable {
        name,
        rows: 2,
        primary_key: &["id"],
        columns: vec![forged("id", "uuid", vec![uuids(&[a, b])])],
    };
    let artifact = forge(&[
        key("first"),
        key("second"),
        ForgedTable {
            name: "uses",
            rows: 2,
            primary_key: &["key"],
            columns: vec![forged("key", "uuid", vec![uuids(&[b, a])])],
        },
    ]);
    let entry = |table, references| ColumnEntry {
        table,
        column: 0,
        transform: Transform::Fk,
        references: Some(references),
        width: 1,
        escapes: 0,
        stream_bytes: 2,
    };

    // Tables without a key part: no primary key, one of two names, one that
    // names no column, and one whose column is not a uuid, is nullable, or is
    // not one part of 16 bytes a row.
    let ids = || uuids(&[a, b]);
    let keyless: Vec<(&'static [&'static str], ForgedColumn)> = vec![
        (&[], forged("id", "uuid", vec![ids()])),
        (&["id", "id"], forged("id", "uuid", vec![ids()])),
        (&["key"], forged("id", "uuid", vec![ids()])),
        (&["id"], forged("id", "decimal", vec![ids()])),
        (
            &["id"],
            forged_nullable("id", "uuid", vec![bitmap(&[true, true]), ids()]),
        ),
        (&["id"], forged("id", "uuid", vec![ids(), Vec::new()])),
        (&["id"], forged("id", "uuid", vec![ids()[..31].to_vec()])),
    ];
    for (case, (primary_key, id)) in keyless.into_iter().enumerate() {
        let artifact = forge(&[
            ForgedTable {
                name: "keys",
                rows: 2,
                primary_key,
                columns: vec![id],
            },
            ForgedTable {
                name: "uses",
                rows: 2,
                primary_key: &[],
                columns: vec![forged("key", "uuid", vec![uuids(&[b, a])])],
            },
        ]);
        let taken = Taken {
            artifact,
            preamble: Preamble {
                version: 1,
                hoisted: Vec::new(),
                columns: vec![entry(1, 0)],
            },
            streams: vec![vec![1, 0]],
        };
        assert_eq!(
            taken.refusal(),
            UnpackError::Column {
                table: 1,
                column: 0,
                defect: Defect::References
            },
            "case {case}"
        );
    }

    let mut chained = Taken {
        artifact,
        preamble: Preamble {
            version: 1,
            hoisted: Vec::new(),
            columns: vec![entry(2, 1)],
        },
        streams: vec![vec![1, 0]],
    };
    assert!(chained.unpacked().unwrap() == chained.artifact);
    chained.preamble.columns.insert(0, entry(1, 0));
    chained.streams.insert(0, vec![0, 1]);
    assert_eq!(
        chained.refusal(),
        UnpackError::Column {
            table: 2,
            column: 0,
            defect: Defect::References
        }
    );
    // A key after its stream, in a table that is not hoisted.
    chained.preamble.columns = vec![entry(1, 2)];
    chained.streams = vec![vec![1, 0]];
    assert_eq!(
        chained.refusal(),
        UnpackError::Column {
            table: 1,
            column: 0,
            defect: Defect::References
        }
    );
    chained.preamble.hoisted = vec![HoistedEntry {
        table: 2,
        column: 0,
        part: 0,
    }];
    assert!(chained.unpacked().unwrap() == chained.artifact);
}

#[test]
fn refuses_hoisted_parts_out_of_order_missing_or_replaced() {
    let taken = sample();
    let hoist = |table, column, part| HoistedEntry {
        table,
        column,
        part,
    };
    let out_of_order = UnpackError::Preamble("its hoisted parts are not in header order");
    let mut swapped = taken.clone();
    swapped.preamble.hoisted.swap(0, 1);
    assert_eq!(swapped.refusal(), out_of_order);
    let mut twice = taken.clone();
    twice.preamble.hoisted.insert(0, hoist(3, 0, 0));
    assert_eq!(twice.refusal(), out_of_order);

    // ticks.level has two parts, and there are seven tables.
    for missing in [hoist(5, 2, 2), hoist(5, 4, 0), hoist(7, 0, 0)] {
        let mut changed = taken.clone();
        changed.preamble.hoisted = vec![missing];
        assert_eq!(
            changed.refusal(),
            UnpackError::Preamble("it hoists a part the artifact does not have")
        );
    }

    // The parts streams stand for: an fk column's values, a numeric column's
    // packed part, and a text column's lengths and values.
    for replaced in [
        hoist(0, 1, 0),
        hoist(0, 3, 1),
        hoist(0, 10, 1),
        hoist(0, 10, 2),
    ] {
        let mut changed = taken.clone();
        changed.preamble.hoisted.insert(0, replaced);
        assert_eq!(
            changed.refusal(),
            UnpackError::Preamble("it hoists a part a stream replaces")
        );
    }

    // Any stored part may be hoisted: here a validity part and block headers.
    let mut changed = taken.clone();
    changed.preamble.hoisted = vec![
        hoist(0, 2, 0),
        hoist(0, 3, 0),
        hoist(3, 0, 0),
        hoist(6, 0, 0),
    ];
    assert!(changed.unpacked().unwrap() == taken.artifact);
}

#[test]
fn refuses_a_row_index_past_its_table() {
    let mut taken = sample();
    let owner = taken.entry("items", "owner_id");
    // Row 0 becomes 301, one past the escape code of 300 owners.
    taken.streams[owner][0] = 0x2d;
    taken.streams[owner][200] = 0x01;
    assert_eq!(
        taken.refusal(),
        taken.defect("items", "owner_id", Defect::RowIndex)
    );
    taken.streams[owner][200] = 0xff;
    assert_eq!(
        taken.refusal(),
        taken.defect("items", "owner_id", Defect::RowIndex)
    );
}

#[test]
fn refuses_a_null_row_with_an_index() {
    let mut taken = sample();
    let zone = taken.entry("items", "zone_id");
    // Row 1 of zone_id is null.
    assert_eq!(taken.streams[zone][1], 0);
    taken.streams[zone][1] = 1;
    assert_eq!(
        taken.refusal(),
        taken.defect("items", "zone_id", Defect::NullRow)
    );
}

#[test]
fn refuses_an_escape_past_the_escapes_and_escapes_no_row_uses() {
    let taken = sample();
    let owner = taken.entry("items", "owner_id");
    // Row 37 holds the escape code, 300, and its value follows the planes.
    assert_eq!(
        (taken.streams[owner][37], taken.streams[owner][237]),
        (0x2c, 0x01)
    );
    let mut second = taken.clone();
    second.streams[owner][0] = 0x2c;
    second.streams[owner][200] = 0x01;
    assert_eq!(
        second.refusal(),
        taken.defect("items", "owner_id", Defect::EscapePastEscapes)
    );

    let mut none = taken.clone();
    none.streams[owner][37] = 0;
    none.streams[owner][237] = 0;
    assert_eq!(
        none.refusal(),
        taken.defect("items", "owner_id", Defect::UnusedEscapes)
    );

    // zone_id has no escape, and 12 zones make 12 its escape code.
    let zone = taken.entry("items", "zone_id");
    let mut lost = taken.clone();
    lost.streams[zone][0] = 12;
    assert_eq!(
        lost.refusal(),
        taken.defect("items", "zone_id", Defect::EscapePastEscapes)
    );
}

#[test]
fn refuses_a_value_2_51_or_more_from_zero() {
    let value = UnpackError::Column {
        table: 1,
        column: 1,
        defect: Defect::Value,
    };
    let artifact = keyed(forged("c", "int64", vec![int64s(&[1, 2, 3])]));
    let stream = |zigzags: [u64; 3]| -> Vec<u8> {
        (0..7)
            .flat_map(|plane| zigzags.map(|zigzag| (zigzag >> (8 * plane)) as u8))
            .collect()
    };
    let unpacked =
        |zigzags| one_stream(&artifact, Transform::Int, None, 7, &stream(zigzags)).unpacked();
    // 2^51 - 1 and its negative are the last values inside.
    let limit = 1_u64 << 52;
    for inside in [[limit - 2, 0, 0], [limit - 3, 0, 0], [2, 2, 2]] {
        assert!(unpacked(inside).is_ok(), "{inside:?}");
    }
    for outside in [
        [limit, 0, 0],
        [limit - 1, 0, 0],
        [limit - 2, 2, 0],
        [limit - 3, 0, 1],
        [u64::MAX >> 8, 0, 0],
        [0, 0, (u64::MAX >> 8) - 1],
    ] {
        assert_eq!(unpacked(outside), Err(value.clone()), "{outside:?}");
    }
}

#[test]
fn refuses_a_block_whose_offset_is_not_the_bytes_written() {
    let mut taken = sample();
    // ticks.at has two blocks; its parts are validity, headers and packed.
    let headers = taken.part("ticks", "at", 1);
    assert_eq!(headers.len(), 32);
    taken.artifact[headers.start + 16] ^= 1;
    assert_eq!(taken.refusal(), taken.defect("ticks", "at", Defect::Block));
    let mut taken = sample();
    taken.artifact[headers.start] = 1;
    assert_eq!(taken.refusal(), taken.defect("ticks", "at", Defect::Block));
}

#[test]
fn refuses_a_block_of_another_scale_width_or_base() {
    let taken = sample();
    let headers = taken.part("ticks", "at", 1);
    // Its blocks are stored in milliseconds.
    assert_eq!(taken.artifact[headers.start + 13], 3);
    for (at, byte) in [
        // The scale: 1, 2 and 4 are not 0 or 3.
        (13, 1),
        (13, 2),
        (16 + 13, 4),
        // The width.
        (12, 65),
        (12, 255),
        // The base's top byte: 2^56 and beyond, and below zero.
        (11, 1),
        (16 + 11, 0x80),
    ] {
        let mut changed = taken.clone();
        changed.artifact[headers.start + at] = byte;
        assert_eq!(
            changed.refusal(),
            taken.defect("ticks", "at", Defect::Block),
            "byte {at} as {byte}"
        );
    }
    // 2^51 itself is outside, in either sign; one less is a base, though
    // not one this block's values fit.
    let position = taken.part("items", "position", 0);
    for base in [1_i64 << 51, -(1 << 51), i64::MAX, i64::MIN] {
        let mut changed = taken.clone();
        changed.artifact[position.start + 4..position.start + 12]
            .copy_from_slice(&base.to_le_bytes());
        assert_eq!(
            changed.refusal(),
            taken.defect("items", "position", Defect::Block),
            "{base}"
        );
    }

    // The scales above also leave their blocks too narrow for the values.
    // Here the values fit, were another scale read as 0.
    let scaled = |scale: u8| {
        let column = encoded(forged(
            "c",
            "int64",
            blocks(&[Block {
                base: 0,
                bits: 8,
                scale,
                numbers: vec![1, 2, 3],
            }])
            .into(),
        ));
        one_stream(&keyed(column), Transform::Int, None, 1, &[2, 2, 2])
    };
    assert!(scaled(0).unpacked().unwrap() == scaled(0).artifact);
    for scale in [1, 2, 4, 255] {
        assert_eq!(
            scaled(scale).refusal(),
            UnpackError::Column {
                table: 1,
                column: 1,
                defect: Defect::Block
            },
            "scale {scale}"
        );
    }
}

#[test]
fn refuses_a_base_2_51_from_zero_whose_block_would_hold_its_rows() {
    // Values 0 to 2 fit 52 bits over a base of -2^51, were that a base.
    const LIMIT: i64 = 1 << 51;
    let column = |base: i64| {
        let numbers = [0_i64, 1, 2].map(|value| (value - base) as u64).to_vec();
        encoded(forged(
            "c",
            "int64",
            blocks(&[Block {
                base,
                bits: 52,
                scale: 0,
                numbers,
            }])
            .into(),
        ))
    };
    let block = UnpackError::Column {
        table: 1,
        column: 1,
        defect: Defect::Block,
    };
    let taken = |base| one_stream(&keyed(column(base)), Transform::Int, None, 1, &[0, 2, 2]);
    assert!(taken(1 - LIMIT).unpacked().unwrap() == taken(1 - LIMIT).artifact);
    assert_eq!(taken(-LIMIT).refusal(), block);

    // No value is as large as a base of 2^51, so only a block of null rows
    // comes down to the base alone. It is refused though no row reads it.
    let nulls = |base: i64| {
        let column = encoded(forged_nullable(
            "c",
            "int64",
            [
                vec![bitmap(&[false; 3])],
                blocks(&[Block {
                    base,
                    bits: 0,
                    scale: 0,
                    numbers: vec![0; 3],
                }])
                .into(),
            ]
            .concat(),
        ));
        one_stream(&keyed(column), Transform::Int, None, 0, &[])
    };
    assert!(nulls(LIMIT - 1).unpacked().unwrap() == nulls(LIMIT - 1).artifact);
    assert_eq!(nulls(LIMIT).refusal(), block);
}

/// A body over one nullable column whose blocks have no bits and these bases,
/// and whose stream has no planes: every present row's value is 0.
fn blocks_without_bits(present: &[bool], bases: &[i64]) -> Taken {
    let of: Vec<Block> = bases
        .iter()
        .enumerate()
        .map(|(block, &base)| Block {
            base,
            bits: 0,
            scale: 0,
            numbers: vec![0; (present.len() - block * NUMERIC_BLOCK_ROWS).min(NUMERIC_BLOCK_ROWS)],
        })
        .collect();
    Taken {
        artifact: block_table("int64", Some(present), &of),
        preamble: Preamble {
            version: 1,
            hoisted: Vec::new(),
            columns: vec![ColumnEntry {
                table: 0,
                column: 0,
                transform: Transform::Int,
                references: None,
                width: 0,
                escapes: 0,
                stream_bytes: 0,
            }],
        },
        streams: vec![Vec::new()],
    }
}

#[test]
fn a_block_without_bits_under_a_stream_without_planes_holds_its_base_to_its_present_rows() {
    // Such a block is checked once and its rows are not walked, so what a
    // walk would refuse is refused here from the validity bitmap: a base
    // other than 0 on a block with a present row, whichever of its bytes
    // holds that row.
    const ROWS: usize = NUMERIC_BLOCK_ROWS + 3;
    let only = |rows: &[usize]| -> Vec<bool> { (0..ROWS).map(|row| rows.contains(&row)).collect() };
    let block = UnpackError::Column {
        table: 0,
        column: 0,
        defect: Defect::Block,
    };
    for (present, bases, refused) in [
        (only(&[]), [7, 9], false),
        (only(&[0]), [0, 9], false),
        (only(&[0]), [7, 0], true),
        // The first block's last row, and the second block's first.
        (only(&[NUMERIC_BLOCK_ROWS - 1]), [0, 9], false),
        (only(&[NUMERIC_BLOCK_ROWS - 1]), [7, 0], true),
        (only(&[NUMERIC_BLOCK_ROWS]), [7, 0], false),
        (only(&[NUMERIC_BLOCK_ROWS]), [0, 9], true),
        // The last row, in a byte the table's rows do not fill.
        (only(&[ROWS - 1]), [7, 0], false),
        (only(&[ROWS - 1]), [0, 9], true),
        (only(&[5, 700, ROWS - 2]), [0, 0], false),
    ] {
        let rows: Vec<usize> = (0..ROWS).filter(|&row| present[row]).collect();
        let taken = blocks_without_bits(&present, &bases);
        if refused {
            assert_eq!(taken.refusal(), block, "{rows:?} {bases:?}");
        } else {
            assert!(
                taken.unpacked().unwrap() == taken.artifact,
                "{rows:?} {bases:?}"
            );
        }
    }

    // The bits of the bitmap's last byte past the table's rows are no rows.
    let mut padded = blocks_without_bits(&only(&[]), &[0, 9]);
    let validity = padded.part("blocks", "value", 0);
    padded.artifact[validity.end - 1] = 0xf8;
    assert!(padded.unpacked().unwrap() == padded.artifact);
    padded.artifact[validity.end - 1] = 0xfc;
    assert_eq!(padded.refusal(), block);
}

#[test]
fn restores_a_column_of_2_30_zeros_a_block_at_a_time() {
    // Every block has no bits and a base of 0, so the packed part and the
    // stream are empty: 16 MiB of block headers stand for the rows, which
    // brotli sends in a few hundred bytes. A row at a time that is 2^30 turns
    // of the row loop, and a block at a time 2^20.
    const ROWS: u32 = 1 << 30;
    let headers = vec![0; NUMERIC_BLOCK_HEADER_BYTES * (ROWS as usize / NUMERIC_BLOCK_ROWS)];
    let artifact = forge(&[ForgedTable {
        name: "zeros",
        rows: ROWS,
        primary_key: &[],
        columns: vec![encoded(forged("c", "int64", vec![headers, Vec::new()]))],
    }]);
    let preamble = Preamble {
        version: 1,
        hoisted: Vec::new(),
        columns: vec![ColumnEntry {
            table: 0,
            column: 0,
            transform: Transform::Int,
            references: None,
            width: 0,
            escapes: 0,
            stream_bytes: 0,
        }],
    };
    let body = assemble(&artifact, &preamble, &[Vec::new()]);
    let started = Instant::now();
    restores(&body, &artifact);
    let took = started.elapsed();
    assert!(took < Duration::from_secs(5), "{took:?}");
}

#[test]
fn refuses_a_value_its_block_cannot_hold() {
    let taken = sample();
    // ticks.at is in whole milliseconds: a microsecond more is no multiple.
    let at = taken.entry("ticks", "at");
    let mut odd = taken.clone();
    assert_eq!(odd.streams[at][1], 0x20);
    odd.streams[at][1] = 0x22;
    assert_eq!(odd.refusal(), taken.defect("ticks", "at", Defect::Block));

    // items.position counts 0, 10, 20 from a base of 0 in 11 bits.
    let position = taken.entry("items", "position");
    assert_eq!(taken.streams[position][..3], [0, 20, 20]);
    let mut below = taken.clone();
    below.streams[position][0] = 1;
    assert_eq!(
        below.refusal(),
        taken.defect("items", "position", Defect::Block)
    );
    let mut wide = taken.clone();
    wide.streams[position][199] = 0xfe;
    assert_eq!(
        wide.refusal(),
        taken.defect("items", "position", Defect::Block)
    );
}

#[test]
fn refuses_blocks_that_run_past_the_packed_part_or_end_before_it() {
    let length = UnpackError::Column {
        table: 1,
        column: 1,
        defect: Defect::PartLength,
    };
    let block = |bits: u8, numbers: Vec<u64>| {
        blocks(&[Block {
            base: 0,
            bits,
            scale: 0,
            numbers,
        }])
    };
    let column = |headers: Vec<u8>, packed: Vec<u8>| {
        keyed(encoded(forged("c", "int64", vec![headers, packed])))
    };
    let stream = [2, 2, 2];
    let taken = |artifact: &[u8]| one_stream(artifact, Transform::Int, None, 1, &stream);

    let [headers, packed] = block(8, vec![1, 2, 3]);
    let good = taken(&column(headers.clone(), packed.clone()));
    assert!(good.unpacked().unwrap() == good.artifact);
    // Three bytes of blocks in a part of two, and in one of four.
    assert_eq!(
        taken(&column(headers.clone(), packed[..2].to_vec())).refusal(),
        length
    );
    assert_eq!(
        taken(&column(headers, [packed, vec![0]].concat())).refusal(),
        length
    );

    // The same at width 64: 24 bytes of rows.
    let [headers, packed] = block(64, vec![1, 2, 3]);
    let good = taken(&column(headers.clone(), packed.clone()));
    assert!(good.unpacked().unwrap() == good.artifact);
    assert_eq!(
        taken(&column(headers.clone(), packed[..23].to_vec())).refusal(),
        length
    );
    assert_eq!(
        taken(&column(headers, [packed, vec![0]].concat())).refusal(),
        length
    );
}

#[test]
fn refuses_a_separator_count_other_than_the_present_rows() {
    let taken = sample();
    for (table, column) in [("items", "title"), ("items", "note")] {
        let entry = taken.entry(table, column);
        let separators = taken.defect(table, column, Defect::Separators);
        let stream = &taken.streams[entry];
        let separator = stream.iter().position(|&byte| byte == 0xff).unwrap();
        let value = stream.iter().position(|&byte| byte != 0xff).unwrap();

        let mut fewer = taken.clone();
        fewer.streams[entry][separator] = b'x';
        assert_eq!(fewer.refusal(), separators);
        let mut more = taken.clone();
        more.streams[entry][value] = 0xff;
        assert_eq!(more.refusal(), separators);
        let mut dropped = taken.clone();
        dropped.restream(entry, stream[..stream.len() - 1].to_vec());
        assert_eq!(dropped.refusal(), separators);
        let mut added = taken.clone();
        added.restream(entry, [stream.as_slice(), &[0xff]].concat());
        assert_eq!(added.refusal(), separators);
    }
    // An all-null column has no separator.
    let ghost = taken.entry("items", "ghost");
    let mut one = taken.clone();
    one.restream(ghost, vec![0xff]);
    assert_eq!(
        one.refusal(),
        taken.defect("items", "ghost", Defect::Separators)
    );
}

#[test]
fn refuses_values_that_do_not_add_up_to_the_values_part() {
    let taken = sample();
    let title = taken.entry("items", "title");
    let length = taken.defect("items", "title", Defect::PartLength);
    let stream = &taken.streams[title];
    let mut longer = taken.clone();
    longer.restream(title, [b"x", stream.as_slice()].concat());
    assert_eq!(longer.refusal(), length);
    let value = stream.iter().position(|&byte| byte != 0xff).unwrap();
    let mut shorter = taken.clone();
    let mut cut = stream.clone();
    cut.remove(value);
    shorter.restream(title, cut);
    assert_eq!(shorter.refusal(), length);
}

#[test]
fn refuses_bytes_after_the_last_separator() {
    let taken = sample();
    let title = taken.entry("items", "title");
    // The last separator moves to the front: as many separators and as many
    // value bytes, the last of them belonging to no row.
    let mut rotated = taken.streams[title].clone();
    rotated.rotate_right(1);
    assert_eq!(rotated[0], 0xff);
    let mut changed = taken.clone();
    changed.restream(title, rotated);
    assert_eq!(
        changed.refusal(),
        taken.defect("items", "title", Defect::PartLength)
    );
}

#[test]
fn refuses_an_output_it_cannot_allocate() {
    // A text column of no row whose values part is 2^62 bytes: its stream is
    // empty, so the body is whole, and the output is what the header says.
    let header = json!({
        "format": FORMAT_NAME,
        "version": 4,
        "tables": [{
            "name": "t",
            "rows": 0,
            "primaryKey": [],
            "columns": [{
                "name": "c",
                "type": "utf8",
                "nullable": false,
                "parts": [{"offset": 0, "length": 4}, {"offset": 8, "length": 1_u64 << 62}],
            }],
        }],
    });
    let prefix = frame(&header, &[]);
    let preamble = Preamble {
        version: 1,
        hoisted: Vec::new(),
        columns: vec![ColumnEntry {
            table: 0,
            column: 0,
            transform: Transform::Sep,
            references: None,
            width: 0,
            escapes: 0,
            stream_bytes: 0,
        }],
    };
    let decoded = prefix.len() as u64 + 8 + (1 << 62);
    assert_eq!(
        refusal(&framed(&preamble.spelled(), &prefix), decoded),
        UnpackError::Allocation(decoded)
    );
}

/// A few kilobytes with each transform, a hoisted key, an escape, null rows
/// and blocks in whole milliseconds.
fn small() -> Vec<u8> {
    use ColumnKind::{Int64, Timestamp, Utf8, Uuid};
    let mut random = Random(5);
    let parents: Vec<[u8; 16]> = (0..5).map(|_| random.uuid()).collect();
    let kids = (0..121)
        .map(|row| {
            vec![
                Value::Int64(row as i64 * 3),
                match row {
                    9 => Value::Uuid(random.uuid()),
                    row if row % 10 == 3 => Value::Null,
                    _ => Value::Uuid(parents[random.below(parents.len())]),
                },
                if row % 4 == 0 {
                    Value::Null
                } else {
                    Value::Text(["a", "", "bc"][row % 3].to_owned())
                },
                Value::TimestampMicros(1_760_000_000_000_000 + row as i64 * 2_000),
            ]
        })
        .collect();
    encode(vec![
        table(
            "kids",
            &["n"],
            &[
                ("n", Int64, false),
                ("parent_id", Uuid, true),
                ("label", Utf8, true),
                ("at", Timestamp, false),
            ],
            kids,
        ),
        table(
            "parents",
            &["id"],
            &[("id", Uuid, false)],
            parents.iter().map(|&id| vec![Value::Uuid(id)]).collect(),
        ),
    ])
}

/// A table of `fill` flags, a table of two key rows, and `rows` rows of one
/// nullable column that names those keys in turn, no row of it null. Nothing
/// in it meets a size test.
fn referring(fill: u32, rows: u32) -> Vec<u8> {
    const KEYS: [[u8; 16]; 2] = [[0xaa; 16], [0xbb; 16]];
    let named: Vec<[u8; 16]> = (0..rows).map(|row| KEYS[row as usize % 2]).collect();
    forge(&[
        ForgedTable {
            name: "fill",
            rows: fill,
            primary_key: &[],
            columns: vec![forged("flag", "bool", vec![vec![1; fill as usize]])],
        },
        ForgedTable {
            name: "keys",
            rows: 2,
            primary_key: &["id"],
            columns: vec![forged("id", "uuid", vec![uuids(&KEYS)])],
        },
        ForgedTable {
            name: "rows",
            rows,
            primary_key: &[],
            columns: vec![forged_nullable(
                "key_id",
                "uuid",
                vec![bitmap(&vec![true; rows as usize]), uuids(&named)],
            )],
        },
    ])
}

#[test]
fn a_body_as_long_as_the_artifact_is_stored_and_one_a_byte_shorter_is_not() {
    // A row more of the coded column shortens the body by 15 bytes against
    // the artifact, and a flag or a byte of validity more moves it by the
    // padding that follows, so some of these bodies are exactly as long as
    // their artifact and some a byte shorter.
    let mut seen = [false; 2];
    for rows in 1..=24 {
        for fill in 0..8 {
            let artifact = referring(fill, rows);
            let every = pack_under(&artifact, Rule::Every).unwrap();
            let frozen = pack(&artifact).unwrap();
            assert_eq!(
                frozen.stored,
                every.body.len() >= artifact.len(),
                "{rows} rows after {fill} flags"
            );
            let body = if frozen.stored {
                &artifact
            } else {
                &every.body
            };
            assert!(frozen.body == *body);
            match artifact.len().checked_sub(every.body.len()) {
                Some(0) => seen[0] = true,
                Some(1) => seen[1] = true,
                _ => {}
            }
        }
    }
    assert_eq!(seen, [true, true]);
}

#[test]
fn a_cancel_at_any_ask_ends_the_pack_and_one_never_given_changes_nothing() {
    // `referring`'s asks are counted: one a table, one a column, one for the
    // table its last column is tried against, and one before a packed body is
    // checked. At 3 rows the body would be no shorter, so the artifact is
    // stored and the search's ask is the last.
    for (artifact, counted) in [
        (small(), None),
        (support::every_kind(), None),
        (referring(0, 100), Some(8)),
        (referring(0, 3), Some(7)),
    ] {
        let asked = Cell::new(0_usize);
        let never = pack_unless(&artifact, &|| {
            asked.set(asked.get() + 1);
            false
        })
        .unwrap();
        assert!(never == pack(&artifact).unwrap());
        assert_eq!(never.stored, counted == Some(7));
        let asks = asked.get();
        match counted {
            Some(counted) => assert_eq!(asks, counted),
            // A size test asks at every read of its input, so there are more
            // asks than a table's, a column's, a search's and the last one.
            None => {
                let tables = header_of(&artifact).0["tables"].as_array().unwrap().len();
                let searches = never
                    .columns
                    .iter()
                    .filter(|column| column.considered == Some(Transform::Fk))
                    .count();
                let least = tables + never.columns.len() + searches + 1;
                assert!(asks > least, "{asks} asks, {least} outside a size test");
            }
        }

        // A cancel given once is not asked for again, and no ask it is given
        // at lets a body through: not the one inside a foreign key's search,
        // where a table not tried reads as a table that does not qualify.
        for at in 0..asks {
            let asked = Cell::new(0_usize);
            let cancelled = pack_unless(&artifact, &|| {
                asked.set(asked.get() + 1);
                asked.get() == at + 1
            });
            assert!(matches!(cancelled, Err(PackError::Cancelled)), "ask {at}");
            assert_eq!(asked.get(), at + 1, "ask {at}");
        }
    }
}

#[test]
fn every_flipped_bit_is_refused_or_restores_other_bytes() {
    let artifact = small();
    let packed = pack_under(&artifact, Rule::Every).unwrap();
    assert_eq!(hoisted(&packed), [("parents", "id", 0)]);
    assert_eq!(choice(&packed, "kids", "parent_id").escapes, Some(1));
    for column in ["n", "parent_id", "label", "at"] {
        assert!(choice(&packed, "kids", column).transform.is_some());
    }
    let mut body = packed.body;
    assert!(body.len() < 8_192, "{} bytes", body.len());
    for bit in 0..8 * body.len() {
        body[bit / 8] ^= 1 << (bit % 8);
        if let Ok(restored) = unpack(&body, artifact.len() as u64) {
            assert!(restored != artifact, "bit {bit}");
        }
        body[bit / 8] ^= 1 << (bit % 8);
    }
    restores(&body, &artifact);
}

#[test]
fn a_mangled_body_is_refused_or_restores_decoded_bytes() {
    let artifact = small();
    let body = pack_under(&artifact, Rule::Every).unwrap().body;
    let mut random = Random(2_026);
    for _ in 0..6_000 {
        let mut mangled = body.clone();
        let at = random.below(mangled.len());
        let run = (1 + random.below(24)).min(mangled.len() - at);
        match random.below(6) {
            0 => mangled.truncate(at),
            1 => mangled[at..at + run].fill_with(|| random.next() as u8),
            2 => {
                let bytes: Vec<u8> = (0..run).map(|_| random.next() as u8).collect();
                mangled.splice(at..at, bytes);
            }
            3 => {
                mangled.drain(at..at + run);
            }
            4 => {
                let from = random.below(mangled.len() - run + 1);
                mangled.copy_within(from..from + run, at);
            }
            _ => mangled[at..at + run].fill([0, 0xff][random.below(2)]),
        }
        if let Ok(restored) = unpack(&mangled, artifact.len() as u64) {
            assert_eq!(restored.len(), artifact.len());
        }
    }
}

#[test]
fn a_preamble_of_any_numbers_is_refused_or_restores_decoded_bytes() {
    const NUMBERS: [u64; 13] = [
        0,
        1,
        2,
        3,
        4,
        7,
        8,
        16,
        255,
        256,
        65_536,
        u32::MAX as u64,
        u64::MAX,
    ];
    let taken = Taken::of(&small());
    let attempt = |changed: &Taken| {
        if let Ok(restored) = changed.unpacked() {
            assert_eq!(restored.len(), taken.artifact.len());
        }
    };
    for entry in 0..taken.preamble.columns.len() {
        for number in NUMBERS {
            for member in 0..6 {
                let mut changed = taken.clone();
                let column = &mut changed.preamble.columns[entry];
                match member {
                    0 => column.table = number as u32,
                    1 => column.column = number as u32,
                    2 => column.references = Some(number as u32),
                    3 => column.width = number as u8,
                    4 => column.escapes = number,
                    _ => column.stream_bytes = number,
                }
                attempt(&changed);
            }
        }
        for transform in [Transform::Fk, Transform::Int, Transform::Sep] {
            for references in [None, Some(0), Some(1)] {
                let mut changed = taken.clone();
                changed.preamble.columns[entry].transform = transform;
                changed.preamble.columns[entry].references = references;
                attempt(&changed);
                changed.preamble.columns[entry].width = 0;
                attempt(&changed);
            }
        }
    }
    for number in NUMBERS {
        for member in 0..3 {
            let mut changed = taken.clone();
            let part = &mut changed.preamble.hoisted[0];
            match member {
                0 => part.table = number as u32,
                1 => part.column = number as u32,
                _ => part.part = number as u32,
            }
            attempt(&changed);
        }
    }
    let mut none = taken.clone();
    none.preamble.hoisted.clear();
    attempt(&none);
}
