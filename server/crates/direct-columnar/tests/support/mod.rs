//! Artifacts the packer's tests and its cross-language fixture are made from.
//! `src/pack/tests.rs` includes this file too.

#![allow(dead_code)]

use direct_columnar::{
    schema_hash, ArtifactIdentity, ColumnKind, ColumnSchema, Encoder, EncoderLimits, TableSchema,
    Value, WorkspaceSchema,
};

pub struct Random(pub u64);

impl Random {
    pub fn next(&mut self) -> u64 {
        self.0 = self.0.wrapping_add(0x9e37_79b9_7f4a_7c15);
        let mut value = self.0;
        value = (value ^ (value >> 30)).wrapping_mul(0xbf58_476d_1ce4_e5b9);
        value = (value ^ (value >> 27)).wrapping_mul(0x94d0_49bb_1331_11eb);
        value ^ (value >> 31)
    }

    pub fn below(&mut self, bound: usize) -> usize {
        (self.next() % bound as u64) as usize
    }

    /// Sixteen bytes that are never all zeros, a null row's stored value.
    pub fn uuid(&mut self) -> [u8; 16] {
        let mut value = [0; 16];
        value[..8].copy_from_slice(&self.next().to_le_bytes());
        value[8..].copy_from_slice(&(self.next() | 1).to_le_bytes());
        value
    }
}

/// One table's schema and rows.
pub struct Rows {
    pub schema: TableSchema,
    pub rows: Vec<Vec<Value>>,
}

pub fn table(
    name: &str,
    primary_key: &[&str],
    columns: &[(&str, ColumnKind, bool)],
    rows: Vec<Vec<Value>>,
) -> Rows {
    Rows {
        schema: TableSchema {
            name: name.to_owned(),
            primary_key: primary_key.iter().map(|&name| name.to_owned()).collect(),
            columns: columns
                .iter()
                .map(|&(name, kind, nullable)| ColumnSchema {
                    name: name.to_owned(),
                    kind,
                    nullable,
                })
                .collect(),
        },
        rows,
    }
}

/// The artifact the encoder writes for `tables`, in the order given.
pub fn encode(tables: Vec<Rows>) -> Vec<u8> {
    let schema = WorkspaceSchema {
        tables: tables.iter().map(|table| table.schema.clone()).collect(),
    };
    let mut encoder = Encoder::new(
        ArtifactIdentity {
            source_epoch: "source-epoch-1".to_owned(),
            manifest_version: "manifest-1".to_owned(),
            collection_set_hash: format!("sha256:{}", "1".repeat(64)),
            schema_hash: schema_hash(&schema).unwrap(),
            authorization_scope_version: "scope-1".to_owned(),
            authorization_scope_hash: format!("sha256:{}", "2".repeat(64)),
            cursor: "cursor-1".to_owned(),
        },
        schema,
        EncoderLimits::default(),
    )
    .unwrap();
    for table in &tables {
        for row in &table.rows {
            encoder.push_row(&table.schema.name, row).unwrap();
        }
    }
    let mut artifact = Vec::new();
    encoder.finish(&mut artifact).unwrap();
    artifact
}

const ITEMS: usize = 200;
const LINKS: usize = 60;
const OWNERS: usize = 300;
const ZONES: usize = 12;
/// More than one numeric block of 1,024 rows.
const TICKS: usize = 1_100;
/// The row of `items` whose owner is in no table.
const ORPHAN: usize = 37;
/// 2025-10-09T08:53:20Z in microseconds.
const EPOCH: i64 = 1_760_000_000_000_000;

/// An artifact with a column of every kind, null rows, an all-null column,
/// empty values, an empty table, numeric columns with and without an encoding,
/// timestamps in whole milliseconds, and foreign keys into a table before the
/// referencing one and into a table after it, one value found in neither.
///
/// The tables are in name order, as calque writes them.
pub fn every_kind() -> Vec<u8> {
    use ColumnKind::{
        Bool, Bytes, Date, Decimal, Dict, Int64, Interval, Json, Time, Timestamp, Uint64, Utf8,
        Uuid, F64,
    };
    const WORDS: [&str; 6] = ["plan", "déjà vu", "漢字", "ship", "", "review 😀"];
    const STATUSES: [&str; 4] = ["open", "closed", "blocked", "done"];
    const LEVELS: [&str; 4] = ["debug", "info", "warn", "error"];

    let mut random = Random(0x0007_e2c0_de00_0007);
    let owners: Vec<[u8; 16]> = (0..OWNERS).map(|_| random.uuid()).collect();
    let zones: Vec<[u8; 16]> = (0..ZONES).map(|_| random.uuid()).collect();

    let items = (0..ITEMS)
        .map(|row| {
            let owner = if row == ORPHAN {
                random.uuid()
            } else {
                owners[random.below(OWNERS)]
            };
            vec![
                Value::Uuid(random.uuid()),
                Value::Uuid(owner),
                if row % 4 == 1 {
                    Value::Null
                } else {
                    Value::Uuid(zones[random.below(ZONES)])
                },
                Value::Int64(10 * row as i64),
                if row % 5 == 2 {
                    Value::Null
                } else {
                    Value::Int64(random.below(500) as i64 - 250)
                },
                Value::Uint64(random.next()),
                if row % 6 == 3 {
                    Value::Null
                } else {
                    Value::F64(row as f64 / 8.0)
                },
                Value::Bool(row % 3 == 0),
                Value::TimestampMicros(EPOCH + row as i64 * 1_000_003),
                Value::Text(if row % 17 == 0 {
                    String::new()
                } else {
                    format!("Item {row}: {}", WORDS[row % WORDS.len()])
                }),
                match row % 15 {
                    0 | 3 | 6 | 9 | 12 => Value::Null,
                    5 | 10 => Value::Text(String::new()),
                    _ => Value::Text(format!("note on {}", WORDS[random.below(WORDS.len())])),
                },
                Value::Null,
                if row % 7 == 4 {
                    Value::Null
                } else {
                    Value::Json(format!(r#"{{"row":{row},"tags":[]}}"#).into_bytes())
                },
                Value::Bytes(if row == 0 {
                    vec![0xff, 0, 0xff]
                } else {
                    (0..random.below(6)).map(|_| random.next() as u8).collect()
                }),
                Value::DecimalText(format!("{}.{:03}{}", row * 13, row, 1 + row % 9)),
                Value::DateText(format!("2026-{:02}-{:02}", 1 + row % 12, 1 + row % 28)),
                Value::TimeText(format!(
                    "{:02}:{:02}:{:02}.{:06}",
                    row % 24,
                    row % 60,
                    row * 7 % 60,
                    1 + row * 4_998
                )),
                Value::IntervalText(format!("P{}DT{}.{:06}S", row % 30, row % 60, 1 + row)),
                Value::Text(STATUSES[row % STATUSES.len()].to_owned()),
            ]
        })
        .collect();
    let links = (0..LINKS)
        .map(|row| {
            vec![
                Value::Int64(row as i64),
                Value::Uuid(owners[random.below(OWNERS)]),
                if row % 6 == 5 {
                    Value::Null
                } else {
                    Value::Uuid(zones[random.below(ZONES)])
                },
            ]
        })
        .collect();
    let owner_rows = owners
        .iter()
        .enumerate()
        .map(|(row, &id)| {
            vec![
                Value::Uuid(id),
                Value::Text(format!("owner-{row:03}")),
                Value::TimestampMicros(EPOCH + row as i64 * 86_400_000_000),
            ]
        })
        .collect();
    let pair = vec![
        vec![
            Value::Int64(3),
            Value::TimestampMicros(EPOCH),
            Value::Int64(1 << 51),
            Value::Null,
        ],
        vec![
            Value::Int64(4),
            Value::TimestampMicros(EPOCH + 1_000_000),
            Value::Int64(5),
            Value::Int64(9),
        ],
    ];
    let ticks = (0..TICKS)
        .map(|row| {
            vec![
                Value::Int64(1_000 + row as i64),
                if row % 9 == 4 {
                    Value::Null
                } else {
                    Value::TimestampMicros(EPOCH + row as i64 * 250_000)
                },
                Value::Text(LEVELS[random.below(LEVELS.len())].to_owned()),
                Value::Int64(random.below(1_000) as i64),
            ]
        })
        .collect();
    let zone_rows = zones
        .iter()
        .enumerate()
        .map(|(row, &id)| vec![Value::Uuid(id), Value::Text(format!("zone {row}"))])
        .collect();

    encode(vec![
        table(
            "items",
            &["id"],
            &[
                ("id", Uuid, false),
                ("owner_id", Uuid, false),
                ("zone_id", Uuid, true),
                ("position", Int64, false),
                ("quantity", Int64, true),
                ("total", Uint64, false),
                ("score", F64, true),
                ("active", Bool, false),
                ("created_at", Timestamp, false),
                ("title", Utf8, false),
                ("note", Utf8, true),
                ("ghost", Utf8, true),
                ("meta", Json, true),
                ("blob", Bytes, false),
                ("amount", Decimal, false),
                ("day", Date, false),
                ("clock", Time, false),
                ("span", Interval, false),
                ("status", Dict, false),
            ],
            items,
        ),
        table(
            "links",
            &["id"],
            &[
                ("id", Int64, false),
                ("owner_id", Uuid, false),
                ("zone_id", Uuid, true),
            ],
            links,
        ),
        table(
            "nothing",
            &["id"],
            &[
                ("id", Uuid, false),
                ("label", Utf8, true),
                ("n", Int64, false),
                ("at", Timestamp, true),
            ],
            Vec::new(),
        ),
        table(
            "owners",
            &["id"],
            &[
                ("id", Uuid, false),
                ("name", Utf8, false),
                ("joined_at", Timestamp, false),
            ],
            owner_rows,
        ),
        table(
            "pair",
            &["id"],
            &[
                ("id", Int64, false),
                ("at", Timestamp, false),
                ("big", Int64, false),
                ("n", Int64, true),
            ],
            pair,
        ),
        table(
            "ticks",
            &["id"],
            &[
                ("id", Int64, false),
                ("at", Timestamp, true),
                ("level", Utf8, false),
                ("jitter", Int64, false),
            ],
            ticks,
        ),
        table(
            "zones",
            &["id"],
            &[("id", Uuid, false), ("label", Utf8, false)],
            zone_rows,
        ),
    ])
}

/// An artifact with no column the packer transforms and too little padding
/// for a packed body to be shorter, so its body is the artifact itself.
pub fn stored() -> Vec<u8> {
    use ColumnKind::{Bool, Uint64, F64};
    encode(vec![table(
        "measures",
        &["id"],
        &[
            ("id", Uint64, false),
            ("value", F64, true),
            ("ok", Bool, false),
        ],
        (0..5_u64)
            .map(|row| {
                vec![
                    Value::Uint64(row),
                    if row == 2 {
                        Value::Null
                    } else {
                        Value::F64(row as f64 * 1.5)
                    },
                    Value::Bool(row % 2 == 0),
                ]
            })
            .collect(),
    )])
}

/// Each table's parts in header order, as `(offset, length)` from the
/// payload's start, and the payload's start.
pub fn parts(artifact: &[u8]) -> (Vec<Vec<(usize, usize)>>, usize) {
    let header_length = u32::from_le_bytes(artifact[8..12].try_into().unwrap()) as usize;
    let header: serde_json::Value =
        serde_json::from_slice(&artifact[12..12 + header_length]).unwrap();
    let tables = header["tables"]
        .as_array()
        .unwrap()
        .iter()
        .map(|table| {
            table["columns"]
                .as_array()
                .unwrap()
                .iter()
                .flat_map(|column| column["parts"].as_array().unwrap())
                .map(|part| {
                    (
                        part["offset"].as_u64().unwrap() as usize,
                        part["length"].as_u64().unwrap() as usize,
                    )
                })
                .collect()
        })
        .collect();
    (tables, (12 + header_length).next_multiple_of(8))
}

/// The by-size table order for tables of these payload bytes: ascending, ties
/// in schema order, the largest second to last.
pub fn smallest_first_order(table_payload_bytes: &[usize]) -> Vec<usize> {
    let mut order: Vec<usize> = (0..table_payload_bytes.len()).collect();
    order.sort_by_key(|&table| (table_payload_bytes[table], table));
    if let Some(largest) = order.pop() {
        order.insert(order.len().saturating_sub(1), largest);
    }
    order
}

/// `artifact` laid out by size: the payload holds the tables in
/// [`smallest_first_order`], and the header keeps them in schema order with
/// their parts' new offsets. The encoder does not write this layout, so this
/// moves the bytes of an artifact it did write.
pub fn smallest_first(artifact: &[u8]) -> Vec<u8> {
    let (tables, payload_start) = parts(artifact);
    let sizes: Vec<usize> = tables
        .iter()
        .map(|parts| parts.iter().map(|&(_, length)| length).sum())
        .collect();
    let mut moved = vec![Vec::new(); tables.len()];
    let mut payload = Vec::new();
    for table in smallest_first_order(&sizes) {
        for &(offset, length) in &tables[table] {
            payload.resize(payload.len().next_multiple_of(8), 0);
            moved[table].push(payload.len());
            payload.extend_from_slice(&artifact[payload_start + offset..][..length]);
        }
    }

    // Only the offsets change, so the header is rewritten as text: every
    // other byte of it stays the encoder's.
    const MEMBER: &str = "\"offset\":";
    let header_length = u32::from_le_bytes(artifact[8..12].try_into().unwrap()) as usize;
    let mut rest = std::str::from_utf8(&artifact[12..12 + header_length]).unwrap();
    let mut offsets = moved.iter().flatten();
    let mut header = String::new();
    while let Some(at) = rest.find(MEMBER) {
        let digits = at + MEMBER.len();
        let end = digits
            + rest[digits..]
                .find(|character: char| !character.is_ascii_digit())
                .unwrap();
        header.push_str(&rest[..digits]);
        header.push_str(&offsets.next().unwrap().to_string());
        rest = &rest[end..];
    }
    header.push_str(rest);
    assert!(offsets.next().is_none());

    let mut relaid = artifact[..8].to_vec();
    relaid.extend_from_slice(&(header.len() as u32).to_le_bytes());
    relaid.extend_from_slice(header.as_bytes());
    relaid.resize(relaid.len().next_multiple_of(8), 0);
    relaid.extend_from_slice(&payload);
    relaid
}
