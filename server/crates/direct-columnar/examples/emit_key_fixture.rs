//! Writes one table keyed on each exact key kind, and one keyed on all of them
//! at once, for the browser decoder's primary-key index test: the decoder
//! builds the index from these columns, and every row must be found again by
//! the key its own columns hold. The numeric tables run one block for each
//! path the decoder reads a stored value by: offsets of up to 24 bits, of up
//! to 45, wider offsets, and raw 64-bit values, with timestamps also stored
//! in whole milliseconds.

use std::{collections::HashSet, path::PathBuf};

use chrono::{NaiveDate, NaiveTime};
use direct_columnar::{
    schema_hash, ArtifactIdentity, ColumnKind, ColumnSchema, Encoder, EncoderLimits, TableSchema,
    Value, WorkspaceSchema,
};

/// The rows of one numeric block, `NUMERIC_BLOCK_ROWS`.
const BLOCK_ROWS: usize = 1_024;
const ROWS: usize = 300;

const MIN_TIMESTAMP: i64 = -2_208_988_800_000_000; // 1900-01-01
const MAX_TIMESTAMP: i64 = 253_402_300_799_999_999; // 9999-12-31T23:59:59.999999

struct Random(u64);

impl Random {
    fn next(&mut self) -> u64 {
        self.0 = self
            .0
            .wrapping_mul(6_364_136_223_846_793_005)
            .wrapping_add(1_442_695_040_888_963_407);
        let value = self.0;
        (value ^ (value >> 31)).wrapping_mul(0xbf58_476d_1ce4_e5b9) ^ (value >> 29)
    }

    fn below(&mut self, bound: u64) -> u64 {
        self.next() % bound
    }

    fn between(&mut self, low: i64, high: i64) -> i64 {
        low + self.below(high.abs_diff(low) + 1) as i64
    }
}

type Generator = fn(&mut Random, usize) -> Value;

fn int64(random: &mut Random, row: usize) -> Value {
    Value::Int64(match row / BLOCK_ROWS {
        0 => random.between(-1_000_000, 1_000_000),
        1 => random.between(1 << 40, (1 << 40) + (1 << 35)),
        2 => random.between(1 << 55, (1 << 55) + (1 << 50)),
        _ if row == 3 * BLOCK_ROWS => i64::MIN,
        _ if row == 3 * BLOCK_ROWS + 1 => i64::MAX,
        _ => random.next() as i64,
    })
}

fn int64_plain(random: &mut Random, _row: usize) -> Value {
    Value::Int64(random.next() as i64)
}

fn uint64(random: &mut Random, row: usize) -> Value {
    Value::Uint64(match row / BLOCK_ROWS {
        0 => random.below(2_000_000),
        1 => (1 << 40) + random.below(1 << 35),
        2 => (1 << 60) + random.below(1 << 50),
        _ if row == 3 * BLOCK_ROWS => u64::MAX,
        _ if row == 3 * BLOCK_ROWS + 1 => 0,
        _ => random.next(),
    })
}

/// No timestamp block is raw: 1900 through 9999 spans fewer than 2^58
/// microseconds.
fn timestamp(random: &mut Random, row: usize) -> Value {
    Value::TimestampMicros(match row / BLOCK_ROWS {
        0 => random.between(-1_000_000, 1_000_000),
        1 => random.between(-1_000_000_000_000, 1_000_000_000_000),
        _ => random.between(MIN_TIMESTAMP, MAX_TIMESTAMP),
    })
}

/// Whole milliseconds, which the encoder stores at a scale of 1,000.
fn timestamp_millis(random: &mut Random, row: usize) -> Value {
    Value::TimestampMicros(
        match row / BLOCK_ROWS {
            0 => random.between(-1_000_000, 1_000_000),
            1 => random.between(-1_000_000_000_000, 1_000_000_000_000),
            _ => random.between(MIN_TIMESTAMP / 1_000, MAX_TIMESTAMP / 1_000),
        } * 1_000,
    )
}

fn boolean(_random: &mut Random, row: usize) -> Value {
    Value::Bool(row == 1)
}

fn word(random: &mut Random) -> String {
    const LETTERS: [&str; 9] = ["a", "b", "z", "0", "-", "é", "ß", "漢", "😀"];
    (0..random.below(7))
        .map(|_| LETTERS[random.below(LETTERS.len() as u64) as usize])
        .collect()
}

fn utf8(random: &mut Random, _row: usize) -> Value {
    Value::Text(word(random))
}

fn bytes(random: &mut Random, _row: usize) -> Value {
    Value::Bytes((0..random.below(9)).map(|_| random.next() as u8).collect())
}

fn uuid(random: &mut Random, _row: usize) -> Value {
    let mut value = [0; 16];
    value[..8].copy_from_slice(&random.next().to_le_bytes());
    value[8..].copy_from_slice(&random.next().to_le_bytes());
    Value::Uuid(value)
}

/// A canonical decimal: no trailing zero in the fraction.
fn decimal(random: &mut Random, _row: usize) -> Value {
    let sign = if random.below(2) == 0 { "" } else { "-" };
    Value::DecimalText(format!(
        "{sign}{}.{:03}{}",
        random.below(1_000_000_000),
        random.below(1_000),
        1 + random.below(9)
    ))
}

fn date(random: &mut Random, _row: usize) -> Value {
    let day = NaiveDate::from_ymd_opt(1_000, 1, 1).expect("valid date")
        + chrono::Days::new(random.below(3_000_000));
    Value::DateText(day.format("%Y-%m-%d").to_string())
}

fn time(random: &mut Random, _row: usize) -> Value {
    let micros = match random.below(3) {
        0 => 0,
        1 => random.below(1_000) as u32 * 1_000,
        _ => random.below(1_000_000) as u32,
    };
    let seconds = random.below(86_400) as u32;
    let clock =
        NaiveTime::from_num_seconds_from_midnight_opt(seconds, micros * 1_000).expect("valid time");
    Value::TimeText(clock.format("%H:%M:%S%.f").to_string())
}

fn interval(random: &mut Random, _row: usize) -> Value {
    let sign = if random.below(2) == 0 { "" } else { "-" };
    Value::IntervalText(format!(
        "{sign}P{}DT{}.{:06}S",
        random.below(10_000),
        random.below(86_400),
        random.below(1_000_000)
    ))
}

const KINDS: [(&str, ColumnKind, Generator, usize); 14] = [
    ("int64", ColumnKind::Int64, int64, 4 * BLOCK_ROWS),
    ("int64_plain", ColumnKind::Int64, int64_plain, ROWS),
    ("uint64", ColumnKind::Uint64, uint64, 4 * BLOCK_ROWS),
    (
        "timestamp",
        ColumnKind::Timestamp,
        timestamp,
        3 * BLOCK_ROWS,
    ),
    (
        "timestamp_millis",
        ColumnKind::Timestamp,
        timestamp_millis,
        3 * BLOCK_ROWS,
    ),
    ("bool", ColumnKind::Bool, boolean, 2),
    ("utf8", ColumnKind::Utf8, utf8, ROWS),
    ("dict", ColumnKind::Dict, utf8, ROWS),
    ("bytes", ColumnKind::Bytes, bytes, ROWS),
    ("uuid", ColumnKind::Uuid, uuid, ROWS),
    ("decimal", ColumnKind::Decimal, decimal, ROWS),
    ("date", ColumnKind::Date, date, ROWS),
    ("time", ColumnKind::Time, time, ROWS),
    ("interval", ColumnKind::Interval, interval, ROWS),
];

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let destination = std::env::args_os()
        .nth(1)
        .map(PathBuf::from)
        .ok_or("expected an output path")?;
    std::fs::write(destination, artifact()?)?;
    Ok(())
}

/// The fixture's bytes. The packer's round-trip test reads them from here.
pub fn artifact() -> Result<Vec<u8>, Box<dyn std::error::Error>> {
    let mut random = Random(0x9e37_79b9_7f4a_7c15);
    let mut tables = Vec::new();
    let mut rows = Vec::new();
    for (name, kind, generate, count) in KINDS {
        tables.push(TableSchema {
            name: name.to_owned(),
            primary_key: vec!["key".to_owned()],
            columns: vec![column("key", kind)],
        });
        let mut seen = HashSet::new();
        let mut values = Vec::new();
        while values.len() < count {
            let value = generate(&mut random, values.len());
            if seen.insert(format!("{value:?}")) {
                values.push(vec![value]);
            }
        }
        rows.push(values);
    }
    // Every kind in one key, the boolean first: two rows share each value of
    // it, so only the columns after it can tell them apart.
    tables.push(TableSchema {
        name: "composite".to_owned(),
        primary_key: KINDS.iter().map(|(name, ..)| (*name).to_owned()).collect(),
        columns: KINDS
            .iter()
            .map(|(name, kind, ..)| column(name, *kind))
            .collect(),
    });
    let mut seen = HashSet::new();
    let mut composite = Vec::new();
    while composite.len() < ROWS {
        let row = KINDS
            .iter()
            .map(|(_, _, generate, count)| generate(&mut random, composite.len() % count))
            .collect::<Vec<_>>();
        if seen.insert(format!("{row:?}")) {
            composite.push(row);
        }
    }
    rows.push(composite);

    let schema = WorkspaceSchema { tables };
    let mut encoder = Encoder::new(
        ArtifactIdentity {
            source_epoch: "source-epoch-1".to_owned(),
            manifest_version: "manifest-1".to_owned(),
            collection_set_hash: format!("sha256:{}", "1".repeat(64)),
            schema_hash: schema_hash(&schema)?,
            authorization_scope_version: "scope-1".to_owned(),
            authorization_scope_hash: format!("sha256:{}", "2".repeat(64)),
            cursor: "cursor-1".to_owned(),
        },
        schema.clone(),
        EncoderLimits::default(),
    )?;
    for (table, values) in schema.tables.iter().zip(rows) {
        for row in values {
            encoder.push_row(&table.name, &row)?;
        }
    }
    let mut artifact = Vec::new();
    encoder.finish(&mut artifact)?;
    Ok(artifact)
}

fn column(name: &str, kind: ColumnKind) -> ColumnSchema {
    ColumnSchema {
        name: name.to_owned(),
        kind,
        nullable: false,
    }
}
