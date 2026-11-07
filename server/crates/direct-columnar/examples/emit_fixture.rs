use std::path::PathBuf;

use direct_columnar::{
    schema_hash, ArtifactIdentity, ColumnKind, ColumnSchema, Encoder, EncoderLimits, TableSchema,
    Value, WorkspaceSchema,
};

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
    let schema = WorkspaceSchema {
        tables: vec![TableSchema {
            name: "items".to_owned(),
            primary_key: vec!["signed".to_owned(), "raw".to_owned(), "uuid".to_owned()],
            columns: vec![
                column("id", ColumnKind::Dict, false),
                column("score", ColumnKind::F64, true),
                column("signed", ColumnKind::Int64, false),
                column("unsigned", ColumnKind::Uint64, false),
                column("active", ColumnKind::Bool, false),
                column("observed_at", ColumnKind::Timestamp, false),
                column("note", ColumnKind::Utf8, true),
                column("raw", ColumnKind::Bytes, false),
                column("uuid", ColumnKind::Uuid, false),
                column("amount", ColumnKind::Decimal, false),
                column("day", ColumnKind::Date, false),
                column("clock", ColumnKind::Time, false),
                column("span", ColumnKind::Interval, false),
                column("meta", ColumnKind::Json, true),
                // A nullable UUID: the key column above can never hold a null.
                column("alias", ColumnKind::Uuid, true),
            ],
        }],
    };
    let schema_hash = schema_hash(&schema)?;
    let mut encoder = Encoder::new(
        ArtifactIdentity {
            source_epoch: "source-epoch-1".to_owned(),
            manifest_version: "manifest-1".to_owned(),
            collection_set_hash: format!("sha256:{}", "1".repeat(64)),
            schema_hash,
            authorization_scope_version: "scope-1".to_owned(),
            authorization_scope_hash: format!("sha256:{}", "2".repeat(64)),
            cursor: "cursor-1".to_owned(),
        },
        schema,
        EncoderLimits::default(),
    )?;
    encoder.push_row(
        "items",
        &[
            Value::Text("βeta".to_owned()),
            Value::F64(42.5),
            Value::Int64(i64::MIN),
            Value::Uint64(u64::MAX),
            Value::Bool(true),
            Value::TimestampMicros(-1),
            Value::Text("déjà vu".to_owned()),
            Value::Bytes(vec![0, 0xff, 7]),
            Value::Uuid([0xab; 16]),
            Value::DecimalText("-1234567890.0123".to_owned()),
            Value::DateText("2026-08-09".to_owned()),
            Value::TimeText("22:30:01.123456".to_owned()),
            Value::IntervalText("P1MT2.000003S".to_owned()),
            Value::Json(br#"{"ok":true}"#.to_vec()),
            Value::Uuid([0xab; 16]),
        ],
    )?;
    encoder.push_row(
        "items",
        &[
            Value::Text("alpha".to_owned()),
            Value::Null,
            Value::Int64(i64::MAX),
            Value::Uint64(0),
            Value::Bool(false),
            Value::TimestampMicros(1_753_560_000_123_456),
            Value::Null,
            Value::Bytes(Vec::new()),
            Value::Uuid([0; 16]),
            Value::DecimalText("0".to_owned()),
            Value::DateText("1970-01-01".to_owned()),
            Value::TimeText("00:00:00".to_owned()),
            Value::IntervalText("PT0S".to_owned()),
            Value::Null,
            Value::Null,
        ],
    )?;
    let mut artifact = Vec::new();
    encoder.finish(&mut artifact)?;
    Ok(artifact)
}

fn column(name: &str, kind: ColumnKind, nullable: bool) -> ColumnSchema {
    ColumnSchema {
        name: name.to_owned(),
        kind,
        nullable,
    }
}
