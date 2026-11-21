//! An artifact cut down to some of its tables.
//!
//! [`write_subset`] copies the kept tables' parts out of an artifact the
//! encoder wrote and lays them out as the encoder would have for those tables
//! alone, so the result is byte for byte the artifact an [`Encoder`] given the
//! same rows of only those tables writes under the same identity.
//!
//! [`Encoder`]: crate::Encoder

use std::io::{self, Read, Seek, SeekFrom, Write};

use serde::Deserialize;

use crate::{
    align8, schema_hash, validate_identity, validate_schema, ArtifactIdentity, ColumnHeader,
    ColumnKind, ColumnSchema, EncodeError, EncodeReport, EncoderLimits, HashingWriter, Header,
    NumericEncoding, Part, TableHeader, TableSchema, WorkspaceSchema, FORMAT_NAME, FORMAT_VERSION,
    MAGIC, NUMERIC_BLOCK_ROWS,
};

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct SourceHeader {
    format: String,
    version: u32,
    #[allow(dead_code)]
    identity: ArtifactIdentity,
    tables: Vec<SourceTable>,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct SourceTable {
    name: String,
    rows: u32,
    primary_key: Vec<String>,
    columns: Vec<SourceColumn>,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct SourceColumn {
    name: String,
    #[serde(rename = "type")]
    kind: ColumnKind,
    nullable: bool,
    dictionary_count: Option<u32>,
    bit_width: Option<u8>,
    encoding: Option<SourceEncoding>,
    parts: Vec<SourcePart>,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct SourceEncoding {
    kind: String,
    block_rows: u32,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct SourcePart {
    offset: u64,
    length: u64,
}

/// Writes the artifact that holds only `tables` of `artifact`, under
/// `identity`, in the order `artifact`'s header lists them.
///
/// `identity.schema_hash` must be the hash of the kept tables' schema, as an
/// encoder for those tables requires, and every name in `tables` must be one
/// of `artifact`'s tables. The kept parts are copied, not decoded, and the
/// result is checked only against the header's own extents.
pub fn write_subset<R: Read + Seek, W: Write>(
    mut artifact: R,
    tables: &[&str],
    identity: &ArtifactIdentity,
    limits: EncoderLimits,
    output: W,
) -> Result<EncodeReport, EncodeError> {
    validate_identity(identity)?;
    let mut magic = [0_u8; 8];
    artifact.read_exact(&mut magic)?;
    if &magic != MAGIC {
        return Err(EncodeError::MalformedHeader("magic"));
    }
    let mut length = [0_u8; 4];
    artifact.read_exact(&mut length)?;
    let source_header_length = u64::from(u32::from_le_bytes(length));
    if source_header_length == 0 || source_header_length > limits.max_header_bytes {
        return Err(EncodeError::MalformedHeader("header length"));
    }
    let mut source_header =
        vec![0_u8; usize::try_from(source_header_length).map_err(|_| EncodeError::SizeOverflow)?];
    artifact.read_exact(&mut source_header)?;
    let source: SourceHeader = serde_json::from_slice(&source_header)?;
    if source.format != FORMAT_NAME || source.version != FORMAT_VERSION {
        return Err(EncodeError::MalformedHeader("format"));
    }
    let source_payload_start = align8(
        u64::try_from(MAGIC.len() + 4)
            .map_err(|_| EncodeError::SizeOverflow)?
            .checked_add(source_header_length)
            .ok_or(EncodeError::SizeOverflow)?,
    )?;

    if tables.is_empty() {
        return Err(EncodeError::InvalidSchema(
            "a subset keeps at least one table",
        ));
    }
    for (index, name) in tables.iter().enumerate() {
        if tables[..index].contains(name) {
            return Err(EncodeError::InvalidSchema("a subset names each table once"));
        }
        if !source.tables.iter().any(|table| table.name == *name) {
            return Err(EncodeError::UnknownTable((*name).to_owned()));
        }
    }
    let kept: Vec<&SourceTable> = source
        .tables
        .iter()
        .filter(|table| tables.contains(&table.name.as_str()))
        .collect();

    let schema = WorkspaceSchema {
        tables: kept
            .iter()
            .map(|table| TableSchema {
                name: table.name.clone(),
                primary_key: table.primary_key.clone(),
                columns: table
                    .columns
                    .iter()
                    .map(|column| ColumnSchema {
                        name: column.name.clone(),
                        kind: column.kind,
                        nullable: column.nullable,
                    })
                    .collect(),
            })
            .collect(),
    };
    validate_schema(&schema, limits)?;
    if identity.schema_hash != schema_hash(&schema)? {
        return Err(EncodeError::SchemaIdentityMismatch);
    }

    // The encoder's layout: every part in header order, each at the next
    // multiple of 8.
    let mut payload_bytes = 0_u64;
    let mut copies = Vec::new();
    let mut rows = 0_u64;
    let mut table_headers = Vec::with_capacity(kept.len());
    for table in &kept {
        rows = rows
            .checked_add(u64::from(table.rows))
            .ok_or(EncodeError::SizeOverflow)?;
        let mut columns = Vec::with_capacity(table.columns.len());
        for column in &table.columns {
            let mut parts = Vec::with_capacity(column.parts.len());
            for part in &column.parts {
                payload_bytes = align8(payload_bytes)?;
                copies.push((part.offset, part.length, payload_bytes));
                parts.push(Part {
                    offset: payload_bytes,
                    length: part.length,
                });
                payload_bytes = payload_bytes
                    .checked_add(part.length)
                    .ok_or(EncodeError::SizeOverflow)?;
            }
            columns.push(ColumnHeader {
                name: &column.name,
                kind: column.kind,
                nullable: column.nullable,
                dictionary_count: column.dictionary_count,
                bit_width: column.bit_width,
                encoding: column.encoding.as_ref().map(numeric_encoding).transpose()?,
                parts,
            });
        }
        table_headers.push(TableHeader {
            name: &table.name,
            rows: table.rows,
            primary_key: &table.primary_key,
            columns,
        });
    }
    if rows > limits.max_rows {
        return Err(EncodeError::RowLimit(limits.max_rows));
    }
    let header = serde_json::to_vec(&Header {
        format: FORMAT_NAME,
        version: FORMAT_VERSION,
        identity,
        tables: table_headers,
    })?;
    if header.is_empty() || header.len() as u64 > limits.max_header_bytes {
        return Err(EncodeError::HeaderLimit(limits.max_header_bytes));
    }
    let header_length = u32::try_from(header.len()).map_err(|_| EncodeError::SizeOverflow)?;
    let payload_start = align8(
        u64::try_from(MAGIC.len() + 4 + header.len()).map_err(|_| EncodeError::SizeOverflow)?,
    )?;
    let expected_bytes = payload_start
        .checked_add(payload_bytes)
        .ok_or(EncodeError::SizeOverflow)?;
    if expected_bytes > limits.max_artifact_bytes {
        return Err(EncodeError::ArtifactLimit(limits.max_artifact_bytes));
    }

    let mut output = HashingWriter::new(output, limits.max_artifact_bytes);
    output.write_all(MAGIC)?;
    output.write_all(&header_length.to_le_bytes())?;
    output.write_all(&header)?;
    output.pad_to(payload_start)?;
    for (source_offset, length, offset) in copies {
        output.pad_to(payload_start + offset)?;
        let start = source_payload_start
            .checked_add(source_offset)
            .ok_or(EncodeError::SizeOverflow)?;
        artifact.seek(SeekFrom::Start(start))?;
        let copied = io::copy(&mut (&mut artifact).take(length), &mut output)?;
        if copied != length {
            return Err(EncodeError::MalformedHeader(
                "a part ends past the artifact",
            ));
        }
    }
    if output.bytes != expected_bytes {
        return Err(EncodeError::EncodedLength {
            expected: expected_bytes,
            actual: output.bytes,
        });
    }
    let (bytes, sha256) = output.finish();
    Ok(EncodeReport {
        bytes,
        sha256,
        rows,
    })
}

fn numeric_encoding(encoding: &SourceEncoding) -> Result<NumericEncoding, EncodeError> {
    if encoding.kind != "for" || encoding.block_rows as usize != NUMERIC_BLOCK_ROWS {
        return Err(EncodeError::MalformedHeader("encoding"));
    }
    Ok(NumericEncoding {
        kind: "for",
        block_rows: encoding.block_rows,
    })
}

#[cfg(test)]
mod tests {
    use std::io::Cursor;

    use super::*;
    use crate::{Encoder, Value};

    fn column(name: &str, kind: ColumnKind, nullable: bool) -> ColumnSchema {
        ColumnSchema {
            name: name.to_owned(),
            kind,
            nullable,
        }
    }

    fn table(name: &str, columns: Vec<ColumnSchema>) -> TableSchema {
        TableSchema {
            name: name.to_owned(),
            primary_key: vec!["id".to_owned()],
            columns,
        }
    }

    /// Three tables with a frame-of-reference column, a dictionary, nullable
    /// text and uuids, whose part lengths are not multiples of 8.
    fn schema() -> WorkspaceSchema {
        WorkspaceSchema {
            tables: vec![
                table(
                    "projects",
                    vec![
                        column("id", ColumnKind::Uuid, false),
                        column("name", ColumnKind::Utf8, true),
                        column("created", ColumnKind::Timestamp, false),
                    ],
                ),
                table(
                    "tags",
                    vec![
                        column("id", ColumnKind::Int64, false),
                        column("label", ColumnKind::Dict, false),
                    ],
                ),
                table(
                    "tasks",
                    vec![
                        column("id", ColumnKind::Int64, false),
                        column("project", ColumnKind::Uuid, true),
                        column("done", ColumnKind::Bool, false),
                        column("note", ColumnKind::Utf8, true),
                    ],
                ),
            ],
        }
    }

    fn kept(schema: &WorkspaceSchema, names: &[&str]) -> WorkspaceSchema {
        WorkspaceSchema {
            tables: schema
                .tables
                .iter()
                .filter(|table| names.contains(&table.name.as_str()))
                .cloned()
                .collect(),
        }
    }

    fn identity(schema: &WorkspaceSchema, collection: char) -> ArtifactIdentity {
        ArtifactIdentity {
            source_epoch: "source-epoch-1".to_owned(),
            manifest_version: "3".to_owned(),
            collection_set_hash: format!("sha256:{}", collection.to_string().repeat(64)),
            schema_hash: schema_hash(schema).unwrap(),
            authorization_scope_version: "1".to_owned(),
            authorization_scope_hash: format!("sha256:{}", "2".repeat(64)),
            cursor: "cursor-7".to_owned(),
        }
    }

    fn rows(table: &str) -> Vec<Vec<Value>> {
        match table {
            "projects" => (0..200_u8)
                .map(|row| {
                    vec![
                        Value::Uuid([row; 16]),
                        if row % 3 == 0 {
                            Value::Null
                        } else {
                            Value::Text(format!("project {row}"))
                        },
                        Value::TimestampMicros(1_700_000_000_000_000 + i64::from(row) * 1_000),
                    ]
                })
                .collect(),
            "tags" => (0..5)
                .map(|row| {
                    vec![
                        Value::Int64(row),
                        Value::Text(["red", "blue", "green"][row as usize % 3].to_owned()),
                    ]
                })
                .collect(),
            _ => (0..2_500)
                .map(|row| {
                    vec![
                        Value::Int64(10_000 + row),
                        if row % 7 == 0 {
                            Value::Null
                        } else {
                            Value::Uuid([(row % 200) as u8; 16])
                        },
                        Value::Bool(row % 2 == 0),
                        if row % 5 == 0 {
                            Value::Null
                        } else {
                            Value::Text("x".repeat((row % 11) as usize))
                        },
                    ]
                })
                .collect(),
        }
    }

    fn encode(identity: &ArtifactIdentity, schema: &WorkspaceSchema) -> Vec<u8> {
        let mut encoder =
            Encoder::new(identity.clone(), schema.clone(), EncoderLimits::default()).unwrap();
        for table in &schema.tables {
            for row in rows(&table.name) {
                encoder.push_row(&table.name, &row).unwrap();
            }
        }
        let mut bytes = Vec::new();
        encoder.finish(&mut bytes).unwrap();
        bytes
    }

    fn subset(
        artifact: &[u8],
        tables: &[&str],
        identity: &ArtifactIdentity,
    ) -> Result<(Vec<u8>, EncodeReport), EncodeError> {
        let mut bytes = Vec::new();
        let report = write_subset(
            Cursor::new(artifact),
            tables,
            identity,
            EncoderLimits::default(),
            &mut bytes,
        )?;
        Ok((bytes, report))
    }

    #[test]
    fn a_subset_is_the_artifact_an_encoder_writes_for_its_tables_alone() {
        let full = schema();
        let artifact = encode(&identity(&full, '1'), &full);
        for names in [
            &["projects", "tasks"][..],
            &["tasks", "projects"],
            &["tags"],
            &["projects", "tags", "tasks"],
        ] {
            let narrow = kept(&full, names);
            let narrow_identity = identity(&narrow, '3');
            let (bytes, report) = subset(&artifact, names, &narrow_identity).unwrap();
            assert_eq!(bytes, encode(&narrow_identity, &narrow), "{names:?}");
            assert_eq!(report.bytes as usize, bytes.len());
            assert_eq!(
                report.sha256,
                hex::encode(<sha2::Sha256 as sha2::Digest>::digest(&bytes))
            );
            let expected_rows: usize = narrow
                .tables
                .iter()
                .map(|table| rows(&table.name).len())
                .sum();
            assert_eq!(report.rows as usize, expected_rows);
        }
    }

    #[test]
    fn a_subset_refuses_a_table_the_artifact_lacks_an_empty_or_repeated_list_and_a_wrong_schema() {
        let full = schema();
        let artifact = encode(&identity(&full, '1'), &full);
        let narrow = kept(&full, &["tasks"]);
        assert!(matches!(
            subset(&artifact, &["tasks", "users"], &identity(&narrow, '3')),
            Err(EncodeError::UnknownTable(table)) if table == "users"
        ));
        assert!(matches!(
            subset(&artifact, &[], &identity(&narrow, '3')),
            Err(EncodeError::InvalidSchema(_))
        ));
        assert!(matches!(
            subset(&artifact, &["tasks", "tasks"], &identity(&narrow, '3')),
            Err(EncodeError::InvalidSchema(_))
        ));
        // An identity whose schema hash names the whole artifact.
        assert!(matches!(
            subset(&artifact, &["tasks"], &identity(&full, '3')),
            Err(EncodeError::SchemaIdentityMismatch)
        ));
    }

    #[test]
    fn a_subset_of_a_truncated_artifact_fails_instead_of_writing_a_short_part() {
        let full = schema();
        let artifact = encode(&identity(&full, '1'), &full);
        let narrow = kept(&full, &["tasks"]);
        let truncated = &artifact[..artifact.len() - 9];
        assert!(matches!(
            subset(truncated, &["tasks"], &identity(&narrow, '3')),
            Err(EncodeError::MalformedHeader(_))
        ));
        let mut damaged = artifact.clone();
        damaged[..8].copy_from_slice(b"CALQPAK1");
        assert!(matches!(
            subset(&damaged, &["tasks"], &identity(&narrow, '3')),
            Err(EncodeError::MalformedHeader("magic"))
        ));
    }
}
