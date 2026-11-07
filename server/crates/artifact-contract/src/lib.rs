//! Validation boundary for immutable snapshot artifact manifests.
//!
//! The checked-in JSON Schema is the structural authority. This crate adds
//! only cross-field checks that JSON Schema cannot express and byte-integrity
//! verification for decoded artifacts and their transport representations.
//! It does not own catalog state, blob storage, delivery URLs, authorization
//! policy, or bootstrap planning.

use std::{
    collections::{HashMap, HashSet},
    sync::OnceLock,
};

use serde::{Deserialize, Serialize};
use serde_json::Value;
use sha2::{Digest, Sha256};
use sync_types::{ArtifactId, ObjectId, OpaqueCursor, SourceEpoch, TableId};
use thiserror::Error;

const MANIFEST_SCHEMA_JSON: &str = include_str!("../../../../spec/artifact-manifest.schema.json");

static MANIFEST_SCHEMA: OnceLock<Result<jsonschema::Validator, String>> = OnceLock::new();

/// A manifest which passed the checked-in JSON Schema and semantic checks.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ValidatedArtifactManifest(ArtifactManifest);

impl ValidatedArtifactManifest {
    /// Parse and validate one complete JSON manifest.
    pub fn parse(bytes: &[u8]) -> Result<Self, ManifestError> {
        let value: Value = serde_json::from_slice(bytes).map_err(ManifestError::Json)?;
        validate_schema(&value)?;

        let manifest: ArtifactManifest =
            serde_json::from_value(value).map_err(ManifestError::TypedDecode)?;
        validate_semantics(&manifest)?;
        Ok(Self(manifest))
    }

    pub fn manifest(&self) -> &ArtifactManifest {
        &self.0
    }

    /// The exact compatibility dimensions an artifact is reused under.
    pub fn cache_key(&self) -> ArtifactCacheKey {
        ArtifactCacheKey {
            source_epoch: self.0.pit.source_epoch.clone(),
            publication_generation: self.0.compatibility.publication_generation,
            schema_generation: self.0.compatibility.schema_generation,
            authorization_generation: self.0.compatibility.authorization_generation,
            authorization_policy_hash: self.0.compatibility.authorization_policy_hash.clone(),
            authorization_scope_hash: self.0.compatibility.authorization_scope_hash.clone(),
            shard_profile_hash: self.0.compatibility.shard_profile_hash.clone(),
            index_profile_hash: self.0.compatibility.index_profile_hash.clone(),
            decoder_profile: self.0.decoded.format.decoder_profile(),
            type_codec_generation: self.0.compatibility.type_codec_generation,
            collation_generation: self.0.compatibility.collation_generation,
        }
    }

    /// Verify the exact decoded artifact bytes before deserialization.
    pub fn verify_decoded_bytes(&self, bytes: &[u8]) -> Result<(), IntegrityError> {
        verify_bytes(
            "decoded artifact",
            self.0.decoded.bytes,
            &self.0.decoded.sha256,
            bytes,
        )
    }

    /// Cross-check the immutable manifest with one dynamic bootstrap selection.
    ///
    /// Cursor tokens remain opaque: this proves equality of the artifact's
    /// snapshot boundary, never client-side ordering.
    pub fn validate_bootstrap_selection(
        &self,
        source: &BootstrapSourceCompatibility,
        install: &BootstrapArtifactInstall,
    ) -> Result<(), SelectionError> {
        let manifest = &self.0;
        if source.source_epoch != manifest.pit.source_epoch {
            return Err(SelectionError::SourceEpoch);
        }
        if source.publication_generation != manifest.compatibility.publication_generation {
            return Err(SelectionError::PublicationGeneration);
        }
        if source.schema_generation != manifest.compatibility.schema_generation {
            return Err(SelectionError::SchemaGeneration);
        }
        if source.authorization_generation != manifest.compatibility.authorization_generation {
            return Err(SelectionError::AuthorizationGeneration);
        }
        if source.authorization_policy_hash != manifest.compatibility.authorization_policy_hash {
            return Err(SelectionError::AuthorizationPolicy);
        }
        if source.authorization_scope_hash != manifest.compatibility.authorization_scope_hash {
            return Err(SelectionError::AuthorizationScope);
        }
        if install.artifact_id != manifest.artifact_id {
            return Err(SelectionError::ArtifactId);
        }
        if install.snapshot_cursor != manifest.pit.snapshot_cursor {
            return Err(SelectionError::SnapshotCursor);
        }
        if install.decoded_sha256 != manifest.decoded.sha256 {
            return Err(SelectionError::DecodedDigest);
        }
        if install.decoded_bytes != manifest.decoded.bytes {
            return Err(SelectionError::DecodedLength);
        }
        if install.buffer_capacity_bytes < install.decoded_bytes {
            return Err(SelectionError::BufferCapacity);
        }

        let manifest_tables: HashSet<_> = manifest
            .tables
            .iter()
            .map(|table| &table.table_id)
            .collect();
        let install_tables: HashSet<_> = install.tables.iter().collect();
        if install.tables.len() != manifest.tables.len() || install_tables != manifest_tables {
            return Err(SelectionError::TableSet);
        }

        let selected = &install.selected_representation;
        let representation = manifest
            .representations
            .iter()
            .find(|candidate| candidate.representation_id == selected.representation_id)
            .ok_or(SelectionError::RepresentationMissing)?;
        if representation.encoding != selected.encoding {
            return Err(SelectionError::RepresentationEncoding);
        }
        if representation.encoded_bytes != selected.encoded_bytes {
            return Err(SelectionError::RepresentationLength);
        }

        Ok(())
    }
}

fn manifest_validator() -> Result<&'static jsonschema::Validator, ManifestError> {
    let compiled = MANIFEST_SCHEMA.get_or_init(|| {
        let schema: Value = serde_json::from_str(MANIFEST_SCHEMA_JSON)
            .map_err(|error| format!("checked-in schema is invalid JSON: {error}"))?;
        jsonschema::draft202012::options()
            .should_validate_formats(true)
            .build(&schema)
            .map_err(|error| format!("checked-in schema cannot compile: {error}"))
    });

    compiled
        .as_ref()
        .map_err(|message| ManifestError::SchemaDefinition(message.clone()))
}

fn validate_schema(value: &Value) -> Result<(), ManifestError> {
    let validator = manifest_validator()?;
    let violations: Vec<_> = validator
        .iter_errors(value)
        .map(|error| SchemaViolation {
            instance_path: error.instance_path().to_string(),
            message: error.to_string(),
        })
        .collect();
    if violations.is_empty() {
        Ok(())
    } else {
        Err(ManifestError::Schema(violations))
    }
}

fn validate_semantics(manifest: &ArtifactManifest) -> Result<(), ManifestError> {
    let mut table_ids = HashSet::with_capacity(manifest.tables.len());
    let mut local_tables = HashSet::with_capacity(manifest.tables.len());
    for table in &manifest.tables {
        if !table_ids.insert(&table.table_id) {
            return Err(SemanticError::DuplicateTableId(table.table_id.to_string()).into());
        }
        if !local_tables.insert((&table.local_schema, &table.local_table)) {
            return Err(SemanticError::DuplicateLocalTable {
                schema: table.local_schema.clone(),
                table: table.local_table.clone(),
            }
            .into());
        }
    }
    let DecodedArtifactFormat::DirectColumnar { schema, chunks, .. } = &manifest.decoded.format;
    validate_direct_columnar_tables(manifest, schema)?;
    validate_chunks(manifest.decoded.bytes, chunks)?;

    let mut representation_ids = HashSet::with_capacity(manifest.representations.len());
    let mut object_ids = HashMap::with_capacity(manifest.representations.len() + 1);
    let decoded_content_id = format!("sha256:{}", manifest.decoded.sha256);
    if manifest.decoded.object_id.to_string() != decoded_content_id {
        return Err(SemanticError::DecodedObjectIdNotContentAddressed.into());
    }
    object_ids.insert(
        &manifest.decoded.object_id,
        (manifest.decoded.sha256.as_str(), manifest.decoded.bytes),
    );
    for representation in &manifest.representations {
        let content_id = format!("sha256:{}", representation.encoded_sha256);
        if representation.representation_id != content_id {
            return Err(SemanticError::RepresentationIdNotContentAddressed.into());
        }
        if representation.object_id.to_string() != content_id {
            return Err(SemanticError::RepresentationObjectIdNotContentAddressed.into());
        }
        if representation.encoding == RepresentationEncoding::Identity
            && (representation.encoded_sha256 != manifest.decoded.sha256
                || representation.encoded_bytes != manifest.decoded.bytes)
        {
            return Err(SemanticError::IdentityRepresentationMismatch.into());
        }
        // What a brotli stream decodes to, whose length its window covers.
        let covered = match representation.encoding {
            RepresentationEncoding::BrotliLargeWindow => Some(manifest.decoded.bytes),
            RepresentationEncoding::PackedBrotliLargeWindow => {
                // The schema requires `packed` on this encoding.
                let body_bytes = representation.packed.map_or(0, |packed| packed.body_bytes);
                // The packer writes a body only where it is shorter than the
                // artifact, and no body is empty.
                if body_bytes == 0 || body_bytes >= manifest.decoded.bytes {
                    return Err(SemanticError::PackedBody {
                        body_bytes,
                        decoded_bytes: manifest.decoded.bytes,
                    }
                    .into());
                }
                Some(body_bytes)
            }
            RepresentationEncoding::Zstd | RepresentationEncoding::Identity => None,
        };
        if let Some(covered) = covered {
            let expected = brotli_covering_window_bits(covered);
            // The schema requires `brotli` on both encodings.
            let actual = representation.brotli.map_or(0, |brotli| brotli.window_bits);
            if expected != Some(actual) {
                return Err(SemanticError::BrotliWindow { expected, actual }.into());
            }
        }
        if !representation_ids.insert(&representation.representation_id) {
            return Err(SemanticError::DuplicateRepresentationId(
                representation.representation_id.clone(),
            )
            .into());
        }

        let fingerprint = (
            representation.encoded_sha256.as_str(),
            representation.encoded_bytes,
        );
        if object_ids
            .insert(&representation.object_id, fingerprint)
            .is_some_and(|existing| existing != fingerprint)
        {
            return Err(SemanticError::ObjectIdContentMismatch(
                representation.object_id.to_string(),
            )
            .into());
        }
    }

    Ok(())
}

/// The chunk digests cover the decoded artifact exactly: one per
/// [`DIRECT_COLUMNAR_CHUNK_BYTES`] and one for what is left, no more and no
/// fewer. The schema pins the size and the digest form; the count depends on
/// the decoded length, which a schema cannot compare against.
fn validate_chunks(decoded_bytes: u64, chunks: &ArtifactChunks) -> Result<(), ManifestError> {
    if chunks.bytes != DIRECT_COLUMNAR_CHUNK_BYTES {
        return Err(SemanticError::ChunkSize(chunks.bytes).into());
    }
    let expected = decoded_bytes.div_ceil(DIRECT_COLUMNAR_CHUNK_BYTES);
    if chunks.sha256.len() as u64 != expected {
        return Err(SemanticError::ChunkCount {
            expected,
            actual: chunks.sha256.len(),
        }
        .into());
    }
    Ok(())
}

/// A direct-columnar manifest binds its physical schema to its tables: the
/// schema names exactly the manifest's local tables (the client looks a
/// collection's physical columns up by that name), and every table carries
/// distinct wire column ids and names (a change contract compiles from them).
fn validate_direct_columnar_tables(
    manifest: &ArtifactManifest,
    schema: &DirectColumnarSchema,
) -> Result<(), ManifestError> {
    let mut expected: Vec<String> = manifest
        .tables
        .iter()
        .map(|table| table.local_table.clone())
        .collect();
    expected.sort();
    let mut actual: Vec<String> = schema
        .tables
        .iter()
        .map(|table| table.name.clone())
        .collect();
    actual.sort();
    if expected != actual {
        return Err(SemanticError::SchemaTableMismatch { expected, actual }.into());
    }
    for table in &manifest.tables {
        if table.columns.is_empty() {
            return Err(SemanticError::WireColumnsMissing(table.table_id.to_string()).into());
        }
        let mut ids = HashSet::with_capacity(table.columns.len());
        let mut names = HashSet::with_capacity(table.columns.len());
        for column in &table.columns {
            if !ids.insert(column.column_id) || !names.insert(column.name.as_str()) {
                return Err(SemanticError::DuplicateWireColumn {
                    table: table.table_id.to_string(),
                }
                .into());
            }
        }
    }
    Ok(())
}

fn verify_bytes(
    kind: &'static str,
    expected_bytes: u64,
    expected_sha256: &str,
    bytes: &[u8],
) -> Result<(), IntegrityError> {
    let actual_bytes = u64::try_from(bytes.len()).map_err(|_| IntegrityError::Length {
        kind,
        expected: expected_bytes,
        actual: u64::MAX,
    })?;
    if actual_bytes != expected_bytes {
        return Err(IntegrityError::Length {
            kind,
            expected: expected_bytes,
            actual: actual_bytes,
        });
    }

    let actual_sha256 = format!("{:x}", Sha256::digest(bytes));
    if actual_sha256 != expected_sha256 {
        return Err(IntegrityError::Digest {
            kind,
            expected: expected_sha256.to_owned(),
            actual: actual_sha256,
        });
    }
    Ok(())
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct ArtifactManifest {
    pub protocol_version: u32,
    pub artifact_id: ArtifactId,
    pub created_at: String,
    pub decoded: DecodedArtifact,
    pub pit: PointInTimeMetadata,
    pub compatibility: ArtifactCompatibility,
    pub tables: Vec<ArtifactTable>,
    pub representations: Vec<ArtifactRepresentation>,
    pub memory: ArtifactMemory,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub signature: Option<ManifestSignature>,
}

/// Media type of a decoded direct-columnar (`CALQART1`-framed) artifact. The
/// suffix tracks the artifact's format version, not the container tag.
pub const DIRECT_COLUMNAR_MEDIA_TYPE: &str = "application/vnd.calque.direct-columnar.v4";

/// The length of each chunk a direct-columnar manifest digests, the last
/// excepted. The client refuses any other size, so no manifest can ask it for
/// a digest of every few bytes.
pub const DIRECT_COLUMNAR_CHUNK_BYTES: u64 = 4 * 1024 * 1024;

/// The smallest and largest window, in bits, a large-window brotli stream
/// declares.
pub const BROTLI_MIN_WINDOW_BITS: u8 = 10;
pub const BROTLI_MAX_WINDOW_BITS: u8 = 30;

/// The version of the packed body a `packed-brotli-large-window`
/// representation holds: `direct-columnar`'s `pack` module writes it and the
/// client's unpacker reads no other.
pub const PACKED_BODY_VERSION: u32 = 1;

/// The smallest window, in bits, that covers `decoded_bytes`, or `None` when
/// the largest does not. The client's `decoderWindowBytes` computes the same
/// from a plan's `decodedBytes`; `spec/fixtures/brotli-large-window/windows.json`
/// holds both to one table. A packed body is shorter than its artifact, so the
/// window the client allows covers a packed object's stream too.
pub fn brotli_covering_window_bits(decoded_bytes: u64) -> Option<u8> {
    (BROTLI_MIN_WINDOW_BITS..=BROTLI_MAX_WINDOW_BITS).find(|bits| decoded_bytes <= 1_u64 << bits)
}

/// One decoded artifact object, described by its own format.
///
/// `deny_unknown_fields` cannot be combined with a flattened field, so unknown
/// keys under `decoded` are rejected by the checked-in JSON Schema, which is
/// the structural authority for this crate.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct DecodedArtifact {
    pub object_id: ObjectId,
    pub sha256: String,
    pub bytes: u64,
    pub media_type: String,
    #[serde(flatten)]
    pub format: DecodedArtifactFormat,
}

/// Format-specific identity of one decoded artifact, selected by the
/// `format` tag the manifest and the bootstrap plan both carry. The protocol
/// defines one format.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
#[serde(tag = "format", deny_unknown_fields)]
pub enum DecodedArtifactFormat {
    /// A `CALQART1` artifact as produced by the `direct-columnar` encoder.
    ///
    /// `identity` and `schema` are the header facts the two hashes do not pin:
    /// the client binds the bytes it downloads to them before decoding, and
    /// re-hashes `schema` to `schema_hash` before it reads payload. They travel
    /// with the format because the manifest is the only thing that may say what
    /// the bytes are; a plan copies them verbatim (`spec/protocol-notes.md`).
    #[serde(rename = "directColumnar", rename_all = "camelCase")]
    DirectColumnar {
        schema_hash: String,
        collection_set_hash: String,
        format_version: u32,
        identity: DirectColumnarIdentity,
        schema: DirectColumnarSchema,
        chunks: ArtifactChunks,
    },
}

/// The SHA-256 of each consecutive `bytes` bytes of a decoded artifact, in
/// order, the last covering what is left, as `sha256:<hex>`. There are
/// exactly ceil(decoded length / `bytes`) of them.
///
/// A plan copies them with the rest of the format, and the client verifies
/// each chunk as it arrives and reads no byte of the artifact until every one
/// has matched. The decoded `sha256` stays the artifact's content address;
/// the preparation that produces these digests reads them off the bytes it
/// checks against that address.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct ArtifactChunks {
    pub bytes: u64,
    pub sha256: Vec<String>,
}

/// The decoder-compatibility identity of a direct-columnar artifact at
/// `format_version`; see [`DecodedArtifactFormat::decoder_profile`].
pub fn direct_columnar_decoder_profile(format_version: u32) -> String {
    format!("calque-direct-columnar/v{format_version}")
}

/// The two header strings of a direct-columnar artifact that `schemaHash`
/// and `collectionSetHash` do not repeat. They are the strings the encoder
/// wrote, verbatim; the client compares them with the header it decodes.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct DirectColumnarIdentity {
    pub manifest_version: String,
    pub authorization_scope_version: String,
}

/// The physical schema of a direct-columnar image, in the key order
/// `schemaHash` digests (`format`, `version`, `tables[name, primaryKey,
/// columns[name, type, nullable]]`). It mirrors the encoder's
/// `SchemaContract` without depending on the encoder.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct DirectColumnarSchema {
    pub format: String,
    pub version: u32,
    pub tables: Vec<DirectColumnarTableSchema>,
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct DirectColumnarTableSchema {
    pub name: String,
    pub primary_key: Vec<String>,
    pub columns: Vec<DirectColumnarColumnSchema>,
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct DirectColumnarColumnSchema {
    pub name: String,
    #[serde(rename = "type")]
    pub kind: DirectColumnarColumnKind,
    pub nullable: bool,
}

/// The physical column kinds of the direct-columnar format, spelled as the
/// encoder serializes them.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize, Serialize)]
#[serde(rename_all = "lowercase")]
pub enum DirectColumnarColumnKind {
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

impl DecodedArtifactFormat {
    /// The decoder-compatibility identity this format contributes to the
    /// artifact cache key: an artifact is only reusable by a client whose
    /// decoder matches the profile that produced it.
    pub fn decoder_profile(&self) -> String {
        match self {
            Self::DirectColumnar { format_version, .. } => {
                direct_columnar_decoder_profile(*format_version)
            }
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct PointInTimeMetadata {
    pub source_epoch: SourceEpoch,
    pub snapshot_cursor: OpaqueCursor,
    pub copy_consistent_lsn: String,
    pub cut_message_lsn: String,
    pub cut_marker_id: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct ArtifactCompatibility {
    pub publication_generation: u64,
    pub schema_generation: u64,
    pub authorization_generation: u64,
    pub authorization_policy_hash: String,
    pub authorization_scope_hash: String,
    pub table_set_hash: String,
    pub shard_profile_hash: String,
    pub index_profile_hash: String,
    pub type_codec_generation: u64,
    pub collation_generation: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct ArtifactTable {
    pub table_id: TableId,
    pub source_schema: String,
    pub source_table: String,
    pub local_schema: String,
    pub local_table: String,
    pub schema_hash: String,
    pub column_profile_hash: String,
    pub row_count: u64,
    pub primary_key_codec: String,
    /// The change stream's column ids for this table, in the image's column
    /// order, so the plan can hand the client its change contract.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub columns: Vec<ArtifactColumn>,
}

/// One wire column of an artifact table: the canonical change stream's
/// column id and the source column it carries.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct ArtifactColumn {
    pub column_id: u32,
    pub name: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct ArtifactRepresentation {
    pub representation_id: String,
    pub object_id: ObjectId,
    pub encoding: RepresentationEncoding,
    pub encoded_sha256: String,
    pub encoded_bytes: u64,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub zstd: Option<ZstdParameters>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub brotli: Option<BrotliParameters>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub packed: Option<PackedParameters>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize, Serialize)]
#[serde(rename_all = "lowercase")]
pub enum RepresentationEncoding {
    Zstd,
    Identity,
    /// One brotli stream with the large-window extension, sent without a
    /// Content-Encoding and decoded by the client's worker.
    #[serde(rename = "brotli-large-window")]
    BrotliLargeWindow,
    /// The artifact's packed body as one such stream. The client's worker
    /// decodes it and restores the artifact's bytes from the body.
    /// A manifest holds this or `BrotliLargeWindow`, never both.
    #[serde(rename = "packed-brotli-large-window")]
    PackedBrotliLargeWindow,
}

/// What a `brotli-large-window` or `packed-brotli-large-window`
/// representation was encoded with.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct BrotliParameters {
    pub quality: u8,
    /// The window the stream declares: the covering window of what it decodes
    /// to, the artifact or its packed body. The client's decoder allows the
    /// artifact's, which is no smaller.
    pub window_bits: u8,
}

/// The packed body a `packed-brotli-large-window` representation's stream
/// decodes to.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct PackedParameters {
    /// The body's format version, [`PACKED_BODY_VERSION`].
    pub version: u32,
    /// The body's length, which the stream's window covers.
    pub body_bytes: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct ZstdParameters {
    pub level: i32,
    pub window_log: u8,
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct ArtifactMemory {
    /// The browser memory the artifact takes at its peak while it loads: its
    /// decoded bytes, the primary-key indexes the engine worker builds over
    /// them, and the growth reserve.
    pub estimated_peak_bytes: u64,
    pub recommended_growth_reserve_bytes: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct ManifestSignature {
    pub algorithm: String,
    pub key_id: String,
    pub signature_base64: String,
}

/// Complete, explicit cache-identity dimensions.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Deserialize, Serialize)]
#[serde(rename_all = "snake_case")]
pub struct ArtifactCacheKey {
    pub source_epoch: SourceEpoch,
    pub publication_generation: u64,
    pub schema_generation: u64,
    pub authorization_generation: u64,
    pub authorization_policy_hash: String,
    pub authorization_scope_hash: String,
    pub shard_profile_hash: String,
    pub index_profile_hash: String,
    /// Decoder-compatibility profile of the decoded artifact, produced by
    /// [`DecodedArtifactFormat::decoder_profile`]. It is persisted: this struct
    /// is the catalog's `cache_key_json` column, which publication lookups match
    /// by exact string equality.
    pub decoder_profile: String,
    pub type_codec_generation: u64,
    pub collation_generation: u64,
}

/// Compatibility facts from the dynamic bootstrap plan.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct BootstrapSourceCompatibility {
    pub source_epoch: SourceEpoch,
    pub publication_generation: u64,
    pub schema_generation: u64,
    pub authorization_generation: u64,
    pub authorization_policy_hash: String,
    pub authorization_scope_hash: String,
}

/// Immutable fields selected for one artifact install in a dynamic plan.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct BootstrapArtifactInstall {
    pub artifact_id: ArtifactId,
    pub snapshot_cursor: OpaqueCursor,
    pub tables: Vec<TableId>,
    pub decoded_sha256: String,
    pub decoded_bytes: u64,
    pub buffer_capacity_bytes: u64,
    pub selected_representation: SelectedRepresentation,
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct SelectedRepresentation {
    pub representation_id: String,
    pub encoding: RepresentationEncoding,
    pub encoded_bytes: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SchemaViolation {
    pub instance_path: String,
    pub message: String,
}

#[derive(Debug, Error)]
pub enum ManifestError {
    #[error("manifest is not valid JSON: {0}")]
    Json(serde_json::Error),
    #[error("artifact manifest schema is unavailable: {0}")]
    SchemaDefinition(String),
    #[error("manifest violates the artifact JSON Schema")]
    Schema(Vec<SchemaViolation>),
    #[error("schema-valid manifest cannot be decoded into the contract type: {0}")]
    TypedDecode(serde_json::Error),
    #[error(transparent)]
    Semantic(#[from] SemanticError),
}

#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub enum SemanticError {
    #[error("table ID is repeated: {0}")]
    DuplicateTableId(String),
    #[error("multiple logical tables target local table {schema}.{table}")]
    DuplicateLocalTable { schema: String, table: String },
    #[error("representation ID is repeated: {0}")]
    DuplicateRepresentationId(String),
    #[error("decoded object ID must be sha256:<decoded SHA-256>")]
    DecodedObjectIdNotContentAddressed,
    #[error("representation ID must be sha256:<encoded SHA-256>")]
    RepresentationIdNotContentAddressed,
    #[error("representation object ID must be sha256:<encoded SHA-256>")]
    RepresentationObjectIdNotContentAddressed,
    #[error("object ID {0} is bound to different content in one manifest")]
    ObjectIdContentMismatch(String),
    #[error("identity representation hash/length does not equal the decoded artifact")]
    IdentityRepresentationMismatch,
    #[error("a brotli representation declares a {actual}-bit window, and the covering window of what its stream decodes to is {expected:?}")]
    BrotliWindow { expected: Option<u8>, actual: u8 },
    #[error("a packed body of {body_bytes} bytes is empty or not shorter than its artifact's {decoded_bytes}")]
    PackedBody { body_bytes: u64, decoded_bytes: u64 },
    #[error("direct-columnar schema names tables {actual:?}, manifest tables are {expected:?}")]
    SchemaTableMismatch {
        expected: Vec<String>,
        actual: Vec<String>,
    },
    #[error("direct-columnar table {0} carries no wire column ids")]
    WireColumnsMissing(String),
    #[error("direct-columnar table {table} repeats a wire column id or name")]
    DuplicateWireColumn { table: String },
    #[error("direct-columnar chunks must be {DIRECT_COLUMNAR_CHUNK_BYTES} bytes, not {0}")]
    ChunkSize(u64),
    #[error("direct-columnar artifact needs {expected} chunk digests, manifest has {actual}")]
    ChunkCount { expected: u64, actual: usize },
}

#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub enum IntegrityError {
    #[error("{kind} length mismatch: expected {expected}, got {actual}")]
    Length {
        kind: &'static str,
        expected: u64,
        actual: u64,
    },
    #[error("{kind} SHA-256 mismatch: expected {expected}, got {actual}")]
    Digest {
        kind: &'static str,
        expected: String,
        actual: String,
    },
}

#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub enum SelectionError {
    #[error("bootstrap source epoch does not match artifact")]
    SourceEpoch,
    #[error("bootstrap publication generation does not match artifact")]
    PublicationGeneration,
    #[error("bootstrap schema generation does not match artifact")]
    SchemaGeneration,
    #[error("bootstrap authorization generation does not match artifact")]
    AuthorizationGeneration,
    #[error("bootstrap authorization policy does not match artifact")]
    AuthorizationPolicy,
    #[error("bootstrap authorization scope does not match artifact")]
    AuthorizationScope,
    #[error("bootstrap artifact ID does not match manifest")]
    ArtifactId,
    #[error("bootstrap snapshot cursor does not match artifact")]
    SnapshotCursor,
    #[error("bootstrap decoded digest does not match artifact")]
    DecodedDigest,
    #[error("bootstrap decoded length does not match artifact")]
    DecodedLength,
    #[error("bootstrap buffer capacity is smaller than decoded artifact length")]
    BufferCapacity,
    #[error("bootstrap table set does not match artifact")]
    TableSet,
    #[error("bootstrap representation is absent from artifact")]
    RepresentationMissing,
    #[error("bootstrap representation encoding does not match artifact")]
    RepresentationEncoding,
    #[error("bootstrap representation length does not match artifact")]
    RepresentationLength,
}

#[cfg(test)]
mod tests {
    use super::*;

    const MANIFEST_EXAMPLE: &[u8] =
        include_bytes!("../../../../spec/artifact-manifest.example.json");

    /// The wire-column check reached directly: the checked-in schema already
    /// requires a non-empty `columns` on every table, and runs first, so
    /// [`ValidatedArtifactManifest::parse`] never reaches it.
    #[test]
    fn a_columnar_manifest_without_wire_columns_is_refused_semantically() {
        let mut manifest = two_table_manifest();
        for table in &mut manifest.tables {
            table.columns.clear();
        }
        assert!(matches!(
            validate_semantics(&manifest),
            Err(ManifestError::Semantic(SemanticError::WireColumnsMissing(table)))
                if table == "table/tasks"
        ));

        // One table's columns are not enough; every table needs its own.
        manifest.tables[0].columns = vec![ArtifactColumn {
            column_id: 1,
            name: "id".to_owned(),
        }];
        assert!(matches!(
            validate_semantics(&manifest),
            Err(ManifestError::Semantic(SemanticError::WireColumnsMissing(table)))
                if table == "table/users"
        ));

        manifest.tables[1].columns = vec![ArtifactColumn {
            column_id: 2,
            name: "id".to_owned(),
        }];
        assert!(validate_semantics(&manifest).is_ok());
    }

    /// The chunk size is checked here as well as by the schema, which pins it
    /// first and so keeps [`ValidatedArtifactManifest::parse`] from reaching
    /// this check; the count is proved through `parse` in the contract tests.
    #[test]
    fn a_columnar_manifest_with_another_chunk_size_is_refused_semantically() {
        let mut manifest = columnar_manifest();
        let decoded_bytes = manifest.decoded.bytes;
        let DecodedArtifactFormat::DirectColumnar { chunks, .. } = &mut manifest.decoded.format;
        // Half the size, with the count that size would need, so only the
        // size is wrong.
        chunks.bytes = DIRECT_COLUMNAR_CHUNK_BYTES / 2;
        chunks.sha256 = (0..decoded_bytes.div_ceil(chunks.bytes))
            .map(|index| format!("sha256:{index:064x}"))
            .collect();
        assert!(matches!(
            validate_semantics(&manifest),
            Err(ManifestError::Semantic(SemanticError::ChunkSize(size)))
                if size == DIRECT_COLUMNAR_CHUNK_BYTES / 2
        ));
    }

    fn columnar_manifest() -> ArtifactManifest {
        ValidatedArtifactManifest::parse(MANIFEST_EXAMPLE)
            .unwrap()
            .manifest()
            .clone()
    }

    /// The checked-in example with a second table, `table/users`, in both the
    /// table list and the physical schema.
    fn two_table_manifest() -> ArtifactManifest {
        let mut manifest = columnar_manifest();
        let mut users = manifest.tables[0].clone();
        users.table_id = TableId::new("table/users").unwrap();
        users.source_table = "users".to_owned();
        users.local_table = "users".to_owned();
        manifest.tables.push(users);
        let DecodedArtifactFormat::DirectColumnar { schema, .. } = &mut manifest.decoded.format;
        let mut users_schema = schema.tables[0].clone();
        users_schema.name = "users".to_owned();
        schema.tables.push(users_schema);
        validate_semantics(&manifest).unwrap();
        manifest
    }
}
