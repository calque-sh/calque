use artifact_contract::{
    direct_columnar_decoder_profile, BootstrapArtifactInstall, BootstrapSourceCompatibility,
    BrotliParameters, DecodedArtifactFormat, IntegrityError, ManifestError, PackedParameters,
    RepresentationEncoding, SelectionError, SemanticError, ValidatedArtifactManifest,
    DIRECT_COLUMNAR_CHUNK_BYTES, DIRECT_COLUMNAR_MEDIA_TYPE, PACKED_BODY_VERSION,
};
use serde_json::{json, Value};
use sha2::{Digest, Sha256};

const MANIFEST_EXAMPLE: &[u8] = include_bytes!("../../../../spec/artifact-manifest.example.json");
const BOOTSTRAP_EXAMPLE: &[u8] =
    include_bytes!("../../../../spec/bootstrap-plan.direct-columnar.example.json");

fn manifest_value() -> Value {
    serde_json::from_slice(MANIFEST_EXAMPLE).expect("checked-in manifest example is JSON")
}

fn parse_value(value: &Value) -> Result<ValidatedArtifactManifest, ManifestError> {
    ValidatedArtifactManifest::parse(&serde_json::to_vec(value).expect("test manifest serializes"))
}

/// The second table [`two_table_value`] adds, as the physical schema
/// describes it.
fn users_schema_value() -> Value {
    json!({
        "name": "users",
        "primaryKey": ["id"],
        "columns": [
            { "name": "id", "type": "int64", "nullable": false },
            { "name": "name", "type": "dict", "nullable": false }
        ]
    })
}

/// The checked-in example with a second table, `table/users`, in both the
/// table list and the physical schema.
fn two_table_value() -> Value {
    let mut value = manifest_value();
    let mut users = value["tables"][0].clone();
    users["tableId"] = json!("table/users");
    users["sourceTable"] = json!("users");
    users["localTable"] = json!("users");
    users["columns"] = json!([{ "columnId": 5, "name": "id" }, { "columnId": 6, "name": "name" }]);
    value["tables"].as_array_mut().unwrap().push(users);
    value["decoded"]["schema"]["tables"]
        .as_array_mut()
        .unwrap()
        .push(users_schema_value());
    value
}

#[test]
fn checked_in_example_passes_real_schema_and_semantics() {
    let manifest = ValidatedArtifactManifest::parse(MANIFEST_EXAMPLE).unwrap();

    assert_eq!(manifest.manifest().protocol_version, 1);
    assert_eq!(manifest.manifest().tables.len(), 1);
    assert_eq!(manifest.manifest().representations.len(), 2);
    assert_eq!(
        manifest.cache_key().source_epoch.as_str(),
        "source_42/epoch_7"
    );

    let encoded = serde_json::to_vec(manifest.manifest()).unwrap();
    ValidatedArtifactManifest::parse(&encoded).unwrap();
}

#[test]
fn schema_rejects_unknown_version_fields_formats_and_zstd_shape() {
    let cases = [
        {
            let mut value = manifest_value();
            value["protocolVersion"] = json!(2);
            value
        },
        {
            let mut value = manifest_value();
            value["unknownContractField"] = json!(true);
            value
        },
        {
            let mut value = manifest_value();
            value["createdAt"] = json!("not-a-date-time");
            value
        },
        {
            let mut value = manifest_value();
            value["representations"][0]["zstd"]["windowLog"] = json!(24);
            value
        },
        {
            let mut value = manifest_value();
            value["representations"][1]["zstd"] = json!({"level": 3, "windowLog": 23});
            value
        },
        {
            let mut value = manifest_value();
            value["representations"].as_array_mut().unwrap().pop();
            value
        },
        {
            let mut value = manifest_value();
            value["representations"][0]["encoding"] = json!("identity");
            value["representations"][0]
                .as_object_mut()
                .unwrap()
                .remove("zstd");
            value
        },
        {
            let mut value = manifest_value();
            value["representations"][0]["encoding"] = json!("br");
            value["representations"][0]
                .as_object_mut()
                .unwrap()
                .remove("zstd");
            value
        },
    ];

    for value in cases {
        assert!(matches!(parse_value(&value), Err(ManifestError::Schema(_))));
    }
}

#[test]
fn semantic_checks_reject_aliases_and_identity_mismatch() {
    assert!(parse_value(&two_table_value()).is_ok());
    let mut duplicate_table = two_table_value();
    duplicate_table["tables"][1]["tableId"] = json!("table/tasks");
    assert!(matches!(
        parse_value(&duplicate_table),
        Err(ManifestError::Semantic(SemanticError::DuplicateTableId(_)))
    ));

    let mut duplicate_local = two_table_value();
    duplicate_local["tables"][1]["localTable"] = json!("tasks");
    assert!(matches!(
        parse_value(&duplicate_local),
        Err(ManifestError::Semantic(
            SemanticError::DuplicateLocalTable { .. }
        ))
    ));

    let mut duplicate_representation = manifest_value();
    for field in [
        "representationId",
        "objectId",
        "encodedSha256",
        "encodedBytes",
    ] {
        duplicate_representation["representations"][0][field] =
            duplicate_representation["representations"][1][field].clone();
    }
    assert!(matches!(
        parse_value(&duplicate_representation),
        Err(ManifestError::Semantic(
            SemanticError::DuplicateRepresentationId(_)
        ))
    ));

    let mut object_alias = manifest_value();
    object_alias["representations"][0]["objectId"] = object_alias["decoded"]["objectId"].clone();
    assert!(matches!(
        parse_value(&object_alias),
        Err(ManifestError::Semantic(
            SemanticError::RepresentationObjectIdNotContentAddressed
        ))
    ));

    let mut decoded_address = manifest_value();
    decoded_address["decoded"]["objectId"] = json!(format!("sha256:{}", "0".repeat(64)));
    assert!(matches!(
        parse_value(&decoded_address),
        Err(ManifestError::Semantic(
            SemanticError::DecodedObjectIdNotContentAddressed
        ))
    ));

    let mut identity_mismatch = manifest_value();
    identity_mismatch["representations"][1]["encodedBytes"] = json!(1);
    assert!(matches!(
        parse_value(&identity_mismatch),
        Err(ManifestError::Semantic(
            SemanticError::IdentityRepresentationMismatch
        ))
    ));
}

#[test]
fn decoded_bytes_are_verified_before_use() {
    let decoded = b"CALQART1test artifact";
    let encoded = b"bounded encoded bytes";
    let decoded_hash = format!("{:x}", Sha256::digest(decoded));
    let encoded_hash = format!("{:x}", Sha256::digest(encoded));

    let mut value = manifest_value();
    value["decoded"]["sha256"] = json!(decoded_hash.clone());
    value["decoded"]["bytes"] = json!(decoded.len());
    value["decoded"]["objectId"] = json!(format!("sha256:{decoded_hash}"));
    value["decoded"]["chunks"]["sha256"] = json!([format!("sha256:{decoded_hash}")]);
    value["representations"][0]["encodedSha256"] = json!(encoded_hash.clone());
    value["representations"][0]["encodedBytes"] = json!(encoded.len());
    value["representations"][0]["representationId"] = json!(format!("sha256:{encoded_hash}"));
    value["representations"][0]["objectId"] = json!(format!("sha256:{encoded_hash}"));
    value["representations"][1]["encodedSha256"] = value["decoded"]["sha256"].clone();
    value["representations"][1]["encodedBytes"] = value["decoded"]["bytes"].clone();
    value["representations"][1]["representationId"] = value["decoded"]["objectId"].clone();
    value["representations"][1]["objectId"] = value["decoded"]["objectId"].clone();
    let manifest = parse_value(&value).unwrap();

    manifest.verify_decoded_bytes(decoded).unwrap();

    let mut tampered = decoded.to_vec();
    *tampered.last_mut().unwrap() ^= 1;
    assert!(matches!(
        manifest.verify_decoded_bytes(&tampered),
        Err(IntegrityError::Digest { .. })
    ));
    assert!(matches!(
        manifest.verify_decoded_bytes(&decoded[..decoded.len() - 1]),
        Err(IntegrityError::Length { .. })
    ));
}

#[test]
fn a_representation_id_is_its_encoded_content_address() {
    let mut changed_representation = manifest_value();
    changed_representation["representations"][0]["encodedSha256"] = json!("e".repeat(64));
    assert!(matches!(
        parse_value(&changed_representation),
        Err(ManifestError::Semantic(
            SemanticError::RepresentationIdNotContentAddressed
        ))
    ));
}

#[test]
fn cache_key_changes_for_every_locked_dimension() {
    let base_value = manifest_value();
    let base = parse_value(&base_value).unwrap().cache_key();
    let mutations: &[(&[&str], Value)] = &[
        (&["pit", "sourceEpoch"], json!("source_42/epoch_8")),
        (&["compatibility", "publicationGeneration"], json!(5)),
        (&["compatibility", "schemaGeneration"], json!(19)),
        (&["compatibility", "authorizationGeneration"], json!(10)),
        (
            &["compatibility", "authorizationPolicyHash"],
            json!(format!("sha256:{}", "9".repeat(64))),
        ),
        (
            &["compatibility", "authorizationScopeHash"],
            json!(format!("sha256:{}", "a".repeat(64))),
        ),
        (
            &["compatibility", "shardProfileHash"],
            json!(format!("sha256:{}", "b".repeat(64))),
        ),
        (
            &["compatibility", "indexProfileHash"],
            json!(format!("sha256:{}", "c".repeat(64))),
        ),
        (&["compatibility", "typeCodecGeneration"], json!(4)),
        (&["compatibility", "collationGeneration"], json!(2)),
    ];

    for (path, replacement) in mutations {
        let mut value = base_value.clone();
        let mut target = &mut value;
        for segment in &path[..path.len() - 1] {
            target = &mut target[*segment];
        }
        target[path[path.len() - 1]] = replacement.clone();
        assert_ne!(parse_value(&value).unwrap().cache_key(), base, "{path:?}");
    }
}

type SelectionEdit = fn(&mut BootstrapSourceCompatibility, &mut BootstrapArtifactInstall);

/// Each field a bootstrap plan repeats from the immutable manifest, changed on
/// its own, is refused under the variant that names it.
#[test]
fn real_bootstrap_selection_must_match_immutable_manifest() {
    let manifest = ValidatedArtifactManifest::parse(MANIFEST_EXAMPLE).unwrap();
    let plan: Value = serde_json::from_slice(BOOTSTRAP_EXAMPLE).unwrap();
    let source = BootstrapSourceCompatibility {
        source_epoch: serde_json::from_value(plan["source"]["sourceEpoch"].clone()).unwrap(),
        publication_generation: plan["source"]["publicationGeneration"].as_u64().unwrap(),
        schema_generation: plan["source"]["schemaGeneration"].as_u64().unwrap(),
        authorization_generation: plan["source"]["authorizationGeneration"].as_u64().unwrap(),
        authorization_policy_hash: plan["source"]["authorizationPolicyHash"]
            .as_str()
            .unwrap()
            .to_owned(),
        authorization_scope_hash: plan["authorization"]["scopeHash"]
            .as_str()
            .unwrap()
            .to_owned(),
    };
    let install: BootstrapArtifactInstall =
        serde_json::from_value(plan["artifacts"][0].clone()).unwrap();

    manifest
        .validate_bootstrap_selection(&source, &install)
        .unwrap();

    let cases: [(SelectionEdit, SelectionError); 17] = [
        (
            |source, _| {
                source.source_epoch = serde_json::from_value(json!("source_42/epoch_8")).unwrap()
            },
            SelectionError::SourceEpoch,
        ),
        (
            |source, _| source.publication_generation += 1,
            SelectionError::PublicationGeneration,
        ),
        (
            |source, _| source.schema_generation += 1,
            SelectionError::SchemaGeneration,
        ),
        (
            |source, _| source.authorization_generation += 1,
            SelectionError::AuthorizationGeneration,
        ),
        (
            |source, _| source.authorization_policy_hash = format!("sha256:{}", "0".repeat(64)),
            SelectionError::AuthorizationPolicy,
        ),
        (
            |source, _| source.authorization_scope_hash = format!("sha256:{}", "0".repeat(64)),
            SelectionError::AuthorizationScope,
        ),
        (
            |_, install| {
                install.artifact_id = serde_json::from_value(json!("artifact_other")).unwrap()
            },
            SelectionError::ArtifactId,
        ),
        (
            |_, install| {
                install.snapshot_cursor =
                    serde_json::from_value(json!("cursor-v1:source_42/epoch_7/other")).unwrap();
            },
            SelectionError::SnapshotCursor,
        ),
        (
            |_, install| install.decoded_sha256 = "0".repeat(64),
            SelectionError::DecodedDigest,
        ),
        (
            |_, install| {
                install.decoded_bytes += 1;
                install.buffer_capacity_bytes += 1;
            },
            SelectionError::DecodedLength,
        ),
        (
            |_, install| install.buffer_capacity_bytes = install.decoded_bytes - 1,
            SelectionError::BufferCapacity,
        ),
        (
            |_, install| install.tables.clear(),
            SelectionError::TableSet,
        ),
        (
            |_, install| install.tables.push(install.tables[0].clone()),
            SelectionError::TableSet,
        ),
        (
            |_, install| install.tables[0] = serde_json::from_value(json!("table/other")).unwrap(),
            SelectionError::TableSet,
        ),
        (
            |_, install| {
                install.selected_representation.representation_id =
                    format!("sha256:{}", "c".repeat(64));
            },
            SelectionError::RepresentationMissing,
        ),
        (
            |_, install| {
                let selected = &mut install.selected_representation;
                selected.encoding = match selected.encoding {
                    RepresentationEncoding::Zstd => RepresentationEncoding::Identity,
                    _ => RepresentationEncoding::Zstd,
                };
            },
            SelectionError::RepresentationEncoding,
        ),
        (
            |_, install| install.selected_representation.encoded_bytes += 1,
            SelectionError::RepresentationLength,
        ),
    ];
    for (edit, expected) in cases {
        let (mut source, mut install) = (source.clone(), install.clone());
        edit(&mut source, &mut install);
        assert_eq!(
            manifest
                .validate_bootstrap_selection(&source, &install)
                .unwrap_err(),
            expected
        );
    }
}

#[test]
fn the_manifest_carries_identity_schema_and_wire_columns() {
    let parsed = ValidatedArtifactManifest::parse(MANIFEST_EXAMPLE).unwrap();
    let manifest = parsed.manifest();
    let DecodedArtifactFormat::DirectColumnar {
        identity, schema, ..
    } = &manifest.decoded.format;
    assert_eq!(identity.manifest_version, "manifest_01JZCOLUMNAR");
    assert_eq!(
        identity.authorization_scope_version,
        "authorization-generation-9"
    );
    assert_eq!(schema.format, "calque-direct-columnar");
    assert_eq!(schema.version, 4);
    assert_eq!(
        schema
            .tables
            .iter()
            .map(|table| table.name.as_str())
            .collect::<Vec<_>>(),
        ["tasks"]
    );
    assert_eq!(
        serde_json::to_value(schema).unwrap(),
        manifest_value()["decoded"]["schema"]
    );
    assert_eq!(
        manifest.tables[0]
            .columns
            .iter()
            .map(|column| (column.column_id, column.name.as_str()))
            .collect::<Vec<_>>(),
        [(1, "id"), (2, "title"), (3, "status"), (4, "updated_at")]
    );

    // The re-serialized manifest is the parsed one: the fields the client
    // binds are the fields the catalog stores.
    let encoded = serde_json::to_value(manifest).unwrap();
    assert_eq!(
        encoded["decoded"]["identity"]["manifestVersion"],
        "manifest_01JZCOLUMNAR"
    );
    assert_eq!(
        encoded["decoded"]["schema"],
        manifest_value()["decoded"]["schema"]
    );
    assert_eq!(encoded["tables"][0]["columns"][3]["columnId"], 4);

    // A manifest is refused without any of the four, structurally, and a
    // second table needs its own wire columns.
    for strip in [
        |value: &mut Value| {
            value["decoded"].as_object_mut().unwrap().remove("identity");
        },
        |value: &mut Value| {
            value["decoded"].as_object_mut().unwrap().remove("schema");
        },
        |value: &mut Value| {
            value["decoded"].as_object_mut().unwrap().remove("chunks");
        },
        |value: &mut Value| {
            value["tables"][1]
                .as_object_mut()
                .unwrap()
                .remove("columns");
        },
    ] {
        let mut stripped = two_table_value();
        strip(&mut stripped);
        assert!(
            matches!(parse_value(&stripped), Err(ManifestError::Schema(_))),
            "{stripped}"
        );
    }

    // Cross-field: the physical schema names exactly the manifest's local
    // tables, and the wire columns of one table are distinct.
    let mut renamed = two_table_value();
    renamed["decoded"]["schema"]["tables"][1]["name"] = json!("people");
    assert!(matches!(
        parse_value(&renamed),
        Err(ManifestError::Semantic(SemanticError::SchemaTableMismatch { expected, actual }))
            if expected == ["tasks", "users"] && actual == ["people", "tasks"]
    ));
    let mut repeated = manifest_value();
    repeated["tables"][0]["columns"][1]["columnId"] = json!(1);
    assert!(matches!(
        parse_value(&repeated),
        Err(ManifestError::Semantic(SemanticError::DuplicateWireColumn { table }))
            if table == "table/tasks"
    ));
    let mut versioned = manifest_value();
    versioned["decoded"]["schema"]["version"] = json!(2);
    assert!(matches!(
        parse_value(&versioned),
        Err(ManifestError::Schema(_))
    ));
}

#[test]
fn direct_columnar_chunks_cover_the_decoded_artifact_exactly() {
    let valid = manifest_value();
    // 24,746,441 bytes: 5 whole chunks and a partial one.
    assert_eq!(valid["decoded"]["bytes"], 24_746_441);
    let columnar = parse_value(&valid).unwrap();
    let DecodedArtifactFormat::DirectColumnar { chunks, .. } = &columnar.manifest().decoded.format;
    assert_eq!(chunks.bytes, DIRECT_COLUMNAR_CHUNK_BYTES);
    assert_eq!(chunks.sha256.len(), 6);
    assert_eq!(
        serde_json::to_value(columnar.manifest()).unwrap()["decoded"]["chunks"],
        valid["decoded"]["chunks"]
    );

    // One digest short, one too many, and none for the partial chunk the
    // length leaves: a count other than ceil(bytes / 4 MiB) is refused.
    for count in [5_usize, 7] {
        let mut miscounted = manifest_value();
        let digests = miscounted["decoded"]["chunks"]["sha256"]
            .as_array_mut()
            .unwrap();
        digests.resize(count, json!(format!("sha256:{}", "f".repeat(64))));
        assert!(
            matches!(
                parse_value(&miscounted),
                Err(ManifestError::Semantic(SemanticError::ChunkCount { expected: 6, actual }))
                    if actual == count
            ),
            "{count}"
        );
    }
    // Exactly 6 whole chunks need exactly 6 digests, and one more byte
    // needs one more digest.
    let resized = |bytes: u64| {
        let mut value = manifest_value();
        value["decoded"]["bytes"] = json!(bytes);
        value["representations"][1]["encodedBytes"] = json!(bytes);
        value
    };
    assert!(parse_value(&resized(6 * 4_194_304)).is_ok());
    let whole = resized(6 * 4_194_304 + 1);
    assert!(matches!(
        parse_value(&whole),
        Err(ManifestError::Semantic(SemanticError::ChunkCount {
            expected: 7,
            actual: 6
        }))
    ));

    // The size is fixed and each digest is a content id, structurally.
    for (field, bad) in [
        ("bytes", json!(4_194_303)),
        ("bytes", json!(2_097_152)),
        ("sha256", json!([])),
        ("extra", json!(1)),
    ] {
        let mut malformed = manifest_value();
        malformed["decoded"]["chunks"][field] = bad;
        assert!(
            matches!(parse_value(&malformed), Err(ManifestError::Schema(_))),
            "{field}"
        );
    }
    for bad in [
        "f".repeat(64),
        format!("sha256:{}", "F".repeat(64)),
        format!("sha256:{}", "f".repeat(63)),
    ] {
        let mut malformed = manifest_value();
        malformed["decoded"]["chunks"]["sha256"][3] = json!(bad);
        assert!(
            matches!(parse_value(&malformed), Err(ManifestError::Schema(_))),
            "{bad}"
        );
    }
}

#[test]
fn the_manifest_names_the_direct_columnar_format() {
    let columnar = ValidatedArtifactManifest::parse(MANIFEST_EXAMPLE).unwrap();
    let DecodedArtifactFormat::DirectColumnar {
        schema_hash,
        collection_set_hash,
        format_version,
        ..
    } = &columnar.manifest().decoded.format;
    assert_eq!(
        schema_hash,
        "sha256:578eee00171f1dbe61e18e10be0acafe8335b630f77639cc0916a5b678e36737"
    );
    assert_eq!(*collection_set_hash, format!("sha256:{}", "b".repeat(64)));
    assert_eq!(*format_version, 4);
    // The schema pins format and mediaType together, so this asserts the
    // constant the contract publishes rather than a Rust-side derivation of it.
    assert_eq!(
        columnar.manifest().decoded.media_type,
        DIRECT_COLUMNAR_MEDIA_TYPE
    );

    let encoded = serde_json::to_vec(columnar.manifest()).unwrap();
    assert_eq!(
        ValidatedArtifactManifest::parse(&encoded).unwrap(),
        columnar
    );
}

#[test]
fn a_manifest_without_the_format_discriminant_is_refused() {
    let mut untagged = manifest_value();
    untagged["decoded"]
        .as_object_mut()
        .unwrap()
        .remove("format")
        .expect("checked-in example carries the discriminant");

    assert!(matches!(
        parse_value(&untagged),
        Err(ManifestError::Schema(_))
    ));
}

#[test]
fn schema_rejects_unknown_and_mislabelled_decoded_formats() {
    let cases = [
        {
            // A field of no format the protocol defines.
            let mut value = manifest_value();
            value["decoded"]["sqliteVersion"] = json!("3.51.3");
            value
        },
        {
            // The media type is bound to the format.
            let mut value = manifest_value();
            value["decoded"]["mediaType"] = json!("application/vnd.sqlite3");
            value
        },
        {
            let mut value = manifest_value();
            value["decoded"]["formatVersion"] = json!(1);
            value
        },
        {
            let mut value = manifest_value();
            value["decoded"]["schemaHash"] = json!("not-a-hash-id");
            value
        },
        {
            let mut value = manifest_value();
            value["decoded"]["format"] = json!("sqlite");
            value
        },
        {
            let mut value = manifest_value();
            value["decoded"]["format"] = json!("parquet");
            value
        },
        {
            // A discriminant alone does not make the payload complete.
            let mut value = manifest_value();
            value["decoded"]
                .as_object_mut()
                .unwrap()
                .remove("schemaHash");
            value
        },
    ];

    for value in cases {
        assert!(
            matches!(parse_value(&value), Err(ManifestError::Schema(_))),
            "{value}"
        );
    }
}

#[test]
fn the_cache_key_serialization_is_pinned_because_it_is_a_persisted_format() {
    // `artifact-catalog` stores the key as `cache_key_json` and matches
    // publication lookups on it by exact string equality, so a renamed field
    // would silently stop every published artifact from being found.
    let manifest = ValidatedArtifactManifest::parse(MANIFEST_EXAMPLE).unwrap();
    let key = serde_json::to_value(manifest.cache_key()).unwrap();

    let mut names: Vec<&str> = key
        .as_object()
        .unwrap()
        .keys()
        .map(String::as_str)
        .collect();
    names.sort_unstable();
    assert_eq!(
        names,
        [
            "authorization_generation",
            "authorization_policy_hash",
            "authorization_scope_hash",
            "collation_generation",
            "decoder_profile",
            "index_profile_hash",
            "publication_generation",
            "schema_generation",
            "shard_profile_hash",
            "source_epoch",
            "type_codec_generation",
        ]
    );

    // The decoder profile carries the format's compatibility identity.
    assert_eq!(key["decoder_profile"], json!("calque-direct-columnar/v4"));
}

/// A manifest this build publishes names format version 4. One a v3 build
/// stored is refused whole, so a state directory holding one is refused with
/// its remedy rather than served to a client that cannot read its offsets.
#[test]
fn a_columnar_manifest_of_another_format_version_is_refused() {
    let mut value = manifest_value();
    assert!(parse_value(&value).is_ok());
    value["decoded"]["mediaType"] = json!("application/vnd.calque.direct-columnar.v3");
    value["decoded"]["formatVersion"] = json!(3);
    value["decoded"]["schema"]["version"] = json!(3);
    assert!(matches!(parse_value(&value), Err(ManifestError::Schema(_))));
    for field in ["mediaType", "formatVersion"] {
        let mut value = manifest_value();
        value["decoded"][field] = match field {
            "mediaType" => json!("application/vnd.calque.direct-columnar.v3"),
            _ => json!(3),
        };
        assert!(
            matches!(parse_value(&value), Err(ManifestError::Schema(_))),
            "{field} alone at v3 is refused"
        );
    }
    let mut value = manifest_value();
    value["decoded"]["schema"]["version"] = json!(3);
    assert!(matches!(parse_value(&value), Err(ManifestError::Schema(_))));
}

#[test]
fn a_columnar_decoder_profile_is_not_a_hash_and_says_so() {
    let columnar = ValidatedArtifactManifest::parse(MANIFEST_EXAMPLE).unwrap();
    assert_eq!(
        columnar.manifest().decoded.format.decoder_profile(),
        "calque-direct-columnar/v4"
    );
    assert_eq!(
        direct_columnar_decoder_profile(4),
        columnar.manifest().decoded.format.decoder_profile()
    );
}

/// The example with a brotli-large-window representation. Its decoded
/// artifact is 24,746,441 bytes, which a 2^25 window covers.
fn with_brotli(mut value: Value) -> Value {
    let sha = "b".repeat(64);
    value["representations"]
        .as_array_mut()
        .unwrap()
        .push(json!({
            "representationId": format!("sha256:{sha}"),
            "objectId": format!("sha256:{sha}"),
            "encoding": "brotli-large-window",
            "encodedSha256": sha,
            "encodedBytes": 6_485_477,
            "brotli": { "quality": 11, "windowBits": 25 }
        }));
    value
}

fn representation_mut<'a>(value: &'a mut Value, encoding: &str) -> &'a mut Value {
    value["representations"]
        .as_array_mut()
        .unwrap()
        .iter_mut()
        .find(|representation| representation["encoding"] == encoding)
        .unwrap()
}

#[test]
fn a_brotli_large_window_representation_records_the_covering_window() {
    let manifest = parse_value(&with_brotli(manifest_value())).unwrap();
    let brotli = manifest.manifest().representations.last().unwrap();
    assert_eq!(brotli.encoding, RepresentationEncoding::BrotliLargeWindow);
    assert_eq!(brotli.zstd, None);
    assert_eq!(
        brotli.brotli,
        Some(BrotliParameters {
            quality: 11,
            window_bits: 25
        })
    );
    // One bit narrower does not cover the artifact; one wider, the client's
    // decoder refuses.
    for window_bits in [24, 26] {
        let mut value = with_brotli(manifest_value());
        representation_mut(&mut value, "brotli-large-window")["brotli"]["windowBits"] =
            json!(window_bits);
        assert!(
            matches!(
                parse_value(&value),
                Err(ManifestError::Semantic(SemanticError::BrotliWindow {
                    expected: Some(25),
                    actual
                })) if actual == window_bits
            ),
            "{window_bits}"
        );
    }
}

#[test]
fn schema_rejects_malformed_brotli_representations() {
    let mut missing = with_brotli(manifest_value());
    representation_mut(&mut missing, "brotli-large-window")
        .as_object_mut()
        .unwrap()
        .remove("brotli");
    let mut on_zstd = manifest_value();
    representation_mut(&mut on_zstd, "zstd")["brotli"] = json!({ "quality": 11, "windowBits": 25 });
    let mut twice = with_brotli(manifest_value());
    let again = twice["representations"][2].clone();
    twice["representations"].as_array_mut().unwrap().push(again);
    let mut wide = with_brotli(manifest_value());
    representation_mut(&mut wide, "brotli-large-window")["brotli"]["windowBits"] = json!(31);
    let mut high = with_brotli(manifest_value());
    representation_mut(&mut high, "brotli-large-window")["brotli"]["quality"] = json!(12);
    let mut extra = with_brotli(manifest_value());
    representation_mut(&mut extra, "brotli-large-window")["brotli"]["mode"] = json!("text");
    for (what, value) in [
        ("no parameters", missing),
        ("parameters on zstd", on_zstd),
        ("two brotli representations", twice),
        ("a 31-bit window", wide),
        ("quality 12", high),
        ("an unknown parameter", extra),
    ] {
        assert!(
            matches!(parse_value(&value), Err(ManifestError::Schema(_))),
            "{what}"
        );
    }
}

/// The example with the packed representation published in the plain one's
/// place. Its body is 16,000,000 bytes, which a 2^24 window covers, where the
/// artifact's 24,746,441 need 2^25.
fn with_packed(mut value: Value) -> Value {
    let sha = "c".repeat(64);
    value["representations"]
        .as_array_mut()
        .unwrap()
        .push(json!({
            "representationId": format!("sha256:{sha}"),
            "objectId": format!("sha256:{sha}"),
            "encoding": "packed-brotli-large-window",
            "encodedSha256": sha,
            "encodedBytes": 6_300_000,
            "brotli": { "quality": 11, "windowBits": 24 },
            "packed": { "version": 1, "bodyBytes": 16_000_000 }
        }));
    value
}

#[test]
fn a_packed_representation_records_its_body_and_the_window_that_covers_it() {
    let manifest = parse_value(&with_packed(manifest_value())).unwrap();
    let packed = manifest.manifest().representations.last().unwrap();
    assert_eq!(
        packed.encoding,
        RepresentationEncoding::PackedBrotliLargeWindow
    );
    assert_eq!(packed.zstd, None);
    assert_eq!(
        packed.brotli,
        Some(BrotliParameters {
            quality: 11,
            window_bits: 24
        })
    );
    assert_eq!(
        packed.packed,
        Some(PackedParameters {
            version: PACKED_BODY_VERSION,
            body_bytes: 16_000_000
        })
    );
    // What the catalog stores parses to the same manifest.
    let stored = serde_json::to_value(manifest.manifest()).unwrap();
    assert_eq!(stored, with_packed(manifest_value()));

    // The window covers the body, not the artifact: 25 bits is the artifact's.
    for window_bits in [23, 25] {
        let mut value = with_packed(manifest_value());
        representation_mut(&mut value, "packed-brotli-large-window")["brotli"]["windowBits"] =
            json!(window_bits);
        assert!(
            matches!(
                parse_value(&value),
                Err(ManifestError::Semantic(SemanticError::BrotliWindow {
                    expected: Some(24),
                    actual
                })) if actual == window_bits
            ),
            "{window_bits}"
        );
    }

    // The packer writes a body only where it is shorter than the artifact.
    let decoded_bytes = manifest.manifest().decoded.bytes;
    for body_bytes in [decoded_bytes, decoded_bytes + 1] {
        let mut value = with_packed(manifest_value());
        let packed = representation_mut(&mut value, "packed-brotli-large-window");
        packed["packed"]["bodyBytes"] = json!(body_bytes);
        packed["brotli"]["windowBits"] = json!(25);
        assert!(
            matches!(
                parse_value(&value),
                Err(ManifestError::Semantic(SemanticError::PackedBody { body_bytes: actual, decoded_bytes: of }))
                    if actual == body_bytes && of == decoded_bytes
            ),
            "{body_bytes}"
        );
    }
    let mut longest = with_packed(manifest_value());
    let packed = representation_mut(&mut longest, "packed-brotli-large-window");
    packed["packed"]["bodyBytes"] = json!(decoded_bytes - 1);
    packed["brotli"]["windowBits"] = json!(25);
    parse_value(&longest).unwrap();
}

#[test]
fn schema_rejects_malformed_packed_representations() {
    const PACKED: &str = "packed-brotli-large-window";
    let without = |member: &str| {
        let mut value = with_packed(manifest_value());
        representation_mut(&mut value, PACKED)
            .as_object_mut()
            .unwrap()
            .remove(member);
        value
    };
    let mut on_brotli = with_brotli(manifest_value());
    representation_mut(&mut on_brotli, "brotli-large-window")["packed"] =
        json!({ "version": 1, "bodyBytes": 16_000_000 });
    let mut on_zstd = manifest_value();
    representation_mut(&mut on_zstd, "zstd")["packed"] =
        json!({ "version": 1, "bodyBytes": 16_000_000 });
    // A manifest holds one of the two brotli representations.
    let both = with_packed(with_brotli(manifest_value()));
    let mut version = with_packed(manifest_value());
    representation_mut(&mut version, PACKED)["packed"]["version"] = json!(2);
    let mut empty = with_packed(manifest_value());
    representation_mut(&mut empty, PACKED)["packed"]["bodyBytes"] = json!(0);
    let mut bodiless = with_packed(manifest_value());
    representation_mut(&mut bodiless, PACKED)["packed"]
        .as_object_mut()
        .unwrap()
        .remove("bodyBytes");
    let mut extra = with_packed(manifest_value());
    representation_mut(&mut extra, PACKED)["packed"]["hoisted"] = json!(0);
    for (what, value) in [
        ("no packed parameters", without("packed")),
        ("no brotli parameters", without("brotli")),
        ("packed parameters on the plain brotli object", on_brotli),
        ("packed parameters on zstd", on_zstd),
        ("both brotli representations", both),
        ("body version 2", version),
        ("an empty body", empty),
        ("no body length", bodiless),
        ("an unknown parameter", extra),
    ] {
        assert!(
            matches!(parse_value(&value), Err(ManifestError::Schema(_))),
            "{what}"
        );
    }
}
