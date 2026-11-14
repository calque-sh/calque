//! `calque init`: turn a `calque.pg-inspection.v1` document into a Calque
//! configuration manifest.
//!
//! It applies `compileConfig`'s rules so `init` never reports success on a
//! manifest that fails to compile. A rule added to `compileConfig` has to be
//! added here and to `every_compile_config_rejection_is_rejected_here`.

use std::{error::Error, fs, path::Path};

use mutation_crud::{admitted_operations, CrudColumnRole, CrudOperations};
use serde::Serialize;
use source_control::{PostgresTypeCodec, QualifiedTable, MAX_POSTGRES_IDENTIFIER_BYTES};

use crate::inspect::{InspectionOutput, INSPECTION_SCHEMA_VERSION};

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
struct Application {
    id: String,
    version: String,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
struct Protocol {
    manifest: u32,
    artifact: u32,
    change_stream: u32,
    mutation: u32,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
struct Source {
    id: String,
    database_url_env: String,
    publication: String,
    slot: String,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
struct Scope {
    id: String,
    source: String,
    kind: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    attribute: Option<String>,
    data_version: u32,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
struct Relation {
    schema: String,
    table: String,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
struct Column {
    name: String,
    postgres_type: String,
    nullable: bool,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
struct Collection {
    id: String,
    source: String,
    kind: String,
    relation: Relation,
    columns: Vec<Column>,
    primary_key: Vec<String>,
    scope: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    partition_column: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    shared: Option<bool>,
    adapter: String,
    encoding: String,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
struct Command {
    id: String,
    kind: String,
    collection: String,
    operations: Vec<String>,
    writable_columns: Vec<String>,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
struct Config {
    application: Application,
    protocol: Protocol,
    sources: Vec<Source>,
    scopes: Vec<Scope>,
    collections: Vec<Collection>,
    commands: Vec<Command>,
}

/// Inputs `init` cannot derive from an inspection document.
pub struct InitOptions {
    pub application_id: String,
    pub application_version: String,
    pub database_url_env: String,
    pub slot: Option<String>,
    pub scope_id: String,
    pub tenants: Option<Tenants>,
}

/// A partitioned scope: each grant reads the rows whose `column` holds its
/// `attribute`, and a table without `column` is shared by every tenant.
#[derive(Debug, Clone)]
pub struct Tenants {
    pub column: String,
    pub attribute: String,
}

const PARTITION_TYPES: [&str; 4] = ["int2", "int4", "int8", "uuid"];

/// The grant verifier's rule for an attribute name.
const MAX_GRANT_ATTRIBUTE_BYTES: usize = 512;

/// The tenant column's type when `relation` holds tenants' rows, or `None`
/// when every tenant shares it.
fn partitioning(
    relation: &crate::inspect::RelationOutput,
    primary_key: &[String],
    tenants: &Tenants,
) -> Result<Option<String>, Box<dyn Error>> {
    let Some(column) = relation
        .columns
        .iter()
        .find(|column| column.name == tenants.column)
    else {
        return Ok(None);
    };
    if primary_key.first() != Some(&tenants.column) {
        return Err(format!(
            "table {}.{} has a {} column, but its primary key is {}; a tenant's table leads its primary key with the tenant column, so make {} the key's first column, or rename the column if the table is shared by every tenant",
            relation.schema,
            relation.table,
            tenants.column,
            primary_key.join(", "),
            tenants.column
        )
        .into());
    }
    if column.type_schema != "pg_catalog" || !PARTITION_TYPES.contains(&column.type_name.as_str()) {
        return Err(format!(
            "table {}.{} holds its tenant in {}, of type {}; a tenant column is int2, int4, int8 or uuid",
            relation.schema, relation.table, tenants.column, column.type_name
        )
        .into());
    }
    Ok(Some(column.type_name.clone()))
}

/// Lower `value` into the configuration identifier grammar, or return `None`
/// when nothing usable survives.
fn identifier(value: &str) -> Option<String> {
    let mut result = String::with_capacity(value.len());
    let mut pending_separator = false;
    for character in value.chars() {
        let lowered = character.to_ascii_lowercase();
        if lowered.is_ascii_lowercase() || lowered.is_ascii_digit() {
            if pending_separator && !result.is_empty() {
                result.push('-');
            }
            pending_separator = false;
            result.push(lowered);
        } else {
            pending_separator = true;
        }
    }
    // The grammar requires a leading letter; a table named `2fa` is still
    // addressable, just prefixed.
    match result.chars().next() {
        None => None,
        Some(first) if first.is_ascii_digit() => Some(format!("t{result}")),
        Some(_) => Some(result),
    }
}

/// The configuration identifier grammar, `compileConfig`'s `ID`.
pub(crate) const IDENTIFIER_GRAMMAR: &str = "^[a-z][a-z0-9]*(?:[._-][a-z0-9]+)*$";

/// Whether `value` already matches the configuration identifier grammar,
/// [`IDENTIFIER_GRAMMAR`].
pub(crate) fn is_identifier(value: &str) -> bool {
    let mut characters = value.chars();
    if !characters
        .next()
        .is_some_and(|first| first.is_ascii_lowercase())
    {
        return false;
    }
    let mut after_separator = false;
    for character in characters {
        if character.is_ascii_lowercase() || character.is_ascii_digit() {
            after_separator = false;
        } else if matches!(character, '.' | '_' | '-') && !after_separator {
            after_separator = true;
        } else {
            return false;
        }
    }
    !after_separator
}

/// Lower `value` into the identifier grammar, naming `subject` when it cannot
/// be represented.
fn named_identifier(
    subject: std::fmt::Arguments<'_>,
    value: &str,
) -> Result<String, Box<dyn Error>> {
    identifier(value).ok_or_else(|| format!("{subject} has no usable identifier").into())
}

/// Reject anything `compileConfig`'s `ENVIRONMENT_VARIABLE`,
/// `^[A-Z_][A-Z0-9_]*$`, would.
///
/// The point of the flag is that the connection string stays in the
/// environment, so a value that is not an environment-variable name is very
/// often the connection string itself.
fn environment_variable_name(value: &str) -> Result<(), Box<dyn Error>> {
    if is_environment_variable_name(value) {
        return Ok(());
    }
    Err(format!(
        "--database-url-env must name an environment variable matching \
         ^[A-Z_][A-Z0-9_]*$, not a connection string; got {value:?}"
    )
    .into())
}

/// Whether `value` matches `compileConfig`'s `ENVIRONMENT_VARIABLE`,
/// `^[A-Z_][A-Z0-9_]*$`.
pub(crate) fn is_environment_variable_name(value: &str) -> bool {
    // `is_some_and` on the first character already rejects an empty name.
    value
        .chars()
        .next()
        .is_some_and(|first| first.is_ascii_uppercase() || first == '_')
        && value.chars().all(|character| {
            character.is_ascii_uppercase() || character.is_ascii_digit() || character == '_'
        })
}

/// Reject anything `compileConfig`'s `postgresIdentifier` would: at most 63
/// bytes, no NUL.
pub(crate) fn postgres_identifier(subject: &str, value: &str) -> Result<(), Box<dyn Error>> {
    if value.is_empty() {
        return Err(format!("{subject} must not be empty").into());
    }
    if value.contains('\0') {
        return Err(format!("{subject} must not contain a NUL byte").into());
    }
    if value.len() > MAX_POSTGRES_IDENTIFIER_BYTES {
        return Err(format!(
            "{subject} is {} bytes; PostgreSQL identifiers are at most {MAX_POSTGRES_IDENTIFIER_BYTES}",
            value.len()
        )
        .into());
    }
    Ok(())
}

/// Build a configuration manifest from `inspection`.
pub fn build(
    inspection: &InspectionOutput,
    options: &InitOptions,
) -> Result<String, Box<dyn Error>> {
    environment_variable_name(&options.database_url_env)?;
    if options.application_version.is_empty() {
        return Err("--application-version must not be empty".into());
    }
    if inspection.schema_version != INSPECTION_SCHEMA_VERSION {
        return Err(format!(
            "inspection document declares {} but {INSPECTION_SCHEMA_VERSION} is required",
            inspection.schema_version
        )
        .into());
    }
    if inspection.relations.is_empty() {
        return Err("inspection document contains no relations".into());
    }
    let source_id = named_identifier(
        format_args!("source {:?}", inspection.source),
        &inspection.source,
    )?;
    let scope_id = named_identifier(
        format_args!("scope {:?}", options.scope_id),
        &options.scope_id,
    )?;
    let application_id = named_identifier(
        format_args!("application id {:?}", options.application_id),
        &options.application_id,
    )?;

    // `serve` refuses a manifest whose relations share a table name, so this
    // does too rather than write one.
    let relations = inspection
        .relations
        .iter()
        .map(|relation| QualifiedTable {
            schema: relation.schema.clone(),
            table: relation.table.clone(),
        })
        .collect::<Vec<_>>();
    if let Some((first, second)) = QualifiedTable::first_shared_collection_name(&relations) {
        return Err(format!(
            "tables {} and {} would both be served as collection {}, which is named by the table alone; inspect one of them",
            first.display_name(),
            second.display_name(),
            second.table
        )
        .into());
    }

    if let Some(tenants) = &options.tenants {
        postgres_identifier("--tenant-column", &tenants.column)?;
        if tenants.attribute.is_empty()
            || tenants.attribute.len() > MAX_GRANT_ATTRIBUTE_BYTES
            || tenants.attribute.chars().any(char::is_control)
        {
            return Err(format!(
                "--tenant-attribute must be a grant attribute name: 1 to {MAX_GRANT_ATTRIBUTE_BYTES} bytes, with no control character"
            )
            .into());
        }
    }
    let mut tenant_types = std::collections::BTreeSet::new();
    let mut collections = Vec::with_capacity(inspection.relations.len());
    let mut commands = Vec::new();
    for relation in &inspection.relations {
        // The client binds a collection only to the artifact table of the same
        // name, so the collection id is the table name; a lowered
        // `Issue_Notes` (`issue-notes`) would compile and then never bind.
        if !is_identifier(&relation.table) {
            return Err(format!(
                "table {}.{} cannot be a collection: a collection's id is its table name, and {:?} does not match {IDENTIFIER_GRAMMAR}, which takes lowercase ASCII letters and digits, starting with a letter, with a single `.`, `_` or `-` between them (issue_notes matches; an uppercase letter, a leading digit, a leading, trailing or doubled underscore, or `$` does not); rename the table",
                relation.schema, relation.table, relation.table
            )
            .into());
        }
        let collection_id = relation.table.clone();

        let mut primary_key: Vec<(u32, String)> = relation
            .columns
            .iter()
            .filter_map(|column| {
                column
                    .primary_key_position
                    .map(|position| (position, column.name.clone()))
            })
            .collect();
        primary_key.sort_by_key(|left| left.0);
        let primary_key: Vec<String> = primary_key.into_iter().map(|(_, name)| name).collect();
        if primary_key.is_empty() {
            return Err(format!(
                "table {}.{} has no primary key; Calque collections require one",
                relation.schema, relation.table
            )
            .into());
        }

        let (partition_column, shared) = match &options.tenants {
            Some(tenants) => match partitioning(relation, &primary_key, tenants)? {
                Some(type_name) => {
                    tenant_types.insert(type_name);
                    (Some(tenants.column.clone()), None)
                }
                None => (None, Some(true)),
            },
            None => (None, None),
        };

        let columns: Vec<Column> = relation
            .columns
            .iter()
            .map(|column| Column {
                name: column.name.clone(),
                postgres_type: column.carried_type_name().to_owned(),
                nullable: column.nullable,
            })
            .collect();

        // `mutation-crud` decides the writable columns and operations; the
        // test `crud_command_operations_match_the_registered_command` holds
        // this file to it.
        let roles: Vec<CrudColumnRole> = relation
            .columns
            .iter()
            .map(|column| {
                CrudColumnRole::of(
                    column.codec(),
                    column.generated,
                    column.primary_key_position.is_some(),
                )
            })
            .collect();
        let writable_columns: Vec<String> = relation
            .columns
            .iter()
            .zip(&roles)
            .filter(|(_, role)| role.writable)
            .map(|(column, _)| column.name.clone())
            .collect();
        // `CrudCommand::for_table` refuses a key the version-1 key codec
        // cannot carry, so the registry registers nothing for such a table and
        // neither may the manifest.
        let key_is_carriable = relation.columns.iter().all(|column| {
            column.primary_key_position.is_none()
                || column
                    .codec()
                    .is_some_and(PostgresTypeCodec::is_primary_key_safe)
        });
        let operations: Vec<String> = if key_is_carriable {
            let operations = CrudOperations::ALL
                .without_insert_if(!relation.unlisted_required_columns.is_empty());
            admitted_operations(roles.iter().copied(), operations)
                .into_iter()
                .map(|operation| operation.as_str().to_owned())
                .collect()
        } else {
            Vec::new()
        };
        if !operations.is_empty() {
            commands.push(Command {
                id: format!("edit-{collection_id}"),
                kind: "crud".to_owned(),
                collection: collection_id.clone(),
                operations,
                writable_columns,
            });
        }

        collections.push(Collection {
            id: collection_id,
            source: source_id.clone(),
            kind: "table".to_owned(),
            relation: Relation {
                schema: relation.schema.clone(),
                table: relation.table.clone(),
            },
            columns,
            primary_key,
            scope: scope_id.clone(),
            partition_column,
            shared,
            adapter: "direct-columnar".to_owned(),
            encoding: "calque-columnar".to_owned(),
        });
    }

    if let Some(tenants) = &options.tenants {
        if tenant_types.is_empty() {
            return Err(format!(
                "no inspected table has a {} column, so every table would be shared and none would hold a tenant's rows",
                tenants.column
            )
            .into());
        }
        if tenant_types.len() > 1 {
            return Err(format!(
                "the {} columns have types {}; every tenant column has one type, the type of the grant attribute {}",
                tenants.column,
                tenant_types.into_iter().collect::<Vec<_>>().join(", "),
                tenants.attribute
            )
            .into());
        }
    }

    if commands.is_empty() {
        return Err(
            "no table admits a CRUD command, so no command can be generated; \
             a configuration requires at least one command"
                .into(),
        );
    }

    let slot = match &options.slot {
        Some(slot) => slot.clone(),
        // `identifier` left the id only lowercase letters, digits and `-`.
        None => format!("calque_{}_slot", application_id.replace('-', "_")),
    };
    // The derived slot name grows with the application id, so a long id
    // produces an over-long identifier rather than an obviously bad flag.
    postgres_identifier("the replication slot name", &slot)?;
    postgres_identifier("the publication name", &inspection.publication)?;

    let config = Config {
        application: Application {
            id: application_id,
            version: options.application_version.clone(),
        },
        protocol: Protocol {
            manifest: 1,
            artifact: 1,
            change_stream: 1,
            mutation: 1,
        },
        sources: vec![Source {
            id: source_id.clone(),
            database_url_env: options.database_url_env.clone(),
            publication: inspection.publication.clone(),
            slot,
        }],
        scopes: vec![Scope {
            id: scope_id,
            source: source_id,
            kind: if options.tenants.is_some() {
                "partitioned"
            } else {
                "source-wide"
            }
            .to_owned(),
            attribute: options
                .tenants
                .as_ref()
                .map(|tenants| tenants.attribute.clone()),
            data_version: 1,
        }],
        collections,
        commands,
    };
    Ok(format!("{}\n", serde_json::to_string_pretty(&config)?))
}

/// Write `contents` to `path`, refusing to clobber an existing file unless
/// `force` is set.
pub fn write(path: &Path, contents: &str, force: bool) -> Result<(), Box<dyn Error>> {
    if path.exists() && !force {
        return Err(format!(
            "{} already exists; pass --force to overwrite it",
            path.display()
        )
        .into());
    }
    if let Some(parent) = path.parent() {
        if !parent.as_os_str().is_empty() {
            fs::create_dir_all(parent)?;
        }
    }
    fs::write(path, contents)?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use super::*;
    use crate::inspect::{ColumnOutput, RelationOutput};
    use mutation_crud::crud_command_summaries;
    use source_control::{ColumnInspection, RegisteredTable, TableInspection, UnlistedColumn};
    use sync_types::{ColumnId, TableId};

    fn column(name: &str, type_name: &str, primary: Option<u32>) -> ColumnOutput {
        ColumnOutput {
            name: name.to_owned(),
            type_schema: "pg_catalog".to_owned(),
            type_name: type_name.to_owned(),
            nullable: primary.is_none(),
            generated: false,
            primary_key_position: primary,
            carried_as: None,
        }
    }

    fn inspection(relations: Vec<RelationOutput>) -> InspectionOutput {
        InspectionOutput {
            schema_version: INSPECTION_SCHEMA_VERSION.to_owned(),
            server_major: 18,
            source: "primary".to_owned(),
            publication: "calque_app".to_owned(),
            schema_fingerprint: "sha256:0".to_owned(),
            relations,
        }
    }

    fn options() -> InitOptions {
        InitOptions {
            application_id: "incident-ops".to_owned(),
            application_version: "0.1.0".to_owned(),
            database_url_env: "CALQUE_DATABASE_URL".to_owned(),
            slot: None,
            scope_id: "all".to_owned(),
            tenants: None,
        }
    }

    /// The checked-in inspection document `calque inspect` writes and `init`
    /// reads.
    const SHARED_INSPECTION_FIXTURE: &str =
        include_str!("../../../../tests/fixtures/config/incident-ops.pg18-inspection.json");

    #[test]
    fn the_shared_inspection_fixture_parses_as_what_inspect_emits() {
        // Binds what `inspect` emits to the fixture `init` is tested on.
        let inspection: InspectionOutput = serde_json::from_str(SHARED_INSPECTION_FIXTURE)
            .expect("the checked-in inspection fixture must parse as InspectionOutput");

        assert_eq!(inspection.schema_version, INSPECTION_SCHEMA_VERSION);
        assert_eq!(inspection.server_major, 18);
        assert!(!inspection.relations.is_empty());
        assert!(
            inspection
                .relations
                .iter()
                .flat_map(|relation| &relation.columns)
                .any(|column| column.primary_key_position.is_some()),
            "the fixture must exercise the optional primary-key field"
        );

        // Round-tripping proves the producer emits the same field spelling the
        // fixture uses, which is the half a parse alone would not catch: serde
        // ignores unknown fields here, so a renamed field would still parse.
        let reserialized: serde_json::Value =
            serde_json::to_value(&inspection).expect("inspection re-serializes");
        let original: serde_json::Value =
            serde_json::from_str(SHARED_INSPECTION_FIXTURE).expect("fixture is JSON");
        assert_eq!(reserialized, original);
    }

    #[test]
    fn the_shared_inspection_fixture_generates_a_configuration() {
        // End of the same chain: what `inspect` writes, `init` must consume.
        let rendered = build(
            &serde_json::from_str(SHARED_INSPECTION_FIXTURE).unwrap(),
            &options(),
        )
        .expect("the shared fixture must produce a configuration");
        let parsed: serde_json::Value = serde_json::from_str(&rendered).unwrap();
        assert!(!parsed["collections"].as_array().unwrap().is_empty());
        assert!(!parsed["commands"].as_array().unwrap().is_empty());
    }

    #[test]
    fn every_compile_config_rejection_is_rejected_here() {
        // Each case below is one `compileConfig` rule `init` must also refuse.
        let long = "x".repeat(MAX_POSTGRES_IDENTIFIER_BYTES + 1);
        // One table with a writable column, the minimum `build` accepts.
        let buildable = || {
            inspection(vec![RelationOutput {
                schema: "public".to_owned(),
                table: "issues".to_owned(),
                replica_identity: "default".to_owned(),
                unlisted_required_columns: Vec::new(),
                columns: vec![column("id", "uuid", Some(1)), column("title", "text", None)],
            }])
        };
        let cases: [(&str, InitOptions, &str); 5] = [
            (
                // `ENVIRONMENT_VARIABLE` -- and a lowercase name is what
                // someone reaching for a connection string types.
                "databaseUrlEnv grammar",
                InitOptions {
                    database_url_env: "pg_url".to_owned(),
                    ..options()
                },
                "--database-url-env",
            ),
            (
                "databaseUrlEnv must not be empty",
                InitOptions {
                    database_url_env: String::new(),
                    ..options()
                },
                "--database-url-env",
            ),
            (
                "application.version must not be empty",
                InitOptions {
                    application_version: String::new(),
                    ..options()
                },
                "--application-version",
            ),
            (
                // `postgresIdentifier`, given explicitly
                "slot identifier length",
                InitOptions {
                    slot: Some(long.clone()),
                    ..options()
                },
                "replication slot name",
            ),
            (
                // The same cap, reached through the derived name rather than
                // the flag.
                "derived slot identifier length",
                InitOptions {
                    application_id: long.clone(),
                    ..options()
                },
                "replication slot name",
            ),
        ];

        for (case, options, expected) in cases {
            let error = build(&buildable(), &options)
                .expect_err(&format!("{case} must be rejected"))
                .to_string();
            assert!(error.contains(expected), "{case}: {error}");
        }

        // `postgresIdentifier` applied to the publication, which arrives from
        // the inspection document rather than from a flag.
        let mut over_long = buildable();
        over_long.publication = long;
        let error = build(&over_long, &options())
            .expect_err("an over-long publication must be rejected")
            .to_string();
        assert!(error.contains("publication name"), "{error}");

        build(&buildable(), &options()).expect("the valid fixture still builds");
    }

    #[test]
    fn identifiers_follow_the_configuration_grammar() {
        assert_eq!(identifier("issues").as_deref(), Some("issues"));
        assert_eq!(identifier("Issue_Notes").as_deref(), Some("issue-notes"));
        assert_eq!(identifier("2fa_tokens").as_deref(), Some("t2fa-tokens"));
        assert_eq!(identifier("__").as_deref(), None);
    }

    fn tenant_inspection(tenant_type: &str) -> InspectionOutput {
        let table = |name: &str, columns| RelationOutput {
            schema: "public".to_owned(),
            table: name.to_owned(),
            replica_identity: "default".to_owned(),
            unlisted_required_columns: Vec::new(),
            columns,
        };
        inspection(vec![
            table(
                "priorities",
                vec![column("id", "int8", Some(1)), column("label", "text", None)],
            ),
            table(
                "tasks",
                vec![
                    column("tenant_id", tenant_type, Some(1)),
                    column("id", "int8", Some(2)),
                    column("title", "text", None),
                ],
            ),
        ])
    }

    fn tenant_options() -> InitOptions {
        InitOptions {
            tenants: Some(Tenants {
                column: "tenant_id".to_owned(),
                attribute: "org".to_owned(),
            }),
            ..options()
        }
    }

    #[test]
    fn a_tenant_column_partitions_the_tables_it_leads_and_shares_the_rest() {
        let parsed: serde_json::Value =
            serde_json::from_str(&build(&tenant_inspection("int8"), &tenant_options()).unwrap())
                .unwrap();
        assert_eq!(
            parsed["scopes"][0],
            serde_json::json!({
                "id": "all",
                "source": "primary",
                "kind": "partitioned",
                "attribute": "org",
                "dataVersion": 1
            })
        );
        assert_eq!(parsed["collections"][0]["shared"], true);
        assert!(parsed["collections"][0].get("partitionColumn").is_none());
        assert_eq!(parsed["collections"][1]["partitionColumn"], "tenant_id");
        assert!(parsed["collections"][1].get("shared").is_none());

        let source_wide: serde_json::Value =
            serde_json::from_str(&build(&tenant_inspection("int8"), &options()).unwrap()).unwrap();
        assert_eq!(source_wide["scopes"][0]["kind"], "source-wide");
        assert!(source_wide["scopes"][0].get("attribute").is_none());
        for collection in source_wide["collections"].as_array().unwrap() {
            assert!(collection.get("partitionColumn").is_none());
            assert!(collection.get("shared").is_none());
        }
    }

    #[test]
    fn a_tenant_column_that_cannot_partition_is_refused() {
        let error = build(&tenant_inspection("text"), &tenant_options())
            .unwrap_err()
            .to_string();
        assert!(
            error.contains("of type text; a tenant column is int2, int4, int8 or uuid"),
            "{error}"
        );

        let mut trailing = tenant_inspection("int8");
        trailing.relations[1].columns[0].primary_key_position = Some(2);
        trailing.relations[1].columns[1].primary_key_position = Some(1);
        let error = build(&trailing, &tenant_options()).unwrap_err().to_string();
        assert!(
            error.contains("its primary key is id, tenant_id"),
            "{error}"
        );

        let mut mixed = tenant_inspection("int8");
        mixed.relations.push(RelationOutput {
            schema: "public".to_owned(),
            table: "projects".to_owned(),
            replica_identity: "default".to_owned(),
            unlisted_required_columns: Vec::new(),
            columns: vec![
                column("tenant_id", "int4", Some(1)),
                column("id", "int8", Some(2)),
            ],
        });
        let error = build(&mixed, &tenant_options()).unwrap_err().to_string();
        assert!(error.contains("have types int4, int8"), "{error}");

        let mut none = tenant_inspection("int8");
        none.relations.truncate(1);
        let error = build(&none, &tenant_options()).unwrap_err().to_string();
        assert!(
            error.contains("no inspected table has a tenant_id column"),
            "{error}"
        );

        for attribute in ["", "a\nb"] {
            let options = InitOptions {
                tenants: Some(Tenants {
                    column: "tenant_id".to_owned(),
                    attribute: attribute.to_owned(),
                }),
                ..options()
            };
            let error = build(&tenant_inspection("int8"), &options)
                .unwrap_err()
                .to_string();
            assert!(error.contains("--tenant-attribute"), "{error}");
        }
    }

    #[test]
    fn a_collection_is_named_by_its_table_or_refused() {
        let document = inspection(vec![RelationOutput {
            schema: "incidents".to_owned(),
            table: "issue_tags".to_owned(),
            replica_identity: "default".to_owned(),
            unlisted_required_columns: Vec::new(),
            columns: vec![
                column("bench_index", "int8", Some(1)),
                column("tag", "text", None),
            ],
        }]);
        let rendered = build(&document, &options()).unwrap();
        let parsed: serde_json::Value = serde_json::from_str(&rendered).unwrap();
        assert_eq!(parsed["collections"][0]["id"], "issue_tags");
        assert_eq!(parsed["commands"][0]["id"], "edit-issue_tags");
    }

    #[test]
    fn a_table_name_outside_the_identifier_grammar_is_refused() {
        for table in [
            "Issue_Notes",
            "_issue_notes",
            "issue__tags",
            "issue_",
            "2fa",
            "issue$notes",
        ] {
            assert!(!is_identifier(table), "{table}");
            let document = inspection(vec![RelationOutput {
                schema: "public".to_owned(),
                table: table.to_owned(),
                replica_identity: "default".to_owned(),
                unlisted_required_columns: Vec::new(),
                columns: vec![column("id", "uuid", Some(1)), column("title", "text", None)],
            }]);
            let error = build(&document, &options())
                .expect_err("a table name the grammar does not admit has no binding id")
                .to_string();
            assert!(
                error.starts_with(&format!(
                    "table public.{table} cannot be a collection: a collection's id is its table name"
                )),
                "{error}"
            );
            assert!(error.contains(IDENTIFIER_GRAMMAR), "{error}");
            assert!(error.contains("(issue_notes matches; "), "{error}");
            assert!(error.ends_with("; rename the table"), "{error}");
        }
    }

    /// The default slot is the manifest's application id with each `-` made
    /// `_`. That id is already normalized, so a `.` or `_` in
    /// `--application-id` reaches the slot only by way of a `-`, and the
    /// flag's help states the rule in those terms.
    #[test]
    fn the_default_slot_is_the_manifests_application_id_as_the_help_says() {
        let document = inspection(vec![RelationOutput {
            schema: "public".to_owned(),
            table: "issues".to_owned(),
            replica_identity: "default".to_owned(),
            unlisted_required_columns: Vec::new(),
            columns: vec![column("id", "uuid", Some(1)), column("title", "text", None)],
        }]);
        let options = InitOptions {
            application_id: "Incident.Ops_v2".to_owned(),
            ..options()
        };
        let parsed: serde_json::Value =
            serde_json::from_str(&build(&document, &options).unwrap()).unwrap();
        assert_eq!(parsed["application"]["id"], "incident-ops-v2");
        assert_eq!(parsed["sources"][0]["slot"], "calque_incident_ops_v2_slot");

        use clap::CommandFactory as _;
        let command = crate::cli::Cli::command();
        let slot = command
            .find_subcommand("init")
            .unwrap()
            .get_arguments()
            .find(|argument| argument.get_id() == "slot")
            .unwrap();
        assert_eq!(
            slot.get_help().unwrap().to_string(),
            "Replication slot name. Defaults to `calque_<id>_slot`, where `<id>` is the \
             manifest's application id with each `-` replaced by `_`"
        );
    }

    #[test]
    fn generated_configuration_carries_keys_columns_and_one_command() {
        let document = inspection(vec![RelationOutput {
            schema: "public".to_owned(),
            table: "issues".to_owned(),
            replica_identity: "default".to_owned(),
            unlisted_required_columns: Vec::new(),
            columns: vec![column("id", "uuid", Some(1)), column("title", "text", None)],
        }]);
        let rendered = build(&document, &options()).unwrap();
        let parsed: serde_json::Value = serde_json::from_str(&rendered).unwrap();
        assert_eq!(parsed["collections"][0]["id"], "issues");
        assert_eq!(parsed["collections"][0]["adapter"], "direct-columnar");
        assert_eq!(parsed["collections"][0]["primaryKey"][0], "id");
        assert_eq!(parsed["commands"][0]["id"], "edit-issues");
        // The primary key is immutable, so only `title` is writable.
        assert_eq!(parsed["commands"][0]["writableColumns"][0], "title");
        assert_eq!(
            parsed["commands"][0]["writableColumns"]
                .as_array()
                .unwrap()
                .len(),
            1
        );
        assert_eq!(parsed["sources"][0]["slot"], "calque_incident_ops_slot");
    }

    #[test]
    fn one_table_name_in_two_schemas_is_refused() {
        let document = inspection(vec![
            RelationOutput {
                schema: "public".to_owned(),
                table: "issues".to_owned(),
                replica_identity: "default".to_owned(),
                unlisted_required_columns: Vec::new(),
                columns: vec![column("id", "uuid", Some(1)), column("title", "text", None)],
            },
            RelationOutput {
                schema: "audit".to_owned(),
                table: "issues".to_owned(),
                replica_identity: "default".to_owned(),
                unlisted_required_columns: Vec::new(),
                columns: vec![column("id", "uuid", Some(1)), column("note", "text", None)],
            },
        ]);
        assert_eq!(
            build(&document, &options()).unwrap_err().to_string(),
            "tables public.issues and audit.issues would both be served as collection issues, \
             which is named by the table alone; inspect one of them"
        );
    }

    #[test]
    fn a_table_without_a_primary_key_is_rejected() {
        let document = inspection(vec![RelationOutput {
            schema: "public".to_owned(),
            table: "events".to_owned(),
            replica_identity: "default".to_owned(),
            unlisted_required_columns: Vec::new(),
            columns: vec![column("payload", "jsonb", None)],
        }]);
        let error = build(&document, &options()).unwrap_err().to_string();
        assert!(error.contains("primary key"), "unexpected error: {error}");
    }

    /// The registry's own view of one inspected relation: what
    /// `source-control` would enroll and `mutation-crud` would register a
    /// command for. Column ids follow catalog order, as `SchemaRegistry`
    /// allocates them.
    fn registered(relation: &RelationOutput) -> RegisteredTable {
        RegisteredTable {
            table_id: TableId::new(format!("{}.{}", relation.schema, relation.table))
                .expect("fixture relations have wire-safe names"),
            column_ids: (1..=u32::try_from(relation.columns.len()).unwrap())
                .map(ColumnId::new)
                .collect(),
            inspection: TableInspection {
                table: QualifiedTable {
                    schema: relation.schema.clone(),
                    table: relation.table.clone(),
                },
                relation_oid: 1,
                in_publication: true,
                relation_kind: 'r',
                persistence: 'p',
                replica_identity: 'd',
                is_partition: false,
                has_publication_row_filter: false,
                has_publication_column_list: !relation.unlisted_required_columns.is_empty(),
                unlisted_columns: relation
                    .unlisted_required_columns
                    .iter()
                    .map(|name| UnlistedColumn {
                        name: name.clone(),
                        primary_key: false,
                        required_on_insert: true,
                    })
                    .collect(),
                columns: relation
                    .columns
                    .iter()
                    .enumerate()
                    .map(|(index, column)| ColumnInspection {
                        attribute_number: i16::try_from(index + 1).unwrap(),
                        name: column.name.clone(),
                        type_oid: 0,
                        type_modifier: -1,
                        type_schema: column.type_schema.clone(),
                        type_name: column.type_name.clone(),
                        nullable: column.nullable,
                        primary_key_ordinal: column.primary_key_position,
                        generated: column.generated,
                        nondeterministic_collation: false,
                        carried_as: column.carried_as.clone(),
                    })
                    .collect(),
            },
        }
    }

    /// The operations `init` writes become facade methods, and `calque-engine`
    /// registers commands from `crud_command_summaries` over the same
    /// relations; this holds the two equal, by collection, on operations and
    /// writable columns. The relations cover each admission branch: every
    /// column writable or key, a generated column (no insert), key columns only
    /// (no update), and a non-key column with no version-1 codec (not
    /// writable).
    #[test]
    fn crud_command_operations_match_the_registered_command() {
        let relations = vec![
            RelationOutput {
                schema: "public".to_owned(),
                table: "tasks".to_owned(),
                replica_identity: "default".to_owned(),
                unlisted_required_columns: Vec::new(),
                columns: vec![
                    column("id", "int8", Some(1)),
                    column("title", "text", None),
                    column("done", "bool", None),
                ],
            },
            RelationOutput {
                schema: "public".to_owned(),
                table: "invoices".to_owned(),
                replica_identity: "default".to_owned(),
                unlisted_required_columns: Vec::new(),
                columns: vec![
                    column("id", "uuid", Some(1)),
                    column("amount", "int4", None),
                    ColumnOutput {
                        generated: true,
                        ..column("amount_with_tax", "int4", None)
                    },
                ],
            },
            RelationOutput {
                schema: "public".to_owned(),
                table: "issue_tags".to_owned(),
                replica_identity: "full".to_owned(),
                unlisted_required_columns: Vec::new(),
                columns: vec![
                    column("issue_id", "uuid", Some(1)),
                    column("tag_id", "uuid", Some(2)),
                ],
            },
            RelationOutput {
                schema: "public".to_owned(),
                table: "shapes".to_owned(),
                replica_identity: "default".to_owned(),
                unlisted_required_columns: Vec::new(),
                columns: vec![
                    column("id", "int8", Some(1)),
                    column("outline", "polygon", None),
                ],
            },
        ];
        let tables: Vec<RegisteredTable> = relations.iter().map(registered).collect();
        let summaries = crud_command_summaries(&tables, CrudOperations::ALL);
        assert_eq!(
            summaries.len(),
            relations.len(),
            "every fixture relation must register a command, or this compares nothing"
        );
        let rendered = build(&inspection(relations), &options()).unwrap();
        let parsed: serde_json::Value = serde_json::from_str(&rendered).unwrap();

        let declared: BTreeMap<String, (Vec<String>, Vec<String>)> = parsed["commands"]
            .as_array()
            .unwrap()
            .iter()
            .map(|command| {
                let strings = |field: &str| -> Vec<String> {
                    command[field]
                        .as_array()
                        .unwrap()
                        .iter()
                        .map(|value| value.as_str().unwrap().to_owned())
                        .collect()
                };
                (
                    command["collection"].as_str().unwrap().to_owned(),
                    (strings("operations"), strings("writableColumns")),
                )
            })
            .collect();
        let registered_by_collection: BTreeMap<String, (Vec<String>, Vec<String>)> = summaries
            .iter()
            .map(|summary| {
                (
                    // The manifest names each collection by its table; the
                    // registry names tables by relation.
                    summary
                        .table_id
                        .as_str()
                        .split_once('.')
                        .expect("fixture table ids are schema-qualified")
                        .1
                        .to_owned(),
                    (
                        summary
                            .operations
                            .iter()
                            .map(|operation| operation.as_str().to_owned())
                            .collect(),
                        summary.writable_columns.clone(),
                    ),
                )
            })
            .collect();
        assert_eq!(declared, registered_by_collection);

        // The comparison is only worth something if it is not four copies of
        // one answer: the fixture must actually separate the branches.
        let operations_of = |collection: &str| declared[collection].0.clone();
        assert_eq!(
            operations_of("tasks"),
            vec!["insert", "update", "delete"],
            "a table whose every column is key or writable admits all three"
        );
        assert_eq!(
            operations_of("invoices"),
            vec!["update", "delete"],
            "a generated column cannot be inserted"
        );
        assert_eq!(
            operations_of("issue_tags"),
            vec!["insert", "delete"],
            "a key-only table has nothing to patch"
        );
        assert_eq!(
            operations_of("shapes"),
            vec!["delete"],
            "a column with no version-1 codec is neither writable nor insertable"
        );
    }

    /// A key the version-1 codec cannot carry registers no command at all, so
    /// the manifest must not declare one either.
    #[test]
    fn a_key_the_wire_codec_cannot_carry_declares_no_command() {
        let unsupported = RelationOutput {
            schema: "public".to_owned(),
            table: "ledger".to_owned(),
            replica_identity: "default".to_owned(),
            unlisted_required_columns: Vec::new(),
            columns: vec![
                column("id", "numeric", Some(1)),
                column("memo", "text", None),
            ],
        };
        let table = registered(&unsupported);
        assert!(
            crud_command_summaries(std::slice::from_ref(&table), CrudOperations::ALL).is_empty(),
            "the registry must refuse this key, or the test proves nothing"
        );
        let error = build(&inspection(vec![unsupported]), &options())
            .unwrap_err()
            .to_string();
        assert!(
            error.contains("no table admits a CRUD command"),
            "unexpected error: {error}"
        );
    }

    #[test]
    fn a_stale_inspection_schema_version_is_rejected() {
        let mut document = inspection(vec![RelationOutput {
            schema: "public".to_owned(),
            table: "issues".to_owned(),
            replica_identity: "default".to_owned(),
            unlisted_required_columns: Vec::new(),
            columns: vec![column("id", "uuid", Some(1)), column("title", "text", None)],
        }]);
        document.schema_version = "calque.pg-inspection.v0".to_owned();
        let error = build(&document, &options()).unwrap_err().to_string();
        assert!(error.contains("v0"), "unexpected error: {error}");
    }
}
