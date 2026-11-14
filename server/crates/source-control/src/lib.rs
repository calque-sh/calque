//! PostgreSQL source enrollment inspection and fail-closed validation.

use std::collections::{BTreeMap, BTreeSet};

use postgres::{Client, IsolationLevel, Row};
use serde::{Deserialize, Serialize};
use sync_types::{CursorCodec, SourceEpoch};
use thiserror::Error;

mod registry;
mod type_codec;

pub use pgoutput_decoder::{PgoutputLaunchProfile, PgoutputProfileError};
pub use registry::{RegisteredTable, SchemaRegistry, SchemaRegistryBinding, SchemaRegistryError};
pub use type_codec::PostgresTypeCodec;

pub const MIN_POSTGRES_MAJOR: u32 = 18;
pub const MAX_POSTGRES_MAJOR: u32 = 18;

/// PostgreSQL's identifier length limit (`NAMEDATALEN - 1`). The server
/// truncates a longer name instead of rejecting it, so derived names are
/// bounded here.
pub const MAX_POSTGRES_IDENTIFIER_BYTES: usize = 63;

/// A source's activation, carried through snapshot publication: its registry
/// binding, source epoch and cursor key, created when a checked schema
/// registry is activated and rotated as one unit.
#[derive(Clone)]
pub struct ActivatedSource {
    registry_binding: SchemaRegistryBinding,
    source_epoch: SourceEpoch,
    cursor_codec: CursorCodec,
    pgoutput_profile: PgoutputLaunchProfile,
}

impl ActivatedSource {
    pub fn new(
        registry: &SchemaRegistry,
        source_epoch: SourceEpoch,
        cursor_codec: CursorCodec,
        pgoutput_profile: PgoutputLaunchProfile,
    ) -> Self {
        Self {
            registry_binding: registry.binding(),
            source_epoch,
            cursor_codec,
            pgoutput_profile,
        }
    }

    pub fn registry_binding(&self) -> &SchemaRegistryBinding {
        &self.registry_binding
    }

    pub fn source_epoch(&self) -> &SourceEpoch {
        &self.source_epoch
    }

    pub fn cursor_codec(&self) -> &CursorCodec {
        &self.cursor_codec
    }

    pub const fn pgoutput_profile(&self) -> PgoutputLaunchProfile {
        self.pgoutput_profile
    }
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct QualifiedTable {
    pub schema: String,
    pub table: String,
}

impl QualifiedTable {
    pub fn display_name(&self) -> String {
        format!("{}.{}", self.schema, self.table)
    }

    /// Parse the `schema.table` spelling every CLI accepts: an exact pair of
    /// simple PostgreSQL identifiers. `a.b.c` and quoted names are rejected.
    pub fn parse_dotted(value: &str) -> Result<Self, String> {
        let mut parts = value.split('.');
        let schema = parts.next().unwrap_or_default();
        let table = parts.next().unwrap_or_default();
        if parts.next().is_some()
            || !is_simple_postgres_identifier(schema)
            || !is_simple_postgres_identifier(table)
        {
            return Err(
                "table must be an exact schema.table pair using simple PostgreSQL identifiers"
                    .to_owned(),
            );
        }
        Ok(Self {
            schema: schema.to_owned(),
            table: table.to_owned(),
        })
    }

    /// The first two of `tables`, in order, whose table names are equal.
    ///
    /// A served table's collection is named by its table alone, in the
    /// generated client schema and in the artifact, so `a.tasks` and `b.tasks`
    /// cannot both be served. Names compare exactly: the client folds no case.
    pub fn first_shared_collection_name<'a>(
        tables: impl IntoIterator<Item = &'a Self>,
    ) -> Option<(&'a Self, &'a Self)> {
        let mut seen = BTreeMap::new();
        tables.into_iter().find_map(|table| {
            seen.insert(table.table.as_str(), table)
                .map(|first| (first, table))
        })
    }
}

fn is_simple_postgres_identifier(value: &str) -> bool {
    let mut bytes = value.bytes();
    matches!(bytes.next(), Some(first) if first.is_ascii_alphabetic() || first == b'_')
        && bytes.all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'_' | b'$'))
}

/// SQL that is true when the `pg_roles` row aliased `role` may open a
/// walsender: it has the REPLICATION attribute or, on Amazon RDS and Aurora,
/// which grant replication through a role instead, it is a member of
/// `rds_replication`.
pub const ROLE_MAY_REPLICATE: &str = "(role.rolreplication OR EXISTS (
    SELECT 1 FROM pg_catalog.pg_roles AS granted
     WHERE granted.rolname = 'rds_replication'
       AND pg_catalog.pg_has_role(role.oid, granted.oid, 'MEMBER')))";

/// Quotes one PostgreSQL identifier, doubling any `"` it contains.
pub fn quote_identifier(identifier: &str) -> String {
    format!("\"{}\"", identifier.replace('"', "\"\""))
}

/// Whether `name` is a replication slot name PostgreSQL accepts: 1 to 63
/// bytes of lowercase ASCII letters, digits and `_`.
pub fn is_valid_slot_name(name: &str) -> bool {
    !name.is_empty()
        && name.len() <= 63
        && name
            .bytes()
            .all(|byte| byte.is_ascii_lowercase() || byte.is_ascii_digit() || byte == b'_')
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct SourceIdentity {
    pub system_identifier: String,
    pub database_name: String,
    pub timeline_id: u32,
    pub previous_timeline_id: u32,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct SourceSettings {
    pub server_version_num: u32,
    pub server_encoding: String,
    pub date_style: String,
    pub time_zone: String,
    pub bytea_output: String,
    pub wal_level: String,
    pub max_replication_slots: u32,
    pub max_wal_senders: u32,
    pub current_user: String,
    pub current_user_is_superuser: bool,
    pub current_user_has_replication: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct PublicationSettings {
    pub name: String,
    pub exists: bool,
    pub publishes_insert: bool,
    pub publishes_update: bool,
    pub publishes_delete: bool,
    pub publishes_truncate: bool,
    pub publishes_via_partition_root: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ColumnInspection {
    /// PostgreSQL catalog identity used only to pin the observed relation layout.
    /// It is never a wire `ColumnId`.
    pub attribute_number: i16,
    pub name: String,
    pub type_oid: u32,
    pub type_modifier: i32,
    pub type_schema: String,
    pub type_name: String,
    pub nullable: bool,
    pub primary_key_ordinal: Option<u32>,
    pub generated: bool,
    /// The column's collation compares unequal byte strings as equal.
    /// Serialized only when set, so it leaves the schema fingerprint of a
    /// column under a deterministic collation unchanged.
    #[serde(default, skip_serializing_if = "is_false")]
    pub nondeterministic_collation: bool,
    /// The pg_catalog type this column's values travel as when its own type
    /// is not a built-in: `text` for an enum, the base type for a domain.
    /// Serialized only when set, so it leaves the schema fingerprint of a
    /// built-in column unchanged.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub carried_as: Option<String>,
}

impl ColumnInspection {
    /// The pg_catalog type name this column's values travel as.
    pub fn carried_type_name(&self) -> &str {
        self.carried_as.as_deref().unwrap_or(&self.type_name)
    }

    /// The codec this column's values travel with, or `None` when Calque
    /// carries no value of its type.
    pub fn codec(&self) -> Option<PostgresTypeCodec> {
        match &self.carried_as {
            Some(carried) => PostgresTypeCodec::from_catalog("pg_catalog", carried),
            None => PostgresTypeCodec::from_catalog(&self.type_schema, &self.type_name),
        }
    }
}

/// A column the publication's column list leaves out. pgoutput never sends
/// it, and no snapshot, plan or command of Calque's reads it.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct UnlistedColumn {
    pub name: String,
    pub primary_key: bool,
    /// An insert must give it a value: it is NOT NULL, directly or through a
    /// domain, and has no default, identity or generation expression.
    pub required_on_insert: bool,
}

fn is_false(value: &bool) -> bool {
    !*value
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct TableInspection {
    pub table: QualifiedTable,
    /// PostgreSQL catalog identity used only by the source decoder.
    /// It is never a stable public `TableId`.
    pub relation_oid: u32,
    pub in_publication: bool,
    pub relation_kind: char,
    pub persistence: char,
    pub replica_identity: char,
    pub is_partition: bool,
    pub has_publication_row_filter: bool,
    pub has_publication_column_list: bool,
    /// The columns the publication carries, in attribute order.
    pub columns: Vec<ColumnInspection>,
    /// The columns its column list leaves out. Serialized only when there
    /// are some, so it leaves the schema fingerprint of a table published
    /// whole unchanged.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub unlisted_columns: Vec<UnlistedColumn>,
}

impl TableInspection {
    /// An insert through Calque cannot succeed: it names only the columns
    /// the publication carries, and an unlisted one needs a value.
    pub fn insert_needs_unlisted_column(&self) -> bool {
        self.unlisted_columns
            .iter()
            .any(|column| column.required_on_insert)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct EnrollmentSnapshot {
    pub identity: SourceIdentity,
    pub settings: SourceSettings,
    pub publication: PublicationSettings,
    pub tables: Vec<TableInspection>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EnrollmentRequirements {
    pub expected_identity: SourceIdentity,
    pub publication_name: String,
    pub tables: BTreeSet<QualifiedTable>,
    /// The `pgoutput` profile every slot of this source is opened with.
    pub pgoutput_profile: PgoutputLaunchProfile,
}

impl EnrollmentRequirements {
    pub fn new(
        expected_identity: SourceIdentity,
        publication_name: impl Into<String>,
        tables: BTreeSet<QualifiedTable>,
    ) -> Self {
        Self {
            expected_identity,
            publication_name: publication_name.into(),
            tables,
            pgoutput_profile: PgoutputLaunchProfile::V1,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum DiagnosticSeverity {
    Error,
    Warning,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct EnrollmentDiagnostic {
    pub severity: DiagnosticSeverity,
    pub code: String,
    pub message: String,
    pub table: Option<QualifiedTable>,
    pub column: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct EnrollmentReport {
    pub diagnostics: Vec<EnrollmentDiagnostic>,
}

impl EnrollmentReport {
    pub fn is_ready(&self) -> bool {
        self.blocking().next().is_none()
    }

    /// The diagnostics that make this report not ready, in report order.
    pub fn blocking(&self) -> impl Iterator<Item = &EnrollmentDiagnostic> {
        self.diagnostics
            .iter()
            .filter(|diagnostic| diagnostic.severity == DiagnosticSeverity::Error)
    }
}

/// Renders the blocking diagnostics, each naming the setting, privilege,
/// publication, or column to change. Warnings are omitted.
impl std::fmt::Display for EnrollmentReport {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let mut blocking = self.blocking().peekable();
        if blocking.peek().is_none() {
            return formatter.write_str("no blocking diagnostics");
        }
        for (index, diagnostic) in blocking.enumerate() {
            if index > 0 {
                formatter.write_str("; ")?;
            }
            write!(formatter, "{}: {}", diagnostic.code, diagnostic.message)?;
            if let Some(table) = &diagnostic.table {
                write!(formatter, " [{}", table.display_name())?;
                if let Some(column) = &diagnostic.column {
                    write!(formatter, ".{column}")?;
                }
                formatter.write_str("]")?;
            }
        }
        Ok(())
    }
}

#[derive(Debug, Error)]
pub enum InspectError {
    #[error("PostgreSQL inspection failed: {0}")]
    Postgres(#[from] postgres::Error),
    #[error("PostgreSQL returned invalid numeric setting {name}={value}")]
    InvalidSetting { name: &'static str, value: i32 },
    #[error("PostgreSQL returned invalid catalog OID {name}={value}")]
    InvalidCatalogOid { name: &'static str, value: i64 },
    #[error("PostgreSQL returned invalid one-character catalog value {field}={value:?}")]
    InvalidCatalogCharacter { field: &'static str, value: String },
}

/// Captures enrollment facts in one repeatable-read transaction.
///
/// This inspects configuration only. An artifact's point-in-time snapshot
/// comes from an exported logical-slot snapshot and a cut-marker proof
/// instead.
pub fn inspect_source(
    client: &mut Client,
    publication_name: &str,
    required_tables: &BTreeSet<QualifiedTable>,
) -> Result<EnrollmentSnapshot, InspectError> {
    let mut transaction = client
        .build_transaction()
        .isolation_level(IsolationLevel::RepeatableRead)
        .read_only(true)
        .start()?;

    let row = transaction.query_one(
        &format!(
            "SELECT current_setting('server_version_num')::int4,
                current_setting('server_encoding'),
                current_setting('DateStyle'),
                current_setting('TimeZone'),
                current_setting('bytea_output'),
                current_setting('wal_level'),
                current_setting('max_replication_slots')::int4,
                current_setting('max_wal_senders')::int4,
                current_user,
                role.rolsuper,
                {ROLE_MAY_REPLICATE},
                current_database(),
                control.system_identifier::text,
                checkpoint.timeline_id,
                checkpoint.prev_timeline_id
           FROM pg_catalog.pg_roles AS role
          CROSS JOIN pg_catalog.pg_control_system() AS control
          CROSS JOIN pg_catalog.pg_control_checkpoint() AS checkpoint
          WHERE role.rolname = current_user"
        ),
        &[],
    )?;
    let settings = SourceSettings {
        server_version_num: non_negative_setting("server_version_num", row.get(0))?,
        server_encoding: row.get(1),
        date_style: row.get(2),
        time_zone: row.get(3),
        bytea_output: row.get(4),
        wal_level: row.get(5),
        max_replication_slots: non_negative_setting("max_replication_slots", row.get(6))?,
        max_wal_senders: non_negative_setting("max_wal_senders", row.get(7))?,
        current_user: row.get(8),
        current_user_is_superuser: row.get(9),
        current_user_has_replication: row.get(10),
    };
    let identity = SourceIdentity {
        database_name: row.get(11),
        system_identifier: row.get(12),
        timeline_id: non_negative_setting("timeline_id", row.get(13))?,
        previous_timeline_id: non_negative_setting("previous_timeline_id", row.get(14))?,
    };

    let publication_row = transaction.query_opt(
        "SELECT pubinsert, pubupdate, pubdelete, pubtruncate, pubviaroot
           FROM pg_catalog.pg_publication
          WHERE pubname = $1",
        &[&publication_name],
    )?;
    let publication = publication_row.map_or_else(
        || PublicationSettings {
            name: publication_name.to_owned(),
            exists: false,
            publishes_insert: false,
            publishes_update: false,
            publishes_delete: false,
            publishes_truncate: false,
            publishes_via_partition_root: false,
        },
        |row| PublicationSettings {
            name: publication_name.to_owned(),
            exists: true,
            publishes_insert: row.get(0),
            publishes_update: row.get(1),
            publishes_delete: row.get(2),
            publishes_truncate: row.get(3),
            publishes_via_partition_root: row.get(4),
        },
    );

    let publication_tables: BTreeSet<QualifiedTable> = transaction
        .query(
            "SELECT schemaname, tablename
               FROM pg_catalog.pg_publication_tables
              WHERE pubname = $1",
            &[&publication_name],
        )?
        .into_iter()
        .map(|row| QualifiedTable {
            schema: row.get(0),
            table: row.get(1),
        })
        .collect();

    // Every required table's relation and publication features in one query,
    // and every found relation's columns in another, so the inspection's round
    // trips do not grow with the table count.
    let schemas: Vec<&str> = required_tables
        .iter()
        .map(|table| table.schema.as_str())
        .collect();
    let names: Vec<&str> = required_tables
        .iter()
        .map(|table| table.table.as_str())
        .collect();
    let mut relations: BTreeMap<i64, Row> = transaction
        .query(
            "SELECT required.ordinality,
                    class.oid::int8,
                    class.relkind::text,
                    class.relpersistence::text,
                    class.relreplident::text,
                    class.relispartition,
                    EXISTS (
                      SELECT 1
                        FROM pg_catalog.pg_publication_rel AS publication_relation
                        JOIN pg_catalog.pg_publication AS publication
                          ON publication.oid = publication_relation.prpubid
                       WHERE publication.pubname = $1
                         AND publication_relation.prrelid = class.oid
                         AND publication_relation.prqual IS NOT NULL
                    ),
                    EXISTS (
                      SELECT 1
                        FROM pg_catalog.pg_publication_rel AS publication_relation
                        JOIN pg_catalog.pg_publication AS publication
                          ON publication.oid = publication_relation.prpubid
                       WHERE publication.pubname = $1
                         AND publication_relation.prrelid = class.oid
                         AND publication_relation.prattrs IS NOT NULL
                    )
               FROM unnest($2::name[], $3::name[])
                    WITH ORDINALITY AS required(schema_name, table_name, ordinality)
               JOIN pg_catalog.pg_namespace AS namespace
                 ON namespace.nspname = required.schema_name
               JOIN pg_catalog.pg_class AS class
                 ON class.relnamespace = namespace.oid
                AND class.relname = required.table_name",
            &[&publication_name, &schemas, &names],
        )?
        .into_iter()
        .map(|row| (row.get(0), row))
        .collect();
    let relation_oids = relations
        .values()
        .map(|relation| catalog_oid("relation_oid", relation.get(1)))
        .collect::<Result<Vec<_>, InspectError>>()?;
    // A column of an enum travels as its label's text, and a column of a
    // domain as the type under it, through any chain of domains. A column the
    // publication's column list leaves out is kept apart: pgoutput never
    // sends it, so no snapshot, plan or command may read it either.
    let mut columns: BTreeMap<u32, Vec<ColumnInspection>> = BTreeMap::new();
    let mut unlisted: BTreeMap<u32, Vec<UnlistedColumn>> = BTreeMap::new();
    for row in transaction.query(
        "WITH RECURSIVE chain(start_oid, oid, depth, not_null, has_default) AS (
             SELECT DISTINCT attribute.atttypid, attribute.atttypid, 0, false, false
               FROM pg_catalog.pg_attribute AS attribute
              WHERE attribute.attrelid = ANY($1::oid[])
                AND attribute.attnum > 0
                AND NOT attribute.attisdropped
             UNION ALL
             SELECT chain.start_oid, domain.typbasetype, chain.depth + 1,
                    chain.not_null OR domain.typnotnull,
                    chain.has_default OR domain.typdefaultbin IS NOT NULL
               FROM chain
               JOIN pg_catalog.pg_type AS domain ON domain.oid = chain.oid
              WHERE domain.typtype = 'd'
         ),
         carried AS (
             SELECT chain.start_oid,
                    CASE WHEN base.typtype = 'e' THEN 'text'
                         WHEN chain.depth > 0 AND base_namespace.nspname = 'pg_catalog'
                         THEN base.typname::text
                    END AS type_name,
                    chain.not_null,
                    chain.has_default
               FROM chain
               JOIN pg_catalog.pg_type AS base ON base.oid = chain.oid
               JOIN pg_catalog.pg_namespace AS base_namespace
                 ON base_namespace.oid = base.typnamespace
              WHERE base.typtype <> 'd'
         )
         SELECT attribute.attrelid::int8,
                attribute.attnum,
                attribute.attname,
                type.oid::int8,
                attribute.atttypmod,
                type_namespace.nspname,
                type.typname,
                NOT attribute.attnotnull,
                key.ordinality::int4,
                attribute.attgenerated <> '',
                NOT COALESCE(column_collation.collisdeterministic, true),
                carried.type_name,
                publication_relation.prattrs IS NULL
                  OR attribute.attnum = ANY(publication_relation.prattrs::int2[]),
                (attribute.attnotnull OR COALESCE(carried.not_null, false))
                  AND NOT attribute.atthasdef
                  AND NOT COALESCE(carried.has_default, false)
                  AND attribute.attidentity = ''
                  AND attribute.attgenerated = ''
           FROM pg_catalog.pg_attribute AS attribute
           JOIN pg_catalog.pg_type AS type ON type.oid = attribute.atttypid
           JOIN pg_catalog.pg_namespace AS type_namespace
             ON type_namespace.oid = type.typnamespace
      LEFT JOIN carried ON carried.start_oid = attribute.atttypid
      LEFT JOIN pg_catalog.pg_collation AS column_collation
             ON column_collation.oid = attribute.attcollation
      LEFT JOIN pg_catalog.pg_publication_rel AS publication_relation
             ON publication_relation.prrelid = attribute.attrelid
            AND publication_relation.prpubid = (
                  SELECT publication.oid FROM pg_catalog.pg_publication AS publication
                   WHERE publication.pubname = $2)
      LEFT JOIN pg_catalog.pg_index AS primary_index
             ON primary_index.indrelid = attribute.attrelid
            AND primary_index.indisprimary
      LEFT JOIN LATERAL unnest(primary_index.indkey)
             WITH ORDINALITY AS key(attnum, ordinality)
             ON key.attnum = attribute.attnum
          WHERE attribute.attrelid = ANY($1::oid[])
            AND attribute.attnum > 0
            AND NOT attribute.attisdropped
       ORDER BY attribute.attrelid, attribute.attnum",
        &[&relation_oids, &publication_name],
    )? {
        let relation_oid = catalog_oid("relation_oid", row.get(0))?;
        let ordinal: Option<i32> = row.get(8);
        let primary_key_ordinal = ordinal
            .map(|value| non_negative_setting("primary_key_ordinal", value))
            .transpose()?;
        if !row.get::<_, bool>(12) {
            unlisted
                .entry(relation_oid)
                .or_default()
                .push(UnlistedColumn {
                    name: row.get(2),
                    primary_key: primary_key_ordinal.is_some(),
                    required_on_insert: row.get(13),
                });
            continue;
        }
        columns
            .entry(relation_oid)
            .or_default()
            .push(ColumnInspection {
                attribute_number: row.get(1),
                name: row.get(2),
                type_oid: catalog_oid("type_oid", row.get(3))?,
                type_modifier: row.get(4),
                type_schema: row.get(5),
                type_name: row.get(6),
                nullable: row.get(7),
                primary_key_ordinal,
                generated: row.get(9),
                nondeterministic_collation: row.get(10),
                carried_as: row.get(11),
            });
    }

    let mut tables = Vec::with_capacity(required_tables.len());
    for (ordinality, table) in (1_i64..).zip(required_tables) {
        let Some(relation) = relations.remove(&ordinality) else {
            tables.push(TableInspection {
                table: table.clone(),
                relation_oid: 0,
                in_publication: false,
                relation_kind: '\0',
                persistence: '\0',
                replica_identity: '\0',
                is_partition: false,
                has_publication_row_filter: false,
                has_publication_column_list: false,
                unlisted_columns: Vec::new(),
                columns: Vec::new(),
            });
            continue;
        };
        let relation_oid = catalog_oid("relation_oid", relation.get(1))?;
        tables.push(TableInspection {
            table: table.clone(),
            relation_oid,
            in_publication: publication_tables.contains(table),
            relation_kind: catalog_character("relkind", relation.get(2))?,
            persistence: catalog_character("relpersistence", relation.get(3))?,
            replica_identity: catalog_character("relreplident", relation.get(4))?,
            is_partition: relation.get(5),
            has_publication_row_filter: relation.get(6),
            has_publication_column_list: relation.get(7),
            columns: columns.remove(&relation_oid).unwrap_or_default(),
            unlisted_columns: unlisted.remove(&relation_oid).unwrap_or_default(),
        });
    }

    transaction.commit()?;
    Ok(EnrollmentSnapshot {
        identity,
        settings,
        publication,
        tables,
    })
}

/// Calque sets these on every connection it opens, so a session that reports
/// anything else was not opened with them.
fn session_setting_refusal(name: &str, found: &str, wanted: &str) -> String {
    format!(
        "this session's {name} is {found:?}, but Calque opens every connection with {name}={wanted}; \
         a connection pooler between Calque and PostgreSQL dropped those startup parameters. \
         Connect directly to PostgreSQL: put the database's own host and port in the database URL, not a pooler's"
    )
}

pub fn validate_enrollment(
    snapshot: &EnrollmentSnapshot,
    requirements: &EnrollmentRequirements,
) -> EnrollmentReport {
    let mut diagnostics = Vec::new();
    let major = snapshot.settings.server_version_num / 10_000;
    if !(MIN_POSTGRES_MAJOR..=MAX_POSTGRES_MAJOR).contains(&major) {
        diagnostics.push(error(
            "POSTGRES_VERSION_UNSUPPORTED",
            format!(
                "PostgreSQL major {major} is outside the supported range {MIN_POSTGRES_MAJOR}-{MAX_POSTGRES_MAJOR}"
            ),
        ));
    }
    if snapshot.settings.server_encoding != "UTF8" {
        diagnostics.push(error(
            "SERVER_ENCODING_UNSUPPORTED",
            format!(
                "server_encoding is {:?}, expected UTF8 for canonical text codecs",
                snapshot.settings.server_encoding
            ),
        ));
    }
    if !snapshot.settings.date_style.starts_with("ISO,") {
        diagnostics.push(error(
            "DATE_STYLE_UNSUPPORTED",
            session_setting_refusal("DateStyle", &snapshot.settings.date_style, "ISO,MDY"),
        ));
    }
    if !matches!(snapshot.settings.time_zone.as_str(), "UTC" | "Etc/UTC") {
        diagnostics.push(error(
            "TIME_ZONE_UNSUPPORTED",
            session_setting_refusal("TimeZone", &snapshot.settings.time_zone, "UTC"),
        ));
    }
    if snapshot.settings.bytea_output != "hex" {
        diagnostics.push(error(
            "BYTEA_OUTPUT_UNSUPPORTED",
            session_setting_refusal("bytea_output", &snapshot.settings.bytea_output, "hex"),
        ));
    }
    if snapshot.settings.wal_level != "logical" {
        diagnostics.push(error(
            "WAL_LEVEL_NOT_LOGICAL",
            format!(
                "wal_level is {:?}, expected logical",
                snapshot.settings.wal_level
            ),
        ));
    }
    if snapshot.settings.max_replication_slots == 0 {
        diagnostics.push(error(
            "NO_REPLICATION_SLOT_CAPACITY",
            "max_replication_slots must be positive".to_owned(),
        ));
    }
    if snapshot.settings.max_wal_senders == 0 {
        diagnostics.push(error(
            "NO_WAL_SENDER_CAPACITY",
            "max_wal_senders must be positive".to_owned(),
        ));
    }
    if snapshot.settings.current_user_is_superuser {
        diagnostics.push(error(
            "SUPERUSER_NOT_LEAST_PRIVILEGE",
            format!(
                "role {:?} is a superuser; enrollment requires a dedicated non-superuser replication role",
                snapshot.settings.current_user
            ),
        ));
    }
    if !snapshot.settings.current_user_has_replication {
        diagnostics.push(error(
            "REPLICATION_PRIVILEGE_REQUIRED",
            format!(
                "role {:?} needs the REPLICATION attribute, or on Amazon RDS and Aurora membership in rds_replication",
                snapshot.settings.current_user
            ),
        ));
    }
    if snapshot.identity != requirements.expected_identity {
        diagnostics.push(error(
            "SOURCE_IDENTITY_MISMATCH",
            format!(
                "observed source {}/{} timeline {} (previous {}) differs from expected {}/{} timeline {} (previous {})",
                snapshot.identity.system_identifier,
                snapshot.identity.database_name,
                snapshot.identity.timeline_id,
                snapshot.identity.previous_timeline_id,
                requirements.expected_identity.system_identifier,
                requirements.expected_identity.database_name,
                requirements.expected_identity.timeline_id,
                requirements.expected_identity.previous_timeline_id,
            ),
        ));
    }
    validate_publication(snapshot, requirements, &mut diagnostics);
    validate_tables(snapshot, requirements, &mut diagnostics);
    EnrollmentReport { diagnostics }
}

fn validate_publication(
    snapshot: &EnrollmentSnapshot,
    requirements: &EnrollmentRequirements,
    diagnostics: &mut Vec<EnrollmentDiagnostic>,
) {
    if snapshot.publication.name != requirements.publication_name || !snapshot.publication.exists {
        diagnostics.push(error(
            "PUBLICATION_MISSING",
            format!(
                "publication {:?} does not exist",
                requirements.publication_name
            ),
        ));
        return;
    }
    let operations = [
        ("INSERT", snapshot.publication.publishes_insert),
        ("UPDATE", snapshot.publication.publishes_update),
        ("DELETE", snapshot.publication.publishes_delete),
        ("TRUNCATE", snapshot.publication.publishes_truncate),
    ];
    for (operation, enabled) in operations {
        if !enabled {
            diagnostics.push(error(
                "PUBLICATION_OPERATION_MISSING",
                format!(
                    "publication {:?} does not publish {operation}",
                    requirements.publication_name
                ),
            ));
        }
    }
    if snapshot.publication.publishes_via_partition_root {
        diagnostics.push(error(
            "PUBLICATION_VIA_PARTITION_ROOT_UNSUPPORTED",
            format!(
                "publication {:?} sets publish_via_partition_root, which Calque does not support; set it to false with ALTER PUBLICATION ... SET (publish_via_partition_root = false)",
                requirements.publication_name
            ),
        ));
    }
}

fn validate_tables(
    snapshot: &EnrollmentSnapshot,
    requirements: &EnrollmentRequirements,
    diagnostics: &mut Vec<EnrollmentDiagnostic>,
) {
    let inspected: BTreeMap<_, _> = snapshot
        .tables
        .iter()
        .map(|table| (table.table.clone(), table))
        .collect();
    for required in &requirements.tables {
        let Some(table) = inspected.get(required) else {
            diagnostics.push(table_error(
                "TABLE_NOT_INSPECTED",
                "required table was absent from the inspection snapshot".to_owned(),
                required,
            ));
            continue;
        };
        if table.relation_kind == '\0' {
            diagnostics.push(table_error(
                "TABLE_MISSING",
                format!("table {} does not exist", required.display_name()),
                required,
            ));
            continue;
        }
        if !table.in_publication {
            diagnostics.push(table_error(
                "TABLE_NOT_IN_PUBLICATION",
                format!(
                    "table {} is not in publication {:?}",
                    required.display_name(),
                    requirements.publication_name
                ),
                required,
            ));
        }
        if table.relation_kind != 'r' || table.is_partition {
            diagnostics.push(table_error(
                "RELATION_KIND_UNSUPPORTED",
                "version 1 currently enrolls only non-partitioned ordinary tables".to_owned(),
                required,
            ));
        }
        if table.persistence != 'p' {
            diagnostics.push(table_error(
                "TABLE_NOT_PERMANENT",
                "only permanent PostgreSQL tables can be synchronized".to_owned(),
                required,
            ));
        }
        let primary_key: Vec<_> = table
            .columns
            .iter()
            .filter(|column| column.primary_key_ordinal.is_some())
            .collect();
        // The rule `authz` enforces again on every policy it compiles: a
        // primary key, and replica identity DEFAULT (which is the primary key)
        // or FULL. A table without a primary key is refused here once, by
        // PRIMARY_KEY_REQUIRED, whatever its replica identity.
        let unlisted_key = table
            .unlisted_columns
            .iter()
            .any(|column| column.primary_key);
        if primary_key.is_empty() && !unlisted_key {
            diagnostics.push(table_error(
                "PRIMARY_KEY_REQUIRED",
                format!(
                    "table {0} has no primary key; add one with ALTER TABLE {0} ADD PRIMARY KEY (...)",
                    required.display_name()
                ),
                required,
            ));
        }
        if table.replica_identity == 'n' {
            diagnostics.push(table_error(
                "REPLICA_IDENTITY_INSUFFICIENT",
                replica_identity_refusal("NOTHING", required),
                required,
            ));
        }
        if table.replica_identity == 'i' {
            diagnostics.push(table_error(
                "CUSTOM_REPLICA_IDENTITY_UNSUPPORTED",
                replica_identity_refusal("USING INDEX", required),
                required,
            ));
        }
        if table.has_publication_row_filter {
            diagnostics.push(table_error(
                "PUBLICATION_ROW_FILTER_UNSUPPORTED",
                "the publication filters this table's rows with a WHERE clause, and Calque serves only complete tables; publish the table without one"
                    .to_owned(),
                required,
            ));
        }
        for column in &table.unlisted_columns {
            if column.primary_key {
                diagnostics.push(column_error(
                    "PUBLICATION_COLUMN_LIST_OMITS_KEY",
                    "the publication's column list leaves out this primary-key column, and Calque identifies every row by its key; add the column to the list"
                        .to_owned(),
                    required,
                    &column.name,
                ));
            }
        }
        if table.has_publication_column_list && table.replica_identity == 'f' {
            diagnostics.push(table_error(
                "PUBLICATION_COLUMN_LIST_WITH_FULL_IDENTITY",
                format!(
                    "the table's replica identity is FULL, and PostgreSQL refuses its updates and deletes while a publication lists its columns, even every one of them; publish {0} without a column list, or run ALTER TABLE {0} REPLICA IDENTITY DEFAULT",
                    required.display_name()
                ),
                required,
            ));
        }
        for column in &table.columns {
            let codec = column.codec();
            if codec.is_none() {
                let declared = match &column.carried_as {
                    Some(base) => format!(
                        "{}.{}, a domain over pg_catalog.{base},",
                        column.type_schema, column.type_name
                    ),
                    None => format!("{}.{}", column.type_schema, column.type_name),
                };
                diagnostics.push(column_error(
                    "TYPE_CODEC_UNSUPPORTED",
                    format!(
                        "column type {declared} is not supported; supported types are pg_catalog {}, enums, and domains over any of them; leave the column out of the publication's column list to enroll the table without it",
                        PostgresTypeCodec::ALL
                            .map(PostgresTypeCodec::catalog_name)
                            .join(", ")
                    ),
                    required,
                    &column.name,
                ));
            }
            if column.generated {
                diagnostics.push(column_error(
                    "GENERATED_COLUMN_UNSUPPORTED",
                    "generated columns are not supported, because logical replication does not carry their values by default; leave the column out of the publication's column list, replace it with an ordinary one, or compute the value in the application".to_owned(),
                    required,
                    &column.name,
                ));
            }
            if column.primary_key_ordinal.is_some() && column.nullable {
                diagnostics.push(column_error(
                    "NULLABLE_PRIMARY_KEY_INVALID",
                    "primary key columns must be non-null".to_owned(),
                    required,
                    &column.name,
                ));
            }
            if column.primary_key_ordinal.is_some()
                && !codec.is_some_and(PostgresTypeCodec::is_primary_key_safe)
            {
                diagnostics.push(column_error(
                    "PRIMARY_KEY_CODEC_UNSUPPORTED",
                    format!(
                        "version-1 primary keys support only pg_catalog bool, int2, int4, int8, text, varchar, uuid, and bytea, enums, and domains over any of them; {}.{} does not yet have proven equality semantics across PostgreSQL and JavaScript",
                        column.type_schema, column.type_name
                    ),
                    required,
                    &column.name,
                ));
            }
            if column.primary_key_ordinal.is_some() && column.nondeterministic_collation {
                diagnostics.push(column_error(
                    "PRIMARY_KEY_COLLATION_NONDETERMINISTIC",
                    "primary-key columns need a deterministic collation: under a nondeterministic one PostgreSQL treats distinct byte strings as the same key, and JavaScript does not".to_owned(),
                    required,
                    &column.name,
                ));
            }
        }
    }
}

fn replica_identity_refusal(identity: &str, table: &QualifiedTable) -> String {
    format!(
        "replica identity {identity} is not supported; use DEFAULT, which is the primary key, or FULL: ALTER TABLE {} REPLICA IDENTITY DEFAULT",
        table.display_name()
    )
}

fn error(code: &'static str, message: String) -> EnrollmentDiagnostic {
    EnrollmentDiagnostic {
        severity: DiagnosticSeverity::Error,
        code: code.to_owned(),
        message,
        table: None,
        column: None,
    }
}

fn table_error(
    code: &'static str,
    message: String,
    table: &QualifiedTable,
) -> EnrollmentDiagnostic {
    EnrollmentDiagnostic {
        severity: DiagnosticSeverity::Error,
        code: code.to_owned(),
        message,
        table: Some(table.clone()),
        column: None,
    }
}

fn column_error(
    code: &'static str,
    message: String,
    table: &QualifiedTable,
    column: &str,
) -> EnrollmentDiagnostic {
    EnrollmentDiagnostic {
        severity: DiagnosticSeverity::Error,
        code: code.to_owned(),
        message,
        table: Some(table.clone()),
        column: Some(column.to_owned()),
    }
}

fn non_negative_setting(name: &'static str, value: i32) -> Result<u32, InspectError> {
    value
        .try_into()
        .map_err(|_| InspectError::InvalidSetting { name, value })
}

fn catalog_oid(name: &'static str, value: i64) -> Result<u32, InspectError> {
    value
        .try_into()
        .map_err(|_| InspectError::InvalidCatalogOid { name, value })
}

fn catalog_character(field: &'static str, value: String) -> Result<char, InspectError> {
    let mut characters = value.chars();
    let character = characters
        .next()
        .ok_or_else(|| InspectError::InvalidCatalogCharacter {
            field,
            value: value.clone(),
        })?;
    if characters.next().is_some() {
        return Err(InspectError::InvalidCatalogCharacter { field, value });
    }
    Ok(character)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn table() -> QualifiedTable {
        QualifiedTable {
            schema: "public".to_owned(),
            table: "tasks".to_owned(),
        }
    }

    fn identity() -> SourceIdentity {
        SourceIdentity {
            system_identifier: "7612345678901234567".to_owned(),
            database_name: "app".to_owned(),
            timeline_id: 1,
            previous_timeline_id: 1,
        }
    }

    fn snapshot() -> EnrollmentSnapshot {
        EnrollmentSnapshot {
            identity: identity(),
            settings: SourceSettings {
                server_version_num: 180_004,
                server_encoding: "UTF8".to_owned(),
                date_style: "ISO, MDY".to_owned(),
                time_zone: "UTC".to_owned(),
                bytea_output: "hex".to_owned(),
                wal_level: "logical".to_owned(),
                max_replication_slots: 10,
                max_wal_senders: 10,
                current_user: "calque".to_owned(),
                current_user_is_superuser: false,
                current_user_has_replication: true,
            },
            publication: PublicationSettings {
                name: "calque".to_owned(),
                exists: true,
                publishes_insert: true,
                publishes_update: true,
                publishes_delete: true,
                publishes_truncate: true,
                publishes_via_partition_root: false,
            },
            tables: vec![TableInspection {
                table: table(),
                relation_oid: 16_384,
                in_publication: true,
                relation_kind: 'r',
                persistence: 'p',
                replica_identity: 'd',
                is_partition: false,
                has_publication_row_filter: false,
                has_publication_column_list: false,
                unlisted_columns: Vec::new(),
                columns: vec![ColumnInspection {
                    attribute_number: 1,
                    name: "id".to_owned(),
                    type_oid: 2_950,
                    type_modifier: -1,
                    type_schema: "pg_catalog".to_owned(),
                    type_name: "uuid".to_owned(),
                    nullable: false,
                    primary_key_ordinal: Some(1),
                    generated: false,
                    nondeterministic_collation: false,
                    carried_as: None,
                }],
            }],
        }
    }

    fn requirements() -> EnrollmentRequirements {
        EnrollmentRequirements {
            expected_identity: identity(),
            publication_name: "calque".to_owned(),
            tables: BTreeSet::from([table()]),
            pgoutput_profile: PgoutputLaunchProfile::V1,
        }
    }

    #[test]
    fn ready_source_has_no_diagnostics() {
        let report = validate_enrollment(&snapshot(), &requirements());
        assert!(report.is_ready());
        assert!(report.diagnostics.is_empty());

        let mut container_default = snapshot();
        container_default.settings.time_zone = "Etc/UTC".to_owned();
        assert!(validate_enrollment(&container_default, &requirements()).is_ready());
    }

    #[test]
    fn a_not_ready_report_names_every_blocker_it_found() {
        let mut snapshot = snapshot();
        snapshot.settings.server_version_num = 160_000;
        snapshot.settings.current_user_is_superuser = true;
        snapshot.tables[0].columns[0].primary_key_ordinal = None;
        let report = validate_enrollment(&snapshot, &requirements());

        assert!(!report.is_ready());
        let rendered = report.to_string();
        assert!(
            rendered.contains("POSTGRES_VERSION_UNSUPPORTED: PostgreSQL major 16"),
            "{rendered}"
        );
        assert!(
            rendered.contains("SUPERUSER_NOT_LEAST_PRIVILEGE"),
            "{rendered}"
        );
        assert!(rendered.contains("[public.tasks]"), "{rendered}");
        // A message may itself contain "; ", so the codes are checked instead
        // of the separators.
        for diagnostic in report.blocking() {
            assert!(rendered.contains(diagnostic.code.as_str()), "{rendered}");
        }
        // The missing primary key is one blocker, not a second one as an
        // insufficient replica identity.
        let codes: Vec<_> = report
            .blocking()
            .map(|diagnostic| diagnostic.code.as_str())
            .collect();
        assert_eq!(
            codes.iter().copied().collect::<BTreeSet<_>>(),
            BTreeSet::from([
                "POSTGRES_VERSION_UNSUPPORTED",
                "PRIMARY_KEY_REQUIRED",
                "SUPERUSER_NOT_LEAST_PRIVILEGE",
            ]),
            "{rendered}"
        );
        assert_eq!(codes.len(), 3, "{rendered}");
    }

    #[test]
    fn a_ready_report_renders_as_having_no_blockers() {
        assert_eq!(
            validate_enrollment(&snapshot(), &requirements()).to_string(),
            "no blocking diagnostics"
        );
    }

    #[test]
    fn version_wal_privilege_and_source_identity_fail_closed() {
        let mut snapshot = snapshot();
        snapshot.settings.server_version_num = 190_000;
        snapshot.settings.server_encoding = "LATIN1".to_owned();
        snapshot.settings.date_style = "SQL, DMY".to_owned();
        snapshot.settings.time_zone = "America/Sao_Paulo".to_owned();
        snapshot.settings.bytea_output = "escape".to_owned();
        snapshot.settings.wal_level = "replica".to_owned();
        snapshot.settings.current_user_has_replication = false;
        snapshot.settings.current_user_is_superuser = true;
        snapshot.identity.system_identifier = "restored-cluster".to_owned();
        let report = validate_enrollment(&snapshot, &requirements());
        let codes: BTreeSet<_> = report
            .diagnostics
            .iter()
            .map(|item| item.code.as_str())
            .collect();

        assert!(!report.is_ready());
        assert!(codes.contains("POSTGRES_VERSION_UNSUPPORTED"));
        assert!(codes.contains("SERVER_ENCODING_UNSUPPORTED"));
        assert!(codes.contains("DATE_STYLE_UNSUPPORTED"));
        assert!(codes.contains("TIME_ZONE_UNSUPPORTED"));
        assert!(codes.contains("BYTEA_OUTPUT_UNSUPPORTED"));
        assert!(codes.contains("WAL_LEVEL_NOT_LOGICAL"));
        assert!(codes.contains("REPLICATION_PRIVILEGE_REQUIRED"));
        assert!(codes.contains("SUPERUSER_NOT_LEAST_PRIVILEGE"));
        assert!(codes.contains("SOURCE_IDENTITY_MISMATCH"));
    }

    #[test]
    fn a_session_setting_refusal_blames_a_pooler_and_says_to_connect_directly() {
        let mut snapshot = snapshot();
        snapshot.settings.date_style = "SQL, DMY".to_owned();
        snapshot.settings.time_zone = "America/Sao_Paulo".to_owned();
        snapshot.settings.bytea_output = "escape".to_owned();
        let report = validate_enrollment(&snapshot, &requirements());
        for (code, name, found) in [
            ("DATE_STYLE_UNSUPPORTED", "DateStyle=ISO,MDY", "SQL, DMY"),
            ("TIME_ZONE_UNSUPPORTED", "TimeZone=UTC", "America/Sao_Paulo"),
            ("BYTEA_OUTPUT_UNSUPPORTED", "bytea_output=hex", "escape"),
        ] {
            let message = &report
                .diagnostics
                .iter()
                .find(|item| item.code == code)
                .unwrap()
                .message;
            assert!(message.contains(name), "{message}");
            assert!(message.contains(found), "{message}");
            assert!(message.contains("connection pooler"), "{message}");
            assert!(message.contains("Connect directly"), "{message}");
        }
    }

    #[test]
    fn superuser_is_rejected_even_with_replication_privilege() {
        let mut snapshot = snapshot();
        snapshot.settings.current_user_is_superuser = true;
        snapshot.settings.current_user_has_replication = true;
        let report = validate_enrollment(&snapshot, &requirements());
        let codes: BTreeSet<_> = report
            .diagnostics
            .iter()
            .map(|item| item.code.as_str())
            .collect();
        assert!(!report.is_ready());
        assert!(codes.contains("SUPERUSER_NOT_LEAST_PRIVILEGE"));
        assert!(!codes.contains("REPLICATION_PRIVILEGE_REQUIRED"));
    }

    #[test]
    fn publication_must_include_every_change_kind_and_table() {
        let mut snapshot = snapshot();
        snapshot.publication.publishes_truncate = false;
        snapshot.tables[0].in_publication = false;
        let report = validate_enrollment(&snapshot, &requirements());
        let codes: BTreeSet<_> = report
            .diagnostics
            .iter()
            .map(|item| item.code.as_str())
            .collect();

        assert!(codes.contains("PUBLICATION_OPERATION_MISSING"));
        assert!(codes.contains("TABLE_NOT_IN_PUBLICATION"));
    }

    #[test]
    fn stable_primary_key_and_replica_identity_are_required() {
        let mut snapshot = snapshot();
        snapshot.tables[0].replica_identity = 'n';
        snapshot.tables[0].columns[0].primary_key_ordinal = None;
        let report = validate_enrollment(&snapshot, &requirements());
        let codes: BTreeSet<_> = report
            .diagnostics
            .iter()
            .map(|item| item.code.as_str())
            .collect();

        assert!(codes.contains("PRIMARY_KEY_REQUIRED"));
        assert!(codes.contains("REPLICA_IDENTITY_INSUFFICIENT"));
    }

    /// Enrollment admits exactly what `authz` compiles a policy for: a primary
    /// key, and replica identity DEFAULT or FULL. Every other combination is
    /// refused by the diagnostics that name its fix, and by no others.
    #[test]
    fn replica_identity_and_primary_key_follow_the_authorization_rule() {
        for identity in ['d', 'f', 'n', 'i'] {
            for has_primary_key in [true, false] {
                let mut snapshot = snapshot();
                snapshot.tables[0].replica_identity = identity;
                if !has_primary_key {
                    snapshot.tables[0].columns[0].primary_key_ordinal = None;
                }
                let report = validate_enrollment(&snapshot, &requirements());
                let codes: Vec<_> = report
                    .blocking()
                    .map(|diagnostic| diagnostic.code.as_str())
                    .collect();
                let mut expected = Vec::new();
                if !has_primary_key {
                    expected.push("PRIMARY_KEY_REQUIRED");
                }
                match identity {
                    'n' => expected.push("REPLICA_IDENTITY_INSUFFICIENT"),
                    'i' => expected.push("CUSTOM_REPLICA_IDENTITY_UNSUPPORTED"),
                    _ => {}
                }
                assert_eq!(
                    codes, expected,
                    "replica identity {identity:?}, primary key {has_primary_key}"
                );
            }
        }
    }

    #[test]
    fn unsupported_and_generated_columns_are_enrollment_errors() {
        let mut snapshot = snapshot();
        snapshot.tables[0].columns.push(ColumnInspection {
            attribute_number: 2,
            name: "location".to_owned(),
            type_oid: 99_999,
            type_modifier: -1,
            type_schema: "public".to_owned(),
            type_name: "geography".to_owned(),
            nullable: true,
            primary_key_ordinal: None,
            generated: true,
            nondeterministic_collation: false,
            carried_as: None,
        });
        let report = validate_enrollment(&snapshot, &requirements());
        let codes: BTreeSet<_> = report
            .diagnostics
            .iter()
            .map(|item| item.code.as_str())
            .collect();

        assert!(codes.contains("TYPE_CODEC_UNSUPPORTED"));
        assert!(codes.contains("GENERATED_COLUMN_UNSUPPORTED"));
    }

    /// Each table and column refusal names the change that clears it. The
    /// messages are pinned whole, because a fragment would still pass a
    /// message that lost its remedy.
    #[test]
    fn each_table_and_column_refusal_names_its_fix() {
        let message = |snapshot: &EnrollmentSnapshot, code: &str| -> String {
            let report = validate_enrollment(snapshot, &requirements());
            let raised = report
                .blocking()
                .find(|diagnostic| diagnostic.code == code)
                .unwrap_or_else(|| panic!("{code} was not raised: {report}"))
                .message
                .clone();
            raised
        };

        let mut without_key = snapshot();
        without_key.tables[0].columns[0].primary_key_ordinal = None;
        assert_eq!(
            message(&without_key, "PRIMARY_KEY_REQUIRED"),
            "table public.tasks has no primary key; \
             add one with ALTER TABLE public.tasks ADD PRIMARY KEY (...)"
        );

        for (identity, code, named) in [
            ('n', "REPLICA_IDENTITY_INSUFFICIENT", "NOTHING"),
            ('i', "CUSTOM_REPLICA_IDENTITY_UNSUPPORTED", "USING INDEX"),
        ] {
            let mut refused = snapshot();
            refused.tables[0].replica_identity = identity;
            assert_eq!(
                message(&refused, code),
                format!(
                    "replica identity {named} is not supported; \
                     use DEFAULT, which is the primary key, or FULL: \
                     ALTER TABLE public.tasks REPLICA IDENTITY DEFAULT"
                )
            );
        }

        let mut columns = snapshot();
        columns.tables[0].columns.push(ColumnInspection {
            attribute_number: 2,
            name: "location".to_owned(),
            type_oid: 99_999,
            type_modifier: -1,
            type_schema: "public".to_owned(),
            type_name: "geography".to_owned(),
            nullable: true,
            primary_key_ordinal: None,
            generated: true,
            nondeterministic_collation: false,
            carried_as: None,
        });
        assert_eq!(
            message(&columns, "TYPE_CODEC_UNSUPPORTED"),
            "column type public.geography is not supported; supported types are pg_catalog \
             bool, int2, int4, int8, float4, float8, numeric, text, varchar, bpchar, bytea, \
             uuid, json, jsonb, date, time, timestamp, timestamptz, enums, and domains over \
             any of them; leave the column out of the publication's column list to enroll \
             the table without it"
        );
        assert_eq!(
            message(&columns, "GENERATED_COLUMN_UNSUPPORTED"),
            "generated columns are not supported, because logical replication does not \
             carry their values by default; leave the column out of the publication's \
             column list, replace it with an ordinary one, or compute the value in the \
             application"
        );

        let mut span = snapshot();
        span.tables[0].columns.push(ColumnInspection {
            type_schema: "public".to_owned(),
            type_name: "span".to_owned(),
            carried_as: Some("interval".to_owned()),
            ..text_column(2, "span")
        });
        assert!(message(&span, "TYPE_CODEC_UNSUPPORTED").starts_with(
            "column type public.span, a domain over pg_catalog.interval, is not supported;"
        ));

        let mut hidden_key = snapshot();
        let key = hidden_key.tables[0].columns.remove(0);
        hidden_key.tables[0].columns.push(text_column(2, "title"));
        hidden_key.tables[0].unlisted_columns.push(UnlistedColumn {
            name: key.name,
            primary_key: true,
            required_on_insert: true,
        });
        assert_eq!(
            message(&hidden_key, "PUBLICATION_COLUMN_LIST_OMITS_KEY"),
            "the publication's column list leaves out this primary-key column, and Calque \
             identifies every row by its key; add the column to the list"
        );

        let mut full = snapshot();
        full.tables[0].replica_identity = 'f';
        full.tables[0].has_publication_column_list = true;
        assert_eq!(
            message(&full, "PUBLICATION_COLUMN_LIST_WITH_FULL_IDENTITY"),
            "the table's replica identity is FULL, and PostgreSQL refuses its updates and \
             deletes while a publication lists its columns, even every one of them; publish \
             public.tasks without a column list, or run ALTER TABLE public.tasks REPLICA \
             IDENTITY DEFAULT"
        );
    }

    fn text_column(attribute_number: i16, name: &str) -> ColumnInspection {
        ColumnInspection {
            attribute_number,
            name: name.to_owned(),
            type_oid: 25,
            type_modifier: -1,
            type_schema: "pg_catalog".to_owned(),
            type_name: "text".to_owned(),
            nullable: true,
            primary_key_ordinal: None,
            generated: false,
            nondeterministic_collation: false,
            carried_as: None,
        }
    }

    fn unlisted(name: &str, required_on_insert: bool) -> UnlistedColumn {
        UnlistedColumn {
            name: name.to_owned(),
            primary_key: false,
            required_on_insert,
        }
    }

    /// An enum travels as its label's text and a domain as its base type, so
    /// each enrolls wherever that type does, the key included.
    #[test]
    fn enums_and_domains_enroll_as_the_type_they_travel_as() {
        let mut snapshot = snapshot();
        let key = &mut snapshot.tables[0].columns[0];
        key.type_schema = "public".to_owned();
        key.type_name = "task_id".to_owned();
        key.carried_as = Some("uuid".to_owned());
        snapshot.tables[0].columns.push(ColumnInspection {
            type_oid: 70_001,
            type_schema: "public".to_owned(),
            type_name: "mood".to_owned(),
            carried_as: Some("text".to_owned()),
            ..text_column(2, "mood")
        });
        assert!(
            validate_enrollment(&snapshot, &requirements()).is_ready(),
            "{}",
            validate_enrollment(&snapshot, &requirements())
        );
        assert_eq!(
            snapshot.tables[0].columns[0].codec(),
            Some(PostgresTypeCodec::Uuid)
        );
        assert_eq!(
            snapshot.tables[0].columns[1].codec(),
            Some(PostgresTypeCodec::Text)
        );
        assert_eq!(snapshot.tables[0].columns[1].carried_type_name(), "text");

        // A key whose domain sits over a type keys cannot use stays refused.
        snapshot.tables[0].columns[0].carried_as = Some("float8".to_owned());
        let codes: Vec<_> = validate_enrollment(&snapshot, &requirements())
            .diagnostics
            .into_iter()
            .map(|diagnostic| diagnostic.code)
            .collect();
        assert_eq!(codes, ["PRIMARY_KEY_CODEC_UNSUPPORTED"]);
    }

    /// A column the publication's column list leaves out is not carried, so a
    /// generated or uncarried type there no longer blocks the table. The key
    /// must stay listed, and a table under REPLICA IDENTITY FULL takes no list,
    /// even one naming every column.
    #[test]
    fn a_column_list_enrolls_the_listed_columns_and_keeps_the_key() {
        let mut snapshot = snapshot();
        snapshot.tables[0].has_publication_column_list = true;
        snapshot.tables[0]
            .unlisted_columns
            .extend([unlisted("location", false), unlisted("secret", true)]);
        let report = validate_enrollment(&snapshot, &requirements());
        assert!(report.is_ready(), "{report}");
        assert!(snapshot.tables[0].insert_needs_unlisted_column());

        snapshot.tables[0]
            .unlisted_columns
            .retain(|column| !column.required_on_insert);
        assert!(!snapshot.tables[0].insert_needs_unlisted_column());

        let mut full = snapshot.clone();
        full.tables[0].replica_identity = 'f';
        let codes: Vec<_> = validate_enrollment(&full, &requirements())
            .diagnostics
            .into_iter()
            .map(|diagnostic| diagnostic.code)
            .collect();
        assert_eq!(codes, ["PUBLICATION_COLUMN_LIST_WITH_FULL_IDENTITY"]);
        full.tables[0].unlisted_columns.clear();
        let codes: Vec<_> = validate_enrollment(&full, &requirements())
            .diagnostics
            .into_iter()
            .map(|diagnostic| diagnostic.code)
            .collect();
        assert_eq!(codes, ["PUBLICATION_COLUMN_LIST_WITH_FULL_IDENTITY"]);

        // With the key unlisted, the table reports the list, not a missing key.
        let mut hidden_key = snapshot;
        hidden_key.tables[0].columns[0].primary_key_ordinal = None;
        hidden_key.tables[0].unlisted_columns.push(UnlistedColumn {
            primary_key: true,
            ..unlisted("id", true)
        });
        let codes: Vec<_> = validate_enrollment(&hidden_key, &requirements())
            .diagnostics
            .into_iter()
            .map(|diagnostic| diagnostic.code)
            .collect();
        assert_eq!(codes, ["PUBLICATION_COLUMN_LIST_OMITS_KEY"]);
    }

    /// The schema fingerprint hashes serialized inspections, so a built-in
    /// column and a table published whole serialize without the new fields.
    #[test]
    fn carried_types_and_unlisted_columns_serialize_only_when_present() {
        let table = snapshot().tables[0].clone();
        let json = serde_json::to_value(&table).unwrap();
        assert!(json.get("unlistedColumns").is_none(), "{json}");
        assert!(json["columns"][0].get("carriedAs").is_none(), "{json}");
        assert_eq!(
            serde_json::from_value::<TableInspection>(json).unwrap(),
            table
        );
    }

    #[test]
    fn primary_keys_without_proven_cross_runtime_equality_fail_enrollment() {
        let mut snapshot = snapshot();
        snapshot.tables[0].columns[0].type_name = "float8".to_owned();
        let report = validate_enrollment(&snapshot, &requirements());
        assert!(!report.is_ready());
        assert!(report
            .diagnostics
            .iter()
            .any(|diagnostic| diagnostic.code == "PRIMARY_KEY_CODEC_UNSUPPORTED"));

        for unsupported in ["numeric", "bpchar", "timestamp"] {
            snapshot.tables[0].columns[0].type_name = unsupported.to_owned();
            let report = validate_enrollment(&snapshot, &requirements());
            assert!(report
                .diagnostics
                .iter()
                .any(|diagnostic| diagnostic.code == "PRIMARY_KEY_CODEC_UNSUPPORTED"));
        }
    }

    /// The schema fingerprint hashes serialized inspections, so a column
    /// under a deterministic collation must serialize without the field.
    #[test]
    fn collation_determinism_serializes_only_when_nondeterministic() {
        let mut column = snapshot().tables[0].columns[0].clone();
        let json = serde_json::to_value(&column).unwrap();
        assert!(json.get("nondeterministicCollation").is_none(), "{json}");
        assert_eq!(
            serde_json::from_value::<ColumnInspection>(json).unwrap(),
            column
        );
        column.nondeterministic_collation = true;
        let json = serde_json::to_value(&column).unwrap();
        assert_eq!(json["nondeterministicCollation"], true);
    }

    #[test]
    fn text_primary_keys_enroll_only_under_a_deterministic_collation() {
        for text in ["text", "varchar"] {
            let mut snapshot = snapshot();
            snapshot.tables[0].columns[0].type_schema = "pg_catalog".to_owned();
            snapshot.tables[0].columns[0].type_name = text.to_owned();
            let report = validate_enrollment(&snapshot, &requirements());
            assert!(report.is_ready(), "{text}: {:?}", report.diagnostics);

            snapshot.tables[0].columns[0].nondeterministic_collation = true;
            let report = validate_enrollment(&snapshot, &requirements());
            let codes: Vec<_> = report
                .diagnostics
                .iter()
                .map(|diagnostic| diagnostic.code.as_str())
                .collect();
            assert_eq!(codes, ["PRIMARY_KEY_COLLATION_NONDETERMINISTIC"], "{text}");
        }

        // A nondeterministic collation outside the key admits the table.
        let mut snapshot = snapshot();
        snapshot.tables[0].columns.push(ColumnInspection {
            attribute_number: 2,
            name: "title".to_owned(),
            type_oid: 25,
            type_modifier: -1,
            type_schema: "pg_catalog".to_owned(),
            type_name: "text".to_owned(),
            nullable: true,
            primary_key_ordinal: None,
            generated: false,
            nondeterministic_collation: true,
            carried_as: None,
        });
        assert!(validate_enrollment(&snapshot, &requirements()).is_ready());
    }

    #[test]
    fn unmodeled_publication_and_identity_features_fail_closed() {
        let mut snapshot = snapshot();
        snapshot.publication.publishes_via_partition_root = true;
        snapshot.tables[0].is_partition = true;
        snapshot.tables[0].replica_identity = 'i';
        snapshot.tables[0].has_publication_row_filter = true;
        let report = validate_enrollment(&snapshot, &requirements());
        let codes: BTreeSet<_> = report
            .diagnostics
            .iter()
            .map(|item| item.code.as_str())
            .collect();

        assert!(codes.contains("PUBLICATION_VIA_PARTITION_ROOT_UNSUPPORTED"));
        assert!(codes.contains("RELATION_KIND_UNSUPPORTED"));
        assert!(codes.contains("CUSTOM_REPLICA_IDENTITY_UNSUPPORTED"));
        assert!(codes.contains("PUBLICATION_ROW_FILTER_UNSUPPORTED"));
    }

    /// Each publication refusal names what to change, not only what is wrong.
    #[test]
    fn publication_refusals_say_what_to_change() {
        let mut snapshot = snapshot();
        snapshot.publication.publishes_via_partition_root = true;
        snapshot.tables[0].has_publication_row_filter = true;
        let rendered = validate_enrollment(&snapshot, &requirements()).to_string();

        for remedy in [
            format!(
                "publication {:?} sets publish_via_partition_root",
                requirements().publication_name
            ),
            "SET (publish_via_partition_root = false)".to_owned(),
            "publish the table without one".to_owned(),
        ] {
            assert!(rendered.contains(&remedy), "{rendered}");
        }
    }

    #[test]
    fn a_slot_name_is_lowercase_ascii_digits_and_underscore_up_to_63_bytes() {
        for name in ["calque_slot", "snapshot_1", "_", &"a".repeat(63)] {
            assert!(is_valid_slot_name(name), "{name}");
        }
        for name in [
            "",
            "Calque_slot",
            "bad-slot",
            "slot.name",
            "slot\u{e9}",
            &"a".repeat(64),
        ] {
            assert!(!is_valid_slot_name(name), "{name}");
        }
    }

    #[test]
    fn a_quoted_identifier_doubles_each_quote() {
        assert_eq!(quote_identifier("tasks"), "\"tasks\"");
        assert_eq!(quote_identifier("we\"ird"), "\"we\"\"ird\"");
    }
}
