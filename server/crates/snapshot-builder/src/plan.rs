use std::collections::{BTreeMap, BTreeSet};

use artifact_contract::{ArtifactColumn, ArtifactTable};
use authz::{AuthorizationIdentity, AuthorizedScope, PartitionValue, TablePermit};
use postgres_types::Type;
use sha2::{Digest, Sha256};
use snapshot_coordinator::CopyRead;
use source_control::{
    quote_identifier, PostgresTypeCodec, QualifiedTable, RegisteredTable, SchemaRegistry,
    SchemaRegistryBinding,
};
use sync_types::{ColumnId, TableId};
use thiserror::Error;

const PLAN_FORMAT_VERSION: u32 = 1;
const PRIMARY_KEY_CODEC: &str = "protobuf-primary-key-v1";

#[derive(Debug, Clone)]
pub struct CopyColumnPlan {
    pub codec: PostgresTypeCodec,
    pub postgres_type: Type,
    pub cast_to_text: bool,
}

#[derive(Debug, Clone)]
pub struct SnapshotColumnPlan {
    pub column_id: ColumnId,
    pub source_name: String,
    pub local_name: String,
    pub nullable: bool,
    pub copy: CopyColumnPlan,
}

#[derive(Debug, Clone)]
pub struct SnapshotTablePlan {
    pub(crate) table_id: TableId,
    pub(crate) source_schema: String,
    pub(crate) source_table: String,
    pub(crate) columns: Vec<SnapshotColumnPlan>,
    pub(crate) primary_key: Vec<ColumnId>,
    copy_sql: String,
    copy_read: CopyRead,
    schema_hash: String,
    column_profile_hash: String,
}

impl SnapshotTablePlan {
    pub fn table_id(&self) -> &TableId {
        &self.table_id
    }

    pub fn source_schema(&self) -> &str {
        &self.source_schema
    }

    pub fn source_table(&self) -> &str {
        &self.source_table
    }

    pub fn columns(&self) -> &[SnapshotColumnPlan] {
        &self.columns
    }

    pub fn primary_key(&self) -> &[ColumnId] {
        &self.primary_key
    }

    pub fn copy_sql(&self) -> &str {
        &self.copy_sql
    }

    pub fn copy_read(&self) -> CopyRead {
        self.copy_read
    }

    pub fn copy_types(&self) -> Vec<Type> {
        self.columns
            .iter()
            .map(|column| column.copy.postgres_type.clone())
            .collect()
    }

    /// This table's manifest entry. The local table is the collection the
    /// bytes are addressed by, and the columns are the change stream's ids so
    /// a plan can hand the client its change contract.
    pub fn artifact_table(&self, row_count: u64) -> ArtifactTable {
        ArtifactTable {
            table_id: self.table_id.clone(),
            source_schema: self.source_schema.clone(),
            source_table: self.source_table.clone(),
            // A required manifest field that every stored manifest carries;
            // the catalog compares a resumed build's re-serialized manifest
            // with the stored bytes, so the value cannot change.
            local_schema: "main".to_owned(),
            local_table: self.source_table.clone(),
            schema_hash: self.schema_hash.clone(),
            column_profile_hash: self.column_profile_hash.clone(),
            row_count,
            primary_key_codec: PRIMARY_KEY_CODEC.to_owned(),
            columns: self
                .columns
                .iter()
                .map(|column| ArtifactColumn {
                    column_id: column.column_id.get(),
                    name: column.source_name.clone(),
                })
                .collect(),
        }
    }
}

#[cfg(test)]
impl SnapshotTablePlan {
    /// A plan for in-crate unit tests that never reach PostgreSQL.
    pub(crate) fn for_tests(
        table_id: TableId,
        source_table: &str,
        columns: Vec<SnapshotColumnPlan>,
        primary_key: Vec<ColumnId>,
    ) -> Self {
        let hash = format!("sha256:{}", "0".repeat(64));
        Self {
            table_id,
            source_schema: "public".to_owned(),
            source_table: source_table.to_owned(),
            columns,
            primary_key,
            copy_sql: String::new(),
            copy_read: CopyRead::Planned,
            schema_hash: hash.clone(),
            column_profile_hash: hash,
        }
    }
}

#[derive(Debug)]
pub struct SnapshotBuildPlan {
    pub(crate) schema_generation: u64,
    pub(crate) tables: Vec<SnapshotTablePlan>,
    authorized_scope: AuthorizedScope,
    authorization_identity: AuthorizationIdentity,
    registry_binding: SchemaRegistryBinding,
    table_set_hash: String,
}

impl SnapshotBuildPlan {
    pub fn from_registry(
        registry: &SchemaRegistry,
        authorized_scope: &AuthorizedScope,
    ) -> Result<Self, SnapshotPlanError> {
        if authorized_scope.registry_binding() != Some(&registry.binding()) {
            return Err(SnapshotPlanError::AuthorizationRegistryMismatch);
        }
        if registry.tables().is_empty() {
            return Err(SnapshotPlanError::EmptyTableSet);
        }
        let mut tables = Vec::with_capacity(authorized_scope.permitted_tables().len());
        let mut served = Vec::with_capacity(tables.capacity());
        for table in registry.tables() {
            let Some(permit) = authorized_scope.table_permit(&table.table_id) else {
                continue;
            };
            tables.push(compile_table(
                table,
                permit,
                authorized_scope.partition_filter(&table.table_id),
            )?);
            served.push(&table.inspection.table);
        }
        if let Some((first, second)) = QualifiedTable::first_shared_collection_name(served) {
            return Err(SnapshotPlanError::SharedCollectionName {
                first: first.display_name(),
                second: second.display_name(),
                collection: second.table.clone(),
            });
        }
        if tables.len() != authorized_scope.permitted_tables().len() {
            return Err(SnapshotPlanError::AuthorizationRegistryMismatch);
        }
        if tables.is_empty() {
            return Err(SnapshotPlanError::EmptyAuthorizedTableSet);
        }
        let table_set_hash = table_set_digest(registry.schema_generation(), &tables);
        Ok(Self {
            schema_generation: registry.schema_generation(),
            tables,
            authorized_scope: authorized_scope.clone(),
            authorization_identity: authorized_scope.identity(),
            registry_binding: registry.binding(),
            table_set_hash,
        })
    }

    pub fn registry_binding(&self) -> &SchemaRegistryBinding {
        &self.registry_binding
    }

    pub const fn schema_generation(&self) -> u64 {
        self.schema_generation
    }

    pub fn tables(&self) -> &[SnapshotTablePlan] {
        &self.tables
    }

    pub fn table_set_hash(&self) -> &str {
        &self.table_set_hash
    }

    pub const fn authorization_identity(&self) -> AuthorizationIdentity {
        self.authorization_identity
    }

    pub fn authorized_scope(&self) -> &AuthorizedScope {
        &self.authorized_scope
    }
}

fn compile_table(
    table: &RegisteredTable,
    permit: &TablePermit,
    partition: Option<(ColumnId, &PartitionValue)>,
) -> Result<SnapshotTablePlan, SnapshotPlanError> {
    if table.column_ids.len() != table.inspection.columns.len()
        || table.inspection.columns.is_empty()
    {
        return Err(SnapshotPlanError::InvalidRegistry(
            "table column mapping is incomplete",
        ));
    }
    let permitted = permit.permitted_columns().collect::<BTreeSet<_>>();
    let mut columns = Vec::with_capacity(permitted.len());
    let mut primary_key = Vec::new();
    let mut key_ordinals = BTreeMap::new();
    for (column_id, inspected) in table.column_ids.iter().zip(&table.inspection.columns) {
        if !permitted.contains(column_id) {
            continue;
        }
        let codec = inspected
            .codec()
            .ok_or_else(|| SnapshotPlanError::UnsupportedType {
                schema: inspected.type_schema.clone(),
                name: inspected.type_name.clone(),
            })?;
        let (postgres_type, cast_to_text) = type_plan(codec);
        columns.push(SnapshotColumnPlan {
            column_id: *column_id,
            source_name: inspected.name.clone(),
            local_name: inspected.name.clone(),
            nullable: inspected.nullable,
            copy: CopyColumnPlan {
                codec,
                postgres_type,
                cast_to_text,
            },
        });
        if let Some(ordinal) = inspected.primary_key_ordinal {
            if ordinal == 0 || key_ordinals.insert(ordinal, *column_id).is_some() {
                return Err(SnapshotPlanError::InvalidRegistry(
                    "primary-key ordinal is zero or duplicate",
                ));
            }
        }
    }
    if key_ordinals
        .keys()
        .copied()
        .ne(1..=u32::try_from(key_ordinals.len()).map_err(|_| {
            SnapshotPlanError::InvalidRegistry("primary-key column count exceeds u32")
        })?)
    {
        return Err(SnapshotPlanError::InvalidRegistry(
            "primary-key ordinals are not contiguous",
        ));
    }
    primary_key.extend(key_ordinals.into_values());
    if primary_key.is_empty() {
        return Err(SnapshotPlanError::InvalidRegistry(
            "table has no primary key",
        ));
    }
    if columns.len() != permitted.len() {
        return Err(SnapshotPlanError::AuthorizationRegistryMismatch);
    }
    let copy_read = if partition.is_some() {
        CopyRead::KeyIndex
    } else {
        CopyRead::Planned
    };
    let copy_sql = copy_sql(table, &columns, &primary_key, partition)?;
    let schema_hash = table_digest(b"calque.snapshot-schema.v1\0", table, &columns);
    let column_profile_hash = table_digest(b"calque.column-profile.v1\0", table, &columns);
    Ok(SnapshotTablePlan {
        table_id: table.table_id.clone(),
        source_schema: table.inspection.table.schema.clone(),
        source_table: table.inspection.table.table.clone(),
        columns,
        primary_key,
        copy_sql,
        copy_read,
        schema_hash,
        column_profile_hash,
    })
}

fn type_plan(codec: PostgresTypeCodec) -> (Type, bool) {
    match codec {
        PostgresTypeCodec::Bool => (Type::BOOL, false),
        PostgresTypeCodec::Int2 => (Type::INT2, false),
        PostgresTypeCodec::Int4 => (Type::INT4, false),
        PostgresTypeCodec::Int8 => (Type::INT8, false),
        PostgresTypeCodec::Float4 => (Type::FLOAT4, false),
        PostgresTypeCodec::Float8 => (Type::FLOAT8, false),
        PostgresTypeCodec::Numeric => (Type::TEXT, true),
        PostgresTypeCodec::Text => (Type::TEXT, false),
        PostgresTypeCodec::Varchar => (Type::VARCHAR, false),
        PostgresTypeCodec::Bpchar => (Type::BPCHAR, false),
        PostgresTypeCodec::Bytea => (Type::BYTEA, false),
        PostgresTypeCodec::Uuid => (Type::UUID, false),
        PostgresTypeCodec::Json | PostgresTypeCodec::Jsonb => (Type::TEXT, true),
        PostgresTypeCodec::Date => (Type::DATE, false),
        PostgresTypeCodec::Time => (Type::TIME, false),
        PostgresTypeCodec::Timestamp => (Type::TIMESTAMP, false),
        PostgresTypeCodec::Timestamptz => (Type::TIMESTAMPTZ, false),
    }
}

/// A SQLite storage-class label for each codec. Version 1 of the schema and
/// column-profile hashes covers these labels, so they stay in the digest and an
/// unchanged column keeps its hash.
const fn digest_storage_label(codec: PostgresTypeCodec) -> &'static str {
    match codec {
        PostgresTypeCodec::Bool
        | PostgresTypeCodec::Int2
        | PostgresTypeCodec::Int4
        | PostgresTypeCodec::Int8
        | PostgresTypeCodec::Timestamp
        | PostgresTypeCodec::Timestamptz => "INTEGER",
        PostgresTypeCodec::Float4 | PostgresTypeCodec::Float8 => "REAL",
        PostgresTypeCodec::Numeric
        | PostgresTypeCodec::Text
        | PostgresTypeCodec::Varchar
        | PostgresTypeCodec::Bpchar
        | PostgresTypeCodec::Json
        | PostgresTypeCodec::Jsonb
        | PostgresTypeCodec::Date
        | PostgresTypeCodec::Time => "TEXT",
        PostgresTypeCodec::Bytea | PostgresTypeCodec::Uuid => "BLOB",
    }
}

/// The table's `COPY`. A partitioned table of a partitioned scope reads only
/// the scope's partition, whose column leads the key, so its read is a range
/// of the primary-key index in key order, and walks that index.
fn copy_sql(
    table: &RegisteredTable,
    columns: &[SnapshotColumnPlan],
    primary_key: &[ColumnId],
    partition: Option<(ColumnId, &PartitionValue)>,
) -> Result<String, SnapshotPlanError> {
    let source_alias = quote_identifier("__calque_source");
    let projections = columns
        .iter()
        .map(|column| {
            let identifier = quote_identifier(&column.source_name);
            if column.copy.cast_to_text {
                format!("{source_alias}.{identifier}::text AS {identifier}")
            } else {
                format!("{source_alias}.{identifier}")
            }
        })
        .collect::<Vec<_>>();
    let key_names = primary_key
        .iter()
        .map(|id| {
            columns
                .iter()
                .find(|column| column.column_id == *id)
                .map(|column| format!("{}.{}", source_alias, quote_identifier(&column.source_name)))
                .ok_or(SnapshotPlanError::InvalidRegistry(
                    "primary key references an unknown column",
                ))
        })
        .collect::<Result<Vec<_>, _>>()?;
    let source = format!(
        "{}.{} AS {}",
        quote_identifier(&table.inspection.table.schema),
        quote_identifier(&table.inspection.table.table),
        source_alias
    );
    let filter = match partition {
        None => String::new(),
        Some((column_id, value)) => {
            let column = columns
                .iter()
                .find(|column| column.column_id == column_id)
                .filter(|_| primary_key.first() == Some(&column_id))
                .ok_or(SnapshotPlanError::InvalidRegistry(
                    "partition column does not lead the primary key",
                ))?;
            format!(
                " WHERE {source_alias}.{} = {}",
                quote_identifier(&column.source_name),
                value.sql_literal(column.copy.codec)
            )
        }
    };
    Ok(format!(
        "COPY (SELECT {} FROM {}{} ORDER BY {}) TO STDOUT (FORMAT BINARY)",
        projections.join(", "),
        source,
        filter,
        key_names.join(", ")
    ))
}

fn table_digest(domain: &[u8], table: &RegisteredTable, columns: &[SnapshotColumnPlan]) -> String {
    let mut digest = Sha256::new();
    digest.update(domain);
    digest.update(PLAN_FORMAT_VERSION.to_be_bytes());
    digest.update(table.table_id.as_str().as_bytes());
    digest.update([0]);
    digest.update(table.inspection.table.schema.as_bytes());
    digest.update([0]);
    digest.update(table.inspection.table.table.as_bytes());
    for column in columns {
        let inspected = table
            .column_ids
            .iter()
            .position(|column_id| *column_id == column.column_id)
            .and_then(|index| table.inspection.columns.get(index))
            .expect("compiled snapshot column references checked registry metadata");
        digest.update(column.column_id.get().to_be_bytes());
        digest.update(column.source_name.as_bytes());
        digest.update([0]);
        digest.update(column.copy.codec.catalog_name().as_bytes());
        digest.update([column.nullable as u8]);
        digest.update(inspected.type_oid.to_be_bytes());
        digest.update(inspected.type_modifier.to_be_bytes());
        digest.update(inspected.primary_key_ordinal.unwrap_or(0).to_be_bytes());
        digest.update(digest_storage_label(column.copy.codec).as_bytes());
    }
    format!("sha256:{}", hex::encode(digest.finalize()))
}

fn table_set_digest(schema_generation: u64, tables: &[SnapshotTablePlan]) -> String {
    let mut digest = Sha256::new();
    digest.update(b"calque.snapshot-table-set.v1\0");
    digest.update(schema_generation.to_be_bytes());
    for table in tables {
        digest.update(table.table_id.as_str().as_bytes());
        digest.update([0]);
        digest.update(table.schema_hash.as_bytes());
        digest.update([0]);
        digest.update(table.column_profile_hash.as_bytes());
        digest.update([0]);
    }
    format!("sha256:{}", hex::encode(digest.finalize()))
}

#[derive(Debug, Error)]
pub enum SnapshotPlanError {
    #[error("snapshot build plan must contain at least one table")]
    EmptyTableSet,
    #[error("authorization scope does not permit any snapshot table")]
    EmptyAuthorizedTableSet,
    #[error("authorization scope and schema registry do not describe the same tables and columns")]
    AuthorizationRegistryMismatch,
    #[error("schema registry cannot compile a snapshot: {0}")]
    InvalidRegistry(&'static str),
    #[error("PostgreSQL type {schema}.{name} has no snapshot codec")]
    UnsupportedType { schema: String, name: String },
    #[error("{first} and {second} would both be served as collection {collection}, which is named by the table alone")]
    SharedCollectionName {
        first: String,
        second: String,
        collection: String,
    },
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeSet;

    use authz::{AuthorizationPolicy, PolicyDefinition, TablePermitDefinition};
    use source_control::{
        ColumnInspection, EnrollmentRequirements, EnrollmentSnapshot, PublicationSettings,
        QualifiedTable, SourceIdentity, SourceSettings, TableInspection,
    };

    use super::*;

    fn identity() -> SourceIdentity {
        SourceIdentity {
            system_identifier: "7612345678901234567".to_owned(),
            database_name: "app".to_owned(),
            timeline_id: 1,
            previous_timeline_id: 1,
        }
    }

    fn table(schema: &str, name: &str, relation_oid: u32) -> TableInspection {
        TableInspection {
            table: QualifiedTable {
                schema: schema.to_owned(),
                table: name.to_owned(),
            },
            relation_oid,
            in_publication: true,
            relation_kind: 'r',
            persistence: 'p',
            replica_identity: 'd',
            is_partition: false,
            has_publication_row_filter: false,
            has_publication_column_list: false,
            unlisted_columns: Vec::new(),
            columns: vec![
                column(1, "id", "uuid", false, Some(1)),
                column(2, "body\"text", "text", true, None),
            ],
        }
    }

    fn column(
        attribute_number: i16,
        name: &str,
        type_name: &str,
        nullable: bool,
        primary_key_ordinal: Option<u32>,
    ) -> ColumnInspection {
        ColumnInspection {
            attribute_number,
            name: name.to_owned(),
            type_oid: u32::from(attribute_number as u16) + 1_000,
            type_modifier: -1,
            type_schema: "pg_catalog".to_owned(),
            type_name: type_name.to_owned(),
            nullable,
            primary_key_ordinal,
            generated: false,
            nondeterministic_collation: false,
            carried_as: None,
        }
    }

    fn registry(tables: Vec<TableInspection>) -> SchemaRegistry {
        let required = tables
            .iter()
            .map(|table| table.table.clone())
            .collect::<BTreeSet<_>>();
        let snapshot = EnrollmentSnapshot {
            identity: identity(),
            settings: SourceSettings {
                server_version_num: 180_000,
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
                name: "sync_publication".to_owned(),
                exists: true,
                publishes_insert: true,
                publishes_update: true,
                publishes_delete: true,
                publishes_truncate: true,
                publishes_via_partition_root: false,
            },
            tables,
        };
        SchemaRegistry::initialize(
            &snapshot,
            &EnrollmentRequirements {
                expected_identity: identity(),
                publication_name: "sync_publication".to_owned(),
                tables: required,
                pgoutput_profile: source_control::PgoutputLaunchProfile::V1,
            },
        )
        .unwrap()
    }

    fn source_wide_scope(registry: &SchemaRegistry) -> AuthorizedScope {
        let definition = PolicyDefinition {
            generation: 7,
            tables: registry
                .tables()
                .iter()
                .map(|table| TablePermitDefinition {
                    table_id: table.table_id.clone(),
                    permitted_columns: table.column_ids.clone(),
                })
                .collect(),
        };
        AuthorizationPolicy::compile_for_registry(definition, registry)
            .unwrap()
            .scope()
    }

    #[test]
    fn detached_authorization_scope_cannot_enter_snapshot_planning() {
        let registry = registry(vec![table("public", "tasks", 10)]);
        let definition = PolicyDefinition {
            generation: 7,
            tables: registry
                .tables()
                .iter()
                .map(|table| TablePermitDefinition {
                    table_id: table.table_id.clone(),
                    permitted_columns: table.column_ids.clone(),
                })
                .collect(),
        };
        let scope = AuthorizationPolicy::compile(definition, registry.tables())
            .unwrap()
            .scope();
        assert!(matches!(
            SnapshotBuildPlan::from_registry(&registry, &scope),
            Err(SnapshotPlanError::AuthorizationRegistryMismatch)
        ));
    }

    #[test]
    fn compiles_quoted_explicit_copy() {
        let registry = registry(vec![
            table("tenant\"one", "same", 10),
            table("tenant_two", "other", 20),
        ]);
        let scope = source_wide_scope(&registry);
        let plan = SnapshotBuildPlan::from_registry(&registry, &scope).unwrap();
        assert_eq!(plan.tables.len(), 2);
        assert_eq!(
            plan.tables[0].copy_sql(),
            "COPY (SELECT \"__calque_source\".\"id\", \"__calque_source\".\"body\"\"text\" \
             FROM \"tenant\"\"one\".\"same\" AS \"__calque_source\" \
             ORDER BY \"__calque_source\".\"id\") TO STDOUT (FORMAT BINARY)"
        );
        assert_eq!(
            plan.tables[1].copy_sql(),
            "COPY (SELECT \"__calque_source\".\"id\", \"__calque_source\".\"body\"\"text\" \
             FROM \"tenant_two\".\"other\" AS \"__calque_source\" \
             ORDER BY \"__calque_source\".\"id\") TO STDOUT (FORMAT BINARY)"
        );
    }

    /// A collection is named by its table alone, so the same table name in two
    /// schemas would give the client two collections of one name, and the
    /// generated schema would describe both with the first table's columns.
    #[test]
    fn one_table_name_in_two_schemas_is_refused() {
        let registry = registry(vec![
            table("tenant_one", "tasks", 10),
            table("tenant_two", "tasks", 20),
        ]);
        let scope = source_wide_scope(&registry);
        let error = SnapshotBuildPlan::from_registry(&registry, &scope).unwrap_err();
        assert!(matches!(
            &error,
            SnapshotPlanError::SharedCollectionName { first, second, collection }
                if first == "tenant_one.tasks"
                    && second == "tenant_two.tasks"
                    && collection == "tasks"
        ));
        assert_eq!(
            error.to_string(),
            "tenant_one.tasks and tenant_two.tasks would both be served as collection tasks, \
             which is named by the table alone"
        );
    }

    #[test]
    fn artifact_table_carries_the_plan_hashes_and_row_count() {
        let registry = registry(vec![table("public", "tasks", 10)]);
        let scope = source_wide_scope(&registry);
        let plan = SnapshotBuildPlan::from_registry(&registry, &scope).unwrap();
        let table = &plan.tables[0];
        let artifact = table.artifact_table(42);
        assert_eq!(artifact.row_count, 42);
        assert_eq!(artifact.local_table, "tasks");
        assert_eq!(artifact.local_schema, "main");
        assert_eq!(
            artifact
                .columns
                .iter()
                .map(|column| column.column_id)
                .collect::<Vec<_>>(),
            table
                .columns
                .iter()
                .map(|column| column.column_id.get())
                .collect::<Vec<_>>()
        );
        assert_eq!(artifact.schema_hash.len(), 71);
        assert_eq!(artifact.column_profile_hash.len(), 71);
        assert_eq!(table.primary_key, vec![ColumnId::new(1)]);
    }

    #[test]
    fn copy_profile_uses_native_codecs_and_explicit_text_fallbacks() {
        let types = [
            "bool",
            "int2",
            "int4",
            "int8",
            "float4",
            "float8",
            "numeric",
            "text",
            "varchar",
            "bpchar",
            "bytea",
            "uuid",
            "json",
            "jsonb",
            "date",
            "time",
            "timestamp",
            "timestamptz",
        ];
        let mut inspected = table("public", "all_types", 10);
        inspected.columns = types
            .iter()
            .enumerate()
            .map(|(index, name)| {
                column(
                    i16::try_from(index + 1).unwrap(),
                    &format!("c{index}"),
                    name,
                    false,
                    (index == 0).then_some(1),
                )
            })
            .collect();
        let registry = registry(vec![inspected]);
        let scope = source_wide_scope(&registry);
        let plan = SnapshotBuildPlan::from_registry(&registry, &scope).unwrap();
        let table = &plan.tables[0];
        let fallbacks = table
            .columns
            .iter()
            .filter(|column| column.copy.cast_to_text)
            .map(|column| column.copy.codec.catalog_name())
            .collect::<Vec<_>>();
        assert_eq!(fallbacks, vec!["numeric", "json", "jsonb"]);
        assert_eq!(table.copy_types()[6], Type::TEXT);
        assert_eq!(table.copy_types()[11], Type::UUID);
        assert!(table.copy_sql().contains("\"c6\"::text AS \"c6\""));
        assert!(table.copy_sql().contains("\"c12\"::text AS \"c12\""));
    }

    /// A partitioned scope copies its own partition of each partitioned table
    /// and every row of a shared one.
    #[test]
    fn a_partitioned_scope_copies_its_partition_of_each_partitioned_table() {
        let mut partitioned = table("public", "issues", 10);
        partitioned.columns = vec![
            column(1, "tenant_id", "int8", false, Some(1)),
            column(2, "id", "int8", false, Some(2)),
            column(3, "title", "text", true, None),
        ];
        let registry = registry(vec![partitioned, table("public", "plans", 20)]);
        let issues = registry
            .tables()
            .iter()
            .find(|table| table.inspection.table.table == "issues")
            .unwrap();
        let policy = AuthorizationPolicy::compile_partitioned_for_registry(
            PolicyDefinition {
                generation: 3,
                tables: registry
                    .tables()
                    .iter()
                    .map(|table| TablePermitDefinition {
                        table_id: table.table_id.clone(),
                        permitted_columns: table.column_ids.clone(),
                    })
                    .collect(),
            },
            authz::PartitionDefinition {
                attribute: "tenant".to_owned(),
                tables: std::collections::BTreeMap::from([(
                    issues.table_id.clone(),
                    issues.column_ids[0],
                )]),
            },
            &registry,
        )
        .unwrap();
        let scope = policy.partition_scope(PartitionValue::Int(-42)).unwrap();
        let plan = SnapshotBuildPlan::from_registry(&registry, &scope).unwrap();
        let copy = |name: &str| {
            plan.tables
                .iter()
                .find(|table| table.source_table == name)
                .unwrap()
                .copy_sql()
                .to_owned()
        };
        assert_eq!(
            copy("issues"),
            "COPY (SELECT \"__calque_source\".\"tenant_id\", \"__calque_source\".\"id\", \
             \"__calque_source\".\"title\" FROM \"public\".\"issues\" AS \"__calque_source\" \
             WHERE \"__calque_source\".\"tenant_id\" = '-42'::int8 \
             ORDER BY \"__calque_source\".\"tenant_id\", \"__calque_source\".\"id\") \
             TO STDOUT (FORMAT BINARY)"
        );
        assert!(!copy("plans").contains("WHERE"));
        let read = |name: &str| {
            plan.tables
                .iter()
                .find(|table| table.source_table == name)
                .unwrap()
                .copy_read()
        };
        assert_eq!(read("issues"), CopyRead::KeyIndex);
        assert_eq!(read("plans"), CopyRead::Planned);
        assert_eq!(
            PartitionValue::Uuid([0xab; 16]).sql_literal(source_control::PostgresTypeCodec::Uuid),
            "'abababab-abab-abab-abab-abababababab'::uuid"
        );
    }

    #[test]
    fn plan_omits_unpermitted_tables_and_columns() {
        let registry = registry(vec![
            table("public", "tasks", 10),
            table("public", "private_notes", 20),
        ]);
        let permitted = &registry.tables()[0];
        let scope = AuthorizationPolicy::compile_for_registry(
            PolicyDefinition {
                generation: 9,
                tables: vec![TablePermitDefinition {
                    table_id: permitted.table_id.clone(),
                    permitted_columns: vec![permitted.column_ids[0]],
                }],
            },
            &registry,
        )
        .unwrap()
        .scope();

        let plan = SnapshotBuildPlan::from_registry(&registry, &scope).unwrap();
        assert_eq!(plan.tables.len(), 1);
        assert_eq!(plan.tables[0].table_id, permitted.table_id);
        assert_eq!(plan.tables[0].columns.len(), 1);
        assert_eq!(plan.tables[0].columns[0].source_name, "id");
        assert!(!plan.tables[0].copy_sql().contains("body"));
    }
}
