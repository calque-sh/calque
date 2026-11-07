use std::{collections::BTreeSet, sync::Arc};

use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use sync_types::{ColumnId, TableId};
use thiserror::Error;

use crate::{
    inspect_source, validate_enrollment, EnrollmentReport, EnrollmentRequirements,
    EnrollmentSnapshot, InspectError, PublicationSettings, QualifiedTable, SourceIdentity,
    TableInspection,
};

const FORMAT_VERSION: u32 = 1;
const MAX_REGISTRY_BYTES: usize = 16 * 1024 * 1024;
const TABLE_ID_DOMAIN: &[u8] = b"calque.table-id.v1\0";

/// Stable schema-generation mapping for one enrolled PostgreSQL table.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct RegisteredTable {
    pub table_id: TableId,
    /// Column IDs follow PostgreSQL catalog column order and are scoped to this
    /// registry's schema generation. They are not attribute numbers or OIDs.
    pub column_ids: Vec<ColumnId>,
    pub inspection: TableInspection,
}

/// Caller-persisted schema registry state. The control-plane storage owner must
/// commit these bytes atomically before activating the corresponding decoder.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct SchemaRegistry {
    format_version: u32,
    source_identity: SourceIdentity,
    publication: PublicationSettings,
    schema_generation: u64,
    source_schema_sha256: String,
    tables: Vec<RegisteredTable>,
}

/// Immutable identity of one activated source/schema/publication registry.
///
/// Snapshot planning and temporary replication carry this value end to end so
/// a plan compiled for one publication cannot be paired with another existing
/// publication that happens to reach the same cut marker.
///
/// Shared because every change build clones its plan's binding.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SchemaRegistryBinding(Arc<BindingFields>);

#[derive(Debug, PartialEq, Eq)]
struct BindingFields {
    source_identity: SourceIdentity,
    publication: PublicationSettings,
    schema_generation: u64,
    source_schema_sha256: String,
    tables: Vec<(TableId, QualifiedTable)>,
}

impl SchemaRegistryBinding {
    pub fn source_identity(&self) -> &SourceIdentity {
        &self.0.source_identity
    }

    pub fn publication(&self) -> &PublicationSettings {
        &self.0.publication
    }

    pub fn schema_generation(&self) -> u64 {
        self.0.schema_generation
    }

    pub fn tables(&self) -> &[(TableId, QualifiedTable)] {
        &self.0.tables
    }
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct RegistryEnvelope {
    registry: SchemaRegistry,
    registry_sha256: String,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
struct FingerprintInput<'a> {
    source_identity: &'a SourceIdentity,
    publication: &'a PublicationSettings,
    tables: Vec<&'a TableInspection>,
}

#[derive(Debug, Error)]
pub enum SchemaRegistryError {
    #[error("source enrollment is not ready: {0}")]
    EnrollmentNotReady(EnrollmentReport),
    #[error("schema registry serialization failed: {0}")]
    Serialization(#[from] serde_json::Error),
    #[error("schema registry is {actual} bytes, exceeding {maximum}")]
    TooLarge { actual: usize, maximum: usize },
    #[error("schema registry checksum is invalid")]
    ChecksumMismatch,
    #[error("schema registry state is invalid: {0}")]
    Invalid(&'static str),
    #[error("stable table ID construction failed: {0}")]
    TableId(#[from] sync_types::IdentifierError),
    #[error("live source reinspection failed: {0}")]
    Inspect(#[from] InspectError),
    #[error("live source schema/publication fingerprint drifted from the activated registry")]
    LiveDrift,
}

impl SchemaRegistry {
    pub fn initialize(
        snapshot: &EnrollmentSnapshot,
        requirements: &EnrollmentRequirements,
    ) -> Result<Self, SchemaRegistryError> {
        require_ready(snapshot, requirements)?;
        Self::compile(snapshot, 1)
    }

    pub const fn schema_generation(&self) -> u64 {
        self.schema_generation
    }

    pub fn source_identity(&self) -> &SourceIdentity {
        &self.source_identity
    }

    pub fn source_schema_sha256(&self) -> &str {
        &self.source_schema_sha256
    }

    pub fn publication(&self) -> &PublicationSettings {
        &self.publication
    }

    pub fn binding(&self) -> SchemaRegistryBinding {
        SchemaRegistryBinding(Arc::new(BindingFields {
            source_identity: self.source_identity.clone(),
            publication: self.publication.clone(),
            schema_generation: self.schema_generation,
            source_schema_sha256: self.source_schema_sha256.clone(),
            tables: self
                .tables
                .iter()
                .map(|table| (table.table_id.clone(), table.inspection.table.clone()))
                .collect(),
        }))
    }

    /// Reinspect the source and reject any schema or publication drift.
    ///
    /// Snapshot finalization calls this only after observing the cut commit, so
    /// a DDL or publication change committed before `P` cannot be mislabeled as
    /// the older activated schema generation.
    pub fn verify_live(&self, client: &mut postgres::Client) -> Result<(), SchemaRegistryError> {
        self.validate()?;
        let requirements = EnrollmentRequirements {
            expected_identity: self.source_identity.clone(),
            publication_name: self.publication.name.clone(),
            tables: self
                .tables
                .iter()
                .map(|table| table.inspection.table.clone())
                .collect(),
            pgoutput_profile: crate::PgoutputLaunchProfile::V1,
        };
        let snapshot = inspect_source(client, &self.publication.name, &requirements.tables)?;
        require_ready(&snapshot, &requirements)?;
        if schema_fingerprint(&snapshot)? != self.source_schema_sha256 {
            return Err(SchemaRegistryError::LiveDrift);
        }
        Ok(())
    }

    pub fn tables(&self) -> &[RegisteredTable] {
        &self.tables
    }

    pub fn encode(&self) -> Result<Vec<u8>, SchemaRegistryError> {
        self.validate()?;
        let registry_bytes = serde_json::to_vec(self)?;
        let envelope = RegistryEnvelope {
            registry: self.clone(),
            registry_sha256: sha256_hex(&registry_bytes),
        };
        let bytes = serde_json::to_vec(&envelope)?;
        enforce_size(bytes.len())?;
        Ok(bytes)
    }

    pub fn decode(bytes: &[u8]) -> Result<Self, SchemaRegistryError> {
        enforce_size(bytes.len())?;
        let envelope: RegistryEnvelope = serde_json::from_slice(bytes)?;
        let registry_bytes = serde_json::to_vec(&envelope.registry)?;
        if envelope.registry_sha256 != sha256_hex(&registry_bytes) {
            return Err(SchemaRegistryError::ChecksumMismatch);
        }
        envelope.registry.validate()?;
        Ok(envelope.registry)
    }

    fn compile(
        snapshot: &EnrollmentSnapshot,
        schema_generation: u64,
    ) -> Result<Self, SchemaRegistryError> {
        let mut inspections = snapshot.tables.clone();
        inspections.sort_by(|left, right| left.table.cmp(&right.table));
        let mut tables = Vec::with_capacity(inspections.len());
        for inspection in inspections {
            let table_id = stable_table_id(&snapshot.identity, &inspection, schema_generation)?;
            let column_ids = (1..=inspection.columns.len())
                .map(|ordinal| {
                    u32::try_from(ordinal)
                        .map(ColumnId::new)
                        .map_err(|_| SchemaRegistryError::Invalid("column count exceeds u32"))
                })
                .collect::<Result<Vec<_>, _>>()?;
            tables.push(RegisteredTable {
                table_id,
                column_ids,
                inspection,
            });
        }
        let registry = Self {
            format_version: FORMAT_VERSION,
            source_identity: snapshot.identity.clone(),
            publication: snapshot.publication.clone(),
            schema_generation,
            source_schema_sha256: schema_fingerprint(snapshot)?,
            tables,
        };
        registry.validate()?;
        Ok(registry)
    }

    fn validate(&self) -> Result<(), SchemaRegistryError> {
        if self.format_version != FORMAT_VERSION {
            return Err(SchemaRegistryError::Invalid(
                "format version is unsupported",
            ));
        }
        if self.schema_generation == 0 || self.tables.is_empty() {
            return Err(SchemaRegistryError::Invalid(
                "generation must be nonzero and tables cannot be empty",
            ));
        }
        if self.source_schema_sha256.len() != 64
            || !self
                .source_schema_sha256
                .bytes()
                .all(|byte| byte.is_ascii_hexdigit() && !byte.is_ascii_uppercase())
        {
            return Err(SchemaRegistryError::Invalid(
                "source schema fingerprint is not lowercase SHA-256 hex",
            ));
        }
        let mut table_ids = BTreeSet::new();
        let mut qualified_tables = BTreeSet::new();
        let mut previous = None;
        for table in &self.tables {
            if previous.is_some_and(|value| value >= &table.inspection.table) {
                return Err(SchemaRegistryError::Invalid(
                    "registered tables are not strictly sorted",
                ));
            }
            previous = Some(&table.inspection.table);
            if !table_ids.insert(table.table_id.clone())
                || !qualified_tables.insert(table.inspection.table.clone())
                || table.column_ids.len() != table.inspection.columns.len()
                || table.column_ids.is_empty()
            {
                return Err(SchemaRegistryError::Invalid(
                    "table or column mapping is duplicate or incomplete",
                ));
            }
            let expected = stable_table_id(
                &self.source_identity,
                &table.inspection,
                self.schema_generation,
            )?;
            if table.table_id != expected
                || table
                    .column_ids
                    .iter()
                    .enumerate()
                    .any(|(index, id)| id.get() != (index as u32) + 1)
            {
                return Err(SchemaRegistryError::Invalid(
                    "stable table or column ID derivation differs",
                ));
            }
        }
        let snapshot = EnrollmentSnapshot {
            identity: self.source_identity.clone(),
            // Operational settings do not participate in schema identity.
            settings: crate::SourceSettings {
                server_version_num: 0,
                server_encoding: String::new(),
                date_style: String::new(),
                time_zone: String::new(),
                bytea_output: String::new(),
                wal_level: String::new(),
                max_replication_slots: 0,
                max_wal_senders: 0,
                current_user: String::new(),
                current_user_is_superuser: false,
                current_user_has_replication: false,
            },
            publication: self.publication.clone(),
            tables: self
                .tables
                .iter()
                .map(|table| table.inspection.clone())
                .collect(),
        };
        if schema_fingerprint(&snapshot)? != self.source_schema_sha256 {
            return Err(SchemaRegistryError::Invalid(
                "source schema fingerprint differs from registry contents",
            ));
        }
        Ok(())
    }
}

fn require_ready(
    snapshot: &EnrollmentSnapshot,
    requirements: &EnrollmentRequirements,
) -> Result<(), SchemaRegistryError> {
    let report = validate_enrollment(snapshot, requirements);
    if report.is_ready() {
        Ok(())
    } else {
        Err(SchemaRegistryError::EnrollmentNotReady(report))
    }
}

fn schema_fingerprint(snapshot: &EnrollmentSnapshot) -> Result<String, serde_json::Error> {
    let mut tables: Vec<_> = snapshot.tables.iter().collect();
    tables.sort_by(|left, right| left.table.cmp(&right.table));
    let input = FingerprintInput {
        source_identity: &snapshot.identity,
        publication: &snapshot.publication,
        tables,
    };
    Ok(sha256_hex(&serde_json::to_vec(&input)?))
}

fn stable_table_id(
    source: &SourceIdentity,
    table: &TableInspection,
    schema_generation: u64,
) -> Result<TableId, sync_types::IdentifierError> {
    let mut digest = Sha256::new();
    digest.update(TABLE_ID_DOMAIN);
    digest.update(source.system_identifier.as_bytes());
    digest.update([0]);
    digest.update(source.database_name.as_bytes());
    digest.update([0]);
    digest.update(source.timeline_id.to_be_bytes());
    digest.update(source.previous_timeline_id.to_be_bytes());
    digest.update(table.table.schema.as_bytes());
    digest.update([0]);
    digest.update(table.table.table.as_bytes());
    digest.update([0]);
    digest.update(schema_generation.to_be_bytes());
    TableId::new(format!(
        "table:{}:g{schema_generation}",
        hex::encode(digest.finalize())
    ))
}

fn sha256_hex(bytes: &[u8]) -> String {
    hex::encode(Sha256::digest(bytes))
}

fn enforce_size(actual: usize) -> Result<(), SchemaRegistryError> {
    if actual > MAX_REGISTRY_BYTES {
        Err(SchemaRegistryError::TooLarge {
            actual,
            maximum: MAX_REGISTRY_BYTES,
        })
    } else {
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeSet;

    use super::*;
    use crate::{
        ColumnInspection, EnrollmentRequirements, EnrollmentSnapshot, PublicationSettings,
        QualifiedTable, SourceSettings,
    };

    fn identity() -> SourceIdentity {
        SourceIdentity {
            system_identifier: "7612345678901234567".to_owned(),
            database_name: "app".to_owned(),
            timeline_id: 1,
            previous_timeline_id: 1,
        }
    }

    fn table_name() -> QualifiedTable {
        QualifiedTable {
            schema: "public".to_owned(),
            table: "tasks".to_owned(),
        }
    }

    fn inspection() -> TableInspection {
        TableInspection {
            table: table_name(),
            relation_oid: 16_384,
            in_publication: true,
            relation_kind: 'r',
            persistence: 'p',
            replica_identity: 'd',
            is_partition: false,
            has_publication_row_filter: false,
            has_publication_column_list: false,
            columns: vec![
                ColumnInspection {
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
                },
                ColumnInspection {
                    attribute_number: 2,
                    name: "title".to_owned(),
                    type_oid: 25,
                    type_modifier: -1,
                    type_schema: "pg_catalog".to_owned(),
                    type_name: "text".to_owned(),
                    nullable: false,
                    primary_key_ordinal: None,
                    generated: false,
                    nondeterministic_collation: false,
                },
            ],
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
                current_user: "sync".to_owned(),
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
            tables: vec![inspection()],
        }
    }

    fn requirements() -> EnrollmentRequirements {
        EnrollmentRequirements {
            expected_identity: identity(),
            publication_name: "sync_publication".to_owned(),
            tables: BTreeSet::from([table_name()]),
            pgoutput_profile: crate::PgoutputLaunchProfile::V1,
        }
    }

    #[test]
    fn ready_enrollment_gets_stable_non_postgres_ids_and_checked_roundtrip() {
        let registry = SchemaRegistry::initialize(&snapshot(), &requirements()).unwrap();
        assert_eq!(registry.schema_generation(), 1);
        assert_eq!(
            registry.tables()[0]
                .column_ids
                .iter()
                .map(|id| id.get())
                .collect::<Vec<_>>(),
            [1, 2]
        );
        let table_id = registry.tables()[0].table_id.as_str();
        assert!(table_id.starts_with("table:"));
        assert!(table_id.ends_with(":g1"));
        assert!(!table_id.contains("16384"));

        let encoded = registry.encode().unwrap();
        assert_eq!(SchemaRegistry::decode(&encoded).unwrap(), registry);
    }

    #[test]
    fn operational_changes_keep_the_fingerprint_but_pinned_layout_changes_move_it() {
        let registry = SchemaRegistry::initialize(&snapshot(), &requirements()).unwrap();
        let mut operational = snapshot();
        operational.settings.max_wal_senders = 20;
        operational.settings.current_user = "other_sync_role".to_owned();
        assert_eq!(
            schema_fingerprint(&operational).unwrap(),
            registry.source_schema_sha256()
        );

        let mut changed = snapshot();
        changed.tables[0].columns[1].type_modifier = 100;
        assert_ne!(
            schema_fingerprint(&changed).unwrap(),
            registry.source_schema_sha256()
        );
    }

    #[test]
    fn checksum_mapping_and_readiness_tampering_fail_closed() {
        let registry = SchemaRegistry::initialize(&snapshot(), &requirements()).unwrap();
        let mut envelope: serde_json::Value =
            serde_json::from_slice(&registry.encode().unwrap()).unwrap();
        envelope["registry"]["tables"][0]["inspection"]["relationOid"] = serde_json::json!(16_385);
        let tampered = serde_json::to_vec(&envelope).unwrap();
        assert!(matches!(
            SchemaRegistry::decode(&tampered),
            Err(SchemaRegistryError::ChecksumMismatch)
        ));

        let mut invalid_mapping = registry.clone();
        invalid_mapping.tables[0].column_ids[0] = ColumnId::new(99);
        assert!(matches!(
            invalid_mapping.encode(),
            Err(SchemaRegistryError::Invalid(_))
        ));

        let mut not_ready = snapshot();
        not_ready.tables[0].in_publication = false;
        assert!(matches!(
            SchemaRegistry::initialize(&not_ready, &requirements()),
            Err(SchemaRegistryError::EnrollmentNotReady(_))
        ));
    }

    #[test]
    fn hostile_postgres_identifiers_are_hashed_not_embedded_in_table_id() {
        let mut snapshot = snapshot();
        snapshot.tables[0].table.schema = "odd schema".to_owned();
        snapshot.tables[0].table.table = "tasks/with spaces".to_owned();
        let requirements = EnrollmentRequirements {
            expected_identity: identity(),
            publication_name: "sync_publication".to_owned(),
            tables: BTreeSet::from([snapshot.tables[0].table.clone()]),
            pgoutput_profile: crate::PgoutputLaunchProfile::V1,
        };
        let registry = SchemaRegistry::initialize(&snapshot, &requirements).unwrap();
        assert!(!registry.tables()[0].table_id.as_str().contains(' '));
        assert!(!registry.tables()[0].table_id.as_str().contains('/'));
    }
}
