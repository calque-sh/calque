//! `calque inspect` — check that a PostgreSQL 18 publication's tables can be
//! served, and print their schema as a `calque.pg-inspection.v1` document on
//! stdout.

use std::{collections::BTreeSet, error::Error};

use calque_engine::{inspect_local_source, DatabaseUrl};
use serde::{Deserialize, Serialize};
use source_control::{
    validate_enrollment, EnrollmentRequirements, PgoutputLaunchProfile, PostgresTypeCodec,
    QualifiedTable, SchemaRegistry,
};

/// Schema version of the document this command prints and `calque init`
/// consumes. Both sides check it so a stale file fails loudly.
pub const INSPECTION_SCHEMA_VERSION: &str = "calque.pg-inspection.v1";

#[derive(Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct InspectionOutput {
    pub schema_version: String,
    pub server_major: u32,
    pub source: String,
    pub publication: String,
    pub schema_fingerprint: String,
    pub relations: Vec<RelationOutput>,
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct RelationOutput {
    pub schema: String,
    pub table: String,
    pub replica_identity: String,
    pub columns: Vec<ColumnOutput>,
    /// Columns the publication's column list leaves out that an insert must
    /// give a value, so the table takes no insert through Calque.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub unlisted_required_columns: Vec<String>,
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ColumnOutput {
    pub name: String,
    pub type_schema: String,
    pub type_name: String,
    pub nullable: bool,
    pub generated: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub primary_key_position: Option<u32>,
    /// The pg_catalog type an enum or domain column travels as.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub carried_as: Option<String>,
}

impl ColumnOutput {
    /// The pg_catalog type name this column's values travel as.
    pub fn carried_type_name(&self) -> &str {
        self.carried_as.as_deref().unwrap_or(&self.type_name)
    }

    pub fn codec(&self) -> Option<PostgresTypeCodec> {
        match &self.carried_as {
            Some(carried) => PostgresTypeCodec::from_catalog("pg_catalog", carried),
            None => PostgresTypeCodec::from_catalog(&self.type_schema, &self.type_name),
        }
    }
}

/// Build the inspection document for `tables` in `publication`.
///
/// `database_url` has passed the same guard as every other command's, so a
/// remote source is reached only over TLS that verifies it.
pub fn run(
    database_url: &DatabaseUrl,
    source: &str,
    publication: &str,
    tables: BTreeSet<QualifiedTable>,
) -> Result<InspectionOutput, Box<dyn Error>> {
    let snapshot = inspect_local_source(database_url, publication, &tables)?;
    let server_major = snapshot.settings.server_version_num / 10_000;
    if server_major != 18 {
        return Err(format!("calque inspect requires PostgreSQL 18, found {server_major}").into());
    }
    let requirements = EnrollmentRequirements {
        expected_identity: snapshot.identity.clone(),
        publication_name: publication.to_owned(),
        tables,
        pgoutput_profile: PgoutputLaunchProfile::V1,
    };
    let report = validate_enrollment(&snapshot, &requirements);
    if !report.is_ready() {
        return Err(format!("source inspection is not ready: {report}").into());
    }
    let registry = SchemaRegistry::initialize(&snapshot, &requirements)?;
    let relations = registry
        .tables()
        .iter()
        .map(|registered| {
            let inspection = &registered.inspection;
            let replica_identity = match inspection.replica_identity {
                'd' => Ok("default"),
                'f' => Ok("full"),
                'i' => Ok("index"),
                value => Err(format!("unsupported replica identity {value:?}")),
            }?;
            Ok(RelationOutput {
                schema: inspection.table.schema.clone(),
                table: inspection.table.table.clone(),
                replica_identity: replica_identity.to_owned(),
                columns: inspection
                    .columns
                    .iter()
                    .map(|column| ColumnOutput {
                        name: column.name.clone(),
                        type_schema: column.type_schema.clone(),
                        type_name: column.type_name.clone(),
                        nullable: column.nullable,
                        generated: column.generated,
                        primary_key_position: column.primary_key_ordinal,
                        carried_as: column.carried_as.clone(),
                    })
                    .collect(),
                unlisted_required_columns: inspection
                    .unlisted_columns
                    .iter()
                    .filter(|column| column.required_on_insert)
                    .map(|column| column.name.clone())
                    .collect(),
            })
        })
        .collect::<Result<Vec<_>, String>>()?;
    Ok(InspectionOutput {
        schema_version: INSPECTION_SCHEMA_VERSION.to_owned(),
        server_major,
        source: source.to_owned(),
        publication: publication.to_owned(),
        schema_fingerprint: format!("sha256:{}", registry.source_schema_sha256()),
        relations,
    })
}
