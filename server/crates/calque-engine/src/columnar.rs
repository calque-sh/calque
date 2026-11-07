//! The direct-columnar artifact the browser client mounts: the decoder profile
//! it is planned under and the artifact id one prepared scope publishes.
//! `calque dev` and `calque serve` build it straight from PostgreSQL binary
//! COPY (`snapshot_builder::DirectBuildSession`).
use direct_columnar::FORMAT_VERSION;
use sync_types::ArtifactId;
use thiserror::Error;

/// The decoder profile the browser client declares. It is the string the
/// planner's cache key carries for a direct-columnar artifact of this format
/// version, so a request declaring it can only ever be planned against these
/// bytes.
pub(crate) fn direct_columnar_decoder_profile() -> String {
    artifact_contract::direct_columnar_decoder_profile(FORMAT_VERSION)
}

/// The direct-columnar artifact one prepared scope publishes.
pub(crate) fn local_columnar_artifact_id(identity: &str) -> Result<ArtifactId, LocalColumnarError> {
    ArtifactId::new(format!("local-columnar-{identity}"))
        .map_err(|_| LocalColumnarError::InvalidConfiguration)
}

#[derive(Debug, Error)]
pub enum LocalColumnarError {
    #[error("direct-columnar artifact configuration is invalid")]
    InvalidConfiguration,
    #[error(transparent)]
    Copy(#[from] snapshot_builder::DirectColumnarCopyError),
    #[error(transparent)]
    Encode(#[from] direct_columnar::EncodeError),
}

#[cfg(test)]
mod tests {
    use direct_columnar::{schema_hash, Encoder, EncoderLimits};
    use snapshot_builder::{
        direct_columnar_artifact_identity, direct_columnar_workspace_schema, SnapshotBuildPlan,
    };
    use sync_types::SourceEpoch;

    use super::*;
    use crate::snapshot::tests::prepared_source;

    #[test]
    fn the_collection_is_the_postgres_table_and_the_hash_binds_the_encoder_schema() {
        let prepared = prepared_source(SourceEpoch::new("columnar-epoch").unwrap());
        let plan = SnapshotBuildPlan::from_registry(&prepared.registry, &prepared.scope).unwrap();
        let schema = direct_columnar_workspace_schema(&plan).unwrap();
        let hash = schema_hash(&schema).unwrap();
        assert_eq!(schema.tables.len(), 1);
        let table = &schema.tables[0];
        assert_eq!(table.name, "tasks");
        assert_eq!(table.primary_key, vec!["id".to_owned()]);
        assert_eq!(
            table
                .columns
                .iter()
                .map(|column| (column.name.as_str(), column.kind))
                .collect::<Vec<_>>(),
            vec![
                ("id", direct_columnar::ColumnKind::Uuid),
                ("title", direct_columnar::ColumnKind::Utf8),
            ]
        );
        let identity = direct_columnar_artifact_identity(
            &plan,
            prepared.activated.source_epoch(),
            &hash,
            "cursor-1",
        );
        assert_eq!(
            identity.manifest_version,
            prepared.registry.schema_generation().to_string()
        );
        assert_eq!(
            identity.authorization_scope_version,
            plan.authorization_identity().generation().to_string()
        );
        assert_eq!(identity.collection_set_hash, plan.table_set_hash());
        assert_eq!(identity.cursor, "cursor-1");
        // The encoder accepts exactly this identity and schema pair.
        Encoder::new(identity, schema, EncoderLimits::default()).unwrap();
    }

    #[test]
    fn the_decoder_profile_is_the_planner_cache_key_string() {
        assert_eq!(
            direct_columnar_decoder_profile(),
            "calque-direct-columnar/v4"
        );
    }
}
