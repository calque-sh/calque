//! Registry fixtures every engine test plans against.

use std::collections::BTreeSet;

use source_control::{
    ColumnInspection, EnrollmentRequirements, EnrollmentSnapshot, PgoutputLaunchProfile,
    PublicationSettings, QualifiedTable, SchemaRegistry, SourceIdentity, SourceSettings,
    TableInspection,
};

pub(crate) fn column(
    attribute_number: i16,
    name: &str,
    type_oid: u32,
    type_name: &str,
    nullable: bool,
    primary_key_ordinal: Option<u32>,
) -> ColumnInspection {
    ColumnInspection {
        attribute_number,
        name: name.to_owned(),
        type_oid,
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

pub(crate) fn inspection(
    table: &str,
    relation_oid: u32,
    columns: Vec<ColumnInspection>,
) -> TableInspection {
    TableInspection {
        table: QualifiedTable {
            schema: "public".to_owned(),
            table: table.to_owned(),
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
        columns,
    }
}

/// The registry every engine test plans against: one uuid-keyed `tasks` table
/// with a text title.
pub(crate) fn registry() -> SchemaRegistry {
    registry_for(vec![inspection(
        "tasks",
        16_384,
        vec![
            column(1, "id", 2_950, "uuid", false, Some(1)),
            column(2, "title", 25, "text", false, None),
        ],
    )])
}

pub(crate) fn registry_for(tables: Vec<TableInspection>) -> SchemaRegistry {
    let identity = SourceIdentity {
        system_identifier: "7612345678901234567".to_owned(),
        database_name: "app".to_owned(),
        timeline_id: 1,
        previous_timeline_id: 1,
    };
    let required = tables
        .iter()
        .map(|table| table.table.clone())
        .collect::<BTreeSet<_>>();
    let snapshot = EnrollmentSnapshot {
        identity: identity.clone(),
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
            name: "calque_engine_test".to_owned(),
            exists: true,
            publishes_insert: true,
            publishes_update: true,
            publishes_delete: true,
            publishes_truncate: true,
            publishes_via_partition_root: false,
        },
        tables,
    };
    let requirements = EnrollmentRequirements {
        expected_identity: identity,
        publication_name: "calque_engine_test".to_owned(),
        tables: required,
        pgoutput_profile: PgoutputLaunchProfile::V1,
    };
    SchemaRegistry::initialize(&snapshot, &requirements).unwrap()
}

/// A TCP listener that accepts every connection and never answers, so a
/// PostgreSQL client waits on its startup reply until its own timeout. Returns
/// a URL that reaches it and a count of accepted connections.
pub(crate) fn silent_listener() -> (String, std::sync::Arc<std::sync::atomic::AtomicUsize>) {
    use std::sync::{atomic::Ordering, Arc};

    let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let url = format!(
        "postgresql://calque@127.0.0.1:{}/calque",
        listener.local_addr().unwrap().port()
    );
    let accepted = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let counter = Arc::clone(&accepted);
    std::thread::spawn(move || {
        let mut held = Vec::new();
        for stream in listener.incoming() {
            let Ok(stream) = stream else { return };
            held.push(stream);
            counter.fetch_add(1, Ordering::SeqCst);
        }
    });
    (url, accepted)
}
