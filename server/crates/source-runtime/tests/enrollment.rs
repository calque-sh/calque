use std::{collections::BTreeSet, env, sync::Arc, time::Duration};

use postgres::{Client, NoTls};
use source_control::{
    inspect_source, validate_enrollment, ActivatedSource, EnrollmentRequirements,
    PgoutputLaunchProfile, QualifiedTable, SchemaRegistry,
};
use source_runtime::{
    establish_persistent_source, inspect_owned_publication, provision_persistent_slot,
    AcquireRequest, FenceAuthority, FenceStore, OwnedPublication, OwnerId, PostgresFenceStore,
    SlotProvisioningConfig, SourceRuntimeError,
};
use sync_types::{CursorCodec, SourceEpoch, SourceId};

fn required(name: &str) -> String {
    env::var(name).unwrap_or_else(|_| panic!("{name} must be set"))
}

fn replication_url(database_url: &str) -> String {
    let separator = if database_url.contains('?') { '&' } else { '?' };
    format!("{database_url}{separator}replication=database")
}

fn request(source_id: &SourceId, epoch: &SourceEpoch, owner: &str) -> AcquireRequest {
    AcquireRequest {
        source_id: source_id.clone(),
        source_epoch: epoch.clone(),
        owner_id: OwnerId::new(owner).unwrap(),
        ttl: Duration::from_secs(5),
    }
}

#[test]
#[ignore = "requires disposable PostgreSQL 18 admin, control, and least-privilege source URLs"]
fn pg18_least_privilege_enrollment_binds_slot_identity_and_epoch() {
    let admin_url = required("CALQUE_ADMIN_DATABASE_URL");
    let source_url = required("CALQUE_SOURCE_DATABASE_URL");
    let control_url = required("CALQUE_CONTROL_DATABASE_URL");
    let suffix = format!(
        "{}_{}",
        std::process::id(),
        std::time::UNIX_EPOCH.elapsed().unwrap().as_nanos()
    );
    let table_name = format!("rm_enroll_{suffix}");
    let publication = format!("rm_enroll_pub_{suffix}");
    let slot = format!("rm_enroll_slot_{suffix}");
    let source_id = SourceId::new(format!("enrollment-{suffix}")).unwrap();
    let epoch = SourceEpoch::new("epoch-1").unwrap();
    let mut admin = Client::connect(&admin_url, NoTls).unwrap();
    admin
        .batch_execute(&format!(
            "CREATE TABLE public.{table_name} (id bigint PRIMARY KEY, value text NOT NULL);
             CREATE PUBLICATION {publication} FOR TABLE public.{table_name};"
        ))
        .unwrap();

    let table = QualifiedTable {
        schema: "public".to_owned(),
        table: table_name.clone(),
    };
    let tables = BTreeSet::from([table]);
    let mut source = Client::connect(&source_url, NoTls).unwrap();
    let snapshot = inspect_source(&mut source, &publication, &tables).unwrap();
    assert_eq!(snapshot.settings.server_version_num / 10_000, 18);
    assert!(!snapshot.settings.current_user_is_superuser);
    assert!(snapshot.settings.current_user_has_replication);
    let requirements = EnrollmentRequirements {
        expected_identity: snapshot.identity.clone(),
        publication_name: publication.clone(),
        tables,
        pgoutput_profile: PgoutputLaunchProfile::V1,
    };
    let report = validate_enrollment(&snapshot, &requirements);
    assert!(report.is_ready(), "{:#?}", report.diagnostics);
    let registry = SchemaRegistry::initialize(&snapshot, &requirements).unwrap();
    let activated = ActivatedSource::new(
        &registry,
        epoch.clone(),
        CursorCodec::new([71_u8; 32]).unwrap(),
        PgoutputLaunchProfile::V1,
    );

    let store = PostgresFenceStore::connect(&control_url).unwrap();
    store.enroll_source(&source_id, &epoch).unwrap();
    let authority = Arc::new(FenceAuthority::new(store));
    let provisioner = authority
        .acquire(request(&source_id, &epoch, "provisioner"))
        .unwrap();
    let enrollment = provision_persistent_slot(
        &authority,
        &provisioner,
        &source_url,
        &activated,
        &SlotProvisioningConfig::new(&slot).unwrap(),
    )
    .unwrap();
    assert_eq!(enrollment.source_epoch(), &epoch);
    assert_eq!(enrollment.slot_name(), slot);
    assert_eq!(enrollment.publication_name(), publication);
    authority.release(&provisioner).unwrap();

    let wrong_epoch = ActivatedSource::new(
        &registry,
        SourceEpoch::new("epoch-2").unwrap(),
        CursorCodec::new([72_u8; 32]).unwrap(),
        PgoutputLaunchProfile::V1,
    );
    let wrong_lease = authority
        .acquire(request(&source_id, &epoch, "wrong-epoch"))
        .unwrap();
    assert!(matches!(
        establish_persistent_source(
            Arc::clone(&authority),
            wrong_lease,
            &source_url,
            &replication_url(&source_url),
            &wrong_epoch,
        ),
        Err(SourceRuntimeError::ActivatedSourceLeaseMismatch)
    ));

    let runtime = authority
        .acquire(request(&source_id, &epoch, "runtime"))
        .unwrap();
    let session = establish_persistent_source(
        Arc::clone(&authority),
        runtime,
        &source_url,
        &replication_url(&source_url),
        &activated,
    )
    .unwrap();
    assert!(session.socket_is_open());
    session.shutdown().unwrap();
    admin
        .query_one("SELECT pg_catalog.pg_drop_replication_slot($1)", &[&slot])
        .unwrap();

    let missing = authority
        .acquire(request(&source_id, &epoch, "missing-slot"))
        .unwrap();
    assert!(matches!(
        establish_persistent_source(
            Arc::clone(&authority),
            missing,
            &source_url,
            &replication_url(&source_url),
            &activated,
        ),
        Err(SourceRuntimeError::SlotMissing { .. })
    ));
    admin
        .batch_execute(&format!(
            "DROP PUBLICATION {publication}; DROP TABLE public.{table_name};"
        ))
        .unwrap();
}

/// Renaming or adding an enum value changes no row, so the publication's
/// fingerprint has to change instead, whether the publication lists the table
/// or reaches it through its schema. An enum only an unlisted column uses
/// leaves it as it was, since no published value changes.
///
/// Only a superuser may create a `FOR TABLES IN SCHEMA` publication, so this
/// test connects as one.
#[test]
#[ignore = "requires a disposable PostgreSQL 18 superuser URL"]
fn the_publication_fingerprint_follows_the_enums_its_published_columns_use() {
    let admin_url = required("CALQUE_SUPERUSER_DATABASE_URL");
    let suffix = format!(
        "{}_{}",
        std::process::id(),
        std::time::UNIX_EPOCH.elapsed().unwrap().as_nanos()
    );
    let schema = format!("rm_enum_{suffix}");
    let listed = format!("rm_enum_listed_{suffix}");
    let in_schema = format!("rm_enum_schema_{suffix}");
    let hidden = format!("rm_enum_hidden_{suffix}");
    let mut admin = Client::connect(&admin_url, NoTls).unwrap();
    admin
        .batch_execute(&format!(
            "CREATE SCHEMA {schema};
             CREATE TYPE {schema}.mood AS ENUM ('calm', 'busy');
             CREATE DOMAIN {schema}.mood_label AS {schema}.mood;
             CREATE TABLE {schema}.items (id bigint PRIMARY KEY, mood {schema}.mood_label NOT NULL);
             CREATE PUBLICATION {listed} FOR TABLE {schema}.items;
             CREATE PUBLICATION {in_schema} FOR TABLES IN SCHEMA {schema};
             CREATE PUBLICATION {hidden} FOR TABLE {schema}.items (id);"
        ))
        .unwrap();
    let tables = BTreeSet::from([QualifiedTable {
        schema: schema.clone(),
        table: "items".to_owned(),
    }]);
    let publications = [listed.as_str(), in_schema.as_str(), hidden.as_str()];
    let fingerprints = |admin: &mut Client| {
        publications.map(|publication| {
            match inspect_owned_publication(admin, publication, &tables).unwrap() {
                OwnedPublication::Matches { fingerprint, .. } => fingerprint,
                other => panic!("{publication}: {other:?}"),
            }
        })
    };

    let before = fingerprints(&mut admin);
    admin
        .batch_execute(&format!(
            "ALTER TYPE {schema}.mood RENAME VALUE 'busy' TO 'swamped'"
        ))
        .unwrap();
    let renamed = fingerprints(&mut admin);
    admin
        .batch_execute(&format!("ALTER TYPE {schema}.mood ADD VALUE 'away'"))
        .unwrap();
    let added = fingerprints(&mut admin);

    for index in 0..2 {
        assert_ne!(before[index], renamed[index], "{}", publications[index]);
        assert_ne!(renamed[index], added[index], "{}", publications[index]);
    }
    assert_eq!(before[2], renamed[2]);
    assert_eq!(renamed[2], added[2]);
    admin
        .batch_execute(&format!(
            "DROP PUBLICATION {listed}, {in_schema}, {hidden}; DROP SCHEMA {schema} CASCADE;"
        ))
        .unwrap();
}
