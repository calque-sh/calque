use std::{collections::BTreeSet, env, sync::Arc, time::Duration};

use postgres::{Client, NoTls};
use source_control::{
    inspect_source, validate_enrollment, ActivatedSource, EnrollmentRequirements,
    PgoutputLaunchProfile, QualifiedTable, SchemaRegistry,
};
use source_runtime::{
    establish_persistent_source, provision_persistent_slot, AcquireRequest, FenceAuthority,
    FenceStore, OwnerId, PostgresFenceStore, SlotProvisioningConfig, SourceRuntimeError,
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
        Err(SourceRuntimeError::SlotMissingReseedRequired)
    ));
    admin
        .batch_execute(&format!(
            "DROP PUBLICATION {publication}; DROP TABLE public.{table_name};"
        ))
        .unwrap();
}
