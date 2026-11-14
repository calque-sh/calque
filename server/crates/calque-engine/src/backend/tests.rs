use super::*;
use crate::{reencode::ReencodeOutcome, revocation::draft_barrier, LOCAL_DEVELOPMENT_SUBJECT};
use artifact_contract::RepresentationEncoding;
use dataset_grant_issuer::GrantIssuer;
use std::{
    panic::AssertUnwindSafe,
    sync::atomic::{AtomicUsize, Ordering},
};

struct RuntimeSeparatedLog {
    source_id: SourceId,
    source_epoch: SourceEpoch,
}

impl CanonicalLogRead for RuntimeSeparatedLog {
    fn source_id(&self) -> &SourceId {
        &self.source_id
    }

    fn source_epoch(&self) -> &SourceEpoch {
        &self.source_epoch
    }

    fn head(&self) -> Result<CursorPosition, LogError> {
        assert!(tokio::runtime::Handle::try_current().is_err());
        Ok(CursorPosition {
            source_epoch: self.source_epoch.clone(),
            sequence: 7,
        })
    }

    fn minimum_retained(&self) -> Result<CursorPosition, LogError> {
        assert!(tokio::runtime::Handle::try_current().is_err());
        Ok(CursorPosition {
            source_epoch: self.source_epoch.clone(),
            sequence: 1,
        })
    }

    fn read_page_after_bounded(
        &self,
        _from_sequence_exclusive: u64,
        _through_sequence_inclusive: Option<u64>,
        _max_records: usize,
        _max_bytes: u64,
    ) -> Result<CanonicalPage, LogError> {
        assert!(tokio::runtime::Handle::try_current().is_err());
        Ok(CanonicalPage {
            records: Vec::new(),
            stored_bytes: 0,
        })
    }
}

impl CanonicalLogPlanRetention for RuntimeSeparatedLog {
    fn acquire_plan_retention_lease(
        &mut self,
        _plan_id: PlanId,
        _minimum_sequence: u64,
        _expires_ms: i64,
        _now_ms: i64,
    ) -> Result<(), LogError> {
        assert!(tokio::runtime::Handle::try_current().is_err());
        Ok(())
    }
}

struct PanicOnHeadLog {
    source_id: SourceId,
    source_epoch: SourceEpoch,
}

impl CanonicalLogRead for PanicOnHeadLog {
    fn source_id(&self) -> &SourceId {
        &self.source_id
    }

    fn source_epoch(&self) -> &SourceEpoch {
        &self.source_epoch
    }

    fn head(&self) -> Result<CursorPosition, LogError> {
        panic!("injected canonical-log worker panic");
    }

    fn minimum_retained(&self) -> Result<CursorPosition, LogError> {
        unreachable!()
    }

    fn read_page_after_bounded(
        &self,
        _from_sequence_exclusive: u64,
        _through_sequence_inclusive: Option<u64>,
        _max_records: usize,
        _max_bytes: u64,
    ) -> Result<CanonicalPage, LogError> {
        unreachable!()
    }
}

impl CanonicalLogPlanRetention for PanicOnHeadLog {
    fn acquire_plan_retention_lease(
        &mut self,
        _plan_id: PlanId,
        _minimum_sequence: u64,
        _expires_ms: i64,
        _now_ms: i64,
    ) -> Result<(), LogError> {
        unreachable!()
    }
}

struct ActiveWorker(Arc<AtomicUsize>);

impl Drop for ActiveWorker {
    fn drop(&mut self) {
        self.0.fetch_sub(1, Ordering::SeqCst);
    }
}

fn counting_spawner(
    active: Arc<AtomicUsize>,
) -> impl FnOnce(CanonicalLogWorker) -> io::Result<JoinHandle<()>> {
    move |worker| {
        std::thread::Builder::new()
            .name("calque-canonical-log-test".to_owned())
            .spawn(move || {
                active.fetch_add(1, Ordering::SeqCst);
                let _active = ActiveWorker(active);
                worker();
            })
    }
}

fn runtime_separated_log() -> (SourceId, SourceEpoch, Arc<Mutex<RuntimeSeparatedLog>>) {
    let source_id = SourceId::new("source-1").unwrap();
    let source_epoch = SourceEpoch::new("epoch-1").unwrap();
    let log = Arc::new(Mutex::new(RuntimeSeparatedLog {
        source_id: source_id.clone(),
        source_epoch: source_epoch.clone(),
    }));
    (source_id, source_epoch, log)
}

#[test]
fn the_development_bearer_resolves_to_exactly_its_configured_subject() {
    let token = SecretToken::generate().unwrap();
    let credential = LocalCredentialPolicy::DevelopmentSharedBearer {
        token: *token.as_bytes(),
        principal: Principal::new(LOCAL_DEVELOPMENT_SUBJECT, BTreeMap::new()).unwrap(),
    };

    let principal = credential.resolve(&token.to_urlsafe(), 1).unwrap();
    // The subject must be the one `prepare_local_source` built the scope
    // for, or every request fails the built-scope check.
    assert_eq!(principal.subject(), LOCAL_DEVELOPMENT_SUBJECT);
    assert_eq!(
        credential.resolve("wrong", 1).unwrap_err(),
        BootstrapHttpError::Unauthorized
    );
}

#[test]
fn a_signed_grant_resolves_per_request_and_fails_closed() {
    let (issuer, _) = GrantIssuer::generate("local-key", "calque-local-test").unwrap();
    let credential = LocalCredentialPolicy::DatasetGrant(Box::new(
        GrantVerifier::new("calque-local-test", [issuer.verification_key().unwrap()]).unwrap(),
    ));
    let now = now_unix_seconds().unwrap();
    let token = issuer
        .issue("tenant-user", BTreeMap::new(), 4, now, 300)
        .unwrap();

    assert_eq!(
        credential.resolve(&token, 4).unwrap().subject(),
        "tenant-user"
    );
    // The same unexpired grant stops working the moment the policy
    // generation moves on.
    assert_eq!(
        credential.resolve(&token, 5).unwrap_err(),
        BootstrapHttpError::Unauthorized
    );
    // Every refusal is indistinguishable from outside.
    assert_eq!(
        credential.resolve("not-a-grant", 4).unwrap_err(),
        BootstrapHttpError::Unauthorized
    );
}

/// A re-encode is published only into the generation it was made under,
/// and only while that generation's artifact is ready.
#[test]
fn a_reencode_is_admitted_only_into_its_served_and_ready_generation() {
    let prepared =
        crate::snapshot::tests::prepared_source(SourceEpoch::new("reencode-epoch").unwrap());
    let generation = prepared.policy.generation();
    let serving = ServingAuthorization::new(prepared.policy, prepared.scope, ARTIFACT_READY);
    assert!(admits_reencode(&serving, generation));
    assert!(!admits_reencode(&serving, generation + 1));
    assert!(!admits_reencode(&serving, generation - 1));
    for state in [ARTIFACT_PREPARING, ARTIFACT_FAILED] {
        serving.set_artifact(state);
        assert!(!admits_reencode(&serving, generation), "{state}");
    }
}

/// A re-cut is published under the fence lock, only into the generation
/// it was cut for, and only while that generation's artifact is ready.
#[test]
fn a_re_cut_is_admitted_under_the_fence_lock_into_its_ready_generation() {
    let prepared =
        crate::snapshot::tests::prepared_source(SourceEpoch::new("recut-epoch").unwrap());
    let generation = prepared.policy.generation();
    let serving = Arc::new(StdRwLock::new(Arc::new(ServingAuthorization::new(
        prepared.policy,
        prepared.scope,
        ARTIFACT_READY,
    ))));
    let fence = Arc::new(RwLock::new(()));
    let admission_for = |generation| {
        let serving = Arc::clone(&serving);
        recut_admission(&fence, move || current_serving(&serving), generation)
    };
    let admission = admission_for(generation);
    assert!(Arc::ptr_eq(&admission.lock, &fence));
    assert!((admission.admit)());
    assert!(!(admission_for(generation + 1).admit)());
    for state in [ARTIFACT_PREPARING, ARTIFACT_FAILED] {
        current_serving(&serving).set_artifact(state);
        assert!(!(admission.admit)(), "{state}");
    }
}

/// A fence that replaces the served generation while a re-encode or its
/// packed brotli artifact waits to publish leaves that one unpublished;
/// with no fence, the same call publishes both.
#[tokio::test]
async fn a_fence_during_a_reencode_refuses_its_publication() {
    let registry = crate::test_support::registry();
    let policy_at = |generation| {
        AuthorizationPolicy::compile_for_registry(
            authz::PolicyDefinition {
                generation,
                tables: registry
                    .tables()
                    .iter()
                    .map(|table| authz::TablePermitDefinition {
                        table_id: table.table_id.clone(),
                        permitted_columns: table.column_ids.clone(),
                    })
                    .collect(),
            },
            &registry,
        )
        .unwrap()
    };
    let serving_at = |generation, artifact| {
        let policy = policy_at(generation);
        let scope = policy.scope();
        Arc::new(ServingAuthorization::new(policy, scope, artifact))
    };

    for fenced_at in [
        None,
        Some(ReencodeStage::Admitting),
        Some(ReencodeStage::BrotliAdmitting),
    ] {
        let fixture = crate::reencode::tests::published().await;
        let catalog_path = fixture.directory.path().join("catalog.sqlite3");
        let fence = RwLock::new(());
        let cancellation = CancellationToken::new();
        let serving = StdRwLock::new(serving_at(1, ARTIFACT_READY));
        let outcomes = reencode_generation(
            fixture.request(&catalog_path, &fence, &cancellation),
            &serving,
            1,
            |stage| {
                if fenced_at == Some(stage) {
                    // What `fence` does under the exclusive fence lock.
                    let _fence = fence.try_write().expect("nothing else holds the fence");
                    *serving.write().unwrap() = serving_at(2, ARTIFACT_PREPARING);
                }
            },
        )
        .await
        .unwrap();
        let published: Vec<_> = fixture
            .catalog()
            .published(&fixture.query)
            .unwrap()
            .iter()
            .map(|manifest| manifest.manifest().artifact_id.clone())
            .collect();
        match fenced_at {
            None => {
                assert_eq!(
                    outcomes.zstd,
                    ReencodeOutcome::Published {
                        artifact: fixture.reencoded_id()
                    }
                );
                assert!(matches!(
                    outcomes.brotli,
                    Some(Ok(ReencodeOutcome::Published { .. }))
                ));
                assert_eq!(published, [fixture.packed_id()]);
            }
            Some(ReencodeStage::Admitting) => {
                assert_eq!(outcomes.zstd, ReencodeOutcome::Refused);
                assert!(outcomes.brotli.is_none());
                assert_eq!(
                    published,
                    [fixture.published.manifest().artifact_id.clone()]
                );
            }
            Some(_) => {
                assert_eq!(
                    outcomes.zstd,
                    ReencodeOutcome::Published {
                        artifact: fixture.reencoded_id()
                    }
                );
                assert!(matches!(
                    outcomes.brotli,
                    Some(Ok(ReencodeOutcome::Refused))
                ));
                assert_eq!(published, [fixture.reencoded_id()]);
            }
        }
    }
}

fn mounted_tasks_and_projects() -> MountedCommands {
    let mut mounted = MountedCommands::default();
    mounted.mount(
        "tasks",
        "crud:public.tasks",
        BTreeSet::from([CommandOperation::Update, CommandOperation::Delete]),
    );
    mounted.mount(
        "projects",
        "crud:public.projects",
        BTreeSet::from([CommandOperation::Update]),
    );
    mounted
}

fn grant_credential(commands: GrantCommandCapabilities) -> ResolvedCredential {
    ResolvedCredential {
        principal: Principal::new("tenant-user", BTreeMap::new()).unwrap(),
        expires_at_unix_seconds: Some(1_000),
        commands,
    }
}

/// The capability a grant reaches here is the intersection of what it
/// signed and what this deployment mounts. A grant can only ever narrow.
#[test]
fn a_signed_capability_is_narrowed_by_what_this_deployment_mounts() {
    let (issuer, _) = GrantIssuer::generate("local-key", "dataset-a").unwrap();
    let verifier = GrantVerifier::new("dataset-a", [issuer.verification_key().unwrap()]).unwrap();
    let credential = LocalCredentialPolicy::DatasetGrant(Box::new(verifier));
    let policy = MutationCommandPolicy::SignedGrant(mounted_tasks_and_projects());
    let now = now_unix_seconds().unwrap();

    // A grant naming an unmounted collection, an unmounted operation on a
    // mounted one, and one operation this deployment does mount.
    let token = issuer
        .issue_with_commands(
            "tenant-user",
            BTreeMap::new(),
            vec![
                dataset_grant::GrantCommandCapability {
                    collection: "tasks".to_owned(),
                    operations: vec![
                        GrantCommandOperation::Update,
                        // Mounted for tasks nowhere below, so it drops.
                        GrantCommandOperation::Insert,
                    ],
                },
                dataset_grant::GrantCommandCapability {
                    collection: "secrets".to_owned(),
                    operations: vec![GrantCommandOperation::Delete],
                },
            ],
            4,
            now,
            300,
        )
        .unwrap();
    let resolved = credential.resolve(&token, 4).unwrap();
    let capability = policy.capability_for(&resolved).unwrap();

    assert!(capability.permits("crud:public.tasks", CommandOperation::Update));
    // Mounted here but not granted.
    assert!(!capability.permits("crud:public.tasks", CommandOperation::Delete));
    // Granted but not mounted here.
    assert!(!capability.permits("crud:public.tasks", CommandOperation::Insert));
    // Named by neither side.
    assert!(!capability.permits("crud:public.secrets", CommandOperation::Delete));
    // A collection this deployment mounts that the grant never named
    // contributes nothing, so it is not in the capability at all.
    assert_eq!(
        capability.command_names().collect::<Vec<_>>(),
        vec!["crud:public.tasks"]
    );
}

/// A grant carrying no command capability reaches no write surface and
/// gets `NotFound`, the answer an absent route gives.
#[test]
fn a_grant_without_a_capability_finds_no_write_surface() {
    let (issuer, _) = GrantIssuer::generate("local-key", "dataset-a").unwrap();
    let credential = LocalCredentialPolicy::DatasetGrant(Box::new(
        GrantVerifier::new("dataset-a", [issuer.verification_key().unwrap()]).unwrap(),
    ));
    let now = now_unix_seconds().unwrap();
    let token = issuer
        .issue("tenant-user", BTreeMap::new(), 4, now, 300)
        .unwrap();
    let resolved = credential.resolve(&token, 4).unwrap();

    assert_eq!(
        MutationCommandPolicy::SignedGrant(mounted_tasks_and_projects())
            .capability_for(&resolved)
            .unwrap_err(),
        MutationHttpError::NotFound
    );

    // Even a fully capable grant finds nothing when nothing is mounted.
    let capable = grant_credential(
        GrantVerifier::new("dataset-a", [issuer.verification_key().unwrap()])
            .unwrap()
            .verify(&GrantPresentation {
                token: &issuer
                    .issue_with_commands(
                        "tenant-user",
                        BTreeMap::new(),
                        vec![dataset_grant::GrantCommandCapability {
                            collection: "tasks".to_owned(),
                            operations: vec![GrantCommandOperation::Update],
                        }],
                        4,
                        now,
                        300,
                    )
                    .unwrap(),
                now_unix_seconds: now,
                authorization_generation: 4,
            })
            .unwrap()
            .commands()
            .clone(),
    );
    assert_eq!(
        MutationCommandPolicy::SignedGrant(MountedCommands::default())
            .capability_for(&capable)
            .unwrap_err(),
        MutationHttpError::NotFound
    );
    assert!(
        MutationCommandPolicy::SignedGrant(mounted_tasks_and_projects())
            .capability_for(&capable)
            .unwrap()
            .permits("crud:public.tasks", CommandOperation::Update)
    );
}

/// The shared bearer carries the whole mounted surface and nothing wider:
/// it is enumerated, not a wildcard, so an empty one still refuses.
#[test]
fn the_development_shared_bearer_carries_exactly_what_it_enumerates() {
    let capability = CommandCapability::new([(
        "crud:public.tasks".to_owned(),
        BTreeSet::from([CommandOperation::Update]),
    )]);
    let credential = grant_credential(GrantCommandCapabilities::default());
    let policy = MutationCommandPolicy::SharedBearer(capability);
    let resolved = policy.capability_for(&credential).unwrap();
    assert!(resolved.permits("crud:public.tasks", CommandOperation::Update));
    assert!(!resolved.permits("crud:public.tasks", CommandOperation::Delete));
    assert_eq!(
        MutationCommandPolicy::SharedBearer(CommandCapability::default())
            .capability_for(&credential)
            .unwrap_err(),
        MutationHttpError::NotFound
    );
}

/// The startup contract's rendering is the mounted table, not the manifest
/// read a second time.
#[test]
fn the_mounted_command_contract_names_every_collection_and_operation() {
    assert_eq!(
        mounted_tasks_and_projects().contract_value(),
        "projects:update,tasks:update+delete"
    );
    assert!(MountedCommands::default().by_collection.is_empty());
    // A collection whose operation set is empty mounts nothing at all.
    let mut empty = MountedCommands::default();
    empty.mount("tasks", "crud:public.tasks", BTreeSet::new());
    assert!(empty.by_collection.is_empty());
    assert_eq!(empty.contract_value(), "");
}

#[test]
fn the_credential_resolver_refuses_an_expired_grant() {
    let (issuer, _) = GrantIssuer::generate("local-key", "dataset-a").unwrap();
    let credential = LocalCredentialPolicy::DatasetGrant(Box::new(
        GrantVerifier::new("dataset-a", [issuer.verification_key().unwrap()]).unwrap(),
    ));
    let token = issuer
        .issue("tenant-user", BTreeMap::new(), 4, 1_000, 300)
        .unwrap();

    assert_eq!(
        credential.resolve_at(&token, 4, 1_300).unwrap_err(),
        BootstrapHttpError::Unauthorized
    );
}

#[test]
fn the_credential_resolver_refuses_a_grant_for_another_dataset() {
    let (issuer, _) = GrantIssuer::generate("local-key", "dataset-b").unwrap();
    let credential = LocalCredentialPolicy::DatasetGrant(Box::new(
        GrantVerifier::new("dataset-a", [issuer.verification_key().unwrap()]).unwrap(),
    ));
    let token = issuer
        .issue("tenant-user", BTreeMap::new(), 4, 1_000, 300)
        .unwrap();

    assert_eq!(
        credential.resolve_at(&token, 4, 1_001).unwrap_err(),
        BootstrapHttpError::Unauthorized
    );
}

#[test]
fn the_credential_resolver_refuses_a_grant_for_another_authorization_generation() {
    let (issuer, _) = GrantIssuer::generate("local-key", "dataset-a").unwrap();
    let credential = LocalCredentialPolicy::DatasetGrant(Box::new(
        GrantVerifier::new("dataset-a", [issuer.verification_key().unwrap()]).unwrap(),
    ));
    let token = issuer
        .issue("tenant-user", BTreeMap::new(), 4, 1_000, 300)
        .unwrap();

    assert_eq!(
        credential.resolve_at(&token, 5, 1_001).unwrap_err(),
        BootstrapHttpError::Unauthorized
    );
}

#[test]
fn replay_of_a_grant_signed_by_a_revoked_key_is_rejected() {
    let (revoked, _) = GrantIssuer::generate("revoked-key", "dataset-a").unwrap();
    let (current, _) = GrantIssuer::generate("current-key", "dataset-a").unwrap();
    let token = revoked
        .issue("tenant-user", BTreeMap::new(), 4, 1_000, 300)
        .unwrap();
    let credential = LocalCredentialPolicy::DatasetGrant(Box::new(
        GrantVerifier::new("dataset-a", [current.verification_key().unwrap()]).unwrap(),
    ));

    assert_eq!(
        credential.resolve_at(&token, 4, 1_001).unwrap_err(),
        BootstrapHttpError::Unauthorized
    );
}

#[test]
fn capability_lifetime_is_capped_before_the_verified_grant_expiry() {
    let (issuer, _) = GrantIssuer::generate("local-key", "dataset-a").unwrap();
    let credential = LocalCredentialPolicy::DatasetGrant(Box::new(
        GrantVerifier::new("dataset-a", [issuer.verification_key().unwrap()]).unwrap(),
    ));
    let token = issuer
        .issue("tenant-user", BTreeMap::new(), 4, 1_000, 300)
        .unwrap();
    let resolved = credential.resolve_at(&token, 4, 1_001).unwrap();

    assert_eq!(
        resolved.capability_ttl(Duration::from_secs(3_600), 1_001),
        Some(Duration::from_secs(298))
    );
    assert_eq!(
        resolved.capability_ttl(Duration::from_secs(60), 1_001),
        Some(Duration::from_secs(60))
    );
    assert_eq!(
        resolved.capability_ttl(Duration::from_secs(3_600), 1_300),
        None
    );
}

/// One plan grant of the kind `bootstrap` commits, expiring at `expires_at`.
///
/// `capability_ttl` caps this at the verified grant's own expiry, so a
/// grant that outlives its dataset grant cannot exist; what this builds is
/// the record `changes` resolves against.
const PLAN_TTL: Duration = Duration::from_secs(60 * 60);
const PLAN_TTL_MICROS: i64 = 3_600_000_000;

fn plan_key(byte: u8) -> PlanKey {
    use base64::Engine as _;
    PlanKey::new(
        &SecretToken::from_urlsafe(
            &base64::engine::general_purpose::URL_SAFE_NO_PAD.encode([byte; 32]),
        )
        .unwrap(),
    )
}

fn plans_for(prepared: &PreparedLocalSource, key: u8, generation: u64) -> Plans {
    Plans {
        tokens: PlanTokens::new(
            plan_key(key),
            SourceId::new("source-1").unwrap(),
            prepared.activated.source_epoch().clone(),
        ),
        verified: VerifiedPlans::new(DEFAULT_MAX_ACTIVE_PLANS),
        retirements: StdRwLock::new(Retirements::new(generation, PLAN_TTL)),
        source_id: SourceId::new("source-1").unwrap(),
        activated: prepared.activated.clone(),
    }
}

/// One process's plans, serving the generation `prepared` was compiled at.
struct Fixture {
    prepared: PreparedLocalSource,
    serving: Arc<ServingAuthorization>,
    plans: Plans,
}

fn fixture() -> Fixture {
    let prepared = crate::snapshot::tests::prepared_source(SourceEpoch::new("plan-epoch").unwrap());
    let plans = plans_for(&prepared, 1, prepared.policy.generation());
    Fixture {
        serving: Arc::new(ServingAuthorization::new(
            prepared.policy.clone(),
            prepared.scope.clone(),
            ARTIFACT_READY,
        )),
        prepared,
        plans,
    }
}

impl Fixture {
    fn generation(&self) -> u64 {
        self.prepared.policy.generation()
    }

    fn scope_hash(&self) -> [u8; 32] {
        *self.prepared.scope.scope_hash().as_bytes()
    }

    fn cursor(&self, sequence: u64) -> OpaqueCursor {
        self.prepared
            .activated
            .cursor_codec()
            .encode(&CursorPosition {
                source_epoch: self.prepared.activated.source_epoch().clone(),
                sequence,
            })
    }

    fn table(&self) -> TableId {
        self.prepared
            .scope
            .permitted_tables()
            .next()
            .expect("the source-wide scope permits the enrolled table")
            .clone()
    }

    fn claims_for(&self, plan_id: &str, scope: &AuthorizedScope, expires_at: i64) -> PlanClaims {
        self.plans.tokens.claims(
            &PlanTerms {
                plan_id: PlanId::new(plan_id).unwrap(),
                from_cursor: self.cursor(1),
                through_cursor: self.cursor(1),
                table_starts: BTreeMap::from([(self.table(), Some(self.cursor(1)))]),
            },
            scope,
            expires_at,
            "client-1",
            "device-1",
        )
    }

    fn token(&self, plan_id: &str, expires_at: i64) -> String {
        self.plans
            .sign(&self.claims_for(plan_id, &self.prepared.scope, expires_at))
            .expect("the generation is in force")
    }

    /// A catch-up request of `plan_id` from the plan's start, or a live one.
    fn request(
        &self,
        plan_id: &str,
        token: &str,
        device_id: &str,
        start: Option<u64>,
    ) -> IncomingChangeRequest {
        let cursor = |sequence| sync_protocol::change::Cursor {
            token: self.cursor(sequence).as_str().as_bytes().to_vec(),
        };
        change_api::parse_change_request(sync_protocol::change::ChangeRequest {
            protocol_version: 1,
            from_cursor_exclusive: Some(cursor(1)),
            through_cursor_inclusive: start.map(|_| cursor(1)),
            tables: vec![sync_protocol::change::TableSubscription {
                table_id: self.table().to_string(),
                shard_snapshot_cursor: start.map(cursor),
            }],
            authorization_scope_token: token.as_bytes().to_vec(),
            max_batch_bytes: 1024 * 1024,
            max_transactions: 10,
            long_poll_timeout_ms: 0,
            client_instance_id: "client-1".to_owned(),
            device_id: device_id.to_owned(),
            pending_mutation_ids: Vec::new(),
            accept_transaction_fragmentation: true,
            accept_revocation_barrier: true,
            plan_id: plan_id.to_owned(),
            stream: false,
        })
        .unwrap()
    }

    fn resolve_request(
        &self,
        incoming: &IncomingChangeRequest,
        now: i64,
    ) -> Result<ResolvedPlan, ChangeBatchError> {
        self.plans
            .resolve(&self.serving, incoming, incoming.request(), now)
    }

    fn resolve(&self, token: &str, now: i64) -> Result<ResolvedPlan, ChangeBatchError> {
        self.resolve_request(&self.request("plan-1", token, "device-1", Some(1)), now)
    }

    fn revocation(&self, token: &str) -> PlanRevocation {
        self.resolve(token, 1_000_002)
            .unwrap()
            .revocation
            .expect("the plan is fenced")
    }

    fn write(&self, token: &str, device_id: &str, now: i64) -> Result<(), MutationHttpError> {
        self.plans.authorize_write(
            token.as_bytes(),
            "client-1",
            device_id,
            &self.prepared.scope,
            now,
        )
    }

    /// A fence as `fence_generation` makes it, with the scopes the store
    /// recorded as issued.
    fn fence(&self, scopes: &[[u8; 32]], head: u64, now: i64) -> Vec<RevocationBarrier> {
        let mut retirements = self.plans.retirements_mut();
        let revoked = retirements.generation;
        let barriers = scopes
            .iter()
            .map(|scope| {
                draft_barrier(
                    revoked,
                    revoked + 1,
                    *scope,
                    &head_cursor(),
                    u64::try_from(now).unwrap(),
                )
            })
            .collect();
        retirements.retire(
            revoked + 1,
            StoredRetirement {
                revoked_generation: revoked,
                effective_generation: revoked + 1,
                ceiling: head,
                barriers,
            },
            Arc::clone(&self.serving),
            |_| None,
        );
        retirements.drafts(revoked)
    }

    fn attach(
        &self,
        signed: &[SignedBarrier],
        trusted_key: impl Fn(&str) -> Option<[u8; 32]>,
        now: i64,
    ) -> Result<(usize, usize), RevocationError> {
        let verified = self
            .plans
            .retirements()
            .verify_signed(signed, trusted_key)?;
        Ok(self.plans.retirements_mut().attach(verified, now))
    }
}

/// A plan resolves from its token until the expiry it signs and never
/// after, for change requests and writes alike.
#[test]
fn a_plan_resolves_from_its_token_until_it_expires() {
    let fixture = fixture();
    let expires_at = 2_000_000;
    let token = fixture.token("plan-1", expires_at);
    let resolved = fixture.resolve(&token, expires_at - 1).unwrap();
    assert!(resolved.revocation.is_none());
    assert_eq!(
        resolved.opened.plan.scope.scope_hash(),
        fixture.prepared.scope.scope_hash()
    );
    assert_eq!(resolved.opened.claims.expires_at_unix_micros, expires_at);
    assert!(fixture.write(&token, "device-1", expires_at - 1).is_ok());
    for now in [expires_at, expires_at + 60_000_000] {
        assert!(matches!(
            fixture.resolve(&token, now),
            Err(ChangeBatchError::InvalidPlanBinding)
        ));
        assert_eq!(
            fixture.write(&token, "device-1", now),
            Err(MutationHttpError::InvalidRequest)
        );
    }
}

/// Nothing in a token is read until its tag verifies under this
/// deployment's key, and a plan of another epoch is stale.
#[test]
fn a_token_is_refused_unless_this_deployment_signed_it() {
    use base64::Engine as _;
    let fixture = fixture();
    let token = fixture.token("plan-1", 2_000_000);
    let (payload, tag) = token.split_once('.').unwrap();
    let flip = |text: &str| {
        let mut bytes = base64::engine::general_purpose::URL_SAFE_NO_PAD
            .decode(text)
            .unwrap();
        let middle = bytes.len() / 2;
        bytes[middle] ^= 1;
        base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(bytes)
    };
    let other_key = plans_for(&fixture.prepared, 2, fixture.generation());
    let other_key_token = other_key
        .sign(&fixture.claims_for("plan-1", &fixture.prepared.scope, 2_000_000))
        .unwrap();
    for forged in [
        format!("{}.{tag}", flip(payload)),
        format!("{payload}.{}", flip(tag)),
        other_key_token,
    ] {
        assert!(matches!(
            fixture.resolve(&forged, 1_000_000),
            Err(ChangeBatchError::InvalidAuthorizationToken)
        ));
        assert_eq!(
            fixture.write(&forged, "device-1", 1_000_000),
            Err(MutationHttpError::InvalidRequest)
        );
    }

    let other_epoch =
        crate::snapshot::tests::prepared_source(SourceEpoch::new("another-epoch").unwrap());
    let restarted = plans_for(&other_epoch, 1, fixture.generation());
    let incoming = fixture.request("plan-1", &token, "device-1", Some(1));
    assert!(matches!(
        restarted.resolve(&fixture.serving, &incoming, incoming.request(), 1_000_000),
        Err(ChangeBatchError::InvalidPlanBinding)
    ));
}

/// The bootstrap names the device, so no change request binds one: a
/// request from another device, client instance or plan is refused.
#[test]
fn a_plan_is_bound_to_the_client_and_device_its_bootstrap_named() {
    let fixture = fixture();
    let token = fixture.token("plan-1", 2_000_000);
    for incoming in [
        fixture.request("plan-1", &token, "device-2", Some(1)),
        fixture.request("plan-2", &token, "device-1", Some(1)),
    ] {
        assert!(matches!(
            fixture.resolve_request(&incoming, 1_000_000),
            Err(ChangeBatchError::InvalidPlanBinding)
        ));
    }
    let mut claims = fixture.claims_for("plan-1", &fixture.prepared.scope, 2_000_000);
    claims = fixture.plans.tokens.claims(
        &PlanTerms {
            plan_id: PlanId::new("plan-1").unwrap(),
            from_cursor: fixture.cursor(1),
            through_cursor: fixture.cursor(1),
            table_starts: BTreeMap::from([(fixture.table(), Some(fixture.cursor(1)))]),
        },
        &fixture.prepared.scope,
        claims.expires_at_unix_micros,
        "client-2",
        "device-1",
    );
    let other_client = fixture.plans.sign(&claims).unwrap();
    assert!(matches!(
        fixture.resolve(&other_client, 1_000_000),
        Err(ChangeBatchError::InvalidPlanBinding)
    ));
    assert_eq!(
        fixture.write(&token, "device-2", 1_000_000),
        Err(MutationHttpError::InvalidRequest)
    );
    assert!(fixture.write(&token, "device-1", 1_000_000).is_ok());
}

/// A catch-up request names the plan's tables at their starts; a live one
/// names its tables.
#[test]
fn a_request_names_the_tables_its_plan_signed() {
    let fixture = fixture();
    let token = fixture.token("plan-1", 2_000_000);
    assert!(fixture
        .resolve_request(
            &fixture.request("plan-1", &token, "device-1", None),
            1_000_000
        )
        .is_ok());
    assert!(matches!(
        fixture.resolve_request(
            &fixture.request("plan-1", &token, "device-1", Some(2)),
            1_000_000
        ),
        Err(ChangeBatchError::InvalidPlanBinding)
    ));
    let resolved = fixture.resolve(&token, 1_000_000).unwrap();
    assert_eq!(
        resolved.opened.table_starts,
        BTreeMap::from([(fixture.table(), Some(fixture.cursor(1)))])
    );
}

/// A bootstrap that verified its grant before a fence and signs after it
/// must not hand out a plan of the generation the fence retired.
#[test]
fn a_plan_of_a_fenced_generation_is_never_signed() {
    let fixture = fixture();
    let claims = fixture.claims_for("plan-1", &fixture.prepared.scope, 2_000_000);
    assert!(fixture.plans.sign(&claims).is_some());
    fixture.fence(&[fixture.scope_hash()], 7, 1_000_000);
    assert_eq!(fixture.plans.sign(&claims), None);
}

/// A fence retires the generation once: one draft for each scope issued
/// under it, standing on the head it read. Every plan of the scope is
/// capped at that head, and a later fence leaves them where the first
/// one put them.
#[test]
fn a_fence_caps_every_plan_of_its_scope_at_the_head_it_read() {
    let fixture = fixture();
    let generation = fixture.generation();
    let first = fixture.token("plan-1", 2_000_000);
    let second = fixture.token("plan-2", 2_000_000);
    let drafts = fixture.fence(&[fixture.scope_hash()], 7, 1_000_001);
    assert_eq!(drafts.len(), 1);
    let draft = &drafts[0];
    assert_eq!(draft.barrier_cursor, Some(head_cursor()));
    assert_eq!(draft.revoked_authorization_generation, generation);
    assert_eq!(draft.effective_authorization_generation, generation + 1);
    assert_eq!(draft.revoked_authorization_scope_hash, fixture.scope_hash());
    assert!(draft.signature.is_empty() && draft.signing_key_id.is_empty());

    let revocation = fixture.revocation(&first);
    assert_eq!(revocation.ceiling, 7);
    assert!(revocation.signed.is_none());
    let sibling = fixture
        .resolve_request(
            &fixture.request("plan-2", &second, "device-1", Some(1)),
            1_000_002,
        )
        .unwrap();
    assert_eq!(sibling.revocation.unwrap().ceiling, 7);

    assert_eq!(fixture.plans.retirements().drafts(generation), drafts);
    assert!(fixture.fence(&[], 9, 1_000_003).is_empty());
    assert_eq!(fixture.revocation(&first).ceiling, 7);
}

/// A plan of a retired generation is served only through that
/// generation's retirement and only for a scope it drafted a barrier
/// for. A generation this process serves no policy of is refused.
#[test]
fn a_plan_of_a_generation_without_its_retirement_or_scope_is_refused() {
    let fixture = fixture();
    let generation = fixture.generation();
    let token = fixture.token("plan-1", 2_000_000);
    let mut future = fixture.claims_for("plan-1", &fixture.prepared.scope, 2_000_000);
    future.generation = generation + 1;
    let future = fixture.plans.tokens.sign(&future);
    let expired = |token: &str| {
        matches!(
            fixture.resolve(token, 1_000_000),
            Err(ChangeBatchError::AuthorizationExpired)
        )
    };
    assert!(expired(&future));

    // New plans moved on before the policy they are served from did.
    fixture.plans.retirements_mut().generation = generation + 1;
    assert!(expired(&future));
    assert!(expired(&token));

    fixture.plans.retirements_mut().hold(
        StoredRetirement {
            revoked_generation: generation,
            effective_generation: generation + 1,
            ceiling: 7,
            barriers: vec![draft_barrier(
                generation,
                generation + 1,
                [9; 32],
                &head_cursor(),
                1,
            )],
        },
        Arc::clone(&fixture.serving),
        |_| None,
    );
    assert!(expired(&token));

    let fenced = self::fixture();
    fenced.fence(&[fenced.scope_hash()], 7, 1_000_000);
    let mut unretired = fenced.claims_for("plan-1", &fenced.prepared.scope, 2_000_000);
    unretired.generation = generation + 5;
    assert!(matches!(
        fenced.resolve(&fenced.plans.tokens.sign(&unretired), 1_000_000),
        Err(ChangeBatchError::AuthorizationExpired)
    ));
}

/// A request of a fenced plan may stand on the ceiling, where its barrier
/// is, and not past it.
#[test]
fn a_fenced_plan_may_start_at_its_ceiling_and_not_past_it() {
    let fixture = fixture();
    assert!(!starts_past(
        &fixture.prepared.activated,
        &fixture.cursor(7),
        7
    ));
    assert!(starts_past(
        &fixture.prepared.activated,
        &fixture.cursor(8),
        7
    ));
}

#[test]
fn a_fenced_plan_takes_no_write() {
    let fixture = fixture();
    let token = fixture.token("plan-1", 2_000_000);
    assert!(fixture.write(&token, "device-1", 1_000_001).is_ok());
    fixture.fence(&[fixture.scope_hash()], 7, 1_000_001);
    assert_eq!(
        fixture.write(&token, "device-1", 1_000_002),
        Err(MutationHttpError::Unauthorized)
    );
}

/// A plan of one scope takes no write a grant of another scope signs.
#[test]
fn a_write_under_a_scope_its_plan_was_not_signed_for_is_forbidden() {
    let fixture = fixture();
    let token = fixture.token("plan-1", 2_000_000);
    let other = other_scope();
    assert_eq!(
        fixture
            .plans
            .authorize_write(token.as_bytes(), "client-1", "device-1", &other, 1_000_000),
        Err(MutationHttpError::Forbidden)
    );
}

fn other_scope() -> AuthorizedScope {
    crate::snapshot::tests::prepared_source_for(
        SourceEpoch::new("plan-epoch").unwrap(),
        crate::test_support::registry_for(vec![crate::test_support::inspection(
            "notes",
            16_385,
            vec![crate::test_support::column(
                1,
                "id",
                2_950,
                "uuid",
                false,
                Some(1),
            )],
        )]),
    )
    .scope
}

/// The cache only saves rebuilding a plan's scope: every request verifies
/// its token, and a token whose scope the cached entry does not hold is
/// checked against the scope this process rebuilds.
#[test]
fn a_cached_plan_is_verified_again_on_every_request() {
    use base64::Engine as _;
    let fixture = fixture();
    let token = fixture.token("plan-1", 2_000_000);
    fixture.resolve(&token, 1_000_000).unwrap();
    assert_eq!(fixture.plans.verified.len(), 1);

    let (payload, tag) = token.split_once('.').unwrap();
    let mut tag = base64::engine::general_purpose::URL_SAFE_NO_PAD
        .decode(tag)
        .unwrap();
    tag[0] ^= 1;
    let forged = format!(
        "{payload}.{}",
        base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(tag)
    );
    assert!(matches!(
        fixture.resolve(&forged, 1_000_000),
        Err(ChangeBatchError::InvalidAuthorizationToken)
    ));

    let other = other_scope();
    assert_eq!(other.generation(), fixture.generation());
    let widened = fixture
        .plans
        .sign(&fixture.claims_for("plan-1", &other, 2_000_000))
        .unwrap();
    assert!(matches!(
        fixture.resolve(&widened, 1_000_000),
        Err(ChangeBatchError::InvalidPlanBinding)
    ));
}

/// A restart on the same state directory keeps every plan, and the
/// retirements it loads keep every fenced plan at its ceiling. A restart
/// that did not load them would refuse the fenced plans.
#[test]
fn a_restart_keeps_every_plan_and_every_fence_it_loads() {
    let fixture = fixture();
    let generation = fixture.generation();
    let token = fixture.token("plan-1", 2_000_000);
    let incoming = fixture.request("plan-1", &token, "device-1", Some(1));

    let restarted = plans_for(&fixture.prepared, 1, generation);
    let resolved = restarted
        .resolve(&fixture.serving, &incoming, incoming.request(), 1_000_000)
        .unwrap();
    assert!(resolved.revocation.is_none());

    let drafts = fixture.fence(&[fixture.scope_hash()], 7, 1_000_001);
    let stored = StoredRetirement {
        revoked_generation: generation,
        effective_generation: generation + 1,
        ceiling: 7,
        barriers: drafts,
    };
    let fenced = plans_for(&fixture.prepared, 1, generation + 1);
    assert!(matches!(
        fenced.resolve(&fixture.serving, &incoming, incoming.request(), 1_000_002),
        Err(ChangeBatchError::AuthorizationExpired)
    ));
    fenced.retirements_mut().hold(
        stored,
        Arc::new(ServingAuthorization::new(
            fixture.prepared.policy.clone(),
            fixture.prepared.scope.clone(),
            ARTIFACT_FAILED,
        )),
        |_| None,
    );
    let resolved = fenced
        .resolve(&fixture.serving, &incoming, incoming.request(), 1_000_002)
        .unwrap();
    assert_eq!(resolved.revocation.unwrap().ceiling, 7);
}

/// A later build of a request reads what its token proved from the plan the
/// first build opened, and is refused from the token's expiry on.
#[test]
fn a_held_plan_is_refused_from_its_expiry() {
    let fixture = fixture();
    let token = fixture.token("plan-1", 2_000_000);
    let incoming = fixture.request("plan-1", &token, "device-1", Some(1));
    let mut held = None;
    let build = |held: &mut Option<OpenedPlan>, now| {
        fixture
            .plans
            .for_build(held, &fixture.serving, &incoming, incoming.request(), now)
            .map(|(_, revocation)| revocation.is_none())
    };

    assert!(matches!(build(&mut held, 1_000_000), Ok(true)));
    assert!(held.is_some());
    assert!(matches!(build(&mut held, 1_999_999), Ok(true)));
    assert!(matches!(
        build(&mut held, 2_000_000),
        Err(ChangeBatchError::InvalidPlanBinding)
    ));
}

/// A fence that lands after the plan was opened applies to its next build:
/// the plan reads up to the fence's ceiling, or is refused when the fence
/// drafted no barrier for its scope.
#[test]
fn a_held_plan_meets_a_fence_that_landed_after_it_was_opened() {
    for (scopes, ceiling) in [(vec![fixture().scope_hash()], Some(7)), (Vec::new(), None)] {
        let fixture = fixture();
        let token = fixture.token("plan-1", 2_000_000);
        let incoming = fixture.request("plan-1", &token, "device-1", Some(1));
        let mut held = None;
        let opened = fixture
            .plans
            .for_build(
                &mut held,
                &fixture.serving,
                &incoming,
                incoming.request(),
                1_000_000,
            )
            .unwrap();
        assert!(opened.1.is_none());

        fixture.fence(&scopes, 7, 1_000_001);
        let built = fixture.plans.for_build(
            &mut held,
            &fixture.serving,
            &incoming,
            incoming.request(),
            1_000_002,
        );
        match ceiling {
            Some(ceiling) => assert_eq!(built.unwrap().1.unwrap().ceiling, ceiling),
            None => assert!(matches!(built, Err(ChangeBatchError::AuthorizationExpired))),
        }
    }
}

/// A plan opened under a generation that the serving authorization has moved
/// past is refused, as a new request with its token would be.
#[test]
fn a_held_plan_is_refused_once_its_generation_stops_being_served() {
    let fixture = fixture();
    let token = fixture.token("plan-1", 2_000_000);
    let incoming = fixture.request("plan-1", &token, "device-1", Some(1));
    let mut held = None;
    fixture
        .plans
        .for_build(
            &mut held,
            &fixture.serving,
            &incoming,
            incoming.request(),
            1_000_000,
        )
        .unwrap();

    let moved = plans_for(&fixture.prepared, 1, fixture.generation() + 1);
    assert!(matches!(
        moved.for_build(
            &mut held,
            &fixture.serving,
            &incoming,
            incoming.request(),
            1_000_001,
        ),
        Err(ChangeBatchError::AuthorizationExpired)
    ));
}

const BARRIER_AUDIENCE: &str = "calque-revocation-test";

fn head_cursor() -> Cursor {
    Cursor {
        token: b"cursor-at-head".to_vec(),
    }
}

fn signer(key_id: &str) -> (GrantIssuer, GrantVerifier) {
    let (issuer, _) = GrantIssuer::generate(key_id, BARRIER_AUDIENCE).unwrap();
    let verifier =
        GrantVerifier::new(BARRIER_AUDIENCE, [issuer.verification_key().unwrap()]).unwrap();
    (issuer, verifier)
}

fn signed(issuer: &GrantIssuer, draft: &RevocationBarrier) -> SignedBarrier {
    use base64::Engine as _;
    let mut barrier = draft.clone();
    issuer.sign_revocation_barrier(&mut barrier).unwrap();
    SignedBarrier {
        barrier_id: barrier.barrier_id,
        signing_key_id: barrier.signing_key_id,
        signature: base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(barrier.signature),
    }
}

/// Only the application's signature over exactly the draft Calque holds,
/// by a key this deployment trusts, attaches; one bad signature in a
/// batch attaches none of it; and one signature serves every plan of the
/// scope.
#[test]
fn a_barrier_attaches_only_with_a_trusted_signature_over_its_draft() {
    let fixture = fixture();
    let first = fixture.token("plan-1", 2_000_000);
    let second = fixture.token("plan-2", 2_000_000);
    let drafts = fixture.fence(&[fixture.scope_hash(), [9; 32]], 7, 1_000_001);
    let draft = drafts
        .iter()
        .find(|draft| draft.revoked_authorization_scope_hash == fixture.scope_hash())
        .unwrap();
    let (issuer, verifier) = signer("app-key-1");
    let trusted = |key_id: &str| verifier.public_key(key_id);
    let good = signed(&issuer, draft);

    // Another key pair under the trusted key id.
    let (impostor, _) = signer("app-key-1");
    let forged = signed(&impostor, draft);
    // A key id this deployment does not hold.
    let (stranger, _) = signer("app-key-2");
    let unknown = signed(&stranger, draft);
    // The trusted key over a draft Calque never issued.
    let mut widened = draft.clone();
    widened.barrier_id = "barrier-chosen-by-the-caller".to_owned();
    let foreign_id = signed(&issuer, &widened);
    let mut moved = draft.clone();
    moved.barrier_cursor = Some(Cursor {
        token: b"cursor-elsewhere".to_vec(),
    });
    let moved_cursor = signed(&issuer, &moved);
    for refused in [forged, unknown, foreign_id, moved_cursor] {
        assert_eq!(
            fixture.attach(&[good.clone(), refused], trusted, 1_000_002),
            Err(RevocationError::Unauthorized)
        );
        assert!(
            fixture.revocation(&first).signed.is_none(),
            "a refused batch attached a barrier"
        );
    }

    assert_eq!(
        fixture.attach(std::slice::from_ref(&good), trusted, 1_000_002),
        Ok((1, 0))
    );
    let minted = fixture
        .revocation(&first)
        .signed
        .expect("the verified barrier is attached");
    assert_eq!(minted.barrier.signing_key_id, "app-key-1");
    assert_eq!(minted.barrier.barrier_id, draft.barrier_id);
    let sibling = fixture
        .resolve_request(
            &fixture.request("plan-2", &second, "device-1", Some(1)),
            1_000_003,
        )
        .unwrap()
        .revocation
        .unwrap()
        .signed
        .expect("one signature serves every plan of the scope");
    assert_eq!(sibling.barrier, minted.barrier);

    // Once every plan the fence retired has expired, a signature is
    // counted, not refused.
    let fenced_at = 1_000_001;
    assert_eq!(
        fixture.attach(
            std::slice::from_ref(&good),
            trusted,
            fenced_at + PLAN_TTL_MICROS - 1
        ),
        Ok((1, 0))
    );
    assert_eq!(
        fixture.attach(&[good], trusted, fenced_at + PLAN_TTL_MICROS),
        Ok((0, 1))
    );
}

/// The application attaches the barrier it names. A valid signature over
/// the draft Calque issued, sent under another barrier id, is refused
/// rather than attached as the draft it happens to sign.
#[tokio::test]
async fn a_signed_draft_sent_under_another_barrier_id_is_refused() {
    use axum::response::IntoResponse as _;
    let fixture = fixture();
    let token = fixture.token("plan-1", 2_000_000);
    let drafts = fixture.fence(&[fixture.scope_hash()], 7, 1_000_001);
    let (issuer, verifier) = signer("app-key-1");
    let mut misnamed = signed(&issuer, &drafts[0]);
    misnamed.barrier_id = "barrier-chosen-by-the-caller".to_owned();

    let refused = fixture
        .attach(&[misnamed], |key_id| verifier.public_key(key_id), 1_000_002)
        .unwrap_err();
    assert_eq!(refused, RevocationError::Unauthorized);
    let response = refused.into_response();
    assert_eq!(response.status(), axum::http::StatusCode::UNAUTHORIZED);
    let body = axum::body::to_bytes(response.into_body(), usize::MAX)
        .await
        .unwrap();
    assert_eq!(
        serde_json::from_slice::<serde_json::Value>(&body).unwrap(),
        serde_json::json!({ "code": "UNAUTHORIZED" })
    );
    assert!(fixture.revocation(&token).signed.is_none());
}

/// A batch that is empty, longer than the drafts, or carries a signature
/// that does not parse is malformed, and attaches nothing.
#[test]
fn a_malformed_barrier_batch_is_invalid_and_attaches_nothing() {
    let fixture = fixture();
    let token = fixture.token("plan-1", 2_000_000);
    let drafts = fixture.fence(&[fixture.scope_hash()], 7, 1_000_001);
    let (issuer, verifier) = signer("app-key-1");
    let trusted = |key_id: &str| verifier.public_key(key_id);
    let good = signed(&issuer, &drafts[0]);
    let mut bad_signature = good.clone();
    bad_signature.signature = "not base64url!".to_owned();

    for batch in [
        Vec::new(),
        vec![good.clone(); 2],
        vec![good.clone(), bad_signature],
    ] {
        assert_eq!(
            fixture.attach(&batch, trusted, 1_000_002),
            Err(RevocationError::InvalidRequest),
            "a batch of {}",
            batch.len()
        );
        assert!(
            fixture.revocation(&token).signed.is_none(),
            "a refused batch attached a barrier"
        );
    }
    assert_eq!(fixture.attach(&[good], trusted, 1_000_002), Ok((1, 0)));
}

#[test]
fn a_signature_for_a_barrier_no_fence_drafted_is_not_authentic() {
    let fixture = fixture();
    let (issuer, verifier) = signer("app-key-1");
    let draft = draft_barrier(
        fixture.generation(),
        fixture.generation() + 1,
        fixture.scope_hash(),
        &head_cursor(),
        1,
    );
    assert_eq!(
        fixture.attach(
            &[signed(&issuer, &draft)],
            |key_id| verifier.public_key(key_id),
            1_000_001
        ),
        Err(RevocationError::Unauthorized)
    );
}

/// A retirement read back at startup carries its stored signature only
/// when a key this deployment trusts verifies it. Otherwise its plans stay
/// capped, without a barrier, until the application signs again.
#[test]
fn a_stored_signature_is_attached_only_under_a_trusted_key() {
    let base = fixture();
    let generation = base.generation();
    let token = base.token("plan-1", 2_000_000);
    let (issuer, trusted) = signer("app-key-1");
    let (_, stranger) = signer("app-key-1");
    let mut stored = draft_barrier(
        generation,
        generation + 1,
        base.scope_hash(),
        &head_cursor(),
        1,
    );
    issuer.sign_revocation_barrier(&mut stored).unwrap();
    let retirement = StoredRetirement {
        revoked_generation: generation,
        effective_generation: generation + 1,
        ceiling: 7,
        barriers: vec![stored],
    };
    for (verifier, attached) in [(&trusted, true), (&stranger, false)] {
        let fixture = Fixture {
            plans: plans_for(&base.prepared, 1, generation + 1),
            serving: Arc::clone(&base.serving),
            prepared: base.prepared.clone(),
        };
        fixture.plans.retirements_mut().hold(
            retirement.clone(),
            Arc::clone(&base.serving),
            |key_id| verifier.public_key(key_id),
        );
        let revocation = fixture.revocation(&token);
        assert_eq!(revocation.ceiling, 7);
        assert_eq!(revocation.signed.is_some(), attached);
    }
}

/// Records every read so the test sees what a capped reader asked for.
struct ReadRecorder {
    source_id: SourceId,
    source_epoch: SourceEpoch,
    head: u64,
    reads: StdMutex<Vec<(u64, Option<u64>)>>,
}

impl CanonicalLogRead for ReadRecorder {
    fn source_id(&self) -> &SourceId {
        &self.source_id
    }

    fn source_epoch(&self) -> &SourceEpoch {
        &self.source_epoch
    }

    fn head(&self) -> Result<CursorPosition, LogError> {
        Ok(CursorPosition {
            source_epoch: self.source_epoch.clone(),
            sequence: self.head,
        })
    }

    fn minimum_retained(&self) -> Result<CursorPosition, LogError> {
        Ok(CursorPosition {
            source_epoch: self.source_epoch.clone(),
            sequence: 1,
        })
    }

    fn read_page_after_bounded(
        &self,
        from_sequence_exclusive: u64,
        through_sequence_inclusive: Option<u64>,
        _max_records: usize,
        _max_bytes: u64,
    ) -> Result<CanonicalPage, LogError> {
        self.reads
            .lock()
            .unwrap()
            .push((from_sequence_exclusive, through_sequence_inclusive));
        Ok(CanonicalPage {
            records: Vec::new(),
            stored_bytes: 0,
        })
    }
}

/// The ceiling is on the log itself: the head a live poll fences at, and
/// the bound of every read, stop at the fence's head however far the
/// source has moved since.
#[test]
fn a_fenced_plan_reads_nothing_past_its_ceiling() {
    let recorder = ReadRecorder {
        source_id: SourceId::new("source-1").unwrap(),
        source_epoch: SourceEpoch::new("epoch-1").unwrap(),
        head: 40,
        reads: StdMutex::new(Vec::new()),
    };
    let capped = CeilingLog {
        log: &recorder,
        ceiling: 7,
    };
    assert_eq!(capped.head().unwrap().sequence, 7);
    let bounds = capped.bounds().unwrap();
    assert_eq!(bounds.head.sequence, 7);
    assert_eq!(bounds.minimum_retained.sequence, 1);
    capped.read_page_after_bounded(3, None, 10, 1_024).unwrap();
    capped
        .read_page_after_bounded(3, Some(30), 10, 1_024)
        .unwrap();
    capped
        .read_page_after_bounded(3, Some(5), 10, 1_024)
        .unwrap();
    capped
        .read_page_after_bounded(7, Some(30), 10, 1_024)
        .unwrap();
    assert_eq!(
        *recorder.reads.lock().unwrap(),
        [(3, Some(7)), (3, Some(7)), (3, Some(5))]
    );
    // The bounds a build reads with its first page stop there too.
    recorder.reads.lock().unwrap().clear();
    for (from, through) in [(3, None), (3, Some(30)), (3, Some(5)), (7, Some(30))] {
        let (bounds, page) = capped
            .read_bounds_and_page_after_bounded(from, through, 10, 1_024)
            .unwrap();
        assert_eq!(bounds.head.sequence, 7);
        assert_eq!(bounds.minimum_retained.sequence, 1);
        assert!(page.unwrap().records.is_empty());
    }
    assert_eq!(
        *recorder.reads.lock().unwrap(),
        [(3, Some(7)), (3, Some(7)), (3, Some(5))]
    );
    // A ceiling above the head leaves the head alone.
    let uncapped = CeilingLog {
        log: &recorder,
        ceiling: 99,
    };
    assert_eq!(uncapped.head().unwrap().sequence, 40);
    assert_eq!(uncapped.bounds().unwrap().head.sequence, 40);
    let (bounds, _) = uncapped
        .read_bounds_and_page_after_bounded(3, None, 10, 1_024)
        .unwrap();
    assert_eq!(bounds.head.sequence, 40);
}

#[test]
fn bearer_validation_is_exact_and_secret_safe() {
    let token = SecretToken::generate().unwrap();
    assert!(valid_bearer(&token.to_urlsafe(), token.as_bytes()));
    assert!(!valid_bearer("wrong", token.as_bytes()));
    assert!(!valid_bearer(
        &format!("{}x", token.to_urlsafe()),
        token.as_bytes()
    ));
}

#[tokio::test]
async fn blocking_log_proxy_keeps_sync_providers_outside_tokio() {
    let (source_id, source_epoch, log) = runtime_separated_log();
    let mut bridge = blocking_canonical_log_proxy(source_id, source_epoch.clone(), log).unwrap();
    let proxy = bridge.proxy_mut();

    assert_eq!(proxy.head().unwrap().sequence, 7);
    assert_eq!(proxy.minimum_retained().unwrap().sequence, 1);
    assert!(proxy
        .read_page_after_bounded(0, Some(7), 1, 1)
        .unwrap()
        .records
        .is_empty());
    proxy
        .acquire_plan_retention_lease(PlanId::new("plan-1").unwrap(), 1, 2_000, 1_000)
        .unwrap();

    bridge.finish().unwrap();
}

#[test]
fn bridge_drop_joins_worker_on_early_return_and_unwind() {
    fn early_return(active: Arc<AtomicUsize>) {
        let (source_id, source_epoch, log) = runtime_separated_log();
        let _bridge = blocking_canonical_log_proxy_with_spawner(
            source_id,
            source_epoch,
            log,
            counting_spawner(active),
        )
        .unwrap();
    }

    let active = Arc::new(AtomicUsize::new(0));
    early_return(Arc::clone(&active));
    assert_eq!(active.load(Ordering::SeqCst), 0);

    let unwind_active = Arc::clone(&active);
    let panic_result = std::panic::catch_unwind(AssertUnwindSafe(move || {
        let (source_id, source_epoch, log) = runtime_separated_log();
        let _bridge = blocking_canonical_log_proxy_with_spawner(
            source_id,
            source_epoch,
            log,
            counting_spawner(unwind_active),
        )
        .unwrap();
        panic!("injected planner panic");
    }));
    assert!(panic_result.is_err());
    assert_eq!(active.load(Ordering::SeqCst), 0);
}

#[tokio::test]
async fn planner_panic_is_joined_and_mapped_to_sanitized_unavailable() {
    let active = Arc::new(AtomicUsize::new(0));
    let active_for_worker = Arc::clone(&active);
    let (source_id, source_epoch, log) = runtime_separated_log();
    let lock_probe = Arc::clone(&log);
    let result: Result<(), BootstrapHttpError> = tokio::task::spawn_blocking(move || {
        let mut bridge = blocking_canonical_log_proxy_with_spawner(
            source_id,
            source_epoch,
            log,
            counting_spawner(active_for_worker),
        )
        .unwrap();
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        runtime.block_on(async {
            assert_eq!(bridge.proxy_mut().head().unwrap().sequence, 7);
            tokio::task::yield_now().await;
            panic!("injected awaited provider panic");
        });
    })
    .await
    .map_err(|_| BootstrapHttpError::Unavailable);

    assert!(matches!(result, Err(BootstrapHttpError::Unavailable)));
    assert_eq!(active.load(Ordering::SeqCst), 0);
    assert!(lock_probe.try_lock().is_ok());
}

#[test]
fn command_and_reply_disconnects_fail_closed() {
    let source_id = SourceId::new("source-1").unwrap();
    let source_epoch = SourceEpoch::new("epoch-1").unwrap();
    let (commands, receiver) = mpsc::sync_channel(1);
    drop(receiver);
    let disconnected = BlockingCanonicalLogProxy {
        source_id: source_id.clone(),
        source_epoch: source_epoch.clone(),
        commands,
    };
    assert!(matches!(
        disconnected.head(),
        Err(LogError::PostgresUnavailable)
    ));

    let (commands, receiver) = mpsc::sync_channel(1);
    let reply_dropper = std::thread::spawn(move || {
        if let Ok(CanonicalLogCommand::Head(reply)) = receiver.recv() {
            drop(reply);
        }
    });
    let reply_disconnected = BlockingCanonicalLogProxy {
        source_id,
        source_epoch,
        commands,
    };
    assert!(matches!(
        reply_disconnected.head(),
        Err(LogError::PostgresUnavailable)
    ));
    drop(reply_disconnected);
    reply_dropper.join().unwrap();
}

#[test]
fn worker_panic_is_joined_and_reported_without_poisoning_the_log() {
    let source_id = SourceId::new("source-1").unwrap();
    let source_epoch = SourceEpoch::new("epoch-1").unwrap();
    let log = Arc::new(Mutex::new(PanicOnHeadLog {
        source_id: source_id.clone(),
        source_epoch: source_epoch.clone(),
    }));
    let lock_probe = Arc::clone(&log);
    let active = Arc::new(AtomicUsize::new(0));
    let mut bridge = blocking_canonical_log_proxy_with_spawner(
        source_id,
        source_epoch,
        log,
        counting_spawner(Arc::clone(&active)),
    )
    .unwrap();

    assert!(matches!(
        bridge.proxy_mut().head(),
        Err(LogError::PostgresUnavailable)
    ));
    assert!(matches!(
        bridge.finish(),
        Err(LogError::PostgresUnavailable)
    ));
    assert_eq!(active.load(Ordering::SeqCst), 0);
    assert!(lock_probe.try_lock().is_ok());
}

#[test]
fn spawn_failure_returns_without_starting_or_retaining_the_log() {
    let (source_id, source_epoch, log) = runtime_separated_log();
    let lock_probe = Arc::clone(&log);
    let error =
        blocking_canonical_log_proxy_with_spawner(source_id, source_epoch, log, |_worker| {
            Err(io::Error::other("injected spawn failure"))
        })
        .err()
        .expect("spawn failure is returned");

    assert_eq!(error.kind(), io::ErrorKind::Other);
    assert!(lock_probe.try_lock().is_ok());
}

#[test]
fn memory_policy_charges_active_state_only_for_complete_residency() {
    let table = TableId::new("table-1").unwrap();
    let policy = LocalMemoryPolicy {
        active_artifact_capacity_bytes: 100,
        staging_overlap_bytes: 20,
        catch_up_buffer_bytes: 30,
        query_result_reserve_bytes: 40,
    };
    let mut request = BootstrapHttpRequest {
        required_tables: BTreeSet::from([table.clone()]),
        resident_state_proof: None,
        resident_tables: BTreeSet::new(),
        resident_cursor: None,
        cached_artifact_hints: BTreeSet::new(),
        accepted_encodings: vec![RepresentationEncoding::Identity],
        memory_budget_bytes: 1_000,
        client_heap_reserve_bytes: 50,
        client_instance_id: "client".to_owned(),
        device_id: "device".to_owned(),
    };
    assert_eq!(
        policy
            .request_budget(&request)
            .unwrap()
            .current_active_artifact_bytes,
        0
    );
    request.resident_tables.insert(table);
    assert_eq!(
        policy
            .request_budget(&request)
            .unwrap()
            .current_active_artifact_bytes,
        100
    );
}

#[test]
fn expiry_parser_is_rfc3339_and_microsecond_bounded() {
    assert_eq!(
        parse_expiry_micros("1970-01-01T00:00:01Z").unwrap(),
        1_000_000
    );
    assert!(parse_expiry_micros("tomorrow").is_err());
}

#[test]
fn artifact_delivery_expiry_must_equal_the_enforced_plan_expiry() {
    let plan_expiry = "2026-07-19T12:05:00Z";
    assert!(delivery_expiry_matches_plan(Some(plan_expiry), plan_expiry));
    assert!(!delivery_expiry_matches_plan(None, plan_expiry));
    assert!(!delivery_expiry_matches_plan(
        Some("2026-07-19T12:05:01Z"),
        plan_expiry
    ));
}

#[test]
fn resident_tables_may_be_any_authorized_complete_subset() {
    let first = TableId::new("table-1").unwrap();
    let second = TableId::new("table-2").unwrap();
    let complete = BTreeSet::from([first.clone(), second]);
    assert!(resident_tables_are_authorized(&BTreeSet::new(), &complete));
    assert!(resident_tables_are_authorized(
        &BTreeSet::from([first]),
        &complete
    ));
    assert!(!resident_tables_are_authorized(
        &BTreeSet::from([TableId::new("outside").unwrap()]),
        &complete
    ));
}

#[test]
fn bootstrap_resolves_relation_names_to_registry_table_ids() {
    let projects = TableId::new("table:aaaa:g3").unwrap();
    let tasks = TableId::new("table:bbbb:g3").unwrap();
    let complete = BTreeSet::from([projects.clone(), tasks.clone()]);
    let relations = BTreeMap::from([
        ("public.projects".to_owned(), projects.clone()),
        ("public.tasks".to_owned(), tasks.clone()),
    ]);
    // Relation spellings, wire ids, and a mix of the two all land on the
    // same set of wire ids, deduplicated.
    let resolved = resolve_required_tables(
        &BTreeSet::from([
            TableId::new("public.projects").unwrap(),
            TableId::new("public.tasks").unwrap(),
            tasks.clone(),
        ]),
        &relations,
        &complete,
    )
    .unwrap();
    assert_eq!(resolved, complete);
    assert_eq!(
        resolve_required_tables(&BTreeSet::from([projects.clone()]), &relations, &complete)
            .unwrap(),
        BTreeSet::from([projects])
    );
    // An unknown wire id is left for the planner to refuse.
    let unknown = TableId::new("table:cccc:g3").unwrap();
    assert_eq!(
        resolve_required_tables(&BTreeSet::from([unknown.clone()]), &relations, &complete).unwrap(),
        BTreeSet::from([unknown])
    );
}

#[test]
fn bootstrap_refuses_an_unenrolled_relation() {
    let tasks = TableId::new("table:bbbb:g3").unwrap();
    let complete = BTreeSet::from([tasks.clone()]);
    let relations = BTreeMap::from([("public.tasks".to_owned(), tasks)]);
    for relation in ["public.users", "other.tasks", "tasks.public"] {
        assert!(matches!(
            resolve_required_tables(
                &BTreeSet::from([TableId::new(relation).unwrap()]),
                &relations,
                &complete,
            ),
            Err(BootstrapHttpError::InvalidRequest)
        ));
    }
}

#[tokio::test]
async fn a_newer_poll_of_a_plan_wakes_the_older_and_keeps_its_place() {
    let polls = Arc::new(StdMutex::new(ParkedPolls::default()));
    let plan = PlanId::new("plan-parked").unwrap();
    let mut first = ParkedPoll::park(&polls, &plan);
    let mut second = ParkedPoll::park(&polls, &plan);
    tokio::time::timeout(Duration::from_secs(5), first.superseded())
        .await
        .expect("the older poll wakes when a newer one parks");
    // A stream asks again on every wait, and the answer stays the same.
    tokio::time::timeout(Duration::from_secs(5), first.superseded())
        .await
        .expect("a superseded poll stays superseded");
    // The woken poll leaving neither wakes the newer one nor frees its place.
    drop(first);
    assert!(matches!(
        second.superseded.as_mut().unwrap().try_recv(),
        Err(oneshot::error::TryRecvError::Empty)
    ));
    let third = ParkedPoll::park(&polls, &plan);
    tokio::time::timeout(Duration::from_secs(5), second.superseded())
        .await
        .expect("the newer poll kept its place");
    drop(second);
    drop(third);
    assert!(polls.lock().unwrap().by_plan.is_empty());
}

#[test]
fn every_plan_parks_one_poll_without_waking_another_plan() {
    let polls = Arc::new(StdMutex::new(ParkedPolls::default()));
    let mut parked = (0..DEFAULT_MAX_ACTIVE_PLANS)
        .map(|index| ParkedPoll::park(&polls, &PlanId::new(format!("plan-{index}")).unwrap()))
        .collect::<Vec<_>>();
    for poll in &mut parked {
        assert!(matches!(
            poll.superseded.as_mut().unwrap().try_recv(),
            Err(oneshot::error::TryRecvError::Empty)
        ));
    }
    assert_eq!(
        polls.lock().unwrap().by_plan.len(),
        DEFAULT_MAX_ACTIVE_PLANS
    );
    drop(parked);
    assert!(polls.lock().unwrap().by_plan.is_empty());
}

/// The process serves the one scope it built and refuses any other, so a
/// policy that scopes differently never reaches those bytes.
#[test]
fn bootstrap_plans_only_the_scope_this_process_built() {
    let epoch = SourceEpoch::new("planning-epoch").unwrap();
    let built = crate::snapshot::tests::prepared_source(epoch.clone()).scope;
    let other = crate::snapshot::tests::prepared_source_for(
        epoch,
        crate::test_support::registry_for(vec![crate::test_support::inspection(
            "notes",
            16_385,
            vec![crate::test_support::column(
                1,
                "id",
                2_950,
                "uuid",
                false,
                Some(1),
            )],
        )]),
    )
    .scope;
    assert_ne!(built.scope_hash(), other.scope_hash());

    let planned = planning_scope(&built, built.clone()).unwrap();
    assert_eq!(planned.scope_hash(), built.scope_hash());
    assert_eq!(
        planning_scope(&built, other).unwrap_err(),
        BootstrapHttpError::Forbidden
    );
}

/// A canonical log in memory that a test appends to while a build runs, as
/// CDC does.
struct EchoLog {
    source_id: SourceId,
    source_epoch: SourceEpoch,
    records: StdMutex<Vec<canonical_log::CanonicalRecord>>,
}

impl EchoLog {
    fn position(&self, sequence: u64) -> CursorPosition {
        CursorPosition {
            source_epoch: self.source_epoch.clone(),
            sequence,
        }
    }

    /// Appends one commit to `table`, carrying `mutation_ids`.
    fn append(&self, table: &TableId, mutation_ids: Vec<sync_types::MutationId>) {
        let mut records = self.records.lock().unwrap();
        let sequence = records.len() as u64 + 1;
        records.push(canonical_log::CanonicalRecord {
            position: self.position(sequence),
            entry: canonical_log::CanonicalEntry::Transaction(
                canonical_log::CanonicalTransaction {
                    key: canonical_log::TransactionKey {
                        source_epoch: self.source_epoch.clone(),
                        end_lsn: format!("0/{sequence:X}"),
                        postgres_xid: u32::try_from(sequence).unwrap(),
                    },
                    transaction_id: format!("tx-{sequence}"),
                    begin_lsn: format!("0/{sequence:X}"),
                    commit_lsn: format!("0/{sequence:X}"),
                    commit_time_unix_micros: sequence,
                    operations: vec![canonical_log::CanonicalOperation {
                        table_id: table.clone(),
                        primary_key: Vec::new(),
                        kind: canonical_log::CanonicalOperationKind::TruncateAuthorizedTable {
                            reason: format!("truncated at {sequence}"),
                        },
                    }],
                    mutation_ids,
                },
            ),
        });
    }
}

impl CanonicalLogRead for EchoLog {
    fn source_id(&self) -> &SourceId {
        &self.source_id
    }

    fn source_epoch(&self) -> &SourceEpoch {
        &self.source_epoch
    }

    fn head(&self) -> Result<CursorPosition, LogError> {
        Ok(self.position(self.records.lock().unwrap().len() as u64))
    }

    fn minimum_retained(&self) -> Result<CursorPosition, LogError> {
        Ok(self.position(1))
    }

    fn read_page_after_bounded(
        &self,
        from_sequence_exclusive: u64,
        through_sequence_inclusive: Option<u64>,
        max_records: usize,
        _max_bytes: u64,
    ) -> Result<CanonicalPage, LogError> {
        let through = through_sequence_inclusive.unwrap_or(u64::MAX);
        let records = self
            .records
            .lock()
            .unwrap()
            .iter()
            .filter(|record| {
                (from_sequence_exclusive + 1..=through).contains(&record.position.sequence)
            })
            .take(max_records)
            .cloned()
            .collect::<Vec<_>>();
        Ok(CanonicalPage {
            stored_bytes: 100 * records.len() as u64,
            records,
        })
    }
}

/// The log as the change builds' pool hands it out.
struct EchoReader(Arc<EchoLog>);

impl PooledLogReader for EchoReader {
    fn is_closed(&self) -> bool {
        false
    }
}

impl CanonicalLogRead for EchoReader {
    fn source_id(&self) -> &SourceId {
        self.0.source_id()
    }

    fn source_epoch(&self) -> &SourceEpoch {
        self.0.source_epoch()
    }

    fn head(&self) -> Result<CursorPosition, LogError> {
        self.0.head()
    }

    fn minimum_retained(&self) -> Result<CursorPosition, LogError> {
        self.0.minimum_retained()
    }

    fn read_page_after_bounded(
        &self,
        from_sequence_exclusive: u64,
        through_sequence_inclusive: Option<u64>,
        max_records: usize,
        max_bytes: u64,
    ) -> Result<CanonicalPage, LogError> {
        self.0.read_page_after_bounded(
            from_sequence_exclusive,
            through_sequence_inclusive,
            max_records,
            max_bytes,
        )
    }
}

fn acknowledged_ids(batch: &ChangeBatch) -> Vec<String> {
    use sync_protocol::change::{change_stream_frame::Body, ChangeStreamFrame};
    let mut decoder = sync_protocol::FrameDecoder::with_max_payload_bytes(
        std::io::Cursor::new(batch.to_bytes()),
        8 << 20,
    );
    while let Some(frame) = decoder.read_message::<ChangeStreamFrame>().unwrap() {
        if let Some(Body::BatchEnd(end)) = frame.body {
            return end.acknowledged_mutation_ids;
        }
    }
    panic!("the batch has no end");
}

/// A poll resolves its pending ids' receipts, the write commits and CDC
/// appends its echo, and only then does the build read the log. The build
/// stops at a head fixed before the receipts were read, so the batch that
/// carries the echo is one whose proof saw the receipt, and the write leaves
/// the page's overlay. Run from the change tail and from a reader of the log,
/// against the receipts table on PostgreSQL.
#[tokio::test]
#[ignore = "requires CALQUE_TEST_DATABASE_URL"]
async fn a_write_committed_while_a_poll_reads_its_receipts_is_acknowledged_with_its_echo() {
    let url =
        std::env::var("CALQUE_TEST_DATABASE_URL").expect("CALQUE_TEST_DATABASE_URL must be set");
    // Each statement runs on a connection of its own, opened and closed on a
    // blocking thread, where `postgres::Client` may drop.
    let on_postgres = |work: Box<dyn FnOnce(&mut postgres::Client) + Send>| {
        let url = url.clone();
        tokio::task::spawn_blocking(move || {
            work(&mut postgres::Client::connect(&url, postgres::NoTls).unwrap())
        })
    };
    on_postgres(Box::new(|client| {
        mutation_api::install_metadata(client).unwrap();
    }))
    .await
    .unwrap();
    let prepared = crate::snapshot::tests::prepared_source(SourceEpoch::new("echo-epoch").unwrap());
    let source_id = SourceId::new("source-1").unwrap();
    let table = prepared.scope.permitted_tables().next().unwrap().clone();
    let codec = prepared.activated.cursor_codec().clone();
    let cursor = |sequence| {
        codec.encode(&CursorPosition {
            source_epoch: prepared.activated.source_epoch().clone(),
            sequence,
        })
    };
    let sequence_of = |batch: &ChangeBatch| {
        codec
            .decode(&batch.through_cursor_inclusive)
            .unwrap()
            .sequence
    };

    let mut outcomes = Vec::new();
    for through_tail in [true, false] {
        let mutation_id = format!("mutation-echo-{}", uuid::Uuid::new_v4());
        let log = Arc::new(EchoLog {
            source_id: source_id.clone(),
            source_epoch: prepared.activated.source_epoch().clone(),
            records: StdMutex::new(Vec::new()),
        });
        log.append(&table, Vec::new());
        let tail = ChangeTail::new(
            source_id.clone(),
            prepared.activated.source_epoch().clone(),
            1 << 20,
        );
        tail.catch_up(&*log, Instant::now()).unwrap();
        let readers = {
            let log = Arc::clone(&log);
            LogReaders::new(1, move || Ok(EchoReader(Arc::clone(&log))))
        };
        let room = LogBatchRoom::new(LOG_BATCH_ROOM_BYTES);
        let plan_room = PlanRoom::new();
        let incoming = change_api::parse_change_request(sync_protocol::change::ChangeRequest {
            protocol_version: 1,
            from_cursor_exclusive: Some(sync_protocol::change::Cursor {
                token: cursor(1).as_str().as_bytes().to_vec(),
            }),
            through_cursor_inclusive: None,
            tables: vec![sync_protocol::change::TableSubscription {
                table_id: table.to_string(),
                shard_snapshot_cursor: None,
            }],
            authorization_scope_token: b"scope-token".to_vec(),
            max_batch_bytes: 1024 * 1024,
            max_transactions: 10,
            long_poll_timeout_ms: 0,
            client_instance_id: "client-1".to_owned(),
            device_id: "device-1".to_owned(),
            pending_mutation_ids: vec![mutation_id.clone()],
            accept_transaction_fragmentation: true,
            accept_revocation_barrier: true,
            plan_id: "plan-1".to_owned(),
            stream: false,
        })
        .unwrap();
        let plan = || ScopedChangePlan {
            plan_id: PlanId::new("plan-1").unwrap(),
            activated_scope: ActivatedChangeScope::new(
                source_id.clone(),
                &prepared.activated,
                prepared.scope.clone(),
            )
            .unwrap(),
            expires_at_unix_micros: PLAN_TTL_MICROS,
            from_cursor_exclusive: cursor(1),
            through_cursor_inclusive: cursor(1),
            table_starts: BTreeMap::from([(table.clone(), Some(cursor(1)))]),
            acknowledgeable_mutation_ids: ResolvedMutationAcknowledgements::default(),
            revocation_barrier: None,
        };
        // The receipts this poll names, read on PostgreSQL; on the first
        // poll, the write then commits its receipt and CDC appends its echo.
        let resolve = |commits_after: bool| {
            let url = url.clone();
            let log = Arc::clone(&log);
            let tail = &tail;
            let (source_id, epoch) = (source_id.clone(), prepared.activated.source_epoch().clone());
            let (scope, table) = (prepared.scope.clone(), table.clone());
            let (requested, mutation_id) =
                (incoming.pending_mutation_ids().clone(), mutation_id.clone());
            let appended = Arc::clone(&log);
            async move {
                let proof = tokio::task::spawn_blocking(move || {
                    let mut client = postgres::Client::connect(&url, postgres::NoTls).unwrap();
                    let proof = mutation_api::resolve_acknowledgements(
                        &mut client,
                        &source_id,
                        &epoch,
                        &scope,
                        "device-1",
                        &requested,
                    )
                    .unwrap();
                    if commits_after {
                        client
                            .execute(
                                "INSERT INTO calque_internal.mutation_receipts \
                                 (mutation_id, source_id, source_epoch, authorization_generation, \
                                  authorization_policy_hash, authorization_scope_hash, \
                                  command_name, command_version, canonical_input_sha256, \
                                  server_transaction_id, device_id, client_instance_id) \
                                 VALUES ($1, $2, $3, $4, $5, $6, 'test.command', 1, $7, $8, \
                                         'device-1', 'client-1')",
                                &[
                                    &mutation_id,
                                    &source_id.as_str(),
                                    &epoch.as_str(),
                                    &i64::try_from(scope.generation()).unwrap(),
                                    &scope.policy_hash().as_bytes().as_slice(),
                                    &scope.scope_hash().as_bytes().as_slice(),
                                    &[1_u8; 32].as_slice(),
                                    &[2_u8; 16].as_slice(),
                                ],
                            )
                            .unwrap();
                        appended.append(
                            &table,
                            vec![sync_types::MutationId::new(mutation_id).unwrap()],
                        );
                    }
                    proof
                })
                .await
                .unwrap();
                if commits_after {
                    tail.catch_up(&*log, Instant::now()).unwrap();
                }
                Ok(proof)
            }
        };

        let mut from = 1;
        let mut acknowledged_with_echo = None;
        for poll in 0..3 {
            let mut request = incoming.request().clone();
            request.from_cursor_exclusive = cursor(from);
            let reads = ChangeReads {
                activated: &prepared.activated,
                tail: through_tail.then_some(&tail),
                readers: &readers,
                room: &room,
            };
            let batch = build_change_batch(
                reads,
                plan(),
                Some(resolve(poll == 0)),
                request,
                1_000,
                None,
                &plan_room,
                &mut None,
            )
            .await
            .unwrap()
            .expect("the room is free");
            let acknowledged = acknowledged_ids(&batch);
            let through = sequence_of(&batch);
            if from < 2 && through >= 2 {
                acknowledged_with_echo = Some(acknowledged.contains(&mutation_id));
            }
            from = through;
        }
        outcomes.push((through_tail, from, acknowledged_with_echo));
        on_postgres(Box::new(move |client| {
            client
                .execute(
                    "DELETE FROM calque_internal.mutation_receipts WHERE mutation_id = $1",
                    &[&mutation_id],
                )
                .unwrap();
        }))
        .await
        .unwrap();
    }
    // Through the tail and from the log, every poll ran to the echo, and the
    // batch carrying it acknowledged the write.
    assert_eq!(outcomes, [(true, 2, Some(true)), (false, 2, Some(true))]);
}
