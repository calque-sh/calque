//! A bootstrap capability that binds the wrong fact prepares nothing.
//!
//! The capability is the server-minted [`AuthorizedBootstrapScope`] plus the
//! HMAC-signed resident-state envelope, whose payload carries the
//! [`ArtifactCacheKey`], the proven collection membership and the predecessor
//! cursor. Each case presents a validly signed capability that binds one wrong
//! fact, and proves the refusal twice: by the planner's typed error, and by the
//! absence of the two reservations a plan makes, the canonical-log plan
//! retention lease ([`Projection::leased_plan_ids`]) and the catalog plan pin
//! ([`Projection::assert_artifact_was_never_pinned`]).
//! `an_honest_capability_is_the_reachability_control` shows both can fire.
//!
//! The optimistic watermark and the owned bytes are not attacked here: the
//! watermark lives only in the client's standby handoff, and the planner
//! derives the owned bytes from the catalog manifest.

use super::*;

use artifact_contract::direct_columnar_decoder_profile;

/// The planner's own clock, so a probe reads the same lease window
/// `pin_published_for_plan` wrote against.
fn now_ms() -> i64 {
    system_time_millis().expect("the test clock is after the Unix epoch")
}

/// The secret this deployment signs resident-state envelopes with.
const PROOF_SECRET: [u8; 32] = [8_u8; 32];

/// Any other 32 bytes: the forger's key.
const OTHER_SECRET: [u8; 32] = [9_u8; 32];

/// One source-wide scope with one published artifact at the log head.
struct Projection {
    directory: tempfile::TempDir,
    catalog: SqliteArtifactCatalog,
    store: LocalFilesystemBlobStore,
    resolver: SameOriginProxyResolver,
    proofs: ResidentStateProofCodec,
    log: FileCanonicalLog,
    source: ActivatedSource,
    table: TableId,
    scope: AuthorizedScope,
    artifact: ArtifactId,
    cursor: OpaqueCursor,
}

impl Projection {
    async fn open() -> Self {
        let (source, table, scope) = activated_source_with_scope();
        Self::open_for(source, table, scope).await
    }

    async fn open_for(source: ActivatedSource, table: TableId, scope: AuthorizedScope) -> Self {
        let directory = tempfile::tempdir().unwrap();
        let log = canonical_log(&directory.path().join("log"), &source);
        let cursor = source.cursor_codec().encode(&log.head());
        let mut catalog =
            SqliteArtifactCatalog::open(directory.path().join("catalog.sqlite3")).unwrap();
        let store = LocalFilesystemBlobStore::open(directory.path().join("objects"), 1024).unwrap();
        let bytes = b"bootstrap capability projection image";
        let manifest = manifest_for_scope(&source, &table, cursor.clone(), bytes, &scope);
        let artifact = manifest.manifest().artifact_id.clone();
        publish(&mut catalog, &store, &manifest, bytes).await;
        Self {
            directory,
            catalog,
            store,
            resolver: SameOriginProxyResolver::new("https://sync.test/").unwrap(),
            proofs: ResidentStateProofCodec::new(PROOF_SECRET).unwrap(),
            log,
            source,
            table,
            scope,
            artifact,
            cursor,
        }
    }

    async fn plan(&mut self, request: BootstrapRequest) -> Result<BootstrapPlan, BootstrapError> {
        BootstrapPlanner::plan(
            &mut self.catalog,
            &self.store,
            &self.resolver,
            &self.source,
            &mut self.log,
            &self.proofs,
            request,
        )
        .await
    }

    /// The request an honest client makes, optionally presenting `proof`.
    fn request(&self, proof: Option<String>) -> BootstrapRequest {
        request(&self.source, &self.scope, &self.table, proof)
    }

    /// The cache key this deployment derives for `scope` and `decoder`. The
    /// planner derives its own from the activated source and the server-minted
    /// scope and never reads one off the wire; this is only how an attack
    /// builds the *wrong* one to sign.
    fn cache_key(&self, scope: &AuthorizedScope, decoder: &str) -> ArtifactCacheKey {
        authorized_cache_key(&self.source, scope, decoder)
    }

    fn honest_cache_key(&self) -> ArtifactCacheKey {
        self.cache_key(&self.scope, DECODER_PROFILE)
    }

    /// The payload an honest client holds after a cold plan: this deployment's
    /// cache key, the collection it downloaded, the cut it stands on.
    fn honest_payload(&self) -> ResidentStatePayload {
        ResidentStatePayload {
            version: RESIDENT_PROOF_VERSION,
            cache_key: self.honest_cache_key(),
            tables: vec![self.table.clone()],
            cursor: self.cursor.clone(),
        }
    }

    /// A resident-state envelope this deployment's own key authenticates.
    fn signed(&self, payload: &ResidentStatePayload) -> String {
        seal(&self.proofs, payload)
    }

    /// The same envelope under a key this deployment never issued.
    fn forged(&self, payload: &ResidentStatePayload) -> String {
        seal(
            &ResidentStateProofCodec::new(OTHER_SECRET).unwrap(),
            payload,
        )
    }

    /// The plan IDs holding a durable canonical-log retention lease, read from
    /// the log's own manifest rather than from any object this test built.
    fn leased_plan_ids(&self) -> Vec<String> {
        let manifest: Value = serde_json::from_slice(
            &std::fs::read(self.directory.path().join("log").join("manifest.json")).unwrap(),
        )
        .unwrap();
        manifest["retentionLeases"]
            .as_array()
            .map(|leases| {
                leases
                    .iter()
                    .map(|lease| lease["planId"].as_str().unwrap().to_owned())
                    .collect()
            })
            .unwrap_or_default()
    }

    fn assert_no_retention_lease(&self, case: &str) {
        assert!(
            self.leased_plan_ids().is_empty(),
            "{case}: the refused request still pinned canonical history for a plan"
        );
    }

    /// Proves no catalog plan pin exists by retiring the artifact: a pinned
    /// artifact refuses with `ArtifactPinnedByPlan`. It retires the artifact,
    /// so call it once, last.
    fn assert_artifact_was_never_pinned(&mut self, case: &str) {
        let retired = self
            .catalog
            .retire(&self.artifact, now_ms(), "plan-pin probe");
        assert!(
            retired.is_ok(),
            "{case}: the refused request still pinned the artifact for a plan: {retired:?}"
        );
    }

    fn assert_nothing_was_prepared(&mut self, case: &str) {
        self.assert_no_retention_lease(case);
        self.assert_artifact_was_never_pinned(case);
    }
}

/// Signs `payload` with `codec` in the wire framing `decode` parses. Written
/// out rather than routed through `ResidentStateProofCodec::encode` so a case
/// can bind a payload the honest encoder would never produce.
fn seal(codec: &ResidentStateProofCodec, payload: &ResidentStatePayload) -> String {
    let bytes = serde_json::to_vec(payload).unwrap();
    let signature = codec.sign(&bytes);
    format!(
        "{}.{}",
        URL_SAFE_NO_PAD.encode(&bytes),
        URL_SAFE_NO_PAD.encode(signature)
    )
}

/// One source, one registry, two authorization generations. Both scopes come
/// from `test_registry()`, which is built from literals alone, so each passes
/// `validate_request`'s registry binding and the refusal is about the
/// generation, not the source.
fn two_authorization_generations() -> (ActivatedSource, TableId, AuthorizedScope, AuthorizedScope) {
    let (source, table_id, first) = activated_source_with_scope_generation(1);
    let (_, _, second) = activated_source_with_scope_generation(2);
    assert_ne!(
        first.generation(),
        second.generation(),
        "the two scopes must differ in exactly the generation under attack"
    );
    (source, table_id, first, second)
}

/// Both witnesses fire when the capability is honest, so every "prepares
/// nothing" assertion below reports a refusal and not an unreachable path.
#[tokio::test]
async fn an_honest_capability_is_the_reachability_control() {
    let mut projection = Projection::open().await;
    let cold = projection.request(None);
    let plan = projection
        .plan(cold)
        .await
        .expect("an honest cold request plans");

    assert_eq!(
        projection.leased_plan_ids(),
        vec![plan.plan_id.to_string()],
        "the issued plan holds exactly its own canonical-log retention lease"
    );
    let retired = projection
        .catalog
        .retire(&projection.artifact, now_ms(), "reachability control");
    assert!(
        matches!(retired, Err(CatalogError::ArtifactPinnedByPlan { .. })),
        "the issued plan did not pin the artifact it told the client to download: {retired:?}"
    );
    // The envelope it hands back re-authenticates, so the warm path the attacks
    // below are diverted from is one an honest client does reach.
    let payload = projection
        .proofs
        .decode(
            &plan.next_resident_state_proof,
            &projection.honest_cache_key(),
        )
        .expect("the minted envelope verifies under the key that minted it");
    assert_eq!(payload.tables, vec![projection.table.clone()]);
    assert_eq!(payload.cursor, projection.cursor);
}

/// A capability presented again after the cut it stands on was retired buys
/// nothing.
///
/// No plan is outstanding: a live plan's retention lease holds its cut open,
/// so replaying inside the plan's lifetime is a warm resume. The envelope here
/// is still validly signed for the right scope, schema, format and epochs; only
/// the history it names is gone.
#[tokio::test]
async fn a_capability_bound_to_a_retired_cut_prepares_nothing() {
    let mut projection = Projection::open().await;
    let spent = projection.signed(&projection.honest_payload());

    // The source moves on and the operator retires the history that cut needs.
    for xid in 2..=6_u32 {
        projection
            .log
            .append_barrier(CanonicalBarrier {
                key: TransactionKey {
                    source_epoch: projection.source.source_epoch().clone(),
                    end_lsn: format!("0/{xid}0"),
                    postgres_xid: xid,
                },
                build_id: "bootstrap-test".to_owned(),
                nonce: format!("nonce-{xid}"),
                message_lsn: format!("0/{xid}F"),
                commit_lsn: format!("0/{xid}0"),
                commit_time_unix_micros: u64::from(xid),
            })
            .unwrap();
    }
    projection.log.retain_from(4).unwrap();
    assert_eq!(
        projection.log.minimum_retained().sequence,
        4,
        "nothing pinned the old history, so the retirement had to take effect"
    );

    let replay = projection.request(Some(spent));
    let refused = projection.plan(replay).await;
    assert!(
        matches!(
            refused,
            Err(BootstrapError::ResidentCursorWindow(
                CursorWindowError::BeforeRetention
            ))
        ),
        "a capability bound to a retired cut was not refused: {refused:?}"
    );
    projection.assert_nothing_was_prepared("capability bound to a retired cut");
}

#[tokio::test]
async fn a_capability_rebound_to_another_scope_prepares_nothing() {
    let (source, table, scope_a, scope_b) = activated_source_with_two_column_sets();
    assert_ne!(
        scope_a.scope_hash().to_string(),
        scope_b.scope_hash().to_string()
    );
    let mut projection = Projection::open_for(source, table, scope_b).await;
    let minted_for_a = projection.signed(&ResidentStatePayload {
        version: RESIDENT_PROOF_VERSION,
        cache_key: projection.cache_key(&scope_a, DECODER_PROFILE),
        tables: vec![projection.table.clone()],
        cursor: projection.cursor.clone(),
    });

    let attack = projection.request(Some(minted_for_a));
    let refused = projection.plan(attack).await;
    assert!(
        matches!(
            refused,
            Err(BootstrapError::ResidentProof(
                ResidentProofError::Compatibility
            ))
        ),
        "a capability minted for another scope was not refused: {refused:?}"
    );
    projection.assert_nothing_was_prepared("scope rebind");
}

#[tokio::test]
async fn a_capability_rebound_to_another_schema_generation_prepares_nothing() {
    let mut projection = Projection::open().await;
    let mut cache_key = projection.honest_cache_key();
    cache_key.schema_generation = cache_key.schema_generation.saturating_add(1);
    let rebound = projection.signed(&ResidentStatePayload {
        version: RESIDENT_PROOF_VERSION,
        cache_key,
        tables: vec![projection.table.clone()],
        cursor: projection.cursor.clone(),
    });

    let attack = projection.request(Some(rebound));
    let refused = projection.plan(attack).await;
    assert!(
        matches!(
            refused,
            Err(BootstrapError::ResidentProof(
                ResidentProofError::Compatibility
            ))
        ),
        "a capability minted against another schema generation was not refused: {refused:?}"
    );
    projection.assert_nothing_was_prepared("schema generation rebind");
}

#[tokio::test]
async fn a_capability_rebound_to_another_decoder_profile_prepares_nothing() {
    let mut projection = Projection::open().await;
    // Minted for a decoder of another format version, presented by a client
    // that declares the current one. The decoder profile is inside the signed
    // cache key so the two cannot be swapped.
    let other_key = projection.cache_key(&projection.scope, &direct_columnar_decoder_profile(3));
    assert_ne!(other_key, projection.honest_cache_key());
    let minted_for_other = projection.signed(&ResidentStatePayload {
        version: RESIDENT_PROOF_VERSION,
        cache_key: other_key,
        tables: vec![projection.table.clone()],
        cursor: projection.cursor.clone(),
    });

    let attack = projection.request(Some(minted_for_other));
    let refused = projection.plan(attack).await;
    assert!(
        matches!(
            refused,
            Err(BootstrapError::ResidentProof(
                ResidentProofError::Compatibility
            ))
        ),
        "a capability minted for another decoder profile was not refused: {refused:?}"
    );
    projection.assert_nothing_was_prepared("decoder profile rebind");
}

#[tokio::test]
async fn a_capability_rebound_to_another_source_epoch_prepares_nothing() {
    let mut projection = Projection::open().await;
    let mut cache_key = projection.honest_cache_key();
    cache_key.source_epoch = SourceEpoch::new("epoch-other").unwrap();
    let rebound = projection.signed(&ResidentStatePayload {
        version: RESIDENT_PROOF_VERSION,
        cache_key,
        tables: vec![projection.table.clone()],
        cursor: projection.cursor.clone(),
    });

    let attack = projection.request(Some(rebound));
    let refused = projection.plan(attack).await;
    assert!(
        matches!(
            refused,
            Err(BootstrapError::ResidentProof(
                ResidentProofError::Compatibility
            ))
        ),
        "a capability minted under another source epoch was not refused: {refused:?}"
    );
    projection.assert_nothing_was_prepared("source epoch rebind");
}

#[tokio::test]
async fn a_capability_rebound_to_another_authorization_generation_prepares_nothing() {
    let (source, table, generation_one, generation_two) = two_authorization_generations();
    let mut projection = Projection::open_for(source, table, generation_two).await;
    let minted_at_generation_one = projection.signed(&ResidentStatePayload {
        version: RESIDENT_PROOF_VERSION,
        cache_key: projection.cache_key(&generation_one, DECODER_PROFILE),
        tables: vec![projection.table.clone()],
        cursor: projection.cursor.clone(),
    });

    let attack = projection.request(Some(minted_at_generation_one));
    let refused = projection.plan(attack).await;
    assert!(
        matches!(
            refused,
            Err(BootstrapError::ResidentProof(
                ResidentProofError::Compatibility
            ))
        ),
        "a capability minted before the authorization fence was not refused: {refused:?}"
    );
    projection.assert_nothing_was_prepared("authorization generation rebind");
}

/// The predecessor cursor sits inside the signed payload, so it cannot be
/// edited without breaking the envelope. It can still be *minted* naming a cut
/// of another timeline, and that is what is refused here.
#[tokio::test]
async fn a_capability_rebound_to_another_predecessor_cursor_prepares_nothing() {
    let mut projection = Projection::open().await;
    let other_timeline = projection.source.cursor_codec().encode(&CursorPosition {
        source_epoch: SourceEpoch::new("epoch-other").unwrap(),
        sequence: 1,
    });
    let rebound = projection.signed(&ResidentStatePayload {
        version: RESIDENT_PROOF_VERSION,
        cache_key: projection.honest_cache_key(),
        tables: vec![projection.table.clone()],
        cursor: other_timeline,
    });

    let attack = projection.request(Some(rebound));
    let refused = projection.plan(attack).await;
    assert!(
        matches!(
            refused,
            Err(BootstrapError::ResidentCursor(
                sync_types::CursorError::EpochMismatch { .. }
            ))
        ),
        "a capability bound to another timeline's predecessor cursor was not refused: {refused:?}"
    );
    projection.assert_nothing_was_prepared("predecessor cursor rebind");
}

/// The canonical transaction a capability claims to stand on has to be one this
/// log committed. A validly signed envelope naming a sequence past the captured
/// head names a transaction that does not exist.
#[tokio::test]
async fn a_capability_naming_an_uncommitted_canonical_transaction_prepares_nothing() {
    let mut projection = Projection::open().await;
    let beyond_head = projection.source.cursor_codec().encode(&CursorPosition {
        source_epoch: projection.source.source_epoch().clone(),
        sequence: projection.log.head().sequence + 1_000,
    });
    let rebound = projection.signed(&ResidentStatePayload {
        version: RESIDENT_PROOF_VERSION,
        cache_key: projection.honest_cache_key(),
        tables: vec![projection.table.clone()],
        cursor: beyond_head,
    });

    let attack = projection.request(Some(rebound));
    let refused = projection.plan(attack).await;
    assert!(
        matches!(
            refused,
            Err(BootstrapError::ResidentCursorWindow(
                CursorWindowError::BeyondHead
            ))
        ),
        "a capability naming a canonical transaction the log never committed was not refused: \
         {refused:?}"
    );
    projection.assert_nothing_was_prepared("uncommitted canonical transaction");
}

/// Collection membership is attacked from both sides: a client claiming more
/// than its envelope proves, and an envelope proving a collection this source
/// does not serve.
#[tokio::test]
async fn a_capability_rebound_to_another_collection_membership_prepares_nothing() {
    let mut projection = Projection::open().await;
    let foreign = TableId::new("table-not-in-this-registry").unwrap();

    // The envelope proves one collection; the request claims another.
    let honest = projection.signed(&projection.honest_payload());
    let mut overclaiming = projection.request(Some(honest));
    overclaiming.resident_tables = BTreeSet::from([foreign.clone()]);
    let refused = projection.plan(overclaiming).await;
    assert!(
        matches!(refused, Err(BootstrapError::ResidentClaimOutsideProof)),
        "a resident claim wider than the signed membership was not refused: {refused:?}"
    );
    projection.assert_no_retention_lease("collection membership overclaim");

    // And an envelope proving a collection outside the activated registry
    // cannot smuggle one in, however well signed.
    let mut wider = vec![foreign.clone(), projection.table.clone()];
    wider.sort();
    let smuggled = projection.signed(&ResidentStatePayload {
        version: RESIDENT_PROOF_VERSION,
        cache_key: projection.honest_cache_key(),
        tables: wider,
        cursor: projection.cursor.clone(),
    });
    let mut smuggling = projection.request(Some(smuggled));
    smuggling.resident_tables = BTreeSet::from([foreign, projection.table.clone()]);
    let refused = projection.plan(smuggling).await;
    assert!(
        matches!(refused, Err(BootstrapError::ResidentProofOutsideRegistry)),
        "a signed membership naming an unregistered collection was not refused: {refused:?}"
    );
    projection.assert_nothing_was_prepared("collection membership smuggling");
}

/// Four ways to break the envelope's authentication, each refused with its own
/// reason and none of them reaching a reservation.
#[tokio::test]
async fn a_forged_or_truncated_envelope_signature_prepares_nothing() {
    let mut projection = Projection::open().await;
    let payload = projection.honest_payload();
    let honest = projection.signed(&payload);

    // Signed under a key this deployment never issued.
    let forged = projection.forged(&payload);
    assert_ne!(
        forged, honest,
        "the forgery must differ from the real thing"
    );
    let attack = projection.request(Some(forged));
    let refused = projection.plan(attack).await;
    assert!(
        matches!(
            refused,
            Err(BootstrapError::ResidentProof(ResidentProofError::Signature))
        ),
        "a forged envelope signature was not refused: {refused:?}"
    );
    projection.assert_no_retention_lease("forged signature");

    // The real signature with its last byte cut off.
    let (body, signature) = honest.split_once('.').unwrap();
    let mut truncated_bytes = URL_SAFE_NO_PAD.decode(signature).unwrap();
    truncated_bytes.pop();
    let truncated = format!("{body}.{}", URL_SAFE_NO_PAD.encode(&truncated_bytes));
    let attack = projection.request(Some(truncated));
    let refused = projection.plan(attack).await;
    assert!(
        matches!(
            refused,
            Err(BootstrapError::ResidentProof(ResidentProofError::Signature))
        ),
        "a truncated envelope signature was not refused: {refused:?}"
    );
    projection.assert_no_retention_lease("truncated signature");

    // The signature removed altogether.
    let attack = projection.request(Some(body.to_owned()));
    let refused = projection.plan(attack).await;
    assert!(
        matches!(
            refused,
            Err(BootstrapError::ResidentProof(ResidentProofError::Framing))
        ),
        "an unsigned envelope was not refused: {refused:?}"
    );
    projection.assert_no_retention_lease("missing signature");

    // The honest signature carried over a payload edited under it.
    let mut edited = payload.clone();
    edited.cache_key.authorization_generation =
        edited.cache_key.authorization_generation.saturating_add(1);
    let spliced = format!(
        "{}.{signature}",
        URL_SAFE_NO_PAD.encode(serde_json::to_vec(&edited).unwrap())
    );
    let attack = projection.request(Some(spliced));
    let refused = projection.plan(attack).await;
    assert!(
        matches!(
            refused,
            Err(BootstrapError::ResidentProof(ResidentProofError::Signature))
        ),
        "an envelope whose payload was edited under its signature was not refused: {refused:?}"
    );
    projection.assert_nothing_was_prepared("spliced payload");
}

/// The server-minted half of the capability. A grant that has already expired
/// buys no plan, with or without a resident envelope.
#[tokio::test]
async fn an_expired_grant_prepares_nothing() {
    let mut projection = Projection::open().await;
    let expired = |projection: &Projection| {
        AuthorizedBootstrapScope::new(
            DeliveryPrincipal::new("user-1").unwrap(),
            &projection.source,
            projection.scope.clone(),
            "opaque-scope-token",
            OffsetDateTime::now_utc() - time::Duration::hours(1),
        )
        .unwrap()
    };

    let mut cold = projection.request(None);
    cold.authorization = expired(&projection);
    let refused = projection.plan(cold).await;
    assert!(
        matches!(refused, Err(BootstrapError::AuthorizationExpiresBeforePlan)),
        "an expired grant still bought a cold plan: {refused:?}"
    );
    projection.assert_no_retention_lease("expired grant, cold");

    let honest = projection.signed(&projection.honest_payload());
    let mut warm = projection.request(Some(honest));
    warm.authorization = expired(&projection);
    let refused = projection.plan(warm).await;
    assert!(
        matches!(refused, Err(BootstrapError::AuthorizationExpiresBeforePlan)),
        "an expired grant still bought a warm plan: {refused:?}"
    );
    projection.assert_nothing_was_prepared("expired grant, warm");
}

/// An envelope version this deployment does not implement is refused before any
/// binding it might carry is read.
#[tokio::test]
async fn a_capability_of_an_unimplemented_envelope_version_prepares_nothing() {
    let mut projection = Projection::open().await;
    let mut payload = projection.honest_payload();
    payload.version = RESIDENT_PROOF_VERSION.saturating_add(1);
    let future = projection.signed(&payload);

    let attack = projection.request(Some(future));
    let refused = projection.plan(attack).await;
    assert!(
        matches!(
            refused,
            Err(BootstrapError::ResidentProof(ResidentProofError::Version(
                _
            )))
        ),
        "an envelope of an unimplemented version was not refused: {refused:?}"
    );
    projection.assert_nothing_was_prepared("unimplemented envelope version");
}

/// The capacity reservation is the only binding the client states rather than
/// carries, and an unaffordable one is a structured refusal, never partial
/// data. The cover search charges `base_memory` and each candidate's peak
/// against the plan's budget, so the refusal lands before the catalog pin.
#[tokio::test]
async fn a_capacity_reservation_the_client_cannot_honour_prepares_nothing() {
    let mut projection = Projection::open().await;
    let mut tight = projection.request(None);
    tight.memory = ClientMemoryBudget {
        safe_budget_bytes: 1,
        current_active_artifact_bytes: 0,
        staging_overlap_bytes: 0,
        catch_up_buffer_bytes: 0,
        query_result_reserve_bytes: 0,
        client_heap_reserve_bytes: 1,
    };
    let refused = projection.plan(tight).await;
    assert!(
        matches!(
            refused,
            Err(BootstrapError::MemoryCapacityExceeded { available: 1, .. })
        ),
        "a capacity reservation the client cannot honour was not refused: {refused:?}"
    );
    projection.assert_nothing_was_prepared("unaffordable capacity reservation");
}

/// The target cursor is the only binding no request can carry: `plan_inner`
/// takes it from `log.head()` after the capability is verified. The activation
/// cursor a client is given moves when the log moves, not when the client asks.
#[tokio::test]
async fn the_target_cursor_is_taken_from_the_log_and_never_from_the_request() {
    let mut projection = Projection::open().await;
    let cold = projection.request(None);
    let first = projection
        .plan(cold)
        .await
        .expect("an honest cold request plans");
    assert_eq!(
        first.activation_cursor,
        projection
            .source
            .cursor_codec()
            .encode(&projection.log.head()),
        "the plan's target cursor is not this log's head"
    );

    projection
        .log
        .append_barrier(CanonicalBarrier {
            key: TransactionKey {
                source_epoch: projection.source.source_epoch().clone(),
                end_lsn: "0/20".to_owned(),
                postgres_xid: 2,
            },
            build_id: "bootstrap-test".to_owned(),
            nonce: "nonce-2".to_owned(),
            message_lsn: "0/1F".to_owned(),
            commit_lsn: "0/20".to_owned(),
            commit_time_unix_micros: 2,
        })
        .unwrap();

    let again = projection.request(None);
    let second = projection
        .plan(again)
        .await
        .expect("an honest cold request plans after the log moved");
    assert_ne!(
        first.activation_cursor, second.activation_cursor,
        "the target cursor did not follow the log"
    );
    assert_eq!(
        second.activation_cursor,
        projection
            .source
            .cursor_codec()
            .encode(&projection.log.head()),
    );
}
