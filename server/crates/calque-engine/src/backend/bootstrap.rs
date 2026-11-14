//! Planning a bootstrap: the request's scope, its plan and token, and the
//! artifact grants it is answered with.

use super::*;
use crate::plans::TokenRefusal;

/// The scope a request may be planned against: the one this process built its
/// artifact set for, and no other.
pub(super) fn planning_scope(
    built_scope: &AuthorizedScope,
    scope: AuthorizedScope,
) -> Result<AuthorizedScope, BootstrapHttpError> {
    if scope.scope_hash() != built_scope.scope_hash() {
        return Err(BootstrapHttpError::Forbidden);
    }
    Ok(scope)
}

/// The log as a bootstrap's planner sees it: the head and the retention floor
/// from a reader of its own, and the plan's retention lease through the
/// provider, which holds the writer identity a lease is taken under.
pub(super) struct PlannerLog {
    pub(super) reader: LogReader,
    pub(super) provider: Arc<Mutex<PostgresFencedCanonicalLog>>,
}

impl CanonicalLogRead for PlannerLog {
    fn source_id(&self) -> &SourceId {
        self.reader.source_id()
    }

    fn source_epoch(&self) -> &SourceEpoch {
        self.reader.source_epoch()
    }

    fn head(&self) -> Result<CursorPosition, LogError> {
        self.reader.head()
    }

    fn minimum_retained(&self) -> Result<CursorPosition, LogError> {
        self.reader.minimum_retained()
    }

    fn bounds(&self) -> Result<LogBounds, LogError> {
        self.reader.bounds()
    }

    fn read_page_after_bounded(
        &self,
        from_sequence_exclusive: u64,
        through_sequence_inclusive: Option<u64>,
        max_records: usize,
        max_bytes: u64,
    ) -> Result<CanonicalPage, LogError> {
        self.reader.read_page_after_bounded(
            from_sequence_exclusive,
            through_sequence_inclusive,
            max_records,
            max_bytes,
        )
    }
}

impl CanonicalLogPlanRetention for PlannerLog {
    fn acquire_plan_retention_lease(
        &mut self,
        plan_id: PlanId,
        minimum_sequence: u64,
        expires_ms: i64,
        now_ms: i64,
    ) -> Result<(), LogError> {
        self.provider.blocking_lock().acquire_plan_retention_lease(
            plan_id,
            minimum_sequence,
            expires_ms,
            now_ms,
        )
    }
}

/// The scope a plan of `serving`'s generation was signed for: the scope its
/// artifacts were built for, or its tenant's.
pub(super) fn plan_scope(
    serving: &ServingAuthorization,
    tenant: Option<PartitionValue>,
) -> Option<AuthorizedScope> {
    match tenant {
        None => Some(serving.built_scope.clone()),
        Some(tenant) => serving.policy.partition_scope(tenant).ok(),
    }
}

/// What a plan's token is checked against: the key that signs it, the fence
/// state, and the scopes this process rebuilt from the tokens it verified.
pub(super) struct Plans {
    pub(super) tokens: PlanTokens,
    pub(super) verified: VerifiedPlans,
    pub(super) retirements: StdRwLock<Retirements>,
    pub(super) source_id: SourceId,
    pub(super) activated: ActivatedSource,
}

impl Plans {
    pub(super) fn retirements(&self) -> std::sync::RwLockReadGuard<'_, Retirements> {
        // Every writer leaves it whole: a retirement is inserted in one step.
        self.retirements
            .read()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }

    pub(super) fn retirements_mut(&self) -> std::sync::RwLockWriteGuard<'_, Retirements> {
        self.retirements
            .write()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }

    /// The token of `claims`, unless a fence has retired their generation.
    pub(super) fn sign(&self, claims: &PlanClaims) -> Option<String> {
        (claims.generation == self.retirements().generation).then(|| self.tokens.sign(claims))
    }

    /// The plan one build of `request` is served from, and the fence it is
    /// served under. The first build opens the plan into `held`. A later one
    /// reads what the token proved from `held`, since the token, this
    /// process's key and the request's client and tables stay as they were,
    /// and asks again only what time and a fence can change.
    pub(super) fn for_build<'held>(
        &self,
        held: &'held mut Option<OpenedPlan>,
        current: &Arc<ServingAuthorization>,
        incoming: &IncomingChangeRequest,
        request: &change_api::ChangeBatchRequest,
        now: i64,
    ) -> Result<(&'held OpenedPlan, Option<PlanRevocation>), ChangeBatchError> {
        match held {
            Some(opened) => {
                if opened.claims.expired_at(now) {
                    return Err(TokenRefusal::Stale.into());
                }
                let (_, revocation) = self.retirements().resolve(
                    opened.claims.generation,
                    &opened.scope_hash,
                    current,
                )?;
                Ok((opened, revocation))
            }
            None => {
                let resolved = self.resolve(current, incoming, request, now)?;
                Ok((held.insert(resolved.opened), resolved.revocation))
            }
        }
    }

    /// The plan one change request's token signs, checked against the
    /// request and the fence state, with its scope as this process rebuilds
    /// it from `current` or from the retired generation's policy.
    pub(super) fn resolve(
        &self,
        current: &Arc<ServingAuthorization>,
        incoming: &IncomingChangeRequest,
        request: &change_api::ChangeBatchRequest,
        now: i64,
    ) -> Result<ResolvedPlan, ChangeBatchError> {
        let claims = self
            .tokens
            .open(incoming.authorization_scope_token(), now)?;
        if claims.plan_id != request.plan_id.as_str()
            || !claims.binds(incoming.client_instance_id(), incoming.device_id())
        {
            return Err(ChangeBatchError::InvalidPlanBinding);
        }
        let table_starts =
            request_table_starts(request).ok_or(ChangeBatchError::InvalidTableSet)?;
        claims.check_tables(&table_starts, request.through_cursor_inclusive.is_none())?;
        let scope_hash = claims
            .scope_hash()
            .ok_or(ChangeBatchError::InvalidPlanBinding)?;
        let (serving, revocation) =
            self.retirements()
                .resolve(claims.generation, &scope_hash, current)?;
        let plan = self
            .verified
            .get_or_build(&request.plan_id, &scope_hash, || {
                let tenant = claims
                    .tenant()
                    .map_err(|()| ChangeBatchError::InvalidPlanBinding)?;
                let scope = plan_scope(&serving, tenant)
                    .filter(|scope| scope.scope_hash().as_bytes() == &scope_hash)
                    .ok_or(ChangeBatchError::InvalidPlanBinding)?;
                Ok::<_, ChangeBatchError>(VerifiedPlan {
                    scope_hash,
                    activated_scope: ActivatedChangeScope::new(
                        self.source_id.clone(),
                        &self.activated,
                        scope.clone(),
                    )
                    .map_err(|_| ChangeBatchError::InvalidPlanBinding)?,
                    scope,
                    room: PlanRoom::new(),
                })
            })?;
        let from_cursor = claims.start_cursor()?;
        let through_cursor = claims.through_cursor()?;
        Ok(ResolvedPlan {
            opened: OpenedPlan {
                claims,
                scope_hash,
                plan,
                table_starts,
                from_cursor,
                through_cursor,
            },
            revocation,
        })
    }

    /// Checks one write's token against the client that sent it and the
    /// scope its bearer's grant receives.
    pub(super) fn authorize_write(
        &self,
        token: &[u8],
        client_instance_id: &str,
        device_id: &str,
        scope: &AuthorizedScope,
        now: i64,
    ) -> Result<(), MutationHttpError> {
        // The token is the secret; a batch from another client instance, or
        // from a device other than the one the bootstrap named, is a bad
        // request.
        let claims = self
            .tokens
            .open(token, now)
            .map_err(|_| MutationHttpError::InvalidRequest)?;
        if !claims.binds(client_instance_id, device_id) {
            return Err(MutationHttpError::InvalidRequest);
        }
        // A fenced plan names a retired generation's scope, and nothing may be
        // written under it any longer.
        if claims.generation != self.retirements().generation {
            return Err(MutationHttpError::Unauthorized);
        }
        // A plan of one tenant takes no write a grant of another signs.
        if claims.scope_hash() != Some(*scope.scope_hash().as_bytes()) {
            return Err(MutationHttpError::Forbidden);
        }
        Ok(())
    }
}

/// What a change request's token proved about it. The stream that opened it
/// keeps it for its life, so a commit does not verify a signature and take the
/// verified plans' lock once per connection.
pub(super) struct OpenedPlan {
    pub(super) claims: PlanClaims,
    pub(super) scope_hash: [u8; 32],
    pub(super) plan: Arc<VerifiedPlan>,
    pub(super) table_starts: BTreeMap<TableId, Option<OpaqueCursor>>,
    pub(super) from_cursor: OpaqueCursor,
    pub(super) through_cursor: OpaqueCursor,
}

/// A plan as one change request's token signs it, and its fence as it stands.
pub(super) struct ResolvedPlan {
    pub(super) opened: OpenedPlan,
    pub(super) revocation: Option<PlanRevocation>,
}

/// The object each of the plan's artifacts reads, as the catalog records it.
pub(super) fn artifact_grants(
    catalog: &mut SqliteArtifactCatalog,
    plan: &BootstrapPlan,
    expires_at_unix_micros: i64,
) -> Result<Vec<ArtifactGrant>, LocalBackendError> {
    let mut grants = Vec::with_capacity(plan.artifacts.len());
    for install in &plan.artifacts {
        if !delivery_expiry_matches_plan(
            install.selected_representation.expires_at.as_deref(),
            &plan.expires_at,
        ) {
            return Err(LocalBackendError::InvalidPlan);
        }
        let manifest = catalog
            .manifest(&install.artifact_id)?
            .ok_or(LocalBackendError::InvalidPlan)?;
        let representation = manifest
            .manifest()
            .representations
            .iter()
            .find(|candidate| {
                candidate.representation_id == install.selected_representation.representation_id
            })
            .ok_or(LocalBackendError::InvalidPlan)?;
        if representation.encoding != install.selected_representation.encoding
            || representation.encoded_bytes != install.selected_representation.encoded_bytes
        {
            return Err(LocalBackendError::InvalidPlan);
        }
        grants.push(ArtifactGrant {
            object_id: representation.object_id.clone(),
            metadata: ObjectMetadata::new(
                representation.encoded_bytes,
                representation.encoded_sha256.clone(),
            )?,
            encoding: representation.encoding,
            media_type: manifest.manifest().decoded.media_type.clone(),
            expires_at_unix_micros,
        });
    }
    Ok(grants)
}

pub(super) fn delivery_expiry_matches_plan(
    delivery_expiry: Option<&str>,
    plan_expiry: &str,
) -> bool {
    delivery_expiry == Some(plan_expiry)
}

pub(super) fn resident_tables_are_authorized(
    resident_tables: &BTreeSet<TableId>,
    complete_tables: &BTreeSet<TableId>,
) -> bool {
    resident_tables.is_subset(complete_tables)
}

/// Resolves the tables a bootstrap request names to the wire ids the plan
/// will carry.
///
/// A generated binding names a collection by its source relation,
/// `schema.table`, because the wire `TableId` is hashed over this source's
/// identity and schema generation and would tie an application build to one
/// database and one timeline. A wire id this process serves passes through, a
/// relation resolves to its enrolled table's wire id, and a relation that
/// names no enrolled table is an invalid request. Anything else is left to the
/// planner, which refuses an unknown wire id.
pub(super) fn resolve_required_tables(
    requested: &BTreeSet<TableId>,
    relations: &BTreeMap<String, TableId>,
    complete_tables: &BTreeSet<TableId>,
) -> Result<BTreeSet<TableId>, BootstrapHttpError> {
    let mut resolved = BTreeSet::new();
    for table in requested {
        if complete_tables.contains(table) {
            resolved.insert(table.clone());
        } else if let Some(wire) = relations.get(table.as_str()) {
            resolved.insert(wire.clone());
        } else if table.as_str().contains('.') {
            return Err(BootstrapHttpError::InvalidRequest);
        } else {
            resolved.insert(table.clone());
        }
    }
    Ok(resolved)
}

pub(super) fn parse_expiry_micros(value: &str) -> Result<i64, LocalBackendError> {
    let timestamp = OffsetDateTime::parse(value, &Rfc3339)?;
    i64::try_from(timestamp.unix_timestamp_nanos() / 1_000).map_err(|_| LocalBackendError::Clock)
}

/// The HTTP answer for each planner error. The match names every variant, so
/// a new one does not compile until it is given an answer here.
pub(super) fn map_bootstrap_error(error: BootstrapError) -> BootstrapHttpError {
    match error {
        BootstrapError::MemoryCapacityExceeded { .. } | BootstrapError::InvalidMemoryBudget => {
            BootstrapHttpError::CapacityExceeded
        }
        BootstrapError::ResidentCursorWindow(_)
        | BootstrapError::ResidentProof(bootstrap_api::ResidentProofError::Compatibility)
        | BootstrapError::NoArtifactCover(_)
        | BootstrapError::NoCatchUpStart => BootstrapHttpError::ResnapshotRequired,
        BootstrapError::UnknownTable
        | BootstrapError::UnauthorizedTable
        | BootstrapError::AuthorizationSourceMismatch => BootstrapHttpError::Forbidden,
        BootstrapError::InvalidRequiredTableCount
        | BootstrapError::TooManySubscribedTables(_)
        | BootstrapError::TooManyCacheHints
        | BootstrapError::InvalidEncodings
        | BootstrapError::InvalidChangeLimits
        | BootstrapError::ResidentCursor(_)
        | BootstrapError::ResidentProof(_)
        | BootstrapError::ResidentProofOutsideRegistry
        | BootstrapError::ResidentClaimOutsideProof
        | BootstrapError::ResidentCursorMissing
        | BootstrapError::ResidentCursorBeforeProof => BootstrapHttpError::InvalidRequest,
        BootstrapError::SourceCompatibilityMismatch
        | BootstrapError::InvalidAuthorizationToken
        | BootstrapError::AuthorizationExpiresBeforePlan
        | BootstrapError::InvalidPlanTtl
        | BootstrapError::ArtifactCursor { .. }
        | BootstrapError::ArtifactOutsideRegistry(_)
        | BootstrapError::InvalidDecoderProfile
        | BootstrapError::TooManyCandidates(_)
        | BootstrapError::CoverSearchLimit
        | BootstrapError::PlanningDeadline
        | BootstrapError::DeliveryExpiresBeforePlan
        | BootstrapError::MemoryOverflow
        | BootstrapError::CatalogManifestChanged
        | BootstrapError::SystemClockBeforeEpoch
        | BootstrapError::TimestampOverflow
        | BootstrapError::TimestampFormat(_)
        | BootstrapError::SchemaDefinition(_)
        | BootstrapError::PlanSchema(_)
        | BootstrapError::Catalog(_)
        | BootstrapError::CanonicalLog(_)
        | BootstrapError::Delivery(_)
        | BootstrapError::Selection(_)
        | BootstrapError::Identifier(_)
        | BootstrapError::Json(_) => BootstrapHttpError::Unavailable,
    }
}

impl LocalBackend {
    pub(super) async fn bootstrap(
        &self,
        bearer: BearerToken,
        request: BootstrapHttpRequest,
    ) -> Result<BootstrapPlan, BootstrapHttpError> {
        // One generation for the whole request: the policy, the scope and the
        // artifact state below are read from this one value.
        let serving = self.serving();
        let credential = self
            .inner
            .credential
            .resolve(bearer.expose(), serving.generation())?;
        // Nothing the client sent reaches this decision: the scope is the
        // policy's, and under a partitioned policy the verified grant's
        // tenant. A refusal here happens before any plan token, artifact
        // capability, or catalog read.
        let scope = match &self.inner.tenants {
            Some(_) => serving
                .policy
                .scope_for(credential.principal())
                .map_err(|_| BootstrapHttpError::Forbidden)?,
            None => planning_scope(&serving.built_scope, serving.policy.scope())?,
        };
        // After the credential and the scope, so an unauthenticated caller
        // learns nothing from it; before any catalog read.
        match serving.artifact() {
            ARTIFACT_READY => {}
            ARTIFACT_PREPARING => {
                return Err(BootstrapHttpError::ArtifactPreparing {
                    retry_after_seconds: FENCED_ARTIFACT_RETRY_AFTER_SECONDS,
                })
            }
            _ => return Err(BootstrapHttpError::Unavailable),
        }
        if !resident_tables_are_authorized(&request.resident_tables, &self.inner.complete_tables) {
            return Err(BootstrapHttpError::InvalidRequest);
        }
        let required_tables = resolve_required_tables(
            &request.required_tables,
            &self.inner.relations,
            &self.inner.complete_tables,
        )?;
        if let Some(tenants) = &self.inner.tenants {
            let tenant = scope
                .partition_value()
                .ok_or(BootstrapHttpError::Forbidden)?;
            // A page that holds every table it asks for renews its plan,
            // which installs no artifact and so waits on no build.
            if !request.resident_tables.is_empty()
                && required_tables.is_subset(&request.resident_tables)
            {
                tenants.renewed(serving.generation(), tenant)?;
            } else {
                tenants.admit(serving.generation(), tenant).await?;
            }
        }
        // A bootstrap waits for a planner rather than being refused one. The
        // in-flight request bound limits how many can wait, and a refusal would
        // fail a client for arriving alongside others, which is how every
        // client of a restarted process arrives.
        let planner_permit = Arc::clone(&self.inner.planner_permits)
            .acquire_owned()
            .await
            .map_err(|_| BootstrapHttpError::Unavailable)?;
        let planner_reader = self
            .inner
            .log_readers
            .planners
            .acquire()
            .await
            .map_err(|_| BootstrapHttpError::Unavailable)?;
        let now = OffsetDateTime::now_utc();
        let plan_ttl = credential
            .capability_ttl(
                self.inner.plan_ttl,
                now_unix_seconds().map_err(|_| BootstrapHttpError::Unavailable)?,
            )
            .ok_or(BootstrapHttpError::Unauthorized)?;
        let authorization_expiry = credential.authorization_expiry(now, self.inner.plan_ttl)?;
        let client_instance_id = request.client_instance_id.clone();
        let device_id = request.device_id.clone();
        let authorization = AuthorizedBootstrapScope::new(
            DeliveryPrincipal::new(credential.subject())
                .map_err(|_| BootstrapHttpError::Unavailable)?,
            &self.inner.activated,
            scope.clone(),
            // Replaced by the plan's token once the plan it signs exists.
            UNSIGNED_PLAN_TOKEN,
            authorization_expiry,
        )
        .map_err(map_bootstrap_error)?;
        let memory = self.inner.memory.request_budget(&request)?;
        let unchanged = self.inner.tenants.as_ref().and_then(|tenants| {
            scope
                .partition_value()
                .and_then(|tenant| tenants.unchanged(tenant))
        });
        let request = BootstrapRequest {
            required_tables,
            resident_state_proof: request.resident_state_proof,
            resident_tables: request.resident_tables,
            resident_cursor: request.resident_cursor,
            authorization,
            cached_artifact_hints: request.cached_artifact_hints,
            accepted_encodings: request.accepted_encodings,
            // The client ships one artifact decoder and does not negotiate,
            // so the composition names the profile once, when it decides
            // which artifact it publishes.
            decoder_profile: self.inner.decoder_profile.clone(),
            memory,
            change_endpoint: self.inner.change_endpoint.clone(),
            replan_endpoint: None,
            plan_ttl,
            long_poll_timeout_ms: LOCAL_LONG_POLL_TIMEOUT_MS,
            max_change_batch_bytes: self.inner.max_change_batch_bytes,
            unchanged,
        };
        // Bootstrap planning holds a SQLite transaction across provider awaits,
        // which makes that future !Send. Keep it on one dedicated thread while
        // preserving the HTTP backend's Send future contract.
        //
        // The runtime is built per request: `block_on` on one shared
        // current-thread runtime would serialize its callers and undo the
        // concurrency `MAX_CONCURRENT_PLANNERS` grants.
        let planner_backend = self.clone();
        let plan = tokio::task::spawn_blocking(move || {
            let _planner_permit = planner_permit;
            let runtime = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .map_err(|_| BootstrapHttpError::Unavailable)?;
            let planner_log = PlannerLog {
                reader: planner_reader,
                provider: Arc::clone(&planner_backend.inner.log),
            };
            let mut log_bridge = blocking_canonical_log_proxy(
                planner_log.source_id().clone(),
                planner_log.source_epoch().clone(),
                Arc::new(Mutex::new(planner_log)),
            )
            .map_err(|_| BootstrapHttpError::Unavailable)?;
            let mut catalog = planner_backend.inner.catalog.blocking_lock();
            let result = runtime.block_on(BootstrapPlanner::plan(
                &mut catalog,
                planner_backend.inner.store.as_ref(),
                &planner_backend.inner.artifact_resolver,
                &planner_backend.inner.activated,
                log_bridge.proxy_mut(),
                &planner_backend.inner.resident_proofs,
                request,
            ));
            log_bridge
                .finish()
                .map_err(|_| BootstrapHttpError::Unavailable)?;
            result.map_err(map_bootstrap_error)
        })
        .await
        .map_err(|_| BootstrapHttpError::Unavailable)??;
        let mut plan = plan;
        let expires_at =
            parse_expiry_micros(&plan.expires_at).map_err(|_| BootstrapHttpError::Unavailable)?;
        let mut catalog = self.inner.catalog.lock().await;
        let artifact_grants = artifact_grants(&mut catalog, &plan, expires_at)
            .map_err(|_| BootstrapHttpError::Unavailable)?;
        drop(catalog);

        let terms = PlanTerms::of(&plan).ok_or(BootstrapHttpError::Unavailable)?;
        let claims = self.inner.plans.tokens.claims(
            &terms,
            &scope,
            expires_at,
            &client_instance_id,
            &device_id,
        );
        let (generation, scope_hash) = (scope.generation(), *scope.scope_hash().as_bytes());
        // Signing happens on one side of any fence: a plan the credential
        // admitted before a fence and that reaches this point after it is
        // refused here, before its token or any of its artifacts is signed.
        let _fence = self.inner.fence_lock.read().await;
        if expires_at <= now_unix_micros().map_err(|_| BootstrapHttpError::Unavailable)? {
            return Err(BootstrapHttpError::Unauthorized);
        }
        // Recorded under the fence lock, so a fence that retires this
        // generation drafts a barrier for this scope, or the token below is
        // never signed.
        if let Some(revocation) = &self.inner.revocation {
            if !revocation.store.is_issued(generation, &scope_hash) {
                let store = revocation.store.clone();
                tokio::task::spawn_blocking(move || store.issue(generation, scope_hash))
                    .await
                    .map_err(|_| BootstrapHttpError::Unavailable)?
                    .map_err(|_| BootstrapHttpError::Unavailable)?;
            }
        }
        plan.authorization.scope_token = self
            .inner
            .plans
            .sign(&claims)
            .ok_or(BootstrapHttpError::Unauthorized)?;
        for (install, artifact) in plan.artifacts.iter_mut().zip(&artifact_grants) {
            let representation = &mut install.selected_representation;
            representation.download_url = self
                .inner
                .artifact_capabilities
                .sign_url(
                    &representation.download_url,
                    artifact,
                    &plan.plan_id,
                    generation,
                )
                .map_err(|_| BootstrapHttpError::Unavailable)?;
        }
        Ok(plan)
    }
}
