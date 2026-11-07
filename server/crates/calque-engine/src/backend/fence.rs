//! Fencing a generation: retiring its plans and the revocation barriers that
//! tell their clients.

use super::*;

/// What a fence did to the plans of the generation it retired.
#[derive(Clone)]
pub(super) struct PlanRevocation {
    /// The canonical head the fence stood on. Nothing after it is ever read
    /// for this plan.
    pub(super) ceiling: u64,
    /// The plan's scope's barrier with the application's verified signature,
    /// once it has one.
    pub(super) signed: Option<MintedRevocationBarrier>,
}

/// One fenced generation: the policy its plans' scopes are rebuilt from, the
/// head they are capped at, and one barrier for each scope issued under it.
/// Neither the ceiling nor a draft changes once written; a barrier only gains
/// its signature.
pub(super) struct Retirement {
    pub(super) serving: Arc<ServingAuthorization>,
    pub(super) ceiling: u64,
    pub(super) barriers: BTreeMap<[u8; 32], ScopeBarrier>,
}

/// A signature that verified, with the retired generation and scope its
/// barrier serves.
pub(super) type VerifiedBarrier = (u64, [u8; 32], MintedRevocationBarrier);

pub(super) struct ScopeBarrier {
    /// The barrier Calque drafted, unsigned. The application signs exactly
    /// this; see `Retirements::attach`.
    pub(super) draft: RevocationBarrier,
    pub(super) signed: Option<MintedRevocationBarrier>,
}

/// The generation plans are signed for, and every fenced generation a plan
/// may still name.
pub(super) struct Retirements {
    pub(super) generation: u64,
    pub(super) retired: BTreeMap<u64, Retirement>,
    /// How long after its fence a retired generation's plans can still be
    /// presented: every one of them was signed before the fence.
    pub(super) plan_ttl_micros: i64,
}

impl Retirements {
    pub(super) fn new(generation: u64, plan_ttl: Duration) -> Self {
        Self {
            generation,
            retired: BTreeMap::new(),
            plan_ttl_micros: i64::try_from(plan_ttl.as_micros()).unwrap_or(i64::MAX),
        }
    }

    /// Moves new plans to `generation` and holds the retirement a fence
    /// stored, whose plans are capped at its ceiling from here on.
    pub(super) fn retire(
        &mut self,
        generation: u64,
        retirement: StoredRetirement,
        serving: Arc<ServingAuthorization>,
        trusted_key: impl Fn(&str) -> Option<[u8; 32]>,
    ) {
        self.generation = generation;
        self.hold(retirement, serving, trusted_key);
    }

    /// Holds a retirement as PostgreSQL stores it, with the policy of the
    /// generation it retired. A stored signature that no key this deployment
    /// trusts verifies is not attached, and its barrier waits to be signed
    /// again.
    pub(super) fn hold(
        &mut self,
        retirement: StoredRetirement,
        serving: Arc<ServingAuthorization>,
        trusted_key: impl Fn(&str) -> Option<[u8; 32]>,
    ) {
        let mut barriers = BTreeMap::new();
        for stored in retirement.barriers {
            let Ok(scope_hash) =
                <[u8; 32]>::try_from(stored.revoked_authorization_scope_hash.as_slice())
            else {
                continue;
            };
            let signed = trusted_key(&stored.signing_key_id).and_then(|public_key| {
                let verification_key = RevocationBarrierVerificationKey::new(
                    stored.signing_key_id.clone(),
                    public_key,
                );
                verify_revocation_barrier(&stored, &verification_key)
                    .ok()
                    .map(|()| MintedRevocationBarrier {
                        barrier: stored.clone(),
                        verification_key,
                    })
            });
            let mut draft = stored;
            draft.signing_key_id.clear();
            draft.signature.clear();
            barriers.insert(scope_hash, ScopeBarrier { draft, signed });
        }
        self.retired
            .entry(retirement.revoked_generation)
            .or_insert(Retirement {
                serving,
                ceiling: retirement.ceiling,
                barriers,
            });
    }

    /// The drafts a fence of `revoked_generation` produced, for an
    /// application repeating its order.
    pub(super) fn drafts(&self, revoked_generation: u64) -> Vec<RevocationBarrier> {
        self.retired
            .get(&revoked_generation)
            .map(|retirement| {
                retirement
                    .barriers
                    .values()
                    .map(|barrier| barrier.draft.clone())
                    .collect()
            })
            .unwrap_or_default()
    }

    /// What a plan of `generation` and `scope_hash` is served from: `current`
    /// with no ceiling while its generation is in force, or the retired
    /// generation's policy, ceiling and barrier. Any other generation, and a
    /// retired one without the plan's scope, fails closed: no such plan was
    /// signed.
    pub(super) fn resolve(
        &self,
        generation: u64,
        scope_hash: &[u8; 32],
        current: &Arc<ServingAuthorization>,
    ) -> Result<(Arc<ServingAuthorization>, Option<PlanRevocation>), ChangeBatchError> {
        if generation == self.generation {
            if current.generation() != generation {
                return Err(ChangeBatchError::AuthorizationExpired);
            }
            return Ok((Arc::clone(current), None));
        }
        let retirement = self
            .retired
            .get(&generation)
            .ok_or(ChangeBatchError::AuthorizationExpired)?;
        let barrier = retirement
            .barriers
            .get(scope_hash)
            .ok_or(ChangeBatchError::AuthorizationExpired)?;
        Ok((
            Arc::clone(&retirement.serving),
            Some(PlanRevocation {
                ceiling: retirement.ceiling,
                signed: barrier.signed.clone(),
            }),
        ))
    }

    /// Verifies every signature against the draft it claims to sign and a key
    /// this deployment trusts. Returns each signed barrier with the retired
    /// generation and scope it serves.
    pub(super) fn verify_signed(
        &self,
        signed: &[SignedBarrier],
        trusted_key: impl Fn(&str) -> Option<[u8; 32]>,
    ) -> Result<Vec<VerifiedBarrier>, RevocationError> {
        use base64::Engine as _;
        let drafted = self
            .retired
            .values()
            .map(|retirement| retirement.barriers.len())
            .sum::<usize>();
        // No batch longer than the drafts names only drafts.
        if signed.is_empty() || signed.len() > drafted.max(1) {
            return Err(RevocationError::InvalidRequest);
        }
        let mut verified = Vec::with_capacity(signed.len());
        for barrier in signed {
            // A signature over a barrier no fence drafted signs nothing
            // Calque issued, so it is not authentic here whatever it
            // verifies as.
            let (revoked_generation, scope_hash, draft) = self
                .retired
                .iter()
                .find_map(|(generation, retirement)| {
                    retirement
                        .barriers
                        .iter()
                        .find(|(_, scope)| scope.draft.barrier_id == barrier.barrier_id)
                        .map(|(hash, scope)| (*generation, *hash, &scope.draft))
                })
                .ok_or(RevocationError::Unauthorized)?;
            let public_key =
                trusted_key(&barrier.signing_key_id).ok_or(RevocationError::Unauthorized)?;
            let mut minted = draft.clone();
            minted.signing_key_id = barrier.signing_key_id.clone();
            minted.signature = base64::engine::general_purpose::URL_SAFE_NO_PAD
                .decode(&barrier.signature)
                .map_err(|_| RevocationError::InvalidRequest)?;
            let verification_key =
                RevocationBarrierVerificationKey::new(barrier.signing_key_id.clone(), public_key);
            verify_revocation_barrier(&minted, &verification_key)
                .map_err(|_| RevocationError::Unauthorized)?;
            verified.push((
                revoked_generation,
                scope_hash,
                MintedRevocationBarrier {
                    barrier: minted,
                    verification_key,
                },
            ));
        }
        Ok(verified)
    }

    /// Attaches verified signatures. Returns how many were attached and how
    /// many came after every plan of their scope had expired.
    pub(super) fn attach(&mut self, verified: Vec<VerifiedBarrier>, now: i64) -> (usize, usize) {
        let mut attached = 0;
        let mut expired = 0;
        for (revoked_generation, scope_hash, minted) in verified {
            let Some(scope) = self
                .retired
                .get_mut(&revoked_generation)
                .and_then(|retirement| retirement.barriers.get_mut(&scope_hash))
            else {
                continue;
            };
            let fenced_at = i64::try_from(scope.draft.issued_at_unix_micros).unwrap_or(i64::MAX);
            if fenced_at.saturating_add(self.plan_ttl_micros) > now {
                attached += 1;
            } else {
                expired += 1;
            }
            scope.signed = Some(minted);
        }
        (attached, expired)
    }
}

impl LocalBackend {
    /// Acts on one signed fence order.
    ///
    /// Under the exclusive fence lock, in this order: compile the new
    /// generation's policy, read the canonical head H, make the fence durable
    /// with the retired generation's ceiling at H and one barrier draft for
    /// each scope issued under it, then move every in-memory decision to the
    /// new generation. Anything that fails before the durable write leaves
    /// the deployment as it was; after it, nothing is allowed to serve the
    /// retired generation again, and a restart comes up at the fenced one
    /// with the retirement loaded.
    pub(crate) async fn fence_generation(
        &self,
        order: &str,
    ) -> Result<FencedGeneration, RevocationError> {
        let (Some(revocation), LocalCredentialPolicy::DatasetGrant(verifier)) =
            (self.inner.revocation.as_ref(), &self.inner.credential)
        else {
            return Err(RevocationError::Unavailable);
        };
        let order = verifier
            .verify_fence_order(
                order,
                now_unix_seconds().map_err(|_| RevocationError::Unavailable)?,
            )
            .map_err(|_| RevocationError::Unauthorized)?;
        let _fence = self.inner.fence_lock.write().await;
        let _builds = self.inner.build_fence.fence().await;
        let current = self.serving().generation();
        let now = now_unix_micros().map_err(|_| RevocationError::Unavailable)?;
        if order.effective_generation == current {
            // The application retrying the call it already made: the same
            // drafts, and nothing moves.
            let drafts = self
                .inner
                .plans
                .retirements()
                .drafts(order.revoked_generation);
            return Ok(FencedGeneration {
                revoked_generation: order.revoked_generation,
                effective_generation: order.effective_generation,
                drafts,
            });
        }
        if order.revoked_generation != current || order.effective_generation < current {
            return Err(RevocationError::Conflict);
        }
        let effective = order.effective_generation;
        let recompile = Arc::clone(&revocation.recompile);
        let prepared = tokio::task::spawn_blocking(move || recompile(effective))
            .await
            .map_err(|_| RevocationError::Unavailable)?
            .map_err(|_| RevocationError::Unavailable)?;
        if prepared.policy.generation() != effective {
            return Err(RevocationError::Unavailable);
        }
        let log = Arc::clone(&self.inner.log);
        let head = tokio::task::spawn_blocking(move || log.blocking_lock().head())
            .await
            .map_err(|_| RevocationError::Unavailable)?
            .map_err(|_| RevocationError::Unavailable)?;
        let barrier_cursor = Cursor {
            token: self
                .inner
                .activated
                .cursor_codec()
                .encode(&head)
                .as_str()
                .as_bytes()
                .to_vec(),
        };
        let store = revocation.store.clone();
        let source_epoch = self.inner.activated.source_epoch().clone();
        let (durable, retirement) = tokio::task::spawn_blocking(move || {
            store.fence(&FenceWrite {
                source_epoch: &source_epoch,
                revoked_generation: current,
                effective_generation: effective,
                ceiling: head.sequence,
                barrier_cursor: &barrier_cursor,
                issued_at_unix_micros: u64::try_from(now).unwrap_or_default(),
            })
        })
        .await
        .map_err(|_| RevocationError::Unavailable)?
        .map_err(|_| RevocationError::Unavailable)?;
        let trusted_key = |key_id: &str| verifier.public_key(key_id);
        if durable != effective {
            // Something other than this process fenced past the order: the
            // source lease admits one `calque serve` per source, so an
            // operator wrote the row by hand. The generation this process
            // serves is retired and fails closed: its plans are capped at the
            // head and lose their artifacts, and it takes no new plan and
            // reports unready until a restart comes up at `durable`.
            self.inner.plans.retirements_mut().retire(
                durable,
                retirement,
                self.serving(),
                trusted_key,
            );
            self.inner.artifact_capabilities.retire_before(durable);
            self.serving().set_artifact(ARTIFACT_FAILED);
            self.cancel_reencode();
            self.wake_builds_for_revocation();
            return Err(RevocationError::Conflict);
        }

        // The retired generation's re-encode could no longer be published.
        self.cancel_reencode();
        let retired = self.serving();
        let serving = Arc::new(ServingAuthorization::new(
            prepared.policy.clone(),
            prepared.scope.clone(),
            ARTIFACT_PREPARING,
        ));
        *self
            .inner
            .serving
            .write()
            .unwrap_or_else(std::sync::PoisonError::into_inner) = Arc::clone(&serving);
        let drafts = {
            let mut retirements = self.inner.plans.retirements_mut();
            retirements.retire(effective, retirement, retired, trusted_key);
            retirements.drafts(current)
        };
        self.inner.artifact_capabilities.retire_before(effective);
        self.wake_builds_for_revocation();
        match &self.inner.tenants {
            Some(tenants) => {
                serving.set_artifact(ARTIFACT_READY);
                tenants.move_to_generation(prepared).await;
                drop(_fence);
            }
            None => {
                drop(_fence);
                self.spawn_publication(prepared, serving);
            }
        }
        Ok(FencedGeneration {
            revoked_generation: order.revoked_generation,
            effective_generation: effective,
            drafts,
        })
    }

    /// Attaches the application's signatures to the drafts a fence produced.
    ///
    /// Every signature is verified, and all of them stored, before any is
    /// attached, so one bad barrier attaches none of the batch. A draft drawn
    /// so long ago that every plan it retired has expired is counted, not
    /// refused: there is no client left to deliver it to.
    pub(crate) async fn attach_revocation_barriers(
        &self,
        signed: Vec<SignedBarrier>,
    ) -> Result<(usize, usize), RevocationError> {
        let (LocalCredentialPolicy::DatasetGrant(verifier), Some(revocation)) =
            (&self.inner.credential, self.inner.revocation.as_ref())
        else {
            return Err(RevocationError::Unavailable);
        };
        let verified = self
            .inner
            .plans
            .retirements()
            .verify_signed(&signed, |key_id| verifier.public_key(key_id))?;
        let store = revocation.store.clone();
        let barriers = verified
            .iter()
            .map(|(_, _, minted)| minted.barrier.clone())
            .collect::<Vec<_>>();
        tokio::task::spawn_blocking(move || store.sign(&barriers))
            .await
            .map_err(|_| RevocationError::Unavailable)?
            .map_err(|_| RevocationError::Unavailable)?;
        let now = now_unix_micros().map_err(|_| RevocationError::Unavailable)?;
        let outcome = self.inner.plans.retirements_mut().attach(verified, now);
        if outcome.0 > 0 {
            self.wake_builds_for_revocation();
        }
        Ok(outcome)
    }
}
