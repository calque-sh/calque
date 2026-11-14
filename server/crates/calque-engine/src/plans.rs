//! A plan is a token this deployment signs at bootstrap and verifies on every
//! change request and write. Nothing about a plan is held in memory that a
//! request could not rebuild from its token: the cache below only saves
//! rebuilding the plan's scope, and a miss, an eviction or a restart rebuilds
//! it.

use std::{
    collections::{BTreeMap, HashMap},
    sync::{Arc, Mutex as StdMutex},
};

use authz::{AuthorizedScope, PartitionValue};
use base64::{engine::general_purpose::URL_SAFE_NO_PAD, Engine as _};
use bootstrap_api::BootstrapPlan;
use change_api::{ActivatedChangeScope, ChangeBatchError, ChangeBatchRequest};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use subtle::ConstantTimeEq;
use sync_types::{OpaqueCursor, PlanId, SourceEpoch, SourceId, TableId};

use crate::{
    batch_room::PlanRoom,
    signing::{PlanKey, PLAN_DOMAIN},
};

/// What a plan token carries. The digests keep it one size at any table
/// count; a request rebuilds what they digest from what it sends.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub(crate) struct PlanClaims {
    key_id: String,
    source_id: String,
    source_epoch: String,
    pub(crate) plan_id: String,
    pub(crate) generation: u64,
    scope_hash: String,
    tenant: Option<String>,
    pub(crate) expires_at_unix_micros: i64,
    from_cursor: String,
    through_cursor: String,
    tables: String,
    starts: String,
    binding: String,
}

impl PlanClaims {
    pub(crate) fn scope_hash(&self) -> Option<[u8; 32]> {
        URL_SAFE_NO_PAD
            .decode(&self.scope_hash)
            .ok()?
            .try_into()
            .ok()
    }

    pub(crate) fn expired_at(&self, now: i64) -> bool {
        self.expires_at_unix_micros <= now
    }

    pub(crate) fn tenant(&self) -> Result<Option<PartitionValue>, ()> {
        self.tenant.as_deref().map(parse_tenant).transpose()
    }

    pub(crate) fn start_cursor(&self) -> Result<OpaqueCursor, ChangeBatchError> {
        OpaqueCursor::parse(self.from_cursor.clone())
            .map_err(|_| ChangeBatchError::InvalidPlanBinding)
    }

    pub(crate) fn through_cursor(&self) -> Result<OpaqueCursor, ChangeBatchError> {
        OpaqueCursor::parse(self.through_cursor.clone())
            .map_err(|_| ChangeBatchError::InvalidPlanBinding)
    }

    pub(crate) fn binds(&self, client_instance_id: &str, device_id: &str) -> bool {
        digest_matches(
            &self.binding,
            &binding_digest(client_instance_id, device_id),
        )
    }

    /// Checks the table starts one change request names against the plan's:
    /// its tables on every request, and their starts on a catch-up request,
    /// since a live one names none.
    pub(crate) fn check_tables(
        &self,
        table_starts: &BTreeMap<TableId, Option<OpaqueCursor>>,
        live: bool,
    ) -> Result<(), ChangeBatchError> {
        if !digest_matches(&self.tables, &tables_digest(table_starts)) {
            return Err(ChangeBatchError::InvalidTableSet);
        }
        if !live && !digest_matches(&self.starts, &starts_digest(table_starts)) {
            return Err(ChangeBatchError::InvalidPlanBinding);
        }
        Ok(())
    }
}

/// Signs this deployment's plans and opens the ones it signed for this source
/// and epoch.
#[derive(Debug, Clone)]
pub(crate) struct PlanTokens {
    key: PlanKey,
    source_id: SourceId,
    source_epoch: SourceEpoch,
}

/// Why a token was refused before anything it claims was read.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum TokenRefusal {
    /// The tag does not verify, or the token is not one this key signs.
    Forged,
    /// A plan of another source or epoch, or past its expiry: the answer a
    /// restart on a new epoch gives, after which the page bootstraps again.
    Stale,
}

impl From<TokenRefusal> for ChangeBatchError {
    fn from(refusal: TokenRefusal) -> Self {
        match refusal {
            TokenRefusal::Forged => Self::InvalidAuthorizationToken,
            TokenRefusal::Stale => Self::InvalidPlanBinding,
        }
    }
}

impl PlanTokens {
    pub(crate) fn new(key: PlanKey, source_id: SourceId, source_epoch: SourceEpoch) -> Self {
        Self {
            key,
            source_id,
            source_epoch,
        }
    }

    /// The claims of a plan with `terms`, granted `scope` until
    /// `expires_at_unix_micros` to one client instance and device.
    pub(crate) fn claims(
        &self,
        terms: &PlanTerms,
        scope: &AuthorizedScope,
        expires_at_unix_micros: i64,
        client_instance_id: &str,
        device_id: &str,
    ) -> PlanClaims {
        PlanClaims {
            key_id: self.key.key_id().to_owned(),
            source_id: self.source_id.as_str().to_owned(),
            source_epoch: self.source_epoch.as_str().to_owned(),
            plan_id: terms.plan_id.as_str().to_owned(),
            generation: scope.generation(),
            scope_hash: URL_SAFE_NO_PAD.encode(scope.scope_hash().as_bytes()),
            tenant: scope.partition_value().map(tenant_text),
            expires_at_unix_micros,
            from_cursor: terms.from_cursor.as_str().to_owned(),
            through_cursor: terms.through_cursor.as_str().to_owned(),
            tables: URL_SAFE_NO_PAD.encode(tables_digest(&terms.table_starts)),
            starts: URL_SAFE_NO_PAD.encode(starts_digest(&terms.table_starts)),
            binding: URL_SAFE_NO_PAD.encode(binding_digest(client_instance_id, device_id)),
        }
    }

    pub(crate) fn sign(&self, claims: &PlanClaims) -> String {
        self.key.sign(PLAN_DOMAIN, claims)
    }

    /// The claims of a token this deployment signed for this source and
    /// epoch, unexpired at `now`.
    pub(crate) fn open(&self, token: &[u8], now: i64) -> Result<PlanClaims, TokenRefusal> {
        let token = std::str::from_utf8(token).map_err(|_| TokenRefusal::Forged)?;
        let claims: PlanClaims = self
            .key
            .open(PLAN_DOMAIN, token)
            .ok_or(TokenRefusal::Forged)?;
        if claims.key_id != self.key.key_id() {
            return Err(TokenRefusal::Forged);
        }
        if claims.source_id != self.source_id.as_str()
            || claims.source_epoch != self.source_epoch.as_str()
            || claims.expired_at(now)
        {
            return Err(TokenRefusal::Stale);
        }
        Ok(claims)
    }
}

/// What a plan's token signs of the plan the bootstrap planned.
pub(crate) struct PlanTerms {
    pub(crate) plan_id: PlanId,
    pub(crate) from_cursor: OpaqueCursor,
    pub(crate) through_cursor: OpaqueCursor,
    /// Each table the plan names and the cursor its catch-up starts at: an
    /// artifact table's cut, or `None` for a table the client already holds.
    pub(crate) table_starts: BTreeMap<TableId, Option<OpaqueCursor>>,
}

impl PlanTerms {
    /// The terms of `plan`, or `None` when it names no table or one table
    /// twice.
    pub(crate) fn of(plan: &BootstrapPlan) -> Option<Self> {
        let mut table_starts = BTreeMap::new();
        for table in &plan.resident_tables {
            table_starts.insert(table.clone(), None);
        }
        for artifact in &plan.artifacts {
            for table in &artifact.tables {
                if table_starts
                    .insert(table.clone(), Some(artifact.snapshot_cursor.clone()))
                    .is_some()
                {
                    return None;
                }
            }
        }
        (!table_starts.is_empty()).then(|| Self {
            plan_id: plan.plan_id.clone(),
            from_cursor: plan.change_stream.from_cursor_exclusive.clone(),
            through_cursor: plan.change_stream.through_cursor_inclusive.clone(),
            table_starts,
        })
    }
}

/// The table starts one change request names, or `None` when it names a
/// table twice. A live request names no start, so each is `None`.
pub(crate) fn request_table_starts(
    request: &ChangeBatchRequest,
) -> Option<BTreeMap<TableId, Option<OpaqueCursor>>> {
    let mut starts = BTreeMap::new();
    for table in &request.tables {
        if starts
            .insert(table.table_id.clone(), table.snapshot_cursor.clone())
            .is_some()
        {
            return None;
        }
    }
    Some(starts)
}

fn framed(digest: &mut Sha256, bytes: &[u8]) {
    digest.update(u64::try_from(bytes.len()).unwrap_or(u64::MAX).to_be_bytes());
    digest.update(bytes);
}

fn tables_digest(starts: &BTreeMap<TableId, Option<OpaqueCursor>>) -> [u8; 32] {
    let mut digest = Sha256::new();
    digest.update(b"calque.plan-tables.v1\0");
    for table in starts.keys() {
        framed(&mut digest, table.as_str().as_bytes());
    }
    digest.finalize().into()
}

fn starts_digest(starts: &BTreeMap<TableId, Option<OpaqueCursor>>) -> [u8; 32] {
    let mut digest = Sha256::new();
    digest.update(b"calque.plan-starts.v1\0");
    for (table, start) in starts {
        framed(&mut digest, table.as_str().as_bytes());
        match start {
            Some(cursor) => {
                digest.update([1]);
                framed(&mut digest, cursor.as_str().as_bytes());
            }
            None => digest.update([0]),
        }
    }
    digest.finalize().into()
}

fn binding_digest(client_instance_id: &str, device_id: &str) -> [u8; 32] {
    let mut digest = Sha256::new();
    digest.update(b"calque.plan-binding.v1\0");
    framed(&mut digest, client_instance_id.as_bytes());
    framed(&mut digest, device_id.as_bytes());
    digest.finalize().into()
}

fn digest_matches(claimed: &str, digest: &[u8; 32]) -> bool {
    URL_SAFE_NO_PAD
        .decode(claimed)
        .is_ok_and(|claimed| bool::from(claimed.ct_eq(digest)))
}

fn tenant_text(value: &PartitionValue) -> String {
    match value {
        PartitionValue::Int(value) => format!("int:{value}"),
        PartitionValue::Uuid(bytes) => format!("uuid:{}", hex::encode(bytes)),
    }
}

fn parse_tenant(text: &str) -> Result<PartitionValue, ()> {
    if let Some(value) = text.strip_prefix("int:") {
        return value.parse().map(PartitionValue::Int).map_err(|_| ());
    }
    let value = text.strip_prefix("uuid:").ok_or(())?;
    let bytes = hex::decode(value).map_err(|_| ())?;
    Ok(PartitionValue::Uuid(bytes.try_into().map_err(|_| ())?))
}

/// One plan's scope as this process rebuilt it from a verified token, with
/// the room its batches share.
pub(crate) struct VerifiedPlan {
    pub(crate) scope_hash: [u8; 32],
    pub(crate) activated_scope: ActivatedChangeScope,
    pub(crate) scope: AuthorizedScope,
    pub(crate) room: PlanRoom,
}

/// The plans this process has verified lately, at most `capacity`, the least
/// recently used dropped first. An entry admits nothing by itself: the token
/// is verified on every request before its entry is looked up.
pub(crate) struct VerifiedPlans {
    entries: StdMutex<Lru>,
}

#[derive(Default)]
struct Lru {
    capacity: usize,
    clock: u64,
    plans: HashMap<PlanId, (u64, Arc<VerifiedPlan>)>,
    by_use: BTreeMap<u64, PlanId>,
}

impl VerifiedPlans {
    pub(crate) fn new(capacity: usize) -> Self {
        Self {
            entries: StdMutex::new(Lru {
                capacity: capacity.max(1),
                ..Lru::default()
            }),
        }
    }

    /// The entry for a verified token's plan, built by `build` unless one
    /// for the same scope is held. A scope's hash covers its generation.
    pub(crate) fn get_or_build<E>(
        &self,
        plan_id: &PlanId,
        scope_hash: &[u8; 32],
        build: impl FnOnce() -> Result<VerifiedPlan, E>,
    ) -> Result<Arc<VerifiedPlan>, E> {
        if let Some(plan) = self
            .lock()
            .touch(plan_id)
            .filter(|plan| &plan.scope_hash == scope_hash)
        {
            return Ok(plan);
        }
        let plan = Arc::new(build()?);
        self.lock().insert(plan_id.clone(), Arc::clone(&plan));
        Ok(plan)
    }

    #[cfg(test)]
    pub(crate) fn len(&self) -> usize {
        self.lock().plans.len()
    }

    fn lock(&self) -> std::sync::MutexGuard<'_, Lru> {
        self.entries
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }
}

impl Lru {
    fn touch(&mut self, plan_id: &PlanId) -> Option<Arc<VerifiedPlan>> {
        self.clock += 1;
        let clock = self.clock;
        let (used, plan) = self.plans.get_mut(plan_id)?;
        self.by_use.remove(used);
        *used = clock;
        self.by_use.insert(clock, plan_id.clone());
        Some(Arc::clone(plan))
    }

    fn insert(&mut self, plan_id: PlanId, plan: Arc<VerifiedPlan>) {
        self.clock += 1;
        if let Some((used, _)) = self.plans.remove(&plan_id) {
            self.by_use.remove(&used);
        }
        while self.plans.len() >= self.capacity {
            let Some((_, oldest)) = self.by_use.pop_first() else {
                break;
            };
            self.plans.remove(&oldest);
        }
        self.by_use.insert(self.clock, plan_id.clone());
        self.plans.insert(plan_id, (self.clock, plan));
    }
}

#[cfg(test)]
mod tests {
    use std::cell::Cell;

    use super::*;
    use crate::{PreparedLocalSource, SecretToken};
    use sync_types::CursorPosition;

    fn key(byte: u8) -> PlanKey {
        PlanKey::new(&SecretToken::from_urlsafe(&URL_SAFE_NO_PAD.encode([byte; 32])).unwrap())
    }

    fn prepared() -> PreparedLocalSource {
        crate::snapshot::tests::prepared_source(SourceEpoch::new("plans-epoch").unwrap())
    }

    fn tokens(key_byte: u8, source: &str, epoch: &str) -> PlanTokens {
        PlanTokens::new(
            key(key_byte),
            SourceId::new(source).unwrap(),
            SourceEpoch::new(epoch).unwrap(),
        )
    }

    fn cursor(prepared: &PreparedLocalSource, sequence: u64) -> OpaqueCursor {
        prepared.activated.cursor_codec().encode(&CursorPosition {
            source_epoch: prepared.activated.source_epoch().clone(),
            sequence,
        })
    }

    fn terms(prepared: &PreparedLocalSource) -> PlanTerms {
        let table = prepared.scope.permitted_tables().next().unwrap().clone();
        PlanTerms {
            plan_id: PlanId::new("plan-1").unwrap(),
            from_cursor: cursor(prepared, 3),
            through_cursor: cursor(prepared, 5),
            table_starts: BTreeMap::from([(table, Some(cursor(prepared, 3)))]),
        }
    }

    fn claims(tokens: &PlanTokens, expires_at: i64) -> PlanClaims {
        let prepared = prepared();
        tokens.claims(
            &terms(&prepared),
            &prepared.scope,
            expires_at,
            "client-1",
            "device-1",
        )
    }

    #[test]
    fn a_token_opens_for_its_key_source_and_epoch_until_it_expires() {
        let signer = tokens(1, "source-1", "epoch-1");
        let expires_at = 2_000_000;
        let signed = claims(&signer, expires_at);
        let token = signer.sign(&signed);
        assert_eq!(
            signer.open(token.as_bytes(), expires_at - 1),
            Ok(signed.clone())
        );
        for now in [expires_at, expires_at + 1] {
            assert_eq!(signer.open(token.as_bytes(), now), Err(TokenRefusal::Stale));
        }
        assert_eq!(
            tokens(2, "source-1", "epoch-1").open(token.as_bytes(), 0),
            Err(TokenRefusal::Forged)
        );
        assert_eq!(
            tokens(1, "source-2", "epoch-1").open(token.as_bytes(), 0),
            Err(TokenRefusal::Stale)
        );
        assert_eq!(
            tokens(1, "source-1", "epoch-2").open(token.as_bytes(), 0),
            Err(TokenRefusal::Stale)
        );
        assert_eq!(signer.open(&[0xff, 0xfe], 0), Err(TokenRefusal::Forged));

        let mut other_key = signed;
        other_key.key_id = "0000000000000000".to_owned();
        assert_eq!(
            signer.open(signer.sign(&other_key).as_bytes(), 0),
            Err(TokenRefusal::Forged)
        );
    }

    #[test]
    fn a_plan_token_never_opens_as_an_artifact_capability_or_the_reverse() {
        let signer = tokens(1, "source-1", "epoch-1");
        let token = signer.sign(&claims(&signer, 2_000_000));
        assert_eq!(
            key(1).open::<PlanClaims>(crate::signing::ARTIFACT_CAPABILITY_DOMAIN, &token),
            None
        );
        let (payload, _) = token.split_once('.').unwrap();
        let payload: serde_json::Value =
            serde_json::from_slice(&URL_SAFE_NO_PAD.decode(payload).unwrap()).unwrap();
        let capability = key(1).sign(crate::signing::ARTIFACT_CAPABILITY_DOMAIN, &payload);
        assert_eq!(
            signer.open(capability.as_bytes(), 0),
            Err(TokenRefusal::Forged)
        );
    }

    #[test]
    fn the_binding_is_the_client_instance_and_device_the_bootstrap_named() {
        let signed = claims(&tokens(1, "source-1", "epoch-1"), 2_000_000);
        assert!(signed.binds("client-1", "device-1"));
        assert!(!signed.binds("client-2", "device-1"));
        assert!(!signed.binds("client-1", "device-2"));
        assert!(!signed.binds("client-1device-1", ""));
        assert!(!signed.binds("", "client-1device-1"));
    }

    #[test]
    fn a_request_names_the_plans_tables_and_on_catch_up_their_starts() {
        let prepared = prepared();
        let signed = claims(&tokens(1, "source-1", "epoch-1"), 2_000_000);
        let starts = terms(&prepared).table_starts;
        let table = starts.keys().next().unwrap().clone();
        for live in [false, true] {
            assert!(signed.check_tables(&starts, live).is_ok());
        }
        let mut moved = starts.clone();
        moved.insert(table.clone(), Some(cursor(&prepared, 4)));
        assert!(matches!(
            signed.check_tables(&moved, false),
            Err(ChangeBatchError::InvalidPlanBinding)
        ));
        assert!(signed.check_tables(&moved, true).is_ok());
        let mut resident = starts.clone();
        resident.insert(table, None);
        assert!(matches!(
            signed.check_tables(&resident, false),
            Err(ChangeBatchError::InvalidPlanBinding)
        ));
        let mut wider = starts.clone();
        wider.insert(TableId::new("t_other").unwrap(), None);
        for tables in [wider, BTreeMap::new()] {
            for live in [false, true] {
                assert!(matches!(
                    signed.check_tables(&tables, live),
                    Err(ChangeBatchError::InvalidTableSet)
                ));
            }
        }
    }

    #[test]
    fn a_tenant_reads_back_as_the_value_it_was_signed_with() {
        for tenant in [
            PartitionValue::Int(-5),
            PartitionValue::Int(i64::MAX),
            PartitionValue::Uuid([7; 16]),
        ] {
            assert_eq!(parse_tenant(&tenant_text(&tenant)), Ok(tenant));
        }
        for text in ["", "int:", "int:1.5", "uuid:00", "text:a", "uuid:zz"] {
            assert_eq!(parse_tenant(text), Err(()), "{text}");
        }
    }

    fn verified(prepared: &PreparedLocalSource, scope_hash: [u8; 32]) -> VerifiedPlan {
        VerifiedPlan {
            scope_hash,
            activated_scope: ActivatedChangeScope::new(
                SourceId::new("source-1").unwrap(),
                &prepared.activated,
                prepared.scope.clone(),
            )
            .unwrap(),
            scope: prepared.scope.clone(),
            room: PlanRoom::new(),
        }
    }

    #[test]
    fn the_cache_keeps_the_most_recently_used_and_rebuilds_on_any_mismatch() {
        let prepared = prepared();
        let plans = VerifiedPlans::new(2);
        let builds = Cell::new(0);
        let get = |plan: &str, hash: [u8; 32]| {
            plans
                .get_or_build(&PlanId::new(plan).unwrap(), &hash, || {
                    builds.set(builds.get() + 1);
                    Ok::<_, ()>(verified(&prepared, hash))
                })
                .unwrap()
        };
        get("plan-a", [1; 32]);
        get("plan-b", [1; 32]);
        get("plan-a", [1; 32]);
        assert_eq!(builds.get(), 2);
        get("plan-c", [1; 32]);
        assert_eq!(plans.len(), 2);
        get("plan-a", [1; 32]);
        assert_eq!(builds.get(), 3);
        get("plan-b", [1; 32]);
        assert_eq!(builds.get(), 4);

        let rebuilt = get("plan-b", [2; 32]);
        assert_eq!((builds.get(), rebuilt.scope_hash), (5, [2; 32]));
        assert_eq!(plans.len(), 2);

        let refused = plans.get_or_build(&PlanId::new("plan-d").unwrap(), &[1; 32], || {
            Err::<VerifiedPlan, _>("refused")
        });
        assert_eq!(refused.err(), Some("refused"));
        assert_eq!(plans.len(), 2);
    }
}
