//! Bootstrap planning over the tables a client names, and provider-neutral
//! delivery resolution.
//!
//! The planner receives the stable table IDs a client requests. It never
//! parses SQL and never treats browser cache hints as proof. Every plan
//! captures one durable canonical-log head, revalidates the selected published
//! artifacts through the blob store, and resolves dynamic URLs outside
//! immutable manifests.

use std::{
    cmp::Ordering,
    collections::{BTreeMap, BTreeSet},
    sync::OnceLock,
    time::{Duration as StdDuration, Instant, SystemTime, UNIX_EPOCH},
};

use artifact_catalog::{CatalogError, SqliteArtifactCatalog};
use artifact_contract::{
    ArtifactCacheKey, ArtifactColumn, ArtifactRepresentation, BootstrapArtifactInstall,
    BootstrapSourceCompatibility, DecodedArtifactFormat, RepresentationEncoding,
    SelectedRepresentation, ValidatedArtifactManifest,
};
use async_trait::async_trait;
use authz::AuthorizedScope;
use base64::{engine::general_purpose::URL_SAFE_NO_PAD, Engine as _};
use blob_store::BlobStore;
use canonical_log::{CanonicalLogPlanRetention, CanonicalLogRead};
use hmac::{Hmac, Mac};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use sha2::Sha256;
use snapshot_builder::{
    COLLATION_GENERATION_V1, NO_INDEX_PROFILE_HASH, PUBLICATION_GENERATION_V1,
    SOURCE_WIDE_SHARD_PROFILE_HASH, TYPE_CODEC_GENERATION_V1,
};
use source_control::ActivatedSource;
use sync_types::{ArtifactId, CursorPosition, ObjectId, OpaqueCursor, PlanId, TableId};
use thiserror::Error;
use time::{format_description::well_known::Rfc3339, OffsetDateTime};
use url::Url;
use uuid::Uuid;

const PLAN_SCHEMA_JSON: &str = include_str!("../../../../spec/bootstrap-plan.schema.json");
const MAX_REQUIRED_TABLES: usize = 256;
const MAX_RESIDENT_TABLES: usize = 1_024;
const MAX_SUBSCRIBED_TABLES: usize = 1_024;
const MAX_CANDIDATES: usize = 4_096;
const MAX_DECODER_PROFILE_BYTES: usize = 128;
const MAX_COVER_SEARCH_STATES: usize = 100_000;
const MAX_COVER_SEARCH_OPERATIONS: usize = 5_000_000;
const MAX_CACHE_HINTS: usize = 4_096;
const MAX_CHANGE_BATCH_BYTES: u64 = 16 * 1024 * 1024;
const MAX_TOKEN_BYTES: usize = 8 * 1024;
const MAX_RESIDENT_PROOF_BYTES: usize = 1024 * 1024;
const MAX_PRINCIPAL_BYTES: usize = 1_024;
const MAX_PLAN_TTL: StdDuration = StdDuration::from_secs(60 * 60);
const MAX_PLANNING_TIME: StdDuration = StdDuration::from_secs(30);
const MIN_PROOF_SECRET_BYTES: usize = 32;
const RESIDENT_PROOF_VERSION: u8 = 1;
const RESIDENT_PROOF_DOMAIN: &[u8] = b"calque.bootstrap.resident-state.v1\0";

static PLAN_SCHEMA: OnceLock<Result<jsonschema::Validator, String>> = OnceLock::new();

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DeliveryPurpose {
    ArtifactRepresentation,
}

/// Authenticated identity passed only to a delivery adapter.
///
/// `subject` never appears in the serialized bootstrap plan.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DeliveryPrincipal {
    subject: String,
}

impl DeliveryPrincipal {
    pub fn new(subject: impl Into<String>) -> Result<Self, DeliveryError> {
        let subject = subject.into();
        validate_bounded_text(&subject, MAX_PRINCIPAL_BYTES)?;
        Ok(Self { subject })
    }
}

/// Authorization the server minted for one verified principal's scope.
///
/// The opaque token and typed scope travel as one capability. The planner never
/// accepts authorization generations or hashes from an HTTP request.
#[derive(Clone)]
pub struct AuthorizedBootstrapScope {
    principal: DeliveryPrincipal,
    source_epoch: sync_types::SourceEpoch,
    scope: AuthorizedScope,
    scope_token: String,
    expires_at: OffsetDateTime,
}

impl AuthorizedBootstrapScope {
    pub fn new(
        principal: DeliveryPrincipal,
        source: &ActivatedSource,
        scope: AuthorizedScope,
        scope_token: impl Into<String>,
        expires_at: OffsetDateTime,
    ) -> Result<Self, BootstrapError> {
        let scope_token = scope_token.into();
        validate_bounded_text(&scope_token, MAX_TOKEN_BYTES)
            .map_err(|_| BootstrapError::InvalidAuthorizationToken)?;
        if scope.registry_binding() != Some(source.registry_binding()) {
            return Err(BootstrapError::AuthorizationSourceMismatch);
        }
        Ok(Self {
            principal,
            source_epoch: source.source_epoch().clone(),
            scope,
            scope_token,
            expires_at,
        })
    }
}

impl std::fmt::Debug for AuthorizedBootstrapScope {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("AuthorizedBootstrapScope")
            .field("principal", &self.principal)
            .field("source_epoch", &self.source_epoch)
            .field("scope", &self.scope)
            .field("scope_token", &"[REDACTED]")
            .field("expires_at", &self.expires_at)
            .finish()
    }
}

/// Authenticates client resident-state claims without storing browser state.
#[derive(Clone)]
pub struct ResidentStateProofCodec {
    secret: Vec<u8>,
}

impl ResidentStateProofCodec {
    pub fn new(secret: impl AsRef<[u8]>) -> Result<Self, ResidentProofError> {
        let secret = secret.as_ref();
        if secret.len() < MIN_PROOF_SECRET_BYTES {
            return Err(ResidentProofError::WeakSecret);
        }
        Ok(Self {
            secret: secret.to_vec(),
        })
    }

    fn encode(
        &self,
        cache_key: &ArtifactCacheKey,
        tables: &BTreeSet<TableId>,
        cursor: &OpaqueCursor,
    ) -> Result<String, ResidentProofError> {
        if tables.len() > MAX_RESIDENT_TABLES {
            return Err(ResidentProofError::TooManyTables(tables.len()));
        }
        let payload = serde_json::to_vec(&ResidentStatePayload {
            version: RESIDENT_PROOF_VERSION,
            cache_key: cache_key.clone(),
            tables: tables.iter().cloned().collect(),
            cursor: cursor.clone(),
        })?;
        let signature = self.sign(&payload);
        let proof = format!(
            "{}.{}",
            URL_SAFE_NO_PAD.encode(payload),
            URL_SAFE_NO_PAD.encode(signature)
        );
        if proof.len() > MAX_RESIDENT_PROOF_BYTES {
            return Err(ResidentProofError::TooLarge);
        }
        Ok(proof)
    }

    fn decode(
        &self,
        proof: &str,
        expected_cache_key: &ArtifactCacheKey,
    ) -> Result<ResidentStatePayload, ResidentProofError> {
        if proof.is_empty() || proof.len() > MAX_RESIDENT_PROOF_BYTES {
            return Err(ResidentProofError::TooLarge);
        }
        let (encoded_payload, encoded_signature) =
            proof.split_once('.').ok_or(ResidentProofError::Framing)?;
        if encoded_signature.contains('.') {
            return Err(ResidentProofError::Framing);
        }
        let payload = URL_SAFE_NO_PAD
            .decode(encoded_payload)
            .map_err(|_| ResidentProofError::Encoding)?;
        let signature = URL_SAFE_NO_PAD
            .decode(encoded_signature)
            .map_err(|_| ResidentProofError::Encoding)?;
        let mut mac = Hmac::<Sha256>::new_from_slice(&self.secret)
            .expect("HMAC accepts every non-empty key length");
        mac.update(RESIDENT_PROOF_DOMAIN);
        mac.update(&payload);
        mac.verify_slice(&signature)
            .map_err(|_| ResidentProofError::Signature)?;
        let state: ResidentStatePayload = serde_json::from_slice(&payload)?;
        if state.version != RESIDENT_PROOF_VERSION {
            return Err(ResidentProofError::Version(state.version));
        }
        if &state.cache_key != expected_cache_key {
            return Err(ResidentProofError::Compatibility);
        }
        if state.tables.is_empty()
            || state.tables.len() > MAX_RESIDENT_TABLES
            || state.tables.windows(2).any(|pair| pair[0] >= pair[1])
        {
            return Err(ResidentProofError::Tables);
        }
        Ok(state)
    }

    fn sign(&self, payload: &[u8]) -> Vec<u8> {
        let mut mac = Hmac::<Sha256>::new_from_slice(&self.secret)
            .expect("HMAC accepts every non-empty key length");
        mac.update(RESIDENT_PROOF_DOMAIN);
        mac.update(payload);
        mac.finalize().into_bytes().to_vec()
    }
}

impl std::fmt::Debug for ResidentStateProofCodec {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("ResidentStateProofCodec")
            .field("secret", &"[REDACTED]")
            .finish()
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct ResidentStatePayload {
    version: u8,
    cache_key: ArtifactCacheKey,
    tables: Vec<TableId>,
    cursor: OpaqueCursor,
}

/// A syntactically validated, dynamically fetchable object location.
#[derive(Clone, PartialEq, Eq)]
pub struct DeliveryLocation {
    url: Url,
    expires_at: Option<OffsetDateTime>,
}

impl std::fmt::Debug for DeliveryLocation {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("DeliveryLocation")
            .field("url", &"[REDACTED]")
            .field("expires_at", &self.expires_at)
            .finish()
    }
}

impl DeliveryLocation {
    pub fn new(
        url: impl AsRef<str>,
        expires_at: Option<OffsetDateTime>,
    ) -> Result<Self, DeliveryError> {
        let url = Url::parse(url.as_ref()).map_err(DeliveryError::InvalidUrl)?;
        validate_http_url(&url)?;
        Ok(Self { url, expires_at })
    }

    pub fn url(&self) -> &Url {
        &self.url
    }

    pub const fn expires_at(&self) -> Option<OffsetDateTime> {
        self.expires_at
    }
}

#[async_trait]
pub trait DeliveryResolver: Send + Sync {
    /// Resolve an object grant whose advertised lifetime is bounded by the
    /// planner's authoritative expiry.
    async fn resolve(
        &self,
        principal: &DeliveryPrincipal,
        object: &ObjectId,
        purpose: DeliveryPurpose,
        plan_expires_at: OffsetDateTime,
    ) -> Result<DeliveryLocation, DeliveryError>;
}

#[derive(Clone)]
pub struct BootstrapRequest {
    pub required_tables: BTreeSet<TableId>,
    pub resident_state_proof: Option<String>,
    /// Exact client-resident subset authenticated by `resident_state_proof`.
    pub resident_tables: BTreeSet<TableId>,
    pub authorization: AuthorizedBootstrapScope,
    pub cached_artifact_hints: BTreeSet<ArtifactId>,
    pub accepted_encodings: Vec<RepresentationEncoding>,
    /// The artifact decoder this client actually has, matched against
    /// [`DecodedArtifactFormat::decoder_profile`]. An artifact built for any
    /// other decoder is not a candidate.
    pub decoder_profile: String,
    pub memory: ClientMemoryBudget,
    pub change_endpoint: DeliveryLocation,
    pub replan_endpoint: Option<DeliveryLocation>,
    pub plan_ttl: StdDuration,
    pub long_poll_timeout_ms: u32,
    pub max_change_batch_bytes: u64,
    /// What the caller knows of the log for this scope. When the plan's
    /// catch-up starts at or after `last_change` and the log's head is at or
    /// before `through`, the plan activates where its catch-up starts, so the
    /// client has nothing to catch up.
    pub unchanged: Option<UnchangedScope>,
}

/// No operation the scope reads committed after `last_change`, up to and
/// including `through`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct UnchangedScope {
    pub last_change: u64,
    pub through: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ClientMemoryBudget {
    pub safe_budget_bytes: u64,
    pub current_active_artifact_bytes: u64,
    pub staging_overlap_bytes: u64,
    pub catch_up_buffer_bytes: u64,
    pub query_result_reserve_bytes: u64,
    /// Fixed main-thread engine heap category precharged inside the hard budget.
    pub client_heap_reserve_bytes: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct BootstrapPlan {
    pub protocol_version: u32,
    pub plan_id: PlanId,
    pub issued_at: String,
    pub expires_at: String,
    pub source: PlanSource,
    pub authorization: PlanAuthorization,
    pub required_tables: Vec<TableId>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub resident_tables: Vec<TableId>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub resident_cursor: Option<OpaqueCursor>,
    pub activation_cursor: OpaqueCursor,
    pub next_resident_state_proof: String,
    pub artifacts: Vec<PlanArtifactInstall>,
    pub change_stream: PlanChangeStream,
    pub memory: PlanMemory,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub replan: Option<PlanReplan>,
    /// Server-side planning telemetry; never serialized onto the wire.
    #[serde(skip, default)]
    pub diagnostics: BootstrapPlanningDiagnostics,
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct BootstrapPlanningDiagnostics {
    pub catalog_candidate_rows: u64,
    pub eligible_candidates: u64,
    pub cover_search_states: u64,
    pub cover_search_operations: u64,
    pub selected_artifacts: u64,
    pub selected_cache_hints: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct PlanSource {
    pub source_epoch: sync_types::SourceEpoch,
    pub publication_generation: u64,
    pub schema_generation: u64,
    pub authorization_generation: u64,
    pub authorization_policy_hash: String,
}

#[derive(Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct PlanAuthorization {
    pub policy_hash: String,
    pub scope_hash: String,
    pub scope_token: String,
}

impl std::fmt::Debug for PlanAuthorization {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("PlanAuthorization")
            .field("policy_hash", &self.policy_hash)
            .field("scope_hash", &self.scope_hash)
            .field("scope_token", &"[REDACTED]")
            .finish()
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct PlanArtifactInstall {
    pub artifact_id: ArtifactId,
    pub manifest_id: ArtifactId,
    pub attach_as: String,
    pub snapshot_cursor: OpaqueCursor,
    pub tables: Vec<TableId>,
    pub decoded_sha256: String,
    pub decoded_bytes: u64,
    pub buffer_capacity_bytes: u64,
    pub install_order: usize,
    /// Authenticated runtime metadata required to validate and attach the
    /// decoded artifact without a second manifest round trip.
    pub runtime: PlanArtifactRuntime,
    pub selected_representation: PlanRepresentation,
}

/// Install-time facts for one selected artifact.
///
/// `deny_unknown_fields` cannot be combined with a flattened field, so
/// structural strictness lives in `spec/bootstrap-plan.schema.json`, which every
/// plan passes through `validate_serialized_plan` before it is returned.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct PlanArtifactRuntime {
    pub table_set_hash: String,
    pub shard_profile_hash: String,
    pub index_profile_hash: String,
    /// Copied from the immutable manifest rather than re-derived, so a plan
    /// cannot describe the artifact as a format its bytes are not.
    #[serde(flatten)]
    pub format: DecodedArtifactFormat,
    pub tables: Vec<PlanArtifactTable>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct PlanArtifactTable {
    pub table_id: TableId,
    pub source_schema: String,
    pub source_table: String,
    pub local_table: String,
    pub schema_hash: String,
    pub column_profile_hash: String,
    pub row_count: u64,
    pub primary_key_codec: String,
    /// The change stream's column ids for this table, copied from the
    /// manifest; the client compiles its change contract from them.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub columns: Vec<ArtifactColumn>,
}

#[derive(Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct PlanRepresentation {
    pub representation_id: String,
    pub encoding: RepresentationEncoding,
    pub encoded_bytes: u64,
    pub download_url: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub expires_at: Option<String>,
}

impl std::fmt::Debug for PlanRepresentation {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("PlanRepresentation")
            .field("representation_id", &self.representation_id)
            .field("encoding", &self.encoding)
            .field("encoded_bytes", &self.encoded_bytes)
            .field("download_url", &"[REDACTED]")
            .field("expires_at", &self.expires_at)
            .finish()
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct PlanChangeStream {
    pub endpoint: String,
    pub wire_protocol: String,
    pub from_cursor_exclusive: OpaqueCursor,
    pub through_cursor_inclusive: OpaqueCursor,
    pub long_poll_timeout_ms: u32,
    pub max_batch_bytes: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct PlanMemory {
    pub minimum_safe_budget_bytes: u64,
    pub staging_overlap_bytes: u64,
    pub query_result_reserve_bytes: u64,
    pub catch_up_reserve_bytes: u64,
    pub client_heap_reserve_bytes: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct PlanReplan {
    pub endpoint: String,
    pub allowed_after_network_miss: bool,
    pub allowed_after_allocation_failure: bool,
}

#[derive(Debug, Clone)]
struct Candidate {
    manifest: ValidatedArtifactManifest,
    representation: ArtifactRepresentation,
    snapshot_position: CursorPosition,
    required_coverage: BTreeSet<TableId>,
    tables: BTreeSet<TableId>,
    cache_hint: bool,
}

struct CandidateContext<'a> {
    missing: &'a BTreeSet<TableId>,
    accepted_encodings: &'a [RepresentationEncoding],
    cache_hints: &'a BTreeSet<ArtifactId>,
    source: &'a ActivatedSource,
    minimum_retained: u64,
    head: u64,
}

pub struct BootstrapPlanner;

impl BootstrapPlanner {
    pub async fn plan<
        S: BlobStore + ?Sized,
        R: DeliveryResolver + ?Sized,
        L: CanonicalLogRead + CanonicalLogPlanRetention + ?Sized,
    >(
        catalog: &mut SqliteArtifactCatalog,
        store: &S,
        resolver: &R,
        source: &ActivatedSource,
        log: &mut L,
        resident_proofs: &ResidentStateProofCodec,
        request: BootstrapRequest,
    ) -> Result<BootstrapPlan, BootstrapError> {
        let timeout = request.plan_ttl.min(MAX_PLANNING_TIME);
        tokio::time::timeout(
            timeout,
            Self::plan_inner(
                catalog,
                store,
                resolver,
                source,
                log,
                resident_proofs,
                request,
            ),
        )
        .await
        .map_err(|_| BootstrapError::PlanningDeadline)?
    }

    async fn plan_inner<
        S: BlobStore + ?Sized,
        R: DeliveryResolver + ?Sized,
        L: CanonicalLogRead + CanonicalLogPlanRetention + ?Sized,
    >(
        catalog: &mut SqliteArtifactCatalog,
        store: &S,
        resolver: &R,
        source: &ActivatedSource,
        log: &mut L,
        resident_proofs: &ResidentStateProofCodec,
        request: BootstrapRequest,
    ) -> Result<BootstrapPlan, BootstrapError> {
        let planning_deadline = Instant::now() + request.plan_ttl.min(MAX_PLANNING_TIME);
        let mut diagnostics = BootstrapPlanningDiagnostics::default();
        validate_request(source, log, &request)?;
        let cache_key = authorized_cache_key(
            source,
            &request.authorization.scope,
            &request.decoder_profile,
        );
        // Delivery grants in the local composition are enforced at microsecond
        // precision. Keep generated plan timestamps on that exact boundary so
        // the advertised expiry can be used without lossy rounding.
        let issued_at = truncate_to_microseconds(OffsetDateTime::from(SystemTime::now()))?;
        let valid_for = time::Duration::try_from(request.plan_ttl)
            .map_err(|_| BootstrapError::InvalidPlanTtl)?;
        let configured_expiry = issued_at
            .checked_add(valid_for)
            .ok_or(BootstrapError::TimestampOverflow)?;
        let expires_at = configured_expiry.min(request.authorization.expires_at);
        if request
            .change_endpoint
            .expires_at()
            .is_some_and(|expiry| expiry < expires_at)
            || request
                .replan_endpoint
                .as_ref()
                .and_then(DeliveryLocation::expires_at)
                .is_some_and(|expiry| expiry < expires_at)
        {
            return Err(BootstrapError::DeliveryExpiresBeforePlan);
        }
        if expires_at <= issued_at {
            return Err(BootstrapError::AuthorizationExpiresBeforePlan);
        }
        let now_ms = system_time_millis()?;
        let expires_ms = timestamp_millis(expires_at)?;
        let plan_id = PlanId::new(format!("plan-{}", Uuid::new_v4()))?;
        let bounds = log.bounds()?;
        let head = bounds.head;
        let minimum_retained = bounds.minimum_retained.sequence;

        let resident_state = request
            .resident_state_proof
            .as_ref()
            .map(|proof| resident_proofs.decode(proof, &cache_key))
            .transpose()
            .map_err(BootstrapError::ResidentProof)?;
        let proven_resident_tables: BTreeSet<_> =
            resident_state.as_ref().map_or_else(BTreeSet::new, |state| {
                state.tables.iter().cloned().collect()
            });
        if !request.resident_tables.is_subset(&proven_resident_tables) {
            return Err(BootstrapError::ResidentClaimOutsideProof);
        }
        let resident_tables = request.resident_tables.clone();
        let resident_cursor = (!resident_tables.is_empty())
            .then(|| resident_state.as_ref().map(|state| state.cursor.clone()))
            .flatten();
        let resident_position = resident_cursor
            .as_ref()
            .map(|cursor| {
                source
                    .cursor_codec()
                    .decode_for_epoch(cursor, source.source_epoch())
            })
            .transpose()
            .map_err(BootstrapError::ResidentCursor)?;
        if let Some(position) = &resident_position {
            validate_cursor_window(position, minimum_retained, head.sequence)
                .map_err(BootstrapError::ResidentCursorWindow)?;
        }
        let registry_tables: BTreeSet<_> = source
            .registry_binding()
            .tables()
            .iter()
            .map(|(table, _)| table.clone())
            .collect();
        if !resident_tables.is_subset(&registry_tables) {
            return Err(BootstrapError::ResidentProofOutsideRegistry);
        }

        let missing: BTreeSet<_> = request
            .required_tables
            .difference(&resident_tables)
            .cloned()
            .collect();
        let mut selected = loop {
            if missing.is_empty() {
                break Vec::new();
            }
            let manifests =
                catalog.published_compatible_for_tables(&cache_key, &missing, MAX_CANDIDATES)?;
            diagnostics.catalog_candidate_rows = diagnostics
                .catalog_candidate_rows
                .saturating_add(saturating_usize(manifests.len()));
            if manifests.len() > MAX_CANDIDATES {
                return Err(BootstrapError::TooManyCandidates(manifests.len()));
            }
            let candidates = compile_candidates(
                manifests,
                CandidateContext {
                    missing: &missing,
                    accepted_encodings: &request.accepted_encodings,
                    cache_hints: &request.cached_artifact_hints,
                    source,
                    minimum_retained,
                    head: head.sequence,
                },
            )?;
            diagnostics.eligible_candidates = diagnostics
                .eligible_candidates
                .saturating_add(saturating_usize(candidates.len()));
            let cover =
                select_feasible_cover(candidates, &missing, request.memory, planning_deadline)?;
            diagnostics.cover_search_states = diagnostics
                .cover_search_states
                .saturating_add(saturating_usize(cover.states));
            diagnostics.cover_search_operations = diagnostics
                .cover_search_operations
                .saturating_add(saturating_usize(cover.operations));
            let selected = cover.candidates;
            let mut stale = false;
            for candidate in &selected {
                let reverify = catalog
                    .reverify_published(&candidate.manifest.manifest().artifact_id, store, now_ms)
                    .await;
                match reverify {
                    Ok(verified) if verified == candidate.manifest => {}
                    Ok(_) => return Err(BootstrapError::CatalogManifestChanged),
                    Err(
                        CatalogError::MissingObject(_)
                        | CatalogError::ObjectMetadataMismatch { .. }
                        | CatalogError::ArtifactIsNotPublished(_),
                    ) => {
                        stale = true;
                        break;
                    }
                    Err(error) => return Err(error.into()),
                }
            }
            if !stale {
                let planned_tables =
                    selected
                        .iter()
                        .fold(resident_tables.clone(), |mut tables, candidate| {
                            tables.extend(candidate.tables.iter().cloned());
                            tables
                        });
                if planned_tables.len() > MAX_SUBSCRIBED_TABLES {
                    return Err(BootstrapError::TooManySubscribedTables(
                        planned_tables.len(),
                    ));
                }
                let artifacts: Vec<_> = selected
                    .iter()
                    .map(|candidate| candidate.manifest.manifest().artifact_id.clone())
                    .collect();
                match catalog.pin_published_for_plan(&plan_id, &artifacts, expires_ms, now_ms) {
                    Ok(()) => {}
                    Err(CatalogError::ArtifactIsNotPublished(_)) => stale = true,
                    Err(error) => return Err(error.into()),
                }
            }
            if !stale {
                break selected;
            }
        };

        selected.sort_by(|left, right| {
            left.manifest
                .manifest()
                .artifact_id
                .cmp(&right.manifest.manifest().artifact_id)
        });
        let memory = compute_memory(&selected, request.memory)?;
        diagnostics.selected_artifacts = saturating_usize(selected.len());
        diagnostics.selected_cache_hints = saturating_usize(
            selected
                .iter()
                .filter(|candidate| candidate.cache_hint)
                .count(),
        );
        let source_compatibility = BootstrapSourceCompatibility {
            source_epoch: cache_key.source_epoch.clone(),
            publication_generation: cache_key.publication_generation,
            schema_generation: cache_key.schema_generation,
            authorization_generation: cache_key.authorization_generation,
            authorization_policy_hash: cache_key.authorization_policy_hash.clone(),
            authorization_scope_hash: cache_key.authorization_scope_hash.clone(),
        };

        let mut catch_up_start = resident_position;
        for candidate in &selected {
            if catch_up_start
                .as_ref()
                .is_none_or(|current| candidate.snapshot_position.sequence < current.sequence)
            {
                catch_up_start = Some(candidate.snapshot_position.clone());
            }
        }
        let catch_up_start = catch_up_start.ok_or(BootstrapError::NoCatchUpStart)?;
        // The scope's state where its catch-up starts is its state at the
        // head when nothing it reads committed in between.
        let unchanged = request.unchanged.is_some_and(|unchanged| {
            unchanged.last_change <= catch_up_start.sequence && head.sequence <= unchanged.through
        });
        let activation_cursor =
            source
                .cursor_codec()
                .encode(if unchanged { &catch_up_start } else { &head });
        log.acquire_plan_retention_lease(
            plan_id.clone(),
            catch_up_start.sequence.saturating_add(1),
            expires_ms,
            now_ms,
        )?;

        let mut installs = Vec::with_capacity(selected.len());
        let mut next_resident_tables = resident_tables.clone();
        for (order, candidate) in selected.into_iter().enumerate() {
            let manifest = candidate.manifest.manifest();
            let location = resolver
                .resolve(
                    &request.authorization.principal,
                    &candidate.representation.object_id,
                    DeliveryPurpose::ArtifactRepresentation,
                    expires_at,
                )
                .await?;
            if location
                .expires_at()
                .is_some_and(|expiry| expiry < expires_at)
            {
                return Err(BootstrapError::DeliveryExpiresBeforePlan);
            }
            let buffer_capacity_bytes = manifest
                .decoded
                .bytes
                .checked_add(manifest.memory.recommended_growth_reserve_bytes)
                .ok_or(BootstrapError::MemoryOverflow)?;
            let tables: Vec<_> = candidate.tables.iter().cloned().collect();
            next_resident_tables.extend(candidate.tables.iter().cloned());
            let selection = BootstrapArtifactInstall {
                artifact_id: manifest.artifact_id.clone(),
                snapshot_cursor: manifest.pit.snapshot_cursor.clone(),
                tables: tables.clone(),
                decoded_sha256: manifest.decoded.sha256.clone(),
                decoded_bytes: manifest.decoded.bytes,
                buffer_capacity_bytes,
                selected_representation: SelectedRepresentation {
                    representation_id: candidate.representation.representation_id.clone(),
                    encoding: candidate.representation.encoding,
                    encoded_bytes: candidate.representation.encoded_bytes,
                },
            };
            candidate
                .manifest
                .validate_bootstrap_selection(&source_compatibility, &selection)?;
            installs.push(PlanArtifactInstall {
                artifact_id: manifest.artifact_id.clone(),
                // The catalog addresses immutable manifest bytes by artifact ID.
                manifest_id: manifest.artifact_id.clone(),
                attach_as: format!("stage_{order:04}"),
                snapshot_cursor: manifest.pit.snapshot_cursor.clone(),
                tables,
                decoded_sha256: manifest.decoded.sha256.clone(),
                decoded_bytes: manifest.decoded.bytes,
                buffer_capacity_bytes,
                install_order: order,
                runtime: PlanArtifactRuntime {
                    table_set_hash: manifest.compatibility.table_set_hash.clone(),
                    shard_profile_hash: manifest.compatibility.shard_profile_hash.clone(),
                    index_profile_hash: manifest.compatibility.index_profile_hash.clone(),
                    format: manifest.decoded.format.clone(),
                    tables: manifest
                        .tables
                        .iter()
                        .map(|table| PlanArtifactTable {
                            table_id: table.table_id.clone(),
                            source_schema: table.source_schema.clone(),
                            source_table: table.source_table.clone(),
                            local_table: table.local_table.clone(),
                            schema_hash: table.schema_hash.clone(),
                            column_profile_hash: table.column_profile_hash.clone(),
                            row_count: table.row_count,
                            primary_key_codec: table.primary_key_codec.clone(),
                            columns: table.columns.clone(),
                        })
                        .collect(),
                },
                selected_representation: PlanRepresentation {
                    representation_id: candidate.representation.representation_id,
                    encoding: candidate.representation.encoding,
                    encoded_bytes: candidate.representation.encoded_bytes,
                    download_url: location.url().as_str().to_owned(),
                    expires_at: location.expires_at().map(format_timestamp).transpose()?,
                },
            });
        }
        let next_resident_state_proof =
            resident_proofs.encode(&cache_key, &next_resident_tables, &activation_cursor)?;
        let plan = BootstrapPlan {
            protocol_version: 1,
            plan_id,
            issued_at: format_timestamp(issued_at)?,
            expires_at: format_timestamp(expires_at)?,
            source: PlanSource {
                source_epoch: cache_key.source_epoch.clone(),
                publication_generation: cache_key.publication_generation,
                schema_generation: cache_key.schema_generation,
                authorization_generation: cache_key.authorization_generation,
                authorization_policy_hash: cache_key.authorization_policy_hash.clone(),
            },
            authorization: PlanAuthorization {
                policy_hash: cache_key.authorization_policy_hash.clone(),
                scope_hash: cache_key.authorization_scope_hash.clone(),
                scope_token: request.authorization.scope_token,
            },
            required_tables: request.required_tables.into_iter().collect(),
            resident_tables: resident_tables.into_iter().collect(),
            resident_cursor,
            activation_cursor: activation_cursor.clone(),
            next_resident_state_proof,
            artifacts: installs,
            change_stream: PlanChangeStream {
                endpoint: request.change_endpoint.url().as_str().to_owned(),
                wire_protocol: "sync.change.v1".to_owned(),
                from_cursor_exclusive: source.cursor_codec().encode(&catch_up_start),
                through_cursor_inclusive: activation_cursor,
                long_poll_timeout_ms: request.long_poll_timeout_ms,
                max_batch_bytes: request.max_change_batch_bytes,
            },
            memory,
            replan: request.replan_endpoint.map(|endpoint| PlanReplan {
                endpoint: endpoint.url().as_str().to_owned(),
                allowed_after_network_miss: true,
                allowed_after_allocation_failure: true,
            }),
            diagnostics,
        };
        if OffsetDateTime::now_utc() >= expires_at {
            return Err(BootstrapError::PlanningDeadline);
        }
        validate_serialized_plan(&plan)?;
        Ok(plan)
    }
}

fn validate_request<L: CanonicalLogRead + ?Sized>(
    source: &ActivatedSource,
    log: &L,
    request: &BootstrapRequest,
) -> Result<(), BootstrapError> {
    if source.source_epoch() != log.source_epoch() {
        return Err(BootstrapError::SourceCompatibilityMismatch);
    }
    if request.authorization.source_epoch != *source.source_epoch()
        || request.authorization.scope.registry_binding() != Some(source.registry_binding())
    {
        return Err(BootstrapError::AuthorizationSourceMismatch);
    }
    if request.required_tables.is_empty() || request.required_tables.len() > MAX_REQUIRED_TABLES {
        return Err(BootstrapError::InvalidRequiredTableCount);
    }
    let registry_tables: BTreeSet<_> = source
        .registry_binding()
        .tables()
        .iter()
        .map(|(table, _)| table.clone())
        .collect();
    if !request.required_tables.is_subset(&registry_tables) {
        return Err(BootstrapError::UnknownTable);
    }
    let authorized_tables: BTreeSet<_> = request
        .authorization
        .scope
        .permitted_tables()
        .cloned()
        .collect();
    if !authorized_tables.is_subset(&registry_tables)
        || !request.required_tables.is_subset(&authorized_tables)
    {
        return Err(BootstrapError::UnauthorizedTable);
    }
    if request.cached_artifact_hints.len() > MAX_CACHE_HINTS {
        return Err(BootstrapError::TooManyCacheHints);
    }
    let duplicate_encoding = request
        .accepted_encodings
        .iter()
        .enumerate()
        .any(|(index, encoding)| request.accepted_encodings[..index].contains(encoding));
    if request.accepted_encodings.is_empty() || duplicate_encoding {
        return Err(BootstrapError::InvalidEncodings);
    }
    if request.decoder_profile.is_empty()
        || request.decoder_profile.len() > MAX_DECODER_PROFILE_BYTES
        || request.decoder_profile.chars().any(char::is_control)
    {
        return Err(BootstrapError::InvalidDecoderProfile);
    }
    if request.plan_ttl.is_zero() || request.plan_ttl > MAX_PLAN_TTL {
        return Err(BootstrapError::InvalidPlanTtl);
    }
    if !(1_000..=120_000).contains(&request.long_poll_timeout_ms)
        || !(1_024..=MAX_CHANGE_BATCH_BYTES).contains(&request.max_change_batch_bytes)
    {
        return Err(BootstrapError::InvalidChangeLimits);
    }
    Ok(())
}

fn authorized_cache_key(
    source: &ActivatedSource,
    scope: &AuthorizedScope,
    decoder_profile: &str,
) -> ArtifactCacheKey {
    ArtifactCacheKey {
        source_epoch: source.source_epoch().clone(),
        publication_generation: PUBLICATION_GENERATION_V1,
        schema_generation: source.registry_binding().schema_generation(),
        authorization_generation: scope.generation(),
        authorization_policy_hash: scope.policy_hash().to_string(),
        authorization_scope_hash: scope.scope_hash().to_string(),
        shard_profile_hash: SOURCE_WIDE_SHARD_PROFILE_HASH.to_owned(),
        index_profile_hash: NO_INDEX_PROFILE_HASH.to_owned(),
        decoder_profile: decoder_profile.to_owned(),
        type_codec_generation: TYPE_CODEC_GENERATION_V1,
        collation_generation: COLLATION_GENERATION_V1,
    }
}

fn compile_candidates(
    manifests: Vec<ValidatedArtifactManifest>,
    context: CandidateContext<'_>,
) -> Result<Vec<Candidate>, BootstrapError> {
    let registry_tables: BTreeSet<_> = context
        .source
        .registry_binding()
        .tables()
        .iter()
        .map(|(table, _)| table.clone())
        .collect();
    let mut candidates = Vec::new();
    for manifest in manifests {
        let tables: BTreeSet<_> = manifest
            .manifest()
            .tables
            .iter()
            .map(|table| table.table_id.clone())
            .collect();
        if !tables.is_subset(&registry_tables) {
            return Err(BootstrapError::ArtifactOutsideRegistry(
                manifest.manifest().artifact_id.clone(),
            ));
        }
        let required_coverage: BTreeSet<_> =
            tables.intersection(context.missing).cloned().collect();
        if required_coverage.is_empty() {
            continue;
        }
        let position = context
            .source
            .cursor_codec()
            .decode_for_epoch(
                &manifest.manifest().pit.snapshot_cursor,
                context.source.source_epoch(),
            )
            .map_err(|source| BootstrapError::ArtifactCursor {
                artifact: manifest.manifest().artifact_id.clone(),
                source,
            })?;
        if validate_cursor_window(&position, context.minimum_retained, context.head).is_err() {
            continue;
        }
        let Some(representation) = choose_representation(&manifest, context.accepted_encodings)
        else {
            continue;
        };
        let cache_hint = context
            .cache_hints
            .contains(&manifest.manifest().artifact_id);
        candidates.push(Candidate {
            manifest,
            representation,
            snapshot_position: position,
            required_coverage,
            tables,
            cache_hint,
        });
    }
    // Only the newest cut of each table set is planned, whatever an older
    // one would save in bytes or a cache hit: its tail is the shortest.
    let mut newest: BTreeMap<BTreeSet<TableId>, u64> = BTreeMap::new();
    for candidate in &candidates {
        let sequence = newest.entry(candidate.tables.clone()).or_default();
        *sequence = (*sequence).max(candidate.snapshot_position.sequence);
    }
    candidates.retain(|candidate| {
        newest.get(&candidate.tables) == Some(&candidate.snapshot_position.sequence)
    });
    Ok(candidates)
}

fn choose_representation(
    manifest: &ValidatedArtifactManifest,
    accepted: &[RepresentationEncoding],
) -> Option<ArtifactRepresentation> {
    manifest
        .manifest()
        .representations
        .iter()
        .filter_map(|representation| {
            accepted
                .iter()
                .position(|encoding| encoding == &representation.encoding)
                .map(|preference| (representation, preference))
        })
        .min_by_key(|(representation, preference)| {
            (
                *preference,
                representation.encoded_bytes,
                representation.representation_id.as_str(),
            )
        })
        .map(|(representation, _)| representation.clone())
}

struct CoverSelection {
    candidates: Vec<Candidate>,
    states: usize,
    operations: usize,
}

fn select_feasible_cover(
    mut candidates: Vec<Candidate>,
    missing: &BTreeSet<TableId>,
    budget: ClientMemoryBudget,
    deadline: Instant,
) -> Result<CoverSelection, BootstrapError> {
    if missing.is_empty() {
        return Ok(CoverSelection {
            candidates: Vec::new(),
            states: 0,
            operations: 0,
        });
    }
    let base_memory = base_memory(budget)?;
    if base_memory > budget.safe_budget_bytes {
        return Err(BootstrapError::MemoryCapacityExceeded {
            required: base_memory,
            available: budget.safe_budget_bytes,
        });
    }
    let artifact_memory_limit = budget.safe_budget_bytes - base_memory;
    candidates.sort_by(candidate_cost_cmp);
    let mut search = CoverSearch::new(&candidates, artifact_memory_limit, deadline);
    search.search(missing, &BTreeSet::new(), &mut Vec::new(), 0);
    if let Some((_, selected)) = search.best {
        Ok(CoverSelection {
            candidates: selected
                .into_iter()
                .map(|index| candidates[index].clone())
                .collect(),
            states: search.states,
            operations: search.operations,
        })
    } else if search.exhausted {
        Err(BootstrapError::CoverSearchLimit)
    } else if let Some(artifact_memory) = search.minimum_artifact_memory {
        Err(BootstrapError::MemoryCapacityExceeded {
            required: base_memory.saturating_add(artifact_memory),
            available: budget.safe_budget_bytes,
        })
    } else {
        Err(BootstrapError::NoArtifactCover(
            missing.iter().cloned().collect(),
        ))
    }
}

type CoverScore = (u64, u64, u64, u64, String);

struct CoverSearch<'a> {
    candidates: &'a [Candidate],
    by_table: BTreeMap<TableId, Vec<usize>>,
    artifact_memory_limit: u64,
    best: Option<(CoverScore, Vec<usize>)>,
    minimum_artifact_memory: Option<u64>,
    states: usize,
    operations: usize,
    exhausted: bool,
    deadline: Instant,
}

impl<'a> CoverSearch<'a> {
    fn new(candidates: &'a [Candidate], artifact_memory_limit: u64, deadline: Instant) -> Self {
        let mut by_table: BTreeMap<TableId, Vec<usize>> = BTreeMap::new();
        let mut operations: usize = 0;
        let mut exhausted = false;
        for (index, candidate) in candidates.iter().enumerate() {
            for table in &candidate.required_coverage {
                operations = operations.saturating_add(1);
                if operations > MAX_COVER_SEARCH_OPERATIONS || Instant::now() >= deadline {
                    exhausted = true;
                    break;
                }
                by_table.entry(table.clone()).or_default().push(index);
            }
            if exhausted {
                break;
            }
        }
        Self {
            candidates,
            by_table,
            artifact_memory_limit,
            best: None,
            minimum_artifact_memory: None,
            states: 0,
            operations,
            exhausted,
            deadline,
        }
    }

    fn charge_operation(&mut self) -> bool {
        self.operations = self.operations.saturating_add(1);
        if self.operations > MAX_COVER_SEARCH_OPERATIONS
            || (self.operations.is_multiple_of(1_024) && Instant::now() >= self.deadline)
        {
            self.exhausted = true;
            return false;
        }
        true
    }

    fn search(
        &mut self,
        remaining: &BTreeSet<TableId>,
        occupied: &BTreeSet<TableId>,
        selected: &mut Vec<usize>,
        artifact_memory: u64,
    ) {
        if self.exhausted {
            return;
        }
        if remaining.is_empty() {
            self.minimum_artifact_memory = Some(
                self.minimum_artifact_memory
                    .map_or(artifact_memory, |current| current.min(artifact_memory)),
            );
            if artifact_memory <= self.artifact_memory_limit {
                let score = cover_score(self.candidates, selected);
                if self
                    .best
                    .as_ref()
                    .is_none_or(|(current, _)| score < *current)
                {
                    self.best = Some((score, selected.clone()));
                }
            }
            return;
        }
        if self.states >= MAX_COVER_SEARCH_STATES || Instant::now() >= self.deadline {
            self.exhausted = true;
            return;
        }
        self.states += 1;

        let mut branches: Option<(TableId, Vec<usize>)> = None;
        for table in remaining {
            if !self.charge_operation() {
                return;
            }
            let indexes = self.by_table.get(table).cloned().unwrap_or_default();
            let mut choices = Vec::with_capacity(indexes.len());
            for index in indexes {
                if !self.charge_operation() {
                    return;
                }
                if self.candidates[index].tables.is_disjoint(occupied) {
                    choices.push(index);
                }
            }
            let replace = branches.as_ref().is_none_or(|(current_table, current)| {
                (choices.len(), table) < (current.len(), current_table)
            });
            if replace {
                branches = Some((table.clone(), choices));
            }
        }
        let Some((_, choices)) = branches else {
            return;
        };
        for index in choices {
            if !self.charge_operation() {
                return;
            }
            let candidate = &self.candidates[index];
            let next = remaining
                .difference(&candidate.required_coverage)
                .cloned()
                .collect();
            let next_occupied = occupied.union(&candidate.tables).cloned().collect();
            let next_memory = artifact_memory
                .saturating_add(candidate.manifest.manifest().memory.estimated_peak_bytes);
            selected.push(index);
            self.search(&next, &next_occupied, selected, next_memory);
            selected.pop();
            if self.exhausted {
                return;
            }
        }
    }
}

fn cover_score(candidates: &[Candidate], selected: &[usize]) -> CoverScore {
    let mut network_bytes = 0_u64;
    let mut encoded_bytes = 0_u64;
    let mut cache_misses = 0_u64;
    let mut lag = 0_u64;
    let mut ids = Vec::with_capacity(selected.len());
    for index in selected {
        let candidate = &candidates[*index];
        encoded_bytes = encoded_bytes.saturating_add(candidate.representation.encoded_bytes);
        if !candidate.cache_hint {
            network_bytes = network_bytes.saturating_add(candidate.representation.encoded_bytes);
            cache_misses = cache_misses.saturating_add(1);
        }
        lag = lag.saturating_add(u64::MAX - candidate.snapshot_position.sequence);
        ids.push(candidate.manifest.manifest().artifact_id.as_str());
    }
    ids.sort_unstable();
    (
        network_bytes,
        encoded_bytes,
        cache_misses,
        lag,
        ids.join("\0"),
    )
}

fn candidate_cost_cmp(left: &Candidate, right: &Candidate) -> Ordering {
    candidate_cost(left).cmp(&candidate_cost(right))
}

fn candidate_cost(candidate: &Candidate) -> (u64, u64, u64, u64, &str) {
    let covered = u64::try_from(candidate.required_coverage.len()).unwrap_or(u64::MAX);
    let network_bytes = if candidate.cache_hint {
        0
    } else {
        candidate.representation.encoded_bytes
    };
    (
        network_bytes.div_ceil(covered.max(1)),
        network_bytes,
        u64::from(!candidate.cache_hint),
        u64::MAX - candidate.snapshot_position.sequence,
        candidate.manifest.manifest().artifact_id.as_str(),
    )
}

fn compute_memory(
    selected: &[Candidate],
    budget: ClientMemoryBudget,
) -> Result<PlanMemory, BootstrapError> {
    let mut estimated = base_memory(budget)?;
    for candidate in selected {
        estimated = estimated
            .checked_add(candidate.manifest.manifest().memory.estimated_peak_bytes)
            .ok_or(BootstrapError::MemoryOverflow)?;
    }
    if estimated > budget.safe_budget_bytes {
        return Err(BootstrapError::MemoryCapacityExceeded {
            required: estimated,
            available: budget.safe_budget_bytes,
        });
    }
    Ok(PlanMemory {
        minimum_safe_budget_bytes: estimated,
        staging_overlap_bytes: budget.staging_overlap_bytes,
        query_result_reserve_bytes: budget.query_result_reserve_bytes,
        catch_up_reserve_bytes: budget.catch_up_buffer_bytes,
        client_heap_reserve_bytes: budget.client_heap_reserve_bytes,
    })
}

fn base_memory(budget: ClientMemoryBudget) -> Result<u64, BootstrapError> {
    if budget.safe_budget_bytes == 0 || budget.client_heap_reserve_bytes == 0 {
        return Err(BootstrapError::InvalidMemoryBudget);
    }
    if budget.client_heap_reserve_bytes > budget.safe_budget_bytes {
        return Err(BootstrapError::MemoryCapacityExceeded {
            required: budget.client_heap_reserve_bytes,
            available: budget.safe_budget_bytes,
        });
    }
    budget
        .current_active_artifact_bytes
        .checked_add(budget.staging_overlap_bytes)
        .and_then(|value| value.checked_add(budget.catch_up_buffer_bytes))
        .and_then(|value| value.checked_add(budget.query_result_reserve_bytes))
        .and_then(|value| value.checked_add(budget.client_heap_reserve_bytes))
        .ok_or(BootstrapError::MemoryOverflow)
}

fn validate_cursor_window(
    position: &CursorPosition,
    minimum_retained: u64,
    head: u64,
) -> Result<(), CursorWindowError> {
    if position.sequence.saturating_add(1) < minimum_retained {
        return Err(CursorWindowError::BeforeRetention);
    }
    if position.sequence > head {
        return Err(CursorWindowError::BeyondHead);
    }
    Ok(())
}

fn validate_serialized_plan(plan: &BootstrapPlan) -> Result<(), BootstrapError> {
    let value = serde_json::to_value(plan)?;
    let validator = plan_validator()?;
    let violations: Vec<_> = validator
        .iter_errors(&value)
        .map(|error| format!("{}: {error}", error.instance_path()))
        .collect();
    if violations.is_empty() {
        Ok(())
    } else {
        Err(BootstrapError::PlanSchema(violations))
    }
}

fn plan_validator() -> Result<&'static jsonschema::Validator, BootstrapError> {
    let validator = PLAN_SCHEMA.get_or_init(|| {
        let schema: Value = serde_json::from_str(PLAN_SCHEMA_JSON)
            .map_err(|error| format!("checked-in schema is invalid JSON: {error}"))?;
        jsonschema::draft202012::options()
            .should_validate_formats(true)
            .build(&schema)
            .map_err(|error| format!("checked-in schema cannot compile: {error}"))
    });
    validator
        .as_ref()
        .map_err(|message| BootstrapError::SchemaDefinition(message.clone()))
}

fn validate_http_url(url: &Url) -> Result<(), DeliveryError> {
    if !matches!(url.scheme(), "http" | "https")
        || !url.has_host()
        || !url.username().is_empty()
        || url.password().is_some()
        || url.fragment().is_some()
    {
        return Err(DeliveryError::UnsafeUrl);
    }
    Ok(())
}

fn validate_bounded_text(value: &str, maximum: usize) -> Result<(), DeliveryError> {
    if value.is_empty() || value.len() > maximum || value.chars().any(char::is_control) {
        return Err(DeliveryError::InvalidText);
    }
    Ok(())
}

fn format_timestamp(value: OffsetDateTime) -> Result<String, BootstrapError> {
    value
        .format(&Rfc3339)
        .map_err(BootstrapError::TimestampFormat)
}

fn truncate_to_microseconds(value: OffsetDateTime) -> Result<OffsetDateTime, BootstrapError> {
    value
        .replace_nanosecond(value.nanosecond() / 1_000 * 1_000)
        .map_err(|_| BootstrapError::TimestampOverflow)
}

fn system_time_millis() -> Result<i64, BootstrapError> {
    let duration = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_err(|_| BootstrapError::SystemClockBeforeEpoch)?;
    i64::try_from(duration.as_millis()).map_err(|_| BootstrapError::TimestampOverflow)
}

fn saturating_usize(value: usize) -> u64 {
    u64::try_from(value).unwrap_or(u64::MAX)
}

fn timestamp_millis(value: OffsetDateTime) -> Result<i64, BootstrapError> {
    i64::try_from(value.unix_timestamp_nanos() / 1_000_000)
        .map_err(|_| BootstrapError::TimestampOverflow)
}

#[derive(Debug, Error)]
pub enum DeliveryError {
    #[error("delivery URL is invalid: {0}")]
    InvalidUrl(url::ParseError),
    #[error(
        "delivery URL must be absolute HTTP(S), host-bound, credential-free, and fragment-free"
    )]
    UnsafeUrl,
    #[error("delivery URL cannot be used as a hierarchical base")]
    CannotBeBaseUrl,
    #[error("delivery principal field is empty, oversized, or contains control characters")]
    InvalidText,
    #[error("delivery provider is temporarily unavailable: {0}")]
    Unavailable(String),
}

#[derive(Debug, Error)]
pub enum ResidentProofError {
    #[error("resident-state proof secret must contain at least 32 bytes")]
    WeakSecret,
    #[error("resident-state proof is empty or exceeds one MiB")]
    TooLarge,
    #[error("resident-state proof framing is invalid")]
    Framing,
    #[error("resident-state proof base64url encoding is invalid")]
    Encoding,
    #[error("resident-state proof signature verification failed")]
    Signature,
    #[error("resident-state proof version {0} is unsupported")]
    Version(u8),
    #[error("resident-state proof belongs to another compatibility partition")]
    Compatibility,
    #[error("resident-state proof table set is empty, oversized, duplicate, or unsorted")]
    Tables,
    #[error("resident-state proof contains {0} tables, exceeding the limit")]
    TooManyTables(usize),
    #[error("resident-state proof JSON is invalid: {0}")]
    Json(#[from] serde_json::Error),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Error)]
pub enum CursorWindowError {
    #[error("cursor is before the canonical retention floor")]
    BeforeRetention,
    #[error("cursor is beyond the captured activation head")]
    BeyondHead,
}

#[derive(Debug, Error)]
pub enum BootstrapError {
    #[error("bootstrap source, registry, log, and artifact compatibility differ")]
    SourceCompatibilityMismatch,
    #[error("required table count must be between 1 and 256")]
    InvalidRequiredTableCount,
    #[error("bootstrap plan contains {0} subscribed tables, exceeding the 1024-table limit")]
    TooManySubscribedTables(usize),
    #[error("bootstrap request contains a table outside the activated schema registry")]
    UnknownTable,
    #[error("bootstrap authorization was minted for another activated source or schema registry")]
    AuthorizationSourceMismatch,
    #[error("bootstrap request contains a table outside the server-authorized scope")]
    UnauthorizedTable,
    #[error("authorization scope token is empty, oversized, or contains control characters")]
    InvalidAuthorizationToken,
    #[error("authorization expires before the bootstrap plan")]
    AuthorizationExpiresBeforePlan,
    #[error("cache hint count exceeds the planning bound")]
    TooManyCacheHints,
    #[error("accepted representation encodings must be non-empty and unique")]
    InvalidEncodings,
    #[error("plan lifetime must be between one nanosecond and one hour")]
    InvalidPlanTtl,
    #[error("change stream limits are outside the protocol bounds")]
    InvalidChangeLimits,
    #[error("resident cursor is invalid: {0}")]
    ResidentCursor(sync_types::CursorError),
    #[error("resident-state proof is invalid: {0}")]
    ResidentProof(#[from] ResidentProofError),
    #[error("resident-state proof contains a table outside the activated registry")]
    ResidentProofOutsideRegistry,
    #[error("reported resident tables are not a subset of the signed resident-state proof")]
    ResidentClaimOutsideProof,
    #[error("resident cursor is unusable: {0}")]
    ResidentCursorWindow(CursorWindowError),
    #[error("published artifact {artifact} has an invalid snapshot cursor: {source}")]
    ArtifactCursor {
        artifact: ArtifactId,
        source: sync_types::CursorError,
    },
    #[error("published artifact {0} contains a table outside the activated registry")]
    ArtifactOutsideRegistry(ArtifactId),
    #[error("bootstrap request carries an unusable artifact decoder profile")]
    InvalidDecoderProfile,
    #[error("catalog returned {0} compatible artifacts, exceeding the planning bound")]
    TooManyCandidates(usize),
    #[error("no disjoint published artifact cover exists for required tables: {0:?}")]
    NoArtifactCover(Vec<TableId>),
    #[error("artifact cover search exceeded its deterministic work bound")]
    CoverSearchLimit,
    #[error("bootstrap planning exceeded its deadline")]
    PlanningDeadline,
    #[error("bootstrap requires a resident or artifact cursor")]
    NoCatchUpStart,
    #[error("delivery location expires before the bootstrap plan")]
    DeliveryExpiresBeforePlan,
    #[error("memory arithmetic overflowed")]
    MemoryOverflow,
    #[error("client memory budget and client heap reserve must be positive")]
    InvalidMemoryBudget,
    #[error("bootstrap memory estimate {required} exceeds client budget {available}")]
    MemoryCapacityExceeded { required: u64, available: u64 },
    #[error("artifact manifest changed between catalog selection and revalidation")]
    CatalogManifestChanged,
    #[error("system clock is before the Unix epoch")]
    SystemClockBeforeEpoch,
    #[error("bootstrap timestamp overflowed")]
    TimestampOverflow,
    #[error("bootstrap timestamp formatting failed: {0}")]
    TimestampFormat(time::error::Format),
    #[error("bootstrap plan schema is unavailable: {0}")]
    SchemaDefinition(String),
    #[error("generated bootstrap plan violates its checked-in schema: {0:?}")]
    PlanSchema(Vec<String>),
    #[error(transparent)]
    Catalog(#[from] CatalogError),
    #[error(transparent)]
    CanonicalLog(#[from] canonical_log::LogError),
    #[error(transparent)]
    Delivery(#[from] DeliveryError),
    #[error(transparent)]
    Selection(#[from] artifact_contract::SelectionError),
    #[error(transparent)]
    Identifier(#[from] sync_types::IdentifierError),
    #[error(transparent)]
    Json(#[from] serde_json::Error),
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Attacks on the bootstrap capability, case by case.
    mod bootstrap_capability;

    /// The plan schema is `include_str!`d at compile time. A binary built from
    /// another checkout, usually through a `CARGO_TARGET_DIR` shared between
    /// git worktrees, validates against that checkout's schema and fails far
    /// from the cause; this test names it. Cargo runs the test binary from the
    /// package root, so the file read here is this tree's.
    #[test]
    fn compiled_in_plan_schema_matches_the_one_on_disk() {
        let on_disk = std::fs::read_to_string("../../../spec/bootstrap-plan.schema.json")
            .expect("the plan schema is readable from the package root");
        if PLAN_SCHEMA_JSON == on_disk {
            return;
        }
        // Report the first divergence, not both schemas: the useful fact is the
        // diagnosis, and two 20 kB dumps bury it.
        let divergence = PLAN_SCHEMA_JSON
            .lines()
            .zip(on_disk.lines())
            .enumerate()
            .find(|(_, (compiled, disk))| compiled != disk)
            .map_or_else(
                || {
                    format!(
                        "same prefix, different length ({} compiled lines against {} on disk)",
                        PLAN_SCHEMA_JSON.lines().count(),
                        on_disk.lines().count()
                    )
                },
                |(index, (compiled, disk))| {
                    format!(
                        "line {}: compiled in `{}`, on disk `{}`",
                        index + 1,
                        compiled.trim(),
                        disk.trim()
                    )
                },
            );
        panic!(
            "this test binary carries a plan schema that is not the one in this worktree, so \
             every schema assertion below is judging the wrong contract. Rebuild with a \
             CARGO_TARGET_DIR that is not shared with another checkout. First divergence: {divergence}"
        );
    }
    use artifact_contract::ValidatedArtifactManifest;
    use authz::{AuthorizationPolicy, PolicyDefinition, TablePermitDefinition};
    use blob_store::{ByteStream, LocalFilesystemBlobStore, ObjectMetadata};
    use canonical_log::{CanonicalBarrier, FileCanonicalLog, TransactionKey};
    use sha2::{Digest, Sha256};
    use source_control::{
        ColumnInspection, EnrollmentRequirements, EnrollmentSnapshot, PublicationSettings,
        QualifiedTable, SchemaRegistry, SourceIdentity, SourceSettings, TableInspection,
    };
    use sync_types::{ColumnId, CursorCodec, SourceEpoch, SourceId};
    use tokio::io::AsyncWriteExt;

    /// The decoder profile a client declares for the manifests these tests
    /// publish.
    const DECODER_PROFILE: &str = "calque-direct-columnar/v4";

    fn digest(bytes: &[u8]) -> String {
        format!("{:x}", Sha256::digest(bytes))
    }

    /// Provider-neutral same-origin proxy adapter.
    ///
    /// `Url::path_segments_mut` performs standards-compliant path-segment escaping;
    /// object IDs are never concatenated into URLs or interpreted as paths.
    #[derive(Debug, Clone)]
    struct SameOriginProxyResolver {
        base: Url,
    }

    impl SameOriginProxyResolver {
        fn new(base: impl AsRef<str>) -> Result<Self, DeliveryError> {
            let mut base = Url::parse(base.as_ref()).map_err(DeliveryError::InvalidUrl)?;
            validate_http_url(&base)?;
            base.set_query(None);
            base.set_fragment(None);
            Ok(Self { base })
        }
    }

    #[async_trait]
    impl DeliveryResolver for SameOriginProxyResolver {
        async fn resolve(
            &self,
            _principal: &DeliveryPrincipal,
            object: &ObjectId,
            purpose: DeliveryPurpose,
            _plan_expires_at: OffsetDateTime,
        ) -> Result<DeliveryLocation, DeliveryError> {
            let mut url = self.base.clone();
            let mut segments = url
                .path_segments_mut()
                .map_err(|()| DeliveryError::CannotBeBaseUrl)?;
            segments.pop_if_empty();
            match purpose {
                DeliveryPurpose::ArtifactRepresentation => {
                    segments.push("v1").push("artifact-objects");
                }
            }
            segments.push(object.as_str());
            drop(segments);
            DeliveryLocation::new(url.as_str(), None)
        }
    }

    fn hash_id(byte: char) -> String {
        format!("sha256:{}", byte.to_string().repeat(64))
    }

    fn activated_source() -> (ActivatedSource, TableId) {
        let (source, table, _) = activated_source_with_scope();
        (source, table)
    }

    fn activated_source_with_scope() -> (ActivatedSource, TableId, AuthorizedScope) {
        activated_source_with_scope_generation(1)
    }

    fn activated_source_with_scope_generation(
        authorization_generation: u64,
    ) -> (ActivatedSource, TableId, AuthorizedScope) {
        let registry = test_registry();
        let registered = &registry.tables()[0];
        let table_id = registered.table_id.clone();
        let policy = AuthorizationPolicy::compile_for_registry(
            PolicyDefinition {
                generation: authorization_generation,
                tables: vec![TablePermitDefinition {
                    table_id: table_id.clone(),
                    permitted_columns: registered.column_ids.clone(),
                }],
            },
            &registry,
        )
        .unwrap();
        let scope = policy.scope();
        (activated(&registry), table_id, scope)
    }

    fn activated(registry: &SchemaRegistry) -> ActivatedSource {
        ActivatedSource::new(
            registry,
            SourceEpoch::new("epoch-1").unwrap(),
            CursorCodec::new([7_u8; 32]).unwrap(),
            source_control::PgoutputLaunchProfile::V1,
        )
    }

    /// The same source and table under two synchronized column sets at one
    /// authorization generation: `id` alone, then `id` and `title`.
    fn activated_source_with_two_column_sets(
    ) -> (ActivatedSource, TableId, AuthorizedScope, AuthorizedScope) {
        let registry = test_registry();
        let registered = &registry.tables()[0];
        let table_id = registered.table_id.clone();
        let scope = |permitted_columns: &[ColumnId]| {
            AuthorizationPolicy::compile_for_registry(
                PolicyDefinition {
                    generation: 1,
                    tables: vec![TablePermitDefinition {
                        table_id: table_id.clone(),
                        permitted_columns: permitted_columns.to_vec(),
                    }],
                },
                &registry,
            )
            .unwrap()
            .scope()
        };
        let id_only = scope(&registered.column_ids[..1]);
        let every_column = scope(&registered.column_ids);
        (activated(&registry), table_id, id_only, every_column)
    }

    fn test_registry() -> SchemaRegistry {
        let identity = SourceIdentity {
            system_identifier: "7612345678901234567".to_owned(),
            database_name: "app".to_owned(),
            timeline_id: 1,
            previous_timeline_id: 1,
        };
        let table = QualifiedTable {
            schema: "public".to_owned(),
            table: "tasks".to_owned(),
        };
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
                name: "sync_publication".to_owned(),
                exists: true,
                publishes_insert: true,
                publishes_update: true,
                publishes_delete: true,
                publishes_truncate: true,
                publishes_via_partition_root: false,
            },
            tables: vec![TableInspection {
                table: table.clone(),
                relation_oid: 16_384,
                in_publication: true,
                relation_kind: 'r',
                persistence: 'p',
                replica_identity: 'd',
                is_partition: false,
                has_publication_row_filter: false,
                has_publication_column_list: false,
                columns: vec![
                    ColumnInspection {
                        attribute_number: 1,
                        name: "id".to_owned(),
                        type_oid: 2_950,
                        type_modifier: -1,
                        type_schema: "pg_catalog".to_owned(),
                        type_name: "uuid".to_owned(),
                        nullable: false,
                        primary_key_ordinal: Some(1),
                        generated: false,
                        nondeterministic_collation: false,
                    },
                    ColumnInspection {
                        attribute_number: 2,
                        name: "title".to_owned(),
                        type_oid: 25,
                        type_modifier: -1,
                        type_schema: "pg_catalog".to_owned(),
                        type_name: "text".to_owned(),
                        nullable: true,
                        primary_key_ordinal: None,
                        generated: false,
                        nondeterministic_collation: false,
                    },
                ],
            }],
        };
        let requirements = EnrollmentRequirements {
            expected_identity: identity,
            publication_name: "sync_publication".to_owned(),
            tables: BTreeSet::from([table]),
            pgoutput_profile: source_control::PgoutputLaunchProfile::V1,
        };
        SchemaRegistry::initialize(&snapshot, &requirements).unwrap()
    }

    fn manifest(
        source: &ActivatedSource,
        table: &TableId,
        cursor: OpaqueCursor,
        bytes: &[u8],
    ) -> ValidatedArtifactManifest {
        manifest_with_authorization(
            source,
            table,
            cursor,
            bytes,
            0,
            &hash_id('9'),
            "sha256:f9154db512dfe802aa41f358909f833e798c077f0bfa6a2af71c91ab2797cf36",
        )
    }

    /// The physical schema the fixture manifest carries for its one `tasks`
    /// table.
    fn direct_columnar_schema() -> serde_json::Value {
        serde_json::json!({
            "format": "calque-direct-columnar",
            "version": 4,
            "tables": [{
                "name": "tasks",
                "primaryKey": ["id"],
                "columns": [
                    { "name": "id", "type": "int64", "nullable": false },
                    { "name": "title", "type": "utf8", "nullable": true }
                ]
            }]
        })
    }

    /// The wire column ids of that fixture's `tasks` table.
    fn direct_columnar_wire_columns() -> serde_json::Value {
        serde_json::json!([
            { "columnId": 1, "name": "id" },
            { "columnId": 2, "name": "title" }
        ])
    }

    /// The `decoded` block of an artifact over `bytes`.
    fn direct_columnar_decoded(bytes: &[u8]) -> serde_json::Value {
        let digest = digest(bytes);
        serde_json::json!({
            "objectId": format!("sha256:{digest}"),
            "sha256": digest,
            "bytes": bytes.len(),
            "mediaType": artifact_contract::DIRECT_COLUMNAR_MEDIA_TYPE,
            "format": "directColumnar",
            "schemaHash": hash_id('a'),
            "collectionSetHash": hash_id('b'),
            "formatVersion": 4,
            "identity": {
                "manifestVersion": "manifest-18",
                "authorizationScopeVersion": "authorization-generation-9"
            },
            "schema": direct_columnar_schema(),
            "chunks": direct_columnar_chunks(bytes)
        })
    }

    /// The digest of each 4 MiB of a direct-columnar artifact's bytes.
    fn direct_columnar_chunks(bytes: &[u8]) -> serde_json::Value {
        serde_json::json!({
            "bytes": artifact_contract::DIRECT_COLUMNAR_CHUNK_BYTES,
            "sha256": bytes
                .chunks(artifact_contract::DIRECT_COLUMNAR_CHUNK_BYTES as usize)
                .map(|chunk| format!("sha256:{}", digest(chunk)))
                .collect::<Vec<_>>()
        })
    }

    fn manifest_for_scope(
        source: &ActivatedSource,
        table: &TableId,
        cursor: OpaqueCursor,
        bytes: &[u8],
        scope: &AuthorizedScope,
    ) -> ValidatedArtifactManifest {
        manifest_with_authorization(
            source,
            table,
            cursor,
            bytes,
            scope.generation(),
            &scope.policy_hash().to_string(),
            &scope.scope_hash().to_string(),
        )
    }

    /// The same manifest under a different artifact id. The fixture manifests
    /// of two scopes share every field but their authorization identity, and
    /// the catalog admits one artifact id once.
    fn with_artifact_id(
        manifest: &ValidatedArtifactManifest,
        id: &str,
    ) -> ValidatedArtifactManifest {
        let mut value = serde_json::to_value(manifest.manifest()).unwrap();
        value["artifactId"] = serde_json::Value::String(id.to_owned());
        ValidatedArtifactManifest::parse(&serde_json::to_vec(&value).unwrap()).unwrap()
    }

    /// `manifest` as the brotli stage republishes it: another id and one more
    /// representation, one byte smaller than the artifact. `encoding` is the
    /// plain object's or the packed one's, whose body here is two bytes
    /// shorter than the artifact.
    fn with_brotli_representation(
        manifest: &ValidatedArtifactManifest,
        id: &str,
        encoding: RepresentationEncoding,
        bytes: &[u8],
    ) -> ValidatedArtifactManifest {
        let sha = digest(&encoded_fixture(encoding, bytes));
        let packed = encoding == RepresentationEncoding::PackedBrotliLargeWindow;
        let covered = bytes.len() as u64 - if packed { 2 } else { 0 };
        let mut representation = serde_json::json!({
            "representationId": format!("sha256:{sha}"),
            "objectId": format!("sha256:{sha}"),
            "encoding": encoding,
            "encodedSha256": sha,
            "encodedBytes": bytes.len() - 1,
            "brotli": {
                "quality": 11,
                "windowBits": artifact_contract::brotli_covering_window_bits(covered).unwrap(),
            }
        });
        if packed {
            representation["packed"] = serde_json::json!({ "version": 1, "bodyBytes": covered });
        }
        let mut value = serde_json::to_value(manifest.manifest()).unwrap();
        value["artifactId"] = serde_json::json!(id);
        value["representations"]
            .as_array_mut()
            .unwrap()
            .push(representation);
        ValidatedArtifactManifest::parse(&serde_json::to_vec(&value).unwrap()).unwrap()
    }

    #[allow(clippy::too_many_arguments)]
    fn manifest_with_authorization(
        source: &ActivatedSource,
        table: &TableId,
        cursor: OpaqueCursor,
        bytes: &[u8],
        authorization_generation: u64,
        authorization_policy_hash: &str,
        authorization_scope_hash: &str,
    ) -> ValidatedArtifactManifest {
        let zstd_digest = digest(&encoded_fixture(RepresentationEncoding::Zstd, bytes));
        let digest = digest(bytes);
        let value = serde_json::json!({
            "protocolVersion": 1,
            "artifactId": "artifact-1",
            "createdAt": "2026-07-14T12:00:00Z",
            "decoded": direct_columnar_decoded(bytes),
            "pit": {
                "sourceEpoch": source.source_epoch(),
                "snapshotCursor": cursor,
                "copyConsistentLsn": "0/A",
                "cutMessageLsn": "0/B",
                "cutMarkerId": "build-1"
            },
            "compatibility": {
                "publicationGeneration": 1,
                "schemaGeneration": source.registry_binding().schema_generation(),
                "authorizationGeneration": authorization_generation,
                "authorizationPolicyHash": authorization_policy_hash,
                "authorizationScopeHash": authorization_scope_hash,
                "tableSetHash": hash_id('c'),
                "shardProfileHash": SOURCE_WIDE_SHARD_PROFILE_HASH,
                "indexProfileHash": NO_INDEX_PROFILE_HASH,
                "typeCodecGeneration": TYPE_CODEC_GENERATION_V1,
                "collationGeneration": COLLATION_GENERATION_V1
            },
            "tables": [{
                "tableId": table,
                "sourceSchema": "public",
                "sourceTable": "tasks",
                "localSchema": "main",
                "localTable": "tasks",
                "schemaHash": hash_id('f'),
                "columnProfileHash": hash_id('1'),
                "rowCount": 2,
                "primaryKeyCodec": "pk-v1/protobuf",
                "columns": direct_columnar_wire_columns()
            }],
            "representations": [
                {
                    "representationId": format!("sha256:{zstd_digest}"),
                    "objectId": format!("sha256:{zstd_digest}"),
                    "encoding": "zstd",
                    "encodedSha256": zstd_digest,
                    "encodedBytes": bytes.len() + 1,
                    "zstd": { "level": 3, "windowLog": 23 }
                },
                {
                    "representationId": format!("sha256:{digest}"),
                    "objectId": format!("sha256:{digest}"),
                    "encoding": "identity",
                    "encodedSha256": digest,
                    "encodedBytes": bytes.len()
                }
            ],
            "memory": {
                "estimatedPeakBytes": 4096,
                "recommendedGrowthReserveBytes": 1024
            }
        });
        ValidatedArtifactManifest::parse(&serde_json::to_vec(&value).unwrap()).unwrap()
    }

    fn encoded_fixture(encoding: RepresentationEncoding, decoded: &[u8]) -> Vec<u8> {
        let prefix = match encoding {
            RepresentationEncoding::Zstd => b'z',
            RepresentationEncoding::Identity => return decoded.to_vec(),
            // One byte shorter than the artifact, so it is the cheapest object.
            RepresentationEncoding::BrotliLargeWindow
            | RepresentationEncoding::PackedBrotliLargeWindow => return decoded[1..].to_vec(),
        };
        let mut encoded = Vec::with_capacity(decoded.len() + 1);
        encoded.push(prefix);
        encoded.extend_from_slice(decoded);
        encoded
    }

    fn stream(bytes: &[u8]) -> ByteStream {
        let (mut writer, reader) = tokio::io::duplex(bytes.len().max(1));
        let bytes = bytes.to_vec();
        tokio::spawn(async move {
            writer.write_all(&bytes).await.unwrap();
        });
        Box::pin(reader)
    }

    #[test]
    fn representation_selection_respects_profile_preference() {
        let (source, table) = activated_source();
        let cursor = source.cursor_codec().encode(&CursorPosition {
            source_epoch: source.source_epoch().clone(),
            sequence: 0,
        });
        let manifest = manifest(&source, &table, cursor, b"decoded artifact");

        let selected = choose_representation(
            &manifest,
            &[
                RepresentationEncoding::Zstd,
                RepresentationEncoding::Identity,
            ],
        )
        .unwrap();
        assert_eq!(selected.encoding, RepresentationEncoding::Zstd);

        let selected =
            choose_representation(&manifest, &[RepresentationEncoding::Identity]).unwrap();
        assert_eq!(selected.encoding, RepresentationEncoding::Identity);
        assert!(choose_representation(&manifest, &[]).is_none());

        // A manifest holds the plain brotli object or the packed one, and a
        // client is planned the one it holds only if it names that encoding.
        use RepresentationEncoding::{
            BrotliLargeWindow as Plain, Identity, PackedBrotliLargeWindow as Packed, Zstd,
        };
        let with_plain = with_brotli_representation(
            &manifest,
            "artifact-1.brotli-11",
            Plain,
            b"decoded artifact",
        );
        let with_packed = with_brotli_representation(
            &manifest,
            "artifact-1.packed-brotli-11",
            Packed,
            b"decoded artifact",
        );
        for (manifest, accepted, chosen) in [
            (&with_plain, &[Packed, Plain, Zstd, Identity][..], Plain),
            (&with_plain, &[Plain, Zstd, Identity], Plain),
            (&with_plain, &[Packed, Zstd, Identity], Zstd),
            (&with_plain, &[Zstd, Identity], Zstd),
            (&with_packed, &[Packed, Plain, Zstd, Identity], Packed),
            (&with_packed, &[Plain, Zstd, Identity], Zstd),
            (&with_packed, &[Zstd, Identity], Zstd),
            (&with_packed, &[Zstd, Identity, Packed], Zstd),
        ] {
            assert_eq!(
                choose_representation(manifest, accepted).unwrap().encoding,
                chosen,
                "{accepted:?}"
            );
        }
    }

    async fn publish(
        catalog: &mut SqliteArtifactCatalog,
        store: &LocalFilesystemBlobStore,
        manifest: &ValidatedArtifactManifest,
        bytes: &[u8],
    ) {
        let lease = catalog
            .register_build(manifest, "test-builder", 1_000, 10_000)
            .unwrap();
        catalog.mark_uploading(&lease, 1_100).unwrap();
        let decoded_metadata = ObjectMetadata::new(bytes.len() as u64, digest(bytes)).unwrap();
        store
            .put_if_absent(
                &manifest.manifest().decoded.object_id,
                stream(bytes),
                &decoded_metadata,
            )
            .await
            .unwrap();
        for representation in &manifest.manifest().representations {
            let encoded = encoded_fixture(representation.encoding, bytes);
            let metadata = ObjectMetadata::new(encoded.len() as u64, digest(&encoded)).unwrap();
            store
                .put_if_absent(&representation.object_id, stream(&encoded), &metadata)
                .await
                .unwrap();
        }
        catalog.mark_uploaded(&lease, 1_200).unwrap();
        catalog
            .verify_and_publish(&lease, store, 1_300)
            .await
            .unwrap();
    }

    fn request(
        source: &ActivatedSource,
        scope: &AuthorizedScope,
        table: &TableId,
        resident_state_proof: Option<String>,
    ) -> BootstrapRequest {
        request_for_decoder(source, scope, table, resident_state_proof, DECODER_PROFILE)
    }

    fn request_for_decoder(
        source: &ActivatedSource,
        scope: &AuthorizedScope,
        table: &TableId,
        resident_state_proof: Option<String>,
        decoder_profile: &str,
    ) -> BootstrapRequest {
        let resident_tables = resident_state_proof
            .as_ref()
            .map_or_else(BTreeSet::new, |_| BTreeSet::from([table.clone()]));
        BootstrapRequest {
            required_tables: BTreeSet::from([table.clone()]),
            resident_state_proof,
            resident_tables,
            authorization: AuthorizedBootstrapScope::new(
                DeliveryPrincipal::new("user-1").unwrap(),
                source,
                scope.clone(),
                "opaque-scope-token",
                OffsetDateTime::now_utc() + time::Duration::hours(1),
            )
            .unwrap(),
            cached_artifact_hints: BTreeSet::from([ArtifactId::new("artifact-1").unwrap()]),
            accepted_encodings: vec![RepresentationEncoding::Identity],
            decoder_profile: decoder_profile.to_owned(),
            memory: ClientMemoryBudget {
                safe_budget_bytes: 64 * 1024,
                current_active_artifact_bytes: 0,
                staging_overlap_bytes: 1024,
                catch_up_buffer_bytes: 1024,
                query_result_reserve_bytes: 1024,
                client_heap_reserve_bytes: 4 * 1024,
            },
            change_endpoint: DeliveryLocation::new("https://sync.test/v1/changes", None).unwrap(),
            replan_endpoint: Some(
                DeliveryLocation::new("https://sync.test/v1/bootstrap/replan", None).unwrap(),
            ),
            plan_ttl: StdDuration::from_secs(600),
            long_poll_timeout_ms: 30_000,
            max_change_batch_bytes: 4 * 1024 * 1024,
            unchanged: None,
        }
    }

    fn unbounded_memory() -> ClientMemoryBudget {
        ClientMemoryBudget {
            safe_budget_bytes: u64::MAX,
            current_active_artifact_bytes: 0,
            staging_overlap_bytes: 0,
            catch_up_buffer_bytes: 0,
            query_result_reserve_bytes: 0,
            client_heap_reserve_bytes: 1,
        }
    }

    fn manifest_with_identity_and_memory(
        manifest: &ValidatedArtifactManifest,
        artifact_id: &str,
        estimated_peak_bytes: u64,
    ) -> ValidatedArtifactManifest {
        let mut value = serde_json::to_value(manifest.manifest()).unwrap();
        value["artifactId"] = serde_json::json!(artifact_id);
        value["memory"]["estimatedPeakBytes"] = serde_json::json!(estimated_peak_bytes);
        ValidatedArtifactManifest::parse(&serde_json::to_vec(&value).unwrap()).unwrap()
    }

    fn canonical_log(root: &std::path::Path, source: &ActivatedSource) -> FileCanonicalLog {
        let mut log = FileCanonicalLog::open(
            root,
            SourceId::new("source-main").unwrap(),
            source.source_epoch().clone(),
        )
        .unwrap();
        log.append_barrier(CanonicalBarrier {
            key: TransactionKey {
                source_epoch: source.source_epoch().clone(),
                end_lsn: "0/10".to_owned(),
                postgres_xid: 1,
            },
            build_id: "bootstrap-test".to_owned(),
            nonce: "nonce-1".to_owned(),
            message_lsn: "0/F".to_owned(),
            commit_lsn: "0/10".to_owned(),
            commit_time_unix_micros: 1,
        })
        .unwrap();
        log
    }

    /// The columnar branch's `identity`, `schema` and wire columns reach the
    /// plan exactly as the manifest carries them (`spec/protocol-notes.md`),
    /// and in the shape of the checked-in vector
    /// `spec/bootstrap-plan.direct-columnar.example.json`: the same member
    /// names, the schema verbatim, and one wire column entry per source column.
    fn assert_columnar_plan_carries_manifest_facts(runtime: &Value) {
        assert_eq!(
            runtime["identity"],
            serde_json::json!({
                "manifestVersion": "manifest-18",
                "authorizationScopeVersion": "authorization-generation-9"
            })
        );
        assert_eq!(runtime["schema"], direct_columnar_schema());
        assert_eq!(
            runtime["tables"][0]["columns"],
            direct_columnar_wire_columns()
        );
        assert_eq!(runtime["chunks"]["bytes"], 4_194_304);
        assert_eq!(runtime["chunks"]["sha256"].as_array().unwrap().len(), 1);
        let vector: Value = serde_json::from_slice(include_bytes!(
            "../../../../spec/bootstrap-plan.direct-columnar.example.json"
        ))
        .unwrap();
        let keys = |value: &Value| -> Vec<String> {
            let mut keys: Vec<String> = value.as_object().unwrap().keys().cloned().collect();
            keys.sort();
            keys
        };
        let vector_runtime = &vector["artifacts"][0]["runtime"];
        assert_eq!(keys(runtime), keys(vector_runtime));
        assert_eq!(
            keys(&runtime["tables"][0]),
            keys(&vector_runtime["tables"][0])
        );
        assert_eq!(
            keys(&runtime["identity"]),
            keys(&vector_runtime["identity"])
        );
        assert_eq!(keys(&runtime["schema"]), keys(&vector_runtime["schema"]));
        assert_eq!(keys(&runtime["chunks"]), keys(&vector_runtime["chunks"]));
    }

    #[tokio::test]
    async fn a_columnar_artifact_reaches_the_client_through_a_schema_valid_plan() {
        let directory = tempfile::tempdir().unwrap();
        let (source, table, scope) = activated_source_with_scope();
        let mut log = canonical_log(&directory.path().join("log"), &source);
        let artifact_cursor = source.cursor_codec().encode(&CursorPosition {
            source_epoch: source.source_epoch().clone(),
            sequence: 0,
        });
        let columnar = b"CALQART1 stand-in bytes";
        let manifest = manifest_for_scope(&source, &table, artifact_cursor, columnar, &scope);
        let mut catalog =
            SqliteArtifactCatalog::open(directory.path().join("catalog.sqlite3")).unwrap();
        let store = LocalFilesystemBlobStore::open(directory.path().join("objects"), 1024).unwrap();
        publish(&mut catalog, &store, &manifest, columnar).await;
        let plan = BootstrapPlanner::plan(
            &mut catalog,
            &store,
            &SameOriginProxyResolver::new("https://sync.test/base/").unwrap(),
            &source,
            &mut log,
            &ResidentStateProofCodec::new([8_u8; 32]).unwrap(),
            request(&source, &scope, &table, None),
        )
        .await
        .unwrap();

        assert_eq!(plan.artifacts.len(), 1);
        assert_eq!(
            plan.artifacts[0].runtime.format,
            manifest.manifest().decoded.format
        );
        assert!(matches!(
            &plan.artifacts[0].runtime.format,
            DecodedArtifactFormat::DirectColumnar {
                schema_hash,
                collection_set_hash,
                format_version: 4,
                ..
            } if *schema_hash == hash_id('a') && *collection_set_hash == hash_id('b')
        ));
        // The planner already schema-validates, so reaching here proves
        // `artifactRuntime` accepts what the planner emits.
        let runtime = &serde_json::to_value(&plan).unwrap()["artifacts"][0]["runtime"];
        assert_eq!(runtime["format"], "directColumnar");
        assert_columnar_plan_carries_manifest_facts(runtime);

        // The same artifact is invisible to a client that declares another
        // decoder, and a request that declares none is refused rather than
        // silently planned against a default.
        for (profile, expected_miss) in [("calque-direct-columnar/v3", true), ("", false)] {
            let outcome = BootstrapPlanner::plan(
                &mut catalog,
                &store,
                &SameOriginProxyResolver::new("https://sync.test/base/").unwrap(),
                &source,
                &mut log,
                &ResidentStateProofCodec::new([8_u8; 32]).unwrap(),
                request_for_decoder(&source, &scope, &table, None, profile),
            )
            .await;
            if expected_miss {
                assert!(matches!(outcome, Err(BootstrapError::NoArtifactCover(_))));
            } else {
                assert!(matches!(
                    outcome,
                    Err(BootstrapError::InvalidDecoderProfile)
                ));
            }
        }
    }

    /// The encoding each of these clients is planned once the brotli stage
    /// has published `published` beside the level-19 artifact.
    async fn planned_after_the_brotli_stage<const N: usize>(
        published: RepresentationEncoding,
        clients: [&[RepresentationEncoding]; N],
    ) -> [(RepresentationEncoding, u64); N] {
        let directory = tempfile::tempdir().unwrap();
        let (source, table, scope) = activated_source_with_scope();
        let mut log = canonical_log(&directory.path().join("log"), &source);
        let artifact_cursor = source.cursor_codec().encode(&CursorPosition {
            source_epoch: source.source_epoch().clone(),
            sequence: 0,
        });
        let columnar = b"CALQART1 stand-in bytes";
        let manifest = manifest_for_scope(&source, &table, artifact_cursor, columnar, &scope);
        let mut catalog =
            SqliteArtifactCatalog::open(directory.path().join("catalog.sqlite3")).unwrap();
        let store = LocalFilesystemBlobStore::open(directory.path().join("objects"), 1024).unwrap();
        publish(&mut catalog, &store, &manifest, columnar).await;
        let with_brotli =
            with_brotli_representation(&manifest, "artifact-1.brotli-11", published, columnar);
        publish(&mut catalog, &store, &with_brotli, columnar).await;
        let resolver = SameOriginProxyResolver::new("https://sync.test/base/").unwrap();
        let proofs = ResidentStateProofCodec::new([8_u8; 32]).unwrap();

        let mut planned = [(RepresentationEncoding::Identity, 0); N];
        for (planned, accepted) in planned.iter_mut().zip(clients) {
            let mut request = request(&source, &scope, &table, None);
            request.cached_artifact_hints.clear();
            request.accepted_encodings = accepted.to_vec();
            let plan = BootstrapPlanner::plan(
                &mut catalog,
                &store,
                &resolver,
                &source,
                &mut log,
                &proofs,
                request,
            )
            .await
            .unwrap();
            let selected = &plan.artifacts[0].selected_representation;
            *planned = (selected.encoding, selected.encoded_bytes);
        }
        planned
    }

    #[tokio::test]
    async fn a_client_is_planned_the_brotli_object_whose_encoding_it_names() {
        use RepresentationEncoding::{
            BrotliLargeWindow as Plain, Identity, PackedBrotliLargeWindow as Packed, Zstd,
        };
        // The stand-in artifact is 23 bytes: its zstd object is one longer
        // and either brotli object one shorter, so the smallest.
        let (zstd, plain, packed) = ((Zstd, 24), (Plain, 22), (Packed, 22));
        let clients: [&[RepresentationEncoding]; 3] = [
            // The client package's list, one that names the plain object and
            // not the packed one, and a browser's own.
            &[Packed, Plain, Zstd, Identity],
            &[Plain, Zstd, Identity],
            &[Zstd, Identity],
        ];
        assert_eq!(
            planned_after_the_brotli_stage(Plain, clients).await,
            [plain, plain, zstd]
        );
        // A client that does not name the packed encoding cannot restore a
        // packed object, and is planned the zstd one though it is larger.
        assert_eq!(
            planned_after_the_brotli_stage(Packed, clients).await,
            [packed, zstd, zstd]
        );
    }

    #[tokio::test]
    async fn an_artifact_runtime_refuses_fields_the_plan_schema_does_not_define() {
        let directory = tempfile::tempdir().unwrap();
        let (source, table, scope) = activated_source_with_scope();
        let mut log = canonical_log(&directory.path().join("log"), &source);
        let artifact_cursor = source.cursor_codec().encode(&CursorPosition {
            source_epoch: source.source_epoch().clone(),
            sequence: 0,
        });
        let artifact_bytes = b"verified artifact image";
        let manifest = manifest_for_scope(&source, &table, artifact_cursor, artifact_bytes, &scope);
        let mut catalog =
            SqliteArtifactCatalog::open(directory.path().join("catalog.sqlite3")).unwrap();
        let store = LocalFilesystemBlobStore::open(directory.path().join("objects"), 1024).unwrap();
        publish(&mut catalog, &store, &manifest, artifact_bytes).await;
        let plan = BootstrapPlanner::plan(
            &mut catalog,
            &store,
            &SameOriginProxyResolver::new("https://sync.test/base/").unwrap(),
            &source,
            &mut log,
            &ResidentStateProofCodec::new([8_u8; 32]).unwrap(),
            request(&source, &scope, &table, None),
        )
        .await
        .unwrap();

        let value = serde_json::to_value(&plan).unwrap();
        let validator = plan_validator().unwrap();
        assert!(validator.is_valid(&value));

        let mut versioned = value.clone();
        versioned["artifacts"][0]["runtime"]["formatVersion"] = serde_json::json!(2);
        assert!(!validator.is_valid(&versioned));

        let mut unknown = value;
        unknown["artifacts"][0]["runtime"]["pageSize"] = serde_json::json!(8192);
        assert!(!validator.is_valid(&unknown));
    }

    #[tokio::test]
    async fn published_artifact_plan_is_fixed_verified_and_schema_valid() {
        let directory = tempfile::tempdir().unwrap();
        let (source, table, scope) = activated_source_with_scope();
        let mut log = canonical_log(&directory.path().join("log"), &source);
        let artifact_cursor = source.cursor_codec().encode(&CursorPosition {
            source_epoch: source.source_epoch().clone(),
            sequence: 0,
        });
        let artifact_bytes = b"verified artifact image";
        let manifest = manifest_for_scope(
            &source,
            &table,
            artifact_cursor.clone(),
            artifact_bytes,
            &scope,
        );
        let mut catalog =
            SqliteArtifactCatalog::open(directory.path().join("catalog.sqlite3")).unwrap();
        let store = LocalFilesystemBlobStore::open(directory.path().join("objects"), 1024).unwrap();
        publish(&mut catalog, &store, &manifest, artifact_bytes).await;
        let resolver = SameOriginProxyResolver::new("https://sync.test/base/").unwrap();
        let resident_proofs = ResidentStateProofCodec::new([8_u8; 32]).unwrap();

        let mut short_auth = request(&source, &scope, &table, None);
        let verified_grant_expiry =
            truncate_to_microseconds(OffsetDateTime::now_utc() + time::Duration::seconds(5))
                .unwrap();
        short_auth.authorization = AuthorizedBootstrapScope::new(
            DeliveryPrincipal::new("user-1").unwrap(),
            &source,
            scope.clone(),
            "short-token",
            verified_grant_expiry,
        )
        .unwrap();
        let short_plan = BootstrapPlanner::plan(
            &mut catalog,
            &store,
            &resolver,
            &source,
            &mut log,
            &resident_proofs,
            short_auth,
        )
        .await
        .unwrap();
        let short_plan_expiry = OffsetDateTime::parse(&short_plan.expires_at, &Rfc3339).unwrap();
        assert_eq!(short_plan_expiry, verified_grant_expiry);

        let plan = BootstrapPlanner::plan(
            &mut catalog,
            &store,
            &resolver,
            &source,
            &mut log,
            &resident_proofs,
            request(&source, &scope, &table, None),
        )
        .await
        .unwrap();

        assert_eq!(plan.artifacts.len(), 1);
        let issued_at = OffsetDateTime::parse(&plan.issued_at, &Rfc3339).unwrap();
        let expires_at = OffsetDateTime::parse(&plan.expires_at, &Rfc3339).unwrap();
        assert_eq!(issued_at.nanosecond() % 1_000, 0);
        assert_eq!(expires_at.nanosecond() % 1_000, 0);
        assert_eq!(
            plan.source.authorization_policy_hash,
            scope.policy_hash().to_string()
        );
        assert_eq!(
            plan.authorization.policy_hash,
            scope.policy_hash().to_string()
        );
        assert_eq!(
            plan.authorization.scope_hash,
            scope.scope_hash().to_string()
        );
        assert_eq!(plan.diagnostics.catalog_candidate_rows, 1);
        assert_eq!(plan.diagnostics.eligible_candidates, 1);
        assert!(plan.diagnostics.cover_search_states > 0);
        assert!(plan.diagnostics.cover_search_operations > 0);
        assert_eq!(plan.diagnostics.selected_artifacts, 1);
        assert_eq!(plan.diagnostics.selected_cache_hints, 1);
        assert_eq!(plan.artifacts[0].snapshot_cursor, artifact_cursor);
        assert!(plan.artifacts[0]
            .selected_representation
            .download_url
            .ends_with(&format!(
                "/v1/artifact-objects/sha256:{}",
                digest(artifact_bytes)
            )));
        assert_eq!(
            source
                .cursor_codec()
                .decode(&plan.activation_cursor)
                .unwrap(),
            log.head()
        );
        assert_eq!(
            source
                .cursor_codec()
                .decode(&plan.change_stream.from_cursor_exclusive)
                .unwrap()
                .sequence,
            0
        );
        let during_plan_ms = system_time_millis().unwrap();
        assert!(matches!(
            catalog.retire(
                &manifest.manifest().artifact_id,
                during_plan_ms,
                "must remain reachable",
            ),
            Err(CatalogError::ArtifactPinnedByPlan { .. })
        ));
        log.retain_from_at(log.head().sequence + 1, during_plan_ms)
            .unwrap();
        assert_eq!(log.minimum_retained().sequence, 1);
        let json = serde_json::to_value(&plan).unwrap();
        assert!(!json.to_string().contains("user-1"));
        assert!(json.get("diagnostics").is_none());
    }

    /// A plan activates where its catch-up starts only when the caller knows
    /// nothing the scope reads committed from there through the log's head.
    #[tokio::test]
    async fn a_plan_activates_at_its_cut_when_its_scope_is_unchanged_through_the_head() {
        let directory = tempfile::tempdir().unwrap();
        let (source, table, scope) = activated_source_with_scope();
        let mut log = canonical_log(&directory.path().join("log"), &source);
        let cut = source.cursor_codec().encode(&CursorPosition {
            source_epoch: source.source_epoch().clone(),
            sequence: 0,
        });
        let head = log.head();
        assert_eq!(
            head.sequence, 1,
            "a barrier stands between the cut and the head"
        );
        let artifact_bytes = b"artifact cut before the head";
        let manifest = manifest_for_scope(&source, &table, cut.clone(), artifact_bytes, &scope);
        let mut catalog =
            SqliteArtifactCatalog::open(directory.path().join("catalog.sqlite3")).unwrap();
        let store = LocalFilesystemBlobStore::open(directory.path().join("objects"), 1024).unwrap();
        publish(&mut catalog, &store, &manifest, artifact_bytes).await;
        let resolver = SameOriginProxyResolver::new("https://sync.test/base/").unwrap();
        let resident_proofs = ResidentStateProofCodec::new([8_u8; 32]).unwrap();

        let cases = [
            (None, false),
            (Some((0, 1)), true),
            // The scope changed after the cut.
            (Some((1, 1)), false),
            // What the caller knows stops before the head.
            (Some((0, 0)), false),
        ];
        for (unchanged, at_cut) in cases {
            let mut request = request(&source, &scope, &table, None);
            request.unchanged = unchanged.map(|(last_change, through)| UnchangedScope {
                last_change,
                through,
            });
            let plan = BootstrapPlanner::plan(
                &mut catalog,
                &store,
                &resolver,
                &source,
                &mut log,
                &resident_proofs,
                request,
            )
            .await
            .unwrap();
            assert_eq!(plan.change_stream.from_cursor_exclusive, cut);
            assert_eq!(
                plan.change_stream.through_cursor_inclusive,
                plan.activation_cursor
            );
            let activation = source
                .cursor_codec()
                .decode(&plan.activation_cursor)
                .unwrap();
            let expected = if at_cut { 0 } else { head.sequence };
            assert_eq!(activation.sequence, expected, "{unchanged:?}");
            let proof = resident_proofs
                .decode(
                    &plan.next_resident_state_proof,
                    &authorized_cache_key(&source, &scope, DECODER_PROFILE),
                )
                .unwrap();
            assert_eq!(proof.cursor, plan.activation_cursor, "{unchanged:?}");
        }
    }

    /// A newer cut of the same tables is planned over an older, smaller one.
    #[tokio::test]
    async fn the_newest_cut_is_planned_over_a_cached_older_one() {
        let directory = tempfile::tempdir().unwrap();
        let (source, table, scope) = activated_source_with_scope();
        let mut log = canonical_log(&directory.path().join("log"), &source);
        let cursor = |sequence| {
            source.cursor_codec().encode(&CursorPosition {
                source_epoch: source.source_epoch().clone(),
                sequence,
            })
        };
        let old_bytes = b"older cut";
        let new_bytes = b"newer cut, one transaction later";
        let old = manifest_for_scope(&source, &table, cursor(0), old_bytes, &scope);
        let new = with_artifact_id(
            &manifest_for_scope(&source, &table, cursor(1), new_bytes, &scope),
            "artifact-2",
        );
        assert_eq!(old.manifest().artifact_id.as_str(), "artifact-1");
        let mut catalog =
            SqliteArtifactCatalog::open(directory.path().join("catalog.sqlite3")).unwrap();
        let store = LocalFilesystemBlobStore::open(directory.path().join("objects"), 1024).unwrap();
        publish(&mut catalog, &store, &old, old_bytes).await;
        publish(&mut catalog, &store, &new, new_bytes).await;

        let plan = BootstrapPlanner::plan(
            &mut catalog,
            &store,
            &SameOriginProxyResolver::new("https://sync.test/base/").unwrap(),
            &source,
            &mut log,
            &ResidentStateProofCodec::new([8_u8; 32]).unwrap(),
            request(&source, &scope, &table, None),
        )
        .await
        .unwrap();

        assert_eq!(plan.diagnostics.catalog_candidate_rows, 2);
        assert_eq!(plan.diagnostics.eligible_candidates, 1);
        assert_eq!(plan.diagnostics.selected_cache_hints, 0);
        assert_eq!(plan.artifacts.len(), 1);
        assert_eq!(plan.artifacts[0].artifact_id.as_str(), "artifact-2");
        assert_eq!(plan.artifacts[0].snapshot_cursor, cursor(1));
        assert_eq!(
            source
                .cursor_codec()
                .decode(&plan.change_stream.from_cursor_exclusive)
                .unwrap()
                .sequence,
            1
        );
    }

    #[tokio::test]
    async fn warm_resident_plan_needs_no_artifact_and_stale_objects_retire() {
        let directory = tempfile::tempdir().unwrap();
        let (source, table, scope) = activated_source_with_scope();
        let mut log = canonical_log(&directory.path().join("log"), &source);
        let cursor = source.cursor_codec().encode(&log.head());
        let artifact_bytes = b"verified artifact image";
        let manifest = manifest_for_scope(&source, &table, cursor.clone(), artifact_bytes, &scope);
        let mut catalog =
            SqliteArtifactCatalog::open(directory.path().join("catalog.sqlite3")).unwrap();
        let store = LocalFilesystemBlobStore::open(directory.path().join("objects"), 1024).unwrap();
        publish(&mut catalog, &store, &manifest, artifact_bytes).await;
        let resolver = SameOriginProxyResolver::new("https://sync.test/").unwrap();
        let resident_proofs = ResidentStateProofCodec::new([8_u8; 32]).unwrap();

        let cold = BootstrapPlanner::plan(
            &mut catalog,
            &store,
            &resolver,
            &source,
            &mut log,
            &resident_proofs,
            request(&source, &scope, &table, None),
        )
        .await
        .unwrap();
        let warm = BootstrapPlanner::plan(
            &mut catalog,
            &store,
            &resolver,
            &source,
            &mut log,
            &resident_proofs,
            request(
                &source,
                &scope,
                &table,
                Some(cold.next_resident_state_proof.clone()),
            ),
        )
        .await
        .unwrap();
        assert!(warm.artifacts.is_empty());
        assert_eq!(warm.resident_cursor, Some(cursor));

        let mut narrowed = request(
            &source,
            &scope,
            &table,
            Some(cold.next_resident_state_proof.clone()),
        );
        narrowed.resident_tables.clear();
        let narrowed = BootstrapPlanner::plan(
            &mut catalog,
            &store,
            &resolver,
            &source,
            &mut log,
            &resident_proofs,
            narrowed,
        )
        .await
        .unwrap();
        assert_eq!(narrowed.artifacts.len(), 1);
        assert!(narrowed.resident_tables.is_empty());
        assert!(narrowed.resident_cursor.is_none());

        let mut forged_claim = request(
            &source,
            &scope,
            &table,
            Some(cold.next_resident_state_proof.clone()),
        );
        forged_claim
            .resident_tables
            .insert(TableId::new("public.unproven").unwrap());
        assert!(matches!(
            BootstrapPlanner::plan(
                &mut catalog,
                &store,
                &resolver,
                &source,
                &mut log,
                &resident_proofs,
                forged_claim,
            )
            .await,
            Err(BootstrapError::ResidentClaimOutsideProof)
        ));

        let mut tampered = cold.next_resident_state_proof;
        let last = tampered.pop().unwrap();
        tampered.push(if last == 'A' { 'B' } else { 'A' });
        assert!(matches!(
            BootstrapPlanner::plan(
                &mut catalog,
                &store,
                &resolver,
                &source,
                &mut log,
                &resident_proofs,
                request(&source, &scope, &table, Some(tampered)),
            )
            .await,
            Err(BootstrapError::ResidentProof(_))
        ));

        let (_, _, rotated_scope) = activated_source_with_scope_generation(2);
        assert!(matches!(
            BootstrapPlanner::plan(
                &mut catalog,
                &store,
                &resolver,
                &source,
                &mut log,
                &resident_proofs,
                request(&source, &rotated_scope, &table, None),
            )
            .await,
            Err(BootstrapError::NoArtifactCover(_))
        ));

        store
            .delete(&manifest.manifest().decoded.object_id)
            .await
            .unwrap();
        let error = BootstrapPlanner::plan(
            &mut catalog,
            &store,
            &resolver,
            &source,
            &mut log,
            &resident_proofs,
            request(&source, &scope, &table, None),
        )
        .await
        .unwrap_err();
        assert!(matches!(error, BootstrapError::NoArtifactCover(_)));
        assert_eq!(
            catalog.state(&manifest.manifest().artifact_id).unwrap(),
            Some(artifact_catalog::ArtifactState::Retired)
        );
    }

    /// The local store trusts a digest it has read while the object's stat
    /// identity holds. A bootstrap after the object changed on disk, behind
    /// the store's back, still sees the change and retires the artifact.
    #[tokio::test]
    async fn an_object_rewritten_or_removed_on_disk_retires_its_artifact() {
        for removed in [false, true] {
            let directory = tempfile::tempdir().unwrap();
            let (source, table, scope) = activated_source_with_scope();
            let mut log = canonical_log(&directory.path().join("log"), &source);
            let cursor = source.cursor_codec().encode(&log.head());
            let artifact_bytes = b"verified artifact image";
            let manifest = manifest_for_scope(&source, &table, cursor, artifact_bytes, &scope);
            let mut catalog =
                SqliteArtifactCatalog::open(directory.path().join("catalog.sqlite3")).unwrap();
            let store =
                LocalFilesystemBlobStore::open(directory.path().join("objects"), 1024).unwrap();
            publish(&mut catalog, &store, &manifest, artifact_bytes).await;
            let resolver = SameOriginProxyResolver::new("https://sync.test/").unwrap();
            let resident_proofs = ResidentStateProofCodec::new([8_u8; 32]).unwrap();

            // Published an hour ago, so the first bootstrap's reads are
            // trusted until the files change.
            let published = std::time::SystemTime::now() - std::time::Duration::from_secs(3_600);
            let mut objects = Vec::new();
            for shard in std::fs::read_dir(store.root().join("objects")).unwrap() {
                for object in std::fs::read_dir(shard.unwrap().path()).unwrap() {
                    let path = object.unwrap().path();
                    std::fs::File::options()
                        .write(true)
                        .open(&path)
                        .unwrap()
                        .set_modified(published)
                        .unwrap();
                    objects.push(path);
                }
            }
            for _ in 0..2 {
                let plan = BootstrapPlanner::plan(
                    &mut catalog,
                    &store,
                    &resolver,
                    &source,
                    &mut log,
                    &resident_proofs,
                    request(&source, &scope, &table, None),
                )
                .await
                .unwrap();
                assert_eq!(plan.artifacts.len(), 1);
            }

            let image = objects
                .iter()
                .find(|path| std::fs::read(path).unwrap() == artifact_bytes)
                .unwrap();
            if removed {
                std::fs::remove_file(image).unwrap();
            } else {
                let mut file = std::fs::File::options().write(true).open(image).unwrap();
                std::io::Write::write_all(&mut file, b"VERIFIED ARTIFACT IMAGE").unwrap();
            }
            let error = BootstrapPlanner::plan(
                &mut catalog,
                &store,
                &resolver,
                &source,
                &mut log,
                &resident_proofs,
                request(&source, &scope, &table, None),
            )
            .await
            .unwrap_err();
            assert!(
                matches!(error, BootstrapError::NoArtifactCover(_)),
                "removed: {removed}"
            );
            assert_eq!(
                catalog.state(&manifest.manifest().artifact_id).unwrap(),
                Some(artifact_catalog::ArtifactState::Retired),
                "removed: {removed}"
            );
        }
    }

    /// A resident-state proof is bound to the whole artifact cache key, and the
    /// cache key carries the authorization scope hash. A browser that keeps
    /// state from one synchronized column set and then presents a grant for
    /// another cannot have that state accepted.
    #[tokio::test]
    async fn resident_proof_from_another_scope_is_rejected() {
        let directory = tempfile::tempdir().unwrap();
        let (source, table, scope_a, scope_b) = activated_source_with_two_column_sets();
        assert_ne!(
            scope_a.scope_hash().to_string(),
            scope_b.scope_hash().to_string()
        );
        let mut log = canonical_log(&directory.path().join("log"), &source);
        let cursor = source.cursor_codec().encode(&log.head());
        let mut catalog =
            SqliteArtifactCatalog::open(directory.path().join("catalog.sqlite3")).unwrap();
        let store = LocalFilesystemBlobStore::open(directory.path().join("objects"), 1024).unwrap();
        // Both scopes have a published artifact, so the refusal below cannot
        // be explained by B simply having no artifact to plan against.
        let bytes_a = b"scope a image";
        let bytes_b = b"scope b image";
        let manifest_a = manifest_for_scope(&source, &table, cursor.clone(), bytes_a, &scope_a);
        let manifest_b = with_artifact_id(
            &manifest_for_scope(&source, &table, cursor.clone(), bytes_b, &scope_b),
            "artifact-scope-b",
        );
        publish(&mut catalog, &store, &manifest_a, bytes_a).await;
        publish(&mut catalog, &store, &manifest_b, bytes_b).await;
        let resolver = SameOriginProxyResolver::new("https://sync.test/").unwrap();
        let resident_proofs = ResidentStateProofCodec::new([8_u8; 32]).unwrap();

        // Each scope plans against its own bytes.
        let cold_a = BootstrapPlanner::plan(
            &mut catalog,
            &store,
            &resolver,
            &source,
            &mut log,
            &resident_proofs,
            request(&source, &scope_a, &table, None),
        )
        .await
        .unwrap();
        let cold_b = BootstrapPlanner::plan(
            &mut catalog,
            &store,
            &resolver,
            &source,
            &mut log,
            &resident_proofs,
            request(&source, &scope_b, &table, None),
        )
        .await
        .unwrap();
        assert_eq!(cold_a.artifacts.len(), 1);
        assert_eq!(cold_b.artifacts.len(), 1);
        assert_ne!(
            cold_a.artifacts[0].artifact_id, cold_b.artifacts[0].artifact_id,
            "two scopes never share one artifact"
        );
        assert_ne!(
            cold_a.next_resident_state_proof, cold_b.next_resident_state_proof,
            "a proof is bound to the scope it was minted for"
        );

        // A's proof under B's grant is refused: the signature verifies, and
        // the cache key it carries is not this request's.
        assert!(matches!(
            BootstrapPlanner::plan(
                &mut catalog,
                &store,
                &resolver,
                &source,
                &mut log,
                &resident_proofs,
                request(
                    &source,
                    &scope_b,
                    &table,
                    Some(cold_a.next_resident_state_proof.clone()),
                ),
            )
            .await,
            Err(BootstrapError::ResidentProof(
                ResidentProofError::Compatibility
            ))
        ));
        // And the mirror case, so the refusal is not one-directional.
        assert!(matches!(
            BootstrapPlanner::plan(
                &mut catalog,
                &store,
                &resolver,
                &source,
                &mut log,
                &resident_proofs,
                request(
                    &source,
                    &scope_a,
                    &table,
                    Some(cold_b.next_resident_state_proof),
                ),
            )
            .await,
            Err(BootstrapError::ResidentProof(
                ResidentProofError::Compatibility
            ))
        ));

        // Presented with its own grant it is still accepted, so the rejection
        // above is the cross-scope fact and not a broken proof.
        let warm_a = BootstrapPlanner::plan(
            &mut catalog,
            &store,
            &resolver,
            &source,
            &mut log,
            &resident_proofs,
            request(
                &source,
                &scope_a,
                &table,
                Some(cold_a.next_resident_state_proof),
            ),
        )
        .await
        .unwrap();
        assert!(warm_a.artifacts.is_empty());
        assert_eq!(warm_a.resident_cursor, Some(cursor));
    }

    #[tokio::test]
    async fn validly_signed_cursor_from_another_epoch_never_enters_a_plan() {
        let directory = tempfile::tempdir().unwrap();
        let (source, table, scope) = activated_source_with_scope();
        let mut log = canonical_log(&directory.path().join("log"), &source);
        let wrong_epoch_cursor = source.cursor_codec().encode(&CursorPosition {
            source_epoch: SourceEpoch::new("epoch-other").unwrap(),
            sequence: 0,
        });
        let artifact_bytes = b"wrong epoch image";
        let manifest =
            manifest_for_scope(&source, &table, wrong_epoch_cursor, artifact_bytes, &scope);
        let mut catalog =
            SqliteArtifactCatalog::open(directory.path().join("catalog.sqlite3")).unwrap();
        let store = LocalFilesystemBlobStore::open(directory.path().join("objects"), 1024).unwrap();
        publish(&mut catalog, &store, &manifest, artifact_bytes).await;
        let resolver = SameOriginProxyResolver::new("https://sync.test/").unwrap();
        let resident_proofs = ResidentStateProofCodec::new([8_u8; 32]).unwrap();

        assert!(matches!(
            BootstrapPlanner::plan(
                &mut catalog,
                &store,
                &resolver,
                &source,
                &mut log,
                &resident_proofs,
                request(&source, &scope, &table, None),
            )
            .await,
            Err(BootstrapError::ArtifactCursor {
                source: sync_types::CursorError::EpochMismatch { .. },
                ..
            })
        ));
    }

    #[test]
    fn unsafe_delivery_urls_and_memory_overcommit_fail_before_download() {
        assert!(matches!(
            DeliveryLocation::new("https://user:secret@sync.test/object", None),
            Err(DeliveryError::UnsafeUrl)
        ));
        assert!(matches!(
            DeliveryLocation::new("file:///tmp/object", None),
            Err(DeliveryError::UnsafeUrl)
        ));

        let (source, table) = activated_source();
        let cursor = source.cursor_codec().encode(&CursorPosition {
            source_epoch: source.source_epoch().clone(),
            sequence: 0,
        });
        let candidate = Candidate {
            manifest: manifest(&source, &table, cursor, b"artifact"),
            representation: ArtifactRepresentation {
                representation_id: hash_id('9'),
                object_id: ObjectId::new("object-9").unwrap(),
                encoding: RepresentationEncoding::Identity,
                encoded_sha256: "9".repeat(64),
                encoded_bytes: 6,
                zstd: None,
                brotli: None,
                packed: None,
            },
            snapshot_position: CursorPosition {
                source_epoch: source.source_epoch().clone(),
                sequence: 0,
            },
            required_coverage: BTreeSet::from([table.clone()]),
            tables: BTreeSet::from([table]),
            cache_hint: false,
        };
        assert!(matches!(
            compute_memory(
                &[candidate],
                ClientMemoryBudget {
                    safe_budget_bytes: 100,
                    current_active_artifact_bytes: 0,
                    staging_overlap_bytes: 0,
                    catch_up_buffer_bytes: 0,
                    query_result_reserve_bytes: 0,
                    client_heap_reserve_bytes: 1,
                }
            ),
            Err(BootstrapError::MemoryCapacityExceeded { .. })
        ));
    }

    #[test]
    fn client_heap_reserve_is_precharged_and_echoed_exactly() {
        let budget = ClientMemoryBudget {
            safe_budget_bytes: 1_000,
            current_active_artifact_bytes: 100,
            staging_overlap_bytes: 200,
            catch_up_buffer_bytes: 30,
            query_result_reserve_bytes: 40,
            client_heap_reserve_bytes: 50,
        };

        assert_eq!(base_memory(budget).unwrap(), 420);
        assert_eq!(
            compute_memory(&[], budget).unwrap(),
            PlanMemory {
                minimum_safe_budget_bytes: 420,
                staging_overlap_bytes: 200,
                query_result_reserve_bytes: 40,
                catch_up_reserve_bytes: 30,
                client_heap_reserve_bytes: 50,
            }
        );
    }

    #[test]
    fn client_heap_reserve_must_be_positive_and_inside_the_hard_budget() {
        let zero_reserve = ClientMemoryBudget {
            safe_budget_bytes: 1,
            current_active_artifact_bytes: 0,
            staging_overlap_bytes: 0,
            catch_up_buffer_bytes: 0,
            query_result_reserve_bytes: 0,
            client_heap_reserve_bytes: 0,
        };
        assert!(matches!(
            compute_memory(&[], zero_reserve),
            Err(BootstrapError::InvalidMemoryBudget)
        ));

        let over_budget = ClientMemoryBudget {
            client_heap_reserve_bytes: 2,
            ..zero_reserve
        };
        assert!(matches!(
            compute_memory(&[], over_budget),
            Err(BootstrapError::MemoryCapacityExceeded {
                required: 2,
                available: 1
            })
        ));
    }

    #[test]
    fn client_heap_reserve_participates_in_checked_memory_arithmetic() {
        let overflowing = ClientMemoryBudget {
            safe_budget_bytes: u64::MAX,
            current_active_artifact_bytes: u64::MAX,
            staging_overlap_bytes: 0,
            catch_up_buffer_bytes: 0,
            query_result_reserve_bytes: 0,
            client_heap_reserve_bytes: 1,
        };
        assert!(matches!(
            compute_memory(&[], overflowing),
            Err(BootstrapError::MemoryOverflow)
        ));
    }

    #[test]
    fn checked_in_plan_example_matches_schema_and_rust_contract_both_ways() {
        let bytes = include_bytes!("../../../../spec/bootstrap-plan.direct-columnar.example.json");
        let plan: BootstrapPlan = serde_json::from_slice(bytes).unwrap();
        validate_serialized_plan(&plan).unwrap();
        let encoded = serde_json::to_vec(&plan).unwrap();
        let roundtrip: BootstrapPlan = serde_json::from_slice(&encoded).unwrap();
        assert_eq!(roundtrip, plan);
        // The Rust contract carries every field of the vector, additive fields
        // included, so the TypeScript parser and this struct read one
        // document.
        let original: Value = serde_json::from_slice(bytes).unwrap();
        let reencoded: Value = serde_json::from_slice(&encoded).unwrap();
        assert_eq!(
            reencoded["artifacts"][0]["runtime"],
            original["artifacts"][0]["runtime"]
        );
    }

    /// The plan `@calque/client` binds, with its additive `identity`, `schema`
    /// and per-table `columns` fields, checked against the schema as it stands,
    /// with the additive fields removed, and with a format the protocol does
    /// not define.
    #[test]
    fn checked_in_direct_columnar_plan_example_matches_schema() {
        let bytes = include_bytes!("../../../../spec/bootstrap-plan.direct-columnar.example.json");
        let mut value: Value = serde_json::from_slice(bytes).unwrap();
        let validator = plan_validator().unwrap();
        let violations: Vec<_> = validator
            .iter_errors(&value)
            .map(|error| format!("{}: {error}", error.instance_path()))
            .collect();
        assert!(violations.is_empty(), "{violations:?}");

        let runtime = value["artifacts"][0]["runtime"].as_object_mut().unwrap();
        let identity = runtime.remove("identity").unwrap();
        let schema = runtime.remove("schema").unwrap();
        let columns = runtime["tables"][0]
            .as_object_mut()
            .unwrap()
            .remove("columns")
            .unwrap();
        assert!(
            validator.is_valid(&value),
            "the additive fields must not be required by the schema"
        );

        let mut restored = value.clone();
        let runtime = &mut restored["artifacts"][0]["runtime"];
        runtime["identity"] = identity.clone();
        runtime["schema"] = schema;
        runtime["tables"][0]["columns"] = columns;
        runtime["format"] = Value::from("sqlite");
        assert!(
            !validator.is_valid(&restored),
            "the plan schema defines one artifact format"
        );

        let mut partial = identity.clone();
        partial
            .as_object_mut()
            .unwrap()
            .remove("authorizationScopeVersion");
        value["artifacts"][0]["runtime"]["identity"] = partial;
        assert!(
            !validator.is_valid(&value),
            "an identity must carry both header strings"
        );
    }

    #[test]
    fn exact_cover_search_backtracks_past_a_cheaper_dead_end() {
        let (source, manifest_table) = activated_source();
        let cursor = source.cursor_codec().encode(&CursorPosition {
            source_epoch: source.source_epoch().clone(),
            sequence: 0,
        });
        let base = Candidate {
            manifest: manifest(&source, &manifest_table, cursor, b"artifact"),
            representation: ArtifactRepresentation {
                representation_id: hash_id('8'),
                object_id: ObjectId::new("object-8").unwrap(),
                encoding: RepresentationEncoding::Identity,
                encoded_sha256: "8".repeat(64),
                encoded_bytes: 1,
                zstd: None,
                brotli: None,
                packed: None,
            },
            snapshot_position: CursorPosition {
                source_epoch: source.source_epoch().clone(),
                sequence: 0,
            },
            required_coverage: BTreeSet::new(),
            tables: BTreeSet::new(),
            cache_hint: false,
        };
        let a = TableId::new("table-a").unwrap();
        let b = TableId::new("table-b").unwrap();
        let c = TableId::new("table-c").unwrap();
        let extra = TableId::new("table-extra").unwrap();
        let mut cheap_dead_end = base.clone();
        cheap_dead_end.required_coverage = BTreeSet::from([a.clone(), b.clone()]);
        cheap_dead_end.tables = BTreeSet::from([a.clone(), b.clone()]);
        let mut single_a = base.clone();
        single_a.required_coverage = BTreeSet::from([a.clone()]);
        single_a.tables = BTreeSet::from([a.clone()]);
        single_a.representation.encoded_bytes = 2;
        let mut pair_bc = base;
        pair_bc.required_coverage = BTreeSet::from([b.clone(), c.clone()]);
        pair_bc.tables = BTreeSet::from([b.clone(), c.clone(), extra.clone()]);
        pair_bc.representation.encoded_bytes = 3;

        let selected = select_feasible_cover(
            vec![cheap_dead_end, single_a, pair_bc],
            &BTreeSet::from([a.clone(), b.clone(), c.clone()]),
            unbounded_memory(),
            Instant::now() + StdDuration::from_secs(5),
        )
        .unwrap();
        assert!(selected.states <= MAX_COVER_SEARCH_STATES);
        assert!(selected.operations <= MAX_COVER_SEARCH_OPERATIONS);
        assert_eq!(selected.candidates.len(), 2);
        let covered: BTreeSet<_> = selected
            .candidates
            .iter()
            .flat_map(|candidate| candidate.tables.iter().cloned())
            .collect();
        assert_eq!(covered, BTreeSet::from([a, b, c, extra]));
    }

    #[test]
    fn exact_cover_handles_the_maximum_query_table_count_with_bounded_work() {
        let (source, manifest_table) = activated_source();
        let cursor = source.cursor_codec().encode(&CursorPosition {
            source_epoch: source.source_epoch().clone(),
            sequence: 0,
        });
        let base = Candidate {
            manifest: manifest(&source, &manifest_table, cursor, b"artifact"),
            representation: ArtifactRepresentation {
                representation_id: hash_id('7'),
                object_id: ObjectId::new("object-7").unwrap(),
                encoding: RepresentationEncoding::Identity,
                encoded_sha256: "7".repeat(64),
                encoded_bytes: 1,
                zstd: None,
                brotli: None,
                packed: None,
            },
            snapshot_position: CursorPosition {
                source_epoch: source.source_epoch().clone(),
                sequence: 0,
            },
            required_coverage: BTreeSet::new(),
            tables: BTreeSet::new(),
            cache_hint: false,
        };
        let mut required = BTreeSet::new();
        let mut candidates = Vec::new();
        for index in 0..MAX_REQUIRED_TABLES {
            let table = TableId::new(format!("table-{index:03}")).unwrap();
            required.insert(table.clone());
            let mut candidate = base.clone();
            candidate.required_coverage = BTreeSet::from([table.clone()]);
            candidate.tables = BTreeSet::from([table]);
            candidates.push(candidate);
        }
        let selected = select_feasible_cover(
            candidates,
            &required,
            unbounded_memory(),
            Instant::now() + StdDuration::from_secs(5),
        )
        .unwrap();
        assert_eq!(selected.candidates.len(), MAX_REQUIRED_TABLES);
        assert!(selected.states <= MAX_COVER_SEARCH_STATES);
        assert!(selected.operations <= MAX_COVER_SEARCH_OPERATIONS);
    }

    /// The cover search is bounded by an operation/state budget and a
    /// wall-clock deadline, and must refuse with `CoverSearchLimit` when either
    /// is spent. Nothing caps how many overlapping published artifacts a
    /// workspace may have, so unbounded backtracking is a request-driven CPU
    /// hang, and reporting exhaustion as "no cover exists" would misinform the
    /// caller.
    #[test]
    fn an_exhausted_cover_search_refuses_instead_of_reporting_no_cover_exists() {
        let (source, manifest_table) = activated_source();
        let cursor = source.cursor_codec().encode(&CursorPosition {
            source_epoch: source.source_epoch().clone(),
            sequence: 0,
        });
        let base = Candidate {
            manifest: manifest(&source, &manifest_table, cursor, b"artifact"),
            representation: ArtifactRepresentation {
                representation_id: hash_id('7'),
                object_id: ObjectId::new("object-7").unwrap(),
                encoding: RepresentationEncoding::Identity,
                encoded_sha256: "7".repeat(64),
                encoded_bytes: 1,
                zstd: None,
                brotli: None,
                packed: None,
            },
            snapshot_position: CursorPosition {
                source_epoch: source.source_epoch().clone(),
                sequence: 0,
            },
            required_coverage: BTreeSet::new(),
            tables: BTreeSet::new(),
            cache_hint: false,
        };
        let table = |index: usize| TableId::new(format!("table-{index:03}")).unwrap();

        // A pathological but well-formed instance: an odd number of tables, and
        // one artifact for every unordered pair. Covering them exactly is a
        // perfect matching, which cannot exist on an odd vertex count, so the
        // planner must enumerate the whole matching tree before concluding
        // anything. The budget is a deterministic counter, so this trips the
        // operation/state limit identically on every machine rather than racing
        // the clock -- the deadline below is far larger than the work needs.
        const TABLES: usize = 17;
        let mut required = BTreeSet::new();
        for index in 0..TABLES {
            required.insert(table(index));
        }
        let mut candidates = Vec::new();
        for left in 0..TABLES {
            for right in (left + 1)..TABLES {
                let mut candidate = base.clone();
                let pair = BTreeSet::from([table(left), table(right)]);
                candidate.required_coverage = pair.clone();
                candidate.tables = pair;
                candidates.push(candidate);
            }
        }
        let refused = select_feasible_cover(
            candidates,
            &required,
            unbounded_memory(),
            Instant::now() + StdDuration::from_secs(120),
        );
        match refused {
            Err(BootstrapError::CoverSearchLimit) => {}
            Err(BootstrapError::NoArtifactCover(_)) => {
                panic!("an exhausted search must not be reported as a definitive absence of cover")
            }
            Err(other) => panic!("an exhausted search must refuse with CoverSearchLimit: {other}"),
            Ok(_) => panic!("an exhausted search must not return a cover"),
        }

        // The wall-clock detector is a separate layer: a deadline captured
        // before the call has already passed once the search reads the clock.
        let expired = Instant::now();
        let mut required = BTreeSet::new();
        let mut candidates = Vec::new();
        for index in 0..MAX_REQUIRED_TABLES {
            let named = table(index);
            required.insert(named.clone());
            let mut candidate = base.clone();
            candidate.required_coverage = BTreeSet::from([named.clone()]);
            candidate.tables = BTreeSet::from([named]);
            candidates.push(candidate);
        }
        // Control: the very same instance is solvable with budget to spare, so
        // the refusal is caused by the spent deadline and not by the input.
        let solved = select_feasible_cover(
            candidates.clone(),
            &required,
            unbounded_memory(),
            Instant::now() + StdDuration::from_secs(120),
        )
        .unwrap();
        assert_eq!(solved.candidates.len(), MAX_REQUIRED_TABLES);

        let refused = select_feasible_cover(candidates, &required, unbounded_memory(), expired);
        match refused {
            Err(BootstrapError::CoverSearchLimit) => {}
            Err(other) => panic!("a spent deadline must refuse with CoverSearchLimit: {other}"),
            Ok(_) => panic!("a spent deadline must refuse, not return a cover"),
        }
    }

    #[test]
    fn cover_search_skips_a_cached_over_budget_artifact_for_a_feasible_one() {
        let (source, table) = activated_source();
        let cursor = source.cursor_codec().encode(&CursorPosition {
            source_epoch: source.source_epoch().clone(),
            sequence: 0,
        });
        let original = manifest(&source, &table, cursor, b"artifact");
        let mut cached_large = Candidate {
            manifest: manifest_with_identity_and_memory(&original, "artifact-large", 10_000),
            representation: original.manifest().representations[0].clone(),
            snapshot_position: CursorPosition {
                source_epoch: source.source_epoch().clone(),
                sequence: 0,
            },
            required_coverage: BTreeSet::from([table.clone()]),
            tables: BTreeSet::from([table.clone()]),
            cache_hint: true,
        };
        cached_large.representation.encoded_bytes = 1;
        let feasible = Candidate {
            manifest: manifest_with_identity_and_memory(&original, "artifact-feasible", 100),
            representation: original.manifest().representations[0].clone(),
            snapshot_position: CursorPosition {
                source_epoch: source.source_epoch().clone(),
                sequence: 0,
            },
            required_coverage: BTreeSet::from([table.clone()]),
            tables: BTreeSet::from([table.clone()]),
            cache_hint: false,
        };
        let selected = select_feasible_cover(
            vec![cached_large, feasible],
            &BTreeSet::from([table]),
            ClientMemoryBudget {
                safe_budget_bytes: 200,
                current_active_artifact_bytes: 0,
                staging_overlap_bytes: 0,
                catch_up_buffer_bytes: 0,
                query_result_reserve_bytes: 0,
                client_heap_reserve_bytes: 1,
            },
            Instant::now() + StdDuration::from_secs(5),
        )
        .unwrap();
        assert_eq!(
            selected.candidates[0]
                .manifest
                .manifest()
                .artifact_id
                .as_str(),
            "artifact-feasible"
        );
    }
}
