//! Revoking an authorization generation on a running deployment.
//!
//! The application backend holds the signing key, so it is the only party
//! that can retire a generation. It does so in two signed steps: a fence
//! order, which Calque makes durable before anything in memory moves, and
//! then one signature per barrier draft the order produced, one for each
//! scope the retired generation's plans hold. This module is the durable
//! fence, the HTTP surface for both steps, and the background publication of
//! the new generation's artifact. The decisions themselves --
//! what is fenced, at which cursor, and what is refused afterwards -- are
//! `LocalBackend`'s.

use std::{
    collections::BTreeSet,
    sync::{Arc, Mutex as StdMutex},
    time::Duration,
};

use artifact_catalog::SqliteArtifactCatalog;
use artifact_contract::ValidatedArtifactManifest;
use axum::{
    body::Bytes,
    extract::{DefaultBodyLimit, State},
    http::{header, HeaderMap, StatusCode},
    response::{IntoResponse, Response},
    routing::post,
    Json, Router,
};
use canonical_log::CONTROL_SCHEMA_BOOTSTRAP_LOCK_KEY;
use serde::{Deserialize, Serialize};
use snapshot_pipeline::PublicationAdmission;
use sync_protocol::change::{Cursor, RevocationBarrier};
use sync_types::{SourceEpoch, SourceId};
use thiserror::Error;
use tokio_util::sync::CancellationToken;
use uuid::Uuid;

use crate::{
    guard::PostgresConnectionFamily, source::connect_owned, ArtifactBuilder, DatabaseUrl,
    LocalBackend, LocalSnapshotError, LocalSourceError, PreparedLocalSource,
};

/// A fence order and a barrier are a few hundred bytes each; a batch of
/// signatures, one for each scope a fence retired, stays well under this.
const MAX_REVOCATION_BODY_BYTES: usize = 256 * 1024;

/// The durable authorization-generation fence of one source: the smallest
/// generation `calque serve` may ever serve again, what each fence retired,
/// and the scopes issued under the generation in force.
///
/// These are PostgreSQL rows rather than files in the state directory, so
/// losing the state directory cannot reopen a fenced generation, and they are
/// keyed by source id for the same reason.
#[derive(Clone)]
pub struct AuthorizationFenceStore {
    database_url: DatabaseUrl,
    source_id: SourceId,
    /// The scopes this process has recorded as issued, so each costs one
    /// write per process.
    issued: Arc<StdMutex<IssuedScopes>>,
}

/// Generation and scope hash.
type IssuedScopes = BTreeSet<(u64, [u8; 32])>;

/// A fenced generation as PostgreSQL holds it: the head its plans are capped
/// at, and one barrier for each scope issued under it, carrying the
/// application's signature once it has one.
#[derive(Debug, Clone, PartialEq)]
pub struct StoredRetirement {
    pub revoked_generation: u64,
    pub effective_generation: u64,
    pub ceiling: u64,
    pub barriers: Vec<RevocationBarrier>,
}

/// What one fence writes beside the fence row.
pub(crate) struct FenceWrite<'a> {
    pub(crate) source_epoch: &'a SourceEpoch,
    pub(crate) revoked_generation: u64,
    pub(crate) effective_generation: u64,
    pub(crate) ceiling: u64,
    pub(crate) barrier_cursor: &'a Cursor,
    pub(crate) issued_at_unix_micros: u64,
}

/// The unsigned barrier of one retired scope, which the application signs.
pub(crate) fn draft_barrier(
    revoked_generation: u64,
    effective_generation: u64,
    scope_hash: [u8; 32],
    barrier_cursor: &Cursor,
    issued_at_unix_micros: u64,
) -> RevocationBarrier {
    RevocationBarrier {
        barrier_cursor: Some(barrier_cursor.clone()),
        revoked_authorization_generation: revoked_generation,
        effective_authorization_generation: effective_generation,
        revoked_authorization_scope_hash: scope_hash.to_vec(),
        barrier_id: format!("barrier-{}", Uuid::new_v4().simple()),
        issued_at_unix_micros,
        signing_key_id: String::new(),
        signature: Vec::new(),
    }
}

#[derive(Debug, Error)]
pub enum AuthorizationFenceError {
    #[error("authorization fence storage failed: {0}")]
    Postgres(#[from] postgres::Error),
    #[error(transparent)]
    Source(#[from] LocalSourceError),
    #[error("authorization generation {0} does not fit the fence's storage")]
    GenerationOutOfRange(u64),
    #[error("stored authorization fence is corrupt")]
    Corrupt,
}

impl AuthorizationFenceStore {
    pub fn new(database_url: DatabaseUrl, source_id: SourceId) -> Self {
        Self {
            database_url,
            source_id,
            issued: Arc::default(),
        }
    }

    /// Installs the fence table if it is missing, advances the fence to at
    /// least `configured`, and returns the generation now in force: the larger
    /// of the two. Startup compiles its policy at that value, so neither an
    /// older manifest nor a lost state directory reopens a fenced generation.
    pub fn establish(&self, configured: u64) -> Result<u64, AuthorizationFenceError> {
        let mut client = self.connect()?;
        let mut transaction = client.transaction()?;
        // The control schema's one bootstrap lock, so this DDL never races the
        // canonical log's or the fence store's own install.
        transaction.execute(
            "SELECT pg_advisory_xact_lock($1)",
            &[&CONTROL_SCHEMA_BOOTSTRAP_LOCK_KEY],
        )?;
        // PostgreSQL reports each object `IF NOT EXISTS` skips as a NOTICE,
        // which the driver logs at INFO on every start.
        transaction.batch_execute(
            "SET LOCAL client_min_messages = warning;
             CREATE SCHEMA IF NOT EXISTS calque_control;
             CREATE TABLE IF NOT EXISTS calque_control.authorization_fence (
                 source_id text PRIMARY KEY,
                 generation bigint NOT NULL CHECK (generation >= 0),
                 fenced_at timestamptz NOT NULL DEFAULT now()
             );
             CREATE TABLE IF NOT EXISTS calque_control.authorization_scope_issued (
                 source_id text NOT NULL,
                 generation bigint NOT NULL CHECK (generation >= 0),
                 scope_hash bytea NOT NULL CHECK (octet_length(scope_hash) = 32),
                 PRIMARY KEY (source_id, generation, scope_hash)
             );
             CREATE TABLE IF NOT EXISTS calque_control.authorization_retirement (
                 source_id text NOT NULL,
                 revoked_generation bigint NOT NULL CHECK (revoked_generation >= 0),
                 effective_generation bigint NOT NULL
                     CHECK (effective_generation > revoked_generation),
                 source_epoch text NOT NULL,
                 ceiling bigint NOT NULL CHECK (ceiling >= 0),
                 fenced_at timestamptz NOT NULL DEFAULT now(),
                 PRIMARY KEY (source_id, revoked_generation)
             );
             CREATE TABLE IF NOT EXISTS calque_control.revocation_barrier (
                 source_id text NOT NULL,
                 revoked_generation bigint NOT NULL,
                 scope_hash bytea NOT NULL CHECK (octet_length(scope_hash) = 32),
                 barrier_id text NOT NULL,
                 barrier_cursor bytea NOT NULL,
                 issued_at_unix_micros bigint NOT NULL CHECK (issued_at_unix_micros >= 0),
                 signing_key_id text,
                 signature bytea,
                 PRIMARY KEY (source_id, revoked_generation, scope_hash),
                 UNIQUE (source_id, barrier_id),
                 FOREIGN KEY (source_id, revoked_generation)
                     REFERENCES calque_control.authorization_retirement,
                 CHECK ((signing_key_id IS NULL) = (signature IS NULL))
             );",
        )?;
        let generation = advance_in(&mut transaction, &self.source_id, configured)?;
        transaction.commit()?;
        Ok(generation)
    }

    /// Advances the fence to `write.effective_generation` and records what it
    /// retired, in one transaction: the revoked generation's ceiling, and one
    /// unsigned barrier for each scope issued under it. A generation is
    /// retired once, so a repeated call returns the retirement already
    /// stored. Returns the generation durably in force and the retirement.
    pub(crate) fn fence(
        &self,
        write: &FenceWrite<'_>,
    ) -> Result<(u64, StoredRetirement), AuthorizationFenceError> {
        let mut client = self.connect()?;
        let mut transaction = client.transaction()?;
        let fenced = advance_in(
            &mut transaction,
            &self.source_id,
            write.effective_generation,
        )?;
        let source_id = self.source_id.as_str();
        let revoked = stored(write.revoked_generation)?;
        let inserted = transaction.execute(
            "INSERT INTO calque_control.authorization_retirement
                 (source_id, revoked_generation, effective_generation, source_epoch, ceiling)
             VALUES ($1, $2, $3, $4, $5)
             ON CONFLICT (source_id, revoked_generation) DO NOTHING",
            &[
                &source_id,
                &revoked,
                &stored(fenced)?,
                &write.source_epoch.as_str(),
                &stored(write.ceiling)?,
            ],
        )?;
        if inserted == 1 {
            let mut scopes = BTreeSet::new();
            for row in transaction.query(
                "SELECT scope_hash FROM calque_control.authorization_scope_issued
                  WHERE source_id = $1 AND generation = $2",
                &[&source_id, &revoked],
            )? {
                scopes.insert(scope_hash(row.get(0))?);
            }
            for scope in scopes {
                let draft = draft_barrier(
                    write.revoked_generation,
                    fenced,
                    scope,
                    write.barrier_cursor,
                    write.issued_at_unix_micros,
                );
                transaction.execute(
                    "INSERT INTO calque_control.revocation_barrier
                         (source_id, revoked_generation, scope_hash, barrier_id,
                          barrier_cursor, issued_at_unix_micros)
                     VALUES ($1, $2, $3, $4, $5, $6)",
                    &[
                        &source_id,
                        &revoked,
                        &draft.revoked_authorization_scope_hash,
                        &draft.barrier_id,
                        &write.barrier_cursor.token,
                        &stored(write.issued_at_unix_micros)?,
                    ],
                )?;
            }
            transaction.execute(
                "DELETE FROM calque_control.authorization_scope_issued
                  WHERE source_id = $1 AND generation <= $2",
                &[&source_id, &revoked],
            )?;
        }
        let retirement = retirement_in(&mut transaction, source_id, revoked)?
            .ok_or(AuthorizationFenceError::Corrupt)?;
        transaction.commit()?;
        Ok((fenced, retirement))
    }

    /// The retirements of `source_epoch` fenced within `within`, which is as
    /// long as any plan they retired can still be presented.
    pub fn retirements(
        &self,
        source_epoch: &SourceEpoch,
        within: Duration,
    ) -> Result<Vec<StoredRetirement>, AuthorizationFenceError> {
        let mut client = self.connect()?;
        let source_id = self.source_id.as_str();
        let revoked: Vec<i64> = client
            .query(
                "SELECT revoked_generation FROM calque_control.authorization_retirement
                  WHERE source_id = $1 AND source_epoch = $2
                    AND fenced_at > now() - make_interval(secs => $3)
                  ORDER BY revoked_generation",
                &[&source_id, &source_epoch.as_str(), &within.as_secs_f64()],
            )?
            .iter()
            .map(|row| row.get(0))
            .collect();
        revoked
            .into_iter()
            .map(|revoked| {
                retirement_in(&mut client, source_id, revoked)?
                    .ok_or(AuthorizationFenceError::Corrupt)
            })
            .collect()
    }

    /// Records the application's verified signature on each barrier.
    pub(crate) fn sign(
        &self,
        barriers: &[RevocationBarrier],
    ) -> Result<(), AuthorizationFenceError> {
        let mut client = self.connect()?;
        let mut transaction = client.transaction()?;
        for barrier in barriers {
            let updated = transaction.execute(
                "UPDATE calque_control.revocation_barrier
                    SET signing_key_id = $4, signature = $5
                  WHERE source_id = $1 AND revoked_generation = $2 AND barrier_id = $3",
                &[
                    &self.source_id.as_str(),
                    &stored(barrier.revoked_authorization_generation)?,
                    &barrier.barrier_id,
                    &barrier.signing_key_id,
                    &barrier.signature,
                ],
            )?;
            if updated != 1 {
                return Err(AuthorizationFenceError::Corrupt);
            }
        }
        transaction.commit()?;
        Ok(())
    }

    /// Records that plans of `scope_hash` are issued under `generation`, so
    /// that a fence of it drafts their barrier whichever process fences.
    pub(crate) fn issue(
        &self,
        generation: u64,
        scope_hash: [u8; 32],
    ) -> Result<(), AuthorizationFenceError> {
        if lock(&self.issued).contains(&(generation, scope_hash)) {
            return Ok(());
        }
        self.connect()?.execute(
            "INSERT INTO calque_control.authorization_scope_issued
                 (source_id, generation, scope_hash)
             VALUES ($1, $2, $3)
             ON CONFLICT DO NOTHING",
            &[
                &self.source_id.as_str(),
                &stored(generation)?,
                &scope_hash.as_slice(),
            ],
        )?;
        lock(&self.issued).insert((generation, scope_hash));
        Ok(())
    }

    pub(crate) fn is_issued(&self, generation: u64, scope_hash: &[u8; 32]) -> bool {
        lock(&self.issued).contains(&(generation, *scope_hash))
    }

    fn connect(&self) -> Result<postgres::Client, AuthorizationFenceError> {
        Ok(connect_owned(
            &self.database_url,
            PostgresConnectionFamily::FenceControl,
            "calque-authorization-fence",
        )?)
    }
}

fn stored(value: u64) -> Result<i64, AuthorizationFenceError> {
    i64::try_from(value).map_err(|_| AuthorizationFenceError::GenerationOutOfRange(value))
}

fn unsigned(value: i64) -> Result<u64, AuthorizationFenceError> {
    u64::try_from(value).map_err(|_| AuthorizationFenceError::Corrupt)
}

fn scope_hash(bytes: Vec<u8>) -> Result<[u8; 32], AuthorizationFenceError> {
    bytes
        .try_into()
        .map_err(|_| AuthorizationFenceError::Corrupt)
}

fn retirement_in(
    client: &mut impl postgres::GenericClient,
    source_id: &str,
    revoked: i64,
) -> Result<Option<StoredRetirement>, AuthorizationFenceError> {
    let Some(row) = client.query_opt(
        "SELECT effective_generation, ceiling FROM calque_control.authorization_retirement
          WHERE source_id = $1 AND revoked_generation = $2",
        &[&source_id, &revoked],
    )?
    else {
        return Ok(None);
    };
    let revoked_generation = unsigned(revoked)?;
    let effective_generation = unsigned(row.get(0))?;
    let ceiling = unsigned(row.get(1))?;
    let barriers = client
        .query(
            "SELECT scope_hash, barrier_id, barrier_cursor, issued_at_unix_micros,
                    signing_key_id, signature
               FROM calque_control.revocation_barrier
              WHERE source_id = $1 AND revoked_generation = $2
              ORDER BY scope_hash",
            &[&source_id, &revoked],
        )?
        .into_iter()
        .map(|row| {
            Ok(RevocationBarrier {
                barrier_cursor: Some(Cursor { token: row.get(2) }),
                revoked_authorization_generation: revoked_generation,
                effective_authorization_generation: effective_generation,
                revoked_authorization_scope_hash: scope_hash(row.get(0))?.to_vec(),
                barrier_id: row.get(1),
                issued_at_unix_micros: unsigned(row.get(3))?,
                signing_key_id: row.get::<_, Option<String>>(4).unwrap_or_default(),
                signature: row.get::<_, Option<Vec<u8>>>(5).unwrap_or_default(),
            })
        })
        .collect::<Result<_, AuthorizationFenceError>>()?;
    Ok(Some(StoredRetirement {
        revoked_generation,
        effective_generation,
        ceiling,
        barriers,
    }))
}

fn advance_in(
    transaction: &mut postgres::Transaction<'_>,
    source_id: &SourceId,
    generation: u64,
) -> Result<u64, AuthorizationFenceError> {
    let stored = stored(generation)?;
    transaction.execute(
        "INSERT INTO calque_control.authorization_fence (source_id, generation)
         VALUES ($1, $2)
         ON CONFLICT (source_id) DO UPDATE
            SET generation = EXCLUDED.generation, fenced_at = now()
          WHERE calque_control.authorization_fence.generation < EXCLUDED.generation",
        &[&source_id.as_str(), &stored],
    )?;
    let fenced: i64 = transaction
        .query_one(
            "SELECT generation FROM calque_control.authorization_fence WHERE source_id = $1",
            &[&source_id.as_str()],
        )?
        .get(0);
    unsigned(fenced)
}

/// Compiles the deployment's policy, and the one scope it serves, at a given
/// generation. The composition owns the manifest, so it supplies this.
pub type RecompileAuthorization =
    Arc<dyn Fn(u64) -> Result<PreparedLocalSource, String> + Send + Sync>;

/// Publishes a new generation's artifact, or cuts the served one again, while
/// the deployment keeps serving.
pub struct GenerationArtifactPublisher {
    builder: ArtifactBuilder,
    slot: PublicationSlot,
}

impl GenerationArtifactPublisher {
    pub fn new(builder: ArtifactBuilder) -> Self {
        Self {
            builder,
            slot: PublicationSlot::default(),
        }
    }

    /// Publishes `prepared`'s artifact, or finds it already published. Runs on
    /// its own thread with its own runtime: the snapshot path holds a SQLite
    /// handle across awaits, as bootstrap planning does.
    pub(crate) fn publish(&self, prepared: &PreparedLocalSource) -> Result<(), PublishFailure> {
        self.slot.publish(|| {
            current_thread_runtime()?.block_on(async {
                let mut catalog = SqliteArtifactCatalog::open(&self.builder.catalog_path)?;
                self.builder
                    .ensure_initial_snapshot(prepared, &mut catalog)
                    .await?;
                Ok(())
            })
        })
    }

    /// How many fenced generations' publications have started. A re-cut is
    /// started with the count read before the served generation was checked,
    /// and is refused if a publication started since.
    pub(crate) fn fences(&self) -> u64 {
        self.slot.fences()
    }

    /// Cuts `prepared`'s scope again and publishes it beside the artifacts
    /// already published, if `admission` holds when it would become visible.
    /// It waits for a fenced generation's publication, and gives way to one.
    pub(crate) fn recut(
        &self,
        prepared: &PreparedLocalSource,
        admission: PublicationAdmission,
        fences: u64,
    ) -> Result<ValidatedArtifactManifest, PublishFailure> {
        self.slot
            .recut(fences, &self.builder.cancellation, |cancellation| {
                current_thread_runtime()?.block_on(async {
                    let mut catalog = SqliteArtifactCatalog::open(&self.builder.catalog_path)?;
                    Ok(self
                        .builder
                        .publish_recut(prepared, &mut catalog, cancellation, admission)
                        .await?)
                })
            })
    }
}

fn current_thread_runtime() -> Result<tokio::runtime::Runtime, PublishFailure> {
    crate::background::current_thread_runtime().map_err(PublishFailure::other)
}

/// Builds one artifact at a time. A fenced generation's bootstraps wait on its
/// publication, and a re-cut only shortens replay, so the publication cancels
/// the re-cut in its way rather than queueing behind it, and a re-cut builds
/// in the background class while a publication builds at normal priority.
#[derive(Default)]
struct PublicationSlot {
    serial: StdMutex<()>,
    recut: StdMutex<RecutSlot>,
}

#[derive(Default)]
struct RecutSlot {
    fences: u64,
    running: Option<CancellationToken>,
}

impl PublicationSlot {
    fn fences(&self) -> u64 {
        lock(&self.recut).fences
    }

    /// Cancels the re-cut being built, refuses every re-cut that read the
    /// count before this, then builds once the slot is free.
    fn publish<T>(
        &self,
        build: impl FnOnce() -> Result<T, PublishFailure>,
    ) -> Result<T, PublishFailure> {
        {
            let mut slot = lock(&self.recut);
            slot.fences = slot.fences.wrapping_add(1);
            if let Some(running) = slot.running.take() {
                running.cancel();
            }
        }
        self.serially(build)
    }

    fn serially<T>(
        &self,
        build: impl FnOnce() -> Result<T, PublishFailure>,
    ) -> Result<T, PublishFailure> {
        let _serial = self
            .serial
            .lock()
            .map_err(|_| PublishFailure::other("publisher poisoned"))?;
        build()
    }

    fn recut<T: Send>(
        &self,
        fences: u64,
        parent: &CancellationToken,
        build: impl FnOnce(&CancellationToken) -> Result<T, PublishFailure> + Send,
    ) -> Result<T, PublishFailure> {
        let cancellation = parent.child_token();
        {
            let mut slot = lock(&self.recut);
            if slot.fences != fences {
                return Err(PublishFailure::preempted());
            }
            slot.running = Some(cancellation.clone());
        }
        let built = self.serially(|| {
            if cancellation.is_cancelled() {
                return Err(PublishFailure::other("the re-cut was cancelled"));
            }
            crate::background::run_in_background("calque-artifact-recut", || build(&cancellation))
                .map_err(PublishFailure::other)?
        });
        let mut slot = lock(&self.recut);
        if slot.fences == fences {
            slot.running = None;
        }
        match built {
            Err(_) if cancellation.is_cancelled() && !parent.is_cancelled() => {
                Err(PublishFailure::preempted())
            }
            built => built,
        }
    }
}

fn lock<T>(mutex: &StdMutex<T>) -> std::sync::MutexGuard<'_, T> {
    mutex
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
}

/// Why a generation's artifact was not published.
#[derive(Debug)]
pub(crate) struct PublishFailure {
    pub(crate) detail: String,
    /// Only `calque destroy` gets past it, so a restart meets it again.
    pub(crate) state_must_be_destroyed: bool,
    /// A re-cut a fence cancelled or refused. Nothing failed.
    pub(crate) preempted: bool,
}

impl PublishFailure {
    fn other(detail: impl std::fmt::Display) -> Self {
        Self {
            detail: detail.to_string(),
            state_must_be_destroyed: false,
            preempted: false,
        }
    }

    fn preempted() -> Self {
        Self {
            preempted: true,
            ..Self::other("a fenced generation's publication pre-empted the re-cut")
        }
    }
}

impl From<artifact_catalog::CatalogError> for PublishFailure {
    fn from(error: artifact_catalog::CatalogError) -> Self {
        Self {
            state_must_be_destroyed: error.requires_destroying_state(),
            detail: error.to_string(),
            preempted: false,
        }
    }
}

impl From<LocalSnapshotError> for PublishFailure {
    fn from(error: LocalSnapshotError) -> Self {
        Self {
            state_must_be_destroyed: error.requires_destroying_state(),
            detail: error.to_string(),
            preempted: false,
        }
    }
}

/// What `calque serve` hands `LocalBackend` to make a generation revocable.
/// `calque dev` hands nothing, and mounts neither route.
pub struct LocalRevocationConfig {
    pub store: AuthorizationFenceStore,
    /// What `store.retirements` read at startup.
    pub retired: Vec<StoredRetirement>,
    pub recompile: RecompileAuthorization,
    pub publisher: Arc<GenerationArtifactPublisher>,
}

/// Why a fence order or a barrier batch was not acted on.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Error)]
pub enum RevocationError {
    /// The order or a barrier is not signed by a key this deployment trusts,
    /// or does not say what it must.
    #[error("revocation request is not authentic")]
    Unauthorized,
    /// Authentic, but it does not follow the generation in force: it revokes a
    /// generation other than the one served or makes an older one effective,
    /// or the durable fence has already moved past the one it would make
    /// effective.
    #[error("revocation request conflicts with the generation in force")]
    Conflict,
    #[error("revocation request is malformed")]
    InvalidRequest,
    #[error("revocation is not available")]
    Unavailable,
}

impl RevocationError {
    const fn status(self) -> (StatusCode, &'static str) {
        match self {
            Self::Unauthorized => (StatusCode::UNAUTHORIZED, "UNAUTHORIZED"),
            Self::Conflict => (StatusCode::CONFLICT, "CONFLICT"),
            Self::InvalidRequest => (StatusCode::BAD_REQUEST, "INVALID_REQUEST"),
            Self::Unavailable => (StatusCode::SERVICE_UNAVAILABLE, "UNAVAILABLE"),
        }
    }
}

impl IntoResponse for RevocationError {
    fn into_response(self) -> Response {
        let (status, code) = self.status();
        no_store(status, Json(serde_json::json!({ "code": code })))
    }
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct FenceRequest {
    order: String,
}

/// One unsigned barrier, spelled so the application can rebuild the exact
/// `RevocationBarrier` whose preimage it signs.
#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
struct BarrierDraft {
    barrier_id: String,
    /// The opaque cursor token, which is UTF-8.
    barrier_cursor: String,
    revoked_authorization_generation: u64,
    effective_authorization_generation: u64,
    /// Lowercase hex of the 32-byte scope hash.
    revoked_authorization_scope_hash: String,
    issued_at_unix_micros: u64,
}

impl BarrierDraft {
    fn from_barrier(barrier: &RevocationBarrier) -> Result<Self, RevocationError> {
        let cursor = barrier
            .barrier_cursor
            .as_ref()
            .ok_or(RevocationError::Unavailable)?;
        Ok(Self {
            barrier_id: barrier.barrier_id.clone(),
            barrier_cursor: String::from_utf8(cursor.token.clone())
                .map_err(|_| RevocationError::Unavailable)?,
            revoked_authorization_generation: barrier.revoked_authorization_generation,
            effective_authorization_generation: barrier.effective_authorization_generation,
            revoked_authorization_scope_hash: hex::encode(
                &barrier.revoked_authorization_scope_hash,
            ),
            issued_at_unix_micros: barrier.issued_at_unix_micros,
        })
    }
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
struct FenceResponse {
    revoked_generation: u64,
    effective_generation: u64,
    drafts: Vec<BarrierDraft>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct BarriersRequest {
    barriers: Vec<SignedBarrier>,
}

/// The application's signature over one draft. Everything else the barrier
/// says is the draft Calque already holds, so it is not accepted from the
/// request at all.
#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub(crate) struct SignedBarrier {
    pub(crate) barrier_id: String,
    pub(crate) signing_key_id: String,
    /// Unpadded base64url of the 64-byte Ed25519 signature.
    pub(crate) signature: String,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
struct BarriersResponse {
    attached: usize,
    /// Barriers drawn so long ago that every plan they retired has expired:
    /// nothing to deliver to.
    expired: usize,
}

/// The two routes, or nothing when this deployment cannot revoke.
pub(crate) fn router(backend: LocalBackend) -> Router {
    if !backend.revocation_enabled() {
        return Router::new();
    }
    Router::new()
        .route("/v1/authorization/fence", post(fence))
        .route("/v1/authorization/barriers", post(attach_barriers))
        .layer(DefaultBodyLimit::max(MAX_REVOCATION_BODY_BYTES))
        .with_state(backend)
}

async fn fence(
    State(backend): State<LocalBackend>,
    headers: HeaderMap,
    body: Bytes,
) -> Result<Response, RevocationError> {
    let request: FenceRequest = parse_json(&headers, &body)?;
    let fenced = backend.fence_generation(&request.order).await?;
    let drafts = fenced
        .drafts
        .iter()
        .map(BarrierDraft::from_barrier)
        .collect::<Result<Vec<_>, _>>()?;
    Ok(no_store(
        StatusCode::OK,
        Json(FenceResponse {
            revoked_generation: fenced.revoked_generation,
            effective_generation: fenced.effective_generation,
            drafts,
        }),
    ))
}

async fn attach_barriers(
    State(backend): State<LocalBackend>,
    headers: HeaderMap,
    body: Bytes,
) -> Result<Response, RevocationError> {
    let request: BarriersRequest = parse_json(&headers, &body)?;
    let (attached, expired) = backend.attach_revocation_barriers(request.barriers).await?;
    Ok(no_store(
        StatusCode::OK,
        Json(BarriersResponse { attached, expired }),
    ))
}

/// A request body declared as JSON and shaped as `T`. Any other body is
/// `InvalidRequest`, so it gets this module's `{"code": ...}` answer rather
/// than the extractor's plain-text one.
pub(crate) fn parse_json<T: serde::de::DeserializeOwned>(
    headers: &HeaderMap,
    body: &[u8],
) -> Result<T, RevocationError> {
    let media_type = headers
        .get(header::CONTENT_TYPE)
        .and_then(|value| value.to_str().ok())
        .and_then(|value| value.split(';').next())
        .map(|media_type| media_type.trim().to_ascii_lowercase());
    let is_json = media_type.is_some_and(|media_type| {
        media_type == "application/json"
            || (media_type.starts_with("application/") && media_type.ends_with("+json"))
    });
    if !is_json {
        return Err(RevocationError::InvalidRequest);
    }
    serde_json::from_slice(body).map_err(|_| RevocationError::InvalidRequest)
}

pub(crate) fn no_store(status: StatusCode, body: impl IntoResponse) -> Response {
    let mut response = (status, body).into_response();
    response.headers_mut().insert(
        header::CACHE_CONTROL,
        header::HeaderValue::from_static("no-store"),
    );
    response
}

/// The generation a fence order moved the deployment to, and the drafts the
/// application must sign, one for each scope it retired.
pub(crate) struct FencedGeneration {
    pub(crate) revoked_generation: u64,
    pub(crate) effective_generation: u64,
    pub(crate) drafts: Vec<RevocationBarrier>,
}

#[cfg(test)]
mod tests {
    use std::{
        io,
        time::{Duration, Instant},
    };

    use artifact_catalog::CatalogError;

    use super::*;

    fn live_database_url() -> Option<String> {
        std::env::var("CALQUE_SOURCE_DATABASE_URL")
            .ok()
            .or_else(|| std::env::var("TEST_DATABASE_URL").ok())
    }

    /// A fence store on the live database under a source id of its own, whose
    /// rows go when it drops.
    struct LiveFence {
        raw_database: String,
        source_id: SourceId,
        store: AuthorizationFenceStore,
    }

    impl LiveFence {
        fn new() -> Self {
            let raw_database = live_database_url().expect("live source database URL");
            let source_id = SourceId::new(format!("fence-{}", Uuid::new_v4().simple())).unwrap();
            Self {
                store: AuthorizationFenceStore::new(
                    DatabaseUrl::parse(&raw_database).unwrap(),
                    source_id.clone(),
                ),
                raw_database,
                source_id,
            }
        }

        /// The same source's store as another process opens it.
        fn other_process(&self) -> AuthorizationFenceStore {
            AuthorizationFenceStore::new(
                DatabaseUrl::parse(&self.raw_database).unwrap(),
                self.source_id.clone(),
            )
        }

        fn client(&self) -> postgres::Client {
            postgres::Client::connect(&self.raw_database, postgres::NoTls).unwrap()
        }

        fn issued(&self) -> Vec<(i64, Vec<u8>)> {
            self.client()
                .query(
                    "SELECT generation, scope_hash FROM calque_control.authorization_scope_issued
                      WHERE source_id = $1 ORDER BY generation, scope_hash",
                    &[&self.source_id.as_str()],
                )
                .unwrap()
                .iter()
                .map(|row| (row.get(0), row.get(1)))
                .collect()
        }
    }

    impl Drop for LiveFence {
        fn drop(&mut self) {
            let _ = self.client().batch_execute(&format!(
                "DELETE FROM calque_control.revocation_barrier WHERE source_id = '{source}';
                 DELETE FROM calque_control.authorization_retirement WHERE source_id = '{source}';
                 DELETE FROM calque_control.authorization_scope_issued WHERE source_id = '{source}';
                 DELETE FROM calque_control.authorization_fence WHERE source_id = '{source}';",
                source = self.source_id
            ));
        }
    }

    fn fence_write<'a>(
        source_epoch: &'a SourceEpoch,
        barrier_cursor: &'a Cursor,
    ) -> FenceWrite<'a> {
        FenceWrite {
            source_epoch,
            revoked_generation: 1,
            effective_generation: 2,
            ceiling: 41,
            barrier_cursor,
            issued_at_unix_micros: 7,
        }
    }

    /// One barrier for each scope issued under the retired generation, by
    /// this process or another; and what one fence stored is what a repeated
    /// fence and the next startup read.
    #[test]
    #[ignore = "requires CALQUE_SOURCE_DATABASE_URL or TEST_DATABASE_URL"]
    fn a_fence_stores_one_barrier_per_issued_scope_and_answers_it_again() {
        let live = LiveFence::new();
        let store = &live.store;
        assert_eq!(store.establish(1).unwrap(), 1);
        live.other_process().issue(1, [1; 32]).unwrap();
        store.issue(1, [2; 32]).unwrap();
        store.issue(1, [3; 32]).unwrap();
        assert!(store.is_issued(1, &[2; 32]));
        store.issue(2, [9; 32]).unwrap();
        let epoch = SourceEpoch::new("epoch-a").unwrap();
        let cursor = Cursor {
            token: b"cursor-at-the-head".to_vec(),
        };

        let (fenced, retirement) = store.fence(&fence_write(&epoch, &cursor)).unwrap();
        assert_eq!(fenced, 2);
        assert_eq!(
            (
                retirement.revoked_generation,
                retirement.effective_generation,
                retirement.ceiling
            ),
            (1, 2, 41)
        );
        let scopes: Vec<_> = retirement
            .barriers
            .iter()
            .map(|barrier| barrier.revoked_authorization_scope_hash.clone())
            .collect();
        assert_eq!(scopes, [vec![1; 32], vec![2; 32], vec![3; 32]]);
        for barrier in &retirement.barriers {
            assert_eq!(barrier.barrier_cursor.as_ref(), Some(&cursor));
            assert_eq!(barrier.revoked_authorization_generation, 1);
            assert_eq!(barrier.effective_authorization_generation, 2);
            assert_eq!(barrier.issued_at_unix_micros, 7);
            assert!(barrier.signing_key_id.is_empty() && barrier.signature.is_empty());
        }
        // The retired generation's issued scopes are its barriers now; a
        // later generation's stay.
        assert_eq!(live.issued(), [(2, vec![9; 32])]);

        // A repeated fence, from any process, changes nothing it stored, not
        // even for a scope issued since.
        live.other_process().issue(1, [4; 32]).unwrap();
        let mut repeated = fence_write(&epoch, &cursor);
        repeated.ceiling = 99;
        assert_eq!(
            live.other_process().fence(&repeated).unwrap(),
            (2, retirement.clone())
        );

        let hour = Duration::from_secs(60 * 60);
        assert_eq!(
            store.retirements(&epoch, hour).unwrap(),
            std::slice::from_ref(&retirement)
        );
        assert!(store
            .retirements(&SourceEpoch::new("epoch-b").unwrap(), hour)
            .unwrap()
            .is_empty());
        assert!(store
            .retirements(&epoch, Duration::ZERO)
            .unwrap()
            .is_empty());

        let mut signed = retirement.barriers[1].clone();
        signed.signing_key_id = "app-key-1".to_owned();
        signed.signature = vec![5; 64];
        store.sign(std::slice::from_ref(&signed)).unwrap();
        assert_eq!(
            store.retirements(&epoch, hour).unwrap()[0].barriers[1],
            signed
        );
        let mut unknown = signed;
        unknown.barrier_id = "barrier-no-fence-stored".to_owned();
        assert!(matches!(
            store.sign(&[unknown]),
            Err(AuthorizationFenceError::Corrupt)
        ));
    }

    /// The fence and what it retired are one transaction: a fence whose
    /// barrier cannot be written leaves the generation in force.
    #[test]
    #[ignore = "requires CALQUE_SOURCE_DATABASE_URL or TEST_DATABASE_URL"]
    fn a_fence_whose_barriers_cannot_be_written_does_not_move() {
        let live = LiveFence::new();
        assert_eq!(live.store.establish(1).unwrap(), 1);
        live.store.issue(1, [1; 32]).unwrap();
        let epoch = SourceEpoch::new("epoch-a").unwrap();
        let cursor = Cursor {
            token: b"cursor-at-the-head".to_vec(),
        };
        let refusal = Refusal::install(&live);

        let result = live.store.fence(&fence_write(&epoch, &cursor));
        drop(refusal);
        assert!(
            matches!(result, Err(AuthorizationFenceError::Postgres(_))),
            "{result:?}"
        );
        assert_eq!(
            live.store.establish(0).unwrap(),
            1,
            "the fence moved without its barriers"
        );
        assert!(live
            .store
            .retirements(&epoch, Duration::from_secs(60))
            .unwrap()
            .is_empty());
        assert_eq!(live.issued(), [(1, vec![1; 32])]);
    }

    /// A trigger that refuses the barrier rows of one source only.
    struct Refusal {
        client: postgres::Client,
        name: String,
    }

    impl Refusal {
        fn install(live: &LiveFence) -> Self {
            let mut client = live.client();
            let name = format!("calque_test_refuse_{}", Uuid::new_v4().simple());
            client
                .batch_execute(&format!(
                    "CREATE FUNCTION calque_control.{name}() RETURNS trigger
                     LANGUAGE plpgsql AS $$
                     BEGIN
                         IF NEW.source_id = '{source}' THEN
                             RAISE EXCEPTION 'refused by the test';
                         END IF;
                         RETURN NEW;
                     END $$;
                     CREATE TRIGGER {name} BEFORE INSERT ON calque_control.revocation_barrier
                         FOR EACH ROW EXECUTE FUNCTION calque_control.{name}();",
                    source = live.source_id
                ))
                .unwrap();
            Self { client, name }
        }
    }

    impl Drop for Refusal {
        fn drop(&mut self) {
            let _ = self.client.batch_execute(&format!(
                "DROP TRIGGER IF EXISTS {name} ON calque_control.revocation_barrier;
                 DROP FUNCTION IF EXISTS calque_control.{name}();",
                name = self.name
            ));
        }
    }

    fn json_headers(content_type: &'static str) -> HeaderMap {
        let mut headers = HeaderMap::new();
        headers.insert(
            header::CONTENT_TYPE,
            header::HeaderValue::from_static(content_type),
        );
        headers
    }

    #[test]
    fn a_request_body_is_read_only_when_declared_json_and_shaped_right() {
        let order = br#"{"order":"signed-order"}"#;
        for content_type in [
            "application/json",
            "application/json; charset=utf-8",
            "Application/JSON",
            "application/vnd.calque+json",
        ] {
            let request: FenceRequest = parse_json(&json_headers(content_type), order).unwrap();
            assert_eq!(request.order, "signed-order", "{content_type}");
        }

        for content_type in ["text/plain", "application/jsonx", "multipart/form-data"] {
            assert_eq!(
                parse_json::<FenceRequest>(&json_headers(content_type), order).unwrap_err(),
                RevocationError::InvalidRequest,
                "{content_type}"
            );
        }
        assert_eq!(
            parse_json::<FenceRequest>(&HeaderMap::new(), order).unwrap_err(),
            RevocationError::InvalidRequest
        );

        let json = json_headers("application/json");
        for body in [
            &b"{"[..],
            b"",
            br#"{"order":7}"#,
            br#"{"order":"signed-order","generation":2}"#,
            br#"{"barriers":[]}"#,
        ] {
            assert_eq!(
                parse_json::<FenceRequest>(&json, body).unwrap_err(),
                RevocationError::InvalidRequest,
                "{}",
                String::from_utf8_lossy(body)
            );
        }
        assert_eq!(
            parse_json::<BarriersRequest>(
                &json,
                br#"{"barriers":[{"barrierId":"barrier-1","signingKeyId":"key-1"}]}"#
            )
            .unwrap_err(),
            RevocationError::InvalidRequest
        );
    }

    #[tokio::test]
    async fn a_malformed_request_is_answered_with_a_code_and_no_store() {
        let response = RevocationError::InvalidRequest.into_response();
        assert_eq!(response.status(), StatusCode::BAD_REQUEST);
        assert_eq!(response.headers()[header::CACHE_CONTROL], "no-store");
        let body = axum::body::to_bytes(response.into_body(), usize::MAX)
            .await
            .unwrap();
        assert_eq!(
            serde_json::from_slice::<serde_json::Value>(&body).unwrap(),
            serde_json::json!({ "code": "INVALID_REQUEST" })
        );
    }

    /// A fenced generation's publication cancels a re-cut blocked mid-build
    /// instead of waiting for it, and a re-cut that checked the served
    /// generation before that publication began never starts. Shutting down
    /// is not a pre-emption.
    #[test]
    fn a_fenced_publication_preempts_a_re_cut_blocked_mid_build() {
        let slot = Arc::new(PublicationSlot::default());
        let process = CancellationToken::new();
        let before_fence = slot.fences();
        let (building, blocked) = std::sync::mpsc::channel();
        let recut = {
            let slot = Arc::clone(&slot);
            let process = process.clone();
            std::thread::spawn(move || {
                slot.recut(before_fence, &process, |cancellation| {
                    building.send(()).unwrap();
                    let gives_up = Instant::now() + Duration::from_secs(20);
                    while !cancellation.is_cancelled() && Instant::now() < gives_up {
                        std::thread::sleep(Duration::from_millis(5));
                    }
                    Err::<(), _>(PublishFailure::other("the build stopped"))
                })
            })
        };
        blocked.recv_timeout(Duration::from_secs(5)).unwrap();

        let started = Instant::now();
        assert!(
            !slot
                .publish(|| Ok(background_thread::current_thread_is_lowered()))
                .unwrap(),
            "a fenced publication builds at normal priority"
        );
        assert!(
            started.elapsed() < Duration::from_secs(5),
            "the publication waited {:?} behind the re-cut",
            started.elapsed()
        );
        let failure = recut.join().unwrap().unwrap_err();
        assert!(failure.preempted, "{}", failure.detail);

        let stale = slot
            .recut(before_fence, &process, |_| -> Result<(), PublishFailure> {
                panic!("a re-cut started after a fenced publication it never saw")
            })
            .unwrap_err();
        assert!(stale.preempted, "{}", stale.detail);
        assert!(
            slot.recut(slot.fences(), &process, |_| Ok(
                background_thread::current_thread_is_lowered()
            ))
            .unwrap(),
            "a re-cut builds in the background class"
        );

        process.cancel();
        let stopped = slot
            .recut(slot.fences(), &process, |_| -> Result<(), PublishFailure> {
                panic!("a re-cut started after shutdown")
            })
            .unwrap_err();
        assert!(!stopped.preempted, "{}", stopped.detail);
    }

    /// A publication that failed on state only `calque destroy` gets past
    /// says so, with the refusal as its detail. One that failed otherwise,
    /// which a restart may get past, does not.
    #[test]
    fn a_failed_publication_says_whether_it_needs_calque_destroy() {
        let state_dir = std::path::PathBuf::from("/srv/calque/state");
        let catalog = CatalogError::EarlierSchemaVersion {
            state_dir: state_dir.clone(),
            version: 2,
        };
        let refusal = catalog.to_string();
        let failure = PublishFailure::from(catalog);
        assert!(failure.state_must_be_destroyed);
        assert_eq!(failure.detail, refusal);
        let failure = PublishFailure::from(LocalSnapshotError::ForeignArtifactBuild {
            state_dir,
            artifact: "local-artifact-earlier-release".to_owned(),
        });
        assert!(failure.state_must_be_destroyed, "{}", failure.detail);

        for failure in [
            PublishFailure::from(CatalogError::Io(io::Error::other("catalog read failed"))),
            PublishFailure::from(LocalSnapshotError::Connection),
            PublishFailure::other("publisher poisoned"),
        ] {
            assert!(!failure.state_must_be_destroyed, "{}", failure.detail);
        }
    }
}
