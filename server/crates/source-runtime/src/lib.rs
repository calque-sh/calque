//! Persistent PostgreSQL source ownership and runtime composition.
//!
//! The runtime composes an external monotonic fence, one-shot persistent-slot
//! enrollment, stale-backend revocation, automatic lease renewal, active
//! replication-session ownership, and the ordered durable-ingest-to-feedback
//! boundary. A [`ProvisionalLease`] remains insufficient to append canonical
//! history or authorize PostgreSQL feedback; promotion requires PostgreSQL
//! slot ownership plus the shared-log writer fence.

use std::{
    fmt,
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc, Mutex, MutexGuard, PoisonError,
    },
    time::{Duration, Instant},
};

use canonical_log::{PostgresDatastoreIdentity, CONTROL_SCHEMA_BOOTSTRAP_LOCK_KEY};
use pg_walstream::PgReplicationShutdownHandle;
use postgres::{error::SqlState, Client, Transaction};
use sync_types::{SourceEpoch, SourceId};
use thiserror::Error;

mod persistent_ingest;
mod publication;
mod slot_owner;

pub use persistent_ingest::{
    PersistentIngestBoundary, PersistentIngestConfig, PersistentIngestDisposition,
    PersistentIngestError, PersistentIngestObserver, PersistentIngestStats,
};
pub use publication::{inspect_owned_publication, OwnedPublication};
pub use slot_owner::{
    establish_persistent_source, establish_persistent_source_interruptible,
    provision_persistent_slot, ActiveSourceSession, OpenTransaction, SlotEnrollment, SlotMetadata,
    SlotProvisioningConfig, SourceRuntimeError, DEFAULT_OPEN_TRANSACTION_WAIT,
};

/// Cross-thread, single-purpose revocation for source-session startup.
///
/// A supervisor may request shutdown before the replication socket exists. The
/// request is remembered and applied as soon as the socket is published, so a
/// bounded startup deadline cannot lose a cancellation race.
#[derive(Default)]
pub struct SourceStartupShutdown {
    state: Mutex<SourceStartupShutdownState>,
}

#[derive(Default)]
struct SourceStartupShutdownState {
    requested: bool,
    handle: Option<PgReplicationShutdownHandle>,
}

impl SourceStartupShutdown {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn request_shutdown(&self) -> Result<(), SourceRuntimeError> {
        let mut state = self
            .state
            .lock()
            .map_err(|_| SourceRuntimeError::ReplicationUnavailable)?;
        state.requested = true;
        if let Some(handle) = state.handle.as_ref() {
            handle
                .request_shutdown()
                .map_err(|_| SourceRuntimeError::ReplicationUnavailable)?;
        }
        Ok(())
    }

    /// Drops the duplicated startup descriptor without touching the live
    /// replication socket. Returns `false` if shutdown already won the race.
    pub fn disarm(&self) -> Result<bool, SourceRuntimeError> {
        let mut state = self
            .state
            .lock()
            .map_err(|_| SourceRuntimeError::ReplicationUnavailable)?;
        if state.requested {
            return Ok(false);
        }
        state.handle.take();
        Ok(true)
    }

    pub(crate) fn publish(&self, handle: PgReplicationShutdownHandle) {
        let Ok(mut state) = self.state.lock() else {
            let _ = handle.request_shutdown();
            return;
        };
        if state.requested {
            let _ = handle.request_shutdown();
        }
        state.handle = Some(handle);
    }
}

const TOKEN_SEQUENCE: &str = "calque_control.source_fence_token_seq";
const LEASE_TABLE: &str = "calque_control.source_fence_lease";
const MIN_LEASE_TTL: Duration = Duration::from_millis(100);
const MAX_LEASE_TTL: Duration = Duration::from_secs(300);
const MAX_OWNER_ID_BYTES: usize = 256;
/// The control connection's `lock_timeout`: the longest one lease operation
/// waits for another transaction to release the lease row.
const CONTROL_LOCK_TIMEOUT: Duration = Duration::from_secs(1);
/// The pause before the first retry of a renewal that met the lease row
/// locked. Each later pause doubles, up to `RENEWAL_RETRY_MAX_PAUSE`.
const RENEWAL_RETRY_FIRST_PAUSE: Duration = Duration::from_millis(50);
const RENEWAL_RETRY_MAX_PAUSE: Duration = Duration::from_millis(400);

// Runs on every connect and reconnect, under a 1 s lock timeout. On existing
// tables `CREATE ... IF NOT EXISTS` takes no table lock, whereas any `ALTER
// TABLE` here would take ACCESS EXCLUSIVE and wait behind an autovacuum.
const INSTALL_SQL: &str = r#"
CREATE SCHEMA IF NOT EXISTS calque_control;
CREATE SEQUENCE IF NOT EXISTS calque_control.source_fence_token_seq
    AS bigint
    MINVALUE 1
    START WITH 1
    NO CYCLE;
CREATE TABLE IF NOT EXISTS calque_control.source_fence_lease (
    source_id text PRIMARY KEY,
    source_epoch text NOT NULL,
    fence_token bigint NOT NULL DEFAULT 0 CHECK (fence_token >= 0),
    owner_id text,
    lease_phase text NOT NULL DEFAULT 'unowned'
        CHECK (lease_phase IN ('unowned', 'provisional', 'active')),
    expires_at timestamptz,
    source_backend_pid integer,
    CHECK (
        (lease_phase = 'unowned'
            AND owner_id IS NULL
            AND expires_at IS NULL
            AND source_backend_pid IS NULL)
        OR
        (lease_phase = 'provisional'
            AND owner_id IS NOT NULL
            AND expires_at IS NOT NULL)
        OR
        (lease_phase = 'active'
            AND owner_id IS NOT NULL
            AND expires_at IS NOT NULL
            AND source_backend_pid IS NOT NULL)
    )
);
CREATE TABLE IF NOT EXISTS calque_control.canonical_log_writer_fence (
    source_id text PRIMARY KEY,
    source_epoch text NOT NULL,
    fence_token bigint NOT NULL CHECK (fence_token > 0)
);
CREATE TABLE IF NOT EXISTS calque_control.source_slot_enrollment (
    source_id text PRIMARY KEY,
    source_epoch text NOT NULL,
    slot_name text NOT NULL,
    publication_name text NOT NULL,
    system_identifier text NOT NULL,
    database_name text NOT NULL,
    timeline_id bigint NOT NULL CHECK (timeline_id >= 0),
    previous_timeline_id bigint NOT NULL CHECK (previous_timeline_id >= 0),
    pgoutput_profile text NOT NULL,
    initial_lsn text NOT NULL,
    durable_canonical_lsn text NOT NULL
);
"#;

/// A non-secret process identity used for exact lease ownership checks.
#[derive(Clone, PartialEq, Eq, Hash)]
pub struct OwnerId(String);

impl OwnerId {
    pub fn new(value: impl Into<String>) -> Result<Self, FenceError> {
        let value = value.into();
        if value.is_empty()
            || value.len() > MAX_OWNER_ID_BYTES
            || !value.bytes().all(|byte| {
                byte.is_ascii_alphanumeric() || matches!(byte, b'.' | b'_' | b':' | b'/' | b'-')
            })
        {
            return Err(FenceError::InvalidOwnerId);
        }
        Ok(Self(value))
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl fmt::Debug for OwnerId {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.debug_tuple("OwnerId").field(&self.0).finish()
    }
}

/// A positive, never-reused signed 64-bit fencing token.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct FenceToken(i64);

impl FenceToken {
    fn from_database(value: i64) -> Result<Self, FenceError> {
        if value <= 0 {
            return Err(FenceError::InvalidStoredToken(value));
        }
        Ok(Self(value))
    }

    pub const fn get(self) -> i64 {
        self.0
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LeasePhase {
    Provisional,
    Active,
}

/// Evidence that stale-backend revocation, slot inactivity, backend binding,
/// and durable-log writer exclusion have all succeeded. Only
/// `establish_persistent_source` builds one.
pub struct ActivationEvidence {
    backend_pid: i32,
    fence_token: FenceToken,
}

impl ActivationEvidence {
    fn from_exclusion(
        lease: &LeaseIdentity,
        backend_pid: i32,
        claim: &DurableLogWriterClaim,
    ) -> Result<Self, FenceError> {
        if backend_pid <= 0
            || claim.source_id != lease.source_id
            || claim.source_epoch != lease.source_epoch
            || claim.token != lease.token
        {
            return Err(FenceError::ActivationEvidenceMismatch);
        }
        Ok(Self {
            backend_pid,
            fence_token: lease.token,
        })
    }
}

impl fmt::Debug for ActivationEvidence {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ActivationEvidence")
            .field("backend_pid", &self.backend_pid)
            .field("fence_token", &self.fence_token)
            .finish()
    }
}

impl LeasePhase {
    const fn as_database(self) -> &'static str {
        match self {
            Self::Provisional => "provisional",
            Self::Active => "active",
        }
    }

    fn from_database(value: &str) -> Result<Self, FenceError> {
        match value {
            "provisional" => Ok(Self::Provisional),
            "active" => Ok(Self::Active),
            _ => Err(FenceError::InvalidStoredPhase(value.to_owned())),
        }
    }
}

#[derive(Debug, Clone)]
pub struct AcquireRequest {
    pub source_id: SourceId,
    pub source_epoch: SourceEpoch,
    pub owner_id: OwnerId,
    pub ttl: Duration,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LeaseIdentity {
    source_id: SourceId,
    source_epoch: SourceEpoch,
    owner_id: OwnerId,
    token: FenceToken,
}

impl LeaseIdentity {
    pub fn source_id(&self) -> &SourceId {
        &self.source_id
    }

    pub fn source_epoch(&self) -> &SourceEpoch {
        &self.source_epoch
    }

    pub fn owner_id(&self) -> &OwnerId {
        &self.owner_id
    }

    pub const fn token(&self) -> FenceToken {
        self.token
    }
}

/// Non-secret state suitable for metrics and operator diagnostics.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LeaseSnapshot {
    pub source_id: SourceId,
    pub source_epoch: SourceEpoch,
    pub owner_id: OwnerId,
    pub token: FenceToken,
    pub phase: LeasePhase,
    pub expires_at_unix_millis: i64,
    pub source_backend_pid: Option<i32>,
}

#[derive(Clone, PartialEq, Eq)]
pub struct DurableLogWriterClaim {
    source_id: SourceId,
    source_epoch: SourceEpoch,
    token: FenceToken,
    datastore: PostgresDatastoreIdentity,
    control_connection: Arc<str>,
}

impl fmt::Debug for DurableLogWriterClaim {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("DurableLogWriterClaim")
            .field("source_id", &self.source_id)
            .field("source_epoch", &self.source_epoch)
            .field("token", &self.token)
            .field("datastore", &self.datastore)
            .finish_non_exhaustive()
    }
}

impl DurableLogWriterClaim {
    pub fn datastore(&self) -> &PostgresDatastoreIdentity {
        &self.datastore
    }

    pub(crate) fn control_connection(&self) -> &str {
        &self.control_connection
    }
}

impl LeaseSnapshot {
    pub fn identity(&self) -> LeaseIdentity {
        LeaseIdentity {
            source_id: self.source_id.clone(),
            source_epoch: self.source_epoch.clone(),
            owner_id: self.owner_id.clone(),
            token: self.token,
        }
    }
}

/// Provider-neutral durable operations required by the source owner.
///
/// Implementations must decide lease expiry using their own authoritative
/// clock and must serialize every mutation for a source.
pub trait FenceStore: Send + Sync {
    fn enroll_source(
        &self,
        source_id: &SourceId,
        source_epoch: &SourceEpoch,
    ) -> Result<(), FenceError>;

    fn acquire(&self, request: &AcquireRequest) -> Result<LeaseSnapshot, FenceError>;

    fn renew(&self, lease: &LeaseIdentity, ttl: Duration) -> Result<LeaseSnapshot, FenceError>;

    fn validate(&self, lease: &LeaseIdentity) -> Result<LeaseSnapshot, FenceError>;

    fn bind_backend(
        &self,
        lease: &LeaseIdentity,
        backend_pid: i32,
    ) -> Result<LeaseSnapshot, FenceError>;

    fn claim_durable_log_writer(
        &self,
        lease: &LeaseIdentity,
    ) -> Result<DurableLogWriterClaim, FenceError>;

    fn load_slot_enrollment(
        &self,
        lease: &LeaseIdentity,
    ) -> Result<Option<SlotEnrollment>, FenceError>;

    fn record_slot_enrollment(
        &self,
        lease: &LeaseIdentity,
        enrollment: &SlotEnrollment,
    ) -> Result<(), FenceError>;

    fn advance_durable_canonical_lsn(
        &self,
        lease: &LeaseIdentity,
        durable_lsn: u64,
    ) -> Result<(), FenceError>;

    fn transition_active(
        &self,
        lease: &LeaseIdentity,
        evidence: &ActivationEvidence,
    ) -> Result<LeaseSnapshot, FenceError>;

    fn release(&self, lease: &LeaseIdentity) -> Result<(), FenceError>;
}

/// A locally revocable provisional lease. It authorizes no append or feedback;
/// only `establish_persistent_source` turns it into an [`ActiveLease`].
#[derive(Clone)]
pub struct ProvisionalLease {
    inner: Arc<ProvisionalLeaseInner>,
}

struct ProvisionalLeaseInner {
    identity: LeaseIdentity,
    locally_valid: AtomicBool,
    ttl: Duration,
    /// As for `ActiveLeaseInner::held_until`. Acquisition sets it, and each
    /// renewal moves it.
    held_until: Mutex<Instant>,
}

#[derive(Clone)]
pub struct ActiveLease {
    inner: Arc<ActiveLeaseInner>,
}

struct ActiveLeaseInner {
    identity: LeaseIdentity,
    locally_valid: AtomicBool,
    ttl: Duration,
    /// A local instant no later than the lease's expiry in the control store:
    /// when the request that last set that expiry was sent, plus its TTL. It
    /// starts as the provisional lease's value, since activation does not
    /// move the expiry. A renewal holds this lock across its store call, so
    /// the value always comes from the renewal that ran last.
    held_until: Mutex<Instant>,
}

impl fmt::Debug for ActiveLease {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ActiveLease")
            .field("identity", &self.inner.identity)
            .field("locally_valid", &self.is_locally_valid())
            .field("ttl", &self.inner.ttl)
            .finish()
    }
}

impl ActiveLease {
    pub fn identity(&self) -> &LeaseIdentity {
        &self.inner.identity
    }

    pub fn is_locally_valid(&self) -> bool {
        self.inner.locally_valid.load(Ordering::Acquire)
    }

    pub fn revoke(&self) {
        self.inner.locally_valid.store(false, Ordering::Release);
    }

    pub fn ttl(&self) -> Duration {
        self.inner.ttl
    }
}

impl fmt::Debug for ProvisionalLease {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ProvisionalLease")
            .field("identity", &self.inner.identity)
            .field("locally_valid", &self.is_locally_valid())
            .field("ttl", &self.inner.ttl)
            .finish()
    }
}

impl ProvisionalLease {
    pub fn identity(&self) -> &LeaseIdentity {
        &self.inner.identity
    }

    pub fn is_locally_valid(&self) -> bool {
        self.inner.locally_valid.load(Ordering::Acquire)
    }

    pub fn revoke(&self) {
        self.inner.locally_valid.store(false, Ordering::Release);
    }

    pub fn ttl(&self) -> Duration {
        self.inner.ttl
    }
}

/// How an active renewal ended when it did not fail.
#[derive(Debug)]
enum ActiveRenewal {
    Renewed,
    /// The caller asked to stop during a pause between attempts. The lease is
    /// left as it was, for the caller to revoke.
    Stopped,
}

/// Time and waiting for the loops that keep a lease renewed, so that a test
/// can bring a lease to its expiry without waiting for it.
pub(crate) trait RenewalClock {
    fn now(&self) -> Instant;

    /// Waits for `pause`, and returns `false` if the caller asked to stop
    /// before it ended.
    fn pause(&mut self, pause: Duration) -> bool;
}

/// A poisoned lock still holds a value no later than the lease's expiry,
/// because a renewal writes it only after the store confirms.
fn lock_held_until(held_until: &Mutex<Instant>) -> MutexGuard<'_, Instant> {
    held_until.lock().unwrap_or_else(PoisonError::into_inner)
}

/// How long a lease of `ttl` goes between two renewals.
fn renewal_interval(ttl: Duration) -> Duration {
    (ttl / 3).max(Duration::from_millis(25))
}

/// How long after `now` a lease of `ttl` held until `held_until` is next
/// renewed: one renewal interval after the request that last set its expiry
/// was sent, and at once when that has passed.
///
/// The wait counts from that request, not from when a loop starts: a lease
/// reaches each loop already aged, and three loops hand one lease on, so
/// waiting a whole interval in each could leave it a whole TTL unrenewed.
fn renewal_wait(ttl: Duration, held_until: Instant, now: Instant) -> Duration {
    (held_until + renewal_interval(ttl)).saturating_duration_since(now + ttl)
}

/// Makes a provisional lease from `request`, a store call that sets the
/// lease's expiry `ttl` after the store runs it. The lease is held locally
/// until `ttl` after the call started, which is no later than that expiry.
fn provisional_lease(
    ttl: Duration,
    request: impl FnOnce() -> Result<LeaseSnapshot, FenceError>,
) -> Result<ProvisionalLease, FenceError> {
    let sent_at = Instant::now();
    let snapshot = request()?;
    if snapshot.phase != LeasePhase::Provisional {
        return Err(FenceError::InvalidStoredPhase(
            snapshot.phase.as_database().to_owned(),
        ));
    }
    Ok(ProvisionalLease {
        inner: Arc::new(ProvisionalLeaseInner {
            identity: snapshot.identity(),
            locally_valid: AtomicBool::new(true),
            ttl,
            held_until: Mutex::new(sent_at + ttl),
        }),
    })
}

/// Applies fail-closed local revocation around a provider-neutral store.
pub struct FenceAuthority<S> {
    store: S,
}

impl<S> FenceAuthority<S>
where
    S: FenceStore,
{
    pub const fn new(store: S) -> Self {
        Self { store }
    }

    pub fn acquire(&self, request: AcquireRequest) -> Result<ProvisionalLease, FenceError> {
        validate_ttl(request.ttl)?;
        provisional_lease(request.ttl, || self.store.acquire(&request))
    }

    pub fn renew(
        &self,
        lease: &ProvisionalLease,
        ttl: Duration,
    ) -> Result<LeaseSnapshot, FenceError> {
        self.renew_sent(lease, ttl, Instant::now)
    }

    /// `renew`, with the instant the request is sent read from `now` once no
    /// other renewal of `lease` is in flight.
    fn renew_sent(
        &self,
        lease: &ProvisionalLease,
        ttl: Duration,
        now: impl FnOnce() -> Instant,
    ) -> Result<LeaseSnapshot, FenceError> {
        validate_ttl(ttl)?;
        let mut held_until = lock_held_until(&lease.inner.held_until);
        let sent_at = now();
        let snapshot = self.operate(lease, |identity| self.store.renew(identity, ttl))?;
        *held_until = sent_at + ttl;
        Ok(snapshot)
    }

    /// Keeps a provisional `lease` renewed for its TTL until `clock` reports
    /// a stop. A renewal that fails revokes the lease and ends the loop.
    pub(crate) fn keep_provisional_renewed(
        &self,
        lease: &ProvisionalLease,
        clock: &mut impl RenewalClock,
    ) {
        loop {
            let held_until = *lock_held_until(&lease.inner.held_until);
            let wait = renewal_wait(lease.ttl(), held_until, clock.now());
            if !clock.pause(wait) {
                return;
            }
            if self.renew_sent(lease, lease.ttl(), || clock.now()).is_err() {
                lease.revoke();
                return;
            }
        }
    }

    pub fn validate(&self, lease: &ProvisionalLease) -> Result<LeaseSnapshot, FenceError> {
        self.operate(lease, |identity| self.store.validate(identity))
    }

    pub fn bind_backend(
        &self,
        lease: &ProvisionalLease,
        backend_pid: i32,
    ) -> Result<LeaseSnapshot, FenceError> {
        if backend_pid <= 0 {
            return Err(FenceError::InvalidBackendPid(backend_pid));
        }
        self.operate(lease, |identity| {
            self.store.bind_backend(identity, backend_pid)
        })
    }

    fn claim_durable_log_writer(
        &self,
        lease: &ProvisionalLease,
    ) -> Result<DurableLogWriterClaim, FenceError> {
        self.operate(lease, |identity| {
            self.store.claim_durable_log_writer(identity)
        })
    }

    fn load_slot_enrollment(
        &self,
        lease: &ProvisionalLease,
    ) -> Result<Option<SlotEnrollment>, FenceError> {
        self.operate(lease, |identity| self.store.load_slot_enrollment(identity))
    }

    fn record_slot_enrollment(
        &self,
        lease: &ProvisionalLease,
        enrollment: &SlotEnrollment,
    ) -> Result<(), FenceError> {
        self.operate(lease, |identity| {
            self.store.record_slot_enrollment(identity, enrollment)
        })
    }

    fn activate(
        &self,
        lease: ProvisionalLease,
        evidence: &ActivationEvidence,
    ) -> Result<ActiveLease, FenceError> {
        let snapshot = self.operate(&lease, |identity| {
            self.store.transition_active(identity, evidence)
        })?;
        if snapshot.phase != LeasePhase::Active {
            lease.revoke();
            return Err(FenceError::InvalidStoredPhase(
                snapshot.phase.as_database().to_owned(),
            ));
        }
        lease.revoke();
        let held_until = *lock_held_until(&lease.inner.held_until);
        Ok(ActiveLease {
            inner: Arc::new(ActiveLeaseInner {
                identity: snapshot.identity(),
                locally_valid: AtomicBool::new(true),
                ttl: lease.ttl(),
                held_until: Mutex::new(held_until),
            }),
        })
    }

    /// Renews `lease` for its TTL.
    ///
    /// A renewal that waits out `lock_timeout` on the lease row, as it does
    /// behind a multi-statement canonical append, is retried after a pause
    /// while one more such wait would still end before the lease can expire.
    /// Any other failure, a locally revoked lease, or a lock that could
    /// outlast that bound revokes the lease. The store refuses a lease that
    /// another owner holds, so a retry cannot renew one.
    fn renew_active(
        &self,
        lease: &ActiveLease,
        clock: &mut impl RenewalClock,
    ) -> Result<ActiveRenewal, FenceError> {
        let ttl = lease.ttl();
        let mut held_until = lock_held_until(&lease.inner.held_until);
        let mut pause = RENEWAL_RETRY_FIRST_PAUSE;
        loop {
            if !lease.is_locally_valid() {
                return Err(FenceError::LeaseLost);
            }
            let sent_at = clock.now();
            match self.store.renew(lease.identity(), ttl) {
                Ok(_) => {
                    *held_until = sent_at + ttl;
                    return Ok(ActiveRenewal::Renewed);
                }
                Err(FenceError::LeaseRowLocked)
                    if clock.now() + pause + CONTROL_LOCK_TIMEOUT < *held_until =>
                {
                    if !clock.pause(pause) {
                        return Ok(ActiveRenewal::Stopped);
                    }
                    pause = (pause * 2).min(RENEWAL_RETRY_MAX_PAUSE);
                }
                Err(error) => {
                    lease.revoke();
                    return Err(error);
                }
            }
        }
    }

    /// Keeps an active `lease` renewed until `clock` reports a stop, and
    /// returns the failure of a renewal that revoked it.
    pub(crate) fn keep_active_renewed(
        &self,
        lease: &ActiveLease,
        clock: &mut impl RenewalClock,
    ) -> Result<(), FenceError> {
        loop {
            let held_until = *lock_held_until(&lease.inner.held_until);
            let wait = renewal_wait(lease.ttl(), held_until, clock.now());
            if !clock.pause(wait) {
                return Ok(());
            }
            if let ActiveRenewal::Stopped = self.renew_active(lease, clock)? {
                return Ok(());
            }
        }
    }

    pub fn validate_active(&self, lease: &ActiveLease) -> Result<LeaseSnapshot, FenceError> {
        self.operate_active(lease, |identity| self.store.validate(identity))
    }

    pub fn advance_durable_canonical_lsn(
        &self,
        lease: &ActiveLease,
        durable_lsn: u64,
    ) -> Result<(), FenceError> {
        self.operate_active(lease, |identity| {
            self.store
                .advance_durable_canonical_lsn(identity, durable_lsn)
        })
    }

    fn release_revoked_active(&self, lease: &ActiveLease) -> Result<(), FenceError> {
        let result = self.store.release(lease.identity());
        lease.revoke();
        result
    }

    pub fn release(&self, lease: &ProvisionalLease) -> Result<(), FenceError> {
        let result = self.operate(lease, |identity| {
            self.store.release(identity)?;
            Ok(())
        });
        lease.revoke();
        result
    }

    /// Releases the lease of a start that failed. The start's own error is
    /// the one to report; a release that fails too leaves the lease to expire.
    pub fn release_after_failure(&self, lease: &ProvisionalLease) {
        if let Err(error) = self.release(lease) {
            tracing::warn!(%error, "a failed start did not release its source lease; the lease expires on its own");
        }
    }

    fn operate<T>(
        &self,
        lease: &ProvisionalLease,
        operation: impl FnOnce(&LeaseIdentity) -> Result<T, FenceError>,
    ) -> Result<T, FenceError> {
        if !lease.is_locally_valid() {
            return Err(FenceError::LeaseLost);
        }
        match operation(lease.identity()) {
            Ok(value) => Ok(value),
            Err(error) => {
                lease.revoke();
                Err(error)
            }
        }
    }

    fn operate_active<T>(
        &self,
        lease: &ActiveLease,
        operation: impl FnOnce(&LeaseIdentity) -> Result<T, FenceError>,
    ) -> Result<T, FenceError> {
        if !lease.is_locally_valid() {
            return Err(FenceError::LeaseLost);
        }
        match operation(lease.identity()) {
            Ok(value) => Ok(value),
            Err(error) => {
                lease.revoke();
                Err(error)
            }
        }
    }
}

impl FenceAuthority<PostgresFenceStore> {
    /// Takes the lease back for the owner that held `previous`, minting a new
    /// provisional token, after its replication session died with the lease
    /// still recorded as its own.
    ///
    /// Unlike `acquire`, it does not wait for that lease to expire. It succeeds
    /// only while the row still carries `previous`'s token or owner; the owner
    /// case covers an earlier reacquire whose reply was lost. Otherwise another
    /// owner took the source over, and it fails with `LeaseSuperseded` without
    /// touching the row.
    pub fn reacquire(
        &self,
        previous: &LeaseIdentity,
        ttl: Duration,
    ) -> Result<ProvisionalLease, FenceError> {
        validate_ttl(ttl)?;
        provisional_lease(ttl, || self.store.reacquire(previous, ttl))
    }

    /// Executes one bounded feedback operation while the exact active lease
    /// row remains locked in the control database.
    ///
    /// A higher-token takeover cannot commit between validation and the
    /// replication-socket flush. Once this method returns and releases the
    /// row lock, takeover may proceed and revoke the old backend normally.
    pub fn with_active_feedback_fence<T>(
        &self,
        lease: &ActiveLease,
        operation: impl FnOnce() -> T,
    ) -> Result<T, FenceError> {
        if !lease.is_locally_valid() {
            return Err(FenceError::LeaseLost);
        }
        match self.store.with_feedback_fence(lease.identity(), operation) {
            Ok(value) => Ok(value),
            Err(error) => {
                lease.revoke();
                Err(error)
            }
        }
    }
}

/// PostgreSQL control-store implementation using row locks, database time, and
/// a non-cycling durable sequence for globally unique monotonic tokens.
pub struct PostgresFenceStore {
    client: Mutex<Client>,
    datastore: PostgresDatastoreIdentity,
    control_connection: Arc<str>,
}

impl PostgresFenceStore {
    pub fn connect(connection_string: &str) -> Result<Self, FenceError> {
        let mut client = postgres_connect::Endpoint::parse(connection_string)
            .map_err(|_| store_error_unavailable())?
            .connect()
            .map_err(store_error)?;
        configure_control_connection(&mut client)?;
        install_schema(&mut client)?;
        let datastore = PostgresDatastoreIdentity::inspect(&mut client)
            .map_err(|_| store_error_unavailable())?;
        Ok(Self {
            client: Mutex::new(client),
            datastore,
            control_connection: Arc::from(connection_string),
        })
    }

    /// Returns the control connection PID for sanitized diagnostics and
    /// failure-injection tests. It is not a source replication backend PID.
    pub fn control_backend_pid(&self) -> Result<i32, FenceError> {
        self.client()?
            .query_one("SELECT pg_backend_pid()", &[])
            .map(|row| row.get(0))
            .map_err(store_error)
    }

    fn client(&self) -> Result<MutexGuard<'_, Client>, FenceError> {
        self.client
            .lock()
            .map_err(|_| FenceError::ControlStoreUnavailable)
    }
}

impl FenceStore for PostgresFenceStore {
    fn enroll_source(
        &self,
        source_id: &SourceId,
        source_epoch: &SourceEpoch,
    ) -> Result<(), FenceError> {
        let affected = self
            .client()?
            .execute(
                &format!(
                    "INSERT INTO {LEASE_TABLE} (source_id, source_epoch) VALUES ($1, $2) ON CONFLICT (source_id) DO NOTHING"
                ),
                &[&source_id.as_str(), &source_epoch.as_str()],
            )
            .map_err(store_error)?;
        if affected == 0 {
            return Err(FenceError::SourceAlreadyEnrolled(source_id.clone()));
        }
        Ok(())
    }

    fn acquire(&self, request: &AcquireRequest) -> Result<LeaseSnapshot, FenceError> {
        validate_ttl(request.ttl)?;
        let ttl_millis = duration_millis(request.ttl)?;
        let mut client = self.client()?;
        let mut transaction = client.transaction().map_err(store_error)?;
        transaction
            .batch_execute("SET LOCAL synchronous_commit = on")
            .map_err(store_error)?;
        let row = load_source_for_update(&mut transaction, &request.source_id)?;
        let stored_epoch: String = row.get("source_epoch");
        if stored_epoch != request.source_epoch.as_str() {
            return Err(FenceError::EpochMismatch {
                expected: request.source_epoch.clone(),
                actual: SourceEpoch::new(stored_epoch)
                    .map_err(|_| FenceError::InvalidStoredEpoch)?,
            });
        }
        let phase: String = row.get("lease_phase");
        let expired: bool = row.get("expired");
        if phase != "unowned" && !expired {
            return Err(FenceError::LeaseHeld {
                expires_at_unix_millis: row.get("expires_at_unix_millis"),
            });
        }

        let token = next_token(&mut transaction)?;
        let snapshot = transaction
            .query_one(
                &format!(
                    "UPDATE {LEASE_TABLE}
                     SET fence_token = $2, owner_id = $3, lease_phase = 'provisional',
                         expires_at = clock_timestamp() + ($4::bigint * interval '1 millisecond'),
                         source_backend_pid = NULL
                     WHERE source_id = $1
                     RETURNING source_id, source_epoch, fence_token, owner_id, lease_phase,
                         floor(extract(epoch FROM expires_at) * 1000)::bigint AS expires_at_unix_millis,
                         source_backend_pid"
                ),
                &[
                    &request.source_id.as_str(),
                    &token,
                    &request.owner_id.as_str(),
                    &ttl_millis,
                ],
            )
            .map_err(store_error)
            .and_then(snapshot_from_row)?;
        transaction.commit().map_err(store_error)?;
        Ok(snapshot)
    }

    fn renew(&self, lease: &LeaseIdentity, ttl: Duration) -> Result<LeaseSnapshot, FenceError> {
        validate_ttl(ttl)?;
        let ttl_millis = duration_millis(ttl)?;
        self.with_owned_lease(lease, |transaction, current| {
            let renewed = transaction
                .query_opt(
                    &format!(
                        "UPDATE {LEASE_TABLE}
                         SET expires_at = clock_timestamp() + ($5::bigint * interval '1 millisecond')
                         WHERE source_id = $1 AND source_epoch = $2 AND owner_id = $3 AND fence_token = $4
                           AND expires_at > clock_timestamp()
                         RETURNING source_id, source_epoch, fence_token, owner_id, lease_phase,
                             floor(extract(epoch FROM expires_at) * 1000)::bigint AS expires_at_unix_millis,
                             source_backend_pid"
                    ),
                    &[
                        &lease.source_id.as_str(),
                        &lease.source_epoch.as_str(),
                        &lease.owner_id.as_str(),
                        &lease.token.get(),
                        &ttl_millis,
                    ],
                )
                .map_err(store_error)?
                // The expiry read with the row lock was computed before any
                // wait for that lock, and a holder that only locked the row
                // leaves it unchecked afterwards. This one is read now.
                .ok_or(FenceError::LeaseLost)
                .and_then(snapshot_from_row)?;
            debug_assert_eq!(renewed.phase, current.phase);
            Ok(renewed)
        })
    }

    fn validate(&self, lease: &LeaseIdentity) -> Result<LeaseSnapshot, FenceError> {
        self.with_owned_lease(lease, |_transaction, current| Ok(current))
    }

    fn bind_backend(
        &self,
        lease: &LeaseIdentity,
        backend_pid: i32,
    ) -> Result<LeaseSnapshot, FenceError> {
        if backend_pid <= 0 {
            return Err(FenceError::InvalidBackendPid(backend_pid));
        }
        self.with_owned_lease(lease, |transaction, current| {
            if current.phase != LeasePhase::Provisional {
                return Err(FenceError::WrongLeasePhase {
                    expected: LeasePhase::Provisional,
                    actual: current.phase,
                });
            }
            transaction
                .query_one(
                    &format!(
                        "UPDATE {LEASE_TABLE} SET source_backend_pid = $5
                         WHERE source_id = $1 AND source_epoch = $2 AND owner_id = $3 AND fence_token = $4
                         RETURNING source_id, source_epoch, fence_token, owner_id, lease_phase,
                             floor(extract(epoch FROM expires_at) * 1000)::bigint AS expires_at_unix_millis,
                             source_backend_pid"
                    ),
                    &[
                        &lease.source_id.as_str(),
                        &lease.source_epoch.as_str(),
                        &lease.owner_id.as_str(),
                        &lease.token.get(),
                        &backend_pid,
                    ],
                )
                .map_err(store_error)
                .and_then(snapshot_from_row)
        })
    }

    fn claim_durable_log_writer(
        &self,
        lease: &LeaseIdentity,
    ) -> Result<DurableLogWriterClaim, FenceError> {
        let control_connection = self.control_connection.clone();
        self.with_owned_lease(lease, |transaction, current| {
            if current.phase != LeasePhase::Provisional {
                return Err(FenceError::WrongLeasePhase {
                    expected: LeasePhase::Provisional,
                    actual: current.phase,
                });
            }
            let claimed = transaction
                .query_opt(
                    "INSERT INTO calque_control.canonical_log_writer_fence
                        (source_id, source_epoch, fence_token)
                     VALUES ($1, $2, $3)
                     ON CONFLICT (source_id) DO UPDATE
                     SET source_epoch = EXCLUDED.source_epoch,
                         fence_token = EXCLUDED.fence_token
                     WHERE calque_control.canonical_log_writer_fence.source_epoch = EXCLUDED.source_epoch
                       AND calque_control.canonical_log_writer_fence.fence_token < EXCLUDED.fence_token
                     RETURNING source_id, source_epoch, fence_token",
                    &[
                        &lease.source_id.as_str(),
                        &lease.source_epoch.as_str(),
                        &lease.token.get(),
                    ],
                )
                .map_err(store_error)?;
            let row = claimed.ok_or(FenceError::DurableLogFenceRejected)?;
            let source_id: String = row.get(0);
            let source_epoch: String = row.get(1);
            let token: i64 = row.get(2);
            Ok(DurableLogWriterClaim {
                source_id: SourceId::new(source_id)
                    .map_err(|_| FenceError::InvalidStoredSource)?,
                source_epoch: SourceEpoch::new(source_epoch)
                    .map_err(|_| FenceError::InvalidStoredEpoch)?,
                token: FenceToken::from_database(token)?,
                datastore: self.datastore.clone(),
                control_connection,
            })
        })
    }

    fn load_slot_enrollment(
        &self,
        lease: &LeaseIdentity,
    ) -> Result<Option<SlotEnrollment>, FenceError> {
        self.with_owned_lease(lease, |transaction, _current| {
            transaction
                .query_opt(
                    "SELECT source_id, source_epoch, slot_name, publication_name,
                            system_identifier, database_name, timeline_id,
                            previous_timeline_id, pgoutput_profile, initial_lsn,
                            durable_canonical_lsn
                       FROM calque_control.source_slot_enrollment
                      WHERE source_id = $1",
                    &[&lease.source_id.as_str()],
                )
                .map_err(store_error)?
                .map(SlotEnrollment::from_control_row)
                .transpose()
        })
    }

    fn record_slot_enrollment(
        &self,
        lease: &LeaseIdentity,
        enrollment: &SlotEnrollment,
    ) -> Result<(), FenceError> {
        if enrollment.source_id != lease.source_id || enrollment.source_epoch != lease.source_epoch
        {
            return Err(FenceError::SlotEnrollmentMismatch);
        }
        self.with_owned_lease(lease, |transaction, current| {
            if current.phase != LeasePhase::Provisional {
                return Err(FenceError::WrongLeasePhase {
                    expected: LeasePhase::Provisional,
                    actual: current.phase,
                });
            }
            let timeline_id = i64::from(enrollment.timeline_id);
            let previous_timeline_id = i64::from(enrollment.previous_timeline_id);
            let affected = transaction
                .execute(
                    "INSERT INTO calque_control.source_slot_enrollment (
                        source_id, source_epoch, slot_name, publication_name,
                        system_identifier, database_name, timeline_id,
                        previous_timeline_id, pgoutput_profile, initial_lsn,
                        durable_canonical_lsn
                     ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $10)
                     ON CONFLICT (source_id) DO NOTHING",
                    &[
                        &enrollment.source_id.as_str(),
                        &enrollment.source_epoch.as_str(),
                        &enrollment.slot_name,
                        &enrollment.publication_name,
                        &enrollment.system_identifier,
                        &enrollment.database_name,
                        &timeline_id,
                        &previous_timeline_id,
                        &enrollment.pgoutput_profile,
                        &enrollment.initial_lsn_text,
                    ],
                )
                .map_err(store_error)?;
            if affected != 1 {
                return Err(FenceError::SlotLifecycleAlreadyInitialized);
            }
            Ok(())
        })
    }

    fn advance_durable_canonical_lsn(
        &self,
        lease: &LeaseIdentity,
        durable_lsn: u64,
    ) -> Result<(), FenceError> {
        self.with_owned_lease(lease, |transaction, current| {
            if current.phase != LeasePhase::Active {
                return Err(FenceError::WrongLeasePhase {
                    expected: LeasePhase::Active,
                    actual: current.phase,
                });
            }
            let durable_lsn = format!("{:X}/{:08X}", durable_lsn >> 32, durable_lsn & 0xffff_ffff);
            let affected = transaction
                .execute(
                    "UPDATE calque_control.source_slot_enrollment
                        SET durable_canonical_lsn = GREATEST(
                            durable_canonical_lsn::pg_lsn, $3::text::pg_lsn
                        )::text
                      WHERE source_id = $1 AND source_epoch = $2",
                    &[
                        &lease.source_id.as_str(),
                        &lease.source_epoch.as_str(),
                        &durable_lsn,
                    ],
                )
                .map_err(store_error)?;
            if affected != 1 {
                return Err(FenceError::SlotEnrollmentMismatch);
            }
            Ok(())
        })
    }

    fn transition_active(
        &self,
        lease: &LeaseIdentity,
        evidence: &ActivationEvidence,
    ) -> Result<LeaseSnapshot, FenceError> {
        self.with_owned_lease(lease, |transaction, current| {
            if current.phase != LeasePhase::Provisional {
                return Err(FenceError::WrongLeasePhase {
                    expected: LeasePhase::Provisional,
                    actual: current.phase,
                });
            }
            if current.source_backend_pid != Some(evidence.backend_pid)
                || evidence.fence_token != lease.token
            {
                return Err(FenceError::BackendNotBound);
            }
            transaction
                .query_one(
                    &format!(
                        "UPDATE {LEASE_TABLE} SET lease_phase = 'active'
                         WHERE source_id = $1 AND source_epoch = $2 AND owner_id = $3 AND fence_token = $4
                         RETURNING source_id, source_epoch, fence_token, owner_id, lease_phase,
                             floor(extract(epoch FROM expires_at) * 1000)::bigint AS expires_at_unix_millis,
                             source_backend_pid"
                    ),
                    &[
                        &lease.source_id.as_str(),
                        &lease.source_epoch.as_str(),
                        &lease.owner_id.as_str(),
                        &lease.token.get(),
                    ],
                )
                .map_err(store_error)
                .and_then(snapshot_from_row)
        })
    }

    fn release(&self, lease: &LeaseIdentity) -> Result<(), FenceError> {
        self.with_owned_lease(lease, |transaction, _current| {
            let affected = transaction
                .execute(
                    &format!(
                        "UPDATE {LEASE_TABLE}
                         SET owner_id = NULL, lease_phase = 'unowned', expires_at = NULL, source_backend_pid = NULL
                         WHERE source_id = $1 AND source_epoch = $2 AND owner_id = $3 AND fence_token = $4"
                    ),
                    &[
                        &lease.source_id.as_str(),
                        &lease.source_epoch.as_str(),
                        &lease.owner_id.as_str(),
                        &lease.token.get(),
                    ],
                )
                .map_err(store_error)?;
            if affected != 1 {
                return Err(FenceError::LeaseLost);
            }
            Ok(())
        })
    }
}

impl PostgresFenceStore {
    fn reacquire(
        &self,
        previous: &LeaseIdentity,
        ttl: Duration,
    ) -> Result<LeaseSnapshot, FenceError> {
        let ttl_millis = duration_millis(ttl)?;
        let mut client = self.client()?;
        let mut transaction = client.transaction().map_err(store_error)?;
        transaction
            .batch_execute("SET LOCAL synchronous_commit = on")
            .map_err(store_error)?;
        let row = load_source_for_update(&mut transaction, &previous.source_id)?;
        let stored_epoch: String = row.get("source_epoch");
        if stored_epoch != previous.source_epoch.as_str() {
            return Err(FenceError::EpochMismatch {
                expected: previous.source_epoch.clone(),
                actual: SourceEpoch::new(stored_epoch)
                    .map_err(|_| FenceError::InvalidStoredEpoch)?,
            });
        }
        let stored_token: i64 = row.get("fence_token");
        let stored_owner: Option<String> = row.get("owner_id");
        if stored_token != previous.token.get()
            && stored_owner.as_deref() != Some(previous.owner_id.as_str())
        {
            return Err(FenceError::LeaseSuperseded);
        }

        let token = next_token(&mut transaction)?;
        let snapshot = transaction
            .query_one(
                &format!(
                    "UPDATE {LEASE_TABLE}
                     SET fence_token = $2, owner_id = $3, lease_phase = 'provisional',
                         expires_at = clock_timestamp() + ($4::bigint * interval '1 millisecond'),
                         source_backend_pid = NULL
                     WHERE source_id = $1
                     RETURNING source_id, source_epoch, fence_token, owner_id, lease_phase,
                         floor(extract(epoch FROM expires_at) * 1000)::bigint AS expires_at_unix_millis,
                         source_backend_pid"
                ),
                &[
                    &previous.source_id.as_str(),
                    &token,
                    &previous.owner_id.as_str(),
                    &ttl_millis,
                ],
            )
            .map_err(store_error)
            .and_then(snapshot_from_row)?;
        transaction.commit().map_err(store_error)?;
        Ok(snapshot)
    }

    fn with_feedback_fence<T>(
        &self,
        lease: &LeaseIdentity,
        operation: impl FnOnce() -> T,
    ) -> Result<T, FenceError> {
        self.with_owned_lease(lease, |_transaction, current| {
            if current.phase != LeasePhase::Active || current.source_backend_pid.is_none() {
                return Err(FenceError::WrongLeasePhase {
                    expected: LeasePhase::Active,
                    actual: current.phase,
                });
            }
            Ok(operation())
        })
    }

    fn with_owned_lease<T>(
        &self,
        lease: &LeaseIdentity,
        operation: impl FnOnce(&mut Transaction<'_>, LeaseSnapshot) -> Result<T, FenceError>,
    ) -> Result<T, FenceError> {
        let mut client = self.client()?;
        let mut transaction = client.transaction().map_err(store_error)?;
        let row = load_source_for_update(&mut transaction, &lease.source_id)?;
        let current = owned_snapshot_from_row(row, lease)?;
        let result = operation(&mut transaction, current)?;
        transaction.commit().map_err(store_error)?;
        Ok(result)
    }
}

fn load_source_for_update(
    transaction: &mut Transaction<'_>,
    source_id: &SourceId,
) -> Result<postgres::Row, FenceError> {
    transaction
        .query_opt(
            &format!(
                "SELECT source_id, source_epoch, fence_token, owner_id, lease_phase,
                    CASE WHEN expires_at IS NULL THEN NULL
                         ELSE floor(extract(epoch FROM expires_at) * 1000)::bigint END AS expires_at_unix_millis,
                    source_backend_pid,
                    COALESCE(expires_at <= clock_timestamp(), false) AS expired
                 FROM {LEASE_TABLE} WHERE source_id = $1 FOR UPDATE"
            ),
            &[&source_id.as_str()],
        )
        .map_err(|error| {
            // Another transaction held the row for the whole `lock_timeout`.
            // Nothing was read or changed, so the caller may try again.
            if error.code() == Some(&SqlState::LOCK_NOT_AVAILABLE) {
                FenceError::LeaseRowLocked
            } else {
                store_error(error)
            }
        })?
        .ok_or_else(|| FenceError::SourceNotEnrolled(source_id.clone()))
}

fn owned_snapshot_from_row(
    row: postgres::Row,
    expected: &LeaseIdentity,
) -> Result<LeaseSnapshot, FenceError> {
    let expired: bool = row.get("expired");
    let stored_epoch: String = row.get("source_epoch");
    let stored_token: i64 = row.get("fence_token");
    let stored_owner: Option<String> = row.get("owner_id");
    if expired
        || stored_epoch != expected.source_epoch.as_str()
        || stored_token != expected.token.get()
        || stored_owner.as_deref() != Some(expected.owner_id.as_str())
    {
        return Err(FenceError::LeaseLost);
    }
    snapshot_from_row(row)
}

fn snapshot_from_row(row: postgres::Row) -> Result<LeaseSnapshot, FenceError> {
    let source_id: String = row.get("source_id");
    let source_epoch: String = row.get("source_epoch");
    let owner_id: Option<String> = row.get("owner_id");
    Ok(LeaseSnapshot {
        source_id: SourceId::new(source_id).map_err(|_| FenceError::InvalidStoredSource)?,
        source_epoch: SourceEpoch::new(source_epoch).map_err(|_| FenceError::InvalidStoredEpoch)?,
        owner_id: OwnerId::new(owner_id.ok_or(FenceError::LeaseLost)?)?,
        token: FenceToken::from_database(row.get("fence_token"))?,
        phase: LeasePhase::from_database(row.get("lease_phase"))?,
        expires_at_unix_millis: row
            .get::<_, Option<i64>>("expires_at_unix_millis")
            .ok_or(FenceError::LeaseLost)?,
        source_backend_pid: row.get("source_backend_pid"),
    })
}

fn next_token(transaction: &mut Transaction<'_>) -> Result<i64, FenceError> {
    transaction
        .query_one(&format!("SELECT nextval('{TOKEN_SEQUENCE}')::bigint"), &[])
        .map(|row| row.get(0))
        .map_err(|error| {
            if error
                .as_db_error()
                .is_some_and(|db_error| db_error.code().code() == "2200H")
            {
                FenceError::TokenExhausted
            } else {
                store_error(error)
            }
        })
}

fn install_schema(client: &mut Client) -> Result<(), FenceError> {
    let mut transaction = client.transaction().map_err(store_error)?;
    // PostgreSQL reports each object `IF NOT EXISTS` skips as a NOTICE,
    // which the driver logs at INFO on every start.
    transaction
        .batch_execute("SET LOCAL synchronous_commit = on; SET LOCAL client_min_messages = warning")
        .map_err(store_error)?;
    // A transaction-scoped advisory lock serializes concurrent first-use
    // `CREATE ... IF NOT EXISTS`. It shares `canonical-log`'s key because both
    // crates create the `calque_control` schema.
    transaction
        .query_one(
            "SELECT pg_advisory_xact_lock($1)",
            &[&CONTROL_SCHEMA_BOOTSTRAP_LOCK_KEY],
        )
        .map_err(store_error)?;
    transaction
        .batch_execute(INSTALL_SQL)
        .map_err(store_error)?;
    transaction.commit().map_err(store_error)
}

fn configure_control_connection(client: &mut Client) -> Result<(), FenceError> {
    client
        .batch_execute(&format!(
            "SET synchronous_commit = on; SET statement_timeout = '2s'; SET lock_timeout = {};",
            CONTROL_LOCK_TIMEOUT.as_millis()
        ))
        .map_err(store_error)
}

fn duration_millis(duration: Duration) -> Result<i64, FenceError> {
    i64::try_from(duration.as_millis()).map_err(|_| FenceError::InvalidLeaseTtl)
}

fn validate_ttl(ttl: Duration) -> Result<(), FenceError> {
    if !(MIN_LEASE_TTL..=MAX_LEASE_TTL).contains(&ttl) {
        return Err(FenceError::InvalidLeaseTtl);
    }
    Ok(())
}

fn store_error(error: postgres::Error) -> FenceError {
    // PostgreSQL error text may name source objects or contain SQL, so only
    // the SQLSTATE is read.
    match PostgresRefusal::of(&error) {
        Some(refusal) => FenceError::ControlStoreRefused(refusal),
        None => FenceError::ControlStoreUnavailable,
    }
}

/// A PostgreSQL refusal that no retry with the same role, credentials and
/// database can get past; only an administrator's change can.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Error)]
pub enum PostgresRefusal {
    /// SQLSTATE class 28: the role does not exist or may not log in, its
    /// password is wrong or expired, or `pg_hba.conf` rejects it.
    #[error("authentication failed")]
    AuthenticationFailed,
    /// SQLSTATE 42501: the role lacks a privilege the connection or a
    /// statement needs, such as CONNECT on the database.
    #[error("insufficient privilege")]
    InsufficientPrivilege,
    /// SQLSTATE 3D000.
    #[error("database does not exist")]
    DatabaseMissing,
    /// The role has neither the REPLICATION nor the SUPERUSER attribute, so
    /// PostgreSQL refuses it a walsender.
    #[error("replication privilege missing")]
    ReplicationPrivilegeMissing,
}

impl PostgresRefusal {
    /// Classifies a SQLSTATE, never PostgreSQL's message text, which follows
    /// the server's `lc_messages` and can name source objects.
    ///
    /// Every other code stays an outage the caller retries, as does an error
    /// with none, such as a refused or reset socket: a server shutting down,
    /// crashing or starting up (57P01, 57P02, 57P03), out of connection slots
    /// (53300), or a connection exception (class 08) recovers by itself, and
    /// a PostgreSQL restart must not stop the runtime.
    pub fn from_sqlstate(code: &SqlState) -> Option<Self> {
        match code.code() {
            "28000" | "28P01" => Some(Self::AuthenticationFailed),
            "42501" => Some(Self::InsufficientPrivilege),
            "3D000" => Some(Self::DatabaseMissing),
            _ => None,
        }
    }

    pub(crate) fn of(error: &postgres::Error) -> Option<Self> {
        error.code().and_then(Self::from_sqlstate)
    }
}

const fn store_error_unavailable() -> FenceError {
    FenceError::ControlStoreUnavailable
}

#[derive(Debug, Error)]
pub enum FenceError {
    #[error("owner ID is empty, too long, or contains unsupported characters")]
    InvalidOwnerId,
    #[error("lease TTL must be between 100 milliseconds and 300 seconds")]
    InvalidLeaseTtl,
    #[error("source backend PID must be positive, got {0}")]
    InvalidBackendPid(i32),
    #[error("source {0} is already enrolled")]
    SourceAlreadyEnrolled(SourceId),
    #[error("source {0} is not enrolled")]
    SourceNotEnrolled(SourceId),
    #[error("source epoch mismatch: expected {expected}, found {actual}")]
    EpochMismatch {
        expected: SourceEpoch,
        actual: SourceEpoch,
    },
    #[error("source lease is held until database timestamp {expires_at_unix_millis} ms")]
    LeaseHeld { expires_at_unix_millis: i64 },
    #[error("source lease was lost, expired, or superseded")]
    LeaseLost,
    #[error("the source lease row stayed locked for the control connection's whole lock timeout")]
    LeaseRowLocked,
    #[error("another owner has taken the source lease since this owner last held it")]
    LeaseSuperseded,
    #[error("expected lease phase {expected:?}, found {actual:?}")]
    WrongLeasePhase {
        expected: LeasePhase,
        actual: LeasePhase,
    },
    #[error("source backend is not bound")]
    BackendNotBound,
    #[error("activation evidence does not match the exact lease and durable-log claim")]
    ActivationEvidenceMismatch,
    #[error("durable canonical-log writer fence rejected the lease token or source epoch")]
    DurableLogFenceRejected,
    #[error("slot enrollment does not match the exact source and epoch lease")]
    SlotEnrollmentMismatch,
    #[error("persistent slot lifecycle was already initialized for this source")]
    SlotLifecycleAlreadyInitialized,
    #[error("fence token sequence is exhausted")]
    TokenExhausted,
    #[error("control store is unavailable or returned an uncertain result")]
    ControlStoreUnavailable,
    #[error("control store refused the connection or a statement: {0}")]
    ControlStoreRefused(PostgresRefusal),
    #[error("control store contains an invalid source identifier")]
    InvalidStoredSource,
    #[error("control store contains an invalid source epoch")]
    InvalidStoredEpoch,
    #[error("control store contains invalid fence token {0}")]
    InvalidStoredToken(i64),
    #[error("control store contains invalid lease phase {0}")]
    InvalidStoredPhase(String),
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn startup_shutdown_remembers_a_request_before_socket_publication() {
        let shutdown = SourceStartupShutdown::new();
        shutdown.request_shutdown().unwrap();
        let state = shutdown.state.lock().unwrap();
        assert!(state.requested);
        assert!(state.handle.is_none());
    }

    #[test]
    fn startup_shutdown_disarms_only_before_a_shutdown_request() {
        let disarmed = SourceStartupShutdown::new();
        assert!(disarmed.disarm().unwrap());

        let cancelled = SourceStartupShutdown::new();
        cancelled.request_shutdown().unwrap();
        assert!(!cancelled.disarm().unwrap());
    }

    #[test]
    fn owner_id_is_bounded_and_non_secret_safe() {
        assert!(OwnerId::new("runtime-a/01").is_ok());
        assert!(matches!(OwnerId::new(""), Err(FenceError::InvalidOwnerId)));
        assert!(matches!(
            OwnerId::new("contains a space"),
            Err(FenceError::InvalidOwnerId)
        ));
        assert!(matches!(
            OwnerId::new("a".repeat(MAX_OWNER_ID_BYTES + 1)),
            Err(FenceError::InvalidOwnerId)
        ));
    }

    #[test]
    fn lease_ttl_is_bounded() {
        assert!(validate_ttl(Duration::from_millis(100)).is_ok());
        assert!(validate_ttl(Duration::from_secs(300)).is_ok());
        assert!(matches!(
            validate_ttl(Duration::from_millis(99)),
            Err(FenceError::InvalidLeaseTtl)
        ));
        assert!(matches!(
            validate_ttl(Duration::from_secs(301)),
            Err(FenceError::InvalidLeaseTtl)
        ));
    }

    #[test]
    fn only_a_sqlstate_no_retry_can_heal_is_a_refusal() {
        let class = |code: &str| PostgresRefusal::from_sqlstate(&SqlState::from_code(code));
        assert_eq!(class("28000"), Some(PostgresRefusal::AuthenticationFailed));
        assert_eq!(class("28P01"), Some(PostgresRefusal::AuthenticationFailed));
        assert_eq!(class("42501"), Some(PostgresRefusal::InsufficientPrivilege));
        assert_eq!(class("3D000"), Some(PostgresRefusal::DatabaseMissing));
        // A restart, a crash, a full server, a lost or unopened connection,
        // the statement and lock timeouts the control connections set, a
        // serialization failure, a deadlock and an internal error.
        for outage in [
            "57P01", "57P02", "57P03", "53300", "08000", "08001", "08003", "08004", "08006",
            "57014", "55P03", "40001", "40P01", "XX000",
        ] {
            assert_eq!(class(outage), None, "{outage}");
        }
    }

    #[test]
    fn a_control_store_refusal_names_its_class_and_nothing_else() {
        assert_eq!(
            FenceError::ControlStoreRefused(PostgresRefusal::AuthenticationFailed).to_string(),
            "control store refused the connection or a statement: authentication failed"
        );
    }

    #[test]
    fn provisional_lease_can_be_revoked_locally() {
        let lease = ProvisionalLease {
            inner: Arc::new(ProvisionalLeaseInner {
                identity: LeaseIdentity {
                    source_id: SourceId::new("source-a").unwrap(),
                    source_epoch: SourceEpoch::new("epoch-a").unwrap(),
                    owner_id: OwnerId::new("owner-a").unwrap(),
                    token: FenceToken(1),
                },
                locally_valid: AtomicBool::new(true),
                ttl: Duration::from_secs(1),
                held_until: Mutex::new(Instant::now()),
            }),
        };
        assert!(lease.is_locally_valid());
        lease.revoke();
        assert!(!lease.is_locally_valid());
    }

    #[test]
    fn schema_names_are_static_and_qualified() {
        assert!(TOKEN_SEQUENCE.starts_with("calque_control."));
        assert!(LEASE_TABLE.starts_with("calque_control."));
    }

    const fn millis(value: u64) -> Duration {
        Duration::from_millis(value)
    }

    /// A control store whose renewals return scripted results. Each renewal
    /// records the test clock when it was sent, then advances it by
    /// `attempt`, as a renewal that waits out `lock_timeout` takes that long.
    /// Acquisition and renewal also take `real_delay` of real time, so a test
    /// can tell a time read before the store call from one read after it.
    ///
    /// With `expires_at` set the store also keeps the lease's expiry on the
    /// test clock, as PostgreSQL keeps it on its own: a renewal sent at or
    /// after it is refused as `LeaseLost` before the script is read, and a
    /// renewal the script lets through moves it one TTL on.
    struct ScriptedStore {
        now: Arc<Mutex<Instant>>,
        attempt: Duration,
        real_delay: Duration,
        renewals: Mutex<std::collections::VecDeque<Result<(), FenceError>>>,
        sent: Mutex<Vec<Instant>>,
        expires_at: Mutex<Option<Instant>>,
    }

    fn snapshot(identity: &LeaseIdentity, phase: LeasePhase) -> LeaseSnapshot {
        LeaseSnapshot {
            source_id: identity.source_id.clone(),
            source_epoch: identity.source_epoch.clone(),
            owner_id: identity.owner_id.clone(),
            token: identity.token,
            phase,
            expires_at_unix_millis: 0,
            source_backend_pid: Some(7_654),
        }
    }

    impl FenceStore for ScriptedStore {
        fn enroll_source(&self, _: &SourceId, _: &SourceEpoch) -> Result<(), FenceError> {
            unreachable!("not scripted")
        }

        fn acquire(&self, request: &AcquireRequest) -> Result<LeaseSnapshot, FenceError> {
            std::thread::sleep(self.real_delay);
            let identity = LeaseIdentity {
                source_id: request.source_id.clone(),
                source_epoch: request.source_epoch.clone(),
                owner_id: request.owner_id.clone(),
                token: FenceToken(1),
            };
            Ok(snapshot(&identity, LeasePhase::Provisional))
        }

        fn renew(&self, lease: &LeaseIdentity, ttl: Duration) -> Result<LeaseSnapshot, FenceError> {
            std::thread::sleep(self.real_delay);
            let mut now = self.now.lock().unwrap();
            let sent_at = *now;
            self.sent.lock().unwrap().push(sent_at);
            *now += self.attempt;
            let mut expires_at = self.expires_at.lock().unwrap();
            if expires_at.is_some_and(|expires_at| sent_at >= expires_at) {
                return Err(FenceError::LeaseLost);
            }
            let result = self
                .renewals
                .lock()
                .unwrap()
                .pop_front()
                .expect("a scripted renewal result");
            if result.is_ok() && expires_at.is_some() {
                *expires_at = Some(sent_at + ttl);
            }
            result.map(|()| snapshot(lease, LeasePhase::Active))
        }

        fn validate(&self, _: &LeaseIdentity) -> Result<LeaseSnapshot, FenceError> {
            unreachable!("not scripted")
        }

        fn bind_backend(&self, _: &LeaseIdentity, _: i32) -> Result<LeaseSnapshot, FenceError> {
            unreachable!("not scripted")
        }

        fn claim_durable_log_writer(
            &self,
            _: &LeaseIdentity,
        ) -> Result<DurableLogWriterClaim, FenceError> {
            unreachable!("not scripted")
        }

        fn load_slot_enrollment(
            &self,
            _: &LeaseIdentity,
        ) -> Result<Option<SlotEnrollment>, FenceError> {
            unreachable!("not scripted")
        }

        fn record_slot_enrollment(
            &self,
            _: &LeaseIdentity,
            _: &SlotEnrollment,
        ) -> Result<(), FenceError> {
            unreachable!("not scripted")
        }

        fn advance_durable_canonical_lsn(
            &self,
            _: &LeaseIdentity,
            _: u64,
        ) -> Result<(), FenceError> {
            unreachable!("not scripted")
        }

        fn transition_active(
            &self,
            lease: &LeaseIdentity,
            _: &ActivationEvidence,
        ) -> Result<LeaseSnapshot, FenceError> {
            Ok(snapshot(lease, LeasePhase::Active))
        }

        fn release(&self, _: &LeaseIdentity) -> Result<(), FenceError> {
            unreachable!("not scripted")
        }
    }

    fn scripted_authority(
        now: &Arc<Mutex<Instant>>,
        attempt: Duration,
        renewals: impl IntoIterator<Item = Result<(), FenceError>>,
    ) -> FenceAuthority<ScriptedStore> {
        FenceAuthority::new(ScriptedStore {
            now: Arc::clone(now),
            attempt,
            real_delay: Duration::ZERO,
            renewals: Mutex::new(renewals.into_iter().collect()),
            sent: Mutex::new(Vec::new()),
            expires_at: Mutex::new(None),
        })
    }

    /// An authority whose store lets the lease expire at `expires_at` on the
    /// test clock unless a renewal reaches it first. Each renewal takes
    /// `attempt`, and `renewals` of them are let through.
    fn expiring_authority(
        now: &Arc<Mutex<Instant>>,
        attempt: Duration,
        expires_at: Instant,
        renewals: usize,
    ) -> FenceAuthority<ScriptedStore> {
        let authority = scripted_authority(now, attempt, std::iter::repeat_n((), renewals).map(Ok));
        *authority.store.expires_at.lock().unwrap() = Some(expires_at);
        authority
    }

    fn sent(authority: &FenceAuthority<ScriptedStore>) -> Vec<Instant> {
        authority.store.sent.lock().unwrap().clone()
    }

    fn lease_identity() -> LeaseIdentity {
        LeaseIdentity {
            source_id: SourceId::new("source-a").unwrap(),
            source_epoch: SourceEpoch::new("epoch-a").unwrap(),
            owner_id: OwnerId::new("owner-a").unwrap(),
            token: FenceToken(1),
        }
    }

    fn active_lease(ttl: Duration, held_until: Instant) -> ActiveLease {
        ActiveLease {
            inner: Arc::new(ActiveLeaseInner {
                identity: lease_identity(),
                locally_valid: AtomicBool::new(true),
                ttl,
                held_until: Mutex::new(held_until),
            }),
        }
    }

    fn active_held_until(lease: &ActiveLease) -> Instant {
        *lease.inner.held_until.lock().unwrap()
    }

    /// Time that passes only through the store's attempts and the renewal's
    /// pauses.
    struct TestClock {
        now: Arc<Mutex<Instant>>,
        pauses: Vec<Duration>,
        /// The pause, counted from 1, during which the caller asks to stop.
        stop_at: Option<usize>,
        /// The pause, counted from 1, during which another thread revokes
        /// the lease.
        revoke_at: Option<(usize, ActiveLease)>,
    }

    impl TestClock {
        fn new(now: &Arc<Mutex<Instant>>) -> Self {
            Self {
                now: Arc::clone(now),
                pauses: Vec::new(),
                stop_at: None,
                revoke_at: None,
            }
        }
    }

    impl RenewalClock for TestClock {
        fn now(&self) -> Instant {
            *self.now.lock().unwrap()
        }

        fn pause(&mut self, pause: Duration) -> bool {
            self.pauses.push(pause);
            if let Some((at, lease)) = &self.revoke_at {
                if *at == self.pauses.len() {
                    lease.revoke();
                }
            }
            if self.stop_at == Some(self.pauses.len()) {
                return false;
            }
            *self.now.lock().unwrap() += pause;
            true
        }
    }

    #[test]
    fn an_active_renewal_retries_a_locked_lease_row_and_holds_the_lease_from_the_attempt_that_succeeded(
    ) {
        let start = Instant::now();
        let now = Arc::new(Mutex::new(start));
        let authority = scripted_authority(
            &now,
            CONTROL_LOCK_TIMEOUT,
            [
                Err(FenceError::LeaseRowLocked),
                Err(FenceError::LeaseRowLocked),
                Err(FenceError::LeaseRowLocked),
                Ok(()),
            ],
        );
        let lease = active_lease(Duration::from_secs(10), start + Duration::from_secs(10));
        let mut clock = TestClock::new(&now);

        assert!(matches!(
            authority.renew_active(&lease, &mut clock),
            Ok(ActiveRenewal::Renewed)
        ));
        assert_eq!(clock.pauses, [millis(50), millis(100), millis(200)]);
        assert_eq!(
            sent(&authority),
            [
                start,
                start + millis(1_050),
                start + millis(2_150),
                start + millis(3_350)
            ]
        );
        assert_eq!(
            active_held_until(&lease),
            start + millis(3_350) + Duration::from_secs(10)
        );
        assert!(lease.is_locally_valid());
    }

    #[test]
    fn an_active_renewal_revokes_while_the_lease_is_held_once_another_lock_wait_could_outlast_it() {
        let start = Instant::now();
        let held_until = start + Duration::from_secs(10);
        let now = Arc::new(Mutex::new(start));
        let authority = scripted_authority(
            &now,
            CONTROL_LOCK_TIMEOUT,
            std::iter::repeat_with(|| Err(FenceError::LeaseRowLocked)).take(20),
        );
        let lease = active_lease(Duration::from_secs(10), held_until);
        let mut clock = TestClock::new(&now);

        assert!(matches!(
            authority.renew_active(&lease, &mut clock),
            Err(FenceError::LeaseRowLocked)
        ));
        assert!(!lease.is_locally_valid());
        // The pause doubles to its bound and stays there.
        assert_eq!(
            clock.pauses,
            [
                millis(50),
                millis(100),
                millis(200),
                millis(400),
                millis(400),
                millis(400),
                millis(400)
            ]
        );
        assert_eq!(sent(&authority).len(), 8);
        // The last attempt ended before the lease could expire, and one more
        // pause and lock wait would have ended after it.
        assert_eq!(clock.now(), start + millis(9_950));
        assert!(clock.now() < held_until);
        assert_eq!(active_held_until(&lease), held_until);
    }

    #[test]
    fn an_active_renewal_retries_only_when_the_next_lock_wait_ends_strictly_before_expiry() {
        let first_wait_ends = millis(1_000) + millis(50) + CONTROL_LOCK_TIMEOUT;
        for (held_for, retried) in [
            (first_wait_ends, false),
            (first_wait_ends + Duration::from_nanos(1), true),
        ] {
            let start = Instant::now();
            let now = Arc::new(Mutex::new(start));
            let authority = scripted_authority(
                &now,
                CONTROL_LOCK_TIMEOUT,
                [Err(FenceError::LeaseRowLocked), Ok(())],
            );
            let lease = active_lease(Duration::from_secs(10), start + held_for);
            let mut clock = TestClock::new(&now);

            let renewal = authority.renew_active(&lease, &mut clock);
            if retried {
                assert!(matches!(renewal, Ok(ActiveRenewal::Renewed)));
                assert_eq!(clock.pauses, [millis(50)]);
                assert!(lease.is_locally_valid());
            } else {
                assert!(matches!(renewal, Err(FenceError::LeaseRowLocked)));
                assert!(clock.pauses.is_empty());
                assert!(!lease.is_locally_valid());
            }
        }
    }

    #[test]
    fn an_active_renewal_revokes_at_once_on_any_failure_but_a_locked_lease_row() {
        // Each script ends with a renewal that would succeed, so a retry of
        // the failure shows in the result rather than exhausting the script.
        let cases: [(Vec<Result<(), FenceError>>, usize); 3] = [
            // The row came free and named another owner: the lease is not ours.
            (
                vec![
                    Err(FenceError::LeaseRowLocked),
                    Err(FenceError::LeaseLost),
                    Ok(()),
                ],
                2,
            ),
            (vec![Err(FenceError::LeaseLost), Ok(())], 1),
            (vec![Err(FenceError::ControlStoreUnavailable), Ok(())], 1),
        ];
        for (renewals, attempts) in cases {
            let start = Instant::now();
            let now = Arc::new(Mutex::new(start));
            let expected = format!("{:?}", renewals[attempts - 1]);
            let authority = scripted_authority(&now, CONTROL_LOCK_TIMEOUT, renewals);
            let lease = active_lease(Duration::from_secs(10), start + Duration::from_secs(10));
            let mut clock = TestClock::new(&now);

            let renewal = authority.renew_active(&lease, &mut clock);
            assert_eq!(format!("{:?}", renewal.map(|_| ())), expected);
            assert_eq!(sent(&authority).len(), attempts);
            assert_eq!(clock.pauses.len(), attempts - 1);
            assert!(!lease.is_locally_valid());
        }
    }

    #[test]
    fn a_stop_during_a_retry_pause_ends_the_renewal_and_leaves_the_lease_to_the_caller() {
        let start = Instant::now();
        let now = Arc::new(Mutex::new(start));
        let authority = scripted_authority(
            &now,
            CONTROL_LOCK_TIMEOUT,
            [Err(FenceError::LeaseRowLocked), Ok(())],
        );
        let held_until = start + Duration::from_secs(10);
        let lease = active_lease(Duration::from_secs(10), held_until);
        let mut clock = TestClock::new(&now);
        clock.stop_at = Some(1);

        assert!(matches!(
            authority.renew_active(&lease, &mut clock),
            Ok(ActiveRenewal::Stopped)
        ));
        assert_eq!(sent(&authority), [start]);
        assert!(lease.is_locally_valid());
        assert_eq!(active_held_until(&lease), held_until);
    }

    #[test]
    fn an_active_renewal_sends_nothing_more_once_the_lease_is_revoked_locally() {
        let start = Instant::now();
        let now = Arc::new(Mutex::new(start));
        let authority = scripted_authority(&now, CONTROL_LOCK_TIMEOUT, [Ok(())]);
        let lease = active_lease(Duration::from_secs(10), start + Duration::from_secs(10));
        lease.revoke();
        assert!(matches!(
            authority.renew_active(&lease, &mut TestClock::new(&now)),
            Err(FenceError::LeaseLost)
        ));
        assert!(sent(&authority).is_empty());

        let authority = scripted_authority(
            &now,
            CONTROL_LOCK_TIMEOUT,
            [Err(FenceError::LeaseRowLocked), Ok(())],
        );
        let lease = active_lease(Duration::from_secs(10), start + Duration::from_secs(10));
        let mut clock = TestClock::new(&now);
        clock.revoke_at = Some((1, lease.clone()));
        assert!(matches!(
            authority.renew_active(&lease, &mut clock),
            Err(FenceError::LeaseLost)
        ));
        assert_eq!(sent(&authority).len(), 1);
    }

    #[test]
    fn a_renewal_is_due_one_interval_after_the_request_that_set_the_expiry() {
        let ttl = Duration::from_secs(3);
        let sent_at = Instant::now();
        let held_until = sent_at + ttl;
        for (elapsed, wait) in [
            (millis(0), millis(1_000)),
            (millis(400), millis(600)),
            (millis(999), millis(1)),
            (millis(1_000), millis(0)),
            // Less than an interval is left, and then the lease has expired.
            (millis(2_500), millis(0)),
            (millis(3_500), millis(0)),
        ] {
            assert_eq!(
                renewal_wait(ttl, held_until, sent_at + elapsed),
                wait,
                "{elapsed:?}"
            );
        }
    }

    const LOOP_TTL: Duration = Duration::from_secs(3);
    /// How long each renewal of a loop test takes in the store.
    const LOOP_ATTEMPT: Duration = millis(100);
    /// How long before a renewal loop starts the lease's expiry was last set,
    /// and how long after its start the loop sends its first two renewals.
    /// The first lease has less than a renewal interval left, as one does
    /// that its holder kept through a wait before handing it to the runtime.
    const LOOP_SCHEDULE: [(Duration, [Duration; 2]); 3] = [
        (millis(2_500), [millis(0), millis(1_000)]),
        (millis(400), [millis(600), millis(1_600)]),
        (millis(0), [millis(1_000), millis(2_000)]),
    ];

    fn provisional_lease_held_until(held_until: Instant) -> ProvisionalLease {
        ProvisionalLease {
            inner: Arc::new(ProvisionalLeaseInner {
                identity: lease_identity(),
                locally_valid: AtomicBool::new(true),
                ttl: LOOP_TTL,
                held_until: Mutex::new(held_until),
            }),
        }
    }

    #[test]
    fn an_active_lease_is_renewed_an_interval_after_its_expiry_was_set_however_late_the_loop_starts(
    ) {
        for (age, sent_after) in LOOP_SCHEDULE {
            let start = Instant::now();
            let now = Arc::new(Mutex::new(start));
            let held_until = start + LOOP_TTL - age;
            let authority = expiring_authority(&now, LOOP_ATTEMPT, held_until, 2);
            let lease = active_lease(LOOP_TTL, held_until);
            let mut clock = TestClock::new(&now);
            clock.stop_at = Some(3);

            authority.keep_active_renewed(&lease, &mut clock).unwrap();
            assert_eq!(
                sent(&authority),
                sent_after.map(|after| start + after),
                "{age:?}"
            );
            assert!(lease.is_locally_valid(), "{age:?}");
            assert_eq!(
                active_held_until(&lease),
                start + sent_after[1] + LOOP_TTL,
                "{age:?}"
            );
        }
    }

    #[test]
    fn a_provisional_lease_is_renewed_an_interval_after_its_expiry_was_set_however_late_the_loop_starts(
    ) {
        for (age, sent_after) in LOOP_SCHEDULE {
            let start = Instant::now();
            let now = Arc::new(Mutex::new(start));
            let held_until = start + LOOP_TTL - age;
            let authority = expiring_authority(&now, LOOP_ATTEMPT, held_until, 2);
            let lease = provisional_lease_held_until(held_until);
            let mut clock = TestClock::new(&now);
            clock.stop_at = Some(3);

            authority.keep_provisional_renewed(&lease, &mut clock);
            assert_eq!(
                sent(&authority),
                sent_after.map(|after| start + after),
                "{age:?}"
            );
            assert!(lease.is_locally_valid(), "{age:?}");
            assert_eq!(
                *lease.inner.held_until.lock().unwrap(),
                start + sent_after[1] + LOOP_TTL,
                "{age:?}"
            );
        }
    }

    #[test]
    fn a_renewal_the_store_refuses_ends_either_loop_with_the_lease_revoked() {
        // The lease expired in the store as each loop starts.
        let start = Instant::now();
        let now = Arc::new(Mutex::new(start));

        let authority = expiring_authority(&now, LOOP_ATTEMPT, start, 1);
        let active = active_lease(LOOP_TTL, start);
        let mut clock = TestClock::new(&now);
        clock.stop_at = Some(2);
        assert!(matches!(
            authority.keep_active_renewed(&active, &mut clock),
            Err(FenceError::LeaseLost)
        ));
        assert_eq!(clock.pauses, [Duration::ZERO]);
        assert_eq!(sent(&authority), [start]);
        assert!(!active.is_locally_valid());

        let start = *now.lock().unwrap();
        let authority = expiring_authority(&now, LOOP_ATTEMPT, start, 1);
        let provisional = provisional_lease_held_until(start);
        let mut clock = TestClock::new(&now);
        clock.stop_at = Some(2);
        authority.keep_provisional_renewed(&provisional, &mut clock);
        assert_eq!(clock.pauses, [Duration::ZERO]);
        assert_eq!(sent(&authority), [start]);
        assert!(!provisional.is_locally_valid());
    }

    #[test]
    fn a_lease_is_held_from_before_each_request_that_set_its_expiry_and_keeps_that_through_activation(
    ) {
        let real_delay = millis(20);
        let ttl = Duration::from_secs(10);
        let authority = FenceAuthority::new(ScriptedStore {
            now: Arc::new(Mutex::new(Instant::now())),
            attempt: Duration::ZERO,
            real_delay,
            renewals: Mutex::new([Ok(())].into_iter().collect()),
            sent: Mutex::new(Vec::new()),
            expires_at: Mutex::new(None),
        });
        let provisional_held_until =
            |lease: &ProvisionalLease| *lease.inner.held_until.lock().unwrap();

        let before = Instant::now();
        let provisional = authority
            .acquire(AcquireRequest {
                source_id: SourceId::new("source-a").unwrap(),
                source_epoch: SourceEpoch::new("epoch-a").unwrap(),
                owner_id: OwnerId::new("owner-a").unwrap(),
                ttl,
            })
            .unwrap();
        let acquired = provisional_held_until(&provisional);
        assert!(acquired >= before + ttl && acquired < before + ttl + real_delay);

        let before = Instant::now();
        authority.renew(&provisional, ttl).unwrap();
        let renewed = provisional_held_until(&provisional);
        assert!(renewed >= before + ttl && renewed < before + ttl + real_delay);

        let evidence = ActivationEvidence {
            backend_pid: 7_654,
            fence_token: provisional.identity().token(),
        };
        let active = authority.activate(provisional, &evidence).unwrap();
        assert_eq!(active_held_until(&active), renewed);
    }

    #[test]
    #[ignore = "requires CALQUE_CONTROL_DATABASE_URL"]
    fn live_control_store_forces_synchronous_commit_and_its_lock_timeout() {
        let url =
            std::env::var("CALQUE_CONTROL_DATABASE_URL").expect("live control-store database URL");
        // A connection string can set both before the store ever runs a
        // statement, so the store has to set them itself.
        let separator = if url.contains('?') { '&' } else { '?' };
        let url = format!(
            "{url}{separator}options=-c%20synchronous_commit%3Doff%20-c%20lock_timeout%3D0"
        );
        let before: String = Client::connect(&url, postgres::NoTls)
            .unwrap()
            .query_one("SHOW synchronous_commit", &[])
            .unwrap()
            .get(0);
        assert_eq!(before, "off");

        let store = PostgresFenceStore::connect(&url).unwrap();
        let mut configured = store.client().unwrap();
        let after: String = configured
            .query_one("SHOW synchronous_commit", &[])
            .unwrap()
            .get(0);
        assert_eq!(after, "on");
        // The active renewal's retry bound assumes this wait.
        let lock_timeout_millis: String = configured
            .query_one(
                "SELECT setting FROM pg_catalog.pg_settings WHERE name = 'lock_timeout'",
                &[],
            )
            .unwrap()
            .get(0);
        assert_eq!(
            lock_timeout_millis,
            CONTROL_LOCK_TIMEOUT.as_millis().to_string()
        );
    }

    #[test]
    #[ignore = "requires CALQUE_CONTROL_DATABASE_URL"]
    fn live_active_transition_requires_bound_backend_and_exact_evidence() {
        let url =
            std::env::var("CALQUE_CONTROL_DATABASE_URL").expect("live control-store database URL");
        let source_id = SourceId::new(format!(
            "fence-activation-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ))
        .unwrap();
        let epoch = SourceEpoch::new("epoch-1").unwrap();
        let store = PostgresFenceStore::connect(&url).unwrap();
        store.enroll_source(&source_id, &epoch).unwrap();
        let snapshot = store
            .acquire(&AcquireRequest {
                source_id,
                source_epoch: epoch,
                owner_id: OwnerId::new("runtime-a").unwrap(),
                ttl: Duration::from_secs(1),
            })
            .unwrap();
        let identity = snapshot.identity();
        let evidence = ActivationEvidence {
            backend_pid: 7_654,
            fence_token: identity.token(),
        };
        assert!(matches!(
            store.transition_active(&identity, &evidence),
            Err(FenceError::BackendNotBound)
        ));
        store.bind_backend(&identity, 7_654).unwrap();
        let wrong_evidence = ActivationEvidence {
            backend_pid: 7_654,
            fence_token: FenceToken(identity.token().get() + 1),
        };
        assert!(matches!(
            store.transition_active(&identity, &wrong_evidence),
            Err(FenceError::BackendNotBound)
        ));
        let active = store.transition_active(&identity, &evidence).unwrap();
        assert_eq!(active.phase, LeasePhase::Active);
        assert_eq!(active.source_backend_pid, Some(7_654));
        store.release(&identity).unwrap();
    }

    #[test]
    #[ignore = "requires CALQUE_CONTROL_DATABASE_URL"]
    fn live_feedback_fence_holds_takeover_until_operation_returns() {
        let url =
            std::env::var("CALQUE_CONTROL_DATABASE_URL").expect("live control-store database URL");
        let source_id = SourceId::new(format!(
            "feedback-fence-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ))
        .unwrap();
        let epoch = SourceEpoch::new("epoch-1").unwrap();
        let store = PostgresFenceStore::connect(&url).unwrap();
        store.enroll_source(&source_id, &epoch).unwrap();
        let authority_a = Arc::new(FenceAuthority::new(store));
        let provisional = authority_a
            .acquire(AcquireRequest {
                source_id: source_id.clone(),
                source_epoch: epoch.clone(),
                owner_id: OwnerId::new("runtime-a").unwrap(),
                ttl: Duration::from_millis(100),
            })
            .unwrap();
        authority_a.bind_backend(&provisional, 7_654).unwrap();
        let claim = authority_a.claim_durable_log_writer(&provisional).unwrap();
        let evidence =
            ActivationEvidence::from_exclusion(provisional.identity(), 7_654, &claim).unwrap();
        let active = authority_a.activate(provisional, &evidence).unwrap();

        let (entered_tx, entered_rx) = std::sync::mpsc::channel();
        let (release_tx, release_rx) = std::sync::mpsc::channel();
        let guard_authority = Arc::clone(&authority_a);
        let guard_lease = active.clone();
        let guard = std::thread::spawn(move || {
            guard_authority
                .with_active_feedback_fence(&guard_lease, || {
                    entered_tx.send(()).unwrap();
                    release_rx.recv().unwrap();
                })
                .unwrap();
        });
        entered_rx.recv().unwrap();
        std::thread::sleep(Duration::from_millis(140));

        let authority_b = Arc::new(FenceAuthority::new(
            PostgresFenceStore::connect(&url).unwrap(),
        ));
        let (takeover_tx, takeover_rx) = std::sync::mpsc::channel();
        let takeover_authority = Arc::clone(&authority_b);
        let takeover_source = source_id.clone();
        let takeover_epoch = epoch.clone();
        let takeover = std::thread::spawn(move || {
            let result = takeover_authority.acquire(AcquireRequest {
                source_id: takeover_source,
                source_epoch: takeover_epoch,
                owner_id: OwnerId::new("runtime-b").unwrap(),
                ttl: Duration::from_secs(1),
            });
            takeover_tx.send(result).unwrap();
        });
        assert!(
            takeover_rx
                .recv_timeout(Duration::from_millis(100))
                .is_err(),
            "takeover must remain blocked while feedback owns the lease-row lock"
        );
        release_tx.send(()).unwrap();
        guard.join().unwrap();
        let replacement = takeover_rx
            .recv_timeout(Duration::from_secs(1))
            .unwrap()
            .unwrap();
        takeover.join().unwrap();
        assert!(replacement.identity().token() > active.identity().token());
        authority_b.release(&replacement).unwrap();
    }
}
