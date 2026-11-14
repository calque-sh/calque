use std::{
    collections::{BTreeMap, BTreeSet},
    panic::{catch_unwind, AssertUnwindSafe},
    path::{Path, PathBuf},
    sync::{
        atomic::{AtomicBool, Ordering},
        mpsc, Arc, Mutex as StdMutex, PoisonError,
    },
    thread,
    time::{Duration, Instant},
};

use authz::{AuthorizationPolicy, AuthorizedScope, PolicyDefinition, TablePermitDefinition};
use canonical_log::{
    FencedWriterIdentity, LogError, PostgresCanonicalLogReader, PostgresDatastoreIdentity,
    PostgresFencedCanonicalLog,
};
use cdc_ingest::{AppendFailure, FenceError as IngestFenceError, IngestError};
use postgres::{error::SqlState, Client};
use source_control::quote_identifier;
use source_control::{
    inspect_source, ActivatedSource, EnrollmentRequirements, EnrollmentSnapshot,
    PgoutputLaunchProfile, QualifiedTable, SchemaRegistry, SourceIdentity, TableInspection,
};
use source_runtime::{
    establish_persistent_source, establish_persistent_source_interruptible,
    inspect_owned_publication, provision_persistent_slot, AcquireRequest, ActiveSourceSession,
    FenceAuthority, FenceError, FenceStore, LeaseIdentity, OwnedPublication, OwnerId,
    PersistentIngestBoundary, PersistentIngestConfig, PersistentIngestDisposition,
    PersistentIngestError, PersistentIngestObserver, PostgresFenceStore, PostgresRefusal,
    SlotProvisioningConfig, SourceRuntimeError, SourceStartupShutdown,
    DEFAULT_OPEN_TRANSACTION_WAIT,
};
use sync_types::{CursorCodec, CursorPosition};
use thiserror::Error;
use tokio::sync::Mutex;

use crate::{
    guard::PostgresConnectionFamily, log_readers::SourceLogReaders, slot_lag::SlotLagSampler,
    DatabaseUrl, LocalSecrets, LocalState, OperationErrorClass, OperationsMetrics,
};

const DEFAULT_LEASE_TTL: Duration = Duration::from_secs(10);
const DEFAULT_INGEST_TIMEOUT: Duration = Duration::from_millis(500);
const DEFAULT_MAX_TRANSACTIONS: usize = 128;
/// The longest a source transaction may wait between its commit and the send
/// of its commit to Calque while the source still reads as current.
const CURRENT_SEND_DELAY_BOUND: Duration = Duration::from_secs(1);
// First ingest may initialize the canonical and receipt connections before its
// bounded replication read. DatabaseUrl gives each connection five seconds,
// and the adapters give setup statements two seconds. Creating the slot at
// first enrollment may spend up to 15 s of the total waiting for open
// transactions (source-runtime's `DEFAULT_OPEN_TRANSACTION_WAIT`). Thirty
// seconds, on top of any wait for a predecessor's lease, bounds the caller
// while leaving cleanup to the session-owning thread on timeout.
const CDC_STARTUP_TIMEOUT: Duration = Duration::from_secs(30);
// When slot creation gives up, its error names the transactions it waited
// for. It reaches the caller before this deadline does only while every other
// step before it fits in what is left, so that wait keeps to half at most.
const _: () = assert!(DEFAULT_OPEN_TRANSACTION_WAIT.as_secs() * 2 <= CDC_STARTUP_TIMEOUT.as_secs());
// A process that stopped without releasing its lease -- killed, or out of stop
// grace before its shutdown finished -- leaves the row held until it expires,
// at most one lease TTL after its last renewal. Startup asks again this often,
// for one TTL plus the margin, before refusing the source as held.
const STARTUP_LEASE_RETRY: Duration = Duration::from_millis(250);
const STARTUP_LEASE_MARGIN: Duration = Duration::from_secs(1);
// Backoff between attempts to re-establish a replication session after a
// transient failure. A terminated walsender leaves PostgreSQL up, so the first
// attempt comes within 100 ms. Doubling fits six attempts into the first 6.3 s
// at most, which spans a routine PostgreSQL restart, before reaching the cap.
// The cap bounds how long a recovered server waits for the runtime to notice
// it, and keeps a long outage at one connection attempt every 2.5 to 5 s. Each
// delay is drawn from the upper half of its step, so several servers sharing
// one PostgreSQL spread out and no attempt comes sooner than half its step.
const RECONNECT_INITIAL_DELAY: Duration = Duration::from_millis(100);
const RECONNECT_MAX_DELAY: Duration = Duration::from_secs(5);

/// The subject the development shared bearer authenticates as.
pub const LOCAL_DEVELOPMENT_SUBJECT: &str = "calque";

#[derive(Clone)]
pub struct PreparedLocalSource {
    pub registry: SchemaRegistry,
    pub activated: ActivatedSource,
    pub policy: AuthorizationPolicy,
    pub scope: AuthorizedScope,
}

pub fn inspect_local_identity(
    database_url: &DatabaseUrl,
) -> Result<SourceIdentity, LocalSourceError> {
    inspect_local_identity_unexplained(database_url)
        .map_err(|error| SourceContext::of(database_url, None).explained(error))
}

fn inspect_local_identity_unexplained(
    database_url: &DatabaseUrl,
) -> Result<SourceIdentity, LocalSourceError> {
    let mut client = connect_owned(
        database_url,
        PostgresConnectionFamily::Inspect,
        "calque-inspect",
    )?;
    inspect_identity(&mut client)
}

pub fn inspect_local_source(
    database_url: &DatabaseUrl,
    publication: &str,
    tables: &BTreeSet<QualifiedTable>,
) -> Result<EnrollmentSnapshot, LocalSourceError> {
    inspect_local_source_unexplained(database_url, publication, tables)
        .map_err(|error| SourceContext::of(database_url, None).explained(error))
}

fn inspect_local_source_unexplained(
    database_url: &DatabaseUrl,
    publication: &str,
    tables: &BTreeSet<QualifiedTable>,
) -> Result<EnrollmentSnapshot, LocalSourceError> {
    let mut client = connect_owned(
        database_url,
        PostgresConnectionFamily::Inspect,
        "calque-inspect",
    )?;
    Ok(inspect_source(&mut client, publication, tables)?)
}

fn inspect_identity(client: &mut Client) -> Result<SourceIdentity, LocalSourceError> {
    let row = client.query_one(
        "SELECT control.system_identifier::text, current_database(),
                checkpoint.timeline_id::bigint, checkpoint.prev_timeline_id::bigint
           FROM pg_catalog.pg_control_system() AS control
          CROSS JOIN pg_catalog.pg_control_checkpoint() AS checkpoint",
        &[],
    )?;
    let timeline_id: i64 = row.get(2);
    let previous_timeline_id: i64 = row.get(3);
    Ok(SourceIdentity {
        system_identifier: row.get(0),
        database_name: row.get(1),
        timeline_id: u32::try_from(timeline_id).map_err(|_| LocalSourceError::InvalidIdentity)?,
        previous_timeline_id: u32::try_from(previous_timeline_id)
            .map_err(|_| LocalSourceError::InvalidIdentity)?,
    })
}

/// The columns `calque serve`'s manifest selects of each table.
pub type SelectedColumns = BTreeMap<QualifiedTable, BTreeSet<String>>;

/// Prepares a source from its publication as it stands, creating it whole
/// when it is missing.
pub fn prepare_local_source(
    database_url: &DatabaseUrl,
    state: &LocalState,
    secrets: &LocalSecrets,
    persisted_registry: Option<SchemaRegistry>,
) -> Result<PreparedLocalSource, LocalSourceError> {
    prepare_local_source_unexplained(database_url, state, secrets, persisted_registry, None)
        .map_err(|error| SourceContext::of(database_url, None).explained(error))
}

/// Prepares a source whose publication publishes exactly `columns` of each
/// table, creating it with those column lists when it is missing.
pub fn prepare_selected_local_source(
    database_url: &DatabaseUrl,
    state: &LocalState,
    secrets: &LocalSecrets,
    persisted_registry: Option<SchemaRegistry>,
    columns: &SelectedColumns,
) -> Result<PreparedLocalSource, LocalSourceError> {
    prepare_local_source_unexplained(
        database_url,
        state,
        secrets,
        persisted_registry,
        Some(columns),
    )
    .map_err(|error| SourceContext::of(database_url, None).explained(error))
}

fn prepare_local_source_unexplained(
    database_url: &DatabaseUrl,
    state: &LocalState,
    secrets: &LocalSecrets,
    persisted_registry: Option<SchemaRegistry>,
    columns: Option<&SelectedColumns>,
) -> Result<PreparedLocalSource, LocalSourceError> {
    let mut client = connect_owned(
        database_url,
        PostgresConnectionFamily::Enroll,
        "calque-enroll",
    )?;
    if inspect_identity(&mut client)? != state.source_identity {
        return Err(LocalSourceError::SourceIdentityChanged);
    }
    if slot_wal_unbounded(&mut client)? {
        tracing::warn!("{UNBOUNDED_SLOT_WAL_WARNING}");
    }
    ensure_owned_publication(
        database_url,
        &mut client,
        &state.names.publication_name,
        &state.tables,
        columns,
    )?;

    let tables = state.tables.iter().cloned().collect::<BTreeSet<_>>();
    let snapshot = inspect_source(&mut client, &state.names.publication_name, &tables)?;
    if snapshot.identity != state.source_identity {
        return Err(LocalSourceError::SourceIdentityChanged);
    }
    let requirements = EnrollmentRequirements::new(
        state.source_identity.clone(),
        state.names.publication_name.clone(),
        tables,
    );
    let registry = match persisted_registry {
        Some(registry) => {
            if column_list_changed(&registry, &snapshot) {
                return Err(LocalSourceError::PublicationDrift {
                    publication: state.names.publication_name.clone(),
                });
            }
            registry.verify_live(&mut client)?;
            if registry.source_identity() != &state.source_identity
                || registry.publication().name != state.names.publication_name
            {
                return Err(LocalSourceError::RegistryStateMismatch);
            }
            registry
        }
        None => SchemaRegistry::initialize(&snapshot, &requirements)?,
    };
    let policy = source_wide_policy(&registry)?;
    let scope = policy.scope();
    let activated = ActivatedSource::new(
        &registry,
        state.source_epoch.clone(),
        CursorCodec::new(*secrets.cursor_secret.as_bytes())?,
        PgoutputLaunchProfile::V1,
    );
    Ok(PreparedLocalSource {
        registry,
        activated,
        policy,
        scope,
    })
}

/// Whether the publication's column list on an enrolled table changed since
/// `registry` was enrolled while the table kept its columns. A start reports
/// that as drift of the publication, which names its remedy, rather than as
/// drift of the schema.
fn column_list_changed(registry: &SchemaRegistry, live: &EnrollmentSnapshot) -> bool {
    fn names(table: &TableInspection) -> (BTreeSet<&str>, BTreeSet<&str>) {
        let listed = table
            .columns
            .iter()
            .map(|column| column.name.as_str())
            .collect::<BTreeSet<_>>();
        let all = table
            .unlisted_columns
            .iter()
            .map(|column| column.name.as_str())
            .chain(listed.iter().copied())
            .collect();
        (listed, all)
    }
    registry.tables().iter().any(|registered| {
        let enrolled = &registered.inspection;
        live.tables
            .iter()
            .find(|table| table.table == enrolled.table)
            .is_some_and(|table| {
                let (enrolled_listed, enrolled_all) = names(enrolled);
                let (live_listed, live_all) = names(table);
                enrolled_all == live_all
                    && (enrolled_listed != live_listed
                        || enrolled.has_publication_column_list
                            != table.has_publication_column_list)
            })
    })
}

/// What [`destroy_local_source`] left on the source.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Destroyed {
    /// The publication, when another role owns it. Calque drops only a
    /// publication its own role owns, so an operator's stays as it was.
    pub kept_publication: Option<String>,
}

pub fn destroy_local_source(
    database_url: &DatabaseUrl,
    state: &LocalState,
) -> Result<Destroyed, LocalSourceError> {
    destroy_local_source_unexplained(database_url, state)
        .map_err(|error| SourceContext::of(database_url, None).explained(error))
}

fn destroy_local_source_unexplained(
    database_url: &DatabaseUrl,
    state: &LocalState,
) -> Result<Destroyed, LocalSourceError> {
    let mut client = connect_owned(
        database_url,
        PostgresConnectionFamily::Destroy,
        "calque-destroy",
    )?;
    if inspect_identity(&mut client)? != state.source_identity {
        return Err(LocalSourceError::SourceIdentityChanged);
    }

    let publication_owned: Option<bool> = client
        .query_opt(
            "SELECT pg_catalog.pg_has_role(current_user, pubowner, 'USAGE')
               FROM pg_catalog.pg_publication WHERE pubname = $1",
            &[&state.names.publication_name],
        )?
        .map(|row| row.get(0));
    let drop_publication = publication_owned == Some(true);
    if drop_publication {
        validate_owned_publication(&mut client, &state.names.publication_name, &state.tables)?;
    }

    let database_name: String = client.query_one("SELECT current_database()", &[])?.get(0);
    if let Some(slot) = client.query_opt(
        "SELECT plugin, database, active_pid FROM pg_catalog.pg_replication_slots
          WHERE slot_name = $1",
        &[&state.names.slot_name],
    )? {
        let plugin: String = slot.get(0);
        let database: Option<String> = slot.get(1);
        let active_pid: Option<i32> = slot.get(2);
        if plugin != "pgoutput" || database.as_deref() != Some(database_name.as_str()) {
            return Err(LocalSourceError::SlotOwnershipMismatch);
        }
        if active_pid.is_some() {
            return Err(LocalSourceError::ActiveRuntime);
        }
    }
    if relation_exists(&mut client, "calque_control.source_fence_lease")? {
        if let Some(row) = client.query_opt(
            "SELECT lease_phase,
                    expires_at IS NULL OR expires_at <= clock_timestamp()
               FROM calque_control.source_fence_lease WHERE source_id = $1",
            &[&state.names.source_id.as_str()],
        )? {
            let phase: String = row.get(0);
            let expired_or_unset: bool = row.get(1);
            if phase != "unowned" && !expired_or_unset {
                return Err(LocalSourceError::ActiveRuntime);
            }
        }
    }

    if client
        .query_opt(
            "SELECT 1 FROM pg_catalog.pg_replication_slots WHERE slot_name = $1",
            &[&state.names.slot_name],
        )?
        .is_some()
    {
        client.query_one(
            "SELECT pg_drop_replication_slot($1)",
            &[&state.names.slot_name],
        )?;
    }

    let mut transaction = client.transaction()?;
    if drop_publication {
        transaction.batch_execute(&format!(
            "DROP PUBLICATION {}",
            quote_identifier(&state.names.publication_name)
        ))?;
    }
    for table in [
        "calque_control.canonical_log_plan_retention_lease",
        "calque_control.canonical_log_build_retention_lease",
        "calque_control.canonical_log_record",
        "calque_control.canonical_log_retired_record",
        "calque_control.canonical_log_head",
        "calque_control.canonical_log_retention",
        "calque_internal.mutation_receipts",
        "calque_control.source_slot_enrollment",
        "calque_control.canonical_log_writer_fence",
        "calque_control.source_fence_lease",
    ] {
        if relation_exists(&mut transaction, table)? {
            transaction.execute(
                &format!("DELETE FROM {table} WHERE source_id = $1"),
                &[&state.names.source_id.as_str()],
            )?;
        }
    }
    transaction.commit()?;
    Ok(Destroyed {
        kept_publication: (publication_owned == Some(false))
            .then(|| state.names.publication_name.clone()),
    })
}

fn relation_exists(
    client: &mut impl postgres::GenericClient,
    relation: &str,
) -> Result<bool, LocalSourceError> {
    let relation: Option<String> = client
        .query_one("SELECT pg_catalog.to_regclass($1)::text", &[&relation])?
        .get(0);
    Ok(relation.is_some())
}

fn source_wide_policy(registry: &SchemaRegistry) -> Result<AuthorizationPolicy, LocalSourceError> {
    Ok(AuthorizationPolicy::compile_for_registry(
        PolicyDefinition {
            generation: 1,
            tables: registry
                .tables()
                .iter()
                .map(|table| TablePermitDefinition {
                    table_id: table.table_id.clone(),
                    permitted_columns: table.column_ids.clone(),
                })
                .collect(),
        },
        registry,
    )?)
}

pub struct LocalSourceRuntime {
    provider: Arc<Mutex<PostgresFencedCanonicalLog>>,
    barrier_provider: Arc<StdMutex<PostgresFencedCanonicalLog>>,
    log_readers: SourceLogReaders,
    healthy: Arc<AtomicBool>,
    current: Arc<AtomicBool>,
    connected: Arc<AtomicBool>,
    failure: Arc<StdMutex<Option<HealthFailure>>>,
    stop: mpsc::Sender<()>,
    join: Option<thread::JoinHandle<Result<(), LocalSourceError>>>,
    _slot_lag: SlotLagSampler,
}

#[derive(Clone)]
pub struct LocalSourceHealth {
    healthy: Arc<AtomicBool>,
    /// The CDC session's last cycle caught up with its replication
    /// connection, and no transaction it decoded was sent later than
    /// `CURRENT_SEND_DELAY_BOUND` after its commit.
    current: Arc<AtomicBool>,
    /// The CDC session's last cycle succeeded, whether or not it reached its
    /// bound. Cleared while the session is re-established and when the
    /// runtime stops.
    connected: Arc<AtomicBool>,
    failure: Arc<StdMutex<Option<HealthFailure>>>,
}

/// What gets past the error a CDC loop stopped on. The command names it in
/// its own words, since only the command knows its name and the variable it
/// read the source from.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CdcStopRemedy {
    /// A rerun, since the cause may not recur.
    Rerun,
    /// `calque destroy`, then a rerun; see
    /// [`LocalSourceError::requires_destroying_state`].
    DestroyState,
    /// Restoring the app-owned publication's enrolled definition, then a
    /// rerun; see [`LocalSourceError::publication_drifted`].
    RestorePublication,
}

/// What the health slot holds while the runtime is not serving.
#[derive(Debug, Clone)]
struct HealthFailure {
    detail: String,
    /// What gets past the error the CDC loop stopped on. Nothing but that
    /// error sets it, and nothing written after it changes it, so a caller
    /// that finds the runtime stopped can name the remedy from the health
    /// slot alone.
    remedy: CdcStopRemedy,
}

impl HealthFailure {
    fn new(detail: impl Into<String>) -> Self {
        Self {
            detail: detail.into(),
            remedy: CdcStopRemedy::Rerun,
        }
    }

    fn stopped_on(error: &LocalSourceError, source: &SourceContext) -> Self {
        let remedy = if error.requires_destroying_state() {
            CdcStopRemedy::DestroyState
        } else if error.publication_drifted() {
            CdcStopRemedy::RestorePublication
        } else {
            CdcStopRemedy::Rerun
        };
        Self {
            detail: cdc_failure_detail(error, source),
            remedy,
        }
    }
}

struct HealthExitGuard {
    healthy: Arc<AtomicBool>,
    current: Arc<AtomicBool>,
    connected: Arc<AtomicBool>,
    failure: Arc<StdMutex<Option<HealthFailure>>>,
}

struct SessionStartupGuard {
    session: Option<ActiveSourceSession>,
    cleanup_failed: Arc<AtomicBool>,
    startup_shutdown: Option<Arc<SourceStartupShutdown>>,
}

impl SessionStartupGuard {
    fn new(
        session: ActiveSourceSession,
        cleanup_failed: Arc<AtomicBool>,
        startup_shutdown: Arc<SourceStartupShutdown>,
    ) -> Self {
        Self {
            session: Some(session),
            cleanup_failed,
            startup_shutdown: Some(startup_shutdown),
        }
    }

    fn session(&self) -> &ActiveSourceSession {
        self.session.as_ref().expect("startup session is owned")
    }

    fn take(&mut self) -> Option<ActiveSourceSession> {
        self.startup_shutdown.take();
        self.session.take()
    }

    fn shutdown_after(mut self, original: LocalSourceError) -> LocalSourceError {
        if let Some(shutdown) = self.startup_shutdown.take() {
            let _ = shutdown.request_shutdown();
        }
        match self.session.take().map(ActiveSourceSession::shutdown) {
            Some(Err(_)) => LocalSourceError::SessionCleanupFailedAfter(Box::new(original)),
            _ => original,
        }
    }
}

impl Drop for SessionStartupGuard {
    fn drop(&mut self) {
        if let Some(session) = self.session.take() {
            if let Some(shutdown) = self.startup_shutdown.take() {
                let _ = shutdown.request_shutdown();
            }
            if session.shutdown().is_err() {
                self.cleanup_failed.store(true, Ordering::Release);
            }
        }
    }
}

impl HealthExitGuard {
    fn observe_cycle(&self, caught_up: bool, longest_send_delay: Duration) {
        // Readiness cannot be slot lag: the slot's confirmed position
        // advances only when Calque acknowledges a published transaction, so
        // WAL written outside the publication reads as lag until the next one.
        // Two signals cover each other's blind spot instead. A cycle that
        // never caught up with its connection is Calque falling behind; a
        // late send, on the source's own clock, is the walsender falling
        // behind while Calque drains everything that arrives.
        let current = caught_up && longest_send_delay <= CURRENT_SEND_DELAY_BOUND;
        self.current.store(current, Ordering::Release);
        self.connected.store(true, Ordering::Release);
    }

    fn mark_ready(&self) {
        self.healthy.store(true, Ordering::Release);
    }

    /// Closes readiness while the runtime re-establishes its session. The
    /// runtime stays healthy: it is still running and will serve again.
    fn mark_reconnecting(&self, error: &LocalSourceError) {
        self.current.store(false, Ordering::Release);
        self.connected.store(false, Ordering::Release);
        if let Ok(mut failure) = self.failure.lock() {
            *failure = Some(HealthFailure::new(format!("cdc-reconnecting: {error}")));
        }
    }

    /// Clears the reconnect detail. Readiness returns with the next ingest
    /// cycle that proves the session caught up, through `observe_cycle`.
    fn mark_reconnected(&self) {
        if let Ok(mut failure) = self.failure.lock() {
            *failure = None;
        }
    }

    fn record_failure(&self, detail: impl Into<String>) {
        self.record(HealthFailure::new(detail));
    }

    /// Records `failure`, then closes readiness. In that order, a caller that
    /// sees the runtime unhealthy reads the failure it stopped on rather than
    /// none, or a reconnect's.
    fn record(&self, failure: HealthFailure) {
        if let Ok(mut slot) = self.failure.lock() {
            *slot = Some(failure);
        }
        self.healthy.store(false, Ordering::Release);
        self.current.store(false, Ordering::Release);
        self.connected.store(false, Ordering::Release);
    }

    /// Records that closing the session failed beside the cause already
    /// recorded, not in its place: the cause, and whether it needs
    /// `calque destroy`, are what the operator acts on.
    fn record_cleanup_failure(&self) {
        if let Ok(mut slot) = self.failure.lock() {
            match slot.as_mut() {
                Some(cause) => {
                    // "<cause>; cdc-runtime-cleanup" reads as one sentence,
                    // so the cause loses its final full stop.
                    if cause.detail.ends_with('.') {
                        cause.detail.pop();
                    }
                    cause.detail.push_str("; cdc-runtime-cleanup");
                }
                None => *slot = Some(HealthFailure::new("cdc-runtime-cleanup")),
            }
        }
        self.healthy.store(false, Ordering::Release);
        self.current.store(false, Ordering::Release);
        self.connected.store(false, Ordering::Release);
    }
}

impl Drop for HealthExitGuard {
    fn drop(&mut self) {
        if let Ok(mut failure) = self.failure.lock() {
            if failure.is_none() {
                *failure = Some(HealthFailure::new("cdc-runtime-stopped"));
            }
        }
        self.healthy.store(false, Ordering::Release);
        self.current.store(false, Ordering::Release);
        self.connected.store(false, Ordering::Release);
    }
}

impl LocalSourceHealth {
    pub fn is_healthy(&self) -> bool {
        self.healthy.load(Ordering::Acquire)
    }

    pub fn failure(&self) -> Option<String> {
        self.failure
            .lock()
            .ok()
            .and_then(|failure| failure.as_ref().map(|failure| failure.detail.clone()))
    }

    /// What gets past the error the CDC loop stopped on: a rerun while the
    /// runtime runs, and for a stop with no such error.
    pub fn failure_remedy(&self) -> CdcStopRemedy {
        self.failure
            .lock()
            .ok()
            .and_then(|failure| failure.as_ref().map(|failure| failure.remedy))
            .unwrap_or(CdcStopRemedy::Rerun)
    }

    /// Readiness: the runtime runs, its last CDC cycle caught up with its
    /// replication connection, and no transaction it decoded was sent more
    /// than `CURRENT_SEND_DELAY_BOUND` after its commit.
    pub fn is_current(&self) -> bool {
        self.is_healthy() && self.current.load(Ordering::Acquire)
    }

    /// Whether `/v1/` is served: the runtime runs and its CDC session's last
    /// cycle succeeded. A source that is behind is served from the canonical
    /// log at the log's own cursor, which a refusal would only delay; one
    /// whose session is being re-established is refused.
    pub(crate) fn is_connected(&self) -> bool {
        self.is_healthy() && self.connected.load(Ordering::Acquire)
    }

    #[cfg(test)]
    pub(crate) fn for_test(healthy: bool) -> Self {
        Self {
            healthy: Arc::new(AtomicBool::new(healthy)),
            current: Arc::new(AtomicBool::new(healthy)),
            connected: Arc::new(AtomicBool::new(healthy)),
            failure: Arc::new(StdMutex::new(None)),
        }
    }

    /// A running runtime whose last cycle did not catch up.
    #[cfg(test)]
    pub(crate) fn behind_for_test() -> Self {
        let health = Self::for_test(true);
        health.current.store(false, Ordering::Release);
        health
    }
}

impl LocalSourceRuntime {
    pub fn provider(&self) -> Arc<Mutex<PostgresFencedCanonicalLog>> {
        Arc::clone(&self.provider)
    }

    pub fn barrier_provider(&self) -> Arc<StdMutex<PostgresFencedCanonicalLog>> {
        Arc::clone(&self.barrier_provider)
    }

    /// The readers change builds, bootstrap planners and the change tail
    /// read the canonical log through, beside the provider.
    pub fn log_readers(&self) -> SourceLogReaders {
        self.log_readers.clone()
    }

    pub fn is_healthy(&self) -> bool {
        self.health().is_healthy()
    }

    pub fn failure(&self) -> Option<String> {
        self.health().failure()
    }

    pub fn failure_remedy(&self) -> CdcStopRemedy {
        self.health().failure_remedy()
    }

    pub fn health(&self) -> LocalSourceHealth {
        LocalSourceHealth {
            healthy: Arc::clone(&self.healthy),
            current: Arc::clone(&self.current),
            connected: Arc::clone(&self.connected),
            failure: Arc::clone(&self.failure),
        }
    }

    pub fn shutdown(mut self) -> Result<(), LocalSourceError> {
        let _ = self.stop.send(());
        let Some(join) = self.join.take() else {
            return Ok(());
        };
        join_cdc_thread(join)
    }
}

/// Joins a CDC thread that reported readiness. The thread catches a panic in
/// its loop and returns it as `IngestThreadPanicked`, so a thread that
/// panicked anyway did so after the loop stopped, closing its session.
fn join_cdc_thread(
    join: thread::JoinHandle<Result<(), LocalSourceError>>,
) -> Result<(), LocalSourceError> {
    join.join()
        .map_err(|_| LocalSourceError::IngestThreadPanickedAfterLoop)?
}

impl Drop for LocalSourceRuntime {
    fn drop(&mut self) {
        let _ = self.stop.send(());
    }
}

pub struct LocalSourceRuntimeConfig<'a> {
    /// The source, which also holds the control store and canonical log: a
    /// trim reads the slot in the transaction that locks the lease.
    pub database_url: &'a DatabaseUrl,
    pub state: &'a LocalState,
    /// The state directory this runtime serves, named in the refusal of a
    /// canonical log an earlier release wrote.
    pub state_dir: &'a Path,
    pub owner_id: String,
    pub lease_ttl: Duration,
    pub ingest_timeout: Duration,
    pub max_transactions_per_batch: usize,
    pub metrics: OperationsMetrics,
    /// The environment variable `database_url` was read from, which a
    /// refusal names so the operator knows what to correct.
    pub database_url_env: Option<String>,
}

impl<'a> LocalSourceRuntimeConfig<'a> {
    /// A runtime with the default lease, ingest timeout and batch bound.
    pub fn new(
        database_url: &'a DatabaseUrl,
        state: &'a LocalState,
        state_dir: &'a Path,
        owner_id: impl Into<String>,
    ) -> Self {
        Self {
            database_url,
            state,
            state_dir,
            owner_id: owner_id.into(),
            lease_ttl: DEFAULT_LEASE_TTL,
            ingest_timeout: DEFAULT_INGEST_TIMEOUT,
            max_transactions_per_batch: DEFAULT_MAX_TRANSACTIONS,
            metrics: OperationsMetrics::new(),
            database_url_env: None,
        }
    }

    pub fn with_database_url_env(mut self, name: impl Into<String>) -> Self {
        self.database_url_env = Some(name.into());
        self
    }

    pub fn with_metrics(mut self, metrics: OperationsMetrics) -> Self {
        self.metrics = metrics;
        self
    }
}

#[derive(Debug)]
struct CanonicalHeadIngestObserver {
    signal: snapshot_pipeline::CanonicalHeadSignal,
    metrics: OperationsMetrics,
    append_started: StdMutex<Option<Instant>>,
}

impl PersistentIngestObserver for CanonicalHeadIngestObserver {
    fn observe(&self, boundary: PersistentIngestBoundary) {
        match boundary {
            PersistentIngestBoundary::BeforeCanonicalAppend => {
                if let Ok(mut started) = self.append_started.lock() {
                    *started = Some(Instant::now());
                }
            }
            PersistentIngestBoundary::AfterCanonicalAppend => {
                if let Ok(mut started) = self.append_started.lock() {
                    if let Some(started) = started.take() {
                        self.metrics
                            .observe_canonical_append_seconds(started.elapsed().as_secs_f64());
                    }
                }
            }
            _ => {}
        }
    }

    fn observe_durable_append(&self, _position: &CursorPosition) {
        self.signal.notify_head_advanced();
    }

    fn observe_appended_entries(&self, entries: usize) {
        self.metrics.record_canonical_entries_appended(entries);
    }
}

pub fn start_local_source_runtime(
    prepared: PreparedLocalSource,
    config: LocalSourceRuntimeConfig<'_>,
    head_signal: snapshot_pipeline::CanonicalHeadSignal,
) -> Result<LocalSourceRuntime, LocalSourceError> {
    let slot_lag_source = config.database_url.clone();
    let source = SourceContext::of(config.database_url, config.database_url_env.as_deref());
    start_local_source_runtime_sampling(prepared, config, head_signal, slot_lag_source)
        .map_err(|error| source.explained(error))
}

/// `start_local_source_runtime`, with the slot-lag sampler reading from
/// `slot_lag_source`.
fn start_local_source_runtime_sampling(
    prepared: PreparedLocalSource,
    config: LocalSourceRuntimeConfig<'_>,
    head_signal: snapshot_pipeline::CanonicalHeadSignal,
    slot_lag_source: DatabaseUrl,
) -> Result<LocalSourceRuntime, LocalSourceError> {
    // Every source-side effect, session command, provider connection, and the
    // first bounded ingest runs inside this one owner thread. The caller starts
    // its deadline immediately after spawn and never exposes an HTTP listener
    // until the owner returns the exact providers it initialized.
    let database_url = config.database_url.clone();
    let source_context = SourceContext::of(config.database_url, config.database_url_env.as_deref());
    let state = config.state.clone();
    let state_dir = config.state_dir.to_path_buf();
    let owner_id = OwnerId::new(config.owner_id)?;
    let lease_ttl = config.lease_ttl;
    let ingest_timeout = config.ingest_timeout;
    let max_transactions_per_batch = config.max_transactions_per_batch;
    let metrics = config.metrics;
    let slot_lag = SlotLagSampler::start(
        slot_lag_source,
        state.names.slot_name.clone(),
        metrics.clone(),
    )
    .map_err(LocalSourceError::ThreadStart)?;
    let healthy = Arc::new(AtomicBool::new(false));
    let current = Arc::new(AtomicBool::new(false));
    let connected = Arc::new(AtomicBool::new(false));
    let failure = Arc::new(StdMutex::new(None));
    let thread_healthy = Arc::clone(&healthy);
    let thread_current = Arc::clone(&current);
    let thread_connected = Arc::clone(&connected);
    let thread_failure = Arc::clone(&failure);
    let startup_shutdown = Arc::new(SourceStartupShutdown::new());
    let thread_startup_shutdown = Arc::clone(&startup_shutdown);
    let (stop, stop_rx) = mpsc::channel();
    let (startup_tx, startup_rx) = mpsc::sync_channel(1);
    let join = thread::Builder::new()
        .name("calque-cdc".to_owned())
        .spawn(move || {
            let health_guard = HealthExitGuard {
                healthy: thread_healthy,
                current: thread_current,
                connected: thread_connected,
                failure: thread_failure,
            };
            let setup = catch_unwind(AssertUnwindSafe(|| -> Result<_, LocalSourceError> {
                startup_not_cancelled(&stop_rx)?;
                let store = PostgresFenceStore::connect(
                    database_url.connection_url(PostgresConnectionFamily::FenceControl),
                )?;
                match store.enroll_source(&state.names.source_id, &state.source_epoch) {
                    Ok(()) | Err(FenceError::SourceAlreadyEnrolled(_)) => {}
                    Err(error) => return Err(error.into()),
                }
                startup_not_cancelled(&stop_rx)?;
                let authority = Arc::new(FenceAuthority::new(store));
                let request = AcquireRequest {
                    source_id: state.names.source_id.clone(),
                    source_epoch: state.source_epoch.clone(),
                    owner_id,
                    ttl: lease_ttl,
                };
                let provisional =
                    acquire_startup_lease(startup_lease_wait(lease_ttl), &stop_rx, || {
                        authority.acquire(request.clone())
                    })?;
                let slot = SlotProvisioningConfig::new(&state.names.slot_name)?;
                match provision_persistent_slot(
                    &authority,
                    &provisional,
                    database_url.connection_url(PostgresConnectionFamily::SlotProvisioning),
                    &prepared.activated,
                    &slot,
                ) {
                    Ok(_)
                    | Err(SourceRuntimeError::Fence(FenceError::SlotLifecycleAlreadyInitialized)) =>
                        {}
                    Err(error) => {
                        authority.release_after_failure(&provisional);
                        return Err(session_error(error, &prepared.activated));
                    }
                }
                if let Err(error) = startup_not_cancelled(&stop_rx) {
                    authority.release_after_failure(&provisional);
                    return Err(error);
                }
                let replication_url = replication_database_url(&database_url)?;
                let session = establish_persistent_source_interruptible(
                    Arc::clone(&authority),
                    provisional,
                    database_url.connection_url(PostgresConnectionFamily::SourceSessionControl),
                    &replication_url,
                    &prepared.activated,
                    Arc::clone(&thread_startup_shutdown),
                )
                .map_err(|error| session_error(error, &prepared.activated))?;
                let cleanup_failed = Arc::new(AtomicBool::new(false));
                let mut startup_guard = SessionStartupGuard::new(
                    session,
                    Arc::clone(&cleanup_failed),
                    Arc::clone(&thread_startup_shutdown),
                );
                let provider_setup = (|| -> Result<_, LocalSourceError> {
                    startup_not_cancelled(&stop_rx)?;
                    let identity = startup_guard.session().lease().identity();
                    let mut control = connect_owned(
                        &database_url,
                        PostgresConnectionFamily::FenceControl,
                        "calque-control-identity",
                    )?;
                    let datastore = PostgresDatastoreIdentity::inspect(&mut control)?;
                    let writer = FencedWriterIdentity::new(
                        identity.source_id().clone(),
                        identity.source_epoch().clone(),
                        identity.token().get(),
                        datastore.clone(),
                    )?;
                    let provider = Arc::new(Mutex::new(PostgresFencedCanonicalLog::connect(
                        database_url.connection_url(PostgresConnectionFamily::CanonicalWriter),
                        writer.clone(),
                        &state_dir,
                    )?));
                    let barrier_provider =
                        Arc::new(StdMutex::new(PostgresFencedCanonicalLog::connect(
                            database_url.connection_url(PostgresConnectionFamily::CanonicalBarrier),
                            writer.clone(),
                            &state_dir,
                        )?));
                    let log_readers = {
                        let url = database_url.clone();
                        let state_dir = state_dir.clone();
                        SourceLogReaders::new(move || {
                            PostgresCanonicalLogReader::connect(
                                url.connection_url(PostgresConnectionFamily::CanonicalReader),
                                &writer,
                                &state_dir,
                            )
                        })
                    };
                    // Readiness waits for a session's first cycle, which ends
                    // once the session is current instead of after its timeout.
                    let ingest = PersistentIngestConfig::new(&state_dir)
                        .with_bounds(max_transactions_per_batch, ingest_timeout)?
                        .ending_once_current()
                        .with_observer(Arc::new(CanonicalHeadIngestObserver {
                            signal: head_signal,
                            metrics: metrics.clone(),
                            append_started: StdMutex::new(None),
                        }));
                    Ok((provider, barrier_provider, log_readers, ingest, datastore))
                })();
                match provider_setup {
                    Ok((provider, barrier_provider, log_readers, ingest, datastore)) => Ok((
                        startup_guard
                            .take()
                            .ok_or(LocalSourceError::SessionCleanupFailed)?,
                        provider,
                        barrier_provider,
                        log_readers,
                        ingest,
                        SessionReestablisher {
                            database_url: database_url.clone(),
                            replication_url,
                            activated: prepared.activated.clone(),
                            lease_ttl,
                            datastore,
                            state_dir: state_dir.clone(),
                        },
                    )),
                    Err(error) => Err(startup_guard.shutdown_after(error)),
                }
            }));
            let (session, provider, barrier_provider, log_readers, ingest, reestablisher) =
                match setup {
                    Ok(Ok(setup)) => setup,
                    Ok(Err(error)) => {
                        health_guard.record_failure("cdc-runtime-startup");
                        let _ = thread_startup_shutdown.request_shutdown();
                        return report_startup_failure(&startup_tx, error);
                    }
                    Err(_) => {
                        health_guard.record_failure("cdc-runtime-panic");
                        let _ = thread_startup_shutdown.request_shutdown();
                        return report_startup_failure(
                            &startup_tx,
                            LocalSourceError::IngestThreadPanicked,
                        );
                    }
                };
            let mut startup_announced = false;
            let startup_lease = session.lease().identity().clone();
            let mut session = Some(session);
            let outcome = catch_unwind(AssertUnwindSafe(|| {
                publish_ready_after_initial_cycle(
                    &health_guard,
                    &startup_tx,
                    CdcStartupSignal::Ready(CdcStartupReady {
                        provider: Arc::clone(&provider),
                        barrier_provider: Arc::clone(&barrier_provider),
                        log_readers,
                    }),
                    || {
                        let stats = session
                            .as_mut()
                            .expect("the startup session is owned until its first cycle")
                            .ingest_bounded(&prepared.registry, &prepared.policy, ingest.clone())?;
                        health_guard.observe_cycle(stats.caught_up, stats.longest_send_delay);
                        startup_not_cancelled(&stop_rx)?;
                        if !thread_startup_shutdown.disarm()? {
                            return Err(LocalSourceError::CdcStartupCancelled);
                        }
                        Ok(())
                    },
                )?;
                startup_announced = true;

                CdcLoop {
                    stop: &stop_rx,
                    prepared: &prepared,
                    ingest: &ingest,
                    health: &health_guard,
                    metrics: &metrics,
                    reestablisher: &reestablisher,
                    provider: &provider,
                    barrier_provider: &barrier_provider,
                }
                .run(&mut session, startup_lease)
            }));
            // Close readiness as soon as the loop outcome is known. Session
            // cleanup can reconnect and wait for slot inactivity; no request
            // may be admitted during that interval.
            match &outcome {
                Ok(Ok(())) => health_guard.record_failure("cdc-runtime-stopped"),
                Ok(Err(error)) => {
                    let failure = HealthFailure::stopped_on(error, &source_context);
                    // A startup failure reaches the caller instead. After
                    // startup nothing else reports the cause until the
                    // process exits.
                    if startup_announced {
                        tracing::error!(
                            failure = %failure.detail,
                            state_must_be_destroyed =
                                failure.remedy == CdcStopRemedy::DestroyState,
                            "CDC runtime stopped without reconnecting"
                        );
                    }
                    health_guard.record(failure)
                }
                Err(_) => health_guard.record_failure("cdc-runtime-panic"),
            }
            let _ = thread_startup_shutdown.request_shutdown();
            // A session is absent only when the loop stopped or failed while
            // re-establishing one. The failed session was already closed and
            // its lease revoked locally; the lease row expires on its own.
            let shutdown = session.take().map_or(Ok(()), ActiveSourceSession::shutdown);
            if shutdown.is_err() {
                health_guard.record_cleanup_failure();
            }
            let result = cdc_thread_result(outcome, shutdown.is_err());
            match result {
                Err(error) if !startup_announced => report_startup_failure(&startup_tx, error),
                result => result,
            }
        });
    let join = join.map_err(LocalSourceError::ThreadStart)?;
    let (join, ready) = await_cdc_startup(
        startup_rx,
        lease_ttl,
        &stop,
        Arc::clone(&startup_shutdown),
        join,
    )?;

    Ok(LocalSourceRuntime {
        provider: ready.provider,
        barrier_provider: ready.barrier_provider,
        log_readers: ready.log_readers,
        healthy,
        current,
        connected,
        failure,
        stop,
        join: Some(join),
        _slot_lag: slot_lag,
    })
}

/// What re-establishing a session needs that startup already resolved.
struct SessionReestablisher {
    database_url: DatabaseUrl,
    replication_url: String,
    activated: ActivatedSource,
    lease_ttl: Duration,
    /// The canonical-log datastore startup inspected. A provider rebuilt
    /// against any other datastore refuses to connect, which is fatal.
    datastore: PostgresDatastoreIdentity,
    state_dir: PathBuf,
}

/// The steady-state ingest loop of a running source runtime.
struct CdcLoop<'a> {
    stop: &'a mpsc::Receiver<()>,
    prepared: &'a PreparedLocalSource,
    ingest: &'a PersistentIngestConfig,
    health: &'a HealthExitGuard,
    metrics: &'a OperationsMetrics,
    reestablisher: &'a SessionReestablisher,
    provider: &'a Arc<Mutex<PostgresFencedCanonicalLog>>,
    barrier_provider: &'a Arc<StdMutex<PostgresFencedCanonicalLog>>,
}

impl CdcLoop<'_> {
    /// Runs bounded ingest cycles until a stop request or a fatal failure.
    ///
    /// A transient failure closes readiness, drops the session and
    /// re-establishes one under backoff. The new session replays from the
    /// slot's confirmed position, which feedback advances only after a
    /// transaction's canonical append and checkpoint are durable, so a
    /// transaction appended but not yet acknowledged is replayed; the
    /// canonical append finds it by its transaction key and does not write it
    /// a second time.
    fn run(
        &self,
        session: &mut Option<ActiveSourceSession>,
        mut lease: LeaseIdentity,
    ) -> Result<(), LocalSourceError> {
        let mut backoff = ReconnectBackoff::new(reconnect_jitter);
        loop {
            if self.stop.try_recv().is_ok() {
                return Ok(());
            }
            let cycle = match session.as_mut() {
                Some(active) => active.ingest_bounded(
                    &self.prepared.registry,
                    &self.prepared.policy,
                    self.ingest.clone(),
                ),
                None => Err(PersistentIngestError::SessionClosed),
            };
            let error = match cycle {
                Ok(stats) => {
                    self.health
                        .observe_cycle(stats.caught_up, stats.longest_send_delay);
                    backoff.reset();
                    continue;
                }
                Err(error) => LocalSourceError::from(error),
            };
            self.metrics.record_error(OperationErrorClass::Cdc);
            if classify_cdc_failure(&error) == CdcFailureClass::Fatal {
                return Err(error);
            }
            self.health.mark_reconnecting(&error);
            tracing::warn!(%error, "CDC replication session failed; re-establishing it");
            // `ingest_bounded` closed the socket and revoked the lease locally
            // before returning, and dropping the session stops its watchdog.
            // It is not shut down: that releases the lease through a control
            // connection a restart has already closed, and the next attempt
            // takes the lease back either way.
            drop(session.take());
            let failed_at = Instant::now();
            let outcome = reconnect_with_backoff(
                &mut backoff,
                |delay| stop_requested_within(self.stop, delay),
                || self.reestablish(&mut lease),
                |error| {
                    self.metrics.record_error(OperationErrorClass::Cdc);
                    self.health.mark_reconnecting(error);
                    tracing::warn!(%error, "CDC reconnect attempt failed; retrying");
                },
            );
            match outcome {
                Reconnect::Established(active) => {
                    *session = Some(active);
                    self.metrics.record_cdc_reconnect();
                    self.health.mark_reconnected();
                    tracing::info!(
                        elapsed_ms = failed_at.elapsed().as_millis(),
                        "CDC replication session re-established"
                    );
                }
                Reconnect::Stopped => return Ok(()),
                Reconnect::Failed(error) => return Err(error),
            }
        }
    }

    /// One attempt to replace a failed session: take the lease back, open
    /// replication from the slot's confirmed position, and move both
    /// canonical-log providers to the new fence token.
    fn reestablish(
        &self,
        lease: &mut LeaseIdentity,
    ) -> Result<ActiveSourceSession, LocalSourceError> {
        let plan = self.reestablisher;
        // A fresh control connection for every attempt: after a restart the
        // one startup opened is closed.
        let store = PostgresFenceStore::connect(
            plan.database_url
                .connection_url(PostgresConnectionFamily::FenceControl),
        )?;
        let authority = Arc::new(FenceAuthority::new(store));
        let provisional = authority.reacquire(lease, plan.lease_ttl)?;
        // Every later step can fail. The next attempt must present the lease
        // as the control store now records it.
        *lease = provisional.identity().clone();
        let session = establish_persistent_source(
            authority,
            provisional,
            plan.database_url
                .connection_url(PostgresConnectionFamily::SourceSessionControl),
            &plan.replication_url,
            &plan.activated,
        )
        .map_err(|error| session_error(error, &plan.activated))?;
        let identity = session.lease().identity();
        let writer = FencedWriterIdentity::new(
            identity.source_id().clone(),
            identity.source_epoch().clone(),
            identity.token().get(),
            plan.datastore.clone(),
        )?;
        let provider = PostgresFencedCanonicalLog::connect(
            plan.database_url
                .connection_url(PostgresConnectionFamily::CanonicalWriter),
            writer.clone(),
            &plan.state_dir,
        )?;
        let barrier_provider = PostgresFencedCanonicalLog::connect(
            plan.database_url
                .connection_url(PostgresConnectionFamily::CanonicalBarrier),
            writer,
            &plan.state_dir,
        )?;
        // The HTTP backend and the snapshot pipeline hold these `Arc`s from
        // startup. Replacing the value behind each lock gives them the new
        // token and live connections without re-wiring either. The reacquired
        // token is higher than the old one, so a provider left on the old
        // token could neither append nor pass the readiness probe again.
        *self.provider.blocking_lock() = provider;
        *self
            .barrier_provider
            .lock()
            .unwrap_or_else(PoisonError::into_inner) = barrier_provider;
        Ok(session)
    }
}

/// Whether the runtime may re-establish its session after a failure.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum CdcFailureClass {
    /// A PostgreSQL connection was lost or could not be opened, or returned an
    /// uncertain result. Every retry starts by reacquiring the lease, which
    /// refuses once another owner has held it, and replays from the slot's
    /// confirmed position into an idempotent canonical append.
    Transient,
    /// Everything else, including a `PostgresRefusal`, which no retry gets
    /// past, and any failure this classification does not name.
    Fatal,
}

/// `error` from opening a session for `activated`, with a publication that
/// no longer has its enrolled definition refused as enrollment refuses it,
/// naming the publication. A reconnect that meets the drift then stops on
/// the refusal a start meets, and the command names the same remedy.
fn session_error(error: SourceRuntimeError, activated: &ActivatedSource) -> LocalSourceError {
    match error {
        SourceRuntimeError::PublicationDrift => LocalSourceError::PublicationDrift {
            publication: activated.registry_binding().publication().name.clone(),
        },
        error => error.into(),
    }
}

/// The readiness detail of a runtime that stopped on `error`. A refusal says
/// what to change on the source.
fn cdc_failure_detail(error: &LocalSourceError, source: &SourceContext) -> String {
    match postgres_refusal(error) {
        Some(refusal) => format!("cdc-refused: {}", source.advice(refusal)),
        None => format!("persistent CDC failed: {error}"),
    }
}

fn postgres_refusal(error: &LocalSourceError) -> Option<PostgresRefusal> {
    match error {
        LocalSourceError::Fence(FenceError::ControlStoreRefused(refusal))
        | LocalSourceError::SourceRefused { refusal, .. }
        | LocalSourceError::Runtime(
            SourceRuntimeError::SourceRefused(refusal)
            | SourceRuntimeError::Fence(FenceError::ControlStoreRefused(refusal)),
        )
        | LocalSourceError::Ingest(PersistentIngestError::Fence(
            FenceError::ControlStoreRefused(refusal),
        )) => Some(*refusal),
        _ => None,
    }
}

/// Who calque connects as and where, for the refusals that tell an operator
/// what to change. The password and the URL never appear in a message.
#[derive(Debug, Clone, Default)]
pub(crate) struct SourceContext {
    role: Option<String>,
    database: Option<String>,
    /// The environment variable the database URL was read from, when the
    /// caller says so.
    url_env: Option<String>,
}

impl SourceContext {
    pub(crate) fn of(database_url: &DatabaseUrl, url_env: Option<&str>) -> Self {
        let decoded = |value: &str| {
            let value = percent_encoding::percent_decode_str(value)
                .decode_utf8_lossy()
                .into_owned();
            (!value.is_empty()).then_some(value)
        };
        let url = url::Url::parse(database_url.expose()).ok();
        Self {
            role: url.as_ref().and_then(|url| decoded(url.username())),
            database: url
                .as_ref()
                .and_then(|url| decoded(url.path().trim_start_matches('/'))),
            url_env: url_env.map(str::to_owned),
        }
    }

    fn role_phrase(&self) -> String {
        self.role.as_deref().map_or_else(
            || "the connecting role".to_owned(),
            |role| format!("role {}", quote_identifier(role)),
        )
    }

    fn role_name(&self) -> String {
        self.role
            .as_deref()
            .map_or_else(|| "<role>".to_owned(), quote_identifier)
    }

    fn database_name(&self) -> String {
        self.database
            .as_deref()
            .map_or_else(|| "<database>".to_owned(), quote_identifier)
    }

    fn url_phrase(&self) -> String {
        self.url_env.as_deref().map_or_else(
            || "the database URL".to_owned(),
            |name| format!("the database URL in {name}"),
        )
    }

    /// What PostgreSQL refused and what an administrator changes to get past
    /// it.
    pub(crate) fn advice(&self, refusal: PostgresRefusal) -> String {
        let role = self.role_phrase();
        let url = self.url_phrase();
        match refusal {
            PostgresRefusal::AuthenticationFailed => format!(
                "PostgreSQL refused to authenticate {role}. Check the password in {url}, that the role may log in, and that pg_hba.conf lets it connect to database {} from this host",
                self.database_name()
            ),
            PostgresRefusal::InsufficientPrivilege => format!(
                "{role} lacks a privilege calque needs on the source. It needs CONNECT and CREATE on database {db} (GRANT CONNECT, CREATE ON DATABASE {db} TO {}) and SELECT on the enrolled tables, with INSERT, UPDATE and DELETE when pages write (GRANT SELECT, INSERT, UPDATE, DELETE ON <table> TO {})",
                self.role_name(),
                self.role_name(),
                db = self.database_name()
            ),
            PostgresRefusal::DatabaseMissing => format!(
                "database {} does not exist on the source. Check the database name in {url}",
                self.database_name()
            ),
            PostgresRefusal::ReplicationPrivilegeMissing => format!(
                "{role} cannot start replication, which needs the REPLICATION attribute. Run ALTER ROLE {} REPLICATION as a superuser; on RDS, run GRANT rds_replication TO {} instead",
                self.role_name(),
                self.role_name()
            ),
        }
    }

    /// `error` as the refusal it carries, worded with this context, so the
    /// caller of a start that PostgreSQL refused reads what to change instead
    /// of the refusal's class.
    fn explained(&self, error: LocalSourceError) -> LocalSourceError {
        if matches!(error, LocalSourceError::SourceRefused { .. }) {
            return error;
        }
        let refusal = postgres_refusal(&error).or_else(|| match &error {
            LocalSourceError::Postgres(error) => PostgresRefusal::of(error)
                .filter(|refusal| *refusal != PostgresRefusal::InsufficientPrivilege),
            _ => None,
        });
        match refusal {
            Some(refusal) => LocalSourceError::SourceRefused {
                refusal,
                advice: self.advice(refusal),
            },
            None => error,
        }
    }
}

fn classify_cdc_failure(error: &LocalSourceError) -> CdcFailureClass {
    let transient = match error {
        LocalSourceError::Ingest(error) => transient_ingest_failure(error),
        LocalSourceError::Runtime(error) => transient_runtime_failure(error),
        LocalSourceError::Fence(error) => transient_fence_failure(error),
        LocalSourceError::Canonical(error) => matches!(error, LogError::PostgresUnavailable),
        _ => false,
    };
    if transient {
        CdcFailureClass::Transient
    } else {
        CdcFailureClass::Fatal
    }
}

fn transient_ingest_failure(error: &PersistentIngestError) -> bool {
    match error {
        PersistentIngestError::SessionClosed
        | PersistentIngestError::ReplicationUnavailable
        | PersistentIngestError::ReplicationSilent(_)
        | PersistentIngestError::MutationReceiptUnavailable => true,
        PersistentIngestError::CanonicalLog(error)
        | PersistentIngestError::Ingest(IngestError::Append(AppendFailure::CanonicalLog(error))) => {
            transient_append_failure(error)
        }
        PersistentIngestError::Fence(error) => transient_fence_failure(error),
        // The lease was revoked locally, which the watchdog does when a
        // renewal fails.
        PersistentIngestError::Ingest(IngestError::Fence(IngestFenceError::NoActiveLease(_))) => {
            true
        }
        // The receipt-marking error type is private to source-runtime; its
        // disposition is Retry only when the receipt connection failed.
        PersistentIngestError::Ingest(IngestError::Append(AppendFailure::Other(_))) => {
            error.disposition() == PersistentIngestDisposition::Retry
        }
        _ => false,
    }
}

/// The append either failed to reach PostgreSQL or found the lease row no
/// longer authorizing this token, which an expiry during an outage also
/// causes. Replay makes an append whose commit went unobserved harmless.
fn transient_append_failure(error: &LogError) -> bool {
    matches!(
        error,
        LogError::PostgresUnavailable | LogError::FencedWriterRejected
    )
}

/// `LeaseLost` cannot tell an expiry from a takeover. It is retried because
/// the retry's `reacquire` makes that distinction and returns
/// `LeaseSuperseded`, which is fatal, for a takeover.
fn transient_fence_failure(error: &FenceError) -> bool {
    matches!(
        error,
        FenceError::ControlStoreUnavailable | FenceError::LeaseLost | FenceError::LeaseRowLocked
    )
}

fn transient_runtime_failure(error: &SourceRuntimeError) -> bool {
    match error {
        SourceRuntimeError::Fence(error) => transient_fence_failure(error),
        // The previous walsender may still hold the slot while PostgreSQL
        // notices its client is gone; the next attempt terminates it again.
        SourceRuntimeError::SourceControlUnavailable
        | SourceRuntimeError::ReplicationUnavailable
        | SourceRuntimeError::BackendRevocationFailed(_)
        | SourceRuntimeError::SlotDidNotBecomeInactive => true,
        _ => false,
    }
}

enum Reconnect<S> {
    Established(S),
    Stopped,
    Failed(LocalSourceError),
}

/// Waits out each backoff delay, then makes one attempt, until an attempt
/// succeeds, fails fatally, or a stop is requested during a wait.
///
/// `wait` sleeps for the delay it is given and returns whether a stop was
/// requested meanwhile. `on_transient` sees each retried failure.
fn reconnect_with_backoff<S>(
    backoff: &mut ReconnectBackoff,
    mut wait: impl FnMut(Duration) -> bool,
    mut attempt: impl FnMut() -> Result<S, LocalSourceError>,
    mut on_transient: impl FnMut(&LocalSourceError),
) -> Reconnect<S> {
    loop {
        if wait(backoff.next_delay()) {
            return Reconnect::Stopped;
        }
        match attempt() {
            Ok(established) => return Reconnect::Established(established),
            Err(error) => match classify_cdc_failure(&error) {
                CdcFailureClass::Transient => on_transient(&error),
                CdcFailureClass::Fatal => return Reconnect::Failed(error),
            },
        }
    }
}

/// Exponential reconnect backoff with jitter; see `RECONNECT_INITIAL_DELAY`.
struct ReconnectBackoff {
    step: Duration,
    random: fn() -> u64,
}

impl ReconnectBackoff {
    fn new(random: fn() -> u64) -> Self {
        Self {
            step: RECONNECT_INITIAL_DELAY,
            random,
        }
    }

    /// Returns a delay drawn uniformly from the upper half of the current
    /// step, then doubles the step up to the cap.
    fn next_delay(&mut self) -> Duration {
        let step = self.step;
        self.step = step.saturating_mul(2).min(RECONNECT_MAX_DELAY);
        let half = step / 2;
        let spread = u64::try_from(half.as_nanos()).unwrap_or(u64::MAX);
        half + Duration::from_nanos((self.random)() % spread.saturating_add(1))
    }

    fn reset(&mut self) {
        self.step = RECONNECT_INITIAL_DELAY;
    }
}

/// Without operating-system randomness every delay is half its step, which
/// the schedule still bounds.
fn reconnect_jitter() -> u64 {
    getrandom::u64().unwrap_or(0)
}

/// Sleeps for `delay` unless a stop arrives first. A dropped runtime handle
/// counts as a stop: nothing would observe a reconnected session.
fn stop_requested_within(stop: &mpsc::Receiver<()>, delay: Duration) -> bool {
    !matches!(
        stop.recv_timeout(delay),
        Err(mpsc::RecvTimeoutError::Timeout)
    )
}

struct CdcStartupReady {
    provider: Arc<Mutex<PostgresFencedCanonicalLog>>,
    barrier_provider: Arc<StdMutex<PostgresFencedCanonicalLog>>,
    log_readers: SourceLogReaders,
}

enum CdcStartupSignal {
    Ready(CdcStartupReady),
    Failed(LocalSourceError),
}

/// What the CDC thread returns once its loop ended with `outcome` and closing
/// its session failed or not. A panic counts as the loop's error, and a failed
/// cleanup after any error keeps that error as the cause beside it.
fn cdc_thread_result(
    outcome: thread::Result<Result<(), LocalSourceError>>,
    cleanup_failed: bool,
) -> Result<(), LocalSourceError> {
    let result = outcome.unwrap_or_else(|_| Err(LocalSourceError::IngestThreadPanicked));
    match result {
        Err(cause) if cleanup_failed => {
            Err(LocalSourceError::SessionCleanupFailedAfter(Box::new(cause)))
        }
        Ok(()) if cleanup_failed => Err(LocalSourceError::SessionCleanupFailed),
        result => result,
    }
}

/// Hands a startup failure to the caller waiting for readiness. That caller
/// drops this thread's handle without joining it, so the signal is the only
/// route by which the cause reaches the operator. The thread keeps the error
/// only when the caller has already stopped waiting.
fn report_startup_failure(
    startup_tx: &mpsc::SyncSender<CdcStartupSignal>,
    error: LocalSourceError,
) -> Result<(), LocalSourceError> {
    match startup_tx.send(CdcStartupSignal::Failed(error)) {
        Err(mpsc::SendError(CdcStartupSignal::Failed(error))) => Err(error),
        _ => Err(LocalSourceError::CdcStartupHandshake),
    }
}

fn publish_ready_after_initial_cycle<T>(
    health: &HealthExitGuard,
    ready_tx: &mpsc::SyncSender<T>,
    ready: T,
    initial_cycle: impl FnOnce() -> Result<(), LocalSourceError>,
) -> Result<(), LocalSourceError> {
    initial_cycle()?;
    health.mark_ready();
    ready_tx
        .send(ready)
        .map_err(|_| LocalSourceError::CdcStartupHandshake)
}

fn startup_lease_wait(lease_ttl: Duration) -> Duration {
    lease_ttl + STARTUP_LEASE_MARGIN
}

/// Takes the source lease at startup, waiting out one that another process
/// holds.
///
/// A holder that died without releasing the lease stops renewing it, so the
/// row expires within `wait` and the next attempt takes it. A holder that is
/// still running renews it before every expiry, and startup refuses it with
/// `LeaseHeld` once `wait` has passed. A stop request ends the wait.
fn acquire_startup_lease<T>(
    wait: Duration,
    stop: &mpsc::Receiver<()>,
    mut acquire: impl FnMut() -> Result<T, FenceError>,
) -> Result<T, LocalSourceError> {
    let started = Instant::now();
    let mut announced = false;
    loop {
        match acquire() {
            Err(error @ FenceError::LeaseHeld { .. }) if started.elapsed() < wait => {
                if !announced {
                    announced = true;
                    tracing::warn!(
                        %error,
                        wait_ms = wait.as_millis(),
                        "another process holds the source lease; waiting for it to be released or to expire"
                    );
                }
                if stop_requested_within(stop, STARTUP_LEASE_RETRY) {
                    return Err(LocalSourceError::CdcStartupCancelled);
                }
            }
            Err(FenceError::EpochMismatch { .. }) => {
                return Err(LocalSourceError::EnrolledByAnotherState)
            }
            result => return Ok(result?),
        }
    }
}

fn startup_not_cancelled(stop: &mpsc::Receiver<()>) -> Result<(), LocalSourceError> {
    match stop.try_recv() {
        Ok(()) | Err(mpsc::TryRecvError::Disconnected) => {
            Err(LocalSourceError::CdcStartupCancelled)
        }
        Err(mpsc::TryRecvError::Empty) => Ok(()),
    }
}

fn await_cdc_startup(
    startup_rx: mpsc::Receiver<CdcStartupSignal>,
    lease_ttl: Duration,
    stop: &mpsc::Sender<()>,
    startup_shutdown: Arc<SourceStartupShutdown>,
    join: thread::JoinHandle<Result<(), LocalSourceError>>,
) -> Result<
    (
        thread::JoinHandle<Result<(), LocalSourceError>>,
        CdcStartupReady,
    ),
    LocalSourceError,
> {
    await_cdc_startup_with_timeout(
        startup_rx,
        CDC_STARTUP_TIMEOUT + startup_lease_wait(lease_ttl),
        stop,
        startup_shutdown,
        join,
    )
}

fn await_cdc_startup_with_timeout(
    startup_rx: mpsc::Receiver<CdcStartupSignal>,
    timeout: Duration,
    stop: &mpsc::Sender<()>,
    startup_shutdown: Arc<SourceStartupShutdown>,
    join: thread::JoinHandle<Result<(), LocalSourceError>>,
) -> Result<
    (
        thread::JoinHandle<Result<(), LocalSourceError>>,
        CdcStartupReady,
    ),
    LocalSourceError,
> {
    match startup_rx.recv_timeout(timeout) {
        Ok(CdcStartupSignal::Ready(ready)) => Ok((join, ready)),
        Ok(CdcStartupSignal::Failed(error)) => {
            let _ = startup_shutdown.request_shutdown();
            // Failed is sent only after session cleanup. Dropping a completed
            // or completing JoinHandle never blocks startup error reporting.
            drop(join);
            Err(error)
        }
        Err(mpsc::RecvTimeoutError::Disconnected) => {
            let _ = startup_shutdown.request_shutdown();
            drop(join);
            Err(LocalSourceError::CdcStartupHandshake)
        }
        Err(mpsc::RecvTimeoutError::Timeout) => {
            let _ = stop.send(());
            let _ = startup_shutdown.request_shutdown();
            // Do not perform an unbounded join. No HTTP listener exists; the
            // session-owning thread remains responsible for fail-closed
            // cleanup after its finite connection/ingest boundary returns.
            drop(join);
            Err(LocalSourceError::CdcStartupTimeout)
        }
    }
}

pub(crate) fn replication_database_url(
    database_url: &DatabaseUrl,
) -> Result<String, LocalSourceError> {
    replication_database_url_with_environment(database_url, |name| std::env::var_os(name))
}

fn replication_database_url_with_environment(
    database_url: &DatabaseUrl,
    environment: impl FnMut(&str) -> Option<std::ffi::OsString>,
) -> Result<String, LocalSourceError> {
    database_url.validate_libpq_environment_with(environment)?;
    let mut url =
        url::Url::parse(database_url.connection_url(PostgresConnectionFamily::Replication))?;
    let hostaddr = database_url
        .tcp_address()
        .map(|address| address.to_string())
        .unwrap_or_default();
    // `PQconnectdb` fills only unset parameters from libpq environment and
    // service defaults. Set hostaddr explicitly (including the empty Unix-
    // socket value) so PGHOSTADDR/PGSERVICE cannot redirect this connection
    // away from the already-normalized host authority.
    url.query_pairs_mut()
        .append_pair("hostaddr", &hostaddr)
        .append_pair("replication", "database")
        .append_pair("application_name", "calque");
    // Appended by hand: the form encoder writes a space as `+`, which libpq
    // reads literally.
    let query = format!(
        "{}&options={}",
        url.query().unwrap_or_default(),
        postgres_connect::session_options_url_value()
    );
    url.set_query(Some(&query));
    Ok(url.to_string())
}

// Every statement on a connection from `connect_owned` runs under these,
// except the waits in `lock_for_publication`.
const OWNED_STATEMENT_TIMEOUT: Duration = Duration::from_secs(2);
const OWNED_LOCK_TIMEOUT: Duration = Duration::from_secs(1);

/// A connection to the source whose statements run under its 2 s
/// `statement_timeout`. A request that hears nothing from the server for
/// longer than that, plus `postgres_connect::REPLY_MARGIN`, fails it.
pub(crate) fn connect_owned(
    database_url: &DatabaseUrl,
    family: PostgresConnectionFamily,
    application_name: &str,
) -> Result<Client, LocalSourceError> {
    connect_owned_waiting(
        database_url,
        family,
        application_name,
        OWNED_STATEMENT_TIMEOUT,
    )
}

/// `connect_owned`, for a connection one of whose statements raises its
/// `statement_timeout` to `longest_statement`.
fn connect_owned_waiting(
    database_url: &DatabaseUrl,
    family: PostgresConnectionFamily,
    application_name: &str,
    longest_statement: Duration,
) -> Result<Client, LocalSourceError> {
    let mut endpoint = database_url.endpoint(family)?;
    endpoint
        .bound_replies(longest_statement)
        .config_mut()
        .application_name(application_name);
    let mut client = endpoint.connect()?;
    client.batch_execute(&format!(
        "SET statement_timeout = {}; SET lock_timeout = {};",
        OWNED_STATEMENT_TIMEOUT.as_millis(),
        OWNED_LOCK_TIMEOUT.as_millis()
    ))?;
    Ok(client)
}

fn ensure_owned_publication(
    database_url: &DatabaseUrl,
    client: &mut Client,
    publication: &str,
    tables: &[QualifiedTable],
    columns: Option<&SelectedColumns>,
) -> Result<(), LocalSourceError> {
    if tables.is_empty() {
        return Err(LocalSourceError::EmptyTableSet);
    }
    let existing = client.query_opt(
        "SELECT pubinsert, pubupdate, pubdelete, pubtruncate, pubviaroot, puballtables
               FROM pg_catalog.pg_publication WHERE pubname = $1",
        &[&publication],
    )?;
    let selection = match columns {
        Some(columns) => select_publication_columns(client, publication, tables, columns)?,
        None => SelectedPublication {
            clauses: quoted_tables(tables),
            published: BTreeMap::new(),
        },
    };
    if existing.is_none() {
        if let Some(table) = first_table_not_owned(client, tables)? {
            return Err(LocalSourceError::PublicationMissing {
                publication: publication.to_owned(),
                table,
                statement: format!(
                    "CREATE PUBLICATION {} FOR TABLE {};",
                    quote_identifier(publication),
                    selection.clauses.join(", ")
                ),
            });
        }
        return match create_owned_publication(
            database_url,
            client,
            publication,
            tables,
            &selection.clauses,
        ) {
            Err(LocalSourceError::Postgres(error))
                if error.code() == Some(&SqlState::INSUFFICIENT_PRIVILEGE) =>
            {
                Err(publication_privilege_refusal(
                    database_url,
                    client,
                    publication,
                ))
            }
            created => created,
        };
    }
    validate_owned_publication(client, publication, tables)?;
    let Some(columns) = columns else {
        return Ok(());
    };
    for (table, published) in &selection.published {
        let selected = &columns[table];
        if published.iter().collect::<BTreeSet<_>>() != selected.iter().collect() {
            return Err(LocalSourceError::PublishedColumnsDiffer {
                publication: publication.to_owned(),
                table: table.display_name(),
                published: published.clone(),
                selected: selected.iter().cloned().collect(),
                statement: format!(
                    "CREATE PUBLICATION {} FOR TABLE {};",
                    quote_identifier(publication),
                    selection.clauses.join(", ")
                ),
            });
        }
    }
    Ok(())
}

/// The publication a manifest's selected columns call for.
struct SelectedPublication {
    /// The `FOR TABLE` entries that create it.
    clauses: Vec<String>,
    /// The columns the publication publishes now of each selected table, in
    /// table order, when it exists.
    published: BTreeMap<QualifiedTable, Vec<String>>,
}

/// Each table with a column list naming its selected columns in table order,
/// also when they are all of its columns, so a column added later is never
/// published. PostgreSQL refuses UPDATE and DELETE on a table with REPLICA
/// IDENTITY FULL under any column list, so such a table is published whole,
/// which it can be only when every column is selected.
fn select_publication_columns(
    client: &mut Client,
    publication: &str,
    tables: &[QualifiedTable],
    columns: &SelectedColumns,
) -> Result<SelectedPublication, LocalSourceError> {
    let mut selection = SelectedPublication {
        clauses: Vec::with_capacity(tables.len()),
        published: BTreeMap::new(),
    };
    for (table, quoted) in tables.iter().zip(quoted_tables(tables)) {
        let Some(selected) = columns.get(table) else {
            selection.clauses.push(quoted);
            continue;
        };
        // A table that does not exist is left for CREATE PUBLICATION and
        // enrollment to refuse, as without a selection.
        let Some(row) = client.query_opt(
            "SELECT class.relreplident = 'f',
                    ARRAY(SELECT attribute.attname::text
                            FROM pg_catalog.pg_attribute AS attribute
                           WHERE attribute.attrelid = class.oid
                             AND attribute.attnum > 0
                             AND NOT attribute.attisdropped
                           ORDER BY attribute.attnum),
                    CASE WHEN publication.oid IS NOT NULL THEN
                      ARRAY(SELECT attribute.attname::text
                              FROM pg_catalog.pg_attribute AS attribute
                             WHERE attribute.attrelid = class.oid
                               AND attribute.attnum > 0
                               AND NOT attribute.attisdropped
                               AND (listed.prattrs IS NULL
                                    OR attribute.attnum = ANY(listed.prattrs::int2[]))
                             ORDER BY attribute.attnum)
                    END
               FROM pg_catalog.pg_class AS class
               JOIN pg_catalog.pg_namespace AS namespace
                 ON namespace.oid = class.relnamespace
          LEFT JOIN pg_catalog.pg_publication AS publication
                 ON publication.pubname = $3
          LEFT JOIN pg_catalog.pg_publication_rel AS listed
                 ON listed.prpubid = publication.oid AND listed.prrelid = class.oid
              WHERE namespace.nspname = $1 AND class.relname = $2",
            &[&table.schema, &table.table, &publication],
        )?
        else {
            selection.clauses.push(quoted);
            continue;
        };
        let full_identity: bool = row.get(0);
        let existing: Vec<String> = row.get(1);
        if let Some(missing) = selected.iter().find(|name| !existing.contains(name)) {
            return Err(LocalSourceError::SelectedColumnMissing {
                table: table.display_name(),
                column: missing.clone(),
            });
        }
        if let Some(published) = row.get::<_, Option<Vec<String>>>(2) {
            selection.published.insert(table.clone(), published);
        }
        if full_identity {
            if existing.len() != selected.len() {
                return Err(LocalSourceError::SelectedColumnsUnderFullIdentity {
                    publication: publication.to_owned(),
                    table: table.display_name(),
                });
            }
            selection.clauses.push(quoted);
            continue;
        }
        let list = existing
            .iter()
            .filter(|name| selected.contains(*name))
            .map(|name| quote_identifier(name))
            .collect::<Vec<_>>();
        selection
            .clauses
            .push(format!("{quoted} ({})", list.join(", ")));
    }
    Ok(selection)
}

fn quoted_tables(tables: &[QualifiedTable]) -> Vec<String> {
    tables
        .iter()
        .map(|table| {
            format!(
                "{}.{}",
                quote_identifier(&table.schema),
                quote_identifier(&table.table)
            )
        })
        .collect()
}

/// The first of `tables` the connected role does not own. `CREATE
/// PUBLICATION ... FOR TABLE` needs ownership of every table it names, so
/// enrollment creates a missing publication only for a role that owns them all.
fn first_table_not_owned(
    client: &mut Client,
    tables: &[QualifiedTable],
) -> Result<Option<String>, LocalSourceError> {
    let schemas: Vec<&str> = tables.iter().map(|table| table.schema.as_str()).collect();
    let names: Vec<&str> = tables.iter().map(|table| table.table.as_str()).collect();
    Ok(client
        .query_opt(
            "SELECT format('%I.%I', namespace.nspname, class.relname)
               FROM unnest($1::name[], $2::name[])
                    WITH ORDINALITY AS required(schema_name, table_name, ordinality)
               JOIN pg_catalog.pg_namespace AS namespace
                 ON namespace.nspname = required.schema_name
               JOIN pg_catalog.pg_class AS class
                 ON class.relnamespace = namespace.oid
                AND class.relname = required.table_name
              WHERE NOT pg_catalog.pg_has_role(current_user, class.relowner, 'USAGE')
              ORDER BY required.ordinality
              LIMIT 1",
            &[&schemas, &names],
        )?
        .map(|row| row.get(0)))
}

/// What a source deserves to hear when nothing bounds the WAL its replication
/// slot holds back. While Calque is stopped, PostgreSQL keeps every WAL
/// segment from the slot's position on, until the disk fills.
pub const UNBOUNDED_SLOT_WAL_WARNING: &str = "neither max_slot_wal_keep_size nor idle_replication_slot_timeout is set on this PostgreSQL, so while Calque is stopped its replication slot keeps every WAL segment written since, until the disk fills; set one of them, as docs/postgresql.md describes under \"Protecting the primary's disk\"";

/// Whether the source leaves the WAL a slot holds back unbounded: neither
/// `max_slot_wal_keep_size` nor `idle_replication_slot_timeout` is set.
fn slot_wal_unbounded(client: &mut impl postgres::GenericClient) -> Result<bool, postgres::Error> {
    let unbounded: Option<bool> = client
        .query_one(
            "SELECT (SELECT setting::int8 < 0 FROM pg_catalog.pg_settings
                      WHERE name = 'max_slot_wal_keep_size')
                AND (SELECT setting::int8 = 0 FROM pg_catalog.pg_settings
                      WHERE name = 'idle_replication_slot_timeout')",
            &[],
        )?
        .get(0);
    Ok(unbounded == Some(true))
}

/// [`UNBOUNDED_SLOT_WAL_WARNING`] when the source at `database_url` deserves
/// it, for `calque inspect` to print.
pub fn inspect_slot_wal_bound(
    database_url: &DatabaseUrl,
) -> Result<Option<&'static str>, LocalSourceError> {
    let mut client = connect_owned(
        database_url,
        PostgresConnectionFamily::Inspect,
        "calque-inspect",
    )?;
    Ok(slot_wal_unbounded(&mut client)?.then_some(UNBOUNDED_SLOT_WAL_WARNING))
}

fn create_owned_publication(
    database_url: &DatabaseUrl,
    client: &mut Client,
    publication: &str,
    tables: &[QualifiedTable],
    clauses: &[String],
) -> Result<(), LocalSourceError> {
    let quoted = quoted_tables(tables);
    let bound = publication_lock_bound(client, tables.len())?;
    // A lock wait sends nothing until it ends, so its replies get a
    // connection bounded past the longest wait.
    let mut creator = connect_owned_waiting(
        database_url,
        PostgresConnectionFamily::Enroll,
        "calque-enroll",
        bound + Duration::from_secs(1),
    )?;
    let deadline = Instant::now() + bound;
    let mut transaction = creator.transaction()?;
    // Every table that is free is held before any wait begins. An
    // autovacuum cancelled on one table moves on to the next in its list,
    // but skips a table it cannot lock at once, so it cannot reach an
    // enrolled table ahead of enrollment and cost a second cancellation.
    let mut held_elsewhere = Vec::new();
    for (table, quoted_table) in tables.iter().zip(&quoted) {
        if !try_lock_for_publication(&mut transaction, quoted_table)? {
            held_elsewhere.push((table, quoted_table));
        }
    }
    for (table, quoted_table) in held_elsewhere {
        lock_for_publication(&mut transaction, table, quoted_table, deadline, bound)?;
    }
    // Every table is held by now. CREATE PUBLICATION goes back to the
    // connection's own timeouts, not what was left of the last wait,
    // which can be a millisecond.
    transaction.batch_execute(&format!(
        "SET LOCAL statement_timeout = {}; SET LOCAL lock_timeout = {}; \
         CREATE PUBLICATION {} FOR TABLE {}",
        OWNED_STATEMENT_TIMEOUT.as_millis(),
        OWNED_LOCK_TIMEOUT.as_millis(),
        quote_identifier(publication),
        clauses.join(", ")
    ))?;
    transaction.commit()?;
    Ok(())
}

/// What PostgreSQL's `permission denied` leaves out when enrollment cannot
/// create the publication of tables its role owns: CREATE on the database,
/// which `CREATE PUBLICATION` needs beside ownership.
fn publication_privilege_refusal(
    database_url: &DatabaseUrl,
    client: &mut Client,
    publication: &str,
) -> LocalSourceError {
    let refusal = PostgresRefusal::InsufficientPrivilege;
    let diagnosis = client.query_one(
        "SELECT current_user::text,
                pg_catalog.current_database()::text,
                pg_catalog.has_database_privilege(
                    current_user, pg_catalog.current_database(), 'CREATE')",
        &[],
    );
    let advice = match diagnosis {
        Ok(row) if !row.get::<_, bool>(2) => {
            let role = quote_identifier(&row.get::<_, String>(0));
            let database = quote_identifier(&row.get::<_, String>(1));
            format!(
                "role {role} cannot create publication {} on database {database}: it lacks CREATE on the database; run GRANT CREATE ON DATABASE {database} TO {role}",
                quote_identifier(publication)
            )
        }
        _ => SourceContext::of(database_url, None).advice(refusal),
    };
    LocalSourceError::SourceRefused { refusal, advice }
}

/// How long enrollment waits, in total, for the table locks `CREATE
/// PUBLICATION` takes.
///
/// `CREATE PUBLICATION ... FOR TABLE` takes SHARE UPDATE EXCLUSIVE on each
/// table, which conflicts with the lock every VACUUM and ANALYZE holds, so a
/// freshly loaded table under autovacuum makes it wait. PostgreSQL cancels a
/// blocking autovacuum once the waiter has waited `deadlock_timeout`. Because
/// enrollment holds every free table before it waits, only the tables under
/// autovacuum at that moment cost a cancellation, and at most
/// `autovacuum_max_workers` of them can be. Both settings are read from the
/// server.
fn publication_lock_bound(
    client: &mut Client,
    tables: usize,
) -> Result<Duration, LocalSourceError> {
    let settings = client.query_one(
        "SELECT (SELECT setting::int8 FROM pg_catalog.pg_settings
                  WHERE name = 'deadlock_timeout'),
                (SELECT setting::int8 FROM pg_catalog.pg_settings
                  WHERE name = 'autovacuum_max_workers')",
        &[],
    )?;
    let deadlock_timeout_millis: i64 = settings.try_get(0)?;
    let autovacuum_max_workers: i64 = settings.try_get(1)?;
    Ok(publication_lock_bound_for(
        Duration::from_millis(u64::try_from(deadlock_timeout_millis).unwrap_or(0)),
        usize::try_from(autovacuum_max_workers).unwrap_or(0),
        tables,
    ))
}

/// One cancellation, `deadlock_timeout` plus a second of margin, for each
/// table that can be under autovacuum at once, and at least ten seconds.
///
/// The total is derived rather than capped: a fixed cap below that product
/// would fail enrollment in the case the wait exists for. The floor leaves room
/// for a loaded server and for a short holder that is never cancelled, such as
/// a manual ANALYZE. A manual VACUUM, DDL and an autovacuum that prevents
/// wraparound are never cancelled either, so the bound ends those waits with
/// `PublicationLockTimeout`.
fn publication_lock_bound_for(
    deadlock_timeout: Duration,
    autovacuum_max_workers: usize,
    tables: usize,
) -> Duration {
    const FLOOR: Duration = Duration::from_secs(10);
    const AUTOVACUUM_CANCEL_MARGIN: Duration = Duration::from_secs(1);
    let cancellations = u32::try_from(tables.min(autovacuum_max_workers)).unwrap_or(u32::MAX);
    FLOOR.max((deadlock_timeout + AUTOVACUUM_CANCEL_MARGIN).saturating_mul(cancellations))
}

/// Takes the lock `CREATE PUBLICATION` takes on `quoted_table` if nothing
/// holds a conflicting one, and reports whether it did.
fn try_lock_for_publication(
    transaction: &mut postgres::Transaction<'_>,
    quoted_table: &str,
) -> Result<bool, LocalSourceError> {
    // A refused NOWAIT aborts the transaction; rolling back to the savepoint
    // keeps the locks already held. PostgreSQL still logs the refusal as an
    // ERROR, one line per table found under autovacuum.
    let mut attempt = transaction.savepoint("calque_publication_lock")?;
    match attempt.batch_execute(&format!(
        "LOCK TABLE {quoted_table} IN SHARE UPDATE EXCLUSIVE MODE NOWAIT"
    )) {
        Ok(()) => {
            attempt.commit()?;
            Ok(true)
        }
        Err(error) if error.code() == Some(&SqlState::LOCK_NOT_AVAILABLE) => {
            attempt.rollback()?;
            Ok(false)
        }
        Err(error) => Err(error.into()),
    }
}

/// Takes the lock `CREATE PUBLICATION` takes on `table`, waiting no later
/// than `deadline`.
///
/// Locking one table per statement names the table a timeout was waiting
/// for. A waiting SHARE UPDATE EXCLUSIVE does not queue the application's
/// reads or writes; only VACUUM, ANALYZE, CREATE INDEX and DDL on that table
/// queue behind it. DDL on a table already held waits until the publication is
/// created or the bound ends the wait, and the application's queries on that
/// table then queue behind the DDL.
fn lock_for_publication(
    transaction: &mut postgres::Transaction<'_>,
    table: &QualifiedTable,
    quoted_table: &str,
    deadline: Instant,
    bound: Duration,
) -> Result<(), LocalSourceError> {
    // A lock wait also counts against `statement_timeout`, so that sits a
    // second past `lock_timeout` for the length of the wait. Zero would
    // disable `lock_timeout`, so the last sub-millisecond rounds up, and both
    // settings must fit PostgreSQL's 32-bit millisecond range.
    let lock_millis = deadline
        .saturating_duration_since(Instant::now())
        .as_millis()
        .clamp(1, i32::MAX as u128 - 1_000);
    transaction.batch_execute(&format!(
        "SET LOCAL lock_timeout = {lock_millis}; SET LOCAL statement_timeout = {};",
        lock_millis + 1_000
    ))?;
    match transaction.batch_execute(&format!(
        "LOCK TABLE {quoted_table} IN SHARE UPDATE EXCLUSIVE MODE"
    )) {
        Ok(()) => Ok(()),
        Err(error) if error.code() == Some(&SqlState::LOCK_NOT_AVAILABLE) => {
            Err(LocalSourceError::PublicationLockTimeout {
                table: table.display_name(),
                bound,
            })
        }
        Err(error) => Err(error.into()),
    }
}

fn validate_owned_publication(
    client: &mut impl postgres::GenericClient,
    publication: &str,
    tables: &[QualifiedTable],
) -> Result<(), LocalSourceError> {
    // Enrollment's inspection refuses a row filter or column list as well,
    // but as a source that is not ready, which names no remedy.
    let tables = tables.iter().cloned().collect::<BTreeSet<_>>();
    match inspect_owned_publication(client, publication, &tables)? {
        OwnedPublication::Matches { .. } => Ok(()),
        OwnedPublication::Missing | OwnedPublication::Drifted => {
            Err(LocalSourceError::PublicationDrift {
                publication: publication.to_owned(),
            })
        }
    }
}

#[derive(Debug, Error)]
pub enum LocalSourceError {
    #[error("PostgreSQL source operation failed: {0}")]
    Postgres(#[from] postgres::Error),
    /// PostgreSQL refused the role, its privileges or the database, which no
    /// retry gets past; `advice` says what to change.
    #[error("{advice}")]
    SourceRefused {
        refusal: PostgresRefusal,
        advice: String,
    },
    #[error(transparent)]
    Guard(#[from] crate::GuardError),
    #[error(transparent)]
    Inspect(#[from] source_control::InspectError),
    #[error(transparent)]
    Registry(#[from] source_control::SchemaRegistryError),
    #[error(transparent)]
    Policy(#[from] authz::PolicyError),
    #[error(transparent)]
    Cursor(#[from] sync_types::CursorError),
    #[error(transparent)]
    Fence(#[from] FenceError),
    #[error(transparent)]
    Runtime(#[from] SourceRuntimeError),
    #[error(transparent)]
    Ingest(#[from] source_runtime::PersistentIngestError),
    #[error(transparent)]
    Canonical(#[from] canonical_log::LogError),
    #[error("database URL could not be converted to a replication URL: {0}")]
    Url(#[from] url::ParseError),
    #[error("source identity contains an invalid timeline")]
    InvalidIdentity,
    // `SourceIdentityChanged`, `RegistryStateMismatch`,
    // `EnrolledByAnotherState`, `PublicationDrift` and `SlotOwnershipMismatch`
    // state the refusal alone, as a sentence. The command that met one names
    // the remedy after it, since only the command knows the state directory
    // and the variable it read the source from.
    #[error("connected PostgreSQL identity changed from persisted local state.")]
    SourceIdentityChanged,
    #[error("persisted schema registry does not match local state.")]
    RegistryStateMismatch,
    /// The lease PostgreSQL holds for this state's names carries another
    /// source epoch. The names derive from the state directory's path and the
    /// source, so a state directory deleted and made anew at the same path
    /// meets the enrollment the old one left.
    #[error("this source was enrolled by another state directory at the same path.")]
    EnrolledByAnotherState,
    #[error("the configured table set is empty")]
    EmptyTableSet,
    #[error("the PostgreSQL publication {publication} was altered, or an enum its columns use gained or renamed a value: Calque needs it to publish exactly the enrolled tables, for insert, update, delete and truncate, without publish_via_partition_root or row filters, with the column lists and enum values it had at enrollment.")]
    PublicationDrift { publication: String },
    #[error("the PostgreSQL publication {publication} does not exist, and Calque's role cannot create it because it does not own {table}. Have the tables' owner create it: {statement}")]
    PublicationMissing {
        publication: String,
        table: String,
        statement: String,
    },
    #[error("the PostgreSQL publication {publication} publishes columns {published} of {table}, and the manifest selects {selected}; Calque publishes exactly the selected columns, as {statement}", published = .published.join(", "), selected = .selected.join(", "))]
    PublishedColumnsDiffer {
        publication: String,
        table: String,
        published: Vec<String>,
        selected: Vec<String>,
        statement: String,
    },
    #[error("the manifest selects column {column} of {table}, which has no such column.")]
    SelectedColumnMissing { table: String, column: String },
    #[error("the manifest selects some columns of {table}, whose replica identity is FULL, and PostgreSQL refuses UPDATE and DELETE on such a table under any column list; select every column of {table}, or run ALTER TABLE {table} REPLICA IDENTITY DEFAULT so that publication {publication} can list only the selected ones.")]
    SelectedColumnsUnderFullIdentity { publication: String, table: String },
    #[error("a lock on {table} was not granted within {bound:?}, so the publication was not created; another session holds a conflicting lock on it (VACUUM, ANALYZE, CREATE INDEX or other DDL, or an autovacuum preventing wraparound, which PostgreSQL never cancels); retry once that finishes")]
    PublicationLockTimeout { table: String, bound: Duration },
    #[error("the logical replication slot no longer matches local state.")]
    SlotOwnershipMismatch,
    #[error("the local source is still owned by an active runtime; stop the calque process that serves it, wait for it to exit, then retry")]
    ActiveRuntime,
    #[error("persistent CDC thread could not start: {0}")]
    ThreadStart(std::io::Error),
    #[error("persistent CDC thread panicked")]
    IngestThreadPanicked,
    #[error("persistent CDC thread panicked after its loop stopped")]
    IngestThreadPanickedAfterLoop,
    #[error("persistent CDC did not initialize before its bounded startup deadline")]
    CdcStartupTimeout,
    #[error("persistent CDC startup readiness handshake failed")]
    CdcStartupHandshake,
    #[error("persistent CDC startup was cancelled before readiness")]
    CdcStartupCancelled,
    #[error("persistent CDC session cleanup failed")]
    SessionCleanupFailed,
    /// Closing the session failed after `0` stopped startup or the CDC loop.
    /// The cause stays `0`, so this needs whatever remedy `0` needs.
    #[error("persistent CDC session cleanup failed after persistent CDC failed: {0}")]
    SessionCleanupFailedAfter(Box<LocalSourceError>),
}

impl LocalSourceError {
    /// Whether only `calque destroy` gets past this refusal: a canonical log
    /// an earlier release wrote, whether connecting the log refused it or an
    /// ingest step met it, a schema registry that does not match the state
    /// beside it, a source another state directory enrolled, a publication
    /// whose columns are not the selected ones, which destroy drops so the
    /// next start creates it with them, or a replication slot that was
    /// dropped or that PostgreSQL invalidated, since the changes it held
    /// cannot be replayed and the working set has to be prepared anew. A
    /// failed session cleanup after one of them needs it too, since the cause
    /// stands.
    pub fn requires_destroying_state(&self) -> bool {
        match self {
            Self::Canonical(error)
            | Self::Ingest(
                PersistentIngestError::CanonicalLog(error)
                | PersistentIngestError::Ingest(IngestError::Append(AppendFailure::CanonicalLog(
                    error,
                ))),
            ) => error.requires_destroying_state(),
            Self::Runtime(
                SourceRuntimeError::SlotMissing { .. } | SourceRuntimeError::SlotLost { .. },
            )
            | Self::RegistryStateMismatch
            | Self::EnrolledByAnotherState
            | Self::PublishedColumnsDiffer { .. } => true,
            Self::SessionCleanupFailedAfter(cause) => cause.requires_destroying_state(),
            _ => false,
        }
    }

    /// Whether this is the publication no longer having its enrolled
    /// definition, or an enum its columns use having changed. Restoring the
    /// definition and then `calque destroy` gets past it. A start meets it
    /// during enrollment and a reconnect when it opens a session. A failed session cleanup after it
    /// is one too, since the cause stands.
    pub fn publication_drifted(&self) -> bool {
        match self {
            Self::PublicationDrift { .. } => true,
            Self::SessionCleanupFailedAfter(cause) => cause.publication_drifted(),
            _ => false,
        }
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use super::*;
    use crate::{LocalNames, SecretToken};

    /// The state directory the runtimes these tests start serve.
    fn state_dir() -> &'static Path {
        Path::new("/srv/calque/state")
    }

    fn live_database_url() -> Option<String> {
        std::env::var("CALQUE_SOURCE_DATABASE_URL")
            .ok()
            .or_else(|| std::env::var("TEST_DATABASE_URL").ok())
    }

    /// A request on a connection to the source whose server stops answering,
    /// such as a mutation's, fails once the connection's reply bound runs out
    /// instead of holding its caller on the socket.
    #[test]
    #[ignore = "requires CALQUE_SOURCE_DATABASE_URL or TEST_DATABASE_URL"]
    fn a_source_request_whose_server_stops_answering_fails_at_its_reply_bound() {
        let url = live_database_url().expect("live source database URL");
        let relay = postgres_connect::relay::SilenceableRelay::start(&url);
        let mut client = connect_owned(
            &DatabaseUrl::parse(relay.url()).unwrap(),
            PostgresConnectionFamily::MutationWriter,
            "calque-mutations",
        )
        .unwrap();
        client.batch_execute("SELECT 1").unwrap();
        relay.silence();
        let (sender, receiver) = mpsc::channel();
        thread::spawn(move || {
            let started = Instant::now();
            let failed = client.batch_execute("SELECT 1").is_err();
            let _ = sender.send((failed, started.elapsed()));
        });
        let (failed, elapsed) = receiver
            .recv_timeout(Duration::from_secs(15))
            .expect("the request on a silent socket had not returned after 15 s");
        assert!(failed);
        assert!(
            elapsed >= Duration::from_secs(5) && elapsed < Duration::from_secs(8),
            "the request returned after {elapsed:?}"
        );
    }

    /// A refusal whose remedy names the state directory or the source's
    /// variable states the refusal alone, as a sentence the command's remedy
    /// can follow. One a retry gets past names the retry.
    #[test]
    fn lifecycle_refusals_name_the_supported_recovery() {
        assert_eq!(
            LocalSourceError::SourceIdentityChanged.to_string(),
            "connected PostgreSQL identity changed from persisted local state."
        );
        assert_eq!(
            LocalSourceError::RegistryStateMismatch.to_string(),
            "persisted schema registry does not match local state."
        );
        assert_eq!(
            LocalSourceError::PublicationDrift {
                publication: "calque_0123456789abcdef01234567".to_owned(),
            }
            .to_string(),
            "the PostgreSQL publication calque_0123456789abcdef01234567 was altered, or an enum its columns use gained or renamed a value: Calque needs it to publish exactly the enrolled tables, for insert, update, delete and truncate, without publish_via_partition_root or row filters, with the column lists and enum values it had at enrollment."
        );
        assert_eq!(
            LocalSourceError::SlotOwnershipMismatch.to_string(),
            "the logical replication slot no longer matches local state."
        );
        assert_eq!(
            LocalSourceError::ActiveRuntime.to_string(),
            "the local source is still owned by an active runtime; stop the calque process that serves it, wait for it to exit, then retry"
        );
        assert_eq!(
            LocalSourceError::PublicationLockTimeout {
                table: "incidents.events".to_owned(),
                bound: Duration::from_secs(14),
            }
            .to_string(),
            "a lock on incidents.events was not granted within 14s, so the publication was not created; another session holds a conflicting lock on it (VACUUM, ANALYZE, CREATE INDEX or other DDL, or an autovacuum preventing wraparound, which PostgreSQL never cancels); retry once that finishes"
        );
    }

    #[test]
    fn publication_lock_bound_covers_one_cancellation_per_autovacuum_worker() {
        let second = Duration::from_secs(1);
        // At the defaults the floor governs, however many tables there are.
        assert_eq!(
            publication_lock_bound_for(second, 3, 1),
            Duration::from_secs(10)
        );
        assert_eq!(
            publication_lock_bound_for(second, 3, 100),
            Duration::from_secs(10)
        );
        // Past the floor, the fewer of tables and workers counts.
        assert_eq!(
            publication_lock_bound_for(Duration::from_secs(5), 3, 100),
            Duration::from_secs(18)
        );
        assert_eq!(
            publication_lock_bound_for(Duration::from_secs(5), 10, 2),
            Duration::from_secs(12)
        );
        assert_eq!(
            publication_lock_bound_for(Duration::from_millis(1_500), 8, 100),
            Duration::from_secs(20)
        );
    }

    #[test]
    fn replication_url_fully_owns_tcp_routing_and_launch_mode() {
        let database = DatabaseUrl::parse("postgresql://127.0.0.1/app?sslmode=disable").unwrap();
        let url = replication_database_url_with_environment(&database, |_| None).unwrap();
        let parsed = url::Url::parse(&url).unwrap();
        let query = parsed.query_pairs().collect::<BTreeMap<_, _>>();
        assert_eq!(parsed.host_str(), Some("127.0.0.1"));
        assert_eq!(parsed.port(), Some(5432));
        assert_eq!(
            query.get("hostaddr").map(|value| value.as_ref()),
            Some("127.0.0.1")
        );
        assert_eq!(
            query.get("connect_timeout").map(|value| value.as_ref()),
            Some("5")
        );
        assert_eq!(
            query.get("sslmode").map(|value| value.as_ref()),
            Some("disable")
        );
        assert_eq!(
            query.get("replication").map(|value| value.as_ref()),
            Some("database")
        );
        assert_eq!(
            query.get("application_name").map(|value| value.as_ref()),
            Some("calque")
        );
        assert_eq!(
            query.get("options").map(|value| value.as_ref()),
            Some(
                "-c TimeZone=UTC -c DateStyle=ISO,MDY -c bytea_output=hex -c extra_float_digits=1"
            )
        );
        assert!(
            !url.contains('+') && url.contains("options=%2Dc%20TimeZone%3DUTC%20%2Dc%20"),
            "libpq reads a `+` in the URL literally: {url}"
        );
    }

    /// A remote host reaches libpq with the guard's TLS parameters and an
    /// empty `hostaddr`, so libpq resolves the name it then verifies.
    #[test]
    fn replication_url_carries_verified_tls_to_a_host_name() {
        let directory = tempfile::tempdir().unwrap();
        let key = rcgen::KeyPair::generate().unwrap();
        let mut params = rcgen::CertificateParams::new(Vec::<String>::new()).unwrap();
        params.is_ca = rcgen::IsCa::Ca(rcgen::BasicConstraints::Unconstrained);
        let root = directory.path().join("ca.pem");
        std::fs::write(&root, params.self_signed(&key).unwrap().pem()).unwrap();
        let database = DatabaseUrl::parse(&format!(
            "postgresql://calque@db.example.com/app?sslrootcert={}",
            root.to_str().unwrap().replace('/', "%2F")
        ))
        .unwrap();
        let parsed = url::Url::parse(
            &replication_database_url_with_environment(&database, |_| None).unwrap(),
        )
        .unwrap();
        let query = parsed.query_pairs().collect::<BTreeMap<_, _>>();
        assert_eq!(parsed.host_str(), Some("db.example.com"));
        assert_eq!(query.get("hostaddr").map(|value| value.as_ref()), Some(""));
        assert_eq!(
            query.get("sslmode").map(|value| value.as_ref()),
            Some("verify-full")
        );
        assert_eq!(
            query.get("sslrootcert").map(|value| value.as_ref()),
            std::fs::canonicalize(&root).unwrap().to_str()
        );
        assert_eq!(
            query.get("replication").map(|value| value.as_ref()),
            Some("database")
        );
    }

    #[test]
    fn replication_url_fully_owns_unix_socket_routing() {
        let database = DatabaseUrl::parse("postgresql:///app?host=%2Ftmp").unwrap();
        let parsed = url::Url::parse(
            &replication_database_url_with_environment(&database, |_| None).unwrap(),
        )
        .unwrap();
        let query = parsed.query_pairs().collect::<BTreeMap<_, _>>();
        assert_eq!(parsed.host(), None);
        assert_eq!(
            query.get("host").map(|value| value.as_ref()),
            std::fs::canonicalize("/tmp").unwrap().to_str()
        );
        assert_eq!(query.get("hostaddr").map(|value| value.as_ref()), Some(""));
        assert_eq!(query.get("port").map(|value| value.as_ref()), Some("5432"));
        assert_eq!(
            query.get("connect_timeout").map(|value| value.as_ref()),
            Some("5")
        );
    }

    #[test]
    fn identifiers_are_always_quoted() {
        assert_eq!(quote_identifier("tasks"), "\"tasks\"");
        assert_eq!(quote_identifier("a\"b"), "\"a\"\"b\"");
    }

    #[test]
    fn secret_type_remains_usable_without_exposure() {
        let token = SecretToken::generate().unwrap();
        assert_eq!(token.as_bytes().len(), 32);
        assert!(!format!("{token:?}").contains(&token.to_urlsafe()));
    }

    #[test]
    fn cdc_health_closes_before_owner_cleanup_or_exit() {
        let healthy = Arc::new(AtomicBool::new(false));
        let current = Arc::new(AtomicBool::new(false));
        let connected = Arc::new(AtomicBool::new(false));
        let failure = Arc::new(StdMutex::new(None));
        let guard = HealthExitGuard {
            healthy: Arc::clone(&healthy),
            current: Arc::clone(&current),
            connected: Arc::clone(&connected),
            failure: Arc::clone(&failure),
        };
        let health = LocalSourceHealth {
            healthy: Arc::clone(&healthy),
            current,
            connected,
            failure,
        };

        assert!(!healthy.load(Ordering::Acquire));
        healthy.store(true, Ordering::Release);
        assert!(healthy.load(Ordering::Acquire));
        guard.record_failure("cdc-runtime-error");
        assert!(!healthy.load(Ordering::Acquire));
        assert_eq!(health.failure().as_deref(), Some("cdc-runtime-error"));

        // Keeping the guard alive represents cleanup that has not completed.
        // Readiness must already be closed during that interval.
        assert!(!healthy.load(Ordering::Acquire));
        drop(guard);
        assert!(!healthy.load(Ordering::Acquire));
    }

    fn guard_and_health(healthy: bool) -> (HealthExitGuard, LocalSourceHealth) {
        let health = LocalSourceHealth::for_test(healthy);
        let guard = HealthExitGuard {
            healthy: Arc::clone(&health.healthy),
            current: Arc::clone(&health.current),
            connected: Arc::clone(&health.connected),
            failure: Arc::clone(&health.failure),
        };
        (guard, health)
    }

    /// The loop's cause says what gets past it, and a session cleanup that
    /// fails afterwards is recorded beside that cause, keeping it and its
    /// remedy. Any other stop, and a cleanup failure with no cause before it,
    /// leaves a rerun as the remedy.
    #[test]
    fn a_failed_cleanup_keeps_the_cause_the_loop_stopped_on() {
        let refusal = LocalSourceError::Canonical(LogError::EarlierEntryFormat {
            state_dir: state_dir().to_path_buf(),
        });
        let (guard, health) = guard_and_health(true);
        assert_eq!(health.failure_remedy(), CdcStopRemedy::Rerun);
        guard.record(HealthFailure::stopped_on(
            &refusal,
            &SourceContext::default(),
        ));
        assert!(!health.is_healthy());
        assert_eq!(
            health.failure(),
            Some(format!("persistent CDC failed: {refusal}"))
        );
        assert_eq!(health.failure_remedy(), CdcStopRemedy::DestroyState);

        guard.record_cleanup_failure();
        drop(guard);
        assert_eq!(
            health.failure().as_deref(),
            Some("persistent CDC failed: /srv/calque/state serves a canonical log an earlier release of calque wrote, and calque does not migrate it; cdc-runtime-cleanup")
        );
        assert_eq!(health.failure_remedy(), CdcStopRemedy::DestroyState);

        let drift = LocalSourceError::PublicationDrift {
            publication: "calque_pub".to_owned(),
        };
        let (guard, health) = guard_and_health(true);
        guard.record(HealthFailure::stopped_on(&drift, &SourceContext::default()));
        assert_eq!(
            health.failure(),
            Some(format!("persistent CDC failed: {drift}"))
        );
        assert_eq!(health.failure_remedy(), CdcStopRemedy::RestorePublication);
        guard.record_cleanup_failure();
        assert_eq!(health.failure_remedy(), CdcStopRemedy::RestorePublication);

        let (guard, health) = guard_and_health(true);
        guard.record(HealthFailure::stopped_on(
            &LocalSourceError::Canonical(LogError::PostgresUnavailable),
            &SourceContext::default(),
        ));
        guard.record_cleanup_failure();
        assert_eq!(health.failure_remedy(), CdcStopRemedy::Rerun);

        let (guard, health) = guard_and_health(true);
        guard.record_cleanup_failure();
        assert_eq!(health.failure().as_deref(), Some("cdc-runtime-cleanup"));
        assert_eq!(health.failure_remedy(), CdcStopRemedy::Rerun);
    }

    /// A session that finds the publication drifted is refused as enrollment
    /// refuses it, naming the publication the registry enrolled; the
    /// runtime's other refusals pass through unchanged.
    #[test]
    fn a_session_refuses_a_drifted_publication_as_enrollment_does() {
        let prepared = crate::snapshot::tests::prepared_source(
            sync_types::SourceEpoch::new("epoch-drift").unwrap(),
        );
        let error = session_error(SourceRuntimeError::PublicationDrift, &prepared.activated);
        assert!(
            matches!(
                &error,
                LocalSourceError::PublicationDrift { publication }
                    if publication == "calque_engine_test"
            ),
            "{error:?}"
        );
        assert!(error.publication_drifted());
        assert!(LocalSourceError::SessionCleanupFailedAfter(Box::new(error)).publication_drifted());

        let error = session_error(SourceRuntimeError::PublicationMissing, &prepared.activated);
        assert!(
            matches!(
                error,
                LocalSourceError::Runtime(SourceRuntimeError::PublicationMissing)
            ),
            "{error:?}"
        );
        assert!(!error.publication_drifted());
    }

    /// A stop writes its failure before it closes readiness, so a caller that
    /// sees the runtime unhealthy reads why. While the test holds the slot,
    /// the stop cannot get as far as closing readiness.
    #[test]
    fn a_stop_writes_its_failure_before_closing_readiness() {
        let (guard, health) = guard_and_health(true);
        let held = health.failure.lock().unwrap();
        let stopping = thread::spawn(move || guard.record_failure("cdc-runtime-stopped"));
        thread::sleep(Duration::from_millis(50));
        assert!(health.is_healthy());
        drop(held);
        stopping.join().unwrap();
        assert!(!health.is_healthy());
        assert_eq!(health.failure().as_deref(), Some("cdc-runtime-stopped"));
    }

    /// A thread that panics after its loop reaches the join as a variant of
    /// its own, apart from the loop's own panic, which the thread returns as
    /// `IngestThreadPanicked`. A thread that returns its result is joined to
    /// it.
    #[test]
    fn a_panic_after_the_loop_is_told_apart_from_the_loops_own() {
        let after_loop = thread::spawn(|| -> Result<(), LocalSourceError> {
            panic!("closing the session panicked")
        });
        assert!(matches!(
            join_cdc_thread(after_loop),
            Err(LocalSourceError::IngestThreadPanickedAfterLoop)
        ));
        let in_loop = thread::spawn(|| Err(LocalSourceError::IngestThreadPanicked));
        assert!(matches!(
            join_cdc_thread(in_loop),
            Err(LocalSourceError::IngestThreadPanicked)
        ));
        assert!(join_cdc_thread(thread::spawn(|| Ok(()))).is_ok());
    }

    #[test]
    fn successful_idle_cycle_is_current_despite_unpublished_wal() {
        let (guard, health) = guard_and_health(true);
        health.current.store(false, Ordering::Release);

        guard.observe_cycle(true, Duration::ZERO);

        assert!(health.is_current());
    }

    /// A cycle that left messages waiting throughout, or decoded a commit
    /// sent more than the bound after it, leaves the source behind, so not
    /// ready, but still connected, so `/v1/` keeps being served. A cycle
    /// that caught up with commits sent within the bound makes it ready
    /// again, however many transactions it took.
    #[test]
    fn a_source_is_current_only_while_it_catches_up_with_commits_sent_promptly() {
        let (guard, health) = guard_and_health(true);

        guard.observe_cycle(false, Duration::ZERO);
        assert!(!health.is_current());
        assert!(health.is_connected());

        guard.observe_cycle(true, CURRENT_SEND_DELAY_BOUND);
        assert!(health.is_current());

        guard.observe_cycle(true, CURRENT_SEND_DELAY_BOUND + Duration::from_micros(1));
        assert!(!health.is_current());
        assert!(health.is_connected());

        guard.observe_cycle(true, Duration::from_millis(3));
        assert!(health.is_current());
        assert!(health.is_connected());
    }

    /// A session being re-established is neither ready nor served, and its
    /// first successful cycle restores service, even one that has not caught
    /// up.
    #[test]
    fn a_reconnecting_session_is_not_served_until_its_first_cycle() {
        let (guard, health) = guard_and_health(true);

        guard.mark_reconnecting(&LocalSourceError::IngestThreadPanicked);
        assert!(!health.is_current());
        assert!(!health.is_connected());

        guard.observe_cycle(false, Duration::ZERO);
        assert!(!health.is_current());
        assert!(health.is_connected());
    }

    /// Whatever stops the runtime closes both, and a stopped runtime is not
    /// served even if its last cycle succeeded.
    #[test]
    fn a_stopped_runtime_is_neither_ready_nor_served() {
        for stop in [
            (|guard: &HealthExitGuard| guard.record_failure("cdc-runtime-error"))
                as fn(&HealthExitGuard),
            |guard: &HealthExitGuard| guard.record_cleanup_failure(),
        ] {
            let (guard, health) = guard_and_health(true);
            stop(&guard);
            assert!(!health.is_current());
            assert!(!health.is_connected());
        }
        let (guard, health) = guard_and_health(true);
        drop(guard);
        assert!(!health.is_current());
        assert!(!health.is_connected());
    }

    #[test]
    fn cdc_failures_retry_only_lost_connections() {
        use CdcFailureClass::{Fatal, Transient};

        let source_id = sync_types::SourceId::new("classified-source").unwrap();
        let epoch = |value: &str| sync_types::SourceEpoch::new(value).unwrap();
        let ingest = LocalSourceError::Ingest;
        let appended = |error: LogError| {
            LocalSourceError::Ingest(PersistentIngestError::Ingest(IngestError::Append(
                AppendFailure::CanonicalLog(error),
            )))
        };
        let ingest_fence = |error: IngestFenceError| {
            LocalSourceError::Ingest(PersistentIngestError::Ingest(IngestError::Fence(error)))
        };
        let cases = [
            // The replication or control connection went away mid-session.
            (ingest(PersistentIngestError::SessionClosed), Transient),
            (
                ingest(PersistentIngestError::ReplicationUnavailable),
                Transient,
            ),
            (
                ingest(PersistentIngestError::ReplicationSilent(
                    Duration::from_secs(60),
                )),
                Transient,
            ),
            (
                ingest(PersistentIngestError::MutationReceiptUnavailable),
                Transient,
            ),
            (
                ingest(PersistentIngestError::Fence(
                    FenceError::ControlStoreUnavailable,
                )),
                Transient,
            ),
            (
                ingest(PersistentIngestError::Fence(FenceError::LeaseLost)),
                Transient,
            ),
            (
                ingest(PersistentIngestError::Fence(FenceError::LeaseRowLocked)),
                Transient,
            ),
            (
                ingest(PersistentIngestError::CanonicalLog(
                    LogError::PostgresUnavailable,
                )),
                Transient,
            ),
            (
                ingest(PersistentIngestError::CanonicalLog(
                    LogError::FencedWriterRejected,
                )),
                Transient,
            ),
            (appended(LogError::PostgresUnavailable), Transient),
            (appended(LogError::FencedWriterRejected), Transient),
            (
                ingest_fence(IngestFenceError::NoActiveLease(source_id.clone())),
                Transient,
            ),
            // Reconnect attempts while PostgreSQL restarts.
            (
                LocalSourceError::Fence(FenceError::ControlStoreUnavailable),
                Transient,
            ),
            (LocalSourceError::Fence(FenceError::LeaseLost), Transient),
            (
                LocalSourceError::Fence(FenceError::LeaseRowLocked),
                Transient,
            ),
            (
                LocalSourceError::Runtime(SourceRuntimeError::SourceControlUnavailable),
                Transient,
            ),
            (
                LocalSourceError::Runtime(SourceRuntimeError::ReplicationUnavailable),
                Transient,
            ),
            (
                LocalSourceError::Runtime(SourceRuntimeError::BackendRevocationFailed(7)),
                Transient,
            ),
            (
                LocalSourceError::Runtime(SourceRuntimeError::SlotDidNotBecomeInactive),
                Transient,
            ),
            (
                LocalSourceError::Runtime(SourceRuntimeError::Fence(
                    FenceError::ControlStoreUnavailable,
                )),
                Transient,
            ),
            (
                LocalSourceError::Runtime(SourceRuntimeError::Fence(FenceError::LeaseLost)),
                Transient,
            ),
            (
                LocalSourceError::Canonical(LogError::PostgresUnavailable),
                Transient,
            ),
            // Another owner, or a durable writer fence it claimed.
            (LocalSourceError::Fence(FenceError::LeaseSuperseded), Fatal),
            (
                LocalSourceError::Runtime(SourceRuntimeError::Fence(FenceError::LeaseSuperseded)),
                Fatal,
            ),
            (
                ingest(PersistentIngestError::Fence(FenceError::LeaseSuperseded)),
                Fatal,
            ),
            (
                LocalSourceError::Fence(FenceError::LeaseHeld {
                    expires_at_unix_millis: 1,
                }),
                Fatal,
            ),
            (
                ingest(PersistentIngestError::Fence(
                    FenceError::DurableLogFenceRejected,
                )),
                Fatal,
            ),
            (
                LocalSourceError::Runtime(SourceRuntimeError::Fence(
                    FenceError::DurableLogFenceRejected,
                )),
                Fatal,
            ),
            (
                LocalSourceError::Runtime(SourceRuntimeError::SlotBackendBindingMismatch {
                    expected: 1,
                    actual: Some(2),
                }),
                Fatal,
            ),
            (
                LocalSourceError::Runtime(SourceRuntimeError::BackendOwnershipChanged),
                Fatal,
            ),
            (
                ingest_fence(IngestFenceError::StaleLease {
                    expected: 2,
                    actual: 1,
                }),
                Fatal,
            ),
            (ingest_fence(IngestFenceError::AuthorityPoisoned), Fatal),
            // The source is no longer the one this runtime enrolled.
            (
                LocalSourceError::Fence(FenceError::EpochMismatch {
                    expected: epoch("epoch-a"),
                    actual: epoch("epoch-b"),
                }),
                Fatal,
            ),
            (
                ingest_fence(IngestFenceError::SourceEpochChanged {
                    expected: epoch("epoch-a"),
                    actual: epoch("epoch-b"),
                }),
                Fatal,
            ),
            (
                LocalSourceError::Fence(FenceError::SourceNotEnrolled(source_id)),
                Fatal,
            ),
            (
                LocalSourceError::Runtime(SourceRuntimeError::SourceIdentityMismatch),
                Fatal,
            ),
            (
                LocalSourceError::Runtime(SourceRuntimeError::UnsupportedSourceSettings),
                Fatal,
            ),
            (LocalSourceError::SourceIdentityChanged, Fatal),
            // PostgreSQL refuses the role until an administrator changes it.
            (
                LocalSourceError::Fence(FenceError::ControlStoreRefused(
                    PostgresRefusal::AuthenticationFailed,
                )),
                Fatal,
            ),
            (
                LocalSourceError::Runtime(SourceRuntimeError::SourceRefused(
                    PostgresRefusal::ReplicationPrivilegeMissing,
                )),
                Fatal,
            ),
            (
                LocalSourceError::Runtime(SourceRuntimeError::SourceRefused(
                    PostgresRefusal::InsufficientPrivilege,
                )),
                Fatal,
            ),
            (
                LocalSourceError::Runtime(SourceRuntimeError::Fence(
                    FenceError::ControlStoreRefused(PostgresRefusal::DatabaseMissing),
                )),
                Fatal,
            ),
            (
                ingest(PersistentIngestError::Fence(
                    FenceError::ControlStoreRefused(PostgresRefusal::InsufficientPrivilege),
                )),
                Fatal,
            ),
            // Schema, publication, or slot drift.
            (
                LocalSourceError::Runtime(SourceRuntimeError::PublicationMissing),
                Fatal,
            ),
            (
                LocalSourceError::Runtime(SourceRuntimeError::PublicationDrift),
                Fatal,
            ),
            (
                LocalSourceError::PublicationDrift {
                    publication: "calque_drifted".to_owned(),
                },
                Fatal,
            ),
            (
                LocalSourceError::Runtime(SourceRuntimeError::SlotMissing {
                    slot_name: "calque_slot".to_owned(),
                }),
                Fatal,
            ),
            (
                LocalSourceError::Runtime(SourceRuntimeError::SlotLost {
                    slot_name: "calque_slot".to_owned(),
                    reason: "reason wal_removed".to_owned(),
                }),
                Fatal,
            ),
            (
                LocalSourceError::Runtime(SourceRuntimeError::UnexpectedSlotMetadata),
                Fatal,
            ),
            (
                LocalSourceError::Runtime(SourceRuntimeError::SlotAheadOfCanonicalTruth {
                    slot_lsn: 2,
                    canonical_lsn: 1,
                }),
                Fatal,
            ),
            (
                ingest(PersistentIngestError::RegistryBindingMismatch),
                Fatal,
            ),
            (
                ingest(PersistentIngestError::AuthorizationPolicyChanged),
                Fatal,
            ),
            // Protocol, decoding, or canonical-log integrity violations.
            (
                ingest(PersistentIngestError::Decoder(
                    pgoutput_decoder::DecodeError::InvalidLimits,
                )),
                Fatal,
            ),
            (appended(LogError::DuplicateTransactionConflict), Fatal),
            (
                appended(LogError::SequenceGap {
                    expected: 2,
                    actual: 3,
                }),
                Fatal,
            ),
            (
                ingest(PersistentIngestError::CanonicalLog(
                    LogError::PostgresHeadCorrupt,
                )),
                Fatal,
            ),
            (
                LocalSourceError::Canonical(LogError::DatastoreIdentityMismatch),
                Fatal,
            ),
            (
                LocalSourceError::Canonical(LogError::FencedWriterRejected),
                Fatal,
            ),
            (
                LocalSourceError::Ingest(PersistentIngestError::Ingest(IngestError::Append(
                    AppendFailure::Other(Box::new(std::io::Error::other("unclassified"))),
                ))),
                Fatal,
            ),
            (
                LocalSourceError::Ingest(PersistentIngestError::Ingest(
                    IngestError::GroupOutcomeMismatch {
                        expected: 2,
                        actual: 1,
                    },
                )),
                Fatal,
            ),
            // Local faults that a reconnect cannot repair.
            (ingest(PersistentIngestError::InvalidConfig), Fatal),
            (LocalSourceError::Fence(FenceError::TokenExhausted), Fatal),
            (
                LocalSourceError::Runtime(SourceRuntimeError::WatchdogUnavailable),
                Fatal,
            ),
            (LocalSourceError::IngestThreadPanicked, Fatal),
            (LocalSourceError::IngestThreadPanickedAfterLoop, Fatal),
            (LocalSourceError::SessionCleanupFailed, Fatal),
        ];
        for (error, expected) in cases {
            assert_eq!(classify_cdc_failure(&error), expected, "{error:?}");
        }
    }

    #[test]
    fn a_stopped_runtime_names_what_to_change_for_a_refusal() {
        let source = SourceContext {
            role: Some("calque_sync".to_owned()),
            database: Some("app".to_owned()),
            url_env: Some("APP_DATABASE_URL".to_owned()),
        };
        let detail = |error: LocalSourceError| cdc_failure_detail(&error, &source);
        assert_eq!(
            detail(LocalSourceError::Fence(FenceError::ControlStoreRefused(
                PostgresRefusal::AuthenticationFailed
            ))),
            "cdc-refused: PostgreSQL refused to authenticate role \"calque_sync\". Check the password in the database URL in APP_DATABASE_URL, that the role may log in, and that pg_hba.conf lets it connect to database \"app\" from this host"
        );
        assert_eq!(
            detail(LocalSourceError::Runtime(
                SourceRuntimeError::SourceRefused(PostgresRefusal::ReplicationPrivilegeMissing)
            )),
            "cdc-refused: role \"calque_sync\" cannot start replication, which needs the REPLICATION attribute. Run ALTER ROLE \"calque_sync\" REPLICATION as a superuser; on RDS, run GRANT rds_replication TO \"calque_sync\" instead"
        );
        assert_eq!(
            detail(LocalSourceError::Runtime(SourceRuntimeError::Fence(
                FenceError::ControlStoreRefused(PostgresRefusal::DatabaseMissing)
            ))),
            "cdc-refused: database \"app\" does not exist on the source. Check the database name in the database URL in APP_DATABASE_URL"
        );
        assert_eq!(
            detail(LocalSourceError::Ingest(PersistentIngestError::Fence(
                FenceError::ControlStoreRefused(PostgresRefusal::InsufficientPrivilege)
            ))),
            "cdc-refused: role \"calque_sync\" lacks a privilege calque needs on the source. It needs CONNECT and CREATE on database \"app\" (GRANT CONNECT, CREATE ON DATABASE \"app\" TO \"calque_sync\") and SELECT on the enrolled tables, with INSERT, UPDATE and DELETE when pages write (GRANT SELECT, INSERT, UPDATE, DELETE ON <table> TO \"calque_sync\")"
        );
        assert_eq!(
            detail(LocalSourceError::Fence(FenceError::LeaseSuperseded)),
            format!(
                "persistent CDC failed: {}",
                LocalSourceError::Fence(FenceError::LeaseSuperseded)
            )
        );
    }

    #[test]
    fn a_refused_start_is_worded_with_the_role_and_never_carries_the_password() {
        let url =
            DatabaseUrl::parse("postgresql://calque%20sync:s3cret%21pw@127.0.0.1:5432/my%20db")
                .unwrap();
        let source = SourceContext::of(&url, Some("APP_DATABASE_URL"));
        let explained = source.explained(LocalSourceError::Fence(FenceError::ControlStoreRefused(
            PostgresRefusal::AuthenticationFailed,
        )));
        let text = explained.to_string();
        assert_eq!(
            text,
            "PostgreSQL refused to authenticate role \"calque sync\". Check the password in the database URL in APP_DATABASE_URL, that the role may log in, and that pg_hba.conf lets it connect to database \"my db\" from this host"
        );
        assert!(!text.contains("s3cret"), "{text}");
        assert_eq!(
            postgres_refusal(&explained),
            Some(PostgresRefusal::AuthenticationFailed)
        );
        assert!(!explained.requires_destroying_state());
        let unrelated = source.explained(LocalSourceError::Fence(FenceError::LeaseSuperseded));
        assert!(matches!(
            unrelated,
            LocalSourceError::Fence(FenceError::LeaseSuperseded)
        ));
        let no_role = SourceContext::default().advice(PostgresRefusal::ReplicationPrivilegeMissing);
        assert!(
            no_role.starts_with("the connecting role cannot start replication"),
            "{no_role}"
        );
        assert!(
            no_role.contains("ALTER ROLE <role> REPLICATION"),
            "{no_role}"
        );
    }

    /// A canonical log an earlier release wrote needs `calque destroy`
    /// whichever step met it: connecting the log, an ingest step, or an
    /// append. So does a schema registry that does not match its state, and
    /// a failed session cleanup after either. A lost log connection on the
    /// same three paths, a failure that another remedy answers or none does,
    /// and a failed cleanup after any of those do not.
    #[test]
    fn only_what_destroy_alone_gets_past_requires_destroying_state() {
        let paths = |error: fn() -> LogError| {
            [
                LocalSourceError::Canonical(error()),
                LocalSourceError::Ingest(PersistentIngestError::CanonicalLog(error())),
                LocalSourceError::Ingest(PersistentIngestError::Ingest(IngestError::Append(
                    AppendFailure::CanonicalLog(error()),
                ))),
            ]
        };
        let earlier = || LogError::EarlierEntryFormat {
            state_dir: state_dir().to_path_buf(),
        };
        let slot_gone = [
            LocalSourceError::Runtime(SourceRuntimeError::SlotMissing {
                slot_name: "calque_slot".to_owned(),
            }),
            LocalSourceError::Runtime(SourceRuntimeError::SlotLost {
                slot_name: "calque_slot".to_owned(),
                reason: "reason wal_removed".to_owned(),
            }),
        ];
        for error in paths(earlier)
            .into_iter()
            .chain([LocalSourceError::RegistryStateMismatch])
            .chain(slot_gone)
        {
            assert!(error.requires_destroying_state(), "{error:?}");
            let after = LocalSourceError::SessionCleanupFailedAfter(Box::new(error));
            assert!(after.requires_destroying_state(), "{after:?}");
        }
        for error in paths(|| LogError::PostgresUnavailable).into_iter().chain([
            LocalSourceError::CdcStartupTimeout,
            LocalSourceError::SessionCleanupFailed,
            LocalSourceError::SourceIdentityChanged,
            LocalSourceError::PublicationDrift {
                publication: "calque_drifted".to_owned(),
            },
        ]) {
            assert!(!error.requires_destroying_state(), "{error:?}");
            let after = LocalSourceError::SessionCleanupFailedAfter(Box::new(error));
            assert!(!after.requires_destroying_state(), "{after:?}");
        }
    }

    /// A session cleanup that fails after the loop stopped on an error or
    /// panicked reports that cause beside it, so the cause and its remedy
    /// survive the join. After a stop request there is no error to keep.
    #[test]
    fn a_failed_cleanup_keeps_the_error_the_loop_stopped_on() {
        let refusal = "/srv/calque/state serves a canonical log an earlier release of calque wrote, and calque does not migrate it.";
        let stopped_on = || -> thread::Result<Result<(), LocalSourceError>> {
            Ok(Err(LocalSourceError::Canonical(
                LogError::EarlierEntryFormat {
                    state_dir: state_dir().to_path_buf(),
                },
            )))
        };
        let panicked =
            || -> thread::Result<Result<(), LocalSourceError>> { Err(Box::new("ingest panicked")) };
        let printed =
            |result: Result<(), LocalSourceError>| result.map_err(|error| error.to_string());

        assert_eq!(
            printed(cdc_thread_result(stopped_on(), true)),
            Err(format!(
                "persistent CDC session cleanup failed after persistent CDC failed: {refusal}"
            ))
        );
        assert_eq!(
            printed(cdc_thread_result(Ok(Ok(())), true)),
            Err("persistent CDC session cleanup failed".to_owned())
        );
        assert_eq!(
            printed(cdc_thread_result(panicked(), true)),
            Err("persistent CDC session cleanup failed after persistent CDC failed: persistent CDC thread panicked".to_owned())
        );

        assert_eq!(
            printed(cdc_thread_result(stopped_on(), false)),
            Err(refusal.to_owned())
        );
        assert_eq!(printed(cdc_thread_result(Ok(Ok(())), false)), Ok(()));
        assert_eq!(
            printed(cdc_thread_result(panicked(), false)),
            Err("persistent CDC thread panicked".to_owned())
        );
    }

    #[test]
    fn reconnect_backoff_doubles_to_its_cap_within_the_jitter_band() {
        let steps =
            [100, 200, 400, 800, 1_600, 3_200, 5_000, 5_000, 5_000].map(Duration::from_millis);

        let mut lowest = ReconnectBackoff::new(|| 0);
        for step in steps {
            assert_eq!(lowest.next_delay(), step / 2);
        }
        lowest.reset();
        assert_eq!(lowest.next_delay(), Duration::from_millis(50));

        for _ in 0..200 {
            let mut drawn = ReconnectBackoff::new(reconnect_jitter);
            for step in steps {
                let delay = drawn.next_delay();
                assert!(
                    step / 2 <= delay && delay <= step,
                    "{delay:?} outside [{:?}, {step:?}]",
                    step / 2
                );
            }
        }
    }

    #[test]
    fn reconnect_retries_transient_failures_until_an_attempt_succeeds() {
        let mut backoff = ReconnectBackoff::new(|| 0);
        let mut waits = Vec::new();
        let mut retried = Vec::new();
        let mut failures = vec![
            LocalSourceError::Runtime(SourceRuntimeError::SourceControlUnavailable),
            LocalSourceError::Fence(FenceError::ControlStoreUnavailable),
        ]
        .into_iter();
        let mut attempts = 0;

        let outcome = reconnect_with_backoff(
            &mut backoff,
            |delay| {
                waits.push(delay);
                false
            },
            || {
                attempts += 1;
                failures.next().map_or(Ok("session"), Err)
            },
            |error| retried.push(error.to_string()),
        );

        assert!(matches!(outcome, Reconnect::Established("session")));
        assert_eq!(attempts, 3);
        assert_eq!(
            waits,
            [50, 100, 200].map(Duration::from_millis).to_vec(),
            "every attempt, including the first, waits out its backoff delay"
        );
        assert_eq!(retried.len(), 2);
    }

    #[test]
    fn reconnect_ends_at_the_first_fatal_failure() {
        let mut backoff = ReconnectBackoff::new(|| 0);
        let mut failures = vec![
            LocalSourceError::Runtime(SourceRuntimeError::ReplicationUnavailable),
            LocalSourceError::Fence(FenceError::LeaseSuperseded),
        ]
        .into_iter();
        let mut attempts = 0;

        let outcome = reconnect_with_backoff(
            &mut backoff,
            |_| false,
            || {
                attempts += 1;
                failures.next().map_or(Ok(()), Err)
            },
            |_| {},
        );

        assert!(matches!(
            outcome,
            Reconnect::Failed(LocalSourceError::Fence(FenceError::LeaseSuperseded))
        ));
        assert_eq!(attempts, 2);
    }

    #[test]
    fn a_stop_during_reconnect_backoff_ends_the_wait_without_another_attempt() {
        let (stop, stop_rx) = mpsc::channel();
        let stopper = thread::spawn(move || {
            thread::sleep(Duration::from_millis(50));
            stop.send(()).unwrap();
        });
        let mut backoff = ReconnectBackoff::new(|| 0);
        for _ in 0..8 {
            backoff.next_delay();
        }
        let mut attempts = 0;
        let started = Instant::now();

        let outcome = reconnect_with_backoff(
            &mut backoff,
            |delay| stop_requested_within(&stop_rx, delay),
            || -> Result<(), LocalSourceError> {
                attempts += 1;
                Ok(())
            },
            |_| {},
        );

        assert!(matches!(outcome, Reconnect::Stopped));
        assert_eq!(attempts, 0);
        assert!(
            started.elapsed() < Duration::from_secs(1),
            "a 2.5 s backoff delay ended {:?} after the stop request",
            started.elapsed()
        );
        stopper.join().unwrap();

        let (dropped, orphaned) = mpsc::channel::<()>();
        drop(dropped);
        assert!(stop_requested_within(&orphaned, Duration::from_secs(5)));
        let (_held, idle) = mpsc::channel::<()>();
        assert!(!stop_requested_within(&idle, Duration::from_millis(10)));
    }

    fn held() -> FenceError {
        FenceError::LeaseHeld {
            expires_at_unix_millis: 1,
        }
    }

    #[test]
    fn startup_takes_a_held_lease_once_its_holder_lets_it_go() {
        let (_stop, stop_rx) = mpsc::channel::<()>();
        let mut attempts = 0;
        let acquired = acquire_startup_lease(Duration::from_secs(5), &stop_rx, || {
            attempts += 1;
            if attempts < 3 {
                Err(held())
            } else {
                Ok("lease")
            }
        });
        assert!(matches!(acquired, Ok("lease")));
        assert_eq!(attempts, 3);
    }

    #[test]
    fn startup_refuses_a_lease_still_held_when_the_wait_ends() {
        let (_stop, stop_rx) = mpsc::channel::<()>();
        let wait = Duration::from_millis(600);
        let started = Instant::now();
        let mut attempts = 0;
        // The holder lets go only after 20 attempts, long after the wait.
        let acquired = acquire_startup_lease(wait, &stop_rx, || -> Result<(), FenceError> {
            attempts += 1;
            if attempts > 20 {
                Ok(())
            } else {
                Err(held())
            }
        });
        assert!(
            matches!(
                acquired,
                Err(LocalSourceError::Fence(FenceError::LeaseHeld { .. }))
            ),
            "{acquired:?}"
        );
        assert!(started.elapsed() >= wait, "{:?}", started.elapsed());
        assert!(
            started.elapsed() < wait + Duration::from_secs(1),
            "{:?}",
            started.elapsed()
        );
        assert!(attempts >= 3, "{attempts}");
    }

    #[test]
    fn startup_waits_out_nothing_but_a_held_lease() {
        let (_stop, stop_rx) = mpsc::channel::<()>();
        let mut attempts = 0;
        let acquired = acquire_startup_lease(
            Duration::from_secs(5),
            &stop_rx,
            || -> Result<(), FenceError> {
                attempts += 1;
                Err(FenceError::LeaseSuperseded)
            },
        );
        assert!(matches!(
            acquired,
            Err(LocalSourceError::Fence(FenceError::LeaseSuperseded))
        ));
        assert_eq!(attempts, 1);
    }

    /// A lease under another epoch is an enrollment another state directory at
    /// the same path made, which only `calque destroy` gets past.
    #[test]
    fn startup_refuses_a_lease_under_another_epoch_as_another_states_enrollment() {
        let (_stop, stop_rx) = mpsc::channel::<()>();
        let epoch = |value: &str| sync_types::SourceEpoch::new(value).unwrap();
        let acquired = acquire_startup_lease(
            Duration::from_secs(5),
            &stop_rx,
            || -> Result<(), FenceError> {
                Err(FenceError::EpochMismatch {
                    expected: epoch("local-new"),
                    actual: epoch("local-old"),
                })
            },
        );
        let Err(refusal) = acquired else {
            panic!("a lease under another epoch was acquired");
        };
        assert!(
            matches!(refusal, LocalSourceError::EnrolledByAnotherState),
            "{refusal:?}"
        );
        assert!(refusal.requires_destroying_state());
    }

    #[test]
    fn a_stop_ends_the_wait_for_a_held_lease() {
        let (stop, stop_rx) = mpsc::channel::<()>();
        stop.send(()).unwrap();
        let started = Instant::now();
        let acquired = acquire_startup_lease(
            Duration::from_secs(30),
            &stop_rx,
            || -> Result<(), FenceError> { Err(held()) },
        );
        assert!(matches!(
            acquired,
            Err(LocalSourceError::CdcStartupCancelled)
        ));
        assert!(started.elapsed() < Duration::from_secs(1));
    }

    #[test]
    #[ignore = "requires CALQUE_SOURCE_DATABASE_URL or TEST_DATABASE_URL"]
    fn durable_append_wakes_head_waiter_before_bounded_ingest_returns() {
        let source_url = live_database_url().expect("live source database URL");
        let database_url = DatabaseUrl::parse(&source_url).unwrap();
        let suffix = uuid::Uuid::new_v4().simple().to_string();
        let table_name = format!("cdc_wake_{suffix}");
        let publication_name = format!("cdc_wake_pub_{suffix}");
        let slot_name = format!("cdc_wake_slot_{suffix}");
        let source_id = sync_types::SourceId::new(format!("cdc-wake-{suffix}")).unwrap();
        let source_epoch = sync_types::SourceEpoch::new(format!("epoch-{suffix}")).unwrap();
        let table = QualifiedTable {
            schema: "public".to_owned(),
            table: table_name.clone(),
        };

        let mut source = Client::connect(&source_url, postgres::NoTls).unwrap();
        source
            .batch_execute(&format!(
                "CREATE TABLE public.{} (id bigint PRIMARY KEY, value text NOT NULL)",
                quote_identifier(&table_name)
            ))
            .unwrap();
        let state = LocalState::new(
            inspect_local_identity(&database_url).unwrap(),
            source_epoch,
            LocalNames {
                source_id,
                publication_name,
                slot_name,
            },
            vec![table],
        )
        .unwrap();
        let secrets = LocalSecrets::generate().unwrap();
        let prepared = prepare_local_source(&database_url, &state, &secrets, None).unwrap();
        let head_signal = snapshot_pipeline::CanonicalHeadSignal::new();
        let mut head_waiter = head_signal.subscribe();
        let runtime = start_local_source_runtime(
            prepared,
            LocalSourceRuntimeConfig {
                database_url: &database_url,
                state: &state,
                state_dir: state_dir(),
                owner_id: format!("cdc-wake-owner-{suffix}"),
                lease_ttl: DEFAULT_LEASE_TTL,
                // Longer than the waiter's 750 ms, so a wake sent when the
                // bounded call returns rather than at the durable append would
                // miss it. Batching is unchanged.
                ingest_timeout: Duration::from_secs(2),
                max_transactions_per_batch: DEFAULT_MAX_TRANSACTIONS,
                metrics: OperationsMetrics::new(),
                database_url_env: None,
            },
            head_signal,
        )
        .unwrap();

        source
            .execute(
                &format!(
                    "INSERT INTO public.{} (id, value) VALUES (1, 'committed')",
                    quote_identifier(&table_name)
                ),
                &[],
            )
            .unwrap();

        tokio::runtime::Runtime::new()
            .unwrap()
            .block_on(async {
                tokio::time::timeout(Duration::from_millis(750), head_waiter.changed()).await
            })
            .expect("durable append did not wake the head waiter before the ingest window closed")
            .expect("head signal closed");

        runtime.shutdown().unwrap();
        destroy_local_source(&database_url, &state).unwrap();
        source
            .batch_execute(&format!(
                "DROP TABLE public.{}",
                quote_identifier(&table_name)
            ))
            .unwrap();
    }

    /// Tables in the live database, not yet enrolled, and the local state
    /// that would enroll them. Enrollment takes tables in name order, and the
    /// names sort in index order.
    struct EnrollmentLockFixture {
        source_url: String,
        database_url: DatabaseUrl,
        source: Client,
        table_names: Vec<String>,
        state: LocalState,
    }

    impl EnrollmentLockFixture {
        fn create(label: &str) -> Self {
            Self::create_with_tables(label, 1)
        }

        fn create_with_tables(label: &str, count: usize) -> Self {
            let source_url = live_database_url().expect("live source database URL");
            let database_url = DatabaseUrl::parse(&source_url).unwrap();
            let suffix = uuid::Uuid::new_v4().simple().to_string();
            assert!(
                count <= 10,
                "names sort in index order only below ten tables"
            );
            let table_names = (0..count)
                .map(|index| format!("{label}_{index}_{suffix}"))
                .collect::<Vec<_>>();
            let mut source = Client::connect(&source_url, postgres::NoTls).unwrap();
            for table_name in &table_names {
                source
                    .batch_execute(&format!(
                        "CREATE TABLE public.{} (id bigint PRIMARY KEY, value text NOT NULL)",
                        quote_identifier(table_name)
                    ))
                    .unwrap();
            }
            let state = LocalState::new(
                inspect_local_identity(&database_url).unwrap(),
                sync_types::SourceEpoch::new(format!("epoch-{suffix}")).unwrap(),
                LocalNames {
                    source_id: sync_types::SourceId::new(format!(
                        "{}-{suffix}",
                        label.replace('_', "-")
                    ))
                    .unwrap(),
                    publication_name: format!("{label}_pub_{suffix}"),
                    slot_name: format!("{label}_slot_{suffix}"),
                },
                table_names
                    .iter()
                    .map(|table_name| QualifiedTable {
                        schema: "public".to_owned(),
                        table: table_name.clone(),
                    })
                    .collect(),
            )
            .unwrap();
            Self {
                source_url,
                database_url,
                source,
                table_names,
                state,
            }
        }

        fn table_name(&self) -> &str {
            &self.table_names[0]
        }

        fn relation(&self, index: usize) -> String {
            format!("public.{}", quote_identifier(&self.table_names[index]))
        }

        /// Runs `statement` in a transaction on a second session and keeps
        /// that transaction open until the returned sender fires or is
        /// dropped, then rolls it back. Returns once the statement has run.
        fn hold_in_transaction(
            &self,
            statement: String,
        ) -> (mpsc::Sender<()>, thread::JoinHandle<()>) {
            let (release, release_rx) = mpsc::channel::<()>();
            let (held, held_rx) = mpsc::channel::<()>();
            let url = self.source_url.clone();
            let holder = thread::spawn(move || {
                let mut client = Client::connect(&url, postgres::NoTls).unwrap();
                let mut transaction = client.transaction().unwrap();
                transaction.batch_execute(&statement).unwrap();
                held.send(()).unwrap();
                let _ = release_rx.recv_timeout(Duration::from_secs(120));
                transaction.rollback().unwrap();
            });
            held_rx
                .recv_timeout(Duration::from_secs(10))
                .expect("the holder session ran its statement");
            (release, holder)
        }

        /// Takes SHARE UPDATE EXCLUSIVE on `relation`, the lock an autovacuum
        /// holds, from a second session.
        fn hold_autovacuum_lock_on(
            &self,
            relation: &str,
        ) -> (mpsc::Sender<()>, thread::JoinHandle<()>) {
            self.hold_in_transaction(format!(
                "LOCK TABLE {relation} IN SHARE UPDATE EXCLUSIVE MODE"
            ))
        }

        fn hold_autovacuum_lock(&self) -> (mpsc::Sender<()>, thread::JoinHandle<()>) {
            self.hold_autovacuum_lock_on(&self.relation(0))
        }

        fn publication_exists(&mut self) -> bool {
            self.source
                .query_opt(
                    "SELECT 1 FROM pg_catalog.pg_publication WHERE pubname = $1",
                    &[&self.state.names.publication_name],
                )
                .unwrap()
                .is_some()
        }

        fn slot_exists(&mut self) -> bool {
            self.source
                .query_opt(
                    "SELECT 1 FROM pg_catalog.pg_replication_slots WHERE slot_name = $1",
                    &[&self.state.names.slot_name],
                )
                .unwrap()
                .is_some()
        }

        fn remove(mut self) {
            destroy_local_source(&self.database_url, &self.state).unwrap();
            for index in 0..self.table_names.len() {
                let relation = self.relation(index);
                self.source
                    .batch_execute(&format!("DROP TABLE {relation}"))
                    .unwrap();
            }
        }
    }

    /// PostgreSQL cancels an autovacuum that blocks a lock request only once
    /// the request has waited `deadlock_timeout`, 1 s by default. The holder
    /// here plays that autovacuum: it keeps the lock past 1 s and then lets
    /// go, as a cancelled autovacuum does.
    #[test]
    #[ignore = "requires CALQUE_SOURCE_DATABASE_URL or TEST_DATABASE_URL"]
    fn enrollment_waits_out_a_lock_held_past_deadlock_timeout() {
        let mut fixture = EnrollmentLockFixture::create("enroll_wait");
        let (release, holder) = fixture.hold_autovacuum_lock();
        let releaser = thread::spawn(move || {
            thread::sleep(Duration::from_millis(1_500));
            let _ = release.send(());
        });
        let started = Instant::now();
        let prepared = prepare_local_source(
            &fixture.database_url,
            &fixture.state,
            &LocalSecrets::generate().unwrap(),
            None,
        );
        let waited = started.elapsed();
        releaser.join().unwrap();
        holder.join().unwrap();
        let prepared = prepared.expect("enrollment outlasts a lock released after 1.5 s");
        assert_eq!(prepared.registry.tables().len(), 1);
        assert!(
            waited > Duration::from_millis(1_200),
            "enrollment returned after {waited:?}, before the held lock was released"
        );
        assert!(fixture.publication_exists());
        fixture.remove();
    }

    /// A holder PostgreSQL never cancels, such as an autovacuum preventing
    /// wraparound, ends enrollment at the bound with the table named, and
    /// leaves nothing in PostgreSQL that a rerun would trip over.
    #[test]
    #[ignore = "requires CALQUE_SOURCE_DATABASE_URL or TEST_DATABASE_URL"]
    fn enrollment_names_the_table_whose_lock_outlasts_the_bound() {
        let mut fixture = EnrollmentLockFixture::create("enroll_bound");
        let (release, holder) = fixture.hold_autovacuum_lock();
        let started = Instant::now();
        let refused = prepare_local_source(
            &fixture.database_url,
            &fixture.state,
            &LocalSecrets::generate().unwrap(),
            None,
        );
        let waited = started.elapsed();
        let left_behind = fixture.publication_exists() || fixture.slot_exists();
        release.send(()).unwrap();
        holder.join().unwrap();
        let bound = Duration::from_secs(10);
        match refused {
            Err(LocalSourceError::PublicationLockTimeout {
                table,
                bound: reported,
            }) => {
                assert_eq!(table, format!("public.{}", fixture.table_name()));
                assert_eq!(reported, bound);
            }
            Err(error) => panic!("enrollment failed with {error:?}, not at the lock bound"),
            Ok(_) => panic!("enrollment succeeded while the lock was held"),
        }
        assert!(
            waited >= bound && waited < bound + Duration::from_secs(3),
            "enrollment gave up after {waited:?}, not at the {bound:?} bound"
        );
        assert!(!left_behind, "a publication or slot outlived the refusal");

        prepare_local_source(
            &fixture.database_url,
            &fixture.state,
            &LocalSecrets::generate().unwrap(),
            None,
        )
        .expect("enrollment succeeds once the lock is released");
        assert!(fixture.publication_exists());
        fixture.remove();
    }

    /// Enrollment holds every table nothing else holds before it waits for
    /// one that something does. An autovacuum cancelled on one enrolled table
    /// then finds the others taken and skips them, so each worker costs at
    /// most one cancellation, as the lock bound assumes.
    #[test]
    #[ignore = "requires CALQUE_SOURCE_DATABASE_URL or TEST_DATABASE_URL"]
    fn enrollment_holds_free_tables_before_it_waits_for_a_held_one() {
        let mut fixture = EnrollmentLockFixture::create_with_tables("enroll_order", 2);
        let (held, free) = (fixture.relation(0), fixture.relation(1));
        let (release, holder) = fixture.hold_autovacuum_lock_on(&held);
        let url = fixture.source_url.clone();
        let tables = [held.clone(), free.clone()];
        let observer = thread::spawn(move || {
            let mut client = Client::connect(&url, postgres::NoTls).unwrap();
            let deadline = Instant::now() + Duration::from_secs(5);
            let mut observed = false;
            while !observed && Instant::now() < deadline {
                thread::sleep(Duration::from_millis(20));
                observed = client
                    .query_one(
                        "SELECT EXISTS (
                            SELECT 1 FROM pg_catalog.pg_locks AS waiting
                              JOIN pg_catalog.pg_locks AS held ON held.pid = waiting.pid
                             WHERE waiting.relation = $1::text::regclass
                               AND NOT waiting.granted
                               AND held.relation = $2::text::regclass
                               AND held.mode = 'ShareUpdateExclusiveLock'
                               AND held.granted)",
                        &[&tables[0], &tables[1]],
                    )
                    .unwrap()
                    .get(0);
            }
            let _ = release.send(());
            observed
        });
        let prepared = prepare_local_source(
            &fixture.database_url,
            &fixture.state,
            &LocalSecrets::generate().unwrap(),
            None,
        );
        let observed = observer.join().unwrap();
        holder.join().unwrap();
        prepared.expect("enrollment succeeds once the held table is released");
        assert!(
            observed,
            "while enrollment waited for {held}, it did not hold {free}"
        );
        assert!(fixture.publication_exists());
        fixture.remove();
    }

    /// A row filter or a column list on an enrolled table refuses the next
    /// start as drift of the publication, ahead of the inspection that would
    /// refuse it with no remedy. Restoring the definition, as the refusal
    /// says, lets the source start again.
    #[test]
    #[ignore = "requires CALQUE_SOURCE_DATABASE_URL or TEST_DATABASE_URL"]
    fn a_row_filter_or_column_list_on_an_enrolled_table_is_publication_drift() {
        let mut fixture = EnrollmentLockFixture::create("pub_filter");
        let start = |fixture: &EnrollmentLockFixture, registry: Option<SchemaRegistry>| {
            prepare_local_source(
                &fixture.database_url,
                &fixture.state,
                &LocalSecrets::generate().unwrap(),
                registry,
            )
        };
        let registry = start(&fixture, None)
            .unwrap_or_else(|error| panic!("enrollment failed: {error:?}"))
            .registry;
        let publication = quote_identifier(&fixture.state.names.publication_name);
        let relation = fixture.relation(0);

        for definition in [
            format!("{relation} WHERE (id > 0)"),
            format!("{relation} (id)"),
        ] {
            fixture
                .source
                .batch_execute(&format!(
                    "ALTER PUBLICATION {publication} SET TABLE {definition}"
                ))
                .unwrap();
            match start(&fixture, Some(registry.clone())) {
                Err(LocalSourceError::PublicationDrift { publication }) => {
                    assert_eq!(publication, fixture.state.names.publication_name);
                }
                Err(error) => panic!("{definition} was refused as {error:?}, not as drift"),
                Ok(_) => panic!("{definition} was accepted"),
            }
        }

        fixture
            .source
            .batch_execute(&format!(
                "ALTER PUBLICATION {publication} SET TABLE {relation}"
            ))
            .unwrap();
        start(&fixture, Some(registry))
            .unwrap_or_else(|error| panic!("the restored publication was refused: {error:?}"));
        assert!(fixture.publication_exists());
        fixture.remove();
    }

    /// Selected columns become the publication's column list, in table
    /// order, so a column added later is never published and needs no
    /// restart. A selection that differs from the published list, names a
    /// column the table lacks, or leaves out a column of a REPLICA IDENTITY
    /// FULL table is refused before anything is created.
    #[test]
    #[ignore = "requires CALQUE_SOURCE_DATABASE_URL or TEST_DATABASE_URL"]
    fn selected_columns_become_the_publication_column_list() {
        let mut fixture = EnrollmentLockFixture::create("pub_cols");
        let relation = fixture.relation(0);
        fixture
            .source
            .batch_execute(&format!("ALTER TABLE {relation} ADD COLUMN secret text"))
            .unwrap();
        let table = fixture.state.tables[0].clone();
        let select = |names: &[&str]| -> SelectedColumns {
            BTreeMap::from([(
                table.clone(),
                names.iter().map(|name| (*name).to_owned()).collect(),
            )])
        };
        let start =
            |fixture: &EnrollmentLockFixture, registry: Option<SchemaRegistry>, names: &[&str]| {
                prepare_selected_local_source(
                    &fixture.database_url,
                    &fixture.state,
                    &LocalSecrets::generate().unwrap(),
                    registry,
                    &select(names),
                )
            };
        let published = |fixture: &mut EnrollmentLockFixture| -> Vec<String> {
            fixture
                .source
                .query_one(
                    "SELECT attnames::text[] FROM pg_catalog.pg_publication_tables
                     WHERE pubname = $1",
                    &[&fixture.state.names.publication_name],
                )
                .unwrap()
                .get(0)
        };

        let registry = start(&fixture, None, &["value", "id"])
            .unwrap_or_else(|error| panic!("enrollment failed: {error:?}"))
            .registry;
        assert_eq!(published(&mut fixture), ["id", "value"]);
        let enrolled = registry.tables()[0]
            .inspection
            .columns
            .iter()
            .map(|column| column.name.as_str())
            .collect::<Vec<_>>();
        assert_eq!(enrolled, ["id", "value"]);

        fixture
            .source
            .batch_execute(&format!("ALTER TABLE {relation} ADD COLUMN later text"))
            .unwrap();
        start(&fixture, Some(registry.clone()), &["id", "value"])
            .unwrap_or_else(|error| panic!("an added column stopped the source: {error:?}"));
        assert_eq!(published(&mut fixture), ["id", "value"]);

        match start(&fixture, Some(registry), &["id"]) {
            Err(LocalSourceError::PublishedColumnsDiffer {
                published,
                selected,
                ..
            }) => {
                assert_eq!(published, ["id", "value"]);
                assert_eq!(selected, ["id"]);
            }
            other => panic!(
                "a narrower selection was not refused as differing: {:?}",
                other.err()
            ),
        }

        destroy_local_source(&fixture.database_url, &fixture.state).unwrap();
        match start(&fixture, None, &["id", "missing"]) {
            Err(LocalSourceError::SelectedColumnMissing { column, .. }) => {
                assert_eq!(column, "missing");
            }
            other => panic!(
                "a missing selected column was not refused: {:?}",
                other.err()
            ),
        }
        assert!(!fixture.publication_exists());

        fixture
            .source
            .batch_execute(&format!("ALTER TABLE {relation} REPLICA IDENTITY FULL"))
            .unwrap();
        match start(&fixture, None, &["id", "value"]) {
            Err(LocalSourceError::SelectedColumnsUnderFullIdentity { .. }) => {}
            other => panic!(
                "a partial selection under FULL identity was accepted: {:?}",
                other.err()
            ),
        }
        assert!(!fixture.publication_exists());
        start(&fixture, None, &["id", "value", "secret", "later"])
            .unwrap_or_else(|error| panic!("a whole FULL table was refused: {error:?}"));
        let listed: bool = fixture
            .source
            .query_one(
                "SELECT rel.prattrs IS NOT NULL
                 FROM pg_catalog.pg_publication_rel AS rel
                 JOIN pg_catalog.pg_publication AS publication ON publication.oid = rel.prpubid
                 WHERE publication.pubname = $1",
                &[&fixture.state.names.publication_name],
            )
            .unwrap()
            .get(0);
        assert!(!listed, "a FULL table was published with a column list");
        fixture.remove();
    }

    /// Mounting mutations takes no lock that an autovacuum of the receipt
    /// table holds. Nothing deletes receipts, so that table and its
    /// autovacuum grow with every mutation; the holder here keeps its lock
    /// for the whole open, past the connection's 1 s lock timeout.
    #[test]
    #[ignore = "requires CALQUE_SOURCE_DATABASE_URL or TEST_DATABASE_URL"]
    fn mounting_mutations_waits_on_no_autovacuum_of_the_receipt_table() {
        let fixture = EnrollmentLockFixture::create("receipt_lock");
        let prepared = prepare_local_source(
            &fixture.database_url,
            &fixture.state,
            &LocalSecrets::generate().unwrap(),
            None,
        )
        .unwrap();
        // The first open creates the table if no earlier test has.
        drop(crate::LocalMutationSupport::open(&fixture.database_url, &prepared).unwrap());
        let (release, holder) =
            fixture.hold_autovacuum_lock_on("calque_internal.mutation_receipts");
        let opened = crate::LocalMutationSupport::open(&fixture.database_url, &prepared);
        release.send(()).unwrap();
        holder.join().unwrap();
        opened.expect("mutations mount while the receipt table is locked");
        fixture.remove();
    }

    /// Creating the replication slot waits for every transaction that has
    /// written to end. An application transaction left open past the source
    /// connection's 1 s lock timeout must not fail enrollment.
    #[test]
    #[ignore = "requires CALQUE_SOURCE_DATABASE_URL or TEST_DATABASE_URL"]
    fn enrollment_waits_for_a_transaction_open_past_the_lock_timeout() {
        let mut fixture = EnrollmentLockFixture::create("enroll_open_xact");
        let prepared = prepare_local_source(
            &fixture.database_url,
            &fixture.state,
            &LocalSecrets::generate().unwrap(),
            None,
        )
        .unwrap();
        let (release, holder) = fixture.hold_in_transaction(format!(
            "INSERT INTO public.{} (id, value) VALUES (1, 'uncommitted')",
            quote_identifier(fixture.table_name())
        ));
        let releaser = thread::spawn(move || {
            thread::sleep(Duration::from_millis(1_500));
            let _ = release.send(());
        });
        let started = Instant::now();
        let runtime = start_local_source_runtime(
            prepared,
            LocalSourceRuntimeConfig::new(
                &fixture.database_url,
                &fixture.state,
                state_dir(),
                "open-transaction-owner",
            ),
            snapshot_pipeline::CanonicalHeadSignal::new(),
        );
        let waited = started.elapsed();
        releaser.join().unwrap();
        holder.join().unwrap();
        let runtime = runtime.expect("enrollment outlasts a transaction that ends after 1.5 s");
        assert!(
            waited > Duration::from_millis(1_200),
            "the runtime started after {waited:?}, before the open transaction ended"
        );
        assert!(fixture.slot_exists());
        runtime.shutdown().unwrap();
        fixture.remove();
    }

    /// A throwaway database holding one published table, so that terminating
    /// backends by database reaches only the connections one runtime opened.
    struct ReconnectFixture {
        admin_url: String,
        database_name: String,
        /// The login role made for this fixture alone, when it has one.
        role: Option<String>,
        database_url: DatabaseUrl,
        state: LocalState,
        client: Client,
    }

    impl ReconnectFixture {
        fn create(label: &str) -> Self {
            Self::create_as(label, false)
        }

        /// `create`, with the database owned by, and every connection made as,
        /// a login role made for this fixture alone, so that changing the
        /// role's attributes or password refuses no other test.
        fn create_with_own_role(label: &str) -> Self {
            Self::create_as(label, true)
        }

        fn create_as(label: &str, own_role: bool) -> Self {
            let url = live_database_url().expect("live source database URL");
            let suffix = uuid::Uuid::new_v4().simple().to_string();
            let (base, _) = url.rsplit_once('/').expect("database URL has a path");
            let admin_url = format!("{base}/postgres");
            let database_name = format!("calque_{label}_{suffix}");
            let mut admin = Client::connect(&admin_url, postgres::NoTls).unwrap();
            let mut source_url = url::Url::parse(&format!("{base}/{database_name}")).unwrap();
            let role = own_role.then(|| {
                let role = format!("{database_name}_role");
                let password = uuid::Uuid::new_v4().simple().to_string();
                // The suite role administers what it creates. SET and INHERIT
                // also let it give the role a database, terminate the role's
                // backends and drop that database.
                admin
                    .batch_execute(&format!(
                        "SET createrole_self_grant = 'set, inherit';
                         CREATE ROLE {role} LOGIN REPLICATION PASSWORD '{password}'"
                    ))
                    .unwrap();
                source_url.set_username(&role).unwrap();
                source_url.set_password(Some(&password)).unwrap();
                role
            });
            let owner = role
                .as_ref()
                .map_or_else(String::new, |role| format!(" OWNER {role}"));
            admin
                .batch_execute(&format!("CREATE DATABASE {database_name}{owner}"))
                .unwrap();
            let source_url = source_url.to_string();
            let database_url = DatabaseUrl::parse(&source_url).unwrap();
            let mut client = Client::connect(&source_url, postgres::NoTls).unwrap();
            client
                .batch_execute(
                    "CREATE TABLE public.items (id bigint PRIMARY KEY, value text NOT NULL)",
                )
                .unwrap();
            let state = LocalState::new(
                inspect_local_identity(&database_url).unwrap(),
                sync_types::SourceEpoch::new(format!("epoch-{suffix}")).unwrap(),
                LocalNames {
                    source_id: sync_types::SourceId::new(format!(
                        "{}-{suffix}",
                        label.replace('_', "-")
                    ))
                    .unwrap(),
                    publication_name: format!("{label}_pub"),
                    slot_name: format!("{label}_slot_{suffix}"),
                },
                vec![QualifiedTable {
                    schema: "public".to_owned(),
                    table: "items".to_owned(),
                }],
            )
            .unwrap();
            Self {
                admin_url,
                database_name,
                role,
                database_url,
                state,
                client,
            }
        }

        /// Applies `change` to the fixture's own role, as an administrator
        /// would.
        fn alter_role(&self, change: &str) {
            let role = self.role.as_ref().expect("the fixture has its own role");
            Client::connect(&self.admin_url, postgres::NoTls)
                .unwrap()
                .batch_execute(&format!("ALTER ROLE {role} {change}"))
                .unwrap();
        }

        /// Runs `sql` as the suite's administrator inside the fixture's database.
        fn admin_in_database(&self, sql: &str) {
            let (base, _) = self.admin_url.rsplit_once('/').unwrap();
            Client::connect(&format!("{base}/{}", self.database_name), postgres::NoTls)
                .unwrap()
                .batch_execute(sql)
                .unwrap();
        }

        fn start(&self, metrics: OperationsMetrics) -> LocalSourceRuntime {
            self.try_start(metrics).unwrap()
        }

        fn try_start(
            &self,
            metrics: OperationsMetrics,
        ) -> Result<LocalSourceRuntime, LocalSourceError> {
            self.try_start_sampling(metrics, None)
        }

        /// `try_start`, with the slot-lag sampler reading from
        /// `slot_lag_source` when one is given.
        fn try_start_sampling(
            &self,
            metrics: OperationsMetrics,
            slot_lag_source: Option<DatabaseUrl>,
        ) -> Result<LocalSourceRuntime, LocalSourceError> {
            let prepared = prepare_local_source(
                &self.database_url,
                &self.state,
                &LocalSecrets::generate().unwrap(),
                None,
            )
            .unwrap();
            let config = LocalSourceRuntimeConfig {
                database_url: &self.database_url,
                state: &self.state,
                state_dir: state_dir(),
                owner_id: format!("cdc-reconnect-{}", uuid::Uuid::new_v4().simple()),
                // A short lease makes the watchdog notice dead control
                // connections within a second.
                lease_ttl: Duration::from_secs(3),
                ingest_timeout: Duration::from_millis(200),
                max_transactions_per_batch: DEFAULT_MAX_TRANSACTIONS,
                metrics,
                database_url_env: None,
            };
            let head_signal = snapshot_pipeline::CanonicalHeadSignal::new();
            match slot_lag_source {
                Some(source) => {
                    start_local_source_runtime_sampling(prepared, config, head_signal, source)
                }
                None => start_local_source_runtime(prepared, config, head_signal),
            }
        }

        /// Commits one row and returns the 32-bit xid its canonical
        /// transaction key records.
        fn commit_row(&mut self, id: i64) -> u32 {
            let mut transaction = self.client.transaction().unwrap();
            transaction
                .execute(
                    "INSERT INTO public.items (id, value) VALUES ($1, 'committed')",
                    &[&id],
                )
                .unwrap();
            let xid: String = transaction
                .query_one("SELECT pg_current_xact_id()::text", &[])
                .unwrap()
                .get(0);
            transaction.commit().unwrap();
            u32::try_from(xid.parse::<u64>().unwrap() % (1 << 32)).unwrap()
        }

        /// Terminates this database's other backends of one type, waiting for
        /// each to exit. Returns one entry per backend: false when it had
        /// already exited, as the runtime closes its other connections once
        /// the first one fails.
        fn terminate(&mut self, backend_type: &str) -> Vec<bool> {
            self.client
                .query(
                    "SELECT pg_terminate_backend(pid, 5000) FROM pg_stat_activity
                      WHERE datname = current_database() AND pid <> pg_backend_pid()
                        AND backend_type = $1",
                    &[&backend_type],
                )
                .unwrap()
                .iter()
                .map(|row| row.get(0))
                .collect()
        }

        fn backends(&mut self, backend_type: &str) -> i64 {
            self.client
                .query_one(
                    "SELECT count(*) FROM pg_stat_activity
                      WHERE datname = current_database() AND backend_type = $1",
                    &[&backend_type],
                )
                .unwrap()
                .get(0)
        }

        /// The lease row: owner, token, phase and bound backend.
        fn lease(&mut self) -> (String, i64, String, Option<i32>) {
            let row = self
                .client
                .query_one(
                    "SELECT owner_id, fence_token, lease_phase, source_backend_pid
                       FROM calque_control.source_fence_lease WHERE source_id = $1",
                    &[&self.state.names.source_id.as_str()],
                )
                .unwrap();
            (row.get(0), row.get(1), row.get(2), row.get(3))
        }

        /// Puts the lease row back as `lease` had it, owned and active until
        /// `seconds` from now.
        fn hold_lease(&mut self, lease: &(String, i64, String, Option<i32>), seconds: f64) {
            let (owner, token, _, backend) = lease;
            let updated = self
                .client
                .execute(
                    "UPDATE calque_control.source_fence_lease
                        SET owner_id = $2, fence_token = $3, lease_phase = 'active',
                            source_backend_pid = $4,
                            expires_at = clock_timestamp() + $5 * interval '1 second'
                      WHERE source_id = $1",
                    &[
                        &self.state.names.source_id.as_str(),
                        owner,
                        token,
                        backend,
                        &seconds,
                    ],
                )
                .unwrap();
            assert_eq!(updated, 1);
        }

        fn wal_lsn(&mut self) -> String {
            self.client
                .query_one("SELECT pg_catalog.pg_current_wal_lsn()::text", &[])
                .unwrap()
                .get(0)
        }

        /// Whether the runtime's slot has confirmed a flush past `lsn`.
        fn acknowledged_past(&mut self, lsn: &str) -> bool {
            self.client
                .query_one(
                    "SELECT coalesce(confirmed_flush_lsn > $2::text::pg_lsn, false)
                       FROM pg_catalog.pg_replication_slots WHERE slot_name = $1",
                    &[&self.state.names.slot_name, &lsn],
                )
                .unwrap()
                .get(0)
        }

        /// The canonical log's records in sequence order, each with the xid of
        /// the source transaction it holds.
        fn canonical_log(&mut self) -> Vec<(i64, u32)> {
            self.client
                .query(
                    "SELECT sequence, transaction_identity
                       FROM calque_control.canonical_log_record
                      WHERE source_id = $1 ORDER BY sequence",
                    &[&self.state.names.source_id.as_str()],
                )
                .unwrap()
                .into_iter()
                .map(|row| {
                    let key: serde_json::Value =
                        serde_json::from_str(row.get::<_, &str>(1)).unwrap();
                    let xid = u32::try_from(key["postgresXid"].as_u64().unwrap()).unwrap();
                    (row.get(0), xid)
                })
                .collect()
        }

        /// Every operation of the canonical log's transactions, in sequence
        /// order, as JSON.
        fn canonical_operations(&mut self) -> Vec<serde_json::Value> {
            self.client
                .query(
                    "SELECT entry_bytes FROM calque_control.canonical_log_record
                      WHERE source_id = $1 ORDER BY sequence",
                    &[&self.state.names.source_id.as_str()],
                )
                .unwrap()
                .iter()
                .flat_map(|row| {
                    let entry =
                        canonical_log::CanonicalEntry::decode(row.get::<_, &[u8]>(0)).unwrap();
                    let entry = serde_json::to_value(entry).unwrap();
                    entry["record"]["operations"]
                        .as_array()
                        .cloned()
                        .unwrap_or_default()
                })
                .collect()
        }

        /// Rewrites record `sequence` in JSON, the form an earlier release
        /// stored every entry in, with a checksum that matches the new bytes.
        fn rewrite_as_an_earlier_release(&mut self, sequence: i64) {
            let rewritten = self
                .client
                .execute(
                    "UPDATE calque_control.canonical_log_record
                        SET entry_bytes = $3, entry_sha256 = encode(sha256($3), 'hex')
                      WHERE source_id = $1 AND sequence = $2",
                    &[
                        &self.state.names.source_id.as_str(),
                        &sequence,
                        &br#"{"recordKind":"TRANSACTION","record":{}}"#.as_slice(),
                    ],
                )
                .unwrap();
            assert_eq!(rewritten, 1);
        }
    }

    impl Drop for ReconnectFixture {
        fn drop(&mut self) {
            // An active walsender blocks DROP DATABASE even WITH (FORCE), and a
            // runtime a failed test left behind may still be reconnecting.
            let Ok(mut admin) = Client::connect(&self.admin_url, postgres::NoTls) else {
                return;
            };
            for _ in 0..50 {
                let _ = admin.execute(
                    "SELECT pg_terminate_backend(pid) FROM pg_stat_activity
                      WHERE datname = $1 AND backend_type = 'walsender'",
                    &[&self.database_name],
                );
                if admin
                    .batch_execute(&format!(
                        "DROP DATABASE IF EXISTS {} WITH (FORCE)",
                        self.database_name
                    ))
                    .is_ok()
                {
                    break;
                }
                thread::sleep(Duration::from_millis(100));
            }
            if let Some(role) = &self.role {
                let _ = admin.batch_execute(&format!("DROP ROLE IF EXISTS {role}"));
            }
        }
    }

    fn database_url_endpoint(
        database_url: &DatabaseUrl,
        family: PostgresConnectionFamily,
    ) -> Client {
        database_url.endpoint(family).unwrap().connect().unwrap()
    }

    fn wait_until(timeout: Duration, what: &str, mut condition: impl FnMut() -> bool) {
        let deadline = Instant::now() + timeout;
        while !condition() {
            assert!(
                Instant::now() < deadline,
                "timed out after {timeout:?} waiting for {what}"
            );
            thread::sleep(Duration::from_millis(20));
        }
    }

    fn cdc_lag_bytes(metrics: &OperationsMetrics) -> u64 {
        metrics
            .encode()
            .unwrap()
            .lines()
            .find_map(|line| line.strip_prefix("calque_cdc_lag_bytes "))
            .expect("lag gauge is exported")
            .parse()
            .unwrap()
    }

    fn reconnects(metrics: &OperationsMetrics) -> u64 {
        metrics
            .encode()
            .unwrap()
            .lines()
            .find_map(|line| line.strip_prefix("calque_cdc_reconnects_total "))
            .expect("reconnect counter is exported")
            .parse()
            .unwrap()
    }

    #[test]
    #[ignore = "requires CALQUE_SOURCE_DATABASE_URL or TEST_DATABASE_URL"]
    fn a_terminated_walsender_reconnects_with_each_transaction_once_in_order() {
        let mut fixture = ReconnectFixture::create("cdc_walsender");
        let metrics = OperationsMetrics::new();
        let runtime = fixture.start(metrics.clone());
        let health = runtime.health();

        let before = fixture.commit_row(1);
        wait_until(Duration::from_secs(10), "the first transaction", || {
            fixture.canonical_log().len() == 1 && health.is_current()
        });

        assert_eq!(fixture.terminate("walsender"), vec![true]);
        // `pg_terminate_backend` waited for the walsender to exit, and the
        // first reconnect attempt waits at least 50 ms, so this commits while
        // nothing streams the slot.
        let while_down = fixture.commit_row(2);
        wait_until(
            Duration::from_secs(30),
            "a reconnected, current runtime",
            || reconnects(&metrics) == 1 && health.is_current(),
        );
        let after = fixture.commit_row(3);
        wait_until(Duration::from_secs(10), "the third transaction", || {
            fixture.canonical_log().len() >= 3
        });

        assert_eq!(
            fixture.canonical_log(),
            vec![(1, before), (2, while_down), (3, after)]
        );
        // The old providers still have live connections here; only the token
        // separates them from the new ones.
        runtime
            .provider()
            .blocking_lock()
            .probe_writable_current_fence()
            .expect("the backend's provider writes under the reacquired token");
        runtime
            .barrier_provider()
            .lock()
            .unwrap()
            .probe_writable_current_fence()
            .expect("the barrier provider writes under the reacquired token");
        assert_eq!(fixture.backends("walsender"), 1);
        assert_eq!(reconnects(&metrics), 1);
        assert_eq!(health.failure(), None);
        runtime.shutdown().unwrap();
    }

    /// The slot-lag sampler reads from a listener that accepts and never
    /// answers, so every sample waits out its connect timeout. The CDC thread
    /// never waits on it: rows committed one after another across several of
    /// the sampler's stalls each reach the canonical log and the slot's
    /// confirmed flush within a second, and the gauge keeps its last reading.
    #[test]
    #[ignore = "requires CALQUE_SOURCE_DATABASE_URL or TEST_DATABASE_URL"]
    fn a_slot_lag_source_that_never_answers_does_not_stall_ingest() {
        let mut fixture = ReconnectFixture::create("cdc_slot_lag");
        let (silent, accepted) = crate::test_support::silent_listener();
        let metrics = OperationsMetrics::new();
        metrics.record_cdc_lag_sample(4_242, std::time::SystemTime::now());
        let runtime = fixture
            .try_start_sampling(metrics.clone(), Some(DatabaseUrl::parse(&silent).unwrap()))
            .unwrap();
        let health = runtime.health();
        wait_until(
            Duration::from_secs(10),
            "a current runtime whose sampler is waiting on the listener",
            || health.is_current() && accepted.load(Ordering::SeqCst) >= 1,
        );

        // Rows 0.7 s apart span more than one 3 s stall-and-wait period of the
        // sampler, so a sampler on the CDC thread would hold at least one row
        // for more than a second.
        let mut expected = Vec::new();
        for id in 1..=6 {
            let floor = fixture.wal_lsn();
            let committed = Instant::now();
            expected.push((id, fixture.commit_row(id)));
            wait_until(
                Duration::from_secs(1),
                "the row in the canonical log and acknowledged",
                || {
                    fixture.canonical_log().len() == expected.len()
                        && fixture.acknowledged_past(&floor)
                },
            );
            thread::sleep(Duration::from_millis(700).saturating_sub(committed.elapsed()));
        }

        assert_eq!(fixture.canonical_log(), expected);
        assert!(accepted.load(Ordering::SeqCst) >= 2);
        assert_eq!(cdc_lag_bytes(&metrics), 4_242);
        assert_eq!(health.failure(), None);
        runtime.shutdown().unwrap();
    }

    #[test]
    #[ignore = "requires CALQUE_SOURCE_DATABASE_URL or TEST_DATABASE_URL"]
    fn lost_control_and_writer_connections_reconnect_with_each_transaction_once() {
        let mut fixture = ReconnectFixture::create("cdc_control");
        let metrics = OperationsMetrics::new();
        let runtime = fixture.start(metrics.clone());
        let health = runtime.health();

        let before = fixture.commit_row(1);
        wait_until(Duration::from_secs(10), "the first transaction", || {
            fixture.canonical_log().len() == 1 && health.is_current()
        });

        // Every connection but the walsender: the fence control store, the
        // ingest session's canonical writer and receipt client, and both
        // providers the backend and snapshot pipeline share.
        let terminated = fixture.terminate("client backend");
        assert!(terminated.len() >= 4, "{terminated:?}");
        let while_down = fixture.commit_row(2);
        wait_until(
            Duration::from_secs(30),
            "a reconnected, current runtime",
            || reconnects(&metrics) == 1 && health.is_current(),
        );
        let after = fixture.commit_row(3);
        wait_until(Duration::from_secs(10), "the third transaction", || {
            fixture.canonical_log().len() >= 3
        });

        assert_eq!(
            fixture.canonical_log(),
            vec![(1, before), (2, while_down), (3, after)]
        );
        runtime
            .provider()
            .blocking_lock()
            .probe_writable_current_fence()
            .expect("the backend's provider has a live connection again");
        runtime
            .barrier_provider()
            .lock()
            .unwrap()
            .probe_writable_current_fence()
            .expect("the barrier provider has a live connection again");
        assert_eq!(fixture.backends("walsender"), 1);
        runtime.shutdown().unwrap();
    }

    #[test]
    #[ignore = "requires CALQUE_SOURCE_DATABASE_URL or TEST_DATABASE_URL"]
    fn a_lease_another_owner_took_stops_the_runtime_instead_of_reconnecting() {
        let mut fixture = ReconnectFixture::create("cdc_takeover");
        let metrics = OperationsMetrics::new();
        let runtime = fixture.start(metrics.clone());
        let health = runtime.health();
        wait_until(Duration::from_secs(10), "a current runtime", || {
            health.is_current()
        });

        // The row as another owner's `acquire` leaves it once this runtime's
        // lease has lapsed: a newer token under a different owner.
        let source_id = fixture.state.names.source_id.as_str().to_owned();
        let taken_token: i64 = fixture
            .client
            .query_one(
                "UPDATE calque_control.source_fence_lease
                    SET owner_id = 'another-runtime',
                        fence_token = nextval('calque_control.source_fence_token_seq'),
                        lease_phase = 'provisional',
                        expires_at = clock_timestamp() + interval '1 minute',
                        source_backend_pid = NULL
                  WHERE source_id = $1
              RETURNING fence_token",
                &[&source_id],
            )
            .unwrap()
            .get(0);
        assert_eq!(fixture.terminate("walsender"), vec![true]);

        wait_until(Duration::from_secs(30), "the runtime to stop", || {
            !health.is_healthy()
        });
        let failure = health.failure().unwrap_or_default();
        assert!(
            failure.contains("another owner has taken the source lease"),
            "{failure}"
        );
        let lease = fixture
            .client
            .query_one(
                "SELECT owner_id, fence_token FROM calque_control.source_fence_lease
                  WHERE source_id = $1",
                &[&source_id],
            )
            .unwrap();
        assert_eq!(lease.get::<_, &str>(0), "another-runtime");
        assert_eq!(lease.get::<_, i64>(1), taken_token);
        assert_eq!(reconnects(&metrics), 0);
        assert!(matches!(
            runtime.shutdown(),
            Err(LocalSourceError::Fence(FenceError::LeaseSuperseded))
        ));
    }

    /// Re-establishing a lost session reconnects the canonical-log providers,
    /// and that connect refuses a log an earlier release wrote. The runtime
    /// stops, and the cause it reports names the state directory it serves
    /// and needs `calque destroy`. Both are readable as soon as the runtime
    /// reads as unhealthy, before its thread is joined.
    #[test]
    #[ignore = "requires CALQUE_SOURCE_DATABASE_URL or TEST_DATABASE_URL"]
    fn a_log_an_earlier_release_wrote_stops_the_reconnect_and_names_the_state_dir() {
        let mut fixture = ReconnectFixture::create("cdc_earlier_log");
        let metrics = OperationsMetrics::new();
        let runtime = fixture.start(metrics.clone());
        let health = runtime.health();
        fixture.commit_row(1);
        wait_until(Duration::from_secs(10), "the first transaction", || {
            fixture.canonical_log().len() == 1 && health.is_current()
        });

        fixture.rewrite_as_an_earlier_release(1);
        assert_eq!(fixture.terminate("walsender"), vec![true]);

        wait_until(Duration::from_secs(30), "the runtime to stop", || {
            !health.is_healthy()
        });
        let refusal = "/srv/calque/state serves a canonical log an earlier release of calque wrote, and calque does not migrate it.";
        assert_eq!(
            health.failure(),
            Some(format!("persistent CDC failed: {refusal}"))
        );
        assert_eq!(health.failure_remedy(), CdcStopRemedy::DestroyState);
        assert_eq!(runtime.shutdown().unwrap_err().to_string(), refusal);
        assert_eq!(reconnects(&metrics), 0);
    }

    /// A publication given a row filter while the runtime ran stops it as a
    /// start refuses it, naming the publication, and the stop names restoring
    /// it. No reconnect follows.
    #[test]
    #[ignore = "requires CALQUE_SOURCE_DATABASE_URL or TEST_DATABASE_URL"]
    fn a_publication_altered_while_running_stops_the_runtime_as_a_start_refuses_it() {
        let mut fixture = ReconnectFixture::create("cdc_drift");
        let metrics = OperationsMetrics::new();
        let runtime = fixture.start(metrics.clone());
        let health = runtime.health();
        wait_until(Duration::from_secs(10), "a current runtime", || {
            health.is_current()
        });

        let publication = fixture.state.names.publication_name.clone();
        fixture
            .client
            .batch_execute(&format!(
                "ALTER PUBLICATION {} SET TABLE public.items WHERE (id > 0)",
                quote_identifier(&publication)
            ))
            .unwrap();

        wait_until(Duration::from_secs(30), "the runtime to stop", || {
            !health.is_healthy()
        });
        let refusal = format!("the PostgreSQL publication {publication} was altered, or an enum its columns use gained or renamed a value: Calque needs it to publish exactly the enrolled tables, for insert, update, delete and truncate, without publish_via_partition_root or row filters, with the column lists and enum values it had at enrollment.");
        assert_eq!(
            health.failure(),
            Some(format!("persistent CDC failed: {refusal}"))
        );
        assert_eq!(health.failure_remedy(), CdcStopRemedy::RestorePublication);
        let error = runtime.shutdown().unwrap_err();
        assert!(
            matches!(
                &error,
                LocalSourceError::PublicationDrift { publication: named } if *named == publication
            ),
            "{error:?}"
        );
        assert_eq!(reconnects(&metrics), 0);
    }

    /// A table dropped from the publication while the runtime streams stops
    /// the runtime within seconds, with no reconnect to notice it: pgoutput
    /// would otherwise leave out that table's changes while the cursor moved
    /// past them.
    #[test]
    #[ignore = "requires CALQUE_SOURCE_DATABASE_URL or TEST_DATABASE_URL"]
    fn a_table_dropped_from_the_publication_while_streaming_stops_the_runtime() {
        let mut fixture = ReconnectFixture::create("cdc_drop");
        let metrics = OperationsMetrics::new();
        let runtime = fixture.start(metrics.clone());
        let health = runtime.health();
        wait_until(Duration::from_secs(10), "a current runtime", || {
            health.is_current()
        });

        let publication = fixture.state.names.publication_name.clone();
        fixture
            .client
            .batch_execute(&format!(
                "ALTER PUBLICATION {} DROP TABLE public.items",
                quote_identifier(&publication)
            ))
            .unwrap();
        fixture.commit_row(1);

        wait_until(Duration::from_secs(10), "the runtime to stop", || {
            !health.is_healthy()
        });
        assert_eq!(health.failure_remedy(), CdcStopRemedy::RestorePublication);
        let error = runtime.shutdown().unwrap_err();
        assert!(
            matches!(
                &error,
                LocalSourceError::PublicationDrift { publication: named } if *named == publication
            ),
            "{error:?}"
        );
    }

    /// A publication altered while nothing ran and put back before the next
    /// start is still refused: the slot would decode the changes made in
    /// between under the altered definition.
    #[test]
    #[ignore = "requires CALQUE_SOURCE_DATABASE_URL or TEST_DATABASE_URL"]
    fn a_publication_altered_and_restored_while_stopped_refuses_the_next_start() {
        let mut fixture = ReconnectFixture::create("cdc_restored");
        let runtime = fixture.start(OperationsMetrics::new());
        let health = runtime.health();
        wait_until(Duration::from_secs(10), "a current runtime", || {
            health.is_current()
        });
        runtime.shutdown().unwrap();

        let publication = quote_identifier(&fixture.state.names.publication_name);
        fixture
            .client
            .batch_execute(&format!(
                "ALTER PUBLICATION {publication} DROP TABLE public.items"
            ))
            .unwrap();
        fixture.commit_row(1);
        fixture
            .client
            .batch_execute(&format!(
                "ALTER PUBLICATION {publication} ADD TABLE public.items"
            ))
            .unwrap();

        let refused = match fixture.try_start(OperationsMetrics::new()) {
            Err(error) => error,
            Ok(runtime) => {
                let health = runtime.health();
                wait_until(Duration::from_secs(10), "the runtime to stop", || {
                    !health.is_healthy()
                });
                runtime.shutdown().unwrap_err()
            }
        };
        assert!(refused.publication_drifted(), "{refused:?}");
    }

    /// An enrollment made before enrollments recorded the publication's
    /// fingerprint records it at its next start, and is held to it from then.
    #[test]
    #[ignore = "requires CALQUE_SOURCE_DATABASE_URL or TEST_DATABASE_URL"]
    fn an_older_enrollment_records_its_fingerprint_at_the_next_start() {
        let mut fixture = ReconnectFixture::create("cdc_adopt");
        let current = |fixture: &ReconnectFixture| {
            let runtime = fixture.start(OperationsMetrics::new());
            let health = runtime.health();
            wait_until(Duration::from_secs(10), "a current runtime", || {
                health.is_current()
            });
            runtime.shutdown().unwrap();
        };
        current(&fixture);
        let source_id = fixture.state.names.source_id.as_str().to_owned();
        let fingerprint = |fixture: &mut ReconnectFixture| -> Option<String> {
            fixture
                .client
                .query_one(
                    "SELECT publication_fingerprint FROM calque_control.source_slot_enrollment
                      WHERE source_id = $1",
                    &[&source_id],
                )
                .unwrap()
                .get(0)
        };
        let enrolled = fingerprint(&mut fixture).expect("enrollment records a fingerprint");
        fixture
            .client
            .execute(
                "UPDATE calque_control.source_slot_enrollment
                    SET publication_fingerprint = NULL WHERE source_id = $1",
                &[&source_id],
            )
            .unwrap();

        current(&fixture);
        assert_eq!(fingerprint(&mut fixture), Some(enrolled));

        let publication = quote_identifier(&fixture.state.names.publication_name);
        fixture
            .client
            .batch_execute(&format!(
                "ALTER PUBLICATION {publication} DROP TABLE public.items;
                 ALTER PUBLICATION {publication} ADD TABLE public.items;"
            ))
            .unwrap();
        let refused = match fixture.try_start(OperationsMetrics::new()) {
            Err(error) => error,
            Ok(runtime) => {
                let health = runtime.health();
                wait_until(Duration::from_secs(10), "the runtime to stop", || {
                    !health.is_healthy()
                });
                runtime.shutdown().unwrap_err()
            }
        };
        assert!(refused.publication_drifted(), "{refused:?}");
    }

    #[test]
    #[ignore = "requires CALQUE_SOURCE_DATABASE_URL or TEST_DATABASE_URL"]
    fn enrollment_names_what_a_role_lacks_to_create_the_publication() {
        let mut fixture = ReconnectFixture::create_with_own_role("create_denied");
        let role = fixture.role.clone().unwrap();
        let database = fixture.database_name.clone();
        let publication = fixture.state.names.publication_name.clone();
        let prepare = |fixture: &ReconnectFixture| {
            prepare_local_source(
                &fixture.database_url,
                &fixture.state,
                &LocalSecrets::generate().unwrap(),
                None,
            )
            .map(|_| ())
        };

        fixture.admin_in_database(&format!("REVOKE CREATE ON DATABASE {database} FROM {role}"));
        let error = prepare(&fixture).unwrap_err();
        assert_eq!(
            error.to_string(),
            format!("role \"{role}\" cannot create publication \"{publication}\" on database \"{database}\": it lacks CREATE on the database; run GRANT CREATE ON DATABASE \"{database}\" TO \"{role}\""),
        );
        assert_eq!(
            postgres_refusal(&error),
            Some(PostgresRefusal::InsufficientPrivilege)
        );

        fixture.admin_in_database(&format!("GRANT CREATE ON DATABASE {database} TO {role}"));
        fixture
            .client
            .batch_execute("GRANT CREATE ON SCHEMA public TO CURRENT_USER")
            .unwrap();
        fixture.admin_in_database(&format!(
            "GRANT CREATE ON SCHEMA public TO CURRENT_USER;
             ALTER TABLE public.items OWNER TO CURRENT_USER;
             GRANT ALL ON public.items TO {role}"
        ));
        let error = prepare(&fixture).unwrap_err();
        assert_eq!(
            error.to_string(),
            format!("the PostgreSQL publication {publication} does not exist, and Calque's role cannot create it because it does not own public.items. Have the tables' owner create it: CREATE PUBLICATION \"{publication}\" FOR TABLE \"public\".\"items\";"),
        );

        fixture.admin_in_database(&format!("ALTER TABLE public.items OWNER TO {role}"));
        prepare(&fixture).unwrap();

        // The control schema is created when the runtime starts, and the same
        // missing CREATE refuses it there.
        fixture.admin_in_database(&format!("REVOKE CREATE ON DATABASE {database} FROM {role}"));
        let error = fixture.try_start(OperationsMetrics::new()).err().unwrap();
        assert_eq!(
            error.to_string(),
            format!("role \"{role}\" lacks a privilege calque needs on the source. It needs CONNECT and CREATE on database \"{database}\" (GRANT CONNECT, CREATE ON DATABASE \"{database}\" TO \"{role}\") and SELECT on the enrolled tables, with INSERT, UPDATE and DELETE when pages write (GRANT SELECT, INSERT, UPDATE, DELETE ON <table> TO \"{role}\")"),
        );
    }

    #[test]
    #[ignore = "requires CALQUE_SOURCE_DATABASE_URL or TEST_DATABASE_URL"]
    fn a_revoked_replication_privilege_stops_the_runtime_instead_of_reconnecting() {
        let mut fixture = ReconnectFixture::create_with_own_role("cdc_revoked");
        let metrics = OperationsMetrics::new();
        let runtime = fixture.start(metrics.clone());
        let health = runtime.health();
        wait_until(Duration::from_secs(10), "a current runtime", || {
            health.is_current()
        });

        // The role still logs in on a regular connection; only a walsender is
        // refused it.
        fixture.alter_role("NOREPLICATION");
        assert_eq!(fixture.terminate("walsender"), vec![true]);

        wait_until(Duration::from_secs(10), "the runtime to stop", || {
            !health.is_healthy()
        });
        let role = fixture.role.clone().unwrap();
        assert_eq!(
            health.failure().as_deref(),
            Some(
                format!("cdc-refused: role \"{role}\" cannot start replication, which needs the REPLICATION attribute. Run ALTER ROLE \"{role}\" REPLICATION as a superuser; on RDS, run GRANT rds_replication TO \"{role}\" instead")
                    .as_str()
            )
        );
        assert_eq!(reconnects(&metrics), 0);
        assert_eq!(fixture.backends("walsender"), 0);
        let error = runtime.shutdown().unwrap_err();
        assert!(
            matches!(
                error,
                LocalSourceError::Runtime(SourceRuntimeError::SourceRefused(
                    PostgresRefusal::ReplicationPrivilegeMissing
                ))
            ),
            "{error:?}"
        );
    }

    /// Amazon RDS and Aurora grant replication through membership in
    /// `rds_replication` and leave the REPLICATION attribute unset. A stock
    /// PostgreSQL refuses such a role a walsender, which stands in here for a
    /// refusal RDS would lift by itself, such as every WAL sender being busy.
    #[test]
    #[ignore = "requires CALQUE_SOURCE_DATABASE_URL or TEST_DATABASE_URL"]
    fn a_role_replicating_through_rds_replication_is_inspected_ready_and_retried() {
        let mut fixture = ReconnectFixture::create_with_own_role("cdc_rds_role");
        let metrics = OperationsMetrics::new();
        let runtime = fixture.start(metrics.clone());
        let health = runtime.health();
        wait_until(Duration::from_secs(10), "a current runtime", || {
            health.is_current()
        });
        let tables = fixture.state.tables.iter().cloned().collect();
        let may_replicate = || {
            inspect_local_source(
                &fixture.database_url,
                &fixture.state.names.publication_name,
                &tables,
            )
            .unwrap()
            .settings
            .current_user_has_replication
        };

        fixture.alter_role("NOREPLICATION");
        assert!(!may_replicate());
        let role = fixture.role.clone().unwrap();
        Client::connect(&fixture.admin_url, postgres::NoTls)
            .unwrap()
            .batch_execute(&format!(
                "DO $$ BEGIN
                   IF NOT EXISTS (SELECT 1 FROM pg_catalog.pg_roles
                                   WHERE rolname = 'rds_replication') THEN
                     CREATE ROLE rds_replication NOLOGIN;
                   END IF;
                 END $$;
                 GRANT rds_replication TO {role};"
            ))
            .unwrap();
        assert!(may_replicate());

        assert_eq!(fixture.terminate("walsender"), vec![true]);
        // Attempts start 100 ms apart and double, so several are refused here.
        std::thread::sleep(Duration::from_secs(3));
        assert!(health.is_healthy(), "{:?}", health.failure());
        assert!(
            health
                .failure()
                .is_some_and(|failure| failure.starts_with("cdc-reconnecting: ")),
            "{:?}",
            health.failure()
        );
        assert_eq!(reconnects(&metrics), 0);

        fixture.alter_role("REPLICATION");
        wait_until(Duration::from_secs(30), "a current runtime again", || {
            health.is_current()
        });
        runtime.shutdown().unwrap();
    }

    #[test]
    #[ignore = "requires CALQUE_SOURCE_DATABASE_URL or TEST_DATABASE_URL"]
    fn a_rotated_password_stops_the_runtime_instead_of_reconnecting() {
        let mut fixture = ReconnectFixture::create_with_own_role("cdc_password");
        let metrics = OperationsMetrics::new();
        let runtime = fixture.start(metrics.clone());
        let health = runtime.health();
        wait_until(Duration::from_secs(10), "a current runtime", || {
            health.is_current()
        });

        // Open sessions keep running; the next login is refused.
        fixture.alter_role(&format!("PASSWORD '{}'", uuid::Uuid::new_v4().simple()));
        assert_eq!(fixture.terminate("walsender"), vec![true]);

        wait_until(Duration::from_secs(10), "the runtime to stop", || {
            !health.is_healthy()
        });
        let role = fixture.role.clone().unwrap();
        let database = fixture.database_name.clone();
        assert_eq!(
            health.failure().as_deref(),
            Some(
                format!("cdc-refused: PostgreSQL refused to authenticate role \"{role}\". Check the password in the database URL, that the role may log in, and that pg_hba.conf lets it connect to database \"{database}\" from this host")
                    .as_str()
            )
        );
        assert_eq!(reconnects(&metrics), 0);
        assert_eq!(fixture.backends("walsender"), 0);
        let error = runtime.shutdown().unwrap_err();
        assert_eq!(
            postgres_refusal(&error),
            Some(PostgresRefusal::AuthenticationFailed),
            "{error:?}"
        );
    }

    /// A role whose defaults would make every text value unreadable to the
    /// decoders enrolls anyway, because calque opens each connection with the
    /// settings it needs. The snapshot's binary COPY and the CDC stream's text
    /// then carry exactly what PostgreSQL holds.
    #[test]
    #[ignore = "requires CALQUE_SOURCE_DATABASE_URL or TEST_DATABASE_URL"]
    fn a_role_with_its_own_session_settings_enrolls_and_its_values_arrive_exact() {
        let mut fixture = ReconnectFixture::create_with_own_role("session_settings");
        fixture
            .client
            .batch_execute(
                "CREATE TABLE public.moments (
                     id bigint PRIMARY KEY,
                     at timestamptz NOT NULL,
                     day date NOT NULL,
                     payload bytea NOT NULL,
                     ratio float8 NOT NULL
                 );
                 INSERT INTO public.moments VALUES
                     (1, '2024-07-14 23:30:15.123456+02', '2024-12-31',
                      decode('00ff5c7800', 'hex'), 0.1::float8 + 0.2::float8);",
            )
            .unwrap();
        let suffix = uuid::Uuid::new_v4().simple().to_string();
        fixture.state = LocalState::new(
            inspect_local_identity(&fixture.database_url).unwrap(),
            sync_types::SourceEpoch::new(format!("epoch-{suffix}")).unwrap(),
            LocalNames {
                source_id: sync_types::SourceId::new(format!("session-settings-{suffix}")).unwrap(),
                publication_name: "session_settings_pub".to_owned(),
                slot_name: format!("session_settings_slot_{suffix}"),
            },
            vec![QualifiedTable {
                schema: "public".to_owned(),
                table: "moments".to_owned(),
            }],
        )
        .unwrap();
        for setting in [
            "SET timezone = 'Europe/Paris'",
            "SET datestyle = 'SQL, DMY'",
            "SET bytea_output = 'escape'",
            "SET extra_float_digits = 0",
        ] {
            fixture.alter_role(setting);
        }

        let prepared = prepare_local_source(
            &fixture.database_url,
            &fixture.state,
            &LocalSecrets::generate().unwrap(),
            None,
        )
        .expect("a role whose defaults differ enrolls");
        let held = |client: &mut Client, id: i64| -> (i64, String, String, f64) {
            let row = client
                .query_one(
                    "SELECT (extract(epoch FROM at) * 1000000)::bigint,
                            to_char(day, 'YYYY-MM-DD'), encode(payload, 'hex'), ratio
                       FROM public.moments WHERE id = $1",
                    &[&id],
                )
                .unwrap();
            (row.get(0), row.get(1), row.get(2), row.get(3))
        };
        assert_eq!(held(&mut fixture.client, 1).0, 1_720_992_615_123_456);
        assert_eq!(held(&mut fixture.client, 1).3, 0.1_f64 + 0.2_f64);

        let snapshot =
            snapshot_builder::SnapshotBuildPlan::from_registry(&prepared.registry, &prepared.scope)
                .unwrap();
        let table = &snapshot.tables()[0];
        let mut copier = database_url_endpoint(
            &fixture.database_url,
            PostgresConnectionFamily::SnapshotCopy,
        );
        let types = table.copy_types();
        let reader = copier.copy_out(table.copy_sql()).unwrap();
        use postgres::fallible_iterator::FallibleIterator as _;
        let mut rows = postgres::binary_copy::BinaryCopyOutIter::new(reader, &types);
        let row = rows.next().unwrap().unwrap();
        let copied = snapshot_builder::decode_direct_columnar_row(
            table,
            &row,
            snapshot_builder::CopyLoadLimits::default(),
        )
        .unwrap()
        .0;
        assert!(rows.next().unwrap().is_none());
        drop(rows);
        let (at, day, payload, ratio) = held(&mut fixture.client, 1);
        assert_eq!(
            copied[1..],
            [
                direct_columnar::Value::TimestampMicros(at),
                direct_columnar::Value::DateText(day),
                direct_columnar::Value::Bytes(hex::decode(payload).unwrap()),
                direct_columnar::Value::F64(ratio),
            ]
        );

        let runtime = fixture.start(OperationsMetrics::new());
        let health = runtime.health();
        wait_until(Duration::from_secs(10), "a current runtime", || {
            health.is_current()
        });
        fixture
            .client
            .batch_execute(
                "INSERT INTO public.moments VALUES
                     (2, '2031-01-02 03:04:05.000001-08', '2031-02-03',
                      decode('5c5c5c', 'hex'), 1e-7::float8);
                 UPDATE public.moments SET at = '1999-12-31 23:59:59.999999+05:30',
                     ratio = 123456789.123456789::float8 WHERE id = 1;",
            )
            .unwrap();
        wait_until(
            Duration::from_secs(10),
            "the transaction to be logged",
            || fixture.canonical_operations().len() >= 2,
        );
        let operations = fixture.canonical_operations();
        let cell = |operation: &serde_json::Value, column: u64| -> serde_json::Value {
            let columns = operation
                .get("columns")
                .or_else(|| operation.get("changed_columns"))
                .unwrap();
            columns
                .as_array()
                .unwrap()
                .iter()
                .find(|cell| cell["columnId"] == column)
                .map_or(serde_json::Value::Null, |cell| cell["value"].clone())
        };
        let (insert, update) = (&operations[0], &operations[1]);
        assert_eq!(insert["kind"], "INSERT_FULL_ROW", "{insert}");
        assert_eq!(update["kind"], "UPDATE_PATCH", "{update}");
        let (at, day, payload, ratio) = held(&mut fixture.client, 2);
        assert_eq!(
            cell(insert, 2),
            serde_json::json!({"kind": "TIMESTAMP_UNIX_MICROS", "value": at})
        );
        assert_eq!(
            cell(insert, 3),
            serde_json::json!({"kind": "DATE_ISO8601", "value": day})
        );
        assert_eq!(
            cell(insert, 4)["value"],
            serde_json::json!(hex::decode(payload).unwrap())
        );
        assert_eq!(cell(insert, 5)["value"].as_f64(), Some(ratio));
        let (at, _, _, ratio) = held(&mut fixture.client, 1);
        assert_eq!(
            cell(update, 2),
            serde_json::json!({"kind": "TIMESTAMP_UNIX_MICROS", "value": at})
        );
        assert_eq!(cell(update, 5)["value"].as_f64(), Some(ratio));
        runtime.shutdown().unwrap();
    }

    #[test]
    #[ignore = "requires CALQUE_SOURCE_DATABASE_URL or TEST_DATABASE_URL"]
    fn a_runtime_stopped_gracefully_releases_the_lease_to_the_next_start() {
        let mut fixture = ReconnectFixture::create("cdc_released");
        let first = fixture.start(OperationsMetrics::new());
        let health = first.health();
        wait_until(Duration::from_secs(10), "a current runtime", || {
            health.is_current()
        });
        let held = fixture.lease();
        assert_eq!(held.2, "active");
        first.shutdown().unwrap();
        // A clean stop releases the row after the slot goes inactive, so the
        // next start has nothing to wait out.
        let released: (String, Option<String>, Option<std::time::SystemTime>) = {
            let row = fixture
                .client
                .query_one(
                    "SELECT lease_phase, owner_id, expires_at
                       FROM calque_control.source_fence_lease WHERE source_id = $1",
                    &[&fixture.state.names.source_id.as_str()],
                )
                .unwrap();
            (row.get(0), row.get(1), row.get(2))
        };
        assert_eq!(released, ("unowned".to_owned(), None, None));

        let second = fixture.start(OperationsMetrics::new());
        let taken = fixture.lease();
        assert_ne!(taken.0, held.0);
        assert!(taken.1 > held.1, "{taken:?} after {held:?}");
        assert_eq!(taken.2, "active");
        second.shutdown().unwrap();
    }

    #[test]
    #[ignore = "requires CALQUE_SOURCE_DATABASE_URL or TEST_DATABASE_URL"]
    fn a_runtime_killed_while_it_held_the_lease_does_not_block_the_next_start() {
        let mut fixture = ReconnectFixture::create("cdc_killed");
        let first = fixture.start(OperationsMetrics::new());
        let health = first.health();
        wait_until(Duration::from_secs(10), "a current runtime", || {
            health.is_current()
        });
        let killed = fixture.lease();
        assert_eq!(killed.2, "active");
        first.shutdown().unwrap();
        // A process killed here, by `docker stop` after its grace for one,
        // leaves the row as it last renewed it: its own and active until one
        // TTL later. Put it back that way, two seconds from expiry.
        fixture.hold_lease(&killed, 2.0);

        let started = Instant::now();
        let second = fixture
            .try_start(OperationsMetrics::new())
            .expect("the next start waits out the lease a killed runtime left");
        let waited = started.elapsed();
        // It took the lease once the row expired, and not before.
        assert!(waited >= Duration::from_millis(1900), "{waited:?}");
        let taken = fixture.lease();
        assert_ne!(taken.0, killed.0);
        assert!(taken.1 > killed.1, "{taken:?} after {killed:?}");
        assert_eq!(taken.2, "active");
        let health = second.health();
        wait_until(
            Duration::from_secs(10),
            "the next runtime to be current",
            || health.is_current(),
        );
        second.shutdown().unwrap();
    }

    #[test]
    #[ignore = "requires CALQUE_SOURCE_DATABASE_URL or TEST_DATABASE_URL"]
    fn a_lease_held_past_the_startup_wait_refuses_the_start_and_says_so() {
        let mut fixture = ReconnectFixture::create("cdc_held");
        let first = fixture.start(OperationsMetrics::new());
        let health = first.health();
        wait_until(Duration::from_secs(10), "a current runtime", || {
            health.is_current()
        });
        let running = fixture.lease();
        first.shutdown().unwrap();
        // A holder that is still running keeps pushing its expiry out.
        fixture.hold_lease(&running, 60.0);

        let started = Instant::now();
        let refused = fixture.try_start(OperationsMetrics::new());
        let waited = started.elapsed();
        assert!(
            matches!(
                refused,
                Err(LocalSourceError::Fence(FenceError::LeaseHeld { .. }))
            ),
            "{:?}",
            refused.err()
        );
        // One three-second TTL plus the margin.
        assert!(waited >= Duration::from_secs(4), "{waited:?}");
        assert_eq!(fixture.lease(), running);
    }

    #[tokio::test]
    async fn existing_published_artifact_cannot_bypass_an_initial_ingest_failure() {
        use artifact_catalog::{PublishOutcome, PublishedArtifactQuery, SqliteArtifactCatalog};
        use blob_store::LocalFilesystemBlobStore;
        use snapshot_builder::SnapshotBuildPlan;
        use sync_types::{ArtifactId, SourceEpoch};

        let temporary = tempfile::tempdir().unwrap();
        let prepared = crate::snapshot::tests::prepared_source(
            SourceEpoch::new("existing-artifact-ingest-failure").unwrap(),
        );
        let plan = SnapshotBuildPlan::from_registry(&prepared.registry, &prepared.scope).unwrap();
        let artifact_id = ArtifactId::new("existing-artifact-ingest-failure").unwrap();
        let bytes = b"published-before-restart";
        let manifest =
            crate::snapshot::tests::retry_manifest(bytes, &artifact_id, &prepared, &plan);
        let mut catalog =
            SqliteArtifactCatalog::open(temporary.path().join("catalog.sqlite3")).unwrap();
        let store: std::sync::Arc<dyn blob_store::ManagedBlobStore> = std::sync::Arc::new(
            LocalFilesystemBlobStore::open(
                temporary.path().join("objects"),
                crate::MAX_LOCAL_ARTIFACT_BYTES,
            )
            .unwrap(),
        );
        let lease = catalog
            .register_build(&manifest, "existing-artifact-owner", 1_000, 60_000)
            .unwrap();
        catalog.mark_uploading(&lease, 1_001).unwrap();
        crate::snapshot::tests::upload_retry_objects(&store, &manifest, bytes).await;
        catalog.mark_uploaded(&lease, 1_002).unwrap();
        assert_eq!(
            catalog
                .verify_and_publish(&lease, store.as_ref(), 1_003)
                .await
                .unwrap(),
            PublishOutcome::Published
        );
        assert_eq!(
            catalog
                .published(&PublishedArtifactQuery {
                    cache_key: manifest.cache_key(),
                    table_set_hash: manifest.manifest().compatibility.table_set_hash.clone(),
                })
                .unwrap()
                .len(),
            1
        );

        let healthy = Arc::new(AtomicBool::new(false));
        let current = Arc::new(AtomicBool::new(false));
        let failure = Arc::new(StdMutex::new(None));
        let guard = HealthExitGuard {
            healthy: Arc::clone(&healthy),
            current,
            connected: Arc::new(AtomicBool::new(false)),
            failure,
        };
        let (ready_tx, ready_rx) = mpsc::sync_channel(1);

        let started = std::time::Instant::now();
        let result = publish_ready_after_initial_cycle(&guard, &ready_tx, (), || {
            Err(LocalSourceError::CdcStartupHandshake)
        });

        assert!(matches!(result, Err(LocalSourceError::CdcStartupHandshake)));
        assert!(started.elapsed() < Duration::from_millis(100));
        assert!(!healthy.load(Ordering::Acquire));
        assert!(matches!(
            ready_rx.try_recv(),
            Err(mpsc::TryRecvError::Empty)
        ));
    }

    #[test]
    fn cdc_startup_failure_is_synchronous_without_joining_cleanup() {
        let (stop, _stop_rx) = mpsc::channel();
        let (startup_tx, startup_rx) = mpsc::sync_channel(1);
        let (release_tx, release_rx) = mpsc::channel::<()>();
        let join = thread::spawn(move || {
            let reported = report_startup_failure(
                &startup_tx,
                LocalSourceError::Fence(FenceError::LeaseHeld {
                    expires_at_unix_millis: 7,
                }),
            );
            let _ = release_rx.recv();
            reported
        });
        let error = match await_cdc_startup_with_timeout(
            startup_rx,
            Duration::from_secs(1),
            &stop,
            Arc::new(SourceStartupShutdown::new()),
            join,
        ) {
            Ok(_) => panic!("startup failure must not return a runtime"),
            Err(error) => error,
        };
        // The cause reaches the caller, not a bare handshake failure.
        assert!(matches!(
            error,
            LocalSourceError::Fence(FenceError::LeaseHeld {
                expires_at_unix_millis: 7
            })
        ));
        drop(release_tx);
    }

    #[test]
    fn a_startup_failure_nobody_waits_for_stays_with_the_thread() {
        let (startup_tx, startup_rx) = mpsc::sync_channel(1);
        drop(startup_rx);
        assert!(matches!(
            report_startup_failure(&startup_tx, LocalSourceError::EmptyTableSet),
            Err(LocalSourceError::EmptyTableSet)
        ));
    }

    #[test]
    fn cdc_startup_timeout_returns_before_session_owner_cleanup_finishes() {
        let (stop, stop_rx) = mpsc::channel();
        let (_startup_tx, startup_rx) = mpsc::sync_channel(1);
        let (release_tx, release_rx) = mpsc::channel();
        let (done_tx, done_rx) = mpsc::channel();
        let join = thread::spawn(move || {
            stop_rx.recv().unwrap();
            release_rx.recv().unwrap();
            done_tx.send(()).unwrap();
            Ok(())
        });

        let started = std::time::Instant::now();
        let error = match await_cdc_startup_with_timeout(
            startup_rx,
            Duration::from_millis(1),
            &stop,
            Arc::new(SourceStartupShutdown::new()),
            join,
        ) {
            Ok(_) => panic!("deadline must fail startup"),
            Err(error) => error,
        };
        assert!(matches!(error, LocalSourceError::CdcStartupTimeout));
        assert!(started.elapsed() < Duration::from_millis(100));

        release_tx.send(()).unwrap();
        done_rx.recv_timeout(Duration::from_secs(1)).unwrap();
    }

    #[test]
    fn the_ingest_observer_counts_appended_entries_apart_from_append_commits() {
        let metrics = OperationsMetrics::new();
        let observer = CanonicalHeadIngestObserver {
            signal: snapshot_pipeline::CanonicalHeadSignal::default(),
            metrics: metrics.clone(),
            append_started: StdMutex::new(None),
        };
        for entries in [128, 0, 3] {
            observer.observe(PersistentIngestBoundary::BeforeCanonicalAppend);
            observer.observe_appended_entries(entries);
            observer.observe(PersistentIngestBoundary::AfterCanonicalAppend);
        }

        let encoded = metrics.encode().unwrap();
        assert!(
            encoded.contains("calque_canonical_entries_appended_total 131\n"),
            "{encoded}"
        );
        assert!(
            encoded.contains("calque_canonical_append_duration_seconds_count 3\n"),
            "{encoded}"
        );
    }
}
