use std::{
    collections::BTreeSet,
    fmt,
    sync::{
        atomic::{AtomicBool, Ordering},
        mpsc, Arc,
    },
    thread,
    time::{Duration, Instant},
};

use canonical_log::PostgresDatastoreIdentity;
use pg_walstream::{parse_lsn, PgReplicationConnection, PgReplicationShutdownHandle};
use postgres::{error::SqlState, Client, Row};
use source_control::{ActivatedSource, SchemaRegistryBinding, SourceIdentity};
use sync_types::{SourceEpoch, SourceId};
use thiserror::Error;

use crate::{
    inspect_owned_publication, ActivationEvidence, ActiveLease, FenceAuthority, FenceError,
    OwnedPublication, PostgresFenceStore, PostgresRefusal, ProvisionalLease, RenewalClock,
};

const SLOT_INACTIVE_POLL_ATTEMPTS: usize = 100;
const SLOT_INACTIVE_POLL_INTERVAL: Duration = Duration::from_millis(10);
const PGOUTPUT_PROFILE_V1: &str = "v1-no-streaming";
// Every source-controller statement runs under these, except slot creation's
// wait for open transactions.
const CONTROLLER_TIMEOUTS: &str = "SET statement_timeout = '2s'; SET lock_timeout = '1s';";
/// How long creating a slot waits for open transactions to end, unless
/// `SlotProvisioningConfig::with_open_transaction_wait` says otherwise.
///
/// Half of calque's 30 s CDC startup deadline, so a transaction open for
/// longer ends startup with `SlotCreationWaitedForOpenTransactions` naming it
/// instead of a startup timeout. The wait holds back only WAL removal and
/// catalog vacuum, and queues no one behind it.
pub const DEFAULT_OPEN_TRANSACTION_WAIT: Duration = Duration::from_secs(15);

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SlotProvisioningConfig {
    slot_name: String,
    open_transaction_wait: Duration,
}

impl SlotProvisioningConfig {
    pub fn new(slot_name: impl Into<String>) -> Result<Self, SourceRuntimeError> {
        let config = Self {
            slot_name: slot_name.into(),
            open_transaction_wait: DEFAULT_OPEN_TRANSACTION_WAIT,
        };
        validate_slot_name(&config.slot_name)?;
        Ok(config)
    }

    /// Replaces how long slot creation waits for open transactions to end.
    #[must_use]
    pub fn with_open_transaction_wait(mut self, wait: Duration) -> Self {
        self.open_transaction_wait = wait;
        self
    }
}

/// Durable one-shot binding between an activated source epoch and its
/// persistent slot baseline. Normal runtime startup can only load this record;
/// it cannot recreate a missing slot under the same epoch.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SlotEnrollment {
    pub(crate) source_id: SourceId,
    pub(crate) source_epoch: SourceEpoch,
    pub(crate) slot_name: String,
    pub(crate) publication_name: String,
    pub(crate) system_identifier: String,
    pub(crate) database_name: String,
    pub(crate) timeline_id: u32,
    pub(crate) previous_timeline_id: u32,
    pub(crate) pgoutput_profile: String,
    pub(crate) initial_lsn_text: String,
    pub(crate) durable_canonical_lsn_text: String,
}

impl SlotEnrollment {
    pub fn source_epoch(&self) -> &SourceEpoch {
        &self.source_epoch
    }

    pub fn slot_name(&self) -> &str {
        &self.slot_name
    }

    pub fn publication_name(&self) -> &str {
        &self.publication_name
    }

    pub fn pgoutput_profile(&self) -> &str {
        &self.pgoutput_profile
    }

    pub fn initial_lsn(&self) -> Result<u64, SourceRuntimeError> {
        parse_lsn(&self.initial_lsn_text).map_err(|_| SourceRuntimeError::InvalidSlotLsn)
    }

    pub fn durable_canonical_lsn(&self) -> Result<u64, SourceRuntimeError> {
        parse_lsn(&self.durable_canonical_lsn_text).map_err(|_| SourceRuntimeError::InvalidSlotLsn)
    }

    pub(crate) fn from_control_row(row: postgres::Row) -> Result<Self, FenceError> {
        let timeline_id: i64 = row.get(6);
        let previous_timeline_id: i64 = row.get(7);
        Ok(Self {
            source_id: SourceId::new(row.get::<_, String>(0))
                .map_err(|_| FenceError::SlotEnrollmentMismatch)?,
            source_epoch: SourceEpoch::new(row.get::<_, String>(1))
                .map_err(|_| FenceError::SlotEnrollmentMismatch)?,
            slot_name: row.get(2),
            publication_name: row.get(3),
            system_identifier: row.get(4),
            database_name: row.get(5),
            timeline_id: u32::try_from(timeline_id)
                .map_err(|_| FenceError::SlotEnrollmentMismatch)?,
            previous_timeline_id: u32::try_from(previous_timeline_id)
                .map_err(|_| FenceError::SlotEnrollmentMismatch)?,
            pgoutput_profile: row.get(8),
            initial_lsn_text: row.get(9),
            durable_canonical_lsn_text: row.get(10),
        })
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SlotMetadata {
    pub slot_name: String,
    pub slot_type: String,
    pub plugin: Option<String>,
    pub database: Option<String>,
    pub temporary: bool,
    pub active_pid: Option<i32>,
    pub confirmed_flush_lsn: u64,
    pub wal_status: Option<String>,
    pub two_phase: bool,
    pub invalidation_reason: Option<String>,
    pub failover: bool,
    pub synchronized: bool,
}

/// Owns the active persistent replication socket and its external fence.
///
/// Establishing the session starts the stream so that PostgreSQL itself binds
/// the slot to the newly fenced backend. The session has no feedback method of
/// its own: reads, append and receipt durability, and the standby-status write
/// happen inside [`Self::ingest_bounded`].
pub struct ActiveSourceSession {
    pub(crate) replication: Option<PgReplicationConnection>,
    pub(crate) io_runtime: Option<tokio::runtime::Runtime>,
    pub(crate) authority: Arc<FenceAuthority<PostgresFenceStore>>,
    pub(crate) lease: ActiveLease,
    pub(crate) slot: SlotMetadata,
    pub(crate) backend_pid: i32,
    pub(crate) source_connection: String,
    pub(crate) canonical_log_connection: String,
    pub(crate) canonical_datastore: PostgresDatastoreIdentity,
    pub(crate) expected_source: SourceIdentity,
    pub(crate) activated_source: ActivatedSource,
    pub(crate) ingest_state: crate::persistent_ingest::PersistentIngestState,
    watchdog: Option<SessionWatchdog>,
}

struct SessionWatchdog {
    stop: mpsc::Sender<()>,
    join: Option<thread::JoinHandle<()>>,
    failed: Arc<AtomicBool>,
}

struct ProvisionalWatchdog {
    stop: mpsc::Sender<()>,
    join: Option<thread::JoinHandle<()>>,
}

impl ProvisionalWatchdog {
    fn stop(mut self) {
        let _ = self.stop.send(());
        if let Some(join) = self.join.take() {
            let _ = join.join();
        }
    }
}

impl std::fmt::Debug for ActiveSourceSession {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("ActiveSourceSession")
            .field("lease", &self.lease)
            .field("slot", &self.slot)
            .field("backend_pid", &self.backend_pid)
            .field("socket_open", &self.socket_is_open())
            .finish()
    }
}

impl ActiveSourceSession {
    pub fn lease(&self) -> &ActiveLease {
        &self.lease
    }

    pub fn slot(&self) -> &SlotMetadata {
        &self.slot
    }

    pub const fn backend_pid(&self) -> i32 {
        self.backend_pid
    }

    pub fn socket_is_open(&self) -> bool {
        self.lease.is_locally_valid()
            && !self
                .watchdog
                .as_ref()
                .is_some_and(|watchdog| watchdog.failed.load(Ordering::Acquire))
            && self
                .replication
                .as_ref()
                .is_some_and(PgReplicationConnection::is_alive)
    }

    pub fn shutdown(mut self) -> Result<(), SourceRuntimeError> {
        self.stop_watchdog();
        self.ingest_state.reset_terminal();
        self.lease.revoke();
        self.replication.take();
        self.io_runtime.take();
        let mut controller = connect_source_controller(&self.source_connection)?;
        wait_for_inactive_slot(&mut controller, &self.expected_source, &self.slot.slot_name)?;
        self.authority.release_revoked_active(&self.lease)?;
        Ok(())
    }

    pub(crate) fn close_source_socket(&mut self) {
        self.stop_watchdog();
        self.ingest_state.reset_terminal();
        self.replication.take();
        self.io_runtime.take();
        self.lease.revoke();
    }

    fn stop_watchdog(&mut self) {
        let Some(mut watchdog) = self.watchdog.take() else {
            return;
        };
        let _ = watchdog.stop.send(());
        if let Some(join) = watchdog.join.take() {
            let _ = join.join();
        }
    }
}

impl Drop for ActiveSourceSession {
    fn drop(&mut self) {
        self.close_source_socket();
    }
}

/// Explicitly creates and durably binds the one persistent slot for an
/// activated source epoch. This is not a normal runtime-start operation.
pub fn provision_persistent_slot(
    authority: &FenceAuthority<PostgresFenceStore>,
    provisional: &ProvisionalLease,
    source_connection: &str,
    activated_source: &ActivatedSource,
    config: &SlotProvisioningConfig,
) -> Result<SlotEnrollment, SourceRuntimeError> {
    validate_activated_lease(provisional, activated_source)?;
    authority.validate(provisional)?;
    if authority.load_slot_enrollment(provisional)?.is_some() {
        return Err(FenceError::SlotLifecycleAlreadyInitialized.into());
    }
    let binding = activated_source.registry_binding();
    let expected_source = binding.source_identity();
    let mut controller = connect_source_controller(source_connection)?;
    verify_regular_source(&mut controller, expected_source)?;
    verify_publication(&mut controller, binding)?;
    if inspect_slot(&mut controller, &config.slot_name)?.is_some() {
        return Err(SourceRuntimeError::SlotAlreadyExists);
    }
    while_renewing(authority, provisional, || {
        create_slot(&mut controller, config)
    })?;
    let slot = inspect_slot(&mut controller, &config.slot_name)?
        .ok_or(SourceRuntimeError::SlotMissingReseedRequired)?;
    validate_slot(&slot, expected_source)?;
    let initial_lsn_text = format_lsn(slot.confirmed_flush_lsn);
    let enrollment = SlotEnrollment {
        source_id: provisional.identity().source_id().clone(),
        source_epoch: provisional.identity().source_epoch().clone(),
        slot_name: config.slot_name.clone(),
        publication_name: binding.publication().name.clone(),
        system_identifier: expected_source.system_identifier.clone(),
        database_name: expected_source.database_name.clone(),
        timeline_id: expected_source.timeline_id,
        previous_timeline_id: expected_source.previous_timeline_id,
        pgoutput_profile: PGOUTPUT_PROFILE_V1.to_owned(),
        initial_lsn_text: initial_lsn_text.clone(),
        durable_canonical_lsn_text: initial_lsn_text,
    };
    authority.record_slot_enrollment(provisional, &enrollment)?;
    Ok(enrollment)
}

/// Takes ownership of the persistent source's slot for this process.
///
/// The steps run in this order: provisional lease, source/slot verification,
/// stale backend termination, observed inactivity, durable-log writer claim,
/// new replication backend binding, PostgreSQL slot claim, and only then the
/// active lease transition.
pub fn establish_persistent_source(
    authority: Arc<FenceAuthority<PostgresFenceStore>>,
    provisional: ProvisionalLease,
    source_connection: &str,
    replication_connection: &str,
    activated_source: &ActivatedSource,
) -> Result<ActiveSourceSession, SourceRuntimeError> {
    establish_persistent_source_observed(
        authority,
        provisional,
        source_connection,
        replication_connection,
        activated_source,
        |_| {},
    )
}

/// Establishes a persistent source with an independent emergency shutdown
/// capability that becomes effective as soon as the replication socket exists.
///
/// A request made before socket creation is remembered. This lets a composition
/// enforce one startup deadline across `IDENTIFY_SYSTEM`, backend discovery,
/// and `START_REPLICATION` without racing socket publication.
pub fn establish_persistent_source_interruptible(
    authority: Arc<FenceAuthority<PostgresFenceStore>>,
    provisional: ProvisionalLease,
    source_connection: &str,
    replication_connection: &str,
    activated_source: &ActivatedSource,
    startup_shutdown: Arc<crate::SourceStartupShutdown>,
) -> Result<ActiveSourceSession, SourceRuntimeError> {
    establish_persistent_source_observed(
        authority,
        provisional,
        source_connection,
        replication_connection,
        activated_source,
        move |handle| startup_shutdown.publish(handle),
    )
}

fn establish_persistent_source_observed<F>(
    authority: Arc<FenceAuthority<PostgresFenceStore>>,
    provisional: ProvisionalLease,
    source_connection: &str,
    replication_connection: &str,
    activated_source: &ActivatedSource,
    on_shutdown_handle: F,
) -> Result<ActiveSourceSession, SourceRuntimeError>
where
    F: FnOnce(PgReplicationShutdownHandle),
{
    if let Err(error) = validate_activated_lease(&provisional, activated_source) {
        authority.release_after_failure(&provisional);
        return Err(error);
    }
    authority.validate(&provisional)?;
    let enrollment = match authority.load_slot_enrollment(&provisional)? {
        Some(enrollment) => enrollment,
        None => {
            authority.release_after_failure(&provisional);
            return Err(SourceRuntimeError::SlotLifecycleNotInitialized);
        }
    };
    if let Err(error) = validate_enrollment_binding(&enrollment, &provisional, activated_source) {
        authority.release_after_failure(&provisional);
        return Err(error);
    }
    let provisional_watchdog =
        match start_provisional_watchdog(Arc::clone(&authority), provisional.clone()) {
            Ok(watchdog) => watchdog,
            Err(error) => {
                authority.release_after_failure(&provisional);
                return Err(error);
            }
        };
    let prepared = prepare_source_session(
        &authority,
        &provisional,
        source_connection,
        replication_connection,
        activated_source,
        &enrollment,
        on_shutdown_handle,
    );
    provisional_watchdog.stop();
    let (replication, io_runtime, slot, backend_pid, claim, flushed_lsn) = match prepared {
        Ok(prepared) => prepared,
        Err(error) => {
            authority.release_after_failure(&provisional);
            return Err(error);
        }
    };
    if let Err(error) = authority.validate(&provisional) {
        drop(replication);
        authority.release_after_failure(&provisional);
        return Err(error.into());
    }
    let evidence =
        match ActivationEvidence::from_exclusion(provisional.identity(), backend_pid, &claim) {
            Ok(evidence) => evidence,
            Err(error) => {
                drop(replication);
                authority.release_after_failure(&provisional);
                return Err(error.into());
            }
        };
    let canonical_log_connection = claim.control_connection().to_owned();
    let canonical_datastore = claim.datastore().clone();
    let lease = match authority.activate(provisional, &evidence) {
        Ok(lease) => lease,
        Err(error) => {
            drop(replication);
            return Err(error.into());
        }
    };
    let expected_source = activated_source
        .registry_binding()
        .source_identity()
        .clone();
    let watchdog = match start_session_watchdog(
        Arc::clone(&authority),
        lease.clone(),
        source_connection.to_owned(),
        expected_source.clone(),
        slot.slot_name.clone(),
        backend_pid,
    ) {
        Ok(watchdog) => watchdog,
        Err(error) => {
            lease.revoke();
            drop(replication);
            if let Ok(mut controller) = connect_source_controller(source_connection) {
                let _ = wait_for_inactive_slot(&mut controller, &expected_source, &slot.slot_name);
            }
            if let Err(error) = authority.release_revoked_active(&lease) {
                tracing::warn!(%error, "a failed start did not release its source lease; the lease expires on its own");
            }
            return Err(error);
        }
    };
    let ingest_state =
        crate::persistent_ingest::PersistentIngestState::new(slot.confirmed_flush_lsn, flushed_lsn);
    Ok(ActiveSourceSession {
        replication: Some(replication),
        io_runtime: Some(io_runtime),
        authority,
        lease,
        slot,
        backend_pid,
        source_connection: source_connection.to_owned(),
        canonical_log_connection,
        canonical_datastore,
        expected_source,
        activated_source: activated_source.clone(),
        ingest_state,
        watchdog: Some(watchdog),
    })
}

fn prepare_source_session<F>(
    authority: &FenceAuthority<PostgresFenceStore>,
    provisional: &ProvisionalLease,
    source_connection: &str,
    replication_connection: &str,
    activated_source: &ActivatedSource,
    enrollment: &SlotEnrollment,
    on_shutdown_handle: F,
) -> Result<
    (
        PgReplicationConnection,
        tokio::runtime::Runtime,
        SlotMetadata,
        i32,
        crate::DurableLogWriterClaim,
        u64,
    ),
    SourceRuntimeError,
>
where
    F: FnOnce(PgReplicationShutdownHandle),
{
    let binding = activated_source.registry_binding();
    let expected_source = binding.source_identity();
    let mut controller = connect_source_controller(source_connection)?;
    verify_regular_source(&mut controller, expected_source)?;
    let quoted_publication = verify_publication(&mut controller, binding)?;
    let mut slot = inspect_slot(&mut controller, &enrollment.slot_name)?
        .ok_or(SourceRuntimeError::SlotMissingReseedRequired)?;
    validate_slot(&slot, expected_source)?;
    if slot.confirmed_flush_lsn > enrollment.durable_canonical_lsn()? {
        return Err(SourceRuntimeError::SlotAheadOfCanonicalTruth {
            slot_lsn: slot.confirmed_flush_lsn,
            canonical_lsn: enrollment.durable_canonical_lsn()?,
        });
    }
    if let Some(active_pid) = slot.active_pid {
        terminate_backend(&mut controller, active_pid)?;
        slot = wait_for_inactive_slot(&mut controller, expected_source, &enrollment.slot_name)?;
    }
    if slot.active_pid.is_some() {
        return Err(SourceRuntimeError::SlotDidNotBecomeInactive);
    }

    let claim = authority.claim_durable_log_writer(provisional)?;
    let io_runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .map_err(|_| SourceRuntimeError::ReplicationUnavailable)?;
    let io_guard = io_runtime.enter();
    let mut replication = PgReplicationConnection::connect(replication_connection)
        .map_err(|_| replication_connect_error(&mut controller))?;
    let shutdown_handle = replication.shutdown_handle().map_err(replication_error)?;
    on_shutdown_handle(shutdown_handle);
    let flushed_lsn = verify_replication_source(&mut replication, expected_source)?;
    let backend_pid = replication_backend_pid(&mut replication)?;
    authority.bind_backend(provisional, backend_pid)?;

    let options = activated_source
        .pgoutput_profile()
        .start_replication_options(&quoted_publication)
        .map_err(|_| SourceRuntimeError::UnsupportedPgoutputProfile)?;
    replication
        .start_replication(&enrollment.slot_name, slot.confirmed_flush_lsn, &options)
        .map_err(replication_error)?;
    let claimed = inspect_slot(&mut controller, &enrollment.slot_name)?
        .ok_or(SourceRuntimeError::SlotMissingReseedRequired)?;
    validate_slot(&claimed, expected_source)?;
    if claimed.active_pid != Some(backend_pid) {
        return Err(SourceRuntimeError::SlotBackendBindingMismatch {
            expected: backend_pid,
            actual: claimed.active_pid,
        });
    }
    drop(io_guard);
    slot = claimed;
    Ok((
        replication,
        io_runtime,
        slot,
        backend_pid,
        claim,
        flushed_lsn,
    ))
}

/// Creates the slot, waiting up to `config.open_transaction_wait` for open
/// transactions to end.
///
/// PostgreSQL starts a new logical slot's decoding only after the transactions
/// holding a transaction id, in any database on the server, have ended: those
/// open when creation begins, then those open once they have. It waits for
/// each on that transaction's lock, so `lock_timeout` ends the wait.
fn create_slot(
    controller: &mut Client,
    config: &SlotProvisioningConfig,
) -> Result<(), SourceRuntimeError> {
    // Zero would disable `lock_timeout`, and both settings must fit
    // PostgreSQL's 32-bit millisecond range; `statement_timeout` sits a second
    // past it, because the wait counts against both.
    let wait_millis = config
        .open_transaction_wait
        .as_millis()
        .clamp(1, i32::MAX as u128 - 1_000);
    controller
        .batch_execute(&format!(
            "SET lock_timeout = {wait_millis}; SET statement_timeout = {};",
            wait_millis + 1_000
        ))
        .map_err(source_error)?;
    let created = controller.query_one(
        "SELECT slot_name::text, lsn::text
           FROM pg_catalog.pg_create_logical_replication_slot(
                $1::name, 'pgoutput', false, false
           )",
        &[&config.slot_name],
    );
    controller
        .batch_execute(CONTROLLER_TIMEOUTS)
        .map_err(source_error)?;
    match created {
        Ok(_) => Ok(()),
        Err(error) if error.code() == Some(&SqlState::LOCK_NOT_AVAILABLE) => {
            Err(SourceRuntimeError::SlotCreationWaitedForOpenTransactions {
                slot_name: config.slot_name.clone(),
                waited: config.open_transaction_wait,
                open: open_transactions(controller)?,
            })
        }
        Err(error) => Err(source_error(error)),
    }
}

/// The transactions holding a transaction id, oldest first: those slot
/// creation waits for.
fn open_transactions(client: &mut Client) -> Result<Vec<OpenTransaction>, SourceRuntimeError> {
    let sessions = client
        .query(
            "SELECT pid, usename::text, datname::text, xact_start::text
               FROM pg_catalog.pg_stat_activity
              WHERE backend_xid IS NOT NULL AND pid <> pg_catalog.pg_backend_pid()
              ORDER BY xact_start NULLS LAST, pid",
            &[],
        )
        .map_err(source_error)?;
    let prepared = client
        .query(
            "SELECT gid, database::text, prepared::text
               FROM pg_catalog.pg_prepared_xacts ORDER BY prepared, gid",
            &[],
        )
        .map_err(source_error)?;
    Ok(sessions
        .into_iter()
        .map(|row| OpenTransaction::Session {
            pid: row.get(0),
            role: row.get(1),
            database: row.get(2),
            started: row.get(3),
        })
        .chain(prepared.into_iter().map(|row| OpenTransaction::Prepared {
            gid: row.get(0),
            database: row.get(1),
            prepared: row.get(2),
        }))
        .collect())
}

fn verify_regular_source(
    client: &mut Client,
    expected: &SourceIdentity,
) -> Result<(), SourceRuntimeError> {
    let row = client
        .query_one(
            "SELECT current_database(), control.system_identifier::text,
                    checkpoint.timeline_id, checkpoint.prev_timeline_id,
                    current_setting('server_version_num')::integer,
                    current_setting('wal_level')
               FROM pg_catalog.pg_control_system() AS control
              CROSS JOIN pg_catalog.pg_control_checkpoint() AS checkpoint",
            &[],
        )
        .map_err(source_error)?;
    let database: String = row.get(0);
    let system_identifier: String = row.get(1);
    let timeline: i32 = row.get(2);
    let previous_timeline: i32 = row.get(3);
    let server_version_num: i32 = row.get(4);
    let wal_level: String = row.get(5);
    if database != expected.database_name
        || system_identifier != expected.system_identifier
        || u32::try_from(timeline).ok() != Some(expected.timeline_id)
        || u32::try_from(previous_timeline).ok() != Some(expected.previous_timeline_id)
    {
        return Err(SourceRuntimeError::SourceIdentityMismatch);
    }
    if !(150_000..190_000).contains(&server_version_num) || wal_level != "logical" {
        return Err(SourceRuntimeError::UnsupportedSourceSettings);
    }
    Ok(())
}

/// Checks the replication connection's source identity and returns the WAL
/// position PostgreSQL had flushed when it answered.
fn verify_replication_source(
    connection: &mut PgReplicationConnection,
    expected: &SourceIdentity,
) -> Result<u64, SourceRuntimeError> {
    let identity = connection.identify_system().map_err(replication_error)?;
    let system_identifier = identity
        .get_value(0, 0)
        .ok_or(SourceRuntimeError::ReplicationIdentityMissing)?;
    let timeline = identity
        .get_value(0, 1)
        .ok_or(SourceRuntimeError::ReplicationIdentityMissing)?;
    let flushed_lsn = identity
        .get_value(0, 2)
        .and_then(|lsn| parse_lsn(&lsn).ok())
        .ok_or(SourceRuntimeError::ReplicationIdentityMissing)?;
    let database = identity
        .get_value(0, 3)
        .ok_or(SourceRuntimeError::ReplicationIdentityMissing)?;
    if system_identifier != expected.system_identifier
        || timeline != expected.timeline_id.to_string()
        || database != expected.database_name
    {
        return Err(SourceRuntimeError::SourceIdentityMismatch);
    }
    Ok(flushed_lsn)
}

/// Checks the publication against its enrolled definition, as a start does,
/// and returns its quoted name. A publication altered while a session ran is
/// refused here, at the next reconnect.
fn verify_publication(
    client: &mut Client,
    binding: &SchemaRegistryBinding,
) -> Result<String, SourceRuntimeError> {
    let tables = binding
        .tables()
        .iter()
        .map(|(_, table)| table.clone())
        .collect::<BTreeSet<_>>();
    match inspect_owned_publication(client, &binding.publication().name, &tables)
        .map_err(source_error)?
    {
        OwnedPublication::Missing => Err(SourceRuntimeError::PublicationMissing),
        OwnedPublication::Drifted => Err(SourceRuntimeError::PublicationDrift),
        OwnedPublication::Matches { quoted_name } => Ok(quoted_name),
    }
}

fn inspect_slot(
    client: &mut Client,
    slot_name: &str,
) -> Result<Option<SlotMetadata>, SourceRuntimeError> {
    client
        .query_opt(
            "SELECT slot_name::text, plugin::text, slot_type, database::text,
                    temporary, active_pid, confirmed_flush_lsn::text,
                    wal_status, two_phase,
                    NULLIF(to_jsonb(slot)->>'invalidation_reason', ''),
                    COALESCE((to_jsonb(slot)->>'failover')::boolean, false),
                    COALESCE((to_jsonb(slot)->>'synced')::boolean, false)
               FROM pg_catalog.pg_replication_slots AS slot
              WHERE slot_name = $1",
            &[&slot_name],
        )
        .map_err(source_error)?
        .map(slot_from_row)
        .transpose()
}

fn slot_from_row(row: Row) -> Result<SlotMetadata, SourceRuntimeError> {
    let confirmed_flush_lsn: Option<String> = row.get(6);
    Ok(SlotMetadata {
        slot_name: row.get(0),
        plugin: row.get(1),
        slot_type: row.get(2),
        database: row.get(3),
        temporary: row.get(4),
        active_pid: row.get(5),
        confirmed_flush_lsn: confirmed_flush_lsn
            .ok_or(SourceRuntimeError::SlotHasNoConfirmedFlushLsn)
            .and_then(|lsn| parse_lsn(&lsn).map_err(|_| SourceRuntimeError::InvalidSlotLsn))?,
        wal_status: row.get(7),
        two_phase: row.get(8),
        invalidation_reason: row.get(9),
        failover: row.get(10),
        synchronized: row.get(11),
    })
}

fn validate_slot(
    slot: &SlotMetadata,
    expected_source: &SourceIdentity,
) -> Result<(), SourceRuntimeError> {
    if slot.slot_type != "logical"
        || slot.plugin.as_deref() != Some("pgoutput")
        || slot.database.as_deref() != Some(expected_source.database_name.as_str())
        || slot.temporary
        || slot.two_phase
        || slot.failover
        || slot.synchronized
    {
        return Err(SourceRuntimeError::UnexpectedSlotMetadata);
    }
    if slot.wal_status.as_deref() == Some("lost") || slot.invalidation_reason.is_some() {
        return Err(SourceRuntimeError::SlotLostReseedRequired);
    }
    Ok(())
}

fn terminate_backend(client: &mut Client, backend_pid: i32) -> Result<(), SourceRuntimeError> {
    let terminated: bool = client
        .query_one(
            "SELECT pg_catalog.pg_terminate_backend($1, $2)",
            &[&backend_pid, &1_000_i64],
        )
        .map_err(source_error)?
        .get(0);
    if !terminated {
        return Err(SourceRuntimeError::BackendRevocationFailed(backend_pid));
    }
    Ok(())
}

fn wait_for_inactive_slot(
    client: &mut Client,
    expected_source: &SourceIdentity,
    slot_name: &str,
) -> Result<SlotMetadata, SourceRuntimeError> {
    for _ in 0..SLOT_INACTIVE_POLL_ATTEMPTS {
        let slot = inspect_slot(client, slot_name)?
            .ok_or(SourceRuntimeError::SlotMissingReseedRequired)?;
        validate_slot(&slot, expected_source)?;
        if slot.active_pid.is_none() {
            return Ok(slot);
        }
        thread::sleep(SLOT_INACTIVE_POLL_INTERVAL);
    }
    Err(SourceRuntimeError::SlotDidNotBecomeInactive)
}

/// Real time for a loop that keeps a lease renewed. A pause ends as soon as
/// the loop's owner sends on `stop` or drops its sender.
struct WatchdogClock<'a> {
    stop: &'a mpsc::Receiver<()>,
}

impl RenewalClock for WatchdogClock<'_> {
    fn now(&self) -> Instant {
        Instant::now()
    }

    fn pause(&mut self, pause: Duration) -> bool {
        matches!(
            self.stop.recv_timeout(pause),
            Err(mpsc::RecvTimeoutError::Timeout)
        )
    }
}

fn start_session_watchdog(
    authority: Arc<FenceAuthority<PostgresFenceStore>>,
    lease: ActiveLease,
    source_connection: String,
    expected_source: SourceIdentity,
    slot_name: String,
    backend_pid: i32,
) -> Result<SessionWatchdog, SourceRuntimeError> {
    let (stop, receiver) = mpsc::channel();
    let failed = Arc::new(AtomicBool::new(false));
    let thread_failed = Arc::clone(&failed);
    let thread_lease = lease.clone();
    let join = thread::Builder::new()
        .name(format!("calque-source-watchdog-{backend_pid}"))
        .spawn(move || {
            let mut clock = WatchdogClock { stop: &receiver };
            if authority
                .keep_active_renewed(&thread_lease, &mut clock)
                .is_err()
            {
                // Local authority disappears before the best-effort
                // server-side kill. Even a stuck/failed controller cannot make
                // append valid because the PostgreSQL log adapter also checks
                // the active, unexpired lease row.
                thread_lease.revoke();
                thread_failed.store(true, Ordering::Release);
                let _ = revoke_exact_source_backend(
                    &source_connection,
                    &expected_source,
                    &slot_name,
                    backend_pid,
                );
            }
        })
        .map_err(|_| SourceRuntimeError::WatchdogUnavailable)?;
    Ok(SessionWatchdog {
        stop,
        join: Some(join),
        failed,
    })
}

fn start_provisional_watchdog(
    authority: Arc<FenceAuthority<PostgresFenceStore>>,
    lease: ProvisionalLease,
) -> Result<ProvisionalWatchdog, SourceRuntimeError> {
    let (stop, receiver) = mpsc::channel();
    let join = thread::Builder::new()
        .name(format!(
            "calque-provisional-watchdog-{}",
            lease.identity().token().get()
        ))
        .spawn(move || {
            authority.keep_provisional_renewed(&lease, &mut WatchdogClock { stop: &receiver });
        })
        .map_err(|_| SourceRuntimeError::WatchdogUnavailable)?;
    Ok(ProvisionalWatchdog {
        stop,
        join: Some(join),
    })
}

/// Runs `wait` while renewing `lease`, so that a wait longer than its TTL does
/// not let it expire before the caller records what the lease guards.
fn while_renewing<T>(
    authority: &FenceAuthority<PostgresFenceStore>,
    lease: &ProvisionalLease,
    wait: impl FnOnce() -> Result<T, SourceRuntimeError>,
) -> Result<T, SourceRuntimeError> {
    let (stop, receiver) = mpsc::channel();
    thread::scope(|scope| {
        thread::Builder::new()
            .name(format!(
                "calque-provisional-renewal-{}",
                lease.identity().token().get()
            ))
            .spawn_scoped(scope, move || {
                authority.keep_provisional_renewed(lease, &mut WatchdogClock { stop: &receiver });
            })
            .map_err(|_| SourceRuntimeError::WatchdogUnavailable)?;
        let result = wait();
        drop(stop);
        result
    })
}

fn revoke_exact_source_backend(
    source_connection: &str,
    expected_source: &SourceIdentity,
    slot_name: &str,
    expected_backend_pid: i32,
) -> Result<(), SourceRuntimeError> {
    let mut controller = connect_source_controller(source_connection)?;
    verify_regular_source(&mut controller, expected_source)?;
    let slot = inspect_slot(&mut controller, slot_name)?
        .ok_or(SourceRuntimeError::SlotMissingReseedRequired)?;
    validate_slot(&slot, expected_source)?;
    match slot.active_pid {
        Some(actual) if actual == expected_backend_pid => {
            terminate_backend(&mut controller, expected_backend_pid)?;
            wait_for_inactive_slot(&mut controller, expected_source, slot_name)?;
        }
        None => {}
        Some(_) => return Err(SourceRuntimeError::BackendOwnershipChanged),
    }
    Ok(())
}

fn connect_source_controller(connection_string: &str) -> Result<Client, SourceRuntimeError> {
    let mut endpoint = postgres_connect::Endpoint::parse(connection_string)
        .map_err(|_| SourceRuntimeError::SourceControlUnavailable)?;
    endpoint
        .config_mut()
        .connect_timeout(Duration::from_secs(2));
    let mut client = endpoint.connect().map_err(source_error)?;
    client
        .batch_execute(CONTROLLER_TIMEOUTS)
        .map_err(source_error)?;
    Ok(client)
}

fn validate_activated_lease(
    lease: &ProvisionalLease,
    activated: &ActivatedSource,
) -> Result<(), SourceRuntimeError> {
    if lease.identity().source_epoch() != activated.source_epoch() {
        return Err(SourceRuntimeError::ActivatedSourceLeaseMismatch);
    }
    Ok(())
}

fn validate_enrollment_binding(
    enrollment: &SlotEnrollment,
    lease: &ProvisionalLease,
    activated: &ActivatedSource,
) -> Result<(), SourceRuntimeError> {
    let binding = activated.registry_binding();
    let identity = binding.source_identity();
    if enrollment.source_id != *lease.identity().source_id()
        || enrollment.source_epoch != *activated.source_epoch()
        || enrollment.publication_name != binding.publication().name
        || enrollment.system_identifier != identity.system_identifier
        || enrollment.database_name != identity.database_name
        || enrollment.timeline_id != identity.timeline_id
        || enrollment.previous_timeline_id != identity.previous_timeline_id
        || enrollment.pgoutput_profile != PGOUTPUT_PROFILE_V1
        || activated.pgoutput_profile() != source_control::PgoutputLaunchProfile::V1
    {
        return Err(SourceRuntimeError::SlotEnrollmentBindingMismatch);
    }
    enrollment.initial_lsn()?;
    enrollment.durable_canonical_lsn()?;
    Ok(())
}

fn format_lsn(lsn: u64) -> String {
    format!("{:X}/{:08X}", lsn >> 32, lsn & 0xffff_ffff)
}

fn replication_backend_pid(
    connection: &mut PgReplicationConnection,
) -> Result<i32, SourceRuntimeError> {
    let result = connection
        .exec("SELECT pg_backend_pid()")
        .map_err(replication_error)?;
    result
        .get_value(0, 0)
        .ok_or(SourceRuntimeError::ReplicationBackendPidMissing)?
        .parse()
        .map_err(|_| SourceRuntimeError::ReplicationBackendPidMissing)
}

fn validate_slot_name(slot_name: &str) -> Result<(), SourceRuntimeError> {
    if !source_control::is_valid_slot_name(slot_name) {
        return Err(SourceRuntimeError::InvalidSlotName);
    }
    Ok(())
}

fn source_error(error: postgres::Error) -> SourceRuntimeError {
    match PostgresRefusal::of(&error) {
        Some(refusal) => SourceRuntimeError::SourceRefused(refusal),
        None => SourceRuntimeError::SourceControlUnavailable,
    }
}

/// Classifies a failed replication connect by asking `controller` whether its
/// role may still start a walsender. Callers derive both connections from one
/// source URL, so both log in as that role.
///
/// libpq reports a failed connection as message text alone, with no SQLSTATE,
/// and the text follows the server's `lc_messages`. A role that lost the
/// REPLICATION attribute is refused a walsender (42501) while its regular
/// connections still succeed, and no retry gets past that until the attribute
/// is granted again. Any other failure is treated as an outage.
fn replication_connect_error(controller: &mut Client) -> SourceRuntimeError {
    // The same test PostgreSQL applies to the login role, superusers included.
    let permitted = controller.query_one(
        "SELECT rolsuper OR rolreplication FROM pg_catalog.pg_roles
          WHERE rolname = session_user",
        &[],
    );
    match permitted {
        Ok(row) if !row.get::<_, bool>(0) => {
            SourceRuntimeError::SourceRefused(PostgresRefusal::ReplicationPrivilegeMissing)
        }
        Ok(_) => SourceRuntimeError::ReplicationUnavailable,
        Err(error) => source_error(error),
    }
}

fn replication_error(_error: pg_walstream::ReplicationError) -> SourceRuntimeError {
    SourceRuntimeError::ReplicationUnavailable
}

/// A transaction that was still open when slot creation stopped waiting.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum OpenTransaction {
    /// A session's transaction. PostgreSQL shows when it started only to roles
    /// allowed to see that session's activity.
    Session {
        pid: i32,
        role: Option<String>,
        database: Option<String>,
        started: Option<String>,
    },
    /// A transaction prepared for two-phase commit, which stays open after its
    /// session ends until someone commits or rolls it back by `gid`.
    Prepared {
        gid: String,
        database: String,
        prepared: String,
    },
}

impl fmt::Display for OpenTransaction {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Session {
                pid,
                role,
                database,
                started,
            } => {
                write!(
                    formatter,
                    "pid {pid} (role {}, database {}, ",
                    role.as_deref().unwrap_or("unknown"),
                    database.as_deref().unwrap_or("none")
                )?;
                match started {
                    Some(started) => write!(formatter, "open since {started})"),
                    None => write!(formatter, "start time not visible to this role)"),
                }
            }
            Self::Prepared {
                gid,
                database,
                prepared,
            } => write!(
                formatter,
                "prepared transaction '{gid}' (database {database}, prepared at {prepared})"
            ),
        }
    }
}

fn describe_open_transactions(open: &[OpenTransaction]) -> String {
    const SHOWN: usize = 5;
    if open.is_empty() {
        return "none is open any more, so a retry should succeed".to_owned();
    }
    let mut described = open
        .iter()
        .take(SHOWN)
        .map(ToString::to_string)
        .collect::<Vec<_>>()
        .join(", ");
    if open.len() > SHOWN {
        described.push_str(&format!(" and {} more", open.len() - SHOWN));
    }
    format!("still open: {described}; end them, or retry once they finish")
}

#[derive(Debug, Error)]
pub enum SourceRuntimeError {
    #[error(transparent)]
    Fence(#[from] FenceError),
    #[error("persistent slot name is invalid")]
    InvalidSlotName,
    #[error("source control connection failed or returned an uncertain result")]
    SourceControlUnavailable,
    #[error("source PostgreSQL refused the connection or a statement: {0}")]
    SourceRefused(PostgresRefusal),
    #[error("replication connection failed or returned an uncertain result")]
    ReplicationUnavailable,
    #[error("regular and replication connections do not match the enrolled source identity")]
    SourceIdentityMismatch,
    #[error("source version or WAL configuration is outside the launch profile")]
    UnsupportedSourceSettings,
    #[error("replication connection did not return a complete source identity")]
    ReplicationIdentityMissing,
    #[error("configured publication does not exist")]
    PublicationMissing,
    #[error("publication no longer has the definition the activated source registry enrolled")]
    PublicationDrift,
    #[error("activated source epoch does not match the provisional lease")]
    ActivatedSourceLeaseMismatch,
    #[error("durable slot enrollment does not match the activated source and lease")]
    SlotEnrollmentBindingMismatch,
    #[error("persistent slot lifecycle is not initialized for this source epoch")]
    SlotLifecycleNotInitialized,
    #[error("persistent slot already exists during explicit create-new enrollment")]
    SlotAlreadyExists,
    #[error(
        "creating replication slot {slot_name} waited {waited:?} for open transactions to end and gave up; PostgreSQL starts a new slot only after every transaction that has written or locked rows, in any database on the server, has ended; {}",
        describe_open_transactions(.open)
    )]
    SlotCreationWaitedForOpenTransactions {
        slot_name: String,
        waited: Duration,
        open: Vec<OpenTransaction>,
    },
    #[error("persistent slot is missing; explicit epoch/reseed is required")]
    SlotMissingReseedRequired,
    #[error("persistent slot metadata does not match the launch profile")]
    UnexpectedSlotMetadata,
    #[error("persistent slot has no confirmed flush LSN")]
    SlotHasNoConfirmedFlushLsn,
    #[error("persistent slot confirmed flush LSN is invalid")]
    InvalidSlotLsn,
    #[error("persistent slot has lost required WAL; explicit epoch/reseed is required")]
    SlotLostReseedRequired,
    #[error("failed to revoke stale source backend {0}")]
    BackendRevocationFailed(i32),
    #[error("source slot is now owned by a different backend and will not be terminated")]
    BackendOwnershipChanged,
    #[error("persistent slot did not become inactive after stale backend revocation")]
    SlotDidNotBecomeInactive,
    #[error("replication connection did not expose its backend PID")]
    ReplicationBackendPidMissing,
    #[error("slot active backend mismatch: expected {expected}, found {actual:?}")]
    SlotBackendBindingMismatch { expected: i32, actual: Option<i32> },
    #[error("unsupported pgoutput profile reached persistent source startup")]
    UnsupportedPgoutputProfile,
    #[error("source ownership watchdog could not be started")]
    WatchdogUnavailable,
    #[error(
        "persistent slot confirmed flush LSN {slot_lsn} is ahead of durable canonical truth {canonical_lsn}"
    )]
    SlotAheadOfCanonicalTruth { slot_lsn: u64, canonical_lsn: u64 },
}

#[cfg(test)]
mod tests {
    use super::*;

    fn healthy_slot() -> SlotMetadata {
        SlotMetadata {
            slot_name: "calque_source_1".to_owned(),
            slot_type: "logical".to_owned(),
            plugin: Some("pgoutput".to_owned()),
            database: Some("calque".to_owned()),
            temporary: false,
            active_pid: None,
            confirmed_flush_lsn: 0,
            wal_status: Some("reserved".to_owned()),
            two_phase: false,
            invalidation_reason: None,
            failover: false,
            synchronized: false,
        }
    }

    fn expected_source() -> SourceIdentity {
        SourceIdentity {
            system_identifier: "7000000000000000000".to_owned(),
            database_name: "calque".to_owned(),
            timeline_id: 1,
            previous_timeline_id: 0,
        }
    }

    /// A slot whose WAL was removed or that PostgreSQL invalidated is a reseed
    /// boundary. No other test brings a server into that state, so this one
    /// drives the slot check directly.
    #[test]
    fn an_invalidated_slot_is_a_reseed_boundary_not_a_resumable_source() {
        // Control: a healthy slot is accepted, so the refusals below are caused
        // by the degraded field and not by the rest of the metadata.
        validate_slot(&healthy_slot(), &expected_source()).unwrap();

        // WAL was removed out from under the slot.
        let mut lost = healthy_slot();
        lost.wal_status = Some("lost".to_owned());
        assert!(matches!(
            validate_slot(&lost, &expected_source()),
            Err(SourceRuntimeError::SlotLostReseedRequired)
        ));

        // PostgreSQL recorded an invalidation reason.
        let mut invalidated = healthy_slot();
        invalidated.invalidation_reason = Some("wal_removed".to_owned());
        assert!(matches!(
            validate_slot(&invalidated, &expected_source()),
            Err(SourceRuntimeError::SlotLostReseedRequired)
        ));

        // A lost slot must not be reported as merely unexpected metadata: the
        // caller reseeds on SlotLostReseedRequired and retries on the other.
        let mut both = healthy_slot();
        both.wal_status = Some("lost".to_owned());
        both.temporary = true;
        assert!(matches!(
            validate_slot(&both, &expected_source()),
            Err(SourceRuntimeError::UnexpectedSlotMetadata)
        ));
    }

    #[test]
    fn config_rejects_unsafe_names() {
        assert!(SlotProvisioningConfig::new("calque_source_1").is_ok());
        for invalid in ["", "UPPER", "hyphen-name", "contains space"] {
            assert!(matches!(
                SlotProvisioningConfig::new(invalid),
                Err(SourceRuntimeError::InvalidSlotName)
            ));
        }
    }

    #[test]
    fn slot_creation_refusal_names_the_transactions_still_open() {
        let session = |pid: i32, started: Option<&str>| OpenTransaction::Session {
            pid,
            role: Some("app".to_owned()),
            database: Some("shop".to_owned()),
            started: started.map(str::to_owned),
        };
        let refusal = |open: Vec<OpenTransaction>| {
            SourceRuntimeError::SlotCreationWaitedForOpenTransactions {
                slot_name: "calque_source_1".to_owned(),
                waited: Duration::from_secs(15),
                open,
            }
            .to_string()
        };
        let preamble = "creating replication slot calque_source_1 waited 15s for open transactions to end and gave up; PostgreSQL starts a new slot only after every transaction that has written or locked rows, in any database on the server, has ended; ";
        assert_eq!(
            refusal(vec![
                session(41, Some("2026-09-27 10:00:00+00")),
                OpenTransaction::Session {
                    pid: 42,
                    role: None,
                    database: None,
                    started: None,
                },
                OpenTransaction::Prepared {
                    gid: "batch-7".to_owned(),
                    database: "shop".to_owned(),
                    prepared: "2026-09-27 09:00:00+00".to_owned(),
                },
                session(43, None),
                session(44, None),
                session(45, None),
                session(46, None),
            ]),
            format!(
                "{preamble}still open: pid 41 (role app, database shop, open since 2026-09-27 10:00:00+00), pid 42 (role unknown, database none, start time not visible to this role), prepared transaction 'batch-7' (database shop, prepared at 2026-09-27 09:00:00+00), pid 43 (role app, database shop, start time not visible to this role), pid 44 (role app, database shop, start time not visible to this role) and 2 more; end them, or retry once they finish"
            )
        );
        assert_eq!(
            refusal(Vec::new()),
            format!("{preamble}none is open any more, so a retry should succeed")
        );
    }

    #[test]
    fn a_watchdog_pause_ends_early_only_when_the_session_stops_the_watchdog() {
        let (stop, receiver) = mpsc::channel();
        let mut clock = WatchdogClock { stop: &receiver };
        assert!(clock.pause(Duration::from_millis(1)));

        stop.send(()).unwrap();
        let paused_at = Instant::now();
        assert!(!clock.pause(Duration::from_secs(60)));
        drop(stop);
        assert!(!clock.pause(Duration::from_secs(60)));
        assert!(paused_at.elapsed() < Duration::from_secs(1));
    }
}
