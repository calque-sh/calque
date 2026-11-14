//! `calque dev`, `calque status` and `calque destroy`.
//!
//! `dev` enrolls a disposable PostgreSQL source, snapshots it, and serves it on
//! loopback behind one shared bearer, so a browser can mount it without a grant
//! issuer. It runs the same engine as `serve` with a different credential
//! policy and a loopback binding. `status` and `destroy` read any state
//! directory the engine writes, `dev`'s or `serve`'s.

use std::{
    collections::{BTreeMap, BTreeSet},
    fs,
    path::{Path, PathBuf},
    time::{Duration, Instant},
};

use artifact_catalog::SqliteArtifactCatalog;
use authz::{
    AuthorizationPolicy, PartitionDefinition, PolicyDefinition, Principal, PrincipalValue,
    TablePermitDefinition,
};
use blob_store::LocalFilesystemBlobStore;
use calque_engine::{
    check_replication_capacity, destroy_local_source, inspect_local_identity, local_router,
    metrics_router, prepare_local_source, start_local_source_runtime, ArtifactBuilder,
    CanonicalHeadSignal, GenerationArtifactPublisher, HttpBinding, LocalArtifactResolver,
    LocalBackend, LocalBackendConfig, LocalCredentialPolicy, LocalMemoryPolicy,
    LocalMutationBackend, LocalMutationSupport, LocalNames, LocalSourceHealth,
    LocalSourceRuntimeConfig, LocalState, MutationCommandPolicy, OperationsMetrics,
    PreparedLocalSource, RecutConfig, ReencodeConfig, StateStore, TenantSettings, Tenants,
    TenantsConfig, LOCAL_ACTIVE_ARTIFACT_CAPACITY_BYTES, LOCAL_DEVELOPMENT_SUBJECT,
    MAX_LOCAL_ARTIFACT_BYTES,
};
use source_control::{PostgresTypeCodec, QualifiedTable, SourceIdentity};
use sync_types::SourceEpoch;
use thiserror::Error;
use tokio_util::sync::CancellationToken;
use uuid::Uuid;

use crate::cli::{DestroyCommand, DevCommand};
use crate::lifecycle::{
    bounded_stop, run_blocking, shutdown_after, shutdown_runtime, shutdown_signal,
    with_state_remedy, CommandError,
};
use crate::listen::{spawn_metrics_server, stop_metrics_server, ListenError, ReservedListener};
use crate::log_sink::print_line;
use crate::open_files::{fit_open_file_limit, ConnectionBounds};
use crate::recovery::{self, RecoveryCliError};
use crate::remedy::{after_stopped_runtime, reconnect_or_forget, restore_publication, Destroy};

const MAX_DEV_TABLES: usize = 256;
const MAX_DEV_ALLOWED_ORIGINS: usize = 16;
const DEV_READY_CATCH_UP_TIMEOUT: Duration = Duration::from_secs(30);
const DEV_READY_POLL_INTERVAL: Duration = Duration::from_millis(25);

impl DevCommand {
    pub fn validate(&self) -> Result<(), DevError> {
        if !self.acknowledge_disposable_source {
            return Err(DevError::DisposableSourceNotAcknowledged);
        }
        if self.port == 0 {
            return Err(DevError::InvalidPort);
        }
        match self.metrics_port {
            Some(0) => return Err(DevError::InvalidMetricsPort),
            Some(port) if port == self.port => {
                return Err(DevError::MetricsPortIsHttpPort { port });
            }
            _ => {}
        }
        if self.tables.len() > MAX_DEV_TABLES {
            return Err(DevError::TooManyTables);
        }
        if self.allowed_origins.len() > MAX_DEV_ALLOWED_ORIGINS {
            return Err(DevError::TooManyOrigins);
        }
        let mut seen_tables = BTreeSet::new();
        if let Some(table) = self.tables.iter().find(|table| !seen_tables.insert(*table)) {
            return Err(DevError::DuplicateTable {
                table: table.display_name(),
            });
        }
        if let Some((first, second)) = QualifiedTable::first_shared_collection_name(&self.tables) {
            return Err(DevError::SharedCollectionName {
                first: first.display_name(),
                second: second.display_name(),
                collection: second.table.clone(),
            });
        }
        let mut seen_origins = BTreeSet::new();
        if let Some(origin) = self
            .allowed_origins
            .iter()
            .find(|origin| !seen_origins.insert(*origin))
        {
            return Err(DevError::DuplicateOrigin {
                origin: origin.to_string(),
            });
        }
        Ok(())
    }
}

/// The scope partitioned by `column`: each table whose primary key leads
/// with it holds tenants' rows, and every other table is shared.
fn partitioned_by(
    prepared: PreparedLocalSource,
    column: &str,
) -> Result<PreparedLocalSource, DevError> {
    let mut partitioned = BTreeMap::new();
    for table in prepared.registry.tables() {
        let columns = &table.inspection.columns;
        let Some(index) = columns
            .iter()
            .position(|candidate| candidate.name == column)
        else {
            continue;
        };
        let leading = columns
            .iter()
            .filter_map(|candidate| {
                candidate
                    .primary_key_ordinal
                    .map(|ordinal| (ordinal, candidate.name.as_str()))
            })
            .min()
            .map(|(_, name)| name);
        if leading != Some(column) {
            return Err(DevError::TenantColumnNotLeading {
                table: table.inspection.table.display_name(),
                column: column.to_owned(),
            });
        }
        partitioned.insert(table.table_id.clone(), table.column_ids[index]);
    }
    if partitioned.is_empty() {
        return Err(DevError::NoTenantTable {
            column: column.to_owned(),
        });
    }
    let definition = PolicyDefinition {
        generation: prepared.policy.generation(),
        tables: prepared
            .registry
            .tables()
            .iter()
            .map(|table| TablePermitDefinition {
                table_id: table.table_id.clone(),
                permitted_columns: table.column_ids.clone(),
            })
            .collect(),
    };
    let policy = AuthorizationPolicy::compile_partitioned_for_registry(
        definition,
        PartitionDefinition {
            attribute: column.to_owned(),
            tables: partitioned,
        },
        &prepared.registry,
    )?;
    let scope = policy.scope();
    Ok(PreparedLocalSource {
        policy,
        scope,
        ..prepared
    })
}

/// The one principal the shared bearer stands for. Under a partitioned scope
/// it carries `tenant` in the partition's attribute, typed as the column is.
fn shared_principal(
    prepared: &PreparedLocalSource,
    tenant: Option<&str>,
) -> Result<Principal, DevError> {
    let mut attributes = BTreeMap::new();
    if let (Some((attribute, codec)), Some(tenant)) = (prepared.policy.partition(), tenant) {
        let value = match codec {
            PostgresTypeCodec::Int2 => tenant.parse().ok().map(PrincipalValue::Int2),
            PostgresTypeCodec::Int4 => tenant.parse().ok().map(PrincipalValue::Int4),
            PostgresTypeCodec::Int8 => tenant.parse().ok().map(PrincipalValue::Int8),
            PostgresTypeCodec::Uuid => Uuid::parse_str(tenant)
                .ok()
                .map(|uuid| PrincipalValue::Uuid(*uuid.as_bytes())),
            _ => None,
        }
        .ok_or_else(|| DevError::InvalidTenant {
            tenant: tenant.to_owned(),
            column: attribute.to_owned(),
        })?;
        attributes.insert(attribute.to_owned(), value);
    }
    Ok(Principal::new(LOCAL_DEVELOPMENT_SUBJECT, attributes)?)
}

pub async fn run_dev(command: DevCommand) -> Result<(), DevError> {
    let state_dir = command.state_dir.clone();
    let database_url_env = command.database.database_url_env.clone();
    prepare_and_serve(command)
        .await
        .map_err(|error| with_state_remedy(error, &state_dir, &database_url_env))
}

async fn prepare_and_serve(command: DevCommand) -> Result<(), DevError> {
    command.validate()?;
    if let Some(name) = crate::serve::object_store_variable_set(|name| {
        std::env::var(name).ok().filter(|value| !value.is_empty())
    }) {
        return Err(DevError::ObjectStoreNotSupported(name));
    }
    let connections = fit_open_file_limit(ConnectionBounds {
        max_open_streams: command.max_open_streams,
        max_inflight_requests: command.max_inflight_requests,
    });
    let database_url = command.database.resolve_allowing_plaintext()?;
    if let Some(host) = database_url.plaintext_remote_host() {
        tracing::warn!("{}", recovery::plaintext_warning(host));
    }
    // Browsers always name the listener 127.0.0.1, also when it binds
    // `0.0.0.0` in a container whose port the host publishes on loopback.
    let binding = HttpBinding::configured(
        command.bind_address,
        command.port,
        Some(&format!("127.0.0.1:{}", command.port)),
    )?;
    let bind_ip = binding.socket_address().ip();
    let base_url = format!("http://{}", binding.expected_host());
    let artifact_resolver =
        LocalArtifactResolver::new(command.public_base_url.as_deref().unwrap_or(&base_url))?;
    let change_endpoint = artifact_resolver.change_endpoint()?;
    // Bound before any state exists, so a port another process holds is
    // refused here rather than after enrollment and the initial snapshot.
    let reserved_listener = ReservedListener::bind(binding.socket_address(), "--port")?;
    let metrics_binding = command
        .metrics_port
        .map(HttpBinding::loopback)
        .transpose()?;
    let reserved_metrics_listener = metrics_binding
        .as_ref()
        .map(|metrics| ReservedListener::bind(metrics.socket_address(), "--metrics-port"))
        .transpose()?;
    let state_store = StateStore::open(&command.state_dir)?;
    tracing::info!(state_dir = %state_store.root().display(), "using state directory");
    let requested_tables = {
        let mut tables = command.tables.clone();
        tables.sort();
        tables
    };
    let state = if state_store.state_path().exists() {
        let state = state_store.read_state()?;
        if state.tables != requested_tables {
            return Err(DevError::PersistedTableSetMismatch {
                state_dir: command.state_dir.clone(),
                database_url_env: command.database.database_url_env.clone(),
                persisted: display_tables(&state.tables),
                requested: display_tables(&requested_tables),
            });
        }
        let identity_database_url = database_url.clone();
        let connected = run_blocking(move || {
            inspect_local_identity(&identity_database_url).map_err(DevError::from)
        })
        .await?;
        if connected != state.source_identity {
            return Err(DevError::DevSourceIdentityMismatch {
                state_dir: command.state_dir.clone(),
                database_url_env: command.database.database_url_env.clone(),
                persisted: describe_source_identity(&state.source_identity),
                connected: describe_source_identity(&connected),
            });
        }
        state
    } else {
        let identity_database_url = database_url.clone();
        let identity = run_blocking(move || {
            inspect_local_identity(&identity_database_url).map_err(DevError::from)
        })
        .await?;
        let names = LocalNames::derive(state_store.root(), &identity)?;
        let state = LocalState::new(
            identity,
            SourceEpoch::new(format!("local-{}", Uuid::new_v4().simple()))?,
            names,
            requested_tables,
        )?;
        state_store.write_state(&state)?;
        state
    };
    let secrets = state_store.load_or_create_secrets()?;
    let had_registry = state_store.registry_path().exists();
    let persisted_registry = if had_registry {
        Some(state_store.read_registry()?)
    } else {
        None
    };
    let prepared_database_url = database_url.clone();
    let prepared_state = state.clone();
    let prepared_secrets = secrets.clone();
    let started = Instant::now();
    let prepared = run_blocking(move || {
        prepare_local_source(
            &prepared_database_url,
            &prepared_state,
            &prepared_secrets,
            persisted_registry,
        )
        .map_err(DevError::from)
    })
    .await?;
    startup_phase_finished("enrollment", started);
    if !had_registry {
        state_store.write_registry(&prepared.registry)?;
    }
    let prepared = match &command.tenant_column {
        Some(column) => partitioned_by(prepared, column)?,
        None => prepared,
    };
    let principal = shared_principal(&prepared, command.tenant.as_deref())?;
    let partitioned = prepared.policy.partition().is_some();
    let tenant_settings = TenantSettings {
        refresh: command.recut.policy(),
        ..TenantSettings::default()
    };
    if partitioned {
        check_replication_capacity(
            &database_url,
            tenant_settings.max_builds,
            &state.names.slot_name,
        )
        .await?;
    }
    state_store.write_client_env(&binding, &secrets.bearer_token)?;

    let mut catalog = SqliteArtifactCatalog::open(state_store.root().join("catalog.sqlite3"))?;
    let store: std::sync::Arc<dyn blob_store::ManagedBlobStore> =
        std::sync::Arc::new(LocalFilesystemBlobStore::open(
            state_store.root().join("objects"),
            MAX_LOCAL_ARTIFACT_BYTES,
        )?);
    let metrics = OperationsMetrics::new();
    let head_signal = CanonicalHeadSignal::new();
    let runtime_database_url = database_url.clone();
    let runtime_state = state.clone();
    let runtime_prepared = prepared.clone();
    let runtime_head_signal = head_signal.clone();
    let runtime_metrics = metrics.clone();
    let runtime_owner = format!("local-runtime-{}", Uuid::new_v4().simple());
    let runtime_state_dir = state_store.root().to_path_buf();
    let runtime_database_url_env = command.database.database_url_env.clone();
    let started = Instant::now();
    let runtime = run_blocking(move || {
        start_local_source_runtime(
            runtime_prepared,
            LocalSourceRuntimeConfig::new(
                &runtime_database_url,
                &runtime_state,
                &runtime_state_dir,
                runtime_owner,
            )
            .with_metrics(runtime_metrics)
            .with_database_url_env(runtime_database_url_env),
            runtime_head_signal,
        )
        .map_err(DevError::from)
    })
    .await?;
    startup_phase_finished("cdc_runtime_start", started);
    let cancellation = CancellationToken::new();
    let builder = ArtifactBuilder {
        database_url: database_url.clone(),
        state_root: state_store.root().to_path_buf(),
        catalog_path: state_store.root().join("catalog.sqlite3"),
        store: std::sync::Arc::clone(&store),
        barrier_provider: runtime.barrier_provider(),
        head_signal: head_signal.clone(),
        cancellation: cancellation.clone(),
        metrics: metrics.clone(),
    };
    let started = Instant::now();
    // A partitioned scope has no source-wide artifact: each tenant's is built
    // on its first request.
    let tenants = if partitioned {
        let opened = Tenants::open(
            TenantsConfig {
                settings: tenant_settings,
                builder: builder.clone(),
                log_readers: runtime.log_readers(),
            },
            &prepared,
        )
        .await;
        match opened {
            Ok(tenants) => Some(tenants),
            Err(error) => return Err(shutdown_after(runtime, error.into()).await),
        }
    } else {
        if let Err(error) = builder
            .recover_interrupted_recuts(&prepared, &mut catalog)
            .await
        {
            return Err(shutdown_after(runtime, error.into()).await);
        }
        if let Err(error) = builder
            .ensure_initial_snapshot(&prepared, &mut catalog)
            .await
        {
            return Err(shutdown_after(runtime, error.into()).await);
        }
        startup_phase_finished("initial_snapshot", started);
        None
    };
    let started = Instant::now();
    match await_current_cdc(
        &runtime.health(),
        DEV_READY_CATCH_UP_TIMEOUT,
        DEV_READY_POLL_INTERVAL,
    )
    .await
    {
        CdcReadiness::Current => {}
        CdcReadiness::Stopped => {
            let error = DevError::RuntimeUnhealthy {
                state_dir: command.state_dir.clone(),
                database_url_env: command.database.database_url_env.clone(),
                failure: runtime
                    .failure()
                    .unwrap_or_else(|| "persistent CDC stopped".to_owned()),
                remedy: runtime.failure_remedy(),
            };
            return Err(shutdown_after(runtime, error).await);
        }
        CdcReadiness::TimedOut => {
            let error = DevError::RuntimeCatchUpTimeout {
                state_dir: command.state_dir.clone(),
                database_url_env: command.database.database_url_env.clone(),
                seconds: DEV_READY_CATCH_UP_TIMEOUT.as_secs(),
            };
            return Err(shutdown_after(runtime, error).await);
        }
    }

    startup_phase_finished("cdc_catch_up", started);

    // The mutation route's receipt metadata is installed before the listener
    // listens, so no request can reach the route ahead of the relations it
    // needs.
    let started = Instant::now();
    let mutation_database_url = database_url.clone();
    let mutation_prepared = prepared.clone();
    let mutation_support = match run_blocking(move || {
        LocalMutationSupport::open(&mutation_database_url, &mutation_prepared)
            .map_err(DevError::from)
    })
    .await
    {
        Ok(support) => support,
        Err(error) => return Err(shutdown_after(runtime, error).await),
    };
    startup_phase_finished("mutation_support", started);
    let listener = match reserved_listener.listen() {
        Ok(listener) => listener,
        Err(error) => return Err(shutdown_after(runtime, error.into()).await),
    };
    let metrics_listener = match reserved_metrics_listener
        .map(ReservedListener::listen)
        .transpose()
    {
        Ok(listener) => listener,
        Err(error) => return Err(shutdown_after(runtime, error.into()).await),
    };
    let reencode_prepared = prepared.clone();
    let backend = match LocalBackend::new(LocalBackendConfig {
        source_id: state.names.source_id.clone(),
        prepared,
        // Every artifact here was built for this principal's scope, so
        // per-request authorization reaches the scope this process
        // precomputed, or under a partitioned scope its one tenant's.
        credential: LocalCredentialPolicy::DevelopmentSharedBearer {
            token: *secrets.bearer_token.as_bytes(),
            principal,
        },
        // The shared bearer holds the whole registered surface. One shared
        // secret cannot tell two writers apart, so `dev` is meant for loopback
        // only: a non-loopback bind address is for a container whose port the
        // host publishes on loopback.
        command_policy: MutationCommandPolicy::SharedBearer(
            mutation_support.registered_capability(),
        ),
        proof_secret: secrets.proof_secret.clone(),
        plan_secret: secrets.plan_secret.clone(),
        catalog,
        store: std::sync::Arc::clone(&store),
        log: runtime.provider(),
        log_readers: runtime.log_readers(),
        head_signal: head_signal.clone(),
        artifact_resolver,
        change_endpoint,
        plan_ttl: Duration::from_secs(60 * 60),
        max_active_plans: command.max_active_plans,
        max_open_streams: connections.max_open_streams,
        change_tail_bytes: command.change_tail_bytes,
        max_change_batch_bytes: 8 * 1024 * 1024,
        memory: dev_memory_policy(),
        metrics: metrics.clone(),
        // One shared bearer is one principal: there is nothing to narrow
        // short of stopping the process.
        revocation: None,
        // Each tenant's artifact is compressed by its own background stage.
        reencode: (!partitioned).then(|| ReencodeConfig {
            state_root: state_store.root().to_path_buf(),
            catalog_path: state_store.root().join("catalog.sqlite3"),
            cancellation: cancellation.clone(),
        }),
        recut: Some(RecutConfig {
            publisher: std::sync::Arc::new(GenerationArtifactPublisher::new(builder)),
            policy: command.recut.policy(),
            cancellation: cancellation.clone(),
        }),
        tenants: tenants.clone(),
    }) {
        Ok(backend) => backend,
        Err(error) => return Err(shutdown_after(runtime, error.into()).await),
    };
    backend.spawn_reencode(&reencode_prepared);
    backend.spawn_recut();
    if let Some(tenants) = &tenants {
        tenants.start();
    }
    let mutations = match LocalMutationBackend::new(backend.clone(), mutation_support) {
        Ok(mutations) => mutations,
        Err(error) => return Err(shutdown_after(runtime, error.into()).await),
    };
    let artifacts = backend.artifact_service();
    let router = local_router(
        backend,
        mutations,
        artifacts,
        runtime.health(),
        metrics.clone(),
        binding,
        command.allowed_origins,
        connections.max_inflight_requests,
    );
    match &metrics_binding {
        None => print_line("Metrics: off; pass --metrics-port to serve them"),
        Some(metrics) => print_line(format_args!(
            "Metrics: http://{}/metrics",
            metrics.expected_host()
        )),
    }
    if let (Some(column), Some(tenant)) = (&command.tenant_column, &command.tenant) {
        print_line(format_args!(
            "Scope: partitioned by {column}; the bearer reads tenant {tenant}"
        ));
    }
    print_line(format_args!("Calque dev is ready at {base_url}"));
    print_line(format_args!(
        "State directory: {}",
        state_store.root().display()
    ));
    print_line(format_args!(
        "Client settings: {}",
        state_store.client_env_path().display()
    ));
    if bind_ip.is_loopback() {
        print_line("This is a development-only loopback server, not a production one.");
    } else {
        print_line(format_args!(
            "This is a development-only server bound to {bind_ip}, not a production one: only a container whose port the host publishes on loopback should run it."
        ));
    }
    let metrics_stop = CancellationToken::new();
    let metrics_server = metrics_listener
        .zip(metrics_binding)
        .map(|(listener, binding)| {
            spawn_metrics_server(
                listener,
                metrics_router(metrics, binding),
                metrics_stop.clone(),
            )
        });
    let serve_result = axum::serve(listener, router)
        .with_graceful_shutdown(bounded_stop(dev_stop_signal()))
        .await;
    let metrics_result = stop_metrics_server(metrics_server, &metrics_stop).await;
    cancellation.cancel();
    let shutdown_result = shutdown_runtime::<DevError>(runtime).await;
    tracing::info!("stopped");
    serve_result?;
    metrics_result?;
    shutdown_result?;
    Ok(())
}

/// Resolves on what stops `serve`, and on two signs that whoever started `dev`
/// is gone: SIGHUP from a closed terminal, and an output line that met a broken
/// pipe (`log_sink`). Either way `dev` stops as on SIGTERM and releases its
/// source lease; SIGHUP's default action would end it before that release.
async fn dev_stop_signal() {
    tokio::select! {
        () = shutdown_signal() => {},
        () = hangup() => tracing::info!("stopping: received SIGHUP, so the terminal that started calque dev closed"),
        () = crate::log_sink::closed() => tracing::info!("stopping: calque's stdout or stderr is a closed pipe, so whoever started calque dev is gone"),
    }
}

#[cfg(unix)]
async fn hangup() {
    use tokio::signal::unix::{signal, SignalKind};

    match signal(SignalKind::hangup()) {
        Ok(mut hangup) => {
            hangup.recv().await;
        }
        Err(_) => std::future::pending().await,
    }
}

#[cfg(not(unix))]
async fn hangup() {
    std::future::pending().await
}

fn dev_memory_policy() -> LocalMemoryPolicy {
    LocalMemoryPolicy {
        active_artifact_capacity_bytes: LOCAL_ACTIVE_ARTIFACT_CAPACITY_BYTES,
        staging_overlap_bytes: LOCAL_ACTIVE_ARTIFACT_CAPACITY_BYTES,
        catch_up_buffer_bytes: 16 * 1024 * 1024,
        query_result_reserve_bytes: calque_engine::LOCAL_QUERY_RESULT_RESERVE_BYTES,
    }
}

pub fn run_status(state_dir: &Path) -> Result<(), DevError> {
    if !state_dir.exists() {
        return Err(DevError::MissingState {
            state_dir: state_dir.to_path_buf(),
        });
    }
    let store = StateStore::open_existing(state_dir)?;
    let state = store.read_state()?;
    println!("source: {}", state.names.source_id);
    println!("epoch: {}", state.source_epoch);
    println!("tables:");
    for table in &state.tables {
        println!("  - {}", table.display_name());
    }
    println!("state: {}", store.root().display());
    Ok(())
}

pub async fn run_destroy(command: DestroyCommand) -> Result<(), DevError> {
    let state_dir = command.state_dir.as_path();
    if !command.yes {
        return Err(DevError::DestroyNotConfirmed);
    }
    if !state_dir.exists() {
        return Err(DevError::MissingState {
            state_dir: state_dir.to_path_buf(),
        });
    }
    let store = StateStore::open_existing(state_dir)?;
    let state = store.read_state()?;
    if command.local_state_only {
        fs::remove_dir_all(store.root())?;
        println!(
            "Removed Calque state at {}. PostgreSQL objects on the original source were not removed.",
            state_dir.display()
        );
        return Ok(());
    }
    let database_url = match command.database.resolve_allowing_plaintext() {
        Ok(url) => url,
        Err(RecoveryCliError::MissingDatabaseUrl(name)) => {
            return Err(DevError::DestroyMissingDatabaseUrl(name))
        }
        Err(error) => return Err(error.into()),
    };
    if let Some(host) = database_url.plaintext_remote_host() {
        eprintln!("warning: {}", recovery::plaintext_warning(host));
    }
    let identity_database_url = database_url.clone();
    let connected = run_blocking(move || {
        inspect_local_identity(&identity_database_url).map_err(DevError::from)
    })
    .await?;
    if connected != state.source_identity {
        return Err(DevError::SourceIdentityMismatch {
            state_dir: state_dir.to_path_buf(),
            database_url_env: command.database.database_url_env.clone(),
            persisted: describe_source_identity(&state.source_identity),
            connected: describe_source_identity(&connected),
        });
    }
    let destroyed =
        run_blocking(move || destroy_local_source(&database_url, &state).map_err(DevError::from))
            .await
            .map_err(|error| {
                destroy_refusal(error, state_dir, &command.database.database_url_env)
            })?;
    fs::remove_dir_all(store.root())?;
    println!("Destroyed Calque state and its owned PostgreSQL objects.");
    if let Some(publication) = destroyed.kept_publication {
        println!("Kept the publication {publication}, which another role owns.");
    }
    Ok(())
}

/// What destroy reports for a refusal `destroy_local_source` raised, with the
/// remedy the engine leaves to the command.
fn destroy_refusal(error: DevError, state_dir: &Path, database_url_env: &str) -> DevError {
    match error {
        DevError::Source(calque_engine::LocalSourceError::ActiveRuntime) => {
            DevError::DestroyActiveRuntime {
                state_dir: state_dir.to_path_buf(),
            }
        }
        DevError::Source(
            calque_engine::LocalSourceError::PublicationDrift { .. }
            | calque_engine::LocalSourceError::SlotOwnershipMismatch,
        ) => DevError::DestroyOwnershipDrift {
            state_dir: state_dir.to_path_buf(),
        },
        // `run_destroy` compared the identities just before; the source
        // changed between that connection and this one.
        refusal @ DevError::Source(calque_engine::LocalSourceError::SourceIdentityChanged) => {
            DevError::DestroySourceChanged {
                refusal: Box::new(refusal),
                state_dir: state_dir.to_path_buf(),
                database_url_env: database_url_env.to_owned(),
            }
        }
        other => other,
    }
}

pub(crate) fn describe_source_identity(identity: &SourceIdentity) -> String {
    format!(
        "database {} on PostgreSQL system {} timeline {} (previous {})",
        identity.database_name,
        identity.system_identifier,
        identity.timeline_id,
        identity.previous_timeline_id
    )
}

fn display_tables(tables: &[QualifiedTable]) -> String {
    tables
        .iter()
        .map(QualifiedTable::display_name)
        .collect::<Vec<_>>()
        .join(", ")
}

#[derive(Debug, Eq, PartialEq)]
enum CdcReadiness {
    Current,
    Stopped,
    TimedOut,
}

/// Logs the wall time of one startup phase; preparation's own phases log
/// under the same target from `snapshot_pipeline`.
fn startup_phase_finished(phase: &'static str, started: Instant) {
    tracing::info!(
        target: "calque::preparation",
        phase,
        elapsed_ms = started.elapsed().as_millis() as u64,
        "startup phase finished"
    );
}

async fn await_current_cdc(
    health: &LocalSourceHealth,
    timeout: Duration,
    poll_interval: Duration,
) -> CdcReadiness {
    await_current_cdc_with(
        timeout,
        poll_interval,
        || health.is_healthy(),
        || health.is_current(),
    )
    .await
}

async fn await_current_cdc_with(
    timeout: Duration,
    poll_interval: Duration,
    is_healthy: impl Fn() -> bool,
    is_current: impl Fn() -> bool,
) -> CdcReadiness {
    match tokio::time::timeout(timeout, async {
        loop {
            if !is_healthy() {
                return CdcReadiness::Stopped;
            }
            if is_current() {
                return CdcReadiness::Current;
            }
            tokio::time::sleep(poll_interval).await;
        }
    })
    .await
    {
        Ok(readiness) => readiness,
        Err(_) => CdcReadiness::TimedOut,
    }
}

#[derive(Debug, Error)]
pub enum DevError {
    #[error("calque dev creates a publication, a replication slot and the calque_control schema in the database it connects to; pass --acknowledge-disposable-source to confirm that database is disposable")]
    DisposableSourceNotAcknowledged,
    #[error("{0} is set, but calque dev always keeps artifacts in the filesystem store under its state directory and would ignore it. Unset it to run dev, or run calque serve to use S3")]
    ObjectStoreNotSupported(&'static str),
    #[error("--port must be nonzero")]
    InvalidPort,
    #[error("--metrics-port must be nonzero")]
    InvalidMetricsPort,
    #[error("--metrics-port {port} is also the HTTP listener's --port; give the metrics listener a port of its own")]
    MetricsPortIsHttpPort { port: u16 },
    #[error("--table {table} is given more than once; pass each table once")]
    DuplicateTable { table: String },
    #[error("--table {first} and --table {second} would both be served as collection {collection}, which is named by the table alone; pass one of them")]
    SharedCollectionName {
        first: String,
        second: String,
        collection: String,
    },
    #[error("--allow-origin {origin} is given more than once; pass each origin once")]
    DuplicateOrigin { origin: String },
    #[error("at most 256 --table values are allowed")]
    TooManyTables,
    #[error("at most 16 --allow-origin values are allowed")]
    TooManyOrigins,
    #[error("table {table} has a {column} column, but its primary key does not lead with it; a tenant's table leads its primary key with the tenant column, so make {column} the key's first column, or rename the column if every tenant shares the table")]
    TenantColumnNotLeading { table: String, column: String },
    #[error("no --table has a {column} column, so every table would be shared and none would hold a tenant's rows")]
    NoTenantTable { column: String },
    #[error("--tenant {tenant} is not a value of {column}: pass an integer, or a UUID when {column} is a uuid")]
    InvalidTenant { tenant: String, column: String },
    #[error(transparent)]
    Policy(#[from] authz::PolicyError),
    #[error(transparent)]
    Principal(#[from] authz::PrincipalError),
    #[error(transparent)]
    Tenants(#[from] calque_engine::TenantError),
    #[error(transparent)]
    DatabaseEnvironment(#[from] RecoveryCliError),
    #[error(transparent)]
    Guard(#[from] calque_engine::GuardError),
    #[error(transparent)]
    State(#[from] calque_engine::StateError),
    #[error(transparent)]
    Source(#[from] calque_engine::LocalSourceError),
    #[error(transparent)]
    Snapshot(#[from] calque_engine::LocalSnapshotError),
    #[error(transparent)]
    Backend(#[from] calque_engine::LocalBackendError),
    #[error(transparent)]
    Mutation(#[from] calque_engine::LocalMutationError),
    #[error(transparent)]
    Artifact(#[from] calque_engine::ArtifactError),
    #[error(transparent)]
    Catalog(#[from] artifact_catalog::CatalogError),
    #[error(transparent)]
    Storage(#[from] blob_store::StorageError),
    #[error(transparent)]
    Identifier(#[from] sync_types::IdentifierError),
    #[error(transparent)]
    Io(#[from] std::io::Error),
    #[error(transparent)]
    Listen(#[from] ListenError),
    #[error("{state_dir} is enrolled for [{persisted}], but dev requested [{requested}]. Calque will not change an existing enrollment in place. Rerun dev with the persisted table set, or run {destroy} and then rerun dev.", destroy = Destroy::reading(state_dir, database_url_env))]
    PersistedTableSetMismatch {
        state_dir: PathBuf,
        database_url_env: String,
        persisted: String,
        requested: String,
    },
    #[error("connected {connected}, but {state_dir} belongs to {persisted}. Calque cannot reuse state from another PostgreSQL source. {remedy}", remedy = reconnect_or_forget(state_dir, database_url_env, "dev"))]
    DevSourceIdentityMismatch {
        state_dir: PathBuf,
        database_url_env: String,
        persisted: String,
        connected: String,
    },
    #[error("connected {connected}, but {state_dir} belongs to {persisted}. Destroy will not remove PostgreSQL objects because their ownership cannot be verified on this server. Point {database_url_env} at the persisted PostgreSQL source and rerun destroy, or rerun with --local-state-only to remove only {state_dir}; PostgreSQL objects on the original source will remain.")]
    SourceIdentityMismatch {
        state_dir: PathBuf,
        database_url_env: String,
        persisted: String,
        connected: String,
    },
    #[error("no Calque state exists at {state_dir}; calque dev or calque serve creates it")]
    MissingState { state_dir: PathBuf },
    #[error("destroy requires --yes")]
    DestroyNotConfirmed,
    #[error("{0} is required for destroy; set it to the persisted PostgreSQL source or name the variable that holds it with --database-url-env, or rerun with --local-state-only to remove only local files")]
    DestroyMissingDatabaseUrl(String),
    #[error("the source at {state_dir} is still active; stop the calque process that serves it, wait for it to exit, then rerun destroy")]
    DestroyActiveRuntime { state_dir: PathBuf },
    #[error("the persisted publication or replication slot at {state_dir} no longer matches the connected PostgreSQL object. Destroy will not remove an object it cannot verify. Restore the persisted object definition and rerun destroy, or rerun with --local-state-only to remove only local files.")]
    DestroyOwnershipDrift { state_dir: PathBuf },
    /// The engine's refusal of a source that changed after destroy compared
    /// identities, with the remedy `SourceIdentityMismatch` names.
    #[error("{refusal} Point {database_url_env} at the persisted PostgreSQL source and rerun destroy, or rerun with --local-state-only to remove only {state_dir}; PostgreSQL objects on the original source will remain.")]
    DestroySourceChanged {
        refusal: Box<DevError>,
        state_dir: PathBuf,
        database_url_env: String,
    },
    #[error("{startup}\nStopping the source runtime afterwards also failed: {cleanup}")]
    RuntimeCleanup {
        startup: Box<DevError>,
        cleanup: Box<DevError>,
    },
    /// `remedy` is the health slot's: what gets past the error the CDC loop
    /// stopped on.
    #[error("persistent CDC stopped before the HTTP service became ready: {failure}. No incomplete browser state was served. {remedy}", failure = failure.strip_suffix('.').unwrap_or(failure.as_str()), remedy = after_stopped_runtime(Destroy::reading(state_dir, database_url_env), "dev", *remedy))]
    RuntimeUnhealthy {
        state_dir: PathBuf,
        database_url_env: String,
        failure: String,
        remedy: calque_engine::CdcStopRemedy,
    },
    #[error("persistent CDC did not catch up within {seconds} seconds, so the HTTP service was not exposed. Rerun dev; if catch-up times out again, run {destroy} and then rerun dev to rebuild from a complete snapshot.", destroy = Destroy::reading(state_dir, database_url_env))]
    RuntimeCatchUpTimeout {
        state_dir: PathBuf,
        database_url_env: String,
        seconds: u64,
    },
    /// A refusal only `calque destroy` gets past, which the library that
    /// raised it leaves this command to name. See `with_state_remedy`.
    #[error("{refusal} {remedy}", remedy = Destroy::reading(state_dir, database_url_env).then_rerun("dev"))]
    StateMustBeDestroyed {
        refusal: Box<DevError>,
        state_dir: PathBuf,
        database_url_env: String,
    },
    /// The engine's refusal of a source other than the one the state belongs
    /// to, which enrollment met after dev's own comparison passed.
    #[error("{refusal} {remedy}", remedy = reconnect_or_forget(state_dir, database_url_env, "dev"))]
    SourceChanged {
        refusal: Box<DevError>,
        state_dir: PathBuf,
        database_url_env: String,
    },
    /// The engine's refusal of a publication that no longer has its enrolled
    /// definition, which enrollment and a CDC reconnect raise alike. See
    /// `remedy::restore_publication`.
    #[error("{refusal} {remedy}", remedy = restore_publication(Destroy::reading(state_dir, database_url_env), "dev"))]
    PublicationDrifted {
        refusal: Box<DevError>,
        state_dir: PathBuf,
        database_url_env: String,
    },
    #[error("a blocking operation did not complete: {0}")]
    BlockingTask(String),
}

impl CommandError for DevError {
    fn blocking_task(message: String) -> Self {
        Self::BlockingTask(message)
    }

    fn source(&self) -> Option<&calque_engine::LocalSourceError> {
        match self {
            Self::Source(error) => Some(error),
            _ => None,
        }
    }

    fn into_source(self) -> Result<calque_engine::LocalSourceError, Self> {
        match self {
            Self::Source(error) => Ok(error),
            error => Err(error),
        }
    }

    fn runtime_stopped(&self) -> bool {
        matches!(self, Self::RuntimeUnhealthy { .. })
    }

    fn names_its_remedy(&self) -> bool {
        matches!(
            self,
            Self::RuntimeUnhealthy { .. } | Self::RuntimeCatchUpTimeout { .. }
        )
    }

    fn requires_destroying_state(&self) -> bool {
        match self {
            Self::Source(error) => error.requires_destroying_state(),
            Self::Snapshot(error) => error.requires_destroying_state(),
            Self::Catalog(error) => error.requires_destroying_state(),
            _ => false,
        }
    }

    fn runtime_cleanup(startup: Self, cleanup: Self) -> Self {
        Self::RuntimeCleanup {
            startup: Box::new(startup),
            cleanup: Box::new(cleanup),
        }
    }

    fn into_runtime_cleanup(self) -> Result<(Self, Self), Self> {
        match self {
            Self::RuntimeCleanup { startup, cleanup } => Ok((*startup, *cleanup)),
            error => Err(error),
        }
    }

    fn state_must_be_destroyed(refusal: Self, state_dir: &Path, database_url_env: &str) -> Self {
        Self::StateMustBeDestroyed {
            refusal: Box::new(refusal),
            state_dir: state_dir.to_path_buf(),
            database_url_env: database_url_env.to_owned(),
        }
    }

    fn source_changed(refusal: Self, state_dir: &Path, database_url_env: &str) -> Self {
        Self::SourceChanged {
            refusal: Box::new(refusal),
            state_dir: state_dir.to_path_buf(),
            database_url_env: database_url_env.to_owned(),
        }
    }

    fn publication_drifted(refusal: Self, state_dir: &Path, database_url_env: &str) -> Self {
        Self::PublicationDrifted {
            refusal: Box::new(refusal),
            state_dir: state_dir.to_path_buf(),
            database_url_env: database_url_env.to_owned(),
        }
    }
}

#[cfg(test)]
mod tests {
    use calque_engine::{AllowedOrigin, LOCAL_LONG_POLL_TIMEOUT_MS};
    use clap::Parser as _;

    use super::*;
    use crate::cli::{Cli, Command, DatabaseEnvironment};
    use crate::lifecycle::stop_after;

    fn dev_command() -> DevCommand {
        DevCommand {
            state_dir: PathBuf::from("state"),
            tables: vec![QualifiedTable {
                schema: "public".to_owned(),
                table: "tasks".to_owned(),
            }],
            allowed_origins: vec![AllowedOrigin::parse("http://127.0.0.1:5173").unwrap()],
            port: 8787,
            bind_address: std::net::IpAddr::V4(std::net::Ipv4Addr::LOCALHOST),
            metrics_port: None,
            public_base_url: None,
            max_active_plans: calque_engine::DEFAULT_MAX_ACTIVE_PLANS,
            max_inflight_requests: calque_engine::DEFAULT_MAX_INFLIGHT_REQUESTS,
            max_open_streams: calque_engine::DEFAULT_MAX_OPEN_STREAMS,
            change_tail_bytes: calque_engine::DEFAULT_CHANGE_TAIL_BYTES,
            recut: crate::cli::RecutOptions::default(),
            tenant_column: None,
            tenant: None,
            acknowledge_disposable_source: true,
            database: DatabaseEnvironment {
                database_url_env: "CALQUE_DATABASE_URL".to_owned(),
            },
        }
    }

    #[test]
    fn dev_cli_matches_the_documented_copy_paste_surface() {
        let cli = Cli::try_parse_from([
            "calque",
            "dev",
            "--state-dir",
            ".dev-test",
            "--table",
            "public.projects",
            "--table",
            "app.tasks",
            "--allow-origin",
            "http://127.0.0.1:5173",
            "--acknowledge-disposable-source",
        ])
        .unwrap();
        let Command::Dev(command) = cli.command else {
            panic!("expected dev command");
        };
        command.validate().unwrap();
        assert_eq!(command.tables[0].display_name(), "public.projects");
        assert_eq!(command.port, 8787);
        assert_eq!(command.public_base_url, None);
        assert_eq!(command.database.database_url_env, "CALQUE_DATABASE_URL");
        assert!(Cli::try_parse_from([
            "calque",
            "dev",
            "--table",
            "public.tasks",
            "--client-schema",
            "web/src/calque.generated.ts",
        ])
        .is_err());
    }

    #[test]
    fn dev_defaults_its_own_state_directory() {
        let cli = Cli::try_parse_from(["calque", "dev", "--table", "public.tasks"]).unwrap();
        let Command::Dev(command) = cli.command else {
            panic!("expected dev command");
        };
        assert_eq!(command.state_dir, PathBuf::from(".calque-dev"));
        assert_eq!(
            command.max_active_plans,
            calque_engine::DEFAULT_MAX_ACTIVE_PLANS
        );
        assert_eq!(
            command.max_inflight_requests,
            calque_engine::DEFAULT_MAX_INFLIGHT_REQUESTS
        );
        assert_eq!(
            command.max_open_streams,
            calque_engine::DEFAULT_MAX_OPEN_STREAMS
        );
        assert_eq!(
            command.change_tail_bytes,
            calque_engine::DEFAULT_CHANGE_TAIL_BYTES
        );
    }

    #[test]
    fn dev_takes_a_stream_bound_and_a_tail_bound() {
        let parse = |flag: &str, value: &str| {
            Cli::try_parse_from(["calque", "dev", "--table", "public.tasks", flag, value])
        };
        let Command::Dev(command) = parse("--max-open-streams", "3200").unwrap().command else {
            panic!("expected dev command");
        };
        assert_eq!(command.max_open_streams, 3_200);
        let Command::Dev(command) = parse("--change-tail-bytes", "0").unwrap().command else {
            panic!("expected dev command");
        };
        assert_eq!(command.change_tail_bytes, 0);
        for refused in ["0", "1000001", "-1", "many"] {
            assert!(
                parse("--max-open-streams", refused).is_err(),
                "{refused} must be refused"
            );
        }
        for refused in ["68719476737", "-1", "many"] {
            assert!(
                parse("--change-tail-bytes", refused).is_err(),
                "{refused} must be refused"
            );
        }
    }

    #[test]
    fn dev_takes_a_plan_bound_of_at_least_one() {
        let parse = |count: &str| {
            Cli::try_parse_from([
                "calque",
                "dev",
                "--table",
                "public.tasks",
                "--max-active-plans",
                count,
            ])
        };
        let Command::Dev(command) = parse("4096").unwrap().command else {
            panic!("expected dev command");
        };
        assert_eq!(command.max_active_plans, 4096);
        for refused in ["0", "-1", "many"] {
            assert!(parse(refused).is_err(), "{refused} must be refused");
        }
    }

    #[test]
    fn dev_takes_a_request_bound_from_one_to_a_million() {
        let parse = |count: &str| {
            Cli::try_parse_from([
                "calque",
                "dev",
                "--table",
                "public.tasks",
                "--max-inflight-requests",
                count,
            ])
        };
        for (accepted, count) in [("1", 1), ("4128", 4_128), ("1000000", 1_000_000)] {
            let Command::Dev(command) = parse(accepted).unwrap().command else {
                panic!("expected dev command");
            };
            assert_eq!(command.max_inflight_requests, count);
        }
        for refused in ["0", "1000001", "-1", "many"] {
            assert!(parse(refused).is_err(), "{refused} must be refused");
        }
    }

    #[test]
    fn dev_takes_a_tenant_column_only_with_the_tenant_its_bearer_reads() {
        let parse = |flags: &[&str]| {
            Cli::try_parse_from(
                ["calque", "dev", "--table", "public.tasks"]
                    .iter()
                    .chain(flags),
            )
        };
        let Command::Dev(command) = parse(&["--tenant-column", "tenant_id", "--tenant", "42"])
            .unwrap()
            .command
        else {
            panic!("expected dev command");
        };
        assert_eq!(command.tenant_column.as_deref(), Some("tenant_id"));
        assert_eq!(command.tenant.as_deref(), Some("42"));
        assert!(parse(&["--tenant-column", "tenant_id"]).is_err());
        assert!(parse(&["--tenant", "42"]).is_err());
    }

    #[test]
    fn dev_validation_rejects_missing_acknowledgement_and_duplicates() {
        let mut command = dev_command();
        command.acknowledge_disposable_source = false;
        assert!(matches!(
            command.validate(),
            Err(DevError::DisposableSourceNotAcknowledged)
        ));
        command.acknowledge_disposable_source = true;
        command.validate().unwrap();
        command.tables.push(command.tables[0].clone());
        assert_eq!(
            command.validate().unwrap_err().to_string(),
            "--table public.tasks is given more than once; pass each table once"
        );

        let mut command = dev_command();
        command
            .allowed_origins
            .push(command.allowed_origins[0].clone());
        assert_eq!(
            command.validate().unwrap_err().to_string(),
            "--allow-origin http://127.0.0.1:5173 is given more than once; pass each origin once"
        );

        let mut command = dev_command();
        command.port = 0;
        assert!(matches!(command.validate(), Err(DevError::InvalidPort)));

        let mut command = dev_command();
        command.metrics_port = Some(0);
        assert!(matches!(
            command.validate(),
            Err(DevError::InvalidMetricsPort)
        ));

        let mut command = dev_command();
        command.metrics_port = Some(command.port);
        assert_eq!(
            command.validate().unwrap_err().to_string(),
            "--metrics-port 8787 is also the HTTP listener's --port; give the metrics listener a port of its own"
        );
    }

    /// Refused before any state is written: a state directory that recorded
    /// both tables would refuse every later table set until destroyed.
    #[test]
    fn dev_refuses_one_table_name_in_two_schemas() {
        let cli = Cli::try_parse_from([
            "calque",
            "dev",
            "--table",
            "public.tasks",
            "--table",
            "archive.tasks",
            "--acknowledge-disposable-source",
        ])
        .unwrap();
        let Command::Dev(command) = cli.command else {
            panic!("expected dev command");
        };
        assert_eq!(
            command.validate().unwrap_err().to_string(),
            "--table public.tasks and --table archive.tasks would both be served as collection \
             tasks, which is named by the table alone; pass one of them"
        );
    }

    #[test]
    fn destroy_accepts_explicit_local_state_only_recovery() {
        let cli = Cli::try_parse_from([
            "calque",
            "destroy",
            "--state-dir",
            ".calque-dev",
            "--yes",
            "--local-state-only",
        ])
        .unwrap();
        let Command::Destroy(command) = cli.command else {
            panic!("expected destroy command");
        };
        assert!(command.yes);
        assert!(command.local_state_only);
        assert!(Cli::try_parse_from(["calque", "destroy", "--local-state-only"]).is_err());
    }

    #[test]
    fn local_change_wait_satisfies_the_public_bootstrap_contract() {
        assert!((1_000..=120_000).contains(&LOCAL_LONG_POLL_TIMEOUT_MS));
    }

    #[test]
    fn identity_mismatch_explains_refusal_and_both_recovery_paths() {
        let error = DevError::SourceIdentityMismatch {
            state_dir: ".calque-dev".into(),
            database_url_env: "CALQUE_DATABASE_URL".to_owned(),
            persisted: "database calque_dev on PostgreSQL system 111 timeline 1 (previous 1)"
                .to_owned(),
            connected: "database calque_dev on PostgreSQL system 222 timeline 2 (previous 1)"
                .to_owned(),
        };
        assert_eq!(
            error.to_string(),
            "connected database calque_dev on PostgreSQL system 222 timeline 2 (previous 1), but .calque-dev belongs to database calque_dev on PostgreSQL system 111 timeline 1 (previous 1). Destroy will not remove PostgreSQL objects because their ownership cannot be verified on this server. Point CALQUE_DATABASE_URL at the persisted PostgreSQL source and rerun destroy, or rerun with --local-state-only to remove only .calque-dev; PostgreSQL objects on the original source will remain."
        );
    }

    #[tokio::test]
    async fn readiness_waits_for_healthy_cdc_to_finish_tail_catch_up() {
        use std::sync::{
            atomic::{AtomicBool, Ordering},
            Arc,
        };

        let healthy = Arc::new(AtomicBool::new(true));
        let current = Arc::new(AtomicBool::new(false));
        let advance = Arc::clone(&current);
        tokio::spawn(async move {
            tokio::time::sleep(Duration::from_millis(40)).await;
            advance.store(true, Ordering::Release);
        });

        assert_eq!(
            await_current_cdc_with(
                Duration::from_secs(1),
                Duration::from_millis(5),
                || healthy.load(Ordering::Acquire),
                || current.load(Ordering::Acquire),
            )
            .await,
            CdcReadiness::Current
        );

        healthy.store(false, Ordering::Release);
        assert_eq!(
            await_current_cdc_with(
                Duration::from_secs(1),
                Duration::from_millis(5),
                || healthy.load(Ordering::Acquire),
                || false,
            )
            .await,
            CdcReadiness::Stopped
        );
        assert_eq!(
            await_current_cdc_with(
                Duration::from_millis(30),
                Duration::from_millis(5),
                || true,
                || false,
            )
            .await,
            CdcReadiness::TimedOut
        );
    }

    #[test]
    fn table_set_mismatch_names_the_state_and_destroy_recovery() {
        let error = DevError::PersistedTableSetMismatch {
            state_dir: ".calque-dev".into(),
            database_url_env: "CALQUE_DEV_SOURCE_URL".to_owned(),
            persisted: "public.projects, public.tasks".to_owned(),
            requested: "public.issues".to_owned(),
        };
        assert_eq!(
            error.to_string(),
            ".calque-dev is enrolled for [public.projects, public.tasks], but dev requested [public.issues]. Calque will not change an existing enrollment in place. Rerun dev with the persisted table set, or run calque destroy --state-dir .calque-dev --database-url-env CALQUE_DEV_SOURCE_URL --yes and then rerun dev."
        );
    }

    /// A runtime that stopped before readiness names the destroy command with
    /// the variable dev read, since destroy's default may name another source.
    #[test]
    fn a_stopped_runtime_names_destroy_with_the_database_variable_dev_read() {
        let error = DevError::RuntimeUnhealthy {
            state_dir: ".calque-dev".into(),
            database_url_env: "CALQUE_DEV_SOURCE_URL".to_owned(),
            failure: "persistent CDC thread panicked".to_owned(),
            remedy: calque_engine::CdcStopRemedy::Rerun,
        };
        assert_eq!(
            error.to_string(),
            "persistent CDC stopped before the HTTP service became ready: persistent CDC thread panicked. No incomplete browser state was served. Rerun dev; if it stops again, run calque destroy --state-dir .calque-dev --database-url-env CALQUE_DEV_SOURCE_URL --yes and then rerun dev to rebuild from a complete snapshot."
        );
    }

    /// A join error names its task by a process-wide counter, so a message
    /// that carries one is compared with the counter taken out.
    fn without_task_id(message: &str) -> String {
        let (before, after) = message
            .split_once("did not complete: task ")
            .unwrap_or_else(|| panic!("no join error in {message:?}"));
        let digits = after
            .find(|character: char| !character.is_ascii_digit())
            .unwrap_or(after.len());
        assert!(digits > 0, "no task id in {message:?}");
        format!("{before}did not complete: task <id>{}", &after[digits..])
    }

    #[tokio::test]
    async fn a_panicked_blocking_operation_names_its_panic() {
        let error = run_blocking(|| -> Result<(), DevError> {
            panic!("the operation panicked on purpose")
        })
        .await
        .expect_err("a panicked operation is refused");
        assert_eq!(
            without_task_id(&error.to_string()),
            "a blocking operation did not complete: task <id> panicked with message \"the operation panicked on purpose\""
        );
    }

    /// A startup failure that `shutdown_after` follows, as dev reports it.
    fn catch_up_timeout() -> DevError {
        DevError::RuntimeCatchUpTimeout {
            state_dir: ".calque-dev".into(),
            database_url_env: "CALQUE_DEV_SOURCE_URL".to_owned(),
            seconds: 30,
        }
    }

    #[tokio::test]
    async fn a_failed_stop_keeps_the_startup_failure() {
        let error = stop_after(
            || Err(calque_engine::LocalSourceError::IngestThreadPanicked),
            catch_up_timeout(),
        )
        .await;
        assert_eq!(
            error.to_string(),
            "persistent CDC did not catch up within 30 seconds, so the HTTP service was not exposed. Rerun dev; if catch-up times out again, run calque destroy --state-dir .calque-dev --database-url-env CALQUE_DEV_SOURCE_URL --yes and then rerun dev to rebuild from a complete snapshot.\nStopping the source runtime afterwards also failed: persistent CDC thread panicked"
        );
    }

    #[tokio::test]
    async fn a_panicked_stop_keeps_the_startup_failure_and_names_the_panic() {
        let error = stop_after(
            || panic!("the source runtime panicked while stopping"),
            catch_up_timeout(),
        )
        .await;
        assert_eq!(
            without_task_id(&error.to_string()),
            "persistent CDC did not catch up within 30 seconds, so the HTTP service was not exposed. Rerun dev; if catch-up times out again, run calque destroy --state-dir .calque-dev --database-url-env CALQUE_DEV_SOURCE_URL --yes and then rerun dev to rebuild from a complete snapshot.\nStopping the source runtime afterwards also failed: a blocking operation did not complete: task <id> panicked with message \"the source runtime panicked while stopping\""
        );
    }

    #[tokio::test]
    async fn a_clean_stop_returns_the_startup_failure_alone() {
        let error = stop_after(|| Ok(()), catch_up_timeout()).await;
        assert_eq!(error.to_string(), catch_up_timeout().to_string());
    }

    /// The startup failure of a runtime whose CDC loop stopped before
    /// readiness, carrying the health text dev read.
    fn stopped_runtime(failure: &str) -> DevError {
        DevError::RuntimeUnhealthy {
            state_dir: ".calque-dev".into(),
            database_url_env: "CALQUE_DEV_SOURCE_URL".to_owned(),
            failure: failure.to_owned(),
            remedy: calque_engine::CdcStopRemedy::Rerun,
        }
    }

    /// A stopped runtime's join returns the error its CDC loop stopped on,
    /// which the health text already reported, so the message holds that
    /// cause and its remedy once.
    #[tokio::test]
    async fn a_stopped_runtime_reports_the_error_it_stopped_on_once() {
        let error = stop_after(
            || Err(calque_engine::LocalSourceError::InvalidIdentity),
            stopped_runtime("persistent CDC failed: source identity contains an invalid timeline"),
        )
        .await;
        assert_eq!(
            error.to_string(),
            "persistent CDC stopped before the HTTP service became ready: persistent CDC failed: source identity contains an invalid timeline. No incomplete browser state was served. Rerun dev; if it stops again, run calque destroy --state-dir .calque-dev --database-url-env CALQUE_DEV_SOURCE_URL --yes and then rerun dev to rebuild from a complete snapshot."
        );
    }

    /// The loop's own panic reaches the join as `IngestThreadPanicked`, and
    /// the health text already reported it as `cdc-runtime-panic`.
    #[tokio::test]
    async fn a_stopped_runtime_reports_its_own_panic_once() {
        let error = stop_after(
            || Err(calque_engine::LocalSourceError::IngestThreadPanicked),
            stopped_runtime("cdc-runtime-panic"),
        )
        .await;
        assert_eq!(
            error.to_string(),
            "persistent CDC stopped before the HTTP service became ready: cdc-runtime-panic. No incomplete browser state was served. Rerun dev; if it stops again, run calque destroy --state-dir .calque-dev --database-url-env CALQUE_DEV_SOURCE_URL --yes and then rerun dev to rebuild from a complete snapshot."
        );
    }

    /// A failed session cleanup is the stop's own failure, which the health
    /// text did not report, so it follows on a line of its own. The join
    /// carries the loop's error beside it, which the health text did report,
    /// and which is not added again.
    #[tokio::test]
    async fn a_stopped_runtime_whose_cleanup_failed_reports_both() {
        let error = stop_after(
            || {
                Err(calque_engine::LocalSourceError::SessionCleanupFailedAfter(
                    Box::new(calque_engine::LocalSourceError::InvalidIdentity),
                ))
            },
            stopped_runtime("persistent CDC failed: source identity contains an invalid timeline"),
        )
        .await;
        assert_eq!(
            error.to_string(),
            "persistent CDC stopped before the HTTP service became ready: persistent CDC failed: source identity contains an invalid timeline. No incomplete browser state was served. Rerun dev; if it stops again, run calque destroy --state-dir .calque-dev --database-url-env CALQUE_DEV_SOURCE_URL --yes and then rerun dev to rebuild from a complete snapshot.\nStopping the source runtime afterwards also failed: persistent CDC session cleanup failed"
        );

        // After the loop's own panic the join carries `IngestThreadPanicked`,
        // which the health text reported as `cdc-runtime-panic`.
        let error = stop_after(
            || {
                Err(calque_engine::LocalSourceError::SessionCleanupFailedAfter(
                    Box::new(calque_engine::LocalSourceError::IngestThreadPanicked),
                ))
            },
            stopped_runtime("cdc-runtime-panic; cdc-runtime-cleanup"),
        )
        .await;
        assert_eq!(
            error.to_string(),
            "persistent CDC stopped before the HTTP service became ready: cdc-runtime-panic; cdc-runtime-cleanup. No incomplete browser state was served. Rerun dev; if it stops again, run calque destroy --state-dir .calque-dev --database-url-env CALQUE_DEV_SOURCE_URL --yes and then rerun dev to rebuild from a complete snapshot.\nStopping the source runtime afterwards also failed: persistent CDC session cleanup failed"
        );
    }

    /// A panic after the loop stopped is the stop's own failure too, apart
    /// from the loop's own panic, so it follows on a line of its own.
    #[tokio::test]
    async fn a_stopped_runtime_whose_cleanup_panicked_reports_both() {
        let error = stop_after(
            || Err(calque_engine::LocalSourceError::IngestThreadPanickedAfterLoop),
            stopped_runtime("persistent CDC failed: source identity contains an invalid timeline"),
        )
        .await;
        assert_eq!(
            error.to_string(),
            "persistent CDC stopped before the HTTP service became ready: persistent CDC failed: source identity contains an invalid timeline. No incomplete browser state was served. Rerun dev; if it stops again, run calque destroy --state-dir .calque-dev --database-url-env CALQUE_DEV_SOURCE_URL --yes and then rerun dev to rebuild from a complete snapshot.\nStopping the source runtime afterwards also failed: persistent CDC thread panicked after its loop stopped"
        );
    }

    const EARLIER_LOG: &str = ".calque-dev serves a canonical log an earlier release of calque wrote, and calque does not migrate it.";
    const FOREIGN_BUILD: &str = ".calque-dev holds an interrupted build of local-artifact-earlier-release, an artifact this build of calque does not make, and calque does not migrate it.";
    const DESTROY_THEN_DEV: &str = "Run calque destroy --state-dir .calque-dev --database-url-env CALQUE_DEV_SOURCE_URL --yes, then rerun dev to prepare the working set anew.";
    const RESTORE_THEN_DEV: &str = "Restore the publication's definition if it was altered, then run calque destroy --state-dir .calque-dev --database-url-env CALQUE_DEV_SOURCE_URL --yes and rerun dev to prepare the working set anew, since the one it holds may no longer match PostgreSQL.";

    /// The refusal of a canonical log an earlier release wrote, as the CDC
    /// loop stops on it and the stop that joins the loop returns it.
    fn earlier_log() -> calque_engine::LocalSourceError {
        calque_engine::LocalSourceError::Canonical(canonical_log::LogError::EarlierEntryFormat {
            state_dir: ".calque-dev".into(),
        })
    }

    fn foreign_build() -> DevError {
        DevError::Snapshot(calque_engine::LocalSnapshotError::ForeignArtifactBuild {
            state_dir: ".calque-dev".into(),
            artifact: "local-artifact-earlier-release".to_owned(),
        })
    }

    const DRIFT: &str = "the PostgreSQL publication calque_0123456789abcdef01234567 was altered, or an enum its columns use gained or renamed a value: Calque needs it to publish exactly the enrolled tables, for insert, update, delete and truncate, without publish_via_partition_root or row filters, with the column lists and enum values it had at enrollment.";

    fn drift() -> calque_engine::LocalSourceError {
        calque_engine::LocalSourceError::PublicationDrift {
            publication: "calque_0123456789abcdef01234567".to_owned(),
        }
    }

    /// The message `run_dev` returns for `error`.
    fn printed(error: DevError) -> String {
        with_state_remedy(error, Path::new(".calque-dev"), "CALQUE_DEV_SOURCE_URL").to_string()
    }

    /// A refusal only `calque destroy` gets past ends with that command and
    /// the variable dev read, whichever library raised it.
    #[test]
    fn a_refusal_only_destroy_gets_past_names_it() {
        let catalog = DevError::Catalog(artifact_catalog::CatalogError::EarlierSchemaVersion {
            state_dir: ".calque-dev".into(),
            version: 2,
        });
        assert_eq!(
            printed(catalog),
            format!(".calque-dev holds an artifact catalog an earlier release of calque wrote (schema version 2), and calque does not migrate it. {DESTROY_THEN_DEV}")
        );
        assert_eq!(
            printed(foreign_build()),
            format!("{FOREIGN_BUILD} {DESTROY_THEN_DEV}")
        );
        assert_eq!(
            printed(DevError::Source(earlier_log())),
            format!("{EARLIER_LOG} {DESTROY_THEN_DEV}")
        );
        assert_eq!(
            printed(DevError::Source(
                calque_engine::LocalSourceError::SessionCleanupFailedAfter(Box::new(earlier_log()))
            )),
            format!("persistent CDC session cleanup failed after persistent CDC failed: {EARLIER_LOG} {DESTROY_THEN_DEV}")
        );
        assert_eq!(
            printed(DevError::Source(
                calque_engine::LocalSourceError::RegistryStateMismatch
            )),
            format!("persisted schema registry does not match local state. {DESTROY_THEN_DEV}")
        );
        assert_eq!(
            printed(DevError::Source(
                calque_engine::LocalSourceError::EnrolledByAnotherState
            )),
            format!("this source was enrolled by another state directory at the same path. {DESTROY_THEN_DEV}")
        );
    }

    /// A changed source names reconnecting it or forgetting the state; a
    /// drifted publication names restoring it, whether enrollment or a running
    /// CDC loop refused it.
    #[test]
    fn a_refusal_destroy_does_not_answer_names_its_own_remedy() {
        assert_eq!(
            printed(DevError::Source(
                calque_engine::LocalSourceError::SourceIdentityChanged
            )),
            "connected PostgreSQL identity changed from persisted local state. Point CALQUE_DEV_SOURCE_URL at the persisted source and rerun dev, or run calque destroy --state-dir .calque-dev --yes --local-state-only and then rerun dev; PostgreSQL objects on the original source will remain."
        );
        assert_eq!(
            printed(DevError::Source(
                calque_engine::LocalSourceError::PublicationDrift {
                    publication: "calque_0123456789abcdef01234567".to_owned(),
                }
            )),
            "the PostgreSQL publication calque_0123456789abcdef01234567 was altered, or an enum its columns use gained or renamed a value: Calque needs it to publish exactly the enrolled tables, for insert, update, delete and truncate, without publish_via_partition_root or row filters, with the column lists and enum values it had at enrollment. Restore the publication's definition if it was altered, then run calque destroy --state-dir .calque-dev --database-url-env CALQUE_DEV_SOURCE_URL --yes and rerun dev to prepare the working set anew, since the one it holds may no longer match PostgreSQL."
        );
        assert_eq!(
            printed(DevError::Source(
                calque_engine::LocalSourceError::SessionCleanupFailedAfter(Box::new(drift()))
            )),
            format!("persistent CDC session cleanup failed after persistent CDC failed: {DRIFT} {RESTORE_THEN_DEV}")
        );
    }

    #[test]
    fn destroy_names_the_remedy_for_what_the_engine_refused() {
        let refused = |error| {
            destroy_refusal(
                DevError::Source(error),
                Path::new(".calque-dev"),
                "CALQUE_DATABASE_URL",
            )
            .to_string()
        };
        assert_eq!(
            refused(calque_engine::LocalSourceError::SourceIdentityChanged),
            "connected PostgreSQL identity changed from persisted local state. Point CALQUE_DATABASE_URL at the persisted PostgreSQL source and rerun destroy, or rerun with --local-state-only to remove only .calque-dev; PostgreSQL objects on the original source will remain."
        );
        let drift = "the persisted publication or replication slot at .calque-dev no longer matches the connected PostgreSQL object. Destroy will not remove an object it cannot verify. Restore the persisted object definition and rerun destroy, or rerun with --local-state-only to remove only local files.";
        assert_eq!(
            refused(calque_engine::LocalSourceError::PublicationDrift {
                publication: "calque_0123456789abcdef01234567".to_owned(),
            }),
            drift
        );
        assert_eq!(
            refused(calque_engine::LocalSourceError::SlotOwnershipMismatch),
            drift
        );
        assert_eq!(
            refused(calque_engine::LocalSourceError::ActiveRuntime),
            "the source at .calque-dev is still active; stop the calque process that serves it, wait for it to exit, then rerun destroy"
        );
        assert_eq!(
            refused(calque_engine::LocalSourceError::SessionCleanupFailed),
            "persistent CDC session cleanup failed"
        );
    }

    #[test]
    fn a_failure_destroy_does_not_answer_is_printed_as_raised() {
        for error in [
            DevError::Source(calque_engine::LocalSourceError::SessionCleanupFailed),
            DevError::Snapshot(calque_engine::LocalSnapshotError::Connection),
            DevError::Catalog(artifact_catalog::CatalogError::UnsupportedSchemaVersion(99)),
            DevError::BlockingTask("task 7 panicked".to_owned()),
        ] {
            let raised = error.to_string();
            assert_eq!(printed(error), raised);
        }
    }

    /// The join returns the refusal the health text already holds, so neither
    /// it nor a failed session cleanup adds a second remedy or a second copy
    /// of the cause.
    #[tokio::test]
    async fn a_runtime_stopped_on_a_refusal_names_destroy_once() {
        let stopped = |failure: String| DevError::RuntimeUnhealthy {
            state_dir: ".calque-dev".into(),
            database_url_env: "CALQUE_DEV_SOURCE_URL".to_owned(),
            failure,
            remedy: calque_engine::CdcStopRemedy::DestroyState,
        };

        let joined = stop_after(
            || Err(earlier_log()),
            stopped(format!("persistent CDC failed: {EARLIER_LOG}")),
        )
        .await;
        assert_eq!(
            printed(joined),
            format!("persistent CDC stopped before the HTTP service became ready: persistent CDC failed: {EARLIER_LOG} No incomplete browser state was served. {DESTROY_THEN_DEV}")
        );

        // The cause as the health slot keeps it beside the cleanup token.
        let cause = EARLIER_LOG.trim_end_matches('.');
        let cleanup_failed = printed(
            stop_after(
                || {
                    Err(calque_engine::LocalSourceError::SessionCleanupFailedAfter(
                        Box::new(earlier_log()),
                    ))
                },
                stopped(format!(
                    "persistent CDC failed: {cause}; cdc-runtime-cleanup"
                )),
            )
            .await,
        );
        assert_eq!(
            cleanup_failed,
            format!("persistent CDC stopped before the HTTP service became ready: persistent CDC failed: {cause}; cdc-runtime-cleanup. No incomplete browser state was served. {DESTROY_THEN_DEV}\nStopping the source runtime afterwards also failed: persistent CDC session cleanup failed")
        );
        assert_eq!(cleanup_failed.matches("calque destroy").count(), 1);
    }

    /// A lost snapshot connection stands in for the barrier timeout a stopped
    /// CDC loop causes, which `calque_engine` does not export.
    #[tokio::test]
    async fn a_refusal_the_stop_returns_names_destroy_after_its_line() {
        let error = stop_after(
            || Err(earlier_log()),
            DevError::Snapshot(calque_engine::LocalSnapshotError::Connection),
        )
        .await;
        assert_eq!(
            printed(error),
            format!("local snapshot PostgreSQL connection failed\nStopping the source runtime afterwards also failed: {EARLIER_LOG} {DESTROY_THEN_DEV}")
        );

        // A session cleanup that failed too keeps the refusal beside it.
        let error = stop_after(
            || {
                Err(calque_engine::LocalSourceError::SessionCleanupFailedAfter(
                    Box::new(earlier_log()),
                ))
            },
            DevError::Snapshot(calque_engine::LocalSnapshotError::Connection),
        )
        .await;
        assert_eq!(
            printed(error),
            format!("local snapshot PostgreSQL connection failed\nStopping the source runtime afterwards also failed: persistent CDC session cleanup failed after persistent CDC failed: {EARLIER_LOG} {DESTROY_THEN_DEV}")
        );
    }

    /// The remedy is restoring the publication, which destroy refuses to drop
    /// otherwise, then destroy, since the working set may miss changes.
    #[tokio::test]
    async fn a_runtime_stopped_on_a_drifted_publication_names_restoring_it_once() {
        let error = stop_after(
            || Err(drift()),
            DevError::RuntimeUnhealthy {
                state_dir: ".calque-dev".into(),
                database_url_env: "CALQUE_DEV_SOURCE_URL".to_owned(),
                failure: format!("persistent CDC failed: {DRIFT}"),
                remedy: calque_engine::CdcStopRemedy::RestorePublication,
            },
        )
        .await;
        let printed = printed(error);
        assert_eq!(
            printed,
            format!("persistent CDC stopped before the HTTP service became ready: persistent CDC failed: {DRIFT} No incomplete browser state was served. {RESTORE_THEN_DEV}")
        );
        assert_eq!(
            printed
                .matches("Restore the publication's definition")
                .count(),
            1
        );
    }

    #[tokio::test]
    async fn a_drifted_publication_the_stop_returns_names_restoring_it_after_its_line() {
        let error = stop_after(
            || Err(drift()),
            DevError::Snapshot(calque_engine::LocalSnapshotError::Connection),
        )
        .await;
        assert_eq!(
            printed(error),
            format!("local snapshot PostgreSQL connection failed\nStopping the source runtime afterwards also failed: {DRIFT} {RESTORE_THEN_DEV}")
        );
    }

    #[tokio::test]
    async fn a_startup_refusal_names_destroy_ahead_of_the_stop() {
        let error = stop_after(
            || Err(calque_engine::LocalSourceError::SessionCleanupFailed),
            foreign_build(),
        )
        .await;
        assert_eq!(
            printed(error),
            format!("{FOREIGN_BUILD} {DESTROY_THEN_DEV}\nStopping the source runtime afterwards also failed: persistent CDC session cleanup failed")
        );

        let error = stop_after(|| Err(earlier_log()), foreign_build()).await;
        assert_eq!(
            printed(error),
            format!("{FOREIGN_BUILD} {DESTROY_THEN_DEV}\nStopping the source runtime afterwards also failed: {EARLIER_LOG}")
        );
    }

    /// A catch-up timeout names `calque destroy` in its own remedy, so a
    /// refusal the stop returns after it brings no second one.
    #[tokio::test]
    async fn a_startup_failure_that_names_destroy_keeps_its_own_remedy() {
        let error = printed(stop_after(|| Err(earlier_log()), catch_up_timeout()).await);
        assert_eq!(
            error,
            format!(
                "{}\nStopping the source runtime afterwards also failed: {EARLIER_LOG}",
                catch_up_timeout()
            )
        );
        assert_eq!(error.matches("calque destroy").count(), 1);
    }

    #[tokio::test]
    async fn local_state_only_destroy_needs_no_database_and_removes_owned_files() {
        let temporary = tempfile::tempdir().unwrap();
        let state_dir = temporary.path().join("orphaned-state");
        let store = StateStore::open(&state_dir).unwrap();
        let identity = SourceIdentity {
            system_identifier: "111".to_owned(),
            database_name: "calque_dev".to_owned(),
            timeline_id: 1,
            previous_timeline_id: 1,
        };
        let names = LocalNames::derive(store.root(), &identity).unwrap();
        store
            .write_state(
                &LocalState::new(
                    identity,
                    SourceEpoch::new("orphaned-epoch").unwrap(),
                    names,
                    vec![QualifiedTable {
                        schema: "public".to_owned(),
                        table: "tasks".to_owned(),
                    }],
                )
                .unwrap(),
            )
            .unwrap();
        drop(store);

        run_destroy(DestroyCommand {
            state_dir: state_dir.clone(),
            yes: true,
            local_state_only: true,
            database: DatabaseEnvironment {
                database_url_env: "CALQUE_ABSENT_DESTROY_URL_FOR_TEST".to_owned(),
            },
        })
        .await
        .unwrap();

        assert!(!state_dir.exists());
    }

    #[tokio::test]
    async fn destroy_names_the_missing_database_variable() {
        let temporary = tempfile::tempdir().unwrap();
        let state_dir = temporary.path().join("state");
        let store = StateStore::open(&state_dir).unwrap();
        let identity = SourceIdentity {
            system_identifier: "111".to_owned(),
            database_name: "calque_dev".to_owned(),
            timeline_id: 1,
            previous_timeline_id: 1,
        };
        let names = LocalNames::derive(store.root(), &identity).unwrap();
        store
            .write_state(
                &LocalState::new(
                    identity,
                    SourceEpoch::new("epoch").unwrap(),
                    names,
                    vec![QualifiedTable {
                        schema: "public".to_owned(),
                        table: "tasks".to_owned(),
                    }],
                )
                .unwrap(),
            )
            .unwrap();
        drop(store);

        let error = run_destroy(DestroyCommand {
            state_dir: state_dir.clone(),
            yes: true,
            local_state_only: false,
            database: DatabaseEnvironment {
                database_url_env: "CALQUE_ABSENT_DESTROY_URL_FOR_TEST".to_owned(),
            },
        })
        .await
        .unwrap_err();
        assert!(
            matches!(&error, DevError::DestroyMissingDatabaseUrl(name) if name == "CALQUE_ABSENT_DESTROY_URL_FOR_TEST"),
            "{error}"
        );
        assert_eq!(
            error.to_string(),
            "CALQUE_ABSENT_DESTROY_URL_FOR_TEST is required for destroy; set it to the persisted PostgreSQL source or name the variable that holds it with --database-url-env, or rerun with --local-state-only to remove only local files"
        );
        assert!(state_dir.exists());
    }

    /// A safety-net listener is installed before anything is spawned, so a
    /// SIGTERM delivered before `shutdown_signal` registers its handler does
    /// not kill the test binary.
    #[cfg(unix)]
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn shutdown_signal_resolves_on_real_sigterm() {
        use tokio::signal::unix::{signal, SignalKind};

        let mut safety_net =
            signal(SignalKind::terminate()).expect("failed to install SIGTERM safety net");

        let handle = tokio::spawn(shutdown_signal());
        tokio::time::sleep(Duration::from_millis(100)).await;

        let pid = std::process::id().to_string();
        let status = std::process::Command::new("kill")
            .args(["-TERM", &pid])
            .status()
            .expect("failed to invoke `kill`");
        assert!(status.success(), "kill -TERM did not run successfully");

        tokio::time::timeout(Duration::from_secs(5), handle)
            .await
            .expect("shutdown_signal did not observe SIGTERM within 5s")
            .expect("shutdown_signal task panicked");

        let _ = tokio::time::timeout(Duration::from_secs(1), safety_net.recv()).await;
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn blocking_postgres_adapter_may_drive_its_own_runtime() {
        struct BlockingDrop;

        impl Drop for BlockingDrop {
            fn drop(&mut self) {
                let runtime = tokio::runtime::Builder::new_current_thread()
                    .enable_all()
                    .build()
                    .unwrap();
                runtime.block_on(async {});
            }
        }

        run_blocking(|| -> Result<(), DevError> {
            let runtime = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .map_err(DevError::Io)?;
            runtime.block_on(async {});
            Ok(())
        })
        .await
        .unwrap();

        let resource = run_blocking(|| Ok::<_, DevError>(BlockingDrop))
            .await
            .unwrap();
        run_blocking(move || {
            drop(resource);
            Ok::<_, DevError>(())
        })
        .await
        .unwrap();
    }
}
