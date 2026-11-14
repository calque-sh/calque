use std::{
    collections::{BTreeMap, BTreeSet},
    env, fs,
    net::SocketAddr,
    path::{Path, PathBuf},
    sync::Arc,
    time::{Duration, Instant},
};

use artifact_catalog::SqliteArtifactCatalog;
use authz::{AuthorizationPolicy, PartitionDefinition, PolicyDefinition, TablePermitDefinition};
use base64::{engine::general_purpose::URL_SAFE_NO_PAD, Engine as _};
use blob_store::{
    LocalFilesystemBlobStore, ManagedBlobStore, ObjectMetadata, S3BlobStore, S3BlobStoreConfig,
    S3Endpoint,
};
use calque_engine::{
    check_replication_capacity, inspect_local_identity, local_router, metrics_router,
    prepare_selected_local_source, start_local_source_runtime, AllowedOrigin, ArtifactBuilder,
    AuthorizationFenceStore, CanonicalHeadSignal, CommandOperation, DatabaseUrl,
    GenerationArtifactPublisher, GuardError, HttpBinding, LocalArtifactResolver, LocalBackend,
    LocalBackendConfig, LocalCredentialPolicy, LocalMemoryPolicy, LocalMutationBackend,
    LocalMutationError, LocalMutationSupport, LocalNames, LocalRevocationConfig,
    LocalSnapshotOutcome, LocalSourceRuntimeConfig, LocalState, MountedCommands,
    MutationCommandPolicy, OperationsMetrics, PreparedLocalSource, RecompileAuthorization,
    RecutConfig, RecutPolicy, ReencodeConfig, SelectedColumns, StateStore, TenantSettings, Tenants,
    TenantsConfig, LOCAL_ACTIVE_ARTIFACT_CAPACITY_BYTES, MAX_LOCAL_ARTIFACT_BYTES,
};
use dataset_grant::{GrantVerificationKey, GrantVerifier};
use mutation_crud::{
    crud_command_summaries, CrudOperationName, CrudOperations, PerTableOperations,
};
use serde::Deserialize;
use source_control::{PostgresTypeCodec, QualifiedTable, SchemaRegistry};
use sync_types::{ColumnId, ObjectId, SourceEpoch, SourceId, TableId};
use thiserror::Error;
use tokio_util::sync::CancellationToken;
use uuid::Uuid;

use crate::cli::{ServeCommand, TenantArguments};
use crate::dev::describe_source_identity;
use crate::init::{
    is_environment_variable_name, is_identifier, postgres_identifier, IDENTIFIER_GRAMMAR,
};
use crate::lifecycle::{
    bounded_stop, shutdown_after, shutdown_runtime, shutdown_signal, with_state_remedy,
    CommandError,
};
use crate::listen::{spawn_metrics_server, stop_metrics_server, ListenError, ReservedListener};
use crate::log_sink::print_line;
use crate::open_files::{fit_open_file_limit, ConnectionBounds};
use crate::remedy::{after_stopped_runtime, reconnect_or_forget, restore_publication, Destroy};

const MAX_CONFIG_BYTES: u64 = 16 * 1024 * 1024;

/// The longest a plan lives.
const PLAN_TTL: Duration = Duration::from_secs(60 * 60);

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct ServeManifest {
    application: ApplicationManifest,
    protocol: ProtocolManifest,
    sources: Vec<SourceManifest>,
    scopes: Vec<ScopeManifest>,
    collections: Vec<CollectionManifest>,
    commands: Vec<CommandManifest>,
}

/// One command the static manifest advertises, in `compileConfig`'s vocabulary.
/// Typed rather than `serde_json::Value` so the tables a `crud` command
/// advertises can be checked against the registry `mutation-crud` executes.
#[derive(Debug, Deserialize)]
#[serde(tag = "kind")]
enum CommandManifest {
    #[serde(rename = "crud")]
    Crud(CrudCommandManifest),
    /// Backend commands are not supported. The kind parses only so the refusal
    /// can name the command, as `compileConfig`'s does.
    #[serde(rename = "backend")]
    Backend { id: String },
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct CrudCommandManifest {
    id: String,
    collection: String,
    operations: Vec<CrudOperationManifest>,
    writable_columns: Vec<String>,
}

/// The three operations `mutation-crud` admits, in the manifest's spelling.
#[derive(Debug, Clone, Copy, Deserialize, PartialEq, Eq, PartialOrd, Ord)]
#[serde(rename_all = "lowercase")]
enum CrudOperationManifest {
    Insert,
    Update,
    Delete,
}

impl CrudOperationManifest {
    const fn as_str(self) -> &'static str {
        match self {
            Self::Insert => "insert",
            Self::Update => "update",
            Self::Delete => "delete",
        }
    }

    const fn command_operation(self) -> CommandOperation {
        match self {
            Self::Insert => CommandOperation::Insert,
            Self::Update => CommandOperation::Update,
            Self::Delete => CommandOperation::Delete,
        }
    }
}

/// One `crud` command after structural validation, resolved to the relation it
/// writes so the registry check below never has to look a collection up again.
#[derive(Debug)]
struct ValidatedCrudCommand {
    id: String,
    collection: String,
    relation: RelationManifest,
    /// The operations this deployment advertises for the collection: the
    /// ceiling `compile_command_manifest` describes.
    operations: BTreeSet<CrudOperationManifest>,
    /// The columns this command advertises as writable, and the columns of the
    /// same collection that can never be written (its primary key).
    writable_columns: BTreeSet<String>,
    never_writable_columns: BTreeSet<String>,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct ApplicationManifest {
    id: String,
    version: String,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct ProtocolManifest {
    manifest: u32,
    artifact: u32,
    change_stream: u32,
    mutation: u32,
}

#[derive(Debug, Clone, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct SourceManifest {
    id: String,
    database_url_env: String,
    publication: String,
    slot: String,
}

/// A source-wide scope, which every grant reads whole, or a partitioned one,
/// whose grants each read the one tenant their `attribute` names. Any other
/// `kind` is refused: serving it as either would hand a principal rows its
/// scope bounds away.
#[derive(Debug, Deserialize)]
#[serde(tag = "kind")]
enum ScopeManifest {
    #[serde(rename = "source-wide", rename_all = "camelCase")]
    SourceWide {
        id: String,
        source: String,
        data_version: u64,
    },
    #[serde(rename = "partitioned", rename_all = "camelCase")]
    Partitioned {
        id: String,
        source: String,
        data_version: u64,
        attribute: String,
    },
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct CollectionManifest {
    id: String,
    source: String,
    kind: String,
    relation: RelationManifest,
    columns: Vec<ColumnManifest>,
    primary_key: Vec<String>,
    scope: String,
    /// Under a partitioned scope, the column holding the tenant, which leads
    /// the primary key.
    #[serde(default)]
    partition_column: Option<String>,
    /// Under a partitioned scope, that every tenant reads the collection
    /// whole.
    #[serde(default)]
    shared: Option<bool>,
    adapter: String,
    encoding: String,
}

#[derive(Debug, Clone, Deserialize, PartialEq, Eq, PartialOrd, Ord)]
#[serde(deny_unknown_fields)]
struct RelationManifest {
    schema: String,
    table: String,
}

impl RelationManifest {
    fn display_name(&self) -> String {
        format!("{}.{}", self.schema, self.table)
    }
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct ColumnManifest {
    name: String,
    postgres_type: String,
    nullable: bool,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct VerificationKeyEnvironment {
    key_id: String,
    public_key: String,
}

struct ValidatedServeConfig {
    application_id: String,
    source: SourceManifest,
    scope_id: String,
    /// The grant attribute naming a grant's tenant, when the scope is
    /// partitioned.
    partition_attribute: Option<String>,
    authorization_generation: u64,
    collections: Vec<CollectionManifest>,
    tables: Vec<QualifiedTable>,
    /// The `crud` commands the manifest advertises. `backend` commands are
    /// validated and then dropped: Calque never executes one.
    crud_commands: Vec<ValidatedCrudCommand>,
}

/// Milliseconds since `started` for the startup timing keys, clamped to `u64`
/// so every reader can parse it.
fn elapsed_millis(started: Instant) -> u128 {
    started.elapsed().as_millis().min(u128::from(u64::MAX))
}

pub async fn run(command: ServeCommand) -> Result<(), ServeError> {
    // First: a policy this build cannot serve must not create state, inspect
    // PostgreSQL, start CDC, bind, or report readiness.
    let config = load_and_validate_config(&command.config)?;
    let state_dir = command.state_dir.clone();
    let database_url_env = config.source.database_url_env.clone();
    serve_configured(command, config)
        .await
        .map_err(|error| with_state_remedy(error, &state_dir, &database_url_env))
}

async fn serve_configured(
    command: ServeCommand,
    mut config: ValidatedServeConfig,
) -> Result<(), ServeError> {
    let verifier =
        load_grant_verifier(&command.grant_audience, &command.grant_verification_key_env)?;
    let raw_database_url = env::var(&config.source.database_url_env)
        .map_err(|_| ServeError::MissingDatabaseUrl(config.source.database_url_env.clone()))?;
    let database_url = DatabaseUrl::parse(&raw_database_url)?;
    // Selected before anything creates state, but opened and probed after
    // `StateStore::open`, because both stores keep private files under the
    // state directory.
    let object_store_kind = selected_object_store_kind(|name| env::var(name).ok())?;
    let binding = HttpBinding::configured(
        command.bind_address,
        command.port,
        command.expected_host.as_deref(),
    )?;
    let metrics_binding = metrics_binding(&command, &binding)?;
    // Behind a TLS proxy the browser-visible base is `--public-base-url`, and
    // the origins are the application's own HTTPS origins; the listener itself
    // still speaks plaintext HTTP to that proxy.
    let base_url = command
        .public_base_url
        .clone()
        .unwrap_or_else(|| format!("http://{}", binding.expected_host()));
    let artifact_resolver = LocalArtifactResolver::new(&base_url)?;
    let change_endpoint = artifact_resolver.change_endpoint()?;
    let origins = command
        .allowed_origins
        .iter()
        .map(|origin| AllowedOrigin::parse_https_or_loopback(origin))
        .collect::<Result<Vec<_>, _>>()?;
    // Bound before any state exists, so a port another process holds is
    // refused here rather than after enrollment and the initial snapshot.
    let max_active_plans = command.max_active_plans;
    let ConnectionBounds {
        max_open_streams,
        max_inflight_requests,
    } = fit_open_file_limit(ConnectionBounds {
        max_open_streams: command.max_open_streams,
        max_inflight_requests: command.max_inflight_requests,
    });
    let change_tail_bytes = command.change_tail_bytes;
    let recut_policy = command.recut.policy();
    let reserved_listener = ReservedListener::bind(binding.socket_address(), "--port")?;
    let reserved_metrics_listener = metrics_binding
        .as_ref()
        .map(|metrics| ReservedListener::bind(metrics.socket_address(), "--metrics-bind-address"))
        .transpose()?;

    let state_store = StateStore::open(&command.state_dir)?;
    tracing::info!(state_dir = %state_store.root().display(), "using state directory");
    let state = load_or_initialize_state(&state_store, &database_url, &config).await?;
    // The fence is read before any policy is compiled. The manifest's
    // generation is a floor: a generation fenced on this source stays fenced
    // whatever an older manifest says, and a newer manifest fences everything
    // below it. From here on `config.authorization_generation` is the
    // generation in force.
    let fence = AuthorizationFenceStore::new(database_url.clone(), state.names.source_id.clone());
    let establish = fence.clone();
    let configured_generation = config.authorization_generation;
    config.authorization_generation =
        run_blocking(move || establish.establish(configured_generation)).await?;
    let secrets = state_store.load_or_create_secrets()?;
    let had_registry = state_store.registry_path().exists();
    let persisted_registry = if had_registry {
        Some(state_store.read_registry()?)
    } else {
        None
    };
    let prepare_database_url = database_url.clone();
    let prepare_state = state.clone();
    let prepare_secrets = secrets.clone();
    let selected = selected_columns(&config);
    let prepared = run_blocking(move || {
        prepare_selected_local_source(
            &prepare_database_url,
            &prepare_state,
            &prepare_secrets,
            persisted_registry,
            &selected,
        )
    })
    .await?;
    let prepared = apply_configured_policy(prepared, &config)?;
    // The generations fenced on this source that a plan may still name.
    let retired = {
        let fence = fence.clone();
        let source_epoch = prepared.activated.source_epoch().clone();
        run_blocking(move || fence.retirements(&source_epoch, PLAN_TTL)).await?
    };
    if !had_registry {
        state_store.write_registry(&prepared.registry)?;
    }
    // What a fence order recompiles: the same permits over the same enrolled
    // registry, at the order's generation.
    let recompile: RecompileAuthorization = {
        let base = prepared.clone();
        let permits = compile_collection_permits(&prepared.registry, &config)?;
        let attribute = config.partition_attribute.clone();
        Arc::new(move |generation| {
            policy_at(&base, &permits, attribute.as_deref(), generation)
                .map_err(|error| error.to_string())
        })
    };
    // The advertised commands and the mounted mutation route must name the
    // same tables; checking here fails a mismatch before CDC starts or a
    // listener exists.
    let commands = compile_command_manifest(&prepared.registry, &config)?;
    let mount = MutationMount::compile(&commands);
    let partitioned = config.partition_attribute.is_some();
    // Before change capture takes its own slot and sender, so a PostgreSQL
    // that cannot hold every tenant build at once is refused at startup
    // rather than when the cap's last build cannot get a slot.
    if partitioned {
        check_replication_capacity(
            &database_url,
            command.tenants.max_tenant_builds,
            &state.names.slot_name,
        )
        .await?;
    }

    let mut catalog = SqliteArtifactCatalog::open(state_store.root().join("catalog.sqlite3"))?;
    let store = open_object_store(object_store_kind, state_store.root())?;
    // A store that cannot round-trip is discovered now, not after the snapshot
    // is cut and a browser holds an artifact URL it cannot fetch.
    probe_object_store(&store).await?;
    let head_signal = CanonicalHeadSignal::new();
    let metrics = OperationsMetrics::new();
    let owner = format!("serve-runtime-{}", Uuid::new_v4().simple());
    let runtime_database_url = database_url.clone();
    let runtime_state = state.clone();
    let runtime_prepared = prepared.clone();
    let runtime_head_signal = head_signal.clone();
    let runtime_metrics = metrics.clone();
    let runtime_state_dir = state_store.root().to_path_buf();
    let runtime_database_url_env = config.source.database_url_env.clone();
    let runtime = run_blocking(move || {
        start_local_source_runtime(
            runtime_prepared,
            LocalSourceRuntimeConfig::new(
                &runtime_database_url,
                &runtime_state,
                &runtime_state_dir,
                owner,
            )
            .with_metrics(runtime_metrics)
            .with_database_url_env(runtime_database_url_env),
            runtime_head_signal,
        )
    })
    .await?;
    let cancellation = CancellationToken::new();
    let builder = ArtifactBuilder {
        database_url: database_url.clone(),
        state_root: state_store.root().to_path_buf(),
        catalog_path: state_store.root().join("catalog.sqlite3"),
        store: store.clone(),
        barrier_provider: runtime.barrier_provider(),
        head_signal: head_signal.clone(),
        cancellation: cancellation.clone(),
        metrics: metrics.clone(),
    };
    let started = Instant::now();
    // A partitioned scope has no source-wide artifact: each tenant's is built
    // on its first request, and the ones published before a restart are
    // found again.
    let (artifact, tenants) = if partitioned {
        let opened = Tenants::open(
            TenantsConfig {
                settings: tenant_settings(&command.tenants, recut_policy),
                builder: builder.clone(),
                log_readers: runtime.log_readers(),
            },
            &prepared,
        )
        .await;
        match opened {
            Ok(tenants) => ("per-tenant", Some(tenants)),
            Err(error) => return Err(shutdown_after(runtime, error.into()).await),
        }
    } else {
        if let Err(error) = builder
            .recover_interrupted_recuts(&prepared, &mut catalog)
            .await
        {
            return Err(shutdown_after(runtime, error.into()).await);
        }
        match builder
            .ensure_initial_snapshot(&prepared, &mut catalog)
            .await
        {
            Ok(LocalSnapshotOutcome::Existing) => ("published-existing", None),
            Ok(LocalSnapshotOutcome::Published) => ("published", None),
            Err(error) => return Err(shutdown_after(runtime, error.into()).await),
        }
    };
    if !runtime.is_healthy() {
        let error = ServeError::RuntimeUnhealthy {
            state_dir: state_store.root().to_path_buf(),
            database_url_env: config.source.database_url_env.clone(),
            failure: runtime
                .failure()
                .unwrap_or_else(|| "persistent CDC stopped".to_owned()),
            remedy: runtime.failure_remedy(),
        };
        return Err(shutdown_after(runtime, error).await);
    }

    // A fence order publishes its generation's artifact in the background,
    // through the same snapshot path startup just ran.
    let publisher = Arc::new(GenerationArtifactPublisher::new(builder));

    // Publication and CDC readiness are proven before a socket can make this
    // process observable as ready: the address is only bound until here.
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
    // The command registry registers only the advertised operations.
    let mutation_support = {
        let mutation_database_url = database_url.clone();
        let mutation_prepared = prepared.clone();
        let registration = mount.registration.clone();
        match run_blocking(move || {
            LocalMutationSupport::open_with_operations(
                &mutation_database_url,
                &mutation_prepared,
                &registration,
            )
        })
        .await
        {
            Ok(support) => support,
            Err(error) => return Err(shutdown_after(runtime, error).await),
        }
    };
    let reencode_prepared = prepared.clone();
    let backend = match LocalBackend::new(LocalBackendConfig {
        metrics: metrics.clone(),
        source_id: state.names.source_id.clone(),
        prepared,
        credential: LocalCredentialPolicy::DatasetGrant(Box::new(verifier)),
        // Reading and writing are two decisions over the same verified grant:
        // `credential` decides what it may read, this what it may write.
        command_policy: MutationCommandPolicy::SignedGrant(mount.commands.clone()),
        proof_secret: secrets.proof_secret,
        plan_secret: secrets.plan_secret,
        catalog,
        store: store.clone(),
        log: runtime.provider(),
        log_readers: runtime.log_readers(),
        head_signal,
        artifact_resolver,
        change_endpoint,
        plan_ttl: PLAN_TTL,
        max_active_plans,
        max_open_streams,
        change_tail_bytes,
        max_change_batch_bytes: 8 * 1024 * 1024,
        memory: LocalMemoryPolicy {
            active_artifact_capacity_bytes: LOCAL_ACTIVE_ARTIFACT_CAPACITY_BYTES,
            staging_overlap_bytes: LOCAL_ACTIVE_ARTIFACT_CAPACITY_BYTES,
            catch_up_buffer_bytes: 16 * 1024 * 1024,
            query_result_reserve_bytes: calque_engine::LOCAL_QUERY_RESULT_RESERVE_BYTES,
        },
        revocation: Some(LocalRevocationConfig {
            store: fence,
            retired,
            recompile,
            publisher: Arc::clone(&publisher),
        }),
        // Each tenant's artifact is compressed by its own background stage.
        reencode: (!partitioned).then(|| ReencodeConfig {
            state_root: state_store.root().to_path_buf(),
            catalog_path: state_store.root().join("catalog.sqlite3"),
            cancellation: cancellation.clone(),
        }),
        // Through the fenced generation's publisher, so a re-cut and a
        // fenced generation's artifact are never built at once.
        recut: Some(RecutConfig {
            publisher,
            policy: recut_policy,
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
    // A mutation needs a signed grant naming its collection and operation;
    // read authorization never implies write authority. A grant with no write
    // capability gets a 404 before its body is read, so it cannot tell that a
    // write surface exists.
    let artifacts = backend.artifact_service();
    let mutations = match LocalMutationBackend::new(backend.clone(), mutation_support) {
        Ok(mutations) => mutations,
        Err(error) => return Err(shutdown_after(runtime, error.into()).await),
    };
    let router = local_router(
        backend,
        mutations,
        artifacts,
        runtime.health(),
        metrics.clone(),
        binding.clone(),
        origins,
        max_inflight_requests,
    );

    match &config.partition_attribute {
        None => print_line("scope_mode=source-wide"),
        Some(attribute) => {
            print_line("scope_mode=partitioned");
            print_line(format_args!("partition_attribute={attribute}"));
            print_line(format_args!(
                "max_tenant_builds={}",
                command.tenants.max_tenant_builds
            ));
        }
    }
    print_line(format_args!("application={}", config.application_id));
    print_line(format_args!("dataset={}", config.scope_id));
    print_line("authorization=verified-dataset-grants");
    // The generation in force after the durable fence, which is the
    // manifest's only when nothing fenced past it.
    print_line(format_args!(
        "authorization_generation={}",
        config.authorization_generation
    ));
    // An operator reading the startup contract has to be able to tell a
    // loopback development run from a container a composition can reach.
    print_line(format_args!("listener={}", listener_reach(&binding)));
    print_line(format_args!("expected_host={}", binding.expected_host()));
    match &metrics_binding {
        None => print_line("metrics_listener=off"),
        Some(metrics) => {
            print_line(format_args!("metrics_listener={}", listener_reach(metrics)));
            print_line(format_args!(
                "metrics_expected_host={}",
                metrics.expected_host()
            ));
        }
    }
    print_line(format_args!("state_dir={}", state_store.root().display()));
    print_line(format_args!("object_store={}", object_store_kind.as_str()));
    print_line(format_args!("commands={}", commands.len()));
    // The mounted collections and operations: the ceiling a signed grant is
    // intersected with.
    print_line("mutations=mounted-per-collection-command-grant");
    print_line(format_args!(
        "mutation_commands={}",
        mount.commands.contract_value()
    ));
    print_line(format_args!("artifact={artifact}"));
    print_line("ready=true");
    print_line(format_args!("startup_millis={}", elapsed_millis(started)));
    print_line(format_args!("Calque serve is ready at {base_url}"));

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
        .with_graceful_shutdown(bounded_stop(shutdown_signal()))
        .await;
    let metrics_result = stop_metrics_server(metrics_server, &metrics_stop).await;
    // A fenced generation's artifact still being cut stops here; the next
    // startup builds it, because it compiles at the fenced generation.
    cancellation.cancel();
    let shutdown_result = shutdown_runtime::<ServeError>(runtime).await;
    tracing::info!("stopped");
    serve_result?;
    metrics_result?;
    shutdown_result?;
    Ok(())
}

fn tenant_settings(arguments: &TenantArguments, refresh: RecutPolicy) -> TenantSettings {
    TenantSettings {
        max_builds: arguments.max_tenant_builds,
        warm_days: arguments.tenant_warm_days,
        refresh,
        hold_open: Duration::from_secs(arguments.tenant_hold_open_seconds),
        compress_above_bytes: arguments.tenant_compress_above_bytes.0,
        max_tenant_bytes: arguments.max_tenant_bytes,
    }
}

/// How the startup contract names who can reach a listener.
fn listener_reach(binding: &HttpBinding) -> &'static str {
    if binding.is_loopback_only() {
        "loopback"
    } else {
        "configured-bind-address"
    }
}

/// The metrics listener `--metrics-bind-address` asks for, if any. It is
/// checked where the HTTP listener's binding is, so a bad one stops startup
/// before any state, PostgreSQL connection or CDC.
fn metrics_binding(
    command: &ServeCommand,
    application: &HttpBinding,
) -> Result<Option<HttpBinding>, ServeError> {
    let Some(address) = command.metrics_bind_address else {
        return Ok(None);
    };
    let expected_host = command.metrics_expected_host.as_deref();
    let binding =
        HttpBinding::configured(address.ip(), address.port(), expected_host).map_err(|source| {
            match (source, expected_host) {
                (GuardError::HttpExpectedHostRequired, _) => {
                    ServeError::MetricsExpectedHostRequired { address }
                }
                (source @ GuardError::HttpExpectedHostMalformed, Some(host)) => {
                    ServeError::MetricsExpectedHost {
                        host: host.to_owned(),
                        source,
                    }
                }
                (source, _) => ServeError::MetricsBinding { address, source },
            }
        })?;
    let http = application.socket_address();
    if share_a_port(address, http) {
        return Err(ServeError::MetricsListenerSharesThePort {
            metrics: address,
            http,
        });
    }
    Ok(Some(binding))
}

/// Whether two listeners would contend for one port: the same port on the
/// same address, or on a wildcard address that covers the other's.
fn share_a_port(first: SocketAddr, second: SocketAddr) -> bool {
    first.port() == second.port()
        && (first.ip() == second.ip()
            || first.ip().is_unspecified()
            || second.ip().is_unspecified())
}

/// The mutation route this process mounts, compiled once at startup from the
/// validated manifest. `validate_commands` refuses a manifest with no crud
/// command, so a running server always mounts one.
struct MutationMount {
    /// The collections and operations a signed command capability is narrowed
    /// to.
    commands: MountedCommands,
    /// The same ceiling, addressed the way `mutation-crud` registers commands.
    /// A table absent here has no registered command at all.
    registration: PerTableOperations,
}

impl MutationMount {
    fn compile(commands: &[MountedCommand]) -> Self {
        let mut mounted = MountedCommands::default();
        let mut registration = BTreeMap::new();
        for command in commands {
            mounted.mount(
                command.collection.clone(),
                command.command_name.clone(),
                command.operations.clone(),
            );
            registration.insert(
                command.table_id.clone(),
                CrudOperations {
                    insert: command.operations.contains(&CommandOperation::Insert),
                    update: command.operations.contains(&CommandOperation::Update),
                    delete: command.operations.contains(&CommandOperation::Delete),
                },
            );
        }
        Self {
            commands: mounted,
            registration: PerTableOperations::ByTable(registration),
        }
    }
}

/// The object store artifacts are published to and served from, selected by
/// `CALQUE_OBJECT_STORE`: the filesystem under the state directory by default,
/// or `s3`, the only choice whose artifacts outlive a container's writable
/// layer.
///
/// Every value comes from an environment variable, never argv, so
/// `CALQUE_S3_SECRET_KEY` stays out of `ps` and shell history. A host-root
/// reader still sees it in `docker inspect` and `/proc/<pid>/environ`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ObjectStoreKind {
    Filesystem,
    S3,
}

impl ObjectStoreKind {
    /// The value the startup contract prints for the store the process opened.
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Filesystem => "filesystem",
            Self::S3 => "s3",
        }
    }
}

pub const OBJECT_STORE_ENV: &str = "CALQUE_OBJECT_STORE";
/// Every variable that configures the S3 store. Setting one while
/// `CALQUE_OBJECT_STORE` is unset or `filesystem` is refused, so a mistyped
/// selector cannot silently write artifacts into a container layer the next
/// `docker compose up` discards.
const S3_ENVIRONMENT: &[&str] = &[
    "CALQUE_S3_BUCKET",
    "CALQUE_S3_PREFIX",
    "CALQUE_S3_REGION",
    "CALQUE_S3_ENDPOINT",
    "CALQUE_S3_ACCESS_KEY",
    "CALQUE_S3_SECRET_KEY",
    "CALQUE_S3_SESSION_TOKEN",
    "CALQUE_S3_FORCE_PATH_STYLE",
];

fn required_environment(name: &str) -> Result<String, ServeError> {
    let value = env::var(name).map_err(|_| ServeError::MissingObjectStoreValue(name.to_owned()))?;
    if value.is_empty() {
        return Err(ServeError::MissingObjectStoreValue(name.to_owned()));
    }
    Ok(value)
}

fn optional_environment(name: &str) -> Option<String> {
    env::var(name).ok().filter(|value| !value.is_empty())
}

/// The first variable that selects or configures an S3 store, for a command
/// that has no S3 store to configure.
pub(crate) fn object_store_variable_set(
    value: impl Fn(&str) -> Option<String>,
) -> Option<&'static str> {
    if value(OBJECT_STORE_ENV).is_some_and(|kind| kind != "filesystem") {
        return Some(OBJECT_STORE_ENV);
    }
    S3_ENVIRONMENT
        .iter()
        .copied()
        .find(|name| value(name).is_some())
}

/// Reads `CALQUE_OBJECT_STORE`, refusing an unknown name and any S3 variable
/// set without selecting `s3`. The recovery commands call it too, so `backup`
/// and `serve` agree on the store.
pub fn selected_object_store_kind(
    value: impl Fn(&str) -> Option<String>,
) -> Result<ObjectStoreKind, ServeError> {
    let kind = match value(OBJECT_STORE_ENV).as_deref() {
        None | Some("filesystem") => ObjectStoreKind::Filesystem,
        Some("s3") => ObjectStoreKind::S3,
        Some(other) => return Err(ServeError::UnknownObjectStore(other.to_owned())),
    };
    if kind == ObjectStoreKind::Filesystem {
        if let Some(name) = S3_ENVIRONMENT.iter().find(|name| value(name).is_some()) {
            return Err(ServeError::ObjectStoreNotSelected((*name).to_owned()));
        }
    }
    Ok(kind)
}

/// Opens the configured store without contacting it; `probe_object_store`
/// refuses a wrong bucket or a rejected credential.
pub fn open_object_store(
    kind: ObjectStoreKind,
    state_root: &Path,
) -> Result<Arc<dyn ManagedBlobStore>, ServeError> {
    match kind {
        ObjectStoreKind::Filesystem => Ok(Arc::new(LocalFilesystemBlobStore::open(
            state_root.join("objects"),
            MAX_LOCAL_ARTIFACT_BYTES,
        )?)),
        ObjectStoreKind::S3 => {
            let endpoint = optional_environment("CALQUE_S3_ENDPOINT");
            // A self-hosted provider is reached by address or by a composition
            // service name, neither of which resolves as a virtual-host bucket
            // name, so an endpoint implies path style unless the operator says
            // otherwise. AWS itself gets virtual-host style.
            let force_path_style = match optional_environment("CALQUE_S3_FORCE_PATH_STYLE") {
                Some(value) if value == "1" => true,
                Some(value) if value == "0" => false,
                Some(value) => {
                    return Err(ServeError::InvalidObjectStoreValue(
                        "CALQUE_S3_FORCE_PATH_STYLE",
                        value,
                        "set it to 1 for path-style bucket addressing or 0 for virtual-host style, or unset it to use path style exactly when CALQUE_S3_ENDPOINT is set",
                    ))
                }
                None => endpoint.is_some(),
            };
            // The S3 adapter hashes and verifies a body on local disk before
            // uploading it, so staging lives under the state directory too.
            let config = S3BlobStoreConfig::new(
                required_environment("CALQUE_S3_BUCKET")?,
                optional_environment("CALQUE_S3_PREFIX").unwrap_or_default(),
                state_root.join("object-store-staging"),
                MAX_LOCAL_ARTIFACT_BYTES,
            )?;
            Ok(Arc::new(S3BlobStore::connect(
                S3Endpoint {
                    region: required_environment("CALQUE_S3_REGION")?,
                    endpoint_url: endpoint,
                    force_path_style,
                    access_key_id: required_environment("CALQUE_S3_ACCESS_KEY")?,
                    secret_access_key: required_environment("CALQUE_S3_SECRET_KEY")?,
                    session_token: optional_environment("CALQUE_S3_SESSION_TOKEN"),
                },
                config,
            )?))
        }
    }
}

/// The bytes the startup probe writes and reads back.
const OBJECT_STORE_PROBE_BODY: &[u8] = b"calque-serve-object-store-probe-v1";

/// Proves the configured store can write, read back and delete an object, so
/// a wrong bucket, an unreachable endpoint or a rejected credential stops
/// startup before the snapshot, CDC or the listener. A failed probe leaves only
/// the state directory and the enrolled source behind; the probe object is
/// deleted on every path after the put.
async fn probe_object_store(store: &Arc<dyn ManagedBlobStore>) -> Result<(), ServeError> {
    let object = ObjectId::new(format!(
        "calque-serve-object-store-probe/{}",
        Uuid::new_v4().simple()
    ))?;
    let metadata = ObjectMetadata::for_bytes(OBJECT_STORE_PROBE_BODY);
    store
        .put_if_absent(
            &object,
            Box::pin(std::io::Cursor::new(OBJECT_STORE_PROBE_BODY.to_vec())),
            &metadata,
        )
        .await?;
    let outcome = read_back_probe(store.as_ref(), &object, &metadata).await;
    // Deletion is attempted whatever the read said, and its own failure is
    // reported only when the read succeeded: a store that cannot delete is
    // still misconfigured, but the read failure is the more useful one.
    let deleted = store.delete(&object).await;
    outcome?;
    deleted?;
    Ok(())
}

async fn read_back_probe(
    store: &dyn ManagedBlobStore,
    object: &ObjectId,
    metadata: &ObjectMetadata,
) -> Result<(), ServeError> {
    if store.head(object).await?.as_ref() != Some(metadata) {
        return Err(ServeError::ObjectStoreProbeFailed(
            "the probe object was absent or its metadata did not match immediately after it was written",
        ));
    }
    let mut stream = store
        .get(object)
        .await?
        .ok_or(ServeError::ObjectStoreProbeFailed(
            "the probe object could not be read back after it was written",
        ))?;
    let mut body = Vec::new();
    tokio::io::AsyncReadExt::read_to_end(&mut stream, &mut body).await?;
    if body != OBJECT_STORE_PROBE_BODY {
        return Err(ServeError::ObjectStoreProbeFailed(
            "the probe object read back with different bytes than were written",
        ));
    }
    Ok(())
}

fn load_and_validate_config(path: &Path) -> Result<ValidatedServeConfig, ServeError> {
    let unreadable = |source| ServeError::ReadConfig {
        path: path.to_owned(),
        source,
    };
    let metadata = fs::metadata(path).map_err(unreadable)?;
    if metadata.len() > MAX_CONFIG_BYTES {
        return Err(ServeError::InvalidConfig("configuration is too large"));
    }
    let bytes = fs::read(path).map_err(unreadable)?;
    let manifest: ServeManifest = serde_json::from_slice(&bytes).map_err(|source| {
        if names_renamed_connection_env(&bytes) {
            ServeError::RenamedConnectionEnv {
                path: path.to_owned(),
            }
        } else {
            ServeError::ParseConfig {
                path: path.to_owned(),
                source,
            }
        }
    })?;
    validate_manifest(manifest)
}

/// Whether a source in the manifest carries `connectionEnv`, the earlier name
/// of `databaseUrlEnv`.
fn names_renamed_connection_env(bytes: &[u8]) -> bool {
    serde_json::from_slice::<serde_json::Value>(bytes)
        .ok()
        .and_then(|manifest| {
            let sources = manifest.get("sources")?.as_array()?;
            Some(
                sources
                    .iter()
                    .any(|source| source.get("connectionEnv").is_some()),
            )
        })
        .unwrap_or(false)
}

fn validate_manifest(manifest: ServeManifest) -> Result<ValidatedServeConfig, ServeError> {
    if manifest.application.id.is_empty() || manifest.application.version.is_empty() {
        return Err(ServeError::InvalidConfig("application identity is empty"));
    }
    if [
        manifest.protocol.manifest,
        manifest.protocol.artifact,
        manifest.protocol.change_stream,
        manifest.protocol.mutation,
    ] != [1, 1, 1, 1]
    {
        return Err(ServeError::InvalidConfig(
            "protocol.manifest, protocol.artifact, protocol.changeStream and protocol.mutation must each be 1",
        ));
    }
    if manifest.sources.len() != 1 || manifest.scopes.len() != 1 {
        return Err(ServeError::InvalidConfig(
            "serve requires exactly one source and one scope",
        ));
    }
    if !is_identifier(&manifest.application.id) {
        return Err(ServeError::InvalidConfigField {
            field: "application.id".to_owned(),
            reason: format!("must match {IDENTIFIER_GRAMMAR}"),
        });
    }
    let source = manifest.sources.into_iter().next().expect("length checked");
    if !is_identifier(&source.id) {
        return Err(ServeError::InvalidConfigField {
            field: "sources[0].id".to_owned(),
            reason: format!("must match {IDENTIFIER_GRAMMAR}"),
        });
    }
    // The value is not echoed: one that is no variable name is often the
    // connection string itself.
    if !is_environment_variable_name(&source.database_url_env) {
        return Err(ServeError::InvalidConfigField {
            field: "sources[0].databaseUrlEnv".to_owned(),
            reason: "must name an environment variable matching ^[A-Z_][A-Z0-9_]*$, not contain a connection string".to_owned(),
        });
    }
    for (field, value) in [
        ("sources[0].publication", &source.publication),
        ("sources[0].slot", &source.slot),
    ] {
        postgres_identifier(field, value).map_err(|error| ServeError::InvalidConfigField {
            field: field.to_owned(),
            reason: error.to_string(),
        })?;
    }
    let (scope_id, scope_source, data_version, partition_attribute) =
        match manifest.scopes.into_iter().next().expect("length checked") {
            ScopeManifest::SourceWide {
                id,
                source,
                data_version,
            } => (id, source, data_version, None),
            ScopeManifest::Partitioned {
                id,
                source,
                data_version,
                attribute,
            } => (id, source, data_version, Some(attribute)),
        };
    if !is_identifier(&scope_id) {
        return Err(ServeError::InvalidConfigField {
            field: "scopes[0].id".to_owned(),
            reason: format!("must match {IDENTIFIER_GRAMMAR}"),
        });
    }
    if scope_source != source.id {
        return Err(ServeError::InvalidConfig(
            "scopes[0].source must be sources[0].id",
        ));
    }
    if data_version == 0 {
        return Err(ServeError::InvalidConfig(
            "scopes[0].dataVersion must be at least 1",
        ));
    }
    if manifest.collections.is_empty() {
        return Err(ServeError::InvalidConfig("dataset has no collections"));
    }
    let mut tables = manifest
        .collections
        .iter()
        .map(|collection| QualifiedTable {
            schema: collection.relation.schema.clone(),
            table: collection.relation.table.clone(),
        })
        .collect::<Vec<_>>();
    // Checked before the collection ids: a collection's id is its table's
    // name, so two schemas' tables of one name also share an id, and this
    // refusal names the cause where the duplicate id would not.
    if let Some((first, second)) = QualifiedTable::first_shared_collection_name(&tables) {
        return Err(ServeError::SharedCollectionName {
            first: first.display_name(),
            second: second.display_name(),
            collection: second.table.clone(),
        });
    }
    let mut ids = BTreeSet::new();
    let mut relations = BTreeSet::new();
    for collection in &manifest.collections {
        let refuse = |reason: String| ServeError::InvalidCollection {
            collection: collection.id.clone(),
            reason,
        };
        let contract = [
            (
                !ids.insert(collection.id.as_str()),
                "another collection has the same id",
            ),
            (
                !relations.insert(collection.relation.clone()),
                "another collection serves the same relation",
            ),
            (
                collection.source != source.id,
                "its source must be sources[0].id",
            ),
            (
                collection.scope != scope_id,
                "its scope must be scopes[0].id",
            ),
            (collection.kind != "table", "its kind must be \"table\""),
            (collection.columns.is_empty(), "it names no columns"),
            (collection.primary_key.is_empty(), "its primaryKey is empty"),
            (
                collection.adapter != "direct-columnar",
                "its adapter must be \"direct-columnar\"",
            ),
            (
                collection.encoding != "calque-columnar",
                "its encoding must be \"calque-columnar\"",
            ),
        ];
        if let Some((_, reason)) = contract.iter().find(|(broken, _)| *broken) {
            return Err(refuse((*reason).to_owned()));
        }
        if !is_identifier(&collection.id) {
            return Err(refuse(format!("its id must match {IDENTIFIER_GRAMMAR}")));
        }
        for (subject, value) in [
            ("relation.schema", &collection.relation.schema),
            ("relation.table", &collection.relation.table),
        ] {
            postgres_identifier(subject, value).map_err(|error| refuse(error.to_string()))?;
        }
        // The artifact names each table after its source relation, and the
        // client binds a collection only to the artifact table of the same
        // name, so a collection under any other id would serve and never bind.
        if collection.id != collection.relation.table {
            return Err(ServeError::CollectionIdIsNotItsTable {
                collection: collection.id.clone(),
                relation: collection.relation.display_name(),
                table: collection.relation.table.clone(),
            });
        }
        let mut selected = BTreeSet::new();
        for column in &collection.columns {
            if !selected.insert(column.name.as_str()) {
                return Err(refuse(format!("column {} is named twice", column.name)));
            }
            postgres_identifier(&format!("column name {:?}", column.name), &column.name)
                .map_err(|error| refuse(error.to_string()))?;
            if PostgresTypeCodec::from_catalog("pg_catalog", &column.postgres_type).is_none() {
                return Err(refuse(format!(
                    "column {} has postgresType {:?}, which is not a type calque serve enrolls",
                    column.name, column.postgres_type
                )));
            }
        }
        if let Some(missing) = collection
            .primary_key
            .iter()
            .find(|column| !selected.contains(column.as_str()))
        {
            return Err(refuse(format!(
                "primaryKey column {missing} is not one of its columns"
            )));
        }
    }
    validate_partitions(partition_attribute.as_deref(), &manifest.collections)?;
    tables.sort();
    let crud_commands = validate_commands(&manifest.commands, &manifest.collections)?;
    Ok(ValidatedServeConfig {
        application_id: manifest.application.id,
        source,
        scope_id,
        partition_attribute,
        authorization_generation: data_version,
        collections: manifest.collections,
        tables,
        crud_commands,
    })
}

/// Checks each collection's partitioning against its scope, as
/// `compileConfig` does: under a partitioned scope a collection names the
/// column holding the tenant, which leads its primary key and is `int2`,
/// `int4`, `int8` or `uuid`, or is shared, and the partition columns have one
/// type. Under a source-wide scope a collection names neither.
fn validate_partitions(
    attribute: Option<&str>,
    collections: &[CollectionManifest],
) -> Result<(), ServeError> {
    let mut types = BTreeSet::new();
    for collection in collections {
        let refuse = |reason: String| ServeError::InvalidCollection {
            collection: collection.id.clone(),
            reason,
        };
        if attribute.is_none() {
            if collection.partition_column.is_some() || collection.shared.is_some() {
                return Err(refuse(
                    "it sets partitionColumn or shared, which only a partitioned scope reads"
                        .to_owned(),
                ));
            }
            continue;
        }
        match (&collection.partition_column, collection.shared) {
            (None, Some(true)) => {}
            (Some(column), None) => {
                if collection.primary_key.first() != Some(column) {
                    return Err(refuse(format!(
                        "it is partitioned by {column}, which must be the first column of its primaryKey"
                    )));
                }
                let postgres_type = collection
                    .columns
                    .iter()
                    .find(|configured| &configured.name == column)
                    .map(|configured| configured.postgres_type.as_str())
                    .unwrap_or_default();
                if !matches!(postgres_type, "int2" | "int4" | "int8" | "uuid") {
                    return Err(refuse(format!(
                        "it is partitioned by {column}, of type {postgres_type}; a partition column is int2, int4, int8 or uuid"
                    )));
                }
                types.insert(postgres_type);
            }
            _ => {
                return Err(refuse(
                    "its scope is partitioned, so it sets exactly one of partitionColumn, naming the column that holds the tenant, or shared: true".to_owned(),
                ))
            }
        }
    }
    if attribute.is_some() && types.is_empty() {
        return Err(ServeError::InvalidConfig(
            "a partitioned scope needs at least one partitioned collection",
        ));
    }
    if types.len() > 1 {
        return Err(ServeError::InvalidConfig(
            "the partition columns of a scope must have one type",
        ));
    }
    Ok(())
}

/// Checks the command manifest against `compileConfig`'s rules before
/// PostgreSQL is reached.
fn validate_commands(
    commands: &[CommandManifest],
    collections: &[CollectionManifest],
) -> Result<Vec<ValidatedCrudCommand>, ServeError> {
    let mut ids = BTreeSet::new();
    let mut written_collections = BTreeSet::new();
    let mut crud_commands = Vec::new();
    for command in commands {
        match command {
            CommandManifest::Crud(CrudCommandManifest {
                id,
                collection,
                operations,
                writable_columns,
            }) => {
                require_command_id(&mut ids, id)?;
                let target = collections
                    .iter()
                    .find(|candidate| candidate.id == *collection)
                    .ok_or_else(|| {
                        ServeError::InvalidCommands(format!(
                            "E_SERVE_COMMAND_INVALID command={id} collection={collection} names no configured collection"
                        ))
                    })?;
                if !written_collections.insert(collection.as_str()) {
                    return Err(ServeError::InvalidCommands(format!(
                        "E_SERVE_COMMAND_INVALID command={id} collection={collection} already has a crud command; one table mounts one crud command"
                    )));
                }
                let declared = operations.len();
                let operations = operations.iter().copied().collect::<BTreeSet<_>>();
                if operations.is_empty() || operations.len() != declared {
                    return Err(ServeError::InvalidCommands(format!(
                        "E_SERVE_COMMAND_INVALID command={id} operations must be a non-empty set without repeats"
                    )));
                }
                let selected = target
                    .columns
                    .iter()
                    .map(|column| column.name.as_str())
                    .collect::<BTreeSet<_>>();
                let immutable = target
                    .primary_key
                    .iter()
                    .map(String::as_str)
                    .collect::<BTreeSet<_>>();
                let unique = writable_columns
                    .iter()
                    .map(String::as_str)
                    .collect::<BTreeSet<_>>();
                if unique.len() != writable_columns.len() {
                    return Err(ServeError::InvalidCommands(format!(
                        "E_SERVE_COMMAND_INVALID command={id} names a writable column twice"
                    )));
                }
                for column in writable_columns {
                    if !selected.contains(column.as_str()) {
                        return Err(ServeError::InvalidCommands(format!(
                            "E_SERVE_COMMAND_INVALID command={id} column={column} is not one of the collection's selected columns"
                        )));
                    }
                    if immutable.contains(column.as_str()) {
                        return Err(ServeError::InvalidCommands(format!(
                            "E_SERVE_COMMAND_INVALID command={id} column={column} is an identity column and can never be written"
                        )));
                    }
                }
                // An insert carries every non-key column.
                if operations.contains(&CrudOperationManifest::Insert) {
                    let omitted = target
                        .columns
                        .iter()
                        .map(|column| column.name.as_str())
                        .filter(|name| !immutable.contains(name) && !unique.contains(name))
                        .collect::<Vec<_>>();
                    if !omitted.is_empty() {
                        return Err(ServeError::InvalidCommands(format!(
                            "E_SERVE_COMMAND_INVALID command={id} declares insert, but its writableColumns omit {}; an insert writes every non-key column",
                            omitted.join(",")
                        )));
                    }
                }
                // An update that can write nothing advertises a capability the
                // route would refuse on every request.
                if operations.contains(&CrudOperationManifest::Update)
                    && writable_columns.is_empty()
                {
                    return Err(ServeError::InvalidCommands(format!(
                        "E_SERVE_COMMAND_INVALID command={id} declares update with no writable column"
                    )));
                }
                crud_commands.push(ValidatedCrudCommand {
                    id: id.clone(),
                    collection: collection.clone(),
                    relation: target.relation.clone(),
                    operations,
                    writable_columns: unique.iter().map(|column| (*column).to_owned()).collect(),
                    never_writable_columns: immutable
                        .iter()
                        .map(|column| (*column).to_owned())
                        .collect(),
                });
            }
            CommandManifest::Backend { id } => {
                return Err(ServeError::InvalidCommands(format!(
                    "E_SERVE_COMMAND_INVALID command={id} backend commands are not supported; declare a crud command"
                )));
            }
        }
    }
    if crud_commands.is_empty() {
        return Err(ServeError::InvalidCommands(
            "E_SERVE_COMMAND_INVALID the manifest advertises no crud command, so the mutation route would mount a writable surface nothing declares".to_owned(),
        ));
    }
    Ok(crud_commands)
}

fn require_command_id<'a>(ids: &mut BTreeSet<&'a str>, id: &'a str) -> Result<(), ServeError> {
    if id.is_empty() {
        return Err(ServeError::InvalidCommands(
            "E_SERVE_COMMAND_INVALID a command id is empty".to_owned(),
        ));
    }
    if !is_identifier(id) {
        return Err(ServeError::InvalidCommands(format!(
            "E_SERVE_COMMAND_INVALID command={id} must match {IDENTIFIER_GRAMMAR}"
        )));
    }
    if !ids.insert(id) {
        return Err(ServeError::InvalidCommands(format!(
            "E_SERVE_COMMAND_INVALID command={id} is named twice"
        )));
    }
    Ok(())
}

/// One collection's mounted command: the table `mutation-crud` registers it
/// for, the name it registers it under, and the operations this deployment
/// advertises for it.
#[derive(Debug, Clone, PartialEq, Eq)]
struct MountedCommand {
    collection: String,
    table_id: TableId,
    command_name: String,
    operations: BTreeSet<CommandOperation>,
}

/// The `crud:<table>` commands the manifest advertises, checked against the
/// surface `mutation-crud` mounts for the enrolled catalog.
///
/// `mutation-crud` registers one command per enrolled table and cannot narrow
/// its writable columns, so the manifest must advertise exactly the enrolled
/// tables with exactly their writable columns. Operations may be a subset: the
/// advertised set is a ceiling that both the signed capability and the
/// registered command enforce.
fn compile_command_manifest(
    registry: &SchemaRegistry,
    config: &ValidatedServeConfig,
) -> Result<Vec<MountedCommand>, ServeError> {
    let mut relation_by_table_id = BTreeMap::new();
    for table in registry.tables() {
        relation_by_table_id.insert(table.table_id.clone(), table.inspection.table.clone());
    }
    let mut registrable = BTreeMap::new();
    for summary in crud_command_summaries(registry.tables(), CrudOperations::ALL) {
        let relation = relation_by_table_id
            .get(&summary.table_id)
            .expect("a crud summary names one of the registry tables it was built from");
        registrable.insert(relation.clone(), summary);
    }

    let mut advertised = BTreeMap::new();
    for command in &config.crud_commands {
        let relation = QualifiedTable {
            schema: command.relation.schema.clone(),
            table: command.relation.table.clone(),
        };
        let summary = registrable.get(&relation).ok_or_else(|| {
            ServeError::InvalidCommands(format!(
                "E_SERVE_COMMAND_TABLE_UNENROLLED command={} collection={} this source enrolls no writable relation the command could execute against",
                command.id, command.collection
            ))
        })?;
        check_advertised_columns(command, &summary.writable_columns)?;
        advertised.insert(
            relation,
            MountedCommand {
                collection: command.collection.clone(),
                table_id: summary.table_id.clone(),
                command_name: summary.name.clone(),
                operations: check_advertised_operations(command, &summary.operations)?,
            },
        );
    }
    let unadvertised = registrable
        .keys()
        .filter(|relation| !advertised.contains_key(*relation))
        .map(QualifiedTable::display_name)
        .collect::<Vec<_>>();
    if !unadvertised.is_empty() {
        return Err(ServeError::InvalidCommands(format!(
            "E_SERVE_COMMAND_TABLE_UNADVERTISED tables={} the catalog registers one crud command per enrolled table, so every one of them must be advertised by a crud command in the manifest",
            unadvertised.join(",")
        )));
    }
    Ok(advertised.into_values().collect())
}

/// The advertised writable columns must equal the catalog's, less the primary
/// key `validate_commands` forbids naming. `mutation-crud` cannot narrow a
/// table's writable columns, so an advertised subset would state a bound
/// nothing enforces, and a superset a column no request could write.
fn check_advertised_columns(
    command: &ValidatedCrudCommand,
    catalog: &[String],
) -> Result<(), ServeError> {
    let catalog = catalog.iter().cloned().collect::<BTreeSet<_>>();
    let expected = catalog
        .difference(&command.never_writable_columns)
        .cloned()
        .collect::<BTreeSet<_>>();
    if expected == command.writable_columns {
        return Ok(());
    }
    Err(ServeError::InvalidCommands(format!(
        "E_SERVE_COMMAND_COLUMNS_MISMATCH command={} collection={} advertised={} catalog={} the catalog's writable columns cannot be narrowed per column, so the manifest has to name exactly them",
        command.id,
        command.collection,
        command
            .writable_columns
            .iter()
            .cloned()
            .collect::<Vec<_>>()
            .join(","),
        expected.into_iter().collect::<Vec<_>>().join(","),
    )))
}

/// The advertised operations, checked against those the catalog admits for
/// the table. Enrollment admits all three for every enrolled table, so only a
/// unit test reaches the refusal; it keeps a wider ceiling from being mounted
/// silently.
fn check_advertised_operations(
    command: &ValidatedCrudCommand,
    admitted: &[CrudOperationName],
) -> Result<BTreeSet<CommandOperation>, ServeError> {
    let admitted = admitted
        .iter()
        .map(|operation| operation.as_str())
        .collect::<BTreeSet<_>>();
    let mut operations = BTreeSet::new();
    for operation in &command.operations {
        if !admitted.contains(operation.as_str()) {
            return Err(ServeError::InvalidCommands(format!(
                "E_SERVE_COMMAND_OPERATION_UNADMITTED command={} collection={} operation={} catalog={} the catalog admits only these operations for this table, so the manifest may advertise no other",
                command.id,
                command.collection,
                operation.as_str(),
                admitted.iter().copied().collect::<Vec<_>>().join(","),
            )));
        }
        operations.insert(operation.command_operation());
    }
    Ok(operations)
}

fn load_grant_verifier(
    audience: &str,
    environment_names: &[String],
) -> Result<GrantVerifier, ServeError> {
    let mut keys = Vec::with_capacity(environment_names.len());
    for name in environment_names {
        let raw = env::var(name).map_err(|_| ServeError::MissingGrantKey(name.clone()))?;
        let configured: VerificationKeyEnvironment =
            serde_json::from_str(&raw).map_err(|_| ServeError::InvalidGrantKey(name.clone()))?;
        let bytes = URL_SAFE_NO_PAD
            .decode(configured.public_key)
            .map_err(|_| ServeError::InvalidGrantKey(name.clone()))?;
        let public_key: [u8; 32] = bytes
            .try_into()
            .map_err(|_| ServeError::InvalidGrantKey(name.clone()))?;
        keys.push(GrantVerificationKey::new(configured.key_id, public_key)?);
    }
    Ok(GrantVerifier::new(audience, keys)?)
}

/// The columns each collection names, which are all the source publishes of
/// its table.
fn selected_columns(config: &ValidatedServeConfig) -> SelectedColumns {
    config
        .collections
        .iter()
        .map(|collection| {
            (
                QualifiedTable {
                    schema: collection.relation.schema.clone(),
                    table: collection.relation.table.clone(),
                },
                collection
                    .columns
                    .iter()
                    .map(|column| column.name.clone())
                    .collect(),
            )
        })
        .collect()
}

async fn load_or_initialize_state(
    store: &StateStore,
    database_url: &DatabaseUrl,
    config: &ValidatedServeConfig,
) -> Result<LocalState, ServeError> {
    let names = LocalNames {
        source_id: SourceId::new(config.source.id.clone())?,
        publication_name: config.source.publication.clone(),
        slot_name: config.source.slot.clone(),
    };
    if store.state_path().exists() {
        let state = store.read_state()?;
        if state.names != names || state.tables != config.tables {
            return Err(ServeError::PersistedStateMismatch {
                state_dir: store.root().to_path_buf(),
                persisted: describe_enrollment(&state.names, &state.tables),
                configured: describe_enrollment(&names, &config.tables),
                database_url_env: config.source.database_url_env.clone(),
            });
        }
        // Compared before the fence is established, which writes to the
        // database the variable names: a state directory serves only the
        // source it belongs to, and serve writes nothing to any other.
        let identity_database_url = database_url.clone();
        let connected =
            run_blocking(move || inspect_local_identity(&identity_database_url)).await?;
        if connected != state.source_identity {
            return Err(ServeError::SourceIdentityMismatch {
                state_dir: store.root().to_path_buf(),
                database_url_env: config.source.database_url_env.clone(),
                persisted: describe_source_identity(&state.source_identity),
                connected: describe_source_identity(&connected),
            });
        }
        return Ok(state);
    }
    let identity_database_url = database_url.clone();
    let identity = run_blocking(move || inspect_local_identity(&identity_database_url)).await?;
    let state = LocalState::new(
        identity,
        SourceEpoch::new(format!("serve-{}", Uuid::new_v4().simple()))?,
        names,
        config.tables.clone(),
    )?;
    store.write_state(&state)?;
    Ok(state)
}

/// What a state directory is enrolled for, as the persisted-state refusal
/// names it.
fn describe_enrollment(names: &LocalNames, tables: &[QualifiedTable]) -> String {
    format!(
        "source {}, publication {}, slot {}, tables [{}]",
        names.source_id.as_str(),
        names.publication_name,
        names.slot_name,
        tables
            .iter()
            .map(QualifiedTable::display_name)
            .collect::<Vec<_>>()
            .join(", ")
    )
}

/// A configured column's type and nullability, as the schema-drift refusal
/// names them.
fn describe_column(postgres_type: &str, nullable: bool) -> String {
    if nullable {
        format!("{postgres_type}, nullable")
    } else {
        format!("{postgres_type}, not null")
    }
}

/// One configured collection resolved against the enrolled PostgreSQL schema.
struct CollectionPermit {
    table_id: TableId,
    permitted_columns: Vec<ColumnId>,
    partition_column: Option<ColumnId>,
}

/// Resolves every configured collection to stable table and column IDs and
/// rejects any drift from the enrolled schema.
fn compile_collection_permits(
    registry: &SchemaRegistry,
    config: &ValidatedServeConfig,
) -> Result<Vec<CollectionPermit>, ServeError> {
    let mut permits = Vec::with_capacity(config.collections.len());
    for collection in &config.collections {
        let table = registry
            .tables()
            .iter()
            .find(|table| {
                table.inspection.table.schema == collection.relation.schema
                    && table.inspection.table.table == collection.relation.table
            })
            .ok_or_else(|| ServeError::CollectionNotEnrolled {
                collection: collection.id.clone(),
                relation: collection.relation.display_name(),
            })?;
        let mut columns = Vec::with_capacity(collection.columns.len());
        for configured in &collection.columns {
            let index = table
                .inspection
                .columns
                .iter()
                .position(|column| column.name == configured.name)
                .ok_or_else(|| ServeError::ConfiguredColumnMissing {
                    collection: collection.id.clone(),
                    relation: collection.relation.display_name(),
                    column: configured.name.clone(),
                })?;
            let live = &table.inspection.columns[index];
            if live.carried_type_name() != configured.postgres_type
                || live.nullable != configured.nullable
            {
                return Err(ServeError::ConfiguredColumnMismatch {
                    collection: collection.id.clone(),
                    column: configured.name.clone(),
                    configured: describe_column(&configured.postgres_type, configured.nullable),
                    actual: describe_column(live.carried_type_name(), live.nullable),
                });
            }
            columns.push(table.column_ids[index]);
        }
        let mut live_key = table
            .inspection
            .columns
            .iter()
            .filter_map(|column| {
                column
                    .primary_key_ordinal
                    .map(|ordinal| (ordinal, column.name.as_str()))
            })
            .collect::<Vec<_>>();
        live_key.sort_unstable();
        if !live_key
            .iter()
            .map(|(_, name)| *name)
            .eq(collection.primary_key.iter().map(String::as_str))
        {
            return Err(ServeError::ConfiguredPrimaryKeyMismatch {
                collection: collection.id.clone(),
                relation: collection.relation.display_name(),
                configured: collection.primary_key.join(", "),
                actual: live_key
                    .iter()
                    .map(|(_, name)| *name)
                    .collect::<Vec<_>>()
                    .join(", "),
            });
        }
        let partition_column = collection.partition_column.as_ref().and_then(|name| {
            table
                .inspection
                .columns
                .iter()
                .position(|column| &column.name == name)
                .map(|index| table.column_ids[index])
        });
        permits.push(CollectionPermit {
            table_id: table.table_id.clone(),
            permitted_columns: columns,
            partition_column,
        });
    }
    Ok(permits)
}

/// The policy over exactly the configured tables and columns: source-wide,
/// or partitioned by `attribute`. A partitioned table's column must lead the
/// primary key PostgreSQL reports, which the policy checks again.
fn compile_configured_policy(
    registry: &SchemaRegistry,
    generation: u64,
    permits: &[CollectionPermit],
    attribute: Option<&str>,
) -> Result<AuthorizationPolicy, ServeError> {
    let definition = PolicyDefinition {
        generation,
        tables: permits
            .iter()
            .map(|permit| TablePermitDefinition {
                table_id: permit.table_id.clone(),
                permitted_columns: permit.permitted_columns.clone(),
            })
            .collect(),
    };
    let Some(attribute) = attribute else {
        return Ok(AuthorizationPolicy::compile_for_registry(
            definition, registry,
        )?);
    };
    let partition = PartitionDefinition {
        attribute: attribute.to_owned(),
        tables: permits
            .iter()
            .filter_map(|permit| Some((permit.table_id.clone(), permit.partition_column?)))
            .collect(),
    };
    Ok(AuthorizationPolicy::compile_partitioned_for_registry(
        definition, partition, registry,
    )?)
}

fn apply_configured_policy(
    prepared: PreparedLocalSource,
    config: &ValidatedServeConfig,
) -> Result<PreparedLocalSource, ServeError> {
    let permits = compile_collection_permits(&prepared.registry, config)?;
    policy_at(
        &prepared,
        &permits,
        config.partition_attribute.as_deref(),
        config.authorization_generation,
    )
}

/// The configured policy at `generation`, and the scope this process builds
/// for: the whole source, or, when partitioned, the shared tables, each
/// tenant's scope being derived from it per grant.
fn policy_at(
    prepared: &PreparedLocalSource,
    permits: &[CollectionPermit],
    attribute: Option<&str>,
    generation: u64,
) -> Result<PreparedLocalSource, ServeError> {
    let policy = compile_configured_policy(&prepared.registry, generation, permits, attribute)?;
    let scope = policy.scope();
    Ok(PreparedLocalSource {
        registry: prepared.registry.clone(),
        activated: prepared.activated.clone(),
        policy,
        scope,
    })
}

async fn run_blocking<T, E>(
    operation: impl FnOnce() -> Result<T, E> + Send + 'static,
) -> Result<T, ServeError>
where
    T: Send + 'static,
    E: Into<ServeError> + Send + 'static,
{
    crate::lifecycle::run_blocking(move || operation().map_err(Into::into)).await
}

/// The remedy for a manifest that no longer matches its tables.
const REGENERATE_MANIFEST: &str = "Regenerate the manifest with calque inspect and calque init --force, and the client's bindings from it with calque-bindings, or change the table back";

/// What to do about a manifest that is not there: write one, or name the one
/// that exists.
fn missing_config_hint(source: &std::io::Error) -> &'static str {
    if source.kind() == std::io::ErrorKind::NotFound {
        ". Run calque init to write calque.config.json, or pass --config with the path of an existing manifest"
    } else {
        ""
    }
}

#[derive(Debug, Error)]
pub enum ServeError {
    #[error("cannot read serve configuration {}: {source}{}", path.display(), missing_config_hint(source))]
    ReadConfig {
        path: PathBuf,
        source: std::io::Error,
    },
    #[error("serve configuration {} is not valid JSON for a manifest: {source}", path.display())]
    ParseConfig {
        path: PathBuf,
        source: serde_json::Error,
    },
    #[error("serve configuration {} names sources[].connectionEnv, which is now sources[].databaseUrlEnv; rename the field", path.display())]
    RenamedConnectionEnv { path: PathBuf },
    #[error("invalid serve configuration: {0}")]
    InvalidConfig(&'static str),
    #[error("invalid serve configuration: {field} {reason}")]
    InvalidConfigField { field: String, reason: String },
    #[error("invalid serve configuration: collection {collection}: {reason}")]
    InvalidCollection { collection: String, reason: String },
    #[error("invalid serve configuration: relations {first} and {second} would both be served as collection {collection}, which is named by the table alone")]
    SharedCollectionName {
        first: String,
        second: String,
        collection: String,
    },
    #[error("invalid serve configuration: collection {collection} reads {relation}, but a collection's id must be its table's name, because the client binds each collection to the artifact table of that name; rename the collection to {table}, along with every command that names it")]
    CollectionIdIsNotItsTable {
        collection: String,
        relation: String,
        table: String,
    },
    #[error("{0}")]
    InvalidCommands(String),
    #[error("mutation support could not be opened: {0}")]
    Mutation(#[from] LocalMutationError),
    #[error("environment variable {0} is not set or not valid UTF-8; set it to the source's PostgreSQL connection string, or change sources[].databaseUrlEnv in the manifest")]
    MissingDatabaseUrl(String),
    #[error(
        "E_SERVE_OBJECT_STORE_UNKNOWN {OBJECT_STORE_ENV}={0} is not an object store this build implements; it is `filesystem` or `s3`"
    )]
    UnknownObjectStore(String),
    #[error(
        "E_SERVE_OBJECT_STORE_NOT_SELECTED {0} is set but {OBJECT_STORE_ENV} does not select `s3`; refusing to start rather than write artifacts to the filesystem store the configuration did not mean"
    )]
    ObjectStoreNotSelected(String),
    #[error("E_SERVE_OBJECT_STORE_INCOMPLETE {0} is required when {OBJECT_STORE_ENV}=s3")]
    MissingObjectStoreValue(String),
    #[error("E_SERVE_OBJECT_STORE_INVALID {0}={1} is not accepted; {2}")]
    InvalidObjectStoreValue(&'static str, String, &'static str),
    #[error("E_SERVE_OBJECT_STORE_PROBE {0}")]
    ObjectStoreProbeFailed(&'static str),
    #[error("environment variable {0}, which --grant-verification-key-env names, is not set or not valid UTF-8")]
    MissingGrantKey(String),
    #[error("environment variable {0} does not hold a grant verification key: a JSON object {{\"keyId\": ..., \"publicKey\": ...}} whose publicKey is a 32-byte Ed25519 key in unpadded base64url")]
    InvalidGrantKey(String),
    #[error("{state_dir} is enrolled for {persisted}, but the manifest configures {configured}. Calque will not change an existing enrollment in place. Restore the manifest's source and tables, or run {destroy} and then rerun serve to enroll again.", destroy = Destroy::reading(state_dir, database_url_env))]
    PersistedStateMismatch {
        state_dir: PathBuf,
        persisted: String,
        configured: String,
        database_url_env: String,
    },
    #[error("collection {collection} reads {relation}, which this source did not enroll")]
    CollectionNotEnrolled {
        collection: String,
        relation: String,
    },
    #[error("collection {collection}: column {column} is in the manifest, but {relation} in PostgreSQL has no such column. {REGENERATE_MANIFEST}")]
    ConfiguredColumnMissing {
        collection: String,
        relation: String,
        column: String,
    },
    #[error("collection {collection}: column {column} is {configured} in the manifest, but {actual} in PostgreSQL. {REGENERATE_MANIFEST}")]
    ConfiguredColumnMismatch {
        collection: String,
        column: String,
        configured: String,
        actual: String,
    },
    #[error("collection {collection}: the primary key is {configured} in the manifest, but {actual} in PostgreSQL's {relation}. {REGENERATE_MANIFEST}")]
    ConfiguredPrimaryKeyMismatch {
        collection: String,
        relation: String,
        configured: String,
        actual: String,
    },
    /// `remedy` is the health slot's: what gets past the error the CDC loop
    /// stopped on.
    #[error("persistent CDC stopped before the source-wide artifact became ready: {failure}. No incomplete browser state was served. {remedy}", failure = failure.strip_suffix('.').unwrap_or(failure.as_str()), remedy = after_stopped_runtime(Destroy::reading(state_dir, database_url_env), "serve", *remedy))]
    RuntimeUnhealthy {
        state_dir: PathBuf,
        database_url_env: String,
        failure: String,
        remedy: calque_engine::CdcStopRemedy,
    },
    #[error("{startup}\nStopping the source runtime afterwards also failed: {cleanup}")]
    RuntimeCleanup {
        startup: Box<ServeError>,
        cleanup: Box<ServeError>,
    },
    /// A refusal only `calque destroy` gets past, which the library that
    /// raised it leaves this command to name. See `with_state_remedy`.
    #[error("{refusal} {remedy}", remedy = Destroy::reading(state_dir, database_url_env).then_rerun("serve"))]
    StateMustBeDestroyed {
        refusal: Box<ServeError>,
        state_dir: PathBuf,
        database_url_env: String,
    },
    #[error("connected {connected}, but {state_dir} belongs to {persisted}. Calque cannot reuse state from another PostgreSQL source. {remedy}", remedy = reconnect_or_forget(state_dir, database_url_env, "serve"))]
    SourceIdentityMismatch {
        state_dir: PathBuf,
        database_url_env: String,
        persisted: String,
        connected: String,
    },
    /// The engine's refusal of a source that changed after
    /// `load_or_initialize_state` compared identities, with the remedy
    /// `SourceIdentityMismatch` names.
    #[error("{refusal} {remedy}", remedy = reconnect_or_forget(state_dir, database_url_env, "serve"))]
    SourceChanged {
        refusal: Box<ServeError>,
        state_dir: PathBuf,
        database_url_env: String,
    },
    /// The engine's refusal of a publication that no longer has its enrolled
    /// definition, which enrollment and a CDC reconnect raise alike. See
    /// `remedy::restore_publication`.
    #[error("{refusal} {remedy}", remedy = restore_publication(Destroy::reading(state_dir, database_url_env), "serve"))]
    PublicationDrifted {
        refusal: Box<ServeError>,
        state_dir: PathBuf,
        database_url_env: String,
    },
    #[error("a blocking startup task did not complete: {0}")]
    BlockingTask(String),
    #[error(transparent)]
    Json(#[from] serde_json::Error),
    #[error(transparent)]
    Io(#[from] std::io::Error),
    #[error(transparent)]
    Listen(#[from] ListenError),
    #[error("--metrics-bind-address {address} is a wildcard address, so --metrics-expected-host has to name the Host a scraper sends")]
    MetricsExpectedHostRequired { address: SocketAddr },
    #[error("--metrics-expected-host {host}: {source}")]
    MetricsExpectedHost { host: String, source: GuardError },
    #[error("--metrics-bind-address {address}: {source}")]
    MetricsBinding {
        address: SocketAddr,
        source: GuardError,
    },
    #[error("--metrics-bind-address {metrics} shares a port with the HTTP listener on {http}; give the metrics listener a port of its own")]
    MetricsListenerSharesThePort {
        metrics: SocketAddr,
        http: SocketAddr,
    },
    #[error(transparent)]
    Guard(#[from] calque_engine::GuardError),
    #[error(transparent)]
    State(#[from] calque_engine::StateError),
    #[error(transparent)]
    Source(#[from] calque_engine::LocalSourceError),
    #[error(transparent)]
    AuthorizationFence(#[from] calque_engine::AuthorizationFenceError),
    #[error(transparent)]
    Snapshot(#[from] calque_engine::LocalSnapshotError),
    #[error(transparent)]
    Backend(#[from] calque_engine::LocalBackendError),
    #[error(transparent)]
    Artifact(#[from] calque_engine::ArtifactError),
    #[error(transparent)]
    Catalog(#[from] artifact_catalog::CatalogError),
    #[error(transparent)]
    Storage(#[from] blob_store::StorageError),
    #[error(transparent)]
    GrantKey(#[from] dataset_grant::GrantKeyError),
    #[error(transparent)]
    Policy(#[from] authz::PolicyError),
    #[error(transparent)]
    Identifier(#[from] sync_types::IdentifierError),
    #[error(transparent)]
    Tenants(#[from] calque_engine::TenantError),
}

impl CommandError for ServeError {
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
        self.runtime_stopped()
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
    use std::collections::BTreeSet;

    use source_control::{
        ColumnInspection, EnrollmentRequirements, EnrollmentSnapshot, PgoutputLaunchProfile,
        PublicationSettings, SourceIdentity, SourceSettings, TableInspection,
    };

    use super::*;
    use crate::lifecycle::stop_after;

    const SOURCE_WIDE_MANIFEST: &str =
        include_str!("../../../../tests/fixtures/config/serve-sourcewide-smoke.config.json");

    #[test]
    fn listeners_share_a_port_on_one_address_or_under_a_wildcard() {
        let address = |text: &str| text.parse::<SocketAddr>().unwrap();
        for (first, second) in [
            ("127.0.0.1:8787", "127.0.0.1:8787"),
            ("0.0.0.0:8787", "127.0.0.1:8787"),
            ("127.0.0.1:8787", "[::]:8787"),
        ] {
            assert!(
                share_a_port(address(first), address(second)),
                "{first} {second}"
            );
        }
        for (first, second) in [
            ("127.0.0.1:8788", "127.0.0.1:8787"),
            ("127.0.0.2:8787", "127.0.0.1:8787"),
            ("0.0.0.0:8788", "0.0.0.0:8787"),
        ] {
            assert!(
                !share_a_port(address(first), address(second)),
                "{first} {second}"
            );
        }
    }

    fn column(
        attribute_number: i16,
        name: &str,
        type_oid: u32,
        type_name: &str,
        primary_key_ordinal: Option<u32>,
    ) -> ColumnInspection {
        ColumnInspection {
            attribute_number,
            name: name.to_owned(),
            type_oid,
            type_modifier: -1,
            type_schema: "pg_catalog".to_owned(),
            type_name: type_name.to_owned(),
            nullable: false,
            primary_key_ordinal,
            generated: false,
            nondeterministic_collation: false,
            carried_as: None,
        }
    }

    /// One enrolled `public.<name>` with the catalog columns given.
    fn enrolled_table_with(
        name: &str,
        relation_oid: u32,
        columns: Vec<ColumnInspection>,
    ) -> TableInspection {
        TableInspection {
            table: QualifiedTable {
                schema: "public".to_owned(),
                table: name.to_owned(),
            },
            relation_oid,
            in_publication: true,
            relation_kind: 'r',
            persistence: 'p',
            replica_identity: 'f',
            is_partition: false,
            has_publication_row_filter: false,
            has_publication_column_list: false,
            unlisted_columns: Vec::new(),
            columns,
        }
    }

    /// The two tables `serve-sourcewide-smoke.config.json` selects, with the
    /// catalog types its collections declare.
    fn smoke_registry() -> SchemaRegistry {
        smoke_registry_of(vec![
            column(1, "id", 20, "int8", Some(1)),
            column(2, "project_id", 20, "int8", None),
            column(3, "title", 25, "text", None),
            column(4, "done", 16, "bool", None),
        ])
    }

    /// `smoke_registry` with `public.tasks` given the catalog columns named.
    fn smoke_registry_of(tasks: Vec<ColumnInspection>) -> SchemaRegistry {
        registry_of(vec![
            smoke_projects(),
            enrolled_table_with("tasks", 16_391, tasks),
        ])
    }

    fn smoke_projects() -> TableInspection {
        enrolled_table_with(
            "projects",
            16_390,
            vec![
                column(1, "id", 20, "int8", Some(1)),
                column(2, "name", 25, "text", None),
                column(3, "status", 25, "text", None),
            ],
        )
    }

    fn source_wide_config() -> ValidatedServeConfig {
        let manifest: ServeManifest =
            serde_json::from_str(SOURCE_WIDE_MANIFEST).expect("fixture manifest parses");
        validate_manifest(manifest).expect("source-wide validates")
    }

    /// The manifests `compileConfig` must refuse too, from the fixture both
    /// test suites read: `serve` refuses each before PostgreSQL is reached.
    #[test]
    fn every_shared_configuration_refusal_is_refused_here() {
        let fixture: serde_json::Value = serde_json::from_str(include_str!(
            "../../../../tests/fixtures/config/refusals.json"
        ))
        .unwrap();
        let bases = BTreeMap::from([
            ("serve-sourcewide-smoke.config.json", SOURCE_WIDE_MANIFEST),
            ("serve-tenants.config.json", TENANT_MANIFEST),
        ]);
        let base = |name: &str| -> serde_json::Value {
            let file = fixture["bases"][name].as_str().unwrap();
            serde_json::from_str(bases[file]).unwrap()
        };
        let verdict = |manifest: serde_json::Value| -> Result<(), String> {
            let manifest: ServeManifest =
                serde_json::from_value(manifest).map_err(|error| error.to_string())?;
            validate_manifest(manifest)
                .map(|_| ())
                .map_err(|error| error.to_string())
        };
        for name in ["source-wide", "tenants"] {
            verdict(base(name)).unwrap_or_else(|error| panic!("base {name} is refused: {error}"));
        }
        let accepted = fixture["cases"]
            .as_array()
            .unwrap()
            .iter()
            .filter(|case| {
                let mut manifest = base(case["base"].as_str().unwrap());
                apply_json_patch(&mut manifest, case["patch"].as_array().unwrap());
                verdict(manifest).is_ok()
            })
            .map(|case| case["name"].as_str().unwrap())
            .collect::<Vec<_>>();
        assert!(accepted.is_empty(), "serve accepts {accepted:#?}");
        for case in fixture["accepted"].as_array().unwrap() {
            let mut manifest = base(case["base"].as_str().unwrap());
            apply_json_patch(&mut manifest, case["patch"].as_array().unwrap());
            verdict(manifest)
                .unwrap_or_else(|error| panic!("{} is refused: {error}", case["name"]));
        }
    }

    /// Applies the `add`, `replace`, `remove` and `copy` operations of an
    /// RFC 6902 JSON Patch whose paths need no escaping.
    fn apply_json_patch(document: &mut serde_json::Value, patch: &[serde_json::Value]) {
        for operation in patch {
            let op = operation["op"].as_str().unwrap();
            let path = operation["path"].as_str().unwrap();
            let value = match op {
                "add" | "replace" => Some(operation["value"].clone()),
                "copy" => Some(
                    document
                        .pointer(operation["from"].as_str().unwrap())
                        .unwrap_or_else(|| panic!("no value at {}", operation["from"]))
                        .clone(),
                ),
                "remove" => None,
                other => panic!("unsupported JSON Patch operation {other}"),
            };
            let (parent, last) = path.rsplit_once('/').unwrap();
            let target = document
                .pointer_mut(parent)
                .unwrap_or_else(|| panic!("no value at {parent}"));
            match (target, value) {
                (serde_json::Value::Object(map), Some(value)) => {
                    map.insert(last.to_owned(), value);
                }
                (serde_json::Value::Object(map), None) => {
                    map.remove(last)
                        .unwrap_or_else(|| panic!("no value at {path}"));
                }
                (serde_json::Value::Array(items), Some(value)) if last == "-" => items.push(value),
                (serde_json::Value::Array(items), Some(value)) => {
                    let index: usize = last.parse().unwrap();
                    if op == "replace" {
                        items[index] = value;
                    } else {
                        items.insert(index, value);
                    }
                }
                (serde_json::Value::Array(items), None) => {
                    items.remove(last.parse::<usize>().unwrap());
                }
                (other, _) => panic!("{parent} holds {other}, not an object or array"),
            }
        }
    }

    /// A manifest written before `sources[].connectionEnv` became
    /// `databaseUrlEnv` is refused with the field's current name.
    #[test]
    fn the_earlier_source_field_name_is_refused_with_the_current_one() {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("calque.config.json");
        let mut manifest: serde_json::Value = serde_json::from_str(SOURCE_WIDE_MANIFEST).unwrap();
        let source = manifest["sources"][0].as_object_mut().unwrap();
        let name = source.remove("databaseUrlEnv").unwrap();
        source.insert("connectionEnv".to_owned(), name);
        fs::write(&path, serde_json::to_vec(&manifest).unwrap()).unwrap();
        assert_eq!(
            load_and_validate_config(&path).err().unwrap().to_string(),
            format!(
                "serve configuration {} names sources[].connectionEnv, which is now sources[].databaseUrlEnv; rename the field",
                path.display()
            )
        );
    }

    /// A configuration that cannot be read or parsed is named, because the
    /// default `--config` is a relative path the operator may not have meant.
    #[test]
    fn an_unreadable_or_unparsable_configuration_names_its_path() {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("calque.config.json");
        let refusal = || load_and_validate_config(&path).err().unwrap().to_string();

        let missing = refusal();
        assert!(
            missing.starts_with(&format!(
                "cannot read serve configuration {}: ",
                path.display()
            )),
            "{missing}"
        );
        assert!(
            missing.ends_with(
                ". Run calque init to write calque.config.json, or pass --config with the path of an existing manifest"
            ),
            "{missing}"
        );
        fs::write(&path, "{").unwrap();
        let unparsable = refusal();
        assert!(
            unparsable.starts_with(&format!(
                "serve configuration {} is not valid JSON for a manifest: ",
                path.display()
            )),
            "{unparsable}"
        );
    }

    #[test]
    fn a_broken_collection_is_named_with_what_is_wrong() {
        let refusal = |edit: fn(&mut serde_json::Value)| {
            let mut manifest: serde_json::Value =
                serde_json::from_str(SOURCE_WIDE_MANIFEST).unwrap();
            edit(&mut manifest["collections"][1]);
            let manifest: ServeManifest = serde_json::from_value(manifest).unwrap();
            validate_manifest(manifest).err().unwrap().to_string()
        };

        assert_eq!(
            refusal(|tasks| tasks["kind"] = "view".into()),
            "invalid serve configuration: collection tasks: its kind must be \"table\""
        );
        assert_eq!(
            refusal(|tasks| {
                let id = tasks["columns"][0].clone();
                tasks["columns"].as_array_mut().unwrap().push(id);
            }),
            "invalid serve configuration: collection tasks: column id is named twice"
        );
        assert_eq!(
            refusal(|tasks| tasks["primaryKey"] = serde_json::json!(["key"])),
            "invalid serve configuration: collection tasks: primaryKey column key is not one of its columns"
        );
    }

    fn registry_of(tables: Vec<TableInspection>) -> SchemaRegistry {
        let identity = SourceIdentity {
            system_identifier: "7612345678901234567".to_owned(),
            database_name: "app".to_owned(),
            timeline_id: 1,
            previous_timeline_id: 1,
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
                name: "calque_incident".to_owned(),
                exists: true,
                publishes_insert: true,
                publishes_update: true,
                publishes_delete: true,
                publishes_truncate: true,
                publishes_via_partition_root: false,
            },
            tables: tables.clone(),
        };
        let requirements = EnrollmentRequirements {
            expected_identity: identity,
            publication_name: "calque_incident".to_owned(),
            tables: tables
                .iter()
                .map(|table| table.table.clone())
                .collect::<BTreeSet<_>>(),
            pgoutput_profile: PgoutputLaunchProfile::V1,
        };
        SchemaRegistry::initialize(&snapshot, &requirements).expect("fixture registry compiles")
    }

    #[test]
    fn a_partition_scope_does_not_parse() {
        let partition = SOURCE_WIDE_MANIFEST.replacen(
            r#""kind": "source-wide","#,
            r#""kind": "partition", "keyColumns": ["id"], "immutableKeys": true,"#,
            1,
        );
        assert_ne!(partition, SOURCE_WIDE_MANIFEST);
        let error = serde_json::from_str::<ServeManifest>(&partition)
            .expect_err("a partition scope must not parse");
        assert!(
            error.to_string().contains("unknown variant `partition`"),
            "{error}"
        );
    }

    const TENANT_MANIFEST: &str =
        include_str!("../../../../tests/fixtures/config/serve-tenants.config.json");

    /// The three tables `serve-tenants.config.json` selects. Every tenant
    /// table repeats `id` across tenants, so only the leading `tenant_id`
    /// keeps two tenants' rows apart.
    fn tenant_registry_of(tasks: Vec<ColumnInspection>) -> SchemaRegistry {
        registry_of(vec![
            enrolled_table_with(
                "priorities",
                16_400,
                vec![
                    column(1, "id", 20, "int8", Some(1)),
                    column(2, "label", 25, "text", None),
                ],
            ),
            enrolled_table_with(
                "projects",
                16_401,
                vec![
                    column(1, "tenant_id", 20, "int8", Some(1)),
                    column(2, "id", 20, "int8", Some(2)),
                    column(3, "name", 25, "text", None),
                ],
            ),
            enrolled_table_with("tasks", 16_402, tasks),
        ])
    }

    fn tenant_tasks() -> Vec<ColumnInspection> {
        vec![
            column(1, "tenant_id", 20, "int8", Some(1)),
            column(2, "id", 20, "int8", Some(2)),
            column(3, "project_id", 20, "int8", None),
            column(4, "title", 25, "text", None),
            column(5, "done", 16, "bool", None),
        ]
    }

    fn tenant_refusal(edit: impl FnOnce(&mut serde_json::Value)) -> String {
        let mut manifest: serde_json::Value = serde_json::from_str(TENANT_MANIFEST).unwrap();
        edit(&mut manifest);
        let manifest: ServeManifest = serde_json::from_value(manifest).unwrap();
        validate_manifest(manifest).err().unwrap().to_string()
    }

    #[test]
    fn a_partitioned_scope_compiles_to_a_policy_partitioned_by_its_attribute() {
        let manifest: ServeManifest = serde_json::from_str(TENANT_MANIFEST).unwrap();
        let config = validate_manifest(manifest).expect("the tenant fixture validates");
        assert_eq!(config.partition_attribute.as_deref(), Some("tenant_id"));
        let registry = tenant_registry_of(tenant_tasks());
        let permits = compile_collection_permits(&registry, &config).unwrap();
        let partitioned = permits
            .iter()
            .filter(|permit| permit.partition_column.is_some())
            .count();
        assert_eq!(partitioned, 2);
        let policy = compile_configured_policy(&registry, 1, &permits, Some("tenant_id")).unwrap();
        assert_eq!(
            policy.partition(),
            Some(("tenant_id", source_control::PostgresTypeCodec::Int8))
        );
        // The scope this process builds for holds the shared table alone.
        assert_eq!(policy.scope().permitted_tables().len(), 1);
    }

    #[test]
    fn the_manifest_init_writes_for_a_tenant_column_validates() {
        let column = |name: &str, type_name: &str, key: Option<u32>| {
            serde_json::json!({
                "name": name,
                "typeSchema": "pg_catalog",
                "typeName": type_name,
                "nullable": key.is_none(),
                "generated": false,
                "primaryKeyPosition": key,
            })
        };
        let inspection = serde_json::json!({
            "schemaVersion": crate::inspect::INSPECTION_SCHEMA_VERSION,
            "serverMajor": 18,
            "source": "serve-tenants",
            "publication": "calque_serve_tenants",
            "schemaFingerprint": "sha256:0",
            "relations": [
                {
                    "schema": "public",
                    "table": "priorities",
                    "replicaIdentity": "default",
                    "columns": [column("id", "int8", Some(1)), column("label", "text", None)],
                },
                {
                    "schema": "public",
                    "table": "tasks",
                    "replicaIdentity": "default",
                    "columns": [
                        column("tenant_id", "int8", Some(1)),
                        column("id", "int8", Some(2)),
                        column("title", "text", None),
                    ],
                },
            ],
        });
        let options = crate::init::InitOptions {
            application_id: "serve-tenants".to_owned(),
            application_version: "0.1.0".to_owned(),
            database_url_env: "SERVE_TENANTS_DATABASE_URL".to_owned(),
            slot: None,
            scope_id: "tenant".to_owned(),
            tenants: Some(crate::init::Tenants {
                column: "tenant_id".to_owned(),
                attribute: "org".to_owned(),
            }),
        };
        let rendered =
            crate::init::build(&serde_json::from_value(inspection).unwrap(), &options).unwrap();
        let config = validate_manifest(serde_json::from_str(&rendered).unwrap())
            .expect("the manifest init writes validates");
        assert_eq!(config.partition_attribute.as_deref(), Some("org"));
    }

    #[test]
    fn a_partitioned_collection_is_refused_unless_its_tenant_column_leads_its_key() {
        let partitioned =
            |collection: &str| format!("invalid serve configuration: collection {collection}: ");
        assert_eq!(
            tenant_refusal(|manifest| {
                manifest["collections"][2]["primaryKey"] = serde_json::json!(["id", "tenant_id"]);
            }),
            format!(
                "{}it is partitioned by tenant_id, which must be the first column of its primaryKey",
                partitioned("tasks")
            )
        );
        assert_eq!(
            tenant_refusal(|manifest| {
                manifest["collections"][2]["columns"][0]["postgresType"] = "text".into();
            }),
            format!(
                "{}it is partitioned by tenant_id, of type text; a partition column is int2, int4, int8 or uuid",
                partitioned("tasks")
            )
        );
        let neither_or_both = format!(
            "{}its scope is partitioned, so it sets exactly one of partitionColumn, naming the column that holds the tenant, or shared: true",
            partitioned("priorities")
        );
        assert_eq!(
            tenant_refusal(|manifest| {
                manifest["collections"][0]
                    .as_object_mut()
                    .unwrap()
                    .remove("shared");
            }),
            neither_or_both
        );
        assert_eq!(
            tenant_refusal(|manifest| manifest["collections"][0]["partitionColumn"] = "id".into()),
            neither_or_both
        );
        assert_eq!(
            tenant_refusal(|manifest| manifest["collections"][0]["shared"] = false.into()),
            neither_or_both
        );
        assert_eq!(
            tenant_refusal(|manifest| {
                manifest["collections"][2]["columns"][0]["postgresType"] = "int4".into();
            }),
            "invalid serve configuration: the partition columns of a scope must have one type"
        );
        assert_eq!(
            tenant_refusal(|manifest| {
                for index in [1, 2] {
                    let collection = manifest["collections"][index].as_object_mut().unwrap();
                    collection.remove("partitionColumn");
                    collection.insert("shared".to_owned(), true.into());
                }
            }),
            "invalid serve configuration: a partitioned scope needs at least one partitioned collection"
        );
    }

    #[test]
    fn a_source_wide_scope_refuses_a_collection_that_names_a_tenant_column() {
        let mut manifest: serde_json::Value = serde_json::from_str(SOURCE_WIDE_MANIFEST).unwrap();
        manifest["collections"][1]["partitionColumn"] = "id".into();
        let manifest: ServeManifest = serde_json::from_value(manifest).unwrap();
        assert_eq!(
            validate_manifest(manifest).err().unwrap().to_string(),
            "invalid serve configuration: collection tasks: it sets partitionColumn or shared, which only a partitioned scope reads"
        );
    }

    /// The manifest's key is checked against the key PostgreSQL reports, so
    /// a table whose real key does not lead with the tenant column is
    /// refused before any policy is compiled.
    #[test]
    fn a_tenant_table_whose_real_key_does_not_lead_with_the_tenant_is_refused() {
        let manifest: ServeManifest = serde_json::from_str(TENANT_MANIFEST).unwrap();
        let config = validate_manifest(manifest).unwrap();
        let rekeyed = tenant_registry_of(vec![
            column(1, "tenant_id", 20, "int8", Some(2)),
            column(2, "id", 20, "int8", Some(1)),
            column(3, "project_id", 20, "int8", None),
            column(4, "title", 25, "text", None),
            column(5, "done", 16, "bool", None),
        ]);
        let error = compile_collection_permits(&rekeyed, &config)
            .err()
            .expect("a key led by another column is refused")
            .to_string();
        assert_eq!(
            error,
            format!(
                "collection tasks: the primary key is tenant_id, id in the manifest, but id, tenant_id in PostgreSQL's public.tasks. {REGENERATE_MANIFEST}"
            )
        );
    }

    /// A `--grant-verification-key-env` value that repeats a field does not
    /// parse, so a deployment never trusts whichever `publicKey` came last.
    #[test]
    fn a_verification_key_value_that_repeats_a_field_does_not_parse() {
        let key = "11qYAYKxCrfVS_7TyWQHOg7hcvPapiMlrwIaaPcHURo";
        let once = format!(r#"{{"keyId":"a","publicKey":"{key}"}}"#);
        serde_json::from_str::<VerificationKeyEnvironment>(&once).unwrap();
        for repeated in [
            format!(r#"{{"keyId":"a","keyId":"b","publicKey":"{key}"}}"#),
            format!(r#"{{"keyId":"a","publicKey":"{key}","publicKey":"{key}"}}"#),
        ] {
            let error = serde_json::from_str::<VerificationKeyEnvironment>(&repeated)
                .expect_err("a repeated field must not parse");
            assert!(error.to_string().starts_with("duplicate field"), "{error}");
        }
    }

    /// Two schemas' `tasks` would reach the client as one collection id, so the
    /// manifest is refused for the shared name, naming both relations.
    #[test]
    fn one_table_name_in_two_schemas_is_refused() {
        let shared = SOURCE_WIDE_MANIFEST
            .replacen(r#""id": "projects","#, r#""id": "tasks","#, 1)
            .replacen(
                r#""relation": { "schema": "public", "table": "projects" }"#,
                r#""relation": { "schema": "archive", "table": "tasks" }"#,
                1,
            );
        assert_ne!(
            shared, SOURCE_WIDE_MANIFEST,
            "the fixture must be rewritten"
        );
        let manifest: ServeManifest = serde_json::from_str(&shared).expect("manifest parses");
        let rendered = validate_manifest(manifest)
            .err()
            .expect("two relations named tasks are refused")
            .to_string();
        assert_eq!(
            rendered,
            "invalid serve configuration: relations archive.tasks and public.tasks would both be \
             served as collection tasks, which is named by the table alone"
        );
    }

    #[test]
    fn serve_registers_one_crud_command_per_enrolled_table() {
        let registry = smoke_registry();
        let config = source_wide_config();
        let advertised =
            compile_command_manifest(&registry, &config).expect("the fixture manifest matches");

        let expected = registry
            .tables()
            .iter()
            .map(|table| mutation_crud::CrudCommand::name_for(&table.table_id))
            .collect::<Vec<_>>();
        assert_eq!(
            advertised
                .iter()
                .map(|command| command.command_name.clone())
                .collect::<Vec<_>>(),
            expected
        );
        assert_eq!(advertised.len(), registry.tables().len());
        assert_eq!(advertised.len(), config.crud_commands.len());
        assert!(advertised
            .iter()
            .all(|command| command.command_name.starts_with("crud:")));
        // Every advertised operation, and no other, becomes a mounted one.
        for (command, validated) in advertised.iter().zip(&config.crud_commands) {
            assert_eq!(command.collection, validated.collection);
            assert_eq!(
                command.operations,
                validated
                    .operations
                    .iter()
                    .map(|operation| operation.command_operation())
                    .collect::<BTreeSet<_>>()
            );
        }

        // A second registration over the same tables collides only if the
        // first one registered them.
        let mut commands = mutation_api::CommandRegistry::default();
        let everything = PerTableOperations::Uniform(CrudOperations::ALL);
        mutation_crud::register_crud_commands_per_table(
            &mut commands,
            registry.tables(),
            &everything,
        )
        .expect("the enrolled tables register");
        let error = mutation_crud::register_crud_commands_per_table(
            &mut commands,
            registry.tables(),
            &everything,
        )
        .expect_err("the same table cannot register twice");
        assert!(
            matches!(
                error,
                mutation_api::MutationBatchError::DuplicateCommandRegistration
            ),
            "{error}"
        );
    }

    #[test]
    fn serve_refuses_a_manifest_command_for_an_unenrolled_table() {
        let drifted = SOURCE_WIDE_MANIFEST
            .replace(r#""id": "tasks","#, r#""id": "notes","#)
            .replace(
                r#""relation": { "schema": "public", "table": "tasks" },"#,
                r#""relation": { "schema": "public", "table": "notes" },"#,
            )
            .replace(r#""collection": "tasks","#, r#""collection": "notes","#);
        assert_ne!(
            drifted, SOURCE_WIDE_MANIFEST,
            "the fixture must be rewritten"
        );
        let manifest: ServeManifest = serde_json::from_str(&drifted).expect("manifest parses");
        let config = validate_manifest(manifest).expect("the manifest is structurally valid");

        let error = compile_command_manifest(&smoke_registry(), &config)
            .expect_err("a command for an unenrolled table is refused at startup");
        let rendered = error.to_string();
        assert!(
            rendered.starts_with("E_SERVE_COMMAND_TABLE_UNENROLLED"),
            "{rendered}"
        );
        assert!(rendered.contains("command=edit-task"), "{rendered}");
        assert!(rendered.contains("collection=notes"), "{rendered}");
    }

    #[test]
    fn the_source_wide_fixture_mounts_one_command_per_collection() {
        let manifest: ServeManifest =
            serde_json::from_str(SOURCE_WIDE_MANIFEST).expect("fixture manifest parses");
        let config = validate_manifest(manifest).expect("source-wide validates");
        assert_eq!(config.crud_commands.len(), 2);
        let advertised = compile_command_manifest(&smoke_registry(), &config)
            .expect("every collection is advertised");
        assert_eq!(advertised.len(), 2);
        // `project_id` is a plain foreign key: the catalog counts it writable,
        // so the manifest has to name it.
        let tasks = config
            .crud_commands
            .iter()
            .find(|command| command.collection == "tasks")
            .expect("the fixture advertises tasks");
        assert!(
            tasks.writable_columns.contains("project_id"),
            "{:?}",
            tasks.writable_columns
        );
    }

    #[test]
    fn a_source_wide_manifest_mounts_exactly_the_operations_it_advertises() {
        let manifest: ServeManifest =
            serde_json::from_str(SOURCE_WIDE_MANIFEST).expect("fixture manifest parses");
        let config = validate_manifest(manifest).expect("source-wide validates");
        let advertised =
            compile_command_manifest(&smoke_registry(), &config).expect("every table matches");
        let mount = MutationMount::compile(&advertised);

        // `projects` advertises update alone; `tasks` advertises all three.
        assert_eq!(
            mount.commands.contract_value(),
            "projects:update,tasks:insert+update+delete"
        );
    }

    /// `MountedCommands::narrow` gates what a batch may name; this checks that
    /// the registered command itself admits only the advertised operations.
    #[test]
    fn the_advertised_ceiling_narrows_the_registered_command_as_well() {
        let manifest: ServeManifest =
            serde_json::from_str(SOURCE_WIDE_MANIFEST).expect("fixture manifest parses");
        let config = validate_manifest(manifest).expect("source-wide validates");
        let registry = smoke_registry();
        let advertised = compile_command_manifest(&registry, &config).expect("every table matches");
        let registration = MutationMount::compile(&advertised).registration;

        let registered =
            mutation_crud::crud_command_summaries_per_table(registry.tables(), &registration);
        let by_name = registered
            .iter()
            .map(|summary| (summary.name.as_str(), summary.operations.clone()))
            .collect::<BTreeMap<_, _>>();
        for command in &advertised {
            let operations = by_name
                .get(command.command_name.as_str())
                .unwrap_or_else(|| panic!("{} was not registered", command.command_name));
            assert_eq!(
                operations
                    .iter()
                    .map(|operation| operation.as_str())
                    .collect::<BTreeSet<_>>(),
                command
                    .operations
                    .iter()
                    .map(|operation| operation.as_str())
                    .collect::<BTreeSet<_>>(),
                "{} registers a surface the manifest does not advertise",
                command.command_name,
            );
        }
        assert_eq!(
            by_name
                .values()
                .map(|operations| operations.len())
                .collect::<Vec<_>>(),
            vec![1, 3],
            "projects advertises update alone and tasks all three"
        );

        // A table the mount does not name registers no command.
        let nothing = PerTableOperations::ByTable(BTreeMap::new());
        assert!(
            mutation_crud::crud_command_summaries_per_table(registry.tables(), &nothing).is_empty()
        );
    }

    /// Driven directly, because enrollment admits all three operations for
    /// every table, so no `SchemaRegistry` reaches the refusal.
    #[test]
    fn an_operation_the_catalog_does_not_admit_is_refused() {
        let manifest: ServeManifest =
            serde_json::from_str(SOURCE_WIDE_MANIFEST).expect("fixture manifest parses");
        let config = validate_manifest(manifest).expect("source-wide validates");
        let tasks = config
            .crud_commands
            .iter()
            .find(|command| command.collection == "tasks")
            .expect("the fixture advertises tasks");

        assert_eq!(
            check_advertised_operations(
                tasks,
                &[
                    CrudOperationName::Insert,
                    CrudOperationName::Update,
                    CrudOperationName::Delete,
                ],
            )
            .expect("every advertised operation is admitted"),
            BTreeSet::from([
                CommandOperation::Insert,
                CommandOperation::Update,
                CommandOperation::Delete,
            ])
        );

        let rendered = check_advertised_operations(
            tasks,
            &[CrudOperationName::Update, CrudOperationName::Delete],
        )
        .expect_err("insert is advertised and not admitted")
        .to_string();
        assert!(
            rendered.starts_with("E_SERVE_COMMAND_OPERATION_UNADMITTED"),
            "{rendered}"
        );
        assert!(rendered.contains("operation=insert"), "{rendered}");
        assert!(rendered.contains("catalog=delete,update"), "{rendered}");

        // The subset direction is mounted, not refused: `projects` advertises
        // update alone against a catalog that admits all three.
        let projects = config
            .crud_commands
            .iter()
            .find(|command| command.collection == "projects")
            .expect("the fixture advertises projects");
        assert_eq!(
            check_advertised_operations(
                projects,
                &[
                    CrudOperationName::Insert,
                    CrudOperationName::Update,
                    CrudOperationName::Delete,
                ],
            )
            .expect("a narrower manifest is a narrower ceiling"),
            BTreeSet::from([CommandOperation::Update])
        );
    }

    #[test]
    fn a_manifest_that_is_not_the_catalogs_writable_column_set_is_refused() {
        // Narrower than the catalog: the update-only command stops naming
        // `status`, which the catalog still counts writable.
        let narrowed = SOURCE_WIDE_MANIFEST.replace(
            r#""writableColumns": ["name", "status"]"#,
            r#""writableColumns": ["name"]"#,
        );
        assert_ne!(
            narrowed, SOURCE_WIDE_MANIFEST,
            "the fixture must be rewritten"
        );
        let manifest: ServeManifest = serde_json::from_str(&narrowed).expect("manifest parses");
        let config = validate_manifest(manifest).expect("structurally valid");
        let rendered = compile_command_manifest(&smoke_registry(), &config)
            .expect_err("a manifest narrower than the catalog is refused at startup")
            .to_string();
        assert!(
            rendered.starts_with("E_SERVE_COMMAND_COLUMNS_MISMATCH"),
            "{rendered}"
        );
        assert!(rendered.contains("command=edit-project"), "{rendered}");
        assert!(rendered.contains("advertised=name"), "{rendered}");
        assert!(rendered.contains("catalog=name,status"), "{rendered}");

        // Wider than the catalog: the enrolled table has no `done` column.
        let manifest: ServeManifest =
            serde_json::from_str(SOURCE_WIDE_MANIFEST).expect("fixture manifest parses");
        let config = validate_manifest(manifest).expect("structurally valid");
        let registry = smoke_registry_of(vec![
            column(1, "id", 20, "int8", Some(1)),
            column(2, "project_id", 20, "int8", None),
            column(3, "title", 25, "text", None),
        ]);
        let rendered = compile_command_manifest(&registry, &config)
            .expect_err("a manifest wider than the catalog is refused at startup")
            .to_string();
        assert!(
            rendered.starts_with("E_SERVE_COMMAND_COLUMNS_MISMATCH"),
            "{rendered}"
        );
        assert!(
            rendered.contains("advertised=done,project_id,title"),
            "{rendered}"
        );
        assert!(rendered.contains("catalog=project_id,title"), "{rendered}");
    }

    #[test]
    fn an_enrolled_table_no_command_advertises_is_refused_at_startup() {
        let registry = registry_of(vec![
            smoke_projects(),
            enrolled_table_with(
                "tasks",
                16_391,
                vec![
                    column(1, "id", 20, "int8", Some(1)),
                    column(2, "project_id", 20, "int8", None),
                    column(3, "title", 25, "text", None),
                    column(4, "done", 16, "bool", None),
                ],
            ),
            enrolled_table_with(
                "notes",
                16_392,
                vec![
                    column(1, "id", 20, "int8", Some(1)),
                    column(2, "body", 25, "text", None),
                ],
            ),
        ]);
        let error = compile_command_manifest(&registry, &source_wide_config())
            .expect_err("an unadvertised writable table is refused");
        let rendered = error.to_string();
        assert!(
            rendered.starts_with("E_SERVE_COMMAND_TABLE_UNADVERTISED"),
            "{rendered}"
        );
        assert!(rendered.contains("public.notes"), "{rendered}");
        assert!(!rendered.contains("public.tasks"), "{rendered}");
    }

    #[test]
    fn schema_drift_names_the_collection_what_changed_and_both_sides() {
        let config = source_wide_config();
        assert!(compile_collection_permits(&smoke_registry(), &config).is_ok());

        let retyped = smoke_registry_of(vec![
            column(1, "id", 20, "int8", Some(1)),
            column(2, "project_id", 20, "int8", None),
            column(3, "title", 20, "int8", None),
            column(4, "done", 16, "bool", None),
        ]);
        let error = compile_collection_permits(&retyped, &config)
            .err()
            .expect("a retyped column is refused")
            .to_string();
        assert_eq!(
            error,
            format!(
                "collection tasks: column title is text, not null in the manifest, but int8, not null in PostgreSQL. {REGENERATE_MANIFEST}"
            )
        );

        let dropped = smoke_registry_of(vec![
            column(1, "id", 20, "int8", Some(1)),
            column(2, "project_id", 20, "int8", None),
            column(4, "done", 16, "bool", None),
        ]);
        let error = compile_collection_permits(&dropped, &config)
            .err()
            .expect("a dropped column is refused")
            .to_string();
        assert_eq!(
            error,
            format!(
                "collection tasks: column title is in the manifest, but public.tasks in PostgreSQL has no such column. {REGENERATE_MANIFEST}"
            )
        );

        let rekeyed = smoke_registry_of(vec![
            column(1, "id", 20, "int8", Some(2)),
            column(2, "project_id", 20, "int8", Some(1)),
            column(3, "title", 25, "text", None),
            column(4, "done", 16, "bool", None),
        ]);
        let error = compile_collection_permits(&rekeyed, &config)
            .err()
            .expect("a changed primary key is refused")
            .to_string();
        assert_eq!(
            error,
            format!(
                "collection tasks: the primary key is id in the manifest, but project_id, id in PostgreSQL's public.tasks. {REGENERATE_MANIFEST}"
            )
        );
    }

    #[tokio::test]
    async fn state_enrolled_for_other_tables_names_both_and_the_destroy_command() {
        let directory = tempfile::tempdir().unwrap();
        let store = StateStore::open(directory.path().join("state")).unwrap();
        let config = source_wide_config();
        let names = LocalNames {
            source_id: SourceId::new(config.source.id.clone()).unwrap(),
            publication_name: config.source.publication.clone(),
            slot_name: config.source.slot.clone(),
        };
        let identity = SourceIdentity {
            system_identifier: "7612345678901234567".to_owned(),
            database_name: "app".to_owned(),
            timeline_id: 1,
            previous_timeline_id: 1,
        };
        let projects_only = vec![QualifiedTable {
            schema: "public".to_owned(),
            table: "projects".to_owned(),
        }];
        let state = LocalState::new(
            identity,
            SourceEpoch::new("serve-test".to_owned()).unwrap(),
            names,
            projects_only,
        )
        .unwrap();
        store.write_state(&state).unwrap();

        // Refused before PostgreSQL is reached, so nothing listens here.
        let database_url = DatabaseUrl::parse("postgresql://127.0.0.1:9/app").unwrap();
        let error = load_or_initialize_state(&store, &database_url, &config)
            .await
            .expect_err("state for other tables is refused")
            .to_string();
        let state_dir = store.root().display();
        assert_eq!(
            error,
            format!(
                "{state_dir} is enrolled for source serve-smoke, publication calque_serve_smoke, slot calque_serve_smoke_slot, tables [public.projects], \
                 but the manifest configures source serve-smoke, publication calque_serve_smoke, slot calque_serve_smoke_slot, tables [public.projects, public.tasks]. \
                 Calque will not change an existing enrollment in place. Restore the manifest's source and tables, \
                 or run calque destroy --state-dir {state_dir} --database-url-env SERVE_SMOKE_DATABASE_URL --yes and then rerun serve to enroll again."
            )
        );
    }

    /// The stop's failure goes on a line of its own, since a startup message
    /// can end in a full stop.
    #[test]
    fn a_failed_cleanup_keeps_the_startup_failure() {
        let error = ServeError::RuntimeCleanup {
            startup: Box::new(ServeError::RuntimeUnhealthy {
                state_dir: PathBuf::from("/srv/calque/source-a"),
                database_url_env: "CALQUE_DATABASE_URL".to_owned(),
                failure: "persistent CDC stopped".to_owned(),
                remedy: calque_engine::CdcStopRemedy::Rerun,
            }),
            cleanup: Box::new(ServeError::BlockingTask("task 7 panicked".to_owned())),
        };
        assert_eq!(
            error.to_string(),
            "persistent CDC stopped before the source-wide artifact became ready: persistent CDC stopped. No incomplete browser state was served. Rerun serve; if it stops again, run calque destroy --state-dir /srv/calque/source-a --database-url-env CALQUE_DATABASE_URL --yes and then rerun serve to rebuild from a complete snapshot.\nStopping the source runtime afterwards also failed: a blocking startup task did not complete: task 7 panicked"
        );
    }

    /// The startup failure of a runtime whose CDC loop stopped before
    /// readiness, carrying the health text serve read.
    fn stopped_runtime(failure: &str) -> ServeError {
        ServeError::RuntimeUnhealthy {
            state_dir: PathBuf::from("/srv/calque/source-a"),
            database_url_env: "CALQUE_DATABASE_URL".to_owned(),
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
            "persistent CDC stopped before the source-wide artifact became ready: persistent CDC failed: source identity contains an invalid timeline. No incomplete browser state was served. Rerun serve; if it stops again, run calque destroy --state-dir /srv/calque/source-a --database-url-env CALQUE_DATABASE_URL --yes and then rerun serve to rebuild from a complete snapshot."
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
            "persistent CDC stopped before the source-wide artifact became ready: persistent CDC failed: source identity contains an invalid timeline. No incomplete browser state was served. Rerun serve; if it stops again, run calque destroy --state-dir /srv/calque/source-a --database-url-env CALQUE_DATABASE_URL --yes and then rerun serve to rebuild from a complete snapshot.\nStopping the source runtime afterwards also failed: persistent CDC session cleanup failed"
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
            "persistent CDC stopped before the source-wide artifact became ready: cdc-runtime-panic; cdc-runtime-cleanup. No incomplete browser state was served. Rerun serve; if it stops again, run calque destroy --state-dir /srv/calque/source-a --database-url-env CALQUE_DATABASE_URL --yes and then rerun serve to rebuild from a complete snapshot.\nStopping the source runtime afterwards also failed: persistent CDC session cleanup failed"
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
            "persistent CDC stopped before the source-wide artifact became ready: persistent CDC failed: source identity contains an invalid timeline. No incomplete browser state was served. Rerun serve; if it stops again, run calque destroy --state-dir /srv/calque/source-a --database-url-env CALQUE_DATABASE_URL --yes and then rerun serve to rebuild from a complete snapshot.\nStopping the source runtime afterwards also failed: persistent CDC thread panicked after its loop stopped"
        );
    }

    /// A runtime still running when startup failed has reported nothing, so
    /// any error its stop returns follows the startup failure.
    #[tokio::test]
    async fn a_running_runtime_that_fails_to_stop_reports_both() {
        let error = stop_after(
            || Err(calque_engine::LocalSourceError::InvalidIdentity),
            ServeError::BlockingTask("task 7 panicked".to_owned()),
        )
        .await;
        assert_eq!(
            error.to_string(),
            "a blocking startup task did not complete: task 7 panicked\nStopping the source runtime afterwards also failed: source identity contains an invalid timeline"
        );
    }

    const EARLIER_LOG: &str = "/srv/calque/source-a serves a canonical log an earlier release of calque wrote, and calque does not migrate it.";
    const FOREIGN_BUILD: &str = "/srv/calque/source-a holds an interrupted build of local-artifact-earlier-release, an artifact this build of calque does not make, and calque does not migrate it.";
    const DESTROY_THEN_SERVE: &str = "Run calque destroy --state-dir /srv/calque/source-a --database-url-env CALQUE_DATABASE_URL --yes, then rerun serve to prepare the working set anew.";
    const RESTORE_THEN_SERVE: &str = "Restore the publication's definition if it was altered, then run calque destroy --state-dir /srv/calque/source-a --database-url-env CALQUE_DATABASE_URL --yes and rerun serve to prepare the working set anew, since the one it holds may no longer match PostgreSQL.";

    /// The refusal of a canonical log an earlier release wrote, as the CDC
    /// loop stops on it and the stop that joins the loop returns it.
    fn earlier_log() -> calque_engine::LocalSourceError {
        calque_engine::LocalSourceError::Canonical(canonical_log::LogError::EarlierEntryFormat {
            state_dir: PathBuf::from("/srv/calque/source-a"),
        })
    }

    fn foreign_build() -> ServeError {
        ServeError::Snapshot(calque_engine::LocalSnapshotError::ForeignArtifactBuild {
            state_dir: PathBuf::from("/srv/calque/source-a"),
            artifact: "local-artifact-earlier-release".to_owned(),
        })
    }

    const DRIFT: &str = "the PostgreSQL publication calque_incident was altered, or an enum its columns use gained or renamed a value: Calque needs it to publish exactly the enrolled tables, for insert, update, delete and truncate, without publish_via_partition_root or row filters, with the column lists and enum values it had at enrollment.";

    fn drift() -> calque_engine::LocalSourceError {
        calque_engine::LocalSourceError::PublicationDrift {
            publication: "calque_incident".to_owned(),
        }
    }

    /// The message `run` returns for `error`.
    fn printed(error: ServeError) -> String {
        with_state_remedy(
            error,
            Path::new("/srv/calque/source-a"),
            "CALQUE_DATABASE_URL",
        )
        .to_string()
    }

    /// A refusal only `calque destroy` gets past ends with that command and
    /// the variable the manifest's source names, whichever library raised it.
    #[test]
    fn a_refusal_only_destroy_gets_past_names_it() {
        let catalog = ServeError::Catalog(artifact_catalog::CatalogError::EarlierSchemaVersion {
            state_dir: PathBuf::from("/srv/calque/source-a"),
            version: 2,
        });
        assert_eq!(
            printed(catalog),
            format!("/srv/calque/source-a holds an artifact catalog an earlier release of calque wrote (schema version 2), and calque does not migrate it. {DESTROY_THEN_SERVE}")
        );
        assert_eq!(
            printed(foreign_build()),
            format!("{FOREIGN_BUILD} {DESTROY_THEN_SERVE}")
        );
        assert_eq!(
            printed(ServeError::Source(earlier_log())),
            format!("{EARLIER_LOG} {DESTROY_THEN_SERVE}")
        );
        assert_eq!(
            printed(ServeError::Source(
                calque_engine::LocalSourceError::SessionCleanupFailedAfter(Box::new(earlier_log()))
            )),
            format!("persistent CDC session cleanup failed after persistent CDC failed: {EARLIER_LOG} {DESTROY_THEN_SERVE}")
        );
        assert_eq!(
            printed(ServeError::Source(
                calque_engine::LocalSourceError::RegistryStateMismatch
            )),
            format!("persisted schema registry does not match local state. {DESTROY_THEN_SERVE}")
        );
        assert_eq!(
            printed(ServeError::Source(
                calque_engine::LocalSourceError::EnrolledByAnotherState
            )),
            format!("this source was enrolled by another state directory at the same path. {DESTROY_THEN_SERVE}")
        );
    }

    /// A changed source names reconnecting it or forgetting the state; a
    /// drifted publication names restoring it, whether enrollment or a running
    /// CDC loop refused it.
    #[test]
    fn a_refusal_destroy_does_not_answer_names_its_own_remedy() {
        assert_eq!(
            printed(ServeError::Source(
                calque_engine::LocalSourceError::SourceIdentityChanged
            )),
            "connected PostgreSQL identity changed from persisted local state. Point CALQUE_DATABASE_URL at the persisted source and rerun serve, or run calque destroy --state-dir /srv/calque/source-a --yes --local-state-only and then rerun serve; PostgreSQL objects on the original source will remain."
        );
        assert_eq!(
            printed(ServeError::Source(
                calque_engine::LocalSourceError::PublicationDrift {
                    publication: "calque_incident".to_owned(),
                }
            )),
            "the PostgreSQL publication calque_incident was altered, or an enum its columns use gained or renamed a value: Calque needs it to publish exactly the enrolled tables, for insert, update, delete and truncate, without publish_via_partition_root or row filters, with the column lists and enum values it had at enrollment. Restore the publication's definition if it was altered, then run calque destroy --state-dir /srv/calque/source-a --database-url-env CALQUE_DATABASE_URL --yes and rerun serve to prepare the working set anew, since the one it holds may no longer match PostgreSQL."
        );
        assert_eq!(
            printed(ServeError::Source(
                calque_engine::LocalSourceError::SessionCleanupFailedAfter(Box::new(drift()))
            )),
            format!("persistent CDC session cleanup failed after persistent CDC failed: {DRIFT} {RESTORE_THEN_SERVE}")
        );
    }

    #[test]
    fn a_failure_destroy_does_not_answer_is_printed_as_raised() {
        for error in [
            ServeError::Source(calque_engine::LocalSourceError::SessionCleanupFailed),
            ServeError::Snapshot(calque_engine::LocalSnapshotError::Connection),
            ServeError::Catalog(artifact_catalog::CatalogError::UnsupportedSchemaVersion(99)),
            ServeError::BlockingTask("task 7 panicked".to_owned()),
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
        let stopped = |failure: String| ServeError::RuntimeUnhealthy {
            state_dir: PathBuf::from("/srv/calque/source-a"),
            database_url_env: "CALQUE_DATABASE_URL".to_owned(),
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
            format!("persistent CDC stopped before the source-wide artifact became ready: persistent CDC failed: {EARLIER_LOG} No incomplete browser state was served. {DESTROY_THEN_SERVE}")
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
            format!("persistent CDC stopped before the source-wide artifact became ready: persistent CDC failed: {cause}; cdc-runtime-cleanup. No incomplete browser state was served. {DESTROY_THEN_SERVE}\nStopping the source runtime afterwards also failed: persistent CDC session cleanup failed")
        );
        assert_eq!(cleanup_failed.matches("calque destroy").count(), 1);
    }

    /// A lost snapshot connection stands in for the barrier timeout a stopped
    /// CDC loop causes, which `calque_engine` does not export.
    #[tokio::test]
    async fn a_refusal_the_stop_returns_names_destroy_after_its_line() {
        let error = stop_after(
            || Err(earlier_log()),
            ServeError::Snapshot(calque_engine::LocalSnapshotError::Connection),
        )
        .await;
        assert_eq!(
            printed(error),
            format!("local snapshot PostgreSQL connection failed\nStopping the source runtime afterwards also failed: {EARLIER_LOG} {DESTROY_THEN_SERVE}")
        );

        // A session cleanup that failed too keeps the refusal beside it.
        let error = stop_after(
            || {
                Err(calque_engine::LocalSourceError::SessionCleanupFailedAfter(
                    Box::new(earlier_log()),
                ))
            },
            ServeError::Snapshot(calque_engine::LocalSnapshotError::Connection),
        )
        .await;
        assert_eq!(
            printed(error),
            format!("local snapshot PostgreSQL connection failed\nStopping the source runtime afterwards also failed: persistent CDC session cleanup failed after persistent CDC failed: {EARLIER_LOG} {DESTROY_THEN_SERVE}")
        );
    }

    /// The remedy is restoring the publication, which destroy refuses to drop
    /// otherwise, then destroy, since the working set may miss changes.
    #[tokio::test]
    async fn a_runtime_stopped_on_a_drifted_publication_names_restoring_it_once() {
        let error = stop_after(
            || Err(drift()),
            ServeError::RuntimeUnhealthy {
                state_dir: PathBuf::from("/srv/calque/source-a"),
                database_url_env: "CALQUE_DATABASE_URL".to_owned(),
                failure: format!("persistent CDC failed: {DRIFT}"),
                remedy: calque_engine::CdcStopRemedy::RestorePublication,
            },
        )
        .await;
        let printed = printed(error);
        assert_eq!(
            printed,
            format!("persistent CDC stopped before the source-wide artifact became ready: persistent CDC failed: {DRIFT} No incomplete browser state was served. {RESTORE_THEN_SERVE}")
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
            ServeError::Snapshot(calque_engine::LocalSnapshotError::Connection),
        )
        .await;
        assert_eq!(
            printed(error),
            format!("local snapshot PostgreSQL connection failed\nStopping the source runtime afterwards also failed: {DRIFT} {RESTORE_THEN_SERVE}")
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
            format!("{FOREIGN_BUILD} {DESTROY_THEN_SERVE}\nStopping the source runtime afterwards also failed: persistent CDC session cleanup failed")
        );

        let error = stop_after(|| Err(earlier_log()), foreign_build()).await;
        assert_eq!(
            printed(error),
            format!("{FOREIGN_BUILD} {DESTROY_THEN_SERVE}\nStopping the source runtime afterwards also failed: {EARLIER_LOG}")
        );
    }

    /// A stopped runtime's startup failure names `calque destroy` in its own
    /// remedy, so a refusal beside it brings no second one. `stop_after`
    /// drops the join's refusal, so the pair is built here directly.
    #[test]
    fn a_startup_failure_that_names_destroy_keeps_its_own_remedy() {
        let startup =
            stopped_runtime("persistent CDC failed: source identity contains an invalid timeline");
        let raised =
            format!("{startup}\nStopping the source runtime afterwards also failed: {EARLIER_LOG}");
        let error = printed(ServeError::RuntimeCleanup {
            startup: Box::new(startup),
            cleanup: Box::new(ServeError::Source(earlier_log())),
        });
        assert_eq!(error, raised);
        assert_eq!(error.matches("calque destroy").count(), 1);
    }

    #[test]
    fn the_command_manifest_is_typed_rather_than_an_untyped_object_list() {
        for (from, to, expected) in [
            (
                r#""collection": "tasks","#,
                r#""collection": "absent","#,
                "names no configured collection",
            ),
            (
                r#""writableColumns": ["project_id", "title", "done"]"#,
                r#""writableColumns": ["id"]"#,
                "is an identity column",
            ),
            (
                r#""operations": ["insert", "update", "delete"]"#,
                r#""operations": ["update", "update"]"#,
                "without repeats",
            ),
            (
                r#""operations": ["update"]"#,
                r#""operations": []"#,
                "without repeats",
            ),
            (
                r#""id": "edit-task","#,
                r#""id": "edit-project","#,
                "is named twice",
            ),
        ] {
            let drifted = SOURCE_WIDE_MANIFEST.replace(from, to);
            assert_ne!(
                drifted, SOURCE_WIDE_MANIFEST,
                "the fixture must be rewritten: {from}"
            );
            let manifest: ServeManifest = serde_json::from_str(&drifted).expect("manifest parses");
            let rendered = validate_manifest(manifest)
                .err()
                .expect("the drifted command manifest is refused")
                .to_string();
            assert!(
                rendered.starts_with("E_SERVE_COMMAND_INVALID"),
                "{from}: {rendered}"
            );
            assert!(rendered.contains(expected), "{from}: {rendered}");
        }
    }

    #[test]
    fn a_collection_id_other_than_its_table_name_is_refused() {
        let renamed = SOURCE_WIDE_MANIFEST.replace(r#""id": "tasks","#, r#""id": "todo","#);
        assert_ne!(renamed, SOURCE_WIDE_MANIFEST);
        let manifest: ServeManifest = serde_json::from_str(&renamed).expect("manifest parses");
        let rendered = validate_manifest(manifest)
            .err()
            .expect("a collection named apart from its table is refused")
            .to_string();
        assert_eq!(
            rendered,
            "invalid serve configuration: collection todo reads public.tasks, but a collection's id must be its table's name, because the client binds each collection to the artifact table of that name; rename the collection to tasks, along with every command that names it"
        );
    }

    #[test]
    fn a_backend_command_is_refused() {
        let with_backend = SOURCE_WIDE_MANIFEST.replace(
            r#""commands": ["#,
            r#""commands": [
    {
      "id": "archive-tasks",
      "kind": "backend",
      "executor": "archive",
      "source": "serve-smoke",
      "scope": "all",
      "collections": ["tasks"],
      "inputVersion": 1
    },"#,
        );
        assert_ne!(with_backend, SOURCE_WIDE_MANIFEST);
        let manifest: ServeManifest =
            serde_json::from_str(&with_backend).expect("a backend command parses");
        let rendered = validate_manifest(manifest)
            .err()
            .expect("a backend command is refused")
            .to_string();
        assert_eq!(
            rendered,
            "E_SERVE_COMMAND_INVALID command=archive-tasks backend commands are not supported; declare a crud command"
        );
    }

    /// With backend commands refused, the only manifest that advertises no
    /// crud command is one with no command at all, and it is refused for that.
    #[test]
    fn a_manifest_with_no_command_is_refused() {
        let mut manifest: ServeManifest =
            serde_json::from_str(SOURCE_WIDE_MANIFEST).expect("fixture manifest parses");
        manifest.commands.clear();
        let rendered = validate_manifest(manifest)
            .err()
            .expect("a manifest with no command is refused")
            .to_string();
        assert_eq!(
            rendered,
            "E_SERVE_COMMAND_INVALID the manifest advertises no crud command, so the mutation route would mount a writable surface nothing declares"
        );
    }

    #[test]
    fn an_unknown_command_kind_or_operation_does_not_parse() {
        for drifted in [
            SOURCE_WIDE_MANIFEST.replace(r#""kind": "crud","#, r#""kind": "sql","#),
            SOURCE_WIDE_MANIFEST
                .replace(r#""operations": ["update"]"#, r#""operations": ["upsert"]"#),
        ] {
            assert_ne!(
                drifted, SOURCE_WIDE_MANIFEST,
                "the fixture must be rewritten"
            );
            assert!(
                serde_json::from_str::<ServeManifest>(&drifted).is_err(),
                "{drifted}"
            );
        }
    }

    #[test]
    fn the_object_store_selector_defaults_to_the_filesystem_and_names_two_stores() {
        let selected = |value: Option<&str>| {
            let value = value.map(str::to_owned);
            selected_object_store_kind(move |name| {
                (name == OBJECT_STORE_ENV).then(|| value.clone()).flatten()
            })
        };
        assert_eq!(selected(None).unwrap(), ObjectStoreKind::Filesystem);
        assert_eq!(
            selected(Some("filesystem")).unwrap(),
            ObjectStoreKind::Filesystem
        );
        assert_eq!(selected(Some("s3")).unwrap(), ObjectStoreKind::S3);
        assert_eq!(ObjectStoreKind::Filesystem.as_str(), "filesystem");
        assert_eq!(ObjectStoreKind::S3.as_str(), "s3");
        // Not a name, not a near miss, and not the empty string a compose file
        // interpolates when the variable it reads is unset.
        for refused in ["", "S3", "minio", "local", "none", "filesystem "] {
            let rendered = selected(Some(refused))
                .err()
                .unwrap_or_else(|| panic!("{refused:?} must be refused"))
                .to_string();
            assert!(
                rendered.starts_with("E_SERVE_OBJECT_STORE_UNKNOWN"),
                "{refused:?}: {rendered}"
            );
        }
    }

    #[test]
    fn s3_configuration_without_the_s3_selector_refuses_to_start() {
        for name in S3_ENVIRONMENT {
            for selector in [None, Some("filesystem")] {
                let rendered = selected_object_store_kind(|requested| {
                    if requested == OBJECT_STORE_ENV {
                        selector.map(str::to_owned)
                    } else if requested == *name {
                        Some("set-by-the-operator".to_owned())
                    } else {
                        None
                    }
                })
                .err()
                .unwrap_or_else(|| panic!("{name} set without the selector must be refused"))
                .to_string();
                assert!(
                    rendered.starts_with("E_SERVE_OBJECT_STORE_NOT_SELECTED"),
                    "{name}: {rendered}"
                );
                assert!(rendered.contains(name), "{name}: {rendered}");
            }
            // With `s3` selected, the same variable is accepted.
            assert_eq!(
                selected_object_store_kind(|requested| {
                    if requested == OBJECT_STORE_ENV {
                        Some("s3".to_owned())
                    } else if requested == *name {
                        Some("set-by-the-operator".to_owned())
                    } else {
                        None
                    }
                })
                .unwrap(),
                ObjectStoreKind::S3
            );
        }
    }

    /// Each double fails one step of put/head/get/delete and answers the others
    /// correctly, so a probe that skipped a step would pass one of them.
    #[tokio::test]
    async fn the_startup_probe_refuses_a_store_that_does_not_round_trip() {
        let directory = tempfile::tempdir().unwrap();
        let backing = || {
            Arc::new(
                LocalFilesystemBlobStore::open(
                    directory.path().join(Uuid::new_v4().simple().to_string()),
                    MAX_LOCAL_ARTIFACT_BYTES,
                )
                .unwrap(),
            )
        };

        let healthy: Arc<dyn ManagedBlobStore> = backing();
        probe_object_store(&healthy).await.unwrap();

        for (fault, expected) in [
            (
                ProbeFault::SwallowPut,
                "absent or its metadata did not match",
            ),
            (
                ProbeFault::EmptyHead,
                "absent or its metadata did not match",
            ),
            (ProbeFault::MissingGet, "could not be read back"),
            (ProbeFault::CorruptGet, "read back with different bytes"),
        ] {
            let store: Arc<dyn ManagedBlobStore> = Arc::new(FaultyStore {
                inner: backing(),
                fault,
            });
            let rendered = probe_object_store(&store)
                .await
                .err()
                .unwrap_or_else(|| panic!("{fault:?} must refuse startup"))
                .to_string();
            assert!(
                rendered.starts_with("E_SERVE_OBJECT_STORE_PROBE"),
                "{fault:?}: {rendered}"
            );
            assert!(rendered.contains(expected), "{fault:?}: {rendered}");
        }

        // A store that cannot delete is misconfigured too: the probe would
        // otherwise leave one object behind on every start.
        let store: Arc<dyn ManagedBlobStore> = Arc::new(FaultyStore {
            inner: backing(),
            fault: ProbeFault::RefuseDelete,
        });
        assert!(probe_object_store(&store).await.is_err());
    }

    #[derive(Debug, Clone, Copy)]
    enum ProbeFault {
        /// Accepts the write and stores nothing.
        SwallowPut,
        /// Stores the bytes and reports the object absent.
        EmptyHead,
        /// Reports the object present and refuses to stream it.
        MissingGet,
        /// Streams bytes that are not the ones that were written.
        CorruptGet,
        RefuseDelete,
    }

    struct FaultyStore {
        inner: Arc<LocalFilesystemBlobStore>,
        fault: ProbeFault,
    }

    impl ManagedBlobStore for FaultyStore {
        fn cleanup_staging(&self) -> blob_store::Result<usize> {
            self.inner.cleanup_staging()
        }
    }

    #[async_trait::async_trait]
    impl blob_store::BlobStore for FaultyStore {
        async fn put_if_absent(
            &self,
            object: &ObjectId,
            body: blob_store::ByteStream,
            metadata: &ObjectMetadata,
        ) -> blob_store::Result<blob_store::PutOutcome> {
            if matches!(self.fault, ProbeFault::SwallowPut) {
                return Ok(blob_store::PutOutcome::Created);
            }
            self.inner.put_if_absent(object, body, metadata).await
        }

        async fn head(&self, object: &ObjectId) -> blob_store::Result<Option<ObjectMetadata>> {
            match self.fault {
                ProbeFault::EmptyHead => Ok(None),
                ProbeFault::MissingGet | ProbeFault::CorruptGet => {
                    Ok(Some(ObjectMetadata::for_bytes(OBJECT_STORE_PROBE_BODY)))
                }
                _ => self.inner.head(object).await,
            }
        }

        async fn get(
            &self,
            object: &ObjectId,
        ) -> blob_store::Result<Option<blob_store::ByteStream>> {
            match self.fault {
                ProbeFault::MissingGet => Ok(None),
                ProbeFault::CorruptGet => Ok(Some(Box::pin(std::io::Cursor::new(
                    b"not the probe body".to_vec(),
                )))),
                _ => self.inner.get(object).await,
            }
        }

        async fn delete(&self, object: &ObjectId) -> blob_store::Result<()> {
            if matches!(self.fault, ProbeFault::RefuseDelete) {
                return Err(blob_store::StorageError::InvalidDigest);
            }
            self.inner.delete(object).await
        }
    }
}
