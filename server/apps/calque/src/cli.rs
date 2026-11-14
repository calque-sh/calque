//! Command-line surface of the `calque` binary.

use std::{
    collections::BTreeSet,
    error::Error,
    fs,
    io::{self, Write as _},
    net::{IpAddr, SocketAddr},
    path::PathBuf,
};

use calque_engine::AllowedOrigin;
use clap::{Args, Parser, Subcommand};
use source_control::QualifiedTable;

use crate::{dev, init, inspect, recovery, serve};

#[derive(Debug, Parser)]
#[command(
    name = "calque",
    about = "Serve complete PostgreSQL collections to browsers, with realtime changes",
    version,
    after_long_help = "DATABASE CONNECTION:\n    Connection strings are read from an environment variable, never from a command-line argument, so credentials do not enter process listings or shell history. serve reads the variable its manifest names in sources[].databaseUrlEnv; every other command that connects reads the one named by --database-url-env, CALQUE_DATABASE_URL by default."
)]
pub struct Cli {
    #[command(subcommand)]
    pub command: Command,
}

#[derive(Debug, Subcommand)]
pub enum Command {
    /// Check that a PostgreSQL 18 publication's tables can be served, and print
    /// their schema as JSON for `init`.
    Inspect(InspectCommand),
    /// Generate a configuration manifest from an inspection document.
    Init(InitCommand),
    /// Serve the tables a manifest from `init` names, to browsers that present
    /// signed dataset grants.
    Serve(ServeCommand),
    /// Enroll a disposable PostgreSQL database and serve it on loopback with a
    /// development bearer instead of signed grants.
    Dev(DevCommand),
    /// Print a state directory's source, epoch and tables without exposing secrets.
    Status(StatusCommand),
    /// Destroy a state directory, its replication slot and the publication it names.
    Destroy(DestroyCommand),
    /// Back up a stopped server's state directory, artifact catalog, artifacts
    /// and control rows into a recovery unit that records their SHA-256
    /// digests.
    Backup(BackupCommand),
    /// Restore a recovery unit into an absent server state directory.
    Restore(RestoreCommand),
    /// Check a restored server's state directory, artifact catalog, artifacts
    /// and control rows against the digests its recovery unit recorded.
    VerifyRestore(VerifyRestoreCommand),
}

/// Where the served source's connection string is read from.
///
/// `serve` takes it from the manifest's `sources[].databaseUrlEnv`; every other
/// command that connects runs without a manifest, so it names the variable
/// directly. It is never a command-line argument, so credentials do not
/// enter process listings or shell history.
#[derive(Debug, Args)]
pub struct DatabaseEnvironment {
    /// Name of the environment variable holding the connection string.
    #[arg(long, value_name = "NAME", default_value = "CALQUE_DATABASE_URL")]
    pub database_url_env: String,
}

#[derive(Debug, Args)]
pub struct BackupCommand {
    /// Private durable state directory of a stopped server.
    #[arg(long, value_name = "PATH", default_value = ".calque-serve")]
    pub state_dir: PathBuf,
    /// New directory that will contain the complete recovery unit.
    #[arg(long, value_name = "PATH")]
    pub output: PathBuf,
    #[command(flatten)]
    pub database: DatabaseEnvironment,
}

#[derive(Debug, Args)]
pub struct RestoreCommand {
    /// Complete recovery-unit directory created by `backup`.
    #[arg(long, value_name = "PATH")]
    pub backup: PathBuf,
    /// Absent directory into which server state will be restored.
    #[arg(long, value_name = "PATH", default_value = ".calque-serve")]
    pub state_dir: PathBuf,
    /// Replace the same source's existing calque_control rows with the unit's
    /// rows instead of refusing them. The source must be stopped (lease
    /// unowned or expired, logical slot inactive) and the slot must not be
    /// ahead of the unit; those gates are not relaxed.
    #[arg(long)]
    pub replace_source_rows: bool,
    #[command(flatten)]
    pub database: DatabaseEnvironment,
}

#[derive(Debug, Args)]
pub struct VerifyRestoreCommand {
    /// Complete recovery-unit directory whose oracle is authoritative.
    #[arg(long, value_name = "PATH")]
    pub backup: PathBuf,
    /// Server state directory `restore` wrote from that unit.
    #[arg(long, value_name = "PATH", default_value = ".calque-serve")]
    pub state_dir: PathBuf,
    #[command(flatten)]
    pub database: DatabaseEnvironment,
}

#[derive(Debug, Args)]
pub struct ServeCommand {
    /// Static Calque configuration manifest.
    #[arg(long, value_name = "PATH", default_value = "calque.config.json")]
    pub config: PathBuf,
    /// Private durable state directory.
    #[arg(long, value_name = "PATH", default_value = ".calque-serve")]
    pub state_dir: PathBuf,
    /// Audience required in every signed dataset grant.
    #[arg(long, value_name = "AUDIENCE")]
    pub grant_audience: String,
    /// Environment variable containing a JSON verification-key object. Repeat for rotation.
    #[arg(long, value_name = "NAME", required = true)]
    pub grant_verification_key_env: Vec<String>,
    /// Browser origin permitted to call this server: an HTTPS origin, or a
    /// loopback one. Repeat when needed.
    #[arg(long = "allow-origin", value_name = "ORIGIN", required = true)]
    pub allowed_origins: Vec<String>,
    /// HTTP port.
    #[arg(long, default_value_t = 8787)]
    pub port: u16,
    /// Address the HTTP listener binds. A container in a composition is not
    /// reachable on the default loopback address; `0.0.0.0` then requires
    /// --expected-host, because the Host check is exact and never widened.
    #[arg(long, value_name = "ADDRESS", default_value = "127.0.0.1")]
    pub bind_address: IpAddr,
    /// The exact `Host` header this process answers, as `host` or
    /// `host:port`. Defaults to the bind address and port.
    #[arg(long, value_name = "HOST")]
    pub expected_host: Option<String>,
    /// Address and port of a second listener that serves `/metrics` and
    /// nothing else. Without it no listener serves metrics: the HTTP listener
    /// has no `/metrics` route. A wildcard address requires
    /// --metrics-expected-host.
    #[arg(long, value_name = "ADDRESS:PORT")]
    pub metrics_bind_address: Option<SocketAddr>,
    /// The exact `Host` header the metrics listener answers, as `host` or
    /// `host:port`. Defaults to its address and port.
    #[arg(long, value_name = "HOST", requires = "metrics_bind_address")]
    pub metrics_expected_host: Option<String>,
    /// Browser-visible base URL when a proxy fronts this process: HTTPS, or
    /// HTTP on 127.0.0.1. Artifact and change URLs are built on it.
    #[arg(long)]
    pub public_base_url: Option<String>,
    /// Plans whose verified scope is kept between change requests. Nothing is
    /// refused at the bound: a plan past it is rebuilt from its token.
    #[arg(long, value_name = "COUNT", default_value_t = calque_engine::DEFAULT_MAX_ACTIVE_PLANS, value_parser = parse_max_active_plans)]
    pub max_active_plans: usize,
    /// Requests answered at once, artifact downloads included. A change
    /// stream holds one only until its response opens; a request beyond the
    /// bound is refused with 503 and Retry-After: 1.
    #[arg(long, value_name = "COUNT", default_value_t = calque_engine::DEFAULT_MAX_INFLIGHT_REQUESTS, value_parser = parse_max_inflight_requests)]
    pub max_inflight_requests: usize,
    /// Change streams held open at once. Every connected browser holds one;
    /// a stream beyond the bound is refused with 503 and retried. At startup
    /// the open-file limit is raised to the hard limit; if that still cannot
    /// hold this and --max-inflight-requests, both are cut to fit and a
    /// warning says by how much.
    #[arg(long, value_name = "COUNT", default_value_t = calque_engine::DEFAULT_MAX_OPEN_STREAMS, value_parser = parse_max_open_streams)]
    pub max_open_streams: usize,
    /// Bytes held in memory of the newest canonical records, which every
    /// change stream reads instead of the log, and of each scope's encoding of
    /// them. A browser behind what it holds reads the log; 0 holds nothing.
    #[arg(long, value_name = "BYTES", default_value_t = calque_engine::DEFAULT_CHANGE_TAIL_BYTES, value_parser = parse_change_tail_bytes)]
    pub change_tail_bytes: u64,
    #[command(flatten)]
    pub recut: RecutOptions,
    #[command(flatten)]
    pub tenants: TenantArguments,
}

/// How a partitioned scope prepares and keeps its tenants. A source-wide
/// scope reads none of these.
#[derive(Debug, Args)]
pub struct TenantArguments {
    /// Tenant builds that run at once. Each holds a replication slot and a
    /// WAL sender until it reaches its cut, and serve refuses to start unless
    /// PostgreSQL leaves one more of each free, for change capture.
    #[arg(long, value_name = "COUNT", default_value_t = calque_engine::DEFAULT_MAX_TENANT_BUILDS, value_parser = parse_max_tenant_builds)]
    pub max_tenant_builds: usize,
    /// A tenant opened within this many days stays warm: it is kept, and cut
    /// again by `--recut-tail-ratio` and `--recut-min-interval` as it
    /// changes. Any other tenant not pinned is evicted, and its next open
    /// waits for a build.
    #[arg(long, value_name = "DAYS", default_value_t = calque_engine::DEFAULT_TENANT_WARM_DAYS, value_parser = clap::value_parser!(u32).range(1..=3650))]
    pub tenant_warm_days: u32,
    /// Seconds a bootstrap for a tenant being built is held open before it is
    /// answered as preparing, which the client retries.
    #[arg(long, value_name = "SECONDS", default_value_t = calque_engine::DEFAULT_TENANT_HOLD_OPEN.as_secs(), value_parser = clap::value_parser!(u64).range(1..=60))]
    pub tenant_hold_open_seconds: u64,
    /// Decoded bytes from which a tenant's artifact is also compressed in the
    /// background, after every queued build; `off` compresses none.
    #[arg(long, value_name = "BYTES", default_value = "16777216", value_parser = parse_compress_above)]
    pub tenant_compress_above_bytes: CompressAbove,
    /// The most decoded bytes one tenant's artifact holds. A tenant above it
    /// is refused, and its grants answered as over capacity.
    #[arg(long, value_name = "BYTES", default_value_t = calque_engine::MAX_LOCAL_ARTIFACT_BYTES, value_parser = clap::value_parser!(u64).range(1..=calque_engine::MAX_LOCAL_ARTIFACT_BYTES))]
    pub max_tenant_bytes: u64,
}

#[derive(Debug, Clone, Copy)]
pub struct CompressAbove(pub Option<u64>);

fn parse_compress_above(value: &str) -> Result<CompressAbove, String> {
    if value == "off" {
        return Ok(CompressAbove(None));
    }
    value
        .parse::<u64>()
        .map(|bytes| CompressAbove(Some(bytes)))
        .map_err(|_| "must be a number of bytes, or off".to_owned())
}

fn parse_max_tenant_builds(value: &str) -> Result<usize, String> {
    match value.parse::<usize>() {
        Ok(count) if (1..=64).contains(&count) => Ok(count),
        _ => Err("must be a whole number of builds from 1 to 64".to_owned()),
    }
}

#[derive(Debug, Args)]
pub struct DevCommand {
    /// Private state directory. It is created with mode 0700.
    #[arg(long, value_name = "PATH", default_value = ".calque-dev")]
    pub state_dir: PathBuf,
    /// Table to synchronize, as `schema.table`. Repeat for more than one.
    #[arg(long = "table", value_name = "SCHEMA.TABLE", value_parser = QualifiedTable::parse_dotted, required = true)]
    pub tables: Vec<QualifiedTable>,
    /// Browser origin permitted to call this server. Repeat when needed.
    #[arg(
        long = "allow-origin",
        value_name = "ORIGIN",
        default_value = "http://127.0.0.1:5173",
        value_parser = parse_origin
    )]
    pub allowed_origins: Vec<AllowedOrigin>,
    /// HTTP port. Browsers address it as 127.0.0.1 on this port, whatever
    /// the bind address is.
    #[arg(long, default_value_t = 8787)]
    pub port: u16,
    /// Address the HTTP listener binds. Only a container whose port the host
    /// publishes on its loopback address needs `0.0.0.0`: the shared bearer
    /// is the only credential, so never put this listener on a network.
    #[arg(long, value_name = "ADDRESS", default_value = "127.0.0.1")]
    pub bind_address: IpAddr,
    /// Loopback port of a second listener that serves `/metrics` and nothing
    /// else. Without it no listener serves metrics: the HTTP listener has no
    /// `/metrics` route.
    #[arg(long, value_name = "PORT")]
    pub metrics_port: Option<u16>,
    /// Browser-visible base URL when a development proxy fronts this server:
    /// HTTPS, or HTTP on 127.0.0.1.
    #[arg(long)]
    pub public_base_url: Option<String>,
    /// Plans whose verified scope is kept between change requests. Nothing is
    /// refused at the bound: a plan past it is rebuilt from its token.
    #[arg(long, value_name = "COUNT", default_value_t = calque_engine::DEFAULT_MAX_ACTIVE_PLANS, value_parser = parse_max_active_plans)]
    pub max_active_plans: usize,
    /// Requests answered at once, artifact downloads included. A change
    /// stream holds one only until its response opens; a request beyond the
    /// bound is refused with 503 and Retry-After: 1.
    #[arg(long, value_name = "COUNT", default_value_t = calque_engine::DEFAULT_MAX_INFLIGHT_REQUESTS, value_parser = parse_max_inflight_requests)]
    pub max_inflight_requests: usize,
    /// Change streams held open at once. Every connected browser holds one;
    /// a stream beyond the bound is refused with 503 and retried. At startup
    /// the open-file limit is raised to the hard limit; if that still cannot
    /// hold this and --max-inflight-requests, both are cut to fit and a
    /// warning says by how much.
    #[arg(long, value_name = "COUNT", default_value_t = calque_engine::DEFAULT_MAX_OPEN_STREAMS, value_parser = parse_max_open_streams)]
    pub max_open_streams: usize,
    /// Bytes held in memory of the newest canonical records, which every
    /// change stream reads instead of the log, and of each scope's encoding of
    /// them. A browser behind what it holds reads the log; 0 holds nothing.
    #[arg(long, value_name = "BYTES", default_value_t = calque_engine::DEFAULT_CHANGE_TAIL_BYTES, value_parser = parse_change_tail_bytes)]
    pub change_tail_bytes: u64,
    #[command(flatten)]
    pub recut: RecutOptions,
    /// Partitions the scope by this column, as `calque init --tenant-column`
    /// does: each table whose primary key leads with it holds tenants' rows,
    /// and every other table is shared. The shared bearer reads the tenant
    /// `--tenant` names.
    #[arg(long, value_name = "COLUMN", requires = "tenant")]
    pub tenant_column: Option<String>,
    /// The tenant the shared bearer reads under `--tenant-column`: an
    /// integer, or a UUID when the column is a uuid.
    #[arg(long, value_name = "VALUE", requires = "tenant_column")]
    pub tenant: Option<String>,
    /// Required acknowledgement that the PostgreSQL source is disposable.
    #[arg(long)]
    pub acknowledge_disposable_source: bool,
    #[command(flatten)]
    pub database: DatabaseEnvironment,
}

/// When the artifact is cut again. A cold load replays every change committed
/// since the cut of the artifact it downloads.
#[derive(Debug, Clone, Copy, Args)]
pub struct RecutOptions {
    /// Cut the artifact again once the changes since its cut hold this many
    /// bytes per byte of the decoded artifact. 0 never cuts again; the change
    /// log is trimmed either way. A re-cut slows live updates while it runs.
    #[arg(long, value_name = "RATIO", default_value_t = calque_engine::DEFAULT_RECUT_TAIL_RATIO, value_parser = parse_recut_tail_ratio)]
    pub recut_tail_ratio: f64,
    /// The least time, in seconds, from the newest cut, or from the start of
    /// the last re-cut, to the next re-cut.
    #[arg(long = "recut-min-interval", value_name = "SECONDS", default_value_t = calque_engine::DEFAULT_RECUT_MIN_INTERVAL.as_secs())]
    pub recut_min_interval_seconds: u64,
}

impl RecutOptions {
    pub fn policy(self) -> calque_engine::RecutPolicy {
        calque_engine::RecutPolicy {
            tail_ratio: self.recut_tail_ratio,
            min_interval: std::time::Duration::from_secs(self.recut_min_interval_seconds),
        }
    }
}

impl Default for RecutOptions {
    fn default() -> Self {
        Self {
            recut_tail_ratio: calque_engine::DEFAULT_RECUT_TAIL_RATIO,
            recut_min_interval_seconds: calque_engine::DEFAULT_RECUT_MIN_INTERVAL.as_secs(),
        }
    }
}

fn parse_recut_tail_ratio(value: &str) -> Result<f64, String> {
    match value.parse::<f64>() {
        Ok(ratio) if ratio.is_finite() && (0.0..=1_000.0).contains(&ratio) => Ok(ratio),
        _ => Err("must be a number from 0 to 1000".to_owned()),
    }
}

#[derive(Debug, Args)]
pub struct StatusCommand {
    /// State directory written by `dev` or `serve`.
    #[arg(long, value_name = "PATH")]
    pub state_dir: PathBuf,
}

#[derive(Debug, Args)]
pub struct DestroyCommand {
    /// State directory written by `dev` or `serve`. It is removed together
    /// with its replication slot and the publication it names, even one the
    /// application created; the next `dev` or `serve` creates it again.
    #[arg(long, value_name = "PATH")]
    pub state_dir: PathBuf,
    /// Confirm the destruction; destroy refuses to run without it.
    #[arg(long)]
    pub yes: bool,
    /// Remove only local files, leaving PostgreSQL objects on the original source intact.
    #[arg(long, requires = "yes")]
    pub local_state_only: bool,
    #[command(flatten)]
    pub database: DatabaseEnvironment,
}

fn parse_origin(value: &str) -> Result<AllowedOrigin, String> {
    AllowedOrigin::parse(value).map_err(|error| error.to_string())
}

fn parse_max_active_plans(value: &str) -> Result<usize, String> {
    match value.parse::<usize>() {
        Ok(count) if count > 0 => Ok(count),
        _ => Err("must be a whole number of plans, at least 1".to_owned()),
    }
}

/// At most a million: the bound is a semaphore's permits, and no process
/// holds more connections than that.
fn parse_max_inflight_requests(value: &str) -> Result<usize, String> {
    match value.parse::<usize>() {
        Ok(count) if (1..=1_000_000).contains(&count) => Ok(count),
        _ => Err("must be a whole number of requests from 1 to 1000000".to_owned()),
    }
}

/// At most a million, for the same reason.
fn parse_max_open_streams(value: &str) -> Result<usize, String> {
    match value.parse::<usize>() {
        Ok(count) if (1..=1_000_000).contains(&count) => Ok(count),
        _ => Err("must be a whole number of streams from 1 to 1000000".to_owned()),
    }
}

/// At most 64 GiB, which no one process's memory holds for this.
fn parse_change_tail_bytes(value: &str) -> Result<u64, String> {
    match value.parse::<u64>() {
        Ok(bytes) if bytes <= 64 << 30 => Ok(bytes),
        _ => Err("must be a whole number of bytes from 0 to 68719476736".to_owned()),
    }
}

#[derive(Debug, Args)]
pub struct InspectCommand {
    #[command(flatten)]
    pub database: DatabaseEnvironment,
    /// Source identifier recorded in the inspection document.
    #[arg(long, value_name = "ID")]
    pub source: String,
    /// PostgreSQL publication to inspect.
    #[arg(long, value_name = "NAME")]
    pub publication: String,
    /// Table to inspect, as `schema.table`. Repeat for more than one.
    #[arg(long = "table", value_name = "SCHEMA.TABLE", value_parser = QualifiedTable::parse_dotted, required = true)]
    pub tables: Vec<QualifiedTable>,
}

#[derive(Debug, Args)]
pub struct InitCommand {
    /// Inspection document produced by `calque inspect`, or `-` for stdin.
    #[arg(long, value_name = "PATH")]
    pub from_inspection: PathBuf,
    /// Application identifier, for example `incident-ops`.
    #[arg(long, value_name = "ID")]
    pub application_id: String,
    /// Application version recorded in the manifest.
    #[arg(long, value_name = "VERSION", default_value = "0.1.0")]
    pub application_version: String,
    /// Environment variable the server reads the source connection string from.
    #[arg(long, value_name = "NAME", default_value = "CALQUE_DATABASE_URL")]
    pub database_url_env: String,
    /// The flag's earlier name, refused with the current one.
    #[arg(long = "connection-env", value_name = "NAME", hide = true)]
    pub renamed_connection_env: Option<String>,
    /// Replication slot name. Defaults to `calque_<id>_slot`, where `<id>` is
    /// the manifest's application id with each `-` replaced by `_`.
    #[arg(long, value_name = "NAME")]
    pub slot: Option<String>,
    /// Identifier of the generated scope.
    #[arg(long, value_name = "ID", default_value = "all")]
    pub scope_id: String,
    /// Give each grant one tenant's rows: a table whose primary key leads
    /// with this column holds tenants' rows, and a table without it is shared
    /// by every tenant.
    #[arg(long, value_name = "COLUMN")]
    pub tenant_column: Option<String>,
    /// The grant attribute that holds a grant's tenant. Defaults to the
    /// tenant column's name.
    #[arg(long, value_name = "NAME", requires = "tenant_column")]
    pub tenant_attribute: Option<String>,
    /// Where to write the manifest.
    #[arg(long, value_name = "PATH", default_value = "calque.config.json")]
    pub output: PathBuf,
    /// Overwrite the output file if it already exists.
    #[arg(long)]
    pub force: bool,
}

pub fn run() -> Result<(), Box<dyn Error>> {
    match Cli::parse().command {
        Command::Inspect(command) => {
            let tables: BTreeSet<QualifiedTable> = command.tables.into_iter().collect();
            let database = command.database.resolve_allowing_plaintext()?;
            if let Some(host) = database.plaintext_remote_host() {
                eprintln!("warning: {}", recovery::plaintext_warning(host));
            }
            let output = inspect::run(&database, &command.source, &command.publication, tables)?;
            println!("{}", serde_json::to_string(&output)?);
            if let Some(warning) = calque_engine::inspect_slot_wal_bound(&database)? {
                let _ = writeln!(io::stderr(), "calque: warning: {warning}");
            }
            Ok(())
        }
        Command::Init(command) => {
            if command.renamed_connection_env.is_some() {
                return Err("--connection-env is now --database-url-env".into());
            }
            let raw = if command.from_inspection.as_os_str() == "-" {
                std::io::read_to_string(std::io::stdin())?
            } else {
                fs::read_to_string(&command.from_inspection)
                    .map_err(|error| format!("{}: {error}", command.from_inspection.display()))?
            };
            let inspection: inspect::InspectionOutput =
                serde_json::from_str(&raw).map_err(|error| {
                    let input = if command.from_inspection.as_os_str() == "-" {
                        "standard input".to_owned()
                    } else {
                        command.from_inspection.display().to_string()
                    };
                    format!("{input} is not a calque inspect document: {error}")
                })?;
            let options = init::InitOptions {
                application_id: command.application_id,
                application_version: command.application_version,
                database_url_env: command.database_url_env,
                slot: command.slot,
                scope_id: command.scope_id,
                tenants: command.tenant_column.map(|column| init::Tenants {
                    attribute: command.tenant_attribute.unwrap_or_else(|| column.clone()),
                    column,
                }),
            };
            let rendered = init::build(&inspection, &options)?;
            init::write(&command.output, &rendered, command.force)?;
            // Not `eprintln!`, which panics when nothing reads stderr.
            let _ = writeln!(
                io::stderr(),
                "calque: wrote {} ({} {})",
                command.output.display(),
                inspection.relations.len(),
                if inspection.relations.len() == 1 {
                    "collection"
                } else {
                    "collections"
                }
            );
            Ok(())
        }
        Command::Serve(command) => {
            multi_thread_runtime()?.block_on(serve::run(command))?;
            Ok(())
        }
        Command::Dev(command) => {
            multi_thread_runtime()?.block_on(dev::run_dev(command))?;
            Ok(())
        }
        Command::Status(command) => {
            dev::run_status(&command.state_dir)?;
            Ok(())
        }
        Command::Destroy(command) => {
            multi_thread_runtime()?.block_on(dev::run_destroy(command))?;
            Ok(())
        }
        Command::Backup(command) => {
            recovery::run_backup(&command)?;
            Ok(())
        }
        Command::Restore(command) => {
            recovery::run_restore(&command)?;
            Ok(())
        }
        Command::VerifyRestore(command) => {
            recovery::run_verify_restore(&command)?;
            Ok(())
        }
    }
}

fn multi_thread_runtime() -> std::io::Result<tokio::runtime::Runtime> {
    tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
}

#[cfg(test)]
mod tests {
    use super::*;
    use clap::CommandFactory;

    #[test]
    fn command_definition_is_valid() {
        Cli::command().debug_assert();
    }

    #[test]
    fn qualified_tables_require_a_schema_and_a_table() {
        for invalid in [
            "public",
            ".issues",
            "public.",
            // An extra dot must not be read as schema "public", table
            // "issues.notes"; it names nothing the user asked for.
            "public.issues.notes",
            "public.\"issues\"",
            "public.bad-name",
        ] {
            assert!(
                QualifiedTable::parse_dotted(invalid).is_err(),
                "expected {invalid:?} to be rejected"
            );
        }
        let parsed = QualifiedTable::parse_dotted("public.issues").unwrap();
        assert_eq!(parsed.schema, "public");
        assert_eq!(parsed.table, "issues");
    }

    #[test]
    fn inspect_requires_at_least_one_table() {
        let error = Cli::try_parse_from([
            "calque",
            "inspect",
            "--database-url-env",
            "TEST_DATABASE_URL",
            "--source",
            "primary",
            "--publication",
            "calque_test",
        ])
        .unwrap_err();
        assert_eq!(
            error.kind(),
            clap::error::ErrorKind::MissingRequiredArgument
        );
    }

    #[test]
    fn inspect_accepts_repeated_tables() {
        let cli = Cli::try_parse_from([
            "calque",
            "inspect",
            "--database-url-env",
            "TEST_DATABASE_URL",
            "--source",
            "primary",
            "--publication",
            "calque_test",
            "--table",
            "public.issues",
            "--table",
            "public.notes",
        ])
        .unwrap();
        let Command::Inspect(command) = cli.command else {
            panic!("expected inspect");
        };
        assert_eq!(command.tables.len(), 2);
        assert_eq!(command.database.database_url_env, "TEST_DATABASE_URL");
    }

    #[test]
    fn inspect_reads_the_same_default_environment_as_the_other_commands() {
        let cli = Cli::try_parse_from([
            "calque",
            "inspect",
            "--source",
            "primary",
            "--publication",
            "calque_test",
            "--table",
            "public.issues",
        ])
        .unwrap();
        let Command::Inspect(command) = cli.command else {
            panic!("expected inspect");
        };
        assert_eq!(command.database.database_url_env, "CALQUE_DATABASE_URL");
    }

    /// `serve` defaults to the loopback binding, and a composition moves the
    /// listener without touching the Host check.
    #[test]
    fn serve_binds_loopback_by_default_and_takes_a_configured_address() {
        let serve = |extra: &[&str]| {
            let mut arguments = vec![
                "calque",
                "serve",
                "--grant-audience",
                "incident.test",
                "--grant-verification-key-env",
                "CALQUE_GRANT_KEY",
                "--allow-origin",
                "https://app.example.com",
            ];
            arguments.extend_from_slice(extra);
            let Command::Serve(command) = Cli::try_parse_from(arguments).unwrap().command else {
                panic!("expected serve");
            };
            command
        };

        let default = serve(&[]);
        assert_eq!(
            default.bind_address,
            IpAddr::V4(std::net::Ipv4Addr::LOCALHOST)
        );
        assert_eq!(default.expected_host, None);
        assert_eq!(default.port, 8787);

        let composed = serve(&[
            "--bind-address",
            "0.0.0.0",
            "--expected-host",
            "calque.internal:8787",
        ]);
        assert_eq!(
            composed.bind_address,
            IpAddr::V4(std::net::Ipv4Addr::UNSPECIFIED)
        );
        assert_eq!(
            composed.expected_host.as_deref(),
            Some("calque.internal:8787")
        );

        // No metrics listener unless one is asked for, and then at the
        // address given, with its own Host check.
        assert_eq!(default.metrics_bind_address, None);
        assert_eq!(default.metrics_expected_host, None);
        let scraped = serve(&[
            "--metrics-bind-address",
            "0.0.0.0:8788",
            "--metrics-expected-host",
            "127.0.0.1:8788",
        ]);
        assert_eq!(
            scraped.metrics_bind_address,
            Some("0.0.0.0:8788".parse().unwrap())
        );
        assert_eq!(
            scraped.metrics_expected_host.as_deref(),
            Some("127.0.0.1:8788")
        );
        for arguments in [
            // An address without its port names no listener.
            &["--metrics-bind-address", "127.0.0.1"][..],
            &["--metrics-bind-address", "localhost:8788"][..],
            // A Host for a listener that does not exist.
            &["--metrics-expected-host", "127.0.0.1:8788"][..],
        ] {
            let mut full = vec![
                "calque",
                "serve",
                "--grant-audience",
                "incident.test",
                "--grant-verification-key-env",
                "CALQUE_GRANT_KEY",
                "--allow-origin",
                "https://app.example.com",
            ];
            full.extend_from_slice(arguments);
            assert!(Cli::try_parse_from(full).is_err(), "{arguments:?}");
        }

        // The flag takes an address, never a host name to resolve.
        let error = Cli::try_parse_from([
            "calque",
            "serve",
            "--grant-audience",
            "incident.test",
            "--grant-verification-key-env",
            "CALQUE_GRANT_KEY",
            "--allow-origin",
            "https://app.example.com",
            "--bind-address",
            "calque.internal",
        ])
        .unwrap_err();
        assert_eq!(error.kind(), clap::error::ErrorKind::ValueValidation);
    }

    /// `dev` binds loopback unless a container asks for another address.
    #[test]
    fn dev_binds_loopback_by_default_and_takes_a_configured_address() {
        let dev = |extra: &[&str]| {
            let mut arguments = vec![
                "calque",
                "dev",
                "--table",
                "public.tasks",
                "--acknowledge-disposable-source",
            ];
            arguments.extend_from_slice(extra);
            let Command::Dev(command) = Cli::try_parse_from(arguments).unwrap().command else {
                panic!("expected dev");
            };
            command
        };

        assert_eq!(
            dev(&[]).bind_address,
            IpAddr::V4(std::net::Ipv4Addr::LOCALHOST)
        );
        assert_eq!(
            dev(&["--bind-address", "0.0.0.0"]).bind_address,
            IpAddr::V4(std::net::Ipv4Addr::UNSPECIFIED)
        );
        let error = Cli::try_parse_from([
            "calque",
            "dev",
            "--table",
            "public.tasks",
            "--acknowledge-disposable-source",
            "--bind-address",
            "localhost",
        ])
        .unwrap_err();
        assert_eq!(error.kind(), clap::error::ErrorKind::ValueValidation);
    }

    #[test]
    fn serve_requires_an_allowed_origin() {
        let error = Cli::try_parse_from([
            "calque",
            "serve",
            "--grant-audience",
            "incident.test",
            "--grant-verification-key-env",
            "CALQUE_GRANT_KEY",
        ])
        .unwrap_err();
        assert_eq!(
            error.kind(),
            clap::error::ErrorKind::MissingRequiredArgument
        );
        assert!(
            error.to_string().contains("--allow-origin <ORIGIN>"),
            "{error}"
        );
    }

    /// `status` and `destroy` read a state directory of either kind, so they
    /// name it rather than default to one kind's.
    #[test]
    fn status_and_destroy_require_a_state_directory() {
        for arguments in [vec!["calque", "status"], vec!["calque", "destroy", "--yes"]] {
            let error = Cli::try_parse_from(&arguments).unwrap_err();
            assert_eq!(
                error.kind(),
                clap::error::ErrorKind::MissingRequiredArgument,
                "{arguments:?}"
            );
            assert!(error.to_string().contains("--state-dir <PATH>"), "{error}");
        }
    }

    #[test]
    fn every_state_directory_flag_has_help() {
        let command = Cli::command();
        let mut named = Vec::new();
        for subcommand in command.get_subcommands() {
            if let Some(state_dir) = subcommand
                .get_arguments()
                .find(|argument| argument.get_id() == "state_dir")
            {
                assert!(
                    state_dir.get_help().is_some(),
                    "{} --state-dir has no help",
                    subcommand.get_name()
                );
                named.push(subcommand.get_name());
            }
        }
        assert_eq!(
            named,
            [
                "serve",
                "dev",
                "status",
                "destroy",
                "backup",
                "restore",
                "verify-restore"
            ]
        );
    }

    #[test]
    fn init_defaults_the_output_path_and_database_url_environment() {
        let cli = Cli::try_parse_from([
            "calque",
            "init",
            "--from-inspection",
            "inspection.json",
            "--application-id",
            "incident-ops",
        ])
        .unwrap();
        let Command::Init(command) = cli.command else {
            panic!("expected init");
        };
        assert_eq!(command.output, PathBuf::from("calque.config.json"));
        assert_eq!(command.database_url_env, "CALQUE_DATABASE_URL");
        assert!(!command.force);
        assert!(command.slot.is_none());
    }
}
