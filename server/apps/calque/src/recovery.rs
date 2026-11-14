//! `calque backup`, `calque restore` and `calque verify-restore`, for a `serve`
//! or a `dev` state directory.
//!
//! The unit format, the stopped-source gates and the correctness oracle are
//! `calque_engine::recovery`'s. This module names the state directory, reads
//! the connection string from the environment variable the operator names,
//! and resolves the object store through `serve`'s selector.

use std::{
    env,
    ffi::OsString,
    fs,
    path::{Path, PathBuf},
    sync::Arc,
};

use blob_store::ManagedBlobStore;
use calque_engine::{
    create_recovery_backup, restore_recovery_backup, verify_restored_recovery_backup, DatabaseUrl,
    ExistingSourceRows, RecoveryObjects, RecoveryReport,
};
use thiserror::Error;

use crate::cli::{BackupCommand, DatabaseEnvironment, RestoreCommand, VerifyRestoreCommand};
use crate::remedy::Destroy;
use crate::serve::{open_object_store, selected_object_store_kind, ObjectStoreKind};

#[derive(Debug, Error)]
pub enum RecoveryCliError {
    #[error("environment variable {0} is not set; set it to the source's PostgreSQL connection string, or name the variable that holds it with --database-url-env")]
    MissingDatabaseUrl(String),
    #[error("environment variable {0} is not valid UTF-8")]
    NonUtf8DatabaseUrl(String),
    /// `serve`'s object-store refusal, with the `E_SERVE_OBJECT_STORE_*` code
    /// `serve` prints.
    #[error("{0}")]
    ObjectStoreSelection(String),
    #[error(transparent)]
    Guard(#[from] calque_engine::GuardError),
    #[error(transparent)]
    Recovery(#[from] calque_engine::RecoveryError),
    /// The engine's refusal of state another source wrote, with the remedy it
    /// leaves to this command: the variable the connection string came from.
    #[error("{refusal} Point {database_url_env} at the persisted source and retry. If the original source no longer exists, run {destroy}; PostgreSQL objects on the original source will remain.", destroy = Destroy::local_state_only(state_dir))]
    SourceIdentityMismatch {
        refusal: Box<calque_engine::RecoveryError>,
        state_dir: PathBuf,
        database_url_env: String,
    },
}

/// Names the remedy for state another source wrote, which needs the variable
/// this command read the connection string from.
fn with_source_remedy(
    error: RecoveryCliError,
    state_dir: &Path,
    database: &DatabaseEnvironment,
) -> RecoveryCliError {
    match error {
        RecoveryCliError::Recovery(
            refusal @ calque_engine::RecoveryError::SourceIdentityMismatch { .. },
        ) => RecoveryCliError::SourceIdentityMismatch {
            refusal: Box::new(refusal),
            state_dir: state_dir.to_path_buf(),
            database_url_env: database.database_url_env.clone(),
        },
        error => error,
    }
}

/// The object store this deployment uses, through `serve`'s selector so
/// `backup` and `serve` agree on it. `None` is the filesystem store, whose
/// objects are the state directory's `objects` tree; a provider store's objects
/// are exported at backup and put back at restore.
///
/// `staging_root` is outside the state directory because `restore` requires
/// its target to be absent, and a store that created a directory there would
/// refuse the restore it was opened for.
fn recovery_object_store(
    value: impl Fn(&str) -> Option<String>,
    staging_root: &Path,
) -> Result<Option<Arc<dyn ManagedBlobStore>>, RecoveryCliError> {
    let kind = selected_object_store_kind(value)
        .map_err(|error| RecoveryCliError::ObjectStoreSelection(error.to_string()))?;
    match kind {
        ObjectStoreKind::Filesystem => Ok(None),
        ObjectStoreKind::S3 => Ok(Some(
            open_object_store(kind, staging_root)
                .map_err(|error| RecoveryCliError::ObjectStoreSelection(error.to_string()))?,
        )),
    }
}

/// The scratch directory a provider store gets for one recovery command:
/// a sibling of the state directory, never the state directory itself.
fn recovery_staging_root(state_dir: &Path) -> PathBuf {
    let mut name = state_dir.as_os_str().to_owned();
    name.push(".recovery-staging");
    PathBuf::from(name)
}

/// Removes the scratch directory. It holds only staging bytes, so failing to
/// remove it does not fail the command.
fn remove_recovery_staging(staging_root: &Path) {
    let _ = fs::remove_dir_all(staging_root);
}

/// The borrowed form `calque_engine::recovery` takes.
fn recovery_objects(store: Option<&Arc<dyn ManagedBlobStore>>) -> RecoveryObjects<'_> {
    match store {
        None => RecoveryObjects::StateDirectory,
        Some(store) => RecoveryObjects::Provider(store.as_ref()),
    }
}

/// Resolves the object store, runs one recovery command with it, and removes
/// the provider adapter's scratch directory whatever the outcome. The store is
/// resolved first, so a configuration `serve` would refuse fails the command
/// before the connection string is read or a byte is copied.
fn with_object_half<T>(
    state_dir: &Path,
    body: impl FnOnce(RecoveryObjects<'_>) -> Result<T, RecoveryCliError>,
) -> Result<T, RecoveryCliError> {
    let staging = recovery_staging_root(state_dir);
    let outcome = recovery_object_store(|name| env::var(name).ok(), &staging)
        .and_then(|store| body(recovery_objects(store.as_ref())));
    remove_recovery_staging(&staging);
    outcome
}

/// What a command reaching its database in plaintext tells the person who ran it.
pub fn plaintext_warning(host: &str) -> String {
    format!(
        "the database at {host} is reached without TLS (sslmode=disable): its password and every row cross the network in plaintext, so use it only for a disposable database"
    )
}

impl DatabaseEnvironment {
    /// The connection string, read only from the named environment variable.
    pub fn resolve(&self) -> Result<DatabaseUrl, RecoveryCliError> {
        Ok(DatabaseUrl::parse(&self.read()?)?)
    }

    /// As [`resolve`](Self::resolve), for the commands a person runs by hand
    /// against a database they chose, `dev`, `inspect` and `destroy`: an
    /// explicit `sslmode=disable` is also accepted on a host that is not
    /// loopback, and the caller prints [`plaintext_warning`].
    pub fn resolve_allowing_plaintext(&self) -> Result<DatabaseUrl, RecoveryCliError> {
        Ok(DatabaseUrl::parse_for_dev(&self.read()?)?)
    }

    fn read(&self) -> Result<String, RecoveryCliError> {
        let raw = env::var_os(&self.database_url_env)
            .ok_or_else(|| RecoveryCliError::MissingDatabaseUrl(self.database_url_env.clone()))?;
        OsString::into_string(raw)
            .map_err(|_| RecoveryCliError::NonUtf8DatabaseUrl(self.database_url_env.clone()))
    }
}

pub fn run_backup(command: &BackupCommand) -> Result<(), RecoveryCliError> {
    let report = with_object_half(&command.state_dir, |objects| {
        let database = command.database.resolve()?;
        Ok(create_recovery_backup(
            &database,
            &command.state_dir,
            &command.output,
            objects,
        )?)
    })
    .map_err(|error| with_source_remedy(error, &command.state_dir, &command.database))?;
    report_unit("backup", &report);
    Ok(())
}

pub fn run_restore(command: &RestoreCommand) -> Result<(), RecoveryCliError> {
    // `Refuse` is the default; `--replace-source-rows` is the only way to the
    // stopped-source replacement path, and its own gates are not relaxed here.
    let existing_rows = if command.replace_source_rows {
        ExistingSourceRows::ReplaceStopped
    } else {
        ExistingSourceRows::Refuse
    };
    let report = with_object_half(&command.state_dir, |objects| {
        let database = command.database.resolve()?;
        Ok(restore_recovery_backup(
            &database,
            &command.backup,
            &command.state_dir,
            existing_rows,
            objects,
        )?)
    })
    .map_err(|error| with_source_remedy(error, &command.state_dir, &command.database))?;
    report_unit("restored", &report);
    Ok(())
}

pub fn run_verify_restore(command: &VerifyRestoreCommand) -> Result<(), RecoveryCliError> {
    let report = with_object_half(&command.state_dir, |objects| {
        let database = command.database.resolve()?;
        Ok(verify_restored_recovery_backup(
            &database,
            &command.backup,
            &command.state_dir,
            objects,
        )?)
    })
    .map_err(|error| with_source_remedy(error, &command.state_dir, &command.database))?;
    report_unit("verified", &report);
    Ok(())
}

/// The three lines every recovery command prints.
fn report_unit(label: &str, report: &RecoveryReport) {
    println!("{label} source: {}", report.source_id);
    println!("{label} epoch: {}", report.source_epoch);
    println!("{label} oracle: {}", report.oracle.aggregate_sha256);
}

#[cfg(test)]
mod tests {
    use std::path::PathBuf;

    use clap::Parser as _;

    use super::*;
    use crate::cli::{Cli, Command};
    use crate::serve::OBJECT_STORE_ENV;

    /// `Result::expect_err` wants the success type to be `Debug`, and a
    /// `dyn ManagedBlobStore` is not one.
    fn refusal(
        result: Result<Option<Arc<dyn ManagedBlobStore>>, RecoveryCliError>,
        what: &str,
    ) -> RecoveryCliError {
        match result {
            Ok(_) => panic!("{what}"),
            Err(error) => error,
        }
    }

    /// Replacing another deployment's control rows is the destructive half of
    /// restore, so it may never be the default.
    #[test]
    fn serve_restore_replaces_source_rows_only_when_asked() {
        let cli = Cli::try_parse_from([
            "calque",
            "restore",
            "--backup",
            "/srv/unit",
            "--state-dir",
            ".calque-serve",
        ])
        .unwrap();
        let Command::Restore(command) = cli.command else {
            panic!("expected restore command");
        };
        assert!(!command.replace_source_rows);
        assert_eq!(command.state_dir, PathBuf::from(".calque-serve"));

        let cli = Cli::try_parse_from([
            "calque",
            "restore",
            "--backup",
            "/srv/unit",
            "--replace-source-rows",
        ])
        .unwrap();
        let Command::Restore(command) = cli.command else {
            panic!("expected restore command");
        };
        assert!(command.replace_source_rows);
        // The default state directory is `serve`'s, not the local harness's.
        assert_eq!(command.state_dir, PathBuf::from(".calque-serve"));
    }

    #[test]
    fn serve_backup_and_verify_restore_name_a_unit_and_an_environment() {
        let cli = Cli::try_parse_from([
            "calque",
            "backup",
            "--output",
            "/srv/unit",
            "--database-url-env",
            "CALQUE_TEST_DATABASE_URL",
        ])
        .unwrap();
        let Command::Backup(command) = cli.command else {
            panic!("expected backup command");
        };
        assert_eq!(command.output, PathBuf::from("/srv/unit"));
        assert_eq!(command.state_dir, PathBuf::from(".calque-serve"));
        assert_eq!(
            command.database.database_url_env,
            "CALQUE_TEST_DATABASE_URL"
        );

        let cli =
            Cli::try_parse_from(["calque", "verify-restore", "--backup", "/srv/unit"]).unwrap();
        let Command::VerifyRestore(command) = cli.command else {
            panic!("expected verify-restore command");
        };
        assert_eq!(command.backup, PathBuf::from("/srv/unit"));
        assert_eq!(command.database.database_url_env, "CALQUE_DATABASE_URL");
    }

    #[test]
    fn a_recovery_command_refuses_a_connection_string_on_the_command_line() {
        assert!(Cli::try_parse_from([
            "calque",
            "backup",
            "--output",
            "/srv/unit",
            "--database-url",
            "postgresql://127.0.0.1/app",
        ])
        .is_err());

        let environment = DatabaseEnvironment {
            database_url_env: "CALQUE_ABSENT_RECOVERY_URL_FOR_TEST".to_owned(),
        };
        let error = environment
            .resolve()
            .expect_err("an unset environment variable is refused");
        assert!(
            matches!(&error, RecoveryCliError::MissingDatabaseUrl(name) if name == &environment.database_url_env),
            "{error}"
        );
        assert_eq!(
            error.to_string(),
            "environment variable CALQUE_ABSENT_RECOVERY_URL_FOR_TEST is not set; set it to the source's PostgreSQL connection string, or name the variable that holds it with --database-url-env"
        );
    }

    /// Only the hand-run commands read an explicit `sslmode=disable` on a
    /// host name; every command that goes through `resolve` refuses it.
    #[test]
    fn only_the_plaintext_resolver_takes_plaintext_to_a_host_name() {
        const NAME: &str = "CALQUE_PLAINTEXT_HOST_URL_FOR_TEST";
        std::env::set_var(
            NAME,
            "postgresql://calque:secret@host.docker.internal:5432/app?sslmode=disable",
        );
        let environment = DatabaseEnvironment {
            database_url_env: NAME.to_owned(),
        };
        let refusal = environment
            .resolve()
            .expect_err("resolve is the strict rule");
        assert!(
            matches!(
                refusal,
                RecoveryCliError::Guard(calque_engine::GuardError::DatabaseTlsRequired)
            ),
            "{refusal}"
        );
        let database = environment.resolve_allowing_plaintext().unwrap();
        assert_eq!(
            database.plaintext_remote_host(),
            Some("host.docker.internal")
        );
        std::env::remove_var(NAME);
    }

    /// `serve` reads its URL through `DatabaseUrl::parse`, and among these
    /// files only the command line's `inspect` reads it through the
    /// plaintext-tolerant resolver.
    #[test]
    fn serve_and_the_recovery_commands_never_call_the_dev_parse() {
        for (file, source) in [
            ("serve.rs", include_str!("serve.rs")),
            ("recovery.rs", include_str!("recovery.rs")),
            ("inspect.rs", include_str!("inspect.rs")),
            ("cli.rs", include_str!("cli.rs")),
        ] {
            let code = source.split("#[cfg(test)]").next().unwrap();
            let calls = code.matches("parse_for_dev(").count()
                + code.matches("resolve_allowing_plaintext(").count();
            // `recovery.rs` defines the resolver and its one call to the parse;
            // `cli.rs` calls it for `inspect`.
            let allowed = match file {
                "recovery.rs" => 2,
                "cli.rs" => 1,
                _ => 0,
            };
            assert_eq!(calls, allowed, "{file}");
        }
    }

    #[test]
    fn state_another_source_wrote_names_the_variable_the_command_read() {
        let database = DatabaseEnvironment {
            database_url_env: "CALQUE_SERVE_URL".to_owned(),
        };
        let state_dir = Path::new(".calque-serve");
        let refusal = calque_engine::RecoveryError::SourceIdentityMismatch {
            state_dir: state_dir.to_path_buf(),
            persisted: "database app on PostgreSQL system 111 timeline 1 (previous 1)".to_owned(),
            connected: "database app on PostgreSQL system 222 timeline 2 (previous 1)".to_owned(),
        };
        assert_eq!(
            with_source_remedy(refusal.into(), state_dir, &database).to_string(),
            "connected database app on PostgreSQL system 222 timeline 2 (previous 1), but state at .calque-serve belongs to database app on PostgreSQL system 111 timeline 1 (previous 1). Point CALQUE_SERVE_URL at the persisted source and retry. If the original source no longer exists, run calque destroy --state-dir .calque-serve --yes --local-state-only; PostgreSQL objects on the original source will remain."
        );

        let other: RecoveryCliError =
            calque_engine::RecoveryError::TargetExists("backup".into()).into();
        let raised = other.to_string();
        assert_eq!(
            with_source_remedy(other, state_dir, &database).to_string(),
            raised
        );
    }

    /// Neither store is a refusal, and neither may silently stand in for the
    /// other.
    #[test]
    fn a_recovery_command_resolves_the_object_half_from_the_configured_store() {
        let staging = PathBuf::from("/srv/state.recovery-staging");
        let unset = |_: &str| None;
        assert!(
            recovery_object_store(unset, &staging)
                .expect("the default store carries its own objects")
                .is_none(),
            "an unset selector is the filesystem store, whose objects are the state tree",
        );
        assert!(recovery_object_store(
            |name| (name == OBJECT_STORE_ENV).then(|| "filesystem".to_owned()),
            &staging,
        )
        .expect("the filesystem store carries its own objects")
        .is_none());

        // With `s3` selected and no bucket named, the error is `serve`'s
        // missing-value refusal: the S3 branch opens a store.
        let error = refusal(
            recovery_object_store(
                |name| (name == OBJECT_STORE_ENV).then(|| "s3".to_owned()),
                &staging,
            ),
            "an S3 selector with no bucket cannot open a store",
        );
        let rendered = error.to_string();
        assert!(
            matches!(&error, RecoveryCliError::ObjectStoreSelection(_)),
            "{rendered}"
        );
        assert!(rendered.contains("CALQUE_S3_BUCKET"), "{rendered}");
    }

    /// The provider adapter's local scratch never lands in the directory a
    /// restore requires to be absent.
    #[test]
    fn the_provider_staging_directory_is_a_sibling_of_the_state_directory() {
        assert_eq!(
            recovery_staging_root(Path::new("/var/lib/calque/state")),
            PathBuf::from("/var/lib/calque/state.recovery-staging")
        );
        assert_eq!(
            recovery_staging_root(Path::new(".calque-serve")),
            PathBuf::from(".calque-serve.recovery-staging")
        );
    }

    #[test]
    fn a_recovery_command_refuses_the_configuration_serve_refuses_to_start_on() {
        let staging = PathBuf::from("/srv/state.recovery-staging");
        let error = refusal(
            recovery_object_store(
                |name| (name == "CALQUE_S3_BUCKET").then(|| "calque-artifacts".to_owned()),
                &staging,
            ),
            "credentials without the selector are refused, not ignored",
        );
        let rendered = error.to_string();
        assert!(
            rendered.contains("E_SERVE_OBJECT_STORE_NOT_SELECTED"),
            "{rendered}"
        );

        let error = refusal(
            recovery_object_store(
                |name| (name == OBJECT_STORE_ENV).then(|| "gcs".to_owned()),
                &staging,
            ),
            "a store this build does not implement is refused",
        );
        assert!(
            error.to_string().contains("E_SERVE_OBJECT_STORE_UNKNOWN"),
            "{error}"
        );
    }
}
