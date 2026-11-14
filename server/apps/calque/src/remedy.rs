//! The `calque destroy` command lines that refusals print as their remedy.
//!
//! The libraries that refuse a state directory leave the command line to this
//! crate, because it needs the database URL variable only the application
//! knows.

use std::{fmt, path::Path};

use calque_engine::CdcStopRemedy;

/// One `calque destroy` command line.
pub(crate) struct Destroy<'a> {
    state_dir: &'a Path,
    /// The variable the command reads the source's connection string from,
    /// or `None` for `--local-state-only`, which connects to nothing.
    database_url_env: Option<&'a str>,
}

impl<'a> Destroy<'a> {
    /// Removes `state_dir` and the publication and replication slot it owns
    /// on the source `database_url_env` names.
    pub(crate) fn reading(state_dir: &'a Path, database_url_env: &'a str) -> Self {
        Self {
            state_dir,
            database_url_env: Some(database_url_env),
        }
    }

    /// Removes `state_dir` alone, for a source that no longer exists.
    pub(crate) fn local_state_only(state_dir: &'a Path) -> Self {
        Self {
            state_dir,
            database_url_env: None,
        }
    }

    /// The remedy for a refusal only this command gets past: run it, then
    /// `rerun`, which prepares the working set anew.
    pub(crate) fn then_rerun(&self, rerun: &str) -> String {
        format!("Run {self}, then rerun {rerun} to prepare the working set anew.")
    }
}

impl fmt::Display for Destroy<'_> {
    /// The state directory is quoted only where a shell would split or expand
    /// it.
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        // Lossy, as `Path::display` is: a message carries only UTF-8.
        let state_dir = self.state_dir.to_string_lossy();
        // `try_quote` refuses only a NUL, which a path given on the command
        // line cannot hold; one that did would print as it is.
        let quoted = shlex::try_quote(&state_dir).unwrap_or_else(|_| state_dir.clone());
        write!(formatter, "calque destroy --state-dir {quoted}")?;
        match self.database_url_env {
            Some(name) => write!(formatter, " --database-url-env {name} --yes"),
            None => formatter.write_str(" --yes --local-state-only"),
        }
    }
}

/// The remedy for state that belongs to another PostgreSQL source than the
/// one `database_url_env` names: reconnect that source, or forget the state.
/// Forgetting it leaves the publication and replication slot on the original
/// source, where the slot retains WAL until someone drops it, so the line
/// says they remain.
pub(crate) fn reconnect_or_forget(state_dir: &Path, database_url_env: &str, rerun: &str) -> String {
    format!(
        "Point {database_url_env} at the persisted source and rerun {rerun}, or run {} and then rerun {rerun}; PostgreSQL objects on the original source will remain.",
        Destroy::local_state_only(state_dir)
    )
}

/// The remedy for the app-owned publication having been altered, which the
/// refusal it follows describes. pgoutput leaves out what an altered
/// definition does not publish, from the alteration's commit on, so the
/// working set is prepared anew even once the definition is back. `calque
/// destroy` drops only a publication it can verify, so the definition is
/// restored first; after `--local-state-only` the next enrollment would take
/// the same publication name and meet it again.
pub(crate) fn restore_publication(destroy: Destroy<'_>, rerun: &str) -> String {
    format!("Restore the publication's definition if it was altered, then run {destroy} and rerun {rerun} to prepare the working set anew, since the one it holds may no longer match PostgreSQL.")
}

/// The remedy a runtime whose CDC loop stopped before readiness names. A
/// cause that needs `calque destroy` gets that command alone, since a rerun
/// meets the same refusal, and a drifted publication gets restoring it, as
/// a start that meets it does; any other cause may not recur, so a rerun
/// comes first.
pub(crate) fn after_stopped_runtime(
    destroy: Destroy<'_>,
    rerun: &str,
    remedy: CdcStopRemedy,
) -> String {
    match remedy {
        CdcStopRemedy::DestroyState => destroy.then_rerun(rerun),
        CdcStopRemedy::RestorePublication => restore_publication(destroy, rerun),
        CdcStopRemedy::Rerun => format!(
            "Rerun {rerun}; if it stops again, run {destroy} and then rerun {rerun} to rebuild from a complete snapshot."
        ),
    }
}

#[cfg(test)]
mod tests {
    use std::path::PathBuf;

    use clap::Parser as _;

    use super::*;
    use crate::cli::{Cli, Command, DestroyCommand};

    /// The command a printed line runs, as a shell splits it and
    /// `calque destroy` parses it.
    fn parsed(line: &str) -> DestroyCommand {
        let words = shlex::split(line).unwrap_or_else(|| panic!("{line} does not split"));
        let cli = Cli::try_parse_from(words).unwrap_or_else(|error| panic!("{line}: {error}"));
        let Command::Destroy(command) = cli.command else {
            panic!("{line} is not a destroy command");
        };
        command
    }

    #[test]
    fn a_printed_destroy_command_parses_to_the_state_it_names() {
        let state_dir = Path::new("/srv/calque/state");

        let line = Destroy::reading(state_dir, "APP_DATABASE_URL").to_string();
        assert_eq!(
            line,
            "calque destroy --state-dir /srv/calque/state --database-url-env APP_DATABASE_URL --yes"
        );
        let command = parsed(&line);
        assert_eq!(command.state_dir, PathBuf::from(state_dir));
        assert_eq!(command.database.database_url_env, "APP_DATABASE_URL");
        assert!(command.yes);
        assert!(!command.local_state_only);

        let line = Destroy::local_state_only(state_dir).to_string();
        assert_eq!(
            line,
            "calque destroy --state-dir /srv/calque/state --yes --local-state-only"
        );
        let command = parsed(&line);
        assert_eq!(command.state_dir, PathBuf::from(state_dir));
        assert!(command.yes);
        assert!(command.local_state_only);
    }

    #[test]
    fn a_state_directory_with_a_space_is_quoted() {
        let state_dir = Path::new("/srv/calque state");
        let line = Destroy::reading(state_dir, "APP_DATABASE_URL").to_string();
        assert_eq!(
            line,
            "calque destroy --state-dir '/srv/calque state' --database-url-env APP_DATABASE_URL --yes"
        );
        assert_eq!(parsed(&line).state_dir, PathBuf::from(state_dir));

        let line = Destroy::local_state_only(state_dir).to_string();
        assert_eq!(
            line,
            "calque destroy --state-dir '/srv/calque state' --yes --local-state-only"
        );
        assert_eq!(parsed(&line).state_dir, PathBuf::from(state_dir));
    }

    #[test]
    fn a_stopped_runtime_names_a_rerun_only_when_the_cause_may_pass() {
        let destroy = || Destroy::reading(Path::new(".calque-dev"), "CALQUE_DATABASE_URL");
        assert_eq!(
            after_stopped_runtime(destroy(), "dev", CdcStopRemedy::DestroyState),
            "Run calque destroy --state-dir .calque-dev --database-url-env CALQUE_DATABASE_URL --yes, then rerun dev to prepare the working set anew."
        );
        assert_eq!(
            after_stopped_runtime(destroy(), "dev", CdcStopRemedy::RestorePublication),
            "Restore the publication's definition if it was altered, then run calque destroy --state-dir .calque-dev --database-url-env CALQUE_DATABASE_URL --yes and rerun dev to prepare the working set anew, since the one it holds may no longer match PostgreSQL."
        );
        assert_eq!(
            after_stopped_runtime(destroy(), "dev", CdcStopRemedy::Rerun),
            "Rerun dev; if it stops again, run calque destroy --state-dir .calque-dev --database-url-env CALQUE_DATABASE_URL --yes and then rerun dev to rebuild from a complete snapshot."
        );
    }
}
