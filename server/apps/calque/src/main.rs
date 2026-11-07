//! Calque command-line entry point.

use std::{
    io::{self, IsTerminal as _, Write as _},
    process::ExitCode,
};

use tracing_subscriber::{fmt, EnvFilter};

mod cli;
mod dev;
mod init;
mod inspect;
mod lifecycle;
mod listen;
mod log_sink;
mod open_files;
mod recovery;
mod remedy;
mod serve;

fn main() -> ExitCode {
    init_tracing();
    match cli::run() {
        Ok(()) => ExitCode::SUCCESS,
        Err(error) => {
            // Not `eprintln!`, which panics when nothing reads stderr.
            let _ = writeln!(io::stderr(), "calque: {}", describe(error.as_ref()));
            ExitCode::FAILURE
        }
    }
}

/// The error and each cause whose message it does not already include. Most
/// errors here embed their source's message; a driver's often does not, and
/// its cause is the part that says what to fix, such as which certificate a
/// TLS handshake refused.
fn describe(error: &dyn std::error::Error) -> String {
    let mut message = error.to_string();
    let mut cause = error.source();
    while let Some(error) = cause {
        let text = error.to_string();
        if !message.contains(&text) {
            message.push_str(": ");
            message.push_str(&text);
        }
        cause = error.source();
    }
    message
}

/// Writes the `tracing` spans and events the HTTP crates already emit to
/// stderr; stdout is reserved for each command's own output. `RUST_LOG` is
/// honored when set; otherwise the level is `info`, and `warn` for the
/// replication client, which logs every idle poll at `info` twice a second.
/// Colors are written only to a terminal, so a redirected log holds plain
/// text. A line stderr cannot take is dropped, and a broken pipe stops `dev`
/// (`log_sink`).
fn init_tracing() {
    let filter = EnvFilter::try_from_default_env()
        .unwrap_or_else(|_| EnvFilter::new("info,pg_walstream=warn"));
    fmt()
        .with_env_filter(filter)
        .with_ansi(io::stderr().is_terminal())
        .with_writer(log_sink::Stderr)
        .init();
}
