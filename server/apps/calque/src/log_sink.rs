//! Where `tracing` writes (stderr) and where `dev` and `serve` write their
//! readiness lines (stdout), each through a writer that never reports a
//! failure.
//!
//! tracing-subscriber reports a failed write with `eprintln!`, which panics
//! when it cannot write either, so once nothing reads stderr the first event a
//! thread logs would unwind that thread, the CDC thread included, before it
//! releases the source lease. `println!` panics the same way, so readiness
//! lines go through [`print_line`].
//!
//! A broken pipe on either latches [`closed`]. `dev` stops on it as on SIGTERM,
//! since whoever started it is gone; `serve` keeps serving, since there it
//! means a log collector failed. Any other write error only loses the line.

use std::{
    fmt::Display,
    io::{self, Write},
    sync::LazyLock,
};

use tokio_util::sync::{CancellationToken, WaitForCancellationFuture};
use tracing_subscriber::fmt::MakeWriter;

static CLOSED: LazyLock<CancellationToken> = LazyLock::new(CancellationToken::new);

/// Resolves once a log line has met a broken pipe on stderr, or a
/// [`print_line`] one on stdout, and at once if one already has.
pub(crate) fn closed() -> WaitForCancellationFuture<'static> {
    CLOSED.cancelled()
}

/// Writes `line` and a newline to stdout, or drops them, closing [`closed`]
/// on a broken pipe. For the lines a long-running command prints before it
/// serves. A one-shot command keeps `println!`, whose panic once the reader
/// has gone ends only that command.
pub(crate) fn print_line(line: impl Display) {
    write_line(io::stdout().lock(), &CLOSED, line);
}

fn write_line(inner: impl Write, closed: &CancellationToken, line: impl Display) {
    let mut writer = SinkWriter { inner, closed };
    // `SinkWriter` reports every write and flush done.
    let _ = writer.write_all(format!("{line}\n").as_bytes());
    let _ = writer.flush();
}

/// The `MakeWriter` `main` installs: stderr, closing [`closed`].
pub(crate) struct Stderr;

impl<'a> MakeWriter<'a> for Stderr {
    type Writer = SinkWriter<'static, io::Stderr>;

    fn make_writer(&'a self) -> Self::Writer {
        SinkWriter {
            inner: io::stderr(),
            closed: &CLOSED,
        }
    }
}

pub(crate) struct SinkWriter<'a, W> {
    inner: W,
    closed: &'a CancellationToken,
}

impl<W: Write> SinkWriter<'_, W> {
    fn dropped(&self, error: &io::Error) {
        if error.kind() == io::ErrorKind::BrokenPipe {
            self.closed.cancel();
        }
    }
}

impl<W: Write> Write for SinkWriter<'_, W> {
    /// Writes all of `buf`, or drops what it could not write, and reports
    /// all of it written either way.
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        if let Err(error) = self.inner.write_all(buf) {
            self.dropped(&error);
        }
        Ok(buf.len())
    }

    fn flush(&mut self) -> io::Result<()> {
        if let Err(error) = self.inner.flush() {
            self.dropped(&error);
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A sink whose every write and flush fails with `kind`.
    struct Failing(io::ErrorKind);

    impl Write for Failing {
        fn write(&mut self, _: &[u8]) -> io::Result<usize> {
            Err(self.0.into())
        }

        fn flush(&mut self) -> io::Result<()> {
            Err(self.0.into())
        }
    }

    #[test]
    fn a_line_that_meets_a_broken_pipe_is_dropped_and_closes_the_sink() {
        let closed = CancellationToken::new();
        let mut writer = SinkWriter {
            inner: Failing(io::ErrorKind::BrokenPipe),
            closed: &closed,
        };
        writer.write_all(b"an event\n").unwrap();
        assert!(closed.is_cancelled());

        let closed = CancellationToken::new();
        let mut writer = SinkWriter {
            inner: Failing(io::ErrorKind::BrokenPipe),
            closed: &closed,
        };
        writer.flush().unwrap();
        assert!(closed.is_cancelled());
    }

    #[test]
    fn any_other_write_error_only_drops_the_line() {
        for kind in [
            io::ErrorKind::Other,
            io::ErrorKind::WriteZero,
            io::ErrorKind::PermissionDenied,
            io::ErrorKind::StorageFull,
        ] {
            let closed = CancellationToken::new();
            let mut writer = SinkWriter {
                inner: Failing(kind),
                closed: &closed,
            };
            writer.write_all(b"an event\n").unwrap();
            writer.flush().unwrap();
            assert!(!closed.is_cancelled(), "{kind:?}");
        }
    }

    #[test]
    fn a_printed_line_that_meets_a_broken_pipe_is_dropped_and_closes_the_sink() {
        let closed = CancellationToken::new();
        write_line(
            Failing(io::ErrorKind::BrokenPipe),
            &closed,
            format_args!("ready at {}", 1),
        );
        assert!(closed.is_cancelled());

        for kind in [
            io::ErrorKind::Other,
            io::ErrorKind::WriteZero,
            io::ErrorKind::PermissionDenied,
            io::ErrorKind::StorageFull,
        ] {
            let closed = CancellationToken::new();
            write_line(Failing(kind), &closed, "ready");
            assert!(!closed.is_cancelled(), "{kind:?}");
        }
    }

    #[test]
    fn a_printed_line_reaches_the_sink_whole_with_its_newline() {
        let closed = CancellationToken::new();
        let mut sink = Vec::new();
        write_line(&mut sink, &closed, format_args!("ready at {}", 1));
        assert_eq!(sink, b"ready at 1\n");
        assert!(!closed.is_cancelled());
    }

    #[test]
    fn a_written_line_reaches_the_sink_whole() {
        let closed = CancellationToken::new();
        let mut writer = SinkWriter {
            inner: Vec::new(),
            closed: &closed,
        };
        writer.write_all(b"an event\n").unwrap();
        assert_eq!(writer.inner, b"an event\n");
        assert!(!closed.is_cancelled());
    }
}
