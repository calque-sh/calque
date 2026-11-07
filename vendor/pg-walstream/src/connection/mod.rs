//! PostgreSQL connection backends.
//!
//! This module provides two interchangeable connection implementations,
//! selected at compile time via feature flags:
//!
//! - **`libpq`** (opt-in): Uses the C libpq library via FFI. Requires
//!   `libpq-dev` and `libssl-dev` at build time.
//!
//! - **`rustls-tls`** (default): Pure-Rust implementation using `rustls` with the
//!   `aws-lc-rs` crypto backend for hardware-accelerated TLS (AES-NI, AVX2).
//!   Requires `cmake` + C compiler at build time.
//!   This is the default backend; enable `libpq` instead with
//!   `--no-default-features --features libpq`. When both are enabled,
//!   `rustls-tls` takes priority.
//!
//! Both backends expose the same public types: `PgReplicationConnection` and
//! `PgResult`.

use std::sync::{Arc, OnceLock};

/// Observable durability boundaries for one standby-status update.
///
/// The observer is instrumentation only: it cannot alter bytes, authorize an
/// update, or create another feedback path. Implementations invoke it
/// synchronously on the socket-owning path so process-level tests can stop at
/// the exact encode, write, and flush boundaries.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StandbyStatusUpdateBoundary {
    Encoded,
    BeforeCopyDataQueue,
    CopyDataQueued,
    BeforeTransportFlush,
    TransportFlushed,
}

/// Trusted instrumentation for standby-status update boundaries.
pub trait StandbyStatusUpdateObserver: std::fmt::Debug + Send + Sync + 'static {
    fn observe(&self, boundary: StandbyStatusUpdateBoundary);
}

#[derive(Debug)]
pub(crate) struct NoopStandbyStatusUpdateObserver;

impl StandbyStatusUpdateObserver for NoopStandbyStatusUpdateObserver {
    fn observe(&self, _boundary: StandbyStatusUpdateBoundary) {}
}

pub(crate) fn noop_standby_status_update_observer() -> Arc<dyn StandbyStatusUpdateObserver> {
    static OBSERVER: OnceLock<Arc<dyn StandbyStatusUpdateObserver>> = OnceLock::new();
    Arc::clone(OBSERVER.get_or_init(|| Arc::new(NoopStandbyStatusUpdateObserver)))
}

// ── libpq backend (opt-in) ───────────────────────────────────────────────────

#[cfg(all(feature = "libpq", not(feature = "rustls-tls")))]
mod libpq;

#[cfg(all(feature = "libpq", not(feature = "rustls-tls")))]
pub use libpq::{PgReplicationConnection, PgReplicationShutdownHandle, PgResult};

// ── rustls-tls backend ──────────────────────────────────────────────────────

#[cfg(feature = "rustls-tls")]
pub(crate) mod native;

#[cfg(feature = "rustls-tls")]
pub use native::{
    NativeConnection as PgReplicationConnection, NativePgResult as PgResult,
    NativeShutdownHandle as PgReplicationShutdownHandle,
};
