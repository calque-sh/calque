//! Fail-closed normalization of `pgoutput` events into complete transactions.
//!
//! PostgreSQL wire decoding is delegated to `pg_walstream`. This crate owns the
//! engine boundary above it: limits, relation layout versioning, patch semantics,
//! transactional logical messages, and normal/streamed transaction assembly.
//! No row change is returned before its transaction commits.

mod decoder;
mod launch_profile;
mod model;

pub use decoder::Decoder;
pub use launch_profile::{PgoutputLaunchProfile, PgoutputProfileError};
pub use model::*;
