use std::time::Duration;

use authz::CanonicalAuthorizationError;
use canonical_log::CanonicalEntry;
use cdc_ingest::{NormalizationError, SchemaNormalizer};
use pgoutput_decoder::DecoderLimits;
use postgres_types::PgLsn;
use snapshot_coordinator::{CutEmission, CutObservation, RollForwardError, SnapshotCoordinator};
use source_control::SchemaRegistry;
use sync_types::SourceEpoch;
use thiserror::Error;
use tokio_util::sync::CancellationToken;

use crate::{
    direct_overlay::{DirectOverlay, DirectOverlayError},
    SnapshotBuildPlan,
};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SnapshotRollForwardStats {
    pub cut: CutObservation,
    pub decoded_transactions: u64,
    pub canonical_transactions: u64,
    pub canonical_barriers: u64,
    pub operations: u64,
    pub rows_changed: u64,
    pub last_applied_lsn: PgLsn,
}

#[derive(Debug, Error)]
pub enum SnapshotApplyError {
    #[error(transparent)]
    Normalize(#[from] NormalizationError),
    #[error(transparent)]
    Overlay(#[from] DirectOverlayError),
    #[error(transparent)]
    Authorization(#[from] CanonicalAuthorizationError),
    #[error("snapshot roll-forward statistic overflow")]
    StatisticsOverflow,
}

#[derive(Debug, Error)]
pub enum SnapshotRollForwardError {
    #[error("failed to create registry-backed canonical normalizer: {0}")]
    Normalizer(#[from] NormalizationError),
    #[error(transparent)]
    Stream(#[from] RollForwardError<SnapshotApplyError>),
}

/// Replays the temporary logical stream into a direct build's overlay through
/// the same registry-backed canonical codec and authorization routing as
/// persistent CDC. The coordinator verifies the cut transaction and never
/// applies it. Cancellation never returns a cut, so it cannot advance the
/// build to persistent-barrier mapping or publication.
#[allow(clippy::too_many_arguments)]
pub(crate) async fn roll_forward_overlay_to_cut_controlled(
    coordinator: &mut SnapshotCoordinator,
    expected: &CutEmission,
    timeout: Duration,
    decoder_limits: DecoderLimits,
    registry: &SchemaRegistry,
    plan: &SnapshotBuildPlan,
    overlay: &mut DirectOverlay,
    source_epoch: &SourceEpoch,
    cancellation: &CancellationToken,
) -> Result<SnapshotRollForwardStats, SnapshotRollForwardError> {
    ensure_same_generation(plan, registry)?;
    let authorized_scope = plan.authorized_scope();
    let mut normalizer = SchemaNormalizer::from_registry_dynamic(registry)?;
    let mut decoded_transactions = 0_u64;
    let mut canonical_transactions = 0_u64;
    let mut canonical_barriers = 0_u64;
    let mut operations = 0_u64;

    let cut = coordinator
        .roll_forward_to_cut_controlled(
            expected,
            timeout,
            decoder_limits,
            cancellation,
            |decoded| {
                decoded_transactions = checked_increment(decoded_transactions)?;
                match normalizer.normalize_committed(decoded, source_epoch)? {
                    CanonicalEntry::Transaction(transaction) => {
                        let transaction = authorized_scope.route_transaction(&transaction)?;
                        let applied = overlay.apply_transaction(transaction)?;
                        canonical_transactions = checked_increment(canonical_transactions)?;
                        operations = operations
                            .checked_add(applied.operations)
                            .ok_or(SnapshotApplyError::StatisticsOverflow)?;
                    }
                    CanonicalEntry::Barrier(_) => {
                        canonical_barriers = checked_increment(canonical_barriers)?;
                    }
                }
                Ok(())
            },
        )
        .await?;

    Ok(SnapshotRollForwardStats {
        cut,
        decoded_transactions,
        canonical_transactions,
        canonical_barriers,
        operations,
        // The overlay changes exactly one row per operation.
        rows_changed: operations,
        last_applied_lsn: overlay.last_end_lsn(),
    })
}

fn ensure_same_generation(
    plan: &SnapshotBuildPlan,
    registry: &SchemaRegistry,
) -> Result<(), SnapshotRollForwardError> {
    if plan.schema_generation() == registry.schema_generation() {
        Ok(())
    } else {
        Err(SnapshotRollForwardError::Normalizer(
            NormalizationError::new("snapshot plan and schema registry generations differ"),
        ))
    }
}

fn checked_increment(value: u64) -> Result<u64, SnapshotApplyError> {
    value
        .checked_add(1)
        .ok_or(SnapshotApplyError::StatisticsOverflow)
}
