use serde::{Deserialize, Serialize};
use std::collections::BTreeSet;
use std::time::Duration;
use thiserror::Error;
use tokio_postgres::Client;

/// The PostgreSQL sessions whose open transactions preserve the exported
/// snapshot while COPY is in progress. The temporary slot's active exporter
/// PID is discovered from PostgreSQL and does not need to be supplied here.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SnapshotPressureTarget {
    temporary_slot_name: String,
    copy_backend_pids: Vec<i32>,
}

impl SnapshotPressureTarget {
    pub fn new(
        temporary_slot_name: impl Into<String>,
        copy_backend_pids: impl IntoIterator<Item = i32>,
    ) -> Result<Self, SnapshotPressureError> {
        let temporary_slot_name = temporary_slot_name.into();
        if !source_control::is_valid_slot_name(&temporary_slot_name) {
            return Err(SnapshotPressureError::InvalidSlotName);
        }

        let copy_backend_pids = copy_backend_pids.into_iter().collect::<BTreeSet<_>>();
        if copy_backend_pids.iter().any(|pid| *pid <= 0) {
            return Err(SnapshotPressureError::InvalidBackendPid);
        }

        Ok(Self {
            temporary_slot_name,
            copy_backend_pids: copy_backend_pids.into_iter().collect(),
        })
    }
}

/// PostgreSQL's retention state for a replication slot.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum SnapshotWalStatus {
    Reserved,
    Extended,
    Unreserved,
    Lost,
}

impl SnapshotWalStatus {
    fn parse(value: &str) -> Result<Self, SnapshotPressureError> {
        match value {
            "reserved" => Ok(Self::Reserved),
            "extended" => Ok(Self::Extended),
            "unreserved" => Ok(Self::Unreserved),
            "lost" => Ok(Self::Lost),
            _ => Err(SnapshotPressureError::UnknownWalStatus),
        }
    }
}

/// Sanitized pressure facts for one active snapshot build. These values contain
/// no connection strings, credentials, SQL, or application row data.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct SnapshotPressureSample {
    pub retained_wal_bytes: u64,
    pub catalog_xid_age: u64,
    /// Oldest visible transaction among the temporary exporter and declared
    /// COPY backends. PostgreSQL's walsender may retain the exported snapshot
    /// without exposing `xact_start`, so this is `None` after COPY commits.
    pub oldest_copy_or_export_transaction_millis: Option<u64>,
    pub wal_status: SnapshotWalStatus,
    /// `None` means PostgreSQL is configured without a finite
    /// `max_slot_wal_keep_size` limit.
    pub safe_wal_size_bytes: Option<u64>,
    pub exporter_backend_pid: i32,
    pub copy_backend_count: u32,
}

/// Warning and hard upper bounds for a monotonically increasing pressure
/// metric such as retained bytes or transaction-ID age.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SnapshotPressureThresholds {
    warning: u64,
    hard: u64,
}

impl SnapshotPressureThresholds {
    pub fn new(warning: u64, hard: u64) -> Result<Self, SnapshotPressureError> {
        if warning == 0 || hard == 0 || warning > hard {
            return Err(SnapshotPressureError::InvalidThresholds);
        }
        Ok(Self { warning, hard })
    }

    fn classify(self, observed: u64) -> Option<(SnapshotPressureLevel, u64)> {
        if observed >= self.hard {
            Some((SnapshotPressureLevel::HardLimit, self.hard))
        } else if observed >= self.warning {
            Some((SnapshotPressureLevel::Warning, self.warning))
        } else {
            None
        }
    }
}

/// Warning and hard lower bounds for PostgreSQL's remaining safe WAL bytes.
/// The hard floor must be less than or equal to the warning floor.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SnapshotRemainingWalThresholds {
    warning_below: u64,
    hard_below: u64,
}

impl SnapshotRemainingWalThresholds {
    pub fn new(warning_below: u64, hard_below: u64) -> Result<Self, SnapshotPressureError> {
        if warning_below == 0 || hard_below > warning_below {
            return Err(SnapshotPressureError::InvalidThresholds);
        }
        Ok(Self {
            warning_below,
            hard_below,
        })
    }

    fn classify(self, observed: u64) -> Option<(SnapshotPressureLevel, u64)> {
        if observed <= self.hard_below {
            Some((SnapshotPressureLevel::HardLimit, self.hard_below))
        } else if observed <= self.warning_below {
            Some((SnapshotPressureLevel::Warning, self.warning_below))
        } else {
            None
        }
    }
}

/// Composition-owned policy. There are no default limits: a deployment chooses
/// them for its WAL budget, transaction-ID policy, and maximum build duration.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SnapshotPressurePolicy {
    retained_wal_bytes: SnapshotPressureThresholds,
    catalog_xid_age: SnapshotPressureThresholds,
    transaction_age_millis: SnapshotPressureThresholds,
    remaining_safe_wal_bytes: SnapshotRemainingWalThresholds,
}

impl SnapshotPressurePolicy {
    pub fn new(
        retained_wal_bytes: SnapshotPressureThresholds,
        catalog_xid_age: SnapshotPressureThresholds,
        transaction_age_warning: Duration,
        transaction_age_hard: Duration,
        remaining_safe_wal_bytes: SnapshotRemainingWalThresholds,
    ) -> Result<Self, SnapshotPressureError> {
        let warning = duration_millis(transaction_age_warning)?;
        let hard = duration_millis(transaction_age_hard)?;
        Ok(Self {
            retained_wal_bytes,
            catalog_xid_age,
            transaction_age_millis: SnapshotPressureThresholds::new(warning, hard)?,
            remaining_safe_wal_bytes,
        })
    }

    pub fn evaluate(self, sample: &SnapshotPressureSample) -> SnapshotPressureEvaluation {
        let mut reasons = Vec::new();

        if let Some((level, limit)) = self.retained_wal_bytes.classify(sample.retained_wal_bytes) {
            reasons.push(SnapshotPressureReason::RetainedWalBytes {
                level,
                observed: sample.retained_wal_bytes,
                limit,
            });
        }
        if let Some((level, limit)) = self.catalog_xid_age.classify(sample.catalog_xid_age) {
            reasons.push(SnapshotPressureReason::CatalogXidAge {
                level,
                observed: sample.catalog_xid_age,
                limit,
            });
        }
        if let Some(observed) = sample.oldest_copy_or_export_transaction_millis {
            if let Some((level, limit)) = self.transaction_age_millis.classify(observed) {
                reasons.push(SnapshotPressureReason::TransactionAgeMillis {
                    level,
                    observed,
                    limit,
                });
            }
        }
        if let Some(remaining) = sample.safe_wal_size_bytes {
            if let Some((level, floor)) = self.remaining_safe_wal_bytes.classify(remaining) {
                reasons.push(SnapshotPressureReason::RemainingSafeWalBytes {
                    level,
                    observed: remaining,
                    floor,
                });
            }
        }
        match sample.wal_status {
            SnapshotWalStatus::Reserved => {}
            SnapshotWalStatus::Extended => {
                reasons.push(SnapshotPressureReason::WalStatus {
                    level: SnapshotPressureLevel::Warning,
                    status: sample.wal_status,
                });
            }
            SnapshotWalStatus::Unreserved | SnapshotWalStatus::Lost => {
                reasons.push(SnapshotPressureReason::WalStatus {
                    level: SnapshotPressureLevel::HardLimit,
                    status: sample.wal_status,
                });
            }
        }

        let level = reasons
            .iter()
            .map(SnapshotPressureReason::level)
            .max()
            .unwrap_or(SnapshotPressureLevel::Healthy);
        SnapshotPressureEvaluation { level, reasons }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum SnapshotPressureLevel {
    Healthy,
    Warning,
    HardLimit,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "metric", rename_all = "SCREAMING_SNAKE_CASE")]
pub enum SnapshotPressureReason {
    RetainedWalBytes {
        level: SnapshotPressureLevel,
        observed: u64,
        limit: u64,
    },
    CatalogXidAge {
        level: SnapshotPressureLevel,
        observed: u64,
        limit: u64,
    },
    TransactionAgeMillis {
        level: SnapshotPressureLevel,
        observed: u64,
        limit: u64,
    },
    RemainingSafeWalBytes {
        level: SnapshotPressureLevel,
        observed: u64,
        floor: u64,
    },
    WalStatus {
        level: SnapshotPressureLevel,
        status: SnapshotWalStatus,
    },
}

impl SnapshotPressureReason {
    pub const fn level(&self) -> SnapshotPressureLevel {
        match self {
            Self::RetainedWalBytes { level, .. }
            | Self::CatalogXidAge { level, .. }
            | Self::TransactionAgeMillis { level, .. }
            | Self::RemainingSafeWalBytes { level, .. }
            | Self::WalStatus { level, .. } => *level,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct SnapshotPressureEvaluation {
    pub level: SnapshotPressureLevel,
    pub reasons: Vec<SnapshotPressureReason>,
}

/// Samples PostgreSQL-owned facts for a running snapshot. Missing or changed
/// slot/session state is an error rather than an apparently healthy sample.
pub async fn sample_snapshot_pressure(
    client: &Client,
    target: &SnapshotPressureTarget,
) -> Result<SnapshotPressureSample, SnapshotPressureError> {
    let row = client
        .query_opt(
            "SELECT temporary,
                    plugin,
                    database = current_database(),
                    active_pid,
                    CASE WHEN restart_lsn IS NULL THEN NULL
                         ELSE GREATEST(
                           pg_catalog.pg_wal_lsn_diff(pg_catalog.pg_current_wal_lsn(), restart_lsn),
                           0
                         )::text
                    END,
                    CASE WHEN catalog_xmin IS NULL THEN NULL
                         ELSE pg_catalog.age(catalog_xmin)::text
                    END,
                    wal_status,
                    safe_wal_size::text
               FROM pg_catalog.pg_replication_slots
              WHERE slot_name = $1",
            &[&target.temporary_slot_name],
        )
        .await?
        .ok_or(SnapshotPressureError::TemporarySlotMissing)?;

    let temporary: bool = row.try_get(0)?;
    let plugin: String = row.try_get(1)?;
    let current_database: Option<bool> = row.try_get(2)?;
    if !temporary || plugin != "pgoutput" || current_database != Some(true) {
        return Err(SnapshotPressureError::UnexpectedSlotMetadata);
    }

    let exporter_backend_pid: Option<i32> = row.try_get(3)?;
    let exporter_backend_pid =
        exporter_backend_pid.ok_or(SnapshotPressureError::TemporarySlotInactive)?;
    let retained_wal_bytes = parse_required_metric(row.try_get(4)?, "retained_wal_bytes")?;
    let catalog_xid_age = parse_required_metric(row.try_get(5)?, "catalog_xid_age")?;
    let wal_status: Option<String> = row.try_get(6)?;
    let wal_status = SnapshotWalStatus::parse(
        wal_status
            .as_deref()
            .ok_or(SnapshotPressureError::MissingMetric("wal_status"))?,
    )?;
    let safe_wal_size_bytes = parse_optional_metric(row.try_get(7)?, "safe_wal_size")?;

    let mut expected_pids = target
        .copy_backend_pids
        .iter()
        .copied()
        .collect::<BTreeSet<_>>();
    expected_pids.insert(exporter_backend_pid);
    let expected_pids = expected_pids.into_iter().collect::<Vec<_>>();
    let activity_rows = client
        .query(
            "SELECT pid,
                    CASE WHEN xact_start IS NULL THEN NULL
                         ELSE GREATEST(
                           floor(EXTRACT(EPOCH FROM (clock_timestamp() - xact_start)) * 1000),
                           0
                         )::text
                    END
               FROM pg_catalog.pg_stat_activity
              WHERE pid = ANY($1::int4[])",
            &[&expected_pids],
        )
        .await?;

    let mut observed_pids = BTreeSet::new();
    let mut oldest_transaction_millis = None::<u64>;
    for activity in activity_rows {
        let pid: i32 = activity.try_get(0)?;
        if !observed_pids.insert(pid) {
            return Err(SnapshotPressureError::DuplicateBackendActivity);
        }
        let age = parse_optional_metric(activity.try_get(1)?, "transaction_age_millis")?;
        if let Some(age) = age {
            oldest_transaction_millis = Some(oldest_transaction_millis.unwrap_or(0).max(age));
        }
    }

    if let Some(pid) = expected_pids
        .iter()
        .find(|pid| !observed_pids.contains(pid))
    {
        return Err(SnapshotPressureError::BackendMissing { pid: *pid });
    }

    Ok(SnapshotPressureSample {
        retained_wal_bytes,
        catalog_xid_age,
        oldest_copy_or_export_transaction_millis: oldest_transaction_millis,
        wal_status,
        safe_wal_size_bytes,
        exporter_backend_pid,
        copy_backend_count: u32::try_from(target.copy_backend_pids.len())
            .map_err(|_| SnapshotPressureError::TooManyBackendPids)?,
    })
}

#[derive(Debug, Error)]
pub enum SnapshotPressureError {
    #[error("temporary replication slot name is invalid")]
    InvalidSlotName,
    #[error("snapshot backend PID must be positive")]
    InvalidBackendPid,
    #[error("snapshot pressure warning/hard thresholds are invalid")]
    InvalidThresholds,
    #[error("snapshot pressure duration cannot be represented in milliseconds")]
    DurationOverflow,
    #[error("PostgreSQL pressure sampling failed")]
    Postgres(#[from] tokio_postgres::Error),
    #[error("temporary snapshot slot is missing")]
    TemporarySlotMissing,
    #[error("temporary snapshot slot metadata is not the expected local pgoutput slot")]
    UnexpectedSlotMetadata,
    #[error("temporary snapshot slot has no active exporter backend")]
    TemporarySlotInactive,
    #[error("PostgreSQL pressure metric {0} is missing")]
    MissingMetric(&'static str),
    #[error("PostgreSQL pressure metric {0} is invalid")]
    InvalidMetric(&'static str),
    #[error("PostgreSQL returned an unknown WAL retention status")]
    UnknownWalStatus,
    #[error("snapshot backend {pid} disappeared while pressure was sampled")]
    BackendMissing { pid: i32 },
    #[error("PostgreSQL returned duplicate snapshot backend activity")]
    DuplicateBackendActivity,
    #[error("snapshot backend count cannot be represented")]
    TooManyBackendPids,
}

fn parse_required_metric(
    value: Option<String>,
    field: &'static str,
) -> Result<u64, SnapshotPressureError> {
    parse_optional_metric(value, field)?.ok_or(SnapshotPressureError::MissingMetric(field))
}

fn parse_optional_metric(
    value: Option<String>,
    field: &'static str,
) -> Result<Option<u64>, SnapshotPressureError> {
    value
        .map(|value| {
            value
                .parse::<u64>()
                .map_err(|_| SnapshotPressureError::InvalidMetric(field))
        })
        .transpose()
}

fn duration_millis(duration: Duration) -> Result<u64, SnapshotPressureError> {
    u64::try_from(duration.as_millis()).map_err(|_| SnapshotPressureError::DurationOverflow)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn thresholds(warning: u64, hard: u64) -> SnapshotPressureThresholds {
        SnapshotPressureThresholds::new(warning, hard).unwrap()
    }

    fn remaining(warning: u64, hard: u64) -> SnapshotRemainingWalThresholds {
        SnapshotRemainingWalThresholds::new(warning, hard).unwrap()
    }

    fn policy() -> SnapshotPressurePolicy {
        SnapshotPressurePolicy::new(
            thresholds(1_000, 2_000),
            thresholds(100, 200),
            Duration::from_millis(500),
            Duration::from_millis(1_000),
            remaining(500, 100),
        )
        .unwrap()
    }

    fn sample() -> SnapshotPressureSample {
        SnapshotPressureSample {
            retained_wal_bytes: 999,
            catalog_xid_age: 99,
            oldest_copy_or_export_transaction_millis: Some(499),
            wal_status: SnapshotWalStatus::Reserved,
            safe_wal_size_bytes: Some(501),
            exporter_backend_pid: 10,
            copy_backend_count: 2,
        }
    }

    #[test]
    fn target_validates_and_deduplicates_backend_pids() {
        let target = SnapshotPressureTarget::new("snapshot_1", [11, 10, 11]).unwrap();
        assert_eq!(target.temporary_slot_name, "snapshot_1");
        assert_eq!(target.copy_backend_pids, [10, 11]);
        assert!(matches!(
            SnapshotPressureTarget::new("bad-slot", []),
            Err(SnapshotPressureError::InvalidSlotName)
        ));
        assert!(matches!(
            SnapshotPressureTarget::new("Snapshot_1", []),
            Err(SnapshotPressureError::InvalidSlotName)
        ));
        assert!(matches!(
            SnapshotPressureTarget::new("snapshot_1", [0]),
            Err(SnapshotPressureError::InvalidBackendPid)
        ));
    }

    #[test]
    fn threshold_constructors_reject_ambiguous_policies() {
        assert!(matches!(
            SnapshotPressureThresholds::new(0, 10),
            Err(SnapshotPressureError::InvalidThresholds)
        ));
        assert!(matches!(
            SnapshotPressureThresholds::new(11, 10),
            Err(SnapshotPressureError::InvalidThresholds)
        ));
        assert!(matches!(
            SnapshotRemainingWalThresholds::new(0, 0),
            Err(SnapshotPressureError::InvalidThresholds)
        ));
        assert!(matches!(
            SnapshotRemainingWalThresholds::new(100, 101),
            Err(SnapshotPressureError::InvalidThresholds)
        ));
        assert!(matches!(
            SnapshotPressurePolicy::new(
                thresholds(1, 2),
                thresholds(1, 2),
                Duration::ZERO,
                Duration::from_secs(1),
                remaining(1, 0),
            ),
            Err(SnapshotPressureError::InvalidThresholds)
        ));
    }

    #[test]
    fn healthy_sample_has_no_reasons() {
        let evaluation = policy().evaluate(&sample());
        assert_eq!(evaluation.level, SnapshotPressureLevel::Healthy);
        assert!(evaluation.reasons.is_empty());
    }

    #[test]
    fn warning_boundaries_are_inclusive_and_complete() {
        let mut sample = sample();
        sample.retained_wal_bytes = 1_000;
        sample.catalog_xid_age = 100;
        sample.oldest_copy_or_export_transaction_millis = Some(500);
        sample.safe_wal_size_bytes = Some(500);
        sample.wal_status = SnapshotWalStatus::Extended;

        let evaluation = policy().evaluate(&sample);
        assert_eq!(evaluation.level, SnapshotPressureLevel::Warning);
        assert_eq!(evaluation.reasons.len(), 5);
        assert!(evaluation
            .reasons
            .iter()
            .all(|reason| reason.level() == SnapshotPressureLevel::Warning));
    }

    #[test]
    fn every_hard_boundary_reports_a_hard_limit_with_the_full_evaluation() {
        let mut sample = sample();
        sample.retained_wal_bytes = 2_000;
        sample.catalog_xid_age = 200;
        sample.oldest_copy_or_export_transaction_millis = Some(1_000);
        sample.safe_wal_size_bytes = Some(100);
        sample.wal_status = SnapshotWalStatus::Lost;

        let evaluation = policy().evaluate(&sample);
        assert_eq!(evaluation.level, SnapshotPressureLevel::HardLimit);
        assert_eq!(evaluation.reasons.len(), 5);
        assert!(evaluation
            .reasons
            .iter()
            .all(|reason| reason.level() == SnapshotPressureLevel::HardLimit));
    }

    #[test]
    fn unreserved_wal_is_hard_even_without_a_finite_safe_wal_limit() {
        let mut sample = sample();
        sample.safe_wal_size_bytes = None;
        sample.wal_status = SnapshotWalStatus::Unreserved;

        assert_eq!(
            policy().evaluate(&sample).level,
            SnapshotPressureLevel::HardLimit
        );
    }

    #[test]
    fn numeric_parsing_rejects_missing_negative_and_overflow_values() {
        assert!(matches!(
            parse_required_metric(None, "test"),
            Err(SnapshotPressureError::MissingMetric("test"))
        ));
        for invalid in ["-1", "18446744073709551616", "one"] {
            assert!(matches!(
                parse_required_metric(Some(invalid.to_owned()), "test"),
                Err(SnapshotPressureError::InvalidMetric("test"))
            ));
        }
    }

    #[test]
    fn pressure_wire_facts_are_sanitized_and_round_trip() {
        let encoded = serde_json::to_vec(&sample()).unwrap();
        let decoded: SnapshotPressureSample = serde_json::from_slice(&encoded).unwrap();
        assert_eq!(decoded, sample());
        let text = String::from_utf8(encoded).unwrap();
        assert!(!text.contains("postgresql://"));
        assert!(!text.contains("SELECT"));
    }
}
