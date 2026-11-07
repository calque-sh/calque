use std::time::{Duration, UNIX_EPOCH};

use calque_engine::{CapacityAdmissionOutcome, OperationErrorClass, OperationsMetrics};

#[test]
fn openmetrics_surface_exposes_bounded_operator_signals() {
    let metrics = OperationsMetrics::new();
    metrics.observe_canonical_append_seconds(0.012);
    metrics.record_canonical_entries_appended(128);
    metrics.record_cdc_lag_sample(41, UNIX_EPOCH + Duration::from_secs(1_700_000_000));
    metrics.record_cdc_reconnect();
    metrics.observe_artifact_build_seconds(2.5);
    metrics.observe_artifact_publication_seconds(0.4);
    metrics.record_artifact_recut();
    metrics.record_artifact_recut_in_progress(true);
    metrics.record_canonical_log_retention(2_500, 8_300_000, 3_100, 7_766_505);
    metrics.record_capacity_admission(CapacityAdmissionOutcome::Admitted);
    metrics.record_capacity_admission(CapacityAdmissionOutcome::Rejected);
    metrics.record_error(OperationErrorClass::CanonicalLog);

    let encoded = metrics.encode().unwrap();
    // The whole set of families: adding or removing one changes this list,
    // and operations.md's metrics table with it.
    let families: Vec<&str> = encoded
        .lines()
        .filter_map(|line| line.strip_prefix("# TYPE "))
        .filter_map(|declaration| declaration.split(' ').next())
        .collect();
    assert_eq!(
        families,
        [
            "calque_canonical_append_duration_seconds",
            "calque_canonical_entries_appended",
            "calque_cdc_lag_bytes",
            "calque_cdc_lag_sample_timestamp_seconds",
            "calque_cdc_reconnects",
            "calque_artifact_build_duration_seconds",
            "calque_artifact_publication_duration_seconds",
            "calque_artifact_recuts",
            "calque_artifact_recut_in_progress",
            "calque_artifact_recut_threshold_bytes",
            "calque_canonical_log_tail_entries",
            "calque_canonical_log_tail_bytes",
            "calque_canonical_log_retained_entries",
            "calque_capacity_admission",
            "calque_errors",
        ],
        "{encoded}"
    );
    assert!(encoded.contains("outcome=\"admitted\""));
    assert!(encoded.contains("outcome=\"rejected\""));
    assert!(encoded.contains("class=\"canonical_log\""));
    assert!(encoded.contains("calque_cdc_lag_bytes 41\n"));
    assert!(encoded.contains("calque_cdc_lag_sample_timestamp_seconds 1700000000.0\n"));
    assert!(encoded.contains("calque_cdc_reconnects_total 1\n"));
    assert!(encoded.contains("calque_canonical_entries_appended_total 128\n"));
    assert!(encoded.contains("calque_canonical_append_duration_seconds_count 1\n"));
    assert!(encoded.contains("calque_artifact_recuts_total 1\n"));
    assert!(encoded.contains("calque_artifact_recut_in_progress 1\n"));
    assert!(encoded.contains("calque_artifact_recut_threshold_bytes 7766505\n"));
    assert!(encoded.contains("calque_canonical_log_tail_entries 2500\n"));
    assert!(encoded.contains("calque_canonical_log_tail_bytes 8300000\n"));
    assert!(encoded.contains("calque_canonical_log_retained_entries 3100\n"));
    assert!(encoded.ends_with("# EOF\n"));
}
