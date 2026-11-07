use std::{
    sync::{
        atomic::{AtomicBool, AtomicU64, Ordering},
        Arc, Mutex,
    },
    time::{SystemTime, UNIX_EPOCH},
};

use prometheus_client::{
    encoding::{text::encode, EncodeLabelSet, EncodeMetric, MetricEncoder},
    metrics::{
        counter::Counter,
        family::Family,
        gauge::Gauge,
        histogram::{exponential_buckets, Histogram},
        MetricType,
    },
    registry::Registry,
};

pub(crate) type MetricsEncodingError = std::fmt::Error;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CapacityAdmissionOutcome {
    Admitted,
    Rejected,
}

impl CapacityAdmissionOutcome {
    const fn label(self) -> &'static str {
        match self {
            Self::Admitted => "admitted",
            Self::Rejected => "rejected",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OperationErrorClass {
    Cdc,
    CanonicalLog,
    ArtifactCatalog,
    ArtifactBuild,
    Capacity,
    Authorization,
    Internal,
}

impl OperationErrorClass {
    const fn label(self) -> &'static str {
        match self {
            Self::Cdc => "cdc",
            Self::CanonicalLog => "canonical_log",
            Self::ArtifactCatalog => "artifact_catalog",
            Self::ArtifactBuild => "artifact_build",
            Self::Capacity => "capacity",
            Self::Authorization => "authorization",
            Self::Internal => "internal",
        }
    }
}

#[derive(Clone, Debug, Hash, PartialEq, Eq, EncodeLabelSet)]
struct CapacityLabels {
    outcome: &'static str,
}

#[derive(Clone, Debug, Hash, PartialEq, Eq, EncodeLabelSet)]
struct ErrorLabels {
    class: &'static str,
}

/// A metric left out of the scrape, descriptor included, until `measured` is
/// set, the way a labelled family is left out until its first observation.
#[derive(Debug)]
struct UntilMeasured<M> {
    measured: Arc<AtomicBool>,
    metric: M,
}

impl<M: EncodeMetric> EncodeMetric for UntilMeasured<M> {
    fn encode(&self, encoder: MetricEncoder) -> Result<(), std::fmt::Error> {
        self.metric.encode(encoder)
    }

    fn metric_type(&self) -> MetricType {
        self.metric.metric_type()
    }

    fn is_empty(&self) -> bool {
        !self.measured.load(Ordering::Acquire)
    }
}

#[derive(Clone)]
pub struct OperationsMetrics {
    registry: Arc<Mutex<Registry>>,
    canonical_append_duration_seconds: Histogram,
    canonical_entries_appended: Counter,
    cdc_lag_measured: Arc<AtomicBool>,
    cdc_lag_bytes: Gauge,
    cdc_lag_sampled_at: Gauge<f64, AtomicU64>,
    cdc_reconnects: Counter,
    artifact_build_duration_seconds: Histogram,
    artifact_publication_duration_seconds: Histogram,
    artifact_recuts: Counter,
    artifact_recut_in_progress: Gauge,
    artifact_recut_threshold_bytes: Gauge,
    canonical_log_tail_entries: Gauge,
    canonical_log_tail_bytes: Gauge,
    canonical_log_retained_entries: Gauge,
    capacity_admission: Family<CapacityLabels, Counter>,
    errors: Family<ErrorLabels, Counter>,
}

impl std::fmt::Debug for OperationsMetrics {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("OperationsMetrics")
            .finish_non_exhaustive()
    }
}

impl Default for OperationsMetrics {
    fn default() -> Self {
        Self::new()
    }
}

impl OperationsMetrics {
    pub fn new() -> Self {
        let canonical_append_duration_seconds =
            Histogram::new(exponential_buckets(0.000_5, 2.0, 16));
        let canonical_entries_appended = Counter::default();
        let cdc_lag_measured = Arc::new(AtomicBool::new(false));
        let cdc_lag_bytes = Gauge::default();
        let cdc_lag_sampled_at = Gauge::<f64, AtomicU64>::default();
        let cdc_reconnects = Counter::default();
        let artifact_build_duration_seconds = Histogram::new(exponential_buckets(0.01, 2.0, 16));
        let artifact_publication_duration_seconds =
            Histogram::new(exponential_buckets(0.001, 2.0, 16));
        let artifact_recuts = Counter::default();
        let artifact_recut_in_progress = Gauge::default();
        let artifact_recut_threshold_bytes = Gauge::default();
        let canonical_log_tail_entries = Gauge::default();
        let canonical_log_tail_bytes = Gauge::default();
        let canonical_log_retained_entries = Gauge::default();
        let capacity_admission = Family::<CapacityLabels, Counter>::default();
        let errors = Family::<ErrorLabels, Counter>::default();
        let mut registry = Registry::with_prefix("calque");
        registry.register(
            "canonical_append_duration_seconds",
            "Fenced canonical append commit latency in seconds",
            canonical_append_duration_seconds.clone(),
        );
        registry.register(
            "canonical_entries_appended",
            "Canonical-log entries appended, one per source transaction; one fenced append commit carries a group of them",
            canonical_entries_appended.clone(),
        );
        registry.register(
            "cdc_lag_bytes",
            "Replication slot lag in bytes: the current WAL position minus the slot's confirmed flush position",
            UntilMeasured {
                measured: cdc_lag_measured.clone(),
                metric: cdc_lag_bytes.clone(),
            },
        );
        registry.register(
            "cdc_lag_sample_timestamp_seconds",
            "Unix time in seconds of the last successful replication slot lag sample",
            UntilMeasured {
                measured: cdc_lag_measured.clone(),
                metric: cdc_lag_sampled_at.clone(),
            },
        );
        registry.register(
            "cdc_reconnects",
            "Replication sessions re-established after a transient source or control-store failure",
            cdc_reconnects.clone(),
        );
        registry.register(
            "artifact_build_duration_seconds",
            "Snapshot supervisor execution through publication in seconds",
            artifact_build_duration_seconds.clone(),
        );
        registry.register(
            "artifact_publication_duration_seconds",
            "Artifact finalization upload and catalog publication duration in seconds",
            artifact_publication_duration_seconds.clone(),
        );
        registry.register(
            "artifact_recuts",
            "Source-wide artifacts cut again because the canonical log grew past the newest one",
            artifact_recuts.clone(),
        );
        registry.register(
            "artifact_recut_in_progress",
            "1 while a re-cut is built or its artifact re-encoded, else 0",
            artifact_recut_in_progress.clone(),
        );
        registry.register(
            "artifact_recut_threshold_bytes",
            "Canonical-log bytes past the newest cut at which a re-cut starts, or under a partitioned scope the refresh of the tenant the tail gauges follow; 0 when re-cutting is off",
            artifact_recut_threshold_bytes.clone(),
        );
        registry.register(
            "canonical_log_tail_entries",
            "Canonical-log entries appended since the newest artifact's cut, which a cold load replays; under a partitioned scope, since the cut of the warm tenant whose tail is longest against its artifact",
            canonical_log_tail_entries.clone(),
        );
        registry.register(
            "canonical_log_tail_bytes",
            "Bytes of the canonical-log entries appended since the newest artifact's cut; under a partitioned scope, of the operations that tenant receives",
            canonical_log_tail_bytes.clone(),
        );
        registry.register(
            "canonical_log_retained_entries",
            "Canonical-log entries kept, from the retention floor through the head",
            canonical_log_retained_entries.clone(),
        );
        registry.register(
            "capacity_admission",
            "Bootstrap capacity admission decisions",
            capacity_admission.clone(),
        );
        registry.register(
            "errors",
            "Operator-relevant failures by bounded stable class",
            errors.clone(),
        );
        Self {
            registry: Arc::new(Mutex::new(registry)),
            canonical_append_duration_seconds,
            canonical_entries_appended,
            cdc_lag_measured,
            cdc_lag_bytes,
            cdc_lag_sampled_at,
            cdc_reconnects,
            artifact_build_duration_seconds,
            artifact_publication_duration_seconds,
            artifact_recuts,
            artifact_recut_in_progress,
            artifact_recut_threshold_bytes,
            canonical_log_tail_entries,
            canonical_log_tail_bytes,
            canonical_log_retained_entries,
            capacity_admission,
            errors,
        }
    }

    pub fn observe_canonical_append_seconds(&self, seconds: f64) {
        self.canonical_append_duration_seconds.observe(seconds);
    }

    pub fn record_canonical_entries_appended(&self, entries: usize) {
        self.canonical_entries_appended
            .inc_by(u64::try_from(entries).unwrap_or(u64::MAX));
    }

    /// Records a successful slot-lag sample taken at `sampled_at`. Neither the
    /// lag nor its sample time is exported before the first one, so a lag
    /// that was never measured is absent instead of reading 0, and the sample
    /// time shows how old the reading is once later samples fail.
    pub fn record_cdc_lag_sample(&self, bytes: u64, sampled_at: SystemTime) {
        self.cdc_lag_bytes.set(saturating_i64(bytes));
        self.cdc_lag_sampled_at.set(
            sampled_at
                .duration_since(UNIX_EPOCH)
                .unwrap_or_default()
                .as_secs_f64(),
        );
        self.cdc_lag_measured.store(true, Ordering::Release);
    }

    pub fn record_cdc_reconnect(&self) {
        self.cdc_reconnects.inc();
    }

    pub fn observe_artifact_build_seconds(&self, seconds: f64) {
        self.artifact_build_duration_seconds.observe(seconds);
    }

    pub fn observe_artifact_publication_seconds(&self, seconds: f64) {
        self.artifact_publication_duration_seconds.observe(seconds);
    }

    pub fn record_artifact_recut(&self) {
        self.artifact_recuts.inc();
    }

    pub fn record_artifact_recut_in_progress(&self, in_progress: bool) {
        self.artifact_recut_in_progress.set(i64::from(in_progress));
    }

    /// What one maintenance pass measured of the log against the newest cut.
    pub fn record_canonical_log_retention(
        &self,
        tail_entries: u64,
        tail_bytes: u64,
        retained_entries: u64,
        recut_threshold_bytes: u64,
    ) {
        self.artifact_recut_threshold_bytes
            .set(saturating_i64(recut_threshold_bytes));
        self.canonical_log_tail_bytes
            .set(saturating_i64(tail_bytes));
        self.canonical_log_retained_entries
            .set(saturating_i64(retained_entries));
        self.canonical_log_tail_entries
            .set(saturating_i64(tail_entries));
    }

    pub fn record_capacity_admission(&self, outcome: CapacityAdmissionOutcome) {
        self.capacity_admission
            .get_or_create(&CapacityLabels {
                outcome: outcome.label(),
            })
            .inc();
    }

    pub fn record_error(&self, class: OperationErrorClass) {
        self.errors
            .get_or_create(&ErrorLabels {
                class: class.label(),
            })
            .inc();
    }

    pub fn encode(&self) -> Result<String, MetricsEncodingError> {
        let registry = self
            .registry
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let mut output = String::new();
        encode(&mut output, &registry)?;
        Ok(output)
    }
}

fn saturating_i64(value: u64) -> i64 {
    i64::try_from(value).unwrap_or(i64::MAX)
}
