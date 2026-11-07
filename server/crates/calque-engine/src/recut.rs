//! Re-cutting the source-wide artifact once the log has grown past it, and
//! trimming the canonical log behind the oldest cut still in use.
//!
//! A cold load replays every entry appended since the cut it was planned
//! from. Once those entries hold `tail_ratio` bytes per decoded artifact byte,
//! the backend cuts the scope again through the path a fenced generation's
//! publication takes, and the planner hands every later plan the newer cut.
//! Until a plan expires, its catalog pin keeps its cut and its retention
//! lease every entry after it; the log keeps every entry after the newest cut.

use std::{sync::Arc, time::Duration};

use time::{format_description::well_known::Rfc3339, OffsetDateTime};

use artifact_contract::ValidatedArtifactManifest;
use canonical_log::LogError;
use source_control::ActivatedSource;
use sync_types::OpaqueCursor;
use tokio_util::sync::CancellationToken;

use crate::GenerationArtifactPublisher;

/// The tail ratio `calque dev` and `calque serve` re-cut at unless told
/// otherwise: 1/32, the ratio the re-cut's benchmark runs measured. Live
/// updates were slower while re-cuts ran. 0 turns re-cutting off; the log is
/// trimmed either way.
pub const DEFAULT_RECUT_TAIL_RATIO: f64 = 0.03125;
/// The least time from one re-cut's start to the next unless told otherwise.
pub const DEFAULT_RECUT_MIN_INTERVAL: Duration = Duration::from_secs(60);

/// Entries trimmed per retention transaction. A trim locks the head row every
/// append locks, so one chunk is the longest it holds an append back.
pub(crate) const TRIM_CHUNK_RECORDS: u64 = 1_024;
/// How often the log is measured against the newest cut.
pub(crate) const MAINTENANCE_INTERVAL: Duration = Duration::from_secs(1);

#[derive(Debug, Clone, Copy, PartialEq)]
pub struct RecutPolicy {
    /// A re-cut starts once the entries appended since the newest cut hold
    /// this many bytes per decoded byte of its artifact. Zero never re-cuts;
    /// the log is still trimmed.
    pub tail_ratio: f64,
    /// The least time from the newest cut, or from the last re-cut's start,
    /// to the next re-cut.
    pub min_interval: Duration,
}

impl Default for RecutPolicy {
    fn default() -> Self {
        Self {
            tail_ratio: DEFAULT_RECUT_TAIL_RATIO,
            min_interval: DEFAULT_RECUT_MIN_INTERVAL,
        }
    }
}

impl RecutPolicy {
    /// Whether `tail_bytes` since a cut whose artifact decodes to
    /// `artifact_bytes` is due a re-cut.
    pub(crate) fn due(&self, tail_bytes: u64, artifact_bytes: u64) -> bool {
        self.tail_ratio > 0.0 && tail_bytes as f64 >= self.tail_ratio * artifact_bytes as f64
    }

    /// Whether `min_interval` has passed, at `now` in Unix microseconds,
    /// since the newest cut was made and since the last re-cut started
    /// `since_started` ago.
    pub(crate) fn rested(
        &self,
        cut_at: Option<i64>,
        now: i64,
        since_started: Option<Duration>,
    ) -> bool {
        let interval = i64::try_from(self.min_interval.as_micros()).unwrap_or(i64::MAX);
        !cut_at.is_some_and(|at| now.saturating_sub(at) < interval)
            && !since_started.is_some_and(|elapsed| elapsed < self.min_interval)
    }

    /// The tail a re-cut starts at, in bytes; zero when re-cutting is off.
    pub(crate) fn threshold_bytes(&self, artifact_bytes: u64) -> u64 {
        if self.tail_ratio > 0.0 {
            (self.tail_ratio * artifact_bytes as f64).ceil() as u64
        } else {
            0
        }
    }
}

/// What `calque dev` and `calque serve` hand `LocalBackend` to re-cut and
/// trim in the background.
pub struct RecutConfig {
    /// Shared with the fenced generation's publication, so the two never
    /// build at once.
    pub publisher: Arc<GenerationArtifactPublisher>,
    pub policy: RecutPolicy,
    /// The process's own: cancelling it stops the maintenance thread.
    pub cancellation: CancellationToken,
}

/// The newest cut `published` holds, when it was made in Unix
/// microseconds, and the decoded size of its artifact.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct NewestCut {
    pub(crate) cursor: OpaqueCursor,
    pub(crate) sequence: u64,
    pub(crate) cut_at: Option<i64>,
    pub(crate) decoded_bytes: u64,
}

pub(crate) fn newest_cut(
    published: &[ValidatedArtifactManifest],
    source: &ActivatedSource,
) -> Option<NewestCut> {
    published
        .iter()
        .filter_map(|manifest| {
            let cursor = &manifest.manifest().pit.snapshot_cursor;
            let position = source
                .cursor_codec()
                .decode_for_epoch(cursor, source.source_epoch())
                .ok()?;
            let cut_at = OffsetDateTime::parse(&manifest.manifest().created_at, &Rfc3339)
                .ok()
                .and_then(|at| i64::try_from(at.unix_timestamp_nanos() / 1_000).ok());
            Some(NewestCut {
                cursor: cursor.clone(),
                sequence: position.sequence,
                cut_at,
                decoded_bytes: manifest.manifest().decoded.bytes,
            })
        })
        .max_by_key(|cut| cut.sequence)
}

/// The lowest sequence the log must keep: the first entry after the newest
/// cut, which a new plan catches up from, and the first the change tail has
/// not read yet. Each plan's lease keeps the log from its own start.
pub(crate) fn retention_floor(newest_cut: u64, tail_head: Option<u64>) -> u64 {
    tail_head
        .map_or(newest_cut, |tail| tail.min(newest_cut))
        .saturating_add(1)
}

/// Raises the log's retention floor from `retained` toward `floor`, one
/// chunk at a time: `chunk` asks the log to keep nothing below its argument
/// and returns the floor the log then reports. The log clamps each request
/// by the leases it holds, so a chunk that does not move the floor ends the
/// trim.
pub(crate) fn trim(
    retained: u64,
    floor: u64,
    mut chunk: impl FnMut(u64) -> Result<u64, LogError>,
) -> Result<u64, LogError> {
    let mut retained = retained;
    while retained < floor {
        let reached = chunk(retained.saturating_add(TRIM_CHUNK_RECORDS).min(floor))?;
        if reached <= retained {
            break;
        }
        retained = reached;
    }
    Ok(retained)
}

/// The bytes of the entries appended since a cut, read a step at a time.
#[derive(Debug, Default)]
pub(crate) struct TailMeter {
    /// The cut measured from, the sequence measured through, and the bytes.
    measured: Option<(u64, u64, u64)>,
}

impl TailMeter {
    /// The tail's bytes after `cut` through `head`. `bytes_between(after,
    /// through)` reads only the entries not measured yet; a new cut starts
    /// the count again.
    pub(crate) fn measure(
        &mut self,
        cut: u64,
        head: u64,
        bytes_between: impl FnOnce(u64, u64) -> Result<u64, LogError>,
    ) -> Result<u64, LogError> {
        let (through, bytes) = match self.measured {
            Some((measured_cut, through, bytes)) if measured_cut == cut && through <= head => {
                (through, bytes)
            }
            _ => (cut, 0),
        };
        let bytes = if head > through {
            bytes.saturating_add(bytes_between(through, head)?)
        } else {
            bytes
        };
        self.measured = Some((cut, head.max(through), bytes));
        Ok(bytes)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_floor_keeps_the_entry_after_the_newest_cut() {
        assert_eq!(retention_floor(40, None), 41);
        assert_eq!(retention_floor(0, None), 1);
        assert_eq!(retention_floor(u64::MAX, None), u64::MAX);
    }

    #[test]
    fn the_floor_keeps_every_entry_the_change_tail_has_not_read() {
        assert_eq!(retention_floor(40, Some(30)), 31);
        assert_eq!(retention_floor(40, Some(90)), 41);
    }

    #[test]
    fn a_re_cut_is_due_at_its_ratio_and_never_at_zero() {
        let policy = RecutPolicy {
            tail_ratio: 0.25,
            min_interval: Duration::ZERO,
        };
        assert!(!policy.due(249, 1_000));
        assert!(policy.due(250, 1_000));
        assert_eq!(policy.threshold_bytes(1_000), 250);
        let off = RecutPolicy {
            tail_ratio: 0.0,
            ..policy
        };
        assert!(!off.due(u64::MAX, 1));
        assert_eq!(off.threshold_bytes(1_000), 0);
    }

    #[test]
    fn the_default_policy_re_cuts_at_1_32_of_the_artifact_and_rests_a_minute() {
        let policy = RecutPolicy::default();
        assert_eq!(policy.tail_ratio, 1.0 / 32.0);
        assert_eq!(policy.min_interval, Duration::from_secs(60));
        assert!(!policy.due(31, 1_024));
        assert!(policy.due(32, 1_024));
        assert_eq!(policy.threshold_bytes(1_024), 32);
    }

    #[test]
    fn a_re_cut_waits_its_interval_after_the_newest_cut_and_after_the_last_start() {
        let policy = RecutPolicy {
            tail_ratio: 0.25,
            min_interval: Duration::from_secs(60),
        };
        let minute = 60_000_000;
        assert!(policy.rested(None, minute, None));
        assert!(policy.rested(Some(0), minute, None));
        assert!(!policy.rested(Some(1), minute, None));
        assert!(policy.rested(Some(0), minute, Some(Duration::from_secs(60))));
        assert!(!policy.rested(Some(0), minute, Some(Duration::from_secs(59))));
        assert!(!policy.rested(None, minute, Some(Duration::ZERO)));
        let eager = RecutPolicy {
            min_interval: Duration::ZERO,
            ..policy
        };
        assert!(eager.rested(Some(minute), minute, Some(Duration::ZERO)));
    }

    #[test]
    fn the_tail_is_read_once_per_entry_and_again_from_a_new_cut() {
        let mut meter = TailMeter::default();
        let mut reads = Vec::new();
        let mut measure = |meter: &mut TailMeter, cut, head| {
            meter
                .measure(cut, head, |after, through| {
                    reads.push((after, through));
                    Ok((through - after) * 10)
                })
                .unwrap()
        };
        assert_eq!(measure(&mut meter, 100, 100), 0);
        assert_eq!(measure(&mut meter, 100, 140), 400);
        assert_eq!(measure(&mut meter, 100, 150), 500);
        assert_eq!(measure(&mut meter, 100, 150), 500);
        assert_eq!(measure(&mut meter, 150, 152), 20);
        assert_eq!(reads, [(100, 140), (140, 150), (150, 152)]);
    }

    /// A log whose floor moves as asked, never past its head or a lease it
    /// holds, and that can refuse a chunk.
    struct LeasedLog {
        head: u64,
        retained: u64,
        lease: Option<u64>,
        requests: Vec<u64>,
        refuse_at: Option<usize>,
    }

    impl LeasedLog {
        fn new(lease: Option<u64>, refuse_at: Option<usize>) -> Self {
            Self {
                head: 5_000,
                retained: 1,
                lease,
                requests: Vec::new(),
                refuse_at,
            }
        }

        fn trim(&mut self, floor: u64) -> Result<u64, LogError> {
            trim(self.retained, floor, |minimum| {
                if self.refuse_at == Some(self.requests.len()) {
                    return Err(LogError::PostgresUnavailable);
                }
                self.requests.push(minimum);
                let mut retained = minimum.max(self.retained).min(self.head + 1);
                if let Some(lease) = self.lease {
                    retained = retained.min(lease);
                }
                self.retained = retained;
                Ok(retained)
            })
        }
    }

    #[test]
    fn a_trim_moves_in_chunks_and_never_past_its_floor() {
        let mut log = LeasedLog::new(None, None);
        assert_eq!(log.trim(2_500).unwrap(), 2_500);
        assert_eq!(log.requests, [1_025, 2_049, 2_500]);
        assert_eq!(log.trim(2_500).unwrap(), 2_500);
        assert_eq!(log.requests.len(), 3);
    }

    #[test]
    fn a_trim_a_lease_holds_back_stops_at_the_lease() {
        let mut log = LeasedLog::new(Some(1_500), None);
        assert_eq!(log.trim(2_500).unwrap(), 1_500);
        assert_eq!(log.requests, [1_025, 2_049, 2_500]);
    }

    #[test]
    fn a_refused_chunk_ends_the_trim_with_its_error() {
        let mut log = LeasedLog::new(None, Some(1));
        assert!(matches!(
            log.trim(2_500),
            Err(LogError::PostgresUnavailable)
        ));
        assert_eq!(log.retained, 1_025);
    }
}
