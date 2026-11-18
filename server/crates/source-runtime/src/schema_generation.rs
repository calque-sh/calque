//! The source's schema generation and the boundary a move opens.
//!
//! A move to generation g+1 is recorded under the fence lease before the
//! stream acknowledges anything: `source_schema_generation` names g+1 and the
//! canonical log head it stands at. From then on the ingest appends no data
//! transaction, only cut markers, and acknowledges each skipped transaction
//! once its WAL is behind the durable checkpoint: the g+1 cut, read from
//! PostgreSQL after it, holds its effects. The barrier of the first g+1 cut
//! closes the boundary. Its WAL end is recorded as the one the generation
//! serves from before the barrier is appended, so a crash between the two
//! replays the barrier into a serving source. A replay from the slot's
//! confirmed position skips every data transaction before it, so none lands
//! after the cut out of WAL order.

use std::sync::{Mutex, PoisonError};

use cdc_ingest::{carries_cut_marker, cut_marker_generation};
use pgoutput_decoder::DecodedTransaction;
use postgres::{types::PgLsn, GenericClient};
use sync_types::SourceId;

use crate::{ColumnVersion, ServedEnumLabel};

/// `calque_control.source_schema_generation` for one source.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RecordedSchemaGeneration {
    pub generation: u64,
    /// The canonical log sequence the move's boundary stands at: no entry
    /// after it was appended at an earlier generation.
    pub boundary_sequence: u64,
    /// The WAL end of the barrier that closed the boundary, once it did.
    pub serving_from_lsn: Option<u64>,
}

/// The recorded schema generation of `source_id`, read without a lease, or
/// `None` while no move was ever recorded and the source serves generation 1.
pub fn read_schema_generation(
    client: &mut impl GenericClient,
    source_id: &SourceId,
) -> Result<Option<RecordedSchemaGeneration>, postgres::Error> {
    let installed: bool = client
        .query_one(
            "SELECT to_regclass('calque_control.source_schema_generation') IS NOT NULL",
            &[],
        )?
        .get(0);
    if !installed {
        return Ok(None);
    }
    client
        .query_opt(
            "SELECT schema_generation, boundary_sequence, serving_from_lsn
               FROM calque_control.source_schema_generation
              WHERE source_id = $1",
            &[&source_id.as_str()],
        )?
        .map(recorded_from_row)
        .transpose()
}

pub(crate) fn recorded_from_row(
    row: postgres::Row,
) -> Result<RecordedSchemaGeneration, postgres::Error> {
    let generation: i64 = row.try_get(0)?;
    let boundary_sequence: i64 = row.try_get(1)?;
    let serving_from_lsn: Option<PgLsn> = row.try_get(2)?;
    Ok(RecordedSchemaGeneration {
        generation: generation.unsigned_abs(),
        boundary_sequence: boundary_sequence.unsigned_abs(),
        serving_from_lsn: serving_from_lsn.map(u64::from),
    })
}

/// The shape sessions after a move are checked against, read from the source
/// when the move is recorded: what the enrollment recorded was the earlier
/// generation's, and every session start would refuse the change.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SchemaMoveBaseline {
    pub(crate) publication_fingerprint: String,
    /// The bound of the enum reading `enum_labels` come from. It is at or
    /// before the barrier that closes the boundary, and so before every
    /// change the moved source appends.
    pub(crate) enum_labels_from_lsn: u64,
    pub(crate) enum_labels: Vec<ServedEnumLabel>,
    pub(crate) column_versions: Vec<ColumnVersion>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum BoundaryState {
    /// Inside a recorded move's boundary: data transactions are skipped.
    Open,
    /// Serving: data transactions before this WAL position are replays from
    /// before the barrier that closed the last boundary, and are skipped.
    Serving { from_lsn: u64 },
}

/// Where the ingest stands against the latest schema move. Shared by every
/// session the runtime opens, so a reconnect keeps it.
#[derive(Debug)]
pub struct SchemaBoundary {
    generation: u64,
    state: Mutex<BoundaryState>,
}

/// What one transaction of a group is to the boundary.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum BoundaryStep {
    /// Normalized and appended.
    Append,
    /// Not appended; acknowledged once the checkpoint passes it.
    Skip,
    /// The barrier of the first cut of the generation the boundary waits for:
    /// appended, and the boundary closes once it is durable.
    Close,
}

impl SchemaBoundary {
    /// A source serving `generation`, whose last boundary, if any, closed at
    /// `from_lsn`.
    pub fn serving(generation: u64, from_lsn: Option<u64>) -> Self {
        Self {
            generation,
            state: Mutex::new(BoundaryState::Serving {
                from_lsn: from_lsn.unwrap_or(0),
            }),
        }
    }

    /// A source whose move to `generation` is recorded and whose boundary is
    /// open until that generation's first cut.
    pub fn open(generation: u64) -> Self {
        Self {
            generation,
            state: Mutex::new(BoundaryState::Open),
        }
    }

    pub const fn generation(&self) -> u64 {
        self.generation
    }

    pub fn is_open(&self) -> bool {
        self.state() == BoundaryState::Open
    }

    fn step(&self, transaction: &DecodedTransaction) -> BoundaryStep {
        match self.state() {
            BoundaryState::Open if cut_marker_generation(transaction) == Some(self.generation) => {
                BoundaryStep::Close
            }
            BoundaryState::Open if carries_cut_marker(transaction) => BoundaryStep::Append,
            BoundaryState::Open => BoundaryStep::Skip,
            BoundaryState::Serving { from_lsn }
                if transaction.end_lsn.get() < from_lsn && !carries_cut_marker(transaction) =>
            {
                BoundaryStep::Skip
            }
            BoundaryState::Serving { .. } => BoundaryStep::Append,
        }
    }

    /// The transactions of `group` to append, in order. The barrier that
    /// closes the boundary is recorded through `record_serving` before it is
    /// returned, with every transaction after it in the group.
    pub(crate) fn keep<E>(
        &self,
        group: Vec<DecodedTransaction>,
        mut record_serving: impl FnMut(u64, u64) -> Result<(), E>,
    ) -> Result<Vec<DecodedTransaction>, E> {
        let mut kept = Vec::with_capacity(group.len());
        for transaction in group {
            match self.step(&transaction) {
                BoundaryStep::Skip => {}
                BoundaryStep::Append => kept.push(transaction),
                BoundaryStep::Close => {
                    let from_lsn = transaction.end_lsn.get();
                    record_serving(self.generation, from_lsn)?;
                    tracing::info!(
                        schema_generation = self.generation,
                        serving_from_lsn = %crate::slot_owner::format_lsn(from_lsn),
                        "the stream serves schema generation {} from its first cut",
                        self.generation
                    );
                    self.close(from_lsn);
                    kept.push(transaction);
                }
            }
        }
        Ok(kept)
    }

    /// Closes the boundary at the WAL end of the barrier that closed it.
    fn close(&self, from_lsn: u64) {
        *self.state.lock().unwrap_or_else(PoisonError::into_inner) =
            BoundaryState::Serving { from_lsn };
    }

    fn state(&self) -> BoundaryState {
        *self.state.lock().unwrap_or_else(PoisonError::into_inner)
    }
}

#[cfg(test)]
mod tests {
    use pgoutput_decoder::{
        DecodedChange, LogicalMessage, LogicalMessageKind, PgLsn as DecoderLsn, TransactionMode,
    };

    use super::*;

    fn transaction(end_lsn: u64, changes: Vec<DecodedChange>) -> DecodedTransaction {
        DecodedTransaction {
            xid: 7,
            mode: TransactionMode::Normal,
            first_observed_wal_lsn: DecoderLsn::new(end_lsn - 2),
            final_lsn: None,
            commit_lsn: DecoderLsn::new(end_lsn - 1),
            end_lsn: DecoderLsn::new(end_lsn),
            commit_time_postgres_micros: 0,
            changes,
        }
    }

    fn data(end_lsn: u64) -> DecodedTransaction {
        transaction(
            end_lsn,
            vec![DecodedChange::LogicalMessage(LogicalMessage {
                message_lsn: DecoderLsn::new(end_lsn - 2),
                prefix: "application".to_owned(),
                content: Vec::new(),
                kind: LogicalMessageKind::Other,
            })],
        )
    }

    fn marker(end_lsn: u64, schema_generation: u64) -> DecodedTransaction {
        transaction(
            end_lsn,
            vec![DecodedChange::LogicalMessage(LogicalMessage {
                message_lsn: DecoderLsn::new(end_lsn - 2),
                prefix: pgoutput_decoder::CUT_MARKER_PREFIX.to_owned(),
                content: format!(
                    r#"{{"build_id":"b","nonce":"n","schema_generation":{schema_generation}}}"#
                )
                .into_bytes(),
                kind: LogicalMessageKind::CutMarker,
            })],
        )
    }

    #[test]
    fn an_open_boundary_skips_data_and_keeps_every_cut_marker() {
        let boundary = SchemaBoundary::open(3);
        assert_eq!(boundary.step(&data(10)), BoundaryStep::Skip);
        assert_eq!(boundary.step(&marker(20, 2)), BoundaryStep::Append);
        assert_eq!(boundary.step(&marker(30, 3)), BoundaryStep::Close);
        assert!(boundary.is_open());
    }

    #[test]
    fn a_closed_boundary_skips_only_data_replayed_from_before_its_barrier() {
        let boundary = SchemaBoundary::open(3);
        boundary.close(30);
        assert!(!boundary.is_open());
        assert_eq!(boundary.step(&data(10)), BoundaryStep::Skip);
        assert_eq!(boundary.step(&marker(20, 2)), BoundaryStep::Append);
        assert_eq!(boundary.step(&marker(30, 3)), BoundaryStep::Append);
        assert_eq!(boundary.step(&data(40)), BoundaryStep::Append);
    }

    #[test]
    fn the_closing_barrier_is_recorded_before_it_and_what_follows_are_kept() {
        let boundary = SchemaBoundary::open(3);
        let mut recorded = Vec::new();
        let kept = boundary
            .keep(
                vec![data(10), marker(20, 2), marker(30, 3), data(40)],
                |generation, from_lsn| {
                    recorded.push((generation, from_lsn));
                    Ok::<_, ()>(())
                },
            )
            .unwrap();
        assert_eq!(recorded, [(3, 30)]);
        let ends: Vec<u64> = kept.iter().map(|kept| kept.end_lsn.get()).collect();
        assert_eq!(ends, [20, 30, 40]);
        assert!(!boundary.is_open());
    }

    #[test]
    fn a_failed_record_keeps_the_boundary_open() {
        let boundary = SchemaBoundary::open(3);
        assert_eq!(
            boundary.keep(vec![marker(30, 3), data(40)], |_, _| Err("fenced")),
            Err("fenced")
        );
        assert!(boundary.is_open());
    }

    #[test]
    fn a_source_that_never_moved_appends_everything() {
        let boundary = SchemaBoundary::serving(1, None);
        assert_eq!(boundary.step(&data(10)), BoundaryStep::Append);
        assert_eq!(boundary.step(&marker(20, 1)), BoundaryStep::Append);
    }
}
