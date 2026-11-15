//! Rewrites of served columns that change values without a row change.
//!
//! `ALTER COLUMN ... TYPE` to a column's own type with a `USING` expression
//! rewrites every row, and pgoutput sends none of it. The rewrite gives the
//! table a new storage file and writes the column's `pg_attribute` row again,
//! in one transaction. Neither alone changes a value: `VACUUM FULL`,
//! `CLUSTER` and `TRUNCATE` give a new file only, and a grant, statistics
//! target, default or `NOT NULL` writes the column's row only. So a served
//! column whose row version and table file both moved since they were last
//! seen may hold values the working set does not, and stops the source. The
//! two moving apart between two readings stops it too, which is the price of
//! never missing a rewrite.

use std::collections::BTreeMap;

use postgres::GenericClient;
use source_control::RegisteredColumn;

/// A served column's `pg_attribute` row version and its table's file.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ColumnVersion {
    pub relation_oid: u32,
    pub attribute_number: i16,
    pub attribute_xmin: u32,
    pub relation_filenode: u32,
}

impl ColumnVersion {
    fn key(&self) -> (u32, i16) {
        (self.relation_oid, self.attribute_number)
    }
}

/// The current version of every served column that still exists, in one
/// snapshot, so a rewrite's file and row are read together.
pub(crate) fn read_column_versions(
    client: &mut impl GenericClient,
    columns: &[RegisteredColumn],
) -> Result<Vec<ColumnVersion>, postgres::Error> {
    let relations: Vec<u32> = columns.iter().map(|column| column.relation_oid).collect();
    let attributes: Vec<i16> = columns
        .iter()
        .map(|column| column.attribute_number)
        .collect();
    client
        .query(
            "SELECT attribute.attrelid, attribute.attnum, attribute.xmin::text::int8,
                    class.relfilenode
               FROM unnest($1::oid[], $2::int2[]) AS served(relation, attnum)
               JOIN pg_catalog.pg_attribute AS attribute
                 ON attribute.attrelid = served.relation AND attribute.attnum = served.attnum
               JOIN pg_catalog.pg_class AS class ON class.oid = served.relation",
            &[&relations, &attributes],
        )?
        .into_iter()
        .map(|row| {
            Ok(ColumnVersion {
                relation_oid: row.get(0),
                attribute_number: row.get(1),
                attribute_xmin: u32::try_from(row.get::<_, i64>(2)).unwrap_or(u32::MAX),
                relation_filenode: row.get(3),
            })
        })
        .collect()
}

/// The versions last seen of every served column, and whether they differ
/// from the ones last recorded.
#[derive(Debug, Default)]
pub(crate) struct ColumnVersionGuard {
    seen: BTreeMap<(u32, i16), ColumnVersion>,
    unrecorded: bool,
}

impl ColumnVersionGuard {
    pub(crate) fn new(recorded: &[ColumnVersion]) -> Self {
        Self {
            seen: recorded
                .iter()
                .map(|version| (version.key(), *version))
                .collect(),
            unrecorded: false,
        }
    }

    /// The first column `reading` finds rewritten since it was last seen.
    /// Otherwise every version in `reading` becomes the one last seen.
    pub(crate) fn observe(&mut self, reading: &[ColumnVersion]) -> Option<ColumnVersion> {
        if let Some(rewritten) = reading.iter().find(|current| {
            self.seen.get(&current.key()).is_some_and(|seen| {
                seen.attribute_xmin != current.attribute_xmin
                    && seen.relation_filenode != current.relation_filenode
            })
        }) {
            return Some(*rewritten);
        }
        for current in reading {
            if self.seen.insert(current.key(), *current) != Some(*current) {
                self.unrecorded = true;
            }
        }
        None
    }

    /// Every version last seen, when they differ from the ones last recorded;
    /// they count as recorded from then on unless [`Self::record_failed`].
    pub(crate) fn take_unrecorded(&mut self) -> Option<Vec<ColumnVersion>> {
        std::mem::take(&mut self.unrecorded).then(|| self.seen.values().copied().collect())
    }

    pub(crate) fn record_failed(&mut self) {
        self.unrecorded = true;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn version(attribute_xmin: u32, relation_filenode: u32) -> ColumnVersion {
        ColumnVersion {
            relation_oid: 16_384,
            attribute_number: 2,
            attribute_xmin,
            relation_filenode,
        }
    }

    #[test]
    fn a_new_row_version_and_a_new_file_together_are_a_rewrite() {
        let mut guard = ColumnVersionGuard::new(&[version(10, 100)]);
        assert_eq!(guard.observe(&[version(11, 101)]), Some(version(11, 101)));
        assert_eq!(guard.take_unrecorded(), None);
    }

    #[test]
    fn a_new_file_or_a_new_row_version_alone_is_recorded_and_kept() {
        let mut guard = ColumnVersionGuard::new(&[version(10, 100)]);
        assert_eq!(guard.observe(&[version(10, 101)]), None);
        assert_eq!(guard.take_unrecorded(), Some(vec![version(10, 101)]));
        assert_eq!(guard.observe(&[version(11, 101)]), None);
        assert_eq!(guard.take_unrecorded(), Some(vec![version(11, 101)]));
        assert_eq!(guard.observe(&[version(11, 101)]), None);
        assert_eq!(guard.take_unrecorded(), None);
        assert_eq!(guard.observe(&[version(12, 102)]), Some(version(12, 102)));
    }

    #[test]
    fn a_failed_record_is_offered_again() {
        let mut guard = ColumnVersionGuard::new(&[version(10, 100)]);
        guard.observe(&[version(10, 101)]);
        assert!(guard.take_unrecorded().is_some());
        guard.record_failed();
        assert_eq!(guard.take_unrecorded(), Some(vec![version(10, 101)]));
    }
}
