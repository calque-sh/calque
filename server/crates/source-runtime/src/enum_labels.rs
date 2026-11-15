//! Which value of an enum each label in the working set names.
//!
//! pgoutput and `COPY` write an enum value as the label `pg_enum` holds for
//! it when they write it, so renaming a value changes the text of every row
//! holding it without a row change. Calque records, for every label a served
//! row may carry, the `pg_enum` row that holds it, and stops once that row
//! changes. Adding a value writes a new row and changes no recorded one.
//!
//! A change's label is served only once Calque proves which value held it at
//! the change's WAL position. A reading of `pg_enum` returns
//! `pg_current_wal_insert_lsn()` from the same statement, its bound: every
//! row version the reading saw had committed before it. A version a reading
//! saw at or before a change's position was visible to the change, and so
//! was its label, as long as the version is still current.

use std::{
    collections::{hash_map::Entry, HashMap},
    sync::{Arc, Mutex, MutexGuard, PoisonError},
};

use pg_walstream::parse_lsn;
use pgoutput_decoder::{CellValue, DecodedChange, DecodedTransaction, RelationMetadata, TupleCell};
use postgres::{Client, GenericClient};
use thiserror::Error;

use crate::FenceError;

/// A `pg_enum` row. Renaming a value keeps its OID and writes a new xmin.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub(crate) struct ValueVersion {
    pub(crate) oid: u32,
    pub(crate) xmin: u32,
}

/// A version of an enum's value, and the earliest bound at which a reading
/// saw it. Recorded, so a later session proves labels from what an earlier
/// one saw.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SeenEnumVersion {
    pub(crate) value: ValueVersion,
    pub(crate) seen_from: u64,
}

/// A label served rows may carry, and the value of the enum that holds it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ServedEnumLabel {
    pub(crate) enum_type: u32,
    pub(crate) label: String,
    pub(crate) value: ValueVersion,
    /// No other value of the enum held `label` at any WAL position from this
    /// one on.
    pub(crate) held_from: u64,
    /// `value` had this version at every WAL position from this one on.
    pub(crate) seen_from: u64,
}

/// Why a label in the working set may no longer be the one PostgreSQL has
/// for the value a row holds.
#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub enum EnumLabelDrift {
    #[error("the value labelled {label:?} of enum {enum_type}, which rows Calque serves may hold, was renamed, or PostgreSQL renumbered the enum's values after many were added before one another, so the working set may carry labels PostgreSQL no longer has.")]
    Changed { enum_type: String, label: String },
    #[error("a change carries the label {label:?} of enum {enum_type}, which no value of the enum holds any more: the value was renamed after the change, so the row it wrote no longer carries that label.")]
    Gone { enum_type: String, label: String },
    #[error("a change carries the label {label:?} of enum {enum_type}, and Calque cannot prove which value of the enum it named: that value and another of the enum were added or renamed after Calque last read the enum and before the change.")]
    Unproven { enum_type: String, label: String },
}

/// The enum each type a column of `publication` is declared with resolves
/// to, directly or through any chain of domains, keyed by the declared type.
pub(crate) fn published_enum_types(
    client: &mut impl GenericClient,
    publication: &str,
) -> Result<HashMap<u32, u32>, postgres::Error> {
    Ok(client
        .query(
            "WITH RECURSIVE declared AS (
                 SELECT DISTINCT attribute.atttypid AS type_oid
                   FROM pg_catalog.pg_publication_tables AS published
                   JOIN pg_catalog.pg_namespace AS namespace
                     ON namespace.nspname = published.schemaname
                   JOIN pg_catalog.pg_class AS class
                     ON class.relnamespace = namespace.oid
                    AND class.relname = published.tablename
                   JOIN pg_catalog.pg_attribute AS attribute
                     ON attribute.attrelid = class.oid
                  WHERE published.pubname = $1
                    AND attribute.attnum > 0
                    AND NOT attribute.attisdropped
                    AND attribute.attname = ANY(published.attnames)
             ), chain(declared, type_oid) AS (
                 SELECT type_oid, type_oid FROM declared
                 UNION
                 SELECT chain.declared, domain.typbasetype
                   FROM chain
                   JOIN pg_catalog.pg_type AS domain ON domain.oid = chain.type_oid
                  WHERE domain.typtype = 'd'
             )
             SELECT chain.declared, chain.type_oid
               FROM chain
               JOIN pg_catalog.pg_type AS enum_type ON enum_type.oid = chain.type_oid
              WHERE enum_type.typtype = 'e'",
            &[&publication],
        )?
        .into_iter()
        .map(|row| (row.get(0), row.get(1)))
        .collect())
}

/// One reading of the values of a set of enums.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct EnumReading {
    /// Every version below had committed before this WAL position.
    pub(crate) bound: u64,
    pub(crate) enums: HashMap<u32, EnumValues>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct EnumValues {
    pub(crate) name: String,
    /// Each value's xmin and label, by OID.
    pub(crate) values: HashMap<u32, (u32, String)>,
}

#[derive(Debug, Error)]
pub(crate) enum ReadingError {
    #[error(transparent)]
    Postgres(#[from] postgres::Error),
    #[error("PostgreSQL returned an invalid WAL position or transaction id")]
    Invalid,
}

/// Reads every value of `enum_types` and the reading's bound, in one
/// statement, so one snapshot serves both.
pub(crate) fn read_enum_values(
    client: &mut impl GenericClient,
    enum_types: &[u32],
) -> Result<EnumReading, ReadingError> {
    let rows = client.query(
        "WITH bound AS MATERIALIZED (
             SELECT pg_catalog.pg_current_wal_insert_lsn()::text AS lsn
         )
         SELECT bound.lsn, value.enumtypid, pg_catalog.format_type(value.enumtypid, NULL),
                value.oid, value.xmin::text, value.enumlabel::text
           FROM bound
           LEFT JOIN pg_catalog.pg_enum AS value ON value.enumtypid = ANY($1::oid[])",
        &[&enum_types],
    )?;
    let mut bound = None;
    let mut enums: HashMap<u32, EnumValues> = HashMap::new();
    for row in rows {
        bound = Some(row.get::<_, String>(0));
        let Some(enum_type) = row.get::<_, Option<u32>>(1) else {
            continue;
        };
        let xmin = row
            .get::<_, String>(4)
            .parse::<u32>()
            .map_err(|_| ReadingError::Invalid)?;
        enums
            .entry(enum_type)
            .or_insert_with(|| EnumValues {
                name: row.get(2),
                values: HashMap::new(),
            })
            .values
            .insert(row.get(3), (xmin, row.get(5)));
    }
    let bound = bound
        .and_then(|lsn| parse_lsn(&lsn).ok())
        .ok_or(ReadingError::Invalid)?;
    Ok(EnumReading { bound, enums })
}

impl EnumReading {
    fn holder(&self, enum_type: u32, label: &[u8]) -> Option<ValueVersion> {
        self.enums
            .get(&enum_type)?
            .values
            .iter()
            .find_map(|(oid, (xmin, held))| {
                (held.as_bytes() == label).then_some(ValueVersion {
                    oid: *oid,
                    xmin: *xmin,
                })
            })
    }

    fn enum_name(&self, enum_type: u32) -> String {
        self.enums.get(&enum_type).map_or_else(
            || format!("with OID {enum_type}"),
            |values| values.name.clone(),
        )
    }
}

/// The labels served rows may carry at the slot's creation, from two
/// readings of the enums: `before` ends before the slot is created, so its
/// bound is at or before every change the slot decodes, and `after` follows
/// it. A row the slot does not decode committed before it was created and
/// holds a value `after` saw.
pub(crate) fn enrolled_labels(before: &EnumReading, after: &EnumReading) -> Vec<ServedEnumLabel> {
    let mut labels = Vec::new();
    for (enum_type, values) in &after.enums {
        for (oid, (xmin, label)) in &values.values {
            let unchanged = before
                .enums
                .get(enum_type)
                .and_then(|values| values.values.get(oid))
                .is_some_and(|(earlier_xmin, earlier_label)| {
                    earlier_xmin == xmin && earlier_label == label
                });
            let seen_from = if unchanged { before.bound } else { after.bound };
            labels.push(ServedEnumLabel {
                enum_type: *enum_type,
                label: label.clone(),
                value: ValueVersion {
                    oid: *oid,
                    xmin: *xmin,
                },
                held_from: seen_from,
                seen_from,
            });
        }
    }
    labels
}

#[derive(Debug, Clone, Copy)]
struct ServedValue {
    value: ValueVersion,
    held_from: u64,
    seen_from: u64,
}

/// What ingest and the session's watch share: the labels served rows may
/// carry, and the earliest bound at which a reading saw each current version.
#[derive(Debug, Default)]
pub(crate) struct EnumLabelState {
    served: HashMap<u32, HashMap<Box<[u8]>, ServedValue>>,
    seen: HashMap<ValueVersion, u64>,
    /// `seen` differs from the versions last recorded.
    unrecorded: bool,
}

impl EnumLabelState {
    /// The state `labels` serve, with the versions an earlier session saw.
    pub(crate) fn new(labels: &[ServedEnumLabel], seen: &[SeenEnumVersion]) -> Self {
        let mut state = Self::default();
        for version in seen {
            state.seen.insert(version.value, version.seen_from);
        }
        state.serve(labels);
        state
    }

    /// Every version seen, when they differ from the versions last recorded;
    /// they count as recorded from then on unless [`Self::record_failed`].
    pub(crate) fn take_unrecorded(&mut self) -> Option<Vec<SeenEnumVersion>> {
        if !std::mem::take(&mut self.unrecorded) {
            return None;
        }
        Some(
            self.seen
                .iter()
                .map(|(value, seen_from)| SeenEnumVersion {
                    value: *value,
                    seen_from: *seen_from,
                })
                .collect(),
        )
    }

    pub(crate) fn record_failed(&mut self) {
        self.unrecorded = true;
    }

    fn serve(&mut self, labels: &[ServedEnumLabel]) {
        for label in labels {
            let seen = self.seen.entry(label.value).or_insert(label.seen_from);
            *seen = (*seen).min(label.seen_from);
            let entry = self
                .served
                .entry(label.enum_type)
                .or_default()
                .entry(label.label.as_bytes().into());
            match entry {
                Entry::Vacant(vacant) => {
                    vacant.insert(ServedValue {
                        value: label.value,
                        held_from: label.held_from,
                        seen_from: label.seen_from,
                    });
                }
                Entry::Occupied(mut occupied) => {
                    let served = occupied.get_mut();
                    served.held_from = served.held_from.min(label.held_from);
                    served.seen_from = served.seen_from.min(label.seen_from);
                }
            }
        }
    }

    /// The enums any served label belongs to.
    pub(crate) fn enum_types(&self) -> impl Iterator<Item = u32> + '_ {
        self.served.keys().copied()
    }

    /// Records the versions `reading` saw, and forgets those of its enums it
    /// no longer saw, since a version that is not current never is again.
    pub(crate) fn observe(&mut self, reading: &EnumReading) {
        let known = self.seen.len();
        self.seen
            .retain(|version, _| reading_has(reading, *version));
        let mut changed = self.seen.len() != known;
        for values in reading.enums.values() {
            for (oid, (xmin, _)) in &values.values {
                let version = ValueVersion {
                    oid: *oid,
                    xmin: *xmin,
                };
                match self.seen.entry(version) {
                    Entry::Vacant(vacant) => {
                        vacant.insert(reading.bound);
                        changed = true;
                    }
                    Entry::Occupied(mut occupied) if reading.bound < *occupied.get() => {
                        occupied.insert(reading.bound);
                        changed = true;
                    }
                    Entry::Occupied(_) => {}
                }
            }
        }
        self.unrecorded |= changed;
    }

    /// The first served label `reading` finds held by another version, or by
    /// none.
    pub(crate) fn changed(&self, reading: &EnumReading) -> Option<EnumLabelDrift> {
        self.served.iter().find_map(|(enum_type, labels)| {
            labels.iter().find_map(|(label, served)| {
                (reading.holder(*enum_type, label) != Some(served.value)).then(|| {
                    EnumLabelDrift::Changed {
                        enum_type: reading.enum_name(*enum_type),
                        label: String::from_utf8_lossy(label).into_owned(),
                    }
                })
            })
        })
    }

    fn seen_from(&self, version: ValueVersion, reading: &EnumReading) -> u64 {
        self.seen.get(&version).copied().unwrap_or(reading.bound)
    }

    /// Whether a change at `reference` may carry `label` as it is served.
    fn proven(&self, enum_type: u32, label: &[u8], reference: u64) -> bool {
        self.served
            .get(&enum_type)
            .and_then(|labels| labels.get(label))
            .is_some_and(|served| served.held_from <= reference)
    }

    /// Proves which value held `label` at `reference`, a WAL position at or
    /// before a change that carries it, from a `reading` taken after the
    /// change committed, in one of two ways:
    ///
    /// 1. A reading at or before `reference` saw the version that holds it
    ///    now: that version was visible to the change.
    /// 2. A reading at or before `reference` saw every other current version
    ///    of the enum. Each was then visible to the change with the label it
    ///    has now, which is not `label`, so none of those values held it.
    ///
    /// Otherwise another value may have held `label` and been renamed since,
    /// and the row would now carry another label than the one served.
    fn decide(
        &self,
        reading: &EnumReading,
        enum_type: u32,
        label: &[u8],
        reference: u64,
    ) -> Result<ServedEnumLabel, EnumLabelDrift> {
        let name = || reading.enum_name(enum_type);
        let text = || String::from_utf8_lossy(label).into_owned();
        let Some(holder) = reading.holder(enum_type, label) else {
            return Err(EnumLabelDrift::Gone {
                enum_type: name(),
                label: text(),
            });
        };
        let seen_from = self.seen_from(holder, reading);
        let held_from = if seen_from <= reference {
            seen_from
        } else {
            let others = reading
                .enums
                .get(&enum_type)
                .into_iter()
                .flat_map(|values| &values.values)
                .filter(|(oid, _)| **oid != holder.oid)
                .map(|(oid, (xmin, _))| {
                    self.seen_from(
                        ValueVersion {
                            oid: *oid,
                            xmin: *xmin,
                        },
                        reading,
                    )
                })
                .max()
                .unwrap_or(0);
            if others > reference {
                return Err(EnumLabelDrift::Unproven {
                    enum_type: name(),
                    label: text(),
                });
            }
            others
        };
        Ok(ServedEnumLabel {
            enum_type,
            label: text(),
            value: holder,
            held_from,
            seen_from,
        })
    }
}

fn reading_has(reading: &EnumReading, version: ValueVersion) -> bool {
    reading.enums.values().any(|values| {
        values
            .values
            .get(&version.oid)
            .is_some_and(|(xmin, _)| *xmin == version.xmin)
    })
}

pub(crate) fn lock(state: &Mutex<EnumLabelState>) -> MutexGuard<'_, EnumLabelState> {
    state.lock().unwrap_or_else(PoisonError::into_inner)
}

#[derive(Debug, Error)]
pub(crate) enum GuardError {
    #[error(transparent)]
    Drift(#[from] EnumLabelDrift),
    #[error("reading the source's enum values failed or returned an uncertain result")]
    Reading,
    #[error(transparent)]
    Fence(#[from] FenceError),
}

/// The enum columns of one relation layout: each column's index and enum.
struct RelationEnums {
    layout_generation: u64,
    columns: Box<[(usize, u32)]>,
}

/// Checks every enum label of the transactions a session decodes before they
/// are appended.
pub(crate) struct EnumLabelGuard {
    /// At or before every change the slot decodes.
    from_lsn: u64,
    declared: HashMap<u32, u32>,
    enum_types: Vec<u32>,
    relations: HashMap<u32, RelationEnums>,
    state: Arc<Mutex<EnumLabelState>>,
    connect: Box<dyn FnMut() -> Option<Client> + Send>,
    controller: Option<Client>,
}

impl EnumLabelGuard {
    pub(crate) fn new(
        from_lsn: u64,
        declared: HashMap<u32, u32>,
        state: Arc<Mutex<EnumLabelState>>,
        connect: Box<dyn FnMut() -> Option<Client> + Send>,
    ) -> Self {
        let mut enum_types: Vec<u32> = declared.values().copied().collect();
        enum_types.extend(lock(&state).enum_types());
        enum_types.sort_unstable();
        enum_types.dedup();
        Self {
            from_lsn,
            declared,
            enum_types,
            relations: HashMap::new(),
            state,
            connect,
            controller: None,
        }
    }

    pub(crate) fn enum_types(&self) -> &[u32] {
        &self.enum_types
    }

    pub(crate) fn state(&self) -> Arc<Mutex<EnumLabelState>> {
        Arc::clone(&self.state)
    }

    /// Proves every enum label `transactions` carry, records the labels it
    /// proved for the first time or from an earlier position through
    /// `record`, and only then returns.
    pub(crate) fn verify(
        &mut self,
        transactions: &[DecodedTransaction],
        record: impl FnOnce(&[ServedEnumLabel]) -> Result<(), FenceError>,
    ) -> Result<(), GuardError> {
        if self.declared.is_empty() {
            return Ok(());
        }
        let unproven = self.unproven(transactions);
        if unproven.is_empty() {
            return Ok(());
        }
        let reading = self.read()?;
        let proved = {
            let mut state = lock(&self.state);
            state.observe(&reading);
            if let Some(drift) = state.changed(&reading) {
                return Err(drift.into());
            }
            unproven
                .iter()
                .map(|(enum_type, label, reference)| {
                    state.decide(&reading, *enum_type, label, *reference)
                })
                .collect::<Result<Vec<_>, _>>()?
        };
        record(&proved)?;
        lock(&self.state).serve(&proved);
        Ok(())
    }

    /// The labels the served ones do not prove, each with the earliest
    /// position a change carries it at.
    fn unproven(&mut self, transactions: &[DecodedTransaction]) -> Vec<(u32, Box<[u8]>, u64)> {
        let Self {
            from_lsn,
            declared,
            relations,
            state,
            ..
        } = self;
        let state = lock(state);
        let mut unproven: Vec<(u32, Box<[u8]>, u64)> = Vec::new();
        for transaction in transactions {
            let reference = transaction.first_observed_wal_lsn.get().max(*from_lsn);
            for change in &transaction.changes {
                let Some(relation) = changed_relation(change) else {
                    continue;
                };
                for &(index, enum_type) in relation_enums(relations, declared, relation) {
                    for label in labels_at(change, index).into_iter().flatten() {
                        if state.proven(enum_type, label, reference) {
                            continue;
                        }
                        match unproven.iter_mut().find(|(other_type, other, _)| {
                            *other_type == enum_type && **other == *label
                        }) {
                            Some((_, _, earliest)) => *earliest = (*earliest).min(reference),
                            None => unproven.push((enum_type, label.into(), reference)),
                        }
                    }
                }
            }
        }
        unproven
    }

    fn read(&mut self) -> Result<EnumReading, GuardError> {
        if self.controller.is_none() {
            self.controller = (self.connect)();
        }
        let client = self.controller.as_mut().ok_or(GuardError::Reading)?;
        read_enum_values(client, &self.enum_types).map_err(|_| {
            self.controller = None;
            GuardError::Reading
        })
    }
}

fn changed_relation(change: &DecodedChange) -> Option<&RelationMetadata> {
    match change {
        DecodedChange::Insert { relation, .. }
        | DecodedChange::Update { relation, .. }
        | DecodedChange::Delete { relation, .. } => Some(relation),
        DecodedChange::Truncate { .. } | DecodedChange::LogicalMessage(_) => None,
    }
}

fn relation_enums<'a>(
    relations: &'a mut HashMap<u32, RelationEnums>,
    declared: &HashMap<u32, u32>,
    relation: &RelationMetadata,
) -> &'a [(usize, u32)] {
    let entry = relations
        .entry(relation.relation_oid)
        .or_insert_with(|| RelationEnums {
            layout_generation: u64::MAX,
            columns: Box::new([]),
        });
    if entry.layout_generation != relation.layout_generation {
        entry.layout_generation = relation.layout_generation;
        entry.columns = relation
            .columns
            .iter()
            .enumerate()
            .filter_map(|(index, column)| {
                declared
                    .get(&column.type_oid)
                    .map(|enum_type| (index, *enum_type))
            })
            .collect();
    }
    &entry.columns
}

/// The labels a change carries in column `index`: its new value, then its
/// old one.
fn labels_at(change: &DecodedChange, index: usize) -> [Option<&[u8]>; 2] {
    match change {
        DecodedChange::Insert { row, .. } => [text(row.cells.get(index)), None],
        DecodedChange::Update {
            patch, old_tuple, ..
        } => [
            tuple_text(patch.cells.get(index)),
            old_tuple
                .as_ref()
                .and_then(|old| tuple_text(old.cells.get(index))),
        ],
        DecodedChange::Delete { old_tuple, .. } => [None, tuple_text(old_tuple.cells.get(index))],
        DecodedChange::Truncate { .. } | DecodedChange::LogicalMessage(_) => [None, None],
    }
}

fn text(cell: Option<&CellValue>) -> Option<&[u8]> {
    match cell {
        Some(CellValue::Text(bytes)) => Some(bytes),
        _ => None,
    }
}

fn tuple_text(cell: Option<&TupleCell>) -> Option<&[u8]> {
    match cell {
        Some(TupleCell::Value(value)) => text(Some(value)),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const MOOD: u32 = 900;

    fn reading(bound: u64, values: &[(u32, u32, &str)]) -> EnumReading {
        EnumReading {
            bound,
            enums: HashMap::from([(
                MOOD,
                EnumValues {
                    name: "mood".to_owned(),
                    values: values
                        .iter()
                        .map(|(oid, xmin, label)| (*oid, (*xmin, (*label).to_owned())))
                        .collect(),
                },
            )]),
        }
    }

    fn served(label: &str, oid: u32, xmin: u32, from: u64) -> ServedEnumLabel {
        ServedEnumLabel {
            enum_type: MOOD,
            label: label.to_owned(),
            value: ValueVersion { oid, xmin },
            held_from: from,
            seen_from: from,
        }
    }

    #[test]
    fn enrollment_serves_every_value_from_the_reading_that_saw_it_first() {
        let before = reading(100, &[(1, 10, "calm"), (2, 10, "busy")]);
        let after = reading(200, &[(1, 10, "calm"), (2, 11, "swamped"), (3, 12, "away")]);
        let mut labels = enrolled_labels(&before, &after);
        labels.sort_by_key(|label| label.value.oid);
        assert_eq!(
            labels,
            vec![
                served("calm", 1, 10, 100),
                served("swamped", 2, 11, 200),
                served("away", 3, 12, 200),
            ]
        );
    }

    #[test]
    fn a_served_label_held_by_another_version_or_by_none_has_changed() {
        let state = EnumLabelState::new(&[served("calm", 1, 10, 100)], &[]);
        assert_eq!(
            state.changed(&reading(300, &[(1, 10, "calm"), (2, 20, "away")])),
            None
        );
        for later in [
            reading(300, &[(1, 11, "calm")]),
            reading(300, &[(1, 11, "cool"), (2, 20, "calm")]),
            reading(300, &[]),
        ] {
            assert_eq!(
                state.changed(&later),
                Some(EnumLabelDrift::Changed {
                    enum_type: "mood".to_owned(),
                    label: "calm".to_owned()
                })
            );
        }
    }

    #[test]
    fn a_label_seen_before_the_change_is_proven_by_its_own_version() {
        let mut state = EnumLabelState::new(&[served("calm", 1, 10, 100)], &[]);
        state.observe(&reading(
            150,
            &[(1, 10, "calm"), (2, 20, "away"), (3, 21, "gone")],
        ));
        let now = reading(400, &[(1, 10, "calm"), (2, 20, "away"), (3, 21, "gone")]);
        state.observe(&now);
        assert_eq!(
            state.decide(&now, MOOD, b"away", 160),
            Ok(served("away", 2, 20, 150))
        );
        assert!(state.decide(&now, MOOD, b"away", 140).is_err());
    }

    #[test]
    fn a_label_no_reading_saw_in_time_is_proven_by_every_other_value() {
        let mut state = EnumLabelState::new(&[served("calm", 1, 10, 100)], &[]);
        let now = reading(400, &[(1, 10, "calm"), (2, 20, "away")]);
        state.observe(&now);
        let proved = state.decide(&now, MOOD, b"away", 300).unwrap();
        assert_eq!(
            (proved.held_from, proved.seen_from),
            (100, 400),
            "held from the last other value's reading, seen from now"
        );
        assert_eq!(
            state.decide(&now, MOOD, b"away", 90),
            Err(EnumLabelDrift::Unproven {
                enum_type: "mood".to_owned(),
                label: "away".to_owned()
            })
        );
    }

    /// `y` was added as `x`, a row took `x`, `y` was renamed and `x` added
    /// again, all before any reading: the row holds `y`'s value, and nothing
    /// but the order of commits tells it from two plain additions.
    #[test]
    fn two_values_no_reading_saw_in_time_leave_either_unproven() {
        let mut state = EnumLabelState::new(&[served("calm", 1, 10, 100)], &[]);
        let now = reading(400, &[(1, 10, "calm"), (2, 20, "y"), (3, 21, "x")]);
        state.observe(&now);
        assert!(matches!(
            state.decide(&now, MOOD, b"x", 300),
            Err(EnumLabelDrift::Unproven { .. })
        ));
        assert!(matches!(
            state.decide(&now, MOOD, b"gone", 300),
            Err(EnumLabelDrift::Gone { .. })
        ));
    }

    /// A transaction is checked at its first published change, and never
    /// before the enrollment's first reading. A label proven there needs no
    /// query; one that is not asks for a reading.
    #[test]
    fn a_change_is_checked_no_earlier_than_the_enrollments_first_reading() {
        use pgoutput_decoder::{
            DecodedTransaction, PgLsn, RelationColumn, ReplicaIdentity, RowImage, TransactionMode,
        };

        const DECLARED: u32 = 901;
        let relation = Arc::new(RelationMetadata {
            relation_oid: 1,
            namespace: "public".to_owned(),
            relation_name: "items".to_owned(),
            replica_identity: ReplicaIdentity::Default,
            columns: vec![RelationColumn {
                name: "mood".to_owned(),
                type_oid: DECLARED,
                type_modifier: -1,
                is_key: false,
            }],
            layout_generation: 1,
            first_observed_wal_lsn: PgLsn::new(0),
            last_observed_wal_lsn: PgLsn::new(0),
        });
        let carrying = |position: u64| DecodedTransaction {
            xid: 1,
            mode: TransactionMode::Normal,
            first_observed_wal_lsn: PgLsn::new(position),
            final_lsn: None,
            commit_lsn: PgLsn::new(position + 1),
            end_lsn: PgLsn::new(position + 2),
            commit_time_postgres_micros: 0,
            changes: vec![DecodedChange::Insert {
                relation: Arc::clone(&relation),
                row: RowImage {
                    cells: vec![CellValue::Text(b"calm".to_vec())],
                },
            }],
        };
        let mut guard = EnumLabelGuard::new(
            100,
            HashMap::from([(DECLARED, MOOD)]),
            Arc::new(Mutex::new(EnumLabelState::new(
                &[served("calm", 1, 10, 100)],
                &[],
            ))),
            Box::new(|| None),
        );
        let recorded = |labels: &[ServedEnumLabel]| -> Result<(), FenceError> {
            panic!("nothing new to record: {labels:?}")
        };
        assert!(guard.verify(&[carrying(0)], recorded).is_ok());
        assert!(guard.verify(&[carrying(150)], recorded).is_ok());

        let mut guard = EnumLabelGuard::new(
            40,
            HashMap::from([(DECLARED, MOOD)]),
            Arc::new(Mutex::new(EnumLabelState::new(
                &[served("calm", 1, 10, 100)],
                &[],
            ))),
            Box::new(|| None),
        );
        assert!(matches!(
            guard.verify(&[carrying(50)], recorded),
            Err(GuardError::Reading)
        ));
    }

    /// A version an earlier session saw proves as it did there: here the
    /// reading at 150 saw `away`, so `x`, which no reading saw before the
    /// change at 300, is held by no other value.
    #[test]
    fn a_version_an_earlier_session_saw_proves_a_label_after_a_restart() {
        let away = SeenEnumVersion {
            value: ValueVersion { oid: 2, xmin: 20 },
            seen_from: 150,
        };
        let now = reading(400, &[(1, 10, "calm"), (2, 20, "away"), (3, 21, "x")]);
        let mut restarted = EnumLabelState::new(&[served("calm", 1, 10, 100)], &[away]);
        restarted.observe(&now);
        assert_eq!(
            restarted
                .decide(&now, MOOD, b"x", 300)
                .map(|label| label.held_from),
            Ok(150)
        );
        let mut forgotten = EnumLabelState::new(&[served("calm", 1, 10, 100)], &[]);
        forgotten.observe(&now);
        assert!(matches!(
            forgotten.decide(&now, MOOD, b"x", 300),
            Err(EnumLabelDrift::Unproven { .. })
        ));
    }

    /// The versions are recorded again only once a reading changes them, and
    /// after a failed record.
    #[test]
    fn the_versions_seen_are_unrecorded_only_once_a_reading_changes_them() {
        let mut state = EnumLabelState::new(&[served("calm", 1, 10, 100)], &[]);
        state.observe(&reading(200, &[(1, 10, "calm")]));
        assert_eq!(state.take_unrecorded(), None);
        state.observe(&reading(300, &[(1, 10, "calm"), (2, 20, "away")]));
        let mut recorded = state.take_unrecorded().unwrap();
        recorded.sort_by_key(|version| version.value.oid);
        assert_eq!(
            recorded,
            [
                SeenEnumVersion {
                    value: ValueVersion { oid: 1, xmin: 10 },
                    seen_from: 100
                },
                SeenEnumVersion {
                    value: ValueVersion { oid: 2, xmin: 20 },
                    seen_from: 300
                },
            ]
        );
        state.observe(&reading(400, &[(1, 10, "calm"), (2, 20, "away")]));
        assert_eq!(state.take_unrecorded(), None);
        state.record_failed();
        assert_eq!(
            state.take_unrecorded().map(|versions| versions.len()),
            Some(2)
        );
        state.observe(&reading(500, &[(2, 20, "away")]));
        assert_eq!(
            state.take_unrecorded().map(|versions| versions.len()),
            Some(1)
        );
    }

    #[test]
    fn a_served_label_proves_a_change_only_from_where_it_is_held() {
        let state = EnumLabelState::new(&[served("calm", 1, 10, 100)], &[]);
        assert!(state.proven(MOOD, b"calm", 100));
        assert!(!state.proven(MOOD, b"calm", 99));
        assert!(!state.proven(MOOD, b"away", 1_000));
    }

    /// Adding or renaming an enum value leaves the publication's fingerprint
    /// alone: the labels a served row may carry are this module's to watch.
    /// It watches every enum a published column uses, directly or through a
    /// chain of domains, whether the publication lists the table or reaches
    /// it through its schema, and no enum only an unlisted column uses.
    ///
    /// Only a superuser may create a `FOR TABLES IN SCHEMA` publication, so the
    /// schema publication comes from `calque_live.create_schema_publication`,
    /// which `scripts/live-postgres-suite.sh bootstrap` installs.
    #[test]
    #[ignore = "requires disposable PostgreSQL 17 or 18 admin URL bootstrapped by scripts/live-postgres-suite.sh"]
    fn the_enums_watched_are_those_the_published_columns_use() {
        use std::collections::BTreeSet;

        use source_control::QualifiedTable;

        use crate::{inspect_owned_publication, OwnedPublication};

        let admin_url = std::env::var("CALQUE_ADMIN_DATABASE_URL")
            .expect("CALQUE_ADMIN_DATABASE_URL must be set");
        let suffix = format!(
            "{}_{}",
            std::process::id(),
            std::time::UNIX_EPOCH.elapsed().unwrap().as_nanos()
        );
        let schema = format!("rm_enum_{suffix}");
        let listed = format!("rm_enum_listed_{suffix}");
        let in_schema = format!("rm_enum_schema_{suffix}");
        let hidden = format!("rm_enum_hidden_{suffix}");
        let mut admin = Client::connect(&admin_url, postgres::NoTls).unwrap();
        admin
            .batch_execute(&format!(
                "CREATE SCHEMA {schema};
                 CREATE TYPE {schema}.mood AS ENUM ('calm', 'busy');
                 CREATE TYPE {schema}.size AS ENUM ('small', 'large');
                 CREATE DOMAIN {schema}.mood_label AS {schema}.mood;
                 CREATE DOMAIN {schema}.mood_note AS {schema}.mood_label;
                 CREATE TABLE {schema}.items (
                     id bigint PRIMARY KEY,
                     mood {schema}.mood_label NOT NULL,
                     note {schema}.mood_note,
                     size {schema}.size);
                 CREATE PUBLICATION {listed} FOR TABLE {schema}.items;
                 SELECT calque_live.create_schema_publication('{in_schema}', '{schema}');
                 CREATE PUBLICATION {hidden} FOR TABLE {schema}.items (id, size);"
            ))
            .unwrap();
        let oid = |admin: &mut Client, name: &str| -> u32 {
            admin
                .query_one(&format!("SELECT '{schema}.{name}'::regtype::oid"), &[])
                .unwrap()
                .get(0)
        };
        let [mood, size, mood_label, mood_note] =
            ["mood", "size", "mood_label", "mood_note"].map(|name| oid(&mut admin, name));
        let every_column = HashMap::from([(mood_label, mood), (mood_note, mood), (size, size)]);
        assert_eq!(
            published_enum_types(&mut admin, &listed).unwrap(),
            every_column
        );
        assert_eq!(
            published_enum_types(&mut admin, &in_schema).unwrap(),
            every_column
        );
        assert_eq!(
            published_enum_types(&mut admin, &hidden).unwrap(),
            HashMap::from([(size, size)])
        );

        let tables = BTreeSet::from([QualifiedTable {
            schema: schema.clone(),
            table: "items".to_owned(),
        }]);
        let publications = [listed.as_str(), in_schema.as_str(), hidden.as_str()];
        let fingerprints = |admin: &mut Client| {
            publications.map(|publication| {
                match inspect_owned_publication(admin, publication, &tables).unwrap() {
                    OwnedPublication::Matches { fingerprint, .. } => fingerprint,
                    other => panic!("{publication}: {other:?}"),
                }
            })
        };
        let before = fingerprints(&mut admin);
        admin
            .batch_execute(&format!(
                "ALTER TYPE {schema}.mood RENAME VALUE 'busy' TO 'swamped';
                 ALTER TYPE {schema}.mood ADD VALUE 'away';
                 ALTER TYPE {schema}.size ADD VALUE 'medium' BEFORE 'large';"
            ))
            .unwrap();
        assert_eq!(fingerprints(&mut admin), before);
        admin
            .batch_execute(&format!(
                "DROP PUBLICATION {listed}, {hidden};
                 SELECT calque_live.drop_schema_publication('{in_schema}');
                 DROP SCHEMA {schema} CASCADE;"
            ))
            .unwrap();
    }
}
