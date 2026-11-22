//! The format of the state a source's runtime last started on.
//!
//! `calque_control.source_state_format` records, per source, the format of
//! the state the last runtime to start it writes. A start that finds a later
//! format refuses before it reads or writes any other state, so an earlier
//! build never serves state it would misread. A start that finds an earlier
//! format resumes it in place when this build reads it as it stands, and
//! otherwise prepares the working set again by moving the source to its next
//! schema generation. A source with no row was last started by a build that
//! recorded none.

use postgres::GenericClient;
use sync_types::SourceId;
use thiserror::Error;

/// The state format this build writes.
///
/// A change to what a build stores for a source, in `calque_control` or the
/// state directory, that an earlier build would misread raises it. Format 1
/// is every build that recorded none, through release 0.4.0.
pub const STATE_FORMAT: u32 = 2;

/// The earliest state format this build resumes in place. A change that this
/// build cannot read as an earlier one left it, such as another artifact or
/// canonical log format, raises it to the format that change introduces.
const EARLIEST_FORMAT_RESUMED: u32 = 1;

/// The format a source with no row is at.
const UNRECORDED_FORMAT: u32 = 1;

/// How a start proceeds from the state format it finds.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StateUpgrade {
    /// This build reads the state as it stands.
    Resume,
    /// This build moves the source to its next schema generation, which
    /// prepares the working set again from PostgreSQL and keeps the slot, the
    /// canonical log, the authorization records and the write receipts.
    Reprepare,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Error)]
#[error("this source's state was written by a later release of calque (state format {recorded}; this build reads formats up to {STATE_FORMAT}), so this build does not start it. Run that release or a later one to keep the state, or start over with this release.")]
pub struct LaterStateFormat {
    pub recorded: u32,
}

/// How a start proceeds from `recorded`, the format its source's state is at.
pub fn state_upgrade(recorded: u32) -> Result<StateUpgrade, LaterStateFormat> {
    upgrade_between(recorded, STATE_FORMAT, EARLIEST_FORMAT_RESUMED)
}

fn upgrade_between(
    recorded: u32,
    writes: u32,
    earliest_resumed: u32,
) -> Result<StateUpgrade, LaterStateFormat> {
    if recorded > writes {
        Err(LaterStateFormat { recorded })
    } else if recorded < earliest_resumed {
        Ok(StateUpgrade::Reprepare)
    } else {
        Ok(StateUpgrade::Resume)
    }
}

/// The state format `source_id` is at, read without a lease.
pub fn read_state_format(
    client: &mut impl GenericClient,
    source_id: &SourceId,
) -> Result<u32, postgres::Error> {
    let installed: bool = client
        .query_one(
            "SELECT to_regclass('calque_control.source_state_format') IS NOT NULL",
            &[],
        )?
        .get(0);
    if !installed {
        return Ok(UNRECORDED_FORMAT);
    }
    Ok(client
        .query_opt(
            "SELECT state_format FROM calque_control.source_state_format WHERE source_id = $1",
            &[&source_id.as_str()],
        )?
        .map_or(UNRECORDED_FORMAT, |row| row.get::<_, i32>(0).unsigned_abs()))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_later_format_is_refused_and_an_earlier_one_resumes_unless_it_is_too_early() {
        assert_eq!(upgrade_between(2, 2, 1), Ok(StateUpgrade::Resume));
        assert_eq!(upgrade_between(1, 2, 1), Ok(StateUpgrade::Resume));
        assert_eq!(upgrade_between(1, 3, 2), Ok(StateUpgrade::Reprepare));
        assert_eq!(upgrade_between(2, 3, 2), Ok(StateUpgrade::Resume));
        assert_eq!(
            upgrade_between(3, 2, 1),
            Err(LaterStateFormat { recorded: 3 })
        );
        assert_eq!(state_upgrade(UNRECORDED_FORMAT), Ok(StateUpgrade::Resume));
        assert_eq!(state_upgrade(STATE_FORMAT), Ok(StateUpgrade::Resume));
        assert!(state_upgrade(STATE_FORMAT + 1).is_err());
    }
}
