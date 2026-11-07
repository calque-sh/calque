//! The open-file limit the connection bounds need.
//!
//! Every open change stream and every request in flight holds a socket. A
//! process at its open-file limit cannot accept the next connection, so a
//! browser past it waits instead of being refused. At startup the soft limit
//! is raised as far as the hard one allows; bounds that still do not fit are
//! cut to what it admits, and the cut is logged.

/// Descriptors the process holds besides the connections the bounds count:
/// its PostgreSQL connections, listeners and state files, and connections
/// open between two requests.
const RESERVED_DESCRIPTORS: u64 = 256;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct ConnectionBounds {
    pub max_open_streams: usize,
    pub max_inflight_requests: usize,
}

/// Raises the open-file limit and returns `asked`, or what of it fits.
pub(crate) fn fit_open_file_limit(asked: ConnectionBounds) -> ConnectionBounds {
    let limit = match rlimit::increase_nofile_limit(u64::MAX)
        .or_else(|_| rlimit::getrlimit(rlimit::Resource::NOFILE).map(|(soft, _)| soft))
    {
        Ok(limit) => limit,
        Err(error) => {
            tracing::warn!(%error, "the open-file limit could not be read; the connection bounds stand as asked");
            return asked;
        }
    };
    let fitted = fit(limit, asked);
    if fitted != asked {
        tracing::warn!(
            open_file_limit = limit,
            max_open_streams = fitted.max_open_streams,
            max_inflight_requests = fitted.max_inflight_requests,
            asked_open_streams = asked.max_open_streams,
            asked_inflight_requests = asked.max_inflight_requests,
            short_by = needed(asked).saturating_sub(limit),
            "the open-file limit admits fewer connections than the bounds ask; \
             the bounds are cut to fit it. Raise the hard limit (ulimit -Hn) to keep them"
        );
    }
    fitted
}

fn needed(bounds: ConnectionBounds) -> u64 {
    count(bounds.max_open_streams)
        .saturating_add(count(bounds.max_inflight_requests))
        .saturating_add(RESERVED_DESCRIPTORS)
}

/// `asked` when it fits under `limit`, and otherwise the room left after the
/// reserved descriptors, split between the two bounds as they were asked,
/// with at least one of each.
fn fit(limit: u64, asked: ConnectionBounds) -> ConnectionBounds {
    if needed(asked) <= limit {
        return asked;
    }
    let room = limit.saturating_sub(RESERVED_DESCRIPTORS);
    let streams = count(asked.max_open_streams);
    let total = streams.saturating_add(count(asked.max_inflight_requests));
    let fitted_streams =
        u64::try_from(u128::from(room) * u128::from(streams) / u128::from(total)).unwrap_or(room);
    let fitted_requests = room.saturating_sub(fitted_streams);
    ConnectionBounds {
        max_open_streams: bound(fitted_streams),
        max_inflight_requests: bound(fitted_requests),
    }
}

fn count(bound: usize) -> u64 {
    u64::try_from(bound).unwrap_or(u64::MAX)
}

fn bound(count: u64) -> usize {
    usize::try_from(count).unwrap_or(usize::MAX).max(1)
}

#[cfg(test)]
mod tests {
    use super::*;

    const DEFAULTS: ConnectionBounds = ConnectionBounds {
        max_open_streams: calque_engine::DEFAULT_MAX_OPEN_STREAMS,
        max_inflight_requests: calque_engine::DEFAULT_MAX_INFLIGHT_REQUESTS,
    };

    #[test]
    fn bounds_that_fit_stand_as_asked() {
        assert_eq!(fit(184_320, DEFAULTS), DEFAULTS);
        assert_eq!(fit(4_096 + 4_096 + 256, DEFAULTS), DEFAULTS);
    }

    #[test]
    fn bounds_past_the_limit_are_cut_to_what_it_admits() {
        for limit in [256 + 2, 1_024, 4_096, 4_096 + 4_096 + 255] {
            let fitted = fit(limit, DEFAULTS);
            assert!(
                needed(fitted) <= limit,
                "{fitted:?} needs more than {limit} descriptors"
            );
            assert!(needed(fitted) >= limit - 1);
        }
        assert_eq!(
            fit(1_024, DEFAULTS),
            ConnectionBounds {
                max_open_streams: 384,
                max_inflight_requests: 384,
            }
        );
        let lopsided = ConnectionBounds {
            max_open_streams: 3_000,
            max_inflight_requests: 1_000,
        };
        assert_eq!(
            fit(2_256, lopsided),
            ConnectionBounds {
                max_open_streams: 1_500,
                max_inflight_requests: 500,
            }
        );
    }

    #[test]
    fn a_limit_under_the_reserve_still_admits_one_of_each() {
        for limit in [0, 64, 256] {
            assert_eq!(
                fit(limit, DEFAULTS),
                ConnectionBounds {
                    max_open_streams: 1,
                    max_inflight_requests: 1,
                }
            );
        }
    }

    #[test]
    fn the_bounds_fit_the_limit_the_process_is_left_with() {
        let fitted = fit_open_file_limit(DEFAULTS);
        let (soft, _) = rlimit::getrlimit(rlimit::Resource::NOFILE).unwrap();
        assert_eq!(fitted, fit(soft, DEFAULTS));
    }
}
