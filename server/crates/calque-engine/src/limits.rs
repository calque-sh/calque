pub(crate) const MIB: u64 = 1024 * 1024;

/// The largest object one build may publish or the object store may hold: a
/// decoded artifact or one encoded representation of it. It stays below the
/// size of one Blink `ArrayBuffer`, which the browser decoder needs.
pub const MAX_LOCAL_ARTIFACT_BYTES: u64 = 1280 * MIB;
/// The client memory model's size for one resident artifact: a fixed planning
/// figure, not derived from the artifact, and smaller than an artifact may be.
pub const LOCAL_ACTIVE_ARTIFACT_CAPACITY_BYTES: u64 = 256 * MIB;
pub(crate) const LOCAL_ARTIFACT_GROWTH_RESERVE_BYTES: u64 = 64 * MIB;
/// The client's query pool: what the local queries running at one time may
/// reserve for their structures. It is large because the client's estimator
/// charges a sort or a filter for every row of the relation it reads.
pub const LOCAL_QUERY_RESULT_RESERVE_BYTES: u64 = 256 * MIB;
/// The change wait a bootstrap plan tells the client to ask for: a live
/// request whose first build finds nothing waits at most this long for the
/// canonical head to move. 1,000 ms is the public bootstrap contract's minimum.
pub const LOCAL_LONG_POLL_TIMEOUT_MS: u32 = 1_000;
/// Requests answered at once unless the deployment sets another bound.
/// Accepted requests, including preflights, hold one permit for the complete
/// response-body lifetime or fail immediately without queueing. A change
/// stream holds one only until its response opens. Every browser of a
/// restarted process bootstraps again at once and waits for a planner's turn
/// holding its permit, and a refused bootstrap is not retried, so the default
/// admits as many requests as browsers it keeps streams open for.
pub const DEFAULT_MAX_INFLIGHT_REQUESTS: usize = DEFAULT_MAX_OPEN_STREAMS;
/// Change streams held open at once unless the deployment sets another bound.
/// Every connected browser holds one; a stream beyond it is refused with 503
/// and retried.
pub const DEFAULT_MAX_OPEN_STREAMS: usize = 4_096;
/// The change tail's bound unless the deployment sets another: the newest
/// records, read once from the log for every change build, and each scope's
/// encoding of them.
pub const DEFAULT_CHANGE_TAIL_BYTES: u64 = 256 * MIB;
/// The bytes of change batches built from the log that the process holds at
/// once, from their build until they are written. A build that finds no room
/// waits for it; a build from the change tail takes none.
pub(crate) const LOG_BATCH_ROOM_BYTES: usize = 256 * 1024 * 1024;
