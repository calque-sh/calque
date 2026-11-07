//! Per-tenant working sets under a partitioned scope.
//!
//! A tenant is one value of the partition attribute, and has an artifact of
//! its own: its rows of every partitioned table and every shared table whole.
//! A tenant with no current artifact is built on its first request, under a
//! cap of concurrent builds, while the request is held open. Tenants opened
//! recently, or pinned by the application, stay warm: they are cut again by
//! the re-cut's rule once the changes routed to them since their cut outgrow
//! a ratio of their artifact, when a row moves into or out of them, and after
//! a fence. A tenant opened neither recently nor pinned is evicted, and its
//! next open is cold.
//!
//! The directory beside the catalog keeps what a restart needs: each tenant's
//! pin, last open and the bytes routed to it since its cut, every open, every
//! tenant order until it expires, and how far the follower has read the log.
//! Published artifacts are the catalog's.

use std::{
    collections::{BTreeMap, BTreeSet, HashMap, VecDeque},
    path::{Path, PathBuf},
    sync::{mpsc, Arc, Mutex as StdMutex, PoisonError},
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};

use artifact_catalog::{ArtifactState, SqliteArtifactCatalog};
use artifact_contract::ValidatedArtifactManifest;
use authz::{PartitionTouch, PartitionValue};
use axum::{
    body::Bytes,
    extract::{DefaultBodyLimit, State},
    http::{HeaderMap, StatusCode},
    response::{IntoResponse, Response},
    routing::post,
    Json, Router,
};
use blob_store::ManagedBlobStore;
use bootstrap_api::UnchangedScope;
use bootstrap_http::BootstrapHttpError;
use canonical_log::{CanonicalEntry, CanonicalLogRead, LogError, PostgresFencedCanonicalLog};
use dataset_grant::{TenantAction, VerifiedTenantOrder};
use rusqlite::{params, Connection, OptionalExtension};
use serde::{Deserialize, Serialize};
use snapshot_pipeline::CanonicalHeadSignal;
use source_control::PostgresTypeCodec;
use sync_types::{ArtifactId, OpaqueCursor};
use thiserror::Error;
use time::{format_description::well_known::Rfc3339, OffsetDateTime};
use tokio::sync::{oneshot, watch, RwLock};
use tokio_util::sync::CancellationToken;
use uuid::Uuid;

use crate::{
    guard::PostgresConnectionFamily,
    limits::MIB,
    log_readers::{LogReaders, SourceLogReaders},
    recut::RecutPolicy,
    reencode::{reencode_leaving_replaced, ReencodeRequest},
    snapshot::{build_scope_artifact, published_query_for, recover_scope_builds},
    source::connect_owned,
    state::SNAPSHOT_BUILD_DIRECTORY,
    ArtifactBuilder, DatabaseUrl, LocalBackend, LocalSnapshotError, OperationErrorClass,
    OperationsMetrics, PreparedLocalSource, MAX_LOCAL_ARTIFACT_BYTES,
};

/// Builds that run at once by default. Each holds a replication slot and a
/// WAL sender until it reaches its cut.
pub const DEFAULT_MAX_TENANT_BUILDS: usize = 4;
/// A tenant opened within this many days stays warm by default.
pub const DEFAULT_TENANT_WARM_DAYS: u32 = 7;
/// How long a bootstrap for a preparing tenant is held open by default: below
/// the 30 s at which common proxies close an idle request.
pub const DEFAULT_TENANT_HOLD_OPEN: Duration = Duration::from_secs(25);
/// The decoded size below which a tenant's artifact gets no background
/// compression stage by default. At 10k rows the stages cost 3.4 s of CPU to
/// save about 100 KB, under 10 ms at 100 Mbit/s.
pub const DEFAULT_TENANT_COMPRESS_ABOVE_BYTES: u64 = 16 * MIB;

const DIRECTORY_FILE: &str = "tenants.sqlite3";
const JOURNAL_DIRECTORY: &str = "tenant-journals";
const SWEEP_INTERVAL: Duration = Duration::from_secs(60);
const FOLLOW_INTERVAL: Duration = Duration::from_secs(1);
const FOLLOW_PAGE_RECORDS: usize = 1_000;
const FOLLOW_PAGE_BYTES: u64 = 16 * 1024 * 1024;
/// How long a bootstrap waits for the follower to read up to the head before
/// it is planned, so a tenant a row has just left is not planned from the cut
/// that row invalidated.
const FOLLOWER_SYNC: Duration = Duration::from_secs(2);
const PREPARING_RETRY_AFTER_SECONDS: u32 = 1;
/// The open record keeps this long.
const OPEN_RETENTION_MS: i64 = 90 * 24 * 60 * 60 * 1000;
/// How long past its expiry an order's id is kept, against a clock stepped
/// back by up to this much.
const ORDER_ID_MARGIN_SECONDS: u64 = 5 * 60;
/// Builds of one tenant in a row that a move may end before the tenant is
/// left cold until it is asked for again.
const MAX_MOVED_BUILDS: u32 = 5;
const DAY_MS: i64 = 24 * 60 * 60 * 1000;
const MAX_TENANT_ORDER_BODY_BYTES: usize = 16 * 1024;

#[derive(Debug, Clone)]
pub struct TenantSettings {
    pub max_builds: usize,
    pub warm_days: u32,
    /// The re-cut's rule, applied to each warm tenant: it is cut again once
    /// the bytes routed to it since its cut outgrow the ratio of its
    /// artifact, at most once per interval. A refresh or a warm order queued
    /// that long takes every other start while requests users wait on are
    /// queued.
    pub refresh: RecutPolicy,
    pub hold_open: Duration,
    /// `None` runs no background compression stage.
    pub compress_above_bytes: Option<u64>,
    /// The most decoded bytes one tenant's artifact may hold. A tenant above
    /// it is refused whole.
    pub max_tenant_bytes: u64,
}

impl Default for TenantSettings {
    fn default() -> Self {
        Self {
            max_builds: DEFAULT_MAX_TENANT_BUILDS,
            warm_days: DEFAULT_TENANT_WARM_DAYS,
            refresh: RecutPolicy::default(),
            hold_open: DEFAULT_TENANT_HOLD_OPEN,
            compress_above_bytes: Some(DEFAULT_TENANT_COMPRESS_ABOVE_BYTES),
            max_tenant_bytes: MAX_LOCAL_ARTIFACT_BYTES,
        }
    }
}

/// What `calque serve` hands [`Tenants::open`].
pub struct TenantsConfig {
    pub settings: TenantSettings,
    pub builder: ArtifactBuilder,
    pub log_readers: SourceLogReaders,
}

#[derive(Debug, Error)]
pub enum TenantError {
    #[error("tenant settings are invalid: {0}")]
    InvalidSettings(&'static str),
    #[error("the authorization policy is not partitioned")]
    NotPartitioned,
    #[error("tenant directory failed: {0}")]
    Directory(#[from] rusqlite::Error),
    #[error("tenant directory I/O failed: {0}")]
    Io(#[from] std::io::Error),
    #[error(transparent)]
    Catalog(#[from] artifact_catalog::CatalogError),
    #[error(transparent)]
    Snapshot(#[from] LocalSnapshotError),
    #[error(transparent)]
    Log(#[from] LogError),
    #[error(transparent)]
    Source(#[from] crate::LocalSourceError),
    #[error("PostgreSQL leaves {free_slots} replication slots and {free_senders} WAL senders free; {needed} of each are needed, one per tenant build and one for change capture. Raise max_replication_slots and max_wal_senders, or lower --max-tenant-builds.")]
    ReplicationCapacity {
        free_slots: i64,
        free_senders: i64,
        needed: i64,
    },
    #[error("tenant task failed")]
    Task,
}

/// Refuses to start when PostgreSQL leaves fewer replication slots or WAL
/// senders than `max_builds` plus one: each build holds one of each until it
/// reaches its cut, and change capture holds one more. `cdc_slot` is this
/// source's own slot, which counts as free since it is change capture's.
pub async fn check_replication_capacity(
    database_url: &DatabaseUrl,
    max_builds: usize,
    cdc_slot: &str,
) -> Result<(), TenantError> {
    let database_url = database_url.clone();
    let cdc_slot = cdc_slot.to_owned();
    let (free_slots, free_senders) = tokio::task::spawn_blocking(move || {
        let mut client = connect_owned(
            &database_url,
            PostgresConnectionFamily::Inspect,
            "calque-tenant-capacity",
        )?;
        let row = client
            .query_one(
                "SELECT current_setting('max_replication_slots')::bigint
                        - (SELECT count(*) FROM pg_replication_slots WHERE slot_name <> $1),
                        current_setting('max_wal_senders')::bigint
                        - (SELECT count(*) FROM pg_stat_replication)",
                &[&cdc_slot],
            )
            .map_err(crate::LocalSourceError::from)?;
        Ok::<_, TenantError>((row.get::<_, i64>(0), row.get::<_, i64>(1)))
    })
    .await
    .map_err(|_| TenantError::Task)??;
    let needed = i64::try_from(max_builds)
        .unwrap_or(i64::MAX)
        .saturating_add(1);
    if free_slots < needed || free_senders < needed {
        return Err(TenantError::ReplicationCapacity {
            free_slots,
            free_senders,
            needed,
        });
    }
    Ok(())
}

/// Where tenant builds journal, beside the source-wide build's journals.
fn journal_root(state_root: &Path) -> PathBuf {
    state_root
        .join(SNAPSHOT_BUILD_DIRECTORY)
        .join(JOURNAL_DIRECTORY)
}

/// The order a build waits in. A request a user is waiting on goes first.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
enum Priority {
    Waiting,
    Warm,
    Refresh,
}

impl Priority {
    const fn as_str(self) -> &'static str {
        match self {
            Self::Waiting => "waiting",
            Self::Warm => "warm",
            Self::Refresh => "refresh",
        }
    }

    const fn lane(self) -> usize {
        self as usize
    }
}

/// Builds waiting for a slot, one lane per priority, each in the order they
/// were queued. While requests users wait on are queued, a build that has
/// waited `aging` in a lower lane takes every other start, so a queue kept
/// full by them starves no refresh and no warm order, and a fleet of warm
/// orders older than `aging` holds back no request.
#[derive(Default)]
struct BuildQueue {
    lanes: [BTreeMap<u64, (PartitionValue, Instant)>; 3],
    /// The last start went to an aged build while a request was waiting.
    yielded: bool,
}

impl BuildQueue {
    fn insert(&mut self, priority: Priority, order: u64, value: PartitionValue, queued: Instant) {
        self.lanes[priority.lane()].insert(order, (value, queued));
    }

    fn remove(&mut self, priority: Priority, order: u64) {
        self.lanes[priority.lane()].remove(&order);
    }

    fn is_empty(&self) -> bool {
        self.lanes.iter().all(BTreeMap::is_empty)
    }

    fn len(&self) -> usize {
        self.lanes.iter().map(BTreeMap::len).sum()
    }

    fn clear(&mut self) {
        self.lanes.iter_mut().for_each(BTreeMap::clear);
    }

    /// The build to start next at `now`.
    fn pop(&mut self, now: Instant, aging: Duration) -> Option<(Priority, PartitionValue)> {
        let head = |priority: Priority| {
            self.lanes[priority.lane()]
                .first_key_value()
                .map(|(order, (_, queued))| (*order, *queued, priority))
        };
        let waiting = head(Priority::Waiting);
        let aged = [Priority::Warm, Priority::Refresh]
            .into_iter()
            .filter_map(head)
            .filter(|(_, queued, _)| now.saturating_duration_since(*queued) >= aging)
            .min_by_key(|(order, _, _)| *order);
        let (order, _, priority) = match (waiting, aged) {
            (Some(waiting), Some(aged)) if !self.yielded && aged.0 < waiting.0 => {
                self.yielded = true;
                aged
            }
            (Some(waiting), _) => {
                self.yielded = false;
                waiting
            }
            (None, aged) => aged.or_else(|| {
                [Priority::Warm, Priority::Refresh]
                    .into_iter()
                    .find_map(head)
            })?,
        };
        let (value, _) = self.lanes[priority.lane()].remove(&order)?;
        Some((priority, value))
    }
}

#[derive(Debug, Clone)]
struct Cut {
    generation: u64,
    cursor: OpaqueCursor,
    sequence: u64,
    decoded_bytes: u64,
    built_ms: i64,
}

enum Job {
    Queued {
        priority: Priority,
        order: u64,
        queued: Instant,
    },
    Running {
        /// Which build: one whose result arrives once its entry's job is
        /// another, or none, is discarded.
        build: u64,
        cancellation: CancellationToken,
        /// The follower's position when the build started. Its cut falls
        /// after it, so the log is kept from here until the cut is the
        /// entry's.
        held_from: u64,
    },
}

struct Entry {
    cut: Option<Cut>,
    pinned: bool,
    last_opened_ms: Option<i64>,
    warmed_ms: Option<i64>,
    /// The stored bytes of the operations routed to the tenant's partition
    /// since its cut.
    tail_bytes: u64,
    /// The shared tables' bytes the follower had read at the tenant's cut.
    shared_at_cut: u64,
    /// When the tenant's last refresh was queued.
    refresh_queued: Option<Instant>,
    /// The latest sequence at which a row moved into or out of the tenant.
    moved_at: Option<u64>,
    /// The latest sequence at which an operation on the tenant's rows
    /// committed, as far as the follower knows: from the entry's making it
    /// counts every operation it read.
    touched_at: u64,
    job: Option<Job>,
    /// Builds in a row a move has ended.
    moved_builds: u32,
    /// Counts the times a build was dropped by an evict or given up after
    /// moves, so a request waiting on it asks again rather than failing.
    interrupted: u64,
    /// The tenant's working set passed a bound on what one artifact holds.
    refused: bool,
    changed: watch::Sender<u64>,
}

impl Entry {
    fn new(touched_at: u64) -> Self {
        Self {
            cut: None,
            pinned: false,
            last_opened_ms: None,
            warmed_ms: None,
            tail_bytes: 0,
            shared_at_cut: 0,
            refresh_queued: None,
            moved_at: None,
            touched_at,
            job: None,
            moved_builds: 0,
            interrupted: 0,
            refused: false,
            changed: watch::Sender::new(0),
        }
    }

    /// The bytes a plan from the tenant's cut replays: its own and the
    /// shared tables'.
    fn tail(&self, shared_bytes: u64) -> u64 {
        self.tail_bytes
            .saturating_add(shared_bytes.saturating_sub(self.shared_at_cut))
    }

    fn current(&self, generation: u64) -> Option<&Cut> {
        self.cut.as_ref().filter(|cut| {
            cut.generation == generation && self.moved_at.is_none_or(|moved| cut.sequence >= moved)
        })
    }

    fn warm(&self, now_ms: i64, warm_days: u32) -> bool {
        let touched = self.last_opened_ms.max(self.warmed_ms);
        self.pinned
            || touched.is_some_and(|touched| {
                now_ms.saturating_sub(touched) <= i64::from(warm_days) * DAY_MS
            })
    }

    fn notify(&self) {
        self.changed
            .send_modify(|version| *version = version.wrapping_add(1));
    }
}

struct TenantState {
    base: Arc<PreparedLocalSource>,
    codec: PostgresTypeCodec,
    entries: HashMap<PartitionValue, Entry>,
    queue: BuildQueue,
    compress_queue: VecDeque<PartitionValue>,
    running_builds: usize,
    compressing: bool,
    next_order: u64,
    next_build: u64,
    /// The stored bytes of every operation on a shared table the follower
    /// has read.
    shared_bytes: u64,
    /// The latest sequence at which an operation every tenant reads
    /// committed, or past which the follower knows nothing.
    shared_touched_at: u64,
    /// The last sequence the follower applied to the counts above.
    followed: u64,
    /// Each tenant order acted on, by id, with its expiry in Unix seconds.
    orders: HashMap<String, u64>,
}

impl TenantState {
    fn generation(&self) -> u64 {
        self.base.policy.generation()
    }

    fn unchanged(&self, value: &PartitionValue) -> Option<UnchangedScope> {
        let entry = self.entries.get(value)?;
        Some(UnchangedScope {
            last_change: entry.touched_at.max(self.shared_touched_at),
            through: self.followed,
        })
    }

    fn entry(&mut self, value: &PartitionValue) -> &mut Entry {
        let followed = self.followed;
        self.entries
            .entry(value.clone())
            .or_insert_with(|| Entry::new(followed))
    }

    /// Queues a build of `value` at `priority`, or raises the priority of one
    /// already queued. A running build is left to finish.
    fn request_build(&mut self, value: &PartitionValue, priority: Priority) {
        let order = self.next_order;
        let followed = self.followed;
        let entry = self
            .entries
            .entry(value.clone())
            .or_insert_with(|| Entry::new(followed));
        match &mut entry.job {
            Some(Job::Running { .. }) => {}
            Some(Job::Queued {
                priority: queued_priority,
                order: queued_order,
                queued,
            }) => {
                if priority < *queued_priority {
                    self.queue.remove(*queued_priority, *queued_order);
                    self.queue
                        .insert(priority, *queued_order, value.clone(), *queued);
                    *queued_priority = priority;
                }
            }
            None => {
                let queued = Instant::now();
                entry.job = Some(Job::Queued {
                    priority,
                    order,
                    queued,
                });
                self.queue.insert(priority, order, value.clone(), queued);
                self.next_order += 1;
            }
        }
    }

    /// Drops the tenant's queued build, or cancels its running one, whose
    /// result is then discarded.
    fn drop_build(&mut self, value: &PartitionValue) {
        let Some(entry) = self.entries.get_mut(value) else {
            return;
        };
        match entry.job.take() {
            Some(Job::Queued {
                priority, order, ..
            }) => self.queue.remove(priority, order),
            Some(Job::Running { cancellation, .. }) => cancellation.cancel(),
            None => {}
        }
    }

    /// Records one build's end, and returns the catalog work and the
    /// directory write it leaves. The result of a build its tenant no longer
    /// runs, because the tenant was evicted, is discarded.
    fn finish_build(
        &mut self,
        settings: &TenantSettings,
        value: &PartitionValue,
        build: u64,
        generation: u64,
        end: BuildEnd,
    ) -> (Vec<Retire>, Option<DirectoryWrite>) {
        self.running_builds = self.running_builds.saturating_sub(1);
        let mut retire = Vec::new();
        let ours = self.entries.get(value).is_some_and(|entry| {
            matches!(entry.job, Some(Job::Running { build: running, .. }) if running == build)
        });
        if !ours {
            if let BuildEnd::Published(manifest) = end {
                tracing::info!(target: "calque::tenants", tenant = %value, "a build the tenant no longer runs was discarded");
                retire.push(Retire::One {
                    artifact: manifest.manifest().artifact_id.clone(),
                    reason: "its tenant was evicted while it was built",
                });
            }
            return (retire, None);
        }
        let current = self.generation();
        let shared = self.shared_bytes;
        let base = Arc::clone(&self.base);
        let prepared = self.prepared_for(value);
        let threshold = settings.compress_above_bytes;
        let warm_days = settings.warm_days;
        let mut tail = None;
        let entry = self.entry(value);
        entry.job = None;
        let mut compress = false;
        let mut stale = generation != current;
        let mut moved = false;
        match end {
            BuildEnd::Published(manifest) => match cut_of(&base, &manifest) {
                Some(cut) if !stale && entry.moved_at.is_none_or(|moved| cut.sequence >= moved) => {
                    compress = threshold.is_some_and(|bytes| cut.decoded_bytes >= bytes);
                    if let Some(prepared) = prepared {
                        retire.push(Retire::Scope {
                            prepared: Box::new(prepared),
                            keep: Some(cut.sequence),
                            reason: "a later cut of the tenant replaced it",
                        });
                    }
                    entry.cut = Some(cut);
                    entry.tail_bytes = 0;
                    entry.shared_at_cut = shared;
                    entry.moved_builds = 0;
                    entry.refused = false;
                    tail = Some(DirectoryWrite::Tail {
                        tenant: value.to_string(),
                        tail_bytes: 0,
                        shared_at_cut: shared,
                    });
                }
                // Cut before a move it does not hold, or under a generation
                // since fenced.
                _ => {
                    moved = !stale;
                    stale = true;
                    retire.push(Retire::One {
                        artifact: manifest.manifest().artifact_id.clone(),
                        reason: "its cut is stale",
                    });
                }
            },
            BuildEnd::Moved => {
                moved = !stale;
                stale = true;
            }
            BuildEnd::OverBound if !stale => entry.refused = true,
            BuildEnd::OverBound | BuildEnd::Failed => {}
        }
        let waiting = entry.changed.receiver_count() > 0;
        let mut rebuild = stale && (waiting || entry.warm(now_ms(), warm_days));
        if rebuild && moved {
            entry.moved_builds += 1;
            if entry.moved_builds > MAX_MOVED_BUILDS {
                tracing::warn!(target: "calque::tenants", tenant = %value, builds = entry.moved_builds, "rows kept moving into or out of the tenant during its builds; it is left cold until it is asked for again");
                rebuild = false;
                entry.interrupted += 1;
            } else {
                tracing::info!(target: "calque::tenants", tenant = %value, builds = entry.moved_builds, "tenant built again after a move");
            }
        }
        entry.notify();
        if compress {
            self.compress_queue.push_back(value.clone());
        }
        if rebuild {
            let priority = if waiting {
                Priority::Waiting
            } else {
                Priority::Refresh
            };
            self.request_build(value, priority);
        }
        (retire, tail)
    }

    /// The scope of one tenant under the generation in force.
    fn prepared_for(&self, value: &PartitionValue) -> Option<PreparedLocalSource> {
        let scope = self.base.policy.partition_scope(value.clone()).ok()?;
        Some(PreparedLocalSource {
            registry: self.base.registry.clone(),
            activated: self.base.activated.clone(),
            policy: self.base.policy.clone(),
            scope,
        })
    }
}

/// The last log sequence a tenant may still read: the oldest of every
/// tenant's cut, each running build's start and the follower's position. A
/// plan from a cut catches up from the entry after it.
fn retained_after<'a>(entries: impl IntoIterator<Item = &'a Entry>, followed: u64) -> u64 {
    entries
        .into_iter()
        .flat_map(|entry| {
            let building = match &entry.job {
                Some(Job::Running { held_from, .. }) => Some(*held_from),
                _ => None,
            };
            entry
                .cut
                .as_ref()
                .map(|cut| cut.sequence)
                .into_iter()
                .chain(building)
        })
        .fold(followed, u64::min)
}

/// One tenant's state, for the application's backend.
#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct TenantReport {
    pub tenant: String,
    /// `cold`, `preparing` or `warm`.
    pub state: &'static str,
    pub pinned: bool,
    /// The tenant's working set is larger than one artifact may hold.
    pub refused: bool,
    pub cut_age_seconds: Option<u64>,
    pub size_bytes: Option<u64>,
    pub last_opened_unix_seconds: Option<u64>,
}

/// Why a tenant order was not acted on.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Error)]
pub enum TenantOrderRefusal {
    #[error("tenant order is not authentic")]
    Unauthorized,
    #[error("tenant order is malformed")]
    InvalidRequest,
    #[error("tenant order was already acted on")]
    Replayed,
    #[error("tenant orders are not available")]
    Unavailable,
}

impl IntoResponse for TenantOrderRefusal {
    fn into_response(self) -> Response {
        let (status, code) = match self {
            Self::Unauthorized => (StatusCode::UNAUTHORIZED, "UNAUTHORIZED"),
            Self::InvalidRequest => (StatusCode::BAD_REQUEST, "INVALID_REQUEST"),
            Self::Replayed => (StatusCode::CONFLICT, "REPLAYED"),
            Self::Unavailable => (StatusCode::SERVICE_UNAVAILABLE, "UNAVAILABLE"),
        };
        crate::revocation::no_store(status, Json(serde_json::json!({ "code": code })))
    }
}

/// Every write that can add a tenant's row carries its `touched_at`, so a
/// row holds one from the start.
enum DirectoryWrite {
    Opened {
        tenant: String,
        at_ms: i64,
        size_bytes: Option<u64>,
        warm: bool,
        touched_at: u64,
    },
    Warmed {
        tenant: String,
        at_ms: i64,
        touched_at: u64,
    },
    Pinned {
        tenant: String,
        pinned: bool,
        touched_at: u64,
    },
    Touched {
        tenant: String,
        touched_at: u64,
    },
    Evicted {
        tenant: String,
    },
    Tail {
        tenant: String,
        tail_bytes: u64,
        shared_at_cut: u64,
    },
    Moved {
        tenant: String,
        sequence: u64,
    },
    Followed {
        source_epoch: String,
        sequence: u64,
        shared_bytes: u64,
        shared_touched_at: u64,
    },
    Order {
        id: String,
        expires_at: u64,
    },
    Prune {
        before_ms: i64,
        orders_before: u64,
    },
    /// Answered once every write sent before it is committed.
    Flush(oneshot::Sender<()>),
}

struct TenantRow {
    tenant: String,
    pinned: bool,
    last_opened_ms: Option<i64>,
    warmed_ms: Option<i64>,
    tail_bytes: u64,
    shared_at_cut: u64,
    moved_at: Option<u64>,
    /// None in a row an earlier build wrote, which kept no such sequence.
    touched_at: Option<u64>,
}

fn open_directory(path: &Path) -> Result<Connection, rusqlite::Error> {
    let connection = Connection::open(path)?;
    connection.busy_timeout(Duration::from_secs(5))?;
    connection.execute_batch(
        "PRAGMA journal_mode = WAL;
         PRAGMA synchronous = NORMAL;
         CREATE TABLE IF NOT EXISTS tenants (
             tenant TEXT PRIMARY KEY,
             pinned INTEGER NOT NULL DEFAULT 0,
             last_opened_ms INTEGER,
             warmed_ms INTEGER,
             tail_bytes INTEGER NOT NULL DEFAULT 0,
             shared_at_cut INTEGER NOT NULL DEFAULT 0,
             moved_at INTEGER,
             touched_at INTEGER
         );
         CREATE TABLE IF NOT EXISTS opens (
             at_ms INTEGER NOT NULL,
             tenant TEXT NOT NULL,
             size_bytes INTEGER,
             warm INTEGER NOT NULL
         );
         CREATE INDEX IF NOT EXISTS opens_at ON opens (at_ms);
         CREATE TABLE IF NOT EXISTS follower (
             id INTEGER PRIMARY KEY CHECK (id = 1),
             source_epoch TEXT NOT NULL,
             sequence INTEGER NOT NULL,
             shared_bytes INTEGER NOT NULL DEFAULT 0,
             shared_touched_at INTEGER
         );
         CREATE TABLE IF NOT EXISTS orders (
             id TEXT PRIMARY KEY,
             expires_at INTEGER NOT NULL
         );",
    )?;
    // A directory an earlier build wrote counts changes, not bytes, and
    // keeps no sequence at which a tenant last changed, which reads as
    // unknown.
    for (table, column, kind) in [
        ("tenants", "tail_bytes", "INTEGER NOT NULL DEFAULT 0"),
        ("tenants", "shared_at_cut", "INTEGER NOT NULL DEFAULT 0"),
        ("follower", "shared_bytes", "INTEGER NOT NULL DEFAULT 0"),
        ("tenants", "touched_at", "INTEGER"),
        ("follower", "shared_touched_at", "INTEGER"),
    ] {
        let present: bool = connection.query_row(
            "SELECT count(*) > 0 FROM pragma_table_info(?1) WHERE name = ?2",
            [table, column],
            |row| row.get(0),
        )?;
        if !present {
            connection.execute_batch(&format!("ALTER TABLE {table} ADD COLUMN {column} {kind}"))?;
        }
    }
    Ok(connection)
}

/// The source epoch, log sequence and shared counts the follower last
/// applied.
struct FollowerPosition {
    source_epoch: String,
    sequence: u64,
    shared_bytes: u64,
    shared_touched_at: Option<u64>,
}

struct Directory {
    tenants: Vec<TenantRow>,
    follower: Option<FollowerPosition>,
    orders: HashMap<String, u64>,
}

fn unsigned(value: i64) -> u64 {
    u64::try_from(value).unwrap_or(0)
}

fn signed(value: u64) -> i64 {
    i64::try_from(value).unwrap_or(i64::MAX)
}

fn load_directory(connection: &Connection, now_seconds: u64) -> rusqlite::Result<Directory> {
    let mut statement = connection.prepare(
        "SELECT tenant, pinned, last_opened_ms, warmed_ms, tail_bytes, shared_at_cut, moved_at,
                touched_at
           FROM tenants",
    )?;
    let tenants = statement
        .query_map([], |row| {
            Ok(TenantRow {
                tenant: row.get(0)?,
                pinned: row.get::<_, i64>(1)? != 0,
                last_opened_ms: row.get(2)?,
                warmed_ms: row.get(3)?,
                tail_bytes: unsigned(row.get(4)?),
                shared_at_cut: unsigned(row.get(5)?),
                moved_at: row
                    .get::<_, Option<i64>>(6)?
                    .and_then(|value| u64::try_from(value).ok()),
                touched_at: row.get::<_, Option<i64>>(7)?.map(unsigned),
            })
        })?
        .collect::<Result<Vec<_>, _>>()?;
    let follower = connection
        .query_row(
            "SELECT source_epoch, sequence, shared_bytes, shared_touched_at
               FROM follower WHERE id = 1",
            [],
            |row| {
                Ok(FollowerPosition {
                    source_epoch: row.get(0)?,
                    sequence: unsigned(row.get(1)?),
                    shared_bytes: unsigned(row.get(2)?),
                    shared_touched_at: row.get::<_, Option<i64>>(3)?.map(unsigned),
                })
            },
        )
        .optional()?;
    let mut statement =
        connection.prepare("SELECT id, expires_at FROM orders WHERE expires_at > ?1")?;
    let orders = statement
        .query_map(
            [signed(now_seconds.saturating_sub(ORDER_ID_MARGIN_SECONDS))],
            |row| Ok((row.get::<_, String>(0)?, unsigned(row.get(1)?))),
        )?
        .collect::<Result<HashMap<_, _>, _>>()?;
    Ok(Directory {
        tenants,
        follower,
        orders,
    })
}

/// Applies directory writes in the order they were sent, on a thread of its
/// own, so a bootstrap never waits on SQLite.
fn spawn_directory_writer(
    mut connection: Connection,
) -> Result<mpsc::Sender<DirectoryWrite>, std::io::Error> {
    let (sender, receiver) = mpsc::channel::<DirectoryWrite>();
    std::thread::Builder::new()
        .name("calque-tenant-directory".to_owned())
        .spawn(move || {
            while let Ok(first) = receiver.recv() {
                let mut batch = vec![first];
                batch.extend(receiver.try_iter());
                if let Err(error) = apply_writes(&mut connection, batch) {
                    tracing::warn!(target: "calque::tenants", %error, "the tenant directory was not written");
                }
            }
        })?;
    Ok(sender)
}

/// A row's `touched_at` only grows: a row an earlier build wrote without one
/// takes the first written.
const TOUCHED: &str =
    "touched_at = max(coalesce(tenants.touched_at, excluded.touched_at), excluded.touched_at)";

fn apply_writes(connection: &mut Connection, batch: Vec<DirectoryWrite>) -> rusqlite::Result<()> {
    let transaction = connection.transaction()?;
    let mut flushed = Vec::new();
    for write in batch {
        match write {
            DirectoryWrite::Flush(done) => flushed.push(done),
            DirectoryWrite::Opened {
                tenant,
                at_ms,
                size_bytes,
                warm,
                touched_at,
            } => {
                transaction.execute(
                    &format!(
                        "INSERT INTO tenants (tenant, last_opened_ms, touched_at) VALUES (?1, ?2, ?3)
                         ON CONFLICT (tenant) DO UPDATE SET last_opened_ms = excluded.last_opened_ms,
                            {TOUCHED}"
                    ),
                    params![tenant, at_ms, signed(touched_at)],
                )?;
                transaction.execute(
                    "INSERT INTO opens (at_ms, tenant, size_bytes, warm) VALUES (?1, ?2, ?3, ?4)",
                    params![
                        at_ms,
                        tenant,
                        size_bytes.and_then(|bytes| i64::try_from(bytes).ok()),
                        i64::from(warm)
                    ],
                )?;
            }
            DirectoryWrite::Warmed {
                tenant,
                at_ms,
                touched_at,
            } => {
                transaction.execute(
                    &format!(
                        "INSERT INTO tenants (tenant, warmed_ms, touched_at) VALUES (?1, ?2, ?3)
                         ON CONFLICT (tenant) DO UPDATE SET warmed_ms = excluded.warmed_ms, {TOUCHED}"
                    ),
                    params![tenant, at_ms, signed(touched_at)],
                )?;
            }
            DirectoryWrite::Pinned {
                tenant,
                pinned,
                touched_at,
            } => {
                transaction.execute(
                    &format!(
                        "INSERT INTO tenants (tenant, pinned, touched_at) VALUES (?1, ?2, ?3)
                         ON CONFLICT (tenant) DO UPDATE SET pinned = excluded.pinned, {TOUCHED}"
                    ),
                    params![tenant, i64::from(pinned), signed(touched_at)],
                )?;
            }
            DirectoryWrite::Touched { tenant, touched_at } => {
                transaction.execute(
                    &format!(
                        "INSERT INTO tenants (tenant, touched_at) VALUES (?1, ?2)
                         ON CONFLICT (tenant) DO UPDATE SET {TOUCHED}"
                    ),
                    params![tenant, signed(touched_at)],
                )?;
            }
            DirectoryWrite::Evicted { tenant } => {
                transaction.execute("DELETE FROM tenants WHERE tenant = ?1", [tenant])?;
            }
            DirectoryWrite::Tail {
                tenant,
                tail_bytes,
                shared_at_cut,
            } => {
                transaction.execute(
                    "INSERT INTO tenants (tenant, tail_bytes, shared_at_cut) VALUES (?1, ?2, ?3)
                     ON CONFLICT (tenant) DO UPDATE
                        SET tail_bytes = excluded.tail_bytes, shared_at_cut = excluded.shared_at_cut",
                    params![tenant, signed(tail_bytes), signed(shared_at_cut)],
                )?;
            }
            DirectoryWrite::Moved { tenant, sequence } => {
                transaction.execute(
                    "INSERT INTO tenants (tenant, moved_at) VALUES (?1, ?2)
                     ON CONFLICT (tenant) DO UPDATE SET moved_at = excluded.moved_at",
                    params![tenant, i64::try_from(sequence).unwrap_or(i64::MAX)],
                )?;
            }
            DirectoryWrite::Followed {
                source_epoch,
                sequence,
                shared_bytes,
                shared_touched_at,
            } => {
                transaction.execute(
                    "INSERT INTO follower (id, source_epoch, sequence, shared_bytes, shared_touched_at)
                     VALUES (1, ?1, ?2, ?3, ?4)
                     ON CONFLICT (id) DO UPDATE
                        SET source_epoch = excluded.source_epoch, sequence = excluded.sequence,
                            shared_bytes = excluded.shared_bytes,
                            shared_touched_at = excluded.shared_touched_at",
                    params![
                        source_epoch,
                        signed(sequence),
                        signed(shared_bytes),
                        signed(shared_touched_at)
                    ],
                )?;
            }
            DirectoryWrite::Order { id, expires_at } => {
                transaction.execute(
                    "INSERT OR IGNORE INTO orders (id, expires_at) VALUES (?1, ?2)",
                    params![id, signed(expires_at)],
                )?;
            }
            DirectoryWrite::Prune {
                before_ms,
                orders_before,
            } => {
                transaction.execute("DELETE FROM opens WHERE at_ms < ?1", [before_ms])?;
                transaction.execute(
                    "DELETE FROM orders WHERE expires_at <= ?1",
                    [signed(orders_before)],
                )?;
            }
        }
    }
    transaction.commit()?;
    for done in flushed {
        let _ = done.send(());
    }
    Ok(())
}

fn parse_tenant(text: &str, codec: PostgresTypeCodec) -> Option<PartitionValue> {
    match codec {
        PostgresTypeCodec::Uuid => Uuid::parse_str(text)
            .ok()
            .map(|uuid| PartitionValue::Uuid(*uuid.as_bytes())),
        _ => text.parse::<i64>().ok().map(PartitionValue::Int),
    }
}

fn now_seconds() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |elapsed| elapsed.as_secs())
}

fn now_ms() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .ok()
        .and_then(|elapsed| i64::try_from(elapsed.as_millis()).ok())
        .unwrap_or(0)
}

/// The cut, size and age one published manifest describes.
fn cut_of(base: &PreparedLocalSource, manifest: &ValidatedArtifactManifest) -> Option<Cut> {
    let manifest = manifest.manifest();
    let position = base
        .activated
        .cursor_codec()
        .decode(&manifest.pit.snapshot_cursor)
        .ok()?;
    let built_ms = OffsetDateTime::parse(&manifest.created_at, &Rfc3339)
        .ok()
        .and_then(|created| i64::try_from(created.unix_timestamp_nanos() / 1_000_000).ok())
        .unwrap_or_else(now_ms);
    Some(Cut {
        generation: manifest.compatibility.authorization_generation,
        cursor: manifest.pit.snapshot_cursor.clone(),
        sequence: position.sequence,
        decoded_bytes: manifest.decoded.bytes,
        built_ms,
    })
}

/// Catalog work decided under the state lock and done after it.
enum Retire {
    /// Every published artifact of the tenant's scope but those of `keep`'s
    /// cut.
    Scope {
        prepared: Box<PreparedLocalSource>,
        keep: Option<u64>,
        reason: &'static str,
    },
    /// Every published artifact.
    All { reason: &'static str },
    One {
        artifact: ArtifactId,
        reason: &'static str,
    },
}

enum BuildEnd {
    Published(Box<ValidatedArtifactManifest>),
    /// The tenant's working set passes a bound on what one artifact holds.
    OverBound,
    /// A row moved into or out of the tenant between the build's snapshot
    /// and its cut.
    Moved,
    Failed,
}

/// One build the scheduler has started.
struct Started {
    value: PartitionValue,
    prepared: PreparedLocalSource,
    build: u64,
    generation: u64,
    cancellation: CancellationToken,
    priority: Priority,
    queued: Instant,
}

/// The warm tenant whose log since its cut is longest against its artifact.
pub(crate) struct TenantTail {
    pub(crate) cut_sequence: u64,
    pub(crate) tail_bytes: u64,
    pub(crate) threshold_bytes: u64,
}

fn retire_in(catalog: &mut SqliteArtifactCatalog, retire: Retire) {
    let now = now_ms();
    let outcome = (|| -> Result<(), TenantError> {
        match retire {
            Retire::Scope {
                prepared,
                keep,
                reason,
            } => {
                let query = published_query_for(&prepared)?;
                for manifest in catalog.published(&query)? {
                    let cut = cut_of(&prepared, &manifest).map(|cut| cut.sequence);
                    if keep.is_none() || cut != keep {
                        catalog.retire_superseded(&manifest.manifest().artifact_id, now, reason)?;
                    }
                }
            }
            Retire::All { reason } => {
                for artifact in catalog.artifacts_in_state(ArtifactState::Published)? {
                    catalog.retire_superseded(&artifact, now, reason)?;
                }
            }
            Retire::One { artifact, reason } => {
                if catalog.state(&artifact)? == Some(ArtifactState::Published) {
                    catalog.retire_superseded(&artifact, now, reason)?;
                }
            }
        }
        Ok(())
    })();
    if let Err(error) = outcome {
        tracing::warn!(target: "calque::tenants", %error, "a superseded tenant artifact was not retired");
    }
}

/// Holds a value whose drop closes a synchronous PostgreSQL client, which
/// panics on an async runtime's thread, and drops it on a thread of its own
/// when the last handle goes away on one.
struct OffRuntime<T: Send + 'static>(Option<T>);

impl<T: Send + 'static> std::ops::Deref for OffRuntime<T> {
    type Target = T;

    fn deref(&self) -> &T {
        self.0.as_ref().expect("held until dropped")
    }
}

impl<T: Send + 'static> Drop for OffRuntime<T> {
    fn drop(&mut self) {
        let Some(value) = self.0.take() else {
            return;
        };
        if tokio::runtime::Handle::try_current().is_ok() {
            let _ = std::thread::Builder::new()
                .name("calque-tenant-drop".to_owned())
                .spawn(move || drop(value));
        }
    }
}

struct Inner {
    settings: TenantSettings,
    database_url: DatabaseUrl,
    state_root: PathBuf,
    journal_root: PathBuf,
    catalog_path: PathBuf,
    store: Arc<dyn ManagedBlobStore>,
    provider: OffRuntime<Arc<StdMutex<PostgresFencedCanonicalLog>>>,
    head_signal: CanonicalHeadSignal,
    /// The follower's reader, and the bootstrap planners' readers, from which
    /// an open reads the log's head.
    follower_reader: OffRuntime<Arc<LogReaders>>,
    head_readers: OffRuntime<Arc<LogReaders>>,
    cancellation: CancellationToken,
    metrics: OperationsMetrics,
    directory: StdMutex<mpsc::Sender<DirectoryWrite>>,
    state: StdMutex<TenantState>,
    /// Held exclusively while a tenant's artifacts are retired or its cut
    /// replaced, and shared by a background stage from the moment it asks
    /// whether it may publish to its publication, so it never publishes for a
    /// cut that has just been replaced.
    publish_fence: RwLock<()>,
    /// The last log sequence the follower has read.
    followed: watch::Sender<u64>,
    /// The retired artifacts the last sweep found. One is deleted only once
    /// a later sweep finds it still retired, so a plan that selected it just
    /// before it was retired has pinned it by then.
    retired_seen: StdMutex<BTreeSet<ArtifactId>>,
}

/// Every tenant's working set, for one partitioned scope.
#[derive(Clone)]
pub struct Tenants {
    inner: Arc<Inner>,
}

impl Tenants {
    /// Recovers every tenant build a stopped process left, reads the
    /// directory, and finds each tenant's published artifact. Every other
    /// published artifact is retired. Nothing builds until [`Self::start`].
    pub async fn open(
        config: TenantsConfig,
        prepared: &PreparedLocalSource,
    ) -> Result<Self, TenantError> {
        let settings = config.settings;
        if settings.max_builds == 0 || settings.max_builds > 64 {
            return Err(TenantError::InvalidSettings("the build cap is 1 to 64"));
        }
        if settings.hold_open.is_zero() || settings.hold_open > Duration::from_secs(60) {
            return Err(TenantError::InvalidSettings(
                "a bootstrap is held open for 1 to 60 seconds",
            ));
        }
        if settings.warm_days == 0
            || settings.max_tenant_bytes == 0
            || settings.max_tenant_bytes > MAX_LOCAL_ARTIFACT_BYTES
        {
            return Err(TenantError::InvalidSettings(
                "warm days and the tenant bound are positive",
            ));
        }
        let (_, codec) = prepared
            .policy
            .partition()
            .ok_or(TenantError::NotPartitioned)?;
        let journal_root = journal_root(&config.builder.state_root);
        std::fs::create_dir_all(&journal_root)?;
        {
            let mut catalog = SqliteArtifactCatalog::open(&config.builder.catalog_path)?;
            let recovered = recover_scope_builds(
                &config.builder.database_url,
                &config.builder.state_root,
                &journal_root,
                prepared,
                Arc::clone(&config.builder.barrier_provider),
                &mut catalog,
                &config.builder.store,
            )
            .await?;
            if recovered > 0 {
                tracing::info!(target: "calque::tenants", recovered, "interrupted tenant builds recovered");
            }
        }

        let directory_path = config.builder.state_root.join(DIRECTORY_FILE);
        let connection = open_directory(&directory_path)?;
        let Directory {
            tenants: rows,
            follower,
            orders,
        } = load_directory(&connection, now_seconds())?;
        let directory = spawn_directory_writer(connection)?;

        let epoch = prepared.activated.source_epoch().as_str().to_owned();
        // A sequence the directory kept of another log, or none, is unknown,
        // and reads as the follower's position: everything may have changed
        // up to it.
        let (follower, shared_bytes, shared_touched_at, known) = match follower {
            Some(position) if position.source_epoch == epoch => (
                position.sequence,
                position.shared_bytes,
                position.shared_touched_at.unwrap_or(position.sequence),
                true,
            ),
            _ => {
                let reader = config.log_readers.tenants.acquire().await?;
                let head = tokio::task::spawn_blocking(move || reader.head())
                    .await
                    .map_err(|_| TenantError::Task)??;
                (head.sequence, 0, head.sequence, false)
            }
        };
        let _ = directory.send(DirectoryWrite::Followed {
            source_epoch: epoch,
            sequence: follower,
            shared_bytes,
            shared_touched_at,
        });

        let mut state = TenantState {
            base: Arc::new(prepared.clone()),
            codec,
            entries: HashMap::new(),
            queue: BuildQueue::default(),
            compress_queue: VecDeque::new(),
            running_builds: 0,
            compressing: false,
            next_order: 0,
            next_build: 0,
            shared_bytes,
            shared_touched_at,
            followed: follower,
            orders,
        };
        let mut catalog = SqliteArtifactCatalog::open(&config.builder.catalog_path)?;
        let mut kept = BTreeSet::new();
        let now = now_ms();
        for row in rows {
            let Some(value) = parse_tenant(&row.tenant, codec) else {
                continue;
            };
            let Some(tenant) = state.prepared_for(&value) else {
                continue;
            };
            let entry = state.entry(&value);
            entry.pinned = row.pinned;
            entry.last_opened_ms = row.last_opened_ms;
            entry.warmed_ms = row.warmed_ms;
            entry.tail_bytes = row.tail_bytes;
            entry.shared_at_cut = row.shared_at_cut;
            entry.moved_at = row.moved_at;
            entry.touched_at = row.touched_at.filter(|_| known).unwrap_or(follower);
            if !entry.warm(now, settings.warm_days) {
                continue;
            }
            let published = catalog.published(&published_query_for(&tenant)?)?;
            let cuts = published
                .iter()
                .map(|manifest| (manifest, cut_of(&tenant, manifest)))
                .filter_map(|(manifest, cut)| Some((manifest, cut?)))
                .filter(|(_, cut)| row.moved_at.is_none_or(|moved| cut.sequence >= moved))
                .collect::<Vec<_>>();
            let newest = cuts
                .iter()
                .map(|(_, cut)| cut)
                .max_by_key(|cut| cut.sequence)
                .cloned();
            for (manifest, cut) in &cuts {
                if newest
                    .as_ref()
                    .is_some_and(|newest| newest.sequence == cut.sequence)
                {
                    kept.insert(manifest.manifest().artifact_id.clone());
                }
            }
            let entry = state.entry(&value);
            entry.cut = newest;
            if entry.cut.is_none() {
                state.request_build(&value, Priority::Refresh);
            }
        }
        for artifact in catalog.artifacts_in_state(ArtifactState::Published)? {
            if !kept.contains(&artifact) {
                catalog.retire_superseded(&artifact, now, "no tenant serves it")?;
            }
        }
        drop(catalog);

        let warm = state
            .entries
            .values()
            .filter(|entry| entry.cut.is_some())
            .count();
        tracing::info!(
            target: "calque::tenants",
            tenants = state.entries.len(),
            warm,
            queued = state.queue.len(),
            "tenants restored"
        );
        Ok(Self {
            inner: Arc::new(Inner {
                settings,
                database_url: config.builder.database_url,
                state_root: config.builder.state_root,
                journal_root,
                catalog_path: config.builder.catalog_path,
                store: config.builder.store,
                provider: OffRuntime(Some(config.builder.barrier_provider)),
                head_signal: config.builder.head_signal,
                follower_reader: OffRuntime(Some(Arc::clone(&config.log_readers.tenants))),
                head_readers: OffRuntime(Some(Arc::clone(&config.log_readers.planners))),
                cancellation: config.builder.cancellation,
                metrics: config.builder.metrics,
                directory: StdMutex::new(directory),
                state: StdMutex::new(state),
                publish_fence: RwLock::new(()),
                followed: watch::Sender::new(follower),
                retired_seen: StdMutex::new(BTreeSet::new()),
            }),
        })
    }

    /// Starts the builds the restore queued, the log follower and the sweep.
    pub fn start(&self) {
        let follower = self.clone();
        tokio::spawn(async move { follower.follow().await });
        let sweeper = self.clone();
        tokio::spawn(async move { sweeper.sweep_forever().await });
        self.schedule();
    }

    pub fn settings(&self) -> &TenantSettings {
        &self.inner.settings
    }

    /// The last log sequence any tenant may still read; the log keeps every
    /// entry after it. A cut made later falls after it.
    pub(crate) fn retained_after(&self) -> u64 {
        let followed = *self.inner.followed.borrow();
        retained_after(self.state().entries.values(), followed)
    }

    fn state(&self) -> std::sync::MutexGuard<'_, TenantState> {
        self.inner
            .state
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
    }

    fn write(&self, write: DirectoryWrite) {
        let _ = self
            .inner
            .directory
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .send(write);
    }

    /// Admits one bootstrap for `value`'s tenant under `generation`: at once
    /// when its artifact is current, after its build when that ends within
    /// the hold, and otherwise as preparing. Every open is recorded.
    pub(crate) async fn admit(
        &self,
        generation: u64,
        value: &PartitionValue,
    ) -> Result<(), BootstrapHttpError> {
        let deadline = tokio::time::Instant::now() + self.inner.settings.hold_open;
        let opened = Instant::now();
        let (mut changed, interrupted) = self.open_tenant(generation, value)?;
        let mut cold = false;
        loop {
            // A tenant is planned only once the follower has read the log to
            // its head, so a cut a row has just left is not planned from.
            if self.prepared(generation, value, interrupted)? {
                self.sync_follower().await;
                if self.prepared(generation, value, interrupted)? {
                    break;
                }
            }
            cold = true;
            self.schedule();
            tokio::select! {
                result = changed.changed() => {
                    if result.is_err() {
                        return Err(BootstrapHttpError::Unavailable);
                    }
                }
                () = tokio::time::sleep_until(deadline) => {
                    return Err(BootstrapHttpError::ArtifactPreparing {
                        retry_after_seconds: PREPARING_RETRY_AFTER_SECONDS,
                    });
                }
            }
        }
        if cold {
            tracing::info!(
                target: "calque::tenants",
                tenant = %value,
                waited_ms = u64::try_from(opened.elapsed().as_millis()).unwrap_or(u64::MAX),
                "cold tenant answered"
            );
        }
        Ok(())
    }

    /// Records one open, queues the tenant's build when it has no current
    /// artifact, and returns what to wait on while it is built.
    fn open_tenant(
        &self,
        generation: u64,
        value: &PartitionValue,
    ) -> Result<(watch::Receiver<u64>, u64), BootstrapHttpError> {
        let mut state = self.state();
        if state.generation() != generation {
            return Err(BootstrapHttpError::Unavailable);
        }
        let now = now_ms();
        let entry = state.entry(value);
        entry.last_opened_ms = Some(now);
        let current = entry.current(generation).map(|cut| cut.decoded_bytes);
        self.write(DirectoryWrite::Opened {
            tenant: value.to_string(),
            at_ms: now,
            size_bytes: current,
            warm: current.is_some(),
            touched_at: entry.touched_at,
        });
        tracing::info!(
            target: "calque::tenants",
            tenant = %value,
            warm = current.is_some(),
            size_bytes = current,
            "tenant opened"
        );
        if current.is_none() && entry.refused {
            return Err(BootstrapHttpError::CapacityExceeded);
        }
        let receiver = entry.changed.subscribe();
        let interrupted = entry.interrupted;
        if current.is_none() {
            if entry.job.is_none() {
                entry.moved_builds = 0;
            }
            state.request_build(value, Priority::Waiting);
        }
        Ok((receiver, interrupted))
    }

    /// Whether the tenant's artifact is current: `Ok(false)` while its build
    /// runs or is queued. A request whose build was dropped since it opened
    /// is asked to come again.
    fn prepared(
        &self,
        generation: u64,
        value: &PartitionValue,
        interrupted: u64,
    ) -> Result<bool, BootstrapHttpError> {
        let mut state = self.state();
        if state.generation() != generation {
            return Err(BootstrapHttpError::Unavailable);
        }
        let entry = state.entry(value);
        if entry.current(generation).is_some() {
            return Ok(true);
        }
        if entry.refused {
            return Err(BootstrapHttpError::CapacityExceeded);
        }
        if entry.job.is_none() {
            return Err(if entry.interrupted == interrupted {
                BootstrapHttpError::Unavailable
            } else {
                BootstrapHttpError::ArtifactPreparing {
                    retry_after_seconds: PREPARING_RETRY_AFTER_SECONDS,
                }
            });
        }
        Ok(false)
    }

    /// What the follower knows of `value`'s scope: the last sequence at which
    /// an operation it reads committed, and how far the follower has read.
    pub(crate) fn unchanged(&self, value: &PartitionValue) -> Option<UnchangedScope> {
        self.state().unchanged(value)
    }

    /// Waits, briefly, for the follower to read the log up to its head.
    async fn sync_follower(&self) {
        let Ok(reader) = self.inner.head_readers.acquire().await else {
            return;
        };
        let head = tokio::task::spawn_blocking(move || reader.head()).await;
        let Ok(Ok(head)) = head else {
            return;
        };
        let mut followed = self.inner.followed.subscribe();
        let _ = tokio::time::timeout(
            FOLLOWER_SYNC,
            followed.wait_for(|sequence| *sequence >= head.sequence),
        )
        .await;
    }

    /// Acts on one authenticated tenant order, once, and answers once what it
    /// changed is on disk. An order's id is kept until after it expires, in
    /// the directory before anything the order changes, so neither a second
    /// presentation nor a restart acts on it again.
    pub(crate) async fn order(
        &self,
        order: &VerifiedTenantOrder,
    ) -> Result<TenantReport, TenantOrderRefusal> {
        let action = order.action;
        let fence = if action == TenantAction::Evict {
            Some(self.inner.publish_fence.write().await)
        } else {
            None
        };
        let mut retire = Vec::new();
        let report = {
            let mut state = self.state();
            let value = PartitionValue::from_principal(&order.tenant, state.codec)
                .ok_or(TenantOrderRefusal::InvalidRequest)?;
            if state.orders.contains_key(&order.order_id) {
                tracing::warn!(target: "calque::tenants", tenant = %value, ?action, "a tenant order was presented again and refused");
                return Err(TenantOrderRefusal::Replayed);
            }
            state
                .orders
                .insert(order.order_id.clone(), order.expires_at_unix_seconds);
            self.write(DirectoryWrite::Order {
                id: order.order_id.clone(),
                expires_at: order.expires_at_unix_seconds,
            });
            let generation = state.generation();
            let now = now_ms();
            let needs_build = state
                .entries
                .get(&value)
                .is_none_or(|entry| entry.current(generation).is_none());
            match action {
                TenantAction::Warm | TenantAction::Pin => {
                    let entry = state.entry(&value);
                    // The application asking again retries a tenant once refused.
                    entry.refused = false;
                    if action == TenantAction::Warm {
                        entry.warmed_ms = Some(now);
                        self.write(DirectoryWrite::Warmed {
                            tenant: value.to_string(),
                            at_ms: now,
                            touched_at: entry.touched_at,
                        });
                    } else {
                        entry.pinned = true;
                        self.write(DirectoryWrite::Pinned {
                            tenant: value.to_string(),
                            pinned: true,
                            touched_at: entry.touched_at,
                        });
                    }
                    if needs_build {
                        state.request_build(&value, Priority::Warm);
                    }
                }
                TenantAction::Unpin => {
                    let entry = state.entry(&value);
                    entry.pinned = false;
                    self.write(DirectoryWrite::Pinned {
                        tenant: value.to_string(),
                        pinned: false,
                        touched_at: entry.touched_at,
                    });
                }
                TenantAction::State => {}
                TenantAction::Evict => {
                    // A build in progress is dropped with the rest: its
                    // result is discarded, and a plan granted earlier keeps
                    // its artifact until it expires.
                    state.drop_build(&value);
                    let prepared = state.prepared_for(&value);
                    let entry = state.entry(&value);
                    entry.pinned = false;
                    entry.last_opened_ms = None;
                    entry.warmed_ms = None;
                    entry.cut = None;
                    entry.interrupted += 1;
                    entry.notify();
                    self.write(DirectoryWrite::Evicted {
                        tenant: value.to_string(),
                    });
                    tracing::info!(target: "calque::tenants", tenant = %value, "tenant evicted");
                    if let Some(prepared) = prepared {
                        retire.push(Retire::Scope {
                            prepared: Box::new(prepared),
                            keep: None,
                            reason: "the tenant was evicted",
                        });
                    }
                }
            }
            tracing::info!(target: "calque::tenants", tenant = %value, ?action, "tenant order");
            match state.entries.get(&value) {
                Some(entry) => report_of(&value, entry, generation, now),
                None => report_of(&value, &Entry::new(0), generation, now),
            }
        };
        self.retire_now(retire).await;
        drop(fence);
        if matches!(action, TenantAction::Warm | TenantAction::Pin) {
            self.schedule();
        }
        if action != TenantAction::State {
            let (done, flushed) = oneshot::channel();
            self.write(DirectoryWrite::Flush(done));
            flushed.await.map_err(|_| TenantOrderRefusal::Unavailable)?;
        }
        Ok(report)
    }

    /// Moves every tenant to the generation a fence put in force. Every
    /// artifact of the retired generation is retired, a build of it is
    /// cancelled, and each warm tenant is built again.
    pub(crate) async fn move_to_generation(&self, prepared: PreparedLocalSource) {
        let _fence = self.inner.publish_fence.write().await;
        {
            let mut state = self.state();
            state.base = Arc::new(prepared);
            // The new policy may read what the old one did not.
            state.shared_touched_at = state.followed;
            state.queue.clear();
            state.compress_queue.clear();
            let now = now_ms();
            let warm_days = self.inner.settings.warm_days;
            let mut rebuild = Vec::new();
            for (value, entry) in &mut state.entries {
                if let Some(Job::Running { cancellation, .. }) = &entry.job {
                    cancellation.cancel();
                } else {
                    entry.job = None;
                }
                entry.cut = None;
                entry.refused = false;
                entry.notify();
                if entry.warm(now, warm_days) {
                    rebuild.push(value.clone());
                }
            }
            for value in rebuild {
                state.request_build(&value, Priority::Refresh);
            }
        }
        let catalog_path = self.inner.catalog_path.clone();
        let _ = tokio::task::spawn_blocking(move || {
            if let Ok(mut catalog) = SqliteArtifactCatalog::open(&catalog_path) {
                retire_in(
                    &mut catalog,
                    Retire::All {
                        reason: "its authorization generation was fenced",
                    },
                );
            }
        })
        .await;
        drop(_fence);
        self.schedule();
    }

    /// Starts every queued build the cap leaves room for, and the background
    /// stage when no build is queued.
    fn schedule(&self) {
        let mut builds = Vec::new();
        let mut compress = None;
        {
            let mut state = self.state();
            let aging = self.inner.settings.refresh.min_interval;
            while state.running_builds < self.inner.settings.max_builds {
                let Some((priority, value)) = state.queue.pop(Instant::now(), aging) else {
                    break;
                };
                let generation = state.generation();
                let Some(prepared) = state.prepared_for(&value) else {
                    state.entry(&value).job = None;
                    continue;
                };
                let cancellation = self.inner.cancellation.child_token();
                let build = state.next_build;
                state.next_build += 1;
                let entry = state.entry(&value);
                let queued = match &entry.job {
                    Some(Job::Queued { queued, .. }) => *queued,
                    _ => Instant::now(),
                };
                entry.job = Some(Job::Running {
                    build,
                    cancellation: cancellation.clone(),
                    held_from: *self.inner.followed.borrow(),
                });
                state.running_builds += 1;
                builds.push(Started {
                    value,
                    prepared,
                    build,
                    generation,
                    cancellation,
                    priority,
                    queued,
                });
            }
            if state.queue.is_empty() && !state.compressing {
                let generation = state.generation();
                while let Some(value) = state.compress_queue.pop_front() {
                    let Some(prepared) = state.prepared_for(&value) else {
                        continue;
                    };
                    let Some(cut) = state.entry(&value).current(generation).cloned() else {
                        continue;
                    };
                    state.compressing = true;
                    compress = Some((value, prepared, cut));
                    break;
                }
            }
        }
        for started in builds {
            let tenants = self.clone();
            let (value, build, generation) =
                (started.value.clone(), started.build, started.generation);
            let spawned = std::thread::Builder::new()
                .name("calque-tenant-build".to_owned())
                .spawn(move || tenants.run_build(started));
            if spawned.is_err() {
                self.finish_build(&value, build, generation, BuildEnd::Failed);
            }
        }
        if let Some((value, prepared, cut)) = compress {
            let tenants = self.clone();
            let spawned = std::thread::Builder::new()
                .name("calque-tenant-compress".to_owned())
                .spawn(move || tenants.run_compress(value, prepared, cut));
            if spawned.is_err() {
                self.state().compressing = false;
            }
        }
    }

    fn run_build(&self, started: Started) {
        let Started {
            value,
            prepared,
            build,
            generation,
            cancellation,
            priority,
            queued,
        } = started;
        let Ok(runtime) = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
        else {
            self.finish_build(&value, build, generation, BuildEnd::Failed);
            self.schedule();
            return;
        };
        let inner = &self.inner;
        tracing::info!(
            target: "calque::tenants",
            tenant = %value,
            priority = priority.as_str(),
            queued_ms = queued.elapsed().as_millis() as u64,
            "tenant build started"
        );
        let started = Instant::now();
        runtime.block_on(async {
            let mut catalog = match SqliteArtifactCatalog::open(&inner.catalog_path) {
                Ok(catalog) => catalog,
                Err(error) => {
                    tracing::warn!(target: "calque::tenants", tenant = %value, %error, "tenant build failed");
                    self.finish_build(&value, build, generation, BuildEnd::Failed);
                    return;
                }
            };
            let nonce = Uuid::new_v4().simple().to_string();
            let result = build_scope_artifact(
                &inner.database_url,
                &inner.state_root,
                &inner.journal_root,
                &prepared,
                &nonce[..16],
                inner.settings.max_tenant_bytes,
                Arc::clone(&inner.provider),
                &inner.head_signal,
                &mut catalog,
                &inner.store,
                &cancellation,
                |stage| {
                    tracing::info!(
                        target: "calque::tenants",
                        tenant = %value,
                        ?stage,
                        elapsed_ms = started.elapsed().as_millis() as u64,
                        "tenant build stage entered"
                    );
                },
            )
            .await;
            let elapsed = started.elapsed();
            let _fence = inner.publish_fence.write().await;
            let end = match result {
                Ok(report) => {
                    inner.metrics.observe_artifact_build_seconds(elapsed.as_secs_f64());
                    let rows: u64 = report.manifest.manifest().tables.iter().map(|table| table.row_count).sum();
                    tracing::info!(
                        target: "calque::tenants",
                        tenant = %value,
                        artifact = %report.manifest.manifest().artifact_id,
                        rows,
                        decoded_bytes = report.decoded_bytes,
                        elapsed_ms = elapsed.as_millis() as u64,
                        "tenant artifact published"
                    );
                    BuildEnd::Published(Box::new(report.manifest))
                }
                Err(error) if error.exceeds_bound() => {
                    tracing::warn!(target: "calque::tenants", tenant = %value, %error, "tenant refused: its working set passes a bound on one artifact");
                    BuildEnd::OverBound
                }
                Err(error) if error.partition_moved() => {
                    tracing::info!(target: "calque::tenants", tenant = %value, "a row moved into or out of the tenant during its build");
                    BuildEnd::Moved
                }
                Err(error) => {
                    if cancellation.is_cancelled() {
                        tracing::info!(target: "calque::tenants", tenant = %value, "tenant build cancelled");
                    } else {
                        inner.metrics.record_error(OperationErrorClass::ArtifactBuild);
                        tracing::warn!(target: "calque::tenants", tenant = %value, %error, "tenant build failed");
                    }
                    BuildEnd::Failed
                }
            };
            let retire = self.finish_build(&value, build, generation, end);
            for retire in retire {
                retire_in(&mut catalog, retire);
            }
        });
        self.schedule();
    }

    /// Records one build's end under the state lock, and returns the catalog
    /// work it leaves.
    fn finish_build(
        &self,
        value: &PartitionValue,
        build: u64,
        generation: u64,
        end: BuildEnd,
    ) -> Vec<Retire> {
        let mut state = self.state();
        let (retire, tail) =
            state.finish_build(&self.inner.settings, value, build, generation, end);
        if let Some(tail) = tail {
            self.write(tail);
        }
        retire
    }

    fn run_compress(&self, value: PartitionValue, prepared: PreparedLocalSource, cut: Cut) {
        let inner = &self.inner;
        let outcome = (|| -> Result<(), String> {
            let runtime = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .map_err(|error| error.to_string())?;
            let query = published_query_for(&prepared).map_err(|error| error.to_string())?;
            let cancellation = inner.cancellation.child_token();
            let generation = cut.generation;
            let started = Instant::now();
            let (outcomes, replaced) = runtime
                .block_on(reencode_leaving_replaced(
                    ReencodeRequest {
                        state_root: &inner.state_root,
                        catalog_path: &inner.catalog_path,
                        store: inner.store.as_ref(),
                        query: &query,
                        cut: &cut.cursor,
                        fence: &inner.publish_fence,
                        cancellation: &cancellation,
                    },
                    || {
                        let mut state = self.state();
                        let current = state.generation();
                        current == generation
                            && state
                                .entry(&value)
                                .current(current)
                                .is_some_and(|current| current.sequence == cut.sequence)
                    },
                    |_| {},
                ))
                .map_err(|error| error.to_string())?;
            let mut catalog = SqliteArtifactCatalog::open(&inner.catalog_path)
                .map_err(|error| error.to_string())?;
            for artifact in replaced {
                retire_in(
                    &mut catalog,
                    Retire::One {
                        artifact,
                        reason: "replaced by its re-encode",
                    },
                );
            }
            tracing::info!(
                target: "calque::tenants",
                tenant = %value,
                zstd = ?outcomes.zstd,
                brotli = ?outcomes.brotli.as_ref().map(|outcome| outcome.as_ref().map_err(ToString::to_string)),
                elapsed_ms = started.elapsed().as_millis() as u64,
                "tenant background stages finished"
            );
            Ok(())
        })();
        if let Err(error) = outcome {
            tracing::warn!(target: "calque::tenants", tenant = %value, %error, "tenant background stages failed");
        }
        self.state().compressing = false;
        self.schedule();
    }

    /// Reads the log after the directory's position for as long as the
    /// process runs: counts the changes routed to each tenant, invalidates
    /// both tenants of a row that moves, and queues the refresh of a warm
    /// tenant past its bound.
    async fn follow(&self) {
        let mut head = self.inner.head_signal.subscribe();
        loop {
            if let Err(error) = self.follow_to_head().await {
                tracing::warn!(target: "calque::tenants", %error, "the tenant follower stopped reading the log");
            }
            tokio::select! {
                () = self.inner.cancellation.cancelled() => return,
                _ = head.changed() => {}
                () = tokio::time::sleep(FOLLOW_INTERVAL) => {}
            }
        }
    }

    async fn follow_to_head(&self) -> Result<(), TenantError> {
        let mut reader = Some(self.inner.follower_reader.acquire().await?);
        loop {
            let from = *self.inner.followed.borrow();
            let Some(held) = reader.take() else {
                return Ok(());
            };
            let (held, page) = tokio::task::spawn_blocking(move || {
                let page = held.read_page_after_bounded(
                    from,
                    None,
                    FOLLOW_PAGE_RECORDS,
                    FOLLOW_PAGE_BYTES,
                );
                (held, page)
            })
            .await
            .map_err(|_| TenantError::Task)?;
            reader = Some(held);
            let page = match page {
                Ok(page) => page,
                Err(LogError::BeforeRetention { minimum, .. }) => {
                    self.skip_to(minimum.saturating_sub(1)).await;
                    continue;
                }
                Err(error) => return Err(error.into()),
            };
            let Some(last) = page.records.last().map(|record| record.position.sequence) else {
                return Ok(());
            };
            let (shared_bytes, shared_touched_at) = self.apply_page(&page.records).await;
            self.inner.followed.send_replace(last);
            let epoch = self
                .state()
                .base
                .activated
                .source_epoch()
                .as_str()
                .to_owned();
            self.write(DirectoryWrite::Followed {
                source_epoch: epoch,
                sequence: last,
                shared_bytes,
                shared_touched_at,
            });
        }
    }

    /// The log no longer holds what the follower had yet to read, so a move
    /// in it cannot be seen: every tenant cut before the floor is dropped.
    async fn skip_to(&self, sequence: u64) {
        let _fence = self.inner.publish_fence.write().await;
        let mut retire = Vec::new();
        {
            let mut state = self.state();
            // Whatever the skipped records held is unknown.
            state.shared_touched_at = state.shared_touched_at.max(sequence);
            state.followed = state.followed.max(sequence);
            let values = state.entries.keys().cloned().collect::<Vec<_>>();
            for value in values {
                let prepared = state.prepared_for(&value);
                let entry = state.entry(&value);
                if entry
                    .cut
                    .as_ref()
                    .is_some_and(|cut| cut.sequence < sequence)
                {
                    entry.cut = None;
                    entry.notify();
                    if let Some(prepared) = prepared {
                        retire.push(Retire::Scope {
                            prepared: Box::new(prepared),
                            keep: None,
                            reason: "the log no longer holds its tail",
                        });
                    }
                }
            }
        }
        self.inner.followed.send_replace(sequence);
        self.retire_now(retire).await;
    }

    /// Returns the shared tables' bytes and the sequence they last changed
    /// at, read through `records`.
    async fn apply_page(&self, records: &[canonical_log::CanonicalRecord]) -> (u64, u64) {
        let mut moves = Vec::new();
        let mut counted = HashMap::<PartitionValue, (u64, u64)>::new();
        let mut touched = HashMap::<PartitionValue, u64>::new();
        let shared = {
            let mut state = self.state();
            let policy = state.base.policy.clone();
            for record in records {
                let CanonicalEntry::Transaction(transaction) = &record.entry else {
                    continue;
                };
                let sequence = record.position.sequence;
                for operation in &transaction.operations {
                    let touch = policy.partition_touch(operation);
                    let values = match &touch {
                        Ok(Some(PartitionTouch::Row(value))) => vec![value],
                        Ok(Some(PartitionTouch::Moved { from, to })) => vec![from, to],
                        Ok(Some(PartitionTouch::Every) | None) | Err(_) => {
                            state.shared_touched_at = sequence;
                            Vec::new()
                        }
                    };
                    for value in values {
                        if let Some(entry) = state.entries.get_mut(value) {
                            entry.touched_at = sequence;
                            touched.insert(value.clone(), sequence);
                        }
                    }
                    match touch {
                        Ok(Some(PartitionTouch::Row(value))) => {
                            let Some(entry) = state.entries.get_mut(&value) else {
                                continue;
                            };
                            if entry
                                .cut
                                .as_ref()
                                .is_some_and(|cut| sequence > cut.sequence)
                            {
                                entry.tail_bytes =
                                    entry.tail_bytes.saturating_add(operation.stored_len());
                                counted.insert(value, (entry.tail_bytes, entry.shared_at_cut));
                            }
                        }
                        Ok(Some(PartitionTouch::Moved { from, to })) => {
                            moves.push((from, sequence));
                            moves.push((to, sequence));
                        }
                        Ok(Some(PartitionTouch::Every) | None) => {
                            state.shared_bytes =
                                state.shared_bytes.saturating_add(operation.stored_len());
                        }
                        Err(_) => {}
                    }
                }
            }
            if let Some(last) = records.last() {
                state.followed = last.position.sequence;
            }
            (state.shared_bytes, state.shared_touched_at)
        };
        for (value, touched_at) in touched {
            self.write(DirectoryWrite::Touched {
                tenant: value.to_string(),
                touched_at,
            });
        }
        for (value, (tail_bytes, shared_at_cut)) in counted {
            self.write(DirectoryWrite::Tail {
                tenant: value.to_string(),
                tail_bytes,
                shared_at_cut,
            });
        }
        if !moves.is_empty() {
            self.invalidate_moved(moves).await;
        }
        self.queue_refreshes();
        shared
    }

    /// A row moved between two tenants at a sequence: neither tenant's cut
    /// before it can be patched forward, so both are retired and each warm
    /// one is built again.
    async fn invalidate_moved(&self, moves: Vec<(PartitionValue, u64)>) {
        let _fence = self.inner.publish_fence.write().await;
        let mut retire = Vec::new();
        {
            let mut state = self.state();
            let now = now_ms();
            let warm_days = self.inner.settings.warm_days;
            for (value, sequence) in moves {
                if !state.entries.contains_key(&value) {
                    continue;
                }
                let prepared = state.prepared_for(&value);
                let entry = state.entry(&value);
                entry.moved_at = Some(entry.moved_at.map_or(sequence, |moved| moved.max(sequence)));
                let had_cut = entry.cut.take().is_some();
                let waiting = entry.changed.receiver_count() > 0;
                let warm = entry.warm(now, warm_days) && entry.moved_builds <= MAX_MOVED_BUILDS;
                entry.notify();
                self.write(DirectoryWrite::Moved {
                    tenant: value.to_string(),
                    sequence,
                });
                tracing::info!(target: "calque::tenants", tenant = %value, sequence, had_cut, "a row moved out of or into the tenant; its cut is retired");
                if let Some(prepared) = prepared {
                    retire.push(Retire::Scope {
                        prepared: Box::new(prepared),
                        keep: None,
                        reason: "a row moved into or out of the tenant",
                    });
                }
                if warm || waiting {
                    state.request_build(
                        &value,
                        if waiting {
                            Priority::Waiting
                        } else {
                            Priority::Refresh
                        },
                    );
                }
            }
        }
        self.retire_now(retire).await;
        drop(_fence);
        self.schedule();
    }

    async fn retire_now(&self, retire: Vec<Retire>) {
        if retire.is_empty() {
            return;
        }
        let catalog_path = self.inner.catalog_path.clone();
        let _ = tokio::task::spawn_blocking(move || {
            if let Ok(mut catalog) = SqliteArtifactCatalog::open(&catalog_path) {
                for retire in retire {
                    retire_in(&mut catalog, retire);
                }
            }
        })
        .await;
    }

    /// Queues the refresh of every warm tenant the re-cut's rule finds due:
    /// the bytes a plan from its cut replays outgrow the ratio of its
    /// artifact, and the interval has passed since its cut and since its last
    /// refresh was queued.
    fn queue_refreshes(&self) {
        let queued = {
            let mut state = self.state();
            let generation = state.generation();
            let shared = state.shared_bytes;
            let now = now_ms();
            let policy = self.inner.settings.refresh;
            let warm_days = self.inner.settings.warm_days;
            let due = state
                .entries
                .iter()
                .filter(|(_, entry)| {
                    let Some(cut) = entry.current(generation) else {
                        return false;
                    };
                    entry.job.is_none()
                        && !entry.refused
                        && entry.warm(now, warm_days)
                        && policy.due(entry.tail(shared), cut.decoded_bytes)
                        && policy.rested(
                            Some(cut.built_ms.saturating_mul(1_000)),
                            now.saturating_mul(1_000),
                            entry.refresh_queued.map(|queued| queued.elapsed()),
                        )
                })
                .map(|(value, _)| value.clone())
                .collect::<Vec<_>>();
            for value in &due {
                tracing::info!(
                    target: "calque::tenants",
                    tenant = %value,
                    tail_bytes = state.entries.get(value).map_or(0, |entry| entry.tail(shared)),
                    "tenant refresh queued"
                );
                state.request_build(value, Priority::Refresh);
                if let Some(entry) = state.entries.get_mut(value) {
                    entry.refresh_queued = Some(Instant::now());
                }
            }
            !due.is_empty()
        };
        if queued {
            self.schedule();
        }
    }

    async fn sweep_forever(&self) {
        loop {
            self.sweep().await;
            tokio::select! {
                () = self.inner.cancellation.cancelled() => return,
                () = tokio::time::sleep(SWEEP_INTERVAL) => {}
            }
        }
    }

    /// Evicts every tenant neither opened within the recency window nor
    /// pinned, and forgets it once nothing waits on it, so the record holds
    /// the tenants opened or warmed within the window, the pinned ones and
    /// those being built. Deletes every retired artifact no plan still pins,
    /// prunes the open record and the expired orders, and queues each refresh
    /// the interval held back.
    async fn sweep(&self) {
        let fence = self.inner.publish_fence.write().await;
        let mut retire = Vec::new();
        let mut forgotten = 0_usize;
        {
            let mut state = self.state();
            let now = now_ms();
            let warm_days = self.inner.settings.warm_days;
            let values = state.entries.keys().cloned().collect::<Vec<_>>();
            for value in values {
                let prepared = state.prepared_for(&value);
                let entry = state.entry(&value);
                if entry.job.is_some() || entry.warm(now, warm_days) {
                    continue;
                }
                if entry.cut.take().is_some() {
                    entry.notify();
                    tracing::info!(target: "calque::tenants", tenant = %value, "tenant evicted");
                    if let Some(prepared) = prepared {
                        retire.push(Retire::Scope {
                            prepared: Box::new(prepared),
                            keep: None,
                            reason: "the tenant was evicted",
                        });
                    }
                }
                if entry.changed.receiver_count() == 0 {
                    state.entries.remove(&value);
                    self.write(DirectoryWrite::Evicted {
                        tenant: value.to_string(),
                    });
                    forgotten += 1;
                }
            }
            let expired = now_seconds().saturating_sub(ORDER_ID_MARGIN_SECONDS);
            state.orders.retain(|_, expires_at| *expires_at > expired);
        }
        if forgotten > 0 {
            tracing::info!(target: "calque::tenants", forgotten, "tenants neither warm nor pinned forgotten");
        }
        self.retire_now(retire).await;
        drop(fence);
        self.queue_refreshes();
        let catalog_path = self.inner.catalog_path.clone();
        let store = Arc::clone(&self.inner.store);
        let seen = std::mem::take(
            &mut *self
                .inner
                .retired_seen
                .lock()
                .unwrap_or_else(PoisonError::into_inner),
        );
        let swept = tokio::task::spawn_blocking(move || -> Result<_, String> {
            let runtime = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .map_err(|error| error.to_string())?;
            let mut catalog =
                SqliteArtifactCatalog::open(&catalog_path).map_err(|error| error.to_string())?;
            let retired = catalog
                .artifacts_in_state(ArtifactState::Retired)
                .map_err(|error| error.to_string())?;
            let mut deleted = 0_u64;
            let mut kept = BTreeSet::new();
            for artifact in retired {
                if !seen.contains(&artifact) {
                    kept.insert(artifact);
                    continue;
                }
                match runtime.block_on(catalog.delete_retired_artifact(
                    &artifact,
                    store.as_ref(),
                    now_ms(),
                )) {
                    Ok(Some(_)) => deleted += 1,
                    Ok(None) => {
                        kept.insert(artifact);
                    }
                    Err(error) => {
                        tracing::warn!(target: "calque::tenants", %error, "a retired tenant artifact was not deleted; the next sweep tries again");
                        kept.insert(artifact);
                    }
                }
            }
            Ok((deleted, kept))
        })
        .await
        .map_err(|error| error.to_string())
        .flatten();
        match swept {
            Ok((deleted, kept)) => {
                *self
                    .inner
                    .retired_seen
                    .lock()
                    .unwrap_or_else(PoisonError::into_inner) = kept;
                if deleted > 0 {
                    tracing::info!(target: "calque::tenants", deleted, "retired tenant artifacts deleted");
                }
            }
            Err(error) => {
                tracing::warn!(target: "calque::tenants", %error, "retired tenant artifacts were not swept");
            }
        }
        self.write(DirectoryWrite::Prune {
            before_ms: now_ms().saturating_sub(OPEN_RETENTION_MS),
            orders_before: now_seconds().saturating_sub(ORDER_ID_MARGIN_SECONDS),
        });
    }

    /// The warm tenant whose log since its cut is longest against its
    /// artifact, for the log-tail gauges.
    pub(crate) fn longest_tail(&self) -> Option<TenantTail> {
        let state = self.state();
        let generation = state.generation();
        let shared = state.shared_bytes;
        let policy = self.inner.settings.refresh;
        state
            .entries
            .values()
            .filter_map(|entry| {
                let cut = entry.current(generation)?;
                Some((cut, entry.tail(shared)))
            })
            .max_by(|(left, left_tail), (right, right_tail)| {
                let ratio = |tail: u64, cut: &Cut| tail as f64 / cut.decoded_bytes.max(1) as f64;
                ratio(*left_tail, left).total_cmp(&ratio(*right_tail, right))
            })
            .map(|(cut, tail_bytes)| TenantTail {
                cut_sequence: cut.sequence,
                tail_bytes,
                threshold_bytes: policy.threshold_bytes(cut.decoded_bytes),
            })
    }
}

fn report_of(value: &PartitionValue, entry: &Entry, generation: u64, now: i64) -> TenantReport {
    let current = entry.current(generation);
    let state = if current.is_some() {
        "warm"
    } else if entry.job.is_some() {
        "preparing"
    } else {
        "cold"
    };
    TenantReport {
        tenant: value.to_string(),
        state,
        pinned: entry.pinned,
        refused: entry.refused,
        cut_age_seconds: current
            .map(|cut| u64::try_from(now.saturating_sub(cut.built_ms) / 1000).unwrap_or(0)),
        size_bytes: current.map(|cut| cut.decoded_bytes),
        last_opened_unix_seconds: entry
            .last_opened_ms
            .and_then(|opened| u64::try_from(opened / 1000).ok()),
    }
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct TenantOrderRequest {
    order: String,
}

/// The application backend's tenant route, or nothing when this deployment
/// serves no partitioned scope.
pub(crate) fn router(backend: LocalBackend) -> Router {
    if !backend.tenant_orders_enabled() {
        return Router::new();
    }
    Router::new()
        .route("/v1/tenants", post(tenant_order))
        .layer(DefaultBodyLimit::max(MAX_TENANT_ORDER_BODY_BYTES))
        .with_state(backend)
}

async fn tenant_order(
    State(backend): State<LocalBackend>,
    headers: HeaderMap,
    body: Bytes,
) -> Result<Response, TenantOrderRefusal> {
    let request: TenantOrderRequest = crate::revocation::parse_json(&headers, &body)
        .map_err(|_| TenantOrderRefusal::InvalidRequest)?;
    let report = backend.tenant_order(&request.order).await?;
    Ok(crate::revocation::no_store(StatusCode::OK, Json(report)))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn cut_at(sequence: u64) -> Entry {
        let mut entry = Entry::new(0);
        entry.cut = Some(Cut {
            generation: 1,
            cursor: OpaqueCursor::parse("cut").unwrap(),
            sequence,
            decoded_bytes: 0,
            built_ms: 0,
        });
        entry
    }

    fn building_from(held_from: u64) -> Entry {
        let mut entry = Entry::new(0);
        entry.job = Some(Job::Running {
            build: 0,
            cancellation: CancellationToken::new(),
            held_from,
        });
        entry
    }

    fn state() -> TenantState {
        let epoch = sync_types::SourceEpoch::new("tenants-epoch").unwrap();
        TenantState {
            base: Arc::new(crate::snapshot::tests::prepared_source(epoch)),
            codec: PostgresTypeCodec::Int8,
            entries: HashMap::new(),
            queue: BuildQueue::default(),
            compress_queue: VecDeque::new(),
            running_builds: 0,
            compressing: false,
            next_order: 0,
            next_build: 0,
            shared_bytes: 0,
            shared_touched_at: 0,
            followed: 0,
            orders: HashMap::new(),
        }
    }

    /// What `schedule` does to start the next queued build.
    fn start(state: &mut TenantState, build: u64) {
        let (_, value) = state
            .queue
            .pop(Instant::now(), Duration::from_secs(60))
            .unwrap();
        state.entry(&value).job = Some(Job::Running {
            build,
            cancellation: CancellationToken::new(),
            held_from: 0,
        });
        state.running_builds += 1;
    }

    #[test]
    fn a_build_the_tenant_no_longer_runs_ends_without_touching_the_tenant() {
        let tenant = PartitionValue::Int(1);
        let settings = TenantSettings::default();
        let mut state = state();
        let generation = state.generation();
        state.request_build(&tenant, Priority::Warm);
        start(&mut state, 0);
        // Evicted, then asked for again before the cancelled build ends.
        state.drop_build(&tenant);
        state.request_build(&tenant, Priority::Waiting);
        start(&mut state, 1);

        let (retire, tail) =
            state.finish_build(&settings, &tenant, 0, generation, BuildEnd::Failed);
        assert!(retire.is_empty() && tail.is_none());
        assert_eq!(state.running_builds, 1);
        assert!(matches!(
            state.entries[&tenant].job,
            Some(Job::Running { build: 1, .. })
        ));

        state.finish_build(&settings, &tenant, 1, generation, BuildEnd::Failed);
        assert!(state.entries[&tenant].job.is_none());
        assert_eq!(state.running_builds, 0);
    }

    #[test]
    fn a_tenant_is_known_unchanged_only_up_to_where_the_follower_has_read() {
        let tenant = PartitionValue::Int(1);
        let mut state = state();
        assert_eq!(state.unchanged(&tenant), None);
        let mut entry = cut_at(40);
        entry.touched_at = 30;
        state.entries.insert(tenant.clone(), entry);
        state.shared_touched_at = 20;
        state.followed = 50;
        assert_eq!(
            state.unchanged(&tenant),
            Some(UnchangedScope {
                last_change: 30,
                through: 50
            })
        );
        state.shared_touched_at = 45;
        assert_eq!(state.unchanged(&tenant).unwrap().last_change, 45);
    }

    #[test]
    fn the_log_is_kept_from_the_oldest_cut_build_or_follower_position() {
        assert_eq!(retained_after([], 70), 70);
        assert_eq!(retained_after(&[cut_at(40), cut_at(55)], 70), 40);
        assert_eq!(retained_after(&[cut_at(40), building_from(30)], 70), 30);
        assert_eq!(retained_after(&[cut_at(80)], 70), 70);
        let mut queued = Entry::new(0);
        queued.job = Some(Job::Queued {
            priority: Priority::Waiting,
            order: 0,
            queued: Instant::now(),
        });
        assert_eq!(retained_after(&[queued, Entry::new(0)], 70), 70);
    }

    #[test]
    fn a_build_queued_past_the_aging_interval_takes_every_other_start_from_waiting_requests() {
        let aging = Duration::from_secs(60);
        let start = Instant::now();
        let tenant = PartitionValue::Int;
        let mut queue = BuildQueue::default();
        queue.insert(Priority::Refresh, 0, tenant(1), start);
        queue.insert(Priority::Warm, 1, tenant(2), start);
        queue.insert(Priority::Waiting, 2, tenant(3), start);
        queue.insert(Priority::Waiting, 3, tenant(4), start);
        queue.insert(Priority::Waiting, 4, tenant(5), start);
        assert_eq!(queue.len(), 5);

        // Within the interval a waiting request goes first.
        assert_eq!(
            queue.pop(start, aging),
            Some((Priority::Waiting, tenant(3)))
        );
        // Past it the refresh and the warm order, queued before the requests,
        // alternate with them, oldest first.
        let later = start + aging;
        assert_eq!(
            queue.pop(later, aging),
            Some((Priority::Refresh, tenant(1)))
        );
        assert_eq!(
            queue.pop(later, aging),
            Some((Priority::Waiting, tenant(4)))
        );
        assert_eq!(queue.pop(later, aging), Some((Priority::Warm, tenant(2))));
        assert_eq!(
            queue.pop(later, aging),
            Some((Priority::Waiting, tenant(5)))
        );
        assert_eq!(queue.pop(later, aging), None);

        // A fleet of aged warm orders holds a request back one start at most.
        for order in 10..100 {
            queue.insert(Priority::Warm, order, tenant(order as i64), start);
        }
        assert_eq!(queue.pop(later, aging), Some((Priority::Warm, tenant(10))));
        queue.insert(Priority::Waiting, 100, tenant(100), later);
        assert_eq!(queue.pop(later, aging), Some((Priority::Warm, tenant(11))));
        assert_eq!(
            queue.pop(later, aging),
            Some((Priority::Waiting, tenant(100)))
        );
        queue.clear();

        // With no request waiting, the lower lanes go in priority order.
        queue.insert(Priority::Refresh, 4, tenant(5), later);
        queue.insert(Priority::Warm, 5, tenant(6), later);
        assert_eq!(queue.pop(later, aging), Some((Priority::Warm, tenant(6))));
        assert_eq!(
            queue.pop(later, aging),
            Some((Priority::Refresh, tenant(5)))
        );
        assert!(queue.is_empty());
    }
}
