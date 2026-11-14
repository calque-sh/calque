# Calque architecture

This document defines the state model and target architecture for the pre-alpha.

## Supported profile

- PostgreSQL 18 is the only required source version.
- Chrome or Chromium on a 16 GiB reference client is the supported browser profile.
- The pre-alpha feasibility path is a Calque-owned direct columnar local-query
  engine. Generated, typed Calque facades are the only public collection,
  query, subscription, and mutation surface.
- React is the official UI integration; the synchronization core remains framework-neutral.
- One active Calque server owns each PostgreSQL source. Automatic HA is not required.
- One client visibility cohort and every public query, dependency graph, derived
  query, join, group, or aggregate may contain collections from exactly one
  PostgreSQL source. Separate source roots may coexist only when no public query
  can combine them; cross-source querying is outside the pre-alpha.
- Deployment is a self-hosted Docker composition. Extra control-database and S3-compatible object-storage containers are acceptable.
- Application configuration is authored in `calque.config.json` and compiled into a deterministic static manifest.
- Pre-alpha APIs have no compatibility guarantee.

## Not built in the pre-alpha

The rest of this document is the target. These parts of it are not built:

- Activation by mounted query: the client starts loading when it is
  constructed (`ColumnarSession`'s constructor in
  `client/packages/client/src/columnar/session.ts`), and one artifact carries
  every collection (`bindColumnarPlan` in
  `client/packages/client/src/columnar/plan-binding.ts`).
- Parameterized collections, and scopes narrower than the source other than
  one tenant: a scope is source-wide or partitioned by one grant attribute,
  and a partitioned grant names exactly one tenant (`scope` in
  `client/packages/config/src/compiler.ts`, `partition_scope` in
  `server/crates/authz/src/lib.rs`).
- Selected columns per grant: a collection carries every column its
  publication publishes, for every grant. Leaving a column out of the
  collection keeps it out of the publication's column list for all of them
  ([Publishing only some columns](../postgresql.md#publishing-only-some-columns));
  one grant cannot see fewer columns than another.
- Collection eviction: a plan over the memory budget fails with
  `CAPACITY_EXCEEDED`, and nothing is evicted (`#mount` in `session.ts`).
- The active/standby swap: a rebuild closes the old engine before the new one
  opens, so INV-VIS-008 is not met (`#restart` in `session.ts`).
- Backend commands and the `CommandExecutor` contract: the compiler refuses a
  backend command (`command` in `compiler.ts`).
- HTTP Range, artifact caching and a long-lived change stream: the artifact
  route answers `200` with `private, no-store` (`serve_artifact` in
  `server/crates/calque-engine/src/artifacts.rs`), and the change stream holds
  each change response open only for the plan's long-poll wait, which `calque`
  serves as 1 s (`LOCAL_LONG_POLL_TIMEOUT_MS` in
  `server/crates/calque-engine/src/limits.rs`), then asks again
  (`client/packages/worker/src/columnar-live.ts`).
- Live delivery to many clients within its target: one process reads each
  commit once and encodes it once per scope for up to 4,096 clients, but
  resolves, frames and writes each client's batch on its own. In directional
  runs a page's commit-to-paint median did not move with
  100 other clients at 220 transactions a second, and rose from 15.6 to
  40.3 ms with 3,000 at 10, beyond the run's error bar; the same machine's
  loopback delays a message about as much among 3,000 connections with no
  calque in it
  ([operations](operations.md#how-many-clients-one-process-serves)).
- The `calque.*` identifier rule: the wire identifiers are `sync.change.v1`
  (`wireProtocol` in `spec/bootstrap-plan.schema.json`) and `sync.mutation.v1`
  (`LOGICAL_MARKER_PREFIX` in `server/crates/mutation-api/src/lib.rs`).
- Upgrade, rollback and rolling deployment: a build reads only state in the
  format it writes ([operations, Upgrades](operations.md#upgrades)), no grace
  rule keeps old application or schema versions on CDC, and there is no
  two-version overlap of `calque`, because two versions would be two writers
  ([operations](operations.md#what-the-self-hosted-composition-does-not-do)).
- A bounded cold-load replay that passed its experiment (INV-CDC-004's
  rebuild for an unaffordable catch-up). The re-cut is on by default at
  `--recut-tail-ratio` 1/32, so a cold load replays the writes of up to one
  interval and one build, but no experiment admitted it: live updates were
  slower while re-cuts ran. `--recut-tail-ratio 0` turns it off
  ([operations](operations.md#re-cutting-the-artifact-and-trimming-the-change-log)).

## Product and data model

Applications declare named, optionally parameterized collections. Each collection describes one PostgreSQL table or application-maintained sync-ready projection table, selected columns, schema version, authorization scope, store adapter and allowed mutation commands.

A mounted Calque query automatically activates its declared collection
dependencies. Its data-only query specification covers predicates, projections,
sorting, limits, joins, grouping, and aggregates, which execute locally in the
configured Calque engine and do not define remote coverage. Calque downloads
every row and column in each authorized collection instance.

Arbitrary server-side live SQL collections, runtime-created remote scopes, remote pagination, partial-result modes, and client-defined authorization are outside the pre-alpha. Applications materialize denormalized projection tables when one-table collections are insufficient.

## Stable invariants

### Visibility and ordering

- **INV-VIS-001 — Complete collections:** application queries never observe a partially downloaded, partially decoded, partially installed, or partially evicted collection.
- **INV-VIS-002 — One canonical cursor:** all collections visible to one Calque
  client cohort come from one PostgreSQL source and represent one complete transaction
  or barrier cursor from that source. Configuration and query activation reject a
  cohort or public query/dependency graph that mixes sources.
- **INV-VIS-003 — Cross-collection atomicity:** a transaction affecting multiple collections becomes visible to public Calque queries as one publication. No query may observe a mixed pre/post transaction state.
- **INV-VIS-004 — Atomic activation:** a prepared collection set remains private until every required collection, index, cursor, schema and authorization check succeeds. Publication switches the whole set atomically from the application's perspective.
- **INV-VIS-005 — Honest cursors:** cursors are opaque, epoch-scoped identifiers for complete canonical transactions or barriers; naked LSNs and client-side cursor ordering are forbidden.
- **INV-VIS-006 — Whole eviction:** active queries pin their dependencies. The memory-budgeted LRU may evict only complete inactive collection instances.
- **INV-VIS-007 — Sealed engine capabilities:** application code never receives
  an internal column buffer, index, query plan, mutable row, sync handle,
  iterator, debug capability, or callback argument. Every public facade consults
  the current Calque authorization and visibility epoch.
- **INV-VIS-008 — Private generation publication:** the active internal generation is
  the only queryable generation. A complete standby generation is prepared privately and
  becomes active through one pointer publication after identities, readiness,
  indexes, query results, optimistic watermark, and capacity pass. Application
  callbacks run only after publication and cannot change its result.

### Canonical truth and delivery

- **INV-CDC-001 — Durable acknowledgement:** PostgreSQL WAL is acknowledged only after the complete normalized transaction and its index are durably appended by the current fenced owner.
- **INV-CDC-002 — At-least-once application:** delivery may repeat; log, artifact, adapter, and mutation application are idempotent.
- **INV-CDC-003 — Ordered convergence:** backpressure, batching, and UI frame coalescing preserve canonical transaction order and eventually converge to PostgreSQL.
- **INV-CDC-004 — Explicit rebuilds:** retention gaps, epoch changes, schema incompatibility, authorization changes, integrity failures, or unaffordable catch-up cross explicit rebuild boundaries. They never silently skip history.
- **INV-CDC-005 — One writer:** the pre-alpha runtime has one fenced active owner per source. Restart recovery must not lose or duplicate visible transactions.

### Artifacts and capacity

- **INV-ART-001 — Store-specific physical form:** each configured store may have its own optimized snapshot format and direct materialization path. There is no required store-neutral payload or full intermediate JavaScript row array.
- **INV-ART-002 — Build only configured formats:** a deployment prepares only its declared adapter formats. A supported adapter must prove its optimized path; a slow generic fallback does not qualify it.
- **INV-ART-003 — Immutable publication:** artifacts and representations are immutable and content-addressed. Catalog publication occurs only after format, scope, schema, cursor, length, digest, and structural validation.
- **INV-ART-004 — Prepared scope:** the client-cold/server-warm target applies only to a registered authorization scope whose configured artifact is already published.
- **INV-ART-005 — Capacity before activation:** the client accounts for artifact, active and standby canonical state, optimistic overlay, indexes, query structures and safety headroom. Insufficient memory returns a structured capacity error, never partial data or a best-effort crash.

Artifact identity includes source epoch, manifest version, collection set, schema versions, authorization-scope version, canonical cursor, adapter and encoding version. Schema or permission widening may continue serving a compatible old version while a replacement builds. After the durable authorization-generation fence, permission narrowing or revocation forbids new delivery authorization for the old scope. After an honest client receives the ordered authenticated revocation barrier, it locks affected queries before clearing and rebuilding.

### Authorization

- **INV-AUTH-001 — Application authority:** the application backend authenticates principals, evaluates business permissions and issues short-lived signed, versioned dataset grants. Calque verifies grants; browsers cannot widen them.
- **INV-AUTH-002 — Read and write separation:** a read grant never implies mutation permission. Commands have explicit authorization and validation.
- **INV-AUTH-003 — Sync-ready scopes:** simple immutable tenant/organization keys have a native path. Complex authorization requires an application-owned PostgreSQL projection containing the scope key and projected business row. Calque does not run arbitrary application code or generic live joins in CDC. A partitioned scope is that native path: the tenant column leads every partitioned table's primary key, a grant names one tenant, and a row that changes tenant is delivered to neither tenant as a patch: both are rebuilt from a fresh cut.
- **INV-AUTH-004 — Revocation:** the server authorizes no new forbidden work after
  the durable authorization-generation fence. A connected honest client suspends
  affected queries upon receipt of the ordered authenticated revocation barrier,
  then cancels or reauthorizes pending mutations, discards resident state, and
  rebuilds from a safe grant. Disconnected stale usability is bounded by grant
  expiry. Revocation cannot retract bytes already observed by a hostile client.
- **INV-AUTH-005 — One authorization source:** business commands and synchronized reads derive permission from the application's same source of truth.
- **INV-AUTH-006 — Authenticated internal publication:** only a private,
  single-use capability minted after authenticated envelope verification may
  prepare standby state. It binds owned bytes, source and authorization epochs,
  scope, schema, format, predecessor and target cursor, collection membership,
  canonical transaction, optimistic watermark, and capacity reservation.

Visibility-projection changes are canonical CDC input. Gaining visibility includes enough projected row data to materialize the grant without a generic CDC-time lookup/join engine.

### Mutations

- **INV-MUT-001 — Optimistic overlay:** adapters maintain canonical CDC-confirmed state plus ordered pending local mutations. Optimistic state is an overlay, not rewritten canonical truth.
- **INV-MUT-002 — Echo confirms:** HTTP acceptance remains pending. Only the matching canonical PostgreSQL CDC echo confirms a mutation.
- **INV-MUT-003 — Automatic repair:** rejection removes the failed mutation and deterministically replays later authorized pending mutations over current canonical state, then emits a structured application error.
- **INV-MUT-004 — Idempotency:** every mutation has a stable identifier; lost responses and retries cannot apply a command twice.
- **INV-MUT-005 — Server order:** pre-alpha conflict resolution is last-write-wins by PostgreSQL commit order, never device clock. Custom conflict resolution is a future extension.

Generic row CRUD may execute through Calque only when an explicit signed command grant permits it. Business commands execute in the application backend through a pluggable `CommandExecutor` contract. The SDK carries principal context, mutation ID, transaction boundaries and idempotency. A worker executor may implement the same contract later.

Transient disconnects leave writes enabled and queued in memory. A durable offline outbox and long-lived divergent operation are not pre-alpha requirements.

## Protocol and runtime boundaries

Bulk snapshots use resumable cacheable HTTP backed by S3-compatible immutable storage. Ordered live changes use a long-lived stream. Commands use ordinary HTTP. All three share canonical identities, transactions, scopes, schemas and integrity rules; one WebSocket is not required to carry everything.

The server normalizes PostgreSQL logical replication into bounded, transaction-framed canonical changes. It must preserve null versus absent values and unchanged TOAST semantics, bound large transactions, and reject unknown required semantics. Browser clients never decode raw WAL.

The application query path is generated Calque bindings backed by the
Calque-owned direct columnar engine. Calque owns the data-only query specification, dependency
activation, materialization, visibility epochs, canonical cursor, optimistic
reconciliation, capacity accounting, and collection lifetime. There is no
public Calque SQL API or raw engine capability in the pre-alpha.

Returned supported values are deeply immutable. A row already returned to
application code cannot be erased after revocation; no later Calque API call
or notification may disclose it. On an authenticated revocation barrier,
facades lock first, queued callbacks are dropped, affected optimistic work is
resolved, both internal generations are destroyed without old-row delete payloads,
and unlock waits for a complete authorized rebuild. Failure before ordinary
publication destroys standby and leaves active unchanged. Failure during
revocation leaves facades locked. After a pointer swap there is no rollback;
cleanup and observer failures are reported separately, and reentrant work joins
a later epoch. Discarded graph microtasks cannot mutate active state or cursor.

Multiple tabs must remain correct but may independently materialize columnar state. The artifact route sends `Cache-Control: private, no-store` today, so HTTP caching does not avoid a repeated artifact transfer. The 100,000-row performance and memory target applies to one active tab; shared cross-tab materialization is future research.

Runtime choice is local to a component. Rust is a candidate for PostgreSQL CDC/COPY, encoding, compression and integrity hot paths; TypeScript/JavaScript is a candidate for configuration, SDK, routing and browser integration. No language or infrastructure product is mandatory without measured value. The core cannot require Cloudflare or another single provider.

## Configuration and rollout

`calque.config.json` is a typed authoring surface compiled before deployment. The static manifest drives source selection, collections, columns, projection scopes, command capabilities, artifact formats, generated Calque facades and compatibility checks. Runtime arbitrary TypeScript is not configuration.

The server prepares and verifies a new registered dataset version before advertising readiness. Compatible old application/schema versions may keep receiving valid CDC during rolling deployment and retire after a configured grace rule. A breaking database change that prevents valid old-version CDC suspends affected old clients and forces rebuild; a stale static artifact may never be presented as realtime.

The intended first-run path is `calque init`, schema inspection and generated typed definitions, manifest editing, `calque dev`, Docker startup, generated Calque imports, automatic query activation, and `/metrics`. The incident-operations page must use only this public path.

## Performance contract

These are product targets, not measurements. Architecture work must preserve them:

- Realistic incident-operations dataset: about 100,000 mixed rows.
- Client cold/server warm target: strictly below 2,000 ms p95 to complete
  stable grid paint on the 16 GiB reference machine, headed Chrome,
  and 1 Gbps low-latency profile.
- A separately published typical-office result at 200 Mbps and about 30 ms latency.
- First preparation: under 10 seconds from PostgreSQL snapshot start through verified artifact publication on 8 modern CPU cores, 16 GiB RAM, NVMe, and low-latency PostgreSQL/object storage.
- Live update: PostgreSQL commit to stable browser paint below 250 ms p95 and below 1 second p99 under the declared load.
- Local interaction: no main-thread task over 50 ms; common filter/sort below 200 ms p95; ordinary grouping/aggregation below 500 ms p95.
- Source throughput: 10,000 row changes/second sustained and bursts of 50,000 without loss.
- Active client: converge under 1,000 relevant changes/second using incremental maintenance and frame-coalesced publication; this does not mean 1,000 physical paints per second.

The benchmark, `pnpm bench`, judges a narrower profile: 100,007 rows of
the content-v1 dataset, below 2,000 ms p95 over 30 cold samples to complete
stable paint, in headless Chrome for Testing at Chrome-emulated 100 Mbit/s and
30 ms. A run on a loaded machine counts, labelled directional.
The 16 GiB reference machine, headed Chrome, the 1 Gbps profile and the
200 Mbps office result above are not judged. The README's [benchmark](../../README.md#the-benchmark)
section has its thresholds and its most recent full run.

The target for every reported run is to separate download, decode, materialization, indexing, query and stable paint, and to publish transfer bytes, CPU, RSS, JS heap, ArrayBuffer use, GC and long tasks. The benchmark records the browser's process-tree RSS, `calque`'s RSS, the benchmark's CPU time, long tasks and GC, and no JS heap or ArrayBuffer figure. Server-cold process recovery, server-warm client load, client-warm load, dataset preparation and live update are distinct scenarios.

## Failure behavior

- Corrupt, truncated, mismatched or oversized data fails before deserialization or activation.
- Artifact catalog publication is the visibility boundary; interrupted builds remain invisible and cleanable.
- A client that falls behind applies bounded ordered backpressure or performs an explicit snapshot rebuild.
- Server process restart recovers from durable state without data loss, duplicate visible transactions, or unnecessary resnapshot.
- Health checks, metrics, backup/restore and tested upgrade/rollback are pre-alpha requirements; automatic failover and horizontal scaling are not.
- No error path returns a plausible but incomplete collection.

## Verification law

No performance result counts unless the reference PostgreSQL-state oracle passes in the same run. Experiments freeze their dataset, environment, metric boundaries, candidates, failure matrix and decision rule before execution. Raw samples, versions, hashes, timeouts and exclusions are retained. Timeouts are failures; best-of-N is forbidden.

Protocol changes require cross-language golden vectors, malformed/truncated/oversized cases and compatibility decisions. CDC requires PostgreSQL 18 normal and large-transaction fixtures, append/ACK failpoints, duplicate/restart and reference convergence. Snapshot/artifact work requires exact-cut, corruption, interruption and provider tests. Client work requires real-Chrome atomicity, memory, cancellation, reload and long-task evidence. Mutation work requires accept/reject, lost response, idempotency, revocation, reload and randomized rollback/replay equivalence.

The review question is: can this produce a fast and locally plausible
public Calque result that is incomplete, unauthorized, not equal to the
complete authorized PostgreSQL state at the advertised cursor, or backed by a
raw engine capability that bypasses its epoch? If yes or uncertain, it is not
ready.

## Schema identifiers

Artifact envelope schema identifiers use the `calque.*` prefix.
