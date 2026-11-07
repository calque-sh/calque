<p align="center">
  <picture>
    <source media="(prefers-color-scheme: dark)" srcset="docs/assets/calque-logo-animated-dark.svg">
    <img src="docs/assets/calque-logo-animated.svg" alt="Calque's logo: an elephant holding a pencil in its trunk, with a grey copy of itself and rows of data streaming in behind it" width="300">
  </picture>
</p>

# Calque

Calque puts your PostgreSQL tables in the browser, complete and live. It
copies each table you enroll into the page, whole or narrowed to one tenant's
rows, keeps that copy current from PostgreSQL's logical replication, and
answers the page's queries locally over all of it: filters, joins, grouping,
aggregates and sorting run without a round trip to a server.

> [!WARNING]
> Calque is pre-alpha and not ready for production. Its APIs, protocol and
> storage formats can change in any release, and there is no upgrade path: a
> new build refuses the state an older one wrote until you clear it and
> prepare again ([Upgrades](docs/design/operations.md#upgrades)).

![The incident-operations page loads 100,000 rows, scrolls its events grid, filters and sorts it, resolves an issue, and shows a transaction committed in psql arrive without a reload](docs/assets/calque-incidents-100k.gif)

The incident-operations example at 100,000 rows, recorded over loopback with
no network emulation; [the benchmark](#the-benchmark) times the same page over
an emulated 100 Mbit/s link.

Pagination, query-scoped sync and caches each give a page part of the data,
so a question outside what was fetched needs another request. Calque sends
all of it, at one PostgreSQL transaction, and follows the database you
already run: a write from any backend, job or migration reaches every open
page. Instead of writing sorts, filters and counts a second time on the
server, you maintain the tables the browser sees, and Calque handles the
transfer, live updates, optimistic writes, recovery and access control.

The name is French for a tracing: a copy made by drawing over the original.

- A collection arrives complete, or not at all. There are no pages to fetch,
  no query runs over rows that are still loading, and a client without the
  memory for it gets a structured capacity error.
- Everything on the page comes from one PostgreSQL transaction boundary. When
  one commit changes several live queries, they update on the same frame.
- In the benchmark, 100,000 rows reached a complete, stable paint in 760 ms
  (p95 of 30 cold loads) over an emulated 100 Mbit/s link. Once they were
  loaded, filtering and sorting them took 35 ms (p95), and a committed change
  reached the screen in 41 ms (p95) while the database took 11,000 row
  changes a second. Every sample was checked against PostgreSQL. The run was
  on a busy laptop, so these numbers are directional
  ([the benchmark](#the-benchmark)).
- A write shows at once and stays optimistic until its change comes back from
  PostgreSQL, so the page never marks a write saved before the database has
  it. A write is an idempotent mutation command, so a retry never applies it
  twice.
- Your backend decides which tenant's rows each user loads and what they may
  write, through short-lived grants it signs, and can revoke or narrow an open
  page's access.
- Rows, keys, queries and writes are typed from your tables, and a query is
  built in TypeScript, never sent as SQL.

Calque is pre-alpha, and its APIs will change. Before you point it at your own
schema:

- Every column of an enrolled table reaches the browser. A table that holds
  secrets, such as password hashes or internal notes, needs a projection
  table that leaves them out.
- A page receives whole tables, or one tenant's rows of them by a tenant
  column that leads the primary key. Visibility per user or per team needs a
  projection table your application maintains.
- Writes are inserts, updates and deletes checked against the grant. No
  backend code runs on a write yet.
- The source must be PostgreSQL 18, and Calque's role must own the tables it
  serves. Enums, arrays, domains, extension types, generated columns and
  partitioned tables are refused
  ([What Calque needs from PostgreSQL](docs/postgresql.md)).
- The client runs only in Chromium-based browsers, tested in Chrome and
  Chromium, and refuses any other with `UNSUPPORTED_BROWSER`.
- Nothing is kept between page loads. Every load, a reload included,
  downloads the whole working set again, and each open tab downloads and
  holds its own copy.
- Performance has been measured at 100,000 rows. At 1,000,000 only
  correctness has been checked.

Today it fits internal tools and operations consoles whose users work in
Chrome, in one tenant's whole dataset, all day.
[`MANIFESTO.md`](MANIFESTO.md) has the reasoning.

## Install

```sh
npm install @calque/client
npm install -D @calque/config
```

`@calque/client` is the browser client. `@calque/config` renders the typed
schema the client is created with, for the client's Vite plugin and as the
`calque-bindings` command. React applications add `@calque/react`.

The server is `calque`, one Rust binary, published as a container image:

```sh
docker pull ghcr.io/calque-sh/calque
```

Its tags are the release version and `latest`. The commands in the next
section run `calque` on your machine;
`cargo install --locked --path server/apps/calque` installs it from a
checkout, with Rust and libpq as
[Building calque from source](docs/development.md#building-calque-from-source)
lists. [Self-hosting](docs/self-host.md) runs it behind HTTPS with grants your
backend signs.

## Querying the working set

`calque inspect` reads the tables of a PostgreSQL publication, and
`calque init` turns them into `calque.config.json`, one collection per table
([the `calque` command](docs/cli.md)):

```sh
calque inspect --source primary --publication calque_app --table public.tasks > calque.inspection.json
calque init --from-inspection calque.inspection.json --application-id my-app
```

The Vite plugin, `calque()` from `@calque/client/vite`, renders that file into
`calque.generated.ts`, the typed schema, and keeps it current: when `vite`
starts, whenever the file changes, and before `vite build`. Outside Vite,
`npx calque-bindings` renders it, and in CI `npx calque-bindings --check`
fails when it is out of date.

```ts
import { createSyncClient, eq } from "@calque/client";
import { schema } from "./calque.generated.js";

const client = createSyncClient({
  endpoint: "https://sync.example.com",
  auth: async () => ({
    bearerToken: await getAccessToken(),
  }),
  // The public half of the key your backend signs grants with. With it, a
  // revoked user's rows leave the page as soon as the revocation arrives.
  grantVerificationKey,
  schema,
});

// `db.tasks` is typed from the `tasks` table: its rows, its key, and the
// writes its configured commands allow.
const open = await client.db.tasks
  .query()
  .where(eq("done", false))
  .orderBy("id", "desc")
  .limit(100)
  .run();

const counts = await client.db.tasks
  .query()
  .groupBy("done")
  .count()
  .orderBy("done")
  .run();
```

A query can also stay subscribed, and a collection carries the writes its
configured commands allow:

```ts
// A live query. Its listener runs again whenever the rows change.
const tasks = client.db.tasks.query().orderBy("id").subscribe();
tasks.subscribe(() => render(tasks.getSnapshot()));

// A write. The promise resolves with the server's answer, and the page shows
// the change optimistically until its echo arrives from PostgreSQL.
await client.db.tasks.update({ id: 1n }, { done: true });
```

`createSyncClient` starts loading as soon as it is called.
[`@calque/client`'s README](client/packages/client/README.md) has the rules a
query follows: NULL in filters, integer types and result limits. It also shows
how to start loading before the client library has loaded, or from the backend
that renders the page, and how a page has to be bundled for the client's
workers to load.

For React, `@calque/react`'s `SyncProvider` puts a client in context,
`useLiveQuery` subscribes a component to a live query, `useSuspenseLiveQuery`
does so under Suspense, `useSyncStatus` follows the client's status,
`usePendingWrite` the latest write to one row, and `useMutationStatus` one
write by its mutation id.
[Its README](client/packages/react/README.md) shows a typed component.

## Try it locally

You need:

- Docker, with Compose 2.17 or newer (`docker compose version`);
- Node 26.12.1 or newer, 24.22.1 or newer on the 24 line, or 22.23.4 or newer
  on the 22 line: the releases with Node's October 2026 security fixes;
- pnpm 11 or newer, which switches to the version the root `package.json`
  pins (`npx get-pnpm` installs the latest; open a new terminal after it);
- Google Chrome or another Chromium-based browser.

Then, from the repository root:

```sh
pnpm install
pnpm demo
```

`pnpm demo` pulls the `calque` image for this checkout's version, starts a
disposable PostgreSQL 18 in Docker on port 55432, seeds a `projects` and a
`tasks` table, runs `calque dev` from the image against it, and prints the
dashboard's URL. On macOS with Google Chrome in `/Applications` it opens that
URL in Chrome; elsewhere, open the printed URL yourself.
`pnpm demo -- --no-open` never opens it.

In another terminal, `pnpm demo:change` commits one transaction that changes
a task and its project, and the open page follows without a reload. `Ctrl-C`
stops the server and keeps the database for the next run. To start over,
`docker compose down -v && rm -rf .calque-dev` removes the database, its volume
and the state `calque dev` kept for it.

`pnpm demo -- --from-source` builds `calque` from the checkout instead of
pulling it. That needs Rust and a C compiler, and the first build leaves
several gigabytes under `target/`:
[Developing Calque](docs/development.md#building-calque-from-source) has the
steps.

## What it does not do

- No remote pagination, predicate replication or partial results: a query
  predicate never changes what is synchronized.
- No server-side incremental SQL. A collection maps to one PostgreSQL table or
  to a projection table the application maintains, and Calque runs no
  application code or live joins inside CDC.
- No offline-first operation. PostgreSQL stays authoritative and a page needs
  the server to load; durable offline work and long-lived divergent replicas
  are out of scope.
- No raw engine access and no public SQL API. The generated facades are the
  only query, subscription and mutation surface.

## How it works

The server reads each source through one PostgreSQL logical replication slot
and writes every complete transaction to a durable log of its own before it
acknowledges the WAL. It cuts each table into
an immutable, content-addressed columnar artifact at an exact point in that
log, from an exported snapshot, and checks every artifact against digests of
both its encoded and its decoded bytes.

A page reaches it over four HTTP routes:

- `POST /v1/bootstrap` returns the plan the page's grant allows: which
  artifacts to fetch and the cursor they activate at;
- `/v1/artifact-objects/` serves those artifacts, each URL carrying a
  capability from the plan;
- `/v1/changes` streams the catch-up from each artifact's cut to the plan's
  activation cursor on one open response, then holds each live request open
  for up to a second, sending a batch each time the log moves;
- `POST /v1/mutations` takes writes.

In the page, Calque's own columnar engine runs in a worker. Hasher workers
digest the decoded artifact in 4 MiB chunks as its bytes arrive, and no row is
released before every chunk has matched the plan's SHA-256 digest, so a
tampered artifact is refused before anything is visible. The collections
activate together at one cursor, and queries run in 8 ms slices beside the
stream that applies live changes.

Revocation stops further access at the server. A client created with
`grantVerificationKey` also empties every live query as soon as the
revocation arrives, ends its pending writes as ambiguous, and rebuilds from
the next grant `auth()` returns. A client created without it keeps showing
the old rows until its next change request is refused, then rebuilds. Bytes
a hostile client already copied stay copied.

Machine-readable contracts live in [`spec`](spec), including
[protocol notes](spec/protocol-notes.md) (framing, cursor rules, the frozen
`body_sha256` preimage) and [schema notes](spec/schema-notes.md).

## Status

What works today, each part covered by an end-to-end test:

- A change committed in PostgreSQL reaches the page, and a write from the page
  is confirmed once it comes back from PostgreSQL.
- Your backend signs the grants the server checks. No request can widen a
  grant, a read grant never allows a write, and an application can revoke or
  narrow a running page's access without a redeploy.
- A self-hosted setup runs Calque behind Caddy over HTTPS, with backup and
  restore to S3-compatible storage ([Self-hosting](docs/self-host.md)).
- Calque reaches a PostgreSQL on another host over TLS that verifies the
  server's certificate and name
  ([What Calque needs from PostgreSQL](docs/postgresql.md)).
- One process can serve many tenants. A grant names one tenant, and its page
  receives that tenant's rows and the shared tables, never another tenant's.
  A tenant is prepared on its first request and kept warm while it is used,
  and your backend can warm, pin or evict one
  ([Tenants](docs/design/operations.md#tenants)).
- Correctness, authorization and recovery hold under the failures the test
  suites inject: a server killed outright, a PostgreSQL restart, a restore
  after the server's state and bucket are destroyed, and a tampered artifact.
- The incident-operations page passes its correctness checks over a
  1,000,007-row fixture.

### The benchmark

`pnpm bench` loads the incident-operations page at 100k rows in a fresh Chrome
with an empty cache, over an emulated 100 Mbit/s link with 30 ms of latency,
and times it to a complete, stable paint. It then times changes from their
PostgreSQL commit to the paint that shows them, under a background write load.
Every sample is compared with PostgreSQL row by row, and a run with one that
differs fails. [`docs/benchmark.md`](docs/benchmark.md) has the method and how
to run it.

A full run passed every threshold. It ran on an Apple M5 Pro laptop (18
cores, 48 GiB, macOS 26.5.1) while a container and other work kept its
one-minute load average above 4, so every number is directional.

| Measure                                         | Threshold   | Result                 |
| ----------------------------------------------- | ----------- | ---------------------- |
| Server preparation, slowest of 3                | < 10,000 ms | 1,466 ms               |
| Cold load to a complete stable paint, p95 of 30 | < 2,000 ms  | 760 ms (median 747 ms) |
| PostgreSQL commit to paint, p95 of 30           | < 250 ms    | 41 ms                  |
| PostgreSQL commit to paint, p99 of 30           | < 1,000 ms  | 45 ms                  |
| Background write load, source row changes/s     | ≥ 10,000    | 11,000                 |
| Peak browser process-tree RSS                   | ≤ 8 GiB     | 1.3 GiB                |
| Longest main-thread task                        | ≤ 50 ms     | none over 50 ms        |
| Filter and sort after the load, p95 of 30       | none        | 35 ms (median 26 ms)   |
| Bytes downloaded per cold load, median of 30    | none        | 7.7 MB                 |
| Peak browser process-tree RSS, an empty page    | none        | 0.89 GiB               |

### Not built yet

These are part of the design:

- A page loads only the collections its mounted views need. Today the client
  loads every collection the schema names as soon as it is created.
- A collection no query uses is evicted under memory pressure, and a rebuild
  prepares the new state beside the old one and switches to it in one step.
  Today nothing is evicted, and a rebuild closes the old state before the new
  one opens.
- A collection carries only the columns it selects. Today it carries every
  column of its table.
- A write can run through a command in your backend. Today writes are
  inserts, updates and deletes checked against the grant, and a failed write
  is resent only when the application calls `retryMutation`.

[Not built in the pre-alpha](docs/design/architecture-guidelines.md#not-built-in-the-pre-alpha)
lists every gap, with the code that falls short.

### Other limits

- One server process keeps up to 4,096 pages live by default
  (`--max-open-streams`), and a page's commit to paint grows with the pages
  connected: with 3,000 at 10 transactions a second its median was 40.3 ms
  against 15.6 ms with none, directional
  ([operations](docs/design/operations.md#how-many-clients-one-process-serves)).
- A cold load replays every transaction committed since the newest artifact
  was cut. By default `calque` cuts the artifact again in the background once
  those transactions reach 1/32 of its decoded size, at most once a minute
  (`--recut-tail-ratio`). Without the re-cut, a cold load after 10,000
  transactions took a directional median of about 1.75 s; with it, about
  0.75 s, as with nothing to replay
  ([benchmark](docs/benchmark.md#replaying-a-long-tail)). While re-cuts ran, a
  change's median commit to paint rose by about 4.5 ms in each of two
  experiments
  ([operations](docs/design/operations.md#re-cutting-the-artifact-and-trimming-the-change-log)).
  `--recut-tail-ratio 0` turns it off.
- Live queries are re-read together, so when one change moves several of
  them, a quick one is painted only once the slowest is ready.
- Under sustained writes, a long query that has restarted three times runs to
  completion in one step, and every change batch waits for it.
- Calque cannot present a client certificate to PostgreSQL. In the
  self-hosted setup, the hops behind Caddy are plaintext on the compose
  network.
- While tenants are prepared, the tenants already loaded get their changes
  later: with four preparing at once and every other tenant being warmed, a
  loaded tenant's commit to paint had a p95 of 157 ms against 46 ms with
  nothing preparing, directional
  ([operations](docs/design/operations.md#tenants)).

## Documentation

- [`llms.txt`](llms.txt): adding Calque to an application step by step, from
  the database to a live page and then production, written for a coding agent.
- [Self-hosting](docs/self-host.md): `calque serve` behind HTTPS with signed
  grants and S3-compatible storage, from a clean checkout to a live page.
- [Operations](docs/design/operations.md): every setting, revocation, backup
  and restore, capacity, and what the self-hosted composition does not
  provide.
- [The `calque` command](docs/cli.md): its subcommands, `inspect` and `init`,
  and the container image.
- [What Calque needs from PostgreSQL](docs/postgresql.md): settings, the role,
  the tables and the supported column types.
- [`@calque/client`](client/packages/client/README.md): the query rules, the
  grant verification key, starting the load early, and bundling.
- [`@calque/react`](client/packages/react/README.md): the hooks.
- [The benchmark](docs/benchmark.md): its method, and how to run it.
- [Architecture guidelines](docs/design/architecture-guidelines.md): the state
  model and the invariants behind what Calque commits to.
- [Developing Calque](docs/development.md): the build, the test gate, CI and
  the browser harnesses.

## Contributing

Calque takes issues, not pull requests, while its internals still change
daily.
[`CONTRIBUTING.md`](CONTRIBUTING.md) says what makes a report useful. Report a
vulnerability privately, as [`SECURITY.md`](SECURITY.md) describes.

## License

Calque is licensed under the Apache License 2.0 (`LICENSE`).
`vendor/pg-walstream` keeps its upstream BSD 3-Clause license,
`vendor/tokio-postgres` its MIT or Apache 2.0 license, and the Brotli decoder
compiled into the worker's `brotli-decoder.wasm` its MIT license; `NOTICE`
lists each.
