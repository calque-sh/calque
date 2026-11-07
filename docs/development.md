# Developing Calque

Node has to be 22.20 or newer on the 22 line, or 24.5 or newer: the workspace
uses Node's built-in zstd, and its tests run the grant issuer example, which
needs Node to strip `.mts` types and to bundle OpenSSL 3.5.

## Building calque from source

The `calque` binary is Rust. Building it needs:

- Rust through [rustup](https://rustup.rs), which installs the toolchain
  `rust-toolchain.toml` pins on the first build, and a C compiler for the
  native dependencies (Xcode's command line tools on macOS, `build-essential`
  on Debian or Ubuntu);
- libpq, which the build finds through `pg_config` on `PATH` or through
  `PQ_LIB_DIR`: Homebrew's `libpq` is keg-only, so add
  `$(brew --prefix libpq)/bin` to `PATH`, and on Debian or Ubuntu install
  `libpq-dev`.

The first build compiles the whole workspace and leaves several gigabytes
under `target/`. From a checkout, Cargo installs the binary:

```sh
cargo install --locked --path server/apps/calque
```

`pnpm demo -- --from-source` runs the checkout's build instead of the
published image.

`docker build -t calque:dev .` builds the same binary as the container image,
without Rust or libpq on the host.

## Building and testing

```sh
# Build before testing. Packages import each other through their built
# entrypoints (`dist/`), not their sources, so a fresh or stale checkout fails to resolve
# `@calque/protocol` and the example decodes against an out-of-date artifact
# fixture. Both surface as ordinary test failures rather than as a missing
# build, so run this first in every new worktree.
pnpm install && pnpm -r build

# Playwright's Chromium and Firefox are prerequisites of the gate's browser
# suites; Firefox runs the test that the client refuses an unsupported browser.
# Without them `pnpm verify` fails while launching Firefox.
pnpm exec playwright install chromium firefox

# Complete local workspace gate. It inventories every Cargo and pnpm package,
# rejects missing package scripts, and reports ignored/skipped tests separately.
pnpm verify

# Self-test the gate with six reversible failure injections.
pnpm verify:probes

# Tests only: Rust, then the `node:test` suites, then each package's runner.
pnpm test

# The `node:test` suites alone. Use this rather than a bare `node --test`,
# which sweeps in the TypeScript files that belong to vitest.
pnpm test:node
```

`rust-toolchain.toml`, `pnpm-lock.yaml`, and the root `packageManager` field pin
the toolchain inputs. `.github/workflows/ci.yml` runs `pnpm verify` itself,
the browser end-to-end scripts (`pnpm demo:e2e:latency`,
`pnpm demo:e2e:mutation` and `pnpm serve:e2e`), `pnpm selfhost:e2e`,
`pnpm quickstart:e2e` and blob-store's MinIO conformance on every push to
`main` and pull request, and the live-PostgreSQL suite, against a
`postgres:18` service container, on pushes to `main` and by manual dispatch
only. Those last two run the Rust tests that need an S3 provider or a live
PostgreSQL 18, which are `#[ignore]`d, so `pnpm verify` skips them. A passing
`pnpm verify` does not mean the performance targets are met; `pnpm bench`
measures those.

## What `pnpm demo` runs

`calque dev` serves the tables of a disposable local PostgreSQL database on
loopback. It runs the same engine as `calque serve` — snapshot,
direct-columnar artifacts, the streamed change route and the CRUD mutation
route — behind one shared development bearer instead of signed grants. With
`--tenant-column` and `--tenant` it serves a partitioned scope, and the bearer
reads that one tenant. It provides no authorization between tenants and no
production operations.

`pnpm demo` starts that database from `docker-compose.yml`, which carries a
committed password and no Calque server; the self-hosted composition is
`docker-compose.selfhost.yml`. The Chrome window it opens on macOS uses a
temporary Calque-owned profile with no extensions, so extensions and
personal-profile policy cannot intercept local sync requests. The Project
Atlas dashboard loads complete `projects` and `tasks` tables, runs joins and
aggregates locally, and keeps one change request open. No database credentials
or bearer token are placed in command-line arguments or a Vite environment file.

By default `calque` runs from its container image, `ghcr.io/calque-sh/calque`
at the version `@calque/client`'s `package.json` carries, so the demo needs
Docker, Node and pnpm and no Rust. `pnpm demo` pulls the image if this host
lacks it. The container joins the PostgreSQL container's network namespace,
because `calque` reaches a database that is not a numeric loopback address only
over verified TLS; `docker-compose.demo-image.yml` publishes the server's port
from that container, on the host's loopback address only, and the container runs
`calque dev --bind-address 0.0.0.0` as the host user on the state directory,
`.calque-dev`, which it mounts. `pnpm demo -- --from-source` builds and runs the
checkout's `calque` with Cargo instead, and `CALQUE_DEMO_IMAGE` runs another
image, such as one from `docker build -t calque:dev .`. The metrics listener
`CALQUE_LOCAL_METRICS_PORT` starts needs `--from-source`.

## Browser harnesses

Headless-Chromium harnesses exercise the `pnpm demo` flow and need Docker
for the disposable PostgreSQL container. They build `calque` from the working
tree (`--from-source`), except `pnpm demo:e2e:image`:

- `pnpm demo:e2e:image` builds the Dockerfile into a local image and starts the
  launcher as a reader does, with no `--from-source`, so `calque` runs from
  that image. It drives the page through one source change, and checks that the
  port is published on loopback only and that stopping the launcher removes the
  container. `CALQUE_DEMO_IMAGE` runs an existing image instead of building.
- `pnpm demo:smoke` drives a running demo (`CALQUE_DEMO_URL`, default
  `http://127.0.0.1:5173`) through one source change.
- `pnpm demo:e2e` starts two demos in turn, checks that an open tab survives
  the restart, and runs a duplicate-visible-transaction oracle: source
  transactions applied on each side of the restart, every publication
  recorded with its cursor, and assertions that no cursor is delivered twice,
  that neither phase is short of the transactions it applied, and that the
  replacement served the same artifact at the same snapshot cursor rather than
  forcing a resnapshot the durable state made unnecessary.
  `CALQUE_RESTART_E2E_FAULT=duplicate-delivery` records one publication twice
  and must turn the run red.
- `pnpm demo:e2e:launcher` lets the launcher open its own Chrome and drives
  that window over its debugging port.
- `pnpm demo:e2e:latency` starts one demo, applies 20 measured changes, and
  writes commit-to-presented-frame timings to
  `target/demo-latency/report.json`.

`pnpm demo:e2e:resnapshot` covers the architecture's "explicit snapshot
rebuild" branch. It answers exactly one change poll with the 410
`CURSOR_BEFORE_RETENTION` refusal `change-http` sends for a cursor the source
can no longer serve, framed by the built `@calque/protocol`, and then asserts
that the page recovers by itself: a second bootstrap plan, a fresh artifact and
a live change stream again, with no main-frame navigation, the JavaScript
sentinel it wrote beforehand still in place, and every `#status` transition
recorded by an observer installed before the refusal — `ready` throughout, so
the page never showed the failure. Only that one response is fabricated;
everything after it is the real server.

Revocation is covered against `calque serve`, not `calque dev`, because only
`serve` verifies grants: `pnpm serve:e2e` runs `scripts/serve-revocation-e2e.ts`
after the mutation run. The script is the application backend. It signs a
fence order with the key `calque-dev-grant` minted the grants with, signs the
barrier drafts Calque answers with, and hands each page the grant its `auth()`
returns next — a narrower one, none, or a renewal. A `MutationObserver` on each
page's task table keeps every row ever inserted. A row committed after the
fence separates the retired generation's rows from the new one's: the run
asserts that a revoked page locks and paints nothing more, that a rebuilt page
paints only rows its new mount delivered, that a write in flight ends ambiguous
with the documented message and is later refused 401, and that an expired grant
locks the page whose application cannot renew it, while the page whose
application renews it rebuilds without going blank.

`pnpm record:incidents` regenerates `docs/assets/calque-incidents-100k.gif`, the
README's recording of `pnpm incidents -- --rows 100k --dataset content-v1`. It
drives the page in headless Chromium, commits a transaction through `psql`,
and needs Docker, Rust and `ffmpeg`.

## The demo by hand

The `pnpm demo` flow by hand, for debugging or a different disposable fixture.
It needs PostgreSQL's client tools, `psql` and `pg_isready`, on the host, and
port 55432 free. If `pnpm demo` has run, its PostgreSQL container is still on
that port; stop it first with `docker compose stop postgres`. First the database
and its least-privilege role:

```sh
docker run --name calque-pg --rm -d \
  -e POSTGRES_PASSWORD=postgres \
  -e POSTGRES_DB=calque_dev \
  -p 127.0.0.1:55432:5432 \
  postgres:18 \
  -c wal_level=logical \
  -c max_replication_slots=10 \
  -c max_wal_senders=10 \
  -c timezone=UTC

for attempt in $(seq 60); do
  pg_isready -h 127.0.0.1 -p 55432 -U postgres -d calque_dev >/dev/null && break
  sleep 1
done
pg_isready -h 127.0.0.1 -p 55432 -U postgres -d calque_dev

psql 'postgresql://postgres:postgres@127.0.0.1:55432/calque_dev' <<'SQL'
CREATE ROLE calque_sync LOGIN PASSWORD 'calque_sync' REPLICATION;
GRANT CREATE ON DATABASE calque_dev TO calque_sync;
GRANT CREATE, USAGE ON SCHEMA public TO calque_sync;
SQL
```

The loop gives PostgreSQL 60 seconds. If the `pg_isready` after it still
reports no response and exits non-zero, `docker logs calque-pg` shows why.

The seed runs as that role too, so it owns the tables the server will publish:

```sh
export CALQUE_DATABASE_URL='postgresql://calque_sync:calque_sync@127.0.0.1:55432/calque_dev'
psql "$CALQUE_DATABASE_URL" <<'SQL'
CREATE TABLE projects (
  id bigint PRIMARY KEY,
  name text NOT NULL,
  status text NOT NULL
);
CREATE TABLE tasks (
  id bigint PRIMARY KEY,
  project_id bigint NOT NULL REFERENCES projects(id),
  title text NOT NULL,
  done boolean NOT NULL DEFAULT false
);
INSERT INTO projects VALUES (1, 'Local demo', 'draft');
INSERT INTO tasks VALUES (1, 1, 'Run the first exact query', false);
SQL
```

The page's bindings are the public path's output and are checked in beside it:
`client/packages/client/tests/demo/calque.config.json` is
`calque init` of `calque inspect` of this seed, and `calque.generated.ts` and
`calque.start.generated.ts` are the `calque-bindings` render of it. `pnpm demo`
proves the render current against the checked-in config before it builds the
page, so the page can never be served against bindings that predate its
configuration.

Regenerate the three files after changing the seed. `calque inspect` refuses a
superuser and requires the tables to already be in a publication, so it runs
as the `calque_sync` role created above, against a publication that role
declares first:

```sh
export CALQUE_DATABASE_URL='postgresql://calque_sync:calque_sync@127.0.0.1:55432/calque_dev'
psql "$CALQUE_DATABASE_URL" \
  -c 'CREATE PUBLICATION calque_demo FOR TABLE public.projects, public.tasks;'

inspection="$(mktemp)"
cargo run -p calque -- inspect --database-url-env CALQUE_DATABASE_URL \
  --source local --publication calque_demo \
  --table public.projects --table public.tasks > "$inspection"
cargo run -p calque -- init --force --from-inspection "$inspection" \
  --application-id demo --connection-env CALQUE_DATABASE_URL \
  --output client/packages/client/tests/demo/calque.config.json
pnpm --filter @calque/config build
pnpm exec calque-bindings \
  --config client/packages/client/tests/demo/calque.config.json \
  --out client/packages/client/tests/demo/calque.generated.ts
pnpm --filter @calque/client exec prettier --write \
  tests/demo/calque.config.json
```

`calque init` writes indented JSON; the `prettier --write` brings it to the
form the package's `lint` gate checks, and `calque init` reproduces the
checked-in `calque.config.json` byte for byte this way.

In a second terminal, start `calque dev` for the same relations:

```sh
export CALQUE_DATABASE_URL='postgresql://calque_sync:calque_sync@127.0.0.1:55432/calque_dev'

cargo run -p calque -- dev \
  --state-dir .calque-dev \
  --table public.projects \
  --table public.tasks \
  --allow-origin http://127.0.0.1:5173 \
  --public-base-url http://127.0.0.1:5173/_calque/ \
  --acknowledge-disposable-source
```

That writes the service's endpoint and bearer to `.calque-dev/client.env`,
which the browser never sees. In a third terminal, build the packages, whose
built output the page imports, and start the page's Vite server:

```sh
pnpm -r build
pnpm --filter @calque/client manual:dev
```

The page's Vite config uses `calque()` from `@calque/client/vite`, which reads
the nearest `.calque-dev/client.env` at or above the page's root and proxies
`/_calque/` to that `calque dev`, swapping the page's placeholder bearer for
the one in the file. `CALQUE_LOCAL_SERVICE_ENDPOINT` and
`CALQUE_LOCAL_BEARER_TOKEN` name another `calque dev` instead.

The page reads and renders as an ordinary `createSyncClient` consumer of the
generated schema: it asks the service for the relations the bindings name, the
service resolves each to the wire table id of the enrolled table and answers a
plan carrying the artifact's identity, physical schema, and wire column ids,
and the client runs the fenced catch-up from the artifact cursor to the
activation cursor before it reports ready. Every row it shows comes from
`client.db.<collection>` facade queries.

It renders with the `@calque/react` hooks, which re-read every live query a
change moved together and paint them on one turn, at one source state. To let
`pnpm demo:e2e:latency` time commit to presented frame, the page records each
batch's source commit time and changed collections from
`client.subscribePublications`, which any application can call.

Nothing the page loads imports `@calque/client/internal`, which is for tests,
or reaches a package by relative path. The page's generated bindings take
`createGeneratedClientSchema` from `@calque/client/generated`, a subpath only a
generated module may import and only for that factory.
`pnpm --filter @calque/client api:check` fails if anything the page loads
breaks either rule.

Open `http://127.0.0.1:5173` in Chrome and wait for the complete dashboard, then
edit PostgreSQL:

```sh
psql "$CALQUE_DATABASE_URL" \
  -c "UPDATE projects SET status = 'active' WHERE id = 1"
```

The dashboard advances once the canonical transaction is durable. `Ctrl-C`
preserves the publication and logical slot for restart. Remove the PostgreSQL
objects it owns and the local state with
`cargo run -p calque -- destroy --state-dir .calque-dev --yes`. A
missing or corrupt published artifact fails closed, and Calque recovers
through that same destroy-and-reseed path rather than automatic repair.

If PostgreSQL was recreated and its identity no longer matches `state.json`,
destroy reports both identities and leaves everything untouched. Reconnect
`CALQUE_DATABASE_URL` to the original source and rerun destroy, or remove
only the now-orphaned local files with
`cargo run -p calque -- destroy --state-dir .calque-dev --yes --local-state-only`.
That explicit recovery does not remove PostgreSQL objects from the original
source.

## Live-PostgreSQL suite

The `#[ignore]`d tests cover what the unit suites cannot reach: CDC decoding,
fenced writer takeover, snapshot export and copy, cancellation and cleanup,
source-wide startup, isolation and revocation.
`cargo test` skips them silently, so they have to be asked for. All but two
belong to this suite (`snapshot_recovery_child_exits_without_cleanup` among
them is a re-exec fixture that returns at once outside its child process); the
other two, in blob-store, need an S3-compatible provider and run through
`pnpm verify:s3:minio`. One test in this suite needs a bucket as well as
PostgreSQL: `recovery::tests::a_filesystem_unit_restores_into_a_provider_store_and_serves_from_it`
fails unless `CALQUE_S3_ENDPOINT`, `CALQUE_S3_BUCKET`, `CALQUE_S3_ACCESS_KEY`
and `CALQUE_S3_SECRET_KEY` name a bucket that exists. Those names cannot be set
for the rest of the suite: its `serve_*_live` tests start `calque serve`, which
refuses them without `CALQUE_OBJECT_STORE=s3`. CI's `live-postgres` job runs
the suite with `--exact --skip` and that test's name, then starts MinIO and
runs `--exact` and the name alone.

They need a **non-superuser** role with `REPLICATION`. Enrollment rejects a
superuser with `SUPERUSER_NOT_LEAST_PRIVILEGE`, so running the suite as
`postgres` fails at setup rather than testing anything. The role also needs
`CREATEDB` and `CREATEROLE`: tests create throwaway databases, and the refusal
tests create a throwaway role and change its `REPLICATION` or password.

`scripts/live-postgres-suite.sh` is the setup CI uses; locally it needs only a
running container:

```sh
docker run --name calque-pg-live --rm -d \
  -e POSTGRES_PASSWORD=postgres -p 127.0.0.1:55433:5432 postgres:18 \
  -c wal_level=logical -c max_replication_slots=20 -c max_wal_senders=20 \
  -c max_connections=400

# Waits for the server, checks wal_level, slots, senders, connections and the
# major version, creates the role and database (idempotent), and asserts the
# role is not a superuser. Then runs the suite and fails if fewer distinct
# tests passed than `cargo test -- --ignored --list` names under the same
# harness args, so `run 55433 --exact <name>` runs one test and a filter
# matching nothing fails.
scripts/live-postgres-suite.sh bootstrap calque-pg-live
scripts/live-postgres-suite.sh run 55433
```

The script's steps one at a time, for running pieces of the suite:

```sh
docker exec -i calque-pg-live psql -U postgres <<'SQL'
CREATE ROLE calque_test LOGIN PASSWORD 'calque_test'
  REPLICATION CREATEDB CREATEROLE NOSUPERUSER;
CREATE DATABASE calque_verify OWNER calque_test;
SQL
docker exec calque-pg-live psql -U postgres -d calque_verify \
  -c 'GRANT ALL ON SCHEMA public TO calque_test;'

# Seven names read by different tests, all wanting the same connection string;
# the script's header says which are required and which are alternates.
eval "$(scripts/live-postgres-suite.sh env 55433)"

# blob-store is excluded: its two S3 conformance tests run through
# `pnpm verify:s3:minio`.
cargo test --workspace --exclude blob-store --no-fail-fast -- --ignored
```

Apart from that S3 test when no bucket is configured, they are expected to pass
with no failures and are safe to run repeatedly against the same database.
`--no-fail-fast` is worth keeping: without it one failing crate hides every
crate after it. The two slowest binaries,
`snapshot-pipeline`'s `live_publication` and `source-runtime`'s
`takeover_matrix`, wait on real lease TTLs, barrier timeouts and process kills.
`takeover_matrix` re-executes its own test binary as a worker it then SIGKILLs
at each of 22 ingest boundaries; it needs nothing beyond POSIX process control,
and its per-schedule reports land in
`server/crates/source-runtime/target/takeover-matrix/reports/`.
