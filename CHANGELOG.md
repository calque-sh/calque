# Changelog

Every `@calque` package, the `calque` binary and the
`ghcr.io/calque-sh/calque` image share one version, and each release below
covers all of them. Calque is pre-alpha: a minor version may break
configurations, stored state and APIs, and its section says how.

## 0.4.0 (2026-11-17)

### Breaking

- A state directory prepared by 0.3.0, and a recovery unit 0.3.0 took, are
  refused and need `calque destroy`. Calque now records each served column's
  version, so `ALTER COLUMN ... TYPE` to the column's own type with a `USING`
  expression stops the server, asking for `calque destroy`, instead of leaving
  the old values served.
- `calque serve` and `calque dev` refuse to start when PostgreSQL could not
  hold their preparations. Each preparation holds a replication slot and a
  WAL sender beside change capture's, and the server needs 24 connections plus
  6 per preparation. A server that started under tighter
  `max_replication_slots`, `max_wal_senders`, `max_connections` or connection
  limits now refuses, naming the limit to raise.

### Added

- PostgreSQL 17 is supported as a source, beside 18.
- After a re-cut, a page that stored an older cut catches up from it instead
  of downloading the newest, while the server keeps that cut.
  `--recut-keep-ratio` (default 0.25) sets how many bytes of changes since an
  older cut the server keeps, per byte of the newest artifact; 0 keeps only
  the newest cut.
- `renderBootstrap` takes the page request's `Cookie` header as `cookie`, and
  leaves out the artifact preload when the page already holds that artifact.
- A page keeps its working set between visits, encrypted in the browser's
  origin private file system, and a returning page reads it from disk instead
  of downloading it again. `createSyncClient({ storeRows: false })` keeps
  nothing, `SyncClient.clearStoredRows()` erases what is kept, and
  `calque serve --no-stored-rows` or `calque dev --no-stored-rows` turns it
  off for every page.

### Changed

- A publication created for another set of tables is refused with the
  `DROP PUBLICATION` and `CREATE PUBLICATION` statements for its owner to
  run, instead of pointing at `calque destroy`, which keeps a publication
  another role owns.
- A returning page catches up sooner and with less memory: it asks for the
  changes since its stored copy while it reads that copy, and it decodes
  change frames with `@bufbuild/protobuf` 2.17.0.

### Fixed

- A table whose row-level security would filter Calque's reads is refused
  with `ROW_LEVEL_SECURITY_ACTIVE`, naming the role that needs `BYPASSRLS`,
  instead of serving an empty snapshot.
- `calque serve` and `calque dev` handle SIGINT and SIGTERM from the moment
  they print that they are ready, so a stop sent right then exits cleanly.
- A React Suspense retry that renders more than 5 seconds after a query
  settled takes back its rows instead of suspending and running the query
  again.

## 0.3.0 (2026-11-14)

### Breaking

- A state directory written by 0.2.0 needs `calque destroy`.

### Added

- `calque dev` reads `calque.config.json`, or the manifest `--config` names,
  and publishes only the columns each collection names, as `calque serve`
  does. Without a manifest, `--table` is still required and each table is
  served whole.

### Changed

- Adding a value to an enum no longer stops change capture. Renaming a
  value, swapping labels, re-adding an old label or renumbering still stops
  it and asks for `calque destroy`.
- The browser sorts an enum column in the order its type declares the
  labels.

## 0.2.0 (2026-11-14)

0.2.0 is tagged on the public repository but was never published to npm or
ghcr.io. Its changes reached them in 0.3.0.

### Breaking

- `sources[].connectionEnv` is now `sources[].databaseUrlEnv`, and
  `calque init --connection-env` is now `--database-url-env`. The old names
  are refused with the new one.
- `insert`, `update`, `delete` and `retryMutation` resolve only when the
  server accepts the write. A refused write rejects with
  `MutationSubmissionError`: `MUTATION_REJECTED`, or the new
  `MUTATION_CONFLICT` carrying the conflict the server named. After a lost
  response or a "try later" answer the client resends the write up to four
  times before it rejects with `MUTATION_AMBIGUOUS`.
- Every package requires Node `^22.23.4 || ^24.22.1 || >=26.12.1`.

### Added

- `@calque/grant` signs dataset grants and tenant orders from a Node
  backend, and `calque-grant keygen` creates the key. It needs no Rust
  toolchain.
- `calque serve` runs as a role that owns no table, and serves enums and
  domains. When the publication is missing it prints the
  `CREATE PUBLICATION` statement for the tables' owner.
- `calque serve` publishes only the columns each collection names, so a
  column a collection leaves out never leaves PostgreSQL.
- A page renews its plan before it expires and keeps its rows, instead of
  downloading its working set again.
- Amazon RDS's `rds_replication` role counts as the replication privilege.
- `calque inspect` and `calque destroy` accept the plaintext database URL
  `calque dev` accepts.

### Changed

- The client checks for the browser APIs it calls instead of the browser's
  brand.
- Refusals name the role, privilege, setting or variable to fix: a refused
  login, a missing privilege, a lost replication slot, a refused `Host` or
  `Origin`, an object-store setting, a missing manifest.
- A source whose publication was altered stops, even if the publication was
  put back before the next start.
- `inspect`, `serve` and `dev` warn when nothing bounds the WAL a stopped
  replication slot holds back.

### Fixed

- A restart after a crash no longer stops change capture with
  `ReplayBehindCompaction`.
- A recovery unit taken by 0.1.0 restores.
- A replication stream that stays silent past its bound is dropped and
  reconnected.
- Live-query resources no component subscribes to are released.

## 0.1.0 (2026-11-07)

The first public pre-alpha: `calque dev`, `calque serve` and the
`ghcr.io/calque-sh/calque` image, with `@calque/client`, `@calque/react`,
`@calque/core`, `@calque/protocol`, `@calque/worker` and `@calque/config`.
