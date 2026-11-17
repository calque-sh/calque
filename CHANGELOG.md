# Changelog

Every `@calque` package, the `calque` binary and the
`ghcr.io/calque-sh/calque` image share one version, and each release below
covers all of them. Calque is pre-alpha: a minor version may break
configurations, stored state and APIs, and its section says how.

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
