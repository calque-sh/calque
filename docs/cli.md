# The `calque` command

`calque` is the one server binary: `serve` for a deployment, `dev` for a
disposable local source, `inspect` and `init` for configuration, `status` and
`destroy` for a state directory, and `backup`, `restore` and `verify-restore`
for recovery. Connection strings are always read from a named environment
variable, never from `argv`, so credentials never reach process listings or
shell history. From a checkout, `cargo run -q -p calque --` builds and runs it,
as the commands below do, and `cargo install --locked --path server/apps/calque`
installs it.

Before anything is enrolled, check that a PostgreSQL 18 publication's tables
can be served, and print their schema as the `calque.pg-inspection.v1` document
`init` reads:

```sh
export CALQUE_DATABASE_URL='postgresql://...'
cargo run -q -p calque -- inspect \
  --database-url-env CALQUE_DATABASE_URL \
  --source primary \
  --publication calque_incident \
  --table public.issues \
  --table public.issue_notes > inspection.json
```

Turn that document into a configuration manifest:

```sh
cargo run -q -p calque -- init --from-inspection inspection.json \
  --application-id incident-ops
```

`init` writes `calque.config.json` and refuses to overwrite an existing file
without `--force`. It accepts `-` to read the inspection from stdin, so the two
commands pipe together. The manifest is accepted by `compileConfig` in
`@calque/config`, primary keys are excluded from its `writableColumns`, and a
table without a primary key is rejected rather than silently emitted. A
collection's id is its table's name, because the client binds a collection only
to the served table of that name. `init` writes each table's name as its id and
refuses a name outside the identifier grammar rather than lowering it;
`compileConfig` and `calque serve` refuse any other id. Two inspected tables
that share a name in different schemas are refused by `init`, which names both,
and by `calque dev` and `calque serve`.

`--tenant-column tenant_id` writes a partitioned scope instead, which gives each
grant one tenant's rows. A table whose primary key leads with `tenant_id` holds
tenants' rows, and a table without that column is shared whole by every tenant.
`init` refuses a table that has the column anywhere but first in its key, a
tenant column that is not `int2`, `int4`, `int8` or `uuid`, and tenant columns
of different types. The grant attribute that carries a grant's tenant is the
column's name unless `--tenant-attribute` names another.

`calque dev --tenant-column tenant_id --tenant 42` serves the same kind of
scope from a disposable source, partitioned by the same rule and refusing the
same tables. Its shared bearer reads tenant 42's rows and every shared table,
and each tenant is built on its first request, as under `calque serve`.
`--tenant` is an integer, or a UUID when the column is a `uuid`.

## The container image

The release workflow publishes the image as `ghcr.io/calque-sh/calque`, tagged
with the release version and `latest`:

```sh
docker pull ghcr.io/calque-sh/calque
docker run --rm ghcr.io/calque-sh/calque --help
```

To build it from a checkout:

```sh
docker build -t calque:dev .
docker run --rm calque:dev --help
```

`calque dev` binds `127.0.0.1`. `--bind-address 0.0.0.0` is for a container
whose port the host publishes on its loopback address, as `pnpm demo` does:
the shared bearer is the only credential, and the browser still addresses the
server as `127.0.0.1` on `--port`.

The image carries the `calque` binary. It needs `libpq` to build and to run,
because the vendored `pg_walstream` fork links `pq-sys`. `protoc` is not needed
(`sync-protocol`'s build script uses `protoc-bin-vendored`), and the binary's
own TLS is rustls, so no OpenSSL development package is needed to build it.
