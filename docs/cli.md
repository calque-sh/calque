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

When PostgreSQL bounds neither `max_slot_wal_keep_size` nor
`idle_replication_slot_timeout`, `inspect` also warns on stderr, and so do
`serve` and `dev` on each start: a stopped Calque's slot would hold WAL until
the disk fills ([docs/postgresql.md](postgresql.md#protecting-the-primarys-disk)).

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
and by `calque dev` and `calque serve`. A table whose publication column list
leaves out a `NOT NULL` column with no default gets no insert command, since
an insert through Calque could not give that column a value.

`calque destroy` removes the state directory, the replication slot and the
source's rows in Calque's own schemas. It drops the publication only when
Calque's role owns it, and prints `Kept the publication ..., which another
role owns.` otherwise.

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

## Running a server

`calque dev` serves one disposable database on loopback with a development
bearer, for working on a page. It needs `--table schema.table`, repeated for
each table, and `--acknowledge-disposable-source`, which confirms the database
holds nothing you need. It reads `CALQUE_DATABASE_URL`, or the variable
`--database-url-env` names, and keeps its state in `--state-dir`, `.calque-dev`
by default, which holds `client.env`, the address and bearer the Vite plugin
reads. It listens on `127.0.0.1:8787` and allows the origin
`http://127.0.0.1:5173`; `--port`, `--bind-address` and `--allow-origin` change
those, and `--public-base-url` names the proxy that fronts it:

```sh
calque dev --table public.tasks \
  --public-base-url http://127.0.0.1:5173/_calque/ \
  --acknowledge-disposable-source
```

`calque dev` also takes a database that is not on this machine over TLS that
verifies its name, as [the database URL](postgresql.md#the-database-url)
describes. When the URL says `sslmode=disable` on a host name, it connects in
plaintext, prints a warning that names the host, and sends the password and
every row in the clear, so use that only for a disposable database. `inspect`
and `destroy` take it too, and print the same warning; `serve`, `backup`,
`restore` and `verify-restore` refuse it. Every command refuses
`sslmode=require`, `verify-ca`, `prefer` and `allow`, because none of them
checks the server's name.

`calque serve` is the deployment server. It verifies signed grants and mints
none. It needs `--grant-audience`, the audience every grant must carry,
`--grant-verification-key-env`, the variable that holds the public half of the
signing key and is repeated for rotation, and `--allow-origin`, repeated for
each browser origin, which is HTTPS or loopback. It reads `--config`,
`calque.config.json` by default, takes the database URL from the variable that
file's `sources[].databaseUrlEnv` names, and keeps its state in `--state-dir`,
`.calque-serve` by default. On a wildcard `--bind-address` it also needs
`--expected-host <host[:port]>`, the exact `Host` header browsers send.
[Self-host](self-host.md) runs it behind HTTPS, and
[operations](design/operations.md) lists every setting.

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

### Running in a container

The image's entrypoint is `calque`, so what follows `ghcr.io/calque-sh/calque`
is the subcommand. It runs as an unprivileged user, and `inspect`, `init` and
`dev` write `calque.inspection.json`, `calque.config.json` and `.calque-dev`
to the working directory. Mount your
application's directory, work in it, and run as your own user so that what the
container writes stays yours. The database URL goes in through the environment,
never as an argument.

On Linux, `--network host` gives the container the host's network, so a
database on this machine is `127.0.0.1`:

```sh
export CALQUE_DATABASE_URL='postgresql://calque_sync:calque_sync@127.0.0.1:5432/app'
docker run --rm -e CALQUE_DATABASE_URL --network host \
  -v "$PWD":/work -w /work --user "$(id -u):$(id -g)" \
  ghcr.io/calque-sh/calque inspect --source primary --publication calque_app \
  --table public.tasks > calque.inspection.json
docker run --rm --network host \
  -v "$PWD":/work -w /work --user "$(id -u):$(id -g)" \
  ghcr.io/calque-sh/calque init --from-inspection calque.inspection.json \
  --application-id my-app
docker run --rm -e CALQUE_DATABASE_URL --network host \
  -v "$PWD":/work -w /work --user "$(id -u):$(id -g)" \
  ghcr.io/calque-sh/calque dev --table public.tasks \
  --public-base-url http://127.0.0.1:5173/_calque/ \
  --acknowledge-disposable-source
```

On Docker Desktop, `--network host` does not reach your machine's loopback
address, and the host answers as `host.docker.internal`. Calque refuses a host
name without verified TLS, except that `dev`, `inspect` and `destroy` accept
an explicit `sslmode=disable` for a disposable database. The server also has
to bind `0.0.0.0` inside the container, and the container has to publish its
port on your loopback address only:

```sh
export CALQUE_DATABASE_URL='postgresql://calque_sync:calque_sync@host.docker.internal:5432/app?sslmode=disable'
docker run --rm -e CALQUE_DATABASE_URL -p 127.0.0.1:8787:8787 \
  -v "$PWD":/work -w /work --user "$(id -u):$(id -g)" \
  ghcr.io/calque-sh/calque dev --table public.tasks --bind-address 0.0.0.0 \
  --public-base-url http://127.0.0.1:5173/_calque/ \
  --acknowledge-disposable-source
```

`inspect` reads the same URL, and `init` reads no database:

```sh
docker run --rm -e CALQUE_DATABASE_URL \
  -v "$PWD":/work -w /work --user "$(id -u):$(id -g)" \
  ghcr.io/calque-sh/calque inspect --source primary --publication calque_app \
  --table public.tasks > calque.inspection.json
docker run --rm \
  -v "$PWD":/work -w /work --user "$(id -u):$(id -g)" \
  ghcr.io/calque-sh/calque init --from-inspection calque.inspection.json \
  --application-id my-app
```

PostgreSQL has to listen on an address the container can reach, and
`pg_hba.conf` has to allow the container's address.

The image carries the `calque` binary. It needs `libpq` to build and to run,
because the vendored `pg_walstream` fork links `pq-sys`. `protoc` is not needed
(`sync-protocol`'s build script uses `protoc-bin-vendored`), and the binary's
own TLS is rustls, so no OpenSSL development package is needed to build it.
