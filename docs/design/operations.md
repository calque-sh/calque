# Operations runbook

This runbook covers two things that run the same engine: the single-process
`calque dev` server, and the self-hosted `calque serve` composition below.
All commands keep PostgreSQL credentials out of argv.

## The self-hosted deployment

`docker-compose.selfhost.yml` is the deployment: PostgreSQL 18, an
S3-compatible object store, `calque serve` from the repository's
`Dockerfile`, and Caddy terminating TLS in front of it. The other compose file,
`docker-compose.yml`, is the development one: one PostgreSQL for `pnpm demo`,
no server, a committed password. It is not a deployment and may not be turned
into one, because it would put a committed secret on the deployment path.

### What it runs

| Service | Image | What it is |
| --- | --- | --- |
| `postgres` | `postgres:18` | The source, with `wal_level=logical`. Its data is the `postgres-data` volume, mounted at `/var/lib/postgresql` — one level above the data directory, because the 18 image refuses a volume at `/var/lib/postgresql/data`. Its Unix socket directory is the `postgres-socket` volume, which is how the server reaches it. |
| `objectstore` | MinIO (`pgsty/minio`), digest-pinned | The S3-compatible provider the published artifacts live in, on the `objectstore-data` volume. This is why the deployment does not use the filesystem store: artifact bytes have to outlive every container in the file. |
| `objectstore-init` | the same image | Creates the bucket with `mc mb --ignore-existing` and exits. `calque serve` never creates a bucket: provisioning storage is an operator action, and a server that creates its own bucket answers a mistyped bucket name by starting somewhere empty. |
| `calque` | built from `Dockerfile` | `calque serve`, reading the manifest mounted at `/etc/calque/calque.config.json` and keeping its private state on the `calque-state` volume. Recovery units go on the `calque-recovery` volume, at `/recovery`: see "Backup and restore against the composition". |
| `caddy` | Caddy 2.11.4, digest-pinned | The TLS edge, and the only way a browser reaches the server: see "TLS at the edge". Its certificates and local CA are on the `caddy-data` volume. |

#### How the server reaches PostgreSQL, and why it is not by name

The server connects over PostgreSQL's Unix socket, on the `postgres-socket`
volume both containers mount. Its connection string is
`postgresql:///calque?host=%2Fvar%2Frun%2Fpostgresql&user=…&password=…`.

`DatabaseUrl::parse` (`server/crates/calque-engine/src/guard.rs`) accepts a
**numeric** loopback address or a Unix socket directory it canonicalizes once,
both in plaintext, and any other host only with `sslmode=verify-full` and a CA
file ([PostgreSQL requirements](../postgresql.md#the-database-url)). A
composition service name — `postgres:5432` — is a host name, so the guard
requires TLS for it, and the composition's PostgreSQL has no certificate. Two
rules decide this:

- **No plaintext on a network.** Every one of the eighteen connection families
  gets the transport built into the normalized URL the guard produces:
  `sslmode=disable` for a socket or a loopback address, `sslmode=verify-full`
  and the canonical CA path for any other host. libpq enforces that itself,
  and the Rust driver connects through a rustls connector that trusts only
  that file. Plaintext to a host name would put the whole replicated dataset,
  the control rows and the session in cleartext on an interface, with no
  server authentication: anything that can answer for that name, or sit
  between, would be the database.
- **One endpoint, chosen once.** The guard resolves nothing. A host name is
  accepted only because the TLS handshake proves the server holds a
  certificate for it, which is why `localhost` needs TLS like any other name.
  The endpoint is chosen once, by the composition, and no later connection
  family, query parameter (`host`, `hostaddr`, `service`,
  `target_session_attrs`, …) or libpq environment variable can move it.

No configuration flag makes cleartext-on-a-network untrue, so there is none.
What removes the need for one is the socket: it satisfies the rule with no
network at all — strictly stronger than the loopback hop it replaces — and it
leaves the two services independent. Each publishes its own port, and recreating
`postgres` does not take the server's network with it. `pnpm selfhost:e2e`
removes the `postgres` container under a running server and asserts exactly
that, and then that the server rejoins the recreated cluster without being
recreated itself — see "When PostgreSQL goes away under a running server".

A socket is a capability, not an identity, so the password still has to be
checked. It is: the `user` and `password` query parameters carry it, because a
URL with an empty authority cannot carry userinfo, and they are refused for a
TCP host so there is one place to write a credential per transport. They are
redacted in every diagnostic and never appear in argv.

That check is only real because the composition initializes the cluster with
`POSTGRES_INITDB_ARGS=--auth=scram-sha-256`; "Configuration" below says why,
and how to check an existing volume.

Two hops in this composition are plaintext, both on the compose bridge. Caddy
forwards each browser request to the server over HTTP, so bearer grants,
artifact bytes and mutations cross the bridge in the clear behind the edge.
The server reaches the object store (`http://objectstore:9000`) by name: SigV4
does not put the secret key on the wire, but artifact bytes cross in the clear
there too. Terminating TLS in front of the object store is a deployment
decision the `CALQUE_S3_ENDPOINT` variable already allows.

### Configuration

Every value is an environment variable. Nothing is a command-line argument:
a secret in argv appears in `ps` and in shell history, which is the same reason
the PostgreSQL connection string has never been one. That is the whole of what
the environment buys — `docker inspect` prints a container's environment
verbatim, and so does `/proc/<pid>/environ`, so these values are readable by
anyone who can reach the Docker socket or the host. Keep them run-scoped. The
variables without a default refuse to interpolate, so `docker compose up`
without them stops with the variable's own message rather than starting on a
guessable password.

A password only buys anything if the cluster checks it, and as `initdb`
generates `pg_hba.conf` by default it does not: the `trust` rules it writes for
the Unix socket and for loopback shadow the `scram-sha-256` rule the image
appends after them, and pg_hba is first-match-wins. Those are the two paths this
composition runs on: the server connects over the socket, and `docker compose
exec … psql -h 127.0.0.1` over the container's loopback. On such a cluster,
anything that reaches the socket connects as the bootstrap superuser, the role
enrollment refuses on purpose. The `postgres` service sets
`POSTGRES_INITDB_ARGS=--auth=scram-sha-256`, so `initdb` writes those rules as
`scram-sha-256` in the first place, and
`docker/selfhost-postgres-init/10-calque-no-trust-rules.sh` refuses to let the
container finish its first start if `pg_hba_file_rules` shows any `trust` rule.
Nothing rewrites or reloads the file, so the rules on disk are the rules the
server loaded. The image runs `initdb` and `/docker-entrypoint-initdb.d` only on
an empty data directory: **a data directory initialized without them keeps its
`trust` rules**, and an operator with such a volume must edit `pg_hba.conf`
inside it and `pg_ctl reload` by hand. `SELECT * FROM pg_hba_file_rules WHERE
auth_method = 'trust'` returning no rows is the check.

| Variable | Default | What it sets |
| --- | --- | --- |
| `CALQUE_SELFHOST_PROJECT` | `calque-selfhost` | The compose project name. |
| `CALQUE_SELFHOST_POSTGRES_SUPERUSER` | `calque_admin` | The image's bootstrap role. `calque serve` never uses it: enrollment refuses a superuser with `SUPERUSER_NOT_LEAST_PRIVILEGE`. |
| `CALQUE_SELFHOST_POSTGRES_SUPERUSER_PASSWORD` | *required* | Its password. |
| `CALQUE_SELFHOST_DATABASE` | `calque` | The database. |
| `CALQUE_SELFHOST_DB_ROLE` | `calque_sync` | The least-privilege `LOGIN REPLICATION` role the server connects as, created by `docker/selfhost-postgres-init`. |
| `CALQUE_SELFHOST_DB_PASSWORD` | *required* | Its password, quoted into SQL by `format()`'s `%L`. It is also interpolated into a URL query value, so it must be URL-safe: hexadecimal is the safe shape, as for the object-store keys. |
| `CALQUE_SELFHOST_POSTGRES_PORT` | `55452` | Published on `127.0.0.1` for the operator: enrollment inspection, seeding, backup. |
| `CALQUE_SELFHOST_PUBLIC_HOST` | `localhost` | The one name browsers use. Caddy serves only this name and takes its certificate for it; the server answers exactly this `Host` and builds every artifact and change URL on `https://<it>:<HTTPS port>/`. |
| `CALQUE_SELFHOST_HTTPS_PORT` | `8443` | The edge's published port. A real host uses `443`. |
| `CALQUE_SELFHOST_HTTPS_ADDRESS` | `127.0.0.1` | The interface the edge is published on. A real host uses `0.0.0.0`. |
| `CALQUE_SELFHOST_HTTP_PORT` | `8787` | The server's own plaintext listener, published on `127.0.0.1` for the operator's probes. A probe there sends `Host: <public host>`; a browser sent there is refused with `421`. |
| `CALQUE_SELFHOST_METRICS_PORT` | `8788` | The metrics listener, the only one that serves `/metrics`, published on `127.0.0.1` for the operator's scraper. It answers `Host: 127.0.0.1:<this port>`, which is what a scraper connecting to that address sends. |
| `CALQUE_SELFHOST_S3_PORT` | `59001` | The object store, published on `127.0.0.1` for the operator and the conformance run below. |
| `CALQUE_SELFHOST_S3_HOST` | `objectstore` | The host the server reaches the object store at: `CALQUE_S3_ENDPOINT` is `http://<it>:9000`. |
| `CALQUE_SELFHOST_CONFIG` | *required* | Absolute path to the `calque.config.json` that `calque init` produced. Its `sources[].connectionEnv` must be `CALQUE_DATABASE_URL`, which is `init`'s default. |
| `CALQUE_SELFHOST_GRANT_AUDIENCE` | *required* | The audience every dataset grant must carry. |
| `CALQUE_SELFHOST_GRANT_VERIFICATION_KEY` | *required* | The JSON verification-key object. `calque serve` verifies grants and mints none; the application backend that issues them owns the private half. |
| `CALQUE_SELFHOST_ALLOWED_ORIGIN` | `http://127.0.0.1:5173` | The one browser origin allowed to call the server: an HTTPS origin, or a loopback one over HTTP. `calque serve` refuses to start on a plaintext origin elsewhere. |
| `CALQUE_SELFHOST_IMAGE` | `calque:selfhost` | The tag the built image gets. |
| `CALQUE_S3_BUCKET` | `calque-artifacts` | The bucket. |
| `CALQUE_S3_PREFIX` | `calque` | The key prefix inside it, so one bucket can hold more than one deployment. |
| `CALQUE_S3_REGION` | `us-east-1` | The region the SDK signs for. |
| `CALQUE_S3_ACCESS_KEY`, `CALQUE_S3_SECRET_KEY` | *required* | The object-store credentials, also the MinIO root user. Hexadecimal is the safe shape: they are interpolated into the `mc` alias URL. |

The compose file sets three more on the server itself:
`CALQUE_OBJECT_STORE=s3`, `CALQUE_S3_ENDPOINT`, and
`CALQUE_S3_FORCE_PATH_STYLE=1` — a self-hosted store is reached by name, which
is not a virtual-host bucket name.

`CALQUE_S3_SESSION_TOKEN`, which the compose file does not set, is the session
token of temporary credentials, such as an STS role session's. `calque serve`
sends it with `CALQUE_S3_ACCESS_KEY` and `CALQUE_S3_SECRET_KEY` when it is set.
The three are read once at startup and never refreshed, so a process has to be
restarted with new credentials before the old ones expire.

### `calque.config.json`

`calque init` writes this manifest from a `calque inspect` document, and
`calque serve --config` reads it once at startup, so a change takes effect at
the next start. `serve` checks the manifest's shape before it creates any state
or connects to PostgreSQL, then checks it against the live catalog, and refuses
to start if either check fails. A field that admits one value is fixed in the
pre-alpha; `calque init` writes it.

| Field | What it does |
| --- | --- |
| `application.id` | Printed at startup as `application=`. `calque init` takes it from `--application-id`, lowercased, with each run of other characters between letters and digits turned into one `-` and a `t` before a leading digit. It derives the default slot name from it: `calque_<id>_slot`, with each `-` replaced by `_`. |
| `application.version` | `serve` checks only that it is not empty. `calque init` writes `--application-version`, default `0.1.0`. |
| `protocol.manifest`, `protocol.artifact`, `protocol.changeStream`, `protocol.mutation` | Fixed in the pre-alpha at `1`; `calque init` writes them. |
| `sources` | Exactly one. |
| `sources[].id` | Names the source in the state directory and in its rows under PostgreSQL's `calque_control` schema, the authorization fence among them. It, the publication, the slot and the set of tables are fixed for the life of a state directory: `serve` refuses a state directory recorded with different ones. |
| `sources[].connectionEnv` | The environment variable `serve` reads the database URL from; `serve` refuses to start when it is unset. With an existing state directory, `serve` refuses a database other than the one the directory was enrolled on before it writes anything there. The composition requires `CALQUE_DATABASE_URL`, `init`'s default. |
| `sources[].publication` | Created `FOR TABLE` the collections' tables if it does not exist. An existing one must cover exactly those tables, publish insert, update, delete and truncate, be neither `FOR ALL TABLES` nor `publish_via_partition_root`, and carry no row filter or column list on any table. |
| `sources[].slot` | The logical replication slot, created if it does not exist. An existing one must use `pgoutput` on the same database. |
| `scopes` | Exactly one. |
| `scopes[].id` | Printed at startup as `dataset=`. `calque init` writes `--scope-id`, default `all`. |
| `scopes[].source` | The source's `id`. |
| `scopes[].kind` | `source-wide`: every grant sees every row. Or `partitioned`: each grant sees one tenant's rows; see "Tenants". `calque init` writes `source-wide`, or `partitioned` given `--tenant-column`. |
| `scopes[].attribute` | A partitioned scope's grant attribute, which names the grant's tenant. |
| `scopes[].dataVersion` | The authorization generation, an integer from `1`. At startup the generation in force becomes the larger of this value and the source's durable fence, printed as `authorization_generation=`. A grant verifies only if it carries that generation (`calque-dev-grant --generation`); any other gets `401`. Raising the value and restarting refuses every grant minted for an older generation and publishes a new artifact, because the artifact's cache key includes the generation. Lowering it changes nothing, because the fence only moves up. |
| `collections[].id` | The collection's name: its key under `client.db` in the generated bindings, and the key a grant's command capability names (`--command <id>=update`). Unique, and equal to the relation's table name, because the client binds a collection only to the served table of that name: `serve` and `compileConfig` refuse any other id. |
| `collections[].source`, `collections[].scope` | The source's and the scope's `id`. |
| `collections[].kind` | Fixed in the pre-alpha at `table`; `calque init` writes it. |
| `collections[].relation` | The table, as `schema` and `table`. Two collections cannot name one table, nor two tables that share a name in different schemas, because a served collection is named by its table alone. |
| `collections[].columns[]` | Each column's `name`, `postgresType`, the type's `pg_catalog` name such as `int4` or `timestamptz`, and `nullable`, which must match the live table. The list names every column of the table, because the table's `crud` command must write every non-key column and may name only listed ones. |
| `collections[].primaryKey` | The table's primary-key columns, in key order, which must match the live table's. No command can write them. |
| `collections[].partitionColumn` | Under a partitioned scope, the column that holds the tenant. It must be the first column of `primaryKey`, of type `int2`, `int4`, `int8` or `uuid`, the same type in every partitioned collection. |
| `collections[].shared` | Under a partitioned scope, `true` for a table no tenant owns: every tenant receives it whole. A collection under a partitioned scope sets exactly one of `partitionColumn` and `shared`. |
| `collections[].adapter`, `collections[].encoding` | Fixed in the pre-alpha at `direct-columnar` and `calque-columnar`; `calque init` writes them. |
| `commands` | Exactly one `crud` command per collection. `serve` refuses a manifest that leaves an enrolled table without one, or gives a table two. |
| `commands[].id` | Unique. It names the command in `serve`'s errors and in the generated bindings; `calque init` writes `edit-<collection id>`. |
| `commands[].kind` | `crud`, the kind `calque init` writes and `serve` runs. Backend commands are not supported: `serve` refuses a `backend` command (`E_SERVE_COMMAND_INVALID`), and so does `compileConfig`. |
| `commands[].collection` | The collection's `id`. |
| `commands[].operations` | A set drawn from `insert`, `update` and `delete`, without repeats: the most any grant can do on this collection, because a grant's command capability is intersected with it. `calque init` writes every operation the catalog admits for the table. |
| `commands[].writableColumns` | Exactly the table's columns less its primary key; `serve` refuses any other set. It must not be empty when `operations` includes `update`. |

### TLS at the edge

Every browser request — bootstrap, artifact objects, the change stream and
mutations — goes to `https://<CALQUE_SELFHOST_PUBLIC_HOST>:<CALQUE_SELFHOST_HTTPS_PORT>/`,
where the `caddy` service terminates TLS and forwards it to `calque serve` on
the compose network. The compose file passes that address to the server as
`--public-base-url`, so every artifact and change URL a plan carries names the
edge, and the startup line `Calque serve is ready at https://localhost:8443/`
says which address it is. `calque serve` refuses a public base URL that is
plaintext anywhere but `127.0.0.1`, because those URLs carry a capability.

Caddy picks the certificate from the name (`docker/selfhost-caddy/Caddyfile`):

- **`localhost`, the default.** Caddy issues the certificate from its own
  local CA, whose root is on the `caddy-data` volume. A browser talks to the
  edge only once it trusts that root:

  ```sh
  docker compose -f docker-compose.selfhost.yml exec -T caddy \
    cat /data/caddy/pki/authorities/local/root.crt > caddy-root.crt
  curl --cacert caddy-root.crt "https://localhost:${CALQUE_SELFHOST_HTTPS_PORT:-8443}/readyz"
  ```

  Import `caddy-root.crt` as a trusted root into the store the browser reads:
  the operating system's, or on Linux the NSS database under `~/.pki/nssdb`.
  The root lasts as long as the volume: `down -v` deletes it, and the next
  start makes a new one that has to be trusted again.

- **A public domain.** Set `CALQUE_SELFHOST_PUBLIC_HOST=sync.example.com`,
  `CALQUE_SELFHOST_HTTPS_ADDRESS=0.0.0.0` and `CALQUE_SELFHOST_HTTPS_PORT=443`,
  with the name's DNS pointing at the host. Caddy then obtains a certificate
  from a public ACME CA through the TLS-ALPN challenge on 443. Port 80 is not
  published, so neither the HTTP challenge nor an HTTP-to-HTTPS redirect is
  available. No run has exercised this path yet.

Behind the edge the server still checks `Host` byte for byte. A browser sends
the port unless it is 443, so Caddy forwards the bare public host and
`--expected-host` is that name. The server's own loopback listener answers the
same `Host`; a browser pointed at it sends `localhost:8787` and gets `421`.
Neither the edge nor that listener serves `/metrics`: the scrape is on the
metrics listener's own loopback port, `CALQUE_SELFHOST_METRICS_PORT`.
The application's origin, `CALQUE_SELFHOST_ALLOWED_ORIGIN`, has to be HTTPS
unless it is loopback.

### Object store

`CALQUE_OBJECT_STORE` is the whole selector. Unset or `filesystem` keeps the
store under the state directory, which is what every single-host run uses; `s3`
selects the provider. Two refusals keep a wrong configuration from becoming a
quiet fallback: any `CALQUE_S3_*` variable set while the selector does not say
`s3` fails startup with `E_SERVE_OBJECT_STORE_NOT_SELECTED` rather than being
ignored, and the selected store must round-trip a probe object — put, head, get
byte-for-byte, delete — before CDC starts or the listener accepts a connection.
The startup contract then prints `object_store=filesystem` or `object_store=s3`,
which is the line to read when asking which store a running process actually
opened. `backup`, `restore` and `verify-restore` read the same selector, through
the same code, so a deployment cannot start on one store and take a unit as
though it were on the other, and a unit taken from a bucket carries the bucket's
objects — see "Backup and restore against the composition".

### Who may write

The mutation route is mounted only for a principal whose signed dataset grant
names the collection *and* the operation it is asking for (INV-AUTH-002). The
grant carries an optional `commands` list — collections, never tables or
columns, and per collection an explicit set of `insert`/`update`/`delete`;
`spec/dataset-grant-v1.md` is the wire contract. A grant that carries none
authorizes no write at all and gets the same `404` an unmounted route gives. A
request with no well-formed bearer gets it before its body is read. Otherwise
the route drains the body, up to 5 MiB (`MAX_REQUEST_BYTES` in
`server/crates/mutation-http/src/lib.rs`), before it admits the grant, and
consults the body only after admission, so an unadmitted caller gets the same
answer whatever it sent and whatever its size. A read-only deployment does not
have to do anything to stay read-only.

What a grant reaches is the **intersection** of what it signed and what this
deployment's manifest advertises. A manifest that advertises `["update"]` for a
collection mounts a ceiling no grant can raise to a delete, and a collection the
manifest does not advertise is unreachable however a grant spells it. Two
startup lines say which it is:

```text
mutations=mounted-per-collection-command-grant
mutation_commands=projects:update,tasks:insert+update+delete
```

`calque serve` refuses to start on a manifest that advertises no crud command
(`E_SERVE_COMMAND_INVALID`), so a running deployment always prints
`mutations=mounted-per-collection-command-grant`.

The advertised ceiling is enforced in two independent places. It narrows the
capability a presented grant is intersected with, and it narrows the command
`mutation-crud` registers: a collection advertised as `["update"]` registers a
command that admits `update` alone and refuses a `DELETE` on its own, and a
collection the manifest does not advertise has no registered command at all.

**What the 404 hides, and what it does not.** A principal this deployment
authenticates but grants no capability cannot tell the mounted route from an
absent one: every request it can shape — any media type, any body, any size —
answers 404, and so does an anonymous request and any method other than POST.
What is *not* hidden is mount state from a caller presenting a bearer that fails
verification: that answers 401, where a deployment without the route answers
404. Anyone can provoke that with one garbage byte, so do not treat a
deployment's mount state as a secret. The 401 is kept on purpose. The client
classifies it as an authorization failure whose recovery is `reset`, a new mount
from a fresh `auth()` grant, and classifies a 404 as a definitive rejection
(`MUTATION_REJECTED`) that drops the mutation's optimistic change. Answering an
expired or revoked grant with 404 would tell a legitimate writer its mutation
was refused for good, when a renewed grant is all it needs.

Grants are minted by the application backend, never by Calque.
`spec/dataset-grant-v1.md` specifies the token for an issuer in any language,
and `client/examples/grant-issuer/` is a Node issuer with no dependency beyond
`node:crypto`. On a development machine `calque-dev-grant` stands in for one:

```sh
cargo run -q -p dataset-grant-issuer --bin calque-dev-grant -- \
  --key-file dev/grant-key.pk8 --key-id dev-key \
  --audience incident.test --generation 1 --subject alice \
  --lifetime-seconds 900 --command tasks=update
```

There is no wildcard: an operator that means all three writes
`--command tasks=insert+update+delete`. Without `--command` the token is a
read-only grant, which is the default and the safe one. Narrowing a principal's
write authority is a matter of minting narrower grants — they last at most 15
minutes — and widening the deployment's ceiling is a manifest change and a
restart.

### Revoking access

A grant lasts at most 15 minutes, so access the application stops granting ends
within that window on its own. To end it at once on a running `calque serve`,
the application backend retires the authorization generation every outstanding
grant carries and puts a later one in force, in two signed requests.
Both are signed with the Ed25519 key the backend signs grants with, whose
`keyId` must be one of the `--grant-verification-key-env` keys. Neither takes a
bearer: only the key's holder can produce either body. `calque dev` mounts
neither route. The backend can reach both through the TLS edge, which forwards
every path; a request without an `Origin` header passes the origin check.

1. **The fence.** `POST /v1/authorization/fence` with `{"order": "<order>"}`.
   The order is `base64url(payload).base64url(signature)`, unpadded. The
   payload is this JSON object, and a field outside it is refused:

   ```text
   {"version":1,"keyId":"…","audience":"…","revokedGeneration":1,
    "effectiveGeneration":2,"issuedAtUnixSeconds":…,"expiresAtUnixSeconds":…}
   ```

   The signature is Ed25519 over `calque.authorization-fence.v1`, a NUL byte,
   and the payload bytes. `audience` is the `--grant-audience` value,
   `revokedGeneration` is the generation in force (`authorization_generation=`
   at startup), `effectiveGeneration` is larger, and the order is valid from
   `issuedAtUnixSeconds` for at most 300 seconds.

   Calque commits the new generation to `calque_control.authorization_fence`
   before it answers, in the same transaction as the old generation's ceiling
   and barrier drafts, so neither a restart nor a lost state directory reopens
   the old one or loses its drafts. From then on bootstrap and the mutation
   route refuse a grant of the old generation with `401`, its plans' artifacts
   answer `404`, and its plans receive nothing committed after the canonical
   head read under the fence. The answer is `200` with `revokedGeneration`,
   `effectiveGeneration` and `drafts`: one unsigned barrier draft for each
   scope a plan of the old generation was issued for, all at that head. A
   source-wide deployment has one scope, so one draft. An order whose
   `effectiveGeneration` is already in force, the same order posted again
   among them, returns that fence's drafts and moves nothing, before or after
   a restart. Any other order whose `revokedGeneration`
   is not the generation in force answers `409`.

2. **The barriers.** `POST /v1/authorization/barriers` with
   `{"barriers": [{"barrierId", "signingKeyId", "signature"}, …]}`,
   one entry per draft. `signature` is the unpadded base64url Ed25519 signature
   over the barrier preimage in `spec/protocol-notes.md`, built from the draft:
   `barrierCursor`'s UTF-8 bytes are the cursor token,
   `revokedAuthorizationScopeHash` is hexadecimal, and `signingKeyId` names the
   key that signed. Every entry is checked before any is attached, so one that
   does not verify, or names a barrier no fence drafted, refuses the whole batch
   with `401`. The answer is `attached` and `expired`, the entries that came
   after every plan of their scope had expired. A signature is stored with
   its draft, so a restart keeps it. Every plan of a scope receives its
   scope's barrier once its change polls reach the fence's head.

`scripts/serve-revocation-e2e.ts` plays the backend for both steps, in
`signFenceOrder` and `signBarrier`. Until the barriers are attached, the old
generation's clients stay at the fence's head and receive no new rows; if the
barriers never come, their plans expire.

A client created with `grantVerificationKey` verifies each barrier itself, so
the backend must sign barriers with the key that option names. The option is
the `{ keyId, publicKey }` object `--grant-verification-key-env` reads and
`calque-dev-grant` prints as `verificationKeyEnvironment`: `publicKey` is the
32-byte Ed25519 public key in unpadded base64url, 43 characters, and
`createSyncClient` throws on a malformed one. On the barrier the session empties
every subscription, ends each pending mutation ambiguous with a message that it
may have committed, and calls `auth()`. A grant of the new generation rebuilds
the session from the new generation's artifact, which the fence starts
publishing; the status is `preparing` until it is ready. Narrowing a
principal's access is this case, with a grant that carries fewer commands. If
`auth()` throws, if the server refuses what it returned, or if the rebuild
fails for any other reason, the status becomes
`{ state: "locked", reason: "revoked" }` and `reset()` is the only way out.

A client without `grantVerificationKey` does not accept barriers. Its next poll
at the barrier is refused with `401`, which it handles as an expired grant: it
rebuilds through `auth()` without emptying its subscriptions first, and locks
with `reason: "expired"` if the rebuild fails.

Every client of the old generation rebuilds, including one whose rights did not
change. Raising `scopes[].dataVersion` and restarting moves the generation too,
with no running process to deliver barriers.

### Bringing it up

The manifest has to exist before the server starts, and `calque inspect` needs
the tables and the publication to exist before it can produce one, so the
composition comes up in two steps.

```sh
export CALQUE_SELFHOST_POSTGRES_SUPERUSER_PASSWORD=… \
       CALQUE_SELFHOST_DB_PASSWORD=… \
       CALQUE_S3_ACCESS_KEY=… CALQUE_S3_SECRET_KEY=… \
       CALQUE_SELFHOST_GRANT_AUDIENCE=incident.example \
       CALQUE_SELFHOST_GRANT_VERIFICATION_KEY='{"keyId":…,"publicKey":…}' \
       CALQUE_SELFHOST_CONFIG="$PWD/calque.config.json"

docker compose -f docker-compose.selfhost.yml up -d --wait postgres
docker compose -f docker-compose.selfhost.yml up -d objectstore objectstore-init

# The application's tables and its publication, as the least-privilege role.
# CREATE PUBLICATION ... FOR TABLE requires ownership of every table named.
PGPASSWORD="$CALQUE_SELFHOST_DB_PASSWORD" \
docker compose -f docker-compose.selfhost.yml exec -T -e PGPASSWORD postgres \
  psql -h 127.0.0.1 -U calque_sync -d calque -f - <<'SQL'
…your schema, then CREATE PUBLICATION calque_app FOR TABLE …;
SQL

# On the host, through the published port: loopback, so the guard's other
# accepted endpoint. Inside the composition the server uses the socket form.
export CALQUE_DATABASE_URL="postgresql://calque_sync:$CALQUE_SELFHOST_DB_PASSWORD@127.0.0.1:${CALQUE_SELFHOST_POSTGRES_PORT:-55452}/calque"
cargo run -p calque -- inspect --database-url-env CALQUE_DATABASE_URL \
  --source primary --publication calque_app --table public.issues \
  | cargo run -p calque -- init --from-inspection - --application-id incident-ops

docker compose -f docker-compose.selfhost.yml up -d --build calque caddy
curl -fsS -o /dev/null -H "Host: ${CALQUE_SELFHOST_PUBLIC_HOST:-localhost}" \
  --retry 90 --retry-delay 2 --retry-all-errors \
  "http://127.0.0.1:${CALQUE_SELFHOST_HTTP_PORT:-8787}/readyz"
docker compose -f docker-compose.selfhost.yml logs calque   # object_store=s3, ready=true
```

Then fetch the edge's root and check the edge, as "TLS at the edge" shows.

The end-to-end that does all of this from a clean state, drives a
headless-Chromium page through `createSyncClient` against the containerized
server over the HTTPS edge, applies a source change, watches the page
advance, writes a row through the generated facade and waits for its CDC
echo, and tears everything down, is:

```sh
pnpm selfhost:e2e
```

It fails loudly rather than passing on a filesystem fallback: it asserts
`object_store=s3` in the startup contract, that the composition's bucket holds
objects under the configured prefix, and that the container's own
`state/objects` tree holds none. It asserts that every request the browser
sent for the page went to the edge, and that a grant without the write
capability gets a 404. Its own requests to the edge trust only
Caddy's local root and check the name. Its Chromium trusts that root through a
private NSS database on Linux. On macOS, where trusting a root means changing
the login keychain, it verifies the chain the edge presents against the root
and the name, passes that chain's keys to
`--ignore-certificate-errors-spki-list` instead, and reports
`browserTrust: spki-allowlist`. The root's key alone would not match: Chromium
checks the list against the certificates a server sends, and Caddy sends its
leaf and intermediate but not its root.

### Object-store conformance against the deployment

`pnpm verify:s3:minio` proves the S3 adapter against a MinIO container it
starts and throws away. The deployment's own store is a different question —
its bucket, its credentials, its endpoint, its path-style setting — so the two
`#[ignore]`d conformance tests run against it as well. With the composition up
and the same environment exported:

```sh
export CARGO_TARGET_DIR="$(git rev-parse --show-toplevel)/target/cargo"
export CALQUE_SELFHOST_S3_PORT="${CALQUE_SELFHOST_S3_PORT:-59001}"
sh scripts/selfhost-object-store-conformance.sh
```

The two tests are `s3_adapter_passes_shared_and_multipart_conformance` and
`cancelled_s3_multipart_is_unselectable_and_aborted`
(`server/crates/blob-store/tests/conformance.rs`). They must report
`test result: ok. 2 passed; 0 failed; 0 ignored`. The script writes under its
own `selfhost-conformance-…` prefix, never the deployment's, and deletes
everything it wrote — that is the only reason it is safe against a running
deployment. On a composition serving six published objects, the object set
under the deployment's prefix is identical before and after the run, and
`/readyz` still answers `204`.

### Backup and restore against the composition

The cycle runs here, under `CALQUE_OBJECT_STORE=s3`, and a unit taken from this
deployment restores it on its own.

Under `s3` the state directory's `objects` tree is empty by construction — the
objects are in the bucket — so a unit made from that tree would name published
artifacts whose bytes it did not carry, and restoring it would produce a catalog
whose artifacts nothing could fetch. `backup` therefore exports every PUBLISHED
`artifact_objects` row from the configured store into the unit, at the same
source epoch as the control rows and the catalog, verifying each object against
its catalog row as it writes; `restore` proves the whole object half before it
uploads anything, puts every object back, and reads each one back through the
store before the control-row restore commits; and the object gate reads the
store the deployment serves from rather than a tree it does not keep. **A
restore cannot commit into a catalog whose artifacts are unfetchable.**

The unit is therefore the whole backup, and it is portable between the two
shipped stores in either direction — a unit taken on `filesystem` restores into
`s3`, and one taken on `s3` restores into `filesystem`. Two things make that
true. Its object half is written under the same
`objects/<first two hex>/<digest>` names both adapters derive, so the bytes
land where the other store looks for them. And the object half of the oracle is
one definition rather than one per store: the SHA-256 of the objects the unit's
own catalog publishes, in that order, read off the tree or out of the bucket.

A unit's manifest records `formatVersion` 2, and `restore` and
`verify-restore` read only that version. A unit of any other version is refused
before anything is read, uploaded or committed, with a message that says to
take a fresh one with `backup`.

What that definition deliberately does not cover is anything a deployment's
object namespace holds *besides* the published set — a retired artifact's
objects, which nothing deletes yet, and a staging file left by a crashed
upload. A unit taken on `filesystem` still carries those (`backup` copies the
tree whole) and a restore onto `filesystem` still puts them back; they are
payload. They cannot be part of the oracle, because a bucket is shared and has
no tree to walk, and because `artifact_objects` rows exist from `BUILDING`
onwards — requiring every row would refuse a backup of a deployment with a
build in flight. A restore onto `s3` keeps only the published set, which is the
set `serve` can fetch.

The cycle, with the composition up. `backup` refuses a live source lease and an
active logical slot, so the server stops first; the unit goes somewhere that is
not the state volume, because losing that volume is the case being prepared
for. That is the `calque-recovery` volume, mounted at `/recovery`, and from
there a tar stream takes the unit off the host.

It is a volume rather than a host directory because the image runs as its own
unprivileged user, uid 10001. A host directory bind-mounted at `/recovery` keeps
its host owner, which on Linux is not that user, so `backup` fails with
`recovery I/O failed: Permission denied (os error 13)`, as it did on a hosted
Linux CI runner; Docker Desktop on macOS does not enforce the owner and hides
it. A fresh volume takes the owner of the image's `/recovery`,
which is that user, and `tar` runs as that user in the container, so neither
side needs the other's uid. `down -v` deletes this volume with every other, so a
unit that has not been streamed off the host is not a backup.

```sh
docker compose -f docker-compose.selfhost.yml stop calque

# One-off container, the server's own volumes and environment.
docker compose -f docker-compose.selfhost.yml run --rm --no-deps calque \
  backup --output /recovery/unit --state-dir /var/lib/calque/state
# backup source: …   backup epoch: …   backup oracle: <sha256>

# Off the host's volumes. -T keeps the archive on stdout byte for byte.
docker compose -f docker-compose.selfhost.yml run --rm --no-deps -T \
  --entrypoint tar calque -c -C /recovery unit > calque-unit.tar

# After a loss of the state volume and the bucket, with PostgreSQL still
# holding this source's control rows. If the recovery volume went too, stream
# the unit back first. --replace-source-rows is the stopped-source replacement
# path, and its gates are not relaxed.
docker compose -f docker-compose.selfhost.yml run --rm --no-deps -T \
  --entrypoint tar calque -x -C /recovery < calque-unit.tar
docker compose -f docker-compose.selfhost.yml run --rm --no-deps calque \
  restore --backup /recovery/unit --state-dir /var/lib/calque/state \
  --replace-source-rows
docker compose -f docker-compose.selfhost.yml run --rm --no-deps calque \
  verify-restore --backup /recovery/unit --state-dir /var/lib/calque/state
# verified oracle: <the same sha256>

docker compose -f docker-compose.selfhost.yml up -d calque
```

`pnpm selfhost:e2e` runs exactly that, unattended, as the tail of its own end
to end: it takes a unit and streams it onto the host, removes the
`calque-state` and `calque-recovery` volumes and every object under the
bucket's prefix, streams the unit back, restores from it alone, and then
fetches the same artifact object the browser fetched before any of it. It also
asserts the refusal — with the bucket emptied under a restored deployment,
`verify-restore` fails naming the object that is missing:

```text
calque: published object <object id> is absent from the configured object
store; a recovery unit must carry every object its catalog publishes, and a
restore must put every one of them back, or the restored deployment serves
artifacts nothing can fetch
```

What still refuses first, before the connection string is read, before an
output directory exists and before a byte is copied, is a configuration
`serve` itself would not start on: `E_SERVE_OBJECT_STORE_NOT_SELECTED` for
`CALQUE_S3_*` set without the selector, `E_SERVE_OBJECT_STORE_INCOMPLETE` for
a selector with no bucket, `E_SERVE_OBJECT_STORE_UNKNOWN` for a store this
build does not implement. The provider adapter's local scratch goes to
`<state-dir>.recovery-staging`, never into the directory `restore` requires to
be absent, and is removed on every path.

The cycle does not back PostgreSQL up. A unit carries this source's
`calque_control` rows except its authorization fence, and the artifact
objects. The fence stays in PostgreSQL and only ever rises: `restore`
neither carries nor writes it, and no `calque` command lowers or deletes it,
so a restore cannot reopen a fenced generation. The application's own tables
are the database's, and `restore` requires the source's logical replication
slot to still exist and to be continuous with the unit
(`SlotContinuityMismatch` otherwise). A cluster that was lost outright is a
destroy-and-re-enroll, not a restore.

### What the self-hosted composition does not do

Each of these is out of scope for the pre-alpha. Failover, horizontal scaling and
rolling upgrades are distributed-systems commitments the correctness argument
does not currently make, and adding one without making that argument is how a
single canonical cursor stops being single.

- **Failover.** There is one PostgreSQL and one server. Both are single points
  of failure. If the host is lost, the deployment is down until it comes back
  or a recovery unit and a bucket backup are restored somewhere else.
- **Horizontal scaling.** One `calque serve` owns the source's writer fence
  and its logical slot. A second process on the same source is refused by the
  fence, which is the design and not a limitation to work around by disabling
  it. Read scale-out would need a second process that holds no fence, which
  does not exist.
- **Rolling upgrades.** There is no upgrade path across releases yet — see
  "Upgrades" below — and there is no two-version overlap, because two versions
  would be two writers.
- **Connection draining.** `serve` shuts down on SIGTERM by releasing its
  fence lease and slot; in-flight `/v1/changes` responses end when the
  listener does. Clients see a dropped connection and reconnect. Nothing waits
  for them.
- **TLS behind the edge.** Caddy terminates the browser's TLS. The server
  speaks plaintext HTTP and binds `0.0.0.0` inside the composition, and the
  hops behind the edge are plaintext (see "How the server reaches PostgreSQL,
  and why it is not by name"). A proxy other than Caddy has to forward exactly
  the `Host` the server expects: the check stays one byte-equal comparison,
  so anything else is refused on purpose.
- **Secret management.** Credentials come from the environment. Where that
  environment comes from — a secret manager, a file with mode 0600, an
  operator's shell — is the deployment's problem and not this file's. The
  environment is not a secrecy boundary against a host-root or Docker-socket
  reader; it is the boundary against argv.

## Probes

The control endpoints are subject to the same exact loopback `Host` policy as
the application. A probe should send the bound host, for example
`Host: 127.0.0.1:8787`. In the self-hosted composition the server answers the
public host instead, `Host: localhost` by default, on its loopback port.

`/metrics` is not one of them. The scrape has a listener of its own, started
only when `calque serve --metrics-bind-address <address:port>` or
`calque dev --metrics-port <port>` asks for it; without either, nothing serves
metrics, and the startup output says so (`metrics_listener=off` from `serve`).
That listener serves `/metrics` alone and applies the same exact `Host` check
against its own binding: `--metrics-expected-host` when given, which a
wildcard address requires, and its address and port otherwise. Its address is
checked with the HTTP listener's, before CDC starts, and one that shares the
HTTP listener's port is refused.

| Endpoint | Success | Meaning | Failure action |
| --- | --- | --- | --- |
| `GET /healthz` | `204` | The HTTP process can accept and answer a request. This is liveness only. | Restart the process after collecting stderr. |
| `GET /readyz` | `204` | The CDC loop is alive; its most recent decode cycle once held every message that had reached its replication connection, and no transaction it decoded was sent more than one second after its commit; the process still owns an active, unexpired PostgreSQL writer fence; a synchronous no-cursor-advance write commits through the canonical log tables; the SQLite artifact catalog answers the same bounded read bootstrap needs; and the generation being served has a published artifact. | Remove the instance from service immediately. Inspect `calque_errors_total` and `calque_cdc_lag_bytes`; do not override readiness. |
| `GET /metrics`, on the metrics listener | `200` | An OpenMetrics 1.0 scrape ending in `# EOF`. It is not a readiness signal. The HTTP listener answers this path with `404`. | Treat scrape failure as lost observability; use `/readyz` separately. |

`/healthz` deliberately remains live while replication is broken so a
supervisor does not hide a diagnosable failure in a restart loop. `/readyz`
fails closed on a dead or behind CDC session, a stale fence, a failed canonical
write transaction, an unreachable catalog, or a fenced generation whose
artifact failed to publish, which a restart retries. Application `/v1/`
requests are rejected only while the CDC runtime is stopped or re-establishing
its session. A source that is behind still serves them from the canonical
log at the log's own cursor: a refusal would not make the client current,
only add its retry delay to the lag.

#### When PostgreSQL goes away under a running server

The replication runtime reconnects. When the replication, fence-control or
canonical-writer connection is lost, the source is recorded not-current, so
`/readyz` and `/v1/` answer `503` while `/healthz` stays `204`. The runtime logs
`CDC replication session failed; re-establishing it` and retries after a delay
that starts at 100 ms and doubles to a 5 s cap, each delay drawn from the upper
half of its step. Each attempt reacquires the fence lease, reopens replication at
the slot's confirmed position, and replaces the canonical-log connections the
HTTP backend and snapshot pipeline share. The up to 8 read-only connections
change builds and bootstrap planners read the log through are not replaced: one
whose connection closed is dropped, and the next build opens another. Once a
session is re-established it logs
`CDC replication session re-established` with the elapsed time. `/v1/` is
served again after its first successful decode cycle. Transactions committed
while it was disconnected are delivered from the slot, each once: the
canonical append is idempotent on the transaction key. They are sent late, so
`/readyz` returns to `204` after the first cycle that catches up and decodes
none sent more than a second after its commit.

`calque_cdc_reconnects_total` counts re-established sessions. The original
failure and every failed attempt count in `calque_errors_total{class="cdc"}`.

A failure that is not a lost connection still stops the runtime, and
`/readyz` stays `503` until the process is restarted once the cause is fixed.
That covers another owner having taken the fence lease, a slot ahead of the
canonical log, a changed source identity or epoch, and decode or schema errors.
Anything the runtime cannot classify is treated the same way.

A refusal no retry can get past stops the runtime too: PostgreSQL rejecting the
role's login (SQLSTATE class 28, such as a rotated password), a missing
privilege (`42501`), a database that no longer exists (`3D000`), or a role that
lost `REPLICATION`. The runtime logs `CDC runtime stopped without reconnecting`
with a `failure` field that names the refusal by class, such as
`cdc-refused: authentication failed` or
`cdc-refused: replication privilege missing`, and never PostgreSQL's message
text. A connection that fails with no SQLSTATE is still retried, as is one
refused by a server that is shutting down, crashing, starting up or out of
connection slots. Alert on `/readyz`, as above.
`scripts/demo-cdc-restart-e2e.ts` restarts PostgreSQL under an open tab,
and `scripts/selfhost-compose-e2e.ts` recreates the self-host composition's
database container under a running server; both assert that readiness returns
without restarting Calque.

`CDC runtime stopped without reconnecting`, and
`the fenced generation's artifact was not published` from a fence order's
background publication, also carry `state_must_be_destroyed`. When it is `true`
the state directory holds state this build does not migrate, and a restart
meets the same refusal: run
`calque destroy --state-dir <dir> --database-url-env <NAME> --yes`, then
restart (see "Upgrades").

A reconnect that finds the publication no longer defined as
`sources[].publication` requires stops the runtime too, and its `failure` field
names the publication. When the process is then stopped, it prints the refusal
a start that meets the same publication prints, then `Restore that definition,
then rerun serve.` (`dev` under `calque dev`). `calque destroy` does not get
past it: destroy will not drop a publication it cannot verify.

#### When the server is stopped or killed

On SIGTERM the server releases its fence lease and leaves the slot idle before
it exits, so the next start takes the lease at once. The self-host composition
gives it a 30 s stop grace for that. A log or startup line the server cannot
write is dropped and the server keeps serving, even when nothing reads its stderr
or stdout: under a supervisor that means the log collector failed. `calque dev`
instead stops the SIGTERM way when a line of its output finds nothing reading it,
as happens when the launcher that started it dies, and on SIGHUP. A server killed
first, by SIGKILL, the OOM killer or a grace too short, leaves the lease held
until it expires, at most one lease TTL (10 s) after its last renewal. The next
start logs
`another process holds the source lease; waiting for it to be released or to expire`
and takes the lease once it expires. A lease still held when that wait of one
TTL plus a second ends means another running process renews it, and startup
refuses with `source lease is held until database timestamp … ms`. Stop the
other process; do not clear the row. `scripts/selfhost-compose-e2e.ts` stops the
server after it has rejoined a recreated database and asserts the lease was
released, then kills the next one and asserts that its successor becomes ready.

Example:

```sh
curl -i -H 'Host: 127.0.0.1:8787' http://127.0.0.1:8787/readyz
# With `--metrics-bind-address 127.0.0.1:8788`:
curl -fsS http://127.0.0.1:8788/metrics
```

## Metrics

Counters and histograms are process-local and reset on restart. Labels are a
closed enum; no source, table, plan, artifact, client, or error text is used as
a label.

| Metric | Type | Meaning |
| --- | --- | --- |
| `calque_canonical_append_duration_seconds` | histogram | Wall time from immediately before one fenced canonical append commit through its durable return. One commit carries a group of source transactions: every one already received when the group formed, up to 256. Buckets start at 0.5 ms and double 15 times. |
| `calque_canonical_entries_appended_total` | counter | Canonical-log entries appended, one per source transaction. An entry a replay finds already in the log is not counted again. `calque_canonical_append_duration_seconds_count` counts commits instead. |
| `calque_cdc_lag_bytes` | gauge | The replication slot's lag, `pg_current_wal_lsn() - confirmed_flush_lsn`, saturating at zero: the WAL the slot retains that Calque has not acknowledged. Calque samples it at most once a second on a thread and a source connection of its own, so a slow or unreachable source delays this gauge and never the CDC loop. The series is absent until a sample first reads the slot's lag, so a lag that was never measured does not read as 0. A failed sample leaves the last reading in place, and `calque_cdc_lag_sample_timestamp_seconds` says when that reading was taken. This is a raw diagnostic, not a readiness input: the slot's confirmed position advances only when Calque acknowledges a published transaction, so WAL written outside the publication, including Calque's own control-store writes, reads as lag until the next one. Readiness instead reads the latest decode cycle, which tells whether Calque caught up with what reached its connection and how late the source sent each commit. |
| `calque_cdc_lag_sample_timestamp_seconds` | gauge | Unix time in seconds of the sample `calque_cdc_lag_bytes` reports, absent until that series appears. `time() - calque_cdc_lag_sample_timestamp_seconds` is the reading's age. A sample starts a second after the previous one ends, so an age that keeps growing means no sample is reading the lag: the source fails or does not answer within the sampler's 2 s connect or 1 s query timeout, or the slot no longer exists. |
| `calque_artifact_build_duration_seconds` | histogram | Snapshot supervisor execution through successful publication. Buckets start at 10 ms and double 15 times. Failed builds increment the artifact-build error class instead of recording a successful duration. |
| `calque_artifact_publication_duration_seconds` | histogram | Builder-reported finalization plus immutable-object upload and atomic catalog publication. Buckets start at 1 ms and double 15 times. |
| `calque_artifact_recuts_total` | counter | Source-wide artifacts cut again because the log grew past the newest cut ([re-cutting](#re-cutting-the-artifact-and-trimming-the-change-log)). A failed re-cut counts in `calque_errors_total{class="artifact_build"}` instead. |
| `calque_artifact_recut_in_progress` | gauge | 1 from a re-cut's start until its zstd and brotli encodes finish, else 0. |
| `calque_artifact_recut_threshold_bytes` | gauge | The tail, in bytes, at which the next re-cut starts: `--recut-tail-ratio` times the newest artifact's decoded size, or 0 with re-cutting off. |
| `calque_canonical_log_tail_entries` | gauge | Canonical-log entries after the newest artifact's cut: the transactions a cold load replays. |
| `calque_canonical_log_tail_bytes` | gauge | Those entries' bytes, the measure `--recut-tail-ratio` is compared against. |
| `calque_canonical_log_retained_entries` | gauge | Canonical-log entries kept, from the trimmed floor through the head. Far above the tail, it means a plan the process still holds started from an older cut. |
| `calque_cdc_reconnects_total` | counter | Replication sessions re-established after a lost replication, fence-control or canonical-writer connection. A rising count with `/readyz` at `204` is a flapping connection; the source is not current while each reconnect is in progress. |
| `calque_capacity_admission_total{outcome}` | counter | Completed bootstrap admissions, with `outcome="admitted"` or `"rejected"`. A rejection is a bounded memory refusal. |
| `calque_errors_total{class}` | counter | Operator failures classified as `cdc`, `canonical_log`, `artifact_catalog`, `artifact_build`, `capacity`, `authorization`, or `internal`. |

No latency or lag alert threshold has been measured for a production workload.
Alert immediately on `/readyz` failure. Alert on a
nonzero rejection rate or sustained lag relative to a locally measured SLO,
but do not adopt an unmeasured threshold from this document.

## Backup and restore

A recovery unit binds these components with one aggregate SHA-256 oracle:

- every `calque_control` row of the source except its authorization fence:
  the writer-fence lease, canonical-log writer fence, slot enrollment,
  canonical log, retention, and retired-record rows, exported with binary
  `COPY` in one transaction;
- `state.json`, `registry.json`, and signing/cursor secrets;
- a checkpointed and integrity-checked SQLite artifact catalog; and
- every immutable object, with published object sizes and hashes validated.

The runtime must be stopped. `backup` refuses a live source lease or active
logical slot and writes `recovery.json` last. Preserve the directory as one
unit; a directory without that marker is incomplete.

```sh
export CALQUE_DATABASE_URL='postgresql://calque_sync:calque_sync@127.0.0.1:55432/calque_dev'

# Stop the calque process and wait for it to exit first.
cargo run -p calque -- backup \
  --state-dir /srv/calque/source-a \
  --output /srv/calque-backups/source-a-2026-08-31
```

Restore requires an absent target state directory and the original PostgreSQL
logical slot: it must exist, be inactive, and not be ahead of the unit's
durable canonical LSN. Those gates prevent a plausible-looking but
discontinuous realtime history and are never relaxed; once the source has
committed past it the unit is refused.

By default restore is also absent-only for the source's `calque_control` rows.
A stopped source normally still has its rows, so restoring over it takes
`--replace-source-rows`: the flag replaces the same source's rows only after
locking its fence lease (which must be unowned or expired) and finding the
slot inactive, the same stopped-source gates `backup` applies; rows of other
sources are untouched. Do not run `destroy` to clear the rows first: it drops
the logical slot, and a unit without its slot cannot be restored.

```sh
export CALQUE_DATABASE_URL='postgresql://calque_sync:calque_sync@127.0.0.1:55432/calque_dev'

# Move the old state directory aside; restore into its place. The stopped
# source's control rows are still in PostgreSQL, so replace them.
mv /srv/calque/source-a /srv/calque/source-a-retired-2026-08-31
cargo run -p calque -- restore \
  --backup /srv/calque-backups/source-a-2026-08-31 \
  --state-dir /srv/calque/source-a \
  --replace-source-rows

cargo run -p calque -- verify-restore \
  --backup /srv/calque-backups/source-a-2026-08-31 \
  --state-dir /srv/calque/source-a

# Start the restored process. Admit traffic only after readiness returns 204.
curl -i -H 'Host: 127.0.0.1:8787' http://127.0.0.1:8787/readyz
```

A restore that fails after it has started copying leaves the target state
directory partially written. A failure before the commit rolls the PostgreSQL
transaction back, so the control rows stay exactly as they were, though objects
already put to an S3-compatible store stay there. The final verification runs
before the commit too. When it fails, no control row is committed, but the
target state directory is complete and, on an S3-compatible store, the objects
are uploaded and the local `objects` tree is removed. `restore` then prints none
of its `restored` lines and exits 1 with `calque: ` and the refusal on stderr,
which is `calque: recovery logical SHA-256 oracle differs` when a recomputed
hash disagrees. Either way the retry trips the absent-target gate. Remove the
partial directory (`rm -rf /srv/calque/source-a`) and run the same command
again; keep the retired directory until `verify-restore` has agreed, and never
start a runtime on a directory a failed restore left behind.

Both restore commands recompute PostgreSQL logical rows, catalog logical rows,
state files, and the published objects' hashes, read from the object tree or, on
an S3-compatible store, from the bucket. A mismatch is a failed restore. Do not
copy selected files over an existing installation. If PostgreSQL or the logical
slot itself was lost, this recovery unit cannot prove CDC continuity; enroll a
new source epoch and perform a full snapshot instead of bypassing the slot gate.

The library-level PostgreSQL 18 drill is:

```sh
export CALQUE_DATABASE_URL='postgresql://calque_sync:calque_sync@127.0.0.1:55432/calque_dev'
CARGO_TARGET_DIR="$PWD/target/cargo" \
  cargo test -p calque-engine --lib \
  recovery::tests::live_backup_restore_round_trip_matches_logical_oracle \
  -- --ignored --nocapture
```

It creates a real publication and logical slot, ingests and publishes an
artifact, stops CDC, backs up, refuses the absent-only restore over the
source's own rows and the replacing restore under a provisional lease,
restores with `--replace-source-rows` and matches the oracle on restore and
explicit re-verification, then removes the fixture's control rows and restores
absent-only.

## Upgrades

There is no upgrade path across releases yet. A build reads only state in the
format it writes and migrates none, so `calque dev` and `calque serve` refuse a
state directory another build wrote, and name the command that clears it:

- an artifact catalog at an earlier schema version:

  ```text
  <dir> holds an artifact catalog an earlier release of calque wrote (schema
  version <n>), and calque does not migrate it. Run calque destroy --state-dir
  <dir> --database-url-env <NAME> --yes, then rerun <command> to prepare the
  working set anew.
  ```

  A catalog at a schema version this build does not know is refused with
  `artifact catalog schema version <n> is unsupported`. Neither refusal changes
  the file.

- a canonical log holding an entry an earlier release wrote, which carries the
  `routingIdentity` field this build no longer writes:

  ```text
  <dir> serves a canonical log an earlier release of calque wrote, and calque
  does not migrate it. Run calque destroy --state-dir <dir> --database-url-env
  <NAME> --yes, then rerun <command> to prepare the working set anew.
  ```

- a published artifact whose stored manifest this build's contract refuses,
  because another build wrote it or it is damaged;
- an interrupted build of an artifact this build does not make.

The last two end with the same remedy (`pnpm incidents --fresh` does both steps
for the incidents demo). `<command>` is `dev` or `serve`, whichever refused, and
`<NAME>` is the variable it read the database URL from: `calque dev`'s
`--database-url-env`, `CALQUE_DATABASE_URL` by default, or the
`sources[].connectionEnv` of `calque serve`'s manifest. `destroy` reads the
database URL from the variable its own `--database-url-env` names, so the
printed line connects to the source the refusing command read.

Recovery units follow the same rule: `restore` and `verify-restore` read only
the unit format this build writes (see "Backup and restore against the
composition").

## Tenants

Under a partitioned scope one process serves every tenant through one change
log, and each tenant has a working set of its own: the rows of every
partitioned table whose partition column equals the tenant, and every shared
table whole. A grant names its tenant in the scope's attribute; a grant
without it, or with a value of another type, is answered `403`. A write is
checked against the grant's own tenant, so a page cannot write another
tenant's row. Each tenant is an authorization scope of its own, so the
clients of one tenant share the frames of each transaction, and two tenants
never do.

A tenant is prepared on its first bootstrap. That request is held open for
up to `--tenant-hold-open-seconds`, 25 by default, below the 30 s at which
common proxies close an idle request, and a second request for the same
tenant waits on the same preparation. A preparation that takes longer is
answered `503` `ARTIFACT_PREPARING` with `Retry-After: 1`; the client reports
the `preparing` status and asks again, for up to two minutes. A tenant with
no rows gets an empty working set. A tenant larger than `--max-tenant-bytes`
decoded, 1,280 MiB by default, is refused with `422` `CAPACITY_EXCEEDED` and
is not built in part. A bootstrap that waits on its tenant's preparation
holds its place under `--max-inflight-requests`. A
preparation reads the tenant's rows of each partitioned table along the
primary key's index: its copy runs with sequential scans, bitmap scans, sorts
and JIT off, so PostgreSQL cannot read a large shared table whole and sort
the tenant's rows before the first leaves it.

At most `--max-tenant-builds` preparations run at once, 4 by default. Each
holds a replication slot and a WAL sender until it reaches its cut, so
`serve` refuses to start unless PostgreSQL's `max_replication_slots` and
`max_wal_senders` leave that many plus one free, the one for change capture.
A request a user waits on is prepared first, then a warm order from the
application, then a refresh. While requests wait, a warm order or a refresh
queued for `--recut-min-interval` or longer takes every other start, so a
full queue of requests delays it but cannot hold it back for good, and a
warm order for every tenant holds back a request by one start at most.
Compressing an artifact of `--tenant-compress-above-bytes` or more, 16 MiB
by default (`off` for none), runs in the background only when no preparation
is queued.

Preparations run at the priority live delivery runs at, so while they run,
the tenants already loaded get their changes later. In one directional run
with four preparing at once and a warm order for every tenant, a loaded
tenant's commit to paint had a p95 of 157 ms, against 46 ms with nothing
preparing.

A tenant opened within `--tenant-warm-days`, 7 by default, or pinned, stays
warm: its artifact is kept, and it is cut again by the re-cut's rule. A
refresh is queued once the bytes of the operations routed to it since its
cut, every operation on a shared table among them, reach
`--recut-tail-ratio` times its artifact's decoded size, and
`--recut-min-interval` seconds have passed since its cut and since its last
refresh was queued. With the ratio at 0, which turns re-cutting off, a warm tenant is not
cut again and its tail grows with every write. Once a minute, and at start,
every other tenant is evicted: its artifact is deleted once no plan holds
it, and its next open waits for a preparation. A tenant that is neither warm
nor pinned, and that no request waits on, is then forgotten. The tenant
directory, `tenants.sqlite3` beside the artifact catalog, records each
tenant's opens, pin, cut, tail and last change, so a restart serves every
warm tenant from its newest valid cut and keeps counting its tail.

A warm tenant's page goes live from its cut, with nothing to catch up, while
nothing it reads has committed since the cut: no write to its rows, no row
moved into or out of it, and no write to a shared table. Other tenants'
writes and preparations move the log's head, but not the tenant's last
change. Once something it reads has changed, its plan ends at the log's
head and the page catches up to it before it is ready.

The change log is trimmed under a partitioned scope too, behind the oldest
of every tenant's cut, the start of each running preparation, the last entry
the tenant follower has read, the catch-up start of each plan the process
still holds, and the last entry the change tail has read. An evicted tenant
stops holding the log back; a plan granted from its cut keeps doing so until
the plan is dropped. No source-wide artifact is cut again. The tail gauges
read the warm tenant whose tail is the largest share of its artifact:
`calque_canonical_log_tail_entries` counts the log entries after its cut,
`calque_canonical_log_tail_bytes` the bytes routed to it since then, and
`calque_artifact_recut_threshold_bytes` the bytes at which it is refreshed.

A row whose partition column changes leaves one tenant and joins another.
Neither tenant receives it as a patch: both tenants' open pages are told to
take a fresh base, both cuts are retired, and each tenant is prepared again
before its next plan. A move that reaches a preparation before its cut ends
it, and the tenant is prepared again, five times at most; past that it is
prepared again only when it is next requested. A truncate reaches every
tenant. A revocation fence retires every tenant's artifact and prepares the
warm ones again under the new generation.

The application's backend can warm a tenant before anyone opens it, pin or
unpin it, evict it at once, and read its state, by posting
`{"order": "<token>"}` to `POST /v1/tenants`. The order is signed with the
grant key under a domain of its own, so a browser's grant is refused there
with `401`
([`spec/dataset-grant-v1.md`](../../spec/dataset-grant-v1.md#tenant-orders)).
The answer is the tenant's state, `cold`, `preparing` or `warm`, whether it is
pinned, and once it has an artifact, the age of its cut and its size. Each
order carries an id, which the process records in the tenant directory
before it acts and keeps until five minutes after the order expires, so a
second presentation of the same order, before or after a restart, is
answered `409` `REPLAYED`. An evict drops a preparation in progress, and a
request waiting on it is answered `ARTIFACT_PREPARING` and asks again; a plan
granted before the evict keeps its artifact until the plan expires. The
startup contract prints `scope_mode=partitioned`, `partition_attribute=` and
`max_tenant_builds=`.

## How many clients one process serves

Each bootstrap answers with a plan token the deployment signs with the plan
key in `secrets.json`. The token carries what every later change request and
write is checked against: the source and its epoch, the authorization
generation and scope, the tenant, the expiry, the catch-up's cursors, a digest
of the plan's tables and their starts, and one of the client instance and
device. No process holds a
plan, so a restart on the same state directory keeps every one: an open page
keeps its artifact and receives what was committed while the process was
down. A plan expires an hour after the bootstrap, or one second before the
grant that admitted it expires if that is sooner. Until then its retention
lease keeps the change log from its catch-up's start and its pin keeps its
artifact, so a closed tab or a reloaded page holds both until its plan
expires.

Each process keeps the scope it rebuilt for each plan it verified lately, at
most `--max-active-plans`, 4,096 by default, on `calque dev` and
`calque serve`. Nothing is refused at that bound: every request verifies its
token again, and a plan whose entry was dropped is rebuilt from it.

Two bootstraps are planned at a time and the others wait for a turn, so
clients that arrive together are planned rather than refused.

Each plan parks at most one live change request. A client keeps one in flight,
so a second request on the same plan means the first was abandoned: the newer
request wakes the older one, which answers at once with what it would have
answered at the end of its wait.

Each connected client holds one open change stream, and a process holds at
most `--max-open-streams` of them, 4,096 by default. A stream beyond that is
answered `503` `INTERNAL_RETRYABLE` with a retry after one second, before
anything is built. Each stream and each request in flight holds a socket, so
at startup the process raises its soft open-file limit to the hard one. If
that still cannot hold both bounds and 256 more descriptors, both bounds are
cut, in proportion, to what it can, and a warning names the limit, the cut
bounds and the shortfall; raise the hard limit (`ulimit -Hn`) to keep them.

A process answers at most `--max-inflight-requests` requests at once, 4,096
by default. A request beyond that is answered `503` with `Retry-After: 1`. A
change request holds its place only until its response opens, so open
streams do not count against it. A bootstrap holds its place while it waits
for a planner's turn, and the client reports a refused bootstrap instead of
retrying it, so the default matches `--max-open-streams`: as many clients as
one process streams to can bootstrap at once. The work behind those
requests stays bounded by the two planners, the eight log connections of the
change builds and the change tail.

The process reads each committed transaction from the canonical log once,
into a change tail held in memory: the newest records and their encodings,
at most `--change-tail-bytes`, 256 MiB by default. Every change request whose
cursor the tail holds is built from it. A transaction's frames are encoded
once for each authorization scope and set of tables, and every client of that
scope is sent the same bytes; only a batch's first and last frames, which
carry the plan, the cursors and the client's acknowledgements, are built for
each client. The grant, its expiry, a fenced plan's ceiling and barrier, and
the acknowledgements are resolved for each client before each batch, as
before. A client further behind than the tail holds, or any client while the
tail has not read the log for two seconds, reads the log through the pool of
read-only connections. A follower read that hangs rather than fails wakes the
waiting streams once those two seconds pass, so they read the log instead.
`--change-tail-bytes 0` turns the tail off. A frame a slow client has not yet
been sent counts against the tail's bound until it is, even after the tail
drops the record: the tail then keeps fewer records.

A batch built from the log is the client's own bytes, so the batches built
from the log that the process holds, from their build until they are
written, are bounded at 256 MiB together and at 16 MiB for each plan, the
largest batch a request may ask for. A build takes room in both for the most
its request may make before it reads and gives back what it did not make; a
build that finds no room waits for it after giving back the fence lock, so a
slow client's unwritten batches delay its own plan's builds first, other
builds from the log only once the budget is full, and never a fence. A
stream stops waiting for room, or for a build in progress, as soon as its
client leaves. A write that waits 30 s without the client taking any of it
closes the connection, which gives back what its response held.

On these defaults one `calque dev` process kept 3,000 clients live at 10
transactions a second, with no request refused, and 100 at 220 a second
(directional). With 100 the page's commit to stable paint did not move, a median of
15.4 ms against 15.0 ms with no other client. With 3,000 its median rose from
15.6 to 40.3 ms and its p95 was 69.8 ms. Each client costs calque about 45 to
53 µs of CPU per transaction, and a commit reached the first and last of eight
observing clients 21 ms apart. The 3,000 clients, arriving together,
bootstrapped in 32.9 s through the two planners. On the same machine a
program with no calque in it that writes one message to each of 3,000
loopback connections delays one more message by 25.2 ms, about as much as
the page's rise.

## Re-cutting the artifact and trimming the change log

A cold load downloads the newest artifact and then replays every transaction
the canonical log holds after that artifact's cut. Once a second, `calque dev`
and `calque serve` measure those entries' bytes. With `--recut-tail-ratio`
above 0, 1/32 by default, once they reach that ratio times the newest
artifact's decoded size, the process cuts the source-wide artifact again in
the background, through the build a fenced generation's publication runs,
provided all of these hold:

- `--recut-min-interval` seconds, 60 by default, have passed since the newest
  cut and since the last re-cut started;
- the generation being served has a published artifact.

A re-cut does not wait for the zstd and brotli encodes of the cut before it,
and cancels them when it starts; they start again if the re-cut fails. Under
writes that keep a re-cut due, an encode longer than the interval never
finishes, and bootstraps are planned with the representation the cut was
published with.

A re-cut's build and its encodes run in the scheduler's background class: on
macOS the background QoS class, which runs them on the slower cores with what
other work leaves idle, and on Linux nice 19. Such a build encodes its zstd
object on its own thread rather than on libzstd's twelve workers, which macOS
would start at normal priority. A preparation and a fenced generation's
publication build and encode at normal priority, and a fenced publication
cancels a re-cut in its way.

`--recut-tail-ratio` defaults to 1/32, the ratio both directional benchmark
runs used, and 0 turns re-cutting off. Without re-cuts a cold load replays
every transaction since the source was prepared or last revoked, and the
cost grows with the writes: at 100k rows the median cold load after a
10,000-transaction tail, about 45 s of the benchmark's own write load, was
1,751.1 ms in the first run and 1,776.5 ms in the second, against 747.0 ms
and 760.7 ms with no tail. A re-cut at 1/32 brought them to 750.6 ms and
755.1 ms.

Live updates pay for it. At normal priority each re-cut took about 41 s
of calque's CPU, most of it in the zstd-19 and brotli encodes, and with
re-cuts back to back the page's median commit to stable paint rose from
15.8 ms to 20.3 ms. In the background class, with a re-cut every 30 s, it
rose from 21.1 ms to 25.5 ms, with a p95 of 57.8 ms and a p99 of 72.2 ms.
Neither run met its rule's allowance for that rise, so the experiments did
not admit the re-cut. It has been on by default since 2026-10-31 anyway, so
that a default deployment's cold loads stop growing with its writes. The
second run's rule would have set the ratio to 1/64 and the
interval to 30 s. The default stays at the 1/32 and 60 s the runs measured:
no run used 1/64, which re-cuts twice as often under the same writes, and the
slowdown above comes from re-cuts. A deployment whose live updates matter
more than its cold loads passes `--recut-tail-ratio 0`.

A bootstrap planned after the new artifact is published downloads it and
catches up from its cut. A plan already granted keeps its artifact and its
catch-up start. Until the new cut's brotli representation is published,
bootstraps are planned with its zstd one. A re-cut places a barrier in the
log as a preparation does, so every connected page receives one batch with no
rows in it.

The same pass trims the log, whatever the ratio. It keeps every entry after
the oldest of these: the newest artifact's cut, the catch-up start of each
plan the process still holds, and the last entry the change tail has read.
Under a partitioned scope each tenant's cut stands in for the newest
artifact's ([Tenants](#tenants)). With re-cutting off, the newest cut is the preparation's or the last
revocation's, so the log keeps every entry since then. Every other published
artifact that no plan pins is retired, and its objects are deleted. The pins
and log leases of plans the process has dropped are released, except while a
bootstrap is being planned. The log is trimmed 1,024 entries per
transaction, each holding back appends for as long as it runs, and after
each one the change tail drops the trimmed entries and their encodings. A
trimmed entry leaves one row in
`calque_control.canonical_log_retired_record`, its sequence, transaction
identity and digest, so that CDC still recognizes a transaction it replays
after a reconnect. Nothing removes those rows before `calque destroy`.

A change request whose cursor is behind the trimmed range is answered `410`
with a `CURSOR_BEFORE_RETENTION` stream error and `resnapshot_required` set,
and the client bootstraps again. Calque never sends part of a batch the log no
longer holds.

## What a restart does to connected clients

A plan is a token the deployment signs with a key in the state directory, so
a process restarted on the same state directory accepts every plan the
previous one granted. An open page reconnects, keeps its artifact, and
receives the changes committed while the process was down, and an artifact
download the restart cut off is retried at the same URL while its plan is
valid. A process started on a new state directory has lost that key: every
plan is refused with `401`, and its page bootstraps again. A stale artifact is
never advertised as realtime.
