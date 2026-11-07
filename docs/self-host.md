# Self-host with signed grants


`docker-compose.selfhost.yml` runs PostgreSQL 18, an S3-compatible object
store (MinIO), `calque serve` built from this repository's `Dockerfile`, and
Caddy terminating TLS in front of it. The browser reaches Calque only through
Caddy, over HTTPS.

## What you need

You need what [Try it locally](../README.md#try-it-locally) lists, plus
`openssl` and `curl`, and on Linux NSS's `certutil` (`libnss3-tools` on Debian
and Ubuntu). Run everything from the repository root.

The composition publishes PostgreSQL on port 55452, MinIO on 59001, the
server's plaintext port on 8787, its metrics port on 8788 and the HTTPS edge on
8443, all on `127.0.0.1`, and the example page is served on 5173. All six must
be free. A running `pnpm demo` takes 8787 and 5173 when they are free, so stop
it first.

## A grant

`calque serve` verifies signed dataset grants and mints none: your
application's backend issues them, following
[`spec/dataset-grant-v1.md`](../spec/dataset-grant-v1.md). A Node backend can
start from [`client/examples/grant-issuer/`](../client/examples/grant-issuer/),
which uses `node:crypto` alone. `calque-dev-grant` stands in for that backend
here. It creates the signing key on first use, prints a grant, and prints the
verification key the server needs:

```sh
cargo run -q -p dataset-grant-issuer --bin calque-dev-grant -- \
  --key-file grant-key.pk8 --key-id quickstart --audience quickstart.example \
  --generation 1 --subject me --lifetime-seconds 900 --command tasks=update \
  > grant.json
```

## Settings

Every setting is an environment variable, and the credentials are yours to
choose; hexadecimal is the safe shape, because some are interpolated into URLs.
Compose needs all of them on every command that reads
`docker-compose.selfhost.yml`, so export them first:

```sh
export CALQUE_SELFHOST_POSTGRES_SUPERUSER_PASSWORD="$(openssl rand -hex 16)" \
       CALQUE_SELFHOST_DB_PASSWORD="$(openssl rand -hex 16)" \
       CALQUE_S3_ACCESS_KEY="ak$(openssl rand -hex 10)" \
       CALQUE_S3_SECRET_KEY="$(openssl rand -hex 24)" \
       CALQUE_SELFHOST_GRANT_AUDIENCE=quickstart.example \
       CALQUE_SELFHOST_GRANT_VERIFICATION_KEY="$(node -p 'JSON.stringify(require("./grant.json").verificationKeyEnvironment)')" \
       CALQUE_SELFHOST_CONFIG="$PWD/calque.config.json"
```

## The database

Start the database and the object store, then create your tables and their
publication as the least-privilege role the composition creates:

```sh
docker compose -f docker-compose.selfhost.yml up -d --wait postgres
docker compose -f docker-compose.selfhost.yml up -d objectstore objectstore-init

PGPASSWORD="$CALQUE_SELFHOST_DB_PASSWORD" \
docker compose -f docker-compose.selfhost.yml exec -T -e PGPASSWORD postgres \
  psql -v ON_ERROR_STOP=1 -h 127.0.0.1 -U calque_sync -d calque <<'SQL'
CREATE TABLE projects (id bigint PRIMARY KEY, name text NOT NULL, status text NOT NULL);
CREATE TABLE tasks (
  id bigint PRIMARY KEY,
  project_id bigint NOT NULL REFERENCES projects(id),
  title text NOT NULL,
  done boolean NOT NULL DEFAULT false
);
INSERT INTO projects VALUES (1, 'Quickstart', 'active');
INSERT INTO tasks VALUES (1, 1, 'Open the page', false);
CREATE PUBLICATION calque_app FOR TABLE projects, tasks;
SQL
```

## The manifest

Describe the source to Calque. `calque inspect` reads the schema through the
database's published loopback port, and `calque init` turns the inspection into
the manifest the server mounts:

```sh
export CALQUE_DATABASE_URL="postgresql://calque_sync:$CALQUE_SELFHOST_DB_PASSWORD@127.0.0.1:${CALQUE_SELFHOST_POSTGRES_PORT:-55452}/calque"
cargo run -q -p calque -- inspect --database-url-env CALQUE_DATABASE_URL \
  --source primary --publication calque_app \
  --table public.projects --table public.tasks > calque.inspection.json
cargo run -q -p calque -- init --from-inspection calque.inspection.json \
  --application-id quickstart
```

## The server and its HTTPS edge

Start the server and the TLS edge, and wait until the server is ready:

```sh
docker compose -f docker-compose.selfhost.yml up -d --build calque caddy
curl -fsS -o /dev/null -H "Host: localhost" --retry 90 --retry-delay 2 \
  --retry-all-errors "http://127.0.0.1:${CALQUE_SELFHOST_HTTP_PORT:-8787}/readyz"
```

The first `up --build` downloads the Rust image and compiles `calque` in
release mode inside Docker, so give Docker at least 4 GB of memory and room for
several gigabytes of build cache. Later builds reuse the cache. The two
`cargo run` steps above also compile on the host the first time.

`curl` gives up after 90 retries 2 seconds apart. If it does,
`docker compose -f docker-compose.selfhost.yml logs calque` shows why the
server is not ready.

The plaintext port listens on loopback for the operator's probes and refuses
a browser with 421; `/metrics` is served only on the metrics port,
`curl http://127.0.0.1:8788/metrics`. The browser uses
`https://localhost:8443/`, whose certificate comes from Caddy's own local CA.
Take that CA's root out of the container and check the edge against it:

```sh
docker compose -f docker-compose.selfhost.yml exec -T caddy \
  cat /data/caddy/pki/authorities/local/root.crt > caddy-root.crt
curl --cacert caddy-root.crt -i "https://localhost:${CALQUE_SELFHOST_HTTPS_PORT:-8443}/readyz"
```

A browser refuses the edge until it trusts `caddy-root.crt`. On macOS, add it
to your login keychain as a trusted root, which asks for your password:
`security add-trusted-cert -r trustRoot -k ~/Library/Keychains/login.keychain-db caddy-root.crt`.
On Linux, add it to the NSS database Chrome reads, which Chrome creates on its
first start:
`certutil -d sql:$HOME/.pki/nssdb -A -t "C,," -n calque-quickstart -i caddy-root.crt`.

## The page

The page's origin must be the one allowed origin,
`CALQUE_SELFHOST_ALLOWED_ORIGIN` (`http://127.0.0.1:5173` unless you set it; an
HTTP origin is accepted only on `localhost` or a loopback IP address, and any
other host needs HTTPS). The example page in `client/examples/quickstart` is
served from that origin. Build the packages and render the typed bindings for
your tables into it:

```sh
pnpm install && pnpm -r build
pnpm exec calque-bindings --out client/examples/quickstart/calque.generated.ts
```

`calque-bindings` reads the `calque.config.json` that `calque init` wrote and
renders the bindings, and the `calque.start.generated.ts` beside them, over
the example's committed ones. For this walk's two tables the render is
byte-identical to them; for other tables,
`git checkout -- client/examples/quickstart` restores the committed files.

A grant lasts at most 15 minutes, so the one minted at the start may have
expired by now. Mint a fresh one before the page loads. The command reuses
`grant-key.pk8`, so the verification key the server was started with still
verifies it:

```sh
cargo run -q -p dataset-grant-issuer --bin calque-dev-grant -- \
  --key-file grant-key.pk8 --key-id quickstart --audience quickstart.example \
  --generation 1 --subject me --lifetime-seconds 900 --command tasks=update \
  > grant.json
```

Start the page. Its dev server serves the repository root's `grant.json` at
`/grant.json` and reads it again on every request, so a grant minted later
reaches the page without a restart. The page's endpoint is fixed at
`https://localhost:8443/`, the edge's address with the default
`CALQUE_SELFHOST_HTTPS_PORT`:

```sh
pnpm --filter @calque/quickstart dev
```

Once your browser trusts `caddy-root.crt`, open `http://127.0.0.1:5173` in
Chrome. The page shows the client's status and subscribes one live query to
every task, redrawing the list whenever its rows change, so a task written in
PostgreSQL appears without a reload. Create the client the way the page's
`main.ts` does, with your backend's grant in `auth`:

```ts
import { createSyncClient } from "@calque/client";
import { schema } from "./calque.generated.js";

const client = createSyncClient({
  endpoint: "https://localhost:8443/",
  // The dev server serves the grant `calque-dev-grant` wrote. In production,
  // your backend issues it.
  auth: async () => {
    const response = await fetch("/grant.json", { cache: "no-store" });
    if (!response.ok) throw new Error(await response.text());
    const { token } = (await response.json()) as { readonly token: string };
    return { bearerToken: token };
  },
  schema,
});
```

A production page also passes `grantVerificationKey`: the public half of the
key its backend signs grants with, which is grant.json's
`verificationKeyEnvironment` here. With it, a revoked user's rows leave the
page as soon as the revocation arrives; without it, they stay until the client
rebuilds. [Revoking access](design/operations.md#revoking-access) has both
cases.

## Tearing it down

`docker compose -p calque-selfhost down -v` removes the containers and every
volume, including Caddy's CA; the next start makes a new root. It names the
composition's project rather than its file, so it works in a shell without the
exports above. Stop trusting the old root before the next walk overwrites
`caddy-root.crt`: on macOS, `security remove-trusted-cert caddy-root.crt`,
which withdraws the trust and leaves the certificate in the login keychain,
untrusted; on Linux, `certutil -d sql:$HOME/.pki/nssdb -D -n calque-quickstart`.

## On a public server

On a server with a public name, set `CALQUE_SELFHOST_PUBLIC_HOST` to it and
publish the edge with `CALQUE_SELFHOST_HTTPS_ADDRESS=0.0.0.0
CALQUE_SELFHOST_HTTPS_PORT=443`. Caddy then requests a certificate from a
public ACME CA; that path has not been run yet.
[`docs/design/operations.md`](design/operations.md) has every variable,
the TLS edge, backup and restore under the S3 store, and what the composition
does not provide. `pnpm selfhost:e2e` brings the same composition up
unattended from a clean state and drives a page of its own in headless Chromium
through the edge.
