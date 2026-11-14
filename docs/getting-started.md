# Getting started

This page takes an existing PostgreSQL 18 database and an existing Vite app to
a page that shows your own tables and follows a change made in `psql`. It runs
Calque's development server, `calque dev`, which serves one disposable
database on your machine without signed grants. [Production](#production)
says what changes for a deployment.

## What you need

- PostgreSQL 18 that you can reach as a superuser once, to create a role and a
  publication, and `psql` to run SQL. The examples use a database named `app`
  on `127.0.0.1:5432` with a superuser named `postgres`.
- A table to serve. `calque dev`, which this guide runs, serves every column
  of it. `calque serve` serves only the columns a collection names
  ([Publishing only some columns](postgresql.md#publishing-only-some-columns)).
  A table needs a primary key and supported column types
  ([What Calque needs from PostgreSQL](postgresql.md)).
- A Vite app, with Vite 8.1.4 or newer, run in Chrome or another
  Chromium-based browser. React pages also need React 19 or newer.
- The `calque` binary, or Docker. [Install](../README.md#install) has both.

## 1. Check PostgreSQL

`calque` needs `wal_level=logical`, and one free replication slot and one free
WAL sender for each server it runs. Read what your server has:

```sh
export ADMIN_URL='postgresql://postgres:postgres@127.0.0.1:5432/app'
psql "$ADMIN_URL" -x -c "SELECT current_setting('server_version') AS version, current_setting('wal_level') AS wal_level, current_setting('max_replication_slots')::int - (SELECT count(*) FROM pg_replication_slots) AS free_slots, current_setting('max_wal_senders')::int - (SELECT count(*) FROM pg_stat_replication) AS free_wal_senders"
```

`wal_level` has to read `logical`, and both free counts at least 1. If not, set
them. A stock PostgreSQL starts with `wal_level=replica`:

```sh
psql "$ADMIN_URL" -c "ALTER SYSTEM SET wal_level = 'logical'"
```

`ALTER SYSTEM SET max_replication_slots = 20` and `ALTER SYSTEM SET
max_wal_senders = 20` raise the other two. None of the three takes effect
until PostgreSQL restarts: `brew services restart postgresql@18`,
`sudo systemctl restart postgresql`, or `docker restart <container>`, as you
run it. A managed database turns logical replication on in its console and
needs its direct endpoint, not a transaction pooler.

## 2. Create the role

Calque connects as a role that is not a superuser, has `REPLICATION`, can
`CREATE` in the database, and owns the tables it serves. As a superuser, in
your database:

```sh
psql "$ADMIN_URL" -v ON_ERROR_STOP=1 <<'SQL'
CREATE ROLE calque_sync LOGIN PASSWORD 'calque_sync' REPLICATION;
GRANT CREATE ON DATABASE app TO calque_sync;
GRANT CREATE, USAGE ON SCHEMA public TO calque_sync;
SQL
```

Use your own password. On Amazon RDS and Aurora, grant `rds_replication`
instead of writing `REPLICATION`.

The role has to own each table because the server runs
`CREATE PUBLICATION ... FOR TABLE`, which PostgreSQL allows only to a table's
owner. For an existing table, find its owner and make `calque_sync` a member
of that role, which leaves the table with its owner and your application's
migrations unchanged:

```sh
psql "$ADMIN_URL" -c "SELECT tablename, tableowner FROM pg_tables WHERE schemaname = 'public'"
psql "$ADMIN_URL" -c "GRANT app_owner TO calque_sync"
```

Here `tasks` belongs to `app_owner`; write your tables' owner. The other way is
`ALTER TABLE tasks OWNER TO calque_sync`, which hands the table over: its
former owner then has to be a member of `calque_sync` to alter it.

## 3. Publish the tables

`calque inspect` reads the tables through a PostgreSQL publication that you
create. Calque connects through `CALQUE_DATABASE_URL`, which every command here
reads. Create the publication as `calque_sync`:

```sh
export CALQUE_DATABASE_URL='postgresql://calque_sync:calque_sync@127.0.0.1:5432/app'
psql "$CALQUE_DATABASE_URL" -c "CREATE PUBLICATION calque_app FOR TABLE public.tasks"
```

Name every table you want on the page after `FOR TABLE`, separated by commas.
Write `127.0.0.1` for a database on this machine: any host name, `localhost`
included, needs TLS that verifies the server
([the database URL](postgresql.md#the-database-url)).

A collection takes its table's name, which has to match
`^[a-z][a-z0-9]*(?:[._-][a-z0-9]+)*$`: lowercase ASCII letters and digits,
starting with a letter, with a single `.`, `_` or `-` between them. `tasks` and
`issue_notes` pass. `User`, `2fa` and `order__items` do not, and `calque init`
refuses them in step 4. Rename such a table, or enroll a projection table
instead: a plain table with a name that passes, holding only the columns a
page may read, which your application or a trigger keeps in step with the
original.

```sql
ALTER TABLE "User" RENAME TO users;
```

## 4. Describe the tables

Run these from your application's root. `calque inspect` checks that every
table can be served and prints its schema, and `calque init` turns that into
`calque.config.json`, one collection per table:

```sh
calque inspect --source primary --publication calque_app --table public.tasks > calque.inspection.json
calque init --from-inspection calque.inspection.json --application-id my-app
```

`inspect` names the table, column and reason for every refusal, with a code
that [the refusal table](postgresql.md#when-calque-refuses-a-table-or-a-setting)
explains. After you change the tables, run both commands again, with
`calque init --force`.

## 5. Install the packages

```sh
npm install @calque/client
npm install -D @calque/config
```

`@calque/client` is the browser client and brings the Vite plugin.
`@calque/config` renders the typed schema from `calque.config.json`.
`@calque/client` needs Vite 8.1.4 or newer. React pages also install
`@calque/react`, which needs React 19 or newer.

## 6. Add the plugin and the client

Add `calque()` to `vite.config.ts`. Serve the page on `127.0.0.1:5173`, the
origin `calque dev` allows by default; `strictPort` makes Vite fail instead of
moving to another port, which `calque dev` would refuse:

```ts title="vite.config.ts"
// vite.config.ts
import { calque } from "@calque/client/vite";
import { defineConfig } from "vite";

export default defineConfig({
  plugins: [calque()],
  server: { host: "127.0.0.1", port: 5173, strictPort: true },
});
```

Under `vite`, the plugin renders `calque.config.json` into
`calque.generated.ts` and `calque.start.generated.ts` when Vite starts and
whenever the file changes, and before `vite build`. It also proxies `/_calque/`
to `calque dev`, so the page never holds the development bearer. Without Vite,
`npx calque-bindings` renders the same files.

Give the page somewhere to draw, in `index.html`, and create the client in
`main.ts`:

```html title="index.html"
<!doctype html>
<html lang="en">
  <body>
    <ul id="tasks"></ul>
    <script type="module" src="/main.ts"></script>
  </body>
</html>
```

```ts title="main.ts"
// main.ts
import { createSyncClient } from "@calque/client";
import { renderedConnection } from "@calque/client/bootstrap-start";
import { schema } from "./calque.generated.js";

const { endpoint, bearerToken } = renderedConnection();
const client = createSyncClient({
  endpoint,
  auth: () => ({ bearerToken }),
  schema,
});

// A live query. Its listener runs again whenever the rows change.
const tasks = client.db.tasks.query().orderBy("id").subscribe();
tasks.subscribe(() => {
  const snapshot = tasks.getSnapshot();
  if (snapshot.status !== "ready") return;
  const list = document.querySelector("#tasks");
  list?.replaceChildren(
    ...snapshot.result.map((task) => {
      const item = document.createElement("li");
      item.textContent = `${task.done ? "done" : "open"}: ${task.title}`;
      return item;
    }),
  );
});
```

`client.db.tasks` is typed from the `tasks` table. A key that is an `int8` is a
`bigint`, so a row's key is `1n`, not `1`. The
[client's README](../client/packages/client/README.md) has the query rules.
`@calque/react` puts the client in a `SyncProvider` and reads with
`useLiveQuery`; [its README](../client/packages/react/README.md) shows a
component.

## 7. Run `calque dev`

`calque dev` enrolls the database, creates its own publication, replication
slot and `calque_control` schema in it, serves it on `127.0.0.1:8787`, and
writes its address and bearer to `.calque-dev/client.env`. Run it from your
application's root, because the Vite plugin reads the nearest `.calque-dev` at
or above the page's root. It reads `CALQUE_DATABASE_URL`, so export the
variable from step 3 in this terminal as well.

### With the calque binary

```sh
calque dev --table public.tasks \
  --public-base-url http://127.0.0.1:5173/_calque/ \
  --acknowledge-disposable-source
```

`--public-base-url` is the Vite proxy's address, which every URL the server
hands the page names. Name each table you serve with its own `--table`. The
server keeps running; leave it in this terminal. `calque dev` allows the origin
`http://127.0.0.1:5173` unless `--allow-origin` names another.

### With the container

The image runs the same `calque`. It reads the URL from the environment, writes
`.calque-dev` into the directory you mount, and has to run as your user so that
directory stays yours. On Linux the container shares the host's network, so
`127.0.0.1` is your machine:

```sh
export CALQUE_DATABASE_URL='postgresql://calque_sync:calque_sync@127.0.0.1:5432/app'
docker run --rm -e CALQUE_DATABASE_URL --network host \
  -v "$PWD":/work -w /work --user "$(id -u):$(id -g)" \
  ghcr.io/calque-sh/calque dev --table public.tasks \
  --public-base-url http://127.0.0.1:5173/_calque/ \
  --acknowledge-disposable-source
```

On Docker Desktop the container has a network of its own, so `127.0.0.1` is the
container, and the host answers as `host.docker.internal`. Calque refuses a
host name without verified TLS, except that `calque dev`, `inspect` and
`destroy` accept an explicit `sslmode=disable` and print a warning naming the
host: the password and every row cross to your machine in plaintext, so use it
only for a disposable database. The container also has to bind `0.0.0.0` and publish its port on your
loopback address:

```sh
export CALQUE_DATABASE_URL='postgresql://calque_sync:calque_sync@host.docker.internal:5432/app?sslmode=disable'
docker run --rm -e CALQUE_DATABASE_URL -p 127.0.0.1:8787:8787 \
  -v "$PWD":/work -w /work --user "$(id -u):$(id -g)" \
  ghcr.io/calque-sh/calque dev --table public.tasks --bind-address 0.0.0.0 \
  --public-base-url http://127.0.0.1:5173/_calque/ \
  --acknowledge-disposable-source
```

PostgreSQL has to listen on an address the container can reach and allow it in
`pg_hba.conf`. Steps 3 and 4 run the same way, without the port and the bind
address; [the container recipes](cli.md#running-in-a-container) show them.

## 8. Open the page

In a second terminal, from the application's root:

```sh
npx vite
```

Open `http://127.0.0.1:5173`. The page lists the rows of `tasks`. If it shows
an error instead, the browser console and the terminal running `calque dev`
name it.

## 9. Change a row

Commit a change from `psql` while the page is open:

```sh
psql "$CALQUE_DATABASE_URL" -c "INSERT INTO tasks VALUES (2, 'Watch it arrive', false)"
```

The new row appears without a reload, and so does an update or a delete. The
page writes back with `client.db.tasks.update({ id: 1n }, { done: true })`,
which shows at once and stays optimistic until PostgreSQL's change comes back.

To remove what `calque dev` created, stop it and run
`calque destroy --state-dir .calque-dev --yes`, which drops its publication
and replication slot. It reads `CALQUE_DATABASE_URL` too.

## Production

`calque dev` is for a disposable database on your machine. `calque serve`
replaces it: it verifies short-lived grants that your backend signs with an
Ed25519 key, serves a database over TLS that verifies its name, and runs
behind HTTPS. It reads the publication its configuration names, so its role
needs to own no table: the tables' owner creates the publication and grants
the role the rows ([The role](postgresql.md#the-role)).
[Self-hosting](self-host.md) walks it from a clean checkout, and
[operations](design/operations.md) lists every setting.
