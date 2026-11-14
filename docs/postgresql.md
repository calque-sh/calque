# What Calque needs from PostgreSQL

PostgreSQL must be version 18 with logical WAL, free replication-slot and
WAL-sender capacity, and UTF-8. Calque opens every connection with
`TimeZone=UTC`, `DateStyle=ISO,MDY`, `bytea_output=hex` and
`extra_float_digits=1`, so a role's or database's own defaults for them do not
matter. The connecting role must be a **non-superuser** with `REPLICATION`: enrollment
rejects a superuser with `SUPERUSER_NOT_LEAST_PRIVILEGE`, so the bootstrap role
a stock `postgres` image creates cannot be used. That role also needs `CREATE`
on the database, for Calque's own `calque_control` and `calque_internal`
schemas, and `SELECT` on the enrolled tables, with `INSERT`, `UPDATE` and
`DELETE` when pages write through Calque. Under `calque serve` it needs to own
nothing.

Enrolled tables must be permanent, non-partitioned ordinary tables with
supported columns and a primary key. The default replica identity, which
PostgreSQL takes from the primary key, is enough. `REPLICA IDENTITY FULL` is
accepted but makes PostgreSQL write the whole old row to the WAL on every
update and delete; `NOTHING` and `USING INDEX` are refused.

A supported column has one of these `pg_catalog` types: `bool`, `int2`,
`int4`, `int8`, `float4`, `float8`, `numeric`, `text`, `varchar`, `bpchar`,
`bytea`, `uuid`, `json`, `jsonb`, `date`, `time`, `timestamp` or
`timestamptz`. An enum travels as its label's text and a domain as the type
under it, through any chain of domains, so either is supported when that type
is. A write still goes through the column's own type, so PostgreSQL checks the
label or the domain's constraints. The browser sorts an enum column by label,
not in the order the enum declares. Any other type, an array, range, composite
or extension type among them, is refused with `TYPE_CODEC_UNSUPPORTED`, and a
generated column with `GENERATED_COLUMN_UNSUPPORTED`. A primary-key column
must be `bool`, `int2`, `int4`, `int8`, `text`, `varchar`, `uuid` or `bytea`,
or an enum or domain carried as one of them
(`PRIMARY_KEY_CODEC_UNSUPPORTED`), and a `text` or `varchar` key needs a
deterministic collation (`PRIMARY_KEY_COLLATION_NONDETERMINISTIC`). A tenant
column must be a built-in `int2`, `int4`, `int8` or `uuid`. `calque inspect`
checks every published column of each table and fails with these codes before
anything is enrolled.

Renaming a value of an enum that an enrolled column uses changes the text of
every row holding it, and PostgreSQL logs no row change for it. Calque watches
those enums with the publication: adding or renaming a value stops change
capture with the publication's error, and `calque destroy`, then a new start,
prepares the working set again from a snapshot.

Change capture gives up a replication stream that carries nothing for its
walsender's `wal_sender_timeout`, at least 5 s and 60 s when it is 0, and
reconnects. A healthy walsender sends a keepalive within half that time.

## The role

Create the role as a superuser, in the application's database. Use your own
password:

```sql
CREATE ROLE calque_sync LOGIN PASSWORD 'calque_sync' REPLICATION;
GRANT CREATE ON DATABASE app TO calque_sync;
```

On Amazon RDS and Aurora, `GRANT rds_replication TO calque_sync` replaces the
`REPLICATION` attribute.

The tables' owner creates the publication `calque serve` reads, naming every
table to serve, and grants Calque's role the rows:

```sql
CREATE PUBLICATION calque_app FOR TABLE public.projects, public.tasks;
GRANT SELECT, INSERT, UPDATE, DELETE ON public.projects, public.tasks TO calque_sync;
```

A table in a schema other than `public` also needs
`GRANT USAGE ON SCHEMA <schema> TO calque_sync`. When the publication is
missing, Calque creates it only if its role owns every table, and otherwise
refuses with that `CREATE PUBLICATION` statement for the owner to run.
`calque destroy` drops the publication only when Calque's role owns it, and
says so when it keeps one.

`calque dev` names and creates its own publication, so its role has to own
the tables it serves. A table that `calque_sync` creates itself is its own;
creating one in `public` needs the schema, granted as a superuser:

```sql
GRANT CREATE, USAGE ON SCHEMA public TO calque_sync;
```

For a table that exists, make `calque_sync` a member of its owner's role,
which leaves the table with its owner and your migrations unchanged:

```sql
SELECT tablename, tableowner FROM pg_tables WHERE schemaname = 'public';
GRANT app_owner TO calque_sync;
```

## Publishing only some columns

A column a collection leaves out of its `columns` in `calque.config.json`
stays inside PostgreSQL. `calque serve` creates its publication with a column
list naming each collection's columns, so pgoutput never sends the others,
and no artifact, change, plan or command carries them, for any grant. To keep
a password hash or an internal note out, delete the column from the
collection's `columns` and from its command's `writableColumns`. When
Calque's role does not own the tables, `serve` prints the statement for their
owner to run, lists included:

```sql
CREATE PUBLICATION calque_app FOR TABLE public.users (id, name, team_id);
```

A publication that already exists must publish exactly each collection's
columns, through a list or as the whole table. `serve` refuses one that
publishes other columns and prints the statement that publishes the right
ones; `calque destroy` drops a publication Calque's role owns, and the next
start creates it anew.

The columns must include the primary key
(`PUBLICATION_COLUMN_LIST_OMITS_KEY`). PostgreSQL refuses every update and
delete on a table with `REPLICA IDENTITY FULL` while a publication lists its
columns, even all of them, so Calque refuses a list on such a table
(`PUBLICATION_COLUMN_LIST_WITH_FULL_IDENTITY`) and `serve` publishes it whole:
its collection names every column, or the table goes back to `REPLICA
IDENTITY DEFAULT`.

A column outside the list may have any type, generated columns included, and
a migration may add, change or drop it while Calque runs. PostgreSQL refuses
to drop a column a publication lists: remove it from `columns` and
`writableColumns`, run `calque destroy`, restart `serve`, then drop it. When a
column outside the list is `NOT NULL` with no default, an insert through
Calque cannot give it a value, so the table takes updates and deletes but no
insert: `calque init` offers none, and `serve` refuses a command that lists
`insert` (`E_SERVE_COMMAND_OPERATION_UNADMITTED`). A row filter is still
refused with `PUBLICATION_ROW_FILTER_UNSUPPORTED`, because Calque serves
complete tables. Changing the publication while Calque runs stops change
capture, as any change to the publication does. `calque dev` creates its own
publication without a list, so under it every column reaches the browser.

## Protecting the primary's disk

A replication slot keeps every WAL segment written since Calque last confirmed
a position. A running Calque confirms continually, but while it is stopped the
slot holds WAL back until the primary's disk fills. PostgreSQL 18 bounds that
two ways; set at least one, in `postgresql.conf` or with `ALTER SYSTEM`, and
reload:

```
max_slot_wal_keep_size = 10GB        # give up a slot that holds back more WAL than this
idle_replication_slot_timeout = 1d   # give up a slot unused for this long
```

`calque serve`, `calque dev` and `calque inspect` warn when neither is set.
Once PostgreSQL gives up the slot, the WAL Calque needs to resume is gone:
Calque stops and names the cause, and `calque destroy`, then a new start,
prepares the working set again from a snapshot. Choose the bound by the
longest outage you want Calque to resume from rather than rebuild.

## The database URL

A database on this machine is reached in plaintext, through a numeric loopback
address such as `127.0.0.1` or a Unix socket directory:

```sh
export CALQUE_DATABASE_URL='postgresql://calque_sync:secret@127.0.0.1:5432/app'
export CALQUE_DATABASE_URL='postgresql:///app?host=%2Fvar%2Frun%2Fpostgresql&user=calque_sync&password=secret'
```

Any other host, `localhost` included, is reached only over TLS that verifies
the server's certificate and name. Add `sslmode=verify-full` and
`sslrootcert`, the absolute path of a PEM file of the CA certificates to trust:

```sh
export CALQUE_DATABASE_URL='postgresql://calque_sync:secret@db.example.com/app?sslmode=verify-full&sslrootcert=/etc/ssl/certs/ca-certificates.crt'
```

A server whose certificate comes from a public CA verifies against the
system's bundle: `/etc/ssl/certs/ca-certificates.crt` on Debian, on Ubuntu and
in Calque's container image, `/etc/ssl/cert.pem` on macOS. A provider that
signs with its own CA publishes that CA as a file to download. Calque refuses
`sslmode=require`, `verify-ca` and `prefer`, because none of them checks the
server's name, and it does not present client certificates.

A managed PostgreSQL has to meet the requirements above. Turn on its logical
replication setting, and use its direct endpoint: a connection pooler in
transaction mode cannot carry the replication connection. Amazon RDS and Aurora
grant replication through membership in `rds_replication` rather than the
`REPLICATION` attribute, and Calque accepts either. Calque has not yet been run
against a managed provider.

The guard rejects service and service-file URL parameters, alternate libpq
routing parameters, a list of hosts, and routing variables such as
`PGSERVICE`, `PGHOST`, and `PGPORT`, then supplies the port, transport profile,
and finite connection timeout itself, so every connection it opens goes to
that one endpoint. Docker may forward a loopback endpoint to a container
address, so `inet_server_addr()` is deliberately not required to be loopback;
treat any local TCP forwarder and the PostgreSQL instance behind it as part of
the trusted environment.

## When Calque refuses a table or a setting

`calque inspect` lists every refusal at once, each with a code, and `calque
dev` and `calque serve` refuse to start on the same ones. Fix them in
PostgreSQL, as a superuser or the table's owner, and run the command again.
Names in the fixes stand for yours: `tasks` is a table, `calque_app` the
publication, `calque_sync` the role.

| Code | Cause | Fix |
| --- | --- | --- |
| `POSTGRES_VERSION_UNSUPPORTED` | The server is not PostgreSQL 18. | Upgrade to 18. |
| `SERVER_ENCODING_UNSUPPORTED` | The database is not UTF-8. | A database's encoding cannot change: create one with `CREATE DATABASE app ENCODING 'UTF8' TEMPLATE template0` and move the tables. |
| `WAL_LEVEL_NOT_LOGICAL` | `wal_level` is `replica`, which is what a stock server starts with. | `ALTER SYSTEM SET wal_level = 'logical'`, then restart PostgreSQL. On a managed database, turn on logical replication in its console. |
| `NO_REPLICATION_SLOT_CAPACITY` | `max_replication_slots` is 0. When it is positive but every slot is taken, PostgreSQL itself refuses the connection with SQLSTATE `53400`. | `ALTER SYSTEM SET max_replication_slots = 10`, then restart. Each server uses one slot, so allow for every server and for leftovers: `SELECT slot_name, active FROM pg_replication_slots` lists them, and `calque destroy` removes the ones Calque made. |
| `NO_WAL_SENDER_CAPACITY` | `max_wal_senders` is 0. | `ALTER SYSTEM SET max_wal_senders = 10`, then restart. Each server uses one WAL sender, and so does every standby and every other replication client. |
| `SUPERUSER_NOT_LEAST_PRIVILEGE` | The role in `CALQUE_DATABASE_URL` is a superuser. | Connect as the role from [The role](#the-role), which is not one. |
| `REPLICATION_PRIVILEGE_REQUIRED` | The role has no `REPLICATION` attribute and is not a member of `rds_replication`. | `ALTER ROLE calque_sync REPLICATION`. On RDS and Aurora, `GRANT rds_replication TO calque_sync`. |
| `SOURCE_IDENTITY_MISMATCH` | The database now reached is not the cluster and timeline the state directory enrolled: it was restored, promoted or replaced. | `calque destroy --state-dir <dir> --yes`, then start the server again. |
| `PUBLICATION_MISSING` | No publication has the name `--publication` gives. | `CREATE PUBLICATION calque_app FOR TABLE public.tasks`, or pass the publication's real name. |
| `PUBLICATION_OPERATION_MISSING` | The publication leaves out `INSERT`, `UPDATE`, `DELETE` or `TRUNCATE`. | `ALTER PUBLICATION calque_app SET (publish = 'insert, update, delete, truncate')` |
| `PUBLICATION_VIA_PARTITION_ROOT_UNSUPPORTED` | The publication sets `publish_via_partition_root`. | `ALTER PUBLICATION calque_app SET (publish_via_partition_root = false)` |
| `TABLE_MISSING`, `TABLE_NOT_INSPECTED` | A `--table` names no table in the database, or no table the inspection could read. | Write the table as `schema.name`, as `public.tasks`, and check it with `\dt public.*`. |
| `TABLE_NOT_IN_PUBLICATION` | The table exists but the publication does not list it. | `ALTER PUBLICATION calque_app ADD TABLE public.tasks` |
| `RELATION_KIND_UNSUPPORTED` | The table is a view, a partitioned table or a partition. | Enroll an ordinary table instead, such as a projection table that your application keeps in step. |
| `TABLE_NOT_PERMANENT` | The table is `UNLOGGED` or temporary. | `ALTER TABLE tasks SET LOGGED` |
| `PRIMARY_KEY_REQUIRED` | The table has no primary key. | `ALTER TABLE tasks ADD PRIMARY KEY (id)` |
| `NULLABLE_PRIMARY_KEY_INVALID` | A primary-key column allows null. | `ALTER TABLE tasks ALTER COLUMN id SET NOT NULL` |
| `PRIMARY_KEY_CODEC_UNSUPPORTED` | A key column has a type outside the key types above, such as `numeric` or `timestamptz`. | Change the column to a key type, as in `ALTER TABLE tasks ALTER COLUMN k TYPE text USING k::text`, or add a surrogate key: `ALTER TABLE tasks DROP CONSTRAINT tasks_pkey, ADD COLUMN id bigint GENERATED ALWAYS AS IDENTITY, ADD PRIMARY KEY (id)` |
| `PRIMARY_KEY_COLLATION_NONDETERMINISTIC` | A `text` or `varchar` key uses a nondeterministic collation. | `ALTER TABLE tasks ALTER COLUMN k TYPE text COLLATE "default"` |
| `REPLICA_IDENTITY_INSUFFICIENT`, `CUSTOM_REPLICA_IDENTITY_UNSUPPORTED` | The replica identity is `NOTHING`, or `USING INDEX`. | `ALTER TABLE tasks REPLICA IDENTITY DEFAULT` |
| `PUBLICATION_ROW_FILTER_UNSUPPORTED` | The publication lists the table with a `WHERE` clause. | Publish every row. `SET TABLE` replaces the list, so name every table: `ALTER PUBLICATION calque_app SET TABLE public.tasks, public.projects` |
| `PUBLICATION_COLUMN_LIST_OMITS_KEY` | The publication's column list leaves out a primary-key column. | Add the column to the collection's `columns`, or to the list, as in `ALTER PUBLICATION calque_app SET TABLE public.tasks (id, title), public.projects` |
| `PUBLICATION_COLUMN_LIST_WITH_FULL_IDENTITY` | The publication lists the columns of a table with `REPLICA IDENTITY FULL`, on which PostgreSQL then refuses every update and delete. | Publish the table whole, or `ALTER TABLE tasks REPLICA IDENTITY DEFAULT` |
| `TYPE_CODEC_UNSUPPORTED` | A column has a type outside the list above, such as an array or a range. | Leave the column out of the publication's column list, change the type, as in `ALTER TABLE tasks ALTER COLUMN tags TYPE jsonb USING to_jsonb(tags)`, or enroll a projection table without the column. |
| `GENERATED_COLUMN_UNSUPPORTED` | A published column is generated. | Leave it out of the publication's column list. For a stored one, `ALTER TABLE tasks ALTER COLUMN total DROP EXPRESSION` keeps its values in an ordinary column. PostgreSQL does not allow that on a virtual one: add an ordinary column, copy the values into it, drop the generated column and rename the new one. |
| `DATE_STYLE_UNSUPPORTED`, `TIME_ZONE_UNSUPPORTED`, `BYTEA_OUTPUT_UNSUPPORTED` | A connection pooler dropped the startup parameters Calque sends. | Connect to PostgreSQL directly, not through the pooler. |

Two refusals come from PostgreSQL and carry no code. `must be owner of table
tasks`, from `CREATE PUBLICATION` in `psql`, means the role running it neither
owns the table nor belongs to a role that does: run it as the table's owner.
`permission denied for database app` means `calque_sync` lacks `CREATE` on it:
`GRANT CREATE ON DATABASE app TO calque_sync`. When `calque dev` or `calque
serve` finds its publication missing, it refuses with the `CREATE PUBLICATION`
statement for the owner to run if its role does not own every table, and names
the missing `CREATE` with the `GRANT` that fixes it.

A table name that `calque init` refuses is a different failure: a collection's
name has to match `^[a-z][a-z0-9]*(?:[._-][a-z0-9]+)*$`. Rename the table, or
enroll a projection table whose name does.
