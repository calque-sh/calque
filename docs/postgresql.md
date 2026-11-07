# What Calque needs from PostgreSQL

PostgreSQL must be version 18 with logical WAL, free replication-slot and
WAL-sender capacity, UTF-8, ISO dates, UTC, and hex `bytea` output. The
connecting role must be a **non-superuser** with `REPLICATION`: enrollment
rejects a superuser with `SUPERUSER_NOT_LEAST_PRIVILEGE`, so the bootstrap role
a stock `postgres` image creates cannot be used. That role also needs `CREATE`
on the database (for the `calque_control` schema) and must own the enrolled
tables, because the server creates the publication with
`CREATE PUBLICATION FOR TABLE`. Enrolled tables must be permanent,
non-partitioned ordinary tables with supported columns and a primary key. The
default replica identity, which PostgreSQL takes from the primary key, is
enough. `REPLICA IDENTITY FULL` is accepted but makes PostgreSQL write the whole
old row to the WAL on every update and delete; `NOTHING` and `USING INDEX` are
refused.

A supported column has one of these `pg_catalog` types: `bool`, `int2`,
`int4`, `int8`, `float4`, `float8`, `numeric`, `text`, `varchar`, `bpchar`,
`bytea`, `uuid`, `json`, `jsonb`, `date`, `time`, `timestamp` or
`timestamptz`. Any other type, an enum, array, domain or extension type among
them, is refused with `TYPE_CODEC_UNSUPPORTED`, and a generated column with
`GENERATED_COLUMN_UNSUPPORTED`. A primary-key column must be `bool`, `int2`,
`int4`, `int8`, `text`, `varchar`, `uuid` or `bytea`
(`PRIMARY_KEY_CODEC_UNSUPPORTED`), and a `text` or `varchar` key needs a
deterministic collation (`PRIMARY_KEY_COLLATION_NONDETERMINISTIC`).
`calque inspect` checks every column of each table and fails with these codes
before anything is enrolled.

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
transaction mode cannot carry the replication connection. Calque has not yet
been run against a managed provider.

The guard rejects service and service-file URL parameters, alternate libpq
routing parameters, a list of hosts, and routing variables such as
`PGSERVICE`, `PGHOST`, and `PGPORT`, then supplies the port, transport profile,
and finite connection timeout itself, so every connection it opens goes to
that one endpoint. Docker may forward a loopback endpoint to a container
address, so `inet_server_addr()` is deliberately not required to be loopback;
treat any local TCP forwarder and the PostgreSQL instance behind it as part of
the trusted environment.
