#!/bin/sh
# Creates the least-privilege role `calque serve` connects as.
#
# It cannot be the image's bootstrap role: that role is a superuser, and
# enrollment refuses a superuser outright with SUPERUSER_NOT_LEAST_PRIVILEGE
# (source-control validates least privilege before anything is enrolled).
#
# REPLICATION is required for the logical slot. CREATE on the database is
# required because the server installs its `calque_control` schema there, and
# CREATE and USAGE on `public` because the application's tables live there and
# `CREATE PUBLICATION ... FOR TABLE` requires ownership of every table named.
#
# The name and the password arrive in the environment, never in this file and
# never on a command line: `docker-compose.selfhost.yml` refuses to start
# without them. They reach `psql` as `:'role'` and `:'password'` and are put
# into SQL by `format()`'s `%I` and `%L`, so a password containing a quote is
# a password and not a statement.
set -eu

: "${CALQUE_SELFHOST_DB_ROLE:?the Calque database role name is required}"
: "${CALQUE_SELFHOST_DB_PASSWORD:?a password for the Calque database role is required}"

psql --no-password --set ON_ERROR_STOP=1 \
	--username "$POSTGRES_USER" --dbname "$POSTGRES_DB" \
	--set=role="$CALQUE_SELFHOST_DB_ROLE" \
	--set=password="$CALQUE_SELFHOST_DB_PASSWORD" <<'SQL'
SELECT format('CREATE ROLE %I LOGIN PASSWORD %L REPLICATION NOSUPERUSER', :'role', :'password')
\gexec
SELECT format('GRANT CREATE ON DATABASE %I TO %I', current_database(), :'role')
\gexec
SELECT format('GRANT CREATE, USAGE ON SCHEMA public TO %I', :'role')
\gexec
SQL
