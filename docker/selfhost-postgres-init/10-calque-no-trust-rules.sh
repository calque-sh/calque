#!/bin/sh
# Fails the cluster's first start if any pg_hba.conf rule authenticates by
# address instead of by password.
#
# By default `initdb` writes `trust` rules for the Unix socket and loopback,
# and the image appends `host all all all scram-sha-256` *after* them. pg_hba
# is first-match-wins, so such a cluster never checks a socket or loopback
# client's password, the least-privilege role's included.
# `docker-compose.selfhost.yml` passes `--auth=scram-sha-256` to initdb in
# POSTGRES_INITDB_ARGS, so initdb writes those rules as `scram-sha-256`
# instead. This script checks that it did.
#
# That matters here and not in the two disposable compositions because this
# is the one that is a deployment: the server reaches PostgreSQL over a Unix
# socket on a shared volume, and a socket is a capability, not an identity.
# With `trust` rules, anything that could open it was the bootstrap superuser,
# the role enrollment refuses on purpose (SUPERUSER_NOT_LEAST_PRIVILEGE).
#
# pg_hba_file_rules reads pg_hba.conf from disk. Nothing changes the file after
# the entrypoint's temporary server loads it, and the server that starts after
# initialization loads the same file, so the rules it shows are the rules both
# enforce. psql authenticates with the PGPASSWORD the entrypoint exports for
# its init scripts.
set -eu

psql --no-password --set ON_ERROR_STOP=1 \
	--username "$POSTGRES_USER" --dbname "$POSTGRES_DB" <<'SQL'
DO $$
DECLARE remaining int;
BEGIN
  SELECT count(*) INTO remaining
    FROM pg_catalog.pg_hba_file_rules
   WHERE auth_method = 'trust';
  IF remaining <> 0 THEN
    RAISE EXCEPTION
      'pg_hba.conf has % trust rule(s), so this cluster would authenticate socket and loopback clients by address; initialize it with POSTGRES_INITDB_ARGS=--auth=scram-sha-256, as docker-compose.selfhost.yml does', remaining;
  END IF;
END
$$;
SQL
