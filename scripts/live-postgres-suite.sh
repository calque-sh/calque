#!/bin/sh
# The ignored live-PostgreSQL suite, set up the same way in CI and locally.
#
# `cargo test` skips every `#[ignore]`d test silently, so the suite has to be
# asked for, and it has to be asked for against a PostgreSQL 18 with logical
# decoding and a NON-superuser role that holds REPLICATION: enrollment refuses
# a superuser, so running as the image's bootstrap role fails at setup instead
# of testing anything.
#
#   scripts/live-postgres-suite.sh bootstrap <container>
#       Wait for the postgres:18 container to accept connections, verify
#       wal_level/slots/senders/connections, and create the least-privilege
#       role and database inside it (idempotent; safe to rerun).
#
#   scripts/live-postgres-suite.sh env <port>
#       Print the eight `export` lines the suite reads (eval or source them):
#       five names some test requires, plus DATABASE_URL and
#       CALQUE_DATABASE_URL, alternates a test falls back from, plus
#       CALQUE_SUPERUSER_DATABASE_URL, the image's superuser, for the one test
#       that needs what only a superuser may create.
#       Under GitHub Actions the same names are appended to $GITHUB_ENV.
#
#   scripts/live-postgres-suite.sh run <port> [test-harness args...]
#       Set those names for this process and run
#       `cargo test --workspace --exclude blob-store --no-fail-fast -- --ignored`,
#       then check that every ignored test `--ignored --list` names under the
#       same harness args was executed. A filter such as `--exact <name>`
#       narrows both, so it runs a subset and still fails unless the whole
#       subset passes; a filter that matches nothing fails outright.
#
# blob-store is excluded because its two ignored tests need an S3-compatible
# provider, not PostgreSQL. `pnpm verify:s3:minio` runs those against MinIO.
#
# One test outside blob-store needs S3 too: calque-engine's
# `recovery::tests::a_filesystem_unit_restores_into_a_provider_store_and_serves_from_it`
# fails without the CALQUE_S3_* names. They cannot be set for the whole suite,
# because `calque serve` refuses to start with `CALQUE_S3_*` set and
# `CALQUE_OBJECT_STORE` unset, so CI runs the suite with
# `--exact --skip <that test>`, then starts MinIO and runs `--exact <that test>`
# with the S3 names set for that run alone. A postgres-only run of the whole
# suite reports that test as a failure; do not make it skip silently, which
# would hide an uncovered provider behind a green run.
#
# Tunables (all optional): CALQUE_LIVEPG_ROLE, CALQUE_LIVEPG_PASSWORD,
# CALQUE_LIVEPG_DATABASE, CALQUE_LIVEPG_HOST, CALQUE_LIVEPG_SUPERUSER,
# CALQUE_LIVEPG_SUPERUSER_PASSWORD (default `postgres`, the image's
# POSTGRES_PASSWORD in CI and in docs/development.md).
set -eu

role=${CALQUE_LIVEPG_ROLE:-calque_test}
password=${CALQUE_LIVEPG_PASSWORD:-calque_test}
database=${CALQUE_LIVEPG_DATABASE:-calque_verify}
host=${CALQUE_LIVEPG_HOST:-127.0.0.1}
superuser=${CALQUE_LIVEPG_SUPERUSER:-postgres}
superuser_password=${CALQUE_LIVEPG_SUPERUSER_PASSWORD:-postgres}

# All of these want the same connection string, and no single one covers the
# suite. Five are each required by some ignored test, which fails rather than
# skips when its name is missing. DATABASE_URL and CALQUE_DATABASE_URL
# are alternates: snapshot-coordinator reads DATABASE_URL before
# TEST_DATABASE_URL, and calque-engine's recovery test falls back from
# CALQUE_CONTROL_DATABASE_URL to CALQUE_DATABASE_URL. Exporting all
# seven keeps every test on the same server whichever name it reads first.
env_names='TEST_DATABASE_URL DATABASE_URL CALQUE_TEST_DATABASE_URL
CALQUE_CONTROL_DATABASE_URL CALQUE_SOURCE_DATABASE_URL
CALQUE_ADMIN_DATABASE_URL CALQUE_DATABASE_URL'

usage() {
  # The header comment above, up to the first non-comment line.
  awk 'NR > 1 && !/^#/ { exit } NR > 1 { sub(/^# ?/, ""); print }' "$0" >&2
  exit 2
}

require_port() {
  case "${1:-}" in
    ''|*[!0-9]*) printf '%s\n' 'a decimal TCP port is required' >&2; exit 2 ;;
  esac
  if [ "$1" -lt 1 ] || [ "$1" -gt 65535 ]; then
    printf '%s\n' 'the port must be between 1 and 65535' >&2
    exit 2
  fi
}

database_url() {
  printf 'postgresql://%s:%s@%s:%s/%s' "$role" "$password" "$host" "$1" "$database"
}

# Only a superuser may create a `FOR TABLES IN SCHEMA` publication, and the
# test that checks Calque follows one reads this name alone.
superuser_url() {
  printf 'postgresql://%s:%s@%s:%s/%s' "$superuser" "$superuser_password" "$host" "$1" "$database"
}

# psql inside the container, as the bootstrap superuser. Using the container's
# own client means neither CI nor a developer needs libpq's psql on the host.
container_psql() {
  container=$1
  shift
  docker exec -i "$container" psql -U "$superuser" -v ON_ERROR_STOP=1 -X -q -A -t "$@"
}

bootstrap() {
  container=${1:-}
  [ -n "$container" ] || usage
  # Over TCP, not the Unix socket. The image's entrypoint first runs a
  # temporary initdb server that listens on the socket only, and `pg_isready`
  # against the socket reports it ready just before it shuts down to make way
  # for the real server; the SELECT below would then land on a server that is
  # going away. TCP answers only once the real one is up.
  attempts=0
  until docker exec "$container" pg_isready -h 127.0.0.1 -U "$superuser" -q; do
    attempts=$((attempts + 1))
    if [ "$attempts" -ge 60 ]; then
      printf 'PostgreSQL in container %s did not become ready within 60 s\n' "$container" >&2
      exit 1
    fi
    sleep 1
  done

  # The suite's assumptions about the server, checked before any test can
  # fail on them with a less direct message. Enrollment also refuses a source
  # whose server_encoding, DateStyle, TimeZone or bytea_output disagree, and
  # nothing in CI sets them, so a change to the image or to
  # POSTGRES_INITDB_ARGS stops the job here with the setting named.
  settings=$(container_psql "$container" -c "SELECT current_setting('server_version_num')::int / 10000,
    current_setting('wal_level'),
    current_setting('max_replication_slots')::int,
    current_setting('max_wal_senders')::int,
    current_setting('max_connections')::int,
    current_setting('server_encoding'),
    current_setting('DateStyle'),
    current_setting('TimeZone'),
    current_setting('bytea_output');")
  major=${settings%%|*}
  rest=${settings#*|}
  wal_level=${rest%%|*}
  rest=${rest#*|}
  slots=${rest%%|*}
  rest=${rest#*|}
  senders=${rest%%|*}
  rest=${rest#*|}
  connections=${rest%%|*}
  rest=${rest#*|}
  encoding=${rest%%|*}
  rest=${rest#*|}
  date_style=${rest%%|*}
  rest=${rest#*|}
  time_zone=${rest%%|*}
  bytea_output=${rest#*|}
  if [ "$major" != 18 ]; then
    printf 'PostgreSQL 18 is the only supported source; the container runs major %s\n' "$major" >&2
    exit 1
  fi
  if [ "$wal_level" != logical ]; then
    printf 'wal_level must be logical, found %s\n' "$wal_level" >&2
    exit 1
  fi
  if [ "$slots" -lt 20 ] || [ "$senders" -lt 20 ]; then
    printf 'max_replication_slots and max_wal_senders must both be at least 20, found %s and %s\n' "$slots" "$senders" >&2
    exit 1
  fi
  # Every test's runtime holds several connections, and cargo runs one test
  # per core: at 18 threads, PostgreSQL's default of 100 runs out.
  if [ "$connections" -lt 400 ]; then
    printf 'max_connections must be at least 400, found %s\n' "$connections" >&2
    exit 1
  fi
  if [ "$encoding" != UTF8 ]; then
    printf 'server_encoding must be UTF8, found %s\n' "$encoding" >&2
    exit 1
  fi
  case "$date_style" in
    ISO,*) ;;
    *) printf 'DateStyle must be ISO, found %s\n' "$date_style" >&2; exit 1 ;;
  esac
  if [ "$time_zone" != Etc/UTC ] && [ "$time_zone" != UTC ]; then
    printf 'TimeZone must be UTC, found %s\n' "$time_zone" >&2
    exit 1
  fi
  if [ "$bytea_output" != hex ]; then
    printf 'bytea_output must be hex, found %s\n' "$bytea_output" >&2
    exit 1
  fi

  # CREATEDB: several tests create and drop a throwaway database to prove
  # identity and fence isolation. CREATEROLE: the refusal tests revoke
  # REPLICATION from, or change the password of, a role each creates for
  # itself, since tests running in parallel log in as this one; PostgreSQL 16+
  # lets a CREATEROLE role manage only the roles it created. NOSUPERUSER is the
  # point of the role. The ALTER updates a role an earlier bootstrap created.
  container_psql "$container" <<SQL
SELECT format('CREATE ROLE %I LOGIN PASSWORD %L REPLICATION CREATEDB CREATEROLE NOSUPERUSER', '$role', '$password')
WHERE NOT EXISTS (SELECT 1 FROM pg_catalog.pg_roles WHERE rolname = '$role')
\\gexec
SELECT format('ALTER ROLE %I CREATEROLE', '$role')
\\gexec
SELECT format('CREATE DATABASE %I OWNER %I', '$database', '$role')
WHERE NOT EXISTS (SELECT 1 FROM pg_catalog.pg_database WHERE datname = '$database')
\\gexec
SQL
  container_psql "$container" -d "$database" -c "GRANT ALL ON SCHEMA public TO \"$role\";"

  privileges=$(container_psql "$container" -c "SELECT rolsuper, rolreplication, rolcreatedb, rolcreaterole FROM pg_catalog.pg_roles WHERE rolname = '$role';")
  if [ "$privileges" != 'f|t|t|t' ]; then
    printf 'role %s must be NOSUPERUSER REPLICATION CREATEDB CREATEROLE; pg_roles reports rolsuper|rolreplication|rolcreatedb|rolcreaterole = %s\n' "$role" "$privileges" >&2
    exit 1
  fi
  printf 'live-postgres: %s ready (PostgreSQL %s, wal_level=%s, slots=%s, senders=%s, connections=%s, encoding=%s, DateStyle=%s, TimeZone=%s, bytea_output=%s, role %s is NOSUPERUSER REPLICATION CREATEDB CREATEROLE)\n' \
    "$container" "$major" "$wal_level" "$slots" "$senders" "$connections" "$encoding" "$date_style" "$time_zone" "$bytea_output" "$role"
}

print_env() {
  require_port "${1:-}"
  url=$(database_url "$1")
  for name in $env_names; do
    printf "export %s='%s'\n" "$name" "$url"
    if [ -n "${GITHUB_ENV:-}" ]; then
      printf '%s=%s\n' "$name" "$url" >>"$GITHUB_ENV"
    fi
  done
  printf "export CALQUE_SUPERUSER_DATABASE_URL='%s'\n" "$(superuser_url "$1")"
  if [ -n "${GITHUB_ENV:-}" ]; then
    printf 'CALQUE_SUPERUSER_DATABASE_URL=%s\n' "$(superuser_url "$1")" >>"$GITHUB_ENV"
  fi
}

run() {
  require_port "${1:-}"
  url=$(database_url "$1")
  CALQUE_SUPERUSER_DATABASE_URL=$(superuser_url "$1")
  export CALQUE_SUPERUSER_DATABASE_URL
  shift
  for name in $env_names; do
    export "$name=$url"
  done

  # `cargo test -- --ignored --list`, under the same harness args as the run,
  # is the authority on how many tests the run must execute. A filter that
  # matches nothing lists nothing and is refused here; a binary that fails to
  # link, or a listed test that never reports, shows up below as fewer passes
  # than listed rather than as a smaller green run.
  expected=$(cargo test --workspace --exclude blob-store -q -- --ignored --list "$@" 2>/dev/null \
    | grep -c ': test$' || true)
  if [ "$expected" -eq 0 ]; then
    printf '%s\n' 'no ignored tests were listed (does the filter match anything?); refusing to report an empty run as a pass' >&2
    exit 1
  fi

  log=$(mktemp "${TMPDIR:-/tmp}/calque-live-postgres.XXXXXX")
  started=$(date +%s)
  status=0
  # tee keeps the output visible in CI while the log feeds the tally below.
  # The status is captured inside the group because a plain pipeline in sh
  # would report tee's exit code, not cargo's.
  { cargo test --workspace --exclude blob-store --no-fail-fast -- --ignored "$@" 2>&1 \
      || printf '%s' "$?" >"$log.status"; } | tee "$log"
  if [ -f "$log.status" ]; then
    status=$(cat "$log.status")
    rm -f "$log.status"
  fi
  elapsed=$(( $(date +%s) - started ))

  # The takeover matrix re-executes its own test binary as a worker process,
  # so the raw `ok` count is higher than the number of distinct tests.
  # Count distinct names instead.
  passed=$(grep -E '^test .* \.\.\. ok$' "$log" | sed -E 's/^test (.*) \.\.\. ok$/\1/' | sort -u | wc -l | tr -d ' ')
  failed=$(grep -E '^test .* \.\.\. FAILED$' "$log" | sed -E 's/^test (.*) \.\.\. FAILED$/\1/' | sort -u | wc -l | tr -d ' ')
  rm -f "$log"
  printf 'live-postgres: %s distinct ignored tests passed, %s failed, %s expected; cargo test exit %s; %s s wall-clock\n' \
    "$passed" "$failed" "$expected" "$status" "$elapsed"
  if [ "$status" -ne 0 ]; then
    exit "$status"
  fi
  if [ "$passed" -ne "$expected" ]; then
    printf 'live-postgres: %s ignored tests were listed but %s passed; something did not run\n' "$expected" "$passed" >&2
    exit 1
  fi
}

case "${1:-}" in
  bootstrap) shift; bootstrap "$@" ;;
  env) shift; print_env "$@" ;;
  run) shift; run "$@" ;;
  *) usage ;;
esac
