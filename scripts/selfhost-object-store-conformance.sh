#!/bin/sh
# Runs the two `#[ignore]`d blob-store S3 conformance tests against the object
# store `docker-compose.selfhost.yml` is running right now: the same image,
# bucket, credentials and path-style setting the deployment uses, so it
# catches a bucket policy or path-style mismatch that the disposable MinIO of
# `pnpm verify:s3:minio` cannot.
#
# It writes under its own prefix, never the deployment's, and deletes
# everything it wrote. That is the only reason it is safe against a running
# deployment: do not point it at a bucket by hand.
#
# Bring the composition up first, with the same environment. Then:
#
#   export CARGO_TARGET_DIR="$(git rev-parse --show-toplevel)/target/cargo"
#   export CALQUE_SELFHOST_S3_PORT="${CALQUE_SELFHOST_S3_PORT:-59001}"
#   sh scripts/selfhost-object-store-conformance.sh
#
# Expects: `test result: ok. 2 passed; 0 failed; 0 ignored`.
set -eu

# No apostrophes in these messages: inside "${var:?word}" a single quote opens
# a quoted string in bash's POSIX mode, and the script then fails to parse.
: "${CALQUE_SELFHOST_S3_PORT:?the published object-store port of the composition is required}"
: "${CALQUE_S3_ACCESS_KEY:?the object-store access key of the composition is required}"
: "${CALQUE_S3_SECRET_KEY:?the object-store secret key of the composition is required}"

repo_root=$(CDPATH='' cd -- "$(dirname -- "$0")/.." && pwd)

case "$CALQUE_SELFHOST_S3_PORT" in
  ''|*[!0-9]*)
    printf '%s\n' 'CALQUE_SELFHOST_S3_PORT must be a decimal TCP port.' >&2
    exit 2
    ;;
esac

# The deployment's bucket, and never the deployment's prefix. The conformance
# suite creates, overwrites and deletes objects; sharing a prefix with the
# published artifacts would let a test delete one.
CALQUE_S3_BUCKET=${CALQUE_S3_BUCKET:-calque-artifacts}
CALQUE_S3_REGION=${CALQUE_S3_REGION:-us-east-1}
CALQUE_S3_PREFIX="selfhost-conformance-$(date -u +%Y%m%dT%H%M%SZ)-$$"
CALQUE_S3_ENDPOINT="http://127.0.0.1:$CALQUE_SELFHOST_S3_PORT"
CALQUE_S3_FORCE_PATH_STYLE=1
export CALQUE_S3_BUCKET CALQUE_S3_REGION CALQUE_S3_PREFIX \
  CALQUE_S3_ENDPOINT CALQUE_S3_FORCE_PATH_STYLE
# The bucket already exists: `objectstore-init` created it, and a server is
# serving out of it. Creating one here would mean the run had been pointed at a
# store the deployment is not using.
unset CALQUE_S3_CREATE_BUCKET

cd "$repo_root"
# GNU mktemp refuses a template without X's, so `mktemp -t prefix` fails on
# Linux.
log=$(mktemp "${TMPDIR:-/tmp}/calque-selfhost-conformance.XXXXXX")
trap 'rm -f "$log"' EXIT INT TERM
if ! cargo test -p blob-store --test conformance -- --ignored --nocapture >"$log" 2>&1; then
  cat "$log" >&2
  exit 1
fi
cat "$log"
grep -q 'test result: ok. 2 passed; 0 failed; 0 ignored;' "$log"
printf 'blob-store S3 conformance passed against the composition at %s (prefix %s)\n' \
  "$CALQUE_S3_ENDPOINT" "$CALQUE_S3_PREFIX"
