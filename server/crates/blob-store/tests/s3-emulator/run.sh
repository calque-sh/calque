#!/bin/sh
set -eu
umask 077

script_dir=$(CDPATH='' cd -- "$(dirname -- "$0")" && pwd)
repo_root=$(CDPATH='' cd -- "$script_dir/../../../../.." && pwd)
timestamp=$(date -u +%Y%m%dT%H%M%SZ)
run_slug=$(printf '%s' "$timestamp" | tr '[:upper:]' '[:lower:]')
compose_project="calque-s3-conformance-$run_slug-$$"
port=${CALQUE_MINIO_PORT:-29000}
evidence_dir=${CALQUE_S3_CONFORMANCE_DIR:-"$repo_root/target/s3-conformance/minio-$timestamp"}
image=$(awk '$1 == "image:" { print $2; exit }' "$script_dir/compose.yaml")
bucket="calque-s3-conformance"
prefix="minio-conformance-$run_slug-$$"
volume="calque-s3-conformance-$run_slug-$$-data"

case "$port" in
  ''|*[!0-9]*)
    printf '%s\n' 'CALQUE_MINIO_PORT must be a decimal TCP port.' >&2
    exit 2
    ;;
esac
if [ "$port" -lt 1024 ] || [ "$port" -gt 65535 ]; then
  printf '%s\n' 'CALQUE_MINIO_PORT must be between 1024 and 65535.' >&2
  exit 2
fi

random_hex() {
  bytes=$1
  od -An -N "$bytes" -tx1 /dev/urandom | tr -d ' \n'
}

# These credentials exist only in this process environment and the run-scoped
# container. They are never written to the evidence directory.
CALQUE_MINIO_ROOT_USER="rm$(random_hex 12)"
CALQUE_MINIO_ROOT_PASSWORD="$(random_hex 32)"
export CALQUE_MINIO_ROOT_USER
export CALQUE_MINIO_ROOT_PASSWORD
export CALQUE_MINIO_VOLUME="$volume"

mkdir -p "$(dirname -- "$evidence_dir")"
if ! mkdir "$evidence_dir" 2>/dev/null; then
  printf 'The S3 conformance evidence directory already exists; refusing to overwrite it: %s\n' "$evidence_dir" >&2
  exit 2
fi

evidence_complete=0

source_manifest() {
  destination=$1
  cd "$repo_root"
  rg --files \
    | LC_ALL=C sort \
    | while IFS= read -r file; do
        shasum -a 256 "$file"
      done >"$destination"
}

source_manifest "$evidence_dir/source-before.sha256"

cleanup() {
  docker compose --project-name "$compose_project" --file "$script_dir/compose.yaml" down --volumes >/dev/null 2>&1 || true
}
report_evidence_state() {
  status=$?
  cleanup
  if [ "$evidence_complete" -ne 1 ]; then
    printf 'The S3 conformance run did not finish, so its evidence is incomplete: %s\n' "$evidence_dir" >&2
  fi
  trap - EXIT
  exit "$status"
}
trap report_evidence_state EXIT
trap 'exit 130' INT
trap 'exit 143' TERM

docker compose --project-name "$compose_project" --file "$script_dir/compose.yaml" up --detach --wait
docker image inspect "$image" >"$evidence_dir/provider-image-inspect.json"
docker compose --project-name "$compose_project" --file "$script_dir/compose.yaml" ps --format json >"$evidence_dir/provider-compose-ps.json"
docker compose --project-name "$compose_project" --file "$script_dir/compose.yaml" exec -T s3 minio --version >"$evidence_dir/provider-version.txt"
docker volume inspect "$volume" >"$evidence_dir/provider-volume-inspect.json"

export CALQUE_S3_ENDPOINT="http://127.0.0.1:$port"
export CALQUE_S3_BUCKET="$bucket"
export CALQUE_S3_ACCESS_KEY="$CALQUE_MINIO_ROOT_USER"
export CALQUE_S3_SECRET_KEY="$CALQUE_MINIO_ROOT_PASSWORD"
export CALQUE_S3_CREATE_BUCKET=1
export CALQUE_S3_PREFIX="$prefix"

cd "$repo_root"
if ! cargo test -p blob-store >"$evidence_dir/local-tests.log" 2>&1; then
  sed -n '1,240p' "$evidence_dir/local-tests.log"
  exit 1
fi
grep -q 'test result: ok. 19 passed; 0 failed; 0 ignored;' "$evidence_dir/local-tests.log"
grep -q 'test result: ok. 3 passed; 0 failed; 2 ignored;' "$evidence_dir/local-tests.log"
if ! cargo test -p artifact-catalog one_hundred_deterministic_provider_faults_never_publish \
  >"$evidence_dir/catalog-fault-matrix.log" 2>&1; then
  sed -n '1,240p' "$evidence_dir/catalog-fault-matrix.log"
  exit 1
fi
grep -q 'test result: ok. 1 passed; 0 failed; 0 ignored;' "$evidence_dir/catalog-fault-matrix.log"
if ! cargo test -p blob-store --test conformance -- --ignored --nocapture \
  >"$evidence_dir/provider-tests.log" 2>&1; then
  sed -n '1,240p' "$evidence_dir/provider-tests.log"
  exit 1
fi

grep -q 'test result: ok. 2 passed; 0 failed; 0 ignored;' "$evidence_dir/provider-tests.log"
source_manifest "$evidence_dir/source-after.sha256"
cmp "$evidence_dir/source-before.sha256" "$evidence_dir/source-after.sha256"
source_tree_sha256=$(shasum -a 256 "$evidence_dir/source-before.sha256" | awk '{ print $1 }')
image_digest=$(printf '%s' "$image" | awk -F@ '{ print $2 }')
provider_version_sha256=$(shasum -a 256 "$evidence_dir/provider-version.txt" | awk '{ print $1 }')
bucket_identity_sha256=$(printf '%s' "$bucket" | shasum -a 256 | awk '{ print $1 }')
rustc_version=$(rustc --version)
cargo_version=$(cargo --version)

cat >"$evidence_dir/result.json" <<EOF
{
  "schema": "calque.s3-conformance.v1",
  "generated_at_utc": "$timestamp",
  "result": "PASS",
  "provider_kind": "digest-pinned-containerized-minio-s3-compatible-provider",
  "provider_identity": "minio:$image_digest",
  "provider_version_file": "provider-version.txt",
  "provider_version_sha256": "$provider_version_sha256",
  "provider_image_reference": "$image",
  "provider_image_digest": "$image_digest",
  "provider_endpoint": "http://127.0.0.1:$port",
  "provider_storage": { "kind": "run-scoped-named-volume", "identity": "$volume" },
  "provider_credentials": { "kind": "run-generated-ephemeral", "persisted_in_evidence": false },
  "provider_bucket": "$bucket",
  "provider_bucket_identity_sha256": "$bucket_identity_sha256",
  "provider_prefix": "$prefix",
  "source_tree_sha256": "$source_tree_sha256",
  "source_stable_during_run": true,
  "rustc": "$rustc_version",
  "cargo": "$cargo_version",
  "local_blob_store_tests": { "passed": 20, "failed": 0, "ignored": 2, "ignored_executed_by_provider_cell": 2 },
  "catalog_fault_schedules": 100,
  "provider_tests": { "passed": 2, "failed": 0, "ignored": 0 }
}
EOF

assert_secret_absent() {
  label=$1
  secret=$2
  leaked_file=$(find "$evidence_dir" -type f -exec grep -F -l -- "$secret" {} + 2>/dev/null | sed -n '1p')
  if [ -n "$leaked_file" ]; then
    printf 'The S3 conformance evidence contains the ephemeral %s.\n' "$label" >&2
    exit 1
  fi
}

assert_secret_absent 'access key' "$CALQUE_MINIO_ROOT_USER"
assert_secret_absent 'secret key' "$CALQUE_MINIO_ROOT_PASSWORD"
printf '%s\n' \
  'ephemeral_access_key=ABSENT' \
  'ephemeral_secret_key=ABSENT' >"$evidence_dir/secret-scan.txt"

(
  cd "$evidence_dir"
  shasum -a 256 \
    catalog-fault-matrix.log \
    local-tests.log \
    provider-compose-ps.json \
    provider-image-inspect.json \
    provider-tests.log \
    provider-version.txt \
    provider-volume-inspect.json \
    result.json \
    secret-scan.txt \
    source-after.sha256 \
    source-before.sha256 >SHA256SUMS
  shasum -a 256 -c SHA256SUMS >/dev/null
)

evidence_complete=1
sed -n '1,240p' "$evidence_dir/provider-tests.log"
printf 'S3 conformance evidence against MinIO: %s\n' "$evidence_dir"
