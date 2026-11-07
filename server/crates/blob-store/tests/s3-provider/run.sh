#!/bin/sh
set -eu
umask 077

script_dir=$(CDPATH= cd -- "$(dirname -- "$0")" && pwd)
repo_root=$(CDPATH= cd -- "$script_dir/../../../../.." && pwd)
timestamp=$(date -u +%Y%m%dT%H%M%SZ)
evidence_dir=${CALQUE_S3_CONFORMANCE_DIR:-"$repo_root/target/s3-conformance/real-$timestamp"}

if [ "${CALQUE_S3_ISOLATED_BUCKET:-}" != "1" ]; then
  printf '%s\n' 'CALQUE_S3_ISOLATED_BUCKET=1 is required; the conformance run creates and deletes test objects.' >&2
  exit 2
fi
: "${CALQUE_S3_PROVIDER_ID:?a stable non-secret provider/account/bucket identity is required}"
: "${CALQUE_S3_BUCKET:?an existing isolated bucket is required}"
: "${CALQUE_S3_REGION:?the provider region is required}"
: "${CALQUE_S3_ACCESS_KEY:?explicit short-lived test credentials are required}"
: "${CALQUE_S3_SECRET_KEY:?explicit short-lived test credentials are required}"
case "$CALQUE_S3_PROVIDER_ID" in
  *[!A-Za-z0-9._:/@-]*|'')
    printf '%s\n' 'CALQUE_S3_PROVIDER_ID must be a non-secret stable identifier using only A-Z, a-z, 0-9, ._:/@-.' >&2
    exit 2
    ;;
esac
case "$CALQUE_S3_REGION" in
  *[!A-Za-z0-9._-]*|'')
    printf '%s\n' 'CALQUE_S3_REGION contains unsupported characters.' >&2
    exit 2
    ;;
esac

export CALQUE_S3_PREFIX=${CALQUE_S3_PREFIX:-"calque-s3-conformance-$timestamp"}
unset CALQUE_S3_CREATE_BUCKET
mkdir -p "$(dirname -- "$evidence_dir")"
if ! mkdir "$evidence_dir" 2>/dev/null; then
  printf 'The S3 conformance evidence directory already exists; refusing to overwrite it: %s\n' "$evidence_dir" >&2
  exit 2
fi

evidence_complete=0
report_evidence_state() {
  status=$?
  if [ "$evidence_complete" -ne 1 ]; then
    printf 'The S3 conformance run did not finish, so its evidence is incomplete: %s\n' "$evidence_dir" >&2
  fi
  trap - EXIT
  exit "$status"
}
trap report_evidence_state EXIT
trap 'exit 130' INT
trap 'exit 143' TERM

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

cd "$repo_root"
if ! cargo test -p blob-store --test conformance -- --ignored --nocapture \
  >"$evidence_dir/provider-tests.log" 2>&1; then
  sed -n '1,240p' "$evidence_dir/provider-tests.log"
  exit 1
fi
grep -q 'test result: ok. 2 passed; 0 failed; 0 ignored;' "$evidence_dir/provider-tests.log"

source_manifest "$evidence_dir/source-after.sha256"
cmp "$evidence_dir/source-before.sha256" "$evidence_dir/source-after.sha256"
source_tree_sha256=$(shasum -a 256 "$evidence_dir/source-before.sha256" | awk '{ print $1 }')
rustc_version=$(rustc --version)
cargo_version=$(cargo --version)

cat >"$evidence_dir/result.json" <<EOF
{
  "schema": "calque.s3-conformance.v1",
  "generated_at_utc": "$timestamp",
  "result": "PASS",
  "provider_kind": "real-s3-compatible-provider",
  "provider_identity": "$CALQUE_S3_PROVIDER_ID",
  "provider_region": "$CALQUE_S3_REGION",
  "source_tree_sha256": "$source_tree_sha256",
  "source_stable_during_run": true,
  "rustc": "$rustc_version",
  "cargo": "$cargo_version",
  "provider_tests": { "passed": 2, "failed": 0, "ignored": 0 }
}
EOF

(
  cd "$evidence_dir"
  shasum -a 256 \
    provider-tests.log \
    result.json \
    source-after.sha256 \
    source-before.sha256 >SHA256SUMS
  shasum -a 256 -c SHA256SUMS >/dev/null
)

evidence_complete=1
sed -n '1,240p' "$evidence_dir/provider-tests.log"
printf 'S3 conformance evidence against the provider: %s\n' "$evidence_dir"
