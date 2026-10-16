#!/usr/bin/env bash
set -euo pipefail

UPSTREAM_DIR="${1:-/opt/splunk-cloud-export/splunk-export}"
RUNNER="$UPSTREAM_DIR/splunk_export_parallel.py"
PATCH_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)/patches"

if [[ ! -f "$RUNNER" ]]; then
  echo "Runner not found: $RUNNER" >&2
  exit 1
fi

apply_patch_once() {
  local marker="$1"
  local patch_file="$2"
  local description="$3"

  if grep -Fq "$marker" "$RUNNER"; then
    echo "$description already applied."
    return
  fi

  patch --directory="$UPSTREAM_DIR" --batch --forward -p1 < "$patch_file"
  echo "Applied $description."
}

apply_patch_once \
  "Waiting 20 seconds before starting worker" \
  "$PATCH_DIR/splunk-export-worker-stagger.patch" \
  "20-second worker stagger patch"

apply_patch_once \
  "Recreate the CSV writer after rotating the output file" \
  "$PATCH_DIR/splunk-export-csv-rotation.patch" \
  "CSV rotation writer patch"
