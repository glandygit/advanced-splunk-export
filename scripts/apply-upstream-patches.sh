#!/usr/bin/env bash
set -euo pipefail

UPSTREAM_DIR="${1:-/opt/splunk-cloud-export/splunk-export}"
RUNNER="$UPSTREAM_DIR/splunk_export_parallel.py"
PATCH_FILE="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)/patches/splunk-export-worker-stagger.patch"

if [[ ! -f "$RUNNER" ]]; then
  echo "Runner not found: $RUNNER" >&2
  exit 1
fi

if grep -Fq "Waiting 20 seconds before starting worker" "$RUNNER"; then
  echo "Worker stagger patch already applied."
  exit 0
fi

patch --directory="$UPSTREAM_DIR" --batch --forward -p1 < "$PATCH_FILE"
echo "Applied 20-second worker stagger patch."
