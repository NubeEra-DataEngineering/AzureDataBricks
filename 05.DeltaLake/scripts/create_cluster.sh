#!/usr/bin/env bash
set -euo pipefail

# Requires:
#  - databricks CLI configured (databricks configure --token)
#  - scripts/cluster.json present
#
# Usage:
#   bash create_cluster.sh
#
# Outputs the created cluster_id.

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
SPEC="${SCRIPT_DIR}/cluster.json"

if ! command -v databricks >/dev/null 2>&1; then
  echo "databricks CLI not found. Install with: pip install databricks-cli" >&2
  exit 1
fi

RESP="$(databricks clusters create --json-file "${SPEC}")"
echo "${RESP}" | python -c 'import sys, json; print(json.load(sys.stdin)["cluster_id"])'
