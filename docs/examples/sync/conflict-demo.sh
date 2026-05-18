#!/usr/bin/env bash
set -euo pipefail

if [[ -n "${BASH_SOURCE[0]:-}" ]]; then
  script_dir=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
else
  script_dir=$(pwd)
fi

repo_root=$(cd "$script_dir/../../.." && pwd)
tmpdir=$(mktemp -d "${TMPDIR:-/tmp}/decentdb-sync-conflict.XXXXXX")
trap 'rm -rf "$tmpdir"' EXIT

ddb_bin="${DDB_BIN:-$repo_root/target/debug/decentdb}"
run_ddb() {
  if [[ -x "$ddb_bin" ]]; then
    "$ddb_bin" "$@"
  else
    cargo run -q -p decentdb-cli -- "$@"
  fi
}

source_db="$tmpdir/source.ddb"
target_db="$tmpdir/target.ddb"
batch_file="$tmpdir/batch.json"

run_ddb exec --db="$source_db" --sql="CREATE TABLE IF NOT EXISTS profiles (id INT64 PRIMARY KEY, name TEXT NOT NULL)" --noRows
run_ddb exec --db="$target_db" --sql="CREATE TABLE IF NOT EXISTS profiles (id INT64 PRIMARY KEY, name TEXT NOT NULL)" --noRows

run_ddb sync init --db="$source_db" --replica-id=node-a
run_ddb sync init --db="$target_db" --replica-id=node-b

run_ddb exec --db="$source_db" --sql="INSERT INTO profiles (id, name) VALUES (1, 'alice-from-a')" --noRows
run_ddb exec --db="$target_db" --sql="INSERT INTO profiles (id, name) VALUES (1, 'alice-from-b')" --noRows

run_ddb sync export --db="$source_db" --since=0 --limit=100 --output="$batch_file"
run_ddb sync import --db="$target_db" --input="$batch_file"

run_ddb sync conflicts --db="$target_db" --all --format=table

conflict_id=$(
  run_ddb exec --db="$target_db" --sql="SELECT * FROM sys_sync_conflicts ORDER BY conflict_id" --format=csv \
    | awk -F, 'NR==2 { gsub(/\r/, "", $1); print $1 }'
)

if [[ -z "$conflict_id" ]]; then
  echo "expected at least one conflict" >&2
  exit 1
fi

run_ddb sync conflict show --db="$target_db" --id="$conflict_id" --format=table
run_ddb sync conflict resolve --db="$target_db" --id="$conflict_id" --action=keep-local --by=demo --note="keep the target row" --format=table
run_ddb sync conflicts --db="$target_db" --all --format=table
run_ddb sync conflict reopen --db="$target_db" --id="$conflict_id" --format=table

# Expected shape:
#   import summary reports conflicted=1
#   sys_sync_conflicts contains one row
#   show / resolve / reopen all operate on the same conflict id

echo "conflict workflow demo complete"
