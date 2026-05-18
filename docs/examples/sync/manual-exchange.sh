#!/usr/bin/env bash
set -euo pipefail

if [[ -n "${BASH_SOURCE[0]:-}" ]]; then
  script_dir=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
else
  script_dir=$(pwd)
fi

repo_root=$(cd "$script_dir/../../.." && pwd)
tmpdir=$(mktemp -d "${TMPDIR:-/tmp}/decentdb-sync-manual.XXXXXX")
trap 'rm -rf "$tmpdir"' EXIT

ddb_bin="${DDB_BIN:-$repo_root/target/debug/decentdb}"
run_ddb() {
  if [[ -x "$ddb_bin" ]]; then
    "$ddb_bin" "$@"
  else
    cargo run -q -p decentdb-cli -- "$@"
  fi
}

src_db="$tmpdir/source.ddb"
dst_db="$tmpdir/target.ddb"
batch_file="$tmpdir/batch.json"

run_ddb exec --db="$src_db" --sql="CREATE TABLE IF NOT EXISTS items (id INT64 PRIMARY KEY, name TEXT NOT NULL, qty INT64 NOT NULL)" --noRows
run_ddb exec --db="$dst_db" --sql="CREATE TABLE IF NOT EXISTS items (id INT64 PRIMARY KEY, name TEXT NOT NULL, qty INT64 NOT NULL)" --noRows

run_ddb sync init --db="$src_db" --replica-id=node-a
run_ddb sync init --db="$dst_db" --replica-id=node-b

run_ddb exec --db="$src_db" --sql="INSERT INTO items (id, name, qty) VALUES (1, 'widget', 3)" --noRows

# Expected shape:
#   sync status -> enabled=true, replica_id=node-a, next_sequence=2
#   sync export  -> writes JSON batch metadata plus records
#   sync import  -> seen=1, applied=1, skipped=0, conflicted=0
run_ddb sync status --db="$src_db" --format=table
run_ddb sync pending --db="$src_db" --since=0 --limit=10 --format=table
run_ddb sync export --db="$src_db" --since=0 --limit=100 --output="$batch_file"
run_ddb sync import --db="$dst_db" --input="$batch_file"

run_ddb exec --db="$dst_db" --sql="SELECT id, name, qty FROM items ORDER BY id" --format=table
run_ddb sync pending --db="$dst_db" --since=0 --limit=10 --format=table

copied_count=$(
  run_ddb exec \
    --db="$dst_db" \
    --sql="SELECT COUNT(*) FROM items WHERE id = 1 AND name = 'widget' AND qty = 3" \
    --format=csv \
    | awk -F, 'NR==2 { gsub(/\r/, "", $1); print $1 }'
)

if [[ "$copied_count" != "1" ]]; then
  echo "expected the target database to contain the synced widget row" >&2
  exit 1
fi

echo "manual sync exchange complete"
