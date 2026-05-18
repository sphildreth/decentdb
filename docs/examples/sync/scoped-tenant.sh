#!/usr/bin/env bash
set -euo pipefail

if [[ -n "${BASH_SOURCE[0]:-}" ]]; then
  script_dir=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
else
  script_dir=$(pwd)
fi

repo_root=$(cd "$script_dir/../../.." && pwd)
tmpdir=$(mktemp -d "${TMPDIR:-/tmp}/decentdb-sync-scope.XXXXXX")
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
ready_file="$tmpdir/server.addr"
server_pid=""

cleanup() {
  if [[ -n "$server_pid" ]]; then
    kill "$server_pid" 2>/dev/null || true
    wait "$server_pid" 2>/dev/null || true
  fi
  rm -rf "$tmpdir"
}

trap cleanup EXIT

tenant_scope="tenant_42"
peer_name="target-local"

run_ddb exec --db="$source_db" --sql="CREATE TABLE IF NOT EXISTS orders (tenant_id INT64, order_id INT64, item TEXT NOT NULL, PRIMARY KEY (tenant_id, order_id))" --noRows
run_ddb exec --db="$target_db" --sql="CREATE TABLE IF NOT EXISTS orders (tenant_id INT64, order_id INT64, item TEXT NOT NULL, PRIMARY KEY (tenant_id, order_id))" --noRows

run_ddb sync init --db="$source_db" --replica-id=node-a
run_ddb sync init --db="$target_db" --replica-id=node-b

run_ddb exec --db="$source_db" --sql="INSERT INTO orders (tenant_id, order_id, item) VALUES (42, 1, 'source-tenant-row')" --noRows
run_ddb exec --db="$source_db" --sql="INSERT INTO orders (tenant_id, order_id, item) VALUES (7, 1, 'source-unscoped-row')" --noRows
run_ddb exec --db="$target_db" --sql="INSERT INTO orders (tenant_id, order_id, item) VALUES (42, 2, 'target-tenant-row')" --noRows
run_ddb exec --db="$target_db" --sql="INSERT INTO orders (tenant_id, order_id, item) VALUES (7, 1, 'target-unscoped-row')" --noRows

run_ddb sync scope create --db="$source_db" --name="$tenant_scope" --include=orders --row-filter="tenant_id = 42"
run_ddb sync scope create --db="$target_db" --name="$tenant_scope" --include=orders --row-filter="tenant_id = 42"
run_ddb sync peer add --db="$source_db" --name="$peer_name" --endpoint="http://127.0.0.1:0"
run_ddb sync scope bind --db="$source_db" --peer="$peer_name" --scope="$tenant_scope"

# Start the target as a short-lived localhost sync endpoint. The max-requests
# value is enough for one handshake + push + pull cycle.
run_ddb sync serve \
  --db="$target_db" \
  --bind=127.0.0.1:0 \
  --scope="$tenant_scope" \
  --ready-file="$ready_file" \
  --max-requests=3 &
server_pid=$!

server_addr=""
for _ in $(seq 1 100); do
  if [[ -s "$ready_file" ]]; then
    server_addr=$(cat "$ready_file")
    break
  fi
  sleep 0.05
done

if [[ -z "$server_addr" ]]; then
  kill "$server_pid" 2>/dev/null || true
  wait "$server_pid" 2>/dev/null || true
  echo "sync server did not start" >&2
  exit 1
fi

run_ddb sync peer add --db="$source_db" --name="$peer_name" --endpoint="http://$server_addr"
run_ddb sync run --db="$source_db" --peer="$peer_name" --direction=both --format=table

wait "$server_pid"

run_ddb exec --db="$source_db" --sql="SELECT tenant_id, order_id, item FROM orders ORDER BY tenant_id, order_id" --format=table
run_ddb exec --db="$target_db" --sql="SELECT tenant_id, order_id, item FROM orders ORDER BY tenant_id, order_id" --format=table

# Expected shape:
#   tenant_id=42 rows converge on both replicas
#   tenant_id=7 rows stay local to each replica

source_scoped_count=$(
  run_ddb exec --db="$source_db" --sql="SELECT COUNT(*) FROM orders WHERE tenant_id = 42" --format=csv \
    | awk -F, 'NR==2 { gsub(/\r/, "", $1); print $1 }'
)
target_scoped_count=$(
  run_ddb exec --db="$target_db" --sql="SELECT COUNT(*) FROM orders WHERE tenant_id = 42" --format=csv \
    | awk -F, 'NR==2 { gsub(/\r/, "", $1); print $1 }'
)
source_unscoped=$(
  run_ddb exec --db="$source_db" --sql="SELECT item FROM orders WHERE tenant_id = 7 AND order_id = 1" --format=csv \
    | awk -F, 'NR==2 { gsub(/\r/, "", $1); print $1 }'
)
target_unscoped=$(
  run_ddb exec --db="$target_db" --sql="SELECT item FROM orders WHERE tenant_id = 7 AND order_id = 1" --format=csv \
    | awk -F, 'NR==2 { gsub(/\r/, "", $1); print $1 }'
)

if [[ "$source_scoped_count" != "2" || "$target_scoped_count" != "2" ]]; then
  echo "expected both replicas to contain both tenant_id=42 rows" >&2
  exit 1
fi

if [[ "$source_unscoped" != "source-unscoped-row" || "$target_unscoped" != "target-unscoped-row" ]]; then
  echo "expected tenant_id=7 rows to remain local to each replica" >&2
  exit 1
fi

echo "scoped tenant sync complete"
