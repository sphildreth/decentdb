#!/usr/bin/env bash
set -euo pipefail

# DecentDB feature showcase script.
#
# Runs a small sequence of CLI commands that demonstrate:
# - DDL + DML
# - basic SELECT + ORDER BY + LIMIT
# - JOIN
# - trigram index + LIKE '%pattern%'
# - optional maintenance commands (checkpoint/stats) if available

say() { printf "\n==> %s\n" "$*"; }
info() { printf "    %s\n" "$*"; }

usage() {
  cat <<'EOF'
Usage:
  examples/showcase.sh [--bin /path/to/decentdb] [--db /path/to/demo.db] [--keep]

Options:
  --bin   Path to the decentdb CLI binary. Defaults to ./decentdb if present, else 'decentdb' from PATH.
  --db    Path to the database file to use. Defaults to a temp file.
  --keep  Do not delete the database file/temp directory on exit.
EOF
}

DEENTDB_BIN=""
DB_PATH=""
KEEP=0

while [[ $# -gt 0 ]]; do
  case "$1" in
    --bin)
      DEENTDB_BIN="$2"
      shift 2
      ;;
    --db)
      DB_PATH="$2"
      shift 2
      ;;
    --keep)
      KEEP=1
      shift
      ;;
    -h|--help)
      usage
      exit 0
      ;;
    *)
      echo "Unknown argument: $1" >&2
      usage >&2
      exit 2
      ;;
  esac
done

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

if [[ -z "$DEENTDB_BIN" ]]; then
  if [[ -x "$REPO_ROOT/decentdb" ]]; then
    DEENTDB_BIN="$REPO_ROOT/decentdb"
  else
    DEENTDB_BIN="decentdb"
  fi
fi

if ! command -v "$DEENTDB_BIN" >/dev/null 2>&1; then
  echo "Could not find DecentDB CLI binary: $DEENTDB_BIN" >&2
  echo "Build it first with: nimble build" >&2
  exit 1
fi

TMP_DIR=""
if [[ -z "$DB_PATH" ]]; then
  TMP_DIR="$(mktemp -d)"
  DB_PATH="$TMP_DIR/decentdb_showcase.db"
fi

cleanup() {
  if [[ $KEEP -eq 1 ]]; then
    info "Keeping DB at: $DB_PATH"
    [[ -n "$TMP_DIR" ]] && info "Keeping temp dir: $TMP_DIR"
    return 0
  fi

  [[ -n "$TMP_DIR" ]] && rm -rf "$TMP_DIR"
}
trap cleanup EXIT

run_repl_script() {
  # Run a non-interactive REPL session by piping a script to stdin.
  # This keeps one process open so DDL/DML/queries can be demonstrated together.
  "$DEENTDB_BIN" repl --db="$DB_PATH" --format=table
}

try_cmd() {
  # Run a command, but don't fail the entire script if it errors.
  set +e
  local out
  out=$("$@" 2>&1)
  local code=$?
  set -e
  if [[ $code -eq 0 ]]; then
    printf "%s\n" "$out"
  else
    info "(skipped) command failed or unavailable: $*"
  fi
}

say "DecentDB CLI: $DEENTDB_BIN"
say "Database: $DB_PATH"

say "SQL walkthrough (DDL/DML/queries)"
cat <<'SQL' | run_repl_script
CREATE TABLE users (id INT PRIMARY KEY, name TEXT, email TEXT);
CREATE TABLE orders (id INT PRIMARY KEY, user_id INT REFERENCES users(id), amount FLOAT64, created_at INT);

INSERT INTO users VALUES (1, 'Alice', 'alice@example.com');
INSERT INTO users VALUES (2, 'Bob', 'bob@example.com');
INSERT INTO users VALUES (3, 'Charlie', 'charlie@example.com');

INSERT INTO orders VALUES (10, 1, 99.99, 1704067200);
INSERT INTO orders VALUES (11, 1, 12.50, 1704067300);
INSERT INTO orders VALUES (12, 2, 42.00, 1704067400);

SELECT id, name FROM users ORDER BY id LIMIT 10;

SELECT users.name, SUM(orders.amount) FROM users JOIN orders ON users.id = orders.user_id GROUP BY users.name ORDER BY 2 DESC;

CREATE INDEX idx_users_name_trgm ON users USING trigram(name);
SELECT id, name FROM users WHERE name LIKE '%Ali%' ORDER BY id;
SQL

say "Maintenance (optional): checkpoint / stats"
try_cmd "$DEENTDB_BIN" checkpoint --db="$DB_PATH"
try_cmd "$DEENTDB_BIN" stats --db="$DB_PATH"

say "Done"
info "Tip: run 'decentdb repl --db <path>' for an interactive session."
