# ADR-0111: Table-Valued Functions (json_each, json_tree)

## Status
Accepted

## Context
DecentDB needs `json_each()` and `json_tree()` as table-valued functions (TVFs)
in the FROM clause. These are the only TVFs currently required. PostgreSQL and
SQLite both support these functions.

## Decision

### Scope
Only `json_each` and `json_tree` are implemented. A general TVF extension
mechanism is out of scope.

### Parser
`RangeFunction` nodes from libpg_query are parsed into a new FROM source type
with a function name, argument expressions, and optional alias.

### Execution Model
TVFs are materialized eagerly: the function arguments are evaluated once, the
function produces all rows into a seq, and the executor scans that seq exactly
like a table scan. This avoids changes to the cursor/iterator model.

### Column Schema

**json_each(json_text)**:
| Column | Type   | Description                |
|--------|--------|----------------------------|
| key    | TEXT   | Object key or array index  |
| value  | TEXT   | JSON-encoded value         |
| type   | TEXT   | JSON type name             |

**json_tree(json_text)**:
| Column | Type   | Description                 |
|--------|--------|-----------------------------|
| key    | TEXT   | Key or array index          |
| value  | TEXT   | JSON-encoded value          |
| type   | TEXT   | JSON type name              |
| path   | TEXT   | JSON path from root (e.g. $, $.a, $.a[0]) |

### NULL Handling
- NULL input → empty result set (zero rows).
- Invalid JSON → SQL error.

### Persistence
No persistence impact. TVFs produce transient row sets.

## Consequences
- Two new virtual column sets ("json_each" and "json_tree") are known to the
  binder and planner.
- `SELECT *` over a TVF expands to the fixed column set above.
- TVFs cannot currently appear as join right-hand sides (only as the primary
  FROM source). This can be relaxed later.
