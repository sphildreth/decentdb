# Built-In Web Console

`decentdb serve` starts a small local HTTP server and self-contained browser
console for quick database inspection and ad hoc SQL.

```bash
decentdb serve --db ./app.ddb
```

By default the server binds to `127.0.0.1:7373`, serves the Web Console at
`http://localhost:7373`, and protects API requests with an ephemeral bearer
token that is injected into the local browser session. Users do not need to
copy or configure a token for the normal localhost workflow.

The console is intentionally lightweight. It is not Decent Bench and does not
try to be a full IDE. It uses embedded HTML, CSS, and vanilla JavaScript, with
no CDN, external font, telemetry, package install, or internet dependency.

## Common Commands

```bash
# Local read-write console
decentdb serve --db ./app.ddb

# Open the default browser
decentdb serve --db ./app.ddb --open

# Safe inspection mode
decentdb serve --db ./prod-copy.ddb --read-only

# Positional database shorthand
decentdb serve ./app.ddb

# Localhost debugging without API auth
decentdb serve --db ./app.ddb --no-auth

# Remote/sidecar mode requires an explicit token source
DECENTDB_TOKEN="$(openssl rand -hex 32)"
decentdb serve --db ./app.ddb --host 0.0.0.0 --token-env DECENTDB_TOKEN
```

`--no-auth` is accepted only for localhost binding. Binding to a non-localhost
host, such as `0.0.0.0`, requires `--token-env`.

## Useful Options

| Option | Default | Purpose |
|---|---:|---|
| `--host` | `127.0.0.1` | Bind host |
| `--port` | `7373` | Bind port |
| `--read-only` | `false` | Reject mutating SQL |
| `--open` | `false` | Open the Web Console in the default browser |
| `--max-result-rows` | `1000` | Maximum rows returned to the browser per result set |
| `--query-timeout` | `30s` | Report a timeout when execution exceeds the limit |
| `--max-body-size` | `4mb` | Maximum HTTP request body size |
| `--max-concurrent-requests` | `32` | Concurrent request cap |
| `--token-env` | unset | Environment variable containing a bearer token |
| `--show-token` | `false` | Print the bearer token for debugging/API clients |
| `--no-auth` | `false` | Disable auth for localhost-only debugging |
| `--cors-origin` | unset | Allow one explicit browser origin |
| `--log-format` | `text` | `text` or `json` request logging |

## HTTP API

All API routes except `/healthz` and `/readyz` require the bearer token unless
`--no-auth` is set.

```text
GET  /healthz
GET  /readyz
GET  /api/v1
GET  /api/v1/info
GET  /api/v1/schema
GET  /api/v1/tables
GET  /api/v1/tables/{tableName}
GET  /api/v1/indexes
GET  /api/v1/views
GET  /api/v1/triggers
POST /api/v1/sql
POST /api/v1/explain
```

SQL requests use JSON:

```json
{
  "sql": "SELECT id, email FROM users WHERE status = $1 LIMIT 100",
  "params": ["active"],
  "readonly": true
}
```

Responses include column names, type metadata, rows, affected row counts,
elapsed time, and truncation status. Add `?format=ndjson` to `/api/v1/sql` for
newline-delimited JSON result output.

## Console Features

- Database metadata and mode display.
- Schema sidebar for tables, views, indexes, and triggers.
- Table detail view with columns, constraints, indexes, triggers, and DDL.
- SQL textarea with Ctrl+Enter/Cmd+Enter execution.
- Query history stored locally in browser `localStorage`.
- Result table with truncation notices, copy, and CSV export.
- Light/dark theme toggle stored locally.
- Structured SQL and request errors.

The console only makes same-origin requests to the running DecentDB server.
