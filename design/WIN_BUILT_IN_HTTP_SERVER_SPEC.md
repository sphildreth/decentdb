# PRD: DecentDB Built-In HTTP Server and Lightweight Web Console

**Product:** DecentDB  
**Feature:** `decentdb serve` — Built-In HTTP Server + Lightweight Web Console  
**Status:** Draft PRD  
**Owner:** TBD  
**Last Updated:** 2026-05-19  

---

## 1. Executive Summary

DecentDB shall provide an optional CLI-hosted HTTP server mode that allows users to open a `.ddb` database through a local browser-based Web Console and a small JSON-over-HTTP API.

The primary user experience is:

```bash
decentdb serve --db ./app.ddb
```

Then open:

```text
http://localhost:7373
```

The Web Console shall allow users to:

- View basic database metadata.
- Browse schema objects.
- Inspect tables, columns, indexes, and constraints.
- Run ad hoc SQL queries.
- View bounded query results in a simple grid.
- Export results in basic formats where appropriate.

This feature is **not** intended to replace Decent Bench. Decent Bench remains the full DecentDB IDE, similar in spirit to SQL Server Management Studio. The built-in Web Console is a small, local-first, self-contained convenience interface for quick inspection, debugging, automation, and simple query execution.

The Web Console must be implemented using **plain HTML5, CSS3, and vanilla JavaScript**, with no runtime dependency on CDNs, external fonts, external JavaScript, external CSS frameworks, telemetry, or internet access.

---

## 2. Product Positioning

### 2.1 Product Surface Separation

| Surface | Purpose |
|---|---|
| DecentDB Embedded Engine | Core storage engine, SQL execution, transactions, type system, and file format |
| `decentdb` CLI | Terminal workflows: create, query, import/export, inspect, backup, serve |
| `decentdb serve` Web Console | Lightweight browser UI for quick local inspection and ad hoc queries |
| Decent Bench IDE | Full DecentDB IDE for advanced database development and administration |

### 2.2 Positioning Statement

> DecentDB is embedded-first, but includes an optional CLI-hosted HTTP server and lightweight Web Console for local inspection, scripting, helper processes, edge workloads, and quick database exploration.

### 2.3 Important Product Boundary

The built-in Web Console must **not** become Decent Bench in a browser.

It should be:

- Slim.
- Local-first.
- Fast to start.
- Easy to reason about.
- Safe by default.
- Useful for quick inspection and query execution.

It should not attempt to provide full IDE capabilities, complex database administration workflows, rich object designers, migration tooling, project management, or advanced visual debugging.

---

## 3. Problem Statement

Users need a simple way to inspect and query a DecentDB database file without writing code, installing Decent Bench, or embedding DecentDB in an application.

Common situations include:

- Inspecting a `.ddb` file during development.
- Debugging an embedded application.
- Running one-off SQL queries.
- Viewing schema information quickly.
- Sharing a database file with another developer or support engineer.
- Running DecentDB as a small helper process for scripts or automation.
- Exposing a local HTTP interface to edge functions, local tools, BI helpers, or sidecar processes.

Today, users may expect a command like:

```bash
decentdb serve --db ./mydata.ddb
```

to start a local server and provide a browser UI at:

```text
http://localhost:7373
```

This expectation is reasonable and should be supported.

---

## 4. Goals

### 4.1 Primary Goals

1. Provide a simple `decentdb serve --db <file>.ddb` command.
2. Start a local HTTP server bound to `127.0.0.1:7373` by default.
3. Serve a lightweight browser-based Web Console at `/`.
4. Expose a small JSON-over-HTTP API under `/api/v1`.
5. Allow users to inspect database metadata and schema.
6. Allow users to run SQL queries and view results.
7. Support read-only mode for safe inspection.
8. Require no internet access at runtime.
9. Require no CDN, external fonts, third-party hosted assets, or telemetry.
10. Keep HTTP/server concerns outside the DecentDB core engine.
11. Protect read-write localhost sessions with transparent, ephemeral auth.

### 4.2 Secondary Goals

1. Make the Web Console visually modern and pleasant despite being lightweight.
2. Provide useful keyboard shortcuts for query execution.
3. Support local query history using browser storage.
4. Provide simple export options such as CSV.
5. Provide clean structured error messages.
6. Provide logging suitable for troubleshooting.
7. Provide a path for future remote access, sidecar, and automation use cases.

---

## 5. Non-Goals

The built-in Web Console shall not attempt to provide:

- A full DecentDB IDE.
- Replacement functionality for Decent Bench.
- Visual schema designer.
- Advanced object editing.
- Multi-database workspaces.
- Project-based database development.
- Migration project management.
- Advanced debugging tools.
- Advanced query plan visualization.
- Role/user administration.
- Enterprise RBAC.
- Public internet production hosting by default.
- PostgreSQL/MySQL wire protocol compatibility.
- Distributed database behavior.
- Built-in identity provider functionality.
- Long-lived remote transactions in v1.
- External BI-driver compatibility in v1.
- Plugin marketplace or extension management UI in v1.

---

## 6. Target Users

### 6.1 Embedded Application Developer

A developer building an application using DecentDB as an embedded database.

Needs:

- Open an application `.ddb` file.
- Inspect tables and columns.
- Run quick queries.
- Verify application state.
- Debug local data issues.

### 6.2 CLI / Automation User

A user who prefers scripting and terminal workflows but wants occasional browser inspection.

Needs:

- Start a quick local server.
- Use browser UI for ad hoc queries.
- Use HTTP API from scripts.
- Avoid installing a full IDE.

### 6.3 Support / Troubleshooting User

A developer or support engineer analyzing a customer-provided `.ddb` file.

Needs:

- Open a database copy in read-only mode.
- Inspect schema and row samples.
- Run diagnostic SQL.
- Avoid accidental mutation.

### 6.4 Edge / Sidecar User

A user who wants DecentDB available as a small HTTP-accessible helper process.

Needs:

- Run DecentDB as a lightweight process.
- Use JSON-over-HTTP requests.
- Avoid native language bindings in every client.
- Keep deployment small and simple.

---

## 7. Core User Stories

### 7.1 Start Server

As a user, I want to run:

```bash
decentdb serve --db ./app.ddb
```

So that I can open a browser at `http://localhost:7373` and inspect the database.

### 7.2 View Database Overview

As a user, I want to see basic database information so that I know I opened the correct file.

### 7.3 Browse Schema

As a user, I want to browse tables, views, indexes, and constraints so that I can understand the database structure.

### 7.4 Inspect Table Details

As a user, I want to click a table and see its columns, data types, nullability, indexes, constraints, and create SQL.

### 7.5 Run SQL Query

As a user, I want to type SQL into a simple editor and run it so that I can inspect or manipulate data.

### 7.6 View Query Results

As a user, I want query results displayed in a readable grid so that I can quickly understand the output.

### 7.7 Read-Only Inspection

As a user, I want to run:

```bash
decentdb serve --db ./prod-copy.ddb --read-only
```

So that I can safely inspect a database without risk of modifying it.

### 7.8 Offline / Firewall-Safe Use

As an enterprise user, I want the Web Console to work with no internet access and behind restrictive corporate tooling such as Zscaler so that the console works in locked-down environments.

---

## 8. CLI Requirements

### 8.1 Primary Command

```bash
decentdb serve --db ./app.ddb
```

### 8.2 Positional Convenience Form

The CLI may also support:

```bash
decentdb serve ./app.ddb
```

This is shorthand for:

```bash
decentdb serve --db ./app.ddb
```

### 8.3 Recommended Options

```bash
decentdb serve --db ./app.ddb   --host 127.0.0.1   --port 7373   --read-only
```

### 8.4 Advanced Options

```bash
decentdb serve --db ./app.ddb   --host 0.0.0.0   --port 7373   --token-env DECENTDB_TOKEN   --max-result-rows 1000   --query-timeout 30s   --max-body-size 4mb
```

### 8.5 Auto-Open Option

```bash
decentdb serve --db ./app.ddb --open
```

When provided, the CLI should attempt to open the default browser at:

```text
http://localhost:7373
```

### 8.6 Startup Output

Example:

```text
DecentDB server started

Database:  ./app.ddb
Mode:      read-write
Web UI:    http://localhost:7373
HTTP API:  http://localhost:7373/api/v1
Binding:   127.0.0.1:7373
Access:    local browser session

Press Ctrl+C to stop.
```

For read-only mode:

```text
DecentDB server started

Database:  ./prod-copy.ddb
Mode:      read-only
Web UI:    http://localhost:7373
HTTP API:  http://localhost:7373/api/v1
Binding:   127.0.0.1:7373
Access:    local browser session

Press Ctrl+C to stop.
```

### 8.7 Default Behavior

| Setting | Default |
|---|---:|
| Host | `127.0.0.1` |
| Port | `7373` |
| Web Console | Enabled |
| HTTP API | Enabled |
| CORS | Disabled |
| Telemetry | Disabled / not present |
| External assets | Not allowed |
| Query result limit | Enabled |
| Query timeout | Enabled |
| Auth for localhost | Transparent ephemeral token |
| Auth for non-localhost | Required; no unauthenticated remote bind |

### 8.8 Transparent Local Authentication

By default, `decentdb serve` shall generate an ephemeral per-process bearer
token and inject it into the launched Web Console session. This protection is
transparent for normal browser use. Users do not need to configure or
understand authentication for the default localhost workflow.

Recommended behavior:

- `decentdb serve --db ./app.ddb --open` opens a local URL containing a
  short-lived bootstrap token.
- The Web Console exchanges or stores that token in browser session scope.
- The Web Console sends `Authorization: Bearer <token>` for API requests.
- The startup output describes this as `Access: local browser session` rather
  than exposing authentication jargon.
- The token is not printed by default.
- `--show-token` may print the token for debugging.

For scripts and automation:

```bash
decentdb serve --db ./app.ddb --token-env DECENTDB_TOKEN
```

For localhost-only debugging, an explicit `--no-auth` option may be provided.
`--no-auth` must not be accepted with non-localhost binding.

---

## 9. Web Console UX Requirements

### 9.1 Layout

The Web Console should use a simple, efficient layout:

```text
┌─────────────────────────────────────────────────────────────┐
│ DecentDB Console     app.ddb       READ-WRITE    localhost  │
├───────────────┬─────────────────────────────────────────────┤
│ Schema        │ SQL Editor                                  │
│               │ SELECT * FROM users LIMIT 100;              │
│ Tables        ├─────────────────────────────────────────────┤
│  users        │ Results                                     │
│  orders       │ id | email | created_at                     │
│  audit_log    │ ...                                         │
└───────────────┴─────────────────────────────────────────────┘
```

### 9.2 Top Bar

The top bar should display:

- Product name: DecentDB Console.
- Database file name.
- Current mode: read-only or read-write.
- Host/port.
- Server status.
- Optional theme toggle.

### 9.3 Schema Sidebar

The left sidebar should display schema objects grouped by type:

```text
Tables
  users
  orders
  audit_log

Views
  active_users

Indexes
  idx_users_email
```

The sidebar should support:

- Expand/collapse groups.
- Click table to inspect.
- Search/filter if schema is large.
- Visual count indicators where useful.

### 9.4 Object Detail Panel

When a user selects a table, the detail panel should show:

- Table name.
- Columns.
- Data types.
- Nullability.
- Defaults.
- Primary key.
- Foreign keys.
- Indexes.
- Constraints.
- Row count or estimated row count when available.
- Create SQL, if available.

### 9.5 Query Editor

The query editor should initially be a simple `<textarea>`.

Minimum features:

- Monospace font.
- Large enough editing area.
- Run button.
- Clear button.
- Ctrl+Enter / Cmd+Enter to run.
- Optional default starter query.
- Preserve query text during execution errors.

Future enhancement:

- Replace or augment textarea with a bundled local code editor only if justified.
- If a richer editor is added, it must still be served locally with no CDN dependency.

### 9.6 Query Results

The result area should display:

- Column names.
- Column data types.
- Rows.
- Row count.
- Query elapsed time.
- Error messages when applicable.
- Truncation notice when result limit is applied.

Minimum grid behavior:

- Horizontal scrolling.
- Sticky column header if feasible.
- Reasonable cell truncation.
- Copy result option.
- Export CSV option.

### 9.7 Query History

The console should support lightweight local query history.

Requirements:

- Stored in browser `localStorage`.
- Never sent to external services.
- Clear history option.
- Bounded number of saved queries.
- Per-browser and per-origin behavior is acceptable.

### 9.8 Visual Style

The console should look modern and polished while remaining slim.

Recommended style characteristics:

- CSS variables for theme tokens.
- Light and dark themes.
- Compact spacing.
- Rounded panels.
- Subtle shadows.
- Clear status badges.
- Good contrast.
- Monospace treatment for SQL and result values.
- Responsive enough for common laptop and desktop browser sizes.

---

## 10. Asset and Dependency Policy

### 10.1 Hard Requirement

The DecentDB Web Console must be fully self-contained.

It must not require:

- Public CDNs.
- External fonts.
- External CSS frameworks.
- External JavaScript libraries.
- Remote icon libraries.
- Runtime NPM package resolution.
- Internet access.
- Telemetry endpoints.
- Analytics scripts.
- Remote license checks.

### 10.2 Runtime Network Policy

At runtime, the Web Console may only make same-origin requests to the DecentDB server, such as:

```text
/
 /assets/console.css
 /assets/console.js
 /api/v1/info
 /api/v1/schema
 /api/v1/query
```

It must not call external domains.

### 10.3 Enterprise Firewall Requirement

The console must function correctly:

- Offline.
- Behind Zscaler.
- Behind corporate TLS inspection.
- In containers.
- On isolated lab networks.
- On development machines without internet access.
- On machines where public package/CDN hosts are blocked.

### 10.4 Recommended Static Asset Structure

```text
web-console/
  index.html
  assets/
    console.css
    console.js
    icons.svg
```

These assets should be embedded into the DecentDB CLI binary or served from a local installation directory.

### 10.5 Frontend Technology Requirement

The v1 Web Console shall be implemented with:

- HTML5.
- CSS3.
- Vanilla JavaScript.
- Browser-native APIs such as `fetch`, `localStorage`, `dialog`, `details`, `table`, `form`, and `textarea`.

The v1 Web Console shall not use:

- React.
- Vue.
- Svelte.
- Angular.
- Tailwind runtime or build pipeline.
- Bootstrap CDN.
- htmx CDN.
- CodeMirror CDN.
- Tabulator CDN.
- Google Fonts.
- Font Awesome CDN.
- Any third-party runtime CDN asset.

Bundled third-party libraries may be reconsidered later only if they are vendored, pinned, license-reviewed, and served locally.

### 10.6 Backend Dependency Policy

The v1 server should avoid adding a broad async web framework or runtime. Prefer
standard-library networking plus small workspace-local helpers. Any new Rust
dependency for HTTP parsing, routing, or browser launching must be justified in
the implementation PR, pinned in `Cargo.lock`, license-reviewed, and usable
offline after checkout.

---

## 11. HTTP API Requirements

The Web Console should consume the same local JSON API available to scripts and helper processes.

### 11.1 Base Path

```text
/api/v1
```

### 11.2 Required Endpoints

```text
GET  /healthz
GET  /readyz
GET  /api/v1/info
GET  /api/v1/schema
GET  /api/v1/tables
GET  /api/v1/tables/{tableName}
POST /api/v1/sql
```

### 11.3 Optional UI-Only Routes

If the UI is server-rendered or partially server-rendered later, routes may exist under:

```text
/ui/*
```

For v1, a JSON API plus vanilla JavaScript rendering is preferred.

---

## 12. API Contract

### 12.1 Database Info

Request:

```http
GET /api/v1/info
```

Example response:

```json
{
  "database": {
    "fileName": "app.ddb",
    "path": "./app.ddb",
    "sizeBytes": 1048576,
    "readOnly": false
  },
  "server": {
    "version": "0.0.0",
    "startedAt": "2026-05-19T10:00:00Z",
    "uptimeSeconds": 120
  },
  "engine": {
    "version": "0.0.0",
    "fileFormatVersion": 1
  }
}
```

### 12.2 Schema

Request:

```http
GET /api/v1/schema
```

Example response:

```json
{
  "tables": [
    {
      "name": "users",
      "columns": [
        {
          "name": "id",
          "type": "INT64",
          "nullable": false,
          "primaryKey": true
        },
        {
          "name": "email",
          "type": "TEXT",
          "nullable": false
        }
      ]
    }
  ],
  "views": [],
  "indexes": [
    {
      "name": "idx_users_email",
      "table": "users",
      "columns": ["email"],
      "unique": true
    }
  ]
}
```

### 12.3 SQL Execution

Request:

```http
POST /api/v1/sql
Content-Type: application/json
```

Body:

```json
{
  "sql": "SELECT id, email FROM users WHERE status = $1 LIMIT 100",
  "params": ["active"],
  "readonly": false
}
```

Response:

```json
{
  "results": [
    {
      "columns": [
        { "name": "id", "type": "INT64" },
        { "name": "email", "type": "TEXT" }
      ],
      "rows": [
        [1, "steven@example.com"]
      ],
      "rowCount": 1,
      "rowsAffected": 1,
      "truncated": false
    }
  ],
  "elapsedMs": 3,
  "truncated": false
}
```

The SQL endpoint should use DecentDB's existing statement-batch semantics.
Clients may submit one SQL statement or a semicolon-separated statement batch.
This keeps v1 simple and avoids separate query/execute/batch endpoints until
real usage justifies them.

---

## 13. Error Handling

### 13.1 Standard Error Shape

All API errors should use a stable shape:

```json
{
  "error": {
    "code": "SQL_SYNTAX_ERROR",
    "message": "Unexpected token near FROM",
    "details": {
      "line": 1,
      "column": 15
    }
  }
}
```

### 13.2 Recommended Error Codes

```text
SQL_SYNTAX_ERROR
CONSTRAINT_VIOLATION
TYPE_MISMATCH
DATABASE_BUSY
READ_ONLY
AUTH_REQUIRED
AUTH_INVALID
REQUEST_TOO_LARGE
QUERY_TIMEOUT
RESULT_LIMIT_EXCEEDED
OBJECT_NOT_FOUND
INVALID_REQUEST
INTERNAL_ERROR
```

### 13.3 UI Error Display

The Web Console should display:

- Error code.
- Human-readable message.
- SQL location when available.
- Details in expandable form.
- No raw panic dumps.
- No sensitive filesystem details unless appropriate for local-only mode.

---

## 14. Data Type Serialization

The HTTP API must preserve DecentDB type metadata in result columns.

### 14.1 Default Encoding

| DecentDB Type | JSON Encoding |
|---|---|
| INT64 / INTEGER | number |
| REAL / DOUBLE | number |
| TEXT | string |
| BOOLEAN | boolean |
| DATE | ISO string |
| TIME | ISO string |
| TIMESTAMP | ISO string |
| TIMESTAMPTZ | ISO string |
| UUID | string |
| IPADDR | string |
| CIDR | string |
| MACADDR / MACADDR8 | string |
| ENUM | string |
| DECIMAL | string |
| GEOMETRY / GEOGRAPHY | hex EWKB string |
| BLOB | base64 string or future binary endpoint |

### 14.2 Column Metadata

Every query response should include column metadata:

```json
{
  "columns": [
    {
      "name": "email",
      "type": "TEXT"
    }
  ]
}
```

This allows the Web Console to display useful type information and future clients to preserve type awareness.

---

## 15. Security Requirements

### 15.1 Safe Defaults

By default:

- Bind to `127.0.0.1`.
- Disable CORS.
- Do not expose publicly.
- Enforce request size limits.
- Enforce query timeout.
- Enforce result row limit.
- Serve only local embedded assets.
- Do not include telemetry.

### 15.2 Remote Binding

If the user binds to a non-localhost address:

```bash
decentdb serve --db ./app.ddb --host 0.0.0.0
```

Then bearer token configuration is required. Do not permit unauthenticated
non-localhost binding in v1.

Recommended token form:

```bash
decentdb serve --db ./app.ddb --host 0.0.0.0 --token-env DECENTDB_TOKEN
```

### 15.3 Authentication

For v1, bearer token authentication is sufficient. Localhost sessions use an
ephemeral token by default, but the CLI should keep this transparent for normal
browser use.

Request:

```http
Authorization: Bearer <token>
```

### 15.4 CORS

CORS should be disabled by default.

Optional future flag:

```bash
decentdb serve --db ./app.ddb --cors-origin http://localhost:3000
```

Do not support permissive `*` CORS by default.

### 15.5 Read-Only Mode

When `--read-only` is enabled:

- Mutating statements must be rejected.
- `/api/v1/sql` should reject mutation requests.
- The UI should visibly display read-only mode.
- Mutating UI actions should be disabled or rejected.

---

## 16. Query Safety and Resource Limits

### 16.1 Required Limits

The server should support configurable limits:

```text
max result rows
query timeout
max request body size
max concurrent requests
database busy timeout
```

### 16.2 Default Limits

Recommended starting defaults:

| Limit | Default |
|---|---:|
| Max result rows | 1,000 |
| Query timeout | 30 seconds |
| Max request body size | 4 MB |
| Max concurrent requests | 32 |
| Busy timeout | 5 seconds |

### 16.3 Result Truncation

If a query exceeds the max result rows, the response should indicate truncation:

```json
{
  "rowCount": 1000,
  "truncated": true,
  "limit": 1000
}
```

The UI should display a clear message:

```text
Results truncated at 1,000 rows.
```

For v1, the result limit is a server response/display limit. The API must not
return more than the configured maximum number of rows to the browser. It may
still execute the underlying SQL normally unless the engine exposes a safe
bounded-execution API for the specific statement. The UI should encourage
explicit `LIMIT` clauses for large tables.

---

## 17. Architecture Requirements

### 17.1 Layering

The server must remain a CLI/server wrapper and must not introduce HTTP concerns into the core engine.

Recommended architecture:

```text
decentdb-core
  Storage engine
  SQL parser/executor
  Type system
  Transactions
  Schema introspection APIs

decentdb-cli
  Command parsing
  serve command

decentdb-server
  HTTP server
  API routing
  Auth middleware
  Static asset serving
  Request limits
  Response serialization

decentdb-web-console
  HTML
  CSS
  Vanilla JavaScript
  Static local assets
```

### 17.2 Core Engine Boundary

The core engine should expose clean APIs such as:

```rust
database.schema()
database.query(sql, params)
database.execute(sql, params)
database.table_info(name)
database.indexes()
database.constraints()
```

The HTTP server consumes these APIs.

The core engine should not know about:

- HTTP.
- HTML.
- JSON API routing.
- Browser UI.
- Static assets.
- Authentication headers.
- CORS.
- Web Console sessions.

### 17.3 Static Asset Serving

Static assets should be embedded in the CLI binary or included in the local installation package.

Requests:

```text
GET /
GET /assets/console.css
GET /assets/console.js
GET /assets/icons.svg
```

All should resolve locally.

---

## 18. Observability and Logging

### 18.1 Server Logs

The server should log:

- Startup configuration.
- Database path.
- Bind address.
- Read-only/read-write mode.
- Request method/path/status.
- Query elapsed time.
- Query errors.
- Server shutdown.

### 18.2 Sensitive Logging

Do not log full SQL or parameters by default if there is risk of sensitive data exposure.

Recommended modes:

```bash
--log-level info
--log-format text
--log-format json
--log-sql off|summary|full
```

Default:

```text
log-sql: summary
```

### 18.3 Metrics

Metrics are optional for v1.

Future endpoint:

```text
GET /metrics
```

Only if DecentDB has a defined metrics strategy.

---

## 19. Accessibility Requirements

The Web Console should support:

- Keyboard navigation for primary actions.
- Visible focus states.
- Sufficient color contrast.
- Semantic HTML structure.
- Labels for form fields.
- Non-color-only status indicators.
- Reasonable screen reader behavior for major sections.

Minimum keyboard shortcuts:

| Shortcut | Action |
|---|---|
| Ctrl+Enter / Cmd+Enter | Run query |
| Escape | Close dialogs / overlays |
| Tab / Shift+Tab | Navigate controls |

---

## 20. Browser Support

The Web Console should support current versions of:

- Chromium-based browsers.
- Firefox.
- Safari where feasible.
- Microsoft Edge.

Because the Web Console is local and simple, broad modern-browser support should be achievable without transpilation.

Avoid cutting-edge browser APIs unless gracefully degraded.

---

## 21. Packaging Requirements

### 21.1 Binary Distribution

The Web Console assets should be included with the DecentDB CLI distribution.

Preferred:

- Embed assets into the CLI binary.

Acceptable:

- Include assets in a local installation directory and serve from there.

Not acceptable:

- Fetching assets from internet at runtime.
- Requiring `npm install` for end users.
- Requiring a frontend dev server.
- Requiring CDN access.

### 21.2 Development Workflow

The Web Console should avoid a required frontend build pipeline for v1.

Recommended:

```text
index.html
console.css
console.js
```

No runtime dependency manager required.

---

## 22. Implementation Phases

### Phase 1: Minimal Local Server + Web Console

Deliverables:

- `decentdb serve --db <file>.ddb`.
- Bind to `127.0.0.1:7373`.
- Serve `/`.
- Serve local static assets.
- `GET /healthz`.
- `GET /readyz`.
- `GET /api/v1/info`.
- `GET /api/v1/schema`.
- `POST /api/v1/query`.
- Basic Web Console layout.
- Schema sidebar.
- Query textarea.
- Results table.
- Structured error display.
- Query timeout.
- Result row limit.
- No external assets.

### Phase 2: Safety and Polish

Deliverables:

- `--read-only`.
- `--open`.
- `--host`.
- `--port`.
- `--max-result-rows`.
- `--query-timeout`.
- `--max-body-size`.
- Query history in `localStorage`.
- CSV export.
- Improved table details.
- Theme toggle.
- Better empty/error/loading states.

### Phase 3: Remote / Sidecar Hardening

Deliverables:

- `--token-env`.
- Auth middleware.
- Remote bind safety checks.
- Optional CORS configuration.
- JSON log mode.
- Graceful shutdown.
- Concurrent request limits.
- Better API docs.

### Phase 4: Advanced Inspection

Deliverables:

- Additional schema introspection.
- Index details.
- Constraint details.
- View definitions.
- Trigger/function details if applicable.
- Query plan text output if engine supports it.
- Optional NDJSON result streaming.

---

## 23. Acceptance Criteria

### 23.1 CLI

- Running `decentdb serve --db ./app.ddb` starts a server.
- The server binds to `127.0.0.1:7373` by default.
- Startup output shows database, mode, Web UI URL, API URL, and bind address.
- Ctrl+C shuts the server down cleanly.

### 23.2 Web Console

- Opening `http://localhost:7373` loads the Web Console.
- The Web Console displays database metadata.
- The Web Console displays schema objects.
- Selecting a table displays table details.
- Users can run a SQL query.
- Results display in a table.
- Query errors display clearly.
- The UI remains usable without internet access.

### 23.3 Asset Policy

- Browser dev tools show no requests to third-party domains.
- No CDN-hosted CSS, JS, fonts, icons, or images are required.
- Disconnecting the machine from the internet does not break the Web Console.
- The console works behind restrictive enterprise firewall/proxy setups such as Zscaler.
- The console sends no telemetry.

### 23.4 Read-Only Mode

- Running `decentdb serve --db ./app.ddb --read-only` opens the database read-only.
- The UI visibly displays read-only mode.
- Mutation attempts are rejected with a structured `READ_ONLY` error.
- The database file is not modified by read-only usage.

### 23.5 API

- `/api/v1/info` returns database and server metadata.
- `/api/v1/schema` returns schema metadata.
- `/api/v1/query` returns columns, rows, row count, elapsed time, and truncation status.
- Errors use the standard structured error shape.
- Query limits are enforced.

---

## 24. Risks and Mitigations

### Risk: Web Console grows into full Decent Bench replacement

Mitigation:

- Explicitly define non-goals.
- Keep advanced IDE features in Decent Bench.
- Position Web Console as lightweight inspection/query tool.

### Risk: Security concerns from exposing database over HTTP

Mitigation:

- Bind to localhost by default.
- Require auth or explicit unsafe override for remote bind.
- Disable CORS by default.
- Support read-only mode.
- Enforce request and query limits.

### Risk: Browser renders too many rows and becomes unusable

Mitigation:

- Enforce max result row limits.
- Display truncation status.
- Encourage `LIMIT`.
- Add streaming later if needed.

### Risk: Enterprise users cannot load CDN assets

Mitigation:

- No CDN dependencies.
- Embed all assets.
- No external network calls.
- Test offline and behind restrictive proxies.

### Risk: Frontend dependency creep

Mitigation:

- Use HTML5, CSS3, and vanilla JS for v1.
- Require explicit approval for any third-party library.
- Vendor and license-review any future dependency.
- No runtime external assets.

---

## 25. Open Questions

1. Should `decentdb serve` default to read-write or read-only?
2. Should mutation queries require an explicit UI confirmation in read-write mode?
3. Should the Web Console have a default starter query?
4. Should SQL query history be enabled by default?
5. Should query history be per database file or global per browser origin?
6. Should the server expose `/api/v1/execute` in v1, or should v1 only support query execution through one endpoint?
7. Should remote binding require a token always, or allow an explicit `--unsafe-no-auth` option?
8. Should CSV export be generated client-side or server-side?
9. Should the Web Console show full database file paths or only file names by default?
10. Should the Web Console include a visible “Use Decent Bench for advanced workflows” link or hint?

---

## 26. Recommended Initial Decision Set

For the first implementation, make these decisions:

```text
Default host: 127.0.0.1
Default port: 7373
Default mode: read-write, with visible banner
Read-only flag: supported
Frontend: HTML5 + CSS3 + vanilla JS
External assets: prohibited
Telemetry: prohibited
Query editor: textarea
Results grid: native HTML table
Query history: localStorage
Result limit: 1,000 rows
Query timeout: 30 seconds
Remote auth: required for non-localhost bind
Decent Bench overlap: explicitly avoided
```

---

## 27. Summary

`decentdb serve` should provide a lightweight, local-first HTTP server and Web Console that makes DecentDB easier to inspect, debug, and automate.

The feature should satisfy the natural user expectation:

```bash
decentdb serve --db ./app.ddb
```

Then:

```text
http://localhost:7373
```

The Web Console should provide schema browsing and ad hoc query execution without becoming a full IDE.

The most important design constraint is that the console must be slim, self-contained, and enterprise-friendly:

```text
No CDN.
No external fonts.
No external JavaScript.
No telemetry.
No internet requirement.
Works behind restrictive firewalls and tools such as Zscaler.
```

This gives DecentDB a practical, polished developer experience while preserving the embedded-first architecture and leaving full power-user workflows to Decent Bench.
