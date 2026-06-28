# DecentDB vs PGlite: When to Choose Which

This document helps developers decide between **DecentDB** and **PGlite** for
local, embedded, browser, and JavaScript-first database workloads. Both can
place a relational database inside an application process, but they optimize
for very different ecosystems.

> **Versions compared:** DecentDB 2.15.0 workspace behavior vs public PGlite
> documentation as of 2026-06-28.
>
> **Scope note:** PGlite is a WASM build of Postgres packaged as a
> TypeScript/JavaScript client library. This page compares the product shape
> and developer fit, not the full PostgreSQL server project.
>
> **See also:** [DecentDB vs SQLite](decentdb-vs-sqlite.md),
> [DecentDB vs DuckDB](decentdb-vs-duckdb.md), [SQL Feature
> Matrix](sql-feature-matrix.md), and [SQL Reference](sql-reference.md).

## They Solve the "Local SQL" Problem Differently

PGlite and DecentDB are both attractive when a developer wants SQL without
running a separate database server. From there, the paths diverge:

- **PGlite** brings Postgres into JavaScript through WebAssembly. It is
  strongest when the application is browser/Node/Bun/Deno-centered, when
  Postgres compatibility matters, or when Postgres extensions such as pgvector
  or PostGIS are important.
- **DecentDB** is a Rust-native embedded database with a stable C ABI and
  maintained bindings. It is strongest when the application needs durable local
  ACID data, multi-language embedding, branch/diff workflows, local-first sync
  inspection, policies/masks, and a small standalone engine that is not trying
  to be Postgres.

The short version:

- Choose **PGlite** when you want "Postgres inside JavaScript."
- Choose **DecentDB** when you want "a durable embedded application database
  with DecentDB-native workflows and bindings."

## At a Glance

| Dimension | DecentDB | PGlite |
|-----------|----------|--------|
| **Core identity** | Rust-native embedded relational database | WASM Postgres packaged as a TypeScript/JavaScript library |
| **Primary ecosystem** | Rust plus C ABI bindings for Python, .NET, Go, Java, Node.js, Dart, C/C++ | Browser, Node.js, Bun, Deno, JavaScript/TypeScript frameworks |
| **SQL direction** | Practical Postgres-like subset with DecentDB-specific features | PostgreSQL compatibility through a WASM Postgres build |
| **Persistence model** | Native `.ddb` file; browser/WASM support through DecentDB surfaces | In-memory, native filesystem in server runtimes, IndexedDB in browsers, filesystem adapters |
| **Default durability posture** | WAL + fsync-on-commit by default for native files | Depends on filesystem/runtime; browser IndexedDB mode can trade durability timing for responsiveness |
| **Concurrency model** | One writer, many readers; local native cross-process WAL coordination when supported | Embedded single-user Postgres-in-WASM shape; JS runtime and worker setup matter |
| **Extensions** | Sandboxed Lua packages; native full-text/trigram search | Postgres extension catalog and PGlite plugins, including pgvector/PostGIS support in documented extension surfaces |
| **Local-first sync** | Built-in sync journal, scopes, conflicts, relay/dev transport, SQL/CLI inspection | Electric/PGlite sync integrations and live-query primitives |
| **Reactive queries** | In-process reactive subscriptions in DecentDB API surfaces | PGlite live-query extension for JavaScript applications |
| **Branches / snapshots** | Built-in local snapshots, branches, diff, guarded restore, constrained merge | Use application, filesystem, or Postgres-compatible dump/restore patterns |
| **Security model** | TDE, row policies, projection masks, audit context | Postgres-derived behavior plus application/runtime controls |
| **Best fit** | Durable embedded app data across languages | Postgres-like local data in JS/browser apps |

## When DecentDB Is the Better Fit

### 1. You need a native embedded database across languages

DecentDB exposes one engine through a stable C ABI and maintained language
bindings:

- Rust;
- C/C++;
- Python;
- .NET;
- Go;
- Java;
- Node.js;
- Dart;
- Web/WASM surfaces.

That matters when the database has to be the same local data component across
desktop, CLI, server, mobile-adjacent, and backend services.

PGlite is intentionally JavaScript/TypeScript-centered. That is a strength when
your application is JS-first, but it is a narrower embedding story.

### 2. You want durable native-file commits by default

DecentDB is built around durable local writes. For native files, commits are
WAL-backed and fsynced before returning unless the application explicitly
selects a relaxed open-time sync mode.

```sql
BEGIN;
INSERT INTO audit_log (actor_id, action) VALUES ($1, $2);
COMMIT; -- durable under default native-file settings
```

PGlite's durability depends on the runtime and filesystem adapter. In browser
IndexedDB mode, PGlite documents that the IndexedDB filesystem loads database
files into memory and flushes changed files back to IndexedDB; it also offers a
relaxed durability mode that returns query results before the flush completes.
That can be the right tradeoff for responsive browser apps, but it is a
different default risk model than DecentDB's native durable commit path.

### 3. You want local branch, diff, and guarded restore workflows

DecentDB treats snapshots and branches as database workflows:

```bash
decentdb snapshot create --db local.ddb --name before-sync
decentdb branch create --db local.ddb --name sync-test --from before-sync
decentdb branch diff --db local.ddb --left main --right sync-test --format json
decentdb branch restore --db local.ddb --branch main --snapshot before-sync --dry-run
```

PGlite users can use Postgres-compatible dumps, filesystem copies, or
application-level versioning. DecentDB's value is that branch/diff/restore are
named local database features.

### 4. You need built-in sync inspection and conflict workflows

DecentDB exposes local-first sync state through SQL and CLI:

```sql
SELECT * FROM sys_sync_status;
SELECT * FROM sys_sync_journal ORDER BY sequence DESC LIMIT 20;
SELECT * FROM sys_sync_conflicts;
SELECT * FROM sys_sync_retention_report;
```

PGlite has strong local-first adjacency through Electric sync integrations and
live queries. DecentDB's distinction is not "has sync, PGlite does not"; it is
that DecentDB stores sync journal, conflict, scope, peer, and retention state as
first-class local database surfaces.

### 5. You need engine-enforced row policies and projection masks

DecentDB includes local policy and masking features:

```sql
SET AUDIT CONTEXT tenant_id = 'tenant-a';

CREATE POLICY tenant_scope ON invoices
USING tenant_id = current_tenant();

CREATE MASK email_mask ON users(email)
USING left(email, 2) || '***';
```

PGlite gives you Postgres-style SQL compatibility in an embedded WASM context,
but application-level authorization and browser/runtime controls still matter.
Choose DecentDB when local policy/mask enforcement is a product requirement.

### 6. You prefer a small DecentDB SQL surface over full Postgres behavior

Full Postgres compatibility is powerful, but it also brings Postgres semantics,
types, extension behavior, planner behavior, and migration expectations.
DecentDB intentionally exposes a smaller application-focused surface.

```sql
SELECT DISTINCT ON (account_id)
       account_id, event_id, created_at
FROM account_events
ORDER BY account_id, created_at DESC;
```

If you want the embedded database to be simpler than Postgres while retaining
useful Postgres-style syntax, DecentDB is the more direct fit.

### 7. You need deterministic engine-level crash testing hooks

DecentDB's engineering posture includes FaultyVFS and WAL failpoint testing for
crash and torn-write behavior. This is useful for products where local storage
durability is a correctness requirement, not an implementation detail.

PGlite benefits from the PostgreSQL lineage and the surrounding Electric
ecosystem, but its WASM and browser persistence behavior should still be tested
against the target runtime and filesystem mode.

## When PGlite Is the Better Fit

### 1. You want Postgres in the browser or JavaScript runtime

This is PGlite's clearest strength:

```ts
import { PGlite } from "@electric-sql/pglite";

const db = new PGlite("idb://my-pgdata");
const result = await db.query("select now() as current_time");
```

If your application is a web app, Electron app, local-first JavaScript app, or
Node/Bun/Deno tool that wants Postgres semantics without a separate server,
PGlite is specifically built for that experience.

### 2. You need closer PostgreSQL compatibility

If the application already has a Postgres schema, migrations, SQL, types, or
ORM stack, PGlite may reduce the dialect gap. Many tools can reason about
"Postgres-like" behavior more easily than a new embedded engine.

DecentDB intentionally borrows useful Postgres syntax, but it is not a
PostgreSQL server and does not claim full Postgres compatibility.

### 3. You need Postgres extensions such as pgvector or PostGIS

PGlite documents support for many Postgres extensions, including pgvector and
PostGIS in its extension surface. That is a major differentiator.

DecentDB currently provides:

- B-tree indexes;
- full-text indexes;
- trigram indexes for substring search;
- expression, partial, and covering-index subsets;
- sandboxed Lua extension packages.

Vector search, geospatial depth, and broader Postgres extension compatibility
are future or out-of-scope DecentDB items.

### 4. You want live queries in a JS UI

PGlite's live-query extension lets JavaScript apps subscribe to query results
and react when underlying tables change.

That is especially attractive in browser UI frameworks where the database is
part of the client-side state model. DecentDB has reactive APIs, but PGlite's
JS-first design is a better fit when the UI/runtime ecosystem is the center of
the product.

### 5. You are already building on Electric/PGlite sync

PGlite fits naturally with Electric's local-first stack. If your architecture
is "cloud Postgres plus local PGlite shape sync," DecentDB would be a different
database and sync model rather than a drop-in replacement.

### 6. You want Postgres development tools and mental models

PGlite lets teams reuse Postgres knowledge:

- PostgreSQL SQL dialect expectations;
- extension concepts;
- migration tooling that targets Postgres;
- ORMs with PGlite/Postgres drivers;
- a familiar schema design model for teams already standardized on Postgres.

DecentDB is easier to justify when its native features are part of the product
requirements, not when the team simply wants local Postgres.

## Side-by-Side Examples

### Browser-local SQL

```ts
// PGlite: browser-local Postgres in IndexedDB.
import { PGlite } from "@electric-sql/pglite";

const db = new PGlite("idb://workspace");
await db.exec(`
  CREATE TABLE notes (id serial primary key, body text not null);
  INSERT INTO notes (body) VALUES ('hello from the browser');
`);
```

Prefer **PGlite** for a JavaScript-first browser app that wants Postgres
semantics and IndexedDB persistence.

### Native durable local workflow

```bash
# DecentDB: native local branch workflow around a durable .ddb file.
decentdb snapshot create --db app.ddb --name before-import
decentdb branch create --db app.ddb --name import-test --from before-import
decentdb branch diff --db app.ddb --left main --right import-test
```

Prefer **DecentDB** when branchable local data and conservative native commit
durability are central.

### Postgres extension dependency

```sql
-- PGlite/Postgres direction: extension-driven capability.
CREATE EXTENSION vector;
CREATE EXTENSION postgis;
```

Prefer **PGlite** when the requirement is compatibility with Postgres
extensions. Prefer **DecentDB** when DecentDB's built-in full-text/trigram
indexes, sync surfaces, and policy model are sufficient and simpler.

## Summary Decision Matrix

| Your situation | Recommendation |
|----------------|----------------|
| Need Postgres in browser/Node/Bun/Deno | **PGlite** |
| Need pgvector, PostGIS, or broad Postgres extension compatibility | **PGlite** |
| Need to reuse a Postgres ORM/migration stack | **PGlite** |
| Need Electric/PGlite live-query or sync architecture | **PGlite** |
| Need JavaScript-first local app state | **PGlite** |
| Need one embedded engine across Rust, C, Python, .NET, Go, Java, Node, Dart | **DecentDB** |
| Need durable native-file commits by default | **DecentDB** |
| Need local branch/diff/restore/time-travel workflows | **DecentDB** |
| Need sync journal/conflict/retention inspection in SQL/CLI | **DecentDB** |
| Need row policies, projection masks, and audit context locally | **DecentDB** |
| Prefer a focused embedded SQL engine over full Postgres semantics | **DecentDB** |

## Bottom Line

Pick **PGlite** when you want Postgres compatibility inside JavaScript,
especially in the browser or in a JS local-first stack.

Pick **DecentDB** when you want a durable, multi-language embedded application
database with DecentDB-native branch, sync, security, and diagnostics
workflows.

Neither choice is universally better. PGlite is the more natural local
Postgres choice; DecentDB is the more natural durable embedded application
database choice.

## External References

- [PGlite documentation](https://pglite.dev/docs/)
- [What is PGlite](https://pglite.dev/docs/about)
- [PGlite filesystems](https://pglite.dev/docs/filesystems)
- [PGlite live queries](https://pglite.dev/docs/live-queries)
- [PGlite extensions](https://pglite.dev/extensions/)
- [Electric PGlite sync page](https://electric.ax/sync/pglite)
