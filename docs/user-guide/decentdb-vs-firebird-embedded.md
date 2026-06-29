# DecentDB vs Firebird Embedded: When to Choose Which

This document helps developers decide between **DecentDB** and **Firebird
Embedded** for local SQL workloads. Both can be embedded into applications, but
they come from different database traditions.

> **Versions compared:** DecentDB 2.15.0 workspace behavior vs public
> Firebird Embedded and Firebird feature documentation as of 2026-06-28.
>
> **Scope note:** Firebird is a full SQL relational database system with server
> and embedded deployment options. This page focuses on the embedded deployment
> option, where the engine is linked into the application process through the
> Firebird client/API libraries.
>
> **See also:** [DecentDB vs SQLite](decentdb-vs-sqlite.md),
> [DecentDB vs H2](decentdb-vs-h2.md), [SQL Feature
> Matrix](sql-feature-matrix.md), and [SQL Reference](sql-reference.md).

## They Are Both SQL Engines, But Not the Same Kind of Embedded Product

Firebird Embedded is a full-featured Firebird engine packaged for local
embedding. Firebird documents the embedded server as having the same features
as the usual server, with local protocol constraints in embedded mode.

DecentDB is a newer Rust-native embedded database designed around local
application data, durable-by-default writes, multi-language bindings, local
branch/sync workflows, policies/masks, and a focused SQL surface.

The short version:

- Choose **Firebird Embedded** when you want the Firebird SQL engine,
  Firebird's stored procedure/trigger ecosystem, and a path between embedded
  and server-style Firebird deployments.
- Choose **DecentDB** when you want a small Rust-native embedded database with
  DecentDB-native branch, sync, policy, diagnostics, and binding workflows.

## At a Glance

| Dimension | DecentDB | Firebird Embedded |
|-----------|----------|-------------------|
| **Core identity** | Rust-native embedded relational database | Embedded mode of the Firebird SQL database engine |
| **Implementation** | Rust | C/C++ Firebird engine |
| **Primary use** | Local embedded app data with DecentDB-native workflows | Firebird-compatible local SQL engine, desktop/local deployments, demos, embedded variants |
| **SQL direction** | Practical Postgres-like application SQL subset | Firebird SQL with PSQL stored procedures/triggers |
| **Deployment shape** | Native library/CLI plus bindings | Firebird engine/client libraries packaged with application |
| **Server path** | Embedded database project; no DecentDB server product | Same engine family as Firebird server deployments |
| **Default durability posture** | WAL + fsync-on-commit by default for native files | Firebird transaction/durability model; configuration and engine mode matter |
| **Concurrency model** | One writer, many readers; local native cross-process WAL coordination when supported | Firebird multi-generational architecture and server/embedded concurrency behavior |
| **Procedural SQL** | Triggers and sandboxed Lua extension packages | Mature PSQL stored procedures and triggers |
| **Branches / snapshots** | Built-in local snapshots, branches, diff, guarded restore, constrained merge | Use Firebird backup/restore, transaction features, or application workflows |
| **Local-first sync** | Built-in sync journal, scopes, conflicts, relay/dev transport, SQL/CLI inspection | Not the core embedded Firebird product surface |
| **Security model** | TDE, row policies, projection masks, audit context | Firebird user/security model and engine features |
| **License** | MIT or Apache-2.0 | Firebird IDPL/IPL-style open-source licensing |

## When DecentDB Is the Better Fit

### 1. You want a lightweight embedded application database, not a server-derived engine

Firebird Embedded is powerful because it is Firebird. That can also make it
more database-server-like in packaging, dependencies, SQL dialect, tooling, and
operational expectations.

DecentDB is designed first as an embedded application database:

```rust
use decentdb::{Db, DbConfig};

let db = Db::open("app.ddb", DbConfig::default())?;
```

If the database should feel like a small local component shipped with an
application, DecentDB may be easier to reason about.

### 2. You need DecentDB's local branch, diff, and restore workflows

DecentDB makes local database change validation explicit:

```bash
decentdb snapshot create --db app.ddb --name before-upgrade
decentdb branch create --db app.ddb --name upgrade-test --from before-upgrade
decentdb branch diff --db app.ddb --left main --right upgrade-test --format table
decentdb branch merge --db app.ddb --source upgrade-test --target main --dry-run
```

Firebird has mature backup/restore and transaction capabilities, but branch and
diff are not the same product-level local workflow. Choose DecentDB when those
workflows are central to the application.

### 3. You need local-first sync state in SQL/CLI

DecentDB exposes sync state, peers, scopes, conflicts, retention, and doctor
checks as local database surfaces:

```sql
SELECT * FROM sys_sync_status;
SELECT * FROM sys_sync_journal ORDER BY sequence DESC LIMIT 20;
SELECT * FROM sys_sync_conflicts;
SELECT * FROM sys_sync_doctor;
```

Firebird can be used in replicated systems, but Firebird Embedded is not
primarily a local-first sync product. If offline exchange, conflict inspection,
and sync diagnostics should be built into the embedded database, DecentDB is
the closer fit.

### 4. You need embedded row policies, projection masks, and audit context

DecentDB includes local policy/masking features:

```sql
SET AUDIT CONTEXT tenant_id = 'tenant-a';

CREATE POLICY tenant_filter ON invoices
USING tenant_id = current_tenant();

CREATE MASK salary_mask ON employees(salary)
USING NULL;
```

Firebird has a serious SQL engine and security model, including stored
procedures and triggers. DecentDB's value is narrower and local-app-specific:
policy and mask rules are engine-enforced features of the embedded database.

### 5. You want Postgres-style application SQL in a focused embedded engine

DecentDB supports useful Postgres-style syntax:

```sql
SELECT DISTINCT ON (account_id)
       account_id, event_id, created_at
FROM account_events
ORDER BY account_id, created_at DESC;

INSERT INTO users (email) VALUES ($1) RETURNING id;
```

Firebird SQL is its own mature dialect. If your application already speaks
Firebird SQL, Firebird Embedded is a natural choice. If you prefer DecentDB's
Postgres-adjacent subset, DecentDB is a cleaner fit.

### 6. You need multi-language bindings around one small C ABI

DecentDB's public embedding strategy is a stable C ABI with maintained
bindings:

- Rust;
- C/C++;
- Python;
- .NET;
- Go;
- Java;
- Node.js;
- Dart.

Firebird has mature drivers and APIs across ecosystems. DecentDB's distinction
is that these bindings are part of one small embedded database project's
contract and release process.

### 7. You need deterministic crash/fault validation hooks

DecentDB includes FaultyVFS and WAL failpoint validation in its durability
testing strategy. Choose DecentDB when local crash behavior is something you
want to test directly through the engine's fault-injection tooling.

Firebird is mature and battle-tested, but the two projects expose different
testing surfaces to application developers.

## When Firebird Embedded Is the Better Fit

### 1. You already use Firebird SQL or Firebird tooling

If your schema, queries, stored procedures, triggers, backup workflow, and team
knowledge are already Firebird-shaped, Firebird Embedded is the natural local
engine.

Moving to DecentDB would be a real database migration: SQL dialect changes,
driver changes, feature review, and data migration.

### 2. You need mature stored procedures and PSQL

Firebird's procedural SQL is a major differentiator:

```sql
-- Firebird-style stored procedure direction.
CREATE PROCEDURE recompute_account_balance (account_id INTEGER)
AS
BEGIN
  -- procedural SQL body
END
```

DecentDB supports triggers and sandboxed Lua extension packages, but it does
not provide Firebird's mature PSQL stored procedure environment. If database
resident procedural logic is central, Firebird Embedded is stronger.

### 3. You want a path from embedded to server-style Firebird deployments

Firebird Embedded belongs to the same engine family as Firebird server
deployments. That can matter when an application might start local and later
move to a server topology, or when teams already operate Firebird elsewhere.

DecentDB is intentionally embedded. It does not currently offer a DecentDB
server deployment path.

### 4. You need Firebird's SQL dialect and ecosystem features

Firebird supports a broad SQL feature set, stored procedures, triggers, common
table expressions, flexible transaction management, monitoring tables, Trace
API, events, and Firebird-specific tooling.

If those are application requirements, Firebird Embedded is a better fit than
asking DecentDB to become Firebird-compatible.

### 5. You rely on Firebird's multi-generational architecture

Firebird is known for its multi-generational architecture and concurrency
model. If your application already benefits from Firebird's transaction
semantics or concurrency behavior, preserve that advantage unless DecentDB's
native features justify a migration.

### 6. You need existing Firebird drivers and support knowledge

Firebird has established drivers and knowledge in Delphi, C/C++, Java, .NET,
ODBC, and other ecosystems. DecentDB has maintained bindings, but it does not
replace Firebird's ecosystem history.

## Side-by-Side Examples

### Firebird-centered application

```sql
-- Firebird direction: database-side procedural SQL and Firebird tooling.
CREATE TRIGGER orders_ai FOR orders
ACTIVE AFTER INSERT POSITION 0
AS
BEGIN
  -- Firebird PSQL trigger body
END
```

Prefer **Firebird Embedded** when the application is built around Firebird SQL,
PSQL, and Firebird deployment knowledge.

### DecentDB local-first application

```sql
-- DecentDB direction: local sync inspection and policy/mask behavior.
SELECT * FROM sys_sync_status;

CREATE POLICY tenant_scope ON invoices
USING tenant_id = current_tenant();
```

Prefer **DecentDB** when local sync state, branch workflows, and local data
governance are the differentiators.

### Server migration path

```text
Firebird Embedded -> Firebird server-family deployment
```

Prefer **Firebird Embedded** if the architecture may need to become a Firebird
server deployment later.

```text
DecentDB local file -> DecentDB local sync / branch / restore workflows
```

Prefer **DecentDB** if the architecture is intentionally local-first embedded
rather than server-transition-oriented.

## Summary Decision Matrix

| Your situation | Recommendation |
|----------------|----------------|
| Existing Firebird schema, drivers, stored procedures, or tooling | **Firebird Embedded** |
| Need mature PSQL stored procedures | **Firebird Embedded** |
| Need an embedded-to-server Firebird path | **Firebird Embedded** |
| Need broad Firebird SQL and engine features | **Firebird Embedded** |
| Need Firebird's established ecosystem/history | **Firebird Embedded** |
| Need small Rust-native embedded database identity | **DecentDB** |
| Need durable local commits by default | **DecentDB** |
| Need branch/diff/restore/time-travel workflows | **DecentDB** |
| Need sync journal/conflict/retention inspection | **DecentDB** |
| Need row policies, projection masks, audit context | **DecentDB** |
| Prefer Postgres-style embedded SQL subset | **DecentDB** |
| Need DecentDB's multi-language binding contract | **DecentDB** |

## Bottom Line

Pick **Firebird Embedded** when you want Firebird locally: Firebird SQL,
stored procedures, triggers, mature drivers, and a path toward Firebird server
deployments.

Pick **DecentDB** when you want a Rust-native embedded database with durable
defaults, branchable local data, inspectable sync, policy/mask features, and a
focused multi-language embedding story.

Both are credible embedded SQL choices. The right decision depends on whether
your application wants the Firebird ecosystem or DecentDB's local application
database workflows.

## External References

- [Firebird Embedded server notes](https://github.com/FirebirdSQL/firebird/blob/master/doc/README.user.embedded)
- [Firebird Windows Embedded documentation](https://firebirdsql.org/rlsnotesh/install2-win32-embed.html)
- [Firebird features](https://www.firebirdsql.org/en/features/)
- [Firebird project home](https://www.firebirdsql.org/)
- [Firebird documentation index](https://www.firebirdsql.org/en/documentation/)
