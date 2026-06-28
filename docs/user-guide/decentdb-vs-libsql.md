# DecentDB vs libSQL: When to Choose Which

This document helps developers decide between **DecentDB** and **libSQL** for
embedded and local-first SQL workloads. Both are embedded relational databases,
but they start from different compatibility goals.

> **Versions compared:** DecentDB 2.15.0 workspace behavior vs public libSQL
> and Turso documentation as of 2026-06-28.
>
> **Scope note:** This page compares DecentDB with **libSQL**, the
> production-ready SQLite fork maintained by Turso. It does not compare
> DecentDB with **Turso Database**, the newer Rust SQLite-compatible rewrite.
> For that, see [DecentDB vs Turso Database](decentdb-vs-tursodatabase.md).
>
> **See also:** [DecentDB vs SQLite](decentdb-vs-sqlite.md),
> [DecentDB vs Turso Database](decentdb-vs-tursodatabase.md), [SQL Feature
> Matrix](sql-feature-matrix.md), and [SQL Reference](sql-reference.md).

## They Are Close in Category, Not in Product Shape

DecentDB and libSQL can both be used where an application might otherwise use
SQLite: local state, embedded server-side storage, desktop apps, edge services,
mobile-adjacent local files, and small durable databases.

The important difference is compatibility:

- **libSQL** is a fork of SQLite. Turso documents it as production-ready,
  SQLite-compatible, and maintaining the same file format and API. Its value is
  "SQLite, but open-contribution and extended."
- **DecentDB** is an independent Rust-native embedded relational database. It
  is intentionally SQLite-comparable where useful, but it has its own file
  format, SQL choices, durability posture, branch/sync/policy features, and
  binding surface.

The short version:

- Choose **libSQL** when SQLite compatibility, Turso Cloud, embedded replicas,
  and existing SQLite-oriented tooling are central to the decision.
- Choose **DecentDB** when you want a standalone embedded database with durable
  defaults, Postgres-style SQL, local branch/diff workflows, built-in
  local-first inspection, row policies, projection masks, and DecentDB's native
  feature set.

## At a Glance

| Dimension | DecentDB | libSQL |
|-----------|----------|--------|
| **Core identity** | Independent Rust-native embedded relational database | Production-ready fork of SQLite |
| **Compatibility goal** | SQLite-compatible probes where useful; Postgres-style SQL where useful | Full SQLite compatibility, same file format and API |
| **File format** | DecentDB `.ddb` format | SQLite-compatible database files |
| **Default durability posture** | WAL + fsync-on-commit by default; relaxed sync is explicit at open time | SQLite-derived durability behavior; configurable through SQLite-style settings and client/service surfaces |
| **Concurrency model** | One writer, many readers; local native cross-process WAL coordination when supported | SQLite-derived single-writer model with Turso/libSQL extensions depending on deployment |
| **SQL direction** | Practical Postgres-like application SQL subset | SQLite SQL compatibility first |
| **Vector search** | Future roadmap item | Native vector search is a libSQL/Turso feature |
| **Local-first sync** | Built-in sync journal, scopes, conflicts, relay/dev transport, SQL/CLI inspection | Turso Embedded Replicas and newer Turso Sync surfaces; cloud-centered options |
| **Branches / snapshots** | Local named snapshots, branches, diff, guarded restore, constrained merge | Turso Cloud has branching; local libSQL files use SQLite-style backup/copy patterns |
| **Security model** | TDE, row policies, projection masks, audit context | SQLite-compatible local behavior plus Turso Cloud auth/token/encryption features when using the service |
| **Extensions** | Sandboxed Lua packages; no arbitrary native `.load` | SQLite ecosystem compatibility direction; libSQL/Turso-specific extensions |
| **Bindings** | C ABI, Rust, Python, .NET, Go, Java, Node.js, Dart | SQLite/libSQL ecosystem bindings and Turso SDKs |
| **License** | MIT or Apache-2.0 | MIT |

## Naming Matters

Turso now uses several related database names:

| Name | What It Means | Why It Matters |
|---|---|---|
| **libSQL** | Production-ready fork of SQLite | Best comparison when you need mature SQLite compatibility today |
| **Turso Database** | Rust rewrite/reimplementation of SQLite | Best comparison when you are evaluating the new Turso engine direction |
| **Turso Cloud** | Managed database service | Best comparison when the managed service is part of the decision |
| **Turso Sync** | Newer explicit push/pull sync path | Different sync model from older libSQL embedded replicas |

A fair comparison should not treat these as one interchangeable thing.
DecentDB competes with all of them in different ways, but this page focuses on
the libSQL lane.

## When DecentDB Is the Better Fit

### 1. You do not need SQLite drop-in compatibility

If your application is new, or if you control the database access layer,
DecentDB lets you choose a database for its native behavior rather than for
SQLite compatibility.

```sql
-- DecentDB: Postgres-style parameter numbering and RETURNING.
INSERT INTO accounts (email, display_name)
VALUES ($1, $2)
RETURNING id, created_at;
```

libSQL is the better fit when existing SQLite SQL, `rowid`, PRAGMAs, file
format assumptions, or SQLite-oriented tooling must keep working. DecentDB is
the better fit when those constraints are not the deciding factor.

### 2. You want durable-by-default local commits

DecentDB's default local database posture is conservative: each commit is
WAL-backed and fsynced before returning. Faster modes exist, but they are
explicit open-time choices rather than casual SQL-level changes.

```sql
BEGIN;
UPDATE ledger SET balance = balance - 100 WHERE account_id = 1;
UPDATE ledger SET balance = balance + 100 WHERE account_id = 2;
COMMIT; -- durable before returning under default settings
```

libSQL inherits SQLite's flexible tuning surface. That flexibility is valuable,
especially for teams already comfortable with SQLite deployment settings. It
also means the application owner is responsible for choosing the durability
profile that matches the risk model.

### 3. You want local branch, diff, restore, and time-travel workflows

DecentDB has built-in local workflows for validating changes before applying
them to the main database:

```bash
decentdb snapshot create --db app.ddb --name before-import
decentdb branch create --db app.ddb --name import-test --from before-import
decentdb exec --db app.ddb --branch import-test --sql "ALTER TABLE users ADD COLUMN tier TEXT"
decentdb branch diff --db app.ddb --left main --right import-test --format table
decentdb branch merge --db app.ddb --source import-test --target main --dry-run
```

libSQL users can use SQLite-style backups, file copies, application migration
tools, and Turso Cloud branching where appropriate. DecentDB's distinction is
that these local branch/diff concepts are first-class database workflows.

### 4. You want built-in local-first sync inspection

DecentDB's local-first sync foundation is visible through SQL and CLI surfaces:

```sql
SELECT * FROM sys_sync_status;
SELECT * FROM sys_sync_journal ORDER BY sequence DESC LIMIT 20;
SELECT * FROM sys_sync_conflicts ORDER BY conflict_id;
SELECT * FROM sys_sync_doctor;
```

This makes local sync state, conflicts, retention, peer lag, and operational
diagnostics ordinary database data that can be tested and inspected.

libSQL/Turso has a different strength: Turso-managed sync options. Embedded
replicas serve reads from a local file and route writes to the cloud primary by
default; newer Turso Sync surfaces support explicit push/pull with local reads
and writes. If you want Turso Cloud in the architecture, that is a strong
reason to choose the Turso/libSQL ecosystem.

### 5. You want Postgres-style SQL conveniences in an embedded engine

DecentDB intentionally supports several Postgres-adjacent conveniences:

```sql
SELECT DISTINCT ON (customer_id)
       customer_id, id, created_at, total
FROM orders
ORDER BY customer_id, created_at DESC;

SELECT STRING_AGG(name, ', ' ORDER BY name)
FROM users
WHERE name ILIKE $1;
```

libSQL stays close to SQLite syntax and behavior. That is exactly what many
applications need, but it is a different SQL ergonomics target.

### 6. You need row policies, projection masks, and audit context locally

DecentDB includes local data-governance features in the embedded engine:

```sql
SET AUDIT CONTEXT tenant_id = 'tenant-a';

CREATE POLICY tenant_filter ON invoices
USING tenant_id = current_tenant();

CREATE MASK ssn_mask ON employees(ssn)
USING '***-**-' || right(ssn, 4);
```

libSQL and SQLite applications can implement equivalent concepts in the
application layer or through service-side controls when using Turso Cloud.
DecentDB's value is that the rules travel with the embedded database and are
enforced by the engine.

### 7. You prefer sandboxed extension packages

DecentDB's extension model is sandboxed Lua packages with manifests,
content-hash trust, and declared SQL objects. It intentionally does not expose
arbitrary native extension loading.

That is useful when your application needs extension logic across native,
server, mobile, or browser-adjacent deployments without distributing per-platform
native extension binaries.

libSQL's strength is compatibility with the SQLite ecosystem and Turso's
extended features. If you need SQLite-native extension semantics, libSQL is the
more natural choice.

## When libSQL Is the Better Fit

### 1. You need SQLite compatibility

This is the clearest libSQL win. If your application already depends on SQLite
behavior, libSQL is designed to preserve that world:

```sql
PRAGMA journal_mode = WAL;
SELECT rowid, * FROM users;
CREATE VIRTUAL TABLE docs USING fts5(title, body);
ATTACH DATABASE 'archive.db' AS archive;
```

DecentDB supports safe SQLite-style PRAGMA probes and many familiar SQL
features, but it is not a SQLite file or API replacement.

### 2. You need a mature SQLite fork today

Turso documents libSQL as production-ready, battle-tested, and fully backwards
compatible with SQLite. If your risk model is "SQLite behavior, plus Turso's
extensions," libSQL has a stronger maturity story than the newer Turso Database
rewrite and a more compatible adoption path than DecentDB.

### 3. You want Turso Cloud or embedded replicas

libSQL is part of the Turso ecosystem. That matters when you want:

- managed databases;
- cloud primary plus local embedded replicas;
- local reads from a file with writes routed to the cloud primary;
- Turso SDKs and auth tokens;
- production service features such as backups and managed operations.

DecentDB is a local embedded database project with local sync tooling. It does
not currently offer a managed cloud database service.

### 4. You need native vector search now

libSQL/Turso includes native vector search as a current feature. DecentDB has
full-text and trigram substring search today, while vector search and hybrid
ranking are future roadmap items.

```sql
-- DecentDB today: keyword and substring search.
CREATE INDEX idx_docs_body_fts ON docs USING fulltext(body);
CREATE INDEX idx_docs_title_trgm ON docs USING trigram(title);
```

If the primary use case is local RAG, embeddings, or vector similarity search,
libSQL/Turso may fit sooner.

### 5. You rely on SQLite tooling, extensions, and ORMs

The SQLite ecosystem is huge. Because libSQL keeps SQLite compatibility as a
core goal, it is usually easier to adopt when your application already depends
on SQLite-facing tools, ORMs, database browsers, migration frameworks, and SQL
patterns.

DecentDB has maintained bindings and compatibility views, but it is still a
different engine with a different dialect envelope and file format.

### 6. You want the broadest SQLite migration path

Migrating from SQLite to libSQL is conceptually a fork-to-fork move: same
general file/API world, with added features. Migrating from SQLite to DecentDB
is a database migration: export/import data, review SQL dialect differences,
and validate application behavior.

That is not a weakness of DecentDB; it is simply a different adoption path.

## Side-by-Side Examples

### Existing SQLite application

```sql
-- SQLite/libSQL-style assumptions.
SELECT rowid, * FROM jobs WHERE status = 'pending';
PRAGMA table_info(jobs);
ATTACH DATABASE 'archive.db' AS archive;
```

Prefer **libSQL** when this compatibility is central. Prefer **DecentDB** only
after confirming the application can move to explicit primary keys and
DecentDB's compatibility subset.

### New embedded application with local validation workflows

```bash
decentdb snapshot create --db app.ddb --name before-upgrade
decentdb branch create --db app.ddb --name upgrade-check --from before-upgrade
decentdb branch diff --db app.ddb --left main --right upgrade-check
```

Prefer **DecentDB** when branch/diff/restore workflows are part of how the
application validates data changes.

### Cloud-backed local reads

```ts
import { createClient } from "@libsql/client";

const db = createClient({
  url: "file:local.db",
  syncUrl: process.env.TURSO_DATABASE_URL,
  authToken: process.env.TURSO_AUTH_TOKEN,
});
```

Prefer **libSQL/Turso** when the architecture intentionally includes Turso
Cloud and embedded replicas.

## Summary Decision Matrix

| Your situation | Recommendation |
|----------------|----------------|
| Existing SQLite app, file, or tooling | **libSQL** |
| Need production-ready SQLite fork behavior | **libSQL** |
| Need Turso Cloud / embedded replicas | **libSQL / Turso** |
| Need native vector search today | **libSQL / Turso** |
| Need SQLite virtual tables, `rowid`, `ATTACH`, or broad PRAGMA behavior | **libSQL** |
| New app can choose a DecentDB-native model | **DecentDB** |
| Need durable local commits by default | **DecentDB** |
| Need local branch/diff/restore/time-travel workflows | **DecentDB** |
| Need sync state, conflicts, and retention visible in SQL/CLI | **DecentDB** |
| Need row policies, projection masks, and audit context locally | **DecentDB** |
| Prefer Postgres-style SQL in an embedded engine | **DecentDB** |
| Prefer sandboxed Lua extensions over native extension loading | **DecentDB** |

## Bottom Line

Pick **libSQL** when the sentence starts with "we already use SQLite" or "we
want Turso Cloud."

Pick **DecentDB** when the sentence starts with "we want a local embedded
database with durable defaults, branchable workflows, inspectable sync,
policies, and Postgres-style SQL."

Both are legitimate choices. The right answer depends mostly on whether SQLite
compatibility is a hard requirement or a historical convenience.

## External References

- [Turso libSQL documentation](https://docs.turso.tech/libsql)
- [Turso Embedded Replicas documentation](https://docs.turso.tech/features/embedded-replicas/introduction)
- [Turso TypeScript SDK reference](https://docs.turso.tech/sdk/ts/reference)
- [Turso Rust SDK reference](https://docs.turso.tech/sdk/rust/reference)
- [libSQL repository](https://github.com/tursodatabase/libsql)
