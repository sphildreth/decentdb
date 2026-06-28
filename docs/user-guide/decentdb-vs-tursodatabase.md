# DecentDB vs Turso Database: When to Choose Which

This document helps developers decide between **DecentDB** and **Turso
Database** for embedded and local-first SQL workloads. Both projects are
Rust-native embedded SQL engines in the SQLite-adjacent space, but they are not
the same product and they optimize for different adoption paths.

> **Versions compared:** DecentDB 2.15.0 workspace behavior vs Turso Database
> public docs and repository state as of 2026-06-28.
>
> **Scope note:** This page compares DecentDB with **Turso Database**, the
> Rust rewrite/reimplementation. Turso also maintains **libSQL**, a
> production-ready SQLite fork, and **Turso Cloud**, a managed service that
> currently runs on libSQL. Those are called out separately where relevant.
>
> **See also:** [DecentDB vs SQLite](decentdb-vs-sqlite.md), [SQL Feature
> Matrix](sql-feature-matrix.md), and [SQL Reference](sql-reference.md) for
> DecentDB's full SQL surface.

## They Are Similar, But Not Equivalent

It is fair to say DecentDB and Turso Database compete in the same broad
category: modern embedded SQL engines for local application state, browser or
device deployment, and local-first workflows.

It is not accurate to say they are "basically the same thing."

- **Turso Database** is explicitly a SQLite-compatible Rust rewrite. Its
  adoption story is: keep SQLite semantics and compatibility, then add modern
  architecture such as async I/O, concurrent writes, vector search, browser
  support, and Turso Cloud sync paths.
- **DecentDB** is an independent Rust-native embedded relational database. Its
  adoption story is: durable ACID writes by default, fast local reads,
  Postgres-style SQL where useful, branches/time-travel, local-first sync,
  structured diagnostics, policies/masks, and a stable C ABI for maintained
  bindings.

The short version:

- Choose **Turso Database** when SQLite compatibility and the Turso ecosystem
  are the center of the decision.
- Choose **DecentDB** when durability-first local application data, branchable
  workflows, local-first inspection, policy/masking, and DecentDB's native
  feature set matter more than being a SQLite drop-in replacement.

## At a Glance

| Dimension | DecentDB | Turso Database |
|-----------|----------|----------------|
| **Core identity** | Independent Rust-native embedded relational database | Rust rewrite/reimplementation of SQLite |
| **Primary compatibility goal** | SQLite-comparable where useful; Postgres-style SQL subset where useful | SQLite-compatible SQL, file/API behavior, and migration path |
| **Maturity framing** | DecentDB 2.15.0 workspace behavior | Turso Database is evolving/beta; libSQL is production-ready |
| **Default durability posture** | WAL + fsync-on-commit by default; relaxed sync is an explicit open-time choice | SQLite-compatible durability behavior; cloud/libSQL surfaces vary by deployment |
| **Concurrency model** | One writer, many readers; local native cross-process WAL coordination when supported | Designed around modern SQLite-compatible concurrency, including concurrent-write direction |
| **SQL direction** | Deliberate Postgres-like application SQL surface | SQLite compatibility first |
| **File format** | DecentDB `.ddb` format | SQLite-compatible direction; encrypted/experimental features may require Turso-specific handling |
| **Local-first sync** | Built-in sync journal, scopes, conflicts, relay/dev transport, CLI/SQL inspection | Turso Sync and Turso Cloud ecosystem, with push/pull and managed-service workflows |
| **Branches / time travel** | Built-in retained snapshots, branch-local writes, diff, guarded restore, constrained merge | Turso Cloud offers branching; local Turso Database branch workflow depends on Turso tooling |
| **Search** | Native full-text and trigram indexes; vector search is future work | Native vector search is a headline feature; FTS/index-method details depend on current feature flags |
| **Security model** | TDE, durable row policies, projection masks, audit context | Turso Cloud BYOK/scoped tokens; Turso Database encryption is documented as experimental in SQL docs |
| **Extensions** | Sandboxed Lua packages; no arbitrary native `.load` | SQLite ecosystem compatibility direction; Turso/libSQL extension story differs by package/service |
| **Managed cloud** | No hosted DecentDB service in core project | Turso Cloud is a primary product surface |
| **License** | MIT or Apache-2.0 | MIT |

## Important Naming Distinction

Turso uses related names for different things:

| Name | What It Means | Maturity Signal |
|---|---|---|
| **Turso Database** | Rust SQLite-compatible rewrite/reimplementation | Evolving/beta according to Turso/libSQL docs and repository language |
| **libSQL** | Open-source SQLite fork maintained by Turso | Production-ready and battle-tested according to Turso docs |
| **Turso Cloud** | Managed SQLite-compatible database service | Production service; currently documented as running on libSQL |

This distinction matters when evaluating risk. A production comparison against
**libSQL/Turso Cloud** is not the same as a production comparison against the
new **Turso Database** engine.

## When DecentDB Is the Better Fit

### 1. You want durability-first local data without SQLite compatibility as the main goal

DecentDB's default posture is conservative: WAL-backed commits are fsynced
before returning unless the application explicitly opens the database with a
relaxed sync policy. SQL cannot casually downgrade commit durability at runtime.

```sql
-- DecentDB: durable by default.
BEGIN;
INSERT INTO ledger (account_id, amount) VALUES (42, 19.95);
COMMIT; -- fsync'd before returning under the default sync mode
```

Turso Database is trying to preserve SQLite compatibility while evolving the
architecture. That is valuable, but it means the evaluation starts from a
different question: "How close is this to SQLite for my workload?" With
DecentDB, the question is: "Do DecentDB's durable defaults and native features
fit my application?"

### 2. You need stable application workflows that are not SQLite-shaped

DecentDB is not trying to be a SQLite file/API clone. It intentionally exposes
features that are natural for modern local applications:

- named snapshots;
- branch-local writes;
- branch diff;
- guarded branch restore;
- constrained branch merge;
- time-travel reads by snapshot name;
- local-first sync inspection through `sys.*`;
- durable policies, projection masks, and audit context.

```bash
decentdb snapshot create --db app.ddb --name before-import
decentdb branch create --db app.ddb --name import-test --from before-import
decentdb branch diff --db app.ddb --left main --right import-test --format json
decentdb branch merge --db app.ddb --source import-test --target main --dry-run
```

If these workflows matter, DecentDB gives them first-class names and
diagnostics instead of requiring SQLite-compatible external tooling.

### 3. You want Postgres-style SQL in an embedded engine

Turso Database's compatibility target is SQLite. DecentDB's SQL surface is
more intentionally Postgres-adjacent where that helps application developers:

- `$1`, `$2` positional parameters;
- `RETURNING`;
- `DISTINCT ON`;
- `ILIKE`;
- `STRING_AGG(... ORDER BY ...)`;
- `INTERVAL` arithmetic;
- richer date/time and statistical functions;
- `TRUNCATE TABLE` with identity behavior.

```sql
SELECT DISTINCT ON (account_id)
       account_id, event_id, created_at
FROM account_events
ORDER BY account_id, created_at DESC;
```

If your application also talks to Postgres, DecentDB can reduce dialect drift.
If your application needs to remain close to SQLite, Turso Database is the more
natural fit.

### 4. You need local-first sync that is inspectable inside the embedded database

DecentDB includes a conservative local-first sync foundation:

- durable local change journal;
- replica IDs;
- peer catalogs;
- named scopes with validated row filters;
- JSON batch export/import;
- local HTTP relay/dev transport;
- conflict recording and manual resolution;
- sync doctor and retention tooling;
- SQL inspection views for sync state.

```sql
SELECT * FROM sys.sync_status;
SELECT * FROM sys_sync_journal ORDER BY sequence DESC LIMIT 20;
SELECT * FROM sys_sync_conflicts ORDER BY conflict_id;
SELECT * FROM sys_sync_doctor;
```

Turso's sync story is tied to Turso Sync and Turso Cloud. That may be the right
answer if you want Turso-managed sync. DecentDB is the better fit when you want
sync state, conflicts, and retention to be ordinary local database surfaces that
can be inspected and tested without adopting a managed cloud service.

### 5. You need row policies, projection masks, and audit context

DecentDB includes embedded local data-security features that are closer to a
small application database than a raw SQLite replacement:

```sql
SET AUDIT CONTEXT tenant_id = 'tenant-a';

CREATE POLICY tenant_filter ON invoices
USING tenant_id = current_tenant();

CREATE MASK ssn_mask ON employees(ssn)
USING '***-**-' || right(ssn, 4);
```

Turso Cloud has important managed-service security features such as scoped
tokens and bring-your-own-key encryption. DecentDB's strength is different:
local policy and masking rules live with the embedded database and are enforced
by the engine during query execution.

### 6. You prefer sandboxed extension packages over native extension loading

DecentDB's extension story is sandboxed Lua packages with manifests, trust, and
declared SQL objects. It intentionally avoids arbitrary native `.load`.

```sql
SELECT normalize_phone('(555) 010-1234');
```

That is a safer fit for applications that want local extensibility across
native, browser, and mobile targets without loading platform-native extension
binaries. If your priority is SQLite ecosystem compatibility and native
extension behavior, Turso/libSQL may be the better fit.

### 7. You need deterministic crash/fault testing hooks

DecentDB exposes engine-level fault-injection paths such as FaultyVFS and WAL
failpoints in its test strategy. This is valuable when the database is part of
a product that must prove crash behavior, not just pass happy-path SQL tests.

Turso Database also documents serious reliability work, including deterministic
simulation testing. The distinction is product shape: DecentDB treats
durability-first local engine behavior as its first design priority and exposes
fault-injection-oriented validation as part of that identity.

### 8. You want one local embedded database project, not a cloud database product

Turso's strongest product story includes Turso Cloud, Turso Sync, platform APIs,
many-database architecture, and managed operational features. That is useful,
but it is also a product ecosystem.

DecentDB is the better fit when you want the core database to remain a local
embedded component with documented behavior, local files, local sync artifacts,
local diagnostics, and no requirement to adopt a hosted control plane.

## When Turso Database Is the Better Fit

### 1. You need SQLite compatibility as the central requirement

Turso Database is explicitly positioned as a SQLite-compatible rewrite. If your
success criteria are "existing SQLite SQL, schema, queries, and application
assumptions should work unchanged," Turso Database is aiming directly at that
requirement.

DecentDB supports many SQLite-compatible probes and features, but it is not a
drop-in SQLite replacement. It has its own file format, SQL choices, and
configuration model.

### 2. You want the Turso Cloud ecosystem

Turso Cloud is a managed SQLite-compatible database platform with:

- managed databases;
- API-based database creation;
- branching;
- backups and recovery;
- vector search;
- replication/sync workflows;
- scoped access tokens;
- bring-your-own-key encryption;
- analytics and team management.

If you want a managed database service rather than only an embedded engine,
Turso Cloud is a much broader product surface. DecentDB does not currently
offer a hosted service.

### 3. You need production-ready SQLite-fork behavior today

If the actual decision is between DecentDB and **libSQL**, not DecentDB and
Turso Database, then the comparison changes. Turso documents libSQL as
production-ready, battle-tested, and fully backwards compatible with SQLite.

That makes libSQL a better fit when you need:

- SQLite file/API continuity;
- mature SQLite-derived behavior;
- ORM support already built around `@libsql/client`, `libsql`, or `go-libsql`;
- Turso Cloud's current production foundation.

Turso Database is the forward-looking rewrite. libSQL is the production-ready
SQLite fork.

### 4. You need native vector search now

Turso markets native vector search as a first-class capability. DecentDB has
native full-text search and trigram substring search, but vector search and
rank-fusion are future roadmap items.

```sql
-- DecentDB today: keyword/substring search, not vector search.
CREATE INDEX idx_docs_body_fts ON docs USING fulltext(body);
CREATE INDEX idx_docs_title_trgm ON docs USING trigram(title);
```

If your near-term application is local RAG, embeddings, or vector similarity
search, Turso's current vector-search surface may be more attractive.

### 5. You need concurrent writes as a core differentiator

DecentDB intentionally preserves a one-writer/many-readers model. It improves
ergonomics with a write queue, backpressure, timeouts, strict group commit, and
cross-process coordination, but it does not present itself as a multi-writer
engine.

Turso Database is designed to move beyond SQLite's single-writer limitations
with concurrent-write architecture. If your workload fundamentally depends on
multiple concurrent writers inside the database engine, Turso Database is aimed
at that problem. Because the new Turso Database engine is still evolving, test
this carefully against your workload before treating it as a production
guarantee.

### 6. You want async-first engine integration

Turso Database emphasizes async design and modern I/O such as Linux
`io_uring`. If your application stack is async-first and you want the database
engine architecture to be shaped around that, Turso Database may fit better.

DecentDB exposes safe embedding through Rust, C ABI, and maintained bindings,
but its primary identity is not "async-first SQLite rewrite."

### 7. You want the many-database architecture

Turso's product positioning emphasizes extremely high database density: many
small databases, often one per user, tenant, or agent, backed by cloud APIs and
database-per-tenant operational tooling.

DecentDB can be embedded per application, user, tenant, or workspace, but it
does not currently provide Turso's managed many-database control plane.

## Where The Choice Is Mostly Product Strategy

Some choices are not technical superiority questions. They are product strategy
questions.

| If your team says... | Prefer |
|---|---|
| "We want the closest modern path from SQLite with cloud sync options." | **Turso/libSQL/Turso Cloud** |
| "We want a Rust SQLite-compatible rewrite and can track beta maturity." | **Turso Database** |
| "We want a production-ready SQLite fork today." | **libSQL** |
| "We want a local embedded database with durable defaults and branch/sync/policy features." | **DecentDB** |
| "We want Postgres-like SQL in an embedded engine." | **DecentDB** |
| "We want managed databases, scoped cloud tokens, API provisioning, and cloud branching." | **Turso Cloud** |
| "We want local policy/mask/audit rules enforced by the embedded engine." | **DecentDB** |

## Side-by-Side Examples

### Branch-style validation

```bash
# DecentDB: branch and diff are local database workflows.
decentdb snapshot create --db app.ddb --name before-migration
decentdb branch create --db app.ddb --name migration-test --from before-migration
decentdb exec --db app.ddb --branch migration-test --sql "ALTER TABLE users ADD COLUMN tier TEXT"
decentdb branch diff --db app.ddb --left main --right migration-test --format table
```

Turso Cloud also offers branching, but that is part of Turso's managed
platform/API story rather than DecentDB's local branch CLI model.

### Local-first sync inspection

```bash
# DecentDB: inspect local sync state from the database and CLI.
decentdb sync status --db app.ddb --format table
decentdb sync conflicts --db app.ddb --format table
decentdb sync doctor --db app.ddb
```

```sql
SELECT * FROM sys_sync_status;
SELECT * FROM sys_sync_conflicts;
```

Turso Sync is centered on explicit push/pull with Turso Cloud. That is a
stronger managed-service story; DecentDB's differentiator is local sync
introspection and conservative conflict surfaces.

### SQLite-style compatibility

```sql
-- Turso Database/libSQL direction: keep existing SQLite assumptions working.
PRAGMA journal_mode = WAL;
SELECT rowid, * FROM users;
ATTACH DATABASE 'archive.db' AS archive;
```

```sql
-- DecentDB direction: accept safe compatibility probes, but expose its own model.
PRAGMA journal_mode;    -- returns DecentDB's WAL behavior
SELECT * FROM users WHERE id = $1;
```

If your code depends deeply on SQLite-specific behavior such as implicit
`rowid`, `ATTACH`, virtual tables, or broad PRAGMA tuning, Turso/libSQL is the
more natural lane.

## Summary Decision Matrix

| Your situation | Recommendation |
|----------------|----------------|
| Need SQLite drop-in compatibility | **Turso Database** or **libSQL** |
| Need production-ready SQLite-fork behavior today | **libSQL / Turso Cloud** |
| Need a managed database service with API provisioning | **Turso Cloud** |
| Need native vector search today | **Turso Database / Turso Cloud** |
| Need concurrent-write architecture as a core requirement | **Turso Database**, with beta-maturity testing |
| Need async-first SQLite rewrite direction | **Turso Database** |
| Need DecentDB's durable fsync-on-commit defaults | **DecentDB** |
| Need branch/diff/restore/time-travel as local database workflows | **DecentDB** |
| Need local-first sync inspection, conflicts, and retention in SQL/CLI | **DecentDB** |
| Need row policies, projection masks, and audit context in an embedded engine | **DecentDB** |
| Need Postgres-style SQL in a local embedded engine | **DecentDB** |
| Need sandboxed Lua extension packages instead of native extension loading | **DecentDB** |
| Need broad SQLite ecosystem compatibility and existing SQLite tooling | **Turso/libSQL** |
| Want a local embedded component without a hosted product dependency | **DecentDB** |

## Bottom Line

Turso Database and DecentDB are peers in the modern embedded SQL landscape, but
they are not interchangeable.

Pick **Turso Database** when SQLite compatibility, Turso Cloud, vector search,
concurrent-write direction, and the Turso ecosystem are the deciding factors.

Pick **DecentDB** when durable local application data, branchable workflows,
local-first sync inspection, policies/masks, Postgres-style SQL, and a
standalone embedded database identity are more important than SQLite drop-in
compatibility.

## External References

- [Turso Database repository](https://github.com/tursodatabase/turso)
- [Turso libSQL documentation](https://docs.turso.tech/libsql)
- [Turso SDK introduction](https://docs.turso.tech/sdk/introduction)
- [Turso Cloud documentation](https://docs.turso.tech/turso-cloud)
- [Turso experimental features](https://docs.turso.tech/sql-reference/experimental-features)
