# DecentDB Future Wins

**Status:** Consolidated roadmap  
**Supersedes:** the earlier DecentDB future-wins roadmap drafts for roadmap prioritization  
**Purpose:** Product and engineering priority index. Dedicated specs and ADRs remain the implementation source of truth when they exist.

DecentDB should not try to win by becoming "SQLite, but with more features."
It should win by becoming the embedded SQL engine that makes local-first sync,
native spatial data, branchable workflows, Lua extensibility, and agent-friendly
developer experience feel native.

## Status Map

Status values:

- `TODO`: prioritized roadmap work that is not actively being implemented right now.
- `IN PROGRESS`: active implementation or design work is underway right now.
- `BACKLOG`: valuable, but not part of the near-term implementation path.

Future version values are planning buckets, not release commitments.

| Priority | Future Version | Status | Feature | Current Source Of Truth | Why This Rank |
|---:|---|---|---|---|---|
| 1 | vNext+1 | TODO | Policy-aware embedded SQL | Needs ADR/spec | Strong regulated/offline/enterprise story beyond encryption alone |
| 2 | vNext+1 | TODO | Application database bundle format | Needs ADR/spec | Makes DecentDB a portable app artifact, support bundle, and sharable dataset format |
| 3 | vNext+1 | TODO | Built-in observability and `sys.*` virtual tables | Needs ADR/spec; doctor v1 is foundation | Complements doctor and makes operational state queryable |
| 4 | vNext+1 | TODO | Lua extension runtime and package model | [`WIN_LUA_EXTENSION_RUNTIME_SPEC.md`](WIN_LUA_EXTENSION_RUNTIME_SPEC.md) | One official extension language gives DecentDB a supportable extensibility story without arbitrary native loading |
| 5 | vNext+2 | TODO | Transparent write queuing and pipelining | Needs ADR/spec | Makes one-writer reality feel modern under concurrent application writes |
| 6 | vNext+2 | TODO | Group commit / WAL batching refinements | ADR 0135 | Async commit exists; strict durable group commit refinements remain |
| 7 | vNext+2 | BACKLOG | Cross-process WAL coordination | Needs ADR/spec | Useful for Electron, helper processes, and background sync workers |
| 8 | vNext+3 | BACKLOG | JSONB binary storage | Needs ADR/spec | Better JSON performance, but less identity-defining after SQLite JSONB |
| 9 | vNext+3 | TODO | Bulk-load follow-ons and external file readers | Existing bulk-load API | Extends shipped foundation into stronger import, ETL, and query-external file workflows |
| 10 | vNext+3 | BACKLOG | Non-blocking schema migration | Needs ADR/spec | Valuable for large evolving databases, but complex and not the clearest identity anchor |
| 11 | Later | BACKLOG | Native vector / HNSW index | Needs ADR/spec | AI-era checkbox, useful but less unique |
| 12 | Later | BACKLOG | Full-text search with BM25 ranking | Needs ADR/spec | Expected search capability; less distinctive because SQLite FTS is mature |
| 13 | Later | BACKLOG | Transparent data encryption | Needs ADR/spec | Practical security feature, but weaker positioning than policy-aware data controls |
| 14 | Later | BACKLOG | Advanced geospatial semantics and analytics | [`WIN_GEOSPATIAL_DATA_SUPPORT.md`](WIN_GEOSPATIAL_DATA_SUPPORT.md) deferred work; needs follow-up ADR/spec | Builds on shipped native spatial support without confusing first-class geospatial as unfinished |
| 15 | Later | BACKLOG | Advanced SQL compatibility surface | [`WIN_ADVANCED_SQL_COMPATIBILITY_SURFACE.md`](WIN_ADVANCED_SQL_COMPATIBILITY_SURFACE.md) | Useful adoption polish; Lua extensions are tracked separately as the extensibility model |
| 16 | Later | BACKLOG | WAL streaming replication | Needs ADR/spec | HA/read-scale story, but not the local-first differentiator |
| 17 | Later | BACKLOG | Cloud-native object storage VFS | Needs ADR/spec | Interesting edge/serverless story, high complexity |

Delivered foundations are intentionally not tracked in this roadmap. Product
docs, changelogs, implementation specs, and ADRs remain the source of truth for
shipped capability.

## Positioning

Good positioning:

- The embedded SQL database for modern local-first apps
- Branchable relational data for apps, agents, and edge
- Embedded SQL with native sync
- Offline-capable spatial data for field, logistics, and IoT applications
- A serious application database, not just a file format

Weak positioning:

- SQLite but faster
- SQLite but with more features
- SQLite alternative
- Embedded Postgres-lite

The remaining roadmap should support one clear lane:

> DecentDB is the embedded SQL database built for modern apps, offline sync,
> native spatial data, branchable workflows, Lua extensibility, and AI-assisted
> development.

## 1. Policy-Aware Embedded SQL

**Status:** `TODO`
**Future Version:** vNext+1
**Source of truth:** Needs ADR/spec before implementation.

### Why This Matters

Encryption-at-rest is useful, but regulated and enterprise offline applications
often need policy built into local data access.

### Possible Direction

```sql
CREATE POLICY tenant_filter
  ON invoices
  USING tenant_id = current_tenant();

CREATE MASK ssn_mask
  ON employees(ssn)
  USING '***-**-' || right(ssn, 4);
```

### Required ADR Topics

- row filters
- masked projections
- column encryption
- auditability
- SQL planning implications
- binding behavior
- interaction with sync and bundles

## 2. Application Database Bundle Format

**Status:** `TODO`  
**Future Version:** vNext+1  
**Source of truth:** Needs ADR/spec before implementation.

### Why This Matters

SQLite is often used as a portable application file format by accident.
DecentDB can make that story deliberate.

### Desired Capability

A DecentDB bundle may contain:

- relational data
- blobs/assets
- metadata manifest
- optional FTS/vector indexes
- optional encryption
- optional signatures
- optional sync metadata
- export/import tooling

### Example Commands

```bash
decentdb export-bundle ./customer.ddb ./customer.ddbx
decentdb verify-bundle ./customer.ddbx
decentdb import-bundle ./customer.ddbx ./restored.ddb
```

### ADR Must Define

- bundle manifest
- integrity and signature model
- asset/blob storage rules
- compatibility guarantees
- interaction with WAL, checkpoints, encryption, and sync metadata

## 3. Built-In Observability And `sys.*` Virtual Tables

**Status:** `TODO`  
**Future Version:** vNext+1  
**Source of truth:** Needs ADR/spec. Doctor v1 is the foundation.

### Why This Matters

Doctor answers "what is wrong now?" Observability answers "what is happening
while the application runs?"

### Target Surfaces

```sql
SELECT * FROM sys.wal_metrics;
SELECT * FROM sys.slow_queries;
SELECT * FROM sys.lock_waits;
SELECT * FROM sys.storage_metrics;
SELECT * FROM sys.index_usage;
```

### Advisor Extensions

Doctor v1 is complete. Follow-on advisor work belongs here rather than in a
separate doctor-v1 roadmap item:

- `PRAGMA doctor`
- `sys.doctor_findings`
- Decent Bench doctor panel
- explicit `doctor --fix-plan`
- query-plan advisor
- missing/unused index advisor
- JSON path advisor
- sync and branch diagnostics after those features exist

### Guardrails

- near-zero overhead by default
- simple atomic counters for always-on metrics
- expensive tracing only after explicit configuration
- in-memory ring buffers for slow queries and lock waits
- no recursive disk writes for telemetry

## 4. Lua Extension Runtime And Package Model

**Status:** `TODO`
**Future Version:** vNext+1
**Source of truth:** [`WIN_LUA_EXTENSION_RUNTIME_SPEC.md`](WIN_LUA_EXTENSION_RUNTIME_SPEC.md)

### Why This Matters

SQLite and DuckDB have strong extension ecosystems. DecentDB should not clone
SQLite's arbitrary native `.load` model, but it does need a credible and
supportable extensibility story.

Lua gives DecentDB one official extension language, one package model, one
runtime contract, one docs path, and one binding surface. That is a better fit
for a durable embedded database than supporting many host-language callback
systems or unbounded native plugins.

### Recommended Direction

- extension packages with `decentdb-extension.toml` manifests
- Lua 5.4 language target
- explicit install/enable/trust lifecycle
- no auto-running extension code when an untrusted database is opened
- scalar functions first
- DecentDB-owned typed wrappers for `DECIMAL`, `UUID`, date/time, `BLOB`, and
  JSON values
- table-valued functions, aggregates, and collations in later slices
- CLI and binding APIs for validate, install, list, enable, disable, and test

### Guardrails

- no SQLite-style `.load` support in v1
- no filesystem, network, process, native-module, or database-write access from
  Lua by default
- no direct WAL, pager, B+Tree, catalog, or transaction internals
- no dynamic SQL signatures or runtime-discovered return schemas
- no loose or lossy type coercions
- Lua execution must be resource-bounded, cancellable, and converted into SQL
  errors without process corruption

## 5. Transparent Write Queuing And Pipelining

**Status:** `TODO`  
**Future Version:** vNext+2  
**Source of truth:** Needs ADR/spec before implementation.

### Why This Matters

DecentDB intentionally keeps one writer. Applications still need concurrent
write calls to feel smooth without pushing queuing and retry logic into every
host language.

### Desired Capability

Concurrent application threads submit write transactions into an engine-owned
queue. The single writer executes them sequentially and returns results to the
callers.

### Guardrails

- preserve one-writer semantics
- no hidden durability weakening
- bounded queue behavior and clear backpressure
- cancellation and timeout semantics must be explicit
- group commit should be considered together with this work

## 6. Group Commit / WAL Batching Refinements

**Status:** `TODO`  
**Future Version:** vNext+2  
**Source of truth:** ADR 0135 for current async commit behavior.

### Current Foundation

`WalSyncMode::AsyncCommit { interval_ms }` exists as an opt-in mode. It trades
a bounded post-crash durability window for higher write throughput and provides
`Db::sync()` as a durability barrier.

### Future Win

Implement strict group commit for concurrent durable transactions when paired
with write queuing:

- multiple concurrent transactions share one sync
- each transaction still gets a committed LSN
- caller-visible durability remains explicit
- default `WalSyncMode::Full` remains uncompromised

## 7. Cross-Process WAL Coordination

**Status:** `BACKLOG`  
**Future Version:** vNext+2  
**Source of truth:** Needs ADR/spec before implementation.

### Why This Matters

DecentDB already has same-process shared WAL visibility. Cross-process
coordination would support app architectures with a foreground process reading
while a background sync/helper process writes.

### Required Work

- file-lock or shared-memory writer coordination
- consistent snapshots across processes
- WAL retention for cross-process long readers
- diagnostics for process-level blockers

## 8. JSONB Binary Storage

**Status:** `BACKLOG`  
**Future Version:** vNext+3  
**Source of truth:** Needs ADR/spec before implementation.

### Why This Matters

DecentDB already supports JSON scalar and table functions. JSONB would remove
repeated parse cost for JSON-heavy workloads and make JSON expression indexes
more effective.

### Required Design Constraints

- zero-copy traversal over pinned page bytes
- no host language requirement to parse binary JSON
- C ABI projects JSONB as text JSON unless raw bytes are explicitly requested
- expression indexes store extracted scalars as ordinary typed index keys
- large JSONB uses existing overflow page mechanics
- partial updates rebuild the binary blob through the single writer

## 9. Bulk-Load Follow-Ons And External File Readers

**Status:** `TODO`  
**Future Version:** vNext+3  
**Source of truth:** Existing bulk-load API and CLI.

### Current Foundation

DecentDB already ships a bulk-load API and CLI workflows.

### Future Win

Build higher-level ingestion workflows:

- `COPY`-style SQL or CLI import commands
- read-only external file table functions such as `read_csv(...)` and
  `read_json(...)`
- CSV/JSON streaming readers for datasets larger than memory
- sorted-input hints for index-friendly loading
- better progress reporting
- resumable import ergonomics
- stronger benchmark coverage for ETL-style workloads

### Guardrails

- external file readers are explicit, read-only statement inputs
- reuse the existing import/export and bulk-load parsing paths where possible
- avoid a general virtual-table subsystem in this slice
- keep path handling, error reporting, and resource cleanup testable from the CLI

## 10. Non-Blocking Schema Migration

**Status:** `BACKLOG`  
**Future Version:** vNext+3  
**Source of truth:** Needs ADR/spec before implementation.

### Why This Matters

DecentDB already has broader raw `ALTER TABLE` coverage than SQLite in several
areas. The remaining win is making heavy schema changes non-blocking for large
databases.

### Desired Capability

- instant metadata-only compatible changes where safe
- background table rebuilds for heavier changes
- old/new schema versions during migration
- atomic catalog swap after completion
- clear diagnostics and rollback behavior

### ADR Triggers

This touches catalog, storage, concurrency, migration safety, and possibly file
format semantics. It needs an ADR before implementation.

## 11. Native Vector / HNSW Index

**Status:** `BACKLOG`  
**Future Version:** Later  
**Source of truth:** Needs ADR/spec before implementation.

### Why This Matters

Vector search is increasingly expected for AI and retrieval workloads.

### Why It Is Not Higher

Vector search is useful, but it is becoming a baseline checkbox rather than a
distinctive identity. It should follow the local-first, branchable, and
developer-experience work.

### Desired Capability

- `VECTOR(dim)` type
- HNSW index
- similarity operators
- no external C extension requirement
- benchmarks against common vector-search extensions

## 12. Full-Text Search With BM25 Ranking

**Status:** `BACKLOG`  
**Future Version:** Later  
**Source of truth:** Needs ADR/spec before implementation.

### Why This Matters

Search is useful for application databases, docs, notes, messages, and local
content. DecentDB should eventually offer a native integrated path.

### Why It Is Not Higher

SQLite FTS is mature, and FTS is increasingly expected rather than decisive.

### Desired Capability

- native text-search type or index mode
- BM25 ranking
- phrase search
- stemming/tokenization policy
- optional fuzzy matching and spelling-suggestion helpers as a later slice
- planner integration
- standard SQL surface without virtual-table awkwardness

## 13. Transparent Data Encryption

**Status:** `BACKLOG`  
**Future Version:** Later  
**Source of truth:** Needs ADR/spec before implementation.

### Why This Matters

Many local/offline apps need encrypted data at rest without SQLCipher-style build
friction.

### Required ADR Topics

- key management
- page encryption mode
- WAL encryption
- temporary file behavior
- recovery semantics
- C ABI and binding contract
- migration from plaintext to encrypted files

Policy-aware SQL may subsume or extend this work, so encryption should be
planned together with policy and audit requirements.

## 14. Advanced Geospatial Semantics And Analytics

**Status:** `BACKLOG`
**Future Version:** Later
**Source of truth:** [`WIN_GEOSPATIAL_DATA_SUPPORT.md`](WIN_GEOSPATIAL_DATA_SUPPORT.md) deferred work; needs follow-up ADR/spec before implementation.

### Why This Matters

Native geospatial types, spatial indexes, planner-visible filters, distance
ordering, and the first point-in-polygon spatial join path are shipped
foundations. More advanced GIS workloads still need a separate, explicit future
track so the completed native geospatial feature does not appear unfinished.

### Possible Direction

- arbitrary planner-native spatial joins beyond the first point-in-polygon shape
- spatial statistics in `ANALYZE`
- true 3D predicate semantics and explicit 3D-aware spatial index modes
- advanced index forms such as partial, expression, multi-column, or unique
  spatial indexes
- additional spatial reference systems and coordinate transformation support
- exposed S2/covering-cell helpers for advanced application workflows

### Guardrails

- preserve the shipped EWKB/C ABI contract
- avoid native GEOS/PROJ/GDAL dependencies unless an ADR justifies the tradeoff
- keep WASM compatibility as a design constraint

## 15. Advanced SQL Compatibility Surface

**Status:** `BACKLOG`  
**Future Version:** Later  
**Source of truth:** [`WIN_ADVANCED_SQL_COMPATIBILITY_SURFACE.md`](WIN_ADVANCED_SQL_COMPATIBILITY_SURFACE.md)

### Why This Matters

DecentDB already has a broad practical SQL surface for an embedded engine. The
remaining advanced compatibility work is useful for migrations, ORMs, power
users, and PostgreSQL-adjacent application code. Lua extensions are now tracked
as their own product capability rather than buried in this late compatibility
bucket.

### Why It Is Not Higher

This is valuable adoption polish, but it is not the clearest DecentDB identity
compared with local-first sync, branchable data workflows, native geospatial
support, browser support, observability, and storage fundamentals.

### Current Direction

- schema-qualified object names
- explicit sequence objects
- materialized views
- built-in table-valued helpers such as `generate_series(...)`
- binding-friendly array parameter/table-valued input support for
  `carray`-style use cases
- built-in collation syntax and compatibility helpers
- SQL-defined functions if DecentDB chooses to support them separately from Lua
  extensions
- user-defined types
- deferred constraints and exclusion constraints
- covering-index execution for existing `INCLUDE (...)` metadata

Full-text search, advanced geospatial follow-ons, and Lua extensions are tracked
as separate roadmap wins.

### Guardrails

- do not duplicate the Lua extension runtime
- do not add arbitrary native `.load` support here
- keep this track focused on SQL syntax, catalog compatibility, and migration
  ergonomics

## 16. WAL Streaming Replication

**Status:** `BACKLOG`  
**Future Version:** Later  
**Source of truth:** Needs ADR/spec before implementation.

### Why This Matters

WAL streaming enables standby databases, read scaling, and HA-style workflows.

### Why It Is Not Higher

The stronger DecentDB differentiator is local-first sync with offline writes and
conflict-aware merge semantics. WAL streaming is valuable, but it solves a more
traditional HA problem.

### Desired Modes

- async standby
- sync standby
- quorum acknowledgement
- explicit consistency/durability tradeoffs

## 17. Cloud-Native Object Storage VFS

**Status:** `BACKLOG`  
**Future Version:** Later  
**Source of truth:** Needs ADR/spec before implementation.

### Why This Matters

Serverless and edge deployments often have ephemeral local disks. An object
storage VFS could allow page-level reads and writes against S3, R2, Azure Blob,
or HTTP-backed storage.

### Why It Is Last

This is high complexity and has serious durability, latency, consistency, and
cache-invalidation risks. It should follow stronger local-first, browser, and
operational foundations.

### Required Direction

- expand VFS semantics deliberately
- use HTTP range requests for page reads
- use local cache aggressively
- define write coordination and consistency rules before implementation

## Near-Term Sequence

1. Advance policy, bundle, `sys.*`, and Lua extension ADRs/specs for the top
   roadmap items.
2. Continue schema-first SDK generation through Decent Bench while keeping
   DecentDB metadata authoritative.
3. Sequence write queuing and durable group-commit refinements together so the
   concurrency and durability contracts stay explicit.

## Market Notes

The roadmap order accounts for current market reality:

- Official SQLite has a WASM/OPFS story.
- SQLite has a mature command-line shell with extensive dot commands.
- SQLite has JSONB support.
- SQLite has mature FTS.
- SQLite R-Tree and Geopoly cover basic geometry use cases through virtual
  tables/extensions, but not native first-class geospatial column types.
- SQLite and DuckDB have mature extension ecosystems. DecentDB's proposed
  response is one official Lua extension language with strict manifests,
  sandboxing, and explicit trust rather than arbitrary native extension loading.
- DuckDB has broadened through FTS and vector extensions.
- The largest gap is integrated local-first workflow plus native application
  capabilities, not raw feature count.

Useful references:

- SQLite WASM / OPFS: https://sqlite.org/wasm/doc/trunk/persistence.md
- SQLite command-line shell: https://sqlite.org/cli.html
- SQLite JSONB: https://sqlite.org/jsonb.html
- SQLite session / changesets: https://sqlite.org/sessionintro.html
- SQLite R-Tree: https://sqlite.org/rtree.html
- SQLite Geopoly: https://www3.sqlite.org/geopoly/
- SpatiaLite: https://www.gaia-gis.it/fossil/libspatialite/index
- PostGIS: https://postgis.net/
- DuckDB full-text search: https://duckdb.org/docs/stable/core_extensions/full_text_search.html
- DuckDB vector similarity search: https://duckdb.org/docs/stable/core_extensions/vss.html
- LiteDB: https://www.litedb.org/docs/
- H2: https://www.h2database.com/html/features.html
