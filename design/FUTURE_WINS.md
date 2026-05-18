# DecentDB Future Wins

**Status:** Consolidated roadmap  
**Supersedes:** the earlier DecentDB future-wins roadmap drafts for roadmap prioritization  
**Purpose:** Product and engineering priority index. Dedicated specs and ADRs remain the implementation source of truth when they exist.

DecentDB should not try to win by becoming "SQLite, but with more features."
It should win by becoming the embedded SQL engine that makes local-first sync,
native spatial data, branchable workflows, and agent-friendly developer
experience feel native.

## Status Map

Status values:

- `TODO`: prioritized roadmap work that is not actively being implemented right now.
- `IN PROGRESS`: active implementation or design work is underway right now.
- `BACKLOG`: valuable, but not part of the near-term implementation path.

Future version values are planning buckets, not release commitments.

| Priority | Future Version | Status | Feature | Current Source Of Truth | Why This Rank |
|---:|---|---|---|---|---|
| 1 | vNext | TODO | Branch, diff, restore, and time-travel workflows | [`WIN05_BRANCH_DIFF_RESTORE_TIME_TRAVEL_IMPLEMENTATION_GUIDE.md`](WIN05_BRANCH_DIFF_RESTORE_TIME_TRAVEL_IMPLEMENTATION_GUIDE.md) | Memorable workflow for agents, test environments, migration rehearsal, and support |
| 2 | vNext | TODO | Native geospatial types and spatial indexes | [`WIN03_GEOSPATIAL_DATA_SUPPORT.md`](WIN03_GEOSPATIAL_DATA_SUPPORT.md), ADR 0124-0128 | Strong local-first wedge for mobile, field service, logistics, IoT, and offline map workflows |
| 3 | vNext | TODO | Schema-first strongly typed SDK generation | [`WIN02_SCHEMA_FIRST_STRONGLY_TYPED_SDK_GENERATION_SPEC.md`](WIN02_SCHEMA_FIRST_STRONGLY_TYPED_SDK_GENERATION_SPEC.md), ADR 0116, ADR 0129 | Adoption accelerator across languages; DecentDB metadata foundation exists |
| 4 | vNext+1 | TODO | WASM and browser OPFS support | [`WIN03_WASM_SUPPORT_IMPLEMENTATION.md`](WIN03_WASM_SUPPORT_IMPLEMENTATION.md) | Essential enabler for browser local-first apps, especially with sync, typed SDKs, and geospatial data |
| 5 | vNext+1 | TODO | Policy-aware embedded SQL | Needs ADR/spec | Strong regulated/offline/enterprise story beyond encryption alone |
| 6 | vNext+1 | TODO | Application database bundle format | Needs ADR/spec | Makes DecentDB a portable app artifact, support bundle, and sharable dataset format |
| 7 | vNext+1 | TODO | Built-in observability and `sys.*` virtual tables | Needs ADR/spec; doctor v1 is foundation | Complements doctor and makes operational state queryable |
| 8 | vNext+1 | TODO | Mature interactive SQL shell and CLI ergonomics | [`../docs/user-guide/repl.md`](../docs/user-guide/repl.md); needs product spec | Baseline developer experience and SQLite migration comfort; not identity-defining but highly visible |
| 9 | vNext+2 | TODO | Transparent write queuing and pipelining | Needs ADR/spec | Makes one-writer reality feel modern under concurrent application writes |
| 10 | vNext+2 | TODO | Group commit / WAL batching refinements | ADR 0135 | Async commit exists; strict durable group commit refinements remain |
| 11 | vNext+2 | BACKLOG | Built-in HTTP / remote server mode | Needs ADR/spec | Useful deployment multiplier, especially after write queuing exists |
| 12 | vNext+2 | BACKLOG | Cross-process WAL coordination | Needs ADR/spec | Useful for Electron, helper processes, and background sync workers |
| 13 | vNext+3 | BACKLOG | JSONB binary storage | Needs ADR/spec | Better JSON performance, but less identity-defining after SQLite JSONB |
| 14 | vNext+3 | TODO | Transparent data compression | Existing compression foundation; needs product spec | Storage/cache multiplier, especially for large overflow payloads |
| 15 | vNext+3 | TODO | Bulk-load follow-ons and external file readers | Existing bulk-load API | Extends shipped foundation into stronger import, ETL, and query-external file workflows |
| 16 | vNext+3 | BACKLOG | Non-blocking schema migration | Needs ADR/spec | Valuable for large evolving databases, but complex and not the clearest identity anchor |
| 17 | Later | BACKLOG | Native vector / HNSW index | Needs ADR/spec | AI-era checkbox, useful but less unique |
| 18 | Later | BACKLOG | Full-text search with BM25 ranking | Needs ADR/spec | Expected search capability; less distinctive because SQLite FTS is mature |
| 19 | Later | BACKLOG | Transparent data encryption | Needs ADR/spec | Practical security feature, but weaker positioning than policy-aware data controls |
| 20 | Later | BACKLOG | Advanced SQL compatibility and controlled extension surface | [`WIN04_ADVANCED_SQL_COMPATIBILITY_SURFACE.md`](WIN04_ADVANCED_SQL_COMPATIBILITY_SURFACE.md) | Useful adoption polish; controlled extension hooks cover common SQLite-extension workflows without arbitrary native loading |
| 21 | Later | BACKLOG | WAL streaming replication | Needs ADR/spec | HA/read-scale story, but not the local-first differentiator |
| 22 | Later | BACKLOG | Cloud-native object storage VFS | Needs ADR/spec | Interesting edge/serverless story, high complexity |

## Current Foundations

These should be treated as shipped or materially advanced foundations rather
than future roadmap claims:

- Native rich types: `TIMESTAMP`, `UUID`, `DECIMAL`
- JSON scalar and table functions
- Trigram indexes, expression indexes, partial indexes
- Recursive CTEs, savepoints, generated columns, temp tables
- `INSERT ... ON CONFLICT`
- `RETURNING`
- EF Core integration
- Cost-based optimizer and `ANALYZE`
- In-memory VFS for testing
- Bulk-load API foundation
- Native local-first sync: durable journal capture, batch-envelope export/import, conflict inspection, peer catalog, session tracking, scoped replication, HTTP client transport, dev sync server, `sync run`, retry handling, session inspection, conflict workflows, retention hardening, operational doctor/reporting, SDK polish through the flagship .NET JSON bridge, and complete documentation/examples
- Same-process shared WAL visibility
- Mature C ABI and multi-language binding surface
- Doctor/advisor v1 CLI, JSON, Markdown, and safe `--fix` surface
- Paged row storage, deferred table materialization, and WAL/page-cache memory work for larger embedded workloads

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
> native spatial data, branchable workflows, and AI-assisted development.

## 1. Branch, Diff, Restore, And Time-Travel Workflows

**Status:** `TODO`
**Future Version:** vNext
**Source of truth:** [`WIN05_BRANCH_DIFF_RESTORE_TIME_TRAVEL_IMPLEMENTATION_GUIDE.md`](WIN05_BRANCH_DIFF_RESTORE_TIME_TRAVEL_IMPLEMENTATION_GUIDE.md)

### Why This Matters

A copied database file is useful. A branchable data workflow is memorable.

This supports:

- cheap local branches
- schema-safe migration rehearsal
- data diffs
- point-in-time restore
- time-travel reads
- AI agent sandboxes
- reproducible support and debugging workflows

### Recommended First Work

Use the implementation guide to drive the ADR sequence before code. The first
ADRs must define:

- branch identity and metadata
- snapshot retention
- parent immutability and branch locks
- diff semantics
- restore safety
- time-travel read boundaries
- merge non-goals and narrow safe merge pathways

### Out Of Scope For The First Slice

- arbitrary Git-like relational merge
- rebasing arbitrary data branches
- OS-specific reflink dependencies
- hidden parent mutation that can corrupt branch pointers

## 2. Native Geospatial Types And Spatial Indexes

**Status:** `TODO`
**Future Version:** vNext
**Source of truth:** [`WIN03_GEOSPATIAL_DATA_SUPPORT.md`](WIN03_GEOSPATIAL_DATA_SUPPORT.md), ADR 0124-0128

### Why This Matters

Location-aware applications are common in mobile, IoT, logistics, and field
service. Combined with native local-first sync, first-class geospatial support
would let DecentDB serve offline map, dispatch, inspection, asset tracking, and
field-data workflows without requiring an external extension stack.

The goal is not to become SpatiaLite or PostGIS in the first pass. The goal is
to make location data feel like a native DecentDB capability: typed, indexable,
planner-aware, syncable, and portable across bindings.

### Current Direction

- `GEOGRAPHY` and `GEOMETRY` types
- normalized EWKB storage and ABI interchange
- `SPATIAL` secondary indexes backed by covering cells in existing B+Tree storage
- planner-native candidate generation and exact refinement
- initial high-value slice: `GEOGRAPHY(POINT,4326)` nearest-neighbor and radius queries

### Guardrails

- reuse B+Tree, WAL, page cache, and planner infrastructure
- keep initial path pure Rust
- avoid GEOS/PROJ/GDAL-style native dependency stacks for the first slice
- preserve WASM compatibility

## 3. Schema-First Strongly Typed SDK Generation

**Status:** `TODO`
**Future Version:** vNext
**Source of truth:** [`WIN02_SCHEMA_FIRST_STRONGLY_TYPED_SDK_GENERATION_SPEC.md`](WIN02_SCHEMA_FIRST_STRONGLY_TYPED_SDK_GENERATION_SPEC.md)

### Why This Matters

The engine can be excellent and still lose adoption if application integration
feels hand-built. Strong schema-first generation turns DecentDB from an embedded
database into a cross-language application platform.

### Current Foundation

DecentDB already has rich schema introspection and a one-shot schema snapshot
surface for tooling and bindings. DecentDB should own stable metadata,
query-contract validation primitives, ABI/binding guarantees, and schema export.
Decent Bench should own the primary generator workflow.

### Target Output

- generated models/types
- typed query result contracts
- parameter binding helpers
- schema drift detection
- migration compatibility checks
- deterministic regenerated output
- C#, TypeScript, and Python first, then Go, Java, and Rust

## 4. WASM And Browser OPFS Support

**Status:** `TODO`
**Future Version:** vNext+1
**Source of truth:** [`WIN03_WASM_SUPPORT_IMPLEMENTATION.md`](WIN03_WASM_SUPPORT_IMPLEMENTATION.md)

### Why This Matters

WASM is essential for browser local-first applications. It is no longer unique
enough to lead the roadmap, but it is a key enabler for the sync story.

### Required Shape

- keep the Rust core synchronous
- compile the core to `wasm32-unknown-unknown`
- run the engine inside a Dedicated Worker
- implement OPFS through synchronous access handles
- expose an async `@decentdb/web` API
- document browser durability limits honestly

### Early Gates

- ADR accepted
- parser strategy validated for WASM
- `cargo check -p decentdb --target wasm32-unknown-unknown`
- native hot paths unchanged
- minimal OPFS create/open/query/reopen smoke test

## 5. Policy-Aware Embedded SQL

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

## 6. Application Database Bundle Format

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

## 7. Built-In Observability And `sys.*` Virtual Tables

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

## 8. Mature Interactive SQL Shell And CLI Ergonomics

**Status:** `TODO`
**Future Version:** vNext+1
**Source of truth:** [`../docs/user-guide/repl.md`](../docs/user-guide/repl.md). Needs product spec before broad implementation.

### Why This Matters

SQLite's `sqlite3` shell sets a strong baseline expectation for embedded
database usability. DecentDB already has `decentdb repl`, but it is intentionally
small today. A more capable shell would make DecentDB easier to learn, debug,
demo, script, and migrate to from SQLite-style workflows.

This is not the primary market differentiator. It is visible developer
experience work that keeps the product from feeling immature next to SQLite.

### Target Capabilities

- richer `.help` output with topic-specific help
- schema inspection commands such as `.tables`, `.schema`, `.indexes`, and
  `.views`
- output controls such as `.mode`, `.headers`, `.nullvalue`, `.width`, and
  `.timer`
- script/file workflows such as `.read`, `.output`, `.once`, and safe
  interrupt/error behavior
- import/export shortcuts that delegate to existing CSV/JSON code paths
- explain/plan helpers for `EXPLAIN`, `EXPLAIN ANALYZE`, and future advisor
  surfaces
- optional parameter helpers for repeatable interactive query testing
- deeper automated coverage for piped input, interactive-style sessions,
  multiline SQL, quoted semicolons, transaction prompts, and error recovery

### Guardrails

- dot commands remain CLI shell behavior, not SQL engine syntax
- reuse existing CLI and engine helpers instead of building parallel behavior
- do not promise exact SQLite shell compatibility where DecentDB semantics
  differ
- keep unsafe file operations explicit and test path handling carefully
- avoid adding a general-purpose extension or shell escape surface as part of
  this work

### Out Of Scope

- full SQLite shell parity in one milestone
- `.load` or arbitrary extension loading
- archive/ZIP database modes
- remote sync administration inside the REPL unless backed by existing
  `decentdb sync` command behavior

## 9. Transparent Write Queuing And Pipelining

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

## 10. Group Commit / WAL Batching Refinements

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

## 11. Built-In HTTP / Remote Server Mode

**Status:** `BACKLOG`
**Future Version:** vNext+2
**Source of truth:** Needs ADR/spec before implementation.

### Why This Matters

Some users need a tiny server wrapper for edge functions, local tools, BI,
automation, or helper processes. This should remain a CLI feature, not a core
engine concern.

### Required Boundaries

- keep `crates/decentdb` network-free
- implement server logic in `crates/decentdb-cli`
- use a stateless request model for statement batches
- use simple bearer-token authentication
- avoid engine-level RBAC in this slice
- integrate with write queuing when available

## 12. Cross-Process WAL Coordination

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

## 13. JSONB Binary Storage

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

## 14. Transparent Data Compression

**Status:** `TODO`  
**Future Version:** vNext+3  
**Source of truth:** Existing compression foundation; needs product spec.

### Why This Matters

Large text, JSON, logs, and document payloads bloat file size and reduce cache
effectiveness. Compression should improve storage and scans without slowing
B+Tree traversal.

### Recommended Direction

- target overflow pages first
- avoid compressing small inline B+Tree cells
- decompress lazily only when the projected/evaluated column needs it
- use established Rust compression crates rather than custom algorithms
- expose user-facing SQL/configuration only after the storage contract is clear

## 15. Bulk-Load Follow-Ons And External File Readers

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

## 16. Non-Blocking Schema Migration

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

## 17. Native Vector / HNSW Index

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

## 18. Full-Text Search With BM25 Ranking

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

## 19. Transparent Data Encryption

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

## 20. Advanced SQL Compatibility And Controlled Extension Surface

**Status:** `BACKLOG`  
**Future Version:** Later  
**Source of truth:** [`WIN04_ADVANCED_SQL_COMPATIBILITY_SURFACE.md`](WIN04_ADVANCED_SQL_COMPATIBILITY_SURFACE.md)

### Why This Matters

DecentDB already has a broad practical SQL surface for an embedded engine. The
remaining advanced compatibility work is useful for migrations, ORMs, power
users, PostgreSQL-adjacent application code, and selected SQLite-extension
workflows that should exist without adopting SQLite's arbitrary native extension
loading model.

### Why It Is Not Higher

This is valuable adoption polish, but it is not the clearest DecentDB identity
compared with local-first sync, branchable data workflows, native geospatial
support, browser support, observability, and storage fundamentals.

### Current Direction

- schema-qualified object names
- explicit sequence objects
- materialized views
- controlled host-defined scalar and table-valued functions
- built-in table-valued helpers such as `generate_series(...)`
- binding-friendly array parameter/table-valued input support for
  `carray`-style use cases
- built-in and host-defined collation hooks
- user-defined functions and types
- deferred constraints and exclusion constraints
- covering-index execution for existing `INCLUDE (...)` metadata

Full-text search and geospatial support are tracked as separate roadmap wins.

### Guardrails

- no arbitrary runtime `.load` support in the first extension surface
- no unstable native plugin ABI before the C ABI and binding story are designed
- host-defined functions must be scoped to a connection or explicit runtime
  registration surface
- table-valued function APIs must define ownership, lifetimes, cancellation, and
  error propagation before implementation

## 21. WAL Streaming Replication

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

## 22. Cloud-Native Object Storage VFS

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

1. Write the branch/diff/restore/time-travel ADR/spec.
2. Refresh the native geospatial first-slice spec around
   `GEOGRAPHY(POINT,4326)`, radius queries, nearest-neighbor queries, and
   planner-visible `SPATIAL` indexes before implementation.
3. Continue schema-first SDK generation through Decent Bench while keeping
   DecentDB metadata authoritative.
4. Start WASM only after the browser-facing parser/durability gates are
   accepted.
5. Advance policy, bundle, `sys.*`, and interactive SQL shell designs after
   the top three roadmap items have accepted implementation specs.

## Market Notes

The roadmap order accounts for current market reality:

- Official SQLite has a WASM/OPFS story.
- SQLite has a mature command-line shell with extensive dot commands.
- SQLite has JSONB support.
- SQLite has mature FTS.
- SQLite R-Tree and Geopoly cover basic geometry use cases through virtual
  tables/extensions, but not native first-class geospatial column types.
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
