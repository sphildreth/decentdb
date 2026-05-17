# DecentDB Future Wins

**Status:** Consolidated roadmap  
**Supersedes:** the earlier DecentDB future-wins roadmap drafts for roadmap prioritization  
**Purpose:** Product and engineering priority index. Dedicated specs and ADRs remain the implementation source of truth when they exist.

DecentDB should not try to win by becoming "SQLite, but with more features."
It should win by becoming the embedded SQL engine that makes local-first sync,
branchable data workflows, and agent-friendly developer experience feel native.

## Status Map

Status values:

- `TODO`: prioritized roadmap work that is not actively being implemented right now.
- `IN PROGRESS`: active implementation or design work is underway right now.
- `BACKLOG`: valuable, but not part of the near-term implementation path.

Future version values are planning buckets, not release commitments.

| Priority | Future Version | Status | Feature | Current Source Of Truth | Why This Rank |
|---:|---|---|---|---|---|
| 1 | vNext | TODO | Native local-first sync, changesets, CDC, and merge | [`WIN01_LOCAL_FIRST_SYNC_FIRST_CLASS_SPEC.md`](WIN01_LOCAL_FIRST_SYNC_FIRST_CLASS_SPEC.md), ADR 0147 | Strongest identity-level differentiator and real application painkiller |
| 2 | vNext | TODO | Branch, diff, restore, and time-travel workflows | Needs ADR/spec | Memorable workflow for agents, test environments, migration rehearsal, and support |
| 3 | vNext | TODO | Schema-first strongly typed SDK generation | [`WIN02_SCHEMA_FIRST_STRONGLY_TYPED_SDK_GENERATION_SPEC.md`](WIN02_SCHEMA_FIRST_STRONGLY_TYPED_SDK_GENERATION_SPEC.md), ADR 0116, ADR 0129 | Adoption accelerator across languages; DecentDB metadata foundation exists |
| 4 | vNext+1 | TODO | WASM and browser OPFS support | [`WIN03_WASM_SUPPORT_IMPLEMENTATION.md`](WIN03_WASM_SUPPORT_IMPLEMENTATION.md) | Essential enabler for browser local-first apps, but no longer unique by itself |
| 5 | vNext+1 | TODO | Application database bundle format | Needs ADR/spec | Makes DecentDB a portable app artifact, support bundle, and sharable dataset format |
| 6 | vNext+1 | TODO | Built-in observability and `sys.*` virtual tables | Needs ADR/spec; doctor v1 is foundation | Complements doctor and makes operational state queryable |
| 7 | vNext+2 | BACKLOG | Policy-aware embedded SQL | Needs ADR/spec | Strong regulated/offline/enterprise story beyond encryption alone |
| 8 | vNext+2 | BACKLOG | Built-in HTTP / remote server mode | Needs ADR/spec | Useful deployment multiplier, especially with write queuing |
| 9 | vNext+2 | TODO | Transparent write queuing and pipelining | Needs ADR/spec | Makes one-writer reality feel modern under concurrent application writes |
| 10 | vNext+2 | TODO | Group commit / WAL batching refinements | ADR 0135 | Async commit exists; strict durable group commit refinements remain |
| 11 | vNext+2 | BACKLOG | Cross-process WAL coordination | Needs ADR/spec | Useful for Electron, helper processes, and background sync workers |
| 12 | vNext+3 | BACKLOG | JSONB binary storage | Needs ADR/spec | Better JSON performance, but less identity-defining after SQLite JSONB |
| 13 | vNext+3 | TODO | Transparent data compression | Existing compression foundation; needs product spec | Storage/cache multiplier, especially for large overflow payloads |
| 14 | vNext+3 | TODO | Bulk-load follow-ons | Existing bulk-load API | Extends shipped foundation into stronger import and ETL workflows |
| 15 | vNext+3 | BACKLOG | Non-blocking schema migration | Needs ADR/spec | Valuable for large evolving databases, but complex and not the clearest identity anchor |
| 16 | vNext+3 | TODO | Native geospatial types and spatial indexes | [`WIN03_GEOSPATIAL_DATA_SUPPORT.md`](WIN03_GEOSPATIAL_DATA_SUPPORT.md), ADR 0124-0128 | Strong feature-completeness win for location-heavy verticals |
| 17 | Later | BACKLOG | Native vector / HNSW index | Needs ADR/spec | AI-era checkbox, useful but less unique |
| 18 | Later | BACKLOG | Full-text search with BM25 ranking | Needs ADR/spec | Expected search capability; less distinctive because SQLite FTS is mature |
| 19 | Later | BACKLOG | Transparent data encryption | Needs ADR/spec | Practical security feature, but weaker positioning than policy-aware data controls |
| 20 | Later | BACKLOG | Advanced SQL compatibility surface | [`WIN04_ADVANCED_SQL_COMPATIBILITY_SURFACE.md`](WIN04_ADVANCED_SQL_COMPATIBILITY_SURFACE.md) | Useful adoption polish, but not an identity-level differentiator |
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
- Local sync Slices 1-3 complete: durable journal capture, batch-envelope export/import, conflict inspection, peer catalog, session tracking, HTTP client transport, dev sync server, `sync run`, retry handling, and session inspection
- Same-process shared WAL visibility
- Mature C ABI and multi-language binding surface
- Doctor/advisor v1 CLI, JSON, Markdown, and safe `--fix` surface
- Paged row storage, deferred table materialization, and WAL/page-cache memory work for larger embedded workloads

## Positioning

Good positioning:

- The embedded SQL database for modern local-first apps
- Branchable relational data for apps, agents, and edge
- Embedded SQL with native sync
- A serious application database, not just a file format

Weak positioning:

- SQLite but faster
- SQLite but with more features
- SQLite alternative
- Embedded Postgres-lite

The remaining roadmap should support one clear lane:

> DecentDB is the embedded SQL database built for modern apps, offline sync,
> branchable data workflows, and AI-assisted development.

## 1. Native Local-First Sync, Changesets, CDC, And Merge

**Status:** `TODO`  
**Future Version:** vNext  
**Source of truth:** [`WIN01_LOCAL_FIRST_SYNC_FIRST_CLASS_SPEC.md`](WIN01_LOCAL_FIRST_SYNC_FIRST_CLASS_SPEC.md), ADR 0147, ADR 0148

### Why This Is First

This is the strongest product identity available to DecentDB. Embedded databases
are good local stores, but offline writes, background sync, conflict handling,
selective replication, and device-to-device movement usually require custom
middleware or third-party products.

The DecentDB win is native SQL-first local/offline sync with conflict-aware merge
semantics as an engine capability.

### Completed Foundation

Slices 1-3 are complete:

- replica identity
- sync enablement metadata
- durable sidecar change journal
- transaction sequence numbers
- tombstones for deletes
- pending-change enumeration
- SQL inspection views
- sync journal integrity checks
- restart/replay and sync journal fault-injection tests
- batch envelope export/import
- conflict recording and inspection
- peer watermarks and journal pruning
- machine-readable sync CLI status, pending, and conflicts output
- peer catalog and credential-reference metadata
- HTTP sync client transport and sync-only dev server
- handshake/capability checks, retry behavior, and session inspection

### Next Implementable Slice

The next work should move to scoped replication now that the peer/session and
transport foundation exists:

- scoped sync and row filters
- scope-to-peer bindings
- capture/apply validation for scoped data
- scoped sync diagnostics and tests

### Later Slices

- manual exchange hardening beyond the Slice 3 transport foundation
- conflict resolution workflows and policies
- retention ergonomics, crash-hardened prune rewrites, and deeper sync doctor checks
- SDK polish, beginning with .NET

### Guardrails

- Preserve durable ACID writes as priority 1.
- Do not weaken WAL semantics.
- Do not add transport before local journal correctness is proven.
- Do not hide conflicts behind silent last-write-wins defaults.
- Keep sync state inspectable by humans, CI, and agents.

## 2. Branch, Diff, Restore, And Time-Travel Workflows

**Status:** `TODO`  
**Future Version:** vNext  
**Source of truth:** Needs ADR/spec before implementation.

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

Write an ADR/spec before code. It must define:

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

## 5. Application Database Bundle Format

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

## 6. Built-In Observability And `sys.*` Virtual Tables

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

## 7. Policy-Aware Embedded SQL

**Status:** `BACKLOG`  
**Future Version:** vNext+2  
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

## 8. Built-In HTTP / Remote Server Mode

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

## 11. Cross-Process WAL Coordination

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

## 12. JSONB Binary Storage

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

## 13. Transparent Data Compression

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

## 14. Bulk-Load Follow-Ons

**Status:** `TODO`  
**Future Version:** vNext+3  
**Source of truth:** Existing bulk-load API and CLI.

### Current Foundation

DecentDB already ships a bulk-load API and CLI workflows.

### Future Win

Build higher-level ingestion workflows:

- `COPY`-style SQL or CLI import commands
- CSV/JSON streaming readers for datasets larger than memory
- sorted-input hints for index-friendly loading
- better progress reporting
- resumable import ergonomics
- stronger benchmark coverage for ETL-style workloads

## 15. Non-Blocking Schema Migration

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

## 16. Native Geospatial Types And Spatial Indexes

**Status:** `TODO`  
**Future Version:** vNext+3  
**Source of truth:** [`WIN03_GEOSPATIAL_DATA_SUPPORT.md`](WIN03_GEOSPATIAL_DATA_SUPPORT.md), ADR 0124-0128

### Why This Matters

Location-aware applications are common in mobile, IoT, logistics, and field
service. The goal is not to become SpatiaLite. The goal is first-class location
data without extensions.

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

## 20. Advanced SQL Compatibility Surface

**Status:** `BACKLOG`  
**Future Version:** Later  
**Source of truth:** [`WIN04_ADVANCED_SQL_COMPATIBILITY_SURFACE.md`](WIN04_ADVANCED_SQL_COMPATIBILITY_SURFACE.md)

### Why This Matters

DecentDB already has a broad practical SQL surface for an embedded engine. The
remaining advanced compatibility work is useful for migrations, ORMs, power
users, and PostgreSQL-adjacent application code.

### Why It Is Not Higher

This is valuable adoption polish, but it is not the clearest DecentDB identity
compared with local-first sync, branchable data workflows, browser support,
observability, and storage fundamentals.

### Current Direction

- schema-qualified object names
- explicit sequence objects
- materialized views
- user-defined functions and types
- deferred constraints and exclusion constraints
- covering-index execution for existing `INCLUDE (...)` metadata

Full-text search and geospatial support are tracked as separate roadmap wins.

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

1. Implement local-first sync Slice 0/1: ADRs, replica identity, durable local
   journal, tombstones, and pending-change inspection.
2. Write the branch/diff/restore/time-travel ADR/spec.
3. Continue schema-first SDK generation through Decent Bench while keeping
   DecentDB metadata authoritative.
4. Extend doctor into `sys.*`/advisor surfaces after the first sync and branch
   metadata exists.
5. Start WASM only after the sync foundation has a clear browser-facing story
   and the parser/durability gates are accepted.

## Market Notes

The roadmap order accounts for current market reality:

- Official SQLite has a WASM/OPFS story.
- SQLite has JSONB support.
- SQLite has mature FTS.
- SQLite R-Tree and Geopoly cover basic geometry use cases.
- DuckDB has broadened through FTS and vector extensions.
- The largest gap is integrated workflow, not raw feature count.

Useful references:

- SQLite WASM / OPFS: https://sqlite.org/wasm/doc/trunk/persistence.md
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
