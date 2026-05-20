# DecentDB Future Wins

**Status:** Consolidated roadmap

**Updated:** 2026-05-20

**Supersedes:** earlier DecentDB future-wins roadmap drafts for roadmap prioritization

**Purpose:** Product and engineering priority index. Dedicated specs and ADRs
remain the implementation source of truth when they exist.

DecentDB should not try to win by becoming "SQLite, but with more features."
It should win by becoming the embedded SQL engine that makes durable local data,
fast reads, local-first sync, branchable workflows, browser deployment,
reactive application data, safe extensibility, and agent-readable operations
feel native.

## Consolidation Filter

This roadmap consolidates review feedback by user impact, onboarding impact, and
implementation leverage. Repeated suggestions were treated as signals, not as
commands.

Accepted high-leverage themes:

- keep performance protected through benchmarks and regression guardrails
- make the one-writer model ergonomic through engine-owned write queuing
- expose operational state through queryable `sys.*` surfaces
- add reactive query and change-stream APIs for modern app frameworks
- harden browser and sync beyond their shipped v1 foundations
- promote practical security, especially transparent data encryption
- promote native full-text search because it is a migration blocker
- keep DecentDB-owned tooling contracts authoritative while Decent Bench owns
  rich IDE/codegen workflows

Intentionally excluded or deferred from the core roadmap:

- expanded import/export workflows, external file readers, and database
  conversion features. Decent Bench is the product home for robust import,
  export, and conversion workflows. DecentDB should keep the stable engine
  contracts those tools need.
- arbitrary native extension loading
- broad foreign-data-wrapper style integration
- a general durable job queue
- text-to-SQL or LLM execution inside the core engine
- large binding rewrites unless a measured hot path requires them

## Delivered Context

These shipped foundations explain why some suggestions are framed as follow-ons
instead of brand-new roadmap items.

| Delivered Foundation | Source | Roadmap Implication |
|---|---|---|
| Local-first sync slices 1-8 | [`WIN_LOCAL_FIRST_SYNC_FIRST_CLASS_SPEC.md`](WIN_LOCAL_FIRST_SYNC_FIRST_CLASS_SPEC.md) | Future work should harden production relay, browser/mobile transport, public changesets, and diagnostics rather than rebuild sync from scratch. |
| Branch, diff, restore, and time travel | ADR 0153-0159, [`docs/api/cli-reference.md`](../docs/api/cli-reference.md#branch) | Future migration and agent workflows should use branches as the safe rehearsal layer. |
| WASM/OPFS browser v1 | ADR 0161, [`docs/api/wasm.md`](../docs/api/wasm.md) | Future browser work is about multi-tab coordination, parser/API parity, sync transport, quota diagnostics, and performance. |
| Native geospatial foundation | ADR 0124-0128, [`docs/user-guide/data-types.md`](../docs/user-guide/data-types.md#geometry), [`docs/user-guide/indexes.md`](../docs/user-guide/indexes.md#spatial-indexes) | Future spatial work is advanced analytics and planner breadth, not proving DecentDB can store spatial values. |
| Built-in HTTP server and web console | [`docs/user-guide/web-console.md`](../docs/user-guide/web-console.md), [`docs/api/cli-reference.md`](../docs/api/cli-reference.md#serve) | Future agent/tooling surfaces can reuse the local HTTP shape, but Decent Bench remains the full IDE. |
| Stable tooling metadata and query contracts | [`STABLE_TOOLING_METADATA_CONTRACT.md`](STABLE_TOOLING_METADATA_CONTRACT.md) | DecentDB owns metadata/query-contract truth. Decent Bench owns generated SDK workflows. |

## Status Map

Status values:

- `TODO`: prioritized roadmap work that is not actively being implemented right
  now.
- `IN PROGRESS`: active implementation or design work is underway right now.
- `BACKLOG`: valuable, but not part of the near-term implementation path.

Future version values are planning buckets, not release commitments.

| Priority | Future Version | Status | Feature | Current Source Of Truth | Why This Rank |
|---:|---|---|---|---|---|
| 1 | vNext | IN PROGRESS | Concurrent write ergonomics: write queue plus strict group commit | [`WIN_CONCURRENT_WRITE_ERGONOMICS_PHASED_APPROACH.md`](WIN_CONCURRENT_WRITE_ERGONOMICS_PHASED_APPROACH.md); ADR 0135 for async commit; needs ADR/spec for durable queue/group commit | Removes the most visible one-writer friction while preserving durability |
| 2 | vNext+1 | TODO | Built-in observability and `sys.*` virtual tables | Needs ADR/spec; Doctor v1 is foundation | Makes performance, locks, WAL, sync, and storage state inspectable |
| 3 | vNext+1 | TODO | Reactive query subscriptions and change streams | Needs ADR/spec; sync journal and branch diff are inputs | Modern local-first apps need live query invalidation without polling |
| 4 | vNext+2 | TODO | Production browser runtime | ADR 0161 and [`docs/api/wasm.md`](../docs/api/wasm.md); needs follow-up ADR/spec | Browser is a primary local-first runtime, and v1 intentionally lacks multi-tab/service-worker/write coordination |
| 5 | vNext+2 | TODO | Production sync relay and public changeset API | [`WIN_LOCAL_FIRST_SYNC_FIRST_CLASS_SPEC.md`](WIN_LOCAL_FIRST_SYNC_FIRST_CLASS_SPEC.md); needs follow-up ADR/spec | Turns shipped sync into a production application platform surface |
| 6 | vNext+2 | TODO | Local data security: TDE, policies, masking, audit context | Needs ADR/spec | TDE is table stakes for SQLCipher-style onboarding; policy is the differentiated regulated/offline story |
| 7 | vNext+2 | TODO | Lua extension runtime and package model | [`WIN_LUA_EXTENSION_RUNTIME_SPEC.md`](WIN_LUA_EXTENSION_RUNTIME_SPEC.md) | One sandboxed extension language is supportable across native, mobile, and WASM targets |
| 8 | vNext+2 | TODO | SQL and PRAGMA compatibility quick wins | [`WIN_ADVANCED_SQL_COMPATIBILITY_SURFACE.md`](WIN_ADVANCED_SQL_COMPATIBILITY_SURFACE.md); needs focused slice spec | Low-friction onboarding from SQLite/PostgreSQL code without chasing full clone compatibility |
| 9 | vNext+3 | TODO | Full-text search with BM25 ranking | Needs ADR/spec | Expected by app databases and a real SQLite FTS migration blocker |
| 10 | vNext+3 | TODO | Cross-process WAL coordination | Needs ADR/spec | Important for Electron/Tauri, helper processes, CLI coexistence, and background sync workers |
| 11 | vNext+3 | BACKLOG | Branch-aware migration rehearsal and promotion | ADR 0153-0159 and branch CLI/API docs; needs ADR/spec | More distinctive than generic online migration and uses shipped branch/diff foundations |
| 12 | vNext+3 | BACKLOG | Agent and tooling integration mode | [`STABLE_TOOLING_METADATA_CONTRACT.md`](STABLE_TOOLING_METADATA_CONTRACT.md); needs ADR/spec | Makes the "agent-friendly" promise concrete without putting LLM behavior in the engine |
| 13 | vNext+3 | BACKLOG | Application and support bundle format | Needs ADR/spec | Useful portable artifact and diagnostics story, but not more urgent than runtime friction |
| 14 | vNext+3 | BACKLOG | Incrementally maintained projections | Needs ADR/spec | Accelerates dashboards, local read models, and reactive query workloads |
| 15 | vNext+3 | BACKLOG | JSONB binary storage | Needs ADR/spec | Important for JSON-heavy workloads, but less urgent than FTS and runtime fundamentals |
| 16 | Later | BACKLOG | Native vector / HNSW index | Needs ADR/spec | Valuable for offline AI/RAG, but less universal than FTS and security |
| 17 | Later | BACKLOG | Temporal row history and auditable state | Needs ADR/spec | Strong regulated/support workflow, but should follow security and sync hardening |
| 18 | Later | BACKLOG | Advanced geospatial semantics and analytics | ADR 0128 deferred work; needs follow-up ADR/spec | Builds on shipped spatial support without implying the foundation is unfinished |
| 19 | Later | BACKLOG | Advanced SQL compatibility surface | [`WIN_ADVANCED_SQL_COMPATIBILITY_SURFACE.md`](WIN_ADVANCED_SQL_COMPATIBILITY_SURFACE.md) | Useful adoption polish after quick wins and higher-impact app workflows |
| 20 | Later | BACKLOG | WAL streaming replication | Needs ADR/spec | Useful HA/read-scale story, but weaker than local-first sync for DecentDB identity |
| 21 | Later | BACKLOG | Cloud-native object storage VFS | Needs ADR/spec | Interesting edge/serverless story with high durability and consistency complexity |

## Positioning

Good positioning:

- The embedded SQL database for modern local-first apps
- Embedded SQL that works offline, syncs when connected, and never loses data
- Branchable relational data for apps, agents, and edge
- Browser-capable local-first SQL with a real native core
- Durable local data with production diagnostics
- A serious application database, not just a file format

Weak positioning:

- SQLite but faster
- SQLite but with more features
- SQLite alternative
- Embedded Postgres-lite
- A general ETL tool

The remaining roadmap should support one clear lane:

> DecentDB is the embedded SQL database built for durable modern apps:
> local-first, fast, reactive, branchable, browser-capable, observable,
> securely extensible, and friendly to coding agents.

## Concurrency Position

DecentDB should preserve the one-writer / many-readers model unless a future ADR
explicitly changes it. The roadmap answer to concurrent-write friction is not
hidden multi-writer MVCC. It is:

- engine-owned write queuing
- explicit backpressure, timeout, and cancellation behavior
- strict durable group commit where several queued transactions can share one
  physical sync without weakening caller-visible durability
- cross-process coordination as a later expansion of the same contract

This should be stated plainly in user docs so developers understand that
DecentDB optimizes the single-writer model rather than pretending it is a
server database.

## 1. Concurrent Write Ergonomics: Write Queue Plus Strict Group Commit

**Status:** `IN PROGRESS`

**Future Version:** vNext

**Source of truth:** [`WIN_CONCURRENT_WRITE_ERGONOMICS_PHASED_APPROACH.md`](WIN_CONCURRENT_WRITE_ERGONOMICS_PHASED_APPROACH.md)
for phased delivery scope; ADR 0135 for current async commit behavior; needs
ADR/spec for the durable queue and strict group commit contract.

### Why This Matters

The one-writer model is correct for DecentDB, but every binding and application
should not have to rediscover queueing, retry, and busy-timeout behavior.
Concurrent application writes should feel orderly, bounded, and predictable.

### Desired Capability

Applications submit write transactions into an engine-owned queue. The engine
executes them serially through the single writer and returns each caller's
result, error, or cancellation status.

Strict group commit is the durable batching mode for queued writes:

- several committed queued transactions may share one physical WAL sync
- each transaction receives its own committed LSN/result
- caller-visible durability remains explicit
- default `WalSyncMode::Full` semantics are not weakened

### Guardrails

- preserve one-writer semantics
- bounded queue capacity and clear backpressure
- explicit timeout and cancellation behavior
- no hidden downgrade from durable commit to async commit
- no cross-process promises until cross-process WAL coordination exists
- binding APIs must expose errors consistently instead of spinning internally

## 2. Built-In Observability And `sys.*` Virtual Tables

**Status:** `TODO`

**Future Version:** vNext+1

**Source of truth:** Needs ADR/spec. Doctor v1 is the foundation.

### Why This Matters

Doctor answers "what is wrong now?" Observability answers "what is happening
while the application runs?" Queryable diagnostics are also required to make
write queuing, sync hardening, policy auditing, and future performance work
operationally visible.

### Target Surfaces

```sql
SELECT * FROM sys.wal_metrics;
SELECT * FROM sys.slow_queries;
SELECT * FROM sys.lock_waits;
SELECT * FROM sys.storage_metrics;
SELECT * FROM sys.index_usage;
SELECT * FROM sys.sync_status;
```

### Advisor Extensions

- `PRAGMA doctor`
- `sys.doctor_findings`
- Decent Bench doctor panel
- explicit `doctor --fix-plan`
- query-plan advisor
- missing/unused index advisor
- JSON path advisor after JSONB exists
- sync, branch, and browser diagnostics as those surfaces mature

### Guardrails

- near-zero overhead by default
- simple atomic counters for always-on metrics
- expensive tracing only after explicit configuration
- in-memory ring buffers for slow queries and lock waits
- no recursive disk writes for telemetry

## 3. Reactive Query Subscriptions And Change Streams

**Status:** `TODO`

**Future Version:** vNext+1

**Source of truth:** Needs ADR/spec before implementation.

### Why This Matters

Local-first and browser/mobile applications expect UI and caches to update when
the database changes. Polling undermines the product story and pushes subtle
invalidation bugs into every host language.

### Desired Capability

- subscribe to a SQL query or table/range dependency
- deliver an initial result, then invalidation events
- expose commit sequence/LSN boundaries
- support table-level invalidation first, then row-level diffs where practical
- provide C ABI and binding APIs for watch handles, callbacks, async streams, or
  equivalent language-native forms
- allow applied sync batches and branch merges to produce the same observable
  events as local writes

### Guardrails

- in-process subscriptions first; no persistent network pub/sub in this item
- no callbacks while internal locks are held
- cancellation must be explicit and leak-free
- subscriptions observe committed state only
- query re-execution and diff calculation must have bounded cost controls

## 4. Production Browser Runtime

**Status:** `TODO`

**Future Version:** vNext+2

**Source of truth:** ADR 0161, [`docs/api/wasm.md`](../docs/api/wasm.md),
and a needed follow-up ADR/spec.

### Why This Matters

Browser WASM/OPFS support is shipped, but v1 intentionally limits ownership to
one Dedicated Worker and one logical database handle. Local-first web apps need
a production story for tabs, workers, browser storage pressure, and sync.

### Required Work

- multi-tab write ownership through Shared Worker, BroadcastChannel, or another
  explicitly chosen browser coordination model
- clear behavior for service worker participation or non-participation
- OPFS lock ownership and stale-owner recovery
- browser sync transport integration
- parser/API parity strategy for common application SQL
- binary size and feature-profile work for web delivery
- quota, persistence, and durability diagnostics exposed through `metrics()` and
  `sys.*`
- browser smoke and performance coverage across supported environments

### Guardrails

- do not rewrite the Rust engine around async browser APIs
- document OPFS durability limits honestly
- avoid silent fallback to weaker storage
- keep native hot paths unaffected by browser-only code

## 5. Production Sync Relay And Public Changeset API

**Status:** `TODO`

**Future Version:** vNext+2

**Source of truth:** [`WIN_LOCAL_FIRST_SYNC_FIRST_CLASS_SPEC.md`](WIN_LOCAL_FIRST_SYNC_FIRST_CLASS_SPEC.md);
needs follow-up ADR/spec.

### Why This Matters

Shipped sync proves the core engine can capture, exchange, inspect, and resolve
changes. Production applications need hardened relay, transport, public
changeset, and shape APIs so sync is not merely a dev workflow.

### Desired Capability

- authenticated relay protocol with explicit tenant/user identity
- HTTP/WebSocket streaming shape for browser and mobile clients
- scoped "shape" subscriptions built on existing sync scopes
- public changeset/diff APIs:
  - create changeset from checkpoint, branch, or snapshot boundary
  - apply changeset transactionally
  - invert or inspect changeset when supported
  - expose conflict metadata and resolution hooks
- retention, pruning, and peer-lag diagnostics through `sys.*`
- compatibility checks for schema and query-contract drift

### Guardrails

- keep DecentDB-to-DecentDB semantics authoritative
- do not turn the engine into a hosted sync service
- external database adapters should be evaluated separately and should not
  become a core import/export replacement
- all sync metadata writes must preserve crash recovery guarantees

## 6. Local Data Security: TDE, Policies, Masking, Audit Context

**Status:** `TODO`

**Future Version:** vNext+2

**Source of truth:** Needs ADR/spec before implementation.

### Why This Matters

Transparent data encryption is a practical onboarding blocker for SQLCipher
users and regulated apps. Policy-aware SQL is a stronger DecentDB-specific
story, but it should be planned with encryption, key management, audit context,
and sync interactions rather than as an isolated syntax feature.

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

- file, WAL, temp, and metadata encryption boundaries
- key derivation and key rotation
- migration from plaintext to encrypted databases
- row filters, masked projections, and column encryption
- audit actor/context propagation through bindings
- planner implications for policies and masks
- interaction with sync, branches, bundles, and backups
- failure modes and recovery semantics

### Guardrails

- encryption must not be implied by policy syntax
- policy features must not hide rows from internal integrity checks
- audit metadata must be explicit and queryable
- key material must never be written to database pages, WAL, or telemetry

## 7. Lua Extension Runtime And Package Model

**Status:** `TODO`

**Future Version:** vNext+2

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
- DecentDB-owned typed wrappers for `DECIMAL`, `UUID`, date/time, `BLOB`, JSON,
  and spatial values
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

## 8. SQL And PRAGMA Compatibility Quick Wins

**Status:** `TODO`

**Future Version:** vNext+2

**Source of truth:** [`WIN_ADVANCED_SQL_COMPATIBILITY_SURFACE.md`](WIN_ADVANCED_SQL_COMPATIBILITY_SURFACE.md);
needs focused slice spec.

### Why This Matters

Some compatibility items are low-effort, high-visibility onboarding wins. They
make SQLite and PostgreSQL-adjacent code feel less foreign without committing
DecentDB to clone every behavior.

### Recommended Slice

- common SQLite-style inspection PRAGMAs such as `table_info` equivalents
- compatibility views/functions for common schema inspection workflows
- schema-qualified object name parsing where it maps cleanly to DecentDB's
  catalog model
- `generate_series(...)` and other small built-in table-valued helpers
- explicit collation syntax and a small set of built-in collations
- clearer `PRAGMA`/option mapping for cache, synchronous mode, and foreign-key
  enforcement where DecentDB already has equivalent configuration

### Guardrails

- do not add a compatibility alias if it implies different durability semantics
- do not make PRAGMA behavior silently diverge from SQLite in dangerous ways
- keep heavier features such as user-defined types, exclusion constraints,
  broad materialized-view semantics, and complex `MERGE` behavior in the
  advanced SQL track unless a separate ADR narrows them
- do not add core import/export features in this slice

## 9. Full-Text Search With BM25 Ranking

**Status:** `TODO`

**Future Version:** vNext+3

**Source of truth:** Needs ADR/spec before implementation.

### Why This Matters

Search is not a novelty feature for application databases. Notes, messages,
documents, local help systems, and content apps often need ranked text search.
Trigram search helps substring matching, but it does not replace FTS with
tokenization, phrase search, and ranking.

### Desired Capability

- native full-text index mode
- BM25 ranking
- phrase search
- tokenization and stemming policy
- planner integration
- incremental index maintenance through the normal write path
- binding-friendly query and ranking result types
- optional fuzzy matching and spelling-suggestion helpers as later slices

### Guardrails

- do not expose FTS through awkward virtual-table-only semantics
- avoid native dependencies that would compromise WASM/mobile portability
- define crash recovery and rebuild behavior before implementation
- benchmark against representative SQLite FTS workloads

## 10. Cross-Process WAL Coordination

**Status:** `TODO`

**Future Version:** vNext+3

**Source of truth:** Needs ADR/spec before implementation.

### Why This Matters

DecentDB already has same-process shared WAL visibility. Cross-process
coordination would support Electron/Tauri apps, helper processes, CLI/app
coexistence, and background sync workers.

### Required Work

- file-lock or shared-memory writer coordination
- cross-process reader registry or equivalent retention model
- consistent snapshots across processes
- stale-owner and crash-recovery behavior
- diagnostics for process-level blockers
- binding and CLI guidance for multi-process applications

### Guardrails

- no weakening of one-writer semantics
- no platform-specific behavior without explicit portability notes
- ADR required because this changes locking and `Send`/`Sync` boundaries
- browser multi-tab coordination is related but tracked separately

## 11. Branch-Aware Migration Rehearsal And Promotion

**Status:** `BACKLOG`

**Future Version:** vNext+3

**Source of truth:** ADR 0153-0159 and branch CLI/API docs; needs ADR/spec
before implementation.

### Why This Matters

DecentDB already has branch, diff, restore, and constrained merge. The more
distinctive migration win is not merely "non-blocking ALTER TABLE." It is a
safe workflow: branch, migrate, validate, diff, detect drift, and promote.

### Desired Capability

- create migration branch from a durable snapshot
- run schema/data migration on the branch
- validate constraints, indexes, query contracts, and sync compatibility
- produce schema and row diffs
- generate rollback/restore plan
- detect Decent Bench SDK/query-contract drift
- promote or merge safely when constraints are satisfied

### Guardrails

- online table rebuilds and dual-schema reads need separate ADR coverage
- branch merge semantics must stay conservative
- do not hide destructive schema changes behind automatic promotion

## 12. Agent And Tooling Integration Mode

**Status:** `BACKLOG`

**Future Version:** vNext+3

**Source of truth:** [`STABLE_TOOLING_METADATA_CONTRACT.md`](STABLE_TOOLING_METADATA_CONTRACT.md);
needs ADR/spec.

### Why This Matters

The "agent-friendly" claim should mean stable, machine-readable surfaces that
let coding agents, CI jobs, and Decent Bench understand a database without
guessing.

### Desired Capability

- local MCP or equivalent agent-tool server mode over the existing local HTTP
  shape
- machine-readable schema, query contract, plan, doctor, sync, and branch
  outputs
- explicit read-only and branch-sandbox modes for agent operations
- query validation without execution
- structured repair/migration proposal outputs that can be reviewed before
  execution
- stable capability manifest for bindings and tools

### Guardrails

- DecentDB should not run an LLM or natural-language agent inside the engine
- no agent write should bypass normal SQL, transaction, branch, and policy
  semantics
- Decent Bench remains the product home for rich visual workflows and generated
  SDK output

## 13. Application And Support Bundle Format

**Status:** `BACKLOG`

**Future Version:** vNext+3

**Source of truth:** Needs ADR/spec before implementation.

### Why This Matters

SQLite is often used as a portable application file format by accident.
DecentDB can make a narrower, deliberate story for portable app artifacts,
support bundles, signed datasets, and reproducible diagnostics.

### Desired Capability

A DecentDB bundle may contain:

- a checkpointed database image
- manifest metadata
- optional application assets/blobs
- optional signatures
- optional sanitized doctor/sys diagnostics
- optional branch/snapshot identifiers
- optional sync metadata summary

### Guardrails

- this is not a general import/export or ETL feature
- do not duplicate Decent Bench's rich import/export tooling
- compatibility, integrity, signature, and recovery rules need an ADR
- bundle creation must checkpoint or otherwise define WAL handling explicitly
- support bundles must have a sanitization/redaction story before use with
  regulated data

## 14. Incrementally Maintained Projections

**Status:** `BACKLOG`

**Future Version:** vNext+3

**Source of truth:** Needs ADR/spec before implementation.

### Why This Matters

Local applications often maintain denormalized read models for dashboards,
search result lists, and frequently viewed screens. DecentDB can make this a
database-native capability that also accelerates reactive queries.

### Possible Direction

- materialized projection definitions
- explicit refresh and incremental maintenance modes
- dependency tracking on base tables
- planner use when a projection can satisfy a query
- invalidation events for reactive subscribers
- diagnostics for stale or expensive projections

### Guardrails

- start with explicit opt-in projections, not hidden automatic rewrites
- keep maintenance work visible in write latency and `sys.*`
- define crash recovery and rebuild semantics before implementation

## 15. JSONB Binary Storage

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

## 16. Native Vector / HNSW Index

**Status:** `BACKLOG`

**Future Version:** Later

**Source of truth:** Needs ADR/spec before implementation.

### Why This Matters

Vector search is useful for offline AI, local RAG, and agent workflows. It is
also becoming a baseline checkbox in modern data products.

### Why It Is Not Higher

FTS, security, and write ergonomics affect more existing
embedded database users. Vector search should follow the runtime fundamentals
and avoid becoming a large storage/index project before the core engine is
faster and easier to operate.

### Desired Capability

- `VECTOR(dim)` type
- HNSW index
- similarity operators
- no external C extension requirement
- WASM/mobile portability story
- benchmarks against common vector-search extensions

## 17. Temporal Row History And Auditable State

**Status:** `BACKLOG`

**Future Version:** Later

**Source of truth:** Needs ADR/spec before implementation.

### Why This Matters

Branch/time-travel, sync journals, and policy-aware SQL create a path toward
auditable local data. Some regulated and support-heavy apps need to answer:
"what changed, who changed it, from which replica or branch, and why?"

### Possible Direction

- temporal tables or opt-in row history
- `FOR SYSTEM_TIME AS OF` style query surface if it fits the planner
- `sys.row_history` inspection surface
- actor/context metadata from bindings
- retention and redaction policies
- sync and branch provenance fields

### Guardrails

- history must be opt-in and storage-cost visible
- redaction must be compatible with retention and audit requirements
- do not conflate branch snapshots with row-level audit history

## 18. Advanced Geospatial Semantics And Analytics

**Status:** `BACKLOG`

**Future Version:** Later

**Source of truth:** ADR 0128 deferred work; needs follow-up ADR/spec before
implementation.

### Why This Matters

Native geospatial types, spatial indexes, planner-visible filters, distance
ordering, and the first point-in-polygon spatial join path are shipped
foundations. More advanced GIS workloads still need a separate future track so
the completed native geospatial feature does not appear unfinished.

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

## 19. Advanced SQL Compatibility Surface

**Status:** `BACKLOG`

**Future Version:** Later

**Source of truth:** [`WIN_ADVANCED_SQL_COMPATIBILITY_SURFACE.md`](WIN_ADVANCED_SQL_COMPATIBILITY_SURFACE.md)

### Why This Matters

DecentDB already has a broad practical SQL surface for an embedded engine. The
remaining advanced compatibility work is useful for migrations, ORMs, power
users, and PostgreSQL-adjacent application code. Quick wins are tracked earlier;
this item is for heavier compatibility work.

### Current Direction

- explicit sequence objects
- materialized views that are not covered by the projection track
- binding-friendly array parameter/table-valued input support for
  `carray`-style use cases
- SQL-defined functions if DecentDB chooses to support them separately from Lua
  extensions
- user-defined types
- deferred constraints and exclusion constraints
- covering-index execution for existing `INCLUDE (...)` metadata
- `MERGE INTO` if scoped carefully for sync/upsert workflows

### Guardrails

- do not duplicate the Lua extension runtime
- do not add arbitrary native `.load` support here
- keep this track focused on SQL syntax, catalog compatibility, and migration
  ergonomics
- avoid expanding core import/export features in this track

## 20. WAL Streaming Replication

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

## 21. Cloud-Native Object Storage VFS

**Status:** `BACKLOG`

**Future Version:** Later

**Source of truth:** Needs ADR/spec before implementation.

### Why This Matters

Serverless and edge deployments often have ephemeral local disks. An object
storage VFS could allow page-level reads and writes against S3, R2, Azure Blob,
or HTTP-backed storage.

### Why It Is Last

This is high complexity and has serious durability, latency, consistency, and
cache-invalidation risks. It should follow stronger local-first, browser,
performance, and operational foundations.

### Required Direction

- expand VFS semantics deliberately
- use HTTP range requests for page reads
- use local cache aggressively
- define write coordination and consistency rules before implementation

## Near-Term Sequence

1. Draft ADR/spec for concurrent write queuing plus strict durable group commit.
2. Draft ADR/spec for `sys.*` observability and wire its first counters into the
   performance/write-queue work.
3. Design reactive subscriptions around committed-state invalidation and binding
   APIs.
4. Design production browser and sync follow-ons together so browser transport,
   multi-tab ownership, relay shape, and changeset APIs do not conflict.
5. Advance local data security and Lua extension work after the runtime
   foundations above are underway.

## Market Notes

The roadmap order accounts for competitive pressure without becoming a feature
clone checklist:

- SQLite has mature FTS, JSONB, WASM/OPFS, PRAGMAs, and CLI workflows.
- SQLCipher-style encrypted local files are a common requirement for mobile,
  desktop, healthcare, finance, and enterprise apps.
- SQLite-compatible ecosystems such as libSQL/Turso and Limbo create pressure
  around sync, embedded deployment, vector search, and SQLite familiarity.
- DuckDB has strong ingestion, extension, FTS, vector, and analytics stories.
  Decent Bench, not DecentDB core, should own rich import/export and conversion
  workflows.
- Local-first stacks such as PGlite/Electric and PowerSync make reactive
  queries, browser/mobile sync, and shape subscriptions part of the expected
  developer conversation.
- SQLite and DuckDB have mature extension ecosystems. DecentDB's proposed
  response is one official Lua extension language with strict manifests,
  sandboxing, and explicit trust rather than arbitrary native extension loading.
- The largest DecentDB opportunity is integrated durable local-first workflow:
  fast embedded reads/writes, sync, branches, browser runtime, observability,
  security, and agent-readable tooling.

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
