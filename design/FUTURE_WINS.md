# DecentDB Future Wins

**Status:** Consolidated roadmap

**Updated:** 2026-05-27

**Supersedes:** earlier DecentDB future-wins roadmap drafts for roadmap
prioritization.

**Purpose:** Product and engineering priority index. Dedicated specs and ADRs
remain the implementation source of truth when they exist.

DecentDB should not try to win by becoming "SQLite, but with more features."
It should win by becoming the embedded SQL engine that makes durable local data,
fast reads, local-first sync, branchable workflows, browser and mobile
deployment, reactive application data, safe extensibility, and agent-readable
operations feel native.

## Consolidation Filter

This roadmap is ordered by adoption impact, product differentiation,
implementation leverage, and the ability to make DecentDB more compelling than
other embedded database engines without abandoning its durability-first identity.

Accepted high-leverage themes:

- remove adoption blockers that make teams choose SQLite, SQLCipher, libSQL,
  PGlite, DuckDB, or app-managed SQLite sync instead
- keep performance protected through benchmark profiles, release metrics, and
  regression guardrails
- build on the shipped engine-owned write queue for process/browser
  coordination
- build on shipped queryable `sys.*` surfaces for tracing and advisors
- build on shipped reactive query and change-stream APIs for projections and
  sync-driven application state
- harden browser and mobile beyond the shipped browser runtime and relay
  foundations
- promote practical local data security, especially transparent data encryption
- build on delivered native full-text search when planning hybrid search and
  relevance workflows
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
| Write queue plus strict group commit | [`WIN_CONCURRENT_WRITE_ERGONOMICS_PHASED_APPROACH.md`](_archive/WIN_CONCURRENT_WRITE_ERGONOMICS_PHASED_APPROACH.md); ADR 0162 | Future concurrency work should extend the shipped one-writer queue contract into cross-process, browser, and mobile coordination rather than reopen multi-writer semantics. |
| Operational `sys.*` metrics | ADR 0163, [`docs/api/sql-functions.md`](../docs/api/sql-functions.md#operational-inspection-views); 2.7.0 prepared-statement fix | Future tracing/advisor work should build on the stable metrics contract without adding always-on hot-path overhead. |
| Reactive subscriptions and change streams | [ADR 0164](adr/0164-reactive-query-subscriptions-and-change-streams.md), [`WIN_REACTIVE_QUERY_SUBSCRIPTIONS_CHANGE_STREAMS_SPEC.md`](_archive/WIN_REACTIVE_QUERY_SUBSCRIPTIONS_CHANGE_STREAMS_SPEC.md) | Future projection and sync-shape work can reuse committed invalidation/change-stream semantics instead of inventing another notification layer. |
| Local-first sync slices 1-8 | [`WIN_LOCAL_FIRST_SYNC_FIRST_CLASS_SPEC.md`](WIN_LOCAL_FIRST_SYNC_FIRST_CLASS_SPEC.md) | Future work should harden production relay, browser/mobile transport, backend bridges, public changesets, and diagnostics rather than rebuild sync from scratch. |
| Production sync relay and public changeset API | [`WIN_PRODUCTION_RELAY_SPEC.md`](_archive/WIN_PRODUCTION_RELAY_SPEC.md); ADR 0166-0168; [`docs/user-guide/sync/relay.md`](../docs/user-guide/sync/relay.md); [`docs/user-guide/sync/changesets.md`](../docs/user-guide/sync/changesets.md) | Public changesets, authenticated relay v2 HTTP/WebSocket routes, sync shapes, browser relay helpers, C ABI JSON entry points, .NET JSON helpers, and relay diagnostics are now delivered foundations. |
| SQL and PRAGMA compatibility quick wins | [`WIN_SQL_PRAGMA_COMPATIBILITY_QUICK_WINS_SPEC.md`](_archive/WIN_SQL_PRAGMA_COMPATIBILITY_QUICK_WINS_SPEC.md); [`docs/user-guide/sql-reference.md`](../docs/user-guide/sql-reference.md); [`docs/api/configuration.md`](../docs/api/configuration.md) | Safe SQLite-style PRAGMAs, compatibility catalog views, minimal `information_schema`, `generate_series`, `main.`/`temp.` qualifiers, query-time built-in collations, and scalar compatibility helpers are delivered onboarding surfaces. |
| Branch, diff, restore, and time travel | ADR 0153-0159, [`docs/api/cli-reference.md`](../docs/api/cli-reference.md#branch); 2.7.0 Dart branch workflow APIs | Future migration, support, and agent workflows should use branches as the safe rehearsal layer. |
| WASM/OPFS browser runtime | ADR 0161, ADR 0165, [`docs/api/wasm.md`](../docs/api/wasm.md), and `@decentdb/web` updates | Browser now has explicit capability probes, OPFS owner routing, Web Locks/BroadcastChannel coordination, relay helpers, diagnostics, and smoke/benchmark coverage. Follow-on work is SQL/API parity, performance, and production packaging. |
| Native geospatial foundation | ADR 0124-0128, [`docs/user-guide/data-types.md`](../docs/user-guide/data-types.md#geometry), [`docs/user-guide/indexes.md`](../docs/user-guide/indexes.md#spatial-indexes) | Future spatial work is advanced analytics and planner breadth, not proving DecentDB can store spatial values. |
| Built-in HTTP server and web console | [`docs/user-guide/web-console.md`](../docs/user-guide/web-console.md), [`docs/api/cli-reference.md`](../docs/api/cli-reference.md#serve) | Future agent/tooling surfaces can reuse the local HTTP shape, but Decent Bench remains the full IDE. |
| Stable tooling metadata and query contracts | [`STABLE_TOOLING_METADATA_CONTRACT.md`](STABLE_TOOLING_METADATA_CONTRACT.md) | DecentDB owns metadata/query-contract truth. Decent Bench owns generated SDK workflows. |
| Lua extension runtime and package model | [`WIN_LUA_EXTENSION_RUNTIME_SPEC.md`](_archive/WIN_LUA_EXTENSION_RUNTIME_SPEC.md); ADR 0169-0173; [`docs/user-guide/lua-extensions.md`](../docs/user-guide/lua-extensions.md) | Sandboxed Lua packages now provide DecentDB's safe extensibility story: manifest validation, install/enable/trust lifecycle, scalar functions, table-valued functions, aggregates, query-time collations, Rust/CLI/C ABI surfaces, and examples. |
| Local data security v1 | [ADR 0174](adr/0174-local-data-security-tde-policies-masking-audit-context.md); [`docs/user-guide/security.md`](../docs/user-guide/security.md); [`docs/api/configuration.md`](../docs/api/configuration.md#local-transparent-data-encryption-tde) | Transparent local encryption, durable row policies, projection masks, audit context, C ABI open options, and queryable audit context are delivered foundations. Future security work should extend this boundary rather than redefining it. |
| Native full-text search with BM25 ranking | [`WIN_FULL_TEXT_SEARCH_BM25_SPEC.md`](WIN_FULL_TEXT_SEARCH_BM25_SPEC.md); ADR 0175-0176; [`docs/user-guide/indexes.md`](../docs/user-guide/indexes.md#full-text-indexes) | `USING fulltext` indexes, analyzer metadata, `fulltext_match`, `bm25`, phrase/prefix queries, write-path maintenance, rebuild/verify, tooling metadata, documentation, and regression tests are delivered. Follow-on search work now belongs under hybrid search, fuzziness/suggestions, or performance-specific roadmap items. |
| Cross-process WAL coordination | [`WIN_CROSS_PROCESS_WAL_COORDINATION_SPEC.md`](WIN_CROSS_PROCESS_WAL_COORDINATION_SPEC.md); ADR 0177-0180; [`docs/user-guide/write-concurrency.md`](../docs/user-guide/write-concurrency.md#cross-process-wal-coordination) | Native OS processes can safely share local on-disk databases through byte-range locks, `.coord` sidecars, cross-process reader retention, WAL refresh, binding open options, `sys.process_*` diagnostics, and Doctor findings. Future runtime work can build on this foundation for browser/mobile hardening and richer operational tracing. |
| Benchmark profiles and release assets | [`BENCHMARKING_GUIDE.md`](BENCHMARKING_GUIDE.md), `crates/decentdb-benchmark`, `data/bench_summary.json` | Performance work should target measured default and tuned profiles, storage efficiency, cold-open behavior, and release-chart regressions. |

## Status Map

Status values:

- `TODO`: prioritized roadmap work that is not actively being implemented right
  now.
- `IN PROGRESS`: active implementation or design work is underway right now.
- `BACKLOG`: valuable, but not part of the near-term implementation path.

Future version values are planning buckets, not release commitments. The latest
public release in this repository is `2.7.0`. `vNext` means the first release
bucket after `2.7.0` only when scope is explicitly accepted. `vNext+1` and
`vNext+2` are follow-on planning buckets, not exact semantic versions.

Roadmap lifecycle: once a Future Win is 100% implemented, tested, and
documented, remove it from this roadmap. Completed and delivered work is no
longer a Future Win. Keep only a concise `Delivered Context` entry when the
shipped foundation affects follow-on roadmap decisions.

| Priority | Future Version | Status | Feature | Current Source Of Truth | Why This Rank |
|---:|---|---|---|---|---|
| 1 | vNext | TODO | Browser SQL/API parity and production web hardening | [`WIN_BROWSER_SQL_API_PARITY_PRODUCTION_WEB_SPEC.md`](WIN_BROWSER_SQL_API_PARITY_PRODUCTION_WEB_SPEC.md) | DecentDB has a browser runtime; the next adoption hurdle is making it feel complete next to SQLite WASM and PGlite |
| 2 | vNext | TODO | Mobile production runtime and SDK hardening | Needs ADR/spec | Local-first without first-class iOS/Android lifecycle, packaging, key storage, and background sync guidance leaves a major adoption gap |
| 3 | vNext | TODO | Default-fast performance and storage efficiency | Benchmarking guide and release metrics; needs ADR/spec for format-affecting work | DecentDB should feel fast without special tuning and should keep file size/cold-open behavior competitive |
| 4 | vNext+1 | TODO | Runtime tracing, advisors, and Doctor integration | Needs ADR/spec; follows shipped operational metrics | Explains slow queries, lock waits, index usage, and maintenance issues once the metrics contract is stable |
| 5 | vNext+1 | TODO | Branch-aware migration rehearsal and promotion | ADR 0153-0159 and branch CLI/API docs; needs ADR/spec | Uses shipped branch/diff foundations for a distinctive safe migration workflow |
| 6 | vNext+1 | TODO | Backend sync bridge for existing app databases, Postgres first | Needs ADR/spec | Makes DecentDB easier to adopt in apps that already have a central Postgres/Supabase-style backend |
| 7 | Later | BACKLOG | Incrementally maintained projections | Needs ADR/spec | Accelerates dashboards, local read models, and reactive query workloads |
| 8 | Later | BACKLOG | JSONB binary storage and JSON path indexing | Needs ADR/spec | Important for JSON-heavy workloads and now a SQLite baseline expectation |
| 9 | Later | BACKLOG | Hybrid local search: FTS, trigram, vector, and rank fusion | FTS foundation is delivered; vector and rank fusion need ADR/spec | More compelling than standalone HNSW: apps want keyword, substring, semantic, and relational filters together |
| 10 | Later | BACKLOG | Authenticated encryption, key rotation, and platform key-store helpers | ADR 0174 follow-up | TDE v1 provides local confidentiality; regulated deployments eventually need tamper-evident page/chunk authentication, key rotation, and turnkey OS/browser/mobile key-store guidance |
| 11 | Later | BACKLOG | Agent and tooling integration mode | [`STABLE_TOOLING_METADATA_CONTRACT.md`](STABLE_TOOLING_METADATA_CONTRACT.md); needs ADR/spec | Makes the agent-friendly promise concrete without putting LLM behavior in the engine |
| 12 | Later | BACKLOG | Application and support bundle format | Needs ADR/spec | Useful portable artifact and diagnostics story, but should follow security/redaction foundations |
| 13 | Later | BACKLOG | Temporal row history and auditable state | Needs ADR/spec | Strong regulated/support workflow, but should follow security, audit context, and sync hardening |
| 14 | Later | BACKLOG | Advanced geospatial semantics and analytics | ADR 0128 deferred work; needs follow-up ADR/spec | Builds on shipped spatial support without implying the foundation is unfinished |
| 15 | Later | BACKLOG | Advanced SQL compatibility surface | [`WIN_ADVANCED_SQL_COMPATIBILITY_SURFACE.md`](WIN_ADVANCED_SQL_COMPATIBILITY_SURFACE.md) | Useful adoption polish after higher-impact runtime and workflow blockers |
| 16 | Later | BACKLOG | WAL streaming replication | Needs ADR/spec | Useful HA/read-scale story, but weaker than local-first sync for DecentDB identity |
| 17 | Later | BACKLOG | Cloud-native object storage VFS | Needs ADR/spec | Interesting edge/serverless story with high durability and consistency complexity |

## Positioning

Good positioning:

- The embedded SQL database for modern local-first apps
- Embedded SQL that works offline, syncs when connected, and never loses data
- Branchable relational data for apps, agents, and edge
- Browser-capable and mobile-ready local-first SQL with a real native core
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
> local-first, fast, reactive, branchable, browser-capable, mobile-ready,
> observable, securely extensible, and friendly to coding agents.

## Concurrency Position

DecentDB should preserve the one-writer / many-readers model unless a future ADR
explicitly changes it. The shipped answer to concurrent-write friction is not
hidden multi-writer MVCC. It is:

- engine-owned write queuing
- explicit backpressure, timeout, and cancellation behavior
- strict durable group commit where several queued transactions can share one
  physical sync without weakening caller-visible durability
- cross-process coordination as a later expansion of the same contract

This should be stated plainly in user docs so developers understand that
DecentDB optimizes the single-writer model rather than pretending it is a server
database.

## 1. Browser SQL/API Parity And Production Web Hardening

**Status:** `TODO`

**Future Version:** vNext

**Source of truth:** [`WIN_BROWSER_SQL_API_PARITY_PRODUCTION_WEB_SPEC.md`](WIN_BROWSER_SQL_API_PARITY_PRODUCTION_WEB_SPEC.md)
and ADR 0161/0165.

### Why This Matters

The shipped browser runtime proves DecentDB can run in a Dedicated Worker with
OPFS persistence, owner routing, relay helpers, and browser diagnostics. The
next adoption hurdle is completeness. Developers comparing DecentDB with SQLite
WASM or PGlite will notice SQL-subset limits, package-size/startup costs,
framework integration friction, and storage lifecycle questions before they
notice deeper engine advantages.

### Desired Capability

- broader browser SQL parser parity with native DecentDB
- prepared statement, result transport, and large result performance targets
- stable bundler recipes for Vite, Next.js, SvelteKit, Electron, and Tauri webviews
- OPFS recovery, quota, persistence, and export/import user guidance
- browser relay sync examples that apply/ack changesets safely
- browser benchmark guardrails for startup, query latency, result decoding, and
  WASM memory growth

### Guardrails

- do not silently fall back to weak storage under the browser durability contract
- keep capability probing explicit
- keep service-worker ownership unsupported unless a new ADR proves it safe
- do not make browser parity depend on arbitrary native extension loading

## 2. Mobile Production Runtime And SDK Hardening

**Status:** `TODO`

**Future Version:** vNext

**Source of truth:** Needs ADR/spec before implementation.

### Why This Matters

Local-first adoption is heavily mobile. DecentDB has a Dart/Flutter desktop
binding and a browser package, but first-class iOS/Android use needs lifecycle,
packaging, key storage, background sync, and platform test guidance. Without
that story, teams building offline field apps, healthcare apps, finance apps,
and consumer mobile apps will default to SQLite plus a sync layer.

### Desired Capability

- documented iOS and Android support tiers
- Flutter mobile packaging, examples, and smoke tests
- keychain/keystore integration guidance for encrypted databases after TDE
- app lifecycle rules for suspend/resume, background sync, file locks, and crash
  recovery
- mobile relay sync examples with durable local apply/ack behavior
- optional Swift/Kotlin/React Native SDK strategy if demand justifies it

### Guardrails

- do not claim background sync guarantees the OS will not honor
- keep mobile file and key lifecycle explicit
- avoid duplicating every binding surface before the C ABI contract is stable
- require real device or simulator smoke coverage before calling a platform
  supported

## 3. Default-Fast Performance And Storage Efficiency

**Status:** `TODO`

**Future Version:** vNext

**Source of truth:** Benchmarking guide and release metrics exist; needs
ADR/spec for persistent-format or planner-contract changes.

### Why This Matters

DecentDB's tuned durable profile is competitive in the release benchmark assets,
but developers judge the engine by defaults, cold-open behavior, file size,
prepared-statement paths, and predictable performance under normal app use.
Making safe defaults fast is more valuable than adding features that only shine
after manual tuning.

### Target Areas

- reduce the gap between default durable and tuned durable profiles
- improve cold-open and first-query behavior for large databases
- improve file size and WAL/checkpoint storage efficiency
- teach the planner/executor to exploit covering indexes where metadata already
  exists
- keep `ANALYZE` and stats useful without turning tuning into required ritual
- improve binding prepared-statement hot paths where measurements show overhead
- add performance Doctor findings only after the runtime can explain them

### Guardrails

- do not weaken durable defaults to win charts
- do not change persistent formats without ADR and migration coverage
- benchmark both native and maintained binding paths when a change targets
  application-facing latency
- keep profile names explicit so tuned and default results are not conflated

## 4. Runtime Tracing, Advisors, And Doctor Integration

**Status:** `TODO`

**Future Version:** vNext+1

**Source of truth:** Needs ADR/spec. Follows the shipped operational metrics
contract.

### Why This Matters

Operational metrics expose current state cheaply. Runtime tracing and advisors
explain why performance, contention, or maintenance problems are happening over
time. This work is valuable, but it should not quietly add overhead to hot
paths.

### Target Surfaces

```sql
SELECT * FROM sys.slow_queries;
SELECT * FROM sys.lock_waits;
SELECT * FROM sys.index_usage;
SELECT * FROM sys.doctor_findings;
```

### Advisor Extensions

- `PRAGMA doctor`
- Decent Bench doctor panel
- explicit `doctor --fix-plan`
- query-plan advisor
- missing/unused index advisor
- JSON path advisor after JSONB exists
- sync, branch, browser, and mobile diagnostics as those surfaces mature

### Required Design Topics

- explicit opt-in configuration for expensive tracing
- in-memory ring buffer sizes, eviction policy, and reset semantics
- SQL text and parameter redaction policy
- lock-wait source classification
- index-usage attribution from planner and executor paths
- Doctor report projection into queryable rows
- advisor severity, confidence, and automation boundaries

### Guardrails

- no recursive disk writes for telemetry
- no tracing while internal locks are held longer than necessary
- no sensitive parameter values in default telemetry
- advisor output must be reviewable and must not auto-apply destructive fixes
- keep hot-path overhead measurable and benchmarked

## 5. Branch-Aware Migration Rehearsal And Promotion

**Status:** `TODO`

**Future Version:** vNext+1

**Source of truth:** ADR 0153-0159 and branch CLI/API docs; needs ADR/spec
before implementation.

### Why This Matters

DecentDB already has branch, diff, restore, constrained merge, C ABI, CLI, and
Dart branch workflow foundations. The more distinctive migration win is not
merely "non-blocking ALTER TABLE." It is a safe workflow: branch, migrate,
validate, diff, detect drift, and promote.

### Desired Capability

- create migration branch from a durable snapshot
- run schema/data migration on the branch
- validate constraints, indexes, query contracts, policy effects, and sync
  compatibility
- produce schema and row diffs
- generate rollback/restore plan
- detect Decent Bench SDK/query-contract drift
- promote or merge safely when constraints are satisfied

### Guardrails

- online table rebuilds and dual-schema reads need separate ADR coverage
- branch merge semantics must stay conservative
- do not hide destructive schema changes behind automatic promotion
- shipped policy/security semantics must participate in validation

## 6. Backend Sync Bridge For Existing App Databases, Postgres First

**Status:** `TODO`

**Future Version:** vNext+1

**Source of truth:** Needs ADR/spec before implementation.

### Why This Matters

Many local-first applications already have a central Postgres or Supabase-style
backend. DecentDB-to-DecentDB sync is a strong product foundation, but adoption
gets easier if teams can bridge an existing backend into DecentDB's local
changeset and relay model without rewriting their server architecture first.

### Possible Direction

- Postgres-first source/sink bridge built around explicit table mappings
- central-server identity and tenant context mapping into DecentDB audit/sync
  context
- schema compatibility checks against DecentDB's query-contract metadata
- import/export through public changesets, not raw internal journals
- conflict and rejection reporting through existing sync conflict surfaces
- later bridges for MySQL, SQL Server, or hosted adapters only after the
  Postgres contract works

### Guardrails

- do not turn DecentDB into a broad FDW or ETL product
- do not bypass DecentDB's local transaction, policy, and sync semantics
- make unsupported schema/type differences explicit before data moves
- keep hosted-service concerns outside the engine

## 7. Incrementally Maintained Projections

**Status:** `BACKLOG`

**Future Version:** Later

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

## 8. JSONB Binary Storage And JSON Path Indexing

**Status:** `BACKLOG`

**Future Version:** Later

**Source of truth:** Needs ADR/spec before implementation.

### Why This Matters

DecentDB already supports JSON scalar and table functions. JSONB would remove
repeated parse cost for JSON-heavy workloads and make JSON expression/path
indexes more effective. SQLite now has JSONB, so binary JSON is increasingly a
baseline expectation rather than a niche feature.

### Required Design Constraints

- zero-copy traversal over pinned page bytes where practical
- no host language requirement to parse binary JSON
- C ABI projects JSONB as text JSON unless raw bytes are explicitly requested
- expression/path indexes store extracted scalars as ordinary typed index keys
- large JSONB uses existing overflow page mechanics
- partial updates rebuild the binary blob through the single writer unless an
  ADR proves a narrower mutation format is safe

## 9. Hybrid Local Search: FTS, Trigram, Vector, And Rank Fusion

**Status:** `BACKLOG`

**Future Version:** Later

**Source of truth:** FTS foundation is delivered; vector/HNSW and rank fusion
need ADR/spec before implementation.

### Why This Matters

Vector search is useful for offline AI, local RAG, and agent workflows, but
standalone HNSW is not the complete app search story. Modern local applications
want keyword search, substring search, semantic search, and relational filters
to work together.

### Desired Capability

- `VECTOR(dim)` or equivalent typed vector storage
- HNSW or another durable approximate nearest-neighbor index
- similarity operators and planner integration
- hybrid query patterns combining FTS, trigram, vector, and scalar filters
- rank fusion helpers such as reciprocal rank fusion if justified by workloads
- no external C extension requirement
- WASM/mobile portability story
- benchmarks against common vector-search extensions and hybrid-search
  workloads

### Why It Is Not Higher

FTS, security, cross-process coordination, mobile/browser hardening, and
default performance affect more existing embedded database users. Hybrid search
should follow the runtime fundamentals and avoid becoming a large storage/index
project before the core engine is easier to adopt and operate.

## 10. Authenticated Encryption, Key Rotation, And Platform Key-Store Helpers

**Status:** `BACKLOG`

**Future Version:** Later

**Source of truth:** [ADR 0174](adr/0174-local-data-security-tde-policies-masking-audit-context.md)
for the delivered v1 boundary; needs a follow-up ADR/spec before implementation.

### Why This Matters

TDE v1 removes the major local confidentiality blocker without changing the
logical database, WAL, branch, or sync-journal formats. The next tier for
regulated apps is tamper evidence, rotation, and safer key handling across
desktop, server, browser, and mobile hosts.

### Desired Capability

- authenticated page or chunk encryption with explicit recovery semantics
- online or staged key rotation without weakening crash recovery
- plaintext-to-encrypted migration tooling
- platform key-store recipes for Windows DPAPI, macOS Keychain, Linux secret
  stores, mobile keychains, browser CryptoKey/OPFS, and server/KMS use
- support-bundle and telemetry redaction rules that understand encrypted
  deployments

### Guardrails

- do not compromise random-access WAL/page writes without a measured design
- do not hide the v1 confidentiality-only boundary behind vague security terms
- keep key material outside database pages, WAL, sync journals, audit rows, and
  diagnostics

## 11. Agent And Tooling Integration Mode

**Status:** `BACKLOG`

**Future Version:** Later

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

## 12. Application And Support Bundle Format

**Status:** `BACKLOG`

**Future Version:** Later

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

## 13. Temporal Row History And Auditable State

**Status:** `BACKLOG`

**Future Version:** Later

**Source of truth:** Needs ADR/spec before implementation.

### Why This Matters

Branch/time-travel, sync journals, and shipped policy-aware SQL create a path toward
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

## 14. Advanced Geospatial Semantics And Analytics

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
- keep WASM/mobile compatibility as a design constraint

## 15. Advanced SQL Compatibility Surface

**Status:** `BACKLOG`

**Future Version:** Later

**Source of truth:** [`WIN_ADVANCED_SQL_COMPATIBILITY_SURFACE.md`](WIN_ADVANCED_SQL_COMPATIBILITY_SURFACE.md)

### Why This Matters

DecentDB already has a broad practical SQL surface for an embedded engine. The
remaining advanced compatibility work is useful for migrations, ORMs, power
users, and PostgreSQL-adjacent application code. The quick-win compatibility
layer is delivered; this item is for heavier compatibility work.

### Current Direction

- explicit sequence objects
- materialized views that are not covered by the projection track
- binding-friendly array parameter/table-valued input support for
  `carray`-style use cases
- SQL-defined functions if DecentDB chooses to support them separately from Lua
  extensions
- user-defined types
- deferred constraints and exclusion constraints
- covering-index execution for existing `INCLUDE (...)` metadata, unless that
  lands under the performance track first
- `MERGE INTO` if scoped carefully for sync/upsert workflows

### Guardrails

- do not duplicate the Lua extension runtime
- do not add arbitrary native `.load` support here
- keep this track focused on SQL syntax, catalog compatibility, and migration
  ergonomics
- avoid expanding core import/export features in this track

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
cache-invalidation risks. It should follow stronger local-first, browser,
mobile, performance, and operational foundations.

### Required Direction

- expand VFS semantics deliberately
- use HTTP range requests for page reads
- use local cache aggressively
- define write coordination and consistency rules before implementation

## Near-Term Sequence

1. Implement browser SQL/API parity and production web hardening from
   [`WIN_BROWSER_SQL_API_PARITY_PRODUCTION_WEB_SPEC.md`](WIN_BROWSER_SQL_API_PARITY_PRODUCTION_WEB_SPEC.md),
   including explicit support tiers, SQL profile tests, OPFS lifecycle coverage,
   and browser benchmark guardrails.
2. Scope mobile hardening into explicit support tiers and tests. This is
   adoption work, not just binding niceties.
3. Use release benchmark profiles to drive default-fast performance and storage
   efficiency work before adding lower-impact feature breadth.
4. Extend shipped `sys.*` metrics into opt-in tracing, advisors, and Doctor
   integration once the hot-path overhead budget is explicit.
5. Promote authenticated encryption/key-rotation work only after the v1 TDE and
   policy surfaces have production feedback and a follow-up ADR.
6. Promote backlog items into TODO only after the top adoption blockers have
   ADR/spec coverage or active implementation ownership.

## Market Notes

The roadmap order accounts for competitive pressure without becoming a feature
clone checklist:

- SQLite has mature FTS, JSONB, WASM/OPFS, PRAGMAs, CLI workflows, process-safe
  access, and decades of binding/tool familiarity.
- SQLCipher-style encrypted local files are a common requirement for mobile,
  desktop, healthcare, finance, and enterprise apps. DecentDB now has TDE v1;
  the next security gap is authenticated encryption, rotation, and key-store
  ergonomics.
- SQLite-compatible ecosystems such as libSQL/Turso create pressure around
  sync, embedded deployment, vector search, encryption-at-rest, and SQLite
  familiarity.
- DuckDB has strong ingestion, extension, FTS, vector, and analytics stories.
  Decent Bench, not DecentDB core, should own rich import/export and conversion
  workflows.
- Local-first stacks such as PGlite/Electric and PowerSync make reactive
  queries, browser/mobile sync, shape subscriptions, and central-backend bridges
  part of the expected developer conversation.
- SQLite and DuckDB have mature extension ecosystems. DecentDB's shipped
  response is one official Lua extension language with strict manifests,
  sandboxing, and explicit trust rather than arbitrary native extension loading.
- The largest DecentDB opportunity is integrated durable local-first workflow:
  fast embedded reads/writes, sync, branches, browser/mobile runtime,
  observability, security, and agent-readable tooling.

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
