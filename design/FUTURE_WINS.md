# DecentDB Future Wins

**Status:** Consolidated roadmap

**Updated:** 2026-05-28

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
- improve developer experience where the current surface is technically correct
  but still hard to diagnose, especially structured errors and safe remediation
  hints
- build on the shipped engine-owned write queue for process/browser
  coordination
- build on shipped queryable `sys.*` surfaces for tracing and advisors
- build on shipped reactive query and change-stream APIs for projections and
  sync-driven application state
- strengthen operational recovery with backup and point-in-time recovery
  (PITR), replay, and support artifacts without turning WAL streaming into the
  default answer
- harden browser and mobile beyond the shipped browser runtime and relay
  foundations
- bound embedded-host resource risk through quotas, memory limits, maintenance,
  and storage lifecycle policies
- promote practical local data security, especially transparent data encryption
- build on delivered native full-text search when planning hybrid search and
  relevance workflows
- grow safe extension distribution around the shipped Lua runtime rather than
  adding arbitrary native extension loading
- keep DecentDB-owned tooling contracts authoritative while Decent Bench owns
  rich IDE/codegen workflows
- build on delivered query plan caching and prepared-statement reuse so that
  future planner work starts from measured cache-hit behavior
- bound result materialization through streaming and cursor-based result sets so
  that embedded hosts with limited memory can query large datasets safely
- make offline-first conflict resolution ergonomic through declarative merge
  policies so that local-first adoption is not blocked by custom conflict code
- close the developer-experience gap around schema migration file management so
  that branch-aware rehearsal has a first-class CLI workflow

Intentionally excluded or deferred from the core roadmap:

- expanded import/export workflows, external file readers, and database
  conversion features. Decent Bench is the product home for robust import,
  export, and conversion workflows. DecentDB should keep the stable engine
  contracts those tools need.
- arbitrary native extension loading
- a second extension runtime such as WASM UDFs until the Lua ecosystem has
  clear unmet demand that justifies another sandbox and dependency surface
- broad foreign-data-wrapper style integration
- a general durable job queue
- text-to-SQL or LLM execution inside the core engine
- large binding rewrites unless a measured hot path requires them
- columnar/OLAP storage modes, time-series-specific storage engines, and other
  workload-specialized engines that would distract from the row-oriented
  embedded OLTP identity
- server-style users, roles, and authentication. DecentDB should keep host
  applications responsible for who can open a handle while the engine enforces
  local policies, masks, audit context, and future scoped isolation rules.

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
| Core SQL transaction and schema surfaces | [`docs/user-guide/transactions.md`](../docs/user-guide/transactions.md), [`docs/user-guide/sql-reference.md`](../docs/user-guide/sql-reference.md), [`docs/api/error-codes.md`](../docs/api/error-codes.md) | Snapshot isolation, savepoints, narrow triggers, partial/expression/covering index syntax, `PRAGMA integrity_check`/`quick_check`, basic structured error codes, and online `save_as` backup are delivered. Future work should target richer diagnostics, PITR, online execution, and broader planner/runtime use rather than re-list these baselines. |
| Rich structured errors and developer diagnostics | [`WIN_RICH_STRUCTURED_ERRORS_DEVELOPER_DIAGNOSTICS_SPEC.md`](WIN_RICH_STRUCTURED_ERRORS_DEVELOPER_DIAGNOSTICS_SPEC.md); ADR 0185; [`docs/api/error-codes.md`](../docs/api/error-codes.md); [`docs/user-guide/error-diagnostics.md`](../docs/user-guide/error-diagnostics.md) | Rich machine-readable diagnostics are delivered through the shared contract in Rust/C ABI and maintained bindings: stable subcodes, retry/permanence flags, redaction rules, docs anchors, and fixture-backed smoke coverage for structured error assertions. |
| Branch, diff, restore, and time travel | ADR 0153-0159, [`docs/api/cli-reference.md`](../docs/api/cli-reference.md#branch); 2.7.0 Dart branch workflow APIs | Future migration, support, and agent workflows should use branches as the safe rehearsal layer. |
| WASM/OPFS browser runtime | ADR 0161, ADR 0165, [`docs/api/wasm.md`](../docs/api/wasm.md), and `@decentdb/web` updates | Browser now has explicit capability probes, OPFS owner routing, Web Locks/BroadcastChannel coordination, relay helpers, diagnostics, and smoke/benchmark coverage. Follow-on work should build from the delivered browser parity baseline rather than reopen the ownership model. |
| Native geospatial foundation | ADR 0124-0128, [`docs/user-guide/data-types.md`](../docs/user-guide/data-types.md#geometry), [`docs/user-guide/indexes.md`](../docs/user-guide/indexes.md#spatial-indexes) | Future spatial work is advanced analytics and planner breadth, not proving DecentDB can store spatial values. |
| Built-in HTTP server and web console | [`docs/user-guide/web-console.md`](../docs/user-guide/web-console.md), [`docs/api/cli-reference.md`](../docs/api/cli-reference.md#serve) | Future agent/tooling surfaces can reuse the local HTTP shape, but Decent Bench remains the full IDE. |
| Stable tooling metadata and query contracts | [`STABLE_TOOLING_METADATA_CONTRACT.md`](STABLE_TOOLING_METADATA_CONTRACT.md) | DecentDB owns metadata/query-contract truth. Decent Bench owns generated SDK workflows. |
| Lua extension runtime and package model | [`WIN_LUA_EXTENSION_RUNTIME_SPEC.md`](_archive/WIN_LUA_EXTENSION_RUNTIME_SPEC.md); ADR 0169-0173; [`docs/user-guide/lua-extensions.md`](../docs/user-guide/lua-extensions.md) | Sandboxed Lua packages now provide DecentDB's safe extensibility story: manifest validation, install/enable/trust lifecycle, scalar functions, table-valued functions, aggregates, query-time collations, Rust/CLI/C ABI surfaces, and examples. |
| Local data security v1 | [ADR 0174](adr/0174-local-data-security-tde-policies-masking-audit-context.md); [`docs/user-guide/security.md`](../docs/user-guide/security.md); [`docs/api/configuration.md`](../docs/api/configuration.md#local-transparent-data-encryption-tde) | Transparent local encryption, durable row policies, projection masks, audit context, C ABI open options, and queryable audit context are delivered foundations. Future security work should extend this boundary rather than redefining it. |
| Native full-text search with BM25 ranking | [`WIN_FULL_TEXT_SEARCH_BM25_SPEC.md`](WIN_FULL_TEXT_SEARCH_BM25_SPEC.md); ADR 0175-0176; [`docs/user-guide/indexes.md`](../docs/user-guide/indexes.md#full-text-indexes) | `USING fulltext` indexes, analyzer metadata, `fulltext_match`, `bm25`, phrase/prefix queries, write-path maintenance, rebuild/verify, tooling metadata, documentation, and regression tests are delivered. Follow-on search work now belongs under hybrid search, fuzziness/suggestions, or performance-specific roadmap items. |
| Large-value overflow compression | [ADR 0048](adr/0048-optional-value-compression.md), [`docs/user-guide/data-types.md`](../docs/user-guide/data-types.md#compression) | Transparent zlib compression for large `TEXT` and `BLOB` overflow payloads is delivered. Future compression work should be framed as measured page/key/layout efficiency, not as first-time compression support. |
| Cross-process WAL coordination | [`WIN_CROSS_PROCESS_WAL_COORDINATION_SPEC.md`](WIN_CROSS_PROCESS_WAL_COORDINATION_SPEC.md); ADR 0177-0180; [`docs/user-guide/write-concurrency.md`](../docs/user-guide/write-concurrency.md#cross-process-wal-coordination) | Native OS processes can safely share local on-disk databases through byte-range locks, `.coord` sidecars, cross-process reader retention, WAL refresh, binding open options, `sys.process_*` diagnostics, and Doctor findings. Future runtime work can build on this foundation for browser/mobile hardening and richer operational tracing. |
| Browser SQL/API parity and production web hardening | [`WIN_BROWSER_SQL_API_PARITY_PRODUCTION_WEB_SPEC.md`](WIN_BROWSER_SQL_API_PARITY_PRODUCTION_WEB_SPEC.md); [`docs/api/wasm.md`](../docs/api/wasm.md); [`bindings/web/README.md`](../bindings/web/README.md) | Browser now has the `browser-app-v2` SQL profile, checked-in parity corpus, stable browser SQL errors, protocol/capability metadata, transaction/savepoint APIs, prepared statement paging, OPFS lifecycle guards/diagnostics, relay apply-before-ack helpers, framework recipes, and benchmark guardrails. Future browser work should target measured parser breadth, TDE/key handling, branch workflows, or browser-specific performance rather than this parity baseline. |
| Mobile production runtime and SDK hardening | [`WIN_MOBILE_PRODUCTION_RUNTIME_SDK_HARDENING_SPEC.md`](WIN_MOBILE_PRODUCTION_RUNTIME_SDK_HARDENING_SPEC.md); ADR 0181-0183; [`docs/api/dart.md`](../docs/api/dart.md); [`bindings/dart/flutter/README.md`](../bindings/dart/flutter/README.md) | Flutter mobile now has a package shell, Android/iOS artifact scripts and GitHub Actions workflow, mobile native loading defaults, redacted open-option diagnostics, key-provider helpers, async worker lifecycle docs/tests, public changeset apply-before-ack wrappers, and mobile storage/sidecar guidance. Follow-on mobile work should target measured device matrices, native Swift/Kotlin SDKs, watch lifecycle guarantees, or key rotation rather than first-class package hardening. |
| Default-fast performance and storage efficiency | [`WIN_DEFAULT_FAST_PERFORMANCE_STORAGE_EFFICIENCY_SPEC.md`](WIN_DEFAULT_FAST_PERFORMANCE_STORAGE_EFFICIENCY_SPEC.md); ADR 0184; [`docs/user-guide/performance.md`](../docs/user-guide/performance.md); [`docs/api/configuration.md`](../docs/api/configuration.md) | **Priority #1 complete:** durable defaults keep the historical 4 MiB cache after executor fast-path fixes recovered read headroom, while explicit low-memory/balanced/tuned durable profile helpers provide 4 MiB, 16 MiB, and 64 MiB options. The delivered slice includes canonical benchmark profile metadata, Python binding hot-path benchmark slices, storage/cold-state metadata, prepared-insert fast-path fixes, transaction-scoped prepared batches for repeated Rust inserts, runtime-only covering-index execution for safe `INCLUDE (...)` projections, parser-bypass metadata/row-id reads for plain `COUNT(*)` and integer primary-key projections, deferred table materialization behavior that preserves valid indexes, scalar aggregate fast paths that scan persisted payload columns without forcing full row materialization, lazy default-open work for checkpoint/reactive/coordination/empty-schema maintenance, and a green rust-baseline smoke/medium/full/huge comparison against the checked-in historical default-profile results. Follow-on performance work should be measured and scoped to new evidence, not this completed baseline. |
| Query plan caching and prepared-statement reuse | [`WIN_QUERY_PLAN_CACHING_AND_STATEMENT_REUSE.md`](WIN_QUERY_PLAN_CACHING_AND_STATEMENT_REUSE.md); ADR 0190-0194; [`docs/user-guide/performance.md`](../docs/user-guide/performance.md#plan-cache) | Connection-local parsed AST and prepared-plan bundle caches are delivered with default-on bounded memory, invalidation, diagnostics, CLI/C ABI access, Doctor guidance, and native/rust-baseline guardrail benchmarks. Future plan-cache work should be scoped as process-global sharing, object-level invalidation, or binding-specific throughput work. |
| Benchmark profiles and release assets | [`BENCHMARKING_GUIDE.md`](BENCHMARKING_GUIDE.md), `crates/decentdb-benchmark`, `data/bench_summary.json` | Performance work should target measured default and tuned profiles, storage efficiency, cold-open behavior, and release-chart regressions. |

## Status Map

Status values:

- `TODO`: prioritized roadmap work that is not actively being implemented right
  now.
- `IN PROGRESS`: active implementation or design work is underway right now.
- `BACKLOG`: valuable, but not part of the near-term implementation path.

Future version values are planning buckets, not release commitments. The latest
public release in this repository is `2.13.0`. `vNext` means the first release
bucket after `2.13.0` only when scope is explicitly accepted. `vNext+1` and
`vNext+2` are follow-on planning buckets, not exact semantic versions.

Roadmap lifecycle: once a Future Win is 100% implemented, tested, and
documented, remove it from this roadmap. Completed and delivered work is no
longer a Future Win. Keep only a concise `Delivered Context` entry when the
shipped foundation affects follow-on roadmap decisions.

| Priority | Future Version | Status | Feature | Current Source Of Truth | Why This Rank |
|---:|---|---|---|---|---|
| 1 | vNext+1 | TODO | Incremental backup and point-in-time recovery | Basic `save_as` backup is delivered; needs ADR/spec for WAL archive/PITR semantics | Durable recovery artifacts are a production requirement and distinct from live replication |
| 2 | vNext+1 | TODO | Runtime tracing, advisors, and Doctor integration | [`WIN_RUNTIME_TRACING_ADVISORS_AND_DOCTOR_INTEGRATION.md`](WIN_RUNTIME_TRACING_ADVISORS_AND_DOCTOR_INTEGRATION.md); ADR 0186-0189 | Explains slow queries, lock waits, index usage, schema lint, and maintenance issues once the metrics contract is stable |
| 3 | vNext+1 | TODO | Branch-aware migration rehearsal and promotion | ADR 0153-0159 and branch CLI/API docs; needs ADR/spec | Uses shipped branch/diff foundations for a distinctive safe migration workflow |
| 4 | vNext+1 | TODO | Online schema change execution | Needs ADR/spec; follows branch migration design | Completes the migration story by reducing reader/write disruption during large table rebuilds and index work |
| 5 | vNext+1 | TODO | Backend sync bridge for existing app databases, Postgres first | Needs ADR/spec | Makes DecentDB easier to adopt in apps that already have a central Postgres/Supabase-style backend |
| 6 | vNext+1 | TODO | Resource governance, quotas, and automated maintenance | Needs ADR/spec; follows browser/mobile/runtime diagnostics | Embedded hosts need explicit storage, WAL, memory, quota, and maintenance behavior to avoid runaway local resource use |
| 7 | vNext+1 | TODO | Streaming and cursor-based result sets | Needs ADR/spec | Memory-bounded hosts need bounded result materialization; enables resource governance and affects large-query adoption |
| 8 | vNext+2 | TODO | Offline-first conflict resolution UX and declarative merge policies | Needs ADR/spec; builds on shipped sync and changeset surfaces | Local-first differentiator; competitors handle conflict UX poorly; declarative merge policies make sync adoption easier |
| 9 | vNext+2 | TODO | Schema migration file management and CLI | Needs ADR/spec; complements branch-aware migration rehearsal | Developer-experience gap for version-tracked migration scripts and framework integration |
| 10 | vNext+2 | TODO | Observability bridge: OpenTelemetry and structured export | Needs ADR/spec; follows tracing/advisors | Production teams need external observability to justify DecentDB over SQLite; builds on item 2 |
| 11 | vNext+2 | TODO | Binding ergonomics and performance contract | Needs ADR/spec | No current roadmap item addresses connection pooling, batch APIs, and cross-target performance guarantees for maintained bindings |
| 12 | Later | BACKLOG | Incrementally maintained projections | Needs ADR/spec | Accelerates dashboards, local read models, and reactive query workloads |
| 13 | Later | BACKLOG | JSONB binary storage and JSON path indexing | Needs ADR/spec | Important for JSON-heavy workloads and now a SQLite baseline expectation |
| 14 | Later | BACKLOG | Hybrid local search: FTS, trigram, vector, and rank fusion | FTS foundation is delivered; vector and rank fusion need ADR/spec | More compelling than standalone HNSW: apps want keyword, substring, semantic, and relational filters together |
| 15 | Later | BACKLOG | Authenticated encryption, key rotation, and platform key-store helpers | ADR 0174 follow-up | TDE v1 provides local confidentiality; regulated deployments eventually need tamper-evident page/chunk authentication, key rotation, and turnkey OS/browser/mobile key-store guidance |
| 16 | Later | BACKLOG | Agent and tooling integration mode | [`STABLE_TOOLING_METADATA_CONTRACT.md`](STABLE_TOOLING_METADATA_CONTRACT.md); needs ADR/spec | Makes the agent-friendly promise concrete without putting LLM behavior in the engine |
| 17 | Later | BACKLOG | Reliability validation, fault injection, and deterministic replay | [`TESTING_STRATEGY.md`](TESTING_STRATEGY.md); needs ADR/spec for replay capture | Raises confidence in ACID/concurrency changes and makes production bugs reproducible without weakening hot paths |
| 18 | Later | BACKLOG | Application and support bundle format | Needs ADR/spec | Useful portable artifact and diagnostics story, but should follow security/redaction foundations |
| 19 | Later | BACKLOG | Temporal row history and auditable state | Needs ADR/spec | Strong regulated/support workflow, but should follow security, audit context, and sync hardening |
| 20 | Later | BACKLOG | Structured CDC and logical change feeds | Change streams, public changesets, and sync journal are delivered; needs ADR/spec | Lets DecentDB feed event-driven systems without becoming a message broker or bypassing local transactions |
| 21 | Later | BACKLOG | Curated Lua extension ecosystem | Lua runtime/package model is delivered; needs ADR/spec outside core engine if registry semantics affect trust | Turns safe extensibility into an adoption moat while preserving the no-native-extension stance |
| 22 | Later | BACKLOG | Multi-tenant scoped isolation | Needs ADR/spec | Narrow scoped-visibility mechanism distinct from shipped masking and excluded server-style auth; enables SaaS-embedded patterns |
| 23 | Later | BACKLOG | Unicode collation and internationalization profile | Query-time built-in and Lua collations are delivered; needs ADR/spec for ICU/data-size strategy | International apps need correct Unicode sort/search semantics, but portability and binary size make it a later tradeoff |
| 24 | Later | BACKLOG | Advanced SQL compatibility surface | [`WIN_ADVANCED_SQL_COMPATIBILITY_SURFACE.md`](WIN_ADVANCED_SQL_COMPATIBILITY_SURFACE.md) | Useful adoption polish after higher-impact runtime, recovery, migration, and workflow blockers |
| 25 | Later | BACKLOG | Advanced geospatial semantics and analytics | ADR 0128 deferred work; needs follow-up ADR/spec | Builds on shipped spatial support without implying the foundation is unfinished |
| 26 | Later | BACKLOG | Deterministic testing and binding snapshot assertions | Needs ADR/spec; follows reliability validation | Binding-level test infrastructure and deterministic assertion patterns improve Priority #3 confidence |
| 27 | Later | BACKLOG | WAL streaming replication | Needs ADR/spec | Useful HA/read-scale story, but weaker than local-first sync and PITR for DecentDB identity |
| 28 | Later | BACKLOG | Cloud-native object storage VFS and WASI edge profiles | Needs ADR/spec | Interesting edge/serverless story with high durability, consistency, packaging, and cache-invalidation complexity |

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

## 1. Incremental Backup And Point-In-Time Recovery

**Status:** `TODO`

**Future Version:** vNext+1

**Source of truth:** Basic online `save_as` backup is delivered through Rust,
C ABI, and bindings; needs ADR/spec before adding WAL archive and PITR
semantics.

### Why This Matters

Applications can already create an online backup copy, but production recovery
often needs smaller incremental artifacts and the ability to recover from an
operator or application mistake at a specific time. This is separate from live
standby replication: backup/PITR is about durable recovery artifacts with clear
retention, encryption, and restore semantics.

### Desired Capability

- consistent base snapshot creation without stopping normal readers/writes
- incremental backup deltas from retained WAL, branch, or checkpoint state
- point-in-time restore to a named timestamp, LSN, snapshot, or backup marker
- TDE-aware backup metadata and restore validation
- backup verification and manifest integrity checks
- retention policy guidance that composes with branches, snapshots, sync
  journals, and cross-process reader retention
- CLI, Rust, C ABI, and maintained binding entry points

### Guardrails

- do not bypass the normal WAL/checkpoint recovery model
- do not create backup artifacts that silently drop encryption, policy, sync, or
  branch metadata
- make retention and storage growth explicit
- keep live replication and external object storage as separate designs

## 2. Runtime Tracing, Advisors, And Doctor Integration

**Status:** `TODO`

**Future Version:** vNext+1

**Source of truth:** [`WIN_RUNTIME_TRACING_ADVISORS_AND_DOCTOR_INTEGRATION.md`](WIN_RUNTIME_TRACING_ADVISORS_AND_DOCTOR_INTEGRATION.md);
ADR 0186-0189.

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
SELECT * FROM sys.sessions;
SELECT * FROM sys.doctor_findings;
```

### Advisor Extensions

- `PRAGMA doctor`
- Decent Bench doctor panel
- explicit `doctor --fix-plan`
- query-plan advisor
- missing/unused index advisor
- schema lint advisor for foreign-key indexes, redundant indexes, constraint
  risk, sync-incompatible schema choices, and migration hazards
- plan-diff and plan-regression reports before considering explicit plan
  pinning or hints
- JSON path advisor after JSONB exists
- sync, branch, browser, and mobile diagnostics as those surfaces mature

### Required Design Topics

- explicit opt-in configuration for expensive tracing
- in-memory ring buffer sizes, eviction policy, and reset semantics
- SQL text and parameter redaction policy
- lock-wait source classification
- index-usage attribution from planner and executor paths
- session/connection lifecycle visibility without turning DecentDB into a
  server-style pool manager
- Doctor report projection into queryable rows
- advisor severity, confidence, and automation boundaries

### Guardrails

- no recursive disk writes for telemetry
- no tracing while internal locks are held longer than necessary
- no sensitive parameter values in default telemetry
- advisor output must be reviewable and must not auto-apply destructive fixes
- keep hot-path overhead measurable and benchmarked

## 3. Branch-Aware Migration Rehearsal And Promotion

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
- validate declared application schema versions and compatibility contracts
- produce schema and row diffs
- record migration history and schema-change metadata that tools can query
- generate rollback/restore plan
- detect Decent Bench SDK/query-contract drift
- promote or merge safely when constraints are satisfied

### Guardrails

- online table rebuilds and dual-schema reads belong to the online schema
  execution track and need separate ADR coverage
- branch merge semantics must stay conservative
- do not hide destructive schema changes behind automatic promotion
- shipped policy/security semantics must participate in validation

## 4. Online Schema Change Execution

**Status:** `TODO`

**Future Version:** vNext+1

**Source of truth:** Needs ADR/spec before implementation. Should be designed
with branch-aware migration rehearsal, not as an isolated `ALTER TABLE`
expansion.

### Why This Matters

Branch rehearsal proves a migration is safe before promotion. It does not by
itself make the production execution non-disruptive. Large table rebuilds,
constraint validation, index creation, type changes, and column drops can still
interrupt normal application work if they require an exclusive database lock for
the entire operation.

### Desired Capability

- narrow online-safe schema-change set with explicit support tiers
- online index build/rebuild where readers keep stable snapshots
- resumable or staged table rewrites where crash recovery is well-defined
- promotion path from a validated migration branch into the active branch
- drift detection when writes continue while a staged migration is prepared
- progress, cancellation, and write-latency impact surfaced through `sys.*`

### Guardrails

- preserve the one-writer/many-readers model
- do not hide destructive changes behind automatic promotion
- dual-schema reads and writes need ADR coverage before implementation
- every format or catalog change must follow ADR 0131 migration requirements
- measure write-path impact and expose it in diagnostics

## 5. Backend Sync Bridge For Existing App Databases, Postgres First

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
- future per-table or custom merge policies only after the bridge proves the
  conservative public changeset contract
- later bridges for MySQL, SQL Server, or hosted adapters only after the
  Postgres contract works

### Guardrails

- do not turn DecentDB into a broad FDW or ETL product
- do not bypass DecentDB's local transaction, policy, and sync semantics
- make unsupported schema/type differences explicit before data moves
- keep hosted-service concerns outside the engine

## 6. Resource Governance, Quotas, And Automated Maintenance

**Status:** `TODO`

**Future Version:** vNext+1

**Source of truth:** Needs ADR/spec before implementation. Should follow
browser/mobile hardening and runtime diagnostics so limits can be enforced and
explained consistently.

### Why This Matters

Embedded databases run inside someone else's process, device, browser quota, or
serverless sandbox. Unbounded result materialization, WAL growth, retained
branches/snapshots, sync journals, extension execution, and maintenance work can
harm the host even when DecentDB remains logically correct.

### Desired Capability

- database-level page/byte quotas with explicit over-limit errors
- optional per-table or per-tenant soft/hard quotas where policy metadata can
  support cheap accounting
- memory budgets for result sets, temporary structures, planner work, extension
  calls, and large binding transports
- WAL, checkpoint, branch, snapshot, and sync-journal retention policies
- vacuum/compaction, `ANALYZE`, integrity checks, and full-text rebuild work
  scheduled or throttled during safe idle windows
- progress and warnings through `sys.*`, Doctor, CLI, and maintained bindings
- browser OPFS/mobile quota guidance that matches platform behavior

### Guardrails

- reject writes cleanly rather than silently dropping data when hard limits are
  reached
- persist quota and retention settings durably
- make background work yield to foreground application operations
- keep accounting cheap; avoid full-page or full-table walks on every write
- do not auto-delete named snapshots, branches, sync data, or audit records
  without explicit retained-policy configuration

## 8. Streaming And Cursor-Based Result Sets

**Status:** `TODO`

**Future Version:** vNext+1

**Source of truth:** Needs ADR/spec before implementation. A streaming result
contract that changes C ABI lifetime or ownership semantics requires a separate
ADR and C ABI version bump.

### Why This Matters

Embedded hosts have bounded memory. Large SELECT results, analytical queries,
sync changeset exports, and support bundle queries can materialize full result
sets that exceed practical memory on mobile devices, browsers, and constrained
servers. DecentDB currently materializes full results before returning them
through most binding surfaces. This creates a hard ceiling on query result size
and forces application authors to paginate manually or risk out-of-memory
conditions.

Resource governance (item 6) needs bounded result materialization as a
governance tool. Reactive subscriptions (shipped) already stream incremental
changes, but bulk reads still require full materialization. A streaming or
cursor-based result path makes resource quotas enforceable and makes large-query
workloads practical on memory-constrained hosts.

### Desired Capability

- cursor-based result iteration through the C ABI that fetches rows in bounded
  batches without materializing the full result set
- connection-scoped result cursors with explicit open, fetch, and close lifecycle
- configurable fetch batch size with a default that respects low-memory profiles
- early query termination through cursor close without completing the full scan
- resource governance integration: queries that would exceed the configured result
  memory budget fail with a clear error rather than silently materializing
- binding-friendly iteration patterns that compose with existing prepared-statement
  APIs
- WASM and mobile cursor patterns that respect single-threaded and async
  constraints

### Guardrails

- do not change existing C ABI result ownership semantics without a version bump
  and ADR
- do not hold write locks or WAL retention barriers indefinitely for open cursors;
  use snapshot isolation correctly and document the retention behavior
- keep the default full-materialization path stable; streaming must be opt-in for
  the first release
- measure cursor overhead versus full materialization for small result sets; do
  not regress the common case
- any new C ABI lifetime contract requires binding updates and migration guidance

## 9. Offline-First Conflict Resolution UX And Declarative Merge Policies

**Status:** `TODO`

**Future Version:** vNext+2

**Source of truth:** Needs ADR/spec. Builds on shipped sync slices, changesets,
and conflict surfaces.

### Why This Matters

DecentDB has delivered local-first sync slices 1-8, public changesets, and a
conflict resolution surface. But conflict handling currently requires application
code for every conflict type. Developers building offline-first apps need
declarative merge policies that cover common patterns (last-write-wins,
application-specific merge, field-level merge, and per-column conflict
strategies) without writing custom resolution logic for every table.

Competing local-first databases such as PowerSync and Electric SQL provide
opinionated merge strategies out of the box. DecentDB's sync foundation is
stronger, but the developer experience of handling conflicts falls short of the
"just works" promise that makes local-first adoption easy. This win does not add
CRDTs or server-side merge engines. It makes the already-shipped conflict
surface easier to consume through declarative policies and ergonomic tooling.

### Desired Capability

- declarative per-table and per-column merge policies (last-write-wins,
  application-merge, fail-and-notify, and custom Lua resolvers)
- conflict preview and inspection through `sys.*` views and CLI
- merge policy definitions stored in catalog metadata with sync shape awareness
- field-level merge for common patterns (increment counters, concat text, choose
  non-null)
- conflict dashboards and debugging surfaces in Doctor and CLI
- binding-friendly conflict enumeration and resolution APIs
- documentation patterns and examples for common offline-first conflict scenarios

### Guardrails

- do not build a general CRDT column type or automatic merge system
- do not bypass local transaction semantics or changeset durability
- keep custom resolution safe: Lua resolvers follow the same sandbox and trust
  model as other Lua extensions
- fail-and-notify is a valid policy; not every conflict should auto-resolve
- merge policies must compose with TDE, masking, and audit context without
  bypassing them

## 10. Schema Migration File Management And CLI

**Status:** `TODO`

**Future Version:** vNext+2

**Source of truth:** Needs ADR/spec. Complements branch-aware migration rehearsal
(item 3).

### Why This Matters

Branch-aware migration rehearsal (item 3) provides the database-side workflow for
safe schema changes: branch, migrate, validate, diff, and promote. But application
developers also need a version-tracked migration file management workflow: SQL
migration files, up/down scripts, version ordering, manifest tracking, and
integration with migration frameworks (golang-migrate, Flyway, Alembic, Entity
Framework migrations).

SQLite's simplicity is partly due to its lightweight CLI migration story. DecentDB
has strong branch and diff foundations, but the developer CLI workflow for managing
migration files, tracking applied versions, and running migration sequences is
incomplete. This win makes the branch rehearsal workflow accessible to developers
who think in terms of numbered migration files, not database branches.

### Desired Capability

- `decentdb migrate` CLI subcommand for create, apply, revert, status, and verify
  operations
- migration file format with ordered up/down SQL scripts and metadata headers
- migration manifest tracking applied versions in durable catalog metadata
- dry-run and plan-only modes that show the SQL and effects without executing
- integration with branch rehearsal: create migration branch, apply migration
  files, validate, diff, and promote
- C ABI and binding surfaces for migration status and version queries
- `sys.migrations` view for applied migration history
- conflict detection when migration history diverges from expected state

### Guardrails

- do not build a general migration framework server or UI
- do not duplicate Decent Bench's SDK generation and visual migration workflows
- migration files are SQL text; do not invent a new DSL
- migration tracking metadata must survive crash recovery and branch promotion
- keep the CLI surface composable so build systems and CI pipelines can drive it

## 11. Observability Bridge: OpenTelemetry And Structured Export

**Status:** `TODO`

**Future Version:** vNext+2

**Source of truth:** Needs ADR/spec. Follows runtime tracing and advisors
(item 2).

### Why This Matters

Runtime tracing and Doctor (item 2) provide internal diagnostic surfaces
through `sys.*` views and CLI output. But production teams running DecentDB
inside larger applications already use OpenTelemetry, Prometheus, structured
logging, and distributed tracing to observe their stack. If DecentDB cannot
export structured traces and metrics to these systems, operators lack visibility
into embedded database behavior and fall back to guessing.

SQLite has limited observability. Making DecentDB observable in standard
observability stacks is a practical differentiator for teams evaluating embedded
databases for production workloads. This win does not add a tracing collector to
the engine core. It adds opt-in export bridges that translate internal metrics
and trace events into standard formats.

### Desired Capability

- opt-in OpenTelemetry trace span export for query execution, transaction
  lifecycle, checkpoint, WAL, sync, and branch operations
- opt-in OpenTelemetry metrics export for cache hit rates, WAL growth, commit
  latency, checkpoint duration, and connection counts
- structured logging bridge that emits JSON-formatted events compatible with
  common log aggregators
- C ABI configuration for enabling, filtering, and endpoint configuration
- maintained binding surfaces for trace context propagation and metric export
- no hot-path overhead when observability is disabled
- documentation for common integration patterns (Jaeger, Zipkin, Grafana,
  Datadog, CloudWatch)

### Guardrails

- do not add always-on tracing or telemetry collection
- do not add a network listener, HTTP server, or gRPC dependency to the core
  engine; export bridges are host-side adapters or binding-side integrations
- do not emit sensitive data (parameter values, TDE keys, SQL text) by default
- keep tracing/metric collection opt-in at open time, not per-query
- follow the redaction policy from the structured errors spec (ADR 0185) for any
  exported data

## 12. Binding Ergonomics And Performance Contract

**Status:** `TODO`

**Future Version:** vNext+2

**Source of truth:** Needs ADR/spec. ADRs 0039-0046 cover .NET-specific
ergonomics; this item covers cross-binding consistency.

### Why This Matters

DecentDB has six maintained bindings (Python, Go, Node, .NET, Java, Dart) plus
WASM/browser. Each binding wraps the C ABI with language-appropriate patterns.
But there is no cross-binding performance contract or ergonomics specification.
Binding authors currently make independent choices about connection pooling,
statement reuse, batch patterns, error projection, type mapping, and async
models. This creates inconsistent developer experience and performance gaps that
are not visible in native-only benchmarks.

SQLite's ecosystem works partly because every binding behaves predictably.
DecentDB bindings should offer a consistent contract: connection pooling
patterns, prepared-statement lifecycle, batch insert APIs, async model
conventions, and minimum performance expectations relative to the native path.
This win formalizes that contract and makes binding performance a first-class
concern.

### Desired Capability

- cross-binding ergonomics contract specifying minimum API surface: open,
  prepare, execute, fetch, close, transaction, error, and metadata patterns
- connection pooling patterns and recommendations for each binding language
- batch insert API contract that uses the shipped write queue for group commit
- prepared-statement lifecycle patterns with explicit reset, reuse, and cleanup
  guidance
- binding performance benchmarks measured against the native C ABI path with
  documented target ratios
- async model conventions for each binding (Node Promises, .NET Task, Go
  goroutines, Java CompletableFuture, Dart async, Python async)
- error projection contract consistent with ADR 0185 across all bindings
- `sys.*` and Doctor integration patterns documented for each binding

### Guardrails

- do not mandate identical internal implementation across bindings; idiomatic
  patterns differ by language
- do not rewrite bindings; formalize existing contracts and fill measured gaps
- do not add binding-specific query features that bypass the C ABI
- keep the C ABI as the single authoritative contract surface
- binding performance targets are measured ratios against native, not absolute
  numbers

## 13. Incrementally Maintained Projections

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

## 14. JSONB Binary Storage And JSON Path Indexing

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

## 15. Hybrid Local Search: FTS, Trigram, Vector, And Rank Fusion

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

## 16. Authenticated Encryption, Key Rotation, And Platform Key-Store Helpers

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

## 17. Agent And Tooling Integration Mode

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
- cross-branch read-only query/diff surfaces for migration validation,
  reconciliation, and reviewed merge proposals
- query validation without execution
- structured repair/migration proposal outputs that can be reviewed before
  execution
- stable capability manifest for bindings and tools
- session and audit context tags that let tools identify their own operations
  in diagnostics

### Guardrails

- DecentDB should not run an LLM or natural-language agent inside the engine
- no agent write should bypass normal SQL, transaction, branch, and policy
  semantics
- Decent Bench remains the product home for rich visual workflows and generated
  SDK output

## 18. Reliability Validation, Fault Injection, And Deterministic Replay

**Status:** `BACKLOG`

**Future Version:** Later

**Source of truth:** [`TESTING_STRATEGY.md`](TESTING_STRATEGY.md) exists for
current test strategy. Needs ADR/spec before adding production replay capture or
a deterministic simulator.

### Why This Matters

DecentDB's first promise is durable ACID writes. Future online schema changes,
PITR, cross-process coordination, sync bridges, and resource governance will
stress WAL, snapshot, locking, and recovery behavior. A stronger reliability
track makes those changes safer and gives support teams a way to reproduce
production failures without sharing raw database files.

### Desired Capability

- deterministic fault-injection harness around VFS, WAL, checkpoint, write
  queue, reader slots, sync apply, and branch promotion paths
- continuous fuzzing for SQL parsing, planning, execution, storage recovery,
  sync changesets, and public JSON inputs
- disk-full, torn-write, corruption, crash, lock-contention, and stale-sidecar
  scenarios as named regression suites
- opt-in logical debug replay capturing SQL text, parameter shapes,
  transaction boundaries, open options, branch/snapshot identity, and relevant
  deterministic seeds
- redaction policy shared with tracing and support bundles
- replay divergence reports that are useful to agents and maintainers

### Guardrails

- production replay capture is strictly opt-in
- no hot-path overhead when replay/fault instrumentation is disabled
- capture formats must redact before persistence
- replay is a diagnostic tool, not a replacement for WAL recovery
- byte-identical database images are not a blanket requirement unless a narrow
  ADR proves the value and cost

## 19. Application And Support Bundle Format

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
- optional immutable/read-only distribution mode for static datasets and
  support-safe inspection

### Guardrails

- this is not a general import/export or ETL feature
- do not duplicate Decent Bench's rich import/export tooling
- compatibility, integrity, signature, and recovery rules need an ADR
- bundle creation must checkpoint or otherwise define WAL handling explicitly
- support bundles must have a sanitization/redaction story before use with
  regulated data

## 20. Temporal Row History And Auditable State

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

## 21. Structured CDC And Logical Change Feeds

**Status:** `BACKLOG`

**Future Version:** Later

**Source of truth:** Change streams, public changesets, and the sync journal are
delivered foundations. Needs ADR/spec before external CDC is a stable product
surface.

### Why This Matters

Reactive subscriptions and sync changesets serve local application state and
DecentDB-aware sync flows. Some deployments also need to feed committed changes
into external audit pipelines, webhook workers, message queues, cloud
functions, or data integration systems without treating DecentDB as a message
broker.

### Desired Capability

- stable row-level change event envelope built on public changeset semantics
- optional before/after images where policy and storage cost allow
- filtered feeds by table, branch, replica, tenant, or policy scope
- durable consumer progress markers or logical slots if the retention model can
  support them safely
- backpressure, buffering, and rate-limit behavior documented explicitly
- CLI/Rust/C ABI/binding surfaces that expose CDC as a read/consume path, not a
  second write path

### Guardrails

- local transaction durability remains the only hard durability boundary
- slow external consumers must not block the writer indefinitely
- delivery to external systems is best-effort unless a separate integration
  owns its own durability
- do not build webhook execution or a message broker into the engine
- reuse changeset compatibility metadata rather than inventing a parallel
  serialization contract

## 22. Curated Lua Extension Ecosystem

**Status:** `BACKLOG`

**Future Version:** Later

**Source of truth:** Lua runtime/package/trust model is delivered through ADR
0169-0173 and [`docs/user-guide/lua-extensions.md`](../docs/user-guide/lua-extensions.md).
Needs ADR/spec if registry trust, package resolution, or signing becomes an
official DecentDB contract.

### Why This Matters

The shipped Lua runtime gives DecentDB a safe extension story without arbitrary
native `.load` support. The next value is discoverability and trust: common
extensions for text processing, validation, math, URL/string helpers,
domain-specific functions, and collation examples should be easy to find,
verify, and test across native, browser, and mobile targets.

### Desired Capability

- curated package index or repository for reviewed Lua extension packages
- package signing, checksums, compatibility metadata, and trust provenance
- deterministic dependency/version resolution if packages can depend on each
  other
- smoke tests and compatibility badges across maintained runtime targets
- extension metrics and diagnostics surfaced through existing `sys.*`
  extension views where practical
- examples for common safe extension categories without expanding core SQL

### Guardrails

- registry work must not weaken the manifest/trust lifecycle
- no arbitrary native extension loading
- avoid shipping network-capable or host-privileged extension behavior inside
  the engine
- keep browser/mobile package-size and sandbox constraints explicit

## 23. Multi-Tenant Scoped Isolation

**Status:** `BACKLOG`

**Future Version:** Later

**Source of truth:** Needs ADR/spec. Follows resource governance, security, and
audit context foundations.

### Why This Matters

DecentDB already ships row-level policies, projection masks, and audit context
(ADR 0174). These provide local data security for single-tenant deployments.
Applications embedding DecentDB in SaaS, multi-app, or multi-workspace
environments need a narrower, more explicit isolation mechanism that goes beyond
masking but falls short of server-style users, roles, and authentication (which
are explicitly excluded from the roadmap).

Multi-tenant scoped isolation is distinct from row-level masking (which hides
data) and from server auth (which assumes a connection manager). It is about
enforcing scoped visibility: a tenant or workspace can only see and modify its
own partition of the database, enforced by the engine rather than by application
convention alone.

### Possible Direction

- tenant or workspace context set at open time or per-transaction
- scoped visibility filters automatically applied to all queries in the scope
- scoped storage quotas composing with resource governance (item 6)
- scoped `sys.*` diagnostics filtered by tenant context
- scoped sync and changeset boundaries
- scoped branch isolation where tenants cannot access other tenants' branches

### Guardrails

- this is not server-style users, roles, or authentication
- the host application remains responsible for who can open a handle
- scoped isolation must not bypass TDE, masking, or audit context
- scoped filters must be verifiable by Doctor and `sys.*` surfaces
- performance overhead of scoped filtering must be measured and documented
- scoped isolation composes with, but does not replace, application-level access
  control

## 24. Unicode Collation And Internationalization Profile

**Status:** `BACKLOG`

**Future Version:** Later

**Source of truth:** Query-time built-in collations and Lua extension
collations are delivered; needs ADR/spec before adding ICU or another Unicode
collation data strategy.

### Why This Matters

International applications need correct Unicode case folding, accent handling,
normalization, and locale-aware ordering. ASCII-oriented compatibility
collations are useful for onboarding but are not enough for many healthcare,
government, education, commerce, and consumer applications.

### Desired Capability

- explicit Unicode collation support tiers
- locale-aware sort and case-insensitive comparison semantics
- normalization policy for comparison and index use
- index/query planner rules for persistent or query-time Unicode collations
- browser/mobile/WASI package-size strategy for collation data
- compatibility tests against ICU/Unicode Collation Algorithm fixtures or
  another chosen reference

### Guardrails

- do not silently change existing binary/default collation semantics
- do not make every build carry large locale data by default without an ADR
- avoid locale-sensitive behavior in durability-critical metadata keys
- keep Lua collations available for narrower application-specific behavior

## 25. Advanced SQL Compatibility Surface

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
- broader expression-index and covering-index execution where that does not
  land under the performance track first
- `MERGE INTO` if scoped carefully for sync/upsert workflows
- narrow local `ATTACH`-style multi-database reads only if an ADR defines
  cross-file transaction, locking, encryption, and recovery semantics

### Guardrails

- do not duplicate the Lua extension runtime
- do not add arbitrary native `.load` support here
- keep this track focused on SQL syntax, catalog compatibility, and migration
  ergonomics
- avoid expanding core import/export features in this track

## 26. Advanced Geospatial Semantics And Analytics

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

## 27. Deterministic Testing And Binding Snapshot Assertions

**Status:** `BACKLOG`

**Future Version:** Later

**Source of truth:** Needs ADR/spec. Follows reliability validation (item 18)
and builds on the Python test harness and binding smoke test infrastructure.

### Why This Matters

DecentDB has a Python test harness, Rust unit tests, and per-binding smoke tests.
But binding authors and application developers have no deterministic test
infrastructure for asserting query results, schema states, and migration
outcomes across engine versions. Test flakiness from non-deterministic date/time
values, auto-increment sequences, floating-point differences, and
platform-dependent sort orders makes it harder to write reliable binding tests.

SQLite's test suite works partly because it offers deterministic mode settings
and fixture-based regression testing. DecentDB should provide similar
determinism controls for testing: seedable sequences, frozen timestamps,
deterministic collation, and snapshot-based assertion helpers that make binding
and application tests reliable across platforms and versions.

### Possible Direction

- deterministic test mode: seedable auto-increment, frozen `NOW()`/`TODAY()`,
  deterministic random, and deterministic collation order
- result snapshot assertion helpers for binding smoke tests
- schema snapshot comparison for migration tests
- migration history assertion for branch rehearsal tests
- test fixture generation from benchmark schemas with deterministic data
- C ABI test configuration for enabling deterministic mode
- documentation patterns for common deterministic testing scenarios

### Guardrails

- deterministic mode is for testing only; it must not be a production
  configuration
- do not change WAL, recovery, or crash-recovery semantics in deterministic
  mode; only suppress non-deterministic observable values
- keep binding smoke tests runnable without deterministic mode; deterministic
  mode should tighten assertions, not gate CI
- deterministic mode must work across all maintained binding targets

## 28. WAL Streaming Replication

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

## 29. Cloud-Native Object Storage VFS And WASI Edge Profiles

**Status:** `BACKLOG`

**Future Version:** Later

**Source of truth:** Needs ADR/spec before implementation.

### Why This Matters

Serverless and edge deployments often have ephemeral local disks. An object
storage VFS could allow page-level reads and writes against S3, R2, Azure Blob,
or HTTP-backed storage. A narrower WASI profile could also support edge
functions with an in-memory or host-provided VFS before full object-storage
writes are safe.

### Why It Is Last

This is high complexity and has serious durability, latency, consistency, and
cache-invalidation risks. It should follow stronger local-first, browser,
mobile, performance, and operational foundations.

### Required Direction

- expand VFS semantics deliberately
- define WASI support tiers separately from browser OPFS and native filesystems
- use HTTP range requests for page reads
- use local cache aggressively
- define write coordination and consistency rules before implementation

## Suggestion Review Notes

The WIP suggestions were consolidated by keeping roadmap items that add clear
adoption, durability, observability, or safe-extensibility value beyond shipped
foundations.

Promoted as distinct roadmap items:

- incremental backup and point-in-time recovery
- online schema change execution
- resource governance, quotas, and automated maintenance
- reliability validation, fault injection, and deterministic replay
- structured CDC and logical change feeds
- curated Lua extension ecosystem
- Unicode collation and internationalization profile
- streaming and cursor-based result sets (resource governance and adoption
  concern; embedded hosts need bounded materialization)
- offline-first conflict resolution UX and declarative merge policies
  (local-first differentiator; builds on shipped sync and changeset surfaces)
- schema migration file management and CLI (developer-experience gap for
  version-tracked migration scripts)
- observability bridge: OpenTelemetry and structured export (production
  observability for embedded database adoption)
- binding ergonomics and performance contract (cross-binding consistency and
  performance guarantees)
- multi-tenant scoped isolation (narrow scoped-visibility mechanism distinct
  from shipped masking and excluded server-style auth)
- deterministic testing and binding snapshot assertions (binding-level test
  infrastructure for reliable cross-platform testing)

Folded into existing tracks instead of duplicated:

- page/key/layout compression and adaptive statistics remain under future
  measured performance follow-ups when benchmark evidence justifies the added
  contract or format risk. Plan caching and streaming result sets were promoted
  as distinct items because their adoption and governance impact exceeds pure
  performance optimization.
- schema linting, plan diff/regression reporting, session visibility, and
  connection lifecycle diagnostics belong under tracing/advisors/Doctor
- schema version registries, migration history, and compatibility validation
  belong under branch-aware migration rehearsal
- richer sync conflict handlers belong after the backend sync bridge proves the
  conservative public changeset path
- cross-branch read-only queries and reconciliation belong under agent/tooling
  integration
- immutable/read-only static distribution belongs under application/support
  bundles
- local `ATTACH`-style multi-database reads belong, if ever, under advanced SQL
  compatibility with strict cross-file transaction and locking rules

Not promoted because the premise is already delivered or the idea is off-lane:

- savepoints, snapshot isolation for readers, narrow triggers, row policies and
  masks, expression indexes, `PRAGMA integrity_check`/`quick_check`, basic
  online `save_as` backup, and large-value overflow compression are delivered
  foundations
- API/versioning guidance already lives in [`VERSIONING_GUIDE.md`](VERSIONING_GUIDE.md),
  the C ABI docs, and stable tooling metadata docs; promote only concrete
  uncovered compatibility gaps
- broad import/export, conversion, and external file-reader workflows remain
  Decent Bench concerns
- server-style users/roles/authentication, a general job queue, text-to-SQL,
  broad FDWs, arbitrary native extensions, CRDT column types, columnar/OLAP
  storage modes, and time-series-specific storage engines remain outside the
  core DecentDB roadmap
- WASM UDFs and shared-memory cross-process read paths are not promoted until
  measured demand justifies another sandbox/runtime or a high-risk read-path
  optimization

## Near-Term Sequence

1. Tighten structured errors and developer diagnostics early because they
   improve every binding and make later runtime work easier to consume.
2. Scope incremental backup/PITR separately from WAL streaming replication; the
   basic online backup API exists, but production recovery semantics need their
   own ADR.
3. Extend shipped `sys.*` metrics into opt-in tracing, advisors, and Doctor
   integration once the hot-path overhead budget is explicit.
4. Design branch-aware migration rehearsal and online schema execution together
   so validation and production promotion do not diverge.
5. Treat resource governance as part of browser/mobile readiness, not as an
   optional tuning-only feature.
6. Promote authenticated encryption/key-rotation work only after the v1 TDE and
   policy surfaces have production feedback and a follow-up ADR.
7. Build on delivered query plan caching and prepared-statement reuse when
   scoping process-global sharing, finer-grained invalidation, or binding
   throughput work.
8. Design streaming and cursor-based result sets as a resource-governance
   building block that enables bounded result materialization for embedded hosts.
9. Design offline-first conflict resolution UX and declarative merge policies
   after the backend sync bridge validates the public changeset contract.
10. Design schema migration file management and CLI as a developer-experience
    complement to branch-aware migration rehearsal.
11. Design the observability bridge after runtime tracing and advisors have a
    stable internal contract; export bridges depend on internal surfaces.
12. Formalize the binding ergonomics and performance contract after structured
    errors and diagnostics are delivered.
13. Promote backlog items into TODO only after the top adoption blockers have
    ADR/spec coverage or active implementation ownership.

## Market Notes

The roadmap order accounts for competitive pressure without becoming a feature
clone checklist:

- SQLite has mature FTS, JSONB, WASM/OPFS, PRAGMAs, CLI workflows, process-safe
  access, and decades of binding/tool familiarity.
- SQLite also has a basic online backup API and a large ecosystem around WAL
  backup/PITR tooling. DecentDB has `save_as`; the remaining gap is
  first-class incremental recovery semantics.
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
- SQLite, PostgreSQL, and many host frameworks have trained developers to
  expect stable machine-readable errors and useful diagnostics. DecentDB should
  compete on error quality as part of being easy to embed.
- International applications expect Unicode-aware collation and comparison
  choices, but DecentDB should avoid making ICU-sized data a hidden cost in
  browser/mobile builds.
- The largest DecentDB opportunity is integrated durable local-first workflow:
  fast embedded reads/writes, sync, branches, browser/mobile runtime,
  observability, security, and agent-readable tooling.
- SQLite caches compiled statements by default and PostgreSQL caches query plans
  across connections. DecentDB currently re-parses and re-validates on every
  execution; plan caching is a baseline expectation for high-throughput embedded
  workloads.
- SQLite, DuckDB, and PostgreSQL all support stepped/cursor result iteration.
  DecentDB's full-result materialization creates a hard ceiling for large queries
  on memory-constrained hosts; streaming results are both a governance and
  adoption concern.
- PowerSync, Electric SQL, and PGlite provide opinionated conflict resolution
  patterns out of the box. DecentDB has the foundation but needs declarative merge
  policies to make offline-first adoption easy.
- SQLite's `sqlite3` CLI and migration tooling (golang-migrate, Alembic, Flyway)
  make schema management feel simple. DecentDB needs a migration file workflow to
  complement its branch rehearsal foundations.
- Production teams running DecentDB inside larger stacks need OpenTelemetry and
  structured export to observe embedded database behavior alongside the rest of
  their infrastructure. SQLite has limited observability; DecentDB can
  differentiate here.
- Multi-tenant SaaS embeddings need scoped isolation that goes beyond row masking
  but does not require server-style authentication. This is a narrower scope than
  excluded auth features but addresses a real adoption blocker for platform
  embedders.

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
