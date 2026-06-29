# DecentDB Future Wins

**Status:** Consolidated roadmap

**Updated:** 2026-06-28

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
- make SQLite adoption assessment concrete with compatibility reports, rewrite
  hints, and migration-risk output rather than broad import/export tooling
- make ORM/framework support explicit enough that teams can evaluate DecentDB
  through the stack they already use
- treat packaging, signed release artifacts, and supported platform matrices as
  adoption work, not release-process trivia

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
| Rich structured errors and developer diagnostics | [`WIN_RICH_STRUCTURED_ERRORS_DEVELOPER_DIAGNOSTICS_SPEC.md`](_archive/WIN_RICH_STRUCTURED_ERRORS_DEVELOPER_DIAGNOSTICS_SPEC.md); ADR 0185; [`docs/api/error-codes.md`](../docs/api/error-codes.md); [`docs/user-guide/error-diagnostics.md`](../docs/user-guide/error-diagnostics.md) | Rich machine-readable diagnostics are delivered through the shared contract in Rust/C ABI and maintained bindings: stable subcodes, retry/permanence flags, redaction rules, docs anchors, and fixture-backed smoke coverage for structured error assertions. |
| Branch, diff, restore, and time travel | ADR 0153-0159, [`docs/api/cli-reference.md`](../docs/api/cli-reference.md#branch); 2.7.0 Dart branch workflow APIs | Future migration, support, and agent workflows should use branches as the safe rehearsal layer. |
| WASM/OPFS browser runtime | ADR 0161, ADR 0165, [`docs/api/wasm.md`](../docs/api/wasm.md), and `@decentdb/web` updates | Browser now has explicit capability probes, OPFS owner routing, Web Locks/BroadcastChannel coordination, relay helpers, diagnostics, and smoke/benchmark coverage. Follow-on work should build from the delivered browser parity baseline rather than reopen the ownership model. |
| Native geospatial foundation | ADR 0124-0128, [`docs/user-guide/data-types.md`](../docs/user-guide/data-types.md#geometry), [`docs/user-guide/indexes.md`](../docs/user-guide/indexes.md#spatial-indexes) | Future spatial work is advanced analytics and planner breadth, not proving DecentDB can store spatial values. |
| Built-in HTTP server and web console | [`docs/user-guide/web-console.md`](../docs/user-guide/web-console.md), [`docs/api/cli-reference.md`](../docs/api/cli-reference.md#serve) | Future agent/tooling surfaces can reuse the local HTTP shape, but Decent Bench remains the full IDE. |
| Stable tooling metadata and query contracts | [`STABLE_TOOLING_METADATA_CONTRACT.md`](STABLE_TOOLING_METADATA_CONTRACT.md) | DecentDB owns metadata/query-contract truth. Decent Bench owns generated SDK workflows. |
| Lua extension runtime and package model | [`WIN_LUA_EXTENSION_RUNTIME_SPEC.md`](_archive/WIN_LUA_EXTENSION_RUNTIME_SPEC.md); ADR 0169-0173; [`docs/user-guide/lua-extensions.md`](../docs/user-guide/lua-extensions.md) | Sandboxed Lua packages now provide DecentDB's safe extensibility story: manifest validation, install/enable/trust lifecycle, scalar functions, table-valued functions, aggregates, query-time collations, Rust/CLI/C ABI surfaces, and examples. |
| Local data security v1 | [ADR 0174](adr/0174-local-data-security-tde-policies-masking-audit-context.md); [`docs/user-guide/security.md`](../docs/user-guide/security.md); [`docs/api/configuration.md`](../docs/api/configuration.md#local-transparent-data-encryption-tde) | Transparent local encryption, durable row policies, projection masks, audit context, C ABI open options, and queryable audit context are delivered foundations. Future security work should extend this boundary rather than redefining it. |
| Native full-text search with BM25 ranking | [`WIN_FULL_TEXT_SEARCH_BM25_SPEC.md`](_archive/WIN_FULL_TEXT_SEARCH_BM25_SPEC.md); ADR 0175-0176; [`docs/user-guide/indexes.md`](../docs/user-guide/indexes.md#full-text-indexes) | `USING fulltext` indexes, analyzer metadata, `fulltext_match`, `bm25`, phrase/prefix queries, write-path maintenance, rebuild/verify, tooling metadata, documentation, and regression tests are delivered. Follow-on search work now belongs under hybrid search, fuzziness/suggestions, or performance-specific roadmap items. |
| Large-value overflow compression | [ADR 0048](adr/0048-optional-value-compression.md), [`docs/user-guide/data-types.md`](../docs/user-guide/data-types.md#compression) | Transparent zlib compression for large `TEXT` and `BLOB` overflow payloads is delivered. Future compression work should be framed as measured page/key/layout efficiency, not as first-time compression support. |
| Cross-process WAL coordination | [`WIN_CROSS_PROCESS_WAL_COORDINATION_SPEC.md`](_archive/WIN_CROSS_PROCESS_WAL_COORDINATION_SPEC.md); ADR 0177-0180; [`docs/user-guide/write-concurrency.md`](../docs/user-guide/write-concurrency.md#cross-process-wal-coordination) | Native OS processes can safely share local on-disk databases through byte-range locks, `.coord` sidecars, cross-process reader retention, WAL refresh, binding open options, `sys.process_*` diagnostics, and Doctor findings. Future runtime work can build on this foundation for browser/mobile hardening and richer operational tracing. |
| Browser SQL/API parity and production web hardening | [`WIN_BROWSER_SQL_API_PARITY_PRODUCTION_WEB_SPEC.md`](_archive/WIN_BROWSER_SQL_API_PARITY_PRODUCTION_WEB_SPEC.md); [`docs/api/wasm.md`](../docs/api/wasm.md); [`bindings/web/README.md`](../bindings/web/README.md) | Browser now has the `browser-app-v2` SQL profile, checked-in parity corpus, stable browser SQL errors, protocol/capability metadata, transaction/savepoint APIs, prepared statement paging, OPFS lifecycle guards/diagnostics, relay apply-before-ack helpers, framework recipes, and benchmark guardrails. Future browser work should target measured parser breadth, TDE/key handling, branch workflows, or browser-specific performance rather than this parity baseline. |
| Mobile production runtime and SDK hardening | [`WIN_MOBILE_PRODUCTION_RUNTIME_SDK_HARDENING_SPEC.md`](_archive/WIN_MOBILE_PRODUCTION_RUNTIME_SDK_HARDENING_SPEC.md); ADR 0181-0183; [`docs/api/dart.md`](../docs/api/dart.md); [`bindings/dart/flutter/README.md`](../bindings/dart/flutter/README.md) | Flutter mobile now has a package shell, Android/iOS artifact scripts and GitHub Actions workflow, mobile native loading defaults, redacted open-option diagnostics, key-provider helpers, async worker lifecycle docs/tests, public changeset apply-before-ack wrappers, and mobile storage/sidecar guidance. Follow-on mobile work should target measured device matrices, native Swift/Kotlin SDKs, watch lifecycle guarantees, or key rotation rather than first-class package hardening. |
| Default-fast performance and storage efficiency | [`WIN_DEFAULT_FAST_PERFORMANCE_STORAGE_EFFICIENCY_SPEC.md`](_archive/WIN_DEFAULT_FAST_PERFORMANCE_STORAGE_EFFICIENCY_SPEC.md); ADR 0184; [`docs/user-guide/performance.md`](../docs/user-guide/performance.md); [`docs/api/configuration.md`](../docs/api/configuration.md) | **Priority #1 complete:** durable defaults keep the historical 4 MiB cache after executor fast-path fixes recovered read headroom, while explicit low-memory/balanced/tuned durable profile helpers provide 4 MiB, 16 MiB, and 64 MiB options. The delivered slice includes canonical benchmark profile metadata, Python binding hot-path benchmark slices, storage/cold-state metadata, prepared-insert fast-path fixes, transaction-scoped prepared batches for repeated Rust inserts, runtime-only covering-index execution for safe `INCLUDE (...)` projections, parser-bypass metadata/row-id reads for plain `COUNT(*)` and integer primary-key projections, deferred table materialization behavior that preserves valid indexes, scalar aggregate fast paths that scan persisted payload columns without forcing full row materialization, lazy default-open work for checkpoint/reactive/coordination/empty-schema maintenance, and a green rust-baseline smoke/medium/full/huge comparison against the checked-in historical default-profile results. Follow-on performance work should be measured and scoped to new evidence, not this completed baseline. |
| Query plan caching and prepared-statement reuse | [`WIN_QUERY_PLAN_CACHING_AND_STATEMENT_REUSE.md`](_archive/WIN_QUERY_PLAN_CACHING_AND_STATEMENT_REUSE.md); ADR 0190-0194; [`docs/user-guide/performance.md`](../docs/user-guide/performance.md#plan-cache) | Connection-local parsed AST and prepared-plan bundle caches are delivered with default-on bounded memory, invalidation, diagnostics, CLI/C ABI access, Doctor guidance, and native/rust-baseline guardrail benchmarks. Future plan-cache work should be scoped as process-global sharing, object-level invalidation, or binding-specific throughput work. |
| Statement row views and early streaming surfaces | [`docs/api/c-cpp.md`](../docs/api/c-cpp.md#streaming-row-views), [`docs/api/dart.md`](../docs/api/dart.md#streaming-and-pagination), `include/decentdb.h` | Borrowed C ABI row views, batch row-view fetch helpers, and Dart `step()`/`nextPage()` streaming are delivered foundations. Future cursor work should finish cross-binding parity and reduce internal executor materialization, not describe streaming as absent. |
| Benchmark profiles and release assets | [`BENCHMARKING_GUIDE.md`](BENCHMARKING_GUIDE.md), `crates/decentdb-benchmark`, `data/bench_summary.json` | Performance work should target measured default and tuned profiles, storage efficiency, cold-open behavior, and release-chart regressions. |

## Status Map

Status values:

- `TODO`: prioritized roadmap work that is not actively being implemented right
  now.
- `IN PROGRESS`: active implementation or design work is underway right now.
- `BACKLOG`: valuable, but not part of the near-term implementation path.

Future version values are planning buckets, not release commitments. The
current public release in this repository is `2.15.0`, and the current
planning release bucket in this repository is `2.15.0`. `vNext` means
the first release bucket after `2.15.0` only when scope is explicitly accepted.
`vNext+1` and `vNext+2` are follow-on planning buckets, not exact semantic
versions.

Roadmap lifecycle: once a Future Win is 100% implemented, tested, and
documented, remove it from this roadmap. Completed and delivered work is no
longer a Future Win. Keep only a concise `Delivered Context` entry when the
shipped foundation affects follow-on roadmap decisions.

| Priority | Future Version | Status | Feature | Current Source Of Truth | Why This Rank |
|---:|---|---|---|---|---|
| 1 | vNext | In Progress | Core read/query engine performance | [`WIN_PERFORMANCE_IMPROVEMENTS_01.md`](WIN_PERFORMANCE_IMPROVEMENTS_01.md); ADR 0112, 0143-0145, 0184, 0190-0194 | Read performance is Priority #2. The 2026-06-29 slice delivered row-id alias join-key trimming, explicit-JOIN cost-based `IndexedJoin`, and full smoke/medium/full/huge evidence; several §6/§13 gates remain unmet (full/huge peak RSS ~23% vs 25% gate, public `insert_rows_per_sec`/`read_p95_ms` regression, huge view 2x gap) — see §14/§15 of the spec |
| 2 | vNext | TODO | Cross-binding cursor, row-view, and batch API parity | C ABI row-view docs and delivered Dart streaming; needs binding parity spec | Existing row-view foundations should become a consistent developer contract across Python, Node, .NET, Go, Java, Dart, WASM, and C ABI |
| 3 | vNext | TODO | Postgres backend sync bridge and declarative conflict policies | Sync slices, relay, public changesets; needs ADR/spec | Existing Postgres/Supabase-style apps are the shortest path to local-first adoption; conflict UX must ship with the bridge rather than trail it |
| 4 | vNext | TODO | Migration workflow v1: files, branch rehearsal, and promotion | Branch/diff/restore foundations; needs ADR/spec | Developers evaluate databases through migration workflows; branch rehearsal is distinctive only if it is reachable from normal migration files and CI |
| 5 | vNext | TODO | Doctor/advisor MVP and runtime tracing foundation | [`WIN_RUNTIME_TRACING_ADVISORS_AND_DOCTOR_INTEGRATION.md`](WIN_RUNTIME_TRACING_ADVISORS_AND_DOCTOR_INTEGRATION.md); ADR 0186-0189 | Explains slow queries, lock waits, index usage, schema lint, and maintenance issues before asking users to trust larger production deployments |
| 6 | vNext+1 | TODO | Incremental backup and point-in-time recovery | Basic `save_as` backup is delivered; needs ADR/spec for WAL archive/PITR semantics | Durable recovery artifacts are a production requirement and distinct from live replication |
| 7 | vNext+1 | TODO | JSONB binary storage and JSON path indexing | Needs ADR/spec | SQLite JSONB is now a baseline expectation for JSON-heavy embedded apps; path indexes also support diagnostics and compatibility work |
| 8 | vNext+1 | TODO | Hybrid local search: FTS, trigram, vector, and rank fusion | FTS foundation is delivered; vector and rank fusion need ADR/spec | Competitors market vector search; DecentDB can differentiate by combining keyword, substring, vector, rank fusion, and relational filters locally |
| 9 | vNext+1 | TODO | Resource governance, quotas, and automated maintenance | Needs ADR/spec; follows row-view/cursor parity and diagnostics | Embedded hosts need explicit storage, WAL, memory, quota, and maintenance behavior to avoid runaway local resource use |
| 10 | vNext+1 | TODO | Authenticated encryption, key rotation, and platform key-store helpers | ADR 0174 follow-up | TDE v1 provides local confidentiality; regulated deployments need tamper evidence, rotation, and turnkey OS/browser/mobile key handling |
| 11 | vNext+1 | TODO | Online schema change execution | Needs ADR/spec; follows migration workflow v1 | Reduces disruption for large table rebuilds and index work after the branch-backed migration workflow exists |
| 12 | vNext+2 | TODO | Observability bridge: OpenTelemetry and structured export | Needs ADR/spec; follows Doctor/advisor MVP | Production teams need external observability once internal trace/advisor contracts are stable |
| 13 | vNext+2 | TODO | Incrementally maintained projections | Needs ADR/spec | Accelerates dashboards, local read models, and reactive query workloads after planner/executor foundations improve |
| 14 | vNext+2 | TODO | SQLite adoption kit and compatibility assessment | Needs ADR/spec; complements advanced SQL compatibility | Converts SQLite users with concrete schema/query compatibility reports, unsupported-feature diagnostics, and rewrite hints |
| 15 | vNext+2 | TODO | ORM and framework certification kits | Needs ADR/spec; complements binding contract | Developers choose embedded databases through SQLAlchemy, Knex, EF Core, Drift, Go `database/sql`, JDBC, and app-framework examples |
| 16 | vNext+2 | TODO | Packaging, install trust, and release artifact matrix | Needs ADR/spec if signing or compatibility contracts expand | First-run success, signed artifacts, package-size budgets, and platform support matrices are adoption blockers for embedded engines |
| 17 | Later | BACKLOG | Agent and tooling integration mode | [`STABLE_TOOLING_METADATA_CONTRACT.md`](STABLE_TOOLING_METADATA_CONTRACT.md); needs ADR/spec | Makes the agent-friendly promise concrete without putting LLM behavior in the engine |
| 18 | Later | BACKLOG | Reliability validation, fault injection, and deterministic replay | [`TESTING_STRATEGY.md`](TESTING_STRATEGY.md); needs ADR/spec for replay capture | Raises confidence in ACID/concurrency changes and makes production bugs reproducible without weakening hot paths |
| 19 | Later | BACKLOG | Application and support bundle format | Needs ADR/spec | Useful portable artifact and diagnostics story, but should follow security/redaction foundations |
| 20 | Later | BACKLOG | Temporal row history and auditable state | Needs ADR/spec | Strong regulated/support workflow, but should follow security, audit context, and sync hardening |
| 21 | Later | BACKLOG | Structured CDC and logical change feeds | Change streams, public changesets, and sync journal are delivered; needs ADR/spec | Lets DecentDB feed event-driven systems without becoming a message broker or bypassing local transactions |
| 22 | Later | BACKLOG | Curated Lua extension ecosystem | Lua runtime/package model is delivered; needs ADR/spec outside core engine if registry semantics affect trust | Turns safe extensibility into an adoption moat while preserving the no-native-extension stance |
| 23 | Later | BACKLOG | Multi-tenant scoped isolation | Needs ADR/spec | Narrow scoped-visibility mechanism distinct from shipped masking and excluded server-style auth; enables SaaS-embedded patterns |
| 24 | Later | BACKLOG | Unicode collation and internationalization profile | Query-time built-in and Lua collations are delivered; needs ADR/spec for ICU/data-size strategy | International apps need correct Unicode sort/search semantics, but portability and binary size make it a later tradeoff |
| 25 | Later | BACKLOG | Advanced SQL compatibility surface | [`WIN_ADVANCED_SQL_COMPATIBILITY_SURFACE.md`](WIN_ADVANCED_SQL_COMPATIBILITY_SURFACE.md) | Useful adoption polish after higher-impact runtime, recovery, migration, and workflow blockers |
| 26 | Later | BACKLOG | Advanced geospatial semantics and analytics | ADR 0128 deferred work; needs follow-up ADR/spec | Builds on shipped spatial support without implying the foundation is unfinished |
| 27 | Later | BACKLOG | Deterministic testing and binding snapshot assertions | Needs ADR/spec; follows reliability validation | Binding-level test infrastructure and deterministic assertion patterns improve Priority #3 confidence |
| 28 | Later | BACKLOG | WAL streaming replication | Needs ADR/spec | Useful HA/read-scale story, but weaker than local-first sync and PITR for DecentDB identity |
| 29 | Later | BACKLOG | Cloud-native object storage VFS and WASI edge profiles | Needs ADR/spec | Interesting edge/serverless story with high durability, consistency, packaging, and cache-invalidation complexity |

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

## 1. Core Read/Query Engine Performance

**Status:** `TODO`

**Future Version:** vNext

**Source of truth:** [`WIN_PERFORMANCE_IMPROVEMENTS_01.md`](WIN_PERFORMANCE_IMPROVEMENTS_01.md), ADR 0112, ADR 0143-0145, ADR 0184, and ADR 0190-0194.

### Why This Matters

DecentDB now wins the public README benchmark metrics across durable profiles,
but the active diagnostic evidence still shows a narrower credibility gap:
generic executor paths, view expansion/execution, and small fixed-overhead read
queries can fall behind SQLite when they miss specialized fast paths.
Read-heavy applications are the common case for embedded databases, so this is
now the highest-leverage engine investment.

### Desired Capability

- late-materialized execution for hot generic scan/filter/project paths
- cost-based access-path, join, and view planning using persisted statistics
- predicate, projection, order, and limit pushdown through view expansion
- first-class planned indexed/hash join and view-scan operators
- reduced `Dataset`, `Vec<Vec<Value>>`, and `Vec<QueryRow>` intermediate materialization
- RSS and per-query allocation reductions in rust-baseline full/huge runs
- public benchmark metrics preserved as regression guardrails

### Guardrails

- no benchmark-name-specific behavior
- no durability weakening to claim a performance win
- no broad parallel query engine in this slice
- no new persistent format without a separate ADR
- every claimed win needs before/after public and rust-baseline evidence

## 2. Cross-Binding Cursor, Row-View, And Batch API Parity

**Status:** `TODO`

**Future Version:** vNext

**Source of truth:** Delivered C ABI row-view APIs, Dart streaming/pagination docs, and a needed cross-binding parity spec.

### Why This Matters

The engine and C ABI already expose borrowed row views and batch row-view fetch
helpers, and Dart already exposes streaming/pagination. The roadmap should no
longer describe streaming as absent. The real adoption gap is parity: Python,
Node, .NET, Go, Java, Dart, WASM, and C ABI should present a consistent story
for prepared statement reuse, row iteration, batch writes, errors, and cleanup.

### Desired Capability

- binding-by-binding cursor and streaming support matrix
- shared row-view lifetime rules and fetch-batch semantics
- consistent prepared statement reset/reuse patterns
- batch insert/update helpers that reduce FFI crossing overhead
- native-to-binding performance target ratios and benchmark gates
- examples for large result iteration, early cursor close, and bounded memory
- clear fallback behavior for bindings that must materialize rows for idiomatic APIs

### Guardrails

- keep the C ABI as the authoritative contract
- do not force identical language APIs where idioms differ
- do not change existing result ownership without ABI/version guidance
- small result sets must not regress materially from streaming overhead

## 3. Postgres Backend Sync Bridge And Declarative Conflict Policies

**Status:** `TODO`

**Future Version:** vNext

**Source of truth:** Shipped sync slices, relay, public changesets, conflict surfaces; needs ADR/spec.

### Why This Matters

Many teams that would consider DecentDB already run Postgres or a
Supabase-style backend. The fastest local-first adoption path is not asking
those teams to replace their backend first; it is giving them a conservative,
observable bridge into DecentDB's local changeset model. That bridge needs a
conflict policy story from day one, because unresolved conflict UX is where
local-first prototypes often become production blockers.

### Desired Capability

- Postgres-first source/sink bridge with explicit table mappings
- server identity, tenant, and audit context mapping into DecentDB metadata
- shape/subset definitions for what syncs to each client or workspace
- schema compatibility checks before data moves
- import/export through public changesets, not raw internal journals
- declarative per-table/per-column conflict policies: fail, last-write-wins,
  field-level merge, application merge, and trusted Lua resolver
- conflict preview, retry, and inspection through CLI, Doctor, and `sys.*`
- examples for Supabase/Postgres-backed desktop, mobile, and browser apps

### Guardrails

- do not turn DecentDB into a general ETL or FDW product
- do not bypass local transaction, policy, TDE, masking, or sync semantics
- fail-and-notify is a valid conflict policy
- unsupported type/schema differences must be explicit before sync starts

## 4. Migration Workflow V1: Files, Branch Rehearsal, And Promotion

**Status:** `TODO`

**Future Version:** vNext

**Source of truth:** Branch, snapshot, diff, restore, and merge foundations; needs ADR/spec.

### Why This Matters

Branch-aware migration rehearsal is distinctive, but most developers think in
migration files, CI jobs, and framework migrations. DecentDB needs a normal
migration workflow that uses branches under the hood: create a branch, apply
files, validate, diff, and promote safely.

### Desired Capability

- `decentdb migrate create/apply/revert/status/verify` CLI commands
- ordered SQL migration files with metadata headers
- durable `sys.migrations` history and expected-state checks
- dry-run and plan-only modes
- branch rehearsal: create branch, apply migration, validate, diff, promote
- rollback/restore plan generation
- compatibility validation for query contracts, policies, sync shapes, and indexes
- integration recipes for common migration tools and CI pipelines

### Guardrails

- migration files remain SQL; do not invent a new DSL
- do not hide destructive schema changes behind automatic promotion
- branch merge semantics remain conservative
- catalog or format changes still follow ADR 0131 migration requirements

## 5. Doctor/Advisor MVP And Runtime Tracing Foundation

**Status:** `TODO`

**Future Version:** vNext

**Source of truth:** [`WIN_RUNTIME_TRACING_ADVISORS_AND_DOCTOR_INTEGRATION.md`](WIN_RUNTIME_TRACING_ADVISORS_AND_DOCTOR_INTEGRATION.md), ADR 0186-0189.

### Why This Matters

Developers need DecentDB to explain slow queries, missing indexes, lock waits,
checkpoint/WAL growth, and schema hazards without requiring source-level engine
knowledge. The near-term win is not broad telemetry export; it is local,
reviewable advisor output that helps users fix their database.

### Desired Capability

- `PRAGMA doctor` and CLI Doctor report output
- `sys.slow_queries`, `sys.lock_waits`, `sys.index_usage`, `sys.sessions`, and
  `sys.doctor_findings` where the accepted ADRs define them
- missing/unused/redundant index advisor
- query-plan and plan-regression advisor
- schema lint for foreign-key indexes, constraints, sync-incompatible schema,
  migration hazards, and expensive rebuild patterns
- WAL/checkpoint/retention warnings
- fix-plan output that is reviewable and not auto-applied destructively

### Guardrails

- expensive tracing is opt-in
- no recursive telemetry writes
- parameter values and sensitive paths are redacted by default
- hot-path overhead is measured and bounded

## 6. Incremental Backup And Point-In-Time Recovery

**Status:** `TODO`

**Future Version:** vNext+1

**Source of truth:** Basic online `save_as` backup is delivered; needs ADR/spec before adding WAL archive/PITR semantics.

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

## 7. JSONB Binary Storage And JSON Path Indexing

**Status:** `TODO`

**Future Version:** vNext+1

**Source of truth:** Needs ADR/spec before implementation.

### Why This Matters

DecentDB supports JSON scalar and table functions, but JSON-heavy applications
still pay repeated text parse costs. SQLite now ships JSONB, and JSON path
indexing is a practical expectation for app state, sync payloads, settings,
metadata, and document-like tables.

### Desired Capability

- binary JSON storage that can be projected as text JSON through existing APIs
- JSONB scalar and table-valued functions matching the chosen compatibility tier
- expression/path indexes over extracted typed scalars
- planner use of JSON path indexes for equality/range predicates
- validation and malformed-value behavior with stable diagnostics
- benchmark coverage against text JSON parse-heavy workloads

### Guardrails

- no raw JSONB ABI exposure unless explicitly requested and versioned
- large JSONB values use existing overflow mechanics unless an ADR says otherwise
- partial updates rebuild the logical value unless a narrower mutation format is
  separately accepted
- avoid promising PostgreSQL binary compatibility

## 8. Hybrid Local Search: FTS, Trigram, Vector, And Rank Fusion

**Status:** `TODO`

**Future Version:** vNext+1

**Source of truth:** FTS foundation is delivered; vector/HNSW and rank fusion need ADR/spec.

### Why This Matters

Vector search has become part of the embedded database evaluation checklist,
but standalone vector indexing is not enough for real applications. DecentDB can
stand out by combining native FTS/BM25, substring/trigram search, vector search,
relational filters, and rank fusion in one local durable engine.

### Desired Capability

- `VECTOR(dim)` or equivalent typed vector storage
- durable approximate nearest-neighbor index with explicit distance metrics
- exact vector distance functions for smaller datasets and validation
- hybrid queries combining FTS, trigram, vector, scalar filters, and joins
- rank-fusion helpers such as reciprocal rank fusion if benchmarked workloads
  justify them
- WASM/mobile portability story
- benchmarks against SQLite/libSQL and DuckDB vector-search surfaces where fair

### Guardrails

- no external native extension requirement
- index build/update costs must be visible in write latency and diagnostics
- no hidden network or model-inference behavior in the engine
- keep vector search scoped to local data indexing, not a hosted AI platform

## 9. Resource Governance, Quotas, And Automated Maintenance

**Status:** `TODO`

**Future Version:** vNext+1

**Source of truth:** Needs ADR/spec; follows cursor parity and Doctor/advisor foundations.

### Why This Matters

Embedded databases run inside someone else's process, device, browser quota, or
serverless sandbox. Unbounded result materialization, WAL growth, retained
branches/snapshots, sync journals, extension execution, and maintenance work can
harm the host even when DecentDB remains logically correct.

### Desired Capability

- database-level page/byte quotas with explicit over-limit errors
- memory budgets for result sets, temporary structures, planner work, extension
  calls, and binding transports
- WAL, checkpoint, branch, snapshot, and sync-journal retention policies
- vacuum/compaction, `ANALYZE`, integrity checks, and full-text rebuild work
  scheduled or throttled during safe idle windows
- progress and warnings through `sys.*`, Doctor, CLI, and bindings
- browser OPFS/mobile quota guidance matching platform behavior

### Guardrails

- reject writes cleanly rather than silently dropping data
- persist quota and retention settings durably
- background work yields to foreground operations
- do not auto-delete named snapshots, branches, sync data, or audit records
  without explicit retained-policy configuration

## 10. Authenticated Encryption, Key Rotation, And Platform Key-Store Helpers

**Status:** `TODO`

**Future Version:** vNext+1

**Source of truth:** ADR 0174 follow-up.

### Why This Matters

TDE v1 removes the major local confidentiality blocker without changing logical
database behavior. Regulated deployments eventually need tamper evidence,
rotation, and safer key handling across desktop, server, browser, and mobile.

### Desired Capability

- authenticated page or chunk encryption with explicit recovery semantics
- online or staged key rotation
- plaintext-to-encrypted migration tooling
- platform key-store recipes for Windows DPAPI, macOS Keychain, Linux secret
  stores, mobile keychains, browser CryptoKey/OPFS, and server/KMS use
- support-bundle and telemetry redaction rules that understand encrypted deployments

### Guardrails

- do not compromise random-access WAL/page writes without a measured design
- do not hide v1 confidentiality-only boundaries behind vague security language
- keep key material outside database pages, WAL, sync journals, audit rows, and diagnostics

## 11. Online Schema Change Execution

**Status:** `TODO`

**Future Version:** vNext+1

**Source of truth:** Needs ADR/spec. Should follow migration workflow v1.

### Why This Matters

Branch rehearsal proves a migration is safe before promotion. It does not by
itself make the production execution non-disruptive. Large table rebuilds,
constraint validation, index creation, type changes, and column drops can still
interrupt normal application work if they require an exclusive database lock for
the entire operation.

### Desired Capability

- narrow online-safe schema-change support tiers
- online index build/rebuild where readers keep stable snapshots
- resumable or staged table rewrites with crash recovery
- drift detection when writes continue while a staged migration is prepared
- progress, cancellation, and write-latency impact surfaced through `sys.*`

### Guardrails

- preserve the one-writer/many-readers model
- dual-schema reads and writes need ADR coverage before implementation
- every format or catalog change follows ADR 0131 migration requirements

## 12. Observability Bridge: OpenTelemetry And Structured Export

**Status:** `TODO`

**Future Version:** vNext+2

**Source of truth:** Needs ADR/spec. Follows Doctor/advisor MVP.

### Why This Matters

Doctor and runtime tracing explain DecentDB locally. Production teams also need
to see embedded database behavior alongside their application telemetry. This
bridge should translate internal events into standard observability formats
without adding collectors or network listeners to the engine core.

### Desired Capability

- opt-in OpenTelemetry spans for query execution, transactions, checkpoint,
  WAL, sync, and branch operations
- opt-in metrics export for cache hit rates, WAL growth, commit latency,
  checkpoint duration, and connection counts
- structured JSON logging bridge
- binding surfaces for trace context propagation

### Guardrails

- no always-on telemetry
- no network listener or exporter dependency in the core engine
- no sensitive SQL text, parameter values, TDE keys, or paths by default

## 13. Incrementally Maintained Projections

**Status:** `TODO`

**Future Version:** vNext+2

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

## 14. SQLite Adoption Kit And Compatibility Assessment

**Status:** `TODO`

**Future Version:** vNext+2

**Source of truth:** Needs ADR/spec; complements advanced SQL compatibility.

### Why This Matters

A team considering DecentDB often starts with an existing SQLite schema, SQL
corpus, or ORM. Broad import/export belongs in Decent Bench, but the engine and
CLI should still make compatibility risk concrete: what will work, what needs a
rewrite, and what tradeoff the user is accepting.

### Desired Capability

- schema compatibility report for SQLite-originated DDL
- SQL corpus parser/planner report with unsupported constructs and rewrite hints
- PRAGMA mapping report showing accepted, ignored, and unsupported pragmas
- index/function/collation compatibility checks
- migration-risk output suitable for CI and agents
- links to Decent Bench for rich conversion workflows

### Guardrails

- this is not a general ETL/import/export product
- do not silently rewrite SQL in production execution
- keep reports deterministic and machine-readable

## 15. ORM And Framework Certification Kits

**Status:** `TODO`

**Future Version:** vNext+2

**Source of truth:** Needs ADR/spec; complements binding parity.

### Why This Matters

Developers rarely evaluate an embedded database through the raw C ABI. They use
SQLAlchemy, Knex, EF Core, Drift, Go `database/sql`, JDBC, and app-framework
recipes. DecentDB needs explicit compatibility targets and examples for those
entry points.

### Desired Capability

- supported ORM/framework matrix with known feature tiers
- smoke suites for top frameworks per maintained binding
- migration, transaction, prepared-statement, and error examples
- performance notes for batching and cursor iteration per framework
- CI-friendly certification fixtures

### Guardrails

- do not add framework-specific SQL behavior to the engine
- keep binding APIs idiomatic but contract-aligned
- unsupported ORM features should fail with actionable diagnostics

## 16. Packaging, Install Trust, And Release Artifact Matrix

**Status:** `TODO`

**Future Version:** vNext+2

**Source of truth:** Needs ADR/spec if signing, compatibility, or package-support guarantees expand.

### Why This Matters

Embedded database adoption can fail before a benchmark ever runs. Users need
prebuilt artifacts, package-size clarity, signed releases, platform support
matrices, and a predictable first install across native, browser, mobile, and
language bindings.

### Desired Capability

- supported platform/toolchain matrix for every maintained binding
- artifact signing and checksum guidance
- package-size budgets for browser/mobile artifacts
- install smoke tests from published packages, not only workspace paths
- release notes that state ABI, file-format, binding, and platform changes clearly

### Guardrails

- do not overpromise unsupported targets
- keep local source builds possible
- signing and artifact retention policies need explicit ownership

## 17. Agent And Tooling Integration Mode

**Status:** `BACKLOG`

**Future Version:** Later

**Source of truth:** [`STABLE_TOOLING_METADATA_CONTRACT.md`](STABLE_TOOLING_METADATA_CONTRACT.md); needs ADR/spec.

### Desired Capability

- local MCP or equivalent agent-tool server mode over the existing local HTTP shape
- machine-readable schema, query contract, plan, doctor, sync, and branch outputs
- explicit read-only and branch-sandbox modes for agent operations
- query validation without execution
- structured repair/migration proposal outputs for review before execution

### Guardrails

- DecentDB should not run an LLM or natural-language agent inside the engine
- no agent write bypasses normal SQL, transaction, branch, and policy semantics
- Decent Bench remains the home for rich visual workflows and generated SDK output

## 18. Reliability Validation, Fault Injection, And Deterministic Replay

**Status:** `BACKLOG`

**Future Version:** Later

**Source of truth:** [`TESTING_STRATEGY.md`](TESTING_STRATEGY.md); needs ADR/spec for production replay capture.

### Desired Capability

- deterministic fault-injection harness around VFS, WAL, checkpoint, write
  queue, reader slots, sync apply, and branch promotion paths
- continuous fuzzing for SQL, planning, execution, recovery, sync changesets,
  and public JSON inputs
- disk-full, torn-write, corruption, crash, lock-contention, and stale-sidecar suites
- opt-in logical debug replay with redaction

### Guardrails

- production replay capture is strictly opt-in
- no hot-path overhead when disabled
- replay is diagnostic tooling, not a replacement for WAL recovery

## 19. Application And Support Bundle Format

**Status:** `BACKLOG`

**Future Version:** Later

**Source of truth:** Needs ADR/spec before implementation.

### Desired Capability

- checkpointed database image plus manifest metadata
- optional assets, signatures, sanitized Doctor/sys diagnostics, branch/snapshot
  identifiers, and sync metadata summary
- immutable/read-only distribution mode for static datasets and support-safe inspection

### Guardrails

- not a general import/export or ETL feature
- compatibility, integrity, signature, and recovery rules need an ADR
- support bundles require sanitization/redaction before regulated use

## 20. Temporal Row History And Auditable State

**Status:** `BACKLOG`

**Future Version:** Later

**Source of truth:** Needs ADR/spec before implementation.

### Possible Direction

- opt-in temporal tables or row history
- `FOR SYSTEM_TIME AS OF` style query surface if it fits the planner
- `sys.row_history` inspection surface
- actor/context metadata from bindings
- retention and redaction policies
- sync and branch provenance fields

### Guardrails

- history must be opt-in and storage-cost visible
- do not conflate branch snapshots with row-level audit history

## 21. Structured CDC And Logical Change Feeds

**Status:** `BACKLOG`

**Future Version:** Later

**Source of truth:** Change streams, public changesets, and sync journal are delivered; needs ADR/spec.

### Desired Capability

- stable row-level change event envelope built on public changeset semantics
- optional before/after images where policy and storage cost allow
- filtered feeds by table, branch, replica, tenant, or policy scope
- durable consumer progress markers if the retention model supports them safely

### Guardrails

- do not build webhook execution or a message broker into the engine
- slow external consumers must not block the writer indefinitely

## 22. Curated Lua Extension Ecosystem

**Status:** `BACKLOG`

**Future Version:** Later

**Source of truth:** Lua runtime/package/trust model is delivered; needs ADR/spec if registry trust becomes official.

### Desired Capability

- curated package index for reviewed Lua packages
- package signing, checksums, compatibility metadata, and trust provenance
- smoke tests and compatibility badges across maintained targets
- examples for common safe extension categories

### Guardrails

- no arbitrary native extension loading
- registry work must not weaken manifest/trust lifecycle

## 23. Multi-Tenant Scoped Isolation

**Status:** `BACKLOG`

**Future Version:** Later

**Source of truth:** Needs ADR/spec. Follows resource governance, security, and audit context foundations.

### Desired Capability

- tenant or workspace context set at open time or per transaction
- scoped visibility filters automatically applied to queries
- scoped storage quotas composing with resource governance
- scoped sync, branch, and diagnostics boundaries

### Guardrails

- this is not server-style users, roles, or authentication
- the host application remains responsible for who can open a handle
- scoped filters must be verifiable by Doctor and `sys.*`

## 24. Unicode Collation And Internationalization Profile

**Status:** `BACKLOG`

**Future Version:** Later

**Source of truth:** Query-time built-in and Lua collations are delivered; needs ADR/spec for ICU/data-size strategy.

### Desired Capability

- explicit Unicode collation support tiers
- locale-aware sort and case-insensitive comparison semantics
- normalization policy for comparison and index use
- browser/mobile/WASI package-size strategy for collation data
- compatibility tests against chosen Unicode fixtures

### Guardrails

- do not silently change existing binary/default collation semantics
- do not make every build carry large locale data by default without an ADR

## 25. Advanced SQL Compatibility Surface

**Status:** `BACKLOG`

**Future Version:** Later

**Source of truth:** [`WIN_ADVANCED_SQL_COMPATIBILITY_SURFACE.md`](WIN_ADVANCED_SQL_COMPATIBILITY_SURFACE.md).

### Current Direction

- explicit sequence objects
- materialized views not covered by the projection track
- binding-friendly array parameter/table-valued input support
- SQL-defined functions if supported separately from Lua extensions
- deferred constraints, exclusion constraints, and `MERGE INTO` if scoped carefully
- narrow local `ATTACH`-style reads only with a strict ADR

### Guardrails

- do not duplicate the Lua extension runtime
- no arbitrary native `.load` support
- keep this focused on SQL syntax, catalog compatibility, and migration ergonomics

## 26. Advanced Geospatial Semantics And Analytics

**Status:** `BACKLOG`

**Future Version:** Later

**Source of truth:** ADR 0128 deferred work; needs follow-up ADR/spec.

### Possible Direction

- arbitrary planner-native spatial joins beyond the first point-in-polygon shape
- spatial statistics in `ANALYZE`
- true 3D predicate semantics and explicit 3D-aware spatial index modes
- additional spatial reference systems and coordinate transformation support

### Guardrails

- preserve the shipped EWKB/C ABI contract
- avoid GEOS/PROJ/GDAL dependencies unless an ADR justifies the tradeoff

## 27. Deterministic Testing And Binding Snapshot Assertions

**Status:** `BACKLOG`

**Future Version:** Later

**Source of truth:** Needs ADR/spec. Follows reliability validation.

### Possible Direction

- deterministic test mode for timestamps, random values, and sequence seeds
- result snapshot assertion helpers for bindings
- schema snapshot comparison for migration tests
- fixture generation from benchmark schemas

### Guardrails

- deterministic mode is test-only
- do not change WAL, recovery, or crash-recovery semantics in deterministic mode

## 28. WAL Streaming Replication

**Status:** `BACKLOG`

**Future Version:** Later

**Source of truth:** Needs ADR/spec before implementation.

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

### Why It Is Last

This is high complexity and has serious durability, latency, consistency, and
cache-invalidation risks. It should follow stronger local-first, browser,
mobile, performance, and operational foundations.

### Required Direction

- expand VFS semantics deliberately
- define WASI support tiers separately from browser OPFS and native filesystems
- use HTTP range requests for page reads where safe
- define write coordination and consistency rules before implementation

## Suggestion Review Notes

The WIP suggestions were consolidated by keeping roadmap items that add clear
adoption, durability, observability, or safe-extensibility value beyond shipped
foundations.

Promoted or reprioritized as near-term roadmap items:

- core read/query engine performance because active benchmark evidence still
  points at generic executor, view, and planner gaps
- cross-binding cursor, row-view, and batch API parity because row-view
  foundations are delivered but not yet an even developer contract
- Postgres backend sync bridge plus declarative conflict policies because they
  jointly define the practical local-first adoption path
- migration workflow v1 because branch rehearsal needs migration files, CI, and
  promotion semantics to be reachable by ordinary application teams
- Doctor/advisor MVP before broad telemetry export
- JSONB/path indexing and hybrid search because they are now competitive
  expectations in SQLite-compatible and DuckDB-adjacent embedded ecosystems
- SQLite adoption assessment, ORM/framework certification, and packaging/install
  trust because those are adoption blockers that are not solved by engine
  benchmarks alone

Folded into existing tracks instead of duplicated:

- offline-first conflict UX is folded into the backend sync bridge because the
  bridge is incomplete without conflict policy ergonomics
- schema migration file management is folded into migration workflow v1 because
  files, branch rehearsal, validation, diff, and promotion should ship together
- streaming result sets are folded into cross-binding cursor parity and core
  read/query performance because the C ABI and Dart surfaces already exist
- schema linting, plan diff/regression reporting, session visibility, and
  connection lifecycle diagnostics remain under Doctor/advisor work
- richer sync conflict handlers should follow the conservative public changeset
  contract rather than bypass it
- immutable/read-only static distribution belongs under application/support bundles
- local `ATTACH`-style multi-database reads belong, if ever, under advanced SQL
  compatibility with strict cross-file transaction and locking rules

Not promoted because the premise is already delivered or the idea is off-lane:

- savepoints, snapshot isolation for readers, narrow triggers, row policies and
  masks, expression indexes, `PRAGMA integrity_check`/`quick_check`, basic
  online `save_as` backup, row-view C ABI helpers, and large-value overflow
  compression are delivered foundations
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

1. Land the core read/query engine performance work from
   `WIN_PERFORMANCE_IMPROVEMENTS_01.md` with before/after public and
   rust-baseline evidence.
2. Turn delivered C ABI/Dart streaming foundations into cross-binding cursor,
   row-view, prepared-statement, and batch API parity.
3. Design the Postgres bridge and declarative conflict policies as one
   local-first adoption slice.
4. Design migration workflow v1 around migration files plus branch rehearsal,
   validation, diff, and promotion.
5. Ship the Doctor/advisor MVP before broad OpenTelemetry export.
6. Scope incremental backup/PITR separately from WAL streaming replication; the
   basic online backup API exists, but production recovery semantics need their
   own ADR.
7. Promote JSONB/path indexing and hybrid search once the top engine and
   local-first adoption slices have active implementation ownership.
8. Treat resource governance as part of browser/mobile readiness, but build it
   on cursor parity and diagnostics so limits can be enforced and explained.
9. Promote authenticated encryption/key-rotation work after the v1 TDE and
   policy surfaces have production feedback and a follow-up ADR.
10. Keep online schema execution behind migration workflow v1 so validation and
    production promotion do not diverge.
11. Promote backlog items into TODO only after the top adoption blockers have
    ADR/spec coverage or active implementation ownership.

## Market Notes

The roadmap order accounts for competitive pressure without becoming a feature
clone checklist:

- SQLite has mature FTS, JSONB, WASM/OPFS, PRAGMAs, CLI workflows, process-safe
  access, session/changeset APIs, and decades of binding/tool familiarity.
- SQLite also has a basic online backup API and a large ecosystem around WAL
  backup/PITR tooling. DecentDB has `save_as`; the remaining gap is first-class
  incremental recovery semantics.
- SQLCipher-style encrypted local files are a common requirement for mobile,
  desktop, healthcare, finance, and enterprise apps. DecentDB now has TDE v1;
  the next security gap is authenticated encryption, rotation, and key-store ergonomics.
- SQLite-compatible ecosystems such as libSQL/Turso create pressure around
  sync, embedded deployment, native vector search, encryption-at-rest, and
  SQLite familiarity.
- libSQL should be treated as a separate adoption competitor from the newer
  Turso Database rewrite: libSQL's strength is production-ready SQLite fork
  compatibility, same-file/API continuity, native vector search, and Turso
  embedded-replica workflows where local reads can pair with cloud-primary
  writes or newer explicit push/pull sync. This reinforces the need for a
  concrete SQLite adoption kit, hybrid search, and a clear local-first sync
  bridge rather than another generic "SQLite alternative" message.
- DuckDB has strong ingestion, extension, FTS, vector, and analytics stories.
  Decent Bench, not DecentDB core, should own rich import/export and conversion workflows.
- Local-first stacks such as PGlite/Electric and PowerSync make reactive
  queries, browser/mobile sync, shape/subset sync, central-backend bridges, and
  developer tooling part of the expected conversation.
- PGlite adds another specific pressure point: browser and JS developers can
  run a real Postgres-shaped engine in WASM, use IndexedDB/filesystem
  persistence, live queries, and Postgres extensions such as pgvector/PostGIS.
  DecentDB should not chase full Postgres compatibility, but browser
  persistence diagnostics, quota/resource governance, reactive APIs, and
  Postgres bridge examples need to be strong enough that this comparison is
  about product fit rather than missing local-first basics.
- H2, LiteDB, and Firebird Embedded are narrower but useful adoption signals:
  H2 wins Java/JDBC and in-memory test-fixture decisions, LiteDB wins .NET
  document/POCO storage decisions, and Firebird Embedded wins Firebird SQL/PSQL
  and embedded-to-server continuity decisions. These do not change the top
  roadmap priorities, but they strengthen the case for ORM/framework
  certification kits, JDBC/.NET examples, and honest comparison docs that avoid
  pretending every embedded database is competing on the same axis.
- SQLite and DuckDB have mature extension ecosystems. DecentDB's shipped
  response is one official Lua extension language with strict manifests,
  sandboxing, and explicit trust rather than arbitrary native extension loading.
- SQLite, PostgreSQL, and many host frameworks have trained developers to
  expect stable machine-readable errors and useful diagnostics. DecentDB's
  structured errors are delivered; the next value is Doctor/advisor guidance.
- International applications expect Unicode-aware collation and comparison
  choices, but DecentDB should avoid making ICU-sized data a hidden cost in
  browser/mobile builds.
- The largest DecentDB opportunity is integrated durable local-first workflow:
  fast embedded reads/writes, sync, branches, browser/mobile runtime,
  observability, security, and agent-readable tooling.
- Plan caching and prepared-plan reuse are delivered. Future performance work
  should focus on planner quality, late materialization, view/join execution,
  cross-binding cursor parity, and measured binding overhead.
- SQLite, DuckDB, and PostgreSQL all support stepped/cursor result iteration.
  DecentDB has row-view foundations, but needs consistent binding parity and
  internal executor paths that avoid full materialization.
- PowerSync, Electric SQL, and PGlite provide opinionated sync/subset patterns.
  DecentDB has the foundation but needs a Postgres bridge and declarative merge
  policies to make offline-first adoption easy.
- SQLite's `sqlite3` CLI and migration tooling (golang-migrate, Alembic, Flyway)
  make schema management feel simple. DecentDB needs migration file workflow to
  complement its branch rehearsal foundations.
- Production teams running DecentDB inside larger stacks need OpenTelemetry and
  structured export eventually, but export bridges should follow stable local
  trace/advisor contracts.
- Multi-tenant SaaS embeddings need scoped isolation that goes beyond row
  masking but does not require server-style authentication. This remains a
  later scoped-visibility feature after resource governance and sync hardening.

Useful references:

- SQLite WASM / OPFS: https://sqlite.org/wasm/doc/trunk/persistence.md
- SQLite command-line shell: https://sqlite.org/cli.html
- SQLite JSONB: https://sqlite.org/jsonb.html
- SQLite session / changesets: https://sqlite.org/sessionintro.html
- SQLite R-Tree: https://sqlite.org/rtree.html
- SQLite Geopoly: https://www3.sqlite.org/geopoly/
- Electric shapes: https://electric.ax/docs/sync/guides/shapes
- PGlite: https://pglite.dev/
- PGlite filesystems and IndexedDB persistence: https://pglite.dev/docs/filesystems
- PGlite live queries: https://pglite.dev/docs/live-queries
- PGlite extensions: https://pglite.dev/extensions/
- Electric PGlite sync: https://electric.ax/sync/pglite
- PowerSync sync streams: https://docs.powersync.com/sync/streams/overview
- Turso/libSQL: https://docs.turso.tech/libsql
- Turso embedded replicas: https://docs.turso.tech/features/embedded-replicas/introduction
- Turso SDK embedded replica reference: https://docs.turso.tech/sdk/ts/reference
- Turso AI and embeddings: https://docs.turso.tech/features/ai-and-embeddings
- SpatiaLite: https://www.gaia-gis.it/fossil/libspatialite/index
- PostGIS: https://postgis.net/
- DuckDB full-text search: https://duckdb.org/docs/stable/core_extensions/full_text_search.html
- DuckDB vector similarity search: https://duckdb.org/docs/stable/core_extensions/vss.html
- LiteDB: https://www.litedb.org/docs/
- LiteDB SQL-like query syntax: https://www.litedb.org/api/query/
- H2: https://www.h2database.com/html/features.html
- Firebird Embedded server notes: https://github.com/FirebirdSQL/firebird/blob/master/doc/README.user.embedded
- Firebird features: https://www.firebirdsql.org/en/features/
