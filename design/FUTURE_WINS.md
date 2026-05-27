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
| 1 | vNext | TODO | Mobile production runtime and SDK hardening | [`WIN_MOBILE_PRODUCTION_RUNTIME_SDK_HARDENING_SPEC.md`](WIN_MOBILE_PRODUCTION_RUNTIME_SDK_HARDENING_SPEC.md) | Local-first without first-class iOS/Android lifecycle, packaging, key storage, and background sync guidance leaves a major adoption gap |
| 2 | vNext | TODO | Default-fast performance and storage efficiency | Benchmarking guide and release metrics; needs ADR/spec for format-affecting work | DecentDB should feel fast without special tuning and should keep file size/cold-open behavior competitive |
| 3 | vNext | TODO | Rich structured errors and developer diagnostics | Existing [`docs/api/error-codes.md`](../docs/api/error-codes.md); needs ADR/spec for stable machine-readable expansion | Low-cost adoption win: better errors, hints, and doc links reduce integration friction across every binding |
| 4 | vNext+1 | TODO | Incremental backup and point-in-time recovery | Basic `save_as` backup is delivered; needs ADR/spec for WAL archive/PITR semantics | Durable recovery artifacts are a production requirement and distinct from live replication |
| 5 | vNext+1 | TODO | Runtime tracing, advisors, and Doctor integration | Needs ADR/spec; follows shipped operational metrics | Explains slow queries, lock waits, index usage, schema lint, and maintenance issues once the metrics contract is stable |
| 6 | vNext+1 | TODO | Branch-aware migration rehearsal and promotion | ADR 0153-0159 and branch CLI/API docs; needs ADR/spec | Uses shipped branch/diff foundations for a distinctive safe migration workflow |
| 7 | vNext+1 | TODO | Online schema change execution | Needs ADR/spec; follows branch migration design | Completes the migration story by reducing reader/write disruption during large table rebuilds and index work |
| 8 | vNext+1 | TODO | Backend sync bridge for existing app databases, Postgres first | Needs ADR/spec | Makes DecentDB easier to adopt in apps that already have a central Postgres/Supabase-style backend |
| 9 | vNext+1 | TODO | Resource governance, quotas, and automated maintenance | Needs ADR/spec; follows browser/mobile/runtime diagnostics | Embedded hosts need explicit storage, WAL, memory, quota, and maintenance behavior to avoid runaway local resource use |
| 10 | Later | BACKLOG | Incrementally maintained projections | Needs ADR/spec | Accelerates dashboards, local read models, and reactive query workloads |
| 11 | Later | BACKLOG | JSONB binary storage and JSON path indexing | Needs ADR/spec | Important for JSON-heavy workloads and now a SQLite baseline expectation |
| 12 | Later | BACKLOG | Hybrid local search: FTS, trigram, vector, and rank fusion | FTS foundation is delivered; vector and rank fusion need ADR/spec | More compelling than standalone HNSW: apps want keyword, substring, semantic, and relational filters together |
| 13 | Later | BACKLOG | Authenticated encryption, key rotation, and platform key-store helpers | ADR 0174 follow-up | TDE v1 provides local confidentiality; regulated deployments eventually need tamper-evident page/chunk authentication, key rotation, and turnkey OS/browser/mobile key-store guidance |
| 14 | Later | BACKLOG | Agent and tooling integration mode | [`STABLE_TOOLING_METADATA_CONTRACT.md`](STABLE_TOOLING_METADATA_CONTRACT.md); needs ADR/spec | Makes the agent-friendly promise concrete without putting LLM behavior in the engine |
| 15 | Later | BACKLOG | Reliability validation, fault injection, and deterministic replay | [`TESTING_STRATEGY.md`](TESTING_STRATEGY.md); needs ADR/spec for replay capture | Raises confidence in ACID/concurrency changes and makes production bugs reproducible without weakening hot paths |
| 16 | Later | BACKLOG | Application and support bundle format | Needs ADR/spec | Useful portable artifact and diagnostics story, but should follow security/redaction foundations |
| 17 | Later | BACKLOG | Temporal row history and auditable state | Needs ADR/spec | Strong regulated/support workflow, but should follow security, audit context, and sync hardening |
| 18 | Later | BACKLOG | Structured CDC and logical change feeds | Change streams, public changesets, and sync journal are delivered; needs ADR/spec | Lets DecentDB feed event-driven systems without becoming a message broker or bypassing local transactions |
| 19 | Later | BACKLOG | Curated Lua extension ecosystem | Lua runtime/package model is delivered; needs ADR/spec outside core engine if registry semantics affect trust | Turns safe extensibility into an adoption moat while preserving the no-native-extension stance |
| 20 | Later | BACKLOG | Unicode collation and internationalization profile | Query-time built-in and Lua collations are delivered; needs ADR/spec for ICU/data-size strategy | International apps need correct Unicode sort/search semantics, but portability and binary size make it a later tradeoff |
| 21 | Later | BACKLOG | Advanced SQL compatibility surface | [`WIN_ADVANCED_SQL_COMPATIBILITY_SURFACE.md`](WIN_ADVANCED_SQL_COMPATIBILITY_SURFACE.md) | Useful adoption polish after higher-impact runtime, recovery, migration, and workflow blockers |
| 22 | Later | BACKLOG | Advanced geospatial semantics and analytics | ADR 0128 deferred work; needs follow-up ADR/spec | Builds on shipped spatial support without implying the foundation is unfinished |
| 23 | Later | BACKLOG | WAL streaming replication | Needs ADR/spec | Useful HA/read-scale story, but weaker than local-first sync and PITR for DecentDB identity |
| 24 | Later | BACKLOG | Cloud-native object storage VFS and WASI edge profiles | Needs ADR/spec | Interesting edge/serverless story with high durability, consistency, packaging, and cache-invalidation complexity |

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

## 1. Mobile Production Runtime And SDK Hardening

**Status:** `TODO`

**Future Version:** vNext

**Source of truth:**
[`WIN_MOBILE_PRODUCTION_RUNTIME_SDK_HARDENING_SPEC.md`](WIN_MOBILE_PRODUCTION_RUNTIME_SDK_HARDENING_SPEC.md).

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

## 2. Default-Fast Performance And Storage Efficiency

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
- evaluate page-level compression, key-prefix compression, and layout changes
  only when benchmark profiles justify the format/recovery complexity
- teach the planner/executor to exploit covering indexes where metadata already
  exists
- keep `ANALYZE` and stats useful without turning tuning into required ritual
- improve plan caching and adaptive statistics where they preserve predictable
  behavior under prepared-statement workloads
- improve binding prepared-statement hot paths where measurements show overhead
- add memory-bounded streaming result APIs where large binding/WASM result
  materialization shows real host pressure
- add performance Doctor findings only after the runtime can explain them

### Guardrails

- do not weaken durable defaults to win charts
- do not change persistent formats without ADR and migration coverage
- benchmark both native and maintained binding paths when a change targets
  application-facing latency
- keep profile names explicit so tuned and default results are not conflated

## 3. Rich Structured Errors And Developer Diagnostics

**Status:** `TODO`

**Future Version:** vNext

**Source of truth:** Existing [`docs/api/error-codes.md`](../docs/api/error-codes.md)
defines broad error categories; needs ADR/spec before expanding the stable
machine-readable contract.

### Why This Matters

DecentDB already has error categories such as `ERR_SQL`, `ERR_CONSTRAINT`,
`ERR_IO`, queue timeout/cancel codes, and JSON error output in CLI paths. The
next win is making failures actionable across bindings and tools. Cryptic SQL,
constraint, lock, coordination, policy, sync, and format errors create adoption
friction even when the engine is behaving correctly.

### Desired Capability

- stable subcodes and optional SQLSTATE-compatible mappings where useful
- structured fields for relation, column, index, constraint, policy, branch,
  sync scope, process owner, and WAL/format context
- safe remediation hints and documentation anchors for common errors
- machine-readable retryability and permanence classification
- binding-consistent error projection through Rust, C ABI, Python, Go, Node,
  .NET, Java, Dart, CLI, and WASM surfaces
- redaction rules for SQL text, parameters, paths, and audit context
- Doctor handoff for errors that need deeper inspection

### Guardrails

- do not make human-readable message text the stable programmatic contract
- do not leak sensitive values in hints, context fields, or doc links
- keep added context cheap unless diagnostics are explicitly enabled
- preserve existing broad error categories for compatibility

## 4. Incremental Backup And Point-In-Time Recovery

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

## 5. Runtime Tracing, Advisors, And Doctor Integration

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

## 6. Branch-Aware Migration Rehearsal And Promotion

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

## 7. Online Schema Change Execution

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

## 8. Backend Sync Bridge For Existing App Databases, Postgres First

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

## 9. Resource Governance, Quotas, And Automated Maintenance

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

## 10. Incrementally Maintained Projections

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

## 11. JSONB Binary Storage And JSON Path Indexing

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

## 12. Hybrid Local Search: FTS, Trigram, Vector, And Rank Fusion

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

## 13. Authenticated Encryption, Key Rotation, And Platform Key-Store Helpers

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

## 14. Agent And Tooling Integration Mode

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

## 15. Reliability Validation, Fault Injection, And Deterministic Replay

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

## 16. Application And Support Bundle Format

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

## 17. Temporal Row History And Auditable State

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

## 18. Structured CDC And Logical Change Feeds

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

## 19. Curated Lua Extension Ecosystem

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

## 20. Unicode Collation And Internationalization Profile

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

## 21. Advanced SQL Compatibility Surface

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

## 22. Advanced Geospatial Semantics And Analytics

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

## 23. WAL Streaming Replication

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

## 24. Cloud-Native Object Storage VFS And WASI Edge Profiles

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

- rich structured errors and developer diagnostics
- incremental backup and point-in-time recovery
- online schema change execution
- resource governance, quotas, and automated maintenance
- reliability validation, fault injection, and deterministic replay
- structured CDC and logical change feeds
- curated Lua extension ecosystem
- Unicode collation and internationalization profile

Folded into existing tracks instead of duplicated:

- page/key/layout compression, streaming result sets, plan caching, and adaptive
  statistics belong under default-fast performance and storage efficiency
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

1. Scope mobile hardening into explicit support tiers and tests. This is
   adoption work, not just binding niceties.
2. Use release benchmark profiles to drive default-fast performance and storage
   efficiency work, including storage-layout ideas such as page/key compression
   only when measurements justify the format risk.
3. Tighten structured errors and developer diagnostics early because they
   improve every binding and make later runtime work easier to consume.
4. Scope incremental backup/PITR separately from WAL streaming replication; the
   basic online backup API exists, but production recovery semantics need their
   own ADR.
5. Extend shipped `sys.*` metrics into opt-in tracing, advisors, and Doctor
   integration once the hot-path overhead budget is explicit.
6. Design branch-aware migration rehearsal and online schema execution together
   so validation and production promotion do not diverge.
7. Treat resource governance as part of browser/mobile readiness, not as an
   optional tuning-only feature.
8. Promote authenticated encryption/key-rotation work only after the v1 TDE and
   policy surfaces have production feedback and a follow-up ADR.
9. Promote backlog items into TODO only after the top adoption blockers have
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
