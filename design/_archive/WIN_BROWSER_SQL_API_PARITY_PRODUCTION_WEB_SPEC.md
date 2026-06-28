# Browser SQL/API Parity And Production Web Hardening

**Date:** 2026-05-27
**Status:** Implemented
**Future Version:** vNext
**Roadmap:** [`FUTURE_WINS.md`](../FUTURE_WINS.md)
**Document Type:** Implementation SPEC
**Audience:** Browser binding maintainers, wasm/VFS maintainers, SQL parser and
executor maintainers, sync maintainers, documentation authors, benchmark
maintainers, coding agents

**Governing ADRs:**

- [`adr/0161-browser-wasm-opfs-runtime.md`](../adr/0161-browser-wasm-opfs-runtime.md)
- [`adr/0165-production-browser-runtime-contract.md`](../adr/0165-production-browser-runtime-contract.md)

**Required follow-up ADRs before implementation:**

- Browser SQL parser parity strategy if a new parser dependency, AST pipeline, or
  parser architecture is introduced.
- Browser worker protocol versioning if the public request/response protocol
  gains backwards-compatible negotiation or streaming/cancellation semantics.
- Browser TDE/key material handling if browser open options expose encryption
  keys or passphrases.

**Implementation status, 2026-05-27:** Implemented. `@decentdb/web` now exposes
`browser-app-v2` parser/profile metadata, stable browser SQL errors, protocol
version and capability flags, transaction/savepoint helpers, prepared statement
reset/clear/page/async iteration, lifecycle guards for closed handles/imports,
browser sync apply-before-ack helpers, expanded OPFS diagnostics, framework
recipes, a checked-in SQL parity corpus, and browser benchmark guardrails.

**Related inputs:**

- [`FUTURE_WINS.md`](../FUTURE_WINS.md)
- [`docs/api/wasm.md`](../../docs/api/wasm.md)
- [`bindings/web/README.md`](../../bindings/web/README.md)
- [`tests/bindings/web/README.md`](../../tests/bindings/web/README.md)
- [`docs/user-guide/sync/relay.md`](../../docs/user-guide/sync/relay.md)
- [`docs/user-guide/sync/changesets.md`](../../docs/user-guide/sync/changesets.md)
- [`WIN_CROSS_PROCESS_WAL_COORDINATION_SPEC.md`](WIN_CROSS_PROCESS_WAL_COORDINATION_SPEC.md)
- [`WIN_FULL_TEXT_SEARCH_BM25_SPEC.md`](WIN_FULL_TEXT_SEARCH_BM25_SPEC.md)
- [`STABLE_TOOLING_METADATA_CONTRACT.md`](../STABLE_TOOLING_METADATA_CONTRACT.md)

---

## 1. Executive Summary

DecentDB already runs in the browser. The remaining adoption problem is not
"can DecentDB open an OPFS database?" It is whether a browser application team
can use DecentDB without constantly falling off a smaller SQL parser, a thinner
TypeScript API, unclear bundler setup, uncertain storage lifecycle behavior, or
unmeasured startup and memory costs.

This win makes the browser runtime feel like a production peer to the native
engine where that is technically safe. It closes the most visible gaps against
SQLite WASM and PGlite:

- a broader documented SQL profile;
- stable TypeScript APIs for common native workflows;
- predictable OPFS recovery, persistence, import/export, and quota behavior;
- real framework and bundler recipes;
- durable relay-sync apply/ack examples;
- browser benchmarks and release guardrails.

The goal is not to pretend browser storage is a native filesystem. The goal is
to make the browser contract explicit, tested, and complete enough for local-
first web apps that care about durable local data.

## 2. Product Goals

- Expand browser SQL coverage from the current `browser-app-v1` bootstrap
  subset to a documented application SQL profile that covers ordinary local-
  first app queries.
- Keep browser SQL unsupported cases explicit with stable errors and capability
  metadata.
- Provide TypeScript API parity for common native workflows: transactions,
  savepoints where supported, prepared statements, typed values, branch/snapshot
  workflows, export/import, metrics, sync helpers, and optional security open
  options.
- Preserve the Dedicated Worker owner model from ADR 0161 and ADR 0165.
- Harden OPFS lifecycle behavior: recovery, quota, persistence, import/export,
  checkpointing, stale owner recovery, and browser restart behavior.
- Provide production-ready recipes for Vite, Next.js, SvelteKit, Electron, and
  Tauri webviews.
- Provide relay-sync examples that apply data durably before acknowledging
  delivered changes.
- Add browser benchmark guardrails for startup, query latency, result decoding,
  memory growth, and package/build size.
- Promote browser tests and docs to release-blocking for claimed Tier 1
  browsers.

## 3. Non-Goals

- No service worker database ownership unless a later ADR changes the browser
  storage contract.
- No silent fallback to IndexedDB, localStorage, or in-memory storage for
  production durability claims.
- No browser claim that OPFS durability equals native filesystem power-loss
  durability.
- No arbitrary native extension loading in browser.
- No browser rewrite of the Rust engine into async internals.
- No SharedWorker ownership unless browser capability evidence supports the same
  OPFS sync access handle contract.
- No promise that every native SQL feature is available in the first parity
  release. The browser profile must be explicit and testable.
- No server-side sync bridge work. Browser relay helpers consume the existing
  production relay and public changeset APIs.

## 4. Current Context

Delivered browser foundations:

- `@decentdb/web` async TypeScript API.
- Dedicated Worker owner for each logical OPFS database path.
- OPFS sync access handles behind the browser VFS.
- Web Locks and BroadcastChannel owner discovery/routing.
- Stable browser probe and unsupported-environment errors.
- Binary and JSON result transports.
- Browser-only `sys.browser_runtime`, `sys.browser_owner`,
  `sys.browser_storage`, and `sys.browser_sync` views.
- `metrics()` with owner, storage, parser, sync, and memory samples.
- `checkpoint()`, `export()`, `import(bytes)`, and `persist()`.
- Owner-routed relay helper methods.
- Playwright OPFS smoke, multi-tab routing coverage, and transport benchmark.

Current limitations:

- Browser SQL uses the narrow `browser-app-v1` parser profile because native
  `pg_query` does not compile for `wasm32-unknown-unknown`.
- Browser API is thinner than native Rust/C/binding APIs for transactions,
  branches, snapshots, local security options, advanced sync, and operational
  workflows.
- Framework integration is documented at a lifecycle-note level, not as
  copy-paste production recipes.
- Browser performance guardrails exist for result transport but not for the full
  startup/query/memory/package-size lifecycle.
- OPFS quota, eviction, recovery, and import/export behavior is documented, but
  not yet backed by a broad matrix of failure and lifecycle tests.

## 5. Definition Of Done

This win is complete only when all of these are true:

- A browser SQL profile broader than `browser-app-v1` is implemented, named,
  documented, and exposed through `probeRuntime()`, `open()`, and
  `sys.browser_runtime`.
- The browser SQL profile has a parity corpus derived from native SQL tests,
  docs examples, and browser-specific app scenarios.
- TypeScript APIs cover the accepted browser parity surface and carry stable
  protocol version/capability metadata.
- OPFS recovery and lifecycle behavior is covered by deterministic browser
  tests.
- Framework/bundler recipes are documented and smoke-tested where practical.
- Browser relay sync examples demonstrate durable apply-before-ack behavior.
- Browser benchmark guardrails run in CI or release validation for claimed Tier
  1 browsers.
- User docs clearly state supported browsers, unsupported modes, storage
  durability boundaries, and recovery guidance.

## 6. SQL Parser And Profile Strategy

### 6.1 Parser Profiles

Browser SQL support must remain profile-based:

| Profile | Meaning |
|---|---|
| `browser-app-v1` | Current shipped bootstrap/smoke profile. |
| `browser-app-v2` | Target profile for this win. Broader application SQL but not every native-only feature. |
| `native` | Native `libpg_query` parser path. Not available in `wasm32-unknown-unknown` unless a future ADR proves otherwise. |

The active profile must be visible through:

- `probeRuntime()`;
- `open()` result metadata;
- `metrics()`;
- `sys.browser_runtime`.

### 6.2 Parser Architecture

V1 must not patch or fork `libpg_query` for wasm without an ADR. The current C
parser dependency is native-oriented and was intentionally excluded by ADR 0161.

The preferred direction is a wasm-compatible Rust SQL frontend that lowers into
DecentDB's existing `sql::ast::Statement` model. Acceptable implementation
paths require an ADR if they add a major dependency or change parser ownership:

- expand the existing `sql::wasm_minimal` parser into a maintained browser
  parser profile;
- adopt a pure-Rust parser dependency and normalize its AST into DecentDB's AST;
- generate a constrained grammar from DecentDB's accepted AST surface.

The chosen parser must not duplicate execution semantics. It only parses and
normalizes into the existing engine AST. The executor, planner, storage, WAL,
policies, masks, indexes, and type system remain authoritative.

### 6.3 Required Browser SQL Coverage

`browser-app-v2` should cover these native DecentDB SQL surfaces unless a
specific item is deferred with a documented reason:

- `CREATE TABLE` with ordinary column types, primary keys, `NOT NULL`, `UNIQUE`,
  `CHECK`, generated defaults where already native, and foreign keys.
- `CREATE INDEX` for B+Tree, trigram, full-text, and spatial indexes when the
  underlying runtime feature is available in wasm.
- `DROP TABLE`, `DROP INDEX`, `ALTER TABLE` add/rename/drop column where native
  support exists.
- `INSERT ... VALUES`, multi-row values, parameterized inserts, `RETURNING`, and
  native upsert syntax.
- `INSERT ... SELECT` for migration/import workflows.
- `UPDATE`, `DELETE`, scalar expressions, `RETURNING`, and ordinary predicates.
- `SELECT` projections, aliases, expressions, scalar functions, `WHERE`,
  `ORDER BY`, `LIMIT`, `OFFSET`, `DISTINCT`, aggregates, `GROUP BY`, and
  `HAVING`.
- Joins: `INNER`, `LEFT`, `RIGHT`, `FULL OUTER`, `CROSS`, and `NATURAL` if the
  native executor supports the shape.
- CTEs, including recursive CTEs if the native executor path supports the tested
  shape.
- Subqueries: `FROM` subqueries, `EXISTS`, scalar subqueries, `IN` subqueries,
  and comparison subqueries where native supported.
- Set operations supported by native DecentDB.
- `PRAGMA` compatibility commands that are safe in browser.
- `sys.*` operational views, including browser-only views and native-compatible
  views that make sense in wasm.
- Native type literals and casts for DECIMAL, UUID, DATE/TIME/TIMESTAMP,
  INTERVAL, JSON, spatial values, and browser-supported binary values.
- `fulltext_match` and `bm25` for full-text search if FTS is compiled into the
  wasm build.

### 6.4 Explicit Deferrals

These may remain unsupported in `browser-app-v2` if documented and covered by
stable errors:

- Lua extension packages in browser, unless the Lua runtime and package trust
  model are separately approved for wasm.
- Native extension loading.
- Service-worker-owned SQL execution.
- SQL features whose native path depends on platform APIs unavailable in wasm.
- SQL syntax accepted by PostgreSQL but intentionally unsupported by DecentDB
  native.

### 6.5 Error Contract

Unsupported browser SQL must produce stable browser errors:

- `ERR_BROWSER_SQL_UNSUPPORTED` for profile-supported parser gaps.
- `ERR_BROWSER_SQL_PARSE` for invalid SQL.
- `ERR_BROWSER_SQL_PROFILE_MISMATCH` when a query requires a profile newer than
  the current runtime.

Errors must include:

- parser profile;
- a short unsupported feature label when known;
- original engine error details where safe;
- no parameter values unless explicitly requested by a debug option.

## 7. API Parity Target

### 7.1 Database API

The browser `Database` API should cover:

- `exec(sql, params?)`;
- `query(sql, params?)`;
- `prepare(sql)`;
- `beginTransaction()`;
- `commitTransaction()`;
- `rollbackTransaction()`;
- `transaction(callback, options?)`;
- `savepoint(name?)`, `releaseSavepoint(name)`, and `rollbackToSavepoint(name)`
  if native savepoint semantics are exposed through wasm;
- `checkpoint()`;
- `export()`;
- `import(bytes)`;
- `persist()`;
- `metrics()`;
- `close()`;
- branch/snapshot APIs matching maintained binding naming where practical;
- sync relay helpers;
- explicit capability inspection.

The callback transaction helper must serialize work through the owner worker and
must roll back if the callback throws or rejects.

### 7.2 Prepared Statement API

The current `Statement` API exposes `bind`, `step`, and `close`. This win should
add or explicitly reject:

- async iteration over statement rows;
- `reset()` / `clearBindings()` behavior;
- row paging for large result sets;
- automatic `close()` via `using` / `Symbol.dispose` where TypeScript runtime
  support permits;
- stable errors for using a closed statement or a statement from a closed
  database.

Large result behavior must not require materializing unbounded rows on the main
thread.

### 7.3 Typed Values

Browser parameter and result transport must round-trip:

- `NULL`, `BOOL`, `INT64`, `FLOAT64`, `TEXT`, `BLOB`;
- `DECIMAL`;
- `UUID`;
- `DATE`, `TIME`, `TIMESTAMP`, `TIMESTAMPTZ`, `INTERVAL`;
- JSON text values and JSON scalar results;
- spatial values in the documented browser representation if spatial is enabled.

Because JavaScript `number` cannot represent all `INT64` values safely, browser
docs must define when values return as `number`, `bigint`, or tagged values.
Lossy conversion must not be the default for exact integer/decimal paths.

### 7.4 Worker Protocol Versioning

The worker RPC protocol should expose:

- protocol version;
- engine version;
- parser profile;
- result transport support;
- API capability flags;
- deprecation notes when a client uses an older protocol.

If this requires backwards-compatible negotiation, create a worker protocol ADR
before implementation.

### 7.5 Cancellation And Timeouts

Browser APIs should support `AbortSignal` or an equivalent cancellation policy
for long-running queries where the engine can safely interrupt. If the engine
cannot interrupt a running synchronous wasm operation, the API must document that
cancellation is cooperative and applies before dispatch or between operations.

Open and owner-discovery timeouts already exist and should remain distinct from
query cancellation.

## 8. OPFS Lifecycle And Durability

### 8.1 File Layout

Browser OPFS uses logical database paths and engine sidecars. The docs must
state which sidecars can exist in OPFS, including WAL and any wasm-supported
index sidecars.

Native `.coord` sidecars do not apply to browser OPFS. Browser owner routing is
the coordination mechanism and remains distinct from native cross-process WAL
coordination.

### 8.2 Open, Close, And Owner Recovery

Tests must cover:

- first open;
- reopen after clean close;
- reopen after owner tab closes;
- reopen after owner tab disappears without clean close;
- non-owner tab request routing;
- owner timeout and retry;
- stale owner recovery without two owners for one logical path;
- close while statements are active;
- close while a sync stream is active.

### 8.3 Import And Export

`export()` must checkpoint before producing bytes and must document whether WAL
or sidecars are included. V1 should export a checkpointed database image without
live WAL bytes.

`import(bytes)` must:

- close existing engine handles safely;
- replace the OPFS database image atomically from the browser API perspective;
- clear stale WAL/sidecar files for that logical path;
- reopen or leave closed according to documented API behavior;
- reject imports while statements, transactions, or sync streams are active
  unless the API explicitly closes them first.

### 8.4 Quota And Persistence

`persist()` must expose whether persistent storage was granted. It must not imply
guaranteed retention.

`metrics()` and `sys.browser_storage` should report:

- quota estimate when available;
- usage estimate when available;
- persistent-storage grant status;
- OPFS support flags;
- last export/import/checkpoint timestamps if cheaply available;
- storage pressure warnings if the browser exposes enough signal.

Docs should include quota-pressure guidance for applications with important
offline data: sync, export, or both.

### 8.5 Crash And Recovery Tests

Browser tests should simulate or approximate:

- owner worker termination after committed writes;
- owner worker termination during a write batch where possible;
- reload with non-empty WAL;
- import interrupted before reopen where practical;
- quota failure during write/import if Playwright/browser APIs can induce it;
- recovery after `pagehide`/BFCache-like lifecycle where testable.

## 9. Sync Relay Browser Contract

Browser sync helpers are owner-routed and use the production relay. This win
should add examples and tests for the safe browser pattern:

1. configure relay peer with explicit tenant/subject/shape context;
2. pull or receive changeset;
3. apply changeset in a local transaction;
4. commit durably;
5. acknowledge the relay only after local commit succeeds;
6. surface conflicts or rejected records through existing sync diagnostics.

The browser API should not acknowledge a relay message before durable local
apply unless the application explicitly chooses an unsafe mode with documented
data-loss semantics.

WebSocket subscriptions must keep the existing browser constraint: custom
headers are unavailable, so short-lived principal context is passed through a
TLS-protected stream URL or a relay-approved token mechanism.

## 10. Security And Privacy

### 10.1 TDE In Browser

Local data security v1 exists in native code. Browser parity should decide
whether `@decentdb/web` exposes encryption open options in this win.

If browser TDE options are included:

- key material must be passed explicitly by the application;
- key material must not be persisted by DecentDB browser runtime;
- diagnostics must not expose keys or passphrases;
- examples must use Web Crypto derivation or application-managed key retrieval
  without implying a turnkey key store;
- platform/browser key-store helpers remain part of the later security roadmap.

If browser TDE is deferred, docs must say browser-local OPFS data is protected
only by browser/profile/OS storage boundaries and application sync/export
strategy.

### 10.2 Diagnostics Redaction

Browser errors, metrics, and system views must avoid leaking:

- parameter values by default;
- relay tokens;
- authorization headers;
- encryption key material;
- full OPFS paths beyond logical database names where path exposure is not
  needed.

## 11. Packaging And Framework Integration

### 11.1 Package Shape

The npm package should make the common path simple:

- TypeScript types are published.
- Worker and wasm asset loading are documented.
- ESM import works in modern bundlers.
- The package can be used without Node.js native bindings.
- Browser-only code does not break server-side rendering imports.

### 11.2 Required Recipes

Ship docs and, where practical, smoke examples for:

- Vite;
- Next.js app router and pages router boundaries;
- SvelteKit;
- Electron renderer with OPFS support caveats;
- Tauri webview with OPFS support caveats;
- plain static ESM.

Each recipe must state:

- where the worker file is served from;
- where the wasm-bindgen JavaScript and wasm assets are served from;
- how to avoid SSR execution;
- how to open one app-level database handle;
- how to close on page/application teardown;
- which browsers/webviews are supported.

### 11.3 Bundle Size And Startup

The spec does not require DecentDB to be the smallest wasm database package, but
package size must be measured and documented. Release notes should report:

- wasm binary size;
- generated JS size;
- worker bundle size;
- cold open time in the browser benchmark;
- first query time after open.

## 12. Browser Benchmarks

### 12.1 Required Benchmark Scenarios

Benchmarks should run in a real browser with OPFS:

- cold worker startup plus open;
- warm reopen;
- first simple query;
- prepared point lookup loop;
- insert transaction batch;
- large result decode with binary transport;
- large result decode with JSON transport for comparison;
- aggregate query on a medium table;
- full-text query if FTS is available in wasm;
- export/import of a representative database;
- memory growth across repeated query cycles.

### 12.2 Guardrails

Phase 0 must capture baseline numbers before large changes. Later phases must
fail or require explicit review when:

- startup/open p95 regresses by more than 10% from the accepted baseline;
- large-result binary transport regresses by more than 10%;
- binary result transport is not materially faster than JSON on the large-result
  benchmark;
- WASM linear memory grows without returning to a stable envelope across repeat
  cycles;
- package size grows by more than the threshold set in Phase 0 without a release
  note and approval.

Thresholds may be adjusted by ADR or spec amendment after baseline data exists.
They must not remain vague before implementation starts.

## 13. Testing Strategy

### 13.1 SQL Parity Corpus

Build a browser SQL corpus from:

- native parser unit tests;
- native executor query tests;
- docs examples;
- binding smoke examples;
- sync relay examples;
- FTS examples;
- branch/snapshot examples if browser branch APIs are in scope.

Every corpus query must be classified:

- `supported`;
- `unsupported_by_browser_profile`;
- `unsupported_by_native_engine`;
- `deferred_requires_ADR`.

Unsupported cases need stable expected errors.

### 13.2 Rust/WASM Tests

- Unit-test parser lowering in native Rust where possible.
- Compile-check wasm target in CI.
- Add wasm-bindgen smoke tests for direct exports where practical.
- Keep browser-only behavior out of native hot paths.

### 13.3 Playwright Tests

Release-blocking Tier 1 browser tests should cover:

- capability probe;
- open/create/reopen;
- multi-tab owner routing;
- SQL profile representative queries;
- prepared statements;
- transactions;
- import/export;
- persistence request;
- metrics and browser system views;
- owner loss/recovery;
- relay apply/ack example with a local test relay or mocked relay.

Candidate browser tests should run separately until promoted.

## 14. Documentation Requirements

Update:

- `docs/api/wasm.md`;
- `bindings/web/README.md`;
- `tests/bindings/web/README.md`;
- `README.md` feature text if browser capability changes materially;
- `docs/user-guide/sync/relay.md` for browser relay examples;
- `docs/about/changelog.md`;
- package README and examples.

Docs must include:

- supported browser matrix;
- unsupported environment explanations;
- SQL profile table;
- framework recipes;
- OPFS durability and quota guidance;
- backup/export and restore/import guidance;
- sync apply-before-ack pattern;
- troubleshooting table with stable browser error codes.

## 15. Phased Implementation Plan

### Phase 0: Spec, Inventory, Baselines

- Land this spec.
- Fix `FUTURE_WINS.md` source-of-truth links and stale section numbering.
- Build SQL parity corpus and classify coverage.
- Record browser benchmark baselines for current `browser-app-v1`.
- Decide which follow-up ADRs are required.

### Phase 1: Parser Strategy And SQL Profile

- Implement or adopt the wasm-compatible parser strategy approved by ADR.
- Expose `browser-app-v2` profile metadata.
- Add SQL parser/lowering tests.
- Add stable unsupported SQL error codes.
- Document the supported SQL profile.

### Phase 2: API Parity And Worker Protocol

- Add accepted transaction, prepared statement, branch/snapshot, security, and
  capability APIs.
- Add worker protocol version/capability metadata.
- Add closed-handle and active-statement lifecycle errors.
- Add typed value roundtrip tests.

### Phase 3: OPFS Lifecycle Hardening

- Harden open/close/reopen/stale-owner flows.
- Harden import/export atomicity.
- Add recovery tests for non-empty WAL and owner loss.
- Expand `sys.browser_storage` and `metrics()` where needed.

### Phase 4: Sync Relay Browser Examples

- Add durable apply-before-ack helper or example.
- Add Playwright or integration coverage with a local relay/mock relay.
- Document browser sync deployment requirements.

### Phase 5: Packaging And Framework Recipes

- Add Vite, Next.js, SvelteKit, Electron, Tauri, and static ESM recipes.
- Verify SSR-safe import patterns.
- Add package asset-loading troubleshooting.

### Phase 6: Benchmarks And Release Guardrails

- Promote benchmark guardrails into pre-release checks.
- Promote Tier 1 browser smoke matrix.
- Update docs and changelog.
- Run full pre-commit and browser validation.

## 16. Acceptance Criteria

- `browser-app-v2` parser profile exists and is documented.
- SQL parity corpus is checked in and classified.
- Browser supports the accepted app SQL profile in real OPFS browser tests.
- TypeScript APIs cover accepted parity workflows with stable types.
- Worker protocol reports version and capabilities.
- OPFS owner recovery and import/export behavior are tested.
- Relay sync browser example applies before acking.
- Framework recipes are documented.
- Browser benchmarks have baselines and guardrails.
- No unsupported browser environment silently falls back to weaker storage.
- Service workers remain explicitly unsupported for database ownership.
- `docs/about/changelog.md` is updated.
- Full pre-commit and browser smoke/bench validation pass.

## 17. Risks

| Risk | Mitigation |
|---|---|
| Parser project grows into a second SQL engine | Lower into existing DecentDB AST and keep executor semantics authoritative. |
| Browser SQL profile overpromises native parity | Publish profile metadata and classify every parity corpus query. |
| WASM package size grows too much | Track package size in benchmark/release output. |
| OPFS behavior differs across browsers | Capability-gated support tiers and real browser tests. |
| Sync ack happens before durable local apply | Provide safe helper/example and test apply-before-ack behavior. |
| Service worker demand pressures unsafe ownership | Keep unsupported unless a new ADR proves a safe storage/ownership model. |
| Security docs imply browser key storage DecentDB does not provide | Separate explicit browser TDE options from later key-store helper roadmap. |

## 18. Implementation Decisions

1. `browser-app-v2` expands the existing in-repo wasm parser. No parser
   dependency or native `pg_query` wasm port was added, so no parser ADR was
   required.
2. Browser TDE open options are deferred. Capability metadata reports
   `browserTdeOpenOptions: false`; browser-local OPFS data remains protected by
   browser/profile/OS storage boundaries plus application sync/export strategy.
3. Browser branch/snapshot workflows are deferred. Capability metadata reports
   `branchSnapshots: false` and the TypeScript API fails explicitly with
   `ERR_BROWSER_BRANCH_UNSUPPORTED`.
4. Query cancellation is not exposed because wasm execution cannot preempt a
   synchronous engine call safely. Capability metadata reports
   `cooperativeCancellation: false`.
5. Benchmark guardrails start with broad browser-safe thresholds in the
   Playwright benchmark and should be tightened from accepted CI baselines per
   release.
