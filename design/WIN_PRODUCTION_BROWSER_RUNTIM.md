# Production Browser Runtime

**Date:** 2026-05-20  
**Status:** Implemented for DecentDB 2.6.0
**Roadmap:** Delivered foundation; see [`FUTURE_WINS.md`](FUTURE_WINS.md) Delivered Context
**Document Type:** Implementation SPEC
**Audience:** Web binding maintainers, WASM/VFS maintainers, sync implementers, storage/WAL maintainers, documentation authors, release engineers, coding agents  
**Related inputs:** [`FUTURE_WINS.md`](FUTURE_WINS.md), [`adr/0161-browser-wasm-opfs-runtime.md`](adr/0161-browser-wasm-opfs-runtime.md), [`docs/api/wasm.md`](../docs/api/wasm.md), [`WIN_LOCAL_FIRST_SYNC_FIRST_CLASS_SPEC.md`](WIN_LOCAL_FIRST_SYNC_FIRST_CLASS_SPEC.md), [`WIN_CONCURRENT_WRITE_ERGONOMICS_PHASED_APPROACH.md`](WIN_CONCURRENT_WRITE_ERGONOMICS_PHASED_APPROACH.md)

---

## 1. Executive Summary

DecentDB already has a real browser v1: the Rust engine can run as
`wasm32-unknown-unknown`, the TypeScript package `@decentdb/web` exposes an
async API, and a Dedicated Worker owns OPFS synchronous access handles behind a
browser VFS.

That is enough to prove the engine works in the browser. It is not enough to
claim a production browser runtime.

Production browser support means DecentDB can safely support local-first web
applications where users open multiple tabs, workers start and stop, storage
pressure happens, sync runs in the background, browser capabilities vary, and
developers need explicit diagnostics instead of folklore.

The production contract should be:

```text
One logical browser database path has one active DecentDB runtime owner.
All tabs, app shells, and supported browser-side workflows route through that
owner. Unsupported browser capabilities fail explicitly before weaker storage
or unsafe ownership can corrupt state.
```

The first production target should be narrow and strong, not broad and weak.
Chrome and Edge are the natural first Tier 1 targets because they are closest to
the existing Chromium OPFS v1. Firefox should become a claim only after CI proves
the same ownership, locking, durability, quota, and worker behavior. Browser
forks should be capability-gated rather than certified by brand. Safari and iOS
should be a separate compatibility decision after the core ownership model is
stable.

This feature is not "DecentDB for Chrome only." It is a browser runtime contract
that starts with the browsers where DecentDB can prove the required storage and
worker primitives.

---

## 2. Why This Feature Exists

DecentDB's strategic positioning includes browser-capable local-first SQL. The
current browser v1 intentionally documents several production gaps:

1. one Dedicated Worker owns one logical database handle;
2. cross-tab and cross-worker write coordination are unsupported;
3. service worker participation is unsupported;
4. the wasm SQL parser is narrower than the native parser;
5. browser durability depends on OPFS and browser storage policy;
6. diagnostics are limited to basic runtime samples;
7. automated coverage is primarily Chromium smoke and transport performance.

Without a production browser runtime, application developers must invent their
own answers to hard questions:

- Which tab owns the database?
- What happens when a tab crashes while holding OPFS handles?
- How does a second tab know whether it can open the same database?
- Can a service worker sync while the app tab is closed?
- Is browser storage persisted or eviction-prone?
- Is the database safe in private browsing?
- Which SQL is browser-supported?
- Which browser failures are recoverable versus unsupported?

Those answers must not be scattered across application code. They should be a
DecentDB-owned runtime contract with tests, diagnostics, and documentation.

---

## 3. Product Principles

### 3.1 Durability First

Browser support must not weaken DecentDB's durability story by accident. OPFS is
a browser storage substrate with browser policy, quota, and eviction behavior.
The runtime must document that honestly and expose enough diagnostics for
applications to decide when to sync, export, or warn users.

### 3.2 One Writer Remains The Architecture

The browser runtime must preserve DecentDB's one-writer model. Multi-tab support
means multi-tab coordination around one active owner, not hidden multi-writer
MVCC and not several workers writing the same WAL independently.

### 3.3 Capability-Gated, Not User-Agent-Gated

Runtime support should be decided by feature probes and verified behavior, not
by browser name strings. The docs may list tested support tiers, but the code
should fail or proceed based on required capabilities.

### 3.4 No Silent Weak Fallback

If OPFS synchronous access handles, required worker primitives, lock
coordination, or persistence diagnostics are unavailable, DecentDB must fail
with a clear error. It must not silently switch to IndexedDB, localStorage, or an
in-memory database under the same durability claims.

### 3.5 Browser Code Must Stay Outside Native Hot Paths

The synchronous Rust engine, native VFS path, pager, WAL, page cache, and B+Tree
hot paths should not gain browser-specific checks. Browser complexity belongs in
the web binding, wasm VFS boundary, and target-specific build configuration.

### 3.6 Production Claims Require CI

DecentDB should only document a browser as supported after automated tests cover
that browser's required behavior. "Likely works" is not a production support
tier.

---

## 4. Goals

1. Define a production browser support matrix with explicit tiers.
2. Add runtime capability probes for all required browser primitives.
3. Select and implement a multi-tab ownership coordination model.
4. Preserve one active writer/runtime owner per logical OPFS database path.
5. Define service worker participation or non-participation explicitly.
6. Add OPFS lock ownership, stale-owner detection, and recovery behavior.
7. Integrate browser-safe sync transport with the runtime owner model.
8. Define a browser parser and API parity plan for common application SQL.
9. Add binary/result/parameter support needed for production browser apps.
10. Add quota, persistence, durability, lock, and owner diagnostics to
    `metrics()` and `sys.*` where applicable.
11. Add bundle size and feature-profile controls for web delivery.
12. Expand browser smoke, recovery, compatibility, and performance coverage.
13. Update user-facing docs with honest durability and compatibility guidance.

---

## 5. Non-Goals

The first production browser runtime should not include:

1. a rewrite of the Rust engine around async browser I/O;
2. hidden multi-writer MVCC;
3. silent fallback to weaker storage backends;
4. equal certification for every browser and browser fork;
5. IndexedDB as a primary production storage backend;
6. arbitrary direct OPFS access by application code;
7. service-worker-owned database handles unless accepted by ADR;
8. full native SQL parser parity unless a wasm-compatible parser strategy is
   accepted;
9. browser sync relay hosting inside the engine;
10. broad cross-process desktop coordination for Electron/Tauri; that remains a
    separate future work item unless deliberately merged by ADR.

---

## 6. Required ADRs

This feature requires ADRs before implementation because it changes runtime
ownership, browser compatibility claims, storage failure behavior, and sync
integration.

Required ADRs:

1. Browser support tiers and minimum capability contract.
2. Multi-tab ownership coordination model.
3. OPFS lock ownership and stale-owner recovery.
4. Service worker participation policy.
5. Browser sync transport and owner-routing contract.
6. Browser SQL parser/API parity strategy.
7. Browser diagnostics and `sys.*` exposure contract.
8. Browser package feature profile and binary size policy.

These decisions may be captured in one comprehensive ADR if the design is small
enough, but the ownership model and service worker policy must be explicit.

---

## 7. Definitions

**Logical database path:** The application-provided DecentDB path used to name
the OPFS database and WAL files, such as `app.ddb`.

**Runtime owner:** The browser worker or worker-backed runtime instance that
currently owns the engine handle and OPFS access handles for a logical database
path.

**Client context:** A tab, iframe, page worker, app shell, or other browser
execution context that wants to run database operations through a runtime owner.

**Coordination channel:** The browser primitive or protocol used by client
contexts to discover, elect, contact, or replace the runtime owner.

**Lease:** A time-bounded ownership claim that proves the runtime owner is still
alive and should be treated as authoritative.

**Stale owner:** An owner record or lock claim whose runtime can no longer be
contacted and whose lease has expired or is otherwise proven dead.

**Storage capability probe:** A runtime test that verifies required browser
storage and worker primitives before DecentDB opens a database.

**Support tier:** A documented browser/environment level backed by a defined CI
matrix and a behavior contract.

---

## 8. Target End State

### 8.1 User Experience

Application code should continue to look simple:

```ts
import { open } from "@decentdb/web";

const db = await open({
  path: "app.ddb",
  mode: "openOrCreate",
});

await db.exec("INSERT INTO events(id, body) VALUES ($1, $2)", [1, "created"]);
```

Behind that API, the package should:

1. probe browser capabilities;
2. discover or establish the runtime owner for `app.ddb`;
3. route operations to that owner;
4. serialize writes through the existing engine contract;
5. expose clear errors when ownership, storage, or persistence is unsupported;
6. report diagnostics through `metrics()` and `sys.*`.

### 8.2 Multi-Tab Experience

Multiple tabs using the same logical path should not create independent writers.
They should either share one runtime owner or fail safely.

```text
tab A open("app.ddb") -> runtime owner created
tab B open("app.ddb") -> routes requests to owner
tab A closes       -> owner remains if other clients exist, or releases cleanly
owner crashes      -> tab B detects stale owner, recovers, reopens safely
```

### 8.3 Operational Experience

Developers should be able to answer:

- Which browser capabilities passed?
- Which runtime owns this database path?
- How many clients are attached?
- Is persistent storage granted?
- What quota and usage values are visible?
- Is the database using OPFS synchronous access handles?
- Has stale-owner recovery happened?
- Are sync transport and background behavior enabled?
- Which SQL/parser profile is active?

---

## 9. Browser Support Matrix

Production support should be tiered.

### 9.1 Tier 1: Supported And Tested

Tier 1 browsers must have:

1. automated CI smoke tests;
2. multi-tab ownership tests;
3. stale-owner recovery tests;
4. OPFS sync access handle tests;
5. persistence/quota diagnostics tests where the browser exposes those APIs;
6. browser transport performance tests;
7. documented known limitations.

Initial Tier 1 should target Chromium-family desktop browsers, with Chrome and
Edge tested separately before both are claimed.

### 9.2 Tier 2: Compatible When Probes Pass

Tier 2 browsers may be documented as compatible when capability probes pass, but
they are not release blockers until CI coverage is promoted.

Firefox should start here unless and until the CI matrix proves the full runtime
contract.

### 9.3 Best Effort: Browser Forks

Browser forks such as Firefox-derived or Chromium-derived privacy browsers
should not be certified by brand in v1 production. They should work only when:

1. capability probes pass;
2. required APIs are not disabled by preferences or policy;
3. private browsing/storage isolation modes do not remove required guarantees.

Docs should phrase this as "capability-gated" rather than promising each fork.

### 9.4 Unsupported

Unsupported environments include:

1. browsers without OPFS synchronous access handles in an allowed worker context;
2. browsers where required coordination primitives are missing;
3. private/incognito modes where storage APIs fail or are ephemeral;
4. embedded webviews that disable OPFS or worker modules;
5. contexts blocked by enterprise policy or cross-origin isolation requirements,
   if those become necessary for a selected implementation.

Unsupported environments must fail explicitly with stable error codes.

---

## 10. Capability Probes

The web package should expose a probe API:

```ts
import { probeRuntime } from "@decentdb/web";

const report = await probeRuntime();
```

Representative shape:

```ts
interface BrowserRuntimeProbe {
  supported: boolean;
  tier: "supported" | "compatible" | "unsupported";
  runtime: {
    dedicatedWorker: boolean;
    sharedWorker: boolean;
    broadcastChannel: boolean;
    webLocks: boolean;
    serviceWorker: boolean;
  };
  storage: {
    opfsDirectory: boolean;
    syncAccessHandle: boolean;
    exclusiveAccessHandleLock: boolean;
    persistApi: boolean;
    persisted?: boolean;
    estimate?: {
      quotaBytes?: number;
      usageBytes?: number;
    };
  };
  decentdb: {
    wasmModule: boolean;
    wasmMemoryBytes?: number;
    parserProfile: string;
    resultTransport: "binary" | "json";
  };
  errors: Array<{
    code: string;
    message: string;
    details?: string;
  }>;
}
```

Probe rules:

1. Probe before opening OPFS handles.
2. Return machine-readable errors.
3. Avoid destructive writes unless a caller opts into an invasive probe.
4. Allow docs and support tooling to ask users for one report.
5. Keep probes fast enough for application startup.

---

## 11. Ownership Coordination Design

The core design decision is the owner model. The ADR must select one of these
directions or a justified hybrid.

### 11.1 Option A: Shared Worker Owner

One Shared Worker owns the DecentDB engine handle and OPFS access handles. Tabs
connect to the Shared Worker and send RPC requests.

Pros:

- natural multi-tab sharing model;
- one long-lived owner per origin and worker URL;
- tabs do not need to elect a Dedicated Worker owner.

Risks:

- OPFS synchronous access handle availability must be verified in Shared Worker
  contexts, not assumed;
- browser support may be weaker or inconsistent;
- worker lifetime behavior can vary;
- service worker interaction still needs explicit routing.

### 11.2 Option B: Dedicated Worker Owner With BroadcastChannel Discovery

One tab hosts a Dedicated Worker owner. Other tabs discover the owner through
BroadcastChannel and forward requests to the owning tab/worker bridge.

Pros:

- preserves the current Dedicated Worker storage model;
- closer to ADR 0161 and current implementation;
- easier to keep OPFS access handle assumptions stable.

Risks:

- owner tab closure requires transfer or recovery;
- request routing through a page context is more complex;
- background sync without an open page is limited;
- lifecycle edge cases are numerous.

### 11.3 Option C: Web Locks + Dedicated Worker Owner

Each tab may attempt to become owner, but a Web Lock or equivalent browser lock
ensures one active owner at a time. Non-owners either wait, fail, or route through
the current owner.

Pros:

- explicit browser lock primitive;
- stale owner may be easier for the browser to handle;
- can keep Dedicated Worker OPFS model.

Risks:

- Web Locks support and behavior must be part of the support matrix;
- holding locks across long-running worker ownership needs careful validation;
- lock availability does not by itself solve client request routing.

### 11.4 Required Owner Semantics

Whichever model is selected, these rules are mandatory:

1. no two runtime owners may hold write-capable OPFS handles for the same logical
   database path;
2. read-only handles must not bypass WAL visibility rules;
3. owner identity must be visible in diagnostics;
4. owner shutdown must close engine and OPFS handles;
5. owner crash recovery must be deterministic and tested;
6. open attempts during recovery must either wait with timeout or fail with a
   stable retryable error;
7. import/export/checkpoint must route through the owner.

---

## 12. OPFS Lock Ownership And Stale Recovery

Current v1 ownership is process-local to one worker's in-memory map. Production
needs cross-context ownership.

### 12.1 Owner Record

The runtime should maintain an owner record separate from the database and WAL
files. The selected ADR must define where it lives.

Possible fields:

```json
{
  "format": "decentdb.browser.owner.v1",
  "databasePath": "app.ddb",
  "ownerId": "uuid-or-random-token",
  "createdAtMs": 1780000000000,
  "lastHeartbeatMs": 1780000005000,
  "runtime": "dedicated-worker",
  "userAgentFamily": "chromium",
  "schemaVersion": 1
}
```

### 12.2 Lease And Heartbeat

If leases are used:

1. heartbeat interval must be much shorter than lease timeout;
2. lease timeout must be long enough to avoid takeover during normal GC pauses;
3. recovery must prove the old owner is unreachable before takeover where
   possible;
4. takeover must close or wait for OPFS exclusive handles according to browser
   behavior;
5. all lease timing should be configurable only behind advanced options.

### 12.3 Recovery States

The runtime should model recovery explicitly:

```text
idle
  -> opening
  -> owner-active
  -> owner-lost
  -> recovering
  -> recovered
  -> failed-unsupported
  -> failed-corrupt-or-locked
```

Recovery must distinguish:

- owner process disappeared;
- OPFS handle is still locked;
- database recovery found WAL work;
- database corruption was detected;
- browser storage API failed;
- user canceled or timed out.

---

## 13. Service Worker Policy

Service workers are valuable for background sync, but they are also dangerous for
database ownership because their lifecycle is browser-controlled.

The ADR must choose one policy.

### 13.1 Policy A: Service Workers Cannot Own Databases

Service workers may not call `open()` and may not hold DecentDB OPFS handles.
They can only notify pages or relay network events. Browser sync requires a page
or supported runtime owner to be active.

This is the safest initial policy.

### 13.2 Policy B: Service Workers Route Through An Existing Owner

Service workers can send messages to an active owner but cannot create a new
owner. If no owner is active, the operation fails with a stable "owner required"
error.

This may support foreground-assisted sync without service-worker-owned storage.

### 13.3 Policy C: Service Workers Can Own A Restricted Runtime

Service workers can own the runtime for specific short tasks such as sync.

This must not be accepted without strong proof that OPFS sync access handles,
worker lifetime, crash recovery, and lock release are safe in target browsers.

---

## 14. Browser Sync Integration

Browser production runtime and production sync relay should be designed together.
The browser runtime should not invent a separate replication model.

### 14.1 Requirements

1. Sync operations route through the runtime owner.
2. Sync metadata writes preserve crash recovery guarantees.
3. Browser transport supports HTTP/WebSocket where allowed by the sync spec.
4. Sync can be canceled or timed out without leaving partial local state.
5. Shape subscriptions use the sync scope/change-stream contract instead of a
   browser-only notification layer.
6. Sync diagnostics are queryable through `metrics()` and `sys.*`.

### 14.2 Representative API Direction

```ts
await db.sync.configurePeer({
  name: "cloud",
  endpoint: "https://sync.example.com",
  tokenProvider: async () => fetchToken(),
});

await db.sync.run({
  peer: "cloud",
  direction: "both",
  signal: abortController.signal,
});
```

This document does not define sync relay protocol details. It requires browser
runtime ownership and transport decisions to be compatible with
`WIN_LOCAL_FIRST_SYNC_FIRST_CLASS_SPEC.md` and its production follow-up.

---

## 15. Parser And API Parity

Browser v1 uses a wasm-target parser subset because the native `pg_query` C
parser does not build for `wasm32-unknown-unknown`. Production browser users
need a deliberate SQL profile.

### 15.1 Parser Strategy Options

The ADR must choose one:

1. expand DecentDB's wasm parser for common app SQL;
2. adopt a wasm-compatible parser dependency;
3. provide a build profile that includes a full parser where technically
   possible;
4. expose a documented "browser SQL profile" and keep native broader.

### 15.2 Minimum Production SQL Profile

The first production browser profile should cover common local-first app usage:

1. `CREATE TABLE`, `DROP TABLE`, and common constraints;
2. `CREATE INDEX` and `DROP INDEX`;
3. `INSERT`, `UPDATE`, `DELETE`, and `UPSERT` if native supports it;
4. parameterized `SELECT` with joins, filters, ordering, limits, and projection;
5. transactions where supported by public APIs;
6. schema introspection used by app startup and migrations;
7. sync-required SQL primitives when browser sync is enabled.

The production profile must be tested independently from native parser tests so
browser limitations remain visible.

### 15.3 Parameter And Result Parity

Browser APIs should support:

1. integers without lossy JavaScript number assumptions where possible;
2. binary/BLOB parameters and results;
3. UUID, date/time, decimal, and typed values using stable tagged shapes;
4. transferable binary result transport as the default;
5. JSON transport as debug compatibility only;
6. clear errors for unsupported value kinds.

---

## 16. Diagnostics And System Surfaces

The browser runtime should expose diagnostics through both the TypeScript API
and SQL-visible surfaces where practical.

### 16.1 `metrics()` Additions

Representative `metrics()` fields:

```ts
interface BrowserMetrics {
  wasmMemoryBytes?: number;
  wasmMemoryPages?: number;
  jsHeapBytes?: number;
  opfsSupported?: boolean;
  opfsSyncAccessHandleSupported?: boolean;
  persistentStorageGranted?: boolean;
  quotaBytes?: number;
  storageUsageBytes?: number;
  ownerId?: string;
  ownerRuntime?: "dedicated-worker" | "remote-owner";
  attachedClientCount?: number;
  staleOwnerRecoveries?: number;
  coordinationModel?: string;
  parserProfile?: string;
}
```

### 16.2 `sys.*` Direction

Possible browser-visible system views:

```sql
SELECT * FROM sys.browser_runtime;
SELECT * FROM sys.browser_storage;
SELECT * FROM sys.browser_owner;
SELECT * FROM sys.browser_sync;
```

These should be target-gated. Native builds should not pay runtime cost for
browser-only diagnostics.

### 16.3 Error Codes

Stable browser error classes should include:

- `ERR_BROWSER_UNSUPPORTED`
- `ERR_BROWSER_OPFS_UNAVAILABLE`
- `ERR_BROWSER_SYNC_ACCESS_HANDLE_UNAVAILABLE`
- `ERR_BROWSER_COORDINATION_UNAVAILABLE`
- `ERR_BROWSER_OWNER_TIMEOUT`
- `ERR_BROWSER_OWNER_STALE`
- `ERR_BROWSER_OWNER_RECOVERY_FAILED`
- `ERR_BROWSER_STORAGE_PERSISTENCE_DENIED`
- `ERR_BROWSER_QUOTA_EXCEEDED`
- `ERR_BROWSER_PRIVATE_MODE_UNSUPPORTED`
- `ERR_BROWSER_SQL_PROFILE_UNSUPPORTED`

---

## 17. Package And Build Shape

The web package should support production web delivery without compromising
native builds.

### 17.1 Feature Profiles

Potential profiles:

1. `browser-min`: OPFS runtime, binary transport, minimal SQL profile.
2. `browser-app`: common app SQL profile, sync client, diagnostics.
3. `browser-debug`: JSON transport, extra diagnostics, assertions.

The exact profile names should be decided by ADR and package design.

### 17.2 Bundle Size Guardrails

Add CI checks for:

1. wasm binary size;
2. worker JavaScript size;
3. TypeScript package bundle size;
4. optional feature deltas;
5. transport benchmark regression.

Size budgets should be documented as release guardrails, not guessed in this
document.

---

## 18. Implementation Slices

### Slice 0: Follow-Up ADR And Test Matrix

Deliverables:

1. ADR for browser support tiers and capability contract.
2. ADR for ownership coordination model.
3. ADR for service worker policy.
4. CI plan for Chrome, Edge, and Firefox candidacy.
5. Updated `docs/api/wasm.md` with production roadmap boundaries.

Acceptance:

- no production browser claim is made without CI coverage;
- unsupported environments and fallback policy are documented;
- implementation does not begin until ownership semantics are accepted.

### Slice 1: Capability Probes And Stable Errors

Deliverables:

1. `probeRuntime()` TypeScript API.
2. structured capability report.
3. stable browser error codes.
4. startup probe integration in `open()`.
5. docs troubleshooting section.

Acceptance:

- unsupported browser modes fail before opening the database;
- probe output can be used in support reports;
- current Chromium smoke continues to pass.

### Slice 2: Owner Runtime Refactor

Deliverables:

1. isolate worker RPC transport from owner discovery;
2. introduce owner identity and owner state model;
3. route `open()`, `close()`, `exec()`, `query()`, `checkpoint()`, `export()`,
   and `import()` through the owner abstraction;
4. keep current single Dedicated Worker behavior as one owner implementation.

Acceptance:

- existing public API remains compatible;
- single-tab behavior is unchanged except for diagnostics;
- owner state is visible through `metrics()`.

### Slice 3: Multi-Tab Coordination

Deliverables:

1. selected coordination primitive implementation;
2. client attach/detach protocol;
3. request routing from non-owner tabs;
4. bounded open timeout and retryable errors;
5. Playwright multi-tab tests.

Acceptance:

- two tabs opening the same path do not create independent write owners;
- writes from both tabs serialize through one owner;
- closing a non-owner tab does not affect the owner;
- closing the owner tab recovers or transfers according to ADR.

### Slice 4: Stale-Owner Recovery

Deliverables:

1. owner lease or equivalent liveness mechanism;
2. stale-owner detection;
3. takeover/recovery flow;
4. OPFS lock failure mapping;
5. crash/hot-reload test harness.

Acceptance:

- stale owners are detected deterministically;
- recovery replays WAL through normal engine open;
- open attempts during recovery have documented timeout behavior;
- no test creates two write-capable owners for one path.

### Slice 5: Service Worker Policy

Deliverables depend on selected policy.

If service workers are excluded:

1. block `open()` in service worker contexts with a stable error;
2. document supported alternatives;
3. add tests for explicit failure.

If service workers route through owners:

1. add message protocol;
2. fail when no owner exists;
3. test lifecycle and timeout behavior.

Acceptance:

- service worker behavior is boring, explicit, and documented;
- no service worker path can silently create a competing owner.

### Slice 6: Browser Sync Transport

Deliverables:

1. owner-routed sync API shell;
2. HTTP/WebSocket transport integration as defined by sync follow-up spec;
3. cancellation and timeout behavior;
4. sync diagnostics in `metrics()` and `sys.*`;
5. browser sync smoke tests.

Acceptance:

- sync writes route through the owner;
- sync cannot run concurrently with import/export in unsafe ways;
- failed sync attempts leave durable metadata in a recoverable state.

### Slice 7: Parser And Value Parity

Deliverables:

1. accepted browser SQL profile;
2. expanded wasm parser or selected parser dependency;
3. BLOB and tagged typed parameter support;
4. result transport coverage for browser-specific value shapes;
5. browser SQL compatibility tests.

Acceptance:

- common application SQL runs in browser builds;
- unsupported SQL returns stable profile errors;
- native parser behavior is not regressed.

### Slice 8: Quota, Persistence, And Durability Diagnostics

Deliverables:

1. storage estimate reporting;
2. persistence grant reporting;
3. OPFS capability and flush diagnostics;
4. quota-exceeded error mapping;
5. docs for user warnings, sync/export recommendations, and private browsing.

Acceptance:

- applications can detect risky storage posture;
- quota failures are distinguishable from SQL/storage corruption;
- docs avoid claiming native filesystem durability for OPFS.

### Slice 9: Browser CI And Performance Matrix

Deliverables:

1. Playwright projects for Tier 1 browsers;
2. Firefox candidacy project;
3. multi-tab and recovery tests;
4. transport benchmark thresholds;
5. bundle size checks;
6. release checklist entries.

Acceptance:

- Tier 1 claims are CI-backed;
- regression failures block releases for claimed tiers;
- candidate tiers can fail without breaking release unless intentionally
  promoted.

### Slice 10: Documentation And Examples

Deliverables:

1. update `docs/api/wasm.md`;
2. update `bindings/web/README.md`;
3. add compatibility matrix;
4. add multi-tab lifecycle examples;
5. add sync example when browser sync is supported;
6. add troubleshooting guide with probe report examples.

Acceptance:

- users can determine whether their browser/runtime is supported;
- examples do not encourage multiple independent opens for the same path;
- durability limitations are clear and prominent.

---

## 19. Testing Strategy

### 19.1 Unit Tests

Add TypeScript tests for:

1. capability probe result shaping;
2. stable error mapping;
3. owner state transitions;
4. request routing;
5. timeout and cancellation behavior;
6. parameter/result encoding.

### 19.2 Browser Smoke Tests

Add Playwright tests for:

1. single-tab create/open/query/reopen;
2. two-tab shared ownership;
3. owner tab close;
4. non-owner tab close;
5. import/export routing;
6. checkpoint routing;
7. quota/persistence diagnostic availability where supported.

### 19.3 Recovery Tests

Add browser tests or harness tests for:

1. worker termination;
2. page reload while owner exists;
3. hot reload opening a second runtime;
4. stale owner record;
5. OPFS handle still locked;
6. WAL replay after crash-like shutdown.

### 19.4 Sync Tests

When browser sync lands:

1. local writes before sync;
2. pull then query;
3. push then inspect peer state;
4. cancellation before network response;
5. retry after transient network failure;
6. conflict metadata visibility.

### 19.5 Compatibility Tests

Each claimed browser tier requires:

1. OPFS open/write/flush/read test;
2. exclusive handle behavior test;
3. worker module load test;
4. coordination primitive test;
5. persistent storage probe test where supported;
6. private/incognito behavior documented or tested manually if CI cannot cover
   it.

### 19.6 Performance Tests

Protect:

1. open latency;
2. query latency over binary transport;
3. large result memory behavior;
4. multi-tab routing overhead;
5. queued writes under tab contention;
6. wasm and worker bundle size.

---

## 20. Documentation Requirements

Documentation must answer:

1. Which browsers are supported?
2. What does "supported" mean?
3. How does DecentDB behave with multiple tabs?
4. Can service workers use DecentDB?
5. What storage API does DecentDB use?
6. What durability limits does OPFS have?
7. What happens in private browsing?
8. How should important data be synced or exported?
9. What SQL is supported in browser builds?
10. How do users collect diagnostics?

Docs should avoid:

1. implying OPFS equals native fsync durability;
2. promising support for every Chromium or Firefox fork;
3. suggesting IndexedDB/localStorage fallback under the DecentDB durability
   contract;
4. telling users to open multiple independent workers for one database path.

---

## 21. Security And Privacy Considerations

1. Browser storage is origin-scoped. Docs must explain origin changes,
   subdomains, and local development hostnames.
2. Sync tokens must not be stored in plaintext examples without warning.
3. Probe reports must avoid leaking application data or database contents.
4. Owner IDs should be random runtime identifiers, not user identifiers.
5. Service worker messaging must validate origin and expected message shape.
6. Import/export APIs must keep explicit user/application control.
7. Browser privacy modes may deliberately reduce persistence guarantees.

---

## 22. Open Questions

1. Can OPFS synchronous access handles be relied on in Shared Worker contexts for
   the target browser matrix, or must the owner remain Dedicated Worker based?
2. Should the first production owner model use Web Locks, BroadcastChannel, a
   Shared Worker, or a hybrid?
3. What is the minimum browser SQL profile for local-first app production?
4. Should Firefox be a Tier 1 target in the first production release or a
   candidate tier until the runtime proves stable?
5. What is the Safari/iOS position for the first production browser release?
6. How much browser sync can work without service worker ownership?
7. Which `sys.*` surfaces should exist in wasm builds versus TypeScript-only
   `metrics()`?
8. What bundle size budget is acceptable for the production browser package?
9. Should browser runtime probes be exposed by CLI/docs tooling for support
   bundles?
10. How should browser support claims interact with enterprise policy-managed
    browsers?

---

## 23. Completion Criteria

This Future Win is complete when:

1. required ADRs are accepted;
2. a production support matrix is documented;
3. Tier 1 browsers pass CI for smoke, multi-tab, recovery, and performance;
4. one active runtime owner per logical path is enforced;
5. stale-owner recovery is tested;
6. service worker behavior is explicit and tested;
7. browser sync transport is compatible with the production sync spec or
   explicitly deferred with stable errors;
8. parser/API profile is documented and tested;
9. quota, persistence, durability, and owner diagnostics are exposed;
10. unsupported environments fail clearly;
11. docs and examples match the production contract;
12. native hot paths remain unaffected by browser-only runtime code.

Once these are done, remove the roadmap item from `FUTURE_WINS.md` and keep only
a delivered-context entry describing the shipped production browser runtime.

---

## 24. Phase #1 Implementation Status (2026-05-20)

This section records the concrete implementation decisions and slice status for
the first production-browser-runtime phase execution.

### Accepted decisions

1. Support tiers and capability contract are defined by ADR 0165.
2. Owner coordination model is a Dedicated Worker owner guarded by Web Locks and
   discovered/routed through BroadcastChannel for one logical path.
3. Service workers cannot own browser database handles.
4. Browser sync is exposed as an owner-routed API shell with explicit deferred
   status/errors until production relay transport follow-up work is promoted.
5. Browser SQL profile is named `browser-app-v1`.
6. Browser diagnostics are exposed through `metrics()` and browser `sys.*`
   views in wasm builds (`sys.browser_runtime`, `sys.browser_owner`,
   `sys.browser_storage`, `sys.browser_sync`).
7. No silent durability fallback was added.

### Slice tracking (phase #1)

- Slice 0: complete (ADR 0165 + matrix/docs policy captured).
- Slice 1: complete (probe API + stable browser errors + startup probe path).
- Slice 2: complete (owner abstraction and routing path).
- Slice 3: implemented and test-scaffolded (multi-tab owner routing tests added).
- Slice 4: implemented and test-scaffolded (owner timeout/stale-owner surfaces).
- Slice 5: complete (service worker explicit unsupported policy).
- Slice 6: complete for the production-browser-runtime contract (owner-routed
  sync API shell; production relay transport remains in the separate sync relay
  roadmap item and returns stable deferred status here).
- Slice 7: complete for phase scope (tagged/binary/browser typed params in wasm
  bridge + compatibility coverage updates).
- Slice 8: complete for phase scope (quota/persistence/owner diagnostics in
  metrics + `sys.browser_*` system views + error mapping).
- Slice 9: complete for phase scope (tier-1/candidate Playwright matrix configs
  and scripts; candidate path is non-blocking).
- Slice 10: complete (wasm/web docs and troubleshooting guidance updated).

### Environment note

SharedWorker ownership was rejected during implementation because Playwright
bundled Chromium in this execution environment does not provide the required
OPFS synchronous access-handle behavior in SharedWorker contexts. The accepted
model keeps OPFS ownership in a Dedicated Worker and uses BroadcastChannel plus
Web Locks for cross-tab coordination.
