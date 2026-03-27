# DecentDB WASM & Browser Support Implementation Plan
**Date:** 2026-03-26  
**Status:** Proposed

## 1. Purpose

This document translates the "First-Class WASM & Browser Support" proposal from
`design/DECENTDB_FUTURE_WINS.md` into an implementation plan that is:

- incremental
- quality-gated
- explicit about risk
- aligned with the 7 PRD tenets in `design/PRD.md`
- careful not to degrade the native engine

This is not an approval to code the entire feature in one change. The work must
be delivered in slices with explicit stop/go gates.

## 2. Why This Needs a Dedicated Plan

Browser support sounds simple on the surface because the engine is written in
Rust, but a production-quality browser port is not "just compile to wasm":

- the browser file API with the best performance path is OPFS synchronous access
  in a Dedicated Worker only
- DecentDB's durability model is WAL-first, so browser flush semantics must be
  mapped carefully
- the current parser dependency and some current build/runtime assumptions may
  not be wasm-friendly
- the browser package must hide worker complexity without hiding important
  durability limits
- none of this work is allowed to harm the native engine's ACID guarantees,
  hot-path latency, or developer feedback loop

This feature also crosses an ADR-required boundary:

- it likely introduces major dependency additions
- it introduces a new VFS/runtime environment
- it creates new product-level durability semantics for a new storage substrate

An ADR must be written and accepted before implementation begins.

## 3. Design Inputs

This plan is derived from the following project documents:

- `design/PRD.md`
- `design/SPEC.md`
- `design/TESTING_STRATEGY.md`
- `docs/design/spec.md`
- `design/DECENTDB_FUTURE_WINS.md`
- `design/adr/0105-in-memory-vfs.md`
- `design/adr/0117-shared-wal-registry.md`
- `design/adr/0119-rust-vfs-pread-pwrite.md`
- `design/adr/0055-thread-safety-and-snapshot-context.md`

## 4. Guiding Constraints

### 4.1 Non-Negotiable Product Constraints

The browser implementation must preserve the same high-level product identity as
the native engine:

- one writer
- multiple concurrent readers as a design goal, but no weaker-than-documented
  semantics hidden behind the API
- WAL-based durability and recovery
- single authoritative Rust engine
- no parallel "browser-only database core"

### 4.2 What "First-Class" Means Here

For this feature, "first-class" means:

- DecentDB can run in modern browsers through a supported package
- OPFS is the primary persistence backend
- the package hides worker orchestration by default
- open/query/prepare/step/export/import are documented and tested
- the browser package is treated as an official binding surface, not a demo

It does **not** mean:

- identical durability guarantees to a desktop OS with power-loss-resistant
  filesystems
- multi-tab write coordination in v1
- zero-copy across every boundary in the strict theoretical sense
- broad platform support before the compatibility matrix is documented

## 5. The 7 PRD Tenets Applied To WASM

This section is the acceptance lens for every slice.

### 5.1 Tenet 1: ACID Compliance Is Forefront

Browser support is acceptable only if the implementation states, tests, and
documents the exact durability contract of OPFS.

Requirements:

- no weakening of native durability semantics
- no silent fallback from OPFS to weaker storage backends
- no "success" returned before WAL/data flush work required by the selected sync
  mode completes
- recovery after worker crash / page reload / abrupt worker termination must be
  tested
- export/import must not silently drop committed WAL state

Interpretation:

- native ACID remains the gold standard
- browser durability must be "as durable as the browser storage substrate
  permits" and must be described precisely

### 5.2 Tenet 2: Uncompromising Performance

The browser port is only worth shipping if it does not regress native hot paths
and if the browser path itself is designed for low overhead.

Requirements:

- no extra branching, locking, or allocation on the native OS VFS hot path
- no mandatory async refactor of the engine core
- worker boundary must avoid per-cell JSON serialization for large result sets
- WASM build changes must not slow native builds materially

Interpretation:

- the engine remains synchronous internally
- the browser binding owns async orchestration around the engine

### 5.3 Tenet 3: Minimal Disk Footprint

Browser support must not fork the file format or inflate on-disk structures.

Requirements:

- database file format unchanged
- WAL format unchanged
- no browser-only record/page padding changes
- package size and wasm binary size tracked with explicit budgets

Interpretation:

- browser storage overhead is allowed only in package/runtime layers, not in the
  database format

### 5.4 Tenet 4: World-Class Documentation

The browser story must be documented with the same seriousness as native
bindings.

Requirements:

- architecture doc
- browser compatibility and limitations doc
- examples for create/open/query/export/import
- clear durability caveats
- troubleshooting for storage persistence and quota eviction

### 5.5 Tenet 5: Best-in-Class Tooling & Bindings

The browser package is a binding. It must behave like an official one.

Requirements:

- package naming, API, and testing quality match other first-class bindings
- no browser binding that bypasses the Rust engine as authoritative core
- no parallel semantic contract drifting from the main engine

### 5.6 Tenet 6: Fantastic CLI Experience

This tenet is indirectly relevant.

Requirements:

- browser work must not destabilize or complicate CLI packaging
- any shared build refactors must preserve CLI behavior
- export/import format decisions should remain compatible with CLI tooling where
  practical

### 5.7 Tenet 7: Fast Developer Feedback Loop

This feature must not wreck CI time or local iteration speed.

Requirements:

- browser testing must be split into fast PR checks and deeper nightly coverage
- wasm compile checks must be narrow and deterministic
- browser integration tests must have a small smoke subset for PRs
- long-running browser matrix/perf runs belong in nightly CI

## 6. Scope

### In Scope

- wasm-compatible engine build
- OPFS-backed VFS
- Dedicated Worker runtime
- official `@decentdb/web` package
- async JS/TS API over a worker-owned synchronous engine
- export/import/persistence helpers
- browser-specific smoke, correctness, recovery, and performance validation

### Out of Scope for Initial Delivery

- multi-tab write coordination
- service worker support
- IndexedDB as a transparent primary storage backend
- changing on-disk DB or WAL format
- rewriting the engine around async/await internally
- broad browser feature parity before compatibility is documented
- cross-origin sync/replication

## 7. Proposed Architecture

### 7.1 High-Level Shape

The recommended architecture is:

1. Keep the DecentDB core engine synchronous.
2. Compile that engine to `wasm32-unknown-unknown`.
3. Run the engine inside a Dedicated Worker.
4. Implement an `OpfsVfs` inside the worker using OPFS synchronous access
   handles.
5. Expose an async main-thread API through a thin RPC layer.
6. Return results in binary batches, not JSON row objects, for large result
   sets.

### 7.2 Why This Shape Is Recommended

This plan fits the current repository better than an engine-wide async rewrite:

- the VFS already exists as the correct abstraction seam
- the native engine is optimized around synchronous pager/WAL behavior
- browser sync file access is available in a worker
- the project already treats bindings as wrappers over the authoritative engine

### 7.3 Core Principle

The browser runtime should adapt to the engine, not force the engine to become a
browser runtime.

That means:

- synchronous engine
- worker-owned database instance
- async binding layer at the package boundary

## 8. Main Technical Risks

### 8.1 Parser Feasibility Risk

The current SQL parser stack may be the first hard blocker for wasm support.

The implementation must not assume the current parser dependency compiles cleanly
to `wasm32-unknown-unknown`. This is an early stop/go gate.

### 8.2 Durability Contract Risk

Browser storage durability is not identical to native filesystems. OPFS can be
subject to quota and eviction behavior depending on browser policies.

The risk is not only technical. It is product and documentation risk. If the API
overstates durability, the feature violates PRD Tenet 1.

### 8.3 Performance Drift Risk

There is a real risk of harming the native engine if wasm support is added as
always-on abstraction cost.

Examples:

- feature gates leaking into hot paths
- new virtual dispatch or dynamic checks in native paths
- heavier dependency graphs for all builds
- packaging decisions that increase compile time materially

### 8.4 Binding Semantics Risk

The browser package must not imply stronger concurrency guarantees than the
engine currently documents.

The safest initial shape is:

- one `Db` per worker-owned connection
- serialized operations per connection
- multiple browser connections allowed only within the documented constraints

### 8.5 Target-Specific Threading / `Send` / `Sync` Risk

The wasm target is single-threaded by default, but the current engine and VFS
surfaces rely heavily on `Send + Sync` bounds and standard synchronization
primitives.

Required posture:

- audit current target-specific thread and auto-trait assumptions
- do not relax core `Send` / `Sync` bounds casually just to make browser code
  compile
- treat any required divergence in core thread-safety assumptions as a
  high-risk design event, not a routine porting detail

Preferred outcome:

- preserve the current native thread-safety contract unchanged
- keep any wasm-specific adaptation outside native hot paths

### 8.6 WASM Linear Memory / Large Result Risk

Large result sets are constrained by more than transport overhead.

The engine currently materializes result structures in Rust memory, which means
browser support must also consider:

- wasm linear memory growth behavior
- peak memory usage during large result materialization
- practical limits on unbounded `fetchAll()` style APIs

This risk is distinct from JSON serialization cost and needs explicit treatment
in S7.

## 9. Slice Status Map

### 9.1 Status Legend

Use these status values in this file and future progress updates:

- `proposed`: defined but not started
- `blocked`: cannot start because a dependency slice is unresolved
- `in_progress`: actively being implemented
- `done`: merged and validated
- `deferred`: intentionally postponed beyond initial browser support

### 9.2 Slice Table

| Slice | Status | Name | Depends On | Exit Gate |
|---|---|---|---|---|
| S0 | `proposed` | ADR + browser contract freeze | none | ADR accepted |
| S1 | `blocked` | Core wasm compile audit | S0 | `cargo check --target wasm32-unknown-unknown` passes for core path |
| S2 | `blocked` | Parser strategy for wasm | S1 | parser chosen and validated for wasm |
| S3 | `blocked` | VFS injection refactor | S1, S2 | core can open DBs against injected VFS cleanly |
| S4 | `blocked` | `OpfsVfs` implementation | S3 | open/read/write/recover/checkpoint work on OPFS |
| S5 | `blocked` | Worker runtime + RPC | S4 | async main-thread API works without manual worker plumbing |
| S6 | `blocked` | `@decentdb/web` API surface | S5 | package API stable enough for smoke coverage |
| S7 | `blocked` | Binary result transport optimization | S6 | large reads avoid JSON row serialization |
| S8 | `blocked` | Export/import/persistence contract | S4, S6 | data backup/restore semantics documented and tested |
| S9 | `blocked` | Browser correctness/perf/CI coverage | S4-S8 | PR and nightly gates defined and green |
| S10 | `blocked` | Docs/examples/release hardening | S6-S9 | docs and examples match shipped behavior |

### 9.3 Recommended Slice Order

The recommended delivery order is:

1. S0
2. S1
3. S2
4. S3
5. S4
6. S5
7. S6
8. S8
9. S7
10. S9
11. S10

Reasoning:

- parser viability is an early hard gate
- VFS and durability must exist before package polish
- export/import must be correct before performance tuning is considered complete
- export/import correctness also provides a stable dataset capture/restore path
  for browser transport benchmarks and regression reproduction
- browser perf work should not begin before correctness and semantics are stable

## 10. Slice Details

## S0. ADR + Browser Contract Freeze

**Status:** `proposed`

### Goal

Write and accept an ADR that defines the browser architecture and product
contract before implementation.

### Scope

- target architecture
- supported targets and browsers
- OPFS as primary backend
- worker ownership model
- durability statement
- sync mode mapping
- unsupported/fallback behavior
- export/import rules
- package ownership and binding contract

### Required Decisions

- whether the browser package is in-tree under `bindings/web/`
- whether a separate crate is introduced for wasm/browser glue
- whether the S0 ADR explicitly covers the S3 VFS injection refactor and `Db`
  open-path semantics, or whether S3 requires an ADR amendment before merge
- whether the current parser stack is retained or replaced for wasm
- whether planning should assume the current C-backed parser path is blocked on
  wasm until proven otherwise
- whether `WalSyncMode::Full` and `WalSyncMode::Normal` collapse to the same OPFS
  flush behavior in browsers
- whether browser v1 routes one logical database path through one worker-owned
  engine instance to avoid ambiguous cross-worker WAL semantics
- whether v1 supports one connection per worker only
- which wasm toolchain is authoritative
  - `wasm-bindgen`
  - packaging/build wrapper such as `wasm-pack` or an explicit workspace build
    flow
  - release optimization tooling such as `wasm-opt`
- which browser test stack is authoritative
  - Rust-side fast wasm tests via `wasm-bindgen-test`
  - browser-side integration tests via Playwright unless a stronger alternative
    is justified

### Exit Criteria

- ADR exists and is accepted
- ADR scope explicitly covers the injected-VFS strategy planned in S3
- unsupported environments are defined explicitly
- browser durability wording is approved
- package/API naming is frozen for initial implementation

## S1. Core WASM Compile Audit

**Status:** `blocked`

### Goal

Make the Rust engine buildable for wasm without changing native semantics.

### Scope

- add target-aware feature gates where needed
- isolate native-only modules
- make build scripts target-safe
- identify parser or dependency blockers early
- audit target-specific thread and `Send` / `Sync` assumptions in shared code

### Likely File Areas

- `crates/decentdb/Cargo.toml`
- `crates/decentdb/build.rs`
- `crates/decentdb/src/lib.rs`
- `crates/decentdb/src/c_api.rs`
- `crates/libpg_query_sys/`

### Constraints

- no browser logic in native hot paths
- no runtime overhead in native builds for code that can be compile-gated
- no weakening of existing C ABI behavior on native targets

### Deliverables

- documented wasm dependency audit
- compile-gated native-only surfaces
- a clean wasm build of the core path, or a written blocker with next action

### Validation

- `cargo check -p decentdb --target wasm32-unknown-unknown`
- `cargo check -p decentdb`
- `cargo clippy -p decentdb --all-targets --all-features`

### No-Harm Checks

- native binary/API unchanged unless explicitly intended
- no new per-call branching on native file I/O operations
- compile-time impact measured before and after
- no wasm-only workaround is allowed to silently weaken native `Send` / `Sync`
  guarantees without explicit design review

## S2. Parser Strategy For WASM

**Status:** `blocked`

### Goal

Resolve the SQL parser story for wasm before deeper browser implementation work.

### Why This Is Its Own Slice

If the parser does not compile to wasm, the feature is blocked regardless of VFS
or package work.

Working planning assumption:

- the current `libpg_query_sys` path wraps a C-based parser and should be
  treated as likely blocked on `wasm32-unknown-unknown` until proven otherwise

This does not force a parser replacement, but it means parser replacement or a
substantial parser adaptation path should be treated as a probable outcome, not
an edge case.

### Options

#### Option A: Current Parser Compiles Cleanly To WASM

Best outcome. Keep one parser path.

#### Option B: Parser Needs Target-Specific Wrapping

Acceptable if the semantic output is unchanged.

#### Option C: Introduce A WASM-Compatible Parser Backend

Allowed only if:

- semantics are kept aligned with the core engine
- normalization remains authoritative in Rust
- drift risk is explicitly managed

This is higher risk and must be justified.

It is also the most likely fallback path if the current C-backed parser cannot
be made wasm-compatible without unacceptable complexity.

### Deliverables

- parser feasibility report
- chosen parser approach
- tests proving the chosen parser path can parse the supported SQL subset in wasm

### Exit Criteria

- wasm parser path exists and passes representative parse tests
- no unsupported silent parser downgrade
- semantic drift risk is documented

### No-Harm Checks

- native parser throughput is not regressed materially
- SQL behavior does not fork between native and browser without documentation and
  tests

## S3. VFS Injection Refactor

**Status:** `blocked`

### Goal

Refine the VFS seam so the browser runtime can supply an injected VFS cleanly,
without path-based hacks.

### Problem Being Solved

Current VFS selection is largely based on path inspection. That is sufficient for
OS and `:memory:` use, but browser support needs a deliberate injected VFS
selection mechanism.

### Scope

- make `Db::open/create/open_or_create` work cleanly with an injected VFS path
- preserve current OS and memory behavior
- avoid broad API churn unless justified

### Recommended Direction

Introduce an internal constructor path that allows the browser glue layer to
choose:

- VFS implementation
- database logical path/key
- optional browser-specific metadata if needed

The public Rust API can remain conservative initially if that reduces churn.

This slice must remain within the ADR scope frozen in S0. If the injected-VFS
design materially expands the public open/create semantics beyond that ADR
scope, an ADR amendment is required before merge.

### Deliverables

- injected VFS opening path
- no regression in existing `:memory:` or OS-backed behavior
- tests covering VFS-driven open/create/reopen flows

### Validation

- existing VFS tests
- OS-backed persistence tests
- in-memory VFS tests
- new injected-VFS smoke tests

### No-Harm Checks

- no additional overhead on `OsVfs` calls
- no new lock contention in pager/WAL open paths
- shared WAL registry behavior unchanged for native on-disk databases

## S4. `OpfsVfs` Implementation

**Status:** `blocked`

### Goal

Implement a browser-backed VFS over OPFS synchronous access handles.

### Scope

- `open`
- `file_exists`
- `remove_file`
- `canonicalize_path` or equivalent stable logical keying
- `read_at`
- `write_at`
- `file_size`
- `set_len`
- `sync_data`
- `sync_metadata`

### Recommended Mapping

- database file -> OPFS file handle + sync access handle
- WAL file -> separate OPFS file handle + sync access handle
- `sync_data` -> `flush()`
- `sync_metadata` -> `flush()` plus any handle-finalization semantics required by
  the browser API

### Initial Sync-Mode Recommendation

The conservative v1 mapping should be:

- `WalSyncMode::Full` -> `flush()` at the browser transaction durability
  boundary
- `WalSyncMode::Normal` -> the same `flush()` behavior as `Full` in v1 unless a
  weaker mapping is explicitly justified and tested
- `WalSyncMode::TestingOnlyUnsafeNoSync` -> no flush, clearly documented as
  unsafe

Rationale:

- OPFS does not expose the same rich sync distinctions as native filesystems
- browser support should bias toward correctness first
- any intentionally widened durability-loss window for browser `Normal` should
  be deferred until measured and justified

This means browser `Full` may not be materially stronger than browser `Normal`
in v1.

### Important Constraint

This VFS must live in a Dedicated Worker because the highest-performance OPFS
sync API is Dedicated-Worker-only.

### Shared WAL Registry Scope

ADR-0117's shared WAL registry is process-global in native builds. In browser
support, that registry should be treated as:

- at most worker-local / wasm-instance-local
- not a cross-worker coordination mechanism
- not a cross-tab coordination mechanism

Implications for v1:

- browser support must not claim cross-worker WAL sharing semantics
- if the same logical OPFS database path is opened in multiple workers, that is
  outside the guarantees of the initial design
- the preferred v1 package shape is to route one logical database path through a
  single worker-owned engine instance

### Durability Notes

The VFS implementation must document:

- what `flush()` means for DecentDB durability
- what it does not guarantee
- how browser eviction/quota behavior changes the long-term persistence story
- that browser durability loss windows come from the platform/storage substrate
  after `flush()`, not from an intentionally weakened v1 sync schedule

### Deliverables

- `OpfsVfs`
- worker-only runtime guardrails
- reopen/recovery tests
- checkpoint tests
- WAL retention/truncation tests on OPFS

### Exit Criteria

- create/open/reopen of a persisted DB works
- committed WAL data survives worker restart in supported environments
- recovery ignores incomplete WAL frames as expected
- checkpoint writes main DB file correctly on OPFS

### No-Harm Checks

- no changes to DB/WAL file format
- no browser-specific branches in native file I/O hot path
- browser-specific dependencies are target-gated

## S5. Worker Runtime + RPC Layer

**Status:** `blocked`

### Goal

Create a worker-owned execution model that hides worker complexity from the
browser developer.

### Architecture

- main thread: API facade only
- worker: owns wasm module instance, DB handles, statement handles, OPFS access
- message protocol: request/response with opaque ids

### Recommended Initial Semantics

- one database handle executes one operation at a time
- statements are worker-owned and referenced by ids
- cancellations/timeouts may be deferred if not cleanly supported in v1

### Why Serialization Is Acceptable Initially

The current engine already documents limits around concurrent use of a single
connection. Serializing operations per connection is the safest browser v1
interpretation.

### Deliverables

- worker bootstrap
- wasm loader
- RPC protocol
- handle lifecycle management
- clean shutdown/close semantics

### Validation

- open/close smoke tests
- repeated open/close leak tests
- statement lifecycle tests
- failure propagation tests

### No-Harm Checks

- no async contamination of the core engine
- browser errors preserve engine error context, not generic wrapper failures

## S6. `@decentdb/web` API Surface

**Status:** `blocked`

### Goal

Ship an official browser package with a clear, async-first API.

### Recommended Initial API

- `open(options)`
- `db.exec(sql, params?)`
- `db.prepare(sql)`
- `stmt.bind*()`
- `stmt.step()` or `stmt.fetch(n)`
- `stmt.close()`
- `db.checkpoint()`
- `db.export()`
- `db.import(...)`
- `db.persist()`
- `db.close()`

### API Principles

- async at the package boundary
- explicit lifecycle where it matters
- error messages preserve operation context
- no false promise of cross-tab or cross-worker coordination

### Deliverables

- package manifest
- typed API
- browser-ready build artifact flow
- minimal developer setup

### Exit Criteria

- package can open a DB and run smoke queries in a supported browser
- worker startup is transparent to caller
- error handling is useful and documented

### No-Harm Checks

- package shape does not require engine changes that penalize native users
- native bindings remain authoritative for non-browser environments

## S7. Binary Result Transport Optimization

**Status:** `blocked`

### Goal

Reduce result transfer overhead for large reads.

### Why This Is Separate

Correctness must ship before transport optimization. This slice improves
performance after the browser path is already working.

### Problem

The current public result model materializes rows and values. Sending these as
JSON or per-row JS objects across the worker boundary will create avoidable CPU
and GC cost.

There is also a second issue:

- large results can exhaust or bloat wasm linear memory before transport itself
  becomes the bottleneck

### Recommended Direction

Introduce a batched binary row transport format with:

- schema metadata
- typed column descriptors
- contiguous buffers for row data where practical
- transferable `ArrayBuffer`s from worker to main thread

The browser API should also steer developers toward:

- incremental fetch
- bounded batch sizes
- async iteration

rather than encouraging unbounded materialization of very large result sets in a
single call.

### Important Precision

The phrase "zero-copy" should be used carefully. The practical goal is:

- avoid per-row JSON serialization
- minimize copies across the worker boundary
- keep decode cost linear and predictable

### Deliverables

- batch fetch transport format
- binary decoding layer in JS
- benchmark proving material improvement versus naive JSON row transport
- peak wasm memory instrumentation during large-result benchmarks
- explicit guidance on batch sizing / streaming for large reads

### Exit Criteria

- large result sets move without row-by-row JSON encoding
- benchmark shows measurable reduction in CPU/GC overhead
- transport benchmarks also track peak wasm memory growth, not just transfer
  speed

### No-Harm Checks

- no regression for small queries due to over-engineered transport
- no native engine changes that worsen current row materialization cost for
  existing bindings

## S8. Export / Import / Persistence Contract

**Status:** `blocked`

### Goal

Define and implement safe backup/restore behavior for browser-hosted databases.

### Why This Matters

OPFS persistence is not a substitute for explicit backup/export. Also, a WAL
database cannot be treated as a single raw DB file safely unless checkpoint
semantics are respected.

### Required Decisions

- whether `export()` forces a checkpoint first
- whether export format is:
  - main DB file only after checkpoint
  - DB+WAL bundle
  - both, with one as the default
- whether import replaces live handles or requires closed DB

### Recommended Initial Behavior

- `export()` defaults to a safe checkpointed export unless explicitly documented
  otherwise
- `import()` is explicit and replaces the target DB atomically from the API
  perspective
- `persist()` wraps `navigator.storage.persist()` and reports the actual result

### Deliverables

- safe export behavior
- safe import behavior
- persistence helper
- documentation for eviction/quota caveats

### Validation

- export after writes
- import then reopen
- export after WAL-heavy session
- persistence helper smoke tests

### No-Harm Checks

- no misleading "durable forever" API language
- no export path that omits committed WAL state silently

## S9. Browser Quality, Performance, And CI Coverage

**Status:** `blocked`

### Goal

Establish browser-specific validation without violating the fast-feedback PRD
tenet.

### Recommended Test Stack

Use two layers that match the repository's tiered testing philosophy:

#### Layer 1: `wasm-bindgen-test` For Fast Rust-Side WASM Checks

Purpose:

- fast wasm correctness checks in PR CI
- Rust-native tests for wasm-targeted engine and VFS behavior

Recommended coverage:

- wasm-targeted engine smoke tests
- worker-independent browser/wasm helper tests where feasible
- non-OPFS wasm correctness checks
- narrow VFS/engine reopen logic that can run without full browser orchestration

Expected usage:

- Rust tests under the browser/wasm crate
- run through a wasm test workflow such as `wasm-pack test --node` for very fast
  coverage and browser-targeted variants where appropriate

Constraint:

- this layer is not sufficient for real OPFS validation because Node.js does not
  provide the actual browser OPFS implementation

#### Layer 2: Playwright For Real Browser / OPFS Validation

Purpose:

- real browser coverage for worker + OPFS behavior
- PR smoke tests and broader nightly browser matrix coverage

Why Playwright is the preferred default:

- practical cross-browser support
- exercises real browser worker and OPFS implementations
- good CI ergonomics for retries, traces, and matrix execution
- fits naturally with the repository's existing JS-based binding test tooling

Recommended placement:

- `tests/bindings/web/` for browser-facing smoke/integration tests, or an
  equivalent clearly named browser test directory if the final package layout
  makes another location cleaner

If another runner is chosen, S0 must justify the trade-off explicitly.

### Test Layers

#### Fast PR Coverage

- wasm compile check
- fast `wasm-bindgen-test` suite
- one supported browser smoke suite
- create/open/query/reopen test
- export/import smoke test
- package type/build verification

#### Nightly Coverage

- larger browser matrix
- broader Playwright browser matrix
- recovery tests
- WAL/checkpoint tests under browser runtime
- larger result transport benchmarks
- storage persistence and quota-related tests where automatable

### Browser-Specific Test Categories

- worker bootstrap correctness
- OPFS read/write/reopen
- crash-like recovery by terminating worker and reopening
- statement lifecycle leaks
- large result transport
- package size tracking

### Exit Criteria

- PR suite is fast and deterministic
- nightly suite covers heavy browser-specific cases
- failures produce actionable logs

### No-Harm Checks

- PR duration remains aligned with the under-10-minute target
- browser jobs do not become a mandatory long-running bottleneck

## S10. Documentation, User-Facing Guides, Examples, And Release Hardening

**Status:** `blocked`

### Goal

Bring the browser binding up to first-class documentation quality.

This slice explicitly includes user-facing developer documentation in the main
`docs/` site. Browser support is not complete if it exists only in design notes
or package READMEs.

### Required Docs

These are required content areas for v1, not necessarily separate pages:

- user-facing browser/WASM guide in the main docs site
- browser quick start
- architecture overview
- compatibility matrix
- durability model
- export/import guide
- persistence and quota guide
- troubleshooting guide
- framework integration guide with frontend examples
- API reference page for the web/browser binding

### Recommended Docs Placement

Given the current documentation layout, the recommended initial placement is:

- `docs/api/wasm.md`
  - primary developer-facing browser/WASM guide
  - setup, lifecycle, persistence, export/import, performance tips, framework
    integration notes, and troubleshooting
- optional follow-on pages if the guide becomes too large:
  - `docs/api/wasm-examples.md`
  - `docs/api/wasm-troubleshooting.md`

Recommended `mkdocs.yml` navigation additions:

- `API Reference -> WASM / Browser`

This is the preferred home because the browser surface is an official binding
API, and the document should be easy for developers to find alongside the other
language and platform bindings.

### Required Examples

- in-browser todo app or minimal local-first demo
- explicit export/import example
- prepared statement example
- frontend integration examples covering multiple UI environments

### Required Frontend Example Coverage

The user-facing docs should include examples for at least:

- Vanilla HTML/TypeScript
- React
- Vue or Nuxt client-side usage
- Svelte or SvelteKit client-side usage

The goal is not to maintain a large matrix of full applications. The goal is to
show developers the integration points they immediately care about:

- where the worker is hidden
- how the DB is opened
- how lifecycle/cleanup works
- how queries are performed
- how export/import works
- what to do with persistence requests

These examples do not all need to be separate pages in v1. They may initially
ship as sections or embedded examples inside `docs/api/wasm.md`, with later
extraction into separate pages only if the document becomes too large.

### Required Developer-Focused Content

The user-facing browser docs must be written for application developers, not
just engine contributors. They should include:

- when to use DecentDB in the browser vs native/Node
- supported environments and current limitations
- storage persistence and eviction caveats in plain language
- guidance for offline/local-first application design
- performance tips for large reads and batched writes
- framework lifecycle tips
  - avoiding repeated DB initialization
  - handling hot reload/dev mode safely
  - cleaning up DB handles on app teardown when appropriate
- guidance on backups/sync strategies
- debugging and troubleshooting tips
  - worker startup failures
  - unsupported browser behavior
  - quota/persistence failures
  - import/export mismatches
- anti-patterns to avoid
  - opening many short-lived DB instances
  - assuming OPFS data is immortal
  - serializing massive result sets to JSON unnecessarily

### Exit Criteria

- docs match shipped APIs exactly
- examples are tested or at minimum build-verified
- limitation statements are easy to find
- the main `docs/` site contains a developer-facing browser guide at
  `docs/api/wasm.md`
- the docs include multiple frontend integration examples and practical
  troubleshooting content

## 11. Quality Gates

No slice is done because "it works on one laptop." The following quality gates
must exist before this feature is considered production-ready.

### 11.1 Rust Quality Gates

Required commands:

- `cargo fmt --check`
- `cargo clippy --workspace --all-targets --all-features`
- `cargo test -p decentdb`
- `cargo check -p decentdb --target wasm32-unknown-unknown`

Additional recommended commands:

- target-specific feature checks for native-only and wasm-only code paths
- focused benchmark smoke runs for touched hot paths

### 11.2 Web Package Quality Gates

Introduce and require equivalent checks for the browser package:

- package install/build check
- TypeScript type check if TypeScript is used
- formatter check for JS/TS sources
- fast `wasm-bindgen-test` coverage for wasm-targeted Rust code
- smoke browser integration tests
- Playwright-based real-browser smoke coverage for OPFS behavior

If a formatter/linter is introduced, it must be deterministic and fast. Do not
add slow or noisy tooling that violates PRD Tenet 7.

### 11.3 Markdown / Design Quality Gates

Before implementation slices are merged:

- ADR accepted
- this implementation plan updated to reflect actual slice status
- docs updated alongside code, not after

## 12. Performance Guardrails: "Cause No Harm"

This section is mandatory. Browser support is not allowed to trade away native
engine excellence.

### 12.1 Native Hot Path Guardrails

The following must remain true:

- native file I/O still goes through `OsVfs` with no browser-specific overhead
- WAL append path does not gain additional abstractions on native targets
- page cache and snapshot reads do not gain browser-related runtime branching
- C ABI behavior and cost remain stable on native targets

### 12.2 Build And Dependency Guardrails

- browser dependencies must be target-gated where possible
- avoid inflating native compile times materially
- avoid introducing a dependency that forces all targets through heavy web tool
  chains

### 12.3 Performance Metrics To Track

Track before and after each major slice:

- native point-read latency
- native range-read latency
- native write/commit latency
- native checkpoint latency
- native memory usage under representative workloads
- wasm binary size
- browser package bundle size
- browser query latency for small and large result sets
- worker startup latency

### 12.4 Regression Policy

If a slice introduces a meaningful native regression, the default action is:

- stop
- measure
- isolate the regression
- fix or redesign before continuing

Do not "accept now, optimize later" on native hot paths.

## 13. Durability And Recovery Contract For Browser Support

This section should be copied into the ADR once finalized.

### 13.1 What The Browser Binding Must Promise

- committed data is written according to DecentDB's configured sync behavior as
  mapped onto OPFS flush semantics
- the engine will recover from interrupted sessions using the same WAL recovery
  model where the storage substrate preserved the written bytes
- export/import tools exist so applications can make explicit backups

### 13.2 What The Browser Binding Must Not Promise

- immunity from browser storage eviction in best-effort storage mode
- native OS power-loss semantics
- permanent retention without user/browser intervention

### 13.3 Required User-Facing Documentation

The docs must explain:

- OPFS is the primary persistence backend
- persistent storage may need to be requested explicitly
- eviction behavior varies by browser policy
- important data should be exported/synced explicitly

## 14. CI Strategy

### 14.1 PR Checks

Keep PR checks narrow:

- rust format/lint/test as already required
- wasm compile check
- one browser smoke suite
- package build/type smoke

### 14.2 Nightly Checks

Move expensive work out of the PR path:

- larger browser matrix
- longer result transport benchmarks
- repeated reopen/recovery loops
- storage pressure and persistence-adjacent tests where feasible

### 14.3 Failure Reporting

Browser failures are often harder to diagnose than native unit tests. CI output
must preserve:

- browser version
- wasm build hash/version
- worker-side logs
- failing SQL / operation name
- storage backend state where possible

## 15. Recommended Repository Layout

The exact final layout can change, but this is a reasonable starting shape:

- `crates/decentdb/`
- `crates/decentdb-web/`
- `crates/decentdb-web/src/opfs_vfs.rs`
- `crates/decentdb-web/src/worker_runtime.rs`
- `bindings/web/package.json`
- `bindings/web/src/index.ts`
- `bindings/web/src/worker.ts`
- `bindings/web/src/protocol.ts`
- `bindings/web/tests/`
- `docs/` entries for browser support

Recommended placement rule:

- browser-specific JS interop and OPFS handle management should live in the
  dedicated browser/wasm crate, not directly in the core engine crate, unless a
  later implementation can prove that doing otherwise does not pull browser
  dependencies into native builds

This should be implemented without forcing existing native bindings to change
their architecture.

## 16. Open Questions

These must be answered in S0 or S1:

1. Does the current parser stack compile to `wasm32-unknown-unknown` cleanly?
2. What minimal hooks must the core crate expose so the dedicated browser/wasm
   crate can implement OPFS and worker integration without leaking browser
   dependencies into native builds?
3. What is the exact mapping of `WalSyncMode::Full` and `WalSyncMode::Normal` to
   OPFS `flush()` behavior?
4. What browser support matrix is officially promised for the first release?
5. Should `export()` produce a checkpointed single-file export, a DB+WAL bundle,
   or both?
6. What bundle size budget is acceptable for the first public browser package?
7. What exact CI split should be used between `wasm-bindgen-test` and
   Playwright so PR coverage stays fast while OPFS coverage stays meaningful?

## 17. Recommended First Milestone

The first milestone should not try to ship the entire browser story. It should
aim for:

- ADR accepted
- core compiles to wasm
- parser path validated for wasm
- `OpfsVfs` supports create/open/query/reopen in a worker
- minimal browser smoke suite passes

That milestone is enough to prove the architecture is viable without prematurely
optimizing transport or over-committing to public API breadth.

## 18. Definition Of Done For Browser Support v1

Browser support v1 is done only when all of the following are true:

- the ADR is accepted
- the wasm target builds cleanly
- OPFS-backed persistence and recovery are tested
- the public browser package exists and is documented
- export/import and persistence helpers are implemented
- browser PR smoke checks are green
- nightly browser validation exists
- native performance has not regressed materially
- docs clearly explain durability limits and supported environments

## 19. Final Recommendation

Proceed, but only as an explicitly gated, slice-based effort.

The correct implementation strategy is:

- preserve the synchronous Rust core
- use the VFS seam
- run in a Dedicated Worker
- treat OPFS as the primary browser storage backend
- expose an async package API
- validate parser feasibility first
- enforce "no harm" performance rules for native builds

If the parser path or durability contract cannot be made crisp and defensible,
the project should stop at the relevant gate rather than shipping a misleading
"browser support" claim.
