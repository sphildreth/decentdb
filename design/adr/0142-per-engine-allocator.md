# Per-Engine Allocator (Decoupled from Host `#[global_allocator]`)
**Date:** 2026-04-22
**Status:** Superseded (2026-04-29) — the unused `EngineAlloc`/`EngineByteBuf` scaffold was removed in favor of plain `Vec<u8>` buffers plus targeted reuse where measurements justify it.

### Supersession note

The accepted scaffold never became a production allocator boundary: every call
site used the default forwarder to Rust's global allocator, and the only
production hot path holding `EngineByteBuf` was the WAL writer scratch state.
Slice S7 retired the scaffold and restored ordinary `Vec<u8>` buffers there.
Future per-engine allocation work should start from fresh measurements and a
new ADR instead of reviving this unused unsafe abstraction by default.

### Decision (proposed)

Route DecentDB's hot-path allocations (WAL frame buffers, WAL index entries,
page-cache slots, query-plan scratch) through an engine-owned allocator
trait rather than the process's `#[global_allocator]`. The default
implementation forwards to the global allocator (zero-cost), so embedders
who do nothing see no change. Embedders who opt in can wire a per-engine
arena, slab, or third-party allocator (mimalloc, jemalloc) without
imposing a process-wide `#[global_allocator]` on every consumer.

This ADR is explicitly **Deferred** until ADRs 0138 (`malloc_trim`),
0139 (`mimalloc` opt-in for the CLI), and 0140 (discriminated WAL payload)
have shipped and been measured. Those three changes address the dominant
allocator-related symptoms; this ADR addresses the residual case where
embedders need allocator policy independent of their process-wide choice.

### Rationale

Bindings ship `cdylib` outputs and cannot dictate the host process's
`#[global_allocator]`. Server embedders want to choose mimalloc/jemalloc
process-wide, but library users (binding consumers, mixed-language
applications) cannot. A per-engine allocator boundary lets DecentDB
benefit from a low-fragmentation allocator on the database's hot path
*regardless of host policy*, which is the only fix that scales across all
embedding scenarios.

The diagnostic probe in
[`design/2026-04-25.ENGINE-MEMORY-WORK.md`](../2026-04-25.ENGINE-MEMORY-WORK.md)
shows that the dominant cost is small-allocation churn on per-commit
buffers and WAL index entries. Routing those through an engine-owned
allocator with arena recycling would eliminate the churn at the source,
not just mitigate its OS-visible effects.

### Why Deferred

- ADRs 0137, 0138, 0140 together are projected to reduce peak RSS by
  ~20× on the workloads measured. The engineering cost of a per-engine
  allocator boundary (auditing every allocation site in the engine) is
  substantial; we defer until empirical data shows the residual gap
  warrants it.
- The Rust ecosystem's per-data-structure allocator support is still
  stabilizing (`Allocator` trait is unstable on stable Rust). A custom
  engine allocator would require either staying on stable with `unsafe`
  newtype wrappers, or pinning to nightly. Both have downstream cost.
- Aligning with `crossbeam`/`bumpalo`/`typed-arena` ecosystem patterns is
  preferable to bespoke design; that picture will be clearer in 6–12
  months.

### Open Questions

- **Granularity.** One allocator per `Db`, or one per WAL/pager/exec
  subsystem? Per-`Db` is simpler; per-subsystem is more flexible.
- **Trait shape.** Bespoke `EngineAlloc` trait, or wait for stable
  `Allocator`?
- **Interop with `Arc<[u8]>`.** `Arc::new_in(...)` requires the unstable
  `Allocator` trait. Workaround: use `Box<[u8], A>` + manual ref counting
  for hot-path payloads.
- **Default behavior.** Forward to global allocator (zero-cost) — agreed.
- **CLI default.** Likely mimalloc per ADR 0139; unchanged by this ADR.

### Out of Scope

- Forcing any specific allocator on bindings. Bindings remain free to use
  the host's global allocator.
- Changing the C ABI. The engine allocator is internal; the C ABI
  continues to allocate via the host's `malloc`/`free` and DecentDB
  continues to use `Db::free_*` callbacks for cross-ABI ownership.

### References

- design/2026-04-25.ENGINE-MEMORY-WORK.md (Phase 4)
- design/adr/0011-memory-management-strategy.md
- design/adr/0025-memory-leak-prevention-strategy.md
- design/adr/0138-post-checkpoint-heap-release.md
- design/adr/0139-optional-mimalloc-feature-for-cli.md
- design/adr/0140-walversion-discriminated-payload.md
- Rust unstable feature: `allocator_api`
