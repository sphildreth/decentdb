# Post-Checkpoint Heap Release on Linux/glibc
**Date:** 2026-04-22
**Status:** Accepted

### Decision

After a successful checkpoint, when running on Linux with the GNU C library,
call `malloc_trim(0)` to return freed heap arenas to the operating system.
The behavior is gated by `DbConfig::release_freed_memory_after_checkpoint:
bool` and by `cfg(all(target_os = "linux", target_env = "gnu"))`. The
default value of the config field is `true` on Linux/glibc and `false`
elsewhere; the field exists on every platform so embedders have a uniform
knob.

The call is invoked from `wal::checkpoint::checkpoint()` after the WAL
index is cleared/pruned and after every WAL lock is released. It is
best-effort: the return value is ignored. It is performed via direct
`extern "C"` declaration to avoid pulling in `libc` if it is not already
in the dependency graph.

On non-Linux/non-glibc targets (musl, macOS, Windows, BSD) the helper
compiles to an inline no-op regardless of the config field's value.

### Rationale

The diagnostic probe captured in
[`design/2026-04-22.ENGINE-MEMORY-PLAN.md`](../2026-04-22.ENGINE-MEMORY-PLAN.md)
showed that after dropping the `Db` and freeing all engine state, RSS
remained at 1.08 GB on a 5 M-row workload. A single `malloc_trim(0)` call
released 1.08 GB → 8.8 MB. The engine has no leak; glibc retains freed
heap in per-arena freelists and rarely returns it to the OS during
sustained allocation churn.

Sustained-write workloads against DecentDB issue many small allocations
per commit (frame batch buffers, prepared-page payloads, WAL index
entries). Even after ADR 0137 bounds in-memory WAL state, the *transient*
allocations made during inter-checkpoint windows accumulate as glibc
fragmentation and present as steadily-growing RSS.

A post-checkpoint trim is the right hook because:

- it runs at the natural high-water mark (immediately after the engine
  has freed the largest single block, the WAL index pages);
- it runs at most once per checkpoint (bounded frequency);
- it runs on the writer thread with no engine locks held (no contention);
- its cost (`O(arena_count)` walk over freelists) is negligible compared
  to the fsync work the checkpoint just performed.

### Alternatives Considered

- **Switch the default global allocator to mimalloc/jemalloc.** Effective
  but invasive — embedders should choose their own `#[global_allocator]`.
  ADR 0139 makes this opt-in for the CLI binary; it does not change the
  library default.
- **Periodic trim on a timer.** Adds a background thread or signal handler
  for a benefit the checkpoint hook already provides. Rejected as
  unnecessary complexity.
- **Trim from `Db::drop`.** Only helps short-lived embedders; production
  workloads keep the `Db` open for the process lifetime.
- **Use `mallopt(M_MMAP_THRESHOLD, ...)`.** Forces large allocations to use
  `mmap` (auto-released on free) but doesn't help the small-allocation
  churn that dominates DecentDB's footprint.

### Trade-offs

- **Pros:** eliminates the dominant cause of observed RSS growth on the
  most common deployment target (Linux/glibc); zero impact on other
  platforms; one-line opt-out via `DbConfig`; no new dependency; no
  `unsafe` outside the single FFI declaration; no on-disk format change.
- **Cons:** adds a brief CPU spike at checkpoint time (typically
  sub-millisecond for the workloads measured); on workloads that
  *immediately* re-allocate after a checkpoint (rare), the OS may
  re-page the same memory back in. The latter case is bounded by the
  page-cache capacity (`cache_size_mb`).
- **Safety:** `malloc_trim` is documented as safe to call from any
  thread; DecentDB's single-writer model means we never call it
  concurrently from multiple threads.

### Implementation Notes

- Helper module:

  ```rust
  pub(crate) mod platform {
      #[cfg(all(target_os = "linux", target_env = "gnu"))]
      pub(crate) fn release_freed() {
          unsafe extern "C" {
              fn malloc_trim(pad: usize) -> i32;
          }
          unsafe { malloc_trim(0) };
      }

      #[cfg(not(all(target_os = "linux", target_env = "gnu")))]
      pub(crate) fn release_freed() {}
  }
  ```

- Call site is the very last statement in `checkpoint::checkpoint()`,
  guarded by `if cfg.release_freed_memory_after_checkpoint`.

### References

- design/2026-04-22.ENGINE-MEMORY-PLAN.md (slice M2)
- design/adr/0011-memory-management-strategy.md
- design/adr/0025-memory-leak-prevention-strategy.md
- design/adr/0137-size-based-auto-checkpoint-trigger.md
- glibc `malloc_trim(3)` man page
