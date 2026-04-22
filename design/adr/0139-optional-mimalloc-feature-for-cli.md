# Optional `mimalloc` Allocator for `decentdb-cli`
**Date:** 2026-04-22
**Status:** Accepted

### Decision

Add an optional `mimalloc` feature to the `decentdb-cli` crate (and *only*
that crate). When the feature is enabled at build time, the CLI installs
`mimalloc` as its `#[global_allocator]`. The feature is **off by default**;
production users who want the lower-fragmentation, higher-throughput
allocator opt in via `cargo build -p decentdb-cli --features mimalloc`.

The `decentdb` library crate is **not** touched. Embedders pick their own
`#[global_allocator]` via the standard Rust mechanism in their host binary.
The library remains allocator-agnostic.

`mimalloc` is added as `mimalloc = { version = "0.1", default-features =
false, optional = true }` on `decentdb-cli` only. It is not a dependency
of any other workspace crate, including binding crates and benchmark
crates.

### Rationale

The diagnostic probe in
[`design/2026-04-22.ENGINE-MEMORY-PLAN.md`](../2026-04-22.ENGINE-MEMORY-PLAN.md)
showed that DecentDB's per-commit allocation churn interacts pathologically
with glibc's `malloc`. ADR 0138 mitigates the steady-state retention via
periodic `malloc_trim(0)`, but does not address allocator-internal
fragmentation that occurs *between* checkpoints. mimalloc's segment-based
allocator handles small-allocation churn more efficiently than glibc and
typically reduces sustained-load peak RSS by 30–50 % on the same workload,
independent of and complementary to ADR 0138.

The CLI is the right place for this opt-in because:

- the CLI is the most common surface against which embedders measure
  DecentDB's memory profile (benchmarks, `decentdb-cli import`,
  `decentdb-cli bench`);
- the CLI is a single binary with no library-consumer surprise;
- the library crate stays allocator-agnostic, preserving binding portability
  (binding crates ship `cdylib` outputs and must not impose an allocator).

### Alternatives Considered

- **Make mimalloc the library default.** Rejected — binding crates,
  third-party embedders, and downstream Rust applications must be free to
  pick their own allocator. Dictating one from the library breaks that.
- **Use `jemalloc` instead.** Comparable benefits, but `jemallocator`
  requires building jemalloc from source on most targets and triples
  compile time. mimalloc is header-only-ish and builds quickly.
- **Add `mimalloc` to multiple crates.** Provides no additional benefit
  (`#[global_allocator]` is process-wide) and creates dependency-graph
  noise. Single-crate placement is sufficient.
- **Skip and rely on ADR 0138 alone.** ADR 0138 reduces post-checkpoint
  RSS but does not reduce *peak* in-window RSS. mimalloc reduces both.
  The two ADRs are stacked, not redundant.

### Trade-offs

- **Pros:** measurable RSS reduction independent of platform-specific
  syscalls; no engine code change; opt-in (off by default); contained to a
  single crate; library crate stays allocator-agnostic.
- **Cons:** adds an optional build-time dependency on `mimalloc 0.1`;
  CLI binary size increases by ~200 KB when the feature is enabled;
  requires a working C toolchain on the build host (already required for
  the engine's existing C-side dependencies).
- **License compatibility:** `mimalloc` is MIT, compatible with
  DecentDB's license.

### Implementation Notes

- `crates/decentdb-cli/Cargo.toml`:
  ```toml
  [features]
  mimalloc = ["dep:mimalloc"]

  [dependencies]
  mimalloc = { version = "0.1", default-features = false, optional = true }
  ```
- `crates/decentdb-cli/src/main.rs` (top of file):
  ```rust
  #[cfg(feature = "mimalloc")]
  #[global_allocator]
  static GLOBAL: mimalloc::MiMalloc = mimalloc::MiMalloc;
  ```
- README and `design/BENCHMARKING_GUIDE.md` document the recommended
  build for production CLI usage.
- CI smoke job builds with `--features mimalloc` and runs the existing
  CLI smoke test to prevent bit-rot.

### References

- design/2026-04-22.ENGINE-MEMORY-PLAN.md (slice M3)
- design/adr/0138-post-checkpoint-heap-release.md
- mimalloc upstream: <https://github.com/microsoft/mimalloc>
