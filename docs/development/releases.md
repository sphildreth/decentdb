# Releases

The Rust rewrite now gates release readiness through the Phase 4 workflows, benchmark binary, and binding matrix.

## CI lanes

- `PR Fast`: `.github/workflows/pr-fast.yml`
  - format
  - clippy
  - engine tests
  - CLI tests
  - C ABI binding validation and smoke matrix
  - rustdoc + doctests
- `Nightly Extended`: `.github/workflows/nightly-extended.yml`
  - workspace clippy and tests
  - storage soak harness
  - release benchmark run
  - full binding matrix
  - docs build

## Release benchmark workloads

The benchmark binary lives at:

```text
crates/decentdb/benches/release_metrics.rs
```

It produces named metrics for:
- point lookup
- FK join expansion
- trigram-backed substring search
- bulk load
- crash recovery / reopen

Run locally with:

```bash
cargo bench -p decentdb --bench release_metrics
```

## Binding verification

The Phase 4 compatibility matrix is documented in:

```text
docs/api/bindings-matrix.md
```

Each listed binding is validated or smoke-tested directly against the stable C ABI before release.

## Release checklist

1. `PR Fast` is green on `main`.
2. The nightly soak and benchmark jobs are green.
3. The benchmark output is captured for the named workloads.
4. The binding matrix remains green.
5. The docs build cleanly.
