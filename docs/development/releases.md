# Releases

Release readiness is gated through the Phase 4 workflows, benchmark binary, and
binding matrix.

## Triggering the GitHub release workflow

The GitHub release workflow in `.github/workflows/release.yml` auto-starts when
a version tag is pushed to the repository:

```bash
git push origin vX.Y.Z
```

That workflow currently listens to `push` events for `v*` tags. Creating a tag
or publishing a release from the GitHub UI does not reliably go through that
same event path, so if a tag is created server-side you may need to use
`workflow_dispatch` to run the release pipeline manually.

NuGet package publication stays on the tag-triggered workflow in
`.github/workflows/nuget.yml`, so the normal publish history remains
tag-oriented.

For manual recovery or validation of an existing tag, use the separate workflow
in `.github/workflows/nuget-manual.yml` from `main`, with:

- `release_tag` set to the existing release tag, such as `v2.1.0`
- `publish_to_nuget` left at `false` for a safe dry run that builds, packs, and
  verifies package contents without publishing

Manual runs use the selected `release_tag` in the workflow run title and stay in
their own workflow history instead of appearing under the primary tag-publish
workflow.

Set `publish_to_nuget` to `true` only when you intentionally want that manual
run to push packages to NuGet.org.

## CI lanes

- `CI`: `.github/workflows/ci.yml`
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

1. `CI` is green on `main`.
2. The nightly soak and benchmark jobs are green.
3. The benchmark output is captured for the named workloads.
4. The binding matrix remains green.
5. The docs build cleanly.
