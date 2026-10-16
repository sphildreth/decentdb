# ADR 0218: Stable Dependency and LTS Toolchain Refresh

**Date:** 2026-10-16
**Status:** Accepted

## Context

The repository contains independent Rust workspaces, language bindings, examples,
benchmarks, documentation tooling, native integrations, and GitHub Actions with
inconsistent dependency and runtime versions. The maintenance requirement is to
use current stable packages and prefer supported LTS runtime releases, resolve
build warnings, and validate every supported platform.

## Decision

- Inventory all tracked dependency declarations, including build, development,
  test, benchmark, documentation, and workflow tools. Ignore generated caches and
  untracked local agent packages. Resolve stable releases only; use compatible
  transitive resolutions and record upstream version constraints.
- Upgrade existing dependency families, adapting APIs at their existing owned
  boundaries. Preserve the engine's C ABI, database and WAL formats, encryption
  algorithm and byte encoding, Lua 5.4 sandbox semantics, durability barriers,
  and one-writer/many-reader contract. A dependency upgrade does not authorize
  a new persisted format or a replacement parser/runtime architecture.
- Use Rust stable, Node 24 LTS, .NET 10 LTS, Java 25 LTS for build tooling,
  current stable Go and Flutter/Dart, and current stable Python. Consumer
  runtime minimums must cover the requirements of the selected dependencies;
  Java bytecode targets must remain compatible with supported host applications.
- Keep Entity Framework package versions aligned and the wasm-bindgen CLI at
  exactly the crate's resolved version. Refresh independent Rust, Go, npm, and
  Dart lockfiles alongside the declaring manifests.
- Fix compiler, restore, analyzer, and deprecation warnings at their sources.
  Keep security audits and meaningful validation enabled. Audit existing warning
  suppression flags rather than adding blanket suppressions.
- Extend validation for projects outside primary workspaces/solutions and for
  Flutter/native integrations. Require the complete paranoid pre-commit suite
  locally and publish platform validation to GitHub only after explicit approval
  for the required git writes. Cross-platform validation must build without
  publishing packages or creating a release.

## Validation

Capture the starting pre-commit results and dependency inventory. Exercise
legacy encrypted data and signature compatibility when cryptography dependencies
change. Run formatting, Clippy with denied warnings, native/WASM compilation,
all binding suites and packaging checks, independent workspaces, crash/recovery,
and benchmark guardrails. Run the complete pre-commit script with no failures
and account for any skipped checks. Validate Linux x64/arm64, Windows x64,
macOS x64/arm64, Android, and iOS on suitable GitHub runners before completion.

## Consequences

Major package updates may require source and test-framework migrations and
higher consumer minimums. Some transitive versions remain constrained by upstream
packages; the final inventory must distinguish those from missed direct updates.
No persistent-format, WAL, concurrency, or public C ABI change is introduced.
