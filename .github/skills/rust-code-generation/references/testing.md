# Rust Testing And Validation Reference

Use this reference when adding, updating, or reviewing tests for Rust engine changes.

## Defaults

- No behavior change is complete without targeted tests.
- Prefer the smallest test surface that proves the changed behavior.
- Keep failures deterministic and reproducible.
- Pair code changes with validation that matches the risk of the change.

## Repository-Specific Guidance

- DecentDB uses layered validation: fast unit tests, property tests, crash-injection testing, differential testing, and binding validation where relevant.
- For engine work, correctness and durability matter more than feature velocity.
- If a change affects shared semantics, C ABI behavior, or bindings, include the corresponding smoke or higher-level validation.
- If a change affects WAL, storage, checkpointing, recovery, or concurrency guarantees, treat crash, invariant, or stress validation as part of the work rather than optional follow-up.

## Prefer

- focused unit tests next to the changed code
- integration tests for cross-module or public behavior
- property tests for invariants and randomized operation sequences when edge-space is large
- crash-injection or harness validation for durability-sensitive changes
- binding smoke tests when behavior crosses the C ABI boundary
- descriptive test names and arrange/act/assert structure

## Validation Layers

Choose the smallest relevant set:

- unit tests for local logic and invariants
- integration tests for end-to-end module behavior
- property tests for ordering, equivalence, and constraint invariants
- crash tests for WAL and recovery-sensitive changes
- differential tests where behavior is compared to a reference engine for supported SQL subsets
- binding tests when language integrations may be affected

## Typical Triggers

- pager, WAL, B+Tree, or record format changes: unit tests plus recovery- or invariant-oriented validation
- concurrency or snapshot changes: race-sensitive or long-running transaction coverage
- C ABI or binding-facing changes: binding smoke tests, and full binding validation when semantics shift
- user-visible SQL behavior changes: integration coverage and docs updates

## Baseline Commands

- `cargo fmt --check`
- `cargo check`
- `cargo clippy --all-targets --all-features -- -D warnings`
- targeted Rust tests for the touched area

Add more when required by the surface area of the change.

## Avoid

- merging behavior changes with no new coverage
- relying only on manual testing for engine correctness
- skipping binding checks for shared-native-boundary changes
- adding broad slow suites when a targeted regression test would prove the fix
- treating crash and durability validation as optional for WAL-sensitive work

## Quick Checks

- What invariant or regression does this test prove?
- Is the smallest effective layer being used?
- Does the changed surface require binding, crash, or differential validation?
- Would a failure be reproducible from logs, seed, or deterministic inputs?