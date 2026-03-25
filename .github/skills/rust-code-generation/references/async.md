# Rust Async And Concurrency Reference

Use this reference when working on async Rust, synchronization, or threaded coordination.

## Defaults

- Keep async boundaries simple and explicit.
- Respect `Send` and `Sync` constraints rather than working around them.
- Avoid spawning tasks unless concurrency is required and justified.
- Keep lock scopes short.
- Release locks before `.await` whenever possible.

## Repository-Specific Guidance

- Preserve the one-writer, many-readers concurrency model.
- Avoid introducing new shared-mutable coordination patterns unless they clearly fit the existing architecture.
- Durability and correctness take priority over speculative concurrency optimizations.

## Prefer

- extracting or cloning only the minimal data needed before `.await`
- using scoped reads and writes with explicit ownership transitions
- documenting thread-safety assumptions when they are not obvious
- targeted tests for races, shutdown, or ordering-sensitive behavior when relevant

## Avoid

- holding `Mutex` or `RwLock` guards across `.await`
- mixing blocking operations into async flows without an explicit boundary
- hidden background task creation that complicates lifecycle or shutdown
- introducing concurrency model changes without surfacing impact

## Quick Checks

- Is any lock held across `.await`?
- Does this change alter writer or reader coordination?
- Would a simpler ownership transfer remove the need for shared mutable state?
- Are concurrency-sensitive invariants documented or test-covered?