# Rust Performance And Allocation Reference

Use this reference when the task involves hot paths, excessive allocation, unnecessary cloning, lock contention, or measurable execution regressions.

## Defaults

- Preserve correctness and durability before chasing speed.
- Prefer simple, explicit optimizations over clever rewrites.
- Measure before and after when making non-trivial performance claims.
- Optimize the narrowest hot path rather than broad surrounding code.

## Repository-Specific Guidance

- In DecentDB, storage correctness, WAL behavior, ABI stability, and predictable semantics matter more than microbench wins.
- Favor targeted improvements in planner, executor, storage, cache, and index paths over speculative global rewrites.
- Avoid adding specialized crates or allocators unless the benefit is measured and the dependency cost is justified.
- If a performance change alters concurrency, I/O ordering, layout, or public semantics, treat it as a compatibility-sensitive change.

## Prefer

- borrowing instead of cloning on hot paths
- `with_capacity()` when expected sizes are known
- reusing existing collections when loops would otherwise reallocate repeatedly
- simpler data access paths that reduce branching, locking, or copying
- iterators and straightforward loops that avoid redundant bounds checks and intermediate collections
- profiling and benchmark evidence before keeping a more complex optimization

## Avoid

- introducing `smallvec`, arena allocators, or other specialized containers without measured justification
- premature inlining, hinting, or low-level tuning without evidence
- broad refactors justified only by assumed speedups
- optimizing benchmark-visible behavior by weakening correctness or durability constraints
- claiming performance improvement without validating the affected path

## Quick Checks

- Is there a measured hotspot or just a hunch?
- Did this change remove unnecessary allocation, copying, or locking?
- Is the optimized code still easy to reason about?
- Did validation include the narrowest benchmark or test that exercises the hot path?
- Could the same gain be achieved with a smaller change?