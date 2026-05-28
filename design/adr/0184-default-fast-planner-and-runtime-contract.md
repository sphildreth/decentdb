# Default-Fast Planner And Runtime Contract
**Date:** 2026-05-27
**Status:** Accepted

### Decision

Default-fast performance work will prioritize measured improvements to the
existing durable default profile before accepting persistent-format, WAL
semantic, or broad binding-contract changes.

The default profile must remain durable. `WalSyncMode::Full` and strict durable
acknowledgement behavior must not be weakened to improve default benchmark
charts. Relaxed, async, unsafe, or tuned modes may continue to exist only as
explicitly named profiles or caller-selected options.

Planner and executor optimizations in this track must be deterministic for a
given schema, statistics state, and SQL statement. Adaptive behavior is allowed
only when it preserves prepared-statement predictability and has explicit
invalidation rules. Schema-cookie changes must invalidate prepared plans that
depend on catalog, index, or statistics metadata.

DecentDB will use existing `IndexSchema.include_columns` metadata for covering
B+Tree execution where the executor can prove that all values required by the
query are available without fetching a base row. The initial covering-index
work must not require a file-format change. It may use in-memory runtime index
payloads derived from the base table and existing catalog metadata. Persistent
covering-index payload encoding, key-prefix compression, or altered B+Tree page
layout requires a separate ADR.

Covering-index execution is allowed only for fresh B+Tree indexes and only when
all of these are true:

- the query's required columns are a subset of index key columns, included
  columns, and row-id metadata already tracked for the index;
- expression evaluation can be satisfied from those values without reading the
  base row;
- row policies, column masks, generated columns, constraints, and partial-index
  predicates cannot change the visible result compared with the ordinary base
  row path;
- transaction-local writes, branch state, and snapshot visibility are handled
  with the same semantics as the ordinary indexed lookup path;
- stale, missing, or unverifiable index state falls back to the base-row path or
  returns an existing clear stale-index error.

The planner must remain conservative. If a query cannot be proven covered, the
executor must fetch the base row or choose the existing non-covering plan.

`ANALYZE` statistics may influence index-vs-scan decisions, join ordering where
supported, and covering-index preference, but missing statistics must not make
ordinary indexed lookups unexpectedly slow by default. The default heuristic
may prefer a fresh unique or primary-key-equivalent index without requiring
`ANALYZE`. New persisted statistics fields require format-version and migration
analysis before implementation.

Plan caching may cache parsed statements, normalized metadata references, and
prepared execution plans when keyed by schema cookie, temporary schema cookie,
connection-local state that affects visibility, and any accepted statistics
generation. Cached plans must not outlive catalog changes, temp schema changes,
policy/mask changes, extension function changes that affect execution, or other
state that can change query semantics.

Default configuration tuning is acceptable without a new ADR when it changes
only open-time memory/cache/checkpoint thresholds and preserves documented
durability, recovery, and public API semantics. Tuning must be benchmarked under
explicit profile names and documented when it changes default memory behavior.

### Rationale

The roadmap goal is to make safe defaults fast, not to make DecentDB look fast
by quietly switching to tuned or weaker durability modes. The current benchmark
gap between default durable and tuned durable is large enough that first-phase
work should target known safe levers: cache behavior, checkpoint policy,
cold-open work, planner choices, and prepared-statement hot paths.

Covering-index execution is a high-leverage planner/executor win because the
catalog already records included columns and compatibility introspection exposes
that metadata. Using that metadata avoids unnecessary base-row materialization
for narrow projections, range scans, and joins. The important boundary is that
the executor must not skip visibility, policy, mask, generated-column, or stale
index semantics merely because an index appears to contain enough values.

Keeping persistent covering payloads and compression out of this ADR prevents a
performance roadmap item from silently becoming a storage-format decision.
Those changes may be worthwhile, but they need concrete encodings, crash
recovery rules, migration coverage, and benchmark proof.

### Alternatives Considered

1. **Make the tuned durable profile the default.** Rejected. Some tuned options
   increase memory, change checkpoint behavior, or disable default paged-row
   storage choices. The default must be selected deliberately, not copied from a
   benchmark profile.
2. **Use covering indexes whenever `INCLUDE` metadata names projected columns.**
   Rejected. Policies, masks, generated columns, partial indexes, stale indexes,
   and transaction-local state can make that unsound.
3. **Add persistent covering-index payloads immediately.** Rejected for this
   ADR. It may require a file-format change and must be evaluated with storage,
   WAL, recovery, TDE, and migration obligations.
4. **Require `ANALYZE` for good default query plans.** Rejected. `ANALYZE`
   should improve plans, but ordinary indexed point and range reads should not
   require a tuning ritual.
5. **Use adaptive runtime plan changes without stable invalidation.** Rejected.
   Prepared-statement workloads need predictable latency and correctness across
   schema changes.

### Trade-offs

- Conservative covering-index selection leaves some possible wins on the table
  until more query shapes are proven safe.
- Runtime-only covering payloads may improve latency before they improve file
  size. Persistent storage efficiency may still need later format work.
- More explicit plan invalidation adds implementation complexity but prevents
  stale prepared plans.
- Keeping `ANALYZE` optional may require better heuristics than a pure
  cost-based planner with missing stats.

### Consequences

- The default-fast WIN spec must treat benchmark profiles as part of the public
  performance contract.
- Covering-index implementation must include tests for safe coverage and
  fallback cases.
- `EXPLAIN` and `EXPLAIN ANALYZE` should report covering-index fast paths when
  they are selected.
- Any persistent covering payload, page compression, key-prefix compression,
  layout rewrite, or new persisted statistics field needs a follow-up ADR.
- Documentation must avoid telling users to weaken durability for normal
  performance.

### References

- `design/WIN_DEFAULT_FAST_PERFORMANCE_STORAGE_EFFICIENCY_SPEC.md`
- `design/FUTURE_WINS.md`
- `design/BENCHMARKING_GUIDE.md`
- `design/adr/0131-legacy-format-migrations.md`
- `design/adr/0143-on-disk-row-scan-executor.md`
- `design/adr/0144-persistent-primary-key-index.md`
- `design/adr/0145-paged-table-row-source.md`
- `design/adr/0162-engine-owned-write-queue-strict-group-commit.md`
- `design/adr/0163-operational-sys-metrics.md`
- `docs/user-guide/performance.md`
