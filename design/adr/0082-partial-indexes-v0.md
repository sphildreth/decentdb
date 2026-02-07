## Partial Indexes (v0)
**Date:** 2026-02-07
**Status:** Accepted

### Decision

Implement v0 partial indexes with a constrained, low-risk surface:

1. Supported syntax and scope
- Support `CREATE INDEX ... WHERE ...` for B-tree indexes only.
- v0 partial predicate form is restricted to:
  - `<indexed_column> IS NOT NULL`
- Predicate column must be the single indexed column.

2. Unsupported in v0
- Trigram partial indexes.
- Multi-column partial indexes.
- `UNIQUE` partial indexes.
- Arbitrary predicates (`=`, ranges, function calls, subqueries, params, etc.).

3. Semantics
- A row is indexed only when the predicate evaluates to `TRUE`.
- For the v0 predicate shape, this means rows are indexed only when the indexed column is non-NULL.
- Rows that do not satisfy the predicate are absent from the index and do not participate in index maintenance.

4. Planner behavior
- Existing equality seek planning on the indexed column may use the partial index.
- Broader predicate implication reasoning is deferred.

5. Durability/persistence
- Persist optional index predicate SQL in index catalog metadata.
- Keep catalog decoding backward compatible for older index records without predicate metadata.
- No WAL/page/checkpoint format changes.

### Rationale

- Roadmap section 5.6 requires partial indexes and ADR-gated predicate semantics.
- Narrowing v0 to `IS NOT NULL` on the indexed column keeps implementation explicit and testable while delivering practical value.
- This avoids high-risk planner implication logic and complex row-level predicate evaluators in storage paths.

### Alternatives Considered

1. General arbitrary partial-index predicates in v0
- Rejected due higher correctness risk and planner complexity.

2. Support partial + unique in v0
- Rejected to avoid additional uniqueness-edge semantics in this slice.

3. Defer all partial-index support
- Rejected; roadmap requires partial indexes in the advanced-index sequence.

### Trade-offs

- v0 partial indexes are intentionally limited and less expressive than PostgreSQL.
- Some practical predicates must wait for a later ADR/slice.
- Planner remains conservative and does not perform general implication proofs.

### References

- Roadmap: `design/SQL_ENHANCEMENTS_PLAN.md` (Section 5.6)
- Existing index baseline: `design/SPEC.md`
