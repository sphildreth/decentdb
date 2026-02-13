## ADR-0095: EF Core Provider Query Translation M0 Policy
**Date:** 2026-02-13
**Status:** Accepted

### Decision

For Phase 3 (M0 query translation) in `DecentDB.EntityFrameworkCore`:

- Use EF relational query pipeline with provider-specific SQL generation via `IQuerySqlGeneratorFactory`.
- Emit paging as `LIMIT/OFFSET` for translated `Skip`/`Take`.
- Add provider method translators for:
  - `string.Contains(string)`
  - `string.StartsWith(string)`
  - `string.EndsWith(string)`
  using `LIKE` patterns and escaping literal `%`/`_` in constant string arguments.
- Enforce a fail-fast guardrail for `IN (...)` lists: max **1000** values. Larger lists throw a provider error.

### Rationale

- Keeps Phase 3 narrow and compatible with DecentDB SQL subset while enabling end-to-end LINQ query execution.
- Avoids silently generating unsupported paging syntax (for example `OFFSET ... FETCH`).
- Provides a predictable bound on SQL size and translation cost for large `IN` lists.

### Trade-offs

- String method translation is intentionally conservative in M0 (constant-pattern path first); broader translation can be expanded in later phases.
- Large `IN` lists require caller-side batching/rewrite until a later phase introduces an alternate translation strategy.

### References

- `design/EF_CORE_PROVIDER_IMPLEMENATION_PLAN.md`
- `design/adr/0094-efcore-provider-v0-scope.md`
- `design/SPEC.md`
- https://github.com/sphildreth/decentdb/issues/20
