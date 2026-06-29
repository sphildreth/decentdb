# ADR 0198: Vectorized RETURNING DML Execution

**Date:** 2026-06-24
**Status:** Proposed

## Context

The fresh full benchmark run in `.tmp/perf-validate/20260624-171109` shows
large SQLite wins for RETURNING-heavy mutation paths:

| Benchmark target | SQLite | DecentDB | Gap |
|---|---:|---:|---:|
| Showdown GLM52 scale `UPDATE RETURNING` | 0.002675s | 0.211947s | 79.247x |
| Showdown GLM52 native defaults `INSERT RETURNING` | 0.029628s | 0.393055s | 13.266x |
| Showdown GLM52 scale `INSERT RETURNING` | 0.029473s | 0.382960s | 12.993x |

The benchmark's `INSERT RETURNING` shape executes the same statement 100 times
inside one explicit transaction and returns only `id, title`. Current DecentDB
execution does not cache the simple returning-insert plan in the same way it
caches non-returning simple DML, and the returning path can clone the full
stored row even when the RETURNING projection needs only two scalar values.

`UPDATE RETURNING` at GLM52 scale has an even larger gap. It needs phase timing,
but the likely structural issues are the same family: repeated statement
execution, repeated projection/materialization work, repeated table/index
maintenance, and lack of a transaction-local vectorized mutation path.

This ADR does not authorize weaker commit durability, benchmark-lane changes,
or broad public ABI churn. The first implementation phase must work through the
existing SQL prepare/execute surfaces and existing binding behavior.

## Decision

Adopt transaction-local vectorized execution for eligible repeated DML with
`RETURNING`, plus prepared-plan caching and direct RETURNING projection.

The accepted direction has three parts:

1. **Prepared RETURNING DML plans.** Extend the plan cache to hold eligible
   simple `INSERT ... RETURNING`, `UPDATE ... RETURNING`, and eventually
   `DELETE ... RETURNING` execution bundles keyed by the existing schema and
   policy invalidation state. The cached plan must include resolved target
   columns, conflict/update assignments, RETURNING projection metadata, and
   fallback requirements.
2. **Direct projection.** Render simple RETURNING values from the mutation's
   inserted or updated value arrays whenever possible. Do not clone the full
   stored row when the projection can be proven to need only a subset of
   already-available values.
3. **Transaction-local vectorized mutation state.** When a write transaction
   repeatedly executes the same compatible prepared DML shape, accumulate
   logical row changes in a transaction-local delta layer and amortize physical
   table rewrite, index maintenance, and persist work across the transaction.
   Each individual statement still returns its own rows immediately and later
   statements in the transaction must read the logical effects through the
   delta layer.

Eligibility must be conservative. The optimized path is allowed only when the
executor can prove equivalence for:

- constraints and conflict detection;
- generated/default values;
- foreign keys;
- triggers and trigger ordering;
- row policies and column masks;
- sync/reactive mutation records;
- branch/snapshot visibility;
- stale-index handling;
- RETURNING row order and values;
- rollback and savepoint behavior.

If any proof fails, execution falls back to the existing per-statement path.

## Alternatives Considered

1. **Prepared-plan caching only.** Rejected as insufficient. It should be part
   of the fix, but the measured gaps are too large to assume planning alone is
   the dominant cost.
2. **Add a new public batch API and change benchmarks to use it.** Rejected for
   the first phase. It may be useful later, but these benchmark shapes exercise
   ordinary repeated `execute` calls that real bindings already use.
3. **Buffer RETURNING rows until commit.** Rejected. `RETURNING` rows are
   statement results and must be produced immediately.
4. **Delay logical mutation visibility until commit.** Rejected. Later
   statements in the same transaction must see earlier statement effects.
5. **Weaken sync mode or commit less often.** Rejected. The target shapes are
   already inside explicit transactions where appropriate, and durability
   policy is outside this ADR.

## Consequences

### Positive

- Repeated RETURNING DML can amortize executor and persistence work without
  changing application SQL.
- Simple projections avoid cloning large row payloads when returning a narrow
  column set.
- The same machinery can later improve non-RETURNING repeated DML in explicit
  transactions.

### Negative

- Transaction-local deltas become part of ordinary read visibility.
- Trigger, FK, sync, and policy fallbacks must be explicit and well tested.
- The plan cache needs new memory accounting for RETURNING DML bundles.
- Some complex RETURNING expressions may keep using the existing slower path.

## Implementation Phases

1. Add microbenchmarks for exact Showdown `INSERT RETURNING` and
   `UPDATE RETURNING` shapes that split prepare, execute, projection, index
   maintenance, persist, commit, and Python binding overhead.
2. Add cached simple RETURNING DML plans with memory accounting and invalidation.
3. Add direct simple RETURNING projection for inserted/updated values.
4. Add transaction-local DML delta state for repeated compatible prepared DML in
   explicit transactions.
5. Teach reads, constraints, indexes, and rollback to observe or discard the
   transaction-local delta correctly.

## Validation Requirements

Correctness:

- repeated `INSERT ... RETURNING id, title` returns the same rows and order as
  the existing path;
- repeated `UPDATE ... RETURNING` sees previous transaction-local changes;
- rollback and savepoint rollback remove pending vectorized changes;
- constraints, conflicts, generated values, defaults, policies, masks, triggers,
  FK checks, and sync mutation records match the fallback path;
- complex RETURNING expressions fall back unless proven safe.

Performance:

- fresh `python scripts/benchmark_runner.py --profile full` output must move
  these rows under `DecentDB better at:`:
  - Showdown GLM52 scale `UPDATE RETURNING`;
  - Showdown GLM52 native defaults `INSERT RETURNING`;
  - Showdown GLM52 scale `INSERT RETURNING`.
- targets with less than a 1.10x DecentDB advantage require at least three
  confirming full benchmark runs.

Required checks before implementation is complete:

```bash
cargo fmt --check
cargo check -p decentdb
cargo test -p decentdb --lib
cargo clippy -p decentdb --all-targets --all-features -- -D warnings
python scripts/do-pre-commit-checks.py --mode fast
python scripts/benchmark_runner.py --profile full
```

## References

- `design/adr/0184-default-fast-planner-and-runtime-contract.md`
- `design/adr/0190-query-plan-cache-scope-key-and-lifecycle.md`
- `design/adr/0191-query-plan-cache-memory-accounting-and-eviction.md`
- `design/adr/0194-query-plan-cache-prepared-plan-reuse.md`
- `design/adr/0196-persisted-dml-and-cascade-delete-performance.md`
- `.tmp/perf-validate/20260624-171109`
