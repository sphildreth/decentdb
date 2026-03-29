# Coverage Tracking

## Baseline (automatically recorded)
- Overall: 79.96%

## Recent updates
- 2026-03-29: Added vfs/faulty classify_* unit tests to exercise classification branches. All tests passed: 662.
- Added exec/dml tests fixing a compile error (replaced private method call with public wrapper).
- Added exec prepare/execute insert test and additional normalize SQL tests.
- Coverage (summary): 79.99% (improved from 79.96%)

## Next targets
- Increase coverage in:
  - exec/mod.rs (large, 18.9k lines, 79.22% coverage)
  - sql/normalize.rs (3.98k lines, 76.81% coverage)
  - record/overflow.rs (1.45k lines, 87.51% coverage)
  - vfs/faulty.rs (578 lines, 78.03% coverage)

## Plan
1. Continue adding focused unit tests for exec (dml/ddl/constraints) to exercise fast-paths and error paths.
2. Add more normalization tests to cover window frames, joins, index expressions, and edge SQL forms.
3. Add overflow/record tests for overflow chain handling.
4. Iterate: run cargo llvm-cov after each batch, update this file.
