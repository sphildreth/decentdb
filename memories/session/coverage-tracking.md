# Coverage Tracking

## Baseline (2026-03-28)
- Overall: 80.76% (baseline measured from initial coverage run)

## Update (2026-03-29)
- Added same-file unit tests to crates/decentdb/src/exec/dml.rs exercising apply_conflict_update paths.
- Fixed compile-time TableData references in dml.rs and removed an unused import in sql/ast_more_tests.rs.
- Re-ran tests; all unit tests passed (692 tests).

### Coverage summary (post-change)
- TOTAL: 80.76% (latest: 80.7557%)
- Notable per-file snapshots:
  - crates/decentdb/src/storage/page.rs: 99.48%
  - crates/decentdb/src/storage/pager.rs: 91.56%
  - crates/decentdb/src/vfs/faulty.rs: 83.82%
  - crates/decentdb/src/vfs/mem.rs: 97.59%
  - crates/decentdb/src/wal/format.rs: 95.54%
  - crates/decentdb/src/wal/writer.rs: 90.79%

More detailed per-file coverage available in target/llvm-cov/html/index.html.

## Next steps
1. Add more same-file tests in exec/dml.rs (apply_prepared_simple_insert_candidate, index update helpers, materialize_insert_source).
2. Add WAL partial-write/recovery tests using FaultyVfs (serialize these tests and clear failpoints between runs).
3. Add storage/pager unit tests (allocation, deallocation, dirty flush, checksum verification).
4. Run cargo clippy --all-targets --all-features -- -D warnings and fix any warnings.
5. Re-run cargo llvm-cov after each batch and update this file.
