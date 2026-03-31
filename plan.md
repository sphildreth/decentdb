# Plan to Resolve DecentDB Engine and .NET Binding Issues

## 1. Engine Issue: Decimal Comparison with Floating-Point Types
**Issue:** `cannot compare values Decimal { ... } and Float64(...)`
**Location:** `crates/decentdb/src/exec/mod.rs` around line 13974.
**Plan:**
- Modify the `PartialOrd` implementation or comparison logic in `exec/mod.rs` (likely in `cmp_values` or similar) to allow comparison between `Value::Decimal` and `Value::Float64`.
- When comparing a `Decimal` and a `Float64`, convert the `Decimal` to an `f64` (or vice versa, though precision might be lost converting `f64` to `Decimal`, so converting `Decimal` to `Float64` for the comparison might be standard SQL behavior, or better, scale the float to decimal if possible).
**Tests:**
- Add a test in `crates/decentdb/src/exec/mod.rs` (or relevant test file) verifying `SELECT * FROM table WHERE decimal_col > 100.0` works.

## 2. Engine Issue: Decimal Aggregation (AVG/SUM)
**Issue:** `numeric aggregate does not support Decimal { ... }`
**Location:** `crates/decentdb/src/exec/mod.rs` around lines 7199, 10423, 10450.
**Plan:**
- Update the aggregation functions (e.g., `sum`, `avg`) in `exec/mod.rs` to support `Value::Decimal`.
- For `SUM`, accumulate the scaled values. Ensure overflow is handled (maybe upgrade to a larger integer type or return an error on overflow).
- For `AVG`, accumulate the sum and count, then perform the division. The result of `AVG` on a `Decimal` is typically a `Decimal` or `Float64`.
**Tests:**
- Add tests in `crates/decentdb/src/exec/mod.rs` verifying `SELECT SUM(decimal_col), AVG(decimal_col) FROM table` works correctly.

## 3. Other Issues (from DOTNET_BINDING_ISSUES.md)
**Table-Level Foreign Key Constraints:**
- **Plan:** Engine currently lacks `ALTER TABLE ... ADD FOREIGN KEY`. We should implement parsing and execution for this DDL statement.
- **Tests:** Add parser and execution tests for adding foreign keys.

**Composite Primary Keys:**
- **Plan:** Engine lacks support for multiple `[Key]` columns. We need to update the storage/B-Tree and planner to handle composite keys.
- **Tests:** Add tests for creating tables with composite keys and querying them.

**Window Functions:**
- **Plan:** Implement basic window functions like `ROW_NUMBER()`, `RANK()`, `OVER()`.
- **Tests:** Add tests for basic window function queries.

**.NET Provider specific issues:**
- **Decimal Type Mapping:** Map to Double or configure precision.
- **Navigation Properties:** Modify EF Core provider to skip FK constraint generation if the engine doesn't support it yet.
