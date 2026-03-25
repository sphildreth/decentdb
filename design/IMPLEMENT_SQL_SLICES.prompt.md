# Implement All SQL Enhancement Slices

**Purpose:** Comprehensive implementation prompt for all SQL enhancement slices defined in `DECENTDB_SQL_ENHANCEMENTS.md`

**Target Agent:** GitHub Copilot or similar coding agent

**Prerequisites:**
- Read `design/DECENTDB_SQL_ENHANCEMENTS.md` for complete feature specifications
- Read `design/PRD.md` for product context
- Read `design/SPEC.md` for architectural constraints
- Read `.github/instructions/rust.instructions.md` for Rust coding standards
- Read `AGENTS.md` for repository workflow

---

## Mission

Implement all 13 SQL enhancement slices (S1-S13) defined in `DECENTDB_SQL_ENHANCEMENTS.md`. Each slice must be:
- ✅ Fully implemented in Rust
- ✅ Comprehensively tested
- ✅ Documented in user-facing docs
- ✅ Validated against PostgreSQL/SQLite behavior where applicable
- ✅ Checked for ABI/binding impact

---

## Implementation Phases

Follow the phased approach from `DECENTDB_SQL_ENHANCEMENTS.md`:

### Phase 1: Foundation (Implement First)
- **S4:** Trigonometric Math Functions
- **S6:** Conditional Functions (GREATEST, LEAST, IIF)
- **S2:** Statistical Aggregates (STDDEV, VAR, BOOL_AND, BOOL_OR)
- **S3:** TRUNCATE TABLE

### Phase 2: Analytics Enhancement
- **S1:** Window Function Enhancements (frames, NTILE, PERCENT_RANK, CUME_DIST)
- **S2:** MEDIAN, PERCENTILE_CONT, PERCENTILE_DISC, ARRAY_AGG
- **S7:** Extended Date/Time Functions (DATE_TRUNC, AGE, INTERVAL arithmetic)

### Phase 3: Query Expressiveness
- **S8:** Subquery Operators (EXISTS, ANY, ALL)
- **S10:** Comparison Operators (IS DISTINCT FROM, regex operators)
- **S9:** Query Features (VALUES clause, CTAS)
- **S3:** UPDATE/DELETE RETURNING

### Phase 4: Advanced Features
- **S1:** RANGE frame specification
- **S9:** LATERAL joins
- **S3:** MERGE statement
- **S12:** EXPLAIN command

---

## Per-Slice Implementation Checklist

For **each slice**, complete all items in this checklist:

### 1. Planning (Before Coding)

- [ ] Read the slice specification in `DECENTDB_SQL_ENHANCEMENTS.md`
- [ ] Identify affected components:
  - SQL parser (`crates/decentdb/src/sql/parser/`)
  - Query planner (`crates/decentdb/src/sql/planner/`)
  - Expression evaluator (`crates/decentdb/src/sql/executor/`)
  - Type system (`crates/decentdb/src/types/`)
  - C ABI (`include/decentdb.h` if applicable)
- [ ] Check for existing similar implementations to reuse patterns
- [ ] Determine if an ADR is needed (see `AGENTS.md` §7)
- [ ] Create implementation plan with:
  - Scope and exclusions
  - Files to modify
  - Ownership/lifetime strategy
  - Test strategy
  - Documentation updates needed

### 2. Implementation (While Coding)

- [ ] Implement in small, incremental commits
- [ ] Follow Rust coding standards:
  - Use `Result<T, E>` with typed errors
  - Avoid `unwrap()`/`expect()` in library code
  - Prefer borrowing over cloning
  - Document `unsafe` blocks with safety invariants
- [ ] Run validation frequently:
  ```bash
  cargo fmt --check
  cargo check
  cargo clippy --all-targets --all-features -- -D warnings
  ```
- [ ] Add/update unit tests alongside implementation
- [ ] Handle edge cases:
  - NULL values
  - Empty sets
  - Type coercion
  - Overflow/underflow
  - Invalid inputs

### 3. Testing Requirements

For each feature, create tests covering:

#### Unit Tests
- [ ] Function-level correctness tests
- [ ] Edge case tests (NULL, empty, boundary conditions)
- [ ] Error case tests (invalid inputs, type mismatches)
- [ ] Place tests in `crates/decentdb/src/` next to implementation

#### Integration Tests
- [ ] SQL parser integration tests
- [ ] End-to-end query execution tests
- [ ] Place in `crates/decentdb/tests/`

#### Compatibility Tests
- [ ] Compare behavior with PostgreSQL for standard SQL features
- [ ] Compare behavior with SQLite for SQLite-compatible features
- [ ] Document any intentional differences

#### Performance Tests
- [ ] Benchmark critical paths if feature affects hot paths
- [ ] Verify no regression on existing workloads
- [ ] Place in `crates/decentdb/benches/`

### 4. Documentation Requirements

For each feature, update:

- [ ] **SQL Feature Matrix:** Update `docs/user-guide/sql-feature-matrix.md`
- [ ] **Function Reference:** Add to `docs/api/sql-functions.md` or create if needed
- [ ] **Examples:** Add practical examples to relevant docs
- [ ] **CHANGELOG:** Add entry to `CHANGELOG.md`
- [ ] **C ABI:** Update `include/decentdb.h` if ABI changes
- [ ] **Bindings:** Update binding docs if API changes

### 5. Validation Checklist

Before marking a slice complete:

- [ ] `cargo clippy` passes with no warnings
- [ ] All tests pass: `cargo test --all`
- [ ] Specific slice tests pass: `cargo test -p decentdb <feature_name>`
- [ ] Binding tests pass (if ABI affected)
- [ ] Documentation builds: `mkdocs serve` (verify locally)
- [ ] No performance regression on existing benchmarks

---

## Slice-Specific Implementation Notes

### S1: Window Function Enhancements

**Priority Order:**
1. Aggregate window functions (SUM, AVG, COUNT, MIN, MAX with OVER)
2. ROWS frame specification
3. NTILE()
4. PERCENT_RANK()
5. CUME_DIST()
6. RANGE frame specification (defer to Phase 4)

**Key Files:**
- `crates/decentdb/src/sql/parser/window.rs` (or create)
- `crates/decentdb/src/sql/planner/window.rs`
- `crates/decentdb/src/sql/executor/window.rs`

**Implementation Notes:**
- Extend existing window function infrastructure
- Frame boundaries require tracking row positions within partition
- Use Welford's algorithm for numerically stable STDDEV in window context
- Test with large partitions for memory efficiency

---

### S2: Aggregate Functions (Statistical)

**Priority Order:**
1. STDDEV, STDDEV_SAMP, STDDEV_POP
2. VAR_SAMP, VAR_POP
3. ARRAY_AGG
4. MEDIAN
5. PERCENTILE_CONT, PERCENTILE_DISC
6. BOOL_AND, BOOL_OR

**Key Files:**
- `crates/decentdb/src/sql/executor/aggregate.rs`
- `crates/decentdb/src/types/aggregate_state.rs` (or create)

**Implementation Notes:**
- Use Welford's online algorithm for STDDEV/VAR (numerical stability)
- MEDIAN requires partial sorting or selection algorithm
- PERCENTILE functions need ordered-set aggregate infrastructure
- ARRAY_AGG returns JSON array in DecentDB
- Consider memory limits for ARRAY_AGG with large datasets

---

### S3: DML Enhancements

**Priority Order:**
1. TRUNCATE TABLE
2. UPDATE ... RETURNING
3. DELETE ... RETURNING
4. MERGE statement (defer to Phase 4)

**Key Files:**
- `crates/decentdb/src/sql/parser/statement.rs`
- `crates/decentdb/src/sql/executor/write.rs`
- `crates/decentdb/src/storage/table.rs`

**Implementation Notes:**
- TRUNCATE should be efficient (not row-by-row delete)
- TRUNCATE must support rollback (ACID compliance)
- RETURNING requires capturing rows before/after modification
- Reuse INSERT RETURNING infrastructure for UPDATE/DELETE RETURNING
- MERGE is complex; consider phased implementation

---

### S4: Trigonometric Math Functions

**Functions:** SIN, COS, TAN, ASIN, ACOS, ATAN, ATAN2, PI, DEGREES, RADIANS, COT

**Key Files:**
- `crates/decentdb/src/sql/executor/scalar/math.rs`

**Implementation Notes:**
- Use Rust `std::f64` consts and methods
- All functions operate on and return FLOAT64
- Handle edge cases:
  - ASIN/ACOS: Return NULL for values outside [-1, 1]
  - TAN: Handle π/2 + nπ (return NULL or error)
  - COT: Handle nπ (return NULL or error)
- Consider adding hyperbolic functions (SINH, COSH, TANH) as bonus

---

### S5: String Functions (Extended)

**Functions:** CONCAT, CONCAT_WS, POSITION, INITCAP, ASCII, REGEXP_REPLACE, SPLIT_PART, STRING_TO_ARRAY, QUOTE_IDENT, QUOTE_LITERAL, MD5, SHA256

**Key Files:**
- `crates/decentdb/src/sql/executor/scalar/string.rs`

**Implementation Notes:**
- CONCAT treats NULL as empty string (unlike `||`)
- CONCAT_WS skips NULL values
- REGEXP_REPLACE requires regex crate (check if already in dependencies)
- MD5/SHA256 return hex strings
- Consider memory limits for large string operations

---

### S6: Conditional Functions

**Functions:** GREATEST, LEAST, IIF

**Key Files:**
- `crates/decentdb/src/sql/executor/scalar/conditional.rs` (or create)

**Implementation Notes:**
- GREATEST/LEAST return NULL if any argument is NULL
- Work with any comparable type
- IIF is shorthand for CASE WHEN ... THEN ... ELSE ... END
- Can implement GREATEST/LEAST as internal CASE expansion

---

### S7: Date/Time Functions (Extended)

**Functions:** DATE_TRUNC, AGE, TO_TIMESTAMP, INTERVAL arithmetic, DATE_PART, DATE_DIFF, LAST_DAY, NEXT_DAY, MAKE_DATE, MAKE_TIMESTAMP

**Key Files:**
- `crates/decentdb/src/sql/executor/scalar/datetime.rs`
- `crates/decentdb/src/types/datetime.rs`

**Implementation Notes:**
- DATE_TRUNC requires precision parsing ('day', 'month', 'year', etc.)
- INTERVAL arithmetic needs interval type support
- TO_TIMESTAMP needs format string parsing
- Consider using `chrono` crate if not already used
- Handle timezone considerations (DecentDB may be timezone-naive)

---

### S8: Subquery Operators (EXISTS, ANY, ALL)

**Key Files:**
- `crates/decentdb/src/sql/parser/expression.rs`
- `crates/decentdb/src/sql/planner/subquery.rs`
- `crates/decentdb/src/sql/executor/subquery.rs`

**Implementation Notes:**
- EXISTS optimization: stop after first match
- ANY/SOME are synonyms
- `expr = ANY (subquery)` equivalent to `expr IN (subquery)`
- ALL with empty subquery returns TRUE (vacuous truth)
- Consider decorrelation optimization for performance

---

### S9: Query Features

**Features:** LATERAL joins, VALUES clause, CREATE TABLE AS SELECT

**Key Files:**
- `crates/decentdb/src/sql/parser/query.rs`
- `crates/decentdb/src/sql/planner/join.rs`
- `crates/decentdb/src/sql/executor/query.rs`

**Implementation Notes:**
- LATERAL allows subquery to reference preceding tables
- VALUES clause creates inline table source
- CTAS should infer column types from query
- Consider `WITH NO DATA` option for CTAS
- Consider `IF NOT EXISTS` for CTAS

---

### S10: Comparison Operators

**Features:** IS [NOT] DISTINCT FROM, regex operators (~, ~*, !~, !~*)

**Key Files:**
- `crates/decentdb/src/sql/parser/expression.rs`
- `crates/decentdb/src/sql/executor/scalar/comparison.rs`

**Implementation Notes:**
- IS NOT DISTINCT FROM treats NULL = NULL as TRUE
- Regex operators require regex crate
- ~ is case-sensitive, ~* is case-insensitive
- !~ is negation of ~

---

### S11: DDL Enhancements

**Features:** ALTER TABLE RENAME, ADD/DROP CONSTRAINT, VIRTUAL generated columns, expression indexes

**Key Files:**
- `crates/decentdb/src/sql/parser/ddl.rs`
- `crates/decentdb/src/storage/schema.rs`
- `crates/decentdb/src/storage/index.rs`

**Implementation Notes:**
- ALTER TABLE RENAME requires catalog update
- VIRTUAL columns computed on read (vs STORED computed on write)
- Expression indexes store computed values
- Consider impact on existing data for schema changes

---

### S12: Utility Commands

**Features:** EXPLAIN, PRAGMA commands

**Key Files:**
- `crates/decentdb/src/sql/parser/statement.rs`
- `crates/decentdb/src/sql/planner/explain.rs` (or create)

**Implementation Notes:**
- EXPLAIN shows query plan without execution
- EXPLAIN ANALYZE executes and shows actual stats
- PRAGMA provides SQLite compatibility
- Consider JSON output format for EXPLAIN

---

### S13: Advanced Features

**Features:** CREATE SCHEMA, CREATE SEQUENCE, CREATE FUNCTION, CREATE TYPE, Materialized Views, DEFERRABLE constraints, EXCLUDE constraints, Covering indexes, GRANT/REVOKE, Full-text search, Geospatial

**Implementation Notes:**
- These are lower priority due to complexity
- May require ADRs before implementation
- Consider user demand before implementing
- Some features may be deferred indefinitely

---

## Testing Strategy

### Test Organization

```
crates/decentdb/
├── src/
│   ├── sql/
│   │   ├── executor/
│   │   │   ├── aggregate.rs        # Unit tests for aggregates
│   │   │   ├── scalar/
│   │   │   │   ├── math.rs         # Unit tests for math functions
│   │   │   │   ├── string.rs       # Unit tests for string functions
│   │   │   │   └── datetime.rs     # Unit tests for datetime functions
│   │   │   └── window.rs           # Unit tests for window functions
│   │   └── parser/
│   │       └── sql_tests.rs        # Parser unit tests
│   └── ...
└── tests/
    ├── sql/
    │   ├── aggregate_integration_tests.rs
    │   ├── window_integration_tests.rs
    │   ├── dml_integration_tests.rs
    │   └── ...
    └── compatibility/
        ├── postgres_comparison_tests.rs
        └── sqlite_comparison_tests.rs
```

### Test Naming Convention

```rust
#[test]
fn test_function_name_description() { }

#[test]
fn test_function_name_null_handling() { }

#[test]
fn test_function_name_edge_case_description() { }
```

### Test Coverage Requirements

- Each function: minimum 3 tests (normal, edge, error)
- Each SQL feature: minimum 5 integration tests
- Window functions: test with partitions, ordering, frames
- Aggregates: test with GROUP BY, FILTER, window context
- DML: test with RETURNING, transactions, rollback

---

## Documentation Strategy

### Files to Update

1. **SQL Feature Matrix** (`docs/user-guide/sql-feature-matrix.md`)
   - Add checkmarks for implemented features
   - Link to function reference

2. **SQL Function Reference** (`docs/api/sql-functions.md`)
   - Create if doesn't exist
   - Document each function with:
     - Syntax
     - Parameters
     - Return type
     - Examples
     - NULL handling
     - Compatibility notes

3. **Examples** (throughout docs)
   - Add practical, realistic examples
   - Show common patterns
   - Include edge case handling

4. **CHANGELOG.md**
   - Add entry for each slice
   - List all new functions/features
   - Note any breaking changes

5. **C ABI** (`include/decentdb.h`)
   - Update if new types or functions exposed
   - Maintain backward compatibility

---

## Validation Commands

Run these commands frequently during implementation:

```bash
# Format check
cargo fmt --check

# Compile check
cargo check

# Lint check
cargo clippy --all-targets --all-features -- -D warnings

# Run all tests
cargo test --all

# Run specific package tests
cargo test -p decentdb

# Run specific test
cargo test -p decentdb test_function_name

# Run with coverage
cargo llvm-cov --html

# Build documentation
cargo doc --no-deps --open

# Build user docs
mkdocs serve
```

---

## Success Criteria

A slice is complete when:

1. ✅ All features in the slice are implemented
2. ✅ All tests pass (unit, integration, compatibility)
3. ✅ `cargo clippy` passes with no warnings
4. ✅ Documentation is updated and accurate
5. ✅ CHANGELOG.md is updated
6. ✅ No performance regression on existing benchmarks
7. ✅ C ABI is updated if affected (with binding tests)
8. ✅ Code review approved (if applicable)

---

## Progress Tracking

Update the Slice Map in `DECENTDB_SQL_ENHANCEMENTS.md` as slices progress:

| Status | Symbol |
|--------|--------|
| Not Started | 🔴 |
| In Progress | 🟡 |
| Completed | 🟢 |
| Deferred | ⚪ |

Update the status column as work progresses.

---

## Questions to Ask Before Starting

1. Does this slice require an ADR? (See `AGENTS.md` §7)
2. Are there existing patterns in the codebase to follow?
3. Does this affect the C ABI or bindings?
4. Are there any dependencies that need to be added?
5. What is the compatibility expectation (PostgreSQL, SQLite, both)?

---

## References

- `design/DECENTDB_SQL_ENHANCEMENTS.md` - Feature specifications
- `design/PRD.md` - Product requirements
- `design/SPEC.md` - Technical specifications
- `design/TESTING_STRATEGY.md` - Testing approach
- `.github/instructions/rust.instructions.md` - Rust coding standards
- `AGENTS.md` - Repository workflow
- [PostgreSQL 17 Documentation](https://www.postgresql.org/docs/17/)
- [SQLite SQL Syntax](https://www.sqlite.org/lang.html)
- [DuckDB SQL Introduction](https://duckdb.org/docs/sql/introduction)

---

## Agent Instructions

When executing this prompt:

1. **Start with Phase 1** - Do not skip to later phases
2. **Complete one slice at a time** - Do not interleave slices
3. **Follow the checklist** - Mark items as you complete them
4. **Run validation frequently** - After each significant change
5. **Update documentation** - As you implement, not after
6. **Ask questions** - If unclear, ask before proceeding
7. **Do not commit** - Without explicit user approval
8. **Report progress** - Update the Slice Map as you progress

---

**End of Prompt**
