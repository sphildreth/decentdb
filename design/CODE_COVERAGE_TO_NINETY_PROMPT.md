# Coding Prompt: Achieve 90% Code Coverage for DecentDB Engine Core

**Date:** 2026-03-28  
**Target:** Achieve and maintain 90% line coverage on `crates/decentdb/src/`  
**Scope:** Engine core only (exclude CLI, bindings, examples)

---

## Objective

Systematically increase unit test coverage for the DecentDB engine core to reach 90% line coverage. Focus on untested or undertested code paths, edge cases, error handling, and critical ACID guarantees.

---

## Prerequisites

Before starting, ensure you understand:

1. **Repository Standards:**
   - Read `AGENTS.md` for engineering standards
   - Read `.github/copilot-instructions.md` for Rust-specific rules
   - Read `design/TESTING_STRATEGY.md` for testing philosophy
   - Load the `rust-code-generation` skill from `.github/skills/rust-code-generation/SKILL.md`

2. **Current State:**
   - 71 source files in `crates/decentdb/src/`
   - 26 test files in `crates/decentdb/tests/`
   - Existing tests cover: btree, catalog, config, db, error, exec, c_api, sql/parser
   - Coverage gaps likely in: storage/*, wal/*, vfs/*, planner/*, record/*, search/*, sql/*

3. **Coverage Tooling:**
   - Use `cargo llvm-cov` for coverage measurement
   - Command: `cargo llvm-cov --workspace --ignore-filename-regex="(tests/|bindings/|examples/)" --html`
   - HTML report generated at `target/llvm-cov/html/index.html`

4. **Existing Test Patterns:**
   - Review `crates/decentdb/tests/` for existing test structure
   - Check for test utilities in `tests/harness/` (if present)
   - Note any test helpers or fixtures already available
   - Read existing tests before writing new ones to match patterns

---

## Implementation Strategy

### Phase 1: Baseline Measurement (Day 1)

1. **Generate coverage report:**
   ```bash
   cargo llvm-cov --workspace --ignore-filename-regex="(tests/|bindings/|examples/)" --html
   ```

2. **Extract baseline percentages:**
   ```bash
   # Get overall coverage percentage
   cargo llvm-cov --workspace --ignore-filename-regex="(tests/|bindings/|examples/)" --summary-only 2>&1 | grep -E "^TOTAL"
   
   # Get per-file coverage (for module breakdown)
   cargo llvm-cov --workspace --ignore-filename-regex="(tests/|bindings/|examples/)" --summary-only 2>&1 | grep -E "src/"
   ```

3. **Populate coverage table:**
   - Fill in the "Current" column in the Coverage Targets table below
   - Identify the 3 lowest-coverage modules to prioritize
   - Prioritize: storage > wal > vfs > record > planner > search > sql

4. **Create coverage tracking file:**
   - Create `/memories/session/coverage-tracking.md`
   - Document current coverage per module
   - Track progress daily

### Phase 2: Module-by-Module Testing (Days 2-10)

For each module, follow this process:

#### Step 1: Read and Understand
- **Read existing tests for this module first** to understand patterns and conventions
- Read the module source code completely
- Identify public APIs, internal functions, error paths
- Note any `#[cfg(test)]` blocks that already exist
- Check for existing integration tests that exercise this module
- **Check if test utilities/fixtures exist that can be reused**

#### Step 2: Identify Test Gaps
For each function/method, ask:
- Is there a happy path test?
- Are error conditions tested?
- Are edge cases tested (empty inputs, max values, boundary conditions)?
- Are concurrent access patterns tested (if applicable)?
- Are crash/recovery scenarios tested (for storage/wal)?

#### Step 3: Write Tests
Create tests following these patterns:

**Unit Tests (in source file with `#[cfg(test)]`):**
- Fast, isolated tests for pure functions
- Test individual components in isolation
- Mock dependencies where necessary

**Integration Tests (in `tests/` directory):**
- Test module interactions
- Test public APIs end-to-end
- Test error propagation across boundaries

**Property Tests (using `proptest`):**
- Use for complex invariants (btree ordering, serialization roundtrips)
- Generate random inputs to find edge cases
- Always seed randomness for reproducibility

**Code Quality Requirements:**
- All tests must compile without warnings
- All tests must pass `cargo clippy --all-targets --all-features -- -D warnings`
- Use `#[allow(clippy::...)]` only when absolutely necessary with a comment explaining why
- Avoid `unwrap()` and `expect()` in tests unless the panic message is the test purpose

#### Step 4: Validate Coverage and Quality
After adding tests, validate both coverage and code quality:

```bash
# Check for compilation errors and warnings
cargo check --all-targets

# Run clippy to ensure no warnings (REQUIRED)
cargo clippy --all-targets --all-features -- -D warnings

# Generate coverage report
cargo llvm-cov --workspace --ignore-filename-regex="(tests/|bindings/|examples/)" --html
```

Verify:
1. ✅ No clippy warnings (build must pass with `-D warnings`)
2. ✅ Coverage increased for the target module
3. ✅ All existing tests still pass

---

## Module-Specific Testing Requirements

### 1. Storage Layer (`storage/`)

**Files:** `cache.rs`, `checksum.rs`, `freelist.rs`, `header.rs`, `page.rs`, `pager.rs`

**Priority:** CRITICAL (ACID foundation)

**Required Tests:**

#### `storage/page.rs`
- [ ] Page header serialization/deserialization roundtrip
- [ ] Cell pointer array operations (insert, delete, rebalance)
- [ ] Overflow page chain traversal
- [ ] Page checksum calculation and verification
- [ ] Edge cases: empty page, full page, max cells

#### `storage/pager.rs`
- [ ] Page allocation and deallocation
- [ ] Cache hit/miss scenarios
- [ ] Dirty page tracking and flush
- [ ] Page read after write roundtrip
- [ ] Concurrent reader scenarios (multiple threads reading same page)
- [ ] Error handling: disk full, I/O errors, corruption detection

#### `storage/freelist.rs`
- [ ] Free page tracking (add, remove, find)
- [ ] Freelist serialization/deserialization
- [ ] Edge cases: empty freelist, full freelist, fragmentation
- [ ] Crash recovery: freelist consistency after partial writes

#### `storage/cache.rs`
- [ ] LRU eviction policy
- [ ] Cache size limits
- [ ] Pin/unpin semantics
- [ ] Thread safety (concurrent access)

#### `storage/header.rs`
- [ ] Database header serialization/deserialization
- [ ] Header field validation
- [ ] Version compatibility checks
- [ ] Corrupted header detection

#### `storage/checksum.rs`
- [ ] Checksum calculation for various page sizes
- [ ] Checksum verification
- [ ] Known test vectors (compare against reference implementation)

#### Error Injection Testing (CRITICAL)
For storage layer testing, inject failures to test error paths:
- [ ] Simulate disk full conditions
- [ ] Simulate I/O errors during read/write
- [ ] Test partial writes (torn pages)
- [ ] Test corruption detection and handling
- [ ] Use trait objects or wrapper types to inject faults

---

### 2. Write-Ahead Log (`wal/`)

**Files:** All files in `wal/` directory

**Priority:** CRITICAL (ACID durability)

**Required Tests:**

#### WAL Frame Encoding/Decoding
- [ ] Frame header serialization
- [ ] Frame checksum calculation
- [ ] Frame validation on read
- [ ] Torn write detection (partial frames)
- [ ] Edge cases: empty frame, max frame size

#### WAL File Management
- [ ] WAL file creation and naming
- [ ] WAL segment rotation
- [ ] WAL file size limits
- [ ] WAL file deletion after checkpoint

#### WAL Recovery
- [ ] Replay committed transactions
- [ ] Ignore uncommitted transactions
- [ ] Handle incomplete frames at end of file
- [ ] Recovery after crash at various points:
  - [ ] Mid-transaction
  - [ ] During commit
  - [ ] During checkpoint
  - [ ] During WAL rotation

#### Checkpointing
- [ ] Checkpoint moves WAL to main database
- [ ] Checkpoint preserves uncommitted data
- [ ] Checkpoint handles errors gracefully
- [ ] Concurrent checkpoint and write scenarios

#### Error Injection Testing (CRITICAL)
For WAL testing, inject failures to test durability guarantees:
- [ ] Simulate I/O errors during WAL write
- [ ] Test torn write detection (partial frames)
- [ ] Test recovery from corrupted WAL segments
- [ ] Simulate disk full during checkpoint
- [ ] Test WAL behavior when database file is corrupted

---

### 3. Virtual File System (`vfs/`)

**Files:** All files in `vfs/` directory

**Priority:** HIGH (abstraction layer for I/O)

**Required Tests:**

#### File Operations
- [ ] Open/create/close file
- [ ] Read/write operations
- [ ] Seek operations
- [ ] File size queries
- [ ] File deletion

#### Error Handling
- [ ] File not found
- [ ] Permission denied
- [ ] Disk full
- [ ] I/O errors

#### Platform-Specific Behavior
- [ ] Test on Linux (primary target)
- [ ] Document expected behavior on Windows/macOS

---

### 4. Record Layer (`record/`)

**Files:** All files in `record/` directory

**Priority:** HIGH (data serialization)

**Required Tests:**

#### Type Encoding/Decoding
- [ ] Integer types (i8, i16, i32, i64, u8, u16, u32, u64)
- [ ] Floating point (f32, f64)
- [ ] Text (ASCII, UTF-8, multi-byte characters)
- [ ] Blob (binary data)
- [ ] NULL values
- [ ] Edge cases: empty strings, max length strings, max blobs

#### Record Serialization
- [ ] Record header encoding
- [ ] Record payload encoding
- [ ] Overflow handling for large records
- [ ] Record comparison and ordering

#### Boundary Values
- [ ] Min/max values for each type
- [ ] Overflow/underflow scenarios
- [ ] Type coercion edge cases

---

### 5. B+Tree Index (`btree/`)

**Files:** All files in `btree/` directory (some tests exist, need more)

**Priority:** HIGH (core data structure)

**Required Tests:**

#### Tree Operations
- [ ] Insert into empty tree
- [ ] Insert causing split
- [ ] Insert with overflow
- [ ] Delete causing merge/redistribute
- [ ] Range scans
- [ ] Cursor navigation

#### Concurrency
- [ ] Multiple readers, single writer
- [ ] Reader sees consistent snapshot during write
- [ ] Lock ordering prevents deadlocks

#### Crash Recovery
- [ ] Tree structure after crash during split
- [ ] Tree structure after crash during merge
- [ ] Tree structure after crash during rebalance

---

### 6. SQL Parser (`sql/`)

**Files:** `sql/parser.rs` (tests exist in `sql/parser_tests.rs`)

**Priority:** MEDIUM (already has good coverage)

**Required Tests:**

#### SQL Syntax Coverage
- [ ] All DDL statements (CREATE TABLE, INDEX, VIEW, TRIGGER)
- [ ] All DML statements (INSERT, UPDATE, DELETE, SELECT)
- [ ] All constraint types
- [ ] Edge cases: nested queries, CTEs, window functions

#### Error Handling
- [ ] Syntax errors report correct location
- [ ] Invalid SQL rejected with clear error
- [ ] SQL injection prevention

---

### 7. Query Planner (`planner/`)

**Files:** All files in `planner/` directory

**Priority:** MEDIUM (query optimization)

**Required Tests:**

#### Plan Generation
- [ ] Simple SELECT plans
- [ ] JOIN plans (nested loop, hash join if supported)
- [ ] Index usage decisions
- [ ] Filter pushdown
- [ ] Aggregate query plans

#### Plan Optimization
- [ ] Predicate pushdown
- [ ] Join reordering (if supported)
- [ ] Index selection

---

### 8. Full-Text Search (`search/`)

**Files:** All files in `search/` directory

**Priority:** MEDIUM (specialized feature)

**Required Tests:**

#### Trigram Index
- [ ] Trigram generation
- [ ] Trigram index building
- [ ] Trigram query execution
- [ ] Unicode handling

#### Posting Lists
- [ ] Posting list encoding/decoding
- [ ] Posting list intersection
- [ ] Ranking/scoring

---

### 9. Execution Engine (`exec/`)

**Files:** All files in `exec/` directory (some tests exist)

**Priority:** HIGH (query execution)

**Required Tests:**

#### Expression Evaluation
- [ ] Arithmetic expressions
- [ ] Comparison expressions
- [ ] Logical expressions (AND, OR, NOT)
- [ ] NULL handling in expressions
- [ ] Type coercion

#### Query Execution
- [ ] Table scan
- [ ] Index scan
- [ ] Filter application
- [ ] Aggregation
- [ ] Sorting
- [ ] Limit/offset

#### Transaction Handling
- [ ] BEGIN/COMMIT/ROLLBACK
- [ ] Savepoints
- [ ] Isolation levels

---

### 10. Catalog (`catalog/`)

**Files:** All files in `catalog/` directory (some tests exist)

**Priority:** MEDIUM (metadata management)

**Required Tests:**

#### Schema Management
- [ ] Table creation/deletion
- [ ] Index creation/deletion
- [ ] View creation/deletion
- [ ] Trigger creation/deletion
- [ ] Constraint enforcement

#### Catalog Persistence
- [ ] Catalog serialization to disk
- [ ] Catalog deserialization from disk
- [ ] Catalog recovery after crash

---

## Testing Patterns and Best Practices

### 1. Test Structure

```rust
#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_happy_path() {
        // Arrange
        let input = /* ... */;
        
        // Act
        let result = function_under_test(input);
        
        // Assert
        assert!(result.is_ok());
        // ... more assertions
    }

    #[test]
    fn test_error_case() {
        // Test error handling
        let result = function_under_test(invalid_input);
        assert!(result.is_err());
        assert_eq!(result.unwrap_err().kind(), ErrorKind::InvalidInput);
    }

    #[test]
    fn test_edge_case() {
        // Test boundary conditions
        // ...
    }
}
```

### 2. Property-Based Testing

```rust
use proptest::prelude::*;

proptest! {
    #[test]
    fn test_serialization_roundtrip(data in any::<MyType>()) {
        let encoded = encode(&data).unwrap();
        let decoded = decode(&encoded).unwrap();
        prop_assert_eq!(data, decoded);
    }
}
```

### 3. Integration Test Pattern

```rust
// In tests/integration_test.rs
use decentdb::{Database, Config};
use tempfile::TempDir;

#[test]
fn test_database_roundtrip() {
    let temp_dir = TempDir::new().unwrap();
    let db_path = temp_dir.path().join("test.ddb");
    
    // Create and write
    let db = Database::open(&db_path, Config::default()).unwrap();
    db.execute("CREATE TABLE test (id INTEGER PRIMARY KEY, value TEXT)").unwrap();
    db.execute("INSERT INTO test VALUES (1, 'hello')").unwrap();
    
    // Read back
    let results = db.query("SELECT * FROM test").unwrap();
    assert_eq!(results.len(), 1);
}
```

### 4. Error Testing

```rust
#[test]
fn test_error_propagation() {
    let result = fallible_operation();
    match result {
        Err(Error::IoError(e)) => {
            // Verify error is propagated correctly
            assert_eq!(e.kind(), std::io::ErrorKind::NotFound);
        }
        Err(e) => panic!("Unexpected error: {:?}", e),
        Ok(_) => panic!("Expected error, got success"),
    }
}
```

---

## Test File Organization

### Unit Tests vs Integration Tests

- **Unit tests:** Place in `#[cfg(test)] mod tests` within the source file
  - Fast, isolated tests for pure functions
  - Test individual components in isolation
  - No file I/O or external dependencies

- **Integration tests:** Create file in `crates/decentdb/tests/` when:
  - Testing cross-module behavior
  - Testing file I/O or persistence
  - Testing public API end-to-end
  - Testing error propagation across boundaries

### Naming Conventions

- **Unit tests:** No special naming required, just `#[cfg(test)] mod tests`
- **Integration tests:** `{module}_test.rs` or `{feature}_test.rs`
- **Property tests:** Can be in either location, use `proptest` crate

### Reuse Existing Test Files

- If a test file already exists for a module, **add to it** rather than creating a new one
- Match the style and patterns of existing tests in that module
- Reuse test utilities and fixtures where available

### Test Isolation

- Use `tempfile` crate for file-based tests (already in dependencies)
- Each test should be independent and not rely on state from other tests
- Use `#[serial]` from `serial_test` crate only when absolutely necessary
- Seed all randomness in property tests for reproducibility

### Code Quality for Tests

- **All tests must pass clippy without warnings**
- Run `cargo clippy --all-targets --all-features -- -D warnings` after adding tests
- Fix all warnings before moving to the next module
- Common test-specific clippy lints to be aware of:
  - `clippy::unwrap_used` - prefer `?` or `assert!` in tests
  - `clippy::expect_used` - document why panic is acceptable
  - `clippy::panic` - only use when testing panic behavior
- Use `#[allow(clippy::...)]` sparingly and always with a comment explaining the exception

---

## Coverage Measurement Commands

### Generate Coverage Report

```bash
# Full coverage report
cargo llvm-cov --workspace --ignore-filename-regex="(tests/|bindings/|examples/)" --html

# Coverage for specific module
cargo llvm-cov --workspace --ignore-filename-regex="(tests/|bindings/|examples/)" -- --test-threads=1

# Coverage summary only
cargo llvm-cov --workspace --ignore-filename-regex="(tests/|bindings/|examples/)" --summary-only
```

### View Coverage Report

```bash
# Open HTML report in browser
xdg-open target/llvm-cov/html/index.html
```

### Coverage Targets

| Module | Current | Target | Priority |
|--------|---------|--------|----------|
| storage/ | ? | 95% | CRITICAL |
| wal/ | ? | 95% | CRITICAL |
| vfs/ | ? | 90% | HIGH |
| record/ | ? | 90% | HIGH |
| btree/ | ~85% | 95% | HIGH |
| sql/ | ? | 85% | MEDIUM |
| planner/ | ? | 85% | MEDIUM |
| search/ | ? | 85% | MEDIUM |
| exec/ | ? | 90% | HIGH |
| catalog/ | ? | 85% | MEDIUM |
| **OVERALL** | **?** | **90%** | **REQUIRED** |

---

## Definition of Done

A module is considered "done" when:

1. ✅ Line coverage ≥ 90% (95% for storage/wal)
2. ✅ All public APIs have at least one test
3. ✅ All error paths have tests
4. ✅ Edge cases are covered
5. ✅ Property tests exist for complex invariants
6. ✅ `cargo clippy --all-targets --all-features -- -D warnings` passes
7. ✅ All tests pass: `cargo test --workspace`
8. ✅ Documentation comments updated if behavior changed

---

## Progress Tracking

Create and maintain `/memories/session/coverage-tracking.md`:

```markdown
# Coverage Tracking

## Baseline (2026-03-28)
- Overall: X%
- storage/: X%
- wal/: X%
- vfs/: X%
- record/: X%
- btree/: X%
- sql/: X%
- planner/: X%
- search/: X%
- exec/: X%
- catalog/: X%

## Daily Updates

### 2026-03-29
- Added tests for storage/page.rs
- Coverage: storage/page.rs 45% → 78%
- Overall: X% → Y%

### 2026-03-30
- ...
```

---

## Common Pitfalls to Avoid

1. **Testing implementation details instead of behavior**
   - Focus on observable behavior, not internal state
   - Use public APIs for testing

2. **Insufficient error testing**
   - Every `Result::Err` path should have a test
   - Use `#[should_panic]` sparingly, prefer `Result` testing

3. **Missing edge cases**
   - Empty inputs
   - Maximum values
   - Boundary conditions
   - Unicode edge cases

4. **Flaky tests**
   - Avoid depending on timing
   - Use deterministic seeds for randomness
   - Isolate tests from each other

5. **Testing the wrong thing**
   - Don't test that code compiles (the compiler does that)
   - Don't test third-party library behavior
   - Test your code's behavior, not dependencies

6. **Not reading existing tests first**
   - Check for existing test patterns before writing new tests
   - Reuse test utilities and fixtures where available
   - Match the style of existing tests in the module

7. **Testing at the wrong level**
   - Unit tests for pure logic and edge cases
   - Integration tests for I/O, persistence, and cross-module behavior
   - Don't write integration tests for simple serialization functions

8. **Missing error injection for storage/wal**
   - For CRITICAL modules, test failure scenarios
   - Use trait objects or wrappers to inject I/O errors
   - Test partial writes, disk full, corruption detection

---

## Decision Tree for Autonomous Operation

Follow this decision tree when working autonomously:

```
1. Is the module already at target coverage?
   ├─ Yes → Skip, move to next module
   └─ No → Continue to step 2

2. Are there existing tests for this module?
   ├─ Yes → Read them, understand patterns, add to them
   └─ No → Create new test file following repository conventions

3. Is the code testable as-is?
   ├─ Yes → Write tests
   └─ No → Document the blocker in session memory and move to next module

4. After adding tests, did coverage increase?
   ├─ Yes → Continue with more tests for this module
   └─ No → Debug why (dead code? unreachable? wrong test target?)

5. Do tests pass clippy without warnings?
   ├─ Yes → Continue to step 6
   └─ No → Fix all clippy warnings before proceeding (REQUIRED)

6. Is the module at target coverage now?
   ├─ Yes → Mark complete in session memory, move to next module
   └─ No → Continue adding tests

7. Is overall coverage at 90%?
   ├─ Yes → Task complete, generate final report
   └─ No → Return to step 1 for next module
```

### Stop and Document Conditions

Stop and document (but don't ask for help) when:
- A module has architectural issues blocking tests (document in session memory)
- Adding tests would require API changes (document and move on)
- Coverage is stuck despite adding tests (document findings)
- A module is at 85%+ and remaining coverage is unreachable/dead code

---

## Additional Modules

### 11. Configuration (`config/`)

**Priority:** MEDIUM

**Required Tests:**
- [ ] Configuration parsing and validation
- [ ] Default values
- [ ] Configuration file loading
- [ ] Environment variable overrides (if supported)
- [ ] Invalid configuration handling

### 12. Error Types (`error/`)

**Priority:** MEDIUM

**Required Tests:**
- [ ] Error type conversions (`From`/`Into` implementations)
- [ ] Error chain propagation
- [ ] Error display and debug formatting
- [ ] Error kind classification

### 13. Database Lifecycle (`db/`)

**Priority:** HIGH

**Required Tests:**
- [ ] Database open/create/close
- [ ] Database recovery on open
- [ ] Connection lifecycle
- [ ] Error handling during initialization
- [ ] Cleanup on close

### 14. C API (`c_api/`)

**Priority:** HIGH (binding stability)

**Required Tests:**
- [ ] All exported functions have smoke tests
- [ ] NULL pointer handling
- [ ] Buffer overflow prevention
- [ ] Error code returns
- [ ] Memory management (leak prevention)

---

## Resources

- **Testing Strategy:** `design/TESTING_STRATEGY.md`
- **Rust Code Generation Skill:** `.github/skills/rust-code-generation/SKILL.md`
- **Repository Standards:** `AGENTS.md`
- **Rust Instructions:** `.github/copilot-instructions.md`
- **Coverage Tool:** `cargo llvm-cov --help`

---

## Success Criteria

The task is complete when:

1. ✅ Overall line coverage ≥ 90% for `crates/decentdb/src/`
2. ✅ All modules meet their individual coverage targets
3. ✅ All tests pass: `cargo test --workspace`
4. ✅ No clippy warnings: `cargo clippy --all-targets --all-features -- -D warnings`
5. ✅ Coverage report generated and reviewed
6. ✅ Progress tracked in `/memories/session/coverage-tracking.md`

---

## Notes for the Agent

- **Start with baseline measurement** - don't assume current coverage
- **Prioritize critical modules** - storage and wal are ACID foundations
- **Write tests incrementally** - commit after each module
- **Run coverage frequently** - after each significant test addition
- **Focus on quality** - 90% coverage with meaningful tests, not just hitting numbers
- **Document gaps** - if 90% is not achievable for a module, document why
- **Ask for clarification** - if requirements are unclear, ask before implementing
- **No clippy warnings** - all tests must pass `cargo clippy --all-targets --all-features -- -D warnings` without any warnings; this is a hard requirement, not optional

Good luck! 🚀
