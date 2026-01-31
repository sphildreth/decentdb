# Testing

DecentDb has comprehensive testing at multiple levels.

## Test Overview

### Test Types

1. **Unit Tests** - Individual module correctness
2. **Property Tests** - Invariants hold under random operations
3. **Crash Tests** - Recovery after failures
4. **Differential Tests** - Match PostgreSQL behavior
5. **Benchmarks** - Performance regression detection

### Test Organization

```
tests/
├── nim/           # Nim unit tests
├── harness/       # Python test framework
├── bench/         # Performance benchmarks
└── data/          # Test datasets
```

## Running Tests

### All Tests

```bash
nimble test
```

Runs:
- All Nim unit tests (30+ test files)
- Python harness tests

Takes ~2-3 minutes.

### Nim Tests Only

```bash
nimble test_nim
```

Or individual test:
```bash
nim c -r tests/nim/test_wal.nim
```

### Python Tests Only

```bash
nimble test_py
```

Or directly:
```bash
python -m unittest tests/harness/test_runner.py
```

### Specific Test Suite

```bash
# WAL tests
nim c -r tests/nim/test_wal.nim

# BTree tests
nim c -r tests/nim/test_btree.nim

# SQL tests
nim c -r tests/nim/test_sql_parser.nim
```

## Test Layers

### 1. Unit Tests

Test individual functions and modules.

**Example** (from `test_btree.nim`):
```nim
test "insert split update delete":
  let tree = newBTree(pager, rootPage)
  
  # Insert
  let insertRes = tree.insert(1, toBytes("value1"))
  check insertRes.ok
  
  # Find
  let findRes = tree.find(1)
  check findRes.ok
  check findRes.value[1] == toBytes("value1")
  
  # Update
  let updateRes = tree.update(1, toBytes("updated"))
  check updateRes.ok
  
  # Delete
  let deleteRes = tree.delete(1)
  check deleteRes.ok
```

### 2. Property-Based Tests

Test that invariants hold for random operations.

**Example** (from `test_property.nim`):
```nim
test "index results == scan results":
  for i in 0..<100:
    let query = generateRandomQuery()
    let indexResults = execWithIndex(query)
    let scanResults = execWithScan(query)
    check indexResults == scanResults
```

### 3. Crash-Injection Tests

Simulate failures to verify recovery.

**Using FaultyVFS**:
```nim
test "torn write ignored on recovery":
  let vfs = newFaultyVfs()
  vfs.addRule(FaultRule(
    op: foWrite,
    action: FaultAction(kind: faPartialWrite, partialBytes: 16)
  ))
  
  # Attempt write
  let res = writeWithVfs(vfs, data)
  check not res.ok  # Should fail
  
  # Recover
  let recovered = openAndRecover()
  check recovered.ok  # Should succeed
```

### 4. Differential Tests

Compare DecentDb results with PostgreSQL.

**Using Python harness**:
```python
def test_select():
    sql = "SELECT * FROM users WHERE age > 18"
    
    # Run on both databases
    decent_results = run_decentdb(sql)
    pg_results = run_postgresql(sql)
    
    # Should match
    assert decent_results == pg_results
```

### 5. Benchmarks

Track performance over time.

```bash
# Run benchmarks
nimble bench

# Compare to baseline
nimble bench_compare
```

## Test Data

### Test Datasets

Located in `tests/data/`:

- **Sequential data**: Scripts to generate sequential ID datasets of various sizes (1K, 10K, 100K, 1M rows)
- **Unicode text**: Multi-byte characters in various scripts (Latin, Cyrillic, Chinese, Japanese, Korean, Greek)
- **Edge cases**: NULLs, empty strings, max values, long text fields

### Generating Test Data

```bash
# Generate edge cases dataset
python tests/data/generate_edge_cases.py

# Creates:
# - Empty strings
# - NULL values
# - Maximum integer values
# - Long text fields

# Generate sequential dataset
python tests/data/generate_sequential.py

# Creates:
# - Sequential ID sequences
# - Various sized datasets (1K, 10K, 100K, 1M rows)

# Generate unicode dataset
python tests/data/generate_unicode.py

# Creates:
# - Multi-script text (Latin, Cyrillic, Chinese, Japanese, Korean, Greek)
# - UTF-8 encoded text fields
```

## Writing Tests

### New Unit Test

Create `tests/nim/test_feature.nim`:

```nim
import unittest
import ../src/engine
import ../src/record/record

suite "Feature Name":
  test "specific behavior":
    # Setup
    let db = openDb(":memory:")
    check db.ok
    
    # Test
    let res = execSql(db.value, "...")
    check res.ok
    check res.value.len == expected
    
    # Cleanup
    discard closeDb(db.value)
```

### New Crash Test

Create scenario file `tests/harness/scenarios/my_crash.json`:

```json
{
  "name": "my_crash_test",
  "description": "Test crash during operation",
  "sql": "CREATE TABLE t (id INT); INSERT INTO t VALUES (1)",
  "failpoint": {
    "label": "wal_frame",
    "kind": "error"
  },
  "verify": {
    "expect_crash": true,
    "post_crash_sql": "SELECT * FROM t",
    "expect_rows": []
  }
}
```

### New Differential Test

Add to `tests/harness/differential_runner.py`:

```python
DifferentialTest(
    name="my_feature",
    description="Test new feature",
    schema_sql="CREATE TABLE t (id INT)",
    test_sql="SELECT * FROM t",
    expect_rows=["..."]
)
```

## Test Coverage

### Measuring Coverage

```bash
# Nim coverage (requires gcov)
nimble coverage_nim

# Python coverage
pip install coverage
coverage run -m unittest tests/harness/test_runner.py
coverage report
```

### Coverage Goals

- Core modules: >90%
- SQL execution: >85%
- VFS: >80%
- CLI: >75%

## Continuous Integration

Tests run automatically on:

- Every pull request
- Every push to main
- Daily scheduled builds

**Platforms tested:**
- Ubuntu 22.04
- macOS 12
- Windows Server 2022

## Debugging Test Failures

### Nim Tests

```bash
# Run with verbose output
nim c -r tests/nim/test_wal.nim --verbose

# Run specific test
nim c -r tests/nim/test_wal.nim --run="specific test name"

# Debug build
nim c -d:debug -r tests/nim/test_wal.nim
```

### Python Tests

```bash
# Verbose output
python -m unittest tests/harness/test_runner.py -v

# Specific test
python -m unittest tests.harness.test_runner.TestClass.test_method -v

# With debugger
python -m pdb -m unittest tests.harness.test_runner
```

### Common Issues

**"Table not found" in tests:**
- Check if previous test cleaned up
- Use unique database names per test
- Or use `:memory:` for in-memory tests

**Timing-related failures:**
- Tests may fail on slow CI runners
- Increase timeouts if needed
- Mark as flaky if necessary

**Platform-specific failures:**
- Check for path separators (/ vs \)
- Use os.path.join() for paths
- Be careful with line endings

## Test Best Practices

1. **Isolate Tests**
   - Each test should be independent
   - Clean up resources in teardown

2. **Use Descriptive Names**
   - `test_insert_updates_index` not `test1`

3. **Test Edge Cases**
   - Empty inputs
   - Maximum values
   - Error conditions

4. **Keep Tests Fast**
   - Use small datasets
   - Mock external dependencies

5. **Document Intent**
   - Comments explaining what and why
   - Link to issue if fixing bug

## Test Maintenance

### Adding Tests for New Features

Every new feature should include:
- Unit tests for core functionality
- Integration tests with other modules
- Property tests if applicable
- Documentation examples

### Updating Tests

When changing behavior:
- Update tests to match new behavior
- Document breaking changes
- Keep old tests for backward compatibility checks

## Further Reading

- [Building from Source](building.md)
- [Contributing](contributing.md)
- [Architecture Overview](../../architecture/overview.md)
