# DecentDB SQL Enhancements Roadmap

**Document Status:** Draft  
**Created:** 2026-03-25  
**Last Updated:** 2026-03-25  
**Purpose:** Comprehensive analysis of SQL features missing from DecentDB with implementation recommendations

---

## Executive Summary

This document catalogs SQL features currently missing from DecentDB that would enhance its competitiveness as an embedded relational database. Features are prioritized based on:

1. **User demand** - How commonly requested the feature is in embedded database workloads
2. **Implementation complexity** - Estimated effort relative to architectural impact
3. **Ecosystem alignment** - Parity with SQLite, PostgreSQL, and DuckDB
4. **Analytics value** - Importance for analytical query patterns

DecentDB currently has strong foundational SQL support including CTEs, window functions, JSON operations, and comprehensive DDL/DML. The gaps identified below represent opportunities to expand utility for both OLTP and OLAP workloads.

---

## Slice Map

The following table tracks the implementation status of each feature slice. Slices are ordered by recommended implementation priority.

| Slice | Category | Priority | Status | Est. Effort |
|-------|----------|----------|--------|-------------|
| S1 | Window Function Enhancements | High | 🟢 Completed | Medium |
| S2 | Aggregate Functions (Statistical) | High | 🟢 Completed | Low |
| S3 | DML Enhancements (RETURNING, TRUNCATE) | High | 🟢 Completed | Medium |
| S4 | Trigonometric Math Functions | High | 🟢 Completed | Low |
| S5 | String Functions (Extended) | High | 🟢 Completed | Low |
| S6 | Conditional Functions | High | 🟢 Completed | Low |
| S7 | Date/Time Functions (Extended) | Medium | 🟢 Completed | Medium |
| S8 | Subquery Operators (EXISTS, ANY, ALL) | Medium | 🟢 Completed | Medium |
| S9 | Query Features (LATERAL, VALUES, CTAS) | Medium | 🟢 Completed | Medium |
| S10 | Comparison Operators (IS DISTINCT FROM) | Medium | 🟢 Completed | Low |
| S11 | DDL Enhancements | Medium | 🟢 Completed | Medium |
| S12 | Utility Commands (EXPLAIN, PRAGMA subset) | Medium | 🟢 Completed | Medium |
| S13 | Advanced Features | Low | 🟡 In Progress | High |

**Status Legend:**
- 🔴 Not Started
- 🟡 In Progress
- 🟢 Completed
- ⚪ Deferred

---

## S1: Window Function Enhancements

### Overview

Window functions are critical for analytical workloads. DecentDB currently supports ranking and value functions (`ROW_NUMBER`, `RANK`, `DENSE_RANK`, `LAG`, `LEAD`, `FIRST_VALUE`, `LAST_VALUE`, `NTH_VALUE`) but lacks window frame specifications and additional ranking functions.

### Missing Features

#### 1.1 Window Frame Specifications

Window frames allow defining a subset of rows within a partition for aggregate calculations. This enables running totals, moving averages, and cumulative calculations.

**Syntax:**
```sql
{ ROWS | RANGE } {
    BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
    | BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING
    | BETWEEN <expr> PRECEDING AND <expr> FOLLOWING
    | <expr> PRECEDING
    | CURRENT ROW
}
```

**Examples:**
```sql
-- Running total
SELECT 
    order_date,
    amount,
    SUM(amount) OVER (
        ORDER BY order_date 
        ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
    ) AS running_total
FROM orders;

-- Moving average (3-day window)
SELECT 
    order_date,
    amount,
    AVG(amount) OVER (
        ORDER BY order_date
        ROWS BETWEEN 1 PRECEDING AND 1 FOLLOWING
    ) AS moving_avg
FROM orders;

-- Cumulative count with partition
SELECT 
    department,
    employee_id,
    COUNT(*) OVER (
        PARTITION BY department
        ORDER BY hire_date
        ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
    ) AS dept_hire_rank
FROM employees;
```

**Implementation Notes:**
- Requires extending the window function executor to track frame boundaries
- `ROWS` framing is based on physical row offsets
- `RANGE` framing is based on logical value ranges (more complex)
- Default frame for ordered windows is `RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW`
- Default frame for unordered windows is entire partition

**Affected Components:**
- SQL parser (frame clause syntax)
- Query planner (frame boundary calculation)
- Window function executor (frame-aware aggregation)

---

#### 1.2 NTILE() Function

Divides rows into a specified number of roughly equal buckets.

**Syntax:**
```sql
NTILE(num_buckets) OVER ([partition_clause] [order_clause])
```

**Examples:**
```sql
-- Divide employees into 4 quartiles by salary
SELECT 
    name,
    salary,
    NTILE(4) OVER (ORDER BY salary DESC) AS salary_quartile
FROM employees;

-- Divide into percentiles within each department
SELECT 
    name,
    department,
    salary,
    NTILE(100) OVER (PARTITION BY department ORDER BY salary DESC) AS dept_percentile
FROM employees;
```

**Implementation Notes:**
- Requires knowing total row count per partition before assignment
- May need two-pass execution or buffering
- Returns integer from 1 to `num_buckets`

---

#### 1.3 PERCENT_RANK() Function

Calculates the relative rank of a row as a percentage from 0 to 1.

**Syntax:**
```sql
PERCENT_RANK() OVER ([partition_clause] order_clause)
```

**Formula:**
```
(rank - 1) / (total_rows - 1)
```

**Examples:**
```sql
SELECT 
    name,
    salary,
    PERCENT_RANK() OVER (ORDER BY salary) AS salary_percentile
FROM employees;
-- Returns 0.0 for lowest, 1.0 for highest, linear interpolation between
```

---

#### 1.4 CUME_DIST() Function

Calculates the cumulative distribution of a row - the proportion of rows with values less than or equal to the current row.

**Syntax:**
```sql
CUME_DIST() OVER ([partition_clause] order_clause)
```

**Formula:**
```
count of rows with value <= current value / total rows
```

**Examples:**
```sql
SELECT 
    name,
    score,
    CUME_DIST() OVER (ORDER BY score) AS cumulative_distribution
FROM test_results;
-- Shows what fraction of students scored <= each score
```

---

#### 1.5 Aggregate Window Functions

Allow using standard aggregate functions (`SUM`, `AVG`, `COUNT`, `MIN`, `MAX`) as window functions with frame specifications.

**Examples:**
```sql
-- Running sum
SELECT date, amount, SUM(amount) OVER (ORDER BY date) AS running_sum
FROM sales;

-- Partitioned running count
SELECT 
    department,
    employee_id,
    COUNT(*) OVER (PARTITION BY department ORDER BY hire_date) AS dept_count
FROM employees;

-- Min/Max in sliding window
SELECT 
    timestamp,
    value,
    MIN(value) OVER (ORDER BY timestamp ROWS BETWEEN 5 PRECEDING AND CURRENT ROW) AS min_6,
    MAX(value) OVER (ORDER BY timestamp ROWS BETWEEN 5 PRECEDING AND CURRENT ROW) AS max_6
FROM sensor_readings;
```

**Implementation Notes:**
- Requires aggregate state to be maintained across frame movements
- May need efficient add/remove semantics for sliding windows
- Consider optimizations for common frame patterns

---

### S1 Implementation Priority

| Feature | Complexity | User Value | Recommended Order |
|---------|------------|------------|-------------------|
| Aggregate window functions | Medium | Very High | 1 |
| ROWS frame specification | Medium | Very High | 2 |
| NTILE() | Low | High | 3 |
| PERCENT_RANK() | Low | Medium | 4 |
| CUME_DIST() | Low | Medium | 5 |
| RANGE frame specification | High | Medium | 6 |

---

## S2: Aggregate Functions (Statistical)

### Overview

Statistical aggregate functions are essential for data analysis, reporting, and scientific computing workloads. DecentDB currently lacks variance, standard deviation, median, and percentile functions.

### Missing Features

#### 2.1 Standard Deviation Functions

**Functions:**
- `STDDEV(expr)` - Sample standard deviation (alias for `STDDEV_SAMP`)
- `STDDEV_SAMP(expr)` - Sample standard deviation (n-1 denominator)
- `STDDEV_POP(expr)` - Population standard deviation (n denominator)

**Syntax:**
```sql
STDDEV(expr) [FILTER (WHERE condition)]
STDDEV_SAMP(expr) [FILTER (WHERE condition)]
STDDEV_POP(expr) [FILTER (WHERE condition)]
```

**Examples:**
```sql
-- Sample standard deviation of salaries
SELECT STDDEV(salary) FROM employees;

-- Population standard deviation by department
SELECT 
    department,
    STDDEV_POP(salary) AS pop_stddev
FROM employees
GROUP BY department;

-- With FILTER clause
SELECT 
    STDDEV(score) FILTER (WHERE score > 0) AS valid_score_stddev
FROM test_results;
```

**Formulas:**
```
STDDEV_SAMP = SQRT(SUM((x - mean)^2) / (n - 1))
STDDEV_POP = SQRT(SUM((x - mean)^2) / n)
```

**Implementation Notes:**
- Can use Welford's online algorithm for numerical stability
- Handle NULL values (exclude from calculation)
- Return NULL for single-row sample in STDDEV_SAMP
- Return 0 for single-row sample in STDDEV_POP

---

#### 2.2 Variance Functions

**Functions:**
- `VARIANCE(expr)` - Sample variance (alias for `VAR_SAMP`)
- `VAR_SAMP(expr)` - Sample variance (n-1 denominator)
- `VAR_POP(expr)` - Population variance (n denominator)

**Syntax:**
```sql
VARIANCE(expr) [FILTER (WHERE condition)]
VAR_SAMP(expr) [FILTER (WHERE condition)]
VAR_POP(expr) [FILTER (WHERE condition)]
```

**Examples:**
```sql
SELECT 
    department,
    VAR_POP(salary) AS pop_variance,
    STDDEV_POP(salary) AS pop_stddev
FROM employees
GROUP BY department;
```

**Implementation Notes:**
- Variance is standard deviation squared
- Can share implementation with STDDEV functions
- Same NULL handling as STDDEV

---

#### 2.3 MEDIAN() Function

Returns the median (middle value) of a set. For even counts, returns the average of the two middle values.

**Syntax:**
```sql
MEDIAN(expr) [FILTER (WHERE condition)]
```

**Examples:**
```sql
-- Median salary
SELECT MEDIAN(salary) FROM employees;

-- Median by department
SELECT 
    department,
    MEDIAN(salary) AS median_salary
FROM employees
GROUP BY department;
```

**Implementation Notes:**
- Requires sorting or partial sorting of values
- For large datasets, consider approximate algorithms (t-digest)
- Handle NULL values (exclude from calculation)
- Return NULL for empty set

---

#### 2.4 Percentile Functions

**Functions:**
- `PERCENTILE_CONT(fraction)` - Continuous percentile (interpolates between values)
- `PERCENTILE_DISC(fraction)` - Discrete percentile (nearest actual value)

**Syntax:**
```sql
PERCENTILE_CONT(fraction) WITHIN GROUP (ORDER BY expr)
PERCENTILE_DISC(fraction) WITHIN GROUP (ORDER BY expr)
```

**Examples:**
```sql
-- 50th percentile (median) with interpolation
SELECT PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY salary) FROM employees;

-- 25th, 50th, 75th percentiles
SELECT 
    PERCENTILE_CONT(0.25) WITHIN GROUP (ORDER BY salary) AS p25,
    PERCENTILE_CONT(0.50) WITHIN GROUP (ORDER BY salary) AS p50,
    PERCENTILE_CONT(0.75) WITHIN GROUP (ORDER BY salary) AS p75
FROM employees;

-- Discrete percentile (actual value)
SELECT PERCENTILE_DISC(0.5) WITHIN GROUP (ORDER BY salary) FROM employees;

-- By department
SELECT 
    department,
    PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY salary) AS median_salary
FROM employees
GROUP BY department;
```

**Implementation Notes:**
- `WITHIN GROUP` syntax is SQL standard
- Requires ordered set aggregate implementation
- `PERCENTILE_CONT` interpolates linearly between adjacent values
- `PERCENTILE_DISC` returns the first input value whose position equals or exceeds the fraction

---

#### 2.5 ARRAY_AGG() Function

Collects values into an array (or JSON array in DecentDB's context).

**Syntax:**
```sql
ARRAY_AGG(expr [ORDER BY sort_expr] [LIMIT n])
```

**Examples:**
```sql
-- Collect all employee names into an array
SELECT department, ARRAY_AGG(name) FROM employees GROUP BY department;

-- Ordered array
SELECT department, ARRAY_AGG(name ORDER BY salary DESC) AS employees_by_salary
FROM employees
GROUP BY department;

-- Limited array
SELECT department, ARRAY_AGG(name ORDER BY hire_date LIMIT 5) AS first_five_hired
FROM employees
GROUP BY department;

-- Distinct values
SELECT ARRAY_AGG(DISTINCT category) FROM products;
```

**Implementation Notes:**
- Return type should be JSON array for DecentDB
- Consider memory limits for large aggregations
- Support `DISTINCT` modifier

---

#### 2.6 Boolean Aggregation Functions

**Functions:**
- `BOOL_AND(expr)` - Returns TRUE if all values are TRUE
- `BOOL_OR(expr)` - Returns TRUE if any value is TRUE

**Syntax:**
```sql
BOOL_AND(expr)
BOOL_OR(expr)
```

**Examples:**
```sql
-- Check if all employees in a department are active
SELECT 
    department,
    BOOL_AND(active) AS all_active
FROM employees
GROUP BY department;

-- Check if any employee exceeds salary threshold
SELECT 
    department,
    BOOL_OR(salary > 100000) AS has_high_earner
FROM employees
GROUP BY department;
```

**Implementation Notes:**
- `BOOL_AND` is equivalent to `MIN` on boolean values
- `BOOL_OR` is equivalent to `MAX` on boolean values
- Return NULL for empty set (or FALSE per SQL standard - verify)
- Ignore NULL values in input

---

### S2 Implementation Priority

| Feature | Complexity | User Value | Recommended Order |
|---------|------------|------------|-------------------|
| STDDEV/STDDEV_POP/STDDEV_SAMP | Low | Very High | 1 |
| VAR_SAMP/VAR_POP | Low | Very High | 2 |
| ARRAY_AGG | Medium | High | 3 |
| MEDIAN | Medium | High | 4 |
| PERCENTILE_CONT/PERCENTILE_DISC | Medium | High | 5 |
| BOOL_AND/BOOL_OR | Low | Medium | 6 |

---

## S3: DML Enhancements

### Overview

DML enhancements improve the ergonomics of data manipulation operations. `UPDATE ... RETURNING`, `DELETE ... RETURNING`, and `TRUNCATE TABLE` are implemented in the current engine; `MERGE` remains the notable backlog item in this category.

### Implemented Features and Remaining Gap

#### 3.1 UPDATE ... RETURNING

Returns the affected rows after an UPDATE operation.

**Syntax:**
```sql
UPDATE table_name 
SET column = expr [, ...]
[WHERE condition]
RETURNING expr [[AS] alias] [, ...]
```

**Examples:**
```sql
-- Update and return the new values
UPDATE accounts 
SET balance = balance - 100 
WHERE id = 1 
RETURNING id, balance;

-- Update and return old and new values
UPDATE products 
SET price = price * 1.1 
WHERE category = 'electronics'
RETURNING 
    id,
    price AS new_price,
    (price / 1.1) AS old_price;

-- Soft delete with audit trail
UPDATE users 
SET deleted_at = CURRENT_TIMESTAMP, 
    deleted_by = 'admin'
WHERE id IN (1, 2, 3)
RETURNING id, deleted_at;
```

**Implementation Notes:**
- Requires capturing rows before and/or after modification
- Consider supporting `OLD` and `NEW` qualifiers for column references
- Works well with the existing INSERT RETURNING infrastructure

---

#### 3.2 DELETE ... RETURNING

Returns the deleted rows.

**Syntax:**
```sql
DELETE FROM table_name
[WHERE condition]
RETURNING expr [[AS] alias] [, ...]
```

**Examples:**
```sql
-- Delete and return what was deleted
DELETE FROM temp_data 
WHERE created_at < '2024-01-01'
RETURNING id, created_at;

-- Archive before delete pattern
DELETE FROM active_sessions 
WHERE last_activity < CURRENT_TIMESTAMP - INTERVAL '1 hour'
RETURNING *;
```

**Implementation Notes:**
- Must capture row data before deletion
- Useful for audit trails and archiving

---

#### 3.3 TRUNCATE TABLE

Quickly removes all rows from a table while preserving transactional rollback semantics.

**Syntax:**
```sql
TRUNCATE TABLE table_name [CONTINUE IDENTITY | RESTART IDENTITY] [CASCADE]
```

**Examples:**
```sql
-- Basic truncate
TRUNCATE TABLE temp_data;

-- Reset auto-increment counter
TRUNCATE TABLE users RESTART IDENTITY;

-- Truncate with cascade (if foreign keys exist)
TRUNCATE TABLE orders CASCADE;
```

**Implementation Notes:**
- More efficient than `DELETE FROM table` (no row-by-row processing)
- Participates in DecentDB transactions and can be rolled back
- `RESTART IDENTITY` resets auto-increment state; `CONTINUE IDENTITY` preserves progression
- `CASCADE` truncates dependent child tables reached through foreign keys
- DDL transaction semantics apply

---

#### 3.4 MERGE Statement

SQL-standard upsert pattern for conditional insert/update/delete.

**Syntax:**
```sql
MERGE INTO target_table [AS alias]
USING source_table | (subquery) [AS alias]
ON (condition)
WHEN MATCHED [AND condition] THEN
    UPDATE SET column = expr [, ...]
    | DELETE
WHEN NOT MATCHED [AND condition] THEN
    INSERT (columns) VALUES (values)
```

**Examples:**
```sql
-- Sync from staging to main table
MERGE INTO products AS target
USING staging_products AS source
ON target.id = source.id
WHEN MATCHED AND source.deleted = 1 THEN DELETE
WHEN MATCHED THEN UPDATE SET
    name = source.name,
    price = source.price
WHEN NOT MATCHED THEN INSERT (id, name, price)
    VALUES (source.id, source.name, source.price);

-- Conditional merge
MERGE INTO accounts AS target
USING (SELECT account_id, amount FROM transactions) AS source
ON target.id = source.account_id
WHEN MATCHED THEN UPDATE SET
    balance = target.balance + source.amount
WHEN NOT MATCHED THEN INSERT (id, balance)
    VALUES (source.account_id, source.amount);
```

**Implementation Notes:**
- Complex feature requiring careful planning
- Can be decomposed into INSERT ON CONFLICT for simpler cases
- Consider phased implementation:
  1. Basic INSERT/UPDATE merge
  2. Add DELETE clause
  3. Add additional conditions

---

### S3 Implementation Priority

| Feature | Complexity | User Value | Recommended Order |
|---------|------------|------------|-------------------|
| TRUNCATE TABLE | Low | High | 1 |
| UPDATE RETURNING | Medium | Very High | 2 |
| DELETE RETURNING | Medium | Very High | 3 |
| MERGE | High | High | 4 |

---

## S4: Trigonometric Math Functions

### Overview

Trigonometric functions are essential for scientific computing, geospatial calculations, and engineering applications.

### Missing Functions

| Function | Description | Example |
|----------|-------------|---------|
| `SIN(x)` | Sine (radians) | `SIN(PI()/2)` → 1.0 |
| `COS(x)` | Cosine (radians) | `COS(0)` → 1.0 |
| `TAN(x)` | Tangent (radians) | `TAN(PI()/4)` → 1.0 |
| `ASIN(x)` | Arc sine | `ASIN(1)` → 1.5707... |
| `ACOS(x)` | Arc cosine | `ACOS(0)` → 1.5707... |
| `ATAN(x)` | Arc tangent | `ATAN(1)` → 0.7853... |
| `ATAN2(y, x)` | Arc tangent of y/x | `ATAN2(1, 1)` → 0.7853... |
| `PI()` | π constant | `PI()` → 3.14159... |
| `DEGREES(x)` | Radians to degrees | `DEGREES(PI())` → 180.0 |
| `RADIANS(x)` | Degrees to radians | `RADIANS(180)` → 3.14159... |
| `COT(x)` | Cotangent | `COT(PI()/4)` → 1.0 |

### Examples

```sql
-- Basic trigonometry
SELECT SIN(PI()/2), COS(0), TAN(PI()/4);
-- Results: 1.0, 1.0, 1.0

-- Inverse functions
SELECT ASIN(1), ACOS(0), ATAN(1);
-- Results: ~1.5708, ~1.5708, ~0.7854

-- ATAN2 for proper quadrant handling
SELECT ATAN2(1, 1), ATAN2(-1, -1);
-- Results: ~0.7854, ~-2.3562

-- Angle conversions
SELECT DEGREES(PI()), RADIANS(180);
-- Results: 180.0, 3.14159...

-- Calculate distance between two points (Haversine formula components)
SELECT 
    lat1, lon1, lat2, lon2,
    SIN(RADIANS(lat2 - lat1) / 2) AS dlat_sin,
    SIN(RADIANS(lon2 - lon1) / 2) AS dlon_sin
FROM locations;
```

### Implementation Notes

- All functions operate on and return `FLOAT64`/`DOUBLE PRECISION`
- Input angles in radians (except `DEGREES`/`RADIANS`)
- Handle edge cases:
  - `ASIN`/`ACOS`: Return NULL or error for values outside [-1, 1]
  - `TAN`: Handle π/2 + nπ (undefined)
  - `COT`: Handle nπ (undefined)
- Use standard library implementations (Rust `std::f64` consts and methods)
- Consider adding `COSH`, `SINH`, `TANH` for hyperbolic functions

---

## S5: String Functions (Extended)

### Overview

Extended string functions improve text processing capabilities for data cleaning, transformation, and analysis.

### Missing Functions

#### 5.1 CONCAT() and CONCAT_WS()

**Syntax:**
```sql
CONCAT(expr [, expr ...])           -- Concatenate with NULL handling
CONCAT_WS(separator, expr [, ...])  -- Concatenate with separator
```

**Examples:**
```sql
-- CONCAT (NULL-safe concatenation)
SELECT CONCAT('Hello', ' ', 'World');           -- 'Hello World'
SELECT CONCAT('Hello', NULL, 'World');          -- 'HelloWorld' (NULL skipped)
SELECT CONCAT(first_name, ' ', last_name) AS full_name FROM users;

-- CONCAT_WS (with separator)
SELECT CONCAT_WS('-', '2024', '03', '25');      -- '2024-03-25'
SELECT CONCAT_WS(', ', 'Alice', NULL, 'Bob');   -- 'Alice, Bob' (NULL skipped)
SELECT CONCAT_WS(' ', first_name, middle_name, last_name) FROM users;
```

**Implementation Notes:**
- `CONCAT` treats NULL as empty string (unlike `||` which returns NULL)
- `CONCAT_WS` skips NULL values entirely
- `CONCAT_WS` with NULL separator returns NULL

---

#### 5.2 POSITION()

**Syntax:**
```sql
POSITION(substring IN string)
```

**Examples:**
```sql
SELECT POSITION('world' IN 'hello world');  -- 7
SELECT POSITION('xyz' IN 'hello world');    -- 0 (not found)
```

**Implementation Notes:**
- SQL-standard syntax (alternative to `INSTR`)
- Returns 1-based position, 0 if not found
- Equivalent to `INSTR(string, substring)`

---

#### 5.3 INITCAP()

**Syntax:**
```sql
INITCAP(string)
```

**Examples:**
```sql
SELECT INITCAP('hello world');      -- 'Hello World'
SELECT INITCAP('JOHN DOE');         -- 'John Doe'
SELECT INITCAP('macdonald');        -- 'Macdonald'
```

**Implementation Notes:**
- Capitalizes first letter of each word
- Lowercases remaining letters
- Word boundaries are whitespace

---

#### 5.4 ASCII()

**Syntax:**
```sql
ASCII(string)
```

**Examples:**
```sql
SELECT ASCII('A');        -- 65
SELECT ASCII('ABC');      -- 65 (first character only)
SELECT ASCII('');         -- NULL
```

**Implementation Notes:**
- Returns ASCII code of first character
- Returns NULL for empty string
- Complement of `CHR()`

---

#### 5.5 REGEXP_REPLACE()

**Syntax:**
```sql
REGEXP_REPLACE(string, pattern, replacement [, flags])
```

**Examples:**
```sql
-- Remove digits
SELECT REGEXP_REPLACE('abc123def', '\d', '', 'g');  -- 'abcdef'

-- Replace whitespace
SELECT REGEXP_REPLACE('hello   world', '\s+', ' '); -- 'hello world'

-- Case-insensitive replacement
SELECT REGEXP_REPLACE('Hello World', 'hello', 'Hi', 'i'); -- 'Hi World'

-- Extract and reformat phone number
SELECT REGEXP_REPLACE(
    'Phone: (555) 123-4567',
    '.*\((\d{3})\)\s*(\d{3})-(\d{4}).*',
    '\1-\2-\3'
);  -- '555-123-4567'
```

**Implementation Notes:**
- Requires regex engine integration
- Consider using `regex` crate (already in ecosystem)
- Flags: 'i' (case-insensitive), 'g' (global), 'm' (multiline)
- Default: replace first occurrence only

---

#### 5.6 Additional String Functions

| Function | Description | Example |
|----------|-------------|---------|
| `SPLIT_PART(string, delimiter, index)` | Split and get nth part | `SPLIT_PART('a,b,c', ',', 2)` → 'b' |
| `STRING_TO_ARRAY(string, delimiter)` | Split into array | `STRING_TO_ARRAY('a,b,c', ',')` → ['a','b','c'] |
| `QUOTE_IDENT(string)` | Quote as identifier | `QUOTE_IDENT('table name')` → '"table name"' |
| `QUOTE_LITERAL(string)` | Quote as literal | `QUOTE_LITERAL("O'Brien")` → '''O''''Brien''' |
| `MD5(string)` | MD5 hash | `MD5('hello')` → '5d41402abc4b2a76b9719d911017c592' |
| `SHA256(string)` | SHA-256 hash | Returns hex string |

---

## S6: Conditional Functions

### Overview

Conditional functions simplify common patterns for selecting values based on conditions.

### Missing Functions

#### 6.1 GREATEST() and LEAST()

**Syntax:**
```sql
GREATEST(expr [, expr ...])  -- Maximum of arguments
LEAST(expr [, expr ...])     -- Minimum of arguments
```

**Examples:**
```sql
-- Maximum of multiple values
SELECT GREATEST(10, 20, 5, 15);              -- 20
SELECT GREATEST(salary, bonus, commission) AS max_compensation FROM employees;

-- Minimum of multiple values
SELECT LEAST(10, 20, 5, 15);                 -- 5
SELECT LEAST(price1, price2, price3) AS best_price FROM products;

-- With NULL handling
SELECT GREATEST(10, NULL, 20);               -- NULL (any NULL = result NULL)
SELECT LEAST('a', 'b', 'c');                 -- 'a' (works with strings)

-- Practical use: cap a value
SELECT LEAST(requested_amount, max_allowed) AS approved_amount FROM requests;

-- Practical use: ensure minimum
SELECT GREATEST(calculated_value, 0) AS non_negative FROM calculations;
```

**Implementation Notes:**
- Return NULL if any argument is NULL
- Work with any comparable type
- Can be implemented as a series of `CASE` expressions internally

---

#### 6.2 IIF()

**Syntax:**
```sql
IIF(condition, true_value, false_value)
```

**Examples:**
```sql
-- Simple conditional
SELECT IIF(score >= 60, 'Pass', 'Fail') AS result FROM tests;

-- Nested IIF
SELECT IIF(
    score >= 90, 'A',
    IIF(score >= 80, 'B',
        IIF(score >= 70, 'C', 'F'))
) AS grade FROM tests;

-- With NULL
SELECT IIF(active = TRUE, 'Active', 'Inactive') AS status FROM users;
```

**Implementation Notes:**
- Shorthand for `CASE WHEN condition THEN true_value ELSE false_value END`
- SQL Server/SQLite compatibility
- PostgreSQL doesn't have IIF (uses CASE)

---

#### 6.3 NULLIF() Enhancement

Already implemented, but document for completeness:

```sql
-- Returns NULL if expr1 = expr2
SELECT NULLIF(value, 0) FROM data;  -- Returns NULL instead of 0
```

---

## S7: Date/Time Functions (Extended)

### Overview

Extended date/time functions enable more sophisticated temporal calculations and formatting.

### Missing Functions

#### 7.1 DATE_TRUNC()

Truncates a timestamp to the specified precision.

**Syntax:**
```sql
DATE_TRUNC(precision, timestamp)
```

**Examples:**
```sql
-- Truncate to month
SELECT DATE_TRUNC('month', '2024-03-15 14:30:00');  -- '2024-03-01 00:00:00'

-- Truncate to year
SELECT DATE_TRUNC('year', '2024-03-15');            -- '2024-01-01 00:00:00'

-- Truncate to day
SELECT DATE_TRUNC('day', '2024-03-15 14:30:00');    -- '2024-03-15 00:00:00'

-- Truncate to hour
SELECT DATE_TRUNC('hour', '2024-03-15 14:30:45');   -- '2024-03-15 14:00:00'

-- Group by week
SELECT DATE_TRUNC('week', created_at) AS week_start, COUNT(*)
FROM orders
GROUP BY DATE_TRUNC('week', created_at);
```

**Precision Options:**
- `microseconds`, `milliseconds`, `second`, `minute`, `hour`
- `day`, `week`, `month`, `quarter`, `year`, `decade`, `century`, `millennium`

---

#### 7.2 AGE()

Calculates the interval between two timestamps or from a timestamp to now.

**Syntax:**
```sql
AGE(timestamp)                    -- Age from now
AGE(timestamp1, timestamp2)       -- Interval between timestamps
```

**Examples:**
```sql
-- Age from now
SELECT AGE('2020-01-01');          -- e.g., '4 years 2 mons 24 days'

-- Interval between dates
SELECT AGE('2024-03-15', '2024-01-01');  -- '2 mons 15 days'

-- Calculate employee tenure
SELECT name, AGE(hire_date) AS tenure FROM employees;
```

---

#### 7.3 TO_TIMESTAMP()

Converts string to timestamp with format specification.

**Syntax:**
```sql
TO_TIMESTAMP(string, format)
TO_TIMESTAMP(unix_epoch)
```

**Examples:**
```sql
-- Parse with format
SELECT TO_TIMESTAMP('2024-03-15 14:30:00', 'YYYY-MM-DD HH24:MI:SS');

-- Unix epoch to timestamp
SELECT TO_TIMESTAMP(1710505800);  -- Unix timestamp

-- Various formats
SELECT TO_TIMESTAMP('15/03/2024', 'DD/MM/YYYY');
SELECT TO_TIMESTAMP('March 15, 2024', 'FMMonth DD, YYYY');
```

---

#### 7.4 INTERVAL Arithmetic

**Syntax:**
```sql
timestamp + INTERVAL 'duration'
timestamp - INTERVAL 'duration'
```

**Examples:**
```sql
-- Add intervals
SELECT CURRENT_TIMESTAMP + INTERVAL '1 day';
SELECT CURRENT_TIMESTAMP + INTERVAL '1 month';
SELECT CURRENT_TIMESTAMP + INTERVAL '1 year 2 months 3 days';

-- Subtract intervals
SELECT CURRENT_DATE - INTERVAL '7 days';

-- Date math
SELECT 
    order_date,
    order_date + INTERVAL '30 days' AS due_date
FROM orders;

-- Interval from subtraction
SELECT '2024-03-15'::date - '2024-01-01'::date AS days_between;
```

---

#### 7.5 Additional Date Functions

| Function | Description | Example |
|----------|-------------|---------|
| `DATE_PART(part, timestamp)` | Extract date part | `DATE_PART('day', timestamp)` |
| `DATE_DIFF(part, t1, t2)` | Difference in specified units | `DATE_DIFF('day', t1, t2)` |
| `LAST_DAY(date)` | Last day of month | `LAST_DAY('2024-03-15')` → '2024-03-31' |
| `NEXT_DAY(date, weekday)` | Next occurrence of weekday | `NEXT_DAY('2024-03-15', 'Monday')` |
| `MAKE_DATE(year, month, day)` | Construct date | `MAKE_DATE(2024, 3, 15)` |
| `MAKE_TIMESTAMP(...)` | Construct timestamp | `MAKE_TIMESTAMP(2024, 3, 15, 14, 30, 0)` |

---

## S8: Subquery Operators (EXISTS, ANY, ALL)

### Overview

Subquery operators enable more expressive queries for existence checks and quantified comparisons.

### Missing Features

#### 8.1 EXISTS / NOT EXISTS

**Syntax:**
```sql
WHERE EXISTS (subquery)
WHERE NOT EXISTS (subquery)
```

**Examples:**
```sql
-- Find users with orders
SELECT * FROM users u
WHERE EXISTS (
    SELECT 1 FROM orders o WHERE o.user_id = u.id
);

-- Find users without orders
SELECT * FROM users u
WHERE NOT EXISTS (
    SELECT 1 FROM orders o WHERE o.user_id = u.id
);

-- Correlated EXISTS
SELECT * FROM products p
WHERE EXISTS (
    SELECT 1 FROM order_items oi
    JOIN orders o ON oi.order_id = o.id
    WHERE oi.product_id = p.id
    AND o.created_at > CURRENT_DATE - INTERVAL '30 days'
);
```

**Implementation Notes:**
- EXISTS returns TRUE if subquery returns any rows
- Optimization: stop scanning after first match
- Often more efficient than IN for correlated subqueries

---

#### 8.2 ANY / SOME

**Syntax:**
```sql
expr op ANY (subquery)
expr op SOME (subquery)  -- SOME is synonym for ANY
```

**Examples:**
```sql
-- Salary greater than any employee in department 5
SELECT * FROM employees
WHERE salary > ANY (
    SELECT salary FROM employees WHERE department_id = 5
);

-- Equivalent to: salary > (SELECT MIN(salary) FROM employees WHERE department_id = 5)

-- User in any of the specified roles
SELECT * FROM users
WHERE role = ANY (SELECT role_name FROM active_roles);

-- With array literal
SELECT * FROM products
WHERE category = ANY ('electronics', 'books', 'toys');
```

**Implementation Notes:**
- `expr = ANY (subquery)` equivalent to `expr IN (subquery)`
- `expr > ANY (subquery)` means greater than at least one
- `expr < ANY (subquery)` means less than at least one
- Returns NULL if subquery is empty and expr is not NULL

---

#### 8.3 ALL

**Syntax:**
```sql
expr op ALL (subquery)
```

**Examples:**
```sql
-- Salary greater than all employees in department 5
SELECT * FROM employees
WHERE salary > ALL (
    SELECT salary FROM employees WHERE department_id = 5
);

-- Equivalent to: salary > (SELECT MAX(salary) FROM employees WHERE department_id = 5)

-- Product price less than all competitors
SELECT * FROM products p
WHERE price < ALL (
    SELECT competitor_price FROM competitor_prices cp
    WHERE cp.product_id = p.id
);

-- Not equal to all (none match)
SELECT * FROM users
WHERE email != ALL (SELECT email FROM banned_emails);
```

**Implementation Notes:**
- `expr > ALL (subquery)` means greater than every value
- `expr = ALL (subquery)` means equal to every value (all same)
- Returns TRUE if subquery is empty (vacuous truth)

---

## S9: Query Features

### Overview

Additional query features for more flexible data retrieval and manipulation. The `LATERAL`, `VALUES`-table-source, and CTAS surfaces described in this slice are implemented in the current engine.

### Implemented Features

#### 9.1 LATERAL Joins

**Syntax:**
```sql
FROM table1 LEFT JOIN LATERAL (subquery referencing table1) AS alias ON true
```

**Examples:**
```sql
-- Top 3 orders per customer
SELECT c.name, o.order_id, o.amount
FROM customers c
CROSS JOIN LATERAL (
    SELECT order_id, amount
    FROM orders
    WHERE customer_id = c.id
    ORDER BY amount DESC
    LIMIT 3
) o;

-- Equivalent to correlated subquery in FROM clause
SELECT u.name, r.recent_order
FROM users u
LEFT JOIN LATERAL (
    SELECT id AS recent_order
    FROM orders
    WHERE user_id = u.id
    ORDER BY created_at DESC
    LIMIT 1
) r ON true;
```

**Implementation Notes:**
- LATERAL allows subquery to reference columns from preceding tables
- Similar to correlated subquery but in FROM clause
- Useful for "top N per group" queries

---

#### 9.2 VALUES Clause as Table Source

**Syntax:**
```sql
FROM (VALUES (v1, v2, ...), (v3, v4, ...)) AS alias(col1, col2, ...)
```

**Examples:**
```sql
-- Inline data
SELECT * FROM (VALUES (1, 'one'), (2, 'two'), (3, 'three')) AS t(num, name);

-- Join with inline values
SELECT u.name, v.status
FROM users u
JOIN (VALUES (1, 'active'), (2, 'inactive')) AS v(id, status)
    ON u.status_id = v.id;

-- Use in INSERT
INSERT INTO lookup_table (code, description)
SELECT * FROM (VALUES ('A', 'Alpha'), ('B', 'Beta')) AS t(code, desc);

-- Comparison with inline values
SELECT * FROM products
WHERE (id, category) IN (VALUES (1, 'A'), (2, 'B'), (3, 'C'));
```

**Implementation Notes:**
- Works with explicit alias column names on subqueries
- Can be used in joins and `INSERT ... SELECT` pipelines
- Supports row-value membership checks such as `(a, b) IN (VALUES (...), (...))`

---

#### 9.3 CREATE TABLE AS SELECT (CTAS)

**Syntax:**
```sql
CREATE TABLE table_name [WITH options] AS SELECT ...
CREATE TEMP TABLE table_name AS SELECT ...
```

**Examples:**
```sql
-- Create table from query
CREATE TABLE active_users AS
SELECT * FROM users WHERE active = TRUE;

-- Create temp table
CREATE TEMP TABLE recent_orders AS
SELECT * FROM orders WHERE created_at > CURRENT_DATE - INTERVAL '7 days';

-- With explicit schema
CREATE TABLE summary AS
SELECT 
    department,
    COUNT(*) AS emp_count,
    AVG(salary) AS avg_salary
FROM employees
GROUP BY department;
```

**Implementation Notes:**
- Should infer column types from query result
- Consider `WITH NO DATA` option (create structure only)
- Consider `IF NOT EXISTS` option

---

## S10: Comparison Operators

### Overview

Additional comparison operators for more expressive conditions.

### Missing Features

#### 10.1 IS [NOT] DISTINCT FROM

NULL-safe equality comparison.

**Syntax:**
```sql
expr1 IS NOT DISTINCT FROM expr2  -- True if equal (both NULL = equal)
expr1 IS DISTINCT FROM expr2      -- True if not equal (NULL != non-NULL)
```

**Examples:**
```sql
-- Standard equality (NULL = NULL returns NULL, not TRUE)
SELECT * FROM users WHERE email = NULL;        -- Returns nothing!
SELECT * FROM users WHERE email IS NULL;       -- Correct way

-- IS NOT DISTINCT FROM treats NULL as equal
SELECT * FROM users u1
JOIN users u2 ON u1.email IS NOT DISTINCT FROM u2.email;

-- Compare nullable columns
SELECT * FROM orders o1
WHERE EXISTS (
    SELECT 1 FROM orders o2
    WHERE o1.customer_id IS NOT DISTINCT FROM o2.customer_id
    AND o1.id != o2.id
);

-- IS DISTINCT FROM (not equal, NULL-safe)
SELECT * FROM products
WHERE price IS DISTINCT FROM original_price;
```

**Comparison Table:**
| a | b | a = b | a IS NOT DISTINCT FROM b |
|---|---|-------|--------------------------|
| 1 | 1 | TRUE | TRUE |
| 1 | 2 | FALSE | FALSE |
| 1 | NULL | NULL | FALSE |
| NULL | NULL | NULL | TRUE |

---

#### 10.2 Regex Operators

**Syntax:**
```sql
string ~ pattern          -- Matches regex (case-sensitive)
string ~* pattern         -- Matches regex (case-insensitive)
string !~ pattern         -- Does not match regex
string !~* pattern        -- Does not match regex (case-insensitive)
```

**Examples:**
```sql
-- Case-sensitive regex match
SELECT * FROM users WHERE email ~ '^[a-z]+@';

-- Case-insensitive regex match
SELECT * FROM products WHERE name ~* 'iphone';

-- Does not match
SELECT * FROM users WHERE phone !~ '^\d{3}-\d{3}-\d{4}$';
```

---

## S11: DDL Enhancements

### Overview

Additional DDL capabilities for schema management. The DDL forms documented in this slice are implemented in the current engine, including named `CHECK`, `FOREIGN KEY`, and `UNIQUE` constraint management via `ALTER TABLE`.

### Implemented Features

#### 11.1 ALTER TABLE RENAME (Table)

**Syntax:**
```sql
ALTER TABLE old_name RENAME TO new_name;
```

**Examples:**
```sql
ALTER TABLE users RENAME TO customers;
ALTER TABLE temp_data RENAME TO archive_data;
```

---

#### 11.2 ALTER TABLE ADD/DROP CONSTRAINT

**Syntax:**
```sql
ALTER TABLE table_name ADD CONSTRAINT constraint_name constraint_definition;
ALTER TABLE table_name DROP CONSTRAINT constraint_name;
```

**Examples:**
```sql
-- Add constraint
ALTER TABLE orders ADD CONSTRAINT fk_user
    FOREIGN KEY (user_id) REFERENCES users(id);

ALTER TABLE products ADD CONSTRAINT chk_price
    CHECK (price >= 0);

-- Drop constraint
ALTER TABLE orders DROP CONSTRAINT fk_user;
```

**Implementation Notes:**
- `ADD CONSTRAINT` supports named `CHECK`, named `FOREIGN KEY`, and named `UNIQUE` definitions
- Existing rows are validated before a new constraint is committed
- `DROP CONSTRAINT` removes named `CHECK`, `FOREIGN KEY`, and backed `UNIQUE` constraints

---

#### 11.3 Generated Columns VIRTUAL

Currently only STORED generated columns are supported. VIRTUAL columns are computed on read.

**Syntax:**
```sql
column_name type GENERATED ALWAYS AS (expression) VIRTUAL
```

**Examples:**
```sql
CREATE TABLE products (
    id INTEGER PRIMARY KEY,
    price DECIMAL(10,2),
    tax_rate DECIMAL(4,2),
    -- VIRTUAL: computed on read, not stored
    total DECIMAL(10,2) GENERATED ALWAYS AS (price * (1 + tax_rate)) VIRTUAL
);
```

---

#### 11.4 Expression Indexes

**Syntax:**
```sql
CREATE INDEX idx_name ON table_name ((expression));
```

**Examples:**
```sql
-- Index on lowercased email
CREATE INDEX idx_email_lower ON users ((LOWER(email)));

-- Index on computed value
CREATE INDEX idx_full_name ON employees ((first_name || ' ' || last_name));

-- Index on JSON field
CREATE INDEX idx_data_type ON events ((data->>'type'));
```

---

## S12: Utility Commands

### Overview

Utility commands for query analysis and database configuration.

### Missing Features

#### 12.1 EXPLAIN

**Syntax:**
```sql
EXPLAIN query;
EXPLAIN ANALYZE query;
```

**Examples:**
```sql
-- Show query plan
EXPLAIN SELECT * FROM users WHERE email = 'test@example.com';

-- Show query plan with actual execution
EXPLAIN ANALYZE SELECT * FROM orders WHERE user_id = 1;

-- Show costs
EXPLAIN (COSTS) SELECT * FROM products WHERE category = 'electronics';
```

**Implementation Notes:**
- Should show table access methods (scan vs index)
- Show join algorithms used
- Show estimated vs actual row counts (ANALYZE)
- Consider JSON output format

---

#### 12.2 PRAGMA Commands (SQLite Compatibility)

**Current status (implemented):**
- `PRAGMA page_size` query
- `PRAGMA cache_size` query
- `PRAGMA integrity_check` query
- `PRAGMA database_list` query
- `PRAGMA table_info(table)` query
- Assignment form (`PRAGMA name = value`) with constrained semantics for supported names

**Syntax:**
```sql
PRAGMA pragma_name;
PRAGMA pragma_name = value;
```

**Examples:**
```sql
-- Get/set page size
PRAGMA page_size;
PRAGMA page_size = 4096;

-- Get/set cache size
PRAGMA cache_size;
PRAGMA cache_size = 10000;

-- Integrity check
PRAGMA integrity_check;

-- Database info
PRAGMA database_list;
PRAGMA table_info(users);
```

---

## S13: Advanced Features

### Overview

Advanced features for specialized use cases. Lower priority due to complexity or niche demand.

### Missing Features

| Feature | Description | Complexity | Use Case |
|---------|-------------|------------|----------|
| `CREATE SCHEMA` | Namespace organization | ✅ Implemented | Catalog namespaces with `IF NOT EXISTS`; schema-qualified object names not yet supported |
| `CREATE SEQUENCE` | Explicit sequence objects | Medium | Custom ID generation |
| `CREATE FUNCTION` | User-defined functions | High | Extensibility |
| `CREATE TYPE` | Custom composite types | High | Complex data modeling |
| Materialized Views | Cached query results | High | Performance optimization |
| `DEFERRABLE` constraints | Transaction-scoped enforcement | Medium | Complex constraint timing |
| `EXCLUDE` constraints | Row-level constraints | High | Preventing overlaps |
| Covering indexes (`INCLUDE`) | Include non-key columns | Medium | Index-only scans |
| `GRANT` / `REVOKE` | Access control | High | Multi-user security |
| Full-text search | Text indexing and search | High | Search functionality |
| Geospatial (PostGIS-like) | Spatial data types and functions | Very High | Location-based apps |

---

## Implementation Recommendations

### Phase 1: Foundation (Estimated: 2-3 months)

Focus on high-value, lower-complexity features that significantly improve usability.

1. **S4: Trigonometric Functions** - Low complexity, high utility for scientific apps
2. **S6: Conditional Functions** - Low complexity, commonly needed
3. **S2: Statistical Aggregates (STDDEV, VAR)** - Low complexity, essential for analytics
4. **S3: TRUNCATE TABLE** - Low complexity, commonly expected

### Phase 2: Analytics Enhancement (Estimated: 3-4 months)

Build out analytical capabilities.

1. **S1: Window Frames** - Medium complexity, critical for analytics
2. **S2: MEDIAN, PERCENTILE** - Medium complexity, statistical completeness
3. **S2: ARRAY_AGG** - Medium complexity, data transformation
4. **S7: DATE_TRUNC, INTERVAL** - Medium complexity, time-series analysis

### Phase 3: Query Expressiveness (Estimated: 2-3 months)

Improve query flexibility.

1. **S8: EXISTS, ANY, ALL** - Medium complexity, SQL completeness
2. **S10: IS DISTINCT FROM** - Low complexity, NULL handling
3. **S9: VALUES clause, CTAS** - Medium complexity, convenience
4. **S3: UPDATE/DELETE RETURNING** - Medium complexity, ergonomics

### Phase 4: Advanced Features (Estimated: 3-6 months)

More complex features based on user demand.

1. **S1: RANGE frames** - High complexity, advanced analytics
2. **S9: LATERAL joins** - Medium complexity, advanced queries
3. **S3: MERGE** - High complexity, enterprise feature
4. **S12: EXPLAIN** - Medium complexity, debugging/optimization

---

## Testing Strategy

Each feature slice should include:

1. **Unit tests** - Function-level correctness
2. **Integration tests** - SQL parser and executor integration
3. **Compatibility tests** - Behavior matches PostgreSQL/SQLite where applicable
4. **Edge case tests** - NULL handling, empty sets, boundary conditions
5. **Performance tests** - No significant regression on existing workloads

---

## Documentation Requirements

For each implemented feature:

1. Update `docs/user-guide/sql-feature-matrix.md` with ✅ status
2. Add examples to relevant documentation sections
3. Update `include/decentdb.h` if C ABI is affected
4. Update binding documentation if API changes
5. Add to CHANGELOG.md

---

## References

- [PostgreSQL 17 Documentation](https://www.postgresql.org/docs/17/)
- [SQLite SQL Syntax](https://www.sqlite.org/lang.html)
- [DuckDB SQL Introduction](https://duckdb.org/docs/sql/introduction)
- [SQL:2023 Standard](https://www.iso.org/standard/76583.html)

---

## Changelog

| Date | Author | Changes |
|------|--------|---------|
| 2026-03-25 | Copilot | Initial document creation |
