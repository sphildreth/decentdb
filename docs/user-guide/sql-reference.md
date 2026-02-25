# SQL Reference

DecentDB supports a PostgreSQL-like SQL subset.

See also: [Comparison: DecentDB vs SQLite vs DuckDB](comparison.md)

## Data Definition Language (DDL)

### CREATE TABLE

```sql
CREATE TABLE table_name (
    column1 datatype [constraints],
    column2 datatype [constraints],
    ...
);
```

Example:
```sql
CREATE TABLE users (
    id INT PRIMARY KEY,
    name TEXT NOT NULL,
    email TEXT UNIQUE,
    created_at INT,
    CHECK (name IS NULL OR LENGTH(name) > 0)
);
```

Constraints:
- `PRIMARY KEY` — enforces uniqueness. `INTEGER PRIMARY KEY` columns are implicitly `NOT NULL` and support auto-increment when omitted from INSERT statements.
- `NOT NULL` — rejects NULL values.
- `UNIQUE` — enforces uniqueness via a secondary index.
- `CHECK (expression)` — row-level validation; the expression must evaluate to `TRUE` or `NULL` (only `FALSE` is a violation).
- `DEFAULT value` — default value used when column is omitted from INSERT.
- `REFERENCES table(column)` — foreign key constraint (see [Foreign Keys](#foreign-keys)).
- `GENERATED ALWAYS AS (expr) STORED` — computed column persisted on INSERT/UPDATE (see [Generated Columns](#generated-columns)).

### CREATE TEMP TABLE / CREATE TEMP VIEW

```sql
CREATE TEMP TABLE temp_results (id INT, value TEXT);
CREATE TEMP VIEW temp_summary AS SELECT category, COUNT(*) AS cnt FROM products GROUP BY category;
```

Session-scoped temporary objects that are not persisted to disk. They are visible only to the connection that created them and are dropped when the connection closes. See [ADR-0109](../../design/adr/0109-temporary-tables-views.md).

### CREATE INDEX

```sql
-- B-Tree index (default)
CREATE INDEX index_name ON table_name(column_name);

-- Trigram index for text search
CREATE INDEX index_name ON table_name USING trigram(column_name);

-- Unique index
CREATE UNIQUE INDEX index_name ON table_name(column_name);

-- Partial index (v0 subset)
CREATE INDEX index_name ON table_name(column_name) WHERE column_name IS NOT NULL;

-- Expression index (v0 subset)
CREATE INDEX index_name ON table_name((LOWER(column_name)));
```

Notes:
- Partial indexes are currently limited to single-column BTREE indexes with predicate form `column IS NOT NULL`.
- `UNIQUE` partial indexes, trigram partial indexes, multi-column partial indexes, and arbitrary partial predicates are not supported in 0.x.
- Expression indexes are currently limited to single-expression BTREE indexes with deterministic expressions:
  - column reference
  - `LOWER(col)`, `UPPER(col)`, `TRIM(col)`, `LENGTH(col)`
  - `CAST(col AS INT64|FLOAT64|TEXT|BOOL)`
- `UNIQUE` expression indexes, partial expression indexes, and multi-expression index keys are not supported in 0.x.

### DROP TABLE / DROP INDEX

```sql
DROP TABLE table_name;
DROP INDEX index_name;
```

### ALTER TABLE

Modify the structure of an existing table.

#### Add Column

```sql
ALTER TABLE table_name ADD COLUMN column_name datatype [constraints];
```

Adds a new column to the table. Existing rows will have `NULL` values for the new column.

Example:
```sql
-- Add a new column with no default
ALTER TABLE users ADD COLUMN age INT;

-- Add a column with NOT NULL constraint
-- (will fail if table has existing rows)
ALTER TABLE users ADD COLUMN status TEXT NOT NULL DEFAULT 'active';
```

#### Drop Column

```sql
ALTER TABLE table_name DROP COLUMN column_name;
```

Removes a column from the table. This operation:
- Deletes all data in that column
- Automatically drops any indexes on that column
- Rebuilds remaining indexes
- Migrates all data to a new table structure

Example:
```sql
ALTER TABLE users DROP COLUMN age;
```

#### Rename Column

```sql
ALTER TABLE table_name RENAME COLUMN old_column_name TO new_column_name;
```

Renames a column in table metadata. This operation also updates index and foreign-key metadata that reference the renamed column.

Example:
```sql
ALTER TABLE users RENAME COLUMN name TO full_name;
```

#### Alter Column Type

```sql
ALTER TABLE table_name ALTER COLUMN column_name TYPE new_datatype;
```

Changes the type of an existing column by rewriting table rows and rebuilding indexes on the table.

Example:
```sql
ALTER TABLE users ALTER COLUMN age TYPE TEXT;
```

**Notes:**
- Supported `ALTER TABLE` operations in 0.x: `ADD COLUMN`, `DROP COLUMN`, `RENAME COLUMN`, `ALTER COLUMN TYPE`
- `ALTER TABLE` operations are currently rejected for tables that define `CHECK` constraints
- `ALTER TABLE` operations are currently rejected for tables that define expression indexes
- `RENAME COLUMN` is rejected when dependent views exist
- `ALTER COLUMN TYPE` currently supports only `INT64`, `FLOAT64`, `TEXT`, and `BOOL`
- `ALTER COLUMN TYPE` is rejected for PRIMARY KEY columns, FK child columns, and columns referenced by foreign keys
- `ADD CONSTRAINT` is not supported
- Schema changes require an exclusive lock on the database

### CREATE VIEW / DROP VIEW / ALTER VIEW

```sql
CREATE VIEW view_name AS SELECT ...;
CREATE VIEW IF NOT EXISTS view_name AS SELECT ...;
CREATE OR REPLACE VIEW view_name AS SELECT ...;

DROP VIEW [IF EXISTS] view_name;

ALTER VIEW view_name RENAME TO new_name;
```

Example:
```sql
CREATE VIEW active_users AS
  SELECT id, email FROM users WHERE status = 'active';

ALTER VIEW active_users RENAME TO active_users_v1;
DROP VIEW active_users_v1;
```

**Notes:**
- View definitions must be a pure `SELECT` statement.
- Views are read-only unless you define `INSTEAD OF` triggers for DML.
- Parameters (`$1`, etc.) are not allowed in view definitions.
- Dropping or renaming a view is `RESTRICT`ed when dependent views exist.

### CREATE TRIGGER / DROP TRIGGER

```sql
CREATE TRIGGER trigger_name
AFTER INSERT OR UPDATE OR DELETE ON table_name
FOR EACH ROW
EXECUTE FUNCTION decentdb_exec_sql('single_dml_sql');

CREATE TRIGGER trigger_name
INSTEAD OF INSERT OR UPDATE OR DELETE ON view_name
FOR EACH ROW
EXECUTE FUNCTION decentdb_exec_sql('single_dml_sql');

DROP TRIGGER [IF EXISTS] trigger_name ON object_name;
```

Example:
```sql
CREATE TABLE audit (tag TEXT);
CREATE TRIGGER users_ins_audit
AFTER INSERT ON users
FOR EACH ROW
EXECUTE FUNCTION decentdb_exec_sql('INSERT INTO audit(tag) VALUES (''I'')');
```

**Notes:**
- v0 trigger support is intentionally narrow:
  - timing: `AFTER` (tables) and `INSTEAD OF` (views)
  - events: `INSERT`, `UPDATE`, `DELETE`
  - scope: `FOR EACH ROW` only
- Trigger action SQL must be exactly one DML statement (`INSERT`, `UPDATE`, or `DELETE`) and cannot use parameters.
- Trigger actions do not support `NEW`/`OLD` row references in 0.x.
- View DML without a matching `INSTEAD OF` trigger remains read-only.

## Data Manipulation Language (DML)

### INSERT

```sql
INSERT INTO table_name VALUES (val1, val2, ...);
INSERT INTO table_name (col1, col2) VALUES (val1, val2);
INSERT INTO table_name (...) VALUES (...) ON CONFLICT DO NOTHING;
INSERT INTO table_name (...) VALUES (...) ON CONFLICT (col1, col2) DO NOTHING;
INSERT INTO table_name (...) VALUES (...) ON CONFLICT ON CONSTRAINT constraint_name DO NOTHING;
INSERT INTO table_name (...) VALUES (...) ON CONFLICT (col1, col2) DO UPDATE SET col3 = EXCLUDED.col3;
INSERT INTO table_name (...) VALUES (...) ON CONFLICT ON CONSTRAINT constraint_name DO UPDATE SET col3 = EXCLUDED.col3 WHERE table_name.col4 > 0;
INSERT INTO table_name (...) VALUES (...) RETURNING *;
INSERT INTO table_name (...) VALUES (...) RETURNING col1, col2;
```

Notes:
- `INTEGER PRIMARY KEY` columns support auto-increment. If the column is omitted from the INSERT column list, DecentDB automatically assigns the next sequential ID:
  ```sql
  CREATE TABLE users (id INT PRIMARY KEY, name TEXT);
  INSERT INTO users (name) VALUES ('Alice');  -- id auto-assigned as 1
  INSERT INTO users (name) VALUES ('Bob');    -- id auto-assigned as 2
  INSERT INTO users VALUES (10, 'Carol');     -- explicit id = 10
  INSERT INTO users (name) VALUES ('Dave');   -- id auto-assigned as 11
  ```
- `ON CONFLICT ... DO NOTHING` is supported.
- `ON CONSTRAINT name` resolves against DecentDB unique index names.
- `ON CONFLICT ... DO UPDATE` is supported with explicit conflict target (`(cols)` or `ON CONSTRAINT name`).
- In `DO UPDATE` expressions, unqualified columns resolve to the target table; `EXCLUDED.col` is supported.
- Targetless `ON CONFLICT DO UPDATE` is not supported.
- `INSERT ... RETURNING` is supported.
- `CHECK` constraints are enforced on `INSERT` and `UPDATE` (including `ON CONFLICT ... DO UPDATE`).
- CHECK fails only when the predicate is `FALSE`; `TRUE` and `NULL` pass.
- `UPDATE ... RETURNING` and `DELETE ... RETURNING` are not supported.

### SELECT

```sql
SELECT * FROM table_name;
SELECT col1, col2 FROM table_name WHERE condition;
SELECT DISTINCT col1 FROM table_name;
SELECT DISTINCT ON (category) category, name FROM products ORDER BY category, price;
SELECT * FROM table_name ORDER BY col1 ASC, col2 DESC;
SELECT * FROM table_name LIMIT 10 OFFSET 20;
SELECT * FROM table_name OFFSET 20 ROWS FETCH FIRST 10 ROWS ONLY;  -- SQL:2008 syntax
SELECT id FROM a UNION ALL SELECT id FROM b;
```

### UPDATE

```sql
UPDATE table_name SET col1 = val1 WHERE condition;
```

### DELETE

```sql
DELETE FROM table_name WHERE condition;
```

### ANALYZE

Collects table and index statistics used by the query planner (row counts and index key cardinality).

```sql
ANALYZE;
ANALYZE table_name;
```

Notes:
- `ANALYZE table_name` computes statistics for a single table.
- `ANALYZE` (no table) analyzes all tables.
- `ANALYZE` is a write operation and is currently rejected inside an explicit transaction (`BEGIN`/`COMMIT`).

## Query Features

### WHERE Clause

Supports:
- Comparison operators: `=`, `!=`, `<>`, `<`, `<=`, `>`, `>=`
- Logical operators: `AND`, `OR`, `NOT`
- Arithmetic operators: `+`, `-`, `*`, `/`
- Pattern matching: `LIKE`, `ILIKE` (case-insensitive), with optional `ESCAPE` clause
- Null checks: `IS NULL`, `IS NOT NULL`
- IN operator: `col IN (val1, val2, ...)`
- Range predicates: `BETWEEN`, `NOT BETWEEN`
- Existence predicates: `EXISTS (SELECT ...)`
- String concatenation: `lhs || rhs`

NULL handling follows SQL three-valued logic:
- Comparisons with `NULL` evaluate to `NULL` (unknown), not `TRUE` or `FALSE`
- `NOT NULL` is `NULL`
- In `WHERE`, only `TRUE` keeps a row (`FALSE` and `NULL` are both filtered out)

```sql
SELECT * FROM users WHERE age > 18 AND name LIKE '%son%';
SELECT * FROM users WHERE email IS NOT NULL;
SELECT * FROM users WHERE id IN (1, 2, 3);
SELECT * FROM users WHERE age BETWEEN 18 AND 30;
SELECT * FROM users WHERE name LIKE 'a\_%' ESCAPE '\';
```

### Scalar Functions

Supported scalar functions:

**General:**
- `COALESCE`
- `NULLIF`
- `CAST(expr AS type)` for `INT/INTEGER/INT64`, `FLOAT/FLOAT64/REAL`, `DECIMAL/NUMERIC`, `TEXT`, `BOOL/BOOLEAN`, `UUID`
- `CASE WHEN ... THEN ... ELSE ... END` and `CASE expr WHEN ... THEN ... ELSE ... END`

**String:**
- `LENGTH`
- `LOWER`
- `UPPER`
- `TRIM`
- `LTRIM(str [, chars])` — remove leading characters (default: whitespace)
- `RTRIM(str [, chars])` — remove trailing characters (default: whitespace)
- `REPLACE`
- `SUBSTRING` / `SUBSTR`
- `INSTR(str, substr)` — returns 1-based position of first occurrence (0 if not found)
- `LEFT(str, n)` — first `n` characters
- `RIGHT(str, n)` — last `n` characters
- `LPAD(str, len [, fill])` — pad left to `len` with `fill` (default: space)
- `RPAD(str, len [, fill])` — pad right to `len` with `fill` (default: space)
- `REPEAT(str, n)` — repeat string `n` times
- `REVERSE(str)` — reverse a string
- `CHR(n)` / `CHAR(n)` — returns the character for ASCII code point `n`
- `HEX(val)` — returns uppercase hexadecimal encoding of an integer, text, or blob

**Math:**
- `ABS`
- `ROUND`
- `CEIL` / `CEILING`
- `FLOOR`
- `SIGN(x)` — returns -1, 0, or 1
- `SQRT(x)` — square root (returns FLOAT64; errors on negative input)
- `POWER(x, y)` / `POW(x, y)` — exponentiation (returns FLOAT64)
- `MOD(x, y)` — modulo (also available as `x % y` operator)
- `LN(x)` — natural logarithm
- `LOG(x)` / `LOG10(x)` — base-10 logarithm; `LOG(base, x)` for custom base
- `EXP(x)` — exponential (e^x)
- `RANDOM()` — random float in [0, 1)

**UUID:**
- `GEN_RANDOM_UUID`
- `UUID_PARSE`
- `UUID_TO_STRING`

**JSON:**
- `JSON_ARRAY_LENGTH(json [, path])` — returns element count of a JSON array
- `JSON_EXTRACT(json, path)` — extracts a value using JSONPath (`$`, `$[N]`, `$.key`)
- `JSON_TYPE(json)` — returns the type as a string (`null`, `boolean`, `integer`, `real`, `text`, `array`, `object`)
- `JSON_VALID(json)` — returns 1 if valid JSON, 0 otherwise
- `JSON_OBJECT(key1, val1, ...)` — creates a JSON object from key-value pairs
- `JSON_ARRAY(val1, val2, ...)` — creates a JSON array from arguments

**Date/Time:**
- `NOW()` / `CURRENT_TIMESTAMP` — current date and time as ISO 8601 TEXT
- `CURRENT_DATE` — current date as `YYYY-MM-DD` TEXT
- `CURRENT_TIME` — current time as `HH:MM:SS` TEXT
- `DATE(value)` — parse/normalize a date string
- `DATETIME(value)` — parse/normalize a datetime string
- `STRFTIME(format, value)` — format a datetime using `%Y`, `%m`, `%d`, `%H`, `%M`, `%S`, `%w`
- `EXTRACT(field FROM value)` — extract `YEAR`, `MONTH`, `DAY`, `HOUR`, `MINUTE`, `SECOND` from a datetime string

**Other:**
- `PRINTF(format, args...)` — formatted string output (SQLite-compatible)

```sql
SELECT COALESCE(nickname, name) FROM users;
SELECT NULLIF(status, 'active') FROM users;
SELECT LENGTH(name), LOWER(name), UPPER(name), TRIM(name) FROM users;
SELECT REPLACE(name, 'old', 'new') FROM users;
SELECT SUBSTRING(name, 1, 3) FROM users;
SELECT LEFT(name, 3), RIGHT(name, 3) FROM users;
SELECT LPAD(code, 5, '0'), RPAD(name, 20) FROM items;
SELECT REPEAT('*', 5);  -- Returns '*****'
SELECT REVERSE('hello');  -- Returns 'olleh'
SELECT ABS(balance), ROUND(price, 2), CEIL(rating), FLOOR(rating) FROM products;
SELECT SQRT(area), POWER(base, 2), MOD(total, 10) FROM data;
SELECT LN(x), LOG(x), EXP(x), SIGN(x) FROM data;
SELECT RANDOM();  -- Random float in [0, 1)
SELECT 17 % 5;  -- Modulo operator, returns 2
SELECT INSTR('hello world', 'world');  -- Returns 7
SELECT CHR(65);  -- Returns 'A'
SELECT HEX(255);  -- Returns 'FF'
SELECT GEN_RANDOM_UUID();
SELECT UUID_TO_STRING(id) FROM users;
SELECT CAST('550e8400-e29b-41d4-a716-446655440000' AS UUID);
SELECT TRIM(name) || '_suffix' FROM users;
SELECT CAST(id AS TEXT) FROM users;
SELECT CAST('12.34' AS DECIMAL(10,2));
SELECT CASE WHEN active THEN 'on' ELSE 'off' END FROM users;
SELECT JSON_ARRAY_LENGTH('["a","b","c"]');  -- Returns 3
SELECT JSON_EXTRACT('{"name":"Alice"}', '$.name');  -- Returns 'Alice'
SELECT JSON_TYPE('{"a":1}');  -- Returns 'object'
SELECT JSON_VALID('not json');  -- Returns 0
SELECT JSON_OBJECT('name', 'Alice', 'age', 30);
SELECT JSON_ARRAY(1, 'two', 3.0);
SELECT NOW(), CURRENT_DATE, CURRENT_TIME;
SELECT EXTRACT(YEAR FROM '2026-02-24');  -- Returns 2026
SELECT STRFTIME('%Y-%m-%d', CURRENT_TIMESTAMP);
SELECT PRINTF('Hello %s, you are %d', name, age) FROM users;
```

### Common Table Expressions (CTE)

Supported:
- Non-recursive `WITH ...` on `SELECT`
- `WITH RECURSIVE` for hierarchical queries (tree traversal, series generation). See [ADR-0107](../../design/adr/0107-recursive-cte-execution.md).
- Multiple CTEs in declaration order (`a`, then `b` may reference `a`)
- Optional CTE output column list (`WITH cte(col1, ...) AS (...)`)

```sql
WITH recent AS (
  SELECT id, name FROM users WHERE id > 10
)
SELECT name FROM recent ORDER BY id;

WITH a AS (SELECT id FROM users), b(x) AS (SELECT id FROM a WHERE id > 1)
SELECT x FROM b ORDER BY x;

-- Recursive CTE: generate numbers 1..5
WITH RECURSIVE cnt(x) AS (
  SELECT 1
  UNION ALL
  SELECT x + 1 FROM cnt WHERE x < 5
)
SELECT x FROM cnt;

-- Recursive CTE: tree traversal
WITH RECURSIVE tree(id, name, lvl) AS (
  SELECT id, name, 0 FROM categories WHERE parent_id IS NULL
  UNION ALL
  SELECT c.id, c.name, t.lvl + 1
  FROM categories c JOIN tree t ON c.parent_id = t.id
)
SELECT * FROM tree;
```

Current limits:
- Recursive CTE iteration limit: 1000 rows (prevents infinite loops)

### Set Operations

Supported:
- `UNION ALL`
- `UNION`
- `INTERSECT`
- `EXCEPT`

Not supported:
- `INTERSECT ALL`
- `EXCEPT ALL`

### JOINs

```sql
-- Inner join
SELECT * FROM users JOIN orders ON users.id = orders.user_id;

-- Left join
SELECT * FROM users LEFT JOIN orders ON users.id = orders.user_id;

-- Right join (rewritten internally as LEFT JOIN with swapped operands)
SELECT * FROM orders RIGHT JOIN users ON users.id = orders.user_id;

-- Full outer join
SELECT * FROM users FULL OUTER JOIN orders ON users.id = orders.user_id;

-- Cross join (Cartesian product)
SELECT * FROM colors CROSS JOIN sizes;

-- Natural join (matches on shared column names)
SELECT * FROM employees NATURAL JOIN departments;
```

### Aggregate Functions

```sql
SELECT COUNT(*) FROM users;
SELECT COUNT(email) FROM users;  -- Count non-NULL
SELECT SUM(amount) FROM orders;
SELECT AVG(price) FROM products;
SELECT MIN(created_at), MAX(created_at) FROM users;
SELECT TOTAL(amount) FROM orders;  -- Like SUM but returns 0.0 for empty sets (never NULL)
SELECT category, SUM(amount) FROM orders GROUP BY category;
SELECT category, COUNT(*) FROM orders GROUP BY category HAVING COUNT(*) > 5;
SELECT GROUP_CONCAT(name, ', ') FROM users;  -- Concatenate with separator
SELECT STRING_AGG(name, ', ') FROM users;    -- Alias for GROUP_CONCAT

-- DISTINCT aggregates: de-duplicate values before aggregating
SELECT COUNT(DISTINCT category) FROM products;
SELECT SUM(DISTINCT amount) FROM orders;
SELECT AVG(DISTINCT score) FROM results;
```

### Window Functions

Supported window functions:

- `ROW_NUMBER() OVER (...)` — sequential row numbering within each partition
- `RANK() OVER (...)` — ranking with gaps for ties (e.g., 1, 1, 3)
- `DENSE_RANK() OVER (...)` — ranking without gaps (e.g., 1, 1, 2)
- `LAG(expr [, offset [, default]]) OVER (...)` — access a previous row's value
- `LEAD(expr [, offset [, default]]) OVER (...)` — access a following row's value
- `FIRST_VALUE(expr) OVER (...)` — first value in the partition
- `LAST_VALUE(expr) OVER (...)` — last value in the partition
- `NTH_VALUE(expr, n) OVER (...)` — nth value in the partition (1-based)

All functions support:

- `PARTITION BY` (optional) — divides rows into groups
- `ORDER BY` inside `OVER (...)` (required) — determines row ordering within partitions

```sql
-- ROW_NUMBER: sequential numbering
SELECT id, ROW_NUMBER() OVER (PARTITION BY dept ORDER BY salary DESC) AS rn
FROM employees ORDER BY dept, id;

-- RANK: ties get same rank, with gaps
SELECT name, RANK() OVER (ORDER BY score DESC) AS rank
FROM scores ORDER BY score DESC;

-- DENSE_RANK: ties get same rank, no gaps
SELECT name, DENSE_RANK() OVER (ORDER BY score DESC) AS drank
FROM scores ORDER BY score DESC;

-- LAG: previous row's value (default offset = 1, default value = NULL)
SELECT name, score, LAG(score, 1, 0) OVER (ORDER BY id) AS prev_score
FROM scores ORDER BY id;

-- LEAD: next row's value
SELECT name, LEAD(score) OVER (PARTITION BY dept ORDER BY id) AS next_score
FROM scores ORDER BY dept, id;

-- FIRST_VALUE / LAST_VALUE / NTH_VALUE
SELECT name,
  FIRST_VALUE(score) OVER (PARTITION BY dept ORDER BY id) AS first,
  LAST_VALUE(score) OVER (PARTITION BY dept ORDER BY id) AS last,
  NTH_VALUE(score, 2) OVER (PARTITION BY dept ORDER BY id) AS second
FROM scores ORDER BY dept, id;
```

Current limits:

- Window expressions are supported only in `SELECT` projection items.
- `ORDER BY` in the outer query cannot reference window function aliases directly; use base column ordering instead.
- Frame clauses (`ROWS BETWEEN ...`, `RANGE BETWEEN ...`) are not supported.

### Transactions

```sql
BEGIN;
BEGIN IMMEDIATE;   -- Synonym for BEGIN (single-writer engine)
BEGIN EXCLUSIVE;   -- Synonym for BEGIN (single-writer engine)
COMMIT;
ROLLBACK;

-- Savepoints (within a transaction)
SAVEPOINT name;
RELEASE SAVEPOINT name;
ROLLBACK TO SAVEPOINT name;
```

For details, see [Transactions](transactions.md).

### Explain

```sql
EXPLAIN SELECT * FROM users WHERE id = 1;
```

Produces a text-based query execution plan.

### Explain Analyze

```sql
EXPLAIN ANALYZE SELECT * FROM users WHERE id = 1;
```

Executes the query and produces the execution plan annotated with actual row counts
and execution time. The parenthesized form `EXPLAIN (ANALYZE) ...` is also supported.

### Table-Valued Functions

Table-valued functions appear in the `FROM` clause and return a set of rows. See [ADR-0111](../../design/adr/0111-table-valued-functions.md).

**`json_each(json)`** — iterates top-level key/value pairs of a JSON object or array.

Returns columns: `key` (TEXT), `value` (TEXT), `type` (TEXT).

```sql
-- Iterate a JSON object
SELECT key, value, type FROM json_each('{"name":"Alice","age":30}');
-- Returns: name|Alice|string, age|30|number

-- Iterate a JSON array
SELECT key, value, type FROM json_each('[10, 20, 30]');
-- Returns: 0|10|number, 1|20|number, 2|30|number
```

**`json_tree(json)`** — recursively walks a nested JSON structure.

Returns columns: `key` (TEXT), `value` (TEXT), `type` (TEXT), `path` (TEXT).

```sql
SELECT key, value, type, path FROM json_tree('{"a":{"b":1},"c":[2,3]}');
```

### Generated Columns

Columns defined with `GENERATED ALWAYS AS (expr) STORED` are computed on every INSERT and UPDATE and persisted to disk. See [ADR-0108](../../design/adr/0108-generated-columns-stored.md).

```sql
CREATE TABLE products (
    id INT PRIMARY KEY,
    price REAL,
    qty INT,
    total REAL GENERATED ALWAYS AS (price * qty) STORED
);

INSERT INTO products (id, price, qty) VALUES (1, 9.99, 3);
SELECT total FROM products WHERE id = 1;  -- Returns 29.97
```

## Constraints

### Primary Key

```sql
CREATE TABLE users (
    id INT PRIMARY KEY,
    ...
);
```

### Foreign Key

```sql
CREATE TABLE orders (
    id INT PRIMARY KEY,
    user_id INT REFERENCES users(id) ON DELETE CASCADE,
    ...
);
```

Notes:
- Supported `ON DELETE` actions: `NO ACTION`/`RESTRICT`, `CASCADE`, `SET NULL`.
- Supported `ON UPDATE` actions: `NO ACTION`/`RESTRICT`, `CASCADE`, `SET NULL`.
- `ON DELETE SET NULL` and `ON UPDATE SET NULL` require the child FK column to be nullable.

### Unique

```sql
CREATE TABLE users (
    id INT PRIMARY KEY,
    email TEXT UNIQUE,
    ...
);
```

### NOT NULL

```sql
CREATE TABLE users (
    id INT PRIMARY KEY,
    name TEXT NOT NULL,
    ...
);
```

### CHECK

```sql
CREATE TABLE products (
    id INT PRIMARY KEY,
    name TEXT NOT NULL,
    price REAL CHECK (price > 0),
    CHECK (name IS NULL OR LENGTH(name) > 0)
);
```

CHECK constraints are evaluated on INSERT and UPDATE (including `ON CONFLICT DO UPDATE`). A CHECK expression that evaluates to `TRUE` or `NULL` passes; only `FALSE` is a violation.

### DEFAULT

```sql
CREATE TABLE users (
    id INT PRIMARY KEY,
    name TEXT NOT NULL,
    status TEXT DEFAULT 'active',
    ref UUID DEFAULT GEN_RANDOM_UUID()
);
```

DEFAULT values are used when a column is omitted from an INSERT statement.

## Parameters

Use positional parameters with `$1`, `$2`, etc.:

```sql
SELECT * FROM users WHERE id = $1 AND name = $2;
```

CLI usage:
```bash
decentdb exec --db=my.ddb --sql="SELECT * FROM users WHERE id = \$1" --params=int:42
```

## Unsupported Features

Not currently supported:
- Window frame clauses (`ROWS BETWEEN ...`, `RANGE BETWEEN ...`)
- Additional window functions (`NTILE`, `PERCENT_RANK`, `CUME_DIST`)
- `INTERSECT ALL`, `EXCEPT ALL`
- Stored procedures
- Distributed transactions

See [Known Limitations](../about/changelog.md#known-limitations) for details.
