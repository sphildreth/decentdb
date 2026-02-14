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
SELECT * FROM table_name ORDER BY col1 ASC, col2 DESC;
SELECT * FROM table_name LIMIT 10 OFFSET 20;
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
- `REPLACE`
- `SUBSTRING` / `SUBSTR`

**Math:**
- `ABS`
- `ROUND`
- `CEIL` / `CEILING`
- `FLOOR`

**UUID:**
- `GEN_RANDOM_UUID`
- `UUID_PARSE`
- `UUID_TO_STRING`

```sql
SELECT COALESCE(nickname, name) FROM users;
SELECT NULLIF(status, 'active') FROM users;
SELECT LENGTH(name), LOWER(name), UPPER(name), TRIM(name) FROM users;
SELECT REPLACE(name, 'old', 'new') FROM users;
SELECT SUBSTRING(name, 1, 3) FROM users;
SELECT ABS(balance), ROUND(price, 2), CEIL(rating), FLOOR(rating) FROM products;
SELECT GEN_RANDOM_UUID();
SELECT UUID_TO_STRING(id) FROM users;
SELECT CAST('550e8400-e29b-41d4-a716-446655440000' AS UUID);
SELECT TRIM(name) || '_suffix' FROM users;
SELECT CAST(id AS TEXT) FROM users;
SELECT CAST('12.34' AS DECIMAL(10,2));
SELECT CASE WHEN active THEN 'on' ELSE 'off' END FROM users;
```

### Common Table Expressions (CTE)

Supported CTE subset:
- Non-recursive `WITH ...` on `SELECT`
- Multiple CTEs in declaration order (`a`, then `b` may reference `a`)
- Optional CTE output column list (`WITH cte(col1, ...) AS (...)`)

Current limits:
- `WITH RECURSIVE` is not supported
- CTE bodies cannot contain `GROUP BY`/`HAVING`, `ORDER BY`, or `LIMIT/OFFSET` in 0.x

```sql
WITH recent AS (
  SELECT id, name FROM users WHERE id > 10
)
SELECT name FROM recent ORDER BY id;

WITH a AS (SELECT id FROM users), b(x) AS (SELECT id FROM a WHERE id > 1)
SELECT x FROM b ORDER BY x;
```

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
```

### Aggregate Functions

```sql
SELECT COUNT(*) FROM users;
SELECT COUNT(email) FROM users;  -- Count non-NULL
SELECT SUM(amount) FROM orders;
SELECT AVG(price) FROM products;
SELECT MIN(created_at), MAX(created_at) FROM users;
SELECT category, SUM(amount) FROM orders GROUP BY category;
SELECT category, COUNT(*) FROM orders GROUP BY category HAVING COUNT(*) > 5;
```

### Window Functions

Supported window subset:
- `ROW_NUMBER() OVER (...)`
- `PARTITION BY` is optional
- `ORDER BY` inside `OVER (...)` is required in 0.x

```sql
SELECT id, ROW_NUMBER() OVER (PARTITION BY grp ORDER BY id) AS rn
FROM t
ORDER BY id;
```

Current limits:
- Only `ROW_NUMBER()` is supported.
- Window expressions are supported only in `SELECT` projection items.

### Transactions

```sql
BEGIN;
-- ... your operations ...
COMMIT;

-- Or rollback
BEGIN;
-- ... your operations ...
ROLLBACK;
```

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
- Advanced window functions beyond `ROW_NUMBER()` (for example `RANK`, `DENSE_RANK`, `LAG`, frame clauses)
- Recursive CTEs (`WITH RECURSIVE`)
- Stored procedures

See [Known Limitations](../about/changelog.md#known-limitations) for details.
