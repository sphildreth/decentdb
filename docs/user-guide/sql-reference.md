# SQL Reference

DecentDb supports a PostgreSQL-like SQL subset.

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
```

Notes:
- Partial indexes are currently limited to single-column BTREE indexes with predicate form `column IS NOT NULL`.
- `UNIQUE` partial indexes, trigram partial indexes, multi-column partial indexes, and arbitrary partial predicates are not supported in 0.x.

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

**Notes:**
- `ADD COLUMN` and `DROP COLUMN` are the only supported operations in v1.0.0
- `ALTER TABLE` operations are currently rejected for tables that define `CHECK` constraints
- Advanced operations like `RENAME COLUMN`, `MODIFY COLUMN` (type changes), and `ADD CONSTRAINT` are planned for future releases
- Schema changes require an exclusive lock on the database

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
- `ON CONFLICT ... DO NOTHING` is supported.
- `ON CONSTRAINT name` resolves against DecentDb unique index names.
- `ON CONFLICT ... DO UPDATE` is supported with explicit conflict target (`(cols)` or `ON CONSTRAINT name`).
- In `DO UPDATE` expressions, unqualified columns resolve to the target table; `EXCLUDED.col` is supported.
- Targetless `ON CONFLICT DO UPDATE` is not yet supported.
- `INSERT ... RETURNING` is supported.
- `CHECK` constraints are enforced on `INSERT` and `UPDATE` (including `ON CONFLICT ... DO UPDATE`).
- CHECK fails only when the predicate is `FALSE`; `TRUE` and `NULL` pass.
- `UPDATE ... RETURNING` and `DELETE ... RETURNING` are not yet supported.

### SELECT

```sql
SELECT * FROM table_name;
SELECT col1, col2 FROM table_name WHERE condition;
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
- Comparison operators: `=`, `!=`, `<`, `<=`, `>`, `>=`
- Logical operators: `AND`, `OR`, `NOT`
- Pattern matching: `LIKE`, `ILIKE`
- Null checks: `IS NULL`, `IS NOT NULL`
- IN operator: `col IN (val1, val2, ...)`
- Range predicates: `BETWEEN`, `NOT BETWEEN`
- Existence predicates: `EXISTS (SELECT ...)` (non-correlated)
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
- `COALESCE`
- `NULLIF`
- `LENGTH`
- `LOWER`
- `UPPER`
- `TRIM`
- `CAST(expr AS type)` for `INT/INTEGER/INT64`, `FLOAT/FLOAT64/REAL`, `TEXT`, `BOOL/BOOLEAN`
- `CASE WHEN ... THEN ... ELSE ... END` and `CASE expr WHEN ... THEN ... ELSE ... END`

```sql
SELECT COALESCE(nickname, name) FROM users;
SELECT NULLIF(status, 'active') FROM users;
SELECT LENGTH(name), LOWER(name), UPPER(name), TRIM(name) FROM users;
SELECT TRIM(name) || '_suffix' FROM users;
SELECT CAST(id AS TEXT) FROM users;
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

Not yet supported:
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
- Correlated subqueries in SELECT
- Window functions
- Recursive CTEs (`WITH RECURSIVE`)
- Stored procedures

See [Known Limitations](../about/changelog.md#known-limitations) for details.
