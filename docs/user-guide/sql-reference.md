# SQL Reference

DecentDb supports a PostgreSQL-like SQL subset.

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
    created_at INT
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
```

### DROP TABLE / DROP INDEX

```sql
DROP TABLE table_name;
DROP INDEX index_name;
```

## Data Manipulation Language (DML)

### INSERT

```sql
INSERT INTO table_name VALUES (val1, val2, ...);
INSERT INTO table_name (col1, col2) VALUES (val1, val2);
```

### SELECT

```sql
SELECT * FROM table_name;
SELECT col1, col2 FROM table_name WHERE condition;
SELECT * FROM table_name ORDER BY col1 ASC, col2 DESC;
SELECT * FROM table_name LIMIT 10 OFFSET 20;
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

```sql
SELECT * FROM users WHERE age > 18 AND name LIKE '%son%';
SELECT * FROM users WHERE email IS NOT NULL;
SELECT * FROM users WHERE id IN (1, 2, 3);
```

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
    user_id INT REFERENCES users(id),
    ...
);
```

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
decentdb exec --db=my.db --sql="SELECT * FROM users WHERE id = \$1" --params=int:42
```

## Unsupported Features

Not currently supported:
- Subqueries in SELECT
- Window functions
- Common Table Expressions (CTE)
- ALTER TABLE
- Views
- Stored procedures

See [Known Limitations](../about/changelog.md#known-limitations) for details.
