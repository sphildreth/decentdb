# SQL Feature Matrix

This document provides a comprehensive matrix of SQL features, comparing support across DecentDB, SQLite, and PostgreSQL.

## DDL (Data Definition Language)

| Feature | DecentDB | SQLite | PostgreSQL |
|---------|----------|--------|------------|
| CREATE TABLE | ✅ | ✅ | ✅ |
| DROP TABLE | ✅ | ✅ | ✅ |
| CREATE INDEX | ✅ | ✅ | ✅ |
| DROP INDEX | ✅ | ✅ | ✅ |
| ALTER TABLE ADD COLUMN | ✅ | ✅ | ✅ |
| ALTER TABLE DROP COLUMN | ✅ | ✅ | ✅ |
| ALTER TABLE RENAME COLUMN | ✅ | ✅ (via ALTER TABLE RENAME) | ✅ |
| ALTER TABLE ALTER COLUMN TYPE | ✅ | ✅ | ✅ |
| CREATE VIEW | ✅ | ✅ | ✅ |
| DROP VIEW | ✅ | ✅ | ✅ |
| CREATE TRIGGER | ✅ | ✅ | ✅ |
| DROP TRIGGER | ✅ | ✅ | ✅ |
| CREATE TEMP TABLE | ❌ | ✅ | ✅ |
| CREATE TEMP VIEW | ❌ | ✅ | ✅ |
| Generated columns (STORED) | ❌ | ✅ | ✅ |
| Table-level FOREIGN KEY | ✅ | ✅ | ✅ |

### Examples

```sql
-- CREATE TABLE (DecentDB, SQLite, PostgreSQL)
CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT NOT NULL);
CREATE TABLE orders (id INTEGER PRIMARY KEY, user_id INTEGER REFERENCES users(id));

-- ALTER TABLE (DecentDB, SQLite, PostgreSQL)
ALTER TABLE users ADD COLUMN email TEXT;
ALTER TABLE users DROP COLUMN email;
ALTER TABLE users ALTER COLUMN name TYPE VARCHAR(255);

-- CREATE VIEW (DecentDB, SQLite, PostgreSQL)
CREATE VIEW user_orders AS SELECT u.name, o.id FROM users u LEFT JOIN orders o ON u.id = o.user_id;
```

## DML (Data Manipulation Language)

| Feature | DecentDB | SQLite | PostgreSQL |
|---------|----------|--------|------------|
| SELECT | ✅ | ✅ | ✅ |
| INSERT | ✅ | ✅ | ✅ |
| UPDATE | ✅ | ✅ | ✅ |
| DELETE | ✅ | ✅ | ✅ |
| INSERT ... RETURNING | ✅ | ❌ | ✅ |
| INSERT ... ON CONFLICT | ✅ (DO NOTHING/DO UPDATE) | ✅ (ON CONFLICT) | ✅ (ON CONFLICT) |
| Bulk INSERT | ✅ | ✅ | ✅ |

### Examples

```sql
-- INSERT with RETURNING (DecentDB, PostgreSQL)
INSERT INTO users (name) VALUES ('Alice') RETURNING id;

-- INSERT ON CONFLICT (DecentDB, SQLite, PostgreSQL)
INSERT INTO users (id, name) VALUES (1, 'Alice')
ON CONFLICT (id) DO UPDATE SET name = EXCLUDED.name;

-- UPSERT pattern (DecentDB)
INSERT INTO users (id, name) VALUES (1, 'Alice')
ON CONFLICT (id) DO NOTHING;
```

## JOINs

| Feature | DecentDB | SQLite | PostgreSQL |
|---------|----------|--------|------------|
| INNER JOIN | ✅ | ✅ | ✅ |
| LEFT JOIN | ✅ | ✅ | ✅ |
| RIGHT JOIN | ❌ (explicit error) | ✅ | ✅ |
| FULL OUTER JOIN | ❌ (explicit error) | ✅ | ✅ |
| CROSS JOIN | ✅ | ✅ | ✅ |
| NATURAL JOIN | ❌ | ✅ | ✅ |

### Examples

```sql
-- INNER JOIN (DecentDB, SQLite, PostgreSQL)
SELECT * FROM users u INNER JOIN orders o ON u.id = o.user_id;

-- LEFT JOIN (DecentDB, SQLite, PostgreSQL)
SELECT * FROM users u LEFT JOIN orders o ON u.id = o.user_id;
```

## Queries and Clauses

| Feature | DecentDB | SQLite | PostgreSQL |
|---------|----------|--------|------------|
| WHERE | ✅ | ✅ | ✅ |
| ORDER BY | ✅ | ✅ | ✅ |
| LIMIT/OFFSET | ✅ | ✅ | ✅ |
| GROUP BY | ✅ | ✅ | ✅ |
| HAVING | ✅ | ✅ | ✅ |
| DISTINCT | ✅ | ✅ | ✅ |
| DISTINCT ON | ❌ | ❌ | ✅ |
| LIMIT ALL | ✅ | ✅ | ✅ |
| OFFSET with FETCH | ❌ | ✅ | ✅ |

### Examples

```sql
-- Basic SELECT (DecentDB, SQLite, PostgreSQL)
SELECT id, name FROM users WHERE id > 10 ORDER BY name LIMIT 5;

-- Aggregate with GROUP BY (DecentDB, SQLite, PostgreSQL)
SELECT user_id, COUNT(*) as order_count FROM orders GROUP BY user_id HAVING COUNT(*) > 5;
```

## Aggregate Functions

| Feature | DecentDB | SQLite | PostgreSQL |
|---------|----------|--------|------------|
| COUNT(*) | ✅ | ✅ | ✅ |
| COUNT(expr) | ✅ | ✅ | ✅ |
| SUM | ✅ | ✅ | ✅ |
| AVG | ✅ | ✅ | ✅ |
| MIN | ✅ | ✅ | ✅ |
| MAX | ✅ | ✅ | ✅ |
| GROUP_CONCAT | ✅ | ✅ | ✅ |
| STRING_AGG | ✅ | ❌ | ✅ |
| TOTAL | ✅ | ✅ | ❌ |
| COUNT(DISTINCT) | ❌ | ✅ | ✅ |
| SUM(DISTINCT) | ❌ | ✅ | ✅ |
| AVG(DISTINCT) | ❌ | ✅ | ✅ |

### Examples

```sql
-- Basic aggregates (DecentDB, SQLite, PostgreSQL)
SELECT COUNT(*), SUM(amount), AVG(amount), MIN(amount), MAX(amount) FROM orders;

-- GROUP_CONCAT (DecentDB, SQLite)
SELECT GROUP_CONCAT(name) FROM users;

-- TOTAL (DecentDB, SQLite - returns 0.0 for empty sets)
SELECT TOTAL(amount) FROM orders;
```

## Window Functions

| Feature | DecentDB | SQLite | PostgreSQL |
|---------|----------|--------|------------|
| ROW_NUMBER() | ✅ | ✅ | ✅ |
| RANK() | ✅ | ✅ | ✅ |
| DENSE_RANK() | ✅ | ✅ | ✅ |
| LAG() | ✅ | ✅ | ✅ |
| LEAD() | ✅ | ✅ | ✅ |
| FIRST_VALUE() | ✅ | ✅ | ✅ |
| LAST_VALUE() | ✅ | ✅ | ✅ |
| NTH_VALUE() | ✅ | ✅ | ✅ |

### Examples

```sql
-- Window function (DecentDB, SQLite, PostgreSQL)
SELECT name, department, salary,
       ROW_NUMBER() OVER (PARTITION BY department ORDER BY salary DESC) as rn,
       LAG(salary) OVER (PARTITION BY department ORDER BY salary) as prev_salary
FROM employees;
```

## Scalar Functions

### Math Functions

| Function | DecentDB | SQLite | PostgreSQL |
|----------|----------|--------|------------|
| ABS() | ✅ | ✅ | ✅ |
| CEIL()/CEILING() | ✅ | ✅ | ✅ |
| FLOOR() | ✅ | ✅ | ✅ |
| ROUND() | ✅ | ✅ | ✅ |
| SQRT() | ✅ | ✅ | ✅ |
| POWER()/POW() | ✅ | ✅ | ✅ |
| MOD() | ✅ | ✅ | ✅ |
| SIGN() | ✅ | ✅ | ✅ |
| LN() | ✅ | ❌ | ✅ |
| LOG() | ✅ | ❌ | ✅ |
| EXP() | ✅ | ✅ | ✅ |
| RANDOM() | ✅ (returns FLOAT64) | ✅ (returns INT64) | ✅ (returns FLOAT64) |

### String Functions

| Function | DecentDB | SQLite | PostgreSQL |
|----------|----------|--------|------------|
| LENGTH() | ✅ | ✅ | ✅ |
| LOWER() | ✅ | ✅ | ✅ |
| UPPER() | ✅ | ✅ | ✅ |
| TRIM() | ✅ | ✅ | ✅ |
| LTRIM() | ✅ | ✅ | ✅ |
| RTRIM() | ✅ | ✅ | ✅ |
| SUBSTR()/SUBSTRING() | ✅ | ✅ | ✅ |
| REPLACE() | ✅ | ✅ | ✅ |
| INSTR() | ✅ | ✅ | ✅ |
| LEFT() | ✅ | ❌ | ✅ |
| RIGHT() | ✅ | ❌ | ✅ |
| LPAD() | ✅ | ❌ | ✅ |
| RPAD() | ✅ | ❌ | ✅ |
| REPEAT() | ✅ | ✅ | ✅ |
| REVERSE() | ✅ | ✅ | ✅ |
| CHR() | ✅ | ❌ (uses CHAR) | ✅ |
| HEX() | ✅ | ✅ | ✅ |

### Date/Time Functions

| Function | DecentDB | SQLite | PostgreSQL |
|----------|----------|--------|------------|
| NOW() | ✅ | ❌ | ✅ |
| CURRENT_TIMESTAMP | ✅ | ✅ | ✅ |
| CURRENT_DATE | ✅ | ✅ | ✅ |
| CURRENT_TIME | ✅ | ✅ | ✅ |
| date() | ✅ | ✅ | ✅ (different) |
| datetime() | ✅ | ✅ | ✅ (different) |
| strftime() | ✅ | ✅ | ❌ |
| EXTRACT() | ✅ | ❌ | ✅ |

### JSON Functions

| Function | DecentDB | SQLite | PostgreSQL |
|----------|----------|--------|------------|
| JSON_EXTRACT() | ✅ | ✅ | ✅ (->) |
| JSON_ARRAY_LENGTH() | ✅ | ✅ | ✅ |
| json_type() | ✅ | ✅ | ✅ |
| json_valid() | ✅ | ✅ | ✅ |
| json_object() | ✅ | ✅ | ✅ |
| json_array() | ❌ (parser limitation) | ✅ | ✅ |
| -> | ✅ | ✅ | ✅ |
| ->> | ✅ | ✅ | ✅ |
| json_each() | ❌ | ✅ | ❌ |
| json_tree() | ❌ | ✅ | ❌ |

### Examples

```sql
-- Math functions (DecentDB, SQLite, PostgreSQL)
SELECT SQRT(16), POWER(2, 10), MOD(10, 3), SIGN(-5);

-- String functions (DecentDB, SQLite, PostgreSQL)
SELECT LENGTH('hello'), UPPER('hello'), SUBSTR('hello', 1, 3), REPLACE('hello', 'l', 'x');

-- Date/Time (DecentDB, SQLite)
SELECT NOW(), CURRENT_DATE, date('now'), strftime('%Y-%m-%d', 'now');
SELECT EXTRACT(YEAR FROM CURRENT_TIMESTAMP);

-- JSON (DecentDB, SQLite, PostgreSQL)
SELECT '{"a":1}'->>'a', json_extract('{"a":1}', '$.a'), json_valid('{"a":1}');
```

## Operators

| Operator | DecentDB | SQLite | PostgreSQL |
|----------|----------|--------|------------|
| + - * / | ✅ | ✅ | ✅ |
| % (modulo) | ✅ | ✅ | ✅ |
| \|\| (concat) | ✅ | ✅ | ✅ |
| LIKE/ILIKE | ✅ | ✅ | ✅ |
| BETWEEN | ✅ | ✅ | ✅ |
| IN | ✅ | ✅ | ✅ |
| IS NULL | ✅ | ✅ | ✅ |
| CASE | ✅ | ✅ | ✅ |
| COALESCE | ✅ | ✅ | ✅ |
| NULLIF | ✅ | ✅ | ✅ |

## Transaction Control

| Feature | DecentDB | SQLite | PostgreSQL |
|---------|----------|--------|------------|
| BEGIN | ✅ | ✅ | ✅ |
| BEGIN IMMEDIATE | ✅ (treated as BEGIN) | ✅ | ❌ |
| COMMIT | ✅ | ✅ | ✅ |
| ROLLBACK | ✅ | ✅ | ✅ |
| SAVEPOINT | ❌ | ✅ | ✅ |
| RELEASE SAVEPOINT | ❌ | ✅ | ✅ |
| ROLLBACK TO SAVEPOINT | ❌ | ✅ | ✅ |

## Data Types

| Type | DecentDB | SQLite | PostgreSQL |
|------|----------|--------|------------|
| NULL | ✅ | ✅ | ✅ |
| INTEGER/INT | ✅ | ✅ | ✅ |
| BIGINT | ✅ | ✅ | ✅ |
| FLOAT/REAL | ✅ | ✅ | ✅ |
| DOUBLE PRECISION | ✅ | ✅ | ✅ |
| TEXT | ✅ | ✅ | ✅ |
| BLOB | ✅ | ✅ | ✅ |
| BOOLEAN | ✅ | ✅ | ✅ |
| UUID | ✅ | ❌ | ✅ |
| DECIMAL/NUMERIC | ✅ | ✅ | ✅ |
| DATE | ❌ (TEXT) | ✅ | ✅ |
| TIMESTAMP | ❌ (TEXT) | ✅ | ✅ |

## Constraints

| Constraint | DecentDB | SQLite | PostgreSQL |
|------------|----------|--------|------------|
| PRIMARY KEY | ✅ | ✅ | ✅ |
| FOREIGN KEY | ✅ | ✅ | ✅ |
| NOT NULL | ✅ | ✅ | ✅ |
| UNIQUE | ✅ | ✅ | ✅ |
| CHECK | ✅ | ✅ | ✅ |
| DEFAULT | ✅ | ✅ | ✅ |

## Set Operations

| Operation | DecentDB | SQLite | PostgreSQL |
|-----------|----------|--------|------------|
| UNION | ✅ | ✅ | ✅ |
| UNION ALL | ✅ | ✅ | ✅ |
| INTERSECT | ✅ | ✅ | ✅ |
| INTERSECT ALL | ✅ | ✅ | ✅ |
| EXCEPT | ✅ | ✅ | ✅ |
| EXCEPT ALL | ✅ | ✅ | ✅ |

## CTEs (Common Table Expressions)

| Feature | DecentDB | SQLite | PostgreSQL |
|---------|----------|--------|------------|
| WITH ... AS | ✅ | ✅ | ✅ |
| WITH RECURSIVE | ❌ | ✅ | ✅ |
| Multiple CTEs | ✅ | ✅ | ✅ |
