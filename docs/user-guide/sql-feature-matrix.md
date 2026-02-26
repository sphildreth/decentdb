# SQL Feature Matrix

This document provides a comprehensive matrix of SQL features, comparing support across DecentDB, SQLite, PostgreSQL, and DuckDB.

> **See also:** [Comparison: DecentDB vs SQLite vs DuckDB](comparison.md) for a narrative discussion of design differences, trade-offs, and architecture.

## DDL (Data Definition Language)

| Feature | DecentDB | SQLite | PostgreSQL | DuckDB |
|---------|----------|--------|------------|--------|
| CREATE TABLE | ✅ | ✅ | ✅ | ✅ |
| DROP TABLE | ✅ | ✅ | ✅ | ✅ |
| CREATE INDEX | ✅ | ✅ | ✅ | ✅ |
| DROP INDEX | ✅ | ✅ | ✅ | ✅ |
| ALTER TABLE ADD COLUMN | ✅ | ✅ | ✅ | ✅ |
| ALTER TABLE DROP COLUMN | ✅ | ✅ | ✅ | ✅ |
| ALTER TABLE RENAME COLUMN | ✅ | ✅ (via ALTER TABLE RENAME) | ✅ | ✅ |
| ALTER TABLE ALTER COLUMN TYPE | ✅ | ✅ | ✅ | ✅ |
| CREATE VIEW | ✅ | ✅ | ✅ | ✅ |
| DROP VIEW | ✅ | ✅ | ✅ | ✅ |
| CREATE TRIGGER | ✅ | ✅ | ✅ | ❌ |
| DROP TRIGGER | ✅ | ✅ | ✅ | ❌ |
| CREATE TEMP TABLE | ✅ | ✅ | ✅ | ✅ |
| CREATE TEMP VIEW | ✅ | ✅ | ✅ | ✅ |
| Generated columns (STORED) | ✅ | ✅ | ✅ | ✅ |
| Table-level FOREIGN KEY | ✅ | ✅ | ✅ | ⚠️ (parsed, not enforced) |

### Examples

```sql
-- CREATE TABLE with constraints
CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT NOT NULL, email TEXT UNIQUE);
CREATE TABLE orders (
  id INTEGER PRIMARY KEY,
  user_id INTEGER REFERENCES users(id) ON DELETE CASCADE,
  amount DECIMAL(10,2) NOT NULL
);

-- Table-level FOREIGN KEY
CREATE TABLE order_items (
  order_id INTEGER,
  product_id INTEGER,
  qty INTEGER NOT NULL,
  FOREIGN KEY (order_id) REFERENCES orders(id),
  FOREIGN KEY (product_id) REFERENCES products(id)
);

-- CREATE INDEX
CREATE INDEX idx_users_name ON users (name);
CREATE INDEX idx_orders_user ON orders (user_id) WHERE user_id IS NOT NULL;

-- ALTER TABLE
ALTER TABLE users ADD COLUMN email TEXT;
ALTER TABLE users DROP COLUMN email;
ALTER TABLE users RENAME COLUMN name TO full_name;
ALTER TABLE users ALTER COLUMN name TYPE VARCHAR(255);

-- CREATE VIEW
CREATE VIEW user_orders AS
  SELECT u.name, o.id FROM users u LEFT JOIN orders o ON u.id = o.user_id;
CREATE OR REPLACE VIEW active_users AS
  SELECT * FROM users WHERE active = TRUE;

-- TEMP objects (session-scoped, not persisted)
CREATE TEMP TABLE scratch (id INTEGER PRIMARY KEY, val TEXT);
CREATE TEMP VIEW recent_orders AS SELECT * FROM orders WHERE id > 100;

-- Generated columns
CREATE TABLE products (
  id INTEGER PRIMARY KEY,
  price DECIMAL(10,2),
  tax_rate DECIMAL(4,2),
  total DECIMAL(10,2) GENERATED ALWAYS AS (price * (1 + tax_rate)) STORED
);

-- CREATE TRIGGER (AFTER row trigger)
CREATE TRIGGER log_insert AFTER INSERT ON users
FOR EACH ROW BEGIN
  SELECT decentdb_exec_sql('INSERT INTO audit_log (msg) VALUES (''user added'')');
END;
```

## DML (Data Manipulation Language)

| Feature | DecentDB | SQLite | PostgreSQL | DuckDB |
|---------|----------|--------|------------|--------|
| SELECT | ✅ | ✅ | ✅ | ✅ |
| INSERT | ✅ | ✅ | ✅ | ✅ |
| UPDATE | ✅ | ✅ | ✅ | ✅ |
| DELETE | ✅ | ✅ | ✅ | ✅ |
| INSERT ... RETURNING | ✅ | ❌ | ✅ | ✅ |
| INSERT ... ON CONFLICT | ✅ (DO NOTHING/DO UPDATE) | ✅ (ON CONFLICT) | ✅ (ON CONFLICT) | ✅ (ON CONFLICT) |
| Bulk INSERT | ✅ | ✅ | ✅ | ✅ |

### Examples

```sql
-- INSERT with RETURNING
INSERT INTO users (name) VALUES ('Alice') RETURNING id;

-- INSERT ON CONFLICT (upsert)
INSERT INTO users (id, name) VALUES (1, 'Alice')
ON CONFLICT (id) DO UPDATE SET name = EXCLUDED.name;

-- ON CONFLICT DO NOTHING
INSERT INTO users (id, name) VALUES (1, 'Alice')
ON CONFLICT (id) DO NOTHING;

-- Bulk INSERT (multiple rows)
INSERT INTO users (name) VALUES ('Alice'), ('Bob'), ('Charlie');

-- UPDATE with WHERE
UPDATE orders SET amount = amount * 1.1 WHERE user_id = 1;

-- DELETE with WHERE
DELETE FROM orders WHERE amount < 1.00;
```

## JOINs

| Feature | DecentDB | SQLite | PostgreSQL | DuckDB |
|---------|----------|--------|------------|--------|
| INNER JOIN | ✅ | ✅ | ✅ | ✅ |
| LEFT JOIN | ✅ | ✅ | ✅ | ✅ |
| RIGHT JOIN | ✅ (rewritten as LEFT JOIN) | ✅ | ✅ | ✅ |
| FULL OUTER JOIN | ✅ | ✅ | ✅ | ✅ |
| CROSS JOIN | ✅ | ✅ | ✅ | ✅ |
| NATURAL JOIN | ✅ | ✅ | ✅ | ✅ |

### Examples

```sql
-- INNER JOIN
SELECT u.name, o.amount
FROM users u INNER JOIN orders o ON u.id = o.user_id;

-- LEFT JOIN (all users, even those without orders)
SELECT u.name, o.amount
FROM users u LEFT JOIN orders o ON u.id = o.user_id;

-- RIGHT JOIN (all orders, even those without matching users)
SELECT u.name, o.amount
FROM users u RIGHT JOIN orders o ON u.id = o.user_id;

-- FULL OUTER JOIN (all rows from both sides)
SELECT u.name, o.amount
FROM users u FULL OUTER JOIN orders o ON u.id = o.user_id;

-- CROSS JOIN (cartesian product)
SELECT u.name, p.name AS product
FROM users u CROSS JOIN products p;

-- NATURAL JOIN (join on columns with matching names)
SELECT * FROM orders NATURAL JOIN order_details;
```

## Queries and Clauses

| Feature | DecentDB | SQLite | PostgreSQL | DuckDB |
|---------|----------|--------|------------|--------|
| WHERE | ✅ | ✅ | ✅ | ✅ |
| ORDER BY | ✅ | ✅ | ✅ | ✅ |
| LIMIT/OFFSET | ✅ | ✅ | ✅ | ✅ |
| GROUP BY | ✅ | ✅ | ✅ | ✅ |
| HAVING | ✅ | ✅ | ✅ | ✅ |
| DISTINCT | ✅ | ✅ | ✅ | ✅ |
| DISTINCT ON | ✅ | ❌ | ✅ | ✅ |
| LIMIT ALL | ✅ | ✅ | ✅ | ✅ |
| OFFSET with FETCH | ✅ | ✅ | ✅ | ✅ |

### Examples

```sql
-- Basic filtering and sorting
SELECT id, name FROM users WHERE id > 10 ORDER BY name LIMIT 5;

-- GROUP BY with HAVING
SELECT user_id, COUNT(*) AS order_count
FROM orders GROUP BY user_id HAVING COUNT(*) > 5;

-- DISTINCT ON (first order per user, by date)
SELECT DISTINCT ON (user_id) user_id, id, created_at
FROM orders ORDER BY user_id, created_at DESC;

-- OFFSET with FETCH (SQL-standard pagination)
SELECT * FROM users ORDER BY id OFFSET 20 ROWS FETCH NEXT 10 ROWS ONLY;

-- LIMIT / OFFSET (traditional form)
SELECT * FROM users ORDER BY id LIMIT 10 OFFSET 20;
```

## Aggregate Functions

| Feature | DecentDB | SQLite | PostgreSQL | DuckDB |
|---------|----------|--------|------------|--------|
| COUNT(*) | ✅ | ✅ | ✅ | ✅ |
| COUNT(expr) | ✅ | ✅ | ✅ | ✅ |
| SUM | ✅ | ✅ | ✅ | ✅ |
| AVG | ✅ | ✅ | ✅ | ✅ |
| MIN | ✅ | ✅ | ✅ | ✅ |
| MAX | ✅ | ✅ | ✅ | ✅ |
| GROUP_CONCAT | ✅ | ✅ | ✅ | ❌ (use STRING_AGG) |
| STRING_AGG | ✅ | ❌ | ✅ | ✅ |
| TOTAL | ✅ | ✅ | ❌ | ❌ |
| COUNT(DISTINCT) | ✅ | ✅ | ✅ | ✅ |
| SUM(DISTINCT) | ✅ | ✅ | ✅ | ✅ |
| AVG(DISTINCT) | ✅ | ✅ | ✅ | ✅ |

### Examples

```sql
-- Basic aggregates
SELECT COUNT(*), SUM(amount), AVG(amount), MIN(amount), MAX(amount) FROM orders;

-- DISTINCT aggregates (unique values only)
SELECT COUNT(DISTINCT user_id) AS unique_customers,
       SUM(DISTINCT amount) AS distinct_totals,
       AVG(DISTINCT amount) AS avg_distinct
FROM orders;

-- GROUP_CONCAT (concatenate values into a string)
SELECT department, GROUP_CONCAT(name, ', ') FROM employees GROUP BY department;

-- STRING_AGG (Postgres-style equivalent)
SELECT department, STRING_AGG(name, ', ') FROM employees GROUP BY department;

-- TOTAL (returns 0.0 for empty sets, unlike SUM which returns NULL)
SELECT TOTAL(amount) FROM orders WHERE 1 = 0;  -- returns 0.0
SELECT SUM(amount) FROM orders WHERE 1 = 0;    -- returns NULL
```

## Window Functions

| Feature | DecentDB | SQLite | PostgreSQL | DuckDB |
|---------|----------|--------|------------|--------|
| ROW_NUMBER() | ✅ | ✅ | ✅ | ✅ |
| RANK() | ✅ | ✅ | ✅ | ✅ |
| DENSE_RANK() | ✅ | ✅ | ✅ | ✅ |
| LAG() | ✅ | ✅ | ✅ | ✅ |
| LEAD() | ✅ | ✅ | ✅ | ✅ |
| FIRST_VALUE() | ✅ | ✅ | ✅ | ✅ |
| LAST_VALUE() | ✅ | ✅ | ✅ | ✅ |
| NTH_VALUE() | ✅ | ✅ | ✅ | ✅ |

### Examples

```sql
-- ROW_NUMBER, RANK, DENSE_RANK
SELECT name, department, salary,
       ROW_NUMBER() OVER (PARTITION BY department ORDER BY salary DESC) AS rn,
       RANK()       OVER (PARTITION BY department ORDER BY salary DESC) AS rnk,
       DENSE_RANK() OVER (PARTITION BY department ORDER BY salary DESC) AS dense_rnk
FROM employees;

-- LAG / LEAD (access previous/next row values)
SELECT name, salary,
       LAG(salary, 1)  OVER (ORDER BY salary) AS prev_salary,
       LEAD(salary, 1) OVER (ORDER BY salary) AS next_salary
FROM employees;

-- FIRST_VALUE / LAST_VALUE / NTH_VALUE
SELECT name, department, salary,
       FIRST_VALUE(name) OVER (PARTITION BY department ORDER BY salary DESC) AS top_earner,
       LAST_VALUE(name)  OVER (PARTITION BY department ORDER BY salary DESC) AS low_earner,
       NTH_VALUE(name, 2) OVER (PARTITION BY department ORDER BY salary DESC) AS second_earner
FROM employees;
```

## Scalar Functions

### Math Functions

| Function | DecentDB | SQLite | PostgreSQL | DuckDB |
|----------|----------|--------|------------|--------|
| ABS() | ✅ | ✅ | ✅ | ✅ |
| CEIL()/CEILING() | ✅ | ✅ | ✅ | ✅ |
| FLOOR() | ✅ | ✅ | ✅ | ✅ |
| ROUND() | ✅ | ✅ | ✅ | ✅ |
| SQRT() | ✅ | ✅ | ✅ | ✅ |
| POWER()/POW() | ✅ | ✅ | ✅ | ✅ |
| MOD() | ✅ | ✅ | ✅ | ✅ |
| SIGN() | ✅ | ✅ | ✅ | ✅ |
| LN() | ✅ | ❌ | ✅ | ✅ |
| LOG() | ✅ | ❌ | ✅ | ✅ |
| EXP() | ✅ | ✅ | ✅ | ✅ |
| RANDOM() | ✅ (returns FLOAT64) | ✅ (returns INT64) | ✅ (returns FLOAT64) | ✅ (returns DOUBLE) |

### String Functions

| Function | DecentDB | SQLite | PostgreSQL | DuckDB |
|----------|----------|--------|------------|--------|
| LENGTH() | ✅ | ✅ | ✅ | ✅ |
| LOWER() | ✅ | ✅ | ✅ | ✅ |
| UPPER() | ✅ | ✅ | ✅ | ✅ |
| TRIM() | ✅ | ✅ | ✅ | ✅ |
| LTRIM() | ✅ | ✅ | ✅ | ✅ |
| RTRIM() | ✅ | ✅ | ✅ | ✅ |
| SUBSTR()/SUBSTRING() | ✅ | ✅ | ✅ | ✅ |
| REPLACE() | ✅ | ✅ | ✅ | ✅ |
| INSTR() | ✅ | ✅ | ✅ | ✅ (via strpos) |
| LEFT() | ✅ | ❌ | ✅ | ✅ |
| RIGHT() | ✅ | ❌ | ✅ | ✅ |
| LPAD() | ✅ | ❌ | ✅ | ✅ |
| RPAD() | ✅ | ❌ | ✅ | ✅ |
| REPEAT() | ✅ | ✅ | ✅ | ✅ |
| REVERSE() | ✅ | ✅ | ✅ | ✅ |
| CHR() | ✅ | ❌ (uses CHAR) | ✅ | ✅ |
| HEX() | ✅ | ✅ | ✅ | ✅ |

### Date/Time Functions

| Function | DecentDB | SQLite | PostgreSQL | DuckDB |
|----------|----------|--------|------------|--------|
| NOW() | ✅ | ❌ | ✅ | ✅ |
| CURRENT_TIMESTAMP | ✅ | ✅ | ✅ | ✅ |
| CURRENT_DATE | ✅ | ✅ | ✅ | ✅ |
| CURRENT_TIME | ✅ | ✅ | ✅ | ✅ |
| date() | ✅ | ✅ | ✅ (different) | ❌ (use CAST) |
| datetime() | ✅ | ✅ | ✅ (different) | ❌ (use CAST) |
| strftime() | ✅ | ✅ | ❌ | ✅ |
| EXTRACT() | ✅ | ❌ | ✅ | ✅ |

### JSON Functions

| Function | DecentDB | SQLite | PostgreSQL | DuckDB |
|----------|----------|--------|------------|--------|
| JSON_EXTRACT() | ✅ | ✅ | ✅ (->) | ✅ |
| JSON_ARRAY_LENGTH() | ✅ | ✅ | ✅ | ✅ |
| json_type() | ✅ | ✅ | ✅ | ✅ |
| json_valid() | ✅ | ✅ | ✅ | ✅ |
| json_object() | ✅ | ✅ | ✅ | ✅ |
| json_array() | ✅ | ✅ | ✅ | ✅ |
| -> | ✅ | ✅ | ✅ | ✅ |
| ->> | ✅ | ✅ | ✅ | ✅ |
| json_each() | ✅ | ✅ | ❌ | ❌ (use unnest) |
| json_tree() | ✅ | ✅ | ❌ | ❌ |

### Math Examples

```sql
SELECT ABS(-42), CEIL(3.2), FLOOR(3.8), ROUND(3.14159, 2);
SELECT SQRT(144), POWER(2, 10), MOD(17, 5), SIGN(-99);
SELECT LN(2.71828), LOG(1000), EXP(1);
SELECT RANDOM();  -- returns a random FLOAT64 in [0.0, 1.0)
```

### String Examples

```sql
SELECT LENGTH('hello'), LOWER('HELLO'), UPPER('hello');
SELECT TRIM('  hello  '), LTRIM('  hello'), RTRIM('hello  ');
SELECT SUBSTR('hello world', 1, 5), REPLACE('hello', 'l', 'r');
SELECT INSTR('hello world', 'world');  -- returns 7
SELECT LEFT('hello', 3), RIGHT('hello', 3);  -- 'hel', 'llo'
SELECT LPAD('42', 5, '0'), RPAD('hi', 5, '!');  -- '00042', 'hi!!!'
SELECT REPEAT('ab', 3), REVERSE('hello');  -- 'ababab', 'olleh'
SELECT CHR(65), HEX('ABC');  -- 'A', '414243'
```

### Date/Time Examples

```sql
-- Current date/time values
SELECT NOW(), CURRENT_TIMESTAMP, CURRENT_DATE, CURRENT_TIME;

-- date() and datetime() (SQLite-compatible)
SELECT date('now'), date('2024-03-15', '+1 month');
SELECT datetime('now'), datetime('2024-03-15 10:30:00', '+2 hours');

-- strftime() formatting
SELECT strftime('%Y-%m-%d', 'now');
SELECT strftime('%H:%M:%S', '2024-03-15 14:30:00');
SELECT strftime('%Y', '2024-03-15');  -- '2024'

-- EXTRACT() (Postgres-compatible)
SELECT EXTRACT(YEAR FROM CURRENT_TIMESTAMP);
SELECT EXTRACT(MONTH FROM '2024-03-15');
SELECT EXTRACT(DOW FROM '2024-03-15');  -- day of week
```

### JSON Examples

```sql
-- JSON extraction
SELECT json_extract('{"name":"Alice","age":30}', '$.name');  -- 'Alice'
SELECT '{"name":"Alice"}'->>'name';  -- 'Alice' (text)
SELECT '{"name":"Alice"}'->'name';   -- '"Alice"' (JSON)

-- JSON construction
SELECT json_object('name', 'Alice', 'age', 30);  -- '{"name":"Alice","age":30}'
SELECT json_array(1, 2, 'three');  -- '[1,2,"three"]'

-- JSON inspection
SELECT json_type('{"a":1}');  -- 'object'
SELECT json_valid('{"a":1}');  -- 1 (true)
SELECT json_valid('not json');  -- 0 (false)
SELECT json_array_length('[1,2,3]');  -- 3

-- Table-valued: json_each (iterate array/object)
SELECT key, value FROM json_each('[10, 20, 30]');
-- Returns: (0, 10), (1, 20), (2, 30)

SELECT key, value FROM json_each('{"a":1,"b":2}');
-- Returns: ('a', 1), ('b', 2)

-- Table-valued: json_tree (recursive traversal)
SELECT key, value, type FROM json_tree('{"a":{"b":1},"c":[2,3]}');
```

## Operators

| Operator | DecentDB | SQLite | PostgreSQL | DuckDB |
|----------|----------|--------|------------|--------|
| + - * / | ✅ | ✅ | ✅ | ✅ |
| % (modulo) | ✅ | ✅ | ✅ | ✅ |
| \|\| (concat) | ✅ | ✅ | ✅ | ✅ |
| LIKE/ILIKE | ✅ | ✅ | ✅ | ✅ |
| BETWEEN | ✅ | ✅ | ✅ | ✅ |
| IN | ✅ | ✅ | ✅ | ✅ |
| IS NULL | ✅ | ✅ | ✅ | ✅ |
| CASE | ✅ | ✅ | ✅ | ✅ |
| COALESCE | ✅ | ✅ | ✅ | ✅ |
| NULLIF | ✅ | ✅ | ✅ | ✅ |

### Examples

```sql
-- Arithmetic and modulo
SELECT 10 + 3, 10 - 3, 10 * 3, 10 / 3, 10 % 3;

-- String concatenation
SELECT 'Hello' || ' ' || 'World';

-- Pattern matching
SELECT * FROM users WHERE name LIKE 'A%';
SELECT * FROM users WHERE name ILIKE '%alice%';  -- case-insensitive

-- Range and membership
SELECT * FROM orders WHERE amount BETWEEN 10.00 AND 100.00;
SELECT * FROM users WHERE id IN (1, 2, 3);

-- NULL handling
SELECT * FROM users WHERE email IS NULL;
SELECT COALESCE(email, 'no-email@example.com') FROM users;
SELECT NULLIF(score, 0) FROM results;  -- returns NULL if score = 0

-- CASE expressions
SELECT name,
  CASE WHEN salary > 100000 THEN 'high'
       WHEN salary > 50000  THEN 'mid'
       ELSE 'low' END AS band
FROM employees;
```

## Transaction Control

| Feature | DecentDB | SQLite | PostgreSQL | DuckDB |
|---------|----------|--------|------------|--------|
| BEGIN | ✅ | ✅ | ✅ | ✅ |
| BEGIN IMMEDIATE | ✅ (treated as BEGIN) | ✅ | ❌ | ❌ |
| COMMIT | ✅ | ✅ | ✅ | ✅ |
| ROLLBACK | ✅ | ✅ | ✅ | ✅ |
| SAVEPOINT | ✅ | ✅ | ✅ | ❌ |
| RELEASE SAVEPOINT | ✅ | ✅ | ✅ | ❌ |
| ROLLBACK TO SAVEPOINT | ✅ | ✅ | ✅ | ❌ |

### Examples

```sql
-- Basic transaction
BEGIN;
INSERT INTO users (name) VALUES ('Alice');
INSERT INTO users (name) VALUES ('Bob');
COMMIT;

-- Rollback on error
BEGIN;
INSERT INTO users (name) VALUES ('Charlie');
ROLLBACK;  -- nothing committed

-- Savepoints (partial rollback within a transaction)
BEGIN;
INSERT INTO users (name) VALUES ('Alice');
SAVEPOINT sp1;
INSERT INTO users (name) VALUES ('Bob');
ROLLBACK TO SAVEPOINT sp1;  -- undoes Bob only
INSERT INTO users (name) VALUES ('Charlie');
RELEASE SAVEPOINT sp1;
COMMIT;  -- Alice and Charlie are committed

-- Nested savepoints
BEGIN;
SAVEPOINT outer;
INSERT INTO users (name) VALUES ('X');
SAVEPOINT inner;
INSERT INTO users (name) VALUES ('Y');
ROLLBACK TO SAVEPOINT inner;  -- undoes Y
RELEASE SAVEPOINT outer;
COMMIT;  -- only X is committed
```

## Data Types

| Type | DecentDB | SQLite | PostgreSQL | DuckDB |
|------|----------|--------|------------|--------|
| NULL | ✅ | ✅ | ✅ | ✅ |
| INTEGER/INT | ✅ | ✅ | ✅ | ✅ |
| BIGINT | ✅ | ✅ | ✅ | ✅ |
| FLOAT/REAL | ✅ | ✅ | ✅ | ✅ |
| DOUBLE PRECISION | ✅ | ✅ | ✅ | ✅ |
| TEXT | ✅ | ✅ | ✅ | ✅ (VARCHAR) |
| BLOB | ✅ | ✅ | ✅ | ✅ |
| BOOLEAN | ✅ | ✅ | ✅ | ✅ |
| UUID | ✅ | ❌ | ✅ | ✅ |
| DECIMAL/NUMERIC | ✅ | ✅ | ✅ | ✅ |
| DATE | ✅ (native int64 µs UTC) | ✅ | ✅ | ✅ (native) |
| TIMESTAMP | ✅ (native int64 µs UTC) | ✅ | ✅ | ✅ (native) |

### Examples

```sql
-- Integer types
CREATE TABLE t1 (a INTEGER, b BIGINT, c INT);

-- Floating point
CREATE TABLE t2 (a FLOAT, b REAL, c DOUBLE PRECISION);

-- Text and binary
CREATE TABLE t3 (name TEXT, data BLOB);

-- Boolean
CREATE TABLE t4 (active BOOLEAN DEFAULT TRUE);

-- Decimal (exact numeric)
CREATE TABLE t5 (price DECIMAL(10,2), tax NUMERIC(5,4));

-- UUID
CREATE TABLE t6 (id UUID PRIMARY KEY DEFAULT GEN_RANDOM_UUID());

-- Date and Timestamp (stored as ISO-format TEXT in DecentDB)
CREATE TABLE events (
  id INTEGER PRIMARY KEY,
  event_date DATE,
  created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
INSERT INTO events (event_date, created_at) VALUES ('2024-03-15', '2024-03-15 14:30:00');
```

## Constraints

| Constraint | DecentDB | SQLite | PostgreSQL | DuckDB |
|------------|----------|--------|------------|--------|
| PRIMARY KEY | ✅ | ✅ | ✅ | ✅ |
| FOREIGN KEY | ✅ | ✅ | ✅ | ⚠️ (parsed, not enforced) |
| NOT NULL | ✅ | ✅ | ✅ | ✅ |
| UNIQUE | ✅ | ✅ | ✅ | ✅ |
| CHECK | ✅ | ✅ | ✅ | ✅ |
| DEFAULT | ✅ | ✅ | ✅ | ✅ |

### Examples

```sql
-- PRIMARY KEY (auto-assignment with a single INT64 PRIMARY KEY; INT/INTEGER/INT64 are aliases)
CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT);

-- FOREIGN KEY with actions
CREATE TABLE orders (
  id INTEGER PRIMARY KEY,
  user_id INTEGER REFERENCES users(id) ON DELETE CASCADE
);

-- NOT NULL and UNIQUE
CREATE TABLE accounts (
  email TEXT NOT NULL UNIQUE,
  username TEXT NOT NULL
);

-- CHECK constraint
CREATE TABLE products (
  price DECIMAL(10,2) CHECK (price >= 0),
  qty INTEGER CHECK (qty >= 0)
);

-- DEFAULT values
CREATE TABLE posts (
  id INTEGER PRIMARY KEY,
  status TEXT DEFAULT 'draft',
  created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
```

## Set Operations

| Operation | DecentDB | SQLite | PostgreSQL | DuckDB |
|-----------|----------|--------|------------|--------|
| UNION | ✅ | ✅ | ✅ | ✅ |
| UNION ALL | ✅ | ✅ | ✅ | ✅ |
| INTERSECT | ✅ | ✅ | ✅ | ✅ |
| INTERSECT ALL | ✅ | ✅ | ✅ | ✅ |
| EXCEPT | ✅ | ✅ | ✅ | ✅ |
| EXCEPT ALL | ✅ | ✅ | ✅ | ✅ |

### Examples

```sql
-- UNION (deduplicated) vs UNION ALL (keeps duplicates)
SELECT name FROM employees UNION SELECT name FROM contractors;
SELECT name FROM employees UNION ALL SELECT name FROM contractors;

-- INTERSECT (rows in both sets)
SELECT user_id FROM orders INTERSECT SELECT user_id FROM returns;

-- INTERSECT ALL (preserves duplicate counts)
SELECT user_id FROM orders INTERSECT ALL SELECT user_id FROM returns;

-- EXCEPT (rows in first set but not second)
SELECT user_id FROM all_users EXCEPT SELECT user_id FROM banned_users;

-- EXCEPT ALL (multiset difference)
SELECT item_id FROM inventory EXCEPT ALL SELECT item_id FROM sold;
```

## CTEs (Common Table Expressions)

| Feature | DecentDB | SQLite | PostgreSQL | DuckDB |
|---------|----------|--------|------------|--------|
| WITH ... AS | ✅ | ✅ | ✅ | ✅ |
| WITH RECURSIVE | ✅ | ✅ | ✅ | ✅ |
| Multiple CTEs | ✅ | ✅ | ✅ | ✅ |

### Examples

```sql
-- Basic CTE
WITH active_users AS (
  SELECT * FROM users WHERE active = TRUE
)
SELECT name FROM active_users ORDER BY name;

-- Multiple CTEs
WITH
  dept_totals AS (
    SELECT department, SUM(salary) AS total FROM employees GROUP BY department
  ),
  high_spend AS (
    SELECT * FROM dept_totals WHERE total > 500000
  )
SELECT * FROM high_spend;

-- WITH RECURSIVE (generate a sequence 1..10)
WITH RECURSIVE cnt(x) AS (
  SELECT 1
  UNION ALL
  SELECT x + 1 FROM cnt WHERE x < 10
)
SELECT x FROM cnt;

-- WITH RECURSIVE (tree traversal — find all descendants of node 1)
WITH RECURSIVE descendants AS (
  SELECT id, name, parent_id FROM categories WHERE id = 1
  UNION ALL
  SELECT c.id, c.name, c.parent_id
  FROM categories c INNER JOIN descendants d ON c.parent_id = d.id
)
SELECT * FROM descendants;
```
