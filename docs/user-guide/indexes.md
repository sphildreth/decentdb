# Indexes

Indexes speed up queries by allowing the database to find data without scanning entire tables.

## B-Tree Indexes

The default index type, ideal for exact matches and range queries.

### Creating B-Tree Indexes

```sql
-- Single column
CREATE INDEX idx_users_email ON users(email);

-- Composite index (useful for multi-column WHERE clauses)
CREATE INDEX idx_users_name_age ON users(name, age);

-- Unique index
CREATE UNIQUE INDEX idx_users_email_unique ON users(email);
```

### When B-Tree Indexes Are Used

B-Tree indexes are used for:
- Equality: `WHERE email = 'user@example.com'`
- Range queries: `WHERE age > 18 AND age < 65`
- Prefix matching: `WHERE name LIKE 'Alice%'`
- ORDER BY: `ORDER BY email`
- JOINs on indexed columns

### Automatic Indexes

DecentDB creates indexes automatically for:
- PRIMARY KEY columns
- FOREIGN KEY columns
- UNIQUE constraints

```sql
-- This creates an automatic unique index on id
CREATE TABLE users (
    id INTEGER PRIMARY KEY,
    email TEXT UNIQUE
);

-- This creates an automatic index on user_id
CREATE TABLE orders (
    id INTEGER PRIMARY KEY,
    user_id INTEGER REFERENCES users(id)
);
```

## Partial Indexes

Create an index that only includes rows meeting a condition.

```sql
-- Index only active users
CREATE INDEX idx_users_active ON users(id) WHERE status IS NOT NULL;
```

**Note:** Partial/filtered indexes are supported for BTREE indexes with arbitrary predicates. Partial trigram indexes are not supported.

## Expression Indexes

Index the result of a function or expression.

```sql
-- Index lowercase name for case-insensitive search
CREATE INDEX idx_users_lower_name ON users((LOWER(name)));
```

**Note:** Expression indexes are supported for BTREE indexes but are currently limited to a single deterministic expression (e.g. `LOWER`, `UPPER`, `TRIM`, `LENGTH`, `CAST`). `UNIQUE` expression indexes are not supported.

## Trigram Indexes

Specialized indexes for fast substring and pattern matching.

### Creating Trigram Indexes

```sql
CREATE INDEX idx_users_name_trgm ON users USING trigram(name);
```

### When Trigram Indexes Are Used

Trigram indexes accelerate:
- Substring search: `WHERE name LIKE '%john%'`
- Case-insensitive search: `WHERE name ILIKE '%JOHN%'`

```sql
-- Uses trigram index
SELECT * FROM users WHERE name LIKE '%smith%';

-- Also uses trigram index
SELECT * FROM users WHERE name ILIKE '%SMITH%';
```

### Trigram Index Limitations

- Patterns must be at least 3 characters
- Very common patterns (like 'the') may not use the index
- Only works with `%pattern%` style LIKE queries

## Spatial Indexes

Spatial indexes accelerate native `GEOMETRY` and `GEOGRAPHY` columns. They store spatial envelopes in a native grid index and refine matches with exact `ST_*` predicate evaluation.

### Creating Spatial Indexes

```sql
CREATE TABLE places (
    id INTEGER PRIMARY KEY,
    geog GEOGRAPHY(POINT,4326)
);

CREATE INDEX idx_places_geog ON places USING spatial(geog);
```

### When Spatial Indexes Are Used

Spatial indexes accelerate single-table filters where one side is an indexed spatial column and the other side is a constant expression:

- `ST_DWithin(geog, ST_GeogPoint(lon, lat), meters)`
- `ST_Intersects(geom, ST_GeomFromText(...))`
- `ST_Contains(geom, ST_GeomFromText(...))`
- `ST_Within(geom, ST_GeomFromText(...))`
- `ST_Equals(geom, ST_GeomFromText(...))`

They also appear in `EXPLAIN` for nearest-neighbor ordering with `<->`:

```sql
EXPLAIN
SELECT id
FROM places
ORDER BY geog <-> ST_GeogPoint(-97.7431, 30.2672)
LIMIT 10;
```

Spatial indexes are also used for the narrow point-in-polygon join shape where one joined table has the indexed spatial column:

```sql
EXPLAIN
SELECT h.id, z.id
FROM houses h
JOIN zones z
  ON ST_Contains(z.boundary, h.location);
```

### Spatial Index Limitations

- Spatial indexes are single-column only.
- `UNIQUE`, partial predicates, expression keys, and `INCLUDE` columns are not supported for spatial indexes.
- GEOGRAPHY indexes use WGS84 lon/lat with SRID 4326.
- Exact predicates still run after candidate lookup, so results remain correct even when the grid returns false positives.

## Index Selectivity

Selectivity measures how unique index values are:

- **High selectivity** (unique values): Excellent for indexing (e.g., email, SSN)
- **Low selectivity** (repeated values): Poor for indexing (e.g., boolean, status)

```sql
-- Good: High selectivity
CREATE INDEX idx_users_email ON users(email);

-- Bad: Low selectivity
-- Don't index columns with few distinct values
-- CREATE INDEX idx_users_active ON users(active);  -- Only true/false
```

## Managing Indexes

### Listing Indexes

```bash
# All indexes
decentdb list-indexes --db=my.ddb

# Indexes for specific table
decentdb list-indexes --db=my.ddb --table=users
```

### Rebuilding Indexes

Over time, indexes can become fragmented. Rebuild them for better performance:

```bash
# Rebuild a specific index
decentdb rebuild-index --db=my.ddb --index=idx_users_email
```

### Dropping Indexes

```sql
DROP INDEX idx_users_email;
```

Drop indexes that aren't being used to save space and improve write performance.

## Index Performance Impact

### Read Performance

Indexes improve read performance:

```sql
-- With index: O(log n) - very fast
SELECT * FROM users WHERE email = 'alice@example.com';

-- Without index: O(n) - full table scan
SELECT * FROM users WHERE name = 'Alice';  -- Slow if name not indexed
```

### Write Performance

Indexes slow down writes:

```sql
-- Slower: Must update both table and all indexes
INSERT INTO users (id, email, name, age) VALUES (1, 'a@b.com', 'Alice', 30);

-- Faster with fewer indexes
```

## Best Practices

### DO Index

- PRIMARY KEY and FOREIGN KEY columns (automatic)
- Columns in WHERE clauses
- Columns in JOIN conditions
- Columns in ORDER BY
- Columns frequently used for lookups

### DON'T Index

- Columns with low cardinality (< 1% distinct values)
- Columns that are rarely queried
- Very small tables (< 100 rows)
- Columns that change frequently (high write, low read)

### Composite Index Ordering

For composite indexes, column order matters:

```sql
-- Good for: WHERE name = 'Alice' AND age > 18
-- Good for: WHERE name = 'Alice'
-- Bad for: WHERE age > 18 (can't use first column)
CREATE INDEX idx_users_name_age ON users(name, age);
```

Put equality columns first, range columns last.

### Covering Indexes

An index "covers" a query if it contains all columns needed:

```sql
-- Index on (name, email)
CREATE INDEX idx_users_name_email ON users(name, email);

-- Covered query (only needs name and email)
SELECT email FROM users WHERE name = 'Alice';

-- Not covered (needs age too)
SELECT email, age FROM users WHERE name = 'Alice';
```

## Index Statistics

View index information:

```bash
# Table description shows indexes
decentdb describe --db=my.ddb --table=users
```

## Troubleshooting

### Query Not Using Index

Check if:
1. Index exists on the right column
2. Query condition is index-friendly (no functions on column)
3. Selectivity is high enough

```sql
-- Index won't be used (function on column without expression index)
SELECT * FROM users WHERE LOWER(email) = 'alice@example.com';

-- Fix: Create an expression index
CREATE INDEX idx_email_lower ON users((LOWER(email)));
-- Now LOWER(email) lookups use the expression index
```
