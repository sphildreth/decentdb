# Data Types

DecentDb supports the following SQL data types.

## Supported Types

### INTEGER / INT / INT64

64-bit signed integer.

Range: -9,223,372,036,854,775,808 to 9,223,372,036,854,775,807

```sql
CREATE TABLE example (
    id INT PRIMARY KEY,
    count INTEGER,
    big_number INT64
);
```

### TEXT

Variable-length UTF-8 string.

Stored with overflow pages for large values (> 512 bytes inline).

```sql
CREATE TABLE example (
    name TEXT,
    description TEXT
);
```

### BLOB

Binary large object.

Stored with overflow pages for large values.

```sql
CREATE TABLE example (
    data BLOB,
    image BLOB
);
```

### BOOLEAN / BOOL

True/false value.

```sql
CREATE TABLE example (
    active BOOLEAN,
    verified BOOL
);
```

### FLOAT / FLOAT64 / REAL

64-bit IEEE 754 floating point number.

```sql
CREATE TABLE example (
    price FLOAT,
    measurement FLOAT64,
    score REAL
);
```

### NULL

Represents missing or unknown values.

All columns can contain NULL unless marked NOT NULL.

## Type Aliases

| Alias | Maps To |
|-------|---------|
| INTEGER | INT64 |
| INT | INT64 |
| TEXT | TEXT |
| VARCHAR | TEXT |
| BLOB | BLOB |
| BOOLEAN | BOOL |
| BOOL | BOOL |
| FLOAT | FLOAT64 |
| REAL | FLOAT64 |
| DOUBLE | FLOAT64 |

## Type Conversion

Implicit conversions happen automatically when safe:
- INT → FLOAT (for comparisons)
- Any type can become NULL

Explicit conversion requires CAST (not yet implemented).

## Storage Details

| Type | Inline Size | Overflow |
|------|-------------|----------|
| INT64 | 8 bytes | Never |
| BOOL | 1 byte | Never |
| FLOAT64 | 8 bytes | Never |
| TEXT | Variable, up to 512 bytes | > 512 bytes |
| BLOB | Variable, up to 512 bytes | > 512 bytes |
| NULL | 0 bytes | Never |

## Best Practices

1. Use INT for primary keys and counters
2. Use TEXT for names, descriptions, JSON
3. Use BLOB for binary data, images
4. Use BOOL for flags and states
5. Use FLOAT for measurements and prices
6. Avoid storing large BLOBs if possible (consider file storage with path in DB)
