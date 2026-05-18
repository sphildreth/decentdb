# Data Types

DecentDB supports the following SQL data types.

## Supported Types

### INTEGER / INT / INT64

64-bit signed integer.

Range: -9,223,372,036,854,775,808 to 9,223,372,036,854,775,807

```sql
CREATE TABLE example (
    id INTEGER PRIMARY KEY,
    count INTEGER,
    big_number INT64
);
```

A single INT64 `PRIMARY KEY` column supports auto-increment: if the column is omitted from an `INSERT` statement, DecentDB automatically assigns the next sequential ID. (`INT`, `INTEGER`, and `INT64` are synonyms here.) Explicit values are also accepted.

```sql
INSERT INTO example (count) VALUES (42);       -- id auto-assigned
INSERT INTO example VALUES (100, 7, 999);      -- explicit id = 100
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

### VARCHAR / CHARACTER VARYING

Variable-length UTF-8 string (alias for TEXT).

DecentDB treats VARCHAR and CHARACTER VARYING as equivalent to TEXT, ignoring any specified length constraints.

```sql
CREATE TABLE example (
    name VARCHAR(255),        -- Same as TEXT
    description VARCHAR,      -- Same as TEXT
    title CHARACTER VARYING(100)  -- Same as TEXT
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

### DECIMAL / NUMERIC

Fixed-point number with user-specified precision and scale.

Suitable for financial calculations where exactness is required.

Range: Precision up to 18 digits (constrained by int64 backing storage).

```sql
CREATE TABLE example (
    balance DECIMAL(10,2),    -- 10 total digits, 2 decimal places
    rate NUMERIC(5,4)         -- 5 total digits, 4 decimal places
);
```

### UUID

16-byte Universally Unique Identifier.

Stored efficiently as 16-byte binary data.

```sql
CREATE TABLE example (
    id UUID PRIMARY KEY DEFAULT GEN_RANDOM_UUID(),
    ref_id UUID
);
```

### TIMESTAMP / DATETIME

Date and time value stored natively as microseconds since the Unix epoch.

Accepts ISO 8601 string literals on INSERT; values are read back as formatted strings
(`YYYY-MM-DD HH:MM:SS[.ffffff]`).

`TIMESTAMP`, `TIMESTAMP WITHOUT TIME ZONE`, and `DATETIME` map to the native
TIMESTAMP type.

```sql
CREATE TABLE events (
    id         INT PRIMARY KEY,
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at DATETIME
);

-- Insert using string literal
INSERT INTO events (id, created_at) VALUES (2, '2026-02-24 17:30:00');

-- Insert using NOW()
INSERT INTO events (id, created_at) VALUES (3, NOW());

-- Query with EXTRACT
SELECT EXTRACT(YEAR FROM created_at) FROM events;
```

**Storage:** TIMESTAMP columns are backed by int64 microseconds since Unix epoch UTC.
To minimize disk footprint and align with SQLite's size, they use Varint encoding (1 to 9 bytes).
String literals are parsed on INSERT and converted transparently.

### DATE

Calendar date stored as signed days since the Unix epoch.

```sql
CREATE TABLE invoices (
    id INT PRIMARY KEY,
    invoice_date DATE NOT NULL
);

INSERT INTO invoices VALUES (1, '2026-05-18');
```

`DATE` accepts `YYYY-MM-DD` text or an integer day count. It is stored as an
integer day value, not as a timestamp string.

### TIME

Time of day stored as microseconds after midnight.

```sql
CREATE TABLE shifts (
    id INT PRIMARY KEY,
    starts_at TIME NOT NULL
);

INSERT INTO shifts VALUES (1, '09:30:00.123456');
```

`TIME` accepts `HH:MM:SS[.ffffff]` text or an integer microsecond count.

### TIMESTAMPTZ

Timestamp with time zone stored as UTC microseconds since the Unix epoch.

```sql
CREATE TABLE audit_log (
    id INT PRIMARY KEY,
    observed_at TIMESTAMPTZ NOT NULL
);

INSERT INTO audit_log VALUES (1, '2026-05-18T09:10:11.123456-05:00');
INSERT INTO audit_log VALUES (2, '2026-05-18 14:10:11.123456Z');
```

`TIMESTAMPTZ` accepts `Z` or numeric offsets and normalizes the stored value to
UTC. `TIMESTAMP WITH TIME ZONE` is an alias for `TIMESTAMPTZ`.

### INTERVAL

Duration value stored as three native components: months, days, and
microseconds.

```sql
CREATE TABLE reminders (
    id INT PRIMARY KEY,
    delay INTERVAL NOT NULL
);

INSERT INTO reminders VALUES (1, '1 year 2 months 3 days 4.5 seconds');
INSERT INTO reminders VALUES (2, '0 7 3600000000'); -- months, days, micros
```

The text form accepts amount/unit pairs such as `2 weeks`, `3 days`, `5 hours`,
`10 minutes`, `1.5 seconds`, `250 milliseconds`, or `100 microseconds`. The
compact numeric form is `months days microseconds`.

### ENUM

Inline enumerated value with catalog-persisted labels and compact row storage.

```sql
CREATE TABLE orders (
    id INT PRIMARY KEY,
    status ENUM('new', 'paid', 'shipped', 'cancelled') NOT NULL
);

INSERT INTO orders VALUES (1, 'paid');
```

Enum rows store a stable enum type id plus a stable label id, not the label
string itself. The catalog stores the label mapping so SQL dump and metadata
paths can preserve labels. Low-level bindings expose those ids directly; see
the binding API pages for language-specific result mappings.

### IPADDR / INET

IPv4 or IPv6 address stored in canonical binary form.

```sql
CREATE TABLE hosts (
    id INT PRIMARY KEY,
    address IPADDR NOT NULL
);

INSERT INTO hosts VALUES (1, '192.168.10.20');
INSERT INTO hosts VALUES (2, '2001:db8::1');
```

`INET` is an alias for `IPADDR`.

### CIDR

IPv4 or IPv6 network stored as an address family, prefix length, and normalized
network address.

```sql
CREATE TABLE networks (
    id INT PRIMARY KEY,
    block CIDR NOT NULL
);

INSERT INTO networks VALUES (1, '192.168.10.0/24');
INSERT INTO networks VALUES (2, '2001:db8::/32');
```

Host bits are cleared on insert, so CIDR values compare and render in canonical
network form.

### MACADDR / MACADDR8

Six-byte or eight-byte MAC address stored in binary form and rendered as
lowercase colon-separated hex.

```sql
CREATE TABLE devices (
    id INT PRIMARY KEY,
    nic MACADDR NOT NULL,
    eui64 MACADDR8
);

INSERT INTO devices VALUES (1, '08:00:2b:01:02:03', '08:00:2b:ff:fe:01:02:03');
```

`MACADDR` and `MACADDR8` currently share the same native column type and validate
the inserted address length from the text literal.

### GEOMETRY

Planar spatial value stored as normalized EWKB. Use `GEOMETRY` for Cartesian coordinates such as projected map coordinates, CAD data, and local coordinate systems.

Optional type modifiers constrain subtype, dimensionality, and SRID:

```sql
CREATE TABLE parcels (
    id INTEGER PRIMARY KEY,
    boundary GEOMETRY(POLYGON,3857),
    centroid GEOMETRY(POINT,3857)
);
```

Supported subtypes are `POINT`, `LINESTRING`, `POLYGON`, `MULTIPOINT`, `MULTILINESTRING`, and `MULTIPOLYGON`. Dimensional suffixes `Z`, `M`, and `ZM` are supported, for example `GEOMETRY(POINTZ,3857)`.

### GEOGRAPHY

WGS84 lon/lat spatial value stored as normalized EWKB with SRID 4326. Use `GEOGRAPHY` for earth-distance queries in meters.

```sql
CREATE TABLE places (
    id INTEGER PRIMARY KEY,
    location GEOGRAPHY(POINT,4326)
);
```

Initial GEOGRAPHY support accepts SRID 4326 and the subtypes `POINT`, `POLYGON`, and `MULTIPOLYGON`. Coordinates are validated as longitude in `[-180, 180]` and latitude in `[-90, 90]`.

### NULL

Represents missing or unknown values.

All columns can contain NULL unless marked NOT NULL.

## Type Aliases

| Alias | Maps To |
|-------|---------|
| INTEGER | INT64 |
| INT | INT64 |
| BIGINT | INT64 |
| SERIAL | INT64 (auto-increment) |
| TEXT | TEXT |
| VARCHAR | TEXT |
| CHARACTER VARYING | TEXT |
| CHAR | TEXT |
| BLOB | BLOB |
| BYTEA | BLOB |
| BOOLEAN | BOOL |
| BOOL | BOOL |
| FLOAT | FLOAT64 |
| REAL | FLOAT64 |
| DOUBLE | FLOAT64 |
| DOUBLE PRECISION | FLOAT64 |
| NUMERIC | DECIMAL |
| DECIMAL | DECIMAL |
| UUID | UUID |
| DATE | DATE |
| TIMESTAMP | TIMESTAMP (native datetime) |
| TIMESTAMP WITHOUT TIME ZONE | TIMESTAMP |
| DATETIME | TIMESTAMP |
| TIMESTAMPTZ | TIMESTAMPTZ |
| TIMESTAMP WITH TIME ZONE | TIMESTAMPTZ |
| TIME | TIME |
| TIME WITHOUT TIME ZONE | TIME |
| INTERVAL | INTERVAL |
| ENUM(...) | ENUM |
| IPADDR | IPADDR |
| INET | IPADDR |
| CIDR | CIDR |
| MACADDR | MACADDR |
| MACADDR8 | MACADDR |
| GEOMETRY | GEOMETRY |
| GEOGRAPHY | GEOGRAPHY |

## Type Conversion

Implicit conversions happen automatically when safe:
- INT → FLOAT (for comparisons)
- Any type can become NULL

Explicit conversion uses CAST:
```sql
SELECT CAST(price AS INT) FROM products;
```

## Storage Details

| Type | Inline Size | Overflow |
|------|-------------|----------|
| INT64 | 1 to 9 bytes (Varint) | Never |
| BOOL | 1 byte | Never |
| FLOAT64 | 8 bytes | Never |
| DECIMAL | 1 to 9 bytes (Varint) | Never |
| UUID | 16 bytes | Never |
| TIMESTAMP | 1 to 9 bytes (Varint) | Never |
| ENUM | Varint type id + label id | Never |
| IPADDR | 5 bytes for IPv4, 17 bytes for IPv6 | Never |
| CIDR | 6 bytes for IPv4, 18 bytes for IPv6 | Never |
| DATE | 1 to 5 bytes (Varint) | Never |
| TIME | 1 to 9 bytes (Varint) | Never |
| TIMESTAMPTZ | 1 to 9 bytes (Varint) | Never |
| INTERVAL | 3 Varint components | Never |
| MACADDR | 7 bytes for MAC-48, 9 bytes for EUI-64 | Never |
| TEXT | Variable, up to 512 bytes | > 512 bytes |
| BLOB | Variable, up to 512 bytes | > 512 bytes |
| GEOMETRY | Variable EWKB | > 512 bytes |
| GEOGRAPHY | Variable EWKB | > 512 bytes |
| NULL | 0 payload bytes (1-byte tag)| Never |

### Compression

TEXT and BLOB values are automatically compressed with zlib when stored on overflow pages. This is transparent to the application — values are decompressed on read.

## Best Practices

1. Use INT for primary keys and counters (auto-incremented when omitted from INSERT)
2. Use TEXT for names, descriptions, JSON
3. Use BLOB for binary data, images
4. Use BOOL for flags and states
5. Use FLOAT for measurements and prices
6. Use GEOGRAPHY for lon/lat distances in meters, and GEOMETRY for planar spatial work
7. Use DATE, TIME, TIMESTAMPTZ, IPADDR, CIDR, MACADDR, and ENUM when the domain
   is known; they are smaller and easier for bindings to decode than ad hoc
   strings
8. Avoid storing large BLOBs if possible (consider file storage with path in DB)
