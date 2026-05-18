# Semantic Type Additions for DecentDB

**Date:** 2026-05-18
**Status:** Draft

**Purpose:** Define which additional `ColumnType` variants DecentDB should add, justify each against the `CHECK + functions` baseline, and specify implementation requirements per type.

**Audience:** Engine implementors, catalog/record/wal owners, binding maintainers.

---

## 1. Decision

Add the following `ColumnType` variants to `crates/decentdb/src/catalog/schema.rs`:

```sql
ENUM
IPADDR
CIDR
DATE
TIME
TIMESTAMPTZ
INTERVAL
MACADDR
```

Types such as `EMAIL`, `URL`, `PHONE`, `JSON`, `SEMVER`, `MONEY`, `GEOPOINT`, `VECTOR`, `HOSTNAME`, and `DOMAIN` are **rejected** as `ColumnType` variants. When in doubt, DecentDB follows PostgreSQL semantics; deviations are documented with rationale. Their validation and query value can be delivered through the existing `CHECK` constraint evaluator plus SQL functions, which is sufficient and avoids permanent type-system tax (see §2).

---

## 2. Why Not Just CHECK + Functions?

DecentDB already has:

- A working `CHECK` constraint evaluator (`exec/constraints.rs:303-326`) that re-parses and evaluates SQL expressions against rows on INSERT/UPDATE.
- A SQL function dispatch system capable of calling per-type validation and extraction logic.
- A `ColumnType` enum (`catalog/schema.rs:18-30`) that is a closed, compile-time exhaustive match used in ~18 sites throughout the engine (row encoding, row decoding, index key encoding, index comparison, value casting, value comparison, JSON parsing, C ABI tags, SQL name mapping, type inference).

Adding a `ColumnType` variant is a **permanent tax**. Every future type-related feature must account for every variant. A type deserves a variant only if `TEXT + CHECK + functions` **cannot** deliver at least two of:

1. **Compact binary storage** — the native encoding is materially smaller than the canonical text form.
2. **Correct ordering** — the natural byte ordering of the native encoding matches the domain's sort semantics, which differ from lexicographic string ordering.
3. **Schema-level type identity** — the type must be introspectable as distinct from `TEXT` (for bindings, tooling, `information_schema`).
4. **Parametric type categories** — the type has sub-parameters (e.g., enum label sets, IP version tagging) that cannot be expressed as a flat `TEXT CHECK` constraint.

### Per-type justification

| Type | Compact storage | Correct ordering | Schema identity | Parametric | Verdict |
|---|---|---|---|---|---|
| `ENUM` | Yes (u8/u16 vs string) | Declaration-order ≠ lexical | Yes | Yes (label set) | **Keep** |
| `IPADDR` | Yes (4/16 B vs 7-39 B) | Binary ordering ≠ lexical | Yes | Yes (v4/v6) | **Keep** |
| `CIDR` | Yes (5/17 B vs 9-49 B) | Depends on IPADDR | Yes | Yes (prefix len) | **Keep** |
| `DATE` | Yes (int32 days vs text) | Same | Yes | — | **Keep** |
| `TIME` | Yes (int64 µs vs text) | Same | Yes | — | **Keep** |
| `TIMESTAMPTZ` | Same as Timestamp | Same | Yes | — | **Keep** |
| `INTERVAL` | Yes (12-20 B vs text) | Same | Yes | Yes (months/days/µs) | **Keep** |
| `EMAIL` | No (same as text) | Lexical = desired | Yes | No | **Reject** |
| `URL` | No | Lexical = desired | Yes | No | **Reject** |
| `PHONE` | Minor | Lexical = desired | Yes | No | **Reject** |
| `SEMVER` | Minor (~5-10 B) | SemVer ≠ lexical | Yes | No | **Reject** |
| `JSON` | No (text) | Lexical = desired | Yes | No | **Reject** |
| `MONEY` | No (same as DECIMAL) | Same as DECIMAL | Yes | No | **Reject** |
| `GEOPOINT` | Yes (16 B) | Same | Yes | No | **Reject** (use GEOMETRY) |
| `VECTOR` | Minor | Specialized | Yes | Yes (dimensions) | **Reject** (future ADR) |
| `MACADDR` | Yes (6/8 B vs 17 B) | Lexical = desired | Yes | Yes (6/8-byte form) | **Keep** |
| `HOSTNAME` | No | Lexical = desired | Yes | No | **Reject** |
| `DOMAIN` | No | Lexical = desired | Yes | No | **Reject** |

The rejected types provide schema introspection value but no storage or ordering win (or, in the case of `GEOPOINT`, are subsumed by the existing `GEOMETRY` type; `VECTOR` may warrant a future ADR). Their value should be delivered as:

```sql
-- Instead of EMAIL as a ColumnType variant:
CREATE TABLE users (
    email TEXT NOT NULL
        CHECK (email_is_valid(email))
);
CREATE UNIQUE INDEX users_email_uq
    ON users (email_normalize(email));

-- Instead of URL as a ColumnType variant:
CREATE TABLE links (
    target TEXT NOT NULL
        CHECK (url_is_valid(target))
);
-- url_host(), url_scheme(), etc. are pure functions.

-- Instead of PHONE as a ColumnType variant:
CREATE TABLE contacts (
    phone TEXT
        CHECK (phone_is_valid(phone) OR phone IS NULL)
);

-- Instead of SEMVER as a ColumnType variant:
CREATE TABLE packages (
    version TEXT NOT NULL
        CHECK (semver_is_valid(version))
);
```

This approach preserves all the query, validation, and indexing benefits without any `ColumnType` expansion. The loss is that `information_schema` reports these columns as `TEXT` rather than `EMAIL`/`URL`/`PHONE`/`SEMVER`. That tradeoff is acceptable.

---

## 3. Type System Architecture (How DecentDB Types Work Today)

The type system is a closed, compile-time enum with exhaustive match dispatch. There is no type registry, no trait-based dispatch, and no extension mechanism.

### Sites that must be modified per new type

| File | Function/Location | What to change |
|---|---|---|
| `catalog/schema.rs` | `ColumnType` enum (L18-30) | Add variant |
| `catalog/schema.rs` | `as_str()` (L34-47) | Add string name |
| `record/value.rs` | `Value` enum (L8-20) | Add variant |
| `record/value.rs` | `approximate_heap_bytes()` | Add branch |
| `record/row.rs` | TAG constants (L12-24) | Add tag (max 255) |
| `record/row.rs` | `encode_values_into_with_overflow()` | Add encode arm |
| `record/row.rs` | `decode_with_overflow()` | Add decode arm |
| `record/row.rs` | WAL log record encoding | Add WAL encode/decode |
| `record/key.rs` | TAG constants + `encode_index_key()` | Add index key encoding |
| `record/key.rs` | `compare_index_values()` | Add index comparison |
| `exec/mod.rs` | `cast_value()` (L26078-26171) | Add coercion logic |
| `exec/mod.rs` | `compare_values()` (L30136-30218) | Add comparison logic |
| `exec/mod.rs` | `infer_column_type_for_ctas()` | Add type inference |
| `db.rs` | `json_to_typed_value()` | Add JSON parsing |
| `sql/normalize.rs` | `normalize_type_name_with_spatial()` | Add SQL name mapping |
| `tooling.rs` | `value_kind()`, `c_value_tag()`, `column_type_from_name()`, `value_column_type()` | Add tooling entries |
| `include/decentdb.h` | `ddb_value_tag_t` enum | Add C tag |

This is ~18 modification sites per new type. The cost is non-trivial but bounded. The set of 8 types above is the smallest set that delivers meaningful technical advantages over `CHECK + functions`.

---

## 4. WAL Encoding

Each new `Value` variant requires a defined WAL log record encoding for ACID durability.

The WAL currently encodes row-level changes. Each new type's tag and binary payload must be decodable during crash recovery. The type-specific encoding rules are specified per-type below.

Row tags allocated:

| Tag | Type |
|---|---|
| 13 | `ENUM` |
| 14 | `IPADDR` |
| 15 | `CIDR` |
| 16 | `DATE` |
| 17 | `TIME` |
| 18 | `TIMESTAMPTZ` |
| 19 | `INTERVAL` |
| 20 | `MACADDR` |

`TIMESTAMPTZ` has the same binary payload as `TIMESTAMP` (zigzag-varint i64 µs since epoch), but it receives a distinct row tag because DecentDB's current row decoder is schema-agnostic. Preserving the semantic `Value::TimestampTzMicros` at row decode time keeps WAL replay, JSON rendering, C ABI views, and bindings from needing column-catalog context for every decoded value.

**ADR requirement:** This document reverses ADR 0114's decision to collapse `DATE` and `TIMESTAMP` into a single type. A formal ADR (To be numbered) should be created to record this reversal before implementation begins.

---

## 5. Per-Type Specification

### 5.1 `ENUM`

#### Motivation

Finite value sets (statuses, roles, priorities, categories) appear in nearly every application. `TEXT` columns require application-level enforcement; `CHECK (value IN (...))` works but duplicates the label set in every constraint and offers no compact storage.

#### Storage format

Each enum is identified by a **stable label identifier** — an integer assigned per-label within the enum type, stored in catalog metadata. This identifier is **not** the declaration position. Labels can be inserted, appended, or removed (if no rows reference the removed label) without invalidating existing row data.

| Enum cardinality | Identifier width | Row payload |
|---|---|---|
| 1–255 labels | `u8` | 1 byte |
| 256–65,535 labels | `u16` | 2 bytes (varint) |
| Larger | varint | variable |

The label-to-id mapping is stored in the catalog alongside the enum type definition. When a label is appended, it receives the next unused id in sequence. When a label is inserted mid-list, it receives a new id; existing ids are unaffected.

#### SQL syntax

Inline:

```sql
CREATE TABLE tasks (
    id INT PRIMARY KEY,
    status ENUM('todo', 'doing', 'done') NOT NULL
);
```

Named (deferred to follow-on, requires `CREATE TYPE` support):

```sql
CREATE TYPE task_status AS ENUM ('todo', 'doing', 'done');
CREATE TABLE tasks (
    id INT PRIMARY KEY,
    status task_status NOT NULL
);
```

#### Literal syntax

```sql
'todo'           -- implicit string-to-enum cast in typed column context
ENUM 'todo'      -- explicit literal (if parser supports typed literals)
```

#### Ordering

Declaration-order semantics, matching PostgreSQL: `ORDER BY` on an enum column sorts by the order the labels were defined in the `CREATE TABLE` or `CREATE TYPE` statement. Internally, stable identifiers are assigned in definition order at DDL time, so identifier ordering matches declaration ordering for the initial label set. Labels appended later receive higher identifiers and sort after the original set. This is consistent with PostgreSQL's behavior.

#### Functions

```sql
enum_label(value)   → TEXT       -- label string for an enum value
enum_id(value)      → INTEGER    -- stable identifier
```

`enum_valid(text)` is **not provided** as a standalone function because it lacks column-type context — a plain `TEXT` input cannot resolve which enum's label set to validate against. Instead, validation is enforced by the type system at INSERT/UPDATE time. If explicit runtime validation is needed, use `CHECK (value IN ('todo', 'doing', 'done'))` or a named-enum validator (deferred, requires `CREATE TYPE`).

`enum_values('type_name')` requires named enum support and is deferred.

#### Schema evolution

| Operation | Supported? | Notes |
|---|---|---|
| Add label | Yes | New id assigned; existing rows unaffected |
| Remove label | Yes, if no rows reference it | Validated at DDL time via catalog check (constant-time for inline enums, requires row scan for large tables — implementation must document cost) |
| Rename label | Yes | Updates catalog mapping; ids unchanged |
| Reorder labels | No | Declaration order is not a storage property |

#### Migration from TEXT

```sql
ALTER TABLE tasks ALTER COLUMN status SET DATA TYPE ENUM('todo', 'doing', 'done')
    USING status;
```

If any row contains a value not in the new enum's label set, the ALTER fails.

#### Per-type checklist

```
Type name:            ENUM
Aliases:              —
Storage class:        Tagged u8/u16/varint label identifier
Inline size:          1 byte (≤255 labels) / 2 bytes (≤65535 labels) / varint (larger)
Overflow behavior:    N/A (fixed width per enum instance)
Literal syntax:       Implicit string in typed column context
Accepted inputs:      Strings matching a declared label (case-sensitive)
Canonical format:     Label string
Comparison:           Declaration-order (identifier-based; initial labels assigned ids in definition order)
Hash:                 Based on label identifier
Index support:        B-tree (on identifier); expression indexes on enum_label()
Casts from:           TEXT
Casts to:             TEXT
Functions:            enum_label(), enum_id()
Binding behavior:     Phase A: canonical string; Phase B+: typed adapter
Migration:            ALTER COLUMN TYPE with USING
Error codes:          E_TYPE_INVALID_ENUM_VALUE
WAL encoding:         Tag 13 + varint label identifier
```

---

### 5.2 `IPADDR`

#### Motivation

IP addresses appear in logs, auth, security, rate limiting, audit, networking, and IoT applications. Storing them as text wastes space (7–39 bytes for a value that fits in 4 or 16 bytes) and produces incorrect ordering (`"10.1.2.3" < "2.2.2.2"` lexicographically).

#### Storage format

Unified 16-byte representation with a family tag:

```
[family: u8] [address_bytes: 16 bytes]
```

- `family = 4`: IPv4 stored in the first 4 bytes of `address_bytes` (IPv4-mapped), remaining 12 bytes zeroed.
- `family = 6`: IPv6 stored in all 16 bytes.

Note: for comparison purposes, the IPv4-mapped representation (`::ffff:x.x.x.x`) is stored only in IPv6 columns. An IPv4-family value stores the address in the first 4 bytes and is distinct from the IPv6-mapped equivalent. See §5.2 Ordering for detail.

Total payload: 17 bytes. An alternative compact encoding (tagged 4-byte IPv4, 16-byte IPv6) saves 13 bytes per IPv4 row but adds a branch to every encode/decode/compare path. Implementation should benchmark both before committing.

#### SQL syntax

```sql
CREATE TABLE login_events (
    id INT PRIMARY KEY,
    source_ip IPADDR NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
```

#### Literal syntax

```sql
IPADDR '10.1.2.3'
IPADDR '::1'
'10.1.2.3'              -- implicit cast in typed column context
```

#### Ordering

Binary ordering matching PostgreSQL's `inet` semantics: IPv4-mapped IPv6 addresses (`::ffff:x.x.x.x`) compare adjacent to their IPv4 counterpart. The family byte is **not** the primary sort key. Instead, the 16-byte address field is compared first; the family byte serves as a tiebreaker. This means `IPADDR '10.1.2.3'` and `IPADDR '::ffff:10.1.2.3'` are adjacent in index order (though not equal — they are distinct values of different families).

This differs from a naive family-byte-first ordering, which would partition all IPv4 before all IPv6. DecentDB follows PostgreSQL's convention where cross-family comparison is meaningful and IPv4-mapped addresses sort near their IPv4 equivalent.

#### Functions

```sql
ip_family(ip)       → INTEGER    -- 4 or 6
ip_is_private(ip)   → BOOLEAN
ip_is_loopback(ip)  → BOOLEAN
ip_is_multicast(ip) → BOOLEAN
ip_to_text(ip)      → TEXT
ip_from_text(text)  → IPADDR
```

#### Per-type checklist

```
Type name:            IPADDR
Aliases:              INET (for PostgreSQL compatibility)
Storage class:        Tagged binary (17 bytes: 1 family + 16 address)
Inline size:          17 bytes
Overflow behavior:    N/A (fixed width)
Literal syntax:       IPADDR 'x.x.x.x' / IPADDR '::1'
Accepted inputs:      IPv4 dotted-decimal, IPv6 colon-hex, IPv4-mapped IPv6
Canonical format:     ip_to_text() output (consistent display)
Comparison:           PostgreSQL-compatible (address bytes primary, family byte secondary; IPv4-mapped sort near IPv4)
Hash:                 Based on full 17-byte representation
Index support:        B-tree
Casts from:           TEXT
Casts to:             TEXT
Functions:            ip_family(), ip_is_private(), ip_is_loopback(), ip_is_multicast(), ip_to_text(), ip_from_text()
Binding behavior:     Phase A: canonical string; Phase B+: binary bytes / native IP types
Migration:            ALTER COLUMN TYPE with USING ip_from_text(old_col)
Error codes:          E_TYPE_INVALID_IPADDR
WAL encoding:         Tag 14 + 17-byte binary payload
```

---

### 5.3 `CIDR`

#### Motivation

CIDR notation (`10.0.0.0/8`) pairs an IP network address with a prefix length. It enables containment queries (`is this IP in this network?`) that are common in security, ACLs, allow/deny lists, and network management.

#### Storage format

```
[family: u8] [prefix_length: u8] [network_address: 16 bytes]
```

Total payload: 18 bytes. The network address is the base address with host bits zeroed.

#### Dependencies

`CIDR` depends on `IPADDR`'s binary representation, comparison behavior, and display formatting. Implement `IPADDR` first.

#### SQL syntax

```sql
CREATE TABLE network_rules (
    id INT PRIMARY KEY,
    network CIDR NOT NULL,
    action ENUM('allow', 'deny') NOT NULL
);
```

#### Literal syntax

```sql
CIDR '10.0.0.0/8'
CIDR '2001:db8::/32'
```

#### Functions

```sql
cidr_contains(cidr, ip)        → BOOLEAN   -- does network contain this IP?
cidr_family(cidr)              → INTEGER   -- 4 or 6
cidr_prefix_length(cidr)       → INTEGER   -- e.g., 8 for /8
cidr_network_address(cidr)     → IPADDR    -- base address with host bits zeroed
cidr_broadcast_address(cidr)   → IPADDR    -- highest address in network
```

No custom operators (`<<`, `<<=`) initially. All containment queries use function syntax. Containment-optimized indexes (e.g., GIN-style prefix indexes) are deferred to a future type-specific index ADR.

#### Per-type checklist

```
Type name:            CIDR
Aliases:              —
Storage class:        Tagged binary (18 bytes: 1 family + 1 prefix + 16 address)
Inline size:          18 bytes
Overflow behavior:    N/A (fixed width)
Literal syntax:       CIDR 'x.x.x.x/n' / CIDR '::1/n'
Accepted inputs:      Standard CIDR notation
Canonical format:     network_address/prefix_length (host bits zeroed)
Comparison:           Binary (family, prefix, then address)
Hash:                 Based on full 18-byte representation
Index support:        B-tree; containment indexes deferred (future GIN-style ADR)
Casts from:           TEXT
Casts to:             TEXT, IPADDR (to base address)
Functions:            cidr_contains(), cidr_family(), cidr_prefix_length(), cidr_network_address(), cidr_broadcast_address()
Binding behavior:     Phase A: canonical string
Migration:            ALTER COLUMN TYPE with USING CAST(old_col AS CIDR)
Error codes:          E_TYPE_INVALID_CIDR
WAL encoding:         Tag 15 + 18-byte binary payload
```

---

### 5.4 `DATE`

#### Motivation

A date-only value is not a timestamp. Birthdays, due dates, billing dates, and calendar days should not carry an implicit midnight-to-midnight time assumption or timezone interpretation.

ADR 0114 collapsed `DATE` and `TIMESTAMP` into a single `TIMESTAMP` ColumnType variant (storing dates as midnight UTC). This decision reverses that collapse: `DATE` becomes a distinct variant.

#### Storage format

Signed 32-bit integer: days since Unix epoch (1970-01-01). Encoded as zigzag varint. Range: approximately ±5.88 million years (int32 bounds). Accepted input range matches PostgreSQL's practical range (0001-01-01 to 5874897-12-31). Values outside this range are rejected at parse time. Year 0 is not valid (proleptic Gregorian calendar, year 1 BCE is followed by year 1 CE).

#### SQL syntax

```sql
CREATE TABLE invoices (
    id INT PRIMARY KEY,
    due_date DATE NOT NULL
);
```

#### Literal syntax

```sql
DATE '2026-05-18'
'2026-05-18'            -- implicit cast in typed column context
```

#### Ordering

Natural integer ordering.

#### Functions

```sql
date_extract_year(d)        → INTEGER
date_extract_month(d)       → INTEGER
date_extract_day(d)         → INTEGER
date_extract_dow(d)         → INTEGER  -- 0=Sun, 6=Sat
date_add_days(d, n)         → DATE
date_diff_days(a, b)        → INTEGER
date_trunc_month(d)         → DATE     -- first day of month
date_to_text(d)             → TEXT
date_from_text(text)        → DATE
```

#### Casting

```sql
CAST(timestamp AS DATE)     -- truncates time component
CAST(date AS TIMESTAMP)     -- assumes midnight UTC
CAST(text AS DATE)          -- parses ISO date string
```

#### Per-type checklist

```
Type name:            DATE
Aliases:              —
Storage class:        Zigzag-varint int32 (days since epoch)
Inline size:          1–5 bytes (varint)
Overflow behavior:    N/A (fixed width)
Literal syntax:       DATE 'YYYY-MM-DD'
Accepted inputs:      ISO 8601 date strings
Canonical format:     YYYY-MM-DD
Comparison:           Integer ordering
Hash:                 Based on int32 value
Index support:        B-tree
Casts from:           TEXT, TIMESTAMP
Casts to:             TEXT, TIMESTAMP
Functions:            date_extract_*(), date_add_days(), date_diff_days(), date_trunc_month(), date_to_text(), date_from_text()
Binding behavior:     Phase A: canonical string; Phase B+: binding-native date type
Migration:            ALTER COLUMN TYPE with USING date_from_text(old_col)
Error codes:          E_TYPE_INVALID_DATE
WAL encoding:         Tag 16 + zigzag-varint int32
```

---

### 5.5 `TIME`

#### Motivation

A time-of-day value is not a timestamp. Business hours, schedules, recurring alarms, and daily event windows need time-only storage without date or timezone assumptions.

#### Storage format

Signed 64-bit integer: microseconds since midnight (0–86,399,999,999 µs). Encoded as zigzag varint. Leap seconds are not supported — `TIME` represents a time-of-day within a standard 86,400-second day. A value representing 23:59:60 is rejected at parse time.

#### SQL syntax

```sql
CREATE TABLE business_hours (
    id INT PRIMARY KEY,
    opens_at TIME NOT NULL,
    closes_at TIME NOT NULL
);
```

#### Literal syntax

```sql
TIME '09:00:00'
TIME '14:30:00.500'
```

#### Ordering

Natural integer ordering (microseconds since midnight).

#### Functions

```sql
time_extract_hour(t)        → INTEGER
time_extract_minute(t)      → INTEGER
time_extract_second(t)      → INTEGER
time_extract_micros(t)      → INTEGER
time_add_interval(t, i)    → TIME       -- add interval to time (days/months ignored)
time_diff_micros(a, b)  → INTERVAL
time_to_text(t)             → TEXT
time_from_text(text)        → TIME
```

#### Casting

```sql
CAST(timestamp AS TIME)     -- extracts time component
CAST(text AS TIME)          -- parses ISO time string
```

#### Per-type checklist

```
Type name:            TIME
Aliases:              TIME WITHOUT TIME ZONE
Storage class:        Zigzag-varint int64 (microseconds since midnight)
Inline size:          1–9 bytes (varint)
Overflow behavior:    N/A (fixed width)
Literal syntax:       TIME 'HH:MM:SS[.ffffff]'
Accepted inputs:      ISO 8601 time strings
Canonical format:     HH:MM:SS.ffffff
Comparison:           Integer ordering
Hash:                 Based on int64 value
Index support:        B-tree
Casts from:           TEXT, TIMESTAMP
Casts to:             TEXT
Functions:            time_extract_*(), time_add_interval(), time_diff_micros(), time_to_text(), time_from_text()
Binding behavior:     Phase A: canonical string; Phase B+: binding-native time type
Migration:            ALTER COLUMN TYPE with USING time_from_text(old_col)
Error codes:          E_TYPE_INVALID_TIME
WAL encoding:         Tag 17 + zigzag-varint int64
```

---

### 5.6 `TIMESTAMPTZ`

#### Motivation

Currently `TIMESTAMP`, `TIMESTAMPTZ`, `DATE`, and `DATETIME` are all aliases for the same `ColumnType::Timestamp` variant, stored as `Value::TimestampMicros(i64)`. The distinction between a naive local timestamp (`TIMESTAMP`) and an absolute UTC instant (`TIMESTAMPTZ`) matters for applications that operate across timezones.

#### Decision

`TIMESTAMPTZ` becomes a distinct `ColumnType` variant with the same underlying binary encoding as `TIMESTAMP` (microseconds since Unix epoch UTC) but distinct catalog and type-identity semantics:

- `TIMESTAMP` — naive/local timestamp. Stored as-is; no implicit UTC normalization.
- `TIMESTAMPTZ` — absolute instant, always stored as UTC. On input, values with an explicit timezone offset are converted to UTC before storage. On display, rendered in the session timezone (or UTC if unset).

The storage engine treats both identically (zigzag-varint int64 of microseconds since epoch). The difference is in session-level display formatting and input interpretation. `ColumnType::TimestampTz` carries the semantic contract distinct from `ColumnType::Timestamp`.

#### SQL syntax

```sql
CREATE TABLE events (
    id INT PRIMARY KEY,
    created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP
);
```

#### Per-type checklist

```
Type name:            TIMESTAMPTZ
Aliases:              TIMESTAMP WITH TIME ZONE
Storage class:        Zigzag-varint int64 (microseconds since epoch UTC)
Inline size:          1–9 bytes (varint)
Overflow behavior:    N/A (fixed width)
Literal syntax:       TIMESTAMPTZ 'YYYY-MM-DD HH:MM:SS[.ffffff][+/-TZ]'
Accepted inputs:      ISO 8601 timestamps, with or without timezone offset
Canonical format:     YYYY-MM-DD HH:MM:SS.ffffff+00 (always UTC)
Comparison:           Integer ordering
Hash:                 Based on int64 value
Index support:        B-tree
Casts from:           TEXT, TIMESTAMP
Casts to:             TEXT, TIMESTAMP
Functions:            Same as TIMESTAMP functions
Binding behavior:     Phase A: canonical string; Phase B+: timezone-aware native types
Migration:            ALTER COLUMN TYPE with USING CAST(old_col AS TIMESTAMPTZ)
Error codes:          E_TYPE_INVALID_TIMESTAMPTZ
WAL encoding:         Tag 18 + zigzag-varint i64; separate tag preserves schema-agnostic decode
```

---

### 5.7 `INTERVAL`

#### Motivation

Intervals (elapsed time and calendar durations) appear in jobs, timers, performance metrics, lease TTLs, cache expirations, retry policies, task estimates, telemetry, and billing. Storing them as text strings (`'30s'`, `'5m'`) requires parsing at query time and produces incorrect ordering (`'9s' < '100ms'` lexicographically).

DecentDB follows PostgreSQL's `INTERVAL` model: intervals are composed of a months component, a days component, and a microseconds component. This tripartite representation is necessary because `1 month` and `30 days` are not equivalent (months vary in length), and `1 day` and `86400 seconds` are not equivalent across DST boundaries. A single scalar (e.g., raw microseconds) cannot represent calendar-relative durations correctly.

#### Storage format

Three-field structure matching PostgreSQL's internal representation:

```
[months: zigzag-varint i32] [days: zigzag-varint i32] [microseconds: zigzag-varint i64]
```

Example encodings:

| Input | months | days | microseconds | Total bytes (approx.) |
|---|---|---|---|---|
| `INTERVAL '30s'` | 0 | 0 | 30,000,000 | ~6 |
| `INTERVAL '7d'` | 0 | 7 | 0 | ~3 |
| `INTERVAL '1 month'` | 1 | 0 | 0 | ~2 |
| `INTERVAL '1 year 2 months 3 days 4 hours'` | 14 | 3 | 14,400,000,000 | ~7 |

The `months` field allows values ≥ 12 to represent multi-year intervals. The `days` and `microseconds` fields store non-calendar units. This is identical to PostgreSQL's approach: `INTERVAL '1 year'` is stored as `{months: 12, days: 0, usecs: 0}`.

#### SQL syntax

```sql
CREATE TABLE jobs (
    id INT PRIMARY KEY,
    timeout INTERVAL NOT NULL,
    retry_delay INTERVAL NOT NULL
);
```

#### Literal syntax

```sql
INTERVAL '30 seconds'
INTERVAL '5 minutes'
INTERVAL '2 hours'
INTERVAL '7 days'
INTERVAL '1 month'
INTERVAL '1 year 2 months 3 days 4 hours 5 minutes 6.789 seconds'
```

Units: `microseconds`/`ms`/`seconds`/`minutes`/`hours`/`days`/`weeks`/`months`/`years`, plus standard abbreviations. `days` (1 day = calendar day, not necessarily 86400 seconds across DST transitions), `months` (1 month = calendar month, 28–31 days), and `years` (1 year = 12 months) are calendar units stored in their respective fields. Sub-day units (hours, minutes, seconds, ms, µs) are accumulated into the `microseconds` field.

#### Ordering

Component-wise ordering: months first, then days, then microseconds. This matches PostgreSQL's interval comparison semantics. Note: intervals with different compositions can be semantically inequivalent but compare equal (e.g., `1 month` vs `30 days` — months=1/days=0 vs months=0/days=30 — months field decides: `1 month > 30 days`).

#### Functions

```sql
interval_months(i)          → INTEGER       -- months field
interval_days(i)            → INTEGER       -- days field
interval_micros(i)          → INTEGER       -- microseconds field (absolute)
interval_from_days(n)       → INTERVAL      -- days-only interval
interval_from_micros(n)     → INTERVAL      -- microseconds-only interval
interval_from_seconds(n)    → INTERVAL      -- seconds-only interval
interval_to_text(i)         → TEXT
interval_from_text(text)    → INTERVAL
```

#### Arithmetic

```sql
TIMESTAMPTZ '2026-01-01 00:00:00Z' + INTERVAL '30 days'
TIMESTAMPTZ '2026-03-15 12:00:00Z' + INTERVAL '1 month'   -- yields 2026-04-15
TIME '09:00:00' + INTERVAL '90 minutes'
DATE '2026-05-18' + INTERVAL '7 days'
TIMESTAMP '2026-01-01 00:00:00' + INTERVAL '1 day'       -- note: naive timestamp, no DST awareness
```

Calendar-aware arithmetic (months, days) requires a temporal operand (TIMESTAMPTZ, DATE). Adding `INTERVAL '1 month'` to `TIMESTAMPTZ` uses calendar arithmetic; adding `INTERVAL '30 seconds'` uses simple µs addition.

#### Per-type checklist

```
Type name:            INTERVAL
Aliases:              —
Storage class:        Tripartite (zigzag-varint i32 months + i32 days + i64 microseconds)
Inline size:          3–14 bytes (varint fields)
Overflow behavior:    N/A (fixed width)
Literal syntax:       INTERVAL 'N unit [N unit ...]'
Accepted inputs:      PostgreSQL-compatible interval literals
Canonical format:     Months-days-microseconds decomposition
Comparison:           Component-wise (months, days, microseconds)
Hash:                 Based on (months, days, microseconds) tuple
Index support:        B-tree
Casts from:           TEXT
Casts to:             TEXT
Functions:            interval_months(), interval_days(), interval_micros(), interval_from_*(), interval_to_text(), interval_from_text()
Binding behavior:     Phase A: canonical string (e.g., '1 mon 3 days 04:05:06'); Phase B+: binding-native interval type
Migration:            ALTER COLUMN TYPE with USING interval_from_text(old_col)
Error codes:          E_TYPE_INVALID_INTERVAL
WAL encoding:         Tag 18 + varint months + varint days + varint microseconds
```

---

## 6. Implementation Order

### Phase 1: Foundation types

```
ENUM
IPADDR
```

`ENUM` is the most broadly useful addition with the clearest storage advantage over `TEXT`. `IPADDR` is the most compact storage win and is a dependency for `CIDR`.

### Phase 2: Network and temporal correctness

```
CIDR
DATE
TIME
INTERVAL
```

`CIDR` depends on `IPADDR`. The three temporal types are independent of each other and of the network types. `DATE` and `TIME` fix the semantic collapse from ADR 0114. `INTERVAL` follows PostgreSQL's tripartite model to correctly handle calendar-relative durations.

### Phase 3: Timezone semantics

```
TIMESTAMPTZ
```

Already has native storage (`TAG_TIMESTAMP`). The addition is catalog-level semantic distinction and session display behavior. This can ship with or after the other temporal types.

### Phase 4: Network address completion

```
MACADDR
```

`MACADDR` meets the storage (6/8 bytes vs 17-byte text) and ordering criteria. It is represented as a native 6- or 8-byte payload with a one-byte length marker.

---

## 7. Competitive Context

### SQLite

SQLite's dynamic type system associates the datatype with the value, not the column. `STRICT` tables limit types to `INT`, `INTEGER`, `REAL`, `TEXT`, `BLOB`, and `ANY`. The proposed types give DecentDB a clear differentiation: embedded simplicity with stronger schema semantics.

### PostgreSQL

PostgreSQL has `inet`, `cidr`, `macaddr`, `enum`, and `interval` types. DecentDB's `IPADDR` and `CIDR` align with PostgreSQL's network address types conceptually but use a simpler binary encoding. DecentDB's `ENUM` follows PostgreSQL's model (ordered label set) but with stable-identifier storage rather than OID-based storage. DecentDB's `INTERVAL` follows PostgreSQL's tripartite `{months, days, microseconds}` storage model directly.

### DuckDB

DuckDB is analytics-oriented with nested/composite types. DecentDB is not competing on that axis. The proposed types target application schema correctness, not analytical query flexibility.

---

## 8. Binding Strategy

Semantic types are exposed to bindings in three maturity phases:

| Phase | Behavior |
|---|---|
| Phase A | New `ddb_value_tag_t` entries (11–18) are added to `include/decentdb.h`. All values read as canonical strings through the existing `ddb_value_t.data`/`len` path. Type metadata exposed through `column_type` introspection. Bindings that have not been updated still see these columns as TEXT-like string values. |
| Phase B | Optional helper types in each language binding (e.g., `DecentDB.IpAddr` in .NET, `decentdb.IPAddr` in Go). |
| Phase C | Typed parameter binding — `stmt.BindIpAddr(1, addr)` rather than binding as bytes/text. |

Phase A is the acceptance criterion for engine implementation. Phases B and C are deferred to binding-specific PRs.

---

## 9. Risks

| Risk | Impact | Mitigation |
|---|---|---|
| ENUM schema evolution breaks rows | High | Stable label identifiers decouple storage from declaration order |
| IPADDR unified 16-byte encoding wastes space for IPv4-heavy datasets | Medium | Tagged encoding (4/16 byte select) benchmarked before final storage decision |
| DATE/TIME/INTERVAL zigzag-varint encoding underflows on extreme negative values | Low | DATE epoch covers all practical ranges; INTERVAL negative values are uncommon but supported |
| TIMESTAMPTZ session timezone requires global state | Medium | Default to UTC; session timezone is a connection-level setting, not engine-global |
| INTERVAL tripartite storage is larger than a single int64 | Medium | Varint encoding compresses zero fields; most intervals use 1–2 of 3 fields. Matches PostgreSQL semantics; avoids DST/calendar bugs |
| INTERVAL component-wise ordering can be unintuitive (1 month ≠ 30 days) | Medium | Documented explicitly; matches PostgreSQL behavior |
| ~18 modification sites per type is maintenance burden | Medium | Each type documents its match sites; regression test suite covers type-dependent paths |
| Row tag namespace exhaustion (255 max) | Low | 8 new types use 13–20 of 255 available (including MACADDR); room for ~235 more before rework needed |
| ADR 0114 reversal (re-introducing DATE as distinct from TIMESTAMP) | Medium | Formal ADR to be created documenting the reversal and rationale before implementation |

---

## 10. Rationale Summary

The guiding question for every type decision is: **Does this require a `ColumnType` variant, or can `TEXT + CHECK + functions` deliver equivalent value?**

Eight types pass this bar:

| Type | Primary justification |
|---|---|
| `ENUM` | Compact storage (u8/u16 vs string) + parametric type category (label set is schema metadata, not a constraint) |
| `IPADDR` | Compact storage (4/16 B vs 7–39 B) + correct binary ordering |
| `CIDR` | Compact storage + containment query semantics dependent on IPADDR binary encoding |
| `DATE` | Compact storage (int32 days) + semantic correctness (date ≠ timestamp) |
| `TIME` | Compact storage (int64 µs) + semantic correctness (time-of-day ≠ timestamp) |
| `TIMESTAMPTZ` | Schema identity (distinct from naive TIMESTAMP) + session display semantics |
| `INTERVAL` | Compact storage (tripartite 12–20 B vs text) + correct calendar ordering + arithmetic integration with temporal types + parametric (months/days/µs fields) |
| `MACADDR` | Compact storage (6/8 B vs 17 B text) + correct binary ordering |

Types rejected (`EMAIL`, `URL`, `PHONE`, `SEMVER`, `JSON`, `MONEY`, `GEOPOINT`, `VECTOR`, `HOSTNAME`, `DOMAIN`) are better served by `TEXT` with `CHECK` constraints and SQL functions. Their schema-level identity wins do not justify permanent `ColumnType` expansion.
