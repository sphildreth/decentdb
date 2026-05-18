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
DURATION
```

These are the only type additions justified at this time. Types such as `EMAIL`, `URL`, `PHONE`, `JSON`, `SEMVER`, `MONEY`, `GEOPOINT`, `VECTOR`, `MACADDR`, `HOSTNAME`, and `DOMAIN` are **rejected** as `ColumnType` variants. Their validation and query value can be delivered through the existing `CHECK` constraint evaluator plus SQL functions, which is sufficient and avoids permanent type-system tax (see §2).

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
| `DURATION` | Yes (int64 µs vs text) | Same | Yes | — | **Keep** |
| `EMAIL` | No (same as text) | Lexical = desired | Yes | No | **Reject** |
| `URL` | No | Lexical = desired | Yes | No | **Reject** |
| `PHONE` | Minor | Lexical = desired | Yes | No | **Reject** |
| `SEMVER` | Minor (~5-10 B) | SemVer ≠ lexical | Yes | No | **Reject** |
| `JSON` | No (text) | Lexical = desired | Yes | No | **Reject** |

The four rejected types (`EMAIL`, `URL`, `PHONE`, `SEMVER`) provide schema introspection value but no storage or ordering win. Their value should be delivered as:

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

This is ~18 modification sites per new type. The cost is non-trivial but bounded. The set of 7 types above is the smallest set that delivers meaningful technical advantages over `CHECK + functions`.

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
| 18 | `DURATION` |

`TIMESTAMPTZ` reuses the existing `TAG_TIMESTAMP = 8` with distinct catalog semantics.

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

Declaration-order semantics deferred. Phase 1 supports equality and inequality only. `ORDER BY` on an enum column compares by label identifier, which reflects insertion/appending order (not reorderable declaration order). This is documented explicitly.

#### Functions

```sql
enum_label(value)   → TEXT       -- label string for an enum value
enum_id(value)      → INTEGER    -- stable identifier
enum_valid(text)    → BOOLEAN    -- check if text is a valid label for this enum's column type
```

`enum_values('type_name')` requires named enum support and is deferred.

#### Schema evolution

| Operation | Supported? | Notes |
|---|---|---|
| Add label | Yes | New id assigned; existing rows unaffected |
| Remove label | Yes, if no rows reference it | Validated at DDL time |
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
Inline size:          1–N bytes (varint-bounded)
Overflow behavior:    N/A (fixed width per enum instance)
Literal syntax:       Implicit string in typed column context
Accepted inputs:      Strings matching a declared label (case-sensitive)
Canonical format:     Label string
Comparison:           Equality only (identifier-based); ordering deferred
Hash:                 Based on label identifier
Index support:        B-tree (on identifier); expression indexes on enum_label()
Casts from:           TEXT
Casts to:             TEXT
Functions:            enum_label(), enum_id(), enum_valid()
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

Natural binary ordering: IPv4-mapped addresses compare correctly against IPv6 addresses when using the unified 16-byte representation. Family byte is the primary ordering key, so all IPv4 addresses sort before all IPv6 addresses.

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
Comparison:           Binary ordering (family-byte primary, address bytes secondary)
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

No custom operators (`<<`, `<<=`) initially. All containment queries use function syntax.

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
Index support:        B-tree; containment indexes deferred
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

Signed 32-bit integer: days since Unix epoch (1970-01-01). Encoded as zigzag varint.

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

Signed 64-bit integer: microseconds since midnight (0–86,399,999,999 µs). Encoded as zigzag varint.

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
time_add_duration(t, d)     → TIME
time_diff_micros(a, b)      → INTEGER (DURATION)
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
Functions:            time_extract_*(), time_add_duration(), time_diff_micros(), time_to_text(), time_from_text()
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
WAL encoding:         Reuses TAG_TIMESTAMP (8); discrimination by catalog column type
```

---

### 5.7 `DURATION`

#### Motivation

Durations (elapsed time intervals) appear in jobs, timers, performance metrics, lease TTLs, cache expirations, retry policies, task estimates, telemetry, and billing. Storing them as text strings (`'30s'`, `'5m'`) requires parsing at query time and produces incorrect ordering (`'9s' < '100ms'` lexicographically).

#### Storage format

Signed 64-bit integer: microseconds. Encoded as zigzag varint.

#### SQL syntax

```sql
CREATE TABLE jobs (
    id INT PRIMARY KEY,
    timeout DURATION NOT NULL,
    retry_delay DURATION NOT NULL
);
```

#### Literal syntax

```sql
DURATION '30s'
DURATION '5m'
DURATION '2h'
DURATION '7d'
DURATION '1500ms'
DURATION '500us'
```

Units: `us` (microseconds), `ms` (milliseconds), `s` (seconds), `m` (minutes), `h` (hours), `d` (days).

#### Ordering

Natural integer ordering (microseconds). `DURATION '9s' > DURATION '100ms'` correctly.

#### Functions

```sql
duration_micros(d)          → INTEGER
duration_millis(d)          → INTEGER
duration_seconds(d)         → INTEGER
duration_from_micros(n)     → DURATION
duration_from_millis(n)     → DURATION
duration_from_seconds(n)    → DURATION
duration_to_text(d)         → TEXT
duration_from_text(text)    → DURATION
```

#### Arithmetic

```sql
TIMESTAMPTZ '2026-01-01 00:00:00Z' + DURATION '30d'
TIME '09:00:00' + DURATION '90m'
date + DURATION '7d'
```

#### Per-type checklist

```
Type name:            DURATION
Aliases:              INTERVAL (subset — only elapsed duration, not calendar intervals)
Storage class:        Zigzag-varint int64 (microseconds)
Inline size:          1–9 bytes (varint)
Overflow behavior:    N/A (fixed width)
Literal syntax:       DURATION 'N unit'
Accepted inputs:      Number + unit (us, ms, s, m, h, d)
Canonical format:     Highest-magnitude unit that preserves precision
Comparison:           Integer ordering
Hash:                 Based on int64 value
Index support:        B-tree
Casts from:           TEXT
Casts to:             TEXT
Functions:            duration_micros(), duration_millis(), duration_seconds(), duration_from_*(), duration_to_text(), duration_from_text()
Binding behavior:     Phase A: canonical string; Phase B+: binding-native duration type
Migration:            ALTER COLUMN TYPE with USING duration_from_text(old_col)
Error codes:          E_TYPE_INVALID_DURATION
WAL encoding:         Tag 18 + zigzag-varint int64
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
DURATION
```

`CIDR` depends on `IPADDR`. The three temporal types are independent of each other and of the network types. `DATE` and `TIME` fix the semantic collapse from ADR 0114.

### Phase 3: Timezone semantics

```
TIMESTAMPTZ
```

Already has native storage (`TAG_TIMESTAMP`). The addition is catalog-level semantic distinction and session display behavior. This can ship with or after the other temporal types.

---

## 7. Competitive Context

### SQLite

SQLite's dynamic type system associates the datatype with the value, not the column. `STRICT` tables limit types to `INT`, `INTEGER`, `REAL`, `TEXT`, `BLOB`, and `ANY`. The proposed types give DecentDB a clear differentiation: embedded simplicity with stronger schema semantics.

### PostgreSQL

PostgreSQL has `inet`, `cidr`, `macaddr`, and enum types. DecentDB's `IPADDR` and `CIDR` align with PostgreSQL's network address types conceptually but use a simpler binary encoding. DecentDB's `ENUM` follows PostgreSQL's model (ordered label set) but with stable-identifier storage rather than OID-based storage.

### DuckDB

DuckDB is analytics-oriented with nested/composite types. DecentDB is not competing on that axis. The proposed types target application schema correctness, not analytical query flexibility.

---

## 8. Binding Strategy

Semantic types are exposed to bindings in three maturity phases:

| Phase | Behavior |
|---|---|
| Phase A | All values read as canonical strings via the C ABI. Type metadata exposed through `column_type` introspection. |
| Phase B | Optional helper types in each language binding (e.g., `DecentDB.IpAddr` in .NET, `decentdb.IPAddr` in Go). |
| Phase C | Typed parameter binding — `stmt.BindIpAddr(1, addr)` rather than binding as bytes/text. |

Phase A is the acceptance criterion for engine implementation. Phases B and C are deferred to binding-specific PRs.

---

## 9. Risks

| Risk | Impact | Mitigation |
|---|---|---|
| ENUM schema evolution breaks rows | High | Stable label identifiers decouple storage from declaration order |
| IPADDR unified 16-byte encoding wastes space for IPv4-heavy datasets | Medium | Tagged encoding (4/16 byte select) benchmarked before final storage decision |
| DATE/TIME/DURATION zigzag-varint encoding underflows on extreme negative values | Low | DATE epoch covers all practical ranges; DURATION negative values are uncommon but supported |
| TIMESTAMPTZ session timezone requires global state | Medium | Default to UTC; session timezone is a connection-level setting, not engine-global |
| ~18 modification sites per type is maintenance burden | Medium | Each type documents its match sites; regression test suite covers type-dependent paths |
| Row tag namespace exhaustion (255 max) | Low | 7 new tags use 13–18 of 255 available; room for ~237 more types before rework needed |

---

## 10. Rationale Summary

The guiding question for every type decision is: **Does this require a `ColumnType` variant, or can `TEXT + CHECK + functions` deliver equivalent value?**

Seven types pass this bar:

| Type | Primary justification |
|---|---|
| `ENUM` | Compact storage (u8/u16 vs string) + parametric type category (label set is schema metadata, not a constraint) |
| `IPADDR` | Compact storage (4/16 B vs 7–39 B) + correct binary ordering |
| `CIDR` | Compact storage + containment query semantics dependent on IPADDR binary encoding |
| `DATE` | Compact storage (int32 days) + semantic correctness (date ≠ timestamp) |
| `TIME` | Compact storage (int64 µs) + semantic correctness (time-of-day ≠ timestamp) |
| `TIMESTAMPTZ` | Schema identity (distinct from naive TIMESTAMP) + session display semantics |
| `DURATION` | Compact storage (int64 µs) + correct ordering + arithmetic integration with temporal types |

Types rejected (`EMAIL`, `URL`, `PHONE`, `SEMVER`, `JSON`, `MONEY`, `GEOPOINT`, `VECTOR`, `MACADDR`, `HOSTNAME`, `DOMAIN`) are better served by `TEXT` with `CHECK` constraints and SQL functions. Their schema-level identity wins do not justify permanent `ColumnType` expansion.
