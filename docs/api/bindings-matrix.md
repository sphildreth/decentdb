# Binding Compatibility Matrix

| Binding surface | Current in-tree scope | Source of truth |
|---|---|---|
| C ABI | Stable handle/result core surface plus C/C++ integration path | `include/decentdb.h`, `crates/decentdb/src/c_api.rs`, `tests/bindings/c/` |
| .NET | `DecentDB.Native`, `DecentDB.AdoNet`, `DecentDB.MicroOrm`, EF Core provider family, migration/query/failure-path coverage, plus smoke validation | `bindings/dotnet/src/`, `bindings/dotnet/tests/`, `tests/bindings/dotnet/Smoke/` |
| Python | DB-API driver, SQLAlchemy dialect, import tools, plus direct native validation | `bindings/python/`, `bindings/python/tests/`, `tests/bindings/python/test_ffi.py` |
| Go | `database/sql` driver, direct DecentDB helper API, plus release smoke | `bindings/go/decentdb-go/`, `tests/bindings/go/smoke.go` |
| Java / JDBC | JDBC driver, JNI bridge, DBeaver extension, plus low-level FFM smoke | `bindings/java/driver/`, `bindings/java/native/`, `bindings/java/dbeaver-extension/`, `tests/bindings/java/Smoke.java` |
| Node.js | `decentdb-native` addon/wrapper, `knex-decentdb` dialect, plus release smoke | `bindings/node/`, `tests/bindings/node/` |
| Dart | Packaged FFI wrapper, examples, and release smoke | `bindings/dart/dart/`, `bindings/dart/examples/`, `tests/bindings/dart/` |

DecentDB treats the C ABI as the shared native boundary across bindings.
`tests/bindings/` contains the narrow cross-language smoke or ABI validation
paths, while the in-tree package implementations live under `bindings/`.

## Semantic Type Result Mapping

Binding-native semantic types are stored by the engine as compact typed values
and surfaced through each binding's closest native shape:

| DecentDB type | Python | Go | .NET | Node.js | Java / JDBC | Dart |
|---|---|---|---|---|---|---|
| `ENUM` | `decentdb.EnumValue` | `decentdb.EnumValue` | `DecentDBEnumValue` | `"typeId:labelId"` string | `String` | `DecentDBEnumValue` |
| `IPADDR` / `INET` | `ipaddress.IPv4Address` / `IPv6Address` | canonical `string` | canonical `string` | canonical `string` | `String` | canonical `String` |
| `CIDR` | `ipaddress.IPv4Network` / `IPv6Network` | canonical `string` | canonical `string` | canonical `string` | `String` | canonical `String` |
| `DATE` | `datetime.date` | `time.Time` at UTC midnight | `DateOnly` via native value object / field conversion | `YYYY-MM-DD` string | `java.sql.Date` / `Types.DATE` | UTC `DateTime` at midnight |
| `TIME` | `datetime.time` | `time.Duration` since midnight | `TimeOnly` via native value object / field conversion | `HH:MM:SS.ffffff` string | `java.sql.Time` / `Types.TIME` | `Duration` since midnight |
| `TIMESTAMPTZ` | timezone-aware `datetime.datetime` | UTC `time.Time` | `DateTimeOffset` | UTC ISO string ending in `Z` | `Timestamp` / `Types.TIMESTAMP` | UTC `DateTime` |
| `INTERVAL` | `decentdb.IntervalValue` | `decentdb.IntervalValue` | `DecentDBIntervalValue` | `"months days micros"` string | `String` | `DecentDBIntervalValue` |
| `MACADDR` / `MACADDR8` | canonical `str` | canonical `string` | canonical `string` | canonical `string` | `String` / `Types.OTHER` | canonical `String` |

For .NET specifically, the in-tree validation now covers ADO.NET operational
APIs, EF Core migration SQL generation, advanced modeling, query translation
including set operations and window functions, bulk mutation paths, async query
streaming, builder-driven EF setup ergonomics, explicit failure-contract tests,
and lightweight performance-sanity coverage in the showcase plus dedicated EF
tests.
