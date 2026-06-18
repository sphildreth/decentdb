# Binding Compatibility Matrix

The status values are intentionally limited to:

`supported`, `partial`, `not supported`, `not applicable`, `unknown`.

## Feature-by-Binding Support

| Capability | Rust Native | C ABI | Python | Go | Java / JDBC | Node.js | Dart | .NET | Web / WASM |
|---|---|---|---|---|---|---|---|---|
| Open/close database | supported | supported | supported | supported | supported | supported | supported | supported | supported |
| Execute SQL | supported | supported | supported | supported | supported | supported | supported | supported | supported |
| Prepared statements | supported | supported | partial | supported | supported | partial | partial | supported | partial |
| Parameter binding | supported | supported | supported | supported | supported | supported | partial | supported | supported |
| Row iteration | supported | supported | supported | supported | supported | supported | partial | supported | partial |
| Row-view or batch fetch | supported | supported | unknown | partial | unknown | partial | partial | partial | partial |
| Branch API | supported | supported | unknown | partial | unknown | unknown | unknown | unknown | not applicable |
| Geometry helpers | supported | supported | partial | partial | partial | partial | partial | partial | not applicable |
| Error codes | supported | supported | partial | supported | partial | partial | partial | supported | partial |
| Watch / change notification | supported | supported | partial | supported | unknown | unknown | unknown | partial | not applicable |
| Sync or write queue | not applicable | supported | partial | supported | unknown | unknown | not applicable | partial | not applicable |
| Package-level smoke tests | supported | supported | supported | supported | supported | supported | supported | supported | not applicable |

Notes:

- Branch API in Go is now available for create/list/delete and branch-scoped execution.
- Go currently does not expose a persistent branch session switch; callers execute work against a branch via explicit branch APIs.
- Web/WASM result paging and binary transport return copied JavaScript-owned rows; borrowed row-view APIs are not exposed in `browser-app-v2`.
- “partial” marks intentionally scoped support in one or more supported entry points.
- “not applicable” indicates the feature is outside the binding surface in this repository context.

## Semantic Type Result Mapping

DecentDB treats the C ABI as the shared native boundary across bindings.
`tests/bindings/` contains the narrow cross-language smoke or ABI validation
paths, while the in-tree package implementations live under `bindings/`.

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
