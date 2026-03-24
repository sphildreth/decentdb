# Binding Compatibility Matrix

| Binding surface | Current in-tree scope | Source of truth |
|---|---|---|
| C ABI | Stable handle/result core surface | `include/decentdb.h`, `crates/decentdb/src/c_api.rs` |
| .NET | `DecentDB.Native`, `DecentDB.AdoNet`, `DecentDB.MicroOrm`, EF Core provider family, plus smoke validation | `bindings/dotnet/src/`, `bindings/dotnet/tests/`, `tests/bindings/dotnet/Smoke/` |
| Python | DB-API driver, SQLAlchemy dialect, import tools, plus direct native validation | `bindings/python/`, `bindings/python/tests/`, `tests/bindings/python/test_ffi.py` |
| Go | `database/sql` driver, direct DecentDB helper API, plus release smoke | `bindings/go/decentdb-go/`, `tests/bindings/go/smoke.go` |
| Java / JDBC | JDBC driver, JNI bridge, DBeaver extension, plus low-level FFM smoke | `bindings/java/driver/`, `bindings/java/native/`, `bindings/java/dbeaver-extension/`, `tests/bindings/java/Smoke.java` |
| Node.js | `decentdb-native` addon/wrapper, `knex-decentdb` dialect, plus release smoke | `bindings/node/`, `tests/bindings/node/` |
| Dart | Packaged FFI wrapper, examples, and release smoke | `bindings/dart/dart/`, `bindings/dart/examples/`, `tests/bindings/dart/` |

DecentDB treats the C ABI as the shared native boundary across bindings.
`tests/bindings/` contains the narrow cross-language smoke or ABI validation
paths, while the in-tree package implementations live under `bindings/`.
