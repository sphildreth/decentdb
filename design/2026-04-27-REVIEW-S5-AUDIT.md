# S5 Binding Row-View Audit

This audit checks whether each language binding uses the C ABI row-view APIs as
the primary streaming read path instead of per-cell `ddb_*_value_copy` calls.

| Binding | Status | Evidence | Notes |
|---|---|---|---|
| Python | PASS | `bindings/python/decentdb/__init__.py:370-380`, `bindings/python/decentdb/_fastdecode.c:1586-1688` | Row-view is enabled by default via `DECENTDB_PY_USE_ROW_VIEW=1`, with native fastdecode paths using `ddb_stmt_step_row_view` and `ddb_stmt_fetch_row_views`. |
| Node.js | PASS | `bindings/node/decentdb/index.js:252-263`, `bindings/node/decentdb/index.js:277-288` | Sync and async `exec` read loops call `stmt.stepRowView()`; native glue resolves `ddb_stmt_step_row_view`. |
| Go | PASS | `bindings/go/decentdb-go/driver.go:1073-1089` | `database/sql` row iteration uses the fused `ddb_stmt_step_row_view` path and decodes borrowed row views into Go driver values. |
| Dart | PASS | `bindings/dart/dart/lib/src/statement.dart:874-882`, `bindings/dart/dart/lib/src/statement.dart:911-925` | Streaming uses `stmtStepRowView`; paged reads use `stmtFetchRowViews`. |
| Java/JDBC | PASS | `bindings/java/driver/src/main/java/com/decentdb/jdbc/DecentDBResultSet.java:78-84`, `bindings/java/native/decentdb_jni.c:978-985` | `ResultSet.next()` steps with row views, and JNI getters read from the cached row view for the current row. |
| .NET | FAIL | `bindings/dotnet/src/DecentDB.Native/DecentDB.cs:680-688`, `bindings/dotnet/src/DecentDB.Native/DecentDB.cs:1174-1208` | The native declarations include row-view APIs, but `Statement.Step()` uses `ddb_stmt_step` and `GetRowView()` loops over `ddb_stmt_value_copy`. Follow-up: make .NET streaming reads use `ddb_stmt_step_row_view`/`ddb_stmt_fetch_row_views` as the default path. |

Copy-out APIs remain valid for ownership-transfer cases, materialized
`ddb_result_t` access, and compatibility fallbacks. The performance guidance is
now documented in `include/decentdb.h` next to the statement row-view and
value-copy declarations.
