/// DecentDB Dart FFI bindings.
///
/// Provides both low-level FFI access and a high-level idiomatic Dart API
/// for the DecentDB embedded database engine.
library decentdb;

export 'src/branch.dart';
export 'src/database.dart';
export 'src/statement.dart';
export 'src/errors.dart';
export 'src/open_options.dart';
export 'src/types.dart';
export 'src/schema.dart';
export 'src/schema_snapshot.dart';
export 'src/async_database.dart'
    show AsyncDatabase, AsyncStatement, AsyncDatabaseClosed;
