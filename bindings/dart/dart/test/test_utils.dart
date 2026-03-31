import 'dart:io';

/// Returns the path to the DecentDB native library for use in tests.
///
/// Resolution order:
/// 1. `DECENTDB_NATIVE_LIB` environment variable (explicit override).
/// 2. Walk up from [Directory.current] looking for `target/debug/` build
///    outputs (supports running tests from any subdirectory of the repo).
///
/// Throws [StateError] if the library cannot be located.
String findNativeLib() {
  final envPath = Platform.environment['DECENTDB_NATIVE_LIB'];
  if (envPath != null && envPath.isNotEmpty) {
    return envPath;
  }

  var dir = Directory.current;
  for (var i = 0; i < 10; i++) {
    for (final candidateName in [
      'target/debug/libdecentdb.so',
      'target/debug/libdecentdb.dylib',
      'target/debug/decentdb.dll',
    ]) {
      final candidate = File('${dir.path}/$candidateName');
      if (candidate.existsSync()) {
        return candidate.path;
      }
    }
    dir = dir.parent;
  }

  throw StateError(
    'Cannot find DecentDB native library. '
    'Set DECENTDB_NATIVE_LIB or run from the repo root after `cargo build -p decentdb`.',
  );
}
