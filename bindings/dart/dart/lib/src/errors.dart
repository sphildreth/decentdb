import 'types.dart';

/// Exception thrown by DecentDB operations.
class DecentDbException implements Exception {
  /// The error code from the native library.
  final ErrorCode code;

  /// Human-readable error message.
  final String message;

  const DecentDbException(this.code, this.message);

  @override
  String toString() => 'DecentDbException(${code.name}): $message';
}

/// Thrown when the loaded native library has the wrong DecentDB C ABI version.
class DecentDbAbiMismatchException implements Exception {
  const DecentDbAbiMismatchException({
    required this.expectedAbiVersion,
    required this.loadedAbiVersion,
    this.artifact,
  });

  final int expectedAbiVersion;
  final int loadedAbiVersion;
  final String? artifact;

  @override
  String toString() {
    final source = artifact == null ? '' : ' from $artifact';
    return 'DecentDbAbiMismatchException: expected ABI '
        '$expectedAbiVersion, loaded ABI $loadedAbiVersion$source. '
        'Align the decentdb Dart package, decentdb_flutter package, and '
        'packaged native DecentDB artifact versions.';
  }
}

/// Thrown when the DecentDB native library cannot be loaded.
class DecentDbNativeLoadException implements Exception {
  const DecentDbNativeLoadException(this.message, {this.artifact});

  final String message;
  final String? artifact;

  @override
  String toString() {
    final source = artifact == null ? '' : ' ($artifact)';
    return 'DecentDbNativeLoadException$source: $message';
  }
}

/// Warning emitted when the prepared-statement cache hit rate is below
/// acceptable thresholds.
///
/// Surface this via [Database.onPerformanceWarning] to receive diagnostics
/// without disrupting normal execution. Semantics mirror Python's
/// `PerformanceWarning(UserWarning)`: it is delivered to a sink, not thrown.
class PerformanceWarning implements Exception {
  const PerformanceWarning(this.message);

  final String message;

  @override
  String toString() => 'PerformanceWarning: $message';
}
