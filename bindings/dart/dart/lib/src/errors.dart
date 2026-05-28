import 'types.dart';

/// Structured diagnostic payload returned by the native error contract.
class DecentDbDiagnostic {
  const DecentDbDiagnostic(this.raw, {this.rawJson});

  final Map<String, Object?> raw;
  final String? rawJson;

  String? get codeName => raw['code_name'] as String?;
  String? get subcode => raw['subcode'] as String?;
  String? get sqlstate => raw['sqlstate'] as String?;
  bool? get retryable => raw['retryable'] as bool?;
  bool? get permanent => raw['permanent'] as bool?;
}

/// Exception thrown by DecentDB operations.
class DecentDbException implements Exception {
  /// The error code from the native library.
  final ErrorCode code;

  /// Human-readable error message.
  final String message;

  /// Optional structured diagnostic payload.
  final DecentDbDiagnostic? diagnostic;

  const DecentDbException(this.code, this.message, {this.diagnostic});

  String? get subcode => diagnostic?.subcode;
  String? get sqlstate => diagnostic?.sqlstate;
  bool? get retryable => diagnostic?.retryable;
  bool? get permanent => diagnostic?.permanent;

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
