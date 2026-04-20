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
