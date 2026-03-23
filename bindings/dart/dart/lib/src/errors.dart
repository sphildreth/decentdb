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
