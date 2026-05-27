import 'dart:io';
import 'dart:typed_data';

import 'package:decentdb/decentdb.dart';
import 'package:path/path.dart' as p;
import 'package:path_provider/path_provider.dart';

/// Supplies database encryption key bytes from an app-owned secure store.
///
/// Implementations typically wrap Keychain on iOS and Android Keystore-backed
/// storage on Android. DecentDB does not persist or rotate keys for the app.
abstract interface class DecentDbKeyProvider {
  /// Return raw key bytes for the database being opened.
  Future<Uint8List> loadDatabaseKey();
}

/// Redacted mobile support diagnostics.
final class DecentDbMobileDiagnostics {
  const DecentDbMobileDiagnostics({
    required this.platform,
    required this.supportTier,
    required this.packageName,
    required this.watchApisAvailable,
    this.engineVersion,
    this.databasePath,
    this.databaseSet,
    this.openOptionsSummary,
  });

  final String platform;
  final String supportTier;
  final String packageName;
  final bool watchApisAvailable;
  final String? engineVersion;
  final String? databasePath;
  final List<String>? databaseSet;
  final String? openOptionsSummary;

  Map<String, Object?> toJson() {
    return <String, Object?>{
      'platform': platform,
      'support_tier': supportTier,
      'package': packageName,
      'watch_apis_available': watchApisAvailable,
      if (engineVersion != null) 'engine_version': engineVersion,
      if (databasePath != null) 'database_path': databasePath,
      if (databaseSet != null) 'database_set': databaseSet,
      if (openOptionsSummary != null) 'open_options': openOptionsSummary,
    };
  }
}

/// Static key provider intended for tests and local examples.
final class StaticDecentDbKeyProvider implements DecentDbKeyProvider {
  StaticDecentDbKeyProvider(Uint8List key) : _key = Uint8List.fromList(key);

  final Uint8List _key;

  @override
  Future<Uint8List> loadDatabaseKey() async => Uint8List.fromList(_key);
}

/// Flutter-first mobile integration helpers.
final class DecentDbMobile {
  const DecentDbMobile._();

  /// Current mobile package name.
  static const packageName = 'decentdb_flutter';

  /// Reactive watch wrappers are intentionally deferred in the first mobile SDK.
  static const watchApisAvailable = false;

  /// Resolve [filename] under Flutter's application-support directory.
  static Future<String> appDatabasePath(String filename) async {
    _assertSafeFilename(filename);
    final directory = await getApplicationSupportDirectory();
    return p.join(directory.path, filename);
  }

  /// Resolve [filename] under a device-local no-backup subdirectory.
  ///
  /// The helper creates a stable app-private subdirectory. Platform-specific
  /// backup-exclusion flags are still the host app's responsibility.
  static Future<String> noBackupDatabasePath(String filename) async {
    _assertSafeFilename(filename);
    final directory = await getApplicationSupportDirectory();
    final noBackup = Directory(p.join(directory.path, 'decentdb-no-backup'));
    await noBackup.create(recursive: true);
    return p.join(noBackup.path, filename);
  }

  /// Return the v1 authoritative database set for backup, restore, and delete.
  static List<String> databaseSetPaths(
    String databasePath, {
    bool includeCoordinationSidecar = true,
  }) {
    return <String>[
      databasePath,
      '$databasePath.wal',
      '$databasePath.sync-journal',
      if (includeCoordinationSidecar) '$databasePath.coord',
    ];
  }

  /// Open [filename] in the application-support directory.
  static Future<Database> openAppDatabase(
    String filename, {
    String? options,
    DecentDbKeyProvider? keyProvider,
    ProcessCoordinationMode? processCoordination,
    int? processCoordinationTimeoutMs,
  }) async {
    final path = await appDatabasePath(filename);
    return openPath(
      path,
      options: options,
      keyProvider: keyProvider,
      processCoordination: processCoordination,
      processCoordinationTimeoutMs: processCoordinationTimeoutMs,
    );
  }

  /// Open an explicit [databasePath].
  static Future<Database> openPath(
    String databasePath, {
    String? options,
    DecentDbKeyProvider? keyProvider,
    ProcessCoordinationMode? processCoordination,
    int? processCoordinationTimeoutMs,
  }) async {
    final mergedOptions = await buildOpenOptions(
      options: options,
      keyProvider: keyProvider,
    );
    return Database.open(
      databasePath,
      options: mergedOptions,
      processCoordination: processCoordination,
      processCoordinationTimeoutMs: processCoordinationTimeoutMs,
    );
  }

  /// Open [databasePath] asynchronously on a worker isolate.
  static Future<AsyncDatabase> openAsyncPath(
    String databasePath, {
    String? options,
    DecentDbKeyProvider? keyProvider,
    ProcessCoordinationMode? processCoordination,
    int? processCoordinationTimeoutMs,
  }) async {
    final mergedOptions = await buildOpenOptions(
      options: options,
      keyProvider: keyProvider,
    );
    return AsyncDatabase.open(
      databasePath,
      options: mergedOptions,
      processCoordination: processCoordination,
      processCoordinationTimeoutMs: processCoordinationTimeoutMs,
    );
  }

  /// Build native open options, adding `encryption_key_hex` from [keyProvider].
  ///
  /// The returned string is suitable for `Database.open(options: ...)`. Use
  /// [redactSensitiveOpenOptions] before logging or including it in diagnostics.
  static Future<String?> buildOpenOptions({
    String? options,
    DecentDbKeyProvider? keyProvider,
  }) async {
    final parts = <String>[];
    if (options != null && options.trim().isNotEmpty) {
      parts.add(options.trim());
    }
    if (keyProvider == null) {
      return parts.isEmpty ? null : parts.join(';');
    }

    final sanitized = sanitizeOpenOptions(options);
    if (sanitized.hasRedactions) {
      throw ArgumentError(
        'Pass encryption key material either through keyProvider or options, '
        'not both.',
      );
    }

    final key = await keyProvider.loadDatabaseKey();
    if (key.isEmpty) {
      throw ArgumentError.value(key.length, 'keyProvider', 'key is empty');
    }
    try {
      parts.add('encryption_key_hex=${_hexEncode(key)}');
      return parts.join(';');
    } finally {
      key.fillRange(0, key.length, 0);
    }
  }

  /// Return a redacted open-options summary for logs or support bundles.
  static String openOptionsSummary(String? options) {
    return redactSensitiveOpenOptions(options);
  }

  /// Build redacted mobile support diagnostics.
  static DecentDbMobileDiagnostics diagnostics({
    Database? database,
    String? databasePath,
    String? options,
  }) {
    return DecentDbMobileDiagnostics(
      platform: Platform.operatingSystem,
      supportTier: _supportTier(),
      packageName: packageName,
      watchApisAvailable: watchApisAvailable,
      engineVersion: database?.engineVersion,
      databasePath: databasePath,
      databaseSet: databasePath == null ? null : databaseSetPaths(databasePath),
      openOptionsSummary:
          options == null ? null : redactSensitiveOpenOptions(options),
    );
  }

  static void _assertSafeFilename(String filename) {
    if (filename.isEmpty || filename != p.basename(filename)) {
      throw ArgumentError.value(
        filename,
        'filename',
        'must be a file name, not a path',
      );
    }
  }

  static String _hexEncode(Uint8List bytes) {
    const digits = '0123456789abcdef';
    final buffer = StringBuffer();
    for (final byte in bytes) {
      buffer
        ..write(digits[(byte >> 4) & 0x0f])
        ..write(digits[byte & 0x0f]);
    }
    return buffer.toString();
  }

  static String _supportTier() {
    if (Platform.isAndroid || Platform.isIOS) {
      return 'Tier 2';
    }
    return 'Unsupported';
  }
}
