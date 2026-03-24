import 'dart:io';
import 'dart:math';

import 'package:decentdb/decentdb.dart';
import 'package:sqlite3/sqlite3.dart' as sqlite;

const int _defaultCount = 1000000;
const int _defaultPointReads = 10000;
const int _defaultPointSeed = 1337;
const int _defaultFetchmanyBatch = 4096;

void main(List<String> args) {
  final options = _Options.parse(args);
  if (options.showHelp) {
    _printUsage();
    return;
  }

  final engines = options.engine == 'all'
      ? const <String>['decentdb', 'sqlite']
      : <String>[options.engine];
  final results = <String, _BenchResult>{};

  for (final engine in engines) {
    final suffix = engine == 'sqlite' ? 'db' : 'ddb';
    final dbPath = '${options.dbPrefix}_${engine}.${suffix}';
    if (engine == 'decentdb') {
      results[engine] = _runDecentDbBenchmark(dbPath, options);
    } else {
      results[engine] = _runSqliteBenchmark(dbPath, options);
    }
  }

  _printComparison(results);
}

void _printUsage() {
  print('Fair Dart benchmark: DecentDB binding vs sqlite3 package');
  print('Usage:');
  print('  dart run benchmarks/bench_fetch.dart [options]');
  print('');
  print('Options:');
  print('  --engine <all|decentdb|sqlite>   Engines to run (default: all)');
  print(
      '  --count <n>                      Rows to insert/fetch (default: $_defaultCount)');
  print(
      '  --fetchmany-batch <n>            Batch size for fetchmany metric (default: $_defaultFetchmanyBatch)');
  print(
      '  --point-reads <n>                Random indexed point lookups (default: $_defaultPointReads)');
  print(
      '  --point-seed <n>                 RNG seed for point lookups (default: $_defaultPointSeed)');
  print(
      '  --db-prefix <path_prefix>        Database prefix (default: dart_bench_fetch)');
  print(
      '                                   DecentDB uses .ddb and SQLite uses .db');
  print('  --keep-db                        Keep generated DB files');
  print('  -h, --help                       Show help');
}

_BenchResult _runDecentDbBenchmark(String dbPath, _Options options) {
  _cleanupDbFiles(dbPath);
  print('\n=== decentdb ===');
  print('Setting up data...');

  final db = Database.open(dbPath, libraryPath: _findNativeLibPath());
  try {
    db.execute('CREATE TABLE bench (id INT64, val TEXT, f FLOAT64)');
    db.execute('CREATE INDEX bench_id_idx ON bench(id)');

    final warmStmt = db.prepare(r'INSERT INTO bench VALUES ($1, $2, $3)');
    try {
      db.begin();
      warmStmt.bindAll(<Object?>[-1, '__warm__', -1.0]);
      warmStmt.execute();
      db.rollback();
    } catch (_) {
      db.rollback();
      rethrow;
    } finally {
      warmStmt.dispose();
    }

    final insertStmt = db.prepare(r'INSERT INTO bench VALUES ($1, $2, $3)');
    final insertWatch = Stopwatch()..start();
    db.begin();
    try {
      for (var i = 0; i < options.count; i++) {
        insertStmt.reset();
        insertStmt.clearBindings();
        insertStmt.bindAll(<Object?>[i, 'value_$i', i.toDouble()]);
        insertStmt.execute();
      }
      db.commit();
    } catch (_) {
      db.rollback();
      rethrow;
    } finally {
      insertStmt.dispose();
    }
    insertWatch.stop();
    final insertSeconds = insertWatch.elapsedMicroseconds / 1000000.0;
    final insertRowsPerSecond = options.count / insertSeconds;
    print(
      'Insert ${options.count} rows: ${insertSeconds.toStringAsFixed(4)}s '
      '(${insertRowsPerSecond.toStringAsFixed(2)} rows/sec)',
    );

    db.query('SELECT id, val, f FROM bench LIMIT 1');

    final fetchallWatch = Stopwatch()..start();
    final fetchallRows = db.query('SELECT id, val, f FROM bench');
    fetchallWatch.stop();
    if (fetchallRows.length != options.count) {
      throw StateError(
        'Expected ${options.count} rows from fetchall, got ${fetchallRows.length}',
      );
    }
    final fetchallSeconds = fetchallWatch.elapsedMicroseconds / 1000000.0;
    print(
        'Fetchall ${options.count} rows: ${fetchallSeconds.toStringAsFixed(4)}s');

    final fetchmanyStmt = db.prepare('SELECT id, val, f FROM bench');
    final fetchmanyWatch = Stopwatch()..start();
    var fetchmanyTotal = 0;
    try {
      while (true) {
        final page = fetchmanyStmt.nextPage(options.fetchmanyBatch);
        fetchmanyTotal += page.rows.length;
        if (page.isLast) {
          break;
        }
      }
    } finally {
      fetchmanyStmt.dispose();
    }
    fetchmanyWatch.stop();
    if (fetchmanyTotal != options.count) {
      throw StateError(
        'Expected ${options.count} rows from fetchmany, got $fetchmanyTotal',
      );
    }
    final fetchmanySeconds = fetchmanyWatch.elapsedMicroseconds / 1000000.0;
    print(
      'Fetchmany(${options.fetchmanyBatch}) ${options.count} rows: '
      '${fetchmanySeconds.toStringAsFixed(4)}s',
    );

    final pointStmt = db.prepare(r'SELECT id, val, f FROM bench WHERE id = $1');
    final pointIds = _buildPointReadIds(
        options.count, options.pointReads, options.pointSeed);
    pointStmt.bindAll(<Object?>[pointIds[pointIds.length ~/ 2]]);
    final warmupRows = pointStmt.query();
    if (warmupRows.isEmpty) {
      pointStmt.dispose();
      throw StateError('Warmup point read missed expected row');
    }
    pointStmt.reset();
    pointStmt.clearBindings();

    final pointLatenciesMs = List<double>.filled(pointIds.length, 0.0);
    for (var i = 0; i < pointIds.length; i++) {
      final started = Stopwatch()..start();
      pointStmt.reset();
      pointStmt.clearBindings();
      pointStmt.bindAll(<Object?>[pointIds[i]]);
      final rows = pointStmt.query();
      if (rows.isEmpty) {
        pointStmt.dispose();
        throw StateError('Point read missed id=${pointIds[i]}');
      }
      started.stop();
      pointLatenciesMs[i] = started.elapsedMicroseconds / 1000.0;
    }
    pointStmt.dispose();
    pointLatenciesMs.sort();
    final pointP50Ms = _percentileSorted(pointLatenciesMs, 50);
    final pointP95Ms = _percentileSorted(pointLatenciesMs, 95);
    print(
      'Random point reads by id (${options.pointReads}, seed=${options.pointSeed}): '
      'p50=${pointP50Ms.toStringAsFixed(6)}ms '
      'p95=${pointP95Ms.toStringAsFixed(6)}ms',
    );

    return _BenchResult(
      insertSeconds: insertSeconds,
      insertRowsPerSecond: insertRowsPerSecond,
      fetchallSeconds: fetchallSeconds,
      fetchmanySeconds: fetchmanySeconds,
      pointP50Ms: pointP50Ms,
      pointP95Ms: pointP95Ms,
    );
  } finally {
    db.close();
    if (!options.keepDb) {
      _cleanupDbFiles(dbPath);
    }
  }
}

_BenchResult _runSqliteBenchmark(String dbPath, _Options options) {
  _cleanupDbFiles(dbPath);
  print('\n=== sqlite ===');
  print('Setting up data...');

  final db = sqlite.sqlite3.open(dbPath);
  try {
    db.execute('PRAGMA journal_mode=WAL');
    db.execute('PRAGMA synchronous=FULL');
    db.execute('PRAGMA wal_autocheckpoint=0');
    db.execute('CREATE TABLE bench (id INTEGER, val TEXT, f REAL)');
    db.execute('CREATE INDEX bench_id_idx ON bench(id)');

    db.execute('BEGIN');
    try {
      db.execute('INSERT INTO bench VALUES (?, ?, ?)',
          <Object?>[-1, '__warm__', -1.0]);
      db.execute('ROLLBACK');
    } catch (_) {
      db.execute('ROLLBACK');
      rethrow;
    }

    final insertStmt = db.prepare('INSERT INTO bench VALUES (?, ?, ?)');
    final insertWatch = Stopwatch()..start();
    db.execute('BEGIN');
    try {
      for (var i = 0; i < options.count; i++) {
        insertStmt.execute(<Object?>[i, 'value_$i', i.toDouble()]);
      }
      db.execute('COMMIT');
    } catch (_) {
      db.execute('ROLLBACK');
      rethrow;
    } finally {
      insertStmt.dispose();
    }
    insertWatch.stop();
    final insertSeconds = insertWatch.elapsedMicroseconds / 1000000.0;
    final insertRowsPerSecond = options.count / insertSeconds;
    print(
      'Insert ${options.count} rows: ${insertSeconds.toStringAsFixed(4)}s '
      '(${insertRowsPerSecond.toStringAsFixed(2)} rows/sec)',
    );

    db.select('SELECT id, val, f FROM bench LIMIT 1');

    final fetchallWatch = Stopwatch()..start();
    final fetchallRows = db.select('SELECT id, val, f FROM bench');
    fetchallWatch.stop();
    if (fetchallRows.length != options.count) {
      throw StateError(
        'Expected ${options.count} rows from fetchall, got ${fetchallRows.length}',
      );
    }
    final fetchallSeconds = fetchallWatch.elapsedMicroseconds / 1000000.0;
    print(
        'Fetchall ${options.count} rows: ${fetchallSeconds.toStringAsFixed(4)}s');

    final fetchmanyStmt =
        db.prepare('SELECT id, val, f FROM bench LIMIT ? OFFSET ?');
    final fetchmanyWatch = Stopwatch()..start();
    var fetchmanyTotal = 0;
    var offset = 0;
    try {
      while (true) {
        final page =
            fetchmanyStmt.select(<Object?>[options.fetchmanyBatch, offset]);
        if (page.isEmpty) {
          break;
        }
        fetchmanyTotal += page.length;
        offset += page.length;
        if (page.length < options.fetchmanyBatch) {
          break;
        }
      }
    } finally {
      fetchmanyStmt.dispose();
    }
    fetchmanyWatch.stop();
    if (fetchmanyTotal != options.count) {
      throw StateError(
        'Expected ${options.count} rows from fetchmany, got $fetchmanyTotal',
      );
    }
    final fetchmanySeconds = fetchmanyWatch.elapsedMicroseconds / 1000000.0;
    print(
      'Fetchmany(${options.fetchmanyBatch}) ${options.count} rows: '
      '${fetchmanySeconds.toStringAsFixed(4)}s',
    );

    final pointStmt = db.prepare('SELECT id, val, f FROM bench WHERE id = ?');
    final pointIds = _buildPointReadIds(
        options.count, options.pointReads, options.pointSeed);
    final warmupRows =
        pointStmt.select(<Object?>[pointIds[pointIds.length ~/ 2]]);
    if (warmupRows.isEmpty) {
      pointStmt.dispose();
      throw StateError('Warmup point read missed expected row');
    }

    final pointLatenciesMs = List<double>.filled(pointIds.length, 0.0);
    for (var i = 0; i < pointIds.length; i++) {
      final started = Stopwatch()..start();
      final rows = pointStmt.select(<Object?>[pointIds[i]]);
      if (rows.isEmpty) {
        pointStmt.dispose();
        throw StateError('Point read missed id=${pointIds[i]}');
      }
      started.stop();
      pointLatenciesMs[i] = started.elapsedMicroseconds / 1000.0;
    }
    pointStmt.dispose();

    pointLatenciesMs.sort();
    final pointP50Ms = _percentileSorted(pointLatenciesMs, 50);
    final pointP95Ms = _percentileSorted(pointLatenciesMs, 95);
    print(
      'Random point reads by id (${options.pointReads}, seed=${options.pointSeed}): '
      'p50=${pointP50Ms.toStringAsFixed(6)}ms '
      'p95=${pointP95Ms.toStringAsFixed(6)}ms',
    );

    db.execute('PRAGMA wal_checkpoint(TRUNCATE)');

    return _BenchResult(
      insertSeconds: insertSeconds,
      insertRowsPerSecond: insertRowsPerSecond,
      fetchallSeconds: fetchallSeconds,
      fetchmanySeconds: fetchmanySeconds,
      pointP50Ms: pointP50Ms,
      pointP95Ms: pointP95Ms,
    );
  } finally {
    db.dispose();
    if (!options.keepDb) {
      _cleanupDbFiles(dbPath);
    }
  }
}

void _printComparison(Map<String, _BenchResult> results) {
  final decent = results['decentdb'];
  final sqlite = results['sqlite'];
  if (decent == null || sqlite == null) {
    return;
  }

  final metrics = <_Metric>[
    _Metric(
      name: 'Insert throughput (higher is better)',
      decent: decent.insertRowsPerSecond,
      sqlite: sqlite.insertRowsPerSecond,
      unit: ' rows/s',
      higherIsBetter: true,
      formatter: (value) => value.toStringAsFixed(2),
    ),
    _Metric(
      name: 'Fetchall time (lower is better)',
      decent: decent.fetchallSeconds,
      sqlite: sqlite.fetchallSeconds,
      unit: 's',
      higherIsBetter: false,
      formatter: (value) => value.toStringAsFixed(6),
    ),
    _Metric(
      name: 'Fetchmany/streaming time (lower is better)',
      decent: decent.fetchmanySeconds,
      sqlite: sqlite.fetchmanySeconds,
      unit: 's',
      higherIsBetter: false,
      formatter: (value) => value.toStringAsFixed(6),
    ),
    _Metric(
      name: 'Point read p50 latency (lower is better)',
      decent: decent.pointP50Ms,
      sqlite: sqlite.pointP50Ms,
      unit: 'ms',
      higherIsBetter: false,
      formatter: (value) => value.toStringAsFixed(6),
    ),
    _Metric(
      name: 'Point read p95 latency (lower is better)',
      decent: decent.pointP95Ms,
      sqlite: sqlite.pointP95Ms,
      unit: 'ms',
      higherIsBetter: false,
      formatter: (value) => value.toStringAsFixed(6),
    ),
  ];

  final decentBetter = <String>[];
  final sqliteBetter = <String>[];
  final ties = <String>[];

  for (final metric in metrics) {
    if (metric.decent == metric.sqlite) {
      ties.add(
          '${metric.name}: tie (${metric.formatter(metric.decent)}${metric.unit})');
      continue;
    }

    final bool decentWins;
    final double winner;
    final double loser;
    final double ratio;
    final String detail;

    if (metric.higherIsBetter) {
      decentWins = metric.decent > metric.sqlite;
      winner = decentWins ? metric.decent : metric.sqlite;
      loser = decentWins ? metric.sqlite : metric.decent;
      ratio = loser == 0 ? double.infinity : winner / loser;
      detail = '${metric.name}: ${metric.formatter(winner)}${metric.unit} vs '
          '${metric.formatter(loser)}${metric.unit} '
          '(${ratio.toStringAsFixed(3)}x higher)';
    } else {
      decentWins = metric.decent < metric.sqlite;
      winner = decentWins ? metric.decent : metric.sqlite;
      loser = decentWins ? metric.sqlite : metric.decent;
      ratio = winner == 0 ? double.infinity : loser / winner;
      detail = '${metric.name}: ${metric.formatter(winner)}${metric.unit} vs '
          '${metric.formatter(loser)}${metric.unit} '
          '(${ratio.toStringAsFixed(3)}x faster/lower)';
    }

    if (decentWins) {
      decentBetter.add(detail);
    } else {
      sqliteBetter.add(detail);
    }
  }

  print('\n=== Comparison (DecentDB vs SQLite) ===');
  print('DecentDB better at:');
  if (decentBetter.isEmpty) {
    print('- none');
  } else {
    for (final line in decentBetter) {
      print('- $line');
    }
  }

  print('SQLite better at:');
  if (sqliteBetter.isEmpty) {
    print('- none');
  } else {
    for (final line in sqliteBetter) {
      print('- $line');
    }
  }

  if (ties.isNotEmpty) {
    print('Ties:');
    for (final line in ties) {
      print('- $line');
    }
  }
}

List<int> _buildPointReadIds(int rowCount, int pointReads, int seed) {
  final random = Random(seed);
  if (pointReads <= rowCount) {
    final ids = List<int>.generate(rowCount, (index) => index);
    for (var i = 0; i < pointReads; i++) {
      final j = i + random.nextInt(rowCount - i);
      final tmp = ids[i];
      ids[i] = ids[j];
      ids[j] = tmp;
    }
    return ids.sublist(0, pointReads);
  }

  return List<int>.generate(pointReads, (_) => random.nextInt(rowCount));
}

double _percentileSorted(List<double> sortedValues, int pct) {
  if (sortedValues.isEmpty) {
    return 0;
  }
  final idx = ((pct / 100.0) * (sortedValues.length - 1)).round();
  return sortedValues[idx.clamp(0, sortedValues.length - 1)];
}

void _cleanupDbFiles(String basePath) {
  _deleteQuietly(basePath);
  _deleteQuietly('$basePath.wal');
  _deleteQuietly('$basePath-wal');
  _deleteQuietly('$basePath-shm');
}

void _deleteQuietly(String path) {
  final file = File(path);
  if (file.existsSync()) {
    file.deleteSync();
  }
}

String _findNativeLibPath() {
  final envPath = Platform.environment['DECENTDB_NATIVE_LIB'];
  if (envPath != null && envPath.isNotEmpty) {
    return envPath;
  }

  final candidates = <String>[
    'target/release/libdecentdb.so',
    'target/release/libdecentdb.dylib',
    'target/release/decentdb.dll',
    'target/debug/libdecentdb.so',
    'target/debug/libdecentdb.dylib',
    'target/debug/decentdb.dll',
  ];

  var dir = Directory.current;
  for (var i = 0; i < 10; i++) {
    for (final candidate in candidates) {
      final path = '${dir.path}/$candidate';
      if (File(path).existsSync()) {
        return path;
      }
    }
    dir = dir.parent;
  }

  throw StateError(
    'Cannot find DecentDB native library. '
    'Set DECENTDB_NATIVE_LIB or run after cargo build -p decentdb --release.',
  );
}

class _BenchResult {
  _BenchResult({
    required this.insertSeconds,
    required this.insertRowsPerSecond,
    required this.fetchallSeconds,
    required this.fetchmanySeconds,
    required this.pointP50Ms,
    required this.pointP95Ms,
  });

  final double insertSeconds;
  final double insertRowsPerSecond;
  final double fetchallSeconds;
  final double fetchmanySeconds;
  final double pointP50Ms;
  final double pointP95Ms;
}

class _Metric {
  _Metric({
    required this.name,
    required this.decent,
    required this.sqlite,
    required this.unit,
    required this.higherIsBetter,
    required this.formatter,
  });

  final String name;
  final double decent;
  final double sqlite;
  final String unit;
  final bool higherIsBetter;
  final String Function(double value) formatter;
}

class _Options {
  _Options({
    required this.engine,
    required this.count,
    required this.pointReads,
    required this.pointSeed,
    required this.fetchmanyBatch,
    required this.dbPrefix,
    required this.keepDb,
    required this.showHelp,
  });

  final String engine;
  final int count;
  final int pointReads;
  final int pointSeed;
  final int fetchmanyBatch;
  final String dbPrefix;
  final bool keepDb;
  final bool showHelp;

  static _Options parse(List<String> args) {
    var engine = 'all';
    var count = _defaultCount;
    var pointReads = _defaultPointReads;
    var pointSeed = _defaultPointSeed;
    var fetchmanyBatch = _defaultFetchmanyBatch;
    var dbPrefix = 'dart_bench_fetch';
    var keepDb = false;
    var showHelp = false;

    for (var i = 0; i < args.length; i++) {
      final arg = args[i];
      switch (arg) {
        case '--help':
        case '-h':
          showHelp = true;
          break;
        case '--engine':
          engine = _nextArg(args, ++i, '--engine');
          break;
        case '--count':
          count = _parsePositiveInt(_nextArg(args, ++i, '--count'), '--count');
          break;
        case '--point-reads':
          pointReads = _parsePositiveInt(
            _nextArg(args, ++i, '--point-reads'),
            '--point-reads',
          );
          break;
        case '--point-seed':
          pointSeed = int.parse(_nextArg(args, ++i, '--point-seed'));
          break;
        case '--fetchmany-batch':
          fetchmanyBatch = _parsePositiveInt(
            _nextArg(args, ++i, '--fetchmany-batch'),
            '--fetchmany-batch',
          );
          break;
        case '--db-prefix':
          dbPrefix = _nextArg(args, ++i, '--db-prefix');
          break;
        case '--keep-db':
          keepDb = true;
          break;
        default:
          throw ArgumentError('Unknown argument: $arg');
      }
    }

    if (!<String>{'all', 'decentdb', 'sqlite'}.contains(engine)) {
      throw ArgumentError('--engine must be one of: all, decentdb, sqlite');
    }
    if (dbPrefix.isEmpty) {
      throw ArgumentError('--db-prefix cannot be empty');
    }

    return _Options(
      engine: engine,
      count: count,
      pointReads: pointReads,
      pointSeed: pointSeed,
      fetchmanyBatch: fetchmanyBatch,
      dbPrefix: dbPrefix,
      keepDb: keepDb,
      showHelp: showHelp,
    );
  }

  static String _nextArg(List<String> args, int index, String name) {
    if (index >= args.length) {
      throw ArgumentError('$name requires a value');
    }
    return args[index];
  }

  static int _parsePositiveInt(String value, String name) {
    final parsed = int.parse(value);
    if (parsed <= 0) {
      throw ArgumentError('$name must be > 0');
    }
    return parsed;
  }
}
