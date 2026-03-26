import 'dart:convert';
import 'dart:io';

void main(List<String> args) {
  final dir = args.isNotEmpty ? args[0] : '.';
  final directory = Directory(dir);
  final files = directory
      .listSync()
      .whereType<File>()
      .where((f) => f.path.endsWith('.json'))
      .where((f) => !f.path.contains('pubspec'))
      .toList();

  if (files.isEmpty) {
    print('No .json result files found in ${directory.absolute.path}');
    return;
  }

  final results = <_RunResult>[];
  for (final file in files) {
    try {
      final raw = json.decode(file.readAsStringSync()) as Map<String, dynamic>;
      results.add(_RunResult.fromMap(file.path, raw));
    } catch (e) {
      stderr.writeln('Skipping ${file.path}: $e');
    }
  }

  if (results.isEmpty) {
    print('No valid result files found.');
    return;
  }

  results.sort((a, b) => a.timestamp.compareTo(b.timestamp));

  if (results.length < 2) {
    _printSingleRun(results.first);
    return;
  }

  _printComparison(results);
}

void _printSingleRun(_RunResult run) {
  final w = _Writer();
  w.header('Single Run: ${run.filename}');
  w.meta('Timestamp', run.timestamp.toIso8601String());
  w.meta('Engine', run.engineVersion);
  w.meta('Scale', '${run.scaleFactor}x (${run.totalRows} rows)');
  w.blank();
  w.divider();
  for (final entry in run.metrics.entries) {
    w.metric(entry.key, _fmt(entry.value));
  }
  w.divider();
  w.flush();
}

void _printComparison(List<_RunResult> runs) {
  final w = _Writer();
  final prev = runs[runs.length - 2];
  final curr = runs.last;

  w.header('DecentDB Performance Comparison');
  w.blank();
  w.meta('Baseline', '${prev.filename}  (${prev.timestamp.toIso8601String()})');
  w.meta('Current', ' ${curr.filename}  (${curr.timestamp.toIso8601String()})');
  if (prev.engineVersion != curr.engineVersion) {
    w.meta('Engine', '${prev.engineVersion}  ->  ${curr.engineVersion}');
  }
  w.blank();

  final allKeys = <String>{...prev.metrics.keys, ...curr.metrics.keys}.toList()
    ..sort();

  final wins = <_Delta>[];
  final losses = <_Delta>[];
  final unchanged = <_Delta>[];
  final newMetrics = <_Delta>[];

  for (final key in allKeys) {
    final oldVal = prev.metrics[key];
    final newVal = curr.metrics[key];
    if (oldVal == null || newVal == null) {
      newMetrics.add(_Delta(key, oldVal, newVal, 0, _DeltaType.newMetric));
      continue;
    }
    if (oldVal == 0) {
      unchanged.add(_Delta(key, oldVal, newVal, 0, _DeltaType.unchanged));
      continue;
    }
    final pctChange = ((newVal - oldVal) / oldVal) * 100.0;
    final isLowerBetter = _isLowerBetter(key);
    final isWin = isLowerBetter ? pctChange < -1.0 : pctChange > 1.0;
    final isLoss = isLowerBetter ? pctChange > 1.0 : pctChange < -1.0;

    if (pctChange.abs() < 1.0) {
      unchanged
          .add(_Delta(key, oldVal, newVal, pctChange, _DeltaType.unchanged));
    } else if (isWin) {
      wins.add(_Delta(key, oldVal, newVal, pctChange, _DeltaType.win));
    } else if (isLoss) {
      losses.add(_Delta(key, oldVal, newVal, pctChange, _DeltaType.loss));
    } else {
      unchanged
          .add(_Delta(key, oldVal, newVal, pctChange, _DeltaType.unchanged));
    }
  }

  wins.sort((a, b) => b.pctChange.abs().compareTo(a.pctChange.abs()));
  losses.sort((a, b) => b.pctChange.abs().compareTo(a.pctChange.abs()));

  // Summary bar
  final total = wins.length + losses.length + unchanged.length;
  final barWidth = 50;
  final winBar = total > 0 ? (wins.length / total * barWidth).round() : 0;
  final lossBar = total > 0 ? (losses.length / total * barWidth).round() : 0;
  final flatBar = barWidth - winBar - lossBar;

  w.line(
      '  ${_green}${_bold}+${wins.length} WIN${wins.length == 1 ? '' : 'S'}${_reset}'
      '  ${_red}${_bold}-${losses.length} LOSS${losses.length == 1 ? 'E' : 'S'}${_reset}'
      '  ${_dim}~${unchanged.length} FLAT${_reset}'
      '${newMetrics.isNotEmpty ? '  ${_yellow}?${newMetrics.length} NEW${_reset}' : ''}');

  w.line(
      '  ${_green}${'\u2588' * winBar}${_red}${'\u2588' * lossBar}${_dim}${'\u2588' * flatBar}${_reset}');
  w.blank();

  if (wins.isNotEmpty) {
    w.section('${_green}${_bold}▲ IMPROVED${_reset}');
    w.line(
        '  ${_dim}${'Metric'.padRight(32)} ${'Before'.padLeft(12)} ${'After'.padLeft(12)}  ${'Change'.padLeft(10)}${_reset}');
    w.line('  ${_dim}${'\u2500' * 70}${_reset}');
    for (final d in wins) {
      final arrow = _green;
      w.line(
          '  ${d.key.padRight(32)} ${_fmt(d.oldVal!).padLeft(12)} ${_fmt(d.newVal!).padLeft(12)}  '
          '${arrow}${d.pctChange >= 0 ? '+' : ''}${d.pctChange.toStringAsFixed(1)}%${_reset}');
    }
    w.blank();
  }

  if (losses.isNotEmpty) {
    w.section('${_red}${_bold}▼ REGRESSED${_reset}');
    w.line(
        '  ${_dim}${'Metric'.padRight(32)} ${'Before'.padLeft(12)} ${'After'.padLeft(12)}  ${'Change'.padLeft(10)}${_reset}');
    w.line('  ${_dim}${'\u2500' * 70}${_reset}');
    for (final d in losses) {
      final arrow = _red;
      w.line(
          '  ${d.key.padRight(32)} ${_fmt(d.oldVal!).padLeft(12)} ${_fmt(d.newVal!).padLeft(12)}  '
          '${arrow}+${d.pctChange.toStringAsFixed(1)}%${_reset}');
    }
    w.blank();
  }

  if (unchanged.isNotEmpty) {
    w.section('${_dim}─ STABLE${_reset}');
    w.line(
        '  ${_dim}${'Metric'.padRight(32)} ${'Before'.padLeft(12)} ${'After'.padLeft(12)}  ${'Change'.padLeft(10)}${_reset}');
    w.line('  ${_dim}${'\u2500' * 70}${_reset}');
    for (final d in unchanged) {
      final sign = d.pctChange >= 0 ? '+' : '';
      w.line(
          '  ${_dim}${d.key.padRight(32)} ${_fmt(d.oldVal!).padLeft(12)} ${_fmt(d.newVal!).padLeft(12)}  '
          '$sign${d.pctChange.toStringAsFixed(1)}%${_reset}');
    }
    w.blank();
  }

  if (newMetrics.isNotEmpty) {
    w.section('${_yellow}? NEW METRICS${_reset}');
    for (final d in newMetrics) {
      w.line(
          '  ${_dim}${d.key.padRight(32)} ${'-'.padLeft(12)} ${_fmt(d.newVal!).padLeft(12)}${_reset}');
    }
    w.blank();
  }

  // Full history table if > 2 runs
  if (runs.length > 2) {
    w.divider();
    w.section('Full History (${runs.length} runs)');
    final sampleKeys = [
      'insert_total_rps',
      'point_read_p50_ms',
      'point_read_p95_ms',
      'fetchall_ms',
      'join_4table_ms',
      'agg_customer_spend_ms',
      'text_search_contains_ms',
      'txn_commit_ms',
    ];
    final headerCols = ['Run', ...sampleKeys.map(_shortLabel)];
    final colWidth = 14;

    w.line(
        '  ${_dim}${headerCols.map((c) => c.padLeft(colWidth)).join(' ')}${_reset}');
    w.line(
        '  ${_dim}${'\u2500' * (colWidth * headerCols.length + headerCols.length)}${_reset}');

    for (var i = 0; i < runs.length; i++) {
      final cols = <String>['#${i + 1}'];
      for (final key in sampleKeys) {
        final val = runs[i].metrics[key];
        cols.add(val != null ? _fmt(val) : '-');
      }
      final isLast = i == runs.length - 1;
      final prefix = isLast ? '$_bold> ' : '  ';
      final suffix = isLast ? _reset : '';
      w.line('$prefix${cols.map((c) => c.padLeft(colWidth)).join(' ')}$suffix');
    }
    w.blank();
  }

  w.divider();
  w.flush();
}

String _shortLabel(String key) {
  const map = {
    'insert_total_rps': 'Insert rps',
    'point_read_p50_ms': 'PtRead p50',
    'point_read_p95_ms': 'PtRead p95',
    'fetchall_ms': 'Fetchall',
    'join_4table_ms': '4tbl Join',
    'agg_customer_spend_ms': 'Cust Spend',
    'text_search_contains_ms': 'LIKE search',
    'txn_commit_ms': 'Txn commit',
  };
  return map[key] ?? key;
}

bool _isLowerBetter(String key) {
  if (key.contains('_rps')) return false;
  if (key.contains('_rows')) return false;
  if (key.contains('_matches')) return false;
  if (key.contains('_count')) return false;
  return true;
}

String _fmt(num value) {
  if (value == value.roundToDouble() && value.abs() < 1e9) {
    final s = value.round().toString();
    final buf = StringBuffer();
    var count = 0;
    for (var i = s.length - 1; i >= 0; i--) {
      if (count == 3 && s[i] != '-') {
        buf.write(',');
        count = 0;
      }
      buf.write(s[i]);
      count++;
    }
    return buf.toString().split('').reversed.join();
  }
  if (value.abs() >= 1000) return value.toStringAsFixed(0);
  if (value.abs() >= 1) return value.toStringAsFixed(2);
  return value.toStringAsFixed(4);
}

const _reset = '\x1b[0m';
const _bold = '\x1b[1m';
const _dim = '\x1b[2m';
const _green = '\x1b[32m';
const _red = '\x1b[31m';
const _yellow = '\x1b[33m';
const _cyan = '\x1b[36m';

enum _DeltaType { win, loss, unchanged, newMetric }

class _Delta {
  final String key;
  final num? oldVal;
  final num? newVal;
  final double pctChange;
  final _DeltaType type;
  _Delta(this.key, this.oldVal, this.newVal, this.pctChange, this.type);
}

class _RunResult {
  final String filename;
  final DateTime timestamp;
  final String engineVersion;
  final int scaleFactor;
  final int totalRows;
  final Map<String, num> metrics;

  _RunResult({
    required this.filename,
    required this.timestamp,
    required this.engineVersion,
    required this.scaleFactor,
    required this.totalRows,
    required this.metrics,
  });

  factory _RunResult.fromMap(String path, Map<String, dynamic> map) {
    final metricsRaw = map['metrics'] as Map<String, dynamic>? ?? {};
    final metrics = <String, num>{};
    for (final entry in metricsRaw.entries) {
      if (entry.value is num) {
        metrics[entry.key] = entry.value as num;
      }
    }
    return _RunResult(
      filename: path.split(Platform.pathSeparator).last,
      timestamp: DateTime.parse(map['timestamp'] as String),
      engineVersion: map['engine_version'] as String? ?? 'unknown',
      scaleFactor: map['scale_factor'] as int? ?? 1,
      totalRows: map['total_target_rows'] as int? ?? 0,
      metrics: metrics,
    );
  }
}

class _Writer {
  final _lines = <String>[];

  void line(String s) => _lines.add(s);
  void blank() => _lines.add('');
  void divider() => _lines.add('  ${_dim}${'\u2500' * 70}$_reset');
  void header(String text) => _lines.add('  ${_cyan}${_bold}$text$_reset');
  void section(String text) => _lines.add('  $text');
  void meta(String label, String value) =>
      _lines.add('  ${_dim}$label:${_reset} $value');

  void metric(String key, String value) =>
      _lines.add('  ${key.padRight(40)} $value');

  void flush() {
    for (final l in _lines) {
      print(l);
    }
  }
}
