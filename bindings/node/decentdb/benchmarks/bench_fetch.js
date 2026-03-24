'use strict';

const fs = require('node:fs');
const process = require('node:process');
const { parseArgs } = require('node:util');
const { DatabaseSync } = require('node:sqlite');
const { Database } = require('..');

const DEFAULT_COUNT = 1_000_000;
const DEFAULT_POINT_READS = 10_000;
const DEFAULT_POINT_SEED = 1337;
const DEFAULT_FETCHMANY_BATCH = 4096;

function parseCli() {
  const { values } = parseArgs({
    options: {
      engine: { type: 'string', default: 'all' },
      count: { type: 'string', default: String(DEFAULT_COUNT) },
      'point-reads': { type: 'string', default: String(DEFAULT_POINT_READS) },
      'point-seed': { type: 'string', default: String(DEFAULT_POINT_SEED) },
      'fetchmany-batch': { type: 'string', default: String(DEFAULT_FETCHMANY_BATCH) },
      'db-prefix': { type: 'string', default: 'node_native_bench_fetch' },
      'keep-db': { type: 'boolean', default: false },
      help: { type: 'boolean', short: 'h', default: false },
    },
    allowPositionals: false,
  });

  if (values.help) {
    printUsage();
    process.exit(0);
  }

  const engine = values.engine;
  const count = parsePositiveInt(values.count, '--count');
  const pointReads = parsePositiveInt(values['point-reads'], '--point-reads');
  const pointSeed = parseInt(values['point-seed'], 10);
  if (!Number.isInteger(pointSeed)) {
    throw new Error('--point-seed must be an integer');
  }
  const fetchmanyBatch = parsePositiveInt(values['fetchmany-batch'], '--fetchmany-batch');
  const dbPrefix = values['db-prefix'];
  if (!dbPrefix) {
    throw new Error('--db-prefix cannot be empty');
  }

  if (!['all', 'decentdb', 'sqlite'].includes(engine)) {
    throw new Error('--engine must be one of: all, decentdb, sqlite');
  }

  return {
    engine,
    count,
    pointReads,
    pointSeed,
    fetchmanyBatch,
    dbPrefix,
    keepDb: values['keep-db'] === true,
  };
}

function printUsage() {
  console.log('Fair Node benchmark: DecentDB native addon vs node:sqlite');
  console.log('Usage:');
  console.log('  node benchmarks/bench_fetch.js [options]');
  console.log('');
  console.log('Options:');
  console.log('  --engine <all|decentdb|sqlite>   Engines to run (default: all)');
  console.log(`  --count <n>                      Rows to insert/fetch (default: ${DEFAULT_COUNT})`);
  console.log(`  --fetchmany-batch <n>            Batch size for fetchmany metric (default: ${DEFAULT_FETCHMANY_BATCH})`);
  console.log(`  --point-reads <n>                Random indexed point lookups (default: ${DEFAULT_POINT_READS})`);
  console.log(`  --point-seed <n>                 RNG seed for point lookups (default: ${DEFAULT_POINT_SEED})`);
  console.log('  --db-prefix <path_prefix>        Database prefix (default: node_native_bench_fetch)');
  console.log('                                   DecentDB uses .ddb and SQLite uses .db');
  console.log('  --keep-db                        Keep generated DB files');
  console.log('  -h, --help                       Show help');
}

function parsePositiveInt(raw, name) {
  const parsed = Number.parseInt(raw, 10);
  if (!Number.isInteger(parsed) || parsed <= 0) {
    throw new Error(`${name} must be an integer > 0`);
  }
  return parsed;
}

function removeIfExists(path) {
  try {
    fs.unlinkSync(path);
  } catch (error) {
    if (error && error.code !== 'ENOENT') {
      throw error;
    }
  }
}

function cleanupDbFiles(basePath) {
  removeIfExists(basePath);
  removeIfExists(basePath + '.wal');
  removeIfExists(basePath + '-wal');
  removeIfExists(basePath + '-shm');
}

function runWithGcDisabled(fn) {
  const hadGc = typeof global.gc === 'function';
  if (hadGc) {
    global.gc();
  }
  return fn();
}

function percentileSorted(sortedValues, pct) {
  if (sortedValues.length === 0) {
    return 0;
  }
  const idx = Math.round((pct / 100) * (sortedValues.length - 1));
  return sortedValues[Math.max(0, Math.min(sortedValues.length - 1, idx))];
}

function buildPointReadIds(rowCount, pointReads, seed) {
  const rng = mulberry32(seed >>> 0);
  if (pointReads <= rowCount) {
    const ids = new Array(rowCount);
    for (let i = 0; i < rowCount; i++) {
      ids[i] = i;
    }
    for (let i = 0; i < pointReads; i++) {
      const j = i + Math.floor(rng() * (rowCount - i));
      const tmp = ids[i];
      ids[i] = ids[j];
      ids[j] = tmp;
    }
    return ids.slice(0, pointReads);
  }

  const out = new Array(pointReads);
  for (let i = 0; i < pointReads; i++) {
    out[i] = Math.floor(rng() * rowCount);
  }
  return out;
}

function mulberry32(seed) {
  let t = seed >>> 0;
  return function next() {
    t += 0x6D2B79F5;
    let r = Math.imul(t ^ (t >>> 15), 1 | t);
    r ^= r + Math.imul(r ^ (r >>> 7), 61 | r);
    return ((r ^ (r >>> 14)) >>> 0) / 4294967296;
  };
}

function runDecentDbBenchmark(dbPath, opts) {
  cleanupDbFiles(dbPath);
  console.log('\n=== decentdb ===');
  console.log('Setting up data...');
  const db = new Database({ path: dbPath });
  try {
    db.exec('CREATE TABLE bench (id INT64, val TEXT, f FLOAT64)');
    db.exec('CREATE INDEX bench_id_idx ON bench(id)');

    const warmStmt = db.prepare('INSERT INTO bench VALUES ($1, $2, $3)');
    db.exec('BEGIN');
    warmStmt.bindAll([-1n, '__warm__', -1]);
    warmStmt.step();
    db.exec('ROLLBACK');
    warmStmt.finalize();

    const insertStmt = db.prepare('INSERT INTO bench VALUES ($1, $2, $3)');
    const insertStart = process.hrtime.bigint();
    runWithGcDisabled(() => {
      db.exec('BEGIN');
      for (let i = 0; i < opts.count; i++) {
        insertStmt.reset();
        insertStmt.clearBindings();
        insertStmt.bindAll([i, `value_${i}`, i]);
        insertStmt.step();
      }
      db.exec('COMMIT');
    });
    insertStmt.finalize();
    const insertSeconds = Number(process.hrtime.bigint() - insertStart) / 1e9;
    const insertRowsPerSecond = opts.count / insertSeconds;
    console.log(`Insert ${opts.count} rows: ${insertSeconds.toFixed(4)}s (${insertRowsPerSecond.toFixed(2)} rows/sec)`);

    const scanSql = 'SELECT id, val, f FROM bench';
    const warmScan = db.prepare(scanSql);
    warmScan.step();
    warmScan.finalize();

    let fetchallRows = null;
    const fetchallStart = process.hrtime.bigint();
    const fetchallSeconds = runWithGcDisabled(() => {
      const queryStart = process.hrtime.bigint();
      fetchallRows = db.exec(scanSql).rows;
      return Number(process.hrtime.bigint() - queryStart) / 1e9;
    });
    if (!Array.isArray(fetchallRows) || fetchallRows.length !== opts.count) {
      throw new Error(`Expected ${opts.count} rows from fetchall, got ${fetchallRows ? fetchallRows.length : 0}`);
    }
    void fetchallStart;
    console.log(`Fetchall ${opts.count} rows: ${fetchallSeconds.toFixed(4)}s`);

    const fetchmanyStmt = db.prepare(scanSql);
    const fetchmanySeconds = runWithGcDisabled(() => {
      const queryStart = process.hrtime.bigint();
      let total = 0;
      let pending = 0;
      while (fetchmanyStmt.step()) {
        pending++;
        if (pending === opts.fetchmanyBatch) {
          total += pending;
          pending = 0;
        }
      }
      total += pending;
      if (total !== opts.count) {
        throw new Error(`Expected ${opts.count} rows from fetchmany, got ${total}`);
      }
      return Number(process.hrtime.bigint() - queryStart) / 1e9;
    });
    fetchmanyStmt.finalize();
    console.log(`Fetchmany(${opts.fetchmanyBatch}) ${opts.count} rows: ${fetchmanySeconds.toFixed(4)}s`);

    const pointIds = buildPointReadIds(opts.count, opts.pointReads, opts.pointSeed);
    const pointStmt = db.prepare('SELECT id, val, f FROM bench WHERE id = $1');
    pointStmt.bindAll([pointIds[Math.floor(pointIds.length / 2)]]);
    if (!pointStmt.step()) {
      pointStmt.finalize();
      throw new Error('Warmup point read missed expected row');
    }
    pointStmt.rowArray();
    pointStmt.reset();
    pointStmt.clearBindings();

    const pointLatenciesMs = runWithGcDisabled(() => {
      const out = new Array(pointIds.length);
      for (let i = 0; i < pointIds.length; i++) {
        const started = process.hrtime.bigint();
        pointStmt.reset();
        pointStmt.clearBindings();
        pointStmt.bindAll([pointIds[i]]);
        if (!pointStmt.step()) {
          throw new Error(`Point read missed id=${pointIds[i]}`);
        }
        pointStmt.rowArray();
        out[i] = Number(process.hrtime.bigint() - started) / 1e6;
      }
      return out;
    });
    pointStmt.finalize();

    pointLatenciesMs.sort((a, b) => a - b);
    const pointP50Ms = percentileSorted(pointLatenciesMs, 50);
    const pointP95Ms = percentileSorted(pointLatenciesMs, 95);
    console.log(`Random point reads by id (${opts.pointReads}, seed=${opts.pointSeed}): p50=${pointP50Ms.toFixed(6)}ms p95=${pointP95Ms.toFixed(6)}ms`);

    return {
      insertSeconds,
      insertRowsPerSecond,
      fetchallSeconds,
      fetchmanySeconds,
      pointP50Ms,
      pointP95Ms,
    };
  } finally {
    db.close();
    if (!opts.keepDb) {
      cleanupDbFiles(dbPath);
    }
  }
}

function runSqliteBenchmark(dbPath, opts) {
  cleanupDbFiles(dbPath);
  console.log('\n=== sqlite ===');
  console.log('Setting up data...');
  const db = new DatabaseSync(dbPath);
  try {
    db.exec('PRAGMA journal_mode=WAL');
    db.exec('PRAGMA synchronous=FULL');
    db.exec('PRAGMA wal_autocheckpoint=0');
    db.exec('CREATE TABLE bench (id INTEGER, val TEXT, f REAL)');
    db.exec('CREATE INDEX bench_id_idx ON bench(id)');

    const warmStmt = db.prepare('INSERT INTO bench VALUES (?, ?, ?)');
    db.exec('BEGIN');
    warmStmt.run(-1, '__warm__', -1);
    db.exec('ROLLBACK');

    const insertStmt = db.prepare('INSERT INTO bench VALUES (?, ?, ?)');
    const insertSeconds = runWithGcDisabled(() => {
      const started = process.hrtime.bigint();
      db.exec('BEGIN');
      for (let i = 0; i < opts.count; i++) {
        insertStmt.run(i, `value_${i}`, i);
      }
      db.exec('COMMIT');
      return Number(process.hrtime.bigint() - started) / 1e9;
    });
    const insertRowsPerSecond = opts.count / insertSeconds;
    console.log(`Insert ${opts.count} rows: ${insertSeconds.toFixed(4)}s (${insertRowsPerSecond.toFixed(2)} rows/sec)`);

    const scanStmt = db.prepare('SELECT id, val, f FROM bench');
    scanStmt.get();

    let fetchallRows = null;
    const fetchallSeconds = runWithGcDisabled(() => {
      const started = process.hrtime.bigint();
      fetchallRows = scanStmt.all();
      return Number(process.hrtime.bigint() - started) / 1e9;
    });
    if (!Array.isArray(fetchallRows) || fetchallRows.length !== opts.count) {
      throw new Error(`Expected ${opts.count} rows from fetchall, got ${fetchallRows ? fetchallRows.length : 0}`);
    }
    console.log(`Fetchall ${opts.count} rows: ${fetchallSeconds.toFixed(4)}s`);

    const fetchmanySeconds = runWithGcDisabled(() => {
      const started = process.hrtime.bigint();
      let total = 0;
      let pending = 0;
      for (const row of scanStmt.iterate()) {
        void row;
        pending++;
        if (pending === opts.fetchmanyBatch) {
          total += pending;
          pending = 0;
        }
      }
      total += pending;
      if (total !== opts.count) {
        throw new Error(`Expected ${opts.count} rows from fetchmany, got ${total}`);
      }
      return Number(process.hrtime.bigint() - started) / 1e9;
    });
    console.log(`Fetchmany(${opts.fetchmanyBatch}) ${opts.count} rows: ${fetchmanySeconds.toFixed(4)}s`);

    const pointStmt = db.prepare('SELECT id, val, f FROM bench WHERE id = ?');
    const pointIds = buildPointReadIds(opts.count, opts.pointReads, opts.pointSeed);
    const warmupRow = pointStmt.get(pointIds[Math.floor(pointIds.length / 2)]);
    if (!warmupRow) {
      throw new Error('Warmup point read missed expected row');
    }

    const pointLatenciesMs = runWithGcDisabled(() => {
      const out = new Array(pointIds.length);
      for (let i = 0; i < pointIds.length; i++) {
        const started = process.hrtime.bigint();
        const row = pointStmt.get(pointIds[i]);
        if (!row) {
          throw new Error(`Point read missed id=${pointIds[i]}`);
        }
        out[i] = Number(process.hrtime.bigint() - started) / 1e6;
      }
      return out;
    });
    pointLatenciesMs.sort((a, b) => a - b);
    const pointP50Ms = percentileSorted(pointLatenciesMs, 50);
    const pointP95Ms = percentileSorted(pointLatenciesMs, 95);
    console.log(`Random point reads by id (${opts.pointReads}, seed=${opts.pointSeed}): p50=${pointP50Ms.toFixed(6)}ms p95=${pointP95Ms.toFixed(6)}ms`);

    db.exec('PRAGMA wal_checkpoint(TRUNCATE)');

    return {
      insertSeconds,
      insertRowsPerSecond,
      fetchallSeconds,
      fetchmanySeconds,
      pointP50Ms,
      pointP95Ms,
    };
  } finally {
    db.close();
    if (!opts.keepDb) {
      cleanupDbFiles(dbPath);
    }
  }
}

function printComparison(results) {
  if (!results.decentdb || !results.sqlite) {
    return;
  }
  const decent = results.decentdb;
  const sqlite = results.sqlite;
  const metrics = [
    {
      name: 'Insert throughput (higher is better)',
      decent: decent.insertRowsPerSecond,
      sqlite: sqlite.insertRowsPerSecond,
      unit: ' rows/s',
      higherIsBetter: true,
      fmt: (n) => n.toFixed(2),
    },
    {
      name: 'Fetchall time (lower is better)',
      decent: decent.fetchallSeconds,
      sqlite: sqlite.fetchallSeconds,
      unit: 's',
      higherIsBetter: false,
      fmt: (n) => n.toFixed(6),
    },
    {
      name: 'Fetchmany/streaming time (lower is better)',
      decent: decent.fetchmanySeconds,
      sqlite: sqlite.fetchmanySeconds,
      unit: 's',
      higherIsBetter: false,
      fmt: (n) => n.toFixed(6),
    },
    {
      name: 'Point read p50 latency (lower is better)',
      decent: decent.pointP50Ms,
      sqlite: sqlite.pointP50Ms,
      unit: 'ms',
      higherIsBetter: false,
      fmt: (n) => n.toFixed(6),
    },
    {
      name: 'Point read p95 latency (lower is better)',
      decent: decent.pointP95Ms,
      sqlite: sqlite.pointP95Ms,
      unit: 'ms',
      higherIsBetter: false,
      fmt: (n) => n.toFixed(6),
    },
  ];

  const decentBetter = [];
  const sqliteBetter = [];
  const ties = [];

  for (const metric of metrics) {
    if (metric.decent === metric.sqlite) {
      ties.push(`${metric.name}: tie (${metric.fmt(metric.decent)}${metric.unit})`);
      continue;
    }
    let decentWins;
    let winner;
    let loser;
    let ratio;
    let detail;
    if (metric.higherIsBetter) {
      decentWins = metric.decent > metric.sqlite;
      winner = decentWins ? metric.decent : metric.sqlite;
      loser = decentWins ? metric.sqlite : metric.decent;
      ratio = loser === 0 ? Number.POSITIVE_INFINITY : winner / loser;
      detail = `${metric.name}: ${metric.fmt(winner)}${metric.unit} vs ${metric.fmt(loser)}${metric.unit} (${ratio.toFixed(3)}x higher)`;
    } else {
      decentWins = metric.decent < metric.sqlite;
      winner = decentWins ? metric.decent : metric.sqlite;
      loser = decentWins ? metric.sqlite : metric.decent;
      ratio = winner === 0 ? Number.POSITIVE_INFINITY : loser / winner;
      detail = `${metric.name}: ${metric.fmt(winner)}${metric.unit} vs ${metric.fmt(loser)}${metric.unit} (${ratio.toFixed(3)}x faster/lower)`;
    }
    if (decentWins) {
      decentBetter.push(detail);
    } else {
      sqliteBetter.push(detail);
    }
  }

  console.log('\n=== Comparison (DecentDB vs SQLite) ===');
  console.log('DecentDB better at:');
  if (decentBetter.length === 0) {
    console.log('- none');
  } else {
    for (const line of decentBetter) {
      console.log(`- ${line}`);
    }
  }

  console.log('SQLite better at:');
  if (sqliteBetter.length === 0) {
    console.log('- none');
  } else {
    for (const line of sqliteBetter) {
      console.log(`- ${line}`);
    }
  }

  if (ties.length > 0) {
    console.log('Ties:');
    for (const line of ties) {
      console.log(`- ${line}`);
    }
  }
}

function main() {
  const opts = parseCli();
  const engines = opts.engine === 'all' ? ['decentdb', 'sqlite'] : [opts.engine];
  const results = {};
  for (const engine of engines) {
    const suffix = engine === 'sqlite' ? 'db' : 'ddb';
    const dbPath = `${opts.dbPrefix}_${engine}.${suffix}`;
    if (engine === 'decentdb') {
      results.decentdb = runDecentDbBenchmark(dbPath, opts);
    } else {
      results.sqlite = runSqliteBenchmark(dbPath, opts);
    }
  }
  printComparison(results);
}

main();
