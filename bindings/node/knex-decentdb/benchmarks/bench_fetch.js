'use strict';

const fs = require('node:fs');
const process = require('node:process');
const { parseArgs } = require('node:util');
const { DatabaseSync } = require('node:sqlite');
const knex = require('knex');
const { Client_DecentDB } = require('..');

const DEFAULT_COUNT = 1_000_000;
const DEFAULT_POINT_READS = 10_000;
const DEFAULT_POINT_SEED = 1337;
const DEFAULT_FETCHMANY_BATCH = 4096;
const DEFAULT_INSERT_BATCH = 4096;

function parseCli() {
  const { values } = parseArgs({
    options: {
      engine: { type: 'string', default: 'all' },
      count: { type: 'string', default: String(DEFAULT_COUNT) },
      'point-reads': { type: 'string', default: String(DEFAULT_POINT_READS) },
      'point-seed': { type: 'string', default: String(DEFAULT_POINT_SEED) },
      'fetchmany-batch': { type: 'string', default: String(DEFAULT_FETCHMANY_BATCH) },
      'insert-batch': { type: 'string', default: String(DEFAULT_INSERT_BATCH) },
      'db-prefix': { type: 'string', default: 'node_knex_bench_fetch' },
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
  const pointSeed = Number.parseInt(values['point-seed'], 10);
  if (!Number.isInteger(pointSeed)) {
    throw new Error('--point-seed must be an integer');
  }
  const fetchmanyBatch = parsePositiveInt(values['fetchmany-batch'], '--fetchmany-batch');
  const insertBatch = parsePositiveInt(values['insert-batch'], '--insert-batch');
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
    insertBatch,
    dbPrefix,
    keepDb: values['keep-db'] === true,
  };
}

function printUsage() {
  console.log('Fair Node Knex benchmark: knex-decentdb vs Knex sqlite (node:sqlite)');
  console.log('Usage:');
  console.log('  node benchmarks/bench_fetch.js [options]');
  console.log('');
  console.log('Options:');
  console.log('  --engine <all|decentdb|sqlite>   Engines to run (default: all)');
  console.log(`  --count <n>                      Rows to insert/fetch (default: ${DEFAULT_COUNT})`);
  console.log(`  --fetchmany-batch <n>            Batch size for fetchmany metric (default: ${DEFAULT_FETCHMANY_BATCH})`);
  console.log(`  --insert-batch <n>               Batch size for insert throughput metric (default: ${DEFAULT_INSERT_BATCH})`);
  console.log(`  --point-reads <n>                Random indexed point lookups (default: ${DEFAULT_POINT_READS})`);
  console.log(`  --point-seed <n>                 RNG seed for point lookups (default: ${DEFAULT_POINT_SEED})`);
  console.log('  --db-prefix <path_prefix>        Database prefix (default: node_knex_bench_fetch)');
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
  if (typeof global.gc === 'function') {
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

function createSqliteKnexConfig(filename) {
  class Client_SqliteSync extends knex.Client {
    constructor(config) {
      super(config);
      this.driverName = 'node:sqlite';
      this.dialect = 'sqlite3';
      if (!this.pool) {
        this.initializePool(config);
      }
    }

    _driver() {
      return { DatabaseSync };
    }

    async acquireRawConnection() {
      const db = new DatabaseSync(filename);
      db.exec('PRAGMA journal_mode=WAL');
      db.exec('PRAGMA synchronous=FULL');
      db.exec('PRAGMA wal_autocheckpoint=0');
      return db;
    }

    async destroyRawConnection(connection) {
      connection.close();
    }

    _query(connection, obj) {
      const bindings = obj.bindings || [];
      let response;
      if (obj.method === 'insert' || obj.method === 'update' || obj.method === 'del' || obj.method === 'counter') {
        response = connection.prepare(obj.sql).run(...bindings);
      } else {
        response = connection.prepare(obj.sql).all(...bindings);
      }
      obj.response = response;
      obj.context = response;
      return Promise.resolve(obj);
    }

    _stream(connection, obj, stream, options) {
      if (!obj.sql) {
        throw new Error('The query is empty');
      }
      return new Promise((resolve, reject) => {
        stream.on('error', reject);
        stream.on('end', resolve);

        try {
          const bindings = obj.bindings || [];
          const stmt = connection.prepare(obj.sql);
          for (const row of stmt.iterate(...bindings)) {
            stream.write(row);
          }
        } catch (error) {
          stream.emit('error', error);
        }
        stream.end();
      });
    }

    processResponse(obj) {
      if (obj.method === 'raw') {
        return obj.response;
      }
      if (obj.method === 'insert') {
        if (Array.isArray(obj.response)) {
          return obj.response;
        }
        return [Number(obj.response.lastInsertRowid)];
      }
      if (obj.method === 'update' || obj.method === 'del' || obj.method === 'counter') {
        return Number(obj.response.changes);
      }
      return obj.response;
    }

    positionBindings(sql) {
      return sql;
    }
  }

  return {
    client: Client_SqliteSync,
    connection: { filename },
    useNullAsDefault: true,
    pool: { min: 1, max: 1 },
  };
}

async function runDecentKnexBenchmark(dbPath, opts) {
  cleanupDbFiles(dbPath);
  console.log('\n=== decentdb ===');
  console.log('Setting up data...');
  const k = knex({
    client: Client_DecentDB,
    connection: { filename: dbPath },
    useNullAsDefault: true,
    pool: { min: 1, max: 1 },
  });

  try {
    await k.raw('CREATE TABLE bench (id INT64, val TEXT, f FLOAT64)');
    await k.raw('CREATE INDEX bench_id_idx ON bench(id)');

    await k.transaction(async (trx) => {
      await trx.raw('INSERT INTO bench VALUES (?, ?, ?)', [-1, '__warm__', -1]);
      throw new Error('__rollback_warmup__');
    }).catch((error) => {
      if (!error || error.message !== '__rollback_warmup__') {
        throw error;
      }
    });

    const insertStart = process.hrtime.bigint();
    await runWithGcDisabled(async () => {
      await k.transaction(async (trx) => {
        for (let start = 0; start < opts.count; start += opts.insertBatch) {
          const end = Math.min(start + opts.insertBatch, opts.count);
          const placeholders = new Array(end - start);
          const bindings = new Array((end - start) * 3);
          for (let i = start, row = 0; i < end; i++, row++) {
            placeholders[row] = '(?, ?, ?)';
            const base = row * 3;
            bindings[base] = i;
            bindings[base + 1] = `value_${i}`;
            bindings[base + 2] = i;
          }
          await trx.raw(`INSERT INTO bench VALUES ${placeholders.join(', ')}`, bindings);
        }
      });
    });
    const insertSeconds = Number(process.hrtime.bigint() - insertStart) / 1e9;
    const insertRowsPerSecond = opts.count / insertSeconds;
    console.log(`Insert ${opts.count} rows: ${insertSeconds.toFixed(4)}s (${insertRowsPerSecond.toFixed(2)} rows/sec)`);

    await k.raw('SELECT id, val, f FROM bench LIMIT 1');

    const fetchallSeconds = await runWithGcDisabled(async () => {
      const started = process.hrtime.bigint();
      const rows = await k.raw('SELECT id, val, f FROM bench');
      const normalized = rows.rows ?? rows;
      if (!Array.isArray(normalized) || normalized.length !== opts.count) {
        throw new Error(`Expected ${opts.count} rows from fetchall, got ${normalized ? normalized.length : 0}`);
      }
      return Number(process.hrtime.bigint() - started) / 1e9;
    });
    console.log(`Fetchall ${opts.count} rows: ${fetchallSeconds.toFixed(4)}s`);

    const fetchmanySeconds = await runWithGcDisabled(async () => {
      const started = process.hrtime.bigint();
      const stream = k.raw('SELECT id, val, f FROM bench').stream();
      let total = 0;
      let pending = 0;
      for await (const row of stream) {
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

    const pointIds = buildPointReadIds(opts.count, opts.pointReads, opts.pointSeed);
    const warm = await k.raw('SELECT id, val, f FROM bench WHERE id = ?', [pointIds[Math.floor(pointIds.length / 2)]]);
    const warmRows = warm.rows ?? warm;
    if (!Array.isArray(warmRows) || warmRows.length === 0) {
      throw new Error('Warmup point read missed expected row');
    }

    const pointLatenciesMs = await runWithGcDisabled(async () => {
      const out = new Array(pointIds.length);
      for (let i = 0; i < pointIds.length; i++) {
        const started = process.hrtime.bigint();
        const rows = await k.raw('SELECT id, val, f FROM bench WHERE id = ?', [pointIds[i]]);
        const normalized = rows.rows ?? rows;
        if (!Array.isArray(normalized) || normalized.length === 0) {
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

    return {
      insertSeconds,
      insertRowsPerSecond,
      fetchallSeconds,
      fetchmanySeconds,
      pointP50Ms,
      pointP95Ms,
    };
  } finally {
    await k.destroy();
    if (!opts.keepDb) {
      cleanupDbFiles(dbPath);
    }
  }
}

async function runSqliteKnexBenchmark(dbPath, opts) {
  cleanupDbFiles(dbPath);
  console.log('\n=== sqlite ===');
  console.log('Setting up data...');

  const k = knex(createSqliteKnexConfig(dbPath));
  try {
    await k.raw('CREATE TABLE bench (id INTEGER, val TEXT, f REAL)');
    await k.raw('CREATE INDEX bench_id_idx ON bench(id)');

    await k.transaction(async (trx) => {
      await trx.raw('INSERT INTO bench VALUES (?, ?, ?)', [-1, '__warm__', -1]);
      throw new Error('__rollback_warmup__');
    }).catch((error) => {
      if (!error || error.message !== '__rollback_warmup__') {
        throw error;
      }
    });

    const insertSeconds = await runWithGcDisabled(async () => {
      const started = process.hrtime.bigint();
      await k.transaction(async (trx) => {
        for (let start = 0; start < opts.count; start += opts.insertBatch) {
          const end = Math.min(start + opts.insertBatch, opts.count);
          const placeholders = new Array(end - start);
          const bindings = new Array((end - start) * 3);
          for (let i = start, row = 0; i < end; i++, row++) {
            placeholders[row] = '(?, ?, ?)';
            const base = row * 3;
            bindings[base] = i;
            bindings[base + 1] = `value_${i}`;
            bindings[base + 2] = i;
          }
          await trx.raw(`INSERT INTO bench VALUES ${placeholders.join(', ')}`, bindings);
        }
      });
      return Number(process.hrtime.bigint() - started) / 1e9;
    });
    const insertRowsPerSecond = opts.count / insertSeconds;
    console.log(`Insert ${opts.count} rows: ${insertSeconds.toFixed(4)}s (${insertRowsPerSecond.toFixed(2)} rows/sec)`);

    await k.raw('SELECT id, val, f FROM bench LIMIT 1');

    const fetchallSeconds = await runWithGcDisabled(async () => {
      const started = process.hrtime.bigint();
      const rows = await k.raw('SELECT id, val, f FROM bench');
      const normalized = rows.rows ?? rows;
      if (!Array.isArray(normalized) || normalized.length !== opts.count) {
        throw new Error(`Expected ${opts.count} rows from fetchall, got ${normalized ? normalized.length : 0}`);
      }
      return Number(process.hrtime.bigint() - started) / 1e9;
    });
    console.log(`Fetchall ${opts.count} rows: ${fetchallSeconds.toFixed(4)}s`);

    const fetchmanySeconds = await runWithGcDisabled(async () => {
      const started = process.hrtime.bigint();
      const stream = k.raw('SELECT id, val, f FROM bench').stream();
      let total = 0;
      let pending = 0;
      for await (const row of stream) {
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

    const pointIds = buildPointReadIds(opts.count, opts.pointReads, opts.pointSeed);
    const warmRows = await k.raw('SELECT id, val, f FROM bench WHERE id = ?', [pointIds[Math.floor(pointIds.length / 2)]]);
    const warm = warmRows.rows ?? warmRows;
    if (!Array.isArray(warm) || warm.length === 0) {
      throw new Error('Warmup point read missed expected row');
    }

    const pointLatenciesMs = await runWithGcDisabled(async () => {
      const out = new Array(pointIds.length);
      for (let i = 0; i < pointIds.length; i++) {
        const started = process.hrtime.bigint();
        const row = await k.raw('SELECT id, val, f FROM bench WHERE id = ?', [pointIds[i]]);
        const normalized = row.rows ?? row;
        if (!Array.isArray(normalized) || normalized.length === 0) {
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

    await k.raw('PRAGMA wal_checkpoint(TRUNCATE)');

    return {
      insertSeconds,
      insertRowsPerSecond,
      fetchallSeconds,
      fetchmanySeconds,
      pointP50Ms,
      pointP95Ms,
    };
  } finally {
    await k.destroy();
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

async function main() {
  const opts = parseCli();
  const engines = opts.engine === 'all' ? ['decentdb', 'sqlite'] : [opts.engine];
  const results = {};
  for (const engine of engines) {
    const suffix = engine === 'sqlite' ? 'db' : 'ddb';
    const dbPath = `${opts.dbPrefix}_${engine}.${suffix}`;
    if (engine === 'decentdb') {
      results.decentdb = await runDecentKnexBenchmark(dbPath, opts);
    } else {
      results.sqlite = await runSqliteKnexBenchmark(dbPath, opts);
    }
  }
  printComparison(results);
}

main().catch((error) => {
  console.error(error);
  process.exit(1);
});
