using System.Data;
using System.Data.Common;
using System.Diagnostics;
using System.Globalization;
using System.Runtime;
using Dapper;
using DecentDB.AdoNet;
using DecentDB.EntityFrameworkCore;
using DecentDB.MicroOrm;
using Microsoft.Data.Sqlite;
using Microsoft.EntityFrameworkCore;

const int DefaultCount = BenchmarkDefaults.Count;
const int DefaultPointReads = BenchmarkDefaults.PointReads;
const int DefaultPointSeed = BenchmarkDefaults.PointSeed;
const int DefaultFetchmanyBatch = BenchmarkDefaults.FetchmanyBatch;
const int DefaultEfInsertBatch = BenchmarkDefaults.EfInsertBatch;

var options = ParseArgs(args);
if (options.ShowHelp)
{
    PrintUsage();
    return;
}

var offerings = ResolveOfferings(options.Engine);
var results = new Dictionary<OfferingKind, OfferingResult>();
foreach (var offering in offerings)
{
    var extension = offering.IsSqlite() ? "db" : "ddb";
    var dbPath = $"{options.DbPrefix}_{offering.ToToken()}.{extension}";
    results[offering] = RunOfferingBenchmark(offering, dbPath, options);
}

PrintPairedComparisons(results);
PrintOverallRanking(results);

static OfferingResult RunOfferingBenchmark(OfferingKind offering, string dbPath, BenchmarkOptions options)
{
    return offering switch
    {
        OfferingKind.DecentDbAdo or OfferingKind.SqliteAdo => RunAdoBenchmark(offering, dbPath, options),
        OfferingKind.DecentDbDapper or OfferingKind.SqliteDapper => RunDapperBenchmark(offering, dbPath, options),
        OfferingKind.DecentDbMicroOrm => RunMicroOrmBenchmark(dbPath, options),
        OfferingKind.DecentDbEfCore or OfferingKind.SqliteEfCore => RunEfCoreBenchmark(offering, dbPath, options),
        _ => throw new ArgumentOutOfRangeException(nameof(offering), offering, "Unknown benchmark offering"),
    };
}

static OfferingResult RunAdoBenchmark(OfferingKind offering, string dbPath, BenchmarkOptions options)
{
    DeleteDbFiles(dbPath);
    Console.WriteLine();
    Console.WriteLine($"=== {offering.ToDisplayName()} ===");
    Console.WriteLine("Setting up data...");

    using var conn = OpenAdoConnection(offering, dbPath);
    SetupSchema(conn, offering.IsSqlite());

    WarmInsertPrepared(conn);
    var insertSeconds = RunWithGcDisabled(() =>
    {
        var started = Stopwatch.GetTimestamp();
        using var tx = conn.BeginTransaction();
        using var cmd = CreateInsertCommand(conn, tx);
        for (var i = 0; i < options.Count; i++)
        {
            BindInsertRow(cmd, i, $"value_{i}", i);
            cmd.ExecuteNonQuery();
        }

        tx.Commit();
        return Stopwatch.GetElapsedTime(started).TotalSeconds;
    });
    var insertRowsPerSecond = options.Count / insertSeconds;
    Console.WriteLine($"Insert {options.Count:N0} rows: {insertSeconds:0.0000}s ({insertRowsPerSecond:N2} rows/sec)");

    using var scanCmd = conn.CreateCommand();
    scanCmd.CommandText = "SELECT id, val, f FROM bench";
    TryPrepare(scanCmd);
    WarmScan(scanCmd);

    var fetchallSeconds = RunWithGcDisabled(() =>
    {
        var started = Stopwatch.GetTimestamp();
        var rows = ReadAllRows(scanCmd, options.Count);
        if (rows.Count != options.Count)
        {
            throw new InvalidOperationException($"Expected {options.Count} rows from fetchall, got {rows.Count}");
        }

        return Stopwatch.GetElapsedTime(started).TotalSeconds;
    });
    Console.WriteLine($"Fetchall {options.Count:N0} rows: {fetchallSeconds:0.0000}s");

    var fetchmanySeconds = RunWithGcDisabled(() =>
    {
        var started = Stopwatch.GetTimestamp();
        var total = ReadRowsInBatches(scanCmd, options.FetchmanyBatch);
        if (total != options.Count)
        {
            throw new InvalidOperationException($"Expected {options.Count} rows from fetchmany, got {total}");
        }

        return Stopwatch.GetElapsedTime(started).TotalSeconds;
    });
    Console.WriteLine($"Fetchmany({options.FetchmanyBatch:N0}) {options.Count:N0} rows: {fetchmanySeconds:0.0000}s");

    using var pointCmd = conn.CreateCommand();
    pointCmd.CommandText = "SELECT id, val, f FROM bench WHERE id = @id";
    var pointIdParam = pointCmd.CreateParameter();
    pointIdParam.ParameterName = "@id";
    pointIdParam.DbType = DbType.Int64;
    pointCmd.Parameters.Add(pointIdParam);
    TryPrepare(pointCmd);

    var pointIds = BuildPointReadIds(options.Count, options.PointReads, options.PointSeed);
    pointIdParam.Value = pointIds[pointIds.Length / 2];
    using (var warm = pointCmd.ExecuteReader())
    {
        if (!warm.Read())
        {
            throw new InvalidOperationException("Warmup point read missed expected row");
        }
    }

    var latencies = RunWithGcDisabled(() =>
    {
        var samples = new double[pointIds.Length];
        for (var i = 0; i < pointIds.Length; i++)
        {
            var started = Stopwatch.GetTimestamp();
            pointIdParam.Value = pointIds[i];
            using var reader = pointCmd.ExecuteReader();
            if (!reader.Read())
            {
                throw new InvalidOperationException($"Point read missed id={pointIds[i]}");
            }

            _ = reader.GetInt64(0);
            _ = reader.GetString(1);
            _ = reader.GetDouble(2);
            samples[i] = Stopwatch.GetElapsedTime(started).TotalMilliseconds;
        }

        return samples;
    });
    Array.Sort(latencies);
    var pointP50Ms = PercentileSorted(latencies, 50);
    var pointP95Ms = PercentileSorted(latencies, 95);
    Console.WriteLine($"Random point reads by id ({options.PointReads:N0}, seed={options.PointSeed}): p50={pointP50Ms:0.000000}ms p95={pointP95Ms:0.000000}ms");

    if (offering.IsSqlite())
    {
        ExecNonQuery(conn, "PRAGMA wal_checkpoint(TRUNCATE)");
    }

    conn.Close();
    if (!options.KeepDb)
    {
        DeleteDbFiles(dbPath);
    }

    return new OfferingResult(offering, insertSeconds, insertRowsPerSecond, fetchallSeconds, fetchmanySeconds, pointP50Ms, pointP95Ms);
}

static OfferingResult RunDapperBenchmark(OfferingKind offering, string dbPath, BenchmarkOptions options)
{
    DeleteDbFiles(dbPath);
    Console.WriteLine();
    Console.WriteLine($"=== {offering.ToDisplayName()} ===");
    Console.WriteLine("Setting up data...");

    using var conn = OpenAdoConnection(offering, dbPath);
    SetupSchema(conn, offering.IsSqlite());

    using (var tx = conn.BeginTransaction())
    {
        _ = conn.Execute("INSERT INTO bench VALUES (@Id, @Val, @F)", new DapperBenchRow { Id = -1, Val = "__warm__", F = -1.0 }, tx);
        tx.Rollback();
    }

    var insertSeconds = RunWithGcDisabled(() =>
    {
        var started = Stopwatch.GetTimestamp();
        using var tx = conn.BeginTransaction();
        _ = conn.Execute("INSERT INTO bench VALUES (@Id, @Val, @F)", BuildDapperRows(options.Count), tx);
        tx.Commit();
        return Stopwatch.GetElapsedTime(started).TotalSeconds;
    });
    var insertRowsPerSecond = options.Count / insertSeconds;
    Console.WriteLine($"Insert {options.Count:N0} rows: {insertSeconds:0.0000}s ({insertRowsPerSecond:N2} rows/sec)");

    const string scanSql = "SELECT id AS Id, val AS Val, f AS F FROM bench ORDER BY id";
    using (var warm = conn.Query<DapperBenchRow>(scanSql, buffered: false).GetEnumerator())
    {
        _ = warm.MoveNext();
    }

    var fetchallSeconds = RunWithGcDisabled(() =>
    {
        var started = Stopwatch.GetTimestamp();
        var rows = conn.Query<DapperBenchRow>(scanSql).AsList();
        if (rows.Count != options.Count)
        {
            throw new InvalidOperationException($"Expected {options.Count} rows from fetchall, got {rows.Count}");
        }

        return Stopwatch.GetElapsedTime(started).TotalSeconds;
    });
    Console.WriteLine($"Fetchall {options.Count:N0} rows: {fetchallSeconds:0.0000}s");

    var fetchmanySeconds = RunWithGcDisabled(() =>
    {
        var started = Stopwatch.GetTimestamp();
        var stream = conn.Query<DapperBenchRow>(
            scanSql,
            buffered: false);
        var total = ConsumeSyncRowsInBatches(stream, options.FetchmanyBatch);
        if (total != options.Count)
        {
            throw new InvalidOperationException($"Expected {options.Count} rows from fetchmany, got {total}");
        }

        return Stopwatch.GetElapsedTime(started).TotalSeconds;
    });
    Console.WriteLine($"Fetchmany({options.FetchmanyBatch:N0}) {options.Count:N0} rows: {fetchmanySeconds:0.0000}s");

    var pointIds = BuildPointReadIds(options.Count, options.PointReads, options.PointSeed);
    var pointParam = new DapperPointParam { Id = pointIds[pointIds.Length / 2] };
    _ = conn.QueryFirstOrDefault<DapperBenchRow>(
        "SELECT id AS Id, val AS Val, f AS F FROM bench WHERE id = @Id",
        pointParam) ?? throw new InvalidOperationException("Warmup point read missed expected row");

    var latencies = RunWithGcDisabled(() =>
    {
        var samples = new double[pointIds.Length];
        for (var i = 0; i < pointIds.Length; i++)
        {
            pointParam.Id = pointIds[i];
            var started = Stopwatch.GetTimestamp();
            var row = conn.QueryFirstOrDefault<DapperBenchRow>(
                "SELECT id AS Id, val AS Val, f AS F FROM bench WHERE id = @Id",
                pointParam);
            if (row == null)
            {
                throw new InvalidOperationException($"Point read missed id={pointIds[i]}");
            }

            _ = row.Id;
            _ = row.Val;
            _ = row.F;
            samples[i] = Stopwatch.GetElapsedTime(started).TotalMilliseconds;
        }

        return samples;
    });
    Array.Sort(latencies);
    var pointP50Ms = PercentileSorted(latencies, 50);
    var pointP95Ms = PercentileSorted(latencies, 95);
    Console.WriteLine($"Random point reads by id ({options.PointReads:N0}, seed={options.PointSeed}): p50={pointP50Ms:0.000000}ms p95={pointP95Ms:0.000000}ms");

    if (offering.IsSqlite())
    {
        _ = conn.Execute("PRAGMA wal_checkpoint(TRUNCATE)");
    }

    conn.Close();
    if (!options.KeepDb)
    {
        DeleteDbFiles(dbPath);
    }

    return new OfferingResult(offering, insertSeconds, insertRowsPerSecond, fetchallSeconds, fetchmanySeconds, pointP50Ms, pointP95Ms);
}

static OfferingResult RunMicroOrmBenchmark(string dbPath, BenchmarkOptions options)
{
    DeleteDbFiles(dbPath);
    Console.WriteLine();
    Console.WriteLine($"=== {OfferingKind.DecentDbMicroOrm.ToDisplayName()} ===");
    Console.WriteLine("Setting up data...");

    using (var conn = new DecentDBConnection($"Data Source={dbPath}"))
    {
        conn.Open();
        SetupSchema(conn, isSqlite: false);
    }

    using var ctx = new DecentDBContext(dbPath, pooling: true);
    var set = ctx.Set<MicroOrmBenchRow>();

    set.InsertAsync(new MicroOrmBenchRow { Id = -1, Val = "__warm__", F = -1.0 }).GetAwaiter().GetResult();
    set.DeleteByIdAsync(-1).GetAwaiter().GetResult();

    var insertSeconds = RunWithGcDisabled(() =>
    {
        var started = Stopwatch.GetTimestamp();
        set.InsertManyAsync(BuildMicroOrmRows(options.Count)).GetAwaiter().GetResult();
        return Stopwatch.GetElapsedTime(started).TotalSeconds;
    });
    var insertRowsPerSecond = options.Count / insertSeconds;
    Console.WriteLine($"Insert {options.Count:N0} rows: {insertSeconds:0.0000}s ({insertRowsPerSecond:N2} rows/sec)");

    var fetchallSeconds = RunWithGcDisabled(() =>
    {
        var started = Stopwatch.GetTimestamp();
        var rows = set.OrderBy(r => r.Id).ToListAsync().GetAwaiter().GetResult();
        if (rows.Count != options.Count)
        {
            throw new InvalidOperationException($"Expected {options.Count} rows from fetchall, got {rows.Count}");
        }

        return Stopwatch.GetElapsedTime(started).TotalSeconds;
    });
    Console.WriteLine($"Fetchall {options.Count:N0} rows: {fetchallSeconds:0.0000}s");

    var fetchmanySeconds = RunWithGcDisabled(() =>
    {
        var started = Stopwatch.GetTimestamp();
        var total = ConsumeAsyncRowsInBatches(set.OrderBy(r => r.Id).StreamAsync(), options.FetchmanyBatch);
        if (total != options.Count)
        {
            throw new InvalidOperationException($"Expected {options.Count} rows from fetchmany, got {total}");
        }

        return Stopwatch.GetElapsedTime(started).TotalSeconds;
    });
    Console.WriteLine($"Fetchmany({options.FetchmanyBatch:N0}) {options.Count:N0} rows: {fetchmanySeconds:0.0000}s");

    var pointIds = BuildPointReadIds(options.Count, options.PointReads, options.PointSeed);
    _ = set.GetAsync(pointIds[pointIds.Length / 2]).GetAwaiter().GetResult() ??
        throw new InvalidOperationException("Warmup point read missed expected row");

    var latencies = RunWithGcDisabled(() =>
    {
        var samples = new double[pointIds.Length];
        for (var i = 0; i < pointIds.Length; i++)
        {
            var started = Stopwatch.GetTimestamp();
            var row = set.GetAsync(pointIds[i]).GetAwaiter().GetResult();
            if (row == null)
            {
                throw new InvalidOperationException($"Point read missed id={pointIds[i]}");
            }

            _ = row.Id;
            _ = row.Val;
            _ = row.F;
            samples[i] = Stopwatch.GetElapsedTime(started).TotalMilliseconds;
        }

        return samples;
    });
    Array.Sort(latencies);
    var pointP50Ms = PercentileSorted(latencies, 50);
    var pointP95Ms = PercentileSorted(latencies, 95);
    Console.WriteLine($"Random point reads by id ({options.PointReads:N0}, seed={options.PointSeed}): p50={pointP50Ms:0.000000}ms p95={pointP95Ms:0.000000}ms");

    if (!options.KeepDb)
    {
        DeleteDbFiles(dbPath);
    }

    return new OfferingResult(OfferingKind.DecentDbMicroOrm, insertSeconds, insertRowsPerSecond, fetchallSeconds, fetchmanySeconds, pointP50Ms, pointP95Ms);
}

static OfferingResult RunEfCoreBenchmark(OfferingKind offering, string dbPath, BenchmarkOptions options)
{
    DeleteDbFiles(dbPath);
    Console.WriteLine();
    Console.WriteLine($"=== {offering.ToDisplayName()} ===");
    Console.WriteLine("Setting up data...");

    using var setupContext = CreateEfContext(offering, dbPath);
    SetupSchemaEf(setupContext, offering.IsSqlite());
    setupContext.Dispose();

    using var ctx = CreateEfContext(offering, dbPath);
    ctx.ChangeTracker.AutoDetectChangesEnabled = false;
    ctx.Database.OpenConnection();
    var pointRowByIdQuery = EF.CompileQuery(
        (BenchEfContext c, long id) => c.BenchRows.AsNoTracking().FirstOrDefault(r => r.Id == id));

    using (var warmTx = ctx.Database.BeginTransaction())
    {
        ctx.BenchRows.Add(new BenchEfRow { Id = -1, Val = "__warm__", F = -1.0 });
        ctx.SaveChanges();
        warmTx.Rollback();
        ctx.ChangeTracker.Clear();
    }

    var insertSeconds = RunWithGcDisabled(() =>
    {
        var started = Stopwatch.GetTimestamp();
        using var tx = ctx.Database.BeginTransaction();
        var pending = 0;
        for (var i = 0; i < options.Count; i++)
        {
            ctx.BenchRows.Add(new BenchEfRow
            {
                Id = i,
                Val = $"value_{i}",
                F = i,
            });
            pending++;
            if (pending >= options.EfInsertBatch)
            {
                ctx.SaveChanges();
                ctx.ChangeTracker.Clear();
                pending = 0;
            }
        }

        if (pending > 0)
        {
            ctx.SaveChanges();
            ctx.ChangeTracker.Clear();
        }

        tx.Commit();
        return Stopwatch.GetElapsedTime(started).TotalSeconds;
    });
    var insertRowsPerSecond = options.Count / insertSeconds;
    Console.WriteLine($"Insert {options.Count:N0} rows: {insertSeconds:0.0000}s ({insertRowsPerSecond:N2} rows/sec)");

    // Warm query compilation/execution for fair steady-state read timing.
    _ = ctx.BenchRows.AsNoTracking().OrderBy(r => r.Id).Take(1).ToList();
    _ = ctx.BenchRows.AsNoTracking().OrderBy(r => r.Id).AsEnumerable().Take(1).Count();
    _ = pointRowByIdQuery(ctx, options.Count / 2);

    var fetchallSeconds = RunWithGcDisabled(() =>
    {
        var started = Stopwatch.GetTimestamp();
        var rows = ctx.BenchRows.AsNoTracking().OrderBy(r => r.Id).ToList();
        if (rows.Count != options.Count)
        {
            throw new InvalidOperationException($"Expected {options.Count} rows from fetchall, got {rows.Count}");
        }

        return Stopwatch.GetElapsedTime(started).TotalSeconds;
    });
    Console.WriteLine($"Fetchall {options.Count:N0} rows: {fetchallSeconds:0.0000}s");

    var fetchmanySeconds = RunWithGcDisabled(() =>
    {
        var started = Stopwatch.GetTimestamp();
        var total = ConsumeSyncRowsInBatches(
            ctx.BenchRows.AsNoTracking().OrderBy(r => r.Id).AsEnumerable(),
            options.FetchmanyBatch);
        if (total != options.Count)
        {
            throw new InvalidOperationException($"Expected {options.Count} rows from fetchmany, got {total}");
        }

        return Stopwatch.GetElapsedTime(started).TotalSeconds;
    });
    Console.WriteLine($"Fetchmany({options.FetchmanyBatch:N0}) {options.Count:N0} rows: {fetchmanySeconds:0.0000}s");

    var pointIds = BuildPointReadIds(options.Count, options.PointReads, options.PointSeed);
    _ = pointRowByIdQuery(ctx, pointIds[pointIds.Length / 2]) ??
        throw new InvalidOperationException("Warmup point read missed expected row");

    var latencies = RunWithGcDisabled(() =>
    {
        var samples = new double[pointIds.Length];
        for (var i = 0; i < pointIds.Length; i++)
        {
            var started = Stopwatch.GetTimestamp();
            var id = pointIds[i];
            var row = pointRowByIdQuery(ctx, id);
            if (row == null)
            {
                throw new InvalidOperationException($"Point read missed id={id}");
            }

            _ = row.Id;
            _ = row.Val;
            _ = row.F;
            samples[i] = Stopwatch.GetElapsedTime(started).TotalMilliseconds;
        }

        return samples;
    });
    Array.Sort(latencies);
    var pointP50Ms = PercentileSorted(latencies, 50);
    var pointP95Ms = PercentileSorted(latencies, 95);
    Console.WriteLine($"Random point reads by id ({options.PointReads:N0}, seed={options.PointSeed}): p50={pointP50Ms:0.000000}ms p95={pointP95Ms:0.000000}ms");

    if (offering.IsSqlite())
    {
        ctx.Database.ExecuteSqlRaw("PRAGMA wal_checkpoint(TRUNCATE)");
    }

    ctx.Database.CloseConnection();
    ctx.Dispose();
    if (!options.KeepDb)
    {
        DeleteDbFiles(dbPath);
    }

    return new OfferingResult(offering, insertSeconds, insertRowsPerSecond, fetchallSeconds, fetchmanySeconds, pointP50Ms, pointP95Ms);
}

static BenchEfContext CreateEfContext(OfferingKind offering, string dbPath)
{
    var builder = new DbContextOptionsBuilder<BenchEfContext>();
    if (offering.IsSqlite())
    {
        builder.UseSqlite($"Data Source={dbPath}");
    }
    else
    {
        builder.UseDecentDB($"Data Source={dbPath}");
    }

    return new BenchEfContext(builder.Options);
}

static void SetupSchemaEf(BenchEfContext context, bool isSqlite)
{
    if (isSqlite)
    {
        context.Database.ExecuteSqlRaw("PRAGMA journal_mode=WAL");
        context.Database.ExecuteSqlRaw("PRAGMA synchronous=FULL");
        context.Database.ExecuteSqlRaw("PRAGMA wal_autocheckpoint=0");
        context.Database.ExecuteSqlRaw("CREATE TABLE bench (id INTEGER, val TEXT, f REAL)");
    }
    else
    {
        context.Database.ExecuteSqlRaw("CREATE TABLE bench (id INT64, val TEXT, f FLOAT64)");
    }

    context.Database.ExecuteSqlRaw("CREATE INDEX bench_id_idx ON bench(id)");
}

static DbConnection OpenAdoConnection(OfferingKind offering, string dbPath)
{
    DbConnection conn = offering.IsSqlite()
        ? new SqliteConnection($"Data Source={dbPath}")
        : new DecentDBConnection($"Data Source={dbPath}");
    conn.Open();
    return conn;
}

static void SetupSchema(DbConnection conn, bool isSqlite)
{
    if (isSqlite)
    {
        ExecNonQuery(conn, "PRAGMA journal_mode=WAL");
        ExecNonQuery(conn, "PRAGMA synchronous=FULL");
        ExecNonQuery(conn, "PRAGMA wal_autocheckpoint=0");
        ExecNonQuery(conn, "CREATE TABLE bench (id INTEGER, val TEXT, f REAL)");
    }
    else
    {
        ExecNonQuery(conn, "CREATE TABLE bench (id INT64, val TEXT, f FLOAT64)");
    }

    ExecNonQuery(conn, "CREATE INDEX bench_id_idx ON bench(id)");
}

static void WarmInsertPrepared(DbConnection conn)
{
    using var tx = conn.BeginTransaction();
    using var cmd = CreateInsertCommand(conn, tx);
    BindInsertRow(cmd, -1, "__warm__", -1.0);
    cmd.ExecuteNonQuery();
    tx.Rollback();
}

static DbCommand CreateInsertCommand(DbConnection conn, DbTransaction tx)
{
    var cmd = conn.CreateCommand();
    cmd.Transaction = tx;
    cmd.CommandText = "INSERT INTO bench VALUES (@id, @val, @f)";

    var pId = cmd.CreateParameter();
    pId.ParameterName = "@id";
    pId.DbType = DbType.Int64;
    cmd.Parameters.Add(pId);

    var pVal = cmd.CreateParameter();
    pVal.ParameterName = "@val";
    pVal.DbType = DbType.String;
    cmd.Parameters.Add(pVal);

    var pF = cmd.CreateParameter();
    pF.ParameterName = "@f";
    pF.DbType = DbType.Double;
    cmd.Parameters.Add(pF);

    TryPrepare(cmd);
    return cmd;
}

static void BindInsertRow(DbCommand cmd, long id, string value, double f)
{
    cmd.Parameters[0].Value = id;
    cmd.Parameters[1].Value = value;
    cmd.Parameters[2].Value = f;
}

static List<(long Id, string Value, double F)> ReadAllRows(DbCommand scanCmd, int expectedCount)
{
    using var reader = scanCmd.ExecuteReader();
    var rows = new List<(long Id, string Value, double F)>(expectedCount);
    while (reader.Read())
    {
        rows.Add((reader.GetInt64(0), reader.GetString(1), reader.GetDouble(2)));
    }

    return rows;
}

static int ReadRowsInBatches(DbCommand scanCmd, int batchSize)
{
    using var reader = scanCmd.ExecuteReader();
    var total = 0;
    var batch = new List<(long Id, string Value, double F)>(batchSize);
    while (reader.Read())
    {
        batch.Add((reader.GetInt64(0), reader.GetString(1), reader.GetDouble(2)));
        if (batch.Count >= batchSize)
        {
            total += batch.Count;
            batch.Clear();
        }
    }

    total += batch.Count;
    return total;
}

static int ConsumeSyncRowsInBatches<T>(IEnumerable<T> rows, int batchSize)
{
    var total = 0;
    var batch = 0;
    foreach (var row in rows)
    {
        _ = row;
        batch++;
        if (batch >= batchSize)
        {
            total += batch;
            batch = 0;
        }
    }

    total += batch;
    return total;
}

static int ConsumeAsyncRowsInBatches<T>(IAsyncEnumerable<T> rows, int batchSize)
{
    var total = 0;
    var batch = 0;
    var enumerator = rows.GetAsyncEnumerator();
    try
    {
        while (enumerator.MoveNextAsync().AsTask().GetAwaiter().GetResult())
        {
            _ = enumerator.Current;
            batch++;
            if (batch >= batchSize)
            {
                total += batch;
                batch = 0;
            }
        }
    }
    finally
    {
        enumerator.DisposeAsync().AsTask().GetAwaiter().GetResult();
    }

    total += batch;
    return total;
}

static void WarmScan(DbCommand scanCmd)
{
    using var reader = scanCmd.ExecuteReader();
    _ = reader.Read();
}

static void TryPrepare(DbCommand command)
{
    try
    {
        command.Prepare();
    }
    catch (NotSupportedException)
    {
        // Some providers may not support explicit prepare.
    }
}

static void ExecNonQuery(DbConnection conn, string sql)
{
    using var cmd = conn.CreateCommand();
    cmd.CommandText = sql;
    cmd.ExecuteNonQuery();
}

static IEnumerable<DapperBenchRow> BuildDapperRows(int count)
{
    for (var i = 0; i < count; i++)
    {
        yield return new DapperBenchRow
        {
            Id = i,
            Val = $"value_{i}",
            F = i,
        };
    }
}

static IEnumerable<MicroOrmBenchRow> BuildMicroOrmRows(int count)
{
    for (var i = 0; i < count; i++)
    {
        yield return new MicroOrmBenchRow
        {
            Id = i,
            Val = $"value_{i}",
            F = i,
        };
    }
}

static int[] BuildPointReadIds(int rowCount, int pointReads, int seed)
{
    var rng = new Random(seed);
    if (pointReads <= rowCount)
    {
        var ids = new int[rowCount];
        for (var i = 0; i < rowCount; i++)
        {
            ids[i] = i;
        }

        for (var i = 0; i < pointReads; i++)
        {
            var j = rng.Next(i, rowCount);
            (ids[i], ids[j]) = (ids[j], ids[i]);
        }

        var sampled = new int[pointReads];
        Array.Copy(ids, sampled, pointReads);
        return sampled;
    }

    var outIds = new int[pointReads];
    for (var i = 0; i < pointReads; i++)
    {
        outIds[i] = rng.Next(rowCount);
    }

    return outIds;
}

static double PercentileSorted(double[] sorted, int pct)
{
    if (sorted.Length == 0)
    {
        return 0.0;
    }

    var idx = (int)Math.Round((pct / 100.0) * (sorted.Length - 1));
    idx = Math.Clamp(idx, 0, sorted.Length - 1);
    return sorted[idx];
}

static void PrintPairedComparisons(IReadOnlyDictionary<OfferingKind, OfferingResult> results)
{
    var pairs = new[]
    {
        new PairedComparison(OfferingKind.DecentDbAdo, OfferingKind.SqliteAdo, "ADO.NET"),
        new PairedComparison(OfferingKind.DecentDbDapper, OfferingKind.SqliteDapper, "Dapper"),
        new PairedComparison(OfferingKind.DecentDbEfCore, OfferingKind.SqliteEfCore, "EF Core"),
        new PairedComparison(OfferingKind.DecentDbMicroOrm, OfferingKind.SqliteAdo, "MicroOrm vs SQLite ADO.NET"),
    };

    foreach (var pair in pairs)
    {
        if (!results.TryGetValue(pair.DecentDb, out var decent) || !results.TryGetValue(pair.Sqlite, out var sqlite))
        {
            continue;
        }

        Console.WriteLine();
        Console.WriteLine($"=== DecentDB vs SQLite ({pair.Label}) ===");

        var decentBetter = new List<string>();
        var sqliteBetter = new List<string>();
        AppendMetricResult("Insert throughput", decent.InsertRowsPerSecond, sqlite.InsertRowsPerSecond, " rows/s", true, "0.00", decentBetter, sqliteBetter);
        AppendMetricResult("Fetchall time", decent.FetchallSeconds, sqlite.FetchallSeconds, "s", false, "0.000000", decentBetter, sqliteBetter);
        AppendMetricResult("Fetchmany time", decent.FetchmanySeconds, sqlite.FetchmanySeconds, "s", false, "0.000000", decentBetter, sqliteBetter);
        AppendMetricResult("Point read p50", decent.PointP50Ms, sqlite.PointP50Ms, "ms", false, "0.000000", decentBetter, sqliteBetter);
        AppendMetricResult("Point read p95", decent.PointP95Ms, sqlite.PointP95Ms, "ms", false, "0.000000", decentBetter, sqliteBetter);

        Console.WriteLine("DecentDB better at:");
        if (decentBetter.Count == 0)
        {
            Console.WriteLine("- none");
        }
        else
        {
            foreach (var line in decentBetter)
            {
                Console.WriteLine($"- {line}");
            }
        }

        Console.WriteLine("SQLite better at:");
        if (sqliteBetter.Count == 0)
        {
            Console.WriteLine("- none");
        }
        else
        {
            foreach (var line in sqliteBetter)
            {
                Console.WriteLine($"- {line}");
            }
        }
    }
}

static void PrintOverallRanking(IReadOnlyDictionary<OfferingKind, OfferingResult> results)
{
    if (results.Count == 0)
    {
        return;
    }

    Console.WriteLine();
    Console.WriteLine("=== Overall Ranking Across Offerings ===");
    PrintMetricRanking(results, "Insert throughput", r => r.InsertRowsPerSecond, " rows/s", true, "0.00");
    PrintMetricRanking(results, "Fetchall time", r => r.FetchallSeconds, "s", false, "0.000000");
    PrintMetricRanking(results, "Fetchmany time", r => r.FetchmanySeconds, "s", false, "0.000000");
    PrintMetricRanking(results, "Point read p50", r => r.PointP50Ms, "ms", false, "0.000000");
    PrintMetricRanking(results, "Point read p95", r => r.PointP95Ms, "ms", false, "0.000000");
}

static void PrintMetricRanking(
    IReadOnlyDictionary<OfferingKind, OfferingResult> results,
    string metricName,
    Func<OfferingResult, double> selector,
    string unit,
    bool higherIsBetter,
    string format)
{
    var ordered = higherIsBetter
        ? results.Values.OrderByDescending(selector).ToList()
        : results.Values.OrderBy(selector).ToList();
    var best = ordered[0];
    var bestVal = selector(best);

    Console.WriteLine($"{metricName}:");
    Console.WriteLine($"- Fastest: {best.Offering.ToDisplayName()} ({bestVal.ToString(format, CultureInfo.InvariantCulture)}{unit})");
    for (var i = 0; i < ordered.Count; i++)
    {
        var current = ordered[i];
        var value = selector(current);
        if (i == 0)
        {
            Console.WriteLine($"- #{i + 1}: {current.Offering.ToDisplayName()} (baseline)");
            continue;
        }

        var ratio = higherIsBetter
            ? (value == 0 ? double.PositiveInfinity : bestVal / value)
            : (bestVal == 0 ? double.PositiveInfinity : value / bestVal);
        Console.WriteLine($"- #{i + 1}: {current.Offering.ToDisplayName()} ({ratio:0.000}x off fastest)");
    }
}

static void AppendMetricResult(
    string metricName,
    double decent,
    double sqlite,
    string unit,
    bool higherIsBetter,
    string format,
    List<string> decentBetter,
    List<string> sqliteBetter)
{
    bool decentWins;
    double winner;
    double loser;
    string detail;
    if (higherIsBetter)
    {
        decentWins = decent > sqlite;
        winner = decentWins ? decent : sqlite;
        loser = decentWins ? sqlite : decent;
        var ratio = loser == 0 ? double.PositiveInfinity : winner / loser;
        detail =
            $"{metricName}: {winner.ToString(format, CultureInfo.InvariantCulture)}{unit} vs {loser.ToString(format, CultureInfo.InvariantCulture)}{unit} " +
            $"({ratio:0.000}x higher)";
    }
    else
    {
        decentWins = decent < sqlite;
        winner = decentWins ? decent : sqlite;
        loser = decentWins ? sqlite : decent;
        var ratio = winner == 0 ? double.PositiveInfinity : loser / winner;
        detail =
            $"{metricName}: {winner.ToString(format, CultureInfo.InvariantCulture)}{unit} vs {loser.ToString(format, CultureInfo.InvariantCulture)}{unit} " +
            $"({ratio:0.000}x faster/lower)";
    }

    if (decentWins)
    {
        decentBetter.Add(detail);
    }
    else
    {
        sqliteBetter.Add(detail);
    }
}

static T RunWithGcDisabled<T>(Func<T> action)
{
    var gcWasEnabled = GCSettings.LatencyMode != GCLatencyMode.NoGCRegion;
    var shouldReEnable = false;
    if (gcWasEnabled)
    {
        GC.Collect();
        GC.WaitForPendingFinalizers();
        GC.Collect();
        if (GC.TryStartNoGCRegion(1024L * 1024L * 256L))
        {
            shouldReEnable = true;
        }
    }

    try
    {
        return action();
    }
    finally
    {
        if (shouldReEnable)
        {
            try
            {
                GC.EndNoGCRegion();
            }
            catch (InvalidOperationException)
            {
                // The runtime can leave no-GC mode early if allocation budget is exceeded.
            }
        }
    }
}

static IReadOnlyList<OfferingKind> ResolveOfferings(string engine)
{
    return engine switch
    {
        "decentdb" => new[]
        {
            OfferingKind.DecentDbAdo,
            OfferingKind.DecentDbMicroOrm,
            OfferingKind.DecentDbDapper,
            OfferingKind.DecentDbEfCore,
        },
        "sqlite" => new[]
        {
            OfferingKind.SqliteAdo,
            OfferingKind.SqliteDapper,
            OfferingKind.SqliteEfCore,
        },
        _ => new[]
        {
            OfferingKind.DecentDbAdo,
            OfferingKind.DecentDbMicroOrm,
            OfferingKind.DecentDbDapper,
            OfferingKind.DecentDbEfCore,
            OfferingKind.SqliteAdo,
            OfferingKind.SqliteDapper,
            OfferingKind.SqliteEfCore,
        },
    };
}

static BenchmarkOptions ParseArgs(string[] args)
{
    var options = new BenchmarkOptions();
    for (var i = 0; i < args.Length; i++)
    {
        switch (args[i])
        {
            case "--help":
            case "-h":
                options.ShowHelp = true;
                break;
            case "--engine":
                options.Engine = NextArg(args, ref i, "--engine");
                break;
            case "--count":
                options.Count = ParseInt(NextArg(args, ref i, "--count"), "--count");
                break;
            case "--fetchmany-batch":
                options.FetchmanyBatch = ParseInt(NextArg(args, ref i, "--fetchmany-batch"), "--fetchmany-batch");
                break;
            case "--point-reads":
                options.PointReads = ParseInt(NextArg(args, ref i, "--point-reads"), "--point-reads");
                break;
            case "--point-seed":
                options.PointSeed = ParseInt(NextArg(args, ref i, "--point-seed"), "--point-seed");
                break;
            case "--ef-insert-batch":
                options.EfInsertBatch = ParseInt(NextArg(args, ref i, "--ef-insert-batch"), "--ef-insert-batch");
                break;
            case "--db-prefix":
                options.DbPrefix = NextArg(args, ref i, "--db-prefix");
                break;
            case "--keep-db":
                options.KeepDb = true;
                break;
            default:
                throw new ArgumentException($"Unknown argument: {args[i]}");
        }
    }

    if (options.Count <= 0) throw new ArgumentException("--count must be > 0");
    if (options.FetchmanyBatch <= 0) throw new ArgumentException("--fetchmany-batch must be > 0");
    if (options.PointReads <= 0) throw new ArgumentException("--point-reads must be > 0");
    if (options.EfInsertBatch <= 0) throw new ArgumentException("--ef-insert-batch must be > 0");
    if (options.Engine is not ("all" or "decentdb" or "sqlite"))
    {
        throw new ArgumentException("--engine must be one of: all, decentdb, sqlite");
    }

    return options;

    static string NextArg(string[] argv, ref int idx, string name)
    {
        if (idx + 1 >= argv.Length)
        {
            throw new ArgumentException($"{name} requires a value");
        }

        idx++;
        return argv[idx];
    }

    static int ParseInt(string value, string name)
    {
        if (!int.TryParse(value, NumberStyles.Integer, CultureInfo.InvariantCulture, out var parsed))
        {
            throw new ArgumentException($"{name} must be an integer");
        }

        return parsed;
    }
}

static void PrintUsage()
{
    Console.WriteLine("Multi-offering .NET benchmark: DecentDB vs SQLite across ADO.NET, Dapper, EF Core, and DecentDB.MicroOrm");
    Console.WriteLine("Usage:");
    Console.WriteLine("  dotnet run -c Release --project bindings/dotnet/benchmarks/DecentDB.Benchmarks/DecentDB.Benchmarks.csproj -- [options]");
    Console.WriteLine();
    Console.WriteLine("Options:");
    Console.WriteLine("  --engine <all|decentdb|sqlite>   Offerings to run (default: all)");
    Console.WriteLine($"  --count <n>                      Rows to insert/fetch (default: {DefaultCount})");
    Console.WriteLine($"  --fetchmany-batch <n>            Batch size for fetchmany metric (default: {DefaultFetchmanyBatch})");
    Console.WriteLine($"  --point-reads <n>                Random indexed point lookups (default: {DefaultPointReads})");
    Console.WriteLine($"  --point-seed <n>                 RNG seed for point lookups (default: {DefaultPointSeed})");
    Console.WriteLine($"  --ef-insert-batch <n>            EF SaveChanges batch size (default: {DefaultEfInsertBatch})");
    Console.WriteLine("  --db-prefix <path_prefix>        Database path prefix (default: dotnet_bench_fetch)");
    Console.WriteLine("                                  DecentDB files use .ddb by default; SQLite files use .db.");
    Console.WriteLine("  --keep-db                        Keep generated DB files");
    Console.WriteLine("  -h, --help                       Show help");
}

static void DeleteDbFiles(string dbPath)
{
    TryDelete(dbPath);
    TryDelete(dbPath + ".wal");
    TryDelete(dbPath + "-wal");
    TryDelete(dbPath + "-shm");

    static void TryDelete(string path)
    {
        try
        {
            if (File.Exists(path))
            {
                File.Delete(path);
            }
        }
        catch
        {
            // Best effort cleanup.
        }
    }
}

readonly record struct PairedComparison(OfferingKind DecentDb, OfferingKind Sqlite, string Label);

readonly record struct OfferingResult(
    OfferingKind Offering,
    double InsertSeconds,
    double InsertRowsPerSecond,
    double FetchallSeconds,
    double FetchmanySeconds,
    double PointP50Ms,
    double PointP95Ms);

enum OfferingKind
{
    DecentDbAdo,
    DecentDbMicroOrm,
    DecentDbDapper,
    DecentDbEfCore,
    SqliteAdo,
    SqliteDapper,
    SqliteEfCore,
}

static class OfferingKindExtensions
{
    public static bool IsSqlite(this OfferingKind offering) => offering switch
    {
        OfferingKind.SqliteAdo or OfferingKind.SqliteDapper or OfferingKind.SqliteEfCore => true,
        _ => false,
    };

    public static string ToToken(this OfferingKind offering) => offering switch
    {
        OfferingKind.DecentDbAdo => "decentdb_ado",
        OfferingKind.DecentDbMicroOrm => "decentdb_microorm",
        OfferingKind.DecentDbDapper => "decentdb_dapper",
        OfferingKind.DecentDbEfCore => "decentdb_efcore",
        OfferingKind.SqliteAdo => "sqlite_ado",
        OfferingKind.SqliteDapper => "sqlite_dapper",
        OfferingKind.SqliteEfCore => "sqlite_efcore",
        _ => offering.ToString().ToLowerInvariant(),
    };

    public static string ToDisplayName(this OfferingKind offering) => offering switch
    {
        OfferingKind.DecentDbAdo => "decentdb + ADO.NET",
        OfferingKind.DecentDbMicroOrm => "decentdb + MicroOrm",
        OfferingKind.DecentDbDapper => "decentdb + Dapper",
        OfferingKind.DecentDbEfCore => "decentdb + EF Core",
        OfferingKind.SqliteAdo => "sqlite + ADO.NET",
        OfferingKind.SqliteDapper => "sqlite + Dapper",
        OfferingKind.SqliteEfCore => "sqlite + EF Core",
        _ => offering.ToString().ToLowerInvariant(),
    };
}

static class BenchmarkDefaults
{
    public const int Count = 1_000_000;
    public const int PointReads = 10_000;
    public const int PointSeed = 1337;
    public const int FetchmanyBatch = 4096;
    public const int EfInsertBatch = 2000;
}

sealed class BenchmarkOptions
{
    public string Engine { get; set; } = "all";
    public int Count { get; set; } = BenchmarkDefaults.Count;
    public int FetchmanyBatch { get; set; } = BenchmarkDefaults.FetchmanyBatch;
    public int PointReads { get; set; } = BenchmarkDefaults.PointReads;
    public int PointSeed { get; set; } = BenchmarkDefaults.PointSeed;
    public int EfInsertBatch { get; set; } = BenchmarkDefaults.EfInsertBatch;
    public string DbPrefix { get; set; } = "dotnet_bench_fetch";
    public bool KeepDb { get; set; }
    public bool ShowHelp { get; set; }
}

sealed class DapperBenchRow
{
    public long Id { get; set; }
    public string Val { get; set; } = string.Empty;
    public double F { get; set; }
}

sealed class DapperPointParam
{
    public long Id { get; set; }
}

[DecentDB.MicroOrm.Table("bench")]
sealed class MicroOrmBenchRow
{
    [DecentDB.MicroOrm.PrimaryKey]
    [DecentDB.MicroOrm.Column("id")]
    public long Id { get; set; }

    [DecentDB.MicroOrm.Column("val")]
    public string Val { get; set; } = string.Empty;

    [DecentDB.MicroOrm.Column("f")]
    public double F { get; set; }
}

sealed class BenchEfRow
{
    public long Id { get; set; }
    public string Val { get; set; } = string.Empty;
    public double F { get; set; }
}

sealed class BenchEfContext : DbContext
{
    public BenchEfContext(DbContextOptions<BenchEfContext> options)
        : base(options)
    {
    }

    public Microsoft.EntityFrameworkCore.DbSet<BenchEfRow> BenchRows => Set<BenchEfRow>();

    protected override void OnModelCreating(ModelBuilder modelBuilder)
    {
        modelBuilder.Entity<BenchEfRow>(entity =>
        {
            entity.ToTable("bench");
            entity.HasKey(r => r.Id);
            entity.Property(r => r.Id).HasColumnName("id").ValueGeneratedNever();
            entity.Property(r => r.Val).HasColumnName("val");
            entity.Property(r => r.F).HasColumnName("f");
        });
    }
}
