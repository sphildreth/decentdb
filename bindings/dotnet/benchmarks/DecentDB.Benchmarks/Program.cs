using System.Data;
using System.Data.Common;
using System.Diagnostics;
using System.Globalization;
using System.Runtime;
using DecentDB.AdoNet;
using Microsoft.Data.Sqlite;

const int DefaultCount = 1_000_000;
const int DefaultPointReads = 10_000;
const int DefaultPointSeed = 1337;
const int DefaultFetchmanyBatch = 4096;

var options = ParseArgs(args);
if (options.ShowHelp)
{
    PrintUsage();
    return;
}

var results = new Dictionary<EngineKind, EngineResult>();
var engines = options.Engine switch
{
    "decentdb" => new[] { EngineKind.DecentDb },
    "sqlite" => new[] { EngineKind.Sqlite },
    _ => new[] { EngineKind.DecentDb, EngineKind.Sqlite },
};

foreach (var engine in engines)
{
    var ext = engine == EngineKind.DecentDb ? "ddb" : "db";
    var dbPath = $"{options.DbPrefix}_{engine.ToString().ToLowerInvariant()}.{ext}";
    results[engine] = RunEngineBenchmark(engine, dbPath, options);
}

PrintComparison(results);

static EngineResult RunEngineBenchmark(EngineKind engine, string dbPath, BenchmarkOptions options)
{
    DeleteDbFiles(dbPath);
    Console.WriteLine();
    Console.WriteLine($"=== {engine.ToDisplayName()} ===");
    Console.WriteLine("Setting up data...");

    using var conn = OpenConnection(engine, dbPath);
    SetupSchema(conn, engine);

    WarmInsertPrepared(conn);
    var insertSeconds = RunWithGcDisabled(() =>
    {
        var started = Stopwatch.GetTimestamp();
        using var tx = conn.BeginTransaction();
        using var cmd = CreateInsertCommand(conn, tx);
        for (var i = 0; i < options.Count; i++)
        {
            BindInsertRow(cmd, i, $"value_{i}", (double)i);
            cmd.ExecuteNonQuery();
        }

        tx.Commit();
        return Stopwatch.GetElapsedTime(started).TotalSeconds;
    });
    var insertRowsPerSecond = options.Count / insertSeconds;
    Console.WriteLine(
        $"Insert {options.Count:N0} rows: {insertSeconds:0.0000}s ({insertRowsPerSecond:N2} rows/sec)");

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
            throw new InvalidOperationException(
                $"Expected {options.Count} rows from fetchall, got {rows.Count}");
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
            throw new InvalidOperationException(
                $"Expected {options.Count} rows from fetchmany, got {total}");
        }

        return Stopwatch.GetElapsedTime(started).TotalSeconds;
    });
    Console.WriteLine(
        $"Fetchmany({options.FetchmanyBatch:N0}) {options.Count:N0} rows: {fetchmanySeconds:0.0000}s");

    using var pointCmd = conn.CreateCommand();
    pointCmd.CommandText = "SELECT id, val, f FROM bench WHERE id = @id";
    var pointIdParam = pointCmd.CreateParameter();
    pointIdParam.ParameterName = "@id";
    pointIdParam.DbType = DbType.Int64;
    pointCmd.Parameters.Add(pointIdParam);
    TryPrepare(pointCmd);

    var pointIds = BuildPointReadIds(options.Count, options.PointReads, options.PointSeed);
    var warmupId = pointIds[pointIds.Length / 2];
    pointIdParam.Value = warmupId;
    using (var warmReader = pointCmd.ExecuteReader())
    {
        if (!warmReader.Read())
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
    Console.WriteLine(
        $"Random point reads by id ({options.PointReads:N0}, seed={options.PointSeed}): " +
        $"p50={pointP50Ms:0.000000}ms p95={pointP95Ms:0.000000}ms");

    if (engine == EngineKind.Sqlite)
    {
        using var checkpoint = conn.CreateCommand();
        checkpoint.CommandText = "PRAGMA wal_checkpoint(TRUNCATE)";
        checkpoint.ExecuteNonQuery();
    }

    conn.Close();
    if (!options.KeepDb)
    {
        DeleteDbFiles(dbPath);
    }

    return new EngineResult(
        InsertSeconds: insertSeconds,
        InsertRowsPerSecond: insertRowsPerSecond,
        FetchallSeconds: fetchallSeconds,
        FetchmanySeconds: fetchmanySeconds,
        PointP50Ms: pointP50Ms,
        PointP95Ms: pointP95Ms);
}

static void SetupSchema(DbConnection conn, EngineKind engine)
{
    if (engine == EngineKind.Sqlite)
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

static DbConnection OpenConnection(EngineKind engine, string dbPath)
{
    DbConnection conn = engine switch
    {
        EngineKind.DecentDb => new DecentDBConnection($"Data Source={dbPath}"),
        EngineKind.Sqlite => new SqliteConnection($"Data Source={dbPath}"),
        _ => throw new ArgumentOutOfRangeException(nameof(engine), engine, "Unknown engine"),
    };
    conn.Open();
    return conn;
}

static void ExecNonQuery(DbConnection conn, string sql)
{
    using var cmd = conn.CreateCommand();
    cmd.CommandText = sql;
    cmd.ExecuteNonQuery();
}

static void PrintComparison(IReadOnlyDictionary<EngineKind, EngineResult> results)
{
    if (!results.TryGetValue(EngineKind.DecentDb, out var decent) ||
        !results.TryGetValue(EngineKind.Sqlite, out var sqlite))
    {
        return;
    }

    var metrics = new[]
    {
        new Metric("Insert throughput", decent.InsertRowsPerSecond, sqlite.InsertRowsPerSecond, " rows/s", true, "0.00"),
        new Metric("Fetchall time", decent.FetchallSeconds, sqlite.FetchallSeconds, "s", false, "0.000000"),
        new Metric("Fetchmany time", decent.FetchmanySeconds, sqlite.FetchmanySeconds, "s", false, "0.000000"),
        new Metric("Point read p50", decent.PointP50Ms, sqlite.PointP50Ms, "ms", false, "0.000000"),
        new Metric("Point read p95", decent.PointP95Ms, sqlite.PointP95Ms, "ms", false, "0.000000"),
    };

    var decentBetter = new List<string>();
    var sqliteBetter = new List<string>();
    var ties = new List<string>();

    foreach (var metric in metrics)
    {
        if (metric.Decent == metric.Sqlite)
        {
            ties.Add($"{metric.Name}: tie ({metric.Decent.ToString(metric.Format, CultureInfo.InvariantCulture)}{metric.Unit})");
            continue;
        }

        string detail;
        bool decentWins;
        if (metric.HigherIsBetter)
        {
            decentWins = metric.Decent > metric.Sqlite;
            var winner = decentWins ? metric.Decent : metric.Sqlite;
            var loser = decentWins ? metric.Sqlite : metric.Decent;
            var ratio = loser == 0 ? double.PositiveInfinity : winner / loser;
            detail =
                $"{metric.Name}: " +
                $"{winner.ToString(metric.Format, CultureInfo.InvariantCulture)}{metric.Unit} vs " +
                $"{loser.ToString(metric.Format, CultureInfo.InvariantCulture)}{metric.Unit} " +
                $"({ratio:0.000}x higher)";
        }
        else
        {
            decentWins = metric.Decent < metric.Sqlite;
            var winner = decentWins ? metric.Decent : metric.Sqlite;
            var loser = decentWins ? metric.Sqlite : metric.Decent;
            var ratio = winner == 0 ? double.PositiveInfinity : loser / winner;
            detail =
                $"{metric.Name}: " +
                $"{winner.ToString(metric.Format, CultureInfo.InvariantCulture)}{metric.Unit} vs " +
                $"{loser.ToString(metric.Format, CultureInfo.InvariantCulture)}{metric.Unit} " +
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

    Console.WriteLine();
    Console.WriteLine("=== Comparison (DecentDB vs SQLite) ===");
    Console.WriteLine("DecentDB better at:");
    if (decentBetter.Count == 0)
    {
        Console.WriteLine("- none");
    }
    else
    {
        foreach (var detail in decentBetter)
        {
            Console.WriteLine($"- {detail}");
        }
    }

    Console.WriteLine("SQLite better at:");
    if (sqliteBetter.Count == 0)
    {
        Console.WriteLine("- none");
    }
    else
    {
        foreach (var detail in sqliteBetter)
        {
            Console.WriteLine($"- {detail}");
        }
    }

    if (ties.Count > 0)
    {
        Console.WriteLine("Ties:");
        foreach (var tie in ties)
        {
            Console.WriteLine($"- {tie}");
        }
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
            GC.EndNoGCRegion();
        }
    }
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
    Console.WriteLine("Fair ADO.NET comparison benchmark: DecentDB vs SQLite");
    Console.WriteLine("Usage:");
    Console.WriteLine("  dotnet run -c Release --project bindings/dotnet/benchmarks/DecentDB.Benchmarks/DecentDB.Benchmarks.csproj -- [options]");
    Console.WriteLine();
    Console.WriteLine("Options:");
    Console.WriteLine($"  --engine <all|decentdb|sqlite>   Engine(s) to run (default: all)");
    Console.WriteLine($"  --count <n>                      Rows to insert/fetch (default: {DefaultCount})");
    Console.WriteLine($"  --fetchmany-batch <n>            Batch size for fetchmany (default: {DefaultFetchmanyBatch})");
    Console.WriteLine($"  --point-reads <n>                Random indexed point lookups (default: {DefaultPointReads})");
    Console.WriteLine($"  --point-seed <n>                 RNG seed for point lookups (default: {DefaultPointSeed})");
    Console.WriteLine("  --db-prefix <path_prefix>        Database path prefix (default: dotnet_bench_fetch)");
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

readonly record struct Metric(
    string Name,
    double Decent,
    double Sqlite,
    string Unit,
    bool HigherIsBetter,
    string Format);

readonly record struct EngineResult(
    double InsertSeconds,
    double InsertRowsPerSecond,
    double FetchallSeconds,
    double FetchmanySeconds,
    double PointP50Ms,
    double PointP95Ms);

enum EngineKind
{
    DecentDb,
    Sqlite,
}

static class EngineKindExtensions
{
    public static string ToDisplayName(this EngineKind engine) => engine switch
    {
        EngineKind.DecentDb => "decentdb",
        EngineKind.Sqlite => "sqlite",
        _ => engine.ToString().ToLowerInvariant(),
    };
}

sealed class BenchmarkOptions
{
    public string Engine { get; set; } = "all";
    public int Count { get; set; } = 1_000_000;
    public int FetchmanyBatch { get; set; } = 4096;
    public int PointReads { get; set; } = 10_000;
    public int PointSeed { get; set; } = 1337;
    public string DbPrefix { get; set; } = "dotnet_bench_fetch";
    public bool KeepDb { get; set; }
    public bool ShowHelp { get; set; }
}
