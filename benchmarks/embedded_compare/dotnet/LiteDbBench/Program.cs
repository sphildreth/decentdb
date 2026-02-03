using System.Diagnostics;
using System.Text.Json;
using LiteDB;

static long NowNs()
{
    // Convert Stopwatch ticks to ns.
    // tick_ns = 1e9 / Stopwatch.Frequency
    var ts = Stopwatch.GetTimestamp();
    return (long)(ts * (1_000_000_000.0 / Stopwatch.Frequency));
}

static double PercentileSorted(double[] sorted, int pct)
{
    if (sorted.Length == 0) return 0;
    var idx = (int)Math.Round((pct / 100.0) * (sorted.Length - 1));
    if (idx < 0) idx = 0;
    if (idx >= sorted.Length) idx = sorted.Length - 1;
    return sorted[idx];
}

static int[] LcgIds(int n, int modulo, uint seed = 0xC0FFEEu)
{
    const uint a = 1664525;
    const uint c = 1013904223;
    uint x = seed;
    var outIds = new int[n];
    for (var i = 0; i < n; i++)
    {
        x = unchecked(a * x + c);
        outIds[i] = (int)(x % (uint)modulo) + 1;
    }
    return outIds;
}

static void TryDelete(string path)
{
    try
    {
        if (File.Exists(path)) File.Delete(path);
    }
    catch
    {
        // best effort
    }
}

static void DeleteDbFiles(string dbPath)
{
    TryDelete(dbPath);
    TryDelete(dbPath + "-wal");
    TryDelete(dbPath + "-shm");
    TryDelete(dbPath + "-journal");
}

static Dictionary<string, object?> BuildManifest(Dictionary<string, object?> extra)
{
    var manifest = new Dictionary<string, object?>
    {
        ["timestamp_utc"] = DateTime.UtcNow.ToString("yyyy-MM-ddTHH:mm:ssZ"),
        ["platform"] = new Dictionary<string, object?>
        {
            ["dotnet"] = Environment.Version.ToString(),
            ["os"] = System.Runtime.InteropServices.RuntimeInformation.OSDescription,
            ["arch"] = System.Runtime.InteropServices.RuntimeInformation.OSArchitecture.ToString(),
        },
        ["deps"] = new Dictionary<string, object?>
        {
            ["litedb"] = typeof(LiteDatabase).Assembly.GetName().Version?.ToString(),
        },
    };

    foreach (var kv in extra)
        manifest[kv.Key] = kv.Value;

    return manifest;
}

static Dictionary<string, object?> RunBench(
    string dbDir,
    string bench,
    int nOps,
    int iterations,
    int warmup)
{
    var dbPath = Path.Combine(dbDir, $"litedb_{bench}_{nOps}.db");

    void Seed(LiteDatabase db, int rows)
    {
        var col = db.GetCollection<BsonDocument>("kv");
        col.EnsureIndex("_id", unique: true);

        db.BeginTrans();
        for (var i = 1; i <= rows; i++)
        {
            col.Insert(new BsonDocument
            {
                ["_id"] = i,
                ["v"] = i,
            });
        }
        db.Commit();
    }

    void PointSelect(LiteDatabase db, int[] ids)
    {
        var col = db.GetCollection<BsonDocument>("kv");
        for (var i = 0; i < ids.Length; i++)
        {
            var doc = col.FindById(ids[i]);
            if (doc == null) throw new Exception("missing doc");
            _ = doc["v"].AsInt32;
        }
    }

    void InsertTxn(LiteDatabase db, int startId, int n)
    {
        var col = db.GetCollection<BsonDocument>("kv");
        db.BeginTrans();
        for (var i = 0; i < n; i++)
        {
            var idv = startId + i;
            col.Insert(new BsonDocument
            {
                ["_id"] = idv,
                ["v"] = idv,
            });
        }
        db.Commit();
    }

    // Setup per bench
    if (bench == "point_select")
    {
        DeleteDbFiles(dbPath);
        using var db = new LiteDatabase($"Filename={dbPath};Connection=direct");

        var seedRows = Math.Max(nOps, 100_000);
        Seed(db, seedRows);
        var ids = LcgIds(nOps, seedRows);

        void Action() => PointSelect(db, ids);

        for (var i = 0; i < warmup; i++) Action();

        var samplesNs = new long[iterations];
        for (var i = 0; i < iterations; i++)
        {
            var t0 = NowNs();
            Action();
            var t1 = NowNs();
            samplesNs[i] = t1 - t0;
        }

        var usPerOp = samplesNs.Select(ns => ns / (double)nOps / 1_000.0).OrderBy(x => x).ToArray();
        return new Dictionary<string, object?>
        {
            ["engine"] = "LiteDB",
            ["bench"] = bench,
            ["n_ops"] = nOps,
            ["unit"] = "us/op",
            ["samples_elapsed_ns"] = samplesNs,
            ["p50_us_per_op"] = PercentileSorted(usPerOp, 50),
            ["p95_us_per_op"] = PercentileSorted(usPerOp, 95),
            ["mean_us_per_op"] = usPerOp.Length == 0 ? 0 : usPerOp.Average(),
            ["min_us_per_op"] = usPerOp.Length == 0 ? 0 : usPerOp.First(),
            ["max_us_per_op"] = usPerOp.Length == 0 ? 0 : usPerOp.Last(),
        };
    }

    if (bench == "insert_txn")
    {
        // For insert benchmark, create a fresh DB per iteration.
        var samplesNs = new long[iterations];

        void ActionOnce()
        {
            DeleteDbFiles(dbPath);
            using var db = new LiteDatabase($"Filename={dbPath};Connection=direct");
            InsertTxn(db, startId: 1, n: nOps);
        }

        for (var i = 0; i < warmup; i++) ActionOnce();

        for (var i = 0; i < iterations; i++)
        {
            var t0 = NowNs();
            ActionOnce();
            var t1 = NowNs();
            samplesNs[i] = t1 - t0;
        }

        var usPerOp = samplesNs.Select(ns => ns / (double)nOps / 1_000.0).OrderBy(x => x).ToArray();
        return new Dictionary<string, object?>
        {
            ["engine"] = "LiteDB",
            ["bench"] = bench,
            ["n_ops"] = nOps,
            ["unit"] = "us/op",
            ["samples_elapsed_ns"] = samplesNs,
            ["p50_us_per_op"] = PercentileSorted(usPerOp, 50),
            ["p95_us_per_op"] = PercentileSorted(usPerOp, 95),
            ["mean_us_per_op"] = usPerOp.Length == 0 ? 0 : usPerOp.Average(),
            ["min_us_per_op"] = usPerOp.Length == 0 ? 0 : usPerOp.First(),
            ["max_us_per_op"] = usPerOp.Length == 0 ? 0 : usPerOp.Last(),
        };
    }

    throw new Exception($"unknown bench: {bench}");
}

string? GetArg(string[] args, string name)
{
    for (var i = 0; i < args.Length - 1; i++)
        if (args[i] == name) return args[i + 1];
    return null;
}

var dbDir = GetArg(args, "--db-dir") ?? "/db";
var outPath = GetArg(args, "--out") ?? "/out/results_litedb.json";
var opCountsArg = GetArg(args, "--op-counts") ?? "10000,100000,1000000";
var benchesArg = GetArg(args, "--benches") ?? "point_select,insert_txn";
var iterations = int.TryParse(GetArg(args, "--iterations"), out var it) ? it : 7;
var warmup = int.TryParse(GetArg(args, "--warmup"), out var wu) ? wu : 2;

Directory.CreateDirectory(dbDir);
Directory.CreateDirectory(Path.GetDirectoryName(outPath) ?? ".");

var opCounts = opCountsArg.Split(',').Select(s => int.Parse(s.Trim())).ToArray();
var benches = benchesArg.Split(',').Select(s => s.Trim()).Where(s => s.Length > 0).ToArray();

var results = new List<Dictionary<string, object?>>();
var skipped = new List<Dictionary<string, object?>>();

foreach (var bench in benches)
{
    foreach (var nOps in opCounts)
    {
        try
        {
            var row = RunBench(dbDir, bench, nOps, iterations, warmup);
            results.Add(row);
            Console.WriteLine($"OK LiteDB {bench} n={nOps} p50={row[\"p50_us_per_op\"]:0.000} us/op");
        }
        catch (Exception e)
        {
            skipped.Add(new Dictionary<string, object?>
            {
                ["engine"] = "LiteDB",
                ["bench"] = bench,
                ["n_ops"] = nOps,
                ["error"] = e.Message,
            });
            Console.WriteLine($"SKIP LiteDB {bench} n={nOps}: {e.Message}");
        }
    }
}

var manifest = BuildManifest(new Dictionary<string, object?>
{
    ["bench"] = new Dictionary<string, object?>
    {
        ["op_counts"] = opCounts,
        ["benches"] = benches,
        ["iterations"] = iterations,
        ["warmup"] = warmup,
        ["db_dir"] = dbDir,
    }
});

var payload = new Dictionary<string, object?>
{
    ["manifest"] = manifest,
    ["results"] = results,
    ["skipped"] = skipped,
};

var json = JsonSerializer.Serialize(payload, new JsonSerializerOptions
{
    WriteIndented = true,
});

File.WriteAllText(outPath, json);
