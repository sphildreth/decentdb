using System.Diagnostics;
using DecentDb.AdoNet;
using DecentDb.MicroOrm;

static string MakeTempDbPath()
{
    var path = Path.Combine(Path.GetTempPath(), $"decentdb_dotnet_bench_{Guid.NewGuid():N}.db");
    return path;
}

static void DeleteDbFiles(string dbPath)
{
    TryDelete(dbPath);
    TryDelete(dbPath + "-wal");

    static void TryDelete(string p)
    {
        try
        {
            if (File.Exists(p)) File.Delete(p);
        }
        catch
        {
            // best effort
        }
    }
}

static double MeasureMs(Action action)
{
    var start = Stopwatch.GetTimestamp();
    action();
    var elapsed = Stopwatch.GetElapsedTime(start);
    return elapsed.TotalMilliseconds;
}

static double[] RunBench(string name, int iterations, int warmup, Action action)
{
    for (var i = 0; i < warmup; i++) action();

    var samples = new double[iterations];
    for (var i = 0; i < iterations; i++)
    {
        samples[i] = MeasureMs(action);
    }

    Array.Sort(samples);
    var p50 = PercentileSorted(samples, 50);
    var p95 = PercentileSorted(samples, 95);

    Console.WriteLine($"{name,-28} p50={p50,8:0.000}ms  p95={p95,8:0.000}ms  iters={iterations}");
    return samples;
}

static double PercentileSorted(double[] sorted, int pct)
{
    if (sorted.Length == 0) return 0;
    var idx = (int)Math.Round((pct / 100.0) * (sorted.Length - 1));
    if (idx < 0) idx = 0;
    if (idx >= sorted.Length) idx = sorted.Length - 1;
    return sorted[idx];
}

static void Exec(DecentDbConnection conn, string sql)
{
    using var cmd = conn.CreateCommand();
    cmd.CommandText = sql;
    cmd.ExecuteNonQuery();
}

static void Seed(DecentDbConnection conn, int rows)
{
    Exec(conn, "DROP TABLE IF EXISTS persons");
    Exec(conn, "CREATE TABLE persons (id INTEGER PRIMARY KEY, name TEXT, age INTEGER)");

    using var tx = conn.BeginTransaction();
    using var cmd = conn.CreateCommand();
    cmd.Transaction = tx;
    cmd.CommandText = "INSERT INTO persons (id, name, age) VALUES (@id, @name, @age)";

    var pId = cmd.CreateParameter();
    pId.ParameterName = "@id";
    cmd.Parameters.Add(pId);

    var pName = cmd.CreateParameter();
    pName.ParameterName = "@name";
    cmd.Parameters.Add(pName);

    var pAge = cmd.CreateParameter();
    pAge.ParameterName = "@age";
    cmd.Parameters.Add(pAge);

    for (var i = 1; i <= rows; i++)
    {
        pId.Value = i;
        pName.Value = "person_" + i;
        pAge.Value = i % 100;
        cmd.ExecuteNonQuery();
    }

    tx.Commit();
}

var dbPath = MakeTempDbPath();
try
{
    using var conn = new DecentDbConnection($"Data Source={dbPath}");
    conn.Open();

    Seed(conn, rows: 10_000);

    Console.WriteLine("=== ADO.NET ===");

    using var byId = conn.CreateCommand();
    byId.CommandText = "SELECT id, name, age FROM persons WHERE id = @id";
    var byIdParam = byId.CreateParameter();
    byIdParam.ParameterName = "@id";
    byId.Parameters.Add(byIdParam);

    RunBench("Single record by ID", iterations: 200, warmup: 50, action: () =>
    {
        byIdParam.Value = 1234;
        using var reader = byId.ExecuteReader();
        if (!reader.Read()) throw new InvalidOperationException("missing row");
        _ = reader.GetInt64(0);
        _ = reader.GetString(1);
        _ = reader.GetInt32(2);
    });

    RunBench("Count with filter", iterations: 200, warmup: 50, action: () =>
    {
        using var cmd = conn.CreateCommand();
        cmd.CommandText = "SELECT COUNT(*) FROM persons WHERE age >= @minAge";
        var p = cmd.CreateParameter();
        p.ParameterName = "@minAge";
        p.Value = 50;
        cmd.Parameters.Add(p);
        var v = cmd.ExecuteScalar();
        _ = Convert.ToInt64(v);
    });

    RunBench("Filtered list (100)", iterations: 100, warmup: 20, action: () =>
    {
        using var cmd = conn.CreateCommand();
        cmd.CommandText = "SELECT id, name, age FROM persons WHERE age >= @minAge ORDER BY id LIMIT 100";
        var p = cmd.CreateParameter();
        p.ParameterName = "@minAge";
        p.Value = 50;
        cmd.Parameters.Add(p);
        using var reader = cmd.ExecuteReader();
        while (reader.Read())
        {
            _ = reader.GetInt64(0);
            _ = reader.GetString(1);
            _ = reader.GetInt32(2);
        }
    });

    Console.WriteLine();
    Console.WriteLine("=== Micro-ORM ===");

    using var ctx = new DecentDbContext(dbPath, pooling: true);
    var persons = ctx.Set<Person>();

    RunBench("GetAsync (by id)", iterations: 200, warmup: 50, action: () =>
    {
        var p = persons.GetAsync(1234).GetAwaiter().GetResult();
        if (p is null) throw new InvalidOperationException("missing row");
        _ = p.Name;
        _ = p.Age;
    });

    RunBench("CountAsync (age>=50)", iterations: 200, warmup: 50, action: () =>
    {
        _ = persons.Where(p => p.Age >= 50).CountAsync().GetAwaiter().GetResult();
    });

    RunBench("Paginated (Skip/Take)", iterations: 100, warmup: 20, action: () =>
    {
        _ = persons.OrderBy(p => p.Id).Skip(2000).Take(100).ToListAsync().GetAwaiter().GetResult();
    });

    RunBench("StreamAsync (Take 100)", iterations: 100, warmup: 20, action: () =>
    {
        var enumerator = persons.OrderBy(p => p.Id).Take(100).StreamAsync().GetAsyncEnumerator();
        try
        {
            while (enumerator.MoveNextAsync().AsTask().GetAwaiter().GetResult())
            {
                _ = enumerator.Current.Id;
            }
        }
        finally
        {
            enumerator.DisposeAsync().AsTask().GetAwaiter().GetResult();
        }
    });
}
finally
{
    DeleteDbFiles(dbPath);
}

public sealed class Person
{
    public long Id { get; set; }
    public string Name { get; set; } = "";
    public int Age { get; set; }
}
