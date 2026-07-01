using System;
using System.Collections.Generic;
using System.Data;
using System.Data.Common;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Runtime.InteropServices;
using System.Text;
using System.Text.Json;
using System.Text.Json.Serialization;
using System.Threading.Tasks;
using DecentDB.AdoNet;
using DecentDB.Native;
using Microsoft.Data.Sqlite;

namespace DecentDB.CrmComparison;

public enum DatabaseEngine { DecentDB, SQLite }

public enum EngineSelection
{
    All,
    DecentDB,
    SQLite
}

public enum ScenarioSize
{
    Tiny,    // quick correctness check
    Small,   // thousands of rows
    Medium,  // hundreds of thousands
    Large,   // millions
    Jumbo    // many millions
}

public enum DurabilityProfile
{
    Relaxed,
    Durable
}

public sealed record WorkloadProfile(
    int Companies,
    int UsersPerCompany,
    int AddressesPerUser,
    int InvoicesPerUser,
    int ItemsPerInvoice,
    int SearchSamples,
    int PointReadSamples);

public static class WorkloadProfiles
{
    public static WorkloadProfile Get(ScenarioSize size) => size switch
    {
        ScenarioSize.Tiny    => new(10,     10,    2,    20,    5,    50,      50),
        ScenarioSize.Small   => new(100,    50,    2,    50,    5,    500,     500),
        ScenarioSize.Medium  => new(1000,   100,   2,    100,   5,    5000,    5000),
        ScenarioSize.Large   => new(5000,   200,   2,    200,   5,    20000,   20000),
        ScenarioSize.Jumbo   => new(10000,  500,   2,    500,   5,    50000,   50000),
        _ => throw new ArgumentOutOfRangeException(nameof(size))
    };

    public static long TotalRows(WorkloadProfile p) =>
        (long)p.Companies
        + (long)p.Companies * p.UsersPerCompany
        + (long)p.Companies * p.UsersPerCompany * p.AddressesPerUser
        + (long)p.Companies * p.UsersPerCompany * p.InvoicesPerUser
        + (long)p.Companies * p.UsersPerCompany * p.InvoicesPerUser * p.ItemsPerInvoice;
}

public sealed record WorkloadData(
    int Seed,
    DateTime BaseUtc,
    IReadOnlyList<int> PointReadUserIds,
    IReadOnlyList<string> SearchPatterns,
    IReadOnlyList<int> DeleteCompanyIds);

public static class WorkloadDataFactory
{
    private static readonly string[] Roles = ["admin", "manager", "sales", "support", "viewer"];
    private static readonly string[] Cities = ["Springfield", "Franklin", "Greenville", "Madison", "Clayton", "Riverside", "Austin", "Denver"];
    private static readonly string[] Regions = ["CA", "TX", "NY", "FL", "WA", "CO", "IL", "OH"];

    public static WorkloadData Create(WorkloadProfile profile, int seed)
    {
        var userCount = checked(profile.Companies * profile.UsersPerCompany);
        var pointReads = new List<int>(profile.PointReadSamples);
        for (var i = 0; i < profile.PointReadSamples; i++)
        {
            pointReads.Add(1 + Range(seed, i, 1001, userCount));
        }

        var searchPatterns = new List<string>(profile.SearchSamples);
        for (var i = 0; i < profile.SearchSamples; i++)
        {
            var userId = 1 + Range(seed, i, 2001, userCount);
            var token = userId.ToString("D9")[^3..];
            searchPatterns.Add($"%{token}%");
        }

        var deleteCount = Math.Min(profile.Companies / 10, 10);
        var deleteCompanyIds = Enumerable.Range(1, deleteCount).ToArray();
        return new WorkloadData(seed, new DateTime(2026, 1, 1, 0, 0, 0, DateTimeKind.Utc), pointReads, searchPatterns, deleteCompanyIds);
    }

    public static string CompanyName(int id, int seed) =>
        $"Company {id:D6} - {Hash(seed, id, 11) & 0xFFFF_FFFFu:x8}";

    public static string Role(int userId, int seed) =>
        Roles[Range(seed, userId, 21, Roles.Length)];

    public static string City(int userId, int addressIndex, int seed) =>
        Cities[Range(seed, userId, 31 + addressIndex, Cities.Length)];

    public static string Region(int userId, int addressIndex, int seed) =>
        Regions[Range(seed, userId, 41 + addressIndex, Regions.Length)];

    public static DateTime DueDateUtc(int invoiceId, DateTime baseUtc, int seed) =>
        baseUtc.AddDays(7 + Range(seed, invoiceId, 51, 83));

    public static DateTime IssuedDateUtc(int invoiceId, DateTime baseUtc, int seed) =>
        DueDateUtc(invoiceId, baseUtc, seed).AddDays(-30);

    public static double InvoiceTotal(int invoiceId, int seed) =>
        Math.Round(Unit(seed, invoiceId, 61) * 5000 + 50, 2);

    public static bool InvoicePaid(int invoiceId, int seed) =>
        Unit(seed, invoiceId, 71) > 0.7;

    public static string Sku(int invoiceId, int itemIndex, int seed) =>
        $"SKU-{1 + Range(seed, invoiceId, 81 + itemIndex, 9999):D5}";

    public static double Quantity(int invoiceId, int itemIndex, int seed) =>
        Math.Round(Unit(seed, invoiceId, 91 + itemIndex) * 10 + 1, 4);

    public static double UnitPrice(int invoiceId, int itemIndex, int seed) =>
        Math.Round(Unit(seed, invoiceId, 101 + itemIndex) * 100 + 5, 4);

    private static int Range(int seed, int value, int salt, int exclusiveMax) =>
        (int)(Hash(seed, value, salt) % (uint)exclusiveMax);

    private static double Unit(int seed, int value, int salt) =>
        (Hash(seed, value, salt) & 0x00FF_FFFFu) / (double)0x0100_0000u;

    private static uint Hash(int seed, int value, int salt)
    {
        unchecked
        {
            var x = (uint)seed;
            x ^= (uint)value * 0x9E37_79B9u;
            x ^= (uint)salt * 0x85EB_CA6Bu;
            x ^= x >> 16;
            x *= 0x7FEB_352Du;
            x ^= x >> 15;
            x *= 0x846C_A68Bu;
            x ^= x >> 16;
            return x;
        }
    }
}

public sealed record BenchmarkResult(
    string Scenario,
    string Engine,
    long TotalRows,
    TimeSpan Duration,
    long? RowsAffected = null,
    long? RowsRead = null)
{
    public double OperationsPerSecond =>
        (RowsAffected ?? RowsRead ?? TotalRows) / Duration.TotalSeconds;
}

public sealed record RunOptions(
    ScenarioSize Size,
    int Iterations,
    int WarmupIterations,
    int DataSeed,
    string? JsonPath,
    string? OutputDirectory,
    bool AlternateOrder,
    bool UseNativeDecentDbHotPaths,
    EngineSelection EngineSelection,
    DurabilityProfile DurabilityProfile)
{
    public static RunOptions Parse(string[] args)
    {
        var size = ScenarioSize.Small;
        var iterations = 1;
        var warmupIterations = 0;
        var dataSeed = 42;
        string? jsonPath = null;
        string? outputDirectory = null;
        var alternateOrder = true;
        var useNativeDecentDbHotPaths = false;
        var engineSelection = EngineSelection.All;
        var durabilityProfile = DurabilityProfile.Relaxed;

        for (var i = 0; i < args.Length; i++)
        {
            var arg = args[i];
            if (arg.Equals("--size", StringComparison.OrdinalIgnoreCase) && i + 1 < args.Length)
            {
                size = ParseSize(args[++i]);
            }
            else if (arg.Equals("--iterations", StringComparison.OrdinalIgnoreCase) && i + 1 < args.Length)
            {
                iterations = Math.Max(1, int.Parse(args[++i]));
            }
            else if (arg.Equals("--warmup-iterations", StringComparison.OrdinalIgnoreCase) && i + 1 < args.Length)
            {
                warmupIterations = Math.Max(0, int.Parse(args[++i]));
            }
            else if (arg.Equals("--seed", StringComparison.OrdinalIgnoreCase) && i + 1 < args.Length)
            {
                dataSeed = int.Parse(args[++i]);
            }
            else if (arg.Equals("--json", StringComparison.OrdinalIgnoreCase) && i + 1 < args.Length)
            {
                jsonPath = args[++i];
            }
            else if (arg.Equals("--out-dir", StringComparison.OrdinalIgnoreCase) && i + 1 < args.Length)
            {
                outputDirectory = args[++i];
            }
            else if (arg.Equals("--no-alternate-order", StringComparison.OrdinalIgnoreCase))
            {
                alternateOrder = false;
            }
            else if (arg.Equals("--decentdb-native-hot-paths", StringComparison.OrdinalIgnoreCase))
            {
                useNativeDecentDbHotPaths = true;
            }
            else if (arg.Equals("--no-decentdb-native-hot-paths", StringComparison.OrdinalIgnoreCase))
            {
                useNativeDecentDbHotPaths = false;
            }
            else if (arg.Equals("--engines", StringComparison.OrdinalIgnoreCase) && i + 1 < args.Length)
            {
                engineSelection = ParseEngineSelection(args[++i]);
            }
            else if (arg.Equals("--decentdb-relaxed", StringComparison.OrdinalIgnoreCase))
            {
                durabilityProfile = DurabilityProfile.Relaxed;
            }
            else if (arg.Equals("--decentdb-durable", StringComparison.OrdinalIgnoreCase))
            {
                durabilityProfile = DurabilityProfile.Durable;
            }
            else if (arg.Equals("--durability", StringComparison.OrdinalIgnoreCase) && i + 1 < args.Length)
            {
                if (!Enum.TryParse<DurabilityProfile>(args[++i], ignoreCase: true, out durabilityProfile))
                {
                    throw new ArgumentException(
                        $"Unknown durability profile '{args[i - 1]}'. Use relaxed or durable.");
                }
            }
            else if (!arg.StartsWith("--", StringComparison.Ordinal) && i == 0)
            {
                size = ParseSize(arg);
            }
            else
            {
                throw new ArgumentException(
                    $"Unknown argument '{arg}'. Use --size <Tiny|Small|Medium|Large|Jumbo> --iterations <n> --warmup-iterations <n> --seed <n> --json <path> --out-dir <path> --durability <relaxed|durable> --engines <all|decentdb|sqlite> [--decentdb-relaxed|--decentdb-durable] [--no-alternate-order] [--decentdb-native-hot-paths|--no-decentdb-native-hot-paths].");
            }
        }

        return new RunOptions(size, iterations, warmupIterations, dataSeed, jsonPath, outputDirectory, alternateOrder, useNativeDecentDbHotPaths, engineSelection, durabilityProfile);
    }

    private static ScenarioSize ParseSize(string value)
    {
        if (!Enum.TryParse<ScenarioSize>(value, true, out var size))
        {
            throw new ArgumentException($"Unknown scenario '{value}'. Use: Tiny, Small, Medium, Large, Jumbo.");
        }

        return size;
    }

    private static EngineSelection ParseEngineSelection(string value)
    {
        return value.ToLowerInvariant() switch
        {
            "all" => EngineSelection.All,
            "decentdb" => EngineSelection.DecentDB,
            "sqlite" => EngineSelection.SQLite,
            _ => throw new ArgumentException(
                $"Unknown engine selection '{value}'. Use all, decentdb, or sqlite.")
        };
    }
}

public sealed record BenchmarkManifest(
    string Benchmark,
    string RunId,
    string ScenarioSize,
    long ApproximateRows,
    int Iterations,
    int WarmupIterations,
    bool AlternateOrder,
    int DataSeed,
    DateTimeOffset StartedAtUtc,
    DateTimeOffset FinishedAtUtc,
    string OutputDirectory,
    string MachineName,
    int ProcessorCount,
    string DotNetVersion,
    string OsDescription,
    string OsArchitecture,
    string ProcessArchitecture,
    string DecentDbAdoNetAssemblyVersion,
    string DecentDbEngineVersion,
    uint DecentDbAbiVersion,
    string DurabilityProfile,
    bool UseNativeDecentDbHotPaths,
    bool NativeDecentDbHotPathsActive,
    string SQLiteProviderVersion,
    string SQLiteNativeVersion,
    [JsonPropertyName("engine_order")]
    IReadOnlyList<string> EngineOrder,
    string DotnetSdkVersion,
    string DecentDbAdoNetVersion,
    string DecentDbPackage);

public sealed record BenchmarkScenarioResult(
    string RunId,
    string Phase,
    int Iteration,
    string Engine,
    int EngineOrder,
    string Scenario,
    long TotalRows,
    double DurationMs,
    long? RowsAffected,
    long? RowsRead,
    double OperationsPerSecond,
    string DatabasePath,
    long DatabaseBytes);

public sealed record BenchmarkSummary(
    string Scenario,
    string Engine,
    int Iterations,
    double MeanMs,
    double MedianMs,
    double P95Ms,
    double MinMs,
    double MaxMs,
    double StdDevMs,
    double MeanOperationsPerSecond);

public sealed record BenchmarkJsonOutput(
    BenchmarkManifest Manifest,
    IReadOnlyList<BenchmarkScenarioResult> Results,
    IReadOnlyList<BenchmarkSummary> Summary);

public interface IDatabaseProvider : IAsyncDisposable
{
    string EngineName { get; }
    string DatabasePath { get; }
    DbConnection Connection { get; }
    Task OpenAsync();
    Task CloseAsync();
    Task ExecuteNonQueryAsync(string sql);
    Task ExecuteNonQueryAsync(string sql, params (string Name, object? Value)[] parameters);
    Task<object?> ExecuteScalarAsync(string sql);
    Task<IDataReader> ExecuteReaderAsync(string sql, params (string Name, object? Value)[] parameters);
    DbTransaction BeginTransaction();
    Task CheckpointAsync();
    string ExplainPlan(string sql);
}

public sealed class DecentDbProvider : IDatabaseProvider
{
    private const string RelaxedNativeOptions = "cache_size=128MB;retain_paged_row_sources_after_commit=true;paged_row_storage=false;wal_autocheckpoint=0;process_coordination=single_process_unsafe;wal_sync_mode=async_commit:10;plan_cache_max_bytes=2097152";
    private const string DurableNativeOptions = "cache_size=128MB;retain_paged_row_sources_after_commit=true;paged_row_storage=false;wal_autocheckpoint=0;process_coordination=single_process_unsafe;plan_cache_max_bytes=2097152";

    private readonly string _path;
    private readonly DurabilityProfile _durabilityProfile;
    private DecentDBConnection? _conn;
    public string EngineName => "DecentDB";
    public DbConnection Connection => _conn ?? throw new InvalidOperationException("Connection is not open");
    public string DatabasePath => _path;

    public DecentDbProvider(string path, DurabilityProfile durabilityProfile)
    {
        _path = path;
        _durabilityProfile = durabilityProfile;
    }

    public static string GetNativeOptions(DurabilityProfile durabilityProfile)
    {
        return durabilityProfile == DurabilityProfile.Relaxed ? RelaxedNativeOptions : DurableNativeOptions;
    }

    public async ValueTask DisposeAsync()
    {
        if (_conn is not null) await _conn.DisposeAsync();
    }

    public Task OpenAsync()
    {
        var csb = new DecentDBConnectionStringBuilder
        {
            DataSource = _path,
            PerformanceProfile = "embedded_fast",
            CacheSize = "128MB",
            RetainPagedRowSourcesAfterCommit = true,
            PagedRowStorage = false,
            WalAutoCheckpoint = "0",
            ProcessCoordination = "single_process_unsafe",
            CommandTimeout = 600
        };
        // Append raw native options for async-commit durability (SQLite NORMAL-style) and larger plan cache.
        var baseCs = csb.ConnectionString.Trim();
        csb.ConnectionString = baseCs + ";" + GetNativeOptions(_durabilityProfile);
        _conn = new DecentDBConnection(csb.ConnectionString);
        return _conn.OpenAsync();
    }

    public Task CloseAsync() => _conn!.CloseAsync();

    public async Task ExecuteNonQueryAsync(string sql)
    {
        using var cmd = _conn!.CreateCommand();
        cmd.CommandText = sql;
        await cmd.ExecuteNonQueryAsync();
    }

    public async Task ExecuteNonQueryAsync(string sql, params (string Name, object? Value)[] parameters)
    {
        using var cmd = _conn!.CreateCommand();
        var (convertedSql, values) = ConvertParameters(sql, parameters);
        cmd.CommandText = convertedSql;
        for (int i = 0; i < values.Count; i++)
            AddParameter(cmd, i + 1, values[i]);
        await cmd.ExecuteNonQueryAsync();
    }

    public async Task<object?> ExecuteScalarAsync(string sql)
    {
        using var cmd = _conn!.CreateCommand();
        cmd.CommandText = sql;
        var result = await cmd.ExecuteScalarAsync();
        return result ?? DBNull.Value;
    }

    public async Task<IDataReader> ExecuteReaderAsync(string sql, params (string Name, object? Value)[] parameters)
    {
        var cmd = _conn!.CreateCommand();
        var (convertedSql, values) = ConvertParameters(sql, parameters);
        cmd.CommandText = convertedSql;
        for (int i = 0; i < values.Count; i++)
            AddParameter(cmd, i + 1, values[i]);
        var reader = await cmd.ExecuteReaderAsync();
        return new DataReaderWrapper(reader, cmd);
    }

    private static (string Sql, List<object?> Values) ConvertParameters(string sql, (string Name, object? Value)[] parameters)
    {
        var values = new List<object?>();
        foreach (var (name, value) in parameters)
        {
            sql = sql.Replace($":{name}", $"${values.Count + 1}", StringComparison.OrdinalIgnoreCase);
            values.Add(value);
        }
        return (sql, values);
    }

    public DbTransaction BeginTransaction() => _conn!.BeginTransaction();

    public Task CheckpointAsync()
    {
        _conn!.Checkpoint();
        return Task.CompletedTask;
    }

    public string ExplainPlan(string sql) => _conn!.ExplainQuery(sql).Text;

    private static void AddParameter(DbCommand cmd, int index, object? value)
    {
        var p = cmd.CreateParameter();
        p.ParameterName = $"{index}";
        p.Value = value ?? DBNull.Value;
        cmd.Parameters.Add(p);
    }
}

public sealed class SqliteProvider : IDatabaseProvider
{
    private readonly string _path;
    private readonly bool _useDurableSync;
    private SqliteConnection? _conn;
    public string EngineName => "SQLite";
    public string DatabasePath => _path;
    public DbConnection Connection => _conn ?? throw new InvalidOperationException("Connection is not open");

    public SqliteProvider(string path, bool useDurableSync) => (_path, _useDurableSync) = (path, useDurableSync);

    public async ValueTask DisposeAsync()
    {
        if (_conn is not null) await _conn.DisposeAsync();
    }

    public async Task OpenAsync()
    {
        var builder = new SqliteConnectionStringBuilder
        {
            DataSource = _path,
            Pooling = false
        };
        _conn = new SqliteConnection(builder.ConnectionString);
        await _conn.OpenAsync();
        await ExecuteNonQueryAsync("PRAGMA journal_mode = WAL;");
        await ExecuteNonQueryAsync(_useDurableSync ? "PRAGMA synchronous = FULL;" : "PRAGMA synchronous = NORMAL;");
        await ExecuteNonQueryAsync("PRAGMA foreign_keys = ON;");
        await ExecuteNonQueryAsync("PRAGMA cache_size = -65536;");
        await ExecuteNonQueryAsync("PRAGMA temp_store = MEMORY;");
    }

    public Task CloseAsync() => _conn!.CloseAsync();

    public async Task ExecuteNonQueryAsync(string sql)
    {
        using var cmd = _conn!.CreateCommand();
        cmd.CommandText = sql;
        await cmd.ExecuteNonQueryAsync();
    }

    public async Task ExecuteNonQueryAsync(string sql, params (string Name, object? Value)[] parameters)
    {
        using var cmd = _conn!.CreateCommand();
        var (convertedSql, values) = ConvertParameters(sql, parameters);
        cmd.CommandText = convertedSql;
        for (int i = 0; i < values.Count; i++)
            AddParameter(cmd, i + 1, values[i]);
        await cmd.ExecuteNonQueryAsync();
    }

    public async Task<object?> ExecuteScalarAsync(string sql)
    {
        using var cmd = _conn!.CreateCommand();
        cmd.CommandText = sql;
        var result = await cmd.ExecuteScalarAsync();
        return result ?? DBNull.Value;
    }

    public async Task<IDataReader> ExecuteReaderAsync(string sql, params (string Name, object? Value)[] parameters)
    {
        var cmd = _conn!.CreateCommand();
        var (convertedSql, values) = ConvertParameters(sql, parameters);
        cmd.CommandText = convertedSql;
        for (int i = 0; i < values.Count; i++)
            AddParameter(cmd, i + 1, values[i]);
        var reader = await cmd.ExecuteReaderAsync();
        return new DataReaderWrapper(reader, cmd);
    }

    private static (string Sql, List<object?> Values) ConvertParameters(string sql, (string Name, object? Value)[] parameters)
    {
        var values = new List<object?>();
        foreach (var (name, value) in parameters)
        {
            sql = sql.Replace($":{name}", $"${values.Count + 1}", StringComparison.OrdinalIgnoreCase);
            values.Add(value);
        }
        return (sql, values);
    }

    public DbTransaction BeginTransaction() => _conn!.BeginTransaction();

    public Task CheckpointAsync()
    {
        using var cmd = _conn!.CreateCommand();
        cmd.CommandText = "PRAGMA wal_checkpoint(TRUNCATE);";
        cmd.ExecuteNonQuery();
        return Task.CompletedTask;
    }

    public string ExplainPlan(string sql)
    {
        using var cmd = _conn!.CreateCommand();
        cmd.CommandText = $"EXPLAIN QUERY PLAN {sql}";
        var sb = new StringBuilder();
        using var r = cmd.ExecuteReader();
        while (r.Read()) sb.AppendLine(r.GetString(3));
        return sb.ToString();
    }

    private static void AddParameter(DbCommand cmd, int index, object? value)
    {
        var p = cmd.CreateParameter();
        p.ParameterName = $"{index}";
        p.Value = value ?? DBNull.Value;
        cmd.Parameters.Add(p);
    }
}

public static class Schema
{
    // Hot-loop DML uses ADO.NET parameters and prepared commands in the harness.

    public const string DecentDdl = """
        CREATE TABLE IF NOT EXISTS companies (
            id INTEGER PRIMARY KEY,
            name TEXT NOT NULL,
            tax_id TEXT UNIQUE,
            created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
            active BOOLEAN NOT NULL DEFAULT TRUE
        );

        CREATE TABLE IF NOT EXISTS users (
            id INTEGER PRIMARY KEY,
            company_id INTEGER NOT NULL REFERENCES companies(id) ON DELETE CASCADE,
            email TEXT UNIQUE NOT NULL,
            full_name TEXT NOT NULL,
            role TEXT NOT NULL,
            created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
            active BOOLEAN NOT NULL DEFAULT TRUE
        );
        CREATE INDEX IF NOT EXISTS idx_users_company ON users(company_id);
        CREATE INDEX IF NOT EXISTS idx_users_name_trgm ON users USING trigram(full_name);

        CREATE TABLE IF NOT EXISTS addresses (
            id INTEGER PRIMARY KEY,
            user_id INTEGER NOT NULL REFERENCES users(id) ON DELETE CASCADE,
            line1 TEXT NOT NULL,
            city TEXT NOT NULL,
            region TEXT NOT NULL,
            postal_code TEXT NOT NULL,
            country TEXT NOT NULL,
            kind TEXT NOT NULL
        );
        CREATE INDEX IF NOT EXISTS idx_addresses_user ON addresses(user_id);

        CREATE TABLE IF NOT EXISTS invoices (
            id INTEGER PRIMARY KEY,
            user_id INTEGER NOT NULL REFERENCES users(id) ON DELETE CASCADE,
            company_id INTEGER NOT NULL REFERENCES companies(id) ON DELETE CASCADE,
            invoice_number TEXT UNIQUE NOT NULL,
            issued_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
            due_at TIMESTAMP NOT NULL,
            total REAL NOT NULL DEFAULT 0,
            paid BOOLEAN NOT NULL DEFAULT FALSE
        );
        CREATE INDEX IF NOT EXISTS idx_invoices_user ON invoices(user_id);
        CREATE INDEX IF NOT EXISTS idx_invoices_company ON invoices(company_id);
        CREATE INDEX IF NOT EXISTS idx_invoices_user_total ON invoices(user_id, total);
        CREATE INDEX IF NOT EXISTS idx_invoices_unpaid_total ON invoices(total) WHERE paid = FALSE;
        CREATE INDEX IF NOT EXISTS idx_invoices_paid_total ON invoices(paid, total);
        CREATE INDEX IF NOT EXISTS idx_invoices_unpaid_due ON invoices(due_at) INCLUDE (user_id, invoice_number, total) WHERE paid = FALSE;

        CREATE TABLE IF NOT EXISTS company_revenue (
            company_id INTEGER PRIMARY KEY REFERENCES companies(id) ON DELETE CASCADE,
            user_count INTEGER NOT NULL DEFAULT 0,
            revenue REAL NOT NULL DEFAULT 0
        );

        CREATE TABLE IF NOT EXISTS invoice_items (
            id INTEGER PRIMARY KEY,
            invoice_id INTEGER NOT NULL REFERENCES invoices(id) ON DELETE CASCADE,
            sku TEXT NOT NULL,
            description TEXT NOT NULL,
            quantity REAL NOT NULL,
            unit_price REAL NOT NULL,
            line_total REAL GENERATED ALWAYS AS (quantity * unit_price) STORED
        );
        CREATE INDEX IF NOT EXISTS idx_items_invoice ON invoice_items(invoice_id);

        CREATE OR REPLACE VIEW v_unpaid_invoices AS
        SELECT i.id, i.invoice_number, u.full_name, u.email, i.total, i.due_at
        FROM invoices i
        JOIN users u ON u.id = i.user_id
        WHERE i.paid = FALSE;
        """;

    public const string SqliteDdl = """
        CREATE TABLE IF NOT EXISTS companies (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            name TEXT NOT NULL,
            tax_id TEXT UNIQUE,
            created_at TEXT NOT NULL DEFAULT (datetime('now')),
            active INTEGER NOT NULL DEFAULT 1
        );

        CREATE TABLE IF NOT EXISTS users (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            company_id INTEGER NOT NULL REFERENCES companies(id) ON DELETE CASCADE,
            email TEXT UNIQUE NOT NULL,
            full_name TEXT NOT NULL,
            role TEXT NOT NULL,
            created_at TEXT NOT NULL DEFAULT (datetime('now')),
            active INTEGER NOT NULL DEFAULT 1
        );
        CREATE INDEX IF NOT EXISTS idx_users_company ON users(company_id);

        CREATE TABLE IF NOT EXISTS addresses (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            user_id INTEGER NOT NULL REFERENCES users(id) ON DELETE CASCADE,
            line1 TEXT NOT NULL,
            city TEXT NOT NULL,
            region TEXT NOT NULL,
            postal_code TEXT NOT NULL,
            country TEXT NOT NULL,
            kind TEXT NOT NULL
        );
        CREATE INDEX IF NOT EXISTS idx_addresses_user ON addresses(user_id);

        CREATE TABLE IF NOT EXISTS invoices (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            user_id INTEGER NOT NULL REFERENCES users(id) ON DELETE CASCADE,
            company_id INTEGER NOT NULL REFERENCES companies(id) ON DELETE CASCADE,
            invoice_number TEXT UNIQUE NOT NULL,
            issued_at TEXT NOT NULL DEFAULT (datetime('now')),
            due_at TEXT NOT NULL,
            total REAL NOT NULL DEFAULT 0,
            paid INTEGER NOT NULL DEFAULT 0
        );
        CREATE INDEX IF NOT EXISTS idx_invoices_user ON invoices(user_id);
        CREATE INDEX IF NOT EXISTS idx_invoices_company ON invoices(company_id);
        CREATE INDEX IF NOT EXISTS idx_invoices_user_total ON invoices(user_id, total);
        CREATE INDEX IF NOT EXISTS idx_invoices_unpaid_total ON invoices(total) WHERE paid = 0;
        CREATE INDEX IF NOT EXISTS idx_invoices_paid_total ON invoices(paid, total);
        CREATE INDEX IF NOT EXISTS idx_invoices_unpaid_due ON invoices(due_at, user_id, invoice_number, total) WHERE paid = 0;

        CREATE TABLE IF NOT EXISTS company_revenue (
            company_id INTEGER PRIMARY KEY REFERENCES companies(id) ON DELETE CASCADE,
            user_count INTEGER NOT NULL DEFAULT 0,
            revenue REAL NOT NULL DEFAULT 0
        );

        CREATE TABLE IF NOT EXISTS invoice_items (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            invoice_id INTEGER NOT NULL REFERENCES invoices(id) ON DELETE CASCADE,
            sku TEXT NOT NULL,
            description TEXT NOT NULL,
            quantity REAL NOT NULL,
            unit_price REAL NOT NULL,
            line_total REAL GENERATED ALWAYS AS (quantity * unit_price) STORED
        );
        CREATE INDEX IF NOT EXISTS idx_items_invoice ON invoice_items(invoice_id);

        CREATE VIEW IF NOT EXISTS v_unpaid_invoices AS
        SELECT i.id, i.invoice_number, u.full_name, u.email, i.total, i.due_at
        FROM invoices i
        JOIN users u ON u.id = i.user_id
        WHERE i.paid = 0;
        """;
}


public sealed class DataReaderWrapper : IDataReader
{
    private readonly IDataReader _reader;
    private readonly DbCommand _command;
    public DataReaderWrapper(IDataReader reader, DbCommand command)
    {
        _reader = reader;
        _command = command;
    }
    public bool Read() => _reader.Read();
    public int FieldCount => _reader.FieldCount;
    public object this[int i] => _reader[i];
    public object this[string name] => _reader[name];
    public void Close() { _reader.Close(); }
    public void Dispose() { _reader.Dispose(); _command.Dispose(); }
    public bool IsClosed => _reader.IsClosed;
    public string GetName(int i) => _reader.GetName(i);
    public int GetOrdinal(string name) => _reader.GetOrdinal(name);
    public bool GetBoolean(int i) => _reader.GetBoolean(i);
    public byte GetByte(int i) => _reader.GetByte(i);
    public long GetBytes(int i, long fieldOffset, byte[]? buffer, int bufferoffset, int length) => _reader.GetBytes(i, fieldOffset, buffer, bufferoffset, length);
    public char GetChar(int i) => _reader.GetChar(i);
    public long GetChars(int i, long fieldoffset, char[]? buffer, int bufferoffset, int length) => _reader.GetChars(i, fieldoffset, buffer, bufferoffset, length);
    public IDataReader GetData(int i) => _reader.GetData(i);
    public string GetDataTypeName(int i) => _reader.GetDataTypeName(i);
    public DateTime GetDateTime(int i) => _reader.GetDateTime(i);
    public decimal GetDecimal(int i) => _reader.GetDecimal(i);
    public double GetDouble(int i) => _reader.GetDouble(i);
    public Type GetFieldType(int i) => _reader.GetFieldType(i);
    public float GetFloat(int i) => _reader.GetFloat(i);
    public Guid GetGuid(int i) => _reader.GetGuid(i);
    public short GetInt16(int i) => _reader.GetInt16(i);
    public int GetInt32(int i) => _reader.GetInt32(i);
    public long GetInt64(int i) => _reader.GetInt64(i);
    public string GetString(int i) => _reader.GetString(i);
    public object GetValue(int i) => _reader.GetValue(i);
    public int GetValues(object[] values) => _reader.GetValues(values);
    public bool IsDBNull(int i) => _reader.IsDBNull(i);
    public DataTable? GetSchemaTable() => _reader.GetSchemaTable();
    public int Depth => _reader.Depth;
    public int RecordsAffected => _reader.RecordsAffected;
    public bool NextResult() => _reader.NextResult();
}

internal sealed class DecentDbNativeHotPathRunner : IDisposable
{
    private readonly global::DecentDB.Native.DecentDB _db;
    private PreparedStatement? _pointReadStatement;
    private PreparedStatement? _updatePaidStatement;
    private PreparedStatement? _verifyPaidStatement;
    private PreparedStatement? _windowQueryStatement;
    private PreparedStatement? _deleteCascadeStatement;

    private const string PointReadSql = "SELECT id, email, full_name FROM users WHERE id = $1;";
    private const string UpdatePaidSql = "UPDATE invoices SET paid = TRUE WHERE paid = FALSE AND total < $1;";
    private const string VerifyPaidSql = "SELECT COUNT(*) FROM invoices WHERE paid = TRUE AND total < $1;";
    private const string WindowQuerySql = "SELECT user_id, invoice_number, total, ROW_NUMBER() OVER (PARTITION BY user_id ORDER BY total DESC) AS rn, RANK() OVER (PARTITION BY user_id ORDER BY total DESC) AS rnk FROM invoices;";
    private const string DeleteCascadeSql = "DELETE FROM companies WHERE id = $1;";

    public DecentDbNativeHotPathRunner(string databasePath, string nativeOptions)
    {
        _db = new global::DecentDB.Native.DecentDB(databasePath, nativeOptions);
    }

    public void Dispose()
    {
        _pointReadStatement?.Dispose();
        _updatePaidStatement?.Dispose();
        _verifyPaidStatement?.Dispose();
        _windowQueryStatement?.Dispose();
        _deleteCascadeStatement?.Dispose();
        _db.Dispose();
    }

    public long RunPointReadScenario(IReadOnlyList<int> pointReadUserIds, int lookups)
    {
        var stmt = _pointReadStatement ??= _db.Prepare(PointReadSql);
        if (pointReadUserIds.Count == 0)
        {
            throw new InvalidOperationException("Point-read benchmark requires at least one existing user id.");
        }

        long checksum = 0;
        long rows = 0;
        _db.BeginTransaction();
        try
        {
            for (var i = 0; i < lookups; i++)
            {
                var rc = ExecuteQueryAndReset(stmt, pointReadUserIds[i % pointReadUserIds.Count]);
                if (rc == 1)
                {
                    rows++;
                    checksum += stmt.GetInt64(0);
                    checksum += stmt.GetText(1).Length;
                    checksum += stmt.GetText(2).Length;
                }
                else if (rc < 0)
                {
                    throw new DecentDBException(stmt.LastErrorCode, stmt.LastErrorMessage, PointReadSql);
                }
            }

            _db.CommitTransaction();
            if (checksum == long.MinValue)
            {
                throw new InvalidOperationException("Unreachable checksum guard.");
            }

            return rows;
        }
        catch
        {
            if (_db.InTransaction)
            {
                _db.RollbackTransaction();
            }

            throw;
        }
    }

    public long RunUpdatePaidScenario(double max)
    {
        var update = _updatePaidStatement ??= _db.Prepare(UpdatePaidSql);
        var verify = _verifyPaidStatement ??= _db.Prepare(VerifyPaidSql);

        _db.BeginTransaction();
        try
        {
            update.BindFloat64(1, max);
            var affected = update.StepRowsAffected();
            update.Reset();

            ExecuteScalarCount(verify, max);
            _db.CommitTransaction();
            return affected;
        }
        catch
        {
            if (_db.InTransaction)
            {
                _db.RollbackTransaction();
            }

            throw;
        }
    }

    public long RunWindowScenario()
    {
        var stmt = _windowQueryStatement ??= _db.Prepare(WindowQuerySql);
        var rows = 0L;

        try
        {
            while (true)
            {
                var rc = stmt.Step();
                if (rc < 0)
                {
                    throw new DecentDBException(stmt.LastErrorCode, stmt.LastErrorMessage, WindowQuerySql);
                }

                if (rc == 0)
                {
                    break;
                }

                rows++;
            }

            return rows;
        }
        finally
        {
            stmt.Reset();
        }
    }

    public long RunDeleteCascadeScenario(IReadOnlyList<int> companyIds)
    {
        var stmt = _deleteCascadeStatement ??= _db.Prepare(DeleteCascadeSql);
        var totalDeleted = 0L;

        _db.BeginTransaction();
        try
        {
            for (var i = 0; i < companyIds.Count; i++)
            {
                var rows = executeDml(stmt, companyIds[i]);
                totalDeleted += rows;
            }

            _db.CommitTransaction();
            return totalDeleted;
        }
        catch
        {
            if (_db.InTransaction)
            {
                _db.RollbackTransaction();
            }

            throw;
        }
    }

    private static void ExecuteScalarCount(PreparedStatement statement, double max)
    {
        statement.BindFloat64(1, max);
        try
        {
            var rc = statement.Step();
            if (rc < 0)
            {
                throw new DecentDBException(statement.LastErrorCode, statement.LastErrorMessage, VerifyPaidSql);
            }

            if (rc == 0)
            {
                throw new DecentDBException(statement.LastErrorCode, statement.LastErrorMessage, VerifyPaidSql);
            }

            _ = statement.GetInt64(0);
        }
        finally
        {
            statement.Reset();
        }
    }

    private static long executeDml(PreparedStatement statement, long id)
    {
        var rows = statement.BindInt64(1, id).StepRowsAffected();
        statement.Reset();
        return rows;
    }

    private static long ExecuteQueryAndReset(PreparedStatement statement, int userId)
    {
        statement.BindInt64(1, userId);
        var result = statement.Step();
        statement.Reset();
        return result;
    }
}

public sealed class BenchmarkHarness
{
    private const int DecentDbBatchRows = 2048;
    private const int DecentDbTextHeavyBatchRows = 1024;
    private const int MinimumScaledTinyOperations = 50_000;
    private const int MinimumScaledPointReadLookups = 50_000;
    private static readonly byte[] DecentDbCompanyBatchSignature = Encoding.ASCII.GetBytes("itt\0");
    private static readonly byte[] DecentDbUserBatchSignature = Encoding.ASCII.GetBytes("iittt\0");
    private static readonly byte[] DecentDbAddressBatchSignature = Encoding.ASCII.GetBytes("iittttt\0");
    private static readonly byte[] DecentDbInvoiceBatchSignature = Encoding.ASCII.GetBytes("iiitttfb\0");
    private static readonly byte[] DecentDbInvoiceItemBatchSignature = Encoding.ASCII.GetBytes("itff\0");

    private readonly IDatabaseProvider _db;
    private readonly DatabaseEngine _engine;
    private readonly WorkloadProfile _profile;
    private readonly WorkloadData _data;
    private readonly string? _explainDirectory;
    private readonly DurabilityProfile _durabilityProfile;
    private readonly List<BenchmarkResult> _results = new();
    private readonly List<int> _companyIds = new();
    private readonly List<int> _userIds = new();
    private readonly List<int> _invoiceIds = new();
    private readonly bool _useNativeDecentDbHotPaths;
    private readonly DecentDbNativeHotPathRunner? _nativeRunner;
    private readonly bool _nativeRunnerActive;

    public bool NativeDecentDbHotPathsActive => _nativeRunnerActive;

    public BenchmarkHarness(
        IDatabaseProvider db,
        DatabaseEngine engine,
        WorkloadProfile profile,
        WorkloadData data,
        string? explainDirectory,
        DurabilityProfile durabilityProfile,
        bool useNativeDecentDbHotPaths)
    {
        _db = db;
        _engine = engine;
        _profile = profile;
        _data = data;
        _explainDirectory = explainDirectory;
        _durabilityProfile = durabilityProfile;
        _useNativeDecentDbHotPaths = useNativeDecentDbHotPaths;

        if (_useNativeDecentDbHotPaths && _db is DecentDbProvider && engine == DatabaseEngine.DecentDB)
        {
            try
            {
                _nativeRunner = new DecentDbNativeHotPathRunner(_db.DatabasePath, DecentDbProvider.GetNativeOptions(_durabilityProfile));
                _nativeRunnerActive = true;
            }
            catch
            {
                _nativeRunnerActive = false;
            }
        }
    }

    public IReadOnlyList<BenchmarkResult> Results => _results;

    private DbCommand CreatePreparedCommand(
        DbTransaction? transaction,
        string sql,
        params (string Name, DbType Type)[] parameters)
    {
        var command = _db.Connection.CreateCommand();
        command.Transaction = transaction;
        command.CommandText = sql;

        foreach (var (name, type) in parameters)
        {
            var parameter = command.CreateParameter();
            parameter.ParameterName = name;
            parameter.DbType = type;
            command.Parameters.Add(parameter);
        }

        try
        {
            command.Prepare();
        }
        catch (NotSupportedException)
        {
            // Some ADO.NET providers treat Prepare as optional.
        }

        return command;
    }

    private static DbParameter Parameter(DbCommand command, int index) =>
        (DbParameter)command.Parameters[index];

    private long ExecuteDecentDbCompanyBatch(
        DecentDBConnection connection,
        int rowCount,
        long[] i64Values,
        List<byte[]> textValues)
    {
        if (rowCount == 0)
        {
            return 0;
        }

        return connection.ExecutePreparedBatchTyped(
            "INSERT INTO companies (id, name, tax_id, active) VALUES ($1, $2, $3, TRUE);",
            DecentDbCompanyBatchSignature,
            rowCount,
            i64Values.AsSpan(0, rowCount),
            ReadOnlySpan<double>.Empty,
            textValues);
    }

    private long ExecuteDecentDbUserBatch(
        DecentDBConnection connection,
        int rowCount,
        long[] i64Values,
        List<byte[]> textValues)
    {
        if (rowCount == 0)
        {
            return 0;
        }

        return connection.ExecutePreparedBatchTyped(
            "INSERT INTO users (id, company_id, email, full_name, role, active) VALUES ($1, $2, $3, $4, $5, TRUE);",
            DecentDbUserBatchSignature,
            rowCount,
            i64Values.AsSpan(0, rowCount * 2),
            ReadOnlySpan<double>.Empty,
            textValues);
    }

    private long ExecuteDecentDbAddressBatch(
        DecentDBConnection connection,
        int rowCount,
        long[] i64Values,
        List<byte[]> textValues)
    {
        if (rowCount == 0)
        {
            return 0;
        }

        return connection.ExecutePreparedBatchTyped(
            "INSERT INTO addresses (id, user_id, line1, city, region, postal_code, country, kind) VALUES ($1, $2, $3, $4, $5, $6, 'US', $7);",
            DecentDbAddressBatchSignature,
            rowCount,
            i64Values.AsSpan(0, rowCount * 2),
            ReadOnlySpan<double>.Empty,
            textValues);
    }

    private long ExecuteDecentDbInvoiceBatch(
        DecentDBConnection connection,
        int rowCount,
        long[] i64Values,
        double[] f64Values,
        List<byte[]> textValues)
    {
        if (rowCount == 0)
        {
            return 0;
        }

        return connection.ExecutePreparedBatchTyped(
            "INSERT INTO invoices (id, user_id, company_id, invoice_number, issued_at, due_at, total, paid) VALUES ($1, $2, $3, $4, $5, $6, $7, $8);",
            DecentDbInvoiceBatchSignature,
            rowCount,
            i64Values.AsSpan(0, rowCount * 4),
            f64Values.AsSpan(0, rowCount),
            textValues);
    }

    private long ExecuteDecentDbInvoiceItemBatch(
        DecentDBConnection connection,
        int rowCount,
        long[] i64Values,
        double[] f64Values,
        List<byte[]> textValues)
    {
        if (rowCount == 0)
        {
            return 0;
        }

        return connection.ExecutePreparedBatchTyped(
            "INSERT INTO invoice_items (invoice_id, sku, description, quantity, unit_price) VALUES ($1, $2, 'Benchmark item', $3, $4);",
            DecentDbInvoiceItemBatchSignature,
            rowCount,
            i64Values.AsSpan(0, rowCount),
            f64Values.AsSpan(0, rowCount * 2),
            textValues);
    }

    private static int RepeatCountForAtLeast(int rowsPerRepeat, int minimumRows)
    {
        if (rowsPerRepeat <= 0)
        {
            return 1;
        }

        return Math.Max(1, (minimumRows + rowsPerRepeat - 1) / rowsPerRepeat);
    }

    public async Task RunAsync()
    {
        try
        {
            Console.WriteLine($"\n--- Running {_db.EngineName} benchmark ---");
            await InitializeSchemaAsync();

            var companyInsertRepeats = RepeatCountForAtLeast(_profile.Companies, MinimumScaledTinyOperations);

        _results.Add(await RunScenarioAsync("01. Bulk Insert Companies", async () =>
        {
            if (_db.Connection is DecentDBConnection decentConnection)
            {
                long count = 0;
                var i64Values = new long[DecentDbBatchRows];
                var textValues = new List<byte[]>(DecentDbBatchRows * 2);

                void InsertDecentCompanyRows(int repeats, int idOffset, bool recordCompanyIds)
                {
                    for (int repeat = 0; repeat < repeats; repeat++)
                    {
                        var batchRows = 0;

                        for (int i = 1; i <= _profile.Companies; i++)
                        {
                            var rowId = idOffset + (repeat * _profile.Companies) + i;
                            i64Values[batchRows] = rowId;
                            textValues.Add(Encoding.UTF8.GetBytes(WorkloadDataFactory.CompanyName(rowId, _data.Seed)));
                            textValues.Add(Encoding.UTF8.GetBytes(recordCompanyIds ? $"TAX-{i:D9}" : $"TAX-S01-{rowId:D12}"));
                            if (recordCompanyIds)
                            {
                                _companyIds.Add(i);
                            }

                            count++;
                            batchRows++;

                            if (batchRows == DecentDbBatchRows)
                            {
                                ExecuteDecentDbCompanyBatch(decentConnection, batchRows, i64Values, textValues);
                                batchRows = 0;
                                textValues.Clear();
                            }
                        }

                        ExecuteDecentDbCompanyBatch(decentConnection, batchRows, i64Values, textValues);
                        textValues.Clear();
                    }
                }

                using (var setupTx = _db.BeginTransaction())
                {
                    InsertDecentCompanyRows(repeats: 1, idOffset: 0, recordCompanyIds: true);
                    setupTx.Commit();
                }

                if (companyInsertRepeats > 1)
                {
                    using var scaleTx = _db.BeginTransaction();
                    InsertDecentCompanyRows(
                        repeats: companyInsertRepeats - 1,
                        idOffset: _profile.Companies,
                        recordCompanyIds: false);
                    scaleTx.Rollback();
                }

                return count;
            }

            long fallbackCount = 0;

            async Task InsertCompanyRowsAsync(DbTransaction tx, int repeats, int idOffset, bool recordCompanyIds)
            {
                if (repeats == 0)
                {
                    return;
                }

                using var cmd = CreatePreparedCommand(
                    tx,
                    "INSERT INTO companies (id, name, tax_id, active) VALUES (@id, @name, @tax, TRUE);",
                    ("@id", DbType.Int64),
                    ("@name", DbType.String),
                    ("@tax", DbType.String));
                var idParam = Parameter(cmd, 0);
                var nameParam = Parameter(cmd, 1);
                var taxParam = Parameter(cmd, 2);

                for (int repeat = 0; repeat < repeats; repeat++)
                {
                    for (int i = 1; i <= _profile.Companies; i++)
                    {
                        var rowId = idOffset + (repeat * _profile.Companies) + i;
                        idParam.Value = rowId;
                        nameParam.Value = WorkloadDataFactory.CompanyName(rowId, _data.Seed);
                        taxParam.Value = recordCompanyIds ? $"TAX-{i:D9}" : $"TAX-S01-{rowId:D12}";
                        await cmd.ExecuteNonQueryAsync();
                        if (recordCompanyIds)
                        {
                            _companyIds.Add(i);
                        }

                        fallbackCount++;
                    }
                }
            }

            using (var setupTx = _db.BeginTransaction())
            {
                await InsertCompanyRowsAsync(setupTx, repeats: 1, idOffset: 0, recordCompanyIds: true);
                setupTx.Commit();
            }

            if (companyInsertRepeats > 1)
            {
                using var scaleTx = _db.BeginTransaction();
                await InsertCompanyRowsAsync(
                    scaleTx,
                    repeats: companyInsertRepeats - 1,
                    idOffset: _profile.Companies,
                    recordCompanyIds: false);
                scaleTx.Rollback();
            }

            return fallbackCount;
        }));

        _results.Add(await RunScenarioAsync("02. Bulk Insert Users", async () =>
        {
            if (_db.Connection is DecentDBConnection decentConnection)
            {
                int count = 0;
                int batchRows = 0;
                var i64Values = new long[DecentDbBatchRows * 2];
                var textValues = new List<byte[]>(DecentDbBatchRows * 3);
                using var bulkTx = _db.BeginTransaction();

                for (int c = 0; c < _profile.Companies; c++)
                {
                    int companyId = _companyIds[c % _companyIds.Count];
                    for (int u = 0; u < _profile.UsersPerCompany; u++)
                    {
                        int id = count + 1;
                        var i64Offset = batchRows * 2;
                        i64Values[i64Offset] = id;
                        i64Values[i64Offset + 1] = companyId;
                        textValues.Add(Encoding.UTF8.GetBytes($"user{id:D9}@bench.local"));
                        textValues.Add(Encoding.UTF8.GetBytes($"User {id:D9}"));
                        textValues.Add(Encoding.UTF8.GetBytes(WorkloadDataFactory.Role(id, _data.Seed)));
                        _userIds.Add(id);
                        count++;
                        batchRows++;

                        if (batchRows == DecentDbBatchRows)
                        {
                            ExecuteDecentDbUserBatch(decentConnection, batchRows, i64Values, textValues);
                            batchRows = 0;
                            textValues.Clear();
                        }
                    }
                }

                ExecuteDecentDbUserBatch(decentConnection, batchRows, i64Values, textValues);
                bulkTx.Commit();
                return count;
            }

            int fallbackCount = 0;
            using var tx = _db.BeginTransaction();
            using var cmd = CreatePreparedCommand(
                tx,
                "INSERT INTO users (company_id, email, full_name, role, active) VALUES (@cid, @email, @name, @role, TRUE);",
                ("@cid", DbType.Int64),
                ("@email", DbType.String),
                ("@name", DbType.String),
                ("@role", DbType.String));
            var companyParam = Parameter(cmd, 0);
            var emailParam = Parameter(cmd, 1);
            var nameParam = Parameter(cmd, 2);
            var roleParam = Parameter(cmd, 3);

            for (int c = 0; c < _profile.Companies; c++)
            {
                int companyId = _companyIds[c % _companyIds.Count];
                for (int u = 0; u < _profile.UsersPerCompany; u++)
                {
                    int id = fallbackCount + 1;
                    companyParam.Value = companyId;
                    emailParam.Value = $"user{id:D9}@bench.local";
                    nameParam.Value = $"User {id:D9}";
                    roleParam.Value = WorkloadDataFactory.Role(id, _data.Seed);
                    await cmd.ExecuteNonQueryAsync();
                    _userIds.Add(id);
                    fallbackCount++;
                }
            }
            tx.Commit();
            return fallbackCount;
        }, rowsAffected: _profile.Companies * _profile.UsersPerCompany));

        _results.Add(await RunScenarioAsync("03. Bulk Insert Addresses", async () =>
        {
            if (_db.Connection is DecentDBConnection decentConnection)
            {
                int count = 0;
                int batchRows = 0;
                var i64Values = new long[DecentDbTextHeavyBatchRows * 2];
                var textValues = new List<byte[]>(DecentDbTextHeavyBatchRows * 5);
                using var bulkTx = _db.BeginTransaction();

                foreach (var userId in _userIds)
                {
                    for (int a = 0; a < _profile.AddressesPerUser; a++)
                    {
                        int id = count + 1;
                        var i64Offset = batchRows * 2;
                        i64Values[i64Offset] = id;
                        i64Values[i64Offset + 1] = userId;
                        textValues.Add(Encoding.UTF8.GetBytes($"{id} Benchmark Blvd"));
                        textValues.Add(Encoding.UTF8.GetBytes(WorkloadDataFactory.City(userId, a, _data.Seed)));
                        textValues.Add(Encoding.UTF8.GetBytes(WorkloadDataFactory.Region(userId, a, _data.Seed)));
                        textValues.Add(Encoding.UTF8.GetBytes($"{10000 + (count % 90000)}"));
                        textValues.Add(Encoding.UTF8.GetBytes(a == 0 ? "billing" : "shipping"));
                        count++;
                        batchRows++;

                        if (batchRows == DecentDbTextHeavyBatchRows)
                        {
                            ExecuteDecentDbAddressBatch(decentConnection, batchRows, i64Values, textValues);
                            batchRows = 0;
                            textValues.Clear();
                        }
                    }
                }

                ExecuteDecentDbAddressBatch(decentConnection, batchRows, i64Values, textValues);
                bulkTx.Commit();
                return count;
            }

            int fallbackCount = 0;
            using var tx = _db.BeginTransaction();
            using var cmd = CreatePreparedCommand(
                tx,
                "INSERT INTO addresses (user_id, line1, city, region, postal_code, country, kind) VALUES (@uid, @line1, @city, @region, @postal, @country, @kind);",
                ("@uid", DbType.Int64),
                ("@line1", DbType.String),
                ("@city", DbType.String),
                ("@region", DbType.String),
                ("@postal", DbType.String),
                ("@country", DbType.String),
                ("@kind", DbType.String));
            var userParam = Parameter(cmd, 0);
            var lineParam = Parameter(cmd, 1);
            var cityParam = Parameter(cmd, 2);
            var regionParam = Parameter(cmd, 3);
            var postalParam = Parameter(cmd, 4);
            var countryParam = Parameter(cmd, 5);
            var kindParam = Parameter(cmd, 6);
            countryParam.Value = "US";

            foreach (var userId in _userIds)
            {
                for (int a = 0; a < _profile.AddressesPerUser; a++)
                {
                    userParam.Value = userId;
                    lineParam.Value = $"{fallbackCount + 1} Benchmark Blvd";
                    cityParam.Value = WorkloadDataFactory.City(userId, a, _data.Seed);
                    regionParam.Value = WorkloadDataFactory.Region(userId, a, _data.Seed);
                    postalParam.Value = $"{10000 + (fallbackCount % 90000)}";
                    kindParam.Value = a == 0 ? "billing" : "shipping";
                    await cmd.ExecuteNonQueryAsync();
                    fallbackCount++;
                }
            }
            tx.Commit();
            return fallbackCount;
        }, rowsAffected: _userIds.Count * _profile.AddressesPerUser));

        _results.Add(await RunScenarioAsync("04. Bulk Insert Invoices", async () =>
        {
            if (_db.Connection is DecentDBConnection decentConnection)
            {
                int bulkCount = 0;
                int batchRows = 0;
                var i64Values = new long[DecentDbBatchRows * 4];
                var f64Values = new double[DecentDbBatchRows];
                var textValues = new List<byte[]>(DecentDbBatchRows * 3);
                using var bulkTx = _db.BeginTransaction();

                foreach (var userId in _userIds)
                {
                    int companyId = ((userId - 1) / _profile.UsersPerCompany) + 1;
                    for (int inv = 0; inv < _profile.InvoicesPerUser; inv++)
                    {
                        int id = bulkCount + 1;
                        var issued = WorkloadDataFactory.IssuedDateUtc(id, _data.BaseUtc, _data.Seed);
                        var due = WorkloadDataFactory.DueDateUtc(id, _data.BaseUtc, _data.Seed);
                        var i64Offset = batchRows * 4;
                        i64Values[i64Offset] = id;
                        i64Values[i64Offset + 1] = userId;
                        i64Values[i64Offset + 2] = companyId;
                        i64Values[i64Offset + 3] = WorkloadDataFactory.InvoicePaid(id, _data.Seed) ? 1 : 0;
                        f64Values[batchRows] = WorkloadDataFactory.InvoiceTotal(id, _data.Seed);
                        textValues.Add(Encoding.UTF8.GetBytes($"INV-{id:D12}"));
                        textValues.Add(Encoding.UTF8.GetBytes(issued.ToString("O")));
                        textValues.Add(Encoding.UTF8.GetBytes(due.ToString("O")));
                        _invoiceIds.Add(id);
                        bulkCount++;
                        batchRows++;

                        if (batchRows == DecentDbBatchRows)
                        {
                            ExecuteDecentDbInvoiceBatch(decentConnection, batchRows, i64Values, f64Values, textValues);
                            batchRows = 0;
                            textValues.Clear();
                        }
                    }
                }

                ExecuteDecentDbInvoiceBatch(decentConnection, batchRows, i64Values, f64Values, textValues);
                bulkTx.Commit();
                return bulkCount;
            }

            int count = 0;
            using var tx = _db.BeginTransaction();
            using var cmd = CreatePreparedCommand(
                tx,
                "INSERT INTO invoices (id, user_id, company_id, invoice_number, issued_at, due_at, total, paid) VALUES (@id, @uid, @cid, @num, @issued, @due, @total, @paid);",
                ("@id", DbType.Int64),
                ("@uid", DbType.Int64),
                ("@cid", DbType.Int64),
                ("@num", DbType.String),
                ("@issued", DbType.String),
                ("@due", DbType.String),
                ("@total", DbType.Double),
                ("@paid", DbType.Boolean));
            var idParam = Parameter(cmd, 0);
            var userParam = Parameter(cmd, 1);
            var companyParam = Parameter(cmd, 2);
            var numberParam = Parameter(cmd, 3);
            var issuedParam = Parameter(cmd, 4);
            var dueParam = Parameter(cmd, 5);
            var totalParam = Parameter(cmd, 6);
            var paidParam = Parameter(cmd, 7);

            foreach (var userId in _userIds)
            {
                int companyId = ((userId - 1) / _profile.UsersPerCompany) + 1;
                for (int inv = 0; inv < _profile.InvoicesPerUser; inv++)
                {
                    int id = count + 1;
                    var issued = WorkloadDataFactory.IssuedDateUtc(id, _data.BaseUtc, _data.Seed);
                    var due = WorkloadDataFactory.DueDateUtc(id, _data.BaseUtc, _data.Seed);
                    idParam.Value = id;
                    userParam.Value = userId;
                    companyParam.Value = companyId;
                    numberParam.Value = $"INV-{id:D12}";
                    issuedParam.Value = issued.ToString("O");
                    dueParam.Value = due.ToString("O");
                    totalParam.Value = WorkloadDataFactory.InvoiceTotal(id, _data.Seed);
                    paidParam.Value = WorkloadDataFactory.InvoicePaid(id, _data.Seed);
                    await cmd.ExecuteNonQueryAsync();
                    _invoiceIds.Add(id);
                    count++;
                }
            }
            tx.Commit();
            return count;
        }, rowsAffected: _userIds.Count * _profile.InvoicesPerUser));

        _results.Add(await RunScenarioAsync("05. Bulk Insert Invoice Items", async () =>
        {
            if (_db.Connection is DecentDBConnection decentConnection)
            {
                int bulkCount = 0;
                int batchRows = 0;
                var i64Values = new long[DecentDbBatchRows];
                var f64Values = new double[DecentDbBatchRows * 2];
                var textValues = new List<byte[]>(DecentDbBatchRows);
                using var bulkTx = _db.BeginTransaction();

                foreach (var invoiceId in _invoiceIds)
                {
                    for (int it = 0; it < _profile.ItemsPerInvoice; it++)
                    {
                        i64Values[batchRows] = invoiceId;
                        var f64Offset = batchRows * 2;
                        f64Values[f64Offset] = WorkloadDataFactory.Quantity(invoiceId, it, _data.Seed);
                        f64Values[f64Offset + 1] = WorkloadDataFactory.UnitPrice(invoiceId, it, _data.Seed);
                        textValues.Add(Encoding.UTF8.GetBytes(WorkloadDataFactory.Sku(invoiceId, it, _data.Seed)));
                        bulkCount++;
                        batchRows++;

                        if (batchRows == DecentDbBatchRows)
                        {
                            ExecuteDecentDbInvoiceItemBatch(decentConnection, batchRows, i64Values, f64Values, textValues);
                            batchRows = 0;
                            textValues.Clear();
                        }
                    }
                }

                ExecuteDecentDbInvoiceItemBatch(decentConnection, batchRows, i64Values, f64Values, textValues);
                bulkTx.Commit();
                return bulkCount;
            }

            int count = 0;
            using var tx = _db.BeginTransaction();
            using var cmd = CreatePreparedCommand(
                tx,
                "INSERT INTO invoice_items (invoice_id, sku, description, quantity, unit_price) VALUES (@inv, @sku, 'Benchmark item', @qty, @price);",
                ("@inv", DbType.Int64),
                ("@sku", DbType.String),
                ("@qty", DbType.Double),
                ("@price", DbType.Double));
            var invoiceParam = Parameter(cmd, 0);
            var skuParam = Parameter(cmd, 1);
            var quantityParam = Parameter(cmd, 2);
            var priceParam = Parameter(cmd, 3);

            foreach (var invoiceId in _invoiceIds)
            {
                for (int it = 0; it < _profile.ItemsPerInvoice; it++)
                {
                    invoiceParam.Value = invoiceId;
                    skuParam.Value = WorkloadDataFactory.Sku(invoiceId, it, _data.Seed);
                    quantityParam.Value = WorkloadDataFactory.Quantity(invoiceId, it, _data.Seed);
                    priceParam.Value = WorkloadDataFactory.UnitPrice(invoiceId, it, _data.Seed);
                    await cmd.ExecuteNonQueryAsync();
                    count++;
                }
            }
            tx.Commit();
            return count;
        }, rowsAffected: _invoiceIds.Count * _profile.ItemsPerInvoice));

        WriteExplainPlans();

        var pointReadLookups = Math.Max(_profile.PointReadSamples, MinimumScaledPointReadLookups);
        _results.Add(await RunScenarioAsync("06. Point Reads (PK lookup)", () =>
        {
            long read = 0;
            var pointReadIds = _data.PointReadUserIds;
            if (pointReadIds.Count == 0)
            {
                throw new InvalidOperationException("Point-read benchmark requires at least one existing user id.");
            }

            if (_nativeRunnerActive)
            {
                return Task.FromResult(_nativeRunner!.RunPointReadScenario(pointReadIds, pointReadLookups));
            }

            using var cmd = CreatePreparedCommand(
                null,
                "SELECT id, email, full_name FROM users WHERE id = @id;",
                ("@id", DbType.Int64));
            var idParam = Parameter(cmd, 0);
            long checksum = 0;

            for (int i = 0; i < pointReadLookups; i++)
            {
                idParam.Value = pointReadIds[i % pointReadIds.Count];
                using var reader = cmd.ExecuteReader();
                while (reader.Read())
                {
                    read++;
                    checksum += reader.GetInt64(0);
                    checksum += reader.GetString(1).Length;
                    checksum += reader.GetString(2).Length;
                }
            }
            if (checksum == long.MinValue)
            {
                throw new InvalidOperationException("Unreachable checksum guard.");
            }
            return Task.FromResult(read);
        }, rowsRead: pointReadLookups));

        var joinedAggregateRepeats = 1;
        _results.Add(await RunScenarioAsync("07a. Raw Joined Aggregate", () =>
        {
            using var cmd = CreatePreparedCommand(
                null,
                """
                SELECT c.name, COUNT(DISTINCT u.id) AS user_count, COALESCE(SUM(i.total), 0) AS revenue
                FROM companies c
                LEFT JOIN users u ON u.company_id = c.id
                LEFT JOIN invoices i ON i.user_id = u.id
                GROUP BY c.id, c.name
                ORDER BY revenue DESC;
                """);
            long read = 0;

            for (int repeat = 0; repeat < joinedAggregateRepeats; repeat++)
            {
                using var reader = cmd.ExecuteReader();
                while (reader.Read()) read++;
            }

            return Task.FromResult(read);
        }, rowsRead: (long)_profile.Companies * joinedAggregateRepeats));

        _results.Add(await RunScenarioAsync("07b. Build Revenue Summary", async () =>
        {
            await _db.ExecuteNonQueryAsync("DELETE FROM company_revenue;");
            await _db.ExecuteNonQueryAsync("""
                INSERT INTO company_revenue (company_id, user_count, revenue)
                SELECT c.id, COUNT(DISTINCT u.id), COALESCE(SUM(i.total), 0)
                FROM companies c
                JOIN users u ON u.company_id = c.id
                LEFT JOIN invoices i ON i.user_id = u.id
                GROUP BY c.id;
                """);
            try
            {
                await _db.CheckpointAsync();
            }
            catch (Exception ex)
            {
                Console.WriteLine($"[WARN] Checkpoint skipped in scenario 07b due non-fatal error: {ex.Message}");
            }
            return _profile.Companies;
        }, rowsAffected: _profile.Companies));

        _results.Add(await RunScenarioAsync("07c. Read Revenue Summary", () =>
        {
            using var cmd = CreatePreparedCommand(
                null,
                """
                SELECT c.name, cr.user_count, cr.revenue
                FROM companies c
                JOIN company_revenue cr ON cr.company_id = c.id
                ORDER BY cr.revenue DESC;
                """);
            long read = 0;

            for (int repeat = 0; repeat < joinedAggregateRepeats; repeat++)
            {
                using var reader = cmd.ExecuteReader();
                while (reader.Read()) read++;
            }

            return Task.FromResult(read);
        }, rowsRead: (long)_profile.Companies * joinedAggregateRepeats));

        _results.Add(await RunScenarioAsync("08. Substring Search (LIKE %pattern%)", () =>
        {
            long read = 0;
            using var cmd = CreatePreparedCommand(
                null,
                "SELECT id, full_name FROM users WHERE full_name LIKE @pattern;",
                ("@pattern", DbType.String));
            var patternParam = Parameter(cmd, 0);

            for (int i = 0; i < _profile.SearchSamples; i++)
            {
                patternParam.Value = _data.SearchPatterns[i];
                using var reader = cmd.ExecuteReader();
                while (reader.Read()) read++;
            }
            return Task.FromResult(read);
        }, rowsRead: _profile.SearchSamples));

        _results.Add(await RunScenarioAsync("09. Update Invoices Paid", () =>
        {
            if (_nativeRunnerActive)
            {
                return Task.FromResult(_nativeRunner!.RunUpdatePaidScenario(100.0));
            }

            using var tx = _db.BeginTransaction();
            using var update = CreatePreparedCommand(
                tx,
                "UPDATE invoices SET paid = TRUE WHERE paid = FALSE AND total < @max;",
                ("@max", DbType.Double));
            Parameter(update, 0).Value = 100.0;
            var affected = update.ExecuteNonQuery();

            using var verify = CreatePreparedCommand(
                tx,
                "SELECT COUNT(*) FROM invoices WHERE paid = TRUE AND total < @max;",
                ("@max", DbType.Double));
            Parameter(verify, 0).Value = 100.0;
            _ = verify.ExecuteScalar();
            tx.Commit();
            return Task.FromResult((long)affected);
        }, rowsAffected: null));

        _results.Add(await RunScenarioAsync("10. Complex Window/Analytic Query", () =>
        {
            if (_nativeRunnerActive)
            {
                return Task.FromResult(_nativeRunner!.RunWindowScenario());
            }

            using var readerCmd = _db.Connection.CreateCommand();
            readerCmd.CommandText = """
                SELECT user_id, invoice_number, total,
                       ROW_NUMBER() OVER (PARTITION BY user_id ORDER BY total DESC) AS rn,
                       RANK() OVER (PARTITION BY user_id ORDER BY total DESC) AS rnk
                FROM invoices;
                """;
            using var reader = readerCmd.ExecuteReader();
            long read = 0;
            while (reader.Read()) read++;
            return Task.FromResult(read);
        }, rowsRead: _invoiceIds.Count));

        _results.Add(await RunScenarioAsync("11. View Query (Unpaid Invoices)", () =>
        {
            using var readerCmd = _db.Connection.CreateCommand();
            readerCmd.CommandText = "SELECT * FROM v_unpaid_invoices ORDER BY due_at DESC LIMIT 1000;";
            using var reader = readerCmd.ExecuteReader();
            long read = 0;
            while (reader.Read()) read++;
            return Task.FromResult(read);
        }, rowsRead: 1000));

        _results.Add(await RunScenarioAsync("12. Delete Cascade Test", () =>
        {
            if (_nativeRunnerActive)
            {
                return Task.FromResult(_nativeRunner!.RunDeleteCascadeScenario(_data.DeleteCompanyIds));
            }

            using var tx = _db.BeginTransaction();
            var ids = _data.DeleteCompanyIds;
            var idPlaceholders = string.Join(",", ids.Select((_, i) => $"@id{i}"));
            var idParameters = ids.Select((_, i) => ($"@id{i}", DbType.Int64)).ToArray();
            using var cmd = CreatePreparedCommand(
                tx,
                $"DELETE FROM companies WHERE id IN ({idPlaceholders});",
                idParameters);

            for (var i = 0; i < ids.Count; i++)
            {
                Parameter(cmd, i).Value = ids[i];
            }

            var deleted = (long)cmd.ExecuteNonQuery();
            tx.Commit();
            return Task.FromResult(deleted);
        }, rowsAffected: _data.DeleteCompanyIds.Count));

            await _db.CheckpointAsync();
        }
        finally
        {
            if (_nativeRunnerActive)
            {
                _nativeRunner?.Dispose();
            }
        }
    }

    private async Task<BenchmarkResult> RunScenarioAsync(
        string name,
        Func<Task<long>> action,
        long? rowsAffected = null,
        long? rowsRead = null,
        Func<Task>? setup = null,
        Func<Task>? cleanup = null)
    {
        if (setup is not null)
        {
            await setup();
        }

            var sw = new Stopwatch();
        long actualRows = 0;

        try
        {
            sw.Start();
            actualRows = await action();
            sw.Stop();
        }
        finally
        {
            if (sw.IsRunning)
            {
                sw.Stop();
            }

            if (cleanup is not null)
            {
                await cleanup();
            }
        }

        var result = new BenchmarkResult(
            name,
            _db.EngineName,
            WorkloadProfiles.TotalRows(_profile),
            sw.Elapsed,
            rowsAffected ?? (rowsRead.HasValue ? null : actualRows),
            rowsRead.HasValue ? actualRows : null);
        Console.WriteLine($"  {name}: {result.Duration.TotalSeconds:F3}s  rows={actualRows:N0}  ops/s={result.OperationsPerSecond:N0}");
        return result;
    }

    private void WriteExplainPlans()
    {
        if (_explainDirectory is null)
        {
            return;
        }

        Directory.CreateDirectory(_explainDirectory);
        foreach (var (fileName, sql) in ExplainQueries())
        {
            var path = Path.Combine(_explainDirectory, fileName);
            var text = new StringBuilder();
            text.AppendLine("SQL:");
            text.AppendLine(sql.Trim());
            text.AppendLine();
            text.AppendLine("PLAN:");

            try
            {
                text.AppendLine(_db.ExplainPlan(sql).TrimEnd());
            }
            catch (Exception ex)
            {
                text.AppendLine($"EXPLAIN failed: {ex.GetType().Name}: {ex.Message}");
            }

            File.WriteAllText(path, text.ToString());
        }
    }

    private static IEnumerable<(string FileName, string Sql)> ExplainQueries()
    {
        yield return ("06-point-read-pk.txt", "SELECT id, email, full_name FROM users WHERE id = 1;");
        yield return ("07a-raw-joined-aggregate.txt", """
            SELECT c.name, COUNT(DISTINCT u.id) AS user_count, COALESCE(SUM(i.total), 0) AS revenue
            FROM companies c
            LEFT JOIN users u ON u.company_id = c.id
            LEFT JOIN invoices i ON i.user_id = u.id
            GROUP BY c.id, c.name
            ORDER BY revenue DESC;
            """);
        yield return ("07b-build-revenue-summary.txt", """
            DELETE FROM company_revenue;
            INSERT INTO company_revenue (company_id, user_count, revenue)
            SELECT c.id, COUNT(DISTINCT u.id), COALESCE(SUM(i.total), 0)
            FROM companies c
            JOIN users u ON u.company_id = c.id
            LEFT JOIN invoices i ON i.user_id = u.id
            GROUP BY c.id;
            """);
        yield return ("07c-read-revenue-summary.txt", """
            SELECT c.name, cr.user_count, cr.revenue
            FROM companies c
            JOIN company_revenue cr ON cr.company_id = c.id
            ORDER BY cr.revenue DESC;
            """);
        yield return ("08-substring-search.txt", "SELECT id, full_name FROM users WHERE full_name LIKE '%001%';");
        yield return ("10-window-query.txt", """
            SELECT user_id, invoice_number, total,
                   ROW_NUMBER() OVER (PARTITION BY user_id ORDER BY total DESC) AS rn,
                   RANK() OVER (PARTITION BY user_id ORDER BY total DESC) AS rnk
            FROM invoices;
            """);
        yield return ("11-view-unpaid-invoices.txt", "SELECT * FROM v_unpaid_invoices ORDER BY due_at DESC LIMIT 1000;");
    }

    private async Task InitializeSchemaAsync()
    {
        var ddl = _engine == DatabaseEngine.DecentDB ? Schema.DecentDdl : Schema.SqliteDdl;
        foreach (var statement in ddl.Split(';', StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries))
        {
            if (string.IsNullOrWhiteSpace(statement)) continue;
            await _db.ExecuteNonQueryAsync(statement + ";");
        }
    }

}

public static class Reporter
{
    public static void PrintComparison(IReadOnlyList<BenchmarkResult> decent, IReadOnlyList<BenchmarkResult> sqlite)
    {
        Console.WriteLine("\n" + new string('=', 120));
        Console.WriteLine("BENCHMARK COMPARISON: DecentDB vs SQLite (lower duration is better)");
        Console.WriteLine(new string('=', 120));
        Console.WriteLine(string.Format("{0,-40} {1,14} {2,14} {3,12} {4,10} {5,14} {6,14}", "Scenario", "DecentDB (s)", "SQLite (s)", "Ratio S/D", "Winner", "D rows/s", "S rows/s"));
        Console.WriteLine(new string('-', 120));

        for (int i = 0; i < decent.Count; i++)
        {
            var d = decent[i];
            var s = sqlite.FirstOrDefault(r => r.Scenario == d.Scenario);
            if (s == null) continue;
            var ratio = s.Duration.TotalSeconds / d.Duration.TotalSeconds;
            var winner = d.Duration < s.Duration ? "DecentDB" : (s.Duration < d.Duration ? "SQLite" : "Tie");
            Console.WriteLine($"{d.Scenario,-40} {d.Duration.TotalSeconds,14:F3} {s.Duration.TotalSeconds,14:F3} {ratio,12:F2} {winner,10} {d.OperationsPerSecond,14:N0} {s.OperationsPerSecond,14:N0}");
        }

        Console.WriteLine(new string('-', 120));
        var decentTotal = decent.Sum(r => r.Duration.TotalSeconds);
        var sqliteTotal = sqlite.Sum(r => r.Duration.TotalSeconds);
        Console.WriteLine(string.Format("{0,-40} {1,14:F3} {2,14:F3} {3,12:F2} {4,10}", "TOTAL", decentTotal, sqliteTotal, sqliteTotal / decentTotal, decentTotal < sqliteTotal ? "DecentDB" : "SQLite"));
        Console.WriteLine(new string('=', 120));
        Console.WriteLine();
        Console.WriteLine("NOTES:");
        Console.WriteLine("- DecentDB tuned with embedded_fast profile, 128MB cache, async_commit:10, wal_autocheckpoint=0, hot row sources, 2MB plan cache.");
        Console.WriteLine("- SQLite tuned with WAL mode, synchronous=NORMAL, 64MB cache, foreign_keys=ON, temp_store=MEMORY.");
        Console.WriteLine("- Hot-loop DML and point/search reads reuse prepared ADO.NET commands and parameters for both engines.");
        Console.WriteLine("- Tiny/noisy company insert, point-read, and summary-read scenarios are scaled to stable operation counts.");
        Console.WriteLine("- Both engines use equivalent tuned CRM schemas with FKs, workload indexes, partial/covering indexes.");
        Console.WriteLine("- Aggregate workload now measures raw aggregation, summary maintenance, and summary-read separately.");
        Console.WriteLine("- DecentDB supports native UUID, RETURNING, TRUNCATE, DISTINCT ON, trigram indexes, and richer analytics.");
        Console.WriteLine("- SQLite has broader ecosystem, smaller footprint, and broader legacy SQL surface.");
    }

    public static void PrintSummary(IReadOnlyList<BenchmarkSummary> summary)
    {
        if (summary.Count == 0)
        {
            return;
        }

        Console.WriteLine("\n" + new string('=', 112));
        Console.WriteLine("MEASURED ITERATION SUMMARY");
        Console.WriteLine(new string('=', 112));
        Console.WriteLine(string.Format("{0,-40} {1,-9} {2,5} {3,12} {4,12} {5,12} {6,12}", "Scenario", "Engine", "N", "mean ms", "median ms", "p95 ms", "stddev ms"));
        Console.WriteLine(new string('-', 112));
        foreach (var item in summary)
        {
            Console.WriteLine($"{item.Scenario,-40} {item.Engine,-9} {item.Iterations,5:N0} {item.MeanMs,12:F3} {item.MedianMs,12:F3} {item.P95Ms,12:F3} {item.StdDevMs,12:F3}");
        }

        Console.WriteLine(new string('=', 112));
    }

    public static void PrintFeatureMatrix()
    {
        Console.WriteLine();
        Console.WriteLine("FEATURE HIGHLIGHTS:");
        Console.WriteLine(new string('-', 80));
        Console.WriteLine(string.Format("{0,-40} {1,-15} {2,-15}", "Feature", "DecentDB", "SQLite"));
        Console.WriteLine(new string('-', 80));
        PrintRow("Foreign Keys", "Yes", "Yes (must enable)");
        PrintRow("Indexes (B-tree)", "Yes", "Yes");
        PrintRow("Trigram substring indexes", "Yes", "No (FTS5 separate)");
        PrintRow("Views", "Yes", "Yes");
        PrintRow("RETURNING clause", "Yes", "Yes (3.35+)");
        PrintRow("TRUNCATE TABLE", "Yes", "No");
        PrintRow("DISTINCT ON", "Yes", "No");
        PrintRow("Native UUID type", "Yes", "No");
        PrintRow("Statistical aggregates", "Built-in", "Extension only");
        PrintRow("Default durability", "Fsync-on-commit", "PRAGMA-tuned");
        PrintRow("ATTACH DATABASE", "No", "Yes");
        PrintRow("Cross-process sharing", "Native coordination", "File locking");
        Console.WriteLine(new string('-', 80));

        static void PrintRow(string feature, string decent, string sqlite)
        {
            Console.WriteLine($"{feature,-40} {decent,-15} {sqlite,-15}");
        }
    }
}

public class Program
{
    public static async Task Main(string[] args)
    {
        var options = RunOptions.Parse(args);
        var size = options.Size;
        var startedAt = DateTimeOffset.UtcNow;

        var profile = WorkloadProfiles.Get(size);
        var data = WorkloadDataFactory.Create(profile, options.DataSeed);
        var runId = $"{size.ToString().ToLowerInvariant()}-{DateTime.UtcNow:yyyyMMddHHmmss}";
        Console.WriteLine($"Scenario: {size}");
        Console.WriteLine($"Approximate total rows: {WorkloadProfiles.TotalRows(profile):N0}");
        Console.WriteLine($"Warmup iterations: {options.WarmupIterations}");
        Console.WriteLine($"Measured iterations: {options.Iterations}");
        Console.WriteLine($"Data seed: {options.DataSeed}");
        Console.WriteLine($"Durability profile: {options.DurabilityProfile}");
        Console.WriteLine($"DecentDB native hot paths: {(options.UseNativeDecentDbHotPaths ? "enabled" : "disabled (default)")}");
        Console.WriteLine($"Engine selection: {options.EngineSelection}");

        var outputRoot = options.OutputDirectory is null
            ? Path.Combine(Path.GetTempPath(), $"decentdb-crm-bench-{runId}")
            : Path.GetFullPath(options.OutputDirectory);
        var root = Path.Combine(outputRoot, runId);
        Directory.CreateDirectory(root);

        var jsonResults = new List<BenchmarkScenarioResult>();
        List<BenchmarkResult>? lastDecentResults = null;
        List<BenchmarkResult>? lastSqliteResults = null;
        bool lastDecentNativeHotPathsActive = false;
        var measuredEngineOrder = GetEngineOrderForIteration(1, options.AlternateOrder, options.EngineSelection);

        for (var warmup = 1; warmup <= options.WarmupIterations; warmup++)
        {
            Console.WriteLine($"\n=== Warmup {warmup:N0}/{options.WarmupIterations:N0} (discarded) ===");
            var warmupRoot = Path.Combine(root, $"warmup-{warmup:D3}");
            Directory.CreateDirectory(warmupRoot);
            await RunEnginePairAsync("warmup", warmup, warmupRoot, runId, profile, data, options.AlternateOrder, options.DurabilityProfile, options.EngineSelection, recordResults: false, jsonResults, options.UseNativeDecentDbHotPaths);
        }

        for (var iteration = 1; iteration <= options.Iterations; iteration++)
        {
            Console.WriteLine($"\n=== Iteration {iteration:N0}/{options.Iterations:N0} ===");
            var iterationRoot = Path.Combine(root, $"iteration-{iteration:D3}");
            Directory.CreateDirectory(iterationRoot);

            (lastDecentResults, lastSqliteResults, lastDecentNativeHotPathsActive, measuredEngineOrder) = await RunEnginePairAsync(
                "measurement",
                iteration,
                iterationRoot,
                runId,
                profile,
                data,
                options.AlternateOrder,
                options.DurabilityProfile,
                options.EngineSelection,
                recordResults: true,
                jsonResults,
                options.UseNativeDecentDbHotPaths);
        }

        if (lastDecentResults is not null && lastSqliteResults is not null)
        {
            Reporter.PrintComparison(lastDecentResults, lastSqliteResults);
        }

        var summary = BuildSummary(jsonResults);
        Reporter.PrintSummary(summary);
        Reporter.PrintFeatureMatrix();

        if (options.JsonPath is not null)
        {
            var manifest = new BenchmarkManifest(
                "dotnet-crm",
                runId,
                size.ToString(),
                WorkloadProfiles.TotalRows(profile),
                options.Iterations,
                options.WarmupIterations,
                options.AlternateOrder,
                options.DataSeed,
                startedAt,
                DateTimeOffset.UtcNow,
                root,
                Environment.MachineName,
                Environment.ProcessorCount,
                Environment.Version.ToString(),
                RuntimeInformation.OSDescription,
                RuntimeInformation.OSArchitecture.ToString(),
                RuntimeInformation.ProcessArchitecture.ToString(),
                typeof(DecentDBConnection).Assembly.GetName().Version?.ToString() ?? "unknown",
                DecentDBConnection.EngineVersion(),
                DecentDBConnection.AbiVersion(),
                options.DurabilityProfile.ToString(),
                options.UseNativeDecentDbHotPaths,
                lastDecentNativeHotPathsActive,
                typeof(SqliteConnection).Assembly.GetName().Version?.ToString() ?? "unknown",
                GetSQLiteNativeVersion(),
                measuredEngineOrder,
                GetDotnetSdkVersion(),
                GetAssemblyVersion(typeof(DecentDBConnection)),
                "DecentDB.AdoNet/" + GetAssemblyVersion(typeof(DecentDBConnection)));

            var payload = new BenchmarkJsonOutput(manifest, jsonResults, summary);
            var jsonPath = Path.GetFullPath(options.JsonPath);
            var directory = Path.GetDirectoryName(jsonPath);
            if (!string.IsNullOrEmpty(directory))
            {
                Directory.CreateDirectory(directory);
            }

            var json = JsonSerializer.Serialize(
                payload,
                new JsonSerializerOptions
                {
                    WriteIndented = true,
                    DefaultIgnoreCondition = JsonIgnoreCondition.WhenWritingNull
                });
            await File.WriteAllTextAsync(jsonPath, json);
            Console.WriteLine($"\nJSON results written to: {jsonPath}");
        }

        Console.WriteLine($"\nBenchmark files written to: {root}");
    }

    private static async Task<(List<BenchmarkResult>? DecentDB, List<BenchmarkResult>? SQLite, bool DecentDBNativeHotPathsActive, IReadOnlyList<string> EngineOrder)> RunEnginePairAsync(
        string phase,
        int iteration,
        string iterationRoot,
        string runId,
        WorkloadProfile profile,
        WorkloadData data,
        bool alternateOrder,
        DurabilityProfile durabilityProfile,
        EngineSelection engineSelection,
        bool recordResults,
        List<BenchmarkScenarioResult> jsonResults,
        bool useNativeDecentDbHotPaths)
    {
        List<BenchmarkResult>? decentResults = null;
        List<BenchmarkResult>? sqliteResults = null;
        bool decentNativeHotPathsActive = false;
        var order = GetEngineOrderForIteration(iteration, alternateOrder, engineSelection).ToList();

        for (var orderIndex = 0; orderIndex < order.Count; orderIndex++)
        {
            var (engineResults, nativeRunnerActive) = await RunEngineAsync(
                order[orderIndex],
                profile,
                data,
                phase,
                iteration,
                orderIndex + 1,
                iterationRoot,
                runId,
                durabilityProfile,
                recordResults,
                jsonResults,
                useNativeDecentDbHotPaths);

            if (order[orderIndex] == DatabaseEngine.DecentDB)
            {
                decentResults = engineResults;
                decentNativeHotPathsActive = nativeRunnerActive;
            }
            else
            {
                sqliteResults = engineResults;
            }
        }

        return (decentResults, sqliteResults, decentNativeHotPathsActive, order.Select(x => x.ToString()).ToArray());
    }

    private static async Task<(List<BenchmarkResult> Results, bool NativeHotPathsActive)> RunEngineAsync(
        DatabaseEngine engine,
        WorkloadProfile profile,
        WorkloadData data,
        string phase,
        int iteration,
        int engineOrder,
        string iterationRoot,
        string runId,
        DurabilityProfile durabilityProfile,
        bool recordResults,
        List<BenchmarkScenarioResult> jsonResults,
        bool useNativeDecentDbHotPaths)
    {
        var engineName = engine == DatabaseEngine.DecentDB ? "decentdb" : "sqlite";
        var engineRoot = Path.Combine(iterationRoot, $"{engineOrder:D2}-{engineName}");
        Directory.CreateDirectory(engineRoot);
        var path = engine == DatabaseEngine.DecentDB
            ? Path.Combine(engineRoot, "decentdb.ddb")
            : Path.Combine(engineRoot, "sqlite.db");
        var explainDirectory = Path.Combine(engineRoot, "explain");

        await using IDatabaseProvider db = engine == DatabaseEngine.DecentDB
            ? new DecentDbProvider(path, durabilityProfile)
            : new SqliteProvider(path, durabilityProfile == DurabilityProfile.Durable);

        await db.OpenAsync();
        var harness = new BenchmarkHarness(db, engine, profile, data, explainDirectory, durabilityProfile, useNativeDecentDbHotPaths);
        await harness.RunAsync();
        var results = harness.Results.ToList();
        var dbBytes = DatabaseFileBytes(path);

        if (recordResults)
        {
            foreach (var result in results)
            {
                jsonResults.Add(new BenchmarkScenarioResult(
                    runId,
                    phase,
                    iteration,
                    result.Engine,
                    engineOrder,
                    result.Scenario,
                    result.TotalRows,
                    result.Duration.TotalMilliseconds,
                    result.RowsAffected,
                    result.RowsRead,
                    result.OperationsPerSecond,
                    path,
                    dbBytes));
            }
        }

        return (results, harness.NativeDecentDbHotPathsActive);
    }

    private static IReadOnlyList<BenchmarkSummary> BuildSummary(IReadOnlyList<BenchmarkScenarioResult> results)
    {
        return results
            .GroupBy(r => new { r.Scenario, r.Engine })
            .OrderBy(g => g.Key.Scenario, StringComparer.Ordinal)
            .ThenBy(g => g.Key.Engine, StringComparer.Ordinal)
            .Select(g =>
            {
                var durations = g.Select(r => r.DurationMs).Order().ToArray();
                var ops = g.Select(r => r.OperationsPerSecond).ToArray();
                return new BenchmarkSummary(
                    g.Key.Scenario,
                    g.Key.Engine,
                    durations.Length,
                    durations.Average(),
                    Percentile(durations, 50),
                    Percentile(durations, 95),
                    durations[0],
                    durations[^1],
                    StdDev(durations),
                    ops.Average());
            })
            .ToArray();
    }

    private static double Percentile(double[] sortedValues, double percentile)
    {
        if (sortedValues.Length == 0)
        {
            return 0;
        }

        if (sortedValues.Length == 1)
        {
            return sortedValues[0];
        }

        var rank = (percentile / 100.0) * (sortedValues.Length - 1);
        var lower = (int)Math.Floor(rank);
        var upper = (int)Math.Ceiling(rank);
        if (lower == upper)
        {
            return sortedValues[lower];
        }

        var weight = rank - lower;
        return sortedValues[lower] + (sortedValues[upper] - sortedValues[lower]) * weight;
    }

    private static double StdDev(double[] values)
    {
        if (values.Length <= 1)
        {
            return 0;
        }

        var mean = values.Average();
        var variance = values.Sum(value => Math.Pow(value - mean, 2)) / (values.Length - 1);
        return Math.Sqrt(variance);
    }

    private static IReadOnlyList<DatabaseEngine> GetEngineOrderForIteration(
        int iteration,
        bool alternateOrder,
        EngineSelection engineSelection)
    {
        var runSQLiteFirst = engineSelection == EngineSelection.All
            && alternateOrder
            && iteration % 2 == 0;

        var order = new List<DatabaseEngine>(2);
        if (engineSelection == EngineSelection.SQLite)
        {
            order.Add(DatabaseEngine.SQLite);
            return order;
        }

        if (engineSelection == EngineSelection.DecentDB)
        {
            order.Add(DatabaseEngine.DecentDB);
            return order;
        }

        if (runSQLiteFirst)
        {
            order.Add(DatabaseEngine.SQLite);
            order.Add(DatabaseEngine.DecentDB);
            return order;
        }

        order.Add(DatabaseEngine.DecentDB);
        order.Add(DatabaseEngine.SQLite);
        return order;
    }

    private static string GetSQLiteNativeVersion()
    {
        try
        {
            using var connection = new SqliteConnection("Data Source=:memory:");
            connection.Open();
            using var command = connection.CreateCommand();
            command.CommandText = "SELECT sqlite_version();";
            return command.ExecuteScalar()?.ToString() ?? "unknown";
        }
        catch
        {
            return "unknown";
        }
    }

    private static string GetDotnetSdkVersion()
    {
        try
        {
            using var process = new Process();
            process.StartInfo = new ProcessStartInfo
            {
                FileName = "dotnet",
                Arguments = "--version",
                RedirectStandardOutput = true,
                UseShellExecute = false,
                CreateNoWindow = true,
            };

            process.Start();
            var output = process.StandardOutput.ReadToEnd().Trim();
            process.WaitForExit(5000);

            return process.ExitCode == 0 && output.Length > 0
                ? output
                : "unknown";
        }
        catch
        {
            return "unknown";
        }
    }

    private static string GetAssemblyVersion(Type targetType)
    {
        try
        {
            return targetType.Assembly.GetName().Version?.ToString() ?? "unknown";
        }
        catch
        {
            return "unknown";
        }
    }

    private static long DatabaseFileBytes(string path)
    {
        long total = 0;
        foreach (var candidate in new[]
        {
            path,
            path + ".wal",
            path + "-wal",
            path + "-shm",
            path + ".coord"
        })
        {
            if (File.Exists(candidate))
            {
                total += new FileInfo(candidate).Length;
            }
        }

        return total;
    }
}
