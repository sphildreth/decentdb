using System.Data.Common;
using Dapper;

var dbPath = Path.GetFullPath(Path.Combine(AppContext.BaseDirectory, "..", "..", "..", "sample.db"));

using var conn = TryOpenDecentDb($"Data Source={dbPath}");
if (conn is null)
{
    Environment.ExitCode = 2;
    return;
}

conn.Execute("CREATE TABLE IF NOT EXISTS artists (id INT PRIMARY KEY, name TEXT)");
conn.Execute("DELETE FROM artists");

conn.Execute(
    "INSERT INTO artists (id, name) VALUES (@id, @name)",
    new[]
    {
        new { id = 1, name = "Alice" },
        new { id = 2, name = "Bob" },
        new { id = 3, name = "Charlie" },
    });

var rows = conn.Query<Artist>(
    "SELECT id, name FROM artists WHERE id >= @minId ORDER BY id LIMIT @limit",
    new { minId = 2, limit = 10 });

foreach (var row in rows)
{
    Console.WriteLine($"{row.Id}: {row.Name}");
}

static DbConnection? TryOpenDecentDb(string connectionString)
{
    try
    {
        var factory = DbProviderFactories.GetFactory("DecentDB");
        var conn = factory.CreateConnection();
        if (conn is null)
        {
            Console.Error.WriteLine("DecentDB provider factory returned null connection.");
            return null;
        }

        conn.ConnectionString = connectionString;
        conn.Open();
        return conn;
    }
    catch (Exception ex)
    {
        Console.Error.WriteLine("This example requires the DecentDB ADO.NET provider (planned).\n");
        Console.Error.WriteLine("Provider not found via DbProviderFactories under invariant name 'DecentDB'.");
        Console.Error.WriteLine("Once the provider exists, register it (or reference a package that registers it) and retry.\n");
        Console.Error.WriteLine($"Details: {ex.GetType().Name}: {ex.Message}");
        return null;
    }
}

sealed record Artist(long Id, string Name);
