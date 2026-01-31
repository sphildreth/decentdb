using DecentDb.AdoNet;
using Dapper;

var dbPath = Path.GetFullPath(Path.Combine(AppContext.BaseDirectory, "..", "..", "..", "sample.db"));

using var conn = new DecentDbConnection($"Data Source={dbPath}");
conn.Open();

conn.Execute("DROP TABLE artists");
conn.Execute("CREATE TABLE artists (id INT PRIMARY KEY, name TEXT)");

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

sealed record Artist(long Id, string Name);
