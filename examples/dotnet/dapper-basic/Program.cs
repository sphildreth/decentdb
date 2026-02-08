using DecentDB.AdoNet;
using Dapper;

var dbPath = Path.GetFullPath(Path.Combine(AppContext.BaseDirectory, "..", "..", "..", "sample.ddb"));

using var conn = new DecentDBConnection($"Data Source={dbPath}");
conn.Open();

conn.Execute("DROP TABLE IF EXISTS artists");
conn.Execute("CREATE TABLE artists (id INTEGER PRIMARY KEY, name TEXT NOT NULL)");

// Auto-increment: omit id and let DecentDB assign sequential IDs
conn.Execute(
    "INSERT INTO artists (name) VALUES (@name)",
    new[]
    {
        new { name = "Alice" },
        new { name = "Bob" },
        new { name = "Charlie" },
    });

// Query with named parameters (rewritten to $1, $2 automatically)
var rows = conn.Query<Artist>(
    "SELECT id, name FROM artists WHERE id >= @minId ORDER BY id LIMIT @limit",
    new { minId = 2, limit = 10 });

foreach (var row in rows)
{
    Console.WriteLine($"{row.Id}: {row.Name}");
}

sealed record Artist(long Id, string Name);
