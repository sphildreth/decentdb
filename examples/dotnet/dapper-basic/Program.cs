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

// ── Window Functions ──
conn.Execute("DROP TABLE IF EXISTS scores");
conn.Execute(@"CREATE TABLE scores (
    id    INTEGER PRIMARY KEY,
    name  TEXT NOT NULL,
    dept  TEXT NOT NULL,
    score INTEGER NOT NULL
)");

conn.Execute(
    "INSERT INTO scores (name, dept, score) VALUES (@name, @dept, @score)",
    new[]
    {
        new { name = "Alice", dept = "eng", score = 95 },
        new { name = "Bob", dept = "eng", score = 95 },
        new { name = "Carol", dept = "eng", score = 80 },
        new { name = "Dave", dept = "sales", score = 90 },
        new { name = "Eve", dept = "sales", score = 85 },
    });

Console.WriteLine("\n── Window Functions ──");

// ROW_NUMBER
var rnRows = conn.Query<WindowResult>(@"
    SELECT name AS Name, dept AS Dept, score AS Score,
           ROW_NUMBER() OVER (PARTITION BY dept ORDER BY score DESC) AS Val
    FROM scores ORDER BY dept, score DESC");
Console.WriteLine("\nROW_NUMBER (ranking within department):");
foreach (var r in rnRows)
    Console.WriteLine($"  {r.Name,-6}  dept={r.Dept,-5}  score={r.Score}  rn={r.Val}");

// RANK
var rankRows = conn.Query<WindowResult>(@"
    SELECT name AS Name, '' AS Dept, score AS Score,
           RANK() OVER (ORDER BY score DESC) AS Val
    FROM scores ORDER BY score DESC, name");
Console.WriteLine("\nRANK (with gaps for ties):");
foreach (var r in rankRows)
    Console.WriteLine($"  {r.Name,-6}  score={r.Score}  rank={r.Val}");

// DENSE_RANK
var drRows = conn.Query<WindowResult>(@"
    SELECT name AS Name, '' AS Dept, score AS Score,
           DENSE_RANK() OVER (ORDER BY score DESC) AS Val
    FROM scores ORDER BY score DESC, name");
Console.WriteLine("\nDENSE_RANK (no gaps):");
foreach (var r in drRows)
    Console.WriteLine($"  {r.Name,-6}  score={r.Score}  dense_rank={r.Val}");

// LAG
var lagRows = conn.Query<WindowResult>(@"
    SELECT name AS Name, '' AS Dept, score AS Score,
           LAG(score, 1, 0) OVER (ORDER BY score DESC) AS Val
    FROM scores ORDER BY Score DESC");
Console.WriteLine("\nLAG (previous score):");
foreach (var r in lagRows)
    Console.WriteLine($"  {r.Name,-6}  score={r.Score}  prev_score={r.Val}");

// LEAD
var leadRows = conn.Query<NullableWindowResult>(@"
    SELECT name AS Name, score AS Score,
           LEAD(score) OVER (PARTITION BY dept ORDER BY score DESC) AS Val
    FROM scores ORDER BY dept, Score DESC");
Console.WriteLine("\nLEAD (next score in dept):");
foreach (var r in leadRows)
    Console.WriteLine($"  {r.Name,-6}  score={r.Score}  next_score={r.Val?.ToString() ?? "NULL"}");

sealed record Artist(long Id, string Name);
sealed record WindowResult(string Name, string Dept, long Score, long Val);
sealed record NullableWindowResult(string Name, long Score, long? Val);
