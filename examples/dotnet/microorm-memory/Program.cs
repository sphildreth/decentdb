// Example: DecentDB in-memory database with MicroOrm.
// Demonstrates using :memory: for ephemeral databases — no files created on disk.

using DecentDB.AdoNet;
using DecentDB.MicroOrm;

using var ctx = new DecentDBContext("Data Source=:memory:");
var persons = ctx.Set<Person>();

// Create table via raw SQL (MicroOrm is convention-based; tables are created with DDL)
await ctx.ExecuteNonQueryAsync("CREATE TABLE persons (id INTEGER PRIMARY KEY, name TEXT NOT NULL, age INT64 NOT NULL)");

// Auto-increment: omit Id and let DecentDB assign it via RETURNING
var people = new[]
{
    new Person { Name = "Alice", Age = 30 },
    new Person { Name = "Bob", Age = 17 },
    new Person { Name = "Ann", Age = 22 },
    new Person { Name = "Charlie", Age = 44 },
};
foreach (var p in people)
{
    await persons.InsertAsync(p);
}

// Fluent LINQ-style query: Where + OrderBy + Skip + Take
var page = await persons
    .Where(p => p.Age >= 18 && p.Name.StartsWith("A"))
    .OrderBy(p => p.Id)
    .Skip(0)
    .Take(10)
    .ToListAsync();

Console.WriteLine("Adults starting with 'A':");
foreach (var p in page)
{
    Console.WriteLine($"  {p.Id}: {p.Name} ({p.Age})");
}

Console.WriteLine();
Console.WriteLine("Streaming all persons by id:");
await foreach (var p in persons.OrderBy(p => p.Id).StreamAsync())
{
    Console.WriteLine($"  {p.Id}: {p.Name} ({p.Age})");
}

// Count and Any
var count = await persons.CountAsync();
var hasAdults = await persons.AnyAsync(p => p.Age >= 18);
Console.WriteLine($"\nTotal: {count}, Has adults: {hasAdults}");

// ── Window Functions (via raw SQL on the same in-memory connection) ──
Console.WriteLine("\n── Window Functions ──");

await ctx.ExecuteNonQueryAsync(@"CREATE TABLE scores (
    id    INTEGER PRIMARY KEY,
    name  TEXT NOT NULL,
    dept  TEXT NOT NULL,
    score INTEGER NOT NULL
)");
foreach (var (name, dept, score) in new[] {
    ("Alice", "eng", 95), ("Bob", "eng", 95),
    ("Carol", "eng", 80), ("Dave", "sales", 90),
    ("Eve", "sales", 85) })
{
    await ctx.ExecuteNonQueryAsync(
        $"INSERT INTO scores (name, dept, score) VALUES ('{name}', '{dept}', {score})");
}

// Use the context's internal connection for raw SQL reads
using var rawConn = new DecentDBConnection("Data Source=:memory:");
rawConn.Open();

// Re-create tables in this second in-memory database to demonstrate raw SQL
rawConn.Execute("CREATE TABLE scores (id INTEGER PRIMARY KEY, name TEXT NOT NULL, dept TEXT NOT NULL, score INTEGER NOT NULL)");
foreach (var (name, dept, score) in new[] {
    ("Alice", "eng", 95), ("Bob", "eng", 95),
    ("Carol", "eng", 80), ("Dave", "sales", 90),
    ("Eve", "sales", 85) })
{
    rawConn.Execute($"INSERT INTO scores (name, dept, score) VALUES ('{name}', '{dept}', {score})");
}

using var cmd = rawConn.CreateCommand();
cmd.CommandText = @"
    SELECT name, score,
           ROW_NUMBER() OVER (ORDER BY score DESC) AS rn,
           RANK()       OVER (ORDER BY score DESC) AS rank,
           DENSE_RANK() OVER (ORDER BY score DESC) AS dense_rank
    FROM scores ORDER BY score DESC, name";
using var reader = cmd.ExecuteReader();
Console.WriteLine("\nComparison: ROW_NUMBER vs RANK vs DENSE_RANK");
Console.WriteLine($"  {"Name",-8} {"Score",5}  {"RN",3}  {"Rank",4}  {"DRank",5}");
while (reader.Read())
{
    Console.WriteLine($"  {reader.GetString(0),-8} {reader.GetInt64(1),5}  {reader.GetInt64(2),3}  {reader.GetInt64(3),4}  {reader.GetInt64(4),5}");
}

// LAG / LEAD
using var cmd2 = rawConn.CreateCommand();
cmd2.CommandText = @"
    SELECT name, score,
           LAG(score, 1, 0) OVER (ORDER BY score DESC) AS prev,
           LEAD(score)      OVER (ORDER BY score DESC) AS next
    FROM scores ORDER BY score DESC";
using var reader2 = cmd2.ExecuteReader();
Console.WriteLine("\nLAG / LEAD:");
while (reader2.Read())
{
    var next = reader2.IsDBNull(3) ? "NULL" : reader2.GetInt64(3).ToString();
    Console.WriteLine($"  {reader2.GetString(0),-8} score={reader2.GetInt64(1)}  prev={reader2.GetInt64(2)}  next={next}");
}

// No cleanup needed — in-memory database is automatically discarded.
Console.WriteLine("\nDone.");

public sealed class Person
{
    public long Id { get; set; }
    public string Name { get; set; } = "";
    public int Age { get; set; }
}

static class DbConnectionExtensions
{
    public static void Execute(this System.Data.IDbConnection conn, string sql)
    {
        using var cmd = conn.CreateCommand();
        cmd.CommandText = sql;
        cmd.ExecuteNonQuery();
    }
}
