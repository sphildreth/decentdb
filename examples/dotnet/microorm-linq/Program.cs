using DecentDB.AdoNet;
using DecentDB.MicroOrm;

var dbPath = Path.GetFullPath(Path.Combine(AppContext.BaseDirectory, "..", "..", "..", "sample.ddb"));

// Clean start
using (var conn = new DecentDBConnection($"Data Source={dbPath}"))
{
    conn.Open();
    using var cmd = conn.CreateCommand();
    cmd.CommandText = "DROP TABLE IF EXISTS persons";
    cmd.ExecuteNonQuery();
}

using var ctx = new DecentDBContext(dbPath);
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

public sealed class Person
{
    public long Id { get; set; }
    public string Name { get; set; } = "";
    public int Age { get; set; }
}
