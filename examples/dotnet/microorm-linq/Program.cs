using DecentDB.AdoNet;
using DecentDB.MicroOrm;

var dbPath = Path.GetFullPath(Path.Combine(AppContext.BaseDirectory, "..", "..", "..", "sample.db"));

// Schema setup (Micro-ORM is convention-based; it doesn't create tables for you).
using (var conn = new DecentDBConnection($"Data Source={dbPath}"))
{
    conn.Open();
    using var cmd = conn.CreateCommand();
    cmd.CommandText = "CREATE TABLE IF NOT EXISTS persons (id INTEGER PRIMARY KEY, name TEXT, age INTEGER)";
    cmd.ExecuteNonQuery();
    cmd.CommandText = "DELETE FROM persons";
    cmd.ExecuteNonQuery();
}

using var ctx = new DecentDBContext(dbPath);
var persons = ctx.Set<Person>();

await persons.InsertManyAsync(new[]
{
    new Person { Id = 1, Name = "Alice", Age = 30 },
    new Person { Id = 2, Name = "Bob", Age = 17 },
    new Person { Id = 3, Name = "Ann", Age = 22 },
    new Person { Id = 4, Name = "Charlie", Age = 44 },
});

var page = await persons
    .Where(p => p.Age >= 18 && p.Name.StartsWith("A"))
    .OrderBy(p => p.Id)
    .Skip(0)
    .Take(10)
    .ToListAsync();

Console.WriteLine("Adults starting with 'A':");
foreach (var p in page)
{
    Console.WriteLine($"{p.Id}: {p.Name} ({p.Age})");
}

Console.WriteLine();
Console.WriteLine("Streaming all persons by id:");
await foreach (var p in persons.OrderBy(p => p.Id).StreamAsync())
{
    Console.WriteLine($"{p.Id}: {p.Name} ({p.Age})");
}

public sealed class Person
{
    public long Id { get; set; }
    public string Name { get; set; } = "";
    public int Age { get; set; }
}
