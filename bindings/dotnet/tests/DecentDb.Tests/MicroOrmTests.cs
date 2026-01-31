using System;
using System.IO;
using System.Linq;
using System.Threading.Tasks;
using DecentDb.AdoNet;
using DecentDb.MicroOrm;
using Xunit;

namespace DecentDb.Tests;

public sealed class MicroOrmTests : IDisposable
{
    private readonly string _dbPath;

    public MicroOrmTests()
    {
        _dbPath = Path.Combine(Path.GetTempPath(), $"test_{Guid.NewGuid():N}.db");

        using var conn = new DecentDbConnection($"Data Source={_dbPath}");
        conn.Open();

        using var cmd = conn.CreateCommand();
        cmd.CommandText = "CREATE TABLE persons (id INTEGER PRIMARY KEY, name TEXT, age INTEGER)";
        cmd.ExecuteNonQuery();
    }

    public void Dispose()
    {
        if (File.Exists(_dbPath))
            File.Delete(_dbPath);

        var walPath = _dbPath + "-wal";
        if (File.Exists(walPath))
            File.Delete(walPath);
    }

    private sealed class Person
    {
        public long Id { get; set; }
        public string Name { get; set; } = "";
        public int Age { get; set; }
    }

    [Fact]
    public async Task InsertQueryUpdateDelete_RoundTrip()
    {
        using var ctx = new DecentDbContext(_dbPath);
        var persons = ctx.Set<Person>();

        await persons.InsertAsync(new Person { Id = 1, Name = "Alice", Age = 30 });
        await persons.InsertAsync(new Person { Id = 2, Name = "Bob", Age = 17 });
        await persons.InsertAsync(new Person { Id = 3, Name = "Ann", Age = 22 });

        var adultsStartingWithA = await persons
            .Where(p => p.Age >= 18 && p.Name.StartsWith("A"))
            .OrderBy(p => p.Id)
            .ToListAsync();

        Assert.Equal(2, adultsStartingWithA.Count);
        Assert.Equal(1L, adultsStartingWithA[0].Id);
        Assert.Equal(3L, adultsStartingWithA[1].Id);

        var countAdults = await persons.Where(p => p.Age >= 18).CountAsync();
        Assert.Equal(2, countAdults);

        var secondById = await persons.OrderBy(p => p.Id).Skip(1).Take(1).FirstAsync();
        Assert.Equal(2L, secondById.Id);

        var alice = await persons.GetAsync(1);
        Assert.NotNull(alice);
        Assert.Equal("Alice", alice!.Name);

        alice.Age = 31;
        await persons.UpdateAsync(alice);

        var alice2 = await persons.GetAsync(1);
        Assert.NotNull(alice2);
        Assert.Equal(31, alice2!.Age);

        await persons.DeleteByIdAsync(2);
        var bob = await persons.GetAsync(2);
        Assert.Null(bob);
    }

    [Fact]
    public async Task AnySingleAndBulkOperations()
    {
        using var ctx = new DecentDbContext(_dbPath);
        var persons = ctx.Set<Person>();

        Assert.False(await persons.AnyAsync());

        await persons.InsertManyAsync(new[]
        {
            new Person { Id = 10, Name = "A", Age = 1 },
            new Person { Id = 11, Name = "B", Age = 2 },
            new Person { Id = 12, Name = "C", Age = 3 },
        });

        Assert.True(await persons.AnyAsync());

        var b = await persons.Where(p => p.Name == "B").SingleAsync();
        Assert.Equal(11, b.Id);

        var deleted = await persons.DeleteManyAsync(p => p.Age >= 2);
        Assert.Equal(2, deleted);

        var remaining = await persons.OrderBy(p => p.Id).ToListAsync();
        Assert.Single(remaining);
        Assert.Equal(10, remaining[0].Id);
    }

    [Fact]
    public async Task SupportsQueryableLinqSyntax()
    {
        using var ctx = new DecentDbContext(_dbPath);
        var persons = ctx.Set<Person>();

        await persons.InsertManyAsync(new[]
        {
            new Person { Id = 1, Name = "A", Age = 10 },
            new Person { Id = 2, Name = "B", Age = 20 },
            new Person { Id = 3, Name = "C", Age = 30 },
        });

        IQueryable<Person> q = persons;

        var list = q
            .Where(p => p.Age >= 20)
            .OrderBy(p => p.Id)
            .Skip(0)
            .Take(10)
            .ToList();

        Assert.Equal(2, list.Count);
        Assert.Equal(2, list[0].Id);
        Assert.Equal(3, list[1].Id);

        var count = q.Count(p => p.Age >= 20);
        Assert.Equal(2, count);

        var any = q.Any(p => p.Name == "C");
        Assert.True(any);
    }
}
