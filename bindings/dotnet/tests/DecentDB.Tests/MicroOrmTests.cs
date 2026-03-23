using System;
using System.Collections.Generic;
using System.ComponentModel.DataAnnotations;
using System.IO;
using System.Linq;
using System.Threading.Tasks;
using DecentDB.AdoNet;
using DecentDB.MicroOrm;
using Xunit;

namespace DecentDB.Tests;

public sealed class MicroOrmTests : IDisposable
{
    private readonly string _dbPath;

    public MicroOrmTests()
    {
        _dbPath = Path.Combine(Path.GetTempPath(), $"test_{Guid.NewGuid():N}.ddb");

        using var conn = new DecentDBConnection($"Data Source={_dbPath}");
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

    [Table("custom_people")]
    private sealed class CustomPerson
    {
        [PrimaryKey]
        [Column("person_id")]
        public long Key { get; set; }

        [Column("full_name")]
        [MaxLength(3)]
        public string Name { get; set; } = "";

        [NotNull]
        [Column("not_nullable")]
        public string NotNullable { get; set; } = "";

        [Nullable]
        [Column("optional_note")]
        public string? OptionalNote { get; set; }

        [Ignore]
        public string? Ignored { get; set; }
    }

    [Table("all_types")]
    private sealed class AllTypesEntity
    {
        [PrimaryKey]
        public long Id { get; set; }

        public string TextVal { get; set; } = "";
        public int IntVal { get; set; }
        public double RealVal { get; set; }
        public decimal DecVal { get; set; }
        public bool BoolVal { get; set; }
        public Guid UuidVal { get; set; }
        public byte[]? BlobVal { get; set; }
    }

    [Table("items_view")]
    private sealed class ItemView
    {
        public long Id { get; set; }
        public string Name { get; set; } = "";
        
        [Column("val_doubled")]
        public int Value { get; set; }
    }

    [Fact]
    public async Task InsertQueryUpdateDelete_RoundTrip()
    {
        using var ctx = new DecentDBContext(_dbPath);
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
        using var ctx = new DecentDBContext(_dbPath);
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
        using var ctx = new DecentDBContext(_dbPath);
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

    [Fact]
    public async Task StreamAsync_YieldsRowsInOrder()
    {
        using var ctx = new DecentDBContext(_dbPath);
        var persons = ctx.Set<Person>();

        await persons.InsertManyAsync(new[]
        {
            new Person { Id = 1, Name = "A", Age = 10 },
            new Person { Id = 2, Name = "B", Age = 20 },
            new Person { Id = 3, Name = "C", Age = 30 },
        });

        var ids = new List<long>();
        await foreach (var p in persons.OrderBy(p => p.Id).StreamAsync())
        {
            ids.Add(p.Id);
        }

        Assert.Equal(new long[] { 1, 2, 3 }, ids);
    }

    [Fact]
    public async Task AttributesControlMappingAndNullability()
    {
        using var conn = new DecentDBConnection($"Data Source={_dbPath}");
        conn.Open();

        using var cmd = conn.CreateCommand();
        cmd.CommandText = "CREATE TABLE custom_people (person_id INTEGER PRIMARY KEY, full_name TEXT, not_nullable TEXT NOT NULL, optional_note TEXT)";
        cmd.ExecuteNonQuery();

        using var ctx = new DecentDBContext(_dbPath);
        var set = ctx.Set<CustomPerson>();

        var ok = new CustomPerson
        {
            Key = 1,
            Name = "Ann",
            NotNullable = "ok",
            OptionalNote = null,
            Ignored = "ignored"
        };

        await set.InsertAsync(ok);

        var fetched = await set.GetAsync(1);
        Assert.NotNull(fetched);
        Assert.Equal("Ann", fetched!.Name);
        Assert.Equal("ok", fetched.NotNullable);
        Assert.Null(fetched.OptionalNote);

        var badNull = new CustomPerson
        {
            Key = 2,
            Name = "Bob",
            NotNullable = null!
        };

        await Assert.ThrowsAsync<ArgumentException>(() => set.InsertAsync(badNull));

        var tooLong = new CustomPerson
        {
            Key = 3,
            Name = "🎉",
            NotNullable = "ok"
        };

        await Assert.ThrowsAsync<ArgumentException>(() => set.InsertAsync(tooLong));
    }

    [Fact]
    public async Task AllDataTypes_RoundTrip()
    {
        using var conn = new DecentDBConnection($"Data Source={_dbPath}");
        conn.Open();

        using var cmd = conn.CreateCommand();
        // Create table supporting all types
        // Note: DecentDB currently uses flexible typing, but we declare types for clarity/affinity
        // Use snake_case to match MicroOrm conventions
        cmd.CommandText = @"
            CREATE TABLE all_types (
                id INTEGER PRIMARY KEY,
                text_val TEXT,
                int_val INTEGER,
                real_val REAL,
                dec_val DECIMAL(10, 5),
                bool_val BOOL,
                uuid_val UUID,
                blob_val BLOB
            )";
        cmd.ExecuteNonQuery();

        using var ctx = new DecentDBContext(_dbPath);
        var set = ctx.Set<AllTypesEntity>();

        var id = Guid.NewGuid();
        var blob = new byte[] { 0xDE, 0xAD, 0xBE, 0xEF };

        var entity = new AllTypesEntity
        {
            Id = 100,
            TextVal = "Hello World",
            IntVal = 42,
            RealVal = 3.14159,
            DecVal = 123.45600m,
            BoolVal = true,
            UuidVal = id,
            BlobVal = blob
        };

        await set.InsertAsync(entity);

        var fetched = await set.GetAsync(100);
        Assert.NotNull(fetched);
        Assert.Equal("Hello World", fetched!.TextVal);
        Assert.Equal(42, fetched.IntVal);
        Assert.Equal(3.14159, fetched.RealVal, 5); // Allow some float precision diff
        Assert.Equal(123.45600m, fetched.DecVal);
        Assert.True(fetched.BoolVal);
        Assert.Equal(id, fetched.UuidVal);
        Assert.Equal(blob, fetched.BlobVal);

        // Test updating boolean and decimal
        fetched.BoolVal = false;
        fetched.DecVal = 999.99000m;
        await set.UpdateAsync(fetched);

        var fetched2 = await set.GetAsync(100);
        Assert.False(fetched2!.BoolVal);
        Assert.Equal(999.99000m, fetched2.DecVal);
    }

    [Fact]
    public async Task Views_CanBeMapped()
    {
        using var conn = new DecentDBConnection($"Data Source={_dbPath}");
        conn.Open();

        using var cmd = conn.CreateCommand();
        cmd.CommandText = "CREATE TABLE items (id INTEGER PRIMARY KEY, name TEXT, value INTEGER)";
        cmd.ExecuteNonQuery();

        // Use separate inserts as multi-row insert might not be supported in 0.x
        cmd.CommandText = "INSERT INTO items VALUES (1, 'A', 10)";
        cmd.ExecuteNonQuery();
        cmd.CommandText = "INSERT INTO items VALUES (2, 'B', 20)";
        cmd.ExecuteNonQuery();
        cmd.CommandText = "INSERT INTO items VALUES (3, 'C', 30)";
        cmd.ExecuteNonQuery();

        // Verify data exists in table
        cmd.CommandText = "SELECT COUNT(*) FROM items";
        var count = Convert.ToInt64(cmd.ExecuteScalar());
        Assert.Equal(3, count);

        // Use different column name to ensure mapping is correct
        cmd.CommandText = "CREATE VIEW items_view AS SELECT id, name, value * 2 AS val_doubled FROM items WHERE value > 10";
        cmd.ExecuteNonQuery();

        // Verify view works via ADO.NET first
        cmd.CommandText = "SELECT COUNT(*) FROM items_view";
        var viewCount = Convert.ToInt64(cmd.ExecuteScalar());
        Assert.Equal(2, viewCount);

        using var ctx = new DecentDBContext(_dbPath);
        var viewSet = ctx.Set<ItemView>();

        var items = await viewSet.OrderBy(x => x.Id).ToListAsync();

        Assert.Equal(2, items.Count);
        Assert.Equal(2, items[0].Id);
        Assert.Equal("B", items[0].Name);
        Assert.Equal(40, items[0].Value); // 20 * 2

        Assert.Equal(3, items[1].Id);
        Assert.Equal("C", items[1].Name);
        Assert.Equal(60, items[1].Value); // 30 * 2
    }

    [Fact]
    public async Task CommonTableExpressions_Supported()
    {
        using var conn = new DecentDBConnection($"Data Source={_dbPath}");
        conn.Open();

        using var cmd = conn.CreateCommand();
        
        // Non-Recursive CTE with explicit column list
        var sql = @"
            WITH cte_source(x) AS (
              SELECT 1
              UNION ALL
              SELECT 2
              UNION ALL
              SELECT 3
            )
            SELECT x FROM cte_source WHERE x > 1 ORDER BY x;
        ";

        using var reader = cmd.Connection!.CreateCommand();
        reader.CommandText = sql;
        using var r = reader.ExecuteReader();
        
        var results = new List<long>();
        while(r.Read())
        {
            results.Add(r.GetInt64(0));
        }

        Assert.Equal(new long[] { 2, 3 }, results);
    }
}

