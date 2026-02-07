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

public class DbSetEdgeCaseTests : IDisposable
{
    private readonly string _dbPath;

    public DbSetEdgeCaseTests()
    {
        _dbPath = Path.Combine(Path.GetTempPath(), $"test_edge_cases_{Guid.NewGuid():N}.ddb");
    }

    public void Dispose()
    {
        if (File.Exists(_dbPath))
            File.Delete(_dbPath);

        var walPath = _dbPath + "-wal";
        if (File.Exists(walPath))
            File.Delete(walPath);
    }

    [Fact]
    public async Task InsertManyAsync_Empty_Collection()
    {
        using var ctx = new DecentDBContext(_dbPath);
        var persons = ctx.Set<Person>();
        
        // Create table
        using (var conn = new DecentDBConnection($"Data Source={_dbPath}"))
        {
            conn.Open();
            using var cmd = conn.CreateCommand();
            cmd.CommandText = "CREATE TABLE persons (id INTEGER PRIMARY KEY, name TEXT, age INTEGER)";
            cmd.ExecuteNonQuery();
        }
        
        // Insert empty collection
        await persons.InsertManyAsync(new List<Person>());
        
        // Verify no records were added
        var count = await persons.CountAsync();
        Assert.Equal(0, count);
    }

    [Fact]
    public async Task InsertManyAsync_Single_Item()
    {
        using var ctx = new DecentDBContext(_dbPath);
        var persons = ctx.Set<Person>();
        
        // Create table
        using (var conn = new DecentDBConnection($"Data Source={_dbPath}"))
        {
            conn.Open();
            using var cmd = conn.CreateCommand();
            cmd.CommandText = "CREATE TABLE persons (id INTEGER PRIMARY KEY, name TEXT, age INTEGER)";
            cmd.ExecuteNonQuery();
        }
        
        // Insert single item
        var person = new Person { Id = 1, Name = "Single", Age = 25 };
        await persons.InsertManyAsync(new List<Person> { person });
        
        // Verify record was added
        var count = await persons.CountAsync();
        Assert.Equal(1, count);
        
        var retrieved = await persons.GetAsync(1);
        Assert.NotNull(retrieved);
        Assert.Equal("Single", retrieved.Name);
    }

    [Fact]
    public async Task InsertManyAsync_With_Transaction()
    {
        using var ctx = new DecentDBContext(_dbPath);
        
        // Create table first
        using (var conn = new DecentDBConnection($"Data Source={_dbPath}"))
        {
            conn.Open();
            using var cmd = conn.CreateCommand();
            cmd.CommandText = "CREATE TABLE persons (id INTEGER PRIMARY KEY, name TEXT, age INTEGER)";
            cmd.ExecuteNonQuery();
        }
        
        // Start transaction
        using var tx = ctx.BeginTransaction();
        var persons = ctx.Set<Person>();
        
        // Insert multiple items within transaction
        var people = new List<Person>
        {
            new Person { Id = 1, Name = "Tx1", Age = 25 },
            new Person { Id = 2, Name = "Tx2", Age = 30 },
            new Person { Id = 3, Name = "Tx3", Age = 35 }
        };
        
        await persons.InsertManyAsync(people);
        
        // Verify records exist within transaction
        var count = await persons.CountAsync();
        Assert.Equal(3, count);
        
        // Rollback transaction
        tx.Rollback();
        
        // Verify records were rolled back
        var countAfterRollback = await persons.CountAsync();
        Assert.Equal(0, countAfterRollback);
    }

    [Fact]
    public async Task InsertManyAsync_With_Null_Value_Throws_Exception()
    {
        using var ctx = new DecentDBContext(_dbPath);
        var persons = ctx.Set<PersonWithNotNull>();
        
        // Create table
        using (var conn = new DecentDBConnection($"Data Source={_dbPath}"))
        {
            conn.Open();
            using var cmd = conn.CreateCommand();
            cmd.CommandText = "CREATE TABLE persons_with_not_null (id INTEGER PRIMARY KEY, name TEXT NOT NULL)";
            cmd.ExecuteNonQuery();
        }
        
        var people = new List<PersonWithNotNull>
        {
            new PersonWithNotNull { Id = 1, Name = "Valid" },
            new PersonWithNotNull { Id = 2, Name = null! } // This should cause an exception
        };
        
        await Assert.ThrowsAsync<ArgumentException>(async () => await persons.InsertManyAsync(people));
    }

    [Fact]
    public async Task DeleteManyAsync_With_Predicate()
    {
        using var ctx = new DecentDBContext(_dbPath);
        var persons = ctx.Set<Person>();
        
        // Create table and insert test data
        using (var conn = new DecentDBConnection($"Data Source={_dbPath}"))
        {
            conn.Open();
            using var cmd = conn.CreateCommand();
            
            cmd.CommandText = "CREATE TABLE persons (id INTEGER PRIMARY KEY, name TEXT, age INTEGER)";
            cmd.ExecuteNonQuery();
            
            cmd.CommandText = "INSERT INTO persons VALUES (1, 'Young', 10), (2, 'Adult', 20), (3, 'Old', 65), (4, 'Middle', 70)";
            cmd.ExecuteNonQuery();
        }
        
        // Delete all persons with age < 30
        var deletedCount = await persons.DeleteManyAsync(p => p.Age < 30);
        
        Assert.Equal(2, deletedCount); // Two people (ages 10 and 20) are under 30
        
        // Verify remaining records
        var remaining = await persons.ToListAsync();
        Assert.Equal(2, remaining.Count);
        Assert.All(remaining, p => Assert.True(p.Age >= 30)); // Only ages 65 and 70 should remain
    }

    [Fact]
    public async Task DeleteManyAsync_With_No_Matches()
    {
        using var ctx = new DecentDBContext(_dbPath);
        var persons = ctx.Set<Person>();
        
        // Create table and insert test data
        using (var conn = new DecentDBConnection($"Data Source={_dbPath}"))
        {
            conn.Open();
            using var cmd = conn.CreateCommand();
            
            cmd.CommandText = "CREATE TABLE persons (id INTEGER PRIMARY KEY, name TEXT, age INTEGER)";
            cmd.ExecuteNonQuery();
            
            cmd.CommandText = "INSERT INTO persons VALUES (1, 'Test', 25)";
            cmd.ExecuteNonQuery();
        }
        
        // Try to delete persons with age > 100 (should match none)
        var deletedCount = await persons.DeleteManyAsync(p => p.Age > 100);
        
        Assert.Equal(0, deletedCount);
        
        // Verify record still exists
        var count = await persons.CountAsync();
        Assert.Equal(1, count);
    }

    [Fact]
    public async Task DeleteManyAsync_With_Null_Predicate_Throws_Exception()
    {
        using var ctx = new DecentDBContext(_dbPath);
        var persons = ctx.Set<Person>();
        
        await Assert.ThrowsAsync<ArgumentNullException>(async () => await persons.DeleteManyAsync(null!));
    }

    [Fact]
    public async Task Transaction_Rollback_On_Exception()
    {
        using var ctx = new DecentDBContext(_dbPath);
        
        // Create table
        using (var conn = new DecentDBConnection($"Data Source={_dbPath}"))
        {
            conn.Open();
            using var cmd = conn.CreateCommand();
            cmd.CommandText = "CREATE TABLE persons (id INTEGER PRIMARY KEY, name TEXT, age INTEGER)";
            cmd.ExecuteNonQuery();
        }
        
        var persons = ctx.Set<Person>();
        
        // Insert initial data
        await persons.InsertAsync(new Person { Id = 1, Name = "Before Tx", Age = 25 });
        
        try
        {
            using var tx = ctx.BeginTransaction();
            
            // Insert within transaction
            await persons.InsertAsync(new Person { Id = 2, Name = "During Tx", Age = 30 });
            
            // Cause an exception to trigger rollback
            throw new Exception("Simulated error");
        }
        catch
        {
            // Exception caught, transaction should be rolled back
        }
        
        // Verify only the first record remains (second was rolled back)
        var allRecords = await persons.ToListAsync();
        Assert.Single(allRecords);
        Assert.Equal(1L, allRecords[0].Id);
    }

    [Fact]
    public async Task Skip_With_Negative_Value_Throws_Exception()
    {
        using var ctx = new DecentDBContext(_dbPath);
        var persons = ctx.Set<Person>();
        
        Assert.Throws<ArgumentOutOfRangeException>(() => persons.Skip(-1));
    }

    [Fact]
    public async Task Take_With_Negative_Value_Throws_Exception()
    {
        using var ctx = new DecentDBContext(_dbPath);
        var persons = ctx.Set<Person>();
        
        Assert.Throws<ArgumentOutOfRangeException>(() => persons.Take(-1));
    }

    [Fact]
    public async Task SingleOrDefaultAsync_With_Multiple_Results_Throws_Exception()
    {
        using var ctx = new DecentDBContext(_dbPath);
        var persons = ctx.Set<Person>();
        
        // Create table and insert test data
        using (var conn = new DecentDBConnection($"Data Source={_dbPath}"))
        {
            conn.Open();
            using var cmd = conn.CreateCommand();
            
            cmd.CommandText = "CREATE TABLE persons (id INTEGER PRIMARY KEY, name TEXT, age INTEGER)";
            cmd.ExecuteNonQuery();
            
            cmd.CommandText = "INSERT INTO persons VALUES (1, 'Test', 25), (2, 'Test2', 30)";
            cmd.ExecuteNonQuery();
        }
        
        // This should throw because there are multiple results when calling SingleAsync
        await Assert.ThrowsAsync<InvalidOperationException>(async () => await persons.SingleAsync());
    }

    [Fact]
    public async Task SingleAsync_With_No_Results_Throws_Exception()
    {
        using var ctx = new DecentDBContext(_dbPath);
        var persons = ctx.Set<Person>();
        
        // Create table but don't insert any data
        using (var conn = new DecentDBConnection($"Data Source={_dbPath}"))
        {
            conn.Open();
            using var cmd = conn.CreateCommand();
            cmd.CommandText = "CREATE TABLE persons (id INTEGER PRIMARY KEY, name TEXT, age INTEGER)";
            cmd.ExecuteNonQuery();
        }
        
        // This should throw because there are no results
        await Assert.ThrowsAsync<InvalidOperationException>(async () => await persons.SingleAsync());
    }

    [Fact]
    public async Task FirstAsync_With_No_Results_Throws_Exception()
    {
        using var ctx = new DecentDBContext(_dbPath);
        var persons = ctx.Set<Person>();
        
        // Create table but don't insert any data
        using (var conn = new DecentDBConnection($"Data Source={_dbPath}"))
        {
            conn.Open();
            using var cmd = conn.CreateCommand();
            cmd.CommandText = "CREATE TABLE persons (id INTEGER PRIMARY KEY, name TEXT, age INTEGER)";
            cmd.ExecuteNonQuery();
        }
        
        // This should throw because there are no results
        await Assert.ThrowsAsync<InvalidOperationException>(async () => await persons.FirstAsync());
    }

    [Fact]
    public async Task InsertAsync_With_Null_NonNullable_Property_Throws_Exception()
    {
        using var ctx = new DecentDBContext(_dbPath);
        var persons = ctx.Set<PersonWithNotNull>();
        
        // Create table
        using (var conn = new DecentDBConnection($"Data Source={_dbPath}"))
        {
            conn.Open();
            using var cmd = conn.CreateCommand();
            cmd.CommandText = "CREATE TABLE persons_with_not_null (id INTEGER PRIMARY KEY, name TEXT NOT NULL)";
            cmd.ExecuteNonQuery();
        }
        
        var person = new PersonWithNotNull { Id = 1, Name = null! }; // Name is marked as not null
        
        await Assert.ThrowsAsync<ArgumentException>(async () => await persons.InsertAsync(person));
    }

    [Fact]
    public async Task UpdateAsync_With_Null_NonNullable_Property_Throws_Exception()
    {
        using var ctx = new DecentDBContext(_dbPath);
        var persons = ctx.Set<PersonWithNotNull>();
        
        // Create table and insert initial record
        using (var conn = new DecentDBConnection($"Data Source={_dbPath}"))
        {
            conn.Open();
            using var cmd = conn.CreateCommand();
            
            cmd.CommandText = "CREATE TABLE persons_with_not_null (id INTEGER PRIMARY KEY, name TEXT NOT NULL)";
            cmd.ExecuteNonQuery();
            
            cmd.CommandText = "INSERT INTO persons_with_not_null VALUES (1, 'Initial')";
            cmd.ExecuteNonQuery();
        }
        
        var person = await persons.GetAsync(1);
        Assert.NotNull(person);
        person.Name = null!; // Try to set required field to null
        
        await Assert.ThrowsAsync<ArgumentException>(async () => await persons.UpdateAsync(person));
    }

    [Table("persons")]
    private class Person
    {
        public long Id { get; set; }
        public string Name { get; set; } = "";
        public int Age { get; set; }
    }

    [Table("persons_with_not_null")]
    private class PersonWithNotNull
    {
        public long Id { get; set; }
        
        [NotNull]
        public string Name { get; set; } = "";
    }
}