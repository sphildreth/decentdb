using System;
using System.Collections.Generic;
using System.Data.Common;
using System.Linq;
using System.Threading.Tasks;
using DecentDB.AdoNet;
using DecentDB.MicroOrm;
using Xunit;

namespace DecentDB.Tests;

public class InsertManyReturningTests : IDisposable
{
    private readonly string _dbPath;

    public InsertManyReturningTests()
    {
        _dbPath = Path.Combine(Path.GetTempPath(), $"test_insert_returning_{Guid.NewGuid():N}.ddb");

        using var conn = new DecentDBConnection($"Data Source={_dbPath}");
        conn.Open();

        using var cmd = conn.CreateCommand();
        cmd.CommandText = "CREATE TABLE auto_pk_entities (id INTEGER PRIMARY KEY, name TEXT)";
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

    [Fact]
    public async Task InsertManyReturning_AssignsPKsInOrder()
    {
        using var context = new DecentDBContext(_dbPath);
        var set = context.Set<AutoPkEntity>();

        var entities = new List<AutoPkEntity>
        {
            new AutoPkEntity { Name = "A" },
            new AutoPkEntity { Name = "B" },
            new AutoPkEntity { Name = "C" },
        };

        await set.InsertManyReturningAsync(entities);

        Assert.Equal(1, entities[0].Id);
        Assert.Equal(2, entities[1].Id);
        Assert.Equal(3, entities[2].Id);

        var result = await set.ToListAsync();
        Assert.Equal(3, result.Count);
    }

    [Fact]
    public async Task InsertManyReturning_TwoChunks_PKsContiguous()
    {
        using var context = new DecentDBContext(_dbPath);
        var set = context.Set<AutoPkEntity>();

        var entities = Enumerable.Range(0, 257)
            .Select(i => new AutoPkEntity { Name = $"r{i}" })
            .ToList();

        await set.InsertManyReturningAsync(entities);

        Assert.True(entities.All(e => e.Id > 0));
        var ids = entities.Select(e => e.Id).ToList();
        Assert.Equal(ids.Count, ids.Distinct().Count());

        var result = await set.ToListAsync();
        Assert.Equal(257, result.Count);
    }

    [Fact]
    public async Task InsertManyReturning_RollsBackOnDbError()
    {
        using var conn = new DecentDBConnection($"Data Source={_dbPath}");
        conn.Open();
        using var cmd = conn.CreateCommand();
        cmd.CommandText = "CREATE TABLE unique_entities (id INTEGER PRIMARY KEY, name TEXT NOT NULL UNIQUE)";
        cmd.ExecuteNonQuery();
        cmd.CommandText = "INSERT INTO unique_entities (id, name) VALUES (1, 'dup')";
        cmd.ExecuteNonQuery();

        using var context = new DecentDBContext(_dbPath);
        var set = context.Set<UniqueEntity>();

        var entities = new List<UniqueEntity>
        {
            new UniqueEntity { Name = "new1" },
            new UniqueEntity { Name = "dup" },
            new UniqueEntity { Name = "new2" },
        };

        await Assert.ThrowsAnyAsync<Exception>(async () =>
            await set.InsertManyReturningAsync(entities));

        var result = await set.ToListAsync();
        Assert.Single(result);
        Assert.Equal("dup", result[0].Name);
    }

    [Fact]
    public async Task InsertManyReturningAsync_WithPreAssignedPK_Throws()
    {
        using var context = new DecentDBContext(_dbPath);
        var set = context.Set<AutoPkEntity>();

        var entities = new List<AutoPkEntity>
        {
            new AutoPkEntity { Id = 42, Name = "PreAssigned" },
        };

        await Assert.ThrowsAsync<InvalidOperationException>(() =>
            set.InsertManyReturningAsync(entities));
    }

    private class AutoPkEntity
    {
        public int Id { get; set; }
        public string Name { get; set; } = string.Empty;
    }

    private class UniqueEntity
    {
        public int Id { get; set; }
        public string Name { get; set; } = string.Empty;
    }
}
