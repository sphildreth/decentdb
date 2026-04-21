using DecentDB.MicroOrm;
using DecentDB.AdoNet;
using Xunit;

namespace DecentDB.Tests;

public sealed class MicroOrmLinqProviderCoverageTests : IDisposable
{
    private readonly string _dbPath = Path.Combine(Path.GetTempPath(), $"test_microorm_linq_{Guid.NewGuid():N}.ddb");

    [Fact]
    public async Task SynchronousQueryableOperations_ExerciseLinqProviderBranches()
    {
        EnsureSchema();

        using var context = new DecentDBContext(_dbPath);
        var set = context.Set<SyncEntity>();

        await set.InsertManyAsync(
        [
            new SyncEntity { Id = 1, Name = "a", GroupId = 1 },
            new SyncEntity { Id = 2, Name = "b", GroupId = 1 },
            new SyncEntity { Id = 3, Name = "c", GroupId = 2 },
            new SyncEntity { Id = 4, Name = "d", GroupId = 2 }
        ]);

        Assert.Equal(4, set.Count());
        Assert.Equal(2L, set.LongCount(e => e.GroupId == 1));
        Assert.True(set.Any());
        Assert.True(set.Any(e => e.Name == "c"));
        Assert.False(set.Any(e => e.Name == "z"));

        Assert.Equal(1, set.First().Id);
        Assert.Null(set.FirstOrDefault(e => e.Name == "missing"));
        Assert.Equal(4, set.Single(e => e.Id == 4).Id);
        Assert.Null(set.SingleOrDefault(e => e.Id == 999));

        var one = 1;
        var ordered = set
            .OrderBy(e => e.GroupId)
            .ThenByDescending(e => e.Name)
            .Skip(one + 0)
            .Take(one + 1)
            .ToList();

        Assert.InRange(ordered.Count, 1, 2);
        Assert.All(ordered, row => Assert.InRange(row.Id, 1, 4));

        var asEnumerable = ((IQueryable<SyncEntity>)set).ToArray();
        Assert.Equal(4, asEnumerable.Length);
    }

    private void EnsureSchema()
    {
        using var connection = new DecentDBConnection($"Data Source={_dbPath}");
        connection.Open();

        using var command = connection.CreateCommand();
        command.CommandText = """
            DROP TABLE IF EXISTS sync_entities;
            CREATE TABLE sync_entities (
                id INTEGER PRIMARY KEY,
                name TEXT NOT NULL,
                group_id INTEGER NOT NULL
            );
            """;
        command.ExecuteNonQuery();
    }

    [Fact]
    public void QueryProvider_ThrowsForCrossTypeAndUnsupportedExecutionShapes()
    {
        using var context = new DecentDBContext(_dbPath);
        var set = context.Set<SyncEntity>();
        IQueryable<SyncEntity> query = set.Where(e => e.Id > 0);

        Assert.Throws<NotSupportedException>(() => query.Provider.CreateQuery<int>(query.Expression));
        Assert.Throws<NotSupportedException>(() => query.Provider.Execute<DateTime>(query.Expression));
    }

    public void Dispose()
    {
        TryDelete(_dbPath);
        TryDelete(_dbPath + "-wal");
    }

    private static void TryDelete(string path)
    {
        if (File.Exists(path))
        {
            File.Delete(path);
        }
    }

    private sealed class SyncEntity
    {
        public int Id { get; set; }
        public string Name { get; set; } = string.Empty;
        public int GroupId { get; set; }
    }
}
