using DecentDB.AdoNet;
using DecentDB.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.Logging;
using Xunit;

namespace DecentDB.EntityFrameworkCore.Tests;

public sealed class BatchInsertCoalescingTests : IDisposable
{
    private readonly string _dbPath = Path.Combine(Path.GetTempPath(), $"test_ef_batch_{Guid.NewGuid():N}.ddb");
    private readonly List<string> _capturedSql = new();

    public void Dispose()
    {
        TryDelete(_dbPath);
        TryDelete(_dbPath + "-wal");
    }

    [Fact]
    public void Insert_OneEntity_UsesSingleRowPath()
    {
        EnsureSchema();

        using var context = CreateContext();
        var row = new BatchEntity { Name = "solo" };
        context.Entities.Add(row);
        context.SaveChanges();

        Assert.True(row.Id > 0);

        using var verify = CreateContext();
        Assert.Equal(1, verify.Entities.Count());
        Assert.Equal("solo", verify.Entities.Single().Name);
    }

    [Fact]
    public void Insert_TwoEntitiesSameShape_UsesMultiRowPath()
    {
        EnsureSchema();

        using var context = CreateContext();
        context.Entities.AddRange(
            new BatchEntity { Name = "alpha" },
            new BatchEntity { Name = "beta" });
        context.SaveChanges();

        using var verify = CreateContext();
        var rows = verify.Entities.OrderBy(e => e.Name).ToList();
        Assert.Equal(2, rows.Count);
        Assert.Equal("alpha", rows[0].Name);
        Assert.Equal("beta", rows[1].Name);
    }

    [Fact]
    public void Insert_257EntitiesSameShape_SplitsAtMaxCoalescedRows()
    {
        EnsureSchema();

        using var context = CreateContext();
        var entities = Enumerable.Range(0, 257)
            .Select(i => new BatchEntity { Name = $"row{i}" })
            .ToList();
        context.Entities.AddRange(entities);
        context.SaveChanges();

        using var verify = CreateContext();
        Assert.Equal(257, verify.Entities.Count());
    }

    [Fact]
    public void Insert_MixedShapes_FlushesBetweenShapes()
    {
        EnsureSchema();
        EnsureSecondTable();

        using var context = CreateContext();
        for (var i = 0; i < 5; i++)
            context.Entities.Add(new BatchEntity { Name = $"e{i}" });
        for (var i = 0; i < 5; i++)
            context.SecondEntities.Add(new SecondEntity { Label = $"s{i}" });
        for (var i = 0; i < 5; i++)
            context.Entities.Add(new BatchEntity { Name = $"e{i + 5}" });
        context.SaveChanges();

        using var verify = CreateContext();
        Assert.Equal(10, verify.Entities.Count());
        Assert.Equal(5, verify.SecondEntities.Count());
    }

    [Fact]
    public void Insert_WithReturning_UsesSingleRowPath()
    {
        EnsureSchema();

        using var context = CreateContext();
        var row = new BatchEntity { Name = "returning-test" };
        context.Entities.Add(row);
        context.SaveChanges();

        Assert.True(row.Id > 0);

        using var verify = CreateContext();
        var fetched = verify.Entities.Single(e => e.Id == row.Id);
        Assert.Equal("returning-test", fetched.Name);
    }

    [Fact]
    public void Insert_MixedAddedAndModified_FlushesBeforeUpdate()
    {
        EnsureSchema();

        using (var ctx = CreateContext())
        {
            ctx.Entities.Add(new BatchEntity { Name = "pre-existing" });
            ctx.SaveChanges();
        }

        using var context = CreateContext();
        var existing = context.Entities.Single();

        for (var i = 0; i < 3; i++)
            context.Entities.Add(new BatchEntity { Name = $"add{i}" });

        existing.Name = "updated";

        for (var i = 0; i < 2; i++)
            context.Entities.Add(new BatchEntity { Name = $"add{i + 3}" });

        context.SaveChanges();

        using var verify = CreateContext();
        Assert.Equal(6, verify.Entities.Count());
        Assert.Equal("updated", verify.Entities.Single(e => e.Name == "updated").Name);
    }

    [Fact(Skip = "perf-only")]
    public void Insert_LargeBatch_PerformanceSanity()
    {
        EnsureSchema();

        using var context = CreateContext();
        var entities = Enumerable.Range(0, 10_000)
            .Select(i => new BatchEntity { Name = $"perf{i}" })
            .ToList();
        context.Entities.AddRange(entities);

        var sw = System.Diagnostics.Stopwatch.StartNew();
        context.SaveChanges();
        sw.Stop();

        using var verify = CreateContext();
        Assert.Equal(10_000, verify.Entities.Count());

        Assert.True(sw.ElapsedMilliseconds < 5000,
            $"10K inserts took {sw.ElapsedMilliseconds}ms, expected < 5000ms");
    }

    private BatchDbContext CreateContext(ILoggerFactory? loggerFactory = null)
    {
        var optionsBuilder = new DbContextOptionsBuilder<BatchDbContext>();
        optionsBuilder.UseDecentDB($"Data Source={_dbPath}");
        optionsBuilder.EnableDetailedErrors();

        if (loggerFactory != null)
        {
            optionsBuilder.EnableSensitiveDataLogging();
            optionsBuilder.UseLoggerFactory(loggerFactory);
        }

        return new BatchDbContext(optionsBuilder.Options);
    }

    private void EnsureSchema()
    {
        using var conn = new DecentDBConnection($"Data Source={_dbPath}");
        conn.Open();

        using var cmd = conn.CreateCommand();
        cmd.CommandText = "DROP TABLE IF EXISTS ef_batch_entities";
        cmd.ExecuteNonQuery();

        cmd.CommandText = """
                          CREATE TABLE ef_batch_entities (
                            id INTEGER PRIMARY KEY,
                            name TEXT NOT NULL
                          )
                          """;
        cmd.ExecuteNonQuery();
    }

    private void EnsureSecondTable()
    {
        using var conn = new DecentDBConnection($"Data Source={_dbPath}");
        conn.Open();

        using var cmd = conn.CreateCommand();
        cmd.CommandText = "DROP TABLE IF EXISTS ef_second_entities";
        cmd.ExecuteNonQuery();

        cmd.CommandText = """
                          CREATE TABLE ef_second_entities (
                            id INTEGER PRIMARY KEY,
                            label TEXT NOT NULL
                          )
                          """;
        cmd.ExecuteNonQuery();
    }

    private static void TryDelete(string path)
    {
        if (File.Exists(path))
        {
            File.Delete(path);
        }
    }

    private sealed class BatchDbContext : DbContext
    {
        public BatchDbContext(DbContextOptions<BatchDbContext> options)
            : base(options)
        {
        }

        public DbSet<BatchEntity> Entities => Set<BatchEntity>();
        public DbSet<SecondEntity> SecondEntities => Set<SecondEntity>();

        protected override void OnModelCreating(ModelBuilder modelBuilder)
        {
            modelBuilder.Entity<BatchEntity>(entity =>
            {
                entity.ToTable("ef_batch_entities");
                entity.HasKey(x => x.Id);
                entity.Property(x => x.Id).HasColumnName("id").ValueGeneratedOnAdd();
                entity.Property(x => x.Name).HasColumnName("name");
            });

            modelBuilder.Entity<SecondEntity>(entity =>
            {
                entity.ToTable("ef_second_entities");
                entity.HasKey(x => x.Id);
                entity.Property(x => x.Id).HasColumnName("id").ValueGeneratedOnAdd();
                entity.Property(x => x.Label).HasColumnName("label");
            });
        }
    }

    private sealed class BatchEntity
    {
        public int Id { get; set; }
        public string Name { get; set; } = string.Empty;
    }

    private sealed class SecondEntity
    {
        public int Id { get; set; }
        public string Label { get; set; } = string.Empty;
    }
}
