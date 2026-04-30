using System.Diagnostics;
using System.Threading.Tasks;
using DecentDB.AdoNet;
using DecentDB.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore;
using Xunit;

namespace DecentDB.EntityFrameworkCore.Tests;

/// <summary>
/// Tests for the DecentDBModelBuilder cache and the UseDecentDB(model) overload.
/// </summary>
public sealed class ModelBuilderCacheTests : IDisposable
{
    private readonly string _dbPath = Path.Combine(Path.GetTempPath(), $"test_modelcache_{Guid.NewGuid():N}.ddb");

    public void Dispose()
    {
        TryDelete(_dbPath);
        TryDelete(_dbPath + "-wal");
    }

    private static void TryDelete(string path)
    {
        if (File.Exists(path)) File.Delete(path);
    }

    // Context type with parameterless constructor; uses OnConfiguring to set up connection.
    private sealed class TestContextA : DbContext
    {
        public TestContextA() : base(new DbContextOptionsBuilder<TestContextA>()
            .UseDecentDB(Path.GetTempFileName())
            .Options)
        {
        }

        // Constructor accepting options for manual construction in tests
        public TestContextA(DbContextOptions<TestContextA> options) : base(options) { }

        public DbSet<TestEntity> Entities => Set<TestEntity>();
        protected override void OnModelCreating(ModelBuilder modelBuilder)
        {
            modelBuilder.Entity<TestEntity>(e =>
            {
                e.ToTable("test_entities_a");
                e.HasKey(x => x.Id);
                e.Property(x => x.Id).HasColumnName("id").ValueGeneratedOnAdd();
                e.Property(x => x.Name).HasColumnName("name");
            });
        }
    }

    private sealed class TestContextB : DbContext
    {
        public TestContextB() : base(new DbContextOptionsBuilder<TestContextB>()
            .UseDecentDB(Path.GetTempFileName())
            .Options)
        {
        }

        public TestContextB(DbContextOptions<TestContextB> options) : base(options) { }

        public DbSet<TestEntity> Entities => Set<TestEntity>();
        protected override void OnModelCreating(ModelBuilder modelBuilder)
        {
            modelBuilder.Entity<TestEntity>(e =>
            {
                e.ToTable("test_entities_b");
                e.HasKey(x => x.Id);
                e.Property(x => x.Id).HasColumnName("id").ValueGeneratedOnAdd();
                e.Property(x => x.Name).HasColumnName("name");
            });
        }
    }

    private sealed class TestEntity
    {
        public int Id { get; set; }
        public string Name { get; set; } = string.Empty;
    }

    [Fact]
    public void BuildModel_CachesPerContextType()
    {
        // First call builds the model
        var model1 = DecentDBModelBuilder.BuildModel<TestContextA>();

        // Second call should hit the cache and return the same instance
        var model2 = DecentDBModelBuilder.BuildModel<TestContextA>();

        Assert.NotNull(model1);
        Assert.Same(model1, model2);
    }

    [Fact]
    public void BuildModel_IsThreadSafe()
    {
        // Stress test: call from 16 parallel threads
        var tasks = new Task[16];
        for (int i = 0; i < tasks.Length; i++)
        {
            tasks[i] = Task.Run(() =>
            {
                var model = DecentDBModelBuilder.BuildModel<TestContextA>();
                Assert.NotNull(model);
            });
        }
        Task.WaitAll(tasks);
    }

    [Fact]
    public void UseDecentDB_WithPrebuiltModel_UsesProvidedModel()
    {
        // Build the model ahead of time
        var prebuiltModel = DecentDBModelBuilder.BuildModel<TestContextB>();

        // Construct context with the prebuilt model — should succeed without rebuilding
        var options = new DbContextOptionsBuilder<TestContextB>()
            .UseDecentDB(_dbPath, prebuiltModel)
            .Options;

        using var ctx = new TestContextB(options);
        // The model used by the context should be the same as the prebuilt one
        Assert.Same(prebuiltModel, ctx.Model);
    }
}
