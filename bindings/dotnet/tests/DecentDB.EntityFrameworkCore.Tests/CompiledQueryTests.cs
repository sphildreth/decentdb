using System.Diagnostics;
using DecentDB.AdoNet;
using DecentDB.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore;
using Xunit;

namespace DecentDB.EntityFrameworkCore.Tests;

/// <summary>
/// N19: Tests for compiled query performance and EF.CompileQuery usage.
/// </summary>
public sealed class CompiledQueryTests : IDisposable
{
    private readonly string _dbPath = Path.Combine(Path.GetTempPath(), $"test_compiled_{Guid.NewGuid():N}.ddb");

    public void Dispose()
    {
        TryDelete(_dbPath);
        TryDelete(_dbPath + "-wal");
    }

    private static void TryDelete(string path)
    {
        if (File.Exists(path)) File.Delete(path);
    }

    private sealed class Artist
    {
        public int Id { get; set; }
        public string Name { get; set; } = string.Empty;
    }

    private sealed class TestContext : DbContext
    {
        public TestContext(DbContextOptions<TestContext> options) : base(options) { }
        public DbSet<Artist> Artists => Set<Artist>();
        protected override void OnModelCreating(ModelBuilder modelBuilder)
        {
            modelBuilder.Entity<Artist>(e =>
            {
                e.ToTable("artists");
                e.HasKey(x => x.Id);
                e.Property(x => x.Id).HasColumnName("id").ValueGeneratedOnAdd();
                e.Property(x => x.Name).HasColumnName("name");
            });
        }
    }

    // Pre-compiled query for hot-path usage
    private static readonly Func<TestContext, int, Artist?> _artistByIdCompiled =
        EF.CompileQuery((TestContext ctx, int id) => ctx.Artists.FirstOrDefault(a => a.Id == id));

    [Fact]
    public void EfCore_CompiledQuery_ExampleCompiles_AndExecutes()
    {
        var options = new DbContextOptionsBuilder<TestContext>()
            .UseDecentDB(_dbPath)
            .Options;

        using (var ctx = new TestContext(options))
        {
            ctx.Database.EnsureCreated();
            ctx.Artists.Add(new Artist { Name = "TestArtist" });
            ctx.SaveChanges();
        }

        using (var ctx = new TestContext(options))
        {
            // Execute the compiled query
            var artist = _artistByIdCompiled(ctx, 1);
            Assert.NotNull(artist);
            Assert.Equal("TestArtist", artist.Name);

            // Non-existent ID returns null
            var missing = _artistByIdCompiled(ctx, 999);
            Assert.Null(missing);
        }
    }

    [Fact]
    public void EfCore_RepeatedCount_StaysUnderFiveMs()
    {
        var options = new DbContextOptionsBuilder<TestContext>()
            .UseDecentDB(_dbPath)
            .Options;

        using (var ctx = new TestContext(options))
        {
            ctx.Database.EnsureCreated();
            ctx.Artists.Add(new Artist { Name = "A" });
            ctx.SaveChanges();
        }

        // Warm up EF Core's query cache
        using (var ctx = new TestContext(options))
        {
            _ = ctx.Artists.Count();
        }

        // Measure repeated Count() — should be fast after warm-up
        using var ctx2 = new TestContext(options);
        var sw = Stopwatch.StartNew();
        for (int i = 0; i < 100; i++)
        {
            _ = ctx2.Artists.Count();
        }
        sw.Stop();

        // Average per-call should be under 5 ms (total under 500 ms for 100 calls)
        var avgMs = sw.ElapsedMilliseconds / 100.0;
        Assert.True(avgMs < 5.0,
            $"Average Count() call took {avgMs:F2} ms (total {sw.ElapsedMilliseconds} ms for 100 calls)");
    }

    [Fact]
    public void EfCore_RepeatedPointLookup_StaysUnderThreeMs()
    {
        var options = new DbContextOptionsBuilder<TestContext>()
            .UseDecentDB(_dbPath)
            .Options;

        using (var ctx = new TestContext(options))
        {
            ctx.Database.EnsureCreated();
            ctx.Artists.Add(new Artist { Name = "A" });
            ctx.SaveChanges();
        }

        // Warm up
        using (var ctx = new TestContext(options))
        {
            _ = ctx.Artists.FirstOrDefault(a => a.Id == 1);
        }

        // Measure repeated point lookup — should be fast after warm-up
        using var ctx2 = new TestContext(options);
        var sw = Stopwatch.StartNew();
        for (int i = 0; i < 100; i++)
        {
            _ = ctx2.Artists.FirstOrDefault(a => a.Id == 1);
        }
        sw.Stop();

        // Average per-call should be under 3 ms (total under 300 ms for 100 calls)
        var avgMs = sw.ElapsedMilliseconds / 100.0;
        Assert.True(avgMs < 3.0,
            $"Average FirstOrDefault() call took {avgMs:F2} ms (total {sw.ElapsedMilliseconds} ms for 100 calls)");
    }
}
