using DecentDB.AdoNet;
using DecentDB.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.DependencyInjection;
using Xunit;

namespace DecentDB.EntityFrameworkCore.Tests;

public sealed class GaConformanceTests : IDisposable
{
    private readonly string _dbPath = Path.Combine(Path.GetTempPath(), $"test_ef_ga_{Guid.NewGuid():N}.ddb");

    public void Dispose()
    {
        TryDelete(_dbPath);
        TryDelete(_dbPath + "-wal");
    }

    [Fact]
    public void BasicPredicatePaging_Works()
    {
        EnsureSchemaAndSeed();
        using var context = CreateContext();

        var ids = context.Items
            .Where(x => x.Value >= 20)
            .OrderBy(x => x.Id)
            .Skip(0)
            .Take(2)
            .Select(x => x.Id)
            .ToList();

        Assert.Equal(new long[] { 2, 3 }, ids);
    }

    [Fact]
    public void CorrelatedSubquery_IsTrackedAsKnownGap()
    {
        EnsureSchemaAndSeed();
        using var context = CreateContext();

        var correlated = context.Items
            .Where(x => context.Items.Any(other => other.Category == x.Category && other.Value > x.Value))
            .OrderBy(x => x.Id)
            .Select(x => x.Id)
            .ToQueryString();

        Assert.Contains("EXISTS", correlated);
        Assert.Throws<DecentDB.Native.DecentDBException>(() => context.Items
            .Where(x => context.Items.Any(other => other.Category == x.Category && other.Value > x.Value))
            .ToList());
    }

    [Fact]
    public void AddDbContextPool_ReusesContexts_Correctly()
    {
        EnsureSchemaAndSeed();

        var services = new ServiceCollection();
        services.AddDbContextPool<GaDbContext>(options => options.UseDecentDb($"Data Source={_dbPath}"));
        using var provider = services.BuildServiceProvider();

        for (var i = 0; i < 20; i++)
        {
            using var scope = provider.CreateScope();
            var context = scope.ServiceProvider.GetRequiredService<GaDbContext>();
            Assert.Equal(3, context.Items.Count());
        }
    }

    private GaDbContext CreateContext()
    {
        var optionsBuilder = new DbContextOptionsBuilder<GaDbContext>();
        optionsBuilder.UseDecentDb($"Data Source={_dbPath}");
        return new GaDbContext(optionsBuilder.Options);
    }

    private void EnsureSchemaAndSeed()
    {
        using var connection = new DecentDBConnection($"Data Source={_dbPath}");
        connection.Open();

        using var command = connection.CreateCommand();
        command.CommandText = "CREATE TABLE ef_ga_items (id INTEGER PRIMARY KEY, category TEXT NOT NULL, value INTEGER NOT NULL)";
        command.ExecuteNonQuery();

        command.CommandText = "INSERT INTO ef_ga_items (id, category, value) VALUES (1, 'a', 10), (2, 'a', 20), (3, 'b', 30)";
        command.ExecuteNonQuery();
    }

    private static void TryDelete(string path)
    {
        if (File.Exists(path))
        {
            File.Delete(path);
        }
    }

    private sealed class GaDbContext : DbContext
    {
        public GaDbContext(DbContextOptions<GaDbContext> options)
            : base(options)
        {
        }

        public DbSet<GaItem> Items => Set<GaItem>();

        protected override void OnModelCreating(ModelBuilder modelBuilder)
        {
            modelBuilder.Entity<GaItem>(entity =>
            {
                entity.ToTable("ef_ga_items");
                entity.HasKey(x => x.Id);
                entity.Property(x => x.Id).HasColumnName("id");
                entity.Property(x => x.Category).HasColumnName("category");
                entity.Property(x => x.Value).HasColumnName("value");
            });
        }
    }

    private sealed class GaItem
    {
        public long Id { get; set; }
        public string Category { get; set; } = string.Empty;
        public int Value { get; set; }
    }
}
