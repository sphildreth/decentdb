using DecentDB.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore;
using Xunit;

namespace DecentDB.EntityFrameworkCore.Tests;

public sealed class HasDataLiteralRegressionTests : IDisposable
{
    private readonly string _boolDbPath = Path.Combine(Path.GetTempPath(), $"test_ef_hasdata_bool_{Guid.NewGuid():N}.ddb");
    private readonly string _guidDbPath = Path.Combine(Path.GetTempPath(), $"test_ef_hasdata_guid_{Guid.NewGuid():N}.ddb");
    private readonly string _nodaDbPath = Path.Combine(Path.GetTempPath(), $"test_ef_hasdata_noda_{Guid.NewGuid():N}.ddb");

    [Fact]
    public void EnsureCreated_WithHasDataBoolSeed_PersistsSeedRow()
    {
        using var context = CreateBoolSeedContext();

        var created = context.Database.EnsureCreated();

        Assert.True(created);
        var seeded = context.Items.Single();
        Assert.Equal(1L, seeded.Id);
        Assert.False(seeded.IsEnabled);
        Assert.Equal("bool-seed", seeded.Name);
    }

    [Fact]
    public void EnsureCreated_WithHasDataGuidSeed_PersistsSeedRow()
    {
        using var context = CreateGuidSeedContext();

        var created = context.Database.EnsureCreated();

        Assert.True(created);
        var seeded = context.Items.Single();
        Assert.Equal(1L, seeded.Id);
        Assert.Equal(Guid.Parse("aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee"), seeded.ExternalId);
        Assert.Equal("guid-seed", seeded.Name);
    }

    [Fact]
    public void EnsureCreated_WithHasDataBoolAndGuidSeed_UsingNodaTime_PersistsSeedRow()
    {
        using var context = CreateNodaLiteralSeedContext();

        var created = context.Database.EnsureCreated();

        Assert.True(created);
        var seeded = context.Items.Single();
        Assert.Equal(1L, seeded.Id);
        Assert.False(seeded.IsEnabled);
        Assert.Equal(Guid.Parse("11111111-2222-3333-4444-555555555555"), seeded.ExternalId);
        Assert.Equal("noda-seed", seeded.Name);
    }

    public void Dispose()
    {
        TryDelete(_boolDbPath);
        TryDelete(_boolDbPath + "-wal");
        TryDelete(_guidDbPath);
        TryDelete(_guidDbPath + "-wal");
        TryDelete(_nodaDbPath);
        TryDelete(_nodaDbPath + "-wal");
    }

    private BoolSeedContext CreateBoolSeedContext()
    {
        var optionsBuilder = new DbContextOptionsBuilder<BoolSeedContext>();
        optionsBuilder.UseDecentDB($"Data Source={_boolDbPath}");
        return new BoolSeedContext(optionsBuilder.Options);
    }

    private GuidSeedContext CreateGuidSeedContext()
    {
        var optionsBuilder = new DbContextOptionsBuilder<GuidSeedContext>();
        optionsBuilder.UseDecentDB($"Data Source={_guidDbPath}");
        return new GuidSeedContext(optionsBuilder.Options);
    }

    private NodaLiteralSeedContext CreateNodaLiteralSeedContext()
    {
        var optionsBuilder = new DbContextOptionsBuilder<NodaLiteralSeedContext>();
        optionsBuilder.UseDecentDB($"Data Source={_nodaDbPath}", builder => builder.UseNodaTime());
        return new NodaLiteralSeedContext(optionsBuilder.Options);
    }

    private static void TryDelete(string path)
    {
        if (File.Exists(path))
        {
            File.Delete(path);
        }
    }

    private sealed class BoolSeedContext(DbContextOptions<BoolSeedContext> options) : DbContext(options)
    {
        public DbSet<BoolSeedItem> Items => Set<BoolSeedItem>();

        protected override void OnModelCreating(ModelBuilder modelBuilder)
        {
            modelBuilder.Entity<BoolSeedItem>(entity =>
            {
                entity.ToTable("bool_seed_items");
                entity.HasKey(x => x.Id);
                entity.HasData(new BoolSeedItem
                {
                    Id = 1,
                    Name = "bool-seed",
                    IsEnabled = false
                });
            });
        }
    }

    private sealed class GuidSeedContext(DbContextOptions<GuidSeedContext> options) : DbContext(options)
    {
        public DbSet<GuidSeedItem> Items => Set<GuidSeedItem>();

        protected override void OnModelCreating(ModelBuilder modelBuilder)
        {
            modelBuilder.Entity<GuidSeedItem>(entity =>
            {
                entity.ToTable("guid_seed_items");
                entity.HasKey(x => x.Id);
                entity.HasData(new GuidSeedItem
                {
                    Id = 1,
                    Name = "guid-seed",
                    ExternalId = Guid.Parse("aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee")
                });
            });
        }
    }

    private sealed class NodaLiteralSeedContext(DbContextOptions<NodaLiteralSeedContext> options) : DbContext(options)
    {
        public DbSet<NodaLiteralSeedItem> Items => Set<NodaLiteralSeedItem>();

        protected override void OnModelCreating(ModelBuilder modelBuilder)
        {
            modelBuilder.Entity<NodaLiteralSeedItem>(entity =>
            {
                entity.ToTable("noda_literal_seed_items");
                entity.HasKey(x => x.Id);
                entity.HasData(new NodaLiteralSeedItem
                {
                    Id = 1,
                    Name = "noda-seed",
                    IsEnabled = false,
                    ExternalId = Guid.Parse("11111111-2222-3333-4444-555555555555")
                });
            });
        }
    }

    private sealed class BoolSeedItem
    {
        public long Id { get; set; }
        public string Name { get; set; } = string.Empty;
        public bool IsEnabled { get; set; }
    }

    private sealed class GuidSeedItem
    {
        public long Id { get; set; }
        public string Name { get; set; } = string.Empty;
        public Guid ExternalId { get; set; }
    }

    private sealed class NodaLiteralSeedItem
    {
        public long Id { get; set; }
        public string Name { get; set; } = string.Empty;
        public bool IsEnabled { get; set; }
        public Guid ExternalId { get; set; }
    }
}
