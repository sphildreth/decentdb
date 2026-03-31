using DecentDB.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore;
using Xunit;

namespace DecentDB.EntityFrameworkCore.Tests;

public sealed class DecimalAggregateQueryTests : IDisposable
{
    private readonly string _dbPath = Path.Combine(Path.GetTempPath(), $"test_ef_decimal_agg_{Guid.NewGuid():N}.ddb");

    [Fact]
    public void GroupBy_MaxDecimalProjection_WithOrdering_Executes()
    {
        using var context = CreateContext();
        context.Database.EnsureCreated();

        context.Items.AddRange(
            new AggregateItem { Id = 1, Category = "Books", Price = 12.10m, IsFeatured = true },
            new AggregateItem { Id = 2, Category = "Books", Price = 48.598m, IsFeatured = false },
            new AggregateItem { Id = 3, Category = "Books", Price = 8.25m, IsFeatured = true },
            new AggregateItem { Id = 4, Category = "Games", Price = 1200.10m, IsFeatured = false },
            new AggregateItem { Id = 5, Category = "Games", Price = 999.95m, IsFeatured = true },
            new AggregateItem { Id = 6, Category = "Garden", Price = 87.50m, IsFeatured = false });
        context.SaveChanges();

        var results = context.Items
            .GroupBy(item => item.Category)
            .Select(group => new
            {
                Category = group.Key,
                ProductCount = group.LongCount(),
                FeaturedCount = group.LongCount(item => item.IsFeatured),
                HighestPrice = group.Max(item => item.Price),
            })
            .OrderByDescending(group => group.ProductCount)
            .ThenBy(group => group.Category)
            .Take(5)
            .ToList();

        Assert.Collection(
            results,
            row =>
            {
                Assert.Equal("Books", row.Category);
                Assert.Equal(3L, row.ProductCount);
                Assert.Equal(2L, row.FeaturedCount);
                Assert.Equal(48.598m, row.HighestPrice);
            },
            row =>
            {
                Assert.Equal("Games", row.Category);
                Assert.Equal(2L, row.ProductCount);
                Assert.Equal(1L, row.FeaturedCount);
                Assert.Equal(1200.10m, row.HighestPrice);
            },
            row =>
            {
                Assert.Equal("Garden", row.Category);
                Assert.Equal(1L, row.ProductCount);
                Assert.Equal(0L, row.FeaturedCount);
                Assert.Equal(87.50m, row.HighestPrice);
            });
    }

    public void Dispose()
    {
        TryDelete(_dbPath);
        TryDelete(_dbPath + "-wal");
    }

    private AggregateDbContext CreateContext()
    {
        var options = new DbContextOptionsBuilder<AggregateDbContext>()
            .UseDecentDB($"Data Source={_dbPath}")
            .Options;

        return new AggregateDbContext(options);
    }

    private static void TryDelete(string path)
    {
        if (File.Exists(path))
        {
            File.Delete(path);
        }
    }

    private sealed class AggregateDbContext : DbContext
    {
        public AggregateDbContext(DbContextOptions<AggregateDbContext> options)
            : base(options)
        {
        }

        public DbSet<AggregateItem> Items => Set<AggregateItem>();

        protected override void OnModelCreating(ModelBuilder modelBuilder)
        {
            modelBuilder.Entity<AggregateItem>(entity =>
            {
                entity.ToTable("aggregate_items");
                entity.HasKey(item => item.Id);
                entity.Property(item => item.Id).HasColumnName("id");
                entity.Property(item => item.Category).HasColumnName("category");
                entity.Property(item => item.IsFeatured).HasColumnName("is_featured");
                entity.Property(item => item.Price).HasColumnName("price").HasPrecision(18, 3);
            });
        }
    }

    private sealed class AggregateItem
    {
        public int Id { get; set; }
        public string Category { get; set; } = string.Empty;
        public decimal Price { get; set; }
        public bool IsFeatured { get; set; }
    }
}
