using DecentDB.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore;
using Xunit;

namespace DecentDB.EntityFrameworkCore.Tests;

public sealed class AggregateShapeTests : IDisposable
{
    private readonly string _dbPath = Path.Combine(Path.GetTempPath(), $"test_ef_aggregates_{Guid.NewGuid():N}.ddb");

    [Fact]
    public void GroupBy_MixedTypeAggregateProjection_Executes()
    {
        using var context = CreateContext();
        context.Database.EnsureCreated();

        context.Items.AddRange(
            new AggregateShapeItem
            {
                Id = 1,
                Bucket = "North",
                Quantity = 10,
                Score = 4.0,
                Price = 12.340m,
                Label = "alpha",
                CreatedAtUtc = new DateTime(2024, 1, 1, 8, 0, 0, DateTimeKind.Utc),
                IsActive = true,
            },
            new AggregateShapeItem
            {
                Id = 2,
                Bucket = "North",
                Quantity = 20,
                Score = 5.0,
                Price = 99.990m,
                Label = "delta",
                CreatedAtUtc = new DateTime(2024, 1, 4, 9, 30, 0, DateTimeKind.Utc),
                IsActive = false,
            },
            new AggregateShapeItem
            {
                Id = 3,
                Bucket = "South",
                Quantity = 5,
                Score = 3.0,
                Price = 5.500m,
                Label = "beta",
                CreatedAtUtc = new DateTime(2024, 2, 1, 6, 0, 0, DateTimeKind.Utc),
                IsActive = true,
            },
            new AggregateShapeItem
            {
                Id = 4,
                Bucket = "South",
                Quantity = 15,
                Score = 6.0,
                Price = 250.000m,
                Label = "gamma",
                CreatedAtUtc = new DateTime(2024, 2, 7, 12, 0, 0, DateTimeKind.Utc),
                IsActive = true,
            },
            new AggregateShapeItem
            {
                Id = 5,
                Bucket = "South",
                Quantity = 25,
                Score = 9.0,
                Price = 19.990m,
                Label = "epsilon",
                CreatedAtUtc = new DateTime(2024, 2, 3, 18, 45, 0, DateTimeKind.Utc),
                IsActive = false,
            });
        context.SaveChanges();

        var results = context.Items
            .GroupBy(item => item.Bucket)
            .Select(group => new
            {
                Bucket = group.Key,
                RowCount = group.LongCount(),
                ActiveCount = group.LongCount(item => item.IsActive),
                TotalQuantity = group.Sum(item => item.Quantity),
                MaxQuantity = group.Max(item => item.Quantity),
                MinQuantity = group.Min(item => item.Quantity),
                AverageScore = group.Average(item => item.Score),
                HighestPrice = group.Max(item => item.Price),
                LowestPrice = group.Min(item => item.Price),
                FirstLabel = group.Min(item => item.Label),
                LastLabel = group.Max(item => item.Label),
                EarliestCreatedAtUtc = group.Min(item => item.CreatedAtUtc),
                LatestCreatedAtUtc = group.Max(item => item.CreatedAtUtc),
            })
            .OrderBy(group => group.Bucket)
            .ToList();

        Assert.Collection(
            results,
            row =>
            {
                Assert.Equal("North", row.Bucket);
                Assert.Equal(2L, row.RowCount);
                Assert.Equal(1L, row.ActiveCount);
                Assert.Equal(30L, row.TotalQuantity);
                Assert.Equal(20L, row.MaxQuantity);
                Assert.Equal(10L, row.MinQuantity);
                Assert.Equal(4.5, row.AverageScore);
                Assert.Equal(99.990m, row.HighestPrice);
                Assert.Equal(12.340m, row.LowestPrice);
                Assert.Equal("alpha", row.FirstLabel);
                Assert.Equal("delta", row.LastLabel);
                Assert.Equal(new DateTime(2024, 1, 1, 8, 0, 0, DateTimeKind.Utc), row.EarliestCreatedAtUtc);
                Assert.Equal(new DateTime(2024, 1, 4, 9, 30, 0, DateTimeKind.Utc), row.LatestCreatedAtUtc);
            },
            row =>
            {
                Assert.Equal("South", row.Bucket);
                Assert.Equal(3L, row.RowCount);
                Assert.Equal(2L, row.ActiveCount);
                Assert.Equal(45L, row.TotalQuantity);
                Assert.Equal(25L, row.MaxQuantity);
                Assert.Equal(5L, row.MinQuantity);
                Assert.Equal(6.0, row.AverageScore);
                Assert.Equal(250.000m, row.HighestPrice);
                Assert.Equal(5.500m, row.LowestPrice);
                Assert.Equal("beta", row.FirstLabel);
                Assert.Equal("gamma", row.LastLabel);
                Assert.Equal(new DateTime(2024, 2, 1, 6, 0, 0, DateTimeKind.Utc), row.EarliestCreatedAtUtc);
                Assert.Equal(new DateTime(2024, 2, 7, 12, 0, 0, DateTimeKind.Utc), row.LatestCreatedAtUtc);
            });
    }

    [Fact]
    public void AggregateProjection_WithoutGroupBy_Executes()
    {
        using var context = CreateContext();
        context.Database.EnsureCreated();

        context.Items.AddRange(
            new AggregateShapeItem
            {
                Id = 1,
                Bucket = "A",
                Quantity = 2,
                Score = 1.5,
                Price = 10.250m,
                Label = "bravo",
                CreatedAtUtc = new DateTime(2024, 3, 1, 0, 0, 0, DateTimeKind.Utc),
                IsActive = true,
            },
            new AggregateShapeItem
            {
                Id = 2,
                Bucket = "B",
                Quantity = 8,
                Score = 2.5,
                Price = 20.750m,
                Label = "charlie",
                CreatedAtUtc = new DateTime(2024, 3, 4, 0, 0, 0, DateTimeKind.Utc),
                IsActive = false,
            });
        context.SaveChanges();

        var summary = new
        {
            RowCount = context.Items.LongCount(),
            ActiveCount = context.Items.LongCount(item => item.IsActive),
            TotalQuantity = context.Items.Sum(item => item.Quantity),
            AverageScore = context.Items.Average(item => item.Score),
            HighestPrice = context.Items.Max(item => item.Price),
            LowestPrice = context.Items.Min(item => item.Price),
            FirstLabel = context.Items.Min(item => item.Label),
            LastLabel = context.Items.Max(item => item.Label),
            EarliestCreatedAtUtc = context.Items.Min(item => item.CreatedAtUtc),
            LatestCreatedAtUtc = context.Items.Max(item => item.CreatedAtUtc),
        };

        Assert.Equal(2L, summary.RowCount);
        Assert.Equal(1L, summary.ActiveCount);
        Assert.Equal(10L, summary.TotalQuantity);
        Assert.Equal(2.0, summary.AverageScore);
        Assert.Equal(20.750m, summary.HighestPrice);
        Assert.Equal(10.250m, summary.LowestPrice);
        Assert.Equal("bravo", summary.FirstLabel);
        Assert.Equal("charlie", summary.LastLabel);
        Assert.Equal(new DateTime(2024, 3, 1, 0, 0, 0, DateTimeKind.Utc), summary.EarliestCreatedAtUtc);
        Assert.Equal(new DateTime(2024, 3, 4, 0, 0, 0, DateTimeKind.Utc), summary.LatestCreatedAtUtc);
    }

    public void Dispose()
    {
        TryDelete(_dbPath);
        TryDelete(_dbPath + "-wal");
    }

    private AggregateShapeDbContext CreateContext()
    {
        var options = new DbContextOptionsBuilder<AggregateShapeDbContext>()
            .UseDecentDB($"Data Source={_dbPath}")
            .Options;

        return new AggregateShapeDbContext(options);
    }

    private static void TryDelete(string path)
    {
        if (File.Exists(path))
        {
            File.Delete(path);
        }
    }

    private sealed class AggregateShapeDbContext : DbContext
    {
        public AggregateShapeDbContext(DbContextOptions<AggregateShapeDbContext> options)
            : base(options)
        {
        }

        public DbSet<AggregateShapeItem> Items => Set<AggregateShapeItem>();

        protected override void OnModelCreating(ModelBuilder modelBuilder)
        {
            modelBuilder.Entity<AggregateShapeItem>(entity =>
            {
                entity.ToTable("aggregate_shape_items");
                entity.HasKey(item => item.Id);
                entity.Property(item => item.Id).HasColumnName("id");
                entity.Property(item => item.Bucket).HasColumnName("bucket");
                entity.Property(item => item.Quantity).HasColumnName("quantity");
                entity.Property(item => item.Score).HasColumnName("score");
                entity.Property(item => item.Price).HasColumnName("price").HasPrecision(18, 3);
                entity.Property(item => item.Label).HasColumnName("label");
                entity.Property(item => item.CreatedAtUtc).HasColumnName("created_at_utc");
                entity.Property(item => item.IsActive).HasColumnName("is_active");
            });
        }
    }

    private sealed class AggregateShapeItem
    {
        public int Id { get; set; }
        public string Bucket { get; set; } = string.Empty;
        public long Quantity { get; set; }
        public double Score { get; set; }
        public decimal Price { get; set; }
        public string Label { get; set; } = string.Empty;
        public DateTime CreatedAtUtc { get; set; }
        public bool IsActive { get; set; }
    }
}
