using DecentDB.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore;
using Xunit;

namespace DecentDB.EntityFrameworkCore.Tests;

public sealed class NullableAggregateShapeTests : IDisposable
{
    private readonly string _dbPath = Path.Combine(Path.GetTempPath(), $"test_ef_nullable_aggregates_{Guid.NewGuid():N}.ddb");

    [Fact]
    public void GroupBy_NullableAggregateProjection_Executes()
    {
        using var context = CreateContext();
        context.Database.EnsureCreated();

        context.Items.AddRange(
            new NullableAggregateItem
            {
                Id = 1,
                Bucket = "A",
                Quantity = 10,
                Score = 4.0,
                Price = 12.340m,
                CreatedAtUtc = new DateTime(2024, 1, 1, 8, 0, 0, DateTimeKind.Utc),
                IsActive = true,
            },
            new NullableAggregateItem
            {
                Id = 2,
                Bucket = "A",
                Quantity = null,
                Score = null,
                Price = null,
                CreatedAtUtc = null,
                IsActive = null,
            },
            new NullableAggregateItem
            {
                Id = 3,
                Bucket = "B",
                Quantity = null,
                Score = null,
                Price = null,
                CreatedAtUtc = null,
                IsActive = false,
            },
            new NullableAggregateItem
            {
                Id = 4,
                Bucket = "B",
                Quantity = null,
                Score = null,
                Price = null,
                CreatedAtUtc = null,
                IsActive = null,
            });
        context.SaveChanges();

        var results = context.Items
            .GroupBy(item => item.Bucket)
            .Select(group => new
            {
                Bucket = group.Key,
                RowCount = group.LongCount(),
                ActiveCount = group.LongCount(item => item.IsActive == true),
                SumQuantity = group.Sum(item => item.Quantity),
                AverageScore = group.Average(item => item.Score),
                MinQuantity = group.Min(item => item.Quantity),
                MaxQuantity = group.Max(item => item.Quantity),
                LowestPrice = group.Min(item => item.Price),
                HighestPrice = group.Max(item => item.Price),
                EarliestCreatedAtUtc = group.Min(item => item.CreatedAtUtc),
                LatestCreatedAtUtc = group.Max(item => item.CreatedAtUtc),
            })
            .OrderBy(group => group.Bucket)
            .ToList();

        Assert.Collection(
            results,
            row =>
            {
                Assert.Equal("A", row.Bucket);
                Assert.Equal(2L, row.RowCount);
                Assert.Equal(1L, row.ActiveCount);
                Assert.Equal(10L, row.SumQuantity);
                Assert.Equal(4.0, row.AverageScore);
                Assert.Equal(10L, row.MinQuantity);
                Assert.Equal(10L, row.MaxQuantity);
                Assert.Equal(12.340m, row.LowestPrice);
                Assert.Equal(12.340m, row.HighestPrice);
                Assert.Equal(new DateTime(2024, 1, 1, 8, 0, 0, DateTimeKind.Utc), row.EarliestCreatedAtUtc);
                Assert.Equal(new DateTime(2024, 1, 1, 8, 0, 0, DateTimeKind.Utc), row.LatestCreatedAtUtc);
            },
            row =>
            {
                Assert.Equal("B", row.Bucket);
                Assert.Equal(2L, row.RowCount);
                Assert.Equal(0L, row.ActiveCount);
                Assert.Equal(0L, row.SumQuantity);
                Assert.Null(row.AverageScore);
                Assert.Null(row.MinQuantity);
                Assert.Null(row.MaxQuantity);
                Assert.Null(row.LowestPrice);
                Assert.Null(row.HighestPrice);
                Assert.Null(row.EarliestCreatedAtUtc);
                Assert.Null(row.LatestCreatedAtUtc);
            });
    }

    [Fact]
    public void AggregateProjection_WithoutGroupBy_NullableAggregates_Execute()
    {
        using var context = CreateContext();
        context.Database.EnsureCreated();

        context.Items.AddRange(
            new NullableAggregateItem
            {
                Id = 1,
                Bucket = "A",
                Quantity = 10,
                Score = 4.0,
                Price = 12.340m,
                CreatedAtUtc = new DateTime(2024, 1, 1, 8, 0, 0, DateTimeKind.Utc),
                IsActive = true,
            },
            new NullableAggregateItem
            {
                Id = 2,
                Bucket = "B",
                Quantity = null,
                Score = null,
                Price = null,
                CreatedAtUtc = null,
                IsActive = null,
            });
        context.SaveChanges();

        var summary = new
        {
            RowCount = context.Items.LongCount(),
            ActiveCount = context.Items.LongCount(item => item.IsActive == true),
            SumQuantity = context.Items.Sum(item => item.Quantity),
            AverageScore = context.Items.Average(item => item.Score),
            MinQuantity = context.Items.Min(item => item.Quantity),
            MaxQuantity = context.Items.Max(item => item.Quantity),
            LowestPrice = context.Items.Min(item => item.Price),
            HighestPrice = context.Items.Max(item => item.Price),
            EarliestCreatedAtUtc = context.Items.Min(item => item.CreatedAtUtc),
            LatestCreatedAtUtc = context.Items.Max(item => item.CreatedAtUtc),
        };

        Assert.Equal(2L, summary.RowCount);
        Assert.Equal(1L, summary.ActiveCount);
        Assert.Equal(10L, summary.SumQuantity);
        Assert.Equal(4.0, summary.AverageScore);
        Assert.Equal(10L, summary.MinQuantity);
        Assert.Equal(10L, summary.MaxQuantity);
        Assert.Equal(12.340m, summary.LowestPrice);
        Assert.Equal(12.340m, summary.HighestPrice);
        Assert.Equal(new DateTime(2024, 1, 1, 8, 0, 0, DateTimeKind.Utc), summary.EarliestCreatedAtUtc);
        Assert.Equal(new DateTime(2024, 1, 1, 8, 0, 0, DateTimeKind.Utc), summary.LatestCreatedAtUtc);
    }

    [Fact]
    public void AggregateProjection_WithoutGroupBy_AllNullNullableAggregates_ReturnNulls()
    {
        using var context = CreateContext();
        context.Database.EnsureCreated();

        context.Items.AddRange(
            new NullableAggregateItem { Id = 1, Bucket = "A" },
            new NullableAggregateItem { Id = 2, Bucket = "B" });
        context.SaveChanges();

        var summary = new
        {
            RowCount = context.Items.LongCount(),
            ActiveCount = context.Items.LongCount(item => item.IsActive == true),
            SumQuantity = context.Items.Sum(item => item.Quantity),
            AverageScore = context.Items.Average(item => item.Score),
            MinQuantity = context.Items.Min(item => item.Quantity),
            MaxQuantity = context.Items.Max(item => item.Quantity),
            LowestPrice = context.Items.Min(item => item.Price),
            HighestPrice = context.Items.Max(item => item.Price),
            EarliestCreatedAtUtc = context.Items.Min(item => item.CreatedAtUtc),
            LatestCreatedAtUtc = context.Items.Max(item => item.CreatedAtUtc),
        };

        Assert.Equal(2L, summary.RowCount);
        Assert.Equal(0L, summary.ActiveCount);
        Assert.Equal(0L, summary.SumQuantity);
        Assert.Null(summary.AverageScore);
        Assert.Null(summary.MinQuantity);
        Assert.Null(summary.MaxQuantity);
        Assert.Null(summary.LowestPrice);
        Assert.Null(summary.HighestPrice);
        Assert.Null(summary.EarliestCreatedAtUtc);
        Assert.Null(summary.LatestCreatedAtUtc);
    }

    public void Dispose()
    {
        TryDelete(_dbPath);
        TryDelete(_dbPath + "-wal");
    }

    private NullableAggregateDbContext CreateContext()
    {
        var options = new DbContextOptionsBuilder<NullableAggregateDbContext>()
            .UseDecentDB($"Data Source={_dbPath}")
            .Options;

        return new NullableAggregateDbContext(options);
    }

    private static void TryDelete(string path)
    {
        if (File.Exists(path))
        {
            File.Delete(path);
        }
    }

    private sealed class NullableAggregateDbContext : DbContext
    {
        public NullableAggregateDbContext(DbContextOptions<NullableAggregateDbContext> options)
            : base(options)
        {
        }

        public DbSet<NullableAggregateItem> Items => Set<NullableAggregateItem>();

        protected override void OnModelCreating(ModelBuilder modelBuilder)
        {
            modelBuilder.Entity<NullableAggregateItem>(entity =>
            {
                entity.ToTable("nullable_aggregate_items");
                entity.HasKey(item => item.Id);
                entity.Property(item => item.Id).HasColumnName("id");
                entity.Property(item => item.Bucket).HasColumnName("bucket");
                entity.Property(item => item.Quantity).HasColumnName("quantity");
                entity.Property(item => item.Score).HasColumnName("score");
                entity.Property(item => item.Price).HasColumnName("price").HasPrecision(18, 3);
                entity.Property(item => item.CreatedAtUtc).HasColumnName("created_at_utc");
                entity.Property(item => item.IsActive).HasColumnName("is_active");
            });
        }
    }

    private sealed class NullableAggregateItem
    {
        public int Id { get; set; }
        public string Bucket { get; set; } = string.Empty;
        public long? Quantity { get; set; }
        public double? Score { get; set; }
        public decimal? Price { get; set; }
        public DateTime? CreatedAtUtc { get; set; }
        public bool? IsActive { get; set; }
    }
}
