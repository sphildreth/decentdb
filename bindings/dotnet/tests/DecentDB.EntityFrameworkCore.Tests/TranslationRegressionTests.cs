using DecentDB.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore;
using Xunit;

namespace DecentDB.EntityFrameworkCore.Tests;

public sealed class TranslationRegressionTests : IDisposable
{
    private readonly string _dbPath = Path.Combine(Path.GetTempPath(), $"test_ef_translation_{Guid.NewGuid():N}.ddb");

    [Fact]
    public void NumericAggregates_ForDecimalAndUnsignedValues_Execute()
    {
        using var context = CreateContext();
        context.Database.EnsureCreated();

        context.Samples.AddRange(
            new TranslationSample
            {
                Id = 1,
                UInt64Value = 10,
                DecimalValue = 10.25m,
                NullableDateTime = new DateTime(2026, 3, 2, 9, 30, 0, DateTimeKind.Utc),
                NullableDateOnly = new DateOnly(2026, 3, 2),
                NullableTimeOnly = new TimeOnly(9, 30, 0)
            },
            new TranslationSample
            {
                Id = 2,
                UInt64Value = 20,
                DecimalValue = 20.75m,
                NullableDateTime = new DateTime(2024, 7, 10, 14, 45, 30, DateTimeKind.Utc),
                NullableDateOnly = new DateOnly(2024, 7, 10),
                NullableTimeOnly = new TimeOnly(14, 45, 30)
            });
        context.SaveChanges();

        var avgUnsigned = context.Samples.Average(x => (double)x.UInt64Value);
        var sumUnsigned = context.Samples.Sum(x => (double)x.UInt64Value);
        var avgDecimal = context.Samples.Average(x => (decimal?)x.DecimalValue);
        var sumDecimal = context.Samples.Sum(x => x.DecimalValue);

        Assert.Equal(15.0, avgUnsigned);
        Assert.Equal(30.0, sumUnsigned);
        Assert.Equal(15.50m, avgDecimal);
        Assert.Equal(31.00m, sumDecimal);
    }

    [Fact]
    public void DateTimeDateOnlyAndTimeOnlyMemberPredicates_Translate()
    {
        using var context = CreateContext();
        context.Database.EnsureCreated();

        context.Samples.AddRange(
            new TranslationSample
            {
                Id = 1,
                UInt64Value = 1,
                DecimalValue = 1m,
                NullableDateTime = new DateTime(2026, 3, 2, 9, 30, 0, DateTimeKind.Utc),
                NullableDateOnly = new DateOnly(2026, 3, 2),
                NullableTimeOnly = new TimeOnly(9, 30, 0)
            },
            new TranslationSample
            {
                Id = 2,
                UInt64Value = 2,
                DecimalValue = 2m,
                NullableDateTime = new DateTime(2024, 7, 10, 14, 45, 30, DateTimeKind.Utc),
                NullableDateOnly = new DateOnly(2024, 7, 10),
                NullableTimeOnly = new TimeOnly(14, 45, 30)
            },
            new TranslationSample
            {
                Id = 3,
                UInt64Value = 3,
                DecimalValue = 3m,
                NullableDateTime = null,
                NullableDateOnly = null,
                NullableTimeOnly = null
            });
        context.SaveChanges();

        var after2025 = context.Samples.Count(x => x.NullableDateTime != null && x.NullableDateTime.Value.Year > 2025);
        var marchDates = context.Samples.Count(x => x.NullableDateOnly != null && x.NullableDateOnly.Value.Month == 3);
        var hourMatches = context.Samples.Count(x => x.NullableTimeOnly != null && x.NullableTimeOnly.Value.Hour == 9);
        var minuteMatches = context.Samples.Count(x => x.NullableTimeOnly != null && x.NullableTimeOnly.Value.Minute == 30);
        var secondMatches = context.Samples.Count(x => x.NullableTimeOnly != null && x.NullableTimeOnly.Value.Second == 0);

        Assert.Equal(1, after2025);
        Assert.Equal(1, marchDates);
        Assert.Equal(1, hourMatches);
        Assert.Equal(1, minuteMatches);
        Assert.Equal(1, secondMatches);
    }

    public void Dispose()
    {
        TryDelete(_dbPath);
        TryDelete(_dbPath + "-wal");
    }

    private TranslationDbContext CreateContext()
    {
        var options = new DbContextOptionsBuilder<TranslationDbContext>()
            .UseDecentDB($"Data Source={_dbPath}")
            .Options;

        return new TranslationDbContext(options);
    }

    private static void TryDelete(string path)
    {
        if (File.Exists(path))
        {
            File.Delete(path);
        }
    }

    private sealed class TranslationDbContext : DbContext
    {
        public TranslationDbContext(DbContextOptions<TranslationDbContext> options)
            : base(options)
        {
        }

        public DbSet<TranslationSample> Samples => Set<TranslationSample>();

        protected override void OnModelCreating(ModelBuilder modelBuilder)
        {
            modelBuilder.Entity<TranslationSample>(entity =>
            {
                entity.ToTable("translation_samples");
                entity.HasKey(x => x.Id);
                entity.Property(x => x.Id).HasColumnName("id");
                entity.Property(x => x.UInt64Value).HasColumnName("uint64_value");
                entity.Property(x => x.DecimalValue).HasColumnName("decimal_value").HasPrecision(18, 2);
                entity.Property(x => x.NullableDateTime).HasColumnName("nullable_date_time");
                entity.Property(x => x.NullableDateOnly).HasColumnName("nullable_date_only");
                entity.Property(x => x.NullableTimeOnly).HasColumnName("nullable_time_only");
            });
        }
    }

    private sealed class TranslationSample
    {
        public int Id { get; set; }
        public ulong UInt64Value { get; set; }
        public decimal DecimalValue { get; set; }
        public DateTime? NullableDateTime { get; set; }
        public DateOnly? NullableDateOnly { get; set; }
        public TimeOnly? NullableTimeOnly { get; set; }
    }
}
