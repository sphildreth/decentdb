using DecentDB.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Infrastructure;
using Microsoft.EntityFrameworkCore.Metadata.Builders;
using NodaTime;
using Xunit;

namespace DecentDB.EntityFrameworkCore.Tests;

public sealed class HasDataLiteralMatrixTests : IDisposable
{
    private readonly List<string> _dbPaths = new();

    [Fact]
    public void EnsureCreated_ByteSeed_PersistsSeedRow()
    {
        using var context = CreateContext(new[] { Row(1, (byte)42) });
        Assert.True(context.Database.EnsureCreated());

        var seeded = context.Rows.Single();
        Assert.Equal((byte)42, seeded.Value);
    }

    [Fact]
    public void EnsureCreated_SByteSeed_PersistsSeedRow()
    {
        using var context = CreateContext(new[] { Row(1, (sbyte)-42) });
        Assert.True(context.Database.EnsureCreated());

        var seeded = context.Rows.Single();
        Assert.Equal((sbyte)-42, seeded.Value);
    }

    [Fact]
    public void EnsureCreated_ShortSeed_PersistsSeedRow()
    {
        using var context = CreateContext(new[] { Row(1, (short)-12345) });
        Assert.True(context.Database.EnsureCreated());

        var seeded = context.Rows.Single();
        Assert.Equal((short)-12345, seeded.Value);
    }

    [Fact]
    public void EnsureCreated_IntSeed_PersistsSeedRow()
    {
        using var context = CreateContext(new[] { Row(1, 123456789) });
        Assert.True(context.Database.EnsureCreated());

        var seeded = context.Rows.Single();
        Assert.Equal(123456789, seeded.Value);
    }

    [Fact]
    public void EnsureCreated_LongSeed_PersistsSeedRow()
    {
        using var context = CreateContext(new[] { Row(1, 9_000_000_000_123L) });
        Assert.True(context.Database.EnsureCreated());

        var seeded = context.Rows.Single();
        Assert.Equal(9_000_000_000_123L, seeded.Value);
    }

    [Fact]
    public void EnsureCreated_UShortSeed_PersistsSeedRow()
    {
        using var context = CreateContext(new[] { Row(1, ushort.MaxValue) });
        Assert.True(context.Database.EnsureCreated());

        var seeded = context.Rows.Single();
        Assert.Equal(ushort.MaxValue, seeded.Value);
    }

    [Fact]
    public void EnsureCreated_UIntSeed_PersistsSeedRow()
    {
        using var context = CreateContext(new[] { Row(1, 4_000_000_000u) });
        Assert.True(context.Database.EnsureCreated());

        var seeded = context.Rows.Single();
        Assert.Equal(4_000_000_000u, seeded.Value);
    }

    [Fact]
    public void EnsureCreated_ULongSeed_InRange_PersistsSeedRow()
    {
        using var context = CreateContext(new[] { Row(1, (ulong)long.MaxValue) });
        Assert.True(context.Database.EnsureCreated());

        var seeded = context.Rows.Single();
        Assert.Equal((ulong)long.MaxValue, seeded.Value);
    }

    [Fact]
    public void EnsureCreated_ULongSeed_Overflow_ThrowsOverflowException()
    {
        using var context = CreateContext(new[] { Row(1, ulong.MaxValue) });

        var ex = Assert.Throws<OverflowException>(() => context.Database.EnsureCreated());
        Assert.Contains("INT64 range", ex.Message, StringComparison.Ordinal);
    }

    [Fact]
    public void EnsureCreated_FloatSeed_PersistsSeedRow()
    {
        using var context = CreateContext(new[] { Row(1, 3.14f) });
        Assert.True(context.Database.EnsureCreated());

        var seeded = context.Rows.Single();
        Assert.Equal(3.14f, seeded.Value);
    }

    [Fact(Skip = "blocked on S2 follow-up: NaN/Infinity literal parsing in HasData")]
    public void EnsureCreated_FloatSeed_NaNAndInfinity()
    {
        using var context = CreateContext(new[]
        {
            Row(1, float.NaN),
            Row(2, float.PositiveInfinity),
            Row(3, float.NegativeInfinity)
        });
        Assert.True(context.Database.EnsureCreated());

        var rows = context.Rows.OrderBy(x => x.Id).ToArray();
        Assert.Equal(float.NaN, rows[0].Value);
        Assert.Equal(float.PositiveInfinity, rows[1].Value);
        Assert.Equal(float.NegativeInfinity, rows[2].Value);
    }

    [Fact]
    public void EnsureCreated_DoubleSeed_PersistsSeedRow()
    {
        using var context = CreateContext(new[] { Row(1, 2.718281828) });
        Assert.True(context.Database.EnsureCreated());

        var seeded = context.Rows.Single();
        Assert.Equal(2.718281828, seeded.Value);
    }

    [Fact(Skip = "blocked on S2 follow-up: NaN/Infinity literal parsing in HasData")]
    public void EnsureCreated_DoubleSeed_NaNAndInfinity()
    {
        using var context = CreateContext(new[]
        {
            Row(1, double.NaN),
            Row(2, double.PositiveInfinity),
            Row(3, double.NegativeInfinity)
        });
        Assert.True(context.Database.EnsureCreated());

        var rows = context.Rows.OrderBy(x => x.Id).ToArray();
        Assert.Equal(double.NaN, rows[0].Value);
        Assert.Equal(double.PositiveInfinity, rows[1].Value);
        Assert.Equal(double.NegativeInfinity, rows[2].Value);
    }

    [Fact]
    public void EnsureCreated_DecimalSeed_DefaultPrecision()
    {
        using var context = CreateContext(new[] { Row(1, 1234.5678m) });
        Assert.True(context.Database.EnsureCreated());

        var seeded = context.Rows.Single();
        Assert.Equal(1234.5678m, seeded.Value);
    }

    [Fact]
    public void EnsureCreated_DecimalSeed_CustomPrecision_10_2()
    {
        using var context = CreateContext(
            new[]
            {
                Row(1, 1.005m),
                Row(2, -1.005m)
            },
            configureValue: property => property.HasPrecision(10, 2));
        Assert.True(context.Database.EnsureCreated());

        var rows = context.Rows.OrderBy(x => x.Id).ToArray();
        Assert.Equal(1.00m, rows[0].Value);
        Assert.Equal(-1.00m, rows[1].Value);
    }

    [Fact]
    public void EnsureCreated_DecimalSeed_CustomPrecision_18_4_MaxScale()
    {
        using var context = CreateContext(
            new[] { Row(1, 1234.56789m) },
            configureValue: property => property.HasPrecision(18, 4));
        Assert.True(context.Database.EnsureCreated());

        var seeded = context.Rows.Single();
        Assert.Equal(1234.5679m, seeded.Value);
    }

    [Fact]
    public void EnsureCreated_StringSeed_PersistsAllEscapedForms()
    {
        using var context = CreateContext(new[]
        {
            Row(1, "simple-ascii"),
            Row(2, "with 'single' quotes"),
            Row(3, "backslash\\newline\nrow"),
            Row(4, "日本語 🎵 Straße")
        });
        Assert.True(context.Database.EnsureCreated());

        var rows = context.Rows.OrderBy(x => x.Id).ToArray();
        Assert.Equal("simple-ascii", rows[0].Value);
        Assert.Equal("with 'single' quotes", rows[1].Value);
        Assert.Equal("backslash\\newline\nrow", rows[2].Value);
        Assert.Equal("日本語 🎵 Straße", rows[3].Value);
    }

    [Fact]
    public void EnsureCreated_CharSeed_PersistsSeedRow()
    {
        using var context = CreateContext(new[] { Row(1, 'A') });
        Assert.True(context.Database.EnsureCreated());

        var seeded = context.Rows.Single();
        Assert.Equal('A', seeded.Value);
    }

    [Fact]
    public void EnsureCreated_CharSeed_Surrogate_ThrowsArgumentException()
    {
        using var context = CreateContext(new[] { Row(1, '\uD83D') });

        var ex = Assert.Throws<ArgumentException>(() => context.Database.EnsureCreated());
        Assert.Contains("Surrogate", ex.Message, StringComparison.Ordinal);
    }

    [Fact(Skip = "blocked on S5: byte[] literal form for HasData is not yet resolved")]
    public void EnsureCreated_ByteArraySeed_PersistsSeedRow()
    {
        using var context = CreateContext(new[] { Row(1, new byte[] { 0x01, 0x02, 0xFF, 0x00, 0xAB }) });
        Assert.True(context.Database.EnsureCreated());

        var seeded = context.Rows.Single();
        Assert.Equal(new byte[] { 0x01, 0x02, 0xFF, 0x00, 0xAB }, seeded.Value);
    }

    [Fact(Skip = "blocked on S5: byte[] literal form for HasData is not yet resolved")]
    public void EnsureCreated_ByteArraySeed_EmptyArray()
    {
        using var context = CreateContext(new[] { Row(1, Array.Empty<byte>()) });
        Assert.True(context.Database.EnsureCreated());

        var seeded = context.Rows.Single();
        Assert.Empty(seeded.Value);
    }

    [Fact]
    public void EnsureCreated_DateTimeSeed_UtcKind()
    {
        var utcValue = new DateTime(2024, 1, 2, 3, 4, 5, DateTimeKind.Utc);
        using var context = CreateContext(new[] { Row(1, utcValue) });
        Assert.True(context.Database.EnsureCreated());

        var seeded = context.Rows.Single();
        Assert.Equal(utcValue, seeded.Value);
    }

    [Fact]
    public void EnsureCreated_DateTimeSeed_LocalKind()
    {
        var localValue = new DateTime(2024, 1, 2, 3, 4, 5, DateTimeKind.Local);
        using var context = CreateContext(new[] { Row(1, localValue) });
        Assert.True(context.Database.EnsureCreated());

        var seeded = context.Rows.Single();
        Assert.Equal(localValue.ToUniversalTime(), seeded.Value);
        Assert.Equal(DateTimeKind.Utc, seeded.Value.Kind);
    }

    [Fact]
    public void EnsureCreated_DateTimeOffsetSeed_VariousOffsets()
    {
        var utc = new DateTimeOffset(2024, 1, 2, 3, 4, 5, TimeSpan.Zero);
        var positive = new DateTimeOffset(2024, 1, 2, 9, 4, 5, TimeSpan.FromHours(6));
        var negative = new DateTimeOffset(2024, 1, 1, 20, 4, 5, TimeSpan.FromHours(-7));

        using var context = CreateContext(new[]
        {
            Row(1, utc),
            Row(2, positive),
            Row(3, negative)
        });
        Assert.True(context.Database.EnsureCreated());

        var rows = context.Rows.OrderBy(x => x.Id).ToArray();
        Assert.Equal(utc.ToUniversalTime(), rows[0].Value.ToUniversalTime());
        Assert.Equal(positive.ToUniversalTime(), rows[1].Value.ToUniversalTime());
        Assert.Equal(negative.ToUniversalTime(), rows[2].Value.ToUniversalTime());
    }

    [Fact]
    public void EnsureCreated_DateOnlySeed_PersistsSeedRow()
    {
        using var context = CreateContext(new[] { Row(1, new DateOnly(2024, 2, 4)) });
        Assert.True(context.Database.EnsureCreated());

        var seeded = context.Rows.Single();
        Assert.Equal(new DateOnly(2024, 2, 4), seeded.Value);
    }

    [Fact]
    public void EnsureCreated_TimeOnlySeed_PersistsSeedRow()
    {
        using var context = CreateContext(new[] { Row(1, new TimeOnly(9, 30, 15)) });
        Assert.True(context.Database.EnsureCreated());

        var seeded = context.Rows.Single();
        Assert.Equal(new TimeOnly(9, 30, 15), seeded.Value);
    }

    [Fact]
    public void EnsureCreated_TimeSpanSeed_PersistsSeedRow()
    {
        using var context = CreateContext(new[] { Row(1, TimeSpan.FromHours(12.5)) });
        Assert.True(context.Database.EnsureCreated());

        var seeded = context.Rows.Single();
        Assert.Equal(TimeSpan.FromHours(12.5), seeded.Value);
    }

    [Fact]
    public void EnsureCreated_GuidSeed_GuidEmpty()
    {
        using var context = CreateContext(new[] { Row(1, Guid.Empty) });
        Assert.True(context.Database.EnsureCreated());

        var seeded = context.Rows.Single();
        Assert.Equal(Guid.Empty, seeded.Value);
    }

    [Fact]
    public void EnsureCreated_EnumSeed_IntBacked()
    {
        using var context = CreateContext(new[] { Row(1, IntBackedState.Active) });
        Assert.True(context.Database.EnsureCreated());

        var seeded = context.Rows.Single();
        Assert.Equal(IntBackedState.Active, seeded.Value);
    }

    [Fact]
    public void EnsureCreated_EnumSeed_LongBacked()
    {
        using var context = CreateContext(new[] { Row(1, LongBackedState.Ready) });
        Assert.True(context.Database.EnsureCreated());

        var seeded = context.Rows.Single();
        Assert.Equal(LongBackedState.Ready, seeded.Value);
    }

    [Fact]
    public void EnsureCreated_NullableBoolSeed_WithAndWithoutValue()
    {
        using var context = CreateContext(new[]
        {
            Row(1, (bool?)true),
            Row(2, (bool?)null)
        });
        Assert.True(context.Database.EnsureCreated());

        var rows = context.Rows.OrderBy(x => x.Id).ToArray();
        Assert.True(rows[0].Value);
        Assert.Null(rows[1].Value);
    }

    [Fact]
    public void EnsureCreated_NullableGuidSeed_WithAndWithoutValue()
    {
        var guid = Guid.Parse("11111111-2222-3333-4444-555555555555");
        using var context = CreateContext(new[]
        {
            Row(1, (Guid?)guid),
            Row(2, (Guid?)null)
        });
        Assert.True(context.Database.EnsureCreated());

        var rows = context.Rows.OrderBy(x => x.Id).ToArray();
        Assert.Equal(guid, rows[0].Value);
        Assert.Null(rows[1].Value);
    }

    [Fact]
    public void EnsureCreated_NullableDateTimeSeed_WithAndWithoutValue()
    {
        var utc = new DateTime(2024, 1, 2, 3, 4, 5, DateTimeKind.Utc);
        using var context = CreateContext(new[]
        {
            Row(1, (DateTime?)utc),
            Row(2, (DateTime?)null)
        });
        Assert.True(context.Database.EnsureCreated());

        var rows = context.Rows.OrderBy(x => x.Id).ToArray();
        Assert.Equal(utc, rows[0].Value);
        Assert.Null(rows[1].Value);
    }

    [Fact]
    public void EnsureCreated_InstantSeed_PersistsSeedRow()
    {
        var instant = Instant.FromDateTimeUtc(new DateTime(2024, 1, 2, 3, 4, 5, DateTimeKind.Utc));
        using var context = CreateContext(
            new[] { Row(1, instant) },
            useNodaTime: true);
        Assert.True(context.Database.EnsureCreated());

        var seeded = context.Rows.Single();
        Assert.Equal(instant, seeded.Value);
    }

    [Fact]
    public void EnsureCreated_LocalDateSeed_PersistsSeedRow()
    {
        var date = new LocalDate(2024, 2, 4);
        using var context = CreateContext(
            new[] { Row(1, date) },
            useNodaTime: true);
        Assert.True(context.Database.EnsureCreated());

        var seeded = context.Rows.Single();
        Assert.Equal(date, seeded.Value);
    }

    [Fact]
    public void EnsureCreated_LocalDateTimeSeed_PersistsSeedRow()
    {
        var localDateTime = new LocalDateTime(2024, 2, 4, 9, 30, 15);
        using var context = CreateContext(
            new[] { Row(1, localDateTime) },
            useNodaTime: true);
        Assert.True(context.Database.EnsureCreated());

        var seeded = context.Rows.Single();
        Assert.Equal(localDateTime, seeded.Value);
    }

    public void Dispose()
    {
        foreach (var path in _dbPaths)
        {
            TryDelete(path);
            TryDelete(path + "-wal");
        }
    }

    private SeedContext<TValue> CreateContext<TValue>(
        SeedRow<TValue>[] seedRows,
        Action<PropertyBuilder<TValue>>? configureValue = null,
        bool useNodaTime = false)
    {
        var dbPath = NewDbPath();
        var tableName = $"matrix_seed_{Guid.NewGuid():N}";
        var optionsBuilder = new DbContextOptionsBuilder<SeedContext<TValue>>();
        if (useNodaTime)
        {
            optionsBuilder.UseDecentDB($"Data Source={dbPath}", options => options.UseNodaTime());
        }
        else
        {
            optionsBuilder.UseDecentDB($"Data Source={dbPath}");
        }
        optionsBuilder.ReplaceService<IModelCacheKeyFactory, SeedContextModelCacheKeyFactory>();

        return new SeedContext<TValue>(optionsBuilder.Options, tableName, seedRows, configureValue);
    }

    private string NewDbPath()
    {
        var path = Path.Combine(Path.GetTempPath(), $"test_ef_hasdata_matrix_{Guid.NewGuid():N}.ddb");
        _dbPaths.Add(path);
        return path;
    }

    private static SeedRow<TValue> Row<TValue>(long id, TValue value)
        => new()
        {
            Id = id,
            Value = value
        };

    private static void TryDelete(string path)
    {
        if (File.Exists(path))
        {
            File.Delete(path);
        }
    }

    private enum IntBackedState
    {
        Unknown = 0,
        Active = 1
    }

    private enum LongBackedState : long
    {
        Unknown = 0,
        Ready = 5_000_000_001L
    }

    private sealed class SeedContextModelCacheKeyFactory : IModelCacheKeyFactory
    {
        public object Create(DbContext context, bool designTime)
            => context is ISeedModelCacheKeyProvider provider
                ? (context.GetType(), provider.ModelCacheKey, designTime)
                : (object)(context.GetType(), designTime);
    }

    private sealed class SeedContext<TValue> : DbContext, ISeedModelCacheKeyProvider
    {
        private readonly string _tableName;
        private readonly SeedRow<TValue>[] _seedRows;
        private readonly Action<PropertyBuilder<TValue>>? _configureValue;

        public SeedContext(
            DbContextOptions<SeedContext<TValue>> options,
            string tableName,
            SeedRow<TValue>[] seedRows,
            Action<PropertyBuilder<TValue>>? configureValue)
            : base(options)
        {
            _tableName = tableName;
            _seedRows = seedRows;
            _configureValue = configureValue;
            ModelCacheKey = tableName;
        }

        public string ModelCacheKey { get; }

        public DbSet<SeedRow<TValue>> Rows => Set<SeedRow<TValue>>();

        protected override void OnModelCreating(ModelBuilder modelBuilder)
        {
            var entity = modelBuilder.Entity<SeedRow<TValue>>();
            entity.ToTable(_tableName);
            entity.HasKey(x => x.Id);
            var valueProperty = entity.Property(x => x.Value).HasColumnName("value");
            _configureValue?.Invoke(valueProperty);
            entity.HasData(_seedRows);
        }
    }

    private interface ISeedModelCacheKeyProvider
    {
        string ModelCacheKey { get; }
    }

    private sealed class SeedRow<TValue>
    {
        public long Id { get; set; }
        public TValue Value { get; set; } = default!;
    }
}
