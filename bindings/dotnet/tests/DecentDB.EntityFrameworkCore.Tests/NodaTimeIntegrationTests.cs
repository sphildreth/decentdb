using DecentDB.AdoNet;
using DecentDB.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Infrastructure;
using Microsoft.EntityFrameworkCore.Storage;
using NodaTime;
using Xunit;

namespace DecentDB.EntityFrameworkCore.Tests;

public sealed class NodaTimeIntegrationTests : IDisposable
{
    private readonly string _dbPath = Path.Combine(Path.GetTempPath(), $"test_ef_nodatime_{Guid.NewGuid():N}.ddb");

    public void Dispose()
    {
        TryDelete(_dbPath);
        TryDelete(_dbPath + "-wal");
    }

    [Fact]
    public void UseNodaTime_RegistersTypeMappings_AndRoundTrips()
    {
        EnsureSchema();

        using var context = CreateContext();
        var mappingSource = context.GetService<IRelationalTypeMappingSource>();
        Assert.Equal("INTEGER", GetMapping<Instant>(mappingSource).StoreType);
        Assert.Equal("INTEGER", GetMapping<LocalDate>(mappingSource).StoreType);
        Assert.Equal("INTEGER", GetMapping<LocalDateTime>(mappingSource).StoreType);

        var row = new NodaEvent
        {
            Name = "n1",
            At = Instant.FromDateTimeUtc(new DateTime(2026, 1, 2, 3, 4, 0, DateTimeKind.Utc)),
            Day = new LocalDate(2026, 1, 2),
            LocalAt = new LocalDateTime(2026, 1, 2, 3, 4, 5)
        };

        context.Events.Add(row);
        context.SaveChanges();

        var loaded = context.Events.Single(x => x.Id == row.Id);
        Assert.Equal(row.At, loaded.At);
        Assert.Equal(row.Day, loaded.Day);
        Assert.Equal(row.LocalAt, loaded.LocalAt);
    }

    [Fact]
    public void UseNodaTime_PreservesTickPrecision_ForInstant()
    {
        EnsureSchema();

        using var context = CreateContext();

        // Instant with sub-millisecond precision (microseconds)
        var preciseInstant = Instant.FromUnixTimeTicks(17095044690001234L);
        var row = new NodaEvent
        {
            Name = "precision_test",
            At = preciseInstant,
            Day = new LocalDate(2026, 1, 2),
            LocalAt = new LocalDateTime(2026, 1, 2, 3, 4, 5, 678).PlusNanoseconds(912300)
        };

        context.Events.Add(row);
        context.SaveChanges();

        var loaded = context.Events.Single(x => x.Name == "precision_test");
        Assert.Equal(preciseInstant, loaded.At);
        Assert.Equal(row.LocalAt, loaded.LocalAt);
    }

    [Fact]
    public void UseNodaTime_TranslatesLocalDateYear_InGroupBy()
    {
        EnsureSchema();

        using var context = CreateContext();
        context.Events.AddRange(
            new NodaEvent { Name = "a", At = Instant.FromUtc(2024, 1, 1, 0, 0), Day = new LocalDate(2024, 3, 15), LocalAt = new LocalDateTime(2024, 3, 15, 0, 0, 0) },
            new NodaEvent { Name = "b", At = Instant.FromUtc(2024, 6, 1, 0, 0), Day = new LocalDate(2024, 6, 1), LocalAt = new LocalDateTime(2024, 6, 1, 0, 0, 0) },
            new NodaEvent { Name = "c", At = Instant.FromUtc(2025, 1, 1, 0, 0), Day = new LocalDate(2025, 1, 10), LocalAt = new LocalDateTime(2025, 1, 10, 0, 0, 0) });
        context.SaveChanges();

        var byYear = context.Events
            .GroupBy(e => e.Day.Year)
            .Select(g => new { Year = g.Key, Count = g.Count() })
            .OrderBy(x => x.Year)
            .ToList();

        Assert.Equal(2, byYear.Count);
        Assert.Equal(2024, byYear[0].Year);
        Assert.Equal(2, byYear[0].Count);
        Assert.Equal(2025, byYear[1].Year);
        Assert.Equal(1, byYear[1].Count);
    }

    [Fact]
    public void UseNodaTime_TranslatesLocalDateMonthAndDay_InProjection()
    {
        EnsureSchema();

        using var context = CreateContext();
        context.Events.Add(new NodaEvent
        {
            Name = "date_parts",
            At = Instant.FromUtc(2026, 7, 23, 0, 0),
            Day = new LocalDate(2026, 7, 23),
            LocalAt = new LocalDateTime(2026, 7, 23, 0, 0, 0)
        });
        context.SaveChanges();

        var result = context.Events
            .Where(e => e.Name == "date_parts")
            .Select(e => new { e.Day.Year, e.Day.Month, e.Day.Day })
            .Single();

        Assert.Equal(2026, result.Year);
        Assert.Equal(7, result.Month);
        Assert.Equal(23, result.Day);
    }

    [Fact]
    public void UseNodaTime_TranslatesLocalDateYear_ForHistoricAndEpochDates()
    {
        EnsureSchema();

        using var context = CreateContext();
        context.Events.AddRange(
            new NodaEvent { Name = "epoch", At = Instant.FromUtc(1970, 1, 1, 0, 0), Day = new LocalDate(1970, 1, 1), LocalAt = new LocalDateTime(1970, 1, 1, 0, 0, 0) },
            new NodaEvent { Name = "pre_epoch", At = Instant.FromUtc(1969, 12, 31, 0, 0), Day = new LocalDate(1969, 12, 31), LocalAt = new LocalDateTime(1969, 12, 31, 0, 0, 0) },
            new NodaEvent { Name = "leap", At = Instant.FromUtc(2000, 2, 29, 0, 0), Day = new LocalDate(2000, 2, 29), LocalAt = new LocalDateTime(2000, 2, 29, 0, 0, 0) });
        context.SaveChanges();

        var results = context.Events
            .OrderBy(e => e.Name)
            .Select(e => new { e.Name, e.Day.Year, e.Day.Month, e.Day.Day })
            .ToList();

        var epoch = results.Single(r => r.Name == "epoch");
        Assert.Equal(1970, epoch.Year);
        Assert.Equal(1, epoch.Month);
        Assert.Equal(1, epoch.Day);

        var preEpoch = results.Single(r => r.Name == "pre_epoch");
        Assert.Equal(1969, preEpoch.Year);
        Assert.Equal(12, preEpoch.Month);
        Assert.Equal(31, preEpoch.Day);

        var leap = results.Single(r => r.Name == "leap");
        Assert.Equal(2000, leap.Year);
        Assert.Equal(2, leap.Month);
        Assert.Equal(29, leap.Day);
    }

    private NodaDbContext CreateContext()
    {
        var optionsBuilder = new DbContextOptionsBuilder<NodaDbContext>();
        optionsBuilder.UseDecentDB($"Data Source={_dbPath}", options => options.UseNodaTime());
        return new NodaDbContext(optionsBuilder.Options);
    }

    private void EnsureSchema()
    {
        using var connection = new DecentDBConnection($"Data Source={_dbPath}");
        connection.Open();

        using var command = connection.CreateCommand();
        command.CommandText = "CREATE TABLE ef_noda_events (id INTEGER PRIMARY KEY, name TEXT NOT NULL, at_ticks INTEGER NOT NULL, day_num INTEGER NOT NULL, local_ticks INTEGER NOT NULL)";
        command.ExecuteNonQuery();
    }

    private static RelationalTypeMapping GetMapping<T>(IRelationalTypeMappingSource mappingSource)
        => (RelationalTypeMapping)mappingSource.FindMapping(typeof(T))!;

    private static void TryDelete(string path)
    {
        if (File.Exists(path))
        {
            File.Delete(path);
        }
    }

    private sealed class NodaDbContext : DbContext
    {
        public NodaDbContext(DbContextOptions<NodaDbContext> options)
            : base(options)
        {
        }

        public DbSet<NodaEvent> Events => Set<NodaEvent>();

        protected override void OnModelCreating(ModelBuilder modelBuilder)
        {
            modelBuilder.Entity<NodaEvent>(entity =>
            {
                entity.ToTable("ef_noda_events");
                entity.HasKey(x => x.Id);
                entity.Property(x => x.Id).HasColumnName("id").ValueGeneratedOnAdd();
                entity.Property(x => x.Name).HasColumnName("name");
                entity.Property(x => x.At).HasColumnName("at_ticks");
                entity.Property(x => x.Day).HasColumnName("day_num");
                entity.Property(x => x.LocalAt).HasColumnName("local_ticks");
            });
        }
    }

    private sealed class NodaEvent
    {
        public long Id { get; set; }
        public string Name { get; set; } = string.Empty;
        public Instant At { get; set; }
        public LocalDate Day { get; set; }
        public LocalDateTime LocalAt { get; set; }
    }
}
