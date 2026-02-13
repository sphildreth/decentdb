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
        command.CommandText = "CREATE TABLE ef_noda_events (id INTEGER PRIMARY KEY, name TEXT NOT NULL, at_ms INTEGER NOT NULL, day_num INTEGER NOT NULL, local_ms INTEGER NOT NULL)";
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
                entity.Property(x => x.At).HasColumnName("at_ms");
                entity.Property(x => x.Day).HasColumnName("day_num");
                entity.Property(x => x.LocalAt).HasColumnName("local_ms");
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
