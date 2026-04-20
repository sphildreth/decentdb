using DecentDB.AdoNet;
using DecentDB.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore;
using Xunit;

namespace DecentDB.EntityFrameworkCore.Tests;

public sealed class GuidAndIdentifierRegressionTests : IDisposable
{
    private readonly string _dbPath = Path.Combine(
        Path.GetTempPath(),
        $"test_ef_guid_identifier_{Guid.NewGuid():N}.ddb");

    public void Dispose()
    {
        TryDelete(_dbPath);
        TryDelete(_dbPath + "-wal");
    }

    [Fact]
    public void EnsureCreated_MixedCaseTable_RemainsReachableFromUnquotedRawSql()
    {
        using var context = CreateIdentifierContext();
        context.Database.EnsureCreated();

        Assert.Equal(
            1,
            context.Database.ExecuteSqlRaw(
                "INSERT INTO ArtistStaging (Id, Name) VALUES (1, 'alpha')"));

        using var connection = new DecentDBConnection($"Data Source={_dbPath}");
        connection.Open();

        using var command = connection.CreateCommand();
        command.CommandText = "SELECT COUNT(*) FROM ArtistStaging";
        Assert.Equal(1L, command.ExecuteScalar());
    }

    [Fact]
    public void EnsureCreated_GuidEquality_PreservesIndexedLookups()
    {
        var apiKey = Guid.NewGuid();

        using (var writeContext = CreateGuidContext())
        {
            writeContext.Database.EnsureCreated();
            writeContext.Users.Add(new GuidLookupUser
            {
                Id = 1,
                ApiKey = apiKey,
                Name = "alpha"
            });
            writeContext.SaveChanges();
        }

        using var readContext = CreateGuidContext();
        var entity = readContext.Users.Single(user => user.ApiKey == apiKey);

        Assert.Equal(1, entity.Id);
        Assert.Equal("alpha", entity.Name);
    }

    private IdentifierResolutionContext CreateIdentifierContext()
    {
        var optionsBuilder = new DbContextOptionsBuilder<IdentifierResolutionContext>();
        optionsBuilder.UseDecentDB($"Data Source={_dbPath}");
        return new IdentifierResolutionContext(optionsBuilder.Options);
    }

    private GuidLookupContext CreateGuidContext()
    {
        var optionsBuilder = new DbContextOptionsBuilder<GuidLookupContext>();
        optionsBuilder.UseDecentDB($"Data Source={_dbPath}");
        return new GuidLookupContext(optionsBuilder.Options);
    }

    private static void TryDelete(string path)
    {
        if (File.Exists(path))
        {
            File.Delete(path);
        }
    }

    private sealed class IdentifierResolutionContext : DbContext
    {
        public IdentifierResolutionContext(DbContextOptions<IdentifierResolutionContext> options)
            : base(options)
        {
        }

        public DbSet<ArtistStagingRow> Rows => Set<ArtistStagingRow>();

        protected override void OnModelCreating(ModelBuilder modelBuilder)
        {
            modelBuilder.Entity<ArtistStagingRow>(entity =>
            {
                entity.ToTable("ArtistStaging");
                entity.HasKey(x => x.Id);
                entity.Property(x => x.Id).ValueGeneratedNever();
            });
        }
    }

    private sealed class GuidLookupContext : DbContext
    {
        public GuidLookupContext(DbContextOptions<GuidLookupContext> options)
            : base(options)
        {
        }

        public DbSet<GuidLookupUser> Users => Set<GuidLookupUser>();

        protected override void OnModelCreating(ModelBuilder modelBuilder)
        {
            modelBuilder.Entity<GuidLookupUser>(entity =>
            {
                entity.ToTable("guid_lookup_users");
                entity.HasKey(x => x.Id);
                entity.Property(x => x.Id).ValueGeneratedNever();
                entity.Property(x => x.ApiKey).HasColumnType("UUID");
                entity.HasIndex(x => x.ApiKey).IsUnique();
            });
        }
    }

    private sealed class ArtistStagingRow
    {
        public int Id { get; set; }

        public string Name { get; set; } = string.Empty;
    }

    private sealed class GuidLookupUser
    {
        public int Id { get; set; }

        public Guid ApiKey { get; set; }

        public string Name { get; set; } = string.Empty;
    }
}
