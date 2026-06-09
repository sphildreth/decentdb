using System.Globalization;
using DecentDB.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore;
using Xunit;

namespace DecentDB.EntityFrameworkCore.Tests;

public sealed class IndexedStringEqualityRegressionTests : IDisposable
{
    private const int RowCount = 6_000;
    private const int TargetId = 4_321;

    private readonly string _dbPath = Path.Combine(
        Path.GetTempPath(),
        $"test_ef_indexed_string_equality_{Guid.NewGuid():N}.ddb");

    public void Dispose()
    {
        TryDelete(_dbPath);
        TryDelete(_dbPath + "-wal");
    }

    [Fact]
    public void ExactEquality_OnLargeIndexedNormalizedNameAndRawId_ReturnsExpectedRow()
    {
        SeedData();

        using var context = CreateContext();
        var targetNormalizedName = BuildNormalizedName(TargetId);
        var targetRawId = BuildRawId(TargetId);

        var byNormalizedNameQuery = context.Artists
            .AsNoTracking()
            .Where(x => x.NameNormalized == targetNormalizedName);
        var byRawIdQuery = context.Artists
            .AsNoTracking()
            .Where(x => x.RawId == targetRawId);

        AssertExactEqualityPredicate(byNormalizedNameQuery.ToQueryString(), "name_normalized", "targetNormalizedName");
        AssertExactEqualityPredicate(byRawIdQuery.ToQueryString(), "raw_id", "targetRawId");

        var byNormalizedName = byNormalizedNameQuery.Single();
        var byRawId = byRawIdQuery.Single();

        Assert.Equal(RowCount, context.Artists.Count());
        Assert.Equal(TargetId, byNormalizedName.Id);
        Assert.Equal(TargetId, byRawId.Id);
        Assert.Equal(targetRawId, byNormalizedName.RawId);
        Assert.Equal(targetNormalizedName, byRawId.NameNormalized);
    }

    private IndexedStringDbContext CreateContext()
    {
        var optionsBuilder = new DbContextOptionsBuilder<IndexedStringDbContext>();
        optionsBuilder.UseDecentDB($"Data Source={_dbPath}");
        return new IndexedStringDbContext(optionsBuilder.Options);
    }

    private void SeedData()
    {
        using var context = CreateContext();
        Assert.True(context.Database.EnsureCreated());

        var artists = Enumerable.Range(1, RowCount)
            .Select(id => new IndexedArtist
            {
                Id = id,
                Name = $"Artist {id.ToString("D5", CultureInfo.InvariantCulture)}",
                NameNormalized = BuildNormalizedName(id),
                RawId = BuildRawId(id)
            })
            .ToArray();

        context.Artists.AddRange(artists);
        context.SaveChanges();
    }

    private static string BuildNormalizedName(int id)
    {
        var stableId = id.ToString("D5", CultureInfo.InvariantCulture);
        var repeatedToken = new string((char)('a' + (id % 26)), 384);
        return $"melodee normalized artist {stableId} {repeatedToken} canonical {stableId}";
    }

    private static string BuildRawId(int id)
    {
        var stableId = id.ToString("D12", CultureInfo.InvariantCulture);
        return $"musicbrainz:artist:00000000-0000-0000-0000-{stableId}";
    }

    private static void AssertExactEqualityPredicate(string sql, string columnName, string parameterName)
    {
        Assert.Contains("WHERE", sql, StringComparison.OrdinalIgnoreCase);
        Assert.Contains($"\"{columnName}\"", sql, StringComparison.Ordinal);
        Assert.Contains(parameterName, sql, StringComparison.Ordinal);
        Assert.Contains(" = ", sql, StringComparison.Ordinal);
        Assert.DoesNotContain("LIKE", sql, StringComparison.OrdinalIgnoreCase);
    }

    private static void TryDelete(string path)
    {
        if (File.Exists(path))
        {
            File.Delete(path);
        }
    }

    private sealed class IndexedStringDbContext : DbContext
    {
        public IndexedStringDbContext(DbContextOptions<IndexedStringDbContext> options)
            : base(options)
        {
        }

        public DbSet<IndexedArtist> Artists => Set<IndexedArtist>();

        protected override void OnModelCreating(ModelBuilder modelBuilder)
        {
            modelBuilder.Entity<IndexedArtist>(entity =>
            {
                entity.ToTable("ef_indexed_string_artists");
                entity.HasKey(x => x.Id);

                entity.Property(x => x.Id)
                    .HasColumnName("id")
                    .ValueGeneratedNever();
                entity.Property(x => x.Name)
                    .HasColumnName("name");
                entity.Property(x => x.NameNormalized)
                    .HasColumnName("name_normalized");
                entity.Property(x => x.RawId)
                    .HasColumnName("raw_id");

                entity.HasIndex(x => x.NameNormalized)
                    .HasDatabaseName("ix_ef_indexed_string_artists_name_normalized");
                entity.HasIndex(x => x.RawId)
                    .HasDatabaseName("ux_ef_indexed_string_artists_raw_id")
                    .IsUnique();
            });
        }
    }

    private sealed class IndexedArtist
    {
        public int Id { get; set; }
        public string Name { get; set; } = string.Empty;
        public string NameNormalized { get; set; } = string.Empty;
        public string RawId { get; set; } = string.Empty;
    }
}
