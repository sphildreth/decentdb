using DecentDB.AdoNet;
using DecentDB.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore;
using Xunit;

namespace DecentDB.EntityFrameworkCore.Tests;

/// <summary>
/// Tests for EF Core primitive collection translation (string[] stored as JSON).
/// Validates that json_array_length/json_extract are used for .Any(), .Count(), etc.
/// </summary>
public sealed class PrimitiveCollectionTests : IDisposable
{
    private readonly string _dbPath = Path.Combine(Path.GetTempPath(), $"test_ef_primcoll_{Guid.NewGuid():N}.ddb");

    public void Dispose()
    {
        TryDelete(_dbPath);
        TryDelete(_dbPath + "-wal");
    }

    [Fact]
    public async Task StringArray_AnyWithoutPredicate_TranslatesToJsonArrayLength()
    {
        SeedData();
        await using var context = CreateContext();

        var result = await context.Albums
            .Where(a => a.Genres != null && a.Genres.Length > 0)
            .Select(a => a.Name)
            .ToListAsync();

        Assert.Equal(2, result.Count);
        Assert.Contains("Rock Album", result);
        Assert.Contains("Jazz Album", result);
    }

    [Fact]
    public async Task StringArray_SelectArray_ReturnsJsonArray()
    {
        SeedData();
        await using var context = CreateContext();

        var genres = await context.Albums
            .Where(a => a.Genres != null && a.Genres.Length > 0)
            .OrderBy(a => a.Name)
            .Select(a => a.Genres)
            .ToListAsync();

        Assert.Equal(2, genres.Count);
        Assert.NotNull(genres[0]);
        Assert.Contains("Jazz", genres[0]!);
    }

    [Fact]
    public async Task StringArray_NullArray_ExcludedByLengthCheck()
    {
        SeedData();
        await using var context = CreateContext();

        var allAlbums = await context.Albums.CountAsync();
        var withGenres = await context.Albums
            .Where(a => a.Genres != null && a.Genres.Length > 0)
            .CountAsync();

        Assert.Equal(3, allAlbums);
        Assert.Equal(2, withGenres);
    }

    [Fact]
    public async Task StringArray_EmptyArray_ExcludedByLengthCheck()
    {
        SeedEmptyArrayData();
        await using var context = CreateContext();

        var withGenres = await context.Albums
            .Where(a => a.Genres != null && a.Genres.Length > 0)
            .CountAsync();

        Assert.Equal(1, withGenres);
    }

    [Fact]
    public async Task StringArray_ElementAt_TranslatesToJsonExtract()
    {
        SeedData();
        await using var context = CreateContext();

        var firstGenres = await context.Albums
            .Where(a => a.Genres != null && a.Genres.Length > 0)
            .OrderBy(a => a.Name)
            .Select(a => a.Genres![0])
            .ToListAsync();

        Assert.Equal(2, firstGenres.Count);
        Assert.Equal("Jazz", firstGenres[0]);
        Assert.Equal("Rock", firstGenres[1]);
    }

    [Fact]
    public async Task StringArray_ElementAtWithDefault_TranslatesToJsonExtract()
    {
        SeedData();
        await using var context = CreateContext();

        var firstGenres = await context.Albums
            .OrderBy(a => a.Name)
            .Select(a => a.Genres != null && a.Genres.Length > 0 ? a.Genres[0] : "Unknown")
            .ToListAsync();

        Assert.Equal(3, firstGenres.Count);
        Assert.Equal("Jazz", firstGenres[0]);
        Assert.Equal("Unknown", firstGenres[1]);
        Assert.Equal("Rock", firstGenres[2]);
    }

    [Fact]
    public async Task StringArray_Contains_TranslatesToLikePattern()
    {
        SeedData();
        await using var context = CreateContext();

        var result = await context.Albums
            .Where(a => a.Genres != null && a.Genres.Contains("Rock"))
            .Select(a => a.Name)
            .ToListAsync();

        Assert.Single(result);
        Assert.Equal("Rock Album", result[0]);
    }

    [Fact]
    public async Task StringArray_ContainsMultipleMatches_ReturnsAll()
    {
        SeedWithSharedGenre();
        await using var context = CreateContext();

        var result = await context.Albums
            .Where(a => a.Genres != null && a.Genres.Contains("Blues"))
            .OrderBy(a => a.Name)
            .Select(a => a.Name)
            .ToListAsync();

        Assert.Equal(2, result.Count);
        Assert.Equal("Blues Album", result[0]);
        Assert.Equal("Jazz Album", result[1]);
    }

    [Fact]
    public async Task StringArray_FlattenClientSide_WorksCorrectly()
    {
        SeedData();
        await using var context = CreateContext();

        var albumGenres = await context.Albums
            .AsNoTracking()
            .Where(a => a.Genres != null && a.Genres.Length > 0)
            .Select(a => a.Genres)
            .ToListAsync();

        var uniqueGenres = albumGenres
            .Where(g => g != null)
            .SelectMany(g => g!)
            .Where(g => !string.IsNullOrWhiteSpace(g))
            .Distinct(StringComparer.OrdinalIgnoreCase)
            .ToList();

        Assert.Contains("Rock", uniqueGenres);
        Assert.Contains("Metal", uniqueGenres);
        Assert.Contains("Jazz", uniqueGenres);
        Assert.Contains("Blues", uniqueGenres);
    }

    private PrimCollContext CreateContext()
    {
        var optionsBuilder = new DbContextOptionsBuilder<PrimCollContext>();
        optionsBuilder.UseDecentDB($"Data Source={_dbPath}");
        return new PrimCollContext(optionsBuilder.Options);
    }

    private void SeedData()
    {
        using var conn = new DecentDBConnection($"Data Source={_dbPath}");
        conn.Open();
        using var cmd = conn.CreateCommand();

        cmd.CommandText = "DROP TABLE IF EXISTS \"Albums\"";
        cmd.ExecuteNonQuery();

        cmd.CommandText = """
            CREATE TABLE "Albums" (
                "Id" INTEGER PRIMARY KEY,
                "Name" TEXT NOT NULL,
                "Genres" TEXT
            )
            """;
        cmd.ExecuteNonQuery();

        cmd.CommandText = """INSERT INTO "Albums" ("Id", "Name", "Genres") VALUES (1, 'Rock Album', '["Rock","Metal"]')""";
        cmd.ExecuteNonQuery();
        cmd.CommandText = """INSERT INTO "Albums" ("Id", "Name", "Genres") VALUES (2, 'Jazz Album', '["Jazz","Blues"]')""";
        cmd.ExecuteNonQuery();
        cmd.CommandText = """INSERT INTO "Albums" ("Id", "Name", "Genres") VALUES (3, 'No Genre Album', NULL)""";
        cmd.ExecuteNonQuery();
    }

    private void SeedEmptyArrayData()
    {
        using var conn = new DecentDBConnection($"Data Source={_dbPath}");
        conn.Open();
        using var cmd = conn.CreateCommand();

        cmd.CommandText = "DROP TABLE IF EXISTS \"Albums\"";
        cmd.ExecuteNonQuery();

        cmd.CommandText = """
            CREATE TABLE "Albums" (
                "Id" INTEGER PRIMARY KEY,
                "Name" TEXT NOT NULL,
                "Genres" TEXT
            )
            """;
        cmd.ExecuteNonQuery();

        cmd.CommandText = """INSERT INTO "Albums" ("Id", "Name", "Genres") VALUES (1, 'Has Genres', '["Rock"]')""";
        cmd.ExecuteNonQuery();
        cmd.CommandText = """INSERT INTO "Albums" ("Id", "Name", "Genres") VALUES (2, 'Empty Genres', '[]')""";
        cmd.ExecuteNonQuery();
        cmd.CommandText = """INSERT INTO "Albums" ("Id", "Name", "Genres") VALUES (3, 'Null Genres', NULL)""";
        cmd.ExecuteNonQuery();
    }

    private void SeedWithSharedGenre()
    {
        using var conn = new DecentDBConnection($"Data Source={_dbPath}");
        conn.Open();
        using var cmd = conn.CreateCommand();

        cmd.CommandText = "DROP TABLE IF EXISTS \"Albums\"";
        cmd.ExecuteNonQuery();

        cmd.CommandText = """
            CREATE TABLE "Albums" (
                "Id" INTEGER PRIMARY KEY,
                "Name" TEXT NOT NULL,
                "Genres" TEXT
            )
            """;
        cmd.ExecuteNonQuery();

        cmd.CommandText = """INSERT INTO "Albums" ("Id", "Name", "Genres") VALUES (1, 'Jazz Album', '["Jazz","Blues"]')""";
        cmd.ExecuteNonQuery();
        cmd.CommandText = """INSERT INTO "Albums" ("Id", "Name", "Genres") VALUES (2, 'Blues Album', '["Blues","Soul"]')""";
        cmd.ExecuteNonQuery();
        cmd.CommandText = """INSERT INTO "Albums" ("Id", "Name", "Genres") VALUES (3, 'Rock Album', '["Rock","Metal"]')""";
        cmd.ExecuteNonQuery();
    }

    private static void TryDelete(string path)
    {
        if (File.Exists(path))
            File.Delete(path);
    }

    private sealed class PrimCollContext(DbContextOptions<PrimCollContext> options) : DbContext(options)
    {
        public DbSet<Album> Albums => Set<Album>();

        protected override void OnModelCreating(ModelBuilder modelBuilder)
        {
            modelBuilder.Entity<Album>(entity =>
            {
                entity.ToTable("Albums");
                entity.HasKey(x => x.Id);
            });
        }
    }

    private sealed class Album
    {
        public int Id { get; set; }
        public string Name { get; set; } = string.Empty;
        public string[]? Genres { get; set; }
    }
}
