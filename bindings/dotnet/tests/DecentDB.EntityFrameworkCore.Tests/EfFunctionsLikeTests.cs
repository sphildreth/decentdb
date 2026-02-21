using DecentDB.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore;
using Xunit;

namespace DecentDB.EntityFrameworkCore.Tests;

/// <summary>
/// Verifies EF.Functions.Like translation for DecentDB.
/// EF Core's base RelationalMethodCallTranslatorProvider should translate
/// EF.Functions.Like to SQL LIKE. This test confirms it works with DecentDB.
/// </summary>
public sealed class EfFunctionsLikeTests : IDisposable
{
    private readonly string _tempDir = Path.Combine(Path.GetTempPath(), $"decentdb_like_{Guid.NewGuid():N}");

    public EfFunctionsLikeTests()
    {
        Directory.CreateDirectory(_tempDir);
    }

    public void Dispose()
    {
        if (Directory.Exists(_tempDir))
            Directory.Delete(_tempDir, true);
    }

    [Fact]
    public void EfFunctionsLike_TranslatesToSqlLike()
    {
        var dbPath = Path.Combine(_tempDir, "like.ddb");
        using var context = CreateContext(dbPath);
        context.Database.EnsureCreated();

        context.Items.AddRange(
            new LikeTestItem { Name = "Alice" },
            new LikeTestItem { Name = "Bob" },
            new LikeTestItem { Name = "Charlie" });
        context.SaveChanges();

        var results = context.Items.Where(i => EF.Functions.Like(i.Name, "%li%")).ToList();
        Assert.Equal(2, results.Count); // Alice, Charlie
    }

    private static LikeTestContext CreateContext(string dbPath)
    {
        var options = new DbContextOptionsBuilder<LikeTestContext>()
            .UseDecentDB($"Data Source={dbPath}")
            .Options;
        return new LikeTestContext(options);
    }

    public class LikeTestItem
    {
        public int Id { get; set; }
        public string Name { get; set; } = "";
    }

    private sealed class LikeTestContext : DbContext
    {
        public LikeTestContext(DbContextOptions<LikeTestContext> options) : base(options) { }
        public DbSet<LikeTestItem> Items { get; set; } = null!;
    }
}
