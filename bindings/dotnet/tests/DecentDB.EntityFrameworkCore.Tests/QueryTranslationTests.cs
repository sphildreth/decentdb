using DecentDB.AdoNet;
using DecentDB.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore;
using Xunit;

namespace DecentDB.EntityFrameworkCore.Tests;

public sealed class QueryTranslationTests : IDisposable
{
    private readonly string _dbPath = Path.Combine(Path.GetTempPath(), $"test_ef_query_{Guid.NewGuid():N}.ddb");

    public void Dispose()
    {
        TryDelete(_dbPath);
        TryDelete(_dbPath + "-wal");
    }

    [Fact]
    public void BasicLinqQuery_TranslatesAndExecutes()
    {
        SeedData();

        using var context = CreateContext();

        var query = context.Items
            .Where(x => x.IsActive && x.Name.Contains("a"))
            .OrderBy(x => x.Id)
            .Skip(1)
            .Take(1)
            .Select(x => x.Name);

        var sql = query.ToQueryString();
        Assert.Contains("LIMIT", sql, StringComparison.OrdinalIgnoreCase);
        Assert.Contains("OFFSET", sql, StringComparison.OrdinalIgnoreCase);
        Assert.Contains("LIKE", sql, StringComparison.OrdinalIgnoreCase);

        var result = query.ToList();
        Assert.Single(result);
    }

    [Fact]
    public void StartsWithAndEndsWith_TranslateWithLike()
    {
        SeedData();

        using var context = CreateContext();

        var startsWithQuery = context.Items.Where(x => x.Name.StartsWith("a"));
        var endsWithQuery = context.Items.Where(x => x.Name.EndsWith("a"));

        var startsSql = startsWithQuery.ToQueryString();
        var endsSql = endsWithQuery.ToQueryString();

        Assert.Contains("LIKE", startsSql, StringComparison.OrdinalIgnoreCase);
        Assert.Contains("LIKE", endsSql, StringComparison.OrdinalIgnoreCase);
    }

    [Fact]
    public void ParameterizedFilter_UsesQueryParameterAndExecutes()
    {
        SeedData();

        using var context = CreateContext();
        var minId = 2;

        var query = context.Items
            .Where(x => x.Id >= minId)
            .OrderBy(x => x.Id);

        var sql = query.ToQueryString();
        Assert.Contains("@minId", sql, StringComparison.Ordinal);
        Assert.Contains("ORDER BY", sql, StringComparison.OrdinalIgnoreCase);

        var result = query.ToList();
        Assert.Equal(4, result.Count);
    }

    [Fact]
    public void LargeInList_ThrowsPredictableError()
    {
        SeedData();
        var ids = Enumerable.Range(1, 1001).ToArray();

        using var context = CreateContext();
        var query = context.Items.Where(x => ids.Contains(x.Id));

        var ex = Assert.Throws<InvalidOperationException>(() => query.ToQueryString());
        Assert.Contains("at most", ex.Message, StringComparison.OrdinalIgnoreCase);
    }

    [Fact]
    public void LikeEscaping_EscapesLiteralWildcards()
    {
        SeedData();

        using var context = CreateContext();
        var query = context.Items.Where(x => x.Name.Contains("%_"));
        var sql = query.ToQueryString();

        Assert.Contains("\\%", sql, StringComparison.Ordinal);
        Assert.Contains("\\_", sql, StringComparison.Ordinal);
    }

    private AppDbContext CreateContext()
    {
        var optionsBuilder = new DbContextOptionsBuilder<AppDbContext>();
        optionsBuilder.UseDecentDb($"Data Source={_dbPath}");
        return new AppDbContext(optionsBuilder.Options);
    }

    private void SeedData()
    {
        using var conn = new DecentDBConnection($"Data Source={_dbPath}");
        conn.Open();

        using var cmd = conn.CreateCommand();
        cmd.CommandText = "DROP TABLE IF EXISTS ef_items";
        cmd.ExecuteNonQuery();

        cmd.CommandText = "CREATE TABLE ef_items (id INTEGER PRIMARY KEY, name TEXT, is_active BOOLEAN)";
        cmd.ExecuteNonQuery();

        cmd.CommandText = "INSERT INTO ef_items (id, name, is_active) VALUES (1, 'alpha', TRUE)";
        cmd.ExecuteNonQuery();
        cmd.CommandText = "INSERT INTO ef_items (id, name, is_active) VALUES (2, 'beta', TRUE)";
        cmd.ExecuteNonQuery();
        cmd.CommandText = "INSERT INTO ef_items (id, name, is_active) VALUES (3, 'gamma', TRUE)";
        cmd.ExecuteNonQuery();
        cmd.CommandText = "INSERT INTO ef_items (id, name, is_active) VALUES (4, 'delta', TRUE)";
        cmd.ExecuteNonQuery();
        cmd.CommandText = "INSERT INTO ef_items (id, name, is_active) VALUES (5, 'raw%_name', TRUE)";
        cmd.ExecuteNonQuery();
    }

    private static void TryDelete(string path)
    {
        if (File.Exists(path))
        {
            File.Delete(path);
        }
    }

    private sealed class AppDbContext : DbContext
    {
        public AppDbContext(DbContextOptions<AppDbContext> options)
            : base(options)
        {
        }

        public DbSet<TestItem> Items => Set<TestItem>();

        protected override void OnModelCreating(ModelBuilder modelBuilder)
        {
            modelBuilder.Entity<TestItem>(entity =>
            {
                entity.ToTable("ef_items");
                entity.HasKey(x => x.Id);
                entity.Property(x => x.Id).HasColumnName("id");
                entity.Property(x => x.Name).HasColumnName("name");
                entity.Property(x => x.IsActive).HasColumnName("is_active");
            });
        }
    }

    private sealed class TestItem
    {
        public int Id { get; set; }
        public string Name { get; set; } = string.Empty;
        public bool IsActive { get; set; }
    }
}
