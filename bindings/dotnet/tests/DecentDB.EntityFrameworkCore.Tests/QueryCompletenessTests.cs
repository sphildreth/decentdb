using DecentDB.AdoNet;
using DecentDB.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore;
using Xunit;

namespace DecentDB.EntityFrameworkCore.Tests;

public sealed class QueryCompletenessTests : IDisposable
{
    private readonly string _dbPath = Path.Combine(Path.GetTempPath(), $"test_ef_query_completeness_{Guid.NewGuid():N}.ddb");

    public void Dispose()
    {
        TryDelete(_dbPath);
        TryDelete(_dbPath + "-wal");
    }

    [Fact]
    public void SetOperations_TranslateAndExecute()
    {
        SeedData();

        using var context = CreateContext();
        var expensive = context.QueryProducts
            .Where(x => x.Score >= 100)
            .Select(x => x.Name);
        var featured = context.QueryProducts
            .Where(x => x.IsFeatured)
            .Select(x => x.Name);

        var unionQuery = expensive.Union(featured).OrderBy(x => x);
        var concatQuery = expensive.Concat(featured);
        var intersectQuery = expensive.Intersect(featured).OrderBy(x => x);
        var exceptQuery = expensive.Except(featured).OrderBy(x => x);

        Assert.Contains("UNION", unionQuery.ToQueryString(), StringComparison.OrdinalIgnoreCase);
        Assert.Contains("UNION ALL", concatQuery.ToQueryString(), StringComparison.OrdinalIgnoreCase);
        Assert.Contains("INTERSECT", intersectQuery.ToQueryString(), StringComparison.OrdinalIgnoreCase);
        Assert.Contains("EXCEPT", exceptQuery.ToQueryString(), StringComparison.OrdinalIgnoreCase);

        Assert.Equal(["alpha", "bravo", "charlie", "delta"], unionQuery.ToList());

        var concatRows = concatQuery.ToList();
        Assert.Equal(5, concatRows.Count);
        Assert.Equal(2, concatRows.Count(x => x == "alpha"));
        Assert.Contains("bravo", concatRows);
        Assert.Contains("charlie", concatRows);
        Assert.Contains("delta", concatRows);

        Assert.Equal(["alpha"], intersectQuery.ToList());
        Assert.Equal(["bravo", "delta"], exceptQuery.ToList());
    }

    [Fact]
    public async Task ExecuteUpdateAndDeleteAsync_ReportAffectedRows_AndPersistResults()
    {
        SeedData();

        await using (var context = CreateContext())
        {
            var updated = await context.QueryProducts
                .Where(x => x.Category == "ops")
                .ExecuteUpdateAsync(setters => setters
                    .SetProperty(x => x.Score, x => x.Score + 5)
                    .SetProperty(x => x.IsArchived, true));

            Assert.Equal(2, updated);
        }

        await using (var verifyUpdate = CreateContext())
        {
            var opsRows = await verifyUpdate.QueryProducts
                .Where(x => x.Category == "ops")
                .OrderBy(x => x.Id)
                .ToListAsync();

            Assert.Equal([125, 110], opsRows.Select(x => x.Score).ToArray());
            Assert.All(opsRows, row => Assert.True(row.IsArchived));
        }

        await using (var deleteContext = CreateContext())
        {
            var deleted = await deleteContext.QueryProducts
                .Where(x => x.IsArchived)
                .ExecuteDeleteAsync();

            Assert.Equal(3, deleted);
        }

        await using var verifyDelete = CreateContext();
        var remaining = await verifyDelete.QueryProducts
            .OrderBy(x => x.Id)
            .Select(x => x.Name)
            .ToListAsync();

        Assert.Equal(["charlie"], remaining);
    }

    [Fact]
    public async Task AsAsyncEnumerable_StreamsRowsInStableQueryOrder()
    {
        SeedData();

        await using var context = CreateContext();
        var streamed = new List<string>();

        await foreach (var name in context.QueryProducts
                           .OrderBy(x => x.Id)
                           .Select(x => x.Name)
                           .AsAsyncEnumerable())
        {
            streamed.Add(name);
        }

        Assert.Equal(["alpha", "bravo", "charlie", "delta"], streamed);
    }

    private QueryCompletenessDbContext CreateContext()
    {
        var options = new DbContextOptionsBuilder<QueryCompletenessDbContext>()
            .UseDecentDB($"Data Source={_dbPath}")
            .Options;

        return new QueryCompletenessDbContext(options);
    }

    private void SeedData()
    {
        TryDelete(_dbPath);
        TryDelete(_dbPath + "-wal");

        using var connection = new DecentDBConnection($"Data Source={_dbPath}");
        connection.Open();

        using var command = connection.CreateCommand();
        command.CommandText = """
            CREATE TABLE query_products (
                id INTEGER PRIMARY KEY,
                name TEXT NOT NULL,
                category TEXT NOT NULL,
                score INTEGER NOT NULL,
                is_featured BOOLEAN NOT NULL,
                is_archived BOOLEAN NOT NULL
            )
            """;
        command.ExecuteNonQuery();

        command.CommandText = """
            INSERT INTO query_products (id, name, category, score, is_featured, is_archived) VALUES
                (1, 'alpha', 'ops', 120, TRUE, FALSE),
                (2, 'bravo', 'ops', 105, FALSE, FALSE),
                (3, 'charlie', 'sales', 95, TRUE, FALSE),
                (4, 'delta', 'sales', 140, FALSE, TRUE)
            """;
        command.ExecuteNonQuery();
    }

    private static void TryDelete(string path)
    {
        if (File.Exists(path))
        {
            File.Delete(path);
        }
    }

    private sealed class QueryCompletenessDbContext : DbContext
    {
        public QueryCompletenessDbContext(DbContextOptions<QueryCompletenessDbContext> options)
            : base(options)
        {
        }

        public DbSet<QueryProduct> QueryProducts => Set<QueryProduct>();

        protected override void OnModelCreating(ModelBuilder modelBuilder)
        {
            modelBuilder.Entity<QueryProduct>(entity =>
            {
                entity.ToTable("query_products");
                entity.HasKey(x => x.Id);
                entity.Property(x => x.Id).HasColumnName("id");
                entity.Property(x => x.Name).HasColumnName("name");
                entity.Property(x => x.Category).HasColumnName("category");
                entity.Property(x => x.Score).HasColumnName("score");
                entity.Property(x => x.IsFeatured).HasColumnName("is_featured");
                entity.Property(x => x.IsArchived).HasColumnName("is_archived");
            });
        }
    }

    private sealed class QueryProduct
    {
        public long Id { get; set; }
        public string Name { get; set; } = string.Empty;
        public string Category { get; set; } = string.Empty;
        public int Score { get; set; }
        public bool IsFeatured { get; set; }
        public bool IsArchived { get; set; }
    }
}
