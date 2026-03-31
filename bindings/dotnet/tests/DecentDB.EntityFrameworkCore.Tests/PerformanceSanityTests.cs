using DecentDB.AdoNet;
using DecentDB.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore;
using Xunit;

namespace DecentDB.EntityFrameworkCore.Tests;

public sealed class PerformanceSanityTests : IDisposable
{
    private readonly string _dbPath = Path.Combine(Path.GetTempPath(), $"test_ef_performance_{Guid.NewGuid():N}.ddb");

    public void Dispose()
    {
        TryDelete(_dbPath);
        TryDelete(_dbPath + "-wal");
    }

    [Fact]
    public async Task AsNoTracking_DoesNotRetainEntriesInChangeTracker()
    {
        SeedData();

        await using var context = CreateContext();
        var tracked = await context.Products
            .Where(x => x.IsActive)
            .Take(3)
            .ToListAsync();
        Assert.NotEmpty(context.ChangeTracker.Entries());

        context.ChangeTracker.Clear();

        var untracked = await context.Products
            .AsNoTracking()
            .Where(x => x.IsActive)
            .Take(3)
            .ToListAsync();

        Assert.Equal(3, tracked.Count);
        Assert.Equal(3, untracked.Count);
        Assert.Empty(context.ChangeTracker.Entries());
    }

    [Fact]
    public async Task Include_AsSplitQuery_UsesMultipleCommands_AndLoadsExpectedRows()
    {
        SeedData();

        await using var context = CreateContext();
        var connection = Assert.IsType<DecentDBConnection>(context.Database.GetDbConnection());
        var executedSql = new List<string>();

        void HandleSqlExecuted(object? _, SqlExecutedEventArgs args) => executedSql.Add(args.Sql);

        connection.SqlExecuted += HandleSqlExecuted;
        try
        {
            var blogs = await context.Blogs
                .Include(x => x.Posts)
                .AsSplitQuery()
                .OrderBy(x => x.Id)
                .ToListAsync();

            Assert.Equal(2, blogs.Count);
            Assert.Equal(2, blogs[0].Posts.Count);
            Assert.Single(blogs[1].Posts);
        }
        finally
        {
            connection.SqlExecuted -= HandleSqlExecuted;
        }

        Assert.True(executedSql.Count >= 2);
    }

    [Fact]
    public async Task KeysetPagination_ProducesStableSecondPage()
    {
        SeedData();

        await using var context = CreateContext();
        var firstPage = await context.Products
            .OrderBy(x => x.Score)
            .ThenBy(x => x.Id)
            .Take(2)
            .Select(x => new { x.Id, x.Score })
            .ToListAsync();

        var last = firstPage[^1];

        var secondPage = await context.Products
            .OrderBy(x => x.Score)
            .ThenBy(x => x.Id)
            .Where(x => x.Score > last.Score || (x.Score == last.Score && x.Id > last.Id))
            .Take(2)
            .Select(x => x.Name)
            .ToListAsync();

        Assert.Equal(["bravo", "alpha"], secondPage);
    }

    [Fact]
    public async Task AsyncMaterialization_And_Streaming_PreserveSameOrder()
    {
        SeedData();

        await using var context = CreateContext();
        var list = await context.Products
            .OrderBy(x => x.Id)
            .Select(x => x.Name)
            .ToListAsync();

        var streamed = new List<string>();
        await foreach (var name in context.Products
                           .OrderBy(x => x.Id)
                           .Select(x => x.Name)
                           .AsAsyncEnumerable())
        {
            streamed.Add(name);
        }

        Assert.Equal(list, streamed);
    }

    [Fact]
    public async Task BulkMutationSanity_ReportsExpectedRowCounts()
    {
        SeedData();

        await using (var context = CreateContext())
        {
            var updated = await context.Products
                .Where(x => x.Category == "ops")
                .ExecuteUpdateAsync(setters => setters.SetProperty(x => x.Score, x => x.Score + 10));

            Assert.Equal(2, updated);
        }

        await using (var context = CreateContext())
        {
            var deleted = await context.Products
                .Where(x => x.Score < 100)
                .ExecuteDeleteAsync();

            Assert.Equal(1, deleted);
        }

        await using var verify = CreateContext();
        Assert.Equal(3, await verify.Products.CountAsync());
        Assert.Equal(130, await verify.Products.Where(x => x.Name == "alpha").Select(x => x.Score).SingleAsync());
    }

    private PerformanceDbContext CreateContext()
    {
        var options = new DbContextOptionsBuilder<PerformanceDbContext>()
            .UseDecentDB($"Data Source={_dbPath}")
            .Options;

        return new PerformanceDbContext(options);
    }

    private void SeedData()
    {
        TryDelete(_dbPath);
        TryDelete(_dbPath + "-wal");

        using var connection = new DecentDBConnection($"Data Source={_dbPath}");
        connection.Open();

        using var command = connection.CreateCommand();
        command.CommandText = """
            CREATE TABLE perf_products (
                id INTEGER PRIMARY KEY,
                name TEXT NOT NULL,
                category TEXT NOT NULL,
                score INTEGER NOT NULL,
                is_active BOOLEAN NOT NULL
            );
            CREATE TABLE perf_blogs (
                id INTEGER PRIMARY KEY,
                name TEXT NOT NULL
            );
            CREATE TABLE perf_posts (
                id INTEGER PRIMARY KEY,
                blog_id INTEGER NOT NULL,
                title TEXT NOT NULL
            );
            """;
        command.ExecuteNonQuery();

        command.CommandText = """
            INSERT INTO perf_products (id, name, category, score, is_active) VALUES
                (1, 'alpha', 'ops', 120, TRUE),
                (2, 'bravo', 'ops', 110, TRUE),
                (3, 'charlie', 'sales', 95, TRUE),
                (4, 'delta', 'sales', 105, TRUE);
            INSERT INTO perf_blogs (id, name) VALUES (1, 'blog-a'), (2, 'blog-b');
            INSERT INTO perf_posts (id, blog_id, title) VALUES
                (1, 1, 'a-1'),
                (2, 1, 'a-2'),
                (3, 2, 'b-1');
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

    private sealed class PerformanceDbContext : DbContext
    {
        public PerformanceDbContext(DbContextOptions<PerformanceDbContext> options)
            : base(options)
        {
        }

        public DbSet<PerfProduct> Products => Set<PerfProduct>();
        public DbSet<PerfBlog> Blogs => Set<PerfBlog>();
        public DbSet<PerfPost> Posts => Set<PerfPost>();

        protected override void OnModelCreating(ModelBuilder modelBuilder)
        {
            modelBuilder.Entity<PerfProduct>(entity =>
            {
                entity.ToTable("perf_products");
                entity.HasKey(x => x.Id);
                entity.Property(x => x.Id).HasColumnName("id");
                entity.Property(x => x.Name).HasColumnName("name");
                entity.Property(x => x.Category).HasColumnName("category");
                entity.Property(x => x.Score).HasColumnName("score");
                entity.Property(x => x.IsActive).HasColumnName("is_active");
            });

            modelBuilder.Entity<PerfBlog>(entity =>
            {
                entity.ToTable("perf_blogs");
                entity.HasKey(x => x.Id);
                entity.Property(x => x.Id).HasColumnName("id");
                entity.Property(x => x.Name).HasColumnName("name");
                entity.HasMany(x => x.Posts).WithOne(x => x.Blog).HasForeignKey(x => x.BlogId);
            });

            modelBuilder.Entity<PerfPost>(entity =>
            {
                entity.ToTable("perf_posts");
                entity.HasKey(x => x.Id);
                entity.Property(x => x.Id).HasColumnName("id");
                entity.Property(x => x.BlogId).HasColumnName("blog_id");
                entity.Property(x => x.Title).HasColumnName("title");
            });
        }
    }

    private sealed class PerfProduct
    {
        public int Id { get; set; }
        public string Name { get; set; } = string.Empty;
        public string Category { get; set; } = string.Empty;
        public int Score { get; set; }
        public bool IsActive { get; set; }
    }

    private sealed class PerfBlog
    {
        public int Id { get; set; }
        public string Name { get; set; } = string.Empty;
        public List<PerfPost> Posts { get; set; } = [];
    }

    private sealed class PerfPost
    {
        public int Id { get; set; }
        public int BlogId { get; set; }
        public string Title { get; set; } = string.Empty;
        public PerfBlog? Blog { get; set; }
    }
}
