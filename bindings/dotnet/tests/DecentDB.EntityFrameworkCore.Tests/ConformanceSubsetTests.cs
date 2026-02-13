using DecentDB.AdoNet;
using DecentDB.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore;
using Xunit;

namespace DecentDB.EntityFrameworkCore.Tests;

public sealed class ConformanceSubsetTests : IDisposable
{
    private readonly string _dbPath = Path.Combine(Path.GetTempPath(), $"test_ef_conformance_{Guid.NewGuid():N}.ddb");

    public void Dispose()
    {
        TryDelete(_dbPath);
        TryDelete(_dbPath + "-wal");
    }

    [Fact]
    public void Include_AsSplitQuery_LoadsRelatedRows()
    {
        SeedData();

        using var context = CreateContext();
        var blogs = context.Blogs
            .Include(x => x.Posts)
            .AsSplitQuery()
            .OrderBy(x => x.Id)
            .ToList();

        Assert.Equal(2, blogs.Count);
        Assert.Equal(2, blogs[0].Posts.Count);
        Assert.Single(blogs[1].Posts);
    }

    [Fact]
    public void FromSqlRaw_WithParameters_ExecutesCorrectly()
    {
        SeedData();

        using var context = CreateContext();
        var result = context.Blogs
            .FromSqlRaw("SELECT id, name FROM ef_blogs WHERE name = @name", new DecentDBParameter("@name", "blog-a"))
            .AsNoTracking()
            .ToList();

        Assert.Single(result);
        Assert.Equal("blog-a", result[0].Name);
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

        cmd.CommandText = "DROP TABLE IF EXISTS ef_posts";
        cmd.ExecuteNonQuery();
        cmd.CommandText = "DROP TABLE IF EXISTS ef_blogs";
        cmd.ExecuteNonQuery();

        cmd.CommandText = "CREATE TABLE ef_blogs (id INTEGER PRIMARY KEY, name TEXT NOT NULL)";
        cmd.ExecuteNonQuery();
        cmd.CommandText = "CREATE TABLE ef_posts (id INTEGER PRIMARY KEY, blog_id INTEGER NOT NULL, title TEXT NOT NULL)";
        cmd.ExecuteNonQuery();

        cmd.CommandText = "INSERT INTO ef_blogs (id, name) VALUES (1, 'blog-a')";
        cmd.ExecuteNonQuery();
        cmd.CommandText = "INSERT INTO ef_blogs (id, name) VALUES (2, 'blog-b')";
        cmd.ExecuteNonQuery();

        cmd.CommandText = "INSERT INTO ef_posts (id, blog_id, title) VALUES (1, 1, 'a-1')";
        cmd.ExecuteNonQuery();
        cmd.CommandText = "INSERT INTO ef_posts (id, blog_id, title) VALUES (2, 1, 'a-2')";
        cmd.ExecuteNonQuery();
        cmd.CommandText = "INSERT INTO ef_posts (id, blog_id, title) VALUES (3, 2, 'b-1')";
        cmd.ExecuteNonQuery();
    }

    private static void TryDelete(string path)
    {
        if (File.Exists(path))
        {
            File.Delete(path);
        }
    }
}

public sealed class AppDbContext : DbContext
{
    public AppDbContext(DbContextOptions<AppDbContext> options)
        : base(options)
    {
    }

    public DbSet<Blog> Blogs => Set<Blog>();
    public DbSet<Post> Posts => Set<Post>();

    protected override void OnModelCreating(ModelBuilder modelBuilder)
    {
        modelBuilder.Entity<Blog>(entity =>
        {
            entity.ToTable("ef_blogs");
            entity.HasKey(x => x.Id);
            entity.Property(x => x.Id).HasColumnName("id");
            entity.Property(x => x.Name).HasColumnName("name");
            entity.HasMany(x => x.Posts).WithOne(x => x.Blog).HasForeignKey(x => x.BlogId);
        });

        modelBuilder.Entity<Post>(entity =>
        {
            entity.ToTable("ef_posts");
            entity.HasKey(x => x.Id);
            entity.Property(x => x.Id).HasColumnName("id");
            entity.Property(x => x.BlogId).HasColumnName("blog_id");
            entity.Property(x => x.Title).HasColumnName("title");
        });
    }
}

public sealed class Blog
{
    public int Id { get; set; }
    public string Name { get; set; } = string.Empty;
    public List<Post> Posts { get; set; } = new();
}

public sealed class Post
{
    public int Id { get; set; }
    public int BlogId { get; set; }
    public string Title { get; set; } = string.Empty;
    public Blog? Blog { get; set; }
}
