using System.ComponentModel.DataAnnotations;
using DecentDB.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore;
using Xunit;

namespace DecentDB.EntityFrameworkCore.Tests;

public sealed class AdvancedModelingTests : IDisposable
{
    private readonly string _dbPath = Path.Combine(Path.GetTempPath(), $"test_ef_advanced_modeling_{Guid.NewGuid():N}.ddb");

    public void Dispose()
    {
        TryDelete(_dbPath);
        TryDelete(_dbPath + "-wal");
    }

    [Fact]
    public void OwnedTypes_AndShadowProperties_Work()
    {
        using var context = CreateContext();
        context.Database.EnsureDeleted();
        context.Database.EnsureCreated();

        var customer = new AdvancedCustomer
        {
            Name = "Ada Lovelace",
            Profile = new CustomerProfile
            {
                Email = "ada@example.com",
                City = "London"
            }
        };

        context.Customers.Add(customer);
        context.Entry(customer).Property("priority").CurrentValue = 7;
        context.SaveChanges();

        var loaded = context.Customers
            .Where(x => x.Profile.City == "London" && EF.Property<int>(x, "priority") == 7)
            .Single();

        Assert.Equal("ada@example.com", loaded.Profile.Email);
        Assert.Equal(7, context.Entry(loaded).Property<int>("priority").CurrentValue);
    }

    [Fact]
    public void Inheritance_SkipNavigations_AndKeylessRawSql_Work()
    {
        using var context = CreateContext();
        context.Database.EnsureDeleted();
        context.Database.EnsureCreated();

        var efTag = new AdvancedTag { Name = "efcore" };
        var dbTag = new AdvancedTag { Name = "database" };
        var videoTag = new AdvancedTag { Name = "video" };

        var article = new AdvancedArticle
        {
            Title = "EF Modeling",
            WordCount = 1200,
            Tags = [efTag, dbTag]
        };

        var video = new AdvancedVideo
        {
            Title = "DecentDB Deep Dive",
            DurationSeconds = 600,
            Tags = [dbTag, videoTag]
        };

        context.ContentItems.AddRange(article, video);
        context.Entry(article).Property("published_order").CurrentValue = 1;
        context.Entry(video).Property("published_order").CurrentValue = 2;
        context.Entry(article).Property("etag").CurrentValue = 1;
        context.Entry(video).Property("etag").CurrentValue = 1;
        context.SaveChanges();

        var titles = context.ContentItems
            .OrderBy(x => EF.Property<int>(x, "published_order"))
            .Select(x => x.Title)
            .ToList();
        Assert.Equal(["EF Modeling", "DecentDB Deep Dive"], titles);

        var loadedArticle = context.Set<AdvancedArticle>()
            .Include(x => x.Tags)
            .Single();
        Assert.Equal(1200, loadedArticle.WordCount);
        Assert.Equal(2, loadedArticle.Tags.Count);

        var loadedVideo = context.Set<AdvancedVideo>().Single();
        Assert.Equal(600, loadedVideo.DurationSeconds);

        var projections = context.ContentProjections
            .FromSqlRaw("""
                        SELECT "title" AS "Title", "content_kind" AS "ContentKind"
                        FROM "adv_content_items"
                        ORDER BY "title"
                        """)
            .ToList();

        Assert.Equal(2, projections.Count);
        Assert.Contains(projections, x => x.Title == "DecentDB Deep Dive" && x.ContentKind == "video");
        Assert.Contains(projections, x => x.Title == "EF Modeling" && x.ContentKind == "article");
    }

    [Fact]
    public void ConcurrencyTokenPatterns_AreConfigured_ForExplicitAndShadowCases()
    {
        using var context = CreateContext();

        var documentEntity = context.Model.FindEntityType(typeof(AdvancedDocument));
        Assert.NotNull(documentEntity);
        Assert.True(documentEntity!.FindProperty(nameof(AdvancedDocument.Revision))!.IsConcurrencyToken);

        var contentEntity = context.Model.FindEntityType(typeof(AdvancedContentItem));
        Assert.NotNull(contentEntity);
        Assert.True(contentEntity!.FindProperty("etag")!.IsConcurrencyToken);
    }

    private AdvancedModelingDbContext CreateContext()
    {
        var optionsBuilder = new DbContextOptionsBuilder<AdvancedModelingDbContext>();
        optionsBuilder.UseDecentDB($"Data Source={_dbPath}");
        return new AdvancedModelingDbContext(optionsBuilder.Options);
    }

    private static void TryDelete(string path)
    {
        if (File.Exists(path))
        {
            File.Delete(path);
        }
    }
}

public sealed class AdvancedModelingDbContext : DbContext
{
    public AdvancedModelingDbContext(DbContextOptions<AdvancedModelingDbContext> options)
        : base(options)
    {
    }

    public DbSet<AdvancedCustomer> Customers => Set<AdvancedCustomer>();
    public DbSet<AdvancedContentItem> ContentItems => Set<AdvancedContentItem>();
    public DbSet<AdvancedArticle> Articles => Set<AdvancedArticle>();
    public DbSet<AdvancedVideo> Videos => Set<AdvancedVideo>();
    public DbSet<AdvancedTag> Tags => Set<AdvancedTag>();
    public DbSet<AdvancedContentProjection> ContentProjections => Set<AdvancedContentProjection>();
    public DbSet<AdvancedDocument> Documents => Set<AdvancedDocument>();

    protected override void OnModelCreating(ModelBuilder modelBuilder)
    {
        modelBuilder.Entity<AdvancedCustomer>(entity =>
        {
            entity.ToTable("adv_customers");
            entity.HasKey(x => x.Id);
            entity.Property(x => x.Id).HasColumnName("id").ValueGeneratedOnAdd();
            entity.Property(x => x.Name).HasColumnName("name");
            entity.Property<int>("priority").HasColumnName("priority");

            entity.OwnsOne(x => x.Profile, owned =>
            {
                owned.Property(x => x.Email).HasColumnName("profile_email");
                owned.Property(x => x.City).HasColumnName("profile_city");
            });
        });

        modelBuilder.Entity<AdvancedContentItem>(entity =>
        {
            entity.ToTable("adv_content_items");
            entity.HasKey(x => x.Id);
            entity.Property(x => x.Id).HasColumnName("id").ValueGeneratedOnAdd();
            entity.Property(x => x.Title).HasColumnName("title");
            entity.Property<int>("published_order").HasColumnName("published_order");
            entity.Property<int>("etag").HasColumnName("etag").IsConcurrencyToken();

            entity.HasDiscriminator<string>("content_kind")
                .HasValue<AdvancedArticle>("article")
                .HasValue<AdvancedVideo>("video");

            entity.HasMany(x => x.Tags)
                .WithMany(x => x.Items)
                .UsingEntity<Dictionary<string, object>>(
                    "AdvancedContentTag",
                    right => right
                        .HasOne<AdvancedTag>()
                        .WithMany()
                        .HasForeignKey("tag_id"),
                    left => left
                        .HasOne<AdvancedContentItem>()
                        .WithMany()
                        .HasForeignKey("content_item_id"),
                    join =>
                    {
                        join.ToTable("adv_content_item_tags");
                        join.HasKey("content_item_id", "tag_id");
                        join.IndexerProperty<long>("content_item_id").HasColumnName("content_item_id");
                        join.IndexerProperty<int>("tag_id").HasColumnName("tag_id");
                    });
        });

        modelBuilder.Entity<AdvancedArticle>(entity =>
        {
            entity.Property(x => x.WordCount).HasColumnName("word_count");
        });

        modelBuilder.Entity<AdvancedVideo>(entity =>
        {
            entity.Property(x => x.DurationSeconds).HasColumnName("duration_seconds");
        });

        modelBuilder.Entity<AdvancedTag>(entity =>
        {
            entity.ToTable("adv_tags");
            entity.HasKey(x => x.Id);
            entity.Property(x => x.Id).HasColumnName("id").ValueGeneratedOnAdd();
            entity.Property(x => x.Name).HasColumnName("name");
        });

        modelBuilder.Entity<AdvancedContentProjection>(entity =>
        {
            entity.HasNoKey();
        });

        modelBuilder.Entity<AdvancedDocument>(entity =>
        {
            entity.ToTable("adv_documents");
            entity.HasKey(x => x.Id);
            entity.Property(x => x.Id).HasColumnName("id").ValueGeneratedOnAdd();
            entity.Property(x => x.Title).HasColumnName("title");
            entity.Property(x => x.Revision).HasColumnName("revision");
        });
    }
}

public sealed class AdvancedCustomer
{
    public long Id { get; set; }
    public string Name { get; set; } = string.Empty;
    public CustomerProfile Profile { get; set; } = new();
}

public sealed class CustomerProfile
{
    public string Email { get; set; } = string.Empty;
    public string City { get; set; } = string.Empty;
}

public abstract class AdvancedContentItem
{
    public long Id { get; set; }
    public string Title { get; set; } = string.Empty;
    public List<AdvancedTag> Tags { get; set; } = [];
}

public sealed class AdvancedArticle : AdvancedContentItem
{
    public int WordCount { get; set; }
}

public sealed class AdvancedVideo : AdvancedContentItem
{
    public int DurationSeconds { get; set; }
}

public sealed class AdvancedTag
{
    public int Id { get; set; }
    public string Name { get; set; } = string.Empty;
    public List<AdvancedContentItem> Items { get; set; } = [];
}

public sealed class AdvancedContentProjection
{
    public string Title { get; set; } = string.Empty;
    public string ContentKind { get; set; } = string.Empty;
}

public sealed class AdvancedDocument
{
    public int Id { get; set; }
    public string Title { get; set; } = string.Empty;

    [ConcurrencyCheck]
    public int Revision { get; set; }
}
