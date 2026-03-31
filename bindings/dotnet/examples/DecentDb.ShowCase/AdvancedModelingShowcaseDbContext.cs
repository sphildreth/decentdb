using System.ComponentModel.DataAnnotations;
using DecentDB.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore;

namespace DecentDb.ShowCase;

internal sealed class AdvancedModelingShowcaseDbContext : DbContext
{
    private readonly string _dbPath;

    public AdvancedModelingShowcaseDbContext(string dbPath)
    {
        _dbPath = dbPath;
    }

    public AdvancedModelingShowcaseDbContext(DbContextOptions<AdvancedModelingShowcaseDbContext> options)
        : base(options)
    {
        _dbPath = string.Empty;
    }

    public DbSet<AdvancedModelingShowcaseCustomer> Customers => Set<AdvancedModelingShowcaseCustomer>();
    public DbSet<AdvancedModelingShowcaseContentItem> ContentItems => Set<AdvancedModelingShowcaseContentItem>();
    public DbSet<AdvancedModelingShowcaseArticle> Articles => Set<AdvancedModelingShowcaseArticle>();
    public DbSet<AdvancedModelingShowcaseVideo> Videos => Set<AdvancedModelingShowcaseVideo>();
    public DbSet<AdvancedModelingShowcaseTag> Tags => Set<AdvancedModelingShowcaseTag>();
    public DbSet<AdvancedModelingShowcaseProjection> ContentProjections => Set<AdvancedModelingShowcaseProjection>();
    public DbSet<AdvancedModelingShowcaseDocument> Documents => Set<AdvancedModelingShowcaseDocument>();

    protected override void OnConfiguring(DbContextOptionsBuilder optionsBuilder)
    {
        if (!optionsBuilder.IsConfigured)
        {
            optionsBuilder.UseDecentDB($"Data Source={_dbPath}");
        }
    }

    protected override void OnModelCreating(ModelBuilder modelBuilder)
    {
        modelBuilder.Entity<AdvancedModelingShowcaseCustomer>(entity =>
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

        modelBuilder.Entity<AdvancedModelingShowcaseContentItem>(entity =>
        {
            entity.ToTable("adv_content_items");
            entity.HasKey(x => x.Id);
            entity.Property(x => x.Id).HasColumnName("id").ValueGeneratedOnAdd();
            entity.Property(x => x.Title).HasColumnName("title");
            entity.Property<int>("published_order").HasColumnName("published_order");
            entity.Property<int>("etag").HasColumnName("etag").IsConcurrencyToken();

            entity.HasDiscriminator<string>("content_kind")
                .HasValue<AdvancedModelingShowcaseArticle>("article")
                .HasValue<AdvancedModelingShowcaseVideo>("video");

            entity.HasMany(x => x.Tags)
                .WithMany(x => x.Items)
                .UsingEntity<Dictionary<string, object>>(
                    "AdvancedContentTag",
                    right => right
                        .HasOne<AdvancedModelingShowcaseTag>()
                        .WithMany()
                        .HasForeignKey("tag_id"),
                    left => left
                        .HasOne<AdvancedModelingShowcaseContentItem>()
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

        modelBuilder.Entity<AdvancedModelingShowcaseArticle>(entity =>
        {
            entity.Property(x => x.WordCount).HasColumnName("word_count");
        });

        modelBuilder.Entity<AdvancedModelingShowcaseVideo>(entity =>
        {
            entity.Property(x => x.DurationSeconds).HasColumnName("duration_seconds");
        });

        modelBuilder.Entity<AdvancedModelingShowcaseTag>(entity =>
        {
            entity.ToTable("adv_tags");
            entity.HasKey(x => x.Id);
            entity.Property(x => x.Id).HasColumnName("id").ValueGeneratedOnAdd();
            entity.Property(x => x.Name).HasColumnName("name");
        });

        modelBuilder.Entity<AdvancedModelingShowcaseProjection>(entity =>
        {
            entity.HasNoKey();
        });

        modelBuilder.Entity<AdvancedModelingShowcaseDocument>(entity =>
        {
            entity.ToTable("adv_documents");
            entity.HasKey(x => x.Id);
            entity.Property(x => x.Id).HasColumnName("id").ValueGeneratedOnAdd();
            entity.Property(x => x.Title).HasColumnName("title");
            entity.Property(x => x.Revision).HasColumnName("revision");
        });
    }
}

internal sealed class AdvancedModelingShowcaseCustomer
{
    public long Id { get; set; }
    public string Name { get; set; } = string.Empty;
    public AdvancedModelingShowcaseProfile Profile { get; set; } = new();
}

internal sealed class AdvancedModelingShowcaseProfile
{
    public string Email { get; set; } = string.Empty;
    public string City { get; set; } = string.Empty;
}

internal abstract class AdvancedModelingShowcaseContentItem
{
    public long Id { get; set; }
    public string Title { get; set; } = string.Empty;
    public List<AdvancedModelingShowcaseTag> Tags { get; set; } = [];
}

internal sealed class AdvancedModelingShowcaseArticle : AdvancedModelingShowcaseContentItem
{
    public int WordCount { get; set; }
}

internal sealed class AdvancedModelingShowcaseVideo : AdvancedModelingShowcaseContentItem
{
    public int DurationSeconds { get; set; }
}

internal sealed class AdvancedModelingShowcaseTag
{
    public int Id { get; set; }
    public string Name { get; set; } = string.Empty;
    public List<AdvancedModelingShowcaseContentItem> Items { get; set; } = [];
}

internal sealed class AdvancedModelingShowcaseProjection
{
    public string Title { get; set; } = string.Empty;
    public string ContentKind { get; set; } = string.Empty;
}

internal sealed class AdvancedModelingShowcaseDocument
{
    public int Id { get; set; }
    public string Title { get; set; } = string.Empty;

    [ConcurrencyCheck]
    public int Revision { get; set; }
}
