using Microsoft.EntityFrameworkCore;

namespace DecentDb.ShowCase;

internal sealed class FailureShowcaseDbContext(string dbPath) : DbContext
{
    public DbSet<FailureShowcaseUser> Users => Set<FailureShowcaseUser>();
    public DbSet<FailureShowcaseParent> Parents => Set<FailureShowcaseParent>();
    public DbSet<FailureShowcaseChild> Children => Set<FailureShowcaseChild>();
    public DbSet<FailureShowcaseDocument> Documents => Set<FailureShowcaseDocument>();

    protected override void OnConfiguring(DbContextOptionsBuilder optionsBuilder)
        => optionsBuilder.UseDecentDB($"Data Source={dbPath}");

    protected override void OnModelCreating(ModelBuilder modelBuilder)
    {
        modelBuilder.Entity<FailureShowcaseUser>(entity =>
        {
            entity.ToTable("failure_users");
            entity.HasIndex(x => x.Email).IsUnique();
            entity.Property(x => x.Email).IsRequired();
            entity.Property(x => x.DisplayName).IsRequired();
        });

        modelBuilder.Entity<FailureShowcaseParent>(entity =>
        {
            entity.ToTable("failure_parents");
            entity.Property(x => x.Name).IsRequired();
        });

        modelBuilder.Entity<FailureShowcaseChild>(entity =>
        {
            entity.ToTable("failure_children");
            entity.Property(x => x.Name).IsRequired();
            entity.HasOne(x => x.Parent)
                .WithMany(x => x.Children)
                .HasForeignKey(x => x.ParentId)
                .OnDelete(DeleteBehavior.Restrict);
        });

        modelBuilder.Entity<FailureShowcaseDocument>(entity =>
        {
            entity.ToTable("failure_documents");
            entity.Property(x => x.Title).IsRequired();
            entity.Property(x => x.Version).IsConcurrencyToken();
        });
    }
}

internal sealed class FailureShowcaseUser
{
    public int Id { get; set; }
    public string Email { get; set; } = string.Empty;
    public string DisplayName { get; set; } = string.Empty;
}

internal sealed class FailureShowcaseParent
{
    public int Id { get; set; }
    public string Name { get; set; } = string.Empty;
    public List<FailureShowcaseChild> Children { get; set; } = [];
}

internal sealed class FailureShowcaseChild
{
    public int Id { get; set; }
    public int ParentId { get; set; }
    public string Name { get; set; } = string.Empty;
    public FailureShowcaseParent? Parent { get; set; }
}

internal sealed class FailureShowcaseDocument
{
    public int Id { get; set; }
    public string Title { get; set; } = string.Empty;
    public int Version { get; set; }
}
