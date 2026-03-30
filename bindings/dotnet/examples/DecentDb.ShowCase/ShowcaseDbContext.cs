using DecentDb.ShowCase.Entities;
using DecentDB.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore;
using NodaTime;

namespace DecentDb.ShowCase;

public class ShowcaseDbContext : DbContext
{
    public DbSet<Product> Products => Set<Product>();
    public DbSet<Category> Categories => Set<Category>();
    public DbSet<Order> Orders => Set<Order>();
    public DbSet<OrderItem> OrderItems => Set<OrderItem>();
    public DbSet<Customer> Customers => Set<Customer>();
    public DbSet<Address> Addresses => Set<Address>();
    public DbSet<Tag> Tags => Set<Tag>();
    public DbSet<ProductTag> ProductTags => Set<ProductTag>();
    public DbSet<Employee> Employees => Set<Employee>();
    public DbSet<AppEventLog> EventLogs => Set<AppEventLog>();
    public DbSet<ScheduleEntry> ScheduleEntries => Set<ScheduleEntry>();

    private readonly string _dbPath;

    public ShowcaseDbContext(string dbPath)
    {
        _dbPath = dbPath;
    }

    public ShowcaseDbContext(DbContextOptions<ShowcaseDbContext> options) : base(options)
    {
        _dbPath = string.Empty;
    }

    protected override void OnConfiguring(DbContextOptionsBuilder optionsBuilder)
    {
        if (!optionsBuilder.IsConfigured)
        {
            var connectionString = $"Data Source={_dbPath}";
            optionsBuilder.UseDecentDB(connectionString, builder =>
            {
                builder.UseNodaTime();
            });
        }
    }

    protected override void OnModelCreating(ModelBuilder modelBuilder)
    {
        base.OnModelCreating(modelBuilder);

        modelBuilder.Entity<Product>(entity =>
        {
            entity.HasIndex(p => p.Name);
            entity.HasIndex(p => p.Price);
            entity.HasIndex(p => new { p.IsActive, p.CategoryId });
        });

        modelBuilder.Entity<Category>(entity =>
        {
            entity.HasIndex(c => c.Name).IsUnique();
            entity.HasIndex(c => c.DisplayOrder);
        });

        modelBuilder.Entity<Order>(entity =>
        {
            entity.HasIndex(o => o.OrderNumber).IsUnique();
            entity.HasIndex(o => o.OrderDate);
            entity.HasIndex(o => o.CustomerId);
            entity.HasIndex(o => o.Status);
        });

        modelBuilder.Entity<OrderItem>(entity =>
        {
            entity.HasIndex(oi => new { oi.OrderId, oi.ProductId });
        });

        modelBuilder.Entity<Customer>(entity =>
        {
            entity.HasIndex(c => c.Email).IsUnique();
            entity.HasIndex(c => c.IsPremium);
        });

        modelBuilder.Entity<ProductTag>(entity =>
        {
            entity.HasKey(pt => new { pt.ProductId, pt.TagId });
        });

        modelBuilder.Entity<Employee>(entity =>
        {
        });

        modelBuilder.Entity<AppEventLog>(entity =>
        {
            entity.HasIndex(e => e.Timestamp);
            entity.HasIndex(e => e.Level);
        });

        modelBuilder.Entity<ScheduleEntry>(entity =>
        {
            entity.HasIndex(e => e.ScheduledDate);
            entity.HasIndex(e => e.ScheduledInstant);
            entity.HasIndex(e => new { e.IsCompleted, e.Priority });
        });
    }
}
