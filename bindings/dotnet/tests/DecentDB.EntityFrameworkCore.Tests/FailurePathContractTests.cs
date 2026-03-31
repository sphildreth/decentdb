using DecentDB.EntityFrameworkCore;
using DecentDB.Native;
using Microsoft.EntityFrameworkCore;
using Xunit;

namespace DecentDB.EntityFrameworkCore.Tests;

public sealed class FailurePathContractTests : IDisposable
{
    private readonly string _dbPath = Path.Combine(Path.GetTempPath(), $"test_ef_failure_{Guid.NewGuid():N}.ddb");

    public void Dispose()
    {
        TryDelete(_dbPath);
        TryDelete(_dbPath + "-wal");
    }

    [Fact]
    public void UniqueViolation_ThrowsDbUpdateException_WithNativeInnerException()
    {
        EnsureSchema();

        using (var seed = CreateContext())
        {
            seed.Users.Add(new FailureUser { Email = "ada@example.com", DisplayName = "Ada" });
            seed.SaveChanges();
        }

        using var context = CreateContext();
        context.Users.Add(new FailureUser { Email = "ada@example.com", DisplayName = "Duplicate Ada" });

        var ex = Assert.Throws<DbUpdateException>(() => context.SaveChanges());
        var inner = Assert.IsType<DecentDBException>(ex.InnerException);
        Assert.NotEqual(0, inner.ErrorCode);
        Assert.False(string.IsNullOrWhiteSpace(inner.Message));
    }

    [Fact]
    public void ForeignKeyViolation_ThrowsDbUpdateException_WithNativeInnerException()
    {
        EnsureSchema();

        using var context = CreateContext();
        context.Children.Add(new FailureChild { ParentId = 999, Name = "orphan" });

        var ex = Assert.Throws<DbUpdateException>(() => context.SaveChanges());
        var inner = Assert.IsType<DecentDBException>(ex.InnerException);
        Assert.NotEqual(0, inner.ErrorCode);
        Assert.False(string.IsNullOrWhiteSpace(inner.Message));
    }

    [Fact]
    public void CheckConstraintViolation_ThrowsDbUpdateException_WithNativeInnerException()
    {
        EnsureSchema();

        using var context = CreateContext();
        context.StockChecks.Add(new FailureStockCheck { Quantity = -1 });

        var ex = Assert.Throws<DbUpdateException>(() => context.SaveChanges());
        var inner = Assert.IsType<DecentDBException>(ex.InnerException);
        Assert.NotEqual(0, inner.ErrorCode);
        Assert.False(string.IsNullOrWhiteSpace(inner.Message));
    }

    [Fact]
    public void FailedSaveChanges_WithinTransaction_CanBeRolledBack()
    {
        EnsureSchema();

        using (var context = CreateContext())
        using (var tx = context.Database.BeginTransaction())
        {
            context.Users.Add(new FailureUser { Email = "ada@example.com", DisplayName = "Ada" });
            context.SaveChanges();

            context.Users.Add(new FailureUser { Email = "ada@example.com", DisplayName = "Duplicate Ada" });
            var ex = Assert.Throws<DbUpdateException>(() => context.SaveChanges());
            Assert.IsType<DecentDBException>(ex.InnerException);

            tx.Rollback();
        }

        using var verify = CreateContext();
        Assert.Equal(0, verify.Users.Count());
    }

    private FailureDbContext CreateContext()
    {
        var optionsBuilder = new DbContextOptionsBuilder<FailureDbContext>();
        optionsBuilder.UseDecentDB($"Data Source={_dbPath}");
        optionsBuilder.EnableDetailedErrors();
        return new FailureDbContext(optionsBuilder.Options);
    }

    private void EnsureSchema()
    {
        TryDelete(_dbPath);
        TryDelete(_dbPath + "-wal");

        using var conn = new DecentDB.AdoNet.DecentDBConnection($"Data Source={_dbPath}");
        conn.Open();
        using var cmd = conn.CreateCommand();
        cmd.CommandText = """
                          DROP TABLE IF EXISTS failure_stock_checks;
                          DROP TABLE IF EXISTS failure_children;
                          DROP TABLE IF EXISTS failure_parents;
                          DROP TABLE IF EXISTS failure_users;
                          CREATE TABLE failure_users (
                              id INTEGER PRIMARY KEY,
                              email TEXT NOT NULL UNIQUE,
                              display_name TEXT NOT NULL
                          );
                          CREATE TABLE failure_parents (
                              id INTEGER PRIMARY KEY,
                              name TEXT NOT NULL
                          );
                          CREATE TABLE failure_children (
                              id INTEGER PRIMARY KEY,
                              parent_id INTEGER NOT NULL,
                              name TEXT NOT NULL,
                              FOREIGN KEY (parent_id) REFERENCES failure_parents (id) ON DELETE RESTRICT
                          );
                          CREATE TABLE failure_stock_checks (
                              id INTEGER PRIMARY KEY,
                              quantity INTEGER NOT NULL CHECK (quantity >= 0)
                          );
                          """;
        cmd.ExecuteNonQuery();
    }

    private static void TryDelete(string path)
    {
        if (File.Exists(path))
        {
            File.Delete(path);
        }
    }

    private sealed class FailureDbContext(DbContextOptions<FailureDbContext> options) : DbContext(options)
    {
        public DbSet<FailureUser> Users => Set<FailureUser>();
        public DbSet<FailureParent> Parents => Set<FailureParent>();
        public DbSet<FailureChild> Children => Set<FailureChild>();
        public DbSet<FailureStockCheck> StockChecks => Set<FailureStockCheck>();

        protected override void OnModelCreating(ModelBuilder modelBuilder)
        {
            modelBuilder.Entity<FailureUser>(entity =>
            {
                entity.ToTable("failure_users");
                entity.HasIndex(x => x.Email).IsUnique();
                entity.Property(x => x.Id).HasColumnName("id");
                entity.Property(x => x.Email).HasColumnName("email").IsRequired();
                entity.Property(x => x.DisplayName).HasColumnName("display_name").IsRequired();
            });

            modelBuilder.Entity<FailureParent>(entity =>
            {
                entity.ToTable("failure_parents");
                entity.Property(x => x.Id).HasColumnName("id");
                entity.Property(x => x.Name).HasColumnName("name").IsRequired();
            });

            modelBuilder.Entity<FailureChild>(entity =>
            {
                entity.ToTable("failure_children");
                entity.Property(x => x.Id).HasColumnName("id");
                entity.Property(x => x.ParentId).HasColumnName("parent_id");
                entity.Property(x => x.Name).HasColumnName("name").IsRequired();
                entity.HasOne(x => x.Parent)
                    .WithMany(x => x.Children)
                    .HasForeignKey(x => x.ParentId)
                    .OnDelete(DeleteBehavior.Restrict);
            });

            modelBuilder.Entity<FailureStockCheck>(entity =>
            {
                entity.ToTable(
                    "failure_stock_checks",
                    tableBuilder => tableBuilder.HasCheckConstraint(
                        "CK_failure_stock_checks_quantity_nonnegative",
                        "\"quantity\" >= 0"));
                entity.Property(x => x.Id).HasColumnName("id");
                entity.Property(x => x.Quantity).HasColumnName("quantity");
            });
        }
    }

    private sealed class FailureStockCheck
    {
        public int Id { get; set; }
        public int Quantity { get; set; }
    }

    private sealed class FailureUser
    {
        public int Id { get; set; }
        public string Email { get; set; } = string.Empty;
        public string DisplayName { get; set; } = string.Empty;
    }

    private sealed class FailureParent
    {
        public int Id { get; set; }
        public string Name { get; set; } = string.Empty;
        public List<FailureChild> Children { get; set; } = [];
    }

    private sealed class FailureChild
    {
        public int Id { get; set; }
        public int ParentId { get; set; }
        public string Name { get; set; } = string.Empty;
        public FailureParent? Parent { get; set; }
    }
}
