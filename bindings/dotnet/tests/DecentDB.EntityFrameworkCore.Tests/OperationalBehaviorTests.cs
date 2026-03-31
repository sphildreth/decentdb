using System.Data;
using DecentDB.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Storage;
using Xunit;

namespace DecentDB.EntityFrameworkCore.Tests;

public sealed class OperationalBehaviorTests : IDisposable
{
    private readonly string _dbPath = Path.Combine(Path.GetTempPath(), $"test_ef_operational_{Guid.NewGuid():N}.ddb");

    public void Dispose()
    {
        TryDelete(_dbPath);
        TryDelete(_dbPath + "-wal");
    }

    [Fact]
    public async Task BeginTransactionAsync_PreservesIsolationLevel_And_ExplicitlyRejectsSavepoints()
    {
        await using var context = CreateContext();
        await context.Database.EnsureCreatedAsync();

        await using var transaction = await context.Database.BeginTransactionAsync(IsolationLevel.ReadCommitted);
        Assert.Equal(IsolationLevel.ReadCommitted, transaction.GetDbTransaction().IsolationLevel);
        Assert.False(transaction.SupportsSavepoints);
        await Assert.ThrowsAsync<NotSupportedException>(() => transaction.CreateSavepointAsync("before_more_work"));
    }

    private OperationalDbContext CreateContext()
    {
        var optionsBuilder = new DbContextOptionsBuilder<OperationalDbContext>();
        optionsBuilder.UseDecentDB($"Data Source={_dbPath}");
        return new OperationalDbContext(optionsBuilder.Options);
    }

    private static void TryDelete(string path)
    {
        if (File.Exists(path))
        {
            File.Delete(path);
        }
    }

    private sealed class OperationalDbContext : DbContext
    {
        public OperationalDbContext(DbContextOptions<OperationalDbContext> options)
            : base(options)
        {
        }

        public DbSet<OperationalRow> Rows => Set<OperationalRow>();

        protected override void OnModelCreating(ModelBuilder modelBuilder)
        {
            modelBuilder.Entity<OperationalRow>(entity =>
            {
                entity.ToTable("operational_rows");
                entity.HasKey(x => x.Id);
                entity.Property(x => x.Id).HasColumnName("id").ValueGeneratedOnAdd();
                entity.Property(x => x.Name).HasColumnName("name");
            });
        }
    }

    private sealed class OperationalRow
    {
        public int Id { get; set; }
        public string Name { get; set; } = string.Empty;
    }
}
