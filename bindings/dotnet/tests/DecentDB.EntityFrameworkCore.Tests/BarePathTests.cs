using DecentDB.AdoNet;
using DecentDB.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore;
using Xunit;

namespace DecentDB.EntityFrameworkCore.Tests;

public sealed class BarePathTests : IDisposable
{
    private readonly string _dbPath = Path.Combine(Path.GetTempPath(), $"test_ef_bare_{Guid.NewGuid():N}.ddb");

    public void Dispose()
    {
        TryDelete(_dbPath);
        TryDelete(_dbPath + "-wal");
    }

    [Fact]
    public void UseDecentDB_AcceptsBarePath()
    {
        var options = new DbContextOptionsBuilder<BareEntityContext>()
            .UseDecentDB(_dbPath)
            .Options;

        using (var context = new BareEntityContext(options))
        {
            context.Database.EnsureCreated();
            context.Entities.Add(new BareEntity { Name = "test" });
            context.SaveChanges();
        }

        using (var context = new BareEntityContext(options))
        {
            Assert.Equal(1, context.Entities.Count());
            Assert.Equal("test", context.Entities.First().Name);
        }
    }

    [Fact]
    public void UseDecentDB_AcceptsDataSourceConnectionString()
    {
        var connStr = $"Data Source={_dbPath}";
        var options = new DbContextOptionsBuilder<BareEntityContext>()
            .UseDecentDB(connStr)
            .Options;

        using (var context = new BareEntityContext(options))
        {
            context.Database.EnsureCreated();
            context.Entities.Add(new BareEntity { Name = "connstr-test" });
            context.SaveChanges();
        }

        using (var context = new BareEntityContext(options))
        {
            Assert.Equal(1, context.Entities.Count());
        }
    }

    [Fact]
    public void UseDecentDB_BothFormsTargetSameFile()
    {
        var bareOptions = new DbContextOptionsBuilder<BareEntityContext>()
            .UseDecentDB(_dbPath)
            .Options;

        using (var context = new BareEntityContext(bareOptions))
        {
            context.Database.EnsureCreated();
            context.Entities.Add(new BareEntity { Name = "shared" });
            context.SaveChanges();
        }

        var connStrOptions = new DbContextOptionsBuilder<BareEntityContext>()
            .UseDecentDB($"Data Source={_dbPath}")
            .Options;

        using (var context = new BareEntityContext(connStrOptions))
        {
            Assert.Equal(1, context.Entities.Count());
            Assert.Equal("shared", context.Entities.First().Name);
        }
    }

    private static void TryDelete(string path)
    {
        if (File.Exists(path)) File.Delete(path);
    }

    private sealed class BareEntityContext : DbContext
    {
        public BareEntityContext(DbContextOptions<BareEntityContext> options) : base(options) { }
        public DbSet<BareEntity> Entities => Set<BareEntity>();
        protected override void OnModelCreating(ModelBuilder modelBuilder)
        {
            modelBuilder.Entity<BareEntity>(e =>
            {
                e.ToTable("bare_entities");
                e.HasKey(x => x.Id);
                e.Property(x => x.Id).HasColumnName("id").ValueGeneratedOnAdd();
                e.Property(x => x.Name).HasColumnName("name");
            });
        }
    }

    private sealed class BareEntity
    {
        public int Id { get; set; }
        public string Name { get; set; } = string.Empty;
    }
}
