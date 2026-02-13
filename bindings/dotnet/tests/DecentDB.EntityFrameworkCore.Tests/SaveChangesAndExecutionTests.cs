using System.Transactions;
using DecentDB.AdoNet;
using DecentDB.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.Logging;
using Xunit;

namespace DecentDB.EntityFrameworkCore.Tests;

public sealed class SaveChangesAndExecutionTests : IDisposable
{
    private readonly string _dbPath = Path.Combine(Path.GetTempPath(), $"test_ef_save_{Guid.NewGuid():N}.ddb");

    public void Dispose()
    {
        TryDelete(_dbPath);
        TryDelete(_dbPath + "-wal");
    }

    [Fact]
    public void SaveChanges_InsertUpdateDelete_Works()
    {
        EnsureSchema();

        using var context = CreateContext();
        var row = new SaveEntity { Name = "alpha", Slug = "a", Version = 1 };
        context.Entities.Add(row);
        context.SaveChanges();

        Assert.True(row.Id > 0);

        row.Name = "alpha2";
        row.Version = 2;
        context.SaveChanges();

        context.Entities.Remove(row);
        context.SaveChanges();

        Assert.Equal(0, context.Entities.Count());
    }

    [Fact]
    public void SaveChanges_ConcurrencyConflict_ThrowsDbUpdateConcurrencyException()
    {
        EnsureSchema();
        SeedRow("alpha", "a", 1);

        using var c1 = CreateContext();
        using var c2 = CreateContext();

        var r1 = c1.Entities.Single(x => x.Slug == "a");
        var r2 = c2.Entities.Single(x => x.Slug == "a");

        r1.Name = "alpha-1";
        r1.Version = 2;
        c1.SaveChanges();

        r2.Name = "alpha-2";
        r2.Version = 3;

        Assert.Throws<DbUpdateConcurrencyException>(() => c2.SaveChanges());
    }

    [Fact]
    public void SaveChanges_ConstraintViolation_ThrowsDbUpdateException()
    {
        EnsureSchema();
        SeedRow("alpha", "seed", 1);

        using var context = CreateContext();
        context.Entities.Add(new SaveEntity { Id = 1, Name = "beta", Slug = "dup", Version = 1 });

        var ex = Assert.Throws<DbUpdateException>(() => context.SaveChanges());
        Assert.NotNull(ex.InnerException);
    }

    [Fact]
    public void BeginTransaction_CommitAndRollback_Work()
    {
        EnsureSchema();

        using (var context = CreateContext())
        {
            using var tx = context.Database.BeginTransaction();
            context.Entities.Add(new SaveEntity { Name = "commit", Slug = "c", Version = 1 });
            context.SaveChanges();
            tx.Commit();
        }

        using (var context = CreateContext())
        {
            Assert.Equal(1, context.Entities.Count());
        }

        using (var context = CreateContext())
        {
            using var tx = context.Database.BeginTransaction();
            context.Entities.Add(new SaveEntity { Name = "rollback", Slug = "r", Version = 1 });
            context.SaveChanges();
            tx.Rollback();
        }

        using (var context = CreateContext())
        {
            Assert.Equal(1, context.Entities.Count());
        }
    }

    [Fact]
    public void TransactionScope_BestEffort_DoesNotBlockSaveChanges()
    {
        EnsureSchema();

        using (var scope = new TransactionScope(TransactionScopeOption.Required, TransactionScopeAsyncFlowOption.Enabled))
        {
            using var context = CreateContext();
            context.Entities.Add(new SaveEntity { Name = "ambient", Slug = "ambient", Version = 1 });
            context.SaveChanges();
            scope.Complete();
        }

        using var verify = CreateContext();
        Assert.Equal(1, verify.Entities.Count(x => x.Slug == "ambient"));
    }

    [Fact]
    public void EfDiagnostics_LogsExecutedSqlAndParameters()
    {
        EnsureSchema();
        SeedRow("alpha", "a", 1);

        var logs = new List<string>();
        using var loggerFactory = LoggerFactory.Create(builder =>
        {
            builder.SetMinimumLevel(LogLevel.Debug);
            builder.AddProvider(new ListLoggerProvider(logs));
        });

        using var context = CreateContext(loggerFactory);
        var value = "a";
        var rows = context.Entities.Where(x => x.Slug == value).ToList();

        Assert.Single(rows);
        Assert.Contains(logs, l => l.Contains("Executed DbCommand", StringComparison.Ordinal));
        Assert.Contains(logs, l => l.Contains("SELECT", StringComparison.OrdinalIgnoreCase));
        Assert.Contains(logs, l => l.Contains("@value", StringComparison.Ordinal));
    }

    private AppDbContext CreateContext(ILoggerFactory? loggerFactory = null)
    {
        var optionsBuilder = new DbContextOptionsBuilder<AppDbContext>();
        optionsBuilder.UseDecentDb($"Data Source={_dbPath}");
        optionsBuilder.EnableDetailedErrors();

        if (loggerFactory != null)
        {
            optionsBuilder.EnableSensitiveDataLogging();
            optionsBuilder.UseLoggerFactory(loggerFactory);
        }

        return new AppDbContext(optionsBuilder.Options);
    }

    private void EnsureSchema()
    {
        using var conn = new DecentDBConnection($"Data Source={_dbPath}");
        conn.Open();

        using var cmd = conn.CreateCommand();
        cmd.CommandText = "DROP TABLE IF EXISTS ef_save_entities";
        cmd.ExecuteNonQuery();

        cmd.CommandText = """
                          CREATE TABLE ef_save_entities (
                            id INTEGER PRIMARY KEY,
                            name TEXT NOT NULL,
                            slug TEXT NOT NULL,
                            version INTEGER NOT NULL
                          )
                          """;
        cmd.ExecuteNonQuery();

        cmd.CommandText = "CREATE UNIQUE INDEX ux_ef_save_entities_slug ON ef_save_entities (slug)";
        cmd.ExecuteNonQuery();
    }

    private void SeedRow(string name, string slug, int version)
    {
        using var conn = new DecentDBConnection($"Data Source={_dbPath}");
        conn.Open();

        using var cmd = conn.CreateCommand();
        cmd.CommandText = "INSERT INTO ef_save_entities (name, slug, version) VALUES (@n, @s, @v)";
        cmd.Parameters.Add(new DecentDBParameter("@n", name));
        cmd.Parameters.Add(new DecentDBParameter("@s", slug));
        cmd.Parameters.Add(new DecentDBParameter("@v", version));
        cmd.ExecuteNonQuery();
    }

    private static void TryDelete(string path)
    {
        if (File.Exists(path))
        {
            File.Delete(path);
        }
    }

    private sealed class AppDbContext : DbContext
    {
        public AppDbContext(DbContextOptions<AppDbContext> options)
            : base(options)
        {
        }

        public DbSet<SaveEntity> Entities => Set<SaveEntity>();

        protected override void OnModelCreating(ModelBuilder modelBuilder)
        {
            modelBuilder.Entity<SaveEntity>(entity =>
            {
                entity.ToTable("ef_save_entities");
                entity.HasKey(x => x.Id);
                entity.HasIndex(x => x.Slug).IsUnique();
                entity.Property(x => x.Id).HasColumnName("id").ValueGeneratedOnAdd();
                entity.Property(x => x.Name).HasColumnName("name");
                entity.Property(x => x.Slug).HasColumnName("slug");
                entity.Property(x => x.Version).HasColumnName("version").IsConcurrencyToken();
            });
        }
    }

    private sealed class SaveEntity
    {
        public int Id { get; set; }
        public string Name { get; set; } = string.Empty;
        public string Slug { get; set; } = string.Empty;
        public int Version { get; set; }
    }

    private sealed class ListLoggerProvider : ILoggerProvider
    {
        private readonly List<string> _logs;

        public ListLoggerProvider(List<string> logs)
        {
            _logs = logs;
        }

        public ILogger CreateLogger(string categoryName) => new ListLogger(_logs);

        public void Dispose()
        {
        }
    }

    private sealed class ListLogger : ILogger
    {
        private readonly List<string> _logs;

        public ListLogger(List<string> logs)
        {
            _logs = logs;
        }

        public IDisposable BeginScope<TState>(TState state) where TState : notnull => NullScope.Instance;

        public bool IsEnabled(LogLevel logLevel) => true;

        public void Log<TState>(LogLevel logLevel, EventId eventId, TState state, Exception? exception, Func<TState, Exception?, string> formatter)
            => _logs.Add(formatter(state, exception));
    }

    private sealed class NullScope : IDisposable
    {
        public static readonly NullScope Instance = new();

        public void Dispose()
        {
        }
    }
}
