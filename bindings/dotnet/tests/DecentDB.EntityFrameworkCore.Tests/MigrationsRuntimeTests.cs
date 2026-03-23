using DecentDB.AdoNet;
using DecentDB.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Infrastructure;
using Microsoft.EntityFrameworkCore.Migrations;
using Microsoft.EntityFrameworkCore.Migrations.Operations;
using Xunit;

namespace DecentDB.EntityFrameworkCore.Tests;

public sealed class MigrationsRuntimeTests : IDisposable
{
    private readonly string _dbPath = Path.Combine(Path.GetTempPath(), $"test_ef_migrate_{Guid.NewGuid():N}.ddb");

    public void Dispose()
    {
        TryDelete(_dbPath);
        TryDelete(_dbPath + "-wal");
    }

    [Fact]
    public void Migrate_AppliesPendingMigrations_AndMaintainsHistory()
    {
        using var context = CreateContext();
        context.Database.Migrate();

        var applied = context.Database.GetAppliedMigrations().ToList();
        Assert.Contains("202602130100_Initial", applied);
        Assert.Contains("202602130101_AddNameIndex", applied);

        using var conn = new DecentDBConnection($"Data Source={_dbPath}");
        conn.Open();
        using var cmd = conn.CreateCommand();
        cmd.CommandText = "SELECT COUNT(*) FROM \"__EFMigrationsHistory\"";
        Assert.Equal(2L, cmd.ExecuteScalar());
    }

    [Fact]
    public void Migrate_CanUpgradeExistingDatabase()
    {
        SeedInitialMigrationState();

        using (var context = CreateContext())
        {
            var applied = context.Database.GetAppliedMigrations().ToList();
            Assert.Contains("202602130100_Initial", applied);
            Assert.DoesNotContain("202602130101_AddNameIndex", applied);
            context.Database.Migrate();
        }

        using (var context = CreateContext())
        {
            var applied = context.Database.GetAppliedMigrations().ToList();
            Assert.Contains("202602130101_AddNameIndex", applied);
        }
    }

    [Fact]
    public void EnsureCreatedAndEnsureDeleted_AreIdempotent()
    {
        using var context = CreateContext();
        context.Database.EnsureDeleted();
        context.Database.EnsureCreated();
        context.Database.EnsureCreated();
        context.Database.EnsureDeleted();
        context.Database.EnsureDeleted();
    }

    [Fact]
    public void EnsureCreated_CanCreateTablesWithForeignKeys()
    {
        var optionsBuilder = new DbContextOptionsBuilder<FkDbContext>();
        optionsBuilder.UseDecentDB($"Data Source={_dbPath}");

        using var context = new FkDbContext(optionsBuilder.Options);
        context.Database.EnsureDeleted();

        // Should not throw "Table-level foreign keys not supported".
        context.Database.EnsureCreated();
    }

    [Fact]
    public void UnsupportedMigrationsOperation_ThrowsActionableError()
    {
        using var context = CreateContext();
        var generator = context.GetService<IMigrationsSqlGenerator>();
        var operation = new AddForeignKeyOperation
        {
            Table = "child",
            Name = "fk_child_parent",
            Columns = ["parent_id"],
            PrincipalTable = "parent",
            PrincipalColumns = ["id"]
        };

        var ex = Assert.Throws<NotSupportedException>(() => generator.Generate([operation], null));
        Assert.Contains("unsupported operation", ex.Message, StringComparison.OrdinalIgnoreCase);
    }

    private MigrationDbContext CreateContext()
    {
        var optionsBuilder = new DbContextOptionsBuilder<MigrationDbContext>();
        optionsBuilder.UseDecentDB($"Data Source={_dbPath}");
        return new MigrationDbContext(optionsBuilder.Options);
    }

    private static void TryDelete(string path)
    {
        if (File.Exists(path))
        {
            File.Delete(path);
        }
    }

    private void SeedInitialMigrationState()
    {
        using var conn = new DecentDBConnection($"Data Source={_dbPath}");
        conn.Open();
        using var cmd = conn.CreateCommand();
        cmd.CommandText = """
                          CREATE TABLE IF NOT EXISTS "__EFMigrationsHistory" (
                            "MigrationId" TEXT NOT NULL PRIMARY KEY,
                            "ProductVersion" TEXT NOT NULL
                          )
                          """;
        cmd.ExecuteNonQuery();
        cmd.CommandText = """
                          CREATE TABLE IF NOT EXISTS ef_migration_entities (
                            id INTEGER PRIMARY KEY,
                            name TEXT NOT NULL
                          )
                          """;
        cmd.ExecuteNonQuery();
        cmd.CommandText = "INSERT INTO \"__EFMigrationsHistory\" (\"MigrationId\", \"ProductVersion\") VALUES ('202602130100_Initial', '10.0.0')";
        cmd.ExecuteNonQuery();
    }
}

public sealed class MigrationDbContext : DbContext
{
    public MigrationDbContext(DbContextOptions<MigrationDbContext> options)
        : base(options)
    {
    }

    public DbSet<MigrationEntity> Entities => Set<MigrationEntity>();

    protected override void OnModelCreating(ModelBuilder modelBuilder)
    {
        modelBuilder.Entity<MigrationEntity>(entity =>
        {
            entity.ToTable("ef_migration_entities");
            entity.HasKey(x => x.Id);
            entity.Property(x => x.Id).HasColumnName("id").ValueGeneratedOnAdd();
            entity.Property(x => x.Name).HasColumnName("name");
        });
    }
}

public sealed class MigrationEntity
{
    public int Id { get; set; }
    public string Name { get; set; } = string.Empty;
}

public sealed class FkDbContext : DbContext
{
    public FkDbContext(DbContextOptions<FkDbContext> options)
        : base(options)
    {
    }

    public DbSet<FkArtist> Artists => Set<FkArtist>();
    public DbSet<FkAlbum> Albums => Set<FkAlbum>();

    protected override void OnModelCreating(ModelBuilder modelBuilder)
    {
        modelBuilder.Entity<FkArtist>(entity =>
        {
            entity.ToTable("fk_artists");
            entity.HasKey(x => x.Id);
            entity.Property(x => x.Id).HasColumnName("id").ValueGeneratedOnAdd();
            entity.Property(x => x.Name).HasColumnName("name");
        });

        modelBuilder.Entity<FkAlbum>(entity =>
        {
            entity.ToTable("fk_albums");
            entity.HasKey(x => x.Id);
            entity.Property(x => x.Id).HasColumnName("id").ValueGeneratedOnAdd();
            entity.Property(x => x.Title).HasColumnName("title");
            entity.Property(x => x.ArtistId).HasColumnName("artist_id");

            entity.HasOne(x => x.Artist)
                .WithMany(x => x.Albums)
                .HasForeignKey(x => x.ArtistId)
                .OnDelete(DeleteBehavior.Cascade);
        });
    }
}

public sealed class FkArtist
{
    public long Id { get; set; }
    public string Name { get; set; } = string.Empty;
    public List<FkAlbum> Albums { get; set; } = [];
}

public sealed class FkAlbum
{
    public long Id { get; set; }
    public string Title { get; set; } = string.Empty;
    public long ArtistId { get; set; }
    public FkArtist Artist { get; set; } = null!;
}

[DbContext(typeof(MigrationDbContext))]
[Migration("202602130100_Initial")]
public sealed class InitialMigration : Migration
{
    protected override void Up(MigrationBuilder migrationBuilder)
    {
        migrationBuilder.CreateTable(
            name: "ef_migration_entities",
            columns: table => new
            {
                id = table.Column<int>(type: "INTEGER", nullable: false),
                name = table.Column<string>(type: "TEXT", nullable: false)
            },
            constraints: table =>
            {
                table.PrimaryKey("PK_ef_migration_entities", x => x.id);
            });
    }

    protected override void Down(MigrationBuilder migrationBuilder)
        => migrationBuilder.DropTable(name: "ef_migration_entities");
}

[DbContext(typeof(MigrationDbContext))]
[Migration("202602130101_AddNameIndex")]
public sealed class AddNameIndexMigration : Migration
{
    protected override void Up(MigrationBuilder migrationBuilder)
        => migrationBuilder.CreateIndex(
            name: "IX_ef_migration_entities_name",
            table: "ef_migration_entities",
            column: "name");

    protected override void Down(MigrationBuilder migrationBuilder)
        => migrationBuilder.DropIndex(
            name: "IX_ef_migration_entities_name",
            table: "ef_migration_entities");
}
