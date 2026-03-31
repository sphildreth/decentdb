using DecentDB.AdoNet;
using DecentDB.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Infrastructure;
using Microsoft.EntityFrameworkCore.Migrations;
using Microsoft.EntityFrameworkCore.Migrations.Operations;
using Xunit;

namespace DecentDB.EntityFrameworkCore.Tests;

public sealed class SchemaLifecycleMigrationTests : IDisposable
{
    private readonly string _dbPath = Path.Combine(Path.GetTempPath(), $"test_ef_schema_lifecycle_{Guid.NewGuid():N}.ddb");

    public void Dispose()
    {
        TryDelete(_dbPath);
        TryDelete(_dbPath + "-wal");
    }

    [Fact]
    public void Migrate_CanUpgradeInitialLifecycleState_AndPreserveData()
    {
        SeedInitialLifecycleState();

        using var context = CreateContext();
        context.Database.Migrate();

        using (var conn = OpenConnection())
        {
            var tablesJson = conn.ListTablesJson();
            Assert.Contains("migration_contacts", tablesJson);
            Assert.DoesNotContain("migration_people", tablesJson);

            var columnsJson = conn.GetTableColumnsJson("migration_contacts");
            Assert.Contains("display_name", columnsJson);
            Assert.Contains("slug", columnsJson);
            Assert.DoesNotContain("full_name", columnsJson);

            Assert.DoesNotContain("IX_migration_contacts_display_name", conn.ListIndexesJson());
            Assert.Equal(2L, ExecuteScalar(conn, "SELECT COUNT(*) FROM \"migration_nodes\""));
            Assert.Equal(1L, ExecuteScalar(conn, "SELECT COUNT(*) FROM \"migration_location_counts\""));
            Assert.Equal("Ada Lovelace", ExecuteScalar(conn, """
                                                       SELECT "display_name"
                                                       FROM "migration_contacts"
                                                       WHERE "id" = 1
                                                       """));
            Assert.Equal("pending", ExecuteScalar(conn, """
                                                  SELECT "slug"
                                                  FROM "migration_contacts"
                                                  WHERE "id" = 1
                                                  """));

            ExecuteNonQuery(conn, """
                                  INSERT INTO "migration_contacts" ("id", "display_name", "slug")
                                  VALUES (2, 'Grace Hopper', 'pending')
                                  """);

            Assert.Equal(2L, ExecuteScalar(conn, """
                                           SELECT COUNT(*)
                                           FROM "migration_contacts"
                                           WHERE "slug" = 'pending'
                                           """));
        }

        var applied = context.Database.GetAppliedMigrations().ToList();
        Assert.Contains("202603302300_Initial", applied);
        Assert.Contains("202603302301_SchemaV2", applied);
        Assert.Contains("202603302302_SchemaV3", applied);
    }

    [Fact]
    public void MigrationsSqlGenerator_SchemaLifecycleOperations_ExecuteAgainstLiveDatabase()
    {
        using var context = CreateContext();
        context.Database.EnsureDeleted();

        using var conn = OpenConnection();
        ExecuteNonQuery(conn, """
                              CREATE TABLE "migration_people" (
                                "id" INTEGER PRIMARY KEY,
                                "full_name" TEXT NOT NULL
                              )
                              """);
        ExecuteNonQuery(conn, """
                              INSERT INTO "migration_people" ("id", "full_name")
                              VALUES (1, 'Ada Lovelace')
                              """);

        var generator = context.GetService<IMigrationsSqlGenerator>();

        ExecuteOperations(
            conn,
            generator,
            new RenameTableOperation
            {
                Name = "migration_people",
                NewName = "migration_contacts"
            },
            new RenameColumnOperation
            {
                Table = "migration_contacts",
                Name = "full_name",
                NewName = "display_name"
            },
            new AddColumnOperation
            {
                Table = "migration_contacts",
                Name = "slug",
                ClrType = typeof(string),
                ColumnType = "TEXT",
                IsNullable = false,
                DefaultValue = "pending"
            },
            new CreateIndexOperation
            {
                Name = "IX_migration_contacts_display_name",
                Table = "migration_contacts",
                Columns = ["display_name"],
                IsUnique = false
            },
            new AddUniqueConstraintOperation
            {
                Name = "AK_migration_contacts_slug",
                Table = "migration_contacts",
                Columns = ["slug"]
            });

        Assert.Equal("Ada Lovelace", ExecuteScalar(conn, """
                                                   SELECT "display_name"
                                                   FROM "migration_contacts"
                                                   WHERE "id" = 1
                                                   """));
        Assert.Equal("pending", ExecuteScalar(conn, """
                                              SELECT "slug"
                                              FROM "migration_contacts"
                                              WHERE "id" = 1
                                              """));
        Assert.Contains("IX_migration_contacts_display_name", conn.ListIndexesJson());

        var duplicateSlug = Assert.ThrowsAny<Exception>(() => ExecuteNonQuery(conn, """
                                                                              INSERT INTO "migration_contacts" ("id", "display_name", "slug")
                                                                              VALUES (2, 'Grace Hopper', 'pending')
                                                                              """));
        Assert.Contains("unique", duplicateSlug.ToString(), StringComparison.OrdinalIgnoreCase);

        ExecuteOperations(
            conn,
            generator,
            new DropUniqueConstraintOperation
            {
                Name = "AK_migration_contacts_slug",
                Table = "migration_contacts"
            },
            new DropIndexOperation
            {
                Name = "IX_migration_contacts_display_name",
                Table = "migration_contacts"
            });

        Assert.DoesNotContain("IX_migration_contacts_display_name", conn.ListIndexesJson());

        ExecuteNonQuery(conn, """
                              INSERT INTO "migration_contacts" ("id", "display_name", "slug")
                              VALUES (2, 'Grace Hopper', 'pending')
                              """);
        Assert.Equal(2L, ExecuteScalar(conn, """
                                       SELECT COUNT(*)
                                       FROM "migration_contacts"
                                       WHERE "slug" = 'pending'
                                       """));
    }

    [Fact]
    public void MigrationsSqlGenerator_AlterColumnType_GeneratesDecentDbTypeChangeSql()
    {
        using var context = CreateContext();
        var generator = context.GetService<IMigrationsSqlGenerator>();
        var operation = new AlterColumnOperation
        {
            Table = "migration_contacts",
            Name = "slug",
            ClrType = typeof(string),
            ColumnType = "TEXT",
            IsNullable = false,
            OldColumn = new AddColumnOperation
            {
                Table = "migration_contacts",
                Name = "slug",
                ClrType = typeof(int),
                ColumnType = "INTEGER",
                IsNullable = false
            }
        };

        var commands = generator.Generate([operation], null);

        Assert.Single(commands);
        Assert.Contains(
            "ALTER TABLE \"migration_contacts\" ALTER COLUMN \"slug\" TYPE TEXT;",
            commands[0].CommandText,
            StringComparison.Ordinal);
    }

    [Fact]
    public void MigrationsSqlGenerator_AlterColumnNullabilityOrDefaultChange_ThrowsActionableError()
    {
        using var context = CreateContext();
        var generator = context.GetService<IMigrationsSqlGenerator>();
        var operation = new AlterColumnOperation
        {
            Table = "migration_contacts",
            Name = "slug",
            ClrType = typeof(string),
            ColumnType = "TEXT",
            IsNullable = false,
            DefaultValue = "pending",
            OldColumn = new AddColumnOperation
            {
                Table = "migration_contacts",
                Name = "slug",
                ClrType = typeof(string),
                ColumnType = "TEXT",
                IsNullable = true,
                DefaultValue = null
            }
        };

        var ex = Assert.Throws<NotSupportedException>(() => generator.Generate([operation], null));
        Assert.Contains("ALTER COLUMN", ex.Message, StringComparison.OrdinalIgnoreCase);
        Assert.Contains("nullability/default", ex.Message, StringComparison.OrdinalIgnoreCase);
    }

    private SchemaLifecycleDbContext CreateContext()
    {
        var optionsBuilder = new DbContextOptionsBuilder<SchemaLifecycleDbContext>();
        optionsBuilder.UseDecentDB($"Data Source={_dbPath}");
        return new SchemaLifecycleDbContext(optionsBuilder.Options);
    }

    private DecentDBConnection OpenConnection()
    {
        var conn = new DecentDBConnection($"Data Source={_dbPath}");
        conn.Open();
        return conn;
    }

    private static void ExecuteNonQuery(DecentDBConnection conn, string sql)
    {
        using var cmd = conn.CreateCommand();
        cmd.CommandText = sql;
        cmd.ExecuteNonQuery();
    }

    private static object ExecuteScalar(DecentDBConnection conn, string sql)
    {
        using var cmd = conn.CreateCommand();
        cmd.CommandText = sql;
        return cmd.ExecuteScalar()!;
    }

    private static void TryDelete(string path)
    {
        if (File.Exists(path))
        {
            File.Delete(path);
        }
    }

    private void SeedInitialLifecycleState()
    {
        using var conn = OpenConnection();
        ExecuteNonQuery(conn, """
                              CREATE TABLE "__EFMigrationsHistory" (
                                "MigrationId" TEXT NOT NULL PRIMARY KEY,
                                "ProductVersion" TEXT NOT NULL
                              )
                              """);
        ExecuteNonQuery(conn, """
                              INSERT INTO "__EFMigrationsHistory" ("MigrationId", "ProductVersion")
                              VALUES ('202603302300_Initial', '10.0.0')
                              """);
        ExecuteNonQuery(conn, """
                              CREATE TABLE "migration_people" (
                                "id" INTEGER PRIMARY KEY,
                                "full_name" TEXT NOT NULL
                              )
                              """);
        ExecuteNonQuery(conn, """
                              CREATE TABLE "migration_nodes" (
                                "id" INTEGER PRIMARY KEY,
                                "name" TEXT NOT NULL,
                                "parent_id" INTEGER,
                                CONSTRAINT "FK_migration_nodes_migration_nodes_parent_id"
                                FOREIGN KEY ("parent_id") REFERENCES "migration_nodes" ("id") ON DELETE RESTRICT
                              )
                              """);
        ExecuteNonQuery(conn, """
                              CREATE TABLE "migration_locations" (
                                "warehouse_code" TEXT NOT NULL,
                                "bin_code" TEXT NOT NULL,
                                "zone" TEXT NOT NULL,
                                CONSTRAINT "PK_migration_locations" PRIMARY KEY ("warehouse_code", "bin_code")
                              )
                              """);
        ExecuteNonQuery(conn, """
                              CREATE TABLE "migration_location_counts" (
                                "id" INTEGER PRIMARY KEY,
                                "warehouse_code" TEXT NOT NULL,
                                "bin_code" TEXT NOT NULL,
                                "product_name" TEXT NOT NULL,
                                "quantity" INTEGER NOT NULL,
                                CONSTRAINT "FK_migration_location_counts_migration_locations_warehouse_code_bin_code"
                                FOREIGN KEY ("warehouse_code", "bin_code")
                                REFERENCES "migration_locations" ("warehouse_code", "bin_code")
                                ON DELETE RESTRICT
                              )
                              """);
        ExecuteNonQuery(conn, """
                              INSERT INTO "migration_people" ("id", "full_name")
                              VALUES (1, 'Ada Lovelace')
                              """);
        ExecuteNonQuery(conn, """
                              INSERT INTO "migration_nodes" ("id", "name", "parent_id")
                              VALUES (1, 'root', NULL), (2, 'child', 1)
                              """);
        ExecuteNonQuery(conn, """
                              INSERT INTO "migration_locations" ("warehouse_code", "bin_code", "zone")
                              VALUES ('WH-1', 'A-01', 'cold')
                              """);
        ExecuteNonQuery(conn, """
                              INSERT INTO "migration_location_counts" ("id", "warehouse_code", "bin_code", "product_name", "quantity")
                              VALUES (10, 'WH-1', 'A-01', 'Widget', 5)
                              """);
    }

    private static void ExecuteOperations(
        DecentDBConnection conn,
        IMigrationsSqlGenerator generator,
        params MigrationOperation[] operations)
    {
        foreach (var command in generator.Generate(operations, null))
        {
            ExecuteNonQuery(conn, command.CommandText);
        }
    }
}

public sealed class SchemaLifecycleDbContext : DbContext
{
    public SchemaLifecycleDbContext(DbContextOptions<SchemaLifecycleDbContext> options)
        : base(options)
    {
    }

    public DbSet<MigrationContact> Contacts => Set<MigrationContact>();
    public DbSet<MigrationNode> Nodes => Set<MigrationNode>();
    public DbSet<MigrationLocation> Locations => Set<MigrationLocation>();
    public DbSet<MigrationLocationCount> LocationCounts => Set<MigrationLocationCount>();

    protected override void OnModelCreating(ModelBuilder modelBuilder)
    {
        modelBuilder.Entity<MigrationContact>(entity =>
        {
            entity.ToTable("migration_contacts");
            entity.HasKey(x => x.Id);
            entity.Property(x => x.Id).HasColumnName("id");
            entity.Property(x => x.DisplayName).HasColumnName("display_name");
            entity.Property(x => x.Slug).HasColumnName("slug");
        });

        modelBuilder.Entity<MigrationNode>(entity =>
        {
            entity.ToTable("migration_nodes");
            entity.HasKey(x => x.Id);
            entity.Property(x => x.Id).HasColumnName("id");
            entity.Property(x => x.Name).HasColumnName("name");
            entity.Property(x => x.ParentId).HasColumnName("parent_id");

            entity.HasOne(x => x.Parent)
                .WithMany(x => x.Children)
                .HasForeignKey(x => x.ParentId)
                .OnDelete(DeleteBehavior.Restrict);
        });

        modelBuilder.Entity<MigrationLocation>(entity =>
        {
            entity.ToTable("migration_locations");
            entity.HasKey(x => new { x.WarehouseCode, x.BinCode });
            entity.Property(x => x.WarehouseCode).HasColumnName("warehouse_code");
            entity.Property(x => x.BinCode).HasColumnName("bin_code");
            entity.Property(x => x.Zone).HasColumnName("zone");
        });

        modelBuilder.Entity<MigrationLocationCount>(entity =>
        {
            entity.ToTable("migration_location_counts");
            entity.HasKey(x => x.Id);
            entity.Property(x => x.Id).HasColumnName("id");
            entity.Property(x => x.WarehouseCode).HasColumnName("warehouse_code");
            entity.Property(x => x.BinCode).HasColumnName("bin_code");
            entity.Property(x => x.ProductName).HasColumnName("product_name");
            entity.Property(x => x.Quantity).HasColumnName("quantity");

            entity.HasOne(x => x.Location)
                .WithMany(x => x.Counts)
                .HasForeignKey(x => new { x.WarehouseCode, x.BinCode })
                .HasPrincipalKey(x => new { x.WarehouseCode, x.BinCode })
                .OnDelete(DeleteBehavior.Restrict);
        });
    }
}

public sealed class MigrationContact
{
    public long Id { get; set; }
    public string DisplayName { get; set; } = string.Empty;
    public string Slug { get; set; } = string.Empty;
}

public sealed class MigrationNode
{
    public long Id { get; set; }
    public string Name { get; set; } = string.Empty;
    public long? ParentId { get; set; }
    public MigrationNode? Parent { get; set; }
    public List<MigrationNode> Children { get; set; } = [];
}

public sealed class MigrationLocation
{
    public string WarehouseCode { get; set; } = string.Empty;
    public string BinCode { get; set; } = string.Empty;
    public string Zone { get; set; } = string.Empty;
    public List<MigrationLocationCount> Counts { get; set; } = [];
}

public sealed class MigrationLocationCount
{
    public long Id { get; set; }
    public string WarehouseCode { get; set; } = string.Empty;
    public string BinCode { get; set; } = string.Empty;
    public string ProductName { get; set; } = string.Empty;
    public int Quantity { get; set; }
    public MigrationLocation Location { get; set; } = null!;
}

[DbContext(typeof(SchemaLifecycleDbContext))]
[Migration("202603302300_Initial")]
public sealed class SchemaLifecycleInitialMigration : Migration
{
    protected override void Up(MigrationBuilder migrationBuilder)
    {
        migrationBuilder.CreateTable(
            name: "migration_people",
            columns: table => new
            {
                id = table.Column<long>(type: "INTEGER", nullable: false),
                full_name = table.Column<string>(type: "TEXT", nullable: false)
            },
            constraints: table =>
            {
                table.PrimaryKey("PK_migration_people", x => x.id);
            });

        migrationBuilder.CreateTable(
            name: "migration_nodes",
            columns: table => new
            {
                id = table.Column<long>(type: "INTEGER", nullable: false),
                name = table.Column<string>(type: "TEXT", nullable: false),
                parent_id = table.Column<long>(type: "INTEGER", nullable: true)
            },
            constraints: table =>
            {
                table.PrimaryKey("PK_migration_nodes", x => x.id);
                table.ForeignKey(
                    name: "FK_migration_nodes_migration_nodes_parent_id",
                    column: x => x.parent_id,
                    principalTable: "migration_nodes",
                    principalColumn: "id",
                    onDelete: ReferentialAction.Restrict);
            });

        migrationBuilder.CreateTable(
            name: "migration_locations",
            columns: table => new
            {
                warehouse_code = table.Column<string>(type: "TEXT", nullable: false),
                bin_code = table.Column<string>(type: "TEXT", nullable: false),
                zone = table.Column<string>(type: "TEXT", nullable: false)
            },
            constraints: table =>
            {
                table.PrimaryKey("PK_migration_locations", x => new { x.warehouse_code, x.bin_code });
            });

        migrationBuilder.CreateTable(
            name: "migration_location_counts",
            columns: table => new
            {
                id = table.Column<long>(type: "INTEGER", nullable: false),
                warehouse_code = table.Column<string>(type: "TEXT", nullable: false),
                bin_code = table.Column<string>(type: "TEXT", nullable: false),
                product_name = table.Column<string>(type: "TEXT", nullable: false),
                quantity = table.Column<int>(type: "INTEGER", nullable: false)
            },
            constraints: table =>
            {
                table.PrimaryKey("PK_migration_location_counts", x => x.id);
                table.ForeignKey(
                    name: "FK_migration_location_counts_migration_locations_warehouse_code_bin_code",
                    columns: x => new { x.warehouse_code, x.bin_code },
                    principalTable: "migration_locations",
                    principalColumns: ["warehouse_code", "bin_code"],
                    onDelete: ReferentialAction.Restrict);
            });
    }

    protected override void Down(MigrationBuilder migrationBuilder)
    {
        migrationBuilder.DropTable(name: "migration_location_counts");
        migrationBuilder.DropTable(name: "migration_locations");
        migrationBuilder.DropTable(name: "migration_nodes");
        migrationBuilder.DropTable(name: "migration_people");
    }
}

[DbContext(typeof(SchemaLifecycleDbContext))]
[Migration("202603302301_SchemaV2")]
public sealed class SchemaLifecycleSchemaV2Migration : Migration
{
    protected override void Up(MigrationBuilder migrationBuilder)
    {
        migrationBuilder.RenameTable(
            name: "migration_people",
            newName: "migration_contacts");

        migrationBuilder.RenameColumn(
            name: "full_name",
            table: "migration_contacts",
            newName: "display_name");

        migrationBuilder.AddColumn<string>(
            name: "slug",
            table: "migration_contacts",
            type: "TEXT",
            nullable: false,
            defaultValue: "pending");

        migrationBuilder.CreateIndex(
            name: "IX_migration_contacts_display_name",
            table: "migration_contacts",
            column: "display_name");

        migrationBuilder.AddUniqueConstraint(
            name: "AK_migration_contacts_slug",
            table: "migration_contacts",
            column: "slug");
    }

    protected override void Down(MigrationBuilder migrationBuilder)
    {
        migrationBuilder.DropUniqueConstraint(
            name: "AK_migration_contacts_slug",
            table: "migration_contacts");

        migrationBuilder.DropIndex(
            name: "IX_migration_contacts_display_name",
            table: "migration_contacts");

        migrationBuilder.DropColumn(
            name: "slug",
            table: "migration_contacts");

        migrationBuilder.RenameColumn(
            name: "display_name",
            table: "migration_contacts",
            newName: "full_name");

        migrationBuilder.RenameTable(
            name: "migration_contacts",
            newName: "migration_people");
    }
}

[DbContext(typeof(SchemaLifecycleDbContext))]
[Migration("202603302302_SchemaV3")]
public sealed class SchemaLifecycleSchemaV3Migration : Migration
{
    protected override void Up(MigrationBuilder migrationBuilder)
    {
        migrationBuilder.DropUniqueConstraint(
            name: "AK_migration_contacts_slug",
            table: "migration_contacts");

        migrationBuilder.DropIndex(
            name: "IX_migration_contacts_display_name",
            table: "migration_contacts");
    }

    protected override void Down(MigrationBuilder migrationBuilder)
    {
        migrationBuilder.CreateIndex(
            name: "IX_migration_contacts_display_name",
            table: "migration_contacts",
            column: "display_name");

        migrationBuilder.AddUniqueConstraint(
            name: "AK_migration_contacts_slug",
            table: "migration_contacts",
            column: "slug");
    }
}
