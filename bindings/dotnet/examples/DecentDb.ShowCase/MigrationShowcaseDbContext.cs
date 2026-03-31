using DecentDB.AdoNet;
using DecentDB.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Infrastructure;
using Microsoft.EntityFrameworkCore.Migrations;

namespace DecentDb.ShowCase;

internal sealed class MigrationShowcaseDbContext : DbContext
{
    private readonly string _dbPath;

    public MigrationShowcaseDbContext(string dbPath)
    {
        _dbPath = dbPath;
    }

    public MigrationShowcaseDbContext(DbContextOptions<MigrationShowcaseDbContext> options)
        : base(options)
    {
        _dbPath = string.Empty;
    }

    public DbSet<MigrationShowcaseContact> Contacts => Set<MigrationShowcaseContact>();
    public DbSet<MigrationShowcaseNode> Nodes => Set<MigrationShowcaseNode>();
    public DbSet<MigrationShowcaseLocation> Locations => Set<MigrationShowcaseLocation>();
    public DbSet<MigrationShowcaseLocationCount> LocationCounts => Set<MigrationShowcaseLocationCount>();

    protected override void OnConfiguring(DbContextOptionsBuilder optionsBuilder)
    {
        if (!optionsBuilder.IsConfigured)
        {
            optionsBuilder.UseDecentDB($"Data Source={_dbPath}");
        }
    }

    protected override void OnModelCreating(ModelBuilder modelBuilder)
    {
        modelBuilder.Entity<MigrationShowcaseContact>(entity =>
        {
            entity.ToTable("migration_contacts");
            entity.HasKey(x => x.Id);
            entity.Property(x => x.Id).HasColumnName("id");
            entity.Property(x => x.DisplayName).HasColumnName("display_name");
            entity.Property(x => x.Slug).HasColumnName("slug");
        });

        modelBuilder.Entity<MigrationShowcaseNode>(entity =>
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

        modelBuilder.Entity<MigrationShowcaseLocation>(entity =>
        {
            entity.ToTable("migration_locations");
            entity.HasKey(x => new { x.WarehouseCode, x.BinCode });
            entity.Property(x => x.WarehouseCode).HasColumnName("warehouse_code");
            entity.Property(x => x.BinCode).HasColumnName("bin_code");
            entity.Property(x => x.Zone).HasColumnName("zone");
        });

        modelBuilder.Entity<MigrationShowcaseLocationCount>(entity =>
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

internal static class MigrationShowcaseSeed
{
    public static void SeedInitialState(string dbPath)
    {
        using var conn = new DecentDBConnection($"Data Source={dbPath}");
        conn.Open();

        MigrationShowcaseSql.ExecuteNonQuery(
            conn,
            """
            CREATE TABLE "__EFMigrationsHistory" (
              "MigrationId" TEXT NOT NULL PRIMARY KEY,
              "ProductVersion" TEXT NOT NULL
            )
            """);
        MigrationShowcaseSql.ExecuteNonQuery(
            conn,
            """
            INSERT INTO "__EFMigrationsHistory" ("MigrationId", "ProductVersion")
            VALUES ('202603302400_Initial', '10.0.0')
            """);
        MigrationShowcaseSql.ExecuteNonQuery(
            conn,
            """
            CREATE TABLE "migration_people" (
              "id" INTEGER PRIMARY KEY,
              "full_name" TEXT NOT NULL
            )
            """);
        MigrationShowcaseSql.ExecuteNonQuery(
            conn,
            """
            CREATE TABLE "migration_nodes" (
              "id" INTEGER PRIMARY KEY,
              "name" TEXT NOT NULL,
              "parent_id" INTEGER,
              CONSTRAINT "FK_migration_nodes_migration_nodes_parent_id"
              FOREIGN KEY ("parent_id") REFERENCES "migration_nodes" ("id") ON DELETE RESTRICT
            )
            """);
        MigrationShowcaseSql.ExecuteNonQuery(
            conn,
            """
            CREATE TABLE "migration_locations" (
              "warehouse_code" TEXT NOT NULL,
              "bin_code" TEXT NOT NULL,
              "zone" TEXT NOT NULL,
              CONSTRAINT "PK_migration_locations" PRIMARY KEY ("warehouse_code", "bin_code")
            )
            """);
        MigrationShowcaseSql.ExecuteNonQuery(
            conn,
            """
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
        MigrationShowcaseSql.ExecuteNonQuery(
            conn,
            """
            INSERT INTO "migration_people" ("id", "full_name")
            VALUES (1, 'Ada Lovelace')
            """);
        MigrationShowcaseSql.ExecuteNonQuery(
            conn,
            """
            INSERT INTO "migration_nodes" ("id", "name", "parent_id")
            VALUES (1, 'root', NULL), (2, 'child', 1)
            """);
        MigrationShowcaseSql.ExecuteNonQuery(
            conn,
            """
            INSERT INTO "migration_locations" ("warehouse_code", "bin_code", "zone")
            VALUES ('WH-1', 'A-01', 'cold')
            """);
        MigrationShowcaseSql.ExecuteNonQuery(
            conn,
            """
            INSERT INTO "migration_location_counts" ("id", "warehouse_code", "bin_code", "product_name", "quantity")
            VALUES (10, 'WH-1', 'A-01', 'Widget', 5)
            """);
    }
}

internal static class MigrationShowcaseSql
{
    public static void ExecuteNonQuery(DecentDBConnection conn, string sql)
    {
        using var cmd = conn.CreateCommand();
        cmd.CommandText = sql;
        cmd.ExecuteNonQuery();
    }

    public static object ExecuteScalar(DecentDBConnection conn, string sql)
    {
        using var cmd = conn.CreateCommand();
        cmd.CommandText = sql;
        return cmd.ExecuteScalar()!;
    }

    public static string ShortMigrationName(string migrationId)
    {
        var separator = migrationId.IndexOf('_');
        return separator >= 0 && separator < migrationId.Length - 1
            ? migrationId[(separator + 1)..]
            : migrationId;
    }
}

internal sealed class MigrationShowcaseContact
{
    public long Id { get; set; }
    public string DisplayName { get; set; } = string.Empty;
    public string Slug { get; set; } = string.Empty;
}

internal sealed class MigrationShowcaseNode
{
    public long Id { get; set; }
    public string Name { get; set; } = string.Empty;
    public long? ParentId { get; set; }
    public MigrationShowcaseNode? Parent { get; set; }
    public List<MigrationShowcaseNode> Children { get; set; } = [];
}

internal sealed class MigrationShowcaseLocation
{
    public string WarehouseCode { get; set; } = string.Empty;
    public string BinCode { get; set; } = string.Empty;
    public string Zone { get; set; } = string.Empty;
    public List<MigrationShowcaseLocationCount> Counts { get; set; } = [];
}

internal sealed class MigrationShowcaseLocationCount
{
    public long Id { get; set; }
    public string WarehouseCode { get; set; } = string.Empty;
    public string BinCode { get; set; } = string.Empty;
    public string ProductName { get; set; } = string.Empty;
    public int Quantity { get; set; }
    public MigrationShowcaseLocation Location { get; set; } = null!;
}

[DbContext(typeof(MigrationShowcaseDbContext))]
[Migration("202603302400_Initial")]
internal sealed class MigrationShowcaseInitialMigration : Migration
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

[DbContext(typeof(MigrationShowcaseDbContext))]
[Migration("202603302401_SchemaV2")]
internal sealed class MigrationShowcaseSchemaV2Migration : Migration
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

[DbContext(typeof(MigrationShowcaseDbContext))]
[Migration("202603302402_SchemaV3")]
internal sealed class MigrationShowcaseSchemaV3Migration : Migration
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
