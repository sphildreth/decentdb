using DecentDB.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Infrastructure;
using Microsoft.EntityFrameworkCore.Migrations;
using Microsoft.EntityFrameworkCore.Migrations.Operations;
using System.Reflection;
using Xunit;

namespace DecentDB.EntityFrameworkCore.Tests;

public sealed class MigrationsSqlGeneratorCoverageTests : IDisposable
{
    private readonly string _dbPath = Path.Combine(Path.GetTempPath(), $"test_migrations_sql_generator_{Guid.NewGuid():N}.ddb");

    [Fact]
    public void EnsureSchemaOperation_GeneratesNoCommands()
    {
        using var context = CreateContext();
        var generator = context.GetService<IMigrationsSqlGenerator>();

        var commands = generator.Generate([new EnsureSchemaOperation { Name = "ignored" }], model: null);
        Assert.Empty(commands);
    }

    [Fact]
    public void RenameTable_WithSchema_ThrowsActionableError()
    {
        using var context = CreateContext();
        var generator = context.GetService<IMigrationsSqlGenerator>();

        var ex = Assert.Throws<NotSupportedException>(() => generator.Generate(
            [
                new RenameTableOperation
                {
                    Name = "old_name",
                    NewName = "new_name",
                    Schema = "app"
                }
            ],
            model: null));

        Assert.Contains("Schema-qualified table renames", ex.Message, StringComparison.OrdinalIgnoreCase);
    }

    [Fact]
    public void RenameColumn_WithSchema_ThrowsActionableError()
    {
        using var context = CreateContext();
        var generator = context.GetService<IMigrationsSqlGenerator>();

        var ex = Assert.Throws<NotSupportedException>(() => generator.Generate(
            [
                new RenameColumnOperation
                {
                    Table = "items",
                    Name = "old_col",
                    NewName = "new_col",
                    Schema = "app"
                }
            ],
            model: null));

        Assert.Contains("Schema-qualified column renames", ex.Message, StringComparison.OrdinalIgnoreCase);
    }

    [Fact]
    public void AlterColumn_ValidationBranches_ThrowWhenMetadataIsInvalid()
    {
        using var context = CreateContext();
        var generator = context.GetService<IMigrationsSqlGenerator>();

        Assert.Throws<NotSupportedException>(() => generator.Generate(
            [
                new AlterColumnOperation
                {
                    Table = "items",
                    Name = "value",
                    Schema = "app",
                    ColumnType = "TEXT",
                    OldColumn = new AddColumnOperation { Table = "items", Name = "value", ColumnType = "TEXT" }
                }
            ],
            model: null));

        Assert.Throws<NotSupportedException>(() => generator.Generate(
            [
                new AlterColumnOperation
                {
                    Table = "items",
                    Name = "value",
                    ColumnType = "TEXT",
                    OldColumn = null!
                }
            ],
            model: null));

        Assert.Throws<NotSupportedException>(() => generator.Generate(
            [
                new AlterColumnOperation
                {
                    Table = "items",
                    Name = "value",
                    OldColumn = new AddColumnOperation { Table = "items", Name = "value", ColumnType = "INTEGER" }
                }
            ],
            model: null));
    }

    [Fact]
    public void AddForeignKey_MissingPrincipalColumns_Throws()
    {
        using var context = CreateContext();
        var generator = context.GetService<IMigrationsSqlGenerator>();

        var ex = Assert.Throws<NotSupportedException>(() => generator.Generate(
            [
                new AddForeignKeyOperation
                {
                    Name = "FK_child_parent",
                    Table = "child",
                    Columns = ["parent_id"],
                    PrincipalTable = "parent"
                }
            ],
            model: null));

        Assert.Contains("principal columns", ex.Message, StringComparison.OrdinalIgnoreCase);
    }

    [Fact]
    public void DropIndex_WithSchema_Throws()
    {
        using var context = CreateContext();
        var generator = context.GetService<IMigrationsSqlGenerator>();

        var ex = Assert.Throws<NotSupportedException>(() => generator.Generate(
            [
                new DropIndexOperation
                {
                    Name = "IX_items_value",
                    Table = "items",
                    Schema = "app"
                }
            ],
            model: null));

        Assert.Contains("Schema-qualified index drops", ex.Message, StringComparison.OrdinalIgnoreCase);
    }

    [Fact]
    public void CreateTable_WithInvalidForeignKeyShape_Throws()
    {
        using var context = CreateContext();
        var generator = context.GetService<IMigrationsSqlGenerator>();

        var operation = new CreateTableOperation
        {
            Name = "child",
            Columns =
            {
                new AddColumnOperation { Name = "id", ClrType = typeof(int), ColumnType = "INTEGER" },
                new AddColumnOperation { Name = "parent_id", ClrType = typeof(int), ColumnType = "INTEGER" }
            },
            PrimaryKey = new AddPrimaryKeyOperation
            {
                Name = "PK_child",
                Table = "child",
                Columns = ["id"]
            },
            ForeignKeys =
            {
                new AddForeignKeyOperation
                {
                    Name = "FK_child_parent_parent_id",
                    Table = "child",
                    Columns = ["parent_id"],
                    PrincipalTable = "",
                    PrincipalColumns = ["id"]
                }
            }
        };

        var ex = Assert.Throws<NotSupportedException>(() => generator.Generate([operation], model: null));
        Assert.Contains("Foreign keys require matching child and parent columns", ex.Message, StringComparison.OrdinalIgnoreCase);
    }

    [Fact]
    public void CreateTable_WithUniqueAndForeignKeyConstraints_GeneratesExpectedSql()
    {
        using var context = CreateContext();
        var generator = context.GetService<IMigrationsSqlGenerator>();

        var operation = new CreateTableOperation
        {
            Name = "child",
            Columns =
            {
                new AddColumnOperation { Name = "id", ClrType = typeof(int), ColumnType = "INTEGER", IsNullable = false },
                new AddColumnOperation { Name = "parent_id", ClrType = typeof(int), ColumnType = "INTEGER", IsNullable = false },
                new AddColumnOperation { Name = "code", ClrType = typeof(string), ColumnType = "TEXT", IsNullable = false }
            },
            PrimaryKey = new AddPrimaryKeyOperation
            {
                Name = "PK_child",
                Table = "child",
                Columns = ["id"]
            },
            UniqueConstraints =
            {
                new AddUniqueConstraintOperation
                {
                    Name = "AK_child_code",
                    Table = "child",
                    Columns = ["code"]
                }
            },
            ForeignKeys =
            {
                new AddForeignKeyOperation
                {
                    Name = "FK_child_parent_parent_id",
                    Table = "child",
                    Columns = ["parent_id"],
                    PrincipalTable = "parent",
                    PrincipalColumns = ["id"],
                    OnDelete = ReferentialAction.Cascade,
                    OnUpdate = ReferentialAction.Restrict
                }
            }
        };

        var commands = generator.Generate([operation], model: null);
        var sql = Assert.Single(commands).CommandText;

        Assert.Contains("CREATE TABLE \"child\"", sql, StringComparison.Ordinal);
        Assert.Contains("CONSTRAINT \"AK_child_code\" UNIQUE (\"code\")", sql, StringComparison.Ordinal);
        Assert.Contains("CONSTRAINT \"FK_child_parent_parent_id\" FOREIGN KEY (\"parent_id\") REFERENCES \"parent\" (\"id\")", sql, StringComparison.Ordinal);
        Assert.Contains("ON DELETE CASCADE", sql, StringComparison.Ordinal);
        Assert.Contains("ON UPDATE RESTRICT", sql, StringComparison.Ordinal);
    }

    [Fact]
    public void ForeignKeySetDefaultAction_IsRejected()
    {
        using var context = CreateContext();
        var generator = context.GetService<IMigrationsSqlGenerator>();

        var ex = Assert.Throws<NotSupportedException>(() => generator.Generate(
            [
                new AddForeignKeyOperation
                {
                    Name = "FK_child_parent_parent_id",
                    Table = "child",
                    Columns = ["parent_id"],
                    PrincipalTable = "parent",
                    PrincipalColumns = ["id"],
                    OnDelete = ReferentialAction.SetDefault
                }
            ],
            model: null));

        Assert.Contains("SET DEFAULT", ex.Message, StringComparison.OrdinalIgnoreCase);
    }

    [Fact]
    public void AddAndDropForeignKey_WithCompositeColumns_GeneratesExpectedSql()
    {
        using var context = CreateContext();
        var generator = context.GetService<IMigrationsSqlGenerator>();

        var addCommands = generator.Generate(
        [
            new AddForeignKeyOperation
            {
                Name = "FK_child_parent",
                Table = "child",
                Columns = ["parent_id_a", "parent_id_b"],
                PrincipalTable = "parent",
                PrincipalColumns = ["id_a", "id_b"],
                OnDelete = ReferentialAction.Cascade,
                OnUpdate = ReferentialAction.Restrict
            }
        ], model: null);

        var addSql = Assert.Single(addCommands).CommandText;
        Assert.Contains("FOREIGN KEY (\"parent_id_a\", \"parent_id_b\")", addSql, StringComparison.Ordinal);
        Assert.Contains("REFERENCES \"parent\" (\"id_a\", \"id_b\")", addSql, StringComparison.Ordinal);
        Assert.Contains("ON UPDATE RESTRICT", addSql, StringComparison.Ordinal);
        Assert.Contains("ON DELETE CASCADE", addSql, StringComparison.Ordinal);

        var dropCommands = generator.Generate(
        [
            new DropForeignKeyOperation
            {
                Name = "FK_child_parent",
                Table = "child"
            }
        ], model: null);

        var dropSql = Assert.Single(dropCommands).CommandText;
        Assert.Contains("ALTER TABLE \"child\" DROP CONSTRAINT \"FK_child_parent\";", dropSql, StringComparison.Ordinal);
    }

    [Fact]
    public void CreateTable_WithOnlyUniqueConstraint_UsesTerminalConstraintFormatting()
    {
        using var context = CreateContext();
        var generator = context.GetService<IMigrationsSqlGenerator>();

        var operation = new CreateTableOperation
        {
            Name = "unique_only",
            Columns =
            {
                new AddColumnOperation { Name = "id", ClrType = typeof(int), ColumnType = "INTEGER", IsNullable = false },
                new AddColumnOperation { Name = "code", ClrType = typeof(string), ColumnType = "TEXT", IsNullable = false }
            },
            PrimaryKey = new AddPrimaryKeyOperation
            {
                Name = "PK_unique_only",
                Table = "unique_only",
                Columns = ["id"]
            },
            UniqueConstraints =
            {
                new AddUniqueConstraintOperation
                {
                    Name = "AK_unique_only_code",
                    Table = "unique_only",
                    Columns = ["code"]
                }
            }
        };

        var sql = Assert.Single(generator.Generate([operation], model: null)).CommandText;
        Assert.Contains("CONSTRAINT \"AK_unique_only_code\" UNIQUE (\"code\")", sql, StringComparison.Ordinal);
    }

    [Fact]
    public void PrivateHelpers_CoverForeignKeyConstraintValidation_AndFkActionSwitch()
    {
        using var context = CreateContext();
        var generator = context.GetService<IMigrationsSqlGenerator>();
        var generatorType = generator.GetType();

        var foreignKeyConstraintMethod = generatorType
            .GetMethods(BindingFlags.Instance | BindingFlags.NonPublic)
            .Single(m =>
                m.Name == "ForeignKeyConstraint" &&
                m.GetParameters().Length == 2 &&
                m.GetParameters()[0].ParameterType == typeof(AddForeignKeyOperation));
        Assert.NotNull(foreignKeyConstraintMethod);

        var missingPrincipalColumns = new AddForeignKeyOperation
        {
            Name = "FK_missing_cols",
            Table = "child",
            Columns = ["parent_id"],
            PrincipalTable = "parent",
            PrincipalColumns = null
        };

        var missingPrincipalEx = Assert.Throws<TargetInvocationException>(() =>
            foreignKeyConstraintMethod!.Invoke(generator, [missingPrincipalColumns, null!]));
        Assert.IsType<NotSupportedException>(missingPrincipalEx.InnerException);

        var fkActionMethod = generatorType.GetMethod("FkAction", BindingFlags.Static | BindingFlags.NonPublic);
        Assert.NotNull(fkActionMethod);

        var noAction = fkActionMethod!.Invoke(null, [ReferentialAction.NoAction]);
        Assert.Equal("NO ACTION", noAction);

        var invalidActionEx = Assert.Throws<TargetInvocationException>(() =>
            fkActionMethod.Invoke(null, [(ReferentialAction)int.MaxValue]));
        Assert.IsType<NotSupportedException>(invalidActionEx.InnerException);
    }

    public void Dispose()
    {
        TryDelete(_dbPath);
        TryDelete(_dbPath + "-wal");
    }

    private DbContext CreateContext()
    {
        var options = new DbContextOptionsBuilder<GeneratorContext>()
            .UseDecentDB($"Data Source={_dbPath}")
            .Options;
        return new GeneratorContext(options);
    }

    private static void TryDelete(string path)
    {
        if (File.Exists(path))
        {
            File.Delete(path);
        }
    }

    private sealed class GeneratorContext(DbContextOptions<GeneratorContext> options) : DbContext(options);
}
