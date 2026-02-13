using Microsoft.EntityFrameworkCore.Metadata;
using Microsoft.EntityFrameworkCore.Migrations;
using Microsoft.EntityFrameworkCore.Migrations.Operations;

namespace DecentDB.EntityFrameworkCore.Migrations.Internal;

internal sealed class DecentDBMigrationsSqlGenerator : MigrationsSqlGenerator
{
    public DecentDBMigrationsSqlGenerator(MigrationsSqlGeneratorDependencies dependencies)
        : base(dependencies)
    {
    }

    protected override void Generate(
        CreateTableOperation operation,
        IModel? model,
        MigrationCommandListBuilder builder,
        bool terminate = true)
    {
        // DecentDB currently supports FOREIGN KEY constraints only as column-level constraints
        // (e.g., `artist_id INTEGER REFERENCES artists(id)`), not as table-level constraints.

        builder
            .Append("CREATE TABLE ")
            .Append(Dependencies.SqlGenerationHelper.DelimitIdentifier(operation.Name))
            .AppendLine(" (");

        using (builder.Indent())
        {
            var foreignKeyByColumn = new Dictionary<string, AddForeignKeyOperation>(StringComparer.OrdinalIgnoreCase);
            foreach (var fk in operation.ForeignKeys)
            {
                if (fk.Columns is null
                    || fk.PrincipalColumns is null
                    || fk.Columns.Length != 1
                    || fk.PrincipalColumns.Length != 1
                    || string.IsNullOrWhiteSpace(fk.PrincipalTable))
                {
                    throw Unsupported(operation, "Composite foreign keys are not supported by DecentDB DDL (table-level FOREIGN KEY constraints are rejected).");
                }

                var column = fk.Columns[0];
                if (foreignKeyByColumn.ContainsKey(column))
                {
                    throw Unsupported(operation, $"Multiple foreign keys for column '{column}' are not supported.");
                }

                foreignKeyByColumn[column] = fk;
            }

            for (var i = 0; i < operation.Columns.Count; i++)
            {
                var column = operation.Columns[i];

                ColumnDefinition(column, model, builder);

                if (foreignKeyByColumn.TryGetValue(column.Name, out var fk))
                {
                    builder.Append(" REFERENCES ")
                        .Append(Dependencies.SqlGenerationHelper.DelimitIdentifier(fk.PrincipalTable))
                        .Append(" (")
                        .Append(Dependencies.SqlGenerationHelper.DelimitIdentifier(fk.PrincipalColumns![0]))
                        .Append(")");

                    // Emit actions explicitly to keep DDL deterministic.
                    builder.Append(" ON DELETE ").Append(FkAction(fk.OnDelete));
                    builder.Append(" ON UPDATE ").Append(FkAction(fk.OnUpdate));
                }

                if (i < operation.Columns.Count - 1
                    || operation.PrimaryKey is not null
                    || operation.UniqueConstraints.Count > 0)
                {
                    builder.AppendLine(",");
                }
                else
                {
                    builder.AppendLine();
                }
            }

            if (operation.PrimaryKey is not null)
            {
                PrimaryKeyConstraint(operation.PrimaryKey, model, builder);

                if (operation.UniqueConstraints.Count > 0)
                {
                    builder.AppendLine(",");
                }
                else
                {
                    builder.AppendLine();
                }
            }

            for (var i = 0; i < operation.UniqueConstraints.Count; i++)
            {
                UniqueConstraint(operation.UniqueConstraints[i], model, builder);

                if (i < operation.UniqueConstraints.Count - 1)
                {
                    builder.AppendLine(",");
                }
                else
                {
                    builder.AppendLine();
                }
            }
        }

        builder.Append(")");

        if (terminate)
        {
            builder.AppendLine(Dependencies.SqlGenerationHelper.StatementTerminator);
            builder.EndCommand();
        }
    }

    protected override void Generate(
        EnsureSchemaOperation operation,
        IModel? model,
        MigrationCommandListBuilder builder)
    {
        // DecentDB does not have schema namespaces.
    }

    protected override void Generate(
        DropSchemaOperation operation,
        IModel? model,
        MigrationCommandListBuilder builder)
    {
        throw Unsupported(operation, "DROP SCHEMA is not supported by DecentDB.");
    }

    protected override void Generate(
        AddForeignKeyOperation operation,
        IModel? model,
        MigrationCommandListBuilder builder,
        bool terminate = true)
    {
        throw Unsupported(operation, "ALTER TABLE ... ADD FOREIGN KEY is not supported by DecentDB migrations yet.");
    }

    protected override void Generate(
        DropForeignKeyOperation operation,
        IModel? model,
        MigrationCommandListBuilder builder,
        bool terminate = true)
    {
        throw Unsupported(operation, "ALTER TABLE ... DROP FOREIGN KEY is not supported by DecentDB migrations yet.");
    }

    private static string FkAction(ReferentialAction action)
        => action switch
        {
            ReferentialAction.NoAction => "NO ACTION",
            ReferentialAction.Restrict => "RESTRICT",
            ReferentialAction.Cascade => "CASCADE",
            ReferentialAction.SetNull => "SET NULL",
            ReferentialAction.SetDefault => throw new NotSupportedException("DecentDB does not support SET DEFAULT referential actions."),
            _ => throw new NotSupportedException($"Unsupported referential action '{action}'.")
        };

    private static NotSupportedException Unsupported(MigrationOperation operation, string message)
        => new($"DecentDB migrations unsupported operation '{operation.GetType().Name}': {message}");
}
