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
        builder
            .Append("CREATE TABLE ")
            .Append(Dependencies.SqlGenerationHelper.DelimitIdentifier(operation.Name))
            .AppendLine(" (");

        using (builder.Indent())
        {
            var foreignKeys = new List<AddForeignKeyOperation>();
            foreach (var fk in operation.ForeignKeys)
            {
                if (fk.Columns is null
                    || fk.PrincipalColumns is null
                    || fk.Columns.Length == 0
                    || fk.PrincipalColumns.Length == 0
                    || fk.Columns.Length != fk.PrincipalColumns.Length
                    || string.IsNullOrWhiteSpace(fk.PrincipalTable))
                {
                    throw Unsupported(operation, "Foreign keys require matching child and parent columns and an explicit principal table.");
                }

                foreignKeys.Add(fk);
            }

            var remainingConstraints = operation.UniqueConstraints.Count
                + foreignKeys.Count
                + (operation.PrimaryKey is null ? 0 : 1);

            for (var i = 0; i < operation.Columns.Count; i++)
            {
                var column = operation.Columns[i];

                ColumnDefinition(column, model, builder);

                if (i < operation.Columns.Count - 1
                    || remainingConstraints > 0)
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
                remainingConstraints--;

                if (remainingConstraints > 0)
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
                remainingConstraints--;

                if (remainingConstraints > 0)
                {
                    builder.AppendLine(",");
                }
                else
                {
                    builder.AppendLine();
                }
            }

            foreach (var fk in foreignKeys)
            {
                ForeignKeyConstraint(fk, builder);
                remainingConstraints--;

                if (remainingConstraints > 0)
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
        var principalColumns = operation.PrincipalColumns;
        if (principalColumns is null || principalColumns.Length == 0)
        {
            throw Unsupported(operation, "Foreign keys require explicit principal columns.");
        }

        builder
            .Append("ALTER TABLE ")
            .Append(Dependencies.SqlGenerationHelper.DelimitIdentifier(operation.Table))
            .Append(" ADD CONSTRAINT ")
            .Append(Dependencies.SqlGenerationHelper.DelimitIdentifier(operation.Name))
            .Append(" FOREIGN KEY (");

        for (var i = 0; i < operation.Columns.Length; i++)
        {
            if (i > 0)
            {
                builder.Append(", ");
            }

            builder.Append(Dependencies.SqlGenerationHelper.DelimitIdentifier(operation.Columns[i]));
        }

        builder.Append(") REFERENCES ")
            .Append(Dependencies.SqlGenerationHelper.DelimitIdentifier(operation.PrincipalTable))
            .Append(" (");

        for (var i = 0; i < principalColumns.Length; i++)
        {
            if (i > 0)
            {
                builder.Append(", ");
            }

            builder.Append(Dependencies.SqlGenerationHelper.DelimitIdentifier(principalColumns[i]));
        }

        builder.Append(")");

        if (operation.OnUpdate != ReferentialAction.NoAction)
        {
            builder.Append(" ON UPDATE ").Append(FkAction(operation.OnUpdate));
        }

        if (operation.OnDelete != ReferentialAction.NoAction)
        {
            builder.Append(" ON DELETE ").Append(FkAction(operation.OnDelete));
        }

        if (terminate)
        {
            builder.AppendLine(Dependencies.SqlGenerationHelper.StatementTerminator);
            builder.EndCommand();
        }
    }

    protected override void Generate(
        DropForeignKeyOperation operation,
        IModel? model,
        MigrationCommandListBuilder builder,
        bool terminate = true)
    {
        builder
            .Append("ALTER TABLE ")
            .Append(Dependencies.SqlGenerationHelper.DelimitIdentifier(operation.Table))
            .Append(" DROP CONSTRAINT ")
            .Append(Dependencies.SqlGenerationHelper.DelimitIdentifier(operation.Name));

        if (terminate)
        {
            builder.AppendLine(Dependencies.SqlGenerationHelper.StatementTerminator);
            builder.EndCommand();
        }
    }

    private void ForeignKeyConstraint(
        AddForeignKeyOperation operation,
        MigrationCommandListBuilder builder)
    {
        var principalColumns = operation.PrincipalColumns;
        if (principalColumns is null || principalColumns.Length == 0)
        {
            throw Unsupported(operation, "Foreign keys require explicit principal columns.");
        }

        if (!string.IsNullOrWhiteSpace(operation.Name))
        {
            builder.Append("CONSTRAINT ")
                .Append(Dependencies.SqlGenerationHelper.DelimitIdentifier(operation.Name!))
                .Append(" ");
        }

        builder.Append("FOREIGN KEY (");

        for (var i = 0; i < operation.Columns.Length; i++)
        {
            if (i > 0)
            {
                builder.Append(", ");
            }

            builder.Append(Dependencies.SqlGenerationHelper.DelimitIdentifier(operation.Columns[i]));
        }

        builder.Append(") REFERENCES ")
            .Append(Dependencies.SqlGenerationHelper.DelimitIdentifier(operation.PrincipalTable))
            .Append(" (");

        for (var i = 0; i < principalColumns.Length; i++)
        {
            if (i > 0)
            {
                builder.Append(", ");
            }

            builder.Append(Dependencies.SqlGenerationHelper.DelimitIdentifier(principalColumns[i]));
        }

        builder.Append(")");

        if (operation.OnDelete != ReferentialAction.NoAction)
        {
            builder.Append(" ON DELETE ").Append(FkAction(operation.OnDelete));
        }

        if (operation.OnUpdate != ReferentialAction.NoAction)
        {
            builder.Append(" ON UPDATE ").Append(FkAction(operation.OnUpdate));
        }
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
