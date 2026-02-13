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

    private static NotSupportedException Unsupported(MigrationOperation operation, string message)
        => new($"DecentDB migrations unsupported operation '{operation.GetType().Name}': {message}");
}
