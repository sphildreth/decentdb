using System.Data.Common;
using DecentDB.Native;
using Microsoft.EntityFrameworkCore.Metadata.Builders;
using Microsoft.EntityFrameworkCore.Migrations;

namespace DecentDB.EntityFrameworkCore.Migrations.Internal;

internal sealed class DecentDBHistoryRepository : HistoryRepository, IHistoryRepository
{
    public DecentDBHistoryRepository(HistoryRepositoryDependencies dependencies)
        : base(dependencies)
    {
    }

    public override LockReleaseBehavior LockReleaseBehavior
        => LockReleaseBehavior.Connection;

    protected override string ExistsSql
        => $"SELECT 1 FROM {SqlGenerationHelper.DelimitIdentifier(TableName, TableSchema)} LIMIT 1";

    protected override bool InterpretExistsResult(object? value)
        => value is not null and not DBNull;

    protected override string GetAppliedMigrationsSql
        => $"""
           SELECT {SqlGenerationHelper.DelimitIdentifier(MigrationIdColumnName)}, {SqlGenerationHelper.DelimitIdentifier(ProductVersionColumnName)}
           FROM {SqlGenerationHelper.DelimitIdentifier(TableName, TableSchema)}
           ORDER BY {SqlGenerationHelper.DelimitIdentifier(MigrationIdColumnName)}
           """;

    public override bool Exists()
    {
        try
        {
            return base.Exists();
        }
        catch (DbException)
        {
            return false;
        }
        catch (DecentDBException)
        {
            return false;
        }
    }

    public override async Task<bool> ExistsAsync(CancellationToken cancellationToken = default)
    {
        try
        {
            return await base.ExistsAsync(cancellationToken);
        }
        catch (DbException)
        {
            return false;
        }
        catch (DecentDBException)
        {
            return false;
        }
    }

    public override string GetCreateIfNotExistsScript()
    {
        var table = SqlGenerationHelper.DelimitIdentifier(TableName, TableSchema);
        var migrationId = SqlGenerationHelper.DelimitIdentifier(MigrationIdColumnName);
        var productVersion = SqlGenerationHelper.DelimitIdentifier(ProductVersionColumnName);
        return $"""
               CREATE TABLE IF NOT EXISTS {table} (
                   {migrationId} TEXT NOT NULL PRIMARY KEY,
                   {productVersion} TEXT NOT NULL
               );
               """;
    }

    public override IMigrationsDatabaseLock AcquireDatabaseLock()
        => new DecentDBMigrationsDatabaseLock(this);

    public override Task<IMigrationsDatabaseLock> AcquireDatabaseLockAsync(CancellationToken cancellationToken = default)
        => Task.FromResult<IMigrationsDatabaseLock>(new DecentDBMigrationsDatabaseLock(this));

    public override string GetBeginIfNotExistsScript(string migrationId)
        => $"-- DecentDB idempotent guard (if not exists): {migrationId}";

    public override string GetBeginIfExistsScript(string migrationId)
        => $"-- DecentDB idempotent guard (if exists): {migrationId}";

    public override string GetEndIfScript()
        => string.Empty;

    bool IHistoryRepository.CreateIfNotExists()
    {
        if (Exists())
        {
            return false;
        }

        Create();
        return true;
    }

    async Task<bool> IHistoryRepository.CreateIfNotExistsAsync(CancellationToken cancellationToken)
    {
        if (await ExistsAsync(cancellationToken))
        {
            return false;
        }

        await CreateAsync(cancellationToken);
        return true;
    }

    protected override void ConfigureTable(EntityTypeBuilder<HistoryRow> history)
        => base.ConfigureTable(history);
}
