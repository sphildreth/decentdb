using System.Data;
using DecentDB.AdoNet;
using Microsoft.EntityFrameworkCore.Storage;

namespace DecentDB.EntityFrameworkCore.Storage.Internal;

internal sealed class DecentDBDatabaseCreator : RelationalDatabaseCreator
{
    public DecentDBDatabaseCreator(RelationalDatabaseCreatorDependencies dependencies)
        : base(dependencies)
    {
    }

    public override bool Exists()
        => File.Exists(GetDatabasePath());

    public override Task<bool> ExistsAsync(CancellationToken cancellationToken = default)
        => Task.FromResult(Exists());

    public override void Create()
    {
        EnsureDirectoryExists();
        OpenAndCloseConnection();
    }

    public override Task CreateAsync(CancellationToken cancellationToken = default)
    {
        Create();
        return Task.CompletedTask;
    }

    public override void Delete()
    {
        var path = GetDatabasePath();
        TryDelete(path);
        TryDelete(path + "-wal");
    }

    public override Task DeleteAsync(CancellationToken cancellationToken = default)
    {
        Delete();
        return Task.CompletedTask;
    }

    public override bool HasTables()
        => Exists();

    public override Task<bool> HasTablesAsync(CancellationToken cancellationToken = default)
        => Task.FromResult(HasTables());

    private void OpenAndCloseConnection()
    {
        var dbConnection = Dependencies.Connection.DbConnection;
        var wasOpen = dbConnection.State == ConnectionState.Open;
        if (!wasOpen)
        {
            dbConnection.Open();
        }

        if (!wasOpen)
        {
            dbConnection.Close();
        }
    }

    private void EnsureDirectoryExists()
    {
        var directory = Path.GetDirectoryName(GetDatabasePath());
        if (!string.IsNullOrWhiteSpace(directory) && !Directory.Exists(directory))
        {
            Directory.CreateDirectory(directory);
        }
    }

    private string GetDatabasePath()
    {
        if (Dependencies.Connection.DbConnection is DecentDBConnection connection
            && !string.IsNullOrWhiteSpace(connection.DataSource))
        {
            return Path.GetFullPath(connection.DataSource);
        }

        throw new InvalidOperationException("DecentDB connection does not have a valid data source.");
    }

    private static void TryDelete(string path)
    {
        if (File.Exists(path))
        {
            File.Delete(path);
        }
    }
}
