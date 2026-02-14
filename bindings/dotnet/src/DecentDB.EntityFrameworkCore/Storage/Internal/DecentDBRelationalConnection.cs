using System.Data.Common;
using DecentDB.AdoNet;
using Microsoft.EntityFrameworkCore.Storage;

namespace DecentDB.EntityFrameworkCore.Storage.Internal;

internal sealed class DecentDBRelationalConnection : RelationalConnection
{
    public DecentDBRelationalConnection(RelationalConnectionDependencies dependencies)
        : base(dependencies)
    {
    }

    protected override DbConnection CreateDbConnection()
    {
        if (!string.IsNullOrWhiteSpace(ConnectionString))
        {
            return new DecentDBConnection(ConnectionString);
        }

        throw new InvalidOperationException("No DecentDB connection string was configured.");
    }
}
