using System.Data.Common;

namespace DecentDB.AdoNet;

public sealed class DecentDBFactory : DbProviderFactory
{
    public static readonly DecentDBFactory Instance = new();

    private DecentDBFactory() { }

    public override DbConnection CreateConnection() => new DecentDBConnection();

    public override DbCommand CreateCommand() => new DecentDBCommand();

    public override DbParameter CreateParameter() => new DecentDBParameter();

    public override DbConnectionStringBuilder CreateConnectionStringBuilder() => new DecentDBConnectionStringBuilder();

    public override bool CanCreateDataSourceEnumerator => false;
}
