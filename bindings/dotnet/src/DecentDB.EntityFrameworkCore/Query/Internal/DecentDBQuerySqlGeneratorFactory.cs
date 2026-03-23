using Microsoft.EntityFrameworkCore.Query;

namespace DecentDB.EntityFrameworkCore.Query.Internal;

public sealed class DecentDBQuerySqlGeneratorFactory : IQuerySqlGeneratorFactory
{
    private readonly QuerySqlGeneratorDependencies _dependencies;

    public DecentDBQuerySqlGeneratorFactory(QuerySqlGeneratorDependencies dependencies)
    {
        _dependencies = dependencies;
    }

    public QuerySqlGenerator Create()
        => new DecentDBQuerySqlGenerator(_dependencies);
}
