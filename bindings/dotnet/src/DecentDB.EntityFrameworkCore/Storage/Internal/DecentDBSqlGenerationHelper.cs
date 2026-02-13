using Microsoft.EntityFrameworkCore.Storage;

namespace DecentDB.EntityFrameworkCore.Storage.Internal;

internal sealed class DecentDBSqlGenerationHelper : RelationalSqlGenerationHelper
{
    public DecentDBSqlGenerationHelper(RelationalSqlGenerationHelperDependencies dependencies)
        : base(dependencies)
    {
    }
}
