using Microsoft.EntityFrameworkCore.Update;

namespace DecentDB.EntityFrameworkCore.Update.Internal;

internal sealed class DecentDBUpdateSqlGenerator : UpdateSqlGenerator
{
    public DecentDBUpdateSqlGenerator(UpdateSqlGeneratorDependencies dependencies)
        : base(dependencies)
    {
    }
}
