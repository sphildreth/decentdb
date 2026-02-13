using Microsoft.EntityFrameworkCore.Storage;

namespace DecentDB.EntityFrameworkCore.Storage.Internal;

internal sealed class DecentDBTypeMappingSource : RelationalTypeMappingSource
{
    public DecentDBTypeMappingSource(
        TypeMappingSourceDependencies dependencies,
        RelationalTypeMappingSourceDependencies relationalDependencies)
        : base(dependencies, relationalDependencies)
    {
    }

    protected override RelationalTypeMapping? FindMapping(in RelationalTypeMappingInfo mappingInfo)
        => null;
}
