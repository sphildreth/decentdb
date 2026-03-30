using Microsoft.EntityFrameworkCore.Query;
using Microsoft.EntityFrameworkCore.Query.Internal;

namespace DecentDB.EntityFrameworkCore.Query.Internal;

#pragma warning disable EF1001
public sealed class DecentDBRelationalParameterBasedSqlProcessorFactory
    : RelationalParameterBasedSqlProcessorFactory
{
    public DecentDBRelationalParameterBasedSqlProcessorFactory(
        RelationalParameterBasedSqlProcessorDependencies dependencies)
        : base(dependencies)
    {
    }

    public override RelationalParameterBasedSqlProcessor Create(
        RelationalParameterBasedSqlProcessorParameters parameters)
        => new DecentDBRelationalParameterBasedSqlProcessor(Dependencies, parameters);
}
#pragma warning restore EF1001
