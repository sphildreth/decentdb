using System.Linq.Expressions;
using Microsoft.EntityFrameworkCore.Query;

namespace DecentDB.EntityFrameworkCore.Query.Internal;

public sealed class DecentDBRelationalParameterBasedSqlProcessor : RelationalParameterBasedSqlProcessor
{
    public DecentDBRelationalParameterBasedSqlProcessor(
        RelationalParameterBasedSqlProcessorDependencies dependencies,
        RelationalParameterBasedSqlProcessorParameters parameters)
        : base(dependencies, parameters)
    {
    }

    protected override Expression ProcessSqlNullability(
        Expression queryExpression,
        ParametersCacheDecorator parametersDecorator)
        => new DecentDBSqlNullabilityProcessor(Dependencies, Parameters).Process(queryExpression, parametersDecorator);
}
