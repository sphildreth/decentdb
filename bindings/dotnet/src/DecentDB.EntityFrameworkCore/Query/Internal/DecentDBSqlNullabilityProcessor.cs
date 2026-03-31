using System.Linq.Expressions;
using DecentDB.EntityFrameworkCore.Query.Internal.SqlExpressions;
using Microsoft.EntityFrameworkCore.Query;
using Microsoft.EntityFrameworkCore.Query.SqlExpressions;

namespace DecentDB.EntityFrameworkCore.Query.Internal;

public sealed class DecentDBSqlNullabilityProcessor : SqlNullabilityProcessor
{
    public DecentDBSqlNullabilityProcessor(
        RelationalParameterBasedSqlProcessorDependencies dependencies,
        RelationalParameterBasedSqlProcessorParameters parameters)
        : base(dependencies, parameters)
    {
    }

    protected override SqlExpression VisitCustomSqlExpression(
        SqlExpression sqlExpression,
        bool allowOptimizedExpansion,
        out bool nullable)
    {
        if (sqlExpression is WindowFunctionExpression windowFunctionExpression)
        {
            var arguments = new SqlExpression[windowFunctionExpression.Arguments.Length];
            for (var i = 0; i < arguments.Length; i++)
            {
                arguments[i] = Visit(windowFunctionExpression.Arguments[i], allowOptimizedExpansion, out _);
            }

            var partitionBy = windowFunctionExpression.PartitionBy is null
                ? null
                : Visit(windowFunctionExpression.PartitionBy, allowOptimizedExpansion, out _);
            var orderBy = Visit(windowFunctionExpression.OrderBy, allowOptimizedExpansion, out _);

            nullable = windowFunctionExpression.FunctionName is not ("ROW_NUMBER" or "RANK" or "DENSE_RANK" or "PERCENT_RANK");
            return windowFunctionExpression.Update(arguments, partitionBy, orderBy);
        }

        return base.VisitCustomSqlExpression(sqlExpression, allowOptimizedExpansion, out nullable);
    }
}
