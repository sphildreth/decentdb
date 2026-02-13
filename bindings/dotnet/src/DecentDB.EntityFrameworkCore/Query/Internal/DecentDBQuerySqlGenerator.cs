using Microsoft.EntityFrameworkCore.Query;
using Microsoft.EntityFrameworkCore.Query.SqlExpressions;

namespace DecentDB.EntityFrameworkCore.Query.Internal;

public sealed class DecentDBQuerySqlGenerator : QuerySqlGenerator
{
    internal const int MaxInListValues = 1000;

    public DecentDBQuerySqlGenerator(QuerySqlGeneratorDependencies dependencies)
        : base(dependencies)
    {
    }

    protected override void GenerateLimitOffset(SelectExpression selectExpression)
    {
        if (selectExpression.Limit is not null)
        {
            Sql.AppendLine().Append("LIMIT ");
            Visit(selectExpression.Limit);
        }

        if (selectExpression.Offset is not null)
        {
            Sql.AppendLine().Append("OFFSET ");
            Visit(selectExpression.Offset);
        }
    }

    protected override void GenerateIn(InExpression inExpression, bool negated)
    {
        if (inExpression.Values is { Count: > MaxInListValues })
        {
            throw new InvalidOperationException(
                $"DecentDB EF provider supports at most {MaxInListValues} values in an IN list.");
        }

        base.GenerateIn(inExpression, negated);
    }
}
