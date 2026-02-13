using System.Linq.Expressions;
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

    protected override Expression VisitSqlBinary(SqlBinaryExpression sqlBinaryExpression)
    {
        // Render string addition as || (PostgreSQL-style concatenation) instead of +.
        if (sqlBinaryExpression.OperatorType == ExpressionType.Add
            && sqlBinaryExpression.Type == typeof(string))
        {
            Sql.Append("(");
            Visit(sqlBinaryExpression.Left);
            Sql.Append(" || ");
            Visit(sqlBinaryExpression.Right);
            Sql.Append(")");
            return sqlBinaryExpression;
        }

        return base.VisitSqlBinary(sqlBinaryExpression);
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
