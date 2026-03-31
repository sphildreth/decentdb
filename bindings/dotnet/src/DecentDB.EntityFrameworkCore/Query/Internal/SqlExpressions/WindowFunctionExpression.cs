using System.Linq.Expressions;
using System.Reflection;
using Microsoft.EntityFrameworkCore.Query;
using Microsoft.EntityFrameworkCore.Query.SqlExpressions;
using Microsoft.EntityFrameworkCore.Storage;

namespace DecentDB.EntityFrameworkCore.Query.Internal.SqlExpressions;

public sealed class WindowFunctionExpression(
    string functionName,
    SqlExpression[] arguments,
    SqlExpression? partitionBy,
    SqlExpression orderBy,
    bool orderByDescending,
    Type type,
    RelationalTypeMapping? typeMapping)
    : SqlExpression(type, typeMapping)
{
    private static ConstructorInfo? _quotingConstructor;

    public string FunctionName { get; } = functionName;
    public SqlExpression[] Arguments { get; } = arguments;
    public SqlExpression? PartitionBy { get; } = partitionBy;
    public SqlExpression OrderBy { get; } = orderBy;
    public bool OrderByDescending { get; } = orderByDescending;

    public WindowFunctionExpression Update(
        SqlExpression[] arguments,
        SqlExpression? partitionBy,
        SqlExpression orderBy)
        => arguments.SequenceEqual(Arguments)
            && partitionBy == PartitionBy
            && orderBy == OrderBy
            ? this
            : new WindowFunctionExpression(
                FunctionName,
                arguments,
                partitionBy,
                orderBy,
                OrderByDescending,
                Type,
                TypeMapping);

    protected override Expression VisitChildren(ExpressionVisitor visitor)
    {
        var visitedArguments = new SqlExpression[Arguments.Length];
        var changed = false;

        for (var i = 0; i < Arguments.Length; i++)
        {
            visitedArguments[i] = (SqlExpression)visitor.Visit(Arguments[i])!;
            changed |= visitedArguments[i] != Arguments[i];
        }

        var visitedPartitionBy = PartitionBy is null ? null : (SqlExpression)visitor.Visit(PartitionBy)!;
        changed |= visitedPartitionBy != PartitionBy;

        var visitedOrderBy = (SqlExpression)visitor.Visit(OrderBy)!;
        changed |= visitedOrderBy != OrderBy;

        return changed
            ? Update(visitedArguments, visitedPartitionBy, visitedOrderBy)
            : this;
    }

    public override Expression Quote()
        => Expression.New(
            _quotingConstructor ??= typeof(WindowFunctionExpression).GetConstructor(
                [
                    typeof(string),
                    typeof(SqlExpression[]),
                    typeof(SqlExpression),
                    typeof(SqlExpression),
                    typeof(bool),
                    typeof(Type),
                    typeof(RelationalTypeMapping)
                ])!,
            Expression.Constant(FunctionName, typeof(string)),
            Expression.NewArrayInit(
                typeof(SqlExpression),
                Arguments.Select(argument => Expression.Convert(argument.Quote(), typeof(SqlExpression)))),
            PartitionBy is null
                ? Expression.Constant(null, typeof(SqlExpression))
                : Expression.Convert(PartitionBy.Quote(), typeof(SqlExpression)),
            Expression.Convert(OrderBy.Quote(), typeof(SqlExpression)),
            Expression.Constant(OrderByDescending),
            Expression.Constant(Type, typeof(Type)),
            Expression.Constant(TypeMapping, typeof(RelationalTypeMapping)));

    protected override void Print(ExpressionPrinter expressionPrinter)
    {
        expressionPrinter.Append(FunctionName);
        expressionPrinter.Append("(");
        for (var i = 0; i < Arguments.Length; i++)
        {
            if (i > 0)
            {
                expressionPrinter.Append(", ");
            }

            expressionPrinter.Visit(Arguments[i]);
        }

        expressionPrinter.Append(") OVER (");
        if (PartitionBy is not null)
        {
            expressionPrinter.Append("PARTITION BY ");
            expressionPrinter.Visit(PartitionBy);
            expressionPrinter.Append(" ");
        }

        expressionPrinter.Append("ORDER BY ");
        expressionPrinter.Visit(OrderBy);
        if (OrderByDescending)
        {
            expressionPrinter.Append(" DESC");
        }

        expressionPrinter.Append(")");
    }

    public override bool Equals(object? obj)
        => ReferenceEquals(this, obj)
            || (obj is WindowFunctionExpression other
                && base.Equals(other)
                && FunctionName == other.FunctionName
                && OrderByDescending == other.OrderByDescending
                && Equals(PartitionBy, other.PartitionBy)
                && Equals(OrderBy, other.OrderBy)
                && Arguments.SequenceEqual(other.Arguments));

    public override int GetHashCode()
    {
        var hash = new HashCode();
        hash.Add(base.GetHashCode());
        hash.Add(FunctionName);
        hash.Add(OrderByDescending);
        hash.Add(PartitionBy);
        hash.Add(OrderBy);
        foreach (var argument in Arguments)
        {
            hash.Add(argument);
        }

        return hash.ToHashCode();
    }
}
