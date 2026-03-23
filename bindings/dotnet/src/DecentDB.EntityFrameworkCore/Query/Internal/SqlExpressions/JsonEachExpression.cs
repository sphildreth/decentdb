using System.Linq.Expressions;
using System.Reflection;
using Microsoft.EntityFrameworkCore.Query;
using Microsoft.EntityFrameworkCore.Query.SqlExpressions;

namespace DecentDB.EntityFrameworkCore.Query.Internal.SqlExpressions;

/// <summary>
/// Represents a json_each() table-valued function call, used as an intermediate
/// expression for primitive collection translation. In most cases this gets optimized
/// away (e.g. .Any() → json_array_length() > 0) before SQL generation.
/// </summary>
public sealed class JsonEachExpression(
    string alias,
    SqlExpression json)
    : TableValuedFunctionExpression(alias, "json_each", schema: null, builtIn: true, [json])
{
    private static ConstructorInfo? _quotingConstructor;

    public const string KeyColumnName = "key";
    public const string ValueColumnName = "value";

    public SqlExpression Json { get; } = json;

    protected override Expression VisitChildren(ExpressionVisitor visitor)
    {
        var visitedJson = (SqlExpression)visitor.Visit(Json);
        return visitedJson == Json ? this : new JsonEachExpression(Alias, visitedJson);
    }

    public JsonEachExpression Update(SqlExpression jsonExpression)
        => jsonExpression == Json ? this : new JsonEachExpression(Alias, jsonExpression);

    public override TableExpressionBase Clone(string? alias, ExpressionVisitor cloningExpressionVisitor)
    {
        var newJson = (SqlExpression)cloningExpressionVisitor.Visit(Json);
        var clone = new JsonEachExpression(alias!, newJson);
        foreach (var annotation in GetAnnotations())
        {
            clone.AddAnnotation(annotation.Name, annotation.Value);
        }
        return clone;
    }

    public override JsonEachExpression WithAlias(string newAlias)
        => new(newAlias, Json);

    public override Expression Quote()
        => Expression.New(
            _quotingConstructor ??= typeof(JsonEachExpression).GetConstructor([typeof(string), typeof(SqlExpression)])!,
            Expression.Constant(Alias, typeof(string)),
            Json.Quote());

    protected override void Print(ExpressionPrinter expressionPrinter)
    {
        expressionPrinter.Append("json_each(");
        expressionPrinter.Visit(Json);
        expressionPrinter.Append(")");
        PrintAnnotations(expressionPrinter);
        expressionPrinter.Append(" AS ");
        expressionPrinter.Append(Alias);
    }

    public override bool Equals(object? obj)
        => ReferenceEquals(this, obj) || (obj is JsonEachExpression other && base.Equals(other));

    public override int GetHashCode()
        => base.GetHashCode();
}
