using System;
using System.Linq.Expressions;
using System.Reflection;
using System.Text;

namespace DecentDb.MicroOrm;

internal sealed class ExpressionSqlBuilder<T>
{
    private readonly EntityMap _map;
    private readonly List<(string Name, object? Value, int? MaxLength)> _parameters = new();

    public ExpressionSqlBuilder(EntityMap map)
    {
        _map = map;
    }

    public (string Sql, IReadOnlyList<(string Name, object? Value, int? MaxLength)> Parameters) BuildWhere(Expression<Func<T, bool>> predicate)
    {
        var where = Visit(predicate.Body);
        return (where, _parameters);
    }

    private string Visit(Expression expr)
    {
        return expr.NodeType switch
        {
            ExpressionType.AndAlso => VisitBinary((BinaryExpression)expr, "AND"),
            ExpressionType.OrElse => VisitBinary((BinaryExpression)expr, "OR"),
            ExpressionType.Equal => VisitBinary((BinaryExpression)expr, "="),
            ExpressionType.NotEqual => VisitBinary((BinaryExpression)expr, "!="),
            ExpressionType.LessThan => VisitBinary((BinaryExpression)expr, "<"),
            ExpressionType.LessThanOrEqual => VisitBinary((BinaryExpression)expr, "<="),
            ExpressionType.GreaterThan => VisitBinary((BinaryExpression)expr, ">"),
            ExpressionType.GreaterThanOrEqual => VisitBinary((BinaryExpression)expr, ">="),
            ExpressionType.Call => VisitCall((MethodCallExpression)expr),
            ExpressionType.MemberAccess => VisitMember((MemberExpression)expr),
            ExpressionType.Convert => Visit(((UnaryExpression)expr).Operand),
            ExpressionType.Constant => AddParameter(((ConstantExpression)expr).Value, null),
            _ => throw new NotSupportedException($"Unsupported expression node: {expr.NodeType}")
        };
    }

    private string VisitBinary(BinaryExpression be, string op)
    {
        // Handle NULL comparisons as IS / IS NOT.
        if (IsNullConstant(be.Right) && TryGetColumn(be.Left, out var colLeft, out _))
        {
            return op == "="
                ? $"({colLeft} IS NULL)"
                : $"({colLeft} IS NOT NULL)";
        }

        if (IsNullConstant(be.Left) && TryGetColumn(be.Right, out var colRight, out _))
        {
            return op == "="
                ? $"({colRight} IS NULL)"
                : $"({colRight} IS NOT NULL)";
        }

        var left = Visit(be.Left);
        var right = Visit(be.Right);
        return $"({left} {op} {right})";
    }

    private string VisitCall(MethodCallExpression mce)
    {
        if (mce.Method.DeclaringType == typeof(string))
        {
            var target = mce.Object;
            if (target == null) throw new NotSupportedException("Static string calls not supported");

            if (!TryGetColumn(target, out var column, out var maxLen))
            {
                throw new NotSupportedException("Only calls on entity string properties are supported");
            }

            if (mce.Method.Name is "Contains" or "StartsWith" or "EndsWith")
            {
                if (mce.Arguments.Count != 1) throw new NotSupportedException("Unexpected string method arity");
                var value = Evaluate(mce.Arguments[0]);
                var s = value as string ?? throw new NotSupportedException("LIKE patterns must be strings");

                var pattern = mce.Method.Name switch
                {
                    "Contains" => $"%{s}%",
                    "StartsWith" => $"{s}%",
                    "EndsWith" => $"%{s}",
                    _ => throw new NotSupportedException()
                };

                var param = AddParameter(pattern, maxLen);
                return $"({column} LIKE {param})";
            }
        }

        throw new NotSupportedException($"Unsupported method call: {mce.Method.DeclaringType?.FullName}.{mce.Method.Name}");
    }

    private string VisitMember(MemberExpression me)
    {
        if (TryGetColumn(me, out var col, out _))
        {
            return col;
        }

        // Captured variables / static members.
        var val = Evaluate(me);
        return AddParameter(val, null);
    }

    private bool TryGetColumn(Expression expr, out string columnSql, out int? maxLength)
    {
        if (expr is MemberExpression me && me.Member is PropertyInfo pi)
        {
            // Entity property access: x.Prop
            if (me.Expression is ParameterExpression)
            {
                var pm = _map.GetPropertyMap(pi);
                columnSql = pm.ColumnName;
                maxLength = pm.MaxLength;
                return true;
            }
        }

        columnSql = string.Empty;
        maxLength = null;
        return false;
    }

    private static bool IsNullConstant(Expression expr)
    {
        return expr is ConstantExpression ce && ce.Value == null;
    }

    private string AddParameter(object? value, int? maxLength)
    {
        var name = $"@p{_parameters.Count}";
        _parameters.Add((name, value, maxLength));
        return name;
    }

    private static object? Evaluate(Expression expr)
    {
        if (expr is ConstantExpression ce) return ce.Value;

        // Evaluate via compilation (safe here; used for captured values only).
        var lambda = Expression.Lambda(expr);
        var del = lambda.Compile();
        return del.DynamicInvoke();
    }
}
