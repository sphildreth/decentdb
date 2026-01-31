using System;
using System.Runtime.CompilerServices;
using System.Linq.Expressions;
using System.Reflection;
using System.Text;

namespace DecentDb.MicroOrm;

internal sealed class ExpressionSqlBuilder<T>
{
    private readonly EntityMap _map;
    private readonly List<(string Name, Func<object?> Getter, int? MaxLength)> _parameters = new();

    private static readonly ConditionalWeakTable<LambdaExpression, CompiledWhere> Cache = new();

    public ExpressionSqlBuilder(EntityMap map)
    {
        _map = map;
    }

    public (string Sql, IReadOnlyList<(string Name, object? Value, int? MaxLength)> Parameters) BuildWhere(Expression<Func<T, bool>> predicate)
    {
        if (Cache.TryGetValue(predicate, out var compiled))
        {
            return (compiled.Sql, compiled.Evaluate());
        }

        var where = Visit(predicate.Body);
        var compiledNew = new CompiledWhere(where, _parameters);
        Cache.Add(predicate, compiledNew);
        return (compiledNew.Sql, compiledNew.Evaluate());
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
            ExpressionType.Constant => AddParameter(() => ((ConstantExpression)expr).Value, null),
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
                var argExpr = mce.Arguments[0];
                Func<object?> getter = () =>
                {
                    var raw = Evaluate(argExpr);
                    var s = raw as string ?? throw new NotSupportedException("LIKE patterns must be strings");
                    return mce.Method.Name switch
                    {
                        "Contains" => $"%{s}%",
                        "StartsWith" => $"{s}%",
                        "EndsWith" => $"%{s}",
                        _ => throw new NotSupportedException()
                    };
                };

                var param = AddParameter(getter, maxLen);
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
        return AddParameter(() => Evaluate(me), null);
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

    private string AddParameter(Func<object?> getter, int? maxLength)
    {
        var name = $"@p{_parameters.Count}";
        _parameters.Add((name, getter, maxLength));
        return name;
    }

    private static object? Evaluate(Expression expr)
    {
        expr = StripConvert(expr);

        if (expr is ConstantExpression ce) return ce.Value;

        if (expr is MemberExpression me)
        {
            var target = Evaluate(me.Expression!);
            return me.Member switch
            {
                FieldInfo fi => fi.GetValue(target),
                PropertyInfo pi => pi.GetValue(target),
                _ => throw new NotSupportedException($"Unsupported member: {me.Member.MemberType}")
            };
        }

        // Fallback: evaluate via compilation.
        var lambda = Expression.Lambda(expr);
        var del = lambda.Compile();
        return del.DynamicInvoke();
    }

    private static Expression StripConvert(Expression expr)
    {
        while (expr is UnaryExpression ue && (ue.NodeType == ExpressionType.Convert || ue.NodeType == ExpressionType.ConvertChecked))
        {
            expr = ue.Operand;
        }
        return expr;
    }

    private sealed class CompiledWhere
    {
        private readonly (string Name, Func<object?> Getter, int? MaxLength)[] _parameters;

        public CompiledWhere(string sql, List<(string Name, Func<object?> Getter, int? MaxLength)> parameters)
        {
            Sql = sql;
            _parameters = parameters.ToArray();
        }

        public string Sql { get; }

        public IReadOnlyList<(string Name, object? Value, int? MaxLength)> Evaluate()
        {
            var result = new (string Name, object? Value, int? MaxLength)[_parameters.Length];
            for (var i = 0; i < _parameters.Length; i++)
            {
                var p = _parameters[i];
                result[i] = (p.Name, p.Getter(), p.MaxLength);
            }
            return result;
        }
    }
}
