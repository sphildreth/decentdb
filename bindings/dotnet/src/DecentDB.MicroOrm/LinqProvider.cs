using System;
using System.Collections;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
using System.Reflection;

namespace DecentDB.MicroOrm;

internal sealed class DecentDBQueryProvider<T> : IQueryProvider where T : class, new()
{
    private readonly DbSet<T> _root;

    public DecentDBQueryProvider(DbSet<T> root)
    {
        _root = root;
    }

    public IQueryable CreateQuery(Expression expression)
    {
        var elementType = expression.Type.GetGenericArguments().FirstOrDefault() ?? typeof(T);
        var queryableType = typeof(MicroOrmQueryable<>).MakeGenericType(elementType);
        return (IQueryable)Activator.CreateInstance(queryableType, this, expression)!;
    }

    public IQueryable<TElement> CreateQuery<TElement>(Expression expression)
    {
        if (typeof(TElement) != typeof(T))
        {
            throw new NotSupportedException("Cross-type queries are not supported.");
        }

        return (IQueryable<TElement>)(object)new MicroOrmQueryable<T>(this, expression);
    }

    public object? Execute(Expression expression)
    {
        return Execute<object?>(expression);
    }

    public TResult Execute<TResult>(Expression expression)
    {
        var resultType = typeof(TResult);

        // Terminal operations (Count/First/Single/Any)
        if (expression is MethodCallExpression mce && mce.Method.DeclaringType == typeof(Queryable))
        {
            var methodName = mce.Method.Name;

            if (methodName == nameof(Queryable.Count))
            {
                var set = BuildSet(mce.Arguments[0]);
                if (mce.Arguments.Count == 2)
                {
                    var pred = (Expression<Func<T, bool>>)StripQuotes(mce.Arguments[1]);
                    set = set.Where(pred);
                }

                var count = set.CountAsync().GetAwaiter().GetResult();
                if (resultType == typeof(int))
                {
                    return (TResult)(object)checked((int)count);
                }
                return (TResult)(object)count;
            }

            if (methodName == nameof(Queryable.LongCount))
            {
                var set = BuildSet(mce.Arguments[0]);
                if (mce.Arguments.Count == 2)
                {
                    var pred = (Expression<Func<T, bool>>)StripQuotes(mce.Arguments[1]);
                    set = set.Where(pred);
                }

                var count = set.CountAsync().GetAwaiter().GetResult();
                return (TResult)(object)count;
            }

            if (methodName == nameof(Queryable.Any))
            {
                var set = BuildSet(mce.Arguments[0]);
                if (mce.Arguments.Count == 2)
                {
                    var pred = (Expression<Func<T, bool>>)StripQuotes(mce.Arguments[1]);
                    set = set.Where(pred);
                }

                var any = set.AnyAsync().GetAwaiter().GetResult();
                return (TResult)(object)any;
            }

            if (methodName == nameof(Queryable.First) || methodName == nameof(Queryable.FirstOrDefault) ||
                methodName == nameof(Queryable.Single) || methodName == nameof(Queryable.SingleOrDefault))
            {
                var set = BuildSet(mce.Arguments[0]);
                if (mce.Arguments.Count == 2)
                {
                    var pred = (Expression<Func<T, bool>>)StripQuotes(mce.Arguments[1]);
                    set = set.Where(pred);
                }

                object? scalar = methodName switch
                {
                    nameof(Queryable.First) => set.FirstAsync().GetAwaiter().GetResult(),
                    nameof(Queryable.FirstOrDefault) => set.FirstOrDefaultAsync().GetAwaiter().GetResult(),
                    nameof(Queryable.Single) => set.SingleAsync().GetAwaiter().GetResult(),
                    nameof(Queryable.SingleOrDefault) => set.SingleOrDefaultAsync().GetAwaiter().GetResult(),
                    _ => throw new NotSupportedException()
                };

                return (TResult)scalar!;
            }
        }

        // Enumeration: execute and return IEnumerable<T>.
        if (resultType == typeof(IEnumerable<T>) || resultType == typeof(IQueryable<T>) || resultType == typeof(object))
        {
            var set = BuildSet(expression);
            var list = set.ToListAsync().GetAwaiter().GetResult();
            return (TResult)(object)list;
        }

        // Enumerable.ToList() and similar will enumerate; fall back to list.
        if (typeof(IEnumerable).IsAssignableFrom(resultType))
        {
            var set = BuildSet(expression);
            var list = set.ToListAsync().GetAwaiter().GetResult();
            return (TResult)(object)list;
        }

        throw new NotSupportedException($"Unsupported LINQ execution result type: {resultType.FullName}");
    }

    internal DbSet<T> BuildSet(Expression expression)
    {
        if (expression is ConstantExpression ce)
        {
            if (ce.Value is DbSet<T> s) return s;
            if (ce.Value is MicroOrmQueryable<T>) return _root;
        }

        if (expression is MethodCallExpression mce && mce.Method.DeclaringType == typeof(Queryable))
        {
            var name = mce.Method.Name;
            if (name == nameof(Queryable.Where))
            {
                var source = BuildSet(mce.Arguments[0]);
                var pred = (Expression<Func<T, bool>>)StripQuotes(mce.Arguments[1]);
                return source.Where(pred);
            }

            if (name == nameof(Queryable.OrderBy) || name == nameof(Queryable.OrderByDescending) ||
                name == nameof(Queryable.ThenBy) || name == nameof(Queryable.ThenByDescending))
            {
                var source = BuildSet(mce.Arguments[0]);
                var lambda = (LambdaExpression)StripQuotes(mce.Arguments[1]);
                var keyType = lambda.ReturnType;

                var isDesc = name.EndsWith("Descending", StringComparison.Ordinal);
                var isThen = name.StartsWith("ThenBy", StringComparison.Ordinal);

                var targetMethod = typeof(DbSet<T>).GetMethods(BindingFlags.Public | BindingFlags.Instance)
                    .First(m => m.Name == (isThen ? (isDesc ? nameof(DbSet<T>.ThenByDescending) : nameof(DbSet<T>.ThenBy))
                                                  : (isDesc ? nameof(DbSet<T>.OrderByDescending) : nameof(DbSet<T>.OrderBy)))
                             && m.IsGenericMethodDefinition);

                var generic = targetMethod.MakeGenericMethod(keyType);
                return (DbSet<T>)generic.Invoke(source, new object[] { lambda })!;
            }

            if (name == nameof(Queryable.Skip))
            {
                var source = BuildSet(mce.Arguments[0]);
                var count = EvaluateInt(mce.Arguments[1]);
                return source.Skip(count);
            }

            if (name == nameof(Queryable.Take))
            {
                var source = BuildSet(mce.Arguments[0]);
                var count = EvaluateInt(mce.Arguments[1]);
                return source.Take(count);
            }
        }

        throw new NotSupportedException($"Unsupported LINQ expression: {expression.NodeType} ({expression.Type.FullName})");
    }

    private static Expression StripQuotes(Expression e)
    {
        while (e.NodeType == ExpressionType.Quote)
        {
            e = ((UnaryExpression)e).Operand;
        }
        return e;
    }

    private static int EvaluateInt(Expression e)
    {
        if (e is ConstantExpression ce && ce.Value is int i) return i;
        var lambda = Expression.Lambda(e);
        var del = lambda.Compile();
        return (int)del.DynamicInvoke()!;
    }
}

internal sealed class MicroOrmQueryable<T> : IOrderedQueryable<T>
{
    public MicroOrmQueryable(IQueryProvider provider, Expression expression)
    {
        Provider = provider;
        Expression = expression;
    }

    public Type ElementType => typeof(T);
    public Expression Expression { get; }
    public IQueryProvider Provider { get; }

    public IEnumerator<T> GetEnumerator()
    {
        var enumerable = Provider.Execute<IEnumerable<T>>(Expression);
        return enumerable.GetEnumerator();
    }

    IEnumerator IEnumerable.GetEnumerator() => GetEnumerator();
}
