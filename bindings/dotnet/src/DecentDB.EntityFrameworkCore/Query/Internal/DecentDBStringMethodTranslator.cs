using System.Reflection;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Diagnostics;
using Microsoft.EntityFrameworkCore.Query;
using Microsoft.EntityFrameworkCore.Query.SqlExpressions;

namespace DecentDB.EntityFrameworkCore.Query.Internal;

public sealed class DecentDBStringMethodTranslator : IMethodCallTranslator
{
    private static readonly MethodInfo StringContainsMethod
        = typeof(string).GetRuntimeMethod(nameof(string.Contains), [typeof(string)])!;

    private static readonly MethodInfo StringStartsWithMethod
        = typeof(string).GetRuntimeMethod(nameof(string.StartsWith), [typeof(string)])!;

    private static readonly MethodInfo StringEndsWithMethod
        = typeof(string).GetRuntimeMethod(nameof(string.EndsWith), [typeof(string)])!;

    private static readonly MethodInfo StringToUpperMethod
        = typeof(string).GetRuntimeMethod(nameof(string.ToUpper), Type.EmptyTypes)!;

    private static readonly MethodInfo StringToLowerMethod
        = typeof(string).GetRuntimeMethod(nameof(string.ToLower), Type.EmptyTypes)!;

    private static readonly MethodInfo StringTrimMethod
        = typeof(string).GetRuntimeMethod(nameof(string.Trim), Type.EmptyTypes)!;

    private static readonly MethodInfo StringTrimStartMethod
        = typeof(string).GetRuntimeMethod(nameof(string.TrimStart), Type.EmptyTypes)!;

    private static readonly MethodInfo StringTrimEndMethod
        = typeof(string).GetRuntimeMethod(nameof(string.TrimEnd), Type.EmptyTypes)!;

    private static readonly MethodInfo StringSubstringMethod1
        = typeof(string).GetRuntimeMethod(nameof(string.Substring), [typeof(int)])!;

    private static readonly MethodInfo StringSubstringMethod2
        = typeof(string).GetRuntimeMethod(nameof(string.Substring), [typeof(int), typeof(int)])!;

    private static readonly MethodInfo StringReplaceMethod
        = typeof(string).GetRuntimeMethod(nameof(string.Replace), [typeof(string), typeof(string)])!;

    private readonly ISqlExpressionFactory _sqlExpressionFactory;

    public DecentDBStringMethodTranslator(ISqlExpressionFactory sqlExpressionFactory)
    {
        _sqlExpressionFactory = sqlExpressionFactory;
    }

    public SqlExpression? Translate(
        SqlExpression? instance,
        MethodInfo method,
        IReadOnlyList<SqlExpression> arguments,
        IDiagnosticsLogger<DbLoggerCategory.Query> logger)
    {
        if (instance is null)
        {
            return null;
        }

        // LIKE-based methods: Contains, StartsWith, EndsWith
        if (arguments.Count == 1)
        {
            var argument = arguments[0];

            if (argument is SqlConstantExpression { Value: string patternValue })
            {
                string? pattern = null;
                if (method.Equals(StringContainsMethod))
                    pattern = $"%{EscapeLikePattern(patternValue)}%";
                else if (method.Equals(StringStartsWithMethod))
                    pattern = $"{EscapeLikePattern(patternValue)}%";
                else if (method.Equals(StringEndsWithMethod))
                    pattern = $"%{EscapeLikePattern(patternValue)}";

                if (pattern is not null)
                {
                    return _sqlExpressionFactory.Like(
                        instance,
                        _sqlExpressionFactory.Constant(pattern),
                        _sqlExpressionFactory.Constant("\\"));
                }
            }

            // Parameterized LIKE patterns
            if (method.Equals(StringContainsMethod))
            {
                return _sqlExpressionFactory.Like(
                    instance,
                    _sqlExpressionFactory.Add(
                        _sqlExpressionFactory.Add(
                            _sqlExpressionFactory.Constant("%"), argument),
                        _sqlExpressionFactory.Constant("%")));
            }
            if (method.Equals(StringStartsWithMethod))
            {
                return _sqlExpressionFactory.Like(
                    instance,
                    _sqlExpressionFactory.Add(argument, _sqlExpressionFactory.Constant("%")));
            }
            if (method.Equals(StringEndsWithMethod))
            {
                return _sqlExpressionFactory.Like(
                    instance,
                    _sqlExpressionFactory.Add(_sqlExpressionFactory.Constant("%"), argument));
            }
        }

        // UPPER / LOWER / TRIM
        if (method.Equals(StringToUpperMethod))
            return _sqlExpressionFactory.Function("UPPER", [instance], nullable: true,
                argumentsPropagateNullability: [true], typeof(string), instance.TypeMapping);
        if (method.Equals(StringToLowerMethod))
            return _sqlExpressionFactory.Function("LOWER", [instance], nullable: true,
                argumentsPropagateNullability: [true], typeof(string), instance.TypeMapping);
        if (method.Equals(StringTrimMethod) || method.Equals(StringTrimStartMethod) || method.Equals(StringTrimEndMethod))
            return _sqlExpressionFactory.Function("TRIM", [instance], nullable: true,
                argumentsPropagateNullability: [true], typeof(string), instance.TypeMapping);

        // REPLACE(instance, old, new)
        if (method.Equals(StringReplaceMethod) && arguments.Count == 2)
            return _sqlExpressionFactory.Function("REPLACE", [instance, arguments[0], arguments[1]], nullable: true,
                argumentsPropagateNullability: [true, true, true], typeof(string), instance.TypeMapping);

        // SUBSTRING(instance, start+1) or SUBSTRING(instance, start+1, length)
        if (method.Equals(StringSubstringMethod1) && arguments.Count == 1)
        {
            // .NET Substring is 0-based; SQL SUBSTRING is 1-based
            return _sqlExpressionFactory.Function("SUBSTRING", new[]
            {
                instance,
                _sqlExpressionFactory.Add(arguments[0], _sqlExpressionFactory.Constant(1))
            }, nullable: true, argumentsPropagateNullability: [true, true], typeof(string), instance.TypeMapping);
        }
        if (method.Equals(StringSubstringMethod2) && arguments.Count == 2)
        {
            return _sqlExpressionFactory.Function("SUBSTRING", new[]
            {
                instance,
                _sqlExpressionFactory.Add(arguments[0], _sqlExpressionFactory.Constant(1)),
                arguments[1]
            }, nullable: true, argumentsPropagateNullability: [true, true, true], typeof(string), instance.TypeMapping);
        }

        return null;
    }

    private static string EscapeLikePattern(string value)
        => value.Replace("\\", "\\\\", StringComparison.Ordinal)
            .Replace("%", "\\%", StringComparison.Ordinal)
            .Replace("_", "\\_", StringComparison.Ordinal);
}
