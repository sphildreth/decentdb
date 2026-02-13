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
        if (instance is null || arguments.Count != 1)
        {
            return null;
        }

        if (arguments[0] is not SqlConstantExpression { Value: string patternValue })
        {
            return null;
        }

        string pattern;
        if (method.Equals(StringContainsMethod))
        {
            pattern = $"%{EscapeLikePattern(patternValue)}%";
        }
        else if (method.Equals(StringStartsWithMethod))
        {
            pattern = $"{EscapeLikePattern(patternValue)}%";
        }
        else if (method.Equals(StringEndsWithMethod))
        {
            pattern = $"%{EscapeLikePattern(patternValue)}";
        }
        else
        {
            return null;
        }

        return _sqlExpressionFactory.Like(
            instance,
            _sqlExpressionFactory.Constant(pattern),
            _sqlExpressionFactory.Constant("\\"));
    }

    private static string EscapeLikePattern(string value)
        => value.Replace("\\", "\\\\", StringComparison.Ordinal)
            .Replace("%", "\\%", StringComparison.Ordinal)
            .Replace("_", "\\_", StringComparison.Ordinal);
}
