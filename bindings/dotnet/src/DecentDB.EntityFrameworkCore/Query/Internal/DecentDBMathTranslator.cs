using System.Reflection;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Diagnostics;
using Microsoft.EntityFrameworkCore.Query;
using Microsoft.EntityFrameworkCore.Query.SqlExpressions;

namespace DecentDB.EntityFrameworkCore.Query.Internal;

public sealed class DecentDBMathTranslator : IMethodCallTranslator
{
    private static readonly Dictionary<MethodInfo, string> MethodToSql = new()
    {
        { typeof(Math).GetRuntimeMethod(nameof(Math.Abs), [typeof(double)])!, "ABS" },
        { typeof(Math).GetRuntimeMethod(nameof(Math.Abs), [typeof(float)])!, "ABS" },
        { typeof(Math).GetRuntimeMethod(nameof(Math.Abs), [typeof(int)])!, "ABS" },
        { typeof(Math).GetRuntimeMethod(nameof(Math.Abs), [typeof(long)])!, "ABS" },
        { typeof(Math).GetRuntimeMethod(nameof(Math.Abs), [typeof(decimal)])!, "ABS" },
        { typeof(Math).GetRuntimeMethod(nameof(Math.Abs), [typeof(short)])!, "ABS" },
        { typeof(Math).GetRuntimeMethod(nameof(Math.Ceiling), [typeof(double)])!, "CEIL" },
        { typeof(Math).GetRuntimeMethod(nameof(Math.Ceiling), [typeof(decimal)])!, "CEIL" },
        { typeof(Math).GetRuntimeMethod(nameof(Math.Floor), [typeof(double)])!, "FLOOR" },
        { typeof(Math).GetRuntimeMethod(nameof(Math.Floor), [typeof(decimal)])!, "FLOOR" },
        { typeof(MathF).GetRuntimeMethod(nameof(MathF.Abs), [typeof(float)])!, "ABS" },
        { typeof(MathF).GetRuntimeMethod(nameof(MathF.Ceiling), [typeof(float)])!, "CEIL" },
        { typeof(MathF).GetRuntimeMethod(nameof(MathF.Floor), [typeof(float)])!, "FLOOR" },
    };

    private static readonly MethodInfo MathRoundDouble2
        = typeof(Math).GetRuntimeMethod(nameof(Math.Round), [typeof(double), typeof(int)])!;

    private static readonly MethodInfo MathRoundDouble1
        = typeof(Math).GetRuntimeMethod(nameof(Math.Round), [typeof(double)])!;

    private static readonly MethodInfo MathRoundDecimal2
        = typeof(Math).GetRuntimeMethod(nameof(Math.Round), [typeof(decimal), typeof(int)])!;

    private static readonly MethodInfo MathRoundDecimal1
        = typeof(Math).GetRuntimeMethod(nameof(Math.Round), [typeof(decimal)])!;

    private static readonly MethodInfo MathMaxInt
        = typeof(Math).GetRuntimeMethod(nameof(Math.Max), [typeof(int), typeof(int)])!;

    private static readonly MethodInfo MathMaxLong
        = typeof(Math).GetRuntimeMethod(nameof(Math.Max), [typeof(long), typeof(long)])!;

    private static readonly MethodInfo MathMaxDouble
        = typeof(Math).GetRuntimeMethod(nameof(Math.Max), [typeof(double), typeof(double)])!;

    private static readonly MethodInfo MathMinInt
        = typeof(Math).GetRuntimeMethod(nameof(Math.Min), [typeof(int), typeof(int)])!;

    private static readonly MethodInfo MathMinLong
        = typeof(Math).GetRuntimeMethod(nameof(Math.Min), [typeof(long), typeof(long)])!;

    private static readonly MethodInfo MathMinDouble
        = typeof(Math).GetRuntimeMethod(nameof(Math.Min), [typeof(double), typeof(double)])!;

    private readonly ISqlExpressionFactory _sqlExpressionFactory;

    public DecentDBMathTranslator(ISqlExpressionFactory sqlExpressionFactory)
    {
        _sqlExpressionFactory = sqlExpressionFactory;
    }

    public SqlExpression? Translate(
        SqlExpression? instance,
        MethodInfo method,
        IReadOnlyList<SqlExpression> arguments,
        IDiagnosticsLogger<DbLoggerCategory.Query> logger)
    {
        // Single-argument functions: ABS, CEIL, FLOOR
        if (MethodToSql.TryGetValue(method, out var sqlFunc))
        {
            return _sqlExpressionFactory.Function(sqlFunc, arguments, nullable: true,
                argumentsPropagateNullability: [true], method.ReturnType);
        }

        // ROUND with 1 or 2 arguments
        if (method.Equals(MathRoundDouble1) || method.Equals(MathRoundDecimal1))
        {
            return _sqlExpressionFactory.Function("ROUND", arguments, nullable: true,
                argumentsPropagateNullability: [true], method.ReturnType);
        }
        if (method.Equals(MathRoundDouble2) || method.Equals(MathRoundDecimal2))
        {
            return _sqlExpressionFactory.Function("ROUND", arguments, nullable: true,
                argumentsPropagateNullability: [true, true], method.ReturnType);
        }

        // MAX / MIN — translate to CASE WHEN for 2-arg scalar max/min
        if (method.Equals(MathMaxInt) || method.Equals(MathMaxLong) || method.Equals(MathMaxDouble))
        {
            return _sqlExpressionFactory.Case(
                [new CaseWhenClause(
                    _sqlExpressionFactory.GreaterThan(arguments[0], arguments[1]),
                    arguments[0])],
                arguments[1]);
        }
        if (method.Equals(MathMinInt) || method.Equals(MathMinLong) || method.Equals(MathMinDouble))
        {
            return _sqlExpressionFactory.Case(
                [new CaseWhenClause(
                    _sqlExpressionFactory.LessThan(arguments[0], arguments[1]),
                    arguments[0])],
                arguments[1]);
        }

        return null;
    }
}
