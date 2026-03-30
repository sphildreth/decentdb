using System.Reflection;
using DecentDB.EntityFrameworkCore.Query.Internal.SqlExpressions;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Diagnostics;
using Microsoft.EntityFrameworkCore.Query;
using Microsoft.EntityFrameworkCore.Query.SqlExpressions;
using Microsoft.EntityFrameworkCore.Storage;

namespace DecentDB.EntityFrameworkCore.Query.Internal;

public sealed class DecentDBWindowFunctionTranslator : IMethodCallTranslator
{
    private static readonly MethodInfo RowNumberOrderMethod
        = GetMethod(nameof(DecentDBDbFunctionsExtensions.RowNumber), genericParameterCount: 1, parameterCount: 3);

    private static readonly MethodInfo RowNumberPartitionMethod
        = GetMethod(nameof(DecentDBDbFunctionsExtensions.RowNumber), genericParameterCount: 2, parameterCount: 4);

    private static readonly MethodInfo RankOrderMethod
        = GetMethod(nameof(DecentDBDbFunctionsExtensions.Rank), genericParameterCount: 1, parameterCount: 3);

    private static readonly MethodInfo RankPartitionMethod
        = GetMethod(nameof(DecentDBDbFunctionsExtensions.Rank), genericParameterCount: 2, parameterCount: 4);

    private static readonly MethodInfo DenseRankOrderMethod
        = GetMethod(nameof(DecentDBDbFunctionsExtensions.DenseRank), genericParameterCount: 1, parameterCount: 3);

    private static readonly MethodInfo DenseRankPartitionMethod
        = GetMethod(nameof(DecentDBDbFunctionsExtensions.DenseRank), genericParameterCount: 2, parameterCount: 4);

    private static readonly MethodInfo PercentRankOrderMethod
        = GetMethod(nameof(DecentDBDbFunctionsExtensions.PercentRank), genericParameterCount: 1, parameterCount: 3);

    private static readonly MethodInfo PercentRankPartitionMethod
        = GetMethod(nameof(DecentDBDbFunctionsExtensions.PercentRank), genericParameterCount: 2, parameterCount: 4);

    private static readonly MethodInfo LagOrderMethod
        = GetMethod(nameof(DecentDBDbFunctionsExtensions.Lag), genericParameterCount: 2, parameterCount: 6);

    private static readonly MethodInfo LagPartitionMethod
        = GetMethod(nameof(DecentDBDbFunctionsExtensions.Lag), genericParameterCount: 3, parameterCount: 7);

    private static readonly MethodInfo LeadOrderMethod
        = GetMethod(nameof(DecentDBDbFunctionsExtensions.Lead), genericParameterCount: 2, parameterCount: 6);

    private static readonly MethodInfo LeadPartitionMethod
        = GetMethod(nameof(DecentDBDbFunctionsExtensions.Lead), genericParameterCount: 3, parameterCount: 7);

    private static readonly MethodInfo FirstValueOrderMethod
        = GetMethod(nameof(DecentDBDbFunctionsExtensions.FirstValue), genericParameterCount: 2, parameterCount: 4);

    private static readonly MethodInfo FirstValuePartitionMethod
        = GetMethod(nameof(DecentDBDbFunctionsExtensions.FirstValue), genericParameterCount: 3, parameterCount: 5);

    private static readonly MethodInfo LastValueOrderMethod
        = GetMethod(nameof(DecentDBDbFunctionsExtensions.LastValue), genericParameterCount: 2, parameterCount: 4);

    private static readonly MethodInfo LastValuePartitionMethod
        = GetMethod(nameof(DecentDBDbFunctionsExtensions.LastValue), genericParameterCount: 3, parameterCount: 5);

    private static readonly MethodInfo NthValueOrderMethod
        = GetMethod(nameof(DecentDBDbFunctionsExtensions.NthValue), genericParameterCount: 2, parameterCount: 5);

    private static readonly MethodInfo NthValuePartitionMethod
        = GetMethod(nameof(DecentDBDbFunctionsExtensions.NthValue), genericParameterCount: 3, parameterCount: 6);

    private readonly ISqlExpressionFactory _sqlExpressionFactory;

    public DecentDBWindowFunctionTranslator(ISqlExpressionFactory sqlExpressionFactory)
    {
        _sqlExpressionFactory = sqlExpressionFactory;
    }

    public SqlExpression? Translate(
        SqlExpression? instance,
        MethodInfo method,
        IReadOnlyList<SqlExpression> arguments,
        IDiagnosticsLogger<DbLoggerCategory.Query> logger)
    {
        var definition = method.IsGenericMethod ? method.GetGenericMethodDefinition() : method;

        if (definition == RowNumberOrderMethod)
        {
            return TranslateRanking("ROW_NUMBER", arguments, partitionByIndex: null, orderByIndex: 1, descendingIndex: 2, typeof(long), (RelationalTypeMapping?)_sqlExpressionFactory.ApplyDefaultTypeMapping(_sqlExpressionFactory.Constant(0L)).TypeMapping);
        }

        if (definition == RowNumberPartitionMethod)
        {
            return TranslateRanking("ROW_NUMBER", arguments, partitionByIndex: 1, orderByIndex: 2, descendingIndex: 3, typeof(long), (RelationalTypeMapping?)_sqlExpressionFactory.ApplyDefaultTypeMapping(_sqlExpressionFactory.Constant(0L)).TypeMapping);
        }

        if (definition == RankOrderMethod)
        {
            return TranslateRanking("RANK", arguments, partitionByIndex: null, orderByIndex: 1, descendingIndex: 2, typeof(long), (RelationalTypeMapping?)_sqlExpressionFactory.ApplyDefaultTypeMapping(_sqlExpressionFactory.Constant(0L)).TypeMapping);
        }

        if (definition == RankPartitionMethod)
        {
            return TranslateRanking("RANK", arguments, partitionByIndex: 1, orderByIndex: 2, descendingIndex: 3, typeof(long), (RelationalTypeMapping?)_sqlExpressionFactory.ApplyDefaultTypeMapping(_sqlExpressionFactory.Constant(0L)).TypeMapping);
        }

        if (definition == DenseRankOrderMethod)
        {
            return TranslateRanking("DENSE_RANK", arguments, partitionByIndex: null, orderByIndex: 1, descendingIndex: 2, typeof(long), (RelationalTypeMapping?)_sqlExpressionFactory.ApplyDefaultTypeMapping(_sqlExpressionFactory.Constant(0L)).TypeMapping);
        }

        if (definition == DenseRankPartitionMethod)
        {
            return TranslateRanking("DENSE_RANK", arguments, partitionByIndex: 1, orderByIndex: 2, descendingIndex: 3, typeof(long), (RelationalTypeMapping?)_sqlExpressionFactory.ApplyDefaultTypeMapping(_sqlExpressionFactory.Constant(0L)).TypeMapping);
        }

        if (definition == PercentRankOrderMethod)
        {
            return TranslateRanking("PERCENT_RANK", arguments, partitionByIndex: null, orderByIndex: 1, descendingIndex: 2, typeof(double), (RelationalTypeMapping?)_sqlExpressionFactory.ApplyDefaultTypeMapping(_sqlExpressionFactory.Constant(0.0)).TypeMapping);
        }

        if (definition == PercentRankPartitionMethod)
        {
            return TranslateRanking("PERCENT_RANK", arguments, partitionByIndex: 1, orderByIndex: 2, descendingIndex: 3, typeof(double), (RelationalTypeMapping?)_sqlExpressionFactory.ApplyDefaultTypeMapping(_sqlExpressionFactory.Constant(0.0)).TypeMapping);
        }

        if (definition == LagOrderMethod)
        {
            return TranslateValueWindow(
                "LAG",
                arguments,
                partitionByIndex: null,
                valueIndex: 1,
                extraArgumentIndexes: [4, 3],
                orderByIndex: 2,
                descendingIndex: 5);
        }

        if (definition == LagPartitionMethod)
        {
            return TranslateValueWindow(
                "LAG",
                arguments,
                partitionByIndex: 1,
                valueIndex: 2,
                extraArgumentIndexes: [5, 4],
                orderByIndex: 3,
                descendingIndex: 6);
        }

        if (definition == LeadOrderMethod)
        {
            return TranslateValueWindow(
                "LEAD",
                arguments,
                partitionByIndex: null,
                valueIndex: 1,
                extraArgumentIndexes: [4, 3],
                orderByIndex: 2,
                descendingIndex: 5);
        }

        if (definition == LeadPartitionMethod)
        {
            return TranslateValueWindow(
                "LEAD",
                arguments,
                partitionByIndex: 1,
                valueIndex: 2,
                extraArgumentIndexes: [5, 4],
                orderByIndex: 3,
                descendingIndex: 6);
        }

        if (definition == FirstValueOrderMethod)
        {
            return TranslateValueWindow(
                "FIRST_VALUE",
                arguments,
                partitionByIndex: null,
                valueIndex: 1,
                extraArgumentIndexes: [],
                orderByIndex: 2,
                descendingIndex: 3);
        }

        if (definition == FirstValuePartitionMethod)
        {
            return TranslateValueWindow(
                "FIRST_VALUE",
                arguments,
                partitionByIndex: 1,
                valueIndex: 2,
                extraArgumentIndexes: [],
                orderByIndex: 3,
                descendingIndex: 4);
        }

        if (definition == LastValueOrderMethod)
        {
            return TranslateValueWindow(
                "LAST_VALUE",
                arguments,
                partitionByIndex: null,
                valueIndex: 1,
                extraArgumentIndexes: [],
                orderByIndex: 2,
                descendingIndex: 3);
        }

        if (definition == LastValuePartitionMethod)
        {
            return TranslateValueWindow(
                "LAST_VALUE",
                arguments,
                partitionByIndex: 1,
                valueIndex: 2,
                extraArgumentIndexes: [],
                orderByIndex: 3,
                descendingIndex: 4);
        }

        if (definition == NthValueOrderMethod)
        {
            return TranslateValueWindow(
                "NTH_VALUE",
                arguments,
                partitionByIndex: null,
                valueIndex: 1,
                extraArgumentIndexes: [2],
                orderByIndex: 3,
                descendingIndex: 4);
        }

        if (definition == NthValuePartitionMethod)
        {
            return TranslateValueWindow(
                "NTH_VALUE",
                arguments,
                partitionByIndex: 1,
                valueIndex: 2,
                extraArgumentIndexes: [3],
                orderByIndex: 4,
                descendingIndex: 5);
        }

        return null;
    }

    private static MethodInfo GetMethod(string name, int genericParameterCount, int parameterCount)
        => typeof(DecentDBDbFunctionsExtensions)
            .GetMethods(BindingFlags.Public | BindingFlags.Static)
            .Single(method => method.Name == name
                && method.IsGenericMethodDefinition
                && method.GetGenericArguments().Length == genericParameterCount
                && method.GetParameters().Length == parameterCount);

    private static bool? TryGetBool(SqlExpression expression)
        => expression is SqlConstantExpression { Value: bool value } ? value : null;

    private SqlExpression? TranslateRanking(
        string functionName,
        IReadOnlyList<SqlExpression> arguments,
        int? partitionByIndex,
        int orderByIndex,
        int descendingIndex,
        Type returnType,
        RelationalTypeMapping? typeMapping)
    {
        var descending = TryGetBool(arguments[descendingIndex]);
        if (descending is null)
        {
            return null;
        }

        return new WindowFunctionExpression(
            functionName,
            [],
            partitionByIndex is null ? null : _sqlExpressionFactory.ApplyDefaultTypeMapping(arguments[partitionByIndex.Value]),
            _sqlExpressionFactory.ApplyDefaultTypeMapping(arguments[orderByIndex]),
            descending.Value,
            returnType,
            typeMapping);
    }

    private SqlExpression? TranslateValueWindow(
        string functionName,
        IReadOnlyList<SqlExpression> arguments,
        int? partitionByIndex,
        int valueIndex,
        int[] extraArgumentIndexes,
        int orderByIndex,
        int descendingIndex)
    {
        var descending = TryGetBool(arguments[descendingIndex]);
        if (descending is null)
        {
            return null;
        }

        var functionArguments = new SqlExpression[1 + extraArgumentIndexes.Length];
        functionArguments[0] = _sqlExpressionFactory.ApplyDefaultTypeMapping(arguments[valueIndex]);
        for (var i = 0; i < extraArgumentIndexes.Length; i++)
        {
            functionArguments[i + 1] = _sqlExpressionFactory.ApplyDefaultTypeMapping(arguments[extraArgumentIndexes[i]]);
        }

        return new WindowFunctionExpression(
            functionName,
            functionArguments,
            partitionByIndex is null ? null : _sqlExpressionFactory.ApplyDefaultTypeMapping(arguments[partitionByIndex.Value]),
            _sqlExpressionFactory.ApplyDefaultTypeMapping(arguments[orderByIndex]),
            descending.Value,
            arguments[valueIndex].Type,
            arguments[valueIndex].TypeMapping);
    }
}
