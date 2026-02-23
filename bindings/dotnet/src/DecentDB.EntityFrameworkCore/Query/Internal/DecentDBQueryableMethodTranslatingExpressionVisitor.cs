using DecentDB.EntityFrameworkCore.Query.Internal.SqlExpressions;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Query;
using Microsoft.EntityFrameworkCore.Query.SqlExpressions;
using Microsoft.EntityFrameworkCore.Storage;

namespace DecentDB.EntityFrameworkCore.Query.Internal;

// Cached nullability arrays (Statics.TrueArrays is internal to EF Core)
file static class NullabilityArrays
{
    internal static readonly bool[] SingleTrue = [true];
}

/// <summary>
/// Translates primitive collections (e.g. string[] stored as JSON) to SQL
/// using json_each/json_array_length functions.
/// </summary>
public class DecentDBQueryableMethodTranslatingExpressionVisitor
    : RelationalQueryableMethodTranslatingExpressionVisitor
{
    private readonly IRelationalTypeMappingSource _typeMappingSource;
    private readonly ISqlExpressionFactory _sqlExpressionFactory;
    private readonly SqlAliasManager _sqlAliasManager;

    public DecentDBQueryableMethodTranslatingExpressionVisitor(
        QueryableMethodTranslatingExpressionVisitorDependencies dependencies,
        RelationalQueryableMethodTranslatingExpressionVisitorDependencies relationalDependencies,
        RelationalQueryCompilationContext queryCompilationContext)
        : base(dependencies, relationalDependencies, queryCompilationContext)
    {
        _typeMappingSource = relationalDependencies.TypeMappingSource;
        _sqlExpressionFactory = relationalDependencies.SqlExpressionFactory;
        _sqlAliasManager = queryCompilationContext.SqlAliasManager;
    }

    protected DecentDBQueryableMethodTranslatingExpressionVisitor(
        DecentDBQueryableMethodTranslatingExpressionVisitor parentVisitor)
        : base(parentVisitor)
    {
        _typeMappingSource = parentVisitor._typeMappingSource;
        _sqlExpressionFactory = parentVisitor._sqlExpressionFactory;
        _sqlAliasManager = parentVisitor._sqlAliasManager;
    }

    protected override QueryableMethodTranslatingExpressionVisitor CreateSubqueryVisitor()
        => new DecentDBQueryableMethodTranslatingExpressionVisitor(this);

    /// <summary>
    /// Optimizes .Any() on a primitive collection to json_array_length() > 0
    /// instead of EXISTS (SELECT 1 FROM json_each(...)).
    /// </summary>
    protected override ShapedQueryExpression? TranslateAny(
        ShapedQueryExpression source,
        System.Linq.Expressions.LambdaExpression? predicate)
    {
        if (predicate is null
            && source.QueryExpression is SelectExpression
            {
                Tables: [JsonEachExpression jsonEach],
                Predicate: null,
                GroupBy: [],
                Having: null,
                IsDistinct: false,
                Limit: null,
                Offset: null
            })
        {
            var translation =
                _sqlExpressionFactory.GreaterThan(
                    _sqlExpressionFactory.Function(
                        "json_array_length",
                        [jsonEach.Json],
                        nullable: true,
                        argumentsPropagateNullability: NullabilityArrays.SingleTrue,
                        typeof(int)),
                    _sqlExpressionFactory.Constant(0));

#pragma warning disable EF1001
            return source.UpdateQueryExpression(new SelectExpression(translation, _sqlAliasManager));
#pragma warning restore EF1001
        }

        return base.TranslateAny(source, predicate);
    }

    /// <summary>
    /// Optimizes .Count() on a primitive collection to json_array_length()
    /// instead of SELECT COUNT(*) FROM json_each(...).
    /// </summary>
    protected override ShapedQueryExpression? TranslateCount(
        ShapedQueryExpression source,
        System.Linq.Expressions.LambdaExpression? predicate)
    {
        if (predicate is null
            && source.QueryExpression is SelectExpression
            {
                Tables: [JsonEachExpression jsonEach],
                Predicate: null,
                GroupBy: [],
                Having: null,
                IsDistinct: false,
                Limit: null,
                Offset: null
            })
        {
            var translation = _sqlExpressionFactory.Function(
                "json_array_length",
                [jsonEach.Json],
                nullable: true,
                argumentsPropagateNullability: NullabilityArrays.SingleTrue,
                typeof(int));

#pragma warning disable EF1001
            return source.UpdateQueryExpression(new SelectExpression(translation, _sqlAliasManager));
#pragma warning restore EF1001
        }

        return base.TranslateCount(source, predicate);
    }

    /// <summary>
    /// Optimizes array.Contains(value) on a string[] primitive collection to
    /// column LIKE '%"' || value || '"%' instead of EXISTS (SELECT FROM json_each(...)).
    /// </summary>
    protected override ShapedQueryExpression? TranslateContains(
        ShapedQueryExpression source,
        System.Linq.Expressions.Expression item)
    {
        if (source.QueryExpression is SelectExpression
            {
                Tables: [JsonEachExpression jsonEach],
                Predicate: null,
                GroupBy: [],
                Having: null,
                IsDistinct: false,
                Limit: null,
                Offset: null
            }
            && TranslateExpression(item) is SqlExpression translatedItem
            && translatedItem.Type == typeof(string))
        {
            var stringMapping = (RelationalTypeMapping)_typeMappingSource.FindMapping(typeof(string))!;

            // Build: '%"' || @value || '"%'
            var pattern = _sqlExpressionFactory.Add(
                _sqlExpressionFactory.Add(
                    _sqlExpressionFactory.Constant("%\"", stringMapping),
                    translatedItem,
                    stringMapping),
                _sqlExpressionFactory.Constant("\"%", stringMapping),
                stringMapping);

            var translation = _sqlExpressionFactory.Like(jsonEach.Json, pattern);

#pragma warning disable EF1001
            return source.UpdateQueryExpression(new SelectExpression(translation, _sqlAliasManager));
#pragma warning restore EF1001
        }

        return base.TranslateContains(source, item);
    }

    /// <summary>
    /// Optimizes array[index] on a primitive collection to json_extract(column, '$[index]')
    /// instead of a json_each() subquery with LIMIT/OFFSET.
    /// </summary>
    protected override ShapedQueryExpression? TranslateElementAtOrDefault(
        ShapedQueryExpression source,
        System.Linq.Expressions.Expression index,
        bool returnDefault)
    {
        if (!returnDefault
            && source.QueryExpression is SelectExpression
            {
                Tables: [JsonEachExpression jsonEach],
                Predicate: null,
                GroupBy: [],
                Having: null,
                IsDistinct: false,
                Limit: null,
                Offset: null
            } selectExpression
            && TranslateExpression(index) is SqlConstantExpression { Value: int indexValue })
        {
            var shaperExpression = source.ShaperExpression;
            if (shaperExpression is System.Linq.Expressions.UnaryExpression
                {
                    NodeType: System.Linq.Expressions.ExpressionType.Convert
                } unaryExpression
                && unaryExpression.Operand.Type.IsValueType
                && Nullable.GetUnderlyingType(unaryExpression.Operand.Type) is not null)
            {
                shaperExpression = unaryExpression.Operand;
            }

            if (shaperExpression is ProjectionBindingExpression projectionBindingExpression
                && selectExpression.GetProjection(projectionBindingExpression) is ColumnExpression projectionColumn)
            {
                var translation = _sqlExpressionFactory.Function(
                    "json_extract",
                    [jsonEach.Json, _sqlExpressionFactory.Constant($"$[{indexValue}]")],
                    nullable: true,
                    argumentsPropagateNullability: [true, false],
                    projectionColumn.Type,
                    projectionColumn.TypeMapping);

#pragma warning disable EF1001
                return source.UpdateQueryExpression(new SelectExpression(translation, _sqlAliasManager));
#pragma warning restore EF1001
            }
        }

        return base.TranslateElementAtOrDefault(source, index, returnDefault);
    }

    /// <summary>
    /// Translates a primitive collection (e.g. string[] column stored as JSON)
    /// into a queryable json_each() table expression.
    /// </summary>
    protected override ShapedQueryExpression? TranslatePrimitiveCollection(
        SqlExpression sqlExpression,
        Microsoft.EntityFrameworkCore.Metadata.IProperty? property,
        string tableAlias)
    {
        var elementTypeMapping = (RelationalTypeMapping?)sqlExpression.TypeMapping?.ElementTypeMapping;
        var elementClrType = GetElementClrType(sqlExpression.Type);
        var jsonEachExpression = new JsonEachExpression(tableAlias, sqlExpression);

        var isElementNullable = property?.GetElementType()!.IsNullable;
        var keyColumnTypeMapping = _typeMappingSource.FindMapping(typeof(int))!;
        var unwrappedElementType = Nullable.GetUnderlyingType(elementClrType) ?? elementClrType;
        var isNullable = isElementNullable ?? (Nullable.GetUnderlyingType(elementClrType) is not null);
        var nullableElementType = elementClrType.IsValueType && Nullable.GetUnderlyingType(elementClrType) is null
            ? typeof(Nullable<>).MakeGenericType(elementClrType)
            : elementClrType;

#pragma warning disable EF1001
        var selectExpression = new SelectExpression(
            [jsonEachExpression],
            new ColumnExpression(
                JsonEachExpression.ValueColumnName,
                tableAlias,
                unwrappedElementType,
                elementTypeMapping,
                isNullable),
            identifier:
            [
                (new ColumnExpression(
                    JsonEachExpression.KeyColumnName,
                    tableAlias,
                    typeof(int),
                    keyColumnTypeMapping,
                    nullable: false),
                    keyColumnTypeMapping.Comparer)
            ],
            _sqlAliasManager);
#pragma warning restore EF1001

        selectExpression.AppendOrdering(
            new OrderingExpression(
                selectExpression.CreateColumnExpression(
                    jsonEachExpression,
                    JsonEachExpression.KeyColumnName,
                    typeof(int),
                    typeMapping: _typeMappingSource.FindMapping(typeof(int)),
                    columnNullable: false),
                ascending: true));

        System.Linq.Expressions.Expression shaperExpression =
            new ProjectionBindingExpression(selectExpression, new ProjectionMember(), nullableElementType);

        if (elementClrType != shaperExpression.Type)
        {
            shaperExpression = System.Linq.Expressions.Expression.Convert(shaperExpression, elementClrType);
        }

        return new ShapedQueryExpression(selectExpression, shaperExpression);
    }

    private static Type GetElementClrType(Type collectionType)
    {
        if (collectionType.IsArray)
        {
            return collectionType.GetElementType()!;
        }

        if (collectionType.IsGenericType)
        {
            return collectionType.GetGenericArguments()[0];
        }

        return collectionType;
    }
}
