using System.Linq.Expressions;
using System.Reflection;
using DecentDB.EntityFrameworkCore.Query.Internal;
using DecentDB.EntityFrameworkCore.Query.Internal.SqlExpressions;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Infrastructure;
using Microsoft.EntityFrameworkCore.Query;
using Microsoft.EntityFrameworkCore.Query.SqlExpressions;
using Microsoft.EntityFrameworkCore.Storage;
using Xunit;

namespace DecentDB.EntityFrameworkCore.Tests;

public sealed class QueryTranslatorAndSqlExpressionCoverageTests : IDisposable
{
    private readonly string _dbPath = Path.Combine(Path.GetTempPath(), $"test_ef_translators_{Guid.NewGuid():N}.ddb");

    [Fact]
    public void StringMethodTranslator_TranslatesCommonMethodsAndFallbacks()
    {
        using var context = CreateContext();
        var sql = context.GetService<ISqlExpressionFactory>();
        var translator = new DecentDBStringMethodTranslator(sql);
        var instance = sql.Constant("abcdef");
        var parameterLikeArg = new DummySqlExpression(typeof(string));

        Assert.NotNull(translator.Translate(instance, GetMethod<string>(nameof(string.Contains), [typeof(string)]), [sql.Constant("cd")], null!));
        Assert.NotNull(translator.Translate(instance, GetMethod<string>(nameof(string.StartsWith), [typeof(string)]), [sql.Constant("ab")], null!));
        Assert.NotNull(translator.Translate(instance, GetMethod<string>(nameof(string.EndsWith), [typeof(string)]), [sql.Constant("ef")], null!));
        Assert.NotNull(translator.Translate(instance, GetMethod<string>(nameof(string.Contains), [typeof(string)]), [parameterLikeArg], null!));

        Assert.Equal("UPPER", Assert.IsType<SqlFunctionExpression>(translator.Translate(instance, GetMethod<string>(nameof(string.ToUpper), []), [], null!)).Name);
        Assert.Equal("LOWER", Assert.IsType<SqlFunctionExpression>(translator.Translate(instance, GetMethod<string>(nameof(string.ToLower), []), [], null!)).Name);
        Assert.Equal("TRIM", Assert.IsType<SqlFunctionExpression>(translator.Translate(instance, GetMethod<string>(nameof(string.Trim), []), [], null!)).Name);
        Assert.Equal("TRIM", Assert.IsType<SqlFunctionExpression>(translator.Translate(instance, GetMethod<string>(nameof(string.TrimStart), []), [], null!)).Name);
        Assert.Equal("TRIM", Assert.IsType<SqlFunctionExpression>(translator.Translate(instance, GetMethod<string>(nameof(string.TrimEnd), []), [], null!)).Name);
        Assert.Equal("REPLACE", Assert.IsType<SqlFunctionExpression>(translator.Translate(instance, GetMethod<string>(nameof(string.Replace), [typeof(string), typeof(string)]), [sql.Constant("a"), sql.Constant("z")], null!)).Name);
        Assert.Equal("SUBSTRING", Assert.IsType<SqlFunctionExpression>(translator.Translate(instance, GetMethod<string>(nameof(string.Substring), [typeof(int)]), [sql.Constant(1)], null!)).Name);
        Assert.Equal("SUBSTRING", Assert.IsType<SqlFunctionExpression>(translator.Translate(instance, GetMethod<string>(nameof(string.Substring), [typeof(int), typeof(int)]), [sql.Constant(1), sql.Constant(2)], null!)).Name);

        Assert.Null(translator.Translate(null, GetMethod<string>(nameof(string.Contains), [typeof(string)]), [sql.Constant("x")], null!));
        Assert.Null(translator.Translate(instance, GetMethod<object>(nameof(object.GetHashCode), []), [], null!));
    }

    [Fact]
    public void MathTranslator_TranslatesMathAndMathFMethods()
    {
        using var context = CreateContext();
        var sql = context.GetService<ISqlExpressionFactory>();
        var translator = new DecentDBMathTranslator(sql);

        Assert.Equal("ABS", Assert.IsType<SqlFunctionExpression>(translator.Translate(null, GetMethod(typeof(Math), nameof(Math.Abs), [typeof(double)]), [sql.Constant(-5.5d)], null!)).Name);
        Assert.Equal("CEIL", Assert.IsType<SqlFunctionExpression>(translator.Translate(null, GetMethod(typeof(Math), nameof(Math.Ceiling), [typeof(double)]), [sql.Constant(1.2d)], null!)).Name);
        Assert.Equal("FLOOR", Assert.IsType<SqlFunctionExpression>(translator.Translate(null, GetMethod(typeof(Math), nameof(Math.Floor), [typeof(double)]), [sql.Constant(1.2d)], null!)).Name);
        Assert.Equal("ABS", Assert.IsType<SqlFunctionExpression>(translator.Translate(null, GetMethod(typeof(MathF), nameof(MathF.Abs), [typeof(float)]), [sql.Constant(-3.0f)], null!)).Name);
        Assert.Equal("ROUND", Assert.IsType<SqlFunctionExpression>(translator.Translate(null, GetMethod(typeof(Math), nameof(Math.Round), [typeof(double)]), [sql.Constant(1.25d)], null!)).Name);
        Assert.Equal("ROUND", Assert.IsType<SqlFunctionExpression>(translator.Translate(null, GetMethod(typeof(Math), nameof(Math.Round), [typeof(double), typeof(int)]), [sql.Constant(1.25d), sql.Constant(1)], null!)).Name);
        Assert.IsType<CaseExpression>(translator.Translate(null, GetMethod(typeof(Math), nameof(Math.Max), [typeof(int), typeof(int)]), [sql.Constant(3), sql.Constant(2)], null!));
        Assert.IsType<CaseExpression>(translator.Translate(null, GetMethod(typeof(Math), nameof(Math.Min), [typeof(int), typeof(int)]), [sql.Constant(3), sql.Constant(2)], null!));

        Assert.Null(translator.Translate(null, GetMethod<object>(nameof(object.GetHashCode), []), [], null!));
    }

    [Fact]
    public void MemberTranslator_TranslatesStringDateTimeDateOnlyAndTimeOnlyMembers()
    {
        using var context = CreateContext();
        var sql = context.GetService<ISqlExpressionFactory>();
        var translator = new DecentDBMemberTranslator(sql);

        Assert.Equal("LENGTH", Assert.IsType<SqlFunctionExpression>(translator.Translate(sql.Constant("abc"), typeof(string).GetProperty(nameof(string.Length))!, typeof(int), null!)).Name);
        Assert.Equal("DATE_PART", Assert.IsType<SqlFunctionExpression>(translator.Translate(sql.Constant(DateTime.UtcNow), typeof(DateTime).GetProperty(nameof(DateTime.Year))!, typeof(int), null!)).Name);
        Assert.NotNull(translator.Translate(sql.Constant(DateOnly.FromDateTime(DateTime.UtcNow)), typeof(DateOnly).GetProperty(nameof(DateOnly.Year))!, typeof(int), null!));
        Assert.NotNull(translator.Translate(sql.Constant(DateOnly.FromDateTime(DateTime.UtcNow)), typeof(DateOnly).GetProperty(nameof(DateOnly.Month))!, typeof(int), null!));
        Assert.NotNull(translator.Translate(sql.Constant(DateOnly.FromDateTime(DateTime.UtcNow)), typeof(DateOnly).GetProperty(nameof(DateOnly.Day))!, typeof(int), null!));
        Assert.NotNull(translator.Translate(sql.Constant(DateOnly.FromDateTime(DateTime.UtcNow)), typeof(DateOnly).GetProperty(nameof(DateOnly.DayOfYear))!, typeof(int), null!));
        Assert.NotNull(translator.Translate(sql.Constant(TimeOnly.FromDateTime(DateTime.UtcNow)), typeof(TimeOnly).GetProperty(nameof(TimeOnly.Hour))!, typeof(int), null!));
        Assert.NotNull(translator.Translate(sql.Constant(TimeOnly.FromDateTime(DateTime.UtcNow)), typeof(TimeOnly).GetProperty(nameof(TimeOnly.Minute))!, typeof(int), null!));
        Assert.NotNull(translator.Translate(sql.Constant(TimeOnly.FromDateTime(DateTime.UtcNow)), typeof(TimeOnly).GetProperty(nameof(TimeOnly.Second))!, typeof(int), null!));
        Assert.NotNull(translator.Translate(sql.Constant(TimeOnly.FromDateTime(DateTime.UtcNow)), typeof(TimeOnly).GetProperty(nameof(TimeOnly.Millisecond))!, typeof(int), null!));

        Assert.Null(translator.Translate(null, typeof(string).GetProperty(nameof(string.Length))!, typeof(int), null!));
        Assert.Null(translator.Translate(sql.Constant("abc"), typeof(string).GetProperty("Chars")!, typeof(char), null!));
    }

    [Fact]
    public void WindowFunctionTranslator_TranslatesAllSupportedFunctionShapes()
    {
        using var context = CreateContext();
        var sql = context.GetService<ISqlExpressionFactory>();
        var translator = new DecentDBWindowFunctionTranslator(sql);

        AssertWindowFunctionName("ROW_NUMBER", translator, MakeExtensionMethod(nameof(DecentDBDbFunctionsExtensions.RowNumber), 1, 3, typeof(int)), [sql.Constant(0), sql.Constant(5), sql.Constant(false)]);
        AssertWindowFunctionName("ROW_NUMBER", translator, MakeExtensionMethod(nameof(DecentDBDbFunctionsExtensions.RowNumber), 2, 4, typeof(string), typeof(int)), [sql.Constant(0), sql.Constant("dep"), sql.Constant(5), sql.Constant(true)]);
        AssertWindowFunctionName("RANK", translator, MakeExtensionMethod(nameof(DecentDBDbFunctionsExtensions.Rank), 1, 3, typeof(int)), [sql.Constant(0), sql.Constant(5), sql.Constant(false)]);
        AssertWindowFunctionName("RANK", translator, MakeExtensionMethod(nameof(DecentDBDbFunctionsExtensions.Rank), 2, 4, typeof(string), typeof(int)), [sql.Constant(0), sql.Constant("dep"), sql.Constant(5), sql.Constant(false)]);
        AssertWindowFunctionName("DENSE_RANK", translator, MakeExtensionMethod(nameof(DecentDBDbFunctionsExtensions.DenseRank), 1, 3, typeof(int)), [sql.Constant(0), sql.Constant(5), sql.Constant(false)]);
        AssertWindowFunctionName("DENSE_RANK", translator, MakeExtensionMethod(nameof(DecentDBDbFunctionsExtensions.DenseRank), 2, 4, typeof(string), typeof(int)), [sql.Constant(0), sql.Constant("dep"), sql.Constant(5), sql.Constant(false)]);
        AssertWindowFunctionName("PERCENT_RANK", translator, MakeExtensionMethod(nameof(DecentDBDbFunctionsExtensions.PercentRank), 1, 3, typeof(int)), [sql.Constant(0), sql.Constant(5), sql.Constant(false)]);
        AssertWindowFunctionName("PERCENT_RANK", translator, MakeExtensionMethod(nameof(DecentDBDbFunctionsExtensions.PercentRank), 2, 4, typeof(string), typeof(int)), [sql.Constant(0), sql.Constant("dep"), sql.Constant(5), sql.Constant(false)]);
        AssertWindowFunctionName("LAG", translator, MakeExtensionMethod(nameof(DecentDBDbFunctionsExtensions.Lag), 2, 6, typeof(string), typeof(int)), [sql.Constant(0), sql.Constant("value"), sql.Constant(2), sql.Constant("default"), sql.Constant(1), sql.Constant(false)]);
        AssertWindowFunctionName("LAG", translator, MakeExtensionMethod(nameof(DecentDBDbFunctionsExtensions.Lag), 3, 7, typeof(string), typeof(string), typeof(int)), [sql.Constant(0), sql.Constant("dep"), sql.Constant("value"), sql.Constant(2), sql.Constant("default"), sql.Constant(1), sql.Constant(false)]);
        AssertWindowFunctionName("LEAD", translator, MakeExtensionMethod(nameof(DecentDBDbFunctionsExtensions.Lead), 2, 6, typeof(string), typeof(int)), [sql.Constant(0), sql.Constant("value"), sql.Constant(2), sql.Constant("default"), sql.Constant(1), sql.Constant(false)]);
        AssertWindowFunctionName("LEAD", translator, MakeExtensionMethod(nameof(DecentDBDbFunctionsExtensions.Lead), 3, 7, typeof(string), typeof(string), typeof(int)), [sql.Constant(0), sql.Constant("dep"), sql.Constant("value"), sql.Constant(2), sql.Constant("default"), sql.Constant(1), sql.Constant(false)]);
        AssertWindowFunctionName("FIRST_VALUE", translator, MakeExtensionMethod(nameof(DecentDBDbFunctionsExtensions.FirstValue), 2, 4, typeof(string), typeof(int)), [sql.Constant(0), sql.Constant("value"), sql.Constant(2), sql.Constant(false)]);
        AssertWindowFunctionName("FIRST_VALUE", translator, MakeExtensionMethod(nameof(DecentDBDbFunctionsExtensions.FirstValue), 3, 5, typeof(string), typeof(string), typeof(int)), [sql.Constant(0), sql.Constant("dep"), sql.Constant("value"), sql.Constant(2), sql.Constant(false)]);
        AssertWindowFunctionName("LAST_VALUE", translator, MakeExtensionMethod(nameof(DecentDBDbFunctionsExtensions.LastValue), 2, 4, typeof(string), typeof(int)), [sql.Constant(0), sql.Constant("value"), sql.Constant(2), sql.Constant(false)]);
        AssertWindowFunctionName("LAST_VALUE", translator, MakeExtensionMethod(nameof(DecentDBDbFunctionsExtensions.LastValue), 3, 5, typeof(string), typeof(string), typeof(int)), [sql.Constant(0), sql.Constant("dep"), sql.Constant("value"), sql.Constant(2), sql.Constant(false)]);
        AssertWindowFunctionName("NTH_VALUE", translator, MakeExtensionMethod(nameof(DecentDBDbFunctionsExtensions.NthValue), 2, 5, typeof(string), typeof(int)), [sql.Constant(0), sql.Constant("value"), sql.Constant(2), sql.Constant(3), sql.Constant(false)]);
        AssertWindowFunctionName("NTH_VALUE", translator, MakeExtensionMethod(nameof(DecentDBDbFunctionsExtensions.NthValue), 3, 6, typeof(string), typeof(string), typeof(int)), [sql.Constant(0), sql.Constant("dep"), sql.Constant("value"), sql.Constant(2), sql.Constant(3), sql.Constant(false)]);

        var descendingNotConstant = translator.Translate(
            null,
            MakeExtensionMethod(nameof(DecentDBDbFunctionsExtensions.RowNumber), 1, 3, typeof(int)),
            [sql.Constant(0), sql.Constant(5), new DummySqlExpression(typeof(bool))],
            null!);
        Assert.Null(descendingNotConstant);
        Assert.Null(translator.Translate(null, GetMethod<object>(nameof(object.GetHashCode), []), [], null!));
    }

    [Fact]
    public void JsonEachExpression_CoversUpdateCloneAliasQuoteAndEquality()
    {
        using var context = CreateContext();
        var sql = context.GetService<ISqlExpressionFactory>();

        var json = new JsonEachExpression("j", sql.Constant("""["a","b"]"""));
        Assert.Same(json, json.Update(json.Json));

        var updated = json.Update(sql.Constant("""["c"]"""));
        Assert.NotSame(json, updated);

        var clone = (JsonEachExpression)json.Clone("j2", new PassThroughVisitor());
        Assert.Equal("j2", clone.Alias);

        var aliased = json.WithAlias("j3");
        Assert.Equal("j3", aliased.Alias);

        Assert.NotNull(json.Quote());
        var rendered = RenderSqlExpression(context, json);
        Assert.Contains("json_each", rendered, StringComparison.OrdinalIgnoreCase);
        Assert.Equal(json, json);
        Assert.NotEqual(json, aliased);
    }

    [Fact]
    public void WindowFunctionExpression_CoversUpdateQuotePrintAndEquality()
    {
        using var context = CreateContext();
        var sql = context.GetService<ISqlExpressionFactory>();
        var longMapping = (RelationalTypeMapping)sql.ApplyDefaultTypeMapping(sql.Constant(0L)).TypeMapping!;
        var orderBy = sql.Constant(2);
        var argument = sql.Constant(1);

        var expression = new WindowFunctionExpression(
            "ROW_NUMBER",
            [argument],
            partitionBy: null,
            orderBy,
            orderByDescending: false,
            type: typeof(long),
            typeMapping: longMapping);

        Assert.Same(expression, expression.Update(expression.Arguments, expression.PartitionBy, expression.OrderBy));
        var updated = expression.Update([sql.Constant(5)], expression.PartitionBy, expression.OrderBy);
        Assert.NotSame(expression, updated);

        Assert.NotNull(expression.Quote());
        var rendered = RenderSqlExpression(context, expression);
        Assert.Contains("OVER", rendered, StringComparison.OrdinalIgnoreCase);
        Assert.Equal(expression, expression);
        Assert.NotEqual(expression, updated);
        Assert.NotEqual(0, expression.GetHashCode());
    }

    public void Dispose()
    {
        TryDelete(_dbPath);
        TryDelete(_dbPath + "-wal");
    }

    private TestContext CreateContext()
    {
        var options = new DbContextOptionsBuilder<TestContext>()
            .UseDecentDB($"Data Source={_dbPath}")
            .Options;
        return new TestContext(options);
    }

    private static void AssertWindowFunctionName(
        string expectedFunctionName,
        DecentDBWindowFunctionTranslator translator,
        MethodInfo method,
        IReadOnlyList<SqlExpression> arguments)
    {
        var translated = translator.Translate(null, method, arguments, null!);
        var expression = Assert.IsType<WindowFunctionExpression>(translated);
        Assert.Equal(expectedFunctionName, expression.FunctionName);
    }

    private static MethodInfo MakeExtensionMethod(string name, int genericCount, int parameterCount, params Type[] genericTypes)
    {
        var method = typeof(DecentDBDbFunctionsExtensions)
            .GetMethods(BindingFlags.Public | BindingFlags.Static)
            .Single(candidate =>
                candidate.Name == name &&
                candidate.IsGenericMethodDefinition &&
                candidate.GetGenericArguments().Length == genericCount &&
                candidate.GetParameters().Length == parameterCount);

        return method.MakeGenericMethod(genericTypes);
    }

    private static MethodInfo GetMethod<TDeclaring>(string name, Type[] parameters)
        => typeof(TDeclaring).GetMethod(name, BindingFlags.Public | BindingFlags.Instance | BindingFlags.Static, binder: null, types: parameters, modifiers: null)!;

    private static MethodInfo GetMethod(Type declaringType, string name, Type[] parameters)
        => declaringType.GetMethod(name, BindingFlags.Public | BindingFlags.Instance | BindingFlags.Static, binder: null, types: parameters, modifiers: null)!;

    private static string RenderSqlExpression(DbContext context, object sqlExpression)
    {
        var printer = Activator.CreateInstance(typeof(ExpressionPrinter), nonPublic: true)
            ?? throw new InvalidOperationException("Unable to create ExpressionPrinter.");
        var printMethod = sqlExpression.GetType().GetMethod("Print", BindingFlags.Instance | BindingFlags.NonPublic)!;
        printMethod.Invoke(sqlExpression, [printer]);
        return printer.ToString()!;
    }

    private static void TryDelete(string path)
    {
        if (File.Exists(path))
        {
            File.Delete(path);
        }
    }

    private sealed class TestContext : DbContext
    {
        public TestContext(DbContextOptions<TestContext> options) : base(options)
        {
        }
    }

    private sealed class PassThroughVisitor : ExpressionVisitor
    {
    }

    private sealed class DummySqlExpression(Type type) : SqlExpression(type, typeMapping: null)
    {
        protected override Expression VisitChildren(ExpressionVisitor visitor) => this;

        public override Expression Quote() => Expression.Constant(this, typeof(SqlExpression));

        protected override void Print(ExpressionPrinter expressionPrinter) => expressionPrinter.Append("dummy");
    }
}
