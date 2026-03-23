using System.Reflection;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Diagnostics;
using Microsoft.EntityFrameworkCore.Query;
using Microsoft.EntityFrameworkCore.Query.SqlExpressions;

namespace DecentDB.EntityFrameworkCore.Query.Internal;

public sealed class DecentDBMemberTranslator : IMemberTranslator
{
    private static readonly MemberInfo StringLengthProperty
        = typeof(string).GetProperty(nameof(string.Length))!;

    private readonly ISqlExpressionFactory _sqlExpressionFactory;

    public DecentDBMemberTranslator(ISqlExpressionFactory sqlExpressionFactory)
    {
        _sqlExpressionFactory = sqlExpressionFactory;
    }

    public SqlExpression? Translate(
        SqlExpression? instance,
        MemberInfo member,
        Type returnType,
        IDiagnosticsLogger<DbLoggerCategory.Query> logger)
    {
        if (instance is not null && member.Equals(StringLengthProperty))
        {
            return _sqlExpressionFactory.Function("LENGTH", [instance], nullable: true,
                argumentsPropagateNullability: [true], typeof(int));
        }

        return null;
    }
}

public sealed class DecentDBMemberTranslatorProvider : RelationalMemberTranslatorProvider
{
    public DecentDBMemberTranslatorProvider(RelationalMemberTranslatorProviderDependencies dependencies)
        : base(dependencies)
    {
        AddTranslators([new DecentDBMemberTranslator(dependencies.SqlExpressionFactory)]);
    }
}
