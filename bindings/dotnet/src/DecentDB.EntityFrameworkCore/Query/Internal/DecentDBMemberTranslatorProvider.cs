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
    private static readonly MemberInfo DateTimeYear = typeof(DateTime).GetProperty(nameof(DateTime.Year))!;
    private static readonly MemberInfo DateTimeMonth = typeof(DateTime).GetProperty(nameof(DateTime.Month))!;
    private static readonly MemberInfo DateTimeDay = typeof(DateTime).GetProperty(nameof(DateTime.Day))!;
    private static readonly MemberInfo DateTimeDayOfYear = typeof(DateTime).GetProperty(nameof(DateTime.DayOfYear))!;
    private static readonly MemberInfo DateTimeHour = typeof(DateTime).GetProperty(nameof(DateTime.Hour))!;
    private static readonly MemberInfo DateTimeMinute = typeof(DateTime).GetProperty(nameof(DateTime.Minute))!;
    private static readonly MemberInfo DateTimeSecond = typeof(DateTime).GetProperty(nameof(DateTime.Second))!;
    private static readonly MemberInfo DateOnlyYear = typeof(DateOnly).GetProperty(nameof(DateOnly.Year))!;
    private static readonly MemberInfo DateOnlyMonth = typeof(DateOnly).GetProperty(nameof(DateOnly.Month))!;
    private static readonly MemberInfo DateOnlyDay = typeof(DateOnly).GetProperty(nameof(DateOnly.Day))!;
    private static readonly MemberInfo DateOnlyDayOfYear = typeof(DateOnly).GetProperty(nameof(DateOnly.DayOfYear))!;
    private static readonly MemberInfo TimeOnlyHour = typeof(TimeOnly).GetProperty(nameof(TimeOnly.Hour))!;
    private static readonly MemberInfo TimeOnlyMinute = typeof(TimeOnly).GetProperty(nameof(TimeOnly.Minute))!;
    private static readonly MemberInfo TimeOnlySecond = typeof(TimeOnly).GetProperty(nameof(TimeOnly.Second))!;
    private static readonly MemberInfo TimeOnlyMillisecond = typeof(TimeOnly).GetProperty(nameof(TimeOnly.Millisecond))!;

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

        if (instance is null)
        {
            return null;
        }

        if (member.DeclaringType == typeof(DateTime))
        {
            return TranslateDateTimeMember(instance, member, returnType);
        }

        if (member.DeclaringType == typeof(DateOnly))
        {
            return TranslateDateOnlyMember(instance, member);
        }

        if (member.DeclaringType == typeof(TimeOnly))
        {
            return TranslateTimeOnlyMember(instance, member);
        }

        return null;
    }

    private SqlExpression? TranslateDateTimeMember(SqlExpression instance, MemberInfo member, Type returnType)
    {
        var field = member switch
        {
            _ when member.Equals(DateTimeYear) => "YEAR",
            _ when member.Equals(DateTimeMonth) => "MONTH",
            _ when member.Equals(DateTimeDay) => "DAY",
            _ when member.Equals(DateTimeDayOfYear) => "DOY",
            _ when member.Equals(DateTimeHour) => "HOUR",
            _ when member.Equals(DateTimeMinute) => "MINUTE",
            _ when member.Equals(DateTimeSecond) => "SECOND",
            _ => null
        };

        return field is null ? null : Extract(field, instance, returnType);
    }

    private SqlExpression? TranslateDateOnlyMember(SqlExpression instance, MemberInfo member)
    {
        if (member.Equals(DateOnlyYear))
        {
            return ExtractDateOnlyYear(instance);
        }

        if (member.Equals(DateOnlyMonth))
        {
            return ExtractDateOnlyMonth(instance);
        }

        if (member.Equals(DateOnlyDay))
        {
            return ExtractDateOnlyDay(instance);
        }

        if (member.Equals(DateOnlyDayOfYear))
        {
            return ExtractDateOnlyDayOfYear(instance);
        }

        return null;
    }

    private SqlExpression? TranslateTimeOnlyMember(SqlExpression instance, MemberInfo member)
    {
        var ticks = TimeOnlyAsLong(instance);
        var ticksPerSecond = LongConst(TimeSpan.TicksPerSecond);
        var ticksPerMinute = Mul(ticksPerSecond, LongConst(60));
        var ticksPerHour = Mul(ticksPerMinute, LongConst(60));

        if (member.Equals(TimeOnlyHour))
        {
            return ToInt(Div(ticks, ticksPerHour));
        }

        if (member.Equals(TimeOnlyMinute))
        {
            return ToInt(Sub(
                Div(ticks, ticksPerMinute),
                Mul(Div(ticks, ticksPerHour), LongConst(60))));
        }

        if (member.Equals(TimeOnlySecond))
        {
            return ToInt(Sub(
                Div(ticks, ticksPerSecond),
                Mul(Div(ticks, ticksPerMinute), LongConst(60))));
        }

        if (member.Equals(TimeOnlyMillisecond))
        {
            return ToInt(Sub(
                Div(ticks, LongConst(TimeSpan.TicksPerMillisecond)),
                Mul(Div(ticks, ticksPerSecond), LongConst(1000))));
        }

        return null;
    }

    private SqlExpression Extract(string field, SqlExpression instance, Type returnType)
        => _sqlExpressionFactory.Function(
            "DATE_PART",
            [_sqlExpressionFactory.Constant(field), instance],
            nullable: true,
            argumentsPropagateNullability: [false, true],
            returnType);

    private SqlExpression ExtractDateOnlyYear(SqlExpression instance)
    {
        var epochDays = DateOnlyAsLong(instance);
        var (yoe, era, _, monthPeriod) = ComputeCivilComponents(epochDays);
        var month = MonthFromMonthPeriod(monthPeriod);
        var monthAdjustment = _sqlExpressionFactory.Case(
            [new CaseWhenClause(_sqlExpressionFactory.LessThanOrEqual(month, Const(2)), Const(1))],
            Const(0));
        return Add(Add(yoe, Mul(era, Const(400))), monthAdjustment);
    }

    private SqlExpression ExtractDateOnlyMonth(SqlExpression instance)
    {
        var epochDays = DateOnlyAsLong(instance);
        var (_, _, _, monthPeriod) = ComputeCivilComponents(epochDays);
        return MonthFromMonthPeriod(monthPeriod);
    }

    private SqlExpression ExtractDateOnlyDay(SqlExpression instance)
    {
        var epochDays = DateOnlyAsLong(instance);
        var (_, _, dayOfYear, monthPeriod) = ComputeCivilComponents(epochDays);
        return Add(Sub(dayOfYear, Div(Add(Mul(Const(153), monthPeriod), Const(2)), Const(5))), Const(1));
    }

    private SqlExpression ExtractDateOnlyDayOfYear(SqlExpression instance)
    {
        var epochDays = DateOnlyAsLong(instance);
        var (_, _, dayOfYear, _) = ComputeCivilComponents(epochDays);
        return Add(dayOfYear, Const(1));
    }

    private SqlExpression DateOnlyAsLong(SqlExpression instance)
        => _sqlExpressionFactory.Convert(instance, typeof(long));

    private SqlExpression TimeOnlyAsLong(SqlExpression instance)
        => _sqlExpressionFactory.Convert(instance, typeof(long));

    private (SqlExpression yoe, SqlExpression era, SqlExpression dayOfYear, SqlExpression monthPeriod) ComputeCivilComponents(SqlExpression epochDays)
    {
        var shifted = Add(epochDays, Const(719468));
        var era = Div(shifted, Const(146097));
        var dayOfEra = Sub(shifted, Mul(era, Const(146097)));
        var yearOfEra = Div(
            Sub(
                Sub(Add(dayOfEra, Div(dayOfEra, Const(36524))), Div(dayOfEra, Const(1460))),
                Div(dayOfEra, Const(146096))),
            Const(365));
        var dayOfYear = Sub(
            dayOfEra,
            Add(
                Sub(Mul(Const(365), yearOfEra), Div(yearOfEra, Const(100))),
                Div(yearOfEra, Const(4))));
        var monthPeriod = Div(Add(Mul(Const(5), dayOfYear), Const(2)), Const(153));
        return (yearOfEra, era, dayOfYear, monthPeriod);
    }

    private SqlExpression MonthFromMonthPeriod(SqlExpression monthPeriod)
        => _sqlExpressionFactory.Case(
            [new CaseWhenClause(_sqlExpressionFactory.LessThan(monthPeriod, Const(10)), Add(monthPeriod, Const(3)))],
            Sub(monthPeriod, Const(9)));

    private SqlExpression Const(int value)
        => _sqlExpressionFactory.Constant(value, typeof(int));

    private SqlExpression LongConst(long value)
        => _sqlExpressionFactory.Constant(value, typeof(long));

    private SqlExpression ToInt(SqlExpression expression)
        => _sqlExpressionFactory.Convert(expression, typeof(int));

    private SqlExpression Add(SqlExpression left, SqlExpression right)
        => _sqlExpressionFactory.Add(left, right);

    private SqlExpression Sub(SqlExpression left, SqlExpression right)
        => _sqlExpressionFactory.Subtract(left, right);

    private SqlExpression Mul(SqlExpression left, SqlExpression right)
        => _sqlExpressionFactory.Multiply(left, right);

    private SqlExpression Div(SqlExpression left, SqlExpression right)
        => _sqlExpressionFactory.Divide(left, right);
}

public sealed class DecentDBMemberTranslatorProvider : RelationalMemberTranslatorProvider
{
    public DecentDBMemberTranslatorProvider(RelationalMemberTranslatorProviderDependencies dependencies)
        : base(dependencies)
    {
        AddTranslators([new DecentDBMemberTranslator(dependencies.SqlExpressionFactory)]);
    }
}
