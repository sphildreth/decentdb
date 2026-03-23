using System.Reflection;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Diagnostics;
using Microsoft.EntityFrameworkCore.Query;
using Microsoft.EntityFrameworkCore.Query.SqlExpressions;
using NodaTime;

namespace DecentDB.EntityFrameworkCore.NodaTime.Query.Internal;

/// <summary>
/// Translates NodaTime member accesses (e.g. LocalDate.Year) into SQL expressions.
/// LocalDate is stored as epoch days (days since 1970-01-01).
/// Uses the Hinnant civil calendar algorithm for correct date component extraction.
/// </summary>
public sealed class DecentDBNodaTimeMemberTranslator : IMemberTranslator
{
    private static readonly MemberInfo LocalDateYear = typeof(LocalDate).GetProperty(nameof(LocalDate.Year))!;
    private static readonly MemberInfo LocalDateMonth = typeof(LocalDate).GetProperty(nameof(LocalDate.Month))!;
    private static readonly MemberInfo LocalDateDay = typeof(LocalDate).GetProperty(nameof(LocalDate.Day))!;
    private static readonly MemberInfo LocalDateDayOfYear = typeof(LocalDate).GetProperty(nameof(LocalDate.DayOfYear))!;

    private readonly ISqlExpressionFactory _sql;

    public DecentDBNodaTimeMemberTranslator(ISqlExpressionFactory sqlExpressionFactory)
    {
        _sql = sqlExpressionFactory;
    }

    public SqlExpression? Translate(
        SqlExpression? instance,
        MemberInfo member,
        Type returnType,
        IDiagnosticsLogger<DbLoggerCategory.Query> logger)
    {
        if (instance is null)
            return null;

        if (member.DeclaringType != typeof(LocalDate))
            return null;

        if (member.Equals(LocalDateYear))
            return ExtractYear(instance);
        if (member.Equals(LocalDateMonth))
            return ExtractMonth(instance);
        if (member.Equals(LocalDateDay))
            return ExtractDay(instance);
        if (member.Equals(LocalDateDayOfYear))
            return ExtractDayOfYear(instance);

        return null;
    }

    // Hinnant civil_from_days algorithm — pure integer arithmetic
    // Input: d = epoch days (days since 1970-01-01)
    // shifted = d + 719468   (days since 0000-03-01)
    // era = shifted / 146097 (400-year era)
    // doe = shifted - era * 146097 (day of era)
    // yoe = (doe - doe/1460 + doe/36524 - doe/146096) / 365 (year of era)
    // doy = doe - (365*yoe + yoe/4 - yoe/100) (day of year, March-based)
    // mp = (5*doy + 2) / 153 (month period)
    // m = mp < 10 ? mp + 3 : mp - 9 (month 1-12)
    // year = yoe + era*400 + (m <= 2 ? 1 : 0)

    private SqlExpression ExtractYear(SqlExpression epochDays)
    {
        var d = EpochDaysAsLong(epochDays);
        var (yoe, era, _, mp) = ComputeCivilComponents(d);
        var m = MonthFromMp(mp);
        var monthAdj = _sql.Case(
            [new CaseWhenClause(_sql.LessThanOrEqual(m, Const(2)), Const(1))],
            Const(0));
        return Add(Add(yoe, Mul(era, Const(400))), monthAdj);
    }

    private SqlExpression ExtractMonth(SqlExpression epochDays)
    {
        var d = EpochDaysAsLong(epochDays);
        var (_, _, _, mp) = ComputeCivilComponents(d);
        return MonthFromMp(mp);
    }

    private SqlExpression ExtractDay(SqlExpression epochDays)
    {
        var d = EpochDaysAsLong(epochDays);
        var (_, _, doy, mp) = ComputeCivilComponents(d);
        return Add(Sub(doy, Div(Add(Mul(Const(153), mp), Const(2)), Const(5))), Const(1));
    }

    private SqlExpression ExtractDayOfYear(SqlExpression epochDays)
    {
        var d = EpochDaysAsLong(epochDays);
        var (_, _, doy, _) = ComputeCivilComponents(d);
        return Add(doy, Const(1));
    }

    /// <summary>
    /// Convert the NodaTime-typed column to long for integer arithmetic.
    /// Without this, SqlBinaryExpression inherits LocalDate as the CLR type,
    /// causing GroupBy translation to fail with "No coercion operator".
    /// </summary>
    private SqlExpression EpochDaysAsLong(SqlExpression instance)
        => _sql.Convert(instance, typeof(long));

    private (SqlExpression yoe, SqlExpression era, SqlExpression doy, SqlExpression mp) ComputeCivilComponents(SqlExpression epochDays)
    {
        var shifted = Add(epochDays, Const(719468));
        var era = Div(shifted, Const(146097));
        var doe = Sub(shifted, Mul(era, Const(146097)));
        var yoe = Div(Sub(Sub(Add(doe, Div(doe, Const(36524))), Div(doe, Const(1460))), Div(doe, Const(146096))), Const(365));
        var doy = Sub(doe, Add(Sub(Mul(Const(365), yoe), Div(yoe, Const(100))), Div(yoe, Const(4))));
        var mp = Div(Add(Mul(Const(5), doy), Const(2)), Const(153));
        return (yoe, era, doy, mp);
    }

    private SqlExpression MonthFromMp(SqlExpression mp)
        => _sql.Case(
            [new CaseWhenClause(_sql.LessThan(mp, Const(10)), Add(mp, Const(3)))],
            Sub(mp, Const(9)));

    private SqlExpression Const(int value)
        => _sql.Constant(value, typeof(int));

    private SqlExpression Add(SqlExpression left, SqlExpression right)
        => _sql.Add(left, right);

    private SqlExpression Sub(SqlExpression left, SqlExpression right)
        => _sql.Subtract(left, right);

    private SqlExpression Mul(SqlExpression left, SqlExpression right)
        => _sql.Multiply(left, right);

    private SqlExpression Div(SqlExpression left, SqlExpression right)
        => _sql.Divide(left, right);
}
