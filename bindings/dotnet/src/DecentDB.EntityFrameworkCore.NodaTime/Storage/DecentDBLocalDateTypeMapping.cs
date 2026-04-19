using System.Globalization;
using Microsoft.EntityFrameworkCore.Storage;
using Microsoft.EntityFrameworkCore.Storage.ValueConversion;
using NodaTime;

namespace DecentDB.EntityFrameworkCore.Storage;

/// <summary>
/// Maps <see cref="LocalDate"/> to DecentDB <c>INTEGER</c> as days since
/// Unix epoch and emits integer SQL literals in the same provider format.
/// </summary>
public sealed class DecentDBLocalDateTypeMapping : RelationalTypeMapping
{
    private static readonly LocalDate NodaEpochDate = new(1970, 1, 1);

    private static readonly ValueConverter<LocalDate, long> LocalDateDaysConverter = new(
        value => Period.Between(NodaEpochDate, value, PeriodUnits.Days).Days,
        value => NodaEpochDate.PlusDays(checked((int)value)));

    public DecentDBLocalDateTypeMapping()
        : base(new RelationalTypeMappingParameters(
            new CoreTypeMappingParameters(typeof(LocalDate), LocalDateDaysConverter),
            storeType: "INTEGER",
            storeTypePostfix: StoreTypePostfix.None,
            dbType: System.Data.DbType.Int64))
    {
    }

    private DecentDBLocalDateTypeMapping(RelationalTypeMappingParameters parameters)
        : base(parameters)
    {
    }

    protected override RelationalTypeMapping Clone(RelationalTypeMappingParameters parameters)
        => new DecentDBLocalDateTypeMapping(parameters);

    public override string GenerateSqlLiteral(object? value)
        => value is null
            ? "NULL"
            : GenerateDaysLiteral(ToProviderDays(value));

    protected override string GenerateNonNullSqlLiteral(object value)
        => GenerateDaysLiteral(ToProviderDays(value));

    private static long ToProviderDays(object value)
        => value switch
        {
            LocalDate localDate => (long)(LocalDateDaysConverter.ConvertToProvider(localDate) ?? 0L),
            long days => days,
            _ => throw new InvalidCastException($"Cannot convert {value.GetType()} to LocalDate literal.")
        };

    private static string GenerateDaysLiteral(long days)
        => string.Format(CultureInfo.InvariantCulture, "CAST('{0}' AS INT64)", days);
}
