using System.Globalization;
using Microsoft.EntityFrameworkCore.Storage;
using Microsoft.EntityFrameworkCore.Storage.ValueConversion;

namespace DecentDB.EntityFrameworkCore.Storage;

/// <summary>
/// Maps <see cref="DateOnly"/> to DecentDB <c>INTEGER</c> as days since Unix
/// epoch and emits integer SQL literals in the same provider format.
/// </summary>
public sealed class DecentDBDateOnlyTypeMapping : RelationalTypeMapping
{
    private static readonly DateOnly EpochDate = DateOnly.FromDateTime(DateTime.UnixEpoch);

    private static readonly ValueConverter<DateOnly, long> DateOnlyDaysConverter = new(
        value => value.DayNumber - EpochDate.DayNumber,
        value => EpochDate.AddDays(checked((int)value)));

    public DecentDBDateOnlyTypeMapping()
        : base(new RelationalTypeMappingParameters(
            new CoreTypeMappingParameters(typeof(DateOnly), DateOnlyDaysConverter),
            storeType: "INTEGER",
            storeTypePostfix: StoreTypePostfix.None,
            dbType: System.Data.DbType.Int64))
    {
    }

    private DecentDBDateOnlyTypeMapping(RelationalTypeMappingParameters parameters)
        : base(parameters)
    {
    }

    protected override RelationalTypeMapping Clone(RelationalTypeMappingParameters parameters)
        => new DecentDBDateOnlyTypeMapping(parameters);

    public override string GenerateSqlLiteral(object? value)
        => value is null
            ? "NULL"
            : GenerateDaysLiteral(ToProviderDays(value));

    protected override string GenerateNonNullSqlLiteral(object value)
        => GenerateDaysLiteral(ToProviderDays(value));

    private static long ToProviderDays(object value)
        => value switch
        {
            DateOnly dateOnly => (long)(DateOnlyDaysConverter.ConvertToProvider(dateOnly) ?? 0L),
            long days => days,
            _ => throw new InvalidCastException($"Cannot convert {value.GetType()} to DateOnly literal.")
        };

    private static string GenerateDaysLiteral(long days)
        => days.ToString(CultureInfo.InvariantCulture);
}
