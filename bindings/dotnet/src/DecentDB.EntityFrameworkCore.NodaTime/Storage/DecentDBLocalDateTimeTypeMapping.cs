using System.Globalization;
using Microsoft.EntityFrameworkCore.Storage;
using Microsoft.EntityFrameworkCore.Storage.ValueConversion;
using NodaTime;

namespace DecentDB.EntityFrameworkCore.Storage;

/// <summary>
/// Maps <see cref="LocalDateTime"/> to DecentDB <c>INTEGER</c> as Unix ticks
/// in UTC and emits integer SQL literals in the same provider format.
/// </summary>
public sealed class DecentDBLocalDateTimeTypeMapping : RelationalTypeMapping
{
    private static readonly ValueConverter<LocalDateTime, long> LocalDateTimeTicksConverter = new(
        value => value.InZoneLeniently(DateTimeZone.Utc).ToInstant().ToUnixTimeTicks(),
        value => Instant.FromUnixTimeTicks(value).InUtc().LocalDateTime);

    public DecentDBLocalDateTimeTypeMapping()
        : base(new RelationalTypeMappingParameters(
            new CoreTypeMappingParameters(typeof(LocalDateTime), LocalDateTimeTicksConverter),
            storeType: "INTEGER",
            storeTypePostfix: StoreTypePostfix.None,
            dbType: System.Data.DbType.Int64))
    {
    }

    private DecentDBLocalDateTimeTypeMapping(RelationalTypeMappingParameters parameters)
        : base(parameters)
    {
    }

    protected override RelationalTypeMapping Clone(RelationalTypeMappingParameters parameters)
        => new DecentDBLocalDateTimeTypeMapping(parameters);

    public override string GenerateSqlLiteral(object? value)
        => value is null
            ? "NULL"
            : GenerateTicksLiteral(ToProviderTicks(value));

    protected override string GenerateNonNullSqlLiteral(object value)
        => GenerateTicksLiteral(ToProviderTicks(value));

    private static long ToProviderTicks(object value)
        => value switch
        {
            LocalDateTime localDateTime => (long)(LocalDateTimeTicksConverter.ConvertToProvider(localDateTime) ?? 0L),
            long ticks => ticks,
            _ => throw new InvalidCastException($"Cannot convert {value.GetType()} to LocalDateTime literal.")
        };

    private static string GenerateTicksLiteral(long ticks)
        => string.Format(CultureInfo.InvariantCulture, "CAST('{0}' AS INT64)", ticks);
}
