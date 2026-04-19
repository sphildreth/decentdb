using System.Globalization;
using Microsoft.EntityFrameworkCore.Storage;
using Microsoft.EntityFrameworkCore.Storage.ValueConversion;
using NodaTime;

namespace DecentDB.EntityFrameworkCore.Storage;

/// <summary>
/// Maps <see cref="Instant"/> to DecentDB <c>INTEGER</c> as Unix ticks and
/// emits integer SQL literals in the same provider format.
/// </summary>
public sealed class DecentDBInstantTypeMapping : RelationalTypeMapping
{
    private static readonly ValueConverter<Instant, long> InstantTicksConverter = new(
        value => value.ToUnixTimeTicks(),
        value => Instant.FromUnixTimeTicks(value));

    public DecentDBInstantTypeMapping()
        : base(new RelationalTypeMappingParameters(
            new CoreTypeMappingParameters(typeof(Instant), InstantTicksConverter),
            storeType: "INTEGER",
            storeTypePostfix: StoreTypePostfix.None,
            dbType: System.Data.DbType.Int64))
    {
    }

    private DecentDBInstantTypeMapping(RelationalTypeMappingParameters parameters)
        : base(parameters)
    {
    }

    protected override RelationalTypeMapping Clone(RelationalTypeMappingParameters parameters)
        => new DecentDBInstantTypeMapping(parameters);

    public override string GenerateSqlLiteral(object? value)
        => value is null
            ? "NULL"
            : GenerateTicksLiteral(ToProviderTicks(value));

    protected override string GenerateNonNullSqlLiteral(object value)
        => GenerateTicksLiteral(ToProviderTicks(value));

    private static long ToProviderTicks(object value)
        => value switch
        {
            Instant instant => (long)(InstantTicksConverter.ConvertToProvider(instant) ?? 0L),
            long ticks => ticks,
            _ => throw new InvalidCastException($"Cannot convert {value.GetType()} to Instant literal.")
        };

    private static string GenerateTicksLiteral(long ticks)
        => string.Format(CultureInfo.InvariantCulture, "CAST('{0}' AS INT64)", ticks);
}
