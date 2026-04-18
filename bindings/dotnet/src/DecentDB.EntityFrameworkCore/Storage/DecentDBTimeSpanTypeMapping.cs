using System.Globalization;
using Microsoft.EntityFrameworkCore.Storage;
using Microsoft.EntityFrameworkCore.Storage.ValueConversion;

namespace DecentDB.EntityFrameworkCore.Storage;

/// <summary>
/// Maps <see cref="TimeSpan"/> to DecentDB <c>INTEGER</c> as .NET ticks and
/// emits integer SQL literals in the same provider format.
/// </summary>
public sealed class DecentDBTimeSpanTypeMapping : RelationalTypeMapping
{
    private static readonly ValueConverter<TimeSpan, long> TimeSpanTicksConverter = new(
        value => value.Ticks,
        value => TimeSpan.FromTicks(value));

    public DecentDBTimeSpanTypeMapping()
        : base(new RelationalTypeMappingParameters(
            new CoreTypeMappingParameters(typeof(TimeSpan), TimeSpanTicksConverter),
            storeType: "INTEGER",
            storeTypePostfix: StoreTypePostfix.None,
            dbType: System.Data.DbType.Int64))
    {
    }

    private DecentDBTimeSpanTypeMapping(RelationalTypeMappingParameters parameters)
        : base(parameters)
    {
    }

    protected override RelationalTypeMapping Clone(RelationalTypeMappingParameters parameters)
        => new DecentDBTimeSpanTypeMapping(parameters);

    public override string GenerateSqlLiteral(object? value)
        => value is null
            ? "NULL"
            : GenerateTicksLiteral(ToProviderTicks(value));

    protected override string GenerateNonNullSqlLiteral(object value)
        => GenerateTicksLiteral(ToProviderTicks(value));

    private static long ToProviderTicks(object value)
        => value switch
        {
            TimeSpan timeSpan => (long)(TimeSpanTicksConverter.ConvertToProvider(timeSpan) ?? 0L),
            long ticks => ticks,
            _ => throw new InvalidCastException($"Cannot convert {value.GetType()} to TimeSpan literal.")
        };

    private static string GenerateTicksLiteral(long ticks)
        => ticks.ToString(CultureInfo.InvariantCulture);
}
