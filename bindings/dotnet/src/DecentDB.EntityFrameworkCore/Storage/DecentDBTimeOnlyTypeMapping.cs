using System.Globalization;
using Microsoft.EntityFrameworkCore.Storage;
using Microsoft.EntityFrameworkCore.Storage.ValueConversion;

namespace DecentDB.EntityFrameworkCore.Storage;

/// <summary>
/// Maps <see cref="TimeOnly"/> to DecentDB <c>INTEGER</c> as .NET ticks and
/// emits integer SQL literals in the same provider format.
/// </summary>
public sealed class DecentDBTimeOnlyTypeMapping : RelationalTypeMapping
{
    private static readonly ValueConverter<TimeOnly, long> TimeOnlyTicksConverter = new(
        value => value.Ticks,
        value => new TimeOnly(value));

    public DecentDBTimeOnlyTypeMapping()
        : base(new RelationalTypeMappingParameters(
            new CoreTypeMappingParameters(typeof(TimeOnly), TimeOnlyTicksConverter),
            storeType: "INTEGER",
            storeTypePostfix: StoreTypePostfix.None,
            dbType: System.Data.DbType.Int64))
    {
    }

    private DecentDBTimeOnlyTypeMapping(RelationalTypeMappingParameters parameters)
        : base(parameters)
    {
    }

    protected override RelationalTypeMapping Clone(RelationalTypeMappingParameters parameters)
        => new DecentDBTimeOnlyTypeMapping(parameters);

    public override string GenerateSqlLiteral(object? value)
        => value is null
            ? "NULL"
            : GenerateTicksLiteral(ToProviderTicks(value));

    protected override string GenerateNonNullSqlLiteral(object value)
        => GenerateTicksLiteral(ToProviderTicks(value));

    private static long ToProviderTicks(object value)
        => value switch
        {
            TimeOnly timeOnly => (long)(TimeOnlyTicksConverter.ConvertToProvider(timeOnly) ?? 0L),
            long ticks => ticks,
            _ => throw new InvalidCastException($"Cannot convert {value.GetType()} to TimeOnly literal.")
        };

    private static string GenerateTicksLiteral(long ticks)
        => ticks.ToString(CultureInfo.InvariantCulture);
}
