using System.Globalization;
using DecentDB.AdoNet;
using Microsoft.EntityFrameworkCore.Storage;

namespace DecentDB.EntityFrameworkCore.Storage;

/// <summary>
/// Maps <see cref="decimal"/> to DecentDB DECIMAL/NUMERIC and normalizes
/// literal scale the same way parameter binding does.
/// </summary>
public sealed class DecentDBDecimalTypeMapping : DecimalTypeMapping
{
    public DecentDBDecimalTypeMapping(
        string storeType,
        System.Data.DbType? dbType = System.Data.DbType.Decimal,
        int? precision = null,
        int? scale = null)
        : base(storeType, dbType, precision, scale)
    {
    }

    private DecentDBDecimalTypeMapping(RelationalTypeMappingParameters parameters)
        : base(parameters)
    {
    }

    protected override RelationalTypeMapping Clone(RelationalTypeMappingParameters parameters)
        => new DecentDBDecimalTypeMapping(parameters);

    protected override string GenerateNonNullSqlLiteral(object value)
    {
        var decimalValue = value is decimal d
            ? d
            : Convert.ToDecimal(value, CultureInfo.InvariantCulture);

        if (Scale.HasValue)
        {
            decimalValue = DecimalScaleNormalizer.Normalize(decimalValue, Scale.Value);
        }

        return decimalValue.ToString(CultureInfo.InvariantCulture);
    }
}
