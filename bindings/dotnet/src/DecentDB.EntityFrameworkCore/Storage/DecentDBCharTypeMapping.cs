using Microsoft.EntityFrameworkCore.Storage;
using Microsoft.EntityFrameworkCore.Storage.ValueConversion;

namespace DecentDB.EntityFrameworkCore.Storage;

/// <summary>
/// Maps <see cref="char"/> to DecentDB <c>TEXT(1)</c>. Surrogate code units
/// are rejected; callers should use <see cref="string"/> for supplementary
/// Unicode code points.
/// </summary>
public sealed class DecentDBCharTypeMapping : RelationalTypeMapping
{
    private static readonly ValueConverter<char, string> CharConverter = new(
        value => ConvertCharToString(value),
        value => ConvertStringToChar(value));

    public DecentDBCharTypeMapping()
        : base(new RelationalTypeMappingParameters(
            new CoreTypeMappingParameters(typeof(char), CharConverter),
            storeType: "TEXT(1)",
            storeTypePostfix: StoreTypePostfix.None,
            dbType: System.Data.DbType.StringFixedLength,
            size: 1))
    {
    }

    private DecentDBCharTypeMapping(RelationalTypeMappingParameters parameters)
        : base(parameters)
    {
    }

    protected override RelationalTypeMapping Clone(RelationalTypeMappingParameters parameters)
        => new DecentDBCharTypeMapping(parameters);

    public override string GenerateSqlLiteral(object? value)
        => value is null
            ? "NULL"
            : GenerateCharLiteral(ToProviderString(value));

    protected override string GenerateNonNullSqlLiteral(object value)
        => GenerateCharLiteral(ToProviderString(value));

    private static string ToProviderString(object value)
        => value switch
        {
            char c => ConvertCharToString(c),
            string s => ConvertStringToChar(s).ToString(),
            _ => throw new InvalidCastException($"Cannot convert {value.GetType()} to char literal.")
        };

    private static string ConvertCharToString(char value)
    {
        if (char.IsSurrogate(value))
        {
            throw new ArgumentException(
                "Surrogate char values are not supported for char mapping; use string for code points above U+FFFF.");
        }

        return value.ToString();
    }

    private static char ConvertStringToChar(string value)
    {
        if (value.Length != 1)
        {
            throw new InvalidCastException($"Expected TEXT(1) for char mapping, got length {value.Length}.");
        }

        return value[0];
    }

    private static string GenerateCharLiteral(string value)
        => $"'{value.Replace("'", "''", StringComparison.Ordinal)}'";
}
