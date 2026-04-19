using System.Globalization;
using Microsoft.EntityFrameworkCore.Storage;
using Microsoft.EntityFrameworkCore.Storage.ValueConversion;

namespace DecentDB.EntityFrameworkCore.Storage;

/// <summary>
/// Maps <see cref="Guid"/> to the DecentDB <c>UUID</c> store type. Values are
/// bound to parameters as 16-byte blobs and emitted as
/// <c>UUID_PARSE('...')</c> SQL literals (DecentDB rejects string and blob
/// literals for <c>UUID</c> columns).
/// </summary>
public sealed class DecentDBGuidTypeMapping : RelationalTypeMapping
{
    private static readonly ValueConverter<Guid, byte[]> GuidBytesConverter = new(
        value => value.ToByteArray(),
        value => new Guid(value));

    public DecentDBGuidTypeMapping()
        : base(new RelationalTypeMappingParameters(
            new CoreTypeMappingParameters(typeof(Guid), GuidBytesConverter),
            storeType: "UUID",
            storeTypePostfix: StoreTypePostfix.None,
            dbType: System.Data.DbType.Guid,
            size: 16))
    {
    }

    private DecentDBGuidTypeMapping(RelationalTypeMappingParameters parameters)
        : base(parameters)
    {
    }

    protected override RelationalTypeMapping Clone(RelationalTypeMappingParameters parameters)
        => new DecentDBGuidTypeMapping(parameters);

    public override string GenerateSqlLiteral(object? value)
        => value is null
            ? "NULL"
            : GenerateGuidLiteral(ExtractGuid(value));

    protected override string GenerateNonNullSqlLiteral(object value)
        => GenerateGuidLiteral(ExtractGuid(value));

    private static Guid ExtractGuid(object value)
        => value switch
        {
            Guid g => g,
            byte[] b when b.Length == 16 => new Guid(b),
            _ => throw new InvalidCastException($"Cannot convert {value.GetType()} to Guid literal.")
        };

    private static string GenerateGuidLiteral(Guid value)
        => string.Format(CultureInfo.InvariantCulture, "UUID_PARSE('{0:D}')", value);
}
