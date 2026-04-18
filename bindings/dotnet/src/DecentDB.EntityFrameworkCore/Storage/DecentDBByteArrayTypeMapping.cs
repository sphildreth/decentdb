using Microsoft.EntityFrameworkCore.Storage;

namespace DecentDB.EntityFrameworkCore.Storage;

/// <summary>
/// Maps <see cref="byte[]"/> to DecentDB <c>BLOB</c>. DecentDB currently has no
/// accepted SQL literal form for BLOB values in inline SQL (e.g. EF Core
/// <c>HasData</c>), so literal generation fails fast with a clear error.
///
/// Probe results (2026-04-18):
/// - <c>X'00FF'</c> → cannot cast <c>Text("x00FF")</c> to BLOB
/// - <c>CAST('00FF' AS BLOB)</c> → cannot cast Text to BLOB
/// - <c>FROM_HEX('00FF')</c> / <c>BLOB_PARSE('00FF')</c> → unsupported function
/// </summary>
public sealed class DecentDBByteArrayTypeMapping : ByteArrayTypeMapping
{
    private const string BlobLiteralError =
        "DecentDB does not currently expose an executable SQL BLOB literal form for EF Core HasData seeds. " +
        "See design/adr/0134-blob-literal-parse-function-for-ef-hasdata.md.";

    public DecentDBByteArrayTypeMapping()
        : base("BLOB", System.Data.DbType.Binary)
    {
    }

    private DecentDBByteArrayTypeMapping(RelationalTypeMappingParameters parameters)
        : base(parameters)
    {
    }

    protected override RelationalTypeMapping Clone(RelationalTypeMappingParameters parameters)
        => new DecentDBByteArrayTypeMapping(parameters);

    public override string GenerateSqlLiteral(object? value)
        => value is null ? "NULL" : throw CreateLiteralNotSupported();

    protected override string GenerateNonNullSqlLiteral(object value)
        => throw CreateLiteralNotSupported();

    private static NotSupportedException CreateLiteralNotSupported()
        => new(BlobLiteralError);
}
