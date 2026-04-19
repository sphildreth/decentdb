using Microsoft.EntityFrameworkCore.Storage;

namespace DecentDB.EntityFrameworkCore.Storage;

/// <summary>
/// Maps <see cref="bool"/> to the DecentDB <c>BOOLEAN</c> store type and emits
/// <c>TRUE</c>/<c>FALSE</c> SQL literals (DecentDB does not implicitly cast
/// <c>INT64</c> to <c>BOOL</c>).
/// </summary>
public sealed class DecentDBBoolTypeMapping : BoolTypeMapping
{
    public DecentDBBoolTypeMapping()
        : base("BOOLEAN", System.Data.DbType.Boolean)
    {
    }

    private DecentDBBoolTypeMapping(RelationalTypeMappingParameters parameters)
        : base(parameters)
    {
    }

    protected override RelationalTypeMapping Clone(RelationalTypeMappingParameters parameters)
        => new DecentDBBoolTypeMapping(parameters);

    protected override string GenerateNonNullSqlLiteral(object value)
        => (bool)value ? "TRUE" : "FALSE";
}
