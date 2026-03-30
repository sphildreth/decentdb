namespace DecentDb.ShowCase.Entities;

public class AllTypesNullableDemo
{
    public long Id { get; set; }

    // Integral numeric types
    public sbyte? SignedByte { get; set; }
    public byte? UnsignedByte { get; set; }
    public short? Int16 { get; set; }
    public ushort? UInt16 { get; set; }
    public int? Int32 { get; set; }
    public uint? UInt32 { get; set; }
    public long? Int64 { get; set; }
    public ulong? UInt64 { get; set; }

    // Floating-point numeric types
    public float? Single { get; set; }
    public double? Double { get; set; }
    public decimal? Decimal { get; set; }

    // Logical type
    public bool? Boolean { get; set; }

    // Text type
    public char? Character { get; set; }

    // String type
    public string? Text { get; set; }

    // Date/Time types
    public DateTime? DateTime { get; set; }
    public DateOnly? DateOnly { get; set; }
    public TimeOnly? TimeOnly { get; set; }

    // Other
    public Guid? Guid { get; set; }
}
