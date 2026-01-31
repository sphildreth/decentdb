using System;

namespace DecentDb.MicroOrm;

[AttributeUsage(AttributeTargets.Class, Inherited = false, AllowMultiple = false)]
public sealed class TableAttribute(string name) : Attribute
{
    public string Name { get; } = name;
}

[AttributeUsage(AttributeTargets.Property, Inherited = true, AllowMultiple = false)]
public sealed class ColumnAttribute(string name) : Attribute
{
    public string Name { get; } = name;
}

[AttributeUsage(AttributeTargets.Property, Inherited = true, AllowMultiple = false)]
public sealed class PrimaryKeyAttribute : Attribute;

[AttributeUsage(AttributeTargets.Property, Inherited = true, AllowMultiple = false)]
public sealed class IgnoreAttribute : Attribute;
