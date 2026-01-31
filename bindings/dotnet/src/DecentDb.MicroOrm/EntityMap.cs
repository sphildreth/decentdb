using System;
using System.Collections.Concurrent;
using System.ComponentModel.DataAnnotations;
using System.Reflection;

namespace DecentDb.MicroOrm;

internal sealed record PropertyMap(
    PropertyInfo Property,
    string ColumnName,
    bool IsPrimaryKey,
    bool IsIgnored,
    int? MaxLength);

internal sealed class EntityMap
{
    private static readonly ConcurrentDictionary<Type, EntityMap> Cache = new();

    public static EntityMap For<T>() => For(typeof(T));

    public static EntityMap For(Type entityType)
    {
        return Cache.GetOrAdd(entityType, static t => new EntityMap(t));
    }

    private EntityMap(Type entityType)
    {
        EntityType = entityType;

        TableName = entityType.GetCustomAttribute<TableAttribute>()?.Name
            ?? Conventions.DefaultTableName(entityType);

        var props = entityType.GetProperties(BindingFlags.Instance | BindingFlags.Public);

        var mapped = new List<PropertyMap>(props.Length);

        PropertyInfo? pk = null;
        foreach (var prop in props)
        {
            if (!prop.CanRead || !prop.CanWrite) continue;
            if (prop.GetIndexParameters().Length != 0) continue;

            var isIgnored = prop.GetCustomAttribute<IgnoreAttribute>() != null;
            var isPk = prop.GetCustomAttribute<PrimaryKeyAttribute>() != null || string.Equals(prop.Name, "Id", StringComparison.Ordinal);

            var colName = prop.GetCustomAttribute<ColumnAttribute>()?.Name
                ?? Conventions.DefaultColumnName(prop.Name);

            var maxLengthAttr = prop.GetCustomAttribute<MaxLengthAttribute>();
            int? maxLength = maxLengthAttr?.Length;

            mapped.Add(new PropertyMap(prop, colName, isPk, isIgnored, maxLength));

            if (isPk && !isIgnored)
            {
                pk ??= prop;
            }
        }

        Properties = mapped.Where(p => !p.IsIgnored).ToArray();
        PrimaryKey = pk;

        if (PrimaryKey == null)
        {
            throw new InvalidOperationException($"Entity type '{entityType.FullName}' must have a primary key (property named 'Id' or marked [PrimaryKey]).");
        }
    }

    public Type EntityType { get; }
    public string TableName { get; }

    public PropertyInfo? PrimaryKey { get; }

    public PropertyMap[] Properties { get; }

    public PropertyMap[] NonPrimaryKeyProperties => Properties.Where(p => !p.IsPrimaryKey).ToArray();

    public string PrimaryKeyColumnName
    {
        get
        {
            var pkProp = PrimaryKey ?? throw new InvalidOperationException("Primary key missing");
            return pkProp.GetCustomAttribute<ColumnAttribute>()?.Name
                ?? Conventions.DefaultColumnName(pkProp.Name);
        }
    }

    public PropertyMap GetPropertyMap(PropertyInfo property)
    {
        foreach (var p in Properties)
        {
            if (p.Property == property) return p;
        }
        throw new InvalidOperationException($"Property '{property.Name}' is not mapped for '{EntityType.FullName}'.");
    }
}
