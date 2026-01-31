using System;
using System.Collections.Generic;
using System.Data.Common;
using System.Linq.Expressions;
using System.Reflection;

namespace DecentDb.MicroOrm;

internal static class FastMaterializer<T> where T : class, new()
{
    private static readonly object Gate = new();
    private static Template? _template;

    public static Func<DbDataReader, T> Bind(EntityMap map, DbDataReader reader)
    {
        var template = GetOrCreateTemplate(map);

        var ordinals = new int[template.MappedProperties.Length];
        for (var i = 0; i < ordinals.Length; i++)
        {
            ordinals[i] = reader.GetOrdinal(template.MappedProperties[i].ColumnName);
        }

        return r =>
        {
            var obj = template.Factory();
            for (var i = 0; i < ordinals.Length; i++)
            {
                template.Assigners[i](obj, r, ordinals[i]);
            }
            return obj;
        };
    }

    private static Template GetOrCreateTemplate(EntityMap map)
    {
        if (_template != null) return _template;

        lock (Gate)
        {
            if (_template != null) return _template;
            _template = Template.Create(map);
            return _template;
        }
    }

    private sealed class Template
    {
        public required Func<T> Factory { get; init; }
        public required PropertyMap[] MappedProperties { get; init; }
        public required Action<T, DbDataReader, int>[] Assigners { get; init; }

        public static Template Create(EntityMap map)
        {
            var factory = Expression.Lambda<Func<T>>(Expression.New(typeof(T))).Compile();

            var props = map.Properties;
            var assigners = new Action<T, DbDataReader, int>[props.Length];

            for (var i = 0; i < props.Length; i++)
            {
                assigners[i] = CompileAssigner(props[i].Property);
            }

            return new Template
            {
                Factory = factory,
                MappedProperties = props,
                Assigners = assigners
            };
        }

        private static Action<T, DbDataReader, int> CompileAssigner(PropertyInfo property)
        {
            var objParam = Expression.Parameter(typeof(T), "obj");
            var readerParam = Expression.Parameter(typeof(DbDataReader), "reader");
            var ordinalParam = Expression.Parameter(typeof(int), "ordinal");

            var isDbNull = Expression.Call(readerParam, typeof(DbDataReader).GetMethod(nameof(DbDataReader.IsDBNull))!, ordinalParam);
            var getFieldValue = typeof(DbDataReader).GetMethod(nameof(DbDataReader.GetFieldValue))!.MakeGenericMethod(property.PropertyType);
            var readExpr = Expression.Call(readerParam, getFieldValue, ordinalParam);

            Expression valueExpr;
            if (!property.PropertyType.IsValueType || Nullable.GetUnderlyingType(property.PropertyType) != null)
            {
                valueExpr = Expression.Condition(
                    isDbNull,
                    Expression.Default(property.PropertyType),
                    readExpr);
            }
            else
            {
                // For non-nullable value types, leave default(T) if NULL.
                valueExpr = Expression.Condition(
                    isDbNull,
                    Expression.Default(property.PropertyType),
                    readExpr);
            }

            var assign = Expression.Assign(Expression.Property(objParam, property), valueExpr);
            return Expression.Lambda<Action<T, DbDataReader, int>>(assign, objParam, readerParam, ordinalParam).Compile();
        }
    }
}
