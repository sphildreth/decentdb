using System;
using System.Collections.Generic;
using System.Data;
using System.Data.Common;
using System.Linq.Expressions;
using System.Reflection;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using DecentDb.AdoNet;

namespace DecentDb.MicroOrm;

public sealed class DbSet<T> where T : class, new()
{
    private readonly DecentDbContext _context;
    private readonly EntityMap _map;
    private readonly List<Expression<Func<T, bool>>> _where;
    private readonly List<(PropertyInfo Property, bool Desc)> _orderBy;
    private readonly int? _skip;
    private readonly int? _take;

    public DbSet(DecentDbContext context)
        : this(context, EntityMap.For<T>(), new(), new(), null, null)
    {
    }

    private DbSet(
        DecentDbContext context,
        EntityMap map,
        List<Expression<Func<T, bool>>> where,
        List<(PropertyInfo Property, bool Desc)> orderBy,
        int? skip,
        int? take)
    {
        _context = context;
        _map = map;
        _where = where;
        _orderBy = orderBy;
        _skip = skip;
        _take = take;
    }

    public DbSet<T> Where(Expression<Func<T, bool>> predicate)
    {
        var next = new List<Expression<Func<T, bool>>>(_where) { predicate };
        return new DbSet<T>(_context, _map, next, new List<(PropertyInfo Property, bool Desc)>(_orderBy), _skip, _take);
    }

    public DbSet<T> OrderBy<TValue>(Expression<Func<T, TValue>> keySelector) => AddOrderBy(keySelector, desc: false, thenBy: false);

    public DbSet<T> OrderByDescending<TValue>(Expression<Func<T, TValue>> keySelector) => AddOrderBy(keySelector, desc: true, thenBy: false);

    public DbSet<T> ThenBy<TValue>(Expression<Func<T, TValue>> keySelector) => AddOrderBy(keySelector, desc: false, thenBy: true);

    public DbSet<T> ThenByDescending<TValue>(Expression<Func<T, TValue>> keySelector) => AddOrderBy(keySelector, desc: true, thenBy: true);

    public DbSet<T> Skip(int count)
    {
        if (count < 0) throw new ArgumentOutOfRangeException(nameof(count));
        return new DbSet<T>(_context, _map, new List<Expression<Func<T, bool>>>(_where), new List<(PropertyInfo Property, bool Desc)>(_orderBy), count, _take);
    }

    public DbSet<T> Take(int count)
    {
        if (count < 0) throw new ArgumentOutOfRangeException(nameof(count));
        return new DbSet<T>(_context, _map, new List<Expression<Func<T, bool>>>(_where), new List<(PropertyInfo Property, bool Desc)>(_orderBy), _skip, count);
    }

    public async Task<List<T>> ToListAsync(CancellationToken cancellationToken = default)
    {
        var (sql, parameters) = BuildSelectSql(selectCount: false);

        using var cmd = CreateCommand(sql, parameters);
        using var reader = await cmd.ExecuteReaderAsync(cancellationToken);

        var mapper = RowMapperCache<T>.GetOrCreate(_map);

        var result = new List<T>();
        while (await reader.ReadAsync(cancellationToken))
        {
            result.Add(mapper(reader));
        }
        return result;
    }

    public async Task<T?> FirstOrDefaultAsync(CancellationToken cancellationToken = default)
    {
        var list = await Take(1).ToListAsync(cancellationToken);
        return list.Count == 0 ? null : list[0];
    }

    public async Task<T> FirstAsync(CancellationToken cancellationToken = default)
    {
        var item = await FirstOrDefaultAsync(cancellationToken);
        if (item == null) throw new InvalidOperationException("Sequence contains no elements");
        return item;
    }

    public async Task<long> CountAsync(CancellationToken cancellationToken = default)
    {
        var (sql, parameters) = BuildSelectSql(selectCount: true);
        using var cmd = CreateCommand(sql, parameters);
        var scalar = await cmd.ExecuteScalarAsync(cancellationToken);
        return scalar == null ? 0 : Convert.ToInt64(scalar);
    }

    public async Task<T?> GetAsync(object id, CancellationToken cancellationToken = default)
    {
        var pkCol = _map.PrimaryKeyColumnName;
        var sql = $"SELECT * FROM {_map.TableName} WHERE {pkCol} = @p0 LIMIT 1";
        using var cmd = CreateCommand(sql, new (string Name, object? Value, int? MaxLength)[] { ("@p0", (object?)id, null) });
        using var reader = await cmd.ExecuteReaderAsync(cancellationToken);
        if (!await reader.ReadAsync(cancellationToken)) return null;
        var mapper = RowMapperCache<T>.GetOrCreate(_map);
        return mapper(reader);
    }

    public async Task InsertAsync(T entity, CancellationToken cancellationToken = default)
    {
        var cols = new List<string>();
        var vals = new List<string>();
        var parameters = new List<(string Name, object? Value, int? MaxLength)>();

        foreach (var prop in _map.Properties)
        {
            if (prop.IsIgnored) continue;

            cols.Add(prop.ColumnName);
            var paramName = $"@p{parameters.Count}";
            vals.Add(paramName);
            parameters.Add((paramName, prop.Property.GetValue(entity), prop.MaxLength));
        }

        var sql = $"INSERT INTO {_map.TableName} ({string.Join(", ", cols)}) VALUES ({string.Join(", ", vals)})";
        using var cmd = CreateCommand(sql, parameters);
        await cmd.ExecuteNonQueryAsync(cancellationToken);
    }

    public async Task UpdateAsync(T entity, CancellationToken cancellationToken = default)
    {
        var pk = _map.PrimaryKey ?? throw new InvalidOperationException("Missing primary key");
        var pkCol = _map.PrimaryKeyColumnName;
        var pkVal = pk.GetValue(entity);

        var sets = new List<string>();
        var parameters = new List<(string Name, object? Value, int? MaxLength)>();

        foreach (var prop in _map.Properties)
        {
            if (prop.IsIgnored || prop.IsPrimaryKey) continue;

            var paramName = $"@p{parameters.Count}";
            sets.Add($"{prop.ColumnName} = {paramName}");
            parameters.Add((paramName, prop.Property.GetValue(entity), prop.MaxLength));
        }

        var pkParam = $"@p{parameters.Count}";
        parameters.Add((pkParam, pkVal, null));

        var sql = $"UPDATE {_map.TableName} SET {string.Join(", ", sets)} WHERE {pkCol} = {pkParam}";
        using var cmd = CreateCommand(sql, parameters);
        await cmd.ExecuteNonQueryAsync(cancellationToken);
    }

    public async Task DeleteAsync(T entity, CancellationToken cancellationToken = default)
    {
        var pk = _map.PrimaryKey ?? throw new InvalidOperationException("Missing primary key");
        var pkVal = pk.GetValue(entity);
        await DeleteByIdAsync(pkVal!, cancellationToken);
    }

    public async Task DeleteByIdAsync(object id, CancellationToken cancellationToken = default)
    {
        var pkCol = _map.PrimaryKeyColumnName;
        var sql = $"DELETE FROM {_map.TableName} WHERE {pkCol} = @p0";
        using var cmd = CreateCommand(sql, new (string Name, object? Value, int? MaxLength)[] { ("@p0", (object?)id, null) });
        await cmd.ExecuteNonQueryAsync(cancellationToken);
    }

    private DbSet<T> AddOrderBy<TValue>(Expression<Func<T, TValue>> keySelector, bool desc, bool thenBy)
    {
        var member = keySelector.Body is UnaryExpression ue ? ue.Operand : keySelector.Body;
        if (member is not MemberExpression me || me.Member is not PropertyInfo pi)
        {
            throw new NotSupportedException("OrderBy key selector must be a property access");
        }

        if (!thenBy)
        {
            return new DbSet<T>(_context, _map, new List<Expression<Func<T, bool>>>(_where), new List<(PropertyInfo Property, bool Desc)>{ (pi, desc) }, _skip, _take);
        }

        var nextOrder = new List<(PropertyInfo Property, bool Desc)>(_orderBy) { (pi, desc) };
        return new DbSet<T>(_context, _map, new List<Expression<Func<T, bool>>>(_where), nextOrder, _skip, _take);
    }

    private (string Sql, List<(string Name, object? Value, int? MaxLength)> Parameters) BuildSelectSql(bool selectCount)
    {
        var sb = new StringBuilder();
        var parameters = new List<(string Name, object? Value, int? MaxLength)>();

        sb.Append(selectCount ? "SELECT COUNT(*)" : "SELECT *");
        sb.Append(" FROM ");
        sb.Append(_map.TableName);

        if (_where.Count > 0)
        {
            sb.Append(" WHERE ");

            for (var i = 0; i < _where.Count; i++)
            {
                if (i != 0) sb.Append(" AND ");

                var builder = new ExpressionSqlBuilder<T>(_map);
                var (whereSql, whereParams) = builder.BuildWhere(_where[i]);

                // Remap parameter names to keep @p0.. contiguous across multiple predicates.
                var rewritten = whereSql;
                foreach (var p in whereParams)
                {
                    var newName = $"@p{parameters.Count}";
                    rewritten = rewritten.Replace(p.Name, newName, StringComparison.Ordinal);
                    parameters.Add((newName, p.Value, p.MaxLength));
                }

                sb.Append(rewritten);
            }
        }

        if (!selectCount && _orderBy.Count > 0)
        {
            sb.Append(" ORDER BY ");
            for (var i = 0; i < _orderBy.Count; i++)
            {
                if (i != 0) sb.Append(", ");

                var pm = _map.GetPropertyMap(_orderBy[i].Property);
                sb.Append(pm.ColumnName);
                sb.Append(_orderBy[i].Desc ? " DESC" : " ASC");
            }
        }

        if (!selectCount && _take.HasValue)
        {
            sb.Append(" LIMIT ");
            sb.Append(_take.Value);
        }

        if (!selectCount && _skip.HasValue)
        {
            sb.Append(" OFFSET ");
            sb.Append(_skip.Value);
        }

        return (sb.ToString(), parameters);
    }

    private DbCommand CreateCommand(string sql, IEnumerable<(string Name, object? Value, int? MaxLength)> parameters)
    {
        var conn = _context.GetConnection();
        var cmd = conn.CreateCommand();
        cmd.CommandText = sql;

        if (_context.CurrentTransaction != null)
        {
            cmd.Transaction = _context.CurrentTransaction;
        }

        foreach (var (name, value, maxLength) in parameters)
        {
            var p = cmd.CreateParameter();
            p.ParameterName = name;
            p.Value = value ?? DBNull.Value;
            if (maxLength.HasValue) p.Size = maxLength.Value;
            cmd.Parameters.Add(p);
        }

        return cmd;
    }
}

internal static class RowMapperCache<T> where T : class, new()
{
    private static readonly object Gate = new();
    private static Func<DbDataReader, T>? _cached;

    public static Func<DbDataReader, T> GetOrCreate(EntityMap map)
    {
        if (_cached != null) return _cached;

        lock (Gate)
        {
            if (_cached != null) return _cached;

            _cached = Create(map);
            return _cached;
        }
    }

    private static Func<DbDataReader, T> Create(EntityMap map)
    {
        // Minimal fast materializer: property name -> ordinal lookup once.
        return reader =>
        {
            var obj = new T();

            foreach (var pm in map.Properties)
            {
                var ordinal = reader.GetOrdinal(pm.ColumnName);
                var val = reader.GetValue(ordinal);
                if (val == DBNull.Value) continue;

                var targetType = Nullable.GetUnderlyingType(pm.Property.PropertyType) ?? pm.Property.PropertyType;

                object? converted = targetType.IsEnum
                    ? Enum.ToObject(targetType, Convert.ToInt64(val))
                    : Convert.ChangeType(val, targetType);

                pm.Property.SetValue(obj, converted);
            }

            return obj;
        };
    }
}
