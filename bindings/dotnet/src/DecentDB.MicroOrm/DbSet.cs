using System;
using System.Collections;
using System.Collections.Generic;
using System.Data;
using System.Data.Common;
using System.Linq;
using System.Linq.Expressions;
using System.Reflection;
using System.Runtime.CompilerServices;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using DecentDB.AdoNet;

namespace DecentDB.MicroOrm;

public sealed class DbSet<T> : IQueryable<T> where T : class, new()
{
    private readonly DecentDBContext _context;
    private readonly EntityMap _map;
    private readonly List<Expression<Func<T, bool>>> _where;
    private readonly List<(PropertyInfo Property, bool Desc)> _orderBy;
    private readonly int? _skip;
    private readonly int? _take;

    private IQueryProvider? _queryProvider;

    public DbSet(DecentDBContext context)
        : this(context, EntityMap.For<T>(), new(), new(), null, null)
    {
    }

    private DbSet(
        DecentDBContext context,
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

        using var scope = _context.AcquireConnectionScope();
        using var cmd = CreateCommand(scope.Connection, sql, parameters);
        using var reader = await cmd.ExecuteReaderAsync(cancellationToken);

        var mapper = FastMaterializer<T>.Bind(_map, reader);

        var result = new List<T>();
        while (await reader.ReadAsync(cancellationToken))
        {
            result.Add(mapper(reader));
        }
        return result;
    }

    public async IAsyncEnumerable<T> StreamAsync([EnumeratorCancellation] CancellationToken cancellationToken = default)
    {
        var (sql, parameters) = BuildSelectSql(selectCount: false);

        using var scope = _context.AcquireConnectionScope();
        using var cmd = CreateCommand(scope.Connection, sql, parameters);
        using var reader = await cmd.ExecuteReaderAsync(cancellationToken);

        var mapper = FastMaterializer<T>.Bind(_map, reader);

        while (await reader.ReadAsync(cancellationToken))
        {
            yield return mapper(reader);
        }
    }

    public async Task<T?> FirstOrDefaultAsync(CancellationToken cancellationToken = default)
    {
        var list = await Take(1).ToListAsync(cancellationToken);
        return list.Count == 0 ? null : list[0];
    }

    public Task<T?> FirstOrDefaultAsync(Expression<Func<T, bool>> predicate, CancellationToken cancellationToken = default)
    {
        if (predicate == null) throw new ArgumentNullException(nameof(predicate));
        return Where(predicate).FirstOrDefaultAsync(cancellationToken);
    }

    public async Task<T> FirstAsync(CancellationToken cancellationToken = default)
    {
        var item = await FirstOrDefaultAsync(cancellationToken);
        if (item == null) throw new InvalidOperationException("Sequence contains no elements");
        return item;
    }

    public Task<T> FirstAsync(Expression<Func<T, bool>> predicate, CancellationToken cancellationToken = default)
    {
        if (predicate == null) throw new ArgumentNullException(nameof(predicate));
        return Where(predicate).FirstAsync(cancellationToken);
    }

    public async Task<long> CountAsync(CancellationToken cancellationToken = default)
    {
        var (sql, parameters) = BuildSelectSql(selectCount: true);
        using var scope = _context.AcquireConnectionScope();
        using var cmd = CreateCommand(scope.Connection, sql, parameters);
        var scalar = await cmd.ExecuteScalarAsync(cancellationToken);
        return scalar == null ? 0 : Convert.ToInt64(scalar);
    }

    public Task<long> CountAsync(Expression<Func<T, bool>> predicate, CancellationToken cancellationToken = default)
    {
        if (predicate == null) throw new ArgumentNullException(nameof(predicate));
        return Where(predicate).CountAsync(cancellationToken);
    }

    public async Task<bool> AnyAsync(CancellationToken cancellationToken = default)
    {
        var (sql, parameters) = BuildSelectSql(selectCount: false, selectExists: true, overrideTake: 1);
        using var scope = _context.AcquireConnectionScope();
        using var cmd = CreateCommand(scope.Connection, sql, parameters);
        using var reader = await cmd.ExecuteReaderAsync(cancellationToken);
        return await reader.ReadAsync(cancellationToken);
    }

    public Task<bool> AnyAsync(Expression<Func<T, bool>> predicate, CancellationToken cancellationToken = default)
    {
        if (predicate == null) throw new ArgumentNullException(nameof(predicate));
        return Where(predicate).AnyAsync(cancellationToken);
    }

    public async Task<T?> GetAsync(object id, CancellationToken cancellationToken = default)
    {
        var pkCol = _map.PrimaryKeyColumnName;
        var sql = $"SELECT * FROM {_map.TableName} WHERE {pkCol} = @p0 LIMIT 1";
        using var scope = _context.AcquireConnectionScope();
        using var cmd = CreateCommand(scope.Connection, sql, new (string Name, object? Value, int? MaxLength)[] { ("@p0", (object?)id, null) });
        using var reader = await cmd.ExecuteReaderAsync(cancellationToken);
        if (!await reader.ReadAsync(cancellationToken)) return null;
        var mapper = FastMaterializer<T>.Bind(_map, reader);
        return mapper(reader);
    }

    public async Task InsertAsync(T entity, CancellationToken cancellationToken = default)
    {
        var cols = new List<string>();
        var vals = new List<string>();
        var parameters = new List<(string Name, object? Value, int? MaxLength)>();

        var pk = _map.PrimaryKey;
        bool omitPk = false;

        foreach (var prop in _map.Properties)
        {
            if (prop.IsIgnored) continue;

            // Omit integer PK with default value — engine will auto-assign
            if (prop.IsPrimaryKey && pk != null && IsDefaultIntegerPk(pk, entity))
            {
                omitPk = true;
                continue;
            }

            var v = prop.Property.GetValue(entity);
            if (!prop.IsNullable && v == null)
            {
                throw new ArgumentException($"Property '{prop.Property.Name}' cannot be NULL.");
            }

            cols.Add(prop.ColumnName);
            var paramName = $"@p{parameters.Count}";
            vals.Add(paramName);
            parameters.Add((paramName, v, prop.MaxLength));
        }

        var sql = $"INSERT INTO {_map.TableName} ({string.Join(", ", cols)}) VALUES ({string.Join(", ", vals)})";

        if (omitPk)
        {
            sql += $" RETURNING {_map.PrimaryKeyColumnName}";
        }

        using var scope = _context.AcquireConnectionScope();
        using var cmd = CreateCommand(scope.Connection, sql, parameters);

        if (omitPk)
        {
            using var reader = await cmd.ExecuteReaderAsync(cancellationToken);
            if (await reader.ReadAsync(cancellationToken))
            {
                var returnedId = reader.GetInt64(0);
                pk!.SetValue(entity, Convert.ChangeType(returnedId, pk.PropertyType));
            }
        }
        else
        {
            await cmd.ExecuteNonQueryAsync(cancellationToken);
        }
    }

    private static bool IsDefaultIntegerPk(PropertyInfo pk, T entity)
    {
        var pkType = pk.PropertyType;
        if (pkType == typeof(long) || pkType == typeof(int) || pkType == typeof(short))
        {
            var val = pk.GetValue(entity);
            if (val == null) return true;
            return Convert.ToInt64(val) == 0;
        }
        return false;
    }

    public async Task InsertManyAsync(IEnumerable<T> entities, CancellationToken cancellationToken = default)
    {
        if (entities == null) throw new ArgumentNullException(nameof(entities));

        // Keep connection stable for the whole batch.
        using var scope = _context.AcquireConnectionScope();

        var ownsTx = _context.CurrentTransaction == null;
        DbTransaction? tx = ownsTx ? scope.Connection.BeginTransaction() : _context.CurrentTransaction;

        // Pre-build SQL shape once.
        var cols = new List<string>();
        var vals = new List<string>();
        for (var i = 0; i < _map.Properties.Length; i++)
        {
            var prop = _map.Properties[i];
            if (prop.IsIgnored) continue;
            cols.Add(prop.ColumnName);
            vals.Add($"@p{vals.Count}");
        }
        var sql = $"INSERT INTO {_map.TableName} ({string.Join(", ", cols)}) VALUES ({string.Join(", ", vals)})";

        try
        {
            foreach (var entity in entities)
            {
                var parameters = new List<(string Name, object? Value, int? MaxLength)>();
                foreach (var prop in _map.Properties)
                {
                    if (prop.IsIgnored) continue;

                    var v = prop.Property.GetValue(entity);
                    if (!prop.IsNullable && v == null)
                    {
                        throw new ArgumentException($"Property '{prop.Property.Name}' cannot be NULL.");
                    }

                    var name = $"@p{parameters.Count}";
                    parameters.Add((name, v, prop.MaxLength));
                }

                using var cmd = CreateCommand(scope.Connection, sql, parameters);
                if (tx != null) cmd.Transaction = tx;
                await cmd.ExecuteNonQueryAsync(cancellationToken);
            }

            if (ownsTx)
            {
                tx!.Commit();
            }
        }
        catch
        {
            if (ownsTx)
            {
                try { tx!.Rollback(); } catch { /* ignore rollback failures */ }
            }
            throw;
        }
        finally
        {
            if (ownsTx)
            {
                tx?.Dispose();
            }
        }
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

            var v = prop.Property.GetValue(entity);
            if (!prop.IsNullable && v == null)
            {
                throw new ArgumentException($"Property '{prop.Property.Name}' cannot be NULL.");
            }

            var paramName = $"@p{parameters.Count}";
            sets.Add($"{prop.ColumnName} = {paramName}");
            parameters.Add((paramName, v, prop.MaxLength));
        }

        var pkParam = $"@p{parameters.Count}";
        parameters.Add((pkParam, pkVal, null));

        var sql = $"UPDATE {_map.TableName} SET {string.Join(", ", sets)} WHERE {pkCol} = {pkParam}";
        using var scope = _context.AcquireConnectionScope();
        using var cmd = CreateCommand(scope.Connection, sql, parameters);
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
        using var scope = _context.AcquireConnectionScope();
        using var cmd = CreateCommand(scope.Connection, sql, new (string Name, object? Value, int? MaxLength)[] { ("@p0", (object?)id, null) });
        await cmd.ExecuteNonQueryAsync(cancellationToken);
    }

    public async Task<long> DeleteManyAsync(Expression<Func<T, bool>> predicate, CancellationToken cancellationToken = default)
    {
        if (predicate == null) throw new ArgumentNullException(nameof(predicate));

        var builder = new ExpressionSqlBuilder<T>(_map);
        var (whereSql, whereParams) = builder.BuildWhere(predicate);

        var sql = $"DELETE FROM {_map.TableName} WHERE {whereSql}";
        using var scope = _context.AcquireConnectionScope();
        using var cmd = CreateCommand(scope.Connection, sql, whereParams);
        var rows = await cmd.ExecuteNonQueryAsync(cancellationToken);
        return rows;
    }

    public async Task UpsertAsync(T entity, CancellationToken cancellationToken = default)
    {
        var pk = _map.PrimaryKey ?? throw new InvalidOperationException("Missing primary key");
        var pkCol = _map.PrimaryKeyColumnName;

        var cols = new List<string>();
        var vals = new List<string>();
        var sets = new List<string>();
        var parameters = new List<(string Name, object? Value, int? MaxLength)>();

        foreach (var prop in _map.Properties)
        {
            if (prop.IsIgnored) continue;
            var v = prop.Property.GetValue(entity);
            var paramName = $"@p{parameters.Count}";
            cols.Add(prop.ColumnName);
            vals.Add(paramName);
            parameters.Add((paramName, v, prop.MaxLength));

            if (!prop.IsPrimaryKey)
            {
                sets.Add($"{prop.ColumnName} = {paramName}");
            }
        }

        var sql = $"INSERT INTO {_map.TableName} ({string.Join(", ", cols)}) VALUES ({string.Join(", ", vals)}) ON CONFLICT ({pkCol}) DO UPDATE SET {string.Join(", ", sets)}";
        using var scope = _context.AcquireConnectionScope();
        using var cmd = CreateCommand(scope.Connection, sql, parameters);
        await cmd.ExecuteNonQueryAsync(cancellationToken);
    }

    public async Task InsertOrIgnoreAsync(T entity, CancellationToken cancellationToken = default)
    {
        var cols = new List<string>();
        var vals = new List<string>();
        var parameters = new List<(string Name, object? Value, int? MaxLength)>();

        foreach (var prop in _map.Properties)
        {
            if (prop.IsIgnored) continue;
            var v = prop.Property.GetValue(entity);
            var paramName = $"@p{parameters.Count}";
            cols.Add(prop.ColumnName);
            vals.Add(paramName);
            parameters.Add((paramName, v, prop.MaxLength));
        }

        var sql = $"INSERT INTO {_map.TableName} ({string.Join(", ", cols)}) VALUES ({string.Join(", ", vals)}) ON CONFLICT DO NOTHING";
        using var scope = _context.AcquireConnectionScope();
        using var cmd = CreateCommand(scope.Connection, sql, parameters);
        await cmd.ExecuteNonQueryAsync(cancellationToken);
    }

    public async Task<T?> SingleOrDefaultAsync(CancellationToken cancellationToken = default)
    {
        var list = await Take(2).ToListAsync(cancellationToken);
        if (list.Count == 0) return null;
        if (list.Count > 1) throw new InvalidOperationException("Sequence contains more than one element");
        return list[0];
    }

    public async Task<T> SingleAsync(CancellationToken cancellationToken = default)
    {
        var item = await SingleOrDefaultAsync(cancellationToken);
        if (item == null) throw new InvalidOperationException("Sequence contains no elements");
        return item;
    }

    /// <summary>
    /// Projects each entity to a subset of columns using a selector expression.
    /// Only simple property access is supported (e.g. x => new { x.Name, x.Age }).
    /// </summary>
    public async Task<List<TResult>> SelectAsync<TResult>(
        Expression<Func<T, TResult>> selector,
        CancellationToken cancellationToken = default)
    {
        var columns = ExtractProjectionColumns(selector);
        var (sql, parameters) = BuildSelectSql(selectCount: false, projectionColumns: columns);

        using var scope = _context.AcquireConnectionScope();
        using var cmd = CreateCommand(scope.Connection, sql, parameters);
        using var reader = await cmd.ExecuteReaderAsync(cancellationToken);

        var compiled = selector.Compile();
        var entityMapper = FastMaterializer<T>.Bind(_map, reader);
        var list = new List<TResult>();
        while (await reader.ReadAsync(cancellationToken))
        {
            var entity = entityMapper(reader);
            list.Add(compiled(entity));
        }
        return list;
    }

    private List<string> ExtractProjectionColumns(LambdaExpression selector)
    {
        var cols = new List<string>();
        switch (selector.Body)
        {
            case MemberExpression me when me.Member is PropertyInfo pi:
                cols.Add(_map.GetPropertyMap(pi).ColumnName);
                break;
            case NewExpression ne:
                foreach (var arg in ne.Arguments)
                {
                    if (arg is MemberExpression ma && ma.Member is PropertyInfo pa)
                        cols.Add(_map.GetPropertyMap(pa).ColumnName);
                }
                break;
            case MemberInitExpression mi:
                foreach (var binding in mi.Bindings)
                {
                    if (binding is MemberAssignment assign && assign.Expression is MemberExpression mae && mae.Member is PropertyInfo pai)
                        cols.Add(_map.GetPropertyMap(pai).ColumnName);
                }
                break;
            default:
                throw new NotSupportedException("Projection selector must use property access or anonymous type constructor");
        }
        return cols;
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

    private (string Sql, List<(string Name, object? Value, int? MaxLength)> Parameters) BuildSelectSql(
        bool selectCount,
        bool selectExists = false,
        int? overrideTake = null,
        List<string>? projectionColumns = null)
    {
        var sb = new StringBuilder();
        var parameters = new List<(string Name, object? Value, int? MaxLength)>();

        if (selectCount)
        {
            sb.Append("SELECT COUNT(*)");
        }
        else if (selectExists)
        {
            sb.Append("SELECT 1");
        }
        else if (projectionColumns is { Count: > 0 })
        {
            sb.Append("SELECT ");
            sb.Append(string.Join(", ", projectionColumns));
        }
        else
        {
            sb.Append("SELECT *");
        }
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

        if (!selectCount && !selectExists && _orderBy.Count > 0)
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

        var take = overrideTake ?? _take;
        if (!selectCount && take.HasValue)
        {
            sb.Append(" LIMIT ");
            sb.Append(take.Value);
        }

        if (!selectCount && _skip.HasValue)
        {
            sb.Append(" OFFSET ");
            sb.Append(_skip.Value);
        }

        return (sb.ToString(), parameters);
    }

    private DbCommand CreateCommand(DecentDBConnection conn, string sql, IEnumerable<(string Name, object? Value, int? MaxLength)> parameters)
    {
        var cmd = conn.CreateCommand();
        cmd.CommandText = sql;

        if (_context.CurrentTransaction != null)
        {
            cmd.Transaction = _context.CurrentTransaction;
        }

        // Transaction is associated with the context's shared connection.
        // If the provider/connection doesn't support ambient transactions, it'll throw here.
        // (In non-pooled mode, transactions should be explicitly managed.)

        // Note: DecentDBCommand exposes Transaction via DbCommand.Transaction.
        // We set it only when we have an active context transaction.
        // Caller passes the right connection scope.

        foreach (var (name, value, maxLength) in parameters)
        {
            var p = cmd.CreateParameter();
            p.ParameterName = name;
            p.Value = DefaultTypeConverters.ToDbValue(value);
            if (maxLength.HasValue)
            {
                // Guardrail uses UTF-8 bytes; enforced in provider binding.
                p.Size = maxLength.Value;
            }
            cmd.Parameters.Add(p);
        }

        return cmd;
    }

    Type IQueryable.ElementType => typeof(T);

    Expression IQueryable.Expression => Expression.Constant(this);

    IQueryProvider IQueryable.Provider => _queryProvider ??= new DecentDBQueryProvider<T>(this);

    IEnumerator<T> IEnumerable<T>.GetEnumerator()
    {
        var provider = ((IQueryable)this).Provider;
        var expr = ((IQueryable)this).Expression;
        var enumerable = provider.Execute<IEnumerable<T>>(expr);
        return enumerable.GetEnumerator();
    }

    IEnumerator IEnumerable.GetEnumerator() => ((IEnumerable<T>)this).GetEnumerator();
}
