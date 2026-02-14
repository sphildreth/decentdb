using System.Data;
using System.Data.Common;
using System.Text;
using Microsoft.EntityFrameworkCore.Storage;
using Microsoft.EntityFrameworkCore.Update;
using Microsoft.EntityFrameworkCore;

namespace DecentDB.EntityFrameworkCore.Update.Internal;

internal sealed class DecentDBModificationCommandBatch : ModificationCommandBatch
{
    private readonly ModificationCommandBatchFactoryDependencies _dependencies;
    private readonly List<IReadOnlyModificationCommand> _commands = new();
    private bool _moreBatchesExpected;

    public DecentDBModificationCommandBatch(ModificationCommandBatchFactoryDependencies dependencies)
    {
        _dependencies = dependencies;
    }

    public override IReadOnlyList<IReadOnlyModificationCommand> ModificationCommands
        => _commands;

    public override bool RequiresTransaction
        => true;

    public override bool AreMoreBatchesExpected
        => _moreBatchesExpected;

    public override bool TryAddCommand(IReadOnlyModificationCommand modificationCommand)
    {
        _commands.Add(modificationCommand);
        return true;
    }

    public override void Complete(bool moreBatchesExpected)
        => _moreBatchesExpected = moreBatchesExpected;

    public override void Execute(IRelationalConnection connection)
    {
        var wasOpen = connection.DbConnection.State == ConnectionState.Open;
        if (!wasOpen)
        {
            connection.Open();
        }

        try
        {
            var nativeDb = (connection.DbConnection as AdoNet.DecentDBConnection)?.GetNativeDb();
            if (nativeDb != null)
            {
                ExecuteWithStatementReuse(connection.DbConnection, nativeDb);
            }
            else
            {
                foreach (var command in _commands)
                {
                    ExecuteCommand(connection.DbConnection, command);
                }
            }
        }
        finally
        {
            if (!wasOpen)
            {
                connection.Close();
            }
        }
    }

    /// <summary>
    /// Executes commands with prepared statement reuse. Commands with the same SQL
    /// shape (same table, entity state, and column set) share a single prepared
    /// statement via Reset/ClearBindings/rebind, avoiding re-parsing overhead.
    /// </summary>
    private void ExecuteWithStatementReuse(DbConnection dbConnection, Native.DecentDB nativeDb)
    {
        string? currentSql = null;
        Native.PreparedStatement? currentStmt = null;

        try
        {
            foreach (var command in _commands)
            {
                var columns = command.ColumnModifications;
                var readColumns = columns.Where(c => c.IsRead).ToList();
                var writeColumns = columns.Where(c => c.IsWrite).ToList();
                var conditionColumns = columns.Where(c => c.IsCondition).ToList();

                var sql = BuildSqlTemplate(command, writeColumns, readColumns, conditionColumns);

                if (currentStmt != null && currentSql == sql)
                {
                    currentStmt.Reset().ClearBindings();
                }
                else
                {
                    currentStmt?.Dispose();
                    currentStmt = null;
                    currentStmt = nativeDb.Prepare(sql);
                    currentSql = sql;
                }

                BindAllParameters(currentStmt, command, writeColumns, conditionColumns);
                ExecuteStepAndRead(currentStmt, command, readColumns);
            }
        }
        finally
        {
            currentStmt?.Dispose();
        }
    }

    /// <summary>
    /// Builds the SQL string for a command using $1, $2, ... positional parameters
    /// directly (DecentDB native format). Uses deterministic parameter ordering so
    /// identical column sets produce the same SQL, enabling prepared statement reuse.
    /// </summary>
    private string BuildSqlTemplate(
        IReadOnlyModificationCommand command,
        List<IColumnModification> writeColumns,
        List<IColumnModification> readColumns,
        List<IColumnModification> conditionColumns)
    {
        var table = _dependencies.SqlGenerationHelper.DelimitIdentifier(command.TableName, command.Schema);
        var sql = new StringBuilder();
        var paramOrdinal = 0;

        switch (command.EntityState)
        {
            case EntityState.Added:
                sql.Append("INSERT INTO ").Append(table);
                if (writeColumns.Count > 0)
                {
                    sql.Append(" (");
                    for (var i = 0; i < writeColumns.Count; i++)
                    {
                        if (i > 0) sql.Append(", ");
                        sql.Append(_dependencies.SqlGenerationHelper.DelimitIdentifier(writeColumns[i].ColumnName));
                    }
                    sql.Append(") VALUES (");
                    for (var i = 0; i < writeColumns.Count; i++)
                    {
                        if (i > 0) sql.Append(", ");
                        sql.Append('$').Append(++paramOrdinal);
                    }
                    sql.Append(')');
                }
                else
                {
                    sql.Append(" DEFAULT VALUES");
                }
                if (readColumns.Count > 0)
                {
                    sql.Append(" RETURNING ");
                    for (var i = 0; i < readColumns.Count; i++)
                    {
                        if (i > 0) sql.Append(", ");
                        sql.Append(_dependencies.SqlGenerationHelper.DelimitIdentifier(readColumns[i].ColumnName));
                    }
                }
                break;

            case EntityState.Modified:
                sql.Append("UPDATE ").Append(table).Append(" SET ");
                if (writeColumns.Count > 0)
                {
                    for (var i = 0; i < writeColumns.Count; i++)
                    {
                        if (i > 0) sql.Append(", ");
                        sql.Append(_dependencies.SqlGenerationHelper.DelimitIdentifier(writeColumns[i].ColumnName))
                            .Append(" = $").Append(++paramOrdinal);
                    }
                }
                else if (conditionColumns.Count > 0)
                {
                    var quoted = _dependencies.SqlGenerationHelper.DelimitIdentifier(conditionColumns[0].ColumnName);
                    sql.Append(quoted).Append(" = ").Append(quoted);
                }
                else
                {
                    sql.Append("1 = 1");
                }
                AppendWhereTemplate(sql, conditionColumns, ref paramOrdinal);
                break;

            case EntityState.Deleted:
                sql.Append("DELETE FROM ").Append(table);
                AppendWhereTemplate(sql, conditionColumns, ref paramOrdinal);
                break;

            default:
                throw new NotSupportedException($"Entity state '{command.EntityState}' is not supported.");
        }

        return sql.ToString();
    }

    private void AppendWhereTemplate(
        StringBuilder sql,
        List<IColumnModification> conditionColumns,
        ref int paramOrdinal)
    {
        if (conditionColumns.Count == 0) return;

        sql.Append(" WHERE ");
        var first = true;
        for (var i = 0; i < conditionColumns.Count; i++)
        {
            var column = conditionColumns[i];
            var value = ConvertToProviderValue(column, column.UseOriginalValue ? column.OriginalValue : column.Value);
            var colName = _dependencies.SqlGenerationHelper.DelimitIdentifier(column.ColumnName);

            if (value is null or DBNull)
            {
                if (!first) sql.Append(" AND ");
                sql.Append(colName).Append(" IS NULL");
                first = false;
            }
            else
            {
                if (!first) sql.Append(" AND ");
                sql.Append(colName).Append(" = $").Append(++paramOrdinal);
                first = false;
            }
        }
    }

    /// <summary>
    /// Binds parameter values directly to a native prepared statement.
    /// Parameter indices match the @p1, @p2, ... order from BuildSqlTemplate.
    /// </summary>
    private void BindAllParameters(
        Native.PreparedStatement stmt,
        IReadOnlyModificationCommand command,
        List<IColumnModification> writeColumns,
        List<IColumnModification> conditionColumns)
    {
        var paramIndex = 1; // 1-based to match $1, $2, ...

        // Write columns first (VALUES or SET clause)
        for (var i = 0; i < writeColumns.Count; i++)
        {
            var value = ConvertToProviderValue(writeColumns[i], writeColumns[i].Value);
            BindValue(stmt, paramIndex++, value);
        }

        // Condition columns (WHERE clause) — skip NULLs since they use IS NULL
        for (var i = 0; i < conditionColumns.Count; i++)
        {
            var value = ConvertToProviderValue(conditionColumns[i],
                conditionColumns[i].UseOriginalValue ? conditionColumns[i].OriginalValue : conditionColumns[i].Value);
            if (value is null or DBNull) continue;
            BindValue(stmt, paramIndex++, value);
        }
    }

    private static void BindValue(Native.PreparedStatement stmt, int index1Based, object? value)
    {
        if (value == null || value == DBNull.Value)
        {
            stmt.BindNull(index1Based);
            return;
        }

        var type = value.GetType();
        if (type == typeof(long) || type == typeof(int) || type == typeof(short) || type == typeof(byte))
            stmt.BindInt64(index1Based, Convert.ToInt64(value));
        else if (type == typeof(ulong) || type == typeof(uint) || type == typeof(ushort))
            stmt.BindInt64(index1Based, (long)Convert.ToUInt64(value));
        else if (type == typeof(double) || type == typeof(float))
            stmt.BindFloat64(index1Based, Convert.ToDouble(value));
        else if (type == typeof(decimal))
            stmt.BindDecimal(index1Based, (decimal)value);
        else if (type == typeof(bool))
            stmt.BindBool(index1Based, (bool)value);
        else if (type == typeof(string))
            stmt.BindText(index1Based, (string)value);
        else if (type == typeof(DateTime))
        {
            var dt = (DateTime)value;
            var utc = dt.Kind == DateTimeKind.Utc ? dt : dt.ToUniversalTime();
            stmt.BindInt64(index1Based, new DateTimeOffset(utc, TimeSpan.Zero).ToUnixTimeMilliseconds());
        }
        else if (type == typeof(DateTimeOffset))
            stmt.BindInt64(index1Based, ((DateTimeOffset)value).ToUniversalTime().ToUnixTimeMilliseconds());
        else if (type == typeof(TimeSpan))
            stmt.BindInt64(index1Based, ((TimeSpan)value).Ticks);
        else if (type == typeof(DateOnly))
        {
            var epoch = DateOnly.FromDateTime(DateTime.UnixEpoch);
            stmt.BindInt64(index1Based, ((DateOnly)value).DayNumber - epoch.DayNumber);
        }
        else if (type == typeof(TimeOnly))
            stmt.BindInt64(index1Based, ((TimeOnly)value).Ticks);
        else if (type == typeof(byte[]))
            stmt.BindBlob(index1Based, (byte[])value);
        else if (type == typeof(Guid))
            stmt.BindBlob(index1Based, ((Guid)value).ToByteArray());
        else if (type.IsEnum)
            stmt.BindInt64(index1Based, Convert.ToInt64(value));
        else
            throw new NotSupportedException($"Unsupported parameter type: {type.FullName}");
    }

    private void ExecuteStepAndRead(
        Native.PreparedStatement stmt,
        IReadOnlyModificationCommand command,
        List<IColumnModification> readColumns)
    {
        try
        {
            var stepResult = stmt.Step();
            if (stepResult < 0)
            {
                throw new Native.DecentDBException(stepResult, "Step failed", command.TableName);
            }

            if (readColumns.Count > 0 && stepResult > 0)
            {
                for (var i = 0; i < readColumns.Count; i++)
                {
                    object value;
                    if (stmt.IsNull(i))
                    {
                        value = DBNull.Value;
                    }
                    else
                    {
                        var colType = stmt.ColumnType(i);
                        value = colType switch
                        {
                            1 => (object)stmt.GetInt64(i),
                            3 => stmt.GetFloat64(i),
                            _ => stmt.GetText(i)
                        };
                    }
                    readColumns[i].Value = ConvertReadValue(readColumns[i], value);
                }
            }
            else if ((command.EntityState == EntityState.Modified || command.EntityState == EntityState.Deleted)
                     && stmt.RowsAffected != 1)
            {
                throw new DbUpdateConcurrencyException(
                    $"The database operation was expected to affect 1 row(s), but actually affected {stmt.RowsAffected} row(s).",
                    command.Entries);
            }
        }
        catch (DbUpdateException) { throw; }
        catch (Exception ex)
        {
            throw new DbUpdateException(
                $"DecentDB failed to execute {command.EntityState} command for table '{command.TableName}'.",
                ex, command.Entries);
        }
    }

    public override Task ExecuteAsync(IRelationalConnection connection, CancellationToken cancellationToken = default)
    {
        cancellationToken.ThrowIfCancellationRequested();
        Execute(connection);
        return Task.CompletedTask;
    }

    private void ExecuteCommand(DbConnection dbConnection, IReadOnlyModificationCommand command)
    {
        using var dbCommand = dbConnection.CreateCommand();

        var readColumns = BuildSqlAndParameters(dbCommand, command);

        try
        {
            if (readColumns.Count > 0)
            {
                using var reader = dbCommand.ExecuteReader();
                if (!reader.Read())
                {
                    throw new DbUpdateConcurrencyException(
                        "The database operation was expected to affect 1 row(s), but actually affected 0 row(s).",
                        command.Entries);
                }

                for (var i = 0; i < readColumns.Count; i++)
                {
                    var value = reader.GetValue(i);
                    readColumns[i].Value = ConvertReadValue(readColumns[i], value);
                }

                return;
            }

            var rowsAffected = dbCommand.ExecuteNonQuery();
            if ((command.EntityState == EntityState.Modified || command.EntityState == EntityState.Deleted) && rowsAffected != 1)
            {
                throw new DbUpdateConcurrencyException(
                    $"The database operation was expected to affect 1 row(s), but actually affected {rowsAffected} row(s).",
                    command.Entries);
            }
        }
        catch (DbUpdateException)
        {
            throw;
        }
        catch (Exception ex)
        {
            throw new DbUpdateException(
                $"DecentDB failed to execute {command.EntityState} command for table '{command.TableName}'.",
                ex,
                command.Entries);
        }
    }

    private List<IColumnModification> BuildSqlAndParameters(DbCommand dbCommand, IReadOnlyModificationCommand command)
    {
        var table = _dependencies.SqlGenerationHelper.DelimitIdentifier(command.TableName, command.Schema);
        var columns = command.ColumnModifications;
        var readColumns = columns.Where(c => c.IsRead).ToList();
        var writeColumns = columns.Where(c => c.IsWrite).ToList();
        var conditionColumns = columns.Where(c => c.IsCondition).ToList();
        var sql = new StringBuilder();
        var syntheticParamOrdinal = 0;

        switch (command.EntityState)
        {
            case EntityState.Added:
                sql.Append("INSERT INTO ").Append(table);
                if (writeColumns.Count > 0)
                {
                    sql.Append(" (");
                    for (var i = 0; i < writeColumns.Count; i++)
                    {
                        if (i > 0) sql.Append(", ");
                        sql.Append(_dependencies.SqlGenerationHelper.DelimitIdentifier(writeColumns[i].ColumnName));
                    }

                    sql.Append(") VALUES (");
                    for (var i = 0; i < writeColumns.Count; i++)
                    {
                        if (i > 0) sql.Append(", ");
                        var placeholder = AddCurrentValueParameter(dbCommand, writeColumns[i], ref syntheticParamOrdinal);
                        sql.Append(placeholder);
                    }

                    sql.Append(')');
                }
                else
                {
                    sql.Append(" DEFAULT VALUES");
                }

                if (readColumns.Count > 0)
                {
                    sql.Append(" RETURNING ");
                    for (var i = 0; i < readColumns.Count; i++)
                    {
                        if (i > 0) sql.Append(", ");
                        sql.Append(_dependencies.SqlGenerationHelper.DelimitIdentifier(readColumns[i].ColumnName));
                    }
                }
                break;

            case EntityState.Modified:
                sql.Append("UPDATE ").Append(table).Append(" SET ");
                if (writeColumns.Count > 0)
                {
                    for (var i = 0; i < writeColumns.Count; i++)
                    {
                        if (i > 0) sql.Append(", ");
                        var column = writeColumns[i];
                        var placeholder = AddCurrentValueParameter(dbCommand, column, ref syntheticParamOrdinal);
                        sql.Append(_dependencies.SqlGenerationHelper.DelimitIdentifier(column.ColumnName))
                            .Append(" = ")
                            .Append(placeholder);
                    }
                }
                else if (conditionColumns.Count > 0)
                {
                    var noOpColumn = conditionColumns[0];
                    var quoted = _dependencies.SqlGenerationHelper.DelimitIdentifier(noOpColumn.ColumnName);
                    sql.Append(quoted).Append(" = ").Append(quoted);
                }
                else
                {
                    sql.Append("1 = 1");
                }

                AppendWhereClause(sql, dbCommand, conditionColumns, ref syntheticParamOrdinal);
                break;

            case EntityState.Deleted:
                sql.Append("DELETE FROM ").Append(table);
                AppendWhereClause(sql, dbCommand, conditionColumns, ref syntheticParamOrdinal);
                break;

            default:
                throw new NotSupportedException($"Entity state '{command.EntityState}' is not supported in DecentDB modification batching.");
        }

        dbCommand.CommandText = sql.ToString();
        return readColumns;
    }

    private void AppendWhereClause(
        StringBuilder sql,
        DbCommand command,
        IReadOnlyList<IColumnModification> conditionColumns,
        ref int syntheticParamOrdinal)
    {
        if (conditionColumns.Count == 0)
        {
            return;
        }

        sql.Append(" WHERE ");
        for (var i = 0; i < conditionColumns.Count; i++)
        {
            if (i > 0) sql.Append(" AND ");
            var column = conditionColumns[i];
            var columnName = _dependencies.SqlGenerationHelper.DelimitIdentifier(column.ColumnName);
            var value = ConvertToProviderValue(column, column.UseOriginalValue ? column.OriginalValue : column.Value);
            if (value is null or DBNull)
            {
                sql.Append(columnName).Append(" IS NULL");
            }
            else
            {
                var placeholder = AddParameter(command, column.OriginalParameterName ?? column.ParameterName, value, ref syntheticParamOrdinal);
                sql.Append(columnName).Append(" = ").Append(placeholder);
            }
        }
    }

    private string AddCurrentValueParameter(DbCommand command, IColumnModification column, ref int syntheticParamOrdinal)
    {
        var name = column.ParameterName;
        var value = ConvertToProviderValue(column, column.Value);
        return AddParameter(command, name, value, ref syntheticParamOrdinal);
    }

    private static string AddParameter(DbCommand command, string? baseName, object? value, ref int syntheticParamOrdinal)
    {
        if (string.IsNullOrWhiteSpace(baseName))
        {
            baseName = $"p{++syntheticParamOrdinal}";
        }

        if (baseName[0] != '@' && baseName[0] != '$')
        {
            baseName = "@" + baseName;
        }

        var parameter = command.CreateParameter();
        parameter.ParameterName = baseName;
        parameter.Value = value ?? DBNull.Value;
        command.Parameters.Add(parameter);
        return baseName;
    }

    private static object? ConvertReadValue(IColumnModification column, object value)
    {
        if (value is DBNull)
        {
            return null;
        }

        var targetType = Nullable.GetUnderlyingType(column.Property?.ClrType ?? typeof(object)) ?? column.Property?.ClrType;
        if (targetType == typeof(int) && value is long l)
        {
            return checked((int)l);
        }

        if (targetType == typeof(short) && value is long l2)
        {
            return checked((short)l2);
        }

        if (targetType == typeof(byte) && value is long l3)
        {
            return checked((byte)l3);
        }

        if (targetType == typeof(bool) && value is long l4)
        {
            return l4 != 0;
        }

        return value;
    }

    private static object? ConvertToProviderValue(IColumnModification column, object? value)
    {
        if (value is null)
        {
            return null;
        }

        var converter = column.TypeMapping?.Converter;
        return converter is null
            ? value
            : converter.ConvertToProvider(value);
    }
}
