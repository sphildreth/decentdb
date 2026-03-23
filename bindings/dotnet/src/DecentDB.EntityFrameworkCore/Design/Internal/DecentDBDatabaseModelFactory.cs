using DecentDB.AdoNet;
using Microsoft.EntityFrameworkCore.Scaffolding;
using Microsoft.EntityFrameworkCore.Scaffolding.Metadata;
using System.Data;
using System.Data.Common;

namespace DecentDB.EntityFrameworkCore.Design.Internal;

public sealed class DecentDBDatabaseModelFactory : DatabaseModelFactory
{
    public override DatabaseModel Create(string connectionString, DatabaseModelFactoryOptions options)
    {
        using var connection = new DecentDBConnection(connectionString);
        connection.Open();
        return Create(connection, options);
    }

    public override DatabaseModel Create(DbConnection connection, DatabaseModelFactoryOptions options)
    {
        if (connection.State != ConnectionState.Open)
        {
            connection.Open();
        }

        var includedTables = options.Tables.Any()
            ? new HashSet<string>(options.Tables, StringComparer.OrdinalIgnoreCase)
            : null;

        var model = new DatabaseModel();
        var tables = connection.GetSchema("Tables");
        var columns = connection.GetSchema("Columns");
        var indexes = connection.GetSchema("Indexes");

        foreach (DataRow tableRow in tables.Rows)
        {
            var tableName = tableRow["TABLE_NAME"]?.ToString();
            if (string.IsNullOrWhiteSpace(tableName))
            {
                continue;
            }

            if (includedTables is not null && !includedTables.Contains(tableName))
            {
                continue;
            }

            var table = new DatabaseTable
            {
                Database = model,
                Name = tableName
            };
            model.Tables.Add(table);

            var isPkByColumnName = new Dictionary<string, bool>(StringComparer.OrdinalIgnoreCase);
            foreach (DataRow columnRow in columns.Rows)
            {
                if (!string.Equals(columnRow["TABLE_NAME"]?.ToString(), tableName, StringComparison.OrdinalIgnoreCase))
                {
                    continue;
                }

                var columnName = columnRow["COLUMN_NAME"]?.ToString();
                if (string.IsNullOrWhiteSpace(columnName))
                {
                    continue;
                }

                var databaseColumn = new DatabaseColumn
                {
                    Table = table,
                    Name = columnName,
                    StoreType = (columnRow["DATA_TYPE"]?.ToString() ?? "TEXT").ToUpperInvariant(),
                    IsNullable = columnRow.Field<bool>("IS_NULLABLE")
                };
                table.Columns.Add(databaseColumn);
                isPkByColumnName[columnName] = columnRow.Field<bool>("IS_PRIMARY_KEY");
            }

            var pkColumns = table.Columns
                .Where(c => isPkByColumnName.TryGetValue(c.Name, out var isPk) && isPk)
                .ToList();
            if (pkColumns.Count > 0)
            {
                var primaryKey = new DatabasePrimaryKey
                {
                    Table = table,
                    Name = $"PK_{table.Name}"
                };
                foreach (var pkColumn in pkColumns)
                {
                    primaryKey.Columns.Add(pkColumn);
                }

                table.PrimaryKey = primaryKey;
            }

            foreach (DataRow indexRow in indexes.Rows)
            {
                if (!string.Equals(indexRow["TABLE_NAME"]?.ToString(), tableName, StringComparison.OrdinalIgnoreCase))
                {
                    continue;
                }

                var indexName = indexRow["INDEX_NAME"]?.ToString();
                if (string.IsNullOrWhiteSpace(indexName))
                {
                    continue;
                }

                var columnNames = (indexRow["COLUMNS"]?.ToString() ?? string.Empty)
                    .Split(',', StringSplitOptions.TrimEntries | StringSplitOptions.RemoveEmptyEntries);
                var indexColumns = table.Columns
                    .Where(column => columnNames.Contains(column.Name, StringComparer.OrdinalIgnoreCase))
                    .ToList();
                if (indexColumns.Count == 0)
                {
                    continue;
                }

                var index = new DatabaseIndex
                {
                    Table = table,
                    Name = indexName,
                    IsUnique = indexRow.Field<bool>("IS_UNIQUE")
                };
                foreach (var indexColumn in indexColumns)
                {
                    index.Columns.Add(indexColumn);
                }

                table.Indexes.Add(index);
            }
        }

        return model;
    }
}
