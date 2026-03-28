package com.decentdb.jdbc;

import java.sql.*;

/**
 * ResultSetMetaData for DecentDB query results.
 *
 * Column names and types are captured at ResultSet creation time from the
 * native statement handle. Types are reported as JDBC SQL type constants.
 */
public final class DecentDBResultSetMetaData implements ResultSetMetaData {

    private final String[] columnNames;
    private final int[] columnKinds;
    private final int[] columnScales;
    private final int columnCount;

    DecentDBResultSetMetaData(String[] columnNames, int[] columnKinds, int[] columnScales) {
        this.columnNames = columnNames;
        this.columnKinds = columnKinds;
        this.columnScales = columnScales;
        this.columnCount = columnNames.length;
    }

    private void checkIndex(int column) throws SQLException {
        if (column < 1 || column > columnCount) {
            throw new SQLException("Column index out of range: " + column, "S1002");
        }
    }

    @Override
    public int getColumnCount() { return columnCount; }

    @Override
    public String getColumnName(int column) throws SQLException {
        checkIndex(column);
        String n = columnNames[column - 1];
        return n != null ? n : "";
    }

    @Override
    public String getColumnLabel(int column) throws SQLException {
        return getColumnName(column);
    }

    @Override
    public int getColumnType(int column) throws SQLException {
        checkIndex(column);
        return TypeMapping.jdbcTypeFromKind(columnKinds[column - 1]);
    }

    @Override
    public String getColumnTypeName(int column) throws SQLException {
        return TypeMapping.typeName(getColumnType(column));
    }

    @Override
    public String getColumnClassName(int column) throws SQLException {
        int t = getColumnType(column);
        switch (t) {
            case Types.BIGINT:    return Long.class.getName();
            case Types.INTEGER:   return Integer.class.getName();
            case Types.DOUBLE:    return Double.class.getName();
            case Types.DECIMAL:   return java.math.BigDecimal.class.getName();
            case Types.VARCHAR:   return String.class.getName();
            case Types.BOOLEAN:   return Boolean.class.getName();
            case Types.BINARY:    return byte[].class.getName();
            case Types.DATE:      return java.sql.Date.class.getName();
            case Types.TIME:      return java.sql.Time.class.getName();
            case Types.TIMESTAMP: return java.sql.Timestamp.class.getName();
            default:              return Object.class.getName();
        }
    }

    @Override
    public boolean isAutoIncrement(int column) throws SQLException { return false; }

    @Override
    public boolean isCaseSensitive(int column) throws SQLException { return true; }

    @Override
    public boolean isSearchable(int column) throws SQLException { return true; }

    @Override
    public boolean isCurrency(int column) throws SQLException { return false; }

    @Override
    public int isNullable(int column) throws SQLException { return ResultSetMetaData.columnNullableUnknown; }

    @Override
    public boolean isSigned(int column) throws SQLException { return true; }

    @Override
    public int getColumnDisplaySize(int column) throws SQLException { return 255; }

    @Override
    public String getSchemaName(int column) throws SQLException { return ""; }

    @Override
    public int getPrecision(int column) throws SQLException {
        int t = getColumnType(column);
        if (t == Types.BIGINT || t == Types.INTEGER) return 19;
        if (t == Types.DECIMAL) return 38;
        if (t == Types.DOUBLE) return 15;
        return 0;
    }

    @Override
    public int getScale(int column) throws SQLException {
        checkIndex(column);
        return getColumnType(column) == Types.DECIMAL ? columnScales[column - 1] : 0;
    }

    @Override
    public String getTableName(int column) throws SQLException { return ""; }

    @Override
    public String getCatalogName(int column) throws SQLException { return ""; }

    @Override
    public boolean isReadOnly(int column) throws SQLException { return true; }

    @Override
    public boolean isWritable(int column) throws SQLException { return false; }

    @Override
    public boolean isDefinitelyWritable(int column) throws SQLException { return false; }

    @Override
    public <T> T unwrap(Class<T> iface) throws SQLException {
        if (iface.isAssignableFrom(getClass())) return iface.cast(this);
        throw new SQLException("Cannot unwrap to " + iface.getName());
    }

    @Override
    public boolean isWrapperFor(Class<?> iface) throws SQLException {
        return iface.isAssignableFrom(getClass());
    }
}
