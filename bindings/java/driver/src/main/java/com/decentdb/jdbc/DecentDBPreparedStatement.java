package com.decentdb.jdbc;

import java.io.InputStream;
import java.io.Reader;
import java.math.BigDecimal;
import java.net.URL;
import java.sql.*;
import java.util.Calendar;

/**
 * DecentDB JDBC PreparedStatement.
 *
 * Parameters are 1-based (per JDBC spec) and map to {@code $1, $2, ...} positional
 * parameters in the underlying DecentDB SQL dialect.
 *
 * Not thread-safe. Do not share across threads.
 */
public class DecentDBPreparedStatement extends DecentDBStatement implements PreparedStatement {

    private final String sql;
    private Object[] params;
    private static final int MAX_PARAMS = 256;

    DecentDBPreparedStatement(DecentDBConnection connection, String sql) throws SQLException {
        super(connection);
        this.sql = sql;
        this.params = new Object[MAX_PARAMS];
    }

    private void checkParamIndex(int parameterIndex) throws SQLException {
        if (parameterIndex < 1 || parameterIndex > MAX_PARAMS) {
            throw new SQLException("Parameter index out of range: " + parameterIndex, "22023");
        }
    }

    @Override
    public ResultSet executeQuery() throws SQLException {
        checkOpen();
        connection.connectionLock.lock();
        try {
            closeCurrentResultSet();
            prepareNative();
            bindAll();
            int rc = DecentDBNative.stmtStep(stmtHandle);
            if (rc < 0) {
                Errors.checkResult(connection.getDbHandle(), rc);
            }
            currentResultSet = new DecentDBResultSet(this, stmtHandle, connection.getDbHandle(), rc);
            return currentResultSet;
        } finally {
            connection.connectionLock.unlock();
        }
    }

    @Override
    public int executeUpdate() throws SQLException {
        checkOpen();
        connection.connectionLock.lock();
        try {
            connection.beginTransactionIfNeeded();
            closeCurrentResultSet();
            prepareNative();
            bindAll();
            int rc = DecentDBNative.stmtStep(stmtHandle);
            if (rc < 0) {
                Errors.checkResult(connection.getDbHandle(), rc);
            }
            updateCount = DecentDBNative.stmtRowsAffected(stmtHandle);
            finalizeStmt();
            return (int) Math.min(updateCount, Integer.MAX_VALUE);
        } finally {
            connection.connectionLock.unlock();
        }
    }

    @Override
    public boolean execute() throws SQLException {
        checkOpen();
        connection.connectionLock.lock();
        try {
            if (!DecentDBStatement.isReadStatement(sql)) {
                connection.beginTransactionIfNeeded();
            }
            closeCurrentResultSet();
            prepareNative();
            bindAll();
            int rc = DecentDBNative.stmtStep(stmtHandle);
            if (rc < 0) {
                Errors.checkResult(connection.getDbHandle(), rc);
            }
            if (rc == 1) {
                currentResultSet = new DecentDBResultSet(this, stmtHandle, connection.getDbHandle(), rc);
                updateCount = -1;
                return true;
            } else {
                updateCount = DecentDBNative.stmtRowsAffected(stmtHandle);
                finalizeStmt();
                return false;
            }
        } finally {
            connection.connectionLock.unlock();
        }
    }

    private void prepareNative() throws SQLException {
        long[] outStmt = new long[1];
        int rc = DecentDBNative.stmtPrepare(connection.getDbHandle(), sql, outStmt);
        if (rc < 0 || outStmt[0] == 0) {
            Errors.checkResult(connection.getDbHandle(), rc < 0 ? rc : -1);
        }
        stmtHandle = outStmt[0];
    }

    private void bindAll() throws SQLException {
        for (int i = 0; i < MAX_PARAMS; i++) {
            if (params[i] == null) continue;
            int col = i + 1; // 1-based
            Object v = params[i];
            if (v == NullPlaceholder.INSTANCE) {
                int rc = DecentDBNative.bindNull(stmtHandle, col);
                if (rc < 0) Errors.checkResult(connection.getDbHandle(), rc);
            } else if (v instanceof Long) {
                int rc = DecentDBNative.bindInt64(stmtHandle, col, (Long) v);
                if (rc < 0) Errors.checkResult(connection.getDbHandle(), rc);
            } else if (v instanceof Integer) {
                int rc = DecentDBNative.bindInt64(stmtHandle, col, ((Integer) v).longValue());
                if (rc < 0) Errors.checkResult(connection.getDbHandle(), rc);
            } else if (v instanceof Short || v instanceof Byte) {
                int rc = DecentDBNative.bindInt64(stmtHandle, col, ((Number) v).longValue());
                if (rc < 0) Errors.checkResult(connection.getDbHandle(), rc);
            } else if (v instanceof Double) {
                int rc = DecentDBNative.bindFloat64(stmtHandle, col, (Double) v);
                if (rc < 0) Errors.checkResult(connection.getDbHandle(), rc);
            } else if (v instanceof Float) {
                int rc = DecentDBNative.bindFloat64(stmtHandle, col, ((Float) v).doubleValue());
                if (rc < 0) Errors.checkResult(connection.getDbHandle(), rc);
            } else if (v instanceof Boolean) {
                int rc = DecentDBNative.bindInt64(stmtHandle, col, (Boolean) v ? 1L : 0L);
                if (rc < 0) Errors.checkResult(connection.getDbHandle(), rc);
            } else if (v instanceof BigDecimal) {
                BigDecimal bd = (BigDecimal) v;
                int scale = bd.scale();
                long unscaled = bd.unscaledValue().longValue();
                int rc = DecentDBNative.bindInt64(stmtHandle, col, unscaled);
                if (rc < 0) Errors.checkResult(connection.getDbHandle(), rc);
            } else if (v instanceof String) {
                int rc = DecentDBNative.bindText(stmtHandle, col, (String) v);
                if (rc < 0) Errors.checkResult(connection.getDbHandle(), rc);
            } else if (v instanceof byte[]) {
                int rc = DecentDBNative.bindBlob(stmtHandle, col, (byte[]) v);
                if (rc < 0) Errors.checkResult(connection.getDbHandle(), rc);
            } else {
                // Fallback: convert to string
                int rc = DecentDBNative.bindText(stmtHandle, col, v.toString());
                if (rc < 0) Errors.checkResult(connection.getDbHandle(), rc);
            }
        }
    }

    // ---- Setters --------------------------------------------------------

    @Override
    public void setNull(int parameterIndex, int sqlType) throws SQLException {
        checkParamIndex(parameterIndex);
        params[parameterIndex - 1] = NullPlaceholder.INSTANCE;
    }

    @Override
    public void setBoolean(int parameterIndex, boolean x) throws SQLException {
        checkParamIndex(parameterIndex);
        params[parameterIndex - 1] = x;
    }

    @Override
    public void setByte(int parameterIndex, byte x) throws SQLException {
        checkParamIndex(parameterIndex);
        params[parameterIndex - 1] = (long) x;
    }

    @Override
    public void setShort(int parameterIndex, short x) throws SQLException {
        checkParamIndex(parameterIndex);
        params[parameterIndex - 1] = (long) x;
    }

    @Override
    public void setInt(int parameterIndex, int x) throws SQLException {
        checkParamIndex(parameterIndex);
        params[parameterIndex - 1] = (long) x;
    }

    @Override
    public void setLong(int parameterIndex, long x) throws SQLException {
        checkParamIndex(parameterIndex);
        params[parameterIndex - 1] = x;
    }

    @Override
    public void setFloat(int parameterIndex, float x) throws SQLException {
        checkParamIndex(parameterIndex);
        params[parameterIndex - 1] = (double) x;
    }

    @Override
    public void setDouble(int parameterIndex, double x) throws SQLException {
        checkParamIndex(parameterIndex);
        params[parameterIndex - 1] = x;
    }

    @Override
    public void setBigDecimal(int parameterIndex, BigDecimal x) throws SQLException {
        checkParamIndex(parameterIndex);
        params[parameterIndex - 1] = x != null ? x : NullPlaceholder.INSTANCE;
    }

    @Override
    public void setString(int parameterIndex, String x) throws SQLException {
        checkParamIndex(parameterIndex);
        params[parameterIndex - 1] = x != null ? x : NullPlaceholder.INSTANCE;
    }

    @Override
    public void setBytes(int parameterIndex, byte[] x) throws SQLException {
        checkParamIndex(parameterIndex);
        params[parameterIndex - 1] = x != null ? x : NullPlaceholder.INSTANCE;
    }

    @Override
    public void setDate(int parameterIndex, Date x) throws SQLException {
        checkParamIndex(parameterIndex);
        params[parameterIndex - 1] = x != null ? x.toString() : NullPlaceholder.INSTANCE;
    }

    @Override
    public void setTime(int parameterIndex, Time x) throws SQLException {
        checkParamIndex(parameterIndex);
        params[parameterIndex - 1] = x != null ? x.toString() : NullPlaceholder.INSTANCE;
    }

    @Override
    public void setTimestamp(int parameterIndex, Timestamp x) throws SQLException {
        checkParamIndex(parameterIndex);
        params[parameterIndex - 1] = x != null ? x.toString() : NullPlaceholder.INSTANCE;
    }

    @Override
    public void setObject(int parameterIndex, Object x) throws SQLException {
        checkParamIndex(parameterIndex);
        params[parameterIndex - 1] = x != null ? x : NullPlaceholder.INSTANCE;
    }

    @Override
    public void setObject(int parameterIndex, Object x, int targetSqlType) throws SQLException {
        setObject(parameterIndex, x);
    }

    @Override
    public void clearParameters() throws SQLException {
        params = new Object[MAX_PARAMS];
    }

    // ---- Unsupported setters (return notSupported) ----------------------

    @Override
    public void setAsciiStream(int parameterIndex, InputStream x, int length) throws SQLException {
        throw Errors.notSupported("setAsciiStream");
    }

    @Override
    public void setUnicodeStream(int parameterIndex, InputStream x, int length) throws SQLException {
        throw Errors.notSupported("setUnicodeStream");
    }

    @Override
    public void setBinaryStream(int parameterIndex, InputStream x, int length) throws SQLException {
        throw Errors.notSupported("setBinaryStream");
    }

    @Override
    public void setCharacterStream(int parameterIndex, Reader reader, int length) throws SQLException {
        throw Errors.notSupported("setCharacterStream");
    }

    @Override
    public void setRef(int parameterIndex, Ref x) throws SQLException { throw Errors.notSupported("setRef"); }

    @Override
    public void setBlob(int parameterIndex, Blob x) throws SQLException { throw Errors.notSupported("setBlob (Blob)"); }

    @Override
    public void setClob(int parameterIndex, Clob x) throws SQLException { throw Errors.notSupported("setClob"); }

    @Override
    public void setArray(int parameterIndex, Array x) throws SQLException { throw Errors.notSupported("setArray"); }

    @Override
    public ResultSetMetaData getMetaData() throws SQLException { return null; }

    @Override
    public void setDate(int parameterIndex, Date x, Calendar cal) throws SQLException { setDate(parameterIndex, x); }

    @Override
    public void setTime(int parameterIndex, Time x, Calendar cal) throws SQLException { setTime(parameterIndex, x); }

    @Override
    public void setTimestamp(int parameterIndex, Timestamp x, Calendar cal) throws SQLException {
        setTimestamp(parameterIndex, x);
    }

    @Override
    public void setNull(int parameterIndex, int sqlType, String typeName) throws SQLException {
        setNull(parameterIndex, sqlType);
    }

    @Override
    public void setURL(int parameterIndex, URL x) throws SQLException { throw Errors.notSupported("setURL"); }

    @Override
    public ParameterMetaData getParameterMetaData() throws SQLException { throw Errors.notSupported("ParameterMetaData"); }

    @Override
    public void setRowId(int parameterIndex, RowId x) throws SQLException { throw Errors.notSupported("setRowId"); }

    @Override
    public void setNString(int parameterIndex, String value) throws SQLException { setString(parameterIndex, value); }

    @Override
    public void setNCharacterStream(int parameterIndex, Reader value, long length) throws SQLException {
        throw Errors.notSupported("setNCharacterStream");
    }

    @Override
    public void setNClob(int parameterIndex, NClob value) throws SQLException { throw Errors.notSupported("setNClob"); }

    @Override
    public void setClob(int parameterIndex, Reader reader, long length) throws SQLException {
        throw Errors.notSupported("setClob(Reader)");
    }

    @Override
    public void setBlob(int parameterIndex, InputStream inputStream, long length) throws SQLException {
        throw Errors.notSupported("setBlob(InputStream)");
    }

    @Override
    public void setNClob(int parameterIndex, Reader reader, long length) throws SQLException {
        throw Errors.notSupported("setNClob(Reader)");
    }

    @Override
    public void setSQLXML(int parameterIndex, SQLXML xmlObject) throws SQLException {
        throw Errors.notSupported("setSQLXML");
    }

    @Override
    public void setObject(int parameterIndex, Object x, int targetSqlType, int scaleOrLength) throws SQLException {
        setObject(parameterIndex, x);
    }

    @Override
    public void setAsciiStream(int parameterIndex, InputStream x, long length) throws SQLException {
        throw Errors.notSupported("setAsciiStream");
    }

    @Override
    public void setBinaryStream(int parameterIndex, InputStream x, long length) throws SQLException {
        throw Errors.notSupported("setBinaryStream");
    }

    @Override
    public void setCharacterStream(int parameterIndex, Reader reader, long length) throws SQLException {
        throw Errors.notSupported("setCharacterStream");
    }

    @Override
    public void setAsciiStream(int parameterIndex, InputStream x) throws SQLException {
        throw Errors.notSupported("setAsciiStream");
    }

    @Override
    public void setBinaryStream(int parameterIndex, InputStream x) throws SQLException {
        throw Errors.notSupported("setBinaryStream");
    }

    @Override
    public void setCharacterStream(int parameterIndex, Reader reader) throws SQLException {
        throw Errors.notSupported("setCharacterStream");
    }

    @Override
    public void setNCharacterStream(int parameterIndex, Reader value) throws SQLException {
        throw Errors.notSupported("setNCharacterStream");
    }

    @Override
    public void setClob(int parameterIndex, Reader reader) throws SQLException {
        throw Errors.notSupported("setClob(Reader)");
    }

    @Override
    public void setBlob(int parameterIndex, InputStream inputStream) throws SQLException {
        throw Errors.notSupported("setBlob(InputStream)");
    }

    @Override
    public void setNClob(int parameterIndex, Reader reader) throws SQLException {
        throw Errors.notSupported("setNClob");
    }

    @Override
    public void addBatch() throws SQLException { throw Errors.notSupported("PreparedStatement.addBatch"); }

    /** Sentinel for explicit NULL binding. */
    private enum NullPlaceholder { INSTANCE }
}
