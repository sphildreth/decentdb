package com.decentdb.jdbc;

import java.io.InputStream;
import java.io.Reader;
import java.math.BigDecimal;
import java.net.URL;
import java.sql.*;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.List;

/**
 * DecentDB JDBC PreparedStatement.
 *
 * Parameters are 1-based (per JDBC spec) and map to {@code $1, $2, ...} positional
 * parameters in the underlying DecentDB SQL dialect.
 *
 * Not thread-safe. Do not share across threads.
 */
@SuppressWarnings("deprecation")
public class DecentDBPreparedStatement extends DecentDBStatement implements PreparedStatement {

    private final String sql;
    private final boolean readQuery;
    private Object[] params;
    private static final int MAX_PARAMS = 256;
    private boolean nativePrepared = false;
    private final List<Object[]> batchParams = new ArrayList<>();

    DecentDBPreparedStatement(DecentDBConnection connection, String sql) throws SQLException {
        super(connection);
        this.sql = sql;
        this.readQuery = DecentDBStatement.isReadStatement(sql);
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
            int rc = executeReadOnce(params);
            if (rc != 0 && rc != 1) {
                Errors.checkStatus(connection.getDbHandle(), rc);
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
        connection.ensureWriteAllowed(readQuery, sql);
        connection.connectionLock.lock();
        try {
            connection.beginTransactionIfNeeded();
            closeCurrentResultSet();
            prepareNative();
            int rc = executeWriteOnce(params);
            if (rc != 0 && rc != 1) {
                Errors.checkStatus(connection.getDbHandle(), rc);
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
        connection.ensureWriteAllowed(readQuery, sql);
        connection.connectionLock.lock();
        try {
            if (!readQuery) {
                connection.beginTransactionIfNeeded();
            }
            closeCurrentResultSet();
            prepareNative();
            int rc = readQuery ? executeReadOnce(params) : executeWriteOnce(params);
            if (rc != 0 && rc != 1) {
                Errors.checkStatus(connection.getDbHandle(), rc);
            }
            if (readQuery) {
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
        if (nativePrepared && stmtHandle != 0) {
            int rc = DecentDBNative.stmtReset(stmtHandle);
            if (rc != 0) {
                Errors.checkStatus(connection.getDbHandle(), rc);
            }
            rc = DecentDBNative.stmtClearBindings(stmtHandle);
            if (rc != 0) {
                Errors.checkStatus(connection.getDbHandle(), rc);
            }
            return;
        }
        long[] outStmt = new long[1];
        int rc = DecentDBNative.stmtPrepare(connection.getDbHandle(), sql, outStmt);
        if (rc != 0 || outStmt[0] == 0) {
            Errors.checkStatus(connection.getDbHandle(), rc != 0 ? rc : DecentDBNative.ERR_INTERNAL);
        }
        stmtHandle = outStmt[0];
        nativePrepared = true;
    }

    private void bindAll(Object[] values) throws SQLException {
        for (int i = 0; i < MAX_PARAMS; i++) {
            if (values[i] == null) continue;
            int col = i + 1; // 1-based
            Object v = values[i];
            if (v == NullPlaceholder.INSTANCE) {
                int rc = DecentDBNative.bindNull(stmtHandle, col);
                if (rc != 0) Errors.checkStatus(connection.getDbHandle(), rc);
            } else if (v instanceof Long) {
                int rc = DecentDBNative.bindInt64(stmtHandle, col, (Long) v);
                if (rc != 0) Errors.checkStatus(connection.getDbHandle(), rc);
            } else if (v instanceof Integer) {
                int rc = DecentDBNative.bindInt64(stmtHandle, col, ((Integer) v).longValue());
                if (rc != 0) Errors.checkStatus(connection.getDbHandle(), rc);
            } else if (v instanceof Short || v instanceof Byte) {
                int rc = DecentDBNative.bindInt64(stmtHandle, col, ((Number) v).longValue());
                if (rc != 0) Errors.checkStatus(connection.getDbHandle(), rc);
            } else if (v instanceof Double) {
                int rc = DecentDBNative.bindFloat64(stmtHandle, col, (Double) v);
                if (rc != 0) Errors.checkStatus(connection.getDbHandle(), rc);
            } else if (v instanceof Float) {
                int rc = DecentDBNative.bindFloat64(stmtHandle, col, ((Float) v).doubleValue());
                if (rc != 0) Errors.checkStatus(connection.getDbHandle(), rc);
            } else if (v instanceof Boolean) {
                int rc = DecentDBNative.bindBool(stmtHandle, col, (Boolean) v);
                if (rc != 0) Errors.checkStatus(connection.getDbHandle(), rc);
            } else if (v instanceof BigDecimal) {
                BigDecimal bd = (BigDecimal) v;
                long unscaled = bd.unscaledValue().longValueExact();
                int rc = DecentDBNative.bindDecimal(stmtHandle, col, unscaled, bd.scale());
                if (rc != 0) Errors.checkStatus(connection.getDbHandle(), rc);
            } else if (v instanceof String) {
                int rc = DecentDBNative.bindText(stmtHandle, col, (String) v);
                if (rc != 0) Errors.checkStatus(connection.getDbHandle(), rc);
            } else if (v instanceof byte[]) {
                int rc = DecentDBNative.bindBlob(stmtHandle, col, (byte[]) v);
                if (rc != 0) Errors.checkStatus(connection.getDbHandle(), rc);
            } else if (v instanceof java.sql.Timestamp) {
                java.sql.Timestamp ts = (java.sql.Timestamp) v;
                long millis = ts.getTime();
                long micros = Math.floorDiv(millis, 1000L) * 1_000_000L + ts.getNanos() / 1000L;
                int rc = DecentDBNative.bindDatetime(stmtHandle, col, micros);
                if (rc != 0) Errors.checkStatus(connection.getDbHandle(), rc);
            } else if (v instanceof java.sql.Date) {
                java.sql.Date d = (java.sql.Date) v;
                long micros = d.getTime() * 1000L;
                int rc = DecentDBNative.bindDatetime(stmtHandle, col, micros);
                if (rc != 0) Errors.checkStatus(connection.getDbHandle(), rc);
            } else {
                // Fallback: convert to string
                int rc = DecentDBNative.bindText(stmtHandle, col, v.toString());
                if (rc != 0) Errors.checkStatus(connection.getDbHandle(), rc);
            }
        }
    }

    private int executeReadOnce(Object[] values) throws SQLException {
        if (singleLongParameter(values)) {
            int rc = DecentDBNative.stmtBindInt64StepRowView(stmtHandle, 1, ((Number) values[0]).longValue());
            if (rc != 0 && rc != 1) {
                Errors.checkStatus(connection.getDbHandle(), rc);
            }
            return rc;
        }
        bindAll(values);
        return DecentDBNative.stmtStepRowView(stmtHandle);
    }

    private int executeWriteOnce(Object[] values) throws SQLException {
        if (singleLongParameter(values)) {
            long[] outAffected = new long[1];
            int rc = DecentDBNative.stmtRebindInt64Execute(stmtHandle, ((Number) values[0]).longValue(), outAffected);
            if (rc == 0) {
                updateCount = outAffected[0];
                return 0;
            }
        } else if (textInt64Parameters(values)) {
            long[] outAffected = new long[1];
            int rc = DecentDBNative.stmtRebindTextInt64Execute(
                stmtHandle,
                (String) values[0],
                ((Number) values[1]).longValue(),
                outAffected
            );
            if (rc == 0) {
                updateCount = outAffected[0];
                return 0;
            }
        } else if (int64TextParameters(values)) {
            long[] outAffected = new long[1];
            int rc = DecentDBNative.stmtRebindInt64TextExecute(
                stmtHandle,
                ((Number) values[0]).longValue(),
                (String) values[1],
                outAffected
            );
            if (rc == 0) {
                updateCount = outAffected[0];
                return 0;
            }
        }
        bindAll(values);
        return DecentDBNative.stmtStep(stmtHandle);
    }

    private static boolean singleLongParameter(Object[] values) {
        return values[0] instanceof Number && highestBoundIndex(values) == 1;
    }

    private static boolean textInt64Parameters(Object[] values) {
        return values[0] instanceof String && values[1] instanceof Number && highestBoundIndex(values) == 2;
    }

    private static boolean int64TextParameters(Object[] values) {
        return values[0] instanceof Number && values[1] instanceof String && highestBoundIndex(values) == 2;
    }

    private static int highestBoundIndex(Object[] values) {
        for (int i = values.length - 1; i >= 0; i--) {
            if (values[i] != null) {
                return i + 1;
            }
        }
        return 0;
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
        params[parameterIndex - 1] = x != null ? x : NullPlaceholder.INSTANCE;
    }

    @Override
    public void setTime(int parameterIndex, Time x) throws SQLException {
        checkParamIndex(parameterIndex);
        params[parameterIndex - 1] = x != null ? x.toString() : NullPlaceholder.INSTANCE;
    }

    @Override
    public void setTimestamp(int parameterIndex, Timestamp x) throws SQLException {
        checkParamIndex(parameterIndex);
        params[parameterIndex - 1] = x != null ? x : NullPlaceholder.INSTANCE;
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

    @Override
    public void clearBatch() throws SQLException {
        batchParams.clear();
        super.clearBatch();
    }

    @Override
    public void addBatch() throws SQLException {
        checkOpen();
        batchParams.add(params.clone());
    }

    @Override
    public int[] executeBatch() throws SQLException {
        checkOpen();
        connection.ensureWriteAllowed(readQuery, sql);
        connection.connectionLock.lock();
        try {
            if (batchParams.isEmpty()) {
                return new int[0];
            }

            connection.beginTransactionIfNeeded();
            closeCurrentResultSet();
            prepareNative();

            int[] fastCounts = tryExecuteFastBatch();
            if (fastCounts != null) {
                batchParams.clear();
                return fastCounts;
            }

            int[] counts = new int[batchParams.size()];
            for (int i = 0; i < batchParams.size(); i++) {
                int rc = executeWriteOnce(batchParams.get(i));
                if (rc != 0 && rc != 1) {
                    Errors.checkStatus(connection.getDbHandle(), rc);
                }
                counts[i] = (int) Math.min(updateCount, Integer.MAX_VALUE);
                DecentDBNative.stmtReset(stmtHandle);
                DecentDBNative.stmtClearBindings(stmtHandle);
            }
            batchParams.clear();
            return counts;
        } finally {
            connection.connectionLock.unlock();
        }
    }

    @Override
    protected void finalizeStmt() {
        if (stmtHandle == 0) {
            return;
        }
        // PreparedStatement should reuse native prepared handles across execute calls.
        // ResultSet close + executeUpdate paths call finalizeStmt(); for prepared usage
        // this means "release row state + clear bindings", not native finalize.
        DecentDBNative.stmtReset(stmtHandle);
        DecentDBNative.stmtClearBindings(stmtHandle);
    }

    @Override
    public void close() throws SQLException {
        if (stmtHandle != 0) {
            DecentDBNative.stmtFinalize(stmtHandle);
            stmtHandle = 0;
        }
        nativePrepared = false;
        batchParams.clear();
        super.close();
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

    /** Sentinel for explicit NULL binding. */
    private enum NullPlaceholder { INSTANCE }

    private int[] tryExecuteFastBatch() throws SQLException {
        Object[][] rows = batchParams.toArray(Object[][]::new);
        long[] outAffected = new long[1];

        if (allSingleLong(rows)) {
            long[] values = new long[rows.length];
            for (int i = 0; i < rows.length; i++) {
                values[i] = ((Number) rows[i][0]).longValue();
            }
            int rc = DecentDBNative.stmtExecuteBatchI64(stmtHandle, values, outAffected);
            if (rc != 0) {
                Errors.checkStatus(connection.getDbHandle(), rc);
            }
            return successNoInfo(rows.length);
        }

        if (allI64TextF64(rows)) {
            long[] ints = new long[rows.length];
            String[] texts = new String[rows.length];
            double[] floats = new double[rows.length];
            for (int i = 0; i < rows.length; i++) {
                ints[i] = ((Number) rows[i][0]).longValue();
                texts[i] = (String) rows[i][1];
                floats[i] = ((Number) rows[i][2]).doubleValue();
            }
            int rc = DecentDBNative.stmtExecuteBatchI64TextF64(stmtHandle, ints, texts, floats, outAffected);
            if (rc != 0) {
                Errors.checkStatus(connection.getDbHandle(), rc);
            }
            return successNoInfo(rows.length);
        }

        BatchSignature signature = BatchSignature.build(rows);
        if (signature == null) {
            return null;
        }
        int rc = DecentDBNative.stmtExecuteBatchTyped(
            stmtHandle,
            signature.signature(),
            signature.i64Values(),
            signature.f64Values(),
            signature.textValues(),
            outAffected
        );
        if (rc != 0) {
            Errors.checkStatus(connection.getDbHandle(), rc);
        }
        return successNoInfo(rows.length);
    }

    private static int[] successNoInfo(int count) {
        int[] out = new int[count];
        java.util.Arrays.fill(out, Statement.SUCCESS_NO_INFO);
        return out;
    }

    private static boolean allSingleLong(Object[][] rows) {
        for (Object[] row : rows) {
            if (!singleLongParameter(row)) {
                return false;
            }
        }
        return true;
    }

    private static boolean allI64TextF64(Object[][] rows) {
        for (Object[] row : rows) {
            if (highestBoundIndex(row) != 3
                || !(row[0] instanceof Number)
                || !(row[1] instanceof String)
                || !(row[2] instanceof Number)) {
                return false;
            }
        }
        return true;
    }

    private record BatchSignature(String signature, long[] i64Values, double[] f64Values, String[] textValues) {
        static BatchSignature build(Object[][] rows) {
            int paramCount = highestBoundIndex(rows[0]);
            if (paramCount == 0) {
                return null;
            }

            StringBuilder signature = new StringBuilder(paramCount);
            for (int col = 0; col < paramCount; col++) {
                char kind = classify(rows[0][col]);
                if (kind == 0) {
                    return null;
                }
                signature.append(kind);
            }

            for (Object[] row : rows) {
                if (highestBoundIndex(row) != paramCount) {
                    return null;
                }
                for (int col = 0; col < paramCount; col++) {
                    if (classify(row[col]) != signature.charAt(col)) {
                        return null;
                    }
                }
            }

            int iCount = 0;
            int fCount = 0;
            int tCount = 0;
            for (int i = 0; i < signature.length(); i++) {
                switch (signature.charAt(i)) {
                    case 'i': iCount++; break;
                    case 'f': fCount++; break;
                    case 't': tCount++; break;
                    default: return null;
                }
            }

            long[] i64Values = iCount == 0 ? new long[0] : new long[rows.length * iCount];
            double[] f64Values = fCount == 0 ? new double[0] : new double[rows.length * fCount];
            String[] textValues = tCount == 0 ? new String[0] : new String[rows.length * tCount];

            for (int rowIndex = 0; rowIndex < rows.length; rowIndex++) {
                int iOffset = rowIndex * iCount;
                int fOffset = rowIndex * fCount;
                int tOffset = rowIndex * tCount;
                int iCursor = 0;
                int fCursor = 0;
                int tCursor = 0;
                for (int col = 0; col < paramCount; col++) {
                    Object value = rows[rowIndex][col];
                    switch (signature.charAt(col)) {
                        case 'i':
                            i64Values[iOffset + iCursor++] = ((Number) value).longValue();
                            break;
                        case 'f':
                            f64Values[fOffset + fCursor++] = ((Number) value).doubleValue();
                            break;
                        case 't':
                            textValues[tOffset + tCursor++] = (String) value;
                            break;
                        default:
                            return null;
                    }
                }
            }

            return new BatchSignature(signature.toString(), i64Values, f64Values, textValues);
        }

        private static char classify(Object value) {
            if (value instanceof Byte || value instanceof Short || value instanceof Integer || value instanceof Long) {
                return 'i';
            }
            if (value instanceof Float || value instanceof Double) {
                return 'f';
            }
            if (value instanceof String) {
                return 't';
            }
            return 0;
        }
    }
}
