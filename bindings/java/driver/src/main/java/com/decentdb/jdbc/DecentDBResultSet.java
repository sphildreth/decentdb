package com.decentdb.jdbc;

import java.io.InputStream;
import java.io.Reader;
import java.math.BigDecimal;
import java.math.RoundingMode;
import java.net.URL;
import java.sql.*;
import java.util.*;

/**
 * DecentDB JDBC ResultSet (forward-only, read-only).
 *
 * The ResultSet takes ownership of the statement handle {@code stmtHandle}.
 * After iteration is complete or the ResultSet is closed, the statement is finalized.
 *
 * The first step has already been executed by the owning Statement before this
 * ResultSet is created. {@link #next()} advances to the next row.
 */
@SuppressWarnings("deprecation")
public class DecentDBResultSet implements ResultSet {

    private final DecentDBStatement owningStatement;
    private long stmtHandle;
    private final long dbHandle;
    private volatile boolean closed = false;
    private boolean wasNull = false;

    // 1 = has first row (from parent step), 0 = exhausted
    protected int lastStepResult;
    protected boolean beforeFirst = true;

    /** Column name → 1-based index cache. */
    private Map<String, Integer> colNameIndex;
    private int colCount = -1;

    DecentDBResultSet(DecentDBStatement owningStatement, long stmtHandle, long dbHandle, int initialStepResult) {
        this.owningStatement = owningStatement;
        this.stmtHandle = stmtHandle;
        this.dbHandle = dbHandle;
        // 1 = parent step found a row; 0 = empty result
        this.lastStepResult = initialStepResult;
    }

    /** Creates an empty ResultSet with no rows. */
    static DecentDBResultSet empty() {
        return new EmptyResultSet();
    }

    private static final class EmptyResultSet extends DecentDBResultSet {
        EmptyResultSet() {
            super(null, 0, 0, 0);
            this.beforeFirst = false;
        }

        @Override
        public boolean next() { return false; }
    }

    private void checkOpen() throws SQLException {
        if (closed) throw new SQLException("ResultSet is closed", "24000");
    }

    @Override
    public boolean next() throws SQLException {
        if (closed) return false;
        if (stmtHandle == 0) return false;

        if (beforeFirst) {
            // First call to next(): check if the parent's step found a row
            beforeFirst = false;
            if (lastStepResult != 1) {
                return false;
            }
            return true;
        }

        // Advance to next row
        int rc = DecentDBNative.stmtStep(stmtHandle);
        if (rc < 0) {
            Errors.checkResult(dbHandle, rc);
        }
        lastStepResult = rc;
        if (rc != 1) {
            // exhausted
            return false;
        }
        return true;
    }

    @Override
    public void close() throws SQLException {
        if (closed) return;
        closed = true;
        if (stmtHandle != 0 && owningStatement != null) {
            owningStatement.finalizeStmt();
            stmtHandle = 0;
        }
    }

    @Override
    public boolean isClosed() { return closed; }

    @Override
    public boolean wasNull() throws SQLException { return wasNull; }

    // ---- Column access by index (1-based) ------------------------------

    private int kind(int columnIndex) throws SQLException {
        checkOpen();
        return DecentDBNative.colType(stmtHandle, columnIndex - 1);
    }

    private boolean isNull(int columnIndex) {
        int n = DecentDBNative.colIsNull(stmtHandle, columnIndex - 1);
        wasNull = (n != 0);
        return wasNull;
    }

    @Override
    public String getString(int columnIndex) throws SQLException {
        checkOpen();
        if (isNull(columnIndex)) return null;
        int k = kind(columnIndex);
        if (k == DecentDBNative.KIND_TEXT) {
            return DecentDBNative.colText(stmtHandle, columnIndex - 1);
        } else if (k == DecentDBNative.KIND_BLOB) {
            byte[] bytes = DecentDBNative.colBlob(stmtHandle, columnIndex - 1);
            if (bytes == null) return null;
            if (isUuidBytes(bytes)) return uuidToString(bytes);
            return bytesToHex(bytes);
        } else if (k == DecentDBNative.KIND_INT64
                || k == DecentDBNative.KIND_INT0 || k == DecentDBNative.KIND_INT1) {
            return Long.toString(DecentDBNative.colInt64(stmtHandle, columnIndex - 1));
        } else if (k == DecentDBNative.KIND_FLOAT64) {
            return Double.toString(DecentDBNative.colFloat64(stmtHandle, columnIndex - 1));
        } else if (k == DecentDBNative.KIND_BOOL
                || k == DecentDBNative.KIND_BOOL_FALSE || k == DecentDBNative.KIND_BOOL_TRUE) {
            return DecentDBNative.colInt64(stmtHandle, columnIndex - 1) != 0 ? "true" : "false";
        } else if (k == DecentDBNative.KIND_DECIMAL) {
            return getDecimal(columnIndex).toPlainString();
        } else if (k == DecentDBNative.KIND_DATETIME) {
            // Format as ISO-8601 string
            long micros = DecentDBNative.colDatetime(stmtHandle, columnIndex - 1);
            long epochMicros = micros;
            long secs = epochMicros / 1_000_000L;
            int us = (int)(epochMicros % 1_000_000L);
            if (us < 0) { secs--; us += 1_000_000; }
            java.time.Instant inst = java.time.Instant.ofEpochSecond(secs, us * 1000L);
            return inst.atOffset(java.time.ZoneOffset.UTC).format(java.time.format.DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss"));
        }
        return DecentDBNative.colText(stmtHandle, columnIndex - 1);
    }

    private static boolean isUuidBytes(byte[] bytes) {
        return bytes.length == 16;
    }

    private static UUID uuidFromBytes(byte[] bytes) {
        long msb = 0;
        long lsb = 0;
        for (int i = 0; i < 8; i++) {
            msb = (msb << 8) | (bytes[i] & 0xffL);
        }
        for (int i = 8; i < 16; i++) {
            lsb = (lsb << 8) | (bytes[i] & 0xffL);
        }
        return new UUID(msb, lsb);
    }

    private static String uuidToString(byte[] bytes) {
        final char[] hex = "0123456789abcdef".toCharArray();
        char[] out = new char[36];
        int o = 0;
        for (int i = 0; i < 16; i++) {
            if (i == 4 || i == 6 || i == 8 || i == 10) {
                out[o++] = '-';
            }
            int b = bytes[i] & 0xff;
            out[o++] = hex[b >>> 4];
            out[o++] = hex[b & 0x0f];
        }
        return new String(out);
    }

    private static String bytesToHex(byte[] bytes) {
        final char[] hex = "0123456789abcdef".toCharArray();
        char[] out = new char[2 + bytes.length * 2];
        out[0] = '0';
        out[1] = 'x';
        int o = 2;
        for (byte aByte : bytes) {
            int b = aByte & 0xff;
            out[o++] = hex[b >>> 4];
            out[o++] = hex[b & 0x0f];
        }
        return new String(out);
    }

    @Override
    public boolean getBoolean(int columnIndex) throws SQLException {
        checkOpen();
        if (isNull(columnIndex)) return false;
        int k = kind(columnIndex);
        if (k == DecentDBNative.KIND_BOOL || k == DecentDBNative.KIND_INT64
                || k == DecentDBNative.KIND_INT0 || k == DecentDBNative.KIND_INT1) {
            return DecentDBNative.colInt64(stmtHandle, columnIndex - 1) != 0;
        }
        if (k == DecentDBNative.KIND_BOOL_TRUE) return true;
        if (k == DecentDBNative.KIND_BOOL_FALSE) return false;
        String s = getString(columnIndex);
        return s != null && ("true".equalsIgnoreCase(s) || "1".equals(s));
    }

    @Override
    public byte getByte(int columnIndex) throws SQLException {
        return (byte) getLong(columnIndex);
    }

    @Override
    public short getShort(int columnIndex) throws SQLException {
        return (short) getLong(columnIndex);
    }

    @Override
    public int getInt(int columnIndex) throws SQLException {
        return (int) getLong(columnIndex);
    }

    @Override
    public long getLong(int columnIndex) throws SQLException {
        checkOpen();
        if (isNull(columnIndex)) return 0L;
        int k = kind(columnIndex);
        if (k == DecentDBNative.KIND_INT64 || k == DecentDBNative.KIND_BOOL
                || k == DecentDBNative.KIND_INT0 || k == DecentDBNative.KIND_INT1
                || k == DecentDBNative.KIND_BOOL_FALSE || k == DecentDBNative.KIND_BOOL_TRUE) {
            return DecentDBNative.colInt64(stmtHandle, columnIndex - 1);
        } else if (k == DecentDBNative.KIND_FLOAT64) {
            return (long) DecentDBNative.colFloat64(stmtHandle, columnIndex - 1);
        } else if (k == DecentDBNative.KIND_DECIMAL) {
            return getDecimal(columnIndex).longValue();
        }
        String s = DecentDBNative.colText(stmtHandle, columnIndex - 1);
        if (s == null || s.isEmpty()) return 0L;
        try { return Long.parseLong(s.trim()); } catch (NumberFormatException e) { return 0L; }
    }

    @Override
    public float getFloat(int columnIndex) throws SQLException {
        return (float) getDouble(columnIndex);
    }

    @Override
    public double getDouble(int columnIndex) throws SQLException {
        checkOpen();
        if (isNull(columnIndex)) return 0.0;
        int k = kind(columnIndex);
        if (k == DecentDBNative.KIND_FLOAT64) {
            return DecentDBNative.colFloat64(stmtHandle, columnIndex - 1);
        } else if (k == DecentDBNative.KIND_INT64 || k == DecentDBNative.KIND_BOOL
                || k == DecentDBNative.KIND_INT0 || k == DecentDBNative.KIND_INT1
                || k == DecentDBNative.KIND_BOOL_FALSE || k == DecentDBNative.KIND_BOOL_TRUE) {
            return DecentDBNative.colInt64(stmtHandle, columnIndex - 1);
        } else if (k == DecentDBNative.KIND_DECIMAL) {
            return getDecimal(columnIndex).doubleValue();
        }
        String s = DecentDBNative.colText(stmtHandle, columnIndex - 1);
        if (s == null || s.isEmpty()) return 0.0;
        try { return Double.parseDouble(s.trim()); } catch (NumberFormatException e) { return 0.0; }
    }

    @Override
    public BigDecimal getBigDecimal(int columnIndex, int scale) throws SQLException {
        BigDecimal d = getBigDecimal(columnIndex);
        return d == null ? null : d.setScale(scale, RoundingMode.HALF_UP);
    }

    @Override
    public BigDecimal getBigDecimal(int columnIndex) throws SQLException {
        checkOpen();
        if (isNull(columnIndex)) return null;
        return getDecimal(columnIndex);
    }

    private BigDecimal getDecimal(int columnIndex) {
        int k = DecentDBNative.colType(stmtHandle, columnIndex - 1);
        if (k == DecentDBNative.KIND_DECIMAL) {
            int scale = DecentDBNative.colDecimalScale(stmtHandle, columnIndex - 1);
            long unscaled = DecentDBNative.colDecimalUnscaled(stmtHandle, columnIndex - 1);
            return BigDecimal.valueOf(unscaled, scale);
        } else if (k == DecentDBNative.KIND_INT64) {
            return BigDecimal.valueOf(DecentDBNative.colInt64(stmtHandle, columnIndex - 1));
        } else if (k == DecentDBNative.KIND_FLOAT64) {
            return BigDecimal.valueOf(DecentDBNative.colFloat64(stmtHandle, columnIndex - 1));
        }
        String s = DecentDBNative.colText(stmtHandle, columnIndex - 1);
        if (s == null || s.isEmpty()) return BigDecimal.ZERO;
        try { return new BigDecimal(s.trim()); } catch (NumberFormatException e) { return BigDecimal.ZERO; }
    }

    @Override
    public byte[] getBytes(int columnIndex) throws SQLException {
        checkOpen();
        if (isNull(columnIndex)) return null;
        return DecentDBNative.colBlob(stmtHandle, columnIndex - 1);
    }

    @Override
    public java.sql.Date getDate(int columnIndex) throws SQLException {
        String s = getString(columnIndex);
        if (s == null) return null;
        try { return java.sql.Date.valueOf(s.trim()); } catch (IllegalArgumentException e) { return null; }
    }

    @Override
    public Time getTime(int columnIndex) throws SQLException {
        String s = getString(columnIndex);
        if (s == null) return null;
        try { return Time.valueOf(s.trim()); } catch (IllegalArgumentException e) { return null; }
    }

    @Override
    public Timestamp getTimestamp(int columnIndex) throws SQLException {
        checkOpen();
        if (isNull(columnIndex)) return null;
        int k = kind(columnIndex);
        if (k == DecentDBNative.KIND_DATETIME) {
            long micros = DecentDBNative.colDatetime(stmtHandle, columnIndex - 1);
            long ms = micros / 1000L;
            int ns = (int)((micros % 1000L) * 1000L);
            Timestamp ts = new Timestamp(ms);
            ts.setNanos(ns >= 0 ? ns : ns + 1_000_000_000);
            return ts;
        }
        String s = getString(columnIndex);
        if (s == null) return null;
        try { return Timestamp.valueOf(s.trim()); } catch (IllegalArgumentException e) { return null; }
    }

    @Override
    public Object getObject(int columnIndex) throws SQLException {
        checkOpen();
        if (isNull(columnIndex)) return null;
        int k = kind(columnIndex);
        switch (k) {
            case DecentDBNative.KIND_INT64: return getLong(columnIndex);
            case DecentDBNative.KIND_FLOAT64: return getDouble(columnIndex);
            case DecentDBNative.KIND_BOOL: return getBoolean(columnIndex);
            case DecentDBNative.KIND_BLOB: {
                byte[] bytes = getBytes(columnIndex);
                if (bytes != null && isUuidBytes(bytes)) return uuidFromBytes(bytes);
                return bytes;
            }
            case DecentDBNative.KIND_DECIMAL: return getBigDecimal(columnIndex);
            case DecentDBNative.KIND_DATETIME: return getTimestamp(columnIndex);
            default: return getString(columnIndex);
        }
    }

    // ---- Column access by label (name) ---------------------------------

    @Override
    public String getString(String columnLabel) throws SQLException {
        return getString(findColumn(columnLabel));
    }

    @Override
    public boolean getBoolean(String columnLabel) throws SQLException {
        return getBoolean(findColumn(columnLabel));
    }

    @Override
    public byte getByte(String columnLabel) throws SQLException {
        return getByte(findColumn(columnLabel));
    }

    @Override
    public short getShort(String columnLabel) throws SQLException {
        return getShort(findColumn(columnLabel));
    }

    @Override
    public int getInt(String columnLabel) throws SQLException {
        return getInt(findColumn(columnLabel));
    }

    @Override
    public long getLong(String columnLabel) throws SQLException {
        return getLong(findColumn(columnLabel));
    }

    @Override
    public float getFloat(String columnLabel) throws SQLException {
        return getFloat(findColumn(columnLabel));
    }

    @Override
    public double getDouble(String columnLabel) throws SQLException {
        return getDouble(findColumn(columnLabel));
    }

    @Override
    public BigDecimal getBigDecimal(String columnLabel, int scale) throws SQLException {
        return getBigDecimal(findColumn(columnLabel), scale);
    }

    @Override
    public byte[] getBytes(String columnLabel) throws SQLException {
        return getBytes(findColumn(columnLabel));
    }

    @Override
    public java.sql.Date getDate(String columnLabel) throws SQLException {
        return getDate(findColumn(columnLabel));
    }

    @Override
    public Time getTime(String columnLabel) throws SQLException {
        return getTime(findColumn(columnLabel));
    }

    @Override
    public Timestamp getTimestamp(String columnLabel) throws SQLException {
        return getTimestamp(findColumn(columnLabel));
    }

    @Override
    public Object getObject(String columnLabel) throws SQLException {
        return getObject(findColumn(columnLabel));
    }

    @Override
    public BigDecimal getBigDecimal(String columnLabel) throws SQLException {
        return getBigDecimal(findColumn(columnLabel));
    }

    @Override
    public int findColumn(String columnLabel) throws SQLException {
        checkOpen();
        if (colNameIndex == null) buildColNameIndex();
        Integer idx = colNameIndex.get(columnLabel);
        if (idx == null) idx = colNameIndex.get(columnLabel.toLowerCase());
        if (idx == null) throw new SQLException("Column not found: " + columnLabel, "42S22");
        return idx;
    }

    private void buildColNameIndex() {
        int n = DecentDBNative.colCount(stmtHandle);
        colCount = n;
        colNameIndex = new LinkedHashMap<>(n * 2);
        for (int i = 0; i < n; i++) {
            String name = DecentDBNative.colName(stmtHandle, i);
            if (name != null) {
                colNameIndex.put(name, i + 1);
                colNameIndex.put(name.toLowerCase(), i + 1);
            }
        }
    }

    @Override
    public ResultSetMetaData getMetaData() throws SQLException {
        checkOpen();
        int n = DecentDBNative.colCount(stmtHandle);
        String[] names = new String[n];
        int[] types = new int[n];
        for (int i = 0; i < n; i++) {
            names[i] = DecentDBNative.colName(stmtHandle, i);
            types[i] = DecentDBNative.colType(stmtHandle, i);
        }
        return new DecentDBResultSetMetaData(names, types);
    }

    // ---- Navigation (forward-only) -------------------------------------

    @Override
    public boolean isBeforeFirst() throws SQLException { return beforeFirst; }

    @Override
    public boolean isAfterLast() throws SQLException { return !beforeFirst && lastStepResult != 1; }

    @Override
    public boolean isFirst() throws SQLException { return false; }

    @Override
    public boolean isLast() throws SQLException { return false; }

    @Override
    public void beforeFirst() throws SQLException { throw Errors.notSupported("beforeFirst (forward-only)"); }

    @Override
    public void afterLast() throws SQLException { throw Errors.notSupported("afterLast (forward-only)"); }

    @Override
    public boolean first() throws SQLException { throw Errors.notSupported("first (forward-only)"); }

    @Override
    public boolean last() throws SQLException { throw Errors.notSupported("last (forward-only)"); }

    @Override
    public int getRow() throws SQLException { return 0; }

    @Override
    public boolean absolute(int row) throws SQLException { throw Errors.notSupported("absolute (forward-only)"); }

    @Override
    public boolean relative(int rows) throws SQLException { throw Errors.notSupported("relative (forward-only)"); }

    @Override
    public boolean previous() throws SQLException { throw Errors.notSupported("previous (forward-only)"); }

    @Override
    public void setFetchDirection(int direction) throws SQLException {}

    @Override
    public int getFetchDirection() throws SQLException { return ResultSet.FETCH_FORWARD; }

    @Override
    public void setFetchSize(int rows) throws SQLException {}

    @Override
    public int getFetchSize() throws SQLException { return 0; }

    @Override
    public int getType() throws SQLException { return ResultSet.TYPE_FORWARD_ONLY; }

    @Override
    public int getConcurrency() throws SQLException { return ResultSet.CONCUR_READ_ONLY; }

    @Override
    public int getHoldability() throws SQLException { return ResultSet.CLOSE_CURSORS_AT_COMMIT; }

    // ---- Unsupported update methods ------------------------------------

    @Override
    public boolean rowUpdated() throws SQLException { return false; }

    @Override
    public boolean rowInserted() throws SQLException { return false; }

    @Override
    public boolean rowDeleted() throws SQLException { return false; }

    @Override
    public void updateNull(int columnIndex) throws SQLException { throw Errors.notSupported("updateNull"); }

    @Override
    public void updateBoolean(int columnIndex, boolean x) throws SQLException { throw Errors.notSupported("updateBoolean"); }

    @Override
    public void updateByte(int columnIndex, byte x) throws SQLException { throw Errors.notSupported("updateByte"); }

    @Override
    public void updateShort(int columnIndex, short x) throws SQLException { throw Errors.notSupported("updateShort"); }

    @Override
    public void updateInt(int columnIndex, int x) throws SQLException { throw Errors.notSupported("updateInt"); }

    @Override
    public void updateLong(int columnIndex, long x) throws SQLException { throw Errors.notSupported("updateLong"); }

    @Override
    public void updateFloat(int columnIndex, float x) throws SQLException { throw Errors.notSupported("updateFloat"); }

    @Override
    public void updateDouble(int columnIndex, double x) throws SQLException { throw Errors.notSupported("updateDouble"); }

    @Override
    public void updateBigDecimal(int columnIndex, BigDecimal x) throws SQLException { throw Errors.notSupported("updateBigDecimal"); }

    @Override
    public void updateString(int columnIndex, String x) throws SQLException { throw Errors.notSupported("updateString"); }

    @Override
    public void updateBytes(int columnIndex, byte[] x) throws SQLException { throw Errors.notSupported("updateBytes"); }

    @Override
    public void updateDate(int columnIndex, java.sql.Date x) throws SQLException { throw Errors.notSupported("updateDate"); }

    @Override
    public void updateTime(int columnIndex, Time x) throws SQLException { throw Errors.notSupported("updateTime"); }

    @Override
    public void updateTimestamp(int columnIndex, Timestamp x) throws SQLException { throw Errors.notSupported("updateTimestamp"); }

    @Override
    public void updateAsciiStream(int columnIndex, InputStream x, int length) throws SQLException { throw Errors.notSupported("updateAsciiStream"); }

    @Override
    public void updateBinaryStream(int columnIndex, InputStream x, int length) throws SQLException { throw Errors.notSupported("updateBinaryStream"); }

    @Override
    public void updateCharacterStream(int columnIndex, Reader x, int length) throws SQLException { throw Errors.notSupported("updateCharacterStream"); }

    @Override
    public void updateObject(int columnIndex, Object x, int scaleOrLength) throws SQLException { throw Errors.notSupported("updateObject"); }

    @Override
    public void updateObject(int columnIndex, Object x) throws SQLException { throw Errors.notSupported("updateObject"); }

    @Override
    public void updateNull(String columnLabel) throws SQLException { throw Errors.notSupported("updateNull"); }

    @Override
    public void updateBoolean(String columnLabel, boolean x) throws SQLException { throw Errors.notSupported("updateBoolean"); }

    @Override
    public void updateByte(String columnLabel, byte x) throws SQLException { throw Errors.notSupported("updateByte"); }

    @Override
    public void updateShort(String columnLabel, short x) throws SQLException { throw Errors.notSupported("updateShort"); }

    @Override
    public void updateInt(String columnLabel, int x) throws SQLException { throw Errors.notSupported("updateInt"); }

    @Override
    public void updateLong(String columnLabel, long x) throws SQLException { throw Errors.notSupported("updateLong"); }

    @Override
    public void updateFloat(String columnLabel, float x) throws SQLException { throw Errors.notSupported("updateFloat"); }

    @Override
    public void updateDouble(String columnLabel, double x) throws SQLException { throw Errors.notSupported("updateDouble"); }

    @Override
    public void updateBigDecimal(String columnLabel, BigDecimal x) throws SQLException { throw Errors.notSupported("updateBigDecimal"); }

    @Override
    public void updateString(String columnLabel, String x) throws SQLException { throw Errors.notSupported("updateString"); }

    @Override
    public void updateBytes(String columnLabel, byte[] x) throws SQLException { throw Errors.notSupported("updateBytes"); }

    @Override
    public void updateDate(String columnLabel, java.sql.Date x) throws SQLException { throw Errors.notSupported("updateDate"); }

    @Override
    public void updateTime(String columnLabel, Time x) throws SQLException { throw Errors.notSupported("updateTime"); }

    @Override
    public void updateTimestamp(String columnLabel, Timestamp x) throws SQLException { throw Errors.notSupported("updateTimestamp"); }

    @Override
    public void updateAsciiStream(String columnLabel, InputStream x, int length) throws SQLException { throw Errors.notSupported("updateAsciiStream"); }

    @Override
    public void updateBinaryStream(String columnLabel, InputStream x, int length) throws SQLException { throw Errors.notSupported("updateBinaryStream"); }

    @Override
    public void updateCharacterStream(String columnLabel, Reader reader, int length) throws SQLException { throw Errors.notSupported("updateCharacterStream"); }

    @Override
    public void updateObject(String columnLabel, Object x, int scaleOrLength) throws SQLException { throw Errors.notSupported("updateObject"); }

    @Override
    public void updateObject(String columnLabel, Object x) throws SQLException { throw Errors.notSupported("updateObject"); }

    @Override
    public void insertRow() throws SQLException { throw Errors.notSupported("insertRow"); }

    @Override
    public void updateRow() throws SQLException { throw Errors.notSupported("updateRow"); }

    @Override
    public void deleteRow() throws SQLException { throw Errors.notSupported("deleteRow"); }

    @Override
    public void refreshRow() throws SQLException { throw Errors.notSupported("refreshRow"); }

    @Override
    public void cancelRowUpdates() throws SQLException { throw Errors.notSupported("cancelRowUpdates"); }

    @Override
    public void moveToInsertRow() throws SQLException { throw Errors.notSupported("moveToInsertRow"); }

    @Override
    public void moveToCurrentRow() throws SQLException { throw Errors.notSupported("moveToCurrentRow"); }

    @Override
    public Statement getStatement() throws SQLException { return owningStatement; }

    @Override
    public Object getObject(int columnIndex, Map<String, Class<?>> map) throws SQLException {
        return getObject(columnIndex);
    }

    @Override
    public Ref getRef(int columnIndex) throws SQLException { throw Errors.notSupported("getRef"); }

    @Override
    public Blob getBlob(int columnIndex) throws SQLException { throw Errors.notSupported("getBlob (Blob)"); }

    @Override
    public Clob getClob(int columnIndex) throws SQLException { throw Errors.notSupported("getClob"); }

    @Override
    public Array getArray(int columnIndex) throws SQLException { throw Errors.notSupported("getArray"); }

    @Override
    public Object getObject(String columnLabel, Map<String, Class<?>> map) throws SQLException {
        return getObject(columnLabel);
    }

    @Override
    public Ref getRef(String columnLabel) throws SQLException { throw Errors.notSupported("getRef"); }

    @Override
    public Blob getBlob(String columnLabel) throws SQLException { throw Errors.notSupported("getBlob (Blob)"); }

    @Override
    public Clob getClob(String columnLabel) throws SQLException { throw Errors.notSupported("getClob"); }

    @Override
    public Array getArray(String columnLabel) throws SQLException { throw Errors.notSupported("getArray"); }

    @Override
    public java.sql.Date getDate(int columnIndex, Calendar cal) throws SQLException { return getDate(columnIndex); }

    @Override
    public java.sql.Date getDate(String columnLabel, Calendar cal) throws SQLException { return getDate(columnLabel); }

    @Override
    public Time getTime(int columnIndex, Calendar cal) throws SQLException { return getTime(columnIndex); }

    @Override
    public Time getTime(String columnLabel, Calendar cal) throws SQLException { return getTime(columnLabel); }

    @Override
    public Timestamp getTimestamp(int columnIndex, Calendar cal) throws SQLException { return getTimestamp(columnIndex); }

    @Override
    public Timestamp getTimestamp(String columnLabel, Calendar cal) throws SQLException { return getTimestamp(columnLabel); }

    @Override
    public URL getURL(int columnIndex) throws SQLException { throw Errors.notSupported("getURL"); }

    @Override
    public URL getURL(String columnLabel) throws SQLException { throw Errors.notSupported("getURL"); }

    @Override
    public void updateRef(int columnIndex, Ref x) throws SQLException { throw Errors.notSupported("updateRef"); }

    @Override
    public void updateRef(String columnLabel, Ref x) throws SQLException { throw Errors.notSupported("updateRef"); }

    @Override
    public void updateBlob(int columnIndex, Blob x) throws SQLException { throw Errors.notSupported("updateBlob"); }

    @Override
    public void updateBlob(String columnLabel, Blob x) throws SQLException { throw Errors.notSupported("updateBlob"); }

    @Override
    public void updateClob(int columnIndex, Clob x) throws SQLException { throw Errors.notSupported("updateClob"); }

    @Override
    public void updateClob(String columnLabel, Clob x) throws SQLException { throw Errors.notSupported("updateClob"); }

    @Override
    public void updateArray(int columnIndex, Array x) throws SQLException { throw Errors.notSupported("updateArray"); }

    @Override
    public void updateArray(String columnLabel, Array x) throws SQLException { throw Errors.notSupported("updateArray"); }

    @Override
    public RowId getRowId(int columnIndex) throws SQLException { throw Errors.notSupported("getRowId"); }

    @Override
    public RowId getRowId(String columnLabel) throws SQLException { throw Errors.notSupported("getRowId"); }

    @Override
    public void updateRowId(int columnIndex, RowId x) throws SQLException { throw Errors.notSupported("updateRowId"); }

    @Override
    public void updateRowId(String columnLabel, RowId x) throws SQLException { throw Errors.notSupported("updateRowId"); }

    @Override
    public void updateNString(int columnIndex, String nString) throws SQLException { throw Errors.notSupported("updateNString"); }

    @Override
    public void updateNString(String columnLabel, String nString) throws SQLException { throw Errors.notSupported("updateNString"); }

    @Override
    public void updateNClob(int columnIndex, NClob nClob) throws SQLException { throw Errors.notSupported("updateNClob"); }

    @Override
    public void updateNClob(String columnLabel, NClob nClob) throws SQLException { throw Errors.notSupported("updateNClob"); }

    @Override
    public NClob getNClob(int columnIndex) throws SQLException { throw Errors.notSupported("getNClob"); }

    @Override
    public NClob getNClob(String columnLabel) throws SQLException { throw Errors.notSupported("getNClob"); }

    @Override
    public SQLXML getSQLXML(int columnIndex) throws SQLException { throw Errors.notSupported("getSQLXML"); }

    @Override
    public SQLXML getSQLXML(String columnLabel) throws SQLException { throw Errors.notSupported("getSQLXML"); }

    @Override
    public void updateSQLXML(int columnIndex, SQLXML xmlObject) throws SQLException { throw Errors.notSupported("updateSQLXML"); }

    @Override
    public void updateSQLXML(String columnLabel, SQLXML xmlObject) throws SQLException { throw Errors.notSupported("updateSQLXML"); }

    @Override
    public String getNString(int columnIndex) throws SQLException { return getString(columnIndex); }

    @Override
    public String getNString(String columnLabel) throws SQLException { return getString(columnLabel); }

    @Override
    public Reader getNCharacterStream(int columnIndex) throws SQLException { throw Errors.notSupported("getNCharacterStream"); }

    @Override
    public Reader getNCharacterStream(String columnLabel) throws SQLException { throw Errors.notSupported("getNCharacterStream"); }

    @Override
    public void updateNCharacterStream(int columnIndex, Reader x, long length) throws SQLException { throw Errors.notSupported("updateNCharacterStream"); }

    @Override
    public void updateNCharacterStream(String columnLabel, Reader reader, long length) throws SQLException { throw Errors.notSupported("updateNCharacterStream"); }

    @Override
    public void updateAsciiStream(int columnIndex, InputStream x, long length) throws SQLException { throw Errors.notSupported("updateAsciiStream"); }

    @Override
    public void updateBinaryStream(int columnIndex, InputStream x, long length) throws SQLException { throw Errors.notSupported("updateBinaryStream"); }

    @Override
    public void updateCharacterStream(int columnIndex, Reader x, long length) throws SQLException { throw Errors.notSupported("updateCharacterStream"); }

    @Override
    public void updateAsciiStream(String columnLabel, InputStream x, long length) throws SQLException { throw Errors.notSupported("updateAsciiStream"); }

    @Override
    public void updateBinaryStream(String columnLabel, InputStream x, long length) throws SQLException { throw Errors.notSupported("updateBinaryStream"); }

    @Override
    public void updateCharacterStream(String columnLabel, Reader reader, long length) throws SQLException { throw Errors.notSupported("updateCharacterStream"); }

    @Override
    public void updateBlob(int columnIndex, InputStream inputStream, long length) throws SQLException { throw Errors.notSupported("updateBlob"); }

    @Override
    public void updateBlob(String columnLabel, InputStream inputStream, long length) throws SQLException { throw Errors.notSupported("updateBlob"); }

    @Override
    public void updateClob(int columnIndex, Reader reader, long length) throws SQLException { throw Errors.notSupported("updateClob"); }

    @Override
    public void updateClob(String columnLabel, Reader reader, long length) throws SQLException { throw Errors.notSupported("updateClob"); }

    @Override
    public void updateNClob(int columnIndex, Reader reader, long length) throws SQLException { throw Errors.notSupported("updateNClob"); }

    @Override
    public void updateNClob(String columnLabel, Reader reader, long length) throws SQLException { throw Errors.notSupported("updateNClob"); }

    @Override
    public void updateNCharacterStream(int columnIndex, Reader x) throws SQLException { throw Errors.notSupported("updateNCharacterStream"); }

    @Override
    public void updateNCharacterStream(String columnLabel, Reader reader) throws SQLException { throw Errors.notSupported("updateNCharacterStream"); }

    @Override
    public void updateAsciiStream(int columnIndex, InputStream x) throws SQLException { throw Errors.notSupported("updateAsciiStream"); }

    @Override
    public void updateBinaryStream(int columnIndex, InputStream x) throws SQLException { throw Errors.notSupported("updateBinaryStream"); }

    @Override
    public void updateCharacterStream(int columnIndex, Reader x) throws SQLException { throw Errors.notSupported("updateCharacterStream"); }

    @Override
    public void updateAsciiStream(String columnLabel, InputStream x) throws SQLException { throw Errors.notSupported("updateAsciiStream"); }

    @Override
    public void updateBinaryStream(String columnLabel, InputStream x) throws SQLException { throw Errors.notSupported("updateBinaryStream"); }

    @Override
    public void updateCharacterStream(String columnLabel, Reader reader) throws SQLException { throw Errors.notSupported("updateCharacterStream"); }

    @Override
    public void updateBlob(int columnIndex, InputStream inputStream) throws SQLException { throw Errors.notSupported("updateBlob"); }

    @Override
    public void updateBlob(String columnLabel, InputStream inputStream) throws SQLException { throw Errors.notSupported("updateBlob"); }

    @Override
    public void updateClob(int columnIndex, Reader reader) throws SQLException { throw Errors.notSupported("updateClob"); }

    @Override
    public void updateClob(String columnLabel, Reader reader) throws SQLException { throw Errors.notSupported("updateClob"); }

    @Override
    public void updateNClob(int columnIndex, Reader reader) throws SQLException { throw Errors.notSupported("updateNClob"); }

    @Override
    public void updateNClob(String columnLabel, Reader reader) throws SQLException { throw Errors.notSupported("updateNClob"); }

    @Override
    public <T> T getObject(int columnIndex, Class<T> type) throws SQLException {
        return type.cast(getObject(columnIndex));
    }

    @Override
    public <T> T getObject(String columnLabel, Class<T> type) throws SQLException {
        return type.cast(getObject(columnLabel));
    }

    @Override
    public InputStream getAsciiStream(int columnIndex) throws SQLException { throw Errors.notSupported("getAsciiStream"); }

    @Override
    public InputStream getUnicodeStream(int columnIndex) throws SQLException { throw Errors.notSupported("getUnicodeStream"); }

    @Override
    public InputStream getBinaryStream(int columnIndex) throws SQLException { throw Errors.notSupported("getBinaryStream"); }

    @Override
    public String getCursorName() throws SQLException { return null; }

    @Override
    public Reader getCharacterStream(int columnIndex) throws SQLException { throw Errors.notSupported("getCharacterStream"); }

    @Override
    public Reader getCharacterStream(String columnLabel) throws SQLException { throw Errors.notSupported("getCharacterStream"); }

    @Override
    public InputStream getAsciiStream(String columnLabel) throws SQLException { throw Errors.notSupported("getAsciiStream"); }

    @Override
    public InputStream getUnicodeStream(String columnLabel) throws SQLException { throw Errors.notSupported("getUnicodeStream"); }

    @Override
    public InputStream getBinaryStream(String columnLabel) throws SQLException { throw Errors.notSupported("getBinaryStream"); }

    @Override
    public SQLWarning getWarnings() throws SQLException { return null; }

    @Override
    public void clearWarnings() throws SQLException {}

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
