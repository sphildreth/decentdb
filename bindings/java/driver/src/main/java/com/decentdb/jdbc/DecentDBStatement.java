package com.decentdb.jdbc;

import java.lang.ref.Cleaner;
import java.sql.*;
import java.util.ArrayList;
import java.util.List;
import java.util.logging.Logger;

/**
 * DecentDB JDBC Statement.
 *
 * Not thread-safe. A single Statement may not be used concurrently from multiple threads.
 */
public class DecentDBStatement implements Statement {

    private static final Cleaner CLEANER = Cleaner.create();
    private static final Logger LOG = Logger.getLogger(DecentDBStatement.class.getName());

    protected final DecentDBConnection connection;
    protected volatile long stmtHandle = 0;
    protected volatile boolean closed = false;
    protected DecentDBResultSet currentResultSet = null;
    protected long updateCount = -1;
    private final List<String> batchSql = new ArrayList<>();

    private final Cleaner.Cleanable cleanable;

    DecentDBStatement(DecentDBConnection connection) {
        this.connection = connection;
        this.cleanable = CLEANER.register(this, () -> {
            if (stmtHandle != 0) {
                DecentDBNative.stmtFinalize(stmtHandle);
            }
        });
    }

    protected void checkOpen() throws SQLException {
        if (closed) throw new SQLException("Statement is closed", "24000");
        connection.checkOpen();
    }

    @Override
    public ResultSet executeQuery(String sql) throws SQLException {
        checkOpen();
        connection.connectionLock.lock();
        try {
            closeCurrentResultSet();
            int stepRc = prepareAndStep(sql);
            if (stmtHandle == 0) {
                throw Errors.general("Failed to prepare statement", -1);
            }
            currentResultSet = new DecentDBResultSet(this, stmtHandle, connection.getDbHandle(), stepRc);
            return currentResultSet;
        } finally {
            connection.connectionLock.unlock();
        }
    }

    @Override
    public int executeUpdate(String sql) throws SQLException {
        checkOpen();
        connection.ensureWriteAllowed(sql);
        connection.connectionLock.lock();
        try {
            connection.beginTransactionIfNeeded();
            closeCurrentResultSet();
            long[] outStmt = new long[1];
            int rc = DecentDBNative.stmtPrepare(connection.getDbHandle(), sql, outStmt);
            if (rc != 0 || outStmt[0] == 0) {
                Errors.checkStatus(connection.getDbHandle(), rc != 0 ? rc : DecentDBNative.ERR_INTERNAL);
            }
            stmtHandle = outStmt[0];
            rc = DecentDBNative.stmtStep(stmtHandle);
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
    public boolean execute(String sql) throws SQLException {
        checkOpen();
        connection.ensureWriteAllowed(sql);
        connection.connectionLock.lock();
        try {
            if (!isReadStatement(sql)) {
                connection.beginTransactionIfNeeded();
            }
            closeCurrentResultSet();
            long[] outStmt = new long[1];
            int rc = DecentDBNative.stmtPrepare(connection.getDbHandle(), sql, outStmt);
            if (rc != 0 || outStmt[0] == 0) {
                Errors.checkStatus(connection.getDbHandle(), rc != 0 ? rc : DecentDBNative.ERR_INTERNAL);
            }
            stmtHandle = outStmt[0];
            rc = isReadStatement(sql) ? DecentDBNative.stmtStepRowView(stmtHandle) : DecentDBNative.stmtStep(stmtHandle);
            if (rc != 0 && rc != 1) {
                Errors.checkStatus(connection.getDbHandle(), rc);
            }
            if (rc == 1 || isReadStatement(sql)) {
                // It is a read query, return a ResultSet even if exhausted (rc == 0).
                currentResultSet = new DecentDBResultSet(this, stmtHandle, connection.getDbHandle(), rc);
                updateCount = -1;
                return true;
            } else {
                // no rows and not a read statement - DML/DDL
                updateCount = DecentDBNative.stmtRowsAffected(stmtHandle);
                finalizeStmt();
                return false;
            }
        } finally {
            connection.connectionLock.unlock();
        }
    }

    /** Prepare and step once to position on first row (or done). Called for executeQuery. Returns rc: 1=row, 0=done. */
    private int prepareAndStep(String sql) throws SQLException {
        long[] outStmt = new long[1];
        int rc = DecentDBNative.stmtPrepare(connection.getDbHandle(), sql, outStmt);
        if (rc != 0 || outStmt[0] == 0) {
            Errors.checkStatus(connection.getDbHandle(), rc != 0 ? rc : DecentDBNative.ERR_INTERNAL);
        }
        stmtHandle = outStmt[0];
        // Step once to prime the cursor: rc=1 means row available, rc=0 means empty result
        rc = DecentDBNative.stmtStepRowView(stmtHandle);
        if (rc != 0 && rc != 1) {
            finalizeStmt();
            Errors.checkStatus(connection.getDbHandle(), rc);
        }
        return rc; // 1 = row available, 0 = exhausted
    }

    /**
     * Returns true if the statement is a read (SELECT/WITH/EXPLAIN/PRAGMA).
     * Used to decide whether to lazily begin a transaction before execution.
     */
    static boolean isReadStatement(String sql) {
        int i = 0;
        while (i < sql.length() && Character.isWhitespace(sql.charAt(i))) i++;
        int end = i;
        while (end < sql.length() && !Character.isWhitespace(sql.charAt(end))) end++;
        String first = sql.substring(i, end).toUpperCase(java.util.Locale.ROOT);
        return first.equals("SELECT") || first.equals("WITH") || first.equals("EXPLAIN") || first.equals("PRAGMA");
    }

    protected void finalizeStmt() {
        if (stmtHandle != 0) {
            DecentDBNative.stmtFinalize(stmtHandle);
            stmtHandle = 0;
        }
    }

    protected void closeCurrentResultSet() throws SQLException {
        if (currentResultSet != null && !currentResultSet.isClosed()) {
            currentResultSet.close();
        }
        currentResultSet = null;
    }

    @Override
    public ResultSet getResultSet() throws SQLException {
        return currentResultSet;
    }

    @Override
    public int getUpdateCount() throws SQLException {
        return (int) Math.min(updateCount, Integer.MAX_VALUE);
    }

    @Override
    public boolean getMoreResults() throws SQLException {
        closeCurrentResultSet();
        return false;
    }

    @Override
    public void close() throws SQLException {
        if (closed) return;
        closed = true;
        closeCurrentResultSet();
        finalizeStmt();
        cleanable.clean();
    }

    @Override
    public boolean isClosed() { return closed; }

    @Override
    public int getMaxFieldSize() throws SQLException { return 0; }

    @Override
    public void setMaxFieldSize(int max) throws SQLException {}

    @Override
    public int getMaxRows() throws SQLException { return 0; }

    @Override
    public void setMaxRows(int max) throws SQLException {}

    @Override
    public void setEscapeProcessing(boolean enable) throws SQLException {}

    @Override
    public int getQueryTimeout() throws SQLException { return 0; }

    @Override
    public void setQueryTimeout(int seconds) throws SQLException {}

    @Override
    public void cancel() throws SQLException {}

    @Override
    public SQLWarning getWarnings() throws SQLException { return null; }

    @Override
    public void clearWarnings() throws SQLException {}

    @Override
    public void setCursorName(String name) throws SQLException {}

    @Override
    public void setFetchDirection(int direction) throws SQLException {}

    @Override
    public int getFetchDirection() throws SQLException { return ResultSet.FETCH_FORWARD; }

    @Override
    public void setFetchSize(int rows) throws SQLException {}

    @Override
    public int getFetchSize() throws SQLException { return 0; }

    @Override
    public int getResultSetConcurrency() throws SQLException { return ResultSet.CONCUR_READ_ONLY; }

    @Override
    public int getResultSetType() throws SQLException { return ResultSet.TYPE_FORWARD_ONLY; }

    @Override
    public void addBatch(String sql) throws SQLException {
        checkOpen();
        if (sql == null || sql.isBlank()) {
            throw new SQLException("Batch SQL must not be blank", "22023");
        }
        batchSql.add(sql);
    }

    @Override
    public void clearBatch() throws SQLException {
        batchSql.clear();
    }

    @Override
    public int[] executeBatch() throws SQLException {
        checkOpen();
        connection.connectionLock.lock();
        try {
            int[] counts = new int[batchSql.size()];
            for (int i = 0; i < batchSql.size(); i++) {
                counts[i] = executeUpdate(batchSql.get(i));
            }
            batchSql.clear();
            return counts;
        } finally {
            connection.connectionLock.unlock();
        }
    }

    @Override
    public Connection getConnection() throws SQLException { return connection; }

    @Override
    public boolean getMoreResults(int current) throws SQLException { return false; }

    @Override
    public ResultSet getGeneratedKeys() throws SQLException {
        return DecentDBResultSet.empty();
    }

    @Override
    public int executeUpdate(String sql, int autoGeneratedKeys) throws SQLException {
        return executeUpdate(sql);
    }

    @Override
    public int executeUpdate(String sql, int[] columnIndexes) throws SQLException {
        return executeUpdate(sql);
    }

    @Override
    public int executeUpdate(String sql, String[] columnNames) throws SQLException {
        return executeUpdate(sql);
    }

    @Override
    public boolean execute(String sql, int autoGeneratedKeys) throws SQLException {
        return execute(sql);
    }

    @Override
    public boolean execute(String sql, int[] columnIndexes) throws SQLException {
        return execute(sql);
    }

    @Override
    public boolean execute(String sql, String[] columnNames) throws SQLException {
        return execute(sql);
    }

    @Override
    public int getResultSetHoldability() throws SQLException { return ResultSet.CLOSE_CURSORS_AT_COMMIT; }

    @Override
    public void setPoolable(boolean poolable) throws SQLException {}

    @Override
    public boolean isPoolable() throws SQLException { return false; }

    @Override
    public void closeOnCompletion() throws SQLException {}

    @Override
    public boolean isCloseOnCompletion() throws SQLException { return false; }

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
