package com.decentdb.jdbc;

import java.lang.ref.Cleaner;
import java.sql.*;
import java.util.Properties;
import java.util.concurrent.Executor;
import java.util.concurrent.locks.ReentrantLock;
import java.util.logging.Logger;
import java.util.Map;

/**
 * DecentDB JDBC Connection.
 *
 * <h3>Thread-safety</h3>
 * Connection-level operations are protected by {@code connectionLock}. Individual
 * Statement/ResultSet objects are NOT thread-safe; do not share them across threads.
 * DecentDB enforces "one writer / many readers" at the engine level; this class
 * does not add additional concurrency beyond serializing native calls per connection.
 *
 * <h3>Auto-commit</h3>
 * Auto-commit is {@code true} by default (each statement executes in its own transaction).
 * Set {@code setAutoCommit(false)} to manage transactions manually.
 *
 * <h3>Isolation</h3>
 * DecentDB implements Snapshot Isolation, which maps to JDBC
 * {@link Connection#TRANSACTION_REPEATABLE_READ}. {@code TRANSACTION_SERIALIZABLE}
 * and {@code TRANSACTION_READ_UNCOMMITTED} are not supported and will throw
 * {@link SQLFeatureNotSupportedException}.
 */
public final class DecentDBConnection implements Connection {

    private static final Cleaner CLEANER = Cleaner.create();
    private static final Logger LOG = Logger.getLogger(DecentDBConnection.class.getName());

    private static final class NativeDbCleanup implements Runnable {
        private volatile long handle;
        private volatile boolean closedExplicitly = false;

        NativeDbCleanup(long handle) {
            this.handle = handle;
        }

        @Override
        public void run() {
            long h = handle;
            handle = 0;
            if (h != 0) {
                if (!closedExplicitly) {
                    LOG.warning("DecentDBConnection was garbage-collected without being closed; closing native handle.");
                }
                DecentDBNative.dbClose(h);
            }
        }

        void closeExplicitly() {
            closedExplicitly = true;
            run();
        }
    }

    final ReentrantLock connectionLock = new ReentrantLock();
    private volatile long dbHandle;
    private volatile boolean closed = false;
    private volatile boolean autoCommit = true;
    private volatile boolean readOnly;
    private final String url;
    private boolean inTransaction = false;

    private final NativeDbCleanup cleanupState;
    private final Cleaner.Cleanable cleanable;

    DecentDBConnection(long dbHandle, String url, boolean readOnly) {
        this.dbHandle = dbHandle;
        this.url = url;
        this.readOnly = readOnly;
        this.cleanupState = new NativeDbCleanup(dbHandle);
        this.cleanable = CLEANER.register(this, cleanupState);
    }

    long getDbHandle() throws SQLException {
        checkOpen();
        return dbHandle;
    }

    String getUrl() {
        return url;
    }

    void checkOpen() throws SQLException {
        if (closed) throw Errors.connectionClosed("Connection is closed");
    }

    void ensureWriteAllowed(String sql) throws SQLException {
        if (readOnly && !DecentDBStatement.isReadStatement(sql)) {
            throw Errors.readOnlyViolation("Connection is read-only: " + sql);
        }
    }

    void ensureWriteAllowed(boolean readQuery, String operation) throws SQLException {
        if (readOnly && !readQuery) {
            throw Errors.readOnlyViolation("Connection is read-only: " + operation);
        }
    }

    private boolean queryInTransactionLocked() throws SQLException {
        int rc = DecentDBNative.dbInTransaction(dbHandle);
        if (rc < 0) {
            Errors.checkStatus(dbHandle, rc);
        }
        return rc != 0;
    }

    private void refreshInTransactionLocked() throws SQLException {
        inTransaction = queryInTransactionLocked();
    }

    @Override
    public Statement createStatement() throws SQLException {
        checkOpen();
        return new DecentDBStatement(this);
    }

    @Override
    public PreparedStatement prepareStatement(String sql) throws SQLException {
        checkOpen();
        if ("SELECT sql_text FROM decentdb_system_views WHERE name=?".equals(sql)) {
            return new DecentDBSystemViewPreparedStatement(this);
        }
        return new DecentDBPreparedStatement(this, sql);
    }

    @Override
    public CallableStatement prepareCall(String sql) throws SQLException {
        throw Errors.notSupported("CallableStatement / stored procedures");
    }

    @Override
    public String nativeSQL(String sql) throws SQLException {
        checkOpen();
        return sql;
    }

    @Override
    public boolean getAutoCommit() throws SQLException {
        checkOpen();
        return autoCommit;
    }

    @Override
    public void setAutoCommit(boolean autoCommit) throws SQLException {
        checkOpen();
        connectionLock.lock();
        try {
            if (this.autoCommit == autoCommit) return;
            if (!autoCommit && !inTransaction) {
                beginTransactionNativeLocked();
                refreshInTransactionLocked();
            } else if (autoCommit && inTransaction) {
                commitTransactionNativeLocked();
                refreshInTransactionLocked();
            }
            this.autoCommit = autoCommit;
        } finally {
            connectionLock.unlock();
        }
    }

    @Override
    public void commit() throws SQLException {
        checkOpen();
        if (autoCommit) throw new SQLException("Cannot commit in auto-commit mode", "25000");
        connectionLock.lock();
        try {
            commitTransactionNativeLocked();
            refreshInTransactionLocked();
        } finally {
            connectionLock.unlock();
        }
    }

    @Override
    public void rollback() throws SQLException {
        checkOpen();
        if (autoCommit) throw new SQLException("Cannot rollback in auto-commit mode", "25000");
        connectionLock.lock();
        try {
            rollbackTransactionNativeLocked();
            refreshInTransactionLocked();
        } finally {
            connectionLock.unlock();
        }
    }

    /**
     * Start a transaction if we are in manual-commit mode and one isn't already active.
     * Called by Statement/PreparedStatement before executing write statements, so that
     * read statements (SELECT) after a COMMIT can still use the WAL overlay
     * (which requires {@code db.activeWriter == nil}).
     */
    void beginTransactionIfNeeded() throws SQLException {
        if (!autoCommit && !inTransaction) {
            beginTransactionNativeLocked();
            refreshInTransactionLocked();
        }
    }

    @Override
    public void close() throws SQLException {
        if (closed) return;
        connectionLock.lock();
        try {
            if (closed) return;
            if (inTransaction) {
                try { rollbackTransactionNativeLocked(); } catch (SQLException ignored) {}
                inTransaction = false;
            }
            closed = true;
            dbHandle = 0;
            cleanupState.closeExplicitly();
            cleanable.clean();
        } finally {
            connectionLock.unlock();
        }
    }

    @Override
    public boolean isClosed() {
        return closed;
    }

    @Override
    public DatabaseMetaData getMetaData() throws SQLException {
        checkOpen();
        return new DecentDBDatabaseMetaData(this);
    }

    @Override
    public boolean isReadOnly() throws SQLException {
        checkOpen();
        return readOnly;
    }

    @Override
    public void setReadOnly(boolean readOnly) throws SQLException {
        checkOpen();
        this.readOnly = readOnly;
    }

    public boolean isInTransaction() throws SQLException {
        checkOpen();
        connectionLock.lock();
        try {
            refreshInTransactionLocked();
            return inTransaction;
        } finally {
            connectionLock.unlock();
        }
    }

    public int getAbiVersion() {
        return DecentDBNative.abiVersion();
    }

    public String getEngineVersion() {
        return DecentDBNative.engineVersion();
    }

    public void checkpoint() throws SQLException {
        checkOpen();
        connectionLock.lock();
        try {
            int rc = DecentDBNative.dbCheckpoint(dbHandle);
            if (rc != 0) {
                Errors.checkStatus(dbHandle, rc);
            }
        } finally {
            connectionLock.unlock();
        }
    }

    public void saveAs(String destPath) throws SQLException {
        checkOpen();
        if (destPath == null || destPath.isBlank()) {
            throw new SQLException("Destination path must not be blank", "22023");
        }
        connectionLock.lock();
        try {
            int rc = DecentDBNative.dbSaveAs(dbHandle, destPath);
            if (rc != 0) {
                Errors.checkStatus(dbHandle, rc);
            }
        } finally {
            connectionLock.unlock();
        }
    }

    @Override
    public String getCatalog() throws SQLException {
        checkOpen();
        return null;
    }

    @Override
    public void setCatalog(String catalog) throws SQLException {
        // DecentDB doesn't support catalogs; ignore silently per JDBC spec
    }

    @Override
    public int getTransactionIsolation() throws SQLException {
        checkOpen();
        return Connection.TRANSACTION_REPEATABLE_READ;
    }

    @Override
    public void setTransactionIsolation(int level) throws SQLException {
        checkOpen();
        switch (level) {
            case Connection.TRANSACTION_REPEATABLE_READ:
            case Connection.TRANSACTION_READ_COMMITTED:
                // Both map to DecentDB's Snapshot Isolation; accept silently
                break;
            case Connection.TRANSACTION_SERIALIZABLE:
                throw Errors.notSupported("TRANSACTION_SERIALIZABLE (DecentDB uses Snapshot Isolation, " +
                    "which does not prevent write skews; use TRANSACTION_REPEATABLE_READ)");
            case Connection.TRANSACTION_READ_UNCOMMITTED:
                throw Errors.notSupported("TRANSACTION_READ_UNCOMMITTED (DecentDB never allows dirty reads)");
            default:
                throw new SQLException("Unknown isolation level: " + level, "HY000");
        }
    }

    @Override
    public SQLWarning getWarnings() throws SQLException { return null; }

    @Override
    public void clearWarnings() throws SQLException {}

    @Override
    public Statement createStatement(int resultSetType, int resultSetConcurrency) throws SQLException {
        return createStatement();
    }

    @Override
    public PreparedStatement prepareStatement(String sql, int resultSetType, int resultSetConcurrency)
            throws SQLException {
        return prepareStatement(sql);
    }

    @Override
    public CallableStatement prepareCall(String sql, int resultSetType, int resultSetConcurrency)
            throws SQLException {
        throw Errors.notSupported("CallableStatement");
    }

    @Override
    public Map<String, Class<?>> getTypeMap() throws SQLException { return null; }

    @Override
    public void setTypeMap(Map<String, Class<?>> map) throws SQLException {}

    @Override
    public void setHoldability(int holdability) throws SQLException {}

    @Override
    public int getHoldability() throws SQLException { return ResultSet.CLOSE_CURSORS_AT_COMMIT; }

    @Override
    public Savepoint setSavepoint() throws SQLException {
        throw Errors.notSupported("setSavepoint() without name; use setSavepoint(name)");
    }

    @Override
    public Savepoint setSavepoint(String name) throws SQLException {
        checkOpen();
        beginTransactionIfNeeded();
        executeUpdate("SAVEPOINT " + name);
        return new DecentDBSavepoint(name);
    }

    @Override
    public void rollback(Savepoint savepoint) throws SQLException {
        checkOpen();
        executeUpdate("ROLLBACK TO SAVEPOINT " + savepoint.getSavepointName());
    }

    @Override
    public void releaseSavepoint(Savepoint savepoint) throws SQLException {
        checkOpen();
        executeUpdate("RELEASE SAVEPOINT " + savepoint.getSavepointName());
    }

    @Override
    public Statement createStatement(int resultSetType, int resultSetConcurrency, int resultSetHoldability)
            throws SQLException {
        return createStatement();
    }

    @Override
    public PreparedStatement prepareStatement(String sql, int resultSetType, int resultSetConcurrency,
            int resultSetHoldability) throws SQLException {
        return prepareStatement(sql);
    }

    @Override
    public CallableStatement prepareCall(String sql, int resultSetType, int resultSetConcurrency,
            int resultSetHoldability) throws SQLException {
        throw Errors.notSupported("CallableStatement");
    }

    @Override
    public PreparedStatement prepareStatement(String sql, int autoGeneratedKeys) throws SQLException {
        return prepareStatement(sql);
    }

    @Override
    public PreparedStatement prepareStatement(String sql, int[] columnIndexes) throws SQLException {
        return prepareStatement(sql);
    }

    @Override
    public PreparedStatement prepareStatement(String sql, String[] columnNames) throws SQLException {
        return prepareStatement(sql);
    }

    @Override
    public Clob createClob() throws SQLException { throw Errors.notSupported("Clob"); }

    @Override
    public Blob createBlob() throws SQLException { throw Errors.notSupported("Blob"); }

    @Override
    public NClob createNClob() throws SQLException { throw Errors.notSupported("NClob"); }

    @Override
    public SQLXML createSQLXML() throws SQLException { throw Errors.notSupported("SQLXML"); }

    @Override
    public boolean isValid(int timeout) throws SQLException {
        if (closed) return false;
        try {
            connectionLock.lock();
            try (Statement s = createStatement(); ResultSet rs = s.executeQuery("SELECT 1")) {
                return rs.next();
            }
        } catch (SQLException e) {
            return false;
        } finally {
            connectionLock.unlock();
        }
    }

    @Override
    public void setClientInfo(String name, String value) throws SQLClientInfoException {}

    @Override
    public void setClientInfo(Properties properties) throws SQLClientInfoException {}

    @Override
    public String getClientInfo(String name) throws SQLException { return null; }

    @Override
    public Properties getClientInfo() throws SQLException { return new Properties(); }

    @Override
    public Array createArrayOf(String typeName, Object[] elements) throws SQLException {
        throw Errors.notSupported("Array");
    }

    @Override
    public Struct createStruct(String typeName, Object[] attributes) throws SQLException {
        throw Errors.notSupported("Struct");
    }

    @Override
    public void setSchema(String schema) throws SQLException {}

    @Override
    public String getSchema() throws SQLException { return null; }

    @Override
    public void abort(Executor executor) throws SQLException { close(); }

    @Override
    public void setNetworkTimeout(Executor executor, int milliseconds) throws SQLException {}

    @Override
    public int getNetworkTimeout() throws SQLException { return 0; }

    @Override
    public <T> T unwrap(Class<T> iface) throws SQLException {
        if (iface.isAssignableFrom(getClass())) return iface.cast(this);
        throw new SQLException("Cannot unwrap to " + iface.getName());
    }

    @Override
    public boolean isWrapperFor(Class<?> iface) throws SQLException {
        return iface.isAssignableFrom(getClass());
    }

    /** Execute a SQL statement with no result set (used internally for BEGIN/COMMIT/ROLLBACK). */
    void executeUpdate(String sql) throws SQLException {
        long[] outAffected = new long[1];
        int rc = DecentDBNative.dbExecuteImmediate(dbHandle, sql, outAffected);
        if (rc != 0) {
            Errors.checkStatus(dbHandle, rc);
        }
    }

    private void beginTransactionNativeLocked() throws SQLException {
        int rc = DecentDBNative.dbBeginTransaction(dbHandle);
        if (rc != 0) {
            Errors.checkStatus(dbHandle, rc);
        }
    }

    private long commitTransactionNativeLocked() throws SQLException {
        long[] outLsn = new long[1];
        int rc = DecentDBNative.dbCommitTransaction(dbHandle, outLsn);
        if (rc != 0) {
            Errors.checkStatus(dbHandle, rc);
        }
        return outLsn[0];
    }

    private void rollbackTransactionNativeLocked() throws SQLException {
        int rc = DecentDBNative.dbRollbackTransaction(dbHandle);
        if (rc != 0) {
            Errors.checkStatus(dbHandle, rc);
        }
    }

    // ---- Internal Savepoint implementation ----------------------------

    private static final class DecentDBSavepoint implements Savepoint {
        private final String name;

        DecentDBSavepoint(String name) { this.name = name; }

        @Override
        public int getSavepointId() throws SQLException {
            throw Errors.notSupported("getSavepointId() - DecentDB uses named savepoints");
        }

        @Override
        public String getSavepointName() { return name; }
    }
}
