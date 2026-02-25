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

    final ReentrantLock connectionLock = new ReentrantLock();
    private volatile long dbHandle;
    private volatile boolean closed = false;
    private volatile boolean autoCommit = true;
    private volatile boolean readOnly;
    private final String url;
    private boolean inTransaction = false;

    private final Cleaner.Cleanable cleanable;

    DecentDBConnection(long dbHandle, String url, boolean readOnly) {
        this.dbHandle = dbHandle;
        this.url = url;
        this.readOnly = readOnly;
        // Safety net: close native handle if GC collects this object before close()
        long handle = dbHandle;
        this.cleanable = CLEANER.register(this, () -> {
            if (handle != 0) {
                LOG.warning("DecentDBConnection was garbage-collected without being closed; closing native handle.");
                DecentDBNative.dbClose(handle);
            }
        });
    }

    long getDbHandle() throws SQLException {
        checkOpen();
        return dbHandle;
    }

    void checkOpen() throws SQLException {
        if (closed) throw Errors.connectionClosed("Connection is closed");
    }

    @Override
    public Statement createStatement() throws SQLException {
        checkOpen();
        return new DecentDBStatement(this);
    }

    @Override
    public PreparedStatement prepareStatement(String sql) throws SQLException {
        checkOpen();
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
                // Begin transaction
                executeUpdate("BEGIN");
                inTransaction = true;
            } else if (autoCommit && inTransaction) {
                // Commit current transaction
                executeUpdate("COMMIT");
                inTransaction = false;
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
            executeUpdate("COMMIT");
            inTransaction = false;
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
            executeUpdate("ROLLBACK");
            inTransaction = false;
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
            executeUpdate("BEGIN");
            inTransaction = true;
        }
    }

    @Override
    public void close() throws SQLException {
        if (closed) return;
        connectionLock.lock();
        try {
            if (closed) return;
            if (inTransaction) {
                try { executeUpdate("ROLLBACK"); } catch (SQLException ignored) {}
                inTransaction = false;
            }
            closed = true;
            dbHandle = 0;
            cleanable.clean(); // triggers lambda: dbClose(original handle)
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
        long[] outStmt = new long[1];
        int rc = DecentDBNative.stmtPrepare(dbHandle, sql, outStmt);
        if (rc < 0 || outStmt[0] == 0) {
            Errors.checkResult(dbHandle, rc < 0 ? rc : -1);
        }
        long stmt = outStmt[0];
        try {
            rc = DecentDBNative.stmtStep(stmt);
            if (rc < 0) {
                Errors.checkResult(dbHandle, rc);
            }
        } finally {
            DecentDBNative.stmtFinalize(stmt);
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
