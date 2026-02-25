package com.decentdb.jdbc;

/**
 * JNI declarations mirroring the DecentDB C API.
 *
 * All methods are declared native and implemented in decentdb_jni.c.
 * Pointer handles are carried as Java {@code long} (opaque native pointers).
 * The caller is responsible for lifetime management via close() on wrapper objects.
 *
 * Thread-safety: individual statement/result handles are NOT thread-safe.
 * Serialize concurrent access at the Connection level if needed.
 */
final class DecentDBNative {

    static {
        NativeLibLoader.ensureLoaded();
    }

    private DecentDBNative() {}

    // ---- Database handle -----------------------------------------------

    /** Opens a database file. Returns native handle or 0 on error. */
    static native long dbOpen(String path, String options);

    /** Closes a database. Returns 0 on success. */
    static native int dbClose(long dbHandle);

    /** Returns last error code for the handle (0 = OK). */
    static native int dbLastErrorCode(long dbHandle);

    /** Returns last error message for the handle. */
    static native String dbLastErrorMessage(long dbHandle);

    // ---- Statement handle ----------------------------------------------

    /**
     * Prepares a SQL statement.
     * Returns a native statement handle (long[0]) and result code.
     */
    static native int stmtPrepare(long dbHandle, String sql, long[] outStmt);

    /** Executes one step. Returns: 1 = row available, 0 = done, <0 = error. */
    static native int stmtStep(long stmtHandle);

    /** Resets the statement for re-execution. */
    static native int stmtReset(long stmtHandle);

    /** Clears all parameter bindings. */
    static native int stmtClearBindings(long stmtHandle);

    /** Finalizes (frees) the statement. */
    static native void stmtFinalize(long stmtHandle);

    /** Returns number of rows affected by last DML statement. */
    static native long stmtRowsAffected(long stmtHandle);

    // ---- Parameter binding (1-based index) ----------------------------

    static native int bindNull(long stmtHandle, int col);
    static native int bindInt64(long stmtHandle, int col, long value);
    static native int bindFloat64(long stmtHandle, int col, double value);
    static native int bindText(long stmtHandle, int col, String value);
    static native int bindBlob(long stmtHandle, int col, byte[] value);

    // ---- Column access (0-based index) --------------------------------

    /** Returns number of result columns. */
    static native int colCount(long stmtHandle);

    /** Returns column name at index (0-based). */
    static native String colName(long stmtHandle, int index);

    /**
     * Returns column type kind at index.
     * 0=NULL, 1=INT64, 2=FLOAT64, 3=TEXT, 4=BLOB, 5=BOOL, 6=DECIMAL
     */
    static native int colType(long stmtHandle, int index);

    /** Returns 1 if column value is NULL. */
    static native int colIsNull(long stmtHandle, int index);

    static native long colInt64(long stmtHandle, int index);
    static native double colFloat64(long stmtHandle, int index);
    static native String colText(long stmtHandle, int index);
    static native byte[] colBlob(long stmtHandle, int index);

    /** Returns decimal scale (digits after decimal point). */
    static native int colDecimalScale(long stmtHandle, int index);

    /** Returns unscaled integer value for DECIMAL columns. */
    static native long colDecimalUnscaled(long stmtHandle, int index);

    // ---- Metadata (return JSON strings) --------------------------------

    /** Returns JSON array of table names: ["t1","t2",...]. */
    static native String metaListTables(long dbHandle);

    /** Returns JSON array of column metadata objects for a table. */
    static native String metaGetTableColumns(long dbHandle, String tableName);

    /** Returns JSON array of index metadata objects. */
    static native String metaListIndexes(long dbHandle);

    // ---- Value kind constants (must match c_api.nim / value.nim) -------
    static final int KIND_NULL    = 0;
    static final int KIND_INT64   = 1;
    static final int KIND_BOOL    = 2;
    static final int KIND_FLOAT64 = 3;
    static final int KIND_TEXT    = 4;
    static final int KIND_BLOB    = 5;
    static final int KIND_DECIMAL = 12;
    // Compact single-byte kinds that may be returned by colType
    static final int KIND_BOOL_FALSE = 13;
    static final int KIND_BOOL_TRUE  = 14;
    static final int KIND_INT0       = 15;
    static final int KIND_INT1       = 16;

    // ---- Error code constants (must match src/errors.nim + c_api.nim) --
    static final int ERR_OK          = 0;
    static final int ERR_IO          = 1;
    static final int ERR_CORRUPTION  = 2;
    static final int ERR_CONSTRAINT  = 3;
    static final int ERR_TRANSACTION = 4;
    static final int ERR_SQL         = 5;
    static final int ERR_INTERNAL    = 6;
}
