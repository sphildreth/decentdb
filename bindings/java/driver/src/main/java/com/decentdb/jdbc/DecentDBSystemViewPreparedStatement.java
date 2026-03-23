package com.decentdb.jdbc;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Collections;

/**
 * Intercepts DBeaver's generic query to get view definition DDL without requiring
 * DecentDB to actually compile and execute the query natively.
 */
class DecentDBSystemViewPreparedStatement extends DecentDBPreparedStatement {
    private String viewName = "";

    DecentDBSystemViewPreparedStatement(DecentDBConnection connection) throws SQLException {
        // Init with a valid dummy statement just to appease the base class constructor.
        // It won't actually be native-prepared or executed.
        super(connection, "SELECT 1");
    }

    @Override
    public void setString(int parameterIndex, String x) throws SQLException {
        // We only expect index 1 to be the view name.
        if (parameterIndex == 1) {
            this.viewName = x;
        }
    }

    @Override
    public void setObject(int parameterIndex, Object x) throws SQLException {
        if (parameterIndex == 1 && x instanceof String) {
            this.viewName = (String) x;
        }
    }

    @Override
    public ResultSet executeQuery() throws SQLException {
        // Fetch it via native C API!
        DecentDBConnection conn = (DecentDBConnection) getConnection();
        String ddl = DecentDBNative.metaGetViewDDL(conn.getDbHandle(), viewName);
        return DecentDBDatabaseMetaData.buildResultSet(
            new String[]{"sql_text"},
            Collections.singletonList(new Object[]{ddl != null ? ddl : "-- View definition not available"})
        );
    }
}
