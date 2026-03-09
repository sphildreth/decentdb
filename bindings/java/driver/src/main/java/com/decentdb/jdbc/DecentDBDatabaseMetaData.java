package com.decentdb.jdbc;

import org.json.JSONArray;
import org.json.JSONObject;

import java.math.BigDecimal;
import java.net.URL;
import java.sql.*;
import java.util.*;

/**
 * DatabaseMetaData for DecentDB.
 *
 * Implements the JDBC metadata methods that DBeaver relies on for:
 * - Navigator tree (tables, columns)
 * - ER diagram (primary keys, foreign keys, imported/exported keys)
 * - Index browser (getIndexInfo)
 *
 * Metadata is fetched via the DecentDB C API JSON functions and parsed here.
 * This avoids any SQL-based metadata queries (no information_schema needed).
 */
@SuppressWarnings("deprecation")
public final class DecentDBDatabaseMetaData implements DatabaseMetaData {

    private final DecentDBConnection connection;

    DecentDBDatabaseMetaData(DecentDBConnection connection) {
        this.connection = connection;
    }

    // ---- Product info --------------------------------------------------

    @Override
    public String getDatabaseProductName() { return "DecentDB"; }

    @Override
    public String getDatabaseProductVersion() { return "1.5.0"; }

    @Override
    public String getDriverName() { return "DecentDB JDBC Driver"; }

    @Override
    public String getDriverVersion() {
        return DecentDBDriver.DRIVER_MAJOR_VERSION + "." + DecentDBDriver.DRIVER_MINOR_VERSION;
    }

    @Override
    public int getDriverMajorVersion() { return DecentDBDriver.DRIVER_MAJOR_VERSION; }

    @Override
    public int getDriverMinorVersion() { return DecentDBDriver.DRIVER_MINOR_VERSION; }

    @Override
    public int getDatabaseMajorVersion() { return 1; }

    @Override
    public int getDatabaseMinorVersion() { return 5; }

    @Override
    public int getJDBCMajorVersion() { return 4; }

    @Override
    public int getJDBCMinorVersion() { return 3; }

    // ---- Capabilities --------------------------------------------------

    @Override
    public boolean allProceduresAreCallable() { return false; }

    @Override
    public boolean allTablesAreSelectable() { return true; }

    @Override
    public String getURL() throws SQLException {
        // url is stored on connection but not exposed; return empty
        return null;
    }

    @Override
    public String getUserName() { return ""; }

    @Override
    public boolean isReadOnly() throws SQLException { return connection.isReadOnly(); }

    @Override
    public boolean nullsAreSortedHigh() { return false; }

    @Override
    public boolean nullsAreSortedLow() { return true; }

    @Override
    public boolean nullsAreSortedAtStart() { return false; }

    @Override
    public boolean nullsAreSortedAtEnd() { return false; }

    @Override
    public boolean usesLocalFiles() { return true; }

    @Override
    public boolean usesLocalFilePerTable() { return false; }

    @Override
    public boolean supportsMixedCaseIdentifiers() { return true; }

    @Override
    public boolean storesUpperCaseIdentifiers() { return false; }

    @Override
    public boolean storesLowerCaseIdentifiers() { return false; }

    @Override
    public boolean storesMixedCaseIdentifiers() { return true; }

    @Override
    public boolean supportsMixedCaseQuotedIdentifiers() { return true; }

    @Override
    public boolean storesUpperCaseQuotedIdentifiers() { return false; }

    @Override
    public boolean storesLowerCaseQuotedIdentifiers() { return false; }

    @Override
    public boolean storesMixedCaseQuotedIdentifiers() { return true; }

    @Override
    public String getIdentifierQuoteString() { return "\""; }

    @Override
    public String getSQLKeywords() {
        return "UPSERT,RETURNING,EXPLAIN,VACUUM,CHECKPOINT,SAVEPOINT,RELEASE,TRIGRAM";
    }

    @Override
    public String getNumericFunctions() { return "abs,ceil,floor,round,mod,power,sqrt,log,ln,exp,sign"; }

    @Override
    public String getStringFunctions() {
        return "length,substr,substring,upper,lower,trim,ltrim,rtrim,replace,concat,like,instr";
    }

    @Override
    public String getSystemFunctions() { return "version,coalesce,nullif,typeof"; }

    @Override
    public String getTimeDateFunctions() {
        return "now,date,time,datetime,strftime,julianday,unixepoch";
    }

    @Override
    public String getSearchStringEscape() { return "\\"; }

    @Override
    public String getExtraNameCharacters() { return ""; }

    @Override
    public boolean supportsAlterTableWithAddColumn() { return false; }

    @Override
    public boolean supportsAlterTableWithDropColumn() { return false; }

    @Override
    public boolean supportsColumnAliasing() { return true; }

    @Override
    public boolean nullPlusNonNullIsNull() { return true; }

    @Override
    public boolean supportsConvert() { return false; }

    @Override
    public boolean supportsConvert(int fromType, int toType) { return false; }

    @Override
    public boolean supportsTableCorrelationNames() { return true; }

    @Override
    public boolean supportsDifferentTableCorrelationNames() { return false; }

    @Override
    public boolean supportsExpressionsInOrderBy() { return true; }

    @Override
    public boolean supportsOrderByUnrelated() { return true; }

    @Override
    public boolean supportsGroupBy() { return true; }

    @Override
    public boolean supportsGroupByUnrelated() { return true; }

    @Override
    public boolean supportsGroupByBeyondSelect() { return true; }

    @Override
    public boolean supportsLikeEscapeClause() { return true; }

    @Override
    public boolean supportsMultipleResultSets() { return false; }

    @Override
    public boolean supportsMultipleTransactions() { return false; }

    @Override
    public boolean supportsNonNullableColumns() { return true; }

    @Override
    public boolean supportsMinimumSQLGrammar() { return true; }

    @Override
    public boolean supportsCoreSQLGrammar() { return true; }

    @Override
    public boolean supportsExtendedSQLGrammar() { return false; }

    @Override
    public boolean supportsANSI92EntryLevelSQL() { return true; }

    @Override
    public boolean supportsANSI92IntermediateSQL() { return false; }

    @Override
    public boolean supportsANSI92FullSQL() { return false; }

    @Override
    public boolean supportsIntegrityEnhancementFacility() { return true; }

    @Override
    public boolean supportsOuterJoins() { return true; }

    @Override
    public boolean supportsFullOuterJoins() { return true; }

    @Override
    public boolean supportsLimitedOuterJoins() { return true; }

    @Override
    public String getSchemaTerm() { return "schema"; }

    @Override
    public String getProcedureTerm() { return "procedure"; }

    @Override
    public String getCatalogTerm() { return "catalog"; }

    @Override
    public boolean isCatalogAtStart() { return false; }

    @Override
    public String getCatalogSeparator() { return "."; }

    @Override
    public boolean supportsSchemasInDataManipulation() { return false; }

    @Override
    public boolean supportsSchemasInProcedureCalls() { return false; }

    @Override
    public boolean supportsSchemasInTableDefinitions() { return false; }

    @Override
    public boolean supportsSchemasInIndexDefinitions() { return false; }

    @Override
    public boolean supportsSchemasInPrivilegeDefinitions() { return false; }

    @Override
    public boolean supportsCatalogsInDataManipulation() { return false; }

    @Override
    public boolean supportsCatalogsInProcedureCalls() { return false; }

    @Override
    public boolean supportsCatalogsInTableDefinitions() { return false; }

    @Override
    public boolean supportsCatalogsInIndexDefinitions() { return false; }

    @Override
    public boolean supportsCatalogsInPrivilegeDefinitions() { return false; }

    @Override
    public boolean supportsPositionedDelete() { return false; }

    @Override
    public boolean supportsPositionedUpdate() { return false; }

    @Override
    public boolean supportsSelectForUpdate() { return false; }

    @Override
    public boolean supportsStoredProcedures() { return false; }

    @Override
    public boolean supportsSubqueriesInComparisons() { return true; }

    @Override
    public boolean supportsSubqueriesInExists() { return true; }

    @Override
    public boolean supportsSubqueriesInIns() { return true; }

    @Override
    public boolean supportsSubqueriesInQuantifieds() { return false; }

    @Override
    public boolean supportsCorrelatedSubqueries() { return true; }

    @Override
    public boolean supportsUnion() { return true; }

    @Override
    public boolean supportsUnionAll() { return true; }

    @Override
    public boolean supportsOpenCursorsAcrossCommit() { return false; }

    @Override
    public boolean supportsOpenCursorsAcrossRollback() { return false; }

    @Override
    public boolean supportsOpenStatementsAcrossCommit() { return false; }

    @Override
    public boolean supportsOpenStatementsAcrossRollback() { return false; }

    @Override
    public int getMaxBinaryLiteralLength() { return 0; }

    @Override
    public int getMaxCharLiteralLength() { return 0; }

    @Override
    public int getMaxColumnNameLength() { return 255; }

    @Override
    public int getMaxColumnsInGroupBy() { return 0; }

    @Override
    public int getMaxColumnsInIndex() { return 16; }

    @Override
    public int getMaxColumnsInOrderBy() { return 0; }

    @Override
    public int getMaxColumnsInSelect() { return 0; }

    @Override
    public int getMaxColumnsInTable() { return 0; }

    @Override
    public int getMaxConnections() { return 1; }

    @Override
    public int getMaxCursorNameLength() { return 0; }

    @Override
    public int getMaxIndexLength() { return 0; }

    @Override
    public int getMaxSchemaNameLength() { return 0; }

    @Override
    public int getMaxProcedureNameLength() { return 0; }

    @Override
    public int getMaxCatalogNameLength() { return 0; }

    @Override
    public int getMaxRowSize() { return 0; }

    @Override
    public boolean doesMaxRowSizeIncludeBlobs() { return false; }

    @Override
    public int getMaxStatementLength() { return 0; }

    @Override
    public int getMaxStatements() { return 0; }

    @Override
    public int getMaxTableNameLength() { return 255; }

    @Override
    public int getMaxTablesInSelect() { return 0; }

    @Override
    public int getMaxUserNameLength() { return 0; }

    @Override
    public int getDefaultTransactionIsolation() { return Connection.TRANSACTION_REPEATABLE_READ; }

    @Override
    public boolean supportsTransactions() { return true; }

    @Override
    public boolean supportsTransactionIsolationLevel(int level) {
        return level == Connection.TRANSACTION_REPEATABLE_READ ||
               level == Connection.TRANSACTION_READ_COMMITTED;
    }

    @Override
    public boolean supportsDataDefinitionAndDataManipulationTransactions() { return true; }

    @Override
    public boolean supportsDataManipulationTransactionsOnly() { return false; }

    @Override
    public boolean dataDefinitionCausesTransactionCommit() { return false; }

    @Override
    public boolean dataDefinitionIgnoredInTransactions() { return false; }

    @Override
    public boolean supportsResultSetType(int type) {
        return type == ResultSet.TYPE_FORWARD_ONLY;
    }

    @Override
    public boolean supportsResultSetConcurrency(int type, int concurrency) {
        return type == ResultSet.TYPE_FORWARD_ONLY && concurrency == ResultSet.CONCUR_READ_ONLY;
    }

    @Override
    public boolean ownUpdatesAreVisible(int type) { return false; }

    @Override
    public boolean ownDeletesAreVisible(int type) { return false; }

    @Override
    public boolean ownInsertsAreVisible(int type) { return false; }

    @Override
    public boolean othersUpdatesAreVisible(int type) { return false; }

    @Override
    public boolean othersDeletesAreVisible(int type) { return false; }

    @Override
    public boolean othersInsertsAreVisible(int type) { return false; }

    @Override
    public boolean updatesAreDetected(int type) { return false; }

    @Override
    public boolean deletesAreDetected(int type) { return false; }

    @Override
    public boolean insertsAreDetected(int type) { return false; }

    @Override
    public boolean supportsBatchUpdates() { return false; }

    @Override
    public boolean supportsSavepoints() { return true; }

    @Override
    public boolean supportsNamedParameters() { return false; }

    @Override
    public boolean supportsMultipleOpenResults() { return false; }

    @Override
    public boolean supportsGetGeneratedKeys() { return false; }

    @Override
    public boolean supportsResultSetHoldability(int holdability) {
        return holdability == ResultSet.CLOSE_CURSORS_AT_COMMIT;
    }

    @Override
    public int getResultSetHoldability() { return ResultSet.CLOSE_CURSORS_AT_COMMIT; }

    @Override
    public int getSQLStateType() { return DatabaseMetaData.sqlStateSQL; }

    @Override
    public boolean locatorsUpdateCopy() { return false; }

    @Override
    public boolean supportsStatementPooling() { return false; }

    @Override
    public RowIdLifetime getRowIdLifetime() { return RowIdLifetime.ROWID_UNSUPPORTED; }

    @Override
    public boolean supportsStoredFunctionsUsingCallSyntax() { return false; }

    @Override
    public boolean autoCommitFailureClosesAllResultSets() { return false; }

    // ---- Metadata result sets ------------------------------------------

    @Override
    public ResultSet getProcedures(String catalog, String schemaPattern, String procedureNamePattern)
            throws SQLException {
        return emptyResultSet("PROCEDURE_CAT", "PROCEDURE_SCHEM", "PROCEDURE_NAME",
            "RESERVED1", "RESERVED2", "RESERVED3", "REMARKS", "PROCEDURE_TYPE", "SPECIFIC_NAME");
    }

    @Override
    public ResultSet getProcedureColumns(String catalog, String schemaPattern, String procedureNamePattern,
            String columnNamePattern) throws SQLException {
        return emptyResultSet("PROCEDURE_CAT","PROCEDURE_SCHEM","PROCEDURE_NAME","COLUMN_NAME",
            "COLUMN_TYPE","DATA_TYPE","TYPE_NAME","PRECISION","LENGTH","SCALE","RADIX",
            "NULLABLE","REMARKS","COLUMN_DEF","SQL_DATA_TYPE","SQL_DATETIME_SUB",
            "CHAR_OCTET_LENGTH","ORDINAL_POSITION","IS_NULLABLE","SPECIFIC_NAME");
    }

    /**
     * Returns tables matching the given criteria.
     *
     * DBeaver calls this with tableNamePattern=null and types={"TABLE"} or types={"VIEW"} to
     * populate the navigator tree.
     *
     * Column schema (JDBC 4.3, Table B-5):
     * TABLE_CAT, TABLE_SCHEM, TABLE_NAME, TABLE_TYPE, REMARKS, TYPE_CAT, TYPE_SCHEM,
     * TYPE_NAME, SELF_REFERENCING_COL_NAME, REF_GENERATION
     */
    @Override
    public ResultSet getTables(String catalog, String schemaPattern, String tableNamePattern, String[] types)
            throws SQLException {
        connection.checkOpen();
        long dbHandle = connection.getDbHandle();

        List<Object[]> rows = new ArrayList<>();

        boolean includeTable = types == null || Arrays.asList(types).contains("TABLE");
        boolean includeView = types == null || Arrays.asList(types).contains("VIEW");

        if (includeTable) {
            String json = DecentDBNative.metaListTables(dbHandle);
            if (json != null) {
                JSONArray arr = new JSONArray(json);
                for (int i = 0; i < arr.length(); i++) {
                    String tbl = arr.getString(i);
                    if (tableNamePattern != null && !tableNamePattern.equals("%") &&
                        !tbl.equalsIgnoreCase(tableNamePattern)) continue;
                    rows.add(new Object[]{
                        null,   // TABLE_CAT
                        null,   // TABLE_SCHEM
                        tbl,    // TABLE_NAME
                        "TABLE", // TABLE_TYPE
                        "",     // REMARKS
                        null, null, null, null, null  // TYPE_CAT, TYPE_SCHEM, TYPE_NAME, SELF_REFERENCING_COL_NAME, REF_GENERATION
                    });
                }
            }
        }

        if (includeView) {
            String json = DecentDBNative.metaListViews(dbHandle);
            if (json != null) {
                JSONArray arr = new JSONArray(json);
                for (int i = 0; i < arr.length(); i++) {
                    String tbl = arr.getString(i);
                    if (tableNamePattern != null && !tableNamePattern.equals("%") &&
                        !tbl.equalsIgnoreCase(tableNamePattern)) continue;
                    rows.add(new Object[]{
                        null,   // TABLE_CAT
                        null,   // TABLE_SCHEM
                        tbl,    // TABLE_NAME
                        "VIEW",  // TABLE_TYPE
                        "",     // REMARKS
                        null, null, null, null, null  // TYPE_CAT, TYPE_SCHEM, TYPE_NAME, SELF_REFERENCING_COL_NAME, REF_GENERATION
                    });
                }
            }
        }

        return buildResultSet(
            new String[]{"TABLE_CAT","TABLE_SCHEM","TABLE_NAME","TABLE_TYPE","REMARKS",
                "TYPE_CAT","TYPE_SCHEM","TYPE_NAME","SELF_REFERENCING_COL_NAME","REF_GENERATION"},
            rows
        );
    }

    @Override
    public ResultSet getSchemas() throws SQLException {
        return emptyResultSet("TABLE_SCHEM", "TABLE_CATALOG");
    }

    @Override
    public ResultSet getSchemas(String catalog, String schemaPattern) throws SQLException {
        return getSchemas();
    }

    @Override
    public ResultSet getCatalogs() throws SQLException {
        return emptyResultSet("TABLE_CAT");
    }

    @Override
    public ResultSet getTableTypes() throws SQLException {
        return buildResultSet(
            new String[]{"TABLE_TYPE"},
            Arrays.asList(new Object[]{"TABLE"}, new Object[]{"VIEW"})
        );
    }

    /**
     * Returns column metadata for a table.
     *
     * JDBC column schema (B-4):
     * TABLE_CAT, TABLE_SCHEM, TABLE_NAME, COLUMN_NAME, DATA_TYPE, TYPE_NAME,
     * COLUMN_SIZE, BUFFER_LENGTH, DECIMAL_DIGITS, NUM_PREC_RADIX, NULLABLE,
     * REMARKS, COLUMN_DEF, SQL_DATA_TYPE, SQL_DATETIME_SUB, CHAR_OCTET_LENGTH,
     * ORDINAL_POSITION, IS_NULLABLE, SCOPE_CATALOG, SCOPE_SCHEMA, SCOPE_TABLE,
     * SOURCE_DATA_TYPE, IS_AUTOINCREMENT, IS_GENERATEDCOLUMN
     */
    @Override
    public ResultSet getColumns(String catalog, String schemaPattern, String tableNamePattern, String columnNamePattern)
            throws SQLException {
        connection.checkOpen();
        long dbHandle = connection.getDbHandle();

        List<Object[]> rows = new ArrayList<>();

        List<String> allTablesAndViews = new ArrayList<>();
        String tablesJson = DecentDBNative.metaListTables(dbHandle);
        if (tablesJson != null) {
            JSONArray arr = new JSONArray(tablesJson);
            for (int i = 0; i < arr.length(); i++) allTablesAndViews.add(arr.getString(i));
        }
        String viewsJson = DecentDBNative.metaListViews(dbHandle);
        if (viewsJson != null) {
            JSONArray arr = new JSONArray(viewsJson);
            for (int i = 0; i < arr.length(); i++) allTablesAndViews.add(arr.getString(i));
        }

        if (allTablesAndViews.isEmpty()) return emptyResultSet(COLUMN_COLUMNS);

        for (String tbl : allTablesAndViews) {
            if (tableNamePattern != null && !tableNamePattern.equals("%") &&
                !tbl.equalsIgnoreCase(tableNamePattern)) continue;

            String colsJson = DecentDBNative.metaGetTableColumns(dbHandle, tbl);
            if (colsJson == null) continue;

            JSONArray cols = new JSONArray(colsJson);
            for (int ci = 0; ci < cols.length(); ci++) {
                JSONObject col = cols.getJSONObject(ci);
                String colName = col.getString("name");
                if (columnNamePattern != null && !columnNamePattern.equals("%") &&
                    !colName.equalsIgnoreCase(columnNamePattern)) continue;

                String typeName = col.getString("type");
                int jdbcType = TypeMapping.jdbcTypeFromName(typeName);
                // DBeaver displays TYPE_NAME in the UI. Our catalog exposes canonical
                // internal names (INT64/FLOAT64/BOOL); normalize them to common SQL
                // names for a more familiar experience.
                String displayTypeName;
                if (typeName == null) {
                    displayTypeName = "OTHER";
                } else {
                    switch (typeName.toUpperCase().trim()) {
                        case "INT64":
                            displayTypeName = "INTEGER";
                            break;
                        case "FLOAT64":
                            displayTypeName = "REAL";
                            break;
                        case "BOOL":
                            displayTypeName = "BOOLEAN";
                            break;
                        default:
                            displayTypeName = typeName;
                            break;
                    }
                }
                boolean notNull = col.optBoolean("not_null", false);

                int columnSize;
                int decimalDigits;
                int radix;
                switch (jdbcType) {
                    case Types.BIGINT:
                        columnSize = 19;
                        decimalDigits = 0;
                        radix = 10;
                        break;
                    case Types.INTEGER:
                        columnSize = 10;
                        decimalDigits = 0;
                        radix = 10;
                        break;
                    case Types.BOOLEAN:
                        columnSize = 1;
                        decimalDigits = 0;
                        radix = 10;
                        break;
                    case Types.DOUBLE:
                        columnSize = 15;
                        decimalDigits = 0;
                        radix = 10;
                        break;
                    case Types.DECIMAL:
                        columnSize = 18;
                        decimalDigits = 0;
                        radix = 10;
                        break;
                    case Types.BINARY:
                        columnSize = typeName != null && typeName.equalsIgnoreCase("UUID") ? 16 : 0;
                        decimalDigits = 0;
                        radix = 0;
                        break;
                    case Types.VARCHAR:
                        columnSize = 0;
                        decimalDigits = 0;
                        radix = 0;
                        break;
                    default:
                        columnSize = 0;
                        decimalDigits = 0;
                        radix = 0;
                        break;
                }

                rows.add(new Object[]{
                    null,                          // TABLE_CAT
                    null,                          // TABLE_SCHEM
                    tbl,                           // TABLE_NAME
                    colName,                       // COLUMN_NAME
                    jdbcType,                      // DATA_TYPE
                    displayTypeName,               // TYPE_NAME
                    columnSize,                    // COLUMN_SIZE
                    null,                          // BUFFER_LENGTH
                    decimalDigits,                 // DECIMAL_DIGITS
                    radix,                         // NUM_PREC_RADIX
                    notNull ? DatabaseMetaData.columnNoNulls : DatabaseMetaData.columnNullable, // NULLABLE
                    "",                            // REMARKS
                    null,                          // COLUMN_DEF
                    null,                          // SQL_DATA_TYPE
                    null,                          // SQL_DATETIME_SUB
                    columnSize,                    // CHAR_OCTET_LENGTH
                    ci + 1,                        // ORDINAL_POSITION (1-based)
                    notNull ? "NO" : "YES",        // IS_NULLABLE
                    null, null, null,              // SCOPE_CATALOG, SCOPE_SCHEMA, SCOPE_TABLE
                    null,                          // SOURCE_DATA_TYPE
                    "NO",                          // IS_AUTOINCREMENT
                    "NO"                           // IS_GENERATEDCOLUMN
                });
            }
        }

        return buildResultSet(COLUMN_COLUMNS, rows);
    }

    private static final String[] COLUMN_COLUMNS = {
        "TABLE_CAT","TABLE_SCHEM","TABLE_NAME","COLUMN_NAME","DATA_TYPE","TYPE_NAME",
        "COLUMN_SIZE","BUFFER_LENGTH","DECIMAL_DIGITS","NUM_PREC_RADIX","NULLABLE",
        "REMARKS","COLUMN_DEF","SQL_DATA_TYPE","SQL_DATETIME_SUB","CHAR_OCTET_LENGTH",
        "ORDINAL_POSITION","IS_NULLABLE","SCOPE_CATALOG","SCOPE_SCHEMA","SCOPE_TABLE",
        "SOURCE_DATA_TYPE","IS_AUTOINCREMENT","IS_GENERATEDCOLUMN"
    };

    /**
     * Returns primary key metadata for a table.
     *
     * JDBC column schema: TABLE_CAT, TABLE_SCHEM, TABLE_NAME, COLUMN_NAME, KEY_SEQ, PK_NAME
     */
    @Override
    public ResultSet getPrimaryKeys(String catalog, String schema, String table) throws SQLException {
        connection.checkOpen();
        if (table == null) return emptyResultSet("TABLE_CAT","TABLE_SCHEM","TABLE_NAME","COLUMN_NAME","KEY_SEQ","PK_NAME");

        long dbHandle = connection.getDbHandle();
        String json = DecentDBNative.metaGetTableColumns(dbHandle, table);
        List<Object[]> rows = new ArrayList<>();

        if (json != null) {
            JSONArray cols = new JSONArray(json);
            int seq = 1;
            for (int i = 0; i < cols.length(); i++) {
                JSONObject col = cols.getJSONObject(i);
                if (col.optBoolean("primary_key", false)) {
                    rows.add(new Object[]{
                        null, null, table,
                        col.getString("name"),
                        seq++,
                        "PRIMARY"
                    });
                }
            }
        }

        return buildResultSet(
            new String[]{"TABLE_CAT","TABLE_SCHEM","TABLE_NAME","COLUMN_NAME","KEY_SEQ","PK_NAME"},
            rows
        );
    }

    /**
     * Returns imported key metadata (foreign keys where this table references another).
     *
     * JDBC column schema (B-5):
     * PKTABLE_CAT, PKTABLE_SCHEM, PKTABLE_NAME, PKCOLUMN_NAME,
     * FKTABLE_CAT, FKTABLE_SCHEM, FKTABLE_NAME, FKCOLUMN_NAME,
     * KEY_SEQ, UPDATE_RULE, DELETE_RULE, FK_NAME, PK_NAME, DEFERRABILITY
     */
    @Override
    public ResultSet getImportedKeys(String catalog, String schema, String table) throws SQLException {
        connection.checkOpen();
        if (table == null) return emptyResultSet(FK_COLUMNS);

        long dbHandle = connection.getDbHandle();
        String json = DecentDBNative.metaGetTableColumns(dbHandle, table);
        List<Object[]> rows = new ArrayList<>();

        if (json != null) {
            JSONArray cols = new JSONArray(json);
            int seq = 1;
            for (int i = 0; i < cols.length(); i++) {
                JSONObject col = cols.getJSONObject(i);
                String refTable = col.optString("ref_table", "");
                String refColumn = col.optString("ref_column", "");
                if (!refTable.isEmpty() && !refColumn.isEmpty()) {
                    String colName = col.getString("name");
                    String fkName = table + "_" + colName + "_fkey";
                    rows.add(buildFkRow(refTable, refColumn, table, colName,
                        1, col.optString("ref_on_update", "NO ACTION"),
                        col.optString("ref_on_delete", "NO ACTION"), fkName));
                }
            }
        }

        return buildResultSet(FK_COLUMNS, rows);
    }

    /**
     * Returns exported key metadata (foreign keys in other tables that reference this table).
     *
     * Column schema: same as getImportedKeys.
     */
    @Override
    public ResultSet getExportedKeys(String catalog, String schema, String table) throws SQLException {
        connection.checkOpen();
        if (table == null) return emptyResultSet(FK_COLUMNS);

        long dbHandle = connection.getDbHandle();
        List<Object[]> rows = new ArrayList<>();

        // Scan all tables looking for FKs pointing to `table`
        String tablesJson = DecentDBNative.metaListTables(dbHandle);
        if (tablesJson != null) {
            JSONArray tables = new JSONArray(tablesJson);
            for (int ti = 0; ti < tables.length(); ti++) {
                String fkTable = tables.getString(ti);
                if (fkTable.equalsIgnoreCase(table)) continue;

                String colsJson = DecentDBNative.metaGetTableColumns(dbHandle, fkTable);
                if (colsJson == null) continue;
                JSONArray cols = new JSONArray(colsJson);
                int seq = 1;
                for (int ci = 0; ci < cols.length(); ci++) {
                    JSONObject col = cols.getJSONObject(ci);
                    String refTable = col.optString("ref_table", "");
                    if (refTable.equalsIgnoreCase(table)) {
                        String refColumn = col.optString("ref_column", "");
                        String fkColName = col.getString("name");
                        String fkName = fkTable + "_" + fkColName + "_fkey";
                        rows.add(buildFkRow(table, refColumn, fkTable, fkColName,
                            1, col.optString("ref_on_update", "NO ACTION"),
                            col.optString("ref_on_delete", "NO ACTION"), fkName));
                    }
                }
            }
        }

        return buildResultSet(FK_COLUMNS, rows);
    }

    @Override
    public ResultSet getCrossReference(String parentCatalog, String parentSchema, String parentTable,
            String foreignCatalog, String foreignSchema, String foreignTable) throws SQLException {
        // Returns FK rows between a specific PK table and FK table
        connection.checkOpen();
        if (parentTable == null || foreignTable == null) return emptyResultSet(FK_COLUMNS);

        long dbHandle = connection.getDbHandle();
        List<Object[]> rows = new ArrayList<>();

        String colsJson = DecentDBNative.metaGetTableColumns(dbHandle, foreignTable);
        if (colsJson != null) {
            JSONArray cols = new JSONArray(colsJson);
            int seq = 1;
            for (int i = 0; i < cols.length(); i++) {
                JSONObject col = cols.getJSONObject(i);
                String refTable = col.optString("ref_table", "");
                if (refTable.equalsIgnoreCase(parentTable)) {
                    String refColumn = col.optString("ref_column", "");
                    String colName = col.getString("name");
                    String fkName = foreignTable + "_" + colName + "_fkey";
                    rows.add(buildFkRow(parentTable, refColumn, foreignTable, colName,
                        1, col.optString("ref_on_update", "NO ACTION"),
                        col.optString("ref_on_delete", "NO ACTION"), fkName));
                }
            }
        }

        return buildResultSet(FK_COLUMNS, rows);
    }

    private static final String[] FK_COLUMNS = {
        "PKTABLE_CAT","PKTABLE_SCHEM","PKTABLE_NAME","PKCOLUMN_NAME",
        "FKTABLE_CAT","FKTABLE_SCHEM","FKTABLE_NAME","FKCOLUMN_NAME",
        "KEY_SEQ","UPDATE_RULE","DELETE_RULE","FK_NAME","PK_NAME","DEFERRABILITY"
    };

    private static Object[] buildFkRow(String pkTable, String pkCol, String fkTable, String fkCol,
            int seq, String onUpdate, String onDelete, String fkName) {
        return new Object[]{
            null, null, pkTable, pkCol,
            null, null, fkTable, fkCol,
            seq,
            ruleCode(onUpdate),
            ruleCode(onDelete),
            fkName,
            "PRIMARY",
            DatabaseMetaData.importedKeyNotDeferrable
        };
    }

    private static int ruleCode(String action) {
        if (action == null) return DatabaseMetaData.importedKeyNoAction;
        switch (action.toUpperCase()) {
            case "CASCADE":     return DatabaseMetaData.importedKeyCascade;
            case "SET NULL":    return DatabaseMetaData.importedKeySetNull;
            case "SET DEFAULT": return DatabaseMetaData.importedKeySetDefault;
            case "RESTRICT":    return DatabaseMetaData.importedKeyRestrict;
            default:            return DatabaseMetaData.importedKeyNoAction;
        }
    }

    /**
     * Returns index metadata for a table.
     *
     * JDBC column schema (B-6):
     * TABLE_CAT, TABLE_SCHEM, TABLE_NAME, NON_UNIQUE, INDEX_QUALIFIER, INDEX_NAME,
     * TYPE, ORDINAL_POSITION, COLUMN_NAME, ASC_OR_DESC, CARDINALITY, PAGES, FILTER_CONDITION
     */
    @Override
    public ResultSet getIndexInfo(String catalog, String schema, String table, boolean unique, boolean approximate)
            throws SQLException {
        connection.checkOpen();
        if (table == null) return emptyResultSet(INDEX_COLUMNS);

        long dbHandle = connection.getDbHandle();
        String json = DecentDBNative.metaListIndexes(dbHandle);
        List<Object[]> rows = new ArrayList<>();

        if (json != null) {
            JSONArray indexes = new JSONArray(json);
            for (int i = 0; i < indexes.length(); i++) {
                JSONObject idx = indexes.getJSONObject(i);
                if (!idx.getString("table").equalsIgnoreCase(table)) continue;
                boolean isUnique = idx.optBoolean("unique", false);
                if (unique && !isUnique) continue;

                JSONArray cols = idx.getJSONArray("columns");
                for (int ci = 0; ci < cols.length(); ci++) {
                    rows.add(new Object[]{
                        null, null, table,
                        !isUnique,           // NON_UNIQUE
                        null,                // INDEX_QUALIFIER
                        idx.getString("name"), // INDEX_NAME
                        DatabaseMetaData.tableIndexOther, // TYPE
                        ci + 1,              // ORDINAL_POSITION
                        cols.getString(ci),  // COLUMN_NAME
                        "A",                 // ASC_OR_DESC
                        0,                   // CARDINALITY (unknown)
                        0,                   // PAGES (unknown)
                        null                 // FILTER_CONDITION
                    });
                }
            }
        }

        return buildResultSet(INDEX_COLUMNS, rows);
    }

    private static final String[] INDEX_COLUMNS = {
        "TABLE_CAT","TABLE_SCHEM","TABLE_NAME","NON_UNIQUE","INDEX_QUALIFIER",
        "INDEX_NAME","TYPE","ORDINAL_POSITION","COLUMN_NAME","ASC_OR_DESC",
        "CARDINALITY","PAGES","FILTER_CONDITION"
    };

    @Override
    public ResultSet getTypeInfo() throws SQLException {
        // Keep this aligned with DecentDB's canonical catalog type names
        // (see src/catalog/catalog.nim columnTypeToText).
        List<Object[]> rows = Arrays.asList(
            typeRow("INT64",   Types.BIGINT, 19, false),
            typeRow("BOOL",    Types.BOOLEAN, 1, false),
            typeRow("FLOAT64", Types.DOUBLE, 15, false),
            typeRow("DECIMAL", Types.DECIMAL, 18, false),
            typeRow("TEXT",    Types.VARCHAR, 0, true),
            typeRow("BLOB",    Types.BINARY, 0, false),
            typeRow("UUID",    Types.BINARY, 16, false)
        );
        return buildResultSet(new String[]{
            "TYPE_NAME","DATA_TYPE","PRECISION","LITERAL_PREFIX","LITERAL_SUFFIX",
            "CREATE_PARAMS","NULLABLE","CASE_SENSITIVE","SEARCHABLE","UNSIGNED_ATTRIBUTE",
            "FIXED_PREC_SCALE","AUTO_INCREMENT","LOCAL_TYPE_NAME","MINIMUM_SCALE",
            "MAXIMUM_SCALE","SQL_DATA_TYPE","SQL_DATETIME_SUB","NUM_PREC_RADIX"
        }, rows);
    }

    private static Object[] typeRow(String name, int type, int prec, boolean quoted) {
        return new Object[]{
            name, type, prec,
            quoted ? "'" : null, quoted ? "'" : null,
            null,
            DatabaseMetaData.typeNullable,
            true,
            DatabaseMetaData.typeSearchable,
            false, false, false,
            name, 0, 0, null, null, 10
        };
    }

    // ---- Unsupported / empty result sets --------------------------------

    @Override
    public ResultSet getColumnPrivileges(String catalog, String schema, String table, String columnNamePattern)
            throws SQLException {
        return emptyResultSet("TABLE_CAT","TABLE_SCHEM","TABLE_NAME","COLUMN_NAME","GRANTOR","GRANTEE","PRIVILEGE","IS_GRANTABLE");
    }

    @Override
    public ResultSet getTablePrivileges(String catalog, String schemaPattern, String tableNamePattern)
            throws SQLException {
        return emptyResultSet("TABLE_CAT","TABLE_SCHEM","TABLE_NAME","GRANTOR","GRANTEE","PRIVILEGE","IS_GRANTABLE");
    }

    @Override
    public ResultSet getBestRowIdentifier(String catalog, String schema, String table, int scope, boolean nullable)
            throws SQLException {
        return emptyResultSet("SCOPE","COLUMN_NAME","DATA_TYPE","TYPE_NAME","COLUMN_SIZE","BUFFER_LENGTH","DECIMAL_DIGITS","PSEUDO_COLUMN");
    }

    @Override
    public ResultSet getVersionColumns(String catalog, String schema, String table) throws SQLException {
        return emptyResultSet("SCOPE","COLUMN_NAME","DATA_TYPE","TYPE_NAME","COLUMN_SIZE","BUFFER_LENGTH","DECIMAL_DIGITS","PSEUDO_COLUMN");
    }

    @Override
    public ResultSet getUDTs(String catalog, String schemaPattern, String typeNamePattern, int[] types)
            throws SQLException {
        return emptyResultSet("TYPE_CAT","TYPE_SCHEM","TYPE_NAME","CLASS_NAME","DATA_TYPE","REMARKS","BASE_TYPE");
    }

    @Override
    public ResultSet getSuperTypes(String catalog, String schemaPattern, String typeNamePattern)
            throws SQLException {
        return emptyResultSet("TYPE_CAT","TYPE_SCHEM","TYPE_NAME","SUPERTYPE_CAT","SUPERTYPE_SCHEM","SUPERTYPE_NAME");
    }

    @Override
    public ResultSet getSuperTables(String catalog, String schemaPattern, String tableNamePattern)
            throws SQLException {
        return emptyResultSet("TABLE_CAT","TABLE_SCHEM","TABLE_NAME","SUPERTABLE_NAME");
    }

    @Override
    public ResultSet getAttributes(String catalog, String schemaPattern, String typeNamePattern, String attributeNamePattern)
            throws SQLException {
        return emptyResultSet("TYPE_CAT","TYPE_SCHEM","TYPE_NAME","ATTR_NAME","DATA_TYPE","ATTR_TYPE_NAME",
            "ATTR_SIZE","DECIMAL_DIGITS","NUM_PREC_RADIX","NULLABLE","REMARKS","ATTR_DEF",
            "SQL_DATA_TYPE","SQL_DATETIME_SUB","CHAR_OCTET_LENGTH","ORDINAL_POSITION","IS_NULLABLE","SCOPE_CATALOG","SCOPE_SCHEMA","SCOPE_TABLE","SOURCE_DATA_TYPE");
    }

    @Override
    public ResultSet getClientInfoProperties() throws SQLException {
        return emptyResultSet("NAME","MAX_LEN","DEFAULT_VALUE","DESCRIPTION");
    }

    @Override
    public ResultSet getFunctions(String catalog, String schemaPattern, String functionNamePattern)
            throws SQLException {
        return emptyResultSet("FUNCTION_CAT","FUNCTION_SCHEM","FUNCTION_NAME","REMARKS","FUNCTION_TYPE","SPECIFIC_NAME");
    }

    @Override
    public ResultSet getFunctionColumns(String catalog, String schemaPattern, String functionNamePattern, String columnNamePattern)
            throws SQLException {
        return emptyResultSet("FUNCTION_CAT","FUNCTION_SCHEM","FUNCTION_NAME","COLUMN_NAME","COLUMN_TYPE","DATA_TYPE",
            "TYPE_NAME","PRECISION","LENGTH","SCALE","RADIX","NULLABLE","REMARKS","CHAR_OCTET_LENGTH",
            "ORDINAL_POSITION","IS_NULLABLE","SPECIFIC_NAME");
    }

    @Override
    public ResultSet getPseudoColumns(String catalog, String schemaPattern, String tableNamePattern, String columnNamePattern)
            throws SQLException {
        return emptyResultSet("TABLE_CAT","TABLE_SCHEM","TABLE_NAME","COLUMN_NAME","DATA_TYPE","COLUMN_SIZE",
            "DECIMAL_DIGITS","NUM_PREC_RADIX","COLUMN_USAGE","REMARKS","CHAR_OCTET_LENGTH","IS_NULLABLE");
    }

    @Override
    public boolean generatedKeyAlwaysReturned() { return false; }

    @Override
    public Connection getConnection() throws SQLException { return connection; }

    // ---- Helpers -------------------------------------------------------

    static ResultSet emptyResultSet(String... columns) {
        return buildResultSet(columns, Collections.emptyList());
    }

    static ResultSet buildResultSet(String[] columns, List<Object[]> rows) {
        return new InMemoryResultSet(columns, rows);
    }

    @Override
    public <T> T unwrap(Class<T> iface) throws SQLException {
        if (iface.isAssignableFrom(getClass())) return iface.cast(this);
        throw new SQLException("Cannot unwrap to " + iface.getName());
    }

    @Override
    public boolean isWrapperFor(Class<?> iface) throws SQLException {
        return iface.isAssignableFrom(getClass());
    }

    // ---- InMemoryResultSet for metadata responses ----------------------

    /**
     * Simple in-memory ResultSet backed by a list of Object arrays.
     * Used for DatabaseMetaData method responses.
     */
    private static final class InMemoryResultSet implements ResultSet {
        private final String[] columns;
        private final List<Object[]> rows;
        private int rowIndex = -1;
        private boolean closed = false;
        private boolean wasNull = false;

        InMemoryResultSet(String[] columns, List<Object[]> rows) {
            this.columns = columns;
            this.rows = rows;
        }

        @Override
        public boolean next() {
            if (closed) return false;
            rowIndex++;
            return rowIndex < rows.size();
        }

        @Override
        public void close() { closed = true; }

        @Override
        public boolean isClosed() { return closed; }

        @Override
        public boolean wasNull() { return wasNull; }

        private Object get(int col) throws SQLException {
            if (rowIndex < 0 || rowIndex >= rows.size()) throw new SQLException("No current row", "24000");
            Object[] row = rows.get(rowIndex);
            if (col < 1 || col > row.length) return null;
            Object v = row[col - 1];
            wasNull = (v == null);
            return v;
        }

        private int findCol(String label) throws SQLException {
            for (int i = 0; i < columns.length; i++) {
                if (columns[i].equalsIgnoreCase(label)) return i + 1;
            }
            throw new SQLException("Column not found: " + label, "42S22");
        }

        @Override
        public String getString(int columnIndex) throws SQLException {
            Object v = get(columnIndex);
            return v == null ? null : v.toString();
        }

        @Override
        public String getString(String columnLabel) throws SQLException {
            return getString(findCol(columnLabel));
        }

        @Override
        public boolean getBoolean(int columnIndex) throws SQLException {
            Object v = get(columnIndex);
            if (v == null) return false;
            if (v instanceof Boolean) return (Boolean) v;
            return "true".equalsIgnoreCase(v.toString());
        }

        @Override
        public boolean getBoolean(String columnLabel) throws SQLException { return getBoolean(findCol(columnLabel)); }

        @Override
        public byte getByte(int columnIndex) throws SQLException { return (byte) getLong(columnIndex); }

        @Override
        public byte getByte(String columnLabel) throws SQLException { return getByte(findCol(columnLabel)); }

        @Override
        public short getShort(int columnIndex) throws SQLException { return (short) getLong(columnIndex); }

        @Override
        public short getShort(String columnLabel) throws SQLException { return getShort(findCol(columnLabel)); }

        @Override
        public int getInt(int columnIndex) throws SQLException { return (int) getLong(columnIndex); }

        @Override
        public int getInt(String columnLabel) throws SQLException { return getInt(findCol(columnLabel)); }

        @Override
        public long getLong(int columnIndex) throws SQLException {
            Object v = get(columnIndex);
            if (v == null) return 0L;
            if (v instanceof Number) return ((Number) v).longValue();
            try { return Long.parseLong(v.toString().trim()); } catch (NumberFormatException e) { return 0L; }
        }

        @Override
        public long getLong(String columnLabel) throws SQLException { return getLong(findCol(columnLabel)); }

        @Override
        public float getFloat(int columnIndex) throws SQLException { return (float) getDouble(columnIndex); }

        @Override
        public float getFloat(String columnLabel) throws SQLException { return getFloat(findCol(columnLabel)); }

        @Override
        public double getDouble(int columnIndex) throws SQLException {
            Object v = get(columnIndex);
            if (v == null) return 0.0;
            if (v instanceof Number) return ((Number) v).doubleValue();
            try { return Double.parseDouble(v.toString().trim()); } catch (NumberFormatException e) { return 0.0; }
        }

        @Override
        public double getDouble(String columnLabel) throws SQLException { return getDouble(findCol(columnLabel)); }

        @Override
        public BigDecimal getBigDecimal(int columnIndex, int scale) throws SQLException { return null; }

        @Override
        public BigDecimal getBigDecimal(int columnIndex) throws SQLException { return null; }

        @Override
        public BigDecimal getBigDecimal(String columnLabel) throws SQLException { return null; }

        @Override
        public byte[] getBytes(int columnIndex) throws SQLException { return null; }

        @Override
        public byte[] getBytes(String columnLabel) throws SQLException { return null; }

        @Override
        public java.sql.Date getDate(int columnIndex) throws SQLException { return null; }

        @Override
        public java.sql.Date getDate(String columnLabel) throws SQLException { return null; }

        @Override
        public Time getTime(int columnIndex) throws SQLException { return null; }

        @Override
        public Time getTime(String columnLabel) throws SQLException { return null; }

        @Override
        public Timestamp getTimestamp(int columnIndex) throws SQLException { return null; }

        @Override
        public Timestamp getTimestamp(String columnLabel) throws SQLException { return null; }

        @Override
        public Object getObject(int columnIndex) throws SQLException { return get(columnIndex); }

        @Override
        public Object getObject(String columnLabel) throws SQLException { return get(findCol(columnLabel)); }

        @Override
        public int findColumn(String columnLabel) throws SQLException { return findCol(columnLabel); }

        @Override
        public ResultSetMetaData getMetaData() throws SQLException {
            String[] names = columns;
            int[] types = new int[names.length];
            Arrays.fill(types, DecentDBNative.KIND_TEXT);
            return new DecentDBResultSetMetaData(names, types);
        }

        // Navigation
        @Override
        public boolean isBeforeFirst() { return rowIndex < 0; }
        @Override
        public boolean isAfterLast() { return rowIndex >= rows.size(); }
        @Override
        public boolean isFirst() { return rowIndex == 0; }
        @Override
        public boolean isLast() { return rowIndex == rows.size() - 1; }
        @Override
        public void beforeFirst() { rowIndex = -1; }
        @Override
        public void afterLast() { rowIndex = rows.size(); }
        @Override
        public boolean first() { rowIndex = 0; return !rows.isEmpty(); }
        @Override
        public boolean last() { rowIndex = rows.size() - 1; return !rows.isEmpty(); }
        @Override
        public int getRow() { return rowIndex + 1; }
        @Override
        public boolean absolute(int row) { rowIndex = row - 1; return rowIndex >= 0 && rowIndex < rows.size(); }
        @Override
        public boolean relative(int r) { rowIndex += r; return rowIndex >= 0 && rowIndex < rows.size(); }
        @Override
        public boolean previous() { if (rowIndex > 0) { rowIndex--; return true; } return false; }

        // All unsupported by forward cursor in real result sets; in-memory supports random access
        @Override
        public void setFetchDirection(int dir) {}
        @Override
        public int getFetchDirection() { return ResultSet.FETCH_FORWARD; }
        @Override
        public void setFetchSize(int rows) {}
        @Override
        public int getFetchSize() { return 0; }
        @Override
        public int getType() { return ResultSet.TYPE_SCROLL_INSENSITIVE; }
        @Override
        public int getConcurrency() { return ResultSet.CONCUR_READ_ONLY; }
        @Override
        public int getHoldability() { return ResultSet.CLOSE_CURSORS_AT_COMMIT; }

        @Override
        public boolean rowUpdated() { return false; }
        @Override
        public boolean rowInserted() { return false; }
        @Override
        public boolean rowDeleted() { return false; }

        @Override
        public Statement getStatement() { return null; }
        @Override
        public SQLWarning getWarnings() { return null; }
        @Override
        public void clearWarnings() {}
        @Override
        public String getCursorName() { return null; }

        // Stub update methods
        @Override public void updateNull(int c) throws SQLException { throw Errors.notSupported("update"); }
        @Override public void updateBoolean(int c, boolean x) throws SQLException { throw Errors.notSupported("update"); }
        @Override public void updateByte(int c, byte x) throws SQLException { throw Errors.notSupported("update"); }
        @Override public void updateShort(int c, short x) throws SQLException { throw Errors.notSupported("update"); }
        @Override public void updateInt(int c, int x) throws SQLException { throw Errors.notSupported("update"); }
        @Override public void updateLong(int c, long x) throws SQLException { throw Errors.notSupported("update"); }
        @Override public void updateFloat(int c, float x) throws SQLException { throw Errors.notSupported("update"); }
        @Override public void updateDouble(int c, double x) throws SQLException { throw Errors.notSupported("update"); }
        @Override public void updateBigDecimal(int c, BigDecimal x) throws SQLException { throw Errors.notSupported("update"); }
        @Override public void updateString(int c, String x) throws SQLException { throw Errors.notSupported("update"); }
        @Override public void updateBytes(int c, byte[] x) throws SQLException { throw Errors.notSupported("update"); }
        @Override public void updateDate(int c, java.sql.Date x) throws SQLException { throw Errors.notSupported("update"); }
        @Override public void updateTime(int c, Time x) throws SQLException { throw Errors.notSupported("update"); }
        @Override public void updateTimestamp(int c, Timestamp x) throws SQLException { throw Errors.notSupported("update"); }
        @Override public void updateAsciiStream(int c, java.io.InputStream x, int l) throws SQLException { throw Errors.notSupported("update"); }
        @Override public void updateBinaryStream(int c, java.io.InputStream x, int l) throws SQLException { throw Errors.notSupported("update"); }
        @Override public void updateCharacterStream(int c, java.io.Reader x, int l) throws SQLException { throw Errors.notSupported("update"); }
        @Override public void updateObject(int c, Object x, int s) throws SQLException { throw Errors.notSupported("update"); }
        @Override public void updateObject(int c, Object x) throws SQLException { throw Errors.notSupported("update"); }
        @Override public void updateNull(String l) throws SQLException { throw Errors.notSupported("update"); }
        @Override public void updateBoolean(String l, boolean x) throws SQLException { throw Errors.notSupported("update"); }
        @Override public void updateByte(String l, byte x) throws SQLException { throw Errors.notSupported("update"); }
        @Override public void updateShort(String l, short x) throws SQLException { throw Errors.notSupported("update"); }
        @Override public void updateInt(String l, int x) throws SQLException { throw Errors.notSupported("update"); }
        @Override public void updateLong(String l, long x) throws SQLException { throw Errors.notSupported("update"); }
        @Override public void updateFloat(String l, float x) throws SQLException { throw Errors.notSupported("update"); }
        @Override public void updateDouble(String l, double x) throws SQLException { throw Errors.notSupported("update"); }
        @Override public void updateBigDecimal(String l, BigDecimal x) throws SQLException { throw Errors.notSupported("update"); }
        @Override public void updateString(String l, String x) throws SQLException { throw Errors.notSupported("update"); }
        @Override public void updateBytes(String l, byte[] x) throws SQLException { throw Errors.notSupported("update"); }
        @Override public void updateDate(String l, java.sql.Date x) throws SQLException { throw Errors.notSupported("update"); }
        @Override public void updateTime(String l, Time x) throws SQLException { throw Errors.notSupported("update"); }
        @Override public void updateTimestamp(String l, Timestamp x) throws SQLException { throw Errors.notSupported("update"); }
        @Override public void updateAsciiStream(String l, java.io.InputStream x, int len) throws SQLException { throw Errors.notSupported("update"); }
        @Override public void updateBinaryStream(String l, java.io.InputStream x, int len) throws SQLException { throw Errors.notSupported("update"); }
        @Override public void updateCharacterStream(String l, java.io.Reader x, int len) throws SQLException { throw Errors.notSupported("update"); }
        @Override public void updateObject(String l, Object x, int s) throws SQLException { throw Errors.notSupported("update"); }
        @Override public void updateObject(String l, Object x) throws SQLException { throw Errors.notSupported("update"); }
        @Override public void insertRow() throws SQLException { throw Errors.notSupported("insertRow"); }
        @Override public void updateRow() throws SQLException { throw Errors.notSupported("updateRow"); }
        @Override public void deleteRow() throws SQLException { throw Errors.notSupported("deleteRow"); }
        @Override public void refreshRow() throws SQLException {}
        @Override public void cancelRowUpdates() throws SQLException {}
        @Override public void moveToInsertRow() throws SQLException { throw Errors.notSupported("moveToInsertRow"); }
        @Override public void moveToCurrentRow() throws SQLException {}

        @Override public Object getObject(int c, Map<String,Class<?>> m) throws SQLException { return get(c); }
        @Override public Ref getRef(int c) throws SQLException { throw Errors.notSupported("getRef"); }
        @Override public Blob getBlob(int c) throws SQLException { throw Errors.notSupported("getBlob"); }
        @Override public Clob getClob(int c) throws SQLException { throw Errors.notSupported("getClob"); }
        @Override public Array getArray(int c) throws SQLException { throw Errors.notSupported("getArray"); }
        @Override public Object getObject(String l, Map<String,Class<?>> m) throws SQLException { return get(findCol(l)); }
        @Override public Ref getRef(String l) throws SQLException { throw Errors.notSupported("getRef"); }
        @Override public Blob getBlob(String l) throws SQLException { throw Errors.notSupported("getBlob"); }
        @Override public Clob getClob(String l) throws SQLException { throw Errors.notSupported("getClob"); }
        @Override public Array getArray(String l) throws SQLException { throw Errors.notSupported("getArray"); }
        @Override public java.sql.Date getDate(int c, java.util.Calendar cal) throws SQLException { return getDate(c); }
        @Override public java.sql.Date getDate(String l, java.util.Calendar cal) throws SQLException { return getDate(l); }
        @Override public Time getTime(int c, Calendar cal) throws SQLException { return getTime(c); }
        @Override public Time getTime(String l, Calendar cal) throws SQLException { return getTime(l); }
        @Override public Timestamp getTimestamp(int c, Calendar cal) throws SQLException { return getTimestamp(c); }
        @Override public Timestamp getTimestamp(String l, Calendar cal) throws SQLException { return getTimestamp(l); }
        @Override public java.net.URL getURL(int c) throws SQLException { throw Errors.notSupported("getURL"); }
        @Override public java.net.URL getURL(String l) throws SQLException { throw Errors.notSupported("getURL"); }
        @Override public void updateRef(int c, Ref x) throws SQLException { throw Errors.notSupported("updateRef"); }
        @Override public void updateRef(String l, Ref x) throws SQLException { throw Errors.notSupported("updateRef"); }
        @Override public void updateBlob(int c, Blob x) throws SQLException { throw Errors.notSupported("updateBlob"); }
        @Override public void updateBlob(String l, Blob x) throws SQLException { throw Errors.notSupported("updateBlob"); }
        @Override public void updateClob(int c, Clob x) throws SQLException { throw Errors.notSupported("updateClob"); }
        @Override public void updateClob(String l, Clob x) throws SQLException { throw Errors.notSupported("updateClob"); }
        @Override public void updateArray(int c, Array x) throws SQLException { throw Errors.notSupported("updateArray"); }
        @Override public void updateArray(String l, Array x) throws SQLException { throw Errors.notSupported("updateArray"); }
        @Override public RowId getRowId(int c) throws SQLException { throw Errors.notSupported("getRowId"); }
        @Override public RowId getRowId(String l) throws SQLException { throw Errors.notSupported("getRowId"); }
        @Override public void updateRowId(int c, RowId x) throws SQLException { throw Errors.notSupported("updateRowId"); }
        @Override public void updateRowId(String l, RowId x) throws SQLException { throw Errors.notSupported("updateRowId"); }
        @Override public void updateNString(int c, String s) throws SQLException { throw Errors.notSupported("updateNString"); }
        @Override public void updateNString(String l, String s) throws SQLException { throw Errors.notSupported("updateNString"); }
        @Override public void updateNClob(int c, NClob v) throws SQLException { throw Errors.notSupported("updateNClob"); }
        @Override public void updateNClob(String l, NClob v) throws SQLException { throw Errors.notSupported("updateNClob"); }
        @Override public NClob getNClob(int c) throws SQLException { throw Errors.notSupported("getNClob"); }
        @Override public NClob getNClob(String l) throws SQLException { throw Errors.notSupported("getNClob"); }
        @Override public SQLXML getSQLXML(int c) throws SQLException { throw Errors.notSupported("getSQLXML"); }
        @Override public SQLXML getSQLXML(String l) throws SQLException { throw Errors.notSupported("getSQLXML"); }
        @Override public void updateSQLXML(int c, SQLXML v) throws SQLException { throw Errors.notSupported("updateSQLXML"); }
        @Override public void updateSQLXML(String l, SQLXML v) throws SQLException { throw Errors.notSupported("updateSQLXML"); }
        @Override public String getNString(int c) throws SQLException { return getString(c); }
        @Override public String getNString(String l) throws SQLException { return getString(l); }
        @Override public java.io.Reader getNCharacterStream(int c) throws SQLException { throw Errors.notSupported("getNCharacterStream"); }
        @Override public java.io.Reader getNCharacterStream(String l) throws SQLException { throw Errors.notSupported("getNCharacterStream"); }
        @Override public void updateNCharacterStream(int c, java.io.Reader x, long len) throws SQLException { throw Errors.notSupported("update"); }
        @Override public void updateNCharacterStream(String l, java.io.Reader r, long len) throws SQLException { throw Errors.notSupported("update"); }
        @Override public void updateAsciiStream(int c, java.io.InputStream x, long len) throws SQLException { throw Errors.notSupported("update"); }
        @Override public void updateBinaryStream(int c, java.io.InputStream x, long len) throws SQLException { throw Errors.notSupported("update"); }
        @Override public void updateCharacterStream(int c, java.io.Reader x, long len) throws SQLException { throw Errors.notSupported("update"); }
        @Override public void updateAsciiStream(String l, java.io.InputStream x, long len) throws SQLException { throw Errors.notSupported("update"); }
        @Override public void updateBinaryStream(String l, java.io.InputStream x, long len) throws SQLException { throw Errors.notSupported("update"); }
        @Override public void updateCharacterStream(String l, java.io.Reader r, long len) throws SQLException { throw Errors.notSupported("update"); }
        @Override public void updateBlob(int c, java.io.InputStream s, long len) throws SQLException { throw Errors.notSupported("update"); }
        @Override public void updateBlob(String l, java.io.InputStream s, long len) throws SQLException { throw Errors.notSupported("update"); }
        @Override public void updateClob(int c, java.io.Reader r, long len) throws SQLException { throw Errors.notSupported("update"); }
        @Override public void updateClob(String l, java.io.Reader r, long len) throws SQLException { throw Errors.notSupported("update"); }
        @Override public void updateNClob(int c, java.io.Reader r, long len) throws SQLException { throw Errors.notSupported("update"); }
        @Override public void updateNClob(String l, java.io.Reader r, long len) throws SQLException { throw Errors.notSupported("update"); }
        @Override public void updateNCharacterStream(int c, java.io.Reader x) throws SQLException { throw Errors.notSupported("update"); }
        @Override public void updateNCharacterStream(String l, java.io.Reader r) throws SQLException { throw Errors.notSupported("update"); }
        @Override public void updateAsciiStream(int c, java.io.InputStream x) throws SQLException { throw Errors.notSupported("update"); }
        @Override public void updateBinaryStream(int c, java.io.InputStream x) throws SQLException { throw Errors.notSupported("update"); }
        @Override public void updateCharacterStream(int c, java.io.Reader x) throws SQLException { throw Errors.notSupported("update"); }
        @Override public void updateAsciiStream(String l, java.io.InputStream x) throws SQLException { throw Errors.notSupported("update"); }
        @Override public void updateBinaryStream(String l, java.io.InputStream x) throws SQLException { throw Errors.notSupported("update"); }
        @Override public void updateCharacterStream(String l, java.io.Reader r) throws SQLException { throw Errors.notSupported("update"); }
        @Override public void updateBlob(int c, java.io.InputStream s) throws SQLException { throw Errors.notSupported("update"); }
        @Override public void updateBlob(String l, java.io.InputStream s) throws SQLException { throw Errors.notSupported("update"); }
        @Override public void updateClob(int c, java.io.Reader r) throws SQLException { throw Errors.notSupported("update"); }
        @Override public void updateClob(String l, java.io.Reader r) throws SQLException { throw Errors.notSupported("update"); }
        @Override public void updateNClob(int c, java.io.Reader r) throws SQLException { throw Errors.notSupported("update"); }
        @Override public void updateNClob(String l, java.io.Reader r) throws SQLException { throw Errors.notSupported("update"); }
        @Override public <T> T getObject(int c, Class<T> type) throws SQLException { return type.cast(get(c)); }
        @Override public <T> T getObject(String l, Class<T> type) throws SQLException { return type.cast(get(findCol(l))); }
        @Override public java.io.InputStream getAsciiStream(int c) throws SQLException { throw Errors.notSupported("getAsciiStream"); }
        @Override public java.io.InputStream getUnicodeStream(int c) throws SQLException { throw Errors.notSupported("getUnicodeStream"); }
        @Override public java.io.InputStream getBinaryStream(int c) throws SQLException { throw Errors.notSupported("getBinaryStream"); }
        @Override public java.io.Reader getCharacterStream(int c) throws SQLException { throw Errors.notSupported("getCharacterStream"); }
        @Override public java.io.Reader getCharacterStream(String l) throws SQLException { throw Errors.notSupported("getCharacterStream"); }
        @Override public java.io.InputStream getAsciiStream(String l) throws SQLException { throw Errors.notSupported("getAsciiStream"); }
        @Override public java.io.InputStream getUnicodeStream(String l) throws SQLException { throw Errors.notSupported("getUnicodeStream"); }
        @Override public java.io.InputStream getBinaryStream(String l) throws SQLException { throw Errors.notSupported("getBinaryStream"); }
        @Override public java.math.BigDecimal getBigDecimal(String l, int scale) throws SQLException { return null; }

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
}
