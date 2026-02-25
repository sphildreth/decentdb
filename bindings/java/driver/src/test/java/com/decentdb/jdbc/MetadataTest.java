package com.decentdb.jdbc;

import org.junit.jupiter.api.*;
import org.junit.jupiter.api.condition.EnabledIfSystemProperty;

import java.io.File;
import java.sql.*;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Integration tests for DatabaseMetaData: getTables, getColumns, getPrimaryKeys,
 * getImportedKeys, getExportedKeys, and getIndexInfo.
 *
 * These tests require the native library and create a temp database with:
 * - A 'departments' table with a PK
 * - An 'employees' table with a FK referencing 'departments'
 * - A unique index on employees.email
 */
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
class MetadataTest {

    private static File tempDb;
    private static Connection connection;
    private static boolean nativeAvailable = false;

    @BeforeAll
    static void setUp() {
        try {
            NativeLibLoader.ensureLoaded();
            nativeAvailable = true;
        } catch (UnsatisfiedLinkError e) {
            System.err.println("Skipping metadata tests: native library not available");
            return;
        }

        try {
            tempDb = File.createTempFile("decentdb_meta_test_", ".ddb");
            tempDb.deleteOnExit();
            new File(tempDb.getAbsolutePath() + "-wal").deleteOnExit();

            connection = DriverManager.getConnection("jdbc:decentdb:" + tempDb.getAbsolutePath());

            // Create schema
            try (Statement s = connection.createStatement()) {
                s.execute("CREATE TABLE departments (" +
                    "dept_id INTEGER PRIMARY KEY, " +
                    "dept_name TEXT NOT NULL)");

                s.execute("CREATE TABLE employees (" +
                    "emp_id INTEGER PRIMARY KEY, " +
                    "emp_name TEXT NOT NULL, " +
                    "email TEXT UNIQUE, " +
                    "dept_id INTEGER REFERENCES departments(dept_id) ON DELETE CASCADE)");

                s.execute("CREATE INDEX idx_emp_name ON employees(emp_name)");
            }
        } catch (Exception e) {
            nativeAvailable = false;
            System.err.println("Failed to set up metadata test: " + e.getMessage());
        }
    }

    @AfterAll
    static void tearDown() throws Exception {
        if (connection != null && !connection.isClosed()) {
            connection.close();
        }
    }

    private void assumeNative() {
        org.junit.jupiter.api.Assumptions.assumeTrue(nativeAvailable,
            "Native library not available; skipping metadata test");
    }

    // ---- DatabaseMetaData product info ----------------------------------

    @Test
    @Order(1)
    void productName() throws Exception {
        assumeNative();
        DatabaseMetaData meta = connection.getMetaData();
        assertEquals("DecentDB", meta.getDatabaseProductName());
        // Must NOT pretend to be PostgreSQL
        assertNotEquals("PostgreSQL", meta.getDatabaseProductName());
    }

    @Test
    @Order(2)
    void driverInfo() throws Exception {
        assumeNative();
        DatabaseMetaData meta = connection.getMetaData();
        assertEquals("DecentDB JDBC Driver", meta.getDriverName());
        assertEquals(1, meta.getDriverMajorVersion());
    }

    // ---- getTables -------------------------------------------------------

    @Test
    @Order(10)
    void getTablesReturnsAllTables() throws Exception {
        assumeNative();
        DatabaseMetaData meta = connection.getMetaData();
        try (ResultSet rs = meta.getTables(null, null, null, new String[]{"TABLE"})) {
            int count = 0;
            boolean foundDepts = false, foundEmps = false;
            while (rs.next()) {
                String name = rs.getString("TABLE_NAME");
                if ("departments".equalsIgnoreCase(name)) foundDepts = true;
                if ("employees".equalsIgnoreCase(name)) foundEmps = true;
                count++;
                assertEquals("TABLE", rs.getString("TABLE_TYPE"));
            }
            assertTrue(foundDepts, "departments table should be listed");
            assertTrue(foundEmps, "employees table should be listed");
            assertTrue(count >= 2);
        }
    }

    @Test
    @Order(11)
    void getTablesByPattern() throws Exception {
        assumeNative();
        DatabaseMetaData meta = connection.getMetaData();
        try (ResultSet rs = meta.getTables(null, null, "departments", new String[]{"TABLE"})) {
            assertTrue(rs.next());
            assertEquals("departments", rs.getString("TABLE_NAME").toLowerCase());
            assertFalse(rs.next());
        }
    }

    // ---- getColumns -------------------------------------------------------

    @Test
    @Order(20)
    void getColumnsForDepartments() throws Exception {
        assumeNative();
        DatabaseMetaData meta = connection.getMetaData();
        try (ResultSet rs = meta.getColumns(null, null, "departments", null)) {
            int count = 0;
            boolean foundDeptId = false, foundDeptName = false;
            while (rs.next()) {
                String col = rs.getString("COLUMN_NAME");
                if ("dept_id".equalsIgnoreCase(col)) foundDeptId = true;
                if ("dept_name".equalsIgnoreCase(col)) foundDeptName = true;
                count++;
                // Verify required JDBC columns are present
                assertNotNull(rs.getString("TABLE_NAME"));
                assertNotNull(rs.getString("COLUMN_NAME"));
                assertTrue(rs.getInt("ORDINAL_POSITION") >= 1);
            }
            assertTrue(foundDeptId, "dept_id column should be present");
            assertTrue(foundDeptName, "dept_name column should be present");
            assertEquals(2, count);
        }
    }

    // ---- getPrimaryKeys --------------------------------------------------

    @Test
    @Order(30)
    void getPrimaryKeys() throws Exception {
        assumeNative();
        DatabaseMetaData meta = connection.getMetaData();
        try (ResultSet rs = meta.getPrimaryKeys(null, null, "departments")) {
            assertTrue(rs.next(), "Should have at least one PK column");
            assertEquals("departments", rs.getString("TABLE_NAME").toLowerCase());
            assertEquals("dept_id", rs.getString("COLUMN_NAME").toLowerCase());
            assertEquals(1, rs.getInt("KEY_SEQ"));
            assertFalse(rs.next());
        }
    }

    @Test
    @Order(31)
    void getPrimaryKeysForEmployees() throws Exception {
        assumeNative();
        DatabaseMetaData meta = connection.getMetaData();
        try (ResultSet rs = meta.getPrimaryKeys(null, null, "employees")) {
            assertTrue(rs.next());
            assertEquals("emp_id", rs.getString("COLUMN_NAME").toLowerCase());
        }
    }

    // ---- getImportedKeys (FK metadata) -----------------------------------

    @Test
    @Order(40)
    void getImportedKeysForEmployees() throws Exception {
        assumeNative();
        DatabaseMetaData meta = connection.getMetaData();
        try (ResultSet rs = meta.getImportedKeys(null, null, "employees")) {
            assertTrue(rs.next(), "employees should have at least one FK");

            // FK: employees.dept_id → departments.dept_id
            String pkTable = rs.getString("PKTABLE_NAME");
            String pkCol = rs.getString("PKCOLUMN_NAME");
            String fkTable = rs.getString("FKTABLE_NAME");
            String fkCol = rs.getString("FKCOLUMN_NAME");

            assertEquals("departments", pkTable.toLowerCase());
            assertEquals("dept_id", pkCol.toLowerCase());
            assertEquals("employees", fkTable.toLowerCase());
            assertEquals("dept_id", fkCol.toLowerCase());

            // Verify DELETE_RULE is CASCADE
            int deleteRule = rs.getInt("DELETE_RULE");
            assertEquals(DatabaseMetaData.importedKeyCascade, deleteRule);

            assertFalse(rs.next());
        }
    }

    @Test
    @Order(41)
    void getExportedKeysForDepartments() throws Exception {
        assumeNative();
        DatabaseMetaData meta = connection.getMetaData();
        try (ResultSet rs = meta.getExportedKeys(null, null, "departments")) {
            assertTrue(rs.next(), "departments should be referenced by at least one FK");

            String pkTable = rs.getString("PKTABLE_NAME");
            String fkTable = rs.getString("FKTABLE_NAME");

            assertEquals("departments", pkTable.toLowerCase());
            assertEquals("employees", fkTable.toLowerCase());
        }
    }

    @Test
    @Order(42)
    void getCrossReference() throws Exception {
        assumeNative();
        DatabaseMetaData meta = connection.getMetaData();
        try (ResultSet rs = meta.getCrossReference(null, null, "departments", null, null, "employees")) {
            assertTrue(rs.next(), "Should find FK from employees to departments");
            assertEquals("departments", rs.getString("PKTABLE_NAME").toLowerCase());
            assertEquals("employees", rs.getString("FKTABLE_NAME").toLowerCase());
        }
    }

    // ---- getIndexInfo ----------------------------------------------------

    @Test
    @Order(50)
    void getIndexInfo() throws Exception {
        assumeNative();
        DatabaseMetaData meta = connection.getMetaData();
        try (ResultSet rs = meta.getIndexInfo(null, null, "employees", false, true)) {
            boolean foundEmailIdx = false;
            boolean foundNameIdx = false;
            while (rs.next()) {
                String idxName = rs.getString("INDEX_NAME");
                if (idxName != null && idxName.toLowerCase().contains("email")) {
                    foundEmailIdx = true;
                    // email should be unique
                    assertFalse(rs.getBoolean("NON_UNIQUE"), "email index should be unique");
                }
                if (idxName != null && idxName.equalsIgnoreCase("idx_emp_name")) {
                    foundNameIdx = true;
                }
            }
            assertTrue(foundEmailIdx || foundNameIdx,
                "Should find at least one index on employees");
        }
    }

    // ---- getTableTypes / getTypeInfo -------------------------------------

    @Test
    @Order(60)
    void getTableTypes() throws Exception {
        assumeNative();
        DatabaseMetaData meta = connection.getMetaData();
        try (ResultSet rs = meta.getTableTypes()) {
            boolean hasTable = false;
            while (rs.next()) {
                if ("TABLE".equals(rs.getString("TABLE_TYPE"))) hasTable = true;
            }
            assertTrue(hasTable);
        }
    }

    @Test
    @Order(61)
    void getTypeInfo() throws Exception {
        assumeNative();
        DatabaseMetaData meta = connection.getMetaData();
        try (ResultSet rs = meta.getTypeInfo()) {
            assertTrue(rs.next(), "Should return at least one type");
        }
    }

    // ---- Feature flags --------------------------------------------------

    @Test
    @Order(70)
    void supportsTransactions() throws Exception {
        assumeNative();
        assertTrue(connection.getMetaData().supportsTransactions());
    }

    @Test
    @Order(71)
    void supportsSavepoints() throws Exception {
        assumeNative();
        assertTrue(connection.getMetaData().supportsSavepoints());
    }

    @Test
    @Order(72)
    void doesNotSupportSerializable() throws Exception {
        assumeNative();
        assertFalse(connection.getMetaData().supportsTransactionIsolationLevel(
            Connection.TRANSACTION_SERIALIZABLE));
    }

    @Test
    @Order(73)
    void doesNotSupportStoredProcedures() throws Exception {
        assumeNative();
        assertFalse(connection.getMetaData().supportsStoredProcedures());
    }
}
