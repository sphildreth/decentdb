package com.decentdb.jdbc;

import org.junit.jupiter.api.*;
import org.junit.jupiter.api.condition.EnabledIfSystemProperty;

import java.io.File;
import java.sql.*;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Integration tests for the DecentDB JDBC driver.
 *
 * These tests require the native library to be available. They are skipped
 * automatically if the native library cannot be loaded (CI without native build).
 *
 * To run: set system property {@code decentdb.native.lib.dir} to the directory
 * containing libdecentdb_jni.so (or equivalent), or set the environment variable
 * {@code DECENTDB_NATIVE_LIB} to the full path.
 *
 * Run via:
 *   gradle :driver:test -PnativeLibDir=/path/to/build
 */
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
class IntegrationTest {

    private static File tempDb;
    private static Connection connection;
    private static boolean nativeAvailable = false;

    @BeforeAll
    static void setUp() {
        try {
            NativeLibLoader.ensureLoaded();
            nativeAvailable = true;
        } catch (UnsatisfiedLinkError e) {
            System.err.println("Skipping integration tests: native library not available: " + e.getMessage());
            return;
        }

        try {
            tempDb = File.createTempFile("decentdb_test_", ".ddb");
            tempDb.deleteOnExit();
            // Also delete WAL file
            new File(tempDb.getAbsolutePath() + "-wal").deleteOnExit();

            String url = "jdbc:decentdb:" + tempDb.getAbsolutePath();
            connection = DriverManager.getConnection(url);
            assertNotNull(connection, "Connection should not be null");
        } catch (Exception e) {
            nativeAvailable = false;
            System.err.println("Failed to set up integration test: " + e.getMessage());
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
            "Native library not available; skipping integration test");
    }

    // ---- Basic connectivity -----------------------------------------------

    @Test
    @Order(1)
    void selectOne() throws Exception {
        assumeNative();
        try (Statement s = connection.createStatement();
             ResultSet rs = s.executeQuery("SELECT 1")) {
            assertTrue(rs.next());
            assertEquals(1, rs.getInt(1));
            assertFalse(rs.next());
        }
    }

    @Test
    @Order(2)
    void connectionNotClosed() throws Exception {
        assumeNative();
        assertFalse(connection.isClosed());
    }

    @Test
    @Order(3)
    void isValid() throws Exception {
        assumeNative();
        assertTrue(connection.isValid(5));
    }

    // ---- DDL / DML round-trip --------------------------------------------

    @Test
    @Order(10)
    void createTable() throws Exception {
        assumeNative();
        try (Statement s = connection.createStatement()) {
            s.execute("CREATE TABLE IF NOT EXISTS users (" +
                "id INTEGER PRIMARY KEY, " +
                "name TEXT NOT NULL, " +
                "email TEXT, " +
                "score REAL)");
        }
        // Verify table exists in metadata
        DatabaseMetaData meta = connection.getMetaData();
        try (ResultSet rs = meta.getTables(null, null, "users", new String[]{"TABLE"})) {
            assertTrue(rs.next(), "Table 'users' should exist");
            assertEquals("users", rs.getString("TABLE_NAME"));
        }
    }

    @Test
    @Order(11)
    void insertRows() throws Exception {
        assumeNative();
        try (Statement s = connection.createStatement()) {
            int rows = s.executeUpdate("INSERT INTO users (id, name, email, score) VALUES (1, 'Alice', 'alice@example.com', 9.5)");
            assertEquals(1, rows);
            rows = s.executeUpdate("INSERT INTO users (id, name, email, score) VALUES (2, 'Bob', 'bob@example.com', 7.0)");
            assertEquals(1, rows);
        }
    }

    @Test
    @Order(12)
    void selectRows() throws Exception {
        assumeNative();
        try (Statement s = connection.createStatement();
             ResultSet rs = s.executeQuery("SELECT id, name, email, score FROM users ORDER BY id")) {
            assertTrue(rs.next());
            assertEquals(1, rs.getLong("id"));
            assertEquals("Alice", rs.getString("name"));
            assertEquals("alice@example.com", rs.getString("email"));
            assertEquals(9.5, rs.getDouble("score"), 0.001);

            assertTrue(rs.next());
            assertEquals(2, rs.getLong("id"));
            assertEquals("Bob", rs.getString("name"));

            assertFalse(rs.next());
        }
    }

    @Test
    @Order(13)
    void updateRows() throws Exception {
        assumeNative();
        try (Statement s = connection.createStatement()) {
            int rows = s.executeUpdate("UPDATE users SET score = 8.0 WHERE id = 2");
            assertEquals(1, rows);
        }
        // Verify update
        try (Statement s = connection.createStatement();
             ResultSet rs = s.executeQuery("SELECT score FROM users WHERE id = 2")) {
            assertTrue(rs.next());
            assertEquals(8.0, rs.getDouble(1), 0.001);
        }
    }

    @Test
    @Order(14)
    void deleteRows() throws Exception {
        assumeNative();
        try (Statement s = connection.createStatement()) {
            int rows = s.executeUpdate("DELETE FROM users WHERE id = 2");
            assertEquals(1, rows);
        }
        try (Statement s = connection.createStatement();
             ResultSet rs = s.executeQuery("SELECT COUNT(*) FROM users")) {
            assertTrue(rs.next());
            assertEquals(1, rs.getLong(1));
        }
    }

    // ---- PreparedStatement -----------------------------------------------

    @Test
    @Order(20)
    void preparedStatementInsert() throws Exception {
        assumeNative();
        try (PreparedStatement ps = connection.prepareStatement(
                "INSERT INTO users (id, name, email, score) VALUES ($1, $2, $3, $4)")) {
            ps.setLong(1, 10);
            ps.setString(2, "Charlie");
            ps.setString(3, "charlie@example.com");
            ps.setDouble(4, 6.5);
            int rows = ps.executeUpdate();
            assertEquals(1, rows);
        }
    }

    @Test
    @Order(21)
    void preparedStatementSelect() throws Exception {
        assumeNative();
        try (PreparedStatement ps = connection.prepareStatement(
                "SELECT name FROM users WHERE id = $1")) {
            ps.setLong(1, 10);
            try (ResultSet rs = ps.executeQuery()) {
                assertTrue(rs.next());
                assertEquals("Charlie", rs.getString(1));
                assertFalse(rs.next());
            }
        }
    }

    @Test
    @Order(22)
    void preparedStatementNullBinding() throws Exception {
        assumeNative();
        try (PreparedStatement ps = connection.prepareStatement(
                "INSERT INTO users (id, name, email, score) VALUES ($1, $2, $3, $4)")) {
            ps.setLong(1, 20);
            ps.setString(2, "Dana");
            ps.setNull(3, java.sql.Types.VARCHAR);
            ps.setNull(4, java.sql.Types.DOUBLE);
            int rows = ps.executeUpdate();
            assertEquals(1, rows);
        }
        // Verify nulls
        try (Statement s = connection.createStatement();
             ResultSet rs = s.executeQuery("SELECT email, score FROM users WHERE id = 20")) {
            assertTrue(rs.next());
            assertNull(rs.getString("email"));
            assertTrue(rs.wasNull());
        }
    }

    // ---- ResultSetMetaData -----------------------------------------------

    @Test
    @Order(30)
    void resultSetMetaData() throws Exception {
        assumeNative();
        try (Statement s = connection.createStatement();
             ResultSet rs = s.executeQuery("SELECT id, name, score FROM users WHERE id = 1")) {
            ResultSetMetaData meta = rs.getMetaData();
            assertEquals(3, meta.getColumnCount());
            assertEquals("id", meta.getColumnName(1).toLowerCase());
            assertEquals("name", meta.getColumnName(2).toLowerCase());
            assertEquals("score", meta.getColumnName(3).toLowerCase());
            assertEquals(java.sql.Types.BIGINT, meta.getColumnType(1));
            assertEquals(java.sql.Types.VARCHAR, meta.getColumnType(2));
        }
    }

    // ---- UUID formatting / BLOB display -----------------------------------

    @Test
    @Order(40)
    void uuidBlobFormatsAsUuidString() throws Exception {
        assumeNative();

        UUID u = UUID.fromString("00112233-4455-6677-8899-aabbccddeeff");
        byte[] bytes = uuidToBytes(u);

        try (Statement s = connection.createStatement()) {
            s.execute("CREATE TABLE IF NOT EXISTS uuid_test (id INTEGER PRIMARY KEY, u UUID NOT NULL)");
            s.execute("DELETE FROM uuid_test");
        }

        try (PreparedStatement ps = connection.prepareStatement(
                "INSERT INTO uuid_test (id, u) VALUES ($1, $2)")) {
            ps.setLong(1, 1);
            ps.setBytes(2, bytes);
            assertEquals(1, ps.executeUpdate());
        }

        try (Statement s = connection.createStatement();
             ResultSet rs = s.executeQuery("SELECT u FROM uuid_test WHERE id = 1")) {
            assertTrue(rs.next());
            assertEquals(u.toString(), rs.getString(1));
            Object obj = rs.getObject(1);
            assertTrue(obj instanceof UUID, "Expected getObject() to return UUID for 16-byte UUID blob");
            assertEquals(u, obj);
            assertArrayEquals(bytes, rs.getBytes(1));
        }
    }

    private static byte[] uuidToBytes(UUID u) {
        long msb = u.getMostSignificantBits();
        long lsb = u.getLeastSignificantBits();
        byte[] out = new byte[16];
        for (int i = 0; i < 8; i++) {
            out[i] = (byte) (msb >>> (56 - (i * 8)));
        }
        for (int i = 0; i < 8; i++) {
            out[8 + i] = (byte) (lsb >>> (56 - (i * 8)));
        }
        return out;
    }

    // ---- Transaction semantics -------------------------------------------

    @Test
    @Order(40)
    void commitAndRollback() throws Exception {
        assumeNative();
        connection.setAutoCommit(false);
        try {
            try (Statement s = connection.createStatement()) {
                s.executeUpdate("INSERT INTO users (id, name) VALUES (100, 'TxUser')");
            }
            connection.rollback();

            // Verify insert was rolled back
            try (Statement s = connection.createStatement();
                 ResultSet rs = s.executeQuery("SELECT COUNT(*) FROM users WHERE id = 100")) {
                assertTrue(rs.next());
                assertEquals(0, rs.getLong(1));
            }

            // Now commit
            try (Statement s = connection.createStatement()) {
                s.executeUpdate("INSERT INTO users (id, name) VALUES (100, 'TxUser')");
            }
            connection.commit();

            try (Statement s = connection.createStatement();
                 ResultSet rs = s.executeQuery("SELECT COUNT(*) FROM users WHERE id = 100")) {
                assertTrue(rs.next());
                assertEquals(1, rs.getLong(1));
            }
        } finally {
            connection.setAutoCommit(true);
        }
    }

    @Test
    @Order(41)
    void savepoints() throws Exception {
        assumeNative();
        connection.setAutoCommit(false);
        try {
            try (Statement s = connection.createStatement()) {
                s.executeUpdate("INSERT INTO users (id, name) VALUES (200, 'SpUser')");
            }
            Savepoint sp = connection.setSavepoint("sp1");
            try (Statement s = connection.createStatement()) {
                s.executeUpdate("INSERT INTO users (id, name) VALUES (201, 'SpUser2')");
            }
            connection.rollback(sp);
            connection.commit();

            // 200 committed, 201 rolled back to savepoint
            try (Statement s = connection.createStatement();
                 ResultSet rs = s.executeQuery("SELECT id FROM users WHERE id IN (200, 201) ORDER BY id")) {
                assertTrue(rs.next());
                assertEquals(200, rs.getLong(1));
                assertFalse(rs.next());
            }
        } finally {
            connection.setAutoCommit(true);
        }
    }

    // ---- Isolation level -------------------------------------------------

    @Test
    @Order(50)
    void repeatedReadIsolation() throws Exception {
        assumeNative();
        // Default isolation is TRANSACTION_REPEATABLE_READ
        assertEquals(Connection.TRANSACTION_REPEATABLE_READ, connection.getTransactionIsolation());
    }

    @Test
    @Order(51)
    void serializableIsolationRejected() {
        assumeNative();
        assertThrows(SQLFeatureNotSupportedException.class,
            () -> connection.setTransactionIsolation(Connection.TRANSACTION_SERIALIZABLE));
    }

    @Test
    @Order(52)
    void readUncommittedRejected() {
        assumeNative();
        assertThrows(SQLFeatureNotSupportedException.class,
            () -> connection.setTransactionIsolation(Connection.TRANSACTION_READ_UNCOMMITTED));
    }
}
