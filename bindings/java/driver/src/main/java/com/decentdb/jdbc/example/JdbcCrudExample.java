package com.decentdb.jdbc.example;

import com.decentdb.jdbc.DecentDBConnection;
import com.decentdb.jdbc.DecentDBDataSource;

import java.math.BigDecimal;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Timestamp;
import java.time.Instant;

/**
 * Standalone JDBC example covering schema creation, CRUD, transactions, metadata,
 * common DecentDB data types, and basic error handling.
 */
public final class JdbcCrudExample {
    private JdbcCrudExample() {
    }

    public static void main(String[] args) throws Exception {
        final Path dbPath = args.length > 0
            ? Path.of(args[0]).toAbsolutePath()
            : Files.createTempFile("decentdb_jdbc_example_", ".ddb");
        dbPath.toFile().deleteOnExit();
        new java.io.File(dbPath + "-wal").deleteOnExit();

        final String url = "jdbc:decentdb:" + dbPath;
        final DecentDBDataSource dataSource = new DecentDBDataSource(url);
        dataSource.setMode("openOrCreate");

        System.out.println("Using database: " + dbPath);

        try (Connection connection = dataSource.getConnection()) {
            connection.setAutoCommit(false);

            try (Statement statement = connection.createStatement()) {
                statement.executeUpdate("DROP TABLE IF EXISTS products");
                statement.executeUpdate(
                    "CREATE TABLE products (" +
                        "id INT64 PRIMARY KEY, " +
                        "name TEXT NOT NULL, " +
                        "price DECIMAL(12,2) NOT NULL, " +
                        "active BOOL NOT NULL, " +
                        "updated_at TIMESTAMP" +
                        ")"
                );
            }

            try (PreparedStatement insert = connection.prepareStatement(
                "INSERT INTO products (id, name, price, active, updated_at) VALUES ($1, $2, $3, $4, $5)"
            )) {
                insert.setLong(1, 1L);
                insert.setString(2, "Mechanical Keyboard");
                insert.setBigDecimal(3, new BigDecimal("129.99"));
                insert.setBoolean(4, true);
                insert.setTimestamp(5, Timestamp.from(Instant.parse("2026-03-24T12:00:00.123456Z")));
                insert.executeUpdate();

                insert.setLong(1, 2L);
                insert.setString(2, "Vertical Mouse");
                insert.setBigDecimal(3, new BigDecimal("79.50"));
                insert.setBoolean(4, true);
                insert.setTimestamp(5, Timestamp.from(Instant.parse("2026-03-24T12:01:00.654321Z")));
                insert.executeUpdate();
            }

            connection.commit();

            System.out.println("Inserted rows:");
            try (PreparedStatement query = connection.prepareStatement(
                "SELECT id, name, price, active, updated_at FROM products ORDER BY id"
            );
                 ResultSet resultSet = query.executeQuery()) {
                while (resultSet.next()) {
                    System.out.printf(
                        "  #%d %s price=%s active=%s updated_at=%s%n",
                        resultSet.getLong("id"),
                        resultSet.getString("name"),
                        resultSet.getBigDecimal("price"),
                        resultSet.getBoolean("active"),
                        resultSet.getTimestamp("updated_at")
                    );
                }
            }

            connection.setAutoCommit(false);
            try (PreparedStatement update = connection.prepareStatement(
                "UPDATE products SET price = $1 WHERE id = $2"
            )) {
                update.setBigDecimal(1, new BigDecimal("149.99"));
                update.setLong(2, 1L);
                update.executeUpdate();
            }
            connection.rollback();

            try (PreparedStatement count = connection.prepareStatement(
                "SELECT COUNT(*) FROM products WHERE price = $1"
            )) {
                count.setBigDecimal(1, new BigDecimal("149.99"));
                try (ResultSet resultSet = count.executeQuery()) {
                    resultSet.next();
                    System.out.println("Rows with rolled-back price=149.99: " + resultSet.getLong(1));
                }
            }
            connection.setAutoCommit(true);

            try (PreparedStatement duplicate = connection.prepareStatement(
                "INSERT INTO products (id, name, price, active, updated_at) VALUES ($1, $2, $3, $4, $5)"
            )) {
                duplicate.setLong(1, 1L);
                duplicate.setString(2, "Duplicate");
                duplicate.setBigDecimal(3, new BigDecimal("1.00"));
                duplicate.setBoolean(4, false);
                duplicate.setTimestamp(5, Timestamp.from(Instant.now()));
                duplicate.executeUpdate();
            } catch (SQLException ex) {
                System.out.println("Expected duplicate-key error: SQLState=" + ex.getSQLState());
            }

            final DatabaseMetaData metaData = connection.getMetaData();
            System.out.println("Tables visible through DatabaseMetaData:");
            try (ResultSet tables = metaData.getTables(null, null, "%", new String[]{"TABLE"})) {
                while (tables.next()) {
                    System.out.println("  " + tables.getString("TABLE_NAME"));
                }
            }

            if (connection instanceof DecentDBConnection decent) {
                System.out.println("Engine version: " + decent.getEngineVersion());
                System.out.println("ABI version: " + decent.getAbiVersion());
                decent.checkpoint();
            }
        }
    }
}
