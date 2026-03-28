package com.decentdb.jdbc;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

class MemoryLeakTest {

    private static boolean nativeAvailable = false;

    @BeforeAll
    static void setUp() {
        try {
            NativeLibLoader.ensureLoaded();
            nativeAvailable = true;
        } catch (UnsatisfiedLinkError e) {
            nativeAvailable = false;
        }
    }

    @Test
    void repeatedOpenQueryCloseKeepsRssBounded() throws Exception {
        assumeTrue(nativeAvailable, "Native library not available; skipping memory regression");
        assumeTrue(System.getProperty("os.name", "").toLowerCase().contains("linux"),
            "RSS regression is Linux-only");

        File tempDb = File.createTempFile("decentdb_memory_leak_", ".ddb");
        tempDb.deleteOnExit();
        new File(tempDb.getAbsolutePath() + "-wal").deleteOnExit();
        String url = "jdbc:decentdb:" + tempDb.getAbsolutePath();

        try {
            try (Connection connection = DriverManager.getConnection(url);
                 Statement statement = connection.createStatement()) {
                statement.execute("CREATE TABLE leak_probe (id INTEGER PRIMARY KEY, payload TEXT)");
                statement.executeUpdate("INSERT INTO leak_probe (id, payload) VALUES (1, 'probe')");
            }

            for (int i = 0; i < 25; i++) {
                runLeakIteration(url);
            }

            trimJvmHeap();
            long before = readRssBytes();

            for (int i = 0; i < 160; i++) {
                runLeakIteration(url);
                if (i % 10 == 0) {
                    trimJvmHeap();
                }
            }

            trimJvmHeap();
            long after = readRssBytes();
            long diff = after - before;

            assertTrue(
                diff < 20L * 1024 * 1024,
                () -> "RSS grew by " + diff + " bytes (before=" + before + ", after=" + after + ")"
            );
        } finally {
            Files.deleteIfExists(tempDb.toPath());
            Files.deleteIfExists(Path.of(tempDb.getAbsolutePath() + "-wal"));
        }
    }

    private static void runLeakIteration(String url) throws Exception {
        try (Connection connection = DriverManager.getConnection(url);
             Statement statement = connection.createStatement();
             ResultSet rs = statement.executeQuery("SELECT COUNT(*) FROM leak_probe")) {
            assertTrue(rs.next());
            assertEquals(1, rs.getInt(1));
        }
    }

    private static void trimJvmHeap() throws InterruptedException {
        for (int i = 0; i < 3; i++) {
            System.gc();
            Thread.sleep(10);
        }
    }

    private static long readRssBytes() throws Exception {
        for (String line : Files.readAllLines(Path.of("/proc/self/status"))) {
            if (!line.startsWith("VmRSS:")) {
                continue;
            }
            String[] parts = line.trim().split("\\s+");
            if (parts.length < 3) {
                break;
            }
            return Long.parseLong(parts[1]) * 1024L;
        }
        throw new IllegalStateException("VmRSS not found in /proc/self/status");
    }
}
