package com.decentdb.jdbc;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.Properties;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Unit tests for DecentDB JDBC URL parsing.
 * These tests do NOT require a native library.
 */
class DriverUrlParseTest {

    @Test
    void acceptsDecentdbUrls() throws Exception {
        DecentDBDriver driver = new DecentDBDriver();
        assertTrue(driver.acceptsURL("jdbc:decentdb:/path/to/db.ddb"));
        assertTrue(driver.acceptsURL("jdbc:decentdb:/absolute/path.ddb"));
        assertTrue(driver.acceptsURL("jdbc:decentdb:/path/to/db.ddb?readOnly=true"));
    }

    @Test
    void rejectsOtherUrls() throws Exception {
        DecentDBDriver driver = new DecentDBDriver();
        assertFalse(driver.acceptsURL("jdbc:sqlite:/path.db"));
        assertFalse(driver.acceptsURL("jdbc:postgresql://localhost/db"));
        assertFalse(driver.acceptsURL("jdbc:mysql://localhost/db"));
        assertFalse(driver.acceptsURL(null));
    }

    @Test
    void parsesSimpleUrl() {
        DecentDBDriver.ParsedUrl parsed = DecentDBDriver.parseUrl("jdbc:decentdb:/path/to/db.ddb");
        assertNotNull(parsed);
        assertEquals("/path/to/db.ddb", parsed.filePath);
        assertFalse(parsed.readOnly);
        assertEquals(0, parsed.busyTimeoutMs);
        assertEquals(0, parsed.cachePages);
    }

    @Test
    void parsesReadOnlyParam() {
        DecentDBDriver.ParsedUrl parsed = DecentDBDriver.parseUrl("jdbc:decentdb:/path/db.ddb?readOnly=true");
        assertNotNull(parsed);
        assertTrue(parsed.readOnly);
    }

    @Test
    void parsesReadOnlyFalse() {
        DecentDBDriver.ParsedUrl parsed = DecentDBDriver.parseUrl("jdbc:decentdb:/path/db.ddb?readOnly=false");
        assertNotNull(parsed);
        assertFalse(parsed.readOnly);
    }

    @Test
    void parsesBusyTimeoutMs() {
        DecentDBDriver.ParsedUrl parsed = DecentDBDriver.parseUrl("jdbc:decentdb:/path/db.ddb?busyTimeoutMs=5000");
        assertNotNull(parsed);
        assertEquals(5000, parsed.busyTimeoutMs);
    }

    @Test
    void parsesCachePages() {
        DecentDBDriver.ParsedUrl parsed = DecentDBDriver.parseUrl("jdbc:decentdb:/path/db.ddb?cachePages=2048");
        assertNotNull(parsed);
        assertEquals(2048, parsed.cachePages);
    }

    @Test
    void parsesMultipleParams() {
        DecentDBDriver.ParsedUrl parsed = DecentDBDriver.parseUrl(
            "jdbc:decentdb:/path/db.ddb?readOnly=true&busyTimeoutMs=3000&cachePages=512");
        assertNotNull(parsed);
        assertTrue(parsed.readOnly);
        assertEquals(3000, parsed.busyTimeoutMs);
        assertEquals(512, parsed.cachePages);
    }

    @Test
    void returnsNullForNonDecentdbUrl() {
        assertNull(DecentDBDriver.parseUrl("jdbc:sqlite:/path.db"));
        assertNull(DecentDBDriver.parseUrl(null));
    }

    @Test
    void returnsNullForEmptyPath() {
        assertNull(DecentDBDriver.parseUrl("jdbc:decentdb:"));
    }

    @Test
    void driverMajorVersion() {
        DecentDBDriver driver = new DecentDBDriver();
        assertEquals(DecentDBDriver.DRIVER_MAJOR_VERSION, driver.getMajorVersion());
    }

    @Test
    void driverMinorVersion() {
        DecentDBDriver driver = new DecentDBDriver();
        assertEquals(DecentDBDriver.DRIVER_MINOR_VERSION, driver.getMinorVersion());
    }

    @Test
    void jdbcNotCompliant() {
        DecentDBDriver driver = new DecentDBDriver();
        // Honest: we don't claim full JDBC compliance
        assertFalse(driver.jdbcCompliant());
    }

    @Test
    void getPropertyInfo() throws Exception {
        DecentDBDriver driver = new DecentDBDriver();
        var infos = driver.getPropertyInfo("jdbc:decentdb:/path/db.ddb", new Properties());
        assertNotNull(infos);
        assertTrue(infos.length >= 3);
        // Verify expected properties are present
        boolean hasReadOnly = false;
        for (var info : infos) {
            if ("readOnly".equals(info.name)) hasReadOnly = true;
        }
        assertTrue(hasReadOnly, "Should have readOnly property");
    }
}
