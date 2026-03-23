package com.decentdb.jdbc;

import java.sql.*;
import java.util.Properties;
import java.util.logging.Logger;

/**
 * JDBC Driver for DecentDB.
 *
 * <h3>URL format</h3>
 * <pre>
 *   jdbc:decentdb:/absolute/path/to/db.ddb
 *   jdbc:decentdb:/path/to/db.ddb?readOnly=true&busyTimeoutMs=5000
 * </pre>
 *
 * <h3>Supported connection properties</h3>
 * <ul>
 *   <li>{@code readOnly} - open database in read-only mode (boolean, default false)</li>
 *   <li>{@code busyTimeoutMs} - milliseconds to wait on a busy write lock (integer)</li>
 *   <li>{@code cachePages} - page cache size (integer)</li>
 * </ul>
 *
 * <h3>Registration</h3>
 * The driver registers itself via {@link java.util.ServiceLoader} (META-INF/services/java.sql.Driver).
 * It is also registered statically in this class initializer for compatibility.
 */
public final class DecentDBDriver implements Driver {

    public static final String URL_PREFIX = "jdbc:decentdb:";
    public static final String DRIVER_VERSION = "1.8.1";
    public static final int DRIVER_MAJOR_VERSION = 1;
    public static final int DRIVER_MINOR_VERSION = 8;

    private static final Logger LOG = Logger.getLogger(DecentDBDriver.class.getName());

    static {
        try {
            DriverManager.registerDriver(new DecentDBDriver());
        } catch (SQLException e) {
            LOG.warning("Failed to auto-register DecentDBDriver: " + e.getMessage());
        }
    }

    @Override
    public Connection connect(String url, Properties info) throws SQLException {
        if (!acceptsURL(url)) return null;
        ParsedUrl parsed = ParsedUrl.parse(url);
        if (parsed == null) {
            throw Errors.connection("Invalid DecentDB URL: " + url);
        }

        // Merge URL query params and Properties; Properties take precedence
        boolean readOnly = boolProp(info, "readOnly", parsed.readOnly);
        int busyTimeoutMs = intProp(info, "busyTimeoutMs", parsed.busyTimeoutMs);
        int cachePages = intProp(info, "cachePages", parsed.cachePages);

        StringBuilder opts = new StringBuilder();
        if (cachePages > 0) appendOpt(opts, "cache_pages=" + cachePages);
        if (busyTimeoutMs > 0) appendOpt(opts, "busy_timeout_ms=" + busyTimeoutMs);

        NativeLibLoader.ensureLoaded();
        long dbHandle = DecentDBNative.dbOpen(parsed.filePath, opts.toString());
        if (dbHandle == 0) {
            // Try to get global error
            String msg = DecentDBNative.dbLastErrorMessage(0);
            throw Errors.connection("Failed to open DecentDB database '" + parsed.filePath + "'" +
                (msg != null && !msg.isEmpty() ? ": " + msg : ""));
        }

        return new DecentDBConnection(dbHandle, url, readOnly);
    }

    @Override
    public boolean acceptsURL(String url) {
        return url != null && url.startsWith(URL_PREFIX);
    }

    @Override
    public DriverPropertyInfo[] getPropertyInfo(String url, Properties info) {
        return new DriverPropertyInfo[]{
            new DriverPropertyInfo("readOnly",
                info != null ? info.getProperty("readOnly", "false") : "false"),
            new DriverPropertyInfo("busyTimeoutMs",
                info != null ? info.getProperty("busyTimeoutMs", "0") : "0"),
            new DriverPropertyInfo("cachePages",
                info != null ? info.getProperty("cachePages", "0") : "0"),
        };
    }

    @Override
    public int getMajorVersion() { return DRIVER_MAJOR_VERSION; }

    @Override
    public int getMinorVersion() { return DRIVER_MINOR_VERSION; }

    @Override
    public boolean jdbcCompliant() { return false; }

    @Override
    public Logger getParentLogger() throws SQLFeatureNotSupportedException {
        return Logger.getLogger("com.decentdb.jdbc");
    }

    // ---- URL parsing ---------------------------------------------------

    /**
     * Parses a DecentDB JDBC URL into its components.
     * Format: {@code jdbc:decentdb:/path/to/db[?key=value&...]}
     */
    static ParsedUrl parseUrl(String url) {
        return ParsedUrl.parse(url);
    }

    static final class ParsedUrl {
        final String filePath;
        final boolean readOnly;
        final int busyTimeoutMs;
        final int cachePages;

        ParsedUrl(String filePath, boolean readOnly, int busyTimeoutMs, int cachePages) {
            this.filePath = filePath;
            this.readOnly = readOnly;
            this.busyTimeoutMs = busyTimeoutMs;
            this.cachePages = cachePages;
        }

        static ParsedUrl parse(String url) {
            if (url == null || !url.startsWith(URL_PREFIX)) return null;
            String rest = url.substring(URL_PREFIX.length());
            // rest is now: /path/to/db.ddb or /path/to/db.ddb?key=value
            String filePath;
            String query = "";
            int qIdx = rest.indexOf('?');
            if (qIdx >= 0) {
                filePath = rest.substring(0, qIdx);
                query = rest.substring(qIdx + 1);
            } else {
                filePath = rest;
            }
            if (filePath.isEmpty()) return null;

            boolean readOnly = false;
            int busyTimeoutMs = 0;
            int cachePages = 0;

            for (String part : query.split("&")) {
                if (part.isEmpty()) continue;
                int eq = part.indexOf('=');
                if (eq < 0) continue;
                String k = part.substring(0, eq).trim().toLowerCase();
                String v = part.substring(eq + 1).trim();
                switch (k) {
                    case "readonly":
                        readOnly = "true".equalsIgnoreCase(v) || "1".equals(v);
                        break;
                    case "busytimeoutms":
                        try { busyTimeoutMs = Integer.parseInt(v); } catch (NumberFormatException ignored) {}
                        break;
                    case "cachepages":
                        try { cachePages = Integer.parseInt(v); } catch (NumberFormatException ignored) {}
                        break;
                }
            }
            return new ParsedUrl(filePath, readOnly, busyTimeoutMs, cachePages);
        }
    }

    private static boolean boolProp(Properties props, String key, boolean defaultVal) {
        if (props == null) return defaultVal;
        String v = props.getProperty(key);
        if (v == null) return defaultVal;
        return "true".equalsIgnoreCase(v) || "1".equals(v);
    }

    private static int intProp(Properties props, String key, int defaultVal) {
        if (props == null) return defaultVal;
        String v = props.getProperty(key);
        if (v == null) return defaultVal;
        try { return Integer.parseInt(v); } catch (NumberFormatException e) { return defaultVal; }
    }

    private static void appendOpt(StringBuilder sb, String kv) {
        if (sb.length() > 0) sb.append('&');
        sb.append(kv);
    }
}
