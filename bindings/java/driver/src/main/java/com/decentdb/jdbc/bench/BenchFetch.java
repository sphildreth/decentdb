package com.decentdb.jdbc.bench;

import com.decentdb.jdbc.NativeLibLoader;

import java.io.File;
import java.net.URL;
import java.net.URLClassLoader;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.Driver;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Random;

public final class BenchFetch {
    private static final int DEFAULT_COUNT = 1_000_000;
    private static final int DEFAULT_POINT_READS = 10_000;
    private static final int DEFAULT_FETCHMANY_BATCH = 4_096;
    private static final int DEFAULT_POINT_SEED = 1337;
    private static final List<ClassLoader> SQLITE_DRIVER_LOADERS = new ArrayList<>();

    private BenchFetch() {
    }

    public static void main(String[] args) throws Exception {
        final Options options = Options.parse(args);
        if (options.showHelp) {
            printUsage();
            return;
        }

        final List<String> engines = options.engine.equals("all")
            ? List.of("decentdb", "sqlite")
            : List.of(options.engine);
        final Map<String, BenchResult> results = new LinkedHashMap<>();

        for (String engine : engines) {
            final String suffix = engine.equals("sqlite") ? "db" : "ddb";
            final String dbPath = options.dbPrefix + "_" + engine + "." + suffix;
            results.put(engine, runEngineBenchmark(engine, dbPath, options));
        }

        printComparison(results);
    }

    private static BenchResult runEngineBenchmark(String engine, String dbPath, Options options) throws Exception {
        cleanupDbFiles(dbPath);
        System.out.println();
        System.out.println("=== " + engine + " ===");
        System.out.println("Setting up data...");

        if (engine.equals("decentdb")) {
            NativeLibLoader.ensureLoaded();
            Class.forName("com.decentdb.jdbc.DecentDBDriver");
        } else if (engine.equals("sqlite")) {
            ensureSqliteDriverLoaded(options.sqliteJdbcPath);
        } else {
            throw new IllegalArgumentException("Unknown engine: " + engine);
        }

        final String url = engine.equals("sqlite")
            ? "jdbc:sqlite:" + dbPath
            : "jdbc:decentdb:" + dbPath;
        try (Connection conn = DriverManager.getConnection(url)) {
            conn.setAutoCommit(true);
            setupSchema(conn, engine);

            final String insertSql = engine.equals("sqlite")
                ? "INSERT INTO bench VALUES (?, ?, ?)"
                : "INSERT INTO bench VALUES ($1, $2, $3)";
            final String pointSql = engine.equals("sqlite")
                ? "SELECT id, val, f FROM bench WHERE id = ?"
                : "SELECT id, val, f FROM bench WHERE id = $1";
            final String scanSql = "SELECT id, val, f FROM bench";

            warmInsertPath(conn, insertSql);

            final double insertSeconds = timedSeconds(() -> {
                final boolean oldAutoCommit = conn.getAutoCommit();
                conn.setAutoCommit(false);
                try (PreparedStatement stmt = conn.prepareStatement(insertSql)) {
                    for (int i = 0; i < options.count; i++) {
                        stmt.setLong(1, i);
                        stmt.setString(2, "value_" + i);
                        stmt.setDouble(3, i);
                        stmt.executeUpdate();
                    }
                    conn.commit();
                } catch (Throwable t) {
                    try {
                        conn.rollback();
                    } catch (SQLException ignored) {
                    }
                    throw t;
                } finally {
                    conn.setAutoCommit(oldAutoCommit);
                }
            });
            final double insertRowsPerSecond = options.count / insertSeconds;
            System.out.printf(Locale.ROOT, "Insert %,d rows: %.4fs (%,.2f rows/sec)%n",
                options.count, insertSeconds, insertRowsPerSecond);

            try (PreparedStatement scanStmt = conn.prepareStatement(scanSql)) {
                warmScanPath(scanStmt);

                final HolderDouble fetchallSeconds = new HolderDouble();
                timedSeconds(() -> {
                    final List<BenchRow> rows = new ArrayList<>(options.count);
                    try (ResultSet rs = scanStmt.executeQuery()) {
                        while (rs.next()) {
                            rows.add(new BenchRow(rs.getLong(1), rs.getString(2), rs.getDouble(3)));
                        }
                    }
                    if (rows.size() != options.count) {
                        throw new IllegalStateException(
                            "Expected " + options.count + " rows from fetchall, got " + rows.size()
                        );
                    }
                }, fetchallSeconds);
                System.out.printf(Locale.ROOT, "Fetchall %,d rows: %.4fs%n", options.count, fetchallSeconds.value);

                final HolderDouble fetchmanySeconds = new HolderDouble();
                timedSeconds(() -> {
                    int total = 0;
                    int batchCount = 0;
                    try (ResultSet rs = scanStmt.executeQuery()) {
                        while (rs.next()) {
                            rs.getLong(1);
                            rs.getString(2);
                            rs.getDouble(3);
                            batchCount++;
                            if (batchCount == options.fetchmanyBatch) {
                                total += batchCount;
                                batchCount = 0;
                            }
                        }
                    }
                    total += batchCount;
                    if (total != options.count) {
                        throw new IllegalStateException(
                            "Expected " + options.count + " rows from fetchmany, got " + total
                        );
                    }
                }, fetchmanySeconds);
                System.out.printf(Locale.ROOT, "Fetchmany(%,d) %,d rows: %.4fs%n",
                    options.fetchmanyBatch, options.count, fetchmanySeconds.value);

                try (PreparedStatement pointStmt = conn.prepareStatement(pointSql)) {
                    final long[] pointIds = buildPointReadIds(options.count, options.pointReads, options.pointSeed);
                    final long warmupId = pointIds[pointIds.length / 2];
                    pointStmt.setLong(1, warmupId);
                    try (ResultSet rs = pointStmt.executeQuery()) {
                        if (!rs.next()) {
                            throw new IllegalStateException("Warmup point read missed expected row");
                        }
                        rs.getLong(1);
                        rs.getString(2);
                        rs.getDouble(3);
                    }

                    final double[] latenciesMs = new double[pointIds.length];
                    for (int i = 0; i < pointIds.length; i++) {
                        final long startedNs = System.nanoTime();
                        pointStmt.setLong(1, pointIds[i]);
                        try (ResultSet rs = pointStmt.executeQuery()) {
                            if (!rs.next()) {
                                throw new IllegalStateException("Point read missed id=" + pointIds[i]);
                            }
                            rs.getLong(1);
                            rs.getString(2);
                            rs.getDouble(3);
                        }
                        latenciesMs[i] = (System.nanoTime() - startedNs) / 1_000_000.0;
                    }
                    Arrays.sort(latenciesMs);
                    final double pointP50Ms = percentileSorted(latenciesMs, 50);
                    final double pointP95Ms = percentileSorted(latenciesMs, 95);
                    System.out.printf(Locale.ROOT,
                        "Random point reads by id (%,d, seed=%d): p50=%.6fms p95=%.6fms%n",
                        options.pointReads, options.pointSeed, pointP50Ms, pointP95Ms);

                    if (engine.equals("sqlite")) {
                        try (Statement stmt = conn.createStatement()) {
                            stmt.execute("PRAGMA wal_checkpoint(TRUNCATE)");
                        }
                    }

                    if (!options.keepDb) {
                        cleanupDbFiles(dbPath);
                    }
                    return new BenchResult(
                        insertSeconds,
                        insertRowsPerSecond,
                        fetchallSeconds.value,
                        fetchmanySeconds.value,
                        pointP50Ms,
                        pointP95Ms
                    );
                }
            }
        }
    }

    private static void setupSchema(Connection conn, String engine) throws SQLException {
        try (Statement stmt = conn.createStatement()) {
            if (engine.equals("sqlite")) {
                stmt.execute("PRAGMA journal_mode=WAL");
                stmt.execute("PRAGMA synchronous=FULL");
                stmt.execute("PRAGMA wal_autocheckpoint=0");
            }
            if (engine.equals("sqlite")) {
                stmt.execute("CREATE TABLE bench (id INTEGER, val TEXT, f REAL)");
            } else {
                stmt.execute("CREATE TABLE bench (id INT64, val TEXT, f FLOAT64)");
            }
            stmt.execute("CREATE INDEX bench_id_idx ON bench(id)");
        }
    }

    private static void warmInsertPath(Connection conn, String insertSql) throws SQLException {
        final boolean oldAutoCommit = conn.getAutoCommit();
        conn.setAutoCommit(false);
        try (PreparedStatement stmt = conn.prepareStatement(insertSql)) {
            stmt.setLong(1, -1L);
            stmt.setString(2, "__warm__");
            stmt.setDouble(3, -1.0);
            stmt.executeUpdate();
            conn.rollback();
        } finally {
            conn.setAutoCommit(oldAutoCommit);
        }
    }

    private static void warmScanPath(PreparedStatement scanStmt) throws SQLException {
        try (ResultSet rs = scanStmt.executeQuery()) {
            if (rs.next()) {
                rs.getLong(1);
                rs.getString(2);
                rs.getDouble(3);
            }
        }
    }

    private static long[] buildPointReadIds(int rowCount, int pointReads, long seed) {
        final Random random = new Random(seed);
        if (pointReads <= rowCount) {
            final long[] ids = new long[rowCount];
            for (int i = 0; i < rowCount; i++) {
                ids[i] = i;
            }
            for (int i = 0; i < pointReads; i++) {
                final int j = i + random.nextInt(rowCount - i);
                final long tmp = ids[i];
                ids[i] = ids[j];
                ids[j] = tmp;
            }
            return Arrays.copyOf(ids, pointReads);
        }

        final long[] out = new long[pointReads];
        for (int i = 0; i < pointReads; i++) {
            out[i] = random.nextInt(rowCount);
        }
        return out;
    }

    private static double percentileSorted(double[] sortedValues, int pct) {
        if (sortedValues.length == 0) {
            return 0.0;
        }
        int idx = (int) Math.round((pct / 100.0) * (sortedValues.length - 1));
        idx = Math.max(0, Math.min(sortedValues.length - 1, idx));
        return sortedValues[idx];
    }

    private static void printComparison(Map<String, BenchResult> results) {
        final BenchResult decent = results.get("decentdb");
        final BenchResult sqlite = results.get("sqlite");
        if (decent == null || sqlite == null) {
            return;
        }

        final List<Metric> metrics = List.of(
            new Metric("Insert throughput (higher is better)", decent.insertRowsPerSecond, sqlite.insertRowsPerSecond, " rows/s", true, "%.2f"),
            new Metric("Fetchall time (lower is better)", decent.fetchallSeconds, sqlite.fetchallSeconds, "s", false, "%.6f"),
            new Metric("Fetchmany/streaming time (lower is better)", decent.fetchmanySeconds, sqlite.fetchmanySeconds, "s", false, "%.6f"),
            new Metric("Point read p50 latency (lower is better)", decent.pointP50Ms, sqlite.pointP50Ms, "ms", false, "%.6f"),
            new Metric("Point read p95 latency (lower is better)", decent.pointP95Ms, sqlite.pointP95Ms, "ms", false, "%.6f")
        );

        final List<String> decentBetter = new ArrayList<>();
        final List<String> sqliteBetter = new ArrayList<>();
        final List<String> ties = new ArrayList<>();

        for (Metric metric : metrics) {
            if (metric.decent == metric.sqlite) {
                ties.add(metric.name + ": tie (" + String.format(Locale.ROOT, metric.format, metric.decent) + metric.unit + ")");
                continue;
            }

            final boolean decentWins;
            final double winner;
            final double loser;
            final double ratio;
            final String detail;

            if (metric.higherIsBetter) {
                decentWins = metric.decent > metric.sqlite;
                winner = decentWins ? metric.decent : metric.sqlite;
                loser = decentWins ? metric.sqlite : metric.decent;
                ratio = loser == 0.0 ? Double.POSITIVE_INFINITY : winner / loser;
                detail = metric.name + ": "
                    + String.format(Locale.ROOT, metric.format, winner) + metric.unit + " vs "
                    + String.format(Locale.ROOT, metric.format, loser) + metric.unit
                    + " (" + String.format(Locale.ROOT, "%.3f", ratio) + "x higher)";
            } else {
                decentWins = metric.decent < metric.sqlite;
                winner = decentWins ? metric.decent : metric.sqlite;
                loser = decentWins ? metric.sqlite : metric.decent;
                ratio = winner == 0.0 ? Double.POSITIVE_INFINITY : loser / winner;
                detail = metric.name + ": "
                    + String.format(Locale.ROOT, metric.format, winner) + metric.unit + " vs "
                    + String.format(Locale.ROOT, metric.format, loser) + metric.unit
                    + " (" + String.format(Locale.ROOT, "%.3f", ratio) + "x faster/lower)";
            }

            if (decentWins) {
                decentBetter.add(detail);
            } else {
                sqliteBetter.add(detail);
            }
        }

        System.out.println();
        System.out.println("=== Comparison (DecentDB vs SQLite) ===");
        System.out.println("DecentDB better at:");
        if (decentBetter.isEmpty()) {
            System.out.println("- none");
        } else {
            for (String line : decentBetter) {
                System.out.println("- " + line);
            }
        }

        System.out.println("SQLite better at:");
        if (sqliteBetter.isEmpty()) {
            System.out.println("- none");
        } else {
            for (String line : sqliteBetter) {
                System.out.println("- " + line);
            }
        }

        if (!ties.isEmpty()) {
            System.out.println("Ties:");
            for (String line : ties) {
                System.out.println("- " + line);
            }
        }
    }

    private static void cleanupDbFiles(String basePath) {
        deleteQuietly(basePath);
        deleteQuietly(basePath + ".wal");
        deleteQuietly(basePath + "-wal");
        deleteQuietly(basePath + "-shm");
    }

    private static void deleteQuietly(String path) {
        try {
            new File(path).delete();
        } catch (Throwable ignored) {
        }
    }

    private static void ensureSqliteDriverLoaded(String sqliteJdbcPath) throws Exception {
        if (hasSqliteDriver()) {
            return;
        }

        final List<String> candidates = new ArrayList<>();
        if (sqliteJdbcPath != null && !sqliteJdbcPath.isBlank()) {
            candidates.add(sqliteJdbcPath);
        }
        final String fromEnv = System.getenv("SQLITE_JDBC_JAR");
        if (fromEnv != null && !fromEnv.isBlank()) {
            candidates.add(fromEnv);
        }
        candidates.addAll(defaultSqliteJdbcCandidates());

        for (String candidate : candidates) {
            if (candidate == null || candidate.isBlank()) {
                continue;
            }
            final File jar = new File(candidate);
            if (!jar.isFile()) {
                continue;
            }
            loadSqliteJdbcFromJar(jar);
            if (hasSqliteDriver()) {
                System.out.println("SQLite JDBC driver: " + jar.getAbsolutePath());
                return;
            }
        }

        throw new IllegalStateException(
            "SQLite JDBC driver not found. Pass --sqlite-jdbc /path/to/sqlite-jdbc.jar or set SQLITE_JDBC_JAR."
        );
    }

    private static List<String> defaultSqliteJdbcCandidates() {
        final String home = System.getProperty("user.home", "");
        final List<String> out = new ArrayList<>();
        out.add(home + "/.local/share/DBeaverData/drivers/maven/maven-central/org.xerial/sqlite-jdbc-3.51.2.0.jar");
        out.add(home + "/.config/JetBrains/Rider2025.3/jdbc-drivers/Xerial SQLiteJDBC/3.45.1/org/xerial/sqlite-jdbc/3.45.1.0/sqlite-jdbc-3.45.1.0.jar");
        out.add(home + "/.config/JetBrains/Rider2025.2/jdbc-drivers/Xerial SQLiteJDBC/3.45.1/org/xerial/sqlite-jdbc/3.45.1.0/sqlite-jdbc-3.45.1.0.jar");

        final Path m2Base = Path.of(home, ".m2", "repository", "org", "xerial", "sqlite-jdbc");
        final File m2Dir = m2Base.toFile();
        if (m2Dir.isDirectory()) {
            final File[] children = m2Dir.listFiles();
            if (children != null) {
                for (File versionDir : children) {
                    final File[] jars = versionDir.listFiles((dir, name) -> name.endsWith(".jar") && name.contains("sqlite-jdbc"));
                    if (jars != null) {
                        for (File jar : jars) {
                            out.add(jar.getAbsolutePath());
                        }
                    }
                }
            }
        }
        return out;
    }

    private static boolean hasSqliteDriver() {
        try (Connection conn = DriverManager.getConnection("jdbc:sqlite::memory:")) {
            return conn != null;
        } catch (SQLException ignored) {
            return false;
        }
    }

    private static void loadSqliteJdbcFromJar(File sqliteJdbcJar) throws Exception {
        final URL jarUrl = sqliteJdbcJar.toURI().toURL();
        final URLClassLoader loader = new URLClassLoader(new URL[]{jarUrl}, BenchFetch.class.getClassLoader());
        SQLITE_DRIVER_LOADERS.add(loader);
        final Class<?> driverClass = Class.forName("org.sqlite.JDBC", true, loader);
        final Driver sqliteDriver = (Driver) driverClass.getDeclaredConstructor().newInstance();
        DriverManager.registerDriver(new DriverShim(sqliteDriver));
    }

    private static double timedSeconds(CheckedRunnable runnable) throws Exception {
        final long started = System.nanoTime();
        runnable.run();
        return (System.nanoTime() - started) / 1_000_000_000.0;
    }

    private static void timedSeconds(CheckedRunnable runnable, HolderDouble out) throws Exception {
        out.value = timedSeconds(runnable);
    }

    private static void printUsage() {
        System.out.println("Fair Java benchmark: DecentDB JDBC vs SQLite JDBC");
        System.out.println("Usage:");
        System.out.println("  ./gradlew :driver:benchmarkFetch -PbenchmarkArgs=\"[options]\"");
        System.out.println();
        System.out.println("Options:");
        System.out.println("  --engine <all|decentdb|sqlite>   Engine(s) to run (default: all)");
        System.out.println("  --count <n>                      Rows to insert/fetch (default: " + DEFAULT_COUNT + ")");
        System.out.println("  --fetchmany-batch <n>            Batch size for fetchmany benchmark (default: " + DEFAULT_FETCHMANY_BATCH + ")");
        System.out.println("  --point-reads <n>                Random indexed point lookups (default: " + DEFAULT_POINT_READS + ")");
        System.out.println("  --point-seed <n>                 RNG seed for point lookups (default: " + DEFAULT_POINT_SEED + ")");
        System.out.println("  --db-prefix <path_prefix>        Database prefix (default: java_bench_fetch)");
        System.out.println("                                   DecentDB uses .ddb and SQLite uses .db");
        System.out.println("  --sqlite-jdbc <jar_path>         Optional sqlite-jdbc jar path");
        System.out.println("  --keep-db                        Keep generated DB files");
        System.out.println("  -h, --help                       Show help");
    }

    private static final class BenchRow {
        final long id;
        final String val;
        final double f;

        BenchRow(long id, String val, double f) {
            this.id = id;
            this.val = val;
            this.f = f;
        }
    }

    private static final class BenchResult {
        final double insertSeconds;
        final double insertRowsPerSecond;
        final double fetchallSeconds;
        final double fetchmanySeconds;
        final double pointP50Ms;
        final double pointP95Ms;

        BenchResult(
            double insertSeconds,
            double insertRowsPerSecond,
            double fetchallSeconds,
            double fetchmanySeconds,
            double pointP50Ms,
            double pointP95Ms
        ) {
            this.insertSeconds = insertSeconds;
            this.insertRowsPerSecond = insertRowsPerSecond;
            this.fetchallSeconds = fetchallSeconds;
            this.fetchmanySeconds = fetchmanySeconds;
            this.pointP50Ms = pointP50Ms;
            this.pointP95Ms = pointP95Ms;
        }
    }

    private static final class Metric {
        final String name;
        final double decent;
        final double sqlite;
        final String unit;
        final boolean higherIsBetter;
        final String format;

        Metric(String name, double decent, double sqlite, String unit, boolean higherIsBetter, String format) {
            this.name = name;
            this.decent = decent;
            this.sqlite = sqlite;
            this.unit = unit;
            this.higherIsBetter = higherIsBetter;
            this.format = format;
        }
    }

    private static final class HolderDouble {
        double value;
    }

    @FunctionalInterface
    private interface CheckedRunnable {
        void run() throws Exception;
    }

    private static final class DriverShim implements Driver {
        private final Driver delegate;

        DriverShim(Driver delegate) {
            this.delegate = delegate;
        }

        @Override
        public Connection connect(String url, java.util.Properties info) throws SQLException {
            return delegate.connect(url, info);
        }

        @Override
        public boolean acceptsURL(String url) throws SQLException {
            return delegate.acceptsURL(url);
        }

        @Override
        public java.sql.DriverPropertyInfo[] getPropertyInfo(String url, java.util.Properties info) throws SQLException {
            return delegate.getPropertyInfo(url, info);
        }

        @Override
        public int getMajorVersion() {
            return delegate.getMajorVersion();
        }

        @Override
        public int getMinorVersion() {
            return delegate.getMinorVersion();
        }

        @Override
        public boolean jdbcCompliant() {
            return delegate.jdbcCompliant();
        }

        @Override
        public java.util.logging.Logger getParentLogger() throws java.sql.SQLFeatureNotSupportedException {
            return delegate.getParentLogger();
        }
    }

    private static final class Options {
        String engine = "all";
        int count = DEFAULT_COUNT;
        int fetchmanyBatch = DEFAULT_FETCHMANY_BATCH;
        int pointReads = DEFAULT_POINT_READS;
        int pointSeed = DEFAULT_POINT_SEED;
        String dbPrefix = "java_bench_fetch";
        String sqliteJdbcPath = "";
        boolean keepDb = false;
        boolean showHelp = false;

        static Options parse(String[] args) {
            final Options options = new Options();
            for (int i = 0; i < args.length; i++) {
                final String arg = args[i];
                switch (arg) {
                    case "--help":
                    case "-h":
                        options.showHelp = true;
                        break;
                    case "--engine":
                        options.engine = nextArg(args, ++i, "--engine");
                        break;
                    case "--count":
                        options.count = parsePositiveInt(nextArg(args, ++i, "--count"), "--count");
                        break;
                    case "--fetchmany-batch":
                        options.fetchmanyBatch = parsePositiveInt(nextArg(args, ++i, "--fetchmany-batch"), "--fetchmany-batch");
                        break;
                    case "--point-reads":
                        options.pointReads = parsePositiveInt(nextArg(args, ++i, "--point-reads"), "--point-reads");
                        break;
                    case "--point-seed":
                        options.pointSeed = Integer.parseInt(nextArg(args, ++i, "--point-seed"));
                        break;
                    case "--db-prefix":
                        options.dbPrefix = nextArg(args, ++i, "--db-prefix");
                        break;
                    case "--sqlite-jdbc":
                        options.sqliteJdbcPath = nextArg(args, ++i, "--sqlite-jdbc");
                        break;
                    case "--keep-db":
                        options.keepDb = true;
                        break;
                    default:
                        throw new IllegalArgumentException("Unknown argument: " + arg);
                }
            }

            if (!options.engine.equals("all") && !options.engine.equals("decentdb") && !options.engine.equals("sqlite")) {
                throw new IllegalArgumentException("--engine must be one of: all, decentdb, sqlite");
            }
            if (options.dbPrefix == null || options.dbPrefix.isBlank()) {
                throw new IllegalArgumentException("--db-prefix cannot be empty");
            }
            return options;
        }

        private static String nextArg(String[] args, int index, String name) {
            if (index >= args.length) {
                throw new IllegalArgumentException(name + " requires a value");
            }
            return args[index];
        }

        private static int parsePositiveInt(String value, String name) {
            final int parsed = Integer.parseInt(value);
            if (parsed <= 0) {
                throw new IllegalArgumentException(name + " must be > 0");
            }
            return parsed;
        }
    }
}
