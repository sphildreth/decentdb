import java.lang.foreign.Arena;
import java.lang.foreign.FunctionDescriptor;
import java.lang.foreign.Linker;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.SymbolLookup;
import java.lang.invoke.MethodHandle;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;

import static java.lang.foreign.ValueLayout.ADDRESS;
import static java.lang.foreign.ValueLayout.JAVA_INT;
import static java.lang.foreign.ValueLayout.JAVA_LONG;

public final class Smoke {
    private static final int DDB_OK = 0;
    private static final int DDB_ERR_SQL = 5;
    private static final int DDB_ERR_TIMEOUT = 10;
    private static final long DDB_WRITE_QUEUE_TIMEOUT_DEFAULT = -1L;

    public static void main(String[] args) throws Throwable {
        Path root = Path.of("").toAbsolutePath();
        Path library = locateLibrary(root);

        try (Arena arena = Arena.ofConfined()) {
            SymbolLookup lookup = SymbolLookup.libraryLookup(library, arena);
            Linker linker = Linker.nativeLinker();

            MethodHandle lastError = linker.downcallHandle(
                lookup.find("ddb_last_error_message").orElseThrow(),
                FunctionDescriptor.of(ADDRESS));
            MethodHandle lastErrorJson = linker.downcallHandle(
                lookup.find("ddb_last_error_json").orElseThrow(),
                FunctionDescriptor.of(JAVA_INT, ADDRESS));
            MethodHandle open = linker.downcallHandle(
                lookup.find("ddb_db_open_or_create").orElseThrow(),
                FunctionDescriptor.of(JAVA_INT, ADDRESS, ADDRESS));
            MethodHandle execute = linker.downcallHandle(
                lookup.find("ddb_db_execute").orElseThrow(),
                FunctionDescriptor.of(JAVA_INT, ADDRESS, ADDRESS, ADDRESS, JAVA_LONG, ADDRESS));
            MethodHandle executeQueued = linker.downcallHandle(
                lookup.find("ddb_db_execute_queued").orElseThrow(),
                FunctionDescriptor.of(JAVA_INT, ADDRESS, ADDRESS, ADDRESS, JAVA_LONG, JAVA_LONG, ADDRESS));
            MethodHandle queueMetrics = linker.downcallHandle(
                lookup.find("ddb_db_write_queue_metrics").orElseThrow(),
                FunctionDescriptor.of(JAVA_INT, ADDRESS, ADDRESS));
            MethodHandle watchQuery = linker.downcallHandle(
                lookup.find("ddb_db_watch_query_json").orElseThrow(),
                FunctionDescriptor.of(JAVA_INT, ADDRESS, ADDRESS, ADDRESS));
            MethodHandle watchNext = linker.downcallHandle(
                lookup.find("ddb_watch_next_json").orElseThrow(),
                FunctionDescriptor.of(JAVA_INT, ADDRESS, JAVA_INT, ADDRESS));
            MethodHandle watchClose = linker.downcallHandle(
                lookup.find("ddb_watch_close").orElseThrow(),
                FunctionDescriptor.of(JAVA_INT, ADDRESS));
            MethodHandle stringFree = linker.downcallHandle(
                lookup.find("ddb_string_free").orElseThrow(),
                FunctionDescriptor.of(JAVA_INT, ADDRESS));
            MethodHandle resultFree = linker.downcallHandle(
                lookup.find("ddb_result_free").orElseThrow(),
                FunctionDescriptor.of(JAVA_INT, ADDRESS));
            MethodHandle rowCount = linker.downcallHandle(
                lookup.find("ddb_result_row_count").orElseThrow(),
                FunctionDescriptor.of(JAVA_INT, ADDRESS, ADDRESS));
            MethodHandle dbFree = linker.downcallHandle(
                lookup.find("ddb_db_free").orElseThrow(),
                FunctionDescriptor.of(JAVA_INT, ADDRESS));

            MemorySegment dbSlot = arena.allocate(ADDRESS);
            check((int) open.invokeExact(arena.allocateUtf8String(":memory:"), dbSlot), "open_or_create", lastError);
            MemorySegment db = dbSlot.get(ADDRESS, 0);

            MemorySegment resultSlot = arena.allocate(ADDRESS);
            check((int) execute.invokeExact(
                db,
                arena.allocateUtf8String("CREATE TABLE smoke (id INT64 PRIMARY KEY, name TEXT)"),
                MemorySegment.NULL,
                0L,
                resultSlot
            ), "create", lastError);
            check((int) resultFree.invokeExact(resultSlot), "free create", lastError);

            check((int) execute.invokeExact(
                db,
                arena.allocateUtf8String("INSERT INTO smoke (id, name) VALUES (1, 'java-smoke')"),
                MemorySegment.NULL,
                0L,
                resultSlot
            ), "insert", lastError);
            check((int) resultFree.invokeExact(resultSlot), "free insert", lastError);

            check((int) executeQueued.invokeExact(
                db,
                arena.allocateUtf8String("INSERT INTO smoke (id, name) VALUES (2, 'java-queued')"),
                MemorySegment.NULL,
                0L,
                DDB_WRITE_QUEUE_TIMEOUT_DEFAULT,
                resultSlot
            ), "queued insert", lastError);
            check((int) resultFree.invokeExact(resultSlot), "free queued insert", lastError);
            MemorySegment metrics = arena.allocate(120);
            check((int) queueMetrics.invokeExact(db, metrics), "queue metrics", lastError);
            if (metrics.get(JAVA_LONG, 16) != 1L || metrics.get(JAVA_LONG, 56) != 1L || metrics.get(JAVA_LONG, 64) != 0L) {
                throw new IllegalStateException("unexpected queue metrics");
            }

            MemorySegment watchSlot = arena.allocate(ADDRESS);
            check((int) watchQuery.invokeExact(
                db,
                arena.allocateUtf8String("{\"sql\":\"SELECT id, name FROM smoke ORDER BY id\"}"),
                watchSlot
            ), "watch query", lastError);
            MemorySegment watch = watchSlot.get(ADDRESS, 0);
            MemorySegment eventSlot = arena.allocate(ADDRESS);
            check((int) watchNext.invokeExact(watch, 1000, eventSlot), "watch initial", lastError);
            String initial = eventSlot.get(ADDRESS, 0).reinterpret(Long.MAX_VALUE).getUtf8String(0);
            if (!initial.contains("\"type\":\"initial\"")) {
                throw new IllegalStateException("unexpected initial watch event: " + initial);
            }
            check((int) stringFree.invokeExact(eventSlot), "free watch initial", lastError);

            check((int) execute.invokeExact(
                db,
                arena.allocateUtf8String("INSERT INTO smoke (id, name) VALUES (3, 'java-watch')"),
                MemorySegment.NULL,
                0L,
                resultSlot
            ), "watch insert", lastError);
            check((int) resultFree.invokeExact(resultSlot), "free watch insert", lastError);
            check((int) watchNext.invokeExact(watch, 1000, eventSlot), "watch invalidate", lastError);
            String invalidate = eventSlot.get(ADDRESS, 0).reinterpret(Long.MAX_VALUE).getUtf8String(0);
            if (!invalidate.contains("\"type\":\"invalidate\"") || !invalidate.contains("\"smoke\"")) {
                throw new IllegalStateException("unexpected invalidate watch event: " + invalidate);
            }
            check((int) stringFree.invokeExact(eventSlot), "free watch invalidate", lastError);
            int timeoutStatus = (int) watchNext.invokeExact(watch, 1, eventSlot);
            if (timeoutStatus != DDB_ERR_TIMEOUT) {
                throw new IllegalStateException("expected watch timeout, got " + timeoutStatus);
            }
            check((int) watchClose.invokeExact(watchSlot), "watch close", lastError);

            check((int) execute.invokeExact(
                db,
                arena.allocateUtf8String("SELECT id, name FROM smoke"),
                MemorySegment.NULL,
                0L,
                resultSlot
            ), "select", lastError);
            MemorySegment rows = arena.allocate(JAVA_LONG);
            MemorySegment result = resultSlot.get(ADDRESS, 0);
            check((int) rowCount.invokeExact(result, rows), "row count", lastError);
            if (rows.get(JAVA_LONG, 0) != 3L) {
                throw new IllegalStateException("expected 3 rows");
            }
            check((int) resultFree.invokeExact(resultSlot), "free select", lastError);

            int status = (int) execute.invokeExact(
                db,
                arena.allocateUtf8String("SELECT * FROM nope"),
                MemorySegment.NULL,
                0L,
                resultSlot
            );
            if (status != DDB_ERR_SQL) {
                throw new IllegalStateException("expected SQL error, got " + status);
            }
            String error = errorString(lastError);
            if (!error.contains("nope")) {
                throw new IllegalStateException("unexpected error: " + error);
            }
            String diagnostic = errorJson(lastErrorJson, stringFree, arena);
            if (!diagnostic.contains("\"code_name\":\"ERR_SQL\"")
                || !diagnostic.contains("\"subcode\":\"sql.relation_not_found\"")
                || !diagnostic.contains("\"relation\":\"nope\"")) {
                throw new IllegalStateException("unexpected diagnostic: " + diagnostic);
            }

            check((int) dbFree.invokeExact(dbSlot), "free db", lastError);
        }
    }

    private static void check(int status, String context, MethodHandle lastError) throws Throwable {
        if (status != DDB_OK) {
            throw new IllegalStateException(context + " failed with status " + status + ": " + errorString(lastError));
        }
    }

    private static String errorString(MethodHandle lastError) throws Throwable {
        MemorySegment segment = (MemorySegment) lastError.invokeExact();
        return segment.equals(MemorySegment.NULL) ? "" : segment.reinterpret(Long.MAX_VALUE).getUtf8String(0);
    }

    private static String errorJson(MethodHandle lastErrorJson, MethodHandle stringFree, Arena arena) throws Throwable {
        MemorySegment slot = arena.allocate(ADDRESS);
        int status = (int) lastErrorJson.invokeExact(slot);
        if (status != DDB_OK) {
            throw new IllegalStateException("last_error_json failed with status " + status);
        }
        MemorySegment segment = slot.get(ADDRESS, 0);
        if (segment.equals(MemorySegment.NULL)) {
            return "";
        }
        try {
            return segment.reinterpret(Long.MAX_VALUE).getUtf8String(0);
        } finally {
            int freeStatus = (int) stringFree.invokeExact(slot);
            if (freeStatus != DDB_OK) {
                throw new IllegalStateException("free diagnostic failed with status " + freeStatus);
            }
        }
    }

    private static Path locateLibrary(Path root) throws IOException {
        final String fileName = libraryFileName();
        final Path debugPath = root.resolve("target/debug").resolve(fileName);
        if (Files.isRegularFile(debugPath)) {
            return debugPath;
        }

        final Path releasePath = root.resolve("target/release").resolve(fileName);
        if (Files.isRegularFile(releasePath)) {
            return releasePath;
        }

        throw new IOException("Could not find " + fileName + " under target/debug or target/release");
    }

    private static String libraryFileName() {
        final String os = System.getProperty("os.name", "").toLowerCase();
        if (os.contains("win")) {
            return "decentdb.dll";
        }
        if (os.contains("mac") || os.contains("darwin")) {
            return "libdecentdb.dylib";
        }
        return "libdecentdb.so";
    }
}
