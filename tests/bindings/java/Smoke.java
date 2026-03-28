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

    public static void main(String[] args) throws Throwable {
        Path root = Path.of("").toAbsolutePath();
        Path library = locateLibrary(root);

        try (Arena arena = Arena.ofConfined()) {
            SymbolLookup lookup = SymbolLookup.libraryLookup(library, arena);
            Linker linker = Linker.nativeLinker();

            MethodHandle lastError = linker.downcallHandle(
                lookup.find("ddb_last_error_message").orElseThrow(),
                FunctionDescriptor.of(ADDRESS));
            MethodHandle open = linker.downcallHandle(
                lookup.find("ddb_db_open_or_create").orElseThrow(),
                FunctionDescriptor.of(JAVA_INT, ADDRESS, ADDRESS));
            MethodHandle execute = linker.downcallHandle(
                lookup.find("ddb_db_execute").orElseThrow(),
                FunctionDescriptor.of(JAVA_INT, ADDRESS, ADDRESS, ADDRESS, JAVA_LONG, ADDRESS));
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
            if (rows.get(JAVA_LONG, 0) != 1L) {
                throw new IllegalStateException("expected 1 row");
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
