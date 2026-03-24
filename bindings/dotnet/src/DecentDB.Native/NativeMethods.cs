using System;
using System.Runtime.InteropServices;
using System.Threading;

namespace DecentDB.Native;

public static class DecentDBNative
{
    private const string NativeLib = "decentdb";

    private static string? s_libraryPath;
    private static int s_resolverRegistered;

    static DecentDBNative()
    {
        EnsureInitialized();
    }

    /// <summary>
    /// Overrides the native library file path used to resolve DllImport("decentdb").
    /// Useful for test runs and custom deployments.
    /// </summary>
    public static void SetLibraryPath(string libraryPath)
    {
        if (string.IsNullOrWhiteSpace(libraryPath))
        {
            throw new ArgumentException("Library path must be non-empty.", nameof(libraryPath));
        }

        s_libraryPath = Path.GetFullPath(libraryPath);
    }

    internal static void EnsureInitialized()
    {
        RegisterDllImportResolver();
    }

    private static void RegisterDllImportResolver()
    {
        if (Interlocked.Exchange(ref s_resolverRegistered, 1) != 0)
        {
            return;
        }

        try
        {
            NativeLibrary.SetDllImportResolver(typeof(DecentDBNative).Assembly, (name, _, _) =>
            {
                if (!string.Equals(name, NativeLib, StringComparison.Ordinal))
                {
                    return IntPtr.Zero;
                }

                foreach (var libPath in EnumerateCandidateLibraryPaths())
                {
                    if (!File.Exists(libPath))
                    {
                        continue;
                    }

                    try
                    {
                        return NativeLibrary.Load(libPath);
                    }
                    catch (DllNotFoundException)
                    {
                        // Keep probing other candidates.
                    }
                    catch (BadImageFormatException)
                    {
                        // Keep probing other candidates.
                    }
                }

                return IntPtr.Zero;
            });
        }
        catch (InvalidOperationException)
        {
            // Another initializer may have already set a resolver for this assembly.
            // In that case we can't override it.
        }
    }

    private static IEnumerable<string> EnumerateCandidateLibraryPaths()
    {
        var envPath = Environment.GetEnvironmentVariable("DECENTDB_NATIVE_LIB_PATH");
        if (!string.IsNullOrWhiteSpace(envPath))
        {
            yield return Path.GetFullPath(envPath);
        }

        if (!string.IsNullOrWhiteSpace(s_libraryPath))
        {
            yield return s_libraryPath!;
        }

        foreach (var dir in EnumerateProbeDirectories())
        {
            foreach (var libName in PlatformLibraryNames())
            {
                yield return Path.Combine(dir, libName);
            }
        }

        foreach (var baseDir in EnumerateProbeDirectories())
        {
            var di = new DirectoryInfo(baseDir);
            for (var cursor = di; cursor != null; cursor = cursor.Parent)
            {
                foreach (var libName in PlatformLibraryNames())
                {
                    yield return Path.Combine(cursor.FullName, libName);
                    yield return Path.Combine(cursor.FullName, "target", "debug", libName);
                    yield return Path.Combine(cursor.FullName, "target", "release", libName);
                    yield return Path.Combine(cursor.FullName, "build", libName);
                }
            }
        }
    }

    private static IEnumerable<string> PlatformLibraryNames()
    {
        if (OperatingSystem.IsWindows())
        {
            yield return "decentdb.dll";
            yield return "c_api.dll";
            yield return "libdecentdb.dll";
        }
        else if (OperatingSystem.IsMacOS())
        {
            yield return "libdecentdb.dylib";
            yield return "libc_api.dylib";
            yield return "decentdb.dylib";
        }
        else
        {
            yield return "libdecentdb.so";
            yield return "libc_api.so";
            yield return "decentdb.so";
        }
    }

    private static IEnumerable<string> EnumerateProbeDirectories()
    {
        if (!string.IsNullOrWhiteSpace(AppContext.BaseDirectory))
        {
            yield return AppContext.BaseDirectory;
        }

        var assemblyDir = Path.GetDirectoryName(typeof(DecentDBNative).Assembly.Location);
        if (!string.IsNullOrWhiteSpace(assemblyDir))
        {
            yield return assemblyDir;
        }

        var cwd = Directory.GetCurrentDirectory();
        if (!string.IsNullOrWhiteSpace(cwd))
        {
            yield return cwd;
        }
    }

    [DllImport(NativeLib, CallingConvention = CallingConvention.Cdecl, EntryPoint = "ddb_last_error_message")]
    internal static extern IntPtr ddb_last_error_message();

    [DllImport(NativeLib, CallingConvention = CallingConvention.Cdecl, EntryPoint = "ddb_string_free")]
    internal static extern uint ddb_string_free(ref IntPtr value);

    [DllImport(NativeLib, CallingConvention = CallingConvention.Cdecl, EntryPoint = "ddb_value_dispose")]
    internal static extern uint ddb_value_dispose(ref DdbValueNative value);

    [DllImport(NativeLib, CallingConvention = CallingConvention.Cdecl, EntryPoint = "ddb_db_free")]
    internal static extern uint ddb_db_free(ref IntPtr db);

    [DllImport(NativeLib, CallingConvention = CallingConvention.Cdecl, EntryPoint = "ddb_db_checkpoint")]
    internal static extern uint ddb_db_checkpoint(IntPtr db);

    [DllImport(NativeLib, CallingConvention = CallingConvention.Cdecl, EntryPoint = "ddb_db_begin_transaction")]
    internal static extern uint ddb_db_begin_transaction(IntPtr db);

    [DllImport(NativeLib, CallingConvention = CallingConvention.Cdecl, EntryPoint = "ddb_db_commit_transaction")]
    internal static extern uint ddb_db_commit_transaction(IntPtr db, out ulong outLsn);

    [DllImport(NativeLib, CallingConvention = CallingConvention.Cdecl, EntryPoint = "ddb_db_rollback_transaction")]
    internal static extern uint ddb_db_rollback_transaction(IntPtr db);

    [DllImport(NativeLib, CallingConvention = CallingConvention.Cdecl, EntryPoint = "ddb_db_save_as")]
    internal static extern uint ddb_db_save_as(IntPtr db, IntPtr destPathUtf8);

    [DllImport(NativeLib, CallingConvention = CallingConvention.Cdecl, EntryPoint = "ddb_stmt_free")]
    internal static extern uint ddb_stmt_free(ref IntPtr stmt);

    [DllImport(NativeLib, CallingConvention = CallingConvention.Cdecl, EntryPoint = "ddb_stmt_reset")]
    internal static extern uint ddb_stmt_reset(IntPtr stmt);

    [DllImport(NativeLib, CallingConvention = CallingConvention.Cdecl, EntryPoint = "ddb_stmt_clear_bindings")]
    internal static extern uint ddb_stmt_clear_bindings(IntPtr stmt);

    [DllImport(NativeLib, CallingConvention = CallingConvention.Cdecl, EntryPoint = "ddb_stmt_step")]
    internal static extern uint ddb_stmt_step(IntPtr stmt, out byte outHasRow);

    [DllImport(NativeLib, CallingConvention = CallingConvention.Cdecl, EntryPoint = "ddb_stmt_column_count")]
    internal static extern uint ddb_stmt_column_count(IntPtr stmt, out nuint outColumns);

    [DllImport(NativeLib, CallingConvention = CallingConvention.Cdecl, EntryPoint = "ddb_stmt_column_name_copy")]
    internal static extern uint ddb_stmt_column_name_copy(IntPtr stmt, nuint columnIndex, out IntPtr outName);

    [DllImport(NativeLib, CallingConvention = CallingConvention.Cdecl, EntryPoint = "ddb_stmt_affected_rows")]
    internal static extern uint ddb_stmt_affected_rows(IntPtr stmt, out ulong outRows);

    [DllImport(NativeLib, CallingConvention = CallingConvention.Cdecl, EntryPoint = "ddb_stmt_value_copy")]
    internal static extern uint ddb_stmt_value_copy(IntPtr stmt, nuint columnIndex, out DdbValueNative outValue);

    [DllImport(NativeLib, CallingConvention = CallingConvention.Cdecl, EntryPoint = "ddb_db_list_tables_json")]
    internal static extern uint ddb_db_list_tables_json(IntPtr db, out IntPtr outJson);

    [DllImport(NativeLib, CallingConvention = CallingConvention.Cdecl, EntryPoint = "ddb_db_list_indexes_json")]
    internal static extern uint ddb_db_list_indexes_json(IntPtr db, out IntPtr outJson);
}

public static unsafe class DecentDBNativeUnsafe
{
    private const string NativeLib = "decentdb";

    static DecentDBNativeUnsafe()
    {
        DecentDBNative.EnsureInitialized();
    }

    [DllImport(NativeLib, CallingConvention = CallingConvention.Cdecl, EntryPoint = "ddb_db_open_or_create")]
    internal static extern uint ddb_db_open_or_create(byte* pathUtf8, out IntPtr outDb);

    [DllImport(NativeLib, CallingConvention = CallingConvention.Cdecl, EntryPoint = "ddb_db_prepare")]
    internal static extern uint ddb_db_prepare(IntPtr db, byte* sqlUtf8, out IntPtr outStmt);

    [DllImport(NativeLib, CallingConvention = CallingConvention.Cdecl, EntryPoint = "ddb_stmt_bind_null")]
    internal static extern uint ddb_stmt_bind_null(IntPtr stmt, nuint index1Based);

    [DllImport(NativeLib, CallingConvention = CallingConvention.Cdecl, EntryPoint = "ddb_stmt_bind_int64")]
    internal static extern uint ddb_stmt_bind_int64(IntPtr stmt, nuint index1Based, long value);

    [DllImport(NativeLib, CallingConvention = CallingConvention.Cdecl, EntryPoint = "ddb_stmt_bind_bool")]
    internal static extern uint ddb_stmt_bind_bool(IntPtr stmt, nuint index1Based, byte value);

    [DllImport(NativeLib, CallingConvention = CallingConvention.Cdecl, EntryPoint = "ddb_stmt_bind_decimal")]
    internal static extern uint ddb_stmt_bind_decimal(IntPtr stmt, nuint index1Based, long scaled, byte scale);

    [DllImport(NativeLib, CallingConvention = CallingConvention.Cdecl, EntryPoint = "ddb_stmt_bind_float64")]
    internal static extern uint ddb_stmt_bind_float64(IntPtr stmt, nuint index1Based, double value);

    [DllImport(NativeLib, CallingConvention = CallingConvention.Cdecl, EntryPoint = "ddb_stmt_bind_text")]
    internal static extern uint ddb_stmt_bind_text(IntPtr stmt, nuint index1Based, byte* utf8, nuint byteLen);

    [DllImport(NativeLib, CallingConvention = CallingConvention.Cdecl, EntryPoint = "ddb_stmt_bind_blob")]
    internal static extern uint ddb_stmt_bind_blob(IntPtr stmt, nuint index1Based, byte* data, nuint byteLen);

    [DllImport(NativeLib, CallingConvention = CallingConvention.Cdecl, EntryPoint = "ddb_stmt_bind_timestamp_micros")]
    internal static extern uint ddb_stmt_bind_timestamp_micros(IntPtr stmt, nuint index1Based, long timestampMicros);

    [DllImport(NativeLib, CallingConvention = CallingConvention.Cdecl, EntryPoint = "ddb_db_describe_table_json")]
    internal static extern uint ddb_db_describe_table_json(IntPtr db, byte* tableNameUtf8, out IntPtr outJson);
}

internal enum DdbValueTag : uint
{
    Null = 0,
    Int64 = 1,
    Float64 = 2,
    Bool = 3,
    Text = 4,
    Blob = 5,
    Decimal = 6,
    Uuid = 7,
    TimestampMicros = 8
}

[StructLayout(LayoutKind.Sequential)]
internal unsafe struct DdbValueNative
{
    public uint tag;
    public byte bool_value;
    public fixed byte reserved0[7];
    public long int64_value;
    public double float64_value;
    public long decimal_scaled;
    public byte decimal_scale;
    public fixed byte reserved1[7];
    public byte* data;
    public nuint len;
    public fixed byte uuid_bytes[16];
    public long timestamp_micros;
}

[StructLayout(LayoutKind.Sequential)]
public struct DecentdbValueView
{
    public int kind;
    public int is_null;
    public long int64_val;
    public double float64_val;
    public IntPtr bytes;
    public int bytes_len;
    public int decimal_scale;
}

public enum DbValueKind : int
{
    Null = 0,
    Int64 = 1,
    Bool = 2,
    Float64 = 3,
    Text = 4,
    Blob = 5,
    Decimal = 12,
    Timestamp = 17
}
