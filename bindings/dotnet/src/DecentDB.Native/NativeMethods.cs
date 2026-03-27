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

    [DllImport(NativeLib, CallingConvention = CallingConvention.Cdecl, EntryPoint = "ddb_abi_version")]
    internal static extern uint ddb_abi_version();

    [DllImport(NativeLib, CallingConvention = CallingConvention.Cdecl, EntryPoint = "ddb_version")]
    internal static extern IntPtr ddb_version();

    [DllImport(NativeLib, CallingConvention = CallingConvention.Cdecl, EntryPoint = "ddb_db_create")]
    internal static extern uint ddb_db_create(IntPtr pathUtf8, out IntPtr outDb);

    [DllImport(NativeLib, CallingConvention = CallingConvention.Cdecl, EntryPoint = "ddb_db_open")]
    internal static extern uint ddb_db_open(IntPtr pathUtf8, out IntPtr outDb);

    [DllImport(NativeLib, CallingConvention = CallingConvention.Cdecl, EntryPoint = "ddb_db_in_transaction")]
    internal static extern uint ddb_db_in_transaction(IntPtr db, out byte outFlag);

    [DllImport(NativeLib, CallingConvention = CallingConvention.Cdecl, EntryPoint = "ddb_db_get_table_ddl")]
    internal static extern uint ddb_db_get_table_ddl(IntPtr db, IntPtr tableNameUtf8, out IntPtr outDdl);

    [DllImport(NativeLib, CallingConvention = CallingConvention.Cdecl, EntryPoint = "ddb_db_list_views_json")]
    internal static extern uint ddb_db_list_views_json(IntPtr db, out IntPtr outJson);

    [DllImport(NativeLib, CallingConvention = CallingConvention.Cdecl, EntryPoint = "ddb_db_get_view_ddl")]
    internal static extern uint ddb_db_get_view_ddl(IntPtr db, IntPtr viewNameUtf8, out IntPtr outDdl);

    [DllImport(NativeLib, CallingConvention = CallingConvention.Cdecl, EntryPoint = "ddb_db_list_triggers_json")]
    internal static extern uint ddb_db_list_triggers_json(IntPtr db, out IntPtr outJson);

    [DllImport(NativeLib, CallingConvention = CallingConvention.Cdecl, EntryPoint = "ddb_db_execute")]
    internal static extern uint ddb_db_execute(IntPtr db, IntPtr sqlUtf8, out IntPtr outResult);

    [DllImport(NativeLib, CallingConvention = CallingConvention.Cdecl, EntryPoint = "ddb_result_free")]
    internal static extern uint ddb_result_free(ref IntPtr result);

    [DllImport(NativeLib, CallingConvention = CallingConvention.Cdecl, EntryPoint = "ddb_result_row_count")]
    internal static extern uint ddb_result_row_count(IntPtr result, out nuint outRows);

    [DllImport(NativeLib, CallingConvention = CallingConvention.Cdecl, EntryPoint = "ddb_result_column_count")]
    internal static extern uint ddb_result_column_count(IntPtr result, out nuint outColumns);

    [DllImport(NativeLib, CallingConvention = CallingConvention.Cdecl, EntryPoint = "ddb_result_affected_rows")]
    internal static extern uint ddb_result_affected_rows(IntPtr result, out ulong outRows);

    [DllImport(NativeLib, CallingConvention = CallingConvention.Cdecl, EntryPoint = "ddb_result_column_name_copy")]
    internal static extern uint ddb_result_column_name_copy(IntPtr result, nuint columnIndex, out IntPtr outName);

    [DllImport(NativeLib, CallingConvention = CallingConvention.Cdecl, EntryPoint = "ddb_result_value_copy")]
    internal static extern uint ddb_result_value_copy(IntPtr result, nuint rowIndex, nuint columnIndex, out DdbValueNative outValue);
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

    [DllImport(NativeLib, CallingConvention = CallingConvention.Cdecl, EntryPoint = "ddb_db_open")]
    internal static extern uint ddb_db_open(byte* pathUtf8, out IntPtr outDb);

    [DllImport(NativeLib, CallingConvention = CallingConvention.Cdecl, EntryPoint = "ddb_db_create")]
    internal static extern uint ddb_db_create(byte* pathUtf8, out IntPtr outDb);

    [DllImport(NativeLib, CallingConvention = CallingConvention.Cdecl, EntryPoint = "ddb_db_execute")]
    internal static extern uint ddb_db_execute(IntPtr db, byte* sqlUtf8, out IntPtr outResult);

    [DllImport(NativeLib, CallingConvention = CallingConvention.Cdecl, EntryPoint = "ddb_db_get_table_ddl")]
    internal static extern uint ddb_db_get_table_ddl(IntPtr db, byte* tableNameUtf8, out IntPtr outDdl);

    [DllImport(NativeLib, CallingConvention = CallingConvention.Cdecl, EntryPoint = "ddb_db_get_view_ddl")]
    internal static extern uint ddb_db_get_view_ddl(IntPtr db, byte* viewNameUtf8, out IntPtr outDdl);

    [DllImport(NativeLib, CallingConvention = CallingConvention.Cdecl, EntryPoint = "ddb_stmt_bind_int64_step_row_view")]
    internal static extern uint ddb_stmt_bind_int64_step_row_view(IntPtr stmt, nuint index1Based, long value,
        out IntPtr outValues, out nuint outCount, out byte outHasRow);

    [DllImport(NativeLib, CallingConvention = CallingConvention.Cdecl, EntryPoint = "ddb_stmt_bind_int64_step_i64_text_f64")]
    internal static extern uint ddb_stmt_bind_int64_step_i64_text_f64(IntPtr stmt, nuint index1Based, long value,
        out long outInt, out IntPtr outTextPtr, out nuint outTextLen, out double outFloat, out nuint outCount, out byte outHasRow);

    [DllImport(NativeLib, CallingConvention = CallingConvention.Cdecl, EntryPoint = "ddb_stmt_step_row_view")]
    internal static extern uint ddb_stmt_step_row_view(IntPtr stmt, out IntPtr outValues, out nuint outCount, out byte outHasRow);

    [DllImport(NativeLib, CallingConvention = CallingConvention.Cdecl, EntryPoint = "ddb_stmt_fetch_row_views")]
    internal static extern uint ddb_stmt_fetch_row_views(IntPtr stmt, byte includeCurrentRow, nuint maxRows,
        out IntPtr outValues, out nuint outRows, out nuint outColumns);

    [DllImport(NativeLib, CallingConvention = CallingConvention.Cdecl, EntryPoint = "ddb_stmt_execute_batch_i64")]
    internal static extern uint ddb_stmt_execute_batch_i64(IntPtr stmt, nuint count, long* values, out ulong outAffected);

    [DllImport(NativeLib, CallingConvention = CallingConvention.Cdecl, EntryPoint = "ddb_stmt_execute_batch_i64_text_f64")]
    internal static extern uint ddb_stmt_execute_batch_i64_text_f64(IntPtr stmt, nuint count,
        long* ids, byte** texts, nuint* textLens, double* floats, out ulong outAffected);

    [DllImport(NativeLib, CallingConvention = CallingConvention.Cdecl, EntryPoint = "ddb_stmt_execute_batch_typed")]
    internal static extern uint ddb_stmt_execute_batch_typed(IntPtr stmt, byte* signatureUtf8, nuint count, out ulong outAffected);

    [DllImport(NativeLib, CallingConvention = CallingConvention.Cdecl, EntryPoint = "ddb_stmt_rebind_int64_execute")]
    internal static extern uint ddb_stmt_rebind_int64_execute(IntPtr stmt, long value, out ulong outAffected);

    [DllImport(NativeLib, CallingConvention = CallingConvention.Cdecl, EntryPoint = "ddb_stmt_rebind_text_int64_execute")]
    internal static extern uint ddb_stmt_rebind_text_int64_execute(IntPtr stmt, byte* text, nuint textLen, long intValue, out ulong outAffected);

    [DllImport(NativeLib, CallingConvention = CallingConvention.Cdecl, EntryPoint = "ddb_stmt_rebind_int64_text_execute")]
    internal static extern uint ddb_stmt_rebind_int64_text_execute(IntPtr stmt, long intValue, byte* text, nuint textLen, out ulong outAffected);
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
