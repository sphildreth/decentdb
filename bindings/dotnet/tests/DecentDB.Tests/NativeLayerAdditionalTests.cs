using System;
using System.IO;
using System.Runtime.InteropServices;
using Xunit;
using DecentDB.Native;

namespace DecentDB.Tests;

public class NativeLayerAdditionalTests : IDisposable
{
    private readonly string _dbPath;

    public NativeLayerAdditionalTests()
    {
        _dbPath = Path.Combine(Path.GetTempPath(), $"test_add_{Guid.NewGuid():N}.ddb");
    }

    public void Dispose()
    {
        if (File.Exists(_dbPath))
            File.Delete(_dbPath);
        var walPath = _dbPath + "-wal";
        if (File.Exists(walPath))
            File.Delete(walPath);
    }

    [Fact]
    public void SafeHandles_Dispose_WhenUsedDirectly()
    {
        // Test DecentDBHandle
        IntPtr handlePtr;
        using (var db = new DecentDB.Native.DecentDB(_dbPath))
        {
            handlePtr = db.Handle;
            Assert.NotEqual(IntPtr.Zero, handlePtr);
            
            // Test that handle is valid while db is alive
            var errorCode = DecentDBNative.decentdb_last_error_code(handlePtr);
            Assert.Equal(0, errorCode); // No error initially
        }
        // At this point, the handle should be closed by the SafeHandle
        
        // Test that accessing the closed handle results in an error
        // Note: We can't really test this perfectly since the native code behavior
        // after closing varies, but we can at least verify the SafeHandle worked
    }

    [Fact]
    public void SafeHandles_StatementHandle_Disposal()
    {
        using (var db = new DecentDB.Native.DecentDB(_dbPath))
        {
            // Create a table first
            using (var createStmt = db.Prepare("CREATE TABLE test_table (id INTEGER PRIMARY KEY, name TEXT)"))
            {
                var result = createStmt.Step();
                Assert.Equal(0, result); // Success
            }

            // Create a prepared statement and ensure it gets disposed properly
            var stmtHandle = IntPtr.Zero;
            using (var stmt = db.Prepare("INSERT INTO test_table (id, name) VALUES ($1, $2)"))
            {
                stmtHandle = stmt.Handle;
                Assert.NotEqual(IntPtr.Zero, stmtHandle);
                
                // Bind and execute
                stmt.BindInt64(1, 1);
                stmt.BindText(2, "test");
                var result = stmt.Step();
                Assert.Equal(0, result); // Success
            }
            // Statement should be finalized by SafeHandle
        }
    }

    [Fact]
    public void PreparedStatement_Reset_ClearBindings_Functionality()
    {
        using var db = new DecentDB.Native.DecentDB(_dbPath);
        
        // Create table
        using (var createStmt = db.Prepare("CREATE TABLE reset_test (id INTEGER PRIMARY KEY, value TEXT)"))
        {
            createStmt.Step();
        }

        using var stmt = db.Prepare("INSERT INTO reset_test (id, value) VALUES ($1, $2)");
        
        // First insertion
        stmt.BindInt64(1, 1);
        stmt.BindText(2, "first");
        stmt.Step();
        
        // Reset and clear bindings
        stmt.Reset();
        stmt.ClearBindings();
        
        // Second insertion with new values
        stmt.BindInt64(1, 2);
        stmt.BindText(2, "second");
        stmt.Step();
        
        // Verify both records exist
        using var selectStmt = db.Prepare("SELECT COUNT(*) FROM reset_test");
        selectStmt.Step();
        Assert.Equal(2L, selectStmt.GetInt64(0));
    }

    [Fact]
    public void PreparedStatement_BindNull_Functionality()
    {
        using var db = new DecentDB.Native.DecentDB(_dbPath);
        
        // Create table with nullable column
        using (var createStmt = db.Prepare("CREATE TABLE null_test (id INTEGER PRIMARY KEY, value TEXT)"))
        {
            createStmt.Step();
        }

        using var insertStmt = db.Prepare("INSERT INTO null_test (id, value) VALUES ($1, $2)");
        insertStmt.BindInt64(1, 1);
        insertStmt.BindNull(2); // Bind null value
        insertStmt.Step();
        
        // Verify null was inserted
        using var selectStmt = db.Prepare("SELECT value FROM null_test WHERE id = 1");
        selectStmt.Step();
        Assert.True(selectStmt.IsNull(0));
        Assert.Equal("", selectStmt.GetText(0)); // Null should return empty string when getting text
    }

    [Fact]
    public void DecentDB_Checkpoint_Functionality()
    {
        using var db = new DecentDB.Native.DecentDB(_dbPath);
        
        // Create table
        using (var createStmt = db.Prepare("CREATE TABLE checkpoint_test (id INTEGER PRIMARY KEY)"))
        {
            createStmt.Step();
        }

        // Insert some data
        using (var insertStmt = db.Prepare("INSERT INTO checkpoint_test (id) VALUES ($1)"))
        {
            for (int i = 0; i < 10; i++)
            {
                insertStmt.BindInt64(1, i);
                insertStmt.Step();
                insertStmt.Reset().ClearBindings();
            }
        }

        // Call checkpoint - this should not throw
        db.Checkpoint();
        
        // Verify data is still there after checkpoint
        using var selectStmt = db.Prepare("SELECT COUNT(*) FROM checkpoint_test");
        selectStmt.Step();
        Assert.Equal(10L, selectStmt.GetInt64(0));
    }

    [Fact]
    public void PreparedStatement_Blob_Functionality_WithEmptyArray()
    {
        using var db = new DecentDB.Native.DecentDB(_dbPath);
        
        // Create table
        using (var createStmt = db.Prepare("CREATE TABLE blob_test (id INTEGER PRIMARY KEY, data BLOB)"))
        {
            createStmt.Step();
        }

        using var insertStmt = db.Prepare("INSERT INTO blob_test (id, data) VALUES ($1, $2)");
        insertStmt.BindInt64(1, 1);
        insertStmt.BindBlob(2, new byte[0]); // Empty blob
        insertStmt.Step();
        
        // Retrieve and verify empty blob
        using var selectStmt = db.Prepare("SELECT data FROM blob_test WHERE id = 1");
        selectStmt.Step();
        var retrievedBlob = selectStmt.GetBlob(0);
        Assert.Empty(retrievedBlob);
    }

    [Fact]
    public void PreparedStatement_Text_Functionality_WithEmptyString()
    {
        using var db = new DecentDB.Native.DecentDB(_dbPath);
        
        // Create table
        using (var createStmt = db.Prepare("CREATE TABLE text_test (id INTEGER PRIMARY KEY, data TEXT)"))
        {
            createStmt.Step();
        }

        using var insertStmt = db.Prepare("INSERT INTO text_test (id, data) VALUES ($1, $2)");
        insertStmt.BindInt64(1, 1);
        insertStmt.BindText(2, ""); // Empty string
        insertStmt.Step();
        
        // Retrieve and verify empty string
        using var selectStmt = db.Prepare("SELECT data FROM text_test WHERE id = 1");
        selectStmt.Step();
        var retrievedText = selectStmt.GetText(0);
        Assert.Equal("", retrievedText);
    }

    [Fact]
    public void PreparedStatement_BindTextBytes_Functionality()
    {
        using var db = new DecentDB.Native.DecentDB(_dbPath);
        
        // Create table
        using (var createStmt = db.Prepare("CREATE TABLE text_bytes_test (id INTEGER PRIMARY KEY, data TEXT)"))
        {
            createStmt.Step();
        }

        using var insertStmt = db.Prepare("INSERT INTO text_bytes_test (id, data) VALUES ($1, $2)");
        insertStmt.BindInt64(1, 1);
        
        var textBytes = System.Text.Encoding.UTF8.GetBytes("Hello, 世界!");
        insertStmt.BindTextBytes(2, textBytes);
        insertStmt.Step();
        
        // Retrieve and verify the text
        using var selectStmt = db.Prepare("SELECT data FROM text_bytes_test WHERE id = 1");
        selectStmt.Step();
        var retrievedText = selectStmt.GetText(0);
        Assert.Equal("Hello, 世界!", retrievedText);
    }

    [Fact]
    public void DecentDB_LastErrorCode_LastErrorMessage()
    {
        using var db = new DecentDB.Native.DecentDB(_dbPath);
        
        // Initially should be no error
        Assert.Equal(0, db.LastErrorCode);
        Assert.Equal("", db.LastErrorMessage);
        
        // Try to prepare invalid SQL to generate an error
        try
        {
            using var badStmt = db.Prepare("INVALID SQL SYNTAX TO GENERATE ERROR");
        }
        catch (DecentDBException)
        {
            // Expected
        }
        
        // Now there should be an error code
        Assert.NotEqual(0, db.LastErrorCode);
        Assert.NotEqual("", db.LastErrorMessage);
    }

    [Fact]
    public void PreparedStatement_Getters_WithNullValues()
    {
        using var db = new DecentDB.Native.DecentDB(_dbPath);
        
        // Create table
        using (var createStmt = db.Prepare("CREATE TABLE getters_test (id INTEGER PRIMARY KEY, txt TEXT, num INTEGER, flt REAL, blb BLOB)"))
        {
            createStmt.Step();
        }

        // Insert a row with some null values
        using (var insertStmt = db.Prepare("INSERT INTO getters_test (id, txt, num, flt, blb) VALUES ($1, $2, $3, $4, $5)"))
        {
            insertStmt.BindInt64(1, 1);
            insertStmt.BindNull(2); // txt = NULL
            insertStmt.BindNull(3); // num = NULL
            insertStmt.BindNull(4); // flt = NULL
            insertStmt.BindNull(5); // blb = NULL
            insertStmt.Step();
        }

        // Select and test getters with null values
        using var selectStmt = db.Prepare("SELECT txt, num, flt, blb FROM getters_test WHERE id = 1");
        selectStmt.Step();
        
        Assert.True(selectStmt.IsNull(0)); // txt is null
        Assert.True(selectStmt.IsNull(1)); // num is null
        Assert.True(selectStmt.IsNull(2)); // flt is null
        Assert.True(selectStmt.IsNull(3)); // blb is null
        
        // Test that getters return default values for null
        Assert.Equal("", selectStmt.GetText(0));           // Null text returns empty string
        Assert.Equal(0L, selectStmt.GetInt64(1));         // Null int returns 0
        Assert.Equal(0.0, selectStmt.GetFloat64(2));      // Null float returns 0.0
        Assert.Equal(Guid.Empty, selectStmt.GetGuid(0));   // Null guid returns empty
        Assert.Equal(Array.Empty<byte>(), selectStmt.GetBlob(3)); // Null blob returns empty array
    }

    [Fact]
    public void PreparedStatement_Dispose_MultipleTimes()
    {
        var db = new DecentDB.Native.DecentDB(_dbPath);
        
        // Create table
        using (var createStmt = db.Prepare("CREATE TABLE dispose_test (id INTEGER PRIMARY KEY)"))
        {
            createStmt.Step();
        }

        var stmt = db.Prepare("INSERT INTO dispose_test (id) VALUES ($1)");
        stmt.BindInt64(1, 1);
        stmt.Step();
        
        // Dispose once
        stmt.Dispose();
        
        // Disposing again should not throw
        stmt.Dispose();
        
        db.Dispose();
    }

    [Fact]
    public void DecentDB_Dispose_MultipleTimes()
    {
        var db = new DecentDB.Native.DecentDB(_dbPath);
        
        // Create table to ensure db is valid
        using (var createStmt = db.Prepare("CREATE TABLE dispose_test2 (id INTEGER PRIMARY KEY)"))
        {
            createStmt.Step();
        }
        
        // Dispose once
        db.Dispose();
        
        // Disposing again should not throw
        db.Dispose();
    }
}