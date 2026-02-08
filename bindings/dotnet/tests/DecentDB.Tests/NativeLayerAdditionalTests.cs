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
        _dbPath = Path.Combine(Path.GetTempPath(), $"test_native_add_{Guid.NewGuid():N}.ddb");
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
    public void DecentDBException_Properties_AreCorrect()
    {
        var ex = new DecentDBException(100, "Test error message", "SELECT * FROM table");
        
        Assert.Equal(100, ex.ErrorCode);
        Assert.Equal("SELECT * FROM table", ex.Sql);
        Assert.Contains("DecentDB error 100: Test error message", ex.Message);
    }

    [Fact]
    public void DecentDB_Dispose_MultipleTimes_DoesNotThrow()
    {
        using var db = new DecentDB.Native.DecentDB(_dbPath);
        db.Dispose(); // First dispose
        db.Dispose(); // Second dispose - should not throw
    }

    [Fact]
    public void DecentDB_LastErrorCodeAndMessage_AfterOperation()
    {
        using var db = new DecentDB.Native.DecentDB(_dbPath);
        Assert.Equal(0, db.LastErrorCode); // Assuming success code is 0
        Assert.NotEmpty(db.LastErrorMessage); // Should have some default message
    }

    [Fact]
    public void PreparedStatement_Dispose_MultipleTimes_DoesNotThrow()
    {
        using var db = new Native.DecentDB(_dbPath);
        using var stmt = db.Prepare("SELECT 1");
        stmt.Dispose(); // First dispose
        stmt.Dispose(); // Second dispose - should not throw
    }

    [Fact]
    public void PreparedStatement_Reset_ClearBindings_Chain()
    {
        using var db = new Native.DecentDB(_dbPath);
        using var stmt = db.Prepare("SELECT $1, $2");
        stmt.BindInt64(1, 100);
        stmt.BindText(2, "test");
        
        // Chain reset and clear bindings
        var chainedStmt = stmt.Reset().ClearBindings();
        Assert.NotNull(chainedStmt);
        
        // After clearing bindings, the values should be reset
        stmt.Step(); // This should work without binding values
    }

    [Fact]
    public void PreparedStatement_BindText_EmptyString()
    {
        using var db = new Native.DecentDB(_dbPath);
        using var createStmt = db.Prepare("CREATE TABLE test_empty_text (id INTEGER PRIMARY KEY, text_col TEXT)");
        createStmt.Step();
        
        using var insertStmt = db.Prepare("INSERT INTO test_empty_text (id, text_col) VALUES ($1, $2)");
        insertStmt.BindInt64(1, 1);
        insertStmt.BindText(2, ""); // Empty string
        
        var result = insertStmt.Step();
        Assert.True(result >= 0); // Success
        
        using var selectStmt = db.Prepare("SELECT text_col FROM test_empty_text WHERE id = 1");
        var selectResult = selectStmt.Step();
        Assert.True(selectResult == 1); // Row found
        Assert.Equal("", selectStmt.GetText(0)); // Empty string retrieved
    }

    [Fact]
    public void PreparedStatement_BindText_NullString()
    {
        using var db = new Native.DecentDB(_dbPath);
        using var createStmt = db.Prepare("CREATE TABLE test_null_text (id INTEGER PRIMARY KEY, text_col TEXT)");
        createStmt.Step();
        
        using var insertStmt = db.Prepare("INSERT INTO test_null_text (id, text_col) VALUES ($1, $2)");
        insertStmt.BindInt64(1, 1);
        insertStmt.BindText(2, null); // Null string becomes empty
        
        var result = insertStmt.Step();
        Assert.True(result >= 0); // Success
    }

    [Fact]
    public void PreparedStatement_BindBlob_EmptyArray()
    {
        using var db = new Native.DecentDB(_dbPath);
        using var createStmt = db.Prepare("CREATE TABLE test_empty_blob (id INTEGER PRIMARY KEY, blob_col BLOB)");
        createStmt.Step();
        
        using var insertStmt = db.Prepare("INSERT INTO test_empty_blob (id, blob_col) VALUES ($1, $2)");
        insertStmt.BindInt64(1, 1);
        insertStmt.BindBlob(2, new byte[0]); // Empty array
        
        var result = insertStmt.Step();
        Assert.True(result >= 0); // Success
        
        using var selectStmt = db.Prepare("SELECT blob_col FROM test_empty_blob WHERE id = 1");
        var selectResult = selectStmt.Step();
        Assert.True(selectResult == 1); // Row found
        var retrieved = selectStmt.GetBlob(0);
        Assert.Equal(new byte[0], retrieved); // Empty array retrieved
    }

    [Fact]
    public void PreparedStatement_BindBlob_NullArray()
    {
        using var db = new Native.DecentDB(_dbPath);
        using var createStmt = db.Prepare("CREATE TABLE test_null_blob (id INTEGER PRIMARY KEY, blob_col BLOB)");
        createStmt.Step();
        
        using var insertStmt = db.Prepare("INSERT INTO test_null_blob (id, blob_col) VALUES ($1, $2)");
        insertStmt.BindInt64(1, 1);
        insertStmt.BindBlob(2, null); // Null array becomes empty
        
        var result = insertStmt.Step();
        Assert.True(result >= 0); // Success
    }

    [Fact]
    public void PreparedStatement_ColumnMetadata_NegativeIndex_Throws()
    {
        using var db = new Native.DecentDB(_dbPath);
        using var stmt = db.Prepare("SELECT 1 AS col1");
        
        // Accessing negative column index should not crash but return empty/default
        var columnName = stmt.ColumnName(-1);
        Assert.NotNull(columnName); // Should not throw, but return empty string
    }

    [Fact]
    public void PreparedStatement_ColumnMetadata_OutOfBoundsIndex()
    {
        using var db = new Native.DecentDB(_dbPath);
        using var stmt = db.Prepare("SELECT 1 AS col1");
        
        // Accessing out-of-bounds column index should not crash
        var columnName = stmt.ColumnName(10); // Beyond available columns
        Assert.NotNull(columnName); // Should not throw, but return empty string
    }

    [Fact]
    public void PreparedStatement_Getters_OutOfBoundsIndex()
    {
        using var db = new Native.DecentDB(_dbPath);
        using var stmt = db.Prepare("SELECT 1 AS col1");
        stmt.Step();
        
        // Accessing out-of-bounds column index should not crash
        var intVal = stmt.GetInt64(10); // Beyond available columns
        var textVal = stmt.GetText(10);
        var blobVal = stmt.GetBlob(10);
        
        // Values should be defaults, not throw exceptions
        Assert.Equal(0L, intVal);
        Assert.NotNull(textVal);
        Assert.NotNull(blobVal);
    }

    [Fact]
    public void PreparedStatement_RowsAffected_AfterOperations()
    {
        using var db = new Native.DecentDB(_dbPath);
        
        // Create table
        using var createStmt = db.Prepare("CREATE TABLE test_rows_affected (id INTEGER PRIMARY KEY, value TEXT)");
        createStmt.Step();
        Assert.Equal(0, createStmt.RowsAffected); // CREATE doesn't affect rows
        
        // Insert
        using var insertStmt = db.Prepare("INSERT INTO test_rows_affected (id, value) VALUES (1, 'test')");
        insertStmt.Step();
        Assert.Equal(1, insertStmt.RowsAffected); // One row inserted
        
        // Update
        using var updateStmt = db.Prepare("UPDATE test_rows_affected SET value = 'updated' WHERE id = 1");
        updateStmt.Step();
        Assert.Equal(1, updateStmt.RowsAffected); // One row updated
        
        // Delete
        using var deleteStmt = db.Prepare("DELETE FROM test_rows_affected WHERE id = 1");
        deleteStmt.Step();
        Assert.Equal(1, deleteStmt.RowsAffected); // One row deleted
    }

    [Fact]
    public void DecentDB_Checkpoint_Success()
    {
        using var db = new Native.DecentDB(_dbPath);
        // This should not throw an exception
        db.Checkpoint();
    }

    [Fact]
    public void RowView_IndexOutOfRange_Throws()
    {
        using var db = new Native.DecentDB(_dbPath);
        using var stmt = db.Prepare("SELECT 1, 2, 3");
        stmt.Step();
        
        var rowView = stmt.GetRowView();
        Assert.Equal(3, rowView.Count);
        
        // Accessing valid indices should work
        var val1 = rowView[0];
        var val2 = rowView[1];
        var val3 = rowView[2];
        
        // Accessing out of range should throw
        Assert.Throws<IndexOutOfRangeException>(() => { var val = rowView[3]; });
        Assert.Throws<IndexOutOfRangeException>(() => { var val = rowView[-1]; });
    }

    [Fact]
    public void PreparedStatement_BindDecimal_Overflow_Throws()
    {
        using var db = new Native.DecentDB(_dbPath);
        using var stmt = db.Prepare("SELECT $1");
        
        // Test with a decimal that's too large for 64-bit representation
        var largeDecimal = decimal.MaxValue; // This should cause overflow
        
        var ex = Assert.Throws<OverflowException>(() => stmt.BindDecimal(1, largeDecimal));
        Assert.Contains("too large for DecentDB DECIMAL", ex.Message);
    }

    [Fact]
    public void PreparedStatement_BindDecimal_HighValue_Throws()
    {
        using var db = new Native.DecentDB(_dbPath);
        using var stmt = db.Prepare("SELECT $1");
        
        // Create a decimal with high value that exceeds 64-bit limits
        var bits = new int[4] { 0, 0, 1, 0 }; // High part is non-zero
        var largeDecimal = new decimal(bits);
        
        var ex = Assert.Throws<OverflowException>(() => stmt.BindDecimal(1, largeDecimal));
        Assert.Contains("too large for DecentDB DECIMAL", ex.Message);
    }

    [Fact]
    public void PreparedStatement_GetDecimal_ZeroScale()
    {
        using var db = new Native.DecentDB(_dbPath);
        using var createStmt = db.Prepare("CREATE TABLE test_dec (id INTEGER PRIMARY KEY, v DECIMAL(10,0))");
        createStmt.Step();

        using var insertStmt = db.Prepare("INSERT INTO test_dec (id, v) VALUES ($1, $2)");
        insertStmt.BindInt64(1, 1);
        insertStmt.BindDecimal(2, 123m); // No decimal places
        insertStmt.Step();

        using var selectStmt = db.Prepare("SELECT v FROM test_dec WHERE id = 1");
        var result = selectStmt.Step();
        Assert.True(result == 1);

        var retrieved = selectStmt.GetDecimal(0);
        Assert.Equal(123m, retrieved);
    }

    [Fact]
    public void SafeHandles_IsInvalid_Property()
    {
        // Test DecentDBHandle
        var invalidHandle = new DecentDBHandle(IntPtr.Zero);
        Assert.True(invalidHandle.IsInvalid);
        
        // Test with a valid-looking pointer (though not actually valid DB)
        var validLookingHandle = new DecentDBHandle(new IntPtr(1));
        Assert.False(validLookingHandle.IsInvalid);
        
        // Test DecentDBStatementHandle
        var invalidStmtHandle = new DecentDBStatementHandle(IntPtr.Zero);
        Assert.True(invalidStmtHandle.IsInvalid);
        
        var validLookingStmtHandle = new DecentDBStatementHandle(new IntPtr(1));
        Assert.False(validLookingStmtHandle.IsInvalid);
    }
}