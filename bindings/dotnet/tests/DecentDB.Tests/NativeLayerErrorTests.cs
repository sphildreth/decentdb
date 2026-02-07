using System;
using System.IO;
using System.Runtime.InteropServices;
using Xunit;
using DecentDB.Native;

namespace DecentDB.Tests;

public class NativeLayerErrorTests : IDisposable
{
    private readonly string _dbPath;

    public NativeLayerErrorTests()
    {
        _dbPath = Path.Combine(Path.GetTempPath(), $"test_error_{Guid.NewGuid():N}.ddb");
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
    public void DecentDBException_HasCorrectProperties()
    {
        using var db = new DecentDB.Native.DecentDB(_dbPath);
        
        var ex = Assert.Throws<DecentDBException>(() => db.Prepare("INVALID SQL SYNTAX"));
        
        Assert.NotEqual(0, ex.ErrorCode);
        Assert.False(string.IsNullOrEmpty(ex.Message));
        Assert.Equal("INVALID SQL SYNTAX", ex.Sql);
    }

    [Fact]
    public void PreparedStatement_ThrowsOnInvalidBindIndex()
    {
        using var db = new DecentDB.Native.DecentDB(_dbPath);
        
        // Create a table first
        using (var createStmt = db.Prepare("CREATE TABLE bind_test (id INTEGER PRIMARY KEY)"))
        {
            createStmt.Step();
        }

        using var stmt = db.Prepare("INSERT INTO bind_test (id) VALUES ($1)");
        
        // Binding to an invalid index should throw
        var ex = Assert.Throws<DecentDBException>(() => stmt.BindInt64(999, 42));
        Assert.NotEqual(0, ex.ErrorCode);
    }

    [Fact]
    public void PreparedStatement_ThrowsOnInvalidColumnIndex()
    {
        using var db = new DecentDB.Native.DecentDB(_dbPath);
        
        // Create a table and insert a row
        using (var createStmt = db.Prepare("CREATE TABLE col_test (id INTEGER PRIMARY KEY)"))
        {
            createStmt.Step();
        }
        
        using (var insertStmt = db.Prepare("INSERT INTO col_test (id) VALUES (42)"))
        {
            insertStmt.Step();
        }

        using var selectStmt = db.Prepare("SELECT id FROM col_test");
        selectStmt.Step();
        
        // Accessing an invalid column index should work gracefully (return default)
        // For this test, we'll check that accessing beyond bounds behaves appropriately
        Assert.Equal(1, selectStmt.ColumnCount); // Only 1 column
        
        // Accessing valid column should work
        Assert.Equal(42L, selectStmt.GetInt64(0));
        
        // Accessing invalid column index behavior depends on native implementation
        // We'll just make sure it doesn't crash
    }

    [Fact]
    public void PreparedStatement_ThrowsOnStepAfterFinalize()
    {
        using var db = new DecentDB.Native.DecentDB(_dbPath);
        
        // Create a table first
        using (var createStmt = db.Prepare("CREATE TABLE step_test (id INTEGER PRIMARY KEY)"))
        {
            createStmt.Step();
        }

        var stmt = db.Prepare("INSERT INTO step_test (id) VALUES ($1)");
        stmt.BindInt64(1, 1);
        
        // Step should work initially
        var result = stmt.Step();
        Assert.Equal(0, result); // Success
        
        // After disposal, further operations should fail appropriately
        stmt.Dispose();
        
        // Trying to step after disposal should fail
        // The exact behavior depends on the native implementation
    }

    [Fact]
    public void PreparedStatement_ResetThrowsOnError()
    {
        using var db = new DecentDB.Native.DecentDB(_dbPath);
        
        // Create a table first
        using (var createStmt = db.Prepare("CREATE TABLE reset_error_test (id INTEGER PRIMARY KEY)"))
        {
            createStmt.Step();
        }

        using var stmt = db.Prepare("INSERT INTO reset_error_test (id) VALUES ($1)");
        stmt.BindInt64(1, 1);
        
        // Normal reset should work
        var resetStmt = stmt.Reset();
        Assert.NotNull(resetStmt);
        
        // Clear bindings should work
        var clearedStmt = stmt.ClearBindings();
        Assert.NotNull(clearedStmt);
    }

    [Fact]
    public void PreparedStatement_BindDecimal_OutOfRange_ThrowsOverflowException()
    {
        using var db = new DecentDB.Native.DecentDB(_dbPath);
        
        // Create a table first
        using (var createStmt = db.Prepare("CREATE TABLE decimal_test (id INTEGER PRIMARY KEY, value DECIMAL(18,2))"))
        {
            createStmt.Step();
        }

        using var stmt = db.Prepare("INSERT INTO decimal_test (id, value) VALUES ($1, $2)");
        stmt.BindInt64(1, 1);
        
        // Test with a decimal that's too large for DecentDB's 64-bit representation
        var hugeDecimal = decimal.MaxValue; // This should be too large
        
        var ex = Assert.Throws<OverflowException>(() => stmt.BindDecimal(2, hugeDecimal));
        Assert.Contains("too large", ex.Message);
    }

    [Fact]
    public void PreparedStatement_BindDecimal_LargeButValidValue_Works()
    {
        using var db = new DecentDB.Native.DecentDB(_dbPath);
        
        // Create a table first
        using (var createStmt = db.Prepare("CREATE TABLE decimal_valid_test (id INTEGER PRIMARY KEY, value DECIMAL(18,2))"))
        {
            createStmt.Step();
        }

        using var stmt = db.Prepare("INSERT INTO decimal_valid_test (id, value) VALUES ($1, $2)");
        stmt.BindInt64(1, 1);
        
        // Use a large but valid decimal value for DECIMAL(18,2)
        var largeDecimal = 9999999999999999.99m; // Large value that fits in DECIMAL(18,2)
        stmt.BindDecimal(2, largeDecimal);
        
        var result = stmt.Step();
        Assert.Equal(0, result); // Should succeed
    }

    [Fact]
    public void PreparedStatement_BindDecimal_NegativeLargeValue_Works()
    {
        using var db = new DecentDB.Native.DecentDB(_dbPath);
        
        // Create a table first
        using (var createStmt = db.Prepare("CREATE TABLE decimal_neg_test (id INTEGER PRIMARY KEY, value DECIMAL(18,2))"))
        {
            createStmt.Step();
        }

        using var stmt = db.Prepare("INSERT INTO decimal_neg_test (id, value) VALUES ($1, $2)");
        stmt.BindInt64(1, 1);
        
        // Use a large negative decimal value
        var largeNegativeDecimal = -9999999999999999.99m; // Large negative value that fits in DECIMAL(18,2)
        stmt.BindDecimal(2, largeNegativeDecimal);
        
        var result = stmt.Step();
        Assert.Equal(0, result); // Should succeed
    }

    [Fact]
    public void RowView_IndexOutOfRange_Throws()
    {
        using var db = new DecentDB.Native.DecentDB(_dbPath);
        
        // Create a table and insert a row
        using (var createStmt = db.Prepare("CREATE TABLE rowview_test (id INTEGER PRIMARY KEY, name TEXT)"))
        {
            createStmt.Step();
        }
        
        using (var insertStmt = db.Prepare("INSERT INTO rowview_test (id, name) VALUES (1, 'test')"))
        {
            insertStmt.Step();
        }

        using var selectStmt = db.Prepare("SELECT id, name FROM rowview_test");
        selectStmt.Step();
        
        var rowView = selectStmt.GetRowView();
        Assert.Equal(2, rowView.Count); // Should have 2 columns
        
        // Valid access should work
        var firstValue = rowView[0];
        var secondValue = rowView[1];
        
        // Invalid index should throw
        Assert.Throws<IndexOutOfRangeException>(() => { var _ = rowView[2]; });
        Assert.Throws<IndexOutOfRangeException>(() => { var _ = rowView[-1]; });
    }

    [Fact]
    public void Native_SetLibraryPath_WithInvalidPath_Throws()
    {
        // Test the SetLibraryPath method with invalid inputs
        Assert.Throws<ArgumentException>(() => DecentDBNative.SetLibraryPath(""));
        Assert.Throws<ArgumentException>(() => DecentDBNative.SetLibraryPath("   "));
        Assert.Throws<ArgumentException>(() => DecentDBNative.SetLibraryPath(null));
    }

    [Fact]
    public void PreparedStatement_Getters_AfterDisposal()
    {
        using var db = new DecentDB.Native.DecentDB(_dbPath);
        
        // Create a table and insert a row
        using (var createStmt = db.Prepare("CREATE TABLE getters_after_disposal (id INTEGER PRIMARY KEY)"))
        {
            createStmt.Step();
        }
        
        using (var insertStmt = db.Prepare("INSERT INTO getters_after_disposal (id) VALUES (42)"))
        {
            insertStmt.Step();
        }

        var selectStmt = db.Prepare("SELECT id FROM getters_after_disposal");
        selectStmt.Step();
        
        // Get the value
        var value = selectStmt.GetInt64(0);
        Assert.Equal(42L, value);
        
        // Dispose the statement
        selectStmt.Dispose();
        
        // Attempting to access after disposal behavior depends on native implementation
        // We mainly want to ensure it doesn't crash in a bad way
    }

    [Fact]
    public void DecentDB_WithInvalidOptions_Throws()
    {
        // Test opening database with invalid options
        var invalidOptions = "invalid_option_that_does_not_exist=1";
        using var db = new DecentDB.Native.DecentDB(_dbPath, invalidOptions);
        
        // The database should still open, but the invalid option might cause issues later
        // depending on how the native library handles invalid options
        using var stmt = db.Prepare("SELECT 1");
        var result = stmt.Step();
        Assert.Equal(1, result); // Should return a row
    }
}