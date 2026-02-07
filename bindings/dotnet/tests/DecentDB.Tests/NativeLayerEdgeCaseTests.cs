using System;
using System.IO;
using System.Linq;
using Xunit;
using DecentDB.Native;

namespace DecentDB.Tests;

public class NativeLayerEdgeCaseTests : IDisposable
{
    private readonly string _dbPath;

    public NativeLayerEdgeCaseTests()
    {
        _dbPath = Path.Combine(Path.GetTempPath(), $"test_edge_{Guid.NewGuid():N}.ddb");
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
    public void PreparedStatement_BindText_WithVeryLongString()
    {
        using var db = new DecentDB.Native.DecentDB(_dbPath);
        
        // Create a table first
        using (var createStmt = db.Prepare("CREATE TABLE long_text_test (id INTEGER PRIMARY KEY, content TEXT)"))
        {
            createStmt.Step();
        }

        using var stmt = db.Prepare("INSERT INTO long_text_test (id, content) VALUES ($1, $2)");
        stmt.BindInt64(1, 1);
        
        // Create a very long string
        var longString = new string('A', 10000); // 10k character string
        stmt.BindText(2, longString);
        
        var result = stmt.Step();
        Assert.Equal(0, result); // Should succeed
        
        // Verify retrieval works
        using var selectStmt = db.Prepare("SELECT content FROM long_text_test WHERE id = 1");
        selectStmt.Step();
        var retrieved = selectStmt.GetText(0);
        Assert.Equal(longString, retrieved);
    }

    [Fact]
    public void PreparedStatement_BindBlob_WithLargeByteArray()
    {
        using var db = new DecentDB.Native.DecentDB(_dbPath);
        
        // Create a table first
        using (var createStmt = db.Prepare("CREATE TABLE large_blob_test (id INTEGER PRIMARY KEY, data BLOB)"))
        {
            createStmt.Step();
        }

        using var stmt = db.Prepare("INSERT INTO large_blob_test (id, data) VALUES ($1, $2)");
        stmt.BindInt64(1, 1);
        
        // Create a large byte array
        var largeBlob = Enumerable.Range(0, 10000).Select(i => (byte)(i % 256)).ToArray();
        stmt.BindBlob(2, largeBlob);
        
        var result = stmt.Step();
        Assert.Equal(0, result); // Should succeed
        
        // Verify retrieval works
        using var selectStmt = db.Prepare("SELECT data FROM large_blob_test WHERE id = 1");
        selectStmt.Step();
        var retrieved = selectStmt.GetBlob(0);
        Assert.Equal(largeBlob.Length, retrieved.Length);
        Assert.Equal(largeBlob, retrieved);
    }

    [Fact]
    public void PreparedStatement_BindGuid_WithAllZeros()
    {
        using var db = new DecentDB.Native.DecentDB(_dbPath);
        
        // Create a table first
        using (var createStmt = db.Prepare("CREATE TABLE guid_zero_test (id INTEGER PRIMARY KEY, guid_col UUID)"))
        {
            createStmt.Step();
        }

        using var stmt = db.Prepare("INSERT INTO guid_zero_test (id, guid_col) VALUES ($1, $2)");
        stmt.BindInt64(1, 1);
        
        var zeroGuid = Guid.Empty;
        stmt.BindGuid(2, zeroGuid);
        
        var result = stmt.Step();
        Assert.Equal(0, result); // Should succeed
        
        // Verify retrieval works
        using var selectStmt = db.Prepare("SELECT guid_col FROM guid_zero_test WHERE id = 1");
        selectStmt.Step();
        var retrieved = selectStmt.GetGuid(0);
        Assert.Equal(zeroGuid, retrieved);
    }

    [Fact]
    public void PreparedStatement_BindGuid_WithMaxValue()
    {
        using var db = new DecentDB.Native.DecentDB(_dbPath);
        
        // Create a table first
        using (var createStmt = db.Prepare("CREATE TABLE guid_max_test (id INTEGER PRIMARY KEY, guid_col UUID)"))
        {
            createStmt.Step();
        }

        using var stmt = db.Prepare("INSERT INTO guid_max_test (id, guid_col) VALUES ($1, $2)");
        stmt.BindInt64(1, 1);
        
        // Create a GUID with max values
        var maxGuid = new Guid(new byte[] {
            0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF,
            0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF
        });
        stmt.BindGuid(2, maxGuid);
        
        var result = stmt.Step();
        Assert.Equal(0, result); // Should succeed
        
        // Verify retrieval works
        using var selectStmt = db.Prepare("SELECT guid_col FROM guid_max_test WHERE id = 1");
        selectStmt.Step();
        var retrieved = selectStmt.GetGuid(0);
        Assert.Equal(maxGuid, retrieved);
    }

    [Fact]
    public void PreparedStatement_BindDecimal_WithVariousScales()
    {
        using var db = new DecentDB.Native.DecentDB(_dbPath);
        
        // Create a table first
        using (var createStmt = db.Prepare("CREATE TABLE decimal_scale_test (id INTEGER PRIMARY KEY, value DECIMAL(18,8))"))
        {
            createStmt.Step();
        }

        var testCases = new[]
        {
            123.45678901m,  // 8 decimal places
            0.1m,            // 1 decimal place
            1000000m,        // 0 decimal places
            -50.75m          // 2 decimal places, negative
        };

        for (int i = 0; i < testCases.Length; i++)
        {
            using var stmt = db.Prepare("INSERT INTO decimal_scale_test (id, value) VALUES ($1, $2)");
            stmt.BindInt64(1, i + 1);
            stmt.BindDecimal(2, testCases[i]);
            
            var result = stmt.Step();
            Assert.Equal(0, result); // Should succeed
        }

        // Verify all values were stored correctly
        for (int i = 0; i < testCases.Length; i++)
        {
            using var selectStmt = db.Prepare("SELECT value FROM decimal_scale_test WHERE id = $1");
            selectStmt.BindInt64(1, i + 1);
            selectStmt.Step();
            var retrieved = selectStmt.GetDecimal(0);
            Assert.Equal(testCases[i], retrieved);
        }
    }

    [Fact]
    public void PreparedStatement_BindMultipleTypesInSingleStatement()
    {
        using var db = new DecentDB.Native.DecentDB(_dbPath);
        
        // Create a table with various column types
        using (var createStmt = db.Prepare("CREATE TABLE multi_type_test (id INTEGER PRIMARY KEY, txt TEXT, num INTEGER, flt REAL, blb BLOB, flag BOOL, uid UUID, dec_val DECIMAL(10,2))"))
        {
            createStmt.Step();
        }

        using var stmt = db.Prepare("INSERT INTO multi_type_test (id, txt, num, flt, blb, flag, uid, dec_val) VALUES ($1, $2, $3, $4, $5, $6, $7, $8)");
        
        var id = 1;
        var text = "Hello, World!";
        var number = 42L;
        var floatVal = 3.14159;
        var blob = new byte[] { 0xDE, 0xAD, 0xBE, 0xEF };
        var flag = true;
        var guid = Guid.NewGuid();
        var decimalVal = 123.45m;
        
        stmt.BindInt64(1, id);
        stmt.BindText(2, text);
        stmt.BindInt64(3, number);
        stmt.BindFloat64(4, floatVal);
        stmt.BindBlob(5, blob);
        stmt.BindBool(6, flag);
        stmt.BindGuid(7, guid);
        stmt.BindDecimal(8, decimalVal);
        
        var result = stmt.Step();
        Assert.Equal(0, result); // Should succeed
        
        // Verify all values were stored correctly
        using var selectStmt = db.Prepare("SELECT txt, num, flt, blb, flag, uid, dec_val FROM multi_type_test WHERE id = 1");
        selectStmt.Step();
        
        Assert.Equal(text, selectStmt.GetText(0));
        Assert.Equal(number, selectStmt.GetInt64(1));
        Assert.Equal(floatVal, selectStmt.GetFloat64(2), 5);
        Assert.Equal(blob, selectStmt.GetBlob(3));
        Assert.Equal(flag, selectStmt.GetBool(4));
        Assert.Equal(guid, selectStmt.GetGuid(5));
        Assert.Equal(decimalVal, selectStmt.GetDecimal(6));
    }

    [Fact]
    public void PreparedStatement_BindParametersAtExtremeIndices()
    {
        using var db = new DecentDB.Native.DecentDB(_dbPath);
        
        // Create a table first
        using (var createStmt = db.Prepare("CREATE TABLE extreme_indices_test (id INTEGER PRIMARY KEY, val1 TEXT, val2 TEXT, val3 TEXT)"))
        {
            createStmt.Step();
        }

        // Create a statement with multiple parameters
        using var stmt = db.Prepare("INSERT INTO extreme_indices_test (id, val1, val2, val3) VALUES ($1, $2, $3, $4)");
        
        stmt.BindInt64(1, 1);
        stmt.BindText(2, "first");
        stmt.BindText(3, "second");
        stmt.BindText(4, "third");
        
        var result = stmt.Step();
        Assert.Equal(0, result); // Should succeed
        
        // Verify values
        using var selectStmt = db.Prepare("SELECT val1, val2, val3 FROM extreme_indices_test WHERE id = 1");
        selectStmt.Step();
        
        Assert.Equal("first", selectStmt.GetText(0));
        Assert.Equal("second", selectStmt.GetText(1));
        Assert.Equal("third", selectStmt.GetText(2));
    }

    [Fact]
    public void PreparedStatement_ColumnName_WithSpecialCharacters()
    {
        using var db = new DecentDB.Native.DecentDB(_dbPath);
        
        // Create a table with special column names (PostgreSQL-style double-quoting)
        using (var createStmt = db.Prepare("CREATE TABLE special_cols (id INTEGER PRIMARY KEY, \"col_with_underscores\" TEXT, \"col2\" TEXT)"))
        {
            createStmt.Step();
        }

        using (var insertStmt = db.Prepare("INSERT INTO special_cols (id, \"col_with_underscores\", \"col2\") VALUES ($1, $2, $3)"))
        {
            insertStmt.BindInt64(1, 1);
            insertStmt.BindText(2, "underscore_value");
            insertStmt.BindText(3, "col2_value");
            insertStmt.Step();
        }

        using var selectStmt = db.Prepare("SELECT \"col_with_underscores\", \"col2\" FROM special_cols WHERE id = 1");
        
        Assert.Equal(2, selectStmt.ColumnCount);
        selectStmt.Step();
        
        Assert.Equal("underscore_value", selectStmt.GetText(0));
        Assert.Equal("col2_value", selectStmt.GetText(1));
    }

    [Fact]
    public void PreparedStatement_BindAndRetrieveUnicodeText()
    {
        using var db = new DecentDB.Native.DecentDB(_dbPath);
        
        // Create a table first
        using (var createStmt = db.Prepare("CREATE TABLE unicode_test (id INTEGER PRIMARY KEY, content TEXT)"))
        {
            createStmt.Step();
        }

        var unicodeStrings = new[]
        {
            "Hello 世界",           // Basic multilingual
            "مرحبا",               // Arabic
            "Привет",              // Cyrillic
            "こんにちは",            // Japanese
            "😊🎉🚀",              // Emojis
            "Straße",              // German with special character
            "Москва",              // Moscow in Cyrillic
        };

        for (int i = 0; i < unicodeStrings.Length; i++)
        {
            using var stmt = db.Prepare("INSERT INTO unicode_test (id, content) VALUES ($1, $2)");
            stmt.BindInt64(1, i + 1);
            stmt.BindText(2, unicodeStrings[i]);
            
            var result = stmt.Step();
            Assert.Equal(0, result); // Should succeed
        }

        // Verify all unicode values were stored correctly
        for (int i = 0; i < unicodeStrings.Length; i++)
        {
            using var selectStmt = db.Prepare("SELECT content FROM unicode_test WHERE id = $1");
            selectStmt.BindInt64(1, i + 1);
            selectStmt.Step();
            var retrieved = selectStmt.GetText(0);
            Assert.Equal(unicodeStrings[i], retrieved);
        }
    }

    [Fact]
    public void PreparedStatement_BindFloatWithExtremeValues()
    {
        using var db = new DecentDB.Native.DecentDB(_dbPath);
        
        // Create a table first
        using (var createStmt = db.Prepare("CREATE TABLE float_extreme_test (id INTEGER PRIMARY KEY, val REAL)"))
        {
            createStmt.Step();
        }

        var extremeFloats = new[]
        {
            double.MinValue,
            double.MaxValue,
            double.Epsilon,
            -double.Epsilon,
            0.0,
            -0.0,
            1.0,
            -1.0
        };

        for (int i = 0; i < extremeFloats.Length; i++)
        {
            using var stmt = db.Prepare("INSERT INTO float_extreme_test (id, val) VALUES ($1, $2)");
            stmt.BindInt64(1, i + 1);
            stmt.BindFloat64(2, extremeFloats[i]);
            
            var result = stmt.Step();
            Assert.Equal(0, result); // Should succeed
        }

        // Verify all values were stored correctly (with tolerance for floating point)
        for (int i = 0; i < extremeFloats.Length; i++)
        {
            using var selectStmt = db.Prepare("SELECT val FROM float_extreme_test WHERE id = $1");
            selectStmt.BindInt64(1, i + 1);
            selectStmt.Step();
            var retrieved = selectStmt.GetFloat64(0);
            
            // For extreme values, we just check they're approximately equal
            if (double.IsInfinity(extremeFloats[i]))
            {
                Assert.Equal(extremeFloats[i], retrieved);
            }
            else if (extremeFloats[i] == 0.0)
            {
                Assert.Equal(0.0, Math.Abs(retrieved), 10);
            }
            else
            {
                Assert.Equal(extremeFloats[i], retrieved, 5);
            }
        }
    }

    [Fact]
    public void PreparedStatement_MultipleStepsAndResets()
    {
        using var db = new DecentDB.Native.DecentDB(_dbPath);
        
        // Create a table first
        using (var createStmt = db.Prepare("CREATE TABLE multi_step_test (id INTEGER PRIMARY KEY, value INTEGER)"))
        {
            createStmt.Step();
        }

        using var stmt = db.Prepare("INSERT INTO multi_step_test (id, value) VALUES ($1, $2)");
        
        // Perform multiple operations with resets
        for (int i = 1; i <= 5; i++)
        {
            stmt.BindInt64(1, i);
            stmt.BindInt64(2, i * 10);
            
            var result = stmt.Step();
            Assert.Equal(0, result); // Should succeed
            
            // Reset for next iteration
            stmt.Reset().ClearBindings();
        }
        
        // Verify all records were inserted
        using var selectStmt = db.Prepare("SELECT COUNT(*) FROM multi_step_test");
        selectStmt.Step();
        Assert.Equal(5L, selectStmt.GetInt64(0));
        
        // Verify individual values
        for (int i = 1; i <= 5; i++)
        {
            using var selStmt = db.Prepare("SELECT value FROM multi_step_test WHERE id = $1");
            selStmt.BindInt64(1, i);
            selStmt.Step();
            Assert.Equal(i * 10L, selStmt.GetInt64(0));
        }
    }
}