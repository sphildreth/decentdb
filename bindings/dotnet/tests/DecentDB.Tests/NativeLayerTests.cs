using System;
using System.IO;
using Xunit;
using DecentDBException = DecentDB.Native.DecentDBException;
using NativeDb = DecentDB.Native.DecentDB;

namespace DecentDB.Tests;

public class NativeLayerTests : IDisposable
{
    private readonly string _dbPath;

    public NativeLayerTests()
    {
        _dbPath = Path.Combine(Path.GetTempPath(), $"test_{Guid.NewGuid():N}.ddb");
    }

    public void Dispose()
    {
        if (File.Exists(_dbPath))
            File.Delete(_dbPath);
        var walPath = _dbPath + "-wal";
        if (File.Exists(walPath))
            File.Delete(walPath);
    }

    private static void EnsureOneRowTable(NativeDb db)
    {
        using var create = db.Prepare("CREATE TABLE one_row (id INTEGER PRIMARY KEY)");
        var createRes = create.Step();
        Assert.True(createRes == 0, $"Expected CREATE TABLE step=0, got {createRes}. LastError={db.LastErrorCode}: {db.LastErrorMessage}");

        using var insert = db.Prepare("INSERT INTO one_row (id) VALUES (1)");
        var insertRes = insert.Step();
        Assert.True(insertRes == 0, $"Expected INSERT step=0, got {insertRes}. LastError={db.LastErrorCode}: {db.LastErrorMessage}");
    }

    private static void AssertStepRow(int stepResult, NativeDb db, string sql)
    {
        Assert.True(stepResult == 1, $"Expected step=1 for '{sql}', got {stepResult}. LastError={db.LastErrorCode}: {db.LastErrorMessage}");
    }

    [Fact]
    public void OpenNativeDatabase()
    {
        using var db = new NativeDb(_dbPath);
        Assert.False(db.Handle == IntPtr.Zero);
    }

    [Fact]
    public void OpenInvalidPathThrows()
    {
        var badPath = Path.Combine(Path.GetTempPath(), $"does_not_exist_{Guid.NewGuid():N}", "x.ddb");
        var ex = Assert.Throws<DecentDBException>(() => new NativeDb(badPath));
        Assert.False(string.IsNullOrWhiteSpace(ex.Message));
    }

    [Fact]
    public void PrepareStatement()
    {
        using var db = new NativeDb(_dbPath);
        using var stmt = db.Prepare("SELECT 1");
        Assert.False(stmt.Handle == IntPtr.Zero);
    }

    [Fact]
    public void BindAndStep()
    {
        using var db = new NativeDb(_dbPath);
        EnsureOneRowTable(db);
        const string sql = "SELECT $1 + $2 FROM one_row WHERE id = 1";
        using var stmt = db.Prepare(sql);
        stmt.BindInt64(1, 10);
        stmt.BindInt64(2, 20);

        var result = stmt.Step();
        AssertStepRow(result, db, sql);

        Assert.Equal(30, stmt.GetInt64(0));
    }

    [Fact]
    public void ColumnMetadata()
    {
        using var db = new NativeDb(_dbPath);
        EnsureOneRowTable(db);
        const string sql = "SELECT 1 AS col1, 'hello' AS col2, 3.14 AS col3 FROM one_row WHERE id = 1";
        using var stmt = db.Prepare(sql);

        Assert.Equal(3, stmt.ColumnCount);
        Assert.Equal("col1", stmt.ColumnName(0));
        Assert.Equal("col2", stmt.ColumnName(1));
        Assert.Equal("col3", stmt.ColumnName(2));

        var stepResult = stmt.Step();
        AssertStepRow(stepResult, db, sql);

        Assert.Equal(1, stmt.ColumnType(0));
        Assert.Equal(4, stmt.ColumnType(1));
        Assert.Equal(3, stmt.ColumnType(2));
    }

    [Fact]
    public void NullHandling()
    {
        using var db = new NativeDb(_dbPath);
        EnsureOneRowTable(db);
        const string sql = "SELECT NULL, $1 FROM one_row WHERE id = 1";
        using var stmt = db.Prepare(sql);
        stmt.BindNull(1);

        var result = stmt.Step();
        AssertStepRow(result, db, sql);

        Assert.True(stmt.IsNull(0));
        Assert.True(stmt.IsNull(1));
    }

    [Fact]
    public void TextBindingAndRetrieval()
    {
        using var db = new NativeDb(_dbPath);
        EnsureOneRowTable(db);
        const string sql = "SELECT $1 FROM one_row WHERE id = 1";
        using var stmt = db.Prepare(sql);
        var testString = "Hello, World! 你好 🌍";
        stmt.BindText(1, testString);

        var result = stmt.Step();
        AssertStepRow(result, db, sql);

        var retrieved = stmt.GetText(0);
        Assert.Equal(testString, retrieved);
    }

    [Fact]
    public void BlobBindingAndRetrieval()
    {
        using var db = new NativeDb(_dbPath);
        EnsureOneRowTable(db);
        const string sql = "SELECT $1 FROM one_row WHERE id = 1";
        using var stmt = db.Prepare(sql);
        var data = new byte[] { 0x01, 0x02, 0x03, 0xFF, 0xFE };
        stmt.BindBlob(1, data);

        var result = stmt.Step();
        AssertStepRow(result, db, sql);

        var retrieved = stmt.GetBlob(0);
        Assert.Equal(data, retrieved);
    }

    [Fact]
    public void FloatBindingAndRetrieval()
    {
        using var db = new NativeDb(_dbPath);
        EnsureOneRowTable(db);
        const string sql = "SELECT $1 FROM one_row WHERE id = 1";
        using var stmt = db.Prepare(sql);
        var value = 3.14159265359;
        stmt.BindFloat64(1, value);

        var result = stmt.Step();
        AssertStepRow(result, db, sql);

        var retrieved = stmt.GetFloat64(0);
        Assert.Equal(value, retrieved, 10);
    }

    [Fact]
    public void MultipleRows()
    {
        using var db = new NativeDb(_dbPath);
        EnsureOneRowTable(db);
        const string sql = "SELECT $1 FROM one_row WHERE id = 1";

        for (int i = 1; i <= 10; i++)
        {
            using var stmt = db.Prepare(sql);
            stmt.BindInt64(1, i);
            var result = stmt.Step();
            AssertStepRow(result, db, sql);
            Assert.Equal(i, stmt.GetInt64(0));
        }
    }

    [Fact]
    public void RowView()
    {
        using var db = new NativeDb(_dbPath);
        EnsureOneRowTable(db);
        const string sql = "SELECT $1, $2, $3 FROM one_row WHERE id = 1";
        using var stmt = db.Prepare(sql);
        stmt.BindInt64(1, 42);
        stmt.BindText(2, "hello");
        stmt.BindFloat64(3, 3.14);

        var result = stmt.Step();
        AssertStepRow(result, db, sql);

        var view = stmt.GetRowView();
        Assert.Equal(3, view.Count);

        Assert.Equal(1, view[0].kind);
        Assert.Equal(0, view[0].is_null);
        Assert.Equal(42, view[0].int64_val);

        Assert.Equal(4, view[1].kind);
        Assert.Equal(0, view[1].is_null);
    }

    [Fact]
    public void RowsAffected()
    {
        using var db = new NativeDb(_dbPath);
        using var stmt = db.Prepare("CREATE TABLE test (id INTEGER PRIMARY KEY, value TEXT)");
        stmt.Step();

        using var insertStmt = db.Prepare("INSERT INTO test (id, value) VALUES ($1, $2)");
        insertStmt.BindInt64(1, 1);
        insertStmt.BindText(2, "test1");
        insertStmt.Step();

        Assert.Equal(1, insertStmt.RowsAffected);

        insertStmt.Reset().ClearBindings().BindInt64(1, 2).BindText(2, "test2");
        insertStmt.Step();
        Assert.Equal(1, insertStmt.RowsAffected);
    }

    [Fact]
    public void ErrorHandling()
    {
        using var db = new NativeDb(_dbPath);
        Assert.Throws<DecentDBException>(() => db.Prepare("INVALID SQL SYNTAX"));
    }

    [Fact]
    public void DoubleBindNull()
    {
        using var db = new NativeDb(_dbPath);
        EnsureOneRowTable(db);
        const string sql = "SELECT $1 FROM one_row WHERE id = 1";
        using var stmt = db.Prepare(sql);
        stmt.BindNull(1);
        var stepResult = stmt.Step();
        AssertStepRow(stepResult, db, sql);

        Assert.True(stmt.IsNull(0));
    }
}
