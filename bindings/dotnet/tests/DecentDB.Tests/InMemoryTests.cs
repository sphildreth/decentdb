using System;
using System.Data;
using Xunit;
using DecentDB.AdoNet;

namespace DecentDB.Tests;

public class InMemoryTests
{
    private const string MemoryConnectionString = "Data Source=:memory:";

    [Fact]
    public void OpenAndClose_MemoryDatabase()
    {
        using var conn = new DecentDBConnection(MemoryConnectionString);
        Assert.Equal(ConnectionState.Closed, conn.State);
        conn.Open();
        Assert.Equal(ConnectionState.Open, conn.State);
        Assert.Equal(":memory:", conn.DataSource);
        conn.Close();
        Assert.Equal(ConnectionState.Closed, conn.State);
    }

    [Fact]
    public void DataSource_IsMemory()
    {
        using var conn = new DecentDBConnection(MemoryConnectionString);
        Assert.Equal(":memory:", conn.DataSource);
    }

    [Fact]
    public void CreateTable_And_Insert()
    {
        using var conn = new DecentDBConnection(MemoryConnectionString);
        conn.Open();

        using (var cmd = conn.CreateCommand())
        {
            cmd.CommandText = "CREATE TABLE items (id INTEGER PRIMARY KEY, name TEXT NOT NULL)";
            cmd.ExecuteNonQuery();
        }

        using (var cmd = conn.CreateCommand())
        {
            cmd.CommandText = "INSERT INTO items (id, name) VALUES (1, 'alpha')";
            cmd.ExecuteNonQuery();
        }

        using (var cmd = conn.CreateCommand())
        {
            cmd.CommandText = "SELECT name FROM items WHERE id = 1";
            var result = cmd.ExecuteScalar();
            Assert.Equal("alpha", result);
        }
    }

    [Fact]
    public void MultipleInserts_And_Count()
    {
        using var conn = new DecentDBConnection(MemoryConnectionString);
        conn.Open();

        using (var cmd = conn.CreateCommand())
        {
            cmd.CommandText = "CREATE TABLE nums (val INTEGER NOT NULL)";
            cmd.ExecuteNonQuery();
        }

        for (int i = 0; i < 50; i++)
        {
            using var cmd = conn.CreateCommand();
            cmd.CommandText = $"INSERT INTO nums (val) VALUES ({i})";
            cmd.ExecuteNonQuery();
        }

        using (var cmd = conn.CreateCommand())
        {
            cmd.CommandText = "SELECT COUNT(*) FROM nums";
            var count = Convert.ToInt64(cmd.ExecuteScalar());
            Assert.Equal(50, count);
        }
    }

    [Fact]
    public void DataReader_MultipleColumns()
    {
        using var conn = new DecentDBConnection(MemoryConnectionString);
        conn.Open();

        using (var cmd = conn.CreateCommand())
        {
            cmd.CommandText = "CREATE TABLE people (id INTEGER PRIMARY KEY, name TEXT, age INTEGER)";
            cmd.ExecuteNonQuery();
        }

        using (var cmd = conn.CreateCommand())
        {
            cmd.CommandText = "INSERT INTO people (id, name, age) VALUES (1, 'Alice', 30)";
            cmd.ExecuteNonQuery();
        }

        using (var cmd = conn.CreateCommand())
        {
            cmd.CommandText = "INSERT INTO people (id, name, age) VALUES (2, 'Bob', 25)";
            cmd.ExecuteNonQuery();
        }

        using (var cmd = conn.CreateCommand())
        {
            cmd.CommandText = "SELECT id, name, age FROM people ORDER BY id";
            using var reader = cmd.ExecuteReader();

            Assert.True(reader.Read());
            Assert.Equal(1L, reader.GetInt64(0));
            Assert.Equal("Alice", reader.GetString(1));
            Assert.Equal(30L, reader.GetInt64(2));

            Assert.True(reader.Read());
            Assert.Equal(2L, reader.GetInt64(0));
            Assert.Equal("Bob", reader.GetString(1));
            Assert.Equal(25L, reader.GetInt64(2));

            Assert.False(reader.Read());
        }
    }

    [Fact]
    public void Transaction_Commit()
    {
        using var conn = new DecentDBConnection(MemoryConnectionString);
        conn.Open();

        using (var cmd = conn.CreateCommand())
        {
            cmd.CommandText = "CREATE TABLE txtest (id INTEGER PRIMARY KEY, val TEXT)";
            cmd.ExecuteNonQuery();
        }

        using (var tx = conn.BeginTransaction())
        {
            using var cmd = conn.CreateCommand();
            cmd.Transaction = tx;
            cmd.CommandText = "INSERT INTO txtest (id, val) VALUES (1, 'committed')";
            cmd.ExecuteNonQuery();
            tx.Commit();
        }

        using (var cmd = conn.CreateCommand())
        {
            cmd.CommandText = "SELECT val FROM txtest WHERE id = 1";
            Assert.Equal("committed", cmd.ExecuteScalar());
        }
    }

    [Fact]
    public void Transaction_Rollback()
    {
        using var conn = new DecentDBConnection(MemoryConnectionString);
        conn.Open();

        using (var cmd = conn.CreateCommand())
        {
            cmd.CommandText = "CREATE TABLE txtest2 (id INTEGER PRIMARY KEY, val TEXT)";
            cmd.ExecuteNonQuery();
        }

        using (var tx = conn.BeginTransaction())
        {
            using var cmd = conn.CreateCommand();
            cmd.Transaction = tx;
            cmd.CommandText = "INSERT INTO txtest2 (id, val) VALUES (1, 'rolled_back')";
            cmd.ExecuteNonQuery();
            tx.Rollback();
        }

        using (var cmd = conn.CreateCommand())
        {
            cmd.CommandText = "SELECT COUNT(*) FROM txtest2";
            Assert.Equal(0L, Convert.ToInt64(cmd.ExecuteScalar()));
        }
    }

    [Fact]
    public void Update_And_Delete()
    {
        using var conn = new DecentDBConnection(MemoryConnectionString);
        conn.Open();

        using (var cmd = conn.CreateCommand())
        {
            cmd.CommandText = "CREATE TABLE mutable (id INTEGER PRIMARY KEY, val TEXT)";
            cmd.ExecuteNonQuery();
        }

        using (var cmd = conn.CreateCommand())
        {
            cmd.CommandText = "INSERT INTO mutable (id, val) VALUES (1, 'original')";
            cmd.ExecuteNonQuery();
        }

        using (var cmd = conn.CreateCommand())
        {
            cmd.CommandText = "UPDATE mutable SET val = 'updated' WHERE id = 1";
            cmd.ExecuteNonQuery();
        }

        using (var cmd = conn.CreateCommand())
        {
            cmd.CommandText = "SELECT val FROM mutable WHERE id = 1";
            Assert.Equal("updated", cmd.ExecuteScalar());
        }

        using (var cmd = conn.CreateCommand())
        {
            cmd.CommandText = "DELETE FROM mutable WHERE id = 1";
            cmd.ExecuteNonQuery();
        }

        using (var cmd = conn.CreateCommand())
        {
            cmd.CommandText = "SELECT COUNT(*) FROM mutable";
            Assert.Equal(0L, Convert.ToInt64(cmd.ExecuteScalar()));
        }
    }

    [Fact]
    public void SecondaryIndex_Query()
    {
        using var conn = new DecentDBConnection(MemoryConnectionString);
        conn.Open();

        using (var cmd = conn.CreateCommand())
        {
            cmd.CommandText = "CREATE TABLE indexed (id INTEGER PRIMARY KEY, name TEXT)";
            cmd.ExecuteNonQuery();
        }

        using (var cmd = conn.CreateCommand())
        {
            cmd.CommandText = "CREATE INDEX idx_indexed_name ON indexed (name)";
            cmd.ExecuteNonQuery();
        }

        using (var cmd = conn.CreateCommand())
        {
            cmd.CommandText = "INSERT INTO indexed (id, name) VALUES (1, 'zebra')";
            cmd.ExecuteNonQuery();
        }

        using (var cmd = conn.CreateCommand())
        {
            cmd.CommandText = "INSERT INTO indexed (id, name) VALUES (2, 'apple')";
            cmd.ExecuteNonQuery();
        }

        using (var cmd = conn.CreateCommand())
        {
            cmd.CommandText = "SELECT name FROM indexed ORDER BY name";
            using var reader = cmd.ExecuteReader();
            Assert.True(reader.Read());
            Assert.Equal("apple", reader.GetString(0));
            Assert.True(reader.Read());
            Assert.Equal("zebra", reader.GetString(0));
        }
    }

    [Fact]
    public void ParameterizedQuery()
    {
        using var conn = new DecentDBConnection(MemoryConnectionString);
        conn.Open();

        using (var cmd = conn.CreateCommand())
        {
            cmd.CommandText = "CREATE TABLE paramtest (id INTEGER PRIMARY KEY, val TEXT)";
            cmd.ExecuteNonQuery();
        }

        using (var cmd = conn.CreateCommand())
        {
            cmd.CommandText = "INSERT INTO paramtest (id, val) VALUES ($1, $2)";
            cmd.Parameters.Add(new DecentDBParameter { Value = 1L });
            cmd.Parameters.Add(new DecentDBParameter { Value = "hello" });
            cmd.ExecuteNonQuery();
        }

        using (var cmd = conn.CreateCommand())
        {
            cmd.CommandText = "SELECT val FROM paramtest WHERE id = $1";
            cmd.Parameters.Add(new DecentDBParameter { Value = 1L });
            Assert.Equal("hello", cmd.ExecuteScalar());
        }
    }

    [Fact]
    public void Join_Query()
    {
        using var conn = new DecentDBConnection(MemoryConnectionString);
        conn.Open();

        using (var cmd = conn.CreateCommand())
        {
            cmd.CommandText = "CREATE TABLE authors (id INTEGER PRIMARY KEY, name TEXT NOT NULL)";
            cmd.ExecuteNonQuery();
        }

        using (var cmd = conn.CreateCommand())
        {
            cmd.CommandText = "CREATE TABLE books (id INTEGER PRIMARY KEY, title TEXT NOT NULL, author_id INTEGER NOT NULL)";
            cmd.ExecuteNonQuery();
        }

        using (var cmd = conn.CreateCommand())
        {
            cmd.CommandText = "INSERT INTO authors (id, name) VALUES (1, 'Tolkien')";
            cmd.ExecuteNonQuery();
        }

        using (var cmd = conn.CreateCommand())
        {
            cmd.CommandText = "INSERT INTO books (id, title, author_id) VALUES (1, 'The Hobbit', 1)";
            cmd.ExecuteNonQuery();
        }

        using (var cmd = conn.CreateCommand())
        {
            cmd.CommandText = "SELECT b.title, a.name FROM books b INNER JOIN authors a ON a.id = b.author_id";
            using var reader = cmd.ExecuteReader();
            Assert.True(reader.Read());
            Assert.Equal("The Hobbit", reader.GetString(0));
            Assert.Equal("Tolkien", reader.GetString(1));
            Assert.False(reader.Read());
        }
    }

    [Fact]
    public void SeparateConnections_AreIndependent()
    {
        using var conn1 = new DecentDBConnection(MemoryConnectionString);
        conn1.Open();

        using (var cmd = conn1.CreateCommand())
        {
            cmd.CommandText = "CREATE TABLE isolated (id INTEGER PRIMARY KEY)";
            cmd.ExecuteNonQuery();
        }

        // A second :memory: connection should be a completely separate database.
        using var conn2 = new DecentDBConnection(MemoryConnectionString);
        conn2.Open();

        using (var cmd = conn2.CreateCommand())
        {
            // This table should not exist in the second connection's database.
            cmd.CommandText = "CREATE TABLE isolated (id INTEGER PRIMARY KEY)";
            // Should succeed because it's a separate database — no conflict.
            cmd.ExecuteNonQuery();
        }
    }

    [Fact]
    public void Checkpoint_Succeeds()
    {
        using var conn = new DecentDBConnection(MemoryConnectionString);
        conn.Open();

        using (var cmd = conn.CreateCommand())
        {
            cmd.CommandText = "CREATE TABLE cktest (id INTEGER PRIMARY KEY)";
            cmd.ExecuteNonQuery();
        }

        using (var cmd = conn.CreateCommand())
        {
            cmd.CommandText = "INSERT INTO cktest (id) VALUES (1)";
            cmd.ExecuteNonQuery();
        }

        // Checkpoint should not throw for in-memory databases.
        conn.Checkpoint();
    }

    [Fact]
    public void CaseInsensitive_MemoryPath()
    {
        // ":MEMORY:" should also work (case-insensitive matching, like SQLite).
        using var conn = new DecentDBConnection("Data Source=:MEMORY:");
        conn.Open();

        using var cmd = conn.CreateCommand();
        cmd.CommandText = "SELECT 1";
        var result = cmd.ExecuteScalar();
        Assert.Equal(1L, result);
    }
}
