using System.Data;
using DecentDB.AdoNet;
using Xunit;

namespace DecentDB.Tests;

public sealed class DataReaderEdgeCoverageTests : IDisposable
{
    private readonly string _dbPath = Path.Combine(Path.GetTempPath(), $"test_reader_edge_{Guid.NewGuid():N}.ddb");

    [Fact]
    public void GetFieldValue_CoversAdditionalClrConversionsAndNullables()
    {
        using var connection = new DecentDBConnection($"Data Source={_dbPath}");
        connection.Open();

        var guid = Guid.NewGuid();
        var guidBytes = guid.ToByteArray();
        var micros = 1_700_000_000_123_456L;
        var daysSinceEpoch = 20_000L;
        var timeTicks = new TimeOnly(10, 11, 12, 123).Ticks;
        var spanTicks = TimeSpan.FromMinutes(7).Ticks;

        using (var setup = connection.CreateCommand())
        {
            setup.CommandText = """
                CREATE TABLE reader_conv (
                    c_char TEXT,
                    c_int INTEGER,
                    c_bool BOOLEAN,
                    c_real REAL,
                    c_blob BLOB,
                    c_ts INTEGER,
                    c_date INTEGER,
                    c_time INTEGER,
                    c_span INTEGER,
                    c_dec_text TEXT,
                    c_enum INTEGER,
                    c_null INTEGER
                );
                """;
            setup.ExecuteNonQuery();
        }

        using (var insert = connection.CreateCommand())
        {
            insert.CommandText = """
                INSERT INTO reader_conv (c_char, c_int, c_bool, c_real, c_blob, c_ts, c_date, c_time, c_span, c_dec_text, c_enum, c_null)
                VALUES ('Z', 7, TRUE, 3.5, @blob, @ts, @date, @time, @span, '123.45', 2, NULL);
                INSERT INTO reader_conv (c_char, c_int, c_bool, c_real, c_blob, c_ts, c_date, c_time, c_span, c_dec_text, c_enum, c_null)
                VALUES ('NO', 1, FALSE, 1.0, @blob, @ts, @date, @time, @span, '1.0', 1, NULL);
                """;
            insert.Parameters.Add(new DecentDBParameter("@blob", guidBytes));
            insert.Parameters.Add(new DecentDBParameter("@ts", micros));
            insert.Parameters.Add(new DecentDBParameter("@date", daysSinceEpoch));
            insert.Parameters.Add(new DecentDBParameter("@time", timeTicks));
            insert.Parameters.Add(new DecentDBParameter("@span", spanTicks));
            insert.ExecuteNonQuery();
        }

        using (var query = connection.CreateCommand())
        {
            query.CommandText = "SELECT * FROM reader_conv ORDER BY c_int DESC";
            using var reader = query.ExecuteReader();
            Assert.True(reader.Read());

            Assert.Equal('Z', reader.GetFieldValue<char>(0));
            Assert.Equal((sbyte)7, reader.GetFieldValue<sbyte>(1));
            Assert.Equal((short)7, reader.GetFieldValue<short>(1));
            Assert.Equal(7, reader.GetFieldValue<int>(1));
            Assert.Equal(7L, reader.GetFieldValue<long>(1));
            Assert.True(reader.GetFieldValue<bool>(2));
            Assert.Equal(3.5f, reader.GetFieldValue<float>(3));
            Assert.Equal(3.5, reader.GetFieldValue<double>(3));
            Assert.Equal(guidBytes, reader.GetFieldValue<byte[]>(4));
            Assert.Equal(guid, reader.GetFieldValue<Guid>(4));

            var expectedDateTime = new DateTime(micros * 10L + DateTime.UnixEpoch.Ticks, DateTimeKind.Utc);
            Assert.Equal(expectedDateTime, reader.GetFieldValue<DateTime>(5));
            Assert.Equal(new DateTimeOffset(expectedDateTime, TimeSpan.Zero), reader.GetFieldValue<DateTimeOffset>(5));

            var epochDate = DateOnly.FromDateTime(DateTime.UnixEpoch);
            Assert.Equal(epochDate.AddDays((int)daysSinceEpoch), reader.GetFieldValue<DateOnly>(6));
            Assert.Equal(new TimeOnly(timeTicks), reader.GetFieldValue<TimeOnly>(7));
            Assert.Equal(TimeSpan.FromTicks(spanTicks), reader.GetFieldValue<TimeSpan>(8));
            Assert.Equal(123.45m, reader.GetFieldValue<decimal>(9));
            Assert.Equal(DayOfWeek.Tuesday, reader.GetFieldValue<DayOfWeek>(10));
            Assert.Null(reader.GetFieldValue<int?>(11));

            var boxed = reader.GetFieldValue<object>(1);
            Assert.Equal(7L, Assert.IsType<long>(boxed));

            Assert.True(reader.Read());
            Assert.Throws<InvalidCastException>(() => reader.GetFieldValue<char>(0));
            Assert.False(reader.Read());
        }
    }

    [Fact]
    public async Task GetCharsAndReadAfterClose_CoverEdgePaths()
    {
        using var connection = new DecentDBConnection($"Data Source={_dbPath}");
        connection.Open();

        using (var setup = connection.CreateCommand())
        {
            setup.CommandText = """
                CREATE TABLE reader_chars (id INTEGER PRIMARY KEY, txt TEXT, data BLOB);
                """;
            setup.ExecuteNonQuery();
        }

        using (var insert = connection.CreateCommand())
        {
            insert.CommandText = "INSERT INTO reader_chars (id, txt, data) VALUES (1, 'abcdef', @data)";
            insert.Parameters.Add(new DecentDBParameter("@data", new byte[] { 0x01, 0x02, 0x03, 0x04 }));
            insert.ExecuteNonQuery();
        }

        using var query = connection.CreateCommand();
        query.CommandText = "SELECT txt, data FROM reader_chars WHERE id = 1";

        using var reader = query.ExecuteReader();
        Assert.True(reader.Read());

        Assert.Equal(6L, reader.GetChars(0, 0, null, 0, 0));
        var chars = new char[3];
        Assert.Equal(3L, reader.GetChars(0, 2, chars, 0, 3));
        Assert.Equal("cde".ToCharArray(), chars);

        Assert.Equal(4L, reader.GetBytes(1, 0, null, 0, 0));
        var bytes = new byte[2];
        Assert.Equal(2L, reader.GetBytes(1, 1, bytes, 0, 2));
        Assert.Equal(new byte[] { 0x02, 0x03 }, bytes);

        Assert.Equal('a', reader.GetChar(0));
        reader.Close();
        Assert.Throws<InvalidOperationException>(() => reader.Read());

        Assert.False(reader.NextResult());
        Assert.False(await reader.NextResultAsync(CancellationToken.None));
        await Assert.ThrowsAsync<InvalidOperationException>(async () => _ = await reader.ReadAsync(CancellationToken.None));
    }

    public void Dispose()
    {
        TryDelete(_dbPath);
        TryDelete(_dbPath + "-wal");
    }

    private static void TryDelete(string path)
    {
        if (File.Exists(path))
        {
            File.Delete(path);
        }
    }
}
