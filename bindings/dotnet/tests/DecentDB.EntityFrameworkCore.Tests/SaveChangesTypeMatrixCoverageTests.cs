using DecentDB.AdoNet;
using DecentDB.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore;
using Xunit;

namespace DecentDB.EntityFrameworkCore.Tests;

public sealed class SaveChangesTypeMatrixCoverageTests : IDisposable
{
    private readonly string _dbPath = Path.Combine(Path.GetTempPath(), $"test_ef_type_matrix_{Guid.NewGuid():N}.ddb");

    [Fact]
    public void SaveChanges_BindsCommonProviderTypes_ForInsertUpdateDelete()
    {
        EnsureSchema();

        using (var context = CreateContext())
        {
            var row = new TypeMatrixRow
            {
                ByteValue = 1,
                ShortValue = 2,
                IntValue = 3,
                LongValue = 4,
                UShortValue = 5,
                UIntValue = 6,
                ULongValue = 7,
                FloatValue = 1.25f,
                DoubleValue = 2.5d,
                DecimalValue = 123.4567m,
                BoolValue = true,
                TextValue = "initial",
                DateTimeValue = DateTime.SpecifyKind(new DateTime(2024, 1, 2, 3, 4, 5), DateTimeKind.Utc),
                DateTimeOffsetValue = new DateTimeOffset(2024, 1, 2, 3, 4, 5, TimeSpan.Zero),
                TimeSpanValue = TimeSpan.FromMinutes(30),
                DateOnlyValue = new DateOnly(2024, 1, 2),
                TimeOnlyValue = new TimeOnly(3, 4, 5),
                BlobValue = [1, 2, 3],
                GuidValue = Guid.NewGuid(),
                State = MatrixState.Active,
                NullableText = "set"
            };

            context.Rows.Add(row);
            context.SaveChanges();

            Assert.True(row.Id > 0);

            row.TextValue = "updated";
            row.UIntValue = 66;
            row.ULongValue = 77;
            row.DecimalValue = 987.6543m;
            row.NullableText = null;
            row.State = MatrixState.Archived;
            context.SaveChanges();
        }

        using (var context = CreateContext())
        {
            var loaded = context.Rows.AsNoTracking().Single();
            Assert.Equal("updated", loaded.TextValue);
            Assert.Equal((uint)66, loaded.UIntValue);
            Assert.Equal((ulong)77, loaded.ULongValue);
            Assert.Equal(987.6543m, loaded.DecimalValue);
            Assert.Null(loaded.NullableText);
            Assert.Equal(MatrixState.Archived, loaded.State);
            Assert.Equal(new byte[] { 1, 2, 3 }, loaded.BlobValue);
        }

        using (var context = CreateContext())
        {
            context.Rows.Remove(context.Rows.Single());
            context.SaveChanges();
            Assert.Empty(context.Rows);
        }
    }

    public void Dispose()
    {
        TryDelete(_dbPath);
        TryDelete(_dbPath + "-wal");
    }

    private MatrixDbContext CreateContext()
    {
        var options = new DbContextOptionsBuilder<MatrixDbContext>()
            .UseDecentDB($"Data Source={_dbPath}")
            .Options;
        return new MatrixDbContext(options);
    }

    private void EnsureSchema()
    {
        using var connection = new DecentDBConnection($"Data Source={_dbPath}");
        connection.Open();

        using var command = connection.CreateCommand();
        command.CommandText = "DROP TABLE IF EXISTS ef_type_matrix;";
        command.ExecuteNonQuery();
        command.CommandText = """
            CREATE TABLE ef_type_matrix (
              id INTEGER PRIMARY KEY,
              byte_value INTEGER NOT NULL,
              short_value INTEGER NOT NULL,
              int_value INTEGER NOT NULL,
              long_value INTEGER NOT NULL,
              ushort_value INTEGER NOT NULL,
              uint_value INTEGER NOT NULL,
              ulong_value INTEGER NOT NULL,
              float_value REAL NOT NULL,
              double_value REAL NOT NULL,
              decimal_value DECIMAL(18,4) NOT NULL,
              bool_value BOOLEAN NOT NULL,
              text_value TEXT NOT NULL,
              datetime_value TIMESTAMP NOT NULL,
              datetimeoffset_value TIMESTAMP NOT NULL,
              timespan_value INTEGER NOT NULL,
              dateonly_value INTEGER NOT NULL,
              timeonly_value INTEGER NOT NULL,
              blob_value BLOB NOT NULL,
              guid_value UUID NOT NULL,
              state_value INTEGER NOT NULL,
              nullable_text TEXT NULL
            );
            """;
        command.ExecuteNonQuery();
    }

    private static void TryDelete(string path)
    {
        if (File.Exists(path))
        {
            File.Delete(path);
        }
    }

    private sealed class MatrixDbContext(DbContextOptions<MatrixDbContext> options) : DbContext(options)
    {
        public DbSet<TypeMatrixRow> Rows => Set<TypeMatrixRow>();

        protected override void OnModelCreating(ModelBuilder modelBuilder)
        {
            modelBuilder.Entity<TypeMatrixRow>(entity =>
            {
                entity.ToTable("ef_type_matrix");
                entity.HasKey(x => x.Id);
                entity.Property(x => x.Id).HasColumnName("id").ValueGeneratedOnAdd();
                entity.Property(x => x.ByteValue).HasColumnName("byte_value");
                entity.Property(x => x.ShortValue).HasColumnName("short_value");
                entity.Property(x => x.IntValue).HasColumnName("int_value");
                entity.Property(x => x.LongValue).HasColumnName("long_value");
                entity.Property(x => x.UShortValue).HasColumnName("ushort_value");
                entity.Property(x => x.UIntValue).HasColumnName("uint_value");
                entity.Property(x => x.ULongValue).HasColumnName("ulong_value");
                entity.Property(x => x.FloatValue).HasColumnName("float_value");
                entity.Property(x => x.DoubleValue).HasColumnName("double_value");
                entity.Property(x => x.DecimalValue).HasColumnName("decimal_value").HasPrecision(18, 4);
                entity.Property(x => x.BoolValue).HasColumnName("bool_value");
                entity.Property(x => x.TextValue).HasColumnName("text_value");
                entity.Property(x => x.DateTimeValue).HasColumnName("datetime_value");
                entity.Property(x => x.DateTimeOffsetValue).HasColumnName("datetimeoffset_value");
                entity.Property(x => x.TimeSpanValue).HasColumnName("timespan_value");
                entity.Property(x => x.DateOnlyValue).HasColumnName("dateonly_value");
                entity.Property(x => x.TimeOnlyValue).HasColumnName("timeonly_value");
                entity.Property(x => x.BlobValue).HasColumnName("blob_value");
                entity.Property(x => x.GuidValue).HasColumnName("guid_value");
                entity.Property(x => x.State).HasColumnName("state_value");
                entity.Property(x => x.NullableText).HasColumnName("nullable_text");
            });
        }
    }

    private sealed class TypeMatrixRow
    {
        public int Id { get; set; }
        public byte ByteValue { get; set; }
        public short ShortValue { get; set; }
        public int IntValue { get; set; }
        public long LongValue { get; set; }
        public ushort UShortValue { get; set; }
        public uint UIntValue { get; set; }
        public ulong ULongValue { get; set; }
        public float FloatValue { get; set; }
        public double DoubleValue { get; set; }
        public decimal DecimalValue { get; set; }
        public bool BoolValue { get; set; }
        public string TextValue { get; set; } = string.Empty;
        public DateTime DateTimeValue { get; set; }
        public DateTimeOffset DateTimeOffsetValue { get; set; }
        public TimeSpan TimeSpanValue { get; set; }
        public DateOnly DateOnlyValue { get; set; }
        public TimeOnly TimeOnlyValue { get; set; }
        public byte[] BlobValue { get; set; } = [];
        public Guid GuidValue { get; set; }
        public MatrixState State { get; set; }
        public string? NullableText { get; set; }
    }

    private enum MatrixState
    {
        Unknown = 0,
        Active = 1,
        Archived = 2
    }
}
