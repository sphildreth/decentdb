using Microsoft.EntityFrameworkCore;

namespace DecentDB.EntityFrameworkCore;

/// <summary>
/// DecentDB-specific EF Core SQL function translations.
/// </summary>
public static class DecentDBDbFunctionsExtensions
{
    public static long RowNumber<TOrder>(
        this DbFunctions _,
        TOrder orderBy,
        bool descending = false)
        => throw new NotSupportedException("This method is only for use in LINQ translation.");

    public static long RowNumber<TPartition, TOrder>(
        this DbFunctions _,
        TPartition partitionBy,
        TOrder orderBy,
        bool descending = false)
        => throw new NotSupportedException("This method is only for use in LINQ translation.");

    public static long Rank<TOrder>(
        this DbFunctions _,
        TOrder orderBy,
        bool descending = false)
        => throw new NotSupportedException("This method is only for use in LINQ translation.");

    public static long Rank<TPartition, TOrder>(
        this DbFunctions _,
        TPartition partitionBy,
        TOrder orderBy,
        bool descending = false)
        => throw new NotSupportedException("This method is only for use in LINQ translation.");

    public static long DenseRank<TOrder>(
        this DbFunctions _,
        TOrder orderBy,
        bool descending = false)
        => throw new NotSupportedException("This method is only for use in LINQ translation.");

    public static long DenseRank<TPartition, TOrder>(
        this DbFunctions _,
        TPartition partitionBy,
        TOrder orderBy,
        bool descending = false)
        => throw new NotSupportedException("This method is only for use in LINQ translation.");

    public static double PercentRank<TOrder>(
        this DbFunctions _,
        TOrder orderBy,
        bool descending = false)
        => throw new NotSupportedException("This method is only for use in LINQ translation.");

    public static double PercentRank<TPartition, TOrder>(
        this DbFunctions _,
        TPartition partitionBy,
        TOrder orderBy,
        bool descending = false)
        => throw new NotSupportedException("This method is only for use in LINQ translation.");

    public static TValue? Lag<TValue, TOrder>(
        this DbFunctions _,
        TValue value,
        TOrder orderBy,
        TValue? defaultValue,
        int offset = 1,
        bool descending = false)
        => throw new NotSupportedException("This method is only for use in LINQ translation.");

    public static TValue? Lag<TPartition, TValue, TOrder>(
        this DbFunctions _,
        TPartition partitionBy,
        TValue value,
        TOrder orderBy,
        TValue? defaultValue,
        int offset = 1,
        bool descending = false)
        => throw new NotSupportedException("This method is only for use in LINQ translation.");

    public static TValue? Lead<TValue, TOrder>(
        this DbFunctions _,
        TValue value,
        TOrder orderBy,
        TValue? defaultValue,
        int offset = 1,
        bool descending = false)
        => throw new NotSupportedException("This method is only for use in LINQ translation.");

    public static TValue? Lead<TPartition, TValue, TOrder>(
        this DbFunctions _,
        TPartition partitionBy,
        TValue value,
        TOrder orderBy,
        TValue? defaultValue,
        int offset = 1,
        bool descending = false)
        => throw new NotSupportedException("This method is only for use in LINQ translation.");

    public static TValue? FirstValue<TValue, TOrder>(
        this DbFunctions _,
        TValue value,
        TOrder orderBy,
        bool descending = false)
        => throw new NotSupportedException("This method is only for use in LINQ translation.");

    public static TValue? FirstValue<TPartition, TValue, TOrder>(
        this DbFunctions _,
        TPartition partitionBy,
        TValue value,
        TOrder orderBy,
        bool descending = false)
        => throw new NotSupportedException("This method is only for use in LINQ translation.");

    public static TValue? LastValue<TValue, TOrder>(
        this DbFunctions _,
        TValue value,
        TOrder orderBy,
        bool descending = false)
        => throw new NotSupportedException("This method is only for use in LINQ translation.");

    public static TValue? LastValue<TPartition, TValue, TOrder>(
        this DbFunctions _,
        TPartition partitionBy,
        TValue value,
        TOrder orderBy,
        bool descending = false)
        => throw new NotSupportedException("This method is only for use in LINQ translation.");

    public static TValue? NthValue<TValue, TOrder>(
        this DbFunctions _,
        TValue value,
        int nth,
        TOrder orderBy,
        bool descending = false)
        => throw new NotSupportedException("This method is only for use in LINQ translation.");

    public static TValue? NthValue<TPartition, TValue, TOrder>(
        this DbFunctions _,
        TPartition partitionBy,
        TValue value,
        int nth,
        TOrder orderBy,
        bool descending = false)
        => throw new NotSupportedException("This method is only for use in LINQ translation.");
}
