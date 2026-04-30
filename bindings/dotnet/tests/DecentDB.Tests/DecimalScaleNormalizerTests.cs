using DecentDB.AdoNet;
using Xunit;

namespace DecentDB.Tests;

public sealed class DecimalScaleNormalizerTests
{
    [Theory]
    [InlineData(123.4567, 2, 123.46)]
    [InlineData(123.4567, 3, 123.457)]
    [InlineData(123.4567, 0, 123)]
    [InlineData(123.4549, 2, 123.45)]
    [InlineData(-123.4567, 2, -123.46)]
    [InlineData(0.999, 2, 1.00)]
    public void Normalize_RoundsToEven(decimal input, int scale, decimal expected)
    {
        var result = DecimalScaleNormalizer.Normalize(input, scale);
        Assert.Equal(expected, result);
    }

    [Theory]
    [InlineData(123.4000, 4, 123.4000)]
    [InlineData(123.0000, 4, 123.0000)]
    [InlineData(0.1000, 4, 0.1000)]
    public void Normalize_PreservesTrailingZeros(decimal input, int scale, decimal expected)
    {
        var result = DecimalScaleNormalizer.Normalize(input, scale);
        Assert.Equal(expected, result);
    }

    [Fact]
    public void Normalize_ZeroScale_ReturnsInteger()
    {
        var result = DecimalScaleNormalizer.Normalize(123.99m, 0);
        Assert.Equal(124m, result);
    }

    [Fact]
    public void Normalize_NegativeScale_ThrowsArgumentOutOfRange()
    {
        Assert.Throws<ArgumentOutOfRangeException>(() => DecimalScaleNormalizer.Normalize(1m, -1));
    }

    [Fact]
    public void Normalize_ScaleExceeds28_ThrowsArgumentOutOfRange()
    {
        Assert.Throws<ArgumentOutOfRangeException>(() => DecimalScaleNormalizer.Normalize(1m, 29));
    }

    [Fact]
    public void Normalize_MaxScale28_Succeeds()
    {
        var result = DecimalScaleNormalizer.Normalize(1m, 28);
        Assert.Equal(1.0000000000000000000000000000m, result);
    }
}
