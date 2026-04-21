using DecentDB.EntityFrameworkCore.Design.Internal;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Design;
using Microsoft.EntityFrameworkCore.Infrastructure;
using Microsoft.EntityFrameworkCore.Scaffolding;
using Microsoft.EntityFrameworkCore.Storage;
using Microsoft.Extensions.DependencyInjection;
using System.Reflection;
using Xunit;

namespace DecentDB.EntityFrameworkCore.Tests;

public sealed class DesignAndNodaTimeMappingCoverageTests : IDisposable
{
    private readonly string _dbPath = Path.Combine(Path.GetTempPath(), $"test_ef_design_noda_{Guid.NewGuid():N}.ddb");

    [Fact]
    public void DesignTimeServices_RegisterExpectedProviderServices()
    {
        var services = new ServiceCollection();
        new DecentDBDesignTimeServices().ConfigureDesignTimeServices(services);
        using var provider = services.BuildServiceProvider();

        var generator = provider.GetRequiredService<IProviderConfigurationCodeGenerator>();
        var modelFactory = provider.GetRequiredService<IDatabaseModelFactory>();

        Assert.IsType<DecentDBCodeGenerator>(generator);
        Assert.IsType<DecentDBDatabaseModelFactory>(modelFactory);
    }

    [Fact]
    public void CodeGenerator_ProducesUseDecentDbFragments_WithAndWithoutOptions()
    {
        var services = new ServiceCollection();
        new DecentDBDesignTimeServices().ConfigureDesignTimeServices(services);
        using var provider = services.BuildServiceProvider();
        var generator = (DecentDBCodeGenerator)provider.GetRequiredService<IProviderConfigurationCodeGenerator>();

        var withoutOptions = generator.GenerateUseProvider("Data Source=:memory:", providerOptions: null);
        Assert.Equal("UseDecentDB", withoutOptions.Method);
        Assert.Single(withoutOptions.Arguments);

        var providerOptions = new MethodCallCodeFragment("EnableDetailedErrors");
        var withOptions = generator.GenerateUseProvider("Data Source=:memory:", providerOptions);
        Assert.Equal("UseDecentDB", withOptions.Method);
        Assert.Equal(2, withOptions.Arguments.Count);
        Assert.Equal(providerOptions, withOptions.Arguments[1]);
    }

    [Fact]
    public void NodaTimeTypeMappingSource_CoversDecimalStoreTypeShapes()
    {
        using var context = CreateNodaContext();
        var mappingSource = context.GetService<IRelationalTypeMappingSource>();

        var decimalExact = Assert.IsType<RelationalTypeMapping>(mappingSource.FindMapping("DECIMAL(9,2)"));
        Assert.Equal("DECIMAL(9,2)", decimalExact.StoreType);

        var numericPrecisionOnly = Assert.IsType<RelationalTypeMapping>(mappingSource.FindMapping("NUMERIC(6)"));
        Assert.Equal("DECIMAL(6,4)", numericPrecisionOnly.StoreType);

        var malformedDecimal = Assert.IsType<RelationalTypeMapping>(mappingSource.FindMapping("DECIMAL(not_a_number)"));
        Assert.Equal("DECIMAL(18,4)", malformedDecimal.StoreType);

        var nullableDecimal = Assert.IsType<RelationalTypeMapping>(mappingSource.FindMapping(typeof(decimal?)));
        Assert.Equal("DECIMAL(18,4)", nullableDecimal.StoreType);
    }

    [Fact]
    public void NodaTimeMappingPrivateHelpers_ParseAndNormalizeStoreTypes()
    {
        var providerAssembly = typeof(global::DecentDB.EntityFrameworkCore.DecentDBNodaTimeDbContextOptionsBuilderExtensions).Assembly;
        var mappingType = providerAssembly.GetType(
            "DecentDB.EntityFrameworkCore.DecentDBNodaTimeTypeMappingSource",
            throwOnError: true)!;

        var parseMethod = mappingType.GetMethod(
            "ParsePrecisionScale",
            BindingFlags.Static | BindingFlags.NonPublic)!;
        var normalizeMethod = mappingType.GetMethod(
            "NormalizeStoreTypeName",
            BindingFlags.Static | BindingFlags.NonPublic)!;

        var parsedPair = ((int? precision, int? scale))parseMethod.Invoke(null, ["DECIMAL(18,5)"])!;
        Assert.Equal(18, parsedPair.precision);
        Assert.Equal(5, parsedPair.scale);

        var parsedPrecisionOnly = ((int? precision, int? scale))parseMethod.Invoke(null, ["NUMERIC(7)"])!;
        Assert.Equal(7, parsedPrecisionOnly.precision);
        Assert.Null(parsedPrecisionOnly.scale);

        var parsedInvalid = ((int? precision, int? scale))parseMethod.Invoke(null, ["DECIMAL(x,y)"])!;
        Assert.Null(parsedInvalid.precision);
        Assert.Null(parsedInvalid.scale);

        var normalized = (string)normalizeMethod.Invoke(null, ["  DECIMAL(10,3)  "])!;
        Assert.Equal("DECIMAL", normalized);
    }

    public void Dispose()
    {
        TryDelete(_dbPath);
        TryDelete(_dbPath + "-wal");
    }

    private DbContext CreateNodaContext()
    {
        var options = new DbContextOptionsBuilder<NodaSmokeContext>()
            .UseDecentDB($"Data Source={_dbPath}", options => options.UseNodaTime())
            .Options;

        return new NodaSmokeContext(options);
    }

    private static void TryDelete(string path)
    {
        if (File.Exists(path))
        {
            File.Delete(path);
        }
    }

    private sealed class NodaSmokeContext(DbContextOptions<NodaSmokeContext> options) : DbContext(options);
}
