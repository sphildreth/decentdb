using System.Diagnostics;
using System.Linq;
using System.Runtime.InteropServices;
using Xunit;

namespace DecentDB.EntityFrameworkCore.Tests;

public sealed class DesignTimeToolingTests
{
    private static readonly TimeSpan DotnetCommandTimeout = TimeSpan.FromMinutes(5);

    [Fact]
    public void DotnetEf_MigrationsAdd_AndDatabaseUpdate_Work()
    {
        if (!IsDotnetEfAvailable())
        {
            return;
        }

        var testRoot = Path.Combine(Path.GetTempPath(), $"ef_design_{Guid.NewGuid():N}");
        try
        {
            Directory.CreateDirectory(testRoot);
            var projectPath = Path.Combine(testRoot, "EfDesignSample.csproj");
            var dbPath = Path.Combine(testRoot, "sample.ddb");
            var providerProject = Path.GetFullPath(Path.Combine(AppContext.BaseDirectory, "..", "..", "..", "..", "..", "src", "DecentDB.EntityFrameworkCore", "DecentDB.EntityFrameworkCore.csproj"));
            var designProject = Path.GetFullPath(Path.Combine(AppContext.BaseDirectory, "..", "..", "..", "..", "..", "src", "DecentDB.EntityFrameworkCore.Design", "DecentDB.EntityFrameworkCore.Design.csproj"));
            var repoRoot = Path.GetFullPath(Path.Combine(providerProject, "..", "..", "..", "..", ".."));

            File.WriteAllText(projectPath, $"""
                                           <Project Sdk="Microsoft.NET.Sdk">
                                             <PropertyGroup>
                                               <OutputType>Exe</OutputType>
                                               <TargetFramework>net10.0</TargetFramework>
                                               <ImplicitUsings>enable</ImplicitUsings>
                                               <Nullable>enable</Nullable>
                                             </PropertyGroup>
                                             <ItemGroup>
                                               <PackageReference Include="Microsoft.EntityFrameworkCore.Design" Version="10.0.3">
                                                 <PrivateAssets>all</PrivateAssets>
                                                 <IncludeAssets>runtime; build; native; contentfiles; analyzers; buildtransitive</IncludeAssets>
                                               </PackageReference>
                                             </ItemGroup>
                                             <ItemGroup>
                                               <ProjectReference Include="{providerProject.Replace("\\", "/")}" />
                                               <ProjectReference Include="{designProject.Replace("\\", "/")}" />
                                             </ItemGroup>
                                           </Project>
                                           """);

            var programSource = """
                                using DecentDB.EntityFrameworkCore;
                                using Microsoft.EntityFrameworkCore;
                                using Microsoft.EntityFrameworkCore.Design;

                                public sealed class SampleContext(DbContextOptions<SampleContext> options) : DbContext(options)
                                {
                                    public DbSet<SampleEntity> Items => Set<SampleEntity>();
                                    protected override void OnModelCreating(ModelBuilder modelBuilder)
                                    {
                                        modelBuilder.Entity<SampleEntity>(entity =>
                                        {
                                            entity.ToTable("ef_design_items");
                                            entity.HasKey(x => x.Id);
                                            entity.Property(x => x.Id).HasColumnName("id").ValueGeneratedOnAdd();
                                            entity.Property(x => x.Name).HasColumnName("name");
                                        });
                                    }
                                }

                                public sealed class SampleEntity
                                {
                                    public int Id { get; set; }
                                    public string Name { get; set; } = string.Empty;
                                }

                                public sealed class SampleContextFactory : IDesignTimeDbContextFactory<SampleContext>
                                {
                                    public SampleContext CreateDbContext(string[] args)
                                    {
                                        var optionsBuilder = new DbContextOptionsBuilder<SampleContext>();
                                        optionsBuilder.UseDecentDB("Data Source=__DB_PATH__");
                                        return new SampleContext(optionsBuilder.Options);
                                    }
                                }

                                public static class Program
                                {
                                    public static void Main()
                                    {
                                    }
                                }

                                """.Replace("__DB_PATH__", dbPath.Replace("\\", "/"));

            File.WriteAllText(Path.Combine(testRoot, "Program.cs"), programSource);

            var build = Run(testRoot, "build -v minimal");
            Assert.True(build.ExitCode == 0, build.Output);
            StageNativeLibrary(repoRoot, Path.Combine(testRoot, "bin", "Debug", "net10.0"));

            var addMigration = Run(testRoot, "ef migrations add Initial --context SampleContext --output-dir Migrations --project . --startup-project . --no-build");
            Assert.True(addMigration.ExitCode == 0, addMigration.Output);

            var rebuild = Run(testRoot, "build -v minimal --no-restore");
            Assert.True(rebuild.ExitCode == 0, rebuild.Output);
            StageNativeLibrary(repoRoot, Path.Combine(testRoot, "bin", "Debug", "net10.0"));

            var updateDb = Run(testRoot, "ef database update --context SampleContext --project . --startup-project . --no-build");
            Assert.True(updateDb.ExitCode == 0, updateDb.Output);

            var scaffold = Run(
                testRoot,
                $"ef dbcontext scaffold \"Data Source={dbPath.Replace("\\", "/")}\" DecentDB.EntityFrameworkCore --context ScaffoldingContext --output-dir ScaffoldingOut --project . --startup-project . --no-build --no-onconfiguring --force");
            Assert.True(scaffold.ExitCode == 0, scaffold.Output);

            Assert.True(Directory.Exists(Path.Combine(testRoot, "Migrations")));
            Assert.True(File.Exists(dbPath));
            Assert.True(File.Exists(Path.Combine(testRoot, "ScaffoldingOut", "ScaffoldingContext.cs")));
        }
        finally
        {
            if (Directory.Exists(testRoot))
            {
                try
                {
                    Directory.Delete(testRoot, recursive: true);
                }
                catch
                {
                    // Best effort cleanup for tooling test temp files.
                }
            }
        }
    }

    private static bool IsDotnetEfAvailable()
    {
        var result = Run(Environment.CurrentDirectory, "ef --version");
        return result.ExitCode == 0;
    }

    private static (int ExitCode, string Output) Run(string workingDirectory, string arguments)
    {
        var psi = new ProcessStartInfo("dotnet", arguments)
        {
            WorkingDirectory = workingDirectory,
            RedirectStandardOutput = true,
            RedirectStandardError = true
        };

        // Prevent MSBuild node reuse to avoid lock contention with the test host's MSBuild server.
        psi.Environment["MSBUILDDISABLENODEREUSE"] = "1";
        psi.Environment["DOTNET_CLI_DO_NOT_USE_MSBUILD_SERVER"] = "1";

        using var process = Process.Start(psi) ?? throw new InvalidOperationException("Failed to start dotnet process.");

        var stdoutTask = process.StandardOutput.ReadToEndAsync();
        var stderrTask = process.StandardError.ReadToEndAsync();

        if (!process.WaitForExit((int)DotnetCommandTimeout.TotalMilliseconds))
        {
            try
            {
                process.Kill(entireProcessTree: true);
            }
            catch
            {
                // Best-effort cleanup.
            }

            throw new TimeoutException($"dotnet {arguments} timed out after {DotnetCommandTimeout}.");
        }

        // Use a bounded wait for output to prevent hangs from orphaned child processes holding pipes.
        if (!Task.WaitAll([stdoutTask, stderrTask], TimeSpan.FromSeconds(10)))
        {
            try
            {
                process.Kill(entireProcessTree: true);
            }
            catch
            {
                // Best-effort cleanup.
            }
        }

        var stdout = stdoutTask.IsCompleted ? stdoutTask.Result : "";
        var stderr = stderrTask.IsCompleted ? stderrTask.Result : "";
        return (process.ExitCode, stdout + stderr);
    }

    private static void StageNativeLibrary(string repoRoot, string outputDirectory)
    {
        Directory.CreateDirectory(outputDirectory);

        // Prefer build/ output (canonical build dir) over repo root to avoid stale binaries.
        if (RuntimeInformation.IsOSPlatform(OSPlatform.Linux))
        {
            CopyFirstExisting(
                Path.Combine(outputDirectory, "libdecentdb.so"),
                Path.Combine(repoRoot, "target", "debug", "libdecentdb.so"),
                Path.Combine(repoRoot, "target", "release", "libdecentdb.so"),
                Path.Combine(repoRoot, "build", "libdecentdb.so"),
                Path.Combine(repoRoot, "build", "libc_api.so"),
                Path.Combine(repoRoot, "libdecentdb.so"));
            return;
        }

        if (RuntimeInformation.IsOSPlatform(OSPlatform.OSX))
        {
            CopyFirstExisting(
                Path.Combine(outputDirectory, "libdecentdb.dylib"),
                Path.Combine(repoRoot, "target", "debug", "libdecentdb.dylib"),
                Path.Combine(repoRoot, "target", "release", "libdecentdb.dylib"),
                Path.Combine(repoRoot, "build", "libdecentdb.dylib"),
                Path.Combine(repoRoot, "build", "libc_api.dylib"),
                Path.Combine(repoRoot, "libdecentdb.dylib"));
            return;
        }

        if (RuntimeInformation.IsOSPlatform(OSPlatform.Windows))
        {
            CopyFirstExisting(
                Path.Combine(outputDirectory, "decentdb.dll"),
                Path.Combine(repoRoot, "target", "debug", "decentdb.dll"),
                Path.Combine(repoRoot, "target", "release", "decentdb.dll"),
                Path.Combine(repoRoot, "build", "decentdb.dll"),
                Path.Combine(repoRoot, "build", "c_api.dll"),
                Path.Combine(repoRoot, "decentdb.dll"));
        }
    }

    private static void CopyFirstExisting(string destinationPath, params string[] candidatePaths)
    {
        var source = candidatePaths.FirstOrDefault(File.Exists);

        if (source is null)
        {
            throw new FileNotFoundException("Native DecentDB library not found for design-time tooling test.");
        }

        File.Copy(source, destinationPath, overwrite: true);
    }
}
