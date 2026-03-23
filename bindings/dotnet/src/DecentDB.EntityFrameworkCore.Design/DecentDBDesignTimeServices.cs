using DecentDB.EntityFrameworkCore.Design.Scaffolding.Internal;
using DecentDB.EntityFrameworkCore.Extensions;
using Microsoft.EntityFrameworkCore.Design;
using Microsoft.EntityFrameworkCore.Scaffolding;
using Microsoft.Extensions.DependencyInjection;

namespace DecentDB.EntityFrameworkCore.Design;

public sealed class DecentDBDesignTimeServices : IDesignTimeServices
{
    public void ConfigureDesignTimeServices(IServiceCollection serviceCollection)
    {
        serviceCollection.AddEntityFrameworkDecentDB();

        new EntityFrameworkRelationalDesignServicesBuilder(serviceCollection)
            .TryAdd<Microsoft.EntityFrameworkCore.Design.IAnnotationCodeGenerator, Microsoft.EntityFrameworkCore.Design.AnnotationCodeGenerator>()
            .TryAdd<IDatabaseModelFactory, DecentDBDatabaseModelFactory>()
            .TryAdd<IProviderConfigurationCodeGenerator, DecentDBCodeGenerator>()
            .TryAddCoreServices();
    }
}
