using DecentDB.EntityFrameworkCore.Extensions;
using Microsoft.EntityFrameworkCore.Design;
using Microsoft.EntityFrameworkCore.Scaffolding;
using Microsoft.EntityFrameworkCore.Scaffolding.Internal;
using Microsoft.Extensions.DependencyInjection;

namespace DecentDB.EntityFrameworkCore.Design.Internal;

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
