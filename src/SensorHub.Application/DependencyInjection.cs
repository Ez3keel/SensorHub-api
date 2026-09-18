using FluentValidation;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using SensorHub.Application.Ingestion;

namespace SensorHub.Application;

public static class DependencyInjection
{
    public static IServiceCollection AddIngestion(this IServiceCollection services, IConfiguration configuration)
    {
        services.Configure<IngestionOptions>(configuration.GetSection(IngestionOptions.SectionName));
        services.AddSingleton(TimeProvider.System);
        services.AddSingleton<IValidator<ReadingRequest>, ReadingRequestValidator>();
        services.AddSingleton<IngestionService>();
        return services;
    }
}
