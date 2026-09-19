using FluentValidation;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using SensorHub.Application.Ingestion;
using SensorHub.Application.Management;
using SensorHub.Application.Queries;

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

    public static IServiceCollection AddManagementServices(this IServiceCollection services)
    {
        services.AddSingleton(TimeProvider.System);
        services.AddScoped<SensorService>();
        services.AddScoped<AlertRuleService>();
        services.AddScoped<AlertService>();
        return services;
    }

    public static IServiceCollection AddReadingQueries(this IServiceCollection services)
    {
        services.AddSingleton(TimeProvider.System);
        services.AddSingleton<ReadingQueryService>();
        return services;
    }
}
