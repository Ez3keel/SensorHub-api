using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using SensorHub.Application.Ingestion;
using SensorHub.Infrastructure.Kafka;

namespace SensorHub.Infrastructure;

public static class DependencyInjection
{
    public static IServiceCollection AddKafkaMessaging(this IServiceCollection services, IConfiguration configuration)
    {
        services.Configure<KafkaOptions>(configuration.GetSection(KafkaOptions.SectionName));
        services.AddHostedService<KafkaTopicProvisioner>();
        return services;
    }

    /// <summary>Producer de leituras (usado pela API). O producer é singleton: é thread-safe e mantém conexões/batches.</summary>
    public static IServiceCollection AddKafkaReadingPublisher(this IServiceCollection services)
    {
        services.AddSingleton<KafkaReadingPublisher>();
        services.AddSingleton<IReadingPublisher>(sp => sp.GetRequiredService<KafkaReadingPublisher>());
        return services;
    }
}
