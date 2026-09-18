using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using Npgsql;
using SensorHub.Application.Ingestion;
using SensorHub.Application.Processing;
using SensorHub.Infrastructure.Kafka;
using SensorHub.Infrastructure.Persistence;

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

    public static IServiceCollection AddPostgresPersistence(this IServiceCollection services, IConfiguration configuration)
    {
        var connectionString = configuration.GetConnectionString(PersistenceOptions.ConnectionStringName)
            ?? throw new InvalidOperationException($"ConnectionStrings:{PersistenceOptions.ConnectionStringName} não configurada.");

        services.Configure<PersistenceOptions>(configuration.GetSection(PersistenceOptions.SectionName));

        // Um DataSource por processo: é ele que mantém o pool de conexões.
        services.AddSingleton(_ => NpgsqlDataSource.Create(connectionString));
        services.AddDbContext<SensorHubDbContext>((sp, options) => options.UseNpgsql(sp.GetRequiredService<NpgsqlDataSource>()));
        services.AddSingleton<IReadingStore, PostgresReadingStore>();
        services.AddHostedService<DatabaseMigrator>();
        return services;
    }

    /// <summary>Consumer que persiste leituras no histórico (grupo <c>sensorhub.persistence</c>).</summary>
    public static IServiceCollection AddReadingPersistenceConsumer(this IServiceCollection services, IConfiguration configuration)
    {
        const string name = "persistence";
        services.Configure<BatchConsumerOptions>(name, configuration.GetSection("Consumers:Persistence"));
        services.AddSingleton<IDeadLetterSink, KafkaDeadLetterSink>();
        services.AddSingleton<PersistReadingsHandler>();

        services.AddSingleton<IHostedService>(sp => new KafkaBatchConsumer(
            name,
            sp.GetRequiredService<IOptions<KafkaOptions>>().Value,
            sp.GetRequiredService<IOptionsMonitor<BatchConsumerOptions>>().Get(name),
            sp.GetRequiredService<PersistReadingsHandler>(),
            sp.GetRequiredService<IDeadLetterSink>(),
            sp.GetRequiredService<ILoggerFactory>().CreateLogger($"Consumer.{name}"),
            PostgresFailureClassifier.IsPoison));
        return services;
    }
}
