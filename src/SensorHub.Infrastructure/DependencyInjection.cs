using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using Npgsql;
using StackExchange.Redis;
using SensorHub.Application.Alerting;
using SensorHub.Application.Ingestion;
using SensorHub.Application.Management;
using SensorHub.Application.Processing;
using SensorHub.Application.LatestValues;
using SensorHub.Application.Queries;
using SensorHub.Infrastructure.Kafka;
using SensorHub.Application.Security;
using SensorHub.Infrastructure.Persistence;
using SensorHub.Infrastructure.Security;
using SensorHub.Infrastructure.Redis;

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
        services.Configure<PersistenceOptions>(configuration.GetSection(PersistenceOptions.SectionName));

        // Um DataSource por processo: é ele que mantém o pool de conexões. A connection string é lida de forma
        // preguiçosa (na 1ª resolução), depois de toda a configuração estar montada.
        services.AddSingleton(sp => NpgsqlDataSource.Create(
            sp.GetRequiredService<IConfiguration>().GetConnectionString(PersistenceOptions.ConnectionStringName)
            ?? throw new InvalidOperationException($"ConnectionStrings:{PersistenceOptions.ConnectionStringName} não configurada.")));
        // Factory (singleton) para o motor de alertas e o catálogo, que vivem em singletons e criam contextos curtos;
        // e o DbContext por escopo para a API (repositórios e Unit of Work por requisição).
        services.AddDbContextFactory<SensorHubDbContext>((sp, options) => options.UseNpgsql(sp.GetRequiredService<NpgsqlDataSource>()));
        services.AddScoped(sp => sp.GetRequiredService<IDbContextFactory<SensorHubDbContext>>().CreateDbContext());
        services.AddSingleton<IReadingStore, PostgresReadingStore>();
        services.AddSingleton<IReadingQueries, TimescaleReadingQueries>();
        services.AddHostedService<DatabaseMigrator>();
        return services;
    }

    /// <summary>Repositórios e Unit of Work da API de administração (sensores, regras e alertas).</summary>
    public static IServiceCollection AddManagementRepositories(this IServiceCollection services)
    {
        services.AddScoped<IUnitOfWork, EfUnitOfWork>();
        services.AddScoped<ISensorRepository, SensorRepository>();
        services.AddScoped<IAlertRuleRepository, AlertRuleRepository>();
        services.AddScoped<IAlertRepository, AlertRepository>();
        return services;
    }

    /// <summary>
    /// O motor de alertas: consumer do grupo <c>sensorhub.alerts</c> + varredura "sem dados". Requer Postgres
    /// (regras e alertas), Redis (estado das regras) e Kafka (eventos de alerta).
    /// </summary>
    public static IServiceCollection AddAlertEngine(this IServiceCollection services, IConfiguration configuration)
    {
        services.Configure<AlertingOptions>(configuration.GetSection(AlertingOptions.SectionName));
        services.AddSingleton(TimeProvider.System);
        services.AddSingleton<IRuleCatalog, CachedRuleCatalog>();
        services.AddSingleton<IAlertStore, PostgresAlertStore>();
        services.AddSingleton<KafkaAlertEventPublisher>();
        services.AddSingleton<IAlertEventPublisher>(sp => sp.GetRequiredService<KafkaAlertEventPublisher>());
        services.AddSingleton<IDistributedLease, RedisDistributedLease>();
        services.AddSingleton<AlertEngine>();
        services.AddHostedService<NoDataSweeper>();
        return services.AddReadingConsumer<EvaluateAlertsHandler>(configuration, "alerts", isPoison: null);
    }

    /// <summary>
    /// Segurança: hash de senha, emissão de JWT, autenticação de dispositivo por chave de API (com cache) e o registro de sensores
    /// usado para validar a propriedade das leituras. Repositórios de usuário/token/dispositivo por escopo de requisição.
    /// </summary>
    public static IServiceCollection AddSecurityInfrastructure(this IServiceCollection services, IConfiguration configuration)
    {
        services.Configure<SecurityOptions>(configuration.GetSection(SecurityOptions.SectionName));
        services.AddMemoryCache();
        services.AddSingleton<IPasswordHasher, IdentityPasswordHasher>();
        services.AddSingleton<IAccessTokenIssuer, JwtAccessTokenIssuer>();
        services.AddSingleton<IDeviceAuthenticator, CachedDeviceAuthenticator>();
        services.AddSingleton<ISensorRegistry, CachedSensorRegistry>();
        services.AddScoped<IUserRepository, UserRepository>();
        services.AddScoped<IRefreshTokenRepository, RefreshTokenRepository>();
        services.AddScoped<IDeviceRepository, DeviceRepository>();
        return services;
    }

    /// <summary>Redis como estado quente: último valor por sensor (e, na Fase 5, estado das regras de alerta).</summary>
    public static IServiceCollection AddRedisState(this IServiceCollection services, IConfiguration configuration)
    {
        services.Configure<RedisOptions>(configuration.GetSection(RedisOptions.SectionName));
        services.AddSingleton<IConnectionMultiplexer>(sp =>
        {
            var options = ConfigurationOptions.Parse(sp.GetRequiredService<IOptions<RedisOptions>>().Value.ConnectionString);
            // O processo sobe mesmo com o Redis fora do ar e reconecta sozinho: o Redis guarda estado derivado,
            // e a indisponibilidade dele não deve impedir a API de responder ou o consumer de manter a fila.
            options.AbortOnConnectFail = false;
            options.ConnectTimeout = 5_000;
            return ConnectionMultiplexer.Connect(options);
        });
        services.AddSingleton<ILastValueStore, RedisLastValueStore>();
        services.AddSingleton<LastValueService>();
        services.AddSingleton<IRuleStateStore, RedisRuleStateStore>(); // a API também precisa: limpa o estado ao desabilitar/excluir uma regra
        return services;
    }

    /// <summary>Consumer que persiste leituras no histórico (grupo <c>sensorhub.persistence</c>).</summary>
    public static IServiceCollection AddReadingPersistenceConsumer(this IServiceCollection services, IConfiguration configuration) =>
        services.AddReadingConsumer<PersistReadingsHandler>(configuration, "persistence", PostgresFailureClassifier.IsPoison);

    /// <summary>Consumer que mantém o último valor de cada sensor no Redis (grupo <c>sensorhub.lastvalue</c>).</summary>
    public static IServiceCollection AddLastValueConsumer(this IServiceCollection services, IConfiguration configuration) =>
        services.AddReadingConsumer<UpdateLastValueHandler>(configuration, "lastvalue", isPoison: null);

    /// <summary>
    /// Registra um consumer de leituras com o SEU consumer group (<c>sensorhub.{name}</c>). Cada grupo é uma
    /// visão independente do mesmo tópico: tem offset e lag próprios, e falhar ou atrasar não afeta os outros.
    /// </summary>
    private static IServiceCollection AddReadingConsumer<THandler>(
        this IServiceCollection services, IConfiguration configuration, string name, Func<Exception, bool>? isPoison)
        where THandler : class, IReadingBatchHandler
    {
        services.Configure<BatchConsumerOptions>(name, options => options.GroupId = $"sensorhub.{name}");
        services.Configure<BatchConsumerOptions>(name, configuration.GetSection($"Consumers:{name}"));
        services.TryAddSingleton<IDeadLetterSink, KafkaDeadLetterSink>();
        services.AddSingleton<THandler>();

        services.AddSingleton<IHostedService>(sp => new KafkaBatchConsumer(
            name,
            sp.GetRequiredService<IOptions<KafkaOptions>>().Value,
            sp.GetRequiredService<IOptionsMonitor<BatchConsumerOptions>>().Get(name),
            sp.GetRequiredService<THandler>(),
            sp.GetRequiredService<IDeadLetterSink>(),
            sp.GetRequiredService<ILoggerFactory>().CreateLogger($"Consumer.{name}"),
            isPoison));
        return services;
    }
}
