using Microsoft.AspNetCore.Cors.Infrastructure;
using Microsoft.Extensions.DependencyInjection.Extensions;
using Npgsql;
using SensorHub.Application.Realtime;
using SensorHub.Infrastructure.Kafka;
using SensorHub.Infrastructure.Realtime;
using StackExchange.Redis;
using SignalRRedisOptions = Microsoft.AspNetCore.SignalR.StackExchangeRedis.RedisOptions;

namespace SensorHub.Api.Realtime;

public static class RealtimeExtensions
{
    public const string CorsPolicy = "dashboard";

    /// <summary>
    /// Tempo real: hub SignalR com backplane Redis, consumers (leituras e alertas), flush periódico e CORS do dashboard.
    /// O backplane é o que permite escalar a API horizontalmente: uma mensagem enviada por uma instância chega aos
    /// clientes conectados em TODAS (o Redis retransmite entre elas).
    /// </summary>
    public static IServiceCollection AddRealtime(this IServiceCollection services, IConfiguration configuration)
    {
        services.Configure<RealtimeOptions>(configuration.GetSection(RealtimeOptions.SectionName));

        services.AddSignalR().AddStackExchangeRedis(_ => { });
        // Configuração preguiçosa: lida quando o SignalR precisa, com toda a configuração já montada.
        services.AddOptions<SignalRRedisOptions>().Configure<IConfiguration>((options, config) =>
        {
            var configurationOptions = ConfigurationOptions.Parse(config["Redis:ConnectionString"] ?? "localhost:6380");
            configurationOptions.AbortOnConnectFail = false;
            options.Configuration = configurationOptions;
            options.Configuration.ChannelPrefix = RedisChannel.Literal("sensorhub:signalr");
        });

        services.TryAddSingleton<IDeadLetterSink, KafkaDeadLetterSink>();
        services.AddSingleton<ISensorDirectory>(sp => new CachedSensorDirectory(
            sp.GetRequiredService<NpgsqlDataSource>(), TimeProvider.System,
            sp.GetRequiredService<Microsoft.Extensions.Options.IOptions<RealtimeOptions>>().Value.DirectoryCacheSeconds));
        services.AddSingleton<ISubscriptionRegistry, RedisSubscriptionRegistry>();
        services.AddSingleton<RealtimeFanout>();
        services.AddSingleton(TimeProvider.System);
        services.AddHostedService<RealtimeReadingsConsumerHost>();
        services.AddHostedService<RealtimeFlushService>();
        services.AddHostedService<AlertEventListener>();

        services.AddCors();
        services.AddOptions<CorsOptions>().Configure<IConfiguration>((cors, config) =>
        {
            var origins = config.GetSection("Cors:Origins").Get<string[]>() ?? ["http://localhost:5173"];
            // SignalR (WebSocket com credenciais) exige origens explícitas: "qualquer origem" não é aceito com credenciais.
            cors.AddPolicy(CorsPolicy, policy => policy.WithOrigins(origins).AllowAnyHeader().AllowAnyMethod().AllowCredentials());
        });

        return services;
    }
}
