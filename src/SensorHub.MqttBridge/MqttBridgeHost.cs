using Microsoft.Extensions.Diagnostics.HealthChecks;
using Microsoft.Extensions.Options;
using OpenTelemetry.Metrics;
using SensorHub.Application;
using SensorHub.Infrastructure;
using SensorHub.Infrastructure.Kafka;
using SensorHub.Infrastructure.Observability;
using SensorHub.Infrastructure.Persistence;

namespace SensorHub.MqttBridge;

/// <summary>
/// Monta o host do bridge. Fica numa classe (e não só no Program.cs) para que os testes de integração subam o mesmo host, com
/// Kestrel de verdade, sem depender do <c>Program</c> (que colidiria com o da API no projeto de testes).
/// </summary>
public static class MqttBridgeHost
{
    public static WebApplication Build(string[] args)
    {
        var builder = WebApplication.CreateBuilder(args);

        builder.Services.Configure<MqttBridgeOptions>(builder.Configuration.GetSection(MqttBridgeOptions.SectionName));

        // Sem segredo forte o bridge abriria o broker a qualquer um: falha na subida, não no primeiro dispositivo.
        var bridgeOptions = builder.Configuration.GetSection(MqttBridgeOptions.SectionName).Get<MqttBridgeOptions>() ?? new MqttBridgeOptions();
        bridgeOptions.Validate(builder.Environment.IsDevelopment());

        var runsAuth = bridgeOptions.HasRole("auth");
        var runsBridge = bridgeOptions.HasRole("bridge");

        // Os dois papéis precisam do Postgres (dispositivos e sensores). Só o bridge fala com o Kafka e com o broker.
        builder.Services.AddPostgresPersistence(builder.Configuration);
        builder.Services.AddSecurityInfrastructure(builder.Configuration);
        builder.Services.AddSingleton<MqttAccessPolicy>();

        var health = builder.Services.AddHealthChecks().AddCheck<PostgresHealthCheck>("postgres", tags: ["ready"]);

        if (runsBridge)
        {
            // O MESMO caminho de ingestão da API HTTP (validação + posse do sensor + Kafka), sem a camada HTTP.
            builder.Services.AddIngestion(builder.Configuration);
            builder.Services.AddKafkaMessaging(builder.Configuration);
            builder.Services.AddKafkaReadingPublisher();
            builder.Services.AddSingleton<MqttBridgeService>();
            builder.Services.AddHostedService(sp => sp.GetRequiredService<MqttBridgeService>());

            health.AddCheck<KafkaHealthCheck>("kafka", tags: ["ready"])
                .AddCheck("mqtt", () => MqttMetrics.IsConnected ? HealthCheckResult.Healthy() : HealthCheckResult.Unhealthy("sem conexão com o broker MQTT"),
                    tags: ["ready"]);
        }

        builder.Services.AddSensorHubObservability(builder.Configuration, runsBridge && !runsAuth ? "sensorhub-mqtt-bridge" : runsAuth && !runsBridge ? "sensorhub-mqtt-auth" : "sensorhub-mqtt",
            metrics: m => m.AddPrometheusExporter());

        var app = builder.Build();

        if (runsAuth) app.MapMqttAuth();
        app.MapPrometheusScrapingEndpoint();
        app.MapHealthChecks("/health/live", new() { Predicate = _ => false });
        app.MapHealthChecks("/health/ready", new() { Predicate = check => check.Tags.Contains("ready") });

        return app;
    }
}
