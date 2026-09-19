using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Npgsql;
using OpenTelemetry.Metrics;
using OpenTelemetry.Resources;
using OpenTelemetry.Trace;
using SensorHub.Application.Observability;

namespace SensorHub.Infrastructure.Observability;

public static class ObservabilityExtensions
{
    /// <summary>
    /// Configuração comum de OpenTelemetry (API e Worker): métricas do SensorHub, runtime do .NET, Npgsql e Redis, e traces
    /// exportados por OTLP para o Jaeger quando <c>Observability:OtlpEndpoint</c> está definido. Cada host acrescenta o que é
    /// dele (ASP.NET Core e o endpoint de métricas do Prometheus) pelos callbacks.
    /// </summary>
    public static IServiceCollection AddSensorHubObservability(
        this IServiceCollection services,
        IConfiguration configuration,
        string serviceName,
        Action<MeterProviderBuilder>? metrics = null,
        Action<TracerProviderBuilder>? tracing = null)
    {
        var otlp = configuration["Observability:OtlpEndpoint"];
        var sampleRatio = double.TryParse(configuration["Observability:TraceSampleRatio"], System.Globalization.CultureInfo.InvariantCulture, out var r) ? r : 1.0;

        services.AddOpenTelemetry()
            .ConfigureResource(resource => resource
                .AddService(serviceName, serviceVersion: typeof(ObservabilityExtensions).Assembly.GetName().Version?.ToString())
                .AddAttributes([new("service.instance.id", Environment.MachineName)]))
            .WithMetrics(builder =>
            {
                builder
                    .AddMeter(SensorHubTelemetry.Name)
                    .AddMeter("Npgsql")
                    .AddRuntimeInstrumentation()
                    // As buckets PADRÃO do OpenTelemetry (0, 5, 10, 25, 50...) foram pensadas para milissegundos. Um histograma em
                    // SEGUNDOS com valores entre 0 e 5 cai inteiro no primeiro balde e o Prometheus interpola o ponto médio: todo
                    // quantil sai como "2,5 s" ou "4,75 s", um número que nunca aconteceu. Buckets explícitos corrigem.
                    .AddView("sensorhub.consumer.processing.delay", new ExplicitBucketHistogramConfiguration
                    {
                        Boundaries = [0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10, 30, 60, 300, 1800]
                    })
                    .AddView("sensorhub.consumer.batch.size", new ExplicitBucketHistogramConfiguration
                    {
                        Boundaries = [1, 5, 10, 50, 100, 250, 500, 1000, 2500, 5000]
                    })
                    .AddView("sensorhub.consumer.handler.duration", new ExplicitBucketHistogramConfiguration
                    {
                        Boundaries = [1, 2.5, 5, 10, 25, 50, 100, 250, 500, 1000, 2500, 5000, 10000]
                    })
                    .AddView("sensorhub.ingest.publish.duration", new ExplicitBucketHistogramConfiguration
                    {
                        Boundaries = [1, 2.5, 5, 10, 25, 50, 100, 250, 500, 1000, 2500, 5000]
                    });
                metrics?.Invoke(builder);
            })
            .WithTracing(builder =>
            {
                builder
                    // Amostragem por razão respeitando o pai: se a API decidiu amostrar a requisição, o consumer (que herda o
                    // contexto pelos headers do Kafka) mantém a decisão, e o trace nunca fica pela metade.
                    .SetSampler(new ParentBasedSampler(new TraceIdRatioBasedSampler(sampleRatio)))
                    .AddSource(SensorHubTelemetry.Name)
                    .AddNpgsql()
                    .AddRedisInstrumentation();
                tracing?.Invoke(builder);

                if (!string.IsNullOrWhiteSpace(otlp))
                    builder.AddOtlpExporter(o => o.Endpoint = new Uri(otlp));
            });

        return services;
    }
}
