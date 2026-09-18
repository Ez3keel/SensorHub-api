using Confluent.Kafka;
using Microsoft.Extensions.Diagnostics.HealthChecks;
using Microsoft.Extensions.Options;

namespace SensorHub.Infrastructure.Kafka;

/// <summary>Readiness: o cluster responde a um pedido de metadados e o tópico de leituras existe.</summary>
public sealed class KafkaHealthCheck(IOptions<KafkaOptions> options) : IHealthCheck
{
    public Task<HealthCheckResult> CheckHealthAsync(HealthCheckContext context, CancellationToken cancellationToken = default)
    {
        try
        {
            using var admin = new AdminClientBuilder(new AdminClientConfig { BootstrapServers = options.Value.BootstrapServers }).Build();
            var metadata = admin.GetMetadata(options.Value.ReadingsTopic, TimeSpan.FromSeconds(3));
            var topic = metadata.Topics.FirstOrDefault(t => t.Topic == options.Value.ReadingsTopic);

            return Task.FromResult(topic is { Error.Code: ErrorCode.NoError }
                ? HealthCheckResult.Healthy($"{metadata.Brokers.Count} broker(s), {topic.Partitions.Count} partições")
                : HealthCheckResult.Unhealthy($"Tópico {options.Value.ReadingsTopic} indisponível"));
        }
        catch (Exception ex)
        {
            return Task.FromResult(HealthCheckResult.Unhealthy("Kafka inacessível", ex));
        }
    }
}
