using Confluent.Kafka;
using Confluent.Kafka.Admin;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;

namespace SensorHub.Infrastructure.Kafka;

/// <summary>
/// Garante que os tópicos existem, com o nº de partições certo, antes de qualquer produce/consume.
/// Criação explícita (e não auto-create do broker) porque o auto-create usaria 1 partição por padrão,
/// o que mataria o paralelismo silenciosamente. Idempotente: tópico existente é ignorado.
/// </summary>
public sealed class KafkaTopicProvisioner(IOptions<KafkaOptions> options, ILogger<KafkaTopicProvisioner> logger) : IHostedService
{
    public async Task StartAsync(CancellationToken cancellationToken)
    {
        var kafka = options.Value;
        if (!kafka.ProvisionTopics) return;

        using var admin = new AdminClientBuilder(new AdminClientConfig { BootstrapServers = kafka.BootstrapServers }).Build();

        var specs = new[]
        {
            Spec(kafka.ReadingsTopic, kafka.ReadingsPartitions, kafka,
                ("retention.ms", TimeSpan.FromHours(kafka.ReadingsRetentionHours).TotalMilliseconds.ToString("0")),
                ("compression.type", "producer")),
            // DLQ: poucas partições, retenção longa (mensagens venenosas precisam de análise humana)
            Spec(kafka.ReadingsDlqTopic, 1, kafka, ("retention.ms", TimeSpan.FromDays(30).TotalMilliseconds.ToString("0"))),
            Spec(kafka.AlertsTopic, kafka.AlertsPartitions, kafka, ("retention.ms", TimeSpan.FromDays(30).TotalMilliseconds.ToString("0")))
        };

        // O broker pode ainda estar subindo (docker compose): tenta por até ~60s.
        for (var attempt = 1; ; attempt++)
        {
            try
            {
                await admin.CreateTopicsAsync(specs, new CreateTopicsOptions { RequestTimeout = TimeSpan.FromSeconds(10) });
                logger.LogInformation("Tópicos Kafka criados: {Topics}", string.Join(", ", specs.Select(s => s.Name)));
                return;
            }
            catch (CreateTopicsException ex) when (ex.Results.All(r =>
                r.Error.Code is ErrorCode.NoError or ErrorCode.TopicAlreadyExists))
            {
                logger.LogInformation("Tópicos Kafka já existiam; nada a criar.");
                return;
            }
            catch (Exception ex) when (attempt < 12 && !cancellationToken.IsCancellationRequested)
            {
                logger.LogWarning("Kafka indisponível (tentativa {Attempt}/12): {Reason}", attempt, ex.Message);
                await Task.Delay(TimeSpan.FromSeconds(5), cancellationToken);
            }
        }
    }

    public Task StopAsync(CancellationToken cancellationToken) => Task.CompletedTask;

    private static TopicSpecification Spec(string name, int partitions, KafkaOptions kafka, params (string Key, string Value)[] config) => new()
    {
        Name = name,
        NumPartitions = partitions,
        ReplicationFactor = kafka.ReplicationFactor,
        Configs = config.ToDictionary(c => c.Key, c => c.Value)
    };
}
