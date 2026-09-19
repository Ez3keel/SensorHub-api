using System.Text;
using Confluent.Kafka;
using Microsoft.Extensions.Options;
using SensorHub.Application.Alerting;

namespace SensorHub.Infrastructure.Kafka;

/// <summary>
/// Publica os eventos de alerta em <c>sensorhub.alerts</c>. Chave = sensorId: os eventos de um sensor ficam
/// em ordem (um "Resolved" nunca é lido antes do "Fired" correspondente). Idempotente e com acks=all, como o
/// producer de leituras; a duplicata que ainda pode ocorrer (reprocessamento) é resolvida por quem consome, via AlertId+Kind.
/// </summary>
public sealed class KafkaAlertEventPublisher : IAlertEventPublisher, IDisposable
{
    private readonly IProducer<string, byte[]> _producer;
    private readonly string _topic;
    private int _disposed;

    public KafkaAlertEventPublisher(IOptions<KafkaOptions> options)
    {
        _topic = options.Value.AlertsTopic;
        _producer = new ProducerBuilder<string, byte[]>(new ProducerConfig
        {
            BootstrapServers = options.Value.BootstrapServers,
            ClientId = "sensorhub-alerts",
            Acks = Acks.All,
            EnableIdempotence = true,
            LingerMs = 5,
            MessageTimeoutMs = 15_000
        }).Build();
    }

    public async Task PublishAsync(IReadOnlyList<AlertEvent> events, CancellationToken cancellationToken)
    {
        var deliveries = events.Select(e => _producer.ProduceAsync(_topic, new Message<string, byte[]>
        {
            Key = e.SensorId.ToString("D"),
            Value = AlertEventSerializer.Serialize(e),
            Timestamp = new Timestamp(e.At),
            Headers = new Headers { { "event-kind", Encoding.UTF8.GetBytes(e.Kind.ToString()) } }
        }, cancellationToken)).ToList();

        await Task.WhenAll(deliveries);
    }

    public void Dispose()
    {
        if (Interlocked.Exchange(ref _disposed, 1) == 1) return;
        _producer.Flush(TimeSpan.FromSeconds(5));
        _producer.Dispose();
    }
}
