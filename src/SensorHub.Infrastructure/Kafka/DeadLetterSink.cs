using System.Text;
using Confluent.Kafka;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;

namespace SensorHub.Infrastructure.Kafka;

/// <summary>Uma mensagem que não pôde ser processada e foi desviada para análise humana.</summary>
public sealed record DeadLetter(
    string? Key,
    byte[]? Value,
    string Reason,
    string SourceTopic,
    int SourcePartition,
    long SourceOffset,
    string ConsumerGroup);

public interface IDeadLetterSink
{
    Task SendAsync(DeadLetter letter, CancellationToken cancellationToken);
}

/// <summary>
/// Dead-letter queue: em vez de travar a partição inteira por causa de UMA mensagem venenosa (que nunca
/// será processável, por mais que se repita), ela é desviada para outro tópico com o contexto do erro
/// (motivo, origem, offset), e o consumer segue. O payload original é preservado byte a byte.
/// </summary>
public sealed class KafkaDeadLetterSink : IDeadLetterSink, IDisposable
{
    private readonly IProducer<string?, byte[]?> _producer;
    private readonly string _topic;
    private int _disposed;

    public KafkaDeadLetterSink(IOptions<KafkaOptions> options)
    {
        _topic = options.Value.ReadingsDlqTopic;
        _producer = new ProducerBuilder<string?, byte[]?>(new ProducerConfig
        {
            BootstrapServers = options.Value.BootstrapServers,
            ClientId = "sensorhub-dlq",
            Acks = Acks.All,
            EnableIdempotence = true
        }).Build();
    }

    public async Task SendAsync(DeadLetter letter, CancellationToken cancellationToken)
    {
        await _producer.ProduceAsync(_topic, new Message<string?, byte[]?>
        {
            Key = letter.Key,
            Value = letter.Value,
            Headers = new Headers
            {
                { "dlq-reason", Encoding.UTF8.GetBytes(Truncate(letter.Reason, 500)) },
                { "dlq-source-topic", Encoding.UTF8.GetBytes(letter.SourceTopic) },
                { "dlq-source-partition", Encoding.UTF8.GetBytes(letter.SourcePartition.ToString()) },
                { "dlq-source-offset", Encoding.UTF8.GetBytes(letter.SourceOffset.ToString()) },
                { "dlq-consumer-group", Encoding.UTF8.GetBytes(letter.ConsumerGroup) }
            }
        }, cancellationToken);
    }

    public void Dispose()
    {
        if (Interlocked.Exchange(ref _disposed, 1) == 1) return;
        _producer.Flush(TimeSpan.FromSeconds(5));
        _producer.Dispose();
    }

    private static string Truncate(string value, int max) => value.Length <= max ? value : value[..max];
}
