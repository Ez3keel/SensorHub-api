using Confluent.Kafka;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using SensorHub.Application.Contracts;
using SensorHub.Application.Ingestion;
using SensorHub.Application.Observability;
using System.Diagnostics;

namespace SensorHub.Infrastructure.Kafka;

/// <summary>
/// Producer de leituras. Decisões (detalhadas no ADR, Fase 1):
/// <list type="bullet">
/// <item><b>Chave = sensorId</b>: o particionador hasheia a chave, então todas as leituras de um sensor
/// caem na mesma partição, e dentro de uma partição a ordem é garantida.</item>
/// <item><b>acks=all + idempotência</b>: o líder só confirma após todas as réplicas in-sync gravarem, e o
/// producer não duplica nem reordena em retries internos.</item>
/// <item>Aguarda o <i>delivery report</i> de cada mensagem antes de responder 202: "aceito" significa
/// "durável no log", não "enfileirado na memória da API".</item>
/// </list>
/// </summary>
public sealed class KafkaReadingPublisher : IReadingPublisher, IDisposable
{
    private readonly IProducer<string, byte[]> _producer;
    private readonly string _topic;
    private readonly ILogger<KafkaReadingPublisher> _logger;
    private int _disposed;

    public KafkaReadingPublisher(IOptions<KafkaOptions> options, ILogger<KafkaReadingPublisher> logger)
    {
        var kafka = options.Value;
        _topic = kafka.ReadingsTopic;
        _logger = logger;

        var config = new ProducerConfig
        {
            BootstrapServers = kafka.BootstrapServers,
            ClientId = "sensorhub-api",
            Acks = Acks.All,
            EnableIdempotence = true,
            LingerMs = kafka.Producer.LingerMs,
            BatchSize = kafka.Producer.BatchSizeBytes,
            CompressionType = Enum.Parse<CompressionType>(kafka.Producer.Compression, ignoreCase: true),
            MessageTimeoutMs = kafka.Producer.MessageTimeoutMs,
            QueueBufferingMaxMessages = kafka.Producer.QueueMaxMessages,
            SocketKeepaliveEnable = true
        };

        _producer = new ProducerBuilder<string, byte[]>(config)
            .SetLogHandler((_, m) => _logger.Log(ToLogLevel(m.Level), "librdkafka: {Message}", m.Message))
            .SetErrorHandler((_, e) => _logger.LogWarning("Kafka producer error: {Reason} (fatal: {Fatal})", e.Reason, e.IsFatal))
            .Build();
    }

    public async Task PublishAsync(IReadOnlyList<ReadingMessage> messages, CancellationToken cancellationToken)
    {
        // Um span por lote publicado (não por mensagem). Seu contexto viaja nos headers de TODAS as mensagens do lote: é o elo
        // que liga a requisição HTTP ao consumer do outro lado da fila.
        using var activity = SensorHubTelemetry.Source.StartActivity("sensorhub.readings publish", ActivityKind.Producer);
        activity?.SetTag("messaging.system", "kafka");
        activity?.SetTag("messaging.destination.name", _topic);
        activity?.SetTag("messaging.batch.message_count", messages.Count);
        var clock = Stopwatch.StartNew();

        var deliveries = new List<Task<DeliveryResult<string, byte[]>>>(messages.Count);

        try
        {
            foreach (var message in messages)
            {
                var headers = new Headers { { "schema-version", [(byte)message.SchemaVersion] } };
                KafkaTraceContext.Inject(headers, activity);
                deliveries.Add(_producer.ProduceAsync(_topic, new Message<string, byte[]>
                {
                    Key = message.SensorId.ToString("D"),
                    Value = ReadingMessageSerializer.Serialize(message),
                    Timestamp = new Timestamp(message.Timestamp),
                    Headers = headers
                }, cancellationToken));
            }

            await Task.WhenAll(deliveries);
            SensorHubTelemetry.PublishDuration.Record(clock.Elapsed.TotalMilliseconds);
        }
        catch (ProduceException<string, byte[]> ex) when (ex.Error.Code == ErrorCode.Local_QueueFull)
        {
            activity?.SetStatus(ActivityStatusCode.Error, "fila local cheia");
            // Fila local cheia: o broker não está escoando tão rápido quanto chegamos. Backpressure.
            throw new IngestionUnavailableException("Fila de publicação cheia; tente novamente em instantes.", ex);
        }
        catch (Exception ex) when (ex is KafkaException or ProduceException<string, byte[]>)
        {
            activity?.SetStatus(ActivityStatusCode.Error, ex.Message);
            throw new IngestionUnavailableException("Não foi possível confirmar a publicação no Kafka.", ex);
        }
        catch (AggregateException ex)
        {
            throw new IngestionUnavailableException("Não foi possível confirmar a publicação no Kafka.", ex);
        }
    }

    public void Dispose()
    {
        // O contêiner de DI registra a mesma instância sob duas chaves (classe e interface) e chama
        // Dispose nas duas: sem este guard o segundo Flush estoura ObjectDisposedException.
        if (Interlocked.Exchange(ref _disposed, 1) == 1) return;

        try
        {
            _producer.Flush(TimeSpan.FromSeconds(5));
        }
        catch (Exception ex)
        {
            _logger.LogWarning(ex, "Falha ao dar flush no producer durante o shutdown.");
        }

        _producer.Dispose();
    }

    private static LogLevel ToLogLevel(SyslogLevel level) => level switch
    {
        SyslogLevel.Emergency or SyslogLevel.Alert or SyslogLevel.Critical => LogLevel.Critical,
        SyslogLevel.Error => LogLevel.Error,
        SyslogLevel.Warning => LogLevel.Warning,
        SyslogLevel.Notice or SyslogLevel.Info => LogLevel.Information,
        _ => LogLevel.Debug
    };
}
