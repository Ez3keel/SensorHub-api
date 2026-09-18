using SensorHub.Application.Contracts;

namespace SensorHub.Application.Ingestion;

/// <summary>Porta de saída da ingestão: entrega leituras ao log durável (Kafka).</summary>
public interface IReadingPublisher
{
    /// <summary>
    /// Publica o lote e só retorna quando TODAS as mensagens foram confirmadas pelo broker.
    /// Lança <see cref="IngestionUnavailableException"/> se qualquer uma falhar: o cliente refaz o
    /// lote inteiro e a idempotência (sensor_id, ts) no consumer absorve as repetições.
    /// </summary>
    Task PublishAsync(IReadOnlyList<ReadingMessage> messages, CancellationToken cancellationToken);
}

/// <summary>
/// O log durável não conseguiu aceitar as leituras (broker fora, fila local cheia, timeout).
/// Vira HTTP 503 + Retry-After: é assim que o backpressure chega ao dispositivo.
/// </summary>
public sealed class IngestionUnavailableException(string message, Exception? inner = null) : Exception(message, inner);
