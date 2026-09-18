using SensorHub.Domain.Readings;

namespace SensorHub.Application.Processing;

/// <summary>Porta de escrita do histórico de leituras (série temporal).</summary>
public interface IReadingStore
{
    /// <summary>
    /// Grava o lote em UMA operação, ignorando leituras cuja chave (sensor_id, ts) já existe.
    /// Retorna quantas linhas foram realmente inseridas (o restante eram duplicatas).
    /// </summary>
    Task<int> InsertBatchAsync(IReadOnlyList<Reading> readings, CancellationToken cancellationToken);
}
