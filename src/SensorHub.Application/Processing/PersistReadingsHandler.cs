using Microsoft.Extensions.Logging;
using SensorHub.Application.Observability;

namespace SensorHub.Application.Processing;

/// <summary>Handler de persistência: grava o lote no histórico. Idempotente via (sensor_id, ts).</summary>
public sealed class PersistReadingsHandler(IReadingStore store, ILogger<PersistReadingsHandler> logger) : IReadingBatchHandler
{
    public async Task HandleAsync(IReadOnlyList<ConsumedReading> batch, CancellationToken cancellationToken)
    {
        var inserted = await store.InsertBatchAsync(batch.Select(b => b.Reading).ToList(), cancellationToken);

        var duplicates = batch.Count - inserted;
        SensorHubTelemetry.PersistedRows.Add(inserted, new KeyValuePair<string, object?>("result", "inserted"));
        SensorHubTelemetry.PersistedRows.Add(duplicates, new KeyValuePair<string, object?>("result", "duplicate"));
        if (duplicates > 0)
            logger.LogInformation("Lote de {Count} leituras: {Inserted} inseridas, {Duplicates} duplicatas ignoradas.",
                batch.Count, inserted, duplicates);
    }
}
