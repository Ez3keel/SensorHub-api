using Microsoft.Extensions.Logging;
using SensorHub.Application.Processing;
using SensorHub.Application.Queries;
using SensorHub.Domain.Readings;

namespace SensorHub.Application.LatestValues;

/// <summary>Último valor conhecido de um sensor (o "estado quente").</summary>
public readonly record struct LastValue(Guid SensorId, DateTimeOffset Timestamp, double Value);

/// <summary>
/// Cache do último valor de cada sensor. É ESTADO DERIVADO: o histórico no banco é a fonte da verdade e
/// este cache pode ser perdido e reconstruído (reprocessando o tópico ou por cache-aside na leitura).
/// </summary>
public interface ILastValueStore
{
    /// <summary>
    /// Grava o valor de cada leitura SOMENTE se for mais novo que o armazenado. Leituras atrasadas ou
    /// reentregues não regridem o estado. Retorna quantos sensores foram efetivamente atualizados.
    /// </summary>
    Task<int> SetIfNewerAsync(IReadOnlyCollection<Reading> readings, CancellationToken cancellationToken);

    Task<LastValue?> GetAsync(Guid sensorId, CancellationToken cancellationToken);

    Task<IReadOnlyDictionary<Guid, LastValue>> GetManyAsync(IReadOnlyCollection<Guid> sensorIds, CancellationToken cancellationToken);
}

/// <summary>
/// Handler do grupo <c>sensorhub.lastvalue</c>: mantém o cache quente. Reduz o lote ao valor MAIS NOVO por
/// sensor antes de escrever (um lote de 5000 leituras de 500 sensores vira 500 escritas, não 5000).
/// </summary>
public sealed class UpdateLastValueHandler(ILastValueStore store, ILogger<UpdateLastValueHandler> logger) : IReadingBatchHandler
{
    public async Task HandleAsync(IReadOnlyList<ConsumedReading> batch, CancellationToken cancellationToken)
    {
        var newestPerSensor = batch
            .GroupBy(b => b.Reading.SensorId)
            .Select(g => g.Select(b => b.Reading).MaxBy(r => r.Timestamp))
            .ToList();

        var updated = await store.SetIfNewerAsync(newestPerSensor, cancellationToken);
        logger.LogDebug("Último valor: {Batch} leituras -> {Sensors} sensores, {Updated} atualizados.", batch.Count, newestPerSensor.Count, updated);
    }
}

/// <summary>
/// Leitura do último valor com padrão <b>cache-aside</b>: tenta o Redis (O(1)); se não há (chave expirada,
/// Redis reiniciado, sensor nunca visto pelo consumer), busca no banco e repovoa o cache.
/// </summary>
public sealed class LastValueService(ILastValueStore store, IReadingQueries queries)
{
    public async Task<LastValue?> GetAsync(Guid sensorId, CancellationToken cancellationToken)
    {
        var cached = await store.GetAsync(sensorId, cancellationToken);
        if (cached is not null) return cached;

        return await LoadFromDatabaseAsync(sensorId, cancellationToken);
    }

    public async Task<IReadOnlyDictionary<Guid, LastValue>> GetManyAsync(IReadOnlyCollection<Guid> sensorIds, CancellationToken cancellationToken)
    {
        var found = new Dictionary<Guid, LastValue>(await store.GetManyAsync(sensorIds, cancellationToken));

        foreach (var missing in sensorIds.Distinct().Where(id => !found.ContainsKey(id)))
        {
            var loaded = await LoadFromDatabaseAsync(missing, cancellationToken);
            if (loaded is { } value) found[missing] = value;
        }

        return found;
    }

    private async Task<LastValue?> LoadFromDatabaseAsync(Guid sensorId, CancellationToken cancellationToken)
    {
        var latest = await queries.GetLatestAsync(sensorId, cancellationToken);
        if (latest is null) return null;

        var (timestamp, value) = (latest.Value.Timestamp, latest.Value.Value);
        await store.SetIfNewerAsync([Reading.Create(sensorId, timestamp, value)], cancellationToken);
        return new LastValue(sensorId, timestamp, value);
    }
}
