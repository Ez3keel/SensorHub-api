using SensorHub.Domain.Common;

namespace SensorHub.Domain.Readings;

/// <summary>
/// Uma medição de um sensor em um instante. É imutável e sua identidade natural é
/// (<see cref="SensorId"/>, <see cref="Timestamp"/>): um sensor não mede duas vezes no mesmo
/// instante, então reenvios e reentregas do Kafka produzem a mesma chave e viram no-op no banco.
/// </summary>
public readonly record struct Reading
{
    public Guid SensorId { get; }
    public DateTimeOffset Timestamp { get; }
    public double Value { get; }

    private Reading(Guid sensorId, DateTimeOffset timestamp, double value)
    {
        SensorId = sensorId;
        Timestamp = timestamp;
        Value = value;
    }

    public static Reading Create(Guid sensorId, DateTimeOffset timestamp, double value)
    {
        if (sensorId == Guid.Empty)
            throw new DomainException("SensorId não pode ser vazio.");

        if (!double.IsFinite(value))
            throw new DomainException("O valor da leitura deve ser um número finito (NaN/Infinity são rejeitados).");

        return new Reading(sensorId, NormalizeTimestamp(timestamp), value);
    }

    /// <summary>
    /// Normaliza para UTC e trunca para microssegundos, a precisão do timestamptz do PostgreSQL.
    /// Sem isso, dois timestamps que diferem em ticks (100ns) seriam "distintos" na mensagem mas
    /// colidiriam na chave (sensor_id, ts) do banco, quebrando a idempotência.
    /// </summary>
    public static DateTimeOffset NormalizeTimestamp(DateTimeOffset timestamp)
    {
        var utc = timestamp.ToUniversalTime();
        var truncatedTicks = utc.Ticks - (utc.Ticks % 10);
        return new DateTimeOffset(truncatedTicks, TimeSpan.Zero);
    }
}
