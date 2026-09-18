namespace SensorHub.Infrastructure.Kafka;

/// <summary>Configuração de um consumer em lote. Cada consumer group é uma "visão" independente do mesmo tópico.</summary>
public sealed class BatchConsumerOptions
{
    /// <summary>
    /// Identidade do grupo. Consumers com o mesmo GroupId dividem as partições; GroupIds diferentes
    /// leem o tópico inteiro cada um, com offsets próprios.
    /// </summary>
    public string GroupId { get; set; } = "sensorhub.persistence";

    /// <summary>Tópico lido; nulo = tópico de leituras.</summary>
    public string? Topic { get; set; }

    /// <summary>Teto de mensagens por lote. Lotes maiores = menos round trips, mais latência e mais reprocessamento numa falha.</summary>
    public int MaxBatchSize { get; set; } = 5000;

    /// <summary>Espera máxima para completar um lote depois da primeira mensagem (vazão vs. latência).</summary>
    public int MaxWaitMs { get; set; } = 250;

    /// <summary>De onde começar quando o grupo não tem offset commitado: Earliest (não perder nada) ou Latest.</summary>
    public bool StartFromEarliest { get; set; } = true;

    public int InitialRetryDelayMs { get; set; } = 200;
    public int MaxRetryDelayMs { get; set; } = 10_000;

    /// <summary>Tempo máximo entre polls. Se um lote demorar mais que isso o broker expulsa o consumer do grupo.</summary>
    public int MaxPollIntervalMs { get; set; } = 300_000;
}
