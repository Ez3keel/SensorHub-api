namespace SensorHub.Application.Ingestion;

/// <summary>Uma leitura como chega pela borda HTTP. <c>Timestamp</c> nulo = "use a hora do servidor" (dispositivo sem relógio).</summary>
public sealed record ReadingRequest(Guid SensorId, DateTimeOffset? Timestamp, double Value, string? Unit);

public sealed record RejectedReading(int Index, IReadOnlyList<string> Errors);

public sealed record IngestionResult(int Accepted, IReadOnlyList<RejectedReading> Rejected)
{
    public bool HasRejections => Rejected.Count > 0;
}

public sealed class IngestionOptions
{
    public const string SectionName = "Ingestion";

    /// <summary>Máximo de leituras por requisição de batch. Limita memória e latência por request.</summary>
    public int MaxBatchSize { get; set; } = 1000;

    /// <summary>Tolerância para timestamps no futuro (relógio de dispositivo desregulado).</summary>
    public TimeSpan MaxFutureSkew { get; set; } = TimeSpan.FromMinutes(5);

    /// <summary>Idade máxima aceita. Dispositivos que ficaram offline reenviam o buffer, mas não de meses atrás.</summary>
    public TimeSpan MaxAge { get; set; } = TimeSpan.FromDays(7);
}
