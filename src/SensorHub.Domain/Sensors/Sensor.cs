using SensorHub.Domain.Common;

namespace SensorHub.Domain.Sensors;

/// <summary>
/// Um canal de medição (uma métrica). Um dispositivo físico pode ter vários sensores; a chave
/// de partição do Kafka e a unidade de ordenação é o sensor, não o dispositivo.
/// </summary>
public sealed class Sensor
{
    public Guid Id { get; private set; }
    public Guid DeviceId { get; private set; }
    public string Name { get; private set; } = null!;
    public MetricType Metric { get; private set; }
    public string Unit { get; private set; } = null!;

    /// <summary>Tag livre de agrupamento (ex: "fabrica-1"); alimenta os grupos do dashboard em tempo real.</summary>
    public string Group { get; private set; } = null!;

    public bool Active { get; private set; }
    public DateTimeOffset CreatedAt { get; private set; }

    private Sensor() { } // EF

    /// <param name="id">
    /// Id opcional: importar/registrar sensores que já têm identidade fora do SensorHub (frota existente, simulador)
    /// exige preservá-la, senão as leituras já publicadas apontariam para sensores "desconhecidos".
    /// </param>
    public static Sensor Create(Guid deviceId, string name, MetricType metric, string unit, string group, DateTimeOffset now, Guid? id = null)
    {
        if (deviceId == Guid.Empty) throw new DomainException("DeviceId não pode ser vazio.");
        if (!Enum.IsDefined(metric)) throw new DomainException("Métrica inválida.");

        return new Sensor
        {
            Id = id is { } given && given != Guid.Empty ? given : Guid.NewGuid(),
            DeviceId = deviceId,
            Name = RequireText(name, nameof(name), 100),
            Metric = metric,
            Unit = RequireText(unit, nameof(unit), 16),
            Group = RequireText(group, nameof(group), 100),
            Active = true,
            CreatedAt = now
        };
    }

    public void Deactivate() => Active = false;

    public void Activate() => Active = true;

    /// <summary>Valor plausível para esta métrica? (Regra de borda: ver <see cref="MetricTypeExtensions.PlausibleRange"/>.)</summary>
    public bool AcceptsValue(double value) => Metric.IsPlausible(value);

    private static string RequireText(string? value, string field, int maxLength)
    {
        if (string.IsNullOrWhiteSpace(value)) throw new DomainException($"{field} é obrigatório.");
        var trimmed = value.Trim();
        if (trimmed.Length > maxLength) throw new DomainException($"{field} excede {maxLength} caracteres.");
        return trimmed;
    }
}
