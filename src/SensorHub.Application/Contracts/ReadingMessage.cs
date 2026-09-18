using System.Text.Json;
using System.Text.Json.Serialization;
using SensorHub.Domain.Readings;

namespace SensorHub.Application.Contracts;

/// <summary>
/// Contrato da mensagem no tópico de leituras. É um contrato PÚBLICO entre a API (producer) e vários
/// consumers (persistência, alertas, tempo real): tem <see cref="SchemaVersion"/> para evoluir sem
/// quebrar consumers antigos. Campos novos devem ser opcionais.
/// </summary>
public sealed record ReadingMessage(
    int SchemaVersion,
    Guid SensorId,
    DateTimeOffset Timestamp,
    double Value,
    string? Unit,
    DateTimeOffset IngestedAt)
{
    public const int CurrentSchemaVersion = 1;

    public static ReadingMessage From(Reading reading, string? unit, DateTimeOffset ingestedAt) =>
        new(CurrentSchemaVersion, reading.SensorId, reading.Timestamp, reading.Value, unit, ingestedAt);

    /// <summary>Reconstrói o value object; passa de novo pelas invariantes do domínio (defesa em profundidade).</summary>
    public Reading ToReading() => Reading.Create(SensorId, Timestamp, Value);
}

[JsonSourceGenerationOptions(
    PropertyNamingPolicy = JsonKnownNamingPolicy.CamelCase,
    DefaultIgnoreCondition = JsonIgnoreCondition.WhenWritingNull)]
[JsonSerializable(typeof(ReadingMessage))]
public sealed partial class ReadingMessageJsonContext : JsonSerializerContext;

public static class ReadingMessageSerializer
{
    public static byte[] Serialize(ReadingMessage message) =>
        JsonSerializer.SerializeToUtf8Bytes(message, ReadingMessageJsonContext.Default.ReadingMessage);

    public static ReadingMessage? Deserialize(ReadOnlySpan<byte> utf8Json) =>
        JsonSerializer.Deserialize(utf8Json, ReadingMessageJsonContext.Default.ReadingMessage);
}
