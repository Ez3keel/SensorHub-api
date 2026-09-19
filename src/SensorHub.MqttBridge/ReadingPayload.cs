using System.Text.Json;
using SensorHub.Application.Ingestion;

namespace SensorHub.MqttBridge;

public sealed record PayloadParseResult(IReadOnlyList<ReadingRequest>? Readings, string? Error)
{
    public bool IsValid => Readings is not null;
}

/// <summary>Payload MQTT: um objeto <c>{sensorId, timestamp?, value, unit?}</c> ou um array deles (buffer reenviado após ficar offline).</summary>
public static class ReadingPayload
{
    private static readonly JsonSerializerOptions Json = new(JsonSerializerDefaults.Web);

    public static PayloadParseResult Parse(ReadOnlySpan<byte> payload, int maxBatchSize)
    {
        if (payload.IsEmpty) return new(null, "payload vazio");

        try
        {
            var reader = new Utf8JsonReader(payload);
            if (!reader.Read()) return new(null, "payload vazio");

            switch (reader.TokenType)
            {
                case JsonTokenType.StartObject:
                    var single = JsonSerializer.Deserialize<ReadingRequest>(payload, Json);
                    return single is null ? new(null, "objeto inválido") : new([single], null);

                case JsonTokenType.StartArray:
                    var many = JsonSerializer.Deserialize<List<ReadingRequest?>>(payload, Json);
                    if (many is null || many.Count == 0) return new(null, "lote vazio");
                    if (many.Any(r => r is null)) return new(null, "lote com elemento nulo");
                    if (many.Count > maxBatchSize) return new(null, $"lote com {many.Count} leituras (máximo {maxBatchSize})");
                    return new(many!, null);

                default:
                    return new(null, "esperado um objeto ou um array JSON");
            }
        }
        catch (JsonException ex)
        {
            return new(null, $"JSON inválido: {ex.Message}");
        }
    }
}
