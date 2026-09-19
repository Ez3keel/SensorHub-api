namespace SensorHub.MqttBridge;

/// <summary>
/// Esquema de tópicos: <c>sensorhub/devices/{deviceId}/readings</c>. O id do dispositivo no tópico é confiável porque o broker só deixa
/// um dispositivo publicar no PRÓPRIO tópico (ver <see cref="MqttAccessPolicy"/>); o bridge deriva a identidade dele do tópico, não do payload.
/// </summary>
public static class MqttTopics
{
    public const string Root = "sensorhub/devices";

    public static string ReadingsTopic(Guid deviceId) => $"{Root}/{deviceId:D}/readings";

    /// <summary>Assinatura compartilhada: várias réplicas do bridge dividem as mensagens em vez de cada uma receber todas.</summary>
    public static string SharedSubscription(string group) => $"$share/{group}/{Root}/+/readings";

    public static bool TryGetDeviceId(string? topic, out Guid deviceId)
    {
        deviceId = Guid.Empty;
        if (topic is null) return false;

        var parts = topic.Split('/');
        return parts.Length == 4
               && parts[0] == "sensorhub" && parts[1] == "devices" && parts[3] == "readings"
               && Guid.TryParseExact(parts[2], "D", out deviceId);
    }
}
