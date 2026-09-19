using System.Diagnostics.Metrics;
using SensorHub.Application.Observability;

namespace SensorHub.MqttBridge;

/// <summary>Métricas do bridge, no mesmo Meter "SensorHub" (o host já o exporta). Rótulos de baixa cardinalidade: nunca o id do dispositivo.</summary>
public static class MqttMetrics
{
    /// <summary>Resultado de cada mensagem MQTT: accepted, partial, rejected, malformed, retry (falha de publicação).</summary>
    public static readonly Counter<long> Messages = SensorHubTelemetry.Meter.CreateCounter<long>(
        "sensorhub.mqtt.messages", "{message}", "Mensagens MQTT tratadas pelo bridge, por resultado.");

    public static readonly Counter<long> Reconnects = SensorHubTelemetry.Meter.CreateCounter<long>(
        "sensorhub.mqtt.reconnects", "{event}", "Reconexões do bridge ao broker.");

    private static int _connected;

    public static bool IsConnected => Volatile.Read(ref _connected) == 1;

    public static void SetConnected(bool connected) => Interlocked.Exchange(ref _connected, connected ? 1 : 0);

    public static readonly ObservableGauge<int> Connected = SensorHubTelemetry.Meter.CreateObservableGauge(
        "sensorhub.mqtt.connected", () => Volatile.Read(ref _connected), "{state}", "1 se o bridge está conectado ao broker MQTT.");
}
