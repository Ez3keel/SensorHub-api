namespace SensorHub.MqttBridge;

// MQTT -> Kafka. Também é o backend HTTP de autenticação do Mosquitto (/mqtt/*), /metrics e /health/*.
// Ponto de entrada explícito (e não top-level statements) para NÃO gerar um tipo "Program" global que colidiria com o da API
// nos projetos que referenciam os dois (testes de integração).
internal static class BridgeEntryPoint
{
    public static void Main(string[] args) => MqttBridgeHost.Build(args).Run();
}
