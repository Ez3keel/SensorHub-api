using System.Globalization;

namespace SensorHub.Simulator;

/// <summary>Configuração da linha de comando: <c>--chave valor</c> (sem dependências externas).</summary>
public sealed record SimulatorSettings(
    string Mode,
    int Sensors,
    int Rate,
    int DurationSeconds,
    int BatchSize,
    int Workers,
    double DuplicateProbability,
    int HotSensorFactor,
    string Url,
    string ApiKey,
    string Rules,
    int NoDataSeconds,
    int SilenceAfterSeconds,
    double SilenceFraction,
    string AdminEmail = "",
    string AdminPassword = "",
    string KeyOut = "",
    string MqttHost = "localhost",
    int MqttPort = 1884)
{
    public static SimulatorSettings Parse(string[] args)
    {
        var map = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase);
        for (var i = 0; i < args.Length; i++)
        {
            if (!args[i].StartsWith("--", StringComparison.Ordinal))
                throw new ArgumentException($"Argumento inesperado '{args[i]}'. Use --chave valor.");
            if (i + 1 >= args.Length)
                throw new ArgumentException($"Falta o valor de '{args[i]}'.");
            map[args[i][2..]] = args[++i];
        }

        string Get(string key, string fallback) => map.TryGetValue(key, out var v) ? v : fallback;
        int GetInt(string key, int fallback) => int.Parse(Get(key, fallback.ToString(CultureInfo.InvariantCulture)), CultureInfo.InvariantCulture);
        double GetDouble(string key, double fallback) => double.Parse(Get(key, fallback.ToString(CultureInfo.InvariantCulture)), CultureInfo.InvariantCulture);

        var settings = new SimulatorSettings(
            Mode: Get("mode", "dry-run").ToLowerInvariant(),
            Sensors: GetInt("sensors", 100),
            Rate: GetInt("rate", 1000),
            DurationSeconds: GetInt("duration", 10),
            BatchSize: GetInt("batch", 100),
            Workers: GetInt("workers", 1),
            DuplicateProbability: GetDouble("duplicates", 0),
            HotSensorFactor: GetInt("hot", 1),
            Url: Get("url", "http://localhost:5080"),
            ApiKey: Get("api-key", ""),
            Rules: Get("rules", ""),
            NoDataSeconds: GetInt("nodata-seconds", 20),
            SilenceAfterSeconds: GetInt("silence-after", 0),
            SilenceFraction: GetDouble("silence-fraction", 0),
            AdminEmail: Get("admin-email", ""),
            // a senha também pode vir do ambiente: argumentos de linha de comando ficam no histórico do shell
            AdminPassword: Get("admin-password", Environment.GetEnvironmentVariable("SENSORHUB_ADMIN_PASSWORD") ?? ""),
            KeyOut: Get("key-out", ""),
            MqttHost: Get("mqtt-host", "localhost"),
            MqttPort: GetInt("mqtt-port", 1884));

        var unknown = map.Keys.Except(
            ["mode", "sensors", "rate", "duration", "batch", "workers", "duplicates", "hot", "url", "api-key", "rules", "nodata-seconds", "silence-after", "silence-fraction", "admin-email", "admin-password", "key-out", "mqtt-host", "mqtt-port"],
            StringComparer.OrdinalIgnoreCase).ToList();
        if (unknown.Count > 0)
            throw new ArgumentException($"Opção desconhecida: {string.Join(", ", unknown.Select(u => "--" + u))}");

        return settings;
    }

    public const string Usage = """
        SensorHub.Simulator: gerador de carga de leituras de sensores

          --mode dry-run|http|mqtt|register
                                  dry-run: só mede o gerador | http: envia leituras | mqtt: publica no broker | register: cadastra a frota
          --sensors N             quantidade de sensores da frota (padrão 100)
          --rate N                leituras por segundo, total (padrão 1000)
          --duration S            duração em segundos (padrão 10)
          --batch N               leituras por requisição/lote (padrão 100)
          --workers N             requisições concorrentes (padrão 1)
          --duplicates P          probabilidade [0,1] de reenviar leitura idêntica (padrão 0)
          --hot N                 o sensor #0 emite N vezes mais que os outros (padrão 1)
          --url URL               base da API no modo http (padrão http://localhost:5080)
          --mqtt-host H --mqtt-port P
                                  (mqtt) broker MQTT (padrão localhost:1884); usuário = id do gateway, senha = --api-key
          --api-key KEY           chave de API do dispositivo (modo http; a frota inteira é um gateway/dispositivo)
          --admin-email E         (register) administrador da API, se a segurança estiver ligada
          --admin-password P      (register) senha (ou variável SENSORHUB_ADMIN_PASSWORD)
          --key-out ARQUIVO       (register) grava aqui a chave de API criada para o dispositivo
          --rules LIST            (register) regras a criar por sensor: threshold,nodata
          --nodata-seconds N      (register) silêncio máximo da regra "sem dados" (padrão 20)
          --silence-after S       (http) depois de S segundos, parte da frota para de emitir
          --silence-fraction P    (http) fração [0,1] da frota que "morre" em --silence-after
        """;
}
