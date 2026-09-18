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
    string ApiKey)
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
            ApiKey: Get("api-key", ""));

        var unknown = map.Keys.Except(
            ["mode", "sensors", "rate", "duration", "batch", "workers", "duplicates", "hot", "url", "api-key"],
            StringComparer.OrdinalIgnoreCase).ToList();
        if (unknown.Count > 0)
            throw new ArgumentException($"Opção desconhecida: {string.Join(", ", unknown.Select(u => "--" + u))}");

        return settings;
    }

    public const string Usage = """
        SensorHub.Simulator: gerador de carga de leituras de sensores

          --mode dry-run|http     destino das leituras (padrão: dry-run, só mede o gerador)
          --sensors N             quantidade de sensores da frota (padrão 100)
          --rate N                leituras por segundo, total (padrão 1000)
          --duration S            duração em segundos (padrão 10)
          --batch N               leituras por requisição/lote (padrão 100)
          --workers N             requisições concorrentes (padrão 1)
          --duplicates P          probabilidade [0,1] de reenviar leitura idêntica (padrão 0)
          --hot N                 o sensor #0 emite N vezes mais que os outros (padrão 1)
          --url URL               base da API no modo http (padrão http://localhost:5080)
          --api-key KEY           chave de API do dispositivo (modo http)
        """;
}
