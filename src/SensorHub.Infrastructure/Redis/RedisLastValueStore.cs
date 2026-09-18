using System.Globalization;
using Microsoft.Extensions.Options;
using SensorHub.Application.LatestValues;
using SensorHub.Domain.Readings;
using StackExchange.Redis;

namespace SensorHub.Infrastructure.Redis;

public sealed class RedisOptions
{
    public const string SectionName = "Redis";

    public string ConnectionString { get; set; } = "localhost:6380";
    public string KeyPrefix { get; set; } = "sensorhub:";

    /// <summary>
    /// Validade do último valor, renovada a cada escrita. Sensor aposentado não deixa lixo eterno; sensor
    /// vivo nunca expira. Chave expirada não é problema: a leitura faz cache-aside no banco.
    /// </summary>
    public int LastValueTtlDays { get; set; } = 7;
}

/// <summary>
/// Último valor por sensor em um HASH do Redis (<c>sensorhub:last:{id}</c> com campos <c>ts</c> e <c>value</c>).
/// A escrita é um script Lua: "grave só se o timestamp for MAIOR que o armazenado". O Redis executa scripts
/// de forma atômica, então a comparação e a escrita não podem ser interrompidas por outro escritor. Sem isso,
/// duas instâncias do consumer (ou uma reentrega) poderiam sobrescrever um valor novo por um antigo.
/// </summary>
public sealed class RedisLastValueStore(IConnectionMultiplexer redis, IOptions<RedisOptions> options) : ILastValueStore
{
    // KEYS[1] chave | ARGV[1] ts (microssegundos desde a época; inteiro exato em double até 2^53) | ARGV[2] valor | ARGV[3] ttl (s)
    private const string SetIfNewerScript = """
        local current = redis.call('HGET', KEYS[1], 'ts')
        if current == false or tonumber(ARGV[1]) > tonumber(current) then
            redis.call('HSET', KEYS[1], 'ts', ARGV[1], 'value', ARGV[2])
            redis.call('EXPIRE', KEYS[1], ARGV[3])
            return 1
        end
        return 0
        """;

    private static readonly RedisValue[] Fields = ["ts", "value"];

    private readonly string _prefix = options.Value.KeyPrefix;
    private readonly int _ttlSeconds = (int)TimeSpan.FromDays(options.Value.LastValueTtlDays).TotalSeconds;

    private RedisKey Key(Guid sensorId) => $"{_prefix}last:{sensorId:D}";

    public async Task<int> SetIfNewerAsync(IReadOnlyCollection<Reading> readings, CancellationToken cancellationToken)
    {
        if (readings.Count == 0) return 0;

        var db = redis.GetDatabase();
        var batch = db.CreateBatch(); // pipeline: um único round trip para todos os scripts
        var results = new List<Task<RedisResult>>(readings.Count);

        foreach (var reading in readings)
        {
            results.Add(batch.ScriptEvaluateAsync(
                SetIfNewerScript,
                [Key(reading.SensorId)],
                [ToMicros(reading.Timestamp), reading.Value.ToString("R", CultureInfo.InvariantCulture), _ttlSeconds]));
        }

        batch.Execute();
        var outcomes = await Task.WhenAll(results);
        return outcomes.Count(r => (int)r == 1);
    }

    public async Task<LastValue?> GetAsync(Guid sensorId, CancellationToken cancellationToken)
    {
        var values = await redis.GetDatabase().HashGetAsync(Key(sensorId), Fields);
        return Parse(sensorId, values);
    }

    public async Task<IReadOnlyDictionary<Guid, LastValue>> GetManyAsync(IReadOnlyCollection<Guid> sensorIds, CancellationToken cancellationToken)
    {
        var ids = sensorIds.Distinct().ToList();
        var batch = redis.GetDatabase().CreateBatch();
        var pending = ids.Select(id => (Id: id, Task: batch.HashGetAsync(Key(id), Fields))).ToList();
        batch.Execute();

        var found = new Dictionary<Guid, LastValue>();
        foreach (var (id, task) in pending)
            if (Parse(id, await task) is { } value) found[id] = value;
        return found;
    }

    private static LastValue? Parse(Guid sensorId, RedisValue[] fields)
    {
        if (fields.Length != 2 || fields[0].IsNull || fields[1].IsNull) return null;

        var micros = long.Parse((string)fields[0]!, CultureInfo.InvariantCulture);
        var value = double.Parse((string)fields[1]!, CultureInfo.InvariantCulture);
        return new LastValue(sensorId, FromMicros(micros), value);
    }

    private static long ToMicros(DateTimeOffset timestamp) => (timestamp.UtcTicks - DateTimeOffset.UnixEpoch.UtcTicks) / 10;

    private static DateTimeOffset FromMicros(long micros) => new(DateTimeOffset.UnixEpoch.UtcTicks + micros * 10, TimeSpan.Zero);
}
