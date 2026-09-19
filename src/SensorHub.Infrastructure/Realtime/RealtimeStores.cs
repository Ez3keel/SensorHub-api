using Microsoft.Extensions.Options;
using Npgsql;
using SensorHub.Application.Realtime;
using SensorHub.Infrastructure.Redis;
using StackExchange.Redis;

namespace SensorHub.Infrastructure.Realtime;

/// <summary>
/// Diretório sensor → grupo em memória, recarregado do banco a cada <c>cacheSeconds</c>. O fanout de tempo real consulta o
/// grupo de cada leitura a cada flush (milhares de vezes por segundo): ir ao banco a cada consulta seria proibitivo.
/// Um sensor recém-cadastrado leva até o TTL para aparecer nos grupos do dashboard.
/// </summary>
public sealed class CachedSensorDirectory(NpgsqlDataSource dataSource, TimeProvider clock, int cacheSeconds = 15) : ISensorDirectory
{
    private readonly SemaphoreSlim _refresh = new(1, 1);
    private (DateTimeOffset LoadedAt, Dictionary<Guid, string> Groups)? _snapshot;

    public async Task<string?> GetGroupAsync(Guid sensorId, CancellationToken cancellationToken)
    {
        var snapshot = await GetSnapshotAsync(cancellationToken);
        return snapshot.TryGetValue(sensorId, out var group) ? group : null;
    }

    private async Task<Dictionary<Guid, string>> GetSnapshotAsync(CancellationToken cancellationToken)
    {
        var ttl = TimeSpan.FromSeconds(cacheSeconds);
        if (_snapshot is { } fresh && clock.GetUtcNow() - fresh.LoadedAt < ttl) return fresh.Groups;

        await _refresh.WaitAsync(cancellationToken);
        try
        {
            if (_snapshot is { } again && clock.GetUtcNow() - again.LoadedAt < ttl) return again.Groups;

            var groups = new Dictionary<Guid, string>();
            await using var command = dataSource.CreateCommand("SELECT id, group_name FROM sensors");
            await using var reader = await command.ExecuteReaderAsync(cancellationToken);
            while (await reader.ReadAsync(cancellationToken))
                groups[reader.GetGuid(0)] = reader.GetString(1);

            _snapshot = (clock.GetUtcNow(), groups);
            return groups;
        }
        finally
        {
            _refresh.Release();
        }
    }
}

/// <summary>
/// Registro, no Redis, de quantos dashboards assistem cada sensor em detalhe (HASH sensorId → contagem). É compartilhado
/// por todas as instâncias da API: quem consome a partição do sensor pode não ser quem tem o navegador conectado.
/// A contagem chega a zero (e o campo some) de forma atômica via Lua. Limitação assumida: se uma instância morre sem
/// desconectar seus clientes, as contagens dela vazam; o efeito é só empurrar mensagens por sensor para um grupo vazio,
/// e a chave inteira expira sozinha após 1 h sem escritas.
/// </summary>
public sealed class RedisSubscriptionRegistry(IConnectionMultiplexer redis, IOptions<RedisOptions> options) : ISubscriptionRegistry
{
    private const string AdjustScript = """
        local n = redis.call('HINCRBY', KEYS[1], ARGV[1], ARGV[2])
        if n <= 0 then redis.call('HDEL', KEYS[1], ARGV[1]) end
        redis.call('EXPIRE', KEYS[1], 3600)
        return n
        """;

    private RedisKey Key => $"{options.Value.KeyPrefix}rt:sensor-subs";

    public Task AddAsync(Guid sensorId, CancellationToken cancellationToken) => Adjust(sensorId, +1);

    public Task RemoveAsync(Guid sensorId, CancellationToken cancellationToken) => Adjust(sensorId, -1);

    public async Task<IReadOnlySet<Guid>> GetSubscribedSensorsAsync(CancellationToken cancellationToken)
    {
        var fields = await redis.GetDatabase().HashKeysAsync(Key);
        return fields.Select(f => Guid.Parse((string)f!)).ToHashSet();
    }

    private Task Adjust(Guid sensorId, int delta) =>
        redis.GetDatabase().ScriptEvaluateAsync(AdjustScript, [Key], [sensorId.ToString("D"), delta]);
}
