using System.Text.Json;
using Microsoft.Extensions.Options;
using SensorHub.Application.Alerting;
using SensorHub.Domain.Alerts;
using StackExchange.Redis;

namespace SensorHub.Infrastructure.Redis;

/// <summary>
/// Estado de avaliação das regras (a máquina de estado e as janelas deslizantes, que são os "contadores de janela")
/// como JSON em uma chave por regra. NÃO há lock: o Kafka garante que as leituras de um sensor chegam em ordem a um
/// único consumer do grupo, então há um único escritor por regra. O que vale é a dupla proteção do domínio
/// (timestamp maior que o último avaliado), então reentrega de um lote não corrompe o estado.
/// Sem TTL: o estado vive enquanto a regra existe (é removido ao desabilitar/excluir a regra).
/// </summary>
public sealed class RedisRuleStateStore(IConnectionMultiplexer redis, IOptions<RedisOptions> options) : IRuleStateStore
{
    private readonly string _prefix = options.Value.KeyPrefix;

    private RedisKey Key(Guid ruleId) => $"{_prefix}rule:{ruleId:D}";

    public async Task<IReadOnlyDictionary<Guid, RuleState>> GetManyAsync(IReadOnlyCollection<Guid> ruleIds, CancellationToken cancellationToken)
    {
        if (ruleIds.Count == 0) return new Dictionary<Guid, RuleState>();

        var ids = ruleIds.Distinct().ToArray();
        var values = await redis.GetDatabase().StringGetAsync(ids.Select(Key).ToArray()); // um único MGET

        var result = new Dictionary<Guid, RuleState>();
        for (var i = 0; i < ids.Length; i++)
        {
            if (values[i].IsNullOrEmpty) continue;
            var state = JsonSerializer.Deserialize<RuleState>((string)values[i]!);
            if (state is not null) result[ids[i]] = state;
        }

        return result;
    }

    public async Task SaveManyAsync(IReadOnlyDictionary<Guid, RuleState> states, CancellationToken cancellationToken)
    {
        if (states.Count == 0) return;

        var batch = redis.GetDatabase().CreateBatch(); // pipeline: um round trip para todas
        var writes = states.Select(kv => batch.StringSetAsync(Key(kv.Key), JsonSerializer.Serialize(kv.Value))).ToList();
        batch.Execute();
        await Task.WhenAll(writes);
    }

    public Task DeleteAsync(Guid ruleId, CancellationToken cancellationToken) => redis.GetDatabase().KeyDeleteAsync(Key(ruleId));
}

/// <summary>
/// Lease (lock com prazo) no Redis: <c>SET chave token NX PX</c>. Só uma instância por vez o detém. O prazo evita
/// que uma instância morta segure o lock para sempre; a liberação compara o token (Lua) para nunca soltar o lock
/// de outro dono depois de o seu ter expirado.
/// </summary>
public sealed class RedisDistributedLease(IConnectionMultiplexer redis, IOptions<RedisOptions> options) : IDistributedLease
{
    private const string ReleaseScript = """
        if redis.call('GET', KEYS[1]) == ARGV[1] then
            return redis.call('DEL', KEYS[1])
        end
        return 0
        """;

    public async Task<IAsyncDisposable?> TryAcquireAsync(string name, TimeSpan duration, CancellationToken cancellationToken)
    {
        var key = (RedisKey)$"{options.Value.KeyPrefix}lease:{name}";
        var token = Guid.NewGuid().ToString("N");
        var db = redis.GetDatabase();

        var acquired = await db.StringSetAsync(key, token, duration, When.NotExists);
        return acquired ? new Lease(db, key, token) : null;
    }

    private sealed class Lease(IDatabase db, RedisKey key, string token) : IAsyncDisposable
    {
        public async ValueTask DisposeAsync() =>
            await db.ScriptEvaluateAsync(ReleaseScript, [key], [token]);
    }
}
