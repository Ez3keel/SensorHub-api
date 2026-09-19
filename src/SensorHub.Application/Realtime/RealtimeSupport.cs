using System.Collections.Concurrent;
using SensorHub.Application.Alerting;
using SensorHub.Domain.Readings;

namespace SensorHub.Application.Realtime;

/// <summary>Uma leitura como vai para o dashboard.</summary>
public sealed record ReadingPush(Guid SensorId, DateTimeOffset Timestamp, double Value);

/// <summary>Um alerta como vai para o dashboard.</summary>
public sealed record AlertPush(Guid AlertId, Guid RuleId, Guid SensorId, string Kind, string Severity, DateTimeOffset At, double? Value, string Message);

/// <summary>
/// Buffer "só o valor mais novo por sensor". O dashboard não precisa de cada leitura: precisa do estado atual, e enviar
/// 100 leituras por segundo de um sensor para um navegador que redesenha 4 vezes por segundo é desperdício de rede, de
/// CPU do servidor e da aba. Entre dois flushes, leituras do mesmo sensor colapsam na mais nova (coalescing), o que
/// também limita a taxa de mensagens por sensor a <c>1 / intervalo</c>, independente da taxa de ingestão.
/// Thread-safe: o consumer escreve enquanto o timer de flush drena.
/// </summary>
public sealed class ReadingCoalescer
{
    private ConcurrentDictionary<Guid, Reading> _latest = new();

    public int Pending => _latest.Count;

    public void Add(Reading reading)
    {
        // Só substitui por um valor MAIS NOVO (dado atrasado ou reentregue não regride o que o dashboard mostra).
        _latest.AddOrUpdate(reading.SensorId, reading, (_, current) => reading.Timestamp > current.Timestamp ? reading : current);
    }

    public void AddRange(IEnumerable<Reading> readings)
    {
        foreach (var reading in readings) Add(reading);
    }

    /// <summary>Entrega o que acumulou e recomeça vazio. Escritas concorrentes caem no próximo ciclo, nunca se perdem.</summary>
    public IReadOnlyList<Reading> Drain()
    {
        var swapped = Interlocked.Exchange(ref _latest, new ConcurrentDictionary<Guid, Reading>());
        return swapped.Values.ToList();
    }
}

/// <summary>
/// Descarta eventos já vistos. Os eventos de alerta são at-least-once (o motor pode republicar após uma queda), e o
/// dashboard não deve piscar/tocar duas vezes. A identidade do evento é (AlertId, Kind). Memória limitada (LRU simples).
/// </summary>
public sealed class RecentEventFilter(int capacity = 10_000)
{
    private readonly object _lock = new();
    private readonly HashSet<(Guid, AlertEventKind)> _seen = [];
    private readonly Queue<(Guid, AlertEventKind)> _order = new();

    /// <summary>Retorna true se o evento é novo (e o registra); false se já foi visto.</summary>
    public bool TryAdd(AlertEvent alertEvent)
    {
        var key = (alertEvent.AlertId, alertEvent.Kind);
        lock (_lock)
        {
            if (!_seen.Add(key)) return false;

            _order.Enqueue(key);
            while (_order.Count > capacity) _seen.Remove(_order.Dequeue());
            return true;
        }
    }
}

/// <summary>Diretório sensor → grupo (o "agrupamento" do dashboard). Cacheado: o fanout consulta a cada flush.</summary>
public interface ISensorDirectory
{
    /// <summary>Grupo do sensor, ou null se o sensor não está cadastrado.</summary>
    Task<string?> GetGroupAsync(Guid sensorId, CancellationToken cancellationToken);
}

/// <summary>
/// Quais sensores têm ao menos um dashboard olhando em detalhe, em TODAS as instâncias da API. O fanout só empurra
/// atualizações por sensor para quem alguém está de fato assistindo, em vez de publicar milhares de mensagens no
/// backplane para grupos vazios.
/// </summary>
public interface ISubscriptionRegistry
{
    Task AddAsync(Guid sensorId, CancellationToken cancellationToken);
    Task RemoveAsync(Guid sensorId, CancellationToken cancellationToken);
    Task<IReadOnlySet<Guid>> GetSubscribedSensorsAsync(CancellationToken cancellationToken);
}
