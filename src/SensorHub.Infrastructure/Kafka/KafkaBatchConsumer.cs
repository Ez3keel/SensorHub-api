using System.Diagnostics;
using Confluent.Kafka;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using SensorHub.Application.Contracts;
using SensorHub.Application.Processing;
using SensorHub.Domain.Common;

namespace SensorHub.Infrastructure.Kafka;

/// <summary>
/// Consumer em lote com semântica <b>at-least-once</b>.
/// <list type="number">
/// <item>Acumula até <c>MaxBatchSize</c> mensagens ou <c>MaxWaitMs</c> (o que vier primeiro).</item>
/// <item>Mensagens ilegíveis (JSON inválido, invariante de domínio violada) vão para a DLQ; o resto segue.</item>
/// <item>O handler processa o lote. Só DEPOIS do sucesso o offset é commitado. Queda entre o processamento
/// e o commit = o lote é reentregue, e o handler idempotente absorve a repetição.</item>
/// </list>
/// <b>Backpressure:</b> o consumer é <i>pull</i>. Se o handler (banco) fica lento, o consumer simplesmente
/// demora a chamar <c>Consume</c> e o backlog cresce no <b>log durável do Kafka</b> (o "lag"), não na memória
/// do processo. Falha transitória (banco fora): pausa a assignment e continua chamando <c>Consume</c> só para
/// manter o consumer vivo no grupo (senão <c>max.poll.interval.ms</c> o expulsa e provoca rebalance),
/// retentando com backoff exponencial sem descartar nada.
/// </summary>
public sealed class KafkaBatchConsumer : BackgroundService
{
    private readonly string _name;
    private readonly KafkaOptions _kafka;
    private readonly BatchConsumerOptions _options;
    private readonly IReadingBatchHandler _handler;
    private readonly IDeadLetterSink _deadLetters;
    private readonly ILogger _logger;
    private readonly Func<Exception, bool> _isPoison;
    private readonly string _topic;

    // Partições perdidas num rebalance enquanto um lote era acumulado. As mensagens delas devem ser
    // descartadas do lote: o novo dono as relê a partir do offset commitado.
    private readonly HashSet<TopicPartition> _revoked = [];
    private readonly List<ConsumeResult<string, byte[]>> _carryOver = [];

    private long _consumed, _batches, _deadLettered, _transientRetries, _bisections;
    private int _assignedPartitions;

    public KafkaBatchConsumer(
        string name,
        KafkaOptions kafka,
        BatchConsumerOptions options,
        IReadingBatchHandler handler,
        IDeadLetterSink deadLetters,
        ILogger logger,
        Func<Exception, bool>? isPoison = null)
    {
        _name = name;
        _kafka = kafka;
        _options = options;
        _handler = handler;
        _deadLetters = deadLetters;
        _logger = logger;
        _isPoison = isPoison ?? (_ => false);
        _topic = options.Topic ?? kafka.ReadingsTopic;
    }

    public string Name => _name;
    public long Consumed => Interlocked.Read(ref _consumed);
    public long Batches => Interlocked.Read(ref _batches);
    public long DeadLettered => Interlocked.Read(ref _deadLettered);
    public long TransientRetries => Interlocked.Read(ref _transientRetries);
    public int AssignedPartitions => Volatile.Read(ref _assignedPartitions);

    /// <summary>O Consume() do Kafka é bloqueante: roda em thread dedicada para não ocupar o thread pool.</summary>
    protected override Task ExecuteAsync(CancellationToken stoppingToken) =>
        Task.Factory.StartNew(() => RunAsync(stoppingToken), stoppingToken,
            TaskCreationOptions.LongRunning, TaskScheduler.Default).Unwrap();

    private async Task RunAsync(CancellationToken ct)
    {
        using var consumer = BuildConsumer();
        consumer.Subscribe(_topic);
        _logger.LogInformation("[{Name}] consumindo {Topic} no grupo {Group}", _name, _topic, _options.GroupId);

        try
        {
            while (!ct.IsCancellationRequested)
            {
                var batch = Accumulate(consumer, ct);
                if (batch.Count == 0) continue;

                await ProcessAsync(consumer, batch, ct);
            }
        }
        catch (OperationCanceledException) when (ct.IsCancellationRequested)
        {
            // shutdown normal
        }
        catch (Exception ex)
        {
            _logger.LogCritical(ex, "[{Name}] consumer terminou com erro fatal.", _name);
            throw;
        }
        finally
        {
            try { consumer.Close(); } // sai do grupo de forma limpa: evita esperar session.timeout para rebalancear
            catch (Exception ex) { _logger.LogWarning(ex, "[{Name}] erro ao fechar consumer.", _name); }
        }
    }

    // ------------------------------------------------------------------ acumulação

    private List<ConsumeResult<string, byte[]>> Accumulate(IConsumer<string, byte[]> consumer, CancellationToken ct)
    {
        var batch = new List<ConsumeResult<string, byte[]>>(_options.MaxBatchSize);
        if (_carryOver.Count > 0)
        {
            batch.AddRange(_carryOver);
            _carryOver.Clear();
        }

        var maxWait = TimeSpan.FromMilliseconds(_options.MaxWaitMs);
        var lingering = batch.Count > 0 ? Stopwatch.StartNew() : null;

        while (batch.Count < _options.MaxBatchSize)
        {
            ct.ThrowIfCancellationRequested();

            var wait = lingering is null ? TimeSpan.FromMilliseconds(500) : maxWait - lingering.Elapsed;
            if (wait <= TimeSpan.Zero) break;

            ConsumeResult<string, byte[]>? result;
            try
            {
                result = consumer.Consume(wait);
            }
            catch (ConsumeException ex) when (!ex.Error.IsFatal)
            {
                _logger.LogWarning("[{Name}] erro ao consumir: {Reason}", _name, ex.Error.Reason);
                continue;
            }

            if (result is null)
            {
                if (batch.Count > 0) break; // fim da espera com lote parcial: processa o que há
                continue;
            }

            if (result.IsPartitionEOF) continue;

            batch.Add(result);
            lingering ??= Stopwatch.StartNew();
        }

        return DropRevoked(batch);
    }

    private List<ConsumeResult<string, byte[]>> DropRevoked(List<ConsumeResult<string, byte[]>> batch)
    {
        if (_revoked.Count == 0) return batch;

        var kept = batch.Where(m => !_revoked.Contains(m.TopicPartition)).ToList();
        if (kept.Count != batch.Count)
            _logger.LogInformation("[{Name}] rebalance: {Dropped} mensagens de partições revogadas descartadas do lote (o novo dono as relê).",
                _name, batch.Count - kept.Count);
        _revoked.Clear();
        return kept;
    }

    // ------------------------------------------------------------------ processamento

    private async Task ProcessAsync(IConsumer<string, byte[]> consumer, List<ConsumeResult<string, byte[]>> batch, CancellationToken ct)
    {
        var valid = new List<(ConsumeResult<string, byte[]> Source, ConsumedReading Reading)>(batch.Count);

        foreach (var message in batch)
        {
            if (TryParse(message, out var reading, out var error))
                valid.Add((message, reading));
            else
                await SendToDeadLetterAsync(message, error, ct);
        }

        if (valid.Count > 0)
            await HandleWithRetryAsync(consumer, valid, ct);

        Commit(consumer, batch);

        Interlocked.Add(ref _consumed, batch.Count);
        Interlocked.Increment(ref _batches);
    }

    private static bool TryParse(ConsumeResult<string, byte[]> message, out ConsumedReading reading, out string error)
    {
        reading = default;
        try
        {
            var dto = ReadingMessageSerializer.Deserialize(message.Message.Value);
            if (dto is null)
            {
                error = "Payload nulo.";
                return false;
            }

            reading = new ConsumedReading(dto.ToReading(), dto.IngestedAt);
            error = "";
            return true;
        }
        catch (System.Text.Json.JsonException ex)
        {
            error = $"JSON inválido: {ex.Message}";
            return false;
        }
        catch (DomainException ex)
        {
            error = $"Leitura inválida: {ex.Message}";
            return false;
        }
    }

    /// <summary>
    /// Executa o handler. Falha "de dado" (isPoison) isola a(s) mensagem(ns) culpada(s) por bissecção e as
    /// manda à DLQ. Qualquer outra falha é tratada como transitória: pausa, espera e tenta de novo, sem limite.
    /// </summary>
    private async Task HandleWithRetryAsync(
        IConsumer<string, byte[]> consumer,
        List<(ConsumeResult<string, byte[]> Source, ConsumedReading Reading)> items,
        CancellationToken ct)
    {
        var delay = _options.InitialRetryDelayMs;
        var paused = false;

        try
        {
            while (true)
            {
                try
                {
                    await _handler.HandleAsync(items.Select(i => i.Reading).ToList(), ct);
                    return;
                }
                catch (Exception ex) when (_isPoison(ex))
                {
                    await IsolatePoisonAsync(consumer, items, ex, ct);
                    return;
                }
                catch (Exception ex) when (!ct.IsCancellationRequested)
                {
                    Interlocked.Increment(ref _transientRetries);
                    _logger.LogWarning(ex, "[{Name}] falha transitória ao processar lote de {Count}; nova tentativa em {Delay}ms (offsets NÃO commitados).",
                        _name, items.Count, delay);

                    if (!paused)
                    {
                        consumer.Pause(consumer.Assignment);
                        paused = true;
                    }

                    KeepAliveFor(consumer, TimeSpan.FromMilliseconds(delay + Random.Shared.Next(0, delay / 4 + 1)), ct);
                    delay = Math.Min(delay * 2, _options.MaxRetryDelayMs);
                }
            }
        }
        finally
        {
            if (paused)
            {
                try { consumer.Resume(consumer.Assignment); }
                catch (Exception ex) { _logger.LogWarning(ex, "[{Name}] erro ao retomar partições.", _name); }
            }
        }
    }

    /// <summary>Espera chamando Consume() numa assignment pausada: age como sleep e mantém o consumer vivo no grupo.</summary>
    private void KeepAliveFor(IConsumer<string, byte[]> consumer, TimeSpan duration, CancellationToken ct)
    {
        var clock = Stopwatch.StartNew();
        while (clock.Elapsed < duration)
        {
            ct.ThrowIfCancellationRequested();
            var remaining = duration - clock.Elapsed;
            var stray = consumer.Consume(remaining < TimeSpan.FromSeconds(1) ? remaining : TimeSpan.FromSeconds(1));
            if (stray is { IsPartitionEOF: false })
                _carryOver.Add(stray); // não deveria acontecer com a assignment pausada, mas nunca se perde mensagem
        }
    }

    private async Task IsolatePoisonAsync(
        IConsumer<string, byte[]> consumer,
        List<(ConsumeResult<string, byte[]> Source, ConsumedReading Reading)> items,
        Exception cause,
        CancellationToken ct)
    {
        if (items.Count == 1)
        {
            await SendToDeadLetterAsync(items[0].Source, $"Falha permanente no handler: {cause.Message}", ct);
            return;
        }

        // Bissecção: metade que falha continua sendo dividida; a que passa é gravada. O(log n) rodadas.
        var mid = items.Count / 2;
        foreach (var half in new[] { items[..mid], items[mid..] })
        {
            Interlocked.Increment(ref _bisections);
            await HandleWithRetryAsync(consumer, half, ct);
        }
    }

    private async Task SendToDeadLetterAsync(ConsumeResult<string, byte[]> source, string reason, CancellationToken ct)
    {
        _logger.LogError("[{Name}] mensagem para DLQ ({Topic}[{Partition}]@{Offset}): {Reason}",
            _name, source.Topic, source.Partition.Value, source.Offset.Value, reason);

        await _deadLetters.SendAsync(new DeadLetter(
            source.Message.Key, source.Message.Value, reason,
            source.Topic, source.Partition.Value, source.Offset.Value, _options.GroupId), ct);

        Interlocked.Increment(ref _deadLettered);
    }

    // ------------------------------------------------------------------ commit

    /// <summary>Commita, por partição, o offset da última mensagem do lote + 1 (a próxima a ser lida).</summary>
    private void Commit(IConsumer<string, byte[]> consumer, List<ConsumeResult<string, byte[]>> batch)
    {
        var offsets = batch
            .GroupBy(m => m.TopicPartition)
            .Where(g => !_revoked.Contains(g.Key))
            .Select(g => new TopicPartitionOffset(g.Key, g.Max(m => m.Offset.Value) + 1))
            .ToList();

        if (offsets.Count == 0) return;

        try
        {
            consumer.Commit(offsets);
        }
        catch (KafkaException ex)
        {
            // Rebalance no meio do commit: o lote será reentregue e reprocessado. Idempotência cobre.
            _logger.LogWarning("[{Name}] falha ao commitar offsets ({Reason}); o lote poderá ser reprocessado.", _name, ex.Error.Reason);
        }
    }

    // ------------------------------------------------------------------ construção

    private IConsumer<string, byte[]> BuildConsumer()
    {
        var config = new ConsumerConfig
        {
            BootstrapServers = _kafka.BootstrapServers,
            GroupId = _options.GroupId,
            ClientId = $"{_name}-{Environment.MachineName}",
            AutoOffsetReset = _options.StartFromEarliest ? AutoOffsetReset.Earliest : AutoOffsetReset.Latest,
            EnableAutoCommit = false,        // commit manual, só depois de processar
            EnableAutoOffsetStore = false,
            // Rebalance cooperativo: ao escalar consumers, só as partições que mudam de dono param;
            // o modo "eager" (padrão) pararia o grupo inteiro a cada entrada/saída.
            PartitionAssignmentStrategy = PartitionAssignmentStrategy.CooperativeSticky,
            MaxPollIntervalMs = _options.MaxPollIntervalMs,
            SessionTimeoutMs = 30_000
        };

        return new ConsumerBuilder<string, byte[]>(config)
            .SetPartitionsAssignedHandler((_, partitions) =>
            {
                // Uma partição revogada que volta para NÓS não pode mais ser filtrada: as mensagens lidas
                // depois da reatribuição são legítimas, e descartá-las e commitar adiante perderia dados.
                foreach (var p in partitions) _revoked.Remove(p);
                Interlocked.Add(ref _assignedPartitions, partitions.Count);
                _logger.LogInformation("[{Name}] partições atribuídas: {Partitions}", _name, string.Join(",", partitions.Select(p => p.Partition.Value)));
            })
            .SetPartitionsRevokedHandler((_, partitions) =>
            {
                foreach (var p in partitions) _revoked.Add(p.TopicPartition);
                Interlocked.Add(ref _assignedPartitions, -partitions.Count);
                _logger.LogInformation("[{Name}] partições revogadas: {Partitions}", _name, string.Join(",", partitions.Select(p => p.Partition.Value)));
            })
            .SetPartitionsLostHandler((_, partitions) =>
            {
                foreach (var p in partitions) _revoked.Add(p.TopicPartition);
                Interlocked.Add(ref _assignedPartitions, -partitions.Count);
                _logger.LogWarning("[{Name}] partições PERDIDAS: {Partitions}", _name, string.Join(",", partitions.Select(p => p.Partition.Value)));
            })
            .SetErrorHandler((_, e) => _logger.LogWarning("[{Name}] kafka: {Reason}", _name, e.Reason))
            .Build();
    }
}
