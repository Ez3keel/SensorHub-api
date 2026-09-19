using System.Text;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;
using SensorHub.Application.Processing;
using SensorHub.Infrastructure.Kafka;
using SensorHub.Infrastructure.Persistence;
using SensorHub.IntegrationTests.Infrastructure;
using static SensorHub.IntegrationTests.Infrastructure.TestHelpers;

namespace SensorHub.IntegrationTests.Consumers;

/// <summary>
/// Testes do consumer com Kafka e Postgres REAIS. Cada teste usa um tópico e um consumer group próprios.
/// O foco é a garantia central do pipeline: nenhuma leitura perdida, nenhuma gravada duas vezes,
/// mesmo com falhas, reentregas, lentidão e escala.
/// </summary>
[Collection(PlatformCollection.Name)]
public class BatchConsumerTests(PlatformFixture platform)
{
    private PersistReadingsHandler PersistHandler() => new(platform.CreateStore(), NullLogger<PersistReadingsHandler>.Instance);

    private static async Task<RunningConsumer> StartAsync(
        KafkaOptions kafka, IReadingBatchHandler handler, string? group = null,
        Func<Exception, bool>? isPoison = null, int maxBatch = 1000, string name = "test")
    {
        var dlq = new KafkaDeadLetterSink(Options.Create(kafka));
        var consumer = new KafkaBatchConsumer(name, kafka,
            new BatchConsumerOptions
            {
                GroupId = group ?? $"g-{Guid.NewGuid():N}",
                MaxBatchSize = maxBatch,
                MaxWaitMs = 100,
                InitialRetryDelayMs = 100,
                MaxRetryDelayMs = 400
            },
            handler, dlq, NullLogger.Instance, isPoison);
        await consumer.StartAsync(default);
        return new RunningConsumer(consumer, dlq);
    }

    // -------------------------------------------------------------- caminho feliz

    [Fact]
    public async Task Persists_every_reading_exactly_once_and_commits_all_offsets()
    {
        var kafka = await platform.CreateIsolatedTopicsAsync();
        var sensors = NewSensors(50);
        await PublishAsync(kafka, Readings(sensors, 5000));
        var group = $"g-{Guid.NewGuid():N}";

        await using var run = await StartAsync(kafka, PersistHandler(), group);

        await EventuallyAsync(async () => await platform.CountReadingsAsync(sensors) == 5000, "5000 leituras persistidas");
        await EventuallyAsync(() => Task.FromResult(GetLag(kafka.BootstrapServers, kafka.ReadingsTopic, group) == 0), "lag do grupo chegou a zero");
        Assert.True(run.Consumer.Consumed >= 5000); // at-least-once: um rebalance pode reler; o banco garante a unicidade
        Assert.Equal(0, run.Consumer.DeadLettered);
    }

    [Fact]
    public async Task Duplicated_messages_in_the_log_are_stored_once()
    {
        var kafka = await platform.CreateIsolatedTopicsAsync();
        var sensors = NewSensors(10);
        var readings = Readings(sensors, 1000);
        await PublishAsync(kafka, readings);
        await PublishAsync(kafka, readings); // o dispositivo reenviou tudo (retry após 503)

        await using var run = await StartAsync(kafka, PersistHandler());

        await EventuallyAsync(() => Task.FromResult(run.Consumer.Consumed >= 2000), "ao menos 2000 mensagens consumidas");
        Assert.Equal(1000, await platform.CountReadingsAsync(sensors));
    }

    // -------------------------------------------------------------- falhas e at-least-once

    [Fact]
    public async Task Crash_after_write_but_before_commit_redelivers_the_batch_without_duplicating_rows()
    {
        var kafka = await platform.CreateIsolatedTopicsAsync();
        var sensors = NewSensors(20);
        await PublishAsync(kafka, Readings(sensors, 2000));
        // Grava de verdade e ENTÃO falha, uma vez: é a janela clássica "gravou mas não commitou o offset".
        var handler = new FailAfterWriteOnceHandler(PersistHandler());

        await using var run = await StartAsync(kafka, handler);

        await EventuallyAsync(async () => await platform.CountReadingsAsync(sensors) == 2000, "2000 linhas, sem duplicar");
        await EventuallyAsync(() => Task.FromResult(run.Consumer.Consumed >= 2000), "consumo completo");
        Assert.True(run.Consumer.TransientRetries >= 1, "o lote deveria ter sido reprocessado");
        Assert.True(handler.Calls >= 2);
        Assert.Equal(2000, await platform.CountReadingsAsync(sensors)); // continua 2000
    }

    [Fact]
    public async Task Transient_failures_lose_nothing_and_keep_per_sensor_order()
    {
        var kafka = await platform.CreateIsolatedTopicsAsync();
        var sensors = NewSensors(12);
        var published = Readings(sensors, 3000);
        await PublishAsync(kafka, published);
        var handler = new RecordingHandler { FailFirstCalls = 3 }; // banco "fora do ar" nas 3 primeiras tentativas

        await using var run = await StartAsync(kafka, handler, maxBatch: 500);

        await EventuallyAsync(() => Task.FromResult(handler.Processed.Count == 3000), "todas as leituras processadas apesar das falhas");
        Assert.Equal(3, run.Consumer.TransientRetries);

        var seen = handler.Processed.ToList();
        Assert.Equal(3000, seen.Select(r => (r.Reading.SensorId, r.Reading.Timestamp)).Distinct().Count()); // nenhuma perdida ou repetida
        foreach (var bySensor in seen.GroupBy(r => r.Reading.SensorId))
        {
            var ts = bySensor.Select(r => r.Reading.Timestamp).ToList();
            Assert.Equal(ts.OrderBy(t => t), ts); // a ordem por sensor sobreviveu às falhas
        }
    }

    [Fact]
    public async Task Consumer_resumes_from_committed_offset_after_restart_without_reprocessing()
    {
        var kafka = await platform.CreateIsolatedTopicsAsync();
        var sensors = NewSensors(10);
        var group = $"g-{Guid.NewGuid():N}";
        await PublishAsync(kafka, Readings(sensors, 1000));

        var first = new RecordingHandler();
        await using (var run1 = await StartAsync(kafka, first, group))
        {
            // at-least-once: um rebalance na entrada do grupo pode reentregar parte do lote, então "== 1000" jamais voltaria a ser
            // verdadeiro depois de passar de 1000. Conta mensagens DISTINTAS (sensor + instante), que é o que importa aqui.
            await EventuallyAsync(() => Task.FromResult(first.Processed.Select(r => (r.Reading.SensorId, r.Reading.Timestamp)).Distinct().Count() >= 1000), "primeira leva");
            // O commit do offset acontece DEPOIS do handler. Parar antes dele é permitido (at-least-once: o lote seria
            // reentregue), mas então o "sem reprocessar" deste teste não se aplica. Espera o commit chegar ao broker.
            await EventuallyAsync(() => Task.FromResult(GetLag(kafka.BootstrapServers, kafka.ReadingsTopic, group) == 0), "offsets commitados");
        }

        // consumer parado; chegam mais 500 (timestamps novos) enquanto ele está fora
        await PublishAsync(kafka, Readings(sensors, 500).Select(m => m with { Timestamp = m.Timestamp.AddDays(1) }).ToList());
        var second = new RecordingHandler();
        await using var run2 = await StartAsync(kafka, second, group);

        // A PROPRIEDADE que importa: as 500 novas chegam e NADA da primeira leva é relido. Não se depende do tempo exato de
        // entrada no grupo (a suíte roda com muitos containers disputando a mesma máquina), então a espera é generosa.
        var firstWave = first.Processed.Select(r => r.Reading.Timestamp).ToHashSet();
        await EventuallyAsync(() => Task.FromResult(second.Processed.Count(r => !firstWave.Contains(r.Reading.Timestamp)) >= 500),
            "as 500 leituras novas chegam ao segundo consumer", TimeSpan.FromSeconds(120));
        await Task.Delay(500);

        Assert.DoesNotContain(second.Processed, r => firstWave.Contains(r.Reading.Timestamp)); // nada da primeira leva foi relido
        Assert.Equal(500, second.Processed.Count);
    }

    // -------------------------------------------------------------- mensagens venenosas

    [Fact]
    public async Task Unreadable_messages_go_to_the_dlq_with_context_and_neighbors_are_still_persisted()
    {
        var kafka = await platform.CreateIsolatedTopicsAsync();
        var sensors = NewSensors(5);
        var good = Readings(sensors, 100);
        await PublishAsync(kafka, good.Take(50).ToList());
        await PublishRawAsync(kafka, "k1", "isto não é json");
        await PublishRawAsync(kafka, "k2", """{"schemaVersion":1,"sensorId":"00000000-0000-0000-0000-000000000000","timestamp":"2026-01-01T00:00:00Z","value":1,"ingestedAt":"2026-01-01T00:00:00Z"}""");
        await PublishAsync(kafka, good.Skip(50).ToList());
        var group = $"g-{Guid.NewGuid():N}";

        await using var run = await StartAsync(kafka, PersistHandler(), group);

        await EventuallyAsync(async () => await platform.CountReadingsAsync(sensors) == 100, "as 100 válidas persistidas");
        await EventuallyAsync(() => Task.FromResult(GetLag(kafka.BootstrapServers, kafka.ReadingsTopic, group) == 0), "partição não ficou travada");
        Assert.Equal(2, run.Consumer.DeadLettered);

        var dlq = ReadAll(kafka.BootstrapServers, kafka.ReadingsDlqTopic, 2);
        Assert.Equal(2, dlq.Count);
        var invalidJson = dlq.Single(m => m.Message.Key == "k1");
        Assert.Equal("isto não é json", Encoding.UTF8.GetString(invalidJson.Message.Value)); // payload original preservado
        Assert.Contains("JSON", Header(invalidJson, "dlq-reason"));
        Assert.Equal(kafka.ReadingsTopic, Header(invalidJson, "dlq-source-topic"));
        Assert.Equal(group, Header(invalidJson, "dlq-consumer-group"));
        Assert.Contains("SensorId", Header(dlq.Single(m => m.Message.Key == "k2"), "dlq-reason"));
    }

    [Fact]
    public async Task A_permanently_failing_reading_is_isolated_by_bisection_and_the_rest_of_the_batch_is_kept()
    {
        var kafka = await platform.CreateIsolatedTopicsAsync(partitions: 1);
        var sensors = NewSensors(4);
        var readings = Readings(sensors, 200);
        readings[137] = readings[137] with { Value = 666 }; // "dado maldito"
        await PublishAsync(kafka, readings);
        var handler = new RecordingHandler { PoisonValue = 666 };

        await using var run = await StartAsync(kafka, handler, isPoison: ex => ex is PoisonDataException);

        await EventuallyAsync(() => Task.FromResult(run.Consumer.Consumed >= 200), "lote consumido");
        Assert.Equal(1, run.Consumer.DeadLettered);
        Assert.Equal(199, handler.Processed.Count); // as 199 boas foram tratadas
        Assert.DoesNotContain(handler.Processed, r => r.Reading.Value == 666);
        Assert.Single(ReadAll(kafka.BootstrapServers, kafka.ReadingsDlqTopic, 1));
    }

    // -------------------------------------------------------------- backpressure e lag

    [Fact]
    public async Task Backlog_built_while_no_consumer_runs_is_drained_when_it_starts()
    {
        var kafka = await platform.CreateIsolatedTopicsAsync();
        var sensors = NewSensors(30);
        var group = $"g-{Guid.NewGuid():N}";
        await PublishAsync(kafka, Readings(sensors, 10_000));

        // sem consumer: o backlog vive no log durável (lag), não na memória de ninguém
        await using var run = await StartAsync(kafka, PersistHandler(), group);

        await EventuallyAsync(async () => await platform.CountReadingsAsync(sensors) == 10_000, "backlog drenado");
        await EventuallyAsync(() => Task.FromResult(GetLag(kafka.BootstrapServers, kafka.ReadingsTopic, group) == 0), "lag zerado");
    }

    [Fact]
    public async Task Slow_handler_makes_lag_grow_in_kafka_instead_of_consumer_memory_and_still_finishes()
    {
        var kafka = await platform.CreateIsolatedTopicsAsync();
        var sensors = NewSensors(10);
        var group = $"g-{Guid.NewGuid():N}";
        await PublishAsync(kafka, Readings(sensors, 4000));
        var handler = new RecordingHandler { DelayPerCall = TimeSpan.FromMilliseconds(400) }; // "banco lento"

        await using var run = await StartAsync(kafka, handler, group, maxBatch: 500);

        await Task.Delay(1200);
        var lagWhileSlow = GetLag(kafka.BootstrapServers, kafka.ReadingsTopic, group);
        Assert.True(lagWhileSlow > 0, "com o handler lento, o backlog deveria estar no Kafka");
        Assert.True(handler.Processed.Count < 4000);

        await EventuallyAsync(() => Task.FromResult(handler.Processed.Count == 4000), "termina mesmo lento", TimeSpan.FromSeconds(90));
    }

    // -------------------------------------------------------------- consumer groups

    [Fact]
    public async Task Consumers_of_the_same_group_split_the_partitions_and_each_message_is_processed_once()
    {
        var kafka = await platform.CreateIsolatedTopicsAsync(partitions: 6);
        var sensors = NewSensors(60);
        var group = $"g-{Guid.NewGuid():N}";
        var h1 = new RecordingHandler();
        var h2 = new RecordingHandler();

        await using var c1 = await StartAsync(kafka, h1, group, name: "c1");
        await EventuallyAsync(() => Task.FromResult(c1.Consumer.AssignedPartitions == 6), "c1 assumiu as 6 partições");
        await using var c2 = await StartAsync(kafka, h2, group, name: "c2");
        await EventuallyAsync(() => Task.FromResult(
            c1.Consumer.AssignedPartitions > 0 && c2.Consumer.AssignedPartitions > 0
            && c1.Consumer.AssignedPartitions + c2.Consumer.AssignedPartitions == 6), "rebalance dividiu as partições entre os dois");

        await PublishAsync(kafka, Readings(sensors, 6000));

        await EventuallyAsync(() => Task.FromResult(h1.Processed.Count + h2.Processed.Count == 6000), "6000 processadas no total");
        Assert.NotEmpty(h1.Processed);
        Assert.NotEmpty(h2.Processed);
        // um sensor nunca é lido por dois consumers do grupo (ordem por chave)
        var sensorsOfC1 = h1.Processed.Select(r => r.Reading.SensorId).ToHashSet();
        Assert.DoesNotContain(h2.Processed, r => sensorsOfC1.Contains(r.Reading.SensorId));
    }

    [Fact]
    public async Task Different_groups_each_receive_the_full_stream_independently()
    {
        var kafka = await platform.CreateIsolatedTopicsAsync();
        var sensors = NewSensors(20);
        await PublishAsync(kafka, Readings(sensors, 2000));
        var persistence = new RecordingHandler();
        var alerts = new RecordingHandler();

        await using var a = await StartAsync(kafka, persistence, name: "persistence");
        await using var b = await StartAsync(kafka, alerts, name: "alerts");

        // é o que uma fila tradicional não faz sem duplicar mensagem: 2 consumidores, cada um com TODAS
        await EventuallyAsync(() => Task.FromResult(persistence.Processed.Count == 2000 && alerts.Processed.Count == 2000), "ambos os grupos leram tudo");
    }

    private static string Header(Confluent.Kafka.ConsumeResult<string, byte[]> m, string key) =>
        Encoding.UTF8.GetString(m.Message.Headers.GetLastBytes(key));

    // ============================================================ test doubles

    private sealed class PoisonDataException() : Exception("dado inválido");

    private sealed class RecordingHandler : IReadingBatchHandler
    {
        private readonly List<ConsumedReading> _processed = [];
        private int _calls;

        public int FailFirstCalls { get; init; }
        public double? PoisonValue { get; init; }
        public TimeSpan DelayPerCall { get; init; }

        public IReadOnlyList<ConsumedReading> Processed { get { lock (_processed) return _processed.ToList(); } }

        public async Task HandleAsync(IReadOnlyList<ConsumedReading> batch, CancellationToken cancellationToken)
        {
            if (DelayPerCall > TimeSpan.Zero) await Task.Delay(DelayPerCall, cancellationToken);
            if (Interlocked.Increment(ref _calls) <= FailFirstCalls) throw new InvalidOperationException("banco fora do ar");
            if (PoisonValue is { } poison && batch.Any(b => b.Reading.Value == poison)) throw new PoisonDataException();
            lock (_processed) _processed.AddRange(batch);
        }
    }

    private sealed class FailAfterWriteOnceHandler(IReadingBatchHandler inner) : IReadingBatchHandler
    {
        private int _calls;
        public int Calls => _calls;

        public async Task HandleAsync(IReadOnlyList<ConsumedReading> batch, CancellationToken cancellationToken)
        {
            await inner.HandleAsync(batch, cancellationToken);
            if (Interlocked.Increment(ref _calls) == 1)
                throw new InvalidOperationException("caiu depois de gravar, antes de commitar o offset");
        }
    }

    private sealed class RunningConsumer(KafkaBatchConsumer consumer, KafkaDeadLetterSink dlq) : IAsyncDisposable
    {
        public KafkaBatchConsumer Consumer => consumer;

        public async ValueTask DisposeAsync()
        {
            await consumer.StopAsync(default);
            consumer.Dispose();
            dlq.Dispose();
        }
    }
}
