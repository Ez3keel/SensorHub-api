using System.Diagnostics;
using System.Diagnostics.Metrics;
using System.Net.Http.Json;
using System.Text;
using Confluent.Kafka;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;
using SensorHub.Application.Observability;
using SensorHub.Application.Processing;
using SensorHub.Infrastructure.Kafka;
using SensorHub.IntegrationTests.Infrastructure;
using static SensorHub.IntegrationTests.Infrastructure.TestHelpers;

namespace SensorHub.IntegrationTests.Observability;

public class KafkaTraceContextTests
{
    private static readonly ActivitySource Source = new("test.trace");

    private static ActivityListener Listen() => new()
    {
        ShouldListenTo = _ => true,
        Sample = (ref ActivityCreationOptions<ActivityContext> _) => ActivitySamplingResult.AllDataAndRecorded
    };

    [Fact]
    public void Context_survives_a_roundtrip_through_kafka_headers()
    {
        using var listener = Listen();
        ActivitySource.AddActivityListener(listener);
        using var activity = Source.StartActivity("producer", ActivityKind.Producer)!;
        activity.TraceStateString = "vendor=abc";
        var headers = new Headers();

        KafkaTraceContext.Inject(headers, activity);
        var extracted = KafkaTraceContext.Extract(headers);

        Assert.NotNull(extracted);
        Assert.Equal(activity.TraceId, extracted.Value.TraceId);
        Assert.Equal(activity.SpanId, extracted.Value.SpanId);
        Assert.Equal("vendor=abc", extracted.Value.TraceState);
        Assert.True(extracted.Value.IsRemote);
    }

    [Fact]
    public void Nothing_is_injected_without_an_active_trace_and_extract_tolerates_garbage()
    {
        var headers = new Headers();
        KafkaTraceContext.Inject(headers, null);
        Assert.Empty(headers);
        Assert.Null(KafkaTraceContext.Extract(headers));
        Assert.Null(KafkaTraceContext.Extract(null));

        headers.Add("traceparent", Encoding.UTF8.GetBytes("isto-nao-e-traceparent"));
        Assert.Null(KafkaTraceContext.Extract(headers));
    }
}

public class KafkaStatisticsTests
{
    private const string Sample = """
        {
          "name": "rdkafka#consumer-1",
          "topics": {
            "sensorhub.readings": {
              "topic": "sensorhub.readings",
              "partitions": {
                "0": { "partition": 0, "consumer_lag": 1200, "hi_offset": 5000 },
                "1": { "partition": 1, "consumer_lag": 0 },
                "2": { "partition": 2, "consumer_lag": -1 },
                "-1": { "partition": -1, "consumer_lag": 77 }
              }
            },
            "outro": { "partitions": { "3": { "partition": 3 } } }
          }
        }
        """;

    [Fact]
    public void Parses_lag_per_partition_ignoring_unknown_and_internal_partitions()
    {
        var lag = KafkaStatistics.ParseLag(Sample);

        Assert.Equal([("sensorhub.readings", 0, 1200L), ("sensorhub.readings", 1, 0L)], lag);
    }

    [Fact]
    public void Statistics_without_topics_yield_no_lag()
    {
        Assert.Empty(KafkaStatistics.ParseLag("""{ "name": "x" }"""));
    }
}

[Collection(PlatformCollection.Name)]
public class ObservabilityIntegrationTests(PlatformFixture platform)
{
    private static ActivityListener Capture(List<Activity> stopped) => new()
    {
        ShouldListenTo = source => source.Name is "SensorHub" or "Microsoft.AspNetCore" or "test.root",
        Sample = (ref ActivityCreationOptions<ActivityContext> _) => ActivitySamplingResult.AllDataAndRecorded,
        ActivityStopped = a => { lock (stopped) stopped.Add(a); }
    };

    private static async Task<(KafkaBatchConsumer Consumer, KafkaDeadLetterSink Dlq)> StartAsync(
        KafkaOptions kafka, IReadingBatchHandler handler, string group, int statisticsMs = 500)
    {
        var dlq = new KafkaDeadLetterSink(Options.Create(kafka));
        var consumer = new KafkaBatchConsumer("obs", kafka,
            new BatchConsumerOptions { GroupId = group, MaxBatchSize = 500, MaxWaitMs = 100, InitialRetryDelayMs = 100, MaxRetryDelayMs = 300, StatisticsIntervalMs = statisticsMs },
            handler, dlq, NullLogger.Instance);
        await consumer.StartAsync(default);
        return (consumer, dlq);
    }

    private static async Task StopAsync(KafkaBatchConsumer consumer, KafkaDeadLetterSink dlq)
    {
        await consumer.StopAsync(default);
        consumer.Dispose();
        dlq.Dispose();
    }

    private sealed class SpyHandler : IReadingBatchHandler
    {
        public List<string?> ActivityNamesSeen { get; } = [];
        public int Processed { get; private set; }
        public int FailFirst { get; init; }
        public TimeSpan Delay { get; init; }
        private int _calls;

        public async Task HandleAsync(IReadOnlyList<ConsumedReading> batch, CancellationToken cancellationToken)
        {
            if (Delay > TimeSpan.Zero) await Task.Delay(Delay, cancellationToken);
            if (Interlocked.Increment(ref _calls) <= FailFirst) throw new InvalidOperationException("banco fora");
            lock (ActivityNamesSeen) ActivityNamesSeen.Add(Activity.Current?.OperationName);
            Processed += batch.Count;
        }
    }

    // ------------------------------------------------------------------ tracing

    [Fact]
    public async Task One_trace_crosses_http_the_producer_kafka_and_the_consumer()
    {
        var stopped = new List<Activity>();
        using var listener = Capture(stopped);
        ActivitySource.AddActivityListener(listener);
        // Tópico ISOLADO: no tópico de produção a minha mensagem cairia no meio de um lote com milhares de outras (de outros
        // testes) e viraria LINK em vez de PAI do span do consumer, o que é correto mas torna o teste dependente de ordem.
        var kafka = await platform.CreateIsolatedTopicsAsync();
        await using var api = new ApiFactory(platform.BootstrapServers,
            new Dictionary<string, string?> { ["Kafka:ReadingsTopic"] = kafka.ReadingsTopic, ["Kafka:ProvisionTopics"] = "false" },
            platform.ConnectionString, platform.RedisConnectionString);
        var http = api.CreateClient();
        var sensor = Guid.NewGuid();
        var spy = new SpyHandler();
        var (consumer, dlq) = await StartAsync(kafka, spy, $"trace-{Guid.NewGuid():N}");
        try
        {
            var response = await http.PostAsJsonAsync("/api/readings/batch",
                Enumerable.Range(0, 20).Select(i => new { sensorId = sensor, timestamp = DateTimeOffset.UtcNow.AddSeconds(-20 + i), value = (double)i }));
            response.EnsureSuccessStatusCode();
            await EventuallyAsync(() => Task.FromResult(HasConsumerSpanFor(stopped, kafka.ReadingsTopic)), "span do consumer recebido");

            Activity server, publish, process;
            lock (stopped)
            {
                publish = stopped.Last(a => a.OperationName == "sensorhub.readings publish");
                server = stopped.Last(a => a.Kind == ActivityKind.Server && a.TraceId == publish.TraceId);
                process = stopped.Last(a => a.OperationName == $"{kafka.ReadingsTopic} process" && a.TraceId == publish.TraceId);
            }

            Assert.Equal(server.TraceId, process.TraceId);                  // UM trace atravessando a fila
            Assert.Equal(server.SpanId, publish.ParentSpanId);              // publish é filho da requisição HTTP
            Assert.Equal(publish.SpanId, process.ParentSpanId);             // o consumer continua o span do producer
            Assert.Equal(ActivityKind.Producer, publish.Kind);
            Assert.Equal(ActivityKind.Consumer, process.Kind);
            Assert.Equal(20, (int)publish.GetTagItem("messaging.batch.message_count")!);
            Assert.Contains($"{kafka.ReadingsTopic} process", spy.ActivityNamesSeen); // o handler roda DENTRO do span do lote
        }
        finally
        {
            await StopAsync(consumer, dlq);
        }
    }

    private static bool HasConsumerSpanFor(List<Activity> stopped, string topic)
    {
        lock (stopped)
        {
            var publish = stopped.LastOrDefault(a => a.OperationName == "sensorhub.readings publish");
            return publish is not null && stopped.Any(a => a.OperationName == $"{topic} process" && a.TraceId == publish.TraceId);
        }
    }

    [Fact]
    public async Task A_batch_mixing_several_requests_links_to_the_other_traces()
    {
        var stopped = new List<Activity>();
        using var listener = Capture(stopped);
        ActivitySource.AddActivityListener(listener);
        var kafka = await platform.CreateIsolatedTopicsAsync(partitions: 1);
        using var publisher = new KafkaReadingPublisher(Options.Create(kafka), NullLogger<KafkaReadingPublisher>.Instance);
        var sensor = Guid.NewGuid();

        // 3 "requisições" independentes (3 traces) publicando na mesma partição
        var traceIds = new List<ActivityTraceId>();
        for (var i = 0; i < 3; i++)
        {
            using var root = new ActivitySource("test.root").StartActivity($"request-{i}", ActivityKind.Server);
            traceIds.Add(root?.TraceId ?? default);
            await publisher.PublishAsync([Message(sensor, DateTimeOffset.UtcNow.AddMilliseconds(i), i)], default);
        }

        var spy = new SpyHandler { Delay = TimeSpan.Zero };
        var (consumer, dlq) = await StartAsync(kafka, spy, $"links-{Guid.NewGuid():N}");
        try
        {
            // O span do lote só FECHA depois do commit do offset (que vem depois do handler): espera pela condição real.
            List<ActivityTraceId> Covered()
            {
                lock (stopped)
                    return stopped.Where(a => a.OperationName == $"{kafka.ReadingsTopic} process")
                        .SelectMany(a => a.Links.Select(l => l.Context.TraceId).Append(a.TraceId)).Distinct().ToList();
            }

            await EventuallyAsync(() => Task.FromResult(Covered().Count >= 3), "3 traces cobertos pelos spans de lote");

            // Pode ter vindo em 1 lote (o trace da 1ª mensagem como pai + 2 links) ou em vários. Em qualquer arranjo, os 3 traces
            // de origem precisam estar cobertos, como pai ou como link: nenhuma requisição some do trace por causa do batching.
            Assert.Equal(3, Covered().Count);
            Assert.All(traceIds, id => Assert.Contains(id, Covered()));
        }
        finally
        {
            await StopAsync(consumer, dlq);
        }
    }

    // ------------------------------------------------------------------ métricas do consumer

    private sealed class Recorder : IDisposable
    {
        private readonly MeterListener _listener = new();
        private readonly List<(string Name, double Value, Dictionary<string, object?> Tags)> _all = [];

        public Recorder(string group)
        {
            _listener.InstrumentPublished = (i, l) => { if (i.Meter.Name == SensorHubTelemetry.Name) l.EnableMeasurementEvents(i); };
            void On<T>(Instrument i, T m, ReadOnlySpan<KeyValuePair<string, object?>> t, object? s) where T : struct
            {
                var tags = t.ToArray().ToDictionary(x => x.Key, x => x.Value);
                if (tags.TryGetValue("group", out var g) && (string?)g != group) return; // só o grupo deste teste
                lock (_all) _all.Add((i.Name, Convert.ToDouble(m), tags));
            }
            _listener.SetMeasurementEventCallback<long>(On);
            _listener.SetMeasurementEventCallback<int>((i, m, t, s) => On(i, m, t, s));
            _listener.SetMeasurementEventCallback<double>(On);
            _listener.Start();
        }

        public double Sum(string name, string? tag = null, string? value = null)
        {
            lock (_all) return _all.Where(x => x.Name == name && (tag is null || (x.Tags.TryGetValue(tag, out var v) && (string?)v == value))).Sum(x => x.Value);
        }

        public int Count(string name) { lock (_all) return _all.Count(x => x.Name == name); }
        public void Dispose() => _listener.Dispose();
    }

    [Fact]
    public async Task The_consumer_reports_throughput_batches_handler_time_and_end_to_end_delay()
    {
        var kafka = await platform.CreateIsolatedTopicsAsync();
        var group = $"m-{Guid.NewGuid():N}";
        using var metrics = new Recorder(group);
        await PublishAsync(kafka, Readings(NewSensors(10), 1000));
        var spy = new SpyHandler();
        var (consumer, dlq) = await StartAsync(kafka, spy, group);
        try
        {
            await EventuallyAsync(() => Task.FromResult(spy.Processed >= 1000), "consumido");
            await Task.Delay(300);

            Assert.True(metrics.Sum("sensorhub.consumer.messages") >= 1000);
            Assert.True(metrics.Sum("sensorhub.consumer.batches") >= 1);
            Assert.True(metrics.Count("sensorhub.consumer.handler.duration") >= 1);
            Assert.True(metrics.Count("sensorhub.consumer.processing.delay") >= 1);
            Assert.True(metrics.Sum("sensorhub.consumer.batch.size") >= 1000);
            Assert.True(metrics.Sum("sensorhub.consumer.rebalances", "kind", "assigned") >= 1);
        }
        finally
        {
            await StopAsync(consumer, dlq);
        }
    }

    [Fact]
    public async Task Transient_failures_and_poison_messages_are_counted()
    {
        var kafka = await platform.CreateIsolatedTopicsAsync();
        var group = $"m-{Guid.NewGuid():N}";
        using var metrics = new Recorder(group);
        await PublishAsync(kafka, Readings(NewSensors(3), 30));
        await PublishRawAsync(kafka, "k", "não é json");
        var spy = new SpyHandler { FailFirst = 2 };
        var (consumer, dlq) = await StartAsync(kafka, spy, group);
        try
        {
            await EventuallyAsync(() => Task.FromResult(spy.Processed >= 30), "processado apesar das falhas");

            Assert.Equal(2, metrics.Sum("sensorhub.consumer.retries"));
            Assert.Equal(1, metrics.Sum("sensorhub.consumer.dead_letters", "reason", "parse"));
        }
        finally
        {
            await StopAsync(consumer, dlq);
        }
    }

    [Fact]
    public async Task Lag_grows_while_the_consumer_is_slow_and_returns_to_zero_when_it_catches_up()
    {
        var kafka = await platform.CreateIsolatedTopicsAsync();
        var group = $"lag-{Guid.NewGuid():N}";
        await PublishAsync(kafka, Readings(NewSensors(20), 5000));
        var spy = new SpyHandler { Delay = TimeSpan.FromMilliseconds(400) }; // "banco lento"
        var (consumer, dlq) = await StartAsync(kafka, spy, group);
        try
        {
            await EventuallyAsync(() => Task.FromResult(ConsumerLagRegistry.TotalFor(group) > 0), "lag observado > 0 enquanto o consumer está lento");
            var peak = ConsumerLagRegistry.TotalFor(group);
            Assert.InRange(peak, 1, 5000);

            await EventuallyAsync(() => Task.FromResult(spy.Processed >= 5000), "consumer alcançou o log", TimeSpan.FromSeconds(90));
            await EventuallyAsync(() => Task.FromResult(ConsumerLagRegistry.TotalFor(group) == 0), "lag voltou a zero");
        }
        finally
        {
            await StopAsync(consumer, dlq);
            Assert.Equal(0, ConsumerLagRegistry.TotalFor(group)); // ao sair, o consumer para de ser reportado (não congela)
        }
    }

    // ------------------------------------------------------------------ endpoint de métricas

    [Fact]
    public async Task The_api_exposes_prometheus_metrics_including_business_and_http_metrics()
    {
        var http = platform.Api.CreateClient();
        await http.PostAsJsonAsync("/api/readings/batch",
            new[] { new { sensorId = Guid.NewGuid(), timestamp = DateTimeOffset.UtcNow, value = 1.0 } });

        // O exportador Prometheus guarda a resposta em cache por 300 ms: se outro teste acabou de raspar /metrics, esta leitura
        // viria sem as medições novas. Em produção o Prometheus raspa a cada 5 s, então o cache é irrelevante lá.
        await Task.Delay(400);
        var body = await http.GetStringAsync("/metrics");

        Assert.Contains("sensorhub_ingest_readings_total", body);
        // (as métricas HTTP do ASP.NET Core vêm de um Meter criado por host via IMeterFactory; com vários hosts no MESMO processo de
        // teste o OpenTelemetry pode não enxergá-las. No processo real da API elas aparecem: verificado no scrape do Prometheus.)
        Assert.Contains("sensorhub_ingest_publish_duration_milliseconds", body);
        // (o gauge de lag só aparece quando algum consumer o reporta: sem consumer no processo não há série para exportar)
    }

    [Fact]
    public async Task The_end_to_end_delay_histogram_uses_explicit_buckets_so_percentiles_are_meaningful()
    {
        var http = platform.Api.CreateClient();
        var kafka = await platform.CreateIsolatedTopicsAsync();
        // dispara uma medição de atraso no processo (o histograma é global)
        var (consumer, dlq) = await StartAsync(kafka, new SpyHandler(), $"buckets-{Guid.NewGuid():N}");
        try
        {
            await PublishAsync(kafka, Readings(NewSensors(2), 10));
            await EventuallyAsync(() => Task.FromResult(consumer.Consumed >= 10), "consumido");
            await Task.Delay(1500); // dá tempo ao exportador de coletar

            var body = await http.GetStringAsync("/metrics");

            // buckets padrão do OTel (0, 5, 10, 25...) fariam todo quantil em segundos sair como 2,5 s / 4,75 s (artefato)
            Assert.Contains("sensorhub_consumer_processing_delay_seconds_bucket", body);
            Assert.Contains("le=\"0.05\"", body);
            Assert.Contains("le=\"0.25\"", body);
        }
        finally
        {
            await StopAsync(consumer, dlq);
        }
    }
}
