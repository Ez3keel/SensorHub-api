using System.Diagnostics;
using Confluent.Kafka;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;
using SensorHub.Application.Contracts;
using SensorHub.Infrastructure.Kafka;

namespace SensorHub.IntegrationTests.Infrastructure;

public static class TestHelpers
{
    public static async Task EventuallyAsync(Func<Task<bool>> condition, string because, TimeSpan? timeout = null)
    {
        var limit = timeout ?? TimeSpan.FromSeconds(60);
        var clock = Stopwatch.StartNew();
        while (clock.Elapsed < limit)
        {
            if (await condition()) return;
            await Task.Delay(100);
        }

        Assert.Fail($"Condição não satisfeita em {limit.TotalSeconds:F0}s: {because}");
    }

    public static ReadingMessage Message(Guid sensor, DateTimeOffset ts, double value) =>
        new(ReadingMessage.CurrentSchemaVersion, sensor, ts, value, null, DateTimeOffset.UtcNow);

    /// <summary>Gera <paramref name="count"/> leituras com chaves (sensor, ts) únicas, espalhadas por <paramref name="sensors"/>.</summary>
    public static List<ReadingMessage> Readings(IReadOnlyList<Guid> sensors, int count, double? value = null)
    {
        var start = new DateTimeOffset(2026, 1, 1, 0, 0, 0, TimeSpan.Zero);
        return Enumerable.Range(0, count)
            .Select(i => Message(sensors[i % sensors.Count], start.AddMilliseconds(i), value ?? i))
            .ToList();
    }

    public static List<Guid> NewSensors(int count) => Enumerable.Range(0, count).Select(_ => Guid.NewGuid()).ToList();

    /// <summary>Publica no tópico de leituras descrito por <paramref name="kafka"/> usando o producer de produção.</summary>
    public static async Task PublishAsync(KafkaOptions kafka, IReadOnlyList<ReadingMessage> messages)
    {
        using var publisher = new KafkaReadingPublisher(Options.Create(kafka), NullLogger<KafkaReadingPublisher>.Instance);
        foreach (var chunk in messages.Chunk(1000))
            await publisher.PublishAsync(chunk, default);
    }

    /// <summary>Publica bytes arbitrários (para simular mensagens venenosas que não passariam pela API).</summary>
    public static async Task PublishRawAsync(KafkaOptions kafka, string key, string rawValue)
    {
        using var producer = new ProducerBuilder<string, byte[]>(new ProducerConfig { BootstrapServers = kafka.BootstrapServers }).Build();
        await producer.ProduceAsync(kafka.ReadingsTopic, new Message<string, byte[]> { Key = key, Value = System.Text.Encoding.UTF8.GetBytes(rawValue) });
    }

    /// <summary>Lag do grupo = mensagens no log que o grupo ainda não commitou (a métrica mais importante de um sistema Kafka).</summary>
    public static long GetLag(string bootstrapServers, string topic, string groupId)
    {
        using var consumer = new ConsumerBuilder<Ignore, Ignore>(new ConsumerConfig
        {
            BootstrapServers = bootstrapServers,
            GroupId = groupId,
            EnableAutoCommit = false
        }).Build();

        using var admin = new AdminClientBuilder(new AdminClientConfig { BootstrapServers = bootstrapServers }).Build();
        var partitions = admin.GetMetadata(topic, TimeSpan.FromSeconds(10)).Topics.Single().Partitions
            .Select(p => new TopicPartition(topic, p.PartitionId)).ToList();

        var committed = consumer.Committed(partitions, TimeSpan.FromSeconds(10));
        long lag = 0;
        foreach (var c in committed)
        {
            var watermarks = consumer.QueryWatermarkOffsets(c.TopicPartition, TimeSpan.FromSeconds(10));
            var position = c.Offset.IsSpecial ? watermarks.Low.Value : c.Offset.Value;
            lag += watermarks.High.Value - position;
        }

        return lag;
    }

    public static List<ConsumeResult<string, byte[]>> ReadAll(string bootstrapServers, string topic, int expected, TimeSpan? timeout = null)
    {
        using var consumer = new ConsumerBuilder<string, byte[]>(new ConsumerConfig
        {
            BootstrapServers = bootstrapServers,
            GroupId = $"reader-{Guid.NewGuid():N}",
            AutoOffsetReset = AutoOffsetReset.Earliest,
            EnableAutoCommit = false
        }).Build();
        consumer.Subscribe(topic);

        var results = new List<ConsumeResult<string, byte[]>>();
        var clock = Stopwatch.StartNew();
        var limit = timeout ?? TimeSpan.FromSeconds(30);
        while (results.Count < expected && clock.Elapsed < limit)
        {
            var r = consumer.Consume(TimeSpan.FromMilliseconds(500));
            if (r is not null) results.Add(r);
        }

        return results;
    }
}
