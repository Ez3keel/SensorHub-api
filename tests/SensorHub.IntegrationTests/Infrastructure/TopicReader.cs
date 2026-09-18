using System.Diagnostics;
using Confluent.Kafka;
using SensorHub.Application.Contracts;

namespace SensorHub.IntegrationTests.Infrastructure;

public sealed record ConsumedReading(int Partition, long Offset, string Key, ReadingMessage Message);

/// <summary>Lê um tópico do início como um consumer de teste independente (grupo próprio).</summary>
public static class TopicReader
{
    /// <summary>Consome até <paramref name="expected"/> mensagens cujas chaves estão em <paramref name="sensorFilter"/>.</summary>
    public static List<ConsumedReading> Read(
        string bootstrapServers, string topic, int expected, ISet<Guid> sensorFilter, TimeSpan? timeout = null)
    {
        var config = new ConsumerConfig
        {
            BootstrapServers = bootstrapServers,
            GroupId = $"test-{Guid.NewGuid():N}",
            AutoOffsetReset = AutoOffsetReset.Earliest,
            EnableAutoCommit = false
        };

        using var consumer = new ConsumerBuilder<string, byte[]>(config).Build();
        consumer.Subscribe(topic);

        var found = new List<ConsumedReading>();
        var clock = Stopwatch.StartNew();
        var limit = timeout ?? TimeSpan.FromSeconds(60);

        while (found.Count < expected && clock.Elapsed < limit)
        {
            var result = consumer.Consume(TimeSpan.FromMilliseconds(500));
            if (result is null) continue;

            var message = ReadingMessageSerializer.Deserialize(result.Message.Value)!;
            if (sensorFilter.Contains(message.SensorId))
                found.Add(new ConsumedReading(result.Partition.Value, result.Offset.Value, result.Message.Key, message));
        }

        return found;
    }
}
