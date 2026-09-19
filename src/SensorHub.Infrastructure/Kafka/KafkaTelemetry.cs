using System.Diagnostics;
using System.Text;
using System.Text.Json;
using Confluent.Kafka;

namespace SensorHub.Infrastructure.Kafka;

/// <summary>
/// Propagação de contexto de trace pelo Kafka (W3C Trace Context nos headers da mensagem). Sem ela, o trace termina no
/// producer: a fila é uma parede entre a requisição HTTP e o consumer. Com ela, "POST /api/readings → publicar → consumir →
/// gravar no banco" é UM trace.
/// </summary>
public static class KafkaTraceContext
{
    public const string TraceParent = "traceparent";
    public const string TraceState = "tracestate";

    /// <summary>Grava o contexto do activity atual nos headers (não faz nada se não há trace ativo).</summary>
    public static void Inject(Headers headers, Activity? activity)
    {
        if (activity?.Id is not { } id) return;

        headers.Remove(TraceParent);
        headers.Add(TraceParent, Encoding.UTF8.GetBytes(id));
        if (!string.IsNullOrEmpty(activity.TraceStateString))
        {
            headers.Remove(TraceState);
            headers.Add(TraceState, Encoding.UTF8.GetBytes(activity.TraceStateString));
        }
    }

    /// <summary>Lê o contexto dos headers, se houver e for válido.</summary>
    public static ActivityContext? Extract(Headers? headers)
    {
        if (headers is null || !headers.TryGetLastBytes(TraceParent, out var parent)) return null;

        string? state = headers.TryGetLastBytes(TraceState, out var stateBytes) ? Encoding.UTF8.GetString(stateBytes) : null;
        return ActivityContext.TryParse(Encoding.UTF8.GetString(parent), state, isRemote: true, out var context) ? context : null;
    }
}

/// <summary>
/// Lê o JSON de estatísticas do librdkafka (emitido a cada <c>statistics.interval.ms</c>) e extrai o lag por partição.
/// O librdkafka calcula <c>consumer_lag</c> = offset final do log − offset commitado, por partição que este consumer lê.
/// </summary>
public static class KafkaStatistics
{
    public static IReadOnlyList<(string Topic, int Partition, long Lag)> ParseLag(string json)
    {
        var result = new List<(string, int, long)>();
        using var doc = JsonDocument.Parse(json);

        if (!doc.RootElement.TryGetProperty("topics", out var topics)) return result;

        foreach (var topic in topics.EnumerateObject())
        {
            if (!topic.Value.TryGetProperty("partitions", out var partitions)) continue;

            foreach (var partition in partitions.EnumerateObject())
            {
                // A partição "-1" é interna (desconhecida); lag negativo significa "ainda não sabemos".
                if (partition.Name == "-1") continue;
                if (!partition.Value.TryGetProperty("consumer_lag", out var lag) || lag.ValueKind != JsonValueKind.Number) continue;
                if (lag.GetInt64() < 0) continue;

                result.Add((topic.Name, int.Parse(partition.Name), lag.GetInt64()));
            }
        }

        return result;
    }
}
