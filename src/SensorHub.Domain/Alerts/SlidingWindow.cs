namespace SensorHub.Domain.Alerts;

/// <summary>
/// Agregados de um intervalo fixo dentro da janela. Guarda apenas (contagem, soma, min, max,
/// primeiro e último valor), então a memória é O(nº de baldes), independente da taxa de leituras.
/// </summary>
public sealed class WindowBucket
{
    public long Index { get; set; }
    public int Count { get; set; }
    public double Sum { get; set; }
    public double Min { get; set; }
    public double Max { get; set; }
    public long FirstTicks { get; set; }
    public double First { get; set; }
    public long LastTicks { get; set; }
    public double Last { get; set; }
}

public readonly record struct WindowStats(
    int Count,
    double Sum,
    double Average,
    double Min,
    double Max,
    double OldestValue,
    double LatestValue,
    DateTimeOffset OldestTimestamp,
    DateTimeOffset LatestTimestamp)
{
    /// <summary>Quanto tempo de dados a janela realmente cobre (do primeiro ao último ponto).</summary>
    public TimeSpan Coverage => LatestTimestamp - OldestTimestamp;
}

/// <summary>
/// Janela deslizante em tempo de EVENTO (o timestamp da leitura, não o relógio da máquina),
/// implementada com baldes de largura fixa. É o "contador de janela" das regras de média e taxa de
/// variação. É serializável em JSON porque o estado vive no Redis entre reentregas/rebalanceamentos.
/// A precisão da borda da janela é a largura do balde.
/// </summary>
public sealed class SlidingWindow
{
    public long WindowTicks { get; set; }
    public long BucketTicks { get; set; }
    public List<WindowBucket> Buckets { get; set; } = [];

    public SlidingWindow() { } // desserialização

    public SlidingWindow(TimeSpan window, TimeSpan bucketWidth)
    {
        if (window <= TimeSpan.Zero) throw new ArgumentOutOfRangeException(nameof(window));
        if (bucketWidth <= TimeSpan.Zero || bucketWidth > window) throw new ArgumentOutOfRangeException(nameof(bucketWidth));
        WindowTicks = window.Ticks;
        BucketTicks = bucketWidth.Ticks;
    }

    public TimeSpan Window => TimeSpan.FromTicks(WindowTicks);

    private long BucketsPerWindow => Math.Max(1, (WindowTicks + BucketTicks - 1) / BucketTicks);

    /// <summary>Adiciona um ponto. Retorna false se o ponto já está fora da janela (dado atrasado demais).</summary>
    public bool Add(DateTimeOffset timestamp, double value)
    {
        var ticks = timestamp.UtcTicks;
        var index = ticks / BucketTicks;

        var newest = Buckets.Count > 0 ? Buckets[^1].Index : index;
        if (index <= newest - BucketsPerWindow) return false; // mais velho que a janela

        var position = Buckets.FindIndex(b => b.Index >= index);
        if (position >= 0 && Buckets[position].Index == index)
        {
            Merge(Buckets[position], ticks, value);
        }
        else
        {
            var bucket = new WindowBucket
            {
                Index = index, Count = 1, Sum = value, Min = value, Max = value,
                FirstTicks = ticks, First = value, LastTicks = ticks, Last = value
            };
            if (position < 0) Buckets.Add(bucket); else Buckets.Insert(position, bucket);
        }

        Evict();
        return true;
    }

    /// <summary>Estatísticas da janela terminando no último ponto conhecido; null se vazia.</summary>
    public WindowStats? Stats()
    {
        if (Buckets.Count == 0) return null;

        var oldest = Buckets[0];
        var newest = Buckets[^1];
        var count = 0;
        var sum = 0d;
        var min = double.MaxValue;
        var max = double.MinValue;
        foreach (var b in Buckets)
        {
            count += b.Count;
            sum += b.Sum;
            if (b.Min < min) min = b.Min;
            if (b.Max > max) max = b.Max;
        }

        return new WindowStats(
            count, sum, sum / count, min, max,
            oldest.First, newest.Last,
            new DateTimeOffset(oldest.FirstTicks, TimeSpan.Zero),
            new DateTimeOffset(newest.LastTicks, TimeSpan.Zero));
    }

    private static void Merge(WindowBucket b, long ticks, double value)
    {
        b.Count++;
        b.Sum += value;
        if (value < b.Min) b.Min = value;
        if (value > b.Max) b.Max = value;
        if (ticks < b.FirstTicks) { b.FirstTicks = ticks; b.First = value; }
        if (ticks >= b.LastTicks) { b.LastTicks = ticks; b.Last = value; }
    }

    private void Evict()
    {
        var newest = Buckets[^1].Index;
        var minIndex = newest - BucketsPerWindow + 1;
        var drop = 0;
        while (drop < Buckets.Count && Buckets[drop].Index < minIndex) drop++;
        if (drop > 0) Buckets.RemoveRange(0, drop);
    }
}
