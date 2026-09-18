using SensorHub.Domain.Readings;

namespace SensorHub.Application.Processing;

/// <summary>Uma leitura já validada, vinda do log, com o instante em que a API a ingeriu.</summary>
public readonly record struct ConsumedReading(Reading Reading, DateTimeOffset IngestedAt);

/// <summary>
/// Processa um LOTE de leituras. É a unidade de trabalho de um consumer: persistir, avaliar alertas,
/// empurrar para o dashboard... cada um é um handler com o seu próprio consumer group.
/// Contrato: ser IDEMPOTENTE. O Kafka entrega "pelo menos uma vez", então o mesmo lote pode chegar de novo
/// (consumer caiu depois de gravar e antes de commitar o offset).
/// </summary>
public interface IReadingBatchHandler
{
    Task HandleAsync(IReadOnlyList<ConsumedReading> batch, CancellationToken cancellationToken);
}
