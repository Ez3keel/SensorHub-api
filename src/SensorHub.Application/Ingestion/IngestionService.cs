using FluentValidation;
using SensorHub.Application.Contracts;
using SensorHub.Application.Observability;
using SensorHub.Domain.Common;
using SensorHub.Domain.Readings;

namespace SensorHub.Application.Ingestion;

/// <summary>
/// Caso de uso de ingestão: valida, normaliza e publica. NÃO escreve em banco: o trabalho lento
/// fica com os consumers, e por isso a API responde em milissegundos mesmo sob pico.
/// Aceitação parcial: uma leitura inválida não derruba as outras 999 do lote.
/// </summary>
public sealed class IngestionService(
    IValidator<ReadingRequest> validator,
    IReadingPublisher publisher,
    TimeProvider clock)
{
    public async Task<IngestionResult> IngestAsync(IReadOnlyList<ReadingRequest> requests, CancellationToken cancellationToken)
    {
        var now = clock.GetUtcNow();
        var accepted = new List<ReadingMessage>(requests.Count);
        var rejected = new List<RejectedReading>();

        for (var i = 0; i < requests.Count; i++)
        {
            var request = requests[i];

            var validation = await validator.ValidateAsync(request, cancellationToken);
            if (!validation.IsValid)
            {
                rejected.Add(new RejectedReading(i, validation.Errors.Select(e => e.ErrorMessage).ToList()));
                continue;
            }

            try
            {
                var reading = Reading.Create(request.SensorId, request.Timestamp ?? now, request.Value);
                accepted.Add(ReadingMessage.From(reading, request.Unit?.Trim(), now));
            }
            catch (DomainException ex)
            {
                rejected.Add(new RejectedReading(i, [ex.Message]));
            }
        }

        if (accepted.Count > 0)
            await publisher.PublishAsync(accepted, cancellationToken);

        SensorHubTelemetry.IngestedReadings.Add(accepted.Count, new KeyValuePair<string, object?>("result", "accepted"));
        if (rejected.Count > 0)
            SensorHubTelemetry.IngestedReadings.Add(rejected.Count, new KeyValuePair<string, object?>("result", "rejected"));

        return new IngestionResult(accepted.Count, rejected);
    }
}
