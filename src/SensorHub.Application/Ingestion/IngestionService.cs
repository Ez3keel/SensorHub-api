using FluentValidation;
using SensorHub.Application.Contracts;
using SensorHub.Application.Observability;
using SensorHub.Application.Security;
using SensorHub.Domain.Sensors;
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
    TimeProvider clock,
    ISensorRegistry? registry = null)
{
    /// <summary>Ingestão sem identidade de dispositivo (segurança desligada: testes e quick-start local).</summary>
    public Task<IngestionResult> IngestAsync(IReadOnlyList<ReadingRequest> requests, CancellationToken cancellationToken) =>
        IngestAsync(requests, device: null, cancellationToken);

    /// <summary>
    /// Ingestão autenticada: além da validação de forma, cada leitura precisa ser de um sensor que EXISTE, está ATIVO e PERTENCE
    /// ao dispositivo autenticado. Sem isso, qualquer dispositivo com uma chave válida poderia injetar leituras no sensor de outro
    /// (um termômetro falso dispararia ou esconderia alertas de uma máquina que não é dele).
    /// </summary>
    public async Task<IngestionResult> IngestAsync(IReadOnlyList<ReadingRequest> requests, DeviceIdentity? device, CancellationToken cancellationToken)
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

            if (device is not null && registry is not null)
            {
                var problem = await CheckOwnershipAsync(request, device, cancellationToken);
                if (problem is not null)
                {
                    rejected.Add(new RejectedReading(i, [problem]));
                    continue;
                }
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

    private async Task<string?> CheckOwnershipAsync(ReadingRequest request, DeviceIdentity device, CancellationToken cancellationToken)
    {
        var sensor = await registry!.GetAsync(request.SensorId, cancellationToken);
        if (sensor is null) return "sensor desconhecido.";
        if (sensor.DeviceId != device.Id) return "o sensor não pertence a este dispositivo.";
        if (!sensor.Active) return "sensor inativo.";
        if (!sensor.Metric.IsPlausible(request.Value)) return $"valor fora da faixa plausível para {sensor.Metric}.";
        if (!string.IsNullOrWhiteSpace(request.Unit) && !string.Equals(request.Unit.Trim(), sensor.Unit, StringComparison.OrdinalIgnoreCase))
            return $"unidade divergente do cadastro do sensor ({sensor.Unit}).";
        return null;
    }
}
