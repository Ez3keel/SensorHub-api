using Microsoft.AspNetCore.Mvc;
using Microsoft.Extensions.Options;
using SensorHub.Application.Ingestion;

namespace SensorHub.Api.Controllers;

/// <summary>
/// Borda de ingestão. Só valida e publica; NUNCA toca o banco. A resposta 202 significa "a leitura está
/// durável no log do Kafka", e o processamento (persistência, alertas, tempo real) é assíncrono.
/// </summary>
[ApiController]
[Route("api/readings")]
public sealed class ReadingsController(IngestionService ingestion, IOptions<IngestionOptions> options) : ControllerBase
{
    /// <summary>Ingere uma leitura.</summary>
    [HttpPost]
    [ProducesResponseType(StatusCodes.Status202Accepted)]
    [ProducesResponseType(typeof(ValidationProblemDetails), StatusCodes.Status400BadRequest)]
    [ProducesResponseType(StatusCodes.Status503ServiceUnavailable)]
    public async Task<IActionResult> Post([FromBody] ReadingRequest request, CancellationToken cancellationToken)
    {
        var result = await ingestion.IngestAsync([request], cancellationToken);

        if (result.HasRejections)
        {
            var errors = new Dictionary<string, string[]> { ["reading"] = result.Rejected[0].Errors.ToArray() };
            return ValidationProblem(new ValidationProblemDetails(errors));
        }

        return Accepted(new { accepted = 1 });
    }

    /// <summary>
    /// Ingere um lote (até <c>Ingestion:MaxBatchSize</c>). Aceitação parcial: leituras inválidas são
    /// devolvidas com o índice e o motivo; as válidas são publicadas.
    /// </summary>
    [HttpPost("batch")]
    [RequestSizeLimit(2_000_000)]
    [ProducesResponseType(typeof(BatchResponse), StatusCodes.Status202Accepted)]
    [ProducesResponseType(typeof(ValidationProblemDetails), StatusCodes.Status400BadRequest)]
    [ProducesResponseType(typeof(BatchResponse), StatusCodes.Status422UnprocessableEntity)]
    [ProducesResponseType(StatusCodes.Status503ServiceUnavailable)]
    public async Task<IActionResult> PostBatch([FromBody] List<ReadingRequest>? requests, CancellationToken cancellationToken)
    {
        var max = options.Value.MaxBatchSize;

        if (requests is null || requests.Count == 0)
            return BatchProblem($"O lote deve conter pelo menos 1 leitura.");

        if (requests.Count > max)
            return BatchProblem($"O lote excede o máximo de {max} leituras.");

        var result = await ingestion.IngestAsync(requests, cancellationToken);
        var response = new BatchResponse(result.Accepted, result.Rejected);

        // 202 se algo foi aceito; 422 se TUDO foi rejeitado (nada a reenviar sem corrigir).
        return result.Accepted > 0 ? Accepted(response) : UnprocessableEntity(response);
    }

    private IActionResult BatchProblem(string detail) =>
        ValidationProblem(new ValidationProblemDetails(new Dictionary<string, string[]> { ["readings"] = [detail] }));

    public sealed record BatchResponse(int Accepted, IReadOnlyList<RejectedReading> Rejected);
}
