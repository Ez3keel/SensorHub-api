using Microsoft.AspNetCore.Diagnostics;
using Microsoft.AspNetCore.Mvc;
using SensorHub.Application.Ingestion;
using SensorHub.Application.Queries;

namespace SensorHub.Api.Infrastructure;

/// <summary>
/// Converte falhas em respostas HTTP corretas. O caso central é <see cref="IngestionUnavailableException"/>:
/// se o Kafka não aceita, respondemos 503 + Retry-After em vez de 500. Isso é o backpressure chegando ao
/// dispositivo, que deve reenviar com backoff (a idempotência no consumer absorve a repetição).
/// </summary>
public sealed class GlobalExceptionHandler(ILogger<GlobalExceptionHandler> logger) : IExceptionHandler
{
    public async ValueTask<bool> TryHandleAsync(HttpContext context, Exception exception, CancellationToken cancellationToken)
    {
        if (exception is IngestionUnavailableException)
        {
            logger.LogWarning(exception, "Ingestão indisponível; devolvendo 503 ao cliente.");
            context.Response.Headers.RetryAfter = "1";
            await Write(context, StatusCodes.Status503ServiceUnavailable, "Serviço temporariamente indisponível", exception.Message, cancellationToken);
            return true;
        }

        if (exception is QueryValidationException invalid)
        {
            await Write(context, StatusCodes.Status400BadRequest, "Consulta inválida", invalid.Message, cancellationToken);
            return true;
        }

        if (exception is BadHttpRequestException bad)
        {
            await Write(context, bad.StatusCode, "Requisição inválida", bad.Message, cancellationToken);
            return true;
        }

        logger.LogError(exception, "Erro não tratado em {Path}", context.Request.Path);
        await Write(context, StatusCodes.Status500InternalServerError, "Erro interno", "Ocorreu um erro inesperado.", cancellationToken);
        return true;
    }

    private static Task Write(HttpContext context, int status, string title, string detail, CancellationToken ct)
    {
        context.Response.StatusCode = status;
        return context.Response.WriteAsJsonAsync(new ProblemDetails { Status = status, Title = title, Detail = detail }, ct);
    }
}
