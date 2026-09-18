using FluentValidation;
using Microsoft.Extensions.Options;

namespace SensorHub.Application.Ingestion;

/// <summary>
/// Validação de FORMA e de plausibilidade temporal, feita na borda antes de publicar no Kafka.
/// Invariantes de domínio (ex: valor finito) são reafirmadas por <c>Reading.Create</c>.
/// </summary>
public sealed class ReadingRequestValidator : AbstractValidator<ReadingRequest>
{
    public ReadingRequestValidator(IOptions<IngestionOptions> options, TimeProvider clock)
    {
        var settings = options.Value;

        RuleFor(r => r.SensorId)
            .NotEmpty().WithMessage("sensorId é obrigatório.");

        RuleFor(r => r.Value)
            .Must(double.IsFinite).WithMessage("value deve ser um número finito.");

        RuleFor(r => r.Unit)
            .MaximumLength(16).WithMessage("unit excede 16 caracteres.");

        RuleFor(r => r.Timestamp)
            .Must(ts => ts is null || ts.Value <= clock.GetUtcNow() + settings.MaxFutureSkew)
            .WithMessage($"timestamp está no futuro além da tolerância de {settings.MaxFutureSkew}.")
            .Must(ts => ts is null || ts.Value >= clock.GetUtcNow() - settings.MaxAge)
            .WithMessage($"timestamp é mais antigo que o máximo aceito ({settings.MaxAge}).");
    }
}
