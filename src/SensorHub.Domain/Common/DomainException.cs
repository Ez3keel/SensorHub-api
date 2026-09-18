namespace SensorHub.Domain.Common;

/// <summary>Violação de invariante de domínio (dado inválido ou transição de estado proibida).</summary>
public class DomainException(string message) : Exception(message);
