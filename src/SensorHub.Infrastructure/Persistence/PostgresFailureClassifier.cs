using Npgsql;

namespace SensorHub.Infrastructure.Persistence;

/// <summary>
/// Separa falha de DADO (repetir nunca resolve: vai para a DLQ) de falha de INFRA (repetir resolve quando
/// o banco voltar). Classificar errado é caro nos dois sentidos: tratar queda de banco como "dado ruim"
/// esvaziaria o tópico para a DLQ; tratar dado ruim como "transitório" travaria a partição para sempre.
/// </summary>
public static class PostgresFailureClassifier
{
    /// <summary>SQLSTATE classe 22 (data exception) e 23 (integrity constraint violation) são falhas de dado.</summary>
    public static bool IsPoison(Exception exception) =>
        exception is PostgresException { SqlState: { } state } && (state.StartsWith("22", StringComparison.Ordinal) || state.StartsWith("23", StringComparison.Ordinal));
}
