using System.Security.Cryptography;
using System.Text;
using Microsoft.Extensions.Options;
using SensorHub.Application.Security;

namespace SensorHub.MqttBridge;

/// <summary>
/// Decide quem entra no broker e o que pode fazer. O Mosquitto (plugin go-auth, backend HTTP) pergunta a este serviço:
///  - dispositivo: usuário = id do dispositivo, senha = a MESMA chave de API da ingestão HTTP (uma credencial só, rotacionável em um lugar);
///  - bridge: usuário/senha próprios, com poder de ler tudo (superusuário);
///  - ACL: um dispositivo só PUBLICA no próprio tópico e não lê nada (não vê dados de outros dispositivos).
/// </summary>
public sealed class MqttAccessPolicy(IOptions<MqttBridgeOptions> options, IDeviceAuthenticator authenticator)
{
    public const int AccessRead = 1;
    public const int AccessWrite = 2;
    public const int AccessSubscribe = 4;

    public bool IsBridge(string? username) =>
        !string.IsNullOrEmpty(username) && string.Equals(username, options.Value.BridgeUsername, StringComparison.Ordinal);

    public async Task<bool> AuthenticateAsync(string? username, string? password, CancellationToken cancellationToken)
    {
        if (string.IsNullOrEmpty(username) || string.IsNullOrEmpty(password)) return false;

        if (IsBridge(username))
            // comparação em tempo constante: não vaza, pelo tempo de resposta, quantos caracteres da senha estão certos
            return CryptographicOperations.FixedTimeEquals(Encoding.UTF8.GetBytes(password), Encoding.UTF8.GetBytes(options.Value.BridgePassword));

        if (!Guid.TryParseExact(username, "D", out var deviceId)) return false;

        var identity = await authenticator.AuthenticateAsync(password, cancellationToken);
        // a chave precisa ser DESTE dispositivo: uma chave válida de outro não autentica como você
        return identity is not null && identity.Id == deviceId;
    }

    /// <summary>Regra pura (sem I/O): o dispositivo só escreve no próprio tópico.</summary>
    public static bool DeviceMayAccess(string username, string topic, int access) =>
        access == AccessWrite
        && Guid.TryParseExact(username, "D", out var deviceId)
        && MqttTopics.TryGetDeviceId(topic, out var topicDevice)
        && topicDevice == deviceId;
}
