namespace SensorHub.MqttBridge;

public sealed class MqttBridgeOptions
{
    public const string SectionName = "Mqtt";
    public const int MinSecretLength = 16;

    /// <summary>
    /// Papéis do processo (como no Worker): <c>auth</c> = backend de autenticação/ACL do broker; <c>bridge</c> = consumidor MQTT -> Kafka.
    /// Vazio = os dois (desenvolvimento). Em produção rodam SEPARADOS: se a autenticação vivesse no mesmo processo do consumidor, um deploy ou
    /// queda do bridge faria o broker falhar TODA verificação de ACL e desconectaria todos os dispositivos. Vazio (e não um default preenchido)
    /// porque o binder de configuração ANEXA itens a um array com valor padrão.
    /// </summary>
    public string[] Roles { get; set; } = [];

    public bool HasRole(string role) => Roles.Length == 0 || Roles.Contains(role, StringComparer.OrdinalIgnoreCase);

    public string Host { get; set; } = "localhost";
    public int Port { get; set; } = 1883;

    /// <summary>Identidade privilegiada do próprio bridge no broker (única com direito de ler os tópicos de todos os dispositivos).</summary>
    public string BridgeUsername { get; set; } = "sensorhub-bridge";
    public string BridgePassword { get; set; } = "";

    /// <summary>Réplicas do bridge no MESMO grupo compartilham a carga (<c>$share</c>): cada mensagem vai a UMA delas.</summary>
    public string SharedGroup { get; set; } = "sensorhub-bridge";

    /// <summary>Quanto tempo o broker guarda a sessão (e as mensagens QoS 1 pendentes) de um bridge que caiu.</summary>
    public int SessionExpirySeconds { get; set; } = 3600;

    /// <summary>Acima disso a mensagem é descartada como malformada (o broker também limita: <c>message_size_limit</c>).</summary>
    public int MaxPayloadBytes { get; set; } = 262_144;

    /// <summary>
    /// Mensagens processadas ao mesmo tempo por réplica. Um dispositivo manda mensagens PEQUENAS; processá-las uma a uma limita a vazão à
    /// latência de cada uma (medido: ~30 ms => ~30 msg/s => centenas de leituras/s). Com N em voo, o produtor Kafka ainda agrupa os lotes.
    /// Chegando a N, o bridge para de ler do socket e o broker (max_inflight_messages) segura o resto: contrapressão sem fila em memória.
    /// </summary>
    public int MaxConcurrency { get; set; } = 64;

    /// <summary>Tentativas de publicar no Kafka antes de desistir e derrubar a conexão para o broker reentregar.</summary>
    public int PublishRetries { get; set; } = 5;

    /// <summary>Falha na SUBIDA, não no primeiro dispositivo: sem segredo forte o bridge abriria o broker a qualquer um.</summary>
    public void Validate(bool isDevelopment)
    {
        if (string.IsNullOrWhiteSpace(BridgePassword) || BridgePassword.Length < MinSecretLength)
            throw new InvalidOperationException($"Mqtt:BridgePassword ausente ou curta (mínimo {MinSecretLength} caracteres). Defina Mqtt__BridgePassword.");
        if (BridgePassword.StartsWith("dev-only", StringComparison.Ordinal) && !isDevelopment)
            throw new InvalidOperationException("Mqtt:BridgePassword é a senha de desenvolvimento do repositório; recusada fora de Development.");
    }
}
