namespace SensorHub.Infrastructure.Kafka;

public sealed class KafkaOptions
{
    public const string SectionName = "Kafka";

    public string BootstrapServers { get; set; } = "localhost:29092";

    /// <summary>Cria os tópicos ao iniciar. Desligar só em testes/ambientes onde outro processo provisiona.</summary>
    public bool ProvisionTopics { get; set; } = true;

    public string ReadingsTopic { get; set; } = "sensorhub.readings";
    public string ReadingsDlqTopic { get; set; } = "sensorhub.readings.dlq";
    public string AlertsTopic { get; set; } = "sensorhub.alerts";

    /// <summary>
    /// Nº de partições = teto de paralelismo de um consumer group (1 partição só é lida por 1 consumer
    /// do grupo). Escolhido acima do nº esperado de consumers para permitir escalar sem repartitionar:
    /// mudar o nº de partições depois REDISTRIBUI as chaves e quebra a ordem por sensor.
    /// </summary>
    public int ReadingsPartitions { get; set; } = 6;
    public int AlertsPartitions { get; set; } = 3;
    public short ReplicationFactor { get; set; } = 1;
    public int ReadingsRetentionHours { get; set; } = 168;

    public ProducerTuning Producer { get; set; } = new();

    public sealed class ProducerTuning
    {
        /// <summary>Espera até N ms para juntar mensagens num mesmo batch de rede (vazão vs. latência).</summary>
        public int LingerMs { get; set; } = 5;
        public int BatchSizeBytes { get; set; } = 131_072;
        public string Compression { get; set; } = "lz4";

        /// <summary>Tempo total para entregar (com retries). Curto de propósito: falhar rápido vira 503, não request pendurado.</summary>
        public int MessageTimeoutMs { get; set; } = 10_000;

        /// <summary>Tamanho da fila local do producer. Cheia = backpressure explícito (503) em vez de memória infinita.</summary>
        public int QueueMaxMessages { get; set; } = 200_000;
    }
}
