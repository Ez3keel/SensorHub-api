using Microsoft.EntityFrameworkCore.Migrations;

#nullable disable

namespace SensorHub.Infrastructure.Persistence.Migrations
{
    /// <summary>
    /// Converte <c>readings</c> em hypertable do TimescaleDB e monta o ciclo de vida do dado:
    /// chunks diários -> columnstore (compressão) após 7 dias -> retenção de 90 dias no bruto, com agregados
    /// contínuos (1 min e 1 h) que sobrevivem por mais tempo. Decisões e números: ADR, Fase 3.
    ///
    /// Cada instrução de agregado contínuo roda FORA da transação da migration (suppressTransaction) porque o
    /// TimescaleDB não permite criar continuous aggregates dentro de um bloco transacional. Todas são
    /// idempotentes (IF NOT EXISTS / if_not_exists) para que uma migration interrompida possa ser reexecutada.
    /// </summary>
    public partial class EnableTimescaleHypertable : Migration
    {
        protected override void Up(MigrationBuilder migrationBuilder)
        {
            migrationBuilder.Sql("CREATE EXTENSION IF NOT EXISTS timescaledb;", suppressTransaction: true);

            // Chunks de 1 dia: cada chunk (dados + índice PK) deve caber em ~25% da RAM para que o índice quente
            // permaneça em memória. A 100 mil leituras/s o dia tem ~8,6 bi de linhas; ajuste conforme o volume real.
            // migrate_data preserva o que a Fase 2 já gravou.
            migrationBuilder.Sql("""
                SELECT create_hypertable('readings', 'ts',
                    chunk_time_interval => INTERVAL '1 day',
                    migrate_data => true,
                    if_not_exists => true);
                """, suppressTransaction: true);

            // Columnstore (a antiga "compressão"): segmenta por sensor e ordena por tempo desc. Uma consulta
            // "histórico de UM sensor" lê só os segmentos daquele sensor, já ordenados.
            migrationBuilder.Sql("""
                ALTER TABLE readings SET (
                    timescaledb.enable_columnstore = true,
                    timescaledb.segmentby = 'sensor_id',
                    timescaledb.orderby = 'ts DESC');
                """, suppressTransaction: true);

            migrationBuilder.Sql("CALL add_columnstore_policy('readings', after => INTERVAL '7 days', if_not_exists => true);", suppressTransaction: true);
            migrationBuilder.Sql("SELECT add_retention_policy('readings', INTERVAL '90 days', if_not_exists => true);", suppressTransaction: true);

            // Agregado de 1 minuto. Guarda SOMA e CONTAGEM (não a média): a média de médias está errada quando
            // as contagens diferem, e é a soma/contagem que permite reagregar em 5 min, 1 h, 1 dia sem perda.
            migrationBuilder.Sql("""
                CREATE MATERIALIZED VIEW IF NOT EXISTS readings_1m
                WITH (timescaledb.continuous) AS
                SELECT sensor_id,
                       time_bucket(INTERVAL '1 minute', ts) AS bucket,
                       sum(value)  AS sum,
                       count(*)    AS count,
                       min(value)  AS min,
                       max(value)  AS max
                FROM readings
                GROUP BY sensor_id, time_bucket(INTERVAL '1 minute', ts)
                WITH NO DATA;
                """, suppressTransaction: true);

            // Agregação em tempo real: a consulta combina o materializado com o dado ainda não materializado,
            // então o último minuto aparece sem esperar o job de refresh.
            migrationBuilder.Sql("ALTER MATERIALIZED VIEW readings_1m SET (timescaledb.materialized_only = false);", suppressTransaction: true);

            // Janela de refresh de 7 dias = a idade máxima aceita pela ingestão: leitura atrasada que chegue
            // dentro desse prazo invalida o balde e é recalculada. O custo é proporcional ao que foi invalidado,
            // não ao tamanho da janela.
            migrationBuilder.Sql("""
                SELECT add_continuous_aggregate_policy('readings_1m',
                    start_offset => INTERVAL '7 days',
                    end_offset => INTERVAL '1 minute',
                    schedule_interval => INTERVAL '30 seconds',
                    if_not_exists => true);
                """, suppressTransaction: true);

            // Agregado hierárquico: 1 hora calculada a partir do de 1 minuto (nunca relê o bruto).
            migrationBuilder.Sql("""
                CREATE MATERIALIZED VIEW IF NOT EXISTS readings_1h
                WITH (timescaledb.continuous) AS
                SELECT sensor_id,
                       time_bucket(INTERVAL '1 hour', bucket) AS bucket,
                       sum(sum)    AS sum,
                       sum(count)  AS count,
                       min(min)    AS min,
                       max(max)    AS max
                FROM readings_1m
                GROUP BY sensor_id, time_bucket(INTERVAL '1 hour', bucket)
                WITH NO DATA;
                """, suppressTransaction: true);

            migrationBuilder.Sql("ALTER MATERIALIZED VIEW readings_1h SET (timescaledb.materialized_only = false);", suppressTransaction: true);

            migrationBuilder.Sql("""
                SELECT add_continuous_aggregate_policy('readings_1h',
                    start_offset => INTERVAL '30 days',
                    end_offset => INTERVAL '1 hour',
                    schedule_interval => INTERVAL '5 minutes',
                    if_not_exists => true);
                """, suppressTransaction: true);

            // Retenção escalonada: bruto 90 dias, minuto 1 ano, hora 5 anos. Quanto mais grosso, mais barato guardar.
            migrationBuilder.Sql("SELECT add_retention_policy('readings_1m', INTERVAL '365 days', if_not_exists => true);", suppressTransaction: true);
            migrationBuilder.Sql("SELECT add_retention_policy('readings_1h', INTERVAL '1825 days', if_not_exists => true);", suppressTransaction: true);
        }

        /// <summary>
        /// Melhor esforço: remove agregados e políticas. Uma hypertable NÃO volta a ser tabela comum sem copiar
        /// os dados, então ela permanece (continua sendo uma tabela válida, com o mesmo esquema e chave).
        /// </summary>
        protected override void Down(MigrationBuilder migrationBuilder)
        {
            migrationBuilder.Sql("DROP MATERIALIZED VIEW IF EXISTS readings_1h;", suppressTransaction: true);
            migrationBuilder.Sql("DROP MATERIALIZED VIEW IF EXISTS readings_1m;", suppressTransaction: true);
            migrationBuilder.Sql("SELECT remove_retention_policy('readings', if_exists => true);", suppressTransaction: true);
            migrationBuilder.Sql("CALL remove_columnstore_policy('readings', if_exists => true);", suppressTransaction: true);
        }
    }
}
