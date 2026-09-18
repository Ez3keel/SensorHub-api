using Microsoft.EntityFrameworkCore.Migrations;

#nullable disable

namespace SensorHub.Infrastructure.Persistence.Migrations
{
    /// <summary>
    /// Tabela de histórico de leituras. SQL explícito (fora do modelo do EF) porque o caminho quente grava com
    /// COPY/unnest do Npgsql, e não por entidades.
    ///
    /// A chave primária (sensor_id, ts) É a chave de idempotência: reentrega do Kafka ou reenvio do dispositivo
    /// produz a mesma chave e é descartado por ON CONFLICT DO NOTHING. Também é a chave natural de consulta
    /// (histórico de UM sensor em um intervalo de tempo), então o índice serve leitura e deduplicação.
    /// </summary>
    public partial class CreateReadingsTable : Migration
    {
        protected override void Up(MigrationBuilder migrationBuilder)
        {
            migrationBuilder.Sql("""
                CREATE TABLE readings (
                    sensor_id uuid             NOT NULL,
                    ts        timestamptz      NOT NULL,
                    value     double precision NOT NULL,
                    CONSTRAINT pk_readings PRIMARY KEY (sensor_id, ts)
                );
                """);
        }

        protected override void Down(MigrationBuilder migrationBuilder)
        {
            migrationBuilder.Sql("DROP TABLE IF EXISTS readings;");
        }
    }
}
