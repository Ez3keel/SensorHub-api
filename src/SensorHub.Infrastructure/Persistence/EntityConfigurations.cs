using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Metadata.Builders;
using SensorHub.Domain.Alerts;
using SensorHub.Domain.Sensors;

namespace SensorHub.Infrastructure.Persistence;

/// <summary>
/// Mapeamento das entidades relacionais. Nomes de coluna explícitos em snake_case e enums gravados como TEXTO
/// (legível em qualquer consulta SQL e sem quebrar se a ordem do enum mudar).
/// </summary>
internal sealed class SensorConfiguration : IEntityTypeConfiguration<Sensor>
{
    public void Configure(EntityTypeBuilder<Sensor> b)
    {
        b.ToTable("sensors");
        b.HasKey(s => s.Id);
        b.Property(s => s.Id).HasColumnName("id");
        b.Property(s => s.DeviceId).HasColumnName("device_id");
        b.Property(s => s.Name).HasColumnName("name").HasMaxLength(100).IsRequired();
        b.Property(s => s.Metric).HasColumnName("metric").HasConversion<string>().HasMaxLength(20);
        b.Property(s => s.Unit).HasColumnName("unit").HasMaxLength(16).IsRequired();
        b.Property(s => s.Group).HasColumnName("group_name").HasMaxLength(100).IsRequired();
        b.Property(s => s.Active).HasColumnName("active");
        b.Property(s => s.CreatedAt).HasColumnName("created_at");

        b.HasIndex(s => s.Group).HasDatabaseName("ix_sensors_group");
        b.HasIndex(s => s.DeviceId).HasDatabaseName("ix_sensors_device");
    }
}

internal sealed class AlertRuleConfiguration : IEntityTypeConfiguration<AlertRule>
{
    public void Configure(EntityTypeBuilder<AlertRule> b)
    {
        b.ToTable("alert_rules");
        b.HasKey(r => r.Id);
        b.Property(r => r.Id).HasColumnName("id");
        b.Property(r => r.SensorId).HasColumnName("sensor_id");
        b.Property(r => r.Name).HasColumnName("name").HasMaxLength(120).IsRequired();
        b.Property(r => r.Type).HasColumnName("type").HasConversion<string>().HasMaxLength(20);
        b.Property(r => r.Comparison).HasColumnName("comparison").HasConversion<string>().HasMaxLength(20);
        b.Property(r => r.Threshold).HasColumnName("threshold");
        b.Property(r => r.Duration).HasColumnName("duration");
        b.Property(r => r.Hysteresis).HasColumnName("hysteresis");
        b.Property(r => r.Severity).HasColumnName("severity").HasConversion<string>().HasMaxLength(20);
        b.Property(r => r.Enabled).HasColumnName("enabled");
        b.Property(r => r.CreatedAt).HasColumnName("created_at");

        // Regra é do sensor: apagar o sensor apaga as regras dele. Alertas históricos permanecem (ver AlertConfiguration).
        b.HasOne<Sensor>().WithMany().HasForeignKey(r => r.SensorId).OnDelete(DeleteBehavior.Cascade);
        b.HasIndex(r => r.SensorId).HasDatabaseName("ix_alert_rules_sensor");
    }
}

internal sealed class AlertConfiguration : IEntityTypeConfiguration<Alert>
{
    public void Configure(EntityTypeBuilder<Alert> b)
    {
        b.ToTable("alerts");
        b.HasKey(a => a.Id);
        b.Property(a => a.Id).HasColumnName("id");
        b.Property(a => a.RuleId).HasColumnName("rule_id");
        b.Property(a => a.SensorId).HasColumnName("sensor_id");
        b.Property(a => a.Severity).HasColumnName("severity").HasConversion<string>().HasMaxLength(20);
        b.Property(a => a.Status).HasColumnName("status").HasConversion<string>().HasMaxLength(20);
        b.Property(a => a.Message).HasColumnName("message").HasMaxLength(500).IsRequired();
        b.Property(a => a.TriggerValue).HasColumnName("trigger_value");
        b.Property(a => a.FiredAt).HasColumnName("fired_at");
        b.Property(a => a.AcknowledgedAt).HasColumnName("acknowledged_at");
        b.Property(a => a.AcknowledgedBy).HasColumnName("acknowledged_by").HasMaxLength(100);
        b.Property(a => a.ResolvedAt).HasColumnName("resolved_at");
        b.Property(a => a.ResolvedValue).HasColumnName("resolved_value");

        // SEM chave estrangeira para a regra: o histórico de alertas sobrevive à exclusão da regra.
        b.HasIndex(a => new { a.SensorId, a.FiredAt }).IsDescending(false, true).HasDatabaseName("ix_alerts_sensor_fired");

        // Garantia no BANCO de que existe no máximo UM alerta aberto por regra. Mesmo que o estado no Redis se
        // perca e a regra "dispare de novo", o segundo INSERT viola este índice e vira no-op. Rede de segurança
        // final contra alerta duplicado, independente de qualquer lógica de aplicação.
        b.HasIndex(a => a.RuleId).IsUnique().HasFilter("status <> 'Resolved'").HasDatabaseName("ux_alerts_one_open_per_rule");
        b.HasIndex(a => a.Status).HasFilter("status <> 'Resolved'").HasDatabaseName("ix_alerts_open");
    }
}
