# SensorHub

Plataforma de telemetria IoT em C#/.NET 10: ingere grande volume de leituras de sensores (temperatura,
vibração, umidade...) sem que picos de tráfego derrubem a API, guarda o histórico em série temporal,
detecta anomalias em stream e mostra um dashboard ao vivo.

Terceiro projeto de portfólio backend (depois de TicketFlow e QuickOrder). Tecnologia nova principal: **Apache Kafka**.

> As decisões de arquitetura, o porquê de cada escolha e os bugs reais encontrados estão em
> [`docs/DECISOES-DE-ARQUITETURA.md`](docs/DECISOES-DE-ARQUITETURA.md).

## Arquitetura (alvo)

```
Dispositivos ──HTTP/MQTT──► API de ingestão ──► Kafka (readings, chave = sensorId)
                                                   │
              ┌────────────────────────────────────┼──────────────────────────┐
              ▼                                    ▼                          ▼
   Worker de persistência              Worker de alertas               Realtime (SignalR)
   (batch → TimescaleDB,               (regras em stream,              (push por sensor/grupo
    último valor → Redis)               estado no Redis)                → dashboard React)
```

Três *consumer groups* independentes leem o mesmo tópico: é o que o Kafka faz e uma fila tradicional não.

## Roadmap

| Fase | Módulo | Status |
|---|---|---|
| 0 | Domínio + simulador de sensores | ✅ |
| 1 | Ingestão HTTP → Kafka | ✅ |
| 2 | Consumer com batching + persistência | ✅ |
| 3 | TimescaleDB: agregações e retenção | ✅ |
| 4 | Redis: último valor (janelas na Fase 5) | ✅ |
| 5 | Alertas em stream | ✅ |
| 6 | Tempo real (SignalR) + dashboard React | ✅ |
| 7 | Observabilidade (OpenTelemetry, lag do consumer) | ⏳ |
| 8 | Segurança e API de administração | ⏳ |
| 9 | Compose completo + prova de carga | ⏳ |
| 10 | MQTT (bridge → Kafka) | ⏳ |

## Executando localmente

```bash
docker compose up -d kafka timescaledb redis   # infraestrutura
dotnet run --project src/SensorHub.Api           # API de ingestão (http://localhost:5080)
dotnet run --project src/SensorHub.Worker        # consumers: persistência, último valor e alertas\n(cd web/dashboard && npm install && npm run dev)  # dashboard em http://localhost:5173
dotnet run --project tools/SensorHub.Simulator -c Release -- --mode register --sensors 300 --rules threshold,nodata   # cadastra a frota + regras\ndotnet run --project tools/SensorHub.Simulator -c Release -- --mode http --sensors 300 --rate 3000 --duration 45 --silence-after 15 --silence-fraction 0.1
```

## Testes

```bash
dotnet test
```

Testes de integração usam Testcontainers e exigem Docker em execução.

## Simulador de carga

```bash
dotnet run --project tools/SensorHub.Simulator -c Release -- --sensors 1000 --rate 20000 --duration 30
```

Veja `--help` para todas as opções (duplicatas, hot key, workers).
