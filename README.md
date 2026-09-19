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
| 7 | Observabilidade (OpenTelemetry, lag do consumer) | ✅ |
| 8 | Segurança (JWT, chaves de API por dispositivo, rate limit) e administração | ✅ |
| 9 | Compose completo + prova de carga | ⏳ |
| 10 | MQTT (bridge → Kafka) | ⏳ |

## Executando localmente

```bash
docker compose up -d kafka timescaledb redis prometheus kafka-exporter jaeger grafana   # infraestrutura + observabilidade
dotnet run --project src/SensorHub.Api           # API (http://localhost:5080); em Development a segurança vem ligada
dotnet run --project src/SensorHub.Worker        # consumers: persistência, último valor e alertas
(cd web/dashboard && npm install && npm run dev)  # dashboard em http://localhost:5173
```

Cadastre a frota (como administrador) e envie carga com a chave do dispositivo criada:

```bash
dotnet run --project tools/SensorHub.Simulator -c Release -- --mode register --sensors 300 --rules threshold,nodata \
  --admin-email admin@sensorhub.local --admin-password Admin-dev-123456 --key-out gateway.key
dotnet run --project tools/SensorHub.Simulator -c Release -- --mode http --sensors 300 --rate 3000 --duration 45 \
  --api-key "$(cat gateway.key)" --silence-after 15 --silence-fraction 0.1
```

O administrador acima existe **só em desenvolvimento** (`appsettings.Development.json`). Para rodar sem autenticação, use `Security__Enabled=false`.

## Segurança

| Quem | Como se autentica | O que pode |
|---|---|---|
| Dispositivo | `X-Api-Key: shk_...` (uma por dispositivo, exibida uma única vez, rotacionável) | ingerir leituras **só dos próprios sensores** |
| Pessoa | `POST /api/auth/login` → JWT de 15 min + refresh rotativo | Viewer lê, Operator reconhece alertas e cria regras, Admin gerencia dispositivos e usuários |

Rate limiting por dispositivo (ingestão), por IP (login) e por usuário (API). Detalhes e bugs encontrados: seção "Fase 8" do `docs/DECISOES-DE-ARQUITETURA.md`.

## Observabilidade

| O quê | Onde |
|---|---|
| Dashboard (lag, latência, throughput) | http://localhost:3001 (Grafana, sem login) |
| Traces (HTTP → Kafka → consumer → banco) | http://localhost:16686 (Jaeger) |
| Métricas brutas | API http://localhost:5080/metrics, Worker http://localhost:9464/metrics |
| Prometheus | http://localhost:9091 |

Para enviar os traces, inicie API e Worker com `Observability__OtlpEndpoint=http://localhost:14317`.

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
