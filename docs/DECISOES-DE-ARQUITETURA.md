# SensorHub: Decisões de Arquitetura

Registro do **porquê** de cada escolha, uma seção por fase. Bugs reais encontrados no caminho ficam na
seção "Bugs encontrados" de cada fase, com a causa raiz.

## Visão geral

SensorHub é uma plataforma de telemetria IoT: ingere grande volume de leituras de sensores sem que picos
derrubem a API, guarda o histórico, detecta anomalias e mostra um dashboard ao vivo.

### Decisões de modelagem (aprovadas antes de começar)

| Decisão | Escolha | Alternativa descartada | Por quê |
|---|---|---|---|
| Formato da leitura | 1 métrica por leitura: `{sensorId, ts, value, unit}` | payload multi-métrica por dispositivo | Regras, cache de último valor, chave de partição e idempotência ficam todos por `sensorId`. O dispositivo com N métricas manda N leituras (em um único batch HTTP). |
| Chave de partição Kafka | `sensorId` | `deviceId` / grupo | Ordem garantida por sensor e distribuição uniforme com muitos sensores. Risco (hot key) é demonstrado e medido na Fase 9. |
| Banco de séries temporais | TimescaleDB + benchmark contra Postgres puro | só Postgres / só Timescale | Hypertable, compressão, retenção e continuous aggregates; o benchmark documenta o ganho real. |
| Protocolos | HTTP no núcleo; MQTT em fase própria (bridge → Kafka) | MQTT como entrada principal | Não arriscar o núcleo; MQTT é o protocolo real de IoT e mostra o padrão de gateway. |
| Autenticação | API key por dispositivo (ingestão) + JWT com refresh rotativo (admin/dashboard) | só API key / sem auth | Dispositivo é máquina (chave revogável e barata de validar); pessoa é usuário (JWT). |
| Idempotência | chave natural `(sensor_id, ts)` + `ON CONFLICT DO NOTHING` | UUID gerado pelo dispositivo | Um sensor não mede duas vezes no mesmo instante; casa com o índice único obrigatório da hypertable. |
| Regras de alerta | limite+duração, sem dados, média em janela, taxa de variação | só limite | Cobrem os 3 padrões de stream: estado por valor, ausência de evento e agregação em janela. |
| Dashboard | frontend React separado (SignalR) | página estática / só hub | Vitrine visual; isolado em `web/` para não contaminar o backend. |
| Plataforma | .NET 10 (LTS), Clean Architecture | . | Mesma base dos projetos anteriores. |

### Estrutura da solution

```
src/SensorHub.Domain          regras de negócio puras (sem dependências)
src/SensorHub.Application     casos de uso e contratos (portas)
src/SensorHub.Infrastructure  Kafka, Postgres/Timescale, Redis (adaptadores)
src/SensorHub.Api             HTTP + SignalR
src/SensorHub.Worker          consumers (persistência, alertas)
tools/SensorHub.Simulator     gerador de carga
tests/*                       xUnit (unitário) + Testcontainers (integração)
web/dashboard                 React
```

---

## Fase 0: Fundação e domínio

**Objetivo:** modelar o domínio sem nenhuma infraestrutura, com testes desde o início, e ter um gerador de
carga pronto para provar throughput nas fases seguintes.

### D0.1: `Reading` é um value object com identidade natural `(sensorId, timestamp)`

`Reading` é um `readonly record struct` imutável. Validações na criação: `sensorId` não vazio e valor
**finito** (NaN e Infinity são rejeitados, porque envenenam médias e comparações de alerta em silêncio).

**Timestamp normalizado para UTC e truncado para microssegundos.** O `timestamptz` do PostgreSQL tem
precisão de microssegundos; o `DateTimeOffset` do .NET, de 100 ns. Sem o truncamento, duas leituras que
diferem em 100 ns seriam "distintas" na mensagem, mas colidiriam na chave `(sensor_id, ts)` do banco, e a
idempotência (`ON CONFLICT DO NOTHING`) descartaria dados que o resto do pipeline considerou diferentes.
Normalizar na criação faz com que **todas as camadas concordem sobre o que é "a mesma leitura"**.

### D0.2: `Unit` fica no sensor, não na linha do histórico

A unidade (`°C`, `%`) é metadado do sensor. Gravá-la em cada linha da série multiplicaria o armazenamento
por nada. A mensagem HTTP/Kafka carrega `unit` (para validar contra o cadastro e auditar), mas a linha
no banco é só `(sensor_id, ts, value)`.

### D0.3: Faixa fisicamente plausível por métrica

`MetricType.PlausibleRange()` rejeita valores impossíveis (temperatura de 1e30, umidade de 250 %), tipicamente
sensor com defeito. Melhor barrar na borda do que poluir histórico e disparar alertas com lixo.

### D0.4: Regras de alerta são funções puras sobre um estado explícito

`AlertRule.Evaluate(state, reading, now)` recebe o estado, muta-o e devolve a transição
(`None`/`Fired`/`Resolved`). Não conhece Redis, Kafka nem relógio: quem chama passa o "agora". Consequências:

- Todas as regras são testáveis com tempo simulado, sem `Thread.Sleep` (68 testes rodam em < 1 s).
- O estado (`RuleState`) é serializável em JSON e vive no Redis entre leituras (Fase 5).
- Como o Kafka garante ordem por sensor e cada partição tem um único consumer por grupo, o estado
  tem **um único escritor por vez**: não é preciso lock distribuído.

**Máquina de estado:** `Normal → Pending → Firing → Normal`. `Pending` = violando, mas ainda dentro da
duração exigida. É isso que impede "disparar a cada leitura": em `Firing`, leituras ainda violando não
produzem evento nenhum.

**Tempo de evento, não de relógio.** Duração e janelas são medidas com o timestamp da leitura. Reprocessar
um backlog do Kafka (replay) produz o mesmo resultado que o processamento ao vivo. Só a regra `NoData`
usa relógio, porque *ausência* de leituras não gera evento e precisa de uma varredura periódica.

**Idempotência da avaliação:** leituras com timestamp `<=` ao último avaliado são ignoradas
(`RuleState.LastEventTime`). Reentrega do Kafka (at-least-once) e dados fora de ordem não corrompem o estado.
Política de dado atrasado: descartado **para alerta** (continua sendo persistido no histórico).

**Histerese:** para *resolver*, o valor precisa voltar além do limite por uma margem. Sem isso, um sinal
oscilando em torno de 80 °C dispararia/resolveria a cada leitura (flapping).

**Aquecimento da média em janela:** `WindowAverage` só avalia quando a janela cobre (quase) a duração
inteira, para não disparar no primeiro ponto após o start do consumer.

### D0.5: Janela deslizante por baldes (`SlidingWindow`)

Regras de média e de taxa de variação precisam olhar N minutos para trás. Guardar cada leitura custaria
memória proporcional à taxa de leitura. `SlidingWindow` guarda apenas agregados (contagem, soma, min, max,
primeiro/último valor) em ~20 baldes por janela: **memória O(baldes), independente do volume**
(testado com 100 mil pontos). O custo é precisão de borda igual à largura do balde (janela/20, mínimo 1 s),
aceitável para alertas. Esta é a estrutura dos "contadores de janela" que ficarão no Redis.

### D0.6: `Alert` com Id determinístico

O Kafka entrega *pelo menos uma vez*. Se o motor de alertas cair depois de disparar e antes de commitar o
offset, o mesmo disparo é reprocessado. O `Alert.Id` é um hash de `(ruleId, instante do disparo)`: o
segundo `INSERT` colide na chave primária e vira no-op. **Alerta duplicado no banco é impossível por
construção**, sem consulta prévia nem lock.

### D0.7: Simulador determinístico

O simulador (`tools/SensorHub.Simulator`) gera senoide + ruído gaussiano + picos raros, com **seed fixo**:
a mesma execução reproduz a mesma carga, o que torna benchmarks comparáveis. IDs de sensores são derivados
do índice (estáveis entre execuções). Tem controles para exercitar exatamente os cenários difíceis:
`--duplicates` (reenvio idêntico, testa idempotência) e `--hot` (um sensor emite N× mais, testa hot key).
O pacing é por relógio (taxa constante) e, se o destino não acompanha, o relatório mostra a taxa
**alcançada** em vez de mascarar. Teto medido do gerador: ~199 mil leituras/s, então ele não será o
gargalo dos testes de carga.

### Bugs encontrados

Nenhum nesta fase. Os testes de domínio passaram de primeira, o que aqui é menos mérito que sinal de que o
domínio puro é fácil de acertar. Os bugs reais tendem a aparecer na fronteira com a infraestrutura
(fases seguintes).
