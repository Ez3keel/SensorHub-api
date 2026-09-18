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

---

## Fase 1: Ingestão HTTP → Kafka

**Objetivo:** um endpoint que só valida e publica, respondendo em milissegundos, e um log durável (Kafka)
absorvendo os picos. Nenhuma escrita em banco no caminho da requisição.

### D1.1: Por que Kafka é a escolha certa aqui (e RabbitMQ não seria)

A diferença é de **modelo**, não de velocidade:

| | RabbitMQ (fila) | Kafka (log) |
|---|---|---|
| Modelo | Mensagem é **entregue e removida** após o ack | Mensagem é **anexada a um log** e permanece pelo período de retenção |
| Vários leitores da mesma mensagem | Exige duplicar a mensagem em N filas (exchange fanout) | Natural: cada *consumer group* lê o log independentemente, com seu próprio offset |
| Reprocessar (replay) | Impossível; a mensagem já foi consumida | Voltar o offset e reler (bug corrigido, nova regra, novo consumer) |
| Ordem | Por fila, e se perde com múltiplos consumers competindo | Por **partição**, preservada mesmo com N consumers (cada partição tem 1 dono) |
| Escala de leitura | Mais consumers na mesma fila (sem garantia de ordem) | Mais partições e consumers no grupo (com ordem por chave) |
| Natureza do dado | Tarefas pontuais ("envie este e-mail") | Fluxo contínuo de eventos ("temperatura do sensor X agora") |

Telemetria é um **fluxo contínuo de fatos imutáveis** que vários sistemas precisam ler: persistência,
alertas, tempo real. No SensorHub, três consumer groups distintos leem o **mesmo** tópico sem que um
atrapalhe o outro, e um alerta novo pode ser avaliado sobre o histórico retido. Em RabbitMQ seria preciso
fanout para 3 filas (3× o armazenamento e o tráfego) e nada de replay. Para tarefas pontuais (TicketFlow e
QuickOrder) a fila é a ferramenta certa; aqui não.

**Conceitos-chave (e onde aparecem no projeto):**

- **Tópico e partições.** O tópico `sensorhub.readings` é dividido em 6 partições, cada uma um log
  ordenado e independente. Partições são a unidade de **paralelismo** e de **ordenação**.
- **Chave de partição.** A chave da mensagem (`sensorId`) passa por um hash (murmur2) que escolhe a
  partição. Mesma chave, mesma partição, sempre. Por isso todas as leituras de um sensor ficam **em ordem**;
  entre sensores diferentes não há ordem, e não precisa haver.
- **Offset.** A posição de uma mensagem dentro da partição. O consumer guarda "até onde já li"
  (offset commitado) por partição. Commitar **depois** de processar dá *at-least-once*: numa queda, relê-se
  desde o último commit, e a idempotência absorve as repetições (Fase 2).
- **Consumer group.** Consumers com o mesmo `group.id` dividem as partições entre si (cada partição é lida
  por **um** consumer do grupo). Grupos diferentes são independentes. O teto de paralelismo de um grupo é o
  nº de partições. Quando um consumer entra ou sai, o Kafka faz *rebalance*.
- **Retenção.** O log guarda 7 dias independentemente de alguém ter lido. É o que permite replay.

### D1.2: 6 partições, chave `sensorId`

- **6 partições:** teto de 6 consumers por grupo. Definido **acima** do necessário hoje (1 a 2 workers)
  porque *aumentar partições depois redistribui as chaves* (o hash muda o módulo) e quebra a ordem por
  sensor durante a transição. É mais barato sobrar partições agora.
- **Chave = `sensorId`** (decisão de modelagem aprovada): ordem por sensor, distribuição uniforme com
  muitos sensores. **Risco (hot key):** um sensor que emite muito mais que os outros sobrecarrega a sua
  partição, pois uma chave não se divide. Medido na Fase 9 com `--hot`.
- Replication factor 1 porque o ambiente local tem um broker. Em produção seria 3 com `min.insync.replicas=2`,
  o que dá sentido real ao `acks=all`.

### D1.3: Producer com `acks=all`, idempotência e espera do delivery report

- `acks=all`: o líder só confirma depois que todas as réplicas em sincronia gravaram.
- `enable.idempotence=true`: o broker deduplica retries internos do producer (por número de sequência) e
  preserva a ordem mesmo com `max.in.flight=5`. Sem isso, um retry após timeout poderia duplicar ou
  reordenar leituras da mesma partição.
- A API **aguarda o delivery report** de cada mensagem antes de responder 202. Assim, **"202 Accepted"
  significa "está durável no log"**, e não "está na memória da API, talvez". É a troca correta: alguns
  milissegundos de latência por nenhuma perda silenciosa se a API cair logo após responder.
- `linger.ms=5` + `batch.size=128KB` + `lz4`: o producer junta mensagens em lotes de rede. Vazão alta com
  latência de poucos ms. É o que permite ~150 mil leituras/s por instância (ver resultados).

### D1.4: Backpressure explícito, 503 + `Retry-After`

Se o broker não escoa tão rápido quanto chega, algo precisa ceder. Sem decisão explícita, cede a memória
da API (fila infinita) até o OOM. Aqui:

1. A fila local do producer é **limitada** (`QueueBufferingMaxMessages`).
2. `message.timeout.ms` é **curto** (10 s): falhar rápido é melhor que pendurar requisições.
3. Fila cheia, timeout ou broker fora viram `IngestionUnavailableException`, traduzida para
   **HTTP 503 com `Retry-After: 1`**, e não 500.
4. O cliente (o simulador implementa isso) reenvia o **mesmo** lote com backoff.

A pressão é empurrada para quem produz, na borda, de forma controlada. O reenvio de um lote parcialmente
publicado gera duplicatas no log, o que é aceitável porque o consumer é idempotente (`(sensor_id, ts)`).

### D1.5: Aceitação parcial no batch

Um lote de 1000 leituras com 1 inválida **não** deve perder as 999 válidas. O batch devolve `202` com
`{accepted, rejected:[{index, errors}]}` se algo foi aceito, e `422` se **tudo** foi rejeitado (nada a
reenviar sem corrigir). Lote vazio ou acima do máximo é `400`. JSON malformado é `400`, nunca `500`.
Dispositivo sem relógio pode omitir `timestamp`: o servidor usa a hora de ingestão.

### D1.6: Tópicos criados explicitamente, com o nº de partições certo

O auto-create do broker criaria tópicos com **1 partição** e o paralelismo sumiria em silêncio. Um
`KafkaTopicProvisioner` (hosted service) cria os tópicos no startup, é idempotente e tenta por ~60 s
(o broker pode estar subindo no `docker compose`). Também cria o tópico DLQ (Fase 2) e o de alertas (Fase 5).

### D1.7: Contrato da mensagem versionado

`ReadingMessage` tem `schemaVersion` e `ingestedAt`. O contrato é público entre producer e vários
consumers: campos novos entram como opcionais e consumers ignoram campos desconhecidos (testado).
JSON em vez de Avro/Protobuf + Schema Registry: escolha consciente de simplicidade. O custo (mensagens maiores,
sem validação central de schema) é compensado por `lz4` e o `schemaVersion` cobre a evolução neste escopo.
`ingestedAt` permite medir a latência ponta a ponta na Fase 7.

### D1.8: Listeners do Kafka no Docker Compose

O broker devolve nos metadados o endereço **anunciado**; se o cliente não alcança esse endereço, a conexão
falha depois do bootstrap. Por isso há dois listeners: `INTERNAL://kafka:9092` (containers) e
`EXTERNAL://localhost:29092` (host: `dotnet run`, simulador, testes). Modo KRaft (sem ZooKeeper).

### Resultados medidos

| Cenário | Resultado |
|---|---|
| Teste de integração (API em memória, Kafka em container, `acks=all`), 20 mil leituras | ~44 mil leituras/s, 20.000 relidas do tópico, 0 perdidas |
| API real + Kafka do Compose, simulador HTTP, 1.000 sensores, 30 mil/s por 15 s, 1% de duplicatas | 449.768 enviadas, **soma dos offsets das 6 partições = 449.768**, 0 falhas, 0 reenvios |
| API real, 5.000 sensores, meta 150 mil/s por 10 s, 8 workers | **149.667/s sustentadas**, 0 falhas |

Todas as medições no mesmo notebook (simulador, API e Kafka disputando a mesma CPU), então o número
real em hardware dedicado seria maior. A distribuição entre partições foi uniforme (69 mil a 78 mil por
partição no cenário de 449 mil).

### Bugs encontrados

**1. Acentos corrompidos (mojibake) ao editar arquivos pelo PowerShell 5.1.**
*Sintoma:* comentários apareciam como `TÃ³picos`, `partiÃ§Ãµes`. *Causa raiz:* `Get-Content` sem
`-Encoding` no Windows PowerShell 5.1 lê UTF-8 como ANSI (cp1252); ao regravar com `-Encoding utf8`, cada
byte de um caractere multibyte vira dois caracteres (dupla codificação). *Correção:* arquivos reescritos e,
daqui em diante, edições de código feitas por ferramenta de edição direta ou com
`[IO.File]::ReadAllText/WriteAllText` com `UTF8Encoding(false)`. Varredura por `Ã|Â` confirmou que nenhum outro
arquivo foi afetado.

**2. `ObjectDisposedException: handle is destroyed` no shutdown da API.**
*Sintoma:* warning "Falha ao dar flush no producer" ao encerrar a API nos testes. *Causa raiz:* o
`KafkaReadingPublisher` é registrado sob duas chaves de DI (a classe concreta e `IReadingPublisher`, via
factory que devolve a mesma instância). O contêiner rastreia **cada registro** como descartável e chamou
`Dispose()` duas vezes; o segundo `Flush()` rodou num producer já destruído. *Correção:* `Dispose()`
idempotente (`Interlocked.Exchange` numa flag).

**3. Colisão de portas com outros containers da máquina.**
O ambiente já roda a stack do CVAT (Traefik em 8080, Redis, etc.). Por isso o Compose do projeto usa portas
fora do padrão (Kafka UI em 8090, e nas próximas fases Postgres 5433, Redis 6380, Grafana/Prometheus
em portas próprias). Não é bug de código, mas um custo real de "rodar tudo local".