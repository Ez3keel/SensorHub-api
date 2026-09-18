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
---

## Fase 2: Consumer com batching e persistência

**Objetivo:** tirar o trabalho lento (gravar no banco) do caminho da requisição. Um Worker separado lê o
Kafka em lotes e grava no PostgreSQL, sem perder nem duplicar leituras, mesmo com falhas, reentregas e picos.

### D2.1: Consumer *pull* sequencial: o backpressure vem de graça, o lag é o amortecedor

A API aceita ~150 mil leituras/s; um escritor Postgres grava dezenas de milhares/s. Essa diferença é
**esperada** e é exatamente o que o Kafka absorve. Como o consumer *puxa* (só chama `Consume` quando termina o
lote anterior), um banco lento não enche a memória do Worker: o excesso fica no **log durável do Kafka**, e a
sua medida é o **lag** (mensagens do log que o grupo ainda não commitou). Nenhum mecanismo de "pause/resume" é
necessário para o caso normal: basta não puxar mais do que se consegue processar. Testado: com um handler
artificialmente lento, o lag cresce no Kafka e o consumer ainda termina.

### D2.2: Batching por tamanho **ou** tempo

Um lote fecha quando atinge `MaxBatchSize` (5000) **ou** passa `MaxWaitMs` (250 ms) desde a primeira mensagem.
O limite de tamanho protege memória e o custo de reprocessar numa falha; o de tempo limita a latência em
tráfego baixo (sem ele, 3 leituras esperariam para sempre um lote de 5000). Lotes maiores = menos round trips,
mais latência e mais retrabalho quando um lote falha: é um botão de ajuste, não uma verdade.

### D2.3: Commit manual **depois** do processamento (at-least-once)

`EnableAutoCommit=false`: o offset só é commitado após o handler concluir. Consequência aceita e desenhada:
se o Worker cair depois de gravar e antes de commitar, o lote é **reentregue**. Por isso o handler é
idempotente (D2.4). Alternativas descartadas:

- **Commit antes de processar (at-most-once):** uma queda perderia dados silenciosamente. Inaceitável.
- **Exactly-once do Kafka (transações):** cobre Kafka→Kafka. Aqui o destino é o Postgres, fora da transação do
  Kafka. O que se consegue na prática é *effectively-once*: at-least-once + escrita idempotente, que é o que
  fazemos.

### D2.4: Idempotência = chave natural `(sensor_id, ts)` + `ON CONFLICT DO NOTHING`

Duplicatas têm 3 origens: retry do dispositivo após 503, retries internos do producer (cobertos pela
idempotência do producer) e **reprocessamento do consumer**. Uma única defesa na última camada cobre todas: a PK
`(sensor_id, ts)` da tabela. O `INSERT` devolve quantas linhas entraram, então `lote - inseridas = duplicatas`
vira log/métrica. Regra de desempate: **a primeira escrita vence** (testado).

### D2.5: Falha de infraestrutura ≠ falha de dado (classificação)

| Tipo | Exemplo | O que fazer |
|---|---|---|
| **Transitória** (infra) | banco fora, timeout, conexão caiu | **Repetir para sempre**, com backoff exponencial + jitter, sem commitar e sem descartar |
| **Permanente** (dado) | SQLSTATE 22/23 (dado inválido, violação de integridade) | **Isolar** a mensagem culpada e mandá-la à DLQ; o resto do lote segue |

Classificar errado custa caro nos dois sentidos: tratar "banco fora" como "dado ruim" esvaziaria o tópico
inteiro para a DLQ; tratar "dado ruim" como transitório travaria a partição para sempre. Exceção desconhecida
é tratada como transitória (o lag alerta um humano; a alternativa silenciosa é pior).

**Durante um retry longo o consumer precisa continuar "vivo".** O broker expulsa do grupo quem não chama
`Poll` em `max.poll.interval.ms`, provocando rebalance no meio da queda do banco. Solução: **pausar** as
partições e continuar chamando `Consume` (que devolve vazio) enquanto espera o backoff. Ao voltar, `Resume`.

### D2.6: DLQ e isolamento por bissecção

Uma mensagem "venenosa" (JSON inválido, `sensorId` vazio) **nunca** ficará processável, por mais que se repita.
Sem DLQ ela bloquearia a partição inteira. Vai para `sensorhub.readings.dlq` com o payload original **byte a
byte** e headers de contexto (`dlq-reason`, tópico/partição/offset de origem, consumer group). Quando o lote
inteiro falha por dado e não se sabe qual linha é a culpada, o lote é **dividido ao meio recursivamente**
(O(log n) rodadas): a metade que passa é gravada, a que falha continua dividindo até sobrar a mensagem única.

### D2.7: Rebalance cooperativo e descarte de partições revogadas

`CooperativeSticky`: ao escalar (entra/sai consumer) só as partições que mudam de dono param; no modo *eager*
(padrão) o grupo inteiro pararia. Cuidado sutil: mensagens já lidas de uma partição **revogada** enquanto o lote
era acumulado são descartadas (o novo dono as relê do offset commitado); e o commit nunca inclui partições
que já não são nossas. Se a partição *volta* para nós, sai do conjunto de revogadas.

### D2.8: Bulk insert: `unnest` vs `COPY` (medido)

Duas estratégias implementadas, ambas idempotentes, comparadas com lotes de 5000 (100 mil linhas cada, 2 rodadas):

| Estratégia | Rodada 1 | Rodada 2 |
|---|---|---|
| `COPY` binário para tabela temporária + `INSERT ... ON CONFLICT` | 35.661 linhas/s | 33.138 linhas/s |
| `INSERT ... SELECT FROM unnest(arrays) ON CONFLICT` | **37.480 linhas/s** | **36.473 linhas/s** |

`unnest` ficou consistentemente ~5–10% à frente e é mais simples (uma instrução, sem criar/dropar tabela
temporária por lote). Adotado como padrão; `COPY` continua selecionável (`Persistence:Strategy`). A diferença
é pequena: o gargalo não é o protocolo de transferência, é a **manutenção do índice PK + fsync** (ver Fase 3).
Os lotes são **ordenados por `(sensor_id, ts)`** antes de gravar: melhora a localidade no índice e evita
deadlock entre escritores concorrentes com chaves sobrepostas (testado com 6 escritores).

### D2.9: EF Core só para o esquema; o caminho quente é Npgsql cru

O `SensorHubDbContext` existe para **migrations** (e, nas próximas fases, para as entidades relacionais:
sensores, regras, alertas, usuários). Leituras NÃO passam pelo EF: o change tracker custaria ordens de
grandeza de vazão em inserts em massa. A tabela `readings` é criada por SQL explícito numa migration.
O Worker aplica as migrations ao subir (o EF 9+ protege a execução concorrente com advisory lock).

### Resultados medidos

| Cenário | Resultado |
|---|---|
| **Backlog real** de 1.947.748 mensagens (sobras dos testes da Fase 1) drenado pelo Worker recém-iniciado | Lag **0** nas 6 partições. 1.943.318 linhas no banco: a diferença de **4.430 são as duplicatas** do 1% que o simulador injetou. Idempotência validada em dado real. |
| Carga de 20 mil/s por 15 s + **segundo Worker entrando no grupo no meio** (rebalance) | 299.733 enviadas → +299.733 linhas. **Exato.** |
| Carga de 25 mil/s por 14 s + **`kill -9` do Worker no meio**, reiniciado 4 s depois | 348.801 enviadas → +348.801 linhas. **Diferença = 0.** |
| Vazão de gravação (1 escritor, Postgres comum em Docker/Windows) | ~35 mil linhas/s no benchmark isolado; ~20 mil/s com API, simulador e banco disputando a mesma CPU |

Sobre o teste de `kill -9`: neste caso o processo caiu **entre** dois commits, então nenhum lote precisou ser
reprocessado (o log não mostrou "duplicatas ignoradas"). A janela exata "gravou mas não commitou" é coberta de
forma **determinística** por um teste de integração (`Crash_after_write_but_before_commit_...`), onde o handler
grava de verdade e falha uma vez antes do commit; o resultado é 2000 linhas, não 4000.

**Achado importante:** a ingestão (~150 mil/s) é 4 a 7 vezes mais rápida que a gravação num Postgres comum
(~20–35 mil/s). Isso é o amortecedor funcionando (o excesso vira lag e drena depois), mas também aponta o próximo
gargalo: a Fase 3 (TimescaleDB) e a escala horizontal (até 6 consumers, 1 por partição) atacam isso.

### Bugs encontrados

**1. Descarte de mensagens legítimas após rebalance (perda de dados em potencial). Achado em revisão, antes de rodar.**
*Sintoma previsto:* após uma partição ser revogada e depois **devolvida ao mesmo consumer**, mensagens válidas
dela eram descartadas do lote e, como o commit avança pelo offset da última mensagem processada, ficariam para
trás sem nunca serem relidas. *Causa raiz:* o conjunto `_revoked` só recebia entradas (no handler de revogação)
e só era limpo quando um lote era filtrado; nada o atualizava quando a partição era atribuída de novo.
*Correção:* o handler de atribuição remove as partições recebidas do conjunto. Cuidado análogo já existia no
`Commit`, que ignora partições revogadas durante o processamento.

**2. Conflito de versões do EF Core (`CS1705` / `MSB3277`).**
*Sintoma:* o projeto de testes não compilava: `Microsoft.EntityFrameworkCore` 10.0.12 (exigido pelo `Design`) vs
10.0.4 (exigido pelo provider `Npgsql.EntityFrameworkCore.PostgreSQL 10.0.3`). *Causa raiz:* cada projeto do
grafo resolve as versões transitivas por caminhos diferentes (o `Design` é `PrivateAssets` no Worker e não
"flui" até os testes), então o projeto de testes acabava com o EF antigo. *Correção:* fixar
`Microsoft.EntityFrameworkCore` e `.Relational` em 10.0.12 diretamente na Infrastructure.

**3. `taskkill` falhou silenciosamente no Git Bash (erro do experimento, não do código).**
*Sintoma:* o "kill -9 do Worker" não matou nada e subi um 2º Worker, transformando o teste num teste de
rebalance acidental. *Causa raiz:* o MSYS do Git Bash reescreve argumentos que começam com `/`
(`/F` virou caminho). *Correção:* refazer o experimento com `Stop-Process -Force` no PowerShell. Lição:
verificar que a fonte de falha injetada realmente aconteceu (aqui o rebalance acidental também deu 0 de
diferença, e o kill real foi repetido e medido).

---

## Fase 3: TimescaleDB, séries temporais e ciclo de vida do dado

**Objetivo:** guardar bilhões de leituras de forma barata, consultá-las por janela de tempo em milissegundos e
descartar o que envelheceu sem esforço manual.

### D3.1: TimescaleDB vs Postgres puro vs particionamento nativo (medido, com ressalvas honestas)

Mesmo esquema, mesma carga (3.000.000 linhas, 500 sensores, 50 h de dados), mesmas consultas e mesmo caminho de
escrita (`unnest` + `ON CONFLICT DO NOTHING`), em três variantes: tabela comum, particionamento declarativo
nativo por hora e hypertable com chunks de 1 h. Mediana de 15 execuções.

| | Postgres puro | Part. nativo (1 h) | TimescaleDB |
|---|---|---|---|
| **Ingestão** (linhas/s) | 31.0 mil | 31.2 mil | 31.6 mil |
| Q1 1 sensor, última hora (bruto) | 1,6 ms | 1,3 ms | 2,0 ms |
| Q2 1 sensor, 50 h, média/min/max por hora | 11,4 ms | 12,1 ms | 16,2 ms |
| **Q3 todos os sensores, última hora** | 64,7 ms | 17,1 ms | **15,0 ms** |
| Q4 varredura total (3 M linhas) | 81,7 ms | 96,9 ms | 96,8 ms |
| **Q2 via agregado contínuo** | n/a | n/a | **2,0 ms** |
| Armazenamento (tabela + índices) | 379 MB | 318 MB | 365 MB |
| **Armazenamento comprimido** | n/a | n/a | **55 MB (6,7x menor)** |
| Q1 / Q2 com dado comprimido | n/a | n/a | 1,7 ms / 49,6 ms |
| **Retenção: apagar 24 h (1,56 M linhas)** | 695 ms (`DELETE`) | 209 ms (24 `DROP TABLE`) | **116 ms** (`drop_chunks`) |

**Leitura honesta dos números:**

- **O TimescaleDB NÃO ganha em ingestão nem em consulta de um sensor.** Ingestão empata nos 3 (~31 mil/s): o limite
  é fsync + manutenção do índice PK no Docker/Windows, com o conjunto inteiro cabendo na RAM. Q1/Q2 ficam um pouco
  piores (2,0 vs 1,6 ms) porque o planner considera ~50 chunks. Com 3 M de linhas, tudo cabe em memória; a vantagem
  de chunks em ingestão aparece quando o índice **não cabe mais na RAM**, cenário que este teste não alcança.
- **Onde ganha:** (1) **poda de chunks** em consultas por janela de tempo entre sensores (Q3: 4x mais rápido que a
  tabela comum); (2) **agregados contínuos**: a mesma resposta de Q2 em 2 ms em vez de 11 a 16 ms, e a diferença
  cresce com o volume porque o agregado tem tamanho fixo por bucket; (3) **compressão 6,7x**; (4) **retenção** sem
  `DELETE` (que deixa bloat e exige VACUUM): `drop_chunks` remove arquivos inteiros.
- **Contra o particionamento nativo a vantagem é operacional**, não de velocidade bruta: o Timescale cria chunks
  sozinho (nativo exige criar partições antes, via cron/pg_partman), e traz compressão, agregados contínuos e
  políticas prontas. Para quem já opera Postgres com pg_partman, o nativo é uma escolha defensável.
- **Compressão custa consulta analítica:** Q2 sobre chunks comprimidos ficou 3x mais lenta (49,6 vs 16,2 ms), pois
  descomprime segmentos. Por isso só se comprime dado **frio** (após 7 dias); o quente permanece em linha.

**Decisão:** TimescaleDB, pelo conjunto (poda + agregados + compressão + retenção + automação), e não por
ingestão. É a resposta correta à pergunta "vale a pena?" para uma plataforma de telemetria: **sim, mas pelos
motivos certos**.

### D3.2: Chunks de 1 dia

Um chunk (dados + índice PK) deve caber em ~25% da RAM para que o índice quente fique em memória. A 100 mil
leituras/s o dia tem ~8,6 bi de linhas; o intervalo deve ser revisto com o volume real (o benchmark usou 1 h para
ter chunks suficientes em poucas horas de dado).

### D3.3: Ciclo de vida em camadas

| Camada | Guarda | Política |
|---|---|---|
| Bruto (`readings`) | leitura a leitura | columnstore após 7 dias; **retenção de 90 dias** |
| `readings_1m` | 1 balde por sensor por minuto | retenção de 1 ano |
| `readings_1h` | 1 balde por sensor por hora (calculado a partir do de 1 min) | retenção de 5 anos |

Quanto mais grosso o dado, mais tempo se guarda: é barato guardar 1 linha por hora por 5 anos, e proibitivo
guardar 1 linha por segundo pelo mesmo tempo. Columnstore segmentado por `sensor_id` e ordenado por `ts DESC`: uma
consulta "histórico de UM sensor" lê só os segmentos dele, já ordenados. `ON CONFLICT DO NOTHING` continua
deduplicando (e aceitando dado atrasado) **em chunks já comprimidos** (testado).

### D3.4: Agregado contínuo guarda SOMA e CONTAGEM, nunca a média

A média de médias é errada quando as contagens diferem. Exemplo testado: minuto A com 1 leitura de 10 e minuto B com 3
leituras de 20. Média das médias = 15 (errado); média real = (10 + 60) / 4 = **17,5**. Guardando `sum` e `count`,
o agregado horário é `sum(sum)/sum(count)`, e qualquer bucket (5 min, 6 h, 1 dia) é recomposto sem perda a partir
do de 1 min. O agregado de 1 h é **hierárquico** (lê o de 1 min, nunca o bruto).

### D3.5: Agregação em tempo real e a "marca d'água" (achado do teste)

`materialized_only = false`: a consulta une o dado materializado com o ainda não materializado, então o último minuto
aparece sem esperar o job. **Limite descoberto:** o "ainda não materializado" só cobre dado MAIS NOVO que a marca
d'água do agregado. Uma leitura **atrasada** cujo balde já ficou atrás da marca não aparece na série até o próximo
refresh (política de 30 s). É consistência eventual de ≤ 30 s para dado atrasado; aceitável (o dado bruto está
consultável na hora) e agora coberto por teste. A janela de refresh é de 7 dias (a idade máxima aceita pela
ingestão): o custo do refresh é proporcional ao que foi **invalidado**, não ao tamanho da janela.

### D3.6: API de séries protege o banco

`GET /api/sensors/{id}/series` valida antes de consultar: buckets permitidos (1m, 5m, 15m, 30m, 1h, 6h, 1d; múltiplos
de 1 min, a granularidade do agregado mais fino), no máximo **5.000 pontos** por resposta, intervalo de até 366 dias,
`bucket=auto` (menor bucket que mantém ~500 pontos, ideal para gráfico). Sem esses limites, um
`from=2020&bucket=1m` devolveria milhões de linhas com uma única requisição. Séries leem **só dos agregados**.
Buckets diários/horários alinham ao UTC. `GET /api/sensors/{id}/readings` devolve o bruto (limite 10 mil).

### D3.7: Migration sobre dado existente, fora de transação

`create_hypertable(..., migrate_data => true)` converteu a tabela da Fase 2 **com 2.591.852 linhas** em ~18 s (incluindo
build), sem perder nenhuma (contagem idêntica antes e depois). Agregados contínuos não podem ser criados dentro de
transação, então as instruções usam `suppressTransaction` (o EF avisa que a migration não é atômica). Todas são
idempotentes (`IF NOT EXISTS`, `if_not_exists => true`), então uma migration interrompida pode ser reexecutada. O
`Down` remove agregados e políticas, mas **não** desfaz a hypertable (isso exigiria copiar os dados).

### Bugs encontrados

**1. Número formatado com vírgula na mensagem de erro da API.**
*Sintoma:* teste esperava `5.000` e a mensagem dizia `5,000`. *Causa raiz:* `{valor:N0}` depende da cultura, e o
projeto usa `InvariantGlobalization`. *Correção:* mensagens de API não formatam número com cultura; usam o inteiro
puro (`5000`).

**2. Testes falhando só na suíte completa: agregação em tempo real e a marca d'água.**
*Sintoma:* 2 testes passavam isolados e falhavam na suíte inteira. *Causa raiz:* outros testes executam
`refresh_continuous_aggregate(NULL, NULL)`, que empurra a marca d'água até perto de "agora"; os dados de teste
(recentes) ficavam **atrás** dela e não entravam na agregação em tempo real (ver D3.5). Não era instabilidade do
banco: era comportamento real do Timescale. *Correção:* o teste de tempo real usa timestamps no futuro (sempre acima
da marca), o E2E dispara o refresh que a política faria, e um teste novo documenta o limite.

**3. Asserção fraca sobre contagem de mensagens consumidas (design de teste).**
*Sintoma:* `Duplicated_messages_in_the_log_are_stored_once` estourou 60 s uma vez, e passou isolado. *Causa raiz:*
esperava `Consumed == 2000`, mas com at-least-once um rebalance pode fazer o consumer reler mensagens e o contador
passar de 2000. A garantia real do sistema é o número de linhas **no banco**. *Correção:* espera `>=` e a asserção
de verdade fica no banco.

**4. `show_chunks(newer_than)` excluía o chunk que começa antes do horário dado (erro do teste).**
Quem começa às 00:00 não é "mais novo que" 09:00. O teste passou a localizar o chunk que **contém** cada leitura.

---

## Fase 4: Redis, o estado quente (último valor)

**Objetivo:** responder "qual o valor deste sensor **agora**?" em tempo constante, sem consultar o histórico.

### D4.1: Último valor em Redis, e Redis é estado DERIVADO

O histórico no banco é a fonte da verdade; o Redis guarda só o "agora" e pode ser perdido e reconstruído (por
reprocessamento do tópico ou por cache-aside na leitura). Isso muda o que se exige dele: sem replicação sofisticada, com
AOF `everysec` só para não reconstruir a cada restart.

Estrutura: um **HASH** por sensor (`sensorhub:last:{id}` com `ts` e `value`), TTL de 7 dias renovado a cada escrita
(sensor aposentado não deixa lixo eterno; expirar não é problema, pois a leitura faz cache-aside).

### D4.2: Escrita "só se for mais novo", atômica, em Lua

Duas instâncias do consumer, uma reentrega ou um dado atrasado poderiam sobrescrever um valor novo por um antigo.
A escrita é um script Lua: `se ts_novo > ts_armazenado então grava`. O Redis executa scripts de forma **atômica**
(comparar e gravar não é interrompido), então não há lock nem read-modify-write no cliente. Testado com 8 escritores
concorrentes gravando 200 timestamps em ordens embaralhadas: sempre converge para o mais novo. O timestamp vai como
**microssegundos inteiros** (exato em `double` até 2^53), o mesmo grão do banco, e o valor como `"R"` (ida e volta exata).

### D4.3: Consumer group próprio (`sensorhub.lastvalue`), não dentro do de persistência

Poderia ser um passo extra do handler de persistência. Foi separado porque cada grupo tem **falha, lag e offset
independentes**: uma queda do Redis não trava a gravação no banco (o teste injeta falha de Redis e nada vai à DLQ; o
lote é repetido até voltar), e um banco lento não deixa o "agora" defasado. O custo é ler o tópico mais uma vez, o que
no Kafka é barato (leitura sequencial do log). O Worker escolhe quais papéis executa (`Worker:Consumers`), então em
produção cada papel escala isoladamente.

### D4.4: Reduzir o lote ao valor mais novo por sensor + pipeline

5.000 leituras de 500 sensores viram **500** escritas, e todas vão num único *round trip* (pipeline). O mais novo é
escolhido pelo **timestamp**, não pela posição no lote (o lote pode ter dado fora de ordem).

### D4.5: Cache-aside na leitura

`GET /api/sensors/{id}/latest`: tenta o Redis (O(1)); se não há (expirou, Redis reiniciou, sensor novo), lê a última
leitura dos últimos 7 dias no banco e **repovoa** o cache. Sensor sem leitura recente é 404 (sem cache negativo: evita
esconder um sensor que voltou). `GET /api/sensors/latest?ids=a,b,c` (até 200) devolve vários numa chamada (o dashboard
inicial), com `ageSeconds` para o cliente decidir se o valor está velho sem comparar relógios.

### D4.6: Redis fora do ar não derruba o processo

`AbortOnConnectFail=false`: API e Worker sobem mesmo sem Redis e reconectam sozinhos. O `/health/ready` reporta
indisponível (testado), mas o `/health/live` continua ok, e a ingestão (que só depende do Kafka) segue funcionando.

### Resultados medidos

| Cenário | Resultado |
|---|---|
| Grupo `lastvalue` drenando ~1,6 milhão de mensagens de backlog (Worker recém-iniciado) | lag 0 em ~25 s: **~60 a 100 mil msgs/s por consumer**, 2 a 3x a vazão de gravação no banco |
| Valor no Redis vs. última linha do banco (sensor amostrado) | **idênticos**: mesmo timestamp em µs (`1789772077888259`) e mesmo valor |
| Cobertura | 5.000 chaves para os 5.000 sensores do simulador; TTL 604.768 s (~7 dias) |
| Recuperação após perder a chave (`DEL`, como se o Redis tivesse reiniciado) | a API devolve o valor correto via banco e repovoa o Redis (testado) |

### Bugs encontrados

**1. Chamada ambígua `long.Parse(RedisValue)` (`CS0121`).**
`RedisValue` converte implicitamente para `string` **e** para `byte[]`, então o compilador não escolhe entre
`Parse(string)` e `Parse(ReadOnlySpan<byte>)`. Correção: conversão explícita `(string)campo`. Detalhe de API, mas ilustra
por que o valor sai do Redis como texto e é interpretado explicitamente (ida e volta exata de `double`).
