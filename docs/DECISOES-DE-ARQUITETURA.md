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
fora do padrão (Kafka UI em 8092, e nas próximas fases Postgres 5433, Redis 6380, Grafana/Prometheus
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

---

## Fase 5: Alertas em stream

**Objetivo:** avaliar regras sobre o fluxo de leituras, sem disparar alerta duplicado a cada leitura, sem perder alerta em
falha e sem falso positivo quando o sistema reprocessa histórico.

### D5.1: O motor é um terceiro consumer group, lendo o mesmo tópico

`sensorhub.alerts` lê `sensorhub.readings` com offsets e lag próprios, ao lado de `persistence` e `lastvalue`. Isso é
exatamente o que uma fila tradicional não faz sem duplicar mensagens: um motor novo (ou uma regra nova) pode ser
avaliado sobre o histórico retido apenas com um `group.id` novo. Testado: um segundo grupo relê o mesmo fluxo e não gera
nenhum alerta a mais.

### D5.2: Ordem das escritas: persistir, publicar, salvar o estado (por último)

```
lote de leituras -> avalia regras (funções puras do domínio) -> transições
   1. INSERT do alerta no Postgres   (idempotente)
   2. publica o evento no Kafka      (confirmado pelo broker)
   3. salva o novo estado no Redis   (POR ÚLTIMO)
```

Uma queda em qualquer ponto reentrega o lote com o estado **antigo** ainda no Redis. A reavaliação produz as mesmas
transições nos mesmos instantes: o INSERT vira no-op e o evento é republicado (duplicata inofensiva). Se o estado fosse
salvo antes, uma queda entre "salvar estado" e "publicar" **perderia o alerta para sempre** (o estado diria "já
disparou"). Cada ponto de queda tem teste: depois de persistir e publicar mas antes do estado, e depois de persistir
mas antes de publicar; nos dois casos o resultado é 1 alerta e o evento sai na reentrega.

### D5.3: Quatro camadas contra alerta duplicado

| Camada | Cobre | Como |
|---|---|---|
| Máquina de estado (`Pending`/`Firing`) | 1000 leituras acima do limite | Em `Firing`, leitura violando não gera evento algum |
| **Id determinístico** do alerta | Reprocessamento do mesmo disparo | `hash(regra, instante)`: o `INSERT` colide na PK e vira no-op |
| **Índice único parcial** no banco | Estado do Redis perdido (flush/failover) | `UNIQUE(rule_id) WHERE status <> 'Resolved'`: no máximo 1 alerta **aberto** por regra, garantido pelo banco |
| Guarda de tempo de evento no estado | Reentrega e dado fora de ordem | Leitura com timestamp `<=` ao último avaliado é ignorada |

Testado: 12 disparos concorrentes da mesma regra resultam em exatamente 1 alerta; com o Redis zerado, a regra "dispara de
novo" e o índice barra. O **evento** é o único ponto at-least-once; quem consome deduplica por `(AlertId, Kind)`.

### D5.4: Estado das regras no Redis, sem lock

O estado (máquina de estado + janela deslizante por baldes, os "contadores de janela") vive em JSON no Redis, uma chave por
regra. Não há lock distribuído: o Kafka entrega as leituras de um sensor em ordem a **um único** consumer do grupo, então
cada regra tem um único escritor por vez. Ler (um `MGET`) e escrever (pipeline) o estado de N regras custa 1 round trip cada.
Sensor sem regra custa ~zero: o lote é agrupado por sensor e quem não tem regra é descartado sem tocar o Redis (testado).

### D5.5: Catálogo de regras em cache (consistência eventual de 10 s)

O motor consulta as regras a cada lote; ir ao banco a cada lote seria o gargalo. O `CachedRuleCatalog` mantém um
snapshot em memória por `RuleCacheSeconds` (10 s). Preço: uma regra criada ou desabilitada leva até 10 s para valer.
Desabilitar apaga o estado (ao reabilitar, a regra recomeça limpa).

### D5.6: "Sem dados": a varredura é por relógio, e o relógio é perigoso

Ausência de leitura não gera evento, então um `NoDataSweeper` pergunta a cada 15 s "há quanto tempo este sensor está mudo?".
Cuidados, cada um nascido de um cenário real:

1. **Só a varredura dispara; uma leitura só resolve.** Se uma leitura pudesse disparar, reprocessar um backlog antigo faria
   um sensor saudável parecer mudo (a leitura tem horas de idade em relação ao relógio). Bug real, ver abaixo.
2. **Instantes determinísticos.** O disparo usa `última leitura + duração` e a resolução usa o timestamp do dado que voltou,
   nunca o "agora". Com `now`, cada replay geraria outro Id e outro alerta (D5.3).
3. **Não declara silêncio enquanto o motor está atrasado.** Se o consumer está 10 minutos atrás no fluxo, "o sensor está
   calado" pode ser só efeito do atraso. O motor mede o atraso (agora menos quando a API ingeriu o dado mais novo do lote) e a
   varredura se abstém acima de 30 s. Com o fluxo ocioso não há atraso a esconder, e o silêncio é real (testado).
4. **A última leitura vem do estado da própria regra**, não do grupo `lastvalue`: assim o "sem dados" não depende de outro
   consumer estar em dia.
5. **Um lease no Redis** (`SET NX PX`, liberação por token via Lua) garante que só UMA instância varre por vez. O lease
   expira sozinho se o dono morrer, e o dono antigo nunca solta o lock do novo (testado).

### D5.7: Eventos de alerta

Tópico `sensorhub.alerts`, chave `sensorId` (eventos de um sensor em ordem: um `Resolved` nunca chega antes do `Fired`),
producer idempotente com `acks=all`, corpo versionado e header `event-kind`.

### D5.8: API de administração: Repository + Unit of Work, sem regra de negócio no controller

`SensorService`, `AlertRuleService` e `AlertService` na Application; repositórios e `IUnitOfWork` (o `DbContext` do escopo) na
Infrastructure. Regras de domínio violadas viram **400**; recurso inexistente **404**; conflito de estado (reconhecer alerta já
reconhecido ou resolvido, id de sensor duplicado) **409**. Enums trafegam e são gravados como **texto**. O cadastro de sensor
aceita um `id` opcional para preservar a identidade de uma frota existente (o simulador depende disso: as leituras já
publicadas apontam para esses ids). Alertas não têm chave estrangeira para a regra: o histórico sobrevive à exclusão dela.

### Resultados medidos (ambiente real: API + Worker com os 3 papéis, Kafka, Timescale e Redis em containers)

| Cenário | Resultado |
|---|---|
| Motor recém-criado reprocessando ~3 milhões de leituras históricas | lag 0 em ~15 s; **27 alertas** (picos reais do simulador) e **0 alertas "sem dados" falsos** |
| 300 sensores cadastrados com 2 regras cada (600 regras); 10% deles morrem aos 15 s | **exatamente 30** alertas "sem dados", ~10 s depois da morte |
| Fim da carga (todos os sensores ficam mudos) | os outros 270 disparam pela mesma varredura; total 300 abertos |
| Consistência entre banco e Kafka | 347 alertas + 47 resoluções = **394 eventos**; soma dos offsets de `sensorhub.alerts` = **394** |
| Duplicatas | **0** regras com mais de 1 alerta aberto |
| Lag dos grupos `persistence`, `lastvalue`, `alerts` | **0** nos três |

### Bugs encontrados

**1. Falso alerta "sem dados" ao reprocessar backlog (achado no desenho do motor, antes de existir o replay).**
*Cenário:* o consumer fica parado 1 h e volta; lê leituras de 1 h atrás. *Causa raiz:* a regra `NoData` avaliava
`agora - timestamp_da_leitura >= duração` também no caminho de leitura, então TODA leitura antiga parecia "silêncio" e
disparava. *Correção:* no caminho de leitura, `NoData` só pode **resolver**; quem dispara é a varredura por relógio. Coberto
por teste de domínio e por teste do motor, e confirmado no replay real (0 falsos em ~3 milhões de leituras).

**2. Instante de disparo do "sem dados" dependia do relógio (achado na revisão do at-least-once).**
*Causa raiz:* o alerta era criado no instante `now` da varredura; após uma queda, a reavaliação usava outro `now`, gerando outro
`Id` determinístico e portanto **outro alerta**. *Correção:* o instante passou a ser um fato observável (`última leitura +
duração` no disparo; timestamp do dado que voltou na resolução). Teste: duas varreduras em momentos diferentes veem o mesmo
instante e o mesmo Id.

**3. `HTTP 500` em toda chamada de regra (achado pelos testes de integração).**
*Causa raiz:* `AlertRuleService` depende de `IRuleStateStore` (para apagar o estado ao desabilitar/excluir a regra), mas ele só
estava registrado no host do Worker; o contêiner de DI da API não conseguia construir o serviço. *Correção:* o estado das
regras é registrado junto com o Redis (`AddRedisState`), que a API também usa.

**4. Teste "retoma do offset commitado" instável (design de teste).**
Parava o consumer assim que o handler terminava, **antes** do commit (que vem depois). Isso é permitido em at-least-once (o lote
seria reentregue), mas invalida a asserção "não relê nada". O teste passou a esperar o lag chegar a 0.

---

## Fase 6: Tempo real (SignalR + backplane Redis) e o dashboard React

**Objetivo:** mostrar o "agora" de cada sensor e os alertas ao vivo num dashboard, sem que a taxa de ingestão vaze para a
rede, e com a API escalável horizontalmente.

### D6.1: O tempo real é MAIS um consumer group (`sensorhub.realtime`), dentro da API

```
Kafka readings ──► [grupo sensorhub.realtime] ──► coalescer ──(flush a cada 250 ms)──► hub SignalR ──► navegadores
Kafka alerts   ──► [grupo sensorhub.realtime.alerts] ─────────────────────────────────► hub SignalR ──► navegadores
                     ▲ todas as instâncias da API no MESMO grupo: cada partição é lida por UMA delas
                                         Redis backplane: o push de uma instância chega aos clientes de TODAS
```

Duas propriedades trabalham juntas: o **consumer group** reparte a leitura do stream entre as instâncias (nenhuma leitura é
processada duas vezes) e o **backplane Redis** faz o push chegar ao cliente onde quer que ele esteja conectado. Sem o
backplane, um cliente ligado à instância A nunca veria as leituras consumidas pela B. Testado com duas instâncias reais da API
no mesmo grupo: um cliente conectado só na A recebeu as 40 leituras de 40 sensores, incluindo as consumidas pela B. O grupo
começa do fim do log (`Latest`): um dashboard novo mostra o "agora", não reprocessa o histórico.

### D6.2: Coalescing, ou "a taxa de saída independe da taxa de entrada"

O handler do consumer não empurra nada: só deposita a leitura num buffer "último valor por sensor" (`ReadingCoalescer`, uma
escrita em memória por leitura). Um timer drena o buffer a cada 250 ms e envia o resultado. Consequências:

- 150 mil leituras/s na entrada continuam sendo, **no máximo, 4 atualizações/s por sensor** na saída.
- O buffer só aceita valor MAIS NOVO por timestamp: dado atrasado ou reentregue nunca faz o dashboard voltar no tempo.
- Concorrência sem lock: o drain troca o dicionário de forma atômica (`Interlocked.Exchange`); escritas simultâneas caem no
  próximo ciclo, e um teste com 20 escritores concorrentes e um leitor drenando garante que o último valor de cada sensor
  sempre chega.

Testado de ponta a ponta: uma rajada de **3.000 leituras do mesmo sensor** chegou ao cliente em **menos de 100 mensagens**,
com o último valor (2999) sempre entregue.

### D6.3: Duas granularidades de assinatura, e a segunda só custa quando há quem assista

1. **`group:{nome}`**: uma mensagem por grupo de sensores a cada flush, com todas as leituras coalescidas do grupo. A rede vê
   dezenas de mensagens por segundo, não milhares. Alimenta a grade de cartões.
2. **`sensor:{id}`**: leituras de UM sensor, para o gráfico de detalhe. Só é alimentado enquanto **alguém assiste** esse sensor.
   Como quem consome a partição do sensor pode não ser a instância onde o navegador está conectado, a contagem de assinantes
   vive no **Redis** (HASH sensorId → contagem, ajustado atomicamente por Lua, campo removido ao chegar a zero) e o fanout
   consulta essa lista (cache de 1 s). Sem isso, seria preciso publicar milhares de mensagens no backplane para grupos vazios.

Limitação assumida e documentada: se uma instância morre sem desconectar seus clientes, as contagens dela vazam. O efeito é só
empurrar mensagens por sensor para um grupo vazio, e a chave inteira expira sozinha após 1 h sem escritas.

O hub também impõe limites por conexão (50 sensores, 10 grupos, nome de grupo de até 100 caracteres): um cliente não pode
assinar o sistema inteiro.

### D6.4: Alertas: imediatos, deduplicados e best-effort por desenho

Eventos de alerta são poucos: sem coalescing, empurrados na hora para o grupo `alerts` e para `sensor:{id}`. O motor é
at-least-once (pode republicar após uma queda), então o listener descarta o que já viu por `(AlertId, Kind)` (LRU de 10 mil).
O listener usa auto-commit e `Latest`: **é uma visão viva, não uma fila de entrega**. Se uma instância cai, o dashboard recarrega
os alertas abertos pela API REST (o Postgres é quem guarda a verdade). Confiabilidade forte aqui seria custo sem benefício.

### D6.5: No SignalR os grupos pertencem à conexão

Uma queda de rede perde silenciosamente TODAS as assinaturas. O `TelemetryClient` guarda o que o usuário assinou e **refaz** as
assinaturas a cada reconexão automática (backoff 0/1/2/5/10/30 s).

### D6.6: Front-end (React + TypeScript estrito, Vite, Recharts)

- **Lógica de estado fora do React** (`telemetryState.ts`), pura e coberta por testes (vitest): aplicar leituras ignora dado
  antigo, histórico limitado a 60 pontos por sensor (memória não cresce com o tempo), semeadura pelo REST nunca sobrescreve um
  valor ao vivo mais novo, evento de alerta é idempotente e retorna a MESMA referência quando é duplicata (nenhuma
  re-renderização), ordenação por severidade e depois recência.
- **Um lote por quadro de animação**: as mensagens são acumuladas num buffer e aplicadas ao estado do React no máximo uma vez
  por `requestAnimationFrame`.
- **Cartão "apagado"** quando não há leitura há mais de 30 s (o sensor pode estar offline); cartão com borda vermelha quando há
  alerta aberto.
- **Detalhe:** últimos 5 min (REST bruto + push ao vivo, mesclados por timestamp) ou séries agregadas de 1 h a 7 d
  (média/mín/máx dos agregados contínuos). O período longo usa `bucket=auto` do servidor.
- **CORS** com origens explícitas e credenciais (o SignalR com WebSocket não aceita "qualquer origem" com credenciais); em dev
  o Vite faz proxy de `/api` e `/hubs` e o navegador enxerga uma origem só.

### Resultados

| Verificação | Resultado |
|---|---|
| Testes de integração do tempo real (SignalR real, Kafka real, Redis real) | 10, todos passando, incluindo o de **2 instâncias + backplane** |
| Coalescing | 3.000 leituras de um sensor → < 100 mensagens; valor final sempre entregue |
| Dashboard em execução (API + Worker + simulador 1.500 leituras/s, 300 sensores, 5% dos sensores morrendo aos 90 s) | conectado "ao vivo", cartões e sparklines atualizando, toasts de alertas críticos, **16 alertas abertos** (críticos em vermelho, "Sensor offline" em amarelo) |
| Front-end | TypeScript estrito compila; 11 testes vitest; bundle de 607 kB (172 kB gzip) |

### Bugs encontrados

**1. Toasts duplicados e alertas em dobro (achado ao olhar o dashboard rodando, não pelos testes).**
*Sintoma:* o mesmo alerta aparecia 2 a 3 vezes e havia mais de 4 toasts empilhados. *Causa raiz:* o React StrictMode (dev) monta
o efeito duas vezes; o primeiro cliente SignalR era parado durante a negociação, mas o `start()` reagendava uma nova tentativa
**depois de parado**, ressuscitando um cliente "zumbi" que alimentava o mesmo estado. *Correção:* um cliente parado nunca
reconecta e fica mudo (não emite mais status nem eventos, para não piscar o badge de "desconectado" por cima do cliente novo);
toasts deduplicados por alerta. O erro `The connection was stopped during negotiation` no console em dev é exatamente esse
primeiro cliente sendo parado e é esperado.

**2. Asserção frágil sobre a atribuição de partições (design de teste).**
O teste das duas instâncias conferia `Members[].Assignment` da Admin API, que vem vazia com rebalance cooperativo. A garantia real
(2 membros no grupo e o cliente recebendo as leituras das DUAS instâncias) já estava coberta; a asserção frágil foi removida.

---

## Fase 7: Observabilidade (métricas, lag do consumer e tracing através do Kafka)

**Objetivo:** responder, com dados e não com palpite, "o sistema está acompanhando a ingestão?", "quanto tempo uma leitura leva
até estar gravada?" e "onde foi parar a requisição X?", inclusive quando ela atravessa uma fila.

### D7.1: Três sinais, cada um para uma pergunta

| Sinal | Ferramenta | Responde |
|---|---|---|
| Métricas | OpenTelemetry → endpoint `/metrics` → **Prometheus** → **Grafana** | "Está saudável? Está acompanhando? Quão rápido?" |
| Traces | OpenTelemetry → OTLP → **Jaeger** | "Onde esta requisição específica gastou tempo?" |
| Logs | console estruturado (sem backend de logs, fora do escopo) | "O que aconteceu neste erro?" |

A Application usa só a BCL (`System.Diagnostics.Metrics` e `ActivitySource`): não depende de OpenTelemetry. O host (API/Worker) é quem
liga o OpenTelemetry aos nomes `SensorHub`, `Npgsql` e ao runtime do .NET.

### D7.2: O lag do consumer é a métrica mais importante, e é medido de duas formas de propósito

Em um sistema de streaming, CPU alta com lag zero é um sistema feliz e CPU baixa com lag crescendo é um sistema falhando em silêncio.
O lag é o **efeito** que o usuário sente (dado atrasado). Duas medições:

1. **Dentro do consumer** (`sensorhub_consumer_lag{group,topic,partition}`): o librdkafka emite estatísticas a cada 5 s com o lag de cada
   partição que o consumer lê; o consumer as publica num registro cujo snapshot é **substituído atomicamente**, então uma partição
   cedida a outra instância some do gauge (senão ficaria congelada e daria falso alarme).
2. **De fora, pelo broker** (`kafka_consumergroup_lag`, via `kafka-exporter`): continua existindo mesmo se TODOS os consumers do grupo
   morrerem.

**Evidência real, observada no Grafana:** ao reiniciar o Worker durante a carga, o lag medido **pelo broker chegou a 123 mil**, enquanto
o gauge **interno só registrou 24,5 mil de pico**, porque o gauge some junto com o consumer parado. É exatamente o cenário em que o lag
mais importa (o consumer caiu) e a medição interna é a mais cega. O alerta de produção deve usar a medição externa.

### D7.3: Atraso ponta a ponta, a métrica de SLO

Cada leitura carrega `ingestedAt` (o instante em que a API a aceitou). Ao fim de cada lote o consumer registra
`agora - ingestedAt` do dado mais novo (`sensorhub_consumer_processing_delay_seconds`). É a resposta direta a "quanto tempo uma leitura
leva até estar gravada / avaliada / empurrada para o dashboard?", por consumer group. Medido com 3.000 leituras/s:

| Grupo | p50 | p95 | p99 |
|---|---|---|---|
| `realtime` | 32 ms | 75 ms | 96 ms |
| `lastvalue` | 41 ms | 126 ms | 225 ms |
| `alerts` | 87 ms | 231 ms | 246 ms |
| `persistence` | 124 ms | 237 ms | 247 ms |

### D7.4: Tracing propagado através da fila

O Kafka é uma parede para o trace: sem propagação ele termina no producer. O contexto W3C (`traceparent`/`tracestate`) viaja nos
**headers da mensagem**: o producer injeta o contexto do span de publicação; o consumer o extrai.

- **Um span por lote publicado, não por mensagem**, e o contexto do span vai em todas as mensagens do lote.
- **Consumo em lote:** um lote mistura mensagens de várias requisições (traces diferentes). Convenção do OpenTelemetry: o span do lote
  **continua o trace da primeira mensagem** e tem **links** para os das demais (limitados a 10, senão um lote de milhares de
  requisições geraria milhares de links). Testado: 3 requisições independentes → os 3 traces ficam cobertos (como pai ou como link).
- O trace segue mais adiante: o motor de alertas injeta o contexto no evento de alerta, e o listener do tempo real o continua.

**Forma medida de um trace real** (Jaeger), 13 spans em 2 serviços e 2 saltos de Kafka:
`POST api/readings/batch` → `sensorhub.readings publish` → 3× `sensorhub.readings process` (persistência, último valor, alertas) →
`MGET` (Redis) + 3× `postgresql` → `sensorhub.alerts publish` → 2× `sensorhub.alerts process` (push ao dashboard na API).
A amostragem é por razão respeitando o pai (`ParentBased(TraceIdRatioBased)`, configurável): se a API amostrou a requisição, o consumer
mantém a decisão e o trace nunca fica pela metade. Health checks e `/metrics` são filtrados para não poluir os traces.

### D7.5: O Worker virou um host web mínimo

Um processo de fundo sem porta é invisível para o Prometheus e para os probes de container. O Worker agora sobe o Kestrel só para
`/metrics`, `/health/live` e `/health/ready` (readiness checa Kafka, Postgres e Redis conforme os papéis que ele executa). Os consumers
continuam sendo hosted services.

### D7.6: Cardinalidade das métricas (o que NÃO virou label)

Labels: `group`, `topic`, `partition` (6), `result`, `kind`, `severity`, `rule_type`, `reason`. **Nunca `sensor_id`**: com 5.000 sensores
cada métrica multiplicaria por 5.000 séries e derrubaria o Prometheus. "O que houve com o sensor X" é pergunta de trace ou de log, não de
métrica.

### D7.7: Grafana como código

O dashboard (14 painéis, 4 seções: ingestão, **lag**, consumers, persistência/alertas/tempo real) é provisionado por arquivo, junto com
as fontes de dados (Prometheus e Jaeger). `docker compose up` entrega tudo pronto, sem clique manual, e o dashboard versiona no git.

### Resultados

| Verificação | Resultado |
|---|---|
| Trace real atravessando HTTP → Kafka → consumers → Postgres/Redis → alerta → dashboard | 13 spans, 2 serviços, spans de lote com 10 links |
| Latência da API (POST de 100 leituras), simulador a 3.000/s | p50 19 ms, p99 63 ms em regime; pico de 242 ms durante o restart do Worker |
| Atraso ponta a ponta | p50 32 a 124 ms, p99 ≤ 250 ms em todos os grupos (tabela em D7.3) |
| Testes | 352 passando no total (71 domínio, 41 simulador, 87 application, 153 integração) |

### Bugs encontrados

**1. Todo quantil de atraso saía como "2,5 s" ou "4,75 s" (artefato de buckets).**
*Sintoma:* os 4 grupos mostravam p50 = 2,5 s e p95 = 4,75 s, números idênticos e sem sentido. *Causa raiz:* as buckets padrão do
OpenTelemetry (0, 5, 10, 25, 50...) foram pensadas para milissegundos; um histograma em **segundos** com valores de 0 a 5 cai inteiro no
primeiro balde e o Prometheus interpola o ponto médio. *Correção:* buckets explícitas por métrica (`AddView`), e os quantis reais
apareceram (p50 32 a 124 ms). Coberto por teste que confere as buckets no `/metrics`. Lição: um percentil bonito e redondo é suspeito.

**2. O gauge de lag interno some junto com o consumer que o reportava (achado observando um restart real).**
Ver D7.2: 123 mil medidos pelo broker contra 24,5 mil internos. Não era bug de código, mas uma limitação de desenho que só a
observação real expôs; a correção é a segunda medição (kafka-exporter) e a recomendação de alertar sobre ela.

**3. Teste do trace dependente de ordem (design de teste).**
A API publicava no tópico de produção, cheio de mensagens de outros testes; a mensagem do teste caía no meio de um lote e virava **link**
em vez de **pai** do span do consumer (comportamento correto, mas o teste assumia "primeira do lote"). Correção: tópico isolado por teste.
Um segundo erro no mesmo teste: o nome do span do consumer é `"{tópico} process"`, e ao isolar o tópico o nome mudou.

**4. Métricas HTTP ausentes no `/metrics` só nos testes.**
Com vários hosts (`WebApplicationFactory`) no mesmo processo, o `Meter` do ASP.NET Core (criado por host via `IMeterFactory`) não é visto
pelo OpenTelemetry. No processo real da API as métricas HTTP aparecem (conferido no scrape do Prometheus); a asserção foi removida do teste
com um comentário explicando, e o restante (métricas de negócio e o endpoint) continua coberto.

**5. Ambiguidade de `Program` (`CS0433`).**
Ao converter o Worker para o SDK Web, o `Program` dele passou a ser visível e colidiu com o da API no projeto de testes, que nem usava o
Worker. Removida a referência de projeto desnecessária.

**6. Teste "retoma do offset commitado" continuou oscilando sob carga (causa raiz NÃO comprovada).**
Mesmo depois de esperar o commit (lag 0), sob a suíte inteira (muitos containers disputando a máquina) ele estourava por tempo. Não isolei a
causa com evidência; a hipótese mais provável é o atraso de entrada do segundo consumer no grupo sob carga. Em vez de esconder, a asserção foi
reescrita para verificar a **propriedade** (as 500 novas chegam e nenhuma leitura antiga é relida), com espera generosa, e a suíte passou
duas vezes seguidas. Se voltar a oscilar, o próximo passo é instrumentar o tempo de rebalance com as métricas desta fase
(`sensorhub_consumer_rebalances_total`).


## Fase 8: Segurança (autenticação, autorização, limites e propriedade dos dados)

Até aqui qualquer um que alcançasse a porta escrevia leituras de qualquer sensor e reconhecia alertas. A fase tem duas identidades diferentes
de propósito: **dispositivos** (máquinas, alto volume, credencial de longa duração) e **pessoas** (dashboard/administração, credencial curta
que se renova). Misturá-las obrigaria a escolher entre "a chave do sensor abre o painel" e "o sensor faz login com senha".

### D8.1: Chave de API por dispositivo (ingestão)
Formato `shk_` + 32 bytes aleatórios do CSPRNG. No banco fica só o **SHA-256** da chave e um "hint" não secreto (os 12 primeiros caracteres)
para o operador identificá-la. Hash rápido (não PBKDF2) é correto aqui: a chave tem 256 bits de entropia, não há dicionário a atacar, e
a verificação roda a cada requisição de ingestão. A chave em texto puro aparece **uma única vez** (criação/rotação, com `Cache-Control: no-store`)
e nunca mais: nem em GET, nem em listagem, nem no banco (teste confere as três coisas). Rotação invalida a anterior; desativar o dispositivo corta a ingestão.

### D8.2: Usuários: PBKDF2 e verificação em tempo constante
Senha com o `PasswordHasher` do Identity (PBKDF2, salt por senha, iterações no próprio hash, portanto atualizáveis). Login com e-mail inexistente
executa uma verificação contra um hash "isca", para que o tempo de resposta não revele quais e-mails existem; a mensagem é a mesma
("Credenciais inválidas.") para e-mail desconhecido, senha errada e usuário desativado.

### D8.3: JWT de acesso curto + refresh token opaco rotativo com detecção de reuso
- Acesso: JWT HS256 de 15 min, algoritmo **fixado** na validação (rejeita `alg: none` e troca de algoritmo), tolerância de relógio de 30 s, claim curta `role`.
- Refresh: 32 bytes aleatórios, guardado só como hash, de **uso único**. Cada troca emite um par novo e marca o antigo como usado, dentro de uma *família*.
  Se um token já usado reaparece, alguém o copiou (ou o cliente legítimo perdeu a resposta): a **família inteira é revogada** e todos precisam autenticar de novo.
  Logout revoga a família. Coberto por teste de integração (rotação, reuso, logout).

### D8.4: Papéis: Viewer < Operator < Admin, políticas por endpoint
Leitura = Viewer; reconhecer alerta e criar regras = Operator; dispositivos, usuários e chaves = Admin. Um requisito `MinimumRoleRequirement` ordena
os papéis, então adicionar um papel intermediário não exige reescrever políticas. Matriz completa testada (401 sem credencial, 403 com papel insuficiente).
Criação de usuário exige senha forte e só o Admin cria; o primeiro Admin vem de `Security:BootstrapAdmin` (idempotente).

### D8.5: Propriedade dos dados: a chave de um dispositivo só escreve nos sensores dele
Autenticar não basta: um dispositivo comprometido poderia injetar leituras em sensores alheios e disparar alertas falsos. O `IngestionService` consulta o registro
de sensores (cache-aside com TTL) e rejeita, **por leitura e com motivo**, sensor desconhecido, de outro dispositivo, inativo, valor fora da faixa plausível ou unidade
divergente; as válidas do mesmo lote passam (202 parcial; se tudo é rejeitado, 422). Custo: uma consulta em cache por leitura; consistência eventual de até `SensorCacheSeconds`.
A mesma janela vale para chaves rotacionadas/desativadas (`DeviceCacheSeconds`, 30 s por padrão): é o preço de não ir ao banco a cada requisição, e está documentado no endpoint.

### D8.6: Rate limiting nativo do ASP.NET, com a partição certa em cada caso
| Rota | Algoritmo | Partição | Por quê |
|---|---|---|---|
| Ingestão | token bucket | dispositivo | um dispositivo com laço infinito esgota o SEU balde, não a capacidade dos outros; o burst absorve lotes legítimos |
| Login/refresh | janela fixa | IP | o usuário ainda é desconhecido; freia força bruta antes do hash de senha, que é caro |
| API geral | janela deslizante | usuário | limite de folga contra clientes descontrolados |

Respostas 429 em `application/problem+json` com `Retry-After`; o simulador respeita o cabeçalho. Health e métricas nunca são limitados.

### D8.7: `Security:Enabled` e a recusa de subir mal configurado
Desenvolvimento simples e os testes antigos rodam com a segurança desligada (o restante da API se comporta igual). Ligada, a API **recusa iniciar** sem chave de assinatura
ou com chave curta (< 32 bytes), e em `Production` também com a chave de desenvolvimento do repositório. Falhar na subida é melhor do que aceitar tokens assinados com um segredo público.
Os valores de desenvolvimento ficam só em `appsettings.Development.json`, com aviso; produção usa variáveis de ambiente.

### D8.8: Migração com backfill (`AddSecurity`)
Sensores passam a pertencer a um dispositivo (FK). Sensores já existentes não têm dispositivo, então a migração **insere primeiro um dispositivo por `device_id` distinto** (desativado, sem chave utilizável)
e só então cria a FK: o inverso falharia em qualquer banco com dados.

### D8.9: Cliente e tokens no navegador
Access token só em memória; refresh token em `sessionStorage` (some ao fechar a aba, sobrevive ao F5), nunca `localStorage`. Renovação **single-flight** (várias requisições com 401 compartilham UMA renovação:
o refresh é de uso único e uma segunda tentativa seria lida pela API como roubo). SignalR usa `accessTokenFactory`, que roda a cada (re)conexão e renova antes de expirar. A UI esconde o que o papel não permite,
mas é cortesia: quem barra é a API. Alternativa mais forte (cookie HttpOnly + CSRF) fica como evolução.
Reconhecer alerta passou a usar o **usuário do token**; o nome enviado pelo cliente só vale com a segurança desligada (antes qualquer um "reconhecia como" outra pessoa).

### D8.10: Simulador
A frota vira **um gateway** (um dispositivo, uma chave). `--mode register` faz login de administrador (`--admin-email`, senha por `--admin-password` ou `SENSORHUB_ADMIN_PASSWORD`, que evita o histórico do shell),
cria o dispositivo com id fixo e grava a chave (`--key-out`). Dispositivo já existente não tem chave recuperável (só rotação), e o simulador avisa.

### Resultados
- Testes: Domínio 94, Aplicação 129, Simulador 43, dashboard 26 (vitest), integração de segurança 42 contra Postgres/Redis/Kafka reais, e a suíte de integração inteira segue verde.
- Verificação ponta a ponta com a segurança ligada: `GET /api/sensors` sem token = 401; o simulador cadastrou 1 dispositivo, 24 sensores e 48 regras como administrador; 2.399 leituras enviadas com a chave do dispositivo, 0 falhas, 2.399 linhas no banco.
- A tela de login do dashboard foi conferida no navegador; o login interativo com senha não foi automatizado (cobertura pelos testes de `auth.ts`).

### Bugs encontrados

**1. O rate limit por dispositivo não isolava ninguém (teste de integração reprovou).**
*Sintoma:* com o balde do dispositivo A esgotado, o dispositivo B também recebia 429. *Causa raiz:* os esquemas de autenticação são **por política** (JWT × chave de API), então a identidade do dispositivo só passa
a existir em `HttpContext.User` quando a autorização roda. O limiter estava ANTES da autorização (com um comentário afirmando o contrário), via um chamador anônimo e particionava tudo por IP.
*Correção:* `UseRateLimiter` depois de `UseAuthorization`. O login (limiter por IP, no controller) continua antes do hash de senha.

**2. O 429 saía como `application/json`, não `problem+json`.** `WriteAsJsonAsync` sobrescreve o `Content-Type` definido antes; é preciso passá-lo como argumento.

**3. Regressão 400 → 404 ao criar sensor com `deviceId` vazio.** Com a checagem de existência do dispositivo antes da validação, um id inválido virou "não encontrado". A forma é validada (construção do domínio) antes da consulta.

**4. Promessa de renovação "presa" no cliente (achada em revisão antes de rodar os testes).** No primeiro desenho o `finally` que zera o single-flight ficava dentro do `try`; o retorno antecipado (sem refresh token) deixava
a promessa resolvida guardada para sempre e nenhuma renovação futura acontecia. Passou a `run().finally(...)`, com teste dedicado.

**5. Nome do "reconhecido por" era escolhido pelo cliente.** Falha de autorização de dados (personificação), corrigida em D8.9.

**6. Teste de "recusa de subir" esperava a exceção no lugar errado.** O host da `WebApplicationFactory` sobe no construtor, então a falha aparece ali e não na primeira requisição; o teste passou a afirmar isso.


## Fase 9: Compose completo e prova de carga

Até a Fase 8 a aplicação rodava no host (`dotnet run`) e só a infraestrutura em containers. A fase fecha o ciclo: **uma linha sobe tudo** e a prova de carga
passa por todo o caminho real (nginx → API → Kafka → 2 Workers → Timescale/Redis), medida pelas métricas da Fase 7, não por estimativa.

### D9.1: Uma imagem .NET parametrizada, multi-stage, sem root
`docker/dotnet.Dockerfile` recebe `PROJECT` e `ENTRY` e serve API, Worker e Simulador. O SDK compila; a imagem final leva só o runtime `aspnet` e roda com o usuário
não-root da imagem base (`USER $APP_UID`). O `restore` acontece depois de copiar **só os `.csproj`**, então a camada de pacotes NuGet só é refeita quando uma dependência muda.
Um `.dockerignore` exclui `bin/`, `obj/`, `node_modules/`, `.git`, `.env` e chaves: segredos não podem entrar em camadas de imagem.

### D9.2: Perfis do Compose: `docker compose up` continua sendo só a infraestrutura
`apps` (api, worker×2, dashboard) e `tools` (register, load) ficam em perfis. Quem desenvolve com `dotnet run` não é atrapalhado; quem quer tudo usa `--profile apps`.
Os hosts .NET compartilham um bloco `x-app-env` (hostnames de serviço em vez de `localhost`) por âncora YAML.

### D9.3: Dashboard servido por nginx, que também é o proxy
O navegador vê **uma origem** (`:8081`): estáticos do nginx, `/api` e `/hubs` repassados para a API. Sem CORS em produção e a API **não publica porta** no host.
`/hubs` leva `Upgrade`/`Connection` e `proxy_read_timeout 1h` (o padrão de 60 s derrubaria o WebSocket do SignalR), e `/assets/` (nome com hash) tem cache imutável enquanto o `index.html` é `no-cache`.

### D9.4: Confiar em `X-Forwarded-For` só quando declarado
Atrás do nginx, o IP de origem que a API enxerga é o do proxy, então o limitador de login (por IP, Fase 8) agruparia **todos os usuários num único balde**. A solução é `UseForwardedHeaders`,
mas confiar nesse cabeçalho sem proxy permite forjá-lo e escapar do limite por IP. Por isso é opt-in (`Security:TrustForwardedHeaders`), ligado só no Compose, onde a API não é alcançável diretamente.
Dois testes: com proxy declarado, clientes diferentes têm baldes diferentes; sem declarar, um `X-Forwarded-For` forjado a cada tentativa NÃO burla o limite.

### D9.5: Segredos fora da imagem e falha rápida
`SENSORHUB_JWT_KEY`, senha do administrador e chave do dispositivo vêm do `.env` (ignorado pelo git; `.env.example` documenta). Sem a chave, a API em `Production` recusa subir (Fase 8): melhor um
container em crash-loop com mensagem clara do que uma API aberta com segredo público. Os `register`/`load` usam `depends_on: service_healthy`.

### D9.6: Healthcheck sem `curl`
A imagem de runtime não tem `curl` nem `wget`; o healthcheck faz um `GET` cru via `/dev/tcp` do bash (bloco literal YAML para as barras invertidas do `printf`). O Worker só sobe depois da API saudável,
porque é a API que aplica as migrações.

### D9.7: Prometheus enxerga cada réplica
`static_configs` com `worker:9464` resolveria o DNS para UM IP. Para dois Workers, `dns_sd_configs` (tipo A, refresh de 10 s) gera um alvo por réplica e o rótulo `instance` as distingue.

### Resultados medidos (Docker Desktop, 1 broker, 1 Timescale, API + 2 Workers em containers, 300 sensores, lote de 500, 8 requisições concorrentes)

**Carga sustentada: 20.000 leituras/s por 60 s**
| | |
|---|---|
| Enviadas / falhas | 1.199.986 / 0 (taxa efetiva 19.997/s, 0 reenvios) |
| Linhas novas no banco | **1.199.986** (igual ao enviado: zero perda, zero duplicata) |
| Lag ao final | 0 |
| Publicação API→Kafka (ack), p95 | 32 ms |
| Atraso de processamento p50/p95/p99, persistência | 231 / 475 / 499 ms |
| idem, último valor | 26 / 181 / 319 ms |
| idem, alertas | 43 / 184 / 264 ms |
| idem, tempo real | 22 / 95 / 568 ms |
| Tamanho médio do lote (persistência) | ~4.750 |
| Recursos (pós-carga, idle) | api ~330 MiB, cada worker ~310 a 350 MiB |

**`kill -9` de um Worker no meio da carga (10.000/s por 45 s; réplica parada aos 12 s, religada aos 22 s)**
- Enviadas 449.901, falhas 0. O lag chegou a **327.864** ao fim da carga (uma réplica só por ~10 s, mais o rebalance) e **drenou a 0** sozinho.
- Linhas novas no banco: **449.901**, exatamente o enviado. O `kill -9` não perde nem duplica nada porque o offset só é commitado depois da persistência e a persistência é idempotente por `(sensor_id, ts)`.

**Chave quente (1 sensor emitindo 100× mais; 10.000/s por 30 s)**
- Mensagens por partição: p0 36.825, p1 39.828, p2 42.843, p3 36.076, **p4 106.761**, p5 37.579. A partição do sensor quente recebeu ~2,9× a média das outras: é o custo direto de
  `chave = sensorId` (a decisão de manter a ordem por sensor). O sistema absorveu (lag final 9.551 → 0, 299.912 linhas = 299.912 enviadas).
- Leitura honesta: com 6 partições e UM sensor quente isso é tolerável; se uma fração grande do tráfego viesse de poucos sensores, a partição vira o gargalo (um consumer por partição). Mitigações
  conhecidas e NÃO implementadas: mais partições (não ajuda um sensor só), *salting* da chave (perde a ordem por sensor) ou tratar sensores quentes num tópico próprio.

### Bugs encontrados

**1. Healthcheck "unhealthy" para sempre: `cannot create /dev/tcp/...: Directory nonexistent`.** `CMD-SHELL` executa `sh` (dash), e `/dev/tcp` é recurso do **bash**. Correção: `CMD bash -c`.
Segunda armadilha no mesmo item: `\r\n` dentro de string YAML entre aspas vira caracteres reais e quebra o comando; usei bloco literal.

**2. Cadastro da frota: 109 e depois 8 falhas com 300 sensores.** O cliente disparava ~900 requisições em um minuto contra o limite da API (600/min por usuário) e contava o 429 como falha. Duas causas:
(a) o cliente não respeitava `429 + Retry-After`; (b) o servidor devolvia `Retry-After: 1` para a janela deslizante, que **não informa** tempo de espera, então esperar 1 s era inútil (um permit só volta a
cada segmento de 10 s). Corrigido nos dois lados (o cliente espera e repete; o servidor diz 10) e coberto por teste em cada um. Depois: 300 sensores, 600 regras, 0 falhas.

**3. Cadastro falhava com login 401 (não era bug de código).** O administrador de bootstrap é idempotente e não sobrescreve a senha de um usuário existente; o banco de desenvolvimento já o tinha com outra senha.
Consequência de desenho aceita: trocar a senha do bootstrap no `.env` não muda um administrador que já existe (é preciso usar a API). Vale documentar; o simulador agora termina com uma mensagem clara em vez de exceção não tratada.

**4. Porta 8090 do Kafka UI em uso por outro container da máquina (Traefik).** Movida para 8092.

**5. Teste `Consumer_resumes_from_committed_offset...` voltou a falhar na suíte completa, agora em "primeira leva".** Em vez de atribuir de novo a "carga na máquina", li o teste: ele esperava `Processed.Count == 1000` num handler
at-least-once; se um rebalance na entrada do grupo reentrega parte do lote, o contador passa de 1000 e a condição nunca mais é verdadeira. É uma falha de **teste**, não de produto. Passou a contar mensagens distintas (sensor + instante).
Isolado ele já passava (3 de 3 antes da correção, então o sintoma só aparece na suíte inteira); a causa do sintoma anterior (Fase 7, item 6) continua sem prova, mas esta é uma causa real e confirmada por leitura do código.

**6. Ruído `libgssapi_krb5.so.2: cannot open shared object file` no log dos containers.** O librdkafka tenta carregar a biblioteca Kerberos (GSSAPI) ao iniciar; a imagem `aspnet` não a traz e o cluster usa PLAINTEXT. Inofensivo (a conexão e o consumo funcionam);
só seria necessário com SASL/GSSAPI.


## Fase 10: MQTT (Mosquitto → bridge → Kafka)

Dispositivos IoT reais raramente falam HTTP: usam MQTT (protocolo leve, sessão persistente, QoS, funciona em redes ruins). A fase põe um **Mosquitto** na frente e um **bridge** que entrega
as mensagens ao MESMO caminho de ingestão da API HTTP (validação, posse do sensor, idempotência, Kafka). O broker não substitui o Kafka: é a borda para dispositivos, e o Kafka segue sendo o log durável.

### D10.1: Um caminho só de ingestão (o bridge reusa o `IngestionService`)
O bridge é um consumidor MQTT que chama o mesmo `IngestionService` da API. Regra de negócio duplicada seria o primeiro lugar a divergir (um limite de faixa que muda de um lado e não do outro),
e a mesma chave de partição (`sensorId`) mantém a ordem por sensor idêntica nos dois protocolos. O teste de integração confere a chave da mensagem no Kafka.

### D10.2: Autenticação: a MESMA chave de API, validada pelo broker via plugin HTTP
Usuário MQTT = id do dispositivo, senha = chave de API (`shk_...`). O Mosquitto roda com `mosquitto-go-auth` e **não guarda credenciais**: a cada CONNECT ele pergunta a um serviço nosso (`/mqtt/auth`),
que usa o `IDeviceAuthenticator` da Fase 8. Assim criar, rotacionar e desativar a chave na API de administração vale para HTTP e MQTT, sem um arquivo de senhas paralelo. A chave precisa ser DESTA identidade
(uma chave válida de outro dispositivo não autentica como você). O bridge se autentica como um superusuário próprio (`sensorhub-bridge`, segredo forte exigido na subida, comparação em tempo constante).

### D10.3: ACL: dispositivo só PUBLICA no próprio tópico e não lê nada
`sensorhub/devices/{deviceId}/readings`, somente escrita, somente o próprio id. Não pode assinar (nem `#`, nem o próprio tópico, nem `$share`), então nunca enxerga dados alheios. Isso é o que torna o **id do tópico confiável**: o bridge deriva a
identidade do tópico, nunca do conteúdo. A ACL também consulta o banco: desativar um dispositivo corta quem JÁ está conectado (o broker só autentica no CONNECT), com o atraso do cache curto do plugin (30 s em produção).
Limite honesto: **rotacionar a chave não derruba uma sessão já aberta**, só impede a próxima conexão; para expulsar na hora, desative o dispositivo.

### D10.4: Autenticação e consumidor são PROCESSOS SEPARADOS (papéis `auth` e `bridge`)
Achado por um teste que falhou (ver bug 1): quando o backend de autenticação vivia no mesmo processo do consumidor, parar o bridge fazia o broker falhar TODA checagem de ACL e desconectava todos os dispositivos. O mesmo binário roda
em dois papéis (como o Worker): `auth` (sem Kafka, só Postgres) e `bridge` (consumidor). Um deploy do bridge não toca a conexão dos dispositivos.

### D10.5: At-least-once de ponta a ponta: PUBACK manual, QoS 1, sessão persistente, assinatura compartilhada
- O bridge assina `$share/sensorhub-bridge/sensorhub/devices/+/readings` com QoS 1 e **sessão persistente**; o PUBACK só sai DEPOIS que o Kafka confirmou (`AutoAcknowledge = false`).
  Se o bridge cai no meio, o broker reentrega e a persistência idempotente por `(sensor_id, ts)` absorve a duplicata.
- Assinatura compartilhada (`$share`): réplicas do bridge dividem a carga (cada mensagem vai a UMA delas) em vez de cada uma receber tudo.
- Falha do Kafka: até N tentativas com backoff; esgotadas, NÃO confirma e derruba a conexão, o que antecipa a reentrega. Mensagem **malformada** (JSON inválido, lote vazio, tópico fora do esquema, payload grande) é confirmada e descartada com log e
  métrica: reentregá-la travaria para sempre a fila do dispositivo (poison message). Rejeições de negócio (sensor alheio, valor absurdo) também são confirmadas: reenviar não muda o veredito.

### D10.6: Contrapressão sem fila em memória
O tratamento é concorrente (`Mqtt:MaxConcurrency`, 64), limitado por um semáforo adquirido NO handler: sem vaga, o cliente para de ler do socket e o `max_inflight_messages` do broker segura o resto. Ver bug 2 (por que a concorrência existe).

### D10.7: Client id estável
A sessão persistente (e as mensagens que o broker guardou) pertencem ao `clientId`. Ver bug 3. É configurável (`Mqtt:ClientId`); com várias réplicas, cada uma precisa do seu id FIXO (ordinal de StatefulSet).

### D10.8: Broker e Compose
`mosquitto` (porta 1884 no host), `mqtt-auth` e `mqtt-bridge` no perfil `apps`; sessões persistidas em volume; limites (`message_size_limit`, `max_queued_messages`, `max_inflight_messages`). O simulador ganhou `--mode mqtt`
(usuário = id do gateway, senha = chave, QoS 1, um array JSON por mensagem). Os testes usam o MESMO `mosquitto.conf` do Compose (só trocam host/porta e o cache).

### Resultados medidos (Compose completo, 1 bridge, 300 sensores, mensagens pequenas de ~3 leituras)
| Cenário | Enviadas | Linhas novas no banco | Observação |
|---|---|---|---|
| 10.000/s por 30 s via MQTT | 299.987 | **299.987** | lag final 0 |
| 30.000/s por 30 s via MQTT | 899.938 | **899.938** | publicação bridge→Kafka p95 25 ms, atraso de persistência p95 0,42 s |
| 10.000/s por 45 s, **`kill -9` do bridge** (10 s fora) | 449.969 | **449.969** | o broker guardou as mensagens e as entregou ao bridge que voltou |
| 8.000/s por 40 s, **container do bridge recriado** | 319.994 | **319.994** | mesmo `clientId`, sessão preservada |

Testes: 19 de integração contra Mosquitto real (autenticação, ACL, posse, veneno, rotação, desativação, queda do bridge, 400 mensagens pequenas), mais 30+ unitários das regras puras (tópicos, payload, política de acesso, destino de cada mensagem).

### Bugs encontrados

**1. Publicar com o bridge fora do ar DESCONECTAVA o dispositivo (teste de integração reprovou: `Unexpected DISCONNECT`).** *Causa raiz:* o backend de autenticação/ACL do Mosquitto estava no mesmo processo do bridge; sem ele, o plugin não consegue
validar a ACL e o broker derruba o publicador. *Correção:* papéis separados (D10.4). A mesma classe de erro (acoplar o caminho de autorização ao de processamento) apareceria em qualquer deploy.

**2. Vazão de ~250 leituras/s com mensagens pequenas (achado na prova de carga, não nos testes).** O simulador enviava 300 mil leituras em ~93 mil mensagens de ~3 leituras; o bridge as tratava UMA POR VEZ (~31 ms cada, dos quais ~9 ms de Kafka), logo ~31 msg/s.
A vazão de um consumidor serial é 1/latência, não a capacidade da máquina. *Correção:* tratamento concorrente limitado (D10.6). Depois: 30.000 leituras/s pelo mesmo caminho, sem perda. Lição: testes funcionais não mostram isso; só a carga com o formato REAL de mensagem (pequenas e muitas).

**3. ~148 mil leituras "sumiram" após um redeploy do bridge (achado na mesma carga).** O `clientId` era derivado do hostname do container, que muda a cada recriação. As mensagens que o broker guardara para a sessão persistente ficaram presas na sessão do id antigo
e o container novo (id novo) nunca as recebeu. *Correção:* `ClientId` estável e configurável, com teste de propriedade; depois, recriar o container no meio da carga não perdeu nada (319.994 = 319.994). Nota: o dado só estava "preso", mas expiraria com a sessão (1 h): em produção equivaleria a perda.

**4. Todas as 300 mil publicações falharam com "Falhas: 299.980" e nenhum motivo.** Duas causas encadeadas: (a) `mosquitto.conf` apontava a autenticação para `mqtt-bridge`, e o serviço de autenticação se chama `mqtt-auth` (erro de nome ao separar os papéis; o broker recusava até o próprio bridge);
(b) o simulador só contava falhas. O relatório agora traz a **primeira falha** (`Primeira falha: ...`), com teste. O bridge, entretanto, se comportou como projetado: ficou tentando reconectar a cada 2 s e conectou sozinho depois da correção.

**5. O cliente MQTTnet 5 não lança exceção quando o broker recusa o CONNECT** (devolve um `ResultCode`), ao contrário do que meus testes assumiam (`ThrowsAny` falhava com "nenhuma exceção"). Achei, pelos logs do broker, que ele recusava corretamente ("not authorised"), então o defeito era só do teste. O helper passou a
lançar `MqttRefusedException` (e o próprio bridge e o simulador já checavam o `ResultCode`). Isso importa em produção: código que ignora o `ResultCode` acharia que está conectado.

**6. Colisão do tipo `Program` (`CS0433`) de novo.** Com dois projetos web referenciados pelos testes. Ponto de entrada explícito (`BridgeEntryPoint.Main`) em vez de instruções de nível superior, e o host fica em `MqttBridgeHost.Build` (usado também pelos testes, com Kestrel real).

**7. Prometheus não enxergava os alvos novos.** O arquivo montado mudou, mas o processo não recarrega sozinho. Reinício do container; um `--web.enable-lifecycle` + reload seria a evolução.

### Limitações conhecidas (não implementadas)
- Os endpoints `/mqtt/*` do serviço de autenticação só são protegidos pelo isolamento de rede (o plugin não envia cabeçalho de segredo); sem TLS entre broker e dispositivos neste ambiente local. Em produção: TLS no listener do broker e rede privada para o auth.
- Sem mensagens de comando para os dispositivos (o dispositivo não pode assinar nada): o sentido broker→dispositivo fica para depois.
- Mensagens descartadas por malformação não vão a uma DLQ MQTT; ficam em métrica (`outcome=malformed`) e log.
