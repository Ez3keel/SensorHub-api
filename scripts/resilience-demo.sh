#!/bin/bash
# Demo de resiliência (Git Bash/Linux): kill -9 de um worker no meio da carga e volta da réplica; confere zero perda e zero duplicata.
# Uso: KILL=1 RATE=10000 DURATION=45 ./scripts/resilience-demo.sh   (HOT=100 KILL=0 para o teste de chave quente)
# MQTT: SERVICE=load-mqtt VICTIM=sensorhub-mqtt-bridge-1 ./scripts/resilience-demo.sh
# Requer a stack de pé (docker compose --profile apps up -d), o dispositivo registrado e SENSORHUB_DEVICE_KEY no .env.
cd "$(dirname "$0")/.."
export MSYS_NO_PATHCONV=1
count() { docker exec sensorhub-db psql -U sensorhub -d sensorhub -tAc "select count(*) from readings;"; }
lag() { python3 - <<'PY'
import json,urllib.parse,urllib.request
u='http://localhost:9091/api/v1/query?'+urllib.parse.urlencode({'query':'sum(kafka_consumergroup_lag)'})
d=json.load(urllib.request.urlopen(u))['data']['result']
print(d[0]['value'][1] if d else 'n/a')
PY
}
partitions() { docker exec sensorhub-kafka /opt/kafka/bin/kafka-get-offsets.sh --bootstrap-server localhost:9092 --topic sensorhub.readings 2>/dev/null | sort; }

BEFORE=$(count)
echo "linhas antes: $BEFORE"
partitions > /tmp/offs_before.txt

RATE=${RATE:-10000} DURATION=${DURATION:-45} BATCH=500 WORKERS=8 HOT=${HOT:-1} docker compose --profile apps --profile tools run --rm ${SERVICE:-load} > /tmp/load.out 2>&1 &
LOADPID=$!

if [ "${KILL:-1}" = "1" ]; then
  sleep 12
  echo "$(date +%T) kill -9 ${VICTIM:-sensorhub-worker-2}"
  docker kill ${VICTIM:-sensorhub-worker-2} > /dev/null
  sleep 10
  echo "$(date +%T) sobe a réplica de novo"
  docker start ${VICTIM:-sensorhub-worker-2} > /dev/null
fi

wait $LOADPID
tail -3 /tmp/load.out
echo "lag ao fim da carga: $(lag)"
for i in $(seq 1 30); do
  L=$(lag); [ "$L" = "0" ] && break; sleep 2
done
echo "lag depois de esperar: $(lag)"
AFTER=$(count)
echo "linhas depois: $AFTER  (delta = $((AFTER-BEFORE)))"
partitions > /tmp/offs_after.txt
echo "mensagens por partição neste teste:"
paste -d' ' /tmp/offs_before.txt /tmp/offs_after.txt | awk '{split($1,a,":"); split($2,b,":"); print "  p" a[2] ": " b[3]-a[3]}' 2>/dev/null || true
