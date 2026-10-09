#!/usr/bin/env bash
# Runs one benchmark against the docker compose stack and writes load/results/<name>.md (+ k6 JSON).
#
#   load/run-benchmark.sh <name>
#
# Knobs (environment): RATE, RAMP, HOLD, ENDPOINTS_PER_TYPE (k6); VIRTUAL_THREADS_ENABLED, RELAY_DISPATCHER_EXECUTOR,
# RELAY_DISPATCHER_PLATFORM_POOL_SIZE, RELAY_BULKHEAD_PER_ENDPOINT, RECEIVER_FAILURE_RATE, RECEIVER_MIN_LATENCY_MS,
# RECEIVER_MAX_LATENCY_MS (stack). The stack's api, dispatcher and receiver are recreated with these settings and
# all relay state (database, Redis, receiver counters) is reset first, so runs do not influence each other.
set -euo pipefail

NAME=${1:?usage: run-benchmark.sh <name>}
cd "$(dirname "$0")/.."
set -a; . ./.env; set +a

export RATE=${RATE:-200} RAMP=${RAMP:-30s} HOLD=${HOLD:-3m} ENDPOINTS_PER_TYPE=${ENDPOINTS_PER_TYPE:-4}
export TRACING_SAMPLING_PROBABILITY=${TRACING_SAMPLING_PROBABILITY:-0.01}
# Caps the retry tail so a run drains in minutes rather than the default 10 min max delay.
export RELAY_RETRY_MAX_DELAY=${RELAY_RETRY_MAX_DELAY:-2m}
OUT=load/results
mkdir -p "$OUT"

psql() { docker compose exec -T postgres psql -U "${POSTGRES_USER:-relay}" -d "${POSTGRES_DB:-relay}" -At -F ' ' -c "$1"; }
prom() { curl -s --get "http://localhost:9090/api/v1/query" --data-urlencode "query=$1" | sed -E 's/.*"value":\[[^,]+,"([^"]+)".*/\1/;t;s/.*/n\/a/'; }

echo "== $NAME: recreating relay services"
docker compose up -d --force-recreate --no-deps mock-receiver relay-api relay-dispatcher >/dev/null
for svc in mock-receiver relay-api relay-dispatcher; do
  until [ "$(docker inspect -f '{{.State.Health.Status}}' "$(docker compose ps -q $svc)")" = healthy ]; do sleep 2; done
done

echo "== $NAME: resetting state"
psql "TRUNCATE delivery_attempts, outbox, deliveries, events, idempotency_keys, endpoints, endpoint_secrets CASCADE" >/dev/null
docker compose exec -T redis redis-cli FLUSHALL >/dev/null
sleep 5

echo "== $NAME: sampling container stats"
STATS="$OUT/$NAME-docker-stats.csv"
echo "epoch,container,cpu,mem" > "$STATS"
( while true; do
    docker stats --no-stream --format '{{.Name}},{{.CPUPerc}},{{.MemUsage}}' \
      | grep -E 'relay-(api|dispatcher)|mock-receiver|kafka|postgres' | sed "s/^/$(date +%s),/" >> "$STATS" || true
    sleep 2
  done ) &
SAMPLER=$!
trap 'kill $SAMPLER 2>/dev/null || true' EXIT

echo "== $NAME: k6 (rate=$RATE/s ramp=$RAMP hold=$HOLD)"
START=$(date +%s)
K6_STATUS=0
HOST_DIR=$( (pwd -W) 2>/dev/null || pwd )   # Windows Git Bash needs a native path for the bind mount
MSYS_NO_PATHCONV=1 docker run --rm --network webhook-relay_default -v "$HOST_DIR/load:/scripts" \
  -e TOKEN="$RELAY_API_TOKEN" -e RATE -e RAMP -e HOLD -e ENDPOINTS_PER_TYPE -e RUN_NAME="$NAME" \
  grafana/k6:2.3.0 run --quiet /scripts/ingest.js | tee "$OUT/$NAME-k6.txt" || K6_STATUS=$?
END=$(date +%s)

echo "== $NAME: waiting for deliveries to drain"
DRAIN_START=$(date +%s)
while :; do
  PENDING=$(psql "SELECT count(*) FROM deliveries WHERE status IN ('PENDING','IN_FLIGHT','RETRYING','PARKED')")
  [ "$PENDING" = 0 ] && break
  [ $(( $(date +%s) - DRAIN_START )) -gt 1800 ] && { echo "drain timeout with $PENDING pending"; break; }
  sleep 5
done
DRAINED=$(date +%s)
kill $SAMPLER 2>/dev/null || true

case "$RAMP" in *m) RAMP_S=$(( ${RAMP%m} * 60 ));; *s) RAMP_S=${RAMP%s};; *) RAMP_S=$RAMP;; esac
HOLD_FROM=$((START + RAMP_S + 5))
WINDOW="to_timestamp($HOLD_FROM) AND to_timestamp($END)"
RANGE="$((DRAINED - START + 60))s"

{
  echo "# Benchmark run: $NAME"
  echo
  echo "- Date: $(date -u +%Y-%m-%dT%H:%M:%SZ)"
  echo "- Settings: RATE=$RATE/s RAMP=$RAMP HOLD=$HOLD ENDPOINTS_PER_TYPE=$ENDPOINTS_PER_TYPE (3 event types)"
  echo "- Retry max delay: $RELAY_RETRY_MAX_DELAY"
  echo "- Dispatcher: VIRTUAL_THREADS_ENABLED=${VIRTUAL_THREADS_ENABLED:-true} EXECUTOR=${RELAY_DISPATCHER_EXECUTOR:-virtual} PLATFORM_POOL=${RELAY_DISPATCHER_PLATFORM_POOL_SIZE:-200} BULKHEAD=${RELAY_BULKHEAD_PER_ENDPOINT:-50}"
  echo "- Receiver: failure rate ${RECEIVER_FAILURE_RATE:-0.10}, latency ${RECEIVER_MIN_LATENCY_MS:-50}-${RECEIVER_MAX_LATENCY_MS:-200} ms"
  echo "- k6 exit code: $K6_STATUS (non-zero means a threshold failed)"
  echo "- Drain after load: $((DRAINED - END)) s"
  echo
  echo '## k6'
  echo '```'
  cat "$OUT/$NAME-k6.txt"
  echo '```'
  echo
  echo '## Outcome (from the database)'
  echo '```'
  echo "events: $(psql "SELECT count(*) FROM events")"
  echo "deliveries by status: $(psql "SELECT string_agg(status || '=' || n, ' ') FROM (SELECT status, count(*) n FROM deliveries GROUP BY status ORDER BY status) s")"
  echo "http attempts: $(psql "SELECT count(*) FROM delivery_attempts")  by outcome: $(psql "SELECT string_agg(outcome || '=' || n, ' ') FROM (SELECT outcome, count(*) n FROM delivery_attempts GROUP BY outcome ORDER BY outcome) s")"
  echo "deliveries succeeded/s during hold: $(psql "SELECT round(count(*) / greatest(1, $END - $HOLD_FROM)::numeric, 1) FROM deliveries WHERE status = 'SUCCEEDED' AND completed_at BETWEEN $WINDOW")"
  echo "accept->delivered latency, first-attempt successes (ms) p50/p95/p99/max: $(psql "SELECT concat_ws(' / ', round(percentile_cont(0.5) WITHIN GROUP (ORDER BY ms)), round(percentile_cont(0.95) WITHIN GROUP (ORDER BY ms)), round(percentile_cont(0.99) WITHIN GROUP (ORDER BY ms)), round(max(ms))) FROM (SELECT EXTRACT(EPOCH FROM completed_at - created_at) * 1000 AS ms FROM deliveries WHERE status = 'SUCCEEDED' AND attempt_count = 1 AND created_at BETWEEN $WINDOW) t")"
  echo "accept->delivered latency, all successes incl. retries (ms) p50/p95/p99: $(psql "SELECT concat_ws(' / ', round(percentile_cont(0.5) WITHIN GROUP (ORDER BY ms)), round(percentile_cont(0.95) WITHIN GROUP (ORDER BY ms)), round(percentile_cont(0.99) WITHIN GROUP (ORDER BY ms))) FROM (SELECT EXTRACT(EPOCH FROM completed_at - created_at) * 1000 AS ms FROM deliveries WHERE status = 'SUCCEEDED' AND created_at BETWEEN $WINDOW) t")"
  echo "receiver: $(curl -s localhost:8090/admin/stats | sed -E 's/,"hooks".*//; s/[{}"]//g')"
  echo '```'
  echo
  echo '## Dispatcher resources (Prometheus, over the run)'
  echo '```'
  echo "max heap used (MB): $(prom "max_over_time(sum(jvm_memory_used_bytes{application=\"relay-dispatcher\",area=\"heap\"})[$RANGE:5s]) / 1048576")"
  echo "max non-heap used (MB): $(prom "max_over_time(sum(jvm_memory_used_bytes{application=\"relay-dispatcher\",area=\"nonheap\"})[$RANGE:5s]) / 1048576")"
  echo "max live platform threads: $(prom "max_over_time(jvm_threads_live_threads{application=\"relay-dispatcher\"}[$RANGE])")"
  echo "max attempts in flight: $(prom "max_over_time(webhook_delivery_inflight{application=\"relay-dispatcher\"}[$RANGE])")"
  echo "max process CPU (cores): $(prom "max_over_time(process_cpu_usage{application=\"relay-dispatcher\"}[$RANGE]) * $(docker info --format '{{.NCPU}}')")"
  echo "max container memory (docker stats, dispatcher): $(grep relay-dispatcher "$STATS" | cut -d, -f4 | cut -d/ -f1 | sort -h | tail -1)"
  echo '```'
} > "$OUT/$NAME.md"

echo "== $NAME: wrote $OUT/$NAME.md"
cat "$OUT/$NAME.md"
