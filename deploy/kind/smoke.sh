#!/usr/bin/env bash
# Deploys webhook-relay to a local kind cluster with KEDA and runs an end-to-end smoke test.
# Needs: docker, kind, kubectl, helm (v4). Images are built locally and side-loaded; nothing is pushed.
#
#   deploy/kind/smoke.sh            # create cluster, deploy, test
#   KEEP=1 deploy/kind/smoke.sh     # leave the cluster running afterwards
set -euo pipefail
cd "$(dirname "$0")/../.."

CLUSTER=${CLUSTER:-webhook-relay}
KEDA_VERSION=v2.21.0
TAG=0.1.0

echo "== building images"
for m in relay-api relay-dispatcher mock-receiver; do
  docker build -q --build-arg MODULE=$m -t ghcr.io/gouranshul/$m:$TAG . >/dev/null
done

echo "== creating kind cluster $CLUSTER"
kind get clusters | grep -qx "$CLUSTER" || kind create cluster --name "$CLUSTER" --wait 120s
for m in relay-api relay-dispatcher mock-receiver; do
  kind load docker-image --name "$CLUSTER" ghcr.io/gouranshul/$m:$TAG
done

echo "== installing KEDA $KEDA_VERSION"
kubectl apply --server-side -f "https://github.com/kedacore/keda/releases/download/$KEDA_VERSION/keda-${KEDA_VERSION#v}.yaml" >/dev/null
kubectl -n keda rollout status deploy/keda-operator --timeout=180s

echo "== secrets and dependencies"
API_TOKEN=$(openssl rand -hex 24)
kubectl create secret generic relay-secrets \
  --from-literal=RELAY_SECRET_KEY="$(openssl rand -base64 32)" \
  --from-literal=RELAY_API_TOKEN="$API_TOKEN" \
  --from-literal=DB_PASSWORD="$(openssl rand -hex 16)" \
  --dry-run=client -o yaml | kubectl apply -f - >/dev/null
kubectl apply -f deploy/kind/infra.yaml >/dev/null
for d in postgres kafka redis mock-receiver; do kubectl rollout status deploy/$d --timeout=180s; done

echo "== helm install"
helm upgrade --install relay deploy/helm/webhook-relay \
  --set secrets.existingSecret=relay-secrets \
  --set config.allowPrivateNetworks=true \
  --set api.replicaCount=1 \
  --set autoscaling.keda.minReplicas=1 --set autoscaling.keda.maxReplicas=4 \
  --set api.resources.requests.cpu=100m --set dispatcher.resources.requests.cpu=100m \
  --wait --timeout 6m

kubectl get scaledobject,hpa,deploy,pods

echo "== smoke test"
kubectl port-forward svc/relay-webhook-relay-api 18080:8080 >/dev/null 2>&1 &
PF=$!
trap 'kill $PF 2>/dev/null || true; [ -n "${KEEP:-}" ] || kind delete cluster --name "$CLUSTER"' EXIT
sleep 3
AUTH="Authorization: Bearer $API_TOKEN"
EP=$(curl -sf localhost:18080/v1/endpoints -H "$AUTH" -H 'Content-Type: application/json' \
  -d '{"url":"http://mock-receiver:8090/hooks/kind","eventTypes":["order.created"]}')
echo "endpoint: $(echo "$EP" | grep -oE '"id":"[^"]+"')"
for i in $(seq 1 20); do
  curl -sf localhost:18080/v1/events -H "$AUTH" -H 'Content-Type: application/json' -H "Idempotency-Key: kind-$i" \
    -d "{\"type\":\"order.created\",\"data\":{\"order_id\":\"ord_$i\"}}" >/dev/null
done
for _ in $(seq 1 30); do
  N=$(curl -sf "localhost:18080/v1/deliveries?status=SUCCEEDED&limit=200" -H "$AUTH" | grep -o '"status":"SUCCEEDED"' | wc -l)
  [ "$N" -ge 20 ] && break
  sleep 2
done
echo "succeeded deliveries: $N / 20"
[ "$N" -ge 20 ] || { echo "SMOKE TEST FAILED"; exit 1; }
echo "SMOKE TEST PASSED"
