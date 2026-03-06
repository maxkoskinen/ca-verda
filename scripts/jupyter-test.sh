#!/usr/bin/env bash
set -e

GPU=false
if [[ "${1}" == "--gpu" ]]; then
  GPU=true
fi

if $GPU; then
  MANIFEST="manifests/examples/jupyter-gpu-test.yaml"
  POD_NAME="jupyter-cloud-gpu-test"
else
  MANIFEST="manifests/examples/jupyter-cpu-test.yaml"
  POD_NAME="jupyter-cloud-cpu-test"
fi

NAMESPACE="default"
LOCAL_PORT=8888
POD_PORT=8888

echo "📦 Applying $($GPU && echo 'GPU' || echo 'CPU') Jupyter manifest..."
kubectl apply -f ${MANIFEST}

echo "⏳ Waiting for pod to be scheduled (may trigger autoscaling)..."
for i in $(seq 1 30); do
  if kubectl get pod ${POD_NAME} -n ${NAMESPACE} >/dev/null 2>&1; then
    break
  fi
  echo "   Waiting for pod object... (attempt $i/30)"
  sleep 5
done

NODE=$(kubectl get pod ${POD_NAME} -n ${NAMESPACE} -o jsonpath='{.spec.nodeName}')
echo "🖥️  Pod scheduled on: ${NODE:-<pending>}"

# Stream scheduling events while waiting (shows autoscaler activity)
echo "📡 Recent events:"
kubectl get events -n ${NAMESPACE} --field-selector involvedObject.name=${POD_NAME} \
  --sort-by='.lastTimestamp' 2>/dev/null | tail -5 || true

echo "⏳ Waiting for pod to be Ready (up to 10min for node provisioning)..."
kubectl wait --for=condition=Ready pod/${POD_NAME} \
  -n ${NAMESPACE} --timeout=600s

NODE=$(kubectl get pod ${POD_NAME} -n ${NAMESPACE} -o jsonpath='{.spec.nodeName}')
NODE_IP=$(kubectl get node ${NODE} -o jsonpath='{.status.addresses[?(@.type=="InternalIP")].address}' 2>/dev/null || echo "unknown")
echo "✅ Running on node: ${NODE} (${NODE_IP})"

echo "🔑 Fetching Jupyter token..."
TOKEN=""
for i in $(seq 1 20); do
  TOKEN=$(kubectl logs ${POD_NAME} -n ${NAMESPACE} 2>/dev/null \
    | grep -oE 'token=[a-f0-9]+' \
    | tail -1 \
    | cut -d= -f2)
  if [ -n "$TOKEN" ]; then
    break
  fi
  echo "   Waiting for token... (attempt $i/20)"
  sleep 5
done

URL="http://localhost:${LOCAL_PORT}/lab?token=${TOKEN}"

if [ -z "$TOKEN" ]; then
  echo "⚠️  Could not extract token. Run: kubectl logs ${POD_NAME} -n ${NAMESPACE} | grep token"
else
  echo ""
  echo "🎉 Jupyter is ready!"
  echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
  echo "   URL: ${URL}"
  echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
  if command -v pbcopy &>/dev/null; then
    echo "${URL}" | pbcopy && echo "📋 URL copied to clipboard"
  elif command -v xclip &>/dev/null; then
    echo "${URL}" | xclip -selection clipboard && echo "📋 URL copied to clipboard"
  fi
fi

cleanup() {
  echo ""
  echo "🛑 Port-forward stopped."
  read -p "🗑️  Delete the Jupyter pod? [y/N] " confirm
  if [[ "$confirm" =~ ^[Yy]$ ]]; then
    kubectl delete -f ${MANIFEST} --ignore-not-found
    echo "✅ Pod deleted."
  fi
}
trap cleanup EXIT

echo "🔄 Starting port-forward (Ctrl+C to stop)..."
kubectl port-forward pod/${POD_NAME} ${LOCAL_PORT}:${POD_PORT} -n ${NAMESPACE}
