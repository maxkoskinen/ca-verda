#!/usr/bin/env bash
set -e

IMAGE_NAME="ca-verda"
TAG="dev"
NAMESPACE="kube-system"
DEPLOYMENT="verda-cloud-provider"

MINIKUBE_ARGS="--driver=docker --kubernetes-version=v1.30.0 --cpus=2 --memory=4096 --embed-certs --apiserver-ips=10.200.0.1"

echo "🔎 Checking if Minikube is running..."

if ! minikube status >/dev/null 2>&1; then
    echo "🚀 Starting Minikube..."
    minikube start ${MINIKUBE_ARGS}

    echo "🔧 Patching kubeadm-config to use standard cert paths..."
    kubectl -n kube-system get cm kubeadm-config -o yaml \
        | sed 's|/var/lib/minikube/certs|/etc/kubernetes/pki|g' \
        | kubectl apply -f -

    echo "🔧 Copying CA cert to standard PKI path inside minikube..."
    minikube ssh -- "sudo mkdir -p /etc/kubernetes/pki && \
        sudo cp /var/lib/minikube/certs/ca.crt /etc/kubernetes/pki/ca.crt && \
        sudo cp /var/lib/minikube/certs/ca.key /etc/kubernetes/pki/ca.key"

    echo "🔧 Patching kubelet-config to use standard cert paths..."
    kubectl -n kube-system get cm kubelet-config -o yaml \
        | sed 's|/var/lib/minikube/certs|/etc/kubernetes/pki|g' \
        | kubectl apply -f -

    echo "🔑 Generating join token..."
    JOIN_CMD=$(minikube ssh -- "sudo /var/lib/minikube/binaries/v1.30.0/kubeadm token create --print-join-command" 2>/dev/null | tr -d '\r')
    TOKEN=$(echo "${JOIN_CMD}"   | awk '{for(i=1;i<=NF;i++) if($i=="--token") print $(i+1)}')
    CA_HASH=$(echo "${JOIN_CMD}" | awk '{for(i=1;i<=NF;i++) if($i=="--discovery-token-ca-cert-hash") print $(i+1)}')

    printf "│ Token:   %s\n" "${TOKEN}"
    printf "│ CA Hash: %s\n" "${CA_HASH}"


else
  echo "✅ Minikube already running."
fi

echo "🌐 Applying Flannel with host-gw backend..."
kubectl apply -f https://github.com/flannel-io/flannel/releases/latest/download/kube-flannel.yml

echo "🔧 Patching Flannel MTU to 1370 (WireGuard overhead)..."
kubectl -n kube-flannel get cm kube-flannel-cfg -o json \
  | jq '.data["net-conf.json"] = (.data["net-conf.json"] | fromjson | .Backend.MTU = 1370 | tojson)' \
  | kubectl apply -f -
kubectl -n kube-flannel rollout restart daemonset/kube-flannel-ds

echo "⏳ Waiting for Flannel and CoreDNS to stabilise..."
kubectl rollout status daemonset/kube-flannel-ds -n kube-flannel --timeout=90s

echo "🔧 Patching CoreDNS upstream DNS..."
kubectl -n kube-system get cm coredns -o yaml \
  | sed 's|forward . /etc/resolv.conf|forward . 1.1.1.1|' \
  | kubectl apply -f -

kubectl rollout restart deployment/coredns -n kube-system
kubectl rollout status deployment/coredns -n kube-system --timeout=60s

curl -sL https://raw.githubusercontent.com/NVIDIA/k8s-device-plugin/v0.17.1/deployments/static/nvidia-device-plugin.yml \
  | kubectl apply -f -

kubectl -n kube-system patch daemonset nvidia-device-plugin-daemonset \
  --type=json \
  -p='[{
    "op": "add",
    "path": "/spec/template/spec/nodeSelector",
    "value": {"verda.com/gpu": "true"}
  }]'

kubectl rollout status daemonset/nvidia-device-plugin-daemonset -n kube-system --timeout=60s

echo "🔨 Building image inside Minikube..."
minikube image build -t ${IMAGE_NAME}:${TAG} .

echo "🗑️  Deleting provider deployment..."
kubectl delete -f manifests/provider --ignore-not-found

echo "⏳ Waiting for provider pod to fully terminate..."
kubectl wait --for=delete pod -l app=verda-cloud-provider \
  -n ${NAMESPACE} --timeout=60s 2>/dev/null || true

echo "📦 Applying provider and autoscaler manifests..."
kubectl apply -f manifests/provider
kubectl apply -f manifests/autoscaler

echo "⏳ Waiting for provider to be ready..."
kubectl rollout status deployment/${DEPLOYMENT} -n ${NAMESPACE} --timeout=120s

echo "🔄 Rolling restart autoscaler..."
kubectl rollout restart deployment/cluster-autoscaler -n ${NAMESPACE}
kubectl rollout status deployment/cluster-autoscaler -n ${NAMESPACE} --timeout=120s

echo "🎉 Dev environment ready."
