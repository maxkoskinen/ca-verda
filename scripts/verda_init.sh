#!/bin/bash

set -euo pipefail

export DEBIAN_FRONTEND=noninteractive
export HOME=/root/

# ---- Fill these in via Verda startup script variables ----
K8S_ENDPOINT="x.x.x.x:6443"
K8S_TOKEN="abcdef.0123456789abcdef"
K8S_CA_HASH="sha256:0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"

# Get instance ID from Verda metadata service
INSTANCE_ID="$(cat /var/lib/cloud/data/instance-id 2>/dev/null || true)"
# -------------------------------------------

apt-get update
apt-get upgrade -y
apt-get install -y ca-certificates curl gpg apt-transport-https

# Kernel modules + sysctl needed for Kubernetes networking
cat <<'EOF' >/etc/modules-load.d/k8s.conf
overlay
br_netfilter
EOF

modprobe overlay
modprobe br_netfilter

cat <<'EOF' >/etc/sysctl.d/99-kubernetes-cri.conf
net.bridge.bridge-nf-call-iptables = 1
net.bridge.bridge-nf-call-ip6tables = 1
net.ipv4.ip_forward = 1
EOF

sysctl --system

# Install containerd
apt-get install -y containerd
mkdir -p /etc/containerd
containerd config default >/etc/containerd/config.toml

# Enable systemd cgroup driver (required for kubeadm)
sed -i 's/SystemdCgroup = false/SystemdCgroup = true/' /etc/containerd/config.toml

systemctl enable --now containerd

# Kubernetes apt repo (v1.30)
install -m 0755 -d /etc/apt/keyrings
curl -fsSL https://pkgs.k8s.io/core:/stable:/v1.30/deb/Release.key | \
  gpg --dearmor -o /etc/apt/keyrings/kubernetes-apt-keyring.gpg
chmod 0644 /etc/apt/keyrings/kubernetes-apt-keyring.gpg

echo "deb [signed-by=/etc/apt/keyrings/kubernetes-apt-keyring.gpg] https://pkgs.k8s.io/core:/stable:/v1.30/deb/ /" \
  | tee /etc/apt/sources.list.d/kubernetes.list > /dev/null

apt-get update
apt-get install -y kubelet kubeadm kubectl
apt-mark hold kubelet kubeadm kubectl

# Configure kubelet with cloud-provider=external and providerID
cat <<EOF >/etc/default/kubelet
KUBELET_EXTRA_ARGS=--cloud-provider=external --provider-id=verda://${INSTANCE_ID}
EOF

systemctl daemon-reload
systemctl enable --now kubelet

# Join cluster if not already joined
if [ ! -f /etc/kubernetes/kubelet.conf ]; then
  echo "Joining Kubernetes cluster..."
  kubeadm join "${K8S_ENDPOINT}" \
    --token "${K8S_TOKEN}" \
    --discovery-token-ca-cert-hash "${K8S_CA_HASH}"

  echo "Successfully joined cluster!"
else
  echo "Node already joined to cluster"
fi
