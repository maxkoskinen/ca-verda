#!/usr/bin/env bash
set -e

kubectl create namespace gpu-operator
kubectl label --overwrite namespace gpu-operator pod-security.kubernetes.io/enforce=privileged

helm repo add nvidia https://helm.ngc.nvidia.com/nvidia
helm repo update

helm upgrade --install gpu-operator \
  -n gpu-operator \
  --create-namespace \
  nvidia/gpu-operator \
  --version v26.3.0 \
  -f deploy/gpu-operator/values.yaml \
  --wait
