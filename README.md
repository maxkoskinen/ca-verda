
# Verda Cloud Provider for Kubernetes Cluster Autoscaler

A gRPC-based cloud provider implementation that enables Kubernetes Cluster Autoscaler to manage GPU nodes on [Verda Cloud](https://verda.com) (formerly DataCrunch). This allows Kubernetes clusters to dynamically scale GPU workloads to the cloud when local capacity is insufficient, and scale down during periods of inactivity to minimize costs.

## Overview

This project implements the [Kubernetes Cluster Autoscaler external gRPC cloud provider interface](https://github.com/kubernetes/autoscaler/blob/master/cluster-autoscaler/cloudprovider/externalgrpc/README.md), allowing seamless integration between Kubernetes and Verda Cloud's GPU infrastructure.

**Key Features:**
- Dynamic scaling of GPU nodes based on workload demand
- Automatic scale-down after configurable inactivity periods
- Support for multiple node groups with different instance types


## Prerequisites

- Python 3.11+
- Verda Cloud account with API credentials
- Kubernetes cluster (v1.35)
- Docker (for containerization)

## Quick Start

### 1. Install Dependencies

```bash
# Create virtual environment
python -m venv .venv
source .venv/bin/activate

# Install package
pip install -e .
```

### 2. Configure Node Groups

Create `config.yaml` with your node group definitions:

```yaml
  CPU.4V.16G-FIN-03:
    instance_type: "CPU.4V.16G"                   # Verda instance type
    image: "ubuntu-24.04-cuda-12.6-docker"        # image
    min_size: 0                                   # min size of node group
    max_size: 10                                  # max size of node group
    location: "FIN-03"                            # Verda location typically FIN-[01-03]
    ssh_key_ids:
      - "15629643-9ac2-4893-b7db-e5a2efd700cd"
    hourly_price: 0.60                            # fallback cost / hour - also dynamically fetched from verda api
    contract: "SPOT" 
    pricing: "FIXED_PRICE"
    labels:                                       # Own lables
      verda.com/gpu: "false"
      k8s.io/role: "cloud-worker"
    resources:                                    # manually definied version - also fetched from verda api
      cpu: 4
      memory_gb: 16
      gpu_count: 0
```

### 3. Set API Credentials

Api credentials can be set in .env file or as environmnet variables

```bash
# Create .env file
cat > .env << EOF
VERDA_CLIENT_ID=your-client-id
VERDA_CLIENT_SECRET=your-client-secret
EOF

chmod 600 .env
```

### 4. Run Locally

```bash
verda-cloud-provider --config config.yaml --port 8086 --log-level INFO
```

## Docker Deployment

### Build Image

```bash
docker build -t verda-cloud-provider:latest .
```

### Run Container

```bash
docker run -d \
  --name verda-provider \
  -p 8086:8086 \
  -v $(pwd)/config.yaml:/config/config.yaml:ro \
  --env-file=.env \
  verda-cloud-provider:latest
```

Or use prebuilt Docker image
```bash
docker run -d \
  --name verda-provider \
  -p 8086:8086 \
  -v $(pwd)/config.yaml:/config/config.yaml:ro \
  --env-file=.env \
  ghcr.io/maxkoskinen/ca-verda:latest
```

## Kubernetes Deployment

### 1. Deploy Provider

```bash
# Update credentials in manifests/provider/secret.yaml
kubectl apply -f manifests/provider/
```

### 2. Deploy Cluster Autoscaler

```bash
kubectl apply -f manifests/autoscaler/
```

### 3. Verify Deployment

```bash
# Check provider logs
kubectl logs -n kube-system -l app=verda-cloud-provider

# Check autoscaler logs
kubectl logs -n kube-system -l app=cluster-autoscaler

# List node groups
kubectl get nodes -o wide
```

## Testing Autoscaling

Deploy a GPU workload that exceeds local capacity:

```bash
# Deploy example GPU job
kubectl apply -f manifests/examples/gpu-test-job.yaml

# Watch for scale-up
kubectl get pods -w
kubectl get nodes -w

# Check autoscaler events
kubectl describe pod <pending-pod-name>
```


### Autoscaler Parameters

Key cluster-autoscaler flags (configured in `manifests/autoscaler/deployment.yaml`):

- `--scale-down-enabled=true` - Enable automatic scale-down
- `--scale-down-unneeded-time=5m` - Wait time before removing unused nodes
- `--scale-down-delay-after-add=5m` - Delay after scale-up before scale-down
- `--max-node-provision-time=15m` - Maximum time to wait for node provisioning

## Development

### Regenerate gRPC Code

```bash
# Install protoc compiler
# See: https://grpc.io/docs/protoc-installation/

# Generate Python code from proto definitions
./scripts/generate_proto.sh
```


## License

[MIT License](LICENSE)

## Resources

- [Verda Cloud Documentation](https://docs.verda.com/)
- [Kubernetes Cluster Autoscaler](https://github.com/kubernetes/autoscaler/tree/master/cluster-autoscaler)
- [External gRPC Provider Guide](https://github.com/kubernetes/autoscaler/blob/master/cluster-autoscaler/cloudprovider/externalgrpc/README.md)
- [Verda Python SDK](https://github.com/DataCrunch-io/datacrunch-python)
