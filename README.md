
# Verda Cloud Provider for Kubernetes Cluster Autoscaler

A gRPC-based cloud provider implementation that enables Kubernetes Cluster Autoscaler to manage GPU nodes on [Verda Cloud](https://verda.ai) (formerly DataCrunch). This allows Kubernetes clusters to dynamically scale GPU workloads to the cloud when local capacity is insufficient, and scale down during periods of inactivity to minimize costs.

## Overview

This project implements the [Kubernetes Cluster Autoscaler external gRPC cloud provider interface](https://github.com/kubernetes/autoscaler/blob/master/cluster-autoscaler/cloudprovider/externalgrpc/README.md), allowing seamless integration between Kubernetes and Verda Cloud's GPU infrastructure.

**Key Features:**
- Dynamic scaling of GPU nodes based on workload demand
- Automatic scale-down after configurable inactivity periods
- Support for multiple node groups with different instance types
- Cost-effective hybrid cloud bursting (local + cloud GPU resources)


## Project Structure

```
.
├── src/verda_cloud_provider/      # Main application source
│   ├── __init__.py
│   ├── main.py                    # gRPC server entrypoint
│   ├── provider.py                # CloudProvider service implementation
│   ├── settings.py                # Configuration models
│   ├── gen/                       # Generated gRPC code
│   │   └── externalgrpc/
│   │       ├── externalgrpc_pb2.py
│   │       └── externalgrpc_pb2_grpc.py
│   └── utils/
│       ├── logging.py
│       └── parse_args.py
├── proto/                         # Protocol buffer definitions
│   └── externalgrpc.proto
├── scripts/                       # Code generation scripts
│   └── generate_proto.sh
├── manifests/                     # Kubernetes deployment manifests
│   ├── provider/                  # Verda provider deployment
│   ├── autoscaler/                # Cluster autoscaler deployment
│   └── examples/                  # Example workloads for testing
├── config.yaml                    # Node group configuration
├── pyproject.toml                 # Python project configuration
├── Dockerfile                     # Container image definition
└── README.md
```

## Prerequisites

- Python 3.11+
- Verda Cloud account with API credentials
- Kubernetes cluster (v1.24+)
- Docker (for containerization)

## Quick Start

### 1. Install Dependencies

```bash
# Create virtual environment
python -m venv .venv
source .venv/bin/activate  # On Windows: .venv\Scripts\activate

# Install package
pip install -e .
```

### 2. Configure Node Groups

Create `config.yaml` with your node group definitions:

```yaml
node_groups:
  gpu-workers:
    instance_type: "1V100.6V"              # Verda instance type
    image: "ubuntu-24.04-cuda-12.8-open-docker"
    min_size: 0                            # Minimum nodes (0 for cost savings)
    max_size: 10                           # Maximum nodes
    location: "FIN-03"                     # Verda datacenter location
    ssh_key_ids:
      - "your-ssh-key-id"
    startup_script_id: "your-startup-script-id"  # Script to join K8s cluster
    hourly_price: 0.60
    contract: "PAY_AS_YOU_GO"
    pricing: "FIXED_PRICE"
    labels:
      verda.com/gpu: "v100"
      node-role.kubernetes.io/gpu: "true"
```

### 3. Set API Credentials

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

## Configuration

### Node Group Options

| Field | Type | Description |
|-------|------|-------------|
| `instance_type` | string | Verda instance type (e.g., "1V100.6V", "8V100.48V") |
| `image` | string | OS image with CUDA and Docker pre-installed |
| `min_size` | int | Minimum nodes (set to 0 for cost optimization) |
| `max_size` | int | Maximum nodes in group |
| `location` | string | Verda datacenter (FIN-01, FIN-03, etc.) |
| `ssh_key_ids` | list | SSH keys for instance access |
| `startup_script_id` | string | Script to configure and join K8s cluster |
| `hourly_price` | float | Expected hourly cost per instance |
| `labels` | dict | Kubernetes labels for node targeting |

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

[Your License Here]

## Resources

- [Verda Cloud Documentation](https://docs.verda.ai)
- [Kubernetes Cluster Autoscaler](https://github.com/kubernetes/autoscaler/tree/master/cluster-autoscaler)
- [External gRPC Provider Guide](https://github.com/kubernetes/autoscaler/blob/master/cluster-autoscaler/cloudprovider/externalgrpc/README.md)
- [Verda Python SDK](https://github.com/DataCrunch-io/datacrunch-python)
