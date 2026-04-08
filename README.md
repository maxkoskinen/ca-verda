# Verda Cloud Provider for Kubernetes Cluster Autoscaler

A gRPC-based external cloud provider that enables the [Kubernetes Cluster Autoscaler](https://github.com/kubernetes/autoscaler/tree/master/cluster-autoscaler) to dynamically provision and manage GPU and CPU nodes on [Verda Cloud](https://verda.com) (formerly DataCrunch).

## What This Repo Contains

This repository implements the [Cluster Autoscaler external gRPC cloud provider interface](https://github.com/kubernetes/autoscaler/blob/master/cluster-autoscaler/cloudprovider/externalgrpc/README.md). It runs as a standalone gRPC service that the Cluster Autoscaler communicates with to scale node groups up and down on Verda Cloud infrastructure.

### What the Service Provides

- **Node group management** — Exposes configurable node groups (GPU and CPU instance types) to the Cluster Autoscaler, each with independent min/max size, location, and instance type settings.
- **Instance lifecycle** — Provisions new Verda Cloud instances when pods are pending due to insufficient capacity, and deletes them when the Cluster Autoscaler determines they are no longer needed.
- **Node bootstrapping** — Generates startup scripts (via Jinja2 templates) that automatically join newly provisioned instances to the Kubernetes cluster, including WireGuard tunnel setup for secure networking.
- **WireGuard networking** — Manages WireGuard peer configuration so cloud nodes can securely communicate with the on-premise cluster over an encrypted tunnel.
- **Instance metadata & pricing** — Fetches instance type metadata (CPU, memory, GPU resources) and pricing information from the Verda API so the autoscaler can make informed scheduling and cost-aware decisions.
- **Node cleanup** — Runs a lightweight background service that monitors and cleans up stale Kubernetes node objects for instances that no longer exist in the cloud.
- **mTLS support** — Optionally secures the gRPC endpoint with mutual TLS.

### Repository Structure

```
src/verda_cloud_provider/   # The cloud provider gRPC service
  main.py                   # Entry point — starts the gRPC server
  provider.py               # Top-level provider, initializes all services
  provider_methods.py       # gRPC method implementations (NodeGroups, IncreaseSize, DeleteNodes, etc.)
  settings.py               # Configuration models (node groups, WireGuard, Kubernetes)
  state_store.py            # Tracks node group target sizes and instance state
  gpu_types.py              # GPU type definitions
  services/                 # Internal services
    wg_service.py             # WireGuard peer management
    startup_script_service.py # Generates cloud-init scripts for new instances
    node_cleanup_service.py   # Background cleanup of orphaned k8s nodes
    node_template_service.py  # Builds node templates for the autoscaler
    instance_metadata_service.py  # Caches instance type metadata from Verda API

src/clusterautoscaler/      # Generated protobuf/gRPC stubs for the external provider interface
src/k8s/                    # Generated protobuf stubs for Kubernetes types

templates/                  # Jinja2 startup script templates (k3s node join, WireGuard setup)
manifests/                  # Kubernetes manifests
  provider/                   # Deployment manifests for the cloud provider service
  autoscaler/                 # Cluster Autoscaler deployment and config
  routemanager/               # DaemonSet for managing routes to cloud nodes via WireGuard
  examples/                   # Example GPU/CPU test workloads
deploy/
  gpu-operator/               # NVIDIA GPU Operator Helm values for cloud worker nodes
scripts/                    # Helper scripts (proto generation, dev deployment, GPU operator install)
```

## Getting Started

### Prerequisites

- Python 3.11+
- A [Verda Cloud](https://verda.com) account with API credentials
- A Kubernetes cluster (tested with k3s v1.32)
- Docker (for containerized deployment)

### Configuration

The provider is configured via a `config.yaml` file. A minimal node group looks like:

```yaml
kubernetes:
  endpoint: "<kubernetes api endpoint>"

node_groups:
  1V100-6V-FIN-01:
    instance_type: "1V100.6V"
    image: "ubuntu-24.04-cuda-12.6-docker"
    min_size: 0
    max_size: 10
    location: "FIN-01"
    ssh_key_ids:
      - "<your-ssh-key-id>"
    hourly_price: 0.60
    contract: "SPOT"
    pricing: "FIXED_PRICE"
    labels:
      verda.com/gpu: "true"
      node.kubernetes.io/role: "cloud-worker"
    taints:
      node-role.kubernetes.io/cloud: "true:NoSchedule"
```

API credentials are provided via environment variables or a `.env` file:

```
VERDA_CLIENT_ID=your-client-id
VERDA_CLIENT_SECRET=your-client-secret
```

### Running Locally

```bash
python -m venv .venv
source .venv/bin/activate
pip install -e .

verda-cloud-provider --config config.yaml --port 8086 --log-level INFO
```

### Docker

```bash
# Build
docker build -t verda-cloud-provider:latest .

# Run
docker run -d \
  --name verda-provider \
  -p 8086:8086 \
  -v $(pwd)/config.yaml:/config/config.yaml:ro \
  --env-file=.env \
  verda-cloud-provider:latest
```

A prebuilt image is also published to GHCR on tagged releases:

```
ghcr.io/maxkoskinen/ca-verda:latest
```

### Kubernetes Deployment

```bash
# Deploy the provider
kubectl apply -f manifests/provider/

# Deploy the Cluster Autoscaler
kubectl apply -f manifests/autoscaler/

# Deploy the route manager (for WireGuard routing on on-premise nodes)
kubectl apply -f manifests/routemanager/
```

## Contributions

The Kubernetes manifests for the **route manager** (`manifests/routemanager/`) base template and the **GPU Operator** Helm values (`deploy/gpu-operator/values.yaml`) were contributed by **Kimmo Rantala**.

## License

[MIT License](LICENSE)

## Resources

- [Verda Cloud Documentation](https://docs.verda.com/)
- [Kubernetes Cluster Autoscaler](https://github.com/kubernetes/autoscaler/tree/master/cluster-autoscaler)
- [External gRPC Cloud Provider Guide](https://github.com/kubernetes/autoscaler/blob/master/cluster-autoscaler/cloudprovider/externalgrpc/README.md)
- [Verda Python SDK](https://github.com/DataCrunch-io/datacrunch-python)
