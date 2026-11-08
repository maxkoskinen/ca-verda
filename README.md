# Verda external cloud provider for Cluster Autoscaler

This repository contains a Python implementation of the Kubernetes Cluster Autoscaler **external gRPC cloud-provider interface** for Verda Cloud (formerly DataCrunch). It lets Cluster Autoscaler provision and remove Verda worker nodes in an existing Kubernetes cluster. It is not a Kubernetes cloud-controller-manager, a second cluster scheduler, or an offloader to a separate HPC system.

The project was developed for Max Koskinen's 2026 master's thesis on bursting an on-premises K3s cluster to Verda GPU capacity. The thesis evaluation used an Aalto test cluster (three K3s servers and two local GPU workers), with a WireGuard gateway to cloud workers. Those environment-specific examples are not a turnkey deployment for another cluster.

## How it works

1. Cluster Autoscaler sees an eligible unschedulable Pod and calls the provider's external gRPC service on port 8086. Configured Verda instance types and locations become autoscaler node groups, with per-group minimum and maximum sizes.
2. On scale-out, the provider prepares a WireGuard peer and startup script and requests a Verda instance. The intended K3s agent joins the **same** on-premises cluster over the tunnel. The provider supplies synthetic node resources, labels and taints to autoscaler for scheduling simulation. Review the current tunnel bootstrap before relying on this sequence: the packaged template does not set an initial gateway `Endpoint` in its WireGuard peer configuration.
3. Once a node is ready and the NVIDIA GPU Operator has exposed its GPU resources, eligible Pods can run there. Cluster Autoscaler initiates scale-in when the node is unneeded; the provider removes its cloud instance and WireGuard peer. A background service cleans up stale Kubernetes node objects.

The thesis's intended placement policy uses `topology.kubernetes.io/zone=verda` and taint `node-role.kubernetes.io/cloud=true:NoSchedule`: cloud-bound Pods select the zone and tolerate the taint. An appropriate GPU request (for example `nvidia.com/gpu: 1`) is also needed for GPU workloads. The maximum node-group size limits the number of provisioned instances; this prototype does not implement a complete cost-admission policy. Review the actual node labels, MIG resource naming, and GPU Operator setup before using example workloads.

## Repository map

- `src/verda_cloud_provider/`: provider implementation, including gRPC methods, API metadata/availability caches, WireGuard management, startup-script rendering and cleanup.
- `src/verda_cloud_provider/templates/`: **packaged templates used at runtime** unless `script_template_path` overrides them. The top-level `templates/` directory contains older copies; do not assume it controls the deployed image.
- `src/clusterautoscaler/`, `src/k8s/`: generated protobuf stubs; `third_party/` contains source submodules for generation.
- `config.yaml`: a local example configuration, **not** the Aalto deployment configuration. `manifests/provider/aalto/configmap.yaml` and `manifests/provider/minikube/configmap.yaml` contain different environment-specific node groups and settings. Configured prices are examples, not live Verda quotations.
- `manifests/provider/aalto/`, `manifests/provider/minikube/`: alternative provider deployment examples; `manifests/autoscaler/` contains the autoscaler and optional monitoring resource; `manifests/routemanager/` is an Aalto-specific route example.
- `manifests/multi-run/`, `manifests/nvidia-test/`: workload manifests; `deploy/gpu-operator/`: GPU Operator values; `scripts/`: development and installation helpers.

## Getting started

**Prerequisites:** Python 3.11+, Verda API credentials, an existing Kubernetes cluster with a working path for cloud workers to join, and appropriate WireGuard/K3s/GPU configuration. Container deployment additionally requires Docker or another compatible builder. The thesis evaluated K3s `v1.32.3+k3s1`, but checked-in templates and autoscaler manifests may target different versions; do not treat the thesis version as a compatibility guarantee for the current tree.

The provider reads a YAML configuration file. At minimum it needs a Kubernetes endpoint and one node group, alongside Verda credentials in `VERDA_CLIENT_ID` and `VERDA_CLIENT_SECRET`. See `config.yaml` for the full **example** schema; adjust instance types, image names, locations, prices, SSH key IDs, network ranges, labels/taints and capacity bounds to your own environment. The Aalto ConfigMap has its own expanded configuration and must not be silently substituted for the root file.

```bash
python -m venv .venv
source .venv/bin/activate
pip install -e .
export VERDA_CLIENT_ID='<client-id>'
export VERDA_CLIENT_SECRET='<client-secret>'
verda-cloud-provider --config config.yaml --port 8086 --log-level INFO
```

Starting the service alone does **not** create a working cloud-bursting cluster. The selected template must match your Kubernetes distribution and network, and Cluster Autoscaler must be configured to connect to it. Do not commit credentials or a K3s join token.

To build the container image locally:

```bash
docker build -t verda-cloud-provider:local .
```

The Aalto manifest currently references `ghcr.io/maxkoskinen/ca-verda:latest`; verify the image and tag you intend to deploy. Review `manifests/provider/aalto/` or `manifests/provider/minikube/` **as alternatives**, not as a combined set. They require different Secrets and site settings. The route-manager manifest also contains site-specific routing and node selectors. See the manifest contents before applying anything to a cluster.

## Thesis evidence and limits

The thesis reports a smoke test in which a Pending CUDA `nbody` Pod triggered a cloud GPU node, completed successfully, and was followed by scale-down. A separate **single-node** heterogeneous batch on a two-A100 instance with MIG reported 21 GPU workload outcomes (20 Job completions plus one Ollama serving workload) and 12 CPU-only Job completions. The thesis labels Ollama as a completion in its results table, though the checked-in Ollama manifest is a long-running Deployment. These are observations of that test environment, **not** a provisioning-time SLA, cross-site MPI benchmark, production-scale throughput result or guarantee that the current manifests reproduce the historical run. In particular, the checked-in autoscaler flags, startup templates and some Job parallelism values differ from the thesis excerpts.

Cross-site storage, robust tunnel-failure handling, distributed jobs, fine-grained cost control, DRA and separate-cluster offloading are discussed as limitations or future work in the thesis, not demonstrated features of this repository.

## Contributions

The route-manager base manifest (`manifests/routemanager/`) and GPU Operator Helm values (`deploy/gpu-operator/values.yaml`) were contributed by Kimmo Rantala.

## License

See the repository's [GNU General Public License, version 3](LICENSE). The former README incorrectly called it MIT; this documentation change does not alter the license file.

## References

- Max Koskinen, 2026 master's thesis: implementation and architecture (sections on the Verda external provider and WireGuard networking), evaluation, and limitations.
- [Kubernetes Cluster Autoscaler](https://github.com/kubernetes/autoscaler/tree/master/cluster-autoscaler)
- [External gRPC cloud-provider interface](https://github.com/kubernetes/autoscaler/blob/master/cluster-autoscaler/cloudprovider/externalgrpc/README.md)
- [Verda Python SDK](https://github.com/DataCrunch-io/datacrunch-python)
