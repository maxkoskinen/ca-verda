from k8s.io.api.core.v1 import generated_pb2 as core_v1
from k8s.io.apimachinery.pkg.api.resource import generated_pb2 as resource_pb2
from k8s.io.apimachinery.pkg.apis.meta.v1 import generated_pb2 as meta_v1
from verda_cloud_provider.gpu_types import (
    GPU_LABEL,
    gpu_operator_labels,
    gpu_spec_for_model,
)
from verda_cloud_provider.services.instance_metadata_service import InstanceTypeMetadata
from verda_cloud_provider.settings import NodeGroupConfig

_VALID_EFFECTS = {"NoSchedule", "PreferNoSchedule", "NoExecute"}


def _parse_taints(taints: dict[str, str]) -> list[core_v1.Taint]:
    result = []
    for key, raw in taints.items():
        parts = raw.rsplit(":", 1)
        if len(parts) == 2 and parts[1] in _VALID_EFFECTS:
            value, effect = parts
        else:
            value, effect = "", raw

        if effect not in _VALID_EFFECTS:
            raise ValueError(
                f"Unknown taint effect '{effect}' for key '{key}'. "
                f"Allowed: {_VALID_EFFECTS}"
            )
        result.append(core_v1.Taint(key=key, value=value, effect=effect))
    return result


class NodeTemplateService:
    def build(
        self,
        group_id: str,
        config: NodeGroupConfig,
        metadata: InstanceTypeMetadata,
    ) -> core_v1.Node:
        cpu_mc = metadata.cpu_cores * 1000
        mem_bytes = metadata.memory_gb * 1024**3
        cpu_reserved = min(100, int(cpu_mc * 0.06))
        mem_reserved = max(int(0.5 * 1024**3), int(mem_bytes * 0.05))

        capacity = {
            "cpu": resource_pb2.Quantity(string=f"{cpu_mc}m"),
            "memory": resource_pb2.Quantity(string=str(mem_bytes)),
            "pods": resource_pb2.Quantity(string="110"),
        }
        allocatable = {
            "cpu": resource_pb2.Quantity(string=f"{cpu_mc - cpu_reserved}m"),
            "memory": resource_pb2.Quantity(string=str(mem_bytes - mem_reserved)),
            "pods": resource_pb2.Quantity(string="110"),
        }

        gpu_spec = gpu_spec_for_model(metadata.gpu_model)
        if metadata.gpu_count > 0:
            gpu_qty = resource_pb2.Quantity(string=str(metadata.gpu_count))
            capacity["nvidia.com/gpu"] = gpu_qty
            allocatable["nvidia.com/gpu"] = gpu_qty

        # Apply override_resources – replaces any auto-detected value for the
        # given resource key in both capacity and allocatable.  This is
        # primarily used for MIG configurations where the number of advertised
        # GPU devices differs from the physical GPU count.
        for resource_name, quantity_str in config.override_resources.items():
            qty = resource_pb2.Quantity(string=str(quantity_str))
            capacity[resource_name] = qty
            allocatable[resource_name] = qty

        labels = {
            "verda.com/instance-type": config.instance_type,
            "verda.com/location": config.location,
            "topology.kubernetes.io/zone": "verda",
            "kubernetes.io/os": "linux",
            "kubernetes.io/arch": "amd64",
        }
        if gpu_spec:
            # Verda-specific label
            labels[GPU_LABEL] = gpu_spec.gpu_type
            # Standard NVIDIA GPU Operator / GPU Feature Discovery labels
            labels.update(gpu_operator_labels(
                gpu_spec,
                metadata.gpu_count,
                gpu_memory_gb=metadata.gpu_memory_gb,
            ))

        # User-supplied labels from the node group config take highest precedence
        for k, v in config.labels.items():
            labels[k] = v

        return core_v1.Node(
            metadata=meta_v1.ObjectMeta(
                name=f"{group_id}-template",
                labels=labels,
            ),
            spec=core_v1.NodeSpec(
                providerID=f"verda://{group_id}",
                unschedulable=False,
                taints=_parse_taints(config.taints),
            ),
            status=core_v1.NodeStatus(
                capacity=capacity,
                allocatable=allocatable,
            ),
        )
