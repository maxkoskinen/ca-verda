from k8s.io.api.core.v1 import generated_pb2 as core_v1
from k8s.io.apimachinery.pkg.api.resource import generated_pb2 as resource_pb2
from k8s.io.apimachinery.pkg.apis.meta.v1 import generated_pb2 as meta_v1
from verda_cloud_provider.gpu_types import GPU_LABEL, gpu_type_for_model
from verda_cloud_provider.instance_metadata_service import InstanceTypeMetadata
from verda_cloud_provider.settings import NodeGroupConfig


class NodeTemplateService:
    def build(
        self,
        group_id: str,
        config: NodeGroupConfig,
        metadata: InstanceTypeMetadata,
    ) -> core_v1.Node:
        cpu_mc = metadata.cpu_cores * 1000
        mem_bytes = metadata.memory_gb * 1024 ** 3
        cpu_reserved = min(100, int(cpu_mc * 0.06))
        mem_reserved = max(int(0.5 * 1024 ** 3), int(mem_bytes * 0.05))

        capacity = {
            "cpu":    resource_pb2.Quantity(string=f"{cpu_mc}m"),
            "memory": resource_pb2.Quantity(string=str(mem_bytes)),
            "pods":   resource_pb2.Quantity(string="110"),
        }
        allocatable = {
            "cpu":    resource_pb2.Quantity(string=f"{cpu_mc - cpu_reserved}m"),
            "memory": resource_pb2.Quantity(string=str(mem_bytes - mem_reserved)),
            "pods":   resource_pb2.Quantity(string="110"),
        }

        gpu_type = gpu_type_for_model(metadata.gpu_model)
        if metadata.gpu_count > 0:
            gpu_qty = resource_pb2.Quantity(string=str(metadata.gpu_count))
            capacity["nvidia.com/gpu"] = gpu_qty
            allocatable["nvidia.com/gpu"] = gpu_qty

        labels = {
            "node.kubernetes.io/instance-type": config.instance_type,
            "topology.kubernetes.io/zone":      config.location,
            "kubernetes.io/os":   "linux",
            "kubernetes.io/arch": "amd64",
        }
        if gpu_type:
            labels[GPU_LABEL] = gpu_type
            labels["nvidia.com/gpu.product"] = gpu_type
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
            ),
            status=core_v1.NodeStatus(
                capacity=capacity,
                allocatable=allocatable,
            ),
        )
