import logging
import os
import uuid
from datetime import UTC, datetime
from typing import override

import grpc
from google.protobuf.timestamp_pb2 import Timestamp
from grpc import ServicerContext
from verda import VerdaClient
from verda.constants import Actions
from verda.constants import InstanceStatus as VerdaInstanceStatus

from clusterautoscaler.cloudprovider.v1.externalgrpc.externalgrpc_pb2 import (
    CleanupRequest,
    CleanupResponse,
    GPULabelRequest,
    GPULabelResponse,
    Instance,
    NodeGroup,
    NodeGroupAutoscalingOptions,
    NodeGroupAutoscalingOptionsRequest,
    NodeGroupAutoscalingOptionsResponse,
    NodeGroupDecreaseTargetSizeRequest,
    NodeGroupDecreaseTargetSizeResponse,
    NodeGroupDeleteNodesRequest,
    NodeGroupDeleteNodesResponse,
    NodeGroupForNodeRequest,
    NodeGroupForNodeResponse,
    NodeGroupIncreaseSizeRequest,
    NodeGroupIncreaseSizeResponse,
    NodeGroupNodesRequest,
    NodeGroupNodesResponse,
    NodeGroupsRequest,
    NodeGroupsResponse,
    NodeGroupTargetSizeRequest,
    NodeGroupTargetSizeResponse,
    NodeGroupTemplateNodeInfoRequest,
    NodeGroupTemplateNodeInfoResponse,
    PricingNodePriceRequest,
    PricingNodePriceResponse,
    RefreshRequest,
    RefreshResponse,
)
from clusterautoscaler.cloudprovider.v1.externalgrpc.externalgrpc_pb2 import (
    InstanceStatus as GRPCInstanceStatus,
)
from clusterautoscaler.cloudprovider.v1.externalgrpc.externalgrpc_pb2_grpc import (
    CloudProviderServicer,
)
from k8s.io.api.core.v1 import generated_pb2 as core_v1
from k8s.io.apimachinery.pkg.api.resource import generated_pb2 as resource_pb2
from k8s.io.apimachinery.pkg.apis.meta.v1 import generated_pb2 as meta_v1
from verda_cloud_provider.instance_metadata_service import InstanceMetadataCache
from verda_cloud_provider.settings import AppConfig
from verda_cloud_provider.startup_script_service import StartupScriptService
from verda_cloud_provider.state_store import InstanceRecord, InstanceStateStore

logger = logging.getLogger(__name__)


class VerdaCloudProvider(CloudProviderServicer):
    def __init__(self, app_config: AppConfig):
        client_id = os.environ.get("VERDA_CLIENT_ID", "")
        client_secret = os.environ.get("VERDA_CLIENT_SECRET", "")
        if not client_id or not client_secret:
            raise ValueError(
                "VERDA_CLIENT_ID and VERDA_CLIENT_SECRET env vars must be set"
            )

        self.client: VerdaClient = VerdaClient(client_id, client_secret)
        self.metadata_cache = InstanceMetadataCache(self.client)

        try:
            self.app_config = app_config
            self.node_groups_config = self.app_config.node_groups
            logging.info(
                f"Loaded configuration for {len(self.node_groups_config)} node groups."
            )

        # 2. Define Node Groups Configuration
        except Exception as e:
            logging.critical(f"Failed to load configuration: {e}")
            raise e

        self.state_store = InstanceStateStore()
        self.startup_script_id: str = ""

        self.startup_script_service = StartupScriptService(
            client=self.client,
            template_path="templates/verda_init.sh.j2",
            k8s_config=self.app_config.kubernetes
        )
        self._initialize()

    def _initialize(self):
        """Sync target sizes with actual cloud state on startup."""
        if not self.client:
            return

        try:
            instances = self.client.instances.get()
            # Sync state store with API
            self.state_store.sync_with_api(instances, self.node_groups_config)
            # Refresh metadata
            self.metadata_cache.refresh()

        except Exception as e:
            logger.error(f"Failed to initialize: {e}")


    def _duration_hours(self, start: Timestamp, end: Timestamp) -> float:
        start_s = start.ToDatetime().timestamp()
        end_s = end.ToDatetime().timestamp()
        return max(0.0, (end_s - start_s) / 3600.0)

    @override
    def NodeGroups(
        self, request: NodeGroupsRequest, context: ServicerContext
    ) -> NodeGroupsResponse:
        """Return list of configured node groups."""

        groups: list[NodeGroup] = []
        for name, config in self.node_groups_config.items():
            groups.append(
                NodeGroup(
                    id=name,
                    minSize=config.min_size,
                    maxSize=config.max_size,
                    debug=f"Verda Group {config.instance_type}",
                )
            )
        return NodeGroupsResponse(nodeGroups=groups)

    @override
    def NodeGroupForNode(
        self,
        request: NodeGroupForNodeRequest,
        context: ServicerContext,
    ):
        """
        NodeGroupForNode returns the node group for the given node.
        The node group id is an empty string if the node should not be
        processed by cluster autoscaler.
        """
        node = request.node

        rec = self.state_store.get_by_provider_id(node.providerID)
        if rec:
            cfg = self.node_groups_config[rec.node_group]
            return NodeGroupForNodeResponse(
                nodeGroup=NodeGroup(
                    id=rec.node_group,
                    minSize=cfg.min_size,
                    maxSize=cfg.max_size,
                    debug="Mapped by providerID",
                )
            )

        return NodeGroupForNodeResponse()

    @override
    def NodeGroupTargetSize(
        self,
        request: NodeGroupTargetSizeRequest,
        context: ServicerContext,
    ) -> NodeGroupTargetSizeResponse:
        """
        NodeGroup specific RPC functions
        NodeGroupTargetSize returns the current target size of the node group.
        It is possible that the number of nodes in Kubernetes is different
        at the moment but should be equal to the size of a node group once everything stabilizes
        (new nodes finish startup and registration or removed nodes are deleted completely).
        """
        group_id = request.id

        nodes = self.state_store.get_by_group(group_id)
        number_of_nodes = len(nodes)
        return NodeGroupTargetSizeResponse(targetSize=number_of_nodes)

    @override
    def NodeGroupIncreaseSize(
        self,
        request: NodeGroupIncreaseSizeRequest,
        context: ServicerContext,
    ) -> NodeGroupIncreaseSizeResponse:
        """
        NodeGroupIncreaseSize increases the size of the node group.
        This function should wait until node group size is updated.
        """
        group_id = request.id
        delta = request.delta

        if group_id not in self.node_groups_config:
            context.set_code(grpc.StatusCode.NOT_FOUND)
            context.set_details(f"Node group '{group_id}' not found")
            return NodeGroupIncreaseSizeResponse()

        config = self.node_groups_config[group_id]

        try:
            startup_script_id = self.startup_script_service.ensure_startup_script(
                group_id=group_id,
                labels=config.labels
            )
        except Exception as e:
            logger.error(f"Failed to prepare startup script: {e}")
            context.set_code(grpc.StatusCode.INTERNAL)
            context.set_details(f"Startup script error: {e}")
            return NodeGroupIncreaseSizeResponse()

        current_target = len(self.state_store.get_by_group(group_id))
        new_target = current_target + delta

        if new_target > config.max_size:
            context.set_code(grpc.StatusCode.RESOURCE_EXHAUSTED)
            context.set_details(
                f"Max size {config.max_size} exceeded (requested: {new_target})"
            )
            return NodeGroupIncreaseSizeResponse()

        logger.info(
            f"Increasing {group_id} by {delta} nodes (current: {current_target}, target: {new_target})"
        )

        # Track successful creations
        created_instances = []

        for i in range(delta):
            try:
                unique_suffix = str(uuid.uuid4())[:8]
                hostname = f"{group_id}-{unique_suffix}"

                logger.info(f"Creating instance {i + 1}/{delta}: {hostname}")

                instance = self.client.instances.create(
                    instance_type=config.instance_type,
                    image=config.image,
                    hostname=hostname,
                    description=f"Autoscaler node for {group_id}",
                    location=config.location,
                    ssh_key_ids=config.ssh_key_ids,
                    startup_script_id=startup_script_id,
                    contract=config.contract,
                    pricing=config.pricing
                )

                # Track the instance
                record = InstanceRecord(
                    instance_id=instance.id,
                    hostname=hostname,
                    node_group=group_id,
                    provider_id=f"verda://{instance.id}",
                    created_at=datetime.now(UTC).isoformat(),
                    status="creating",
                )
                self.state_store.add_instance(record)
                created_instances.append(instance.id)

                logger.info(f"Created instance {instance.id} ({hostname})")

            except Exception as e:
                logger.error(
                    f"Failed to create instance {i + 1}/{delta} for {group_id}: {e}"
                )
                break

        # Update target size to reflect actual successful creations
        actual_increase = len(created_instances)

        if actual_increase < delta:
            context.set_code(grpc.StatusCode.ABORTED)
            context.set_details(
                f"Only {actual_increase}/{delta} instances created successfully"
            )

        return NodeGroupIncreaseSizeResponse()

    @override
    def NodeGroupDeleteNodes(
        self,
        request: NodeGroupDeleteNodesRequest,
        context: ServicerContext,
    ) -> NodeGroupDeleteNodesResponse:
        """Delete specific nodes from the node group."""
        group_id = request.id
        nodes_to_delete = request.nodes

        if group_id not in self.node_groups_config:
            context.set_code(grpc.StatusCode.NOT_FOUND)
            context.set_details(f"Node group '{group_id}' not found")
            return NodeGroupDeleteNodesResponse()

        logger.info(f"Deleting {len(nodes_to_delete)} nodes from {group_id}")

        deleted_count = 0

        for node in nodes_to_delete:
            try:
                instance_id = None

                if node.providerID and node.providerID.startswith("verda://"):
                    instance_id = node.providerID.replace("verda://", "")
                else:
                    logger.warning(f"Cannot find instance ID for node {node.name}")
                    continue

                if not instance_id:
                    logger.error(f"No instance ID found for node {node.name}")
                    continue

                # Delete the instance
                logger.info(f"Deleting instance {instance_id} (node: {node.name})")
                self.client.instances.action(instance_id, Actions.DELETE)

                # Remove from state store
                self.state_store.remove_instance(instance_id)

                deleted_count += 1

            except Exception as e:
                logger.error(f"Failed to delete node {node.name}: {e}")

        # Update target size
        if deleted_count > 0:
            new_target = len(self.state_store.get_by_group(group_id))
            logger.info(
                "Deleted %d nodes from %s, new target: %d",
                deleted_count,
                group_id,
                new_target,
            )

        return NodeGroupDeleteNodesResponse()

    @override
    def NodeGroupDecreaseTargetSize(
        self,
        request: NodeGroupDecreaseTargetSizeRequest,
        context: ServicerContext,
    ) -> NodeGroupDecreaseTargetSizeResponse:
        group_id = request.id
        if group_id not in self.node_groups_config:
            context.set_code(grpc.StatusCode.NOT_FOUND)
            context.set_details(f"Node group '{group_id}' not found")

        return NodeGroupDecreaseTargetSizeResponse()

    @override
    def NodeGroupNodes(
        self,
        request: NodeGroupNodesRequest,
        context: ServicerContext,
    ) -> NodeGroupNodesResponse:
        """NodeGroupNodes returns a list of all nodes that belong to this node group."""
        group_id = request.id
        instances_proto: list[Instance] = []

        try:
            # Use state store for reliable tracking
            tracked_instances = self.state_store.get_by_group(group_id)

            # Fetch current state from API
            all_instances = {i.id: i for i in self.client.instances.get()}

            for record in tracked_instances:
                api_instance = all_instances.get(record.instance_id)

                if not api_instance:
                    # Instance deleted outside of autoscaler - mark as deleting
                    status = GRPCInstanceStatus.InstanceState.instanceCreating
                else:
                    # Map Verda status to proto status
                    status = self._map_instance_status(api_instance.status)

                instances_proto.append(
                    Instance(
                        id=record.instance_id,
                        status=GRPCInstanceStatus(instanceState=status),
                    )
                )

            logger.debug(
                f"NodeGroupNodes({group_id}): {len(instances_proto)} instances"
            )

        except Exception as e:
            logger.error(f"Error fetching nodes for {group_id}: {e}")

        return NodeGroupNodesResponse(instances=instances_proto)

    def _map_instance_status(
        self, verda_status: str
    ) -> GRPCInstanceStatus.InstanceState:
        """Map Verda instance status to gRPC proto status."""
        status_map = {
            VerdaInstanceStatus.RUNNING: GRPCInstanceStatus.InstanceState.instanceRunning,
            VerdaInstanceStatus.PROVISIONING: GRPCInstanceStatus.InstanceState.instanceCreating,
            VerdaInstanceStatus.OFFLINE: GRPCInstanceStatus.InstanceState.instanceCreating,
            VerdaInstanceStatus.ORDERED: GRPCInstanceStatus.InstanceState.instanceCreating,
        }
        return status_map.get(
            verda_status, GRPCInstanceStatus.InstanceState.unspecified
        )

    @override
    def GPULabel(
        self, request: GPULabelRequest, context: ServicerContext
    ) -> GPULabelResponse:
        return GPULabelResponse(label="verda.com/gpu")

    @override
    def Refresh(
        self, request: RefreshRequest, context: ServicerContext
    ) -> RefreshResponse:
        """Refresh is called before every main loop - sync with cloud state."""
        try:
            # Fetch current instances from Verda API
            instances = self.client.instances.get()

            # Reconcile state store
            self.state_store.sync_with_api(instances, self.node_groups_config)

            # Get metadata for instances
            self.metadata_cache.refresh()

            logger.debug("Refresh completed successfully")

        except Exception as e:
            logger.error(f"Refresh failed: {e}")

        return RefreshResponse()

    @override
    def Cleanup(
        self, request: CleanupRequest, context: ServicerContext
    ) -> CleanupResponse:
        """Clean up resources on shutdown."""

        return CleanupResponse()

    @override
    def PricingNodePrice(
        self,
        request: PricingNodePriceRequest,
        context: ServicerContext,
    ):
        rec = self.state_store.get_by_provider_id(request.node.providerID)
        if not rec:
            context.set_code(grpc.StatusCode.NOT_FOUND)
            context.set_details("Unknown node/providerID")
            return PricingNodePriceResponse()

        cfg = self.node_groups_config[rec.node_group]

        metadata = self.metadata_cache.get(cfg.instance_type)
        hourly_price = None

        if metadata:
            if cfg.pricing == "DYNAMIC_PRICE":
                hourly_price = metadata.current_spot_price
            else:
                hourly_price = metadata.current_ondemand_price

        if hourly_price is None:
            hourly_price = cfg.hourly_price

        hours = self._duration_hours(request.startTimestamp, request.endTimestamp)
        return PricingNodePriceResponse(price=cfg.hourly_price * hours)

    @override
    def NodeGroupTemplateNodeInfo(
        self, request: NodeGroupTemplateNodeInfoRequest, context: ServicerContext
    ):
        group_id = request.id

        # 1. Get config for this group
        if group_id not in self.node_groups_config:
            context.set_code(grpc.StatusCode.NOT_FOUND)
            return NodeGroupTemplateNodeInfoResponse()

        config = self.node_groups_config[group_id]

        instance_type = config.instance_type
        instance_metadata = self.metadata_cache.get(instance_type)

        if not instance_metadata:
            logger.warning(f"No metadata for group-id: {group_id}")
            context.set_code(grpc.StatusCode.INTERNAL)
            context.set_details(f"Group-id metadata not available: {config.instance_type}")
            return NodeGroupTemplateNodeInfoResponse()

        cpu_cores = instance_metadata.cpu_cores
        memory_gb = instance_metadata.memory_gb
        # gpu_count = instance_metadata.gpu_count
        # gpu_memory = instance_metadata.gpu_memory_gb
        # gpu_model = instance_metadata.gpu_model

        # Calculate capacity and allocatable
        cpu_millicores = cpu_cores * 1000
        memory_bytes = memory_gb * 1024 * 1024 * 1024

        # Reserve resources for system (kubelet, OS, etc.)
        cpu_reserved_millicores = min(100, int(cpu_millicores * 0.06))  # 6% or 100m
        memory_reserved_gb = max(0.5, memory_gb * 0.05)  # 5% or 0.5GB

        allocatable_cpu_millicores = cpu_millicores - cpu_reserved_millicores
        allocatable_memory_gb = memory_gb - memory_reserved_gb

        # Build capacity resources
        capacity = {
            "cpu": resource_pb2.Quantity(string=f"{cpu_millicores}m"),
            "memory": resource_pb2.Quantity(string=f"{int(memory_bytes)}"),
            "pods": resource_pb2.Quantity(string="110"),
        }
        # Build allocatable resources
        allocatable = {
            "cpu": resource_pb2.Quantity(string=f"{allocatable_cpu_millicores}m"),
            "memory": resource_pb2.Quantity(string=f"{int(allocatable_memory_gb * 1024 * 1024 * 1024)}"),
            "pods": resource_pb2.Quantity(string="110"),
        }

        metadata = meta_v1.ObjectMeta(name=f"{group_id}-template")

        # Assign labels
        metadata.labels["node.kubernetes.io/instance-type"] = config.instance_type
        metadata.labels["node.kubernetes.io/zone"] = config.location

        for key, val in config.labels.items():
            metadata.labels[key] = val
        # build node status
        nodeStatus = core_v1.NodeStatus(
            capacity=capacity,
            allocatable=allocatable
        )

        node = core_v1.Node(
            metadata=metadata,
            status=nodeStatus
        )

        # 3. Serialize to bytes
        node_bytes = node.SerializeToString()

        # 4. Return wrapped in the response
        return NodeGroupTemplateNodeInfoResponse(nodeBytes=node_bytes)

    @override
    def NodeGroupGetOptions(
        self, request: NodeGroupAutoscalingOptionsRequest, context: ServicerContext
    ):
        context.set_code(grpc.StatusCode.UNIMPLEMENTED)
        context.set_details("NodeGroupTemplateNodeInfo not implemented")
        return NodeGroupAutoscalingOptionsResponse()
