from __future__ import annotations

import logging
import uuid
from datetime import UTC, datetime
from typing import Any, override

import grpc
from grpc import ServicerContext
from verda import VerdaClient
from verda.constants import Actions
from verda.constants import InstanceStatus as VerdaInstanceStatus
from verda.instances import Contract, OSVolume

from clusterautoscaler.cloudprovider.v1.externalgrpc.externalgrpc_pb2 import (
    CleanupRequest,
    CleanupResponse,
    GetAvailableGPUTypesRequest,
    GetAvailableGPUTypesResponse,
    GPULabelRequest,
    GPULabelResponse,
    Instance,
    NodeGroup,
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
from k8s.io.apimachinery.pkg.apis.meta.v1 import generated_pb2 as meta_v1
from verda_cloud_provider.gpu_types import GPU_LABEL, gpu_type_for_model
from verda_cloud_provider.services.instance_metadata_service import (
    InstanceMetadataCache,
)
from verda_cloud_provider.services.node_cleanup_service import NodeCleanupService
from verda_cloud_provider.services.node_template_service import NodeTemplateService
from verda_cloud_provider.services.startup_script_service import StartupScriptService
from verda_cloud_provider.services.wg_service import WireguardService
from verda_cloud_provider.settings import AppConfig, NodeGroupConfig
from verda_cloud_provider.state_store import InstanceRecord, InstanceStateStore

logger = logging.getLogger(__name__)


class VerdaCloudProviderMethodsMixin(CloudProviderServicer):
    """
    gRPC method implementations for the Verda cloud provider.

    Inherits CloudProviderServicer so @override is statically verified.
    Attribute declarations below are supplied by VerdaCloudProvider.__init__.
    """

    client: VerdaClient
    app_config: AppConfig
    node_groups_config: dict[str, NodeGroupConfig]
    state_store: InstanceStateStore
    metadata_cache: InstanceMetadataCache
    template_service: NodeTemplateService
    startup_script_service: StartupScriptService
    wg_service: WireguardService | None
    node_cleanup_service: NodeCleanupService

    def _duration_hours(self, start: meta_v1.Time, end: meta_v1.Time) -> float:
        start_s = start.seconds + start.nanos / 1e9
        end_s = end.seconds + end.nanos / 1e9
        return max(0.0, (end_s - start_s) / 3600.0)

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

    # gRPC overrides

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
            wg_peer = None
            startup_script_id = None
            try:
                unique_suffix = str(uuid.uuid4())[:8]
                hostname = f"{group_id}-{unique_suffix}"

                if self.wg_service:
                    wg_peer = self.wg_service.reserve()
                    logger.debug(f"reserved wg peer: {wg_peer.tunnel_ip}")

                node_labels = {
                    "verda.com/instance-type": config.instance_type,
                    "verda.com/location": config.location,
                    **config.labels,
                }

                startup_script_id = self.startup_script_service.ensure_startup_script(
                    group_id=group_id,
                    labels=node_labels,
                    taints=config.taints,
                    wg=wg_peer,
                )

                logger.info(f"Creating instance {i + 1}/{delta}: {hostname}")

                on_spot_discontinue = "delete_permanently" if config.contract == "SPOT" else None
                os_volume = (
                    OSVolume(name=f"{hostname}-os", size=config.os_volume_gb, on_spot_discontinue=on_spot_discontinue)
                    if config.os_volume_gb
                    else None
                )

                instance = self.client.instances.create(
                    instance_type=config.instance_type,
                    image=config.image,
                    hostname=hostname,
                    description=f"Autoscaler node for {group_id}",
                    location=config.location,
                    ssh_key_ids=config.ssh_key_ids,
                    startup_script_id=startup_script_id,
                    os_volume=os_volume,
                    is_spot=False,
                    contract=config.contract,
                    # pricing=config.pricing
                )

                node_endpoint = None
                if (
                    hasattr(instance, "ip")
                    and instance.ip
                    and self.app_config.wireguard
                ):
                    node_endpoint = f"{instance.ip}:{self.app_config.wireguard.listen_port or str(5)}"

                if self.wg_service and wg_peer:
                    self.wg_service.commit(
                        reservation_id=wg_peer.reservation_id,
                        instance_id=instance.id,
                        node_endpoint=node_endpoint,
                    )

                # Track the instance
                record = InstanceRecord(
                    instance_id=instance.id,
                    hostname=hostname,
                    node_group=group_id,
                    provider_id=f"verda://{instance.id}",
                    created_at=datetime.now(UTC).isoformat(),
                    status="creating",
                    node_ip=getattr(instance, "ip", None),
                )

                self.state_store.add_instance(record)
                created_instances.append(instance.id)

                logger.info(
                    f"Created instance {instance.id} ({hostname}) {instance.ip}"
                )

            except Exception as e:
                logger.error(
                    f"Failed to create instance {i + 1}/{delta} for {group_id}: {e}"
                )

                if self.wg_service and wg_peer:
                    self.wg_service.release(wg_peer.reservation_id)
                if startup_script_id:
                    self.startup_script_service.delete_startup_script(startup_script_id)
                break

            # Script was consumed at boot — clean it up
            if startup_script_id:
                self.startup_script_service.delete_startup_script(startup_script_id)
                startup_script_id = None

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
                if not node.providerID or not node.providerID.startswith("verda://"):
                    logger.warning("Cannot find instance ID for node %s", node.name)
                    continue

                instance_id = node.providerID.removeprefix("verda://")

                logger.info("Deleting instance %s (node: %s)", instance_id, node.name)
                self.client.instances.action(instance_id, Actions.DELETE)

                # Only clean up WG peer and state after successful delete
                if self.wg_service:
                    self.wg_service.remove_peer(instance_id)

                self.state_store.remove_instance(instance_id)
                deleted_count += 1

            except Exception as e:
                logger.error(f"Failed to delete node {node.name}: {e}")

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
        """
        NodeGroupDecreaseTargetSize decreases the target size of the node group.
        This function doesn’t permit to delete any existing node and can be used only to
        reduce the request for new nodes that have not been yet fulfilled. Delta should be negative.
        It is assumed that cloud provider will not delete the existing nodes if the size when
        there is an option to just decrease the target.
        """
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
                        id=record.provider_id,
                        status=GRPCInstanceStatus(instanceState=status),
                    )
                )

            logger.debug(
                f"NodeGroupNodes({group_id}): {len(instances_proto)} instances"
            )

        except Exception as e:
            logger.error(f"Error fetching nodes for {group_id}: {e}")

        return NodeGroupNodesResponse(instances=instances_proto)

    @override
    def GetAvailableGPUTypes(
        self, request: GetAvailableGPUTypesRequest, context: ServicerContext
    ) -> GetAvailableGPUTypesResponse:
        gpu_types: dict[str, Any] = {}
        for meta in self.metadata_cache.get_all().values():
            gpu_type = gpu_type_for_model(meta.gpu_model)
            if gpu_type and meta.gpu_count > 0:
                gpu_types[gpu_type] = ""  # empty str is fine maybe
        return GetAvailableGPUTypesResponse(gpuTypes=gpu_types)

    @override
    def GPULabel(
        self, request: GPULabelRequest, context: ServicerContext
    ) -> GPULabelResponse:
        return GPULabelResponse(label=GPU_LABEL)

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
        try:
            self.node_cleanup_service.stop()
            logger.info("Node cleanup service stopped during Cleanup")
        except Exception as e:
            logger.error(f"Error stopping node cleanup service: {e}")

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

        hours = self._duration_hours(request.startTime, request.endTime)
        return PricingNodePriceResponse(price=cfg.hourly_price * hours)

    @override
    def NodeGroupTemplateNodeInfo(
        self, request: NodeGroupTemplateNodeInfoRequest, context: ServicerContext
    ):
        group_id = request.id

        # 1. Get config for this group
        if group_id not in self.node_groups_config:
            logger.warning(f"TemplateNodeInfo({group_id}): not found in config")
            context.set_code(grpc.StatusCode.NOT_FOUND)
            return NodeGroupTemplateNodeInfoResponse()

        config = self.node_groups_config[group_id]

        instance_type = config.instance_type
        instance_metadata = self.metadata_cache.get(instance_type)

        if not instance_metadata:
            logger.warning(f"TemplateNodeInfo({group_id}): no metadata for {instance_type}")
            context.set_code(grpc.StatusCode.INTERNAL)
            context.set_details(f"Group-id metadata not available: {config.instance_type}")
            return NodeGroupTemplateNodeInfoResponse()

        try:
            node = self.template_service.build(group_id, config, instance_metadata)
            logger.debug(
                f"TemplateNodeInfo({group_id}): labels={dict(node.metadata.labels)}, "
                f"capacity keys={list(node.status.capacity.keys())}, "
                f"allocatable keys={list(node.status.allocatable.keys())}"
            )
            return NodeGroupTemplateNodeInfoResponse(nodeInfo=node)
        except Exception:
            logger.exception(f"TemplateNodeInfo({group_id}): build() failed")
            context.set_code(grpc.StatusCode.INTERNAL)
            context.set_details(f"Failed to build template for {group_id}")
            return NodeGroupTemplateNodeInfoResponse()

    @override
    def NodeGroupGetOptions(
        self, request: NodeGroupAutoscalingOptionsRequest, context: ServicerContext
    ) -> NodeGroupAutoscalingOptionsResponse:
        return NodeGroupAutoscalingOptionsResponse(
            nodeGroupAutoscalingOptions=request.defaults
        )
