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
from verda_cloud_provider.settings import AppConfig
from verda_cloud_provider.state_store import InstanceRecord, InstanceStateStore

logger = logging.getLogger(__name__)


class VerdaCloudProvider(CloudProviderServicer):
    def __init__(self, config_path: str):
        client_id = os.environ.get("VERDA_CLIENT_ID", "")
        client_secret = os.environ.get("VERDA_CLIENT_SECRET", "")
        if not client_id or not client_secret:
            raise ValueError(
                "VERDA_CLIENT_ID and VERDA_CLIENT_SECRET env vars must be set"
            )

        self.client: VerdaClient = VerdaClient(client_id, client_secret)

        try:
            self.app_config = AppConfig.load(config_path)
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
        self._initialize()
        self._initialize_startup_script()

    def _initialize(self):
        """Sync target sizes with actual cloud state on startup."""
        if not self.client:
            return

        try:
            instances = self.client.instances.get()
            # Sync state store with API
            self.state_store.sync_with_api(instances, self.node_groups_config)

        except Exception as e:
            logger.error(f"Failed to initialize target sizes: {e}")

    def _initialize_startup_script(self) -> None:
        """
        Ensure the standard kubeadm/join startup script exists in Verda.

        Logic:
        - Read local scripts/verda_init.sh
        - Compare (content-equal) with existing startup scripts in Verda
        - If a match exists, reuse its ID
        - Otherwise, create a new startup script and store its ID
        """
        if not self.client:
            return

        # Already initialized
        if self.startup_script_id:
            return

        script_path = "scripts/verda_init.sh"
        try:
            with open(script_path, "r", encoding="utf-8") as f:
                local_script = f.read()
        except FileNotFoundError:
            logger.error("Local startup script not found at %s", script_path)
            return
        except Exception:
            logger.exception("Failed to read local startup script")
            return

        # Helper to normalize script text (ignore minor whitespace differences)
        def _normalize(content: str) -> str:
            lines = content.replace("\r\n", "\n").splitlines()
            # Optionally drop comment-only lines to be less strict
            normalized = []
            for line in lines:
                stripped = line.strip()
                if not stripped:
                    continue
                # if stripped.startswith("#"):
                #     continue
                normalized.append(stripped)
            return "\n".join(normalized)

        local_norm = _normalize(local_script)

        try:
            matched_id: str | None = None

            # List existing startup scripts from Verda
            scripts = self.client.startup_scripts.get()
            for script in scripts:
                try:
                    remote_norm = _normalize(script.script)
                except AttributeError:
                    logger.debug(
                        "Startup script object missing 'script' field: %r", script
                    )
                    continue

                if remote_norm == local_norm:
                    matched_id = script.id
                    logger.info(
                        "Reusing existing startup script %s for verda_init.sh",
                        script.id,
                    )
                    break

            if matched_id is None:
                logger.info("No matching startup script found, creating a new one")

                new_script = self.client.startup_scripts.create(
                    name="k8s-verda-init",
                    script=local_script,
                )
                matched_id = new_script.id
                logger.info("Created new startup script with id %s", matched_id)

            self.startup_script_id = matched_id

            # Optionally, if you want to default group configs with no explicit startup_script_id:
            for group_id, cfg in self.node_groups_config.items():
                if not cfg.startup_script_id:
                    logger.info(
                        "Setting default startup_script_id=%s for node group %s",
                        matched_id,
                        group_id,
                    )
                    cfg.startup_script_id = matched_id

        except Exception:
            logger.exception("Exception occurred while initializing startup script")

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
                    startup_script_id=self.startup_script_id,
                    contract=config.contract,
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
        if not getattr(cfg, "hourly_price", None):
            context.set_code(grpc.StatusCode.UNIMPLEMENTED)
            context.set_details("No price configured")
            return PricingNodePriceResponse()

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

        # 2. Build the Node object using YOUR generated protobuf classes
        # Note: Resource quantities in proto are strings or specific messages,
        # but k8s often expects the Quantity wrapper if complex.
        # For simple CPU/Mem, strings usually work if the proto definition allows it,
        # otherwise you need the resource_pb2.Quantity.

        node = core_v1.Node(
            metadata=meta_v1.ObjectMeta(
                name=f"{group_id}-template",
                # labels={
                #    "node.kubernetes.io/instance-type": config.instance_type,
                #    "topology.kubernetes.io/zone": config.location,
                # }
            ),
            status=core_v1.NodeStatus(
                capacity={
                    "cpu": resource_pb2.Quantity(string="4000m"),
                    "memory": resource_pb2.Quantity(string="16Gi"),
                    "pods": resource_pb2.Quantity(string="110"),
                },
                allocatable={
                    "cpu": resource_pb2.Quantity(string="4000m"),
                    "memory": resource_pb2.Quantity(string="15Gi"),
                    "pods": resource_pb2.Quantity(string="110"),
                },
            ),
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
