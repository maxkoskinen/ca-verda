import logging
import os
import uuid
from datetime import UTC, datetime
from typing import override

import grpc
from google.protobuf.timestamp_pb2 import Timestamp
from verda import VerdaClient
from verda.constants import Actions, InstanceStatus

from verda_cloud_provider.gen.externalgrpc import (
    externalgrpc_pb2,
    externalgrpc_pb2_grpc,
)
from verda_cloud_provider.settings import AppConfig
from verda_cloud_provider.state_store import InstanceRecord, InstanceStateStore

logger = logging.getLogger(__name__)


class VerdaCloudProvider(externalgrpc_pb2_grpc.CloudProviderServicer):
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

        self.target_sizes: dict[str, int] = {}
        self.state_store = InstanceStateStore()
        self.startup_script_id: str = ""
        self._initialize_target_sizes()
        self._initialize_startup_script()

    def _initialize_target_sizes(self):
        """Sync target sizes with actual cloud state on startup."""
        if not self.client:
            return

        try:
            instances = self.client.instances.get()

            # Sync state store with API
            self.state_store.sync_with_api(instances, self.node_groups_config)

            # Initialize target sizes
            for group_id, config in self.node_groups_config.items():
                tracked = self.state_store.get_by_group(group_id)
                count = len(tracked)
                self.target_sizes[group_id] = max(count, config.min_size)
                logger.info(
                    f"Initialized {group_id}: {count} instances, target={self.target_sizes[group_id]}"
                )

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
        self, request: externalgrpc_pb2.NodeGroupsRequest, context: grpc.ServicerContext
    ) -> externalgrpc_pb2.NodeGroupsResponse:
        """Return list of configured node groups."""

        groups: list[externalgrpc_pb2.NodeGroup] = []
        for name, config in self.node_groups_config.items():
            groups.append(
                externalgrpc_pb2.NodeGroup(
                    id=name,
                    minSize=config.min_size,
                    maxSize=config.max_size,
                    debug=f"Verda Group {config.instance_type}",
                )
            )
        return externalgrpc_pb2.NodeGroupsResponse(nodeGroups=groups)

    @override
    def NodeGroupForNode(
        self,
        request: externalgrpc_pb2.NodeGroupForNodeRequest,
        context: grpc.ServicerContext,
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
            return externalgrpc_pb2.NodeGroupForNodeResponse(
                nodeGroup=externalgrpc_pb2.NodeGroup(
                    id=rec.node_group,
                    minSize=cfg.min_size,
                    maxSize=cfg.max_size,
                    debug="Mapped by providerID",
                )
            )

        return externalgrpc_pb2.NodeGroupForNodeResponse()

    @override
    def NodeGroupTargetSize(
        self,
        request: externalgrpc_pb2.NodeGroupTargetSizeRequest,
        context: grpc.ServicerContext,
    ) -> externalgrpc_pb2.NodeGroupTargetSizeResponse:
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
        return externalgrpc_pb2.NodeGroupTargetSizeResponse(targetSize=number_of_nodes)

    @override
    def NodeGroupIncreaseSize(
        self,
        request: externalgrpc_pb2.NodeGroupIncreaseSizeRequest,
        context: grpc.ServicerContext,
    ) -> externalgrpc_pb2.NodeGroupIncreaseSizeResponse:
        """
        NodeGroupIncreaseSize increases the size of the node group.
        This function should wait until node group size is updated.
        """
        group_id = request.id
        delta = request.delta

        if group_id not in self.node_groups_config:
            context.set_code(grpc.StatusCode.NOT_FOUND)
            context.set_details(f"Node group '{group_id}' not found")
            return externalgrpc_pb2.NodeGroupIncreaseSizeResponse()

        config = self.node_groups_config[group_id]
        current_target = len(self.state_store.get_by_group(group_id))
        new_target = current_target + delta

        if new_target > config.max_size:
            context.set_code(grpc.StatusCode.RESOURCE_EXHAUSTED)
            context.set_details(
                f"Max size {config.max_size} exceeded (requested: {new_target})"
            )
            return externalgrpc_pb2.NodeGroupIncreaseSizeResponse()

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
        if actual_increase > 0:
            self.target_sizes[group_id] = current_target + actual_increase
            logger.info(
                f"Updated {group_id} target size to {self.target_sizes[group_id]} ({actual_increase}/{delta} successful)"
            )

        if actual_increase < delta:
            context.set_code(grpc.StatusCode.ABORTED)
            context.set_details(
                f"Only {actual_increase}/{delta} instances created successfully"
            )

        return externalgrpc_pb2.NodeGroupIncreaseSizeResponse()

    @override
    def NodeGroupDeleteNodes(
        self,
        request: externalgrpc_pb2.NodeGroupDeleteNodesRequest,
        context: grpc.ServicerContext,
    ) -> externalgrpc_pb2.NodeGroupDeleteNodesResponse:
        """Delete specific nodes from the node group."""
        group_id = request.id
        nodes_to_delete = request.nodes

        if group_id not in self.target_sizes:
            context.set_code(grpc.StatusCode.NOT_FOUND)
            context.set_details(f"Node group '{group_id}' not found")
            return externalgrpc_pb2.NodeGroupDeleteNodesResponse()

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
            self.target_sizes[group_id] = max(
                0, self.target_sizes[group_id] - deleted_count
            )
            logger.info(
                f"Deleted {deleted_count} nodes from {group_id}, new target: {self.target_sizes[group_id]}"
            )

        return externalgrpc_pb2.NodeGroupDeleteNodesResponse()

    @override
    def NodeGroupDecreaseTargetSize(
        self,
        request: externalgrpc_pb2.NodeGroupDecreaseTargetSizeRequest,
        context: grpc.ServicerContext,
    ) -> externalgrpc_pb2.NodeGroupDecreaseTargetSizeResponse:
        group_id = request.id
        delta = request.delta

        if group_id in self.target_sizes:
            new_size = self.target_sizes[group_id] + delta
            self.target_sizes[group_id] = max(0, new_size)

        return externalgrpc_pb2.NodeGroupDecreaseTargetSizeResponse()

    @override
    def NodeGroupNodes(
        self,
        request: externalgrpc_pb2.NodeGroupNodesRequest,
        context: grpc.ServicerContext,
    ) -> externalgrpc_pb2.NodeGroupNodesResponse:
        """NodeGroupNodes returns a list of all nodes that belong to this node group."""
        group_id = request.id
        instances_proto: list[externalgrpc_pb2.Instance] = []

        try:
            # Use state store for reliable tracking
            tracked_instances = self.state_store.get_by_group(group_id)

            # Fetch current state from API
            all_instances = {i.id: i for i in self.client.instances.get()}

            for record in tracked_instances:
                api_instance = all_instances.get(record.instance_id)

                if not api_instance:
                    # Instance deleted outside of autoscaler - mark as deleting
                    status = (
                        externalgrpc_pb2.InstanceStatus.InstanceState.instanceDeleting
                    )
                else:
                    # Map Verda status to proto status
                    status = self._map_instance_status(api_instance.status)

                instances_proto.append(
                    externalgrpc_pb2.Instance(
                        id=record.instance_id,
                        status=externalgrpc_pb2.InstanceStatus(instanceState=status),
                    )
                )

            logger.debug(
                f"NodeGroupNodes({group_id}): {len(instances_proto)} instances"
            )

        except Exception as e:
            logger.error(f"Error fetching nodes for {group_id}: {e}")

        return externalgrpc_pb2.NodeGroupNodesResponse(instances=instances_proto)

    def _map_instance_status(
        self, verda_status: str
    ) -> externalgrpc_pb2.InstanceStatus.InstanceState:
        """Map Verda instance status to gRPC proto status."""
        status_map = {
            InstanceStatus.RUNNING: externalgrpc_pb2.InstanceStatus.InstanceState.instanceRunning,
            InstanceStatus.PROVISIONING: externalgrpc_pb2.InstanceStatus.InstanceState.instanceCreating,
            InstanceStatus.OFFLINE: externalgrpc_pb2.InstanceStatus.InstanceState.instanceCreating,
            InstanceStatus.ORDERED: externalgrpc_pb2.InstanceStatus.InstanceState.instanceCreating,
        }
        return status_map.get(
            verda_status, externalgrpc_pb2.InstanceStatus.InstanceState.unspecified
        )

    @override
    def GPULabel(
        self, request: externalgrpc_pb2.GPULabelRequest, context: grpc.ServicerContext
    ) -> externalgrpc_pb2.GPULabelResponse:
        return externalgrpc_pb2.GPULabelResponse(label="verda.com/gpu")

    @override
    def Refresh(
        self, request: externalgrpc_pb2.RefreshRequest, context: grpc.ServicerContext
    ) -> externalgrpc_pb2.RefreshResponse:
        """Refresh is called before every main loop - sync with cloud state."""
        try:
            # Fetch current instances from Verda API
            instances = self.client.instances.get()

            # Reconcile state store
            self.state_store.sync_with_api(instances, self.node_groups_config)

            # Update target sizes based on actual state
            for group_id in self.node_groups_config:
                tracked = self.state_store.get_by_group(group_id)
                actual_count = len(tracked)

                # Only update if there's a discrepancy
                if self.target_sizes.get(group_id, 0) != actual_count:
                    logger.warning(
                        f"Target size mismatch for {group_id}: "
                        f"expected {self.target_sizes.get(group_id, 0)}, actual {actual_count}"
                    )
                    # Trust the actual state from API
                    self.target_sizes[group_id] = actual_count

            logger.debug("Refresh completed successfully")

        except Exception as e:
            logger.error(f"Refresh failed: {e}")

        return externalgrpc_pb2.RefreshResponse()

    @override
    def Cleanup(
        self, request: externalgrpc_pb2.CleanupRequest, context: grpc.ServicerContext
    ) -> externalgrpc_pb2.CleanupResponse:
        """Clean up resources on shutdown."""

        return externalgrpc_pb2.CleanupResponse()

    @override
    def PricingNodePrice(
        self,
        request: externalgrpc_pb2.PricingNodePriceRequest,
        context: grpc.ServicerContext,
    ):
        rec = self.state_store.get_by_provider_id(request.node.providerID)
        if not rec:
            context.set_code(grpc.StatusCode.NOT_FOUND)
            context.set_details("Unknown node/providerID")
            return externalgrpc_pb2.PricingNodePriceResponse()

        cfg = self.node_groups_config[rec.node_group]
        if not getattr(cfg, "hourly_price", None):
            context.set_code(grpc.StatusCode.UNIMPLEMENTED)
            context.set_details("No price configured")
            return externalgrpc_pb2.PricingNodePriceResponse()

        hours = self._duration_hours(request.startTimestamp, request.endTimestamp)
        return externalgrpc_pb2.PricingNodePriceResponse(price=cfg.hourly_price * hours)
