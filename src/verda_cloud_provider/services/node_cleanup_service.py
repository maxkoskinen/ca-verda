"""
Lightweight CCM-style node controller for Verda Cloud.

Periodically reconciles Kubernetes Node objects against the Verda API.
Nodes whose backing cloud instances no longer exist are deleted from the
cluster so they don't linger in NotReady state forever.

This fills the gap left by the absence of a proper Cloud Controller Manager
for the Verda cloud platform.
"""

from __future__ import annotations

import logging
import os
import threading
from datetime import UTC, datetime, timedelta

from kubernetes import client as k8s_client
from kubernetes import config as k8s_config
from kubernetes.client.rest import ApiException
from urllib3.contrib.socks import SOCKSProxyManager
from verda import VerdaClient

from verda_cloud_provider.services.wg_service import WireguardService
from verda_cloud_provider.state_store import InstanceStateStore

logger = logging.getLogger(__name__)

# Prefix used on all Verda-managed node providerIDs.
_PROVIDER_ID_PREFIX = "verda://"

# How long a node must be NotReady before we consider removing it.
# Prevents racing with a node that's still booting.
_NOT_READY_GRACE_PERIOD = timedelta(minutes=5)

# Default interval between reconciliation sweeps.
_DEFAULT_POLL_INTERVAL_SECONDS = 60

# Timeout in seconds for Kubernetes API requests so that out-of-cluster
# connections don't hang the reconciliation thread indefinitely.
_K8S_REQUEST_TIMEOUT_SECONDS = 30


class NodeCleanupService:
    """
    Background thread that garbage-collects Kubernetes Node objects
    whose backing Verda instances have been deleted.

    Covers:
      - Nodes deleted via the autoscaler (NodeGroupDeleteNodes)
      - Out-of-band deletions (spot preemptions, manual API deletes, etc.)
    """

    def __init__(
        self,
        verda_client: VerdaClient,
        state_store: InstanceStateStore,
        wg_service: WireguardService | None = None,
        poll_interval: int = _DEFAULT_POLL_INTERVAL_SECONDS,
        node_groups_config: dict | None = None,
    ) -> None:
        self._verda_client = verda_client
        self._state_store = state_store
        self._wg_service = wg_service
        self._poll_interval = poll_interval
        self._node_groups_config = node_groups_config or {}

        self._stop_event = threading.Event()
        self._thread: threading.Thread | None = None

        # Initialise the Kubernetes API client.
        self._k8s_available = False
        try:
            k8s_config.load_incluster_config()
            logger.info("NodeCleanupService: using in-cluster kubeconfig")
            self._k8s_available = True
        except k8s_config.ConfigException:
            try:
                k8s_config.load_kube_config()
                logger.info("NodeCleanupService: using local kubeconfig")
                self._k8s_available = True
            except k8s_config.ConfigException:
                logger.warning(
                    "NodeCleanupService: no kubeconfig found — "
                    "node cleanup will not function"
                )

        socks_proxy = os.environ.get("K8S_SOCKS_PROXY")
        api_client = k8s_client.ApiClient()

        if socks_proxy:
            cfg = api_client.configuration
            ssl_kwargs = {}
            if cfg.verify_ssl:
                ssl_kwargs["cert_reqs"] = "CERT_REQUIRED"
                if cfg.ssl_ca_cert:
                    ssl_kwargs["ca_certs"] = cfg.ssl_ca_cert
            else:
                ssl_kwargs["cert_reqs"] = "CERT_NONE"
            if cfg.cert_file:
                ssl_kwargs["cert_file"] = cfg.cert_file
            if cfg.key_file:
                ssl_kwargs["key_file"] = cfg.key_file

            api_client.rest_client.pool_manager = SOCKSProxyManager(
                socks_proxy,
                num_pools=4,
                **ssl_kwargs,
            )
            logger.info(f"NodeCleanupService: using SOCKS proxy {socks_proxy}")

        self._k8s = k8s_client.CoreV1Api(api_client=api_client)

    # ------------------------------------------------------------------
    # Lifecycle
    # ------------------------------------------------------------------

    def start(self) -> None:
        """Start the background reconciliation loop."""
        if not self._k8s_available:
            logger.warning(
                "NodeCleanupService: skipping start — no kubeconfig available"
            )
            return

        if self._thread is not None and self._thread.is_alive():
            logger.warning("NodeCleanupService already running")
            return

        self._stop_event.clear()
        self._thread = threading.Thread(
            target=self._run_loop,
            name="verda-node-cleanup",
            daemon=True,
        )
        self._thread.start()
        logger.info(f"NodeCleanupService started (poll every {self._poll_interval}ds)")

    def stop(self) -> None:
        """Signal the background thread to stop and wait for it."""
        self._stop_event.set()
        if self._thread is not None:
            self._thread.join(timeout=self._poll_interval + 5)
            self._thread = None
        logger.info("NodeCleanupService stopped")

    # ------------------------------------------------------------------
    # Main loop
    # ------------------------------------------------------------------

    def _run_loop(self) -> None:
        logger.info(
            "NodeCleanupService: background thread started, waiting 10s before first sweep"
        )
        # Short initial delay to let the cluster settle on startup.
        self._stop_event.wait(10)

        while not self._stop_event.is_set():
            logger.info("NodeCleanupService: starting reconciliation sweep")
            try:
                self.reconcile()
                logger.info("NodeCleanupService: reconciliation sweep completed")
            except Exception:
                logger.exception("NodeCleanupService: reconcile sweep failed")

            self._stop_event.wait(self._poll_interval)

    # ------------------------------------------------------------------
    # Reconciliation
    # ------------------------------------------------------------------

    def reconcile(self) -> None:
        """
        Single reconciliation pass:

        1.  List all K8s nodes whose providerID starts with ``verda://``.
        2.  Fetch all live Verda instances in one API call.
        3.  For each managed node that has *no* matching instance AND has
            been NotReady beyond the grace period, delete the K8s Node
            object (and tidy up local state / WireGuard).
        """
        k8s_nodes = self._list_verda_nodes()
        logger.info(
            f"NodeCleanupService: found {len(k8s_nodes)} Verda-managed K8s node(s)"
        )
        if not k8s_nodes:
            return

        live_instance_ids = self._fetch_live_instance_ids()
        if live_instance_ids is None:
            # Verda API unreachable — skip this sweep to avoid
            # accidentally deleting every node.
            logger.warning("Skipping reconciliation: Verda API unreachable")
            return

        logger.info(
            f"NodeCleanupService: {len(live_instance_ids)} live Verda instance(s) from API"
        )

        now = datetime.now(UTC)

        for node in k8s_nodes:
            provider_id: str = node.spec.provider_id or ""
            instance_id = provider_id.removeprefix(_PROVIDER_ID_PREFIX)
            node_name = node.metadata.name
            ready = self._is_node_ready(node)

            if instance_id in live_instance_ids:
                logger.info(
                    f"NodeCleanupService: node {node_name} (instance {instance_id}) — instance alive, ready={ready}"
                )
                continue  # Instance still exists — nothing to do.

            # Instance is gone. Check the grace period before removing.
            if not self._past_grace_period(node, now):
                last_ts = self._last_transition_time(node)
                logger.info(
                    f"NodeCleanupService: node {node_name} (instance {instance_id}) — instance GONE, "
                    f"ready={ready}, lastTransition={last_ts} — still within grace period, skipping"
                )
                continue

            logger.info(
                f"Node {node_name} (instance {instance_id}) has no backing Verda instance — "
                f"deleting from cluster"
            )
            self._delete_kubernetes_node(node_name, instance_id)

    # ------------------------------------------------------------------
    # Kubernetes helpers
    # ------------------------------------------------------------------

    def _list_verda_nodes(self) -> list:
        """Return K8s Node objects that have a verda:// providerID."""
        logger.info(
            f"NodeCleanupService: listing K8s nodes (timeout={_K8S_REQUEST_TIMEOUT_SECONDS}ds)..."
        )
        try:
            nodes = self._k8s.list_node(
                _request_timeout=_K8S_REQUEST_TIMEOUT_SECONDS,
            )
        except ApiException as exc:
            logger.error(f"Failed to list Kubernetes nodes: {exc.reason}")
            return []
        except Exception as exc:
            logger.error(f"Failed to list Kubernetes nodes (non-API error): {exc}")
            return []

        managed: list = []
        for node in nodes.items:
            pid = (node.spec.provider_id or "") if node.spec else ""
            if pid.startswith(_PROVIDER_ID_PREFIX):
                managed.append(node)
        return managed

    @staticmethod
    def _is_node_ready(node) -> bool:
        """Return True if the node currently has condition Ready=True."""
        if not node.status or not node.status.conditions:
            return False
        for cond in node.status.conditions:
            if cond.type == "Ready":
                return cond.status == "True"
        return False

    @staticmethod
    def _last_transition_time(node) -> datetime | None:
        """
        Return the last time the Ready condition transitioned,
        which approximates when the node became NotReady.
        """
        if not node.status or not node.status.conditions:
            return None
        for cond in node.status.conditions:
            if cond.type == "Ready" and cond.last_transition_time:
                # The k8s client returns a datetime object already.
                ts = cond.last_transition_time
                if isinstance(ts, datetime):
                    if ts.tzinfo is None:
                        return ts.replace(tzinfo=UTC)
                    return ts
        return None

    def _past_grace_period(self, node, now: datetime) -> bool:
        """
        Return True if we are confident the node should be cleaned up.

        A node that is still Ready is never cleaned up here — the Verda
        instance may have just been deleted and kubelet hasn't noticed yet.
        We wait for it to go NotReady *and* stay that way for the grace
        period before removing it.
        """
        if self._is_node_ready(node):
            return False

        last_transition = self._last_transition_time(node)
        if last_transition is None:
            # No transition time — be safe and skip.
            return False

        return (now - last_transition) >= _NOT_READY_GRACE_PERIOD

    def _delete_kubernetes_node(self, node_name: str, instance_id: str) -> None:
        """Delete a Node object from the Kubernetes API and tidy up."""
        logger.info(
            f"NodeCleanupService: sending delete request for K8s node {node_name} ..."
        )
        try:
            self._k8s.delete_node(
                name=node_name,
                body=k8s_client.V1DeleteOptions(
                    grace_period_seconds=0,
                    propagation_policy="Foreground",
                ),
                _request_timeout=_K8S_REQUEST_TIMEOUT_SECONDS,
            )
            logger.info(f"Deleted Kubernetes node {node_name}")
        except ApiException as exc:
            if exc.status == 404:
                logger.info(f"Node {node_name} already removed (404)")
            else:
                logger.error(
                    f"Failed to delete node {node_name}: {exc.status} {exc.reason}"
                )
                return  # Don't clean up state if the node is still there.
        except Exception as exc:
            logger.error(f"Failed to delete node {node_name} (non-API error): {exc}")
            return

        # Clean up WireGuard peer if applicable.
        if self._wg_service:
            try:
                self._wg_service.remove_peer(instance_id)
            except Exception:
                logger.warning(
                    f"Failed to remove WireGuard peer for instance {instance_id}",
                    exc_info=True,
                )

        # Remove from in-memory state store so target sizes stay correct.
        self._state_store.remove_instance(instance_id)

    # ------------------------------------------------------------------
    # Verda helpers
    # ------------------------------------------------------------------

    def _fetch_live_instance_ids(self) -> set[str] | None:
        """
        Fetch all instance IDs currently known to the Verda API.

        Returns None when the API is unreachable so the caller can
        skip the reconciliation sweep entirely — returning an empty set
        would be indistinguishable from "all instances are gone" and
        would cause every managed node to be deleted.
        """
        logger.info("NodeCleanupService: fetching live instances from Verda API...")
        try:
            instances = self._verda_client.instances.get()
            return {inst.id for inst in instances}
        except Exception:
            logger.exception("Failed to fetch instances from Verda API")
            return None
