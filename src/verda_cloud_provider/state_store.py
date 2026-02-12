import logging
from dataclasses import dataclass
from datetime import UTC, datetime
from threading import RLock

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class InstanceRecord:
    instance_id: str  # Verda instance id
    hostname: str  # typically: "{group_id}-{rand}"
    node_group: str  # group_id
    provider_id: str  # "verda://{instance_id}"
    created_at: str
    status: str


class InstanceStateStore:
    """
    In-memory instance tracking.

    Source of truth is the Verda API. This cache is rebuilt/adjusted via
    sync_with_api() (called on startup + Refresh()).
    """

    def __init__(self) -> None:
        self._lock = RLock()
        self._cache: dict[str, InstanceRecord] = {}

    def add_instance(self, record: InstanceRecord) -> None:
        with self._lock:
            self._cache[record.instance_id] = record

    def get_instance(self, instance_id: str) -> InstanceRecord | None:
        with self._lock:
            return self._cache.get(instance_id)

    def get_by_group(self, node_group: str) -> list[InstanceRecord]:
        with self._lock:
            return [r for r in self._cache.values() if r.node_group == node_group]

    def remove_instance(self, instance_id: str) -> None:
        with self._lock:
            self._cache.pop(instance_id, None)

    def get_by_provider_id(self, provider_id: str) -> InstanceRecord | None:
        prefix = "verda://"
        if not provider_id or not provider_id.startswith(prefix):
            return None
        instance_id = provider_id[len(prefix) :]
        return self.get_instance(instance_id)

    def sync_with_api(self, api_instances: list, node_groups_config: dict) -> None:
        """
        Reconcile local cache with Verda API result.

        - Removes local records not present in API anymore
        - Adds new API instances if their hostname indicates a known nodegroup
        - Updates status/hostname if changed
        """
        with self._lock:
            api_by_id = {i.id: i for i in api_instances}

            # Remove instances that no longer exist
            for instance_id in list(self._cache.keys()):
                if instance_id not in api_by_id:
                    logger.info("Removing deleted instance %s from state", instance_id)
                    self._cache.pop(instance_id, None)

            # Add/update instances from API
            for instance_id, inst in api_by_id.items():
                # Determine node group from hostname prefix: "{group_id}-..."
                node_group = None
                hostname = getattr(inst, "hostname", "") or ""
                for group_id in node_groups_config.keys():
                    if hostname.startswith(f"{group_id}-"):
                        node_group = group_id
                        break

                if not node_group:
                    continue  # not managed by this provider

                created_at = datetime.now(tz=UTC).isoformat()
                status = getattr(inst, "status", "") or ""

                existing = self._cache.get(instance_id)
                if existing is None:
                    self._cache[instance_id] = InstanceRecord(
                        instance_id=instance_id,
                        hostname=hostname,
                        node_group=node_group,
                        provider_id=f"verda://{instance_id}",
                        created_at=created_at,
                        status=status,
                    )
                else:
                    # Update mutable fields (status/hostname) while preserving created_at
                    if (
                        existing.hostname != hostname
                        or existing.status != status
                        or existing.node_group != node_group
                    ):
                        self._cache[instance_id] = InstanceRecord(
                            instance_id=existing.instance_id,
                            hostname=hostname,
                            node_group=node_group,
                            provider_id=existing.provider_id,
                            created_at=existing.created_at,
                            status=status,
                        )
