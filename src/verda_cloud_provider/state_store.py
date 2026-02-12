import json
import logging
from dataclasses import asdict, dataclass
from datetime import UTC, datetime
from pathlib import Path

logger = logging.getLogger(__name__)


@dataclass
class InstanceRecord:
    instance_id: str  # verda instance id
    hostname: str  # group_id-[rand-uuid[8]]
    node_group: str  # group_id
    provider_id: str  # verda://<instance_id>
    created_at: str
    status: str


class InstanceStateStore:
    """Lightweight persistence for instance tracking."""

    def __init__(self, state_file: str = "instances.json"):
        self.state_file = Path(state_file)
        self.state_file.parent.mkdir(parents=True, exist_ok=True)
        self._cache: dict[str, InstanceRecord] = {}
        self._load()

    def _load(self):
        """Load state from disk."""
        if self.state_file.exists():
            try:
                with open(self.state_file, "r") as f:
                    data = json.load(f)
                    self._cache = {k: InstanceRecord(**v) for k, v in data.items()}
            except Exception as e:
                logger.error(f"Failed to load state: {e}")
                self._cache = {}

    def _save(self):
        """Persist state to disk."""
        try:
            with open(self.state_file, "w") as f:
                data = {k: asdict(v) for k, v in self._cache.items()}
                json.dump(data, f, indent=2)
        except Exception as e:
            logger.error(f"Failed to save state: {e}")

    def add_instance(self, record: InstanceRecord):
        """Track a new instance."""
        self._cache[record.instance_id] = record
        self._save()

    def get_instance(self, instance_id: str) -> InstanceRecord | None:
        """Get instance by ID."""
        return self._cache.get(instance_id)

    def get_by_group(self, node_group: str) -> list[InstanceRecord]:
        """Get all instances in a node group."""
        return [r for r in self._cache.values() if r.node_group == node_group]

    def remove_instance(self, instance_id: str):
        """Remove instance from tracking."""
        if instance_id in self._cache:
            del self._cache[instance_id]
            self._save()

    def sync_with_api(self, api_instances: list, node_groups_config: dict):
        """Reconcile local state with Verda API."""
        api_ids = {i.id for i in api_instances}
        local_ids = set(self._cache.keys())

        # Remove instances that no longer exist
        for instance_id in local_ids - api_ids:
            logger.info(f"Removing deleted instance {instance_id} from state")
            self.remove_instance(instance_id)

        # Add instances that aren't tracked yet
        for instance in api_instances:
            if instance.id not in local_ids:
                # Try to determine node group from hostname
                node_group = None
                for group_id in node_groups_config:
                    if instance.hostname.startswith(f"{group_id}-"):
                        node_group = group_id
                        break

                if node_group:
                    record = InstanceRecord(
                        instance_id=instance.id,
                        hostname=instance.hostname,
                        node_group=node_group,
                        provider_id=f"verda://{instance.id}",
                        created_at=datetime.now(tz=UTC).isoformat(),
                        status=instance.status,
                    )
                    self.add_instance(record)

    def get_by_provider_id(self, provider_id: str) -> InstanceRecord | None:
        """Get instance record by Kubernetes providerID (verda://<instance_id>)."""
        prefix = "verda://"
        if not provider_id or not provider_id.startswith(prefix):
            return None
        instance_id = provider_id[len(prefix) :]
        return self.get_instance(instance_id)
