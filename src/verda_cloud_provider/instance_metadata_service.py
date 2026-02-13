import logging
from dataclasses import dataclass
from threading import RLock
from typing import Dict, Optional

from verda import VerdaClient

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class InstanceTypeMetadata:
    """Metadata for a Verda instance type."""
    instance_type: str
    cpu_cores: int
    memory_gb: int
    gpu_memory_gb: int
    gpu_count: int
    gpu_model: str | None
    current_spot_price: float | None
    current_ondemand_price: float | None


class InstanceMetadataCache:
    """
    Simple cache for instance type metadata from Verda API.
    Updated via explicit refresh() calls (no background threads).
    """

    def __init__(self, client: VerdaClient):
        self.client = client
        self._lock = RLock()
        self._cache: Dict[str, InstanceTypeMetadata] = {}

    def get(self, instance_type: str) -> Optional[InstanceTypeMetadata]:
        """
        Get cached metadata for an instance type.

        Args:
            instance_type: The Verda instance type (e.g., "1V100.6V")

        Returns:
            InstanceTypeMetadata or None if not found
        """
        with self._lock:
            return self._cache.get(instance_type)

    def get_all(self) -> Dict[str, InstanceTypeMetadata]:
        """Get all cached metadata."""
        with self._lock:
            return dict(self._cache)

    def refresh(self) -> None:
        """
        Fetch latest instance type metadata from Verda API.
        Called by the provider's Refresh() method.
        """
        try:
            instance_types = self.client.instance_types.get()

            new_cache = {}
            for instance in instance_types:

                metadata = InstanceTypeMetadata(
                    instance_type=instance.instance_type,
                    cpu_cores=instance.cpu.get("number_of_cores",0),
                    memory_gb=instance.memory.get("size_in_gigabytes",0),
                    gpu_memory_gb=instance.gpu_memory.get("size_in_gigabytes",0),
                    gpu_count=instance.gpu.get("number_of_gpus", 0),
                    gpu_model=instance.gpu.get("description", None),
                    current_spot_price=instance.spot_price_per_hour,
                    current_ondemand_price=instance.price_per_hour,
                )

                new_cache[instance.instance_type] = metadata

            with self._lock:
                self._cache = new_cache

            logger.info("Refreshed metadata for %d instance types", len(new_cache))

        except Exception as e:
            logger.error("Failed to refresh instance metadata: %s", e, exc_info=True)
            # Keep existing cache on error
