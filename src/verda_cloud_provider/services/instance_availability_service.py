import logging
from threading import RLock
from typing import Dict

from verda import VerdaClient

logger = logging.getLogger(__name__)


# Key is (instance_type, location_code)
AvailabilityKey = tuple[str, str]


class InstanceAvailabilityCache:
    """
    Cache for instance type availability from Verda API.
    Updated via explicit refresh() calls during the Refresh() loop.

    The Verda API returns a list of dicts from get_availabilities().
    We parse these into a lookup keyed by (instance_type, location) -> bool.
    """

    def __init__(self, client: VerdaClient):
        self.client = client
        self._lock = RLock()
        # Maps (instance_type, location) -> True if available
        self._cache: Dict[AvailabilityKey, bool] = {}
        # Track whether we've ever successfully loaded data
        self._loaded: bool = False

    def is_available(self, instance_type: str, location: str) -> bool:
        """
        Check if an instance type is available at a given location.

        If availability data has never been loaded successfully,
        we assume available (fail-open) so we don't block scaling
        just because the availability API was unreachable.
        """
        with self._lock:
            if not self._loaded:
                return True  # fail-open
            return self._cache.get((instance_type, location), False)

    def refresh(self) -> None:
        """
        Fetch latest availability data from Verda API.
        Called by the provider's Refresh() method.
        """
        try:
            raw_availabilities = self.client.instances.get_availabilities()

            new_cache: Dict[AvailabilityKey, bool] = {}
            for entry in raw_availabilities:
                instance_type = entry.get("instance_type")
                location = entry.get("location_code") or entry.get("location")
                available = entry.get("available", False)

                if instance_type and location:
                    new_cache[(instance_type, location)] = bool(available)

            with self._lock:
                self._cache = new_cache
                self._loaded = True

            available_count = sum(1 for v in new_cache.values() if v)
            logger.info(
                "Refreshed availability for %d instance-type/location pairs (%d available)",
                len(new_cache),
                available_count,
            )

        except Exception as e:
            logger.error("Failed to refresh instance availability: %s", e, exc_info=True)
            # Keep existing cache on error — don't flip _loaded to False