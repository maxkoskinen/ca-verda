import logging
from threading import RLock

from verda import VerdaClient

logger = logging.getLogger(__name__)


# Key is (instance_type, location_code)
AvailabilityKey = tuple[str, str]


class InstanceAvailabilityCache:
    """
    Cache for instance type availability from Verda API.
    Updated via explicit refresh() calls during the Refresh() loop.

    The Verda API returns a list of dicts shaped like::

        [
            {"location_code": "FIN-01", "availabilities": []},
            {"location_code": "FIN-02", "availabilities": ["1H200.141S.44V", "CPU.4V.16G", ...]},
        ]

    We flatten these into a set of (instance_type, location) tuples
    representing the available combinations.
    """

    def __init__(self, client: VerdaClient):
        self.client = client
        self._lock = RLock()
        # Set of (instance_type, location) pairs that are currently available
        self._available: set[AvailabilityKey] = set()
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
            return (instance_type, location) in self._available

    def refresh(self) -> None:
        """
        Fetch latest availability data from Verda API.
        Called by the provider's Refresh() method.
        """
        try:
            raw_availabilities = self.client.instances.get_availabilities()

            new_available: set[AvailabilityKey] = set()
            for entry in raw_availabilities:
                location = entry.get("location_code", "")
                for instance_type in entry.get("availabilities", []):
                    new_available.add((instance_type, location))

            with self._lock:
                self._available = new_available
                self._loaded = True

            logger.info(
                "Refreshed availability: %d instance-type/location pairs available",
                len(new_available),
            )

        except Exception as e:
            logger.error("Failed to refresh instance availability: %s", e, exc_info=True)
            # Keep existing cache on error — don't flip _loaded to False