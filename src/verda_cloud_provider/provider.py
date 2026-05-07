import logging

from verda_cloud_provider.provider_factory import build_provider_services
from verda_cloud_provider.provider_methods import VerdaCloudProviderMethodsMixin
from verda_cloud_provider.settings import AppConfig

logger = logging.getLogger(__name__)


class VerdaCloudProvider(VerdaCloudProviderMethodsMixin):
    def __init__(self, app_config: AppConfig):
        services = build_provider_services(app_config)

        self.client = services.client
        self.app_config = services.app_config
        self.node_groups_config = self.app_config.node_groups
        self.state_store = services.state_store
        self.template_service = services.template_service
        self.metadata_cache = services.metadata_cache
        self.availability_cache = services.availability_cache
        self.startup_script_service = services.startup_script_service
        self.wg_service = services.wg_service
        self.node_cleanup_service = services.node_cleanup_service

        self._initialize()

    def _initialize(self):
        """Sync target sizes with actual cloud state on startup."""
        if not self.client:
            return

        try:
            instances = self.client.instances.get()
            # Sync state store with API
            self.state_store.sync_with_api(instances, self.node_groups_config)
            # Refresh metadata and availability
            self.metadata_cache.refresh()
            self.availability_cache.refresh()

        except Exception as e:
            logger.error(f"Failed to initialize: {e}")

        # Start the background node cleanup loop (very lightweight CCM replacement).
        try:
            self.node_cleanup_service.start()
            logger.info("Node cleanup service started")
        except Exception as e:
            logger.error(f"Failed to start node cleanup service: {e}")

    def shutdown(self):
        """Stop background services. Called on server shutdown."""
        try:
            self.node_cleanup_service.stop()
        except Exception as e:
            logger.error(f"Error stopping node cleanup service: {e}")
