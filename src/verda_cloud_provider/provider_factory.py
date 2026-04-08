import logging
import os
from dataclasses import dataclass
from pathlib import Path

from verda import VerdaClient

from verda_cloud_provider.services import (
    InstanceMetadataCache,
    NodeCleanupService,
    NodeTemplateService,
    StartupScriptService,
    WireguardService,
)
from verda_cloud_provider.settings import AppConfig
from verda_cloud_provider.state_store import InstanceStateStore

logger = logging.getLogger(__name__)

_TEMPLATES_DIR = Path(__file__).parent / "templates"


@dataclass
class ProviderServices:
    """All wired-up services the provider needs at runtime."""

    client: VerdaClient
    app_config: AppConfig
    state_store: InstanceStateStore
    template_service: NodeTemplateService
    metadata_cache: InstanceMetadataCache
    startup_script_service: StartupScriptService
    wg_service: WireguardService | None
    node_cleanup_service: NodeCleanupService


def _build_client() -> VerdaClient:
    client_id = os.environ.get("VERDA_CLIENT_ID", "")
    client_secret = os.environ.get("VERDA_CLIENT_SECRET", "")

    if not client_id or not client_secret:
        raise ValueError("VERDA_CLIENT_ID and VERDA_CLIENT_SECRET env vars must be set")

    return VerdaClient(client_id, client_secret)


def _resolve_startup_script_template(app_config: AppConfig) -> str:
    if app_config.script_template_path:
        logger.info(
            "Using custom startup script template: %s",
            app_config.script_template_path,
        )
        return app_config.script_template_path

    default_path = str(_TEMPLATES_DIR / "verda_init.sh.j2")

    match app_config.script_template:
        case "k3s":
            return str(_TEMPLATES_DIR / "verda_init_k3s.sh.j2")
        case _:
            return default_path


def _build_wg_service(app_config: AppConfig) -> WireguardService | None:
    if app_config.wireguard:
        return WireguardService(app_config.wireguard)
    return None


def build_provider_services(app_config: AppConfig) -> ProviderServices:
    """
    Wire up every service the provider depends on.

    Raises on fatal mis-configuration (missing credentials, bad config, etc.).
    """
    client = _build_client()

    try:
        node_groups_config = app_config.node_groups
        logging.info(f"Loaded configuration for {len(node_groups_config)} node groups.")
    except Exception as e:
        logging.critical(f"Failed to load configuration: {e}")
        raise e

    wg_service = _build_wg_service(app_config)

    state_store = InstanceStateStore()
    template_service = NodeTemplateService()

    configured_types = {cfg.instance_type for cfg in node_groups_config.values()}
    metadata_cache = InstanceMetadataCache(client, configured_types)

    startup_script_template_path = _resolve_startup_script_template(app_config)
    startup_script_service = StartupScriptService(
        client=client,
        template_path=startup_script_template_path,
        k8s_config=app_config.kubernetes,
    )

    node_cleanup_service = NodeCleanupService(
        verda_client=client,
        state_store=state_store,
        wg_service=wg_service,
        node_groups_config=node_groups_config,
    )

    return ProviderServices(
        client=client,
        app_config=app_config,
        state_store=state_store,
        template_service=template_service,
        metadata_cache=metadata_cache,
        startup_script_service=startup_script_service,
        wg_service=wg_service,
        node_cleanup_service=node_cleanup_service,
    )
