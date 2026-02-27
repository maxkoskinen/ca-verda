import logging
from typing import Optional

from jinja2 import Template
from verda import VerdaClient

from verda_cloud_provider.settings import KubernetesConfig
from verda_cloud_provider.wg_service import WireguardPeerConfig

logger = logging.getLogger(__name__)


class StartupScriptService:
    def __init__(self, client: VerdaClient, template_path: str, k8s_config: KubernetesConfig):
        self.client = client
        self.k8s_config = k8s_config
        try:
            with open(template_path, 'r') as f:
                self.template = Template(f.read())
        except Exception as e:
            logger.error("Failed to load startup script template: %s", e)
            raise

    def _render_script(self, labels: dict[str, str], wg: WireguardPeerConfig | None = None) -> str:
        label_str = ",".join(f"{k}={v}" for k, v in labels.items())
        return self.template.render(
            k8s_endpoint=self.k8s_config.endpoint,
            k8s_token=self.k8s_config.token,
            k8s_ca_hash=self.k8s_config.ca_hash,
            labels=label_str,
            wg_tunnel_ip=wg.tunnel_ip if wg else None,
            wg_private_key=wg.private_key if wg else None,
            wg_bastion_pubkey=wg.bastion_pubkey if wg else None,
            wg_bastion_endpoint=wg.bastion_endpoint if wg else None,
            wg_allowed_ips=",".join(wg.allowed_ips) if wg else None,
        )

    def ensure_startup_script(self, group_id: str, labels: dict[str, str], wg: WireguardPeerConfig | None = None) -> str:
        """
        Create a per-node startup script with the rendered wg config baked in.
        Returns the script ID. Caller should delete it after instance creation.
        """
        script_name = f"k8s-verda-init-{group_id}-{id(wg)}"
        content = self._render_script(labels=labels, wg=wg)
        logger.info("Creating per-node startup script '%s'", script_name)
        script = self.client.startup_scripts.create(name=script_name, script=content)
        return script.id

    def delete_startup_script(self, script_id: str) -> None:
        try:
            self.client.startup_scripts.delete_by_id(id=script_id)
            logger.debug("Deleted startup script %s", script_id)
        except Exception:
            logger.warning("Failed to delete startup script %s", script_id, exc_info=True)
