import logging

from jinja2 import Template
from verda import VerdaClient

from verda_cloud_provider.settings import KubernetesConfig

logger = logging.getLogger(__name__)

class StartupScriptService:
    def __init__(self, client: VerdaClient, template_path: str, k8s_config: KubernetesConfig):
        self.client = client
        self.k8s_config = k8s_config
        self.template_path = template_path

        try:
            with open(template_path, 'r') as f:
                self.template = Template(f.read())
        except Exception as e:
            logger.error(f"Failed to load startup script template: {e}")
            raise

    def _render_script(self, labels: dict[str, str]) -> str:
        # Format labels as key=value,key2=value2
        label_str = ",".join([f"{k}={v}" for k, v in labels.items()])

        return self.template.render(
            k8s_endpoint=self.k8s_config.endpoint,
            k8s_token=self.k8s_config.token,
            k8s_ca_hash=self.k8s_config.ca_hash,
            labels=label_str
        )

    def ensure_startup_script(self, group_id: str, labels: dict[str, str]) -> str:
        """
        Ensures a startup script exists for the given group and configuration.
        Returns the script ID.
        """
        script_name = f"k8s-verda-init-{group_id}"
        content = self._render_script(labels)

        # Check if script exists
        scripts = self.client.startup_scripts.get()
        existing_script = next((s for s in scripts if s.name == script_name), None)

        if existing_script:
            if existing_script.script != content:
                logger.info(f"Updating startup script '{script_name}' due to configuration change.")
                try:
                    self.client.startup_scripts.delete_by_id(id=existing_script.id)
                    script = self.client.startup_scripts.create(name=script_name, script=content)
                    return script.id
                except Exception:
                    logger.exception("Exception occured during updating startup script returning old id")
                    return existing_script.id
            else:
                logger.debug(f"Using existing startup script '{script_name}'")
                return existing_script.id
        else:
            logger.info(f"Creating new startup script '{script_name}'")
            new_script = self.client.startup_scripts.create(
                name=script_name,
                script=content
            )
            return new_script.id
