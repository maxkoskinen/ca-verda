from dataclasses import field
from typing import Literal, Union, get_args

import yaml
from pydantic import BaseModel, Field, field_validator, model_validator
from verda.constants import Locations
from verda.instances import Contract, Pricing

_ALLOWED_LOCATIONS = {
    value
    for name, value in vars(Locations).items()
    if not name.startswith("_") and isinstance(value, str)
}

_ALLOWED_CONTRACTS = set(get_args(Contract))
_ALLOWED_PRICING = set(get_args(Pricing))


class LocalWireguardBackendConfig(BaseModel):
    type: Literal["local"] = "local"


class SSHWireguardBackendConfig(BaseModel):
    type: Literal["ssh"] = "ssh"
    host: str
    user: str
    private_key_path: str
    port: int = 22


WireguardBackendConfig = Union[
    LocalWireguardBackendConfig,
    SSHWireguardBackendConfig,
]


class WireguardConfig(BaseModel):
    interface: str = "wg0"
    tunnel_network: str = "10.200.0.0/24"
    listen_port: int = 51820
    server_pub_key: str | None = None
    server_privkey_path: str = "/etc/wireguard/wg0.key"
    cloud_allowed_ips: list[str] = field(default_factory=lambda: ["10.200.0.0/24"])
    keepalive: int = 25
    node_wg_port: int = 51820
    backend: WireguardBackendConfig = Field(default_factory=LocalWireguardBackendConfig)


class KubernetesConfig(BaseModel):
    endpoint: str


class ResourcesConfig(BaseModel):
    cpu: int = Field(gt=0)
    memory_gb: int = Field(gt=0)
    gpu_count: int = Field(ge=0, default=0)
    gpu_model: str | None = None
    gpu_memory_gb: int | None = None


class NodeGroupConfig(BaseModel):
    instance_type: str
    image: str
    min_size: int = Field(ge=0, default=0)
    max_size: int = Field(gt=0)
    location: str = "FIN-01"
    ssh_key_ids: list[str] = Field(default_factory=list)
    contract: Literal["LONG_TERM", "PAY_AS_YOU_GO", "SPOT"] = "PAY_AS_YOU_GO"
    pricing: Literal["FIXED_PRICE", "DYNAMIC_PRICE"] = "DYNAMIC_PRICE"
    hourly_price: float
    resources: ResourcesConfig | None = None
    os_volume_gb: int | None = None
    labels: dict[str, str] = Field(default_factory=dict)
    taints: dict[str, str] = Field(default_factory=dict)
    override_resources: dict[str, str] = Field(default_factory=dict)

    @field_validator("max_size")
    def check_max_size(cls, v, values):
        return v

    @field_validator("pricing")
    def check_pricing_type(cls, v):
        if v is None:
            return v
        if v not in _ALLOWED_PRICING:
            raise ValueError(
                f"Invalid pricing '{v}'. Allowed values: {_ALLOWED_PRICING}"
            )
        return v

    @field_validator("location")
    def check_location(cls, v, values):
        if v is None:
            return v
        if v not in _ALLOWED_LOCATIONS:
            raise ValueError(
                f"Invalid location '{v}'. Allowed values: {_ALLOWED_LOCATIONS}"
            )
        return v


class NodeGroupInputConfig(BaseModel):
    """
    User-facing node group config that accepts either a single location string
    or a list of locations. When multiple locations are given, the group is
    expanded into one NodeGroupConfig per location with the key format
    ``{original_key}-{LOCATION}``.

    When a single location string is provided (legacy format), the group key
    is kept as-is and no expansion happens.
    """

    instance_type: str
    image: str
    min_size: int = Field(ge=0, default=0)
    max_size: int = Field(gt=0)
    locations: list[str] | str = Field(default="FIN-01")
    ssh_key_ids: list[str] = Field(default_factory=list)
    contract: Literal["LONG_TERM", "PAY_AS_YOU_GO", "SPOT"] = "PAY_AS_YOU_GO"
    pricing: Literal["FIXED_PRICE", "DYNAMIC_PRICE"] = "DYNAMIC_PRICE"
    hourly_price: float
    resources: ResourcesConfig | None = None
    os_volume_gb: int | None = None
    labels: dict[str, str] = Field(default_factory=dict)
    taints: dict[str, str] = Field(default_factory=dict)
    override_resources: dict[str, str] = Field(default_factory=dict)

    # Accept legacy "location" key as an alias for "locations"
    location: str | None = Field(default=None, exclude=True)

    @model_validator(mode="before")
    @classmethod
    def _normalise_location_field(cls, data):
        if isinstance(data, dict):
            if "locations" not in data and "location" in data:
                data["locations"] = data.pop("location")
            elif "locations" in data and "location" in data:
                # locations takes precedence; drop legacy key
                data.pop("location", None)
        return data

    @field_validator("locations")
    @classmethod
    def check_locations(cls, v):
        locs = [v] if isinstance(v, str) else v
        for loc in locs:
            if loc not in _ALLOWED_LOCATIONS:
                raise ValueError(
                    f"Invalid location '{loc}'. Allowed values: {_ALLOWED_LOCATIONS}"
                )
        return v

    def _build_node_group_config(self, location: str) -> NodeGroupConfig:
        """Build a single ``NodeGroupConfig`` for the given *location*."""
        return NodeGroupConfig(
            instance_type=self.instance_type,
            image=self.image,
            min_size=self.min_size,
            max_size=self.max_size,
            location=location,
            ssh_key_ids=list(self.ssh_key_ids),
            contract=self.contract,
            pricing=self.pricing,
            hourly_price=self.hourly_price,
            resources=self.resources,
            os_volume_gb=self.os_volume_gb,
            labels=dict(self.labels),
            taints=dict(self.taints),
            override_resources=dict(self.override_resources),
        )

    def expand(self, key: str) -> dict[str, NodeGroupConfig]:
        """
        Expand this input config into one or more ``NodeGroupConfig`` entries.

        - If ``locations`` is a list, produces one entry per location with key
          ``{key}-{location}``.
        - If ``locations`` is a single string (legacy), produces one entry with
          the original ``key`` unchanged.
        """
        if isinstance(self.locations, list):
            result: dict[str, NodeGroupConfig] = {}
            for loc in self.locations:
                expanded_key = f"{key}-{loc}"
                result[expanded_key] = self._build_node_group_config(loc)
            return result
        else:
            # Single string location — keep the original key as-is (legacy)
            return {
                key: self._build_node_group_config(self.locations),
            }


_NODE_GROUP_DEFAULT_KEYS = ("ssh_key_ids", "contract", "pricing", "os_volume_gb", "labels", "taints", "override_resources")
_DICT_MERGE_KEYS = {"labels", "taints", "override_resources"}


class AppConfig(BaseModel):
    node_groups: dict[str, NodeGroupConfig]
    kubernetes: KubernetesConfig
    wireguard: WireguardConfig | None = None
    script_template: Literal["k3s"] | None = None
    script_template_path: str | None = None
    ssh_key_ids: list[str] = Field(default_factory=list, exclude=True)
    contract: Literal["LONG_TERM", "PAY_AS_YOU_GO", "SPOT"] = Field(
        default="PAY_AS_YOU_GO", exclude=True
    )
    pricing: Literal["FIXED_PRICE", "DYNAMIC_PRICE"] = Field(
        default="DYNAMIC_PRICE", exclude=True
    )
    os_volume_gb: int | None = Field(default=100, exclude=True)
    labels: dict[str, str] = Field(default_factory=dict, exclude=True)
    taints: dict[str, str] = Field(default_factory=dict, exclude=True)
    override_resources: dict[str, str] = Field(default_factory=dict, exclude=True)

    @model_validator(mode="before")
    @classmethod
    def _expand_node_groups(cls, data):
        """
        Pre-process the raw config dict:

        1. Read top-level defaults for ``ssh_key_ids``, ``contract``,
           ``pricing``, ``labels`` and ``taints``.
        2. Merge those defaults into every node-group entry (the node-group
           value wins when both define the same key; for dict fields like
           ``labels``/``taints`` a shallow merge is performed so individual
           keys from the default can still be overridden).
        3. Parse each entry as a ``NodeGroupInputConfig`` and expand
           multi-location groups into individual ``NodeGroupConfig`` entries
           keyed by ``{group_name}-{location}``.
        """
        if not isinstance(data, dict):
            return data

        raw_groups = data.get("node_groups")
        if not raw_groups or not isinstance(raw_groups, dict):
            return data

        #collect top-level defaults
        defaults: dict[str, object] = {}
        for key in _NODE_GROUP_DEFAULT_KEYS:
            if key in data:
                defaults[key] = data[key]

        # apply defaults + expand
        expanded: dict[str, object] = {}
        for key, value in raw_groups.items():
            # If value is already a NodeGroupConfig (programmatic use), keep it
            if isinstance(value, NodeGroupConfig):
                expanded[key] = value.model_dump()
                continue

            group_dict = dict(value) if isinstance(value, dict) else {}

            # merge defaults
            for dk, dv in defaults.items():
                if dk in _DICT_MERGE_KEYS:
                    # Shallow-merge: default dict, then override with group dict
                    merged = dict(dv) if isinstance(dv, dict) else {}
                    merged.update(group_dict.get(dk, {}))
                    group_dict[dk] = merged
                else:
                    # Scalar / list: use group value if present, else default
                    if dk not in group_dict:
                        group_dict[dk] = dv

            input_cfg = NodeGroupInputConfig(**group_dict)
            for expanded_key, node_group_cfg in input_cfg.expand(key).items():
                if expanded_key in expanded:
                    raise ValueError(
                        f"Duplicate node group key '{expanded_key}' after "
                        f"location expansion of group '{key}'"
                    )
                expanded[expanded_key] = node_group_cfg.model_dump()

        data["node_groups"] = expanded
        return data

    @classmethod
    def load(cls, path: str = "config.yaml") -> "AppConfig":
        with open(path, "r") as f:
            raw_config = yaml.safe_load(f)
        return cls(**raw_config)
