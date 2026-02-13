from typing import get_args

import yaml
from pydantic import BaseModel, Field, field_validator
from typing_extensions import Literal
from verda.constants import Locations
from verda.instances import Contract, Pricing

_ALLOWED_LOCATIONS = {
    value
    for name, value in vars(Locations).items()
    if not name.startswith("_") and isinstance(value, str)
}

_ALLOWED_CONTRACTS = set(get_args(Contract))
_ALLOWED_PRICING = set(get_args(Pricing))

class KubernetesConfig(BaseModel):
    endpoint: str
    token: str
    ca_hash: str

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
    startup_script_id: str | None = None
    contract: Literal["LONG_TERM", "PAY_AS_YOU_GO", "SPOT"] = "PAY_AS_YOU_GO"
    pricing: Literal["FIXED_PRICE", "DYNAMIC_PRICE"] = "DYNAMIC_PRICE"
    hourly_price: float
    resources: ResourcesConfig | None = None
    labels: dict[str, str] = Field(default_factory=dict)

    @field_validator("max_size")
    def check_max_size(cls, v, values):
        # Access min_size from the validation info if needed,
        # generally complex cross-field validation happens in model_validator
        return v

    @field_validator("pricing")
    def check_pricing_type(cls, v):
        if v is None:
            return v
        if v not in _ALLOWED_PRICING:
            raise ValueError(
                f"Invalid pricing '{v}'. Allowed values: {_ALLOWED_PRICING}"
            )

    @field_validator("location")
    def check_location(cls, v, values):
        if v is None:
            return v
        if v not in _ALLOWED_LOCATIONS:
            raise ValueError(
                f"Invalid location '{v}'. Allowed values: {_ALLOWED_LOCATIONS}"
            )
        pass


class AppConfig(BaseModel):
    node_groups: dict[str, NodeGroupConfig]
    kubernetes: KubernetesConfig

    @classmethod
    def load(cls, path: str = "config.yaml") -> "AppConfig":
        with open(path, "r") as f:
            raw_config = yaml.safe_load(f)
        return cls(**raw_config)
