GPU_LABEL = "verda.com/gpu-type"

VERDA_MODEL_TO_GPU_TYPE: dict[str, str] = {
    "GB300": "nvidia-gb300",
    "B300":  "nvidia-b300",
    "B200":  "nvidia-b200",
    "H200":  "nvidia-h200",
    "H100":  "nvidia-h100",
    "A100":  "nvidia-a100",
    "A6000": "nvidia-a6000",
    "A40":   "nvidia-a40",
    "V100":  "nvidia-v100",
    # CPU-only instances have no model, skip them
}

def gpu_type_for_model(model: str | None) -> str | None:
    if not model:
        return None
    return VERDA_MODEL_TO_GPU_TYPE.get(model.strip())
