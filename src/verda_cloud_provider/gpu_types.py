"""GPU type definitions and NVIDIA GPU Operator compatible label mappings.

The NVIDIA GPU Operator (via GPU Feature Discovery / Node Feature Discovery)
applies ``nvidia.com/gpu.*`` labels to real nodes by inspecting hardware.
Since the cluster autoscaler's virtual template nodes never run the operator,
we maintain a static mapping here so that workloads using standard
``nodeSelector`` / ``nodeAffinity`` rules schedule correctly.

Keys in :data:`GPU_SPECS` correspond to the ``gpu.model`` strings returned
by the Verda instance-types API (e.g. ``"Tesla V100"``, ``"A100 80GB"``).

Reference:
    https://docs.nvidia.com/datacenter/cloud-native/gpu-operator/latest/
"""

from __future__ import annotations

from dataclasses import dataclass

# ---------------------------------------------------------------------------
# Legacy label kept for any Verda-specific scheduling rules
# ---------------------------------------------------------------------------
GPU_LABEL = "verda.com/gpu-type"


# ---------------------------------------------------------------------------
# Per-model GPU metadata
# ---------------------------------------------------------------------------
@dataclass(frozen=True)
class GpuSpec:
    """Static specification for a single GPU model.

    Attributes:
        gpu_type:       Short identifier used for the ``verda.com/gpu-type``
                        label (e.g. ``nvidia-h100``).
        product_fmt:    Format string for ``nvidia.com/gpu.product``.  May
                        contain a ``{memory_gb}`` placeholder that is resolved
                        at label-build time with the actual per-GPU memory
                        (mirrors what ``nvidia-smi`` reports, spaces → hyphens).
        memory_mib:     Default per-GPU framebuffer in MiB.  Used as a fallback
                        when the Verda API does not provide ``gpu_memory_gb``.
        family:         Architecture family for ``nvidia.com/gpu.family``.
        compute_major:  CUDA compute capability major version.
        compute_minor:  CUDA compute capability minor version.
    """

    gpu_type: str
    product_fmt: str
    memory_mib: int
    family: str
    compute_major: int
    compute_minor: int


# ---------------------------------------------------------------------------
# Mapping from Verda API ``gpu.model`` → GPU specification
# ---------------------------------------------------------------------------
# Keys **must** match the exact strings returned by the Verda instance-types
# API (``instance.gpu["model"]``).
#
# ``product_fmt`` may contain a ``{memory_gb}`` placeholder filled at
# label-build time with the real per-GPU memory derived from the API's total
# ``gpu_memory.size_in_gigabytes ÷ gpu_count``.
#
# ``memory_mib`` is the *default* per-GPU value used when the API value is
# unavailable.
GPU_SPECS: dict[str, GpuSpec] = {
    # ── Blackwell ──────────────────────────────────────────────────────
    "GB300": GpuSpec(
        gpu_type="nvidia-gb300",
        product_fmt="NVIDIA-GB300-SXM6-{memory_gb}GB",
        memory_mib=294912,       # 288 GB
        family="blackwell",
        compute_major=10,
        compute_minor=0,
    ),
    "B300": GpuSpec(
        gpu_type="nvidia-b300",
        product_fmt="NVIDIA-B300-SXM6-{memory_gb}GB",
        memory_mib=268288,       # 262 GB
        family="blackwell",
        compute_major=10,
        compute_minor=0,
    ),
    "B200": GpuSpec(
        gpu_type="nvidia-b200",
        product_fmt="NVIDIA-B200-SXM6-{memory_gb}GB",
        memory_mib=184320,       # 180 GB
        family="blackwell",
        compute_major=10,
        compute_minor=0,
    ),
    "RTX PRO 6000": GpuSpec(
        gpu_type="nvidia-rtx-pro-6000",
        product_fmt="NVIDIA-RTX-PRO-6000-{memory_gb}GB",
        memory_mib=98304,        # 96 GB
        family="blackwell",
        compute_major=10,
        compute_minor=2,
    ),
    # ── Hopper ─────────────────────────────────────────────────────────
    "H200": GpuSpec(
        gpu_type="nvidia-h200",
        product_fmt="NVIDIA-H200-{memory_gb}GB-HBM3e",
        memory_mib=144384,       # 141 GB
        family="hopper",
        compute_major=9,
        compute_minor=0,
    ),
    "H100": GpuSpec(
        gpu_type="nvidia-h100",
        product_fmt="NVIDIA-H100-{memory_gb}GB-HBM3",
        memory_mib=81920,        # 80 GB
        family="hopper",
        compute_major=9,
        compute_minor=0,
    ),
    # ── Ada Lovelace ───────────────────────────────────────────────────
    "L40S": GpuSpec(
        gpu_type="nvidia-l40s",
        product_fmt="NVIDIA-L40S-{memory_gb}GB",
        memory_mib=49152,        # 48 GB
        family="ada-lovelace",
        compute_major=8,
        compute_minor=9,
    ),
    "RTX 6000 Ada": GpuSpec(
        gpu_type="nvidia-rtx-6000-ada",
        product_fmt="NVIDIA-RTX-6000-Ada-Generation-{memory_gb}GB",
        memory_mib=49152,        # 48 GB
        family="ada-lovelace",
        compute_major=8,
        compute_minor=9,
    ),
    # ── Ampere ─────────────────────────────────────────────────────────
    "A100 80GB": GpuSpec(
        gpu_type="nvidia-a100",
        product_fmt="NVIDIA-A100-SXM4-{memory_gb}GB",
        memory_mib=81920,        # 80 GB
        family="ampere",
        compute_major=8,
        compute_minor=0,
    ),
    "A100 40GB": GpuSpec(
        gpu_type="nvidia-a100",
        product_fmt="NVIDIA-A100-SXM4-{memory_gb}GB",
        memory_mib=40960,        # 40 GB
        family="ampere",
        compute_major=8,
        compute_minor=0,
    ),
    "RTX A6000": GpuSpec(
        gpu_type="nvidia-rtx-a6000",
        product_fmt="NVIDIA-RTX-A6000-{memory_gb}GB",
        memory_mib=49152,        # 48 GB
        family="ampere",
        compute_major=8,
        compute_minor=6,
    ),
    # ── Volta ──────────────────────────────────────────────────────────
    "Tesla V100": GpuSpec(
        gpu_type="nvidia-v100",
        product_fmt="Tesla-V100-SXM2-{memory_gb}GB",
        memory_mib=16384,        # 16 GB
        family="volta",
        compute_major=7,
        compute_minor=0,
    ),
}


# ---------------------------------------------------------------------------
# Public helpers
# ---------------------------------------------------------------------------
def gpu_type_for_model(model: str | None) -> str | None:
    """Return the short ``gpu_type`` string (e.g. ``nvidia-h100``) for a
    Verda GPU model name, or ``None`` for CPU-only instances."""
    if not model:
        return None
    spec = GPU_SPECS.get(model.strip())
    return spec.gpu_type if spec else None


def gpu_spec_for_model(model: str | None) -> GpuSpec | None:
    """Return the full :class:`GpuSpec` for a Verda GPU model name."""
    if not model:
        return None
    return GPU_SPECS.get(model.strip())


def gpu_operator_labels(
    spec: GpuSpec,
    gpu_count: int,
    gpu_memory_gb: int | None = None,
) -> dict[str, str]:
    """Build the ``nvidia.com/gpu.*`` labels that the GPU Operator would set.

    These labels allow standard Kubernetes manifests to use
    ``nodeSelector`` or ``nodeAffinity`` expressions such as::

        nodeSelector:
          nvidia.com/gpu.product: NVIDIA-H100-80GB-HBM3

    or::

        - matchExpressions:
            - key: nvidia.com/gpu.memory
              operator: Gt
              values: ["81919"]

    Args:
        spec: The :class:`GpuSpec` for the GPU model.
        gpu_count: Number of GPUs on the node.
        gpu_memory_gb: Total GPU memory in GB as reported by the Verda API
            (``gpu_memory.size_in_gigabytes``).  This is the **total** across
            all GPUs on the node and will be divided by *gpu_count* to obtain
            the per-GPU value.  When provided, this value takes precedence
            over the static ``spec.memory_mib`` so that different SKUs of the
            same model (e.g. 40 GB vs 80 GB A100) get the correct label
            automatically.

    Returns:
        A ``dict`` of label key → value strings ready to merge into
        ``Node.metadata.labels``.
    """
    # Prefer the live value from the Verda API; fall back to the static spec.
    # The Verda API reports *total* GPU memory on the node, so divide by the
    # number of GPUs to get the per-GPU value that the GPU Operator uses.
    if gpu_memory_gb is not None and gpu_memory_gb > 0 and gpu_count > 0:
        memory_mib = (gpu_memory_gb * 1024) // gpu_count
    else:
        memory_mib = spec.memory_mib

    # Resolve the product format string with the actual per-GPU memory.
    memory_gb = memory_mib // 1024
    product = spec.product_fmt.format(memory_gb=memory_gb)

    return {
        # Core identification
        "nvidia.com/gpu.product":       product,
        "nvidia.com/gpu.memory":        str(memory_mib),
        "nvidia.com/gpu.count":         str(gpu_count),
        "nvidia.com/gpu.family":        spec.family,
        # CUDA compute capability
        "nvidia.com/gpu.compute.major": str(spec.compute_major),
        "nvidia.com/gpu.compute.minor": str(spec.compute_minor),
        # Static flags common across all modern data-center GPUs
        "nvidia.com/gpu.replicas":      "1",
        "nvidia.com/gpu.present":       "true",
    }
