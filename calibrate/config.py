"""代价表配置键读取与口径字段采集（D30、D32、D36）。"""

from __future__ import annotations

import json
import os
import subprocess
import urllib.request
from dataclasses import dataclass
from pathlib import Path

ENV_COST_TABLE_ROOT = "KVFERRY_COST_TABLE_ROOT"
ENV_PROBE_INTERVAL_S = "KVFERRY_PROBE_INTERVAL_S"
ENV_PROBE_BLOCK_MIB = "KVFERRY_PROBE_BLOCK_MIB"
ENV_DRIFT_THRESHOLD_PCT = "KVFERRY_DRIFT_THRESHOLD_PCT"
ENV_DRIFT_TRIGGER_CONSECUTIVE = "KVFERRY_DRIFT_TRIGGER_CONSECUTIVE"
ENV_DRIFT_SAMPLE_MIN = "KVFERRY_DRIFT_SAMPLE_MIN"
ENV_DRIFT_WINDOW_MIN_S = "KVFERRY_DRIFT_WINDOW_MIN_S"
ENV_DRIFT_WINDOW_MAX_S = "KVFERRY_DRIFT_WINDOW_MAX_S"

_ENV_COST_TABLE_VERSION = "KVFERRY_COST_TABLE_VERSION"
_UNKNOWN = "unknown"
_CMD_TIMEOUT_S = 10


@dataclass(frozen=True)
class RuntimeConfig:
    table_root: Path           # 表库根目录，KVFERRY_COST_TABLE_ROOT，必填（D36）
    declared_version: str      # KVFERRY_COST_TABLE_VERSION，未设置按 unknown（D30）
    probe_interval_s: int      # 缺省 30（D32）
    probe_block_mib: int       # 缺省 64（D32）
    drift_threshold_pct: float # 缺省 20（D32）
    drift_trigger_consecutive: int  # 缺省 3（D32）
    drift_sample_min: int      # 缺省 100（D32）
    drift_window_min_s: int    # 缺省 60（D32）
    drift_window_max_s: int    # 缺省 1800（D32）


def load_runtime_config() -> RuntimeConfig:
    root_raw = os.environ.get(ENV_COST_TABLE_ROOT, "").strip()
    if not root_raw:
        raise ValueError(f"{ENV_COST_TABLE_ROOT} 未设置，表库根目录必填（D36）")
    return RuntimeConfig(
        table_root=Path(root_raw).expanduser(),
        declared_version=os.environ.get(_ENV_COST_TABLE_VERSION, "").strip() or _UNKNOWN,
        probe_interval_s=_positive_int(ENV_PROBE_INTERVAL_S, 30),
        probe_block_mib=_positive_int(ENV_PROBE_BLOCK_MIB, 64),
        drift_threshold_pct=_positive_float(ENV_DRIFT_THRESHOLD_PCT, 20),
        drift_trigger_consecutive=_positive_int(ENV_DRIFT_TRIGGER_CONSECUTIVE, 3),
        drift_sample_min=_positive_int(ENV_DRIFT_SAMPLE_MIN, 100),
        drift_window_min_s=_positive_int(ENV_DRIFT_WINDOW_MIN_S, 60),
        drift_window_max_s=_positive_int(ENV_DRIFT_WINDOW_MAX_S, 1800),
    )


def _positive_int(name: str, default: int) -> int:
    raw = os.environ.get(name, "").strip()
    if not raw:
        return default
    try:
        value = int(raw)
    except ValueError:
        raise ValueError(f"{name} 不是合法整数: {raw!r}（D32）") from None
    if value <= 0:
        raise ValueError(f"{name} 必须为正数，得到: {raw!r}（D32）")
    return value


def _positive_float(name: str, default: float) -> float:
    raw = os.environ.get(name, "").strip()
    if not raw:
        return default
    try:
        value = float(raw)
    except ValueError:
        raise ValueError(f"{name} 不是合法数字: {raw!r}（D32）") from None
    if value <= 0:
        raise ValueError(f"{name} 必须为正数，得到: {raw!r}（D32）")
    return value


@dataclass(frozen=True)
class Provenance:
    gpu_name: str
    nic: str
    pcie_topology: str
    driver_version: str
    engine_version: str
    model: str
    kv_precision: str
    block_tokens: int


def collect_provenance(engine_endpoint: str, model: str,
                       kv_precision: str, block_tokens: int) -> Provenance:
    gpu_name, driver_version = _query_gpu()
    return Provenance(
        gpu_name=gpu_name,
        nic=_probe_nic(),
        pcie_topology=_probe_pcie_topology(),
        driver_version=driver_version,
        engine_version=_query_engine_version(engine_endpoint),
        model=model,
        kv_precision=kv_precision,
        block_tokens=block_tokens,
    )


def _run_command(cmd: list[str]) -> str | None:
    try:
        result = subprocess.run(cmd, capture_output=True, text=True, timeout=_CMD_TIMEOUT_S)
    except (OSError, subprocess.SubprocessError):
        return None
    if result.returncode != 0:
        return None
    return result.stdout.strip() or None


def _query_gpu() -> tuple[str, str]:
    raw = _run_command(
        ["nvidia-smi", "--query-gpu=name,driver_version", "--format=csv,noheader"])
    if not raw:
        return _UNKNOWN, _UNKNOWN
    parts = [part.strip() for part in raw.splitlines()[0].split(",")]
    gpu_name = parts[0] if parts and parts[0] else _UNKNOWN
    driver = parts[1] if len(parts) > 1 and parts[1] else _UNKNOWN
    return gpu_name, driver


def _probe_nic() -> str:
    try:
        ifaces = sorted(p.name for p in Path("/sys/class/net").iterdir())
    except OSError:
        return _UNKNOWN
    for iface in ifaces:
        if iface == "lo":
            continue
        if _run_command(["ethtool", iface]) is not None:
            return iface
    return _UNKNOWN


def _probe_pcie_topology() -> str:
    raw = _run_command(["nvidia-smi", "--query-gpu=pci.bus_id", "--format=csv,noheader"])
    if not raw:
        return _UNKNOWN
    bus_id = raw.splitlines()[0].strip()
    segments = bus_id.split(":")
    if len(segments) == 4 and len(segments[0]) == 8:
        bus_id = "0000:" + ":".join(segments[1:])
    return _run_command(["lspci", "-s", bus_id]) or _UNKNOWN


def _query_engine_version(engine_endpoint: str) -> str:
    endpoint = (engine_endpoint or "").strip().rstrip("/")
    if not endpoint:
        return _UNKNOWN
    try:
        with urllib.request.urlopen(f"{endpoint}/version", timeout=_CMD_TIMEOUT_S) as resp:
            payload = json.loads(resp.read().decode("utf-8"))
    except (OSError, ValueError):
        return _UNKNOWN
    version = payload.get("version") if isinstance(payload, dict) else None
    return str(version) if version else _UNKNOWN