"""装置参数唯一来源：默认值、常量、格定义与路径推导。"""

from __future__ import annotations

import hashlib
import os
import re
import subprocess
from dataclasses import dataclass
from pathlib import Path
from typing import Any

ENV_DATA_ROOT = "KVFERRY_BENCH_DATA_ROOT"
ENV_GATEWAY_ENDPOINT = "KVFERRY_GATEWAY_ENDPOINT"
ENV_STORAGE_ENDPOINT = "KVFERRY_STORAGE_ENDPOINT"
ENV_DECISION_ENDPOINT = "KVFERRY_DECISION_ENDPOINT"
ENV_FETCH_BUDGET = "KVFERRY_FETCH_BUDGET_BYTES"
ENV_CONNECTOR_BUDGET = "KVFERRY_CONNECTOR_BUDGET_BYTES"
ENV_COST_TABLE_VERSION = "KVFERRY_COST_TABLE_VERSION"
ENV_IMAGE_VERSION = "KVFERRY_IMAGE_VERSION"

DEFAULT_DATA_ROOT = "~/kvferry-bench/data"

REPO_ROOT = Path(__file__).resolve().parents[1]

TIERS = ("S", "M", "L")
MODEL_NAME_BY_TIER = {
    "S": "Qwen/Qwen3-1.7B",
    "M": "Qwen/Qwen3-8B",
    "L": "Qwen/Qwen3-14B",
}
MODEL_DIR_BY_TIER = {
    "S": "qwen3-1.7b",
    "M": "qwen3-8b",
    "L": "qwen3-14b",
}
MODEL_ROOT = Path.home() / "models"
TOKEN_LIMIT_BY_TIER = {"S": 16 * 1024, "M": 30 * 1024, "L": 16 * 1024}
MIN_GPU_GIB_BY_TIER = {"S": 7, "M": 32, "L": 37}

LENGTH_SHORT_RANGE = (512, 2048)
LENGTH_MID_RANGE = (2048, 8192)
LENGTH_LONG_MAIN_RANGE = (12288, 16384)
LENGTH_LONG_TAIL_RANGE = (8192, 12288)
LENGTH_LONG_MAIN_SHARE = 0.9
LENGTH_TIERS = ("short", "mid", "long")

PREFIX_TAGS = ("p0", "p50", "p80")
PREFIX_RATIO_BY_TAG = {"p0": 0.0, "p50": 0.5, "p80": 0.8}
LOAD_TIERS = ("low", "high")

LOW_LOAD_SCALE = 0.5
HIGH_LOAD_SCALE = 1.5
MEASURE_WINDOW_S = 300
SAT_SAMPLE_INTERVAL_S = 1
HIGH_LOAD_MIN_BACKLOG_S = 30
LOW_LOAD_MAX_BUSY_RATIO = 0.10

LOADGEN_CPU_SAT_PCT = 90.0
LOADGEN_CONNECTION_SAT = 1024

KV_BYTES_PER_GIB = 1024 ** 3
DEFAULT_PORT = 8000
DEFAULT_HEALTH_TIMEOUT_S = 180.0
DEFAULT_REQUEST_TIMEOUT_S = 3600.0
DEFAULT_MAX_OUTPUT_TOKENS = 128
DEFAULT_REPS = 2
DEFAULT_SEED = 0
DEFAULT_CELL = "all"
DEFAULT_MODE = "all"
DEFAULT_GROUP = "native"

TABLES = ("decision.jsonl", "fallback.jsonl", "evict.jsonl", "drill.jsonl")

GROUPS = (
    "native",
    "fetch-all",
    "recompute-all",
    "sticky-static",
    "connector",
    "kvferry",
)

GROUP_SPEC: dict[str, dict[str, Any]] = {
    "native": {
        "storage": None,
        "fetch_policy": None,
        "needs_fetch_budget": False,
        "needs_connector_budget": False,
        "offload": None,
    },
    "fetch-all": {
        "storage": "kvferry",
        "fetch_policy": "force-fetch",
        "needs_fetch_budget": False,
        "needs_connector_budget": False,
        "offload": None,
    },
    "recompute-all": {
        "storage": "kvferry",
        "fetch_policy": "force-recompute",
        "needs_fetch_budget": False,
        "needs_connector_budget": False,
        "offload": None,
    },
    "sticky-static": {
        "storage": "layered",
        "fetch_policy": "static",
        "needs_fetch_budget": True,
        "needs_connector_budget": False,
        "offload": None,
    },
    "connector": {
        "storage": "vllm-cpu-offload",
        "fetch_policy": None,
        "needs_fetch_budget": False,
        "needs_connector_budget": True,
        "offload": "vllm-offloading-connector",
    },
    "kvferry": {
        "storage": "kvferry",
        "fetch_policy": "online",
        "needs_fetch_budget": False,
        "needs_connector_budget": False,
        "offload": None,
    },
}

COMPONENT_LABELS = {
    "kvferry-storage": "KVFerry 存储",
    "layered-storage": "分层存储",
    "fetch-engine": "取回组件",
    "decision-service": "决策服务",
    "vllm-offloading-connector": "vLLM CPU offload 连接器",
}

GROUP_DEPENDENCIES = {
    "native": (),
    "fetch-all": ("kvferry-storage", "fetch-engine"),
    "recompute-all": ("kvferry-storage", "fetch-engine"),
    "sticky-static": ("layered-storage", "fetch-engine"),
    "connector": ("vllm-offloading-connector",),
    "kvferry": ("kvferry-storage", "decision-service", "fetch-engine"),
}

STORAGE_GROUPS = ("fetch-all", "recompute-all", "sticky-static", "kvferry")

SUMMARY_COLUMNS = (
    "tier",
    "group",
    "cell",
    "mode",
    "valid_reps",
    "ttft_p50_ms",
    "ttft_p90_ms",
    "prefix_hit_rate",
    "prompt_tokens",
    "cached_tokens",
    "fetch_bytes",
)


@dataclass(frozen=True)
class Cell:
    cell_id: str
    prefix_tag: str
    prefix_ratio: float
    length_tier: str
    load_tier: str


def _build_cells() -> tuple[Cell, ...]:
    cells: list[Cell] = []
    for prefix_tag in PREFIX_TAGS:
        for length_tier in LENGTH_TIERS:
            for load_tier in LOAD_TIERS:
                cell_id = f"{prefix_tag}-len{length_tier}-{load_tier}"
                cells.append(
                    Cell(
                        cell_id=cell_id,
                        prefix_tag=prefix_tag,
                        prefix_ratio=PREFIX_RATIO_BY_TAG[prefix_tag],
                        length_tier=length_tier,
                        load_tier=load_tier,
                    )
                )
    return tuple(cells)


CELLS = _build_cells()
CELL_IDS = tuple(cell.cell_id for cell in CELLS)
CELL_BY_ID = {cell.cell_id: cell for cell in CELLS}
CELL_RE = re.compile(r"^(p0|p50|p80)-len(short|mid|long)-(low|high)$")


def normalize_tier(tier: str) -> str:
    value = tier.strip().upper()
    if value not in TIERS:
        raise ValueError(f"未知模型档: {tier}（取值 {'/'.join(TIERS)}）")
    return value


def parse_cell(cell_id: str) -> Cell:
    cell_id = cell_id.strip()
    if not CELL_RE.match(cell_id) or cell_id not in CELL_BY_ID:
        raise ValueError(
            f"未知格 ID: {cell_id}（形如 p<0|50|80>-len<short|mid|long>-<low|high>）"
        )
    return CELL_BY_ID[cell_id]


def resolve_cells(cell: str) -> tuple[Cell, ...]:
    cell = cell.strip()
    if cell == "all":
        return CELLS
    return (parse_cell(cell),)


def resolve_modes(mode: str) -> tuple[str, ...]:
    mode = mode.strip()
    if mode == "all":
        return ("cold", "hot")
    if mode not in ("cold", "hot"):
        raise ValueError(f"未知跑法: {mode}（取值 cold/hot/all）")
    return (mode,)


def resolve_group(group: str) -> str:
    group = group.strip()
    if group == "all":
        return "all"
    if group not in GROUPS:
        raise ValueError(f"未知组: {group}（取值 {'/'.join(GROUPS)}/all）")
    return group


def load_scale_for(load_tier: str) -> float:
    if load_tier == "low":
        return LOW_LOAD_SCALE
    if load_tier == "high":
        return HIGH_LOAD_SCALE
    raise ValueError(f"未知压力档: {load_tier}")


def data_root() -> Path:
    raw = os.environ.get(ENV_DATA_ROOT, DEFAULT_DATA_ROOT)
    return Path(raw).expanduser()


def ensure_data_root() -> Path:
    root = data_root()
    root.mkdir(parents=True, exist_ok=True)
    return root


def traces_dir(tier: str) -> Path:
    return data_root() / "traces" / tier


def trace_path(tier: str, cell_id: str) -> Path:
    return traces_dir(tier) / f"trace-{cell_id}.jsonl"


def runs_root(tier: str) -> Path:
    return data_root() / "runs" / tier


def cell_dir(tier: str, group: str, cell_id: str) -> Path:
    return runs_root(tier) / group / cell_id


def round_dir(tier: str, group: str, cell_id: str, mode: str, rep: int) -> Path:
    return cell_dir(tier, group, cell_id) / f"{mode}-{rep}"


def quarantine_dir(tier: str, group: str, cell_id: str) -> Path:
    return cell_dir(tier, group, cell_id) / "quarantine"


def summary_path(tier: str) -> Path:
    return data_root() / "summary" / tier / "summary.csv"


def procs_path(tier: str) -> Path:
    return data_root() / "procs" / f"{tier}.json"


def logs_dir(tier: str) -> Path:
    return data_root() / "logs" / tier


def default_model_path(tier: str) -> Path:
    return MODEL_ROOT / MODEL_DIR_BY_TIER[tier]


def gateway_endpoint() -> str | None:
    value = os.environ.get(ENV_GATEWAY_ENDPOINT, "").strip()
    return value or None


def _int_env(name: str) -> int | None:
    raw = os.environ.get(name, "").strip()
    if not raw:
        return None
    return int(raw)


def fetch_budget_bytes() -> int | None:
    return _int_env(ENV_FETCH_BUDGET)


def connector_budget_bytes() -> int | None:
    return _int_env(ENV_CONNECTOR_BUDGET)


def cost_table_version() -> str:
    return os.environ.get(ENV_COST_TABLE_VERSION, "").strip() or "unknown"


def image_version() -> str:
    return os.environ.get(ENV_IMAGE_VERSION, "").strip() or "unknown"


def group_launch_params(group: str) -> dict[str, Any]:
    spec = GROUP_SPEC[group]
    params: dict[str, Any] = {"route": "sticky"}
    if spec["fetch_policy"] is not None:
        params["fetch-policy"] = spec["fetch_policy"]
    if spec["needs_fetch_budget"]:
        params["fetch-budget"] = fetch_budget_bytes()
    if spec["needs_connector_budget"]:
        params["connector-budget"] = connector_budget_bytes()
    if spec["storage"] is not None:
        params["storage"] = spec["storage"]
    if spec["offload"] is not None:
        params["offload"] = spec["offload"]
    return params


def group_dependency_error(group: str) -> str | None:
    dependencies = GROUP_DEPENDENCIES[group]
    if not dependencies:
        return None
    labels = "、".join(COMPONENT_LABELS.get(item, item) for item in dependencies)
    params = group_launch_params(group)
    rendered = ", ".join(f"{key}={value}" for key, value in params.items())
    return (
        f"依赖组件未就绪：组 {group} 需要 {labels}"
        f"（装置阶段未实现，D14/D15；已解析配置: {rendered}）"
    )


def seed_for_cell(seed: int, cell_id: str) -> int:
    digest = hashlib.sha256(f"{seed}:{cell_id}".encode("utf-8")).digest()
    return int.from_bytes(digest[:8], "big")


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with open(path, "rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def bench_code_version() -> str:
    try:
        result = subprocess.run(
            ["git", "-C", str(REPO_ROOT), "rev-parse", "--short", "HEAD"],
            capture_output=True,
            text=True,
            timeout=10,
        )
        if result.returncode == 0:
            return result.stdout.strip() or "unknown"
    except (OSError, subprocess.SubprocessError):
        pass
    return "unknown"


def machine_info() -> dict[str, str]:
    info = {"gpu_name": "unknown", "driver_version": "unknown"}
    try:
        result = subprocess.run(
            [
                "nvidia-smi",
                "--query-gpu=name,driver_version",
                "--format=csv,noheader",
            ],
            capture_output=True,
            text=True,
            timeout=10,
        )
        if result.returncode == 0 and result.stdout.strip():
            first = result.stdout.strip().splitlines()[0]
            parts = [part.strip() for part in first.split(",")]
            if parts:
                info["gpu_name"] = parts[0]
            if len(parts) > 1:
                info["driver_version"] = parts[1]
    except (OSError, subprocess.SubprocessError):
        pass
    return info