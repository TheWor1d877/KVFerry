"""环境自检：Python、vLLM、显存、模型目录与数据目录磁盘余量。"""

from __future__ import annotations

import argparse
import shutil
import subprocess
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

import config

MIN_FREE_GIB = 20.0
VLLM_REQUIRED_VERSION = "0.34.0"


def _check(name: str, ok: bool, detail: str) -> bool:
    state = "PASS" if ok else "FAIL"
    print(f"[{state}] {name}: {detail}")
    return ok


def _python_ok() -> tuple[bool, str]:
    version = sys.version_info
    return version >= (3, 10), f"{version.major}.{version.minor}.{version.micro}"


def _vllm_version() -> str | None:
    try:
        from importlib.metadata import version

        return version("vllm")
    except Exception:
        return None


def _gpu_total_gib() -> float | None:
    try:
        result = subprocess.run(
            ["nvidia-smi", "--query-gpu=memory.total", "--format=csv,noheader,nounits"],
            capture_output=True,
            text=True,
            timeout=10,
        )
    except (OSError, subprocess.SubprocessError):
        return None
    if result.returncode != 0 or not result.stdout.strip():
        return None
    values = []
    for line in result.stdout.strip().splitlines():
        try:
            values.append(float(line.split()[0]) / 1024.0)
        except (ValueError, IndexError):
            continue
    return max(values) if values else None


def _model_weight_present(path: Path) -> bool:
    if not path.is_dir():
        return False
    patterns = ("*.safetensors", "*.bin", "*.pt", "*.gguf")
    for pattern in patterns:
        if any(path.glob(pattern)):
            return True
    return False


def _free_gib(path: Path) -> float | None:
    probe = path
    while not probe.exists() and probe.parent != probe:
        probe = probe.parent
    try:
        return shutil.disk_usage(probe).free / (1024 ** 3)
    except OSError:
        return None


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="KVFerry 实验装置环境自检")
    parser.add_argument("--tier", required=True, help="模型档 S / M / L")
    args = parser.parse_args(argv)
    try:
        tier = config.normalize_tier(args.tier)
    except ValueError as exc:
        print(f"错误: {exc}", file=sys.stderr)
        return 2

    results: list[bool] = []
    results.append(_check("Python", *_python_ok()))

    vllm = _vllm_version()
    results.append(
        _check(
            "vLLM",
            vllm == VLLM_REQUIRED_VERSION,
            f"已装 {vllm}，要求 {VLLM_REQUIRED_VERSION}",
        )
    )

    total_gib = _gpu_total_gib()
    minimum = config.MIN_GPU_GIB_BY_TIER[tier]
    if total_gib is None:
        results.append(_check("显存", False, "nvidia-smi 未返回总量"))
    else:
        results.append(
            _check(
                "显存",
                total_gib >= minimum,
                f"{total_gib:.1f} GiB，{tier} 档下限 {minimum} GiB",
            )
        )

    model_path = config.default_model_path(tier)
    results.append(
        _check("模型目录", _model_weight_present(model_path), str(model_path))
    )

    free = _free_gib(config.data_root())
    if free is None:
        results.append(_check("磁盘余量", False, f"无法读取 {config.data_root()}"))
    else:
        results.append(
            _check(
                "磁盘余量",
                free >= MIN_FREE_GIB,
                f"{free:.1f} GiB 可用，建议下限 {MIN_FREE_GIB:.0f} GiB",
            )
        )

    return 0 if all(results) else 1


if __name__ == "__main__":
    sys.exit(main())