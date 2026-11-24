"""表内存结构、版本目录落盘与加载校验（D26、D27、D28、D29）。"""

from __future__ import annotations

import hashlib
import json
import os
import re
import shutil
import time
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from pathlib import Path

import config

T1_FILE = "t1_fetch.json"
T2_FILE = "t2_recompute.json"
T3_FILE = "t3_interference.json"
T4_FILE = "t4_decision.json"
TUNED_FILE = "tuned.json"
MANIFEST_FILE = "manifest.json"
TABLE_FILES = (T1_FILE, T2_FILE, T3_FILE, T4_FILE, TUNED_FILE)

_DIR_NAME_RE = re.compile(r"^\d{8}T\d{6}Z$")
_UNKNOWN = "unknown"
_COLLISION_RETRIES = 3
_GIB_BYTES = 1024 ** 3
_MIB_BYTES = 1024 ** 2


@dataclass(frozen=True)
class PathFit:
    bandwidth_gib_s_median: float
    bandwidth_gib_s_p25: float
    fixed_cost_s_median: float
    fixed_cost_s_p25: float
    min_block_mib: int           # 最小档字节，直测档
    min_block_s_median: float    # 最小档实测耗时，秒
    min_block_s_p25: float


@dataclass(frozen=True)
class RecomputeCell:
    per_token_s_median: float
    per_token_s_p25: float


@dataclass
class CostTables:
    version: str
    provenance: config.Provenance
    calibrated_at: str         # ISO 8601 UTC 时刻
    t1: dict[str, PathFit]                    # 键为路径名，值为拟合参数与最小档直测值
    t2: dict[str, dict[int, RecomputeCell]]   # 外层键为口径，内层键为长度档（token 数）
    t3_forward: dict[int, dict[int, float]]   # 外层键为 decode 批，内层键为 prefill 段量（token 数，0 为纯计算列），值为折扣
    t3_reverse: dict[int, dict[int, float]]   # 外层键为在飞块数，内层键为 decode 批，值为增幅
    t4: dict[str, float]                      # lookup_s 与 rpc_s 两键
    tuned: dict[str, float]                   # hysteresis、gate_pct、concurrency_cap 三键

    @classmethod
    def load(cls, table_root: Path, declared_version: str) -> CostTables:
        version_dir = _resolve_version_dir(table_root, declared_version)
        payloads: dict[str, dict] = {}
        for name in TABLE_FILES:
            payload = _read_json(version_dir / name)
            if declared_version != _UNKNOWN and payload.get("version") != declared_version:
                raise RuntimeError(
                    f"{name} 内 version {payload.get('version')!r} 与声明版本 "
                    f"{declared_version!r} 不一致，拒绝加载（D30）"
                )
            payloads[name] = payload
        manifest = _read_json(version_dir / MANIFEST_FILE)
        if manifest.get("version") != version_dir.name:
            raise RuntimeError(
                f"manifest 目录版本号 {manifest.get('version')!r} 与目录名 "
                f"{version_dir.name!r} 不一致（D29）"
            )
        manifest_files = manifest.get("files")
        if not isinstance(manifest_files, dict):
            raise RuntimeError(f"manifest 缺少文件记录（D29）: {version_dir / MANIFEST_FILE}")
        for name in TABLE_FILES:
            entry = manifest_files.get(name)
            if not isinstance(entry, dict):
                raise RuntimeError(f"manifest 缺少 {name} 记录（D29）")
            if _sha256_file(version_dir / name) != entry.get("sha256"):
                raise RuntimeError(f"{name} SHA-256 与 manifest 记录不一致（D29）")
            if payloads[name].get("version") != entry.get("version"):
                raise RuntimeError(f"{name} version 与 manifest 记录不一致（D29）")
        head = payloads[T1_FILE]
        provenance = config.Provenance(
            gpu_name=str(head["gpu_name"]),
            nic=str(head["nic"]),
            pcie_topology=str(head["pcie_topology"]),
            driver_version=str(head["driver_version"]),
            engine_version=str(head["engine_version"]),
            model=str(head["model"]),
            kv_precision=str(head["kv_precision"]),
            block_tokens=int(head["block_tokens"]),
        )
        t1 = {path: PathFit(**body) for path, body in head["t1"].items()}
        t2 = {
            mode: {int(length): RecomputeCell(**cell) for length, cell in cells.items()}
            for mode, cells in payloads[T2_FILE]["t2"].items()
        }
        t3_payload = payloads[T3_FILE]
        t3_forward = {
            int(batch): {int(chunk): float(value) for chunk, value in row.items()}
            for batch, row in t3_payload["t3_forward"].items()
        }
        t3_reverse = {
            int(blocks): {int(batch): float(value) for batch, value in row.items()}
            for blocks, row in t3_payload["t3_reverse"].items()
        }
        t4 = {str(key): float(value) for key, value in payloads[T4_FILE]["t4"].items()}
        tuned = {str(key): float(value) for key, value in payloads[TUNED_FILE]["tuned"].items()}
        return cls(
            version=str(manifest["version"]),
            provenance=provenance,
            calibrated_at=str(head["calibrated_at"]),
            t1=t1,
            t2=t2,
            t3_forward=t3_forward,
            t3_reverse=t3_reverse,
            t4=t4,
            tuned=tuned,
        )

    # 查询统一走 p25（R16）；返回值不含排队延迟，排队延迟由调用方按快照叠加（R20）
    def lookup_fetch(self, path: str, fetch_bytes: int) -> float:
        fit = self.t1.get(path)
        if fit is None:
            raise RuntimeError(f"T1 无该路径: {path}")
        min_block_bytes = fit.min_block_mib * _MIB_BYTES
        if fetch_bytes < min_block_bytes:
            return fit.min_block_s_p25 * (fetch_bytes / min_block_bytes)
        return fetch_bytes / (fit.bandwidth_gib_s_p25 * _GIB_BYTES) + fit.fixed_cost_s_p25

    def lookup_recompute(self, mode: str, computed_tokens: int) -> float:
        ladder_map = self.t2.get(mode)
        if ladder_map is None:
            raise RuntimeError(f"T2 无该口径: {mode}")
        ladder = sorted(ladder_map)
        if not ladder:
            raise RuntimeError(f"T2 口径 {mode} 无长度档")
        direct = ladder[-1]
        if computed_tokens >= direct:
            return ladder_map[direct].per_token_s_p25 * computed_tokens
        if computed_tokens <= ladder[0]:
            return ladder_map[ladder[0]].per_token_s_p25 * computed_tokens
        lo = max(k for k in ladder if k <= computed_tokens)
        hi = min(k for k in ladder if k >= computed_tokens)
        per_lo = ladder_map[lo].per_token_s_p25
        per_hi = ladder_map[hi].per_token_s_p25
        if hi == lo:
            per = per_lo
        else:
            per = per_lo + (per_hi - per_lo) * ((computed_tokens - lo) / (hi - lo))
        return per * computed_tokens


def write_version(table_root: Path, tables: CostTables) -> Path:
    table_root.mkdir(parents=True, exist_ok=True)
    for attempt in range(_COLLISION_RETRIES + 1):
        dirname = _format_timestamp(_now_utc())
        final = table_root / dirname
        if final.exists():
            if attempt == _COLLISION_RETRIES:
                raise RuntimeError(f"版本目录名连续撞名: {dirname}（D29）")
            time.sleep(1)
            continue
        staging = table_root / f".{dirname}.tmp"
        try:
            staging.mkdir()
            tables.version = dirname  # 按 D30：表文件 version 与目录版本号一致
            manifest_files: dict[str, dict[str, str]] = {}
            for name, payload in _table_payloads(tables).items():
                _write_file(staging / name, _json_text(payload))
                manifest_files[name] = {
                    "version": str(payload["version"]),
                    "sha256": _sha256_file(staging / name),
                }
            manifest = {"version": dirname, "files": manifest_files}
            _write_file(staging / MANIFEST_FILE, _json_text(manifest))
            _fsync_dir(staging)
            os.rename(staging, final)
            return final
        except Exception:
            shutil.rmtree(staging, ignore_errors=True)
            raise
    raise RuntimeError("write_version 未产出版本目录")


def verify_dir(version_dir: Path, declared_version: str) -> None:
    if not _DIR_NAME_RE.fullmatch(version_dir.name):
        raise RuntimeError(f"目录名不符合版本号格式: {version_dir.name}（D29）")
    try:
        datetime.strptime(version_dir.name, "%Y%m%dT%H%M%SZ")
    except ValueError:
        raise RuntimeError(f"目录名不是合法 UTC 时刻: {version_dir.name}（D29）") from None
    manifest_path = version_dir / MANIFEST_FILE
    if not manifest_path.is_file():
        raise RuntimeError(f"manifest 缺失: {manifest_path}（D29）")
    manifest = _read_json(manifest_path)
    if manifest.get("version") != version_dir.name:
        raise RuntimeError(
            f"manifest 目录版本号 {manifest.get('version')!r} 与目录名 "
            f"{version_dir.name!r} 不一致（D29）"
        )
    if declared_version != _UNKNOWN and declared_version != version_dir.name:
        raise RuntimeError(
            f"声明版本 {declared_version!r} 与目录版本 {version_dir.name!r} 不一致（D30）"
        )
    manifest_files = manifest.get("files")
    if not isinstance(manifest_files, dict):
        raise RuntimeError(f"manifest 缺少文件记录（D29）: {manifest_path}")
    for name in TABLE_FILES:
        entry = manifest_files.get(name)
        if not isinstance(entry, dict):
            raise RuntimeError(f"manifest 缺少 {name} 记录（D29）")
        table_path = version_dir / name
        if not table_path.is_file():
            raise RuntimeError(f"表文件缺失: {table_path}（D29）")
        if _sha256_file(table_path) != entry.get("sha256"):
            raise RuntimeError(f"{name} SHA-256 与 manifest 记录不一致（D29）")


def _resolve_version_dir(table_root: Path, declared_version: str) -> Path:
    if declared_version != _UNKNOWN:
        version_dir = table_root / declared_version
        if not version_dir.is_dir():
            raise RuntimeError(f"声明的版本目录不存在: {version_dir}（D30、D36）")
        return version_dir
    try:
        candidates = sorted(
            (p for p in table_root.iterdir()
             if p.is_dir() and _DIR_NAME_RE.fullmatch(p.name)),
            key=lambda p: p.name,
        )
    except OSError as exc:
        raise RuntimeError(f"表库根目录不可读: {table_root}（D36）") from exc
    if not candidates:
        raise RuntimeError(f"表库根目录下没有版本目录: {table_root}")
    return candidates[-1]


def _table_payloads(tables: CostTables) -> dict[str, dict[str, object]]:
    provenance = tables.provenance
    base: dict[str, object] = {
        "version": tables.version,
        "calibrated_at": tables.calibrated_at,
        "gpu_name": provenance.gpu_name,
        "nic": provenance.nic,
        "pcie_topology": provenance.pcie_topology,
        "driver_version": provenance.driver_version,
        "engine_version": provenance.engine_version,
        "model": provenance.model,
        "kv_precision": provenance.kv_precision,
        "block_tokens": provenance.block_tokens,
    }
    return {
        T1_FILE: {
            **base,
            "t1": {path: asdict(fit) for path, fit in tables.t1.items()},
        },
        T2_FILE: {
            **base,
            "t2": {
                mode: {length: asdict(cell) for length, cell in cells.items()}
                for mode, cells in tables.t2.items()
            },
        },
        T3_FILE: {
            **base,
            "t3_forward": tables.t3_forward,
            "t3_reverse": tables.t3_reverse,
        },
        T4_FILE: {**base, "t4": tables.t4},
        TUNED_FILE: {**base, "tuned": tables.tuned},
    }


def _read_json(path: Path) -> dict:
    try:
        text = path.read_text(encoding="utf-8")
    except OSError as exc:
        raise RuntimeError(f"文件不可读: {path}") from exc
    try:
        payload = json.loads(text)
    except ValueError as exc:
        raise RuntimeError(f"文件不是合法 JSON: {path}") from exc
    if not isinstance(payload, dict):
        raise RuntimeError(f"文件顶层不是 JSON 对象: {path}")
    return payload


def _sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with open(path, "rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _write_file(path: Path, text: str) -> None:
    with open(path, "w", encoding="utf-8") as handle:
        handle.write(text)
        handle.flush()
        os.fsync(handle.fileno())


def _fsync_dir(path: Path) -> None:
    fd = os.open(path, os.O_RDONLY)
    try:
        os.fsync(fd)
    finally:
        os.close(fd)


def _now_utc() -> datetime:
    return datetime.now(timezone.utc)


def _format_timestamp(moment: datetime) -> str:
    return moment.strftime("%Y%m%dT%H%M%SZ")


def _json_text(payload: dict[str, object]) -> str:
    return json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True) + "\n"