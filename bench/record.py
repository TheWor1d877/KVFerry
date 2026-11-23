"""实验记录表与 run_meta.json 的写入通路（D8、D9、D13）。"""

from __future__ import annotations

import json
import os
import shutil
import time
from pathlib import Path
from typing import Any, Iterator

import config


def jsonl_line(obj: Any) -> str:
    return json.dumps(obj, ensure_ascii=False, separators=(",", ":")) + "\n"


def append_jsonl(path: Path, obj: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "a", encoding="utf-8", newline="\n") as handle:
        handle.write(jsonl_line(obj))
        handle.flush()
        os.fsync(handle.fileno())


def init_round_tables(run_dir: Path) -> dict[str, Path]:
    run_dir.mkdir(parents=True, exist_ok=True)
    paths: dict[str, Path] = {}
    for name in config.TABLES:
        path = run_dir / name
        path.touch(exist_ok=True)
        paths[name] = path
    return paths


def append_decision(run_dir: Path, record: dict[str, Any]) -> None:
    append_jsonl(run_dir / "decision.jsonl", record)


def append_fallback(run_dir: Path, record: dict[str, Any]) -> None:
    append_jsonl(run_dir / "fallback.jsonl", record)


def append_evict(run_dir: Path, record: dict[str, Any]) -> None:
    append_jsonl(run_dir / "evict.jsonl", record)


def append_drill(run_dir: Path, record: dict[str, Any]) -> None:
    append_jsonl(run_dir / "drill.jsonl", record)


def write_run_meta(run_dir: Path, meta: dict[str, Any]) -> Path:
    path = run_dir / "run_meta.json"
    tmp = run_dir / "run_meta.json.tmp"
    with open(tmp, "w", encoding="utf-8", newline="\n") as handle:
        json.dump(meta, handle, ensure_ascii=False, indent=2)
        handle.write("\n")
        handle.flush()
        os.fsync(handle.fileno())
    os.replace(tmp, path)
    return path


def read_run_meta(source: Path) -> dict[str, Any]:
    path = source / "run_meta.json" if source.is_dir() else source
    with open(path, "r", encoding="utf-8") as handle:
        return json.load(handle)


def iter_round_metas(
    tier: str, group: str | None = None
) -> Iterator[tuple[Path, dict[str, Any]]]:
    root = config.runs_root(tier)
    if not root.is_dir():
        return
    for group_dir in sorted(path for path in root.iterdir() if path.is_dir()):
        if group is not None and group_dir.name != group:
            continue
        for cell_dir in sorted(path for path in group_dir.iterdir() if path.is_dir()):
            for round_dir in sorted(path for path in cell_dir.iterdir() if path.is_dir()):
                if round_dir.name == "quarantine":
                    continue
                meta_path = round_dir / "run_meta.json"
                if not meta_path.is_file():
                    continue
                try:
                    yield round_dir, read_run_meta(meta_path)
                except (OSError, json.JSONDecodeError):
                    continue


def find_previous_round(
    tier: str, group: str, cell_id: str, mode: str, rep: int
) -> dict[str, Any] | None:
    base = config.cell_dir(tier, group, cell_id)
    if not base.is_dir():
        return None
    candidates: list[tuple[int, Path]] = []
    for path in base.iterdir():
        if not path.is_dir() or path.name == "quarantine":
            continue
        prefix = f"{mode}-"
        if not path.name.startswith(prefix):
            continue
        suffix = path.name[len(prefix):]
        if not suffix.isdigit():
            continue
        number = int(suffix)
        if number < rep:
            candidates.append((number, path))
    for _, path in sorted(candidates, reverse=True):
        meta_path = path / "run_meta.json"
        if not meta_path.is_file():
            continue
        try:
            meta = read_run_meta(meta_path)
        except (OSError, json.JSONDecodeError):
            continue
        validity = meta.get("validity") or {}
        if validity.get("valid") is True:
            return meta
    return None


def move_to_quarantine(
    run_dir: Path, tier: str, group: str, cell_id: str
) -> Path:
    target_root = config.quarantine_dir(tier, group, cell_id)
    target_root.mkdir(parents=True, exist_ok=True)
    target = target_root / run_dir.name
    if target.exists():
        target = target_root / f"{run_dir.name}-{int(time.time())}"
    shutil.move(str(run_dir), str(target))
    return target