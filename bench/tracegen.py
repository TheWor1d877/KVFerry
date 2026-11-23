"""轨迹生成：底稿清洗、长度采样、前缀构造与合成、到达时刻缩放、落盘（D1、D2、D4~D7）。"""

from __future__ import annotations

import hashlib
import json
import math
import os
import random
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Iterable

import config

TIME_FIELDS_MS = ("t_ms", "arrival_ms", "arrival_time_ms")
TIME_FIELDS_S = ("timestamp", "arrival_time", "time", "t")
LENGTH_FIELDS = ("n_input", "input_length", "input_tokens", "tokens", "num_input_tokens")
SESSION_FIELDS = ("session", "session_id", "conversation_id", "trace_id")
PREFIX_FIELDS = ("hash_ids", "block_hashes", "prefix_hash", "prefix_hashes")

CALIBRATION_ATTEMPTS = 8


@dataclass
class BaseTrace:
    intervals_ms: list[float]
    lengths: list[int]
    sessions: list[str | None]
    prefix_keys: list[tuple[str, ...] | None]
    dropped: int


@dataclass
class GeneratedTrace:
    meta: dict[str, Any]
    requests: list[dict[str, Any]]


def _first_present(obj: dict[str, Any], names: Iterable[str]) -> tuple[str | None, Any]:
    for name in names:
        if name in obj and obj[name] is not None:
            return name, obj[name]
    return None, None


def _to_ms(field_name: str, value: Any) -> float:
    number = float(value)
    if field_name in TIME_FIELDS_MS:
        return number
    return number * 1000.0


def _to_prefix_key(value: Any) -> tuple[str, ...] | None:
    if value is None:
        return None
    if isinstance(value, str):
        return tuple(part for part in value.split(",") if part)
    if isinstance(value, (list, tuple)):
        return tuple(str(item) for item in value)
    return None


def load_base_trace(path: Path) -> BaseTrace:
    raw: list[tuple[float, int, str | None, tuple[str, ...] | None]] = []
    dropped = 0
    with open(path, "r", encoding="utf-8") as handle:
        for line in handle:
            line = line.strip()
            if not line:
                continue
            try:
                obj = json.loads(line)
            except json.JSONDecodeError:
                dropped += 1
                continue
            if not isinstance(obj, dict):
                dropped += 1
                continue
            time_field, time_value = _first_present(obj, TIME_FIELDS_MS + TIME_FIELDS_S)
            length_field, length_value = _first_present(obj, LENGTH_FIELDS)
            if time_field is None or length_field is None:
                dropped += 1
                continue
            try:
                arrival_ms = _to_ms(time_field, time_value)
                length = int(round(float(length_value)))
            except (TypeError, ValueError):
                dropped += 1
                continue
            if length <= 0 or not math.isfinite(arrival_ms):
                dropped += 1
                continue
            _, session_value = _first_present(obj, SESSION_FIELDS)
            _, prefix_value = _first_present(obj, PREFIX_FIELDS)
            session = None if session_value is None else str(session_value)
            raw.append((arrival_ms, length, session, _to_prefix_key(prefix_value)))
    if not raw:
        raise ValueError(f"底稿中没有可用请求: {path}")
    raw.sort(key=lambda item: item[0])
    intervals_ms = [
        max(0.0, raw[index][0] - raw[index - 1][0]) for index in range(1, len(raw))
    ]
    return BaseTrace(
        intervals_ms=intervals_ms,
        lengths=[item[1] for item in raw],
        sessions=[item[2] for item in raw],
        prefix_keys=[item[3] for item in raw],
        dropped=dropped,
    )



def scale_arrivals(
    base: BaseTrace, baseline_rate: float, load_tier: str, count: int
) -> list[float]:
    if baseline_rate <= 0:
        raise ValueError("baseline-rate 必须为正数")
    if count <= 0:
        return []
    if count == 1:
        return [0.0]
    intervals = base.intervals_ms[: count - 1]
    if not intervals:
        raise ValueError("底稿请求数不足以支撑目标请求数")
    mean_interval = sum(intervals) / len(intervals)
    if mean_interval <= 0:
        raise ValueError("底稿到达间隔全为 0，无法按基线完成速率缩放")
    target_rate = baseline_rate * config.load_scale_for(load_tier)
    target_mean_ms = 1000.0 / target_rate
    factor = target_mean_ms / mean_interval
    arrivals = [0.0]
    clock = 0.0
    for interval in intervals:
        clock += interval * factor
        arrivals.append(round(clock, 3))
    return arrivals


def load_corpus(path: Path) -> list[str]:
    lines: list[str] = []
    with open(path, "r", encoding="utf-8") as handle:
        for line in handle:
            text = line.strip()
            if text:
                lines.append(text)
    if not lines:
        raise ValueError(f"语料文件没有可用文本: {path}")
    return lines


def load_tokenizer(model_path: Path):
    from transformers import AutoTokenizer

    return AutoTokenizer.from_pretrained(str(model_path), trust_remote_code=True)


def _corpus_token_ids(tokenizer, lines: list[str], need: int) -> list[int]:
    ids: list[int] = []
    for line in lines:
        ids.extend(tokenizer.encode(line, add_special_tokens=False))
        if len(ids) >= need:
            break
    if not ids:
        raise ValueError("语料经 tokenizer 编码后为空")
    if len(ids) < need:
        repeats = need // len(ids) + 2
        ids = (ids * repeats)[:need]
    return ids


def _take(ids: list[int], start: int, count: int) -> list[int]:
    if count <= 0:
        return []
    total = len(ids)
    return [ids[(start + index) % total] for index in range(count)]


def _calibrate_text(
    tokenizer,
    ids: list[int],
    target: int,
    corpus_ids: list[int],
    body_start: int,
) -> tuple[str, int]:
    for _ in range(CALIBRATION_ATTEMPTS):
        text = tokenizer.decode(ids, skip_special_tokens=True)
        encoded = tokenizer.encode(text, add_special_tokens=False)
        delta = target - len(encoded)
        if delta == 0:
            return text, len(encoded)
        if delta < 0:
            ids = ids[: max(1, len(ids) + delta)]
        else:
            ids = ids + _take(corpus_ids, body_start + len(ids), delta)
    text = tokenizer.decode(ids, skip_special_tokens=True)
    return text, len(tokenizer.encode(text, add_special_tokens=False))


def _session_for(base: BaseTrace, index: int) -> str:
    session = base.sessions[index] if index < len(base.sessions) else None
    if session:
        return session
    return f"s{index:06d}"


def generate_cell(
    tier: str,
    cell: config.Cell,
    seed: int,
    base: BaseTrace,
    corpus_lines: list[str],
    tokenizer,
    baseline_rate: float,
    count: int | None,
) -> GeneratedTrace:
    token_limit = config.TOKEN_LIMIT_BY_TIER[tier]
    available = len(base.lengths)
    request_count = available if count is None else min(count, available)
    if request_count <= 0:
        raise ValueError("请求数必须为正数")
    if count is not None and count > available:
        raise ValueError(
            f"请求数 {count} 超过清洗后底稿可用请求数 {available}"
        )

    rng = random.Random(config.seed_for_cell(seed, cell.cell_id))
    lengths = sample_lengths(rng, cell.length_tier, request_count, token_limit)
    arrivals = scale_arrivals(base, baseline_rate, cell.load_tier, request_count)

    prefix_targets = [int(round(length * cell.prefix_ratio)) for length in lengths]
    max_prefix = max(prefix_targets) if prefix_targets else 0
    corpus_need = max_prefix + max(lengths) + 64
    corpus_ids = _corpus_token_ids(tokenizer, corpus_lines, corpus_need)
    shared_prefix = corpus_ids[:max_prefix]


    requests: list[dict[str, Any]] = []
    synthetic_tokens = 0
    total_tokens = 0
    for index in range(request_count):
        prefix_tokens = prefix_targets[index]
        body_tokens = lengths[index] - prefix_tokens
        body_start = rng.randrange(len(corpus_ids))
        ids = shared_prefix[:prefix_tokens] + _take(corpus_ids, body_start, body_tokens)
        text, final_length = _calibrate_text(
            tokenizer, ids, lengths[index], corpus_ids, body_start
        )
        synthetic_tokens += min(prefix_tokens, final_length)
        total_tokens += final_length
        requests.append(
            {
                "t_ms": arrivals[index],
                "n_input": final_length,
                "session": _session_for(base, index),
                "text": text,
            }
        )

    meta = {
        "prefix_ratio": cell.prefix_ratio,
        "length_tier": cell.length_tier,
        "load_tier": cell.load_tier,
        "model_tier": tier,
        "synthetic_ratio": round(synthetic_tokens / total_tokens, 6) if total_tokens else 0.0,
        "n_requests": len(requests),
        "seed": seed,
    }
    return GeneratedTrace(meta=meta, requests=requests)


def write_trace(path: Path, trace: GeneratedTrace) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_name(path.name + ".tmp")
    with open(tmp, "w", encoding="utf-8", newline="\n") as handle:
        handle.write(record_line(trace.meta))
        for request in trace.requests:
            handle.write(record_line(request))
        handle.flush()
        os.fsync(handle.fileno())
    os.replace(tmp, path)


def record_line(obj: Any) -> str:
    return json.dumps(obj, ensure_ascii=False, separators=(",", ":")) + "\n"


def print_plan(
    tier: str,
    cells: tuple[config.Cell, ...],
    baseline_rate: float | None,
    seed: int,
) -> None:
    print(f"tier={tier} cells={len(cells)} seed={seed}")
    print(
        "cell_id\tprefix_ratio\tlength_tier\tload_tier\tload_scale\ttarget_rate_req_s"
    )
    for cell in cells:
        scale = config.load_scale_for(cell.load_tier)
        if baseline_rate is None:
            rate = "未提供 baseline-rate"
        else:
            rate = f"{baseline_rate * scale:.4f}"
        print(
            f"{cell.cell_id}\t{cell.prefix_ratio}\t{cell.length_tier}"
            f"\t{cell.load_tier}\t{scale}\t{rate}"
        )


def run_tracegen(
    tier: str,
    cell_arg: str,
    base_trace_path: Path | None,
    corpus_path: Path | None,
    model_path: Path | None,
    baseline_rate: float | None,
    seed: int,
    n_requests: int | None,
    dry_run: bool,
) -> int:
    cells = config.resolve_cells(cell_arg)
    if dry_run:
        print_plan(tier, cells, baseline_rate, seed)
        return 0
    if base_trace_path is None:
        raise ValueError("tracegen 需要 --base-trace 指向 Mooncake 底稿 JSONL")
    if corpus_path is None:
        raise ValueError("tracegen 需要 --corpus 指向本地语料文件")
    if baseline_rate is None:
        raise ValueError("tracegen 需要 --baseline-rate（单位 req/s）")
    model_path = model_path or config.default_model_path(tier)

    base = load_base_trace(base_trace_path)
    corpus_lines = load_corpus(corpus_path)
    tokenizer = load_tokenizer(model_path)

    total = 0
    for cell in cells:
        trace = generate_cell(
            tier=tier,
            cell=cell,
            seed=seed,
            base=base,
            corpus_lines=corpus_lines,
            tokenizer=tokenizer,
            baseline_rate=baseline_rate,
            count=n_requests,
        )
        path = config.trace_path(tier, cell.cell_id)
        write_trace(path, trace)
        total += len(trace.requests)
        print(
            f"{cell.cell_id}\trequests={len(trace.requests)}"
            f"\tsynthetic_ratio={trace.meta['synthetic_ratio']}\t{path}"
        )
    print(f"完成 {len(cells)} 格，共 {total} 个请求")
    return 0