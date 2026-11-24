"""三类事件的 JSON 行输出（D35）。"""

from __future__ import annotations

import json
import sys
import time

COMPONENT = "cost"


def emit_expire(source: str, table: str, target: str, expected: float,
                observed: float, deviation_pct: float) -> None:
    _emit({
        "event": "expire",
        "component": COMPONENT,
        "ts_ms": _now_ms(),
        "source": source,
        "table": table,
        "target": target,
        "expected": expected,
        "observed": observed,
        "deviation_pct": abs(deviation_pct),  # 按 D35 偏差取绝对值
    })


def emit_recalibrate(source: str, table: str, target: str, phase: str,
                     result: str | None = None) -> None:
    payload: dict[str, object] = {
        "event": "recalibrate",
        "component": COMPONENT,
        "ts_ms": _now_ms(),
        "source": source,
        "table": table,
        "target": target,
        "phase": phase,
    }
    if result is not None:
        payload["result"] = result
    _emit(payload)


def emit_degrade(source: str, table: str, target: str, phase: str) -> None:
    _emit({
        "event": "degrade",
        "component": COMPONENT,
        "ts_ms": _now_ms(),
        "source": source,
        "table": table,
        "target": target,
        "phase": phase,
    })


def _now_ms() -> int:
    return int(time.time() * 1000)


def _emit(payload: dict[str, object]) -> None:
    sys.stdout.write(json.dumps(payload, ensure_ascii=False) + "\n")
    sys.stdout.flush()