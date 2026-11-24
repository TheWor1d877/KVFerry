"""被动统计聚合器：样本准入、窗口聚合与 T2 过期判定（D34）。"""

from __future__ import annotations

import bisect
import sys
import threading
import time
from dataclasses import dataclass, field

import config
import events
import recalib
import recompbench
import tables

_SOURCE = "passive"
_TABLE = "t2"
_MODE = "mixed"  # 与 T2 同档混批口径的 median 比对（D34）
_CLOSE_JOIN_TIMEOUT_S = 5.0


def _now() -> float:
    return time.time()


@dataclass(frozen=True)
class PrefillSample:
    computed_tokens: int
    segment_s: float  # prefill 段耗时，单位秒
    infight_blocks: int


@dataclass
class _Window:
    start_s: float
    samples: list[PrefillSample] = field(default_factory=list)


class PassiveAggregator:
    """嵌进决策服务进程，决策服务在每次 prefill 完成后调用 record。"""

    def __init__(
        self,
        cfg: config.RuntimeConfig,
        tables: tables.CostTables,
        engine: recompbench.EngineClient,
    ) -> None:
        self._cfg = cfg
        self._tables = tables
        self._engine = engine
        self._ladder: list[int] = []
        self._refresh_ladder()
        self._windows: dict[int, _Window] = {}  # 长度档 → 开着的窗口，按档独立
        self._consecutive: dict[int, int] = {}  # 长度档 → 越阈连续计数（D34）
        self._thread: threading.Thread | None = None  # 进程内同一时刻至多一个重标线程
        self._active: int | None = None  # 当前重标线程正在处理的档
        self._pending: list[int] = []  # 跨档排队，等当前线程跑完依次执行
        self._closed = False

    def record(self, sample: PrefillSample) -> None:
        if sample.infight_blocks != 0:
            # 传输在飞时 prefill 被拖累，聚合值无法与冷态表值比对（D34 准入）
            return
        length = self._map_length(sample.computed_tokens)
        now = _now()
        window = self._windows.get(length)
        if window is None:
            window = _Window(start_s=now)  # 该档首个有效样本到达时开窗
            self._windows[length] = window
        window.samples.append(sample)
        elapsed = now - window.start_s
        if (
            len(window.samples) >= self._cfg.drift_sample_min
            and elapsed >= self._cfg.drift_window_min_s
        ):
            # 开满下限时长且样本凑齐：聚合比对后关窗，样本不滚入下一窗口（D34）
            self._close_window(length)
        elif elapsed >= self._cfg.drift_window_max_s:
            # 窗口到上限仍不足样本：本窗口不产出比对、整池丢弃、连续计数
            # 不清零也不推进，随后重开新窗口从零计样本（D34）
            self._windows.pop(length, None)

    def close(self) -> None:
        self._closed = True
        self._pending.clear()
        self._windows.clear()  # 丢弃未关窗样本池
        thread = self._thread
        if thread is not None:
            thread.join(timeout=_CLOSE_JOIN_TIMEOUT_S)
            self._thread = None
            self._active = None

    def _map_length(self, computed_tokens: int) -> int:
        ladder = self._ladder
        if not ladder:
            raise RuntimeError("T2 无长度档，样本无法按档映射（D34）")
        if computed_tokens >= ladder[-1]:
            return ladder[-1]  # 超过 64k 的样本映射到 64k 直测档
        index = bisect.bisect_left(ladder, computed_tokens)
        if index == 0:
            return ladder[0]
        lo, hi = ladder[index - 1], ladder[index]
        if (computed_tokens - lo) < (hi - computed_tokens):
            return lo
        return hi  # 两档距离相等取更高档

    def _refresh_ladder(self) -> None:
        modes = self._tables.t2
        ladder_map = modes.get(_MODE) or (next(iter(modes.values())) if modes else {})
        self._ladder = sorted(ladder_map)

    def _close_window(self, length: int) -> None:
        window = self._windows.pop(length)
        # 聚合值为样本段耗时总和除以 token 总数（D34）
        total_tokens = sum(sample.computed_tokens for sample in window.samples)
        observed = sum(sample.segment_s for sample in window.samples) / total_tokens
        expected = self._tables.t2[_MODE][length].per_token_s_median
        deviation_pct = abs(observed - expected) / expected * 100.0
        if deviation_pct > self._cfg.drift_threshold_pct:
            self._consecutive[length] = self._consecutive.get(length, 0) + 1
        else:
            self._consecutive[length] = 0  # 未超阈窗口把连续计数清零
        if self._consecutive.get(length, 0) >= self._cfg.drift_trigger_consecutive:
            events.emit_expire(
                _SOURCE, _TABLE, str(length), expected, observed, deviation_pct
            )
            events.emit_degrade(_SOURCE, _TABLE, str(length), "start")
            self._start_recalib(length)

    def _start_recalib(self, length: int) -> None:
        # 重标在后台线程跑，决策服务的请求路径不能阻塞（D34）
        if self._thread is not None:
            if length == self._active or length in self._pending:
                return  # 同档重标线程在跑：只打事件不起线程
            self._pending.append(length)  # 跨档排队，避免两个重标同时打引擎互相污染
            return
        self._active = length
        self._thread = threading.Thread(
            target=self._recalib_worker, args=(length,), daemon=True
        )
        self._thread.start()

    def _recalib_worker(self, length: int) -> None:
        try:
            while True:
                self._recalib_once(length)
                if self._closed or not self._pending:
                    break
                length = self._pending.pop(0)
        finally:
            self._thread = None
            self._active = None

    def _recalib_once(self, length: int) -> None:
        ok = recalib.recalibrate_t2_length(
            self._cfg.table_root, length, _SOURCE, self._engine
        )
        if not ok:
            return  # 重标失败保持降级态，该档后续窗口再次越阈时重新触发
        try:
            # 成功后重载 T2，档位映射随新表刷新（D34）
            self._tables = tables.CostTables.load(
                self._cfg.table_root, self._cfg.declared_version
            )
            self._refresh_ladder()
        except (RuntimeError, OSError) as exc:
            print(f"重标成功但 T2 重载失败，沿用旧表: {exc}", file=sys.stderr)
