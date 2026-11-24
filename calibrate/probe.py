"""主动探针进程：探针循环与 T1 过期判定（D33）。"""

from __future__ import annotations

import sys
import time

import config
import events
import fetchbench
import recalib
import tables

_GIB_BYTES = 1024**3
_MIB_BYTES = 1024**2


class ProbeLoop:
    """常驻探针主体：每 probe_interval_s 对每条传输路径比对实测耗时与表值。"""

    def __init__(
        self, cfg: config.RuntimeConfig, nominal: dict[str, float] | None = None
    ) -> None:
        self._cfg = cfg
        self._nominal = dict(nominal) if nominal else {}
        try:
            # 按 D30 加载表：声明版本与表文件 version 不一致时拒绝加载，
            # 探针没有表不启动，以退出码 1 结束
            self._tables = tables.CostTables.load(cfg.table_root, cfg.declared_version)
        except (RuntimeError, OSError) as exc:
            print(f"表加载失败，探针不启动: {exc}", file=sys.stderr)
            sys.exit(1)
        self._consecutive: dict[str, int] = {}  # 路径 → 越阈连续计数（D33）
        self._expired: set[str] = set()  # 过期态路径：跳过常规比对直接重试重标
        # 路径 → 最近一次观测 (表值, 实测, 偏差%)，供 expire 事件取值
        self._last: dict[str, tuple[float, float, float]] = {}

    def run(self) -> None:
        while True:
            self._tick()
            self._sleep(self._cfg.probe_interval_s)

    def _tick(self) -> None:
        for path in fetchbench.PATH_BLOCK_LADDERS:
            self._check_path(path)

    def _check_path(self, path: str) -> None:
        if path in self._expired:
            # 过期态跳过常规比对直接重试重标，直到某次成功（D33）
            self._recalibrate(path)
            return
        if self.probe_once(path, self._tables):
            self._consecutive[path] = self._consecutive.get(path, 0) + 1
        else:
            self._consecutive[path] = 0  # 不越阈窗口把连续计数清零（D33）
        if self._consecutive.get(path, 0) >= self._cfg.drift_trigger_consecutive:
            expected, observed, deviation_pct = self._last[path]
            events.emit_expire("probe", "t1", path, expected, observed, deviation_pct)
            events.emit_degrade("probe", "t1", path, "start")
            self._expired.add(path)
            # 重标同步执行，换取实现简单：探针进程没有其他职责
            self._recalibrate(path)

    def probe_once(self, path: str, tables: tables.CostTables) -> bool:
        # 参数名 tables 沿用设计签名（D33）；探针块固定 probe_block_mib 一个大小
        fit = tables.t1[path]
        block_mib = self._cfg.probe_block_mib
        sample = fetchbench.measure_block(path, block_mib)
        observed = _median(sample.elapsed_s)
        min_block_bytes = fit.min_block_mib * _MIB_BYTES
        if block_mib >= fit.min_block_mib:
            # 表值按 median 口径公式代入：块字节除以 median 带宽加 median 固定开销（R16）
            expected = (block_mib * _MIB_BYTES) / (
                fit.bandwidth_gib_s_median * _GIB_BYTES
            ) + fit.fixed_cost_s_median
        else:
            # 小于最小档字节按最小档直测耗时折算（D33）
            expected = fit.min_block_s_median * (
                (block_mib * _MIB_BYTES) / min_block_bytes
            )
        deviation_pct = abs(observed - expected) / expected * 100.0
        self._last[path] = (expected, observed, deviation_pct)
        # 偏差绝对值超过阈值记该窗口越阈（D33）
        return deviation_pct > self._cfg.drift_threshold_pct

    def _recalibrate(self, path: str) -> None:
        ok = recalib.recalibrate_t1_path(
            self._cfg.table_root, path, "probe", self._nominal.get(path)
        )
        if ok:
            try:
                # 成功后重新加载表，过期态解除，该路径恢复常规比对（D33）
                self._tables = tables.CostTables.load(
                    self._cfg.table_root, self._cfg.declared_version
                )
            except (RuntimeError, OSError) as exc:
                print(f"重标成功但表重载失败，沿用旧表: {exc}", file=sys.stderr)
            self._expired.discard(path)
            self._consecutive[path] = 0

    def _sleep(self, seconds: float) -> None:
        time.sleep(seconds)


def _median(values: tuple[float, ...]) -> float:
    ordered = sorted(values)
    return ordered[len(ordered) // 2]
