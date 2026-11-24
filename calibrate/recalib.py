"""局部重标执行器与新版本落盘（R18、D29）。"""

from __future__ import annotations

import threading
from pathlib import Path

import events
import fetchbench
import recompbench
import tables

# 进程内重标串行；探针与被动分属两个进程，目录撞名由 write_version 的等待重试兜住
_RECALIB_LOCK = threading.Lock()


def recalibrate_t1_path(table_root: Path, path: str, trigger: str,
                        nominal_gib_s: float | None) -> bool:
    """单路径重标：重测拟合、以当前生效表为基线替换该路径行、落新版本目录。"""
    with _RECALIB_LOCK:
        events.emit_recalibrate(trigger, "t1", path, "start")
        try:
            # 与离线标定同款流程：同预热、同五次中位数、同三重检查（D33）
            fit = fetchbench.remeasure_path(path, nominal_gib_s)
            # 只替换受影响的路径行，未重标数据原样保留（R18）；基线取表库
            # 最新版本目录，即当前生效的 CostTables
            baseline = tables.CostTables.load(table_root, "unknown")
            baseline.t1[path] = fit
            # 落新版本目录，全部表文件 version 统一写为新目录版本号（D29）
            tables.write_version(table_root, baseline)
        except Exception:
            # 三重检查未过、测量异常或落盘失败：旧版本目录与降级状态保持，
            # 不补 degrade end，触发方按自己的周期重试（D35）
            events.emit_recalibrate(trigger, "t1", path, "end", "fail")
            return False
        events.emit_recalibrate(trigger, "t1", path, "end", "success")
        events.emit_degrade(trigger, "t1", path, "end")
        return True


def recalibrate_t2_length(table_root: Path, length_tokens: int,
                          trigger: str, engine: recompbench.EngineClient) -> bool:
    """单长度档重标：两种口径都重测、以当前生效表为基线替换该档格值、落新版本目录。"""
    with _RECALIB_LOCK:
        target = str(length_tokens)
        events.emit_recalibrate(trigger, "t2", target, "start")
        try:
            # 单档重标两种口径都重测（D34）
            cells = recompbench.remeasure_length(engine, length_tokens)
            baseline = tables.CostTables.load(table_root, "unknown")
            for mode, cell in cells.items():
                # 只替换受影响的长度档格值（R18）
                baseline.t2[mode][length_tokens] = cell
            tables.write_version(table_root, baseline)
        except Exception:
            events.emit_recalibrate(trigger, "t2", target, "end", "fail")
            return False
        events.emit_recalibrate(trigger, "t2", target, "end", "success")
        events.emit_degrade(trigger, "t2", target, "end")
        return True