"""干扰对测、开销实测与定值扫描，产出 T3、T4 与 tuned（D28、D31、R19）。

引擎客户端经 set_engine 装配（run 装配三条标定链路）：标定注入真实客户端，
单测注入假客户端。真实客户端除协议两方法外另实现 clear_kv_pool，供每格
测前清一次引擎 KV 池；句柄无该能力时清池跳过，协议两方法不变。
"""

from __future__ import annotations

import json
import mmap
import os
import socket
import tempfile
import threading
import time
import urllib.request
from pathlib import Path

import recompbench
import tables

DECODE_BATCH_LADDER = (1, 2, 4, 8, 16, 32)
PREFILL_CHUNK_LADDER_TOKENS = (512, 1024, 2048, 4096)
INFIGHT_BLOCKS_LADDER = (1, 2, 4, 8, 16)
REVERSE_PROBE_PATH = "nvme-direct"
GATE_PCT = 5.0
HYSTERESIS_LADDER = (0.6, 0.7, 0.8, 0.9, 1.0)

_MEASURE_REPS = 5  # 两个测量函数每格测 5 次取中位数
_FORWARD_BLOCK_MIB = 64  # 正向空载与负载对测的块大小
_INFIGHT_BLOCK_MIB = 64  # 反向在飞块的块大小，与正向对测块一致
_SATURATION_BANDWIDTH_RATIO = 0.9  # 并发上限的正向带宽保底：不低于空载 90%
_LOOKUP_LOOP_N = 100000  # lookup_s 的查表循环轮数（D28）
_RPC_PROBE_N = 100  # rpc_s 的探测次数（D28）
_RPC_TIMEOUT_S = 2
_OVERHEAD_FETCH_BYTES = 64 * 1024 * 1024
_OVERHEAD_TOKENS = 4096
_REPLAY_KV_BYTES_PER_TOKEN = 256 * 1024  # 重放 KV 字节换算，真机标定前需裁决口径
_REPLAY_FETCH_PATH = REVERSE_PROBE_PATH  # 重放取回账固定一条可复现路径
_REPLAY_MODE = "mixed"  # 重算账取混批口径：决策语境引擎常驻 decode
_D28_FORWARD_COLUMNS = (0, 512, 1024, 2048, 4096)
_D28_DECODE_BATCH = (1, 2, 4, 8, 16, 32)
_D28_INFIGHT_BLOCKS = (1, 2, 4, 8, 16)

_engine: recompbench.EngineClient | None = None
_BLOCK_FILES: dict[int, Path] = {}


def set_engine(engine: recompbench.EngineClient) -> None:
    """装配引擎客户端，build_t3 与两个测量函数经模块句柄访问引擎。"""
    global _engine
    _engine = engine


def measure_forward_discount(
    path: str, decode_batch: int, prefill_tokens: int
) -> float:
    # 先空载测该路径 64 MiB 块传输耗时，再挂 decode_batch 路 decode 与每步
    # prefill_tokens 段量的 prefill 负载重测同一块，负载除以空载得折扣（D28）
    empty = [_transfer_once(path, _FORWARD_BLOCK_MIB) for _ in range(_MEASURE_REPS)]
    loaded = [
        _transfer_under_load_s(path, _FORWARD_BLOCK_MIB, decode_batch, prefill_tokens)
        for _ in range(_MEASURE_REPS)
    ]
    base = _median(tuple(empty))
    if base <= 0:
        raise RuntimeError(f"空载块耗时非正: {path}")
    return _median(tuple(loaded)) / base


def measure_reverse_amplification(decode_batch: int, infight_blocks: int) -> float:
    # 先单独跑 decode_batch 路 decode 记步时间基线，再叠加 infight_blocks 个
    # 在飞块传输重记步时间，两者之差除以基线得增幅（D28）；传输固定走
    # REVERSE_PROBE_PATH 一条路径，路径间带宽差异由 T1 承担
    if _engine is None:
        raise RuntimeError("引擎客户端未装配，反向增幅测量不可用")
    baseline = [_engine.decode_batch(decode_batch, 0) for _ in range(_MEASURE_REPS)]
    loaded = [
        _decode_step_under_transfers(decode_batch, infight_blocks)
        for _ in range(_MEASURE_REPS)
    ]
    base = _median(tuple(baseline))
    if base <= 0:
        raise RuntimeError("decode 基线步时间非正")
    return (_median(tuple(loaded)) - base) / base


def measure_decision_overhead(
    tables: tables.CostTables, rpc_endpoint: str
) -> dict[str, float]:
    # lookup_s 循环查表取均值（进程内纯算术），每轮查 T1 与 T2 各一笔，
    # 对应一次决策的查表算术；rpc_s 对端点发探测请求取中位数（D28）
    start = time.perf_counter()
    for _ in range(_LOOKUP_LOOP_N):
        tables.lookup_fetch(_REPLAY_FETCH_PATH, _OVERHEAD_FETCH_BYTES)
        tables.lookup_recompute(_REPLAY_MODE, _OVERHEAD_TOKENS)
    lookup_s = (time.perf_counter() - start) / _LOOKUP_LOOP_N
    probes = [_probe_rpc_once(rpc_endpoint) for _ in range(_RPC_PROBE_N)]
    return {"lookup_s": lookup_s, "rpc_s": _median(tuple(probes))}


def sweep_tuned(trace_path: Path, tables: tables.CostTables) -> dict[str, float]:
    inputs = _load_trace_requests(trace_path)
    if not inputs:
        raise RuntimeError(f"轨迹没有可重放请求: {trace_path}")
    # 迟滞五档各重放一遍轨迹（D31）：取翻转最少且首 token 时间不劣的一档，
    # 并列时按 (翻转次数, 首 token 均值) 字典序取最小
    best_key: tuple[int, float] | None = None
    best_h: float | None = None
    for hysteresis in HYSTERESIS_LADDER:
        flips, ttft_sum = _replay_once(hysteresis, tables, inputs)
        key = (flips, ttft_sum / len(inputs))
        if best_key is None or key < best_key:
            best_key, best_h = key, hysteresis
    return {
        "hysteresis": float(best_h),
        "gate_pct": GATE_PCT,  # decode 步时间增幅的容忍值（D31）
        "concurrency_cap": _concurrency_cap(tables),
    }


def build_t3(
    path: str,
) -> tuple[dict[int, dict[int, float]], dict[int, dict[int, float]]]:
    # 正向行是 decode 批、列是 prefill 段量加纯计算列，在飞块数维度不进
    # 正向子表（R19）；反向行是在飞块数、列是 decode 批（D28）。
    # 格间按行优先固定顺序，每格测前清一次引擎 KV 池，保证重跑可对账
    forward: dict[int, dict[int, float]] = {}
    for batch in DECODE_BATCH_LADDER:
        row: dict[int, float] = {}
        for chunk in (*PREFILL_CHUNK_LADDER_TOKENS, 0):
            _clear_engine_kv_pool()
            row[chunk] = measure_forward_discount(path, batch, chunk)
        forward[batch] = row
    reverse: dict[int, dict[int, float]] = {}
    for blocks in INFIGHT_BLOCKS_LADDER:
        row = {}
        for batch in DECODE_BATCH_LADDER:
            _clear_engine_kv_pool()
            row[batch] = measure_reverse_amplification(batch, blocks)
        reverse[blocks] = row
    _verify_t3_shape(forward, reverse)
    return forward, reverse


def _verify_t3_shape(
    forward: dict[int, dict[int, float]], reverse: dict[int, dict[int, float]]
) -> None:
    # 行列按 D28 契约值硬编码校验，与梯子常量漂移时抛错拒绝出表
    if set(forward) != set(_D28_DECODE_BATCH) or any(
        set(row) != set(_D28_FORWARD_COLUMNS) for row in forward.values()
    ):
        raise RuntimeError(f"正向子表行列与 D28 不符: {sorted(forward)}")
    if set(reverse) != set(_D28_INFIGHT_BLOCKS) or any(
        set(row) != set(_D28_DECODE_BATCH) for row in reverse.values()
    ):
        raise RuntimeError(f"反向子表行列与 D28 不符: {sorted(reverse)}")


def _clear_engine_kv_pool() -> None:
    # 每格测前清一次引擎 KV 池（D28）：前一格的 KV 状态影响后一格；
    # 引擎句柄带 clear_kv_pool 能力时执行，否则跳过
    if _engine is None:
        return
    clear = getattr(_engine, "clear_kv_pool", None)
    if callable(clear):
        clear()


def _replay_once(
    hysteresis: float, tables: tables.CostTables, inputs: list[int]
) -> tuple[int, float]:
    # 重放循环查 T1 与 T2 算两笔账（D31）：两笔账都走 p25 查表（R16），
    # 不含排队延迟与折扣增幅，离线重放没有快照输入（R20）
    last: str | None = None
    flips = 0
    ttft_sum = 0.0
    for tokens in inputs:
        fetch_s = tables.lookup_fetch(
            _REPLAY_FETCH_PATH, tokens * _REPLAY_KV_BYTES_PER_TOKEN
        )
        recompute_s = tables.lookup_recompute(_REPLAY_MODE, tokens)
        cheaper = "fetch" if fetch_s <= recompute_s else "recompute"
        gap = min(fetch_s, recompute_s) / max(fetch_s, recompute_s)
        if gap > hysteresis and last is not None:
            action = last  # 两笔耗时的差距落在迟滞带内，沿用上次决策
        else:
            action = cheaper
        if last is not None and action != last:
            flips += 1
        last = action
        ttft_sum += fetch_s if action == "fetch" else recompute_s
    return flips, ttft_sum


def _concurrency_cap(tables: tables.CostTables) -> float:
    # 正向网格取带宽不低于空载 90% 的最高负载格的 decode 批，反向网格取
    # 行内所有 decode 批增幅都不超过闸门的最高在飞块数，两者取较小值（D31）
    if not tables.t3_forward or not tables.t3_reverse:
        raise RuntimeError("正向或反向子表为空，并发上限无从判定")
    bandwidth_floor = 1.0 / _SATURATION_BANDWIDTH_RATIO
    forward_cells = [
        (batch, chunk)
        for batch, row in tables.t3_forward.items()
        for chunk, discount in row.items()
        if discount <= bandwidth_floor
    ]
    if not forward_cells:
        raise RuntimeError("正向网格没有带宽不低于空载 90% 的负载格")
    forward_cap = max(forward_cells)[0]
    reverse_rows = [
        blocks
        for blocks, row in tables.t3_reverse.items()
        if row and max(row.values()) <= GATE_PCT / 100.0
    ]
    if not reverse_rows:
        raise RuntimeError("反向网格没有增幅不超过闸门的在飞块数")
    return float(min(forward_cap, max(reverse_rows)))


def _load_trace_requests(trace_path: Path) -> list[int]:
    # bench 轨迹 JSONL：首行 meta，其余行每行一个请求，重放只取 n_input
    inputs: list[int] = []
    expected: int | None = None
    with open(trace_path, encoding="utf-8") as handle:
        for lineno, line in enumerate(handle, 1):
            line = line.strip()
            if not line:
                continue
            try:
                obj = json.loads(line)
            except ValueError as exc:
                raise RuntimeError(
                    f"轨迹第 {lineno} 行不是合法 JSON: {trace_path}"
                ) from exc
            if not isinstance(obj, dict):
                raise RuntimeError(f"轨迹第 {lineno} 行不是 JSON 对象: {trace_path}")
            if "n_requests" in obj:
                expected = int(obj["n_requests"])
                continue
            if "n_input" not in obj:
                raise RuntimeError(f"轨迹第 {lineno} 行缺 n_input: {trace_path}")
            inputs.append(int(obj["n_input"]))
    if expected is None:
        raise RuntimeError(f"轨迹缺 meta 首行: {trace_path}")
    if expected != len(inputs):
        raise RuntimeError(
            f"轨迹请求数与 meta 不一致: 声明 {expected}，实际 {len(inputs)}"
        )
    return inputs


def _transfer_under_load_s(
    path: str, block_mib: int, decode_batch: int, prefill_tokens: int
) -> float:
    if _engine is None:
        raise RuntimeError("引擎客户端未装配，正向负载对测不可用")
    stop = threading.Event()

    def drive_load() -> None:
        while not stop.is_set():
            _engine.decode_batch(decode_batch, prefill_tokens)

    thread = threading.Thread(target=drive_load)
    thread.start()
    try:
        return _transfer_once(path, block_mib)
    finally:
        stop.set()
        thread.join(timeout=30)


def _decode_step_under_transfers(decode_batch: int, infight_blocks: int) -> float:
    stop = threading.Event()

    def drive_transfers() -> None:
        while not stop.is_set():
            _transfer_once(REVERSE_PROBE_PATH, _INFIGHT_BLOCK_MIB)

    threads = [threading.Thread(target=drive_transfers) for _ in range(infight_blocks)]
    for thread in threads:
        thread.start()
    try:
        return _engine.decode_batch(decode_batch, 0)
    finally:
        stop.set()
        for thread in threads:
            thread.join(timeout=30)


def _transfer_once(path: str, block_mib: int) -> float:
    block = block_mib * 1024 * 1024
    if path == "dram":
        return _dram_copy_s(block)
    if path == "nvme-direct":
        return _nvme_direct_read_s(block)
    if path == "nvme-staged":
        return _nvme_staged_s(block)
    if path == "tcp-loopback":
        return _tcp_loopback_s(block)
    raise RuntimeError(f"未知传输路径: {path}")


def _torch():
    try:
        import torch
    except ImportError as exc:
        raise RuntimeError(
            "dram 与 nvme-staged 路径需要 torch 做异步拷贝进显存"
        ) from exc
    if not torch.cuda.is_available():
        raise RuntimeError("dram 与 nvme-staged 路径需要可用 CUDA 设备")
    return torch


def _block_source_file(block: int) -> Path:
    path = _BLOCK_FILES.get(block)
    if path is not None and path.is_file():
        return path
    fd, raw = tempfile.mkstemp(prefix="kvferry-block-", suffix=".bin")
    os.close(fd)
    path = Path(raw)
    try:
        with open(path, "wb") as handle:
            handle.write(b"\0" * block)
            handle.flush()
            os.fsync(handle.fileno())
    except Exception:
        path.unlink(missing_ok=True)
        raise
    _BLOCK_FILES[block] = path
    return path


def _open_direct(path: Path):
    return open(
        path, "rb", buffering=0, opener=lambda p, flags: os.open(p, flags | os.O_DIRECT)
    )


def _dram_copy_s(block: int) -> float:
    torch = _torch()
    cpu = torch.empty(block, dtype=torch.uint8, pin_memory=True)
    gpu = torch.empty(block, dtype=torch.uint8, device="cuda")
    start = torch.cuda.Event(enable_timing=True)
    end = torch.cuda.Event(enable_timing=True)
    start.record()
    gpu.copy_(cpu, non_blocking=True)
    end.record()
    torch.cuda.synchronize()
    return start.elapsed_time(end) / 1000.0


def _nvme_direct_read_s(block: int) -> float:
    source = _block_source_file(block)
    buf = mmap.mmap(-1, block)
    try:
        with _open_direct(source) as handle:
            start = time.perf_counter()
            got = handle.readinto(memoryview(buf))
            elapsed = time.perf_counter() - start
        if got != block:
            raise RuntimeError(f"块文件读短: {source}")
        return elapsed
    finally:
        buf.close()


def _nvme_staged_s(block: int) -> float:
    torch = _torch()
    source = _block_source_file(block)
    buf = mmap.mmap(-1, block)
    try:
        with _open_direct(source) as handle:
            t0 = time.perf_counter()
            got = handle.readinto(memoryview(buf))
            if got != block:
                raise RuntimeError(f"块文件读短: {source}")
            staged = torch.frombuffer(memoryview(buf), dtype=torch.uint8)
            gpu = torch.empty(block, dtype=torch.uint8, device="cuda")
            start = torch.cuda.Event(enable_timing=True)
            end = torch.cuda.Event(enable_timing=True)
            start.record()
            gpu.copy_(staged, non_blocking=True)
            end.record()
            torch.cuda.synchronize()
            return (time.perf_counter() - t0) + start.elapsed_time(end) / 1000.0
    finally:
        buf.close()


def _tcp_loopback_s(block: int) -> float:
    payload = b"\0" * block
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as server:
        server.bind(("127.0.0.1", 0))
        server.listen(1)
        port = server.getsockname()[1]

        def sink() -> None:
            conn, _ = server.accept()
            with conn:
                conn.settimeout(30)
                received = 0
                while received < block:
                    data = conn.recv(1024 * 1024)
                    if not data:
                        break
                    received += len(data)

        thread = threading.Thread(target=sink)
        thread.start()
        with socket.create_connection(("127.0.0.1", port), timeout=30) as client:
            start = time.perf_counter()
            client.sendall(payload)
            thread.join(timeout=30)
            elapsed = time.perf_counter() - start
        if thread.is_alive():
            raise RuntimeError("loopback 接收线程超时未结束")
        return elapsed


def _probe_rpc_once(endpoint: str) -> float:
    url = (endpoint or "").rstrip("/") + "/health"
    start = time.perf_counter()
    with urllib.request.urlopen(url, timeout=_RPC_TIMEOUT_S) as resp:
        resp.read()
    return time.perf_counter() - start


def _median(values: tuple[float, ...]) -> float:
    ordered = sorted(values)
    return ordered[round((len(ordered) - 1) * 0.5)]
