"""存储写基准：数据目录定长段顺序写实测，产出 T1 写方向列（D39）。

写方向只有数据目录一条：KV 数据落在部署的 NVMe 数据目录，写上限按该
目录实测；DRAM 读写对称，中转与 loopback 是读路径形态，均不设写档。
拟合、三重检查与加密档位复用 fetchbench 的读口径规则（D39）。
"""

from __future__ import annotations

import mmap
import os
import time
from pathlib import Path

import config
import fetchbench
import tables

WRITE_BLOCK_LADDER_MIB = (8, 64, 256, 1024, 4096)  # 与读口径 nvme-direct 同五档（D39）
WRITE_ROW = "nvme-write"  # T1 写方向列组内的数据目录顺序写行（D39）
MEASURE_REPS = 5

_MIB_BYTES = 1024**2
_IO_CHUNK_BYTES = 64 * _MIB_BYTES
_WRITE_DIR = ".writebench"


def measure_write_block(
    block_mib: int, reps: int = MEASURE_REPS
) -> fetchbench.BlockSample:
    block_bytes = block_mib * _MIB_BYTES
    times = _timed_direct_writes(_write_target(block_mib), block_bytes, reps)
    return fetchbench.BlockSample(
        path=WRITE_ROW, block_mib=block_mib, elapsed_s=tuple(times)
    )


def build_write_fit(nominal_gib_s: float | None = None) -> tables.PathFit:
    # 拟合、三重检查与加密档位沿用读口径规则（D39）：复用 fetchbench 的
    # fit_bandwidth、check_fit 与加密轮数上限，测量函数换成数据目录顺序写
    samples: dict[int, fetchbench.BlockSample] = {
        block: measure_write_block(block) for block in WRITE_BLOCK_LADDER_MIB
    }
    fit = fetchbench.fit_bandwidth(list(samples.values()))
    violations = fetchbench.check_fit(list(samples.values()), fit, nominal_gib_s)
    rounds = 0
    while violations and rounds < fetchbench._REFINE_ROUNDS_MAX:
        mids = fetchbench._refine_midpoints(list(samples.values()), fit)
        if not mids:
            break
        for mid in mids:
            samples[mid] = measure_write_block(mid)
        rounds += 1
        fit = fetchbench.fit_bandwidth(list(samples.values()))
        violations = fetchbench.check_fit(list(samples.values()), fit, nominal_gib_s)
    if violations:
        detail = "；".join(violations)
        raise RuntimeError(
            f"存储写三重检查未过，加密 {rounds} 轮后仍有违规（D39）: {detail}"
        )
    return fetchbench._path_fit(WRITE_ROW, samples, fit)


def _write_target(block_mib: int) -> Path:
    # 写方向大文件落表库根（D36：表库根位于数据目录）下的 .writebench 目录，
    # 与读探针的块文件目录分开；目标已存在且大小一致时复用
    root = config.load_runtime_config().table_root
    directory = root / _WRITE_DIR
    directory.mkdir(parents=True, exist_ok=True)
    target = directory / f"seg-{block_mib}.bin"
    size = block_mib * _MIB_BYTES
    if target.is_file() and target.stat().st_size == size:
        return target
    with open(target, "wb") as handle:
        handle.truncate(size)
        handle.flush()
        os.fsync(handle.fileno())
    return target


def _timed_direct_writes(target: Path, block_bytes: int, reps: int) -> list[float]:
    # O_DIRECT 顺序写绕开页缓存，与读方向直读口径对称（D39）
    buf = mmap.mmap(-1, block_bytes)
    try:
        fd = os.open(target, os.O_WRONLY | os.O_DIRECT)
        try:
            return _timed_writes(fd, memoryview(buf), block_bytes, reps)
        finally:
            os.close(fd)
    finally:
        buf.close()


def _timed_writes(
    fd: int, view: memoryview, block_bytes: int, reps: int
) -> list[float]:
    # 定长段循环写满整档，预热一次后计 reps 次耗时
    times: list[float] = []
    for attempt in range(reps + 1):
        start = time.perf_counter()
        offset = 0
        while offset < block_bytes:
            chunk = min(_IO_CHUNK_BYTES, block_bytes - offset)
            written = os.pwritev(fd, [view[offset : offset + chunk]], offset)
            if written <= 0:
                raise RuntimeError(f"段写断流，offset {offset}")
            offset += written
        elapsed = time.perf_counter() - start
        if attempt:
            times.append(elapsed)
    return times