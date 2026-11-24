"""传输探针、线性拟合与三重检查，产出 T1（D31、R21）。"""

from __future__ import annotations

import ctypes
import math
import mmap
import os
import socket
import threading
import time
from dataclasses import dataclass
from pathlib import Path

import config
import tables

PATH_BLOCK_LADDERS: dict[str, tuple[int, ...]] = {
    "dram": (8, 64, 256, 1024, 4096),
    "nvme-direct": (8, 64, 256, 1024, 4096),
    "nvme-staged": (8, 64, 256),
    "tcp-loopback": (8, 64, 256, 1024, 4096),
}
MIN_FIT_BLOCK_MIB = 8
MEASURE_REPS = 5

_MIB_BYTES = 1024**2
_GIB_BYTES = 1024**3
_IO_CHUNK_BYTES = 64 * _MIB_BYTES
_FILL_CHUNK_BYTES = 4 * _MIB_BYTES
_BENCH_DIR = ".fetchbench"
_GAP_DEVIATION_LIMIT = 0.10  # 相邻档隐含带宽偏差阈值（D31）
_NOMINAL_RATIO = 0.80  # 带宽低于标称 80% 记违规（D31）
_FIXED_COST_LIMIT_S = 0.1  # 截距上限（D31）
_REFINE_ROUNDS_MAX = 2  # 档位加密至多两轮（D31）


@dataclass(frozen=True)
class BlockSample:
    path: str
    block_mib: int
    elapsed_s: tuple[float, ...]  # 原始耗时序列，单位秒


@dataclass(frozen=True)
class FitResult:
    bandwidth_gib_s_median: float
    bandwidth_gib_s_p25: float
    fixed_cost_s_median: float
    fixed_cost_s_p25: float
    residuals_s: dict[int, float]  # 键为字节档，值为中位数拟合残差，单位秒


def measure_block(path: str, block_mib: int, reps: int = MEASURE_REPS) -> BlockSample:
    if path not in PATH_BLOCK_LADDERS:
        raise RuntimeError(f"未知传输路径: {path}（R21）")
    block_bytes = block_mib * _MIB_BYTES
    if path == "dram":
        times = _measure_dram(block_bytes, reps)
    elif path == "nvme-direct":
        times = _measure_nvme_direct(_block_file(block_mib), block_bytes, reps)
    elif path == "nvme-staged":
        times = _measure_nvme_staged(_block_file(block_mib), block_bytes, reps)
    else:
        times = _measure_tcp_loopback(block_bytes, reps)
    return BlockSample(path=path, block_mib=block_mib, elapsed_s=tuple(times))


def fit_bandwidth(samples: list[BlockSample]) -> FitResult:
    seqs = _seqs_by_block(samples)
    med_by_block = {block: _median(seq) for block, seq in seqs.items()}
    p25_by_block = {block: _p25(seq) for block, seq in seqs.items()}
    fit_blocks = sorted(block for block in med_by_block if block != MIN_FIT_BLOCK_MIB)
    if len(fit_blocks) < 2:
        raise RuntimeError(
            f"剔除最小档 {MIN_FIT_BLOCK_MIB} MiB 后不足两档，无法拟合（D27）"
        )
    xs = [block * _MIB_BYTES for block in fit_blocks]
    slope_med, ic_med = _least_squares(xs, [med_by_block[b] for b in fit_blocks])
    slope_p25, ic_p25 = _least_squares(xs, [p25_by_block[b] for b in fit_blocks])
    if slope_med <= 0 or slope_p25 <= 0:
        raise RuntimeError("拟合斜率非正，稳态带宽无意义（D27）")
    residuals = {
        block: med_by_block[block] - (slope_med * (block * _MIB_BYTES) + ic_med)
        for block in sorted(med_by_block)
    }
    return FitResult(
        bandwidth_gib_s_median=1.0 / (slope_med * _GIB_BYTES),
        bandwidth_gib_s_p25=1.0 / (slope_p25 * _GIB_BYTES),
        fixed_cost_s_median=ic_med,
        fixed_cost_s_p25=ic_p25,
        residuals_s=residuals,
    )


def check_fit(
    samples: list[BlockSample], fit: FitResult, nominal_gib_s: float | None
) -> list[str]:
    violations: list[str] = []
    med_by_block = _median_by_block(samples)
    blocks = sorted(med_by_block)
    fit_mib_s = fit.bandwidth_gib_s_median * 1024.0
    for lo, hi in zip(blocks, blocks[1:], strict=False):
        delta = med_by_block[hi] - med_by_block[lo]
        if delta <= 0:
            violations.append(f"相邻档 {lo}-{hi} MiB 耗时非递增，隐含带宽无效")
            continue
        implied = (hi - lo) / delta
        deviation = abs(implied - fit_mib_s) / fit_mib_s
        if deviation > _GAP_DEVIATION_LIMIT:
            violations.append(
                f"相邻档 {lo}-{hi} MiB 隐含带宽偏差 {deviation * 100:.1f}% 超过 10%"
            )
    spreads = _spread_by_block(samples)
    for block in blocks:
        residual = abs(fit.residuals_s[block])
        if residual > spreads[block]:
            violations.append(
                f"档 {block} MiB 中位数拟合残差 {residual:.6f}s 超过极差 {spreads[block]:.6f}s"
            )
    if (
        nominal_gib_s is not None
        and fit.bandwidth_gib_s_median < nominal_gib_s * _NOMINAL_RATIO
    ):
        violations.append(
            f"带宽 {fit.bandwidth_gib_s_median:.3f} GiB/s 低于标称 "
            f"{nominal_gib_s:.3f} GiB/s 的 80%"
        )
    if fit.fixed_cost_s_median > _FIXED_COST_LIMIT_S:
        violations.append(f"截距 {fit.fixed_cost_s_median:.6f}s 超过 0.1s")
    return violations


def build_t1(nominal: dict[str, float] | None = None) -> dict[str, tables.PathFit]:
    result: dict[str, tables.PathFit] = {}
    for path, ladder in PATH_BLOCK_LADDERS.items():
        result[path] = _calibrate_path(
            path, ladder, nominal.get(path) if nominal else None
        )
    return result


def remeasure_path(path: str, nominal_gib_s: float | None) -> tables.PathFit:
    ladder = PATH_BLOCK_LADDERS.get(path)
    if ladder is None:
        raise RuntimeError(f"未知传输路径: {path}（R21）")
    return _calibrate_path(path, ladder, nominal_gib_s)


def _calibrate_path(
    path: str, ladder: tuple[int, ...], nominal_gib_s: float | None
) -> tables.PathFit:
    samples: dict[int, BlockSample] = {
        block: measure_block(path, block) for block in ladder
    }
    fit = fit_bandwidth(list(samples.values()))
    violations = check_fit(list(samples.values()), fit, nominal_gib_s)
    refine_rounds = 0
    while violations and refine_rounds < _REFINE_ROUNDS_MAX:
        mids = _refine_midpoints(list(samples.values()), fit)
        if not mids:
            break
        for mid in mids:
            samples[mid] = measure_block(path, mid)
        refine_rounds += 1
        fit = fit_bandwidth(list(samples.values()))
        violations = check_fit(list(samples.values()), fit, nominal_gib_s)
    if violations:
        detail = "；".join(violations)
        raise RuntimeError(
            f"路径 {path} 三重检查未过，加密 {refine_rounds} 轮后仍有违规（D31）: {detail}"
        )
    return _path_fit(path, samples, fit)


def _refine_midpoints(samples: list[BlockSample], fit: FitResult) -> list[int]:
    existing = {sample.block_mib for sample in samples}
    mids: list[int] = []
    for lo, hi in _violated_gaps(samples, fit):
        mid = _geometric_midpoint(lo, hi)
        if mid not in existing and mid not in mids:
            mids.append(mid)
    return mids


def _violated_gaps(samples: list[BlockSample], fit: FitResult) -> list[tuple[int, int]]:
    med_by_block = _median_by_block(samples)
    blocks = sorted(med_by_block)
    fit_mib_s = fit.bandwidth_gib_s_median * 1024.0
    gaps: list[tuple[int, int]] = []
    for lo, hi in zip(blocks, blocks[1:], strict=False):
        delta = med_by_block[hi] - med_by_block[lo]
        if delta <= 0:
            gaps.append((lo, hi))
            continue
        implied = (hi - lo) / delta
        if abs(implied - fit_mib_s) / fit_mib_s > _GAP_DEVIATION_LIMIT:
            gaps.append((lo, hi))
    return gaps


def _geometric_midpoint(lo: int, hi: int) -> int:
    return 2 ** math.ceil((math.log2(lo) + math.log2(hi)) / 2)


def _path_fit(
    path: str, samples: dict[int, BlockSample], fit: FitResult
) -> tables.PathFit:
    min_block = min(samples)
    seq = sorted(samples[min_block].elapsed_s)
    return tables.PathFit(
        bandwidth_gib_s_median=fit.bandwidth_gib_s_median,
        bandwidth_gib_s_p25=fit.bandwidth_gib_s_p25,
        fixed_cost_s_median=fit.fixed_cost_s_median,
        fixed_cost_s_p25=fit.fixed_cost_s_p25,
        min_block_mib=min_block,
        min_block_s_median=_median(seq),
        min_block_s_p25=_p25(seq),
    )


def _seqs_by_block(samples: list[BlockSample]) -> dict[int, list[float]]:
    seqs: dict[int, list[float]] = {}
    for sample in samples:
        seqs.setdefault(sample.block_mib, []).extend(sample.elapsed_s)
    return seqs


def _median_by_block(samples: list[BlockSample]) -> dict[int, float]:
    return {block: _median(seq) for block, seq in _seqs_by_block(samples).items()}


def _spread_by_block(samples: list[BlockSample]) -> dict[int, float]:
    return {
        block: max(seq) - min(seq) for block, seq in _seqs_by_block(samples).items()
    }


def _median(seq: list[float]) -> float:
    values = sorted(seq)
    return values[len(values) // 2]


def _p25(seq: list[float]) -> float:
    values = sorted(seq)
    return values[len(values) // 4]


def _least_squares(xs: list[float], ys: list[float]) -> tuple[float, float]:
    n = len(xs)
    mean_x = sum(xs) / n
    mean_y = sum(ys) / n
    sxx = sum((x - mean_x) ** 2 for x in xs)
    sxy = sum((x - mean_x) * (y - mean_y) for x, y in zip(xs, ys, strict=False))
    if sxx == 0:
        raise RuntimeError("拟合样本字节档重合，无法最小二乘拟合（D27）")
    slope = sxy / sxx
    return slope, mean_y - slope * mean_x


class _CudaRuntime:
    """libcuda 最小封装：pinned 与显存分配、异步拷贝与同步。"""

    def __init__(self) -> None:
        try:
            self._lib = ctypes.CDLL("libcuda.so.1")
        except OSError as exc:
            raise RuntimeError(f"libcuda 不可用，无法测量显存拷贝: {exc}") from exc
        lib = self._lib
        lib.cuInit.argtypes = [ctypes.c_int]
        lib.cuDeviceGet.argtypes = [ctypes.POINTER(ctypes.c_int), ctypes.c_int]
        lib.cuDevicePrimaryCtxRetain.argtypes = [
            ctypes.POINTER(ctypes.c_void_p),
            ctypes.c_int,
        ]
        lib.cuCtxSetCurrent.argtypes = [ctypes.c_void_p]
        lib.cuStreamCreate.argtypes = [ctypes.POINTER(ctypes.c_void_p), ctypes.c_uint]
        lib.cuMemAllocHost.argtypes = [ctypes.POINTER(ctypes.c_void_p), ctypes.c_size_t]
        lib.cuMemAlloc.argtypes = [ctypes.POINTER(ctypes.c_uint64), ctypes.c_size_t]
        lib.cuMemcpyHtoDAsync.argtypes = [
            ctypes.c_uint64,
            ctypes.c_void_p,
            ctypes.c_size_t,
            ctypes.c_void_p,
        ]
        lib.cuStreamSynchronize.argtypes = [ctypes.c_void_p]
        lib.cuMemFreeHost.argtypes = [ctypes.c_void_p]
        lib.cuMemFree.argtypes = [ctypes.c_uint64]
        self._check(lib.cuInit(0))
        device = ctypes.c_int()
        self._check(lib.cuDeviceGet(ctypes.byref(device), 0))
        ctx = ctypes.c_void_p()
        self._check(lib.cuDevicePrimaryCtxRetain(ctypes.byref(ctx), device))
        self._check(lib.cuCtxSetCurrent(ctx))
        stream = ctypes.c_void_p()
        self._check(lib.cuStreamCreate(ctypes.byref(stream), 0))
        self._ctx = ctx
        self._stream = stream

    def _check(self, result: int) -> None:
        if int(result) != 0:
            raise RuntimeError(f"CUDA 调用失败，返回码 {result}")

    def alloc_pinned(self, size: int) -> ctypes.c_void_p:
        ptr = ctypes.c_void_p()
        self._check(self._lib.cuMemAllocHost(ctypes.byref(ptr), ctypes.c_size_t(size)))
        return ptr

    def free_pinned(self, ptr: ctypes.c_void_p) -> None:
        self._check(self._lib.cuMemFreeHost(ptr))

    def alloc_device(self, size: int) -> int:
        ptr = ctypes.c_uint64()
        self._check(self._lib.cuMemAlloc(ctypes.byref(ptr), ctypes.c_size_t(size)))
        return ptr.value

    def free_device(self, ptr: int) -> None:
        self._check(self._lib.cuMemFree(ctypes.c_uint64(ptr)))

    def memcpy_h2d(self, dst_device: int, src_host: ctypes.c_void_p, size: int) -> None:
        self._check(
            self._lib.cuMemcpyHtoDAsync(
                ctypes.c_uint64(dst_device),
                src_host,
                ctypes.c_size_t(size),
                self._stream,
            )
        )
        self._check(self._lib.cuStreamSynchronize(self._stream))


_CUDA: _CudaRuntime | None = None


def _cuda() -> _CudaRuntime:
    global _CUDA
    if _CUDA is None:
        _CUDA = _CudaRuntime()
    return _CUDA


def _fill_buffer(buf, size: int) -> None:
    view = memoryview(buf)
    offset = 0
    while offset < size:
        chunk = min(_FILL_CHUNK_BYTES, size - offset)
        view[offset : offset + chunk] = os.urandom(chunk)
        offset += chunk


def _measure_dram(block_bytes: int, reps: int) -> list[float]:
    cuda = _cuda()
    src = cuda.alloc_pinned(block_bytes)
    dst = cuda.alloc_device(block_bytes)
    try:
        _fill_buffer((ctypes.c_char * block_bytes).from_address(src.value), block_bytes)
        times: list[float] = []
        for attempt in range(reps + 1):
            start = time.perf_counter()
            cuda.memcpy_h2d(dst, src, block_bytes)
            elapsed = time.perf_counter() - start
            if attempt:
                times.append(elapsed)
        return times
    finally:
        cuda.free_device(dst)
        cuda.free_pinned(src)


def _measure_nvme_direct(block_file: Path, block_bytes: int, reps: int) -> list[float]:
    buf = mmap.mmap(-1, block_bytes)
    try:
        fd = os.open(block_file, os.O_RDONLY | os.O_DIRECT)
        try:
            return _timed_direct_reads(fd, memoryview(buf), block_bytes, reps)
        finally:
            os.close(fd)
    finally:
        buf.close()


def _timed_direct_reads(
    fd: int, view: memoryview, block_bytes: int, reps: int
) -> list[float]:
    times: list[float] = []
    for attempt in range(reps + 1):
        start = time.perf_counter()
        offset = 0
        while offset < block_bytes:
            chunk = min(_IO_CHUNK_BYTES, block_bytes - offset)
            read = os.preadv(fd, [view[offset : offset + chunk]], offset)
            if read <= 0:
                raise RuntimeError(f"块文件读断流，offset {offset}")
            offset += read
        elapsed = time.perf_counter() - start
        if attempt:
            times.append(elapsed)
    return times


def _measure_nvme_staged(block_file: Path, block_bytes: int, reps: int) -> list[float]:
    cuda = _cuda()
    pinned = cuda.alloc_pinned(block_bytes)
    dst = cuda.alloc_device(block_bytes)
    view = memoryview((ctypes.c_char * block_bytes).from_address(pinned.value))
    fd = os.open(block_file, os.O_RDONLY | os.O_DIRECT)
    try:
        times: list[float] = []
        for attempt in range(reps + 1):
            start = time.perf_counter()
            offset = 0
            while offset < block_bytes:
                chunk = min(_IO_CHUNK_BYTES, block_bytes - offset)
                read = os.preadv(fd, [view[offset : offset + chunk]], offset)
                if read <= 0:
                    raise RuntimeError(f"块文件读断流，offset {offset}")
                offset += read
            cuda.memcpy_h2d(dst, pinned, block_bytes)
            elapsed = time.perf_counter() - start
            if attempt:
                times.append(elapsed)
        return times
    finally:
        os.close(fd)
        cuda.free_device(dst)
        cuda.free_pinned(pinned)


def _measure_tcp_loopback(block_bytes: int, reps: int) -> list[float]:
    payload = bytearray(block_bytes)
    _fill_buffer(payload, block_bytes)
    listener = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    listener.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    listener.bind(("127.0.0.1", 0))
    listener.listen(1)

    def _sink() -> None:
        try:
            conn, _ = listener.accept()
            with conn:
                got = 0
                while got < block_bytes:
                    data = conn.recv(min(_IO_CHUNK_BYTES, block_bytes - got))
                    if not data:
                        return
                    got += len(data)
                conn.sendall(b"\x01")
        except OSError:
            return

    thread = threading.Thread(target=_sink, daemon=True)
    thread.start()
    try:
        with socket.create_connection(
            ("127.0.0.1", listener.getsockname()[1])
        ) as sender:
            view = memoryview(payload)
            times: list[float] = []
            for attempt in range(reps + 1):
                start = time.perf_counter()
                sender.sendall(view)
                if not sender.recv(1):
                    raise RuntimeError("loopback 对端未确认完整接收")
                elapsed = time.perf_counter() - start
                if attempt:
                    times.append(elapsed)
            return times
    finally:
        listener.close()
        thread.join(timeout=1)


def _block_file(block_mib: int) -> Path:
    root = config.load_runtime_config().table_root  # D36：标定命令同样经该键定位表库根
    directory = root / _BENCH_DIR
    directory.mkdir(parents=True, exist_ok=True)
    target = directory / f"block-{block_mib}.bin"
    size = block_mib * _MIB_BYTES
    if target.is_file() and target.stat().st_size == size:
        return target
    with open(target, "wb") as handle:
        offset = 0
        while offset < size:
            chunk = min(_FILL_CHUNK_BYTES, size - offset)
            handle.write(os.urandom(chunk))
            offset += chunk
        handle.flush()
        os.fsync(handle.fileno())
    return target
