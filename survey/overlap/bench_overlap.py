#!/usr/bin/env python3
"""测 pinned 与 pageable 拷贝带宽，以及串行和双 stream 重叠的耗时差。"""
import argparse
import json
import socket
from datetime import datetime
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]


def gpu_available():
    try:
        import torch
    except ImportError:
        return False, "未安装 torch，先运行 bash scripts/setup_env.sh"
    if not torch.cuda.is_available():
        return False, "torch 检测不到可用 CUDA 设备"
    return True, None


def timed_callable(fn, iters, warmup=3):
    import torch
    for _ in range(warmup):
        fn()
    torch.cuda.synchronize()
    start = torch.cuda.Event(enable_timing=True)
    end = torch.cuda.Event(enable_timing=True)
    start.record()
    for _ in range(iters):
        fn()
    end.record()
    torch.cuda.synchronize()
    return start.elapsed_time(end) / 1000.0


def main():
    ap = argparse.ArgumentParser(description="拷贝与重叠基准")
    ap.add_argument("--size-mib", type=int, default=256)
    ap.add_argument("--iters", type=int, default=20)
    ap.add_argument("--matmul", type=int, default=4096, help="matmul 方阵边长")
    ap.add_argument("--device", type=int, default=0)
    ap.add_argument("--out", help="输出根目录")
    args = ap.parse_args()

    ok, reason = gpu_available()
    if not ok:
        print(f"跳过：{reason}")
        return

    import torch

    dev = f"cuda:{args.device}"
    size = args.size_mib * 1024 * 1024
    nbytes = size

    gpu_buf = torch.empty(size, dtype=torch.uint8, device=dev)
    pinned = torch.empty(size, dtype=torch.uint8, pin_memory=True)
    pageable = torch.empty(size, dtype=torch.uint8)

    def h2d(src):
        return lambda: gpu_buf.copy_(src, non_blocking=True)

    def d2h(src):
        return lambda: src.copy_(gpu_buf, non_blocking=True)

    def bandwidth(fn, iters):
        sec = timed_callable(fn, iters)
        return nbytes * iters / sec / 1024**3

    bw = {
        "pinned": {"h2d_gib_s": bandwidth(h2d(pinned), args.iters),
                   "d2h_gib_s": bandwidth(d2h(pinned), args.iters)},
        "pageable": {"h2d_gib_s": bandwidth(h2d(pageable), args.iters),
                     "d2h_gib_s": bandwidth(d2h(pageable), args.iters)},
    }

    side = args.matmul
    a = torch.randn(side, side, device=dev, dtype=torch.float16)
    b = torch.randn(side, side, device=dev, dtype=torch.float16)

    def copy_once():
        gpu_buf.copy_(pinned, non_blocking=True)

    def matmul_once():
        torch.matmul(a, b)

    def serial():
        for _ in range(args.iters):
            copy_once()
            matmul_once()

    stream_copy = torch.cuda.Stream(device=dev)
    stream_calc = torch.cuda.Stream(device=dev)

    def overlapped():
        with torch.cuda.stream(stream_copy):
            for _ in range(args.iters):
                copy_once()
        with torch.cuda.stream(stream_calc):
            for _ in range(args.iters):
                matmul_once()
        torch.cuda.current_stream(dev).wait_stream(stream_copy)
        torch.cuda.current_stream(dev).wait_stream(stream_calc)

    serial_s = timed_callable(serial, 1)
    overlap_s = timed_callable(overlapped, 1)

    data = {
        "时间": datetime.now().isoformat(timespec="seconds"),
        "主机名": socket.gethostname(),
        "设备": torch.cuda.get_device_name(args.device),
        "参数": {"size_mib": args.size_mib, "iters": args.iters, "matmul": side},
        "bandwidth": bw,
        "pinned_vs_pageable": round(bw["pinned"]["h2d_gib_s"] / bw["pageable"]["h2d_gib_s"], 2)
        if bw["pageable"]["h2d_gib_s"] else None,
        "serial_s": round(serial_s, 4),
        "overlap_s": round(overlap_s, 4),
        "speedup": round(serial_s / overlap_s, 3) if overlap_s else None,
    }

    stamp = datetime.now().strftime("%Y%m%d-%H%M%S")
    out = Path(args.out) if args.out else ROOT / "survey/results" / socket.gethostname() / stamp / "overlap"
    out.mkdir(parents=True, exist_ok=True)
    (out / "overlap.json").write_text(json.dumps(data, ensure_ascii=False, indent=2))

    print(f"设备 {data['设备']}")
    print(f"pinned H2D {bw['pinned']['h2d_gib_s']:.2f} GiB/s，"
          f"pageable {bw['pageable']['h2d_gib_s']:.2f} GiB/s，"
          f"倍数 {data['pinned_vs_pageable']}")
    print(f"串行 {serial_s:.3f} s，重叠 {overlap_s:.3f} s，收益 {data['speedup']}x")
    print(f"已写入 {out}")


if __name__ == "__main__":
    main()