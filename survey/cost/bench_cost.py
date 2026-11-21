#!/usr/bin/env python3
"""代价表基准：本地拷贝、NVMe 取回、重算代理、并发干扰。"""
import argparse
import csv
import json
import os
import socket
from datetime import datetime
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]


def torch_or_none():
    try:
        import torch
    except ImportError:
        print("跳过：未安装 torch，先运行 bash scripts/setup_env.sh")
        return None
    if not torch.cuda.is_available():
        print("跳过：torch 检测不到可用 CUDA 设备")
        return None
    return torch


def timed(torch, fn, iters, warmup=2):
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
    ap = argparse.ArgumentParser(description="代价表基准")
    ap.add_argument("--kv-gib", type=float, default=1.0)
    ap.add_argument("--block-mib", type=int, default=16)
    ap.add_argument("--iters", type=int, default=20)
    ap.add_argument("--nvme-path")
    ap.add_argument("--matmul", type=int, default=4096)
    ap.add_argument("--params-b", type=float, default=8.0)
    ap.add_argument("--tokens", type=int, default=30000)
    ap.add_argument("--device", type=int, default=0)
    ap.add_argument("--out")
    args = ap.parse_args()

    torch = torch_or_none()
    if torch is None:
        return

    dev = f"cuda:{args.device}"
    block = args.block_mib * 1024 * 1024
    blocks = max(1, round(args.kv_gib * 1024 / args.block_mib))
    total_bytes = blocks * block

    cpu_buf = torch.empty(block, dtype=torch.uint8, pin_memory=True)
    gpu_buf = torch.empty(block, dtype=torch.uint8, device=dev)

    def one_copy():
        gpu_buf.copy_(cpu_buf, non_blocking=True)

    def copy_round():
        for _ in range(blocks):
            one_copy()

    copy_s = timed(torch, copy_round, args.iters)
    copy_gib_s = (total_bytes * args.iters) / copy_s / 1024**3
    copy_sec_per_gib = 1.0 / copy_gib_s if copy_gib_s else None

    nvme = {"状态": "未测"}
    if args.nvme_path:
        path = Path(args.nvme_path)
        if path.is_file():
            buf = memoryview(cpu_buf.numpy())
            want = total_bytes
            read_total = 0

            def nvme_round():
                nonlocal read_total
                read_total = 0
                f = path.open("rb", buffering=0)
                try:
                    if hasattr(os, "posix_fadvise"):
                        os.posix_fadvise(f.fileno(), 0, 0, os.POSIX_FADV_DONTNEED)
                    for _ in range(blocks):
                        n = f.readinto(buf)
                        if not n:
                            f.seek(0)
                            n = f.readinto(buf)
                        read_total += n
                        gpu_buf.copy_(cpu_buf, non_blocking=True)
                finally:
                    f.close()

            nvme_s = timed(torch, nvme_round, args.iters)
            nvme_gib_s = (read_total * args.iters) / nvme_s / 1024**3 if nvme_s else None
            nvme = {
                "状态": "实测",
                "文件": str(path),
                "gib_s": round(nvme_gib_s, 3) if nvme_gib_s else None,
                "sec_per_gib": round(1.0 / nvme_gib_s, 3) if nvme_gib_s else None,
            }
        else:
            nvme = {"状态": "未测", "原因": f"文件不存在：{path}"}

    side = args.matmul
    a = torch.randn(side, side, device=dev, dtype=torch.float16)
    b = torch.randn(side, side, device=dev, dtype=torch.float16)

    def matmul_once():
        torch.matmul(a, b)

    calc_s = timed(torch, matmul_once, args.iters)
    flops = 2.0 * side**3 * args.iters
    tflops = flops / calc_s / 1e12

    per_token = 2.0 * args.params_b * 1e9
    recompute_s = per_token * args.tokens / (tflops * 1e12) if tflops else None

    calc_alone_s = timed(torch, copy_round, 1)
    single_calc_s = timed(torch, matmul_once, 1)
    n_calc = max(1, int(round(calc_alone_s / single_calc_s))) if single_calc_s else 1

    stream_copy = torch.cuda.Stream(device=dev)
    stream_calc = torch.cuda.Stream(device=dev)

    def concurrent():
        with torch.cuda.stream(stream_copy):
            copy_round()
        with torch.cuda.stream(stream_calc):
            for _ in range(n_calc):
                matmul_once()
        torch.cuda.current_stream(dev).wait_stream(stream_copy)
        torch.cuda.current_stream(dev).wait_stream(stream_calc)

    concurrent_s = timed(torch, concurrent, 1)
    calc_n_s = single_calc_s * n_calc
    denom = max(calc_alone_s, calc_n_s)
    factor = concurrent_s / denom if denom else None

    data = {
        "时间": datetime.now().isoformat(timespec="seconds"),
        "主机名": socket.gethostname(),
        "设备": torch.cuda.get_device_name(args.device),
        "参数": {
            "kv_gib": args.kv_gib, "block_mib": args.block_mib, "iters": args.iters,
            "matmul": side, "params_b": args.params_b, "tokens": args.tokens,
        },
        "local_copy": {"gib_s": round(copy_gib_s, 3), "sec_per_gib": round(copy_sec_per_gib, 3)
                       if copy_sec_per_gib else None},
        "nvme": nvme,
        "recompute": {"tflops": round(tflops, 2), "recompute_s": round(recompute_s, 2)
                      if recompute_s else None},
        "interference": {
            "copy_alone_s": round(calc_alone_s, 4),
            "calc_alone_s": round(calc_n_s, 4),
            "calc_batches": n_calc,
            "concurrent_s": round(concurrent_s, 4),
            "factor": round(factor, 3) if factor else None,
        },
    }

    stamp = datetime.now().strftime("%Y%m%d-%H%M%S")
    out = Path(args.out) if args.out else ROOT / "survey/results" / socket.gethostname() / stamp / "cost"
    out.mkdir(parents=True, exist_ok=True)
    (out / "cost.json").write_text(json.dumps(data, ensure_ascii=False, indent=2))

    rows = [
        ("local_copy_gib_s", data["local_copy"]["gib_s"], "GiB/s"),
        ("local_copy_sec_per_gib", data["local_copy"]["sec_per_gib"], "s/GiB"),
        ("nvme_gib_s", nvme.get("gib_s", ""), "GiB/s"),
        ("recompute_tflops", data["recompute"]["tflops"], "TFLOPS"),
        ("recompute_s", data["recompute"]["recompute_s"], "s"),
        ("interference_factor", data["interference"]["factor"], "x"),
    ]
    with (out / "cost.csv").open("w", newline="") as f:
        w = csv.writer(f)
        w.writerow(["metric", "value", "unit"])
        w.writerows(rows)

    print(f"设备 {data['设备']}")
    print(f"本地拷贝 {copy_gib_s:.2f} GiB/s，每 GiB {copy_sec_per_gib:.3f} s")
    if nvme.get("状态") == "实测":
        print(f"NVMe {nvme['gib_s']} GiB/s")
    else:
        print(f"NVMe 未测：{nvme.get('原因', '未提供 --nvme-path')}")
    print(f"重算代理 {tflops:.2f} TFLOPS，{args.tokens} token 估算 {recompute_s:.2f} s")
    print(f"干扰系数 {data['interference']['factor']}")
    print(f"已写入 {out}")


if __name__ == "__main__":
    main()