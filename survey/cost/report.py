#!/usr/bin/env python3
"""把 cost.json 汇总成 Markdown，换算 3.8 GiB 与 30K token。"""
import argparse
import json
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]

TARGET_GIB = 3.8
TARGET_TOKENS = 30000


def latest_cost(base):
    files = sorted(base.rglob("cost.json"), key=lambda p: p.stat().st_mtime, reverse=True)
    return files[0] if files else None


def main():
    ap = argparse.ArgumentParser(description="代价表汇总")
    ap.add_argument("--input", default=str(ROOT / "survey/results"),
                    help="搜索 cost.json 的根目录")
    ap.add_argument("--kv-gib", type=float, default=TARGET_GIB)
    ap.add_argument("--tokens", type=int, default=TARGET_TOKENS)
    args = ap.parse_args()

    path = latest_cost(Path(args.input))
    if not path:
        print("没有找到 cost.json，先运行 bench_cost.py")
        return

    d = json.loads(path.read_text())
    copy_gib_s = d["local_copy"]["gib_s"]
    copy_sec_per_gib = d["local_copy"]["sec_per_gib"]
    nvme = d.get("nvme", {})
    tflops = d["recompute"]["tflops"]
    params_b = d["参数"].get("params_b", 8)

    lines = [
        "# 代价表报告",
        "",
        f"- 设备：{d['设备']}",
        f"- 主机：{d['主机名']}",
        f"- 时间：{d['时间']}",
        f"- 参数：KV {d['参数']['kv_gib']} GiB，块 {d['参数']['block_mib']} MiB，"
        f"重复 {d['参数']['iters']} 轮，方阵 {d['参数']['matmul']}",
        "",
        "## 本地拷贝",
        "",
        f"- 带宽：{copy_gib_s} GiB/s",
        f"- 每 GiB：{copy_sec_per_gib} s",
        f"- {args.kv_gib} GiB 估算：{round(copy_sec_per_gib * args.kv_gib, 2)} s",
        "",
        "## NVMe 取回",
        "",
    ]
    if nvme.get("状态") == "实测":
        lines += [
            f"- 带宽：{nvme['gib_s']} GiB/s",
            f"- 每 GiB：{nvme['sec_per_gib']} s",
            f"- {args.kv_gib} GiB 估算：{round(nvme['sec_per_gib'] * args.kv_gib, 2)} s",
        ]
    else:
        lines.append(f"- 未测：{nvme.get('原因', '未提供 --nvme-path')}")

    recompute = round(2.0 * params_b * 1e9 * args.tokens / (tflops * 1e12), 2) if tflops else None
    lines += [
        "",
        "## 重算",
        "",
        f"- 矩阵乘法吞吐：{tflops} TFLOPS",
        f"- {params_b}B 模型、{args.tokens} token 估算：{recompute} s",
        "- 代理值是矩阵乘法吞吐，真实 attention 的数值会有差距",
        "",
        "## 干扰",
        "",
        f"- 并发系数：{d['interference']['factor']}（1.0 表示没有互相拖慢）",
        "",
        f"来源：{path}",
    ]

    out = path.parent / "report.md"
    out.write_text("\n".join(lines) + "\n")
    print("\n".join(lines))
    print(f"已写入 {out}")


if __name__ == "__main__":
    main()