#!/usr/bin/env python3
"""采集本机硬件参数，输出 JSON 与 Markdown。"""
import argparse
import json
import platform
import re
import shutil
import socket
import subprocess
from datetime import datetime
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]


def sh(cmd, timeout=15):
    try:
        p = subprocess.run(cmd, shell=True, capture_output=True, text=True, timeout=timeout)
        out = (p.stdout or "").strip()
        return out if p.returncode == 0 and out else None
    except Exception:
        return None


def cpu_info():
    txt = sh("LC_ALL=C lscpu")
    if not txt:
        return {"未测": "lscpu 不可用"}
    d = {}
    for line in txt.splitlines():
        if ":" in line:
            k, v = line.split(":", 1)
            d[k.strip()] = v.strip()

    def pick(*names):
        for name in names:
            for key, value in d.items():
                if name in key:
                    return value
        return None

    return {
        "型号": pick("Model name", "型号名称"),
        "物理核": pick("Core(s) per socket", "每个插槽的核心"),
        "每核线程": pick("Thread(s) per core", "每个核心的线程"),
        "插槽": pick("Socket(s)", "插槽"),
        "NUMA 节点": pick("NUMA node(s)", "NUMA 节点"),
    }


def memory_gib():
    try:
        for line in Path("/proc/meminfo").read_text().splitlines():
            if line.startswith("MemTotal:"):
                return round(int(line.split()[1]) / 1024 / 1024, 1)
    except Exception:
        pass
    return None


def gpu_info():
    q = sh("nvidia-smi --query-gpu=index,name,memory.total,driver_version "
           "--format=csv,noheader,nounits")
    if not q:
        return []
    link = sh("nvidia-smi --query-gpu=index,pcie.link.gen.current,pcie.link.width.current "
              "--format=csv,noheader,nounits")
    links = {}
    if link:
        for line in link.splitlines():
            parts = [p.strip() for p in line.split(",")]
            if len(parts) >= 3:
                links[parts[0]] = {"代数": parts[1], "宽度": parts[2]}
    cuda = None
    raw = sh("nvidia-smi")
    if raw:
        m = re.search(r"CUDA\s+(?:UMD\s+)?Version:\s*([\d.]+)", raw)
        if m:
            cuda = m.group(1)
    gpus = []
    for line in q.splitlines():
        parts = [p.strip() for p in line.split(",")]
        if len(parts) >= 4:
            idx = parts[0]
            gpus.append({
                "序号": idx,
                "型号": parts[1],
                "显存_MiB": parts[2],
                "驱动": parts[3],
                "CUDA": cuda,
                "PCIe": links.get(idx, "未测"),
            })
    return gpus


def pcie_devices():
    txt = sh("lspci")
    if not txt:
        return []
    keys = ("VGA", "3D controller", "Display", "Ethernet", "InfiniBand", "Non-Volatile")
    return [line.strip() for line in txt.splitlines() if any(k in line for k in keys)]


def nvme_info():
    txt = sh("nvme list")
    if txt:
        rows = [l.strip() for l in txt.splitlines() if "/dev/nvme" in l]
        if rows:
            return rows
    txt = sh("lsblk -d -o NAME,MODEL,SIZE,ROTA")
    if txt:
        return [l.strip() for l in txt.splitlines()]
    return []


def gds_info():
    loaded = sh("lsmod | grep -c nvidia_fs")
    probes = {
        "/dev/nvidia-fs 存在": Path("/dev/nvidia-fs").exists(),
        "nvidia_fs 模块已加载": bool(loaded and loaded != "0"),
        "sysfs 有 nvidia_fs": Path("/sys/module/nvidia_fs").exists(),
        "nvidia-smi 报告 GDS": bool(sh("nvidia-smi -q | grep -i 'GDS'")),
    }
    if any(probes.values()):
        return {"状态": "可用", "依据": [k for k, v in probes.items() if v]}
    return {"状态": "未测", "依据": "未发现 nvidia-fs 设备或模块"}


def nic_info():
    txt = sh("ip -br link")
    if not txt:
        return []
    skip = ("lo", "docker", "br-", "veth", "virbr")
    nics = []
    for line in txt.splitlines():
        parts = line.split()
        if not parts or any(parts[0].startswith(p) for p in skip):
            continue
        name = parts[0]
        raw = sh(f"ethtool {name} 2>/dev/null | grep -E 'Speed:|Duplex:'")
        speed = "未测"
        if raw:
            vals = []
            for item in raw.replace("\t", " ").splitlines():
                if ":" in item:
                    k, v = item.split(":", 1)
                    vals.append(f"{k.strip()} {v.strip()}")
            if vals:
                speed = "，".join(vals)
        nics.append({"名称": name, "速率": speed})
    rdma = sh("rdma link show")
    if rdma:
        for nic in nics:
            if nic["名称"] in rdma:
                nic["RoCE"] = "是"
    return nics


def strip_ansi(text):
    return re.sub(r"\x1b\[[0-9;]*m", "", text) if text else text


def collect():
    return {
        "主机": {
            "主机名": socket.gethostname(),
            "系统": platform.platform(),
            "内核": platform.release(),
            "采集时间": datetime.now().isoformat(timespec="seconds"),
        },
        "CPU": cpu_info(),
        "内存_GiB": memory_gib(),
        "GPU": gpu_info(),
        "GPU 拓扑": strip_ansi(sh("nvidia-smi topo -m")) or "未测",
        "PCIe 设备": pcie_devices(),
        "NVMe": nvme_info(),
        "GDS": gds_info(),
        "网卡": nic_info(),
    }


def to_markdown(data):
    lines = ["# 硬件参数", ""]
    host = data["主机"]
    lines += [f"- 主机名：{host['主机名']}", f"- 系统：{host['系统']}",
              f"- 内核：{host['内核']}", f"- 采集时间：{host['采集时间']}", ""]
    cpu = data["CPU"]
    if isinstance(cpu, dict):
        lines += ["## CPU", ""] + [f"- {k}：{v}" for k, v in cpu.items()] + [""]
    lines += ["## 内存", "", f"- 总量：{data['内存_GiB']} GiB", ""]
    lines += ["## GPU", ""]
    if data["GPU"]:
        for g in data["GPU"]:
            lines.append(f"- 卡 {g['序号']}：{g['型号']}，显存 {g['显存_MiB']} MiB，"
                         f"驱动 {g['驱动']}，CUDA {g['CUDA']}，PCIe Gen{g['PCIe'].get('代数', '?')} "
                         f"x{g['PCIe'].get('宽度', '?')}" if isinstance(g["PCIe"], dict) else
                         f"- 卡 {g['序号']}：{g['型号']}，显存 {g['显存_MiB']} MiB，"
                         f"驱动 {g['驱动']}，CUDA {g['CUDA']}，PCIe 未测")
    else:
        lines.append("- 未检测到 NVIDIA GPU")
    lines += ["", "## GPU 拓扑", "", "```", data["GPU 拓扑"], "```", ""]
    lines += ["## PCIe 设备", ""] + [f"- {d}" for d in data["PCIe 设备"]] + [""]
    lines += ["## NVMe", ""] + [f"- {d}" for d in data["NVMe"]] + [""]
    lines += ["## GDS", "", f"- 状态：{data['GDS']['状态']}", f"- 依据：{data['GDS']['依据']}", ""]
    lines += ["## 网卡", ""]
    for n in data["网卡"]:
        lines.append(f"- {n['名称']}：{n['速率']}" + (f"，RoCE {n['RoCE']}" if "RoCE" in n else ""))
    return "\n".join(lines) + "\n"


def main():
    ap = argparse.ArgumentParser(description="采集本机硬件参数")
    ap.add_argument("--out", help="输出根目录，默认 survey/results/<主机名>/<时间戳>/hardware")
    args = ap.parse_args()

    stamp = datetime.now().strftime("%Y%m%d-%H%M%S")
    out = Path(args.out) if args.out else ROOT / "survey/results" / socket.gethostname() / stamp / "hardware"
    out.mkdir(parents=True, exist_ok=True)

    data = collect()
    (out / "hardware.json").write_text(json.dumps(data, ensure_ascii=False, indent=2))
    (out / "hardware.md").write_text(to_markdown(data))
    print(to_markdown(data))
    print(f"已写入 {out}")


if __name__ == "__main__":
    main()