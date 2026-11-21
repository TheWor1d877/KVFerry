# hardware

采集这台机器的硬件参数，作为代价表的机器口径。

## 运行

```
python survey/hardware/collect.py
```

可选参数：`--out 目录`。

## 采集项

| 项 | 内容 |
|---|---|
| 主机 | 主机名、系统、内核、时间 |
| CPU | 型号、物理核、逻辑核 |
| 内存 | 总量 |
| GPU | 型号、显存、驱动、CUDA 版本、PCIe 链路代数与宽度、GPU 间拓扑 |
| PCIe | 显卡、网卡、存储控制器 |
| NVMe | 型号、容量 |
| GDS | GPU 直接读盘是否可用 |
| 网卡 | 名称、MAC、速率、RoCE 能力 |

采不到的项写「未测」，不中断脚本。

## 结果

`survey/results/<主机名>/<时间戳>/hardware/hardware.json` 和 `hardware.md`。

`hardware.md` 直接读，`hardware.json` 给后续脚本取参数。

## 字段说明

- `pcie.link_gen` / `pcie.link_width`：GPU 当前协商到的 PCIe 代数与通道数。降级运行会显示比规格低的值。
- `topology`：`nvidia-smi topo -m` 的原始输出。PIX 表示 GPU 与网卡同桥，SYS 表示跨 CPU。
- `gds`：GPU 直接读盘是否可用。`未测` 表示探测命令不可用。
- `nic.roce`：网卡是否暴露 RoCE 设备。