# overlap

测显存与主机内存之间的拷贝带宽，以及拷贝和计算能不能同时进行。

## 运行

```
python survey/overlap/bench_overlap.py --size-mib 256 --iters 20
```

参数 `--device` 指定显卡序号。没有可用 GPU 时脚本打印原因并退出。

## 测什么

| 项 | 含义 |
|---|---|
| pinned 带宽 | 锁页主机内存与显存之间的拷贝速率，分 H2D 和 D2H |
| pageable 带宽 | 普通主机内存与显存之间的拷贝速率，用来对比 pinned 的收益 |
| 串行耗时 | 同一队列里先拷贝再计算，跑 N 轮 |
| 重叠耗时 | 拷贝与计算各占一个队列同时跑，跑同样 N 轮 |
| 收益 | 串行耗时除以重叠耗时 |

## 结果

`survey/results/<主机名>/<时间戳>/overlap/overlap.json`。

## 字段说明

- `bandwidth.pinned.h2d_gib_s`：每 GiB 秒数的倒数。
- `serial_s` / `overlap_s`：同样工作量下的总耗时。
- `speedup`：大于 1 表示重叠有效，等于 1 表示没有重叠。
- `pinned_vs_pageable`：pinned 带宽除以 pageable 带宽。