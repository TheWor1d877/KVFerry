# survey

前期现勘装置。三个装置各回答一个问题：

| 目录 | 回答的问题 | 需要什么 |
|---|---|---|
| `hardware/` | 这台机器长什么样 | 无，只要命令行 |
| `overlap/` | 拷贝和计算能不能同时跑 | NVIDIA GPU + torch |
| `cost/` | 取回、重算各要多久 | NVIDIA GPU + torch |

## 顺序

```
bash scripts/setup_env.sh
bash survey/run_all.sh
```

`run_all.sh` 依次跑采集、重叠、代价表。没有 GPU 或没有 NVMe 的项会自动跳过并写明原因。

单跑某个装置：

```
python survey/hardware/collect.py
python survey/overlap/bench_overlap.py --size-mib 256 --iters 20
python survey/cost/bench_cost.py --kv-gib 1 --block-mib 16 --iters 20
python survey/cost/report.py --input survey/results/cost
```

## 结果

全部落在 `survey/results/<主机名>/<时间戳>/<装置>/`，每个子目录里的 README 说明字段含义。结果不入库。

脚本不依赖固定显卡号、固定路径和主机名，换机器只改参数。