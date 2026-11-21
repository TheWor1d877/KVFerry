# cost

测决策层需要的代价数字：本地拷贝、NVMe 取回、重算、干扰。结果换算到 3.8 GiB 与 30K token。

## 运行

```
python survey/cost/bench_cost.py --kv-gib 1 --block-mib 16 --iters 20
python survey/cost/report.py --input survey/results/cost
```

参数：

| 参数 | 默认 | 含义 |
|---|---|---|
| `--kv-gib` | 1 | 单次测试搬运的 KV 总量，4060 上不要超过 2 |
| `--block-mib` | 16 | 每块大小，接近引擎的块粒度 |
| `--iters` | 20 | 重复轮数 |
| `--nvme-path` | 无 | 指定一个文件测 NVMe 读取；不填则该项记「未测」。文件应大于内存，或让脚本每轮前丢弃页缓存 |
| `--matmul` | 4096 | 重算代理的方阵边长 |
| `--params-b` | 8 | 用于换算的模型参数量，单位十亿 |
| `--tokens` | 30000 | 用于换算的前缀长度 |

## 四张表

| 表 | 测什么 | 换算方式 |
|---|---|---|
| 本地拷贝 | 锁页内存到显存的带宽 | GiB/s 与每 GiB 秒数 |
| NVMe 取回 | 文件读到主机内存再上显存的带宽 | GiB/s 与每 GiB 秒数 |
| 重算 | 方阵乘法的吞吐，作为 prefill 的代理 | TFLOPS 换算成 30K token 的秒数 |
| 干扰 | 拷贝与计算并发时，相对单独运行拖慢多少 | 并发耗时除以两者单独耗时的较大值 |

## 结果

`survey/results/<主机名>/<时间戳>/cost/cost.json` 与 `cost.csv`，`report.py` 在同目录生成 `report.md`。

## 字段说明

- `local_copy.gib_s`：拷贝带宽。
- `local_copy.sec_per_gib`：搬 1 GiB 需要多少秒，3.8 GiB 由报告换算。
- `nvme.gib_s`：文件读取加搬运的端到端带宽。
- `recompute.tflops`：矩阵乘法实测吞吐。
- `recompute.recompute_s`：按参数量与 token 数换算的重算时间。
- `interference.factor`：1.0 表示并发没有互相拖慢，大于 1 表示拖慢。

矩阵乘法是 prefill 的代理，数值与真实 attention 有差距，报告里按代理值记录。