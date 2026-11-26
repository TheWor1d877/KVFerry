# 指标

全局指标规范，按模块分节；新模块接入时在此增加一节，不写进各模块的 x.1。

## 通用约定

指标端点统一路径 /metrics，Prometheus 文本格式，按 Prometheus 拉取对接。指标命名 `kferry_<模块>_<指标名>`，标签先带 module 区分组件。

## 网关

网关当前不暴露指标端点：演练与对账的观测走日志与 /drill-events 事件接口（D22），检测延迟、摘除与改路的计数从日志与事件统计得出。接入 Prometheus 时在网关一节按通用约定补指标族清单与标签（候选：请求计数、改路计数、健康副本数 gauge），清单定稿前进 5.2 契约。

## 实验装置

bench 不暴露抓取端点：装置产出是数据目录里的轮次统计与汇总文件，指标口径由装置指标文档定义，抓取通道与之互不替代。

## 代价表

calibrate 不暴露指标端点：漂移与重标的观测走 D35 事件流，越阈窗口数、重标耗时与降级窗口时长从 expire、recalibrate、degrade 三类事件统计得出；表值与偏差的明细随版本目录的表文件与 manifest 落盘，对账按版本号与 calibrated_at 关联。接入 Prometheus 时在本节按通用约定补指标族清单与标签，候选为越阈窗口计数、重标耗时 gauge 与生效表版本信息。
## 存储节点

存储节点与决策服务当前不暴露 /metrics 端点：命中率分层统计从读回打点日志的 result 字段聚合（docs/logging.md 存储节点节），失联跳过由决策侧命中查询跳过失联节点产生。接入 Prometheus 时按通用约定补指标族，五类命中落在读回计数指标的 result 标签上，标签取值与含义：

| result 取值       | 含义         |
|-------------------|--------------|
| `hit_nvme`        | 本机盘读     |
| `hit_cache`       | 读缓存       |
| `hit_remote`      | 远端命中     |
| `skip_unreachable`| 失联跳过     |
| `miss`            | 未命中       |

候选指标族（命名按 `kferry_<模块>_<指标名>` 通用约定）：

| 指标名                          | 类型    | 标签   | 含义                                     |
|---------------------------------|---------|--------|------------------------------------------|
| `kferry_storage_get_total`      | counter | result | 读回请求计数，result 五类取值见上表      |
| `kferry_storage_put_total`      | counter | result | 写入计数，result 区分完成与拒收          |
| `kferry_storage_evict_total`    | counter | reason | 驱逐计数，reason 记水位、pin 配额、抽读  |
| `kferry_storage_used_segments`  | gauge   | node_id| 占用段数，容量水位按段总数换算           |
| `kferry_storage_event_lag`      | gauge   | node_id| 事件缓冲中未确认的 seq 数                |