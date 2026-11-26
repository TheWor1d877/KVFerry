# 日志

全局日志规范，按模块分节；新模块接入时在此增加一节，不写进各模块的 x.1。

## 通用约定

每条日志带 component 字段区分组件，取值 gateway、bench、storage、decision，新增组件先在这里登记取值再落日志。请求正文、响应正文与 token 内容一律不落日志。组件进程把日志输出到 stdout 的 JSON 行，日志组件接入时按 stdout JSON 行对接，不写文件、不改 rotation。

## 网关

log/slog 的 JSONHandler 输出到 stdout，五处打点：

| 打点     | 级别  | 字段                                      |
|----------|-------|-------------------------------------------|
| 启动     | info  | listen、upstreams                         |
| 摘除     | warn  | node、failures                            |
| 恢复     | info  | node                                      |
| 改路     | info  | request_id、session、from、to、elapsed_ms |
| 请求失败 | error | request_id、error                         |

`request_id` 在请求未带 `X-Request-Id` 时为空串；`from` 与 `to` 是副本地址；`failures` 是触发摘除时的连续失败次数。

改路日志在 transport 层打，请求失败日志在主流程打，两处都带 request_id，演练记录对账时按 request_id 关联。

## 实验装置

装置侧当前无统一进程日志：bench/loadgen.py 的 RoundLogger 按轮把轮次日志写进当轮目录（runs 层），轮次日志即观测记录，随轮落盘、不入库 stdout。其余 bench 模块的日志随实现补进实验装置一节。

## 代价表

事件日志按 D35 输出：stdout JSON 行，component 固定取 cost，无级别字段，按 event 字段区分 expire、recalibrate、degrade 三类。打点三处：探针进程判定路径行过期时打 expire 与 degrade start；重标执行器打 recalibrate start 与 end，end 带 result（success 或 fail）；被动统计聚合器判定长度档过期时打 expire 与 degrade start。degrade end 在重标成功落盘后随 recalibrate end 打，重标失败时降级窗口保持开启、不补 degrade end。公共字段 ts_ms、source、table、target；expire 另带 expected、observed、deviation_pct，分别记表值、实测值与偏差百分比。事件不含请求正文、token 内容与租户信息，路径名与长度档为非敏感字段，无需脱敏。标定命令的进度与失败原因写 stderr，不进事件流；日志组件按 stdout JSON 行对接事件流。
## 存储节点

存储节点（component=storage）与决策服务（component=decision）都是 stdout JSON 行。块字节与 prompt 内容不落日志，只记块哈希；请求级打点带 trace_id，过程级打点省略。节点侧打点位置与字段：

| 打点             | 级别  | 字段                                                                  |
|------------------|-------|-----------------------------------------------------------------------|
| 启动口径校验     | info  | node_id、spec、segments、duration_ms、result                          |
| 口径变更         | warn  | node_id、spec、duration_ms、reason、result                            |
| 清空重建完成     | info  | node_id、spec、segments、duration_ms、result                          |
| 清单重报         | info/warn | node_id、spec、entries、duration_ms、reason、result               |
| 写入完成         | info  | trace_id、node_id、block_hash、segment_seq、bytes、result、layer      |
| 写入拒收         | warn  | trace_id、node_id、block_hash、bytes、reason、result                  |
| 读回返回         | info  | trace_id、node_id、block_hash、segment_seq、bytes、result、layer、duration_ms |
| 读回失败         | warn  | trace_id、node_id、block_hash、reason、result、layer、duration_ms     |
| 段校验失败       | error | node_id、block_hash、segment_seq、reason、result                      |
| 驱逐             | info  | node_id、block_hash、segment_seq、reason、watermark_pct、result       |
| 下沉             | info  | node_id、block_hash、target、duration_ms、result、layer               |
| 事件流连接与断开 | info/warn | node_id、seq、retry、reason、result                              |
| 对账             | info/warn | node_id、block_hash、segment_seq、reason、result                 |

`spec` 是口径四项的紧凑描述（block_bytes、precision、bytes_per_token、alignment_pad），启动四处打点必带；`result` 区分读回五类去向（hit_nvme、hit_cache、hit_remote、failed 与对账结果），命中率分层统计从这里聚合。同一请求的完成与拒收打点经 trace_id 关联。

决策服务侧打点位置与字段：

| 打点                 | 级别 | 字段                                                            |
|----------------------|------|-----------------------------------------------------------------|
| 重建开始             | info | node_id、nodes、shards、result                                  |
| 分片接收             | info | trace_id、node_id、shard_index、entries、duration_ms、result    |
| 重建完成             | info | node_id、nodes、shards、entries、duration_ms、result            |
| 重报完成             | info | trace_id、node_id、shards、entries、duration_ms、result         |
| 重建窗口内未命中     | info | trace_id、node_id、duration_ms、result                          |
| 节点连接与断开       | info/warn | node_id、state_from、state_to、duration_ms、result         |
| 判死与复活           | warn/info | node_id、state_from、state_to、duration_ms、result         |

重建完成打点记本次重建窗口的节点数、分片数与条目数；单节点重报完成另打重报完成打点，记该节点的分片数与条目数，两类打点不复用累计值。失联四态迁移经 state_from 与 state_to 记状态前后值，duration_ms 记当前状态持续时长。