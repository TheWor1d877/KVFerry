# 日志

全局日志规范，按模块分节；新模块接入时在此增加一节，不写进各模块的 x.1。

## 通用约定

每条日志带 component 字段区分组件，取值 gateway、bench，新增组件先在这里登记取值再落日志。请求正文、响应正文与 token 内容一律不落日志。组件进程把日志输出到 stdout 的 JSON 行，日志组件接入时按 stdout JSON 行对接，不写文件、不改 rotation。

## 网关

log/slog 的 JSONHandler 输出到 stdout，五处打点：

| 打点 | 级别 | 字段 |
|---|---|---|
| 启动 | info | listen、副本数 |
| 摘除 | warn | node、连续失败次数 |
| 恢复 | info | node |
| 改路 | info | request_id、session、旧副本、新副本、耗时 ms |
| 请求失败 | error | request_id、error |

改路日志在 transport 层打，请求失败日志在主流程打，两处都带 request_id，演练记录对账时按 request_id 关联。

## 实验装置

装置侧当前无统一进程日志：bench/loadgen.py 的 RoundLogger 按轮把轮次日志写进当轮目录（runs 层），轮次日志即观测记录，随轮落盘、不入库 stdout。其余 bench 模块的日志随实现补进实验装置一节。