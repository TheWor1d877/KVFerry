(thewor1d877)： [B] 底稿无会话字段时的会话分组规则未定：官方 trace 只有 timestamp、input_length、hash_ids，无会话标识，当前按请求序号逐请求独立会话

(thewor1d877)： [B] 54 份轨迹以占位 baseline-rate=1.0 req/s 生成，native 组实测基线完成速率后需按实测值重新生成三档全部轨迹

(thewor1d877)： [C] KVFERRY_STORAGE_ENDPOINT 与 KVFERRY_DECISION_ENDPOINT 已定义未使用，存储与决策服务的就绪判定协议待定

(thewor1d877)： [C] 代价表版本的注入方式待标定模块落地后定，run_meta.json 目前缺省记 unknown
(thewor1d877)： [B] 网关对真实引擎副本的全链路行为（SSE 流式、真实健康端点）由装置 native 组单格联调验证，网关单测不覆盖

(thewor1d877)： [C] 网关在 k8s 清单拉起与滚动升级时的摘除行为，集群部署后做一次副本升级演练