# 流水线设计（CI / RELEASE）

> 本文档是三层流水线的设计占位与决策记录。L1 已实现（`.github/workflows/ci.yml`），
> L2 / L3 尚未实现——实现时先按本文档核对设计，有出入先更新本文档再动手。

## 总览

| 层 | 触发 | 内容 | 状态 |
|----|------|------|------|
| L1 | push 任意分支 / PR → main | lint + build + 单测（gateway + calibrate） | ✅ 已实现 |
| L2 | push 到 main | L1 + 全量 e2e/smoke | 规划中 |
| L3 | push tag `v*` | 对 tag commit 重跑全量 e2e/smoke，通过后构建并发布 | 规划中 |

范围约定：进 CI 的只有 `gateway/`（Go）与 `calibrate/`（Python）。
`survey/`、`bench/`、`docs/`、`scripts/` 为调研/环境脚本，不进 CI 卡点。

## L1 现状（ci.yml）

- changes job（dorny/paths-filter）按路径拆分：`gateway/**` → go job，`calibrate/**` + `ruff.toml` → python job；workflow 自身变更时全部强制跑。
- go job：build + vet + golangci-lint（v2.15.0，standard 默认集，配置在 `gateway/.golangci.yml`）+ `go test -race`。
- python job：ruff check（默认集 + B/I/UP，配置在根 `ruff.toml`）+ ruff format --check + pytest。
- `gate` job（`if: always()`）汇总：skip 视为通过，failure/cancelled 视为失败。分支保护唯一 required check。
- 工具版本钉死：golangci-lint v2.15.0、ruff 0.17.6、pytest 9.2.0、Python 3.12。

## L2 规划：main 全量验证（main.yml）

- 触发：`push` 到 `main`。
- 内容：reusable workflow 复用 L1（workflow_call）+ 新增 e2e/smoke job。
- e2e/smoke 实体**待定**（候选：网关起服 + calibrate 冒烟联通）。实体定型前不建此 workflow。
- 语义：事后验证（merge 后跑），红则 revert。main 的 required check 仍只挂 L1 的 `gate`。

## L3 规划：tag 门禁 + 自动发布（release.yml）

- 触发：push tag `vX.Y.Z`。
- 门禁（路径 C）：第一步对 tag 指向的 commit 重跑全量 e2e/smoke；不通过 → workflow 失败，人工删 tag。
- 通过后：构建 Go 网关二进制 + 镜像，按 CLAUDE.md 版本与镜像规范发布：
  - 版本唯一来源为 git tag `vX.Y.Z`，镜像 tag、二进制注入版本、CHANGELOG 标题三处逐字一致；
  - 镜像名 `ghcr.io/<owner>/<repo>/<组件>`：开发镜像用版本标签，候选镜像 `sha-<7位短sha>`，发布清单用 `@sha256` 固定摘要。

### 决策记录：为什么选路径 C

备选路径：A = main smoke 通过后给 commit 记 status，tag 时校验（省时间，但门禁是事后拒且要维护状态）；B = release 由 workflow_dispatch 触发、CI 检查后自己打 tag（门禁物理不可绕过，但改操作习惯）。
选 C 的理由：零状态、实现最简、无法作弊；e2e 时长在可接受范围内时重复跑的代价最小。
演进条件：若 e2e 时长超过约 30 分钟，重跑代价过高，届时评估演进到 A 或 B。

### 决策记录：分支工作流

双分支制（main + dev），单人仓库。main 与 dev 均通过 Rulesets 挂 `CI / gate` 为 required check，禁 force push / 禁删除；**不开 approval 要求**（GitHub 不允许 approve 自己的 PR，单人仓库会锁死）。合并方式 squash only。