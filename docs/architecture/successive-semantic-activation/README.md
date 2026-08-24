# 连续语义激活：从 R4 OPEN 到 Resource-X

PGRAC 可以在同一套四节点集群中分轮启用集群能力。每一轮先完成全成员确认和持久提交，
再把新能力切换为可服务状态；后一轮从前一轮已经生效的能力集合继续推进。

本组文档说明连续激活的公开行为：

1. [状态模型与字段语义](01-state-model.md)：准备、提交、开放分别代表什么。
2. [连续轮次时序](02-successive-rounds.md)：R4 完成后如何继续启用 Resource-X。
3. [多数派、幂等与失败关闭](03-quorum-idempotence-and-fail-closed.md)：磁盘分歧、重试和崩溃时的行为。
4. [观测、验证与 Oracle RAC 对照](04-observability-validation-and-oracle.md)：如何判断切换完成，以及与 Oracle 公开架构的关系。

## 一张图看懂

```mermaid
flowchart LR
    Z[初始能力集合] --> R4P[R4 准备]
    R4P --> R4C[R4 提交]
    R4C --> R4O[R4 OPEN<br/>R4 开始服务]
    R4O --> RXP[Resource-X 准备<br/>以 R4 为当前基础]
    RXP --> RXC[Resource-X 提交]
    RXC --> RXO[Resource-X OPEN<br/>R4 + Resource-X 服务]
```

核心规则只有一句：

> 新一轮必须从上一轮**已经 OPEN 的能力集合**出发，不能从上一轮切换前的旧集合重新开始。

## 对用户可见的保证

- 部署了代码不等于能力已经启用；OPEN 前仍按旧能力集合服务。
- 新能力只有在集群持久状态达到多数派并完成成员确认后才会生效。
- 同一个已完成请求可安全重放，不会额外推进一次状态。
- generation、成员、formation 或持久状态不一致时，系统拒绝推进。
- 部分磁盘写入、无法形成多数派或读回不完整时保持关闭，不把不确定状态当成成功。
- 后继能力的启用不会重新打开已经下线的 legacy 正常路径。

## 术语

| 术语 | 含义 |
|---|---|
| source | 本轮开始时仍在服务的能力集合 |
| target | 本轮成功后将要服务的能力集合 |
| PREPARE | 已冻结本轮目标，target 尚未对业务开放 |
| COMMIT | 集群已经持久提交本轮，target 仍处于关闭应用状态 |
| OPEN | target 成为当前对业务生效的能力集合 |
| generation | 每次持久状态推进所使用的单调代际 |
| formation | 当前参与协调的成员集合及其身份上下文 |

## 文档边界

本文描述公开可观察的架构行为，不披露内部持久记录布局、私有设计决策或未公开的 Oracle
内部协议。Oracle 对照仅引用 Oracle 官方公开资料；PGRAC 自有的持久化与协调实现不会被描述成
Oracle 的内部算法。
