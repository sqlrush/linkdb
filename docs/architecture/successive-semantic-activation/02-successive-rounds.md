# 2. 连续轮次时序

## 2.1 第一轮：启用 R4

四节点 clean formation 中，R4 轮按以下顺序推进：

```mermaid
sequenceDiagram
    participant C as 协调节点
    participant N as 四节点成员
    participant Q as 多数派持久层

    C->>N: SAMPLE：采集当前成员与能力
    N-->>C: 全成员精确确认
    C->>N: BARRIER：冻结本轮上下文
    N-->>C: barrier 完成
    C->>Q: 持久 PREPARE
    Q-->>C: 多数派确认
    C->>N: target closed apply
    N-->>C: PREPARED
    C->>Q: 持久 COMMIT
    Q-->>C: 多数派确认
    C->>N: COMMIT_APPLIED
    C->>Q: 持久 OPEN
    Q-->>C: 多数派确认
    C->>N: OPEN_APPLIED
```

OPEN 完成前，普通请求不能把 R4 当成已生效能力。OPEN 完成后，四节点根据同一持久 generation
观察 R4 已启用。

## 2.2 第二轮：启用 Resource-X

第二轮不回到初始空集合，而是从 R4 开始：

```mermaid
sequenceDiagram
    participant C as 协调节点
    participant N as 四节点成员
    participant P as 持久状态

    Note over P: 上一轮 OPEN：当前能力 = R4
    C->>P: 读取当前 generation 与生效集合
    P-->>C: generation=G, active=R4
    C->>N: SAMPLE：R4 → R4+Resource-X
    N-->>C: 全成员确认
    C->>P: PREPARE，前驱必须是 (G, R4)
    P-->>C: 多数派完成
    C->>N: target closed apply
    C->>P: COMMIT
    P-->>C: 多数派完成
    C->>P: OPEN
    P-->>C: 多数派完成
    C->>N: Resource-X OPEN_APPLIED
```

## 2.3 成员确认不是持久 OPEN 的替代物

全成员 ACK 证明本轮参与者都对同一上下文完成了规定动作；持久多数派证明切换结果能够跨进程
和节点重启恢复。两者缺一不可：

```mermaid
flowchart LR
    A[全成员 ACK] --> C{两类证据均成立?}
    B[持久多数派状态] --> C
    C -->|是| D[允许 OPEN]
    C -->|否| E[保持关闭]
```

本地 callback 成功、单节点已进入新路径或某个计数器增加，都不能代替 OPEN。

## 2.4 formation 变化时整轮失效

轮次绑定当前成员集合、节点身份和 formation。以下任一变化都会使未完成轮次失效：

- 节点重连导致 incarnation 改变；
- formation epoch 改变；
- admitted member 集合改变；
- capability 采样不再匹配；
- 协调节点身份改变。

失效后不会局部修补旧轮次。系统重新读取当前稳定状态，再在新的 formation 下开始新轮。

## 2.5 重启恢复

节点重启时以多数派持久状态为准：

| 观察到的稳定状态 | 恢复行为 |
|---|---|
| PREPARE | target 仍关闭，不能作为当前服务集合 |
| COMMIT | target 仍关闭，等待既有恢复流程收敛 |
| OPEN | target 是当前生效集合 |
| 无多数派 | 保持 fail closed |

后继轮次只有在前一轮 OPEN 被稳定观察后才会开始。
