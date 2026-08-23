# Oracle RAC 与 PGRAC 节点变化机制图解

这组文档解释共享磁盘集群里最容易混淆的一组问题：节点退出后谁还能代表集群、重启实例如何重新加入、成员集合何时才算稳定，以及全局锁、缓存块和事务如何恢复到可继续服务的状态。

这里比较的是公开可验证的行为与职责，不猜测 Oracle 未公开的内部消息格式、状态枚举或算法。PGRAC 部分以本仓库当前公开代码、测试和用户接口为准。

## 一张图看懂阅读顺序

```mermaid
flowchart LR
    A[01 架构与权威<br/>谁负责什么] --> B[02 Rejoin 与 Formation<br/>谁能重新成为成员]
    B --> C[03 Recovery 与节点变化<br/>资源和数据如何恢复]
    C --> D[04 差异与运维<br/>怎么操作和排障]
```

- [01：架构、职责与权威边界](01-architecture-and-authority.md)
- [02：Rejoin、Formation 与成员状态机](02-rejoin-and-formation.md)
- [03：Recovery、故障重配置与节点生命周期](03-recovery-and-node-change.md)
- [04：差异矩阵、运维流程与当前边界](04-differences-and-operations.md)

专题深化：

- [Clean Formation 的 Epoch 0：Oracle 对齐与 PGRAC 安全边界](../formation-epoch-zero/README.md)

## 先分清六个词

| 术语 | 本文含义 |
|---|---|
| node | 运行数据库实例的物理机或虚拟机。一个 node 不等于一个正在服务的 instance。 |
| instance | 一组数据库进程和内存。instance crash 后，node 可能仍活着。 |
| incarnation | PGRAC 中一次实例生命期的身份。相同 `node_id` 重启后必须用更新的 incarnation，防止旧实例冒充新实例。 |
| epoch | 成员关系发生变化时推进的逻辑版本。它帮助丢弃旧消息，但单独一个较大 epoch 不是入群证明。 |
| rejoin | 曾经属于集群的实例在重启后重新获得成员资格。它不是简单 TCP 重连。 |
| formation | PGRAC 的项目术语：对“当前允许参与恢复与服务的精确成员集合及其权威证据”的一致快照。Oracle 公开资料使用 cluster membership/reorganization 等术语，并未公开同名结构。 |
| recovery | 上下文相关：可能指成员重配置、GES/GCS/GRD 资源恢复、失败实例 redo/undo 恢复，或实例重启。本文会明确指出是哪一层。 |

## 证据标识

正文使用四种标识，避免把“行为相似”写成“内部实现相同”：

- **Oracle 已验证**：Oracle 官方公开文档明确描述的行为。
- **PGRAC 已实现**：当前公开主线已有生产接线，并有源码或测试锚点。
- **语义映射**：两者承担相近安全职责，但不能据此推断 Oracle 内部协议与 PGRAC 相同。
- **当前边界**：当前公开版本尚未提供、仅有部分证据，或必须由部署系统补足的能力。

## 三个核心结论

1. **“活着”不等于“有权服务”。** 心跳只证明进程近期可通信；成员身份还要满足 quorum、incarnation、持久 marker、epoch 和恢复权威等条件。
2. **节点变化是多层协议，不是一个开关。** 先确定成员，再迁移或重建全局资源，完成必要的数据恢复，最后才重新开放锁请求和写入。
3. **Oracle 与 PGRAC 的安全目标相近，故障域并不相同。** Oracle Clusterware 的 CSS/CRSD 位于数据库实例之外，而 PGRAC 当前把 CSSD/QVOTEC/LMON 作为 postmaster auxiliary processes；PGRAC 的 LMON 类职责仍然属于数据库内协调层。

## 公开依据

Oracle 官方入口：

- [Oracle Clusterware 架构与 CSS/CRSD 职责](https://docs.oracle.com/en/database/oracle/oracle-database/26/cwadd/introduction-to-oracle-clusterware.html)
- [Oracle RAC 架构、GCS/GES/GRD 与后台进程](https://docs.oracle.com/en/database/oracle/oracle-database/26/racad/introduction-to-oracle-rac.html)
- [Oracle RAC 实例恢复](https://docs.oracle.com/en/database/oracle/oracle-database/26/racad/managing-backup-and-recovery.html)
- [Oracle RAC 节点添加与删除](https://docs.oracle.com/en/database/oracle/oracle-database/21/racad/adding-and-deleting-oracle-rac-from-nodes-on-linux-and-unix-systems.html)
- [Oracle OCR 与 voting files](https://docs.oracle.com/en/database/oracle/oracle-database/19/cwadd/managing-oracle-cluster-registry-and-voting-files.html)

PGRAC 公开入口：

- [架构概览](../overview.md)
- [配置参考](../../user-guide/configuration.md)
- [计划内 clean leave](../../cluster/clean-leave.md)
- [永久删除节点](../../cluster/node-removal.md)
- [成员状态定义](../../../src/include/cluster/cluster_membership.h)
- [重配置协议定义](../../../src/include/cluster/cluster_reconfig.h)
- [恢复分类定义](../../../src/include/cluster/cluster_recovery_plan.h)

> 文档只描述当前公开主线可以证实的能力。测试中标为可跳过、仅验证接口存在或仍需外部系统的部分，不会写成生产闭环。
