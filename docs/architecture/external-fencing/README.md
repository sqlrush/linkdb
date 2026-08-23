# 外部 I/O Fencing：从节点驱逐到可验证的恢复权限

本文解释共享存储集群中最容易被低估、也最不能含糊的一条安全链：**为什么“节点失联”不等于“节点已经不能写”，以及 PGRAC 如何把外部设备的终态读回转换成数据库可消费、可失效、可审计的恢复准入证明。**

文中同时对照 Oracle Clusterware 的公开行为，但不会把 PGRAC 自己的协议字段、证书格式或进程划分描述成 Oracle 内部实现。

> **当前主干状态（2026-08-21）**
>
> PGRAC 公开主干已经包含 provider-neutral wire protocol、<code>pgrac-fenced</code> 守护进程骨架、终态分类器、持久日志、rejoin 控制消息以及数据库侧 NeedSet/AdmissionSet 验证框架；但是主干尚未注册经过生产认证的 fencing provider，数据库侧运行时激活条件仍返回关闭。因此，当前不能把外部 fencing 证书用于开放真实 takeover/recovery 写路径：缺少肯定证明时一律 fail closed。

## 1. 阅读地图与证据标签

本文使用四种标签，避免“目标相似”被误读成“内部实现相同”：

| 标签 | 含义 |
|---|---|
| **Oracle 已验证事实** | 可由 Oracle 官方公开文档直接支持 |
| **PGRAC 已实现** | 当前公开主干中可定位到对应源码 |
| **PGRAC 适配设计** | 为 PostgreSQL/PGRAC 架构设计的公开实现，不声称是 Oracle 内部协议 |
| **当前边界** | 代码刻意保持关闭、尚不能作为生产能力宣称的部分 |

建议按以下顺序阅读：

~~~mermaid
flowchart LR
    A[双写威胁] --> B[Oracle failure isolation]
    B --> C[PGRAC 组件分层]
    C --> D[终态证明与精确绑定]
    D --> E[数据库侧准入]
    E --> F[恢复与 rejoin 生命周期]
    F --> G[部署认证与故障诊断]
~~~

## 2. 核心问题：失联不是写隔离

共享存储集群至少存在四种不同的事实。它们相关，但不能互相替代：

| 事实 | 能回答什么 | 不能回答什么 |
|---|---|---|
| 心跳/liveness | 最近是否还能收到对端消息 | 对端是否仍可向共享存储发 I/O |
| membership/quorum | 哪些节点属于当前成员视图 | 被移出视图的旧进程是否已失去设备访问能力 |
| external write exclusion | 指定物理目标是否已到达不可写终态 | 当前恢复请求是否绑定了正确 epoch、incarnation 和资源集合 |
| recovery authority | 当前恢复者是否可对精确资源集合执行接管 | 被隔离节点是否可以直接重新加入 |

危险窗口发生在“旧节点已经从成员视图消失，但它的操作系统、HBA、网络或缓存路径并未真正停止”时：

~~~mermaid
flowchart TB
    subgraph N0[幸存节点 node0]
        A1[判定 node1 超时]
        A2[准备恢复 node1 的资源]
        A3[可能写共享页、日志或元数据]
    end

    subgraph N1[失联但未真正停止的 node1]
        B1[集群网络不可达]
        B2[数据库或内核仍运行]
        B3[仍可能写共享存储]
    end

    S[(共享存储)]

    A1 --> A2 --> A3 --> S
    B1 --> B2 --> B3 --> S
    S --> X{两个故障域同时写}
    X --> Y[页损坏、redo 顺序冲突、锁主权分裂]
~~~

安全关系不是：

~~~text
peer heartbeat timeout  ⇒  recovery may write
~~~

而是：

~~~text
peer heartbeat timeout
  ∧ current membership/quorum
  ∧ exact victim incarnation
  ∧ exact protected resource set
  ∧ independently read-back write-excluded terminal state
  ∧ fresh, durable and live-connection-bound proof
  ⇒ recovery admission for that exact need
~~~

External fencing 不是替代 membership，而是补上 membership 无法证明的**物理写隔离**事实。

## 3. Oracle RAC 的公开责任划分

### 3.1 Oracle 已验证事实

Oracle 官方文档公开了以下行为：

- Cluster Synchronization Services（CSS）控制集群成员关系，并在节点加入或离开时通知集群组件；
- <code>cssdagent</code> 监控集群并提供 I/O fencing；
- failure isolation 的目的，是阻止故障节点继续破坏数据；
- 外部隔离机制可以在不依赖受害节点操作系统或 Clusterware 配合的情况下重启问题节点；
- Oracle Clusterware 支持通过经过认证的 IPMI-over-LAN 与节点 BMC 交互；IPMI 配置要求节点一致，并建议使用独立管理网络；
- Oracle 还公开了 ASM 相关的 <code>FENC</code> 后台进程，但这是 Oracle ASM/RDBMS 体系中的具体事实，不能据此推导 PGRAC 必须复制相同内部协议。

Oracle 公共资料给出的关键语义可以概括为：

~~~mermaid
flowchart LR
    CSS[CSS membership 和 heartbeat] --> D[判定问题节点]
    D --> AGENT[Clusterware failure-isolation actor]
    AGENT --> BMC[BMC 或 IPMI 管理面]
    BMC --> VICTIM[目标节点被外部重启或隔离]
    VICTIM --> SAFE[故障节点不再威胁共享数据]
    SAFE --> REC[集群恢复继续]
~~~

### 3.2 Oracle 没有公开的部分

Oracle 公开文档没有披露下列 PGRAC 所需内部细节：

- target/protected-set digest 的字节布局；
- 数据库恢复请求与 fencing 证明之间的 wire frame；
- proof freshness 的具体时间窗口；
- daemon journal 的记录格式；
- backend 消费证书时逐字段校验的先后顺序。

因此，本文后续的 digest、nonce、journal、NeedSet/AdmissionSet 和 rejoin opcode 都属于 **PGRAC 适配设计或已实现代码**，不是对 Oracle 私有实现的猜测。

## 4. PGRAC 的进程与信任边界

PGRAC 把“判断集群需要什么”和“操作外部设备并验证终态”放在不同权限域：

~~~mermaid
flowchart TB
    subgraph DB[数据库权限域]
        CSSD[cssd: liveness]
        QV[quorum 和 membership]
        LMON[lmon: recovery coordinator]
        BE[recovery backend consumer]
        ROOT[control-root feature state]
    end

    subgraph HOST[root 管理权限域]
        SOCK[Unix domain socket 和 peer credentials]
        FD[pgrac-fenced]
        CFG[root-owned config]
        J[(durable journal)]
        P[provider ABI v1]
    end

    subgraph OOB[带外管理域]
        MGT[management network]
        DEV[BMC、power controller 或 provider target]
    end

    CSSD --> LMON
    QV --> LMON
    LMON -->|NeedSet| BE
    BE --> SOCK --> FD
    CFG --> FD
    FD <--> J
    FD --> P --> MGT --> DEV
    DEV -->|independent readback| P
    P --> FD -->|typed proof| SOCK --> BE
    BE -->|AdmissionSet| LMON
    LMON --> ROOT

    classDef closed fill:#ffe6e6,stroke:#b30000,color:#5c0000;
    class P,ROOT closed;
~~~

图中的红色节点表示当前公开主干的生产边界：provider ABI 和数据库激活门已经存在，但没有已注册、已部署认证的生产 provider，因而 control-root 不能把 external-fence 能力作为开放恢复写路径的依据。

这种拆分有三个安全目的：

1. 数据库进程不直接持有 BMC/管理平面凭据；
2. 外部设备操作失败、超时或崩溃不会在数据库进程中被误解释为成功；
3. daemon 返回的只是候选证书，数据库仍必须用当前 formation、incarnation 和 protected set 重新验证。

## 5. 激活链：每一环都必须为真

生产激活不是“把一个布尔开关改为 true”，而是一条完整的可验证链：

~~~mermaid
flowchart LR
    A[已认证 provider] --> B[精确解析 target UUID]
    B --> C[执行 OFF 或 write exclusion]
    C --> D[独立 readback]
    D --> E{OFF 且 I/O DRAINED?}
    E -- 否或未知 --> Z[fail closed]
    E -- 是 --> F[持久化 terminal proof]
    F --> G[绑定 victim incarnation]
    G --> H[绑定 protected-set digest]
    H --> I[绑定 mapping generation]
    I --> J[绑定 current formation 和 duty]
    J --> K[校验 freshness 和 live connection]
    K --> L[构造 AdmissionSet]
    L --> M[数据库恢复路径可消费]
    M --> N[功能状态允许开放]
~~~

任意一环缺失，结果都不是“较弱的成功”，而是不可用、未知、拒绝或超时。尤其要避免以下错误降级：

| 错误做法 | 为什么不安全 |
|---|---|
| 只相信 power-off 命令返回 0 | 命令接受不代表目标已经关闭，也不代表 I/O 已排空 |
| 只相信节点不再心跳 | 网络隔离不等于存储隔离 |
| 只校验 node id | node id 可复用，必须同时绑定 incarnation |
| 只校验目标机器 | 恢复权限还必须绑定确切受保护存储集合 |
| 重启 daemon 后复用旧成功 | daemon boot、journal generation、freshness 和当前连接都可能已变化 |
| 把 OFF 证明用于 rejoin | rejoin 需要相反方向的 ON 且 DRAINED 终态及新的生命周期授权 |

## 6. 两个不可混用的终态谓词

### 6.1 恢复侧：WRITE_EXCLUDED

Provider 的一次动作分成三步：解析、执行、独立读回。只有最后的精确读回才可形成肯定结果：

~~~text
resolved_target_uuid == configured_target_uuid
AND target_state == OFF
AND io_drain_state == DRAINED
AND provider_result == OK
~~~

下列结果全部不是肯定证明：

| 读回结果 | 恢复准入 |
|---|---|
| 精确 target + <code>OFF + DRAINED</code> | 可以继续生成候选 proof |
| 精确 target + <code>OFF + NOT_DRAINED</code> | 拒绝，I/O 尚未排空 |
| 精确 target + <code>ON</code> | 拒绝，目标仍开启 |
| target UUID 不一致 | 拒绝，可能操作错设备 |
| <code>TRANSITIONING</code> / <code>UNKNOWN</code> | 未知，继续等待或失败关闭 |
| provider 超时、崩溃或不可达 | 不可用，失败关闭 |

### 6.2 Rejoin 侧：ON_AND_DRAINED

节点重新加入时，所需终态方向相反：目标必须已重新开启并完成 I/O 稳定化。公开代码使用独立分类器要求：

~~~text
resolved_target_uuid == configured_target_uuid
AND target_state == ON
AND io_drain_state == DRAINED
AND provider_result == OK
~~~

恢复侧的 <code>OFF + DRAINED</code> 证明绝不能自动变成 rejoin 准入；同样，rejoin 的 <code>ON + DRAINED</code> 也不能授权恢复接管。

## 7. 证书到底绑定什么

<code>pgrac_external_fence_protocol.h</code> 定义的是 provider-neutral 语义对象和手工 codec。语义结构不能直接强转为 wire frame，避免编译器 padding、端序和 ABI 差异进入持久协议。

### 7.1 Need：数据库提出的精确问题

| 字段 | 作用 |
|---|---|
| <code>system_identifier</code> | 防止跨数据库集群复用证明 |
| <code>canonical_duty_digest</code> | 绑定当前恢复职责或 formation 语境 |
| <code>victim_node_id</code> | 指定被隔离节点 |
| <code>victim_incarnation</code> | 防止 node id 复用后的旧证明重放 |
| <code>protected_set_digest</code> | 指定本次恢复会触及的共享资源集合 |
| <code>predicate_id/version</code> | 区分 WRITE_EXCLUDED 与 rejoin 等谓词版本 |
| <code>request_nonce</code> | 绑定单次请求与响应 |
| <code>timeout_ms</code> | 给 provider 工作设置有界期限 |

### 7.2 Binding：daemon 解析后的设备映射

Binding 在 Need 基础上加入 <code>target_mapping_generation</code>。它解决一个常见陷阱：配置中的 node id 到物理 BMC/设备 target 的映射可能变更。即使其它字段都相同，旧 generation 的证明也不能在新映射下继续使用。

### 7.3 Response：终态、持久性与新鲜度

肯定响应还携带：

| 字段组 | 防御目标 |
|---|---|
| <code>daemon_boot_id</code> | daemon 重启后拒绝无条件继承旧进程语境 |
| <code>journal_seq</code> | 定位形成证明的持久事件位置 |
| <code>verified_mono_ns / fresh_until_mono_ns</code> | 限制终态证明的有效时间窗 |
| <code>proof_generation</code> | 区分同一 target 的后续操作或失效世代 |
| <code>provider_id / provider_abi_version</code> | 绑定已认证 provider 实现 |
| <code>provider_result / provider_native_status</code> | 保留规范化与原生诊断结果 |
| <code>target_state_digest</code> | 绑定 target、状态、I/O drain、映射与 proof 世代 |
| <code>deny_reason</code> | 给 fail-closed 路径提供结构化原因 |

公开协议当前固定使用 160-byte request、256-byte response 和 256-byte rejoin frame，digest 为 32 bytes，nonce/operation id 为 16 bytes。

## 8. 从节点失联到恢复准入

下面是 provider 与部署认证完成后必须形成的完整调用链；当前公开主干会在生产 provider/激活门之前停止。时序强调“动作完成”与“数据库获得权限”不是同一事件：

~~~mermaid
sequenceDiagram
    participant C as cssd/quorum
    participant L as LMON coordinator
    participant B as DB recovery consumer
    participant F as pgrac-fenced
    participant P as provider worker
    participant T as physical target
    participant J as durable journal

    C->>L: node N lost / membership changes
    L->>B: Need(N, incarnation, duty, protected set)
    B->>F: ACQUIRE(request nonce + exact Need)
    F->>J: REQUEST_ACCEPTED + fsync
    F->>P: resolve exact target
    P-->>F: target UUID + mapping generation
    F->>J: ACTUATION_ISSUED + fsync
    F->>P: actuate_off(deadline)
    P->>T: provider-specific external isolation
    P-->>F: action result
    F->>J: ACTUATION_RESULT + fsync
    F->>P: independent readback(deadline)
    P->>T: query actual terminal state
    T-->>P: OFF + DRAINED + exact target
    P-->>F: typed readback
    F->>J: READBACK_RESULT / PROOF_SERVED + fsync
    F-->>B: candidate proof + freshness window
    B->>B: revalidate formation, incarnation, set, generation and connection
    B-->>L: Admission for this exact Need
    L->>L: only now may the matching recovery action proceed
~~~

如果 provider 命令返回成功，但 readback 是 <code>TRANSITIONING</code>；或者读回是肯定的，但数据库收到时已超过 freshness；或者期间 formation 已变化，最后三步都必须停止。

## 9. Daemon 状态机：成功不是一个瞬时返回值

公开主干把操作生命周期表示为闭合状态机：

~~~mermaid
stateDiagram-v2
    [*] --> UNAVAILABLE
    UNAVAILABLE --> IDLE: capability ready
    IDLE --> QUEUED: accepted but capacity unavailable
    QUEUED --> RESOLVING: capacity ready
    IDLE --> RESOLVING: request target free
    RESOLVING --> ACTUATING: exact target resolved
    RESOLVING --> REJECTED: mapping rejected
    RESOLVING --> UNKNOWN: resolve unknown
    RESOLVING --> UNAVAILABLE: provider unavailable
    ACTUATING --> VERIFYING: actuation finished
    VERIFYING --> PROVEN_DURABLE: exact OFF + DRAINED
    VERIFYING --> REJECTED: OFF but I/O not drained
    VERIFYING --> UNKNOWN: ON, transitioning or unknown
    PROVEN_DURABLE --> INVALIDATED: mapping/proof invalidated
    PROVEN_DURABLE --> REENABLING: authorized rejoin
    REENABLING --> IDLE: exact ON + DRAINED
    REENABLING --> UNKNOWN: failure or uncertain readback
    QUEUED --> UNKNOWN: timeout
    QUEUED --> INVALIDATED: request invalidated
~~~

<code>PROVEN_DURABLE</code> 表示形成证明所需事件已经进入可校验 journal，不等于数据库可以永久缓存该权限。数据库侧仍受 freshness、连接存活和当前集群事实约束。

## 10. 多受害节点：NeedSet 与 AdmissionSet

一次重构可能涉及多个失联写者。PGRAC 不使用“集群已经 fence 完”这种全局布尔值，而是对精确 need 集合逐项收集 admission：

~~~mermaid
flowchart TB
    N[NeedSet for current recovery]
    N --> N1[node1/inc7 + protected-set A]
    N --> N2[node2/inc4 + protected-set A]
    N --> N3[node3/inc9 + protected-set B]

    N1 --> P1{proof exact + fresh?}
    N2 --> P2{proof exact + fresh?}
    N3 --> P3{proof exact + fresh?}

    P1 -- yes --> A1[admission 1]
    P2 -- yes --> A2[admission 2]
    P3 -- no --> X[overall blocked]

    A1 --> AS[AdmissionSet]
    A2 --> AS
    AS --> X
~~~

这样可以防止：

- 用 node1 的证明替代 node2；
- 用旧 incarnation 的证明覆盖新进程；
- 用 storage-set A 的证明授权 storage-set B；
- 因为大多数受害节点已隔离，就忽略最后一个未知节点。

AdmissionSet 是进程内、短生命周期对象；释放时清零，不能成为跨 formation 的永久权威缓存。

## 11. 持久 Journal 与崩溃恢复

### 11.1 为什么需要 journal

外部电源操作有经典崩溃窗口：

~~~text
发送 OFF 成功
    ↓
daemon 在记录结果前崩溃
    ↓
重启后究竟是“未执行”、“已执行”还是“状态又变化”？
~~~

安全答案不是猜测，也不是把最后一条命令的退出码当真值，而是通过 journal 恢复操作上下文，再执行独立 readback；只有当前可验证终态才能重新形成证明。

### 11.2 公开主干的 journal 约束

- 固定 256-byte records；
- sequence 单调递增；
- 每条记录带 CRC32C；
- 通过 SHA-256 previous-record digest 形成链；
- append 使用追加语义，写入后执行 <code>fsync</code>；
- 事件类型区分 config、request、action、readback、proof、invalidate、reenable 和 reconcile；
- 扫描时验证 sequence、前驱 digest 和 CRC；
- 完整性失败使服务进入 unavailable，而不是从损坏日志推导成功；
- 仅在严格条件下允许修复不完整尾记录，不能把任意损坏静默截断成绿色。

~~~mermaid
flowchart LR
    R1[seq 101 CONFIG] -->|prev digest| R2[seq 102 REQUEST]
    R2 -->|prev digest| R3[seq 103 ACTUATION]
    R3 -->|prev digest| R4[seq 104 READBACK]
    R4 -->|prev digest| R5[seq 105 PROOF]
    R5 --> V{CRC + digest chain + seq valid?}
    V -- yes --> C[可恢复上下文，仍需当前 readback/freshness]
    V -- no --> U[UNAVAILABLE / fail closed]
~~~

## 12. Rejoin：恢复后重新加入不是反向播放

被隔离节点要重新加入，不能简单地“开机，然后把旧 fence 状态清掉”。公开协议把管理者准备和 LMON 授权拆开：

~~~mermaid
sequenceDiagram
    participant A as Administrator/control plane
    participant F as pgrac-fenced
    participant L as current LMON
    participant T as target/BMC
    participant J as joining instance

    A->>F: ADMIN_PREPARE(old node/inc, candidate inc)
    F-->>A: ADMIN_PREPARE_RESULT / operation id
    L->>F: LMON_CLAIM_NEXT(current gate digest)
    F-->>L: LMON_OFFER
    L->>F: LMON_AUTHORIZE_ON(exact operation)
    F->>T: actuate_on
    F->>T: independent readback
    T-->>F: exact target ON + DRAINED
    F-->>L: LMON_ON_RESULT + fresh proof
    J->>L: membership/rejoin request with candidate incarnation
    L->>F: optional REFRESH_ON
    F-->>L: REFRESH_RESULT
    L-->>J: admission only if current formation and identities still match
~~~

这条链解决四个问题：

1. 管理操作不能自己授予数据库 membership；
2. LMON 只能接管为当前 gate/formation 准备的 operation；
3. 开机动作必须通过独立 <code>ON + DRAINED</code> 读回；
4. candidate incarnation 与旧 incarnation 明确分离，避免旧进程身份复活。

如果 formation、target mapping、operation ownership 或 candidate incarnation 在途中变化，操作应 invalidated 或 cancel，然后按新事实重新开始。

## 13. 数据库侧为什么还要重新验证

Daemon 是设备终态的权威观察者，但不是数据库恢复职责的最终权威。数据库收到 proof 后仍需验证：

~~~mermaid
flowchart TD
    R[收到候选 proof] --> V1{protocol + nonce 匹配?}
    V1 -- no --> D[deny]
    V1 -- yes --> V2{system + duty/formation 当前?}
    V2 -- no --> D
    V2 -- yes --> V3{victim node + incarnation 精确?}
    V3 -- no --> D
    V3 -- yes --> V4{protected set + writer set 精确?}
    V4 -- no --> D
    V4 -- yes --> V5{mapping/provider ABI 当前?}
    V5 -- no --> D
    V5 -- yes --> V6{journal/proof generation 单调?}
    V6 -- no --> D
    V6 -- yes --> V7{freshness 未过期且连接仍活跃?}
    V7 -- no --> D
    V7 -- yes --> V8{terminal predicate 肯定?}
    V8 -- no --> D
    V8 -- yes --> A[创建 process-local admission]
~~~

公开主干定义的最大 proof freshness 为 5 秒，默认 acquire timeout 为 120 秒。前者不是“设备五秒后一定重新开启”，而是限定数据库可以把某次观察当作当前事实的最长时间；过期必须重新获取或刷新。

## 14. 配置身份与映射世代

默认公开路径如下：

| 用途 | 路径 |
|---|---|
| root-owned daemon 配置 | <code>/etc/pgrac/pgrac-fenced.conf</code> |
| 数据库客户端 socket | <code>/var/run/pgrac/pgrac-fenced.sock</code> |
| 管理 socket | <code>/var/run/pgrac/pgrac-fenced-admin.sock</code> |
| durable journal | <code>/var/lib/pgrac-fenced/journal</code> |

配置绑定：cluster system identifier、storage backend/UUID、provider id/ABI、mapping generation、允许连接的数据库 uid/gid，以及每个节点的 node id、target UUID 和 provider adapter data。

<code>mapping_generation</code> 是配置变化的安全轴：

- generation 回退必须拒绝；
- 同一 generation 下配置字节发生变化必须拒绝；
- generation 前进会使依赖旧 target map 的 proof 失效；
- 数据库 proof 和 rejoin operation 都必须携带并重新核对 generation。

这样即使运维人员把 node2 的 BMC 地址改成另一台设备，也不会让旧的“node2 已关闭”证明在新映射下继续生效。

## 15. 安全模型

### 15.1 权限分离

| 主体 | 应拥有 | 不应拥有 |
|---|---|---|
| PostgreSQL backend/LMON | 构造 Need、验证 proof、消费短期 admission | BMC 管理凭据、直接执行设备命令 |
| <code>pgrac-fenced</code> | root-owned config、journal、provider 调度 | 决定数据库 membership 或恢复资源范围 |
| provider worker | 单次、有 deadline 的设备操作能力 | 长期数据库状态、无限执行时间 |
| administrator | 发起受控 rejoin prepare | 绕过 LMON 直接授予 membership |

### 15.2 本地 IPC

Daemon 通过 Unix domain socket 获取 peer credential：管理 socket 只接受 root 语义，数据库 socket 依据 root-owned 配置中的 uid/gid 认证。仅知道 socket 路径不能获得 fencing 权限。

### 15.3 Provider 隔离

Provider 操作运行在短生命周期 worker 中，并受 deadline 约束。超时先终止，必要时强制结束；worker 崩溃映射为非肯定结果，不会把父 daemon 或数据库带入“可能成功”的模糊状态。

### 15.4 重放与陈旧证明

以下字段共同压缩重放空间：system id、duty digest、request nonce、victim incarnation、protected-set digest、mapping generation、daemon boot id、journal sequence、proof generation 和 freshness window。少校验一个维度，都可能把“过去对另一个对象成立”误当成“现在对这个恢复请求成立”。

## 16. Fail-closed 矩阵

| 场景 | daemon 结果 | 数据库行为 |
|---|---|---|
| provider 未注册 | UNAVAILABLE | 不创建 admission |
| 配置 target 无法精确解析 | REJECTED/UNKNOWN | 不创建 admission |
| OFF 命令退出 0，但读回仍 ON | UNKNOWN/REJECTED | 不创建 admission |
| 目标 OFF，但 I/O 未 drained | <code>DENY_IO_NOT_DRAINED</code> | 不创建 admission |
| 读回 target UUID 不符 | REJECTED | 不创建 admission，报告映射风险 |
| journal CRC/digest 链损坏 | <code>DENY_JOURNAL</code> / UNAVAILABLE | 停止服务能力，不复用旧 proof |
| config mapping generation 改变 | INVALIDATED | 旧 proof/operation 失效 |
| proof 超过 freshness | stale | 重新获取，不降级使用 |
| daemon 连接断开 | unavailable | process-local admission 不能继续扩张 |
| formation 或 victim incarnation 改变 | mismatch | 丢弃旧响应，按新 Need 重来 |
| rejoin 只看到 ON，未 drained | 非 READY | 不接纳节点 |

## 17. 生产部署认证清单

代码中的 provider ABI 只是机制。要让 external fencing 真正成为生产恢复权威，至少需要完成以下部署级闭包。

### 17.1 带外管理面

- 每个节点具有不可由本机数据库进程篡改的外部控制目标；
- 对 Oracle/IPMI 类部署，BMC 支持 IPMI-over-LAN，并通过独立或受控管理网络到达；
- 凭据只存放在 root 可读配置或等价 secret store；
- 每个 node id 到 target UUID 的映射经过双人核验；
- 误操作测试能证明不会 fence 到错误节点。

### 17.2 Provider 认证

- <code>resolve</code> 能返回稳定、唯一的 target identity；
- <code>actuate_off</code> 与 <code>actuate_on</code> 均有有界 deadline；
- <code>readback</code> 独立于动作返回值，能区分 ON、OFF、TRANSITIONING、UNKNOWN；
- I/O drain 语义与实际共享存储路径一致，而不只是电源状态；
- provider native errors 全部映射到闭合结果，不存在“未知码按成功处理”；
- worker timeout、kill、daemon crash 和设备离线均经过故障注入验证。

### 17.3 集群绑定

- system identifier 与目标数据库一致；
- protected-set digest 覆盖恢复会写到的全部共享资源；
- current writer set、victim incarnation 和 formation 在消费点重新读取；
- mapping generation 的升级、回滚和同世代篡改测试通过；
- 多受害节点必须逐一获得 admission，不允许多数替代全集。

### 17.4 持久性与安全运维

- journal 所在文件系统支持所依赖的追加和 <code>fsync</code> 语义；
- journal 损坏、部分尾写和磁盘满均有演练；
- daemon restart 后不会直接复用过期 admission；
- rejoin 必须走 ADMIN prepare + 当前 LMON 授权 + <code>ON + DRAINED</code> readback；
- 审计能关联 request nonce、operation id、target、journal sequence 和最终数据库 admission。

只有 provider、设备、网络、配置、journal 和数据库 consumer 一起通过认证，生产激活门才有安全意义。

## 18. 故障诊断：先看哪一层

~~~mermaid
flowchart TD
    S[恢复未获得 external-fence admission] --> A{daemon 可连接?}
    A -- no --> A1[检查进程、socket、peer uid/gid、配置权限]
    A -- yes --> B{provider 可用?}
    B -- no --> B1[检查 provider id/ABI、管理网络、凭据]
    B -- yes --> C{target 精确解析?}
    C -- no --> C1[核对 node-to-target map 与 generation]
    C -- yes --> D{OFF + DRAINED?}
    D -- no --> D1[区分 ON、TRANSITIONING、NOT_DRAINED、UNKNOWN]
    D -- yes --> E{journal 完整?}
    E -- no --> E1[停止恢复，处理 CRC/digest/磁盘持久性故障]
    E -- yes --> F{proof 当前且精确匹配?}
    F -- no --> F1[检查 freshness、boot id、incarnation、formation、protected set]
    F -- yes --> G{NeedSet 全部满足?}
    G -- no --> G1[定位尚无 admission 的 victim/resource]
    G -- yes --> H[检查数据库功能激活与调用路径]
~~~

常见 deny reason 的解释：

| 原因 | 优先检查 |
|---|---|
| <code>PROTOCOL</code> | frame version、长度、codec、nonce |
| <code>PROVIDER_REJECTED</code> | 目标映射、权限、设备明确拒绝 |
| <code>PROVIDER_UNKNOWN</code> | readback 不确定、设备转换中 |
| <code>DAEMON_UNAVAILABLE</code> | provider registry、配置、socket、worker |
| <code>TIMEOUT</code> | 管理网络、BMC 响应、deadline |
| <code>JOURNAL</code> | journal CRC、digest chain、<code>fsync</code>、磁盘空间 |
| <code>MAPPING_CHANGED</code> | mapping generation 或同世代配置变更 |
| <code>REJOIN_INVALIDATED</code> | rejoin gate、formation 或 operation ownership 改变 |
| <code>IO_NOT_DRAINED</code> | 目标虽 OFF，但共享存储 I/O 仍未证明排空 |

## 19. Oracle RAC 与 PGRAC 的逐项对应

| 维度 | Oracle 已验证公开行为 | PGRAC 公开实现或目标 | 结论 |
|---|---|---|---|
| membership | CSS 管理成员关系与 join/leave 通知 | cssd/quorum/reconfiguration 提供集群事实 | 语义同类，不推断内部协议相同 |
| failure isolation owner | <code>cssdagent</code> 等 Clusterware 组件提供 I/O fencing | 独立 <code>pgrac-fenced</code> 权限域 | 责任分层相近，进程名与 wire 自研 |
| 外部控制 | 支持 IPMI/BMC，能不依赖受害 OS 重启节点 | provider ABI 支持外部 target；主干未含认证生产 provider | Oracle 行为已验证；PGRAC 当前仍 fail closed |
| 终态确认 | 公开文档强调 failure isolation 结果 | 精确 target + OFF/ON + DRAINED readback | PGRAC 显式适配，Oracle 未公开同样字段 |
| 恢复绑定 | RAC 在成员/恢复框架内隔离并恢复实例 | proof 绑定 formation、incarnation、protected set | 外部安全语义对齐，证书格式自研 |
| 持久证据 | Oracle 未公开 journal 格式 | CRC32C + SHA-256 链式 fixed-record journal | PGRAC 自研实现 |
| rejoin | Clusterware 管理节点或实例重新加入 | ADMIN/LMON 分权，要求 ON + DRAINED | 目标语义对齐，opcode 自研 |
| 当前生产可用性 | Oracle 产品提供相应 Clusterware 能力 | PGRAC 主干 provider-neutral 机制存在，生产激活关闭 | 不能宣称当前能力等价 |

## 20. 公开源码导航

- 数据库侧 external-fence 消费与激活边界：[cluster_external_fence.c](../../../src/backend/cluster/cluster_external_fence.c)
- 数据库侧 NeedSet/AdmissionSet API：[cluster_external_fence.h](../../../src/include/cluster/cluster_external_fence.h)
- provider-neutral wire protocol：[pgrac_external_fence_protocol.h](../../../src/include/common/pgrac_external_fence_protocol.h)
- daemon 入口与当前生产边界：[pgrac_fenced.c](../../../src/bin/pgrac_fenced/pgrac_fenced.c)
- daemon FSM 与 peer credential：[pgrac_fenced_core.c](../../../src/bin/pgrac_fenced/pgrac_fenced_core.c)
- provider ABI、worker 与终态分类：[pgrac_fenced_provider.c](../../../src/bin/pgrac_fenced/pgrac_fenced_provider.c)
- 固定记录 journal：[pgrac_fenced_journal.c](../../../src/bin/pgrac_fenced/pgrac_fenced_journal.c)
- root-owned 配置与映射世代：[pgrac_fenced_config.c](../../../src/bin/pgrac_fenced/pgrac_fenced_config.c)
- control-root 功能激活：[cluster_control_root.c](../../../src/backend/cluster/cluster_control_root.c)

相关架构文档：

- [PGRAC 架构总览](../overview.md)
- [节点变化：rejoin、formation 与 recovery](../rac-node-change/README.md)
- [集群配置](../../user-guide/configuration.md)

## 21. Oracle 官方资料

- [Introduction to Oracle Clusterware](https://docs.oracle.com/en/database/oracle/oracle-database/26/cwadd/introduction-to-oracle-clusterware.html)
- [Oracle Clusterware Administration — Node Failure Isolation](https://docs.oracle.com/en/database/oracle/oracle-database/26/cwadd/oracle-clusterware-administration.html)
- [Requirements for Enabling IPMI](https://docs.oracle.com/en/database/oracle/oracle-database/26/cwlin/requirements-for-enabling-ipmi.html)
- [Configuring the IPMI Management Network](https://docs.oracle.com/en/database/oracle/oracle-database/26/cwlin/configuring-the-ipmi-management-network.html)
- [Oracle Database Background Processes — FENC](https://docs.oracle.com/en/database/oracle/oracle-database/26/refrn/background-processes.html)

## 22. 结论

External fencing 的价值不在于“能远程关机”，而在于把一个容易误判的物理世界事实，变成数据库能安全消费的短期权威：

~~~text
外部 target 的精确终态
  × 正确的 node/incarnation
  × 正确的 protected set
  × 正确的 mapping/formation
  × durable journal
  × fresh proof
  × live consumer validation
= 仅对本次恢复需要有效的 admission
~~~

Oracle Clusterware 公开证明了 failure isolation、外部 BMC/IPMI 控制与集群恢复之间的必要关系；PGRAC 在这一语义边界上保持同样的安全方向，同时用 provider-neutral codec、链式 journal、精确 binding 和短期 AdmissionSet 完成 PostgreSQL 架构适配。

当前公开主干最重要的事实也同样明确：**机制框架已经存在，但没有认证生产 provider 就不开放恢复写权限。** 这不是功能缺陷的掩盖，而是共享存储集群在证据不完整时必须保持的 fail-closed 边界。
