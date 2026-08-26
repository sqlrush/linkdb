# 拓扑、成员关系与重配置视图

以下字段表与当前 SQL catalog 定义逐列对应。

## `pg_cluster_nodes`

启动时读取的集群拓扑。

- 行基数：每个声明节点一行；无配置时为本地回退行。
- 刷新来源：启动配置快照。
- 查询成本：低。
- 读取原则：同一行的 epoch/generation/state 应作为一个诊断 tuple 解读。

| 字段 | SQL 类型 | 单位/格式 | 空值或哨兵 | 含义 |
|---|---|---|---|---|
| `node_id` | `integer` | — | -1 或 0 的含义依视图而定，通常表示未分配/无 owner | 产生该行的节点编号。 |
| `hostname` | `text` | — | 枚举 unknown、空串或 NULL 均按本行说明解释 | 拓扑中声明的主机名。 |
| `interconnect_addr` | `text` | — | 枚举 unknown、空串或 NULL 均按本行说明解释 | 节点间通信地址。 |
| `public_addr` | `text` | — | 枚举 unknown、空串或 NULL 均按本行说明解释 | 客户端访问地址。 |
| `role` | `text` | — | 枚举 unknown、空串或 NULL 均按本行说明解释 | 节点或实例的当前角色。 |
| `region` | `text` | — | 枚举 unknown、空串或 NULL 均按本行说明解释 | 拓扑中的 region 标签。 |
| `is_self` | `boolean` | 布尔 | false 表示该条件当前不成立 | 该行是否对应当前实例。 |

示例：

```sql
SELECT * FROM pg_cluster_nodes LIMIT 50;
```

## `pg_stat_cluster_nodes`

节点运行身份和版本状态；当前 producer 固定返回本地节点且 state 固定为 online。

- 行基数：当前实现固定一行。
- 刷新来源：启动身份与 build 常量。
- 查询成本：低。
- 读取原则：同一行的 epoch/generation/state 应作为一个诊断 tuple 解读。

| 字段 | SQL 类型 | 单位/格式 | 空值或哨兵 | 含义 |
|---|---|---|---|---|
| `node_id` | `integer` | — | -1 或 0 的含义依视图而定，通常表示未分配/无 owner | 产生该行的节点编号。 |
| `role` | `text` | — | 枚举 unknown、空串或 NULL 均按本行说明解释 | 节点或实例的当前角色。 |
| `state` | `text` | — | 枚举 unknown、空串或 NULL 均按本行说明解释 | 该对象当前状态；枚举域由所属视图定义。 |
| `startup_time` | `timestamptz` | 时间戳 | NULL 通常表示尚未发生或尚未记录 | 当前实例的启动时间。 |
| `pgrac_version` | `text` | — | 枚举 unknown、空串或 NULL 均按本行说明解释 | 当前实例报告的 PGRAC 版本。 |
| `pg_version` | `text` | — | 枚举 unknown、空串或 NULL 均按本行说明解释 | 当前实例报告的 PostgreSQL 版本。 |

示例：

```sql
SELECT * FROM pg_stat_cluster_nodes LIMIT 50;
```

## `pg_cluster_cssd_peers`

CSSD 应用层存活判断。

- 行基数：每个声明 peer 一行。
- 刷新来源：CSSD 共享状态实时快照。
- 查询成本：低。
- 读取原则：同一行的 epoch/generation/state 应作为一个诊断 tuple 解读。

| 字段 | SQL 类型 | 单位/格式 | 空值或哨兵 | 含义 |
|---|---|---|---|---|
| `node_id` | `integer` | — | -1 或 0 的含义依视图而定，通常表示未分配/无 owner | 产生该行的节点编号。 |
| `state` | `text` | — | 枚举 unknown、空串或 NULL 均按本行说明解释 | CSSD 应用层状态：alive、suspected 或 dead；与 transport state 分层。 |
| `last_heartbeat_send_at` | `timestamptz` | 时间戳 | NULL 通常表示尚未发生或尚未记录 | 最近 / 心跳 / 发送的时间戳。 |
| `last_heartbeat_recv_at` | `timestamptz` | 时间戳 | NULL 通常表示尚未发生或尚未记录 | 最近 / 心跳 / 接收的时间戳。 |
| `heartbeat_send_count` | `bigint` | 次/个 | 0 表示本计数生命周期内未观察到事件 | 心跳 / 发送的累计计数。 |
| `heartbeat_recv_count` | `bigint` | 次/个 | 0 表示本计数生命周期内未观察到事件 | 心跳 / 接收的累计计数。 |
| `suspected_since` | `timestamptz` | 时间戳 | NULL 通常表示尚未发生或尚未记录 | peer 首次进入 suspected 状态的时间。 |
| `dead_since` | `timestamptz` | 时间戳 | NULL 通常表示尚未发生或尚未记录 | peer 首次进入 dead 状态的时间。 |
| `suspected_transitions` | `bigint` | — | 枚举 unknown、空串或 NULL 均按本行说明解释 | peer 进入 suspected 状态的累计次数。 |

示例：

```sql
SELECT * FROM pg_cluster_cssd_peers LIMIT 50;
```

## `pg_cluster_quorum_state`

本节点 quorum/lease 综合判定。

- 行基数：固定一行。
- 刷新来源：QVOTEC 共享状态实时快照。
- 查询成本：低。
- 读取原则：同一行的 epoch/generation/state 应作为一个诊断 tuple 解读。

| 字段 | SQL 类型 | 单位/格式 | 空值或哨兵 | 含义 |
|---|---|---|---|---|
| `in_quorum` | `boolean` | 布尔 | false 表示该条件当前不成立 | 综合 lease、磁盘多数和冻结状态后的最终 quorum 布尔结论。 |
| `quorum_size` | `integer` | — | 枚举 unknown、空串或 NULL 均按本行说明解释 | 当前拓扑下要求的多数派票数。 |
| `disks_ok` | `integer` | — | 枚举 unknown、空串或 NULL 均按本行说明解释 | 当前成功参与 quorum 的 voting disk 数量。 |
| `disks_total` | `integer` | — | 枚举 unknown、空串或 NULL 均按本行说明解释 | 配置的 voting disk 总数。 |
| `current_epoch_at_boot` | `bigint` | — | 枚举 unknown、空串或 NULL 均按本行说明解释 | 本次启动从 quorum 介质读取并采纳的 epoch。 |
| `last_quorum_loss_at` | `timestamptz` | 时间戳 | NULL 通常表示尚未发生或尚未记录 | 最近 / 多数派 / 丢失的时间戳。 |
| `collision_state` | `text` | — | 枚举 unknown、空串或 NULL 均按本行说明解释 | voting disk 上是否观察到身份或 epoch 冲突。 |

示例：

```sql
SELECT * FROM pg_cluster_quorum_state LIMIT 50;
```

## `pg_cluster_voting_disks`

voting disk 路径目录；当前 per-disk state/timestamp/counter 尚未接入 driver，固定投影 unknown/NULL/0。

- 行基数：每个配置路径一行。
- 刷新来源：postmaster-frozen GUC 字符串。
- 查询成本：低。
- 读取原则：同一行的 epoch/generation/state 应作为一个诊断 tuple 解读。

| 字段 | SQL 类型 | 单位/格式 | 空值或哨兵 | 含义 |
|---|---|---|---|---|
| `path` | `text` | — | 枚举 unknown、空串或 NULL 均按本行说明解释 | 配置的文件、设备或目录路径。 |
| `state` | `text` | — | 枚举 unknown、空串或 NULL 均按本行说明解释 | 当前固定为 unknown；尚不能作为 per-disk 健康结论。 |
| `last_read_at` | `timestamptz` | 时间戳 | NULL 通常表示尚未发生或尚未记录 | 最近 / 读取的时间戳。 |
| `last_write_at` | `timestamptz` | 时间戳 | NULL 通常表示尚未发生或尚未记录 | 最近 / 写入的时间戳。 |
| `read_count` | `bigint` | 次/个 | 0 表示本计数生命周期内未观察到事件 | 当前固定为 0 的占位计数；尚未接入 per-disk driver。 |
| `write_count` | `bigint` | 次/个 | 0 表示本计数生命周期内未观察到事件 | 当前固定为 0 的占位计数；尚未接入 per-disk driver。 |
| `io_error_count` | `bigint` | 次/个 | 0 表示本计数生命周期内未观察到事件 | 当前固定为 0 的占位计数；尚未接入 per-disk driver。 |

示例：

```sql
SELECT * FROM pg_cluster_voting_disks LIMIT 50;
```

## `pg_cluster_fence_state`

freeze/thaw 与 self-fence 状态。

- 行基数：固定一行。
- 刷新来源：fence 共享状态实时快照。
- 查询成本：低。
- 读取原则：同一行的 epoch/generation/state 应作为一个诊断 tuple 解读。

| 字段 | SQL 类型 | 单位/格式 | 空值或哨兵 | 含义 |
|---|---|---|---|---|
| `last_freeze_at` | `timestamptz` | 时间戳 | NULL 通常表示尚未发生或尚未记录 | 最近 / 冻结的时间戳。 |
| `last_thaw_at` | `timestamptz` | 时间戳 | NULL 通常表示尚未发生或尚未记录 | 最近 / 解冻的时间戳。 |
| `self_fence_pending` | `boolean` | 布尔 | false 表示该条件当前不成立 | 本节点是否处于等待 self-fence 宽限期结束的状态。 |
| `self_fence_grace_remaining_ms` | `integer` | 毫秒 | 枚举 unknown、空串或 NULL 均按本行说明解释 | 本节点 / 隔离 / 宽限 / 剩余，单位毫秒。 |
| `freeze_broadcast_count` | `bigint` | 次/个 | 0 表示本计数生命周期内未观察到事件 | 冻结 / 广播的累计计数。 |
| `thaw_broadcast_count` | `bigint` | 次/个 | 0 表示本计数生命周期内未观察到事件 | 解冻 / 广播的累计计数。 |
| `self_fence_initiated_count` | `bigint` | 次/个 | 0 表示本计数生命周期内未观察到事件 | 本节点 / 隔离 / 已发起的累计计数。 |
| `freeze_signal_received_count` | `bigint` | 次/个 | 0 表示本计数生命周期内未观察到事件 | 冻结 / 信号 / 已接收的累计计数。 |

示例：

```sql
SELECT * FROM pg_cluster_fence_state LIMIT 50;
```

## `pg_cluster_reconfig_state`

最近一次重配置 episode。

- 行基数：启用集群时一行，否则零行。
- 刷新来源：reconfig 共享状态快照。
- 查询成本：低。
- 读取原则：同一行的 epoch/generation/state 应作为一个诊断 tuple 解读。

| 字段 | SQL 类型 | 单位/格式 | 空值或哨兵 | 含义 |
|---|---|---|---|---|
| `event_id` | `bigint` | — | -1 或 0 的含义依视图而定，通常表示未分配/无 owner | 重配置 episode 标识；0 表示从未应用 episode。 |
| `coordinator_node_id` | `integer` | — | -1 或 0 的含义依视图而定，通常表示未分配/无 owner | `coordinator` / 节点的标识符。 |
| `old_epoch` | `bigint` | — | 枚举 unknown、空串或 NULL 均按本行说明解释 | episode 应用前的 membership epoch。 |
| `new_epoch` | `bigint` | — | 枚举 unknown、空串或 NULL 均按本行说明解释 | episode 计划并应用后的 membership epoch。 |
| `dead_bitmap` | `text` | 位图 | 枚举 unknown、空串或 NULL 均按本行说明解释 | 本次重配置认定失效节点的十六进制位图。 |
| `applied_at` | `timestamptz` | 时间戳 | NULL 通常表示尚未发生或尚未记录 | `applied`的时间戳。 |
| `observer_role` | `text` | — | 枚举 unknown、空串或 NULL 均按本行说明解释 | 本节点在该重配置 episode 中承担的角色。 |
| `event_seq` | `bigint` | — | 枚举 unknown、空串或 NULL 均按本行说明解释 | 本地观察的重配置事件序号。 |
| `cssd_dead_generation` | `bigint` | — | 枚举 unknown、空串或 NULL 均按本行说明解释 | 触发该 episode 的 CSSD dead evidence generation。 |
| `reconfig_kind` | `text` | — | 枚举 unknown、空串或 NULL 均按本行说明解释 | 本次重配置的事件种类。 |

示例：

```sql
SELECT * FROM pg_cluster_reconfig_state LIMIT 50;
```

## `pg_cluster_membership`

每个节点的 admission/membership 决策。

- 行基数：每个声明节点一行。
- 刷新来源：membership 共享状态快照。
- 查询成本：低。
- 读取原则：同一行的 epoch/generation/state 应作为一个诊断 tuple 解读。

| 字段 | SQL 类型 | 单位/格式 | 空值或哨兵 | 含义 |
|---|---|---|---|---|
| `node_id` | `integer` | — | -1 或 0 的含义依视图而定，通常表示未分配/无 owner | 产生该行的节点编号。 |
| `declared` | `boolean` | 布尔 | false 表示该条件当前不成立 | 节点是否存在于当前拓扑声明中。 |
| `state` | `text` | — | 枚举 unknown、空串或 NULL 均按本行说明解释 | decision state：absent、dead、joining、member 或 rejected。 |
| `presented_incarnation` | `bigint` | — | 枚举 unknown、空串或 NULL 均按本行说明解释 | QVOTEC 最近观察到的节点 incarnation。 |
| `last_admitted_incarnation` | `bigint` | — | 枚举 unknown、空串或 NULL 均按本行说明解释 | 该节点已准入 incarnation 的单调 floor。 |
| `admitted_epoch` | `bigint` | — | 枚举 unknown、空串或 NULL 均按本行说明解释 | 该 membership 行当前观察/准入所绑定的 epoch。 |
| `removed` | `boolean` | 布尔 | false 表示该条件当前不成立 | 节点是否已进入永久移除终态。 |
| `removed_epoch` | `bigint` | — | 枚举 unknown、空串或 NULL 均按本行说明解释 | 永久移除写入的 epoch；未移除时为 0。 |

示例：

```sql
SELECT * FROM pg_cluster_membership LIMIT 50;
```

## `pg_cluster_clean_leave_state`

本节点协作式离开进度。

- 行基数：启用集群时一行，否则零行。
- 刷新来源：clean-leave 共享状态快照。
- 查询成本：低。
- 读取原则：同一行的 epoch/generation/state 应作为一个诊断 tuple 解读。

| 字段 | SQL 类型 | 单位/格式 | 空值或哨兵 | 含义 |
|---|---|---|---|---|
| `phase` | `text` | — | 枚举 unknown、空串或 NULL 均按本行说明解释 | idle/requested/quiescing/ges_draining/gcs_flushing/barrier_wait/committed/aborted 系列阶段。 |
| `leaving_node_id` | `integer` | — | -1 或 0 的含义依视图而定，通常表示未分配/无 owner | `leaving` / 节点的标识符。 |
| `leave_epoch` | `bigint` | — | 枚举 unknown、空串或 NULL 均按本行说明解释 | `leave` / epoch的当前快照值。 |
| `ges_drained_count` | `bigint` | 次/个 | 0 表示本计数生命周期内未观察到事件 | `ges` / `drained`的累计计数。 |
| `gcs_flushed_count` | `bigint` | 次/个 | 0 表示本计数生命周期内未观察到事件 | `gcs` / `flushed`的累计计数。 |
| `shards_remastered` | `bigint` | — | 枚举 unknown、空串或 NULL 均按本行说明解释 | `shards` / `remastered`的当前快照值。 |
| `survivor_ack_count` | `integer` | 次/个 | 0 表示本计数生命周期内未观察到事件 | ack bitmap 的 popcount，而不是独立累加器。 |
| `barrier_deadline` | `timestamptz` | 时间戳 | NULL 通常表示尚未发生或尚未记录 | 屏障 / 截止时间的当前快照值。 |
| `escalate_count` | `bigint` | 次/个 | 0 表示本计数生命周期内未观察到事件 | `escalate`的累计计数。 |

示例：

```sql
SELECT * FROM pg_cluster_clean_leave_state LIMIT 50;
```

## `pg_cluster_node_removal_state`

永久移除节点的当前进度。

- 行基数：启用集群时一行，否则零行。
- 刷新来源：node-removal 共享状态快照。
- 查询成本：低。
- 读取原则：同一行的 epoch/generation/state 应作为一个诊断 tuple 解读。

| 字段 | SQL 类型 | 单位/格式 | 空值或哨兵 | 含义 |
|---|---|---|---|---|
| `phase` | `text` | — | 枚举 unknown、空串或 NULL 均按本行说明解释 | idle 到 precheck、fence、shrink、cleanup、committed/aborted 的阶段。 |
| `target_node_id` | `integer` | — | -1 或 0 的含义依视图而定，通常表示未分配/无 owner | 目标 / 节点的标识符。 |
| `coordinator_node_id` | `integer` | — | -1 或 0 的含义依视图而定，通常表示未分配/无 owner | `coordinator` / 节点的标识符。 |
| `remove_epoch` | `bigint` | — | 枚举 unknown、空串或 NULL 均按本行说明解释 | `remove` / epoch的当前快照值。 |
| `fence_armed` | `boolean` | 布尔 | false 表示该条件当前不成立 | 隔离 / `armed`的当前快照值。 |
| `membership_shrunk` | `boolean` | 布尔 | false 表示该条件当前不成立 | 成员关系 / `shrunk`的当前快照值。 |
| `grd_cleaned` | `boolean` | 布尔 | false 表示该条件当前不成立 | `grd` / `cleaned`的当前快照值。 |
| `pcm_cleaned` | `boolean` | 布尔 | false 表示该条件当前不成立 | `pcm` / `cleaned`的当前快照值。 |
| `ack_count` | `integer` | 次/个 | 0 表示本计数生命周期内未观察到事件 | 当前 removal ack bitmap 的 popcount。 |
| `deadline_us` | `bigint` | 微秒 | 枚举 unknown、空串或 NULL 均按本行说明解释 | 截止时间，单位微秒。 |
| `removal_committed_count` | `bigint` | 次/个 | 0 表示本计数生命周期内未观察到事件 | `removal` / 已提交的累计计数。 |
| `cleanup_blocked_count` | `bigint` | 次/个 | 0 表示本计数生命周期内未观察到事件 | 清理 / 阻塞的累计计数。 |
| `leftover_detected_count` | `bigint` | 次/个 | 0 表示本计数生命周期内未观察到事件 | `leftover` / `detected`的累计计数。 |
| `zombie_write_rejected_count` | `bigint` | 次/个 | 0 表示本计数生命周期内未观察到事件 | `zombie` / 写入 / 拒绝的累计计数。 |

示例：

```sql
SELECT * FROM pg_cluster_node_removal_state LIMIT 50;
```
