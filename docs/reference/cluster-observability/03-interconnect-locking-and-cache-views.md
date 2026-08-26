# Interconnect、锁与 Cache Fusion 视图

以下字段表与当前 SQL catalog 定义逐列对应。

## `pg_stat_cluster_wait_events`

本地已注册的集群等待事件。

- 行基数：每个等待事件一行。
- 刷新来源：注册表，查询时生成。
- 查询成本：低。
- 读取原则：同一行的 epoch/generation/state 应作为一个诊断 tuple 解读。

| 字段 | SQL 类型 | 单位/格式 | 空值或哨兵 | 含义 |
|---|---|---|---|---|
| `type` | `text` | — | 枚举 unknown、空串或 NULL 均按本行说明解释 | PostgreSQL wait-event type/class 名称。 |
| `name` | `text` | — | 枚举 unknown、空串或 NULL 均按本行说明解释 | PGRAC 注册的等待事件名称。 |

示例：

```sql
SELECT * FROM pg_stat_cluster_wait_events LIMIT 50;
```

## `pg_stat_gcluster_wait_events`

带 node_id 的集群等待事件；当前实现返回本节点。

- 行基数：每个可见节点/等待事件一行；当前为本节点。
- 刷新来源：注册表，查询时生成。
- 查询成本：低。
- 读取原则：同一行的 epoch/generation/state 应作为一个诊断 tuple 解读。

| 字段 | SQL 类型 | 单位/格式 | 空值或哨兵 | 含义 |
|---|---|---|---|---|
| `node_id` | `integer` | — | -1 或 0 的含义依视图而定，通常表示未分配/无 owner | 产生该行的节点编号。 |
| `type` | `text` | — | 枚举 unknown、空串或 NULL 均按本行说明解释 | PostgreSQL wait-event type/class 名称。 |
| `name` | `text` | — | 枚举 unknown、空串或 NULL 均按本行说明解释 | PGRAC 注册的等待事件名称。 |

示例：

```sql
SELECT * FROM pg_stat_gcluster_wait_events LIMIT 50;
```

## `pg_cluster_ic_peers`

Tier1 TCP peer 连接、心跳和流量状态。

- 行基数：每个声明 peer 一行。
- 刷新来源：共享内存实时快照。
- 查询成本：低至中。
- 读取原则：同一行的 epoch/generation/state 应作为一个诊断 tuple 解读。

| 字段 | SQL 类型 | 单位/格式 | 空值或哨兵 | 含义 |
|---|---|---|---|---|
| `node_id` | `integer` | — | -1 或 0 的含义依视图而定，通常表示未分配/无 owner | 产生该行的节点编号。 |
| `state` | `text` | — | 枚举 unknown、空串或 NULL 均按本行说明解释 | Tier1 transport 状态：down、connecting、connected 或 rejected；不代表 membership。 |
| `interconnect_addr` | `text` | — | 枚举 unknown、空串或 NULL 均按本行说明解释 | 节点间通信地址。 |
| `last_connect_at` | `timestamptz` | 时间戳 | NULL 通常表示尚未发生或尚未记录 | 最近 / 连接的时间戳。 |
| `last_send_at` | `timestamptz` | 时间戳 | NULL 通常表示尚未发生或尚未记录 | 最近 / 发送的时间戳。 |
| `last_recv_at` | `timestamptz` | 时间戳 | NULL 通常表示尚未发生或尚未记录 | 最近 / 接收的时间戳。 |
| `last_heartbeat_sent_at` | `timestamptz` | 时间戳 | NULL 通常表示尚未发生或尚未记录 | 最近 / 心跳 / `sent`的时间戳。 |
| `last_heartbeat_recv_at` | `timestamptz` | 时间戳 | NULL 通常表示尚未发生或尚未记录 | 最近 / 心跳 / 接收的时间戳。 |
| `heartbeat_send_count` | `bigint` | 次/个 | 0 表示本计数生命周期内未观察到事件 | 心跳 / 发送的累计计数。 |
| `heartbeat_recv_count` | `bigint` | 次/个 | 0 表示本计数生命周期内未观察到事件 | 心跳 / 接收的累计计数。 |
| `msg_send_count` | `bigint` | 次/个 | 0 表示本计数生命周期内未观察到事件 | `msg` / 发送的累计计数。 |
| `msg_recv_count` | `bigint` | 次/个 | 0 表示本计数生命周期内未观察到事件 | `msg` / 接收的累计计数。 |
| `bytes_send` | `bigint` | 字节 | 枚举 unknown、空串或 NULL 均按本行说明解释 | 字节 / 发送的当前快照值。 |
| `bytes_recv` | `bigint` | 字节 | 枚举 unknown、空串或 NULL 均按本行说明解释 | 字节 / 接收的当前快照值。 |
| `reconnect_count` | `integer` | 次/个 | 0 表示本计数生命周期内未观察到事件 | 重连的累计计数。 |
| `connect_error_count` | `integer` | 次/个 | 0 表示本计数生命周期内未观察到事件 | 连接 / 错误的累计计数。 |
| `last_errno` | `integer` | — | 枚举 unknown、空串或 NULL 均按本行说明解释 | 最近一次底层 socket 错误的 errno；0 表示无已记录 errno。 |
| `last_error_code` | `text` | — | 枚举 unknown、空串或 NULL 均按本行说明解释 | 最近一次 PGRAC/SQLSTATE 风格错误码；空串表示无记录。 |
| `last_error` | `text` | — | 枚举 unknown、空串或 NULL 均按本行说明解释 | 最近一次 transport 错误文本；空串表示无记录。 |
| `stale_epoch_drop_count` | `bigint` | 次/个 | 0 表示本计数生命周期内未观察到事件 | 陈旧 / epoch / 丢弃的累计计数。 |
| `chunk_reassembly_active` | `integer` | — | 枚举 unknown、空串或 NULL 均按本行说明解释 | 当前正在重组的 chunked message 数量。 |
| `chunk_reassembly_timeout_count` | `bigint` | 次/个 | 0 表示本计数生命周期内未观察到事件 | 分片 / `reassembly` / 超时的累计计数。 |
| `lamport_observe_advance_count` | `bigint` | 次/个 | 0 表示本计数生命周期内未观察到事件 | 接收消息导致本地 Lamport 时钟前移的累计次数。 |

示例：

```sql
SELECT * FROM pg_cluster_ic_peers LIMIT 50;
```

## `pg_stat_cluster_ic`

TCP/RDMA mux、RDMA 与 block-reply lane 状态。

- 行基数：每个 peer 一行。
- 刷新来源：共享内存实时快照。
- 查询成本：低至中。
- 读取原则：同一行的 epoch/generation/state 应作为一个诊断 tuple 解读。

| 字段 | SQL 类型 | 单位/格式 | 空值或哨兵 | 含义 |
|---|---|---|---|---|
| `node_id` | `integer` | — | -1 或 0 的含义依视图而定，通常表示未分配/无 owner | 产生该行的节点编号。 |
| `transport` | `text` | — | 枚举 unknown、空串或 NULL 均按本行说明解释 | 当前选中的传输方式。 |
| `rdma_state` | `text` | — | 枚举 unknown、空串或 NULL 均按本行说明解释 | 该 peer 的 RDMA 生命周期状态。 |
| `provider` | `text` | — | 枚举 unknown、空串或 NULL 均按本行说明解释 | 当前 RDMA/transport provider 名称。 |
| `rdma_addr` | `text` | — | 枚举 unknown、空串或 NULL 均按本行说明解释 | 拓扑中配置的 RDMA 地址；未配置时 NULL。 |
| `rdma_gid` | `text` | — | 枚举 unknown、空串或 NULL 均按本行说明解释 | 拓扑中配置的 RDMA GID；未配置时 NULL。 |
| `rdma_port` | `integer` | — | 枚举 unknown、空串或 NULL 均按本行说明解释 | 拓扑中配置的 RDMA port。 |
| `mr_registered` | `boolean` | 布尔 | false 表示该条件当前不成立 | 本节点 RDMA memory region 是否注册成功。 |
| `cq_depth` | `integer` | — | 枚举 unknown、空串或 NULL 均按本行说明解释 | 该 peer completion queue 当前/配置深度投影。 |
| `fallback_count` | `bigint` | 次/个 | 0 表示本计数生命周期内未观察到事件 | 回退的累计计数。 |
| `send_count` | `bigint` | 次/个 | 0 表示本计数生命周期内未观察到事件 | 发送的累计计数。 |
| `recv_count` | `bigint` | 次/个 | 0 表示本计数生命周期内未观察到事件 | 接收的累计计数。 |
| `bytes_send` | `bigint` | 字节 | 枚举 unknown、空串或 NULL 均按本行说明解释 | 字节 / 发送的当前快照值。 |
| `bytes_recv` | `bigint` | 字节 | 枚举 unknown、空串或 NULL 均按本行说明解释 | 字节 / 接收的当前快照值。 |
| `block_sge_send_count` | `bigint` | 次/个 | 0 表示本计数生命周期内未观察到事件 | 数据块 / `sge` / 发送的累计计数。 |
| `block_sge_fallback_count` | `bigint` | 次/个 | 0 表示本计数生命周期内未观察到事件 | 数据块 / `sge` / 回退的累计计数。 |
| `tier3_send_count` | `bigint` | 次/个 | 0 表示本计数生命周期内未观察到事件 | `tier3` / 发送的累计计数。 |
| `inline_send_count` | `bigint` | 次/个 | 0 表示本计数生命周期内未观察到事件 | 内联 / 发送的累计计数。 |
| `unsignaled_batch_count` | `bigint` | 次/个 | 0 表示本计数生命周期内未观察到事件 | `unsignaled` / `batch`的累计计数。 |
| `busypoll_us_burned` | `bigint` | — | 枚举 unknown、空串或 NULL 均按本行说明解释 | `busypoll` / `us` / `burned`的当前快照值。 |
| `busypoll_fallback_count` | `bigint` | 次/个 | 0 表示本计数生命周期内未观察到事件 | `busypoll` / 回退的累计计数。 |
| `block_reply_lane_state` | `text` | — | 枚举 unknown、空串或 NULL 均按本行说明解释 | block reply 专用 lane 的当前状态。 |
| `block_reply_lane_fallback_count` | `bigint` | 次/个 | 0 表示本计数生命周期内未观察到事件 | 数据块 / 响应 / `lane` / 回退的累计计数。 |
| `block_reply_lane_error_count` | `bigint` | 次/个 | 0 表示本计数生命周期内未观察到事件 | 数据块 / 响应 / `lane` / 错误的累计计数。 |
| `latency_us_sum` | `bigint` | — | 枚举 unknown、空串或 NULL 均按本行说明解释 | 已采样 RDMA 延迟的累计微秒数；与 sample_count 相除得到均值。 |
| `latency_sample_count` | `bigint` | 次/个 | 0 表示本计数生命周期内未观察到事件 | RDMA 延迟样本数。 |
| `last_error_code` | `text` | — | 枚举 unknown、空串或 NULL 均按本行说明解释 | 最近 / 错误 / `code`的当前快照值。 |
| `last_error` | `text` | — | 枚举 unknown、空串或 NULL 均按本行说明解释 | 最近 / 错误的当前快照值。 |
| `last_block_reply_error` | `text` | — | 枚举 unknown、空串或 NULL 均按本行说明解释 | block reply lane 最近错误；无记录时 NULL。 |

示例：

```sql
SELECT * FROM pg_stat_cluster_ic LIMIT 50;
```

## `pg_cluster_ic_msg_types`

已注册的 interconnect 消息类型。

- 行基数：每个消息类型一行。
- 刷新来源：进程内 dispatch 注册表。
- 查询成本：低。
- 读取原则：同一行的 epoch/generation/state 应作为一个诊断 tuple 解读。

| 字段 | SQL 类型 | 单位/格式 | 空值或哨兵 | 含义 |
|---|---|---|---|---|
| `msg_type` | `integer` | — | 枚举 unknown、空串或 NULL 均按本行说明解释 | wire message type 的数值编号。 |
| `name` | `text` | — | 枚举 unknown、空串或 NULL 均按本行说明解释 | 注册对象的稳定名称。 |
| `allowed_producer_mask` | `bigint` | 位图 | 枚举 unknown、空串或 NULL 均按本行说明解释 | 允许产生该消息的进程角色位图。 |
| `broadcast_ok` | `boolean` | 布尔 | false 表示该条件当前不成立 | 该消息是否允许广播。 |
| `handler_present` | `boolean` | 布尔 | false 表示该条件当前不成立 | 本进程是否注册了入站处理器；false 可表示 send-only 类型。 |

示例：

```sql
SELECT * FROM pg_cluster_ic_msg_types LIMIT 50;
```

## `pg_cluster_grd_shards`

GRD shard 到 master node 的映射。

- 行基数：固定 4096 行。
- 刷新来源：GRD master map 快照。
- 查询成本：中。
- 读取原则：同一行的 epoch/generation/state 应作为一个诊断 tuple 解读。

| 字段 | SQL 类型 | 单位/格式 | 空值或哨兵 | 含义 |
|---|---|---|---|---|
| `shard_id` | `integer` | — | -1 或 0 的含义依视图而定，通常表示未分配/无 owner | GRD shard 编号，范围 0..4095。 |
| `master_node_id` | `integer` | — | -1 或 0 的含义依视图而定，通常表示未分配/无 owner | 查询时该 shard 映射到的 resource master 节点。 |
| `is_local` | `boolean` | 布尔 | false 表示该条件当前不成立 | master_node_id 是否等于本节点 cluster.node_id。 |

示例：

```sql
SELECT * FROM pg_cluster_grd_shards LIMIT 50;
```

## `pg_cluster_grd_entries`

GRD resource entry 诊断快照。

- 行基数：每个当前 entry 一行。
- 刷新来源：遍历 GRD hash 表。
- 查询成本：高；不要在高频监控中全表扫描。
- 读取原则：同一行的 epoch/generation/state 应作为一个诊断 tuple 解读。

| 字段 | SQL 类型 | 单位/格式 | 空值或哨兵 | 含义 |
|---|---|---|---|---|
| `shard_id` | `integer` | — | -1 或 0 的含义依视图而定，通常表示未分配/无 owner | 由完整 resource identity hash 得出的 GRD shard。 |
| `field1` | `integer` | — | 枚举 unknown、空串或 NULL 均按本行说明解释 | resource identity 的第 1 个 32 位字段；需与 type/lockmethodid 联合解释。 |
| `field2` | `integer` | — | 枚举 unknown、空串或 NULL 均按本行说明解释 | resource identity 的第 2 个 32 位字段；需与 type/lockmethodid 联合解释。 |
| `field3` | `integer` | — | 枚举 unknown、空串或 NULL 均按本行说明解释 | resource identity 的第 3 个 32 位字段；需与 type/lockmethodid 联合解释。 |
| `field4` | `integer` | — | 枚举 unknown、空串或 NULL 均按本行说明解释 | resource identity 的第 4 个 32 位字段；需与 type/lockmethodid 联合解释。 |
| `type` | `integer` | — | 枚举 unknown、空串或 NULL 均按本行说明解释 | resource identity 类型编号。 |
| `lockmethodid` | `integer` | — | 枚举 unknown、空串或 NULL 均按本行说明解释 | PostgreSQL lock method 编号。 |
| `ngranted` | `integer` | — | 枚举 unknown、空串或 NULL 均按本行说明解释 | 当前 grant 数量。 |
| `nwaiters` | `integer` | — | 枚举 unknown、空串或 NULL 均按本行说明解释 | 当前 waiter 数量。 |
| `nconverts` | `integer` | — | 枚举 unknown、空串或 NULL 均按本行说明解释 | 当前 convert waiter 数量。 |
| `state_flags` | `integer` | — | 枚举 unknown、空串或 NULL 均按本行说明解释 | entry 状态位图；作为诊断值，不能单独授权。 |

示例：

```sql
SELECT * FROM pg_cluster_grd_entries LIMIT 50;
```

## `pg_cluster_lmd`

LMD 生命周期和工作计数。

- 行基数：固定一行。
- 刷新来源：LMD 共享状态实时快照。
- 查询成本：低。
- 读取原则：同一行的 epoch/generation/state 应作为一个诊断 tuple 解读。

| 字段 | SQL 类型 | 单位/格式 | 空值或哨兵 | 含义 |
|---|---|---|---|---|
| `pid` | `integer` | — | 枚举 unknown、空串或 NULL 均按本行说明解释 | 后台进程 PID；未启动时使用该视图定义的空值。 |
| `state` | `text` | — | 枚举 unknown、空串或 NULL 均按本行说明解释 | disabled/not_started/starting/ready/draining/stopped 生命周期状态。 |
| `reason` | `text` | — | 枚举 unknown、空串或 NULL 均按本行说明解释 | 只有 READY 时为 NULL；其他状态给出 disabled_by_config、lmd_not_ready 或 crashed_unavailable。 |
| `started_at` | `timestamptz` | 时间戳 | NULL 通常表示尚未发生或尚未记录 | 开始的时间戳。 |
| `ready_at` | `timestamptz` | 时间戳 | NULL 通常表示尚未发生或尚未记录 | `ready`的时间戳。 |
| `started_count` | `bigint` | 次/个 | 0 表示本计数生命周期内未观察到事件 | 开始的累计计数。 |
| `edge_submission_count` | `bigint` | 次/个 | 0 表示本计数生命周期内未观察到事件 | `edge` / `submission`的累计计数。 |
| `wake_count` | `bigint` | 次/个 | 0 表示本计数生命周期内未观察到事件 | 唤醒的累计计数。 |
| `idle_count` | `bigint` | 次/个 | 0 表示本计数生命周期内未观察到事件 | `idle`的累计计数。 |
| `error_count` | `bigint` | 次/个 | 0 表示本计数生命周期内未观察到事件 | 错误的累计计数。 |

示例：

```sql
SELECT * FROM pg_cluster_lmd LIMIT 50;
```

## `pg_cluster_shmem`

PGRAC 共享内存 region 注册表。

- 行基数：每个 region 一行。
- 刷新来源：启动注册表快照。
- 查询成本：低。
- 读取原则：同一行的 epoch/generation/state 应作为一个诊断 tuple 解读。

| 字段 | SQL 类型 | 单位/格式 | 空值或哨兵 | 含义 |
|---|---|---|---|---|
| `name` | `text` | — | 枚举 unknown、空串或 NULL 均按本行说明解释 | 注册对象的稳定名称。 |
| `size_bytes` | `bigint` | 字节 | 枚举 unknown、空串或 NULL 均按本行说明解释 | 大小，单位字节。 |
| `lwlock_count` | `integer` | 次/个 | 0 表示本计数生命周期内未观察到事件 | `lwlock`的累计计数。 |
| `owner_subsys` | `text` | — | 枚举 unknown、空串或 NULL 均按本行说明解释 | 注册并拥有该共享内存 region 的子系统。 |

示例：

```sql
SELECT * FROM pg_cluster_shmem LIMIT 50;
```

## `pg_stat_cluster_injections`

当前进程可见的故障注入点状态。

- 行基数：每个编译期注入点一行。
- 刷新来源：查询进程内注册表。
- 查询成本：低。
- 读取原则：同一行的 epoch/generation/state 应作为一个诊断 tuple 解读。

| 字段 | SQL 类型 | 单位/格式 | 空值或哨兵 | 含义 |
|---|---|---|---|---|
| `name` | `text` | — | 枚举 unknown、空串或 NULL 均按本行说明解释 | 注册对象的稳定名称。 |
| `fault_type` | `text` | — | 枚举 unknown、空串或 NULL 均按本行说明解释 | 当前 arm 的 fault 类型；none 表示未武装。 |
| `param` | `bigint` | — | 枚举 unknown、空串或 NULL 均按本行说明解释 | fault 类型解释的 64 位参数，例如 sleep 时长或 skip 次数。 |
| `hits` | `bigint` | — | 枚举 unknown、空串或 NULL 均按本行说明解释 | 当前进程中该注入点的累计命中次数。 |

示例：

```sql
SELECT * FROM pg_stat_cluster_injections LIMIT 50;
```
