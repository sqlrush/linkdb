# ADG、备份与恢复视图

以下字段表与当前 SQL catalog 定义逐列对应。

## `pg_stat_cluster_adg`

本节点 ADG 接收、应用和只读水位。

- 行基数：固定一行。
- 刷新来源：ADG 共享状态实时快照。
- 查询成本：低。
- 读取原则：同一行的 epoch/generation/state 应作为一个诊断 tuple 解读。

| 字段 | SQL 类型 | 单位/格式 | 空值或哨兵 | 含义 |
|---|---|---|---|---|
| `node_id` | `integer` | — | -1 或 0 的含义依视图而定，通常表示未分配/无 owner | 产生该行的节点编号。 |
| `dg_role` | `text` | — | 枚举 unknown、空串或 NULL 均按本行说明解释 | Data Guard 角色。 |
| `dg_mode` | `text` | — | 枚举 unknown、空串或 NULL 均按本行说明解释 | WAL shipping/acknowledgement 模式。 |
| `adg_enabled` | `boolean` | 布尔 | false 表示该条件当前不成立 | 本实例是否启用 ADG 运行路径。 |
| `apply_master_node_id` | `integer` | — | -1 或 0 的含义依视图而定，通常表示未分配/无 owner | 应用 / master / 节点的标识符。 |
| `apply_master_term` | `bigint` | — | 枚举 unknown、空串或 NULL 均按本行说明解释 | Apply Master 的单调 term，用于区分旧 owner。 |
| `mrp_status` | `text` | — | 枚举 unknown、空串或 NULL 均按本行说明解释 | managed recovery process 的生命周期状态。 |
| `receive_lsn` | `pg_lsn` | WAL LSN | NULL/无效 LSN 表示当前没有可报告水位 | 接收对应的 WAL LSN。 |
| `apply_lsn` | `pg_lsn` | WAL LSN | NULL/无效 LSN 表示当前没有可报告水位 | 应用对应的 WAL LSN。 |
| `standby_consistent_scn` | `bigint` | SCN | 0 通常表示尚无可用 SCN；不得解释为已证明的业务水位 | `standby` / 一致对应的集群 SCN。 |
| `lag_bytes` | `bigint` | 字节 | 枚举 unknown、空串或 NULL 均按本行说明解释 | 延迟，单位字节。 |
| `lag_seconds` | `double precision` | 秒 | 枚举 unknown、空串或 NULL 均按本行说明解释 | 接收与应用进度之间按时间估算的延迟秒数。 |
| `apply_rate_bytes_per_sec` | `double precision` | 字节/秒 | 枚举 unknown、空串或 NULL 均按本行说明解释 | 按当前 lag 样本估算的 WAL 应用速率。 |

示例：

```sql
SELECT * FROM pg_stat_cluster_adg LIMIT 50;
```

## `pg_stat_gcluster_adg`

带节点维度的 ADG 状态；当前实现返回本节点。

- 行基数：当前固定一行。
- 刷新来源：ADG 共享状态实时快照。
- 查询成本：低。
- 读取原则：同一行的 epoch/generation/state 应作为一个诊断 tuple 解读。

| 字段 | SQL 类型 | 单位/格式 | 空值或哨兵 | 含义 |
|---|---|---|---|---|
| `node_id` | `integer` | — | -1 或 0 的含义依视图而定，通常表示未分配/无 owner | 产生该行的节点编号。 |
| `dg_role` | `text` | — | 枚举 unknown、空串或 NULL 均按本行说明解释 | Data Guard 角色。 |
| `dg_mode` | `text` | — | 枚举 unknown、空串或 NULL 均按本行说明解释 | WAL shipping/acknowledgement 模式。 |
| `adg_enabled` | `boolean` | 布尔 | false 表示该条件当前不成立 | 本实例是否启用 ADG 运行路径。 |
| `apply_master_node_id` | `integer` | — | -1 或 0 的含义依视图而定，通常表示未分配/无 owner | 应用 / master / 节点的标识符。 |
| `apply_master_term` | `bigint` | — | 枚举 unknown、空串或 NULL 均按本行说明解释 | Apply Master 的单调 term，用于区分旧 owner。 |
| `mrp_status` | `text` | — | 枚举 unknown、空串或 NULL 均按本行说明解释 | managed recovery process 的生命周期状态。 |
| `receive_lsn` | `pg_lsn` | WAL LSN | NULL/无效 LSN 表示当前没有可报告水位 | 接收对应的 WAL LSN。 |
| `apply_lsn` | `pg_lsn` | WAL LSN | NULL/无效 LSN 表示当前没有可报告水位 | 应用对应的 WAL LSN。 |
| `standby_consistent_scn` | `bigint` | SCN | 0 通常表示尚无可用 SCN；不得解释为已证明的业务水位 | `standby` / 一致对应的集群 SCN。 |
| `lag_bytes` | `bigint` | 字节 | 枚举 unknown、空串或 NULL 均按本行说明解释 | 延迟，单位字节。 |
| `lag_seconds` | `double precision` | 秒 | 枚举 unknown、空串或 NULL 均按本行说明解释 | 接收与应用进度之间按时间估算的延迟秒数。 |
| `apply_rate_bytes_per_sec` | `double precision` | 字节/秒 | 枚举 unknown、空串或 NULL 均按本行说明解释 | 按当前 lag 样本估算的 WAL 应用速率。 |

示例：

```sql
SELECT * FROM pg_stat_gcluster_adg LIMIT 50;
```

## `pg_stat_cluster_backup`

当前或最近一次 cluster backup 状态。

- 行基数：固定一行。
- 刷新来源：backup 共享状态快照。
- 查询成本：低。
- 读取原则：同一行的 epoch/generation/state 应作为一个诊断 tuple 解读。

| 字段 | SQL 类型 | 单位/格式 | 空值或哨兵 | 含义 |
|---|---|---|---|---|
| `in_progress` | `boolean` | 布尔 | false 表示该条件当前不成立 | 操作当前是否仍在进行。 |
| `backup_id` | `text` | — | 枚举 unknown、空串或 NULL 均按本行说明解释 | backup 的稳定标识/标签。 |
| `coordinator_node_id` | `integer` | — | -1 或 0 的含义依视图而定，通常表示未分配/无 owner | `coordinator` / 节点的标识符。 |
| `start_redo_lsn` | `pg_lsn` | WAL LSN | NULL/无效 LSN 表示当前没有可报告水位 | `start` / redo对应的 WAL LSN。 |
| `checkpoint_lsn` | `pg_lsn` | WAL LSN | NULL/无效 LSN 表示当前没有可报告水位 | 检查点对应的 WAL LSN。 |
| `stop_cut_lsn` | `pg_lsn` | WAL LSN | NULL/无效 LSN 表示当前没有可报告水位 | `stop` / cut对应的 WAL LSN。 |
| `consistent_scn` | `bigint` | SCN | 0 通常表示尚无可用 SCN；不得解释为已证明的业务水位 | 一致对应的集群 SCN。 |
| `manifest_crc` | `bigint` | — | 枚举 unknown、空串或 NULL 均按本行说明解释 | manifest 内容的 CRC32C 校验值。 |
| `started_at` | `timestamptz` | 时间戳 | NULL 通常表示尚未发生或尚未记录 | 开始的时间戳。 |
| `stopped_at` | `timestamptz` | 时间戳 | NULL 通常表示尚未发生或尚未记录 | 结束的时间戳。 |
| `backup_parallel_channels` | `integer` | — | 枚举 unknown、空串或 NULL 均按本行说明解释 | 配置并投影到备份状态的并行 copy channel 数。 |
| `backup_wal_retention` | `integer` | — | 枚举 unknown、空串或 NULL 均按本行说明解释 | 配置并投影到备份状态的 WAL 保留提示，单位 MiB。 |
| `restore_points_enabled` | `boolean` | 布尔 | false 表示该条件当前不成立 | 恢复 / 点 / 启用状态的当前快照值。 |
| `restore_point_interval_ms` | `integer` | 毫秒 | 枚举 unknown、空串或 NULL 均按本行说明解释 | 恢复 / `point` / `interval`，单位毫秒。 |
| `backup_set_path` | `text` | — | 枚举 unknown、空串或 NULL 均按本行说明解释 | 最近一次 backup set 的文件系统路径。 |
| `manifest_path` | `text` | — | 枚举 unknown、空串或 NULL 均按本行说明解释 | 最近一次成功发布的 cluster manifest 路径。 |

示例：

```sql
SELECT * FROM pg_stat_cluster_backup LIMIT 50;
```

## `pg_cluster_backup_history`

最近保留的 backup manifest 摘要。

- 行基数：零或一行。
- 刷新来源：manifest 摘要快照。
- 查询成本：低。
- 读取原则：同一行的 epoch/generation/state 应作为一个诊断 tuple 解读。

| 字段 | SQL 类型 | 单位/格式 | 空值或哨兵 | 含义 |
|---|---|---|---|---|
| `backup_id` | `text` | — | 枚举 unknown、空串或 NULL 均按本行说明解释 | backup 的稳定标识/标签。 |
| `consistent_scn` | `bigint` | SCN | 0 通常表示尚无可用 SCN；不得解释为已证明的业务水位 | 一致对应的集群 SCN。 |
| `scn_durable_peak` | `bigint` | — | 枚举 unknown、空串或 NULL 均按本行说明解释 | backup cut 证明覆盖的最高 durable SCN。 |
| `timeline` | `integer` | — | 枚举 unknown、空串或 NULL 均按本行说明解释 | manifest 记录的 WAL timeline ID。 |
| `catversion` | `bigint` | — | 枚举 unknown、空串或 NULL 均按本行说明解释 | manifest 记录的 PostgreSQL CatalogVersionNo。 |
| `storage_id` | `integer` | — | -1 或 0 的含义依视图而定，通常表示未分配/无 owner | manifest 记录的 shared-storage backend 枚举 ID。 |
| `node_count` | `integer` | 次/个 | 0 表示本计数生命周期内未观察到事件 | 节点的累计计数。 |
| `thread_count` | `integer` | 次/个 | 0 表示本计数生命周期内未观察到事件 | 线程的累计计数。 |
| `manifest_crc` | `bigint` | — | 枚举 unknown、空串或 NULL 均按本行说明解释 | manifest 内容的 CRC32C 校验值。 |
| `backup_set_path` | `text` | — | 枚举 unknown、空串或 NULL 均按本行说明解释 | 最近一次 backup set 的文件系统路径。 |
| `manifest_path` | `text` | — | 枚举 unknown、空串或 NULL 均按本行说明解释 | 最近一次成功发布的 cluster manifest 路径。 |

示例：

```sql
SELECT * FROM pg_cluster_backup_history LIMIT 50;
```

## `pg_cluster_restore_points`

可用于 cluster PITR 的 restore point。

- 行基数：每个已记录 restore point 一行。
- 刷新来源：restore-point 目录快照。
- 查询成本：低。
- 读取原则：同一行的 epoch/generation/state 应作为一个诊断 tuple 解读。

| 字段 | SQL 类型 | 单位/格式 | 空值或哨兵 | 含义 |
|---|---|---|---|---|
| `restore_point_name` | `text` | — | 枚举 unknown、空串或 NULL 均按本行说明解释 | 关联的 restore point 名称。 |
| `cut_scn` | `bigint` | SCN | 0 通常表示尚无可用 SCN；不得解释为已证明的业务水位 | cut对应的集群 SCN。 |
| `thread_count` | `integer` | 次/个 | 0 表示本计数生命周期内未观察到事件 | 线程的累计计数。 |
| `incarnation` | `integer` | — | 枚举 unknown、空串或 NULL 均按本行说明解释 | 创建 restore point 时记录的 cluster incarnation。 |
| `created_at` | `timestamptz` | 时间戳 | NULL 通常表示尚未发生或尚未记录 | `created`的时间戳。 |

示例：

```sql
SELECT * FROM pg_cluster_restore_points LIMIT 50;
```

## `pg_cluster_pitr_status`

当前 cluster PITR target 的解析结果。

- 行基数：固定一行。
- 刷新来源：PITR 配置与 manifest 的查询时解析。
- 查询成本：低。
- 读取原则：同一行的 epoch/generation/state 应作为一个诊断 tuple 解读。

| 字段 | SQL 类型 | 单位/格式 | 空值或哨兵 | 含义 |
|---|---|---|---|---|
| `target_type` | `text` | — | 枚举 unknown、空串或 NULL 均按本行说明解释 | PITR target 的类型。 |
| `target_action` | `text` | — | 枚举 unknown、空串或 NULL 均按本行说明解释 | 到达 target 后执行 pause、promote 或 shutdown。 |
| `reachable` | `boolean` | 布尔 | false 表示该条件当前不成立 | 当前 PITR target 是否能由已知证据解析并到达。 |
| `reason` | `text` | — | 枚举 unknown、空串或 NULL 均按本行说明解释 | 当前结论或拒绝的可读原因；空值表示没有附加原因。 |
| `resolved_scn` | `bigint` | SCN | 0 通常表示尚无可用 SCN；不得解释为已证明的业务水位 | target 可达时最终选择的 restore-point SCN。 |
| `restore_point_name` | `text` | — | 枚举 unknown、空串或 NULL 均按本行说明解释 | 关联的 restore point 名称。 |

示例：

```sql
SELECT * FROM pg_cluster_pitr_status LIMIT 50;
```
