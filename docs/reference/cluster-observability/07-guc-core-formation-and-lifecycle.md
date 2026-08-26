# 核心、formation 与生命周期参数

每个条目均来自当前 `cluster.*` 注册表。`postmaster` 表示修改后必须重启；`sighup` 表示 reload 生效；
`superuser`/`user` 表示可以在允许的 SQL 配置范围内修改。实际当前值请以 `pg_settings` 为准。

## `cluster.node_id`

| 属性 | 值 |
|---|---|
| 类型 | `int` |
| 默认值 | `-1` |
| 范围/枚举 | -1 .. 127 |
| Context | `postmaster` |
| 生效方式 | 修改后重启实例 |
| 单位 | — |
| 内置短说明（与 `pg_settings.short_desc` 对应） | Numeric identifier of this node in the cluster. |

工作原理与调节影响：设置 节点 / `id`。具体行为以本参数内置说明及关联状态视图为准。

依赖与保护：无额外配置钩子；组合约束仍可能在启动或运行入口检查。

观察入口：`pg_settings；必要时结合 pg_cluster_state`。

```sql
SELECT name, setting, unit, context, source, pending_restart FROM pg_settings WHERE name = 'cluster.node_id';
```

## `cluster.config_file`

| 属性 | 值 |
|---|---|
| 类型 | `string` |
| 默认值 | `pgrac.conf` |
| 范围/枚举 | 由 check hook 与消费者校验 |
| Context | `postmaster` |
| 生效方式 | 修改后重启实例 |
| 单位 | — |
| 内置短说明（与 `pg_settings.short_desc` 对应） | Path to the pgrac cluster topology configuration file. |

工作原理与调节影响：设置 `config` / `file`。具体行为以本参数内置说明及关联状态视图为准。

依赖与保护：无额外配置钩子；组合约束仍可能在启动或运行入口检查。

观察入口：`pg_settings；必要时结合 pg_cluster_state`。

```sql
SELECT name, setting, unit, context, source, pending_restart FROM pg_settings WHERE name = 'cluster.config_file';
```

## `cluster.apply_master_election`

| 属性 | 值 |
|---|---|
| 类型 | `bool` |
| 默认值 | `true` |
| 范围/枚举 | on / off |
| Context | `postmaster` |
| 生效方式 | 修改后重启实例 |
| 单位 | — |
| 内置短说明（与 `pg_settings.short_desc` 对应） | Enable automatic ADG Apply Master election. |

工作原理与调节影响：开关 应用 / master / `election` 路径。改变值只影响该开关明确覆盖的路径；安全拒绝条件不因关闭诊断开关而消失。

依赖与保护：无额外配置钩子；组合约束仍可能在启动或运行入口检查。

观察入口：`pg_settings；必要时结合 pg_cluster_state`。

```sql
SELECT name, setting, unit, context, source, pending_restart FROM pg_settings WHERE name = 'cluster.apply_master_election';
```

## `cluster.apply_master_switch_drain_ms`

| 属性 | 值 |
|---|---|
| 类型 | `int` |
| 默认值 | `5000` |
| 范围/枚举 | 0 .. 600000 |
| Context | `sighup` |
| 生效方式 | 修改配置并 reload |
| 单位 | 毫秒 |
| 内置短说明（与 `pg_settings.short_desc` 对应） | ADG Apply Master switch drain window. |

工作原理与调节影响：设置 应用 / master / `switch` / 排空 / `ms`。具体行为以本参数内置说明及关联状态视图为准。

依赖与保护：无额外配置钩子；组合约束仍可能在启动或运行入口检查。

观察入口：`pg_settings；必要时结合 pg_cluster_state`。

```sql
SELECT name, setting, unit, context, source, pending_restart FROM pg_settings WHERE name = 'cluster.apply_master_switch_drain_ms';
```

## `cluster.multi_xmax_remote_resolve`

| 属性 | 值 |
|---|---|
| 类型 | `bool` |
| 默认值 | `true` |
| 范围/枚举 | on / off |
| Context | `sighup` |
| 生效方式 | 修改配置并 reload |
| 单位 | — |
| 内置短说明（与 `pg_settings.short_desc` 对应） | Resolve foreign multixact xmax through the cluster member overlay (当前实现). |

工作原理与调节影响：开关 `multi` / `xmax` / `remote` / `resolve` 路径。改变值只影响该开关明确覆盖的路径；安全拒绝条件不因关闭诊断开关而消失。

依赖与保护：无额外配置钩子；组合约束仍可能在启动或运行入口检查。

观察入口：`pg_settings；必要时结合 pg_cluster_state`。

```sql
SELECT name, setting, unit, context, source, pending_restart FROM pg_settings WHERE name = 'cluster.multi_xmax_remote_resolve';
```

## `cluster.block_self_contained`

| 属性 | 值 |
|---|---|
| 类型 | `bool` |
| 默认值 | `false` |
| 范围/枚举 | on / off |
| Context | `superuser` |
| 生效方式 | 超级用户可在会话/系统范围修改 |
| 单位 | — |
| 内置短说明（与 `pg_settings.short_desc` 对应） | Deprecated compatibility setting; changing it has no effect. |

工作原理与调节影响：开关 数据块 / 本节点 / `contained` 路径。改变值只影响该开关明确覆盖的路径；安全拒绝条件不因关闭诊断开关而消失。

依赖与保护：兼容参数；on/off 在当前实现中行为相同。

观察入口：`pg_settings；必要时结合 pg_cluster_state`。

```sql
SELECT name, setting, unit, context, source, pending_restart FROM pg_settings WHERE name = 'cluster.block_self_contained';
```

## `cluster.controlfile_shared_authority`

| 属性 | 值 |
|---|---|
| 类型 | `bool` |
| 默认值 | `false` |
| 范围/枚举 | on / off |
| Context | `postmaster` |
| 生效方式 | 修改后重启实例 |
| 单位 | — |
| 内置短说明（与 `pg_settings.short_desc` 对应） | Use a single shared pg_control authority under cluster.shared_data_dir. |

工作原理与调节影响：开关 `controlfile` / 共享 / `authority` 路径。改变值只影响该开关明确覆盖的路径；安全拒绝条件不因关闭诊断开关而消失。

依赖与保护：无额外配置钩子；组合约束仍可能在启动或运行入口检查。

观察入口：`pg_settings；必要时结合 pg_cluster_state`。

```sql
SELECT name, setting, unit, context, source, pending_restart FROM pg_settings WHERE name = 'cluster.controlfile_shared_authority';
```

## `cluster.shmem_max_regions`

| 属性 | 值 |
|---|---|
| 类型 | `int` |
| 默认值 | `96` |
| 范围/枚举 | 40（启用 injection 的构建为 41） .. 256 |
| Context | `postmaster` |
| 生效方式 | 修改后重启实例 |
| 单位 | — |
| 内置短说明（与 `pg_settings.short_desc` 对应） | Capacity of the pgrac cluster shmem region registry. |

工作原理与调节影响：限定 `shmem` / 上限 / `regions` 的容量或并发。调大可减少容量型拒绝但增加共享内存/进程/队列占用；调小节省资源但更容易触发回退或 fail-closed。

依赖与保护：无额外配置钩子；组合约束仍可能在启动或运行入口检查。

观察入口：`pg_settings；必要时结合 pg_cluster_state`。

```sql
SELECT name, setting, unit, context, source, pending_restart FROM pg_settings WHERE name = 'cluster.shmem_max_regions';
```

## `cluster.tm_convert_mode`

| 属性 | 值 |
|---|---|
| 类型 | `enum` |
| 默认值 | `convert` |
| 范围/枚举 | convert, additive |
| Context | `sighup` |
| 生效方式 | 修改配置并 reload |
| 单位 | — |
| 内置短说明（与 `pg_settings.short_desc` 对应） | How a same-backend TM table-lock upgrade is routed across nodes. |

工作原理与调节影响：选择 `tm` / `convert` / 模式 的运行策略。枚举值改变的是路径/策略，不应按数值大小比较。

依赖与保护：无额外配置钩子；组合约束仍可能在启动或运行入口检查。

观察入口：`pg_settings；必要时结合 pg_cluster_state`。

```sql
SELECT name, setting, unit, context, source, pending_restart FROM pg_settings WHERE name = 'cluster.tm_convert_mode';
```

## `cluster.hw_remaster_retry_backoff_ms`

| 属性 | 值 |
|---|---|
| 类型 | `int` |
| 默认值 | `1000` |
| 范围/枚举 | 100 .. 60000 |
| Context | `sighup` |
| 生效方式 | 修改配置并 reload |
| 单位 | 毫秒 |
| 内置短说明（与 `pg_settings.short_desc` 对应） | Initial backoff before retrying a BLOCKED HW remaster worker (ms). |

工作原理与调节影响：定义 `hw` / remaster / 重试 / `backoff` / `ms` 的运行周期。调小反应更快但增加 CPU、网络或 I/O 频率；调大降低开销但延长发现/处理延迟。

依赖与保护：无额外配置钩子；组合约束仍可能在启动或运行入口检查。

观察入口：`pg_settings；必要时结合 pg_cluster_state`。

```sql
SELECT name, setting, unit, context, source, pending_restart FROM pg_settings WHERE name = 'cluster.hw_remaster_retry_backoff_ms';
```

## `cluster.hw_remaster_retry_max_attempts`

| 属性 | 值 |
|---|---|
| 类型 | `int` |
| 默认值 | `16` |
| 范围/枚举 | 0 .. 1000 |
| Context | `sighup` |
| 生效方式 | 修改配置并 reload |
| 单位 | — |
| 内置短说明（与 `pg_settings.short_desc` 对应） | Maximum same-episode retries for a BLOCKED HW remaster worker. |

工作原理与调节影响：限定 `hw` / remaster / 重试 / 上限 / `attempts` 的容量或并发。调大可减少容量型拒绝但增加共享内存/进程/队列占用；调小节省资源但更容易触发回退或 fail-closed。

依赖与保护：无额外配置钩子；组合约束仍可能在启动或运行入口检查。

观察入口：`pg_settings；必要时结合 pg_cluster_state`。

```sql
SELECT name, setting, unit, context, source, pending_restart FROM pg_settings WHERE name = 'cluster.hw_remaster_retry_max_attempts';
```

## `cluster.phase1_timeout`

| 属性 | 值 |
|---|---|
| 类型 | `int` |
| 默认值 | `60` |
| 范围/枚举 | 1 .. 3600 |
| Context | `postmaster` |
| 生效方式 | 修改后重启实例 |
| 单位 | 秒 |
| 内置短说明（与 `pg_settings.short_desc` 对应） | Phase 1 (cluster basics) transition timeout in seconds. |

工作原理与调节影响：定义 `phase1` / 超时 的有界等待。调大可容忍更慢的 peer/I/O，但延长故障收敛；调小更快 fail-closed，也更容易把短抖动判为失败。

依赖与保护：无额外配置钩子；组合约束仍可能在启动或运行入口检查。

观察入口：`pg_settings；必要时结合 pg_cluster_state`。

```sql
SELECT name, setting, unit, context, source, pending_restart FROM pg_settings WHERE name = 'cluster.phase1_timeout';
```

## `cluster.phase2_timeout`

| 属性 | 值 |
|---|---|
| 类型 | `int` |
| 默认值 | `30` |
| 范围/枚举 | 1 .. 3600 |
| Context | `postmaster` |
| 生效方式 | 修改后重启实例 |
| 单位 | 秒 |
| 内置短说明（与 `pg_settings.short_desc` 对应） | Phase 2 (lock services) transition timeout in seconds. |

工作原理与调节影响：定义 `phase2` / 超时 的有界等待。调大可容忍更慢的 peer/I/O，但延长故障收敛；调小更快 fail-closed，也更容易把短抖动判为失败。

依赖与保护：无额外配置钩子；组合约束仍可能在启动或运行入口检查。

观察入口：`pg_settings；必要时结合 pg_cluster_state`。

```sql
SELECT name, setting, unit, context, source, pending_restart FROM pg_settings WHERE name = 'cluster.phase2_timeout';
```

## `cluster.phase3_timeout`

| 属性 | 值 |
|---|---|
| 类型 | `int` |
| 默认值 | `600` |
| 范围/枚举 | 60 .. 3600 |
| Context | `postmaster` |
| 生效方式 | 修改后重启实例 |
| 单位 | 秒 |
| 内置短说明（与 `pg_settings.short_desc` 对应） | Phase 3 (recovery) transition timeout in seconds. |

工作原理与调节影响：定义 `phase3` / 超时 的有界等待。调大可容忍更慢的 peer/I/O，但延长故障收敛；调小更快 fail-closed，也更容易把短抖动判为失败。

依赖与保护：无额外配置钩子；组合约束仍可能在启动或运行入口检查。

观察入口：`pg_settings；必要时结合 pg_cluster_state`。

```sql
SELECT name, setting, unit, context, source, pending_restart FROM pg_settings WHERE name = 'cluster.phase3_timeout';
```

## `cluster.phase4_timeout`

| 属性 | 值 |
|---|---|
| 类型 | `int` |
| 默认值 | `30` |
| 范围/枚举 | 1 .. 3600 |
| Context | `postmaster` |
| 生效方式 | 修改后重启实例 |
| 单位 | 秒 |
| 内置短说明（与 `pg_settings.short_desc` 对应） | Phase 4 (normal startup) transition timeout in seconds. |

工作原理与调节影响：定义 `phase4` / 超时 的有界等待。调大可容忍更慢的 peer/I/O，但延长故障收敛；调小更快 fail-closed，也更容易把短抖动判为失败。

依赖与保护：无额外配置钩子；组合约束仍可能在启动或运行入口检查。

观察入口：`pg_settings；必要时结合 pg_cluster_state`。

```sql
SELECT name, setting, unit, context, source, pending_restart FROM pg_settings WHERE name = 'cluster.phase4_timeout';
```

## `cluster.lmon_main_loop_interval`

| 属性 | 值 |
|---|---|
| 类型 | `int` |
| 默认值 | `1000` |
| 范围/枚举 | 100 .. 60000 |
| Context | `sighup` |
| 生效方式 | 修改配置并 reload |
| 单位 | 毫秒 |
| 内置短说明（与 `pg_settings.short_desc` 对应） | LMON main-loop tick interval in milliseconds. |

工作原理与调节影响：定义 `lmon` / `main` / `loop` / `interval` 的运行周期。调小反应更快但增加 CPU、网络或 I/O 频率；调大降低开销但延长发现/处理延迟。

依赖与保护：无额外配置钩子；组合约束仍可能在启动或运行入口检查。

观察入口：`pg_settings；必要时结合 pg_cluster_state`。

```sql
SELECT name, setting, unit, context, source, pending_restart FROM pg_settings WHERE name = 'cluster.lmon_main_loop_interval';
```

## `cluster.lmon_slow_iteration_warn_ms`

| 属性 | 值 |
|---|---|
| 类型 | `int` |
| 默认值 | `1000` |
| 范围/枚举 | 0 .. 60000 |
| Context | `sighup` |
| 生效方式 | 修改配置并 reload |
| 单位 | 毫秒 |
| 内置短说明（与 `pg_settings.short_desc` 对应） | LMON main-loop slow-iteration warning threshold in milliseconds. |

工作原理与调节影响：设置 `lmon` / `slow` / `iteration` / `warn` / `ms`。具体行为以本参数内置说明及关联状态视图为准。

依赖与保护：无额外配置钩子；组合约束仍可能在启动或运行入口检查。

观察入口：`pg_settings；必要时结合 pg_cluster_state`。

```sql
SELECT name, setting, unit, context, source, pending_restart FROM pg_settings WHERE name = 'cluster.lmon_slow_iteration_warn_ms';
```

## `cluster.lck_main_loop_interval`

| 属性 | 值 |
|---|---|
| 类型 | `int` |
| 默认值 | `1000` |
| 范围/枚举 | 100 .. 60000 |
| Context | `sighup` |
| 生效方式 | 修改配置并 reload |
| 单位 | 毫秒 |
| 内置短说明（与 `pg_settings.short_desc` 对应） | LCK main-loop tick interval in milliseconds. |

工作原理与调节影响：定义 `lck` / `main` / `loop` / `interval` 的运行周期。调小反应更快但增加 CPU、网络或 I/O 频率；调大降低开销但延长发现/处理延迟。

依赖与保护：无额外配置钩子；组合约束仍可能在启动或运行入口检查。

观察入口：`pg_settings；必要时结合 pg_cluster_state`。

```sql
SELECT name, setting, unit, context, source, pending_restart FROM pg_settings WHERE name = 'cluster.lck_main_loop_interval';
```

## `cluster.diag_main_loop_interval`

| 属性 | 值 |
|---|---|
| 类型 | `int` |
| 默认值 | `1000` |
| 范围/枚举 | 100 .. 60000 |
| Context | `sighup` |
| 生效方式 | 修改配置并 reload |
| 单位 | 毫秒 |
| 内置短说明（与 `pg_settings.short_desc` 对应） | DIAG main-loop tick interval in milliseconds. |

工作原理与调节影响：定义 `diag` / `main` / `loop` / `interval` 的运行周期。调小反应更快但增加 CPU、网络或 I/O 频率；调大降低开销但延长发现/处理延迟。

依赖与保护：无额外配置钩子；组合约束仍可能在启动或运行入口检查。

观察入口：`pg_settings；必要时结合 pg_cluster_state`。

```sql
SELECT name, setting, unit, context, source, pending_restart FROM pg_settings WHERE name = 'cluster.diag_main_loop_interval';
```

## `cluster.cluster_stats_main_loop_interval`

| 属性 | 值 |
|---|---|
| 类型 | `int` |
| 默认值 | `1000` |
| 范围/枚举 | 100 .. 60000 |
| Context | `sighup` |
| 生效方式 | 修改配置并 reload |
| 单位 | 毫秒 |
| 内置短说明（与 `pg_settings.short_desc` 对应） | Cluster Stats main-loop tick interval in milliseconds. |

工作原理与调节影响：定义 `cluster` / `stats` / `main` / `loop` / `interval` 的运行周期。调小反应更快但增加 CPU、网络或 I/O 频率；调大降低开销但延长发现/处理延迟。

依赖与保护：无额外配置钩子；组合约束仍可能在启动或运行入口检查。

观察入口：`pg_settings；必要时结合 pg_cluster_state`。

```sql
SELECT name, setting, unit, context, source, pending_restart FROM pg_settings WHERE name = 'cluster.cluster_stats_main_loop_interval';
```

## `cluster.cssd_main_loop_interval_ms`

| 属性 | 值 |
|---|---|
| 类型 | `int` |
| 默认值 | `1000` |
| 范围/枚举 | 100 .. 60000 |
| Context | `postmaster` |
| 生效方式 | 修改后重启实例 |
| 单位 | 毫秒 |
| 内置短说明（与 `pg_settings.short_desc` 对应） | CSSD aux process main-loop tick interval in milliseconds. |

工作原理与调节影响：定义 `cssd` / `main` / `loop` / `interval` / `ms` 的运行周期。调小反应更快但增加 CPU、网络或 I/O 频率；调大降低开销但延长发现/处理延迟。

依赖与保护：无额外配置钩子；组合约束仍可能在启动或运行入口检查。

观察入口：`membership/CSSD/quorum/reconfig 相关结构化视图`。

```sql
SELECT name, setting, unit, context, source, pending_restart FROM pg_settings WHERE name = 'cluster.cssd_main_loop_interval_ms';
```

## `cluster.cssd_heartbeat_interval_ms`

| 属性 | 值 |
|---|---|
| 类型 | `int` |
| 默认值 | `1000` |
| 范围/枚举 | 100 .. 10000 |
| Context | `postmaster` |
| 生效方式 | 修改后重启实例 |
| 单位 | 毫秒 |
| 内置短说明（与 `pg_settings.short_desc` 对应） | CSSD heartbeat broadcast period in milliseconds. |

工作原理与调节影响：定义 `cssd` / 心跳 / `interval` / `ms` 的运行周期。调小反应更快但增加 CPU、网络或 I/O 频率；调大降低开销但延长发现/处理延迟。

依赖与保护：无额外配置钩子；组合约束仍可能在启动或运行入口检查。

观察入口：`membership/CSSD/quorum/reconfig 相关结构化视图`。

```sql
SELECT name, setting, unit, context, source, pending_restart FROM pg_settings WHERE name = 'cluster.cssd_heartbeat_interval_ms';
```

## `cluster.cssd_dead_deadband_factor`

| 属性 | 值 |
|---|---|
| 类型 | `int` |
| 默认值 | `3` |
| 范围/枚举 | 2 .. 10 |
| Context | `postmaster` |
| 生效方式 | 修改后重启实例 |
| 单位 | — |
| 内置短说明（与 `pg_settings.short_desc` 对应） | CSSD dead-detection deadband as a multiple of heartbeat interval. |

工作原理与调节影响：设置 `cssd` / 失效 / `deadband` / `factor`。具体行为以本参数内置说明及关联状态视图为准。

依赖与保护：无额外配置钩子；组合约束仍可能在启动或运行入口检查。

观察入口：`membership/CSSD/quorum/reconfig 相关结构化视图`。

```sql
SELECT name, setting, unit, context, source, pending_restart FROM pg_settings WHERE name = 'cluster.cssd_dead_deadband_factor';
```

## `cluster.voting_disks`

| 属性 | 值 |
|---|---|
| 类型 | `string` |
| 默认值 | `空值` |
| 范围/枚举 | 由 check hook 与消费者校验 |
| Context | `postmaster` |
| 生效方式 | 修改后重启实例 |
| 单位 | — |
| 内置短说明（与 `pg_settings.short_desc` 对应） | Comma-separated list of voting disk file paths. |

工作原理与调节影响：设置 `voting` / 磁盘。具体行为以本参数内置说明及关联状态视图为准。

依赖与保护：无额外配置钩子；组合约束仍可能在启动或运行入口检查。

观察入口：`membership/CSSD/quorum/reconfig 相关结构化视图`。

```sql
SELECT name, setting, unit, context, source, pending_restart FROM pg_settings WHERE name = 'cluster.voting_disks';
```

## `cluster.quorum_poll_interval_ms`

| 属性 | 值 |
|---|---|
| 类型 | `int` |
| 默认值 | `2000` |
| 范围/枚举 | 500 .. 30000 |
| Context | `postmaster` |
| 生效方式 | 修改后重启实例 |
| 单位 | 毫秒 |
| 内置短说明（与 `pg_settings.short_desc` 对应） | Quorum voting disk poll period in milliseconds. |

工作原理与调节影响：定义 多数派 / 轮询 / `interval` / `ms` 的运行周期。调小反应更快但增加 CPU、网络或 I/O 频率；调大降低开销但延长发现/处理延迟。

依赖与保护：无额外配置钩子；组合约束仍可能在启动或运行入口检查。

观察入口：`membership/CSSD/quorum/reconfig 相关结构化视图`。

```sql
SELECT name, setting, unit, context, source, pending_restart FROM pg_settings WHERE name = 'cluster.quorum_poll_interval_ms';
```

## `cluster.voting_disk_io_timeout_ms`

| 属性 | 值 |
|---|---|
| 类型 | `int` |
| 默认值 | `5000` |
| 范围/枚举 | 500 .. 60000 |
| Context | `postmaster` |
| 生效方式 | 修改后重启实例 |
| 单位 | 毫秒 |
| 内置短说明（与 `pg_settings.short_desc` 对应） | Voting disk single I/O timeout in milliseconds. |

工作原理与调节影响：定义 `voting` / 磁盘 / I/O / 超时 / `ms` 的有界等待。调大可容忍更慢的 peer/I/O，但延长故障收敛；调小更快 fail-closed，也更容易把短抖动判为失败。

依赖与保护：无额外配置钩子；组合约束仍可能在启动或运行入口检查。

观察入口：`membership/CSSD/quorum/reconfig 相关结构化视图`。

```sql
SELECT name, setting, unit, context, source, pending_restart FROM pg_settings WHERE name = 'cluster.voting_disk_io_timeout_ms';
```

## `cluster.voting_disk_size_bytes`

| 属性 | 值 |
|---|---|
| 类型 | `int` |
| 默认值 | `394240` |
| 范围/枚举 | 4096 .. 1048576 |
| Context | `postmaster` |
| 生效方式 | 修改后重启实例 |
| 单位 | 字节 |
| 内置短说明（与 `pg_settings.short_desc` 对应） | Voting disk file size in bytes. |

工作原理与调节影响：设置 `voting` / 磁盘 / 大小 / 字节。具体行为以本参数内置说明及关联状态视图为准。

依赖与保护：无额外配置钩子；组合约束仍可能在启动或运行入口检查。

观察入口：`membership/CSSD/quorum/reconfig 相关结构化视图`。

```sql
SELECT name, setting, unit, context, source, pending_restart FROM pg_settings WHERE name = 'cluster.voting_disk_size_bytes';
```

## `cluster.online_join`

| 属性 | 值 |
|---|---|
| 类型 | `bool` |
| 默认值 | `false` |
| 范围/枚举 | on / off |
| Context | `postmaster` |
| 生效方式 | 修改后重启实例 |
| 单位 | — |
| 内置短说明（与 `pg_settings.short_desc` 对应） | Allow a declared node to join/rejoin live membership online (without a full cluster restart). |

工作原理与调节影响：开关 `online` / 加入 路径。改变值只影响该开关明确覆盖的路径；安全拒绝条件不因关闭诊断开关而消失。

依赖与保护：无额外配置钩子；组合约束仍可能在启动或运行入口检查。

观察入口：`membership/CSSD/quorum/reconfig 相关结构化视图`。

```sql
SELECT name, setting, unit, context, source, pending_restart FROM pg_settings WHERE name = 'cluster.online_join';
```

## `cluster.join_remaster_enabled`

| 属性 | 值 |
|---|---|
| 类型 | `bool` |
| 默认值 | `false` |
| 范围/枚举 | on / off |
| Context | `postmaster` |
| 生效方式 | 修改后重启实例 |
| 单位 | — |
| 内置短说明（与 `pg_settings.short_desc` 对应） | On node rejoin, move the joiner's home-shard GES mastership back from the survivor (optional rebalance). |

工作原理与调节影响：开关 加入 / remaster / 启用状态 路径。改变值只影响该开关明确覆盖的路径；安全拒绝条件不因关闭诊断开关而消失。

依赖与保护：无额外配置钩子；组合约束仍可能在启动或运行入口检查。

观察入口：`membership/CSSD/quorum/reconfig 相关结构化视图`。

```sql
SELECT name, setting, unit, context, source, pending_restart FROM pg_settings WHERE name = 'cluster.join_remaster_enabled';
```

## `cluster.join_convergence_timeout_ms`

| 属性 | 值 |
|---|---|
| 类型 | `int` |
| 默认值 | `30000` |
| 范围/枚举 | 5000 .. 120000 |
| Context | `sighup` |
| 生效方式 | 修改配置并 reload |
| 单位 | 毫秒 |
| 内置短说明（与 `pg_settings.short_desc` 对应） | Deadline for an online join to converge + commit. |

工作原理与调节影响：定义 加入 / `convergence` / 超时 / `ms` 的有界等待。调大可容忍更慢的 peer/I/O，但延长故障收敛；调小更快 fail-closed，也更容易把短抖动判为失败。

依赖与保护：无额外配置钩子；组合约束仍可能在启动或运行入口检查。

观察入口：`membership/CSSD/quorum/reconfig 相关结构化视图`。

```sql
SELECT name, setting, unit, context, source, pending_restart FROM pg_settings WHERE name = 'cluster.join_convergence_timeout_ms';
```

## `cluster.online_node_removal`

| 属性 | 值 |
|---|---|
| 类型 | `bool` |
| 默认值 | `false` |
| 范围/枚举 | on / off |
| Context | `postmaster` |
| 生效方式 | 修改后重启实例 |
| 单位 | — |
| 内置短说明（与 `pg_settings.short_desc` 对应） | Enable permanent removal (decommission) of a declared node. |

工作原理与调节影响：开关 `online` / 节点 / `removal` 路径。改变值只影响该开关明确覆盖的路径；安全拒绝条件不因关闭诊断开关而消失。

依赖与保护：无额外配置钩子；组合约束仍可能在启动或运行入口检查。

观察入口：`membership/CSSD/quorum/reconfig 相关结构化视图`。

```sql
SELECT name, setting, unit, context, source, pending_restart FROM pg_settings WHERE name = 'cluster.online_node_removal';
```

## `cluster.node_removal_cleanup_timeout_ms`

| 属性 | 值 |
|---|---|
| 类型 | `int` |
| 默认值 | `30000` |
| 范围/枚举 | 5000 .. 120000 |
| Context | `sighup` |
| 生效方式 | 修改配置并 reload |
| 单位 | 毫秒 |
| 内置短说明（与 `pg_settings.short_desc` 对应） | Deadline for the post-shrink cluster-wide removal cleanup. |

工作原理与调节影响：定义 节点 / `removal` / 清理 / 超时 / `ms` 的有界等待。调大可容忍更慢的 peer/I/O，但延长故障收敛；调小更快 fail-closed，也更容易把短抖动判为失败。

依赖与保护：无额外配置钩子；组合约束仍可能在启动或运行入口检查。

观察入口：`membership/CSSD/quorum/reconfig 相关结构化视图`。

```sql
SELECT name, setting, unit, context, source, pending_restart FROM pg_settings WHERE name = 'cluster.node_removal_cleanup_timeout_ms';
```

## `cluster.freeze_writes_enabled`

| 属性 | 值 |
|---|---|
| 类型 | `bool` |
| 默认值 | `true` |
| 范围/枚举 | on / off |
| Context | `postmaster` |
| 生效方式 | 修改后重启实例 |
| 单位 | — |
| 内置短说明（与 `pg_settings.short_desc` 对应） | Enable PROCSIG_CLUSTER_FREEZE_WRITES in-flight transaction abort. |

工作原理与调节影响：开关 冻结 / `writes` / 启用状态 路径。改变值只影响该开关明确覆盖的路径；安全拒绝条件不因关闭诊断开关而消失。

依赖与保护：无额外配置钩子；组合约束仍可能在启动或运行入口检查。

观察入口：`pg_settings；必要时结合 pg_cluster_state`。

```sql
SELECT name, setting, unit, context, source, pending_restart FROM pg_settings WHERE name = 'cluster.freeze_writes_enabled';
```

## `cluster.resolver_cache_enabled`

| 属性 | 值 |
|---|---|
| 类型 | `bool` |
| 默认值 | `false` |
| 范围/枚举 | on / off |
| Context | `postmaster` |
| 生效方式 | 修改后重启实例 |
| 单位 | — |
| 内置短说明（与 `pg_settings.short_desc` 对应） | Enable 当前实现 shared resolver cache TRUST mode (skip the by-xid scan on a re-validated + accepted hint). |

工作原理与调节影响：开关 `resolver` / 缓存 / 启用状态 路径。改变值只影响该开关明确覆盖的路径；安全拒绝条件不因关闭诊断开关而消失。

依赖与保护：无额外配置钩子；组合约束仍可能在启动或运行入口检查。

观察入口：`pg_settings；必要时结合 pg_cluster_state`。

```sql
SELECT name, setting, unit, context, source, pending_restart FROM pg_settings WHERE name = 'cluster.resolver_cache_enabled';
```

## `cluster.resolver_cache_measure`

| 属性 | 值 |
|---|---|
| 类型 | `bool` |
| 默认值 | `false` |
| 范围/枚举 | on / off |
| Context | `postmaster` |
| 生效方式 | 修改后重启实例 |
| 单位 | — |
| 内置短说明（与 `pg_settings.short_desc` 对应） | Enable 当前实现 shared resolver cache MEASURE mode (value gate, no trust). |

工作原理与调节影响：开关 `resolver` / 缓存 / `measure` 路径。改变值只影响该开关明确覆盖的路径；安全拒绝条件不因关闭诊断开关而消失。

依赖与保护：无额外配置钩子；组合约束仍可能在启动或运行入口检查。

观察入口：`pg_settings；必要时结合 pg_cluster_state`。

```sql
SELECT name, setting, unit, context, source, pending_restart FROM pg_settings WHERE name = 'cluster.resolver_cache_measure';
```

## `cluster.resolver_cache_entries`

| 属性 | 值 |
|---|---|
| 类型 | `int` |
| 默认值 | `0` |
| 范围/枚举 | 0 .. 1048576 |
| Context | `postmaster` |
| 生效方式 | 修改后重启实例 |
| 单位 | — |
| 内置短说明（与 `pg_settings.short_desc` 对应） | Shared resolver cache hint-slot count (0 = disabled / zero memory). |

工作原理与调节影响：限定 `resolver` / 缓存 / `entries` 的容量或并发。调大可减少容量型拒绝但增加共享内存/进程/队列占用；调小节省资源但更容易触发回退或 fail-closed。

依赖与保护：无额外配置钩子；组合约束仍可能在启动或运行入口检查。

观察入口：`pg_settings；必要时结合 pg_cluster_state`。

```sql
SELECT name, setting, unit, context, source, pending_restart FROM pg_settings WHERE name = 'cluster.resolver_cache_entries';
```

## `cluster.boc_sweep_interval_ms`

| 属性 | 值 |
|---|---|
| 类型 | `int` |
| 默认值 | `100` |
| 范围/枚举 | 1 .. 1000 |
| Context | `sighup` |
| 生效方式 | 修改配置并 reload |
| 单位 | 毫秒 |
| 内置短说明（与 `pg_settings.short_desc` 对应） | walwriter BOC sweep staleness target in milliseconds. |

工作原理与调节影响：定义 `boc` / `sweep` / `interval` / `ms` 的运行周期。调小反应更快但增加 CPU、网络或 I/O 频率；调大降低开销但延长发现/处理延迟。

依赖与保护：无额外配置钩子；组合约束仍可能在启动或运行入口检查。

观察入口：`pg_settings；必要时结合 pg_cluster_state`。

```sql
SELECT name, setting, unit, context, source, pending_restart FROM pg_settings WHERE name = 'cluster.boc_sweep_interval_ms';
```

## `cluster.boc_event_publish`

| 属性 | 值 |
|---|---|
| 类型 | `bool` |
| 默认值 | `true` |
| 范围/枚举 | on / off |
| Context | `sighup` |
| 生效方式 | 修改配置并 reload |
| 单位 | — |
| 内置短说明（与 `pg_settings.short_desc` 对应） | Publish the durable SCN frontier on commit events. |

工作原理与调节影响：开关 `boc` / 事件 / `publish` 路径。改变值只影响该开关明确覆盖的路径；安全拒绝条件不因关闭诊断开关而消失。

依赖与保护：无额外配置钩子；组合约束仍可能在启动或运行入口检查。

观察入口：`pg_settings；必要时结合 pg_cluster_state`。

```sql
SELECT name, setting, unit, context, source, pending_restart FROM pg_settings WHERE name = 'cluster.boc_event_publish';
```

## `cluster.enabled`

| 属性 | 值 |
|---|---|
| 类型 | `bool` |
| 默认值 | `true` |
| 范围/枚举 | on / off |
| Context | `postmaster` |
| 生效方式 | 修改后重启实例 |
| 单位 | — |
| 内置短说明（与 `pg_settings.short_desc` 对应） | Runtime cluster mode gate (当前版本 Sprint B HC4 闭环). |

工作原理与调节影响：开关 启用状态 路径。改变值只影响该开关明确覆盖的路径；安全拒绝条件不因关闭诊断开关而消失。

依赖与保护：无额外配置钩子；组合约束仍可能在启动或运行入口检查。

观察入口：`pg_settings；必要时结合 pg_cluster_state`。

```sql
SELECT name, setting, unit, context, source, pending_restart FROM pg_settings WHERE name = 'cluster.enabled';
```

## `cluster.allow_single_node`

| 属性 | 值 |
|---|---|
| 类型 | `bool` |
| 默认值 | `true` |
| 范围/枚举 | on / off |
| Context | `postmaster` |
| 生效方式 | 修改后重启实例 |
| 单位 | — |
| 内置短说明（与 `pg_settings.short_desc` 对应） | Allow pgrac to start in single-node mode (no pgrac.conf or invalid cluster.node_id). |

工作原理与调节影响：开关 `allow` / `single` / 节点 路径。改变值只影响该开关明确覆盖的路径；安全拒绝条件不因关闭诊断开关而消失。

依赖与保护：无额外配置钩子；组合约束仍可能在启动或运行入口检查。

观察入口：`pg_settings；必要时结合 pg_cluster_state`。

```sql
SELECT name, setting, unit, context, source, pending_restart FROM pg_settings WHERE name = 'cluster.allow_single_node';
```

## `cluster.advisory_lock_enabled`

| 属性 | 值 |
|---|---|
| 类型 | `bool` |
| 默认值 | `true` |
| 范围/枚举 | on / off |
| Context | `superuser` |
| 生效方式 | 超级用户可在会话/系统范围修改 |
| 单位 | — |
| 内置短说明（与 `pg_settings.short_desc` 对应） | Enable cross-node globalization of advisory (user) locks. |

工作原理与调节影响：开关 `advisory` / 锁 / 启用状态 路径。改变值只影响该开关明确覆盖的路径；安全拒绝条件不因关闭诊断开关而消失。

依赖与保护：无额外配置钩子；组合约束仍可能在启动或运行入口检查。

观察入口：`pg_settings；必要时结合 pg_cluster_state`。

```sql
SELECT name, setting, unit, context, source, pending_restart FROM pg_settings WHERE name = 'cluster.advisory_lock_enabled';
```

## `cluster.global_dd_interval_ms`

| 属性 | 值 |
|---|---|
| 类型 | `int` |
| 默认值 | `2000` |
| 范围/枚举 | 100 .. 600000 |
| Context | `sighup` |
| 生效方式 | 修改配置并 reload |
| 单位 | 毫秒（名称约定） |
| 内置短说明（与 `pg_settings.short_desc` 对应） | Coordinator cross-node deadlock scan period (ms). |

工作原理与调节影响：定义 `global` / `dd` / `interval` / `ms` 的运行周期。调小反应更快但增加 CPU、网络或 I/O 频率；调大降低开销但延长发现/处理延迟。

依赖与保护：无额外配置钩子；组合约束仍可能在启动或运行入口检查。

观察入口：`pg_settings；必要时结合 pg_cluster_state`。

```sql
SELECT name, setting, unit, context, source, pending_restart FROM pg_settings WHERE name = 'cluster.global_dd_interval_ms';
```

## `cluster.cancel_ack_timeout_ms`

| 属性 | 值 |
|---|---|
| 类型 | `int` |
| 默认值 | `1000` |
| 范围/枚举 | 50 .. 60000 |
| Context | `superuser` |
| 生效方式 | 超级用户可在会话/系统范围修改 |
| 单位 | 毫秒 |
| 内置短说明（与 `pg_settings.short_desc` 对应） | Coordinator wait for a cross-node deadlock CANCEL_ACK before retransmit (ms). |

工作原理与调节影响：定义 `cancel` / 确认 / 超时 / `ms` 的有界等待。调大可容忍更慢的 peer/I/O，但延长故障收敛；调小更快 fail-closed，也更容易把短抖动判为失败。

依赖与保护：无额外配置钩子；组合约束仍可能在启动或运行入口检查。

观察入口：`pg_settings；必要时结合 pg_cluster_state`。

```sql
SELECT name, setting, unit, context, source, pending_restart FROM pg_settings WHERE name = 'cluster.cancel_ack_timeout_ms';
```

## `cluster.cancel_max_retransmit`

| 属性 | 值 |
|---|---|
| 类型 | `int` |
| 默认值 | `3` |
| 范围/枚举 | 0 .. 100 |
| Context | `superuser` |
| 生效方式 | 超级用户可在会话/系统范围修改 |
| 单位 | — |
| 内置短说明（与 `pg_settings.short_desc` 对应） | Bounded cross-node deadlock cancel retransmit attempts before escalation. |

工作原理与调节影响：限定 `cancel` / 上限 / `retransmit` 的容量或并发。调大可减少容量型拒绝但增加共享内存/进程/队列占用；调小节省资源但更容易触发回退或 fail-closed。

依赖与保护：无额外配置钩子；组合约束仍可能在启动或运行入口检查。

观察入口：`pg_settings；必要时结合 pg_cluster_state`。

```sql
SELECT name, setting, unit, context, source, pending_restart FROM pg_settings WHERE name = 'cluster.cancel_max_retransmit';
```

## `cluster.victim_repeat_window_ms`

| 属性 | 值 |
|---|---|
| 类型 | `int` |
| 默认值 | `5000` |
| 范围/枚举 | 0 .. 600000 |
| Context | `superuser` |
| 生效方式 | 超级用户可在会话/系统范围修改 |
| 单位 | 毫秒 |
| 内置短说明（与 `pg_settings.short_desc` 对应） | Anti-thrash window for repeated deadlock victim selection (ms). |

工作原理与调节影响：设置 `victim` / `repeat` / `window` / `ms`。具体行为以本参数内置说明及关联状态视图为准。

依赖与保护：无额外配置钩子；组合约束仍可能在启动或运行入口检查。

观察入口：`pg_settings；必要时结合 pg_cluster_state`。

```sql
SELECT name, setting, unit, context, source, pending_restart FROM pg_settings WHERE name = 'cluster.victim_repeat_window_ms';
```

## `cluster.ic_duty_lazy`

| 属性 | 值 |
|---|---|
| 类型 | `bool` |
| 默认值 | `true` |
| 范围/枚举 | on / off |
| Context | `sighup` |
| 生效方式 | 修改配置并 reload |
| 单位 | — |
| 内置短说明（与 `pg_settings.short_desc` 对应） | Run lazy-able LMON duty-chain drains on demand instead of every iteration. |

工作原理与调节影响：开关 `ic` / `duty` / `lazy` 路径。改变值只影响该开关明确覆盖的路径；安全拒绝条件不因关闭诊断开关而消失。

依赖与保护：无额外配置钩子；组合约束仍可能在启动或运行入口检查。

观察入口：`pg_stat_cluster_ic / pg_cluster_ic_peers / pg_cluster_state(category='ic')`。

```sql
SELECT name, setting, unit, context, source, pending_restart FROM pg_settings WHERE name = 'cluster.ic_duty_lazy';
```
