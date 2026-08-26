# `pg_stat_cluster_counters` 计数器字典

该视图返回 `(name text, value bigint)`。注册表是固定名称集合；value 在查询时从原子计数器读取，
少数值会在查询入口从实际 owner 单向镜像。大部分计数在 postmaster/进程生命周期内累计。

## `pg_stat_cluster_counters`

| 字段 | SQL 类型 | 含义 |
|---|---|---|
| `name` | `text` | 编译期注册的稳定计数器名称。 |
| `value` | `bigint` | 查询时读取或同步镜像后的 64 位值；按下表区分累计事件、gauge 与时间戳。 |

当前注册表固定返回 33 行；计数器属于本进程或明确的共享 owner，不自动等于四节点总和。

| 计数器 | 类型 | 含义 | 使用方法 |
|---|---|---|---|
| `cluster.inject.armed_count` | gauge | 当前进程已武装的故障注入点数量；这是 gauge。 | 采集时间窗增量；若值下降，先判定是否发生进程或共享内存重建。 |
| `cluster.smgr.remote_invalidation_stub_call_count` | 累计事件/时间戳（按名称） | remote invalidation stub 被调用的累计次数。 | 采集时间窗增量；若值下降，先判定是否发生进程或共享内存重建。 |
| `cluster.qvotec.poll_cycle_count` | 累计事件/时间戳（按名称） | `qvotec` / 轮询 / `cycle` / 累计次数。 | 采集时间窗增量；若值下降，先判定是否发生进程或共享内存重建。 |
| `cluster.qvotec.quorum_loss_event_count` | 累计事件/时间戳（按名称） | `qvotec` / 多数派 / 丢失 / 事件 / 累计次数。 | 采集时间窗增量；若值下降，先判定是否发生进程或共享内存重建。 |
| `cluster.qvotec.collision_detect_event_count` | 累计事件/时间戳（按名称） | `qvotec` / 冲突 / `detect` / 事件 / 累计次数。 | 采集时间窗增量；若值下降，先判定是否发生进程或共享内存重建。 |
| `cluster.qvotec.disk_io_failure_count` | 累计事件/时间戳（按名称） | `qvotec` / 磁盘 / I/O / `failure` / 累计次数。 | 采集时间窗增量；若值下降，先判定是否发生进程或共享内存重建。 |
| `cluster.fence.freeze_broadcast_count` | 累计事件/时间戳（按名称） | 隔离 / 冻结 / 广播 / 累计次数。 | 采集时间窗增量；若值下降，先判定是否发生进程或共享内存重建。 |
| `cluster.fence.thaw_broadcast_count` | 累计事件/时间戳（按名称） | 隔离 / 解冻 / 广播 / 累计次数。 | 采集时间窗增量；若值下降，先判定是否发生进程或共享内存重建。 |
| `cluster.fence.self_fence_initiated_count` | 累计事件/时间戳（按名称） | 隔离 / 本节点 / 隔离 / 已发起 / 累计次数。 | 采集时间窗增量；若值下降，先判定是否发生进程或共享内存重建。 |
| `cluster.fence.freeze_signal_received_count` | 累计事件/时间戳（按名称） | 隔离 / 冻结 / 信号 / 已接收 / 累计次数。 | 采集时间窗增量；若值下降，先判定是否发生进程或共享内存重建。 |
| `cluster.page.source_selected_current` | 累计事件/时间戳（按名称） | 页面 / 来源 / `selected` / 当前。 | 采集时间窗增量；若值下降，先判定是否发生进程或共享内存重建。 |
| `cluster.page.source_selected_pi` | 累计事件/时间戳（按名称） | 页面 / 来源 / `selected` / `pi`。 | 采集时间窗增量；若值下降，先判定是否发生进程或共享内存重建。 |
| `cluster.page.source_selected_storage` | 累计事件/时间戳（按名称） | 页面 / 来源 / `selected` / 存储。 | 采集时间窗增量；若值下降，先判定是否发生进程或共享内存重建。 |
| `cluster.page.source_invalid` | 累计事件/时间戳（按名称） | 页面 / 来源 / 无效。 | 采集时间窗增量；若值下降，先判定是否发生进程或共享内存重建。 |
| `cluster.page.source_missing` | 累计事件/时间戳（按名称） | 页面 / 来源 / `missing`。 | 采集时间窗增量；若值下降，先判定是否发生进程或共享内存重建。 |
| `cluster.page.source_conflict` | 累计事件/时间戳（按名称） | 页面 / 来源 / `conflict`。 | 采集时间窗增量；若值下降，先判定是否发生进程或共享内存重建。 |
| `cluster.page.result_skip` | 累计事件/时间戳（按名称） | 页面 / `result` / `skip`。 | 采集时间窗增量；若值下降，先判定是否发生进程或共享内存重建。 |
| `cluster.page.apply_count` | 累计事件/时间戳（按名称） | 页面 / 应用 / 累计次数。 | 采集时间窗增量；若值下降，先判定是否发生进程或共享内存重建。 |
| `cluster.page.version_mismatch` | 累计事件/时间戳（按名称） | 页面 / 版本 / `mismatch`。 | 采集时间窗增量；若值下降，先判定是否发生进程或共享内存重建。 |
| `cluster.page.unknown_class_blocked` | 累计事件/时间戳（按名称） | 页面 / 未知 / `class` / 阻塞。 | 采集时间窗增量；若值下降，先判定是否发生进程或共享内存重建。 |
| `cluster.page.authority_stale_rejected` | 累计事件/时间戳（按名称） | 页面 / `authority` / 陈旧 / 拒绝。 | 采集时间窗增量；若值下降，先判定是否发生进程或共享内存重建。 |
| `cluster.page.resource_early_release` | 累计事件/时间戳（按名称） | 页面 / `resource` / `early` / `release`。 | 采集时间窗增量；若值下降，先判定是否发生进程或共享内存重建。 |
| `cluster.page.retire_denied` | 累计事件/时间戳（按名称） | 页面 / `retire` / `denied`。 | 采集时间窗增量；若值下降，先判定是否发生进程或共享内存重建。 |
| `cluster.page.d3_rebuild` | 累计事件/时间戳（按名称） | 页面 / `d3` / `rebuild`。 | 采集时间窗增量；若值下降，先判定是否发生进程或共享内存重建。 |
| `cluster.page.d3_optimization_hit` | 累计事件/时间戳（按名称） | 页面 / `d3` / `optimization` / `hit`。 | 采集时间窗增量；若值下降，先判定是否发生进程或共享内存重建。 |
| `cluster.page.stable_base_unresolved` | 累计事件/时间戳（按名称） | 页面 / `stable` / `base` / `unresolved`。 | 采集时间窗增量；若值下降，先判定是否发生进程或共享内存重建。 |
| `cluster.page.contributor_records` | gauge | 页面 / `contributor` / `records`。 | 采集时间窗增量；若值下降，先判定是否发生进程或共享内存重建。 |
| `cluster.page.contributor_threads` | gauge | 页面 / `contributor` / `threads`。 | 采集时间窗增量；若值下降，先判定是否发生进程或共享内存重建。 |
| `cluster.page.contributor_gaps` | gauge | 页面 / `contributor` / `gaps`。 | 采集时间窗增量；若值下降，先判定是否发生进程或共享内存重建。 |
| `cluster.page.retained_pinned_bytes` | gauge | 页面 / `retained` / `pinned` / 字节。 | 采集时间窗增量；若值下降，先判定是否发生进程或共享内存重建。 |
| `cluster.page.last_page_write_ts` | 时间戳编码 | 页面 / 最近 / 页面 / 写入 / `ts`。 | 与当前时间比较新鲜度；0 表示尚未发布。 |
| `cluster.page.last_durability_barrier_ts` | 时间戳编码 | 页面 / 最近 / `durability` / 屏障 / `ts`。 | 与当前时间比较新鲜度；0 表示尚未发布。 |
| `cluster.page.last_post_read_ts` | 时间戳编码 | 页面 / 最近 / `post` / 读取 / `ts`。 | 与当前时间比较新鲜度；0 表示尚未发布。 |

示例：

```sql
SELECT name, value
  FROM pg_stat_cluster_counters
 ORDER BY name;
```
