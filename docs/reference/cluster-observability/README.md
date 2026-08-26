# PGRAC 集群系统视图与参数参考

本手册覆盖当前主干注册的 **28 个系统视图、248 个视图字段、250 个 `cluster.*` 参数、961 个静态状态键**，以及动态键族和全部公开计数器。

> 文档描述查询瞬间的运行状态。系统视图不是持久业务表；跨行读取不应被当作一个事务级原子快照。需要比较趋势时，请由采集器带采样时间保存结果。

## 阅读顺序

1. [读取模型、权限、刷新和哨兵值](01-reading-model.md)
2. [拓扑、成员关系与重配置视图](02-topology-membership-and-reconfiguration-views.md)
3. [Interconnect、锁与 Cache Fusion 视图](03-interconnect-locking-and-cache-views.md)
4. [ADG、备份与恢复视图](04-adg-backup-and-recovery-views.md)
5. [`pg_cluster_state` 完整键字典](05-pg-cluster-state-key-dictionary.md)
6. [`pg_stat_cluster_counters` 计数器字典](06-cluster-counter-dictionary.md)
7. [核心、formation 与生命周期参数](07-guc-core-formation-and-lifecycle.md)
8. [Interconnect、GES/GCS、锁与缓存参数](08-guc-interconnect-locking-and-cache.md)
9. [存储、WAL、SCN、Undo 与事务参数](09-guc-storage-wal-scn-and-transactions.md)
10. [恢复、ADG、备份与 fencing 参数](10-guc-recovery-adg-and-fencing.md)
11. [诊断与测试控制参数](11-guc-diagnostics-and-test-controls.md)
12. [运维查询、采样方法与查询成本](12-operations-and-query-cost.md)

## 快速自查

```sql
SELECT name, setting, unit, context, pending_restart
  FROM pg_settings
 WHERE name LIKE 'cluster.%'
 ORDER BY name;

SELECT category, key, value
  FROM pg_cluster_state
 ORDER BY category, key;
```
