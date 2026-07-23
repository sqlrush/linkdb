/*-------------------------------------------------------------------------
 *
 * cluster_multixact_current_stats.c
 *	  Shared observability for current-DML MultiXact authority.
 *
 * Portions Copyright (c) 1996-2024, PostgreSQL Global Development Group
 * Portions Copyright (c) 2026, pgrac contributors
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "miscadmin.h"
#include "port/atomics.h"
#include "storage/shmem.h"

#include "cluster/cluster_guc.h"
#include "cluster/cluster_multixact_current_stats.h"
#include "cluster/cluster_shmem.h"

#ifdef USE_PGRAC_CLUSTER

typedef struct ClusterCurrentMxStatsShmem {
	pg_atomic_uint64 counters[CMX_STAT_COUNT];
} ClusterCurrentMxStatsShmem;

static ClusterCurrentMxStatsShmem *ClusterCurrentMxStats;

Size
cluster_multixact_current_stats_shmem_size(void)
{
	if (IsBootstrapProcessingMode() || !cluster_enabled || cluster_node_id < 0)
		return 0;
	return MAXALIGN(sizeof(ClusterCurrentMxStatsShmem));
}

void
cluster_multixact_current_stats_shmem_init(void)
{
	bool found;
	int i;

	if (IsBootstrapProcessingMode() || !cluster_enabled || cluster_node_id < 0)
		return;
	ClusterCurrentMxStats = ShmemInitStruct("pgrac current multixact stats",
										   sizeof(ClusterCurrentMxStatsShmem), &found);
	if (!found)
		for (i = 0; i < CMX_STAT_COUNT; i++)
			pg_atomic_init_u64(&ClusterCurrentMxStats->counters[i], 0);
}

static const ClusterShmemRegion cluster_multixact_current_stats_region = {
	.name = "pgrac current multixact stats",
	.size_fn = cluster_multixact_current_stats_shmem_size,
	.init_fn = cluster_multixact_current_stats_shmem_init,
	.lwlock_count = 0,
	.owner_subsys = "cluster_multixact_current",
	.reserved_flags = 0,
};

void
cluster_multixact_current_stats_shmem_register(void)
{
	cluster_shmem_register_region(&cluster_multixact_current_stats_region);
}

void
cluster_multixact_current_stats_bump(ClusterCurrentMxStatId stat)
{
	if (ClusterCurrentMxStats != NULL && stat >= 0 && stat < CMX_STAT_COUNT)
		pg_atomic_fetch_add_u64(&ClusterCurrentMxStats->counters[stat], 1);
}

uint64
cluster_multixact_current_stats_get(ClusterCurrentMxStatId stat)
{
	if (ClusterCurrentMxStats == NULL || stat < 0 || stat >= CMX_STAT_COUNT)
		return 0;
	return pg_atomic_read_u64(&ClusterCurrentMxStats->counters[stat]);
}

#endif /* USE_PGRAC_CLUSTER */
