/*-------------------------------------------------------------------------
 *
 * cluster_multixact_current_stats.h
 *	  Shared observability for current-DML MultiXact authority.
 *
 * Portions Copyright (c) 1996-2024, PostgreSQL Global Development Group
 * Portions Copyright (c) 2026, pgrac contributors
 *
 *-------------------------------------------------------------------------
 */
#ifndef CLUSTER_MULTIXACT_CURRENT_STATS_H
#define CLUSTER_MULTIXACT_CURRENT_STATS_H

#include "storage/shmem.h"

#ifdef USE_PGRAC_CLUSTER

typedef enum ClusterCurrentMxStatId {
	CMX_STAT_DESCRIBE_LOCAL = 0,
	CMX_STAT_DESCRIBE_REMOTE_ASK,
	CMX_STAT_DESCRIBE_REMOTE_HIT,
	CMX_STAT_DESCRIBE_REMOTE_DENIED,
	CMX_STAT_DESCRIBE_REMOTE_SUPPORTED_LIMIT,
	CMX_STAT_DESCRIBE_REMOTE_TIMEOUT,
	CMX_STAT_DESCRIBE_REMOTE_UNKNOWN,
	CMX_STAT_DESCRIBE_INVALID_REPLY,
	CMX_STAT_MEMBER_PROOF_ASK,
	CMX_STAT_MEMBER_PROOF_HIT,
	CMX_STAT_MEMBER_PROOF_UNKNOWN,
	CMX_STAT_MEMBER_PROOF_DENIED,
	CMX_STAT_MEMBER_PROOF_SUPPORTED_LIMIT,
	CMX_STAT_MEMBER_PROOF_TIMEOUT,
	CMX_STAT_MEMBER_PROOF_INVALID_REPLY,
	CMX_STAT_WAIT,
	CMX_STAT_WAIT_RESOLVED,
	CMX_STAT_WAIT_DEAD_HOLDER,
	CMX_STAT_WAIT_TIMEOUT,
	CMX_STAT_WAIT_RETRY,
	CMX_STAT_WAIT_INTERRUPTED,
	CMX_STAT_DEADLOCK_VICTIM,
	CMX_STAT_WAKEUP,
	CMX_STAT_RECOMPOSE_SUCCESS,
	CMX_STAT_RECOMPOSE_FAILCLOSED,
	CMX_STAT_HOT_PROOF_HIT,
	CMX_STAT_HOT_PROOF_FAILCLOSED,
	CMX_STAT_ABA_RESTART,
	CMX_STAT_RESTART_BUCKET_0,
	CMX_STAT_RESTART_BUCKET_1,
	CMX_STAT_RESTART_BUCKET_2_3,
	CMX_STAT_RESTART_BUCKET_4_7,
	CMX_STAT_RESTART_BUCKET_8_PLUS,
	CMX_STAT_RESTART_MAX,
	CMX_STAT_FOREIGN_SLRU_GUARD,
	CMX_STAT_COUNT
} ClusterCurrentMxStatId;

extern Size cluster_multixact_current_stats_shmem_size(void);
extern void cluster_multixact_current_stats_shmem_init(void);
extern void cluster_multixact_current_stats_shmem_register(void);
extern void cluster_multixact_current_stats_bump(ClusterCurrentMxStatId stat);
extern uint64 cluster_multixact_current_stats_get(ClusterCurrentMxStatId stat);

#endif /* USE_PGRAC_CLUSTER */

#endif /* CLUSTER_MULTIXACT_CURRENT_STATS_H */
