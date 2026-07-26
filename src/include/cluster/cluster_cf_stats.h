/*-------------------------------------------------------------------------
 *
 * cluster_cf_stats.h
 *	  CF (control file) shared-authority observability counters (spec-5.6).
 *
 *	  A small, dependency-light owner for the CF shared-authority
 *	  counters and the fixed published-slot census.  Every CF module
 *	  (authority read, storage bootstrap, enqueue) can include this header
 *	  without pulling in GES machinery.  Counters remain lock-free; live
 *	  holder cells use the region's census LWLock.
 *
 *
 * Portions Copyright (c) 1996-2024, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2026, pgrac contributors
 *
 * Author: SqlRush <sqlrush@gmail.com>
 *
 * IDENTIFICATION
 *	  src/include/cluster/cluster_cf_stats.h
 *
 * NOTES
 *	  This is a pgrac-original file (no derivation from PostgreSQL).
 *	  Spec: spec-5.6-cf-enqueue-shared-controlfile-authority.md
 *
 *-------------------------------------------------------------------------
 */
#ifndef CLUSTER_CF_STATS_H
#define CLUSTER_CF_STATS_H

#include "c.h"

/*
 * spec-5.6 -- CF shared-authority observability counters.  Each has at
 * least one call site (DoD); they are pure observability and never gate logic.
 */
typedef enum ClusterCfCounter {
	CLUSTER_CF_X_ACQUIRE = 0,			  /* CF X granted (write authority) */
	CLUSTER_CF_S_ACQUIRE = 1,			  /* CF S granted (strong-consistency read) */
	CLUSTER_CF_FAILCLOSED = 2,			  /* CF lock unprovable -> caller fails closed */
	CLUSTER_CF_SINGLE_NODE_AUTHORITY = 3, /* bootstrap single-node authority window opened */
	CLUSTER_CF_BAK_FALLBACK = 4,		  /* authority read fell back to a valid .bak */
	/* spec-5.6a: per-node recovery anchor observability */
	CLUSTER_CF_RECOVERY_ANCHOR_WRITE = 5,	   /* anchor durably (re)written */
	CLUSTER_CF_RECOVERY_ANCHOR_BOOT_ADOPT = 6, /* boot adopted a valid anchor */
	/* CF exact-holder release confirmed by S6 (attempts/failures excluded). */
	CLUSTER_CF_S6_RELEASE_CONFIRMED = 7,
	CLUSTER_CF_COUNTER_COUNT = 8
} ClusterCfCounter;

typedef enum ClusterCfSlotMode {
	CLUSTER_CF_SLOT_MODE_X = 0,
	CLUSTER_CF_SLOT_MODE_S = 1,
	CLUSTER_CF_SLOT_MODE_COUNT = 2
} ClusterCfSlotMode;

typedef enum ClusterCfPublishedSlotState {
	CLUSTER_CF_SLOT_EMPTY = 0,
	CLUSTER_CF_SLOT_HELD = 1,
	CLUSTER_CF_SLOT_RELEASE_PENDING = 2,
	CLUSTER_CF_SLOT_INVALID = 3
} ClusterCfPublishedSlotState;

typedef struct ClusterCfPublishedSlot {
	uint32 state;
	uint32 mode;
	int32 owner_pid;
	uint32 owner_procno;
	int64 owner_start_ts_us;
	int32 node_id;
	uint32 coordinated;
	uint64 cluster_epoch;
	uint64 request_id;
} ClusterCfPublishedSlot;

typedef enum ClusterCfXOwnerState {
	CLUSTER_CF_X_OWNER_EMPTY = 0,
	CLUSTER_CF_X_OWNER_HELD,
	CLUSTER_CF_X_OWNER_RELEASE_PENDING,
	CLUSTER_CF_X_OWNER_AMBIGUOUS,
	CLUSTER_CF_X_OWNER_INVALID
} ClusterCfXOwnerState;

typedef struct ClusterCfSlotCensus {
	bool valid;
	uint64 x_held_count;
	uint64 s_held_count;
	uint64 x_release_pending_count;
	uint64 s_release_pending_count;
	uint64 pending_retry_count;
	uint64 invalid_count;
	ClusterCfXOwnerState x_owner_state;
	ClusterCfPublishedSlot x_owner;
} ClusterCfSlotCensus;

/* shmem region lifecycle (mirror cluster_advisory). */
extern Size cluster_cf_stats_shmem_size(void);
extern void cluster_cf_stats_shmem_init(void);
extern void cluster_cf_stats_shmem_register(void);

/* counter mutate + read (NULL/uninit-safe; out-of-range is a no-op / 0). */
extern void cluster_cf_counter_inc(ClusterCfCounter which);
extern uint64 cluster_cf_counter_read(ClusterCfCounter which);

/*
 * Fixed per-PGPROC published-slot ledger.  Mutation helpers are nonthrowing
 * after initialization and preserve contradictory evidence as INVALID.
 */
extern bool cluster_cf_slot_is_empty(uint32 owner_procno, ClusterCfSlotMode mode);
extern bool cluster_cf_slot_publish_held(ClusterCfSlotMode mode,
										 const ClusterCfPublishedSlot *slot);
extern bool cluster_cf_slot_publish_release_pending(ClusterCfSlotMode mode,
													const ClusterCfPublishedSlot *slot);
extern bool cluster_cf_slot_clear_exact(ClusterCfSlotMode mode, const ClusterCfPublishedSlot *slot);
extern bool cluster_cf_slot_census(ClusterCfSlotCensus *out);

/*
 * spec-5.6 increment (iii) follow-up -- cross-process JOIN_READONLY bring-up
 * flag.  Lives in this lock-free CF shmem region (not a process-local static)
 * so the checkpointer can see the role the startup process set: a multi-node
 * node whose Phase-2 proved a peer alive attaches read-only and, during the
 * pre-GES bring-up window, must skip CF X + the shared-authority write (the
 * checkpointer's end-of-recovery checkpoint would otherwise try CF X before
 * LMS/GES is ready and fail).  The flag is set by the startup-process role gate
 * and cleared once GES is available so EVERY steady-state checkpoint takes CF X
 * (it is strictly a bring-up window, never a steady-state CF-X bypass).
 * NULL/uninit-safe: reads false when the region is absent (single-node / off).
 */
extern void cluster_cf_stats_set_join_readonly(bool on);
extern bool cluster_cf_stats_get_join_readonly(void);

#endif /* CLUSTER_CF_STATS_H */
