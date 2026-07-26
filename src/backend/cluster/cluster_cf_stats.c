/*-------------------------------------------------------------------------
 *
 * cluster_cf_stats.c
 *	  CF (control file) shared-authority observability counters (spec-5.6).
 *
 *	  Lock-free shmem counters bumped from the CF modules (acquire, cleanup,
 *	  authority, and recovery paths), plus a fixed per-PGPROC published-slot
 *	  ledger protected by one census LWLock.
 *
 *
 * Portions Copyright (c) 1996-2024, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2026, pgrac contributors
 *
 * Author: SqlRush <sqlrush@gmail.com>
 *
 * IDENTIFICATION
 *	  src/backend/cluster/cluster_cf_stats.c
 *
 * NOTES
 *	  This is a pgrac-original file (no derivation from PostgreSQL).
 *	  Spec: spec-5.6-cf-enqueue-shared-controlfile-authority.md
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "miscadmin.h"
#include "cluster/cluster_cf_stats.h"
#include "cluster/cluster_shmem.h"
#include "port/atomics.h"
#include "storage/lwlock.h"
#include "storage/proc.h"
#include "storage/shmem.h"

/*
 * Counters remain lock-free.  Slot mutations and snapshots use the same lock
 * so a census observes every X/S cell from one node-wide lock epoch.
 */
typedef struct ClusterCfStatsSharedState {
	pg_atomic_uint64 counters[CLUSTER_CF_COUNTER_COUNT];
	/*
	 * Cross-process JOIN_READONLY bring-up flag (0/1).  Node-level: the startup
	 * process sets it at the role gate; the checkpointer reads it to skip CF X
	 * during the pre-GES bring-up window and clears it once GES is available.
	 */
	pg_atomic_uint32 join_readonly;
	LWLockPadded slot_census_lock;
	uint32 slot_capacity;
	uint32 reserved;
	ClusterCfPublishedSlot slots[FLEXIBLE_ARRAY_MEMBER];
} ClusterCfStatsSharedState;

static ClusterCfStatsSharedState *cluster_cf_stats_state = NULL;


/* ============================================================
 * shmem region lifecycle (mirror cluster_advisory pattern).
 * ============================================================ */

static Size
cluster_cf_slot_capacity_configured(void)
{
	if (MaxBackends <= 0)
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("invalid CF published-slot process capacity"),
				 errhint("Restart with a positive max_connections-derived backend capacity.")));

	return add_size((Size)MaxBackends, (Size)NUM_AUXILIARY_PROCS);
}

Size
cluster_cf_stats_shmem_size(void)
{
	Size capacity = cluster_cf_slot_capacity_configured();
	Size slot_count = mul_size(capacity, (Size)CLUSTER_CF_SLOT_MODE_COUNT);

	return add_size(offsetof(ClusterCfStatsSharedState, slots),
					mul_size(slot_count, sizeof(ClusterCfPublishedSlot)));
}

void
cluster_cf_stats_shmem_init(void)
{
	ClusterCfStatsSharedState *state;
	Size capacity;
	Size size;
	bool found;

	cluster_cf_stats_state = NULL;
	capacity = cluster_cf_slot_capacity_configured();
	size = cluster_cf_stats_shmem_size();
	if (capacity > UINT32_MAX || ProcGlobal == NULL || (Size)ProcGlobal->allProcCount != capacity)
		ereport(FATAL,
				(errcode(ERRCODE_INTERNAL_ERROR),
				 errmsg("CF published-slot capacity does not match ProcGlobal"),
				 errhint("Restart the postmaster with a consistent backend process layout.")));

	state = ShmemInitStruct("pgrac cluster cf stats", size, &found);
	if (!found) {
		int i;

		memset(state, 0, size);
		for (i = 0; i < CLUSTER_CF_COUNTER_COUNT; i++)
			pg_atomic_init_u64(&state->counters[i], 0);
		pg_atomic_init_u32(&state->join_readonly, 0);
		LWLockInitialize(&state->slot_census_lock.lock, LWTRANCHE_CLUSTER_CF);
		state->slot_capacity = (uint32)capacity;
	} else if (state->slot_capacity != (uint32)capacity)
		ereport(FATAL,
				(errcode(ERRCODE_DATA_CORRUPTED),
				 errmsg("CF published-slot shared memory layout is incompatible"),
				 errhint("Restart the postmaster with one binary and one process capacity.")));

	cluster_cf_stats_state = state;
}

static const ClusterShmemRegion cluster_cf_stats_region = {
	.name = "pgrac cluster cf stats",
	.size_fn = cluster_cf_stats_shmem_size,
	.init_fn = cluster_cf_stats_shmem_init,
	.lwlock_count = 1,
	.owner_subsys = "cluster_cf_stats",
	.reserved_flags = 0,
};

void
cluster_cf_stats_shmem_register(void)
{
	cluster_shmem_register_region(&cluster_cf_stats_region);
}


/* ============================================================
 * counter mutate + read (NULL/uninit-safe).
 * ============================================================ */

void
cluster_cf_counter_inc(ClusterCfCounter which)
{
	if (cluster_cf_stats_state == NULL || (int)which < 0 || (int)which >= CLUSTER_CF_COUNTER_COUNT)
		return;
	pg_atomic_fetch_add_u64(&cluster_cf_stats_state->counters[which], 1);
}

uint64
cluster_cf_counter_read(ClusterCfCounter which)
{
	if (cluster_cf_stats_state == NULL || (int)which < 0 || (int)which >= CLUSTER_CF_COUNTER_COUNT)
		return 0;
	return pg_atomic_read_u64(&cluster_cf_stats_state->counters[which]);
}

/* ============================================================
 * fixed per-PGPROC published-slot ledger.
 * ============================================================ */

static bool
cluster_cf_slot_mode_valid(ClusterCfSlotMode mode)
{
	return mode == CLUSTER_CF_SLOT_MODE_X || mode == CLUSTER_CF_SLOT_MODE_S;
}

static bool
cluster_cf_slot_is_zero(const ClusterCfPublishedSlot *slot)
{
	return slot->state == CLUSTER_CF_SLOT_EMPTY && slot->mode == 0 && slot->owner_pid == 0
		   && slot->owner_procno == 0 && slot->owner_start_ts_us == 0 && slot->node_id == 0
		   && slot->coordinated == 0 && slot->cluster_epoch == 0 && slot->request_id == 0;
}

static ClusterCfPublishedSlot *
cluster_cf_slot_cell(uint32 owner_procno, ClusterCfSlotMode mode)
{
	Size index = (Size)owner_procno * (Size)CLUSTER_CF_SLOT_MODE_COUNT + (Size)mode;

	return &cluster_cf_stats_state->slots[index];
}

static bool
cluster_cf_slot_identity_valid(const ClusterCfPublishedSlot *slot, uint32 owner_procno,
							   ClusterCfSlotMode mode, ClusterCfPublishedSlotState state)
{
	if (slot->state != (uint32)state || slot->mode != (uint32)mode
		|| slot->owner_procno != owner_procno || slot->owner_pid <= 0
		|| slot->owner_start_ts_us <= 0 || slot->node_id < 0 || slot->coordinated > 1)
		return false;

	if (slot->coordinated != 0)
		return slot->request_id != 0;
	return slot->cluster_epoch == 0 && slot->request_id == 0;
}

static bool
cluster_cf_slot_identity_equal(const ClusterCfPublishedSlot *left,
							   const ClusterCfPublishedSlot *right)
{
	return left->mode == right->mode && left->owner_pid == right->owner_pid
		   && left->owner_procno == right->owner_procno
		   && left->owner_start_ts_us == right->owner_start_ts_us && left->node_id == right->node_id
		   && left->coordinated == right->coordinated && left->cluster_epoch == right->cluster_epoch
		   && left->request_id == right->request_id;
}

static void
cluster_cf_slot_mark_invalid(ClusterCfPublishedSlot *cell, const ClusterCfPublishedSlot *evidence)
{
	if (cluster_cf_slot_is_zero(cell))
		*cell = *evidence;
	cell->state = CLUSTER_CF_SLOT_INVALID;
}

bool
cluster_cf_slot_is_empty(uint32 owner_procno, ClusterCfSlotMode mode)
{
	bool empty;

	if (cluster_cf_stats_state == NULL || !cluster_cf_slot_mode_valid(mode)
		|| owner_procno >= cluster_cf_stats_state->slot_capacity)
		return false;

	LWLockAcquire(&cluster_cf_stats_state->slot_census_lock.lock, LW_SHARED);
	empty = cluster_cf_slot_is_zero(cluster_cf_slot_cell(owner_procno, mode));
	LWLockRelease(&cluster_cf_stats_state->slot_census_lock.lock);
	return empty;
}

bool
cluster_cf_slot_publish_held(ClusterCfSlotMode mode, const ClusterCfPublishedSlot *slot)
{
	ClusterCfPublishedSlot *cell;
	bool published = false;

	if (cluster_cf_stats_state == NULL || slot == NULL || !cluster_cf_slot_mode_valid(mode)
		|| slot->owner_procno >= cluster_cf_stats_state->slot_capacity)
		return false;

	LWLockAcquire(&cluster_cf_stats_state->slot_census_lock.lock, LW_EXCLUSIVE);
	cell = cluster_cf_slot_cell(slot->owner_procno, mode);
	if (cluster_cf_slot_identity_valid(slot, slot->owner_procno, mode, CLUSTER_CF_SLOT_HELD)
		&& cluster_cf_slot_is_zero(cell)) {
		*cell = *slot;
		published = true;
	} else
		cluster_cf_slot_mark_invalid(cell, slot);
	LWLockRelease(&cluster_cf_stats_state->slot_census_lock.lock);
	return published;
}

bool
cluster_cf_slot_publish_release_pending(ClusterCfSlotMode mode, const ClusterCfPublishedSlot *slot)
{
	ClusterCfPublishedSlot *cell;
	bool published = false;

	if (cluster_cf_stats_state == NULL || slot == NULL || !cluster_cf_slot_mode_valid(mode)
		|| slot->owner_procno >= cluster_cf_stats_state->slot_capacity)
		return false;

	LWLockAcquire(&cluster_cf_stats_state->slot_census_lock.lock, LW_EXCLUSIVE);
	cell = cluster_cf_slot_cell(slot->owner_procno, mode);
	if (cluster_cf_slot_identity_valid(slot, slot->owner_procno, mode,
									   CLUSTER_CF_SLOT_RELEASE_PENDING)
		&& cell->state == CLUSTER_CF_SLOT_HELD && cluster_cf_slot_identity_equal(cell, slot)) {
		cell->state = CLUSTER_CF_SLOT_RELEASE_PENDING;
		published = true;
	} else
		cluster_cf_slot_mark_invalid(cell, slot);
	LWLockRelease(&cluster_cf_stats_state->slot_census_lock.lock);
	return published;
}

bool
cluster_cf_slot_clear_exact(ClusterCfSlotMode mode, const ClusterCfPublishedSlot *slot)
{
	ClusterCfPublishedSlot *cell;
	bool cleared = false;

	if (cluster_cf_stats_state == NULL || slot == NULL || !cluster_cf_slot_mode_valid(mode)
		|| slot->owner_procno >= cluster_cf_stats_state->slot_capacity)
		return false;

	LWLockAcquire(&cluster_cf_stats_state->slot_census_lock.lock, LW_EXCLUSIVE);
	cell = cluster_cf_slot_cell(slot->owner_procno, mode);
	if (cluster_cf_slot_identity_valid(slot, slot->owner_procno, mode,
									   CLUSTER_CF_SLOT_RELEASE_PENDING)
		&& cell->state == CLUSTER_CF_SLOT_RELEASE_PENDING
		&& cluster_cf_slot_identity_equal(cell, slot)) {
		memset(cell, 0, sizeof(*cell));
		cleared = true;
	} else
		cluster_cf_slot_mark_invalid(cell, slot);
	LWLockRelease(&cluster_cf_stats_state->slot_census_lock.lock);
	return cleared;
}

static bool
cluster_cf_slot_census_cell_valid(const ClusterCfPublishedSlot *slot, uint32 owner_procno,
								  ClusterCfSlotMode mode)
{
	if (slot->state == CLUSTER_CF_SLOT_HELD)
		return cluster_cf_slot_identity_valid(slot, owner_procno, mode, CLUSTER_CF_SLOT_HELD);
	if (slot->state == CLUSTER_CF_SLOT_RELEASE_PENDING)
		return cluster_cf_slot_identity_valid(slot, owner_procno, mode,
											  CLUSTER_CF_SLOT_RELEASE_PENDING);
	return false;
}

bool
cluster_cf_slot_census(ClusterCfSlotCensus *out)
{
	uint32 owner_procno;
	uint64 x_nonempty_count = 0;
	bool first_x_valid = false;

	if (out == NULL)
		return false;
	memset(out, 0, sizeof(*out));
	out->x_owner_state = CLUSTER_CF_X_OWNER_EMPTY;
	if (cluster_cf_stats_state == NULL || ProcGlobal == NULL
		|| cluster_cf_stats_state->slot_capacity == 0
		|| ProcGlobal->allProcCount != cluster_cf_stats_state->slot_capacity)
		return false;

	LWLockAcquire(&cluster_cf_stats_state->slot_census_lock.lock, LW_SHARED);
	for (owner_procno = 0; owner_procno < cluster_cf_stats_state->slot_capacity; owner_procno++) {
		ClusterCfSlotMode mode;

		for (mode = CLUSTER_CF_SLOT_MODE_X; mode < CLUSTER_CF_SLOT_MODE_COUNT; mode++) {
			ClusterCfPublishedSlot *slot = cluster_cf_slot_cell(owner_procno, mode);
			bool cell_valid;

			if (cluster_cf_slot_is_zero(slot))
				continue;
			cell_valid = cluster_cf_slot_census_cell_valid(slot, owner_procno, mode);
			if (mode == CLUSTER_CF_SLOT_MODE_X) {
				x_nonempty_count++;
				if (x_nonempty_count == 1) {
					out->x_owner = *slot;
					first_x_valid = cell_valid;
				}
			}
			if (!cell_valid) {
				out->invalid_count++;
				continue;
			}
			if (slot->state == CLUSTER_CF_SLOT_HELD) {
				if (mode == CLUSTER_CF_SLOT_MODE_X)
					out->x_held_count++;
				else
					out->s_held_count++;
			} else {
				if (mode == CLUSTER_CF_SLOT_MODE_X)
					out->x_release_pending_count++;
				else
					out->s_release_pending_count++;
			}
		}
	}
	LWLockRelease(&cluster_cf_stats_state->slot_census_lock.lock);

	out->pending_retry_count = out->x_release_pending_count + out->s_release_pending_count;
	if (x_nonempty_count > 1) {
		memset(&out->x_owner, 0, sizeof(out->x_owner));
		out->x_owner_state = CLUSTER_CF_X_OWNER_AMBIGUOUS;
		out->invalid_count++;
	} else if (x_nonempty_count == 1) {
		if (!first_x_valid)
			out->x_owner_state = CLUSTER_CF_X_OWNER_INVALID;
		else if (out->x_owner.state == CLUSTER_CF_SLOT_HELD)
			out->x_owner_state = CLUSTER_CF_X_OWNER_HELD;
		else
			out->x_owner_state = CLUSTER_CF_X_OWNER_RELEASE_PENDING;
	}
	out->valid = out->invalid_count == 0;
	return out->valid;
}


/* ============================================================
 * cross-process JOIN_READONLY bring-up flag (NULL/uninit-safe).
 * ============================================================ */

void
cluster_cf_stats_set_join_readonly(bool on)
{
	if (cluster_cf_stats_state == NULL)
		return;
	pg_atomic_write_u32(&cluster_cf_stats_state->join_readonly, on ? 1 : 0);
}

bool
cluster_cf_stats_get_join_readonly(void)
{
	if (cluster_cf_stats_state == NULL)
		return false;
	return pg_atomic_read_u32(&cluster_cf_stats_state->join_readonly) != 0;
}
