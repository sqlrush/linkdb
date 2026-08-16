/*-------------------------------------------------------------------------
 *
 * cluster_lms_lifecycle.c
 *    Lock-free LMS shared-state invalidation used by the postmaster reaper.
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "cluster/cluster_lms.h"

void
cluster_lms_shared_mark_child_exit(ClusterLmsSharedState *state)
{
	if (state == NULL)
		return;

	/* The child may have died while holding its LWLock.  The reaper must never
	 * wait on that lock: retract both generation capabilities first, clear the
	 * diagnostic ownership bytes, then publish STOPPED as the final gate. */
	pg_atomic_write_u64(&state->recovery_ready_generation, 0);
	pg_atomic_write_u64(&state->serving_requested_generation, 0);
	state->pid = 0;
	state->worker_pids[0] = 0;
	pg_atomic_write_u32(&state->lms_state, (uint32)CLUSTER_LMS_STOPPED);
}
