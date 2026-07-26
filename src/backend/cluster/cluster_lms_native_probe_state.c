/*-------------------------------------------------------------------------
 *
 * cluster_lms_native_probe_state.c
 *	  Atomic lifecycle for LMS native-lock probe collector slots.
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "cluster/cluster_lms_native_probe_state.h"


ClusterLmsNativeProbeSlotState
cluster_lms_native_probe_state_read(pg_atomic_uint64 *state)
{
	return (ClusterLmsNativeProbeSlotState)pg_atomic_read_u64(state);
}

bool
cluster_lms_native_probe_state_try_reserve(pg_atomic_uint64 *state)
{
	uint64 expected = CLUSTER_LMS_NATIVE_PROBE_SLOT_FREE;

	return pg_atomic_compare_exchange_u64(
		state, &expected, CLUSTER_LMS_NATIVE_PROBE_SLOT_RESERVED);
}

bool
cluster_lms_native_probe_state_try_publish_active(pg_atomic_uint64 *state)
{
	uint64 expected = CLUSTER_LMS_NATIVE_PROBE_SLOT_RESERVED;

	return pg_atomic_compare_exchange_u64(
		state, &expected, CLUSTER_LMS_NATIVE_PROBE_SLOT_ACTIVE);
}

bool
cluster_lms_native_probe_state_is_active(pg_atomic_uint64 *state)
{
	return cluster_lms_native_probe_state_read(state)
		   == CLUSTER_LMS_NATIVE_PROBE_SLOT_ACTIVE;
}

bool
cluster_lms_native_probe_state_try_claim_resolving(pg_atomic_uint64 *state)
{
	uint64 expected = CLUSTER_LMS_NATIVE_PROBE_SLOT_ACTIVE;

	return pg_atomic_compare_exchange_u64(
		state, &expected, CLUSTER_LMS_NATIVE_PROBE_SLOT_RESOLVING);
}

bool
cluster_lms_native_probe_state_try_release(pg_atomic_uint64 *state)
{
	uint64 observed = pg_atomic_read_u64(state);

	for (;;) {
		if (observed == CLUSTER_LMS_NATIVE_PROBE_SLOT_FREE)
			return false;
		if (observed > CLUSTER_LMS_NATIVE_PROBE_SLOT_RESOLVING)
			return false;
		if (pg_atomic_compare_exchange_u64(
				state, &observed, CLUSTER_LMS_NATIVE_PROBE_SLOT_FREE))
			return true;
	}
}

void
cluster_lms_native_probe_force_clear_once_init(
	ClusterLmsNativeProbeForceClearOnce *seam, uint32 remaining,
	int32 target_node_id, const LOCKTAG *target_locktag,
	uint8 target_request_opcode, LOCKMODE target_lockmode)
{
	memset(seam, 0, sizeof(*seam));
	seam->target_node_id = target_node_id;
	if (target_locktag != NULL)
		memcpy(&seam->target_locktag, target_locktag, sizeof(LOCKTAG));
	seam->target_request_opcode = target_request_opcode;
	seam->target_lockmode = target_lockmode;
	pg_atomic_init_u32(&seam->remaining, remaining == 0 ? 0 : 1);
}

bool
cluster_lms_native_probe_force_clear_once_try_consume(
	ClusterLmsNativeProbeForceClearOnce *seam, int32 node_id,
	const LOCKTAG *locktag, uint8 request_opcode, LOCKMODE lockmode)
{
	uint32 expected = 1;

	if (node_id != seam->target_node_id || locktag == NULL
		|| memcmp(locktag, &seam->target_locktag, sizeof(LOCKTAG)) != 0
		|| request_opcode != seam->target_request_opcode
		|| lockmode != seam->target_lockmode)
		return false;

	/*
	 * The target comparison deliberately precedes the CAS: every wrong-node,
	 * wrong-tag, wrong-opcode, or wrong-mode probe must leave the one shot
	 * armed for the exact target.  Concurrent exact observers race on 1 -> 0,
	 * so only one can win.
	 */
	return pg_atomic_compare_exchange_u32(&seam->remaining, &expected, 0);
}
