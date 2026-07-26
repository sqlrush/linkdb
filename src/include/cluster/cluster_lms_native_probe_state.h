/*-------------------------------------------------------------------------
 *
 * cluster_lms_native_probe_state.h
 *	  Atomic lifecycle for LMS native-lock probe collector slots.
 *
 * A reserved slot is deliberately invisible to reply, retry, and aggregate
 * observers until its complete request identity and expected-reply set have
 * been initialized.  A terminal observer must claim RESOLVING before it may
 * perform any external side effect.
 *
 *-------------------------------------------------------------------------
 */
#ifndef CLUSTER_LMS_NATIVE_PROBE_STATE_H
#define CLUSTER_LMS_NATIVE_PROBE_STATE_H

#include "port/atomics.h"
#include "storage/lock.h"

typedef enum ClusterLmsNativeProbeSlotState {
	CLUSTER_LMS_NATIVE_PROBE_SLOT_FREE = 0,
	CLUSTER_LMS_NATIVE_PROBE_SLOT_ACTIVE = 1,
	CLUSTER_LMS_NATIVE_PROBE_SLOT_RESERVED = 2,
	CLUSTER_LMS_NATIVE_PROBE_SLOT_RESOLVING = 3
} ClusterLmsNativeProbeSlotState;

/*
 * Assertion-build-only production seam state.  The caller stores this in
 * shared memory so the postmaster can arm the exact LMS process that later
 * aggregates the probe.  A false return means "run the real probe result";
 * only one exact (node, full 16-byte LOCKTAG) match may consume remaining.
 */
typedef struct ClusterLmsNativeProbeForceClearOnce {
	int32 target_node_id;
	LOCKTAG target_locktag;
	uint8 target_request_opcode;
	LOCKMODE target_lockmode;
	pg_atomic_uint32 remaining;
} ClusterLmsNativeProbeForceClearOnce;

extern ClusterLmsNativeProbeSlotState
cluster_lms_native_probe_state_read(pg_atomic_uint64 *state);
extern bool cluster_lms_native_probe_state_try_reserve(pg_atomic_uint64 *state);
extern bool cluster_lms_native_probe_state_try_publish_active(pg_atomic_uint64 *state);
extern bool cluster_lms_native_probe_state_is_active(pg_atomic_uint64 *state);
extern bool cluster_lms_native_probe_state_try_claim_resolving(pg_atomic_uint64 *state);
extern bool cluster_lms_native_probe_state_try_release(pg_atomic_uint64 *state);
extern void cluster_lms_native_probe_force_clear_once_init(
	ClusterLmsNativeProbeForceClearOnce *seam, uint32 remaining,
	int32 target_node_id, const LOCKTAG *target_locktag,
	uint8 target_request_opcode, LOCKMODE target_lockmode);
extern bool cluster_lms_native_probe_force_clear_once_try_consume(
	ClusterLmsNativeProbeForceClearOnce *seam, int32 node_id,
	const LOCKTAG *locktag, uint8 request_opcode, LOCKMODE lockmode);

#endif							/* CLUSTER_LMS_NATIVE_PROBE_STATE_H */
