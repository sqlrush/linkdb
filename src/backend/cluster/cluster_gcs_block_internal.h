/*-------------------------------------------------------------------------
 *
 * cluster_gcs_block_internal.h
 *	  Private executable policy seams for cluster_gcs_block.c.
 *
 * These pure helpers keep irreversible PCM-X handler decisions directly
 * unit-testable without exporting production symbols or changing wire/shmem
 * ABI.  This header is private to the backend compilation unit and its
 * cluster_unit test.
 *
 *-------------------------------------------------------------------------
 */
#ifndef CLUSTER_GCS_BLOCK_INTERNAL_H
#define CLUSTER_GCS_BLOCK_INTERNAL_H

#include "cluster/cluster_gcs_block.h"
#include "cluster/cluster_ic.h"

#define GCS_BLOCK_FORWARD_CANCEL_REPLAY_POLICY_V1 1

typedef enum GcsBlockForwardCancelReplayPhase {
	GCS_BLOCK_FORWARD_CANCEL_REPLAY_PARKED_PHASE = 0,
	GCS_BLOCK_FORWARD_CANCEL_REPLAY_RELEASE_STAGED = 1,
	GCS_BLOCK_FORWARD_CANCEL_REPLAY_ACK_STAGED = 2
} GcsBlockForwardCancelReplayPhase;

typedef struct GcsBlockForwardCancelReplayEntry {
	bool in_use;
	uint8 phase;
	GcsBlockForwardCancelPayload barrier;
} GcsBlockForwardCancelReplayEntry;

typedef enum GcsBlockForwardCancelReplayParkResult {
	GCS_BLOCK_FORWARD_CANCEL_REPLAY_INVALID = -1,
	GCS_BLOCK_FORWARD_CANCEL_REPLAY_PARKED = 0,
	GCS_BLOCK_FORWARD_CANCEL_REPLAY_DUPLICATE = 1,
	GCS_BLOCK_FORWARD_CANCEL_REPLAY_COLLISION = 2,
	GCS_BLOCK_FORWARD_CANCEL_REPLAY_FULL = 3
} GcsBlockForwardCancelReplayParkResult;

typedef enum GcsBlockForwardCancelReplayIdentityVerdict {
	GCS_BLOCK_FORWARD_CANCEL_REPLAY_IDENTITY_EXACT = 0,
	GCS_BLOCK_FORWARD_CANCEL_REPLAY_IDENTITY_RETRY = 1,
	GCS_BLOCK_FORWARD_CANCEL_REPLAY_IDENTITY_RETIRED = 2
} GcsBlockForwardCancelReplayIdentityVerdict;

typedef struct GcsBlockForwardCancelReplayObservation {
	GcsBlockForwardCancelReplayIdentityVerdict identity_verdict;
	GcsBlockForwardCancelReplayPhase phase;
	bool newer_request_active;
	bool grant_pending;
	PcmLockMode local_mode;
} GcsBlockForwardCancelReplayObservation;

typedef enum GcsBlockForwardCancelReplayAction {
	GCS_BLOCK_FORWARD_CANCEL_REPLAY_RETAIN = 0,
	GCS_BLOCK_FORWARD_CANCEL_REPLAY_DROP_S = 1,
	GCS_BLOCK_FORWARD_CANCEL_REPLAY_STAGE_RELEASE = 2,
	GCS_BLOCK_FORWARD_CANCEL_REPLAY_STAGE_ACK = 3,
	GCS_BLOCK_FORWARD_CANCEL_REPLAY_DISCARD = 4
} GcsBlockForwardCancelReplayAction;

static inline bool
gcs_block_forward_cancel_replay_same_key(
	const GcsBlockForwardCancelPayload *left,
	const GcsBlockForwardCancelPayload *right)
{
	return left != NULL && right != NULL
		   && left->request_id == right->request_id
		   && left->request_epoch == right->request_epoch
		   && left->requester_node == right->requester_node
		   && left->requester_backend_id == right->requester_backend_id
		   && left->requester_incarnation == right->requester_incarnation;
}

static inline bool
gcs_block_forward_cancel_replay_payload_exact(
	const GcsBlockForwardCancelPayload *left,
	const GcsBlockForwardCancelPayload *right)
{
	return left != NULL && right != NULL
		   && memcmp(left, right, sizeof(*left)) == 0;
}

static inline bool
gcs_block_forward_cancel_replay_barrier_shape_valid(
	const GcsBlockForwardCancelPayload *barrier)
{
	return barrier != NULL && barrier->request_id != 0
		   && barrier->request_epoch != 0
		   && barrier->requester_incarnation != 0
		   && barrier->master_incarnation != 0
		   && barrier->holder_incarnation != 0
		   && barrier->requester_backend_id > 0
		   && barrier->phase
				  == (uint8)GCS_FORWARD_CANCEL_PHASE_HOLDER_BARRIER
		   && barrier->reason
				  == (uint8)GCS_FORWARD_CANCEL_REASON_PENDING_X
		   && barrier->proof == GCS_FORWARD_CANCEL_PROOF_BARRIER_MASK
		   && barrier->transition_id == (uint8)PCM_TRANS_N_TO_S
		   && barrier->master_holder_capability_generation != 0
		   && barrier->holder_requester_capability_generation != 0
		   && barrier->requester_master_capability_generation == 0
		   && GcsBlockForwardCancelReservedZero(barrier);
}

static inline GcsBlockForwardCancelReplayParkResult
gcs_block_forward_cancel_replay_ledger_park(
	GcsBlockForwardCancelReplayEntry *entries, size_t entry_count,
	const GcsBlockForwardCancelPayload *barrier, size_t *slot_out)
{
	size_t free_slot = entry_count;
	size_t i;

	if (slot_out != NULL)
		*slot_out = entry_count;
	if (entries == NULL || entry_count == 0 || slot_out == NULL
		|| !gcs_block_forward_cancel_replay_barrier_shape_valid(barrier))
		return GCS_BLOCK_FORWARD_CANCEL_REPLAY_INVALID;
	for (i = 0; i < entry_count; i++) {
		if (!entries[i].in_use) {
			if (free_slot == entry_count)
				free_slot = i;
			continue;
		}
		if (!gcs_block_forward_cancel_replay_same_key(
				&entries[i].barrier, barrier))
			continue;
		*slot_out = i;
		return gcs_block_forward_cancel_replay_payload_exact(
				   &entries[i].barrier, barrier)
				   ? GCS_BLOCK_FORWARD_CANCEL_REPLAY_DUPLICATE
				   : GCS_BLOCK_FORWARD_CANCEL_REPLAY_COLLISION;
	}
	if (free_slot == entry_count)
		return GCS_BLOCK_FORWARD_CANCEL_REPLAY_FULL;
	memset(&entries[free_slot], 0, sizeof(entries[free_slot]));
	entries[free_slot].barrier = *barrier;
	entries[free_slot].phase
		= (uint8)GCS_BLOCK_FORWARD_CANCEL_REPLAY_PARKED_PHASE;
	entries[free_slot].in_use = true;
	*slot_out = free_slot;
	return GCS_BLOCK_FORWARD_CANCEL_REPLAY_PARKED;
}

static inline bool
gcs_block_forward_cancel_replay_mark_release_exact(
	GcsBlockForwardCancelReplayEntry *entries, size_t entry_count,
	size_t slot, const GcsBlockForwardCancelPayload *barrier)
{
	GcsBlockForwardCancelReplayEntry *entry;

	if (entries == NULL || slot >= entry_count)
		return false;
	entry = &entries[slot];
	if (!entry->in_use
		|| !gcs_block_forward_cancel_replay_payload_exact(
			&entry->barrier, barrier))
		return false;
	if (entry->phase
		== (uint8)GCS_BLOCK_FORWARD_CANCEL_REPLAY_PARKED_PHASE)
		entry->phase
			= (uint8)GCS_BLOCK_FORWARD_CANCEL_REPLAY_RELEASE_STAGED;
	return entry->phase
			   == (uint8)GCS_BLOCK_FORWARD_CANCEL_REPLAY_RELEASE_STAGED
		   || entry->phase
				  == (uint8)GCS_BLOCK_FORWARD_CANCEL_REPLAY_ACK_STAGED;
}

static inline bool
gcs_block_forward_cancel_replay_mark_ack_exact(
	GcsBlockForwardCancelReplayEntry *entries, size_t entry_count,
	size_t slot, const GcsBlockForwardCancelPayload *barrier)
{
	GcsBlockForwardCancelReplayEntry *entry;

	if (entries == NULL || slot >= entry_count)
		return false;
	entry = &entries[slot];
	if (!entry->in_use
		|| !gcs_block_forward_cancel_replay_payload_exact(
			&entry->barrier, barrier)
		|| entry->phase
			   == (uint8)GCS_BLOCK_FORWARD_CANCEL_REPLAY_PARKED_PHASE)
		return false;
	entry->phase = (uint8)GCS_BLOCK_FORWARD_CANCEL_REPLAY_ACK_STAGED;
	return true;
}

static inline bool
gcs_block_forward_cancel_replay_finish_exact(
	GcsBlockForwardCancelReplayEntry *entries, size_t entry_count,
	size_t slot, const GcsBlockForwardCancelPayload *barrier)
{
	if (entries == NULL || slot >= entry_count
		|| !entries[slot].in_use
		|| entries[slot].phase
			   != (uint8)GCS_BLOCK_FORWARD_CANCEL_REPLAY_ACK_STAGED
		|| !gcs_block_forward_cancel_replay_payload_exact(
			&entries[slot].barrier, barrier))
		return false;
	memset(&entries[slot], 0, sizeof(entries[slot]));
	return true;
}

static inline GcsBlockForwardCancelReplayAction
gcs_block_forward_cancel_replay_next_action(
	const GcsBlockForwardCancelReplayObservation *observation)
{
	if (observation == NULL)
		return GCS_BLOCK_FORWARD_CANCEL_REPLAY_RETAIN;
	if (observation->identity_verdict
		== GCS_BLOCK_FORWARD_CANCEL_REPLAY_IDENTITY_RETIRED)
		return GCS_BLOCK_FORWARD_CANCEL_REPLAY_DISCARD;
	if (observation->identity_verdict
			!= GCS_BLOCK_FORWARD_CANCEL_REPLAY_IDENTITY_EXACT
		|| observation->newer_request_active
		|| observation->grant_pending
		|| observation->local_mode == PCM_LOCK_MODE_X)
		return GCS_BLOCK_FORWARD_CANCEL_REPLAY_RETAIN;
	if (observation->phase
		== GCS_BLOCK_FORWARD_CANCEL_REPLAY_ACK_STAGED)
		return GCS_BLOCK_FORWARD_CANCEL_REPLAY_RETAIN;
	if (observation->phase
		== GCS_BLOCK_FORWARD_CANCEL_REPLAY_RELEASE_STAGED)
		return observation->local_mode == PCM_LOCK_MODE_N
				   ? GCS_BLOCK_FORWARD_CANCEL_REPLAY_STAGE_ACK
				   : GCS_BLOCK_FORWARD_CANCEL_REPLAY_RETAIN;
	if (observation->phase
		!= GCS_BLOCK_FORWARD_CANCEL_REPLAY_PARKED_PHASE)
		return GCS_BLOCK_FORWARD_CANCEL_REPLAY_RETAIN;
	if (observation->local_mode == PCM_LOCK_MODE_S)
		return GCS_BLOCK_FORWARD_CANCEL_REPLAY_DROP_S;
	if (observation->local_mode == PCM_LOCK_MODE_N)
		return GCS_BLOCK_FORWARD_CANCEL_REPLAY_STAGE_RELEASE;
	return GCS_BLOCK_FORWARD_CANCEL_REPLAY_RETAIN;
}

static inline bool
gcs_block_forward_cancel_master_ack_ready(
	const PcmAuthoritySnapshot *authority, int32 requester_node)
{
	uint32 requester_bit;

	if (authority == NULL || requester_node < 0
		|| requester_node >= PCM_X_PROTOCOL_NODE_LIMIT)
		return false;
	requester_bit = UINT32_C(1) << requester_node;
	return (authority->s_holders_bitmap & requester_bit) == 0
		   && authority->x_holder_node != requester_node;
}

static inline bool
gcs_block_pcm_x_retire_send_committed(ClusterICSendResult send_result)
{
	return send_result == CLUSTER_IC_SEND_DONE || send_result == CLUSTER_IC_SEND_WOULD_BLOCK;
}

static inline bool
gcs_block_pcm_x_retire_resolve_committed(PcmXQueueResult result)
{
	return result == PCM_X_QUEUE_OK || result == PCM_X_QUEUE_DUPLICATE;
}

static inline bool
gcs_block_pcm_x_retire_observation_visible(uint64 frontier_before, uint64 frontier_after,
										   uint64 removed)
{
	return frontier_before != frontier_after || removed != 0;
}

/*
 * The late-DRAIN replay branch has already observed local DUPLICATE and a
 * missing DRAINED certificate.  RETIRED completes the replay; a concurrent
 * later RETIRE makes the proof transient and must be retried.  Every stable
 * refusal is unexplained certificate loss and remains on the caller's fuse.
 */
static inline PcmXQueueResult
gcs_block_pcm_x_late_drain_retired_proof(PcmXQueueResult retired_result)
{
	if (retired_result == PCM_X_QUEUE_RETIRED)
		return PCM_X_QUEUE_DUPLICATE;
	if (retired_result == PCM_X_QUEUE_BUSY || retired_result == PCM_X_QUEUE_NOT_READY)
		return retired_result;
	return PCM_X_QUEUE_CORRUPT;
}

/*
 * A retired REVOKE replay is acknowledged with the exact ref and no phase or
 * reason bits.  Returning false means the normal image path owns the frame.
 */
static inline bool
gcs_block_pcm_x_retired_revoke_ack_build(const PcmXTicketRef *ref,
										 PcmXQueueResult retired_result,
										 PcmXPhasePayload *ack_out)
{
	if (ack_out != NULL)
		memset(ack_out, 0, sizeof(*ack_out));
	if (ref == NULL || ack_out == NULL || retired_result != PCM_X_QUEUE_RETIRED)
		return false;
	ack_out->ref = *ref;
	return true;
}

#endif /* CLUSTER_GCS_BLOCK_INTERNAL_H */
