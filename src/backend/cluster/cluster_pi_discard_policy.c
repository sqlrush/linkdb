/*-------------------------------------------------------------------------
 *
 * cluster_pi_discard_policy.c
 *	  Pure, typed policy for bounded Past Image discard.
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#ifdef USE_PGRAC_CLUSTER

#include "cluster/cluster_pi_discard_policy.h"

ClusterBufmgrPiDiscardResult
cluster_pi_discard_classify_locked(const ClusterPiDiscardShape *shape)
{
	if (shape == NULL)
		return CLUSTER_BUFMGR_PI_DISCARD_CORRUPT;
	if (!shape->tag_matches)
		return CLUSTER_BUFMGR_PI_DISCARD_TERMINAL_NOOP;
	if (!shape->is_pi)
		return shape->is_current
				   ? CLUSTER_BUFMGR_PI_DISCARD_TERMINAL_NOOP
				   : CLUSTER_BUFMGR_PI_DISCARD_CORRUPT;
	if (shape->bm_valid || !shape->pcm_is_n)
		return CLUSTER_BUFMGR_PI_DISCARD_CORRUPT;
	if (shape->own_shape != CLUSTER_PCM_OWN_OK
		|| shape->own_flags != 0
		|| shape->writer_activation_token != 0)
		return CLUSTER_BUFMGR_PI_DISCARD_CORRUPT;
	if (shape->refcount != 0)
		return CLUSTER_BUFMGR_PI_DISCARD_RETRYABLE_BUSY;
	return CLUSTER_BUFMGR_PI_DISCARD_DROPPED;
}

ClusterPiDiscardAction
cluster_pi_discard_action(ClusterBufmgrPiDiscardResult result)
{
	switch (result) {
	case CLUSTER_BUFMGR_PI_DISCARD_DROPPED:
		return CLUSTER_PI_DISCARD_ACTION_TERMINAL_DROPPED;
	case CLUSTER_BUFMGR_PI_DISCARD_TERMINAL_NOOP:
		return CLUSTER_PI_DISCARD_ACTION_TERMINAL_NOOP;
	case CLUSTER_BUFMGR_PI_DISCARD_RETRYABLE_BUSY:
		return CLUSTER_PI_DISCARD_ACTION_RETRY;
	case CLUSTER_BUFMGR_PI_DISCARD_CORRUPT:
		return CLUSTER_PI_DISCARD_ACTION_FAIL_CLOSED;
	}
	return CLUSTER_PI_DISCARD_ACTION_FAIL_CLOSED;
}

ClusterPiDiscardCommitPlan
cluster_pi_discard_commit_plan(ClusterBufmgrPiDiscardResult classification,
							   ClusterPcmOwnResult ownership_result)
{
	ClusterPiDiscardCommitPlan plan;

	memset(&plan, 0, sizeof(plan));
	plan.result = CLUSTER_BUFMGR_PI_DISCARD_CORRUPT;

	switch (classification)
	{
	case CLUSTER_BUFMGR_PI_DISCARD_TERMINAL_NOOP:
		plan.result = CLUSTER_BUFMGR_PI_DISCARD_TERMINAL_NOOP;
		return plan;
	case CLUSTER_BUFMGR_PI_DISCARD_RETRYABLE_BUSY:
		plan.result = CLUSTER_BUFMGR_PI_DISCARD_RETRYABLE_BUSY;
		return plan;
	case CLUSTER_BUFMGR_PI_DISCARD_CORRUPT:
		return plan;
	case CLUSTER_BUFMGR_PI_DISCARD_DROPPED:
		break;
	default:
		return plan;
	}

	if (ownership_result == CLUSTER_PCM_OWN_BUSY)
	{
		plan.result = CLUSTER_BUFMGR_PI_DISCARD_RETRYABLE_BUSY;
		return plan;
	}
	if (ownership_result != CLUSTER_PCM_OWN_OK)
		return plan;

	plan.result = CLUSTER_BUFMGR_PI_DISCARD_DROPPED;
	plan.clear_shadow = true;
	plan.run_common_tail = true;
	return plan;
}

ClusterPiDiscardParkOfferResult
cluster_pi_discard_park_offer(ClusterPiDiscardParkSlot *slots, Size slot_count,
							  BufferTag tag, uint64 epoch)
{
	Size		free_slot = slot_count;
	Size		i;

	if (slots == NULL || slot_count == 0)
		return CLUSTER_PI_DISCARD_PARK_FULL;

	for (i = 0; i < slot_count; i++)
	{
		if (slots[i].in_use)
		{
			if (BufferTagsEqual(&slots[i].tag, &tag))
			{
				slots[i].epoch = epoch;
				return CLUSTER_PI_DISCARD_PARK_UPDATED;
			}
		}
		else if (free_slot == slot_count)
			free_slot = i;
	}

	if (free_slot == slot_count)
		return CLUSTER_PI_DISCARD_PARK_FULL;

	slots[free_slot].tag = tag;
	slots[free_slot].epoch = epoch;
	slots[free_slot].in_use = true;
	return CLUSTER_PI_DISCARD_PARK_INSERTED;
}

#endif /* USE_PGRAC_CLUSTER */
