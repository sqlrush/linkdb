/*-------------------------------------------------------------------------
 *
 * cluster_pi_discard_policy.h
 *	  Pure, typed policy for bounded Past Image discard.
 *
 *-------------------------------------------------------------------------
 */
#ifndef CLUSTER_PI_DISCARD_POLICY_H
#define CLUSTER_PI_DISCARD_POLICY_H

#include "c.h"
#include "cluster/cluster_pcm_own.h"
#include "storage/buf_internals.h"

typedef enum ClusterBufmgrPiDiscardResult {
	CLUSTER_BUFMGR_PI_DISCARD_DROPPED = 0,
	CLUSTER_BUFMGR_PI_DISCARD_TERMINAL_NOOP,
	CLUSTER_BUFMGR_PI_DISCARD_RETRYABLE_BUSY,
	CLUSTER_BUFMGR_PI_DISCARD_CORRUPT,
} ClusterBufmgrPiDiscardResult;

typedef struct ClusterPiDiscardShape {
	bool tag_matches;
	bool is_current;
	bool is_pi;
	bool bm_valid;
	uint32 refcount;
	bool pcm_is_n;
	ClusterPcmOwnResult own_shape;
	uint32 own_flags;
	uint64 writer_activation_token;
} ClusterPiDiscardShape;

typedef enum ClusterPiDiscardAction {
	CLUSTER_PI_DISCARD_ACTION_TERMINAL_DROPPED = 0,
	CLUSTER_PI_DISCARD_ACTION_TERMINAL_NOOP,
	CLUSTER_PI_DISCARD_ACTION_RETRY,
	CLUSTER_PI_DISCARD_ACTION_FAIL_CLOSED,
} ClusterPiDiscardAction;

typedef struct ClusterPiDiscardCommitPlan {
	ClusterBufmgrPiDiscardResult result;
	bool clear_shadow;
	bool run_common_tail;
} ClusterPiDiscardCommitPlan;

typedef struct ClusterPiDiscardParkSlot {
	bool in_use;
	BufferTag tag;
	uint64 epoch;
} ClusterPiDiscardParkSlot;

typedef enum ClusterPiDiscardParkOfferResult {
	CLUSTER_PI_DISCARD_PARK_INSERTED = 0,
	CLUSTER_PI_DISCARD_PARK_UPDATED,
	CLUSTER_PI_DISCARD_PARK_FULL,
} ClusterPiDiscardParkOfferResult;

extern ClusterBufmgrPiDiscardResult
cluster_pi_discard_classify_locked(const ClusterPiDiscardShape *shape);
extern ClusterPiDiscardAction
cluster_pi_discard_action(ClusterBufmgrPiDiscardResult result);
extern ClusterPiDiscardCommitPlan
cluster_pi_discard_commit_plan(ClusterBufmgrPiDiscardResult classification,
							   ClusterPcmOwnResult ownership_result);
extern ClusterPiDiscardParkOfferResult
cluster_pi_discard_park_offer(ClusterPiDiscardParkSlot *slots, Size slot_count,
							  BufferTag tag, uint64 epoch);

#endif /* CLUSTER_PI_DISCARD_POLICY_H */
