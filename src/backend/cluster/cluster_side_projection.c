/*-------------------------------------------------------------------------
 *
 * cluster_side_projection.c
 *	  RF-SIDE D-SIDE-04 — derived-projection judgements (implementation).
 *
 *	  Spec: specs/spec-rf-side-typed-recovery-and-shared-terminal.md
 *	  §1.2 D-SIDE-04 / §2.4 / §5.1 U-SIDE-08/09/10.
 *
 *	  Pure judgement; the projection store, invalidation triggers and
 *	  rebuild execution stay with the production cluster_remote_xact
 *	  wiring (RED).
 *
 * Portions Copyright (c) 1996-2024, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2026, pgrac contributors
 *
 * IDENTIFICATION
 *	  src/backend/cluster/cluster_side_projection.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "cluster/cluster_remote_xact.h"
#include "cluster/cluster_side_projection.h"

#define RF_SIDE_CLOG_XACTS_PER_PAGE ((uint32) BLCKSZ * 4)
#define RF_SIDE_COMMIT_TS_ENTRY_BYTES UINT32_C(10)
#define RF_SIDE_COMMIT_TS_XACTS_PER_PAGE \
	((uint32) BLCKSZ / RF_SIDE_COMMIT_TS_ENTRY_BYTES)

static bool
cluster_side_projection_zero_range(
	const ClusterSideProjectionApplyInputV1 *input,
	TransactionId *first_xid, uint32 *xid_count)
{
	const ClusterSideProjectionOperationV1 *operation = input->operation;
	uint32 per_page;
	uint64 first;
	uint64 remaining;

	if (operation->action != CLUSTER_SIDE_PROJECTION_ACTION_ZERO_PAGE ||
		operation->page_number < 0 || first_xid == NULL || xid_count == NULL)
		return false;
	if (operation->kind == CLUSTER_SIDE_PROJECTION_CLOG)
		per_page = RF_SIDE_CLOG_XACTS_PER_PAGE;
	else if (operation->kind == CLUSTER_SIDE_PROJECTION_COMMIT_TS)
		per_page = RF_SIDE_COMMIT_TS_XACTS_PER_PAGE;
	else
		return false;
	first = (uint64) (uint32) operation->page_number * per_page;
	if (first > UINT32_MAX)
		return false;
	remaining = (uint64) UINT32_MAX - first + 1;
	*first_xid = (TransactionId) first;
	*xid_count = (uint32) Min((uint64) per_page, remaining);
	return cluster_remote_xact_reset_range_valid_v2(
		input->origin_thread - 1, *first_xid, *xid_count);
}

ClusterSideProjectionApplyResultV1
cluster_side_projection_target_preflight_v1(
	const ClusterSideProjectionApplyInputV1 *input)
{
	const ClusterSideProjectionOperationV1 *operation;
	TransactionId first_xid;
	uint32 xid_count;

	if (input == NULL || input->operation == NULL || input->origin_thread == 0 ||
		input->origin_thread > (1 << 7))
		return CLUSTER_SIDE_PROJECTION_APPLY_BLOCKED;
	operation = input->operation;
	if (operation->kind == CLUSTER_SIDE_PROJECTION_MULTIXACT)
		return CLUSTER_SIDE_PROJECTION_APPLY_BLOCKED;
	if (operation->kind != CLUSTER_SIDE_PROJECTION_CLOG &&
		operation->kind != CLUSTER_SIDE_PROJECTION_COMMIT_TS)
		return CLUSTER_SIDE_PROJECTION_APPLY_BLOCKED;
	if (operation->kind == CLUSTER_SIDE_PROJECTION_COMMIT_TS &&
		!input->source_retained)
		return CLUSTER_SIDE_PROJECTION_APPLY_BLOCKED;
	if (operation->action == CLUSTER_SIDE_PROJECTION_ACTION_ZERO_PAGE)
		return cluster_side_projection_zero_range(input, &first_xid, &xid_count)
			? CLUSTER_SIDE_PROJECTION_APPLY_OK
			: CLUSTER_SIDE_PROJECTION_APPLY_BLOCKED;
	if (operation->action == CLUSTER_SIDE_PROJECTION_ACTION_TRUNCATE &&
		operation->page_number >= 0 &&
		TransactionIdIsNormal(operation->oldest_xid))
		return CLUSTER_SIDE_PROJECTION_APPLY_OK;
	return CLUSTER_SIDE_PROJECTION_APPLY_BLOCKED;
}

ClusterSideProjectionApplyResultV1
cluster_side_projection_apply_owned_v1(
	const ClusterSideProjectionApplyInputV1 *input,
	const ClusterSideProjectionApplyOpsV1 *ops)
{
	const ClusterSideProjectionOperationV1 *operation;
	TransactionId first_xid;
	uint32 xid_count;
	int origin_slot;

	if (cluster_side_projection_target_preflight_v1(input) !=
		CLUSTER_SIDE_PROJECTION_APPLY_OK || ops == NULL)
		return CLUSTER_SIDE_PROJECTION_APPLY_BLOCKED;
	operation = input->operation;
	origin_slot = input->origin_thread - 1;
	if (operation->action == CLUSTER_SIDE_PROJECTION_ACTION_ZERO_PAGE)
	{
		if (ops->reset_remote_xact_range == NULL ||
			ops->remote_xact_range_empty == NULL ||
			!cluster_side_projection_zero_range(input, &first_xid, &xid_count) ||
			!ops->reset_remote_xact_range(ops->arg, origin_slot,
				first_xid, xid_count))
			return CLUSTER_SIDE_PROJECTION_APPLY_BLOCKED;
		if (!ops->remote_xact_range_empty(ops->arg, origin_slot,
				first_xid, xid_count))
			return CLUSTER_SIDE_PROJECTION_APPLY_POST_READ_FAILED;
		return CLUSTER_SIDE_PROJECTION_APPLY_OK;
	}
	if (ops->truncate_remote_xact_before == NULL ||
		!ops->truncate_remote_xact_before(ops->arg, origin_slot,
			operation->oldest_xid))
		return CLUSTER_SIDE_PROJECTION_APPLY_BLOCKED;
	return CLUSTER_SIDE_PROJECTION_APPLY_OK;
}

bool
cluster_side_projection_verified(ClusterSideProjectionKind kind,
								 const ClusterSideProjectionVerifyInput *in)
{
	/*
	 * §2.4: every projection serves only behind the canonical producer
	 * bidirectional match + exact coverage + integrity.  "Durable" is
	 * never "authoritative": the caller re-checks these facts before
	 * every serve.  Any false closes the scope (U-SIDE-08: a local CLOG
	 * bit cannot override the canonical TT/redo truth; U-SIDE-09:
	 * empty/missing is never "no locker"; U-SIDE-10: a missing timestamp
	 * stays unknown, it cannot flip UNKNOWN to COMMITTED).
	 */
	if (in == NULL)
		return false;
	if (!in->canonical_truth_ok || !in->coverage_ok || !in->integrity_ok)
		return false;
	/* The kind itself only selects the contract; the facts are the same
	 * three.  A kind value out of range fails closed. */
	if (kind != CLUSTER_SIDE_PROJECTION_CLOG
		&& kind != CLUSTER_SIDE_PROJECTION_MULTIXACT
		&& kind != CLUSTER_SIDE_PROJECTION_COMMIT_TS)
		return false;
	return true;
}

bool
cluster_side_projection_rebuildable(ClusterSideProjectionKind kind,
									bool source_retained,
									bool canonical_producer_ok)
{
	/*
	 * §2.4 rebuild rule:
	 *   - CLOG rebuilds from the canonical transaction truth, so the
	 *     failed-origin redo need not be retained for the rebuild;
	 *   - MULTIXACT and COMMIT_TS rebuild from the retained redo — the
	 *     source must still be retained (U-SIDE-09: "在 source redo 仍
	 *     retained 时重建；retire 后若不存在独立可验证 canonical
	 *     source，相关 tuple/resource 保持 BLOCKED").
	 * The canonical producer identity/version/coverage check is required
	 * for every kind (a rebuild without a verified producer is a guess).
	 */
	if (!canonical_producer_ok)
		return false;
	switch (kind)
	{
		case CLUSTER_SIDE_PROJECTION_CLOG:
			return true;		/* canonical truth rebuild */
		case CLUSTER_SIDE_PROJECTION_MULTIXACT:
		case CLUSTER_SIDE_PROJECTION_COMMIT_TS:
			return source_retained;
	}
	return false;
}

ClusterSideProjectionLookup
cluster_side_projection_lookup(bool verified)
{
	/* §2.4 common rule 5: a miss/UNKNOWN fails closed until the rebuild
	 * completes — the judgement adds no synchronous network or durable
	 * I/O (pure function). */
	return verified ? CLUSTER_SIDE_PROJECTION_LOOKUP_OK
		: CLUSTER_SIDE_PROJECTION_LOOKUP_FAIL_CLOSED;
}
