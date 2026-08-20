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

#include "cluster/cluster_side_projection.h"

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
