/*-------------------------------------------------------------------------
 *
 * cluster_cr_server_policy.c
 *	  pgrac spec-6.12b — pure split policy for the CR-server (no shmem, no
 *	  locks, no elog) so cluster_unit exercises every branch standalone.
 *
 *	  See cluster_cr_server.h for the FULL / PARTIAL / DENY contract and
 *	  why an interleaved home order must refuse (write_scn-DESC peel
 *	  ordering across chains is a correctness invariant, spec-3.10 Q10).
 *
 * Portions Copyright (c) 1996-2024, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2026, pgrac contributors
 *
 * Author: SqlRush <sqlrush@gmail.com>
 *
 * IDENTIFICATION
 *	  src/backend/cluster/cluster_cr_server_policy.c
 *
 * NOTES
 *	  This is a pgrac-original file.  Compiled only in --enable-cluster
 *	  builds.
 *	  Spec: spec-6.12-crossnode-cache-fusion-perf-optimization.md (wave b)
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#ifdef USE_PGRAC_CLUSTER

#include "cluster/cluster_cr_server.h"
#include "cluster/cluster_tt_slot.h"

ClusterCrServerSplit
cluster_cr_server_split_classify(const int32 *chain_origins, int nchains, int32 self_node,
								 int *out_prefix_len)
{
	int prefix = 0;

	if (out_prefix_len != NULL)
		*out_prefix_len = 0;
	if (nchains < 0 || (nchains > 0 && chain_origins == NULL))
		return CLUSTER_CR_SPLIT_DENY; /* malformed input: fail closed */

	/* Leading self-home run = the peelable DESC prefix. */
	while (prefix < nchains && chain_origins[prefix] == self_node)
		prefix++;

	/* Any self-home chain AFTER a foreign one = interleave = refuse. */
	for (int i = prefix; i < nchains; i++) {
		if (chain_origins[i] == self_node)
			return CLUSTER_CR_SPLIT_DENY;
	}

	if (out_prefix_len != NULL)
		*out_prefix_len = prefix;
	return (prefix == nchains) ? CLUSTER_CR_SPLIT_FULL : CLUSTER_CR_SPLIT_PARTIAL;
}

/*
 * cluster_cr_server_invalid_scn_verdict — spec-7.1 D1 serve pure decision.
 *
 *	The origin's durable by-xid scan matched our own xid but the slot carries
 *	no stamped commit_scn (the delayed-cleanout window: XID_MATCH_INVALID_SCN).
 *	Per IN-5 the real population is aborted-unstamped -- an abort writes no
 *	durable commit_scn -- so cross-checking CLOG lets us answer a provably
 *	ABORTED xid positively (invisible at the requester) instead of 53R97.
 *
 *	8.A (positive proof only): ONLY an explicit CLOG abort upgrades.  A
 *	committed-but-unstamped xid (we must never fabricate its commit_scn), an
 *	in-flight / 2PC-prepared / crashed-without-abort-record xid -- for all of
 *	which TransactionIdDidAbort is false -- stays REFUSE (fail-closed, the
 *	refuse direction is unchanged).
 */
ClusterCrInvalidScnVerdict
cluster_cr_server_invalid_scn_verdict(bool clog_did_abort)
{
	return clog_did_abort ? CLUSTER_CR_INVALID_SCN_ABORTED : CLUSTER_CR_INVALID_SCN_REFUSE;
}

ClusterUndoVerdictKind
cluster_cr_server_resolved_scn_verdict(bool clog_did_commit, bool clog_did_abort,
									   bool xid_is_in_progress)
{
	if (clog_did_commit && clog_did_abort)
		return CLUSTER_UNDO_VERDICT_UNKNOWN_FAIL_CLOSED;
	if (clog_did_commit)
		return CLUSTER_UNDO_VERDICT_COMMITTED_EXACT;
	if (clog_did_abort)
		return CLUSTER_UNDO_VERDICT_ABORTED;
	if (xid_is_in_progress)
		return CLUSTER_UNDO_VERDICT_IN_PROGRESS;
	return CLUSTER_UNDO_VERDICT_UNKNOWN_FAIL_CLOSED;
}

ClusterUndoVerdictKind
cluster_cr_server_resolved_scn_resampled_verdict(bool clog_did_commit_before,
												 bool clog_did_abort_before,
												 bool xid_is_in_progress,
												 bool clog_did_commit_after,
												 bool clog_did_abort_after)
{
	return cluster_cr_server_resolved_scn_verdict(
		clog_did_commit_before || clog_did_commit_after,
		clog_did_abort_before || clog_did_abort_after,
		xid_is_in_progress);
}

bool
cluster_cr_server_terminal_resample_allowed(bool allow_live, bool exact_binding,
											bool clog_did_commit_before,
											bool clog_did_abort_before,
											bool xid_is_in_progress)
{
	return allow_live && exact_binding && !clog_did_commit_before
		   && !clog_did_abort_before && !xid_is_in_progress;
}

bool
cluster_cr_server_live_binding_exact(bool xid_is_mine,
									 uint32 expected_segment_id,
									 uint32 expected_tt_slot_id,
									 uint16 matched_segment,
									 uint16 matched_slot,
									 bool xid_is_in_progress,
									 bool durable_binding_stable)
{
	return xid_is_mine && expected_segment_id > 0
		   && expected_segment_id <= UINT16_MAX
		   && expected_segment_id == (uint32)matched_segment
		   && expected_tt_slot_id >= 1
		   && expected_tt_slot_id <= TT_SLOTS_PER_SEGMENT
		   && expected_tt_slot_id == (uint32)matched_slot + 1
		   && xid_is_in_progress && durable_binding_stable;
}

ClusterCrServerExactDiagnostic
cluster_cr_server_exact_diagnostic(bool allow_live, bool xid_is_mine,
								   uint32 expected_segment_id,
								   uint32 expected_tt_slot_id,
								   uint16 matched_segment,
								   uint16 matched_slot)
{
	if (!allow_live)
		return CLUSTER_CR_SERVER_EXACT_NOT_AUTHORITATIVE;
	if (!xid_is_mine)
		return CLUSTER_CR_SERVER_EXACT_NOT_MINE;
	if (expected_segment_id == 0 || expected_segment_id > UINT16_MAX)
		return CLUSTER_CR_SERVER_EXACT_EXPECTED_SEGMENT_INVALID;
	if (expected_tt_slot_id < 1 || expected_tt_slot_id > TT_SLOTS_PER_SEGMENT)
		return CLUSTER_CR_SERVER_EXACT_EXPECTED_SLOT_INVALID;
	if (expected_segment_id != (uint32)matched_segment)
		return CLUSTER_CR_SERVER_EXACT_SEGMENT_MISMATCH;
	if (expected_tt_slot_id != (uint32)matched_slot + 1)
		return CLUSTER_CR_SERVER_EXACT_SLOT_MISMATCH;
	return CLUSTER_CR_SERVER_EXACT_OK;
}

ClusterCrServerConfirmDiagnostic
cluster_cr_server_confirm_diagnostic(bool confirm_resolved_scn,
									 uint16 matched_segment,
									 uint16 matched_slot,
									 uint16 matched_wrap,
									 SCN matched_scn,
									 uint16 confirm_segment,
									 uint16 confirm_slot,
									 uint16 confirm_wrap,
									 SCN confirm_scn)
{
	if (!confirm_resolved_scn)
		return CLUSTER_CR_SERVER_CONFIRM_RESOLVE_KIND;
	if (confirm_segment != matched_segment)
		return CLUSTER_CR_SERVER_CONFIRM_SEGMENT_MISMATCH;
	if (confirm_slot != matched_slot)
		return CLUSTER_CR_SERVER_CONFIRM_SLOT_MISMATCH;
	if (confirm_wrap != matched_wrap)
		return CLUSTER_CR_SERVER_CONFIRM_WRAP_MISMATCH;
	if (confirm_scn != matched_scn)
		return CLUSTER_CR_SERVER_CONFIRM_SCN_MISMATCH;
	return CLUSTER_CR_SERVER_CONFIRM_STABLE;
}

ClusterCrServerOtherRefusalDetail
cluster_cr_server_other_refusal_detail_normalize(
	ClusterCrServerOtherRefusalDetail detail)
{
	Assert((int)detail >= 0
		   && detail < CLUSTER_CR_SERVER_OTHER_DETAIL_COUNT);
	if ((int)detail < 0
		|| detail >= CLUSTER_CR_SERVER_OTHER_DETAIL_COUNT)
		return CLUSTER_CR_SERVER_OTHER_RESIDUAL;
	return detail;
}

#endif /* USE_PGRAC_CLUSTER */
