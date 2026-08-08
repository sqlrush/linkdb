/*-------------------------------------------------------------------------
 *
 * cluster_tx_resolve.c
 *	  Exact transaction identity and outcome entry points for R4.
 *
 * Portions Copyright (c) 1996-2024, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2026, pgrac contributors
 *
 * IDENTIFICATION
 *	  src/backend/cluster/cluster_tx_resolve.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#ifdef USE_PGRAC_CLUSTER

#include "cluster/cluster_tx_resolve.h"

bool
cluster_tx_locator_from_itl(Page page, uint8 slot_index, ClusterTxLocator *out,
							ClusterTxResolveReason *reason_out)
{
	ClusterItlSlotData *slot;
	uint32 segment_id;
	uint32 block_no;
	uint16 tt_slot_offset;
	uint16 row_offset;
	bool data_slot;

	if (out != NULL)
		memset(out, 0, sizeof(*out));
	if (reason_out != NULL)
		*reason_out = CLUSTER_TX_RESOLVE_BAD_LOCATOR;

	if (page == NULL || out == NULL || !PageHasItl(page)
		|| PageGetSpecialSize(page) < CLUSTER_ITL_ARRAY_SIZE)
		return false;
	if (slot_index >= CLUSTER_ITL_INITRANS_DEFAULT) {
		if (reason_out != NULL)
			*reason_out = CLUSTER_TX_RESOLVE_SLOT_MISMATCH;
		return false;
	}

	slot = &ClusterPageGetItlSlots(page)[slot_index];
	data_slot = slot->flags == ITL_FLAG_ACTIVE || slot->flags == ITL_FLAG_COMMITTED
				|| slot->flags == ITL_FLAG_ABORTED || slot->flags == ITL_FLAG_NEEDS_CLEANOUT;
	if (!data_slot && !ITL_FLAG_IS_LOCK_ONLY(slot->flags))
		return false;
	if (!TransactionIdIsNormal(slot->xid)) {
		if (reason_out != NULL)
			*reason_out = CLUSTER_TX_RESOLVE_XID_MISMATCH;
		return false;
	}
	if (slot->wrap == TT_WRAP_INVALID) {
		if (reason_out != NULL)
			*reason_out = CLUSTER_TX_RESOLVE_WRAP_MISMATCH;
		return false;
	}
	if (!uba_decode(slot->undo_segment_head, &segment_id, &block_no, &tt_slot_offset,
					&row_offset)) {
		if (reason_out != NULL)
			*reason_out = CLUSTER_TX_RESOLVE_BAD_UBA;
		return false;
	}

	out->uba = slot->undo_segment_head;
	out->xid = slot->xid;
	out->tt_wrap = slot->wrap;
	out->itl_kind = slot->flags;
	out->itl_slot_index = slot_index;
	if (reason_out != NULL)
		*reason_out = CLUSTER_TX_RESOLVE_NONE;
	return true;
}

ClusterTxOutcome
cluster_tx_resolve_exact(const ClusterTxLocator *locator pg_attribute_unused(),
						 ClusterTxResolveMode mode pg_attribute_unused(), ClusterTxResolution *out,
						 ClusterTxResolveReason *reason_out)
{
	if (out != NULL)
		memset(out, 0, sizeof(*out));
	if (reason_out != NULL)
		*reason_out = CLUSTER_TX_RESOLVE_TARGET_DISABLED;
	return CLUSTER_TX_UNKNOWN;
}

ClusterTxOutcome
cluster_tx_resolve_multixact(MultiXactId mxid pg_attribute_unused(), ClusterMultiResolution *out,
							 ClusterTxResolveReason *reason_out)
{
	if (out != NULL)
		memset(out, 0, sizeof(*out));
	if (reason_out != NULL)
		*reason_out = CLUSTER_TX_RESOLVE_TARGET_DISABLED;
	return CLUSTER_TX_UNKNOWN;
}

const char *
cluster_tx_resolve_reason_name(ClusterTxResolveReason reason)
{
	switch (reason) {
	case CLUSTER_TX_RESOLVE_NONE:
		return "none";
	case CLUSTER_TX_RESOLVE_TARGET_DISABLED:
		return "target_disabled";
	case CLUSTER_TX_RESOLVE_RF_DEFERRED:
		return "rf_deferred";
	case CLUSTER_TX_RESOLVE_BAD_LOCATOR:
		return "bad_locator";
	case CLUSTER_TX_RESOLVE_BAD_UBA:
		return "bad_uba";
	case CLUSTER_TX_RESOLVE_XID_MISMATCH:
		return "xid_mismatch";
	case CLUSTER_TX_RESOLVE_WRAP_MISMATCH:
		return "wrap_mismatch";
	case CLUSTER_TX_RESOLVE_SLOT_MISMATCH:
		return "slot_mismatch";
	case CLUSTER_TX_RESOLVE_AUTHORITY_UNAVAILABLE:
		return "authority_unavailable";
	case CLUSTER_TX_RESOLVE_AUTHORITY_STALE:
		return "authority_stale";
	case CLUSTER_TX_RESOLVE_AUTHORITY_CONFLICT:
		return "authority_conflict";
	case CLUSTER_TX_RESOLVE_COVERAGE_GAP:
		return "coverage_gap";
	case CLUSTER_TX_RESOLVE_SUBTRANS_CHANGED:
		return "subtrans_changed";
	case CLUSTER_TX_RESOLVE_SUBTRANS_CYCLE:
		return "subtrans_cycle";
	case CLUSTER_TX_RESOLVE_SUBTRANS_DEPTH:
		return "subtrans_depth";
	case CLUSTER_TX_RESOLVE_TWOPHASE_CONFLICT:
		return "twophase_conflict";
	case CLUSTER_TX_RESOLVE_BAD_COMPOSITION:
		return "bad_composition";
	case CLUSTER_TX_RESOLVE_COMPOSITION_CHANGED:
		return "composition_changed";
	case CLUSTER_TX_RESOLVE_HORIZON_RECYCLED:
		return "horizon_recycled";
	case CLUSTER_TX_RESOLVE_HOLDER_MOVED:
		return "holder_moved";
	case CLUSTER_TX_RESOLVE_CAPACITY:
		return "capacity";
	case CLUSTER_TX_RESOLVE_TIMEOUT:
		return "timeout";
	case CLUSTER_TX_RESOLVE_CANCELLED:
		return "cancelled";
	case CLUSTER_TX_RESOLVE_REENTRANT:
		return "reentrant";
	case CLUSTER_TX_RESOLVE_IO_ERROR:
		return "io_error";
	case CLUSTER_TX_RESOLVE_PROTOCOL:
		return "protocol";
	default:
		return "invalid_reason";
	}
}

#endif /* USE_PGRAC_CLUSTER */
