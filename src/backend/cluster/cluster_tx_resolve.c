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

#include "cluster/cluster_epoch.h"
#include "cluster/cluster_semantic_activation.h"
#include "cluster/cluster_tx_resolve.h"

static bool
cluster_tx_locator_is_well_formed(const ClusterTxLocator *locator,
								  ClusterTxResolveReason *reason_out)
{
	uint32 segment_id;
	uint32 block_no;
	uint16 tt_slot_offset;
	uint16 row_offset;
	bool data_kind;

	if (reason_out != NULL)
		*reason_out = CLUSTER_TX_RESOLVE_BAD_LOCATOR;
	if (locator == NULL)
		return false;

	data_kind = locator->itl_kind == ITL_FLAG_ACTIVE
				|| locator->itl_kind == ITL_FLAG_COMMITTED
				|| locator->itl_kind == ITL_FLAG_ABORTED
				|| locator->itl_kind == ITL_FLAG_NEEDS_CLEANOUT;
	if (locator->itl_slot_index >= CLUSTER_ITL_INITRANS_DEFAULT
		|| (!data_kind && !ITL_FLAG_IS_LOCK_ONLY(locator->itl_kind)))
		return false;
	if (!TransactionIdIsNormal(locator->xid)) {
		if (reason_out != NULL)
			*reason_out = CLUSTER_TX_RESOLVE_XID_MISMATCH;
		return false;
	}
	if (locator->tt_wrap == TT_WRAP_INVALID) {
		if (reason_out != NULL)
			*reason_out = CLUSTER_TX_RESOLVE_WRAP_MISMATCH;
		return false;
	}
	if (!uba_decode(locator->uba, &segment_id, &block_no, &tt_slot_offset, &row_offset)
		|| block_no == 0) {
		if (reason_out != NULL)
			*reason_out = CLUSTER_TX_RESOLVE_BAD_UBA;
		return false;
	}

	if (reason_out != NULL)
		*reason_out = CLUSTER_TX_RESOLVE_NONE;
	return true;
}

static bool
cluster_tx_locator_equal(const ClusterTxLocator *left, const ClusterTxLocator *right)
{
	return left->uba.raw[0] == right->uba.raw[0] && left->uba.raw[1] == right->uba.raw[1]
		   && left->xid == right->xid && left->tt_wrap == right->tt_wrap
		   && left->itl_kind == right->itl_kind
		   && left->itl_slot_index == right->itl_slot_index;
}

static bool
cluster_tx_resolve_reason_is_known(ClusterTxResolveReason reason)
{
	return (unsigned int)reason <= (unsigned int)CLUSTER_TX_RESOLVE_PROTOCOL;
}

static bool
cluster_tx_resolution_is_publishable(const ClusterTxLocator *locator, ClusterTxResolveMode mode,
									 uint64 formation_epoch, ClusterTxOutcome returned,
									 const ClusterTxResolution *resolution,
									 ClusterTxResolveReason provider_reason)
{
	if (returned == CLUSTER_TX_UNKNOWN || returned != resolution->outcome
		|| provider_reason != CLUSTER_TX_RESOLVE_NONE
		|| !cluster_tx_locator_equal(locator, &resolution->locator_echo)
		|| !TransactionIdIsNormal(resolution->top_xid)
		|| !cluster_tx_outcome_proof_is_valid(resolution->outcome, resolution->proof_kind)
		|| resolution->authority.origin_epoch != formation_epoch
		|| !SCN_VALID(resolution->authority.authority_scn))
		return false;

	if (resolution->outcome == CLUSTER_TX_COMMITTED) {
		if (!SCN_VALID(resolution->commit_scn))
			return false;
	} else if (SCN_VALID(resolution->commit_scn))
		return false;

	/* CLEANOUT may publish only an irreversible terminal result. */
	if (mode == CLUSTER_TX_RESOLVE_CLEANOUT_HINT
		&& resolution->outcome != CLUSTER_TX_COMMITTED
		&& resolution->outcome != CLUSTER_TX_ABORTED)
		return false;

	return true;
}

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
cluster_tx_resolve_exact(const ClusterTxLocator *locator, ClusterTxResolveMode mode,
						 ClusterTxResolution *out,
						 ClusterTxResolveReason *reason_out)
{
	ClusterSemanticAdmissionToken admission;
	ClusterSemanticAdmissionResult admission_result;
	ClusterTxResolution candidate;
	ClusterTxResolveReason reason = CLUSTER_TX_RESOLVE_TARGET_DISABLED;
	ClusterTxResolveReason provider_reason = CLUSTER_TX_RESOLVE_NONE;
	ClusterTxOutcome outcome = CLUSTER_TX_UNKNOWN;
	uint64 formation_epoch;

	if (out != NULL)
		memset(out, 0, sizeof(*out));
	if (reason_out != NULL)
		*reason_out = CLUSTER_TX_RESOLVE_TARGET_DISABLED;
	memset(&admission, 0, sizeof(admission));
	memset(&candidate, 0, sizeof(candidate));

	admission_result = cluster_semantic_activation_enter(
		CLUSTER_SEMANTIC_FEATURE_R4_SYNC_CR_V1, CLUSTER_SEMANTIC_TARGET_SIDE, &admission);
	if (admission_result != CLUSTER_SEMANTIC_ADMISSION_OK) {
		reason = admission_result == CLUSTER_SEMANTIC_ADMISSION_TARGET_DISABLED
				 ? CLUSTER_TX_RESOLVE_TARGET_DISABLED
				 : CLUSTER_TX_RESOLVE_RF_DEFERRED;
		goto done;
	}

	if (out == NULL || (unsigned int)mode > (unsigned int)CLUSTER_TX_RESOLVE_CLEANOUT_HINT) {
		reason = CLUSTER_TX_RESOLVE_PROTOCOL;
		goto admitted_done;
	}
	if (!cluster_tx_locator_is_well_formed(locator, &reason))
		goto admitted_done;

	formation_epoch = cluster_epoch_get_current();
	outcome = cluster_runtime_visibility_resolve_exact_origin(
		locator, mode, formation_epoch, &candidate, &provider_reason);
	if (outcome == CLUSTER_TX_UNKNOWN) {
		reason = provider_reason == CLUSTER_TX_RESOLVE_NONE
					 || !cluster_tx_resolve_reason_is_known(provider_reason)
				 ? CLUSTER_TX_RESOLVE_PROTOCOL
				 : provider_reason;
		goto admitted_done;
	}
	if (!cluster_tx_resolution_is_publishable(locator, mode, formation_epoch, outcome, &candidate,
										  provider_reason)) {
		outcome = CLUSTER_TX_UNKNOWN;
		reason = CLUSTER_TX_RESOLVE_PROTOCOL;
		goto admitted_done;
	}
	if (cluster_epoch_get_current() != formation_epoch
		|| !cluster_semantic_activation_recheck(&admission)) {
		outcome = CLUSTER_TX_UNKNOWN;
		reason = CLUSTER_TX_RESOLVE_RF_DEFERRED;
		goto admitted_done;
	}

	*out = candidate;
	reason = CLUSTER_TX_RESOLVE_NONE;

admitted_done:
	cluster_semantic_activation_leave(&admission);
done:
	if (reason_out != NULL)
		*reason_out = reason;
	return outcome;
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
