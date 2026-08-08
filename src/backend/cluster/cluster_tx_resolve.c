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
cluster_tx_locator_from_itl(Page page pg_attribute_unused(), uint8 slot_index pg_attribute_unused(),
							ClusterTxLocator *out, ClusterTxResolveReason *reason_out)
{
	if (out != NULL)
		memset(out, 0, sizeof(*out));
	if (reason_out != NULL)
		*reason_out = CLUSTER_TX_RESOLVE_BAD_LOCATOR;
	return false;
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
