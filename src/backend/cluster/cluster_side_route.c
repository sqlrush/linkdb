/*-------------------------------------------------------------------------
 *
 * cluster_side_route.c
 *	  RF-SIDE D-SIDE-01 — total route registry + §2.1 verdicts
 *	  (implementation).
 *
 *	  Spec: specs/spec-rf-side-typed-recovery-and-shared-terminal.md
 *	  §1.2 D-SIDE-01 / §2.1 / §3.2 / §5.1 U-SIDE-01..03.
 *
 *	  The registry mirrors the §3.2 routing matrix: opcode-granular rows
 *	  for the matrix-named opcodes, rmgr-granular rows otherwise, and no
 *	  row at all for unknown rmgrs (lookup fails -> the caller treats it
 *	  as BLOCKED).  The verdict function is pure, so cold and online
 *	  wrappers always agree on route + verdict for the same record.
 *
 * Portions Copyright (c) 1996-2024, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2026, pgrac contributors
 *
 * IDENTIFICATION
 *	  src/backend/cluster/cluster_side_route.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#ifdef USE_PGRAC_CLUSTER

#include "access/clog.h"
#include "access/commit_ts.h"
#include "access/multixact.h"
#include "access/rmgr.h"		/* RM_* IDs */
#include "access/xact.h"
#include "catalog/pg_control.h" /* XLOG_* opcodes */
#include "catalog/storage_xlog.h"	/* XLOG_SMGR_* */
#include "cluster/cluster_adg_xlog.h"
#include "cluster/cluster_side_route.h"
#include "cluster/cluster_xid_stripe_xlog.h"
#include "cluster/storage/cluster_undo_xlog.h"
#include "commands/dbcommands_xlog.h" /* XLOG_DBASE_* (unused rows) */
#include "replication/message.h"	/* XLOG_LOGICAL_MESSAGE */
#include "replication/origin.h" /* XLOG_REPLORIGIN_* */
#include "storage/standbydefs.h"	/* XLOG_STANDBY_* */
#include "utils/relmapper.h"	/* XLOG_RELMAP_UPDATE */

/*
 * §3.2 route rows.  Opcode-granular rows use the exact matrix-named
 * opcodes; rmgr-granular rows (opcode 0, mask 0) cover the whole rmgr.
 * The 132/132 opcode census mechanically generated from the exact object
 * headers is the U-SIDE-01 G1 work; the rows below are the matrix-named
 * set and every unknown combination fails closed via lookup()==false.
 */
static const ClusterSideRouteRow cluster_side_route_table[] = {
	/* ---- RM_XLOG_ID: page / control-only no-op / BLOCKED control ---- */
	{ RM_XLOG_ID, XLOG_FPI, 0, CLUSTER_SIDE_ROUTE_PAGE, NULL },
	{ RM_XLOG_ID, XLOG_FPI_FOR_HINT, 0, CLUSTER_SIDE_ROUTE_PAGE, NULL },
	{ RM_XLOG_ID, XLOG_NOOP, 0, CLUSTER_SIDE_ROUTE_PROVED_NOOP, "CONTROL_ONLY" },
	{ RM_XLOG_ID, XLOG_SWITCH, 0, CLUSTER_SIDE_ROUTE_PROVED_NOOP, "CONTROL_ONLY" },
	{ RM_XLOG_ID, XLOG_RESTORE_POINT, 0, CLUSTER_SIDE_ROUTE_PROVED_NOOP, "CONTROL_ONLY" },
	{ RM_XLOG_ID, XLOG_BACKUP_END, 0, CLUSTER_SIDE_ROUTE_PROVED_NOOP, "CONTROL_ONLY" },
	{ RM_XLOG_ID, XLOG_CHECKPOINT_SHUTDOWN, 0, CLUSTER_SIDE_ROUTE_BLOCKED, NULL },
	{ RM_XLOG_ID, XLOG_CHECKPOINT_ONLINE, 0, CLUSTER_SIDE_ROUTE_BLOCKED, NULL },
	{ RM_XLOG_ID, XLOG_NEXTOID, 0, CLUSTER_SIDE_ROUTE_BLOCKED, NULL },
	{ RM_XLOG_ID, XLOG_PARAMETER_CHANGE, 0, CLUSTER_SIDE_ROUTE_BLOCKED, NULL },
	{ RM_XLOG_ID, XLOG_FPW_CHANGE, 0, CLUSTER_SIDE_ROUTE_BLOCKED, NULL },
	{ RM_XLOG_ID, XLOG_END_OF_RECOVERY, 0, CLUSTER_SIDE_ROUTE_BLOCKED, NULL },
	{ RM_XLOG_ID, XLOG_OVERWRITE_CONTRECORD, 0, CLUSTER_SIDE_ROUTE_BLOCKED, NULL },
	/* ---- RM_XACT_ID: TT/undo truth; the HAS_INFO modifier bit rides a
	 * mask so COMMIT and COMMIT|HAS_INFO are the same row (§3.2). ---- */
	{ RM_XACT_ID, XLOG_XACT_COMMIT, XLOG_XACT_HAS_INFO, CLUSTER_SIDE_ROUTE_TT_UNDO, NULL },
	{ RM_XACT_ID, XLOG_XACT_ABORT, XLOG_XACT_HAS_INFO, CLUSTER_SIDE_ROUTE_TT_UNDO, NULL },
	{ RM_XACT_ID, XLOG_XACT_PREPARE, 0, CLUSTER_SIDE_ROUTE_TT_UNDO, NULL },
	{ RM_XACT_ID, XLOG_XACT_COMMIT_PREPARED, 0, CLUSTER_SIDE_ROUTE_TT_UNDO, NULL },
	{ RM_XACT_ID, XLOG_XACT_ABORT_PREPARED, 0, CLUSTER_SIDE_ROUTE_TT_UNDO, NULL },
	{ RM_XACT_ID, XLOG_XACT_ASSIGNMENT, 0, CLUSTER_SIDE_ROUTE_TT_UNDO, NULL },
	{ RM_XACT_ID, XLOG_XACT_INVALIDATIONS, 0, CLUSTER_SIDE_ROUTE_BLOCKED, NULL },
	/* ---- RM_SMGR_ID: canonical storage lifecycle ---- */
	{ RM_SMGR_ID, XLOG_SMGR_CREATE, 0, CLUSTER_SIDE_ROUTE_STORAGE, NULL },
	{ RM_SMGR_ID, XLOG_SMGR_TRUNCATE, 0, CLUSTER_SIDE_ROUTE_STORAGE, NULL },
	/* ---- RM_CLOG_ID: derived projection ---- */
	{ RM_CLOG_ID, CLOG_ZEROPAGE, 0, CLUSTER_SIDE_ROUTE_PROJECTION, NULL },
	{ RM_CLOG_ID, CLOG_TRUNCATE, 0, CLUSTER_SIDE_ROUTE_PROJECTION, NULL },
	/* ---- RM_DBASE_ID / RM_TBLSPC_ID: shared-directory authority not
	 * owned by SIDE -> BLOCKED (no raw survivor-local apply). ---- */
	{ RM_DBASE_ID, 0, 0xFFFF, CLUSTER_SIDE_ROUTE_BLOCKED, NULL },
	{ RM_TBLSPC_ID, 0, 0xFFFF, CLUSTER_SIDE_ROUTE_BLOCKED, NULL },
	/* ---- RM_MULTIXACT_ID: derived projection ---- */
	{ RM_MULTIXACT_ID, XLOG_MULTIXACT_ZERO_OFF_PAGE, 0, CLUSTER_SIDE_ROUTE_PROJECTION, NULL },
	{ RM_MULTIXACT_ID, XLOG_MULTIXACT_ZERO_MEM_PAGE, 0, CLUSTER_SIDE_ROUTE_PROJECTION, NULL },
	{ RM_MULTIXACT_ID, XLOG_MULTIXACT_CREATE_ID, 0, CLUSTER_SIDE_ROUTE_PROJECTION, NULL },
	{ RM_MULTIXACT_ID, XLOG_MULTIXACT_TRUNCATE_ID, 0, CLUSTER_SIDE_ROUTE_PROJECTION, NULL },
	/* ---- RM_RELMAP_ID: existing relmap authority, not a SIDE domain
	 * -> BLOCKED here (the canonical owner handles it). ---- */
	{ RM_RELMAP_ID, 0, 0xFFFF, CLUSTER_SIDE_ROUTE_BLOCKED, NULL },
	/* ---- RM_STANDBY_ID ---- */
	{ RM_STANDBY_ID, XLOG_STANDBY_LOCK, 0, CLUSTER_SIDE_ROUTE_PROVED_NOOP,
	  "PRIMARY_NO_STANDBY_CONSUMER" },
	{ RM_STANDBY_ID, XLOG_RUNNING_XACTS, 0, CLUSTER_SIDE_ROUTE_PROVED_NOOP,
	  "PRIMARY_NO_STANDBY_CONSUMER" },
	{ RM_STANDBY_ID, XLOG_INVALIDATIONS, 0, CLUSTER_SIDE_ROUTE_BLOCKED, NULL },
	/* ---- Page-family rmgrs: route PAGE (RF-PAGE owns the mutation). ---- */
	{ RM_HEAP2_ID, 0, 0xFFFF, CLUSTER_SIDE_ROUTE_PAGE, NULL },
	{ RM_HEAP_ID, 0, 0xFFFF, CLUSTER_SIDE_ROUTE_PAGE, NULL },
	{ RM_BTREE_ID, 0, 0xFFFF, CLUSTER_SIDE_ROUTE_PAGE, NULL },
	{ RM_HASH_ID, 0, 0xFFFF, CLUSTER_SIDE_ROUTE_PAGE, NULL },
	{ RM_GIN_ID, 0, 0xFFFF, CLUSTER_SIDE_ROUTE_PAGE, NULL },
	{ RM_GIST_ID, 0, 0xFFFF, CLUSTER_SIDE_ROUTE_PAGE, NULL },
	{ RM_SEQ_ID, 0, 0xFFFF, CLUSTER_SIDE_ROUTE_PAGE, NULL },
	{ RM_SPGIST_ID, 0, 0xFFFF, CLUSTER_SIDE_ROUTE_PAGE, NULL },
	{ RM_BRIN_ID, 0, 0xFFFF, CLUSTER_SIDE_ROUTE_PAGE, NULL },
	{ RM_GENERIC_ID, 0, 0xFFFF, CLUSTER_SIDE_ROUTE_PAGE, NULL },
	/* ---- RM_COMMIT_TS_ID: derived projection ---- */
	{ RM_COMMIT_TS_ID, COMMIT_TS_ZEROPAGE, 0, CLUSTER_SIDE_ROUTE_PROJECTION, NULL },
	{ RM_COMMIT_TS_ID, COMMIT_TS_TRUNCATE, 0, CLUSTER_SIDE_ROUTE_PROJECTION, NULL },
	/* ---- RM_REPLORIGIN_ID: origin authority not safely aliased -> BLOCKED. ---- */
	{ RM_REPLORIGIN_ID, 0, 0xFFFF, CLUSTER_SIDE_ROUTE_BLOCKED, NULL },
	/* ---- RM_LOGICALMSG_ID: physical-only context ---- */
	{ RM_LOGICALMSG_ID, 0, 0xFFFF, CLUSTER_SIDE_ROUTE_PROVED_NOOP,
	  "LOGICAL_MESSAGE_ONLY" },
	/* ---- RM_CLUSTER_UNDO_ID: shared undo/TT truth; HWM stays BLOCKED
	 * under STOP-RF-SIDE-SPACE-ABI. ---- */
	{ RM_CLUSTER_UNDO_ID, XLOG_UNDO_SEGMENT_INIT, 0, CLUSTER_SIDE_ROUTE_TT_UNDO, NULL },
	{ RM_CLUSTER_UNDO_ID, XLOG_UNDO_TT_SLOT_COMMIT, 0, CLUSTER_SIDE_ROUTE_TT_UNDO, NULL },
	{ RM_CLUSTER_UNDO_ID, XLOG_UNDO_TT_SLOT_ABORT, 0, CLUSTER_SIDE_ROUTE_TT_UNDO, NULL },
	{ RM_CLUSTER_UNDO_ID, XLOG_UNDO_TT_SLOT_SET_HEAD, 0, CLUSTER_SIDE_ROUTE_TT_UNDO, NULL },
	{ RM_CLUSTER_UNDO_ID, XLOG_UNDO_SEGMENT_RECYCLE, 0, CLUSTER_SIDE_ROUTE_TT_UNDO, NULL },
	{ RM_CLUSTER_UNDO_ID, XLOG_UNDO_SEGMENT_REUSE, 0, CLUSTER_SIDE_ROUTE_TT_UNDO, NULL },
	{ RM_CLUSTER_UNDO_ID, XLOG_UNDO_BLOCK_WRITE, 0, CLUSTER_SIDE_ROUTE_TT_UNDO, NULL },
	{ RM_CLUSTER_UNDO_ID, XLOG_UNDO_BLOCK_WRITE_MULTI, 0, CLUSTER_SIDE_ROUTE_TT_UNDO, NULL },
	{ RM_CLUSTER_UNDO_ID, XLOG_HW_RESERVE, 0, CLUSTER_SIDE_ROUTE_BLOCKED, NULL },
	/* ---- Other PGRAC rmgrs: owners outside SIDE -> BLOCKED. ---- */
	{ RM_CLUSTER_RAW_LAYOUT_ID, 0, 0xFFFF, CLUSTER_SIDE_ROUTE_BLOCKED, NULL },
	{ RM_CLUSTER_ADG_ID, 0, 0xFFFF, CLUSTER_SIDE_ROUTE_BLOCKED, NULL },
	{ RM_CLUSTER_XID_STRIPE_ID, 0, 0xFFFF, CLUSTER_SIDE_ROUTE_BLOCKED, NULL },
};

bool
cluster_side_route_lookup(uint8 rmid, uint16 opcode, ClusterSideRouteRow *out)
{
	int			i;

	if (out == NULL)
		return false;
	memset(out, 0, sizeof(*out));
	for (i = 0; i < (int) lengthof(cluster_side_route_table); i++) {
		const ClusterSideRouteRow *row = &cluster_side_route_table[i];

		if (row->rmid != rmid)
			continue;
		if (row->opcode_mask != 0) {
			/* mask = ignored bits (e.g. the XACT HAS_INFO modifier):
			 * all OTHER bits must match exactly.  A mask of 0xFFFF is
			 * the rmgr-granular row: every opcode of this rmgr matches. */
			if ((opcode & ~row->opcode_mask) != (row->opcode & ~row->opcode_mask))
				continue;
		} else {
			/* Exact opcode match — including opcode 0 (a real opcode,
			 * e.g. XLOG_CHECKPOINT_SHUTDOWN): an exact row must never
			 * fall through as a wildcard. */
			if (row->opcode != opcode)
				continue;
		}
		*out = *row;
		return true;
	}
	return false;				/* unknown rmgr/opcode: BLOCKED by default */
}

ClusterSideRouteVerdict
cluster_side_route_verdict(const ClusterSideRouteRow *row)
{
	/* Pure: cold and online wrappers always agree (U-SIDE-02).  A
	 * PROVED_NOOP row carries its proof kind; every other row either
	 * applies under its owning primitive or is BLOCKED. */
	if (row == NULL)
		return CLUSTER_SIDE_ROUTE_VERDICT_BLOCKED;
	switch (row->kind)
	{
		case CLUSTER_SIDE_ROUTE_PROVED_NOOP:
			return CLUSTER_SIDE_ROUTE_VERDICT_PROVED_NOOP;
		case CLUSTER_SIDE_ROUTE_PAGE:
		case CLUSTER_SIDE_ROUTE_TT_UNDO:
		case CLUSTER_SIDE_ROUTE_PROJECTION:
		case CLUSTER_SIDE_ROUTE_STORAGE:
			return CLUSTER_SIDE_ROUTE_VERDICT_APPLY;
		case CLUSTER_SIDE_ROUTE_BLOCKED:
		default:
			return CLUSTER_SIDE_ROUTE_VERDICT_BLOCKED;
	}
}

#else							/* !USE_PGRAC_CLUSTER */

/* Disable-cluster build: this file compiles to nothing. */

#endif							/* USE_PGRAC_CLUSTER */
