/*-------------------------------------------------------------------------
 *
 * cluster_side_undo.c
 *	  RF-SIDE D-SIDE-02 — TT/undo decode + preflight primitives
 *	  (implementation).
 *
 *	  Spec: specs/spec-rf-side-typed-recovery-and-shared-terminal.md
 *	  §1.2 D-SIDE-02 / §2.1 / §5.1 U-SIDE-04.
 *
 *	  decode() is pure parsing: length check + field extraction, no I/O,
 *	  no mutation, no raw-record re-read.  preflight() combines the
 *	  D-SIDE-01 route with field-integrity checks; any false means the
 *	  caller performs ZERO mutation.
 *
 * Portions Copyright (c) 1996-2024, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2026, pgrac contributors
 *
 * IDENTIFICATION
 *	  src/backend/cluster/cluster_side_undo.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#ifdef USE_PGRAC_CLUSTER

#include "access/rmgr.h"
#include "access/xlogrecord.h"
#include "cluster/cluster_side_route.h"
#include "cluster/cluster_tt_slot.h" /* TT_SLOTS_PER_SEGMENT */
#include "cluster/cluster_side_undo.h"
#include "cluster/storage/cluster_undo_xlog.h"

static ClusterUndoDecodedKind
cluster_undo_kind_for_opcode(uint16 opcode)
{
	switch (opcode)
	{
		case XLOG_UNDO_SEGMENT_INIT:
			return CLUSTER_UNDO_KIND_SEGMENT_INIT;
		case XLOG_UNDO_TT_SLOT_COMMIT:
			return CLUSTER_UNDO_KIND_TT_COMMIT;
		case XLOG_UNDO_TT_SLOT_ABORT:
			return CLUSTER_UNDO_KIND_TT_ABORT;
		case XLOG_UNDO_TT_SLOT_SET_HEAD:
			return CLUSTER_UNDO_KIND_TT_SET_HEAD;
		case XLOG_UNDO_SEGMENT_RECYCLE:
			return CLUSTER_UNDO_KIND_SEGMENT_RECYCLE;
		case XLOG_UNDO_SEGMENT_REUSE:
			return CLUSTER_UNDO_KIND_SEGMENT_REUSE;
		case XLOG_UNDO_BLOCK_WRITE:
			return CLUSTER_UNDO_KIND_BLOCK_WRITE;
		case XLOG_UNDO_BLOCK_WRITE_MULTI:
			return CLUSTER_UNDO_KIND_BLOCK_WRITE_MULTI;
		case XLOG_HW_RESERVE:
			return CLUSTER_UNDO_KIND_HW_RESERVE;
	}
	return CLUSTER_UNDO_KIND_UNKNOWN;
}

bool
cluster_undo_decode(XLogReaderState *record, ClusterUndoDecoded *out)
{
	uint16		opcode;
	ClusterUndoDecodedKind kind;

	if (record == NULL || out == NULL)
		return false;
	memset(out, 0, sizeof(*out));
	if (XLogRecGetRmid(record) != RM_CLUSTER_UNDO_ID)
		return false;			/* not a cluster-undo record */
	opcode = XLogRecGetInfo(record) & XLR_RMGR_INFO_MASK;
	kind = cluster_undo_kind_for_opcode(opcode);
	if (kind == CLUSTER_UNDO_KIND_UNKNOWN)
		return false;			/* unknown/illegal info: BLOCKED */
	out->kind = kind;
	out->opcode = opcode;

	/* Per-kind parse: exact payload length + field extraction (the same
	 * shapes the production redo handlers validate). */
	switch (kind)
	{
		case CLUSTER_UNDO_KIND_TT_COMMIT:
			{
				const xl_undo_tt_slot_commit *rec;

				if (XLogRecGetDataLen(record) != sizeof(*rec))
					return false;	/* malformed: BLOCKED (U-SIDE-04) */
				rec = (const xl_undo_tt_slot_commit *) XLogRecGetData(record);
				out->instance = rec->instance;
				out->segment_id = rec->segment_id;
				out->slot_offset = rec->slot_offset;
				out->wrap = rec->wrap;
				out->xid = rec->xid;
				out->commit_scn = rec->commit_scn;
				break;
			}
		case CLUSTER_UNDO_KIND_TT_ABORT:
			{
				const xl_undo_tt_slot_abort *rec;

				if (XLogRecGetDataLen(record) != sizeof(*rec))
					return false;
				rec = (const xl_undo_tt_slot_abort *) XLogRecGetData(record);
				out->instance = rec->instance;
				out->segment_id = rec->segment_id;
				out->slot_offset = rec->slot_offset;
				out->wrap = rec->wrap;
				out->xid = rec->xid;
				break;
			}
		case CLUSTER_UNDO_KIND_TT_SET_HEAD:
			{
				const xl_undo_tt_slot_set_head *rec;

				if (XLogRecGetDataLen(record) != sizeof(*rec))
					return false;
				rec = (const xl_undo_tt_slot_set_head *) XLogRecGetData(record);
				out->instance = rec->instance;
				out->segment_id = rec->segment_id;
				out->slot_offset = rec->slot_offset;
				out->wrap = rec->wrap;
				out->xid = rec->xid;
				break;
			}
		case CLUSTER_UNDO_KIND_SEGMENT_RECYCLE:
		case CLUSTER_UNDO_KIND_SEGMENT_REUSE:
			{
				const xl_undo_segment_recycle *rec;

				if (XLogRecGetDataLen(record) != sizeof(*rec))
					return false;
				rec = (const xl_undo_segment_recycle *) XLogRecGetData(record);
				out->instance = rec->instance;
				out->segment_id = rec->segment_id;
				break;
			}
		case CLUSTER_UNDO_KIND_BLOCK_WRITE:
		case CLUSTER_UNDO_KIND_BLOCK_WRITE_MULTI:
			{
				const xl_undo_block_write *rec;

				if (XLogRecGetDataLen(record) < sizeof(*rec))
					return false;
				rec = (const xl_undo_block_write *) XLogRecGetData(record);
				out->instance = rec->instance;
				out->segment_id = rec->segment_id;
				out->block_no = rec->block_no;
				out->has_payload = XLogRecGetDataLen(record) > sizeof(*rec);
				break;
			}
		case CLUSTER_UNDO_KIND_SEGMENT_INIT:
			{
				const xl_cluster_undo_segment_init *rec;

				if (XLogRecGetDataLen(record) < sizeof(*rec))
					return false;
				rec = (const xl_cluster_undo_segment_init *) XLogRecGetData(record);
				out->instance = rec->instance;
				out->segment_id = rec->segment_id;
				break;
			}
		case CLUSTER_UNDO_KIND_HW_RESERVE:
			{
				const xl_hw_reserve *rec;

				if (XLogRecGetDataLen(record) != sizeof(*rec))
					return false;
				rec = (const xl_hw_reserve *) XLogRecGetData(record);
				/* HWM carries relation/block facts, not undo segment
				 * identity; it stays BLOCKED at preflight anyway
				 * (STOP-RF-SIDE-SPACE-ABI). */
				out->block_no = rec->new_hwm;
				break;
			}
		default:
			return false;
	}
	return true;
}

bool
cluster_undo_preflight(const ClusterUndoDecoded *decoded)
{
	ClusterSideRouteRow route;
	uint16		opcode;

	if (decoded == NULL || decoded->kind == CLUSTER_UNDO_KIND_UNKNOWN)
		return false;

	/* §2.1-2 route: the D-SIDE-01 registry is the single judgement.
	 * XLOG_HW_RESERVE routes BLOCKED while STOP-RF-SIDE-SPACE-ABI is
	 * active (spec §3.2: never raise shmem and call complete). */
	opcode = decoded->opcode;
	if (!cluster_side_route_lookup(RM_CLUSTER_UNDO_ID, opcode, &route))
		return false;
	if (route.kind != CLUSTER_SIDE_ROUTE_TT_UNDO)
		return false;

	/* Field integrity: TT slots must be in range (mirrors the production
	 * handler's PANIC checks as pre-mutation gates — a malformed record
	 * is BLOCKED, never half-applied). */
	switch (decoded->kind)
	{
		case CLUSTER_UNDO_KIND_TT_COMMIT:
		case CLUSTER_UNDO_KIND_TT_ABORT:
		case CLUSTER_UNDO_KIND_TT_SET_HEAD:
			if (decoded->slot_offset >= TT_SLOTS_PER_SEGMENT)
				return false;
			if (decoded->xid == InvalidTransactionId)
				return false;
			break;
		default:
			break;
	}
	return true;
}

#else							/* !USE_PGRAC_CLUSTER */

/* Disable-cluster build: this file compiles to nothing. */

#endif							/* USE_PGRAC_CLUSTER */
