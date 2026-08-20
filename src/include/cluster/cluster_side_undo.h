/*-------------------------------------------------------------------------
 *
 * cluster_side_undo.h
 *	  RF-SIDE D-SIDE-02 — decode/apply separation for the shared
 *	  TT/undo truth: one decode primitive + one preflight gate.
 *
 *	  Spec: specs/spec-rf-side-typed-recovery-and-shared-terminal.md
 *	  §1.2 D-SIDE-02 ("将 decode 与 apply 分离；normal TT/undo truth
 *	  只走一份 primitive；不得宣称现有 recovery writer 已安全"), §2.1
 *	  single-parser contract (decode once -> parsed components -> no
 *	  raw-record re-read; preflight ALL components before any mutation;
 *	  apply receives parsed only), §5.1 U-SIDE-04 (malformed length /
 *	  illegal modifiers pre-mutation BLOCKED).
 *
 *	  DELIVERED HERE:
 *	    - cluster_undo_decode: PURE parsing of one RM_CLUSTER_UNDO record
 *	      into caller-owned immutable fields (length check + field
 *	      extraction only; no I/O, no mutation, no raw-record re-read);
 *	      unknown/illegal info -> false (BLOCKED);
 *	    - cluster_undo_preflight: the §2.1-2 gate — route (D-SIDE-01:
 *	      TT_UNDO rows pass, XLOG_HW_RESERVE stays BLOCKED under
 *	      STOP-RF-SIDE-SPACE-ABI) + field-integrity checks (e.g. TT slot
 *	      offset bounds).  Any false means ZERO mutation.
 *
 *	  NOT DELIVERED HERE (stays RED): the apply primitive's execution —
 *	  the existing recovery writer (cluster_undo_redo) is NOT claimed
 *	  safe (spec §1.2); the mutation/durability/post-read execution of
 *	  the decoded components belongs to the RF-ROOT/RF-PAGE integration
 *	  round.  No ABI change.
 *
 * Portions Copyright (c) 1996-2024, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2026, pgrac contributors
 *
 * IDENTIFICATION
 *	  src/include/cluster/cluster_side_undo.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef CLUSTER_SIDE_UNDO_H
#define CLUSTER_SIDE_UNDO_H

#include "access/xlogreader.h"
#include "cluster/cluster_scn.h" /* SCN */
#include "storage/itemptr.h" /* TransactionId */

typedef enum ClusterUndoDecodedKind
{
	CLUSTER_UNDO_KIND_SEGMENT_INIT = 0,
	CLUSTER_UNDO_KIND_TT_COMMIT,
	CLUSTER_UNDO_KIND_TT_ABORT,
	CLUSTER_UNDO_KIND_TT_SET_HEAD,
	CLUSTER_UNDO_KIND_SEGMENT_RECYCLE,
	CLUSTER_UNDO_KIND_SEGMENT_REUSE,
	CLUSTER_UNDO_KIND_BLOCK_WRITE,
	CLUSTER_UNDO_KIND_BLOCK_WRITE_MULTI,
	CLUSTER_UNDO_KIND_HW_RESERVE,	/* BLOCKED: STOP-RF-SIDE-SPACE-ABI */
	CLUSTER_UNDO_KIND_UNKNOWN
} ClusterUndoDecodedKind;

/*
 * Parsed immutable fields of one RM_CLUSTER_UNDO record.  `opcode` is the
 * rmgr info byte (after XLR_INFO_MASK) so the D-SIDE-01 route can be
 * re-derived; the per-kind fields are filled only for the kinds that
 * carry them (0 otherwise).
 */
typedef struct ClusterUndoDecoded
{
	ClusterUndoDecodedKind kind;
	uint16		opcode;			/* rmgr info after XLR_INFO_MASK */
	uint8		instance;		/* owner instance (1..128) */
	uint32		segment_id;
	uint16		slot_offset;	/* TT slot offset */
	uint16		wrap;			/* TT reuse generation */
	TransactionId xid;			/* TT slot owner */
	SCN			commit_scn;		/* TT commit SCN */
	uint32		block_no;		/* BLOCK_WRITE target block */
	bool		has_payload;	/* BLOCK_WRITE carries a payload image */
} ClusterUndoDecoded;

/*
 * §2.1-1 decode: pure parse of one record into parsed fields.  Returns
 * false (BLOCKED) on NULL inputs, unknown/illegal info, or a malformed
 * payload length — the caller must not mutate anything (U-SIDE-04).
 */
extern bool cluster_undo_decode(XLogReaderState *record, ClusterUndoDecoded *out);

/*
 * §2.1-2 preflight: route (D-SIDE-01) + field integrity for the parsed
 * components.  false -> ZERO mutation (the record's whole component set
 * stays frozen).  XLOG_HW_RESERVE is always false while
 * STOP-RF-SIDE-SPACE-ABI is active.
 */
extern bool cluster_undo_preflight(const ClusterUndoDecoded *decoded);

#endif							/* CLUSTER_SIDE_UNDO_H */
