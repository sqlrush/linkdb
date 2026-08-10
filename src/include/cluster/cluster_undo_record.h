/*-------------------------------------------------------------------------
 *
 * cluster_undo_record.h
 *	  pgrac undo record format (spec-3.7 D3) — 64B common header +
 *	  4 op-specific payload structs.
 *
 *	  Stage 3 第 11 sub-spec.  builds on spec-1.5 UBA 16B + spec-1.21/1.22
 *	  segment header.  本 header 定义 undo record level layout:
 *	    - UndoRecordHeader 64B common (含 physical target locator per
 *	      codex review F4)
 *	    - UndoInsertPayload 4B
 *	    - UndoUpdatePayload 12B + var bytes(full old HeapTuple image per F5)
 *	    - UndoDeletePayload 8B + var bytes
 *	    - UndoItlPayload 40B
 *
 *	  Record layout in block(自上而下):
 *	    [UndoRecordHeader 64B][payload struct][var bytes(tuple image)]
 *
 *	  prev_uba (16B in header) = backward chain to previous record in
 *	  the same xid.  TT slot first_undo_block (rename semantics per
 *	  spec-3.7 §3.2:  head undo UBA, not literal first) points to the
 *	  *latest* record;  rollback traverses prev_uba chain to oldest.
 *
 *	  Physical target locator(per codex review F4 — common header
 *	  carries RelFileLocator + ForkNumber + BlockNumber + OffsetNumber)
 *	  identifies the heap tuple this undo record refers to.  Required
 *	  for rollback apply(spec-3.X) and CR construction(spec-3.9)to
 *	  locate the target tuple without parsing payload bytes.
 *
 *	  HC213 ABI lock(static asserts):
 *	    HC213 sizeof(UndoRecordHeader) == 64
 *	    HC214 sizeof(UndoInsertPayload) == 4
 *	    HC215 sizeof(UndoUpdatePayload) == 12
 *	    HC216 sizeof(UndoDeletePayload) == 8
 *	    HC217 sizeof(UndoItlPayload) == 40
 *
 *	  Frontend-safe: include chain stays storage/blocks 层不引 backend-only.
 *
 * Portions Copyright (c) 1996-2024, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2026, pgrac contributors
 *
 * Author: SqlRush <sqlrush@gmail.com>
 *
 * Spec: spec-3.7-undo-record-format-allocator.md (FROZEN v0.4)
 *
 * IDENTIFICATION
 *	  src/include/cluster/cluster_undo_record.h
 *
 * NOTES
 *	  This is a pgrac-original file (no derivation from PostgreSQL).
 *	  Pure ABI typedef + static asserts;  no functions.  Frontend-safe.
 *
 *-------------------------------------------------------------------------
 */
#ifndef CLUSTER_UNDO_RECORD_H
#define CLUSTER_UNDO_RECORD_H

#include "c.h"
#include "access/transam.h"				 /* TransactionId */
#include "storage/block.h"				 /* BlockNumber */
#include "storage/itemptr.h"			 /* OffsetNumber */
#include "storage/relfilelocator.h"		 /* RelFileLocator */
#include "common/relpath.h"				 /* ForkNumber */
#include "cluster/cluster_scn.h"		 /* SCN */
#include "cluster/cluster_itl_slot.h"	 /* UBA */
#include "cluster/cluster_undo_format.h" /* UndoBlockHeader for cap macro */

#ifdef USE_PGRAC_CLUSTER
#include "cluster/cluster_tx_resolve.h" /* R4 exact locator + typed reason */
#endif


/*
 * UndoRecordType -- op-specific record type discriminator.
 *
 *	Values match spec-3.7 §2.1 + D6 DML emit hook mapping.
 *	0 reserved as invalid sentinel.
 */
typedef enum UndoRecordType {
	UNDO_RECORD_INVALID = 0,
	UNDO_RECORD_INSERT = 1,
	UNDO_RECORD_UPDATE = 2,
	UNDO_RECORD_DELETE = 3,
	UNDO_RECORD_ITL = 4,
} UndoRecordType;


/*
 * UndoRecordFlags -- record header flags field.
 */
#define UNDO_REC_FLAG_FIRST_IN_TX 0x01 /* first record in this xid's undo chain */
#define UNDO_REC_FLAG_CONTINUED 0x02   /* continued from previous block (future) */
#define UNDO_REC_FLAG_TOAST 0x04	   /* payload references TOAST (future) */


/*
 * UndoRecordHeader -- 64-byte common header.
 *
 *	Layout(per codex review F2 + F4 + Hardening v1.0.1 verified):
 *
 *	  offset  size  field                description
 *	  ------  ----  -------------------- ----------------------------
 *	  0       1     record_type          UndoRecordType
 *	  1       1     flags                UNDO_REC_FLAG_*
 *	  2       2     payload_length       bytes after this header
 *	  4       4     xid                  writer xid (TransactionId)
 *	  8       2     origin_node_id       writer node
 *	  10      2     tt_slot_segment_id   exact TT key segment (spec-3.4b)
 *	  12      4     tt_slot_id           exact TT key slot
 *	  16      8     write_scn            SCN at write time
 *	  24      16    prev_uba             16B backward chain (spec-1.5 UBA)
 *	  40      12    target_locator       RelFileLocator (spc/db/rel)
 *	  52      4     target_fork          ForkNumber
 *	  56      4     target_block         BlockNumber
 *	  60      2     target_offset        OffsetNumber
 *	  62      2     _pad_target          alignment padding
 *	  ------  ----
 *	  64    total
 */
typedef struct UndoRecordHeader {
	uint8 record_type;			   /* offset  0 */
	uint8 flags;				   /* offset  1 */
	uint16 payload_length;		   /* offset  2 */
	TransactionId xid;			   /* offset  4 */
	uint16 origin_node_id;		   /* offset  8 */
	uint16 tt_slot_segment_id;	   /* offset 10 */
	uint32 tt_slot_id;			   /* offset 12 */
	SCN write_scn;				   /* offset 16 */
	UBA prev_uba;				   /* offset 24 (16B) */
	RelFileLocator target_locator; /* offset 40 (12B) */
	ForkNumber target_fork;		   /* offset 52 (4B) */
	BlockNumber target_block;	   /* offset 56 */
	OffsetNumber target_offset;	   /* offset 60 (2B) */
	uint16 tt_wrap_plus1;		   /* offset 62 (2B); spec-4.5a G4 (F3): the
									* bound TT slot's reuse generation at
									* record-write time, stored +1 so the
									* historical zero in this formerly-
									* padding byte pair reads as "unknown".
									* Readers (the G5 per-origin resolver /
									* remote chain walkers) derive
									* expected_wrap from it; a durable slot
									* whose wrap differs was recycled --
									* possibly to a same-valued xid after a
									* 32-bit wrap -- and must not match. */
} UndoRecordHeader;

StaticAssertDecl(sizeof(UndoRecordHeader) == 64, "UndoRecordHeader must be 64B — HC213");


#ifdef USE_PGRAC_CLUSTER
/*
 * R4 transaction-head identity validation.  These helpers are pure over a
 * record already read at the supplied UBA.  Physical heap target and payload
 * validation belongs to the CR builder, not this transaction identity gate.
 */
static inline bool
cluster_undo_record_identity_fail(ClusterTxResolveReason *reason_out, ClusterTxResolveReason reason)
{
	if (reason_out != NULL)
		*reason_out = reason;
	return false;
}

static inline bool
cluster_undo_record_uba_equal(UBA left, UBA right)
{
	return left.raw[0] == right.raw[0] && left.raw[1] == right.raw[1];
}

static inline bool
cluster_undo_record_validate_member(const ClusterTxLocator *locator, UBA record_uba,
									const UndoRecordHeader *record,
									ClusterTxResolveReason *reason_out)
{
	uint32 decoded_segment;
	uint32 decoded_block;
	uint16 locator_tt_offset;
	uint16 record_tt_offset;
	uint16 decoded_row;
	NodeId locator_origin;
	NodeId record_origin;
	NodeId tt_binding_origin;
	UBA tt_binding_uba;
	bool data_kind;

	if (locator == NULL || record == NULL)
		return cluster_undo_record_identity_fail(reason_out, CLUSTER_TX_RESOLVE_BAD_LOCATOR);

	data_kind = locator->itl_kind == ITL_FLAG_ACTIVE || locator->itl_kind == ITL_FLAG_COMMITTED
				|| locator->itl_kind == ITL_FLAG_ABORTED
				|| locator->itl_kind == ITL_FLAG_NEEDS_CLEANOUT;
	if (locator->itl_slot_index >= CLUSTER_ITL_INITRANS_DEFAULT
		|| (!data_kind && !ITL_FLAG_IS_LOCK_ONLY(locator->itl_kind)))
		return cluster_undo_record_identity_fail(reason_out, CLUSTER_TX_RESOLVE_BAD_LOCATOR);
	if (!TransactionIdIsNormal(locator->xid))
		return cluster_undo_record_identity_fail(reason_out, CLUSTER_TX_RESOLVE_XID_MISMATCH);
	if (locator->tt_wrap == TT_WRAP_INVALID)
		return cluster_undo_record_identity_fail(reason_out, CLUSTER_TX_RESOLVE_WRAP_MISMATCH);

	if (!uba_decode(locator->uba, &decoded_segment, &decoded_block, &locator_tt_offset,
					&decoded_row)
		|| decoded_block == 0
		|| decoded_row >= (BLCKSZ - sizeof(UndoBlockHeader)) / sizeof(UndoSlotDirEntry))
		return cluster_undo_record_identity_fail(reason_out, CLUSTER_TX_RESOLVE_BAD_UBA);
	locator_origin = uba_origin_node_id(locator->uba);
	if (locator_origin == InvalidNodeId)
		return cluster_undo_record_identity_fail(reason_out, CLUSTER_TX_RESOLVE_BAD_UBA);

	if (!uba_decode(record_uba, &decoded_segment, &decoded_block, &record_tt_offset, &decoded_row)
		|| decoded_block == 0
		|| decoded_row >= (BLCKSZ - sizeof(UndoBlockHeader)) / sizeof(UndoSlotDirEntry))
		return cluster_undo_record_identity_fail(reason_out, CLUSTER_TX_RESOLVE_BAD_UBA);
	if (record_tt_offset != locator_tt_offset)
		return cluster_undo_record_identity_fail(reason_out, CLUSTER_TX_RESOLVE_SLOT_MISMATCH);

	record_origin = uba_origin_node_id(record_uba);
	if (record_origin == InvalidNodeId || record_origin != locator_origin)
		return cluster_undo_record_identity_fail(reason_out, CLUSTER_TX_RESOLVE_BAD_UBA);

	/*
	 * The physical record segment may roll over inside one owner partition.
	 * Validate the persisted canonical TT segment as a separate identity.
	 */
	tt_binding_uba = record_uba;
	tt_binding_uba.raw[0] = ((uint64)decoded_block << 32) | record->tt_slot_segment_id;
	tt_binding_origin = uba_origin_node_id(tt_binding_uba);
	if (tt_binding_origin == InvalidNodeId || tt_binding_origin != locator_origin
		|| record->tt_slot_id != (uint32)locator_tt_offset + 1)
		return cluster_undo_record_identity_fail(reason_out, CLUSTER_TX_RESOLVE_SLOT_MISMATCH);
	if (record->origin_node_id != (uint16)record_origin)
		return cluster_undo_record_identity_fail(reason_out, CLUSTER_TX_RESOLVE_BAD_UBA);
	if (record->xid != locator->xid)
		return cluster_undo_record_identity_fail(reason_out, CLUSTER_TX_RESOLVE_XID_MISMATCH);
	if (record->tt_wrap_plus1 == 0 || (uint16)(record->tt_wrap_plus1 - 1) != locator->tt_wrap)
		return cluster_undo_record_identity_fail(reason_out, CLUSTER_TX_RESOLVE_WRAP_MISMATCH);

	if (reason_out != NULL)
		*reason_out = CLUSTER_TX_RESOLVE_NONE;
	return true;
}

static inline bool
cluster_undo_record_validate_identity(const ClusterTxLocator *locator, UBA record_uba,
									  const UndoRecordHeader *record,
									  ClusterTxResolveReason *reason_out)
{
	if (locator == NULL || record == NULL)
		return cluster_undo_record_identity_fail(reason_out, CLUSTER_TX_RESOLVE_BAD_LOCATOR);
	if (!cluster_undo_record_uba_equal(locator->uba, record_uba))
		return cluster_undo_record_identity_fail(reason_out, CLUSTER_TX_RESOLVE_BAD_UBA);
	return cluster_undo_record_validate_member(locator, record_uba, record, reason_out);
}

static inline bool
cluster_undo_record_validate_prev_edge(const ClusterTxLocator *locator, UBA current_uba,
									   const UndoRecordHeader *current, UBA previous_uba,
									   const UndoRecordHeader *previous,
									   ClusterTxResolveReason *reason_out)
{
	uint32 current_segment;
	uint32 current_block;
	uint16 current_tt_offset;
	uint16 current_row;
	uint32 previous_segment;
	uint32 previous_block;
	uint16 previous_tt_offset;
	uint16 previous_row;

	if (locator == NULL || current == NULL || previous == NULL)
		return cluster_undo_record_identity_fail(reason_out, CLUSTER_TX_RESOLVE_BAD_LOCATOR);
	if (!cluster_undo_record_validate_member(locator, current_uba, current, reason_out))
		return false;
	if (UBA_is_invalid(current->prev_uba)
		|| !cluster_undo_record_uba_equal(current->prev_uba, previous_uba)
		|| cluster_undo_record_uba_equal(current_uba, previous_uba))
		return cluster_undo_record_identity_fail(reason_out, CLUSTER_TX_RESOLVE_BAD_UBA);
	if (!uba_decode(current_uba, &current_segment, &current_block, &current_tt_offset, &current_row)
		|| !uba_decode(previous_uba, &previous_segment, &previous_block, &previous_tt_offset,
					   &previous_row))
		return cluster_undo_record_identity_fail(reason_out, CLUSTER_TX_RESOLVE_BAD_UBA);
	if (previous_tt_offset != current_tt_offset
		|| previous->tt_slot_segment_id != current->tt_slot_segment_id
		|| previous->tt_slot_id != current->tt_slot_id)
		return cluster_undo_record_identity_fail(reason_out, CLUSTER_TX_RESOLVE_SLOT_MISMATCH);
	if (previous_segment == current_segment
		&& (previous_block > current_block
			|| (previous_block == current_block && previous_row >= current_row)))
		return cluster_undo_record_identity_fail(reason_out, CLUSTER_TX_RESOLVE_BAD_UBA);
	return cluster_undo_record_validate_member(locator, previous_uba, previous, reason_out);
}
#endif /* USE_PGRAC_CLUSTER */


/*
 * UndoInsertPayload -- INSERT undo, 4 bytes.
 *
 *	Common header target locator identifies the inserted tuple location.
 *	Payload only carries optional sanity length + flags;  rollback apply
 *	(spec-3.X) does heap delete at target_block:target_offset.
 */
typedef struct UndoInsertPayload {
	uint16 inserted_tuple_len; /* optional sanity length; 0 for delete-line-pointer undo */
	uint16 flags;
} UndoInsertPayload;

StaticAssertDecl(sizeof(UndoInsertPayload) == 4, "UndoInsertPayload must be 4B — HC214");


/*
 * UndoUpdatePayload -- UPDATE undo, 12 bytes + var bytes (full old HeapTuple image).
 *
 *	Followed by `old_tuple_length` bytes of HeapTupleHeaderData + data.
 *	Common header target_locator + target_block + target_offset identify
 *	old tuple location.  new_block/new_offset locate replacement tuple
 *	(for HOT chain reconstruction in spec-3.9 CR construction).
 */
typedef struct UndoUpdatePayload {
	BlockNumber new_block;	 /* offset  0,  replacement tuple location */
	OffsetNumber new_offset; /* offset  4 */
	uint16 old_tuple_length; /* offset  6,  HeapTupleHeaderData + data byte count */
	uint16 old_tuple_offset; /* offset  8,  offset within record to old tuple bytes */
	uint16 flags;			 /* offset 10 */
} UndoUpdatePayload;

StaticAssertDecl(sizeof(UndoUpdatePayload) == 12, "UndoUpdatePayload must be 12B — HC215");


/*
 * UndoDeletePayload -- DELETE undo, 8 bytes + var bytes (full old HeapTuple image).
 *
 *	Followed by `full_tuple_length` bytes of HeapTupleHeaderData + data.
 *	Common header target_locator + target_block + target_offset identify
 *	deleted tuple location.
 */
typedef struct UndoDeletePayload {
	uint16 full_tuple_length; /* HeapTupleHeaderData + data byte count */
	uint16 full_tuple_offset; /* offset within record to tuple bytes */
	uint32 flags;
} UndoDeletePayload;

StaticAssertDecl(sizeof(UndoDeletePayload) == 8, "UndoDeletePayload must be 8B — HC216");


/*
 * UndoItlPayload -- ITL / lock-only undo, 40 bytes.
 *
 *	Restores ITL slot + tuple lock header state before the lock-only
 *	transition.  Used by spec-3.X rollback apply for ROLLBACK after
 *	SELECT ... FOR SHARE / FOR UPDATE / FOR NO KEY UPDATE.
 */
typedef struct UndoItlPayload {
	uint8 itl_slot_idx;
	uint8 prev_flags; /* ITL_FLAG_* before transition */
	uint8 new_flags;  /* ITL_FLAG_* after */
	uint8 lock_mode;  /* HEAP_XMAX_* semantic snapshot */
	TransactionId lock_xid;
	TransactionId prev_xmax; /* tuple header before lock-only change */
	uint16 prev_infomask;
	uint16 prev_infomask2;
	SCN prev_commit_scn;
	UBA prev_undo_segment_head; /* 16B */
} UndoItlPayload;

StaticAssertDecl(sizeof(UndoItlPayload) == 40, "UndoItlPayload must be 40B — HC217");


/*
 * Convenience macros for record total length computation.
 */
#define UNDO_REC_INSERT_TOTAL_LEN (sizeof(UndoRecordHeader) + sizeof(UndoInsertPayload))

#define UNDO_REC_UPDATE_TOTAL_LEN(tuple_bytes)                                                     \
	(sizeof(UndoRecordHeader) + sizeof(UndoUpdatePayload) + (tuple_bytes))

#define UNDO_REC_DELETE_TOTAL_LEN(tuple_bytes)                                                     \
	(sizeof(UndoRecordHeader) + sizeof(UndoDeletePayload) + (tuple_bytes))

#define UNDO_REC_ITL_TOTAL_LEN (sizeof(UndoRecordHeader) + sizeof(UndoItlPayload))


/*
 * Max record length cap — per spec-3.7 §3.2 + GUC
 *	cluster.undo_record_inline_max_bytes (default 1024).  Larger records
 *	→ ereport 53R9D fail-closed (caller in heap critical section before).
 */
#define UNDO_RECORD_MAX_INLINE_BYTES_DEFAULT 1024
#define UNDO_RECORD_HARD_CAP_BYTES                                                                 \
	(BLCKSZ - sizeof(UndoBlockHeader) - 16) /* leave room for at least 1 slot dir + safety */


#endif /* CLUSTER_UNDO_RECORD_H */
