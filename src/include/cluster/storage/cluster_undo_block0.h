/*-------------------------------------------------------------------------
 *
 * cluster_undo_block0.h
 *	  Local identity and resident-state core for undo block zero.
 *
 *
 * Portions Copyright (c) 1996-2024, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2026, pgrac contributors
 *
 * Author: SqlRush <sqlrush@gmail.com>
 *
 * IDENTIFICATION
 *	  src/include/cluster/storage/cluster_undo_block0.h
 *
 * NOTES
 *	  This is a pgrac-original file.  All exported symbols use the
 *	  cluster_undo_block0_ namespace.
 *	  Spec: spec-8.4a-undo-block0-authority-prerequisite.md
 *
 *-------------------------------------------------------------------------
 */
#ifndef CLUSTER_UNDO_BLOCK0_H
#define CLUSTER_UNDO_BLOCK0_H

#include "c.h"

#include "cluster/cluster_undo_segment.h"
#include "cluster/storage/cluster_undo_alloc.h"

#define CLUSTER_UNDO_BLOCK0_SLOTS_PER_OWNER CLUSTER_UNDO_SEGS_PER_INSTANCE
#define CLUSTER_UNDO_BLOCK0_SLOT_COUNT                                                             \
	((uint32)UNDO_OWNER_INSTANCE_MAX * CLUSTER_UNDO_BLOCK0_SLOTS_PER_OWNER)

StaticAssertDecl(CLUSTER_UNDO_BLOCK0_SLOT_COUNT == 32768,
				 "undo block-zero logical slot count must remain 32768");

/* One logical segment-header identity. */
typedef struct ClusterUndoBlock0LogicalKey {
	uint32 segment_id;
	uint8 owner_instance;
} ClusterUndoBlock0LogicalKey;

/* Resolved storage target retained inside one logical slot. */
typedef struct ClusterUndoBlock0ResolvedRoot {
	ClusterUndoPathIntent intent;
	uint64 root_id;
	uint64 root_generation;
} ClusterUndoBlock0ResolvedRoot;

/* Presence is independent of value because generation zero is valid. */
typedef struct ClusterUndoBlock0Generation {
	bool known;
	uint32 value;
} ClusterUndoBlock0Generation;

typedef enum ClusterUndoBlock0Result {
	CLUSTER_UNDO_BLOCK0_OK,
	CLUSTER_UNDO_BLOCK0_NOT_FOUND,
	CLUSTER_UNDO_BLOCK0_NOT_PUBLISHED,
	CLUSTER_UNDO_BLOCK0_GENERATION_MISMATCH,
	CLUSTER_UNDO_BLOCK0_IDENTITY_MISMATCH,
	CLUSTER_UNDO_BLOCK0_AUTHORITY_DENIED,
	CLUSTER_UNDO_BLOCK0_CAPACITY_UNAVAILABLE,
	CLUSTER_UNDO_BLOCK0_IO_ERROR
} ClusterUndoBlock0Result;

/* Resident payload states.  Recovery-private images never enter this FSM. */
typedef enum ClusterUndoBlock0SlotState {
	CLUSTER_UNDO_BLOCK0_SLOT_EMPTY,
	CLUSTER_UNDO_BLOCK0_SLOT_FILLING,
	CLUSTER_UNDO_BLOCK0_SLOT_VALID_CLEAN,
	CLUSTER_UNDO_BLOCK0_SLOT_VALID_DIRTY,
	CLUSTER_UNDO_BLOCK0_SLOT_RETIRING
} ClusterUndoBlock0SlotState;

typedef enum ClusterR4PrerequisiteStatus {
	CLUSTER_R4_PREREQUISITE_RF_DEFERRED = 0
} ClusterR4PrerequisiteStatus;

typedef struct ClusterR4PrerequisiteSnapshot {
	ClusterR4PrerequisiteStatus status;
	bool ready;
	uint8 reserved[3];
} ClusterR4PrerequisiteSnapshot;

StaticAssertDecl(sizeof(ClusterR4PrerequisiteSnapshot) == 8,
				 "R4 prerequisite snapshot must remain 8 bytes");

extern ClusterUndoBlock0Result
cluster_undo_block0_logical_slot(const ClusterUndoBlock0LogicalKey *logical, uint32 *slot);
extern bool cluster_undo_block0_root_valid(const ClusterUndoBlock0ResolvedRoot *root);
extern bool cluster_undo_block0_root_matches(const ClusterUndoBlock0ResolvedRoot *observed,
											 const ClusterUndoBlock0ResolvedRoot *expected);
extern bool cluster_undo_block0_generation_matches(const ClusterUndoBlock0Generation *observed,
												   const ClusterUndoBlock0Generation *expected);
extern bool cluster_undo_block0_generation_advance(const ClusterUndoBlock0Generation *current,
												   ClusterUndoBlock0Generation *next);
extern bool cluster_undo_block0_state_transition_allowed(ClusterUndoBlock0SlotState from,
														 ClusterUndoBlock0SlotState to);
extern ClusterR4PrerequisiteSnapshot cluster_undo_block0_r4_prerequisite_snapshot(void);

#endif /* CLUSTER_UNDO_BLOCK0_H */
