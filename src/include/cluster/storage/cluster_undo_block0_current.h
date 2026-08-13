/*-------------------------------------------------------------------------
 *
 * cluster_undo_block0_current.h
 *	  Cooperative Candidate-2 current guard for undo block zero.
 *
 * Portions Copyright (c) 1996-2024, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2026, pgrac contributors
 *
 * IDENTIFICATION
 *	  src/include/cluster/storage/cluster_undo_block0_current.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef CLUSTER_UNDO_BLOCK0_CURRENT_H
#define CLUSTER_UNDO_BLOCK0_CURRENT_H

#include "c.h"

#include "cluster/storage/cluster_undo_block0.h"
#include "storage/lockdefs.h"

/* Generation is protected data and therefore never participates in this key. */
#define CLUSTER_UNDO_BLOCK0_CURRENT_RESID_TYPE 0xFB

typedef enum ClusterUndoBlock0CurrentMode {
	CLUSTER_UNDO_BLOCK0_SCUR = ShareLock,
	CLUSTER_UNDO_BLOCK0_XCUR = ExclusiveLock
} ClusterUndoBlock0CurrentMode;

typedef enum ClusterUndoBlock0CurrentStep {
	CLUSTER_UNDO_BLOCK0_CURRENT_PENDING = 0,
	CLUSTER_UNDO_BLOCK0_CURRENT_HELD = 1,
	CLUSTER_UNDO_BLOCK0_CURRENT_RELEASED = 2,
	CLUSTER_UNDO_BLOCK0_CURRENT_FAILED = 3
} ClusterUndoBlock0CurrentStep;

/*
 * Process-local guard.  Its body is deliberately private to the adapter.
 * uint64 supplies the alignment required by its embedded dlist_node.
 */
typedef union ClusterUndoBlock0CurrentGuard {
	uint64 align;
	uint8 opaque[168];
} ClusterUndoBlock0CurrentGuard;

StaticAssertDecl(sizeof(ClusterUndoBlock0CurrentGuard) == 168,
				 "block0 current guard ABI must remain exactly 168 bytes");

extern void cluster_undo_block0_current_init(void);
extern ClusterUndoBlock0CurrentStep cluster_undo_block0_current_acquire_begin(
	const ClusterUndoBlock0LogicalKey *key, ClusterUndoBlock0CurrentMode mode, int timeout_ms,
	ClusterUndoBlock0CurrentGuard *guard, ClusterUndoBlock0Result *failure);
extern ClusterUndoBlock0CurrentStep
cluster_undo_block0_current_acquire_poll(ClusterUndoBlock0CurrentGuard *guard,
										ClusterUndoBlock0Result *failure);
extern void cluster_undo_block0_current_cancel(ClusterUndoBlock0CurrentGuard *guard);
extern ClusterUndoBlock0CurrentStep
cluster_undo_block0_current_release_begin(ClusterUndoBlock0CurrentGuard *guard,
										ClusterUndoBlock0Result *failure);
extern ClusterUndoBlock0CurrentStep
cluster_undo_block0_current_release_poll(ClusterUndoBlock0CurrentGuard *guard,
									   ClusterUndoBlock0Result *failure);

extern ClusterUndoBlock0Result cluster_undo_block0_current_sample_generation(
	ClusterUndoBlock0CurrentGuard *guard, const ClusterUndoBlock0ResolvedRoot *root,
	ClusterUndoBlock0Generation *observed);
extern ClusterUndoBlock0Result cluster_undo_block0_current_copy_resident(
	ClusterUndoBlock0CurrentGuard *guard, const ClusterUndoBlock0ResolvedRoot *root,
	const ClusterUndoBlock0Generation *expected, char private_page[BLCKSZ]);
extern ClusterUndoBlock0Result cluster_undo_block0_current_pin_exclusive(
	ClusterUndoBlock0CurrentGuard *guard, const ClusterUndoBlock0ResolvedRoot *root,
	const ClusterUndoBlock0Generation *expected, ClusterUndoBlock0Pin *pin, char **page);

/* Target Startup's sole no-live-GES lane.  READY publication additionally
 * revalidates this process-local ownership through the query below. */
extern bool cluster_undo_block0_current_startup_fenced_begin(
	ClusterUndoBlock0CurrentGuard *guard);
extern bool cluster_undo_block0_current_startup_fenced_end(
	ClusterUndoBlock0CurrentGuard *guard);
extern bool cluster_undo_block0_current_startup_fenced_owned(void);

#endif /* CLUSTER_UNDO_BLOCK0_CURRENT_H */
