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

typedef struct ClusterSemanticAdmissionToken ClusterSemanticAdmissionToken;

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

typedef enum ClusterUndoBlock0RecycleResult {
	CLUSTER_UNDO_BLOCK0_RECYCLE_ADVANCED = 0,
	CLUSTER_UNDO_BLOCK0_RECYCLE_ALREADY = 1,
	CLUSTER_UNDO_BLOCK0_RECYCLE_NOT_COMMITTED = 2,
	CLUSTER_UNDO_BLOCK0_RECYCLE_RETAINED = 3,
	CLUSTER_UNDO_BLOCK0_RECYCLE_FAILED = 4
} ClusterUndoBlock0RecycleResult;

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

/* Process-local, non-authorizing receipt for an exact live-owner resident
 * publication.  Its body is private to the current adapter. */
typedef union ClusterUndoBlock0LiveOwnerPublication {
	uint64 align;
	uint8 opaque[192];
} ClusterUndoBlock0LiveOwnerPublication;

StaticAssertDecl(sizeof(ClusterUndoBlock0LiveOwnerPublication) == 192,
				 "live-owner publication receipt ABI must remain 192 bytes");

extern void cluster_undo_block0_current_init(void);
extern void cluster_undo_block0_current_ensure_exit_hooks(void);
extern ClusterUndoBlock0CurrentStep cluster_undo_block0_current_acquire_begin(
	const ClusterUndoBlock0LogicalKey *key, ClusterUndoBlock0CurrentMode mode, int timeout_ms,
	ClusterUndoBlock0CurrentGuard *guard, ClusterUndoBlock0Result *failure);
extern ClusterUndoBlock0CurrentStep
cluster_undo_block0_current_acquire_begin_admitted(
	const ClusterUndoBlock0LogicalKey *key, ClusterUndoBlock0CurrentMode mode,
	int timeout_ms, const ClusterSemanticAdmissionToken *admission,
	ClusterUndoBlock0CurrentGuard *guard, ClusterUndoBlock0Result *failure);
extern ClusterUndoBlock0CurrentStep
cluster_undo_block0_current_acquire_begin_live_owner_source(
	const ClusterUndoBlock0LogicalKey *key, int timeout_ms,
	const ClusterSemanticAdmissionToken *admission,
	ClusterUndoBlock0CurrentGuard *guard, ClusterUndoBlock0Result *failure);
extern ClusterUndoBlock0CurrentStep
cluster_undo_block0_current_acquire_begin_live_owner_target(
	const ClusterUndoBlock0LogicalKey *key, int timeout_ms,
	const ClusterSemanticAdmissionToken *admission,
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
extern ClusterUndoBlock0Result cluster_undo_block0_current_sample_generation_exclusive(
	ClusterUndoBlock0CurrentGuard *guard, const ClusterUndoBlock0ResolvedRoot *root,
	ClusterUndoBlock0Generation *observed);
extern ClusterUndoBlock0Result
cluster_undo_block0_current_prove_strict_empty_exclusive(
	ClusterUndoBlock0CurrentGuard *guard);
extern ClusterUndoBlock0Result cluster_undo_block0_current_copy_resident(
	ClusterUndoBlock0CurrentGuard *guard, const ClusterUndoBlock0ResolvedRoot *root,
	const ClusterUndoBlock0Generation *expected, char private_page[BLCKSZ]);
extern ClusterUndoBlock0Result cluster_undo_block0_current_pin_exclusive(
	ClusterUndoBlock0CurrentGuard *guard, const ClusterUndoBlock0ResolvedRoot *root,
	const ClusterUndoBlock0Generation *expected, ClusterUndoBlock0Pin *pin, char **page);
extern ClusterUndoBlock0Result cluster_undo_block0_current_recheck_exclusive(
	ClusterUndoBlock0CurrentGuard *guard);
extern ClusterUndoBlock0Result
cluster_undo_block0_current_live_owner_ensure_resident(
	const ClusterUndoBlock0LogicalKey *key, int timeout_ms);
extern ClusterUndoBlock0Result
cluster_undo_block0_current_live_owner_ensure_resident_exact(
	const ClusterUndoBlock0LogicalKey *key, int timeout_ms,
	ClusterUndoBlock0LiveOwnerPublication *publication);
extern ClusterUndoBlock0Result
cluster_undo_block0_current_live_owner_reuse_exact(
	const ClusterUndoBlock0LogicalKey *key,
	const ClusterUndoBlock0Generation *expected,
	const char successor_page[BLCKSZ], int timeout_ms);
extern ClusterUndoBlock0Result
cluster_undo_block0_current_live_owner_mutate_exact(
	const ClusterUndoBlock0LogicalKey *key,
	const ClusterUndoBlock0Generation *expected,
	const char predecessor_page[BLCKSZ],
	const char successor_page[BLCKSZ], int timeout_ms);
extern ClusterUndoBlock0RecycleResult
cluster_undo_block0_current_live_owner_recycle_exact(
	const ClusterUndoBlock0LogicalKey *key, SCN horizon,
	uint64 expected_epoch, int timeout_ms);
extern bool cluster_undo_block0_current_live_owner_publication_recheck(
	const ClusterUndoBlock0LiveOwnerPublication *publication);
extern bool
cluster_undo_block0_current_live_owner_publication_recheck_conditional(
	const ClusterUndoBlock0LiveOwnerPublication *publication);

/* Target Startup's sole no-live-GES lane.  READY publication additionally
 * revalidates this process-local ownership through the query below. */
extern bool cluster_undo_block0_current_startup_fenced_begin(
	ClusterUndoBlock0CurrentGuard *guard);
extern bool cluster_undo_block0_current_startup_fenced_end(
	ClusterUndoBlock0CurrentGuard *guard);
extern bool cluster_undo_block0_current_startup_fenced_owned(void);

#endif /* CLUSTER_UNDO_BLOCK0_CURRENT_H */
