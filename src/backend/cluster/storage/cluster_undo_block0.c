/*-------------------------------------------------------------------------
 *
 * cluster_undo_block0.c
 *	  Local identity and resident-state core for undo block zero.
 *
 *	  This dependency-light layer validates logical identities, resolved
 *	  storage roots, explicit segment generations, and resident-state edges.
 *	  It performs no I/O, shared-memory access, recovery admission, or
 *	  authority acquisition.
 *
 *
 * Portions Copyright (c) 1996-2024, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2026, pgrac contributors
 *
 * Author: SqlRush <sqlrush@gmail.com>
 *
 * IDENTIFICATION
 *	  src/backend/cluster/storage/cluster_undo_block0.c
 *
 * NOTES
 *	  This is a pgrac-original file.  All symbols use the
 *	  cluster_undo_block0_ namespace.
 *	  Spec: spec-8.4a-undo-block0-authority-prerequisite.md
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "cluster/storage/cluster_undo_block0.h"

/*
 * cluster_undo_block0_logical_slot -- Validate and map a logical identity.
 *
 *	The owner range partitions segment identifiers into fixed runs of 256.
 *	A segment from another owner's run is an identity mismatch, not an alias.
 *	The output remains unchanged on failure.
 */
ClusterUndoBlock0Result
cluster_undo_block0_logical_slot(const ClusterUndoBlock0LogicalKey *logical, uint32 *slot)
{
	uint32 first_segment;
	uint32 mapped_slot;
	uint32 owner_slot;
	uint32 segment_slot;

	if (logical == NULL || slot == NULL)
		return CLUSTER_UNDO_BLOCK0_IDENTITY_MISMATCH;
	if (logical->owner_instance < 1 || logical->owner_instance > UNDO_OWNER_INSTANCE_MAX)
		return CLUSTER_UNDO_BLOCK0_IDENTITY_MISMATCH;

	owner_slot = (uint32)logical->owner_instance - 1;
	first_segment = owner_slot * CLUSTER_UNDO_BLOCK0_SLOTS_PER_OWNER + 1;
	if (logical->segment_id < first_segment
		|| logical->segment_id >= first_segment + CLUSTER_UNDO_BLOCK0_SLOTS_PER_OWNER)
		return CLUSTER_UNDO_BLOCK0_IDENTITY_MISMATCH;

	segment_slot = logical->segment_id - first_segment;
	mapped_slot = owner_slot * CLUSTER_UNDO_BLOCK0_SLOTS_PER_OWNER + segment_slot;
	if (mapped_slot >= CLUSTER_UNDO_BLOCK0_SLOT_COUNT)
		return CLUSTER_UNDO_BLOCK0_IDENTITY_MISMATCH;
	*slot = mapped_slot;

	return CLUSTER_UNDO_BLOCK0_OK;
}

/*
 * cluster_undo_block0_root_valid -- Accept only declared path intents.
 */
bool
cluster_undo_block0_root_valid(const ClusterUndoBlock0ResolvedRoot *root)
{
	if (root == NULL)
		return false;

	switch (root->intent) {
	case CLUSTER_UNDO_PATH_RUNTIME_SHARED:
	case CLUSTER_UNDO_PATH_MATERIALIZED_LOCAL:
	case CLUSTER_UNDO_PATH_RUNTIME_SHARED_AUTHORITY_BLOCK0:
		return true;
	default:
		return false;
	}
}

/*
 * cluster_undo_block0_root_matches -- Compare a resolved target exactly.
 */
bool
cluster_undo_block0_root_matches(const ClusterUndoBlock0ResolvedRoot *observed,
								 const ClusterUndoBlock0ResolvedRoot *expected)
{
	if (!cluster_undo_block0_root_valid(observed) || !cluster_undo_block0_root_valid(expected))
		return false;

	return observed->intent == expected->intent && observed->root_id == expected->root_id
		   && observed->root_generation == expected->root_generation;
}

/*
 * cluster_undo_block0_generation_matches -- Apply an optional exact guard.
 *
 *	An unknown expected generation imposes no comparison.  A known expected
 *	generation requires an observed presence bit and an exact value match.
 */
bool
cluster_undo_block0_generation_matches(const ClusterUndoBlock0Generation *observed,
									   const ClusterUndoBlock0Generation *expected)
{
	if (observed == NULL || expected == NULL)
		return false;
	if (!expected->known)
		return true;

	return observed->known && observed->value == expected->value;
}

/*
 * cluster_undo_block0_generation_advance -- Derive the next generation.
 *
 *	An absent current generation cannot be advanced, and UINT32_MAX is an
 *	exhausted identity.  The output remains unchanged on failure.
 */
bool
cluster_undo_block0_generation_advance(const ClusterUndoBlock0Generation *current,
									   ClusterUndoBlock0Generation *next)
{
	if (current == NULL || next == NULL || !current->known || current->value == UINT32_MAX)
		return false;

	next->known = true;
	next->value = current->value + 1;
	return true;
}

/*
 * cluster_undo_block0_state_transition_allowed -- Validate resident edges.
 *
 *	No same-state transition, direct publication, or S-to-X-style shortcut is
 *	accepted here.  Recovery-private images do not enter the resident FSM.
 */
bool
cluster_undo_block0_state_transition_allowed(ClusterUndoBlock0SlotState from,
											 ClusterUndoBlock0SlotState to)
{
	switch (from) {
	case CLUSTER_UNDO_BLOCK0_SLOT_EMPTY:
		return to == CLUSTER_UNDO_BLOCK0_SLOT_FILLING;
	case CLUSTER_UNDO_BLOCK0_SLOT_FILLING:
		return to == CLUSTER_UNDO_BLOCK0_SLOT_VALID_CLEAN || to == CLUSTER_UNDO_BLOCK0_SLOT_EMPTY;
	case CLUSTER_UNDO_BLOCK0_SLOT_VALID_CLEAN:
		return to == CLUSTER_UNDO_BLOCK0_SLOT_VALID_DIRTY
			   || to == CLUSTER_UNDO_BLOCK0_SLOT_RETIRING;
	case CLUSTER_UNDO_BLOCK0_SLOT_VALID_DIRTY:
		return to == CLUSTER_UNDO_BLOCK0_SLOT_VALID_CLEAN
			   || to == CLUSTER_UNDO_BLOCK0_SLOT_RETIRING;
	case CLUSTER_UNDO_BLOCK0_SLOT_RETIRING:
		return to == CLUSTER_UNDO_BLOCK0_SLOT_EMPTY;
	default:
		return false;
	}
}
