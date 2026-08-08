/*-------------------------------------------------------------------------
 *
 * test_cluster_undo_block0.c
 *	  Unit tests for the undo block-zero local identity core.
 *
 *
 * Portions Copyright (c) 1996-2024, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2026, pgrac contributors
 *
 * Author: SqlRush <sqlrush@gmail.com>
 *
 * IDENTIFICATION
 *	  src/test/cluster_unit/test_cluster_undo_block0.c
 *
 * NOTES
 *	  This is a pgrac-original file.
 *	  Spec: spec-8.4a-undo-block0-authority-prerequisite.md
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "cluster/storage/cluster_undo_block0.h"

#undef printf
#undef fprintf
#undef snprintf

#include "unit_test.h"

UT_DEFINE_GLOBALS();

static ClusterUndoBlock0LogicalKey
make_key(uint8 owner_instance, uint32 segment_id)
{
	ClusterUndoBlock0LogicalKey key;

	key.owner_instance = owner_instance;
	key.segment_id = segment_id;
	return key;
}

static ClusterUndoBlock0ResolvedRoot
make_root(ClusterUndoPathIntent intent, uint64 root_id, uint64 root_generation)
{
	ClusterUndoBlock0ResolvedRoot root;

	root.intent = intent;
	root.root_id = root_id;
	root.root_generation = root_generation;
	return root;
}

static ClusterUndoBlock0Generation
make_generation(bool known, uint32 value)
{
	ClusterUndoBlock0Generation generation;

	generation.known = known;
	generation.value = value;
	return generation;
}

UT_TEST(test_block0_key_endpoints_map_to_direct_slots)
{
	ClusterUndoBlock0LogicalKey key;
	uint32 slot = UINT32_MAX;

	key = make_key(1, 1);
	UT_ASSERT_EQ(cluster_undo_block0_logical_slot(&key, &slot), CLUSTER_UNDO_BLOCK0_OK);
	UT_ASSERT_EQ(slot, 0);

	key = make_key(1, 256);
	UT_ASSERT_EQ(cluster_undo_block0_logical_slot(&key, &slot), CLUSTER_UNDO_BLOCK0_OK);
	UT_ASSERT_EQ(slot, 255);

	key = make_key(128, 32513);
	UT_ASSERT_EQ(cluster_undo_block0_logical_slot(&key, &slot), CLUSTER_UNDO_BLOCK0_OK);
	UT_ASSERT_EQ(slot, 32512);

	key = make_key(128, 32768);
	UT_ASSERT_EQ(cluster_undo_block0_logical_slot(&key, &slot), CLUSTER_UNDO_BLOCK0_OK);
	UT_ASSERT_EQ(slot, 32767);
}

UT_TEST(test_block0_key_rejects_owner_segment_aliases)
{
	const ClusterUndoBlock0LogicalKey invalid[] = {
		{ .segment_id = 1, .owner_instance = 0 },	{ .segment_id = 1, .owner_instance = 129 },
		{ .segment_id = 0, .owner_instance = 1 },	{ .segment_id = 257, .owner_instance = 1 },
		{ .segment_id = 256, .owner_instance = 2 }, { .segment_id = 32769, .owner_instance = 128 },
	};
	uint32 slot;
	int i;

	for (i = 0; i < lengthof(invalid); i++) {
		slot = UINT32_MAX;
		UT_ASSERT_EQ(cluster_undo_block0_logical_slot(&invalid[i], &slot),
					 CLUSTER_UNDO_BLOCK0_IDENTITY_MISMATCH);
		UT_ASSERT_EQ(slot, UINT32_MAX);
	}
	UT_ASSERT_EQ(cluster_undo_block0_logical_slot(NULL, &slot),
				 CLUSTER_UNDO_BLOCK0_IDENTITY_MISMATCH);
	UT_ASSERT_EQ(cluster_undo_block0_logical_slot(&invalid[0], NULL),
				 CLUSTER_UNDO_BLOCK0_IDENTITY_MISMATCH);
}

UT_TEST(test_block0_root_accepts_only_declared_intents)
{
	ClusterUndoBlock0ResolvedRoot root;

	root = make_root(CLUSTER_UNDO_PATH_RUNTIME_SHARED, 0, 0);
	UT_ASSERT(cluster_undo_block0_root_valid(&root));
	root = make_root(CLUSTER_UNDO_PATH_MATERIALIZED_LOCAL, 7, 0);
	UT_ASSERT(cluster_undo_block0_root_valid(&root));
	root = make_root(CLUSTER_UNDO_PATH_RUNTIME_SHARED_AUTHORITY_BLOCK0, 9, 3);
	UT_ASSERT(cluster_undo_block0_root_valid(&root));
	root.intent = (ClusterUndoPathIntent)3;
	UT_ASSERT(!cluster_undo_block0_root_valid(&root));
	UT_ASSERT(!cluster_undo_block0_root_valid(NULL));
}

UT_TEST(test_block0_root_match_is_field_exact)
{
	ClusterUndoBlock0ResolvedRoot observed = make_root(CLUSTER_UNDO_PATH_RUNTIME_SHARED, 7, 9);
	ClusterUndoBlock0ResolvedRoot expected = observed;

	UT_ASSERT(cluster_undo_block0_root_matches(&observed, &expected));
	expected.intent = CLUSTER_UNDO_PATH_MATERIALIZED_LOCAL;
	UT_ASSERT(!cluster_undo_block0_root_matches(&observed, &expected));
	expected = observed;
	expected.root_id++;
	UT_ASSERT(!cluster_undo_block0_root_matches(&observed, &expected));
	expected = observed;
	expected.root_generation++;
	UT_ASSERT(!cluster_undo_block0_root_matches(&observed, &expected));
	UT_ASSERT(!cluster_undo_block0_root_matches(NULL, &expected));
}

UT_TEST(test_block0_generation_keeps_zero_distinct_from_absent)
{
	ClusterUndoBlock0Generation unknown = make_generation(false, 0);
	ClusterUndoBlock0Generation known_zero = make_generation(true, 0);
	ClusterUndoBlock0Generation known_one = make_generation(true, 1);

	UT_ASSERT(cluster_undo_block0_generation_matches(&unknown, &unknown));
	UT_ASSERT(cluster_undo_block0_generation_matches(&known_zero, &unknown));
	UT_ASSERT(!cluster_undo_block0_generation_matches(&unknown, &known_zero));
	UT_ASSERT(cluster_undo_block0_generation_matches(&known_zero, &known_zero));
	UT_ASSERT(!cluster_undo_block0_generation_matches(&known_one, &known_zero));
	UT_ASSERT(!cluster_undo_block0_generation_matches(NULL, &known_zero));
}

UT_TEST(test_block0_generation_exhaustion_never_wraps)
{
	ClusterUndoBlock0Generation current = make_generation(true, 0);
	ClusterUndoBlock0Generation next = make_generation(false, 77);

	UT_ASSERT(cluster_undo_block0_generation_advance(&current, &next));
	UT_ASSERT(next.known);
	UT_ASSERT_EQ(next.value, 1);

	current = make_generation(true, UINT32_MAX);
	next = make_generation(false, 77);
	UT_ASSERT(!cluster_undo_block0_generation_advance(&current, &next));
	UT_ASSERT(!next.known);
	UT_ASSERT_EQ(next.value, 77);

	current = make_generation(false, 0);
	UT_ASSERT(!cluster_undo_block0_generation_advance(&current, &next));
}

UT_TEST(test_block0_slot_state_allows_only_frozen_edges)
{
	UT_ASSERT(cluster_undo_block0_state_transition_allowed(CLUSTER_UNDO_BLOCK0_SLOT_EMPTY,
														   CLUSTER_UNDO_BLOCK0_SLOT_FILLING));
	UT_ASSERT(cluster_undo_block0_state_transition_allowed(CLUSTER_UNDO_BLOCK0_SLOT_FILLING,
														   CLUSTER_UNDO_BLOCK0_SLOT_VALID_CLEAN));
	UT_ASSERT(cluster_undo_block0_state_transition_allowed(CLUSTER_UNDO_BLOCK0_SLOT_FILLING,
														   CLUSTER_UNDO_BLOCK0_SLOT_EMPTY));
	UT_ASSERT(cluster_undo_block0_state_transition_allowed(CLUSTER_UNDO_BLOCK0_SLOT_VALID_CLEAN,
														   CLUSTER_UNDO_BLOCK0_SLOT_VALID_DIRTY));
	UT_ASSERT(cluster_undo_block0_state_transition_allowed(CLUSTER_UNDO_BLOCK0_SLOT_VALID_DIRTY,
														   CLUSTER_UNDO_BLOCK0_SLOT_VALID_CLEAN));
	UT_ASSERT(cluster_undo_block0_state_transition_allowed(CLUSTER_UNDO_BLOCK0_SLOT_VALID_CLEAN,
														   CLUSTER_UNDO_BLOCK0_SLOT_RETIRING));
	UT_ASSERT(cluster_undo_block0_state_transition_allowed(CLUSTER_UNDO_BLOCK0_SLOT_VALID_DIRTY,
														   CLUSTER_UNDO_BLOCK0_SLOT_RETIRING));
	UT_ASSERT(cluster_undo_block0_state_transition_allowed(CLUSTER_UNDO_BLOCK0_SLOT_RETIRING,
														   CLUSTER_UNDO_BLOCK0_SLOT_EMPTY));
}

UT_TEST(test_block0_slot_state_rejects_direct_publish_and_double_publish)
{
	UT_ASSERT(!cluster_undo_block0_state_transition_allowed(CLUSTER_UNDO_BLOCK0_SLOT_EMPTY,
															CLUSTER_UNDO_BLOCK0_SLOT_VALID_CLEAN));
	UT_ASSERT(!cluster_undo_block0_state_transition_allowed(CLUSTER_UNDO_BLOCK0_SLOT_EMPTY,
															CLUSTER_UNDO_BLOCK0_SLOT_VALID_DIRTY));
	UT_ASSERT(!cluster_undo_block0_state_transition_allowed(CLUSTER_UNDO_BLOCK0_SLOT_EMPTY,
															CLUSTER_UNDO_BLOCK0_SLOT_RETIRING));
	UT_ASSERT(!cluster_undo_block0_state_transition_allowed(CLUSTER_UNDO_BLOCK0_SLOT_VALID_CLEAN,
															CLUSTER_UNDO_BLOCK0_SLOT_VALID_CLEAN));
	UT_ASSERT(!cluster_undo_block0_state_transition_allowed(CLUSTER_UNDO_BLOCK0_SLOT_VALID_DIRTY,
															CLUSTER_UNDO_BLOCK0_SLOT_VALID_DIRTY));
	UT_ASSERT(!cluster_undo_block0_state_transition_allowed(CLUSTER_UNDO_BLOCK0_SLOT_RETIRING,
															CLUSTER_UNDO_BLOCK0_SLOT_FILLING));
	UT_ASSERT(!cluster_undo_block0_state_transition_allowed((ClusterUndoBlock0SlotState)-1,
															CLUSTER_UNDO_BLOCK0_SLOT_EMPTY));
}

int
main(void)
{
	UT_PLAN(8);
	UT_RUN(test_block0_key_endpoints_map_to_direct_slots);
	UT_RUN(test_block0_key_rejects_owner_segment_aliases);
	UT_RUN(test_block0_root_accepts_only_declared_intents);
	UT_RUN(test_block0_root_match_is_field_exact);
	UT_RUN(test_block0_generation_keeps_zero_distinct_from_absent);
	UT_RUN(test_block0_generation_exhaustion_never_wraps);
	UT_RUN(test_block0_slot_state_allows_only_frozen_edges);
	UT_RUN(test_block0_slot_state_rejects_direct_publish_and_double_publish);
	UT_DONE();
	return ut_failed_count == 0 ? 0 : 1;
}
