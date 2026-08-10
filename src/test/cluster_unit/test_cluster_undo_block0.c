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
#include "port/atomics.h"
#include "port/pg_pthread.h"

#undef printf
#undef fprintf
#undef snprintf

#include "unit_test.h"

UT_DEFINE_GLOBALS();

void
ExceptionalCondition(const char *condition_name pg_attribute_unused(),
					 const char *file_name pg_attribute_unused(),
					 int line_number pg_attribute_unused())
{
	abort();
}

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

static bool
r4_prerequisite_snapshot_is_fixed_false(const ClusterR4PrerequisiteSnapshot *snapshot)
{
	static const uint8 expected[8] = {0};

	return snapshot != NULL && snapshot->status == CLUSTER_R4_PREREQUISITE_RF_DEFERRED
		   && !snapshot->ready && snapshot->reserved[0] == 0 && snapshot->reserved[1] == 0
		   && snapshot->reserved[2] == 0
		   && memcmp(snapshot, expected, sizeof(expected)) == 0;
}

#define R4_PREREQUISITE_THREAD_COUNT 8
#define R4_PREREQUISITE_CALLS_PER_THREAD 10000

typedef struct R4PrerequisiteThreadResult {
	pg_atomic_uint32 *ready;
	pg_atomic_uint32 *start;
	uint32 calls;
	uint32 mismatches;
} R4PrerequisiteThreadResult;

static void *
r4_prerequisite_snapshot_caller(void *arg)
{
	R4PrerequisiteThreadResult *result = (R4PrerequisiteThreadResult *)arg;
	int i;

	pg_atomic_fetch_add_u32(result->ready, 1);
	while (pg_atomic_read_u32(result->start) == 0)
		;

	for (i = 0; i < R4_PREREQUISITE_CALLS_PER_THREAD; i++) {
		ClusterR4PrerequisiteSnapshot snapshot;

		snapshot = cluster_undo_block0_r4_prerequisite_snapshot();
		result->calls++;
		if (!r4_prerequisite_snapshot_is_fixed_false(&snapshot))
			result->mismatches++;
	}

	return NULL;
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

UT_TEST(test_r4_prerequisite_snapshot_is_exact_and_repeatable)
{
	int i;

	UT_ASSERT_EQ((int)sizeof(ClusterR4PrerequisiteSnapshot), 8);
	UT_ASSERT_EQ((int)CLUSTER_R4_PREREQUISITE_RF_DEFERRED, 0);
	for (i = 0; i < 4096; i++) {
		ClusterR4PrerequisiteSnapshot snapshot;

		memset(&snapshot, 0xA5, sizeof(snapshot));
		snapshot = cluster_undo_block0_r4_prerequisite_snapshot();
		UT_ASSERT(r4_prerequisite_snapshot_is_fixed_false(&snapshot));
	}
}

UT_TEST(test_r4_prerequisite_snapshot_is_fixed_for_concurrent_callers)
{
	R4PrerequisiteThreadResult results[R4_PREREQUISITE_THREAD_COUNT];
	pthread_t threads[R4_PREREQUISITE_THREAD_COUNT];
	pg_atomic_uint32 ready;
	pg_atomic_uint32 start;
	uint32 calls = 0;
	uint32 mismatches = 0;
	int created = 0;
	int i;
	int rc;

	pg_atomic_init_u32(&ready, 0);
	pg_atomic_init_u32(&start, 0);
	memset(results, 0, sizeof(results));
	for (i = 0; i < R4_PREREQUISITE_THREAD_COUNT; i++) {
		results[i].ready = &ready;
		results[i].start = &start;
		rc = pthread_create(&threads[i], NULL, r4_prerequisite_snapshot_caller, &results[i]);
		UT_ASSERT_EQ(rc, 0);
		if (rc != 0)
			break;
		created++;
	}
	if (created != R4_PREREQUISITE_THREAD_COUNT) {
		pg_atomic_write_u32(&start, 1);
		for (i = 0; i < created; i++)
			(void)pthread_join(threads[i], NULL);
		return;
	}

	while (pg_atomic_read_u32(&ready) != R4_PREREQUISITE_THREAD_COUNT)
		;
	pg_atomic_write_u32(&start, 1);
	for (i = 0; i < R4_PREREQUISITE_THREAD_COUNT; i++) {
		rc = pthread_join(threads[i], NULL);
		UT_ASSERT_EQ(rc, 0);
		calls += results[i].calls;
		mismatches += results[i].mismatches;
	}
	UT_ASSERT_EQ(calls,
				 R4_PREREQUISITE_THREAD_COUNT * R4_PREREQUISITE_CALLS_PER_THREAD);
	UT_ASSERT_EQ(mismatches, 0);
}

int
main(void)
{
	UT_PLAN(10);
	UT_RUN(test_block0_key_endpoints_map_to_direct_slots);
	UT_RUN(test_block0_key_rejects_owner_segment_aliases);
	UT_RUN(test_block0_root_accepts_only_declared_intents);
	UT_RUN(test_block0_root_match_is_field_exact);
	UT_RUN(test_block0_generation_keeps_zero_distinct_from_absent);
	UT_RUN(test_block0_generation_exhaustion_never_wraps);
	UT_RUN(test_block0_slot_state_allows_only_frozen_edges);
	UT_RUN(test_block0_slot_state_rejects_direct_publish_and_double_publish);
	UT_RUN(test_r4_prerequisite_snapshot_is_exact_and_repeatable);
	UT_RUN(test_r4_prerequisite_snapshot_is_fixed_for_concurrent_callers);
	UT_DONE();
	return ut_failed_count == 0 ? 0 : 1;
}
