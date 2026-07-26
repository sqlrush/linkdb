/*-------------------------------------------------------------------------
 *
 * test_cluster_pi_discard_policy.c
 *	  Production-linked policy tests for bounded Past Image discard.
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "cluster/cluster_buffer_desc.h"
#include "cluster/cluster_pcm_lock.h"
#include "cluster/cluster_pi_discard_policy.h"

#undef printf
#undef fprintf
#undef snprintf
#undef sprintf
#undef vsnprintf
#undef vfprintf
#undef vprintf
#undef vsprintf
#undef strerror
#undef strerror_r

#include "unit_test.h"

UT_DEFINE_GLOBALS();

void
ExceptionalCondition(const char *conditionName pg_attribute_unused(),
					 const char *fileName pg_attribute_unused(),
					 int lineNumber pg_attribute_unused())
{
	abort();
}

static ClusterPiDiscardShape
exact_pi_shape(void)
{
	ClusterPiDiscardShape shape;

	memset(&shape, 0, sizeof(shape));
	shape.tag_matches = true;
	shape.is_pi = true;
	shape.pcm_is_n = true;
	shape.own_shape = CLUSTER_PCM_OWN_OK;
	return shape;
}

UT_TEST(test_final_raced_pin_is_retryable_and_classifier_is_pure)
{
	ClusterPiDiscardShape shape = exact_pi_shape();
	ClusterPiDiscardShape before;

	/*
	 * The caller captured this from the final mapping-X/header lock epoch:
	 * entry saw zero, but the authoritative recheck sees a foreign raw pin.
	 * The byte comparison proves classifier purity only; the bufmgr source
	 * contract separately proves no mutation precedes this classification.
	 */
	shape.refcount = 1;
	before = shape;

	UT_ASSERT_EQ((int)cluster_pi_discard_classify_locked(&shape),
				 (int)CLUSTER_BUFMGR_PI_DISCARD_RETRYABLE_BUSY);
	UT_ASSERT_EQ(memcmp(&shape, &before, sizeof(shape)), 0);
	UT_ASSERT_EQ(
		(int)cluster_pi_discard_action(CLUSTER_BUFMGR_PI_DISCARD_RETRYABLE_BUSY),
		(int)CLUSTER_PI_DISCARD_ACTION_RETRY);
}

UT_TEST(test_terminal_replacement_is_distinct_from_retryable_pin)
{
	ClusterPiDiscardShape shape = exact_pi_shape();

	shape.is_pi = false;
	shape.is_current = true;
	shape.refcount = 1;
	UT_ASSERT_EQ((int)cluster_pi_discard_classify_locked(&shape),
				 (int)CLUSTER_BUFMGR_PI_DISCARD_TERMINAL_NOOP);
	UT_ASSERT_EQ(
		(int)cluster_pi_discard_action(CLUSTER_BUFMGR_PI_DISCARD_TERMINAL_NOOP),
		(int)CLUSTER_PI_DISCARD_ACTION_TERMINAL_NOOP);
}

UT_TEST(test_contradictory_pi_shapes_fail_closed)
{
	ClusterPiDiscardShape shape = exact_pi_shape();
	ClusterPiDiscardShape before;
	ClusterBufmgrPiDiscardResult combined_result;

	shape.bm_valid = true;
	UT_ASSERT_EQ((int)cluster_pi_discard_classify_locked(&shape),
				 (int)CLUSTER_BUFMGR_PI_DISCARD_CORRUPT);
	shape = exact_pi_shape();
	shape.pcm_is_n = false;
	UT_ASSERT_EQ((int)cluster_pi_discard_classify_locked(&shape),
				 (int)CLUSTER_BUFMGR_PI_DISCARD_CORRUPT);
	shape = exact_pi_shape();
	shape.own_flags = PCM_OWN_FLAG_REVOKING;
	UT_ASSERT_EQ((int)cluster_pi_discard_classify_locked(&shape),
				 (int)CLUSTER_BUFMGR_PI_DISCARD_CORRUPT);
	shape = exact_pi_shape();
	shape.writer_activation_token = 99;
	UT_ASSERT_EQ((int)cluster_pi_discard_classify_locked(&shape),
				 (int)CLUSTER_BUFMGR_PI_DISCARD_CORRUPT);
	shape = exact_pi_shape();
	shape.own_shape = CLUSTER_PCM_OWN_BUSY;
	UT_ASSERT_EQ((int)cluster_pi_discard_classify_locked(&shape),
				 (int)CLUSTER_BUFMGR_PI_DISCARD_CORRUPT);
	shape = exact_pi_shape();
	shape.refcount = 1;
	shape.own_shape = CLUSTER_PCM_OWN_BUSY;
	shape.own_flags = PCM_OWN_FLAG_REVOKING;
	before = shape;
	combined_result = cluster_pi_discard_classify_locked(&shape);
	UT_ASSERT_EQ((int)combined_result,
				 (int)CLUSTER_BUFMGR_PI_DISCARD_CORRUPT);
	UT_ASSERT_EQ(memcmp(&shape, &before, sizeof(shape)), 0);
	UT_ASSERT_EQ((int)cluster_pi_discard_action(combined_result),
				 (int)CLUSTER_PI_DISCARD_ACTION_FAIL_CLOSED);
	shape = exact_pi_shape();
	shape.refcount = 1;
	shape.writer_activation_token = 99;
	before = shape;
	combined_result = cluster_pi_discard_classify_locked(&shape);
	UT_ASSERT_EQ((int)combined_result,
				 (int)CLUSTER_BUFMGR_PI_DISCARD_CORRUPT);
	UT_ASSERT_EQ(memcmp(&shape, &before, sizeof(shape)), 0);
	UT_ASSERT_EQ((int)cluster_pi_discard_action(combined_result),
				 (int)CLUSTER_PI_DISCARD_ACTION_FAIL_CLOSED);

	UT_ASSERT_EQ(
		(int)cluster_pi_discard_action(CLUSTER_BUFMGR_PI_DISCARD_CORRUPT),
		(int)CLUSTER_PI_DISCARD_ACTION_FAIL_CLOSED);
}

UT_TEST(test_exact_pi_is_the_only_drop_eligible_shape)
{
	ClusterPiDiscardShape shape = exact_pi_shape();

	UT_ASSERT_EQ((int)cluster_pi_discard_classify_locked(&shape),
				 (int)CLUSTER_BUFMGR_PI_DISCARD_DROPPED);
	UT_ASSERT_EQ(
		(int)cluster_pi_discard_action(CLUSTER_BUFMGR_PI_DISCARD_DROPPED),
		(int)CLUSTER_PI_DISCARD_ACTION_TERMINAL_DROPPED);

	shape.tag_matches = false;
	UT_ASSERT_EQ((int)cluster_pi_discard_classify_locked(&shape),
				 (int)CLUSTER_BUFMGR_PI_DISCARD_TERMINAL_NOOP);
}

UT_TEST(test_exact_pi_commit_plan_is_terminal_only_after_shadow_and_tail)
{
	ClusterPiDiscardShape shape = exact_pi_shape();
	ClusterBufmgrPiDiscardResult classification;
	ClusterPiDiscardCommitPlan plan;
	bool shadow_present = true;
	int common_tail_calls = 0;
	static const ClusterPcmOwnResult corrupt_results[] = {
		CLUSTER_PCM_OWN_STALE,
		CLUSTER_PCM_OWN_EXHAUSTED,
		CLUSTER_PCM_OWN_NOT_READY,
		CLUSTER_PCM_OWN_CORRUPT,
		CLUSTER_PCM_OWN_INVALID,
	};
	int i;

	classification = cluster_pi_discard_classify_locked(&shape);
	plan = cluster_pi_discard_commit_plan(classification, CLUSTER_PCM_OWN_OK);
	if (plan.clear_shadow)
		shadow_present = false;
	if (plan.run_common_tail)
		common_tail_calls++;
	UT_ASSERT_EQ((int)plan.result, (int)CLUSTER_BUFMGR_PI_DISCARD_DROPPED);
	UT_ASSERT(!shadow_present);
	UT_ASSERT_EQ(common_tail_calls, 1);

	plan = cluster_pi_discard_commit_plan(
		classification, CLUSTER_PCM_OWN_BUSY);
	UT_ASSERT_EQ((int)plan.result,
				 (int)CLUSTER_BUFMGR_PI_DISCARD_RETRYABLE_BUSY);
	UT_ASSERT(!plan.clear_shadow);
	UT_ASSERT(!plan.run_common_tail);

	for (i = 0; i < (int)lengthof(corrupt_results); i++)
	{
		plan = cluster_pi_discard_commit_plan(
			classification, corrupt_results[i]);
		UT_ASSERT_EQ((int)plan.result,
					 (int)CLUSTER_BUFMGR_PI_DISCARD_CORRUPT);
		UT_ASSERT(!plan.clear_shadow);
		UT_ASSERT(!plan.run_common_tail);
	}

	classification = CLUSTER_BUFMGR_PI_DISCARD_RETRYABLE_BUSY;
	plan = cluster_pi_discard_commit_plan(classification, CLUSTER_PCM_OWN_OK);
	UT_ASSERT_EQ((int)plan.result,
				 (int)CLUSTER_BUFMGR_PI_DISCARD_RETRYABLE_BUSY);
	UT_ASSERT(!plan.clear_shadow);
	UT_ASSERT(!plan.run_common_tail);

	plan = cluster_pi_discard_commit_plan(
		CLUSTER_BUFMGR_PI_DISCARD_TERMINAL_NOOP, CLUSTER_PCM_OWN_OK);
	UT_ASSERT_EQ((int)plan.result,
				 (int)CLUSTER_BUFMGR_PI_DISCARD_TERMINAL_NOOP);
	UT_ASSERT(!plan.clear_shadow);
	UT_ASSERT(!plan.run_common_tail);

	plan = cluster_pi_discard_commit_plan(
		(ClusterBufmgrPiDiscardResult)99, CLUSTER_PCM_OWN_OK);
	UT_ASSERT_EQ((int)plan.result,
				 (int)CLUSTER_BUFMGR_PI_DISCARD_CORRUPT);
	UT_ASSERT(!plan.clear_shadow);
	UT_ASSERT(!plan.run_common_tail);
}

UT_TEST(test_pi_replay_lot_is_bounded_and_does_not_touch_normal_lot)
{
	struct
	{
		unsigned char normal_invalidate_lot[32];
		ClusterPiDiscardParkSlot pi_lot[2];
		unsigned char tail_guard[32];
	} state;
	unsigned char normal_before[sizeof(state.normal_invalidate_lot)];
	unsigned char tail_before[sizeof(state.tail_guard)];
	ClusterPiDiscardParkSlot full_before[lengthof(state.pi_lot)];
	BufferTag tag1 = {0};
	BufferTag tag2 = {0};
	BufferTag tag3 = {0};

	memset(&state, 0, sizeof(state));
	memset(state.normal_invalidate_lot, 0xa5, sizeof(state.normal_invalidate_lot));
	memset(state.tail_guard, 0x5a, sizeof(state.tail_guard));
	memcpy(normal_before, state.normal_invalidate_lot, sizeof(normal_before));
	memcpy(tail_before, state.tail_guard, sizeof(tail_before));
	tag1.blockNum = 11;
	tag2.blockNum = 22;
	tag3.blockNum = 33;

	UT_ASSERT_EQ(
		(int)cluster_pi_discard_park_offer(state.pi_lot,
										 lengthof(state.pi_lot), tag1, 7),
		(int)CLUSTER_PI_DISCARD_PARK_INSERTED);
	UT_ASSERT_EQ(
		(int)cluster_pi_discard_park_offer(state.pi_lot,
										 lengthof(state.pi_lot), tag1, 8),
		(int)CLUSTER_PI_DISCARD_PARK_UPDATED);
	UT_ASSERT_EQ(state.pi_lot[0].epoch, (uint64)8);
	UT_ASSERT_EQ(
		(int)cluster_pi_discard_park_offer(state.pi_lot,
										 lengthof(state.pi_lot), tag2, 9),
		(int)CLUSTER_PI_DISCARD_PARK_INSERTED);
	memcpy(full_before, state.pi_lot, sizeof(full_before));

	UT_ASSERT_EQ(
		(int)cluster_pi_discard_park_offer(state.pi_lot,
										 lengthof(state.pi_lot), tag3, 10),
		(int)CLUSTER_PI_DISCARD_PARK_FULL);
	UT_ASSERT_EQ(memcmp(state.pi_lot, full_before, sizeof(full_before)), 0);
	UT_ASSERT_EQ(memcmp(state.normal_invalidate_lot, normal_before,
					   sizeof(normal_before)), 0);
	UT_ASSERT_EQ(memcmp(state.tail_guard, tail_before, sizeof(tail_before)), 0);
}

int
main(void)
{
	UT_PLAN(6);
	UT_RUN(test_final_raced_pin_is_retryable_and_classifier_is_pure);
	UT_RUN(test_terminal_replacement_is_distinct_from_retryable_pin);
	UT_RUN(test_contradictory_pi_shapes_fail_closed);
	UT_RUN(test_exact_pi_is_the_only_drop_eligible_shape);
	UT_RUN(test_exact_pi_commit_plan_is_terminal_only_after_shadow_and_tail);
	UT_RUN(test_pi_replay_lot_is_bounded_and_does_not_touch_normal_lot);
	UT_DONE();
	return ut_failed_count == 0 ? 0 : 1;
}
