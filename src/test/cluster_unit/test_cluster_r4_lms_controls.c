/*-------------------------------------------------------------------------
 *
 * test_cluster_r4_lms_controls.c
 *	Stage 8 R4 D4-B LMS worker-incarnation publication.
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "cluster/cluster_lms.h"

#undef printf

#include "unit_test.h"

UT_DEFINE_GLOBALS();

extern uint64 cluster_lms_test_publish_r4_worker_incarnation(ClusterLmsSharedState *state,
													 int worker_id);

static int ut_lock_acquire_count;
static int ut_lock_release_count;
static LWLock *ut_lock;
static LWLockMode ut_lock_mode;

bool
LWLockAcquire(LWLock *lock, LWLockMode mode)
{
	ut_lock_acquire_count++;
	ut_lock = lock;
	ut_lock_mode = mode;
	return true;
}

void
LWLockRelease(LWLock *lock)
{
	ut_lock_release_count++;
	UT_ASSERT(lock == ut_lock);
}

static void
reset_fixture(ClusterLmsSharedState *state)
{
	memset(state, 0, sizeof(*state));
	ut_lock_acquire_count = 0;
	ut_lock_release_count = 0;
	ut_lock = NULL;
	ut_lock_mode = LW_SHARED;
}

UT_TEST(test_worker0_clears_stale_ack_before_fresh_incarnation)
{
	ClusterLmsSharedState state;
	uint64 published;

	reset_fixture(&state);
	state.r4_controls.data_worker_incarnation[0] = UINT64_C(7);
	state.r4_controls.drain_request_generation = UINT64_C(17);
	state.r4_controls.drain_ack_generation = UINT64_C(99);

	published = cluster_lms_test_publish_r4_worker_incarnation(&state, 0);

	UT_ASSERT_EQ(published, UINT64_C(8));
	UT_ASSERT_EQ(state.r4_controls.data_worker_incarnation[0], UINT64_C(8));
	UT_ASSERT_EQ(state.r4_controls.drain_request_generation, UINT64_C(17));
	UT_ASSERT_EQ(state.r4_controls.drain_ack_generation, UINT64_C(0));
	UT_ASSERT_EQ(ut_lock_acquire_count, 1);
	UT_ASSERT_EQ(ut_lock_release_count, 1);
	UT_ASSERT(ut_lock == &state.lwlock);
	UT_ASSERT_EQ(ut_lock_mode, LW_EXCLUSIVE);
}

UT_TEST(test_nonzero_worker_publishes_only_own_incarnation)
{
	ClusterLmsSharedState state;
	uint64 published;

	reset_fixture(&state);
	state.r4_controls.data_worker_incarnation[0] = UINT64_C(7);
	state.r4_controls.data_worker_incarnation[3] = UINT64_C(41);
	state.r4_controls.drain_request_generation = UINT64_C(17);
	state.r4_controls.drain_ack_generation = UINT64_C(99);

	published = cluster_lms_test_publish_r4_worker_incarnation(&state, 3);

	UT_ASSERT_EQ(published, UINT64_C(42));
	UT_ASSERT_EQ(state.r4_controls.data_worker_incarnation[0], UINT64_C(7));
	UT_ASSERT_EQ(state.r4_controls.data_worker_incarnation[3], UINT64_C(42));
	UT_ASSERT_EQ(state.r4_controls.drain_request_generation, UINT64_C(17));
	UT_ASSERT_EQ(state.r4_controls.drain_ack_generation, UINT64_C(99));
	UT_ASSERT_EQ(ut_lock_acquire_count, 1);
	UT_ASSERT_EQ(ut_lock_release_count, 1);
}

UT_TEST(test_incarnation_exhaustion_refuses_without_wrap)
{
	ClusterLmsSharedState state;
	uint64 published;

	reset_fixture(&state);
	state.r4_controls.data_worker_incarnation[0] = UINT64_MAX;
	state.r4_controls.drain_ack_generation = UINT64_C(99);

	published = cluster_lms_test_publish_r4_worker_incarnation(&state, 0);

	UT_ASSERT_EQ(published, UINT64_C(0));
	UT_ASSERT_EQ(state.r4_controls.data_worker_incarnation[0], UINT64_MAX);
	UT_ASSERT_EQ(state.r4_controls.drain_ack_generation, UINT64_C(0));
	UT_ASSERT_EQ(ut_lock_acquire_count, 1);
	UT_ASSERT_EQ(ut_lock_release_count, 1);
}

UT_TEST(test_invalid_worker_refuses_without_lock_or_mutation)
{
	ClusterLmsSharedState state;
	ClusterLmsSharedState before;

	reset_fixture(&state);
	state.r4_controls.data_worker_incarnation[0] = UINT64_C(7);
	state.r4_controls.drain_ack_generation = UINT64_C(99);
	before = state;

	UT_ASSERT_EQ(cluster_lms_test_publish_r4_worker_incarnation(&state, -1), UINT64_C(0));
	UT_ASSERT_EQ(cluster_lms_test_publish_r4_worker_incarnation(
					 &state, CLUSTER_LMS_MAX_WORKERS),
				 UINT64_C(0));
	UT_ASSERT(memcmp(&state, &before, sizeof(state)) == 0);
	UT_ASSERT_EQ(ut_lock_acquire_count, 0);
	UT_ASSERT_EQ(ut_lock_release_count, 0);
}

int
main(void)
{
	UT_PLAN(4);
	UT_RUN(test_worker0_clears_stale_ack_before_fresh_incarnation);
	UT_RUN(test_nonzero_worker_publishes_only_own_incarnation);
	UT_RUN(test_incarnation_exhaustion_refuses_without_wrap);
	UT_RUN(test_invalid_worker_refuses_without_lock_or_mutation);
	UT_DONE();
	return ut_failed_count == 0 ? 0 : 1;
}
