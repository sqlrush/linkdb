/*-------------------------------------------------------------------------
 *
 * test_cluster_r4_slot_reservation.c
 *	Stage 8 R4 D4-B common legacy slot-reservation proof window.
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "cluster/cluster_cr_server.h"
#include "cluster/cluster_lms.h"

#undef printf
#undef snprintf

#include "unit_test.h"

#ifndef CR_SERVER_SOURCE_PATH
#error "CR_SERVER_SOURCE_PATH must identify cluster_cr_server.c"
#endif

UT_DEFINE_GLOBALS();

extern bool cluster_cr_server_test_reserve_legacy_slot(ClusterLmsCrSlot *slot,
													uint32 reserved_state);

void
ExceptionalCondition(const char *condition_name pg_attribute_unused(),
					 const char *file_name pg_attribute_unused(),
					 int line_number pg_attribute_unused())
{
	abort();
}

static ClusterLmsSharedState *ut_lms_state;
static int ut_lock_acquire_count;
static int ut_lock_release_count;
static LWLock *ut_lock;
static LWLockMode ut_lock_mode;

ClusterLmsSharedState *
cluster_lms_shared_state(void)
{
	return ut_lms_state;
}

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
reset_fixture(ClusterLmsSharedState *state, ClusterLmsCrSlot *slot)
{
	memset(state, 0, sizeof(*state));
	memset(slot, 0, sizeof(*slot));
	pg_atomic_init_u32(&slot->state, CLUSTER_LMS_CR_FREE);
	ut_lms_state = state;
	ut_lock_acquire_count = 0;
	ut_lock_release_count = 0;
	ut_lock = NULL;
	ut_lock_mode = LW_SHARED;
}

static void
dirty_owner_stamp(ClusterLmsCrSlot *slot)
{
	memset(&slot->r4.owner, 0xa5, sizeof(slot->r4.owner));
}

static bool
owner_stamp_is_zero(const ClusterLmsCrSlot *slot)
{
	ClusterR4CrOwnerStamp zero;

	memset(&zero, 0, sizeof(zero));
	return memcmp(&slot->r4.owner, &zero, sizeof(zero)) == 0;
}

UT_TEST(test_free_to_pending_canonicalizes_owner_under_lms_lock)
{
	ClusterLmsSharedState state;
	ClusterLmsCrSlot slot;

	reset_fixture(&state, &slot);
	dirty_owner_stamp(&slot);

	UT_ASSERT(cluster_cr_server_test_reserve_legacy_slot(&slot, CLUSTER_LMS_CR_PENDING));
	UT_ASSERT_EQ(pg_atomic_read_u32(&slot.state), CLUSTER_LMS_CR_PENDING);
	UT_ASSERT(owner_stamp_is_zero(&slot));
	UT_ASSERT_EQ(ut_lock_acquire_count, 1);
	UT_ASSERT_EQ(ut_lock_release_count, 1);
	UT_ASSERT(ut_lock == &state.lwlock);
	UT_ASSERT_EQ(ut_lock_mode, LW_EXCLUSIVE);
}

UT_TEST(test_free_to_filling_uses_same_proof_window)
{
	ClusterLmsSharedState state;
	ClusterLmsCrSlot slot;

	reset_fixture(&state, &slot);
	dirty_owner_stamp(&slot);

	UT_ASSERT(cluster_cr_server_test_reserve_legacy_slot(&slot, CLUSTER_LMS_CR_FILLING));
	UT_ASSERT_EQ(pg_atomic_read_u32(&slot.state), CLUSTER_LMS_CR_FILLING);
	UT_ASSERT(owner_stamp_is_zero(&slot));
	UT_ASSERT_EQ(ut_lock_acquire_count, 1);
	UT_ASSERT_EQ(ut_lock_release_count, 1);
}

UT_TEST(test_busy_slot_preserves_winner_owner_stamp)
{
	ClusterLmsSharedState state;
	ClusterLmsCrSlot slot;
	ClusterR4CrOwnerStamp before;

	reset_fixture(&state, &slot);
	pg_atomic_write_u32(&slot.state, CLUSTER_LMS_CR_R4_QUEUED);
	dirty_owner_stamp(&slot);
	before = slot.r4.owner;

	UT_ASSERT(!cluster_cr_server_test_reserve_legacy_slot(&slot, CLUSTER_LMS_CR_PENDING));
	UT_ASSERT_EQ(pg_atomic_read_u32(&slot.state), CLUSTER_LMS_CR_R4_QUEUED);
	UT_ASSERT(memcmp(&slot.r4.owner, &before, sizeof(before)) == 0);
	UT_ASSERT_EQ(ut_lock_acquire_count, 1);
	UT_ASSERT_EQ(ut_lock_release_count, 1);
}

UT_TEST(test_missing_lms_state_refuses_before_mutation)
{
	ClusterLmsSharedState state;
	ClusterLmsCrSlot slot;
	ClusterLmsCrSlot before;

	reset_fixture(&state, &slot);
	dirty_owner_stamp(&slot);
	before = slot;
	ut_lms_state = NULL;

	UT_ASSERT(!cluster_cr_server_test_reserve_legacy_slot(&slot, CLUSTER_LMS_CR_PENDING));
	UT_ASSERT(memcmp(&slot, &before, sizeof(slot)) == 0);
	UT_ASSERT_EQ(ut_lock_acquire_count, 0);
	UT_ASSERT_EQ(ut_lock_release_count, 0);
}

static char *
read_source(void)
{
	FILE *fp;
	char *contents;
	long length;

	fp = fopen(CR_SERVER_SOURCE_PATH, "rb");
	if (fp == NULL || fseek(fp, 0, SEEK_END) != 0)
		return NULL;
	length = ftell(fp);
	if (length <= 0 || fseek(fp, 0, SEEK_SET) != 0) {
		fclose(fp);
		return NULL;
	}
	contents = malloc((size_t)length + 1);
	if (contents == NULL || fread(contents, 1, (size_t)length, fp) != (size_t)length) {
		free(contents);
		fclose(fp);
		return NULL;
	}
	fclose(fp);
	contents[length] = '\0';
	return contents;
}

static bool
function_uses_common_reserver(const char *source, const char *symbol, const char *next_symbol)
{
	char start_marker[128];
	char end_marker[128];
	const char *start;
	const char *end;
	const char *call;

	if (source == NULL)
		return false;
	snprintf(start_marker, sizeof(start_marker), "\n%s(", symbol);
	snprintf(end_marker, sizeof(end_marker), "\n%s(", next_symbol);
	start = strstr(source, start_marker);
	end = start != NULL ? strstr(start + 1, end_marker) : NULL;
	call = start != NULL ? strstr(start, "cr_server_reserve_legacy_slot(slot,") : NULL;
	return start != NULL && end != NULL && call != NULL && call < end;
}

UT_TEST(test_all_four_legacy_submitters_use_common_reserver)
{
	char *source = read_source();

	UT_ASSERT_NOT_NULL(source);
	if (source != NULL) {
		UT_ASSERT(function_uses_common_reserver(source, "cluster_lms_cr_submit",
										 "cluster_lms_cr_submit_r4"));
		UT_ASSERT(function_uses_common_reserver(source, "cluster_lms_undo_fetch_submit",
										 "cluster_lms_undo_verdict_submit"));
		UT_ASSERT(function_uses_common_reserver(source, "cluster_lms_undo_verdict_submit",
										 "cluster_lms_undo_multi_verdict_submit"));
		UT_ASSERT(function_uses_common_reserver(source, "cluster_lms_undo_multi_verdict_submit",
										 "lms_undo_fetch_serve"));
	}
	free(source);
}

int
main(void)
{
	UT_PLAN(5);
	UT_RUN(test_free_to_pending_canonicalizes_owner_under_lms_lock);
	UT_RUN(test_free_to_filling_uses_same_proof_window);
	UT_RUN(test_busy_slot_preserves_winner_owner_stamp);
	UT_RUN(test_missing_lms_state_refuses_before_mutation);
	UT_RUN(test_all_four_legacy_submitters_use_common_reserver);
	UT_DONE();
	return ut_failed_count == 0 ? 0 : 1;
}
