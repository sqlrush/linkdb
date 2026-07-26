/*-------------------------------------------------------------------------
 *
 * test_cluster_lms_native_probe_state.c
 *	  Behavioral tests for the production LMS native-probe slot lifecycle.
 *
 * The observer hook is a controllable barrier between reservation and
 * publication.  It pins the concurrency contract that an LMS tick, reply,
 * or retry observer cannot claim a half-configured slot.
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include <stdlib.h>

#include "cluster/cluster_ges.h"
#include "cluster/cluster_lms_native_probe_state.h"

#undef printf
#undef fprintf
#undef snprintf

#include "unit_test.h"

UT_DEFINE_GLOBALS();

void
ExceptionalCondition(const char *conditionName pg_attribute_unused(),
					 const char *fileName pg_attribute_unused(),
					 int lineNumber pg_attribute_unused())
{
	abort();
}

typedef void (*NativeProbeBarrierHook)(pg_atomic_uint64 *state);

static int reserved_barrier_visits;
static int error_barrier_visits;

static void
observe_reserved_slot(pg_atomic_uint64 *state)
{
	reserved_barrier_visits++;
	UT_ASSERT_EQ(cluster_lms_native_probe_state_read(state),
				 CLUSTER_LMS_NATIVE_PROBE_SLOT_RESERVED);
	UT_ASSERT(!cluster_lms_native_probe_state_is_active(state));
	UT_ASSERT(!cluster_lms_native_probe_state_try_claim_resolving(state));
}

static void
abort_reserved_slot(pg_atomic_uint64 *state)
{
	error_barrier_visits++;
	UT_ASSERT(cluster_lms_native_probe_state_try_release(state));
}

static bool
reserve_configure_publish(pg_atomic_uint64 *state, NativeProbeBarrierHook hook)
{
	if (!cluster_lms_native_probe_state_try_reserve(state))
		return false;
	if (hook != NULL)
		hook(state);
	return cluster_lms_native_probe_state_try_publish_active(state);
}

UT_TEST(test_reserved_is_invisible_until_single_publish)
{
	pg_atomic_uint64 state;

	pg_atomic_init_u64(&state, CLUSTER_LMS_NATIVE_PROBE_SLOT_FREE);
	reserved_barrier_visits = 0;

	UT_ASSERT(reserve_configure_publish(&state, observe_reserved_slot));
	UT_ASSERT_EQ(reserved_barrier_visits, 1);
	UT_ASSERT(cluster_lms_native_probe_state_is_active(&state));
	UT_ASSERT_EQ(cluster_lms_native_probe_state_read(&state),
				 CLUSTER_LMS_NATIVE_PROBE_SLOT_ACTIVE);
	UT_ASSERT(!cluster_lms_native_probe_state_try_publish_active(&state));
}

UT_TEST(test_terminal_resolution_has_exactly_one_owner)
{
	pg_atomic_uint64 state;

	pg_atomic_init_u64(&state, CLUSTER_LMS_NATIVE_PROBE_SLOT_FREE);
	UT_ASSERT(reserve_configure_publish(&state, NULL));
	UT_ASSERT(cluster_lms_native_probe_state_try_claim_resolving(&state));
	UT_ASSERT_EQ(cluster_lms_native_probe_state_read(&state),
				 CLUSTER_LMS_NATIVE_PROBE_SLOT_RESOLVING);
	UT_ASSERT(!cluster_lms_native_probe_state_try_claim_resolving(&state));
	UT_ASSERT(cluster_lms_native_probe_state_try_release(&state));
	UT_ASSERT_EQ(cluster_lms_native_probe_state_read(&state),
				 CLUSTER_LMS_NATIVE_PROBE_SLOT_FREE);
	UT_ASSERT(!cluster_lms_native_probe_state_try_release(&state));
}

UT_TEST(test_failed_init_and_active_cancel_are_reclaimable)
{
	pg_atomic_uint64 state;

	pg_atomic_init_u64(&state, CLUSTER_LMS_NATIVE_PROBE_SLOT_FREE);
	UT_ASSERT(cluster_lms_native_probe_state_try_reserve(&state));
	UT_ASSERT(cluster_lms_native_probe_state_try_release(&state));
	UT_ASSERT_EQ(cluster_lms_native_probe_state_read(&state),
				 CLUSTER_LMS_NATIVE_PROBE_SLOT_FREE);

	UT_ASSERT(reserve_configure_publish(&state, NULL));
	UT_ASSERT(cluster_lms_native_probe_state_try_release(&state));
	UT_ASSERT_EQ(cluster_lms_native_probe_state_read(&state),
				 CLUSTER_LMS_NATIVE_PROBE_SLOT_FREE);
}

UT_TEST(test_error_barrier_and_cancel_reopen_collector_capacity)
{
	pg_atomic_uint64 state;

	pg_atomic_init_u64(&state, CLUSTER_LMS_NATIVE_PROBE_SLOT_FREE);
	error_barrier_visits = 0;

	/* ERROR between reserve and publish: FINALLY releases RESERVED. */
	UT_ASSERT(!reserve_configure_publish(&state, abort_reserved_slot));
	UT_ASSERT_EQ(error_barrier_visits, 1);
	UT_ASSERT(cluster_lms_native_probe_state_try_reserve(&state));
	UT_ASSERT(cluster_lms_native_probe_state_try_publish_active(&state));

	/* Backend cancel while waiting releases ACTIVE and capacity is reusable. */
	UT_ASSERT(cluster_lms_native_probe_state_try_release(&state));
	UT_ASSERT(cluster_lms_native_probe_state_try_reserve(&state));
	UT_ASSERT(cluster_lms_native_probe_state_try_publish_active(&state));

	/* ERROR after terminal claim also reclaims RESOLVING. */
	UT_ASSERT(cluster_lms_native_probe_state_try_claim_resolving(&state));
	UT_ASSERT(cluster_lms_native_probe_state_try_release(&state));
	UT_ASSERT(cluster_lms_native_probe_state_try_reserve(&state));
	UT_ASSERT(cluster_lms_native_probe_state_try_release(&state));
}

static LOCKTAG
relation_locktag(Oid database_oid, Oid relation_oid)
{
	LOCKTAG tag;

	SET_LOCKTAG_RELATION(tag, database_oid, relation_oid);
	return tag;
}

UT_TEST(test_force_clear_seam_unarmed_does_not_trigger)
{
	ClusterLmsNativeProbeForceClearOnce seam;
	LOCKTAG target = relation_locktag(5, 2964);

	cluster_lms_native_probe_force_clear_once_init(
		&seam, 0, 1, &target, GES_REQ_OPCODE_CONVERT,
		AccessExclusiveLock);
	UT_ASSERT(!cluster_lms_native_probe_force_clear_once_try_consume(
		&seam, 1, &target, GES_REQ_OPCODE_CONVERT,
		AccessExclusiveLock));
	UT_ASSERT_EQ(pg_atomic_read_u32(&seam.remaining), 0);
}

UT_TEST(test_force_clear_seam_wrong_node_does_not_consume)
{
	ClusterLmsNativeProbeForceClearOnce seam;
	LOCKTAG target = relation_locktag(5, 2964);

	cluster_lms_native_probe_force_clear_once_init(
		&seam, 1, 1, &target, GES_REQ_OPCODE_CONVERT,
		AccessExclusiveLock);
	UT_ASSERT(!cluster_lms_native_probe_force_clear_once_try_consume(
		&seam, 0, &target, GES_REQ_OPCODE_CONVERT,
		AccessExclusiveLock));
	UT_ASSERT_EQ(pg_atomic_read_u32(&seam.remaining), 1);
	UT_ASSERT(cluster_lms_native_probe_force_clear_once_try_consume(
		&seam, 1, &target, GES_REQ_OPCODE_CONVERT,
		AccessExclusiveLock));
}

UT_TEST(test_force_clear_seam_wrong_full_locktag_does_not_consume)
{
	ClusterLmsNativeProbeForceClearOnce seam;
	LOCKTAG target = relation_locktag(5, 2964);
	LOCKTAG wrong = target;

	wrong.locktag_field3 = 1;
	cluster_lms_native_probe_force_clear_once_init(
		&seam, 1, 1, &target, GES_REQ_OPCODE_CONVERT,
		AccessExclusiveLock);
	UT_ASSERT(!cluster_lms_native_probe_force_clear_once_try_consume(
		&seam, 1, &wrong, GES_REQ_OPCODE_CONVERT,
		AccessExclusiveLock));
	UT_ASSERT_EQ(pg_atomic_read_u32(&seam.remaining), 1);
	UT_ASSERT(cluster_lms_native_probe_force_clear_once_try_consume(
		&seam, 1, &target, GES_REQ_OPCODE_CONVERT,
		AccessExclusiveLock));
}

UT_TEST(test_force_clear_seam_wrong_opcode_same_tag_and_mode_does_not_consume)
{
	ClusterLmsNativeProbeForceClearOnce seam;
	LOCKTAG target = relation_locktag(5, 2964);

	cluster_lms_native_probe_force_clear_once_init(
		&seam, 1, 1, &target, GES_REQ_OPCODE_CONVERT,
		AccessExclusiveLock);
	UT_ASSERT(!cluster_lms_native_probe_force_clear_once_try_consume(
		&seam, 1, &target, GES_REQ_OPCODE_REQUEST,
		AccessExclusiveLock));
	UT_ASSERT_EQ(pg_atomic_read_u32(&seam.remaining), 1);
	UT_ASSERT(cluster_lms_native_probe_force_clear_once_try_consume(
		&seam, 1, &target, GES_REQ_OPCODE_CONVERT,
		AccessExclusiveLock));
}

UT_TEST(test_force_clear_seam_wrong_mode_same_tag_and_opcode_does_not_consume)
{
	ClusterLmsNativeProbeForceClearOnce seam;
	LOCKTAG target = relation_locktag(5, 2964);

	cluster_lms_native_probe_force_clear_once_init(
		&seam, 1, 1, &target, GES_REQ_OPCODE_CONVERT,
		AccessExclusiveLock);
	UT_ASSERT(!cluster_lms_native_probe_force_clear_once_try_consume(
		&seam, 1, &target, GES_REQ_OPCODE_CONVERT, ShareLock));
	UT_ASSERT_EQ(pg_atomic_read_u32(&seam.remaining), 1);
	UT_ASSERT(cluster_lms_native_probe_force_clear_once_try_consume(
		&seam, 1, &target, GES_REQ_OPCODE_CONVERT,
		AccessExclusiveLock));
}

UT_TEST(test_force_clear_seam_is_exactly_one_shot)
{
	ClusterLmsNativeProbeForceClearOnce seam;
	LOCKTAG target = relation_locktag(5, 2964);

	cluster_lms_native_probe_force_clear_once_init(
		&seam, 1, 1, &target, GES_REQ_OPCODE_CONVERT,
		AccessExclusiveLock);
	UT_ASSERT(cluster_lms_native_probe_force_clear_once_try_consume(
		&seam, 1, &target, GES_REQ_OPCODE_CONVERT,
		AccessExclusiveLock));
	UT_ASSERT_EQ(pg_atomic_read_u32(&seam.remaining), 0);
	UT_ASSERT(!cluster_lms_native_probe_force_clear_once_try_consume(
		&seam, 1, &target, GES_REQ_OPCODE_CONVERT,
		AccessExclusiveLock));
}

int
main(void)
{
	UT_PLAN(10);

	UT_RUN(test_reserved_is_invisible_until_single_publish);
	UT_RUN(test_terminal_resolution_has_exactly_one_owner);
	UT_RUN(test_failed_init_and_active_cancel_are_reclaimable);
	UT_RUN(test_error_barrier_and_cancel_reopen_collector_capacity);
	UT_RUN(test_force_clear_seam_unarmed_does_not_trigger);
	UT_RUN(test_force_clear_seam_wrong_node_does_not_consume);
	UT_RUN(test_force_clear_seam_wrong_full_locktag_does_not_consume);
	UT_RUN(test_force_clear_seam_wrong_opcode_same_tag_and_mode_does_not_consume);
	UT_RUN(test_force_clear_seam_wrong_mode_same_tag_and_opcode_does_not_consume);
	UT_RUN(test_force_clear_seam_is_exactly_one_shot);

	UT_DONE();
	return ut_failed_count == 0 ? 0 : 1;
}
