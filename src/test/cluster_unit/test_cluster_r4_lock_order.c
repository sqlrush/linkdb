/*-------------------------------------------------------------------------
 *
 * test_cluster_r4_lock_order.c
 *	  Control-plane wait-for and held-lock policy tests for R4 D13.
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "cluster/cluster_semantic_activation.h"
#include "storage/shmem.h"

#include "cluster_r4_activation_test_stubs.h"

void *
ShmemInitStruct(const char *name pg_attribute_unused(), Size size pg_attribute_unused(),
				bool *foundPtr pg_attribute_unused())
{
	return NULL;
}

/* Exercise the real product-local policy helpers without exporting a test API. */
#include "../../backend/cluster/cluster_semantic_activation.c"

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

#define DEFINE_WAIT_ALLOWED_TEST(test_name, edge_value)                                            \
	UT_TEST(test_name)                                                                             \
	{                                                                                              \
		UT_ASSERT(semantic_activation_control_wait_allowed((edge_value),                           \
														   SEMANTIC_ACTIVATION_HELD_NONE));        \
	}

#define DEFINE_WAIT_FORBIDDEN_TEST(test_name, edge_value, lock_value)                              \
	UT_TEST(test_name)                                                                             \
	{                                                                                              \
		UT_ASSERT(!semantic_activation_control_wait_allowed((edge_value), (lock_value)));          \
	}

UT_TEST(test_01_held_lock_bits_are_independent)
{
	UT_ASSERT_EQ(SEMANTIC_ACTIVATION_HELD_RESOURCE, UINT32_C(1));
	UT_ASSERT_EQ(SEMANTIC_ACTIVATION_HELD_BUFFER, UINT32_C(2));
	UT_ASSERT_EQ(SEMANTIC_ACTIVATION_HELD_SLRU, UINT32_C(4));
	UT_ASSERT_EQ(SEMANTIC_ACTIVATION_HELD_UNDO_IO, UINT32_C(8));
	UT_ASSERT_EQ(SEMANTIC_ACTIVATION_HELD_IC_DISPATCH, UINT32_C(16));
}

UT_TEST(test_02_wait_edge_values_are_closed)
{
	UT_ASSERT_EQ(SEMANTIC_ACTIVATION_WAIT_UTILITY_TO_LMON, 0);
	UT_ASSERT_EQ(SEMANTIC_ACTIVATION_WAIT_LMON_TO_QVOTEC, 1);
	UT_ASSERT_EQ(SEMANTIC_ACTIVATION_WAIT_LMON_TO_PEER_ACK, 2);
	UT_ASSERT_EQ(SEMANTIC_ACTIVATION_WAIT_LMON_TO_CONTROL_BARRIER, 3);
}

DEFINE_WAIT_ALLOWED_TEST(test_03_utility_to_lmon_wait_with_no_lock_is_allowed,
						 SEMANTIC_ACTIVATION_WAIT_UTILITY_TO_LMON)
DEFINE_WAIT_FORBIDDEN_TEST(test_04_utility_wait_rejects_resource_lock,
						   SEMANTIC_ACTIVATION_WAIT_UTILITY_TO_LMON,
						   SEMANTIC_ACTIVATION_HELD_RESOURCE)
DEFINE_WAIT_FORBIDDEN_TEST(test_05_utility_wait_rejects_buffer_lock,
						   SEMANTIC_ACTIVATION_WAIT_UTILITY_TO_LMON,
						   SEMANTIC_ACTIVATION_HELD_BUFFER)
DEFINE_WAIT_FORBIDDEN_TEST(test_06_utility_wait_rejects_slru_lock,
						   SEMANTIC_ACTIVATION_WAIT_UTILITY_TO_LMON, SEMANTIC_ACTIVATION_HELD_SLRU)
DEFINE_WAIT_FORBIDDEN_TEST(test_07_utility_wait_rejects_undo_io_ownership,
						   SEMANTIC_ACTIVATION_WAIT_UTILITY_TO_LMON,
						   SEMANTIC_ACTIVATION_HELD_UNDO_IO)
DEFINE_WAIT_FORBIDDEN_TEST(test_08_utility_wait_rejects_ic_dispatch_ownership,
						   SEMANTIC_ACTIVATION_WAIT_UTILITY_TO_LMON,
						   SEMANTIC_ACTIVATION_HELD_IC_DISPATCH)
DEFINE_WAIT_FORBIDDEN_TEST(test_09_utility_wait_rejects_combined_forbidden_locks,
						   SEMANTIC_ACTIVATION_WAIT_UTILITY_TO_LMON,
						   SEMANTIC_ACTIVATION_HELD_RESOURCE | SEMANTIC_ACTIVATION_HELD_BUFFER
							   | SEMANTIC_ACTIVATION_HELD_SLRU)

DEFINE_WAIT_ALLOWED_TEST(test_10_lmon_to_qvotec_wait_with_no_lock_is_allowed,
						 SEMANTIC_ACTIVATION_WAIT_LMON_TO_QVOTEC)
DEFINE_WAIT_FORBIDDEN_TEST(test_11_qvotec_wait_rejects_resource_lock,
						   SEMANTIC_ACTIVATION_WAIT_LMON_TO_QVOTEC,
						   SEMANTIC_ACTIVATION_HELD_RESOURCE)
DEFINE_WAIT_FORBIDDEN_TEST(test_12_qvotec_wait_rejects_buffer_lock,
						   SEMANTIC_ACTIVATION_WAIT_LMON_TO_QVOTEC, SEMANTIC_ACTIVATION_HELD_BUFFER)
DEFINE_WAIT_FORBIDDEN_TEST(test_13_qvotec_wait_rejects_slru_lock,
						   SEMANTIC_ACTIVATION_WAIT_LMON_TO_QVOTEC, SEMANTIC_ACTIVATION_HELD_SLRU)
DEFINE_WAIT_FORBIDDEN_TEST(test_14_qvotec_wait_rejects_undo_io_ownership,
						   SEMANTIC_ACTIVATION_WAIT_LMON_TO_QVOTEC,
						   SEMANTIC_ACTIVATION_HELD_UNDO_IO)
DEFINE_WAIT_FORBIDDEN_TEST(test_15_qvotec_wait_rejects_ic_dispatch_ownership,
						   SEMANTIC_ACTIVATION_WAIT_LMON_TO_QVOTEC,
						   SEMANTIC_ACTIVATION_HELD_IC_DISPATCH)
DEFINE_WAIT_FORBIDDEN_TEST(test_16_qvotec_wait_rejects_all_forbidden_locks,
						   SEMANTIC_ACTIVATION_WAIT_LMON_TO_QVOTEC,
						   SEMANTIC_ACTIVATION_HELD_ALL_FORBIDDEN)

DEFINE_WAIT_ALLOWED_TEST(test_17_peer_ack_wait_with_no_lock_is_allowed,
						 SEMANTIC_ACTIVATION_WAIT_LMON_TO_PEER_ACK)
DEFINE_WAIT_FORBIDDEN_TEST(test_18_peer_ack_wait_rejects_resource_lock,
						   SEMANTIC_ACTIVATION_WAIT_LMON_TO_PEER_ACK,
						   SEMANTIC_ACTIVATION_HELD_RESOURCE)
DEFINE_WAIT_FORBIDDEN_TEST(test_19_peer_ack_wait_rejects_buffer_lock,
						   SEMANTIC_ACTIVATION_WAIT_LMON_TO_PEER_ACK,
						   SEMANTIC_ACTIVATION_HELD_BUFFER)
DEFINE_WAIT_FORBIDDEN_TEST(test_20_peer_ack_wait_rejects_slru_lock,
						   SEMANTIC_ACTIVATION_WAIT_LMON_TO_PEER_ACK, SEMANTIC_ACTIVATION_HELD_SLRU)
DEFINE_WAIT_FORBIDDEN_TEST(test_21_peer_ack_wait_rejects_undo_io_ownership,
						   SEMANTIC_ACTIVATION_WAIT_LMON_TO_PEER_ACK,
						   SEMANTIC_ACTIVATION_HELD_UNDO_IO)
DEFINE_WAIT_FORBIDDEN_TEST(test_22_peer_ack_wait_rejects_ic_dispatch_ownership,
						   SEMANTIC_ACTIVATION_WAIT_LMON_TO_PEER_ACK,
						   SEMANTIC_ACTIVATION_HELD_IC_DISPATCH)

DEFINE_WAIT_ALLOWED_TEST(test_23_control_barrier_with_no_lock_is_allowed,
						 SEMANTIC_ACTIVATION_WAIT_LMON_TO_CONTROL_BARRIER)
DEFINE_WAIT_FORBIDDEN_TEST(test_24_control_barrier_rejects_resource_lock,
						   SEMANTIC_ACTIVATION_WAIT_LMON_TO_CONTROL_BARRIER,
						   SEMANTIC_ACTIVATION_HELD_RESOURCE)
DEFINE_WAIT_FORBIDDEN_TEST(test_25_control_barrier_rejects_buffer_lock,
						   SEMANTIC_ACTIVATION_WAIT_LMON_TO_CONTROL_BARRIER,
						   SEMANTIC_ACTIVATION_HELD_BUFFER)
DEFINE_WAIT_FORBIDDEN_TEST(test_26_control_barrier_rejects_slru_lock,
						   SEMANTIC_ACTIVATION_WAIT_LMON_TO_CONTROL_BARRIER,
						   SEMANTIC_ACTIVATION_HELD_SLRU)
DEFINE_WAIT_FORBIDDEN_TEST(test_27_control_barrier_rejects_undo_io_ownership,
						   SEMANTIC_ACTIVATION_WAIT_LMON_TO_CONTROL_BARRIER,
						   SEMANTIC_ACTIVATION_HELD_UNDO_IO)
DEFINE_WAIT_FORBIDDEN_TEST(test_28_control_barrier_rejects_ic_dispatch_ownership,
						   SEMANTIC_ACTIVATION_WAIT_LMON_TO_CONTROL_BARRIER,
						   SEMANTIC_ACTIVATION_HELD_IC_DISPATCH)

UT_TEST(test_29_process_utility_may_only_wait_on_lmon)
{
	UT_ASSERT(semantic_activation_actor_edge_allowed(SEMANTIC_ACTIVATION_ACTOR_PROCESS_UTILITY,
													 SEMANTIC_ACTIVATION_ACTOR_LMON));
}

UT_TEST(test_30_lmon_may_only_delegate_durable_io_to_qvotec)
{
	UT_ASSERT(semantic_activation_actor_edge_allowed(SEMANTIC_ACTIVATION_ACTOR_LMON,
													 SEMANTIC_ACTIVATION_ACTOR_QVOTEC));
}

UT_TEST(test_31_lmon_control_path_never_enters_holder_lms)
{
	UT_ASSERT(!semantic_activation_actor_edge_allowed(SEMANTIC_ACTIVATION_ACTOR_LMON,
													  SEMANTIC_ACTIVATION_ACTOR_LMS));
}

UT_TEST(test_32_qvotec_completion_never_enters_origin_data)
{
	UT_ASSERT(!semantic_activation_actor_edge_allowed(SEMANTIC_ACTIVATION_ACTOR_QVOTEC,
													  SEMANTIC_ACTIVATION_ACTOR_DATA));
}

int
main(void)
{
	UT_PLAN(32);
	UT_RUN(test_01_held_lock_bits_are_independent);
	UT_RUN(test_02_wait_edge_values_are_closed);
	UT_RUN(test_03_utility_to_lmon_wait_with_no_lock_is_allowed);
	UT_RUN(test_04_utility_wait_rejects_resource_lock);
	UT_RUN(test_05_utility_wait_rejects_buffer_lock);
	UT_RUN(test_06_utility_wait_rejects_slru_lock);
	UT_RUN(test_07_utility_wait_rejects_undo_io_ownership);
	UT_RUN(test_08_utility_wait_rejects_ic_dispatch_ownership);
	UT_RUN(test_09_utility_wait_rejects_combined_forbidden_locks);
	UT_RUN(test_10_lmon_to_qvotec_wait_with_no_lock_is_allowed);
	UT_RUN(test_11_qvotec_wait_rejects_resource_lock);
	UT_RUN(test_12_qvotec_wait_rejects_buffer_lock);
	UT_RUN(test_13_qvotec_wait_rejects_slru_lock);
	UT_RUN(test_14_qvotec_wait_rejects_undo_io_ownership);
	UT_RUN(test_15_qvotec_wait_rejects_ic_dispatch_ownership);
	UT_RUN(test_16_qvotec_wait_rejects_all_forbidden_locks);
	UT_RUN(test_17_peer_ack_wait_with_no_lock_is_allowed);
	UT_RUN(test_18_peer_ack_wait_rejects_resource_lock);
	UT_RUN(test_19_peer_ack_wait_rejects_buffer_lock);
	UT_RUN(test_20_peer_ack_wait_rejects_slru_lock);
	UT_RUN(test_21_peer_ack_wait_rejects_undo_io_ownership);
	UT_RUN(test_22_peer_ack_wait_rejects_ic_dispatch_ownership);
	UT_RUN(test_23_control_barrier_with_no_lock_is_allowed);
	UT_RUN(test_24_control_barrier_rejects_resource_lock);
	UT_RUN(test_25_control_barrier_rejects_buffer_lock);
	UT_RUN(test_26_control_barrier_rejects_slru_lock);
	UT_RUN(test_27_control_barrier_rejects_undo_io_ownership);
	UT_RUN(test_28_control_barrier_rejects_ic_dispatch_ownership);
	UT_RUN(test_29_process_utility_may_only_wait_on_lmon);
	UT_RUN(test_30_lmon_may_only_delegate_durable_io_to_qvotec);
	UT_RUN(test_31_lmon_control_path_never_enters_holder_lms);
	UT_RUN(test_32_qvotec_completion_never_enters_origin_data);
	UT_DONE();
	return ut_failed_count == 0 ? 0 : 1;
}
