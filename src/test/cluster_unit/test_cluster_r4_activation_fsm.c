/*-------------------------------------------------------------------------
 *
 * test_cluster_r4_activation_fsm.c
 *	  Exact closed-transition, ACK and dormant-admission tests for R4 D13.
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "cluster/cluster_conf.h"
#include "cluster/cluster_epoch.h"
#include "cluster/cluster_semantic_activation.h"
#include "cluster/cluster_sf_dep.h"
#include "port/atomics.h"
#include "storage/ipc.h"
#include "storage/shmem.h"

#define TEST_SEMANTIC_SHMEM_BYTES 1104
#define TEST_GATE_SEQ_OFFSET 552
#define TEST_GATE_ACTIVE_BITS_OFFSET 560
#define TEST_GATE_RECORD_GENERATION_OFFSET 568
#define TEST_GATE_FORMATION_EPOCH_OFFSET 576
#define TEST_GATE_CLOSED_OFFSET 584
#define TEST_GATE_INFLIGHT_OFFSET 588

typedef union TestSemanticShmemStorage {
	pg_atomic_uint64 align;
	uint8 bytes[TEST_SEMANTIC_SHMEM_BYTES];
} TestSemanticShmemStorage;

static TestSemanticShmemStorage test_semantic_shmem;
static bool test_shmem_found;
static Size test_shmem_requested_size;
static pg_on_exit_callback test_exit_callback;
static Datum test_exit_callback_arg;
static int test_exit_registration_count;
static uint64 test_current_epoch = 7;
static int test_read_barrier_count;
static int test_advance_epoch_on_read_barrier;
static bool test_peer_capability_matches;
static int test_peer_capability_match_calls;
static int32 test_peer_capability_match_peer;
static uint32 test_peer_capability_match_caps;
static uint32 test_peer_capability_match_generation;

int MyProcPid = 101;
volatile sig_atomic_t InterruptPending = false;
volatile uint32 InterruptHoldoffCount = 0;
volatile uint32 QueryCancelHoldoffCount = 0;
volatile uint32 CritSectionCount = 0;

void ProcessInterrupts(void);

void *
ShmemInitStruct(const char *name pg_attribute_unused(), Size size, bool *foundPtr)
{
	test_shmem_requested_size = size;
	*foundPtr = test_shmem_found;
	return test_semantic_shmem.bytes;
}

uint64
cluster_epoch_get_current(void)
{
	return test_current_epoch;
}

void
on_shmem_exit(pg_on_exit_callback function, Datum arg)
{
	test_exit_callback = function;
	test_exit_callback_arg = arg;
	test_exit_registration_count++;
}

void
ProcessInterrupts(void)
{}

bool
cluster_sf_peer_capability_generation_matches(int32 peer_id, uint32 required_capabilities,
									  uint32 expected_generation)
{
	test_peer_capability_match_calls++;
	test_peer_capability_match_peer = peer_id;
	test_peer_capability_match_caps = required_capabilities;
	test_peer_capability_match_generation = expected_generation;
	return test_peer_capability_matches;
}

static void
test_read_barrier(void)
{
	pg_read_barrier_impl();
	test_read_barrier_count++;
	if (test_advance_epoch_on_read_barrier == test_read_barrier_count)
		test_current_epoch++;
}

#undef pg_read_barrier
#define pg_read_barrier() test_read_barrier()

bool
errstart(int elevel pg_attribute_unused(), const char *domain pg_attribute_unused())
{
	return false;
}

bool
errstart_cold(int elevel pg_attribute_unused(), const char *domain pg_attribute_unused())
{
	return false;
}

void
errfinish(const char *filename pg_attribute_unused(), int lineno pg_attribute_unused(),
		  const char *funcname pg_attribute_unused())
{}

int
errcode(int sqlerrcode pg_attribute_unused())
{
	return 0;
}

int
errmsg(const char *fmt pg_attribute_unused(), ...)
{
	return 0;
}

int
errhint(const char *fmt pg_attribute_unused(), ...)
{
	return 0;
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

static pg_atomic_uint64 *
test_gate_u64(Size offset)
{
	return (pg_atomic_uint64 *)(test_semantic_shmem.bytes + offset);
}

static pg_atomic_uint32 *
test_gate_u32(Size offset)
{
	return (pg_atomic_uint32 *)(test_semantic_shmem.bytes + offset);
}

static pg_atomic_uint32 *
test_gate_inflight(ClusterSemanticAdmissionSide side, int feature_index)
{
	return test_gate_u32(TEST_GATE_INFLIGHT_OFFSET
						 + ((Size)side * 64 + (Size)feature_index) * sizeof(pg_atomic_uint32));
}

static void
test_gate_reset(void)
{
	memset(&test_semantic_shmem, 0, sizeof(test_semantic_shmem));
	test_shmem_found = false;
	test_shmem_requested_size = 0;
	test_exit_callback = NULL;
	test_exit_callback_arg = (Datum)0;
	test_exit_registration_count = 0;
	test_current_epoch = 7;
	test_read_barrier_count = 0;
	test_advance_epoch_on_read_barrier = 0;
	test_peer_capability_matches = false;
	test_peer_capability_match_calls = 0;
	test_peer_capability_match_peer = -1;
	test_peer_capability_match_caps = 0;
	test_peer_capability_match_generation = UINT32_MAX;
	MyProcPid = 101;
	SemanticActivationShmem = NULL;
	memset(semantic_activation_local_inflight, 0, sizeof(semantic_activation_local_inflight));
	semantic_activation_exit_hook_pid = 0;
	cluster_semantic_activation_shmem_init();
}

static void
test_gate_publish(uint64 seq, uint64 active_bits, uint64 generation, uint64 formation_epoch,
				  bool closed)
{
	pg_atomic_write_u64(test_gate_u64(TEST_GATE_SEQ_OFFSET), seq);
	pg_atomic_write_u64(test_gate_u64(TEST_GATE_ACTIVE_BITS_OFFSET), active_bits);
	pg_atomic_write_u64(test_gate_u64(TEST_GATE_RECORD_GENERATION_OFFSET), generation);
	pg_atomic_write_u64(test_gate_u64(TEST_GATE_FORMATION_EPOCH_OFFSET), formation_epoch);
	pg_atomic_write_u32(test_gate_u32(TEST_GATE_CLOSED_OFFSET), closed ? 1 : 0);
}

static uint64
test_token_formation_epoch(const ClusterSemanticAdmissionToken *token)
{
	uint64 formation_epoch;

	memcpy(&formation_epoch, ((const uint8 *)token) + 16, sizeof(formation_epoch));
	return formation_epoch;
}

static SemanticActivationAckTuple
valid_ack(void)
{
	return (SemanticActivationAckTuple){
		.node_id = 7,
		.boot_id = UINT64_C(0x101),
		.admitted_incarnation = UINT64_C(0x202),
		.control_connection_generation = UINT64_C(0x303),
		.capability_word = UINT32_C(0x3000),
		.capability_generation = UINT64_C(0x404),
		.transition_epoch = UINT64_C(0x505),
		.record_generation = UINT64_C(0x606),
	};
}

#define DEFINE_STATE_VALUE_TEST(test_name, state_value, expected_value)                            \
	UT_TEST(test_name)                                                                             \
	{                                                                                              \
		UT_ASSERT_EQ((state_value), (expected_value));                                             \
	}

#define DEFINE_FSM_EDGE_TEST(test_name, reverse_value, from_value, to_value)                       \
	UT_TEST(test_name)                                                                             \
	{                                                                                              \
		SemanticActivationState next = SEMANTIC_ACTIVATION_STATE_INVALID;                          \
		UT_ASSERT(semantic_activation_fsm_next((from_value), (reverse_value), &next));             \
		UT_ASSERT_EQ(next, (to_value));                                                            \
	}

#define DEFINE_CALLBACK_TEST(test_name, state_value, callback_value)                               \
	UT_TEST(test_name)                                                                             \
	{                                                                                              \
		UT_ASSERT_EQ(semantic_activation_callback_for_state((state_value)), (callback_value));     \
	}

#define DEFINE_INVALID_SELF_EDGE_TEST(test_name, state_value)                                      \
	UT_TEST(test_name)                                                                             \
	{                                                                                              \
		SemanticActivationState next = (state_value);                                              \
		UT_ASSERT(!semantic_activation_fsm_next((state_value), false, &next));                     \
		UT_ASSERT_EQ(next, (state_value));                                                         \
	}

#define DEFINE_FORWARD_NOT_SELF_EDGE_TEST(test_name, state_value)                                  \
	UT_TEST(test_name)                                                                             \
	{                                                                                              \
		SemanticActivationState next = (state_value);                                              \
		UT_ASSERT(semantic_activation_fsm_next((state_value), false, &next));                      \
		UT_ASSERT(next != (state_value));                                                          \
	}

#define DEFINE_FAILURE_TEST(test_name, state_value, expected_closed, expected_revert)              \
	UT_TEST(test_name)                                                                             \
	{                                                                                              \
		SemanticActivationFailurePolicy policy;                                                    \
		memset(&policy, 0, sizeof(policy));                                                        \
		UT_ASSERT(semantic_activation_failure_policy((state_value), &policy));                     \
		UT_ASSERT_EQ(policy.target, SEMANTIC_ACTIVATION_STATE_SOURCE_OPEN);                        \
		UT_ASSERT_EQ(policy.admission_closed_until_source_open, (expected_closed));                \
		UT_ASSERT_EQ(policy.revert_source_closed, (expected_revert));                              \
	}

UT_TEST(test_01_feature_bit_is_one)
{
	UT_ASSERT_EQ(CLUSTER_SEMANTIC_FEATURE_R4_SYNC_CR_V1, UINT64_C(1));
}

UT_TEST(test_02_required_hello_caps_are_frozen)
{
	UT_ASSERT_EQ(PGRAC_IC_HELLO_CAP_SEMANTIC_ACTIVATION_V1, UINT32_C(0x00001000));
	UT_ASSERT_EQ(PGRAC_IC_HELLO_CAP_R4_SYNC_CR_V1, UINT32_C(0x00002000));
}

UT_TEST(test_03_action_values_are_frozen)
{
	UT_ASSERT_EQ(CLUSTER_SEMANTIC_ENABLE_ALL, 0);
	UT_ASSERT_EQ(CLUSTER_SEMANTIC_DISABLE_ALL, 1);
	UT_ASSERT_EQ(CLUSTER_SEMANTIC_ROLLBACK_ALL, 2);
	UT_ASSERT_EQ(CLUSTER_SEMANTIC_ROLLBACK_ABORT, 3);
}

UT_TEST(test_04_admission_values_are_frozen)
{
	UT_ASSERT_EQ(CLUSTER_SEMANTIC_ADMISSION_OK, 0);
	UT_ASSERT_EQ(CLUSTER_SEMANTIC_ADMISSION_SOURCE_DORMANT, 1);
	UT_ASSERT_EQ(CLUSTER_SEMANTIC_ADMISSION_TARGET_DISABLED, 2);
	UT_ASSERT_EQ(CLUSTER_SEMANTIC_ADMISSION_GENERATION_CHANGED, 3);
	UT_ASSERT_EQ(CLUSTER_SEMANTIC_ADMISSION_CLOSED, 4);
}

UT_TEST(test_05_activation_result_values_are_frozen)
{
	UT_ASSERT_EQ(CLUSTER_SEMANTIC_ACTIVATION_OK, 0);
	UT_ASSERT_EQ(CLUSTER_SEMANTIC_ACTIVATION_RF_DEFERRED, 3);
	UT_ASSERT_EQ(CLUSTER_SEMANTIC_ACTIVATION_QUORUM_HOLD, 9);
	UT_ASSERT_EQ(CLUSTER_SEMANTIC_ACTIVATION_RECORD_CONFLICT, 10);
	UT_ASSERT_EQ(CLUSTER_SEMANTIC_ACTIVATION_BAD_STATE, 12);
}

UT_TEST(test_06_admission_side_values_are_frozen)
{
	UT_ASSERT_EQ(CLUSTER_SEMANTIC_SOURCE_SIDE, 0);
	UT_ASSERT_EQ(CLUSTER_SEMANTIC_TARGET_SIDE, 1);
}

UT_TEST(test_07_r4_descriptor_identity)
{
	const ClusterSemanticActivationDescriptor *descriptor
		= cluster_semantic_activation_r4_descriptor();

	UT_ASSERT_NOT_NULL(descriptor);
	UT_ASSERT_STR_EQ(descriptor->name, "R4_SYNC_CR_V1");
	UT_ASSERT_EQ(descriptor->feature_bit, CLUSTER_SEMANTIC_FEATURE_R4_SYNC_CR_V1);
}

UT_TEST(test_08_r4_descriptor_caps_and_active_bits)
{
	const ClusterSemanticActivationDescriptor *descriptor
		= cluster_semantic_activation_r4_descriptor();

	UT_ASSERT_EQ(descriptor->required_hello_caps, UINT32_C(0x00003000));
	UT_ASSERT_EQ(descriptor->required_active_bits, 0);
}

UT_TEST(test_09_r4_descriptor_retains_source)
{
	const ClusterSemanticActivationDescriptor *descriptor
		= cluster_semantic_activation_r4_descriptor();

	UT_ASSERT(descriptor->source_available);
}

UT_TEST(test_10_r4_descriptor_has_every_callback)
{
	const ClusterSemanticActivationDescriptor *descriptor
		= cluster_semantic_activation_r4_descriptor();

	UT_ASSERT_NOT_NULL(descriptor->pre_prepare_readiness);
	UT_ASSERT_NOT_NULL(descriptor->close_source_admission);
	UT_ASSERT_NOT_NULL(descriptor->source_logical_debt_zero);
	UT_ASSERT_NOT_NULL(descriptor->source_transport_zero);
	UT_ASSERT_NOT_NULL(descriptor->prepare_target);
	UT_ASSERT_NOT_NULL(descriptor->apply_target_closed);
	UT_ASSERT_NOT_NULL(descriptor->revert_source_closed);
	UT_ASSERT_NOT_NULL(descriptor->open_target_admission);
}

UT_TEST(test_11_source_only_is_exclusive)
{
	UT_ASSERT(semantic_activation_source_target_exclusive(true, false));
}

UT_TEST(test_12_target_only_is_exclusive)
{
	UT_ASSERT(semantic_activation_source_target_exclusive(false, true));
	UT_ASSERT(semantic_activation_source_target_exclusive(false, false));
	UT_ASSERT(!semantic_activation_source_target_exclusive(true, true));
}

DEFINE_FSM_EDGE_TEST(test_13_enable_source_open_to_admission_stopped, false,
					 SEMANTIC_ACTIVATION_STATE_SOURCE_OPEN,
					 SEMANTIC_ACTIVATION_STATE_SOURCE_ADMISSION_STOPPED)
DEFINE_FSM_EDGE_TEST(test_14_enable_admission_stopped_to_drain, false,
					 SEMANTIC_ACTIVATION_STATE_SOURCE_ADMISSION_STOPPED,
					 SEMANTIC_ACTIVATION_STATE_SOURCE_DRAIN_OR_RECOVERY)
DEFINE_FSM_EDGE_TEST(test_15_enable_drain_to_logical_zero, false,
					 SEMANTIC_ACTIVATION_STATE_SOURCE_DRAIN_OR_RECOVERY,
					 SEMANTIC_ACTIVATION_STATE_SOURCE_LOGICAL_ZERO)
DEFINE_FSM_EDGE_TEST(test_16_enable_logical_zero_to_transport_barrier, false,
					 SEMANTIC_ACTIVATION_STATE_SOURCE_LOGICAL_ZERO,
					 SEMANTIC_ACTIVATION_STATE_ORDERED_TRANSPORT_BARRIER)
DEFINE_FSM_EDGE_TEST(test_17_enable_transport_barrier_to_transport_zero, false,
					 SEMANTIC_ACTIVATION_STATE_ORDERED_TRANSPORT_BARRIER,
					 SEMANTIC_ACTIVATION_STATE_TRANSPORT_BACKED_ZERO)
DEFINE_FSM_EDGE_TEST(test_18_enable_transport_zero_to_epoch_barrier, false,
					 SEMANTIC_ACTIVATION_STATE_TRANSPORT_BACKED_ZERO,
					 SEMANTIC_ACTIVATION_STATE_EPOCH_CAPABILITY_BARRIER)
DEFINE_FSM_EDGE_TEST(test_19_enable_epoch_barrier_to_target_staged, false,
					 SEMANTIC_ACTIVATION_STATE_EPOCH_CAPABILITY_BARRIER,
					 SEMANTIC_ACTIVATION_STATE_TARGET_STAGED)
DEFINE_FSM_EDGE_TEST(test_20_enable_target_staged_to_committed_closed, false,
					 SEMANTIC_ACTIVATION_STATE_TARGET_STAGED,
					 SEMANTIC_ACTIVATION_STATE_TARGET_COMMITTED_CLOSED)
DEFINE_FSM_EDGE_TEST(test_21_enable_committed_closed_to_target_open, false,
					 SEMANTIC_ACTIVATION_STATE_TARGET_COMMITTED_CLOSED,
					 SEMANTIC_ACTIVATION_STATE_TARGET_OPEN)

UT_TEST(test_22_enable_target_open_is_terminal)
{
	SemanticActivationState next = SEMANTIC_ACTIVATION_STATE_INVALID;

	UT_ASSERT(!semantic_activation_fsm_next(SEMANTIC_ACTIVATION_STATE_TARGET_OPEN, false, &next));
}

DEFINE_FSM_EDGE_TEST(test_23_disable_source_open_to_admission_stopped, true,
					 SEMANTIC_ACTIVATION_STATE_SOURCE_OPEN,
					 SEMANTIC_ACTIVATION_STATE_SOURCE_ADMISSION_STOPPED)
DEFINE_FSM_EDGE_TEST(test_24_disable_admission_stopped_to_drain, true,
					 SEMANTIC_ACTIVATION_STATE_SOURCE_ADMISSION_STOPPED,
					 SEMANTIC_ACTIVATION_STATE_SOURCE_DRAIN_OR_RECOVERY)
DEFINE_FSM_EDGE_TEST(test_25_disable_drain_to_logical_zero, true,
					 SEMANTIC_ACTIVATION_STATE_SOURCE_DRAIN_OR_RECOVERY,
					 SEMANTIC_ACTIVATION_STATE_SOURCE_LOGICAL_ZERO)
DEFINE_FSM_EDGE_TEST(test_26_disable_logical_zero_to_transport_barrier, true,
					 SEMANTIC_ACTIVATION_STATE_SOURCE_LOGICAL_ZERO,
					 SEMANTIC_ACTIVATION_STATE_ORDERED_TRANSPORT_BARRIER)
DEFINE_FSM_EDGE_TEST(test_27_disable_transport_barrier_to_transport_zero, true,
					 SEMANTIC_ACTIVATION_STATE_ORDERED_TRANSPORT_BARRIER,
					 SEMANTIC_ACTIVATION_STATE_TRANSPORT_BACKED_ZERO)
DEFINE_FSM_EDGE_TEST(test_28_disable_transport_zero_to_epoch_barrier, true,
					 SEMANTIC_ACTIVATION_STATE_TRANSPORT_BACKED_ZERO,
					 SEMANTIC_ACTIVATION_STATE_EPOCH_CAPABILITY_BARRIER)
DEFINE_FSM_EDGE_TEST(test_29_disable_epoch_barrier_to_target_staged, true,
					 SEMANTIC_ACTIVATION_STATE_EPOCH_CAPABILITY_BARRIER,
					 SEMANTIC_ACTIVATION_STATE_TARGET_STAGED)
DEFINE_FSM_EDGE_TEST(test_30_disable_target_staged_to_committed_closed, true,
					 SEMANTIC_ACTIVATION_STATE_TARGET_STAGED,
					 SEMANTIC_ACTIVATION_STATE_TARGET_COMMITTED_CLOSED)
DEFINE_FSM_EDGE_TEST(test_31_disable_committed_closed_to_target_open, true,
					 SEMANTIC_ACTIVATION_STATE_TARGET_COMMITTED_CLOSED,
					 SEMANTIC_ACTIVATION_STATE_TARGET_OPEN)

UT_TEST(test_32_disable_target_open_is_terminal)
{
	SemanticActivationState next = SEMANTIC_ACTIVATION_STATE_INVALID;

	UT_ASSERT(!semantic_activation_fsm_next(SEMANTIC_ACTIVATION_STATE_TARGET_OPEN, true, &next));
}

DEFINE_CALLBACK_TEST(test_33_admission_stop_calls_close_source,
					 SEMANTIC_ACTIVATION_STATE_SOURCE_ADMISSION_STOPPED,
					 SEMANTIC_ACTIVATION_CALLBACK_CLOSE_SOURCE)
DEFINE_CALLBACK_TEST(test_34_drain_has_no_eraser_callback,
					 SEMANTIC_ACTIVATION_STATE_SOURCE_DRAIN_OR_RECOVERY,
					 SEMANTIC_ACTIVATION_CALLBACK_NONE)
DEFINE_CALLBACK_TEST(test_35_logical_zero_calls_logical_proof,
					 SEMANTIC_ACTIVATION_STATE_SOURCE_LOGICAL_ZERO,
					 SEMANTIC_ACTIVATION_CALLBACK_LOGICAL_ZERO)
DEFINE_CALLBACK_TEST(test_36_ordered_barrier_calls_transport_barrier,
					 SEMANTIC_ACTIVATION_STATE_ORDERED_TRANSPORT_BARRIER,
					 SEMANTIC_ACTIVATION_CALLBACK_TRANSPORT_BARRIER)
DEFINE_CALLBACK_TEST(test_37_transport_zero_calls_transport_proof,
					 SEMANTIC_ACTIVATION_STATE_TRANSPORT_BACKED_ZERO,
					 SEMANTIC_ACTIVATION_CALLBACK_TRANSPORT_ZERO)
DEFINE_CALLBACK_TEST(test_38_epoch_state_calls_exact_ack_barrier,
					 SEMANTIC_ACTIVATION_STATE_EPOCH_CAPABILITY_BARRIER,
					 SEMANTIC_ACTIVATION_CALLBACK_EPOCH_CAPABILITY_BARRIER)
DEFINE_CALLBACK_TEST(test_39_target_staged_calls_prepare, SEMANTIC_ACTIVATION_STATE_TARGET_STAGED,
					 SEMANTIC_ACTIVATION_CALLBACK_PREPARE_TARGET)
DEFINE_CALLBACK_TEST(test_40_committed_closed_calls_apply,
					 SEMANTIC_ACTIVATION_STATE_TARGET_COMMITTED_CLOSED,
					 SEMANTIC_ACTIVATION_CALLBACK_APPLY_TARGET_CLOSED)
DEFINE_CALLBACK_TEST(test_41_target_open_calls_open_admission,
					 SEMANTIC_ACTIVATION_STATE_TARGET_OPEN,
					 SEMANTIC_ACTIVATION_CALLBACK_OPEN_TARGET)
DEFINE_CALLBACK_TEST(test_42_source_open_has_no_transition_callback,
					 SEMANTIC_ACTIVATION_STATE_SOURCE_OPEN, SEMANTIC_ACTIVATION_CALLBACK_NONE)

DEFINE_FORWARD_NOT_SELF_EDGE_TEST(test_43_source_open_has_no_self_edge,
								  SEMANTIC_ACTIVATION_STATE_SOURCE_OPEN)
DEFINE_FORWARD_NOT_SELF_EDGE_TEST(test_44_admission_stopped_has_no_self_edge,
								  SEMANTIC_ACTIVATION_STATE_SOURCE_ADMISSION_STOPPED)
DEFINE_FORWARD_NOT_SELF_EDGE_TEST(test_45_drain_has_no_self_edge,
								  SEMANTIC_ACTIVATION_STATE_SOURCE_DRAIN_OR_RECOVERY)
DEFINE_FORWARD_NOT_SELF_EDGE_TEST(test_46_logical_zero_has_no_self_edge,
								  SEMANTIC_ACTIVATION_STATE_SOURCE_LOGICAL_ZERO)
DEFINE_FORWARD_NOT_SELF_EDGE_TEST(test_47_transport_barrier_has_no_self_edge,
								  SEMANTIC_ACTIVATION_STATE_ORDERED_TRANSPORT_BARRIER)
DEFINE_FORWARD_NOT_SELF_EDGE_TEST(test_48_transport_zero_has_no_self_edge,
								  SEMANTIC_ACTIVATION_STATE_TRANSPORT_BACKED_ZERO)
DEFINE_FORWARD_NOT_SELF_EDGE_TEST(test_49_epoch_barrier_has_no_self_edge,
								  SEMANTIC_ACTIVATION_STATE_EPOCH_CAPABILITY_BARRIER)
DEFINE_FORWARD_NOT_SELF_EDGE_TEST(test_50_target_staged_has_no_self_edge,
								  SEMANTIC_ACTIVATION_STATE_TARGET_STAGED)
DEFINE_FORWARD_NOT_SELF_EDGE_TEST(test_51_committed_closed_has_no_self_edge,
								  SEMANTIC_ACTIVATION_STATE_TARGET_COMMITTED_CLOSED)
DEFINE_INVALID_SELF_EDGE_TEST(test_52_target_open_has_no_self_edge,
							  SEMANTIC_ACTIVATION_STATE_TARGET_OPEN)

UT_TEST(test_53_invalid_low_state_has_no_edge)
{
	SemanticActivationState next = SEMANTIC_ACTIVATION_STATE_SOURCE_OPEN;

	UT_ASSERT(!semantic_activation_fsm_next(SEMANTIC_ACTIVATION_STATE_INVALID, false, &next));
}

UT_TEST(test_54_invalid_high_state_has_no_edge)
{
	SemanticActivationState next = SEMANTIC_ACTIVATION_STATE_SOURCE_OPEN;

	UT_ASSERT(!semantic_activation_fsm_next((SemanticActivationState)10, false, &next));
}

DEFINE_FAILURE_TEST(test_55_failure_at_source_open_restores_source,
					SEMANTIC_ACTIVATION_STATE_SOURCE_OPEN, false, false)
DEFINE_FAILURE_TEST(test_56_failure_after_admission_stop_restores_source,
					SEMANTIC_ACTIVATION_STATE_SOURCE_ADMISSION_STOPPED, true, false)
DEFINE_FAILURE_TEST(test_57_failure_during_drain_restores_source,
					SEMANTIC_ACTIVATION_STATE_SOURCE_DRAIN_OR_RECOVERY, true, false)
DEFINE_FAILURE_TEST(test_58_failure_after_logical_zero_restores_source,
					SEMANTIC_ACTIVATION_STATE_SOURCE_LOGICAL_ZERO, true, false)
DEFINE_FAILURE_TEST(test_59_failure_during_ordered_barrier_restores_source,
					SEMANTIC_ACTIVATION_STATE_ORDERED_TRANSPORT_BARRIER, true, false)
DEFINE_FAILURE_TEST(test_60_failure_after_transport_zero_restores_source,
					SEMANTIC_ACTIVATION_STATE_TRANSPORT_BACKED_ZERO, true, false)
DEFINE_FAILURE_TEST(test_61_failure_at_epoch_barrier_restores_source,
					SEMANTIC_ACTIVATION_STATE_EPOCH_CAPABILITY_BARRIER, true, false)
DEFINE_FAILURE_TEST(test_62_failure_after_prepare_restores_source,
					SEMANTIC_ACTIVATION_STATE_TARGET_STAGED, true, false)
DEFINE_FAILURE_TEST(test_63_failure_after_commit_requires_revert_closed,
					SEMANTIC_ACTIVATION_STATE_TARGET_COMMITTED_CLOSED, true, true)

UT_TEST(test_64_target_open_is_not_reinterpreted_as_transition_failure)
{
	SemanticActivationFailurePolicy policy;

	memset(&policy, 0, sizeof(policy));
	UT_ASSERT(!semantic_activation_failure_policy(SEMANTIC_ACTIVATION_STATE_TARGET_OPEN, &policy));
}

UT_TEST(test_65_identical_ack_tuple_matches)
{
	SemanticActivationAckTuple a = valid_ack();
	SemanticActivationAckTuple b = a;

	UT_ASSERT(semantic_activation_ack_matches(&a, &b));
}

#define DEFINE_ACK_INVALIDATION_TEST(test_name, field_name)                                        \
	UT_TEST(test_name)                                                                             \
	{                                                                                              \
		SemanticActivationAckTuple expected = valid_ack();                                         \
		SemanticActivationAckTuple observed = expected;                                            \
		observed.field_name++;                                                                     \
		UT_ASSERT(!semantic_activation_ack_matches(&observed, &expected));                         \
	}

DEFINE_ACK_INVALIDATION_TEST(test_66_node_change_invalidates_ack, node_id)
DEFINE_ACK_INVALIDATION_TEST(test_67_boot_change_invalidates_ack, boot_id)
DEFINE_ACK_INVALIDATION_TEST(test_68_incarnation_change_invalidates_ack, admitted_incarnation)
DEFINE_ACK_INVALIDATION_TEST(test_69_control_reconnect_invalidates_ack,
							 control_connection_generation)
DEFINE_ACK_INVALIDATION_TEST(test_70_capability_word_change_invalidates_ack, capability_word)
DEFINE_ACK_INVALIDATION_TEST(test_71_capability_generation_change_invalidates_ack,
							 capability_generation)
DEFINE_ACK_INVALIDATION_TEST(test_72_epoch_change_invalidates_ack, transition_epoch)
DEFINE_ACK_INVALIDATION_TEST(test_73_record_generation_change_invalidates_ack, record_generation)

UT_TEST(test_74_null_observed_ack_never_matches)
{
	SemanticActivationAckTuple expected = valid_ack();

	UT_ASSERT(!semantic_activation_ack_matches(NULL, &expected));
}

UT_TEST(test_75_null_expected_ack_never_matches)
{
	SemanticActivationAckTuple observed = valid_ack();

	UT_ASSERT(!semantic_activation_ack_matches(&observed, NULL));
}

UT_TEST(test_76_inactive_feature_source_is_admitted)
{
	UT_ASSERT_EQ(
		semantic_activation_admission_policy(1, 0, false, CLUSTER_SEMANTIC_SOURCE_SIDE, 4, 4),
		CLUSTER_SEMANTIC_ADMISSION_OK);
}

UT_TEST(test_77_inactive_feature_target_is_disabled)
{
	UT_ASSERT_EQ(
		semantic_activation_admission_policy(1, 0, false, CLUSTER_SEMANTIC_TARGET_SIDE, 4, 4),
		CLUSTER_SEMANTIC_ADMISSION_TARGET_DISABLED);
}

UT_TEST(test_78_active_feature_source_is_dormant)
{
	UT_ASSERT_EQ(
		semantic_activation_admission_policy(1, 1, false, CLUSTER_SEMANTIC_SOURCE_SIDE, 4, 4),
		CLUSTER_SEMANTIC_ADMISSION_SOURCE_DORMANT);
}

UT_TEST(test_79_active_feature_target_is_admitted)
{
	UT_ASSERT_EQ(
		semantic_activation_admission_policy(1, 1, false, CLUSTER_SEMANTIC_TARGET_SIDE, 4, 4),
		CLUSTER_SEMANTIC_ADMISSION_OK);
}

UT_TEST(test_80_transition_closes_source_admission)
{
	UT_ASSERT_EQ(
		semantic_activation_admission_policy(1, 0, true, CLUSTER_SEMANTIC_SOURCE_SIDE, 4, 4),
		CLUSTER_SEMANTIC_ADMISSION_CLOSED);
}

UT_TEST(test_81_transition_closes_target_admission)
{
	UT_ASSERT_EQ(
		semantic_activation_admission_policy(1, 0, true, CLUSTER_SEMANTIC_TARGET_SIDE, 4, 4),
		CLUSTER_SEMANTIC_ADMISSION_CLOSED);
}

UT_TEST(test_82_source_generation_change_is_typed)
{
	UT_ASSERT_EQ(
		semantic_activation_admission_policy(1, 0, false, CLUSTER_SEMANTIC_SOURCE_SIDE, 4, 5),
		CLUSTER_SEMANTIC_ADMISSION_GENERATION_CHANGED);
}

UT_TEST(test_83_target_generation_change_is_typed)
{
	UT_ASSERT_EQ(
		semantic_activation_admission_policy(1, 1, false, CLUSTER_SEMANTIC_TARGET_SIDE, 4, 5),
		CLUSTER_SEMANTIC_ADMISSION_GENERATION_CHANGED);
}

UT_TEST(test_84_unknown_side_is_closed)
{
	UT_ASSERT_EQ(
		semantic_activation_admission_policy(1, 0, false, (ClusterSemanticAdmissionSide)2, 4, 4),
		CLUSTER_SEMANTIC_ADMISSION_CLOSED);
}

UT_TEST(test_85_zero_feature_is_closed)
{
	UT_ASSERT_EQ(
		semantic_activation_admission_policy(0, 0, false, CLUSTER_SEMANTIC_SOURCE_SIDE, 4, 4),
		CLUSTER_SEMANTIC_ADMISSION_CLOSED);
}

UT_TEST(test_86_r4a_snapshot_is_fixed_false)
{
	ClusterR4PrerequisiteSnapshot snapshot = cluster_undo_block0_r4_prerequisite_snapshot();

	UT_ASSERT(!snapshot.ready);
	UT_ASSERT_EQ(snapshot.status, CLUSTER_R4_PREREQUISITE_RF_DEFERRED);
	UT_ASSERT_EQ(snapshot.reserved[0] | snapshot.reserved[1] | snapshot.reserved[2], 0);
}

UT_TEST(test_87_readiness_adapter_returns_rf_deferred)
{
	ClusterSemanticActivationRefusal refusal;

	memset(&refusal, 0, sizeof(refusal));
	UT_ASSERT_EQ(r4_pre_prepare_readiness(19, &refusal), CLUSTER_SEMANTIC_ACTIVATION_RF_DEFERRED);
}

UT_TEST(test_88_readiness_adapter_names_r4_feature)
{
	ClusterSemanticActivationRefusal refusal;

	memset(&refusal, 0, sizeof(refusal));
	(void)r4_pre_prepare_readiness(19, &refusal);
	UT_ASSERT_EQ(refusal.feature_bit, CLUSTER_SEMANTIC_FEATURE_R4_SYNC_CR_V1);
}

UT_TEST(test_89_readiness_adapter_preserves_expected_generation)
{
	ClusterSemanticActivationRefusal refusal;

	memset(&refusal, 0, sizeof(refusal));
	(void)r4_pre_prepare_readiness(19, &refusal);
	UT_ASSERT_EQ(refusal.expected_generation, 19);
}

UT_TEST(test_90_descriptor_uses_the_only_r4a_adapter)
{
	const ClusterSemanticActivationDescriptor *descriptor
		= cluster_semantic_activation_r4_descriptor();

	UT_ASSERT(descriptor->pre_prepare_readiness == r4_pre_prepare_readiness);
}

UT_TEST(test_91_preflight_refusal_is_before_every_mutation)
{
	ClusterSemanticActivationRefusal refusal;
	uint32 effects = UINT32_MAX;

	UT_ASSERT_EQ(semantic_activation_preflight(CLUSTER_SEMANTIC_ENABLE_ALL, 0, &refusal, &effects),
				 CLUSTER_SEMANTIC_ACTIVATION_RF_DEFERRED);
	UT_ASSERT_EQ(effects, SEMANTIC_ACTIVATION_EFFECT_NONE);
}

UT_TEST(test_92_preflight_refusal_names_condition_feature)
{
	ClusterSemanticActivationRefusal refusal;
	uint32 effects = UINT32_MAX;

	(void)semantic_activation_preflight(CLUSTER_SEMANTIC_ENABLE_ALL, 23, &refusal, &effects);
	UT_ASSERT_EQ(refusal.result, CLUSTER_SEMANTIC_ACTIVATION_RF_DEFERRED);
	UT_ASSERT_EQ(refusal.feature_bit, CLUSTER_SEMANTIC_FEATURE_R4_SYNC_CR_V1);
	UT_ASSERT_EQ(refusal.expected_generation, 23);
}

UT_TEST(test_93_preflight_rejects_bad_action_without_effects)
{
	ClusterSemanticActivationRefusal refusal;
	uint32 effects = UINT32_MAX;

	UT_ASSERT_EQ(
		semantic_activation_preflight((ClusterSemanticActivationAction)4, 0, &refusal, &effects),
		CLUSTER_SEMANTIC_ACTIVATION_BAD_STATE);
	UT_ASSERT_EQ(effects, SEMANTIC_ACTIVATION_EFFECT_NONE);
}

UT_TEST(test_94_public_submit_refuses_before_prepare)
{
	ClusterSemanticActivationRefusal refusal;

	memset(&refusal, 0, sizeof(refusal));
	UT_ASSERT_EQ(cluster_semantic_activation_submit(CLUSTER_SEMANTIC_ENABLE_ALL, &refusal),
				 CLUSTER_SEMANTIC_ACTIVATION_RF_DEFERRED);
	UT_ASSERT_EQ(refusal.result, CLUSTER_SEMANTIC_ACTIVATION_RF_DEFERRED);
}

UT_TEST(test_95_dormant_target_enter_has_no_token)
{
	ClusterSemanticAdmissionToken token;

	test_gate_reset();
	test_gate_publish(2, 0, 0, test_current_epoch, false);
	memset(&token, 0xa5, sizeof(token));
	UT_ASSERT_EQ(cluster_semantic_activation_enter(CLUSTER_SEMANTIC_FEATURE_R4_SYNC_CR_V1,
												   CLUSTER_SEMANTIC_TARGET_SIDE, &token),
				 CLUSTER_SEMANTIC_ADMISSION_TARGET_DISABLED);
	UT_ASSERT(!token.entered);
}

UT_TEST(test_96_source_token_recheck_and_leave_are_generation_scoped)
{
	ClusterSemanticAdmissionToken token;

	test_gate_reset();
	test_gate_publish(2, 0, 0, test_current_epoch, false);
	memset(&token, 0, sizeof(token));
	UT_ASSERT_EQ(cluster_semantic_activation_enter(CLUSTER_SEMANTIC_FEATURE_R4_SYNC_CR_V1,
												   CLUSTER_SEMANTIC_SOURCE_SIDE, &token),
				 CLUSTER_SEMANTIC_ADMISSION_OK);
	UT_ASSERT(token.entered);
	UT_ASSERT(cluster_semantic_activation_recheck(&token));
	cluster_semantic_activation_leave(&token);
	UT_ASSERT(!token.entered);
}

UT_TEST(test_97_old_epoch_completion_is_inert_and_requires_revalidation)
{
	ClusterSemanticActivationShmem shmem;
	ClusterSemanticActivationCasRequest request;
	ClusterSemanticActivationRecord desired_record;
	ClusterSemanticActivationResult result = CLUSTER_SEMANTIC_ACTIVATION_BAD_STATE;
	SemanticActivationAckTuple expected = valid_ack();
	SemanticActivationAckTuple observed = expected;
	uint8 desired[CLUSTER_SEMANTIC_ACTIVATION_RECORD_BYTES];
	uint64 before_active_bits;
	uint64 before_generation;
	bool before_closed;
	uint64 seq = 0;

	memset(&shmem, 0, sizeof(shmem));
	pg_atomic_init_u64(&shmem.record_cas_request_seq, 0);
	pg_atomic_init_u64(&shmem.record_cas_completion_seq, 0);
	pg_atomic_init_u32(&shmem.record_cas_result, CLUSTER_SEMANTIC_ACTIVATION_BAD_STATE);
	pg_atomic_init_u64(&shmem.admission_seq, 0);
	pg_atomic_init_u64(&shmem.active_bits, CLUSTER_SEMANTIC_FEATURE_R4_SYNC_CR_V1);
	pg_atomic_init_u64(&shmem.record_generation, 7);
	pg_atomic_init_u64(&shmem.formation_epoch, test_current_epoch);
	pg_atomic_init_u32(&shmem.transition_closed, 1);
	SemanticActivationShmem = &shmem;
	before_active_bits = pg_atomic_read_u64(&shmem.active_bits);
	before_generation = pg_atomic_read_u64(&shmem.record_generation);
	before_closed = pg_atomic_read_u32(&shmem.transition_closed) != 0;

	memset(&desired_record, 0, sizeof(desired_record));
	desired_record.source_feature_bitmap = UINT64_C(0x11);
	desired_record.target_feature_bitmap = UINT64_C(0x22);
	desired_record.transition_epoch = expected.transition_epoch - 1;
	desired_record.record_generation = 8;
	desired_record.coordinator_node = expected.node_id;
	desired_record.coordinator_incarnation = expected.admitted_incarnation;
	desired_record.phase = CLUSTER_SEMANTIC_PHASE_COMMIT;
	UT_ASSERT(cluster_semantic_activation_record_encode(&desired_record, desired));
	UT_ASSERT(semantic_activation_record_cas_mailbox_submit(7, UINT64_C(0x11), desired, &seq));
	UT_ASSERT(cluster_semantic_activation_qvotec_poll_record_cas(&request));
	UT_ASSERT(cluster_semantic_activation_qvotec_complete_record_cas(
		request.request_seq, CLUSTER_SEMANTIC_ACTIVATION_OK));
	UT_ASSERT(semantic_activation_record_cas_mailbox_poll_completion(seq, &result));
	UT_ASSERT_EQ(result, CLUSTER_SEMANTIC_ACTIVATION_OK);

	cluster_semantic_activation_lmon_tick();
	UT_ASSERT_EQ(pg_atomic_read_u64(&shmem.active_bits), before_active_bits);
	UT_ASSERT_EQ(pg_atomic_read_u64(&shmem.record_generation), before_generation);
	UT_ASSERT_EQ(pg_atomic_read_u32(&shmem.transition_closed) != 0, before_closed);
	observed.transition_epoch = desired_record.transition_epoch;
	UT_ASSERT(!semantic_activation_ack_matches(&observed, &expected));
	SemanticActivationShmem = NULL;
}

UT_TEST(test_98_admission_token_has_frozen_natural_layout)
{
	ClusterSemanticAdmissionToken token;

	memset(&token, 0, sizeof(token));
	UT_ASSERT_EQ(sizeof(token), 32);
	UT_ASSERT_EQ((Size)((char *)&token.feature_bit - (char *)&token), 0);
	UT_ASSERT_EQ((Size)((char *)&token.record_generation - (char *)&token), 8);
	UT_ASSERT_EQ((Size)((char *)&token.side - (char *)&token), 24);
	UT_ASSERT_EQ((Size)((char *)&token.entered - (char *)&token), 25);
}

UT_TEST(test_99_shared_gate_layout_and_bootstrap_are_fail_closed)
{
	test_gate_reset();
	UT_ASSERT_EQ(test_shmem_requested_size, TEST_SEMANTIC_SHMEM_BYTES);
	UT_ASSERT_EQ(pg_atomic_read_u64(test_gate_u64(TEST_GATE_SEQ_OFFSET)), 0);
	UT_ASSERT_EQ(pg_atomic_read_u64(test_gate_u64(TEST_GATE_ACTIVE_BITS_OFFSET)), 0);
	UT_ASSERT_EQ(pg_atomic_read_u64(test_gate_u64(TEST_GATE_RECORD_GENERATION_OFFSET)), 0);
	UT_ASSERT_EQ(pg_atomic_read_u64(test_gate_u64(TEST_GATE_FORMATION_EPOCH_OFFSET)), 0);
	UT_ASSERT_EQ(pg_atomic_read_u32(test_gate_u32(TEST_GATE_CLOSED_OFFSET)), 1);
}

UT_TEST(test_100_source_enter_owns_shared_debt_and_epoch_token)
{
	ClusterSemanticAdmissionToken token;

	test_gate_reset();
	test_gate_publish(2, 0, 11, test_current_epoch, false);
	UT_ASSERT_EQ(cluster_semantic_activation_enter(CLUSTER_SEMANTIC_FEATURE_R4_SYNC_CR_V1,
												   CLUSTER_SEMANTIC_SOURCE_SIDE, &token),
				 CLUSTER_SEMANTIC_ADMISSION_OK);
	UT_ASSERT(token.entered);
	UT_ASSERT_EQ(test_token_formation_epoch(&token), test_current_epoch);
	UT_ASSERT_EQ(pg_atomic_read_u32(test_gate_inflight(CLUSTER_SEMANTIC_SOURCE_SIDE, 0)), 1);
	UT_ASSERT_EQ(test_exit_registration_count, 1);
}

UT_TEST(test_101_active_source_refuses_before_debt)
{
	ClusterSemanticAdmissionToken token;

	test_gate_reset();
	test_gate_publish(2, CLUSTER_SEMANTIC_FEATURE_R4_SYNC_CR_V1, 12, test_current_epoch, false);
	UT_ASSERT_EQ(cluster_semantic_activation_enter(CLUSTER_SEMANTIC_FEATURE_R4_SYNC_CR_V1,
												   CLUSTER_SEMANTIC_SOURCE_SIDE, &token),
				 CLUSTER_SEMANTIC_ADMISSION_SOURCE_DORMANT);
	UT_ASSERT(!token.entered);
	UT_ASSERT_EQ(pg_atomic_read_u32(test_gate_inflight(CLUSTER_SEMANTIC_SOURCE_SIDE, 0)), 0);
}

UT_TEST(test_102_inactive_target_refuses_before_debt)
{
	ClusterSemanticAdmissionToken token;

	test_gate_reset();
	test_gate_publish(2, 0, 13, test_current_epoch, false);
	UT_ASSERT_EQ(cluster_semantic_activation_enter(CLUSTER_SEMANTIC_FEATURE_R4_SYNC_CR_V1,
												   CLUSTER_SEMANTIC_TARGET_SIDE, &token),
				 CLUSTER_SEMANTIC_ADMISSION_TARGET_DISABLED);
	UT_ASSERT(!token.entered);
	UT_ASSERT_EQ(pg_atomic_read_u32(test_gate_inflight(CLUSTER_SEMANTIC_TARGET_SIDE, 0)), 0);
}

UT_TEST(test_103_epoch_drift_invalidates_recheck_without_losing_debt)
{
	ClusterSemanticAdmissionToken token;

	test_gate_reset();
	test_gate_publish(2, 0, 14, test_current_epoch, false);
	UT_ASSERT_EQ(cluster_semantic_activation_enter(CLUSTER_SEMANTIC_FEATURE_R4_SYNC_CR_V1,
												   CLUSTER_SEMANTIC_SOURCE_SIDE, &token),
				 CLUSTER_SEMANTIC_ADMISSION_OK);
	test_current_epoch++;
	UT_ASSERT(!cluster_semantic_activation_recheck(&token));
	UT_ASSERT_EQ(pg_atomic_read_u32(test_gate_inflight(CLUSTER_SEMANTIC_SOURCE_SIDE, 0)), 1);
	cluster_semantic_activation_leave(&token);
	UT_ASSERT_EQ(pg_atomic_read_u32(test_gate_inflight(CLUSTER_SEMANTIC_SOURCE_SIDE, 0)), 0);
}

UT_TEST(test_104_close_invalidates_recheck_and_leave_balances_once)
{
	ClusterSemanticAdmissionToken token;

	test_gate_reset();
	test_gate_publish(2, 0, 15, test_current_epoch, false);
	UT_ASSERT_EQ(cluster_semantic_activation_enter(CLUSTER_SEMANTIC_FEATURE_R4_SYNC_CR_V1,
												   CLUSTER_SEMANTIC_SOURCE_SIDE, &token),
				 CLUSTER_SEMANTIC_ADMISSION_OK);
	test_gate_publish(4, 0, 15, test_current_epoch, true);
	UT_ASSERT(!cluster_semantic_activation_recheck(&token));
	cluster_semantic_activation_leave(&token);
	cluster_semantic_activation_leave(&token);
	UT_ASSERT_EQ(pg_atomic_read_u32(test_gate_inflight(CLUSTER_SEMANTIC_SOURCE_SIDE, 0)), 0);
}

UT_TEST(test_105_pid_change_discards_inherited_local_ledger_only)
{
	ClusterSemanticAdmissionToken parent_token;
	ClusterSemanticAdmissionToken child_token;

	test_gate_reset();
	test_gate_publish(2, 0, 16, test_current_epoch, false);
	UT_ASSERT_EQ(cluster_semantic_activation_enter(CLUSTER_SEMANTIC_FEATURE_R4_SYNC_CR_V1,
												   CLUSTER_SEMANTIC_SOURCE_SIDE, &parent_token),
				 CLUSTER_SEMANTIC_ADMISSION_OK);
	MyProcPid = 202;
	UT_ASSERT_EQ(cluster_semantic_activation_enter(CLUSTER_SEMANTIC_FEATURE_R4_SYNC_CR_V1,
												   CLUSTER_SEMANTIC_SOURCE_SIDE, &child_token),
				 CLUSTER_SEMANTIC_ADMISSION_OK);
	UT_ASSERT_EQ(test_exit_registration_count, 2);
	UT_ASSERT_EQ(pg_atomic_read_u32(test_gate_inflight(CLUSTER_SEMANTIC_SOURCE_SIDE, 0)), 2);
	cluster_semantic_activation_leave(&child_token);
	UT_ASSERT_EQ(pg_atomic_read_u32(test_gate_inflight(CLUSTER_SEMANTIC_SOURCE_SIDE, 0)), 1);
}

UT_TEST(test_106_exit_hook_drains_both_side_ledgers)
{
	ClusterSemanticAdmissionToken source_token;
	ClusterSemanticAdmissionToken target_token;

	test_gate_reset();
	test_gate_publish(2, 0, 17, test_current_epoch, false);
	UT_ASSERT_EQ(cluster_semantic_activation_enter(CLUSTER_SEMANTIC_FEATURE_R4_SYNC_CR_V1,
												   CLUSTER_SEMANTIC_SOURCE_SIDE, &source_token),
				 CLUSTER_SEMANTIC_ADMISSION_OK);
	test_gate_publish(4, CLUSTER_SEMANTIC_FEATURE_R4_SYNC_CR_V1, 18, test_current_epoch, false);
	UT_ASSERT_EQ(cluster_semantic_activation_enter(CLUSTER_SEMANTIC_FEATURE_R4_SYNC_CR_V1,
												   CLUSTER_SEMANTIC_TARGET_SIDE, &target_token),
				 CLUSTER_SEMANTIC_ADMISSION_OK);
	UT_ASSERT_NOT_NULL(test_exit_callback);
	if (test_exit_callback != NULL)
		test_exit_callback(0, test_exit_callback_arg);
	UT_ASSERT_EQ(pg_atomic_read_u32(test_gate_inflight(CLUSTER_SEMANTIC_SOURCE_SIDE, 0)), 0);
	UT_ASSERT_EQ(pg_atomic_read_u32(test_gate_inflight(CLUSTER_SEMANTIC_TARGET_SIDE, 0)), 0);
}

UT_TEST(test_107_odd_snapshot_is_bounded_closed_without_debt)
{
	ClusterSemanticAdmissionToken token;

	test_gate_reset();
	test_gate_publish(3, 0, 19, test_current_epoch, false);
	UT_ASSERT_EQ(cluster_semantic_activation_enter(CLUSTER_SEMANTIC_FEATURE_R4_SYNC_CR_V1,
												   CLUSTER_SEMANTIC_SOURCE_SIDE, &token),
				 CLUSTER_SEMANTIC_ADMISSION_CLOSED);
	UT_ASSERT_EQ(pg_atomic_read_u32(test_gate_inflight(CLUSTER_SEMANTIC_SOURCE_SIDE, 0)), 0);
}

UT_TEST(test_108_nonregistered_feature_is_closed_without_debt)
{
	ClusterSemanticAdmissionToken token;

	test_gate_reset();
	test_gate_publish(2, 0, 20, test_current_epoch, false);
	UT_ASSERT_EQ(
		cluster_semantic_activation_enter(UINT64_C(1) << 7, CLUSTER_SEMANTIC_SOURCE_SIDE, &token),
		CLUSTER_SEMANTIC_ADMISSION_CLOSED);
	UT_ASSERT_EQ(pg_atomic_read_u32(test_gate_inflight(CLUSTER_SEMANTIC_SOURCE_SIDE, 7)), 0);
}

UT_TEST(test_109_lmon_without_validated_majority_remains_closed)
{
	ClusterSemanticAdmissionToken token;

	test_gate_reset();
	cluster_semantic_activation_lmon_tick();
	UT_ASSERT_EQ(pg_atomic_read_u64(test_gate_u64(TEST_GATE_SEQ_OFFSET)), 2);
	UT_ASSERT_EQ(pg_atomic_read_u64(test_gate_u64(TEST_GATE_FORMATION_EPOCH_OFFSET)),
				 test_current_epoch);
	UT_ASSERT_EQ(pg_atomic_read_u32(test_gate_u32(TEST_GATE_CLOSED_OFFSET)), 1);
	UT_ASSERT_EQ(cluster_semantic_activation_enter(CLUSTER_SEMANTIC_FEATURE_R4_SYNC_CR_V1,
											   CLUSTER_SEMANTIC_SOURCE_SIDE, &token),
				 CLUSTER_SEMANTIC_ADMISSION_CLOSED);
	UT_ASSERT(!token.entered);
}

UT_TEST(test_110_lmon_odd_writer_remains_fail_closed)
{
	test_gate_reset();
	test_gate_publish(3, 0, 0, test_current_epoch, true);
	cluster_semantic_activation_lmon_tick();
	UT_ASSERT_EQ(pg_atomic_read_u64(test_gate_u64(TEST_GATE_SEQ_OFFSET)), 3);
	UT_ASSERT_EQ(pg_atomic_read_u32(test_gate_u32(TEST_GATE_CLOSED_OFFSET)), 1);
}

UT_TEST(test_111_formation_change_closes_before_debt_drain)
{
	ClusterSemanticAdmissionToken old_token;
	ClusterSemanticAdmissionToken new_token;

	test_gate_reset();
	test_gate_publish(2, 0, 0, test_current_epoch, false);
	UT_ASSERT_EQ(cluster_semantic_activation_enter(CLUSTER_SEMANTIC_FEATURE_R4_SYNC_CR_V1,
											   CLUSTER_SEMANTIC_SOURCE_SIDE, &old_token),
				 CLUSTER_SEMANTIC_ADMISSION_OK);
	test_current_epoch++;
	cluster_semantic_activation_lmon_tick();
	UT_ASSERT_EQ(pg_atomic_read_u64(test_gate_u64(TEST_GATE_FORMATION_EPOCH_OFFSET)),
				 test_current_epoch);
	UT_ASSERT_EQ(pg_atomic_read_u32(test_gate_u32(TEST_GATE_CLOSED_OFFSET)), 1);
	UT_ASSERT_EQ(pg_atomic_read_u32(test_gate_inflight(CLUSTER_SEMANTIC_SOURCE_SIDE, 0)), 1);
	UT_ASSERT_EQ(cluster_semantic_activation_enter(CLUSTER_SEMANTIC_FEATURE_R4_SYNC_CR_V1,
											   CLUSTER_SEMANTIC_SOURCE_SIDE, &new_token),
				 CLUSTER_SEMANTIC_ADMISSION_CLOSED);
	if (new_token.entered)
		cluster_semantic_activation_leave(&new_token);
	cluster_semantic_activation_leave(&old_token);
}

UT_TEST(test_112_enter_samples_second_snapshot_before_epoch)
{
	ClusterSemanticAdmissionToken token;
	ClusterSemanticAdmissionResult result;

	test_gate_reset();
	test_gate_publish(2, 0, 21, test_current_epoch, false);
	test_advance_epoch_on_read_barrier = 4;
	result = cluster_semantic_activation_enter(CLUSTER_SEMANTIC_FEATURE_R4_SYNC_CR_V1,
											   CLUSTER_SEMANTIC_SOURCE_SIDE, &token);
	UT_ASSERT_EQ(result, CLUSTER_SEMANTIC_ADMISSION_GENERATION_CHANGED);
	UT_ASSERT(!token.entered);
	UT_ASSERT_EQ(pg_atomic_read_u32(test_gate_inflight(CLUSTER_SEMANTIC_SOURCE_SIDE, 0)), 0);
	if (token.entered)
		cluster_semantic_activation_leave(&token);
}

UT_TEST(test_113_recheck_samples_snapshot_before_epoch)
{
	ClusterSemanticAdmissionToken token;

	test_gate_reset();
	test_gate_publish(2, 0, 22, test_current_epoch, false);
	UT_ASSERT_EQ(cluster_semantic_activation_enter(CLUSTER_SEMANTIC_FEATURE_R4_SYNC_CR_V1,
											   CLUSTER_SEMANTIC_SOURCE_SIDE, &token),
				 CLUSTER_SEMANTIC_ADMISSION_OK);
	test_read_barrier_count = 0;
	test_advance_epoch_on_read_barrier = 2;
	UT_ASSERT(!cluster_semantic_activation_recheck(&token));
	UT_ASSERT_EQ(pg_atomic_read_u32(test_gate_inflight(CLUSTER_SEMANTIC_SOURCE_SIDE, 0)), 1);
	cluster_semantic_activation_leave(&token);
}

UT_TEST(test_114_peer_open_matcher_stays_closed_until_d13_ack_table)
{
	ClusterSemanticAdmissionToken token;

	test_gate_reset();
	test_gate_publish(2, CLUSTER_SEMANTIC_FEATURE_R4_SYNC_CR_V1, 23, test_current_epoch, false);
	UT_ASSERT_EQ(cluster_semantic_activation_enter(CLUSTER_SEMANTIC_FEATURE_R4_SYNC_CR_V1,
											   CLUSTER_SEMANTIC_TARGET_SIDE, &token),
				 CLUSTER_SEMANTIC_ADMISSION_OK);
	test_peer_capability_matches = true;
	UT_ASSERT(!cluster_semantic_activation_peer_open_matches(
		&token, 7, PGRAC_IC_HELLO_CAP_SEMANTIC_ACTIVATION_V1
					   | PGRAC_IC_HELLO_CAP_R4_SYNC_CR_V1,
		0));
	UT_ASSERT_EQ(test_peer_capability_match_calls, 1);
	UT_ASSERT_EQ(test_peer_capability_match_peer, 7);
	UT_ASSERT_EQ(test_peer_capability_match_caps,
				 PGRAC_IC_HELLO_CAP_SEMANTIC_ACTIVATION_V1
					 | PGRAC_IC_HELLO_CAP_R4_SYNC_CR_V1);
	UT_ASSERT_EQ(test_peer_capability_match_generation, 0);
	cluster_semantic_activation_leave(&token);
}

UT_TEST(test_115_peer_open_matcher_rejects_invalid_inputs_before_capability_match)
{
	ClusterSemanticAdmissionToken target_token;
	ClusterSemanticAdmissionToken source_token;
	ClusterSemanticAdmissionToken wrong_feature_token;
	uint32 required_caps = PGRAC_IC_HELLO_CAP_SEMANTIC_ACTIVATION_V1
						  | PGRAC_IC_HELLO_CAP_R4_SYNC_CR_V1;

	test_gate_reset();
	test_gate_publish(2, 0, 24, test_current_epoch, false);
	UT_ASSERT_EQ(cluster_semantic_activation_enter(CLUSTER_SEMANTIC_FEATURE_R4_SYNC_CR_V1,
											   CLUSTER_SEMANTIC_SOURCE_SIDE, &source_token),
				 CLUSTER_SEMANTIC_ADMISSION_OK);
	UT_ASSERT(!cluster_semantic_activation_peer_open_matches(&source_token, 7, required_caps, 0));
	UT_ASSERT_EQ(test_peer_capability_match_calls, 0);
	cluster_semantic_activation_leave(&source_token);

	test_gate_reset();
	test_gate_publish(2, CLUSTER_SEMANTIC_FEATURE_R4_SYNC_CR_V1, 24, test_current_epoch, false);
	UT_ASSERT_EQ(cluster_semantic_activation_enter(CLUSTER_SEMANTIC_FEATURE_R4_SYNC_CR_V1,
											   CLUSTER_SEMANTIC_TARGET_SIDE, &target_token),
				 CLUSTER_SEMANTIC_ADMISSION_OK);
	wrong_feature_token = target_token;
	wrong_feature_token.feature_bit = 0;
	test_peer_capability_matches = true;
	UT_ASSERT(!cluster_semantic_activation_peer_open_matches(NULL, 7, required_caps, 0));
	UT_ASSERT(!cluster_semantic_activation_peer_open_matches(&(ClusterSemanticAdmissionToken){0}, 7,
															  required_caps, 0));
	UT_ASSERT(!cluster_semantic_activation_peer_open_matches(&wrong_feature_token, 7,
															  required_caps, 0));
	UT_ASSERT(!cluster_semantic_activation_peer_open_matches(&target_token, -1, required_caps, 0));
	UT_ASSERT(!cluster_semantic_activation_peer_open_matches(&target_token, CLUSTER_MAX_NODES,
															  required_caps, 0));
	UT_ASSERT(!cluster_semantic_activation_peer_open_matches(&target_token, 7, 0, 0));
	UT_ASSERT_EQ(test_peer_capability_match_calls, 0);
	test_current_epoch++;
	UT_ASSERT(!cluster_semantic_activation_peer_open_matches(&target_token, 7, required_caps, 0));
	UT_ASSERT_EQ(test_peer_capability_match_calls, 0);
	test_current_epoch--;
	test_peer_capability_matches = false;
	UT_ASSERT(!cluster_semantic_activation_peer_open_matches(&target_token, 7, required_caps, 0));
	UT_ASSERT_EQ(test_peer_capability_match_calls, 1);
	cluster_semantic_activation_leave(&target_token);
}

int
main(void)
{
	UT_PLAN(115);
	UT_RUN(test_01_feature_bit_is_one);
	UT_RUN(test_02_required_hello_caps_are_frozen);
	UT_RUN(test_03_action_values_are_frozen);
	UT_RUN(test_04_admission_values_are_frozen);
	UT_RUN(test_05_activation_result_values_are_frozen);
	UT_RUN(test_06_admission_side_values_are_frozen);
	UT_RUN(test_07_r4_descriptor_identity);
	UT_RUN(test_08_r4_descriptor_caps_and_active_bits);
	UT_RUN(test_09_r4_descriptor_retains_source);
	UT_RUN(test_10_r4_descriptor_has_every_callback);
	UT_RUN(test_11_source_only_is_exclusive);
	UT_RUN(test_12_target_only_is_exclusive);
	UT_RUN(test_13_enable_source_open_to_admission_stopped);
	UT_RUN(test_14_enable_admission_stopped_to_drain);
	UT_RUN(test_15_enable_drain_to_logical_zero);
	UT_RUN(test_16_enable_logical_zero_to_transport_barrier);
	UT_RUN(test_17_enable_transport_barrier_to_transport_zero);
	UT_RUN(test_18_enable_transport_zero_to_epoch_barrier);
	UT_RUN(test_19_enable_epoch_barrier_to_target_staged);
	UT_RUN(test_20_enable_target_staged_to_committed_closed);
	UT_RUN(test_21_enable_committed_closed_to_target_open);
	UT_RUN(test_22_enable_target_open_is_terminal);
	UT_RUN(test_23_disable_source_open_to_admission_stopped);
	UT_RUN(test_24_disable_admission_stopped_to_drain);
	UT_RUN(test_25_disable_drain_to_logical_zero);
	UT_RUN(test_26_disable_logical_zero_to_transport_barrier);
	UT_RUN(test_27_disable_transport_barrier_to_transport_zero);
	UT_RUN(test_28_disable_transport_zero_to_epoch_barrier);
	UT_RUN(test_29_disable_epoch_barrier_to_target_staged);
	UT_RUN(test_30_disable_target_staged_to_committed_closed);
	UT_RUN(test_31_disable_committed_closed_to_target_open);
	UT_RUN(test_32_disable_target_open_is_terminal);
	UT_RUN(test_33_admission_stop_calls_close_source);
	UT_RUN(test_34_drain_has_no_eraser_callback);
	UT_RUN(test_35_logical_zero_calls_logical_proof);
	UT_RUN(test_36_ordered_barrier_calls_transport_barrier);
	UT_RUN(test_37_transport_zero_calls_transport_proof);
	UT_RUN(test_38_epoch_state_calls_exact_ack_barrier);
	UT_RUN(test_39_target_staged_calls_prepare);
	UT_RUN(test_40_committed_closed_calls_apply);
	UT_RUN(test_41_target_open_calls_open_admission);
	UT_RUN(test_42_source_open_has_no_transition_callback);
	UT_RUN(test_43_source_open_has_no_self_edge);
	UT_RUN(test_44_admission_stopped_has_no_self_edge);
	UT_RUN(test_45_drain_has_no_self_edge);
	UT_RUN(test_46_logical_zero_has_no_self_edge);
	UT_RUN(test_47_transport_barrier_has_no_self_edge);
	UT_RUN(test_48_transport_zero_has_no_self_edge);
	UT_RUN(test_49_epoch_barrier_has_no_self_edge);
	UT_RUN(test_50_target_staged_has_no_self_edge);
	UT_RUN(test_51_committed_closed_has_no_self_edge);
	UT_RUN(test_52_target_open_has_no_self_edge);
	UT_RUN(test_53_invalid_low_state_has_no_edge);
	UT_RUN(test_54_invalid_high_state_has_no_edge);
	UT_RUN(test_55_failure_at_source_open_restores_source);
	UT_RUN(test_56_failure_after_admission_stop_restores_source);
	UT_RUN(test_57_failure_during_drain_restores_source);
	UT_RUN(test_58_failure_after_logical_zero_restores_source);
	UT_RUN(test_59_failure_during_ordered_barrier_restores_source);
	UT_RUN(test_60_failure_after_transport_zero_restores_source);
	UT_RUN(test_61_failure_at_epoch_barrier_restores_source);
	UT_RUN(test_62_failure_after_prepare_restores_source);
	UT_RUN(test_63_failure_after_commit_requires_revert_closed);
	UT_RUN(test_64_target_open_is_not_reinterpreted_as_transition_failure);
	UT_RUN(test_65_identical_ack_tuple_matches);
	UT_RUN(test_66_node_change_invalidates_ack);
	UT_RUN(test_67_boot_change_invalidates_ack);
	UT_RUN(test_68_incarnation_change_invalidates_ack);
	UT_RUN(test_69_control_reconnect_invalidates_ack);
	UT_RUN(test_70_capability_word_change_invalidates_ack);
	UT_RUN(test_71_capability_generation_change_invalidates_ack);
	UT_RUN(test_72_epoch_change_invalidates_ack);
	UT_RUN(test_73_record_generation_change_invalidates_ack);
	UT_RUN(test_74_null_observed_ack_never_matches);
	UT_RUN(test_75_null_expected_ack_never_matches);
	UT_RUN(test_76_inactive_feature_source_is_admitted);
	UT_RUN(test_77_inactive_feature_target_is_disabled);
	UT_RUN(test_78_active_feature_source_is_dormant);
	UT_RUN(test_79_active_feature_target_is_admitted);
	UT_RUN(test_80_transition_closes_source_admission);
	UT_RUN(test_81_transition_closes_target_admission);
	UT_RUN(test_82_source_generation_change_is_typed);
	UT_RUN(test_83_target_generation_change_is_typed);
	UT_RUN(test_84_unknown_side_is_closed);
	UT_RUN(test_85_zero_feature_is_closed);
	UT_RUN(test_86_r4a_snapshot_is_fixed_false);
	UT_RUN(test_87_readiness_adapter_returns_rf_deferred);
	UT_RUN(test_88_readiness_adapter_names_r4_feature);
	UT_RUN(test_89_readiness_adapter_preserves_expected_generation);
	UT_RUN(test_90_descriptor_uses_the_only_r4a_adapter);
	UT_RUN(test_91_preflight_refusal_is_before_every_mutation);
	UT_RUN(test_92_preflight_refusal_names_condition_feature);
	UT_RUN(test_93_preflight_rejects_bad_action_without_effects);
	UT_RUN(test_94_public_submit_refuses_before_prepare);
	UT_RUN(test_95_dormant_target_enter_has_no_token);
	UT_RUN(test_96_source_token_recheck_and_leave_are_generation_scoped);
	UT_RUN(test_97_old_epoch_completion_is_inert_and_requires_revalidation);
	UT_RUN(test_98_admission_token_has_frozen_natural_layout);
	UT_RUN(test_99_shared_gate_layout_and_bootstrap_are_fail_closed);
	UT_RUN(test_100_source_enter_owns_shared_debt_and_epoch_token);
	UT_RUN(test_101_active_source_refuses_before_debt);
	UT_RUN(test_102_inactive_target_refuses_before_debt);
	UT_RUN(test_103_epoch_drift_invalidates_recheck_without_losing_debt);
	UT_RUN(test_104_close_invalidates_recheck_and_leave_balances_once);
	UT_RUN(test_105_pid_change_discards_inherited_local_ledger_only);
	UT_RUN(test_106_exit_hook_drains_both_side_ledgers);
	UT_RUN(test_107_odd_snapshot_is_bounded_closed_without_debt);
	UT_RUN(test_108_nonregistered_feature_is_closed_without_debt);
	UT_RUN(test_109_lmon_without_validated_majority_remains_closed);
	UT_RUN(test_110_lmon_odd_writer_remains_fail_closed);
	UT_RUN(test_111_formation_change_closes_before_debt_drain);
	UT_RUN(test_112_enter_samples_second_snapshot_before_epoch);
	UT_RUN(test_113_recheck_samples_snapshot_before_epoch);
	UT_RUN(test_114_peer_open_matcher_stays_closed_until_d13_ack_table);
	UT_RUN(test_115_peer_open_matcher_rejects_invalid_inputs_before_capability_match);
	UT_DONE();
	return ut_failed_count == 0 ? 0 : 1;
}
