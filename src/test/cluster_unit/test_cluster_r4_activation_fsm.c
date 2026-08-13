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
#include "cluster/cluster_epoch_ballot.h"
#include "cluster/cluster_membership.h"
#include "cluster/cluster_qvotec.h"
#include "cluster/cluster_reconfig.h"
#include "cluster/cluster_replacement_wire.h"
#include "cluster/cluster_semantic_activation.h"
#include "cluster/cluster_sf_dep.h"
#include "cluster/storage/cluster_undo_block0_current.h"
#include "port/atomics.h"
#include "storage/ipc.h"
#include "storage/shmem.h"

#define TEST_SEMANTIC_GATE_SHMEM_BYTES 1104
#define TEST_SEMANTIC_UTILITY_MAILBOX_BYTES 80
#define TEST_GATE_SEQ_OFFSET 552
#define TEST_GATE_ACTIVE_BITS_OFFSET 560
#define TEST_GATE_RECORD_GENERATION_OFFSET 568
#define TEST_GATE_FORMATION_EPOCH_OFFSET 576
#define TEST_GATE_CLOSED_OFFSET 584
#define TEST_GATE_INFLIGHT_OFFSET 588

typedef union TestSemanticShmemStorage {
	pg_atomic_uint64 align;
	uint8 bytes[TEST_SEMANTIC_GATE_SHMEM_BYTES];
} TestSemanticShmemStorage;

typedef union TestSemanticUtilityMailboxStorage {
	pg_atomic_uint64 align;
	uint8 bytes[TEST_SEMANTIC_UTILITY_MAILBOX_BYTES];
} TestSemanticUtilityMailboxStorage;

static TestSemanticShmemStorage test_semantic_shmem;
static TestSemanticUtilityMailboxStorage test_semantic_utility_mailbox;
static bool test_shmem_found;
static bool test_utility_mailbox_found;
static Size test_shmem_requested_size;
static Size test_utility_mailbox_requested_size;
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
static int test_phase3_observe_calls;
static ClusterReplacementPhase3HandoffItem test_phase3_observed_item;
static int test_admitted_snapshot_calls;
static bool test_admitted_snapshot_valid;
static ClusterReplacementEpisode test_admitted_snapshot_episode;
static ClusterReplacementCommitMarkerV3 test_admitted_snapshot_marker;
static int test_membership_snapshot_calls;
static bool test_membership_snapshot_valid;
static uint64 test_membership_snapshot_lo;
static uint64 test_membership_snapshot_hi;
static uint64 test_membership_snapshot_epoch;
static int test_r4a_snapshot_calls;
static ClusterR4PrerequisiteSnapshot test_r4a_snapshot = {
	.target_node_id = -1,
};
static int test_wait_sleep_calls;
static int test_complete_after_wait_sleeps;
static uint64 test_wait_completion_request_seq;
static bool test_wait_completion_succeeded;

int MyProcPid = 101;
int cluster_node_id = 1;
volatile sig_atomic_t InterruptPending = false;
volatile uint32 InterruptHoldoffCount = 0;
volatile uint32 QueryCancelHoldoffCount = 0;
volatile uint32 CritSectionCount = 0;

static bool semantic_activation_utility_mailbox_complete(
	uint64 request_seq, ClusterSemanticActivationResult result,
	uint64 feature_bit, uint64 expected_generation);

void *
ShmemInitStruct(const char *name, Size size, bool *foundPtr)
{
	if (strcmp(name, "pgrac cluster semantic activation utility mailbox") == 0) {
		test_utility_mailbox_requested_size = size;
		*foundPtr = test_utility_mailbox_found;
		return test_semantic_utility_mailbox.bytes;
	}
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

static void
test_pg_usleep(long microsec pg_attribute_unused())
{
	test_wait_sleep_calls++;
	if (test_complete_after_wait_sleeps > 0
		&& test_wait_sleep_calls == test_complete_after_wait_sleeps)
		test_wait_completion_succeeded
			= semantic_activation_utility_mailbox_complete(
				test_wait_completion_request_seq,
				CLUSTER_SEMANTIC_ACTIVATION_OK, 0, 0);
}

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

int
cluster_membership_member_count(void)
{
	return 4;
}

bool
cluster_membership_is_member(int32 node_id)
{
	return node_id >= 0 && node_id < 4;
}

bool
cluster_reconfig_lmon_observe_replacement_ready(
	const ClusterReplacementPhase3HandoffItem *item)
{
	test_phase3_observe_calls++;
	if (item != NULL)
		test_phase3_observed_item = *item;
	return item != NULL;
}

bool
cluster_reconfig_lmon_snapshot_replacement_admitted(
	ClusterReplacementEpisode *out_episode,
	ClusterReplacementCommitMarkerV3 *out_marker)
{
	test_admitted_snapshot_calls++;
	if (!test_admitted_snapshot_valid || out_episode == NULL || out_marker == NULL)
		return false;
	*out_episode = test_admitted_snapshot_episode;
	*out_marker = test_admitted_snapshot_marker;
	return true;
}

bool
cluster_reconfig_lmon_snapshot_admitted_membership(
	uint64 *out_members_lo, uint64 *out_members_hi,
	uint64 *out_formation_epoch)
{
	test_membership_snapshot_calls++;
	if (!test_membership_snapshot_valid || out_members_lo == NULL
		|| out_members_hi == NULL || out_formation_epoch == NULL)
		return false;
	*out_members_lo = test_membership_snapshot_lo;
	*out_members_hi = test_membership_snapshot_hi;
	*out_formation_epoch = test_membership_snapshot_epoch;
	return true;
}

static ClusterR4PrerequisiteSnapshot
test_r4a_prerequisite_snapshot(void)
{
	test_r4a_snapshot_calls++;
	return test_r4a_snapshot;
}

/* Satisfy the independently linked block0 facade; this fixture overrides the
 * activation module's prerequisite call with test_r4a_prerequisite_snapshot. */
ClusterR4PrerequisiteSnapshot
cluster_reconfig_r4_prerequisite_snapshot(void)
{
	return test_r4a_snapshot;
}

bool
cluster_reconfig_r4_publish_ready(
	const ClusterR4PrerequisiteSnapshot *expected pg_attribute_unused())
{
	return false;
}

bool
cluster_undo_block0_current_startup_fenced_owned(void)
{
	return false;
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
#define cluster_undo_block0_r4_prerequisite_snapshot test_r4a_prerequisite_snapshot
#define pg_usleep test_pg_usleep
#include "../../backend/cluster/cluster_semantic_activation.c"
#undef pg_usleep

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
	memset(&test_semantic_utility_mailbox, 0,
		   sizeof(test_semantic_utility_mailbox));
	test_shmem_found = false;
	test_utility_mailbox_found = false;
	test_shmem_requested_size = 0;
	test_utility_mailbox_requested_size = 0;
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
	test_phase3_observe_calls = 0;
	memset(&test_phase3_observed_item, 0, sizeof(test_phase3_observed_item));
	test_admitted_snapshot_calls = 0;
	test_admitted_snapshot_valid = false;
	memset(&test_admitted_snapshot_episode, 0,
		   sizeof(test_admitted_snapshot_episode));
	memset(&test_admitted_snapshot_marker, 0,
		   sizeof(test_admitted_snapshot_marker));
	test_membership_snapshot_calls = 0;
	test_membership_snapshot_valid = true;
	test_membership_snapshot_lo = UINT64_C(0x0f);
	test_membership_snapshot_hi = 0;
	test_membership_snapshot_epoch = test_current_epoch;
	test_r4a_snapshot_calls = 0;
	memset(&test_r4a_snapshot, 0, sizeof(test_r4a_snapshot));
	test_r4a_snapshot.target_node_id = -1;
	test_wait_sleep_calls = 0;
	test_complete_after_wait_sleeps = 0;
	test_wait_completion_request_seq = 0;
	test_wait_completion_succeeded = false;
	MyProcPid = 101;
	SemanticActivationShmem = NULL;
	SemanticActivationUtilityMailbox = NULL;
	memset(semantic_activation_local_inflight, 0, sizeof(semantic_activation_local_inflight));
	semantic_activation_exit_hook_pid = 0;
	semantic_activation_lmon_record_read_seq = 0;
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
		.capability_word = UINT32_C(0x00303000),
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
	UT_ASSERT_EQ(PGRAC_IC_HELLO_CAP_CANDIDATE2_CORRECTED_A1_V1, UINT32_C(0x00100000));
	UT_ASSERT_EQ(PGRAC_IC_HELLO_CAP_UNDO_ROOT_DESCRIPTOR_V1, UINT32_C(0x00200000));
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

	UT_ASSERT_EQ(descriptor->required_hello_caps, UINT32_C(0x00303000));
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

UT_TEST(test_75a_d13_full_ack_table_requires_exact_member_set_and_tuples)
{
	SemanticActivationAckTuple expected[CLUSTER_MAX_NODES];
	SemanticActivationAckTuple observed[CLUSTER_MAX_NODES];
	uint64 members_lo = (UINT64_C(1) << 1) | (UINT64_C(1) << 3);

	memset(expected, 0, sizeof(expected));
	memset(observed, 0, sizeof(observed));
	expected[1] = valid_ack();
	expected[1].node_id = 1;
	expected[3] = valid_ack();
	expected[3].node_id = 3;
	expected[3].boot_id++;
	observed[1] = expected[1];
	observed[3] = expected[3];

	UT_ASSERT(semantic_activation_full_ack_table_matches(
		observed, members_lo, 0, expected, members_lo, 0));
	observed[3].capability_generation++;
	UT_ASSERT(!semantic_activation_full_ack_table_matches(
		observed, members_lo, 0, expected, members_lo, 0));
	observed[3] = expected[3];
	UT_ASSERT(!semantic_activation_full_ack_table_matches(
		observed, members_lo & ~(UINT64_C(1) << 3), 0,
		expected, members_lo, 0));
	UT_ASSERT(!semantic_activation_full_ack_table_matches(
		observed, members_lo | (UINT64_C(1) << 4), 0,
		expected, members_lo, 0));
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

UT_TEST(test_85a_modifier_gate_requires_durable_source_or_target_open)
{
	UT_ASSERT_EQ(
		semantic_activation_admission_policy(1, 0, true, CLUSTER_SEMANTIC_SOURCE_SIDE, 0, 0),
		CLUSTER_SEMANTIC_ADMISSION_CLOSED);
	UT_ASSERT(!semantic_activation_modifier_policy(0, 0, true));
	UT_ASSERT(semantic_activation_modifier_policy(0, 8, false));
	UT_ASSERT(semantic_activation_modifier_policy(
		CLUSTER_SEMANTIC_FEATURE_R4_SYNC_CR_V1, 9, false));
}

UT_TEST(test_85b_modifier_gate_closes_replacement_until_uniform_open)
{
	UT_ASSERT(!semantic_activation_modifier_policy(0, 9, true));
	UT_ASSERT(!semantic_activation_modifier_policy(
		CLUSTER_SEMANTIC_FEATURE_R4_SYNC_CR_V1, 9, true));
	UT_ASSERT(!semantic_activation_modifier_policy(UINT64_C(1) << 63, 9, false));
}

UT_TEST(test_86_r4a_snapshot_is_fixed_false)
{
	ClusterR4PrerequisiteSnapshot snapshot = cluster_undo_block0_r4_prerequisite_snapshot();

	UT_ASSERT(!snapshot.ready);
	UT_ASSERT_EQ(snapshot.status, CLUSTER_R4_PREREQUISITE_RF_DEFERRED);
	UT_ASSERT_EQ(snapshot.reserved0[0] | snapshot.reserved0[1] | snapshot.reserved0[2], 0);
	UT_ASSERT_EQ(snapshot.target_node_id, -1);
	UT_ASSERT_EQ((unsigned long long)snapshot.jcmk_generation, 0ULL);
	UT_ASSERT_EQ((unsigned long long)snapshot.grammar_fingerprint, 0ULL);
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

static ClusterR4PrerequisiteSnapshot
valid_r4a_ready_snapshot(void)
{
	ClusterR4PrerequisiteSnapshot snapshot;

	memset(&snapshot, 0, sizeof(snapshot));
	snapshot.status = CLUSTER_R4_PREREQUISITE_R4A_READY;
	snapshot.ready = true;
	snapshot.target_node_id = 3;
	snapshot.episode_state_generation = UINT32_C(17);
	snapshot.jcmk_generation = UINT64_C(41);
	snapshot.request_nonce = UINT64_C(0x123456789abcdef0);
	snapshot.old_admitted_incarnation = UINT64_C(9001);
	snapshot.fresh_incarnation = UINT64_C(9002);
	snapshot.committed_epoch = UINT64_C(71);
	snapshot.grammar_fingerprint
		= CANDIDATE2_CORRECTED_A1_GRAMMAR_FINGERPRINT;
	return snapshot;
}

UT_TEST(test_89d_local_ready_alone_cannot_enter_prepare)
{
	ClusterR4PrerequisiteSnapshot snapshot = valid_r4a_ready_snapshot();

	test_gate_reset();
	test_r4a_snapshot = snapshot;
	UT_ASSERT_EQ(r4_pre_prepare_readiness(19, NULL),
				 CLUSTER_SEMANTIC_ACTIVATION_RF_DEFERRED);
}

UT_TEST(test_89e_malformed_local_ready_remains_typed_deferred)
{
	ClusterSemanticActivationRefusal refusal;

	test_gate_reset();
	test_r4a_snapshot = valid_r4a_ready_snapshot();
	test_r4a_snapshot.reserved0[1] = 1;
	memset(&refusal, 0, sizeof(refusal));
	UT_ASSERT_EQ(r4_pre_prepare_readiness(19, &refusal),
				 CLUSTER_SEMANTIC_ACTIVATION_RF_DEFERRED);
	UT_ASSERT_EQ(refusal.result, CLUSTER_SEMANTIC_ACTIVATION_RF_DEFERRED);
}

static ClusterReplacementCommitMarkerV3
valid_d13_admitted_marker(void)
{
	ClusterReplacementCommitMarkerV3 marker;

	memset(&marker, 0, sizeof(marker));
	marker.magic = CLUSTER_JCMK_MAGIC;
	marker.version = CLUSTER_JCMK_REPLACEMENT_VERSION;
	marker.target_node_id = 3;
	marker.phase = CLUSTER_JCMK_REPLACEMENT_PHASE_ADMITTED;
	marker.generation = 42;
	marker.old_admitted_incarnation = UINT64_C(9001);
	marker.fresh_incarnation = UINT64_C(9002);
	marker.baseline_epoch = UINT64_C(70);
	marker.reserved_or_committed_epoch = UINT64_C(71);
	marker.request_nonce = UINT64_C(0x123456789abcdef0);
	marker.expected_purge_survivors[0] = UINT8_C(0x05);
	marker.grammar_fingerprint = UINT64_C(0x8e0dae5b428905e4);
	marker.ready_state_generation = UINT32_C(17);
	return marker;
}

static ClusterReplacementEpisode
valid_d13_admitted_episode(void)
{
	ClusterReplacementEpisode episode;

	memset(&episode, 0, sizeof(episode));
	episode.request_nonce = UINT64_C(0x123456789abcdef0);
	episode.baseline_epoch = UINT64_C(70);
	episode.reserved_or_committed_epoch = UINT64_C(71);
	episode.old_admitted_incarnation = UINT64_C(9001);
	episode.fresh_incarnation = UINT64_C(9002);
	episode.grammar_fingerprint = UINT64_C(0x8e0dae5b428905e4);
	episode.expected_survivors[0] = UINT8_C(0x05);
	episode.target_node_id = 3;
	episode.coordinator_node_id = 0;
	episode.state_generation = UINT32_C(17);
	episode.phase = CLUSTER_REPLACEMENT_EPISODE_ADMITTED;
	episode.readiness_flags = CLUSTER_REPLACEMENT_EPISODE_READINESS_MASK;
	return episode;
}

UT_TEST(test_89a_d13_prepare_basis_accepts_only_exact_admitted_ready_lineage)
{
	ClusterReplacementCommitMarkerV3 marker = valid_d13_admitted_marker();
	ClusterReplacementEpisode episode = valid_d13_admitted_episode();

	UT_ASSERT(semantic_activation_r4_prepare_basis_matches(&marker, &episode));
	UT_ASSERT(!semantic_activation_r4_prepare_basis_matches(NULL, &episode));
	UT_ASSERT(!semantic_activation_r4_prepare_basis_matches(&marker, NULL));
}

UT_TEST(test_89b_d13_prepare_basis_requires_admitted_ready_polarity)
{
	ClusterReplacementCommitMarkerV3 marker = valid_d13_admitted_marker();
	ClusterReplacementEpisode episode = valid_d13_admitted_episode();

	marker.phase = CLUSTER_JCMK_REPLACEMENT_PHASE_COMMITTED_CLOSED;
	UT_ASSERT(!semantic_activation_r4_prepare_basis_matches(&marker, &episode));
	marker = valid_d13_admitted_marker();
	marker.ready_state_generation = 0;
	UT_ASSERT(!semantic_activation_r4_prepare_basis_matches(&marker, &episode));
	marker = valid_d13_admitted_marker();
	episode.phase = CLUSTER_REPLACEMENT_EPISODE_POST_EPOCH;
	UT_ASSERT(!semantic_activation_r4_prepare_basis_matches(&marker, &episode));
	episode = valid_d13_admitted_episode();
	episode.readiness_flags &= (uint8)~CLUSTER_REPLACEMENT_EPISODE_R4A_TARGET_READY;
	UT_ASSERT(!semantic_activation_r4_prepare_basis_matches(&marker, &episode));
	episode = valid_d13_admitted_episode();
	episode.reserved[1] = 1;
	UT_ASSERT(!semantic_activation_r4_prepare_basis_matches(&marker, &episode));
}

UT_TEST(test_89c_d13_prepare_basis_rejects_generation_or_identity_drift)
{
	ClusterReplacementCommitMarkerV3 marker = valid_d13_admitted_marker();
	ClusterReplacementEpisode episode = valid_d13_admitted_episode();

	episode.state_generation++;
	UT_ASSERT(!semantic_activation_r4_prepare_basis_matches(&marker, &episode));
	episode = valid_d13_admitted_episode();
	episode.target_node_id++;
	UT_ASSERT(!semantic_activation_r4_prepare_basis_matches(&marker, &episode));
	episode = valid_d13_admitted_episode();
	episode.request_nonce++;
	UT_ASSERT(!semantic_activation_r4_prepare_basis_matches(&marker, &episode));
	episode = valid_d13_admitted_episode();
	episode.old_admitted_incarnation++;
	UT_ASSERT(!semantic_activation_r4_prepare_basis_matches(&marker, &episode));
	episode = valid_d13_admitted_episode();
	episode.fresh_incarnation++;
	UT_ASSERT(!semantic_activation_r4_prepare_basis_matches(&marker, &episode));
	episode = valid_d13_admitted_episode();
	episode.baseline_epoch++;
	UT_ASSERT(!semantic_activation_r4_prepare_basis_matches(&marker, &episode));
	episode = valid_d13_admitted_episode();
	episode.reserved_or_committed_epoch++;
	UT_ASSERT(!semantic_activation_r4_prepare_basis_matches(&marker, &episode));
	episode = valid_d13_admitted_episode();
	episode.expected_survivors[1] = UINT8_C(1);
	UT_ASSERT(!semantic_activation_r4_prepare_basis_matches(&marker, &episode));
	episode = valid_d13_admitted_episode();
	episode.grammar_fingerprint++;
	UT_ASSERT(!semantic_activation_r4_prepare_basis_matches(&marker, &episode));
}

UT_TEST(test_89f_positive_ready_stays_deferred_until_full_d13_conjunction)
{
	ClusterSemanticActivationRefusal refusal;

	test_gate_reset();
	test_r4a_snapshot = valid_r4a_ready_snapshot();
	memset(&refusal, 0, sizeof(refusal));
	UT_ASSERT_EQ(r4_pre_prepare_readiness(19, &refusal),
				 CLUSTER_SEMANTIC_ACTIVATION_RF_DEFERRED);
	test_gate_reset();
}

UT_TEST(test_89g_d13_current_coordinator_handoff_consumes_exact_admitted_basis)
{
	test_gate_reset();
	test_admitted_snapshot_valid = true;
	test_admitted_snapshot_episode = valid_d13_admitted_episode();
	test_admitted_snapshot_marker = valid_d13_admitted_marker();
	UT_ASSERT(semantic_activation_r4_current_admitted_basis());
	UT_ASSERT_EQ(test_admitted_snapshot_calls, 1);

	test_admitted_snapshot_marker.ready_state_generation++;
	UT_ASSERT(!semantic_activation_r4_current_admitted_basis());
	UT_ASSERT_EQ(test_admitted_snapshot_calls, 2);
}

UT_TEST(test_89h_pre_prepare_consumes_durable_admitted_not_ready_getter)
{
	ClusterSemanticActivationRefusal refusal;

	test_gate_reset();
	test_admitted_snapshot_valid = true;
	test_admitted_snapshot_episode = valid_d13_admitted_episode();
	test_admitted_snapshot_marker = valid_d13_admitted_marker();
	memset(&refusal, 0, sizeof(refusal));
	UT_ASSERT_EQ(r4_pre_prepare_readiness(19, &refusal),
				 CLUSTER_SEMANTIC_ACTIVATION_OK);
	UT_ASSERT_EQ(test_admitted_snapshot_calls, 1);
	UT_ASSERT_EQ(test_r4a_snapshot_calls, 0);
	UT_ASSERT_EQ(refusal.result, CLUSTER_SEMANTIC_ACTIVATION_OK);
	UT_ASSERT_EQ(refusal.feature_bit,
				 CLUSTER_SEMANTIC_FEATURE_R4_SYNC_CR_V1);
	UT_ASSERT_EQ(refusal.expected_generation, 19);
	test_gate_reset();
}

UT_TEST(test_89i_d13_invalidator_rescan_accepts_only_same_settled_closed_head)
{
	ClusterReplacementEpisode episode = valid_d13_admitted_episode();
	ClusterReplacementCommitMarkerV3 marker = valid_d13_admitted_marker();
	ClusterQvotecMailboxCompletion completion;
	ClusterEpochAuthorityValue head;
	ClusterEpochBallotId ballot;

	memset(&completion, 0, sizeof(completion));
	completion.request_seq = UINT64_C(2);
	completion.result = CLUSTER_QVOTEC_MAILBOX_CHOSEN;
	completion.observed_disk_bitmap = UINT8_C(0x01);
	completion.actor_phase = CLUSTER_QVOTEC_ACTOR_RECOVER_SCAN_B;

	memset(&head, 0, sizeof(head));
	head.value_version = CLUSTER_EPOCH_AUTHORITY_VALUE_VERSION;
	head.transition = CLUSTER_EPOCH_AUTHORITY_COMMIT_CLOSED;
	head.event_kind = CLUSTER_EPOCH_EVENT_SAME_NODE_REPLACEMENT;
	head.request_origin_node = episode.target_node_id;
	head.target_node_id = episode.target_node_id;
	head.authority_generation = UINT64_C(103);
	head.baseline_epoch = episode.baseline_epoch;
	head.reserved_epoch = episode.reserved_or_committed_epoch;
	head.old_incarnation = episode.old_admitted_incarnation;
	head.fresh_incarnation = episode.fresh_incarnation;
	head.request_nonce = episode.request_nonce;
	memcpy(head.authority_member_bitmap, episode.expected_survivors,
		   sizeof(head.authority_member_bitmap));
	head.event_subject_bitmap[episode.target_node_id / 8]
		= (uint8)(1u << (episode.target_node_id % 8));
	head.grammar_fingerprint = episode.grammar_fingerprint;
	memset(head.predecessor_digest, 0x5a, sizeof(head.predecessor_digest));
	UT_ASSERT(cluster_epoch_authority_value_encode(
		&head, CLUSTER_EPOCH_BALLOT_GRAMMAR_FINGERPRINT,
		completion.completion_value));

	memset(&ballot, 0, sizeof(ballot));
	ballot.counter = UINT64_C(7);
	ballot.proposer_node_id = 1;
	ballot.proposer_admitted_incarnation = UINT64_C(111);
	ballot.nonce = UINT64_C(0xabcdef);
	UT_ASSERT(cluster_epoch_ballot_id_encode(
		&ballot, completion.completion_ballot));

	UT_ASSERT(semantic_activation_r4_invalidator_rescan_matches(
		&completion, &marker, &episode));
	completion.result = CLUSTER_QVOTEC_MAILBOX_ADOPTED_OTHER;
	UT_ASSERT(!semantic_activation_r4_invalidator_rescan_matches(
		&completion, &marker, &episode));
	completion.result = CLUSTER_QVOTEC_MAILBOX_CHOSEN;
	head.fresh_incarnation++;
	UT_ASSERT(cluster_epoch_authority_value_encode(
		&head, CLUSTER_EPOCH_BALLOT_GRAMMAR_FINGERPRINT,
		completion.completion_value));
	UT_ASSERT(!semantic_activation_r4_invalidator_rescan_matches(
		&completion, &marker, &episode));
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

UT_TEST(test_93a_activation_actor_effect_ownership_is_exact)
{
	UT_ASSERT(semantic_activation_actor_effect_allowed(
		SEMANTIC_ACTIVATION_ACTOR_PROCESS_UTILITY,
		SEMANTIC_ACTIVATION_EFFECT_REQUEST_PUBLICATION));
	UT_ASSERT(!semantic_activation_actor_effect_allowed(SEMANTIC_ACTIVATION_ACTOR_PROCESS_UTILITY,
										SEMANTIC_ACTIVATION_EFFECT_PGSA_WRITE));
	UT_ASSERT(semantic_activation_actor_effect_allowed(SEMANTIC_ACTIVATION_ACTOR_LMON,
									  SEMANTIC_ACTIVATION_EFFECT_SOURCE_CLOSE));
	UT_ASSERT(semantic_activation_actor_effect_allowed(SEMANTIC_ACTIVATION_ACTOR_LMON,
									  SEMANTIC_ACTIVATION_EFFECT_TARGET_OPEN));
	UT_ASSERT(semantic_activation_actor_effect_allowed(SEMANTIC_ACTIVATION_ACTOR_LMON,
									  SEMANTIC_ACTIVATION_EFFECT_ACK_MUTATION));
	UT_ASSERT(semantic_activation_actor_effect_allowed(SEMANTIC_ACTIVATION_ACTOR_LMON,
									  SEMANTIC_ACTIVATION_EFFECT_CONTROL_WIRE));
	UT_ASSERT(!semantic_activation_actor_effect_allowed(SEMANTIC_ACTIVATION_ACTOR_LMON,
									   SEMANTIC_ACTIVATION_EFFECT_PGSA_WRITE));
	UT_ASSERT(semantic_activation_actor_effect_allowed(SEMANTIC_ACTIVATION_ACTOR_QVOTEC,
									  SEMANTIC_ACTIVATION_EFFECT_PGSA_WRITE));
	UT_ASSERT(!semantic_activation_actor_effect_allowed(SEMANTIC_ACTIVATION_ACTOR_QVOTEC,
									   SEMANTIC_ACTIVATION_EFFECT_ACK_MUTATION));
	UT_ASSERT(!semantic_activation_actor_effect_allowed(SEMANTIC_ACTIVATION_ACTOR_LMS,
									   SEMANTIC_ACTIVATION_EFFECT_DATA_WIRE));
	UT_ASSERT(!semantic_activation_actor_effect_allowed(SEMANTIC_ACTIVATION_ACTOR_DATA,
									   SEMANTIC_ACTIVATION_EFFECT_DATA_WIRE));
	UT_ASSERT(!semantic_activation_actor_effect_allowed(
		SEMANTIC_ACTIVATION_ACTOR_PROCESS_UTILITY,
		(SemanticActivationEffect)(SEMANTIC_ACTIVATION_EFFECT_REQUEST_PUBLICATION
							   | SEMANTIC_ACTIVATION_EFFECT_PGSA_WRITE)));
}

UT_TEST(test_93b_activation_mailbox_route_has_no_owner_bypass)
{
	UT_ASSERT(semantic_activation_actor_edge_allowed(
		SEMANTIC_ACTIVATION_ACTOR_PROCESS_UTILITY, SEMANTIC_ACTIVATION_ACTOR_LMON));
	UT_ASSERT(semantic_activation_actor_edge_allowed(SEMANTIC_ACTIVATION_ACTOR_LMON,
										SEMANTIC_ACTIVATION_ACTOR_QVOTEC));
	UT_ASSERT(!semantic_activation_actor_edge_allowed(
		SEMANTIC_ACTIVATION_ACTOR_PROCESS_UTILITY, SEMANTIC_ACTIVATION_ACTOR_QVOTEC));
	UT_ASSERT(!semantic_activation_actor_edge_allowed(SEMANTIC_ACTIVATION_ACTOR_PROCESS_UTILITY,
										 SEMANTIC_ACTIVATION_ACTOR_LMS));
	UT_ASSERT(!semantic_activation_actor_edge_allowed(SEMANTIC_ACTIVATION_ACTOR_LMON,
										 SEMANTIC_ACTIVATION_ACTOR_DATA));
	UT_ASSERT(!semantic_activation_actor_edge_allowed(SEMANTIC_ACTIVATION_ACTOR_QVOTEC,
										 SEMANTIC_ACTIVATION_ACTOR_LMON));
}

UT_TEST(test_93c_utility_mailbox_preserves_exact_owner_tuple_and_completion)
{
	SemanticActivationUtilityRequest request;
	ClusterSemanticActivationRefusal refusal;
	uint64 request_seq = 0;
	uint64 blocked_seq = 0;

	test_gate_reset();
	memset(&request, 0, sizeof(request));
	memset(&refusal, 0, sizeof(refusal));
	UT_ASSERT(semantic_activation_utility_mailbox_submit(
		CLUSTER_SEMANTIC_ENABLE_ALL, UINT64_C(0x11), UINT64_C(0x22),
		UINT64_C(0x33), UINT64_C(41), &request_seq));
	UT_ASSERT(request_seq != 0);
	UT_ASSERT(!semantic_activation_utility_mailbox_submit(
		CLUSTER_SEMANTIC_DISABLE_ALL, 0, 0, 0, 0, &blocked_seq));
	UT_ASSERT(semantic_activation_utility_mailbox_poll(&request));
	UT_ASSERT_EQ(request.request_seq, request_seq);
	UT_ASSERT_EQ(request.action, CLUSTER_SEMANTIC_ENABLE_ALL);
	UT_ASSERT_EQ(request.source_feature_bitmap, UINT64_C(0x11));
	UT_ASSERT_EQ(request.target_feature_bitmap, UINT64_C(0x22));
	UT_ASSERT_EQ(request.rollback_feature_bitmap, UINT64_C(0x33));
	UT_ASSERT_EQ(request.expected_record_generation, UINT64_C(41));
	UT_ASSERT_EQ(pg_atomic_read_u64(
		&SemanticActivationShmem->record_cas_request_seq), UINT64_C(0));
	UT_ASSERT(!semantic_activation_utility_mailbox_complete(
		request_seq + 1, CLUSTER_SEMANTIC_ACTIVATION_OK, 0, 41));
	UT_ASSERT(semantic_activation_utility_mailbox_complete(
		request_seq, CLUSTER_SEMANTIC_ACTIVATION_RF_DEFERRED,
		CLUSTER_SEMANTIC_FEATURE_R4_SYNC_CR_V1, 41));
	UT_ASSERT(semantic_activation_utility_mailbox_poll_completion(
		request_seq, &refusal));
	UT_ASSERT_EQ(refusal.result, CLUSTER_SEMANTIC_ACTIVATION_RF_DEFERRED);
	UT_ASSERT_EQ(refusal.feature_bit,
				 CLUSTER_SEMANTIC_FEATURE_R4_SYNC_CR_V1);
	UT_ASSERT_EQ(refusal.expected_generation, UINT64_C(41));
	UT_ASSERT(!semantic_activation_utility_mailbox_poll_completion(
		request_seq, &refusal));
}

UT_TEST(test_93d_formation_lmon_alone_consumes_utility_request)
{
	ClusterSemanticActivationRefusal refusal;
	uint64 request_seq = 0;

	test_gate_reset();
	test_admitted_snapshot_valid = true;
	test_admitted_snapshot_episode = valid_d13_admitted_episode();
	test_admitted_snapshot_marker = valid_d13_admitted_marker();
	UT_ASSERT(semantic_activation_utility_mailbox_submit(
		CLUSTER_SEMANTIC_ENABLE_ALL, 0,
		CLUSTER_SEMANTIC_FEATURE_R4_SYNC_CR_V1, 0, 0, &request_seq));
	cluster_semantic_activation_lmon_tick();
	memset(&refusal, 0, sizeof(refusal));
	UT_ASSERT(semantic_activation_utility_mailbox_poll_completion(
		request_seq, &refusal));
	UT_ASSERT_EQ(refusal.result, CLUSTER_SEMANTIC_ACTIVATION_RF_DEFERRED);
	UT_ASSERT_EQ(refusal.feature_bit,
				 CLUSTER_SEMANTIC_FEATURE_R4_SYNC_CR_V1);
	UT_ASSERT_EQ(refusal.expected_generation, UINT64_C(0));
	UT_ASSERT_EQ(test_admitted_snapshot_calls, 1);
	UT_ASSERT_EQ(pg_atomic_read_u64(
		&SemanticActivationShmem->record_cas_request_seq), UINT64_C(0));
	test_gate_reset();
}

UT_TEST(test_93e_utility_wait_returns_only_matching_terminal_result)
{
	SemanticActivationUtilityRequest request;
	ClusterSemanticActivationRefusal refusal;
	uint64 request_seq = 0;

	test_gate_reset();
	UT_ASSERT(semantic_activation_utility_mailbox_submit(
		CLUSTER_SEMANTIC_ENABLE_ALL, 0,
		CLUSTER_SEMANTIC_FEATURE_R4_SYNC_CR_V1, 0, 0, &request_seq));
	UT_ASSERT(semantic_activation_utility_mailbox_poll(&request));
	UT_ASSERT(semantic_activation_utility_mailbox_complete(
		request.request_seq, CLUSTER_SEMANTIC_ACTIVATION_OK, 0, 0));
	memset(&refusal, 0xa5, sizeof(refusal));
	UT_ASSERT_EQ(semantic_activation_utility_mailbox_wait(
					 request_seq, &refusal),
				 CLUSTER_SEMANTIC_ACTIVATION_OK);
	UT_ASSERT_EQ(refusal.result, CLUSTER_SEMANTIC_ACTIVATION_OK);
	UT_ASSERT_EQ(refusal.feature_bit, UINT64_C(0));
	UT_ASSERT_EQ(refusal.expected_generation, UINT64_C(0));
	UT_ASSERT(!semantic_activation_utility_mailbox_poll_completion(
		request_seq, &refusal));
}

UT_TEST(test_93ea_utility_wait_does_not_synthesize_elapsed_terminal)
{
	ClusterSemanticActivationRefusal refusal;
	uint64 request_seq = 0;

	test_gate_reset();
	UT_ASSERT(semantic_activation_utility_mailbox_submit(
		CLUSTER_SEMANTIC_ENABLE_ALL, 0,
		CLUSTER_SEMANTIC_FEATURE_R4_SYNC_CR_V1, 0, 0, &request_seq));
	test_complete_after_wait_sleeps = 6000;
	test_wait_completion_request_seq = request_seq;
	memset(&refusal, 0xa5, sizeof(refusal));
	UT_ASSERT_EQ(semantic_activation_utility_mailbox_wait(
					 request_seq, &refusal),
				 CLUSTER_SEMANTIC_ACTIVATION_OK);
	UT_ASSERT(test_wait_completion_succeeded);
	UT_ASSERT_EQ(test_wait_sleep_calls, 6000);
	UT_ASSERT_EQ(refusal.result, CLUSTER_SEMANTIC_ACTIVATION_OK);
	UT_ASSERT(!semantic_activation_utility_mailbox_poll_completion(
		request_seq, &refusal));
}

UT_TEST(test_93f_pgsa_read_mailbox_round_trip_is_qvotec_owned)
{
	ClusterSemanticActivationReadRequest request;
	ClusterSemanticActivationReadCompletion completion;
	ClusterSemanticActivationRecord record;
	uint8 bytes[CLUSTER_SEMANTIC_ACTIVATION_RECORD_BYTES];
	uint64 request_seq = 0;

	test_gate_reset();
	memset(&record, 0, sizeof(record));
	record.source_feature_bitmap = 0;
	record.target_feature_bitmap = CLUSTER_SEMANTIC_FEATURE_R4_SYNC_CR_V1;
	record.transition_epoch = test_current_epoch;
	record.record_generation = 1;
	record.admitted_members_lo = UINT64_C(0x0f);
	record.capability_sample_digest = UINT64_C(0x1234);
	record.coordinator_incarnation = UINT64_C(0x55);
	record.coordinator_node = 1;
	record.phase = CLUSTER_SEMANTIC_PHASE_PREPARE;
	UT_ASSERT(cluster_semantic_activation_record_encode(&record, bytes));
	UT_ASSERT(semantic_activation_record_read_mailbox_submit(&request_seq));
	UT_ASSERT(cluster_semantic_activation_qvotec_poll_record_read(&request));
	UT_ASSERT_EQ(request.request_seq, request_seq);
	UT_ASSERT(!cluster_semantic_activation_qvotec_complete_record_read(
		request_seq + 1, CLUSTER_SEMANTIC_ACTIVATION_OK, false, bytes));
	UT_ASSERT(cluster_semantic_activation_qvotec_complete_record_read(
		request_seq, CLUSTER_SEMANTIC_ACTIVATION_OK, false, bytes));
	memset(&completion, 0, sizeof(completion));
	UT_ASSERT(semantic_activation_record_read_mailbox_poll_completion(
		request_seq, &completion));
	UT_ASSERT_EQ(completion.result, CLUSTER_SEMANTIC_ACTIVATION_OK);
	UT_ASSERT(!completion.implicit_open);
	UT_ASSERT_EQ(memcmp(completion.selected_bytes, bytes, sizeof(bytes)), 0);
}

UT_TEST(test_93fa_wrong_read_completion_cannot_mutate_pending_cas)
{
	ClusterSemanticActivationCasRequest request;
	ClusterSemanticActivationRecord record;
	uint8 desired[CLUSTER_SEMANTIC_ACTIVATION_RECORD_BYTES];
	uint8 zero[CLUSTER_SEMANTIC_ACTIVATION_RECORD_BYTES] = { 0 };
	uint64 request_seq = 0;

	test_gate_reset();
	memset(&record, 0, sizeof(record));
	record.source_feature_bitmap = 0;
	record.target_feature_bitmap = CLUSTER_SEMANTIC_FEATURE_R4_SYNC_CR_V1;
	record.transition_epoch = test_current_epoch;
	record.record_generation = 1;
	record.admitted_members_lo = UINT64_C(0x0f);
	record.capability_sample_digest = UINT64_C(0x1234);
	record.coordinator_incarnation = UINT64_C(0x55);
	record.coordinator_node = 1;
	record.phase = CLUSTER_SEMANTIC_PHASE_PREPARE;
	UT_ASSERT(cluster_semantic_activation_record_encode(&record, desired));
	UT_ASSERT(semantic_activation_record_cas_mailbox_submit(
		0, 0, desired, &request_seq));
	UT_ASSERT(!cluster_semantic_activation_qvotec_complete_record_read(
		request_seq, CLUSTER_SEMANTIC_ACTIVATION_OK, true, zero));
	memset(&request, 0, sizeof(request));
	UT_ASSERT(cluster_semantic_activation_qvotec_poll_record_cas(&request));
	UT_ASSERT_EQ(memcmp(request.desired_bytes, desired, sizeof(desired)), 0);
}

UT_TEST(test_94_public_submit_refuses_before_prepare)
{
	ClusterSemanticActivationRefusal refusal;

	memset(&refusal, 0, sizeof(refusal));
	UT_ASSERT_EQ(cluster_semantic_activation_submit(CLUSTER_SEMANTIC_ENABLE_ALL, &refusal),
				 CLUSTER_SEMANTIC_ACTIVATION_RF_DEFERRED);
	UT_ASSERT_EQ(refusal.result, CLUSTER_SEMANTIC_ACTIVATION_RF_DEFERRED);
}

UT_TEST(test_94a_public_submit_cannot_bypass_busy_lmon_mailbox)
{
	ClusterSemanticActivationRefusal refusal;
	uint64 request_seq = 0;

	test_gate_reset();
	test_admitted_snapshot_valid = true;
	test_admitted_snapshot_episode = valid_d13_admitted_episode();
	test_admitted_snapshot_marker = valid_d13_admitted_marker();
	UT_ASSERT(semantic_activation_utility_mailbox_submit(
		CLUSTER_SEMANTIC_ENABLE_ALL, 0,
		CLUSTER_SEMANTIC_FEATURE_R4_SYNC_CR_V1, 0, 0, &request_seq));
	memset(&refusal, 0, sizeof(refusal));
	UT_ASSERT_EQ(cluster_semantic_activation_submit(
					 CLUSTER_SEMANTIC_ENABLE_ALL, &refusal),
				 CLUSTER_SEMANTIC_ACTIVATION_QUORUM_HOLD);
	UT_ASSERT_EQ(refusal.result, CLUSTER_SEMANTIC_ACTIVATION_QUORUM_HOLD);
	UT_ASSERT_EQ(refusal.expected_generation, UINT64_C(0));
	UT_ASSERT_EQ(pg_atomic_read_u64(
		&SemanticActivationShmem->record_cas_request_seq), UINT64_C(0));
	test_gate_reset();
}

UT_TEST(test_94b_utility_cannot_close_source_before_prepare_commit)
{
	ClusterSemanticActivationRefusal refusal;
	uint64 request_seq = 0;

	test_gate_reset();
	test_gate_publish(2, 0, 0, test_current_epoch, false);
	test_admitted_snapshot_valid = true;
	test_admitted_snapshot_episode = valid_d13_admitted_episode();
	test_admitted_snapshot_marker = valid_d13_admitted_marker();
	UT_ASSERT(semantic_activation_utility_mailbox_submit(
		CLUSTER_SEMANTIC_ENABLE_ALL, 0,
		CLUSTER_SEMANTIC_FEATURE_R4_SYNC_CR_V1, 0, 0, &request_seq));
	cluster_semantic_activation_lmon_tick();
	memset(&refusal, 0, sizeof(refusal));
	UT_ASSERT(semantic_activation_utility_mailbox_poll_completion(
		request_seq, &refusal));
	UT_ASSERT_EQ(refusal.result, CLUSTER_SEMANTIC_ACTIVATION_RF_DEFERRED);
	UT_ASSERT_EQ(pg_atomic_read_u64(test_gate_u64(TEST_GATE_SEQ_OFFSET)),
				 UINT64_C(2));
	UT_ASSERT_EQ(pg_atomic_read_u32(test_gate_u32(TEST_GATE_CLOSED_OFFSET)), 0);
	UT_ASSERT_EQ(pg_atomic_read_u64(
		&SemanticActivationShmem->record_cas_request_seq), UINT64_C(0));
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
	UT_ASSERT_EQ(test_shmem_requested_size, TEST_SEMANTIC_GATE_SHMEM_BYTES);
	UT_ASSERT_EQ(test_utility_mailbox_requested_size,
				 TEST_SEMANTIC_UTILITY_MAILBOX_BYTES);
	UT_ASSERT_EQ(cluster_semantic_activation_shmem_size(),
				 TEST_SEMANTIC_GATE_SHMEM_BYTES
				 + TEST_SEMANTIC_UTILITY_MAILBOX_BYTES);
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

UT_TEST(test_100a_modifier_bootstrap_source_requires_ordinary_write_gate)
{
	ClusterSemanticAdmissionToken token;

	test_gate_reset();
	test_gate_publish(2, 0, 0, test_current_epoch, true);
	UT_ASSERT_EQ(cluster_semantic_activation_modifier_enter(true, &token),
				 CLUSTER_SEMANTIC_ADMISSION_OK);
	UT_ASSERT(token.entered);
	UT_ASSERT(cluster_semantic_activation_modifier_recheck(&token, true));
	UT_ASSERT_EQ(pg_atomic_read_u32(test_gate_inflight(CLUSTER_SEMANTIC_SOURCE_SIDE, 0)), 1);
	cluster_semantic_activation_leave(&token);
}

UT_TEST(test_100b_modifier_bootstrap_source_refuses_replacement_closed_member)
{
	ClusterSemanticAdmissionToken token;

	test_gate_reset();
	test_gate_publish(2, 0, 0, test_current_epoch, true);
	UT_ASSERT_EQ(cluster_semantic_activation_modifier_enter(false, &token),
				 CLUSTER_SEMANTIC_ADMISSION_CLOSED);
	UT_ASSERT(!token.entered);
	UT_ASSERT_EQ(pg_atomic_read_u32(test_gate_inflight(CLUSTER_SEMANTIC_SOURCE_SIDE, 0)), 0);
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

UT_TEST(test_109a_lmon_publishes_source_open_only_after_majority_legacy_zero)
{
	ClusterSemanticActivationReadRequest request;
	ClusterSemanticAdmissionToken token;
	uint8 zero[CLUSTER_SEMANTIC_ACTIVATION_RECORD_BYTES] = { 0 };

	test_gate_reset();
	cluster_semantic_activation_lmon_tick();
	UT_ASSERT_EQ(pg_atomic_read_u32(test_gate_u32(TEST_GATE_CLOSED_OFFSET)), 1);
	cluster_semantic_activation_lmon_tick();
	UT_ASSERT(cluster_semantic_activation_qvotec_poll_record_read(&request));
	UT_ASSERT(cluster_semantic_activation_qvotec_complete_record_read(
		request.request_seq, CLUSTER_SEMANTIC_ACTIVATION_OK, true, zero));
	cluster_semantic_activation_lmon_tick();
	UT_ASSERT_EQ(pg_atomic_read_u64(test_gate_u64(TEST_GATE_ACTIVE_BITS_OFFSET)),
				 UINT64_C(0));
	UT_ASSERT_EQ(pg_atomic_read_u64(
				 test_gate_u64(TEST_GATE_RECORD_GENERATION_OFFSET)),
				 UINT64_C(0));
	UT_ASSERT_EQ(pg_atomic_read_u64(test_gate_u64(TEST_GATE_FORMATION_EPOCH_OFFSET)),
				 test_current_epoch);
	UT_ASSERT_EQ(pg_atomic_read_u32(test_gate_u32(TEST_GATE_CLOSED_OFFSET)), 0);
	UT_ASSERT_EQ(test_membership_snapshot_calls, 1);
	UT_ASSERT_EQ(cluster_semantic_activation_modifier_enter(true, &token),
				 CLUSTER_SEMANTIC_ADMISSION_OK);
	cluster_semantic_activation_leave(&token);
}

UT_TEST(test_109b_lmon_legacy_zero_requires_coherent_admitted_membership)
{
	ClusterSemanticActivationReadRequest request;
	uint8 zero[CLUSTER_SEMANTIC_ACTIVATION_RECORD_BYTES] = { 0 };

	test_gate_reset();
	test_membership_snapshot_valid = false;
	cluster_semantic_activation_lmon_tick();
	cluster_semantic_activation_lmon_tick();
	UT_ASSERT(cluster_semantic_activation_qvotec_poll_record_read(&request));
	UT_ASSERT(cluster_semantic_activation_qvotec_complete_record_read(
		request.request_seq, CLUSTER_SEMANTIC_ACTIVATION_OK, true, zero));
	cluster_semantic_activation_lmon_tick();
	UT_ASSERT_EQ(test_membership_snapshot_calls, 1);
	UT_ASSERT_EQ(pg_atomic_read_u32(test_gate_u32(TEST_GATE_CLOSED_OFFSET)), 1);
}

UT_TEST(test_109c_lmon_legacy_zero_rejects_membership_epoch_drift)
{
	ClusterSemanticActivationReadRequest request;
	uint8 zero[CLUSTER_SEMANTIC_ACTIVATION_RECORD_BYTES] = { 0 };

	test_gate_reset();
	test_membership_snapshot_epoch = test_current_epoch + 1;
	cluster_semantic_activation_lmon_tick();
	cluster_semantic_activation_lmon_tick();
	UT_ASSERT(cluster_semantic_activation_qvotec_poll_record_read(&request));
	UT_ASSERT(cluster_semantic_activation_qvotec_complete_record_read(
		request.request_seq, CLUSTER_SEMANTIC_ACTIVATION_OK, true, zero));
	cluster_semantic_activation_lmon_tick();
	UT_ASSERT_EQ(test_membership_snapshot_calls, 1);
	UT_ASSERT_EQ(pg_atomic_read_u32(test_gate_u32(TEST_GATE_CLOSED_OFFSET)), 1);
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

UT_TEST(test_111a_close_source_publishes_closed_before_debt_result)
{
	ClusterSemanticAdmissionToken token;

	test_gate_reset();
	test_gate_publish(2, 0, 25, test_current_epoch, false);
	UT_ASSERT_EQ(cluster_semantic_activation_enter(
					 CLUSTER_SEMANTIC_FEATURE_R4_SYNC_CR_V1,
					 CLUSTER_SEMANTIC_SOURCE_SIDE, &token),
				 CLUSTER_SEMANTIC_ADMISSION_OK);
	UT_ASSERT_EQ(r4_descriptor.close_source_admission(25),
				 CLUSTER_SEMANTIC_ACTIVATION_DEBT_NONZERO);
	UT_ASSERT_EQ(pg_atomic_read_u64(test_gate_u64(TEST_GATE_SEQ_OFFSET)),
				 UINT64_C(4));
	UT_ASSERT_EQ(pg_atomic_read_u64(test_gate_u64(TEST_GATE_ACTIVE_BITS_OFFSET)),
				 UINT64_C(0));
	UT_ASSERT_EQ(pg_atomic_read_u64(
				 test_gate_u64(TEST_GATE_RECORD_GENERATION_OFFSET)),
				 UINT64_C(25));
	UT_ASSERT_EQ(pg_atomic_read_u64(test_gate_u64(TEST_GATE_FORMATION_EPOCH_OFFSET)),
				 test_current_epoch);
	UT_ASSERT_EQ(pg_atomic_read_u32(test_gate_u32(TEST_GATE_CLOSED_OFFSET)), 1);
	cluster_semantic_activation_leave(&token);
	UT_ASSERT_EQ(r4_descriptor.close_source_admission(25),
				 CLUSTER_SEMANTIC_ACTIVATION_OK);
	UT_ASSERT_EQ(pg_atomic_read_u64(test_gate_u64(TEST_GATE_SEQ_OFFSET)),
				 UINT64_C(4));
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
					   | PGRAC_IC_HELLO_CAP_R4_SYNC_CR_V1
					   | PGRAC_IC_HELLO_CAP_CANDIDATE2_CORRECTED_A1_V1
					   | PGRAC_IC_HELLO_CAP_UNDO_ROOT_DESCRIPTOR_V1,
		0));
	UT_ASSERT_EQ(test_peer_capability_match_calls, 1);
	UT_ASSERT_EQ(test_peer_capability_match_peer, 7);
	UT_ASSERT_EQ(test_peer_capability_match_caps,
				 PGRAC_IC_HELLO_CAP_SEMANTIC_ACTIVATION_V1
					 | PGRAC_IC_HELLO_CAP_R4_SYNC_CR_V1
					 | PGRAC_IC_HELLO_CAP_CANDIDATE2_CORRECTED_A1_V1
					 | PGRAC_IC_HELLO_CAP_UNDO_ROOT_DESCRIPTOR_V1);
	UT_ASSERT_EQ(test_peer_capability_match_generation, 0);
	cluster_semantic_activation_leave(&token);
}

UT_TEST(test_115_peer_open_matcher_rejects_invalid_inputs_before_capability_match)
{
	ClusterSemanticAdmissionToken target_token;
	ClusterSemanticAdmissionToken source_token;
	ClusterSemanticAdmissionToken wrong_feature_token;
	uint32 required_caps = PGRAC_IC_HELLO_CAP_SEMANTIC_ACTIVATION_V1
						  | PGRAC_IC_HELLO_CAP_R4_SYNC_CR_V1
						  | PGRAC_IC_HELLO_CAP_CANDIDATE2_CORRECTED_A1_V1
						  | PGRAC_IC_HELLO_CAP_UNDO_ROOT_DESCRIPTOR_V1;

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

/* Break caught: authenticated opcode-18 phase-3 ingress must terminate at
 * formation LMON, not remain forever in the process-local handoff.  This test
 * exercises the real codec/ingress and asserts that one LMON tick consumes the
 * exact item and delegates only to the reconfig observer. */
UT_TEST(test_116_formation_lmon_consumes_phase3_handoff)
{
	ClusterReplacementPhase3HandoffItem ignored;
	ClusterReplacementWireMessage message;
	ClusterICEnvelope envelope;
	uint8 bytes[CLUSTER_REPLACEMENT_WIRE_BYTES];

	while (cluster_replacement_phase3_handoff_poll_local(&ignored))
		;
	test_gate_reset();
	test_peer_capability_matches = true;
	memset(&message, 0, sizeof(message));
	message.phase = CLUSTER_REPLACEMENT_WIRE_PHASE_TARGET_RECOVERY_READY;
	message.target_node_id = 3;
	message.epoch = test_current_epoch - 1;
	message.request_nonce = UINT64_C(0x1112131415161718);
	message.identity0 = UINT64_C(9001);
	message.identity1 = UINT64_C(9002);
	message.body.phase3.jcmk_generation = UINT64_C(41);
	message.body.phase3.episode_state_generation = UINT32_C(17);
	message.grammar_fingerprint
		= CANDIDATE2_CORRECTED_A1_GRAMMAR_FINGERPRINT;
	UT_ASSERT(cluster_replacement_wire_encode(&message, bytes));
	memset(&envelope, 0, sizeof(envelope));
	envelope.msg_type = PGRAC_IC_MSG_GES_REQUEST;
	envelope.source_node_id = (uint32)message.target_node_id;
	envelope.dest_node_id = 1;
	envelope.epoch = test_current_epoch;
	envelope.payload_length = sizeof(bytes);
	UT_ASSERT_EQ((int)cluster_replacement_wire_phase3_ingress_local(
					 &envelope, bytes, sizeof(bytes), message.target_node_id, 1,
					 test_current_epoch, 9),
				 (int)CLUSTER_REPLACEMENT_PHASE3_INGRESS_ENQUEUED);

	cluster_semantic_activation_lmon_tick();

	UT_ASSERT_EQ((int)cluster_replacement_phase3_handoff_pending_local(), 0);
	UT_ASSERT_EQ(test_phase3_observe_calls, 1);
	UT_ASSERT_EQ(memcmp(&test_phase3_observed_item.message, &message,
						 sizeof(message)),
				 0);
	UT_ASSERT_EQ(test_phase3_observed_item.authenticated_source_node_id, 3);
	UT_ASSERT_EQ(test_phase3_observed_item.local_receiver_node_id, 1);
	UT_ASSERT_EQ((int)test_phase3_observed_item.control_connection_generation, 9);
	UT_ASSERT_EQ(test_peer_capability_match_calls, 1);
	UT_ASSERT_EQ(test_peer_capability_match_peer, 3);
	UT_ASSERT_EQ(test_peer_capability_match_caps, (uint32)0x00100000U);
	UT_ASSERT_EQ(test_peer_capability_match_generation, (uint32)9);
}

int
main(void)
{
	UT_PLAN(144);
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
	UT_RUN(test_75a_d13_full_ack_table_requires_exact_member_set_and_tuples);
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
	UT_RUN(test_85a_modifier_gate_requires_durable_source_or_target_open);
	UT_RUN(test_85b_modifier_gate_closes_replacement_until_uniform_open);
	UT_RUN(test_86_r4a_snapshot_is_fixed_false);
	UT_RUN(test_87_readiness_adapter_returns_rf_deferred);
	UT_RUN(test_88_readiness_adapter_names_r4_feature);
	UT_RUN(test_89_readiness_adapter_preserves_expected_generation);
	UT_RUN(test_89d_local_ready_alone_cannot_enter_prepare);
	UT_RUN(test_89e_malformed_local_ready_remains_typed_deferred);
	UT_RUN(test_89a_d13_prepare_basis_accepts_only_exact_admitted_ready_lineage);
	UT_RUN(test_89b_d13_prepare_basis_requires_admitted_ready_polarity);
	UT_RUN(test_89c_d13_prepare_basis_rejects_generation_or_identity_drift);
	UT_RUN(test_89f_positive_ready_stays_deferred_until_full_d13_conjunction);
	UT_RUN(test_89g_d13_current_coordinator_handoff_consumes_exact_admitted_basis);
	UT_RUN(test_89h_pre_prepare_consumes_durable_admitted_not_ready_getter);
	UT_RUN(test_89i_d13_invalidator_rescan_accepts_only_same_settled_closed_head);
	UT_RUN(test_90_descriptor_uses_the_only_r4a_adapter);
	UT_RUN(test_91_preflight_refusal_is_before_every_mutation);
	UT_RUN(test_92_preflight_refusal_names_condition_feature);
	UT_RUN(test_93_preflight_rejects_bad_action_without_effects);
	UT_RUN(test_93a_activation_actor_effect_ownership_is_exact);
	UT_RUN(test_93b_activation_mailbox_route_has_no_owner_bypass);
	UT_RUN(test_93c_utility_mailbox_preserves_exact_owner_tuple_and_completion);
	UT_RUN(test_93d_formation_lmon_alone_consumes_utility_request);
	UT_RUN(test_93e_utility_wait_returns_only_matching_terminal_result);
	UT_RUN(test_93ea_utility_wait_does_not_synthesize_elapsed_terminal);
	UT_RUN(test_93f_pgsa_read_mailbox_round_trip_is_qvotec_owned);
	UT_RUN(test_93fa_wrong_read_completion_cannot_mutate_pending_cas);
	UT_RUN(test_94_public_submit_refuses_before_prepare);
	UT_RUN(test_94a_public_submit_cannot_bypass_busy_lmon_mailbox);
	UT_RUN(test_94b_utility_cannot_close_source_before_prepare_commit);
	UT_RUN(test_95_dormant_target_enter_has_no_token);
	UT_RUN(test_96_source_token_recheck_and_leave_are_generation_scoped);
	UT_RUN(test_97_old_epoch_completion_is_inert_and_requires_revalidation);
	UT_RUN(test_98_admission_token_has_frozen_natural_layout);
	UT_RUN(test_99_shared_gate_layout_and_bootstrap_are_fail_closed);
	UT_RUN(test_100_source_enter_owns_shared_debt_and_epoch_token);
	UT_RUN(test_100a_modifier_bootstrap_source_requires_ordinary_write_gate);
	UT_RUN(test_100b_modifier_bootstrap_source_refuses_replacement_closed_member);
	UT_RUN(test_101_active_source_refuses_before_debt);
	UT_RUN(test_102_inactive_target_refuses_before_debt);
	UT_RUN(test_103_epoch_drift_invalidates_recheck_without_losing_debt);
	UT_RUN(test_104_close_invalidates_recheck_and_leave_balances_once);
	UT_RUN(test_105_pid_change_discards_inherited_local_ledger_only);
	UT_RUN(test_106_exit_hook_drains_both_side_ledgers);
	UT_RUN(test_107_odd_snapshot_is_bounded_closed_without_debt);
	UT_RUN(test_108_nonregistered_feature_is_closed_without_debt);
	UT_RUN(test_109_lmon_without_validated_majority_remains_closed);
	UT_RUN(test_109a_lmon_publishes_source_open_only_after_majority_legacy_zero);
	UT_RUN(test_109b_lmon_legacy_zero_requires_coherent_admitted_membership);
	UT_RUN(test_109c_lmon_legacy_zero_rejects_membership_epoch_drift);
	UT_RUN(test_110_lmon_odd_writer_remains_fail_closed);
	UT_RUN(test_111_formation_change_closes_before_debt_drain);
	UT_RUN(test_111a_close_source_publishes_closed_before_debt_result);
	UT_RUN(test_112_enter_samples_second_snapshot_before_epoch);
	UT_RUN(test_113_recheck_samples_snapshot_before_epoch);
	UT_RUN(test_114_peer_open_matcher_stays_closed_until_d13_ack_table);
	UT_RUN(test_115_peer_open_matcher_rejects_invalid_inputs_before_capability_match);
	UT_RUN(test_116_formation_lmon_consumes_phase3_handoff);
	UT_DONE();
	return ut_failed_count == 0 ? 0 : 1;
}
