/*-------------------------------------------------------------------------
 *
 * test_cluster_r4_activation_fsm.c
 *	  Exact closed-transition, ACK and dormant-admission tests for R4 D13.
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "cluster/cluster_semantic_activation.h"

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

#define DEFINE_FAILURE_TEST(test_name, state_value, expected_revert)                               \
	UT_TEST(test_name)                                                                             \
	{                                                                                              \
		SemanticActivationFailurePolicy policy;                                                    \
		memset(&policy, 0, sizeof(policy));                                                        \
		UT_ASSERT(semantic_activation_failure_policy((state_value), &policy));                     \
		UT_ASSERT_EQ(policy.target, SEMANTIC_ACTIVATION_STATE_SOURCE_OPEN);                        \
		UT_ASSERT(policy.admission_closed_until_source_open);                                      \
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

DEFINE_INVALID_SELF_EDGE_TEST(test_43_source_open_has_no_self_edge,
							  SEMANTIC_ACTIVATION_STATE_SOURCE_OPEN)
DEFINE_INVALID_SELF_EDGE_TEST(test_44_admission_stopped_has_no_self_edge,
							  SEMANTIC_ACTIVATION_STATE_SOURCE_ADMISSION_STOPPED)
DEFINE_INVALID_SELF_EDGE_TEST(test_45_drain_has_no_self_edge,
							  SEMANTIC_ACTIVATION_STATE_SOURCE_DRAIN_OR_RECOVERY)
DEFINE_INVALID_SELF_EDGE_TEST(test_46_logical_zero_has_no_self_edge,
							  SEMANTIC_ACTIVATION_STATE_SOURCE_LOGICAL_ZERO)
DEFINE_INVALID_SELF_EDGE_TEST(test_47_transport_barrier_has_no_self_edge,
							  SEMANTIC_ACTIVATION_STATE_ORDERED_TRANSPORT_BARRIER)
DEFINE_INVALID_SELF_EDGE_TEST(test_48_transport_zero_has_no_self_edge,
							  SEMANTIC_ACTIVATION_STATE_TRANSPORT_BACKED_ZERO)
DEFINE_INVALID_SELF_EDGE_TEST(test_49_epoch_barrier_has_no_self_edge,
							  SEMANTIC_ACTIVATION_STATE_EPOCH_CAPABILITY_BARRIER)
DEFINE_INVALID_SELF_EDGE_TEST(test_50_target_staged_has_no_self_edge,
							  SEMANTIC_ACTIVATION_STATE_TARGET_STAGED)
DEFINE_INVALID_SELF_EDGE_TEST(test_51_committed_closed_has_no_self_edge,
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
					SEMANTIC_ACTIVATION_STATE_SOURCE_OPEN, false)
DEFINE_FAILURE_TEST(test_56_failure_after_admission_stop_restores_source,
					SEMANTIC_ACTIVATION_STATE_SOURCE_ADMISSION_STOPPED, false)
DEFINE_FAILURE_TEST(test_57_failure_during_drain_restores_source,
					SEMANTIC_ACTIVATION_STATE_SOURCE_DRAIN_OR_RECOVERY, false)
DEFINE_FAILURE_TEST(test_58_failure_after_logical_zero_restores_source,
					SEMANTIC_ACTIVATION_STATE_SOURCE_LOGICAL_ZERO, false)
DEFINE_FAILURE_TEST(test_59_failure_during_ordered_barrier_restores_source,
					SEMANTIC_ACTIVATION_STATE_ORDERED_TRANSPORT_BARRIER, false)
DEFINE_FAILURE_TEST(test_60_failure_after_transport_zero_restores_source,
					SEMANTIC_ACTIVATION_STATE_TRANSPORT_BACKED_ZERO, false)
DEFINE_FAILURE_TEST(test_61_failure_at_epoch_barrier_restores_source,
					SEMANTIC_ACTIVATION_STATE_EPOCH_CAPABILITY_BARRIER, false)
DEFINE_FAILURE_TEST(test_62_failure_after_prepare_restores_source,
					SEMANTIC_ACTIVATION_STATE_TARGET_STAGED, false)
DEFINE_FAILURE_TEST(test_63_failure_after_commit_requires_revert_closed,
					SEMANTIC_ACTIVATION_STATE_TARGET_COMMITTED_CLOSED, true)
DEFINE_FAILURE_TEST(test_64_failure_before_open_never_leaves_half_open,
					SEMANTIC_ACTIVATION_STATE_TARGET_OPEN, true)

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

	memset(&token, 0xa5, sizeof(token));
	UT_ASSERT_EQ(cluster_semantic_activation_enter(CLUSTER_SEMANTIC_FEATURE_R4_SYNC_CR_V1,
												   CLUSTER_SEMANTIC_TARGET_SIDE, &token),
				 CLUSTER_SEMANTIC_ADMISSION_TARGET_DISABLED);
	UT_ASSERT(!token.entered);
}

UT_TEST(test_96_source_token_recheck_and_leave_are_generation_scoped)
{
	ClusterSemanticAdmissionToken token;

	memset(&token, 0, sizeof(token));
	UT_ASSERT_EQ(cluster_semantic_activation_enter(CLUSTER_SEMANTIC_FEATURE_R4_SYNC_CR_V1,
												   CLUSTER_SEMANTIC_SOURCE_SIDE, &token),
				 CLUSTER_SEMANTIC_ADMISSION_OK);
	UT_ASSERT(token.entered);
	UT_ASSERT(cluster_semantic_activation_recheck(&token));
	cluster_semantic_activation_leave(&token);
	UT_ASSERT(!token.entered);
}

int
main(void)
{
	UT_PLAN(96);
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
	UT_RUN(test_64_failure_before_open_never_leaves_half_open);
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
	UT_DONE();
	return ut_failed_count == 0 ? 0 : 1;
}
