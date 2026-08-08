/*-------------------------------------------------------------------------
 *
 * cluster_semantic_activation.c
 *	  Shared two-stage semantic activation framework.
 *
 * This first dependency-light body is deliberately fail-closed.  It gives
 * the exact frozen public types and the real R4A readiness adapter an
 * executable home before any durable, LMON, IC, parser, or positive
 * activation integration is admitted.
 *
 * Portions Copyright (c) 1996-2024, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2026, pgrac contributors
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "cluster/cluster_semantic_activation.h"
#include "cluster/storage/cluster_undo_block0.h"

#define CLUSTER_SEMANTIC_RECORD_MAGIC UINT32_C(0x50475341)
#define CLUSTER_SEMANTIC_RECORD_VERSION 1
#define CLUSTER_SEMANTIC_RECORD_HEADER_LEN 104
#define CLUSTER_SEMANTIC_RECORD_CRC_OFFSET 96

typedef struct ClusterSemanticRecordSample {
	bool readable;
	uint8 bytes[CLUSTER_SEMANTIC_ACTIVATION_RECORD_BYTES];
} ClusterSemanticRecordSample;

typedef enum SemanticActivationState {
	SEMANTIC_ACTIVATION_STATE_INVALID = -1,
	SEMANTIC_ACTIVATION_STATE_SOURCE_OPEN = 0,
	SEMANTIC_ACTIVATION_STATE_SOURCE_ADMISSION_STOPPED = 1,
	SEMANTIC_ACTIVATION_STATE_SOURCE_DRAIN_OR_RECOVERY = 2,
	SEMANTIC_ACTIVATION_STATE_SOURCE_LOGICAL_ZERO = 3,
	SEMANTIC_ACTIVATION_STATE_ORDERED_TRANSPORT_BARRIER = 4,
	SEMANTIC_ACTIVATION_STATE_TRANSPORT_BACKED_ZERO = 5,
	SEMANTIC_ACTIVATION_STATE_EPOCH_CAPABILITY_BARRIER = 6,
	SEMANTIC_ACTIVATION_STATE_TARGET_STAGED = 7,
	SEMANTIC_ACTIVATION_STATE_TARGET_COMMITTED_CLOSED = 8,
	SEMANTIC_ACTIVATION_STATE_TARGET_OPEN = 9
} SemanticActivationState;

typedef enum SemanticActivationCallbackKind {
	SEMANTIC_ACTIVATION_CALLBACK_NONE = 0,
	SEMANTIC_ACTIVATION_CALLBACK_CLOSE_SOURCE,
	SEMANTIC_ACTIVATION_CALLBACK_LOGICAL_ZERO,
	SEMANTIC_ACTIVATION_CALLBACK_TRANSPORT_BARRIER,
	SEMANTIC_ACTIVATION_CALLBACK_TRANSPORT_ZERO,
	SEMANTIC_ACTIVATION_CALLBACK_EPOCH_CAPABILITY_BARRIER,
	SEMANTIC_ACTIVATION_CALLBACK_PREPARE_TARGET,
	SEMANTIC_ACTIVATION_CALLBACK_APPLY_TARGET_CLOSED,
	SEMANTIC_ACTIVATION_CALLBACK_OPEN_TARGET
} SemanticActivationCallbackKind;

typedef struct SemanticActivationFailurePolicy {
	SemanticActivationState target;
	bool admission_closed_until_source_open;
	bool revert_source_closed;
} SemanticActivationFailurePolicy;

typedef struct SemanticActivationAckTuple {
	uint32 node_id;
	uint64 boot_id;
	uint64 admitted_incarnation;
	uint64 control_connection_generation;
	uint32 capability_word;
	uint64 capability_generation;
	uint64 transition_epoch;
	uint64 record_generation;
} SemanticActivationAckTuple;

typedef enum SemanticActivationHeldLocks {
	SEMANTIC_ACTIVATION_HELD_NONE = 0,
	SEMANTIC_ACTIVATION_HELD_RESOURCE = UINT32_C(1),
	SEMANTIC_ACTIVATION_HELD_BUFFER = UINT32_C(2),
	SEMANTIC_ACTIVATION_HELD_SLRU = UINT32_C(4),
	SEMANTIC_ACTIVATION_HELD_UNDO_IO = UINT32_C(8),
	SEMANTIC_ACTIVATION_HELD_IC_DISPATCH = UINT32_C(16),
	SEMANTIC_ACTIVATION_HELD_ALL_FORBIDDEN = UINT32_C(31)
} SemanticActivationHeldLocks;

typedef enum SemanticActivationWaitEdge {
	SEMANTIC_ACTIVATION_WAIT_UTILITY_TO_LMON = 0,
	SEMANTIC_ACTIVATION_WAIT_LMON_TO_QVOTEC = 1,
	SEMANTIC_ACTIVATION_WAIT_LMON_TO_PEER_ACK = 2,
	SEMANTIC_ACTIVATION_WAIT_LMON_TO_CONTROL_BARRIER = 3
} SemanticActivationWaitEdge;

typedef enum SemanticActivationActor {
	SEMANTIC_ACTIVATION_ACTOR_PROCESS_UTILITY = 0,
	SEMANTIC_ACTIVATION_ACTOR_LMON,
	SEMANTIC_ACTIVATION_ACTOR_QVOTEC,
	SEMANTIC_ACTIVATION_ACTOR_LMS,
	SEMANTIC_ACTIVATION_ACTOR_DATA
} SemanticActivationActor;

typedef enum SemanticActivationEffect {
	SEMANTIC_ACTIVATION_EFFECT_NONE = 0,
	SEMANTIC_ACTIVATION_EFFECT_REQUEST_PUBLICATION = UINT32_C(1),
	SEMANTIC_ACTIVATION_EFFECT_PGSA_WRITE = UINT32_C(2),
	SEMANTIC_ACTIVATION_EFFECT_SOURCE_CLOSE = UINT32_C(4),
	SEMANTIC_ACTIVATION_EFFECT_TARGET_OPEN = UINT32_C(8),
	SEMANTIC_ACTIVATION_EFFECT_ACK_MUTATION = UINT32_C(16),
	SEMANTIC_ACTIVATION_EFFECT_CONTROL_WIRE = UINT32_C(32),
	SEMANTIC_ACTIVATION_EFFECT_DATA_WIRE = UINT32_C(64)
} SemanticActivationEffect;

static ClusterSemanticActivationResult
r4_pre_prepare_readiness(uint64 expected_generation, ClusterSemanticActivationRefusal *refusal);

static ClusterSemanticActivationResult
semantic_activation_select_majority(const ClusterSemanticRecordSample *samples, uint32 n_samples,
									uint8 selected[CLUSTER_SEMANTIC_ACTIVATION_RECORD_BYTES],
									bool *implicit_open)
{
	if (selected != NULL)
		memset(selected, 0, CLUSTER_SEMANTIC_ACTIVATION_RECORD_BYTES);
	if (implicit_open != NULL)
		*implicit_open = false;
	return CLUSTER_SEMANTIC_ACTIVATION_QUORUM_HOLD;
}

static bool
semantic_activation_fsm_next(SemanticActivationState current, bool reverse,
							 SemanticActivationState *next)
{
	return false;
}

static SemanticActivationCallbackKind
semantic_activation_callback_for_state(SemanticActivationState state)
{
	return SEMANTIC_ACTIVATION_CALLBACK_NONE;
}

static bool
semantic_activation_source_target_exclusive(bool source_open, bool target_open)
{
	return false;
}

static bool
semantic_activation_failure_policy(SemanticActivationState state,
								   SemanticActivationFailurePolicy *policy)
{
	return false;
}

static bool
semantic_activation_ack_matches(const SemanticActivationAckTuple *observed,
								const SemanticActivationAckTuple *expected)
{
	return false;
}

static ClusterSemanticAdmissionResult
semantic_activation_admission_policy(uint64 feature_bit, uint64 active_bits, bool transition_closed,
									 ClusterSemanticAdmissionSide side, uint64 expected_generation,
									 uint64 current_generation)
{
	return CLUSTER_SEMANTIC_ADMISSION_CLOSED;
}

static ClusterSemanticActivationResult
semantic_activation_preflight(ClusterSemanticActivationAction action, uint64 expected_generation,
							  ClusterSemanticActivationRefusal *refusal, uint32 *effects)
{
	ClusterSemanticActivationResult result;

	if (effects != NULL)
		*effects = SEMANTIC_ACTIVATION_EFFECT_NONE;
	if (action < CLUSTER_SEMANTIC_ENABLE_ALL || action > CLUSTER_SEMANTIC_ROLLBACK_ABORT) {
		if (refusal != NULL) {
			refusal->result = CLUSTER_SEMANTIC_ACTIVATION_BAD_STATE;
			refusal->feature_bit = 0;
			refusal->expected_generation = expected_generation;
		}
		return CLUSTER_SEMANTIC_ACTIVATION_BAD_STATE;
	}

	result = r4_pre_prepare_readiness(expected_generation, refusal);
	return result;
}

static bool
semantic_activation_control_wait_allowed(SemanticActivationWaitEdge edge, uint32 held_locks)
{
	return false;
}

static bool
semantic_activation_actor_edge_allowed(SemanticActivationActor from, SemanticActivationActor to)
{
	return false;
}

static ClusterSemanticActivationResult
r4_pre_prepare_readiness(uint64 expected_generation, ClusterSemanticActivationRefusal *refusal)
{
	ClusterR4PrerequisiteSnapshot snapshot = cluster_undo_block0_r4_prerequisite_snapshot();

	if (snapshot.ready)
		return CLUSTER_SEMANTIC_ACTIVATION_OK;
	if (refusal != NULL) {
		refusal->result = CLUSTER_SEMANTIC_ACTIVATION_RF_DEFERRED;
		refusal->feature_bit = CLUSTER_SEMANTIC_FEATURE_R4_SYNC_CR_V1;
		refusal->expected_generation = expected_generation;
	}
	return CLUSTER_SEMANTIC_ACTIVATION_RF_DEFERRED;
}

static ClusterSemanticActivationResult
r4_stage_fail_closed(uint64 generation)
{
	return CLUSTER_SEMANTIC_ACTIVATION_RF_DEFERRED;
}

static ClusterSemanticActivationResult
r4_zero_fail_closed(uint64 generation, ClusterSemanticZeroProof *proof)
{
	return CLUSTER_SEMANTIC_ACTIVATION_RF_DEFERRED;
}

static const ClusterSemanticActivationDescriptor r4_descriptor = {
	.name = "R4_SYNC_CR_V1",
	.feature_bit = CLUSTER_SEMANTIC_FEATURE_R4_SYNC_CR_V1,
	.required_hello_caps
	= PGRAC_IC_HELLO_CAP_SEMANTIC_ACTIVATION_V1 | PGRAC_IC_HELLO_CAP_R4_SYNC_CR_V1,
	.required_active_bits = 0,
	.source_available = true,
	.pre_prepare_readiness = r4_pre_prepare_readiness,
	.close_source_admission = r4_stage_fail_closed,
	.source_logical_debt_zero = r4_zero_fail_closed,
	.source_transport_zero = r4_zero_fail_closed,
	.prepare_target = r4_stage_fail_closed,
	.apply_target_closed = r4_stage_fail_closed,
	.revert_source_closed = r4_stage_fail_closed,
	.open_target_admission = r4_stage_fail_closed,
};

ClusterSemanticAdmissionResult
cluster_semantic_activation_enter(uint64 feature_bit, ClusterSemanticAdmissionSide side,
								  ClusterSemanticAdmissionToken *token)
{
	if (token != NULL)
		memset(token, 0, sizeof(*token));
	return CLUSTER_SEMANTIC_ADMISSION_CLOSED;
}

bool
cluster_semantic_activation_recheck(const ClusterSemanticAdmissionToken *token)
{
	return false;
}

void
cluster_semantic_activation_leave(ClusterSemanticAdmissionToken *token)
{
	if (token != NULL)
		token->entered = false;
}

Size
cluster_semantic_activation_shmem_size(void)
{
	return 0;
}

void
cluster_semantic_activation_shmem_init(void)
{}

void
cluster_semantic_activation_register(const ClusterSemanticActivationDescriptor *descriptor)
{}

bool
cluster_semantic_activation_record_encode(const ClusterSemanticActivationRecord *record,
										  uint8 bytes[512])
{
	return false;
}

bool
cluster_semantic_activation_record_decode(const uint8 bytes[512],
										  ClusterSemanticActivationRecord *record,
										  ClusterSemanticActivationRefusal *refusal)
{
	return false;
}

void
cluster_semantic_activation_lmon_tick(void)
{}

ClusterSemanticActivationResult
cluster_semantic_activation_submit(ClusterSemanticActivationAction action,
								   ClusterSemanticActivationRefusal *refusal)
{
	uint32 effects;

	return semantic_activation_preflight(action, 0, refusal, &effects);
}

void
ExecAlterSystemRacTwoStage(AlterSystemRacTwoStageStmt *stmt)
{
	ClusterSemanticActivationRefusal refusal;

	if (stmt != NULL)
		(void)cluster_semantic_activation_submit(stmt->action, &refusal);
}

const ClusterSemanticActivationDescriptor *
cluster_semantic_activation_r4_descriptor(void)
{
	return &r4_descriptor;
}
