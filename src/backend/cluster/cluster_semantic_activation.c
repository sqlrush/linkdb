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
#include "port/pg_crc32c.h"

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

/*
 * B2 is deliberately deployed with the source semantics open and the target
 * semantics dormant.  The later shared-memory/control-plane integration owns
 * changing this snapshot; the dependency-light core only consumes it.
 */
static uint64 semantic_activation_active_bits = 0;
static uint64 semantic_activation_record_generation = 0;
static bool semantic_activation_transition_closed = false;

static uint16
semantic_activation_read_u16_le(const uint8 *bytes)
{
	return (uint16)bytes[0] | ((uint16)bytes[1] << 8);
}

static uint32
semantic_activation_read_u32_le(const uint8 *bytes)
{
	return (uint32)bytes[0] | ((uint32)bytes[1] << 8) | ((uint32)bytes[2] << 16)
		   | ((uint32)bytes[3] << 24);
}

static uint64
semantic_activation_read_u64_le(const uint8 *bytes)
{
	uint64 value = 0;
	int i;

	for (i = 7; i >= 0; i--)
		value = (value << 8) | bytes[i];
	return value;
}

static void
semantic_activation_write_u16_le(uint8 *bytes, uint16 value)
{
	bytes[0] = (uint8)value;
	bytes[1] = (uint8)(value >> 8);
}

static void
semantic_activation_write_u32_le(uint8 *bytes, uint32 value)
{
	bytes[0] = (uint8)value;
	bytes[1] = (uint8)(value >> 8);
	bytes[2] = (uint8)(value >> 16);
	bytes[3] = (uint8)(value >> 24);
}

static void
semantic_activation_write_u64_le(uint8 *bytes, uint64 value)
{
	int i;

	for (i = 0; i < 8; i++) {
		bytes[i] = (uint8)value;
		value >>= 8;
	}
}

static bool
semantic_activation_bytes_are_zero(const uint8 *bytes, Size len)
{
	Size i;

	for (i = 0; i < len; i++) {
		if (bytes[i] != 0)
			return false;
	}
	return true;
}

static void
semantic_activation_set_refusal(ClusterSemanticActivationRefusal *refusal,
								ClusterSemanticActivationResult result, uint64 feature_bit,
								uint64 expected_generation)
{
	if (refusal == NULL)
		return;

	refusal->result = result;
	refusal->feature_bit = feature_bit;
	refusal->expected_generation = expected_generation;
}

static ClusterSemanticActivationResult
r4_pre_prepare_readiness(uint64 expected_generation, ClusterSemanticActivationRefusal *refusal);

static ClusterSemanticActivationResult
semantic_activation_select_majority(const ClusterSemanticRecordSample *samples, uint32 n_samples,
									uint8 selected[CLUSTER_SEMANTIC_ACTIVATION_RECORD_BYTES],
									bool *implicit_open)
{
	uint32 majority;
	uint32 zero_count = 0;
	uint32 i;

	if (selected != NULL)
		memset(selected, 0, CLUSTER_SEMANTIC_ACTIVATION_RECORD_BYTES);
	if (implicit_open != NULL)
		*implicit_open = false;
	if (samples == NULL || n_samples == 0 || selected == NULL)
		return CLUSTER_SEMANTIC_ACTIVATION_QUORUM_HOLD;

	majority = n_samples / 2 + 1;
	for (i = 0; i < n_samples; i++) {
		if (samples[i].readable
			&& semantic_activation_bytes_are_zero(samples[i].bytes,
												  CLUSTER_SEMANTIC_ACTIVATION_RECORD_BYTES))
			zero_count++;
	}
	if (zero_count >= majority) {
		if (implicit_open != NULL)
			*implicit_open = true;
		return CLUSTER_SEMANTIC_ACTIVATION_OK;
	}

	/* An exact valid byte image, rather than merely a generation, must win. */
	for (i = 0; i < n_samples; i++) {
		ClusterSemanticActivationRecord record;
		uint32 identical = 0;
		uint32 j;

		if (!samples[i].readable
			|| semantic_activation_bytes_are_zero(samples[i].bytes,
												  CLUSTER_SEMANTIC_ACTIVATION_RECORD_BYTES)
			|| !cluster_semantic_activation_record_decode(samples[i].bytes, &record, NULL))
			continue;

		for (j = 0; j < n_samples; j++) {
			if (samples[j].readable
				&& memcmp(samples[i].bytes, samples[j].bytes,
						  CLUSTER_SEMANTIC_ACTIVATION_RECORD_BYTES)
					   == 0)
				identical++;
		}
		if (identical >= majority) {
			memcpy(selected, samples[i].bytes, CLUSTER_SEMANTIC_ACTIVATION_RECORD_BYTES);
			return CLUSTER_SEMANTIC_ACTIVATION_OK;
		}
	}

	/* A split record at one generation is never reclassified as legacy zero. */
	for (i = 0; i < n_samples; i++) {
		ClusterSemanticActivationRecord left;
		uint32 j;

		if (!samples[i].readable
			|| semantic_activation_bytes_are_zero(samples[i].bytes,
												  CLUSTER_SEMANTIC_ACTIVATION_RECORD_BYTES)
			|| !cluster_semantic_activation_record_decode(samples[i].bytes, &left, NULL))
			continue;
		for (j = i + 1; j < n_samples; j++) {
			ClusterSemanticActivationRecord right;

			if (!samples[j].readable
				|| semantic_activation_bytes_are_zero(samples[j].bytes,
													  CLUSTER_SEMANTIC_ACTIVATION_RECORD_BYTES)
				|| !cluster_semantic_activation_record_decode(samples[j].bytes, &right, NULL))
				continue;
			if (left.record_generation == right.record_generation
				&& memcmp(samples[i].bytes, samples[j].bytes,
						  CLUSTER_SEMANTIC_ACTIVATION_RECORD_BYTES)
					   != 0)
				return CLUSTER_SEMANTIC_ACTIVATION_RECORD_CONFLICT;
		}
	}

	return CLUSTER_SEMANTIC_ACTIVATION_QUORUM_HOLD;
}

static bool
semantic_activation_fsm_next(SemanticActivationState current, bool reverse,
							 SemanticActivationState *next)
{
	(void)reverse;
	if (next == NULL || current < SEMANTIC_ACTIVATION_STATE_SOURCE_OPEN
		|| current >= SEMANTIC_ACTIVATION_STATE_TARGET_OPEN)
		return false;

	*next = (SemanticActivationState)(current + 1);
	return true;
}

static SemanticActivationCallbackKind
semantic_activation_callback_for_state(SemanticActivationState state)
{
	switch (state) {
	case SEMANTIC_ACTIVATION_STATE_SOURCE_ADMISSION_STOPPED:
		return SEMANTIC_ACTIVATION_CALLBACK_CLOSE_SOURCE;
	case SEMANTIC_ACTIVATION_STATE_SOURCE_LOGICAL_ZERO:
		return SEMANTIC_ACTIVATION_CALLBACK_LOGICAL_ZERO;
	case SEMANTIC_ACTIVATION_STATE_ORDERED_TRANSPORT_BARRIER:
		return SEMANTIC_ACTIVATION_CALLBACK_TRANSPORT_BARRIER;
	case SEMANTIC_ACTIVATION_STATE_TRANSPORT_BACKED_ZERO:
		return SEMANTIC_ACTIVATION_CALLBACK_TRANSPORT_ZERO;
	case SEMANTIC_ACTIVATION_STATE_EPOCH_CAPABILITY_BARRIER:
		return SEMANTIC_ACTIVATION_CALLBACK_EPOCH_CAPABILITY_BARRIER;
	case SEMANTIC_ACTIVATION_STATE_TARGET_STAGED:
		return SEMANTIC_ACTIVATION_CALLBACK_PREPARE_TARGET;
	case SEMANTIC_ACTIVATION_STATE_TARGET_COMMITTED_CLOSED:
		return SEMANTIC_ACTIVATION_CALLBACK_APPLY_TARGET_CLOSED;
	case SEMANTIC_ACTIVATION_STATE_TARGET_OPEN:
		return SEMANTIC_ACTIVATION_CALLBACK_OPEN_TARGET;
	default:
		return SEMANTIC_ACTIVATION_CALLBACK_NONE;
	}
}

static bool
semantic_activation_source_target_exclusive(bool source_open, bool target_open)
{
	return !(source_open && target_open);
}

static bool
semantic_activation_failure_policy(SemanticActivationState state,
								   SemanticActivationFailurePolicy *policy)
{
	if (policy == NULL || state < SEMANTIC_ACTIVATION_STATE_SOURCE_OPEN
		|| state > SEMANTIC_ACTIVATION_STATE_TARGET_COMMITTED_CLOSED)
		return false;

	policy->target = SEMANTIC_ACTIVATION_STATE_SOURCE_OPEN;
	policy->admission_closed_until_source_open = state != SEMANTIC_ACTIVATION_STATE_SOURCE_OPEN;
	policy->revert_source_closed = state == SEMANTIC_ACTIVATION_STATE_TARGET_COMMITTED_CLOSED;
	return true;
}

static bool
semantic_activation_ack_matches(const SemanticActivationAckTuple *observed,
								const SemanticActivationAckTuple *expected)
{
	return observed != NULL && expected != NULL && observed->node_id == expected->node_id
		   && observed->boot_id == expected->boot_id
		   && observed->admitted_incarnation == expected->admitted_incarnation
		   && observed->control_connection_generation == expected->control_connection_generation
		   && observed->capability_word == expected->capability_word
		   && observed->capability_generation == expected->capability_generation
		   && observed->transition_epoch == expected->transition_epoch
		   && observed->record_generation == expected->record_generation;
}

static ClusterSemanticAdmissionResult
semantic_activation_admission_policy(uint64 feature_bit, uint64 active_bits, bool transition_closed,
									 ClusterSemanticAdmissionSide side, uint64 expected_generation,
									 uint64 current_generation)
{
	bool active;

	if (feature_bit == 0
		|| (side != CLUSTER_SEMANTIC_SOURCE_SIDE && side != CLUSTER_SEMANTIC_TARGET_SIDE))
		return CLUSTER_SEMANTIC_ADMISSION_CLOSED;
	if (expected_generation != current_generation)
		return CLUSTER_SEMANTIC_ADMISSION_GENERATION_CHANGED;
	if (transition_closed)
		return CLUSTER_SEMANTIC_ADMISSION_CLOSED;

	active = (active_bits & feature_bit) != 0;
	if (side == CLUSTER_SEMANTIC_SOURCE_SIDE)
		return active ? CLUSTER_SEMANTIC_ADMISSION_SOURCE_DORMANT : CLUSTER_SEMANTIC_ADMISSION_OK;
	return active ? CLUSTER_SEMANTIC_ADMISSION_OK : CLUSTER_SEMANTIC_ADMISSION_TARGET_DISABLED;
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
	return edge >= SEMANTIC_ACTIVATION_WAIT_UTILITY_TO_LMON
		   && edge <= SEMANTIC_ACTIVATION_WAIT_LMON_TO_CONTROL_BARRIER
		   && held_locks == SEMANTIC_ACTIVATION_HELD_NONE;
}

static bool
semantic_activation_actor_edge_allowed(SemanticActivationActor from, SemanticActivationActor to)
{
	return (from == SEMANTIC_ACTIVATION_ACTOR_PROCESS_UTILITY
			&& to == SEMANTIC_ACTIVATION_ACTOR_LMON)
		   || (from == SEMANTIC_ACTIVATION_ACTOR_LMON && to == SEMANTIC_ACTIVATION_ACTOR_QVOTEC);
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
	(void)generation;
	return CLUSTER_SEMANTIC_ACTIVATION_RF_DEFERRED;
}

static ClusterSemanticActivationResult
r4_zero_fail_closed(uint64 generation, ClusterSemanticZeroProof *proof)
{
	(void)generation;
	(void)proof;
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
	ClusterSemanticAdmissionResult result;

	if (token != NULL)
		memset(token, 0, sizeof(*token));
	if (token == NULL)
		return CLUSTER_SEMANTIC_ADMISSION_CLOSED;

	result = semantic_activation_admission_policy(
		feature_bit, semantic_activation_active_bits, semantic_activation_transition_closed, side,
		semantic_activation_record_generation, semantic_activation_record_generation);
	if (result == CLUSTER_SEMANTIC_ADMISSION_OK) {
		token->feature_bit = feature_bit;
		token->record_generation = semantic_activation_record_generation;
		token->side = (uint8)side;
		token->entered = true;
	}
	return result;
}

bool
cluster_semantic_activation_recheck(const ClusterSemanticAdmissionToken *token)
{
	if (token == NULL || !token->entered)
		return false;

	return semantic_activation_admission_policy(
			   token->feature_bit, semantic_activation_active_bits,
			   semantic_activation_transition_closed, (ClusterSemanticAdmissionSide)token->side,
			   token->record_generation, semantic_activation_record_generation)
		   == CLUSTER_SEMANTIC_ADMISSION_OK;
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
	uint8 encoded[CLUSTER_SEMANTIC_ACTIVATION_RECORD_BYTES];
	pg_crc32c crc;

	if (record == NULL || bytes == NULL || record->record_generation == 0
		|| record->phase < CLUSTER_SEMANTIC_PHASE_PREPARE
		|| record->phase > CLUSTER_SEMANTIC_PHASE_ROLLBACK_COMPLETE
		|| (record->phase != CLUSTER_SEMANTIC_PHASE_ROLLBACK_COMPLETE
			&& record->rollback_feature_bitmap != 0))
		return false;

	memset(encoded, 0, sizeof(encoded));
	semantic_activation_write_u32_le(encoded, CLUSTER_SEMANTIC_RECORD_MAGIC);
	semantic_activation_write_u16_le(encoded + 4, CLUSTER_SEMANTIC_RECORD_VERSION);
	semantic_activation_write_u16_le(encoded + 6, CLUSTER_SEMANTIC_RECORD_HEADER_LEN);
	semantic_activation_write_u64_le(encoded + 8, record->record_generation);
	encoded[16] = (uint8)record->phase;
	semantic_activation_write_u64_le(encoded + 24, record->source_feature_bitmap);
	semantic_activation_write_u64_le(encoded + 32, record->target_feature_bitmap);
	semantic_activation_write_u64_le(encoded + 40, record->transition_epoch);
	semantic_activation_write_u32_le(encoded + 48, record->coordinator_node);
	semantic_activation_write_u64_le(encoded + 56, record->coordinator_incarnation);
	semantic_activation_write_u64_le(encoded + 64, record->admitted_members_lo);
	semantic_activation_write_u64_le(encoded + 72, record->admitted_members_hi);
	semantic_activation_write_u64_le(encoded + 80, record->capability_sample_digest);
	semantic_activation_write_u64_le(encoded + 88, record->rollback_feature_bitmap);

	INIT_CRC32C(crc);
	COMP_CRC32C(crc, encoded, CLUSTER_SEMANTIC_RECORD_CRC_OFFSET);
	FIN_CRC32C(crc);
	semantic_activation_write_u32_le(encoded + CLUSTER_SEMANTIC_RECORD_CRC_OFFSET, (uint32)crc);

	memcpy(bytes, encoded, sizeof(encoded));
	return true;
}

bool
cluster_semantic_activation_record_decode(const uint8 bytes[512],
										  ClusterSemanticActivationRecord *record,
										  ClusterSemanticActivationRefusal *refusal)
{
	ClusterSemanticActivationRecord decoded;
	uint32 stored_crc;
	pg_crc32c crc;

	if (bytes == NULL || record == NULL) {
		semantic_activation_set_refusal(refusal, CLUSTER_SEMANTIC_ACTIVATION_BAD_STATE, 0, 0);
		return false;
	}

	memset(&decoded, 0, sizeof(decoded));
	if (semantic_activation_read_u32_le(bytes) != CLUSTER_SEMANTIC_RECORD_MAGIC
		|| semantic_activation_read_u16_le(bytes + 4) != CLUSTER_SEMANTIC_RECORD_VERSION
		|| semantic_activation_read_u16_le(bytes + 6) != CLUSTER_SEMANTIC_RECORD_HEADER_LEN
		|| semantic_activation_read_u64_le(bytes + 8) == 0
		|| bytes[16] < CLUSTER_SEMANTIC_PHASE_PREPARE
		|| bytes[16] > CLUSTER_SEMANTIC_PHASE_ROLLBACK_COMPLETE
		|| !semantic_activation_bytes_are_zero(bytes + 17, 7)
		|| !semantic_activation_bytes_are_zero(bytes + 52, 4)
		|| !semantic_activation_bytes_are_zero(bytes + 100,
											   CLUSTER_SEMANTIC_ACTIVATION_RECORD_BYTES - 100)) {
		semantic_activation_set_refusal(refusal, CLUSTER_SEMANTIC_ACTIVATION_BAD_STATE, 0,
										semantic_activation_read_u64_le(bytes + 8));
		return false;
	}

	stored_crc = semantic_activation_read_u32_le(bytes + CLUSTER_SEMANTIC_RECORD_CRC_OFFSET);
	INIT_CRC32C(crc);
	COMP_CRC32C(crc, bytes, CLUSTER_SEMANTIC_RECORD_CRC_OFFSET);
	FIN_CRC32C(crc);
	if (stored_crc != (uint32)crc) {
		semantic_activation_set_refusal(refusal, CLUSTER_SEMANTIC_ACTIVATION_BAD_STATE, 0,
										semantic_activation_read_u64_le(bytes + 8));
		return false;
	}

	decoded.record_generation = semantic_activation_read_u64_le(bytes + 8);
	decoded.phase = (ClusterSemanticActivationPhase)bytes[16];
	decoded.source_feature_bitmap = semantic_activation_read_u64_le(bytes + 24);
	decoded.target_feature_bitmap = semantic_activation_read_u64_le(bytes + 32);
	decoded.transition_epoch = semantic_activation_read_u64_le(bytes + 40);
	decoded.coordinator_node = semantic_activation_read_u32_le(bytes + 48);
	decoded.coordinator_incarnation = semantic_activation_read_u64_le(bytes + 56);
	decoded.admitted_members_lo = semantic_activation_read_u64_le(bytes + 64);
	decoded.admitted_members_hi = semantic_activation_read_u64_le(bytes + 72);
	decoded.capability_sample_digest = semantic_activation_read_u64_le(bytes + 80);
	decoded.rollback_feature_bitmap = semantic_activation_read_u64_le(bytes + 88);
	if (decoded.phase != CLUSTER_SEMANTIC_PHASE_ROLLBACK_COMPLETE
		&& decoded.rollback_feature_bitmap != 0) {
		semantic_activation_set_refusal(refusal, CLUSTER_SEMANTIC_ACTIVATION_BAD_STATE, 0,
										decoded.record_generation);
		return false;
	}

	*record = decoded;
	semantic_activation_set_refusal(refusal, CLUSTER_SEMANTIC_ACTIVATION_OK, 0,
									decoded.record_generation);
	return true;
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
