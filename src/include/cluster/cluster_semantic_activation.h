/*-------------------------------------------------------------------------
 *
 * cluster_semantic_activation.h
 *	  Shared two-stage semantic activation framework.
 *
 * Portions Copyright (c) 1996-2024, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2026, pgrac contributors
 *
 *-------------------------------------------------------------------------
 */
#ifndef CLUSTER_SEMANTIC_ACTIVATION_H
#define CLUSTER_SEMANTIC_ACTIVATION_H

#include "c.h"
#include "nodes/nodes.h"

#define PGRAC_IC_HELLO_CAP_SEMANTIC_ACTIVATION_V1 UINT32_C(0x00001000)
#define PGRAC_IC_HELLO_CAP_R4_SYNC_CR_V1 UINT32_C(0x00002000)
#define CLUSTER_SEMANTIC_FEATURE_R4_SYNC_CR_V1 (UINT64_C(1) << 0)
#define CLUSTER_SEMANTIC_ACTIVATION_RECORD_BYTES 512

typedef enum ClusterSemanticAdmissionSide {
	CLUSTER_SEMANTIC_SOURCE_SIDE = 0,
	CLUSTER_SEMANTIC_TARGET_SIDE = 1
} ClusterSemanticAdmissionSide;

typedef enum ClusterSemanticAdmissionResult {
	CLUSTER_SEMANTIC_ADMISSION_OK = 0,
	CLUSTER_SEMANTIC_ADMISSION_SOURCE_DORMANT = 1,
	CLUSTER_SEMANTIC_ADMISSION_TARGET_DISABLED = 2,
	CLUSTER_SEMANTIC_ADMISSION_GENERATION_CHANGED = 3,
	CLUSTER_SEMANTIC_ADMISSION_CLOSED = 4
} ClusterSemanticAdmissionResult;

typedef struct ClusterSemanticAdmissionToken {
	uint64 feature_bit;
	uint64 record_generation;
	uint8 side;
	bool entered;
} ClusterSemanticAdmissionToken;

typedef enum ClusterSemanticActivationAction {
	CLUSTER_SEMANTIC_ENABLE_ALL = 0,
	CLUSTER_SEMANTIC_DISABLE_ALL = 1,
	CLUSTER_SEMANTIC_ROLLBACK_ALL = 2,
	CLUSTER_SEMANTIC_ROLLBACK_ABORT = 3
} ClusterSemanticActivationAction;

typedef enum ClusterSemanticActivationPhase {
	CLUSTER_SEMANTIC_PHASE_NONE = 0,
	CLUSTER_SEMANTIC_PHASE_PREPARE = 1,
	CLUSTER_SEMANTIC_PHASE_COMMIT = 2,
	CLUSTER_SEMANTIC_PHASE_OPEN = 3,
	CLUSTER_SEMANTIC_PHASE_ROLLBACK_COMPLETE = 4
} ClusterSemanticActivationPhase;

typedef enum ClusterSemanticActivationResult {
	CLUSTER_SEMANTIC_ACTIVATION_OK = 0,
	CLUSTER_SEMANTIC_ACTIVATION_TARGET_DISABLED = 1,
	CLUSTER_SEMANTIC_ACTIVATION_SOURCE_DORMANT = 2,
	CLUSTER_SEMANTIC_ACTIVATION_RF_DEFERRED = 3,
	CLUSTER_SEMANTIC_ACTIVATION_HETEROGENEOUS = 4,
	CLUSTER_SEMANTIC_ACTIVATION_MEMBERSHIP_CHANGED = 5,
	CLUSTER_SEMANTIC_ACTIVATION_CAPABILITY_CHANGED = 6,
	CLUSTER_SEMANTIC_ACTIVATION_DEBT_NONZERO = 7,
	CLUSTER_SEMANTIC_ACTIVATION_TRANSPORT_NONZERO = 8,
	CLUSTER_SEMANTIC_ACTIVATION_QUORUM_HOLD = 9,
	CLUSTER_SEMANTIC_ACTIVATION_RECORD_CONFLICT = 10,
	CLUSTER_SEMANTIC_ACTIVATION_PARTIAL_APPLY = 11,
	CLUSTER_SEMANTIC_ACTIVATION_BAD_STATE = 12
} ClusterSemanticActivationResult;

typedef struct ClusterSemanticActivationRefusal {
	ClusterSemanticActivationResult result;
	uint64 feature_bit;
	uint64 expected_generation;
} ClusterSemanticActivationRefusal;

typedef struct ClusterSemanticZeroProof {
	uint64 record_generation;
	uint64 debt_count;
	uint64 sample_digest;
} ClusterSemanticZeroProof;

typedef struct ClusterSemanticActivationRecord {
	uint64 source_feature_bitmap;
	uint64 target_feature_bitmap;
	uint64 transition_epoch;
	uint64 record_generation;
	uint64 admitted_members_lo;
	uint64 admitted_members_hi;
	uint64 capability_sample_digest;
	uint64 rollback_feature_bitmap;
	uint64 coordinator_incarnation;
	uint32 coordinator_node;
	ClusterSemanticActivationPhase phase;
} ClusterSemanticActivationRecord;

typedef ClusterSemanticActivationResult (*ClusterSemanticReadinessCallback)(
	uint64 expected_generation, ClusterSemanticActivationRefusal *refusal);
typedef ClusterSemanticActivationResult (*ClusterSemanticStageCallback)(uint64 generation);
typedef ClusterSemanticActivationResult (*ClusterSemanticZeroCallback)(
	uint64 generation, ClusterSemanticZeroProof *proof);

typedef struct ClusterSemanticActivationDescriptor {
	const char *name;
	uint64 feature_bit;
	uint32 required_hello_caps;
	uint64 required_active_bits;
	bool source_available;
	ClusterSemanticReadinessCallback pre_prepare_readiness;
	ClusterSemanticStageCallback close_source_admission;
	ClusterSemanticZeroCallback source_logical_debt_zero;
	ClusterSemanticZeroCallback source_transport_zero;
	ClusterSemanticStageCallback prepare_target;
	ClusterSemanticStageCallback apply_target_closed;
	ClusterSemanticStageCallback revert_source_closed;
	ClusterSemanticStageCallback open_target_admission;
} ClusterSemanticActivationDescriptor;

typedef struct AlterSystemRacTwoStageStmt {
	NodeTag type;
	ClusterSemanticActivationAction action;
} AlterSystemRacTwoStageStmt;

extern ClusterSemanticAdmissionResult
cluster_semantic_activation_enter(uint64 feature_bit, ClusterSemanticAdmissionSide side,
								  ClusterSemanticAdmissionToken *token);
extern bool cluster_semantic_activation_recheck(const ClusterSemanticAdmissionToken *token);
extern void cluster_semantic_activation_leave(ClusterSemanticAdmissionToken *token);
extern Size cluster_semantic_activation_shmem_size(void);
extern void cluster_semantic_activation_shmem_init(void);
extern void
cluster_semantic_activation_register(const ClusterSemanticActivationDescriptor *descriptor);
extern bool cluster_semantic_activation_record_encode(const ClusterSemanticActivationRecord *record,
													  uint8 bytes[512]);
extern bool cluster_semantic_activation_record_decode(const uint8 bytes[512],
													  ClusterSemanticActivationRecord *record,
													  ClusterSemanticActivationRefusal *refusal);
extern ClusterSemanticActivationResult
cluster_semantic_activation_record_cas_write(uint64 expected_generation, const uint8 bytes[512]);
extern void cluster_semantic_activation_lmon_tick(void);
extern ClusterSemanticActivationResult
cluster_semantic_activation_submit(ClusterSemanticActivationAction action,
								   ClusterSemanticActivationRefusal *refusal);
extern void ExecAlterSystemRacTwoStage(AlterSystemRacTwoStageStmt *stmt);
extern const ClusterSemanticActivationDescriptor *cluster_semantic_activation_r4_descriptor(void);

#endif /* CLUSTER_SEMANTIC_ACTIVATION_H */
