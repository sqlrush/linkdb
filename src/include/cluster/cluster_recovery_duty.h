/*-------------------------------------------------------------------------
 *
 * cluster_recovery_duty.h
 *	  No-generation failed-origin recovery-duty identity (RF-ROOT P2).
 *
 *-------------------------------------------------------------------------
 */
#ifndef CLUSTER_RECOVERY_DUTY_H
#define CLUSTER_RECOVERY_DUTY_H

#include "cluster/cluster_control_root.h"
#include "cluster/cluster_reconfig.h"
#include "cluster/cluster_wal_thread.h"
#include "cluster/cluster_write_fence.h"

#define CLUSTER_RECOVERY_DUTY_KEY_V1_BYTES 74
#define CLUSTER_RECOVERY_DUTY_DIGEST_BYTES 32

typedef struct ClusterRecoveryDutyDigest {
	uint8 bytes[CLUSTER_RECOVERY_DUTY_DIGEST_BYTES];
} ClusterRecoveryDutyDigest;

typedef enum ClusterRecoveryDutyCompare {
	CLUSTER_RECOVERY_DUTY_COMPARE_EXACT = 0,
	CLUSTER_RECOVERY_DUTY_COMPARE_DIFFERENT = 1,
	CLUSTER_RECOVERY_DUTY_COMPARE_INVALID = 2
} ClusterRecoveryDutyCompare;

typedef enum ClusterRecoveryOwnerImportResult {
	CLUSTER_RECOVERY_OWNER_IMPORT_JCMK = 0,
	CLUSTER_RECOVERY_OWNER_IMPORT_VOTING_SLOT = 1,
	CLUSTER_RECOVERY_OWNER_IMPORT_BAD_ARGUMENT = 2,
	CLUSTER_RECOVERY_OWNER_IMPORT_CLAIM_MISMATCH = 3,
	CLUSTER_RECOVERY_OWNER_IMPORT_JCMK_UNPROVEN = 4,
	CLUSTER_RECOVERY_OWNER_IMPORT_SLOT_UNPROVEN = 5,
	CLUSTER_RECOVERY_OWNER_IMPORT_IO_FAILED = 6,
	CLUSTER_RECOVERY_OWNER_IMPORT_CAPABILITY_UNAVAILABLE = 7,
	CLUSTER_RECOVERY_OWNER_IMPORT_BAD_CONFIG = 8
} ClusterRecoveryOwnerImportResult;

typedef struct ClusterRecoveryOwnerDiskSampleV1 {
	ClusterVotingDiskIoState join_io_state;
	ClusterJoinCommitMarker join_marker;
	ClusterVotingDiskIoState slot_io_state;
	ClusterVotingSlot slot;
} ClusterRecoveryOwnerDiskSampleV1;

typedef struct ClusterFormationWitnessV1 ClusterFormationWitnessV1;

typedef enum ClusterFormationWitnessResult {
	CLUSTER_FORMATION_WITNESS_READY = 0,
	CLUSTER_FORMATION_WITNESS_BAD_ARGUMENT = 1,
	CLUSTER_FORMATION_WITNESS_UNSTABLE = 2,
	CLUSTER_FORMATION_WITNESS_MARKER_UNPROVEN = 3,
	CLUSTER_FORMATION_WITNESS_ORIGIN_NOT_EXCLUDED = 4,
	CLUSTER_FORMATION_WITNESS_OWNER_MISMATCH = 5,
	CLUSTER_FORMATION_WITNESS_FULL_OUTAGE_UNRECOVERED = 6,
	CLUSTER_FORMATION_WITNESS_CAPABILITY_UNAVAILABLE = 7,
	CLUSTER_FORMATION_WITNESS_IO_FAILED = 8,
	CLUSTER_FORMATION_WITNESS_CORRUPT = 9
} ClusterFormationWitnessResult;

/* Internal canonical snapshot captured under the reconfig lock. */
typedef struct ClusterFormationSnapshotV1 {
	ReconfigEvent applied;
	ClusterMembershipTable membership;
	uint8 pending_join_bitmap[CLUSTER_RECONFIG_DEAD_BITMAP_BYTES];
	uint8 clean_departed_bitmap[CLUSTER_RECONFIG_DEAD_BITMAP_BYTES];
	uint8 removed_bitmap[CLUSTER_RECONFIG_DEAD_BITMAP_BYTES];
	uint8 excluded_bitmap[CLUSTER_RECONFIG_DEAD_BITMAP_BYTES];
	uint64 local_epoch;
	uint64 victim_incarnation;
	uint32 prebump_sync_active;
	uint8 self_join_admitted;
	uint8 self_join_failed;
	uint8 reserved[2];
} ClusterFormationSnapshotV1;

StaticAssertDecl(sizeof(ClusterRecoveryDutyDigest) == 32,
				 "ClusterRecoveryDutyDigest ABI");

extern bool cluster_recovery_duty_key_encode_v1(
	const ClusterRecoveryDutyKey *key,
	uint8 out[CLUSTER_RECOVERY_DUTY_KEY_V1_BYTES]);
extern ClusterRecoveryDutyCompare cluster_recovery_duty_key_compare(
	const ClusterRecoveryDutyKey *expected, const ClusterRecoveryDutyKey *observed);
extern bool cluster_recovery_duty_digest_v1(const ClusterRecoveryDutyKey *key,
									ClusterRecoveryDutyDigest *out);
extern ClusterRecoveryOwnerImportResult cluster_recovery_owner_import_select_v1(
	int32 node_id, const ClusterWalThreadClaim *immutable_claim,
	uint64 frozen_admitted_bitmap_low, uint64 frozen_admitted_bitmap_high,
	const ClusterRecoveryOwnerDiskSampleV1 *samples, int total_disk_count,
	uint64 *out_incarnation);
extern ClusterRecoveryOwnerImportResult cluster_recovery_owner_import_read_v1(
	int32 node_id, const ClusterWalThreadClaim *immutable_claim,
	uint64 frozen_admitted_bitmap_low, uint64 frozen_admitted_bitmap_high,
	uint64 *out_incarnation);
extern bool cluster_recovery_owner_rejoin_v1(int32 node_id,
									 uint64 admitted_incarnation);
extern ClusterFormationWitnessResult cluster_formation_witness_decide_v1(
	const ClusterFormationSnapshotV1 *f1, const ClusterFenceAuthorityProof *authority,
	const ClusterFormationSnapshotV1 *f2, uint16 origin_thread, bool opening_new_duty);
extern ClusterFormationWitnessResult cluster_formation_witness_build_wait(
	uint16 origin_thread, bool opening_new_duty, int timeout_ms,
	ClusterFormationWitnessV1 **out);
extern ClusterFormationWitnessResult cluster_formation_witness_revalidate_nowait(
	const ClusterFormationWitnessV1 *witness);
extern const ClusterFenceAuthorityProof *cluster_formation_witness_authority(
	const ClusterFormationWitnessV1 *witness);
extern void cluster_formation_witness_destroy(ClusterFormationWitnessV1 **witness);

#endif /* CLUSTER_RECOVERY_DUTY_H */
