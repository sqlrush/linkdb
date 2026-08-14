/*-------------------------------------------------------------------------
 *
 * cluster_recovery_duty.c
 *	  No-generation failed-origin recovery-duty identity (RF-ROOT P2).
 *
 * The full STOP-01 root identity is the sole durable duty key.  Formation
 * freshness and execution authority remain separate STOP-02/STOP-03 proofs;
 * this module deliberately creates no scalar generation or durable object.
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "cluster/cluster_recovery_duty.h"
#include "cluster/cluster_wal_thread.h"
#include "common/cryptohash.h"
#include "common/sha2.h"
#include "portability/instr_time.h"

static void
write_u16_le(uint8 *dst, uint16 value)
{
	dst[0] = (uint8)value;
	dst[1] = (uint8)(value >> 8);
}

static void
write_u32_le(uint8 *dst, uint32 value)
{
	dst[0] = (uint8)value;
	dst[1] = (uint8)(value >> 8);
	dst[2] = (uint8)(value >> 16);
	dst[3] = (uint8)(value >> 24);
}

static void
write_u64_le(uint8 *dst, uint64 value)
{
	int i;

	for (i = 0; i < 8; i++) {
		dst[i] = (uint8)value;
		value >>= 8;
	}
}

bool
cluster_recovery_duty_key_encode_v1(
	const ClusterRecoveryDutyKey *key,
	uint8 out[CLUSTER_RECOVERY_DUTY_KEY_V1_BYTES])
{
	if (out == NULL)
		return false;
	memset(out, 0, CLUSTER_RECOVERY_DUTY_KEY_V1_BYTES);
	if (!cluster_recovery_duty_key_valid_v1(key))
		return false;
	write_u64_le(out, key->system_identifier);
	memcpy(out + 8, key->storage_uuid, 16);
	memcpy(out + 24, key->authority_uuid, 16);
	write_u16_le(out + 40, key->origin_thread_id);
	write_u32_le(out + 42, (uint32)key->origin_node_id);
	write_u64_le(out + 46, (uint64)key->thread_claim_created_at);
	write_u32_le(out + 54, key->thread_claim_crc32c);
	write_u64_le(out + 58, key->origin_owner_incarnation);
	write_u64_le(out + 66, key->root_lineage_seq);
	return true;
}

ClusterRecoveryDutyCompare
cluster_recovery_duty_key_compare(const ClusterRecoveryDutyKey *expected,
								  const ClusterRecoveryDutyKey *observed)
{
	uint8 expected_bytes[CLUSTER_RECOVERY_DUTY_KEY_V1_BYTES];
	uint8 observed_bytes[CLUSTER_RECOVERY_DUTY_KEY_V1_BYTES];

	if (!cluster_recovery_duty_key_encode_v1(expected, expected_bytes)
		|| !cluster_recovery_duty_key_encode_v1(observed, observed_bytes))
		return CLUSTER_RECOVERY_DUTY_COMPARE_INVALID;
	return memcmp(expected_bytes, observed_bytes, sizeof(expected_bytes)) == 0
			   ? CLUSTER_RECOVERY_DUTY_COMPARE_EXACT
			   : CLUSTER_RECOVERY_DUTY_COMPARE_DIFFERENT;
}

bool
cluster_recovery_duty_digest_v1(const ClusterRecoveryDutyKey *key,
								ClusterRecoveryDutyDigest *out)
{
	static const uint8 domain[19] = "PGRAC-ROOT-DUTY-V1";
	uint8 preimage[19 + 4 + CLUSTER_RECOVERY_DUTY_KEY_V1_BYTES];
	pg_cryptohash_ctx *ctx;
	bool success = false;

	if (out == NULL)
		return false;
	memset(out, 0, sizeof(*out));
	memcpy(preimage, domain, sizeof(domain));
	write_u32_le(preimage + sizeof(domain), CLUSTER_RECOVERY_DUTY_KEY_V1_BYTES);
	if (!cluster_recovery_duty_key_encode_v1(
			key, preimage + sizeof(domain) + sizeof(uint32)))
		return false;
	ctx = pg_cryptohash_create(PG_SHA256);
	if (ctx == NULL)
		return false;
	if (pg_cryptohash_init(ctx) >= 0
		&& pg_cryptohash_update(ctx, preimage, sizeof(preimage)) >= 0
		&& pg_cryptohash_final(ctx, out->bytes, sizeof(out->bytes)) >= 0)
		success = true;
	pg_cryptohash_free(ctx);
	if (!success)
		memset(out, 0, sizeof(*out));
	return success;
}

static bool
owner_marker_absent(const ClusterJoinCommitMarker *marker)
{
	const uint8 *bytes = (const uint8 *)marker;
	size_t i;

	for (i = 0; i < sizeof(*marker); i++)
		if (bytes[i] != 0)
			return false;
	return true;
}

static bool
owner_marker_committed_valid(const ClusterJoinCommitMarker *marker, int32 node_id)
{
	pg_crc32c crc;

	if (marker->magic != CLUSTER_JCMK_MAGIC
		|| marker->version != CLUSTER_JCMK_VERSION
		|| marker->node_id != node_id
		|| marker->phase != CLUSTER_JCMK_PHASE_COMMITTED
		|| marker->admitted_incarnation == 0)
		return false;
	INIT_CRC32C(crc);
	COMP_CRC32C(crc, marker, offsetof(ClusterJoinCommitMarker, crc32c));
	FIN_CRC32C(crc);
	return (uint32)crc == marker->crc32c;
}

ClusterRecoveryOwnerImportResult
cluster_recovery_owner_import_select_v1(
	int32 node_id, const ClusterWalThreadClaim *immutable_claim,
	uint64 frozen_admitted_bitmap_low, uint64 frozen_admitted_bitmap_high,
	const ClusterRecoveryOwnerDiskSampleV1 *samples, int total_disk_count,
	uint64 *out_incarnation)
{
	ClusterJoinCommitMarker committed[CLUSTER_MAX_VOTING_DISKS];
	uint32 majority;
	int n_committed = 0;
	bool join_unreadable = false;
	bool join_present = false;
	bool initial_member;
	int i;

	if (out_incarnation != NULL)
		*out_incarnation = 0;
	if (out_incarnation == NULL || immutable_claim == NULL || samples == NULL
		|| node_id < 0 || node_id >= CLUSTER_MAX_NODES
		|| total_disk_count <= 0 || total_disk_count > CLUSTER_MAX_VOTING_DISKS)
		return CLUSTER_RECOVERY_OWNER_IMPORT_BAD_ARGUMENT;
	if (!cluster_wal_thread_claim_validate(
			immutable_claim, (uint16)(node_id + 1), node_id, NULL))
		return CLUSTER_RECOVERY_OWNER_IMPORT_CLAIM_MISMATCH;
	initial_member = node_id < 64
					 ? (frozen_admitted_bitmap_low & (UINT64_C(1) << node_id)) != 0
					 : (frozen_admitted_bitmap_high
						& (UINT64_C(1) << (node_id - 64))) != 0;
	majority = (uint32)(total_disk_count / 2) + 1;
	for (i = 0; i < total_disk_count; i++) {
		const ClusterJoinCommitMarker *marker = &samples[i].join_marker;

		if (samples[i].join_io_state != CLUSTER_VOTING_DISK_IO_OK) {
			join_unreadable = true;
			continue;
		}
		if (owner_marker_absent(marker))
			continue;
		join_present = true;
		if (owner_marker_committed_valid(marker, node_id))
			committed[n_committed++] = *marker;
	}
	if (join_present) {
		int a;

		for (a = 0; a < n_committed; a++) {
			uint32 same = 0;
			int b;

			for (b = 0; b < n_committed; b++)
				if (memcmp(&committed[a], &committed[b], sizeof(committed[a])) == 0)
					same++;
			if (same >= majority) {
				*out_incarnation = committed[a].admitted_incarnation;
				return CLUSTER_RECOVERY_OWNER_IMPORT_JCMK;
			}
		}
		return CLUSTER_RECOVERY_OWNER_IMPORT_JCMK_UNPROVEN;
	}
	if (join_unreadable)
		return CLUSTER_RECOVERY_OWNER_IMPORT_IO_FAILED;
	if (!initial_member)
		return CLUSTER_RECOVERY_OWNER_IMPORT_SLOT_UNPROVEN;
	for (i = 0; i < total_disk_count; i++) {
		uint32 same = 0;
		uint64 candidate;
		int j;

		if (samples[i].slot_io_state != CLUSTER_VOTING_DISK_IO_OK
			|| samples[i].slot.node_id != (uint32)node_id
			|| samples[i].slot.incarnation == 0)
			continue;
		candidate = samples[i].slot.incarnation;
		for (j = 0; j < total_disk_count; j++)
			if (samples[j].slot_io_state == CLUSTER_VOTING_DISK_IO_OK
				&& samples[j].slot.node_id == (uint32)node_id
				&& samples[j].slot.incarnation == candidate)
				same++;
		if (same >= majority) {
			*out_incarnation = candidate;
			return CLUSTER_RECOVERY_OWNER_IMPORT_VOTING_SLOT;
		}
	}
	return CLUSTER_RECOVERY_OWNER_IMPORT_SLOT_UNPROVEN;
}

bool
cluster_recovery_owner_rejoin_v1(int32 node_id, uint64 admitted_incarnation)
{
	ClusterControlRootIdentity identity;
	ClusterControlRootSnapshot snapshot;
	ClusterControlRootSnapshot published;
	ClusterControlRootReadToken token;
	ClusterControlRootReadToken published_token;
	ClusterControlRootPatch patch;
	ClusterControlRootResult root_result;
	ClusterRecoveryOwnerImportResult owner_result;
	ClusterWalThreadClaim immutable_claim;
	uint64 proven_incarnation = 0;

	if (node_id < 0 || node_id >= CLUSTER_MAX_NODES
		|| admitted_incarnation == 0)
		return false;
	root_result = cluster_control_root_lookup_owner_by_node_runtime(
		node_id, &identity, &snapshot, &token);
	if ((root_result != CLUSTER_CONTROL_ROOT_OK_PRIMARY
		 && root_result != CLUSTER_CONTROL_ROOT_OK_PRIMARY_DEGRADED)
		|| !cluster_recovery_duty_key_valid_v1(&identity)
		|| cluster_recovery_duty_key_compare(&identity, &snapshot.identity)
			   != CLUSTER_RECOVERY_DUTY_COMPARE_EXACT
		|| (snapshot.lifecycle
				!= CLUSTER_CONTROL_ROOT_LIFECYCLE_RECOVERY_COMPLETE
			&& snapshot.lifecycle != CLUSTER_CONTROL_ROOT_LIFECYCLE_OPEN)
		|| (snapshot.lifecycle == CLUSTER_CONTROL_ROOT_LIFECYCLE_OPEN
			&& identity.origin_owner_incarnation != admitted_incarnation)
		|| (snapshot.lifecycle
				== CLUSTER_CONTROL_ROOT_LIFECYCLE_RECOVERY_COMPLETE
			&& (identity.root_lineage_seq == UINT64_MAX
				|| identity.origin_owner_incarnation >= admitted_incarnation)))
		return false;
	cluster_wal_thread_claim_fill(
		&immutable_claim, identity.origin_thread_id, identity.origin_node_id,
		identity.thread_claim_created_at);
	if (immutable_claim.crc != identity.thread_claim_crc32c)
		return false;
	owner_result = cluster_recovery_owner_import_read_v1(
		node_id, &immutable_claim, 0, 0, &proven_incarnation);
	if (owner_result != CLUSTER_RECOVERY_OWNER_IMPORT_JCMK
		|| proven_incarnation != admitted_incarnation)
		return false;
	/* A previous attempt may have completed the single ROOT CAS and then lost
	 * the local membership publish race.  Exact OPEN+owner plus the same direct
	 * majority JCMK is the already-satisfied gate; never advance lineage twice. */
	if (snapshot.lifecycle == CLUSTER_CONTROL_ROOT_LIFECYCLE_OPEN)
		return true;

	memset(&patch, 0, sizeof(patch));
	patch.mask = CLUSTER_CONTROL_ROOT_PATCH_LIFECYCLE
				 | CLUSTER_CONTROL_ROOT_PATCH_OWNER_LINEAGE
				 | CLUSTER_CONTROL_ROOT_PATCH_CHECKPOINT
				 | CLUSTER_CONTROL_ROOT_PATCH_TAIL
				 | CLUSTER_CONTROL_ROOT_PATCH_RECOVERY_PROGRESS;
	patch.expected_lifecycle =
		CLUSTER_CONTROL_ROOT_LIFECYCLE_RECOVERY_COMPLETE;
	patch.desired.lifecycle = CLUSTER_CONTROL_ROOT_LIFECYCLE_OPEN;
	patch.desired.identity.origin_owner_incarnation = admitted_incarnation;
	patch.desired.identity.root_lineage_seq = identity.root_lineage_seq + 1;
	patch.desired.root_flags = snapshot.root_flags;
	patch.desired.checkpoint_tli = snapshot.checkpoint_tli;
	patch.desired.checkpoint_source_kind = snapshot.checkpoint_source_kind;
	patch.desired.checkpoint_lower_lsn = snapshot.checkpoint_lower_lsn;
	patch.desired.checkpoint_record_crc32c = snapshot.checkpoint_record_crc32c;
	patch.desired.tail_tli = snapshot.tail_tli;
	patch.desired.tail_validation_kind = snapshot.tail_validation_kind;
	patch.desired.validated_tail_lsn_exclusive =
		snapshot.validated_tail_lsn_exclusive;
	patch.desired.tail_last_record_lsn = snapshot.tail_last_record_lsn;
	patch.desired.tail_last_record_crc32c = snapshot.tail_last_record_crc32c;
	patch.desired.recovered_tli = snapshot.recovered_tli;
	patch.desired.recovered_through_lsn_exclusive =
		snapshot.recovered_through_lsn_exclusive;
	patch.desired.recovered_last_record_lsn = snapshot.recovered_last_record_lsn;
	patch.desired.recovered_last_record_crc32c =
		snapshot.recovered_last_record_crc32c;

	root_result = cluster_control_root_compare_and_publish(
		&token, &patch, CLUSTER_CONTROL_ROOT_PUBLISH_OWNER_REJOIN,
		&published, &published_token);
	return root_result == CLUSTER_CONTROL_ROOT_OK_PRIMARY
		   && published.lifecycle == CLUSTER_CONTROL_ROOT_LIFECYCLE_OPEN
		   && published.identity.origin_owner_incarnation == admitted_incarnation
		   && published.identity.root_lineage_seq == identity.root_lineage_seq + 1;
}

static bool
formation_bitmap_has_node(const uint8 bitmap[CLUSTER_RECONFIG_DEAD_BITMAP_BYTES], int32 node_id)
{
	return node_id >= 0 && node_id < CLUSTER_MAX_NODES
		   && (bitmap[node_id / 8] & (uint8)(1u << (node_id % 8))) != 0;
}

static bool
formation_bitmap_nonempty(const uint8 bitmap[CLUSTER_RECONFIG_DEAD_BITMAP_BYTES])
{
	int i;

	for (i = 0; i < CLUSTER_RECONFIG_DEAD_BITMAP_BYTES; i++)
		if (bitmap[i] != 0)
			return true;
	return false;
}

static void
formation_expected_marker(const ClusterFormationSnapshotV1 *snapshot,
						  ClusterFenceMarker *expected)
{
	memset(expected, 0, sizeof(*expected));
	expected->magic = CLUSTER_FENCE_MARKER_MAGIC;
	expected->version = CLUSTER_FENCE_MARKER_VERSION;
	expected->fence_epoch = snapshot->applied.new_epoch;
	expected->fence_event_id = snapshot->applied.event_id;
	expected->fence_generation = snapshot->applied.cssd_dead_generation;
	expected->issuer_node_id = snapshot->applied.event_id == 0
								   ? CLUSTER_FENCE_BASELINE_INITIAL_ISSUER
								   : snapshot->applied.coordinator_node_id;
	memcpy(expected->fenced_dead_bitmap, snapshot->excluded_bitmap,
		   sizeof(expected->fenced_dead_bitmap));
	expected->marker_kind = snapshot->applied.event_id == 0
							? CLUSTER_FENCE_MARKER_KIND_BASELINE
							: CLUSTER_FENCE_MARKER_KIND_FENCE;
}

ClusterFormationWitnessResult
cluster_formation_witness_decide_v1(const ClusterFormationSnapshotV1 *f1,
									const ClusterFenceAuthorityProof *authority,
									const ClusterFormationSnapshotV1 *f2,
									uint16 origin_thread, bool opening_new_duty)
{
	ClusterFenceMarker expected;
	int32 origin_node;
	int survivor_count = 0;
	int i;

	if (f1 == NULL || authority == NULL || f2 == NULL || origin_thread == 0
		|| origin_thread > CLUSTER_MAX_NODES)
		return CLUSTER_FORMATION_WITNESS_BAD_ARGUMENT;
	if (memcmp(f1, f2, sizeof(*f1)) != 0)
		return CLUSTER_FORMATION_WITNESS_UNSTABLE;
	if (f2->prebump_sync_active != 0 || !f2->self_join_admitted || f2->self_join_failed
		|| formation_bitmap_nonempty(f2->pending_join_bitmap)
		|| f2->applied.reconfig_kind == RECONFIG_KIND_JOIN_PENDING)
		return CLUSTER_FORMATION_WITNESS_UNSTABLE;
	if (f2->local_epoch != f2->applied.new_epoch)
		return CLUSTER_FORMATION_WITNESS_UNSTABLE;
	if (authority->total_disk_count == 0
		|| authority->agree_disk_count <= authority->total_disk_count / 2
		|| !cluster_fence_marker_valid_v1(&authority->marker))
		return CLUSTER_FORMATION_WITNESS_MARKER_UNPROVEN;
	formation_expected_marker(f2, &expected);
	if (!cluster_fence_marker_valid_v1(&expected)
		|| !cluster_fence_marker_tuple_equal(&authority->marker, &expected))
		return CLUSTER_FORMATION_WITNESS_MARKER_UNPROVEN;

	origin_node = (int32)origin_thread - 1;
	if (!formation_bitmap_has_node(f2->excluded_bitmap, origin_node)
		|| (opening_new_duty
			&& !formation_bitmap_has_node(f2->applied.dead_bitmap, origin_node)))
		return CLUSTER_FORMATION_WITNESS_ORIGIN_NOT_EXCLUDED;
	if (f2->victim_incarnation == 0
		|| f2->victim_incarnation
			   != f2->membership.last_admitted_incarnation[origin_node]
		|| (f2->membership.membership_state[origin_node] != CLUSTER_MEMBER_DEAD
			&& f2->membership.membership_state[origin_node] != CLUSTER_MEMBER_REMOVED))
		return CLUSTER_FORMATION_WITNESS_OWNER_MISMATCH;

	for (i = 0; i < CLUSTER_MAX_NODES; i++)
		if (f2->membership.membership_state[i] == CLUSTER_MEMBER_MEMBER
			&& !formation_bitmap_has_node(f2->excluded_bitmap, i))
			survivor_count++;
	if (survivor_count == 0)
		return CLUSTER_FORMATION_WITNESS_FULL_OUTAGE_UNRECOVERED;
	return CLUSTER_FORMATION_WITNESS_READY;
}

#define CLUSTER_FORMATION_WITNESS_MAGIC UINT32_C(0x46575631) /* FWV1 */

struct ClusterFormationWitnessV1 {
	uint32 magic;
	uint16 origin_thread;
	bool opening_new_duty;
	uint8 reserved;
	ClusterFenceAuthorityProof authority;
	ClusterFormationSnapshotV1 f1;
	ClusterFormationSnapshotV1 f2;
};

static uint64
formation_monotonic_us(void)
{
	instr_time now;
	int64 ns;

	INSTR_TIME_SET_CURRENT(now);
	ns = INSTR_TIME_GET_NANOSEC(now);
	return ns <= 0 ? 0 : (uint64)ns / UINT64_C(1000);
}

static ClusterFormationWitnessResult
formation_authority_result(ClusterFenceAuthorityReadResult result)
{
	switch (result) {
		case CLUSTER_FENCE_AUTHORITY_OK:
			return CLUSTER_FORMATION_WITNESS_READY;
		case CLUSTER_FENCE_AUTHORITY_NO_MAJORITY:
			return CLUSTER_FORMATION_WITNESS_MARKER_UNPROVEN;
		case CLUSTER_FENCE_AUTHORITY_IO_UNAVAILABLE:
			return CLUSTER_FORMATION_WITNESS_IO_FAILED;
		case CLUSTER_FENCE_AUTHORITY_ENFORCEMENT_OFF:
		case CLUSTER_FENCE_AUTHORITY_NO_CONFIG:
			return CLUSTER_FORMATION_WITNESS_CAPABILITY_UNAVAILABLE;
		case CLUSTER_FENCE_AUTHORITY_BAD_ARGUMENT:
		case CLUSTER_FENCE_AUTHORITY_BAD_CONFIG:
		case CLUSTER_FENCE_AUTHORITY_MIXED_VERSION:
		case CLUSTER_FENCE_AUTHORITY_CORRUPT:
		default:
			return CLUSTER_FORMATION_WITNESS_CORRUPT;
	}
}

ClusterFormationWitnessResult
cluster_formation_witness_build_wait(uint16 origin_thread, bool opening_new_duty,
									 int timeout_ms, ClusterFormationWitnessV1 **out)
{
	ClusterFormationWitnessResult last = CLUSTER_FORMATION_WITNESS_UNSTABLE;
	uint64 start_us;
	uint64 deadline_us;

	if (out == NULL || *out != NULL || origin_thread == 0 || origin_thread > CLUSTER_MAX_NODES
		|| timeout_ms < 1 || timeout_ms > 600000)
		return CLUSTER_FORMATION_WITNESS_BAD_ARGUMENT;
	start_us = formation_monotonic_us();
	if (start_us == 0 || start_us > UINT64_MAX - (uint64)timeout_ms * UINT64_C(1000))
		return CLUSTER_FORMATION_WITNESS_CAPABILITY_UNAVAILABLE;
	deadline_us = start_us + (uint64)timeout_ms * UINT64_C(1000);
	for (;;) {
		ClusterFormationSnapshotV1 f1;
		ClusterFormationSnapshotV1 f2;
		ClusterFenceAuthorityProof authority;
		ClusterFenceAuthorityReadResult read_result;
		uint64 proof_sequence;
		uint64 now_us;

		proof_sequence = cluster_write_fence_authority_cache_sequence();
		if ((proof_sequence & UINT64_C(1)) != 0
			|| proof_sequence >= UINT64_MAX - 1) {
			last = CLUSTER_FORMATION_WITNESS_UNSTABLE;
			goto retry;
		}
		if (!cluster_reconfig_capture_formation_snapshot_v1(origin_thread, &f1))
			return CLUSTER_FORMATION_WITNESS_CAPABILITY_UNAVAILABLE;
		read_result = cluster_write_fence_read_durable_authority(&authority);
		last = formation_authority_result(read_result);
		if (last == CLUSTER_FORMATION_WITNESS_READY) {
			if (!cluster_reconfig_capture_formation_snapshot_v1(origin_thread, &f2))
				return CLUSTER_FORMATION_WITNESS_CAPABILITY_UNAVAILABLE;
			last = cluster_formation_witness_decide_v1(
				&f1, &authority, &f2, origin_thread, opening_new_duty);
			if (last == CLUSTER_FORMATION_WITNESS_READY) {
				ClusterFormationWitnessV1 *witness;

				now_us = formation_monotonic_us();
				if (now_us == 0)
					return CLUSTER_FORMATION_WITNESS_CAPABILITY_UNAVAILABLE;
				if (!cluster_write_fence_authority_cache_publish_if_unchanged(
						&authority.marker, now_us, proof_sequence)) {
					last = CLUSTER_FORMATION_WITNESS_UNSTABLE;
					goto retry;
				}
				witness = (ClusterFormationWitnessV1 *)palloc(sizeof(*witness));
				memset(witness, 0, sizeof(*witness));
				witness->magic = CLUSTER_FORMATION_WITNESS_MAGIC;
				witness->origin_thread = origin_thread;
				witness->opening_new_duty = opening_new_duty;
				witness->authority = authority;
				witness->f1 = f1;
				witness->f2 = f2;
				*out = witness;
				return CLUSTER_FORMATION_WITNESS_READY;
			}
		}
		if (last != CLUSTER_FORMATION_WITNESS_UNSTABLE
			&& last != CLUSTER_FORMATION_WITNESS_MARKER_UNPROVEN
			&& last != CLUSTER_FORMATION_WITNESS_IO_FAILED)
			return last;
	retry:
		now_us = formation_monotonic_us();
		if (now_us == 0 || now_us >= deadline_us)
			return last;
		pg_usleep(1000L);
	}
}

ClusterFormationWitnessResult
cluster_formation_witness_revalidate_nowait(const ClusterFormationWitnessV1 *witness)
{
	ClusterFenceAuthorityCacheResult result;
	uint64 now_us;

	if (witness == NULL || witness->magic != CLUSTER_FORMATION_WITNESS_MAGIC)
		return CLUSTER_FORMATION_WITNESS_BAD_ARGUMENT;
	now_us = formation_monotonic_us();
	if (now_us == 0)
		return CLUSTER_FORMATION_WITNESS_CAPABILITY_UNAVAILABLE;
	result = cluster_write_fence_revalidate_cached_nowait(&witness->authority.marker, now_us);
	switch (result) {
		case CLUSTER_FENCE_CACHE_MATCH:
			return CLUSTER_FORMATION_WITNESS_READY;
		case CLUSTER_FENCE_CACHE_STALE:
		case CLUSTER_FENCE_CACHE_EXPIRED:
			return CLUSTER_FORMATION_WITNESS_UNSTABLE;
		case CLUSTER_FENCE_CACHE_INVALID:
		case CLUSTER_FENCE_CACHE_UNAVAILABLE:
		default:
			return CLUSTER_FORMATION_WITNESS_CAPABILITY_UNAVAILABLE;
	}
}

const ClusterFenceAuthorityProof *
cluster_formation_witness_authority(const ClusterFormationWitnessV1 *witness)
{
	if (witness == NULL || witness->magic != CLUSTER_FORMATION_WITNESS_MAGIC)
		return NULL;
	return &witness->authority;
}

void
cluster_formation_witness_destroy(ClusterFormationWitnessV1 **witness)
{
	if (witness == NULL || *witness == NULL)
		return;
	(*witness)->magic = 0;
	pfree(*witness);
	*witness = NULL;
}
