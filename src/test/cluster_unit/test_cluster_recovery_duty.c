/*-------------------------------------------------------------------------
 *
 * test_cluster_recovery_duty.c
 *	  RF-ROOT P2 tests for the no-generation recovery-duty identity.
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include <stdlib.h>

#include "cluster/cluster_recovery_duty.h"
#include "cluster/cluster_wal_thread.h"
#include "common/cryptohash.h"
#include "common/sha2.h"

#undef printf
#undef fprintf
#undef snprintf
#undef sprintf
#undef vsnprintf
#undef vfprintf
#undef vprintf
#undef vsprintf

#include "unit_test.h"

UT_DEFINE_GLOBALS();

static ClusterControlRootResult ut_root_lookup_result;
static ClusterControlRootResult ut_root_publish_result;
static ClusterRecoveryOwnerImportResult ut_owner_read_result;
static ClusterControlRootIdentity ut_root_identity;
static ClusterControlRootSnapshot ut_root_snapshot;
static ClusterControlRootReadToken ut_root_token;
static uint64 ut_owner_read_incarnation;
static int ut_owner_read_calls;
static int ut_root_publish_calls;
static ClusterControlRootPatch ut_root_published_patch;

ClusterControlRootResult
cluster_control_root_lookup_owner_by_node_runtime(
	int32 old_node_id, ClusterControlRootIdentity *out_identity,
	ClusterControlRootSnapshot *out_snapshot, ClusterControlRootReadToken *out_token)
{
	(void)old_node_id;
	if (ut_root_lookup_result == CLUSTER_CONTROL_ROOT_OK_PRIMARY
		|| ut_root_lookup_result == CLUSTER_CONTROL_ROOT_OK_PRIMARY_DEGRADED) {
		*out_identity = ut_root_identity;
		*out_snapshot = ut_root_snapshot;
		*out_token = ut_root_token;
	}
	return ut_root_lookup_result;
}

ClusterRecoveryOwnerImportResult
cluster_recovery_owner_import_read_v1(
	int32 node_id, const ClusterWalThreadClaim *immutable_claim,
	uint64 frozen_admitted_bitmap_low, uint64 frozen_admitted_bitmap_high,
	uint64 *out_incarnation)
{
	(void)node_id;
	(void)immutable_claim;
	(void)frozen_admitted_bitmap_low;
	(void)frozen_admitted_bitmap_high;
	ut_owner_read_calls++;
	*out_incarnation = ut_owner_read_incarnation;
	return ut_owner_read_result;
}

ClusterControlRootResult
cluster_control_root_compare_and_publish(
	const ClusterControlRootReadToken *expected_token,
	const ClusterControlRootPatch *patch, ClusterControlRootPublishReason reason,
	ClusterControlRootSnapshot *out_snapshot, ClusterControlRootReadToken *out_token)
{
	(void)expected_token;
	(void)reason;
	ut_root_publish_calls++;
	ut_root_published_patch = *patch;
	if (ut_root_publish_result == CLUSTER_CONTROL_ROOT_OK_PRIMARY) {
		*out_snapshot = ut_root_snapshot;
		out_snapshot->lifecycle = patch->desired.lifecycle;
		out_snapshot->identity.origin_owner_incarnation =
			patch->desired.identity.origin_owner_incarnation;
		out_snapshot->identity.root_lineage_seq =
			patch->desired.identity.root_lineage_seq;
		memset(out_token, 0, sizeof(*out_token));
	}
	return ut_root_publish_result;
}

void *
palloc(Size size)
{
	return malloc(size);
}

void
pfree(void *pointer)
{
	free(pointer);
}

void
ExceptionalCondition(const char *conditionName, const char *fileName, int lineNumber)
{
	printf("# Assert failed: %s at %s:%d\n", conditionName, fileName, lineNumber);
	abort();
}

bool
cluster_reconfig_capture_formation_snapshot_v1(uint16 origin_thread,
											ClusterFormationSnapshotV1 *out)
{
	(void)origin_thread;
	(void)out;
	return false;
}

ClusterFenceAuthorityReadResult
cluster_write_fence_read_durable_authority(ClusterFenceAuthorityProof *out)
{
	(void)out;
	return CLUSTER_FENCE_AUTHORITY_NO_MAJORITY;
}

uint64
cluster_write_fence_authority_cache_sequence(void)
{
	return 0;
}

bool
cluster_write_fence_authority_cache_publish_if_unchanged(const ClusterFenceMarker *marker,
												 uint64 published_at_us,
												 uint64 expected_sequence)
{
	(void)marker;
	(void)published_at_us;
	(void)expected_sequence;
	return false;
}

ClusterFenceAuthorityCacheResult
cluster_write_fence_revalidate_cached_nowait(const ClusterFenceMarker *expected, uint64 now_us)
{
	(void)expected;
	(void)now_us;
	return CLUSTER_FENCE_CACHE_INVALID;
}

static void
put_u32_le(uint8 *dst, uint32 value)
{
	dst[0] = (uint8)value;
	dst[1] = (uint8)(value >> 8);
	dst[2] = (uint8)(value >> 16);
	dst[3] = (uint8)(value >> 24);
}

static void
put_u64_le(uint8 *dst, uint64 value)
{
	int i;

	for (i = 0; i < 8; i++) {
		dst[i] = (uint8)value;
		value >>= 8;
	}
}

static void
sha256_bytes(const uint8 *bytes, size_t len, uint8 out[PG_SHA256_DIGEST_LENGTH])
{
	pg_cryptohash_ctx *ctx = pg_cryptohash_create(PG_SHA256);

	if (ctx == NULL || pg_cryptohash_init(ctx) < 0
		|| pg_cryptohash_update(ctx, bytes, len) < 0
		|| pg_cryptohash_final(ctx, out, PG_SHA256_DIGEST_LENGTH) < 0)
		abort();
	pg_cryptohash_free(ctx);
}

static void
build_valid_key(ClusterRecoveryDutyKey *key)
{
	ClusterWalThreadClaim claim;
	int i;

	memset(key, 0, sizeof(*key));
	key->system_identifier = UINT64_C(0x0123456789abcdef);
	for (i = 0; i < 16; i++) {
		key->storage_uuid[i] = (uint8)(i + 1);
		key->authority_uuid[i] = (uint8)(0xa0 + i);
	}
	key->authority_uuid[6] = 0x46;
	key->authority_uuid[8] = 0x8a;
	key->origin_thread_id = 4;
	key->origin_node_id = 3;
	key->thread_claim_created_at = INT64_C(1700000000000123);
	cluster_wal_thread_claim_fill(&claim, key->origin_thread_id, key->origin_node_id,
								 key->thread_claim_created_at);
	key->thread_claim_crc32c = claim.crc;
	key->origin_owner_incarnation = UINT64_C(0x1122334455667788);
	key->root_lineage_seq = UINT64_C(9);
}

static void
setup_owner_rejoin(uint64 old_incarnation, uint64 new_incarnation)
{
	memset(&ut_root_identity, 0, sizeof(ut_root_identity));
	build_valid_key(&ut_root_identity);
	ut_root_snapshot = (ClusterControlRootSnapshot){ 0 };
	ut_root_snapshot.identity = ut_root_identity;
	ut_root_snapshot.identity.origin_owner_incarnation = old_incarnation;
	ut_root_identity = ut_root_snapshot.identity;
	ut_root_snapshot.lifecycle = CLUSTER_CONTROL_ROOT_LIFECYCLE_RECOVERY_COMPLETE;
	ut_root_snapshot.root_flags = CLUSTER_CONTROL_ROOT_FLAG_CLAIM_VALID
								 | CLUSTER_CONTROL_ROOT_FLAG_CHECKPOINT_VALID
								 | CLUSTER_CONTROL_ROOT_FLAG_TAIL_VALID
								 | CLUSTER_CONTROL_ROOT_FLAG_RECOVERED_VALID;
	ut_root_snapshot.checkpoint_tli = 1;
	ut_root_snapshot.tail_tli = 1;
	ut_root_snapshot.recovered_tli = 1;
	ut_root_snapshot.checkpoint_source_kind =
		CLUSTER_CONTROL_ROOT_CHECKPOINT_NATIVE_V1;
	ut_root_snapshot.tail_validation_kind =
		CLUSTER_CONTROL_ROOT_TAIL_WAL_RECORD_SCAN_V1;
	ut_root_snapshot.checkpoint_lower_lsn = UINT64_C(0x1000000);
	ut_root_snapshot.validated_tail_lsn_exclusive = UINT64_C(0x1100000);
	ut_root_snapshot.recovered_through_lsn_exclusive = UINT64_C(0x1100000);
	ut_root_snapshot.checkpoint_record_crc32c = UINT32_C(0x33445566);
	memset(&ut_root_token, 0, sizeof(ut_root_token));
	ut_root_token.origin_thread_id = ut_root_identity.origin_thread_id;
	ut_root_lookup_result = CLUSTER_CONTROL_ROOT_OK_PRIMARY;
	ut_root_publish_result = CLUSTER_CONTROL_ROOT_OK_PRIMARY;
	ut_owner_read_result = CLUSTER_RECOVERY_OWNER_IMPORT_JCMK;
	ut_owner_read_incarnation = new_incarnation;
	ut_owner_read_calls = 0;
	ut_root_publish_calls = 0;
	memset(&ut_root_published_patch, 0, sizeof(ut_root_published_patch));
}

UT_TEST(test_owner_rejoin_requires_jcmk_and_publishes_exact_root_cas)
{
	uint64 old_incarnation = UINT64_C(70);
	uint64 new_incarnation = UINT64_C(77);

	setup_owner_rejoin(old_incarnation, new_incarnation);
	UT_ASSERT(cluster_recovery_owner_rejoin_v1(3, new_incarnation));
	UT_ASSERT_EQ(ut_owner_read_calls, 1);
	UT_ASSERT_EQ(ut_root_publish_calls, 1);
	UT_ASSERT_EQ(ut_root_published_patch.mask, UINT64_C(0x3b));
	UT_ASSERT_EQ(ut_root_published_patch.expected_lifecycle,
				 CLUSTER_CONTROL_ROOT_LIFECYCLE_RECOVERY_COMPLETE);
	UT_ASSERT_EQ(ut_root_published_patch.desired.lifecycle,
				 CLUSTER_CONTROL_ROOT_LIFECYCLE_OPEN);
	UT_ASSERT_EQ(
		ut_root_published_patch.desired.identity.origin_owner_incarnation,
		new_incarnation);
	UT_ASSERT_EQ(ut_root_published_patch.desired.identity.root_lineage_seq,
				 ut_root_identity.root_lineage_seq + 1);
}

UT_TEST(test_owner_rejoin_fails_closed_on_non_jcmk_drift_or_exhaustion)
{
	setup_owner_rejoin(UINT64_C(70), UINT64_C(77));
	ut_owner_read_result = CLUSTER_RECOVERY_OWNER_IMPORT_VOTING_SLOT;
	UT_ASSERT(!cluster_recovery_owner_rejoin_v1(3, UINT64_C(77)));
	UT_ASSERT_EQ(ut_root_publish_calls, 0);

	setup_owner_rejoin(UINT64_C(70), UINT64_C(78));
	UT_ASSERT(!cluster_recovery_owner_rejoin_v1(3, UINT64_C(77)));
	UT_ASSERT_EQ(ut_root_publish_calls, 0);

	setup_owner_rejoin(UINT64_C(70), UINT64_C(77));
	ut_root_snapshot.lifecycle = CLUSTER_CONTROL_ROOT_LIFECYCLE_OPEN;
	UT_ASSERT(!cluster_recovery_owner_rejoin_v1(3, UINT64_C(77)));
	UT_ASSERT_EQ(ut_owner_read_calls, 0);
	setup_owner_rejoin(UINT64_C(77), UINT64_C(77));
	ut_root_snapshot.lifecycle = CLUSTER_CONTROL_ROOT_LIFECYCLE_OPEN;
	ut_root_identity = ut_root_snapshot.identity;
	UT_ASSERT(cluster_recovery_owner_rejoin_v1(3, UINT64_C(77)));
	UT_ASSERT_EQ(ut_owner_read_calls, 1);
	UT_ASSERT_EQ(ut_root_publish_calls, 0);

	setup_owner_rejoin(UINT64_C(70), UINT64_C(77));
	ut_root_snapshot.identity.root_lineage_seq = UINT64_MAX;
	ut_root_identity = ut_root_snapshot.identity;
	UT_ASSERT(!cluster_recovery_owner_rejoin_v1(3, UINT64_C(77)));
	UT_ASSERT_EQ(ut_owner_read_calls, 0);

	setup_owner_rejoin(UINT64_C(70), UINT64_C(77));
	ut_root_publish_result = CLUSTER_CONTROL_ROOT_CAS_CONFLICT;
	UT_ASSERT(!cluster_recovery_owner_rejoin_v1(3, UINT64_C(77)));
	UT_ASSERT_EQ(ut_root_publish_calls, 1);
}

static void
build_expected_encoding(const ClusterRecoveryDutyKey *key,
						uint8 out[CLUSTER_RECOVERY_DUTY_KEY_V1_BYTES])
{
	memset(out, 0, CLUSTER_RECOVERY_DUTY_KEY_V1_BYTES);
	put_u64_le(out, key->system_identifier);
	memcpy(out + 8, key->storage_uuid, 16);
	memcpy(out + 24, key->authority_uuid, 16);
	out[40] = (uint8)key->origin_thread_id;
	out[41] = (uint8)(key->origin_thread_id >> 8);
	put_u32_le(out + 42, (uint32)key->origin_node_id);
	put_u64_le(out + 46, (uint64)key->thread_claim_created_at);
	put_u32_le(out + 54, key->thread_claim_crc32c);
	put_u64_le(out + 58, key->origin_owner_incarnation);
	put_u64_le(out + 66, key->root_lineage_seq);
}

static void
build_valid_formation(ClusterFormationSnapshotV1 *snapshot,
					  ClusterFenceAuthorityProof *proof, uint16 origin_thread)
{
	int32 origin_node = (int32)origin_thread - 1;

	memset(snapshot, 0, sizeof(*snapshot));
	snapshot->applied.event_id = UINT64_C(0xAA);
	snapshot->applied.coordinator_node_id = 0;
	snapshot->applied.new_epoch = 9;
	snapshot->applied.cssd_dead_generation = 5;
	snapshot->applied.reconfig_kind = RECONFIG_KIND_FAIL_STOP;
	snapshot->applied.dead_bitmap[origin_node / 8] |= (uint8)(1u << (origin_node % 8));
	snapshot->membership.membership_state[0] = CLUSTER_MEMBER_MEMBER;
	snapshot->membership.membership_state[origin_node] = CLUSTER_MEMBER_DEAD;
	snapshot->membership.last_admitted_incarnation[origin_node] = UINT64_C(77);
	snapshot->victim_incarnation = UINT64_C(77);
	snapshot->local_epoch = 9;
	memcpy(snapshot->excluded_bitmap, snapshot->applied.dead_bitmap,
		   sizeof(snapshot->excluded_bitmap));
	snapshot->self_join_admitted = 1;

	memset(proof, 0, sizeof(*proof));
	proof->marker.magic = CLUSTER_FENCE_MARKER_MAGIC;
	proof->marker.version = CLUSTER_FENCE_MARKER_VERSION;
	proof->marker.fence_epoch = 9;
	proof->marker.fence_event_id = UINT64_C(0xAA);
	proof->marker.fence_generation = 5;
	proof->marker.issuer_node_id = 0;
	memcpy(proof->marker.fenced_dead_bitmap, snapshot->excluded_bitmap,
		   sizeof(proof->marker.fenced_dead_bitmap));
	proof->agree_disk_count = 2;
	proof->total_disk_count = 3;
}

static void
build_owner_samples(ClusterRecoveryOwnerDiskSampleV1 samples[3], int32 node_id,
					uint64 incarnation)
{
	int i;

	memset(samples, 0, sizeof(*samples) * 3);
	for (i = 0; i < 3; i++) {
		samples[i].join_io_state = CLUSTER_VOTING_DISK_IO_OK;
		samples[i].slot_io_state = CLUSTER_VOTING_DISK_IO_OK;
		samples[i].slot.node_id = (uint32)node_id;
		samples[i].slot.incarnation = incarnation;
	}
}

static void
set_committed_join_marker(ClusterJoinCommitMarker *marker, int32 node_id,
						  uint64 incarnation, uint64 nonce)
{
	pg_crc32c crc;

	memset(marker, 0, sizeof(*marker));
	marker->magic = CLUSTER_JCMK_MAGIC;
	marker->version = CLUSTER_JCMK_VERSION;
	marker->node_id = node_id;
	marker->phase = CLUSTER_JCMK_PHASE_COMMITTED;
	marker->generation = 1;
	marker->admitted_incarnation = incarnation;
	marker->admitted_epoch = 9;
	marker->commit_nonce = nonce;
	INIT_CRC32C(crc);
	COMP_CRC32C(crc, marker, offsetof(ClusterJoinCommitMarker, crc32c));
	FIN_CRC32C(crc);
	marker->crc32c = (uint32)crc;
}

UT_TEST(test_owner_import_prefers_exact_jcmk_majority)
{
	ClusterRecoveryOwnerDiskSampleV1 samples[3];
	ClusterWalThreadClaim claim;
	uint64 incarnation = 0;

	build_owner_samples(samples, 3, 70);
	cluster_wal_thread_claim_fill(&claim, 4, 3, INT64_C(12345));
	set_committed_join_marker(&samples[0].join_marker, 3, 77, 11);
	samples[1].join_marker = samples[0].join_marker;
	set_committed_join_marker(&samples[2].join_marker, 3, 88, 12);
	UT_ASSERT_EQ(cluster_recovery_owner_import_select_v1(
				 3, &claim, UINT64_C(1) << 3, 0, samples, 3, &incarnation),
				 CLUSTER_RECOVERY_OWNER_IMPORT_JCMK);
	UT_ASSERT_EQ(incarnation, 77);
}

UT_TEST(test_owner_import_never_falls_back_from_split_jcmk)
{
	ClusterRecoveryOwnerDiskSampleV1 samples[3];
	ClusterWalThreadClaim claim;
	uint64 incarnation = UINT64_C(0xdeadbeef);

	build_owner_samples(samples, 3, 70);
	cluster_wal_thread_claim_fill(&claim, 4, 3, INT64_C(12345));
	set_committed_join_marker(&samples[0].join_marker, 3, 77, 11);
	set_committed_join_marker(&samples[1].join_marker, 3, 88, 12);
	set_committed_join_marker(&samples[2].join_marker, 3, 99, 13);
	UT_ASSERT_EQ(cluster_recovery_owner_import_select_v1(
				 3, &claim, UINT64_C(1) << 3, 0, samples, 3, &incarnation),
				 CLUSTER_RECOVERY_OWNER_IMPORT_JCMK_UNPROVEN);
	UT_ASSERT_EQ(incarnation, 0);
}

UT_TEST(test_owner_import_slot_fallback_requires_absent_jcmk_and_claim)
{
	ClusterRecoveryOwnerDiskSampleV1 samples[3];
	ClusterWalThreadClaim claim;
	uint64 incarnation = 0;

	build_owner_samples(samples, 3, 70);
	samples[2].slot.incarnation = 71;
	cluster_wal_thread_claim_fill(&claim, 4, 3, INT64_C(12345));
	UT_ASSERT_EQ(cluster_recovery_owner_import_select_v1(
				 3, &claim, UINT64_C(1) << 3, 0, samples, 3, &incarnation),
				 CLUSTER_RECOVERY_OWNER_IMPORT_VOTING_SLOT);
	UT_ASSERT_EQ(incarnation, 70);
	claim.node_id = 4;
	incarnation = UINT64_C(0xdeadbeef);
	UT_ASSERT_EQ(cluster_recovery_owner_import_select_v1(
				 3, &claim, UINT64_C(1) << 3, 0, samples, 3, &incarnation),
				 CLUSTER_RECOVERY_OWNER_IMPORT_CLAIM_MISMATCH);
	UT_ASSERT_EQ(incarnation, 0);
	cluster_wal_thread_claim_fill(&claim, 4, 3, INT64_C(12345));
	UT_ASSERT_EQ(cluster_recovery_owner_import_select_v1(
				 3, &claim, 0, 0, samples, 3, &incarnation),
				 CLUSTER_RECOVERY_OWNER_IMPORT_SLOT_UNPROVEN);
}

UT_TEST(test_owner_import_cannot_prove_jcmk_absence_with_unreadable_disk)
{
	ClusterRecoveryOwnerDiskSampleV1 samples[3];
	ClusterWalThreadClaim claim;
	uint64 incarnation = 0;

	build_owner_samples(samples, 3, 70);
	samples[2].join_io_state = CLUSTER_VOTING_DISK_IO_FAILED;
	cluster_wal_thread_claim_fill(&claim, 4, 3, INT64_C(12345));
	UT_ASSERT_EQ(cluster_recovery_owner_import_select_v1(
				 3, &claim, UINT64_C(1) << 3, 0, samples, 3, &incarnation),
				 CLUSTER_RECOVERY_OWNER_IMPORT_IO_FAILED);
	UT_ASSERT_EQ(incarnation, 0);
}

UT_TEST(test_exact_74_byte_encoding)
{
	ClusterRecoveryDutyKey key;
	uint8 actual[CLUSTER_RECOVERY_DUTY_KEY_V1_BYTES];
	uint8 expected[CLUSTER_RECOVERY_DUTY_KEY_V1_BYTES];

	build_valid_key(&key);
	build_expected_encoding(&key, expected);
	memset(actual, 0xee, sizeof(actual));
	UT_ASSERT(cluster_recovery_duty_key_encode_v1(&key, actual));
	UT_ASSERT(memcmp(actual, expected, sizeof(actual)) == 0);
	UT_ASSERT_EQ(sizeof(actual), 74);
}

UT_TEST(test_domain_separated_digest)
{
	ClusterRecoveryDutyKey key;
	ClusterRecoveryDutyDigest actual;
	uint8 identity[CLUSTER_RECOVERY_DUTY_KEY_V1_BYTES];
	uint8 preimage[19 + 4 + CLUSTER_RECOVERY_DUTY_KEY_V1_BYTES];
	uint8 expected[PG_SHA256_DIGEST_LENGTH];

	build_valid_key(&key);
	build_expected_encoding(&key, identity);
	memset(preimage, 0, sizeof(preimage));
	memcpy(preimage, "PGRAC-ROOT-DUTY-V1", 18);
	put_u32_le(preimage + 19, CLUSTER_RECOVERY_DUTY_KEY_V1_BYTES);
	memcpy(preimage + 23, identity, sizeof(identity));
	sha256_bytes(preimage, sizeof(preimage), expected);
	memset(&actual, 0xee, sizeof(actual));
	UT_ASSERT(cluster_recovery_duty_digest_v1(&key, &actual));
	UT_ASSERT(memcmp(actual.bytes, expected, sizeof(expected)) == 0);
}

UT_TEST(test_full_key_compare_has_no_numeric_order)
{
	ClusterRecoveryDutyKey expected;
	ClusterRecoveryDutyKey observed;

	build_valid_key(&expected);
	observed = expected;
	UT_ASSERT_EQ(cluster_recovery_duty_key_compare(&expected, &observed),
				 CLUSTER_RECOVERY_DUTY_COMPARE_EXACT);
	observed.root_lineage_seq++;
	UT_ASSERT_EQ(cluster_recovery_duty_key_compare(&expected, &observed),
				 CLUSTER_RECOVERY_DUTY_COMPARE_DIFFERENT);
	observed = expected;
	observed.root_lineage_seq--;
	UT_ASSERT_EQ(cluster_recovery_duty_key_compare(&expected, &observed),
				 CLUSTER_RECOVERY_DUTY_COMPARE_DIFFERENT);
	observed = expected;
	observed.authority_uuid[15] ^= 1;
	UT_ASSERT_EQ(cluster_recovery_duty_key_compare(&expected, &observed),
				 CLUSTER_RECOVERY_DUTY_COMPARE_DIFFERENT);
}

static void
assert_invalid_key(ClusterRecoveryDutyKey *key)
{
	ClusterRecoveryDutyKey valid;
	ClusterRecoveryDutyDigest digest;
	uint8 encoded[CLUSTER_RECOVERY_DUTY_KEY_V1_BYTES];

	build_valid_key(&valid);
	memset(encoded, 0xee, sizeof(encoded));
	memset(&digest, 0xee, sizeof(digest));
	UT_ASSERT(!cluster_recovery_duty_key_encode_v1(key, encoded));
	UT_ASSERT(memcmp(encoded, (uint8[CLUSTER_RECOVERY_DUTY_KEY_V1_BYTES]){0},
				 sizeof(encoded)) == 0);
	UT_ASSERT(!cluster_recovery_duty_digest_v1(key, &digest));
	UT_ASSERT(memcmp(digest.bytes, (uint8[CLUSTER_RECOVERY_DUTY_DIGEST_BYTES]){0},
				 sizeof(digest.bytes)) == 0);
	UT_ASSERT_EQ(cluster_recovery_duty_key_compare(key, &valid),
				 CLUSTER_RECOVERY_DUTY_COMPARE_INVALID);
}

UT_TEST(test_zero_and_reserved_fields_are_invalid)
{
	ClusterRecoveryDutyKey key;

	build_valid_key(&key);
	key.system_identifier = 0;
	assert_invalid_key(&key);
	build_valid_key(&key);
	memset(key.storage_uuid, 0, sizeof(key.storage_uuid));
	assert_invalid_key(&key);
	build_valid_key(&key);
	memset(key.authority_uuid, 0, sizeof(key.authority_uuid));
	assert_invalid_key(&key);
	build_valid_key(&key);
	key.origin_owner_incarnation = 0;
	assert_invalid_key(&key);
	build_valid_key(&key);
	key.root_lineage_seq = 0;
	assert_invalid_key(&key);
	build_valid_key(&key);
	key.reserved42 = 1;
	assert_invalid_key(&key);
	build_valid_key(&key);
	key.reserved60 = 1;
	assert_invalid_key(&key);
}

UT_TEST(test_thread_node_and_claim_binding_are_exact)
{
	ClusterRecoveryDutyKey key;

	build_valid_key(&key);
	key.origin_thread_id = 0;
	assert_invalid_key(&key);
	build_valid_key(&key);
	key.origin_thread_id = 5;
	assert_invalid_key(&key);
	build_valid_key(&key);
	key.origin_node_id = 128;
	assert_invalid_key(&key);
	build_valid_key(&key);
	key.thread_claim_created_at = 0;
	assert_invalid_key(&key);
	build_valid_key(&key);
	key.thread_claim_crc32c ^= 1;
	assert_invalid_key(&key);
}

UT_TEST(test_authority_uuid_must_be_v4_and_max_lineage_is_valid)
{
	ClusterRecoveryDutyKey key;
	uint8 encoded[CLUSTER_RECOVERY_DUTY_KEY_V1_BYTES];

	build_valid_key(&key);
	key.authority_uuid[6] = 0x36;
	assert_invalid_key(&key);
	build_valid_key(&key);
	key.authority_uuid[8] = 0xca;
	assert_invalid_key(&key);
	build_valid_key(&key);
	key.root_lineage_seq = UINT64_MAX;
	UT_ASSERT(cluster_recovery_duty_key_encode_v1(&key, encoded));
}

UT_TEST(test_formation_f1_majority_f2_ready)
{
	ClusterFormationSnapshotV1 f1;
	ClusterFormationSnapshotV1 f2;
	ClusterFenceAuthorityProof proof;

	build_valid_formation(&f1, &proof, 4);
	f2 = f1;
	UT_ASSERT_EQ(cluster_formation_witness_decide_v1(&f1, &proof, &f2, 4, true),
				 CLUSTER_FORMATION_WITNESS_READY);
}

UT_TEST(test_formation_snapshot_or_marker_drift_is_rejected)
{
	ClusterFormationSnapshotV1 f1;
	ClusterFormationSnapshotV1 f2;
	ClusterFenceAuthorityProof proof;

	build_valid_formation(&f1, &proof, 4);
	f2 = f1;
	f2.local_epoch++;
	UT_ASSERT_EQ(cluster_formation_witness_decide_v1(&f1, &proof, &f2, 4, true),
				 CLUSTER_FORMATION_WITNESS_UNSTABLE);
	f2 = f1;
	proof.marker.fence_event_id++;
	UT_ASSERT_EQ(cluster_formation_witness_decide_v1(&f1, &proof, &f2, 4, true),
				 CLUSTER_FORMATION_WITNESS_MARKER_UNPROVEN);
}

UT_TEST(test_formation_new_duty_requires_current_event_delta)
{
	ClusterFormationSnapshotV1 f1;
	ClusterFormationSnapshotV1 f2;
	ClusterFenceAuthorityProof proof;

	build_valid_formation(&f1, &proof, 4);
	f1.applied.dead_bitmap[0] &= (uint8)~0x08;
	/* The origin remains in the full excluded set, but not this event's delta. */
	f2 = f1;
	UT_ASSERT_EQ(cluster_formation_witness_decide_v1(&f1, &proof, &f2, 4, true),
				 CLUSTER_FORMATION_WITNESS_ORIGIN_NOT_EXCLUDED);
	UT_ASSERT_EQ(cluster_formation_witness_decide_v1(&f1, &proof, &f2, 4, false),
				 CLUSTER_FORMATION_WITNESS_READY);
}

UT_TEST(test_formation_pending_owner_and_full_outage_fail_closed)
{
	ClusterFormationSnapshotV1 f1;
	ClusterFormationSnapshotV1 f2;
	ClusterFenceAuthorityProof proof;

	build_valid_formation(&f1, &proof, 4);
	f1.prebump_sync_active = 1;
	f2 = f1;
	UT_ASSERT_EQ(cluster_formation_witness_decide_v1(&f1, &proof, &f2, 4, true),
				 CLUSTER_FORMATION_WITNESS_UNSTABLE);
	build_valid_formation(&f1, &proof, 4);
	f1.victim_incarnation++;
	f2 = f1;
	UT_ASSERT_EQ(cluster_formation_witness_decide_v1(&f1, &proof, &f2, 4, true),
				 CLUSTER_FORMATION_WITNESS_OWNER_MISMATCH);
	build_valid_formation(&f1, &proof, 4);
	f1.membership.membership_state[0] = CLUSTER_MEMBER_DEAD;
	f2 = f1;
	UT_ASSERT_EQ(cluster_formation_witness_decide_v1(&f1, &proof, &f2, 4, true),
				 CLUSTER_FORMATION_WITNESS_FULL_OUTAGE_UNRECOVERED);
}

int
main(void)
{
	UT_PLAN(16);
	UT_RUN(test_exact_74_byte_encoding);
	UT_RUN(test_domain_separated_digest);
	UT_RUN(test_full_key_compare_has_no_numeric_order);
	UT_RUN(test_zero_and_reserved_fields_are_invalid);
	UT_RUN(test_thread_node_and_claim_binding_are_exact);
	UT_RUN(test_authority_uuid_must_be_v4_and_max_lineage_is_valid);
	UT_RUN(test_owner_import_prefers_exact_jcmk_majority);
	UT_RUN(test_owner_import_never_falls_back_from_split_jcmk);
	UT_RUN(test_owner_import_slot_fallback_requires_absent_jcmk_and_claim);
	UT_RUN(test_owner_import_cannot_prove_jcmk_absence_with_unreadable_disk);
	UT_RUN(test_owner_rejoin_requires_jcmk_and_publishes_exact_root_cas);
	UT_RUN(test_owner_rejoin_fails_closed_on_non_jcmk_drift_or_exhaustion);
	UT_RUN(test_formation_f1_majority_f2_ready);
	UT_RUN(test_formation_snapshot_or_marker_drift_is_rejected);
	UT_RUN(test_formation_new_duty_requires_current_event_delta);
	UT_RUN(test_formation_pending_owner_and_full_outage_fail_closed);
	UT_DONE();

	return ut_failed_count == 0 ? 0 : 1;
}
