/*-------------------------------------------------------------------------
 *
 * cluster_control_root.c
 *	  Survivor-readable failed-origin control-root carrier (RF-ROOT P1).
 *
 * The carrier is the user-approved PGRAC adaptation in frozen private spec
 * spec-s8-stop-01-root-control.md section 17.  It does not claim that these
 * bytes are Oracle control-file bytes.  Oracle alignment is at the authority
 * boundary: shared durable control metadata serialized by the CF enqueue.
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include <fcntl.h>
#include <sys/stat.h>
#include <unistd.h>

#include "access/xlog.h"
#include "cluster/cluster_cf_enqueue.h"
#include "cluster/cluster_cf_storage.h"
#include "cluster/cluster_conf.h"
#include "cluster/cluster_control_root.h"
#include "cluster/cluster_guc.h"
#include "cluster/cluster_semantic_activation.h"
#include "cluster/cluster_wal_state.h"
#include "cluster/cluster_wal_thread.h"
#include "cluster/storage/cluster_shared_fs.h"
#include "common/cryptohash.h"
#include "common/sha2.h"
#include "miscadmin.h"
#include "port/pg_crc32c.h"
#include "storage/fd.h"
#include "utils/timestamp.h"

#define CONTROL_ROOT_HEADER_MAGIC "PGCH"
#define CONTROL_ROOT_RECORD_MAGIC "PGRT"
#define CONTROL_ROOT_FORMAT_VERSION UINT16_C(1)
#define CONTROL_ROOT_ENDIAN_TAG UINT32_C(0x01020304)
#define CONTROL_ROOT_READER_VERSION UINT16_C(1)
#define CONTROL_ROOT_WRITER_VERSION UINT16_C(1)
#define CONTROL_ROOT_SOURCE_PRIMARY UINT8_C(1)
#define CONTROL_ROOT_SOURCE_BAK_BLOCKED UINT8_C(2)
#define CONTROL_ROOT_SOURCE_BOOTSTRAP_PRIMARY UINT8_C(3)

#define CONTROL_ROOT_HEADER_CRC_OFFSET 504
#define CONTROL_ROOT_RECORD_CRC_OFFSET 504
#define CONTROL_ROOT_BODY_OFFSET CLUSTER_CONTROL_ROOT_HEADER_BYTES

typedef struct ControlRootHeader {
	uint64 file_txn_seq;
	uint64 system_identifier;
	uint8 storage_uuid[16];
	uint8 authority_uuid[16];
	uint32 activation_state;
	int64 created_at_usec;
	int64 published_at_usec;
	uint32 body_crc32c;
	uint8 migration_round_sha256[PG_SHA256_DIGEST_LENGTH];
	uint8 source_wal_state_sha256[PG_SHA256_DIGEST_LENGTH];
	uint64 migration_prepare_generation;
	uint64 migration_transition_epoch;
	uint64 source_feature_bitmap;
	uint64 target_feature_bitmap;
	uint32 header_crc32c;
} ControlRootHeader;

typedef struct ControlRootImage {
	uint8 bytes[CLUSTER_CONTROL_ROOT_FILE_BYTES];
	ControlRootHeader header;
	ClusterControlRootSnapshot records[CLUSTER_CONTROL_ROOT_RECORD_COUNT];
	uint32 record_crc32c[CLUSTER_CONTROL_ROOT_RECORD_COUNT];
	bool present[CLUSTER_CONTROL_ROOT_RECORD_COUNT];
} ControlRootImage;

static uint16
read_u16_le(const uint8 *src)
{
	return (uint16)src[0] | ((uint16)src[1] << 8);
}

static uint32
read_u32_le(const uint8 *src)
{
	return (uint32)src[0] | ((uint32)src[1] << 8) | ((uint32)src[2] << 16)
		   | ((uint32)src[3] << 24);
}

static uint64
read_u64_le(const uint8 *src)
{
	uint64 value = 0;
	int i;

	for (i = 7; i >= 0; i--)
		value = (value << 8) | src[i];
	return value;
}

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

static uint32
control_root_crc(const uint8 *bytes, size_t len)
{
	pg_crc32c crc;

	INIT_CRC32C(crc);
	COMP_CRC32C(crc, bytes, len);
	FIN_CRC32C(crc);
	return (uint32)crc;
}

static bool
control_root_sha256(const uint8 *bytes, size_t len, uint8 digest[PG_SHA256_DIGEST_LENGTH])
{
	pg_cryptohash_ctx *ctx;
	bool ok = false;

	ctx = pg_cryptohash_create(PG_SHA256);
	if (ctx == NULL)
		return false;
	if (pg_cryptohash_init(ctx) >= 0 && pg_cryptohash_update(ctx, bytes, len) >= 0
		&& pg_cryptohash_final(ctx, digest, PG_SHA256_DIGEST_LENGTH) >= 0)
		ok = true;
	pg_cryptohash_free(ctx);
	return ok;
}

static bool
bytes_are_zero(const void *ptr, size_t len)
{
	const uint8 *bytes = ptr;
	size_t i;

	for (i = 0; i < len; i++) {
		if (bytes[i] != 0)
			return false;
	}
	return true;
}

static bool
uuid_v4_valid(const uint8 uuid[16])
{
	return !bytes_are_zero(uuid, 16) && (uuid[6] & UINT8_C(0xf0)) == UINT8_C(0x40)
		   && (uuid[8] & UINT8_C(0xc0)) == UINT8_C(0x80);
}

bool
cluster_control_root_identity_equal(const ClusterControlRootIdentity *left,
									const ClusterControlRootIdentity *right)
{
	return left != NULL && right != NULL
		   && left->system_identifier == right->system_identifier
		   && memcmp(left->storage_uuid, right->storage_uuid, 16) == 0
		   && memcmp(left->authority_uuid, right->authority_uuid, 16) == 0
		   && left->origin_thread_id == right->origin_thread_id
		   && left->reserved42 == 0 && right->reserved42 == 0
		   && left->origin_node_id == right->origin_node_id
		   && left->thread_claim_created_at == right->thread_claim_created_at
		   && left->thread_claim_crc32c == right->thread_claim_crc32c
		   && left->reserved60 == 0 && right->reserved60 == 0
		   && left->origin_owner_incarnation == right->origin_owner_incarnation
		   && left->root_lineage_seq == right->root_lineage_seq;
}

bool
cluster_control_root_feature_bitmap_is_known(uint64 active_feature_bitmap)
{
	const uint64 known = CLUSTER_SEMANTIC_FEATURE_R4_SYNC_CR_V1
		| PGRAC_CONTROL_ROOT_FEATURE_WAL_REUSE_V1
		| PGRAC_CONTROL_ROOT_FEATURE_PAGE_STABLE_BASE_V1
		| PGRAC_CONTROL_ROOT_FEATURE_SPACE_METADATA_V1
		| PGRAC_CONTROL_ROOT_FEATURE_CONSERVATIVE_COMMIT_SCN_V1
		| PGRAC_CONTROL_ROOT_FEATURE_RECOVERY_DUTY_IDENTITY_V1
		| PGRAC_CONTROL_ROOT_FEATURE_RECOVERY_SERIAL_V1
		| PGRAC_CONTROL_ROOT_FEATURE_EXTERNAL_FENCE_V1;

	return (active_feature_bitmap & ~known) == 0;
}

static bool
build_control_path(char *dst, size_t dstlen, const char *relative)
{
	int written;

	if (dst == NULL || dstlen == 0 || relative == NULL || cluster_shared_data_dir == NULL
		|| cluster_shared_data_dir[0] == '\0')
		return false;
	written = snprintf(dst, dstlen, "%s/%s", cluster_shared_data_dir, relative);
	return written > 0 && (size_t)written < dstlen;
}

static bool
regular_or_absent_nosymlink(const char *path, bool allow_absent)
{
	struct stat st;

	if (lstat(path, &st) != 0)
		return allow_absent && errno == ENOENT;
	return S_ISREG(st.st_mode);
}

static bool
control_paths_safe(void)
{
	char global_path[MAXPGPATH];
	char primary[MAXPGPATH];
	char bak[MAXPGPATH];
	struct stat st;

	if (cluster_shared_data_dir == NULL || lstat(cluster_shared_data_dir, &st) != 0
		|| !S_ISDIR(st.st_mode))
		return false;
	if (snprintf(global_path, sizeof(global_path), "%s/global", cluster_shared_data_dir) <= 0
		|| lstat(global_path, &st) != 0 || !S_ISDIR(st.st_mode))
		return false;
	if (!build_control_path(primary, sizeof(primary), CLUSTER_CONTROL_ROOT_REL_PATH)
		|| !build_control_path(bak, sizeof(bak), CLUSTER_CONTROL_ROOT_BAK_REL_PATH))
		return false;
	return regular_or_absent_nosymlink(primary, true)
		   && regular_or_absent_nosymlink(bak, true);
}

static int
hex_digit_value(char ch)
{
	if (ch >= '0' && ch <= '9')
		return ch - '0';
	if (ch >= 'a' && ch <= 'f')
		return ch - 'a' + 10;
	return -1;
}

static bool
current_storage_uuid(uint8 uuid[16])
{
	char text[CLUSTER_SHARED_UUID_LEN];
	int i;

	memset(text, 0, sizeof(text));
	cluster_shared_fs_get_storage_uuid(text, sizeof(text));
	if (strlen(text) != 32)
		return false;
	for (i = 0; i < 16; i++) {
		int high = hex_digit_value(text[i * 2]);
		int low = hex_digit_value(text[i * 2 + 1]);

		if (high < 0 || low < 0)
			return false;
		uuid[i] = (uint8)((high << 4) | low);
	}
	return !bytes_are_zero(uuid, 16);
}

static ClusterControlRootResult
storage_contract_check(const uint8 *expected_storage_uuid, bool require_local_probe)
{
	uint8 current_uuid[16];
	ClusterCfContractState state;
	bool multi_node;

	if (!current_storage_uuid(current_uuid))
		return CLUSTER_CONTROL_ROOT_STORAGE_CONTRACT_UNVERIFIED;
	if (expected_storage_uuid != NULL && memcmp(current_uuid, expected_storage_uuid, 16) != 0)
		return CLUSTER_CONTROL_ROOT_IDENTITY_MISMATCH;
	if (DataDir == NULL || DataDir[0] == '\0')
		return CLUSTER_CONTROL_ROOT_STORAGE_CONTRACT_UNVERIFIED;
	state = cluster_cf_contract_load(DataDir);
	multi_node = cluster_conf_node_count() > 1;
	if ((multi_node && !cluster_cf_storage_write_allowed(state, true))
		|| (!multi_node && require_local_probe && !cluster_cf_storage_probe_local()))
		return CLUSTER_CONTROL_ROOT_STORAGE_CONTRACT_UNVERIFIED;
	if (!control_paths_safe())
		return CLUSTER_CONTROL_ROOT_IO_ERROR;
	return CLUSTER_CONTROL_ROOT_OK_PRIMARY;
}

static bool
snapshot_reserved_zero(const ClusterControlRootSnapshot *snapshot)
{
	return snapshot->identity.reserved42 == 0 && snapshot->identity.reserved60 == 0
		   && snapshot->reserved96 == 0 && snapshot->reserved122 == 0
		   && snapshot->reserved124 == 0 && snapshot->reserved160 == 0
		   && snapshot->reserved208 == 0;
}

static ClusterControlRootResult
snapshot_validate(const ClusterControlRootSnapshot *snapshot, uint16 expected_thread,
				  uint64 system_identifier, const uint8 storage_uuid[16],
				  const uint8 authority_uuid[16])
{
	uint32 flags;
	bool checkpoint_valid;
	bool tail_valid;
	bool recovered_valid;
	bool tail_last_valid;
	bool recovered_last_valid;
	bool bound_valid;

	if (snapshot == NULL || !snapshot_reserved_zero(snapshot))
		return CLUSTER_CONTROL_ROOT_BAD_RESERVED;
	if (snapshot->identity.system_identifier != system_identifier
		|| memcmp(snapshot->identity.storage_uuid, storage_uuid, 16) != 0
		|| memcmp(snapshot->identity.authority_uuid, authority_uuid, 16) != 0)
		return CLUSTER_CONTROL_ROOT_IDENTITY_MISMATCH;
	if (snapshot->identity.origin_thread_id != expected_thread
		|| snapshot->identity.origin_node_id < 0
		|| snapshot->identity.origin_node_id >= CLUSTER_CONTROL_ROOT_RECORD_COUNT
		|| snapshot->identity.thread_claim_created_at == 0
		|| snapshot->identity.thread_claim_crc32c == 0
		|| snapshot->identity.origin_owner_incarnation == 0
		|| snapshot->identity.root_lineage_seq == 0)
		return CLUSTER_CONTROL_ROOT_IDENTITY_MISMATCH;
	if (snapshot->lifecycle < CLUSTER_CONTROL_ROOT_LIFECYCLE_OPEN
		|| snapshot->lifecycle > CLUSTER_CONTROL_ROOT_LIFECYCLE_RETIRED
		|| snapshot->root_publish_seq == 0)
		return CLUSTER_CONTROL_ROOT_LIFECYCLE_INVALID;
	flags = snapshot->root_flags;
	if ((flags & ~CLUSTER_CONTROL_ROOT_FLAGS_V1) != 0
		|| (flags & CLUSTER_CONTROL_ROOT_FLAG_CLAIM_VALID) == 0)
		return CLUSTER_CONTROL_ROOT_BAD_RESERVED;
	checkpoint_valid = (flags & CLUSTER_CONTROL_ROOT_FLAG_CHECKPOINT_VALID) != 0;
	tail_valid = (flags & CLUSTER_CONTROL_ROOT_FLAG_TAIL_VALID) != 0;
	recovered_valid = (flags & CLUSTER_CONTROL_ROOT_FLAG_RECOVERED_VALID) != 0;
	tail_last_valid = (flags & CLUSTER_CONTROL_ROOT_FLAG_TAIL_LAST_RECORD_VALID) != 0;
	recovered_last_valid =
		(flags & CLUSTER_CONTROL_ROOT_FLAG_RECOVERED_LAST_RECORD_VALID) != 0;
	bound_valid = (flags & CLUSTER_CONTROL_ROOT_FLAG_CONSERVATIVE_SCN_VALID) != 0;

	if (!checkpoint_valid || snapshot->checkpoint_tli == 0
		|| snapshot->checkpoint_lower_lsn == 0
		|| snapshot->checkpoint_record_crc32c == 0
		|| (snapshot->checkpoint_source_kind != CLUSTER_CONTROL_ROOT_CHECKPOINT_NATIVE_V1
			&& snapshot->checkpoint_source_kind
				   != CLUSTER_CONTROL_ROOT_CHECKPOINT_RECOVERY_ANCHOR_V1))
		return CLUSTER_CONTROL_ROOT_RANGE_INVALID;
	if (tail_valid) {
		if (snapshot->tail_tli == 0
			|| snapshot->tail_validation_kind
				   != CLUSTER_CONTROL_ROOT_TAIL_WAL_RECORD_SCAN_V1
			|| snapshot->validated_tail_lsn_exclusive < snapshot->checkpoint_lower_lsn)
			return CLUSTER_CONTROL_ROOT_RANGE_INVALID;
		if (snapshot->validated_tail_lsn_exclusive == snapshot->checkpoint_lower_lsn) {
			if (tail_last_valid || snapshot->tail_last_record_lsn != 0
				|| snapshot->tail_last_record_crc32c != 0)
				return CLUSTER_CONTROL_ROOT_RANGE_INVALID;
		} else if (!tail_last_valid || snapshot->tail_last_record_lsn == 0
				   || snapshot->tail_last_record_crc32c == 0
				   || snapshot->tail_last_record_lsn
					  >= snapshot->validated_tail_lsn_exclusive)
			return CLUSTER_CONTROL_ROOT_RANGE_INVALID;
	} else if (snapshot->tail_tli != 0 || snapshot->tail_validation_kind != 0
			   || snapshot->validated_tail_lsn_exclusive != 0 || tail_last_valid
			   || snapshot->tail_last_record_lsn != 0
			   || snapshot->tail_last_record_crc32c != 0)
		return CLUSTER_CONTROL_ROOT_RANGE_INVALID;

	if (recovered_valid) {
		if (!tail_valid || snapshot->recovered_tli == 0
			|| snapshot->recovered_through_lsn_exclusive < snapshot->checkpoint_lower_lsn
			|| snapshot->recovered_through_lsn_exclusive
				   > snapshot->validated_tail_lsn_exclusive)
			return CLUSTER_CONTROL_ROOT_RANGE_INVALID;
		if (snapshot->recovered_through_lsn_exclusive == snapshot->checkpoint_lower_lsn) {
			if (recovered_last_valid || snapshot->recovered_last_record_lsn != 0
				|| snapshot->recovered_last_record_crc32c != 0)
				return CLUSTER_CONTROL_ROOT_RANGE_INVALID;
		} else if (!recovered_last_valid || snapshot->recovered_last_record_lsn == 0
				   || snapshot->recovered_last_record_crc32c == 0
				   || snapshot->recovered_last_record_lsn
					  >= snapshot->recovered_through_lsn_exclusive)
			return CLUSTER_CONTROL_ROOT_RANGE_INVALID;
	} else if (snapshot->recovered_tli != 0 || recovered_last_valid
			   || snapshot->recovered_last_record_lsn != 0
			   || snapshot->recovered_last_record_crc32c != 0
			   || (snapshot->recovered_through_lsn_exclusive != 0
				   && snapshot->recovered_through_lsn_exclusive
					  != snapshot->checkpoint_lower_lsn))
		return CLUSTER_CONTROL_ROOT_RANGE_INVALID;

	if (bound_valid) {
		if (snapshot->conservative_bound_kind
				!= CLUSTER_CONTROL_ROOT_BOUND_R14_M1_PARTITION_S_V1
			|| snapshot->conservative_commit_scn == 0)
			return CLUSTER_CONTROL_ROOT_RANGE_INVALID;
	} else if (snapshot->conservative_bound_kind != CLUSTER_CONTROL_ROOT_BOUND_NONE
			   || snapshot->conservative_commit_scn != 0)
		return CLUSTER_CONTROL_ROOT_RANGE_INVALID;
	if (snapshot->lifecycle_reason < CLUSTER_CONTROL_ROOT_PUBLISH_MIGRATION_IMPORT
		|| snapshot->lifecycle_reason > CLUSTER_CONTROL_ROOT_PUBLISH_COPY_REPAIR)
		return CLUSTER_CONTROL_ROOT_LIFECYCLE_INVALID;
	return CLUSTER_CONTROL_ROOT_OK_PRIMARY;
}

static void
encode_record(uint8 *dst, const ClusterControlRootSnapshot *snapshot,
			  uint64 publisher_incarnation, uint32 publisher_node,
			  ClusterControlRootPublishReason reason)
{
	memset(dst, 0, CLUSTER_CONTROL_ROOT_RECORD_BYTES);
	memcpy(dst, CONTROL_ROOT_RECORD_MAGIC, 4);
	write_u16_le(dst + 4, CONTROL_ROOT_FORMAT_VERSION);
	write_u16_le(dst + 6, CLUSTER_CONTROL_ROOT_RECORD_BYTES);
	write_u16_le(dst + 8, snapshot->identity.origin_thread_id);
	dst[10] = (uint8)snapshot->lifecycle;
	write_u32_le(dst + 12, (uint32)snapshot->identity.origin_node_id);
	write_u64_le(dst + 16, snapshot->root_publish_seq);
	write_u64_le(dst + 24, snapshot->identity.root_lineage_seq);
	write_u64_le(dst + 32, snapshot->identity.system_identifier);
	memcpy(dst + 40, snapshot->identity.storage_uuid, 16);
	memcpy(dst + 56, snapshot->identity.authority_uuid, 16);
	write_u64_le(dst + 72, (uint64)snapshot->identity.thread_claim_created_at);
	write_u64_le(dst + 80, snapshot->identity.origin_owner_incarnation);
	write_u32_le(dst + 96, snapshot->checkpoint_tli);
	write_u32_le(dst + 100, snapshot->tail_tli);
	write_u32_le(dst + 104, snapshot->recovered_tli);
	write_u32_le(dst + 108, snapshot->root_flags);
	write_u64_le(dst + 112, snapshot->checkpoint_lower_lsn);
	write_u64_le(dst + 120, snapshot->validated_tail_lsn_exclusive);
	write_u64_le(dst + 128, snapshot->recovered_through_lsn_exclusive);
	write_u64_le(dst + 136, snapshot->conservative_commit_scn);
	write_u64_le(dst + 144, publisher_incarnation);
	write_u32_le(dst + 152, publisher_node);
	write_u32_le(dst + 156, (uint32)reason);
	write_u64_le(dst + 160, (uint64)snapshot->published_at_usec);
	write_u32_le(dst + 168, snapshot->identity.thread_claim_crc32c);
	write_u32_le(dst + 172, snapshot->checkpoint_record_crc32c);
	write_u64_le(dst + 176, snapshot->tail_last_record_lsn);
	write_u32_le(dst + 184, snapshot->tail_last_record_crc32c);
	write_u32_le(dst + 188, snapshot->recovered_last_record_crc32c);
	write_u16_le(dst + 192, snapshot->tail_validation_kind);
	write_u16_le(dst + 194, snapshot->checkpoint_source_kind);
	write_u16_le(dst + 196, snapshot->conservative_bound_kind);
	write_u64_le(dst + 208, snapshot->recovered_last_record_lsn);
	write_u32_le(dst + CONTROL_ROOT_RECORD_CRC_OFFSET,
				 control_root_crc(dst, CONTROL_ROOT_RECORD_CRC_OFFSET));
}

static ClusterControlRootResult
decode_record(const uint8 *src, uint16 expected_thread, const ControlRootHeader *header,
			  ClusterControlRootSnapshot *snapshot, uint32 *crc_out)
{
	ClusterControlRootResult result;
	uint64 publisher_incarnation;
	uint32 publisher_node;
	uint32 reason;
	uint32 stored_crc;

	memset(snapshot, 0, sizeof(*snapshot));
	if (bytes_are_zero(src, CLUSTER_CONTROL_ROOT_RECORD_BYTES)) {
		*crc_out = 0;
		return CLUSTER_CONTROL_ROOT_ABSENT;
	}
	if (memcmp(src, CONTROL_ROOT_RECORD_MAGIC, 4) != 0)
		return CLUSTER_CONTROL_ROOT_BAD_MAGIC;
	if (read_u16_le(src + 4) != CONTROL_ROOT_FORMAT_VERSION
		|| read_u16_le(src + 6) != CLUSTER_CONTROL_ROOT_RECORD_BYTES)
		return CLUSTER_CONTROL_ROOT_BAD_VERSION;
	stored_crc = read_u32_le(src + CONTROL_ROOT_RECORD_CRC_OFFSET);
	if (stored_crc != control_root_crc(src, CONTROL_ROOT_RECORD_CRC_OFFSET))
		return CLUSTER_CONTROL_ROOT_BAD_RECORD_CRC;
	if (src[11] != 0 || !bytes_are_zero(src + 88, 8) || !bytes_are_zero(src + 198, 10)
		|| !bytes_are_zero(src + 216, 288) || !bytes_are_zero(src + 508, 4))
		return CLUSTER_CONTROL_ROOT_BAD_RESERVED;
	if (read_u16_le(src + 8) != expected_thread)
		return CLUSTER_CONTROL_ROOT_IDENTITY_MISMATCH;

	snapshot->identity.system_identifier = read_u64_le(src + 32);
	memcpy(snapshot->identity.storage_uuid, src + 40, 16);
	memcpy(snapshot->identity.authority_uuid, src + 56, 16);
	snapshot->identity.origin_thread_id = read_u16_le(src + 8);
	snapshot->identity.origin_node_id = (int32)read_u32_le(src + 12);
	snapshot->identity.thread_claim_created_at = (int64)read_u64_le(src + 72);
	snapshot->identity.thread_claim_crc32c = read_u32_le(src + 168);
	snapshot->identity.origin_owner_incarnation = read_u64_le(src + 80);
	snapshot->identity.root_lineage_seq = read_u64_le(src + 24);
	snapshot->lifecycle = src[10];
	snapshot->root_flags = read_u32_le(src + 108);
	snapshot->root_publish_seq = read_u64_le(src + 16);
	snapshot->checkpoint_tli = read_u32_le(src + 96);
	snapshot->tail_tli = read_u32_le(src + 100);
	snapshot->recovered_tli = read_u32_le(src + 104);
	snapshot->checkpoint_source_kind = read_u16_le(src + 194);
	snapshot->tail_validation_kind = read_u16_le(src + 192);
	snapshot->conservative_bound_kind = read_u16_le(src + 196);
	snapshot->checkpoint_lower_lsn = read_u64_le(src + 112);
	snapshot->validated_tail_lsn_exclusive = read_u64_le(src + 120);
	snapshot->recovered_through_lsn_exclusive = read_u64_le(src + 128);
	snapshot->conservative_commit_scn = read_u64_le(src + 136);
	snapshot->tail_last_record_lsn = read_u64_le(src + 176);
	snapshot->recovered_last_record_lsn = read_u64_le(src + 208);
	snapshot->published_at_usec = (int64)read_u64_le(src + 160);
	snapshot->tail_last_record_crc32c = read_u32_le(src + 184);
	snapshot->checkpoint_record_crc32c = read_u32_le(src + 172);
	snapshot->recovered_last_record_crc32c = read_u32_le(src + 188);
	snapshot->lifecycle_reason = read_u32_le(src + 156);

	publisher_incarnation = read_u64_le(src + 144);
	publisher_node = read_u32_le(src + 152);
	reason = read_u32_le(src + 156);
	if (publisher_incarnation == 0 || publisher_node >= CLUSTER_CONTROL_ROOT_RECORD_COUNT
		|| reason < CLUSTER_CONTROL_ROOT_PUBLISH_MIGRATION_IMPORT
		|| reason > CLUSTER_CONTROL_ROOT_PUBLISH_COPY_REPAIR)
		return CLUSTER_CONTROL_ROOT_LIFECYCLE_INVALID;
	result = snapshot_validate(snapshot, expected_thread, header->system_identifier,
							   header->storage_uuid, header->authority_uuid);
	if (result != CLUSTER_CONTROL_ROOT_OK_PRIMARY)
		return result;
	*crc_out = stored_crc;
	return CLUSTER_CONTROL_ROOT_OK_PRIMARY;
}

static void
encode_header(ControlRootImage *image)
{
	uint8 *dst = image->bytes;

	memset(dst, 0, CLUSTER_CONTROL_ROOT_HEADER_BYTES);
	memcpy(dst, CONTROL_ROOT_HEADER_MAGIC, 4);
	write_u16_le(dst + 4, CONTROL_ROOT_FORMAT_VERSION);
	write_u16_le(dst + 6, CLUSTER_CONTROL_ROOT_HEADER_BYTES);
	write_u16_le(dst + 8, CLUSTER_CONTROL_ROOT_RECORD_BYTES);
	write_u16_le(dst + 10, CLUSTER_CONTROL_ROOT_RECORD_COUNT);
	write_u32_le(dst + 12, CONTROL_ROOT_ENDIAN_TAG);
	write_u64_le(dst + 16, image->header.file_txn_seq);
	write_u64_le(dst + 24, image->header.system_identifier);
	memcpy(dst + 32, image->header.storage_uuid, 16);
	memcpy(dst + 48, image->header.authority_uuid, 16);
	write_u64_le(dst + 64, CLUSTER_CONTROL_ROOT_FORMAT_FLAGS_V1);
	write_u16_le(dst + 72, CONTROL_ROOT_READER_VERSION);
	write_u16_le(dst + 74, CONTROL_ROOT_WRITER_VERSION);
	write_u32_le(dst + 76, image->header.activation_state);
	write_u64_le(dst + 80, (uint64)image->header.created_at_usec);
	write_u64_le(dst + 88, (uint64)image->header.published_at_usec);
	image->header.body_crc32c = control_root_crc(
		image->bytes + CONTROL_ROOT_BODY_OFFSET,
		CLUSTER_CONTROL_ROOT_FILE_BYTES - CONTROL_ROOT_BODY_OFFSET);
	write_u32_le(dst + 96, image->header.body_crc32c);
	memcpy(dst + 100, image->header.migration_round_sha256, PG_SHA256_DIGEST_LENGTH);
	memcpy(dst + 132, image->header.source_wal_state_sha256, PG_SHA256_DIGEST_LENGTH);
	write_u64_le(dst + 164, image->header.migration_prepare_generation);
	write_u64_le(dst + 172, image->header.migration_transition_epoch);
	write_u64_le(dst + 180, image->header.source_feature_bitmap);
	write_u64_le(dst + 188, image->header.target_feature_bitmap);
	image->header.header_crc32c = control_root_crc(dst, CONTROL_ROOT_HEADER_CRC_OFFSET);
	write_u32_le(dst + CONTROL_ROOT_HEADER_CRC_OFFSET, image->header.header_crc32c);
}

static ClusterControlRootResult
decode_image(ControlRootImage *image, const uint8 current_uuid[16], uint64 current_sysid)
{
	const uint8 *src = image->bytes;
	uint32 stored_crc;
	uint16 i;

	memset(&image->header, 0, sizeof(image->header));
	memset(image->records, 0, sizeof(image->records));
	memset(image->record_crc32c, 0, sizeof(image->record_crc32c));
	memset(image->present, 0, sizeof(image->present));
	if (memcmp(src, CONTROL_ROOT_HEADER_MAGIC, 4) != 0)
		return CLUSTER_CONTROL_ROOT_BAD_MAGIC;
	if (read_u16_le(src + 4) != CONTROL_ROOT_FORMAT_VERSION
		|| read_u16_le(src + 6) != CLUSTER_CONTROL_ROOT_HEADER_BYTES
		|| read_u16_le(src + 8) != CLUSTER_CONTROL_ROOT_RECORD_BYTES
		|| read_u16_le(src + 10) != CLUSTER_CONTROL_ROOT_RECORD_COUNT
		|| read_u16_le(src + 72) != CONTROL_ROOT_READER_VERSION
		|| read_u16_le(src + 74) != CONTROL_ROOT_WRITER_VERSION
		|| read_u64_le(src + 64) != CLUSTER_CONTROL_ROOT_FORMAT_FLAGS_V1)
		return CLUSTER_CONTROL_ROOT_BAD_VERSION;
	if (read_u32_le(src + 12) != CONTROL_ROOT_ENDIAN_TAG)
		return CLUSTER_CONTROL_ROOT_BAD_ENDIAN;
	stored_crc = read_u32_le(src + CONTROL_ROOT_HEADER_CRC_OFFSET);
	if (stored_crc != control_root_crc(src, CONTROL_ROOT_HEADER_CRC_OFFSET))
		return CLUSTER_CONTROL_ROOT_BAD_HEADER_CRC;
	if (!bytes_are_zero(src + 196, 308) || !bytes_are_zero(src + 508, 4))
		return CLUSTER_CONTROL_ROOT_BAD_RESERVED;
	image->header.file_txn_seq = read_u64_le(src + 16);
	image->header.system_identifier = read_u64_le(src + 24);
	memcpy(image->header.storage_uuid, src + 32, 16);
	memcpy(image->header.authority_uuid, src + 48, 16);
	image->header.activation_state = read_u32_le(src + 76);
	image->header.created_at_usec = (int64)read_u64_le(src + 80);
	image->header.published_at_usec = (int64)read_u64_le(src + 88);
	image->header.body_crc32c = read_u32_le(src + 96);
	memcpy(image->header.migration_round_sha256, src + 100, PG_SHA256_DIGEST_LENGTH);
	memcpy(image->header.source_wal_state_sha256, src + 132, PG_SHA256_DIGEST_LENGTH);
	image->header.migration_prepare_generation = read_u64_le(src + 164);
	image->header.migration_transition_epoch = read_u64_le(src + 172);
	image->header.source_feature_bitmap = read_u64_le(src + 180);
	image->header.target_feature_bitmap = read_u64_le(src + 188);
	image->header.header_crc32c = stored_crc;
	if (image->header.file_txn_seq == 0 || image->header.system_identifier == 0
		|| image->header.system_identifier != current_sysid
		|| memcmp(image->header.storage_uuid, current_uuid, 16) != 0
		|| !uuid_v4_valid(image->header.authority_uuid))
		return CLUSTER_CONTROL_ROOT_IDENTITY_MISMATCH;
	if (image->header.activation_state != CLUSTER_CONTROL_ROOT_ACTIVATION_PREPARED
		&& image->header.activation_state != CLUSTER_CONTROL_ROOT_ACTIVATION_ACTIVE)
		return CLUSTER_CONTROL_ROOT_LIFECYCLE_INVALID;
	if (bytes_are_zero(image->header.migration_round_sha256, PG_SHA256_DIGEST_LENGTH)
		|| bytes_are_zero(image->header.source_wal_state_sha256, PG_SHA256_DIGEST_LENGTH)
		|| image->header.migration_prepare_generation == 0
		|| !cluster_control_root_feature_bitmap_is_known(image->header.source_feature_bitmap)
		|| !cluster_control_root_feature_bitmap_is_known(image->header.target_feature_bitmap)
		|| (image->header.target_feature_bitmap
			& PGRAC_CONTROL_ROOT_FEATURE_RECOVERY_DUTY_IDENTITY_V1) == 0)
		return CLUSTER_CONTROL_ROOT_MIXED_VERSION;
	if (image->header.body_crc32c
		!= control_root_crc(src + CONTROL_ROOT_BODY_OFFSET,
						CLUSTER_CONTROL_ROOT_FILE_BYTES - CONTROL_ROOT_BODY_OFFSET))
		return CLUSTER_CONTROL_ROOT_BAD_BODY_CRC;

	for (i = 0; i < CLUSTER_CONTROL_ROOT_RECORD_COUNT; i++) {
		ClusterControlRootResult result = decode_record(
			src + CLUSTER_CONTROL_ROOT_HEADER_BYTES
				+ (size_t)i * CLUSTER_CONTROL_ROOT_RECORD_BYTES,
			(uint16)(i + 1), &image->header, &image->records[i],
			&image->record_crc32c[i]);

		if (result == CLUSTER_CONTROL_ROOT_ABSENT)
			continue;
		if (result != CLUSTER_CONTROL_ROOT_OK_PRIMARY)
			return result;
		image->present[i] = true;
	}
	return CLUSTER_CONTROL_ROOT_OK_PRIMARY;
}

static ClusterControlRootResult
read_exact_file(const char *path, uint8 *bytes)
{
	struct stat st;
	size_t done = 0;
	int fd;

	if (!regular_or_absent_nosymlink(path, true))
		return CLUSTER_CONTROL_ROOT_IO_ERROR;
	fd = OpenTransientFile(path, O_RDONLY | PG_BINARY);
	if (fd < 0)
		return errno == ENOENT ? CLUSTER_CONTROL_ROOT_ABSENT : CLUSTER_CONTROL_ROOT_IO_ERROR;
	if (fstat(fd, &st) != 0) {
		CloseTransientFile(fd);
		return CLUSTER_CONTROL_ROOT_IO_ERROR;
	}
	if (st.st_size != CLUSTER_CONTROL_ROOT_FILE_BYTES) {
		CloseTransientFile(fd);
		return CLUSTER_CONTROL_ROOT_BAD_SIZE;
	}
	while (done < CLUSTER_CONTROL_ROOT_FILE_BYTES) {
		ssize_t n = read(fd, bytes + done, CLUSTER_CONTROL_ROOT_FILE_BYTES - done);

		if (n <= 0) {
			CloseTransientFile(fd);
			return CLUSTER_CONTROL_ROOT_IO_ERROR;
		}
		done += (size_t)n;
	}
	if (CloseTransientFile(fd) != 0)
		return CLUSTER_CONTROL_ROOT_IO_ERROR;
	return CLUSTER_CONTROL_ROOT_OK_PRIMARY;
}

static ClusterControlRootResult
read_one_image(const char *path, const uint8 current_uuid[16], uint64 current_sysid,
			   ControlRootImage *image)
{
	ClusterControlRootResult result;

	result = read_exact_file(path, image->bytes);
	if (result != CLUSTER_CONTROL_ROOT_OK_PRIMARY)
		return result;
	return decode_image(image, current_uuid, current_sysid);
}

static bool
same_immutable_header(const ControlRootImage *left, const ControlRootImage *right)
{
	return left->header.system_identifier == right->header.system_identifier
		   && memcmp(left->header.storage_uuid, right->header.storage_uuid, 16) == 0
		   && memcmp(left->header.authority_uuid, right->header.authority_uuid, 16) == 0
		   && memcmp(left->header.migration_round_sha256,
					 right->header.migration_round_sha256, PG_SHA256_DIGEST_LENGTH) == 0
		   && memcmp(left->header.source_wal_state_sha256,
					 right->header.source_wal_state_sha256, PG_SHA256_DIGEST_LENGTH) == 0
		   && left->header.migration_prepare_generation
				  == right->header.migration_prepare_generation
		   && left->header.migration_transition_epoch
				  == right->header.migration_transition_epoch
		   && left->header.source_feature_bitmap == right->header.source_feature_bitmap
		   && left->header.target_feature_bitmap == right->header.target_feature_bitmap;
}

static ClusterControlRootResult
read_canonical_pair(ControlRootImage *primary, ControlRootImage *bak)
{
	char primary_path[MAXPGPATH];
	char bak_path[MAXPGPATH];
	uint8 current_uuid[16];
	uint64 current_sysid;
	ClusterControlRootResult primary_result;
	ClusterControlRootResult bak_result;

	if (!current_storage_uuid(current_uuid))
		return CLUSTER_CONTROL_ROOT_STORAGE_CONTRACT_UNVERIFIED;
	current_sysid = GetSystemIdentifier();
	if (current_sysid == 0)
		return CLUSTER_CONTROL_ROOT_IDENTITY_MISMATCH;
	if (!build_control_path(primary_path, sizeof(primary_path), CLUSTER_CONTROL_ROOT_REL_PATH)
		|| !build_control_path(bak_path, sizeof(bak_path), CLUSTER_CONTROL_ROOT_BAK_REL_PATH))
		return CLUSTER_CONTROL_ROOT_IO_ERROR;
	primary_result = read_one_image(primary_path, current_uuid, current_sysid, primary);
	bak_result = read_one_image(bak_path, current_uuid, current_sysid, bak);
	if (primary_result == CLUSTER_CONTROL_ROOT_OK_PRIMARY) {
		if (bak_result == CLUSTER_CONTROL_ROOT_OK_PRIMARY) {
			if (!same_immutable_header(primary, bak)
				|| bak->header.file_txn_seq > primary->header.file_txn_seq
				|| (bak->header.file_txn_seq == primary->header.file_txn_seq
					&& memcmp(primary->bytes, bak->bytes,
							  CLUSTER_CONTROL_ROOT_FILE_BYTES) != 0))
				return CLUSTER_CONTROL_ROOT_COPY_DIVERGENT;
			return CLUSTER_CONTROL_ROOT_OK_PRIMARY;
		}
		return CLUSTER_CONTROL_ROOT_OK_PRIMARY_DEGRADED;
	}
	if (bak_result == CLUSTER_CONTROL_ROOT_OK_PRIMARY)
		return CLUSTER_CONTROL_ROOT_OK_BAK_BLOCKED;
	if (primary_result != CLUSTER_CONTROL_ROOT_ABSENT)
		return primary_result;
	return bak_result;
}

static void
make_read_token(const ControlRootImage *image, uint16 thread_id, uint8 source,
				ClusterControlRootReadToken *token)
{
	const ClusterControlRootSnapshot *snapshot = &image->records[thread_id - 1];

	memset(token, 0, sizeof(*token));
	memcpy(token->authority_uuid, snapshot->identity.authority_uuid, 16);
	token->origin_thread_id = thread_id;
	token->source = source;
	token->lifecycle = (uint8)snapshot->lifecycle;
	token->root_lineage_seq = snapshot->identity.root_lineage_seq;
	token->file_txn_seq = image->header.file_txn_seq;
	token->root_publish_seq = snapshot->root_publish_seq;
	token->record_crc32c = image->record_crc32c[thread_id - 1];
	token->root_flags = snapshot->root_flags;
}

static bool
read_token_equal(const ClusterControlRootReadToken *left,
				 const ClusterControlRootReadToken *right)
{
	return left != NULL && right != NULL && memcmp(left, right, sizeof(*left)) == 0;
}

static bool
acquire_clusterwide_cf(LOCKMODE mode)
{
	if (!cluster_cf_lock(mode))
		return false;
	if (!cluster_cf_held_is_clusterwide(mode)) {
		(void)cluster_cf_unlock_confirmed(mode);
		return false;
	}
	return true;
}

static ClusterControlRootResult
release_cf(LOCKMODE mode, ClusterControlRootResult result)
{
	if (cluster_cf_unlock_confirmed(mode) != CLUSTER_CF_RELEASE_CONFIRMED)
		return CLUSTER_CONTROL_ROOT_RELEASE_UNCERTAIN;
	return result;
}

ClusterControlRootResult
cluster_control_root_read_canonical(uint16 origin_thread_id,
								const ClusterControlRootIdentity *expected_identity,
								ClusterControlRootReadMode mode,
								ClusterControlRootSnapshot *out_snapshot,
								ClusterControlRootReadToken *out_token)
{
	ControlRootImage *primary;
	ControlRootImage *bak;
	ClusterControlRootSnapshot snapshot;
	ClusterControlRootReadToken token;
	ClusterControlRootResult result;
	uint8 expected_storage[16];
	bool strong;

	if (out_snapshot != NULL)
		memset(out_snapshot, 0, sizeof(*out_snapshot));
	if (out_token != NULL)
		memset(out_token, 0, sizeof(*out_token));
	strong = mode == CLUSTER_CONTROL_ROOT_READ_STRONG;
	if (origin_thread_id == 0 || origin_thread_id > CLUSTER_CONTROL_ROOT_RECORD_COUNT
		|| (mode != CLUSTER_CONTROL_ROOT_READ_STRONG
			&& mode != CLUSTER_CONTROL_ROOT_READ_BOOTSTRAP_VALIDATE)
		|| (strong && expected_identity == NULL))
		return CLUSTER_CONTROL_ROOT_INVALID_ARGUMENT;
	if (expected_identity != NULL)
		memcpy(expected_storage, expected_identity->storage_uuid, 16);
	result = storage_contract_check(expected_identity != NULL ? expected_storage : NULL, strong);
	if (result != CLUSTER_CONTROL_ROOT_OK_PRIMARY)
		return result;
	if (strong && !acquire_clusterwide_cf(ShareLock))
		return CLUSTER_CONTROL_ROOT_LOCK_UNAVAILABLE;

	primary = palloc(sizeof(*primary));
	bak = palloc(sizeof(*bak));
	result = read_canonical_pair(primary, bak);
	if ((result == CLUSTER_CONTROL_ROOT_OK_PRIMARY
		 || result == CLUSTER_CONTROL_ROOT_OK_PRIMARY_DEGRADED)
		&& !primary->present[origin_thread_id - 1])
		result = CLUSTER_CONTROL_ROOT_ABSENT;
	if ((result == CLUSTER_CONTROL_ROOT_OK_PRIMARY
		 || result == CLUSTER_CONTROL_ROOT_OK_PRIMARY_DEGRADED)
		&& expected_identity != NULL
		&& !cluster_control_root_identity_equal(
			expected_identity, &primary->records[origin_thread_id - 1].identity))
		result = CLUSTER_CONTROL_ROOT_IDENTITY_MISMATCH;
	if (result == CLUSTER_CONTROL_ROOT_OK_PRIMARY
		|| result == CLUSTER_CONTROL_ROOT_OK_PRIMARY_DEGRADED) {
		snapshot = primary->records[origin_thread_id - 1];
		memset(&token, 0, sizeof(token));
		if (strong)
			make_read_token(primary, origin_thread_id, CONTROL_ROOT_SOURCE_PRIMARY, &token);
	}
	pfree(bak);
	pfree(primary);
	if (strong)
		result = release_cf(ShareLock, result);
	if (result == CLUSTER_CONTROL_ROOT_OK_PRIMARY
		|| result == CLUSTER_CONTROL_ROOT_OK_PRIMARY_DEGRADED) {
		if (out_snapshot != NULL)
			*out_snapshot = snapshot;
		if (strong && out_token != NULL)
			*out_token = token;
	}
	return result;
}

ClusterControlRootResult
cluster_control_root_lookup_owner_by_node_runtime(int32 old_node_id,
										  ClusterControlRootIdentity *out_identity,
										  ClusterControlRootSnapshot *out_snapshot,
										  ClusterControlRootReadToken *out_token)
{
	ControlRootImage *primary;
	ControlRootImage *bak;
	ClusterControlRootIdentity identity;
	ClusterControlRootSnapshot snapshot;
	ClusterControlRootReadToken token;
	ClusterControlRootResult result;
	uint16 thread_id;

	if (out_identity != NULL)
		memset(out_identity, 0, sizeof(*out_identity));
	if (out_snapshot != NULL)
		memset(out_snapshot, 0, sizeof(*out_snapshot));
	if (out_token != NULL)
		memset(out_token, 0, sizeof(*out_token));
	if (old_node_id < 0 || old_node_id >= CLUSTER_CONTROL_ROOT_RECORD_COUNT)
		return CLUSTER_CONTROL_ROOT_INVALID_ARGUMENT;
	thread_id = (uint16)(old_node_id + 1);
	result = storage_contract_check(NULL, true);
	if (result != CLUSTER_CONTROL_ROOT_OK_PRIMARY)
		return result;
	if (!acquire_clusterwide_cf(ShareLock))
		return CLUSTER_CONTROL_ROOT_LOCK_UNAVAILABLE;
	primary = palloc(sizeof(*primary));
	bak = palloc(sizeof(*bak));
	result = read_canonical_pair(primary, bak);
	if ((result == CLUSTER_CONTROL_ROOT_OK_PRIMARY
		 || result == CLUSTER_CONTROL_ROOT_OK_PRIMARY_DEGRADED)
		&& (!primary->present[thread_id - 1]
			|| primary->records[thread_id - 1].identity.origin_node_id != old_node_id))
		result = CLUSTER_CONTROL_ROOT_IDENTITY_MISMATCH;
	if (result == CLUSTER_CONTROL_ROOT_OK_PRIMARY
		|| result == CLUSTER_CONTROL_ROOT_OK_PRIMARY_DEGRADED) {
		identity = primary->records[thread_id - 1].identity;
		snapshot = primary->records[thread_id - 1];
		make_read_token(primary, thread_id, CONTROL_ROOT_SOURCE_PRIMARY, &token);
	}
	pfree(bak);
	pfree(primary);
	result = release_cf(ShareLock, result);
	if (result == CLUSTER_CONTROL_ROOT_OK_PRIMARY
		|| result == CLUSTER_CONTROL_ROOT_OK_PRIMARY_DEGRADED) {
		if (out_identity != NULL)
			*out_identity = identity;
		if (out_snapshot != NULL)
			*out_snapshot = snapshot;
		if (out_token != NULL)
			*out_token = token;
	}
	return result;
}

static bool
encode_round(const ClusterControlRootMigrationRoundV1 *round, uint8 bytes[80])
{
	if (round == NULL || memcmp(round->magic, "PCRM", 4) != 0 || round->version != 1
		|| round->bytes != sizeof(*round) || round->prepare_generation == 0
		|| round->coordinator_incarnation == 0
		|| round->coordinator_node_id >= CLUSTER_CONTROL_ROOT_RECORD_COUNT
		|| round->reserved76 != 0
		|| !cluster_control_root_feature_bitmap_is_known(round->source_feature_bitmap)
		|| !cluster_control_root_feature_bitmap_is_known(round->target_feature_bitmap)
		|| (round->target_feature_bitmap
			& PGRAC_CONTROL_ROOT_FEATURE_RECOVERY_DUTY_IDENTITY_V1) == 0)
		return false;
	memset(bytes, 0, 80);
	memcpy(bytes, "PCRM", 4);
	write_u16_le(bytes + 4, 1);
	write_u16_le(bytes + 6, 80);
	write_u64_le(bytes + 8, round->prepare_generation);
	write_u64_le(bytes + 16, round->transition_epoch);
	write_u64_le(bytes + 24, round->source_feature_bitmap);
	write_u64_le(bytes + 32, round->target_feature_bitmap);
	write_u64_le(bytes + 40, round->admitted_bitmap_low);
	write_u64_le(bytes + 48, round->admitted_bitmap_high);
	write_u64_le(bytes + 56, round->capability_sample_digest);
	write_u64_le(bytes + 64, round->coordinator_incarnation);
	write_u32_le(bytes + 72, round->coordinator_node_id);
	return true;
}

static bool
migration_image_validate(const ClusterControlRootMigrationImage *image,
						 const ClusterControlRootMigrationRoundV1 *round)
{
	uint32 assigned = 0;
	uint16 i;

	if (image == NULL || round == NULL || image->system_identifier == 0
		|| image->system_identifier != GetSystemIdentifier() || image->reserved52 != 0
		|| image->assigned_record_count > CLUSTER_CONTROL_ROOT_RECORD_COUNT
		|| !uuid_v4_valid(image->authority_uuid))
		return false;
	for (i = 0; i < CLUSTER_CONTROL_ROOT_RECORD_COUNT; i++) {
		const ClusterControlRootSnapshot *snapshot = &image->records[i];

		if (snapshot->identity.system_identifier == 0) {
			if (!bytes_are_zero(snapshot, sizeof(*snapshot)))
				return false;
			continue;
		}
		if (snapshot_validate(snapshot, (uint16)(i + 1), image->system_identifier,
							 image->storage_uuid, image->authority_uuid)
			!= CLUSTER_CONTROL_ROOT_OK_PRIMARY)
			return false;
		if (snapshot->identity.root_lineage_seq != 1)
			return false;
		if (snapshot->lifecycle_reason != CLUSTER_CONTROL_ROOT_PUBLISH_MIGRATION_IMPORT)
			return false;
		assigned++;
	}
	return assigned == image->assigned_record_count;
}

static bool
read_thread_claim_exact(uint16 thread_id, int32 node_id,
						const ClusterControlRootIdentity *expected)
{
	ClusterWalThreadClaim claim;
	char dirname[MAXPGPATH];
	char dirpath[MAXPGPATH];
	char path[MAXPGPATH];
	struct stat st;
	size_t done = 0;
	int fd;
	int written;

	cluster_wal_thread_dir_name(thread_id, dirname, sizeof(dirname));
	written = snprintf(dirpath, sizeof(dirpath), "%s/%s", cluster_wal_threads_dir, dirname);
	if (dirname[0] == '\0' || written <= 0 || (size_t)written >= sizeof(dirpath)
		|| lstat(dirpath, &st) != 0 || !S_ISDIR(st.st_mode))
		return false;
	written = snprintf(path, sizeof(path), "%s/%s", dirpath,
					   CLUSTER_WAL_THREAD_CLAIM_FILENAME);
	if (written <= 0 || (size_t)written >= sizeof(path)
		|| !regular_or_absent_nosymlink(path, false))
		return false;
	fd = OpenTransientFile(path, O_RDONLY | PG_BINARY);
	if (fd < 0 || fstat(fd, &st) != 0 || !S_ISREG(st.st_mode)
		|| st.st_size != sizeof(claim)) {
		if (fd >= 0)
			CloseTransientFile(fd);
		return false;
	}
	while (done < sizeof(claim)) {
		ssize_t n = read(fd, (uint8 *)&claim + done, sizeof(claim) - done);

		if (n <= 0) {
			CloseTransientFile(fd);
			return false;
		}
		done += (size_t)n;
	}
	if (CloseTransientFile(fd) != 0
		|| !cluster_wal_thread_claim_validate(&claim, thread_id, node_id, NULL)
		|| !bytes_are_zero(claim._pad_12, sizeof(claim._pad_12))
		|| !bytes_are_zero(claim._reserved_24, sizeof(claim._reserved_24))
		|| !bytes_are_zero(claim._pad_36, sizeof(claim._pad_36)))
		return false;
	return claim.created_at == expected->thread_claim_created_at
		   && claim.crc == expected->thread_claim_crc32c;
}

static ClusterControlRootResult
read_source_wal_state(const ClusterControlRootSnapshot *expected_records,
					  const uint8 *expected_hash, uint8 hash_out[PG_SHA256_DIGEST_LENGTH])
{
	uint8 *first;
	uint8 *second;
	char path[MAXPGPATH];
	struct stat st;
	uint16 bad_thread;
	const char *reason;
	size_t done;
	int fd;
	int pass;
	uint16 i;
	uint32 assigned = 0;
	uint32 expected_assigned = 0;

	if (cluster_wal_threads_dir == NULL || cluster_wal_threads_dir[0] == '\0'
		|| lstat(cluster_wal_threads_dir, &st) != 0 || !S_ISDIR(st.st_mode)
		|| snprintf(path, sizeof(path), "%s/%s", cluster_wal_threads_dir,
					CLUSTER_WAL_STATE_FILENAME) <= 0
		|| !regular_or_absent_nosymlink(path, false))
		return CLUSTER_CONTROL_ROOT_IO_ERROR;
	first = palloc(CLUSTER_WAL_STATE_FILE_SIZE);
	second = palloc(CLUSTER_WAL_STATE_FILE_SIZE);
	for (pass = 0; pass < 2; pass++) {
		uint8 *dst = pass == 0 ? first : second;

		fd = OpenTransientFile(path, O_RDONLY | PG_BINARY);
		if (fd < 0 || fstat(fd, &st) != 0 || st.st_size != CLUSTER_WAL_STATE_FILE_SIZE) {
			if (fd >= 0)
				CloseTransientFile(fd);
			pfree(second);
			pfree(first);
			return CLUSTER_CONTROL_ROOT_BAD_SIZE;
		}
		done = 0;
		while (done < CLUSTER_WAL_STATE_FILE_SIZE) {
			ssize_t n = read(fd, dst + done, CLUSTER_WAL_STATE_FILE_SIZE - done);

			if (n <= 0) {
				CloseTransientFile(fd);
				pfree(second);
				pfree(first);
				return CLUSTER_CONTROL_ROOT_IO_ERROR;
			}
			done += (size_t)n;
		}
		if (CloseTransientFile(fd) != 0
			|| !cluster_wal_state_image_validate(dst, CLUSTER_WAL_STATE_FILE_SIZE,
											 &bad_thread, &reason)) {
			pfree(second);
			pfree(first);
			return CLUSTER_CONTROL_ROOT_HASH_MISMATCH;
		}
	}
	if (memcmp(first, second, CLUSTER_WAL_STATE_FILE_SIZE) != 0) {
		pfree(second);
		pfree(first);
		return CLUSTER_CONTROL_ROOT_HASH_MISMATCH;
	}
	for (i = 0; i < CLUSTER_WAL_STATE_SLOT_COUNT; i++) {
		ClusterWalStateSlot slot;
		ClusterWalSlotVerdict verdict;
		const ClusterControlRootSnapshot *expected = &expected_records[i];

		if (expected->identity.system_identifier != 0)
			expected_assigned++;
		memcpy(&slot, first + CLUSTER_WAL_STATE_SLOT_OFFSET(i + 1), sizeof(slot));
		verdict = cluster_wal_state_slot_classify(&slot, (uint16)(i + 1), -1, NULL);
		if (verdict == CLUSTER_WAL_SLOT_EMPTY) {
			if (expected->identity.system_identifier != 0) {
				pfree(second);
				pfree(first);
				return CLUSTER_CONTROL_ROOT_HASH_MISMATCH;
			}
			continue;
		}
		if (verdict != CLUSTER_WAL_SLOT_OK || slot.state != CLUSTER_WAL_SLOT_STATE_STOPPED
			|| slot.checkpoint_redo_lsn == 0 || slot.merge_recovered_lsn != 0
			|| expected->identity.system_identifier == 0
			|| expected->identity.origin_node_id != slot.node_id
			|| !read_thread_claim_exact((uint16)(i + 1), slot.node_id,
									&expected->identity)) {
			pfree(second);
			pfree(first);
			return CLUSTER_CONTROL_ROOT_HASH_MISMATCH;
		}
		assigned++;
	}
	if (assigned != expected_assigned) {
		pfree(second);
		pfree(first);
		return CLUSTER_CONTROL_ROOT_HASH_MISMATCH;
	}
	if (!control_root_sha256(first, CLUSTER_WAL_STATE_FILE_SIZE, hash_out)) {
		pfree(second);
		pfree(first);
		return CLUSTER_CONTROL_ROOT_IO_ERROR;
	}
	pfree(second);
	pfree(first);
	if (expected_hash != NULL
		&& memcmp(expected_hash, hash_out, PG_SHA256_DIGEST_LENGTH) != 0)
		return CLUSTER_CONTROL_ROOT_HASH_MISMATCH;
	return CLUSTER_CONTROL_ROOT_OK_PRIMARY;
}

static bool
fsync_parent_global(void)
{
	char path[MAXPGPATH];
	int fd;
	bool fsync_ok;
	bool close_ok;

	if (snprintf(path, sizeof(path), "%s/global", cluster_shared_data_dir) <= 0)
		return false;
	fd = OpenTransientFile(path, O_RDONLY | PG_BINARY);
	if (fd < 0)
		return false;
	fsync_ok = pg_fsync(fd) == 0;
	close_ok = CloseTransientFile(fd) == 0;
	return fsync_ok && close_ok;
}

static bool
write_durable_image(const char *final_path, const uint8 *bytes)
{
	uint8 random_bytes[8];
	char temp_path[MAXPGPATH];
	char suffix[17];
	const char *base;
	size_t done = 0;
	int fd = -1;
	int i;
	bool created = false;
	bool renamed = false;
	bool ok = false;

	if (!regular_or_absent_nosymlink(final_path, true) || !pg_strong_random(random_bytes, 8))
		return false;
	for (i = 0; i < 8; i++)
		snprintf(suffix + i * 2, 3, "%02x", random_bytes[i]);
	suffix[16] = '\0';
	base = strrchr(final_path, '/');
	if (base == NULL)
		return false;
	if (snprintf(temp_path, sizeof(temp_path), "%.*s/%s.tmp.%d.%ld.%s",
				 (int)(base - final_path), final_path, base + 1, cluster_node_id,
				 (long)getpid(), suffix) <= 0)
		return false;
	fd = BasicOpenFilePerm(temp_path, O_WRONLY | O_CREAT | O_EXCL | PG_BINARY, 0600);
	if (fd < 0)
		return false;
	created = true;
	while (done < CLUSTER_CONTROL_ROOT_FILE_BYTES) {
		ssize_t n = write(fd, bytes + done, CLUSTER_CONTROL_ROOT_FILE_BYTES - done);

		if (n <= 0)
			goto cleanup;
		done += (size_t)n;
	}
	if (pg_fsync(fd) != 0) {
		(void)CloseTransientFile(fd);
		fd = -1;
		goto cleanup_closed;
	}
	if (CloseTransientFile(fd) != 0) {
		fd = -1;
		goto cleanup_closed;
	}
	fd = -1;
	if (durable_rename(temp_path, final_path, LOG) != 0)
		goto cleanup_closed;
	renamed = true;
	if (!fsync_parent_global())
		goto cleanup_closed;
	{
		uint8 *readback = palloc(CLUSTER_CONTROL_ROOT_FILE_BYTES);
		ClusterControlRootResult result = read_exact_file(final_path, readback);

		ok = result == CLUSTER_CONTROL_ROOT_OK_PRIMARY
			 && memcmp(readback, bytes, CLUSTER_CONTROL_ROOT_FILE_BYTES) == 0;
		pfree(readback);
	}
	return ok;

cleanup:
	(void)CloseTransientFile(fd);
	fd = -1;
cleanup_closed:
	if (created && !renamed)
		(void)unlink(temp_path);
	return false;
}

static bool
publish_initial_image(const ControlRootImage *image)
{
	char primary[MAXPGPATH];
	char bak[MAXPGPATH];
	struct stat st;

	if (!build_control_path(primary, sizeof(primary), CLUSTER_CONTROL_ROOT_REL_PATH)
		|| !build_control_path(bak, sizeof(bak), CLUSTER_CONTROL_ROOT_BAK_REL_PATH))
		return false;
	if (lstat(primary, &st) == 0 || errno != ENOENT)
		return false;
	if (lstat(bak, &st) == 0 || errno != ENOENT)
		return false;
	return write_durable_image(bak, image->bytes)
		   && write_durable_image(primary, image->bytes);
}

static bool
publish_updated_image(const ControlRootImage *old_image, const ControlRootImage *new_image)
{
	char primary[MAXPGPATH];
	char bak[MAXPGPATH];

	if (!build_control_path(primary, sizeof(primary), CLUSTER_CONTROL_ROOT_REL_PATH)
		|| !build_control_path(bak, sizeof(bak), CLUSTER_CONTROL_ROOT_BAK_REL_PATH))
		return false;
	return write_durable_image(bak, old_image->bytes)
		   && write_durable_image(primary, new_image->bytes);
}

static void
make_file_token(const ControlRootImage *image, ClusterControlRootFileToken *token)
{
	memset(token, 0, sizeof(*token));
	memcpy(token->authority_uuid, image->header.authority_uuid, 16);
	token->file_txn_seq = image->header.file_txn_seq;
	token->body_crc32c = image->header.body_crc32c;
	token->header_crc32c = image->header.header_crc32c;
	token->activation_state = image->header.activation_state;
	token->format_version = CONTROL_ROOT_FORMAT_VERSION;
	token->record_count = CLUSTER_CONTROL_ROOT_RECORD_COUNT;
	token->system_identifier = image->header.system_identifier;
	if (!control_root_sha256(image->bytes, CLUSTER_CONTROL_ROOT_FILE_BYTES,
						 token->image_sha256))
		memset(token, 0, sizeof(*token));
}

static bool
file_token_equal(const ClusterControlRootFileToken *left,
				 const ClusterControlRootFileToken *right)
{
	return left != NULL && right != NULL && memcmp(left, right, sizeof(*left)) == 0;
}

ClusterControlRootResult
cluster_control_root_create_prepared(const ClusterControlRootMigrationImage *image,
									 const ClusterControlRootMigrationRoundV1 *round,
									 ClusterControlRootFileToken *out_token)
{
	ControlRootImage *root;
	ControlRootImage *primary;
	ControlRootImage *bak;
	ClusterControlRootFileToken token;
	ClusterControlRootResult result;
	uint8 round_bytes[80];
	uint8 current_uuid[16];
	uint16 i;

	if (out_token != NULL)
		memset(out_token, 0, sizeof(*out_token));
	if (!encode_round(round, round_bytes) || !migration_image_validate(image, round))
		return CLUSTER_CONTROL_ROOT_INVALID_ARGUMENT;
	result = storage_contract_check(image->storage_uuid, true);
	if (result != CLUSTER_CONTROL_ROOT_OK_PRIMARY)
		return result;
	if (!current_storage_uuid(current_uuid)
		|| memcmp(current_uuid, image->storage_uuid, 16) != 0)
		return CLUSTER_CONTROL_ROOT_IDENTITY_MISMATCH;
	if (!acquire_clusterwide_cf(ExclusiveLock))
		return CLUSTER_CONTROL_ROOT_LOCK_UNAVAILABLE;

	root = palloc(sizeof(*root));
	memset(root, 0, sizeof(*root));
	root->header.file_txn_seq = 1;
	root->header.system_identifier = image->system_identifier;
	memcpy(root->header.storage_uuid, image->storage_uuid, 16);
	memcpy(root->header.authority_uuid, image->authority_uuid, 16);
	root->header.activation_state = CLUSTER_CONTROL_ROOT_ACTIVATION_PREPARED;
	root->header.created_at_usec = image->created_at_usec;
	root->header.published_at_usec = GetCurrentTimestamp();
	root->header.migration_prepare_generation = round->prepare_generation;
	root->header.migration_transition_epoch = round->transition_epoch;
	root->header.source_feature_bitmap = round->source_feature_bitmap;
	root->header.target_feature_bitmap = round->target_feature_bitmap;
	if (!control_root_sha256(round_bytes, sizeof(round_bytes),
						 root->header.migration_round_sha256))
		result = CLUSTER_CONTROL_ROOT_IO_ERROR;
	else
		result = read_source_wal_state(image->records, NULL,
								 root->header.source_wal_state_sha256);
	if (result == CLUSTER_CONTROL_ROOT_OK_PRIMARY) {
		for (i = 0; i < CLUSTER_CONTROL_ROOT_RECORD_COUNT; i++) {
			if (image->records[i].identity.system_identifier == 0)
				continue;
			root->records[i] = image->records[i];
			root->records[i].published_at_usec = GetCurrentTimestamp();
			root->records[i].lifecycle_reason =
				CLUSTER_CONTROL_ROOT_PUBLISH_MIGRATION_IMPORT;
			encode_record(root->bytes + CLUSTER_CONTROL_ROOT_HEADER_BYTES
						  + (size_t)i * CLUSTER_CONTROL_ROOT_RECORD_BYTES,
					  &root->records[i], round->coordinator_incarnation,
					  round->coordinator_node_id,
					  CLUSTER_CONTROL_ROOT_PUBLISH_MIGRATION_IMPORT);
			root->present[i] = true;
		}
		encode_header(root);
		if (!publish_initial_image(root))
			result = CLUSTER_CONTROL_ROOT_IO_ERROR;
	}
	if (result == CLUSTER_CONTROL_ROOT_OK_PRIMARY) {
		primary = palloc(sizeof(*primary));
		bak = palloc(sizeof(*bak));
		result = read_canonical_pair(primary, bak);
		if (result != CLUSTER_CONTROL_ROOT_OK_PRIMARY
			|| memcmp(primary->bytes, root->bytes, CLUSTER_CONTROL_ROOT_FILE_BYTES) != 0
			|| memcmp(bak->bytes, root->bytes, CLUSTER_CONTROL_ROOT_FILE_BYTES) != 0)
			result = CLUSTER_CONTROL_ROOT_POSTREAD_FAILED;
		else {
			make_file_token(primary, &token);
			if (token.file_txn_seq == 0)
				result = CLUSTER_CONTROL_ROOT_IO_ERROR;
		}
		pfree(bak);
		pfree(primary);
	}
	pfree(root);
	result = release_cf(ExclusiveLock, result);
	if (result == CLUSTER_CONTROL_ROOT_OK_PRIMARY && out_token != NULL)
		*out_token = token;
	return result;
}

ClusterControlRootResult
cluster_control_root_activate_prepared(const ClusterControlRootFileToken *expected_token,
									   const uint8 expected_round_sha256[32],
									   ClusterControlRootFileToken *out_token)
{
	ControlRootImage *primary;
	ControlRootImage *bak;
	ControlRootImage *updated;
	ClusterControlRootFileToken actual;
	ClusterControlRootFileToken token;
	ClusterControlRootResult result;
	uint8 source_hash[PG_SHA256_DIGEST_LENGTH];

	if (out_token != NULL)
		memset(out_token, 0, sizeof(*out_token));
	if (expected_token == NULL || expected_round_sha256 == NULL
		|| expected_token->file_txn_seq == 0
		|| expected_token->activation_state != CLUSTER_CONTROL_ROOT_ACTIVATION_PREPARED
		|| expected_token->format_version != CONTROL_ROOT_FORMAT_VERSION
		|| expected_token->record_count != CLUSTER_CONTROL_ROOT_RECORD_COUNT
		|| bytes_are_zero(expected_round_sha256, PG_SHA256_DIGEST_LENGTH))
		return CLUSTER_CONTROL_ROOT_INVALID_ARGUMENT;
	result = storage_contract_check(NULL, true);
	if (result != CLUSTER_CONTROL_ROOT_OK_PRIMARY)
		return result;
	if (!acquire_clusterwide_cf(ExclusiveLock))
		return CLUSTER_CONTROL_ROOT_LOCK_UNAVAILABLE;
	primary = palloc(sizeof(*primary));
	bak = palloc(sizeof(*bak));
	updated = palloc(sizeof(*updated));
	result = read_canonical_pair(primary, bak);
	if (result == CLUSTER_CONTROL_ROOT_OK_PRIMARY
		|| result == CLUSTER_CONTROL_ROOT_OK_PRIMARY_DEGRADED) {
		make_file_token(primary, &actual);
		if (!file_token_equal(expected_token, &actual))
			result = CLUSTER_CONTROL_ROOT_STALE_TOKEN;
		else if (memcmp(primary->header.migration_round_sha256, expected_round_sha256,
						PG_SHA256_DIGEST_LENGTH) != 0)
			result = CLUSTER_CONTROL_ROOT_MIGRATION_ROUND_MISMATCH;
		else
			result = read_source_wal_state(primary->records,
								   primary->header.source_wal_state_sha256,
								   source_hash);
	}
	if (result == CLUSTER_CONTROL_ROOT_OK_PRIMARY
		|| result == CLUSTER_CONTROL_ROOT_OK_PRIMARY_DEGRADED) {
		memcpy(updated, primary, sizeof(*updated));
		if (updated->header.file_txn_seq == UINT64_MAX)
			result = CLUSTER_CONTROL_ROOT_SEQUENCE_EXHAUSTED;
		else {
			updated->header.file_txn_seq++;
			updated->header.activation_state = CLUSTER_CONTROL_ROOT_ACTIVATION_ACTIVE;
			updated->header.published_at_usec = GetCurrentTimestamp();
			encode_header(updated);
			if (!publish_updated_image(primary, updated))
				result = CLUSTER_CONTROL_ROOT_IO_ERROR;
			else {
				ControlRootImage *readback = palloc(sizeof(*readback));
				uint8 current_uuid[16];
				char path[MAXPGPATH];

				if (!current_storage_uuid(current_uuid)
					|| !build_control_path(path, sizeof(path),
									   CLUSTER_CONTROL_ROOT_REL_PATH))
					result = CLUSTER_CONTROL_ROOT_POSTREAD_FAILED;
				else
					result = read_one_image(path, current_uuid, GetSystemIdentifier(), readback);
				if (result == CLUSTER_CONTROL_ROOT_OK_PRIMARY
					&& memcmp(readback->bytes, updated->bytes,
							  CLUSTER_CONTROL_ROOT_FILE_BYTES) != 0)
					result = CLUSTER_CONTROL_ROOT_POSTREAD_FAILED;
				if (result == CLUSTER_CONTROL_ROOT_OK_PRIMARY)
					make_file_token(readback, &token);
				pfree(readback);
			}
		}
	}
	pfree(updated);
	pfree(bak);
	pfree(primary);
	result = release_cf(ExclusiveLock, result);
	if (result == CLUSTER_CONTROL_ROOT_OK_PRIMARY && out_token != NULL)
		*out_token = token;
	return result;
}

static uint64
reason_mask(ClusterControlRootPublishReason reason)
{
	switch (reason) {
	case CLUSTER_CONTROL_ROOT_PUBLISH_THREAD_OPEN:
	case CLUSTER_CONTROL_ROOT_PUBLISH_OWNER_REJOIN:
		return UINT64_C(0x3b);
	case CLUSTER_CONTROL_ROOT_PUBLISH_THREAD_CLEAN_CLOSE:
		return UINT64_C(0x39);
	case CLUSTER_CONTROL_ROOT_PUBLISH_FAILURE_DUTY_OPEN:
		return UINT64_C(0xb1);
	case CLUSTER_CONTROL_ROOT_PUBLISH_FAILURE_TAIL_VALIDATED:
		return CLUSTER_CONTROL_ROOT_PATCH_TAIL;
	case CLUSTER_CONTROL_ROOT_PUBLISH_RECOVERY_PROGRESS:
		return CLUSTER_CONTROL_ROOT_PATCH_RECOVERY_PROGRESS;
	case CLUSTER_CONTROL_ROOT_PUBLISH_RECOVERY_COMPLETE:
		return UINT64_C(0x21);
	case CLUSTER_CONTROL_ROOT_PUBLISH_THREAD_RETIRE:
		return CLUSTER_CONTROL_ROOT_PATCH_LIFECYCLE;
	case CLUSTER_CONTROL_ROOT_PUBLISH_CONSERVATIVE_BOUND:
		return CLUSTER_CONTROL_ROOT_PATCH_CONSERVATIVE_BOUND;
	case CLUSTER_CONTROL_ROOT_PUBLISH_CHECKPOINT_ADVANCE:
		return UINT64_C(0x38);
	case CLUSTER_CONTROL_ROOT_PUBLISH_FPW_STICKY:
		return CLUSTER_CONTROL_ROOT_PATCH_FPW_STICKY;
	default:
		return 0;
	}
}

static bool
patch_shape_valid(const ClusterControlRootPatch *patch,
				  ClusterControlRootPublishReason reason)
{
	ClusterControlRootSnapshot allowed;
	uint64 mask = reason_mask(reason);

	if (patch == NULL || mask == 0 || patch->mask != mask || patch->reserved20 != 0
		|| patch->reserved24 != 0
		|| patch->expected_lifecycle < CLUSTER_CONTROL_ROOT_LIFECYCLE_OPEN
		|| patch->expected_lifecycle > CLUSTER_CONTROL_ROOT_LIFECYCLE_RETIRED
		|| (patch->expected_flags_mask & ~CLUSTER_CONTROL_ROOT_FLAGS_V1) != 0
		|| (patch->expected_flags_value & ~patch->expected_flags_mask) != 0
		|| !snapshot_reserved_zero(&patch->desired)
		|| (patch->desired.root_flags & ~CLUSTER_CONTROL_ROOT_FLAGS_V1) != 0)
		return false;
	if (reason == CLUSTER_CONTROL_ROOT_PUBLISH_OWNER_REJOIN
		&& (patch->expected_lifecycle
				!= CLUSTER_CONTROL_ROOT_LIFECYCLE_RECOVERY_COMPLETE
			|| patch->desired.lifecycle != CLUSTER_CONTROL_ROOT_LIFECYCLE_OPEN
			|| patch->desired.identity.origin_owner_incarnation == 0
			|| patch->desired.identity.root_lineage_seq == 0))
		return false;
	memset(&allowed, 0, sizeof(allowed));
	if ((mask & CLUSTER_CONTROL_ROOT_PATCH_LIFECYCLE) != 0)
		allowed.lifecycle = patch->desired.lifecycle;
	if ((mask & CLUSTER_CONTROL_ROOT_PATCH_OWNER_LINEAGE) != 0) {
		allowed.identity.origin_owner_incarnation =
			patch->desired.identity.origin_owner_incarnation;
		allowed.identity.root_lineage_seq = patch->desired.identity.root_lineage_seq;
	}
	if ((mask & CLUSTER_CONTROL_ROOT_PATCH_CHECKPOINT) != 0) {
		allowed.checkpoint_tli = patch->desired.checkpoint_tli;
		allowed.checkpoint_source_kind = patch->desired.checkpoint_source_kind;
		allowed.checkpoint_lower_lsn = patch->desired.checkpoint_lower_lsn;
		allowed.checkpoint_record_crc32c = patch->desired.checkpoint_record_crc32c;
	}
	if ((mask & CLUSTER_CONTROL_ROOT_PATCH_TAIL) != 0) {
		allowed.tail_tli = patch->desired.tail_tli;
		allowed.tail_validation_kind = patch->desired.tail_validation_kind;
		allowed.validated_tail_lsn_exclusive = patch->desired.validated_tail_lsn_exclusive;
		allowed.tail_last_record_lsn = patch->desired.tail_last_record_lsn;
		allowed.tail_last_record_crc32c = patch->desired.tail_last_record_crc32c;
	}
	if ((mask & CLUSTER_CONTROL_ROOT_PATCH_RECOVERY_PROGRESS) != 0) {
		allowed.recovered_tli = patch->desired.recovered_tli;
		allowed.recovered_through_lsn_exclusive =
			patch->desired.recovered_through_lsn_exclusive;
		allowed.recovered_last_record_lsn = patch->desired.recovered_last_record_lsn;
		allowed.recovered_last_record_crc32c =
			patch->desired.recovered_last_record_crc32c;
	}
	if ((mask & CLUSTER_CONTROL_ROOT_PATCH_CONSERVATIVE_BOUND) != 0) {
		allowed.conservative_bound_kind = patch->desired.conservative_bound_kind;
		allowed.conservative_commit_scn = patch->desired.conservative_commit_scn;
	}
	if ((mask & (CLUSTER_CONTROL_ROOT_PATCH_CHECKPOINT | CLUSTER_CONTROL_ROOT_PATCH_TAIL
				 | CLUSTER_CONTROL_ROOT_PATCH_RECOVERY_PROGRESS
				 | CLUSTER_CONTROL_ROOT_PATCH_FPW_STICKY
				 | CLUSTER_CONTROL_ROOT_PATCH_CONSERVATIVE_BOUND)) != 0)
		allowed.root_flags = patch->desired.root_flags;
	return memcmp(&allowed, &patch->desired, sizeof(allowed)) == 0;
}

static void
merge_flag_group(uint32 *flags, uint32 desired, uint32 group)
{
	*flags = (*flags & ~group) | (desired & group);
}

static bool
apply_patch(ClusterControlRootSnapshot *snapshot, const ClusterControlRootPatch *patch)
{
	uint64 mask = patch->mask;
	uint32 old_flags = snapshot->root_flags;

	if ((mask & CLUSTER_CONTROL_ROOT_PATCH_LIFECYCLE) != 0)
		snapshot->lifecycle = patch->desired.lifecycle;
	if ((mask & CLUSTER_CONTROL_ROOT_PATCH_OWNER_LINEAGE) != 0) {
		snapshot->identity.origin_owner_incarnation =
			patch->desired.identity.origin_owner_incarnation;
		snapshot->identity.root_lineage_seq = patch->desired.identity.root_lineage_seq;
	}
	if ((mask & CLUSTER_CONTROL_ROOT_PATCH_CHECKPOINT) != 0) {
		snapshot->checkpoint_tli = patch->desired.checkpoint_tli;
		snapshot->checkpoint_source_kind = patch->desired.checkpoint_source_kind;
		snapshot->checkpoint_lower_lsn = patch->desired.checkpoint_lower_lsn;
		snapshot->checkpoint_record_crc32c = patch->desired.checkpoint_record_crc32c;
		merge_flag_group(&snapshot->root_flags, patch->desired.root_flags,
					 CLUSTER_CONTROL_ROOT_FLAG_CHECKPOINT_VALID);
	}
	if ((mask & CLUSTER_CONTROL_ROOT_PATCH_TAIL) != 0) {
		snapshot->tail_tli = patch->desired.tail_tli;
		snapshot->tail_validation_kind = patch->desired.tail_validation_kind;
		snapshot->validated_tail_lsn_exclusive = patch->desired.validated_tail_lsn_exclusive;
		snapshot->tail_last_record_lsn = patch->desired.tail_last_record_lsn;
		snapshot->tail_last_record_crc32c = patch->desired.tail_last_record_crc32c;
		merge_flag_group(&snapshot->root_flags, patch->desired.root_flags,
					 CLUSTER_CONTROL_ROOT_FLAG_TAIL_VALID
						 | CLUSTER_CONTROL_ROOT_FLAG_TAIL_LAST_RECORD_VALID);
	}
	if ((mask & CLUSTER_CONTROL_ROOT_PATCH_RECOVERY_PROGRESS) != 0) {
		snapshot->recovered_tli = patch->desired.recovered_tli;
		snapshot->recovered_through_lsn_exclusive =
			patch->desired.recovered_through_lsn_exclusive;
		snapshot->recovered_last_record_lsn = patch->desired.recovered_last_record_lsn;
		snapshot->recovered_last_record_crc32c =
			patch->desired.recovered_last_record_crc32c;
		merge_flag_group(&snapshot->root_flags, patch->desired.root_flags,
					 CLUSTER_CONTROL_ROOT_FLAG_RECOVERED_VALID
						 | CLUSTER_CONTROL_ROOT_FLAG_RECOVERED_LAST_RECORD_VALID);
	}
	if ((mask & CLUSTER_CONTROL_ROOT_PATCH_FPW_STICKY) != 0) {
		if ((old_flags & CLUSTER_CONTROL_ROOT_FLAG_FPW_WAS_OFF) != 0
			&& (patch->desired.root_flags & CLUSTER_CONTROL_ROOT_FLAG_FPW_WAS_OFF) == 0)
			return false;
		merge_flag_group(&snapshot->root_flags, patch->desired.root_flags,
					 CLUSTER_CONTROL_ROOT_FLAG_FPW_WAS_OFF);
	}
	if ((mask & CLUSTER_CONTROL_ROOT_PATCH_CONSERVATIVE_BOUND) != 0) {
		snapshot->conservative_bound_kind = patch->desired.conservative_bound_kind;
		snapshot->conservative_commit_scn = patch->desired.conservative_commit_scn;
		merge_flag_group(&snapshot->root_flags, patch->desired.root_flags,
					 CLUSTER_CONTROL_ROOT_FLAG_CONSERVATIVE_SCN_VALID);
	}
	return true;
}

ClusterControlRootResult
cluster_control_root_compare_and_publish(const ClusterControlRootReadToken *expected_token,
									 const ClusterControlRootPatch *patch,
									 ClusterControlRootPublishReason reason,
									 ClusterControlRootSnapshot *out_snapshot,
									 ClusterControlRootReadToken *out_token)
{
	ControlRootImage *primary;
	ControlRootImage *bak;
	ControlRootImage *updated;
	ClusterControlRootReadToken actual;
	ClusterControlRootReadToken token;
	ClusterControlRootSnapshot snapshot;
	ClusterControlRootResult result;
	uint16 thread_id;

	if (out_snapshot != NULL)
		memset(out_snapshot, 0, sizeof(*out_snapshot));
	if (out_token != NULL)
		memset(out_token, 0, sizeof(*out_token));
	if (expected_token == NULL || expected_token->source != CONTROL_ROOT_SOURCE_PRIMARY
		|| expected_token->origin_thread_id == 0
		|| expected_token->origin_thread_id > CLUSTER_CONTROL_ROOT_RECORD_COUNT
		|| expected_token->reserved20 != 0 || expected_token->reserved32 != 0
		|| !patch_shape_valid(patch, reason))
		return CLUSTER_CONTROL_ROOT_INVALID_ARGUMENT;
	thread_id = expected_token->origin_thread_id;
	result = storage_contract_check(NULL, true);
	if (result != CLUSTER_CONTROL_ROOT_OK_PRIMARY)
		return result;
	if (!acquire_clusterwide_cf(ExclusiveLock))
		return CLUSTER_CONTROL_ROOT_LOCK_UNAVAILABLE;
	primary = palloc(sizeof(*primary));
	bak = palloc(sizeof(*bak));
	updated = palloc(sizeof(*updated));
	result = read_canonical_pair(primary, bak);
	if ((result == CLUSTER_CONTROL_ROOT_OK_PRIMARY
		 || result == CLUSTER_CONTROL_ROOT_OK_PRIMARY_DEGRADED)
		&& !primary->present[thread_id - 1])
		result = CLUSTER_CONTROL_ROOT_ABSENT;
	if (result == CLUSTER_CONTROL_ROOT_OK_PRIMARY
		|| result == CLUSTER_CONTROL_ROOT_OK_PRIMARY_DEGRADED) {
		make_read_token(primary, thread_id, CONTROL_ROOT_SOURCE_PRIMARY, &actual);
		if (!read_token_equal(expected_token, &actual))
			result = CLUSTER_CONTROL_ROOT_STALE_TOKEN;
		else if (primary->records[thread_id - 1].lifecycle != patch->expected_lifecycle
				 || (primary->records[thread_id - 1].root_flags
					 & patch->expected_flags_mask) != patch->expected_flags_value)
			result = CLUSTER_CONTROL_ROOT_CAS_CONFLICT;
		else if (reason == CLUSTER_CONTROL_ROOT_PUBLISH_OWNER_REJOIN
				 && (primary->records[thread_id - 1].identity.root_lineage_seq
						 == UINT64_MAX
					 || patch->desired.identity.root_lineage_seq
							!= primary->records[thread_id - 1].identity.root_lineage_seq + 1
					 || patch->desired.identity.origin_owner_incarnation
							<= primary->records[thread_id - 1]
								   .identity.origin_owner_incarnation))
			result = CLUSTER_CONTROL_ROOT_CAS_CONFLICT;
	}
	if (result == CLUSTER_CONTROL_ROOT_OK_PRIMARY
		|| result == CLUSTER_CONTROL_ROOT_OK_PRIMARY_DEGRADED) {
		memcpy(updated, primary, sizeof(*updated));
		snapshot = updated->records[thread_id - 1];
		if (updated->header.file_txn_seq == UINT64_MAX
			|| snapshot.root_publish_seq == UINT64_MAX)
			result = CLUSTER_CONTROL_ROOT_SEQUENCE_EXHAUSTED;
		else if (!apply_patch(&snapshot, patch))
			result = CLUSTER_CONTROL_ROOT_INVALID_ARGUMENT;
		else {
			snapshot.root_publish_seq++;
			snapshot.published_at_usec = GetCurrentTimestamp();
			snapshot.lifecycle_reason = reason;
			result = snapshot_validate(&snapshot, thread_id,
								   updated->header.system_identifier,
								   updated->header.storage_uuid,
								   updated->header.authority_uuid);
		}
		if (result == CLUSTER_CONTROL_ROOT_OK_PRIMARY) {
			updated->records[thread_id - 1] = snapshot;
			encode_record(updated->bytes + CLUSTER_CONTROL_ROOT_HEADER_BYTES
						  + (size_t)(thread_id - 1) * CLUSTER_CONTROL_ROOT_RECORD_BYTES,
					  &snapshot, snapshot.identity.origin_owner_incarnation,
					  (uint32)cluster_node_id, reason);
			updated->header.file_txn_seq++;
			updated->header.published_at_usec = snapshot.published_at_usec;
			encode_header(updated);
			if (!publish_updated_image(primary, updated))
				result = CLUSTER_CONTROL_ROOT_IO_ERROR;
			else {
				ControlRootImage *readback = palloc(sizeof(*readback));
				uint8 current_uuid[16];
				char path[MAXPGPATH];

				if (!current_storage_uuid(current_uuid)
					|| !build_control_path(path, sizeof(path),
									   CLUSTER_CONTROL_ROOT_REL_PATH))
					result = CLUSTER_CONTROL_ROOT_POSTREAD_FAILED;
				else
					result = read_one_image(path, current_uuid, GetSystemIdentifier(), readback);
				if (result == CLUSTER_CONTROL_ROOT_OK_PRIMARY
					&& memcmp(readback->bytes, updated->bytes,
							  CLUSTER_CONTROL_ROOT_FILE_BYTES) != 0)
					result = CLUSTER_CONTROL_ROOT_POSTREAD_FAILED;
				if (result == CLUSTER_CONTROL_ROOT_OK_PRIMARY) {
					snapshot = readback->records[thread_id - 1];
					make_read_token(readback, thread_id, CONTROL_ROOT_SOURCE_PRIMARY,
								&token);
				}
				pfree(readback);
			}
		}
	}
	pfree(updated);
	pfree(bak);
	pfree(primary);
	result = release_cf(ExclusiveLock, result);
	if (result == CLUSTER_CONTROL_ROOT_OK_PRIMARY) {
		if (out_snapshot != NULL)
			*out_snapshot = snapshot;
		if (out_token != NULL)
			*out_token = token;
	}
	return result;
}

ClusterControlRootResult
cluster_control_root_revalidate(const ClusterControlRootReadToken *token,
								const ClusterControlRootIdentity *expected_identity,
								ClusterControlRootSnapshot *out_snapshot)
{
	ClusterControlRootReadToken fresh;
	ClusterControlRootSnapshot snapshot;
	ClusterControlRootResult result;

	if (out_snapshot != NULL)
		memset(out_snapshot, 0, sizeof(*out_snapshot));
	if (token == NULL || expected_identity == NULL)
		return CLUSTER_CONTROL_ROOT_INVALID_ARGUMENT;
	result = cluster_control_root_read_canonical(token->origin_thread_id, expected_identity,
										 CLUSTER_CONTROL_ROOT_READ_STRONG, &snapshot,
										 &fresh);
	if (result != CLUSTER_CONTROL_ROOT_OK_PRIMARY
		&& result != CLUSTER_CONTROL_ROOT_OK_PRIMARY_DEGRADED)
		return result;
	if (!read_token_equal(token, &fresh))
		return CLUSTER_CONTROL_ROOT_STALE_TOKEN;
	if (out_snapshot != NULL)
		*out_snapshot = snapshot;
	return result;
}

ClusterControlRootResult
cluster_control_root_discard_inactive(const ClusterControlRootFileToken *expected_token,
									  const uint8 expected_round_sha256[32])
{
	/* P1 deliberately exposes no cutover path.  The R4 abort callback and its
	 * both-admissions-closed proof arrive at P2-P5; until then this API is
	 * fail-closed before CF or storage I/O. */
	(void)expected_token;
	(void)expected_round_sha256;
	return CLUSTER_CONTROL_ROOT_INVALID_ARGUMENT;
}
