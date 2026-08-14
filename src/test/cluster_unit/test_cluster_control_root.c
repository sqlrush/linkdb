/*-------------------------------------------------------------------------
 *
 * test_cluster_control_root.c
 *	  RF-ROOT P1 tests for the survivor-readable control-root carrier.
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include <errno.h>
#include <fcntl.h>
#include <stdlib.h>
#include <sys/stat.h>
#include <unistd.h>

#include "access/xlog.h"
#include "cluster/cluster_cf_enqueue.h"
#include "cluster/cluster_cf_storage.h"
#include "cluster/cluster_control_root.h"
#include "cluster/cluster_wal_state.h"
#include "cluster/cluster_wal_thread.h"
#include "cluster/storage/cluster_shared_fs.h"
#include "common/cryptohash.h"
#include "common/sha2.h"
#include "storage/fd.h"
#include "utils/timestamp.h"

#undef printf
#undef fprintf
#undef snprintf
#undef sprintf
#undef vsnprintf
#undef vfprintf
#undef vprintf
#undef vsprintf
#undef strerror
#undef strerror_r

#include "unit_test.h"

UT_DEFINE_GLOBALS();

#define TEST_SYSID UINT64_C(0x0123456789abcdef)

char *cluster_shared_data_dir = NULL;
char *cluster_wal_threads_dir = NULL;
char *DataDir = NULL;
int cluster_node_id = 0;

static char test_root[MAXPGPATH];
static char test_wal_root[MAXPGPATH];
static ClusterCfContractState test_contract = CLUSTER_CF_CONTRACT_CROSSNODE_VERIFIED;
static int test_node_count = 4;
static bool test_local_probe = true;
static bool test_cf_grant = true;
static bool test_cf_clusterwide = true;
static bool test_cf_release_confirmed = true;
static int test_cf_lock_calls = 0;
static int test_durable_rename_calls = 0;
static bool test_fail_primary_rename = false;
static TimestampTz test_now = INT64_C(1700000000000000);

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

int
OpenTransientFile(const char *fileName, int fileFlags)
{
	return open(fileName, fileFlags, 0600);
}

int
BasicOpenFilePerm(const char *fileName, int fileFlags, mode_t fileMode)
{
	return open(fileName, fileFlags, fileMode);
}

int
CloseTransientFile(int fd)
{
	return close(fd);
}

int
pg_fsync(int fd)
{
	return fsync(fd);
}

int
durable_rename(const char *oldfile, const char *newfile, int elevel pg_attribute_unused())
{
	test_durable_rename_calls++;
	if (test_fail_primary_rename
		&& strstr(newfile, CLUSTER_CONTROL_ROOT_REL_PATH) != NULL
		&& strstr(newfile, ".bak") == NULL) {
		errno = EIO;
		return -1;
	}
	return rename(oldfile, newfile);
}

bool
pg_strong_random(void *buf, size_t len)
{
	static uint8 seed = 0x31;
	uint8 *bytes = buf;
	size_t i;

	for (i = 0; i < len; i++)
		bytes[i] = seed++;
	return true;
}

TimestampTz
GetCurrentTimestamp(void)
{
	return ++test_now;
}

uint64
GetSystemIdentifier(void)
{
	return TEST_SYSID;
}

int
cluster_conf_node_count(void)
{
	return test_node_count;
}

void
cluster_shared_fs_get_storage_uuid(char *out, size_t outlen)
{
	strlcpy(out, "00112233445566778899aabbccddeeff", outlen);
}

ClusterCfContractState
cluster_cf_contract_load(const char *pgdata pg_attribute_unused())
{
	return test_contract;
}

bool
cluster_cf_storage_write_allowed(ClusterCfContractState state, bool multi_node)
{
	return !multi_node || state == CLUSTER_CF_CONTRACT_CROSSNODE_VERIFIED;
}

bool
cluster_cf_storage_probe_local(void)
{
	return test_local_probe;
}

bool
cluster_cf_lock(LOCKMODE mode pg_attribute_unused())
{
	test_cf_lock_calls++;
	return test_cf_grant;
}

bool
cluster_cf_held_is_clusterwide(LOCKMODE mode pg_attribute_unused())
{
	return test_cf_grant && test_cf_clusterwide;
}

ClusterCfReleaseResult
cluster_cf_unlock_confirmed(LOCKMODE mode pg_attribute_unused())
{
	return test_cf_release_confirmed ? CLUSTER_CF_RELEASE_CONFIRMED
									 : CLUSTER_CF_RELEASE_UNCONFIRMED;
}

static void
put_u16_le(uint8 *dst, uint16 value)
{
	dst[0] = (uint8)value;
	dst[1] = (uint8)(value >> 8);
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

static uint32
image_crc(const uint8 *bytes, size_t len)
{
	pg_crc32c crc;

	INIT_CRC32C(crc);
	COMP_CRC32C(crc, bytes, len);
	FIN_CRC32C(crc);
	return (uint32)crc;
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
round_sha256(const ClusterControlRootMigrationRoundV1 *round,
			 uint8 out[PG_SHA256_DIGEST_LENGTH])
{
	uint8 bytes[80];

	memset(bytes, 0, sizeof(bytes));
	memcpy(bytes, "PCRM", 4);
	put_u16_le(bytes + 4, 1);
	put_u16_le(bytes + 6, 80);
	put_u64_le(bytes + 8, round->prepare_generation);
	put_u64_le(bytes + 16, round->transition_epoch);
	put_u64_le(bytes + 24, round->source_feature_bitmap);
	put_u64_le(bytes + 32, round->target_feature_bitmap);
	put_u64_le(bytes + 40, round->admitted_bitmap_low);
	put_u64_le(bytes + 48, round->admitted_bitmap_high);
	put_u64_le(bytes + 56, round->capability_sample_digest);
	put_u64_le(bytes + 64, round->coordinator_incarnation);
	put_u32_le(bytes + 72, round->coordinator_node_id);
	sha256_bytes(bytes, sizeof(bytes), out);
}

static void
path_for(char *dst, size_t dstlen, const char *rel)
{
	snprintf(dst, dstlen, "%s/%s", test_root, rel);
}

static void
wipe_root_files(void)
{
	char path[MAXPGPATH];

	path_for(path, sizeof(path), CLUSTER_CONTROL_ROOT_REL_PATH);
	unlink(path);
	path_for(path, sizeof(path), CLUSTER_CONTROL_ROOT_BAK_REL_PATH);
	unlink(path);
	test_contract = CLUSTER_CF_CONTRACT_CROSSNODE_VERIFIED;
	test_node_count = 4;
	test_local_probe = true;
	test_cf_grant = true;
	test_cf_clusterwide = true;
	test_cf_release_confirmed = true;
	test_cf_lock_calls = 0;
	test_durable_rename_calls = 0;
	test_fail_primary_rename = false;
}

static void
write_all_or_abort(const char *path, const void *buf, size_t len)
{
	const uint8 *bytes = buf;
	size_t done = 0;
	int fd = open(path, O_WRONLY | O_CREAT | O_TRUNC, 0600);

	if (fd < 0)
		abort();
	while (done < len) {
		ssize_t n = write(fd, bytes + done, len - done);

		if (n <= 0)
			abort();
		done += (size_t)n;
	}
	close(fd);
}

static void
read_all_or_abort(const char *path, void *buf, size_t len)
{
	uint8 *bytes = buf;
	size_t done = 0;
	int fd = open(path, O_RDONLY);

	if (fd < 0)
		abort();
	while (done < len) {
		ssize_t n = read(fd, bytes + done, len - done);

		if (n <= 0)
			abort();
		done += (size_t)n;
	}
	close(fd);
}

static void
build_source_wal_state(void)
{
	uint8 bytes[CLUSTER_WAL_STATE_FILE_SIZE];
	ClusterWalStateHeader header;
	ClusterWalStateSlot slot;
	ClusterWalThreadClaim claim;
	char path[MAXPGPATH];
	char thread_dir[MAXPGPATH];

	memset(bytes, 0, sizeof(bytes));
	cluster_wal_state_header_fill(&header, INT64_C(1699999999000000));
	memcpy(bytes, &header, sizeof(header));
	cluster_wal_state_slot_fill(&slot, 1, 0, CLUSTER_WAL_SLOT_STATE_STOPPED, 1,
								INT64_C(1699999999000001),
								INT64_C(1699999999000002), UINT64_C(0x1000000), 1);
	slot.checkpoint_redo_lsn = UINT64_C(0x1000000);
	slot.crc = cluster_wal_state_block_crc(&slot);
	memcpy(bytes + CLUSTER_WAL_STATE_SLOT_OFFSET(1), &slot, sizeof(slot));
	snprintf(path, sizeof(path), "%s/%s", test_wal_root, CLUSTER_WAL_STATE_FILENAME);
	write_all_or_abort(path, bytes, sizeof(bytes));
	cluster_wal_thread_claim_fill(&claim, 1, 0, INT64_C(1699999999000001));
	snprintf(thread_dir, sizeof(thread_dir), "%s/thread_1", test_wal_root);
	if (mkdir(thread_dir, 0700) != 0 && errno != EEXIST)
		abort();
	snprintf(path, sizeof(path), "%s/%s", thread_dir,
			 CLUSTER_WAL_THREAD_CLAIM_FILENAME);
	write_all_or_abort(path, &claim, sizeof(claim));
}

static void
fill_identity(ClusterControlRootIdentity *identity)
{
	ClusterWalThreadClaim claim;
	int i;

	memset(identity, 0, sizeof(*identity));
	identity->system_identifier = TEST_SYSID;
	for (i = 0; i < 16; i++) {
		identity->storage_uuid[i] = (uint8)(i * 0x11);
		identity->authority_uuid[i] = (uint8)(0xa0 + i);
	}
	identity->authority_uuid[6] = 0x46;
	identity->authority_uuid[8] = 0x8a;
	identity->origin_thread_id = 1;
	identity->origin_node_id = 0;
	cluster_wal_thread_claim_fill(&claim, 1, 0, INT64_C(1699999999000001));
	identity->thread_claim_created_at = claim.created_at;
	identity->thread_claim_crc32c = claim.crc;
	identity->origin_owner_incarnation = UINT64_C(0x1122334455667788);
	identity->root_lineage_seq = 1;
}

static void
build_migration(ClusterControlRootMigrationImage *image,
				ClusterControlRootMigrationRoundV1 *round)
{
	ClusterControlRootSnapshot *record;

	memset(image, 0, sizeof(*image));
	image->system_identifier = TEST_SYSID;
	fill_identity(&image->records[0].identity);
	memcpy(image->storage_uuid, image->records[0].identity.storage_uuid, 16);
	memcpy(image->authority_uuid, image->records[0].identity.authority_uuid, 16);
	image->created_at_usec = INT64_C(1699999999000002);
	image->assigned_record_count = 1;
	record = &image->records[0];
	record->lifecycle = CLUSTER_CONTROL_ROOT_LIFECYCLE_CLOSED;
	record->root_flags = CLUSTER_CONTROL_ROOT_FLAG_CLAIM_VALID
						 | CLUSTER_CONTROL_ROOT_FLAG_CHECKPOINT_VALID
						 | CLUSTER_CONTROL_ROOT_FLAG_TAIL_VALID
						 | CLUSTER_CONTROL_ROOT_FLAG_RECOVERED_VALID;
	record->root_publish_seq = 1;
	record->checkpoint_tli = 1;
	record->tail_tli = 1;
	record->recovered_tli = 1;
	record->checkpoint_source_kind = CLUSTER_CONTROL_ROOT_CHECKPOINT_NATIVE_V1;
	record->tail_validation_kind = CLUSTER_CONTROL_ROOT_TAIL_WAL_RECORD_SCAN_V1;
	record->checkpoint_lower_lsn = UINT64_C(0x1000000);
	record->validated_tail_lsn_exclusive = UINT64_C(0x1000000);
	record->recovered_through_lsn_exclusive = UINT64_C(0x1000000);
	record->published_at_usec = image->created_at_usec;
	record->checkpoint_record_crc32c = UINT32_C(0x33445566);
	record->lifecycle_reason = CLUSTER_CONTROL_ROOT_PUBLISH_MIGRATION_IMPORT;

	memset(round, 0, sizeof(*round));
	memcpy(round->magic, "PCRM", 4);
	round->version = 1;
	round->bytes = sizeof(*round);
	round->prepare_generation = 1;
	round->transition_epoch = 7;
	round->target_feature_bitmap = PGRAC_CONTROL_ROOT_FEATURE_RECOVERY_DUTY_IDENTITY_V1;
	round->admitted_bitmap_low = 1;
	round->capability_sample_digest = UINT64_C(0x8877665544332211);
	round->coordinator_incarnation = UINT64_C(0x7766554433221100);
	round->coordinator_node_id = 0;
}

static ClusterControlRootResult
create_prepared(ClusterControlRootMigrationImage *image,
				ClusterControlRootMigrationRoundV1 *round,
				ClusterControlRootFileToken *token)
{
	build_migration(image, round);
	return cluster_control_root_create_prepared(image, round, token);
}

static void
force_first_record_lineage(uint64 lineage)
{
	uint8 bytes[CLUSTER_CONTROL_ROOT_FILE_BYTES];
	uint8 *record = bytes + CLUSTER_CONTROL_ROOT_HEADER_BYTES;
	char primary[MAXPGPATH];
	char bak[MAXPGPATH];

	path_for(primary, sizeof(primary), CLUSTER_CONTROL_ROOT_REL_PATH);
	path_for(bak, sizeof(bak), CLUSTER_CONTROL_ROOT_BAK_REL_PATH);
	read_all_or_abort(primary, bytes, sizeof(bytes));
	put_u64_le(record + 24, lineage);
	put_u32_le(record + 504, image_crc(record, 504));
	put_u32_le(bytes + 96,
			   image_crc(bytes + CLUSTER_CONTROL_ROOT_HEADER_BYTES,
						 sizeof(bytes) - CLUSTER_CONTROL_ROOT_HEADER_BYTES));
	put_u32_le(bytes + 504, image_crc(bytes, 504));
	write_all_or_abort(primary, bytes, sizeof(bytes));
	write_all_or_abort(bak, bytes, sizeof(bytes));
}

static void
build_owner_rejoin_patch(const ClusterControlRootSnapshot *snapshot,
						 uint64 new_incarnation, uint64 new_lineage,
						 ClusterControlRootPatch *patch)
{
	memset(patch, 0, sizeof(*patch));
	patch->mask = UINT64_C(0x3b);
	patch->expected_lifecycle = CLUSTER_CONTROL_ROOT_LIFECYCLE_RECOVERY_COMPLETE;
	patch->desired.lifecycle = CLUSTER_CONTROL_ROOT_LIFECYCLE_OPEN;
	patch->desired.identity.origin_owner_incarnation = new_incarnation;
	patch->desired.identity.root_lineage_seq = new_lineage;
	patch->desired.root_flags = snapshot->root_flags;
	patch->desired.checkpoint_tli = snapshot->checkpoint_tli;
	patch->desired.checkpoint_source_kind = snapshot->checkpoint_source_kind;
	patch->desired.checkpoint_lower_lsn = snapshot->checkpoint_lower_lsn;
	patch->desired.checkpoint_record_crc32c = snapshot->checkpoint_record_crc32c;
	patch->desired.tail_tli = snapshot->tail_tli;
	patch->desired.tail_validation_kind = snapshot->tail_validation_kind;
	patch->desired.validated_tail_lsn_exclusive = snapshot->validated_tail_lsn_exclusive;
	patch->desired.tail_last_record_lsn = snapshot->tail_last_record_lsn;
	patch->desired.tail_last_record_crc32c = snapshot->tail_last_record_crc32c;
	patch->desired.recovered_tli = snapshot->recovered_tli;
	patch->desired.recovered_through_lsn_exclusive =
		snapshot->recovered_through_lsn_exclusive;
	patch->desired.recovered_last_record_lsn = snapshot->recovered_last_record_lsn;
	patch->desired.recovered_last_record_crc32c = snapshot->recovered_last_record_crc32c;
}

static void
setup_fixture(void)
{
	char tmpl[MAXPGPATH];
	char path[MAXPGPATH];

	strlcpy(tmpl, "/tmp/pgrac_control_root_XXXXXX", sizeof(tmpl));
	if (mkdtemp(tmpl) == NULL)
		abort();
	strlcpy(test_root, tmpl, sizeof(test_root));
	cluster_shared_data_dir = test_root;
	DataDir = test_root;

	snprintf(path, sizeof(path), "%s/global", test_root);
	if (mkdir(path, 0700) != 0)
		abort();
	snprintf(test_wal_root, sizeof(test_wal_root), "%s/wal", test_root);
	if (mkdir(test_wal_root, 0700) != 0)
		abort();
	cluster_wal_threads_dir = test_wal_root;
	build_source_wal_state();
}

UT_TEST(test_abi_identity_and_features)
{
	ClusterControlRootIdentity left;
	ClusterControlRootIdentity right;
	uint64 known = PGRAC_CONTROL_ROOT_FEATURE_WAL_REUSE_V1
				   | PGRAC_CONTROL_ROOT_FEATURE_PAGE_STABLE_BASE_V1
				   | PGRAC_CONTROL_ROOT_FEATURE_SPACE_METADATA_V1
				   | PGRAC_CONTROL_ROOT_FEATURE_CONSERVATIVE_COMMIT_SCN_V1
				   | PGRAC_CONTROL_ROOT_FEATURE_RECOVERY_DUTY_IDENTITY_V1
				   | PGRAC_CONTROL_ROOT_FEATURE_RECOVERY_SERIAL_V1
				   | PGRAC_CONTROL_ROOT_FEATURE_EXTERNAL_FENCE_V1;

	UT_ASSERT_EQ(CLUSTER_CONTROL_ROOT_FILE_BYTES, 66048);
	UT_ASSERT_EQ(CLUSTER_CONTROL_ROOT_FORMAT_FLAGS_V1, UINT64_C(0x0d));
	UT_ASSERT_EQ(CLUSTER_CONTROL_ROOT_FLAGS_V1, UINT32_C(0x1fd));
	UT_ASSERT_EQ(CLUSTER_CONTROL_ROOT_PATCH_ALL_V1, UINT64_C(0xfb));
	fill_identity(&left);
	right = left;
	UT_ASSERT(cluster_control_root_identity_equal(&left, &right));
	right.root_lineage_seq++;
	UT_ASSERT(!cluster_control_root_identity_equal(&left, &right));
	UT_ASSERT(cluster_control_root_feature_bitmap_is_known(known));
	UT_ASSERT(!cluster_control_root_feature_bitmap_is_known(UINT64_C(1) << 63));
}

UT_TEST(test_invalid_argument_precedes_authority_io)
{
	ClusterControlRootMigrationImage image;
	ClusterControlRootMigrationRoundV1 round;
	ClusterControlRootFileToken token;

	wipe_root_files();
	build_migration(&image, &round);
	round.reserved76 = 1;
	memset(&token, 0xee, sizeof(token));
	UT_ASSERT_EQ(cluster_control_root_create_prepared(&image, &round, &token),
				 CLUSTER_CONTROL_ROOT_INVALID_ARGUMENT);
	UT_ASSERT_EQ(test_cf_lock_calls, 0);
	UT_ASSERT_EQ(token.file_txn_seq, 0);
}

UT_TEST(test_create_and_read_primary)
{
	ClusterControlRootMigrationImage image;
	ClusterControlRootMigrationRoundV1 round;
	ClusterControlRootFileToken file_token;
	ClusterControlRootSnapshot snapshot;
	ClusterControlRootReadToken read_token;
	uint8 primary[CLUSTER_CONTROL_ROOT_FILE_BYTES];
	uint8 bak[CLUSTER_CONTROL_ROOT_FILE_BYTES];
	char primary_path[MAXPGPATH];
	char bak_path[MAXPGPATH];

	wipe_root_files();
	UT_ASSERT_EQ(create_prepared(&image, &round, &file_token), CLUSTER_CONTROL_ROOT_OK_PRIMARY);
	UT_ASSERT_EQ(file_token.file_txn_seq, 1);
	UT_ASSERT_EQ(file_token.activation_state, CLUSTER_CONTROL_ROOT_ACTIVATION_PREPARED);
	path_for(primary_path, sizeof(primary_path), CLUSTER_CONTROL_ROOT_REL_PATH);
	path_for(bak_path, sizeof(bak_path), CLUSTER_CONTROL_ROOT_BAK_REL_PATH);
	read_all_or_abort(primary_path, primary, sizeof(primary));
	read_all_or_abort(bak_path, bak, sizeof(bak));
	UT_ASSERT(memcmp(primary, bak, sizeof(primary)) == 0);
	UT_ASSERT_EQ(image_crc(primary + CLUSTER_CONTROL_ROOT_HEADER_BYTES,
					   sizeof(primary) - CLUSTER_CONTROL_ROOT_HEADER_BYTES),
				 file_token.body_crc32c);

	memset(&snapshot, 0xee, sizeof(snapshot));
	memset(&read_token, 0xee, sizeof(read_token));
	UT_ASSERT_EQ(cluster_control_root_read_canonical(1, &image.records[0].identity,
											  CLUSTER_CONTROL_ROOT_READ_STRONG,
											  &snapshot, &read_token),
				 CLUSTER_CONTROL_ROOT_OK_PRIMARY);
	UT_ASSERT(cluster_control_root_identity_equal(&snapshot.identity,
											 &image.records[0].identity));
	UT_ASSERT_EQ(read_token.file_txn_seq, 1);
	UT_ASSERT_EQ(read_token.origin_thread_id, 1);
}

UT_TEST(test_bootstrap_read_never_returns_authority_token)
{
	ClusterControlRootMigrationImage image;
	ClusterControlRootMigrationRoundV1 round;
	ClusterControlRootFileToken file_token;
	ClusterControlRootSnapshot snapshot;
	ClusterControlRootReadToken read_token;

	wipe_root_files();
	UT_ASSERT_EQ(create_prepared(&image, &round, &file_token), CLUSTER_CONTROL_ROOT_OK_PRIMARY);
	test_cf_lock_calls = 0;
	memset(&read_token, 0xee, sizeof(read_token));
	UT_ASSERT_EQ(cluster_control_root_read_canonical(1, NULL,
											  CLUSTER_CONTROL_ROOT_READ_BOOTSTRAP_VALIDATE,
											  &snapshot, &read_token),
				 CLUSTER_CONTROL_ROOT_OK_PRIMARY);
	UT_ASSERT_EQ(test_cf_lock_calls, 0);
	UT_ASSERT_EQ(read_token.file_txn_seq, 0);
}

UT_TEST(test_valid_bak_blocks_corrupt_primary)
{
	ClusterControlRootMigrationImage image;
	ClusterControlRootMigrationRoundV1 round;
	ClusterControlRootFileToken file_token;
	ClusterControlRootSnapshot snapshot;
	ClusterControlRootReadToken read_token;
	char path[MAXPGPATH];
	int fd;
	uint8 byte;

	wipe_root_files();
	UT_ASSERT_EQ(create_prepared(&image, &round, &file_token), CLUSTER_CONTROL_ROOT_OK_PRIMARY);
	path_for(path, sizeof(path), CLUSTER_CONTROL_ROOT_REL_PATH);
	fd = open(path, O_RDWR);
	UT_ASSERT(fd >= 0);
	UT_ASSERT_EQ(pread(fd, &byte, 1, 0), 1);
	byte ^= 0xff;
	UT_ASSERT_EQ(pwrite(fd, &byte, 1, 0), 1);
	close(fd);
	memset(&snapshot, 0xee, sizeof(snapshot));
	memset(&read_token, 0xee, sizeof(read_token));
	UT_ASSERT_EQ(cluster_control_root_read_canonical(1, &image.records[0].identity,
											  CLUSTER_CONTROL_ROOT_READ_STRONG,
											  &snapshot, &read_token),
				 CLUSTER_CONTROL_ROOT_OK_BAK_BLOCKED);
	UT_ASSERT_EQ(snapshot.identity.system_identifier, 0);
	UT_ASSERT_EQ(read_token.file_txn_seq, 0);
}

UT_TEST(test_storage_contract_fails_before_cf_or_file_io)
{
	ClusterControlRootMigrationImage image;
	ClusterControlRootMigrationRoundV1 round;
	ClusterControlRootFileToken token;

	wipe_root_files();
	test_contract = CLUSTER_CF_CONTRACT_UNVERIFIED;
	UT_ASSERT_EQ(create_prepared(&image, &round, &token),
				 CLUSTER_CONTROL_ROOT_STORAGE_CONTRACT_UNVERIFIED);
	UT_ASSERT_EQ(test_cf_lock_calls, 0);
	UT_ASSERT_EQ(test_durable_rename_calls, 0);
}

UT_TEST(test_single_node_local_probe_fails_before_cf_or_file_io)
{
	ClusterControlRootMigrationImage image;
	ClusterControlRootMigrationRoundV1 round;
	ClusterControlRootFileToken token;

	wipe_root_files();
	test_node_count = 1;
	test_local_probe = false;
	UT_ASSERT_EQ(create_prepared(&image, &round, &token),
				 CLUSTER_CONTROL_ROOT_STORAGE_CONTRACT_UNVERIFIED);
	UT_ASSERT_EQ(test_cf_lock_calls, 0);
	UT_ASSERT_EQ(test_durable_rename_calls, 0);
}

UT_TEST(test_activate_and_stale_token)
{
	ClusterControlRootMigrationImage image;
	ClusterControlRootMigrationRoundV1 round;
	ClusterControlRootFileToken prepared;
	ClusterControlRootFileToken active;
	ClusterControlRootFileToken stale_out;
	uint8 round_sha[PG_SHA256_DIGEST_LENGTH];

	wipe_root_files();
	UT_ASSERT_EQ(create_prepared(&image, &round, &prepared), CLUSTER_CONTROL_ROOT_OK_PRIMARY);
	round_sha256(&round, round_sha);
	UT_ASSERT_EQ(cluster_control_root_activate_prepared(&prepared, round_sha, &active),
				 CLUSTER_CONTROL_ROOT_OK_PRIMARY);
	UT_ASSERT_EQ(active.activation_state, CLUSTER_CONTROL_ROOT_ACTIVATION_ACTIVE);
	UT_ASSERT_EQ(active.file_txn_seq, 2);
	memset(&stale_out, 0xee, sizeof(stale_out));
	UT_ASSERT_EQ(cluster_control_root_activate_prepared(&prepared, round_sha, &stale_out),
				 CLUSTER_CONTROL_ROOT_STALE_TOKEN);
	UT_ASSERT_EQ(stale_out.file_txn_seq, 0);
}

UT_TEST(test_native_cf_hold_cannot_authorize_strong_read)
{
	ClusterControlRootMigrationImage image;
	ClusterControlRootMigrationRoundV1 round;
	ClusterControlRootFileToken file_token;
	ClusterControlRootSnapshot snapshot;
	ClusterControlRootReadToken token;

	wipe_root_files();
	UT_ASSERT_EQ(create_prepared(&image, &round, &file_token), CLUSTER_CONTROL_ROOT_OK_PRIMARY);
	test_cf_clusterwide = false;
	memset(&snapshot, 0xee, sizeof(snapshot));
	memset(&token, 0xee, sizeof(token));
	UT_ASSERT_EQ(cluster_control_root_read_canonical(1, &image.records[0].identity,
											  CLUSTER_CONTROL_ROOT_READ_STRONG,
											  &snapshot, &token),
				 CLUSTER_CONTROL_ROOT_LOCK_UNAVAILABLE);
	UT_ASSERT_EQ(snapshot.identity.system_identifier, 0);
	UT_ASSERT_EQ(token.file_txn_seq, 0);
}

UT_TEST(test_activation_rejects_changed_source_wal_bytes)
{
	ClusterControlRootMigrationImage image;
	ClusterControlRootMigrationRoundV1 round;
	ClusterControlRootFileToken prepared;
	ClusterControlRootFileToken active;
	ClusterWalStateHeader header;
	uint8 round_sha[PG_SHA256_DIGEST_LENGTH];
	char path[MAXPGPATH];
	int fd;

	wipe_root_files();
	UT_ASSERT_EQ(create_prepared(&image, &round, &prepared), CLUSTER_CONTROL_ROOT_OK_PRIMARY);
	snprintf(path, sizeof(path), "%s/%s", test_wal_root, CLUSTER_WAL_STATE_FILENAME);
	fd = open(path, O_RDWR);
	UT_ASSERT(fd >= 0);
	cluster_wal_state_header_fill(&header, INT64_C(1700000000000999));
	UT_ASSERT_EQ(pwrite(fd, &header, sizeof(header), 0), sizeof(header));
	close(fd);
	round_sha256(&round, round_sha);
	memset(&active, 0xee, sizeof(active));
	UT_ASSERT_EQ(cluster_control_root_activate_prepared(&prepared, round_sha, &active),
				 CLUSTER_CONTROL_ROOT_HASH_MISMATCH);
	UT_ASSERT_EQ(active.file_txn_seq, 0);
	build_source_wal_state();
}

UT_TEST(test_activation_rejects_same_node_thread_claim_drift)
{
	ClusterControlRootMigrationImage image;
	ClusterControlRootMigrationRoundV1 round;
	ClusterControlRootFileToken prepared;
	ClusterControlRootFileToken active;
	ClusterWalThreadClaim claim;
	uint8 round_sha[PG_SHA256_DIGEST_LENGTH];
	char path[MAXPGPATH];

	wipe_root_files();
	build_source_wal_state();
	UT_ASSERT_EQ(create_prepared(&image, &round, &prepared), CLUSTER_CONTROL_ROOT_OK_PRIMARY);
	cluster_wal_thread_claim_fill(&claim, 1, 0, INT64_C(1699999999000999));
	snprintf(path, sizeof(path), "%s/thread_1/%s", test_wal_root,
			 CLUSTER_WAL_THREAD_CLAIM_FILENAME);
	write_all_or_abort(path, &claim, sizeof(claim));
	round_sha256(&round, round_sha);
	memset(&active, 0xee, sizeof(active));
	UT_ASSERT_EQ(cluster_control_root_activate_prepared(&prepared, round_sha, &active),
				 CLUSTER_CONTROL_ROOT_HASH_MISMATCH);
	UT_ASSERT_EQ(active.file_txn_seq, 0);
	build_source_wal_state();
}

UT_TEST(test_forbidden_patch_rejected_before_cf_and_file_io)
{
	ClusterControlRootReadToken token;
	ClusterControlRootPatch patch;
	ClusterControlRootSnapshot snapshot;

	wipe_root_files();
	memset(&token, 0, sizeof(token));
	token.source = 1;
	token.origin_thread_id = 1;
	memset(&patch, 0, sizeof(patch));
	patch.mask = UINT64_C(0x04);
	patch.expected_lifecycle = CLUSTER_CONTROL_ROOT_LIFECYCLE_CLOSED;
	test_cf_lock_calls = 0;
	UT_ASSERT_EQ(cluster_control_root_compare_and_publish(
				 &token, &patch, CLUSTER_CONTROL_ROOT_PUBLISH_THREAD_RETIRE,
				 &snapshot, NULL), CLUSTER_CONTROL_ROOT_INVALID_ARGUMENT);
	UT_ASSERT_EQ(test_cf_lock_calls, 0);
}

UT_TEST(test_lookup_and_revalidate_use_exact_primary_identity)
{
	ClusterControlRootMigrationImage image;
	ClusterControlRootMigrationRoundV1 round;
	ClusterControlRootFileToken file_token;
	ClusterControlRootIdentity identity;
	ClusterControlRootSnapshot snapshot;
	ClusterControlRootReadToken token;
	ClusterControlRootReadToken lookup_token;

	wipe_root_files();
	UT_ASSERT_EQ(create_prepared(&image, &round, &file_token), CLUSTER_CONTROL_ROOT_OK_PRIMARY);
	UT_ASSERT_EQ(cluster_control_root_read_canonical(1, &image.records[0].identity,
											  CLUSTER_CONTROL_ROOT_READ_STRONG,
											  &snapshot, &token),
				 CLUSTER_CONTROL_ROOT_OK_PRIMARY);
	UT_ASSERT_EQ(cluster_control_root_revalidate(&token, &image.records[0].identity,
										 &snapshot),
				 CLUSTER_CONTROL_ROOT_OK_PRIMARY);
	UT_ASSERT_EQ(cluster_control_root_lookup_owner_by_node_runtime(
				 0, &identity, &snapshot, &lookup_token), CLUSTER_CONTROL_ROOT_OK_PRIMARY);
	UT_ASSERT(cluster_control_root_identity_equal(&identity, &image.records[0].identity));
	UT_ASSERT_EQ(lookup_token.file_txn_seq, token.file_txn_seq);
}

UT_TEST(test_lifecycle_publish_exact_token_cas)
{
	ClusterControlRootMigrationImage image;
	ClusterControlRootMigrationRoundV1 round;
	ClusterControlRootFileToken file_token;
	ClusterControlRootSnapshot snapshot;
	ClusterControlRootSnapshot published;
	ClusterControlRootReadToken read_token;
	ClusterControlRootReadToken new_token;
	ClusterControlRootPatch patch;

	wipe_root_files();
	UT_ASSERT_EQ(create_prepared(&image, &round, &file_token), CLUSTER_CONTROL_ROOT_OK_PRIMARY);
	UT_ASSERT_EQ(cluster_control_root_read_canonical(1, &image.records[0].identity,
											  CLUSTER_CONTROL_ROOT_READ_STRONG,
											  &snapshot, &read_token),
				 CLUSTER_CONTROL_ROOT_OK_PRIMARY);
	memset(&patch, 0, sizeof(patch));
	patch.mask = CLUSTER_CONTROL_ROOT_PATCH_LIFECYCLE;
	patch.expected_lifecycle = CLUSTER_CONTROL_ROOT_LIFECYCLE_CLOSED;
	patch.desired.lifecycle = CLUSTER_CONTROL_ROOT_LIFECYCLE_RETIRED;
	UT_ASSERT_EQ(cluster_control_root_compare_and_publish(
				 &read_token, &patch, CLUSTER_CONTROL_ROOT_PUBLISH_THREAD_RETIRE,
				 &published, &new_token), CLUSTER_CONTROL_ROOT_OK_PRIMARY);
	UT_ASSERT_EQ(published.lifecycle, CLUSTER_CONTROL_ROOT_LIFECYCLE_RETIRED);
	UT_ASSERT_EQ(published.root_publish_seq, snapshot.root_publish_seq + 1);
	UT_ASSERT_EQ(new_token.file_txn_seq, read_token.file_txn_seq + 1);
	UT_ASSERT_EQ(cluster_control_root_compare_and_publish(
				 &read_token, &patch, CLUSTER_CONTROL_ROOT_PUBLISH_THREAD_RETIRE,
				 &published, &new_token), CLUSTER_CONTROL_ROOT_STALE_TOKEN);
}

UT_TEST(test_owner_rejoin_rejects_non_new_incarnation)
{
	ClusterControlRootMigrationImage image;
	ClusterControlRootMigrationRoundV1 round;
	ClusterControlRootFileToken file_token;
	ClusterControlRootSnapshot snapshot;
	ClusterControlRootSnapshot published;
	ClusterControlRootReadToken read_token;
	ClusterControlRootReadToken new_token;
	ClusterControlRootPatch patch;

	wipe_root_files();
	build_migration(&image, &round);
	image.records[0].lifecycle = CLUSTER_CONTROL_ROOT_LIFECYCLE_RECOVERY_COMPLETE;
	UT_ASSERT_EQ(cluster_control_root_create_prepared(&image, &round, &file_token),
				 CLUSTER_CONTROL_ROOT_OK_PRIMARY);
	UT_ASSERT_EQ(cluster_control_root_read_canonical(1, &image.records[0].identity,
											  CLUSTER_CONTROL_ROOT_READ_STRONG,
											  &snapshot, &read_token),
				 CLUSTER_CONTROL_ROOT_OK_PRIMARY);
	memset(&patch, 0, sizeof(patch));
	patch.mask = UINT64_C(0x3b);
	patch.expected_lifecycle = CLUSTER_CONTROL_ROOT_LIFECYCLE_RECOVERY_COMPLETE;
	patch.desired.lifecycle = CLUSTER_CONTROL_ROOT_LIFECYCLE_OPEN;
	patch.desired.identity.origin_owner_incarnation =
		snapshot.identity.origin_owner_incarnation;
	patch.desired.identity.root_lineage_seq = snapshot.identity.root_lineage_seq + 1;
	patch.desired.root_flags = CLUSTER_CONTROL_ROOT_FLAG_CLAIM_VALID
							   | CLUSTER_CONTROL_ROOT_FLAG_CHECKPOINT_VALID;
	patch.desired.checkpoint_tli = snapshot.checkpoint_tli;
	patch.desired.checkpoint_source_kind = snapshot.checkpoint_source_kind;
	patch.desired.checkpoint_lower_lsn = snapshot.checkpoint_lower_lsn;
	patch.desired.checkpoint_record_crc32c = snapshot.checkpoint_record_crc32c;
	patch.desired.recovered_through_lsn_exclusive = snapshot.checkpoint_lower_lsn;
	memset(&published, 0xee, sizeof(published));
	memset(&new_token, 0xee, sizeof(new_token));
	UT_ASSERT_EQ(cluster_control_root_compare_and_publish(
				 &read_token, &patch, CLUSTER_CONTROL_ROOT_PUBLISH_OWNER_REJOIN,
				 &published, &new_token), CLUSTER_CONTROL_ROOT_CAS_CONFLICT);
	UT_ASSERT_EQ(published.identity.system_identifier, 0);
	UT_ASSERT_EQ(new_token.file_txn_seq, 0);
}

UT_TEST(test_owner_rejoin_advances_exact_lineage_and_exhausts_at_max)
{
	ClusterControlRootMigrationImage image;
	ClusterControlRootMigrationRoundV1 round;
	ClusterControlRootFileToken file_token;
	ClusterControlRootIdentity identity;
	ClusterControlRootSnapshot snapshot;
	ClusterControlRootSnapshot published;
	ClusterControlRootReadToken read_token;
	ClusterControlRootReadToken new_token;
	ClusterControlRootPatch patch;
	uint64 new_incarnation;

	wipe_root_files();
	build_migration(&image, &round);
	image.records[0].lifecycle = CLUSTER_CONTROL_ROOT_LIFECYCLE_RECOVERY_COMPLETE;
	UT_ASSERT_EQ(cluster_control_root_create_prepared(&image, &round, &file_token),
				 CLUSTER_CONTROL_ROOT_OK_PRIMARY);
	UT_ASSERT_EQ(cluster_control_root_lookup_owner_by_node_runtime(
				 0, &identity, &snapshot, &read_token),
				 CLUSTER_CONTROL_ROOT_OK_PRIMARY);
	new_incarnation = snapshot.identity.origin_owner_incarnation + 1;
	build_owner_rejoin_patch(&snapshot, new_incarnation,
						 snapshot.identity.root_lineage_seq + 1, &patch);
	UT_ASSERT_EQ(cluster_control_root_compare_and_publish(
				 &read_token, &patch, CLUSTER_CONTROL_ROOT_PUBLISH_OWNER_REJOIN,
				 &published, &new_token), CLUSTER_CONTROL_ROOT_OK_PRIMARY);
	UT_ASSERT_EQ(published.lifecycle, CLUSTER_CONTROL_ROOT_LIFECYCLE_OPEN);
	UT_ASSERT_EQ(published.identity.origin_owner_incarnation, new_incarnation);
	UT_ASSERT_EQ(published.identity.root_lineage_seq, 2);

	/* Rebuild an otherwise valid RECOVERY_COMPLETE root at the terminal
	 * lineage.  OWNER_REJOIN must fail closed; UINT64_MAX never wraps. */
	wipe_root_files();
	build_migration(&image, &round);
	image.records[0].lifecycle = CLUSTER_CONTROL_ROOT_LIFECYCLE_RECOVERY_COMPLETE;
	UT_ASSERT_EQ(cluster_control_root_create_prepared(&image, &round, &file_token),
				 CLUSTER_CONTROL_ROOT_OK_PRIMARY);
	force_first_record_lineage(UINT64_MAX);
	UT_ASSERT_EQ(cluster_control_root_lookup_owner_by_node_runtime(
				 0, &identity, &snapshot, &read_token),
				 CLUSTER_CONTROL_ROOT_OK_PRIMARY);
	UT_ASSERT_EQ(snapshot.identity.root_lineage_seq, UINT64_MAX);
	build_owner_rejoin_patch(&snapshot,
						 snapshot.identity.origin_owner_incarnation + 1,
						 UINT64_C(1), &patch);
	memset(&published, 0xee, sizeof(published));
	memset(&new_token, 0xee, sizeof(new_token));
	UT_ASSERT_EQ(cluster_control_root_compare_and_publish(
				 &read_token, &patch, CLUSTER_CONTROL_ROOT_PUBLISH_OWNER_REJOIN,
				 &published, &new_token), CLUSTER_CONTROL_ROOT_CAS_CONFLICT);
	UT_ASSERT_EQ(published.identity.system_identifier, 0);
	UT_ASSERT_EQ(new_token.file_txn_seq, 0);
}

UT_TEST(test_initial_migration_requires_lineage_one)
{
	ClusterControlRootMigrationImage image;
	ClusterControlRootMigrationRoundV1 round;
	ClusterControlRootFileToken token;

	wipe_root_files();
	build_migration(&image, &round);
	image.records[0].identity.root_lineage_seq = 2;
	memset(&token, 0xee, sizeof(token));
	UT_ASSERT_EQ(cluster_control_root_create_prepared(&image, &round, &token),
				 CLUSTER_CONTROL_ROOT_INVALID_ARGUMENT);
	UT_ASSERT_EQ(token.file_txn_seq, 0);
}

UT_TEST(test_unconfirmed_release_returns_no_authority)
{
	ClusterControlRootMigrationImage image;
	ClusterControlRootMigrationRoundV1 round;
	ClusterControlRootFileToken file_token;
	ClusterControlRootSnapshot snapshot;
	ClusterControlRootReadToken token;

	wipe_root_files();
	UT_ASSERT_EQ(create_prepared(&image, &round, &file_token), CLUSTER_CONTROL_ROOT_OK_PRIMARY);
	test_cf_release_confirmed = false;
	memset(&snapshot, 0xee, sizeof(snapshot));
	memset(&token, 0xee, sizeof(token));
	UT_ASSERT_EQ(cluster_control_root_read_canonical(1, &image.records[0].identity,
											  CLUSTER_CONTROL_ROOT_READ_STRONG,
											  &snapshot, &token),
				 CLUSTER_CONTROL_ROOT_RELEASE_UNCERTAIN);
	UT_ASSERT_EQ(snapshot.identity.system_identifier, 0);
	UT_ASSERT_EQ(token.file_txn_seq, 0);
}

UT_TEST(test_primary_rename_failure_is_not_success)
{
	ClusterControlRootMigrationImage image;
	ClusterControlRootMigrationRoundV1 round;
	ClusterControlRootFileToken token;
	char path[MAXPGPATH];
	struct stat st;

	wipe_root_files();
	test_fail_primary_rename = true;
	memset(&token, 0xee, sizeof(token));
	UT_ASSERT_EQ(create_prepared(&image, &round, &token), CLUSTER_CONTROL_ROOT_IO_ERROR);
	UT_ASSERT_EQ(token.file_txn_seq, 0);
	path_for(path, sizeof(path), CLUSTER_CONTROL_ROOT_REL_PATH);
	UT_ASSERT(lstat(path, &st) != 0 && errno == ENOENT);
}

UT_TEST(test_reserved_bytes_and_symlink_fail_closed)
{
	ClusterControlRootMigrationImage image;
	ClusterControlRootMigrationRoundV1 round;
	ClusterControlRootFileToken token;
	ClusterControlRootSnapshot snapshot;
	uint8 bytes[CLUSTER_CONTROL_ROOT_FILE_BYTES];
	char primary[MAXPGPATH];
	char bak[MAXPGPATH];

	wipe_root_files();
	UT_ASSERT_EQ(create_prepared(&image, &round, &token), CLUSTER_CONTROL_ROOT_OK_PRIMARY);
	path_for(primary, sizeof(primary), CLUSTER_CONTROL_ROOT_REL_PATH);
	path_for(bak, sizeof(bak), CLUSTER_CONTROL_ROOT_BAK_REL_PATH);
	read_all_or_abort(primary, bytes, sizeof(bytes));
	bytes[196] = 1;
	put_u32_le(bytes + 504, image_crc(bytes, 504));
	write_all_or_abort(primary, bytes, sizeof(bytes));
	unlink(bak);
	UT_ASSERT_EQ(cluster_control_root_read_canonical(1, &image.records[0].identity,
											  CLUSTER_CONTROL_ROOT_READ_STRONG,
											  &snapshot, NULL),
				 CLUSTER_CONTROL_ROOT_BAD_RESERVED);

	wipe_root_files();
	UT_ASSERT_EQ(symlink("/tmp/foreign-pgrac-root", primary), 0);
	UT_ASSERT_EQ(create_prepared(&image, &round, &token), CLUSTER_CONTROL_ROOT_IO_ERROR);
}

int
main(void)
{
	setup_fixture();

	UT_PLAN(20);
	UT_RUN(test_abi_identity_and_features);
	UT_RUN(test_invalid_argument_precedes_authority_io);
	UT_RUN(test_create_and_read_primary);
	UT_RUN(test_bootstrap_read_never_returns_authority_token);
	UT_RUN(test_valid_bak_blocks_corrupt_primary);
	UT_RUN(test_storage_contract_fails_before_cf_or_file_io);
	UT_RUN(test_single_node_local_probe_fails_before_cf_or_file_io);
	UT_RUN(test_activate_and_stale_token);
	UT_RUN(test_native_cf_hold_cannot_authorize_strong_read);
	UT_RUN(test_activation_rejects_changed_source_wal_bytes);
	UT_RUN(test_activation_rejects_same_node_thread_claim_drift);
	UT_RUN(test_forbidden_patch_rejected_before_cf_and_file_io);
	UT_RUN(test_lookup_and_revalidate_use_exact_primary_identity);
	UT_RUN(test_lifecycle_publish_exact_token_cas);
	UT_RUN(test_owner_rejoin_rejects_non_new_incarnation);
	UT_RUN(test_owner_rejoin_advances_exact_lineage_and_exhausts_at_max);
	UT_RUN(test_initial_migration_requires_lineage_one);
	UT_RUN(test_unconfirmed_release_returns_no_authority);
	UT_RUN(test_primary_rename_failure_is_not_success);
	UT_RUN(test_reserved_bytes_and_symlink_fail_closed);
	UT_DONE();

	return ut_failed_count == 0 ? 0 : 1;
}
