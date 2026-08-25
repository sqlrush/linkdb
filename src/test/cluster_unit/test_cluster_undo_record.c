/*-------------------------------------------------------------------------
 *
 * test_cluster_undo_record.c
 *	  pgrac spec-3.7 D12 — cluster_unit encode/decode round-trip tests
 *	  for UndoRecordHeader + 4 op payloads + slot directory.
 *
 *	  19 tests covering:
 *	    T1   UndoRecordHeader encode + decode round-trip (full fields)
 *	    T2   UndoInsertPayload encode + decode round-trip
 *	    T3   UndoUpdatePayload encode + decode round-trip
 *	    T4   UndoDeletePayload encode + decode round-trip
 *	    T5   UndoItlPayload encode + decode round-trip (40B all fields)
 *	    T6   multi-record in single block (slot dir advance)
 *	    T7   block has-space invariant at boundary (7K record OK)
 *	    T8   block has-space invariant rejection (8K + 1 byte)
 *	    T9   slot dir grow-downward addressing (slot N at offset BLCKSZ - 8*(N+1))
 *	    T10  PGRAC_UNDO_BLOCK_MAGIC roundtrip after init
 *	    T11  flags + record_type byte-for-byte fidelity
 *	    T12  prev_uba 16B preserved through encode → decode chain
 *	    T13–T19  R1 O4 complete-pool scan, failure classification,
 *	              once-per-postmaster publication, restart reconstruction,
 *	              create/reuse cardinality and effective-cap floor
 *
 * Copyright (c) 1996-2024, PostgreSQL Global Development Group
 * Portions Copyright (c) 2026, pgrac contributors
 *
 * Author: SqlRush <sqlrush@gmail.com>
 *
 * Spec: spec-3.7-undo-record-format-allocator.md (FROZEN v0.4 +
 *       Hardening v1.0.1 H-1/H-2)
 *
 *-------------------------------------------------------------------------
 */
#define USE_PGRAC_CLUSTER 1

#include "postgres.h"

#include <errno.h>
#include <fcntl.h>
#include <stddef.h>
#include <string.h>
#include <sys/stat.h>
#include <unistd.h>

#include "cluster/cluster_conf.h"
#include "cluster/cluster_shmem.h"
#include "cluster/cluster_undo_gcs.h"
#include "cluster/cluster_undo_format.h"
#include "cluster/cluster_undo_record.h"
#include "cluster/cluster_undo_record_api.h"
#include "cluster/cluster_undo_segment.h"
#include "cluster/cluster_xnode_profile.h"
#include "cluster/storage/cluster_shared_fs.h"
#include "cluster/storage/cluster_undo_alloc.h"
#include "storage/backendid.h"
#include "storage/fd.h"
#include "storage/proc.h"
#include "utils/elog.h"
#include "utils/wait_event.h"

#undef printf
#undef fprintf
#undef snprintf

#include "unit_test.h"

UT_DEFINE_GLOBALS();

#ifndef UNDO_RECORD_SOURCE_PATH
#error "UNDO_RECORD_SOURCE_PATH must identify cluster_undo_record.c"
#endif


#define UNDO_TEST_SHMEM_BYTES 16384

static union {
	max_align_t alignment;
	char bytes[UNDO_TEST_SHMEM_BYTES];
} undo_test_shmem;
static bool undo_test_shmem_found;
static char undo_test_root[MAXPGPATH];
static char undo_test_open_fail_path[MAXPGPATH];
static int undo_test_observer_open_calls;
static bool undo_test_track_observer_opens;
static uint32 undo_test_wait_event_info;

char *DataDir = NULL;
bool cluster_enabled = false;
bool cluster_undo_gcs_coherence = false;
int cluster_node_id = 0;
int cluster_undo_segments_max_per_instance = CLUSTER_UNDO_SEGS_PER_INSTANCE;
int cluster_undo_extent_blocks = 1;
int cluster_undo_record_inline_max_bytes = 2048;
bool cluster_undo_record_segment_commit_on_rollover = true;
ClusterConf *ClusterConfShmem = NULL;
ClusterXnodeProfileShared *ClusterXnodeProfileCtl = NULL;
bool cluster_xnode_profile_enabled = false;
int MaxBackends = 1;
BackendId MyBackendId = InvalidBackendId;
PGPROC *MyProc = NULL;
int max_prepared_xacts = 0;
uint32 *my_wait_event_info = &undo_test_wait_event_info;
sigjmp_buf *PG_exception_stack = NULL;
ErrorContextCallback *error_context_stack = NULL;


static char *
read_undo_record_source(void)
{
	FILE *fp;
	char *source;
	long length;

	fp = fopen(UNDO_RECORD_SOURCE_PATH, "rb");
	UT_ASSERT_NOT_NULL(fp);
	if (fp == NULL)
		return NULL;
	UT_ASSERT_EQ(fseek(fp, 0, SEEK_END), 0);
	length = ftell(fp);
	UT_ASSERT(length > 0);
	UT_ASSERT_EQ(fseek(fp, 0, SEEK_SET), 0);
	if (length <= 0) {
		fclose(fp);
		return NULL;
	}
	source = malloc((size_t)length + 1);
	UT_ASSERT_NOT_NULL(source);
	if (source == NULL) {
		fclose(fp);
		return NULL;
	}
	UT_ASSERT_EQ((long)fread(source, 1, (size_t)length, fp), length);
	source[length] = '\0';
	fclose(fp);
	return source;
}


/*
 * The O4 tests below link the real record/allocator implementation, trimmed to
 * the functions reachable from this binary.  These stubs are the runtime
 * boundaries actually crossed by that observation slice; no product branch is
 * replaced.
 */
void
ExceptionalCondition(const char *conditionName pg_attribute_unused(),
					 const char *fileName pg_attribute_unused(), int lineNumber pg_attribute_unused())
{
	abort();
}

Size
add_size(Size s1, Size s2)
{
	if (s1 > SIZE_MAX - s2)
		abort();
	return s1 + s2;
}

Size
mul_size(Size s1, Size s2)
{
	if (s1 != 0 && s2 > SIZE_MAX / s1)
		abort();
	return s1 * s2;
}

void *
ShmemInitStruct(const char *name pg_attribute_unused(), Size size, bool *foundPtr)
{
	if (size > sizeof(undo_test_shmem))
		abort();
	if (foundPtr != NULL)
		*foundPtr = undo_test_shmem_found;
	undo_test_shmem_found = true;
	return undo_test_shmem.bytes;
}

int
LWLockNewTrancheId(void)
{
	return 1;
}

void
LWLockRegisterTranche(int tranche_id pg_attribute_unused(),
					  const char *tranche_name pg_attribute_unused())
{}

void
LWLockInitialize(LWLock *lock pg_attribute_unused(), int tranche_id pg_attribute_unused())
{}

bool
LWLockAcquire(LWLock *lock pg_attribute_unused(), LWLockMode mode pg_attribute_unused())
{
	return true;
}

bool
LWLockHeldByMeInMode(LWLock *lock pg_attribute_unused(), LWLockMode mode pg_attribute_unused())
{
	return true;
}

void
LWLockRelease(LWLock *lock pg_attribute_unused())
{}

int
BasicOpenFile(const char *fileName, int fileFlags)
{
	if (undo_test_track_observer_opens)
		undo_test_observer_open_calls++;
	if (undo_test_open_fail_path[0] != '\0'
		&& strcmp(fileName, undo_test_open_fail_path) == 0) {
		errno = EIO;
		return -1;
	}
	if ((fileFlags & O_CREAT) != 0)
		return open(fileName, fileFlags, S_IRUSR | S_IWUSR);
	return open(fileName, fileFlags);
}

bool
cluster_undo_path_uses_shared_root(ClusterUndoPathIntent intent pg_attribute_unused(),
								   bool peer_mode pg_attribute_unused(),
								   bool coherence_on pg_attribute_unused())
{
	return false;
}

int
cluster_shared_fs_undo_path_resolve(uint8 owner_instance pg_attribute_unused(),
									uint32 segment_id pg_attribute_unused(),
									char *buf pg_attribute_unused(),
									size_t buf_size pg_attribute_unused())
{
	return -1;
}


static void
undo_test_segment_path(uint32 segment_id, char *path, size_t path_size)
{
	(void)snprintf(path, path_size, "%s/pg_undo/instance_0/seg_%u.dat", undo_test_root,
				   (unsigned)segment_id);
}

static bool
undo_test_fixture_begin(void)
{
	char template[] = "/tmp/pgrac-r1-undo-XXXXXX";
	char path[MAXPGPATH];

	if (mkdtemp(template) == NULL) {
		UT_ASSERT(false);
		return false;
	}
	(void)snprintf(undo_test_root, sizeof(undo_test_root), "%s", template);
	if (strncmp(undo_test_root, "/tmp/pgrac-r1-undo-", strlen("/tmp/pgrac-r1-undo-")) != 0) {
		UT_ASSERT(false);
		return false;
	}

	(void)snprintf(path, sizeof(path), "%s/pg_undo", undo_test_root);
	if (mkdir(path, S_IRWXU) != 0) {
		UT_ASSERT(false);
		return false;
	}
	(void)snprintf(path, sizeof(path), "%s/pg_undo/instance_0", undo_test_root);
	if (mkdir(path, S_IRWXU) != 0) {
		UT_ASSERT(false);
		return false;
	}

	DataDir = undo_test_root;
	cluster_node_id = 0;
	cluster_enabled = false;
	cluster_undo_gcs_coherence = false;
	cluster_undo_segments_max_per_instance = CLUSTER_UNDO_SEGS_PER_INSTANCE;
	undo_test_open_fail_path[0] = '\0';
	undo_test_observer_open_calls = 0;
	undo_test_track_observer_opens = false;
	return true;
}

static void
undo_test_fixture_end(void)
{
	char path[MAXPGPATH];
	uint32 segment_id;

	if (undo_test_root[0] == '\0')
		return;
	if (strncmp(undo_test_root, "/tmp/pgrac-r1-undo-", strlen("/tmp/pgrac-r1-undo-")) != 0)
		abort();

	for (segment_id = 1; segment_id <= CLUSTER_UNDO_SEGS_PER_INSTANCE; segment_id++) {
		undo_test_segment_path(segment_id, path, sizeof(path));
		if (unlink(path) != 0 && errno != ENOENT)
			abort();
	}
	(void)snprintf(path, sizeof(path), "%s/pg_undo/instance_0", undo_test_root);
	if (rmdir(path) != 0)
		abort();
	(void)snprintf(path, sizeof(path), "%s/pg_undo", undo_test_root);
	if (rmdir(path) != 0)
		abort();
	if (rmdir(undo_test_root) != 0)
		abort();

	DataDir = NULL;
	undo_test_root[0] = '\0';
}

static void
undo_test_make_header(uint32 segment_id, uint8 owner_instance, uint8 state, char *page)
{
	UndoSegmentHeaderData *header = (UndoSegmentHeaderData *)page;

	memset(page, 0, BLCKSZ);
	header->pd_flags = PD_UNDO_SEG_HEADER;
	header->pd_lower = SizeOfPageHeaderData;
	header->pd_upper = BLCKSZ;
	header->pd_special = BLCKSZ;
	header->pd_pagesize_version = BLCKSZ | PG_PAGE_LAYOUT_VERSION;
	header->segment_id = segment_id;
	header->segment_size_bytes = UNDO_SEGMENT_SIZE_BYTES;
	header->segment_state = state;
	header->owner_instance = owner_instance;
	header->tt_slots_count = TT_SLOTS_PER_SEGMENT;
}

static bool
undo_test_write_header(uint32 segment_id, uint8 state)
{
	char page[BLCKSZ];
	char path[MAXPGPATH];
	ssize_t written;
	int fd;

	undo_test_segment_path(segment_id, path, sizeof(path));
	undo_test_make_header(segment_id, 1, state, page);
	fd = open(path, O_WRONLY | O_CREAT | O_TRUNC, S_IRUSR | S_IWUSR);
	if (fd < 0)
		return false;
	written = write(fd, page, sizeof(page));
	if (close(fd) != 0)
		return false;
	return written == sizeof(page);
}

static bool
undo_test_write_invalid_header(uint32 segment_id)
{
	char page[BLCKSZ];
	char path[MAXPGPATH];
	ssize_t written;
	int fd;

	memset(page, 0, sizeof(page));
	undo_test_segment_path(segment_id, path, sizeof(path));
	fd = open(path, O_WRONLY | O_CREAT | O_TRUNC, S_IRUSR | S_IWUSR);
	if (fd < 0)
		return false;
	written = write(fd, page, sizeof(page));
	if (close(fd) != 0)
		return false;
	return written == sizeof(page);
}

static void
undo_test_reset_record_shmem(void)
{
	memset(&undo_test_shmem, 0, sizeof(undo_test_shmem));
	undo_test_shmem_found = false;
	cluster_undo_record_shmem_init();
}


/* ---- T1: UndoRecordHeader encode + decode round-trip ---- */
UT_TEST(test_record_header_roundtrip)
{
	UndoRecordHeader src;
	UndoRecordHeader dst;
	char buf[sizeof(UndoRecordHeader)];

	memset(&src, 0, sizeof(src));
	src.record_type = UNDO_RECORD_INSERT;
	src.flags = UNDO_REC_FLAG_FIRST_IN_TX;
	src.payload_length = 12;
	src.xid = 12345;
	src.origin_node_id = 2;
	src.tt_slot_segment_id = 1;
	src.tt_slot_id = 7;
	src.write_scn = 999;
	src.target_fork = MAIN_FORKNUM;
	src.target_block = 100;
	src.target_offset = 5;

	memcpy(buf, &src, sizeof(src));
	memset(&dst, 0xff, sizeof(dst));
	memcpy(&dst, buf, sizeof(dst));

	UT_ASSERT_EQ(dst.record_type, src.record_type);
	UT_ASSERT_EQ(dst.flags, src.flags);
	UT_ASSERT_EQ(dst.payload_length, src.payload_length);
	UT_ASSERT_EQ((long long)dst.xid, (long long)src.xid);
	UT_ASSERT_EQ(dst.origin_node_id, src.origin_node_id);
	UT_ASSERT_EQ(dst.target_fork, src.target_fork);
	UT_ASSERT_EQ((long long)dst.target_block, (long long)src.target_block);
	UT_ASSERT_EQ(dst.target_offset, src.target_offset);
}

/* ---- T2: UndoInsertPayload encode + decode ---- */
UT_TEST(test_insert_payload_roundtrip)
{
	UndoInsertPayload src = { .inserted_tuple_len = 256, .flags = 1 };
	UndoInsertPayload dst;
	char buf[sizeof(src)];

	memcpy(buf, &src, sizeof(src));
	memcpy(&dst, buf, sizeof(dst));

	UT_ASSERT_EQ(dst.inserted_tuple_len, 256);
	UT_ASSERT_EQ(dst.flags, 1);
}

/* ---- T3: UndoUpdatePayload round-trip ---- */
UT_TEST(test_update_payload_roundtrip)
{
	UndoUpdatePayload src;
	UndoUpdatePayload dst;
	char buf[sizeof(src)];

	memset(&src, 0, sizeof(src));
	src.new_block = 200;
	src.new_offset = 8;
	src.old_tuple_length = 128;
	src.old_tuple_offset = sizeof(UndoUpdatePayload);
	src.flags = 2;

	memcpy(buf, &src, sizeof(src));
	memcpy(&dst, buf, sizeof(dst));

	UT_ASSERT_EQ((long long)dst.new_block, 200LL);
	UT_ASSERT_EQ(dst.new_offset, 8);
	UT_ASSERT_EQ(dst.old_tuple_length, 128);
	UT_ASSERT_EQ(dst.old_tuple_offset, sizeof(UndoUpdatePayload));
	UT_ASSERT_EQ(dst.flags, 2);
}

/* ---- T4: UndoDeletePayload round-trip ---- */
UT_TEST(test_delete_payload_roundtrip)
{
	UndoDeletePayload src
		= { .full_tuple_length = 200, .full_tuple_offset = sizeof(UndoDeletePayload), .flags = 4 };
	UndoDeletePayload dst;
	char buf[sizeof(src)];

	memcpy(buf, &src, sizeof(src));
	memcpy(&dst, buf, sizeof(dst));

	UT_ASSERT_EQ(dst.full_tuple_length, 200);
	UT_ASSERT_EQ(dst.full_tuple_offset, sizeof(UndoDeletePayload));
	UT_ASSERT_EQ((long long)dst.flags, 4LL);
}

/* ---- T5: UndoItlPayload round-trip (40B) ---- */
UT_TEST(test_itl_payload_roundtrip)
{
	UndoItlPayload src;
	UndoItlPayload dst;
	char buf[sizeof(src)];

	memset(&src, 0, sizeof(src));
	src.itl_slot_idx = 3;
	src.prev_flags = 0;
	src.new_flags = 5;
	src.lock_mode = 2;
	src.lock_xid = 7777;
	src.prev_xmax = 5555;
	src.prev_infomask = 0x0100;
	src.prev_infomask2 = 0x0080;
	src.prev_commit_scn = 88888;

	memcpy(buf, &src, sizeof(src));
	memcpy(&dst, buf, sizeof(dst));

	UT_ASSERT_EQ(dst.itl_slot_idx, 3);
	UT_ASSERT_EQ(dst.new_flags, 5);
	UT_ASSERT_EQ(dst.lock_mode, 2);
	UT_ASSERT_EQ((long long)dst.lock_xid, 7777LL);
	UT_ASSERT_EQ(dst.prev_infomask, 0x0100);
	UT_ASSERT_EQ((long long)dst.prev_commit_scn, 88888LL);
}

/* ---- T6: multi-record in single block (slot dir advance) ---- */
UT_TEST(test_multi_record_block)
{
	char block[BLCKSZ];
	UndoBlockHeader *blkhdr = (UndoBlockHeader *)block;
	UndoSlotDirEntry *slot0;
	UndoSlotDirEntry *slot1;
	UndoSlotDirEntry *slot2;
	uint32 record_off_0 = sizeof(UndoBlockHeader);
	uint32 record_off_1 = record_off_0 + sizeof(UndoRecordHeader) + 4;
	uint32 record_off_2 = record_off_1 + sizeof(UndoRecordHeader) + 12;

	memset(block, 0, BLCKSZ);
	blkhdr->magic = PGRAC_UNDO_BLOCK_MAGIC;
	blkhdr->block_version = UNDO_BLOCK_VERSION_1;
	blkhdr->slot_count = 3;
	blkhdr->free_offset = record_off_2 + sizeof(UndoRecordHeader) + 8;

	slot0 = UNDO_SLOT_DIR_PTR(block, 0);
	slot0->record_offset = record_off_0;
	slot0->record_length = sizeof(UndoRecordHeader) + 4;
	slot0->record_type = UNDO_RECORD_INSERT;

	slot1 = UNDO_SLOT_DIR_PTR(block, 1);
	slot1->record_offset = record_off_1;
	slot1->record_length = sizeof(UndoRecordHeader) + 12;
	slot1->record_type = UNDO_RECORD_UPDATE;

	slot2 = UNDO_SLOT_DIR_PTR(block, 2);
	slot2->record_offset = record_off_2;
	slot2->record_length = sizeof(UndoRecordHeader) + 8;
	slot2->record_type = UNDO_RECORD_DELETE;

	/* Verify slot dir reads back correctly */
	UT_ASSERT_EQ((long long)UNDO_SLOT_DIR_PTR(block, 0)->record_offset, (long long)record_off_0);
	UT_ASSERT_EQ((long long)UNDO_SLOT_DIR_PTR(block, 1)->record_offset, (long long)record_off_1);
	UT_ASSERT_EQ((long long)UNDO_SLOT_DIR_PTR(block, 2)->record_offset, (long long)record_off_2);
	UT_ASSERT_EQ(UNDO_SLOT_DIR_PTR(block, 0)->record_type, UNDO_RECORD_INSERT);
	UT_ASSERT_EQ(UNDO_SLOT_DIR_PTR(block, 1)->record_type, UNDO_RECORD_UPDATE);
	UT_ASSERT_EQ(UNDO_SLOT_DIR_PTR(block, 2)->record_type, UNDO_RECORD_DELETE);
}

/* ---- T7: block has-space invariant at boundary (7K record OK) ---- */
UT_TEST(test_block_has_space_boundary_ok)
{
	UT_ASSERT(cluster_undo_block_has_space(UNDO_BLOCK_INIT_FREE_OFFSET, 0, 7000));
}

/* ---- T8: block has-space invariant rejection ---- */
UT_TEST(test_block_has_space_overflow)
{
	UT_ASSERT(!cluster_undo_block_has_space(UNDO_BLOCK_INIT_FREE_OFFSET, 0, BLCKSZ));
	/* with slot_count=255 the dir takes 256×8 = 2048 bytes from end */
	UT_ASSERT(!cluster_undo_block_has_space(UNDO_BLOCK_INIT_FREE_OFFSET, 255, BLCKSZ - 2048));
}

/* ---- T9: slot dir grow-downward addressing ---- */
UT_TEST(test_slot_dir_addressing)
{
	UT_ASSERT_EQ(UNDO_SLOT_DIR_OFFSET(0), BLCKSZ - 8);
	UT_ASSERT_EQ(UNDO_SLOT_DIR_OFFSET(1), BLCKSZ - 16);
	UT_ASSERT_EQ(UNDO_SLOT_DIR_OFFSET(255), BLCKSZ - 2048);
}

/* ---- T10: block magic + version after manual init ---- */
UT_TEST(test_block_magic_init)
{
	char block[BLCKSZ];
	UndoBlockHeader *blkhdr = (UndoBlockHeader *)block;

	memset(block, 0, BLCKSZ);
	blkhdr->magic = PGRAC_UNDO_BLOCK_MAGIC;
	blkhdr->block_version = UNDO_BLOCK_VERSION_1;
	blkhdr->free_offset = UNDO_BLOCK_INIT_FREE_OFFSET;

	UT_ASSERT_EQ((long long)blkhdr->magic, (long long)PGRAC_UNDO_BLOCK_MAGIC);
	UT_ASSERT_EQ(blkhdr->block_version, 1);
	UT_ASSERT_EQ((long long)blkhdr->free_offset, (long long)sizeof(UndoBlockHeader));
}

/* ---- T11: flags + record_type byte-for-byte fidelity ---- */
UT_TEST(test_record_type_flags_bytes)
{
	UndoRecordHeader hdr;
	const uint8 *bytes;

	memset(&hdr, 0, sizeof(hdr));
	hdr.record_type = UNDO_RECORD_ITL;
	hdr.flags = UNDO_REC_FLAG_FIRST_IN_TX | UNDO_REC_FLAG_CONTINUED;

	bytes = (const uint8 *)&hdr;
	UT_ASSERT_EQ(bytes[0], UNDO_RECORD_ITL);
	UT_ASSERT_EQ(bytes[1], UNDO_REC_FLAG_FIRST_IN_TX | UNDO_REC_FLAG_CONTINUED);
}

/* ---- T12: prev_uba 16B preserved through copy ---- */
UT_TEST(test_prev_uba_preserved)
{
	UndoRecordHeader src;
	UndoRecordHeader dst;
	char buf[sizeof(UndoRecordHeader)];

	memset(&src, 0, sizeof(src));
	src.prev_uba.raw[0] = 0x1234567890abcdefULL;
	src.prev_uba.raw[1] = 0xfedcba0987654321ULL;

	memcpy(buf, &src, sizeof(src));
	memcpy(&dst, buf, sizeof(dst));

	UT_ASSERT_EQ((long long)dst.prev_uba.raw[0], (long long)src.prev_uba.raw[0]);
	UT_ASSERT_EQ((long long)dst.prev_uba.raw[1], (long long)src.prev_uba.raw[1]);
}


/* ---- R1-A O4: real full-pool observer and record publication ---- */
UT_TEST(test_undo_pool_observer_scans_all_256_slots_across_gaps)
{
	ClusterUndoPoolObservation observation;
	ClusterUndoPoolObservationResult result;

	if (!undo_test_fixture_begin())
		return;
	UT_ASSERT(undo_test_write_header(1, SEGMENT_ALLOCATED));
	UT_ASSERT(undo_test_write_header(3, SEGMENT_ACTIVE));

	undo_test_track_observer_opens = true;
	result = cluster_undo_segment_observe_pool(1, &observation);
	undo_test_track_observer_opens = false;

	UT_ASSERT_EQ(result, CLUSTER_UNDO_POOL_OBS_OK);
	UT_ASSERT_EQ(observation.allocated_count, 2);
	UT_ASSERT_EQ(observation.configured_cap, CLUSTER_UNDO_SEGS_PER_INSTANCE);
	UT_ASSERT_EQ(observation.effective_cap, CLUSTER_UNDO_SEGS_PER_INSTANCE);
	UT_ASSERT_EQ(undo_test_observer_open_calls, CLUSTER_UNDO_SEGS_PER_INSTANCE);
	undo_test_fixture_end();
}

UT_TEST(test_undo_pool_observer_rejects_invalid_owner_and_null_output)
{
	ClusterUndoPoolObservation observation = { 7, 8, 9 };

	UT_ASSERT_EQ(cluster_undo_segment_observe_pool(0, &observation),
				 CLUSTER_UNDO_POOL_OBS_INVALID_OWNER);
	UT_ASSERT_EQ(observation.allocated_count, 0);
	UT_ASSERT_EQ(observation.configured_cap, 0);
	UT_ASSERT_EQ(observation.effective_cap, 0);
	UT_ASSERT_EQ(cluster_undo_segment_observe_pool(1, NULL),
				 CLUSTER_UNDO_POOL_OBS_INVALID_OWNER);
}

UT_TEST(test_undo_pool_observer_distinguishes_io_failure_and_invalid_header)
{
	ClusterUndoPoolObservation observation;
	char path[MAXPGPATH];

	if (!undo_test_fixture_begin())
		return;
	undo_test_segment_path(19, path, sizeof(path));
	(void)snprintf(undo_test_open_fail_path, sizeof(undo_test_open_fail_path), "%s", path);
	undo_test_track_observer_opens = true;
	UT_ASSERT_EQ(cluster_undo_segment_observe_pool(1, &observation),
				 CLUSTER_UNDO_POOL_OBS_IO_ERROR);
	undo_test_track_observer_opens = false;
	UT_ASSERT_EQ(undo_test_observer_open_calls, 19);
	UT_ASSERT_EQ(observation.allocated_count, 0);

	undo_test_open_fail_path[0] = '\0';
	undo_test_observer_open_calls = 0;
	UT_ASSERT(undo_test_write_invalid_header(7));
	undo_test_track_observer_opens = true;
	UT_ASSERT_EQ(cluster_undo_segment_observe_pool(1, &observation),
				 CLUSTER_UNDO_POOL_OBS_INVALID_HEADER);
	undo_test_track_observer_opens = false;
	UT_ASSERT_EQ(undo_test_observer_open_calls, 7);
	UT_ASSERT_EQ(observation.allocated_count, 0);
	undo_test_fixture_end();
}

UT_TEST(test_undo_record_restart_reconstructs_and_dump_ensure_never_rescans)
{
	int first_scan_calls;

	if (!undo_test_fixture_begin())
		return;
	UT_ASSERT(undo_test_write_header(1, SEGMENT_ALLOCATED));
	UT_ASSERT(undo_test_write_header(3, SEGMENT_COMMITTED));
	undo_test_reset_record_shmem();

	undo_test_track_observer_opens = true;
	cluster_undo_record_observation_ensure();
	undo_test_track_observer_opens = false;
	first_scan_calls = undo_test_observer_open_calls;
	UT_ASSERT_STR_EQ(cluster_undo_segment_observation_status_string(), "READY");
	UT_ASSERT_EQ(cluster_undo_segment_allocated_count(), 2);
	UT_ASSERT_EQ(cluster_undo_segment_allocated_high_water(), 2);
	UT_ASSERT_EQ(first_scan_calls, CLUSTER_UNDO_SEGS_PER_INSTANCE);

	/* A later file must not make a diagnostic dump rescan this incarnation. */
	UT_ASSERT(undo_test_write_header(5, SEGMENT_RECYCLABLE));
	undo_test_track_observer_opens = true;
	cluster_undo_record_observation_ensure();
	undo_test_track_observer_opens = false;
	UT_ASSERT_EQ(undo_test_observer_open_calls, first_scan_calls);
	UT_ASSERT_EQ(cluster_undo_segment_allocated_count(), 2);

	/* A fresh postmaster incarnation reconstructs all three on-disk headers. */
	undo_test_reset_record_shmem();
	undo_test_observer_open_calls = 0;
	undo_test_track_observer_opens = true;
	cluster_undo_record_observation_ensure();
	undo_test_track_observer_opens = false;
	UT_ASSERT_STR_EQ(cluster_undo_segment_observation_status_string(), "READY");
	UT_ASSERT_EQ(cluster_undo_segment_allocated_count(), 3);
	UT_ASSERT_EQ(cluster_undo_segment_allocated_high_water(), 3);
	UT_ASSERT_EQ(undo_test_observer_open_calls, CLUSTER_UNDO_SEGS_PER_INSTANCE);
	undo_test_fixture_end();
}

UT_TEST(test_undo_record_failed_lazy_scan_is_attempted_once)
{
	char path[MAXPGPATH];
	int failed_scan_calls;

	if (!undo_test_fixture_begin())
		return;
	undo_test_segment_path(4, path, sizeof(path));
	(void)snprintf(undo_test_open_fail_path, sizeof(undo_test_open_fail_path), "%s", path);
	undo_test_reset_record_shmem();

	undo_test_track_observer_opens = true;
	cluster_undo_record_observation_ensure();
	undo_test_track_observer_opens = false;
	failed_scan_calls = undo_test_observer_open_calls;
	UT_ASSERT_STR_EQ(cluster_undo_segment_observation_status_string(), "UNAVAILABLE_IO_ERROR");
	UT_ASSERT_EQ(cluster_undo_segment_allocated_count(), 0);
	UT_ASSERT_EQ(cluster_undo_segment_allocated_high_water(), 0);
	UT_ASSERT_EQ(failed_scan_calls, 4);

	undo_test_open_fail_path[0] = '\0';
	undo_test_track_observer_opens = true;
	cluster_undo_record_observation_ensure();
	undo_test_track_observer_opens = false;
	UT_ASSERT_EQ(undo_test_observer_open_calls, failed_scan_calls);
	UT_ASSERT_STR_EQ(cluster_undo_segment_observation_status_string(), "UNAVAILABLE_IO_ERROR");
	undo_test_fixture_end();
}

UT_TEST(test_undo_pool_create_increments_but_reuse_keeps_cardinality)
{
	ClusterUndoPoolObservation observation;

	if (!undo_test_fixture_begin())
		return;
	UT_ASSERT(undo_test_write_header(1, SEGMENT_ACTIVE));
	UT_ASSERT_EQ(cluster_undo_segment_observe_pool(1, &observation), CLUSTER_UNDO_POOL_OBS_OK);
	UT_ASSERT_EQ(observation.allocated_count, 1);

	UT_ASSERT(undo_test_write_header(2, SEGMENT_ALLOCATED));
	UT_ASSERT_EQ(cluster_undo_segment_observe_pool(1, &observation), CLUSTER_UNDO_POOL_OBS_OK);
	UT_ASSERT_EQ(observation.allocated_count, 2);

	/* Rebirth rewrites one valid identity in place; it cannot create a row. */
	UT_ASSERT(undo_test_write_header(2, SEGMENT_RECYCLABLE));
	UT_ASSERT_EQ(cluster_undo_segment_observe_pool(1, &observation), CLUSTER_UNDO_POOL_OBS_OK);
	UT_ASSERT_EQ(observation.allocated_count, 2);
	undo_test_fixture_end();
}

UT_TEST(test_undo_effective_cap_clamps_and_never_falls_below_current)
{
	ClusterUndoPoolObservation observation;
	uint32 segment_id;

	if (!undo_test_fixture_begin())
		return;
	for (segment_id = 1; segment_id <= 17; segment_id++)
		UT_ASSERT(undo_test_write_header(segment_id, SEGMENT_ALLOCATED));

	cluster_undo_segments_max_per_instance = 4;
	UT_ASSERT_EQ(cluster_undo_segment_observe_pool(1, &observation), CLUSTER_UNDO_POOL_OBS_OK);
	UT_ASSERT_EQ(observation.allocated_count, 17);
	UT_ASSERT_EQ(observation.configured_cap, 16);
	UT_ASSERT_EQ(observation.effective_cap, 17);

	undo_test_reset_record_shmem();
	cluster_undo_record_observation_ensure();
	UT_ASSERT_EQ(cluster_undo_segment_effective_cap(), 17);
	cluster_undo_segments_max_per_instance = 32;
	UT_ASSERT_EQ(cluster_undo_segment_effective_cap(), 32);
	cluster_undo_segments_max_per_instance = 999;
	UT_ASSERT_EQ(cluster_undo_segment_effective_cap(), CLUSTER_UNDO_SEGS_PER_INSTANCE);
	cluster_undo_segments_max_per_instance = 4;
	UT_ASSERT_EQ(cluster_undo_segment_effective_cap(), 17);
	undo_test_fixture_end();
}

/* Spec 8.4A I18/I19: one admission debt covers record allocation and every
 * lifecycle/record-seal block0 mutation it can reach. */
UT_TEST(test_record_allocator_owns_modifier_debt_outside_lifecycle_locks)
{
	char *source = read_undo_record_source();
	const char *body;
	const char *start;
	const char *end;
	const char *enter;
	const char *try_block;
	const char *recheck;
	const char *call_body;
	const char *finally_block;
	const char *leave;

	if (source == NULL)
		return;
	body = strstr(source, "\ncluster_undo_record_alloc_body(");
	start = strstr(source, "\ncluster_undo_record_alloc(");
	end = start == NULL ? NULL : strstr(start, "\n}\n\n\n/*\n * cluster_undo_get_record");
	enter = start == NULL ? NULL : strstr(start, "cluster_semantic_activation_modifier_enter(");
	try_block = start == NULL ? NULL : strstr(start, "PG_TRY();");
	recheck = start == NULL
				  ? NULL
				  : strstr(start, "cluster_semantic_activation_modifier_recheck(");
	call_body = start == NULL ? NULL : strstr(start, "cluster_undo_record_alloc_body(");
	finally_block = start == NULL ? NULL : strstr(start, "PG_FINALLY();");
	leave = finally_block == NULL
				? NULL
				: strstr(finally_block, "cluster_semantic_activation_leave(");

	UT_ASSERT_NOT_NULL(body);
	UT_ASSERT_NOT_NULL(start);
	UT_ASSERT_NOT_NULL(end);
	UT_ASSERT_NOT_NULL(enter);
	UT_ASSERT_NOT_NULL(try_block);
	UT_ASSERT_NOT_NULL(recheck);
	UT_ASSERT_NOT_NULL(call_body);
	UT_ASSERT_NOT_NULL(finally_block);
	UT_ASSERT_NOT_NULL(leave);
	if (body != NULL && start != NULL && end != NULL && enter != NULL && try_block != NULL
		&& recheck != NULL && call_body != NULL && finally_block != NULL && leave != NULL)
		UT_ASSERT(body < start && start < enter && enter < try_block && try_block < recheck
				  && recheck < call_body && call_body < finally_block && finally_block < leave
				  && leave < end);
	free(source);
}

/* The TT-only segment must become block0-current resident before the allocator
 * publishes it as the canonical TT binding.  XCUR may not run while the
 * lifecycle LWLock is held, so the producer is bracketed by an unlock and an
 * exact current-segment recheck after relock. */
UT_TEST(test_tt_rollover_publishes_current_before_binding_exposure)
{
	char *source = read_undo_record_source();
	const char *start;
	const char *end;
	const char *mark_active;
	const char *freeze_logical;
	const char *unlock_for_current;
	const char *ensure_current;
	const char *relock;
	const char *recheck_current;
	const char *recheck_publication;
	const char *publish_binding;

	if (source == NULL)
		return;
	start = strstr(source, "\ncluster_undo_tt_rollover_locked(");
	end = start == NULL ? NULL
		: strstr(start, "\nuint64\ncluster_undo_tt_retention_rollover_count(");
	mark_active = start == NULL ? NULL
		: strstr(start, "cluster_undo_segment_mark_active(");
	freeze_logical = mark_active == NULL ? NULL
		: strstr(mark_active, "logical.segment_id = new_segment_id;");
	unlock_for_current = freeze_logical == NULL ? NULL
		: strstr(freeze_logical,
			"LWLockRelease(&UndoRecordShared->lifecycle_lock.lock);");
	ensure_current = unlock_for_current == NULL ? NULL
		: strstr(unlock_for_current,
			"cluster_undo_block0_current_live_owner_ensure_resident_exact(");
	relock = ensure_current == NULL ? NULL
		: strstr(ensure_current,
			"LWLockAcquire(&UndoRecordShared->lifecycle_lock.lock, LW_EXCLUSIVE);");
	recheck_current = relock == NULL ? NULL
		: strstr(relock, "cluster_tt_slot_current_segment(node_id)");
	recheck_publication = recheck_current == NULL ? NULL
		: strstr(recheck_current,
			"cluster_undo_block0_current_live_owner_publication_recheck(");
	publish_binding = recheck_publication == NULL ? NULL
		: strstr(recheck_publication, "cluster_tt_slot_rollover(");

	UT_ASSERT_NOT_NULL(start);
	UT_ASSERT_NOT_NULL(end);
	UT_ASSERT_NOT_NULL(mark_active);
	UT_ASSERT_NOT_NULL(freeze_logical);
	UT_ASSERT_NOT_NULL(unlock_for_current);
	UT_ASSERT_NOT_NULL(ensure_current);
	UT_ASSERT_NOT_NULL(relock);
	UT_ASSERT_NOT_NULL(recheck_current);
	UT_ASSERT_NOT_NULL(recheck_publication);
	UT_ASSERT_NOT_NULL(publish_binding);
	if (start != NULL && end != NULL && mark_active != NULL
		&& freeze_logical != NULL
		&& unlock_for_current != NULL && ensure_current != NULL
		&& relock != NULL && recheck_current != NULL
		&& recheck_publication != NULL
		&& publish_binding != NULL)
		UT_ASSERT(start < mark_active && mark_active < freeze_logical
				  && freeze_logical < unlock_for_current
				  && unlock_for_current < ensure_current
				  && ensure_current < relock && relock < recheck_current
				  && recheck_current < recheck_publication
				  && recheck_publication < publish_binding
				  && publish_binding < end);
	free(source);
}


int
main(int argc, char **argv)
{
	UT_PLAN(21);

	UT_RUN(test_record_header_roundtrip);
	UT_RUN(test_insert_payload_roundtrip);
	UT_RUN(test_update_payload_roundtrip);
	UT_RUN(test_delete_payload_roundtrip);
	UT_RUN(test_itl_payload_roundtrip);
	UT_RUN(test_multi_record_block);
	UT_RUN(test_block_has_space_boundary_ok);
	UT_RUN(test_block_has_space_overflow);
	UT_RUN(test_slot_dir_addressing);
	UT_RUN(test_block_magic_init);
	UT_RUN(test_record_type_flags_bytes);
	UT_RUN(test_prev_uba_preserved);
	UT_RUN(test_undo_pool_observer_scans_all_256_slots_across_gaps);
	UT_RUN(test_undo_pool_observer_rejects_invalid_owner_and_null_output);
	UT_RUN(test_undo_pool_observer_distinguishes_io_failure_and_invalid_header);
	UT_RUN(test_undo_record_restart_reconstructs_and_dump_ensure_never_rescans);
	UT_RUN(test_undo_record_failed_lazy_scan_is_attempted_once);
	UT_RUN(test_undo_pool_create_increments_but_reuse_keeps_cardinality);
	UT_RUN(test_undo_effective_cap_clamps_and_never_falls_below_current);
	UT_RUN(test_record_allocator_owns_modifier_debt_outside_lifecycle_locks);
	UT_RUN(test_tt_rollover_publishes_current_before_binding_exposure);

	UT_DONE();
	return ut_failed_count != 0 ? 1 : 0;
}
