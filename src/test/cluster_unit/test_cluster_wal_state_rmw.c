/*-------------------------------------------------------------------------
 *
 * test_cluster_wal_state_rmw.c
 *	  RF A1 common verified-CF fresh-image RMW tests.
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include <sys/stat.h>
#include <unistd.h>

#include "access/xlog.h"
#include "access/xlogrecovery.h"
#include "cluster/cluster_cf_enqueue.h"
#include "cluster/cluster_guc.h"
#include "cluster/cluster_inject.h"
#include "cluster/cluster_lms.h"
#include "cluster/cluster_scn.h"
#include "cluster/cluster_wal_state.h"
#include "cluster/cluster_wal_thread.h"
#include "miscadmin.h"
#include "storage/fd.h"
#include "storage/proc.h"
#include "utils/timestamp.h"
#include "utils/wait_event.h"

#undef printf
#undef fprintf
#undef snprintf

#include "unit_test.h"

UT_DEFINE_GLOBALS();

/* ---- backend globals and non-common dependencies ---- */
bool cluster_enabled = true;
bool cluster_controlfile_shared_authority = true;
bool cluster_lms_enabled = true;
char *cluster_wal_threads_dir = "/virtual/pgrac-wal";
int cluster_node_id = 3;

static PGPROC dummy_proc;
PGPROC *MyProc = &dummy_proc;

static uint32 local_wait_event_info = 0;
uint32 *my_wait_event_info = &local_wait_event_info;

void
ExceptionalCondition(const char *conditionName, const char *fileName, int lineNumber)
{
	printf("# Assert failed: %s at %s:%d\n", conditionName, fileName, lineNumber);
	abort();
}

bool
errstart(int elevel, const char *domain pg_attribute_unused())
{
	if (elevel >= ERROR)
		abort();
	return false;
}

bool
errstart_cold(int elevel, const char *domain)
{
	return errstart(elevel, domain);
}

void
errfinish(const char *filename pg_attribute_unused(), int lineno pg_attribute_unused(),
		  const char *funcname pg_attribute_unused())
{}

int
errcode(int sqlerrcode pg_attribute_unused())
{
	return 0;
}

int
errmsg(const char *fmt pg_attribute_unused(), ...)
{
	return 0;
}

int
errmsg_internal(const char *fmt pg_attribute_unused(), ...)
{
	return 0;
}

int
errdetail(const char *fmt pg_attribute_unused(), ...)
{
	return 0;
}

int
errhint(const char *fmt pg_attribute_unused(), ...)
{
	return 0;
}

void *
palloc0(Size size)
{
	return calloc(1, size);
}

void
pfree(void *pointer)
{
	free(pointer);
}

uint16
cluster_wal_thread_id(void)
{
	return 4;
}

TimestampTz
GetCurrentTimestamp(void)
{
	return 1000;
}

bool
RecoveryInProgress(void)
{
	return false;
}

XLogRecPtr
GetXLogReplayRecPtr(TimeLineID *replayTLI)
{
	if (replayTLI != NULL)
		*replayTLI = 1;
	return 200;
}

TimeLineID
GetWALInsertionTimeLine(void)
{
	return 1;
}

XLogRecPtr
GetXLogWriteRecPtr(void)
{
	return 300;
}

SCN
cluster_scn_current(void)
{
	return 400;
}

bool
cluster_injection_should_skip(const char *name pg_attribute_unused())
{
	return false;
}

uint64
cluster_wal_thread_refresh_fail_fetch_add(void)
{
	return 0;
}

uint64
cluster_wal_thread_refresh_fail_read(void)
{
	return 0;
}

/* ---- exact I/O event log ---- */
typedef enum RmwEvent {
	RMW_EVENT_CF_LOCK = 1,
	RMW_EVENT_OPEN,
	RMW_EVENT_FSTAT,
	RMW_EVENT_PREAD_HEADER,
	RMW_EVENT_PREAD_SLOT,
	RMW_EVENT_PWRITE_SLOT,
	RMW_EVENT_FSYNC,
	RMW_EVENT_CLOSE,
	RMW_EVENT_CF_UNLOCK,
} RmwEvent;

static RmwEvent event_log[64];
static int event_count = 0;

static void
record_event(RmwEvent event)
{
	UT_ASSERT(event_count < (int)lengthof(event_log));
	event_log[event_count++] = event;
}

/* ---- controlled CF predicates ---- */
static bool stub_lms_ready = true;
static int32 stub_cf_master = 0;
static bool stub_cf_lock_ok = true;
static bool stub_cf_held = false;
static int stub_cf_lock_count = 0;
static int stub_cf_unlock_count = 0;

bool
cluster_lms_is_ready(void)
{
	return stub_lms_ready;
}

void
cluster_cf_resid_encode(ClusterResId *dst)
{
	memset(dst, 0, sizeof(*dst));
	dst->type = CLUSTER_CF_RESID_TYPE;
	dst->lockmethodid = DEFAULT_LOCKMETHOD;
}

int32
cluster_grd_lookup_master(const ClusterResId *resid pg_attribute_unused())
{
	return stub_cf_master;
}

bool
cluster_cf_lock(LOCKMODE mode)
{
	UT_ASSERT_EQ(mode, ExclusiveLock);
	stub_cf_lock_count++;
	record_event(RMW_EVENT_CF_LOCK);
	if (stub_cf_lock_ok)
		stub_cf_held = true;
	return stub_cf_lock_ok;
}

bool
cluster_cf_held(LOCKMODE mode)
{
	UT_ASSERT_EQ(mode, ExclusiveLock);
	return stub_cf_held;
}

void
cluster_cf_unlock(LOCKMODE mode)
{
	UT_ASSERT_EQ(mode, ExclusiveLock);
	stub_cf_unlock_count++;
	record_event(RMW_EVENT_CF_UNLOCK);
	stub_cf_held = false;
}

/* ---- virtual registry ---- */
static unsigned char virtual_file[CLUSTER_WAL_STATE_FILE_SIZE];
static off_t virtual_file_size = CLUSTER_WAL_STATE_FILE_SIZE;
static int open_count = 0;
static int read_only_open_count = 0;
static int mutating_open_count = 0;
static int pwrite_count = 0;
static int fsync_count = 0;
static int slot_pread_count = 0;
static ssize_t forced_pwrite_result = -1;
static int forced_fsync_result = 0;
static bool force_postread_mismatch = false;
static void *first_slot_read_buffer = NULL;
static void *second_slot_read_buffer = NULL;
static ClusterWalStateSlot last_pwrite_image;

int
BasicOpenFile(const char *fileName pg_attribute_unused(), int fileFlags)
{
	UT_ASSERT((fileFlags & (O_CREAT | O_TRUNC)) == 0);
	UT_ASSERT((fileFlags & O_RDWR) != 0 || (fileFlags & O_ACCMODE) == O_RDONLY);
	open_count++;
	if ((fileFlags & O_RDWR) != 0)
		mutating_open_count++;
	else
		read_only_open_count++;
	record_event(RMW_EVENT_OPEN);
	return 42;
}

int
fstat(int fd, struct stat *st)
{
	UT_ASSERT_EQ(fd, 42);
	memset(st, 0, sizeof(*st));
	st->st_size = virtual_file_size;
	record_event(RMW_EVENT_FSTAT);
	return 0;
}

int
stat(const char *path pg_attribute_unused(), struct stat *st)
{
	memset(st, 0, sizeof(*st));
	st->st_size = virtual_file_size;
	return 0;
}

ssize_t
pread(int fd, void *buf, size_t nbyte, off_t offset)
{
	UT_ASSERT_EQ(fd, 42);
	UT_ASSERT_EQ(nbyte, (size_t)CLUSTER_WAL_STATE_SLOT_SIZE);
	if (offset == 0)
		record_event(RMW_EVENT_PREAD_HEADER);
	else {
		UT_ASSERT_EQ(offset, CLUSTER_WAL_STATE_SLOT_OFFSET(4));
		record_event(RMW_EVENT_PREAD_SLOT);
		slot_pread_count++;
		if (slot_pread_count == 1)
			first_slot_read_buffer = buf;
		else if (slot_pread_count == 2)
			second_slot_read_buffer = buf;
	}
	if (offset < 0 || offset + (off_t)nbyte > virtual_file_size)
		return 0;
	memcpy(buf, virtual_file + offset, nbyte);
	if (offset != 0 && slot_pread_count == 2 && force_postread_mismatch) {
		ClusterWalStateSlot *slot = (ClusterWalStateSlot *)buf;

		slot->_reserved[17] ^= 1;
		slot->crc = cluster_wal_state_block_crc(slot);
	}
	return (ssize_t)nbyte;
}

ssize_t
pwrite(int fd, const void *buf, size_t nbyte, off_t offset)
{
	ssize_t result = (forced_pwrite_result >= 0) ? forced_pwrite_result : (ssize_t)nbyte;
	size_t copied = (result > 0) ? Min((size_t)result, nbyte) : 0;

	UT_ASSERT_EQ(fd, 42);
	UT_ASSERT_EQ(nbyte, (size_t)CLUSTER_WAL_STATE_SLOT_SIZE);
	UT_ASSERT_EQ(offset, CLUSTER_WAL_STATE_SLOT_OFFSET(4));
	pwrite_count++;
	record_event(RMW_EVENT_PWRITE_SLOT);
	memcpy(&last_pwrite_image, buf, sizeof(last_pwrite_image));
	if (copied > 0)
		memcpy(virtual_file + offset, buf, copied);
	return result;
}

int
pg_fsync(int fd)
{
	UT_ASSERT_EQ(fd, 42);
	fsync_count++;
	record_event(RMW_EVENT_FSYNC);
	return forced_fsync_result;
}

int
close(int fd)
{
	UT_ASSERT_EQ(fd, 42);
	record_event(RMW_EVENT_CLOSE);
	return 0;
}

int
unlink(const char *path pg_attribute_unused())
{
	return -1;
}

static void
fixture_reset(void)
{
	ClusterWalStateHeader *header = (ClusterWalStateHeader *)virtual_file;
	ClusterWalStateSlot *slot
		= (ClusterWalStateSlot *)(virtual_file + CLUSTER_WAL_STATE_SLOT_OFFSET(4));

	memset(virtual_file, 0, sizeof(virtual_file));
	cluster_wal_state_header_fill(header, 123);
	cluster_wal_state_slot_fill(slot, 4, 3, CLUSTER_WAL_SLOT_STATE_ACTIVE, 7, 100, 200, 300,
								400);
	slot->checkpoint_redo_lsn = 500;
	slot->refresh_interval_ms = 600;
	slot->fpw_was_off = 1;
	slot->merge_recovered_lsn = 700;
	slot->_reserved[17] = 0x5a;
	slot->crc = cluster_wal_state_block_crc(slot);

	cluster_enabled = true;
	cluster_controlfile_shared_authority = true;
	cluster_lms_enabled = true;
	stub_lms_ready = true;
	stub_cf_master = 0;
	stub_cf_lock_ok = true;
	stub_cf_held = false;
	stub_cf_lock_count = 0;
	stub_cf_unlock_count = 0;
	virtual_file_size = CLUSTER_WAL_STATE_FILE_SIZE;
	event_count = 0;
	open_count = 0;
	read_only_open_count = 0;
	mutating_open_count = 0;
	pwrite_count = 0;
	fsync_count = 0;
	slot_pread_count = 0;
	forced_pwrite_result = -1;
	forced_fsync_result = 0;
	force_postread_mismatch = false;
	first_slot_read_buffer = NULL;
	second_slot_read_buffer = NULL;
	memset(&last_pwrite_image, 0, sizeof(last_pwrite_image));
}

static ClusterWalStateUpdate
telemetry_update(void)
{
	ClusterWalStateUpdate update;

	memset(&update, 0, sizeof(update));
	update.kind = CLUSTER_WAL_STATE_UPDATE_TELEMETRY;
	update.tli = 8;
	update.last_updated = 1100;
	update.highest_lsn = 1200;
	update.highest_scn = 1300;
	update.refresh_interval_ms = 1400;
	return update;
}

static void
assert_no_registry_io(void)
{
	UT_ASSERT_EQ(open_count, 0);
	UT_ASSERT_EQ(pwrite_count, 0);
	UT_ASSERT_EQ(fsync_count, 0);
	UT_ASSERT_EQ(slot_pread_count, 0);
}

UT_TEST(test_a1_verified_cf_gate_rejects_before_io)
{
	ClusterWalStateUpdate update = telemetry_update();

	fixture_reset();
	cluster_controlfile_shared_authority = false;
	UT_ASSERT_EQ((int)cluster_wal_state_update_own(
					 &update, CLUSTER_WAL_STATE_CF_ACQUIRE_X, NULL),
				 (int)CLUSTER_WAL_STATE_UPDATE_CF_UNAVAILABLE);
	assert_no_registry_io();
	UT_ASSERT_EQ(stub_cf_lock_count, 0);

	fixture_reset();
	cluster_lms_enabled = false;
	stub_lms_ready = true;
	UT_ASSERT_EQ((int)cluster_wal_state_update_own(
					 &update, CLUSTER_WAL_STATE_CF_ACQUIRE_X, NULL),
				 (int)CLUSTER_WAL_STATE_UPDATE_CF_UNAVAILABLE);
	assert_no_registry_io();
	UT_ASSERT_EQ(stub_cf_lock_count, 0);

	fixture_reset();
	stub_lms_ready = false;
	UT_ASSERT_EQ((int)cluster_wal_state_update_own(
					 &update, CLUSTER_WAL_STATE_CF_ACQUIRE_X, NULL),
				 (int)CLUSTER_WAL_STATE_UPDATE_CF_UNAVAILABLE);
	assert_no_registry_io();
	UT_ASSERT_EQ(stub_cf_lock_count, 0);

	fixture_reset();
	stub_cf_master = -1;
	UT_ASSERT_EQ((int)cluster_wal_state_update_own(
					 &update, CLUSTER_WAL_STATE_CF_ACQUIRE_X, NULL),
				 (int)CLUSTER_WAL_STATE_UPDATE_CF_UNAVAILABLE);
	assert_no_registry_io();
	UT_ASSERT_EQ(stub_cf_lock_count, 0);

	fixture_reset();
	stub_cf_lock_ok = false;
	UT_ASSERT_EQ((int)cluster_wal_state_update_own(
					 &update, CLUSTER_WAL_STATE_CF_ACQUIRE_X, NULL),
				 (int)CLUSTER_WAL_STATE_UPDATE_CF_UNAVAILABLE);
	assert_no_registry_io();
	UT_ASSERT_EQ(stub_cf_lock_count, 1);
	UT_ASSERT_EQ(stub_cf_unlock_count, 0);
}

UT_TEST(test_a1_acquire_fresh_rmw_exact_order_and_distinct_postread)
{
	ClusterWalStateUpdate update = telemetry_update();
	ClusterWalStateSlot before;
	ClusterWalStateSlot published;
	ClusterWalStateSlot *ondisk;
	RmwEvent expected_events[] = {RMW_EVENT_CF_LOCK, RMW_EVENT_OPEN, RMW_EVENT_FSTAT,
		RMW_EVENT_PREAD_HEADER, RMW_EVENT_PREAD_SLOT, RMW_EVENT_PWRITE_SLOT, RMW_EVENT_FSYNC,
		RMW_EVENT_PREAD_SLOT, RMW_EVENT_CLOSE, RMW_EVENT_CF_UNLOCK};
	size_t i;

	fixture_reset();
	memcpy(&before, virtual_file + CLUSTER_WAL_STATE_SLOT_OFFSET(4), sizeof(before));
	UT_ASSERT_EQ((int)cluster_wal_state_update_own(
					 &update, CLUSTER_WAL_STATE_CF_ACQUIRE_X, &published),
				 (int)CLUSTER_WAL_STATE_UPDATE_OK);
	UT_ASSERT_EQ(event_count, (int)lengthof(expected_events));
	for (i = 0; i < lengthof(expected_events); i++)
		UT_ASSERT_EQ((int)event_log[i], (int)expected_events[i]);
	UT_ASSERT(first_slot_read_buffer != second_slot_read_buffer);
	UT_ASSERT_EQ(last_pwrite_image.crc, cluster_wal_state_block_crc(&last_pwrite_image));
	ondisk = (ClusterWalStateSlot *)(virtual_file + CLUSTER_WAL_STATE_SLOT_OFFSET(4));
	UT_ASSERT(memcmp(&published, ondisk, sizeof(published)) == 0);
	UT_ASSERT_EQ((int)ondisk->tli, 8);
	UT_ASSERT_EQ(ondisk->last_updated, 1100);
	UT_ASSERT_EQ(ondisk->highest_lsn, 1200);
	UT_ASSERT_EQ(ondisk->highest_scn, 1300);
	UT_ASSERT_EQ(ondisk->refresh_interval_ms, 1400);
	UT_ASSERT_EQ(ondisk->state, before.state);
	UT_ASSERT_EQ(ondisk->started_at, before.started_at);
	UT_ASSERT_EQ(ondisk->checkpoint_redo_lsn, before.checkpoint_redo_lsn);
	UT_ASSERT_EQ(ondisk->fpw_was_off, before.fpw_was_off);
	UT_ASSERT_EQ(ondisk->merge_recovered_lsn, before.merge_recovered_lsn);
	UT_ASSERT_EQ(ondisk->_reserved[17], before._reserved[17]);
	UT_ASSERT_EQ(ondisk->_pad_508[0], before._pad_508[0]);
}

UT_TEST(test_a1_short_write_and_fsync_fail_without_compensation)
{
	ClusterWalStateUpdate update = telemetry_update();

	fixture_reset();
	forced_pwrite_result = 511;
	UT_ASSERT_EQ((int)cluster_wal_state_update_own(
					 &update, CLUSTER_WAL_STATE_CF_ACQUIRE_X, NULL),
				 (int)CLUSTER_WAL_STATE_UPDATE_IO_ERROR);
	UT_ASSERT_EQ(pwrite_count, 1);
	UT_ASSERT_EQ(fsync_count, 0);
	UT_ASSERT_EQ(slot_pread_count, 1);
	UT_ASSERT_EQ(stub_cf_unlock_count, 1);

	fixture_reset();
	forced_fsync_result = -1;
	UT_ASSERT_EQ((int)cluster_wal_state_update_own(
					 &update, CLUSTER_WAL_STATE_CF_ACQUIRE_X, NULL),
				 (int)CLUSTER_WAL_STATE_UPDATE_IO_ERROR);
	UT_ASSERT_EQ(pwrite_count, 1);
	UT_ASSERT_EQ(fsync_count, 1);
	UT_ASSERT_EQ(slot_pread_count, 1);
	UT_ASSERT_EQ(stub_cf_unlock_count, 1);
}

UT_TEST(test_a1_postread_mismatch_fails_without_compensation)
{
	ClusterWalStateUpdate update = telemetry_update();
	ClusterWalStateSlot published;
	ClusterWalStateSlot sentinel;

	fixture_reset();
	memset(&sentinel, 0xa5, sizeof(sentinel));
	memcpy(&published, &sentinel, sizeof(published));
	force_postread_mismatch = true;
	UT_ASSERT_EQ((int)cluster_wal_state_update_own(
					 &update, CLUSTER_WAL_STATE_CF_ACQUIRE_X, &published),
				 (int)CLUSTER_WAL_STATE_UPDATE_POSTREAD_MISMATCH);
	UT_ASSERT_EQ(pwrite_count, 1);
	UT_ASSERT_EQ(fsync_count, 1);
	UT_ASSERT_EQ(slot_pread_count, 2);
	UT_ASSERT_EQ(stub_cf_unlock_count, 1);
	UT_ASSERT(memcmp(&published, &sentinel, sizeof(published)) == 0);
}

UT_TEST(test_a1_borrow_verified_cf_does_not_reenter_or_unlock)
{
	ClusterWalStateUpdate update = telemetry_update();

	fixture_reset();
	stub_cf_held = true;
	UT_ASSERT_EQ((int)cluster_wal_state_update_own(
					 &update, CLUSTER_WAL_STATE_CF_BORROW_X, NULL),
				 (int)CLUSTER_WAL_STATE_UPDATE_OK);
	UT_ASSERT_EQ(stub_cf_lock_count, 0);
	UT_ASSERT_EQ(stub_cf_unlock_count, 0);
	UT_ASSERT_EQ(pwrite_count, 1);
	UT_ASSERT_EQ(fsync_count, 1);

	fixture_reset();
	stub_cf_held = false;
	UT_ASSERT_EQ((int)cluster_wal_state_update_own(
					 &update, CLUSTER_WAL_STATE_CF_BORROW_X, NULL),
				 (int)CLUSTER_WAL_STATE_UPDATE_CF_UNAVAILABLE);
	assert_no_registry_io();
	UT_ASSERT_EQ(stub_cf_lock_count, 0);
	UT_ASSERT_EQ(stub_cf_unlock_count, 0);
}

UT_TEST(test_a1_fresh_header_slot_typed_rejections)
{
	ClusterWalStateUpdate update = telemetry_update();
	ClusterWalStateHeader *header;
	ClusterWalStateSlot *slot;

	fixture_reset();
	virtual_file_size--;
	UT_ASSERT_EQ((int)cluster_wal_state_update_own(
					 &update, CLUSTER_WAL_STATE_CF_ACQUIRE_X, NULL),
				 (int)CLUSTER_WAL_STATE_UPDATE_CORRUPT);
	UT_ASSERT_EQ(pwrite_count, 0);
	UT_ASSERT_EQ(fsync_count, 0);
	UT_ASSERT_EQ(stub_cf_unlock_count, 1);

	fixture_reset();
	header = (ClusterWalStateHeader *)virtual_file;
	header->crc ^= 1;
	UT_ASSERT_EQ((int)cluster_wal_state_update_own(
					 &update, CLUSTER_WAL_STATE_CF_ACQUIRE_X, NULL),
				 (int)CLUSTER_WAL_STATE_UPDATE_CORRUPT);
	UT_ASSERT_EQ(pwrite_count, 0);
	UT_ASSERT_EQ(fsync_count, 0);
	UT_ASSERT_EQ(stub_cf_unlock_count, 1);

	fixture_reset();
	slot = (ClusterWalStateSlot *)(virtual_file + CLUSTER_WAL_STATE_SLOT_OFFSET(4));
	memset(slot, 0, sizeof(*slot));
	UT_ASSERT_EQ((int)cluster_wal_state_update_own(
					 &update, CLUSTER_WAL_STATE_CF_ACQUIRE_X, NULL),
				 (int)CLUSTER_WAL_STATE_UPDATE_EMPTY);
	UT_ASSERT_EQ(pwrite_count, 0);
	UT_ASSERT_EQ(fsync_count, 0);
	UT_ASSERT_EQ(stub_cf_unlock_count, 1);

	fixture_reset();
	slot = (ClusterWalStateSlot *)(virtual_file + CLUSTER_WAL_STATE_SLOT_OFFSET(4));
	slot->crc ^= 1;
	UT_ASSERT_EQ((int)cluster_wal_state_update_own(
					 &update, CLUSTER_WAL_STATE_CF_ACQUIRE_X, NULL),
				 (int)CLUSTER_WAL_STATE_UPDATE_CORRUPT);
	UT_ASSERT_EQ(pwrite_count, 0);
	UT_ASSERT_EQ(fsync_count, 0);
	UT_ASSERT_EQ(stub_cf_unlock_count, 1);

	fixture_reset();
	slot = (ClusterWalStateSlot *)(virtual_file + CLUSTER_WAL_STATE_SLOT_OFFSET(4));
	slot->node_id = 9;
	slot->crc = cluster_wal_state_block_crc(slot);
	UT_ASSERT_EQ((int)cluster_wal_state_update_own(
					 &update, CLUSTER_WAL_STATE_CF_ACQUIRE_X, NULL),
				 (int)CLUSTER_WAL_STATE_UPDATE_FOREIGN);
	UT_ASSERT_EQ(pwrite_count, 0);
	UT_ASSERT_EQ(fsync_count, 0);
	UT_ASSERT_EQ(stub_cf_unlock_count, 1);

	fixture_reset();
	slot = (ClusterWalStateSlot *)(virtual_file + CLUSTER_WAL_STATE_SLOT_OFFSET(4));
	slot->state = CLUSTER_WAL_SLOT_STATE_STOPPED;
	slot->crc = cluster_wal_state_block_crc(slot);
	UT_ASSERT_EQ((int)cluster_wal_state_update_own(
					 &update, CLUSTER_WAL_STATE_CF_ACQUIRE_X, NULL),
				 (int)CLUSTER_WAL_STATE_UPDATE_WRONG_STATE);
	UT_ASSERT_EQ(pwrite_count, 0);
	UT_ASSERT_EQ(fsync_count, 0);
	UT_ASSERT_EQ(stub_cf_unlock_count, 1);
}

UT_TEST(test_a1_w5_checkpoint_publish_requires_verified_outer_cf)
{
	ClusterWalStateSlot before;
	ClusterWalStateSlot *ondisk;

	fixture_reset();
	memcpy(&before, virtual_file + CLUSTER_WAL_STATE_SLOT_OFFSET(4), sizeof(before));
	stub_cf_held = false;

	cluster_wal_state_publish_checkpoint_redo(900, 1000);

	ondisk = (ClusterWalStateSlot *)(virtual_file + CLUSTER_WAL_STATE_SLOT_OFFSET(4));
	UT_ASSERT(memcmp(ondisk, &before, sizeof(before)) == 0);
	UT_ASSERT_EQ(mutating_open_count, 0);
	UT_ASSERT_EQ(pwrite_count, 0);
	UT_ASSERT_EQ(fsync_count, 0);
	UT_ASSERT_EQ(stub_cf_lock_count, 0);
	UT_ASSERT_EQ(stub_cf_unlock_count, 0);
}

UT_TEST(test_a1_w5_fpw_sticky_requires_verified_outer_cf)
{
	ClusterWalStateSlot before;
	ClusterWalStateSlot *ondisk;

	fixture_reset();
	ondisk = (ClusterWalStateSlot *)(virtual_file + CLUSTER_WAL_STATE_SLOT_OFFSET(4));
	ondisk->fpw_was_off = 0;
	ondisk->crc = cluster_wal_state_block_crc(ondisk);
	memcpy(&before, ondisk, sizeof(before));
	stub_cf_held = false;

	cluster_wal_state_mark_fpw_off();

	UT_ASSERT(memcmp(ondisk, &before, sizeof(before)) == 0);
	UT_ASSERT_EQ(mutating_open_count, 0);
	UT_ASSERT_EQ(pwrite_count, 0);
	UT_ASSERT_EQ(fsync_count, 0);
	UT_ASSERT_EQ(stub_cf_lock_count, 0);
	UT_ASSERT_EQ(stub_cf_unlock_count, 0);
}

UT_TEST(test_a1_w5_checkpoint_borrows_cf_and_changes_only_redo)
{
	ClusterWalStateSlot before;
	ClusterWalStateSlot expected;
	ClusterWalStateSlot *ondisk;

	fixture_reset();
	stub_cf_held = true;
	ondisk = (ClusterWalStateSlot *)(virtual_file + CLUSTER_WAL_STATE_SLOT_OFFSET(4));
	memcpy(&before, ondisk, sizeof(before));
	memcpy(&expected, &before, sizeof(expected));
	expected.checkpoint_redo_lsn = 900;
	expected.crc = cluster_wal_state_block_crc(&expected);

	cluster_wal_state_publish_checkpoint_redo(900, 1000);

	UT_ASSERT(memcmp(ondisk, &expected, sizeof(expected)) == 0);
	UT_ASSERT_EQ(open_count, 1);
	UT_ASSERT_EQ(read_only_open_count, 0);
	UT_ASSERT_EQ(mutating_open_count, 1);
	UT_ASSERT_EQ(slot_pread_count, 2);
	UT_ASSERT(first_slot_read_buffer != second_slot_read_buffer);
	UT_ASSERT_EQ(pwrite_count, 1);
	UT_ASSERT_EQ(fsync_count, 1);
	UT_ASSERT_EQ(stub_cf_lock_count, 0);
	UT_ASSERT_EQ(stub_cf_unlock_count, 0);
}

UT_TEST(test_a1_w5_fpw_sticky_borrows_cf_and_preserves_other_bytes)
{
	ClusterWalStateSlot before;
	ClusterWalStateSlot expected;
	ClusterWalStateSlot *ondisk;

	fixture_reset();
	stub_cf_held = true;
	ondisk = (ClusterWalStateSlot *)(virtual_file + CLUSTER_WAL_STATE_SLOT_OFFSET(4));
	ondisk->fpw_was_off = 0;
	ondisk->crc = cluster_wal_state_block_crc(ondisk);
	memcpy(&before, ondisk, sizeof(before));
	memcpy(&expected, &before, sizeof(expected));
	expected.fpw_was_off = 1;
	expected.crc = cluster_wal_state_block_crc(&expected);

	cluster_wal_state_mark_fpw_off();

	UT_ASSERT(memcmp(ondisk, &expected, sizeof(expected)) == 0);
	UT_ASSERT_EQ(open_count, 1);
	UT_ASSERT_EQ(read_only_open_count, 0);
	UT_ASSERT_EQ(mutating_open_count, 1);
	UT_ASSERT_EQ(slot_pread_count, 2);
	UT_ASSERT(first_slot_read_buffer != second_slot_read_buffer);
	UT_ASSERT_EQ(pwrite_count, 1);
	UT_ASSERT_EQ(fsync_count, 1);
	UT_ASSERT_EQ(stub_cf_lock_count, 0);
	UT_ASSERT_EQ(stub_cf_unlock_count, 0);
}

int
main(int argc pg_attribute_unused(), char **argv pg_attribute_unused())
{
	UT_PLAN(10);

	UT_RUN(test_a1_verified_cf_gate_rejects_before_io);
	UT_RUN(test_a1_acquire_fresh_rmw_exact_order_and_distinct_postread);
	UT_RUN(test_a1_short_write_and_fsync_fail_without_compensation);
	UT_RUN(test_a1_postread_mismatch_fails_without_compensation);
	UT_RUN(test_a1_borrow_verified_cf_does_not_reenter_or_unlock);
	UT_RUN(test_a1_fresh_header_slot_typed_rejections);
	UT_RUN(test_a1_w5_checkpoint_publish_requires_verified_outer_cf);
	UT_RUN(test_a1_w5_fpw_sticky_requires_verified_outer_cf);
	UT_RUN(test_a1_w5_checkpoint_borrows_cf_and_changes_only_redo);
	UT_RUN(test_a1_w5_fpw_sticky_borrows_cf_and_preserves_other_bytes);

	UT_DONE();
	return ut_failed_count != 0 ? 1 : 0;
}
