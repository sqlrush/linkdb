/*-------------------------------------------------------------------------
 *
 * test_cluster_cf_enqueue.c
 *	  Unit tests for the CF enqueue layer (spec-5.6 Db1/Db2).
 *
 *	  U1 covers the pure resid encoder.  The Db2 tests stub the GES
 *	  seven-step / S5 promote / S6 release entry points so the
 *	  correctness-critical result-to-action mapping in cluster_cf_lock /
 *	  cluster_cf_unlock can be exercised deterministically: a grant must
 *	  register a holder and the matching release must drain exactly that CF
 *	  holder; an OK_NATIVE (cluster layer inactive) must NOT register a
 *	  holder, so release is a no-op; and any failure must fail closed
 *	  without claiming the lock.  The real cross-node grant/release is the
 *	  2-node TAP (t/288).
 *
 *
 * Portions Copyright (c) 1996-2024, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2026, pgrac contributors
 *
 * Author: SqlRush <sqlrush@gmail.com>
 *
 * IDENTIFICATION
 *	  src/test/cluster_unit/test_cluster_cf_enqueue.c
 *
 * NOTES
 *	  This is a pgrac-original file.
 *	  Spec: spec-5.6-cf-enqueue-shared-controlfile-authority.md (Db1/Db2)
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "cluster/cluster_cf_enqueue.h"
#include "cluster/cluster_cf_stats.h"
#include "cluster/cluster_lock_acquire.h"
#include "cluster/cluster_sequence.h"
#include "storage/proc.h"
#include "utils/memutils.h"
#include "utils/resowner.h"
#include "utils/timestamp.h"
#include "utils/wait_event.h"

#undef printf
#undef fprintf
#undef snprintf

#include "unit_test.h"

UT_DEFINE_GLOBALS();

void
ExceptionalCondition(const char *conditionName, const char *fileName, int lineNumber)
{
	printf("# Assert failed: %s at %s:%d\n", conditionName, fileName, lineNumber);
	abort();
}

/* ---- GES substrate stubs (settable outcomes) ---- */
static ClusterLockAcquireResult g_seven_result = CLUSTER_LOCK_ACQUIRE_OK_GRANTED;
static ClusterLockAcquireResult g_s5_result = CLUSTER_LOCK_ACQUIRE_OK_GRANTED;
static ClusterLockAcquireResult g_s6_result = CLUSTER_LOCK_ACQUIRE_OK_GRANTED;
static bool g_s6_throw = false;
static int g_seven_count = 0;
static int g_s6_count = 0;
static uint8 g_s6_last_resid_type = 0;
static ClusterLockAcquireRequest g_last_acquire_req;
static ClusterLockAcquireRequest g_s6_last_req;
static ResourceReleaseCallback g_release_callback = NULL;
static void *g_release_callback_arg = NULL;
static char g_owner_a_storage;
static char g_owner_b_storage;
static char g_memory_context_storage;
static int g_error_level = 0;
#define OWNER_A ((ResourceOwner) & g_owner_a_storage)
#define OWNER_B ((ResourceOwner) & g_owner_b_storage)
/* spec-5.6 Dc4b: capture what cluster_cf_lock threaded into the request. */
static int g_last_timeout_ms = -999;
static uint32 g_last_wait_event = 0xFFFFFFFFu;
static int g_slot_empty_checks = 0;
static int g_slot_held_publishes = 0;
static int g_slot_pending_publishes = 0;
static int g_slot_exact_clears = 0;
static bool g_slot_is_empty = true;
static bool g_slot_publish_held_result = true;
static bool g_slot_publish_pending_result = true;
static bool g_slot_clear_result = true;
static uint32 g_slot_empty_owner_procno = UINT32_MAX;
static ClusterCfSlotMode g_slot_empty_mode = CLUSTER_CF_SLOT_MODE_COUNT;
static ClusterCfPublishedSlot g_slot_last;
static int g_slot_events[64];
static int g_slot_event_count;

enum {
	SLOT_EVENT_EMPTY = 1,
	SLOT_EVENT_SEVEN,
	SLOT_EVENT_HELD,
	SLOT_EVENT_CENSUS,
	SLOT_EVENT_PENDING,
	SLOT_EVENT_S6,
	SLOT_EVENT_CLEAR
};

ResourceOwner CurrentResourceOwner = NULL;
ResourceOwner CurTransactionResourceOwner = NULL;
ResourceOwner TopTransactionResourceOwner = NULL;
ResourceOwner AuxProcessResourceOwner = NULL;
MemoryContext CurrentMemoryContext = NULL;
MemoryContext TopMemoryContext = NULL;
static PGPROC g_my_proc_storage;
PGPROC *MyProc = &g_my_proc_storage;
int MyProcPid = 4242;
TimestampTz MyStartTimestamp = INT64_C(987654321);
int cluster_node_id = 1;
sigjmp_buf *PG_exception_stack = NULL;
ErrorContextCallback *error_context_stack = NULL;

static void
record_slot_event(int event)
{
	UT_ASSERT(g_slot_event_count < (int)lengthof(g_slot_events));
	if (g_slot_event_count < (int)lengthof(g_slot_events))
		g_slot_events[g_slot_event_count++] = event;
}

int cluster_injection_armed_count = 1;

void
cluster_injection_run(const char *name)
{
	UT_ASSERT_EQ(strcmp(name, "cluster-cf-held-census-window"), 0);
	record_slot_event(SLOT_EVENT_CENSUS);
}

static void
reset_slot_fakes(void)
{
	g_slot_empty_checks = 0;
	g_slot_held_publishes = 0;
	g_slot_pending_publishes = 0;
	g_slot_exact_clears = 0;
	g_slot_is_empty = true;
	g_slot_publish_held_result = true;
	g_slot_publish_pending_result = true;
	g_slot_clear_result = true;
	g_slot_empty_owner_procno = UINT32_MAX;
	g_slot_empty_mode = CLUSTER_CF_SLOT_MODE_COUNT;
	memset(&g_slot_last, 0, sizeof(g_slot_last));
	memset(g_slot_events, 0, sizeof(g_slot_events));
	g_slot_event_count = 0;
}

void
RegisterResourceReleaseCallback(ResourceReleaseCallback callback, void *arg)
{
	g_release_callback = callback;
	g_release_callback_arg = arg;
}

void
UnregisterResourceReleaseCallback(ResourceReleaseCallback callback pg_attribute_unused(),
								  void *arg pg_attribute_unused())
{}

void
pg_re_throw(void)
{
	if (PG_exception_stack != NULL)
		siglongjmp(*PG_exception_stack, 1);
	abort();
}

void
FlushErrorState(void)
{
	g_error_level = 0;
}

ErrorData *
CopyErrorData(void)
{
	static ErrorData edata;

	memset(&edata, 0, sizeof(edata));
	edata.message = "test S6 error";
	return &edata;
}

void
FreeErrorData(ErrorData *edata pg_attribute_unused())
{}

bool
errstart(int elevel, const char *domain pg_attribute_unused())
{
	g_error_level = elevel;
	return elevel >= ERROR;
}

bool
errstart_cold(int elevel, const char *domain pg_attribute_unused())
{
	return errstart(elevel, domain);
}

void
errfinish(const char *filename pg_attribute_unused(), int lineno pg_attribute_unused(),
		  const char *funcname pg_attribute_unused())
{
	if (g_error_level >= ERROR && PG_exception_stack != NULL)
		siglongjmp(*PG_exception_stack, 1);
	abort();
}

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
errhint(const char *fmt pg_attribute_unused(), ...)
{
	return 0;
}

int
errmsg_internal(const char *fmt pg_attribute_unused(), ...)
{
	return 0;
}

ClusterLockAcquireResult
cluster_lock_acquire_seven_step(const ClusterLockAcquireRequest *req)
{
	ClusterLockAcquireRequest *mut = (ClusterLockAcquireRequest *)req;

	record_slot_event(SLOT_EVENT_SEVEN);
	g_seven_count++;
	/* spec-5.6 Dc4b: record the CF acquire's timeout + wait-event override. */
	g_last_timeout_ms = req->timeout_ms;
	g_last_wait_event = req->wait_event;

	/* simulate S3 fill_request_holder filling the holder + request id */
	mut->holder.node_id = 1;
	mut->holder.procno = 42;
	mut->holder.cluster_epoch = UINT64_C(0x1122334455667788);
	mut->holder.request_id = UINT64_C(7000) + (uint64)g_seven_count;
	mut->request_id = mut->holder.request_id;
	g_last_acquire_req = *mut;
	return g_seven_result;
}

ClusterLockAcquireResult
cluster_lock_acquire_s5_promote(const ClusterLockAcquireRequest *req)
{
	(void)req;
	return g_s5_result;
}

ClusterLockAcquireResult
cluster_lock_acquire_s6_release(const ClusterLockAcquireRequest *req)
{
	g_s6_count++;
	g_s6_last_resid_type = req->resid.type;
	g_s6_last_req = *req;
	if (g_s6_throw && PG_exception_stack != NULL)
		siglongjmp(*PG_exception_stack, 1);
	return g_s6_result;
}

ClusterLockAcquireResult
cluster_lock_acquire_s6_release_nothrow(const ClusterLockAcquireRequest *req)
{
	record_slot_event(SLOT_EVENT_S6);
	g_s6_count++;
	g_s6_last_resid_type = req->resid.type;
	g_s6_last_req = *req;
	if (g_s6_throw)
		return CLUSTER_LOCK_ACQUIRE_FAIL_INTERNAL;
	return g_s6_result;
}

/* spec-5.6 Dc4: cluster_cf_lock bumps CF acquire/fail-closed counters;
 * cluster_cf_stats.o is not linked here, so a no-op stub satisfies the link
 * (the counter mechanism is covered by test_cluster_cf_stats). */
void
cluster_cf_counter_inc(ClusterCfCounter which pg_attribute_unused())
{}

/* spec-5.6 Dc4b: cluster_cf_lock reads this GUC into req.timeout_ms; cluster_
 * guc.o is not linked here, so define it locally. */
int cluster_cf_enqueue_timeout_ms = 30000;

/* spec-5.6 increment (iii) follow-up: join-readonly is now a cross-process CF
 * shmem flag (cluster_cf_stats.o, not linked here).  A stateful stub keeps the
 * set->get behaviour the write-permission test exercises. */
static bool g_join_ro = false;
void
cluster_cf_stats_set_join_readonly(bool on)
{
	g_join_ro = on;
}
bool
cluster_cf_stats_get_join_readonly(void)
{
	return g_join_ro;
}

bool
cluster_cf_slot_is_empty(uint32 owner_procno, ClusterCfSlotMode mode)
{
	record_slot_event(SLOT_EVENT_EMPTY);
	g_slot_empty_checks++;
	g_slot_empty_owner_procno = owner_procno;
	g_slot_empty_mode = mode;
	return g_slot_is_empty;
}

bool
cluster_cf_slot_publish_held(ClusterCfSlotMode mode, const ClusterCfPublishedSlot *slot)
{
	record_slot_event(SLOT_EVENT_HELD);
	g_slot_held_publishes++;
	g_slot_last = *slot;
	UT_ASSERT_EQ(g_slot_last.mode, mode);
	return g_slot_publish_held_result;
}

bool
cluster_cf_slot_publish_release_pending(ClusterCfSlotMode mode, const ClusterCfPublishedSlot *slot)
{
	record_slot_event(SLOT_EVENT_PENDING);
	g_slot_pending_publishes++;
	g_slot_last = *slot;
	UT_ASSERT_EQ(g_slot_last.mode, mode);
	return g_slot_publish_pending_result;
}

bool
cluster_cf_slot_clear_exact(ClusterCfSlotMode mode, const ClusterCfPublishedSlot *slot)
{
	record_slot_event(SLOT_EVENT_CLEAR);
	g_slot_exact_clears++;
	g_slot_last = *slot;
	UT_ASSERT_EQ(g_slot_last.mode, mode);
	return g_slot_clear_result;
}

TimestampTz
GetCurrentTimestamp(void)
{
	return 0;
}

static bool
invoke_release_callback(ResourceReleasePhase phase, ResourceOwner owner)
{
	CurrentResourceOwner = owner;
	UT_ASSERT_NOT_NULL(g_release_callback);
	if (g_release_callback == NULL)
		return false;
	g_release_callback(phase, false, true, g_release_callback_arg);
	return true;
}

static bool
invoke_unlock_catching_error(LOCKMODE mode, ResourceOwner owner)
{
	bool caught = false;

	CurrentResourceOwner = owner;
	PG_TRY();
	{
		cluster_cf_unlock(mode);
	}
	PG_CATCH();
	{
		caught = true;
		FlushErrorState();
	}
	PG_END_TRY();

	return caught;
}

static void
assert_last_release_exact(const ClusterLockAcquireRequest *expected)
{
	UT_ASSERT_EQ(g_s6_last_req.resid.type, expected->resid.type);
	UT_ASSERT_EQ(g_s6_last_req.holder.node_id, expected->holder.node_id);
	UT_ASSERT_EQ(g_s6_last_req.holder.procno, expected->holder.procno);
	UT_ASSERT_EQ(g_s6_last_req.holder.cluster_epoch, expected->holder.cluster_epoch);
	UT_ASSERT_EQ(g_s6_last_req.holder.request_id, expected->holder.request_id);
	UT_ASSERT_EQ(g_s6_last_req.request_id, expected->request_id);
}

/* ======================================================================
 * U1 -- CF resid encoding
 * ====================================================================== */
UT_TEST(test_cf_resid_encode)
{
	ClusterResId r;

	memset(&r, 0xEE, sizeof(r));
	cluster_cf_resid_encode(&r);

	UT_ASSERT_EQ(r.field1, 0);
	UT_ASSERT_EQ(r.field2, 0);
	UT_ASSERT_EQ(r.field3, 0);
	UT_ASSERT_EQ(r.field4, 0);
	UT_ASSERT_EQ(r.type, CLUSTER_CF_RESID_TYPE);
	UT_ASSERT_EQ(r.type, 0xF1);
	UT_ASSERT_NE(r.type, CLUSTER_SQ_RESID_TYPE);
	UT_ASSERT_EQ(r.lockmethodid, DEFAULT_LOCKMETHOD);
}

/* ======================================================================
 * Db2 -- a cluster grant registers a holder; release drains the CF holder
 * ====================================================================== */
UT_TEST(test_lock_grant_then_release)
{
	g_seven_result = CLUSTER_LOCK_ACQUIRE_OK_GRANTED;
	g_s5_result = CLUSTER_LOCK_ACQUIRE_OK_GRANTED;
	g_s6_count = 0;
	g_s6_last_resid_type = 0;

	UT_ASSERT(cluster_cf_lock(ExclusiveLock));
	UT_ASSERT(cluster_cf_held(ExclusiveLock)); /* held while locked */
	cluster_cf_unlock(ExclusiveLock);
	UT_ASSERT(!cluster_cf_held(ExclusiveLock)); /* released */

	UT_ASSERT_EQ(g_s6_count, 1);			  /* exactly one release */
	UT_ASSERT_EQ(g_s6_last_resid_type, 0xF1); /* of the CF resid, not a locktag */
}

/* ======================================================================
 * Db3 -- write permission: held CF X, or the bootstrap authority window
 * ====================================================================== */
UT_TEST(test_held_and_write_permitted)
{
	g_seven_result = CLUSTER_LOCK_ACQUIRE_OK_GRANTED;
	g_s5_result = CLUSTER_LOCK_ACQUIRE_OK_GRANTED;

	/* nothing held -> no write permitted */
	UT_ASSERT(!cluster_cf_held(ExclusiveLock));
	UT_ASSERT(!cluster_cf_write_permitted());

	/* holding CF X permits a write */
	UT_ASSERT(cluster_cf_lock(ExclusiveLock));
	UT_ASSERT(cluster_cf_write_permitted());
	cluster_cf_unlock(ExclusiveLock);
	UT_ASSERT(!cluster_cf_write_permitted());

	/* the bootstrap window permits a write without a held CF X */
	cluster_cf_set_bootstrap_authority(true);
	UT_ASSERT(cluster_cf_write_permitted());
	cluster_cf_set_bootstrap_authority(false);
	UT_ASSERT(!cluster_cf_write_permitted());

	/*
	 * Join read-only (increment ii) is an orthogonal signal: it marks an
	 * attaching node whose recovery writes are skipped, and it must NEVER by
	 * itself grant write permission (a join node is a reader, not a writer).
	 */
	UT_ASSERT(!cluster_cf_join_readonly());
	cluster_cf_set_join_readonly(true);
	UT_ASSERT(cluster_cf_join_readonly());
	UT_ASSERT(!cluster_cf_write_permitted()); /* join != write permission */
	cluster_cf_set_join_readonly(false);
	UT_ASSERT(!cluster_cf_join_readonly());

	/*
	 * The process-local bring-up write-skip is what the chokepoint consults; it
	 * is orthogonal to write permission (skipping a write is not permission to
	 * write) and independent of the node-wide join flag.
	 */
	UT_ASSERT(!cluster_cf_write_skip());
	cluster_cf_set_write_skip(true);
	UT_ASSERT(cluster_cf_write_skip());
	UT_ASSERT(!cluster_cf_write_permitted()); /* write-skip != write permission */
	cluster_cf_set_write_skip(false);
	UT_ASSERT(!cluster_cf_write_skip());
}

/* ======================================================================
 * Db2 -- OK_NATIVE (cluster layer inactive) registers no holder
 * ====================================================================== */
UT_TEST(test_lock_native_no_release)
{
	g_seven_result = CLUSTER_LOCK_ACQUIRE_OK_NATIVE;
	g_s6_count = 0;

	UT_ASSERT(cluster_cf_lock(ShareLock));
	cluster_cf_unlock(ShareLock);

	UT_ASSERT_EQ(g_s6_count, 0); /* uncoordinated -> no S6 */
}

/* ======================================================================
 * Db2 -- a GES failure fails closed and registers nothing
 * ====================================================================== */
UT_TEST(test_lock_failclosed_timeout)
{
	g_seven_result = CLUSTER_LOCK_ACQUIRE_FAIL_TIMEOUT;
	g_s6_count = 0;

	UT_ASSERT(!cluster_cf_lock(ExclusiveLock));
	UT_ASSERT(invoke_release_callback(RESOURCE_RELEASE_LOCKS, CurrentResourceOwner));
	cluster_cf_unlock(ExclusiveLock); /* not held -> no-op */

	UT_ASSERT_EQ(g_s6_count, 0);
}

/* ======================================================================
 * Db2 -- a granted reservation that fails the S5 promote fails closed
 * ====================================================================== */
UT_TEST(test_lock_s5_fail)
{
	g_seven_result = CLUSTER_LOCK_ACQUIRE_OK_GRANTED;
	g_s5_result = CLUSTER_LOCK_ACQUIRE_FAIL_INTERNAL;
	g_s6_count = 0;

	UT_ASSERT(!cluster_cf_lock(ExclusiveLock));
	UT_ASSERT(invoke_release_callback(RESOURCE_RELEASE_LOCKS, CurrentResourceOwner));
	cluster_cf_unlock(ExclusiveLock); /* not held -> no-op */
	UT_ASSERT_EQ(g_s6_count, 0);

	g_s5_result = CLUSTER_LOCK_ACQUIRE_OK_GRANTED; /* reset */
}

/* ======================================================================
 * Db2 -- a try-conflict (NOT_AVAIL) does not claim the lock
 * ====================================================================== */
UT_TEST(test_lock_notavail)
{
	g_seven_result = CLUSTER_LOCK_ACQUIRE_NOT_AVAIL;

	UT_ASSERT(!cluster_cf_lock(ShareLock));
}

/* ======================================================================
 * Dc4b -- the CF acquire carries cluster.cf_enqueue_timeout_ms +
 * WAIT_EVENT_CLUSTER_CF_ENQUEUE so the GES wait is bounded + observable
 * ====================================================================== */
UT_TEST(test_lock_timeout_and_wait_event)
{
	g_seven_result = CLUSTER_LOCK_ACQUIRE_OK_GRANTED;
	g_s5_result = CLUSTER_LOCK_ACQUIRE_OK_GRANTED;
	g_last_timeout_ms = -999;
	g_last_wait_event = 0xFFFFFFFFu;

	UT_ASSERT(cluster_cf_lock(ExclusiveLock));
	cluster_cf_unlock(ExclusiveLock);

	/* CF threads the GUC timeout + its own wait-event label into the request */
	UT_ASSERT_EQ(g_last_timeout_ms, cluster_cf_enqueue_timeout_ms);
	UT_ASSERT_EQ(g_last_wait_event, (uint32)WAIT_EVENT_CLUSTER_CF_ENQUEUE);
}

/*
 * A coordinated CF grant is owned by the acquiring ResourceOwner.  Only its
 * LOCKS-phase callback consumes the grant; phase replay must not double S6.
 */
UT_TEST(test_resowner_matching_locks_phase_releases_exact_once)
{
	ClusterLockAcquireRequest acquired;

	CurrentResourceOwner = OWNER_A;
	g_seven_result = CLUSTER_LOCK_ACQUIRE_OK_GRANTED;
	g_s5_result = CLUSTER_LOCK_ACQUIRE_OK_GRANTED;
	g_s6_result = CLUSTER_LOCK_ACQUIRE_OK_GRANTED;
	g_s6_throw = false;
	g_s6_count = 0;
	reset_slot_fakes();

	UT_ASSERT(cluster_cf_lock(ExclusiveLock));
	acquired = g_last_acquire_req;

	if (!invoke_release_callback(RESOURCE_RELEASE_BEFORE_LOCKS, OWNER_A)) {
		cluster_cf_unlock(ExclusiveLock);
		return;
	}
	UT_ASSERT(cluster_cf_held(ExclusiveLock));
	UT_ASSERT_EQ(g_s6_count, 0);
	UT_ASSERT(invoke_release_callback(RESOURCE_RELEASE_AFTER_LOCKS, OWNER_A));
	UT_ASSERT(cluster_cf_held(ExclusiveLock));
	UT_ASSERT_EQ(g_s6_count, 0);
	UT_ASSERT(invoke_release_callback(RESOURCE_RELEASE_LOCKS, OWNER_A));
	UT_ASSERT(!cluster_cf_held(ExclusiveLock));
	UT_ASSERT_EQ(g_s6_count, 1);
	assert_last_release_exact(&acquired);

	UT_ASSERT(invoke_release_callback(RESOURCE_RELEASE_LOCKS, OWNER_A));
	UT_ASSERT_EQ(g_s6_count, 1);
}

/* A child/wrong owner callback cannot consume its parent's exact CF holder. */
UT_TEST(test_resowner_wrong_owner_then_parent_release)
{
	CurrentResourceOwner = OWNER_A;
	g_s6_count = 0;
	reset_slot_fakes();

	UT_ASSERT(cluster_cf_lock(ExclusiveLock));
	if (!invoke_release_callback(RESOURCE_RELEASE_LOCKS, OWNER_B)) {
		cluster_cf_unlock(ExclusiveLock);
		return;
	}
	UT_ASSERT(cluster_cf_held(ExclusiveLock));
	UT_ASSERT_EQ(g_s6_count, 0);
	UT_ASSERT_EQ(g_slot_pending_publishes, 0);
	UT_ASSERT_EQ(g_slot_exact_clears, 0);

	UT_ASSERT(invoke_release_callback(RESOURCE_RELEASE_LOCKS, OWNER_A));
	UT_ASSERT(!cluster_cf_held(ExclusiveLock));
	UT_ASSERT_EQ(g_s6_count, 1);
	UT_ASSERT_EQ(g_slot_pending_publishes, 1);
	UT_ASSERT_EQ(g_slot_exact_clears, 1);
}

/* Native grants are owner-scoped local resources but never send S6. */
UT_TEST(test_resowner_native_grant_clears_without_s6)
{
	CurrentResourceOwner = OWNER_A;
	g_seven_result = CLUSTER_LOCK_ACQUIRE_OK_NATIVE;
	g_s6_count = 0;
	reset_slot_fakes();

	UT_ASSERT(cluster_cf_lock(ExclusiveLock));
	UT_ASSERT(cluster_cf_held(ExclusiveLock));
	UT_ASSERT_EQ(g_slot_empty_owner_procno, (uint32)g_my_proc_storage.pgprocno);
	UT_ASSERT_EQ(g_slot_empty_mode, CLUSTER_CF_SLOT_MODE_X);
	UT_ASSERT_EQ(g_slot_held_publishes, 1);
	UT_ASSERT_EQ(g_slot_last.state, CLUSTER_CF_SLOT_HELD);
	UT_ASSERT_EQ(g_slot_last.mode, CLUSTER_CF_SLOT_MODE_X);
	UT_ASSERT_EQ(g_slot_last.owner_pid, MyProcPid);
	UT_ASSERT_EQ(g_slot_last.owner_procno, (uint32)g_my_proc_storage.pgprocno);
	UT_ASSERT_EQ(g_slot_last.owner_start_ts_us, MyStartTimestamp);
	UT_ASSERT_EQ(g_slot_last.node_id, cluster_node_id);
	UT_ASSERT_EQ(g_slot_last.cluster_epoch, 0);
	UT_ASSERT_EQ(g_slot_last.request_id, 0);
	UT_ASSERT_EQ(g_slot_last.coordinated, 0);
	UT_ASSERT_EQ(g_slot_event_count, 4);
	UT_ASSERT_EQ(g_slot_events[2], SLOT_EVENT_HELD);
	UT_ASSERT_EQ(g_slot_events[3], SLOT_EVENT_CENSUS);
	if (!invoke_release_callback(RESOURCE_RELEASE_LOCKS, OWNER_A)) {
		cluster_cf_unlock(ExclusiveLock);
		g_seven_result = CLUSTER_LOCK_ACQUIRE_OK_GRANTED;
		return;
	}
	UT_ASSERT(!cluster_cf_held(ExclusiveLock));
	UT_ASSERT_EQ(g_s6_count, 0);
	UT_ASSERT_EQ(g_slot_pending_publishes, 1);
	UT_ASSERT_EQ(g_slot_exact_clears, 1);

	g_seven_result = CLUSTER_LOCK_ACQUIRE_OK_GRANTED;
}

/* Normal unlock consumes owner state, so the later callback is a no-op. */
UT_TEST(test_normal_unlock_then_callback_does_not_double_release)
{
	CurrentResourceOwner = OWNER_A;
	g_s6_count = 0;

	UT_ASSERT(cluster_cf_lock(ExclusiveLock));
	cluster_cf_unlock(ExclusiveLock);
	UT_ASSERT_EQ(g_s6_count, 1);
	if (!invoke_release_callback(RESOURCE_RELEASE_LOCKS, OWNER_A))
		return;
	UT_ASSERT_EQ(g_s6_count, 1);
}

UT_TEST(test_coordinated_holder_publishes_shared_held_then_exact_clear)
{
	CurrentResourceOwner = OWNER_A;
	reset_slot_fakes();
	g_s6_result = CLUSTER_LOCK_ACQUIRE_OK_GRANTED;

	UT_ASSERT(cluster_cf_lock(ExclusiveLock));
	UT_ASSERT_EQ(g_slot_empty_checks, 1);
	UT_ASSERT_EQ(g_slot_held_publishes, 1);
	UT_ASSERT_EQ(g_slot_pending_publishes, 0);
	UT_ASSERT_EQ(g_slot_last.state, CLUSTER_CF_SLOT_HELD);
	UT_ASSERT_EQ(g_slot_last.mode, CLUSTER_CF_SLOT_MODE_X);
	UT_ASSERT_EQ(g_slot_last.owner_pid, MyProcPid);
	UT_ASSERT_EQ(g_slot_last.owner_procno, g_last_acquire_req.holder.procno);
	UT_ASSERT_EQ(g_slot_last.owner_start_ts_us, MyStartTimestamp);
	UT_ASSERT_EQ(g_slot_last.node_id, g_last_acquire_req.holder.node_id);
	UT_ASSERT_EQ(g_slot_last.cluster_epoch, g_last_acquire_req.holder.cluster_epoch);
	UT_ASSERT_EQ(g_slot_last.request_id, g_last_acquire_req.request_id);
	UT_ASSERT(g_slot_last.coordinated);
	UT_ASSERT_EQ(g_slot_event_count, 4);
	UT_ASSERT_EQ(g_slot_events[0], SLOT_EVENT_EMPTY);
	UT_ASSERT_EQ(g_slot_events[1], SLOT_EVENT_SEVEN);
	UT_ASSERT_EQ(g_slot_events[2], SLOT_EVENT_HELD);
	UT_ASSERT_EQ(g_slot_events[3], SLOT_EVENT_CENSUS);

	cluster_cf_unlock(ExclusiveLock);
	UT_ASSERT_EQ(g_slot_pending_publishes, 1);
	UT_ASSERT_EQ(g_slot_exact_clears, 1);
	UT_ASSERT_EQ(g_slot_last.request_id, g_last_acquire_req.request_id);
	UT_ASSERT_EQ(g_slot_event_count, 7);
	UT_ASSERT_EQ(g_slot_events[4], SLOT_EVENT_PENDING);
	UT_ASSERT_EQ(g_slot_events[5], SLOT_EVENT_S6);
	UT_ASSERT_EQ(g_slot_events[6], SLOT_EVENT_CLEAR);
}

UT_TEST(test_stale_published_slot_blocks_before_seven_step)
{
	int seven_before = g_seven_count;

	CurrentResourceOwner = OWNER_A;
	reset_slot_fakes();
	g_slot_is_empty = false;

	UT_ASSERT(!cluster_cf_lock(ExclusiveLock));
	UT_ASSERT_EQ(g_slot_empty_checks, 1);
	UT_ASSERT_EQ(g_slot_empty_owner_procno, (uint32)g_my_proc_storage.pgprocno);
	UT_ASSERT_EQ(g_slot_empty_mode, CLUSTER_CF_SLOT_MODE_X);
	UT_ASSERT_EQ(g_seven_count, seven_before);
	UT_ASSERT_EQ(g_slot_held_publishes, 0);
	UT_ASSERT_EQ(g_slot_pending_publishes, 0);
	UT_ASSERT_EQ(g_slot_exact_clears, 0);

	g_slot_is_empty = true;
}

/*
 * Explicit unlock is owner-scoped just like ResourceOwner cleanup.  A child
 * owner must not be able to consume the parent's exact CF holder.
 */
UT_TEST(test_explicit_unlock_wrong_owner_fails_closed)
{
	ClusterLockAcquireRequest acquired;

	CurrentResourceOwner = OWNER_A;
	g_s6_count = 0;
	g_s6_result = CLUSTER_LOCK_ACQUIRE_OK_GRANTED;
	reset_slot_fakes();

	UT_ASSERT(cluster_cf_lock(ExclusiveLock));
	acquired = g_last_acquire_req;

	UT_ASSERT(invoke_unlock_catching_error(ExclusiveLock, OWNER_B));
	UT_ASSERT(cluster_cf_held(ExclusiveLock));
	UT_ASSERT_EQ(g_s6_count, 0);
	UT_ASSERT_EQ(g_slot_pending_publishes, 0);
	UT_ASSERT_EQ(g_slot_exact_clears, 0);

	UT_ASSERT(!invoke_unlock_catching_error(ExclusiveLock, OWNER_A));
	UT_ASSERT(!cluster_cf_held(ExclusiveLock));
	UT_ASSERT_EQ(g_s6_count, 1);
	UT_ASSERT_EQ(g_slot_pending_publishes, 1);
	UT_ASSERT_EQ(g_slot_exact_clears, 1);
	assert_last_release_exact(&acquired);
}

/*
 * A normal explicit unlock must expose an unconfirmed S6 to its caller.  The
 * exact pending handle must still gate a fresh request_id and remain
 * replayable until the same release is confirmed.
 */
UT_TEST(test_explicit_unlock_s6_failure_errors_and_retries_exact)
{
	ClusterLockAcquireRequest failed_release;
	int seven_before_retry;

	CurrentResourceOwner = OWNER_A;
	g_s6_count = 0;
	g_s6_result = CLUSTER_LOCK_ACQUIRE_FAIL_TIMEOUT;
	reset_slot_fakes();

	UT_ASSERT(cluster_cf_lock(ExclusiveLock));
	UT_ASSERT(invoke_unlock_catching_error(ExclusiveLock, OWNER_A));
	UT_ASSERT(!cluster_cf_held(ExclusiveLock));
	UT_ASSERT_EQ(g_s6_count, 1);
	UT_ASSERT_EQ(g_slot_pending_publishes, 1);
	UT_ASSERT_EQ(g_slot_exact_clears, 0);
	failed_release = g_s6_last_req;

	seven_before_retry = g_seven_count;
	CurrentResourceOwner = OWNER_B;
	UT_ASSERT(!cluster_cf_lock(ExclusiveLock));
	UT_ASSERT_EQ(g_seven_count, seven_before_retry);
	UT_ASSERT_EQ(g_s6_count, 2);
	UT_ASSERT_EQ(g_slot_pending_publishes, 1);
	UT_ASSERT_EQ(g_slot_exact_clears, 0);
	assert_last_release_exact(&failed_release);

	g_s6_result = CLUSTER_LOCK_ACQUIRE_OK_GRANTED;
	UT_ASSERT(cluster_cf_lock(ExclusiveLock));
	UT_ASSERT_EQ(g_seven_count, seven_before_retry + 1);
	UT_ASSERT_EQ(g_s6_count, 3);
	UT_ASSERT_EQ(g_slot_exact_clears, 1);
	assert_last_release_exact(&failed_release);
	cluster_cf_unlock(ExclusiveLock);
}

/*
 * A returned S6 failure revokes local write permission but retains the exact
 * holder.  A same-process next acquire must retry it before seven-step and
 * fail closed while that retry remains unconfirmed.
 */
UT_TEST(test_resowner_s6_failure_retains_exact_and_gates_next_acquire)
{
	ClusterLockAcquireRequest failed_release;
	int seven_before_retry;

	CurrentResourceOwner = OWNER_A;
	g_s6_count = 0;
	g_s6_result = CLUSTER_LOCK_ACQUIRE_FAIL_TIMEOUT;
	reset_slot_fakes();

	UT_ASSERT(cluster_cf_lock(ExclusiveLock));
	if (!invoke_release_callback(RESOURCE_RELEASE_LOCKS, OWNER_A)) {
		g_s6_result = CLUSTER_LOCK_ACQUIRE_OK_GRANTED;
		cluster_cf_unlock(ExclusiveLock);
		return;
	}
	UT_ASSERT(!cluster_cf_held(ExclusiveLock));
	UT_ASSERT_EQ(g_s6_count, 1);
	UT_ASSERT_EQ(g_slot_pending_publishes, 1);
	UT_ASSERT_EQ(g_slot_exact_clears, 0);
	failed_release = g_s6_last_req;

	UT_ASSERT(invoke_release_callback(RESOURCE_RELEASE_LOCKS, OWNER_A));
	UT_ASSERT_EQ(g_s6_count, 1);

	seven_before_retry = g_seven_count;
	CurrentResourceOwner = OWNER_B;
	UT_ASSERT(!cluster_cf_lock(ExclusiveLock));
	UT_ASSERT_EQ(g_seven_count, seven_before_retry);
	UT_ASSERT_EQ(g_s6_count, 2);
	UT_ASSERT_EQ(g_slot_pending_publishes, 1);
	UT_ASSERT_EQ(g_slot_exact_clears, 0);
	assert_last_release_exact(&failed_release);

	g_s6_result = CLUSTER_LOCK_ACQUIRE_OK_GRANTED;
	UT_ASSERT(cluster_cf_lock(ExclusiveLock));
	UT_ASSERT_EQ(g_seven_count, seven_before_retry + 1);
	UT_ASSERT_EQ(g_s6_count, 3);
	UT_ASSERT_EQ(g_slot_exact_clears, 1);
	assert_last_release_exact(&failed_release);
	cluster_cf_unlock(ExclusiveLock);
}

/* An S6 ERROR/longjmp is swallowed during unwind and leaves exact pending. */
UT_TEST(test_resowner_s6_error_is_nonthrowing_and_replay_safe)
{
	ClusterLockAcquireRequest failed_release;
	int seven_before_retry;

	CurrentResourceOwner = OWNER_A;
	g_s6_count = 0;
	g_s6_result = CLUSTER_LOCK_ACQUIRE_OK_GRANTED;
	g_s6_throw = true;
	reset_slot_fakes();

	UT_ASSERT(cluster_cf_lock(ExclusiveLock));
	if (!invoke_release_callback(RESOURCE_RELEASE_LOCKS, OWNER_A)) {
		g_s6_throw = false;
		cluster_cf_unlock(ExclusiveLock);
		return;
	}
	UT_ASSERT(!cluster_cf_held(ExclusiveLock));
	UT_ASSERT_EQ(g_s6_count, 1);
	UT_ASSERT_EQ(g_slot_pending_publishes, 1);
	UT_ASSERT_EQ(g_slot_exact_clears, 0);
	failed_release = g_s6_last_req;

	UT_ASSERT(invoke_release_callback(RESOURCE_RELEASE_LOCKS, OWNER_A));
	UT_ASSERT_EQ(g_s6_count, 1);

	g_s6_throw = false;
	seven_before_retry = g_seven_count;
	CurrentResourceOwner = OWNER_B;
	UT_ASSERT(cluster_cf_lock(ExclusiveLock));
	UT_ASSERT_EQ(g_seven_count, seven_before_retry + 1);
	UT_ASSERT_EQ(g_s6_count, 2);
	UT_ASSERT_EQ(g_slot_exact_clears, 1);
	assert_last_release_exact(&failed_release);
	cluster_cf_unlock(ExclusiveLock);
}

/*
 * If the shared HELD publication fails after a coordinated grant, the caller
 * must not observe success.  Local permission is revoked first, the same S6
 * handle is released, and the unresolved shared evidence keeps a new acquire
 * off the seven-step path.
 */
UT_TEST(test_held_publish_failure_releases_and_stays_fail_closed)
{
	int seven_before;

	CurrentResourceOwner = OWNER_A;
	g_seven_result = CLUSTER_LOCK_ACQUIRE_OK_GRANTED;
	g_s5_result = CLUSTER_LOCK_ACQUIRE_OK_GRANTED;
	g_s6_result = CLUSTER_LOCK_ACQUIRE_OK_GRANTED;
	g_s6_count = 0;
	reset_slot_fakes();
	g_slot_publish_held_result = false;
	g_slot_publish_pending_result = false;
	g_slot_clear_result = false;
	seven_before = g_seven_count;

	UT_ASSERT(!cluster_cf_lock(ExclusiveLock));
	UT_ASSERT(!cluster_cf_held(ExclusiveLock));
	UT_ASSERT_EQ(g_seven_count, seven_before + 1);
	UT_ASSERT_EQ(g_slot_held_publishes, 1);
	UT_ASSERT_EQ(g_slot_pending_publishes, 1);
	UT_ASSERT_EQ(g_s6_count, 1);
	UT_ASSERT_EQ(g_slot_exact_clears, 0);

	CurrentResourceOwner = OWNER_B;
	UT_ASSERT(!cluster_cf_lock(ExclusiveLock));
	UT_ASSERT_EQ(g_seven_count, seven_before + 1);
	UT_ASSERT_EQ(g_s6_count, 2);
	UT_ASSERT_EQ(g_slot_exact_clears, 1);
}

int
main(void)
{
	CurrentMemoryContext = (MemoryContext)&g_memory_context_storage;
	TopMemoryContext = CurrentMemoryContext;
	CurrentResourceOwner = OWNER_A;
	memset(&g_my_proc_storage, 0, sizeof(g_my_proc_storage));
	g_my_proc_storage.pgprocno = 42;

	UT_PLAN(19);
	UT_RUN(test_cf_resid_encode);
	UT_RUN(test_lock_grant_then_release);
	UT_RUN(test_held_and_write_permitted);
	UT_RUN(test_lock_native_no_release);
	UT_RUN(test_lock_failclosed_timeout);
	UT_RUN(test_lock_s5_fail);
	UT_RUN(test_lock_notavail);
	UT_RUN(test_lock_timeout_and_wait_event);
	UT_RUN(test_resowner_matching_locks_phase_releases_exact_once);
	UT_RUN(test_resowner_wrong_owner_then_parent_release);
	UT_RUN(test_resowner_native_grant_clears_without_s6);
	UT_RUN(test_normal_unlock_then_callback_does_not_double_release);
	UT_RUN(test_coordinated_holder_publishes_shared_held_then_exact_clear);
	UT_RUN(test_stale_published_slot_blocks_before_seven_step);
	UT_RUN(test_explicit_unlock_wrong_owner_fails_closed);
	UT_RUN(test_explicit_unlock_s6_failure_errors_and_retries_exact);
	UT_RUN(test_resowner_s6_failure_retains_exact_and_gates_next_acquire);
	UT_RUN(test_resowner_s6_error_is_nonthrowing_and_replay_safe);
	UT_RUN(test_held_publish_failure_releases_and_stays_fail_closed);
	UT_DONE();

	return ut_failed_count == 0 ? 0 : 1;
}
