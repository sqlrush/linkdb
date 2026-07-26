/*-------------------------------------------------------------------------
 *
 * test_cluster_cf_stats.c
 *	  Standalone unit tests for the spec-5.6 Dc4 CF shared-authority
 *	  observability counters: inc / read / per-counter independence / bounds
 *	  guard / NULL-safety before the shmem region is initialised.
 *
 *	  Links cluster_cf_stats.o; ShmemInitStruct + region register are stubbed
 *	  locally (a static atomic array backs the region).  The counters' real
 *	  call sites (CF X/S acquire, fail-closed, single-node authority, .bak
 *	  fallback) are exercised by the cluster_tap CF harness.
 *
 *
 * Portions Copyright (c) 1996-2024, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2026, pgrac contributors
 *
 * Author: SqlRush <sqlrush@gmail.com>
 *
 * IDENTIFICATION
 *	  src/test/cluster_unit/test_cluster_cf_stats.c
 *
 * NOTES
 *	  This is a pgrac-original file.
 *	  Spec: spec-5.6-cf-enqueue-shared-controlfile-authority.md (Dc4)
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "cluster/cluster_cf_stats.h"
#include "cluster/cluster_shmem.h"
#include "port/atomics.h"
#include "storage/lwlock.h"
#include "storage/proc.h"

#ifdef vprintf
#undef vprintf
#endif
#ifdef printf
#undef printf
#endif
#ifdef fprintf
#undef fprintf
#endif

#include "unit_test.h"


/* ============================================================
 * PG runtime stubs.
 * ============================================================ */

int MaxBackends = 3;
static PROC_HDR fake_proc_global;
PROC_HDR *ProcGlobal = &fake_proc_global;

static union {
	LWLockPadded align;
	uint64 align64;
	unsigned char bytes[65536];
} fake_cf_shmem;
static bool fake_cf_initialized;
static Size fake_cf_requested_size;
static const ClusterShmemRegion *fake_registered_region;
static int fake_lwlock_init_count;
static int fake_lwlock_tranche = -1;
static LWLock *fake_census_lock;
static int fake_lwlock_acquire_count;
static int fake_lwlock_shared_count;
static int fake_lwlock_exclusive_count;
static int fake_lwlock_release_count;
static int fake_lwlock_held;

void
ExceptionalCondition(const char *conditionName pg_attribute_unused(),
					 const char *fileName pg_attribute_unused(),
					 int lineNumber pg_attribute_unused())
{
	printf("# Assert failed: %s at %s:%d\n", conditionName, fileName, lineNumber);
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

Size
add_size(Size s1, Size s2)
{
	UT_ASSERT(s1 <= SIZE_MAX - s2);
	return s1 + s2;
}

Size
mul_size(Size s1, Size s2)
{
	UT_ASSERT(s1 == 0 || s2 <= SIZE_MAX / s1);
	return s1 * s2;
}

void *
ShmemInitStruct(const char *name, Size size, bool *foundPtr)
{
	if (name != NULL && strcmp(name, "pgrac cluster cf stats") == 0) {
		Assert(size <= sizeof(fake_cf_shmem.bytes));
		fake_cf_requested_size = size;
		*foundPtr = fake_cf_initialized;
		fake_cf_initialized = true;
		return fake_cf_shmem.bytes;
	}

	*foundPtr = true;
	return NULL;
}

void
cluster_shmem_register_region(const ClusterShmemRegion *region)
{
	fake_registered_region = region;
}

void
LWLockInitialize(LWLock *lock, int tranche_id)
{
	fake_lwlock_init_count++;
	fake_lwlock_tranche = tranche_id;
	fake_census_lock = lock;
}

bool
LWLockAcquire(LWLock *lock, LWLockMode mode)
{
	UT_ASSERT_EQ(lock, fake_census_lock);
	UT_ASSERT_EQ(fake_lwlock_held, 0);
	fake_lwlock_acquire_count++;
	if (mode == LW_SHARED)
		fake_lwlock_shared_count++;
	else if (mode == LW_EXCLUSIVE)
		fake_lwlock_exclusive_count++;
	else
		UT_ASSERT(false);
	fake_lwlock_held++;
	return true;
}

void
LWLockRelease(LWLock *lock)
{
	UT_ASSERT_EQ(lock, fake_census_lock);
	UT_ASSERT_EQ(fake_lwlock_held, 1);
	fake_lwlock_held--;
	fake_lwlock_release_count++;
}

static ClusterCfPublishedSlot
make_slot(uint32 procno, ClusterCfSlotMode mode, ClusterCfPublishedSlotState state,
		  bool coordinated, uint64 request_id)
{
	ClusterCfPublishedSlot slot;

	memset(&slot, 0, sizeof(slot));
	slot.state = state;
	slot.mode = mode;
	slot.owner_pid = 4000 + (int32)procno;
	slot.owner_procno = procno;
	slot.owner_start_ts_us = INT64_C(9000000) + (int64)procno;
	slot.node_id = 1;
	slot.coordinated = coordinated ? 1 : 0;
	slot.cluster_epoch = coordinated ? UINT64_C(77) : 0;
	slot.request_id = coordinated ? request_id : 0;
	return slot;
}

static void
clear_published_slot(ClusterCfPublishedSlot *slot)
{
	if (slot->state == CLUSTER_CF_SLOT_HELD) {
		slot->state = CLUSTER_CF_SLOT_RELEASE_PENDING;
		UT_ASSERT(cluster_cf_slot_publish_release_pending((ClusterCfSlotMode)slot->mode, slot));
	}
	UT_ASSERT(cluster_cf_slot_clear_exact((ClusterCfSlotMode)slot->mode, slot));
}


/* ============================================================
 * U -- CF observability counters: NULL-safe before init.
 * ============================================================ */
UT_TEST(test_cf_counters_null_safe_before_init)
{
	/* Before cluster_cf_stats_shmem_init(), the state pointer is NULL; inc is
	 * a no-op and read returns 0 rather than dereferencing NULL. */
	cluster_cf_counter_inc(CLUSTER_CF_X_ACQUIRE);
	UT_ASSERT_EQ((int)cluster_cf_counter_read(CLUSTER_CF_X_ACQUIRE), 0);
}

UT_TEST(test_cf_slot_layout_and_init)
{
	ClusterCfSlotCensus census;
	Size size_three_backends;
	Size size_four_backends;

	cluster_cf_stats_shmem_register();
	UT_ASSERT_NOT_NULL(fake_registered_region);
	UT_ASSERT_EQ(fake_registered_region->lwlock_count, 1);

	MaxBackends = 3;
	size_three_backends = cluster_cf_stats_shmem_size();
	MaxBackends = 4;
	size_four_backends = cluster_cf_stats_shmem_size();
	UT_ASSERT_EQ(size_four_backends - size_three_backends, 2 * sizeof(ClusterCfPublishedSlot));
	MaxBackends = 3;

	memset(fake_cf_shmem.bytes, 0xa5, sizeof(fake_cf_shmem.bytes));
	fake_proc_global.allProcCount = (uint32)(MaxBackends + NUM_AUXILIARY_PROCS);
	cluster_cf_stats_shmem_init();

	UT_ASSERT_EQ(fake_cf_requested_size, size_three_backends);
	UT_ASSERT_EQ(fake_lwlock_init_count, 1);
	UT_ASSERT_EQ(fake_lwlock_tranche, LWTRANCHE_CLUSTER_CF);
	UT_ASSERT(cluster_cf_slot_census(&census));
	UT_ASSERT(census.valid);
	UT_ASSERT_EQ(census.x_held_count, 0);
	UT_ASSERT_EQ(census.s_held_count, 0);
	UT_ASSERT_EQ(census.x_release_pending_count, 0);
	UT_ASSERT_EQ(census.s_release_pending_count, 0);
	UT_ASSERT_EQ(census.pending_retry_count, 0);
	UT_ASSERT_EQ(census.invalid_count, 0);
	UT_ASSERT_EQ(census.x_owner_state, CLUSTER_CF_X_OWNER_EMPTY);
}


/* ============================================================
 * U -- CF observability counters: inc / read / independence / bounds.
 * ============================================================ */
UT_TEST(test_cf_counters_inc_read_bounds)
{
	cluster_cf_stats_shmem_init();

	/* All counters start at zero. */
	UT_ASSERT_EQ((int)cluster_cf_counter_read(CLUSTER_CF_X_ACQUIRE), 0);
	UT_ASSERT_EQ((int)cluster_cf_counter_read(CLUSTER_CF_BAK_FALLBACK), 0);

	/* Independent accumulation per counter. */
	cluster_cf_counter_inc(CLUSTER_CF_X_ACQUIRE);
	cluster_cf_counter_inc(CLUSTER_CF_X_ACQUIRE);
	cluster_cf_counter_inc(CLUSTER_CF_S_ACQUIRE);
	cluster_cf_counter_inc(CLUSTER_CF_FAILCLOSED);
	cluster_cf_counter_inc(CLUSTER_CF_S6_RELEASE_CONFIRMED);
	UT_ASSERT_EQ((int)cluster_cf_counter_read(CLUSTER_CF_X_ACQUIRE), 2);
	UT_ASSERT_EQ((int)cluster_cf_counter_read(CLUSTER_CF_S_ACQUIRE), 1);
	UT_ASSERT_EQ((int)cluster_cf_counter_read(CLUSTER_CF_FAILCLOSED), 1);
	UT_ASSERT_EQ((int)cluster_cf_counter_read(CLUSTER_CF_S6_RELEASE_CONFIRMED), 1);
	/* Untouched counters stay zero. */
	UT_ASSERT_EQ((int)cluster_cf_counter_read(CLUSTER_CF_SINGLE_NODE_AUTHORITY), 0);
	UT_ASSERT_EQ((int)cluster_cf_counter_read(CLUSTER_CF_BAK_FALLBACK), 0);

	/* Out-of-range index is a no-op / zero (bounds guard). */
	cluster_cf_counter_inc(CLUSTER_CF_COUNTER_COUNT);
	UT_ASSERT_EQ((int)cluster_cf_counter_read(CLUSTER_CF_COUNTER_COUNT), 0);
}

UT_TEST(test_cf_slot_census_counts_multi_proc_x_and_s)
{
	ClusterCfPublishedSlot x_held;
	ClusterCfPublishedSlot s_held;
	ClusterCfPublishedSlot s_pending;
	ClusterCfSlotCensus census;

	x_held = make_slot(0, CLUSTER_CF_SLOT_MODE_X, CLUSTER_CF_SLOT_HELD, true, 101);
	s_held = make_slot(1, CLUSTER_CF_SLOT_MODE_S, CLUSTER_CF_SLOT_HELD, false, 0);
	s_pending = make_slot(2, CLUSTER_CF_SLOT_MODE_S, CLUSTER_CF_SLOT_HELD, true, 303);

	UT_ASSERT(cluster_cf_slot_publish_held(CLUSTER_CF_SLOT_MODE_X, &x_held));
	UT_ASSERT(cluster_cf_slot_publish_held(CLUSTER_CF_SLOT_MODE_S, &s_held));
	UT_ASSERT(cluster_cf_slot_publish_held(CLUSTER_CF_SLOT_MODE_S, &s_pending));
	s_pending.state = CLUSTER_CF_SLOT_RELEASE_PENDING;
	UT_ASSERT(cluster_cf_slot_publish_release_pending(CLUSTER_CF_SLOT_MODE_S, &s_pending));

	fake_lwlock_acquire_count = 0;
	fake_lwlock_shared_count = 0;
	fake_lwlock_exclusive_count = 0;
	fake_lwlock_release_count = 0;
	UT_ASSERT(cluster_cf_slot_census(&census));
	UT_ASSERT(census.valid);
	UT_ASSERT_EQ(census.x_held_count, 1);
	UT_ASSERT_EQ(census.s_held_count, 1);
	UT_ASSERT_EQ(census.x_release_pending_count, 0);
	UT_ASSERT_EQ(census.s_release_pending_count, 1);
	UT_ASSERT_EQ(census.pending_retry_count, 1);
	UT_ASSERT_EQ(census.invalid_count, 0);
	UT_ASSERT_EQ(census.x_owner_state, CLUSTER_CF_X_OWNER_HELD);
	UT_ASSERT_EQ(census.x_owner.owner_pid, x_held.owner_pid);
	UT_ASSERT_EQ(census.x_owner.owner_procno, x_held.owner_procno);
	UT_ASSERT_EQ(census.x_owner.owner_start_ts_us, x_held.owner_start_ts_us);
	UT_ASSERT_EQ(census.x_owner.node_id, x_held.node_id);
	UT_ASSERT_EQ(census.x_owner.cluster_epoch, x_held.cluster_epoch);
	UT_ASSERT_EQ(census.x_owner.request_id, x_held.request_id);
	UT_ASSERT_EQ(census.x_owner.coordinated, x_held.coordinated);
	UT_ASSERT_EQ(fake_lwlock_acquire_count, 1);
	UT_ASSERT_EQ(fake_lwlock_shared_count, 1);
	UT_ASSERT_EQ(fake_lwlock_exclusive_count, 0);
	UT_ASSERT_EQ(fake_lwlock_release_count, 1);
	UT_ASSERT_EQ(fake_lwlock_held, 0);

	x_held.state = CLUSTER_CF_SLOT_RELEASE_PENDING;
	UT_ASSERT(cluster_cf_slot_publish_release_pending(CLUSTER_CF_SLOT_MODE_X, &x_held));
	UT_ASSERT(cluster_cf_slot_census(&census));
	UT_ASSERT(census.valid);
	UT_ASSERT_EQ(census.x_held_count, 0);
	UT_ASSERT_EQ(census.x_release_pending_count, 1);
	UT_ASSERT_EQ(census.s_release_pending_count, 1);
	UT_ASSERT_EQ(census.pending_retry_count, 2);
	UT_ASSERT_EQ(census.x_owner_state, CLUSTER_CF_X_OWNER_RELEASE_PENDING);
	UT_ASSERT_EQ(census.x_owner.request_id, x_held.request_id);

	clear_published_slot(&x_held);
	clear_published_slot(&s_held);
	clear_published_slot(&s_pending);
}

UT_TEST(test_cf_slot_census_fails_closed_on_capacity_drift)
{
	ClusterCfSlotCensus census;

	fake_proc_global.allProcCount++;
	fake_lwlock_acquire_count = 0;
	memset(&census, 0x7f, sizeof(census));
	UT_ASSERT(!cluster_cf_slot_census(&census));
	UT_ASSERT(!census.valid);
	UT_ASSERT_EQ(census.x_held_count, 0);
	UT_ASSERT_EQ(census.invalid_count, 0);
	UT_ASSERT_EQ(fake_lwlock_acquire_count, 0);
	fake_proc_global.allProcCount--;
}

UT_TEST(test_cf_slot_census_marks_multiple_x_owners_ambiguous)
{
	ClusterCfPublishedSlot x_held;
	ClusterCfPublishedSlot x_pending;
	ClusterCfSlotCensus census;

	x_held = make_slot(0, CLUSTER_CF_SLOT_MODE_X, CLUSTER_CF_SLOT_HELD, true, 401);
	x_pending = make_slot(1, CLUSTER_CF_SLOT_MODE_X, CLUSTER_CF_SLOT_HELD, true, 402);
	UT_ASSERT(cluster_cf_slot_publish_held(CLUSTER_CF_SLOT_MODE_X, &x_held));
	UT_ASSERT(cluster_cf_slot_publish_held(CLUSTER_CF_SLOT_MODE_X, &x_pending));
	x_pending.state = CLUSTER_CF_SLOT_RELEASE_PENDING;
	UT_ASSERT(cluster_cf_slot_publish_release_pending(CLUSTER_CF_SLOT_MODE_X, &x_pending));

	UT_ASSERT(!cluster_cf_slot_census(&census));
	UT_ASSERT(!census.valid);
	UT_ASSERT_EQ(census.x_held_count, 1);
	UT_ASSERT_EQ(census.x_release_pending_count, 1);
	UT_ASSERT_EQ(census.pending_retry_count, 1);
	UT_ASSERT_EQ(census.invalid_count, 1);
	UT_ASSERT_EQ(census.x_owner_state, CLUSTER_CF_X_OWNER_AMBIGUOUS);

	clear_published_slot(&x_held);
	clear_published_slot(&x_pending);
}

UT_TEST(test_cf_slot_census_counts_mode_contradiction_invalid)
{
	ClusterCfPublishedSlot malformed;
	ClusterCfSlotCensus census;

	malformed = make_slot(2, CLUSTER_CF_SLOT_MODE_S, CLUSTER_CF_SLOT_HELD, true, 501);
	UT_ASSERT(!cluster_cf_slot_publish_held(CLUSTER_CF_SLOT_MODE_X, &malformed));
	UT_ASSERT(!cluster_cf_slot_census(&census));
	UT_ASSERT(!census.valid);
	UT_ASSERT_EQ(census.x_held_count, 0);
	UT_ASSERT_EQ(census.x_release_pending_count, 0);
	UT_ASSERT_EQ(census.invalid_count, 1);
	UT_ASSERT_EQ(census.x_owner_state, CLUSTER_CF_X_OWNER_INVALID);
}

UT_TEST(test_cf_slot_census_is_fail_closed_before_init)
{
	ClusterCfSlotCensus census;

	memset(&census, 0x7f, sizeof(census));
	UT_ASSERT(!cluster_cf_slot_census(&census));
	UT_ASSERT(!census.valid);
	UT_ASSERT_EQ(census.x_held_count, 0);
	UT_ASSERT_EQ(census.s_held_count, 0);
	UT_ASSERT_EQ(census.x_release_pending_count, 0);
	UT_ASSERT_EQ(census.s_release_pending_count, 0);
	UT_ASSERT_EQ(census.pending_retry_count, 0);
	UT_ASSERT_EQ(census.invalid_count, 0);
	UT_ASSERT(!cluster_cf_slot_census(NULL));
}


UT_DEFINE_GLOBALS();


int
main(int argc pg_attribute_unused(), char **const argv pg_attribute_unused())
{
	UT_PLAN(8);

	UT_RUN(test_cf_counters_null_safe_before_init);
	UT_RUN(test_cf_slot_census_is_fail_closed_before_init);
	UT_RUN(test_cf_slot_layout_and_init);
	UT_RUN(test_cf_counters_inc_read_bounds);
	UT_RUN(test_cf_slot_census_counts_multi_proc_x_and_s);
	UT_RUN(test_cf_slot_census_fails_closed_on_capacity_drift);
	UT_RUN(test_cf_slot_census_marks_multiple_x_owners_ambiguous);
	UT_RUN(test_cf_slot_census_counts_mode_contradiction_invalid);

	UT_DONE();
	return ut_failed_count == 0 ? 0 : 1;
}
