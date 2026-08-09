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
#include "miscadmin.h"
#include "port/atomics.h"

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


AuxProcType MyAuxProcType = NotAnAuxProcess;

/* ============================================================
 * PG runtime stubs.
 * ============================================================ */

static pg_atomic_uint64 cf_buf[2][CLUSTER_CF_COUNTER_COUNT + 2];
static bool cf_initialized[2];
static int cf_buf_slot;

void
ExceptionalCondition(const char *conditionName pg_attribute_unused(),
					 const char *fileName pg_attribute_unused(),
					 int lineNumber pg_attribute_unused())
{
	abort();
}

/*
 * ShmemInitStruct stub -- a static pg_atomic_uint64 array is naturally 8-byte
 * aligned and matches the ClusterCfStatsSharedState layout (counters[]), so it
 * backs the region without a force-align union.
 */
void *
ShmemInitStruct(const char *name, Size size, bool *foundPtr)
{
	if (name != NULL && strcmp(name, "pgrac cluster cf stats") == 0) {
		/* counters[] plus two uint32 atomics, with one spare aligned slot. */
		Assert(size <= sizeof(cf_buf[cf_buf_slot])); /* catch shmem layout growth */
		*foundPtr = cf_initialized[cf_buf_slot];
		cf_initialized[cf_buf_slot] = true;
		return cf_buf[cf_buf_slot];
	}

	*foundPtr = true;
	return NULL;
}

void
cluster_shmem_register_region(const void *r pg_attribute_unused())
{}


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
	UT_ASSERT_EQ((int)cluster_cf_counter_read(CLUSTER_CF_X_ACQUIRE), 2);
	UT_ASSERT_EQ((int)cluster_cf_counter_read(CLUSTER_CF_S_ACQUIRE), 1);
	UT_ASSERT_EQ((int)cluster_cf_counter_read(CLUSTER_CF_FAILCLOSED), 1);
	/* Untouched counters stay zero. */
	UT_ASSERT_EQ((int)cluster_cf_counter_read(CLUSTER_CF_SINGLE_NODE_AUTHORITY), 0);
	UT_ASSERT_EQ((int)cluster_cf_counter_read(CLUSTER_CF_BAK_FALLBACK), 0);

	/* Out-of-range index is a no-op / zero (bounds guard). */
	cluster_cf_counter_inc(CLUSTER_CF_COUNTER_COUNT);
	UT_ASSERT_EQ((int)cluster_cf_counter_read(CLUSTER_CF_COUNTER_COUNT), 0);
}


/* ============================================================
 * R18 -- exact actor-bound OWNER -> EOR phase lifecycle.
 * ============================================================ */
UT_TEST(test_owner_eor_phase_exact_lifecycle)
{
	cf_buf_slot = 0;
	memset(cf_buf[cf_buf_slot], 0xA5, sizeof(cf_buf[cf_buf_slot]));
	cf_initialized[cf_buf_slot] = false;
	cluster_cf_stats_shmem_init();
	UT_ASSERT_EQ(cluster_cf_owner_eor_phase_read(), CLUSTER_CF_OWNER_EOR_EMPTY);

	/* Only Startup may install/clear, and only the checkpointer activates/dones. */
	MyAuxProcType = CheckpointerProcess;
	UT_ASSERT(!cluster_cf_owner_eor_phase_install());
	MyAuxProcType = StartupProcess;
	UT_ASSERT(cluster_cf_owner_eor_phase_install());
	UT_ASSERT(!cluster_cf_owner_eor_phase_install());
	UT_ASSERT_EQ(cluster_cf_owner_eor_phase_read(), CLUSTER_CF_OWNER_EOR_INSTALLED);

	MyAuxProcType = StartupProcess;
	UT_ASSERT(!cluster_cf_owner_eor_phase_activate());
	MyAuxProcType = CheckpointerProcess;
	UT_ASSERT(cluster_cf_owner_eor_phase_activate());
	UT_ASSERT(!cluster_cf_owner_eor_phase_activate());
	UT_ASSERT_EQ(cluster_cf_owner_eor_phase_read(), CLUSTER_CF_OWNER_EOR_ACTIVE);

	MyAuxProcType = StartupProcess;
	UT_ASSERT(!cluster_cf_owner_eor_phase_clear());
	MyAuxProcType = CheckpointerProcess;
	UT_ASSERT(cluster_cf_owner_eor_phase_done());
	UT_ASSERT(!cluster_cf_owner_eor_phase_done());
	UT_ASSERT_EQ(cluster_cf_owner_eor_phase_read(), CLUSTER_CF_OWNER_EOR_DONE);

	MyAuxProcType = CheckpointerProcess;
	UT_ASSERT(!cluster_cf_owner_eor_phase_clear());
	MyAuxProcType = StartupProcess;
	UT_ASSERT(cluster_cf_owner_eor_phase_clear());
	UT_ASSERT_EQ(cluster_cf_owner_eor_phase_read(), CLUSTER_CF_OWNER_EOR_EMPTY);
}


/* ============================================================
 * R18 -- found attach preserves live phase; fresh postmaster starts EMPTY.
 * ============================================================ */
UT_TEST(test_owner_eor_phase_attach_and_fresh_init)
{
	cf_buf_slot = 0;
	MyAuxProcType = StartupProcess;
	UT_ASSERT(cluster_cf_owner_eor_phase_install());
	MyAuxProcType = CheckpointerProcess;
	UT_ASSERT(cluster_cf_owner_eor_phase_activate());

	/* found=true reattach must not overwrite ACTIVE. */
	cluster_cf_stats_shmem_init();
	UT_ASSERT_EQ(cluster_cf_owner_eor_phase_read(), CLUSTER_CF_OWNER_EOR_ACTIVE);

	/* A separate fresh region may contain garbage before init, but starts EMPTY. */
	cf_buf_slot = 1;
	memset(cf_buf[cf_buf_slot], 0x5A, sizeof(cf_buf[cf_buf_slot]));
	cf_initialized[cf_buf_slot] = false;
	cluster_cf_stats_shmem_init();
	UT_ASSERT_EQ(cluster_cf_owner_eor_phase_read(), CLUSTER_CF_OWNER_EOR_EMPTY);

	/* The clean nondelegated path is the only other legal clear edge. */
	MyAuxProcType = StartupProcess;
	UT_ASSERT(cluster_cf_owner_eor_phase_install());
	UT_ASSERT(cluster_cf_owner_eor_phase_clear());
	UT_ASSERT_EQ(cluster_cf_owner_eor_phase_read(), CLUSTER_CF_OWNER_EOR_EMPTY);
	MyAuxProcType = NotAnAuxProcess;
}


UT_DEFINE_GLOBALS();


int
main(int argc pg_attribute_unused(), char **const argv pg_attribute_unused())
{
	UT_PLAN(4);

	UT_RUN(test_cf_counters_null_safe_before_init);
	UT_RUN(test_cf_counters_inc_read_bounds);
	UT_RUN(test_owner_eor_phase_exact_lifecycle);
	UT_RUN(test_owner_eor_phase_attach_and_fresh_init);

	UT_DONE();
	return ut_failed_count == 0 ? 0 : 1;
}
