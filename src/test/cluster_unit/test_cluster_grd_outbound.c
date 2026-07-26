/*-------------------------------------------------------------------------
 *
 * test_cluster_grd_outbound.c
 *    Standalone regression tests for reliable GES cleanup staging.
 *
 * Portions Copyright (c) 1996-2024, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2026, pgrac contributors
 *
 * Author: SqlRush <sqlrush@gmail.com>
 *
 * IDENTIFICATION
 *    src/test/cluster_unit/test_cluster_grd_outbound.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include <signal.h>
#include <stdlib.h>
#include <string.h>
#include <sys/wait.h>
#include <unistd.h>

#include "cluster/cluster_gcs_block.h"
#include "cluster/cluster_ges.h"
#include "cluster/cluster_grd.h"
#include "cluster/cluster_grd_outbound.h"
#include "cluster/cluster_ic.h"
#include "cluster/cluster_ic_router.h"
#include "cluster/cluster_inject.h"
#include "cluster/cluster_lmon.h"
#include "cluster/cluster_lms.h"
#include "cluster/cluster_sf_dep.h"
#include "cluster/cluster_shmem.h"
#include "miscadmin.h"
#include "storage/lwlock.h"

#undef printf

#include "unit_test.h"

UT_DEFINE_GLOBALS();

ProcessingMode Mode = NormalProcessing;
int cluster_node_id = 0;
int cluster_lms_workers = 1;
int cluster_lmon_main_loop_interval = 1000;
int MaxBackends = 200;

static bool ut_capability_generation_matches = true;
static uint32 ut_required_capability_seen;
static uint32 ut_capability_generation_seen;
static bool ut_cf_s6_double_full_armed;

int cluster_injection_armed_count = 0;

bool
cluster_injection_is_armed(const char *name)
{
	return ut_cf_s6_double_full_armed
		   && strcmp(name, "cluster-cf-s6-outbound-double-full") == 0;
}

bool
cluster_sf_peer_capability_generation_matches(
	int32 peer_id pg_attribute_unused(),
	uint32 required_capabilities,
	uint32 expected_generation)
{
	ut_required_capability_seen = required_capabilities;
	ut_capability_generation_seen = expected_generation;
	return ut_capability_generation_matches;
}

void
ExceptionalCondition(const char *conditionName, const char *fileName, int lineNumber)
{
	printf("# Assert failed: %s at %s:%d\n", conditionName, fileName, lineNumber);
	abort();
}

static uint64 ut_log_count;
static char ut_last_internal_log_format[256];

bool
errstart(int elevel, const char *domain pg_attribute_unused())
{
	if (elevel == LOG) {
		ut_log_count++;
		return true;
	}
	return false;
}

bool
errstart_cold(int elevel pg_attribute_unused(), const char *domain pg_attribute_unused())
{
	return false;
}

void
errfinish(const char *filename pg_attribute_unused(), int lineno pg_attribute_unused(),
		  const char *funcname pg_attribute_unused())
{}

int
errmsg_internal(const char *fmt, ...)
{
	strlcpy(ut_last_internal_log_format, fmt,
			sizeof(ut_last_internal_log_format));
	return 0;
}

static const ClusterShmemRegion *ut_region;
static LWLockPadded ut_lock;

void *
ShmemInitStruct(const char *name pg_attribute_unused(), Size size, bool *found)
{
	void *p = malloc(size);

	UT_ASSERT(p != NULL);
	memset(p, 0, size);
	*found = false;
	return p;
}

void
cluster_shmem_register_region(const ClusterShmemRegion *region)
{
	ut_region = region;
}

LWLockPadded *
GetNamedLWLockTranche(const char *tranche_name pg_attribute_unused())
{
	return &ut_lock;
}

bool
LWLockAcquire(LWLock *lock pg_attribute_unused(), LWLockMode mode pg_attribute_unused())
{
	return true;
}

void
LWLockRelease(LWLock *lock pg_attribute_unused())
{}

static uint64 ut_cleanup_deferred;
static uint64 ut_reply_deferred;
static uint64 ut_reply_dropped;
static uint64 ut_lmon_wakeup;

void
cluster_grd_inc_ges_cleanup_deferred(void)
{
	ut_cleanup_deferred++;
}

void
cluster_grd_inc_ges_reply_deferred(void)
{
	ut_reply_deferred++;
}

void
cluster_grd_inc_ges_reply_dropped(void)
{
	ut_reply_dropped++;
}

void
cluster_lmon_duty_mark_dirty(ClusterLmonDuty duty pg_attribute_unused())
{}

void
cluster_lmon_wakeup(void)
{
	ut_lmon_wakeup++;
}

const ClusterICMsgTypeInfo *
cluster_ic_get_msg_type_info(uint8 msg_type pg_attribute_unused())
{
	return NULL;
}

int
cluster_gcs_block_payload_shard(uint8 msg_type pg_attribute_unused(),
								const void *payload pg_attribute_unused(),
								uint16 payload_len pg_attribute_unused(),
								int nworkers pg_attribute_unused())
{
	return -1;
}

bool
cluster_lms_outbound_enqueue(int worker_id pg_attribute_unused(),
							 uint8 msg_type pg_attribute_unused(),
							 uint32 dest_node_id pg_attribute_unused(),
							 const void *payload pg_attribute_unused(),
							 uint16 payload_len pg_attribute_unused())
{
	return false;
}

void
cluster_gcs_block_lmon_prepare_outbound_request(GcsBlockRequestPayload *req pg_attribute_unused(),
												int32 dest_node pg_attribute_unused())
{}

static ClusterICSendResult ut_send_result = CLUSTER_IC_SEND_DONE;
static uint64 ut_send_count;
static uint64 ut_release_seen[2048];
static int ut_release_seen_count;
static int ut_cleanup_bypass_seen_count;

ClusterICSendResult
cluster_ic_send_envelope(uint8 msg_type pg_attribute_unused(),
						 int32 dest_node_id pg_attribute_unused(), const void *payload,
						 uint32 payload_len)
{
	ut_send_count++;
	if (payload != NULL && payload_len == sizeof(GesRequestPayload)
		&& ((const GesRequestPayload *)payload)->opcode == GES_REQ_OPCODE_RELEASE
		&& ut_release_seen_count < (int)lengthof(ut_release_seen)) {
		const GesRequestPayload *rel = (const GesRequestPayload *)payload;

		ut_release_seen[ut_release_seen_count++]
			= ((uint64)rel->holder_request_id_lo) | (((uint64)rel->holder_request_id_hi) << 32);
		if (rel->current_mode == GES_RELEASE_CURRENT_MODE_CLEANUP_BYPASS)
			ut_cleanup_bypass_seen_count++;
	}
	return ut_send_result;
}

static void
ut_reset_state(void)
{
	UT_ASSERT(ut_region != NULL);
	ut_region->init_fn();
	ut_send_result = CLUSTER_IC_SEND_DONE;
	ut_send_count = 0;
	ut_release_seen_count = 0;
	ut_cleanup_bypass_seen_count = 0;
	ut_log_count = 0;
	memset(ut_last_internal_log_format, 0,
		   sizeof(ut_last_internal_log_format));
	ut_capability_generation_matches = true;
	ut_required_capability_seen = 0;
	ut_capability_generation_seen = 0;
	ut_cf_s6_double_full_armed = false;
	memset(ut_release_seen, 0, sizeof(ut_release_seen));
}

static GesRequestPayload
ut_release(uint64 request_id)
{
	GesRequestPayload rel;

	memset(&rel, 0, sizeof(rel));
	rel.opcode = GES_REQ_OPCODE_RELEASE;
	rel.holder_node_id = 0;
	rel.holder_procno = 17;
	rel.holder_request_id_lo = (uint32)(request_id & UINT64CONST(0xffffffff));
	rel.holder_request_id_hi = (uint32)(request_id >> 32);
	return rel;
}

static void
ut_fill_main_ring(void)
{
	uint8 payload = 0xA5;
	int i;

	for (i = 0; i < PGRAC_GES_OUTBOUND_RING_CAPACITY; i++)
		cluster_grd_outbound_enqueue_lmon_reply(1, &payload, sizeof(payload));
	UT_ASSERT_EQ(cluster_grd_outbound_ring_depth(), (uint32)PGRAC_GES_OUTBOUND_RING_CAPACITY);
}

UT_TEST(test_cleanup_retry_queue_never_overwrites_oldest)
{
	int i;

	ut_reset_state();
	ut_fill_main_ring();

	for (i = 1; i <= 65; i++) {
		GesRequestPayload rel = ut_release((uint64)i);

		cluster_grd_outbound_enqueue_cleanup_release(1, &rel, sizeof(rel));
	}

	/* Pre-fix cleanup_dirty overwrote request_id=1 at the old 64-slot limit. */
	UT_ASSERT_EQ(cluster_grd_outbound_cleanup_dirty_depth(), (uint32)65);

	/* Drain the filler, then the deferred releases. */
	while (cluster_grd_outbound_ring_depth() > 0 || cluster_grd_outbound_cleanup_dirty_depth() > 0)
		(void)cluster_grd_outbound_lmon_drain_send();

	UT_ASSERT_EQ(ut_release_seen_count, 65);
	for (i = 0; i < 65; i++)
		UT_ASSERT_EQ(ut_release_seen[i], (uint64)(i + 1));
}

UT_TEST(test_cleanup_hard_error_is_deferred_for_retry)
{
	GesRequestPayload rel;

	ut_reset_state();
	rel = ut_release(UINT64CONST(0xABCDEF));
	cluster_grd_outbound_enqueue_cleanup_release(3, &rel, sizeof(rel));

	ut_send_result = CLUSTER_IC_SEND_HARD_ERROR;
	(void)cluster_grd_outbound_lmon_drain_send();
	UT_ASSERT_EQ(cluster_grd_outbound_ring_depth() + cluster_grd_outbound_cleanup_dirty_depth(),
				 (uint32)1);

	ut_send_result = CLUSTER_IC_SEND_DONE;
	(void)cluster_grd_outbound_lmon_drain_send();
	UT_ASSERT_EQ(cluster_grd_outbound_ring_depth() + cluster_grd_outbound_cleanup_dirty_depth(),
				 (uint32)0);
	UT_ASSERT_EQ(ut_release_seen_count, 2);
	UT_ASSERT_EQ(ut_release_seen[0], UINT64CONST(0xABCDEF));
	UT_ASSERT_EQ(ut_release_seen[1], UINT64CONST(0xABCDEF));
}

UT_TEST(test_cleanup_retry_pressure_logs_once_per_postmaster_lifetime)
{
	int i;

	ut_reset_state();
	ut_fill_main_ring();

	for (i = 1; i < PGRAC_GES_CLEANUP_DIRTY_WARN50_DEPTH; i++) {
		GesRequestPayload rel = ut_release((uint64)i);

		cluster_grd_outbound_enqueue_cleanup_release(1, &rel, sizeof(rel));
	}
	UT_ASSERT_EQ(cluster_grd_outbound_cleanup_retry_warn50_count(), UINT64CONST(0));
	UT_ASSERT_EQ(cluster_grd_outbound_cleanup_retry_warn90_count(), UINT64CONST(0));
	UT_ASSERT_EQ(ut_log_count, UINT64CONST(0));

	{
		GesRequestPayload rel = ut_release((uint64)PGRAC_GES_CLEANUP_DIRTY_WARN50_DEPTH);

		cluster_grd_outbound_enqueue_cleanup_release(1, &rel, sizeof(rel));
	}
	UT_ASSERT_EQ(cluster_grd_outbound_cleanup_retry_warn50_count(), UINT64CONST(1));
	UT_ASSERT_EQ(cluster_grd_outbound_cleanup_retry_warn90_count(), UINT64CONST(0));
	UT_ASSERT_EQ(ut_log_count, UINT64CONST(1));

	for (i = PGRAC_GES_CLEANUP_DIRTY_WARN50_DEPTH + 1;
		 i <= PGRAC_GES_CLEANUP_DIRTY_WARN90_DEPTH + 1; i++) {
		GesRequestPayload rel = ut_release((uint64)i);

		cluster_grd_outbound_enqueue_cleanup_release(1, &rel, sizeof(rel));
	}
	UT_ASSERT_EQ(cluster_grd_outbound_cleanup_retry_warn50_count(), UINT64CONST(1));
	UT_ASSERT_EQ(cluster_grd_outbound_cleanup_retry_warn90_count(), UINT64CONST(1));
	UT_ASSERT_EQ(ut_log_count, UINT64CONST(2));

	/* Draining below both thresholds does not re-arm lifetime LOG-once. */
	while (cluster_grd_outbound_ring_depth() > 0 || cluster_grd_outbound_cleanup_dirty_depth() > 0)
		(void)cluster_grd_outbound_lmon_drain_send();
	ut_fill_main_ring();
	for (i = 1; i <= PGRAC_GES_CLEANUP_DIRTY_WARN90_DEPTH; i++) {
		GesRequestPayload rel = ut_release((uint64)i);

		cluster_grd_outbound_enqueue_cleanup_release(1, &rel, sizeof(rel));
	}
	UT_ASSERT_EQ(cluster_grd_outbound_cleanup_retry_warn50_count(), UINT64CONST(1));
	UT_ASSERT_EQ(cluster_grd_outbound_cleanup_retry_warn90_count(), UINT64CONST(1));
	UT_ASSERT_EQ(ut_log_count, UINT64CONST(2));
}

UT_TEST(test_cleanup_release_is_marked_dedup_bypass)
{
	GesRequestPayload rel;

	ut_reset_state();
	rel = ut_release(99);
	UT_ASSERT_EQ(rel.current_mode, 0);
	cluster_grd_outbound_enqueue_cleanup_release(1, &rel, sizeof(rel));
	(void)cluster_grd_outbound_lmon_drain_send();
	UT_ASSERT_EQ(ut_release_seen_count, 1);
	UT_ASSERT_EQ(ut_cleanup_bypass_seen_count, 1);
}

/*
 * Removing the nonthrow scope, treating a failed enqueue as success, or
 * changing retry to a rebuilt request must fail this test.
 */
UT_TEST(test_cleanup_release_saturation_is_nonthrowing_and_exactly_retryable)
{
	ClusterGrdOutboundSlot discarded;
	GesRequestPayload exact;
	uint64 exact_request_id = UINT64CONST(0x1122334455667788);
	pid_t child;
	int child_status;
	int i;

	ut_reset_state();
	ut_fill_main_ring();
	for (i = 1; i <= PGRAC_GES_CLEANUP_DIRTY_BUDGET; i++) {
		GesRequestPayload rel = ut_release((uint64)i);

		cluster_grd_outbound_enqueue_cleanup_release(1, &rel, sizeof(rel));
	}
	UT_ASSERT_EQ(cluster_grd_outbound_ring_depth(),
				 (uint32)PGRAC_GES_OUTBOUND_RING_CAPACITY);
	UT_ASSERT_EQ(cluster_grd_outbound_cleanup_dirty_depth(),
				 (uint32)PGRAC_GES_CLEANUP_DIRTY_BUDGET);

	exact = ut_release(exact_request_id);
	cluster_grd_outbound_cleanup_release_nonthrow_begin();
	cluster_grd_outbound_enqueue_cleanup_release(3, &exact, sizeof(exact));
	UT_ASSERT(!cluster_grd_outbound_cleanup_release_nonthrow_end());
	UT_ASSERT_EQ(cluster_grd_outbound_ring_depth(),
				 (uint32)PGRAC_GES_OUTBOUND_RING_CAPACITY);
	UT_ASSERT_EQ(cluster_grd_outbound_cleanup_dirty_depth(),
				 (uint32)PGRAC_GES_CLEANUP_DIRTY_BUDGET);
	UT_ASSERT_EQ(((uint64)exact.holder_request_id_lo
				  | ((uint64)exact.holder_request_id_hi << 32)),
				 exact_request_id);

	/*
	 * Remote S6 terminalization also stages its committed dedup intent on
	 * this same fixed queue.  The journal owns the exact retry, so saturation
	 * must be reported through the same nonthrow scope.
	 */
	cluster_grd_outbound_cleanup_release_nonthrow_begin();
	cluster_grd_outbound_enqueue_ges_dedup_lifecycle(
		PGRAC_IC_MSG_GES_DEDUP_DONE, 3, &exact, sizeof(exact), 17);
	UT_ASSERT(!cluster_grd_outbound_cleanup_release_nonthrow_end());

	/*
	 * The exception is scope-bound: ordinary producers still fail closed
	 * instead of silently dropping correctness cleanup.
	 */
	fflush(stdout);
	child = fork();
	UT_ASSERT(child >= 0);
	if (child == 0) {
		cluster_grd_outbound_enqueue_cleanup_release(3, &exact, sizeof(exact));
		_exit(0);
	}
	UT_ASSERT_EQ(waitpid(child, &child_status, 0), child);
	UT_ASSERT(WIFSIGNALED(child_status));
	UT_ASSERT_EQ(WTERMSIG(child_status), SIGABRT);

	/* Free one real ring slot, then retry the caller-retained exact payload. */
	UT_ASSERT(cluster_grd_outbound_dequeue(&discarded));
	cluster_grd_outbound_cleanup_release_nonthrow_begin();
	cluster_grd_outbound_enqueue_cleanup_release(3, &exact, sizeof(exact));
	UT_ASSERT(cluster_grd_outbound_cleanup_release_nonthrow_end());

	while (cluster_grd_outbound_ring_depth() > 0
		   || cluster_grd_outbound_cleanup_dirty_depth() > 0)
		(void)cluster_grd_outbound_lmon_drain_send();

	UT_ASSERT_EQ(ut_release_seen_count, PGRAC_GES_CLEANUP_DIRTY_BUDGET + 1);
	UT_ASSERT_EQ(ut_release_seen[0], exact_request_id);
	for (i = 1; i <= PGRAC_GES_CLEANUP_DIRTY_BUDGET; i++)
		UT_ASSERT_EQ(ut_release_seen[i], (uint64)i);
}

/*
 * Cassert product-bound seam for t/417: when the dedicated point is armed
 * inside a real S6 nonthrow scope, the actual dedup terminal producer sees a
 * physically full main ring + cleanup dirty list and reports failure without
 * terminating the process.  The seam must then remove only its own markers
 * before unlocking so the committed DONE journal cannot race an LMON retry
 * into the artificial full/full state and PANIC.
 */
UT_TEST(test_cf_s6_product_seam_physically_saturates_and_drains)
{
	ClusterGrdOutboundSlot marker;
	GesRequestPayload exact;
	uint64 wakeup_before;

	ut_reset_state();
	exact = ut_release(UINT64CONST(0xA1B2C3D4E5F60718));
	ut_cf_s6_double_full_armed = true;

	/*
	 * The ordinary cleanup RELEASE producer runs earlier on some S6 error
	 * paths.  It must not consume the one-shot seam or emit the lifecycle
	 * proof log; otherwise t/417 can go green without reaching DONE.
	 */
	cluster_grd_outbound_cleanup_release_nonthrow_begin();
	cluster_grd_outbound_enqueue_cleanup_release(3, &exact, sizeof(exact));
	UT_ASSERT(cluster_grd_outbound_cleanup_release_nonthrow_end());
	UT_ASSERT_EQ(cluster_grd_outbound_ring_depth(), (uint32)1);
	UT_ASSERT_EQ(cluster_grd_outbound_cleanup_dirty_depth(), (uint32)0);
	UT_ASSERT_EQ(ut_log_count, UINT64CONST(0));
	UT_ASSERT(cluster_grd_outbound_dequeue(&marker));
	UT_ASSERT_EQ(marker.origin, CLUSTER_GRD_OUTBOUND_CLEANUP_RELEASE);
	wakeup_before = ut_lmon_wakeup;

	cluster_grd_outbound_cleanup_release_nonthrow_begin();
	cluster_grd_outbound_enqueue_ges_dedup_lifecycle(
		PGRAC_IC_MSG_GES_DEDUP_DONE, 3, &exact, sizeof(exact), 19);
	UT_ASSERT(!cluster_grd_outbound_cleanup_release_nonthrow_end());
	UT_ASSERT_EQ(cluster_grd_outbound_ring_depth(), (uint32)0);
	UT_ASSERT_EQ(cluster_grd_outbound_cleanup_dirty_depth(), (uint32)0);
	UT_ASSERT_EQ(ut_lmon_wakeup, wakeup_before);
	UT_ASSERT_EQ(ut_log_count, UINT64CONST(1));
	UT_ASSERT(strstr(ut_last_internal_log_format,
					 "producer=ges_dedup_lifecycle") != NULL);
	UT_ASSERT(strstr(ut_last_internal_log_format,
					 "message=GES_DEDUP_DONE") != NULL);

	ut_cf_s6_double_full_armed = false;
	UT_ASSERT_EQ(cluster_grd_outbound_ring_depth(), (uint32)0);
	UT_ASSERT_EQ(cluster_grd_outbound_cleanup_dirty_depth(), (uint32)0);
}

UT_TEST(test_capability_bound_backend_request_drops_on_generation_change)
{
	GesRequestPayload req;

	ut_reset_state();
	memset(&req, 0, sizeof(req));
	req.opcode = GES_REQ_OPCODE_REQUEST;
	UT_ASSERT(cluster_grd_outbound_enqueue_backend_request_capability(
		2, &req, sizeof(req), PGRAC_IC_HELLO_CAP_GES_DEDUP_DONE_V1, 71));
	ut_capability_generation_matches = false;
	(void)cluster_grd_outbound_lmon_drain_send();
	UT_ASSERT_EQ(ut_send_count, UINT64CONST(0));
	UT_ASSERT_EQ(cluster_grd_outbound_ring_depth(), (uint32)0);
	UT_ASSERT_EQ(ut_required_capability_seen,
				 PGRAC_IC_HELLO_CAP_GES_DEDUP_DONE_V1);
	UT_ASSERT_EQ(ut_capability_generation_seen, (uint32)71);
}

UT_TEST(test_capability_bound_backend_request_sends_on_exact_generation)
{
	GesRequestPayload req;

	ut_reset_state();
	memset(&req, 0, sizeof(req));
	req.opcode = GES_REQ_OPCODE_REQUEST;
	UT_ASSERT(cluster_grd_outbound_enqueue_backend_request_capability(
		2, &req, sizeof(req), PGRAC_IC_HELLO_CAP_GES_DEDUP_DONE_V1, 73));
	(void)cluster_grd_outbound_lmon_drain_send();
	UT_ASSERT_EQ(ut_send_count, UINT64CONST(1));
	UT_ASSERT_EQ(cluster_grd_outbound_ring_depth(), (uint32)0);
	UT_ASSERT_EQ(ut_required_capability_seen,
				 PGRAC_IC_HELLO_CAP_GES_DEDUP_DONE_V1);
	UT_ASSERT_EQ(ut_capability_generation_seen, (uint32)73);
}

/*
 * A capability-bound dedup lifecycle frame may overflow into cleanup dirty.
 * Moving it back into the main ring must preserve the reconnect generation;
 * otherwise a stale pre-reconnect DONE could cross the replacement link.
 */
UT_TEST(test_capability_generation_survives_cleanup_dirty_drain)
{
	ClusterGrdOutboundSlot slot;
	GesRequestPayload req;
	int i;

	ut_reset_state();
	ut_fill_main_ring();
	memset(&req, 0, sizeof(req));
	req.opcode = GES_REQ_OPCODE_RELEASE;
	cluster_grd_outbound_enqueue_ges_dedup_lifecycle(
		PGRAC_IC_MSG_GES_DEDUP_DONE, 2, &req, sizeof(req), 79);
	UT_ASSERT_EQ(cluster_grd_outbound_cleanup_dirty_depth(), (uint32)1);

	UT_ASSERT(cluster_grd_outbound_dequeue(&slot));
	UT_ASSERT_EQ(cluster_grd_outbound_drain_dirty_lists(), 1);
	for (i = 1; i < PGRAC_GES_OUTBOUND_RING_CAPACITY; i++)
		UT_ASSERT(cluster_grd_outbound_dequeue(&slot));
	UT_ASSERT(cluster_grd_outbound_dequeue(&slot));

	UT_ASSERT_EQ(slot.msg_type, PGRAC_IC_MSG_GES_DEDUP_DONE);
	UT_ASSERT_EQ(slot.origin, CLUSTER_GRD_OUTBOUND_GES_DEDUP_LIFECYCLE);
	UT_ASSERT_EQ(slot.required_capability,
				 PGRAC_IC_HELLO_CAP_GES_DEDUP_DONE_V1);
	UT_ASSERT_EQ(slot.capability_generation, (uint32)79);
}

int
main(void)
{
	cluster_grd_outbound_shmem_register();
	UT_PLAN(9);

	UT_RUN(test_cleanup_retry_queue_never_overwrites_oldest);
	UT_RUN(test_cleanup_hard_error_is_deferred_for_retry);
	UT_RUN(test_cleanup_retry_pressure_logs_once_per_postmaster_lifetime);
	UT_RUN(test_cleanup_release_is_marked_dedup_bypass);
	UT_RUN(test_cleanup_release_saturation_is_nonthrowing_and_exactly_retryable);
	UT_RUN(test_cf_s6_product_seam_physically_saturates_and_drains);
	UT_RUN(test_capability_bound_backend_request_drops_on_generation_change);
	UT_RUN(test_capability_bound_backend_request_sends_on_exact_generation);
	UT_RUN(test_capability_generation_survives_cleanup_dirty_drain);

	UT_DONE();
	return ut_failed_count == 0 ? 0 : 1;
}
