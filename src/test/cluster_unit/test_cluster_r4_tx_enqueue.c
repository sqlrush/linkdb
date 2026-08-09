/*-------------------------------------------------------------------------
 *
 * test_cluster_r4_tx_enqueue.c
 *    R4 D9 exact TARGET transaction-wait contract.
 *
 * The production source is included so this standalone fixture can inspect
 * its private fixed-size waiter slots and prove cleanup after every exit.
 * All external resolver, latch, WFG, epoch and shmem services are scripted.
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "access/xact.h"
#include "cluster/cluster_tx_enqueue.h"
#include "miscadmin.h"
#include "storage/proc.h"
#include "utils/elog.h"

#include "../../backend/cluster/cluster_tx_enqueue.c"

#undef printf
#undef fprintf
#undef snprintf

#include "unit_test.h"

UT_DEFINE_GLOBALS();

#define TEST_NSLOTS 2
#define TEST_XID ((TransactionId)900)
#define TEST_EPOCH UINT64CONST(77)

static char test_txw_storage[MAXALIGN(offsetof(ClusterTxwShmem, slots)
											 + TEST_NSLOTS * sizeof(ClusterTxwWaitSlot))];
static PGPROC test_procs[TEST_NSLOTS];
static PROC_HDR test_proc_hdr;

static ClusterTxOutcome test_resolve_outcomes[16];
static ClusterTxResolveReason test_resolve_reasons[16];
static int test_resolve_count;
static int test_resolve_pos;
static uint64 test_epochs[16];
static int test_epoch_count;
static int test_epoch_pos;
static PcmXQueueResult test_guard_result;
static NodeId test_uba_origin;
static bool test_wfg_accept;
static bool test_wfg_live;
static int test_wfg_submit_calls;
static int test_wfg_cancel_calls;
static ClusterLmdVertex test_wfg_waiter;
static ClusterLmdVertex test_wfg_blocker;
static int test_wait_publish_calls;
static int test_wait_clear_calls;
static uint8 test_wait_kind;
static uint64 test_wait_epoch;
static ClusterLmdWaitStateReadResult test_wait_read_result;
static ClusterLmdWaitStateSnapshot test_wait_snapshot;
static int test_wait_latch_calls;
static int test_set_latch_calls[TEST_NSLOTS];
static bool test_wait_latch_throws;
static bool test_wait_latch_sleeps;
static TransactionId test_local_xid;
static bool test_fail_stop_armed;
static sigjmp_buf test_fail_stop_stack;

PGPROC *MyProc;
PROC_HDR *ProcGlobal;
Latch *MyLatch;
int MaxBackends;
ProcessingMode Mode;
BackendType MyBackendType = B_BACKEND;
bool cluster_enabled;
int cluster_node_id;
volatile sig_atomic_t InterruptPending;
volatile sig_atomic_t QueryCancelPending;
sigjmp_buf *PG_exception_stack;
ErrorContextCallback *error_context_stack;

void
ExceptionalCondition(const char *conditionName pg_attribute_unused(),
					 const char *fileName pg_attribute_unused(), int lineNumber pg_attribute_unused())
{
	if (test_fail_stop_armed) {
		test_fail_stop_armed = false;
		siglongjmp(test_fail_stop_stack, 1);
	}
	abort();
}

bool
errstart(int elevel, const char *domain pg_attribute_unused())
{
	if (elevel >= ERROR && test_fail_stop_armed) {
		test_fail_stop_armed = false;
		siglongjmp(test_fail_stop_stack, 1);
	}
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
errmsg(const char *fmt pg_attribute_unused(), ...)
{
	return 0;
}

bool
LWLockAcquire(LWLock *lock pg_attribute_unused(), LWLockMode mode pg_attribute_unused())
{
	return true;
}

void
LWLockRelease(LWLock *lock pg_attribute_unused())
{}

void
LWLockInitialize(LWLock *lock pg_attribute_unused(), int tranche_id pg_attribute_unused())
{}

void *
ShmemInitStruct(const char *name pg_attribute_unused(), Size size pg_attribute_unused(), bool *found)
{
	*found = true;
	return test_txw_storage;
}

void
cluster_shmem_register_region(const ClusterShmemRegion *region pg_attribute_unused())
{}

TransactionId
GetTopTransactionIdIfAny(void)
{
	return test_local_xid;
}

uint64
cluster_epoch_get_current(void)
{
	int pos;

	UT_ASSERT(test_epoch_count > 0);
	pos = test_epoch_pos < test_epoch_count ? test_epoch_pos++ : test_epoch_count - 1;
	return test_epochs[pos];
}

PcmXQueueResult
cluster_pcm_x_nested_wait_guard_before_block(void)
{
	return test_guard_result;
}

NodeId
uba_origin_node_id(UBA uba pg_attribute_unused())
{
	return test_uba_origin;
}

ClusterTxOutcome
cluster_tx_resolve_exact(const ClusterTxLocator *locator pg_attribute_unused(),
						 ClusterTxResolveMode mode, ClusterTxResolution *out,
						 ClusterTxResolveReason *reason_out)
{
	int pos;

	UT_ASSERT_EQ((int)mode, (int)CLUSTER_TX_RESOLVE_ROW_WAIT);
	UT_ASSERT(test_resolve_count > 0);
	pos = test_resolve_pos < test_resolve_count ? test_resolve_pos++ : test_resolve_count - 1;
	if (out != NULL)
		memset(out, 0, sizeof(*out));
	if (reason_out != NULL)
		*reason_out = test_resolve_reasons[pos];
	return test_resolve_outcomes[pos];
}

bool
cluster_tt_status_lookup_exact(const ClusterTTStatusKey *key pg_attribute_unused(),
							   ClusterTTStatusResult *out pg_attribute_unused())
{
	return false;
}

uint64
cluster_lmd_wait_state_publish(ClusterLmdProcWaitState *ws pg_attribute_unused(), uint8 kind,
							   uint64 request_id, uint64 epoch, TransactionId xid)
{
	test_wait_publish_calls++;
	test_wait_kind = kind;
	test_wait_epoch = epoch;
	UT_ASSERT_EQ(request_id, 0);
	UT_ASSERT_EQ((int)xid, (int)test_local_xid);
	test_wait_read_result = CLUSTER_LMD_WAIT_STATE_READ_ACTIVE;
	test_wait_snapshot.active = true;
	test_wait_snapshot.kind = kind;
	test_wait_snapshot.request_id = request_id;
	test_wait_snapshot.cluster_epoch = epoch;
	test_wait_snapshot.xid = xid;
	test_wait_snapshot.wait_seq = UINT64CONST(1234);
	return UINT64CONST(1234);
}

void
cluster_lmd_wait_state_clear(ClusterLmdProcWaitState *ws pg_attribute_unused())
{
	test_wait_clear_calls++;
	test_wait_read_result = CLUSTER_LMD_WAIT_STATE_READ_INACTIVE;
	memset(&test_wait_snapshot, 0, sizeof(test_wait_snapshot));
}

ClusterLmdWaitStateReadResult
cluster_lmd_wait_state_read_exact(ClusterLmdProcWaitState *ws pg_attribute_unused(),
								  ClusterLmdWaitStateSnapshot *out)
{
	*out = test_wait_snapshot;
	return test_wait_read_result;
}

bool
cluster_lmd_submit_wait_edge_real(const ClusterLmdVertex *waiter,
								  const ClusterLmdVertex *blocker, uint64 request_id)
{
	test_wfg_submit_calls++;
	test_wfg_waiter = *waiter;
	test_wfg_blocker = *blocker;
	UT_ASSERT_EQ(request_id, 0);
	if (test_wfg_accept)
		test_wfg_live = true;
	return test_wfg_accept;
}

void
cluster_lmd_cancel_wait_edge_real(const ClusterLmdVertex *waiter)
{
	test_wfg_cancel_calls++;
	UT_ASSERT_EQ(memcmp(waiter, &test_wfg_waiter, sizeof(*waiter)), 0);
	test_wfg_live = false;
}

void
ResetLatch(Latch *latch pg_attribute_unused())
{}

void
SetLatch(Latch *latch)
{
	int i;

	for (i = 0; i < TEST_NSLOTS; i++)
		if (latch == &test_procs[i].procLatch)
			test_set_latch_calls[i]++;
}

void
pg_usleep(long microsec)
{
	struct timespec delay;

	delay.tv_sec = microsec / 1000000L;
	delay.tv_nsec = (microsec % 1000000L) * 1000L;
	(void)nanosleep(&delay, NULL);
}

int
WaitLatch(Latch *latch pg_attribute_unused(), int wakeEvents pg_attribute_unused(), long timeout,
		  uint32 wait_event_info pg_attribute_unused())
{
	test_wait_latch_calls++;
	if (test_wait_latch_throws)
		siglongjmp(*PG_exception_stack, 1);
	if (test_wait_latch_sleeps)
		pg_usleep((long)Max(timeout, 2) * 1000L);
	return WL_TIMEOUT;
}

TimestampTz
GetCurrentTimestamp(void)
{
	return 0;
}

void
ProcessInterrupts(void)
{
	InterruptPending = false;
}

void
pg_re_throw(void)
{
	if (PG_exception_stack != NULL)
		siglongjmp(*PG_exception_stack, 1);
	abort();
}

static ClusterTxLocator
test_locator(void)
{
	ClusterTxLocator locator;

	memset(&locator, 0, sizeof(locator));
	locator.uba = uba_encode(1, 9, 3, 4);
	locator.xid = TEST_XID;
	locator.tt_wrap = 5;
	locator.itl_kind = ITL_FLAG_ACTIVE;
	locator.itl_slot_index = 2;
	return locator;
}

static ClusterTTStatusKey
test_source_key(void)
{
	ClusterTTStatusKey key;

	memset(&key, 0, sizeof(key));
	key.origin_node_id = 3;
	key.undo_segment_id = 4;
	key.tt_slot_id = 5;
	key.cluster_epoch = 6;
	key.local_xid = TEST_XID;
	return key;
}

static void
script_resolve(int index, ClusterTxOutcome outcome, ClusterTxResolveReason reason)
{
	UT_ASSERT(index >= 0 && index < (int)lengthof(test_resolve_outcomes));
	test_resolve_outcomes[index] = outcome;
	test_resolve_reasons[index] = reason;
	if (test_resolve_count <= index)
		test_resolve_count = index + 1;
}

static void
reset_fixture(void)
{
	ClusterTxwShmem *state = (ClusterTxwShmem *)test_txw_storage;

	memset(test_txw_storage, 0, sizeof(test_txw_storage));
	state->nslots = TEST_NSLOTS;
	pg_atomic_init_u32(&state->active_waiters, 0);
	pg_atomic_init_u64(&state->wait_count, 0);
	pg_atomic_init_u64(&state->wakeup_count, 0);
	pg_atomic_init_u64(&state->timeout_count, 0);
	ClusterTxw = state;

	memset(test_procs, 0, sizeof(test_procs));
	test_procs[0].pgprocno = 0;
	test_procs[1].pgprocno = 1;
	memset(&test_proc_hdr, 0, sizeof(test_proc_hdr));
	test_proc_hdr.allProcs = test_procs;
	test_proc_hdr.allProcCount = TEST_NSLOTS;
	ProcGlobal = &test_proc_hdr;
	MyProc = &test_procs[0];
	MyLatch = &test_procs[0].procLatch;
	MaxBackends = TEST_NSLOTS;
	Mode = NormalProcessing;
	cluster_enabled = true;
	cluster_node_id = 1;

	memset(test_resolve_outcomes, 0, sizeof(test_resolve_outcomes));
	memset(test_resolve_reasons, 0, sizeof(test_resolve_reasons));
	test_resolve_count = 0;
	test_resolve_pos = 0;
	memset(test_epochs, 0, sizeof(test_epochs));
	test_epochs[0] = TEST_EPOCH;
	test_epoch_count = 1;
	test_epoch_pos = 0;
	test_guard_result = PCM_X_QUEUE_OK;
	test_uba_origin = 0;
	test_wfg_accept = true;
	test_wfg_live = false;
	test_wfg_submit_calls = 0;
	test_wfg_cancel_calls = 0;
	memset(&test_wfg_waiter, 0, sizeof(test_wfg_waiter));
	memset(&test_wfg_blocker, 0, sizeof(test_wfg_blocker));
	test_wait_publish_calls = 0;
	test_wait_clear_calls = 0;
	test_wait_kind = 0;
	test_wait_epoch = 0;
	test_wait_read_result = CLUSTER_LMD_WAIT_STATE_READ_INACTIVE;
	memset(&test_wait_snapshot, 0, sizeof(test_wait_snapshot));
	test_wait_latch_calls = 0;
	memset(test_set_latch_calls, 0, sizeof(test_set_latch_calls));
	test_wait_latch_throws = false;
	test_wait_latch_sleeps = false;
	test_local_xid = (TransactionId)700;
	test_fail_stop_armed = false;
	InterruptPending = false;
	QueryCancelPending = false;
}

static void
assert_slot_clean(void)
{
	UT_ASSERT_EQ(ClusterTxw->slots[0].waiting, 0);
	UT_ASSERT_EQ(pg_atomic_read_u32(&ClusterTxw->active_waiters), 0);
}

static bool
invoke_exit_callback_expect_fail_stop(void)
{
	if (sigsetjmp(test_fail_stop_stack, 1) == 0) {
		test_fail_stop_armed = true;
		cluster_tx_enqueue_cleanup_on_backend_exit_callback(0, (Datum)0);
		test_fail_stop_armed = false;
		return false;
	}
	test_fail_stop_armed = false;
	return true;
}

UT_TEST(test_exact_wait_abi_and_shmem_size_are_frozen)
{
	reset_fixture();
	UT_ASSERT_EQ(CLUSTER_TXW_RESOLVED, 0);
	UT_ASSERT_EQ(CLUSTER_TXW_TIMEOUT, 1);
	UT_ASSERT_EQ(CLUSTER_TXW_DEAD_HOLDER, 2);
	UT_ASSERT_EQ(CLUSTER_TXW_RETRY, 3);
	UT_ASSERT_EQ(CLUSTER_TXW_UNPROVABLE, 4);
	UT_ASSERT_EQ(sizeof(ClusterTTStatusKey), 24);
	UT_ASSERT_EQ(__alignof__(ClusterTTStatusKey), 4);
	UT_ASSERT_EQ(sizeof(ClusterTxLocator), 24);
	UT_ASSERT_EQ(__alignof__(ClusterTxLocator), 8);
	UT_ASSERT_EQ(sizeof(ClusterTxwIdentity), 24);
	UT_ASSERT_EQ(__alignof__(ClusterTxwIdentity), 4);
	UT_ASSERT_EQ(offsetof(ClusterTxwWaitSlot, waiting), 24);
	UT_ASSERT_EQ(sizeof(ClusterTxwWaitSlot), 28);
	UT_ASSERT_EQ(__alignof__(ClusterTxwWaitSlot), 4);
	UT_ASSERT_EQ(cluster_tx_enqueue_shmem_size(),
				 MAXALIGN(offsetof(ClusterTxwShmem, slots) + TEST_NSLOTS * 28));
}

UT_TEST(test_fixed_false_precedes_malformed_and_shared_state)
{
	ClusterTxResolveReason reason = CLUSTER_TX_RESOLVE_PROTOCOL;

	reset_fixture();
	ClusterTxw = NULL;
	script_resolve(0, CLUSTER_TX_UNKNOWN, CLUSTER_TX_RESOLVE_TARGET_DISABLED);
	UT_ASSERT_EQ(cluster_tx_enqueue_wait_exact(NULL, 1, &reason), CLUSTER_TXW_UNPROVABLE);
	UT_ASSERT_EQ(reason, CLUSTER_TX_RESOLVE_TARGET_DISABLED);
	UT_ASSERT_EQ(test_resolve_pos, 1);
}

UT_TEST(test_initial_terminal_never_registers)
{
	ClusterTxLocator locator = test_locator();
	ClusterTxResolveReason reason = CLUSTER_TX_RESOLVE_PROTOCOL;

	reset_fixture();
	script_resolve(0, CLUSTER_TX_COMMITTED, CLUSTER_TX_RESOLVE_NONE);
	UT_ASSERT_EQ(cluster_tx_enqueue_wait_exact(&locator, 1, &reason), CLUSTER_TXW_RESOLVED);
	UT_ASSERT_EQ(reason, CLUSTER_TX_RESOLVE_NONE);
	UT_ASSERT_EQ(pg_atomic_read_u64(&ClusterTxw->wait_count), 0);
	UT_ASSERT_EQ(test_wait_publish_calls, 0);
	assert_slot_clean();
}

UT_TEST(test_live_post_registration_terminal_race_resolves)
{
	ClusterTxLocator locator = test_locator();
	ClusterTxResolveReason reason = CLUSTER_TX_RESOLVE_PROTOCOL;

	reset_fixture();
	script_resolve(0, CLUSTER_TX_IN_PROGRESS, CLUSTER_TX_RESOLVE_NONE);
	script_resolve(1, CLUSTER_TX_ABORTED, CLUSTER_TX_RESOLVE_NONE);
	UT_ASSERT_EQ(cluster_tx_enqueue_wait_exact(&locator, 1000, &reason), CLUSTER_TXW_RESOLVED);
	UT_ASSERT_EQ(reason, CLUSTER_TX_RESOLVE_NONE);
	UT_ASSERT_EQ(test_resolve_pos, 2);
	UT_ASSERT_EQ(pg_atomic_read_u64(&ClusterTxw->wait_count), 1);
	UT_ASSERT_EQ(test_wait_publish_calls, 1);
	UT_ASSERT_EQ(test_wait_clear_calls, 1);
	UT_ASSERT_EQ(test_wfg_submit_calls, 1);
	UT_ASSERT_EQ(test_wfg_cancel_calls, 1);
	UT_ASSERT_EQ(test_wait_epoch, TEST_EPOCH);
	UT_ASSERT_EQ(test_wfg_waiter.node_id, cluster_node_id);
	UT_ASSERT_EQ(test_wfg_waiter.cluster_epoch, TEST_EPOCH);
	UT_ASSERT_EQ(test_wfg_blocker.node_id, test_uba_origin);
	UT_ASSERT_EQ(test_wfg_blocker.procno, CLUSTER_LMD_TX_HOLDER_PROCNO);
	UT_ASSERT_EQ(test_wfg_blocker.cluster_epoch, TEST_EPOCH);
	UT_ASSERT_EQ((int)test_wfg_blocker.xid, (int)locator.xid);
	UT_ASSERT_EQ(test_wait_latch_calls, 0);
	assert_slot_clean();
}

UT_TEST(test_prepared_waits_and_lost_wakes_only_repoll)
{
	ClusterTxLocator locator = test_locator();
	ClusterTxResolveReason reason = CLUSTER_TX_RESOLVE_PROTOCOL;

	reset_fixture();
	script_resolve(0, CLUSTER_TX_PREPARED, CLUSTER_TX_RESOLVE_NONE);
	script_resolve(1, CLUSTER_TX_PREPARED, CLUSTER_TX_RESOLVE_NONE);
	script_resolve(2, CLUSTER_TX_IN_PROGRESS, CLUSTER_TX_RESOLVE_NONE);
	script_resolve(3, CLUSTER_TX_COMMITTED, CLUSTER_TX_RESOLVE_NONE);
	UT_ASSERT_EQ(cluster_tx_enqueue_wait_exact(&locator, 1000, &reason), CLUSTER_TXW_RESOLVED);
	UT_ASSERT_EQ(reason, CLUSTER_TX_RESOLVE_NONE);
	UT_ASSERT_EQ(test_wait_latch_calls, 2);
	assert_slot_clean();
}

UT_TEST(test_unknown_fails_before_registration_with_exact_reason)
{
	ClusterTxLocator locator = test_locator();
	ClusterTxResolveReason reason = CLUSTER_TX_RESOLVE_PROTOCOL;

	reset_fixture();
	script_resolve(0, CLUSTER_TX_UNKNOWN, CLUSTER_TX_RESOLVE_COVERAGE_GAP);
	UT_ASSERT_EQ(cluster_tx_enqueue_wait_exact(&locator, 1, &reason), CLUSTER_TXW_UNPROVABLE);
	UT_ASSERT_EQ(reason, CLUSTER_TX_RESOLVE_COVERAGE_GAP);
	UT_ASSERT_EQ(pg_atomic_read_u64(&ClusterTxw->wait_count), 0);
	assert_slot_clean();
}

UT_TEST(test_formation_drift_cleans_registered_state)
{
	ClusterTxLocator locator = test_locator();
	ClusterTxResolveReason reason = CLUSTER_TX_RESOLVE_PROTOCOL;

	reset_fixture();
	script_resolve(0, CLUSTER_TX_IN_PROGRESS, CLUSTER_TX_RESOLVE_NONE);
	test_epochs[0] = TEST_EPOCH;
	test_epochs[1] = TEST_EPOCH;
	test_epochs[2] = TEST_EPOCH;
	test_epochs[3] = TEST_EPOCH;
	test_epochs[4] = TEST_EPOCH + 1;
	test_epoch_count = 5;
	UT_ASSERT_EQ(cluster_tx_enqueue_wait_exact(&locator, 1000, &reason), CLUSTER_TXW_UNPROVABLE);
	UT_ASSERT_EQ(reason, CLUSTER_TX_RESOLVE_RF_DEFERRED);
	UT_ASSERT_EQ(test_wait_clear_calls, 1);
	UT_ASSERT_EQ(test_wfg_cancel_calls, 1);
	assert_slot_clean();
}

UT_TEST(test_zero_epoch_is_a_valid_stable_formation)
{
	ClusterTxLocator locator = test_locator();
	ClusterTxResolveReason reason = CLUSTER_TX_RESOLVE_PROTOCOL;

	reset_fixture();
	script_resolve(0, CLUSTER_TX_IN_PROGRESS, CLUSTER_TX_RESOLVE_NONE);
	script_resolve(1, CLUSTER_TX_ABORTED, CLUSTER_TX_RESOLVE_NONE);
	test_epochs[0] = 0;
	UT_ASSERT_EQ(cluster_tx_enqueue_wait_exact(&locator, 1000, &reason), CLUSTER_TXW_RESOLVED);
	UT_ASSERT_EQ(reason, CLUSTER_TX_RESOLVE_NONE);
	UT_ASSERT_EQ(test_wait_publish_calls, 1);
	UT_ASSERT_EQ(test_wfg_submit_calls, 1);
	assert_slot_clean();
}

UT_TEST(test_zero_to_nonzero_epoch_drift_fails_closed)
{
	ClusterTxLocator locator = test_locator();
	ClusterTxResolveReason reason = CLUSTER_TX_RESOLVE_PROTOCOL;

	reset_fixture();
	script_resolve(0, CLUSTER_TX_IN_PROGRESS, CLUSTER_TX_RESOLVE_NONE);
	test_epochs[0] = 0;
	test_epochs[1] = 0;
	test_epochs[2] = 0;
	test_epochs[3] = 0;
	test_epochs[4] = 1;
	test_epoch_count = 5;
	UT_ASSERT_EQ(cluster_tx_enqueue_wait_exact(&locator, 1000, &reason), CLUSTER_TXW_UNPROVABLE);
	UT_ASSERT_EQ(reason, CLUSTER_TX_RESOLVE_RF_DEFERRED);
	UT_ASSERT_EQ(test_wait_clear_calls, 1);
	UT_ASSERT_EQ(test_wfg_cancel_calls, 1);
	assert_slot_clean();
}

UT_TEST(test_monotonic_timeout_uses_only_timeout_counter)
{
	ClusterTxLocator locator = test_locator();
	ClusterTxResolveReason reason = CLUSTER_TX_RESOLVE_PROTOCOL;

	reset_fixture();
	script_resolve(0, CLUSTER_TX_IN_PROGRESS, CLUSTER_TX_RESOLVE_NONE);
	test_wait_latch_sleeps = true;
	UT_ASSERT_EQ(cluster_tx_enqueue_wait_exact(&locator, 1, &reason), CLUSTER_TXW_TIMEOUT);
	UT_ASSERT_EQ(reason, CLUSTER_TX_RESOLVE_TIMEOUT);
	UT_ASSERT_EQ(pg_atomic_read_u64(&ClusterTxw->timeout_count), 1);
	UT_ASSERT_EQ(pg_atomic_read_u64(&ClusterTxw->wait_count), 1);
	assert_slot_clean();
}

UT_TEST(test_nested_guard_retries_without_slot_mutation)
{
	ClusterTxLocator locator = test_locator();
	ClusterTxResolveReason reason = CLUSTER_TX_RESOLVE_PROTOCOL;

	reset_fixture();
	script_resolve(0, CLUSTER_TX_IN_PROGRESS, CLUSTER_TX_RESOLVE_NONE);
	test_guard_result = PCM_X_QUEUE_GATE_RETRY;
	UT_ASSERT_EQ(cluster_tx_enqueue_wait_exact(&locator, 1, &reason), CLUSTER_TXW_RETRY);
	UT_ASSERT_EQ(reason, CLUSTER_TX_RESOLVE_NONE);
	UT_ASSERT_EQ(pg_atomic_read_u64(&ClusterTxw->wait_count), 0);
	UT_ASSERT_EQ(test_wait_publish_calls, 0);
	assert_slot_clean();
}

UT_TEST(test_reentrant_source_and_target_slots_are_not_overwritten)
{
	ClusterTxLocator locator = test_locator();
	ClusterTTStatusKey source = test_source_key();
	ClusterTxResolveReason reason = CLUSTER_TX_RESOLVE_PROTOCOL;
	unsigned char before[sizeof(ClusterTxwWaitSlot)];

	reset_fixture();
	txw_slot_set(0, &source);
	memcpy(before, &ClusterTxw->slots[0], sizeof(before));
	script_resolve(0, CLUSTER_TX_IN_PROGRESS, CLUSTER_TX_RESOLVE_NONE);
	UT_ASSERT_EQ(cluster_tx_enqueue_wait_exact(&locator, 1, &reason), CLUSTER_TXW_UNPROVABLE);
	UT_ASSERT_EQ(reason, CLUSTER_TX_RESOLVE_REENTRANT);
	UT_ASSERT_EQ(memcmp(before, &ClusterTxw->slots[0], sizeof(before)), 0);

	reset_fixture();
	memcpy(&ClusterTxw->slots[0], &locator, sizeof(locator));
	ClusterTxw->slots[0].waiting = 2;
	pg_atomic_write_u32(&ClusterTxw->active_waiters, 1);
	memcpy(before, &ClusterTxw->slots[0], sizeof(before));
	script_resolve(0, CLUSTER_TX_PREPARED, CLUSTER_TX_RESOLVE_NONE);
	UT_ASSERT_EQ(cluster_tx_enqueue_wait_exact(&locator, 1, &reason), CLUSTER_TXW_UNPROVABLE);
	UT_ASSERT_EQ(reason, CLUSTER_TX_RESOLVE_REENTRANT);
	UT_ASSERT_EQ(memcmp(before, &ClusterTxw->slots[0], sizeof(before)), 0);
}

UT_TEST(test_wfg_capacity_refusal_runs_full_cleanup)
{
	ClusterTxLocator locator = test_locator();
	ClusterTxResolveReason reason = CLUSTER_TX_RESOLVE_PROTOCOL;

	reset_fixture();
	script_resolve(0, CLUSTER_TX_IN_PROGRESS, CLUSTER_TX_RESOLVE_NONE);
	test_wfg_accept = false;
	UT_ASSERT_EQ(cluster_tx_enqueue_wait_exact(&locator, 1000, &reason), CLUSTER_TXW_UNPROVABLE);
	UT_ASSERT_EQ(reason, CLUSTER_TX_RESOLVE_CAPACITY);
	UT_ASSERT_EQ(test_wait_publish_calls, 1);
	UT_ASSERT_EQ(test_wait_clear_calls, 1);
	UT_ASSERT_EQ(test_wfg_submit_calls, 1);
	UT_ASSERT_EQ(test_wfg_cancel_calls, 0);
	assert_slot_clean();
}

UT_TEST(test_error_longjmp_runs_same_cleanup_funnel)
{
	ClusterTxLocator locator = test_locator();
	ClusterTxResolveReason reason = CLUSTER_TX_RESOLVE_PROTOCOL;
	bool caught = false;

	reset_fixture();
	script_resolve(0, CLUSTER_TX_IN_PROGRESS, CLUSTER_TX_RESOLVE_NONE);
	test_wait_latch_throws = true;
	PG_TRY();
	{
		(void)cluster_tx_enqueue_wait_exact(&locator, 1000, &reason);
	}
	PG_CATCH();
	{
		caught = true;
	}
	PG_END_TRY();
	UT_ASSERT(caught);
	UT_ASSERT_EQ(test_wait_clear_calls, 1);
	UT_ASSERT_EQ(test_wfg_cancel_calls, 1);
	assert_slot_clean();
}

UT_TEST(test_hint_waker_matches_source_discriminant_only)
{
	ClusterTTStatusKey source = test_source_key();

	reset_fixture();
	memcpy(&ClusterTxw->slots[0], &source, sizeof(source));
	ClusterTxw->slots[0].waiting = 1;
	memcpy(&ClusterTxw->slots[1], &source, sizeof(source));
	ClusterTxw->slots[1].waiting = 2;
	pg_atomic_write_u32(&ClusterTxw->active_waiters, 2);
	cluster_txw_wake_waiters(&source);
	UT_ASSERT_EQ(test_set_latch_calls[0], 1);
	UT_ASSERT_EQ(test_set_latch_calls[1], 0);
	UT_ASSERT_EQ(pg_atomic_read_u64(&ClusterTxw->wakeup_count), 1);
}

/*
 * A backend can leave the exact wait through proc_exit/FATAL rather than the
 * stack PG_FINALLY.  The before_shmem_exit hook must remove only this
 * backend's exact TARGET ownership, and it must be idempotent so pgprocno
 * reuse can immediately register a fresh wait.
 */
extern void cluster_tx_enqueue_cleanup_on_backend_exit_callback(int code, Datum arg);

UT_TEST(test_backend_exit_cleans_exact_target_and_allows_procno_reuse)
{
	ClusterTxLocator locator = test_locator();
	ClusterLmdVertex waiter;
	ClusterLmdVertex blocker;
	uint64 wait_seq;

	reset_fixture();
	UT_ASSERT(txw_target_slot_set_if_free(0, &locator));
	wait_seq = cluster_lmd_wait_state_publish(&MyProc->cluster_lmd_wait,
										  CLUSTER_LMD_WAIT_TX, 0, TEST_EPOCH,
										  test_local_xid);
	txw_exact_waiter_vertex(&waiter, 0, TEST_EPOCH, test_local_xid, wait_seq);
	memset(&blocker, 0, sizeof(blocker));
	UT_ASSERT(cluster_lmd_submit_wait_edge_real(&waiter, &blocker, 0));

	cluster_tx_enqueue_cleanup_on_backend_exit_callback(0, (Datum)0);
	UT_ASSERT_EQ(test_wfg_cancel_calls, 1);
	UT_ASSERT(!test_wfg_live);
	UT_ASSERT_EQ(test_wait_clear_calls, 1);
	UT_ASSERT_EQ(test_wait_read_result, CLUSTER_LMD_WAIT_STATE_READ_INACTIVE);
	assert_slot_clean();

	/* Re-entry after cleanup is a no-op, and the reused procno is writable. */
	cluster_tx_enqueue_cleanup_on_backend_exit_callback(0, (Datum)0);
	UT_ASSERT_EQ(test_wfg_cancel_calls, 1);
	UT_ASSERT_EQ(test_wait_clear_calls, 1);
	UT_ASSERT(txw_target_slot_set_if_free(0, &locator));
	UT_ASSERT_EQ(pg_atomic_read_u32(&ClusterTxw->active_waiters), 1);
	txw_slot_clear(0);
}

UT_TEST(test_backend_exit_cleans_exact_source_and_allows_procno_reuse)
{
	ClusterTxLocator locator = test_locator();
	ClusterTTStatusKey source = test_source_key();
	ClusterLmdVertex waiter;
	ClusterLmdVertex blocker;
	uint64 wait_seq;

	reset_fixture();
	txw_slot_set(0, &source);
	wait_seq = cluster_lmd_wait_state_publish(&MyProc->cluster_lmd_wait,
										  CLUSTER_LMD_WAIT_TX, 0, TEST_EPOCH,
										  test_local_xid);
	txw_exact_waiter_vertex(&waiter, 0, TEST_EPOCH, test_local_xid, wait_seq);
	memset(&blocker, 0, sizeof(blocker));
	UT_ASSERT(cluster_lmd_submit_wait_edge_real(&waiter, &blocker, 0));

	cluster_tx_enqueue_cleanup_on_backend_exit_callback(0, (Datum)0);
	UT_ASSERT_EQ(test_wfg_cancel_calls, 1);
	UT_ASSERT(!test_wfg_live);
	UT_ASSERT_EQ(test_wait_clear_calls, 1);
	UT_ASSERT_EQ(test_wait_read_result, CLUSTER_LMD_WAIT_STATE_READ_INACTIVE);
	assert_slot_clean();

	/* A duplicate callback is harmless and the procno can be reused now. */
	cluster_tx_enqueue_cleanup_on_backend_exit_callback(0, (Datum)0);
	UT_ASSERT_EQ(test_wfg_cancel_calls, 1);
	UT_ASSERT_EQ(test_wait_clear_calls, 1);
	UT_ASSERT(txw_target_slot_set_if_free(0, &locator));
	UT_ASSERT_EQ(pg_atomic_read_u32(&ClusterTxw->active_waiters), 1);
	txw_slot_clear(0);
}

UT_TEST(test_backend_exit_inactive_state_cleans_owned_slot_without_wfg_cancel)
{
	ClusterTxLocator locator = test_locator();

	reset_fixture();
	UT_ASSERT(txw_target_slot_set_if_free(0, &locator));
	cluster_tx_enqueue_cleanup_on_backend_exit_callback(0, (Datum)0);
	UT_ASSERT_EQ(test_wfg_cancel_calls, 0);
	UT_ASSERT_EQ(test_wait_clear_calls, 1);
	assert_slot_clean();
}

UT_TEST(test_backend_exit_busy_state_fails_stop_without_releasing_owner)
{
	ClusterTxLocator locator = test_locator();

	reset_fixture();
	UT_ASSERT(txw_target_slot_set_if_free(0, &locator));
	test_wait_read_result = CLUSTER_LMD_WAIT_STATE_READ_BUSY;
	UT_ASSERT(invoke_exit_callback_expect_fail_stop());
	UT_ASSERT_EQ(ClusterTxw->slots[0].waiting, CLUSTER_TXW_SLOT_TARGET);
	UT_ASSERT_EQ(pg_atomic_read_u32(&ClusterTxw->active_waiters), 1);
	UT_ASSERT_EQ(test_wait_clear_calls, 0);
	txw_slot_clear(0);
}

UT_TEST(test_backend_exit_malformed_active_state_fails_stop_without_releasing_owner)
{
	ClusterTxLocator locator = test_locator();

	reset_fixture();
	UT_ASSERT(txw_target_slot_set_if_free(0, &locator));
	test_wait_read_result = CLUSTER_LMD_WAIT_STATE_READ_ACTIVE;
	test_wait_snapshot.active = true;
	test_wait_snapshot.kind = CLUSTER_LMD_WAIT_GES;
	test_wait_snapshot.wait_seq = 1;
	UT_ASSERT(invoke_exit_callback_expect_fail_stop());
	UT_ASSERT_EQ(ClusterTxw->slots[0].waiting, CLUSTER_TXW_SLOT_TARGET);
	UT_ASSERT_EQ(pg_atomic_read_u32(&ClusterTxw->active_waiters), 1);
	UT_ASSERT_EQ(test_wait_clear_calls, 0);
	txw_slot_clear(0);
}

UT_TEST(test_backend_exit_counter_underflow_fails_stop_without_freeing_slot)
{
	ClusterTxLocator locator = test_locator();

	reset_fixture();
	memcpy(ClusterTxw->slots[0].identity.target_locator_words, &locator, sizeof(locator));
	ClusterTxw->slots[0].waiting = CLUSTER_TXW_SLOT_TARGET;
	UT_ASSERT(invoke_exit_callback_expect_fail_stop());
	UT_ASSERT_EQ(ClusterTxw->slots[0].waiting, CLUSTER_TXW_SLOT_TARGET);
	UT_ASSERT_EQ(pg_atomic_read_u32(&ClusterTxw->active_waiters), 0);
	ClusterTxw->slots[0].waiting = CLUSTER_TXW_SLOT_FREE;
}

int
main(void)
{
	UT_PLAN(21);
	UT_RUN(test_exact_wait_abi_and_shmem_size_are_frozen);
	UT_RUN(test_fixed_false_precedes_malformed_and_shared_state);
	UT_RUN(test_initial_terminal_never_registers);
	UT_RUN(test_live_post_registration_terminal_race_resolves);
	UT_RUN(test_prepared_waits_and_lost_wakes_only_repoll);
	UT_RUN(test_unknown_fails_before_registration_with_exact_reason);
	UT_RUN(test_formation_drift_cleans_registered_state);
	UT_RUN(test_zero_epoch_is_a_valid_stable_formation);
	UT_RUN(test_zero_to_nonzero_epoch_drift_fails_closed);
	UT_RUN(test_monotonic_timeout_uses_only_timeout_counter);
	UT_RUN(test_nested_guard_retries_without_slot_mutation);
	UT_RUN(test_reentrant_source_and_target_slots_are_not_overwritten);
	UT_RUN(test_wfg_capacity_refusal_runs_full_cleanup);
	UT_RUN(test_error_longjmp_runs_same_cleanup_funnel);
	UT_RUN(test_hint_waker_matches_source_discriminant_only);
	UT_RUN(test_backend_exit_cleans_exact_target_and_allows_procno_reuse);
	UT_RUN(test_backend_exit_cleans_exact_source_and_allows_procno_reuse);
	UT_RUN(test_backend_exit_inactive_state_cleans_owned_slot_without_wfg_cancel);
	UT_RUN(test_backend_exit_busy_state_fails_stop_without_releasing_owner);
	UT_RUN(test_backend_exit_malformed_active_state_fails_stop_without_releasing_owner);
	UT_RUN(test_backend_exit_counter_underflow_fails_stop_without_freeing_slot);
	UT_DONE();
	return ut_failed_count == 0 ? 0 : 1;
}
