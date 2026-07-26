/*-------------------------------------------------------------------------
 *
 * test_cluster_pcm_lock.c
 *	  Compile-time + link-time + behavioral invariants for the PCM lock
 *	  9-state machine activated in spec-2.30.
 *
 *	  spec-2.30 replaces spec-1.7 4-stub bodies with the real 9-transition
 *	  state machine + GrdEntry HTAB + per-entry LWLockPadded.  This test
 *	  binary links cluster_pcm_lock.o + provides minimal stubs for all PG
 *	  runtime deps (ShmemInit*, LWLock*, hash_*, ereport, etc) so that
 *	  both pure-function paths and the real acquire/release/upgrade/
 *	  downgrade/query state machine can be exercised standalone.
 *
 *	  Test plan (26 tests; spec-2.30 §4.1 + codereview hardening):
 *	    T-pcm-1..8   :  transition validator returns true for legal (from, to, trans)
 *	    T-pcm-9      :  Trans-9 reserved as legal entry (validator accepts)
 *	    T-pcm-10     :  transition validator rejects illegal combinations
 *	    T-pcm-11     :  disable path (ClusterPcm == NULL):  counter accessors return 0
 *	    T-pcm-12     :  HTAB-FULL surface (link-only;  cap enforcement)
 *	    T-pcm-13     :  per-entry LWLock granularity invariant (symbol existence)
 *	    T-pcm-14     :  PI bitmap atomic primitive present (link-only)
 *	    T-pcm-15     :  9 counter accessor surface returns 0 under disabled path
 *	    T-pcm-16..20 :  real acquire/release/upgrade/downgrade/query paths,
 *	                   live summary rows, and wait-event callsites
 *
 *	  The fake shared HTAB below is intentionally tiny, but it models the
 *	  behaviours that matter for PCM correctness: key lookup, cap-full,
 *	  shared holder bitmap updates, per-entry LWLock ownership assertions,
 *	  and SQL-visible summary counters.
 *
 *	  Spec: spec-2.30-pcm-9-state-machine-activation.md (FROZEN v0.3)
 *
 *
 * Portions Copyright (c) 1996-2024, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2026, pgrac contributors
 *
 * Author: SqlRush <sqlrush@gmail.com>
 *
 * IDENTIFICATION
 *	  src/test/cluster_unit/test_cluster_pcm_lock.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "cluster/cluster_buffer_desc.h" /* PcmState (1.6) */
#include "cluster/cluster_cssd.h"		 /* spec-4.7a D4 — ClusterCssdPeerState for stub */
#include "cluster/cluster_epoch.h"
#include "cluster/cluster_gcs.h"
#include "cluster/cluster_inject.h"
#include "cluster/cluster_gcs_block.h" /* spec-4.7 D1 — ClusterGcsBlockPhase + phase_for_tag proto */
#include "cluster/cluster_pcm_lock.h"
#include "cluster/cluster_shmem.h"
#include "storage/backendid.h"	   /* spec-6.14 D9 amend — MyBackendId stub */
#include "storage/buf_internals.h" /* BufferTag */
#include "storage/lwlock.h"
#include "utils/hsearch.h"
#include "utils/timestamp.h"
#include "utils/wait_event.h"

#include <setjmp.h>

/* Drop PG's port.h printf -> pg_printf override; unit_test.h uses
 * stdlib printf and we don't link libpgport in this test binary. */
#undef printf

#include "unit_test.h"


UT_DEFINE_GLOBALS();


/* ============================================================
 * PG-runtime stubs + fake shared HTAB.
 * ============================================================ */

int cluster_node_id = 0;
int NBuffers = 0;
int cluster_injection_armed_count = 0;
static uint64 fake_pcm_current_epoch = 0;
static int fake_pcm_epoch_read_count = 0;
static int fake_pcm_epoch_flip_on_read = 0;
static uint32 ut_wait_event_info_storage = 0;
uint32 *my_wait_event_info = &ut_wait_event_info_storage;

extern PcmGcsTransitionApplyResult
cluster_pcm_lock_apply_gcs_transition_exact_result(
	BufferTag tag, PcmLockTransition trans, int holder_node_id,
	uint64 expected_epoch, const PcmAuthoritySnapshot *expected_authority)
	;

uint64
cluster_epoch_get_current(void)
{
	fake_pcm_epoch_read_count++;
	if (fake_pcm_epoch_flip_on_read > 0
		&& fake_pcm_epoch_read_count >= fake_pcm_epoch_flip_on_read)
		return fake_pcm_current_epoch + 1;
	return fake_pcm_current_epoch;
}

__attribute__((weak)) PcmGcsTransitionApplyResult
cluster_pcm_lock_apply_gcs_transition_exact_result(
	BufferTag tag pg_attribute_unused(),
	PcmLockTransition trans pg_attribute_unused(),
	int holder_node_id pg_attribute_unused(),
	uint64 expected_epoch pg_attribute_unused(),
	const PcmAuthoritySnapshot *expected_authority pg_attribute_unused())
{
	return PCM_GCS_TRANSITION_INCOMPATIBLE;
}

#define FAKE_PCM_MAX_ENTRIES 8
#define FAKE_PCM_ENTRY_BYTES 1024

static union {
	uint64 force_align;
	/* Sized above sizeof(ClusterPcmShared): the S3-forensics step-1b
	 * per-tag watermark provenance table (8192 x 64B slots + lock) grew
	 * the header past 512KB;  the fake ShmemInitStruct asserts fit. */
	char data[1048576];
} fake_pcm_header;

static union {
	uint64 force_align;
	char data[FAKE_PCM_MAX_ENTRIES][FAKE_PCM_ENTRY_BYTES];
} fake_pcm_entries;

static char fake_pcm_htab_token;
static bool fake_pcm_header_found = false;
static long fake_pcm_entry_count = 0;
static long fake_pcm_entry_max = 0;
static Size fake_pcm_keysize = 0;
static Size fake_pcm_entrysize = 0;
static LWLock *fake_lwlock_held = NULL;
static LWLockMode fake_lwlock_mode = LW_EXCLUSIVE;
static LWLock *fake_lwlock_stack[16];
static LWLockMode fake_lwlock_mode_stack[16];
static int fake_lwlock_depth = 0;
static uint32 fake_init_wait_event_seen = 0;
static uint32 fake_lwlock_wait_event_seen = 0;

/* PGRAC: spec-2.31 D1 v0.4 — ConditionVariable stub counters (declared
 * here so reset_fake_pcm_runtime() can clear them;  definitions of the
 * stub functions themselves live below LWLockHeldByMeInMode). */
static int fake_cv_init_count = 0;
static int fake_cv_prepare_count = 0;
static int fake_cv_sleep_count = 0;
static int fake_cv_cancel_count = 0;
static int fake_cv_broadcast_count = 0;
static uint32 fake_cv_sleep_wait_event = 0;
static struct {
	BufferTag tag;
	int holder_node;
	bool armed;
} fake_cv_wake_release = { { 0 }, 0, false };
static struct {
	BufferTag tag;
	int requester_node;
	uint64 ticket_id;
	bool armed;
} fake_cv_wake_pending_x_clear = { { 0 }, 0, 0, false };

static sigjmp_buf ut_ereport_jump;
static bool ut_ereport_jump_armed = false;
static int ut_ereport_fired_count = 0;
static bool fake_local_x_upgrade_result = false;
static bool fake_acquire_entry_handoff_armed = false;
static BufferTag fake_acquire_entry_handoff_tag;
static int fake_acquire_entry_handoff_source = -1;
static int fake_acquire_entry_handoff_target = -1;
static PcmLockTransition fake_acquire_entry_handoff_release = PCM_TRANS_X_TO_N_RELEASE;
static int fake_local_read_image_count = 0;
static int fake_local_read_image_holder = -1;
static PcmAuthoritySnapshot fake_local_read_image_expected;
static int fake_local_x_transfer_count = 0;
static int fake_local_x_transfer_holder = -1;
static PcmAuthoritySnapshot fake_local_x_transfer_expected;

void
ExceptionalCondition(const char *conditionName pg_attribute_unused(),
					 const char *fileName pg_attribute_unused(),
					 int lineNumber pg_attribute_unused())
{
	abort();
}

/* spec-4.6a Amendment v1.2 (R5): the S->X upgrade is now wrapped in
 * PG_TRY/PG_CATCH, which references the exception stack + re-throw.  The
 * unit's errfinish mock longjmps to its own buffer (never through
 * PG_exception_stack), so these exist for the linker only. */
sigjmp_buf *PG_exception_stack = NULL;
ErrorContextCallback *error_context_stack = NULL;

void
pg_re_throw(void)
{
	abort();
}

static BufferTag
make_tag(uint32 blockno)
{
	BufferTag tag;

	memset(&tag, 0, sizeof(tag));
	tag.spcOid = 1663;
	tag.dbOid = 1;
	tag.relNumber = 100;
	tag.forkNum = MAIN_FORKNUM;
	tag.blockNum = blockno;
	return tag;
}

static void
reset_fake_pcm_runtime(int max_entries)
{
	memset(&fake_pcm_header, 0, sizeof(fake_pcm_header));
	memset(&fake_pcm_entries, 0, sizeof(fake_pcm_entries));
	fake_pcm_header_found = false;
	fake_pcm_entry_count = 0;
	fake_pcm_entry_max = max_entries;
	fake_pcm_keysize = 0;
	fake_pcm_entrysize = 0;
	fake_lwlock_held = NULL;
	fake_lwlock_mode = LW_EXCLUSIVE;
	fake_lwlock_depth = 0;
	memset(fake_lwlock_stack, 0, sizeof(fake_lwlock_stack));
	memset(fake_lwlock_mode_stack, 0, sizeof(fake_lwlock_mode_stack));
	fake_init_wait_event_seen = 0;
	fake_lwlock_wait_event_seen = 0;
	ut_wait_event_info_storage = 0;
	ut_ereport_fired_count = 0;
	ut_ereport_jump_armed = false;
	fake_cv_init_count = 0;
	fake_cv_prepare_count = 0;
	fake_cv_sleep_count = 0;
	fake_cv_cancel_count = 0;
	fake_cv_broadcast_count = 0;
	fake_cv_sleep_wait_event = 0;
	fake_cv_wake_release.armed = false;
	fake_cv_wake_pending_x_clear.armed = false;
	fake_acquire_entry_handoff_armed = false;
	fake_acquire_entry_handoff_release = PCM_TRANS_X_TO_N_RELEASE;
	fake_local_read_image_count = 0;
	fake_local_read_image_holder = -1;
	memset(&fake_local_read_image_expected, 0, sizeof(fake_local_read_image_expected));
	fake_local_x_upgrade_result = false;
	cluster_node_id = 0;
	NBuffers = max_entries;
	cluster_pcm_grd_max_entries = max_entries;
	cluster_pcm_grd_init();
}

/*
 * spec-4.7a B — cluster_pcm_lock.o's local acquire wait-path reads this GUC to
 * decide the bounded-fail-closed for a cross-node write transfer.  Stubbed OFF
 * here: the acquire state-machine tests below exercise transition logic, which
 * is GUC-independent.  The GUC-on bounded-fail-closed (B) needs a REAL remote
 * live holder (cssd liveness of a peer); the single-node unit harness stubs
 * cssd always-alive and fakes the GrdEntry, so it cannot model that path
 * honestly — it is e2e-tested by t/252 L3b (2-node, real cssd liveness).
 */
bool cluster_gcs_block_local_cache = false;

/* spec-6.12a stubs — the local-master S->X upgrade path is only reached with
 * the wave GUC on (default off here, so the branch is inert); provide inert
 * link-surface satisfaction.  Real coverage is t/352 (2-node). */
bool cluster_read_scache = false;

bool cluster_gcs_block_local_x_upgrade(BufferTag tag);
bool
cluster_gcs_block_local_x_upgrade(BufferTag tag)
{
	uint32 holders_bm;
	int n;

	if (!fake_local_x_upgrade_result)
		return false;
	holders_bm = cluster_pcm_lock_query_s_holders_bitmap(tag);
	if (cluster_node_id >= 0 && cluster_node_id < 32)
		holders_bm &= ~((uint32)1u << (uint32)cluster_node_id);
	for (n = 0; n < 32; n++)
		if ((holders_bm & ((uint32)1u << n)) != 0)
			(void)cluster_pcm_lock_apply_gcs_transition(tag, PCM_TRANS_S_TO_N_INVALIDATE, n);
	return cluster_pcm_lock_apply_gcs_transition(tag, PCM_TRANS_S_TO_X_UPGRADE, cluster_node_id);
}

/* spec-4.7a D4 — stub CSSD peer liveness for the other-live-holder gate.
 * Default: every peer ALIVE.  A test sets fake_cssd_dead_node to mark one
 * peer DEAD (to verify a dead holder is NOT counted by the D4 gate). */
static int32 fake_cssd_dead_node = -1;

ClusterCssdPeerState
cluster_cssd_get_peer_state(int32 peer_id)
{
	return (peer_id == fake_cssd_dead_node) ? CLUSTER_CSSD_PEER_DEAD : CLUSTER_CSSD_PEER_ALIVE;
}

/*
 * spec-4.7 D1 (L238) — cluster_pcm_lock.o's acquire_buffer now opens with a
 * RECOVERING gate that references cluster_gcs_block_phase_for_tag,
 * cluster_gcs_block_recovery_wait_ms and CHECK_FOR_INTERRUPTS.  This test
 * links cluster_pcm_lock.o but not cluster_gcs_block.o / cluster_guc.o /
 * postmaster core, so provide link-time stubs.  phase_for_tag → NORMAL keeps
 * the gate a no-op so these tests exercise the local acquire state machine,
 * not the recovery path (covered e2e by t/251).
 */
volatile sig_atomic_t InterruptPending = false;
void ProcessInterrupts(void);
void
ProcessInterrupts(void)
{}
int cluster_gcs_block_recovery_wait_ms = 200;

/* Controllable phase: default NORMAL (gate no-op so the state-machine tests
 * pass straight through);  a test sets it RECOVERING to drive the D1 gate. */
static ClusterGcsBlockPhase fake_block_phase = GCS_BLOCK_NORMAL;
ClusterGcsBlockPhase
cluster_gcs_block_phase_for_tag(BufferTag tag pg_attribute_unused())
{
	return fake_block_phase;
}

/* spec-4.7 D3 (L238) — the rebuild fn's not-double-X branch bumps this 4.6
 * counter;  stub it no-op for the unit harness. */
void cluster_grd_inc_block_path_failclosed(void);
void
cluster_grd_inc_block_path_failclosed(void)
{}

void *
ShmemInitStruct(const char *name pg_attribute_unused(), Size size pg_attribute_unused(),
				bool *foundPtr)
{
	Assert(size <= sizeof(fake_pcm_header.data));
	fake_init_wait_event_seen = ut_wait_event_info_storage;
	*foundPtr = fake_pcm_header_found;
	fake_pcm_header_found = true;
	return fake_pcm_header.data;
}

HTAB *
ShmemInitHash(const char *name pg_attribute_unused(), long init_size pg_attribute_unused(),
			  long max_size pg_attribute_unused(), HASHCTL *infoP pg_attribute_unused(),
			  int hash_flags pg_attribute_unused())
{
	Assert((hash_flags & HASH_ELEM) != 0);
	Assert(infoP->entrysize <= FAKE_PCM_ENTRY_BYTES);
	Assert(max_size <= FAKE_PCM_MAX_ENTRIES);
	fake_pcm_keysize = infoP->keysize;
	fake_pcm_entrysize = infoP->entrysize;
	fake_pcm_entry_max = max_size;
	fake_pcm_entry_count = 0;
	memset(&fake_pcm_entries, 0, sizeof(fake_pcm_entries));
	return (HTAB *)&fake_pcm_htab_token;
}

void *
hash_search(HTAB *hashp pg_attribute_unused(), const void *keyPtr pg_attribute_unused(),
			HASHACTION action pg_attribute_unused(), bool *foundPtr pg_attribute_unused())
{
	long i;

	Assert(hashp == (HTAB *)&fake_pcm_htab_token);
	Assert(fake_pcm_keysize > 0);
	Assert(fake_pcm_entrysize > 0);

	for (i = 0; i < fake_pcm_entry_count; i++) {
		char *entry = fake_pcm_entries.data[i];

		if (memcmp(entry, keyPtr, fake_pcm_keysize) == 0) {
			if (foundPtr != NULL)
				*foundPtr = true;
			if (action == HASH_REMOVE) {
				if (i + 1 < fake_pcm_entry_count)
					memmove(fake_pcm_entries.data[i], fake_pcm_entries.data[i + 1],
							(size_t)(fake_pcm_entry_count - i - 1) * FAKE_PCM_ENTRY_BYTES);
				fake_pcm_entry_count--;
				return entry;
			}
			return entry;
		}
	}

	if (foundPtr != NULL)
		*foundPtr = false;
	if (action == HASH_FIND || action == HASH_REMOVE)
		return NULL;
	if (action == HASH_ENTER_NULL && fake_pcm_entry_count >= fake_pcm_entry_max)
		return NULL;
	if (action == HASH_ENTER || action == HASH_ENTER_NULL) {
		char *entry = fake_pcm_entries.data[fake_pcm_entry_count++];

		memset(entry, 0, FAKE_PCM_ENTRY_BYTES);
		memcpy(entry, keyPtr, fake_pcm_keysize);
		return entry;
	}
	return NULL;
}

long
hash_get_num_entries(HTAB *hashp pg_attribute_unused())
{
	return fake_pcm_entry_count;
}

Size
hash_estimate_size(long num_entries pg_attribute_unused(), Size entrysize pg_attribute_unused())
{
	return (Size)num_entries * entrysize;
}

void
hash_seq_init(HASH_SEQ_STATUS *status, HTAB *hashp)
{
	status->hashp = hashp;
	status->curBucket = 0;
	status->curEntry = NULL;
}

void *
hash_seq_search(HASH_SEQ_STATUS *status)
{
	if (status->curBucket >= (uint32)fake_pcm_entry_count)
		return NULL;
	return fake_pcm_entries.data[status->curBucket++];
}

void
hash_seq_term(HASH_SEQ_STATUS *status pg_attribute_unused())
{}

void
LWLockInitialize(LWLock *lock pg_attribute_unused(), int tranche_id pg_attribute_unused())
{}

bool
LWLockAcquire(LWLock *lock, LWLockMode mode)
{
	Assert(fake_lwlock_depth < (int)lengthof(fake_lwlock_stack));
	fake_lwlock_stack[fake_lwlock_depth] = lock;
	fake_lwlock_mode_stack[fake_lwlock_depth] = mode;
	fake_lwlock_depth++;
	fake_lwlock_held = lock;
	fake_lwlock_mode = mode;
	fake_lwlock_wait_event_seen = ut_wait_event_info_storage;
	return true;
}

void
LWLockRelease(LWLock *lock)
{
	Assert(fake_lwlock_depth > 0);
	Assert(fake_lwlock_stack[fake_lwlock_depth - 1] == lock);
	fake_lwlock_depth--;
	fake_lwlock_stack[fake_lwlock_depth] = NULL;
	if (fake_lwlock_depth > 0) {
		fake_lwlock_held = fake_lwlock_stack[fake_lwlock_depth - 1];
		fake_lwlock_mode = fake_lwlock_mode_stack[fake_lwlock_depth - 1];
	} else {
		fake_lwlock_held = NULL;
		fake_lwlock_mode = LW_EXCLUSIVE;
	}
}

bool
LWLockHeldByMeInMode(LWLock *lock, LWLockMode mode)
{
	int i;

	for (i = fake_lwlock_depth - 1; i >= 0; i--)
		if (fake_lwlock_stack[i] == lock && fake_lwlock_mode_stack[i] == mode)
			return true;
	return false;
}

/* ----------
 * PGRAC: spec-2.31 D1 v0.4 — ConditionVariable stubs for PCM-H1..H4.
 *
 *	cluster_pcm_lock.c now uses ConditionVariable for incompatible-state
 *	wait.  Unit tests are single-threaded, so the Sleep stub can't really
 *	block;  instead it records the call and (if armed) performs a release
 *	on a target tag so the acquire loop sees compatible state on retry.
 *	Counter variable declarations live above (before reset_fake_pcm_runtime).
 * ----------
 */
void
ConditionVariableInit(ConditionVariable *cv pg_attribute_unused())
{
	fake_cv_init_count++;
}

void
ConditionVariablePrepareToSleep(ConditionVariable *cv pg_attribute_unused())
{
	fake_cv_prepare_count++;
}

void
ConditionVariableSleep(ConditionVariable *cv pg_attribute_unused(), uint32 wait_event_info)
{
	fake_cv_sleep_count++;
	fake_cv_sleep_wait_event = wait_event_info;
	if (fake_cv_wake_release.armed) {
		int save_node = cluster_node_id;

		fake_cv_wake_release.armed = false;
		cluster_node_id = fake_cv_wake_release.holder_node;
		cluster_pcm_lock_release(fake_cv_wake_release.tag);
		cluster_node_id = save_node;
	}
	if (fake_cv_wake_pending_x_clear.armed) {
		fake_cv_wake_pending_x_clear.armed = false;
		UT_ASSERT(cluster_pcm_lock_clear_queue_pending_x_exact(
			fake_cv_wake_pending_x_clear.tag, fake_cv_wake_pending_x_clear.requester_node,
			fake_cv_wake_pending_x_clear.ticket_id));
	}
}

bool
ConditionVariableCancelSleep(void)
{
	fake_cv_cancel_count++;
	return false;
}

void
ConditionVariableBroadcast(ConditionVariable *cv pg_attribute_unused())
{
	fake_cv_broadcast_count++;
}

void
ConditionVariableSignal(ConditionVariable *cv pg_attribute_unused())
{
	/* unused by cluster_pcm_lock.c but linker may require */
}

TimestampTz
GetCurrentTimestamp(void)
{
	return (TimestampTz)0;
}

Size
add_size(Size s1, Size s2)
{
	return s1 + s2;
}

Size
mul_size(Size s1, Size s2)
{
	return s1 * s2;
}

void
cluster_shmem_register_region(const ClusterShmemRegion *region pg_attribute_unused())
{}

void
cluster_injection_run(const char *name)
{
	if (fake_acquire_entry_handoff_armed && strcmp(name, "cluster-pcm-acquire-entry") == 0) {
		fake_acquire_entry_handoff_armed = false;
		cluster_injection_armed_count = 0;
		UT_ASSERT_EQ((int)cluster_pcm_lock_apply_gcs_transition(fake_acquire_entry_handoff_tag,
																fake_acquire_entry_handoff_release,
																fake_acquire_entry_handoff_source),
					 1);
		UT_ASSERT_EQ((int)cluster_pcm_lock_apply_gcs_transition(fake_acquire_entry_handoff_tag,
																PCM_TRANS_N_TO_X,
																fake_acquire_entry_handoff_target),
					 1);
	}
}

/* PGRAC spec-2.32 D5 stubs:  cluster_pcm_lock.c now calls cluster_gcs
 * helpers from each mutation entry point (master lookup branch).  Test
 * fixture forces local path by returning cluster_node_id from lookup. */
int
cluster_gcs_lookup_master(BufferTag tag pg_attribute_unused())
{
	return cluster_node_id;
}

void
cluster_gcs_send_transition_and_wait(BufferTag tag pg_attribute_unused(),
									 PcmLockTransition trans pg_attribute_unused(),
									 int master_node pg_attribute_unused())
{
	/* Unreachable when lookup returns self;  abort to catch test fixture bugs. */
	abort();
}

/* spec-2.33 D3 stub: cluster_pcm_lock_acquire_buffer (D7) takes the data-
 * plane branch when master is remote.  Fixture forces local path; reaching
 * here = test bug. */
bool
cluster_gcs_send_block_request_and_wait(struct BufferDesc *buf pg_attribute_unused(),
										PcmLockTransition trans pg_attribute_unused(),
										int master_node pg_attribute_unused(),
										bool clean_eligible pg_attribute_unused(),
										bool *out_retry_denied pg_attribute_unused())
{
	abort();
}

/* spec-5.2 D2 sub-case B stub: local-master read-image forward.  The pcm_lock
 * fixture records the selected holder so optimistic-precheck handoff tests can
 * prove the buffer-aware S path routes to the existing one-shot image helper
 * instead of the tag-only fail-closed terminal. */
bool
cluster_gcs_local_master_read_image_and_wait(struct BufferDesc *buf pg_attribute_unused(),
											 const PcmAuthoritySnapshot *expected,
											 bool *out_retry_denied)
{
	*out_retry_denied = false;
	fake_local_read_image_count++;
	fake_local_read_image_expected = *expected;
	fake_local_read_image_holder = expected->x_holder_node;
	return false;
}

/* spec-5.2 D11 stub: record the authoritative holder selected by the
 * buffer-aware local-master path.  The real data-plane behavior is covered by
 * the GCS block tests; this fixture only proves PCM routing and authority. */
bool
cluster_gcs_local_master_x_transfer_and_wait(struct BufferDesc *buf pg_attribute_unused(),
											 const PcmAuthoritySnapshot *expected,
											 bool clean_eligible pg_attribute_unused(),
											 bool *out_retry_denied)
{
	*out_retry_denied = false;
	fake_local_x_transfer_count++;
	fake_local_x_transfer_expected = *expected;
	fake_local_x_transfer_holder = expected->x_holder_node;
	return true;
}

/* spec-2.35 D3 stub:  HC110 master_holder lifecycle counter bump invoked
 * from cluster_pcm_transition_apply helpers.  Standalone fixture has no
 * ClusterGcsBlockShared; vacuous no-op. */
void
cluster_gcs_block_bump_master_holder_lifecycle(void)
{}

/* GCS-race round-4c FUNC-1 stub: the tag-only local-master grant tail calls
 * the storage-fallback SCN verify.  The standalone fixture has no
 * ClusterGcsBlockShared / no watermark (query returns InvalidScn), so the
 * real helper would short-circuit to a no-op — mirror that here. */
void
cluster_gcs_block_fallback_verify_refresh(struct BufferDesc *buf pg_attribute_unused(),
										  BufferTag tag pg_attribute_unused(),
										  SCN expected_scn pg_attribute_unused())
{}

/* ereport stubs — minimal enough to satisfy linker.  ereport(ERROR, ...) in
 * cluster_pcm_lock.o calls errstart_cold + errfinish; test_pcm_b_local_master_
 * remote_x_holder_fail_closed exercises that path via UT_EXPECT_EREPORT. */
bool
errstart(int elevel, const char *domain pg_attribute_unused())
{
	return elevel >= ERROR;
}

bool
errstart_cold(int elevel, const char *domain)
{
	return errstart(elevel, domain);
}

void
errfinish(const char *filename pg_attribute_unused(), int lineno pg_attribute_unused(),
		  const char *funcname pg_attribute_unused())
{
	ut_ereport_fired_count++;
	if (ut_ereport_jump_armed)
		siglongjmp(ut_ereport_jump, 1);
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
errmsg_internal(const char *fmt pg_attribute_unused(), ...)
{
	return 0;
}

int
errhint(const char *fmt pg_attribute_unused(), ...)
{
	return 0;
}

int
errdetail(const char *fmt pg_attribute_unused(), ...)
{
	return 0;
}

/* spec-6.14 D9 amend: acquire_buffer's no-backend-identity guard reads
 * MyBackendId; a valid id (1) keeps the historical acquire paths open. */
BackendId MyBackendId = 1;

#define UT_EXPECT_EREPORT(stmt)                                                                    \
	do {                                                                                           \
		if (sigsetjmp(ut_ereport_jump, 1) == 0) {                                                  \
			ut_ereport_jump_armed = true;                                                          \
			stmt;                                                                                  \
			ut_ereport_jump_armed = false;                                                         \
			UT_ASSERT(false);                                                                      \
		} else {                                                                                   \
			ut_ereport_jump_armed = false;                                                         \
			UT_ASSERT(ut_ereport_fired_count > 0);                                                 \
		}                                                                                          \
	} while (0)


/* ============================================================
 * Tests
 * ============================================================ */

UT_TEST(test_pcm_lock_mode_constant_aliases_match_pcm_state)
{
	UT_ASSERT_EQ((int)PCM_LOCK_MODE_N, 0);
	UT_ASSERT_EQ((int)PCM_LOCK_MODE_S, 1);
	UT_ASSERT_EQ((int)PCM_LOCK_MODE_X, 2);
	UT_ASSERT_EQ((int)PCM_LOCK_MODE_N, (int)PCM_STATE_N);
	UT_ASSERT_EQ((int)PCM_LOCK_MODE_S, (int)PCM_STATE_S);
	UT_ASSERT_EQ((int)PCM_LOCK_MODE_X, (int)PCM_STATE_X);
}

UT_TEST(test_pcm_lock_transition_count_is_9)
{
	UT_ASSERT_EQ(PCM_TRANSITION_COUNT, 9);
}

UT_TEST(test_pcm_lock_transition_enum_values_are_1_to_9)
{
	UT_ASSERT_EQ((int)PCM_TRANS_N_TO_S, 1);
	UT_ASSERT_EQ((int)PCM_TRANS_N_TO_X, 2);
	UT_ASSERT_EQ((int)PCM_TRANS_S_TO_X_UPGRADE, 3);
	UT_ASSERT_EQ((int)PCM_TRANS_X_TO_S_DOWNGRADE, 4);
	UT_ASSERT_EQ((int)PCM_TRANS_X_TO_N_DOWNGRADE, 5);
	UT_ASSERT_EQ((int)PCM_TRANS_X_TO_N_RELEASE, 6);
	UT_ASSERT_EQ((int)PCM_TRANS_S_TO_N_INVALIDATE, 7);
	UT_ASSERT_EQ((int)PCM_TRANS_S_TO_N_RELEASE, 8);
	UT_ASSERT_EQ((int)PCM_TRANS_S_TO_X_CLEANOUT, 9);
}

UT_TEST(test_pcm_grd_max_entries_default_is_minus_one)
{
	/*
	 * spec-2.30 D5:  default changed 0 → -1 sentinel (auto-resolve to
	 * NBuffers at startup);  explicit 0 = disable path.
	 */
	extern int cluster_pcm_grd_max_entries;
	UT_ASSERT_EQ(cluster_pcm_grd_max_entries, -1);
}

UT_TEST(test_pcm_buffer_desc_invariants_hold_at_stage_2_30)
{
	UT_ASSERT_EQ((int)PCM_STATE_N, 0);
	UT_ASSERT_EQ((int)PCM_STATE_S, 1);
	UT_ASSERT_EQ((int)PCM_STATE_X, 2);
}

UT_TEST(test_pcm_lock_module_init_symbol_is_callable)
{
	void (*fn)(void) = cluster_pcm_lock_module_init;
	UT_ASSERT(fn != NULL);
}


/* ============================================================
 * spec-2.30 NEW tests T-pcm-1..15.
 * ============================================================ */

/* T-pcm-1..8: validator accepts each of 8 active transitions. */
UT_TEST(test_pcm_trans_1_n_to_s_validator_accepts)
{
	UT_ASSERT(cluster_pcm_transition_legal(PCM_STATE_N, PCM_STATE_S, PCM_TRANS_N_TO_S));
}

UT_TEST(test_pcm_trans_2_n_to_x_validator_accepts)
{
	UT_ASSERT(cluster_pcm_transition_legal(PCM_STATE_N, PCM_STATE_X, PCM_TRANS_N_TO_X));
}

UT_TEST(test_pcm_trans_3_s_to_x_upgrade_validator_accepts)
{
	UT_ASSERT(cluster_pcm_transition_legal(PCM_STATE_S, PCM_STATE_X, PCM_TRANS_S_TO_X_UPGRADE));
}

UT_TEST(test_pcm_trans_4_x_to_s_downgrade_validator_accepts)
{
	UT_ASSERT(cluster_pcm_transition_legal(PCM_STATE_X, PCM_STATE_S, PCM_TRANS_X_TO_S_DOWNGRADE));
}

UT_TEST(test_pcm_trans_5_x_to_n_downgrade_validator_accepts)
{
	UT_ASSERT(cluster_pcm_transition_legal(PCM_STATE_X, PCM_STATE_N, PCM_TRANS_X_TO_N_DOWNGRADE));
}

UT_TEST(test_pcm_trans_6_x_to_n_release_validator_accepts)
{
	UT_ASSERT(cluster_pcm_transition_legal(PCM_STATE_X, PCM_STATE_N, PCM_TRANS_X_TO_N_RELEASE));
}

UT_TEST(test_pcm_trans_7_s_to_n_invalidate_validator_accepts)
{
	UT_ASSERT(cluster_pcm_transition_legal(PCM_STATE_S, PCM_STATE_N, PCM_TRANS_S_TO_N_INVALIDATE));
}

UT_TEST(test_pcm_trans_8_s_to_n_release_validator_accepts)
{
	UT_ASSERT(cluster_pcm_transition_legal(PCM_STATE_S, PCM_STATE_N, PCM_TRANS_S_TO_N_RELEASE));
}


/* T-pcm-9: HC60 — Trans-9 reachable from validator. */
UT_TEST(test_pcm_trans_9_cleanout_validator_reachable_but_apply_fail_closed)
{
	/*
	 * HC60 — validator accepts as legal entry transition (reachable from
	 * validator);  apply body fail-closed FEATURE_NOT_SUPPORTED until
	 * Stage 3 AD-006 第五轮 wires ITL cleanout.  Counter永 0.
	 */
	UT_ASSERT(cluster_pcm_transition_legal(PCM_STATE_S, PCM_STATE_X, PCM_TRANS_S_TO_X_CLEANOUT));
	UT_ASSERT_EQ((int)cluster_pcm_get_trans_s_to_x_cleanout_count(), 0);
}


/* T-pcm-10: HC56 — validator rejects illegal combinations. */
UT_TEST(test_pcm_illegal_transition_validator_rejects)
{
	/* (N → X) with trans=N_TO_S code → illegal */
	UT_ASSERT(!cluster_pcm_transition_legal(PCM_STATE_N, PCM_STATE_X, PCM_TRANS_N_TO_S));
	/* (S → S) any trans → illegal (no self-transition) */
	UT_ASSERT(!cluster_pcm_transition_legal(PCM_STATE_S, PCM_STATE_S, PCM_TRANS_N_TO_S));
	/* (X → X) any trans → illegal */
	UT_ASSERT(!cluster_pcm_transition_legal(PCM_STATE_X, PCM_STATE_X, PCM_TRANS_N_TO_X));
}


/* T-pcm-11: disable path — ClusterPcm == NULL → counter accessors return 0. */
UT_TEST(test_pcm_disable_path_counters_return_zero)
{
	/*
	 * In cluster_unit binary cluster_pcm_grd_init is never called so
	 * ClusterPcm stays NULL — all 9 counter accessors return 0.
	 */
	UT_ASSERT_EQ((int)cluster_pcm_get_trans_n_to_s_count(), 0);
	UT_ASSERT_EQ((int)cluster_pcm_get_trans_n_to_x_count(), 0);
	UT_ASSERT_EQ((int)cluster_pcm_get_trans_s_to_x_upgrade_count(), 0);
	UT_ASSERT_EQ((int)cluster_pcm_get_trans_x_to_s_downgrade_count(), 0);
	UT_ASSERT_EQ((int)cluster_pcm_get_trans_x_to_n_downgrade_count(), 0);
	UT_ASSERT_EQ((int)cluster_pcm_get_trans_x_to_n_release_count(), 0);
	UT_ASSERT_EQ((int)cluster_pcm_get_trans_s_to_n_invalidate_count(), 0);
	UT_ASSERT_EQ((int)cluster_pcm_get_trans_s_to_n_release_count(), 0);
	UT_ASSERT_EQ((int)cluster_pcm_get_trans_s_to_x_cleanout_count(), 0);
}


/* T-pcm-12: HC59 fail-closed cap — link-only surface verification. */
UT_TEST(test_pcm_grd_entry_lifecycle_link_surface)
{
	/*
	 * HC59 lifecycle (alloc-on-first-acquire / never-freed-until-shutdown)
	 * is verified by cluster_tap 108 under a live postmaster.  Here we
	 * verify the cap GUC surface exists.
	 */
	extern int cluster_pcm_grd_max_entries;
	UT_ASSERT(&cluster_pcm_grd_max_entries != NULL);
}


/* T-pcm-13: HC61 per-entry LWLock granularity — symbol existence. */
UT_TEST(test_pcm_per_entry_lwlock_independence_link_surface)
{
	/*
	 * HC61 per-entry LWLockPadded granularity (vs per-shard / global).
	 *  Real concurrency test deferred to cluster_tap.  Here verify
	 *  cluster_pcm_lock_module_init symbol is linkable (drives shmem +
	 *  LWTRANCHE_CLUSTER_PCM registration).
	 */
	void (*fn)(void) = cluster_pcm_lock_module_init;
	UT_ASSERT(fn != NULL);
}


/* T-pcm-14: HC58 PI bitmap atomic — verify accessor symbol exists. */
UT_TEST(test_pcm_pi_bitmap_atomic_accessor_linkable)
{
	/*
	 * HC58 PI bitmap atomic update — bitmap field is internal to file-
	 *  private GrdEntry;  observation surface is dump_pcm + future
	 *  cluster_tap.  Here verify cluster_pcm_get_trans_x_to_s_downgrade_count
	 *  (the PI-set transition counter accessor) symbol is linkable.
	 */
	uint64 (*fn)(void) = cluster_pcm_get_trans_x_to_s_downgrade_count;
	UT_ASSERT(fn != NULL);
}


/* T-pcm-15: 9 counter accessor surface — all linkable + return 0 under disabled. */
UT_TEST(test_pcm_counter_observability_9_accessors_linkable)
{
	uint64 (*fns[9])(void) = {
		cluster_pcm_get_trans_n_to_s_count,
		cluster_pcm_get_trans_n_to_x_count,
		cluster_pcm_get_trans_s_to_x_upgrade_count,
		cluster_pcm_get_trans_x_to_s_downgrade_count,
		cluster_pcm_get_trans_x_to_n_downgrade_count,
		cluster_pcm_get_trans_x_to_n_release_count,
		cluster_pcm_get_trans_s_to_n_invalidate_count,
		cluster_pcm_get_trans_s_to_n_release_count,
		cluster_pcm_get_trans_s_to_x_cleanout_count,
	};
	int i;

	for (i = 0; i < 9; i++) {
		UT_ASSERT(fns[i] != NULL);
		UT_ASSERT_EQ((int)fns[i](), 0);
	}
}


UT_TEST(test_pcm_real_shared_s_holders_release_independently)
{
	BufferTag tag = make_tag(1);

	reset_fake_pcm_runtime(4);

	cluster_node_id = 0;
	cluster_pcm_lock_acquire(tag, PCM_LOCK_MODE_S);
	UT_ASSERT_EQ((int)cluster_pcm_lock_query(tag), (int)PCM_LOCK_MODE_S);
	UT_ASSERT_EQ((int)cluster_pcm_get_trans_n_to_s_count(), 1);

	cluster_node_id = 1;
	cluster_pcm_lock_acquire(tag, PCM_LOCK_MODE_S);
	UT_ASSERT_EQ((int)cluster_pcm_lock_query(tag), (int)PCM_LOCK_MODE_S);
	UT_ASSERT_EQ((int)cluster_pcm_get_trans_n_to_s_count(), 1);

	cluster_node_id = 0;
	cluster_pcm_lock_release(tag);
	UT_ASSERT_EQ((int)cluster_pcm_lock_query(tag), (int)PCM_LOCK_MODE_S);

	cluster_node_id = 1;
	cluster_pcm_lock_release(tag);
	UT_ASSERT_EQ((int)cluster_pcm_lock_query(tag), (int)PCM_LOCK_MODE_N);
	UT_ASSERT_EQ((int)cluster_pcm_get_trans_s_to_n_release_count(), 2);
}


UT_TEST(test_pcm_real_x_release_and_downgrade_require_owner)
{
	BufferTag tag = make_tag(2);

	reset_fake_pcm_runtime(4);

	cluster_node_id = 0;
	cluster_pcm_lock_acquire(tag, PCM_LOCK_MODE_X);
	UT_ASSERT_EQ((int)cluster_pcm_lock_query(tag), (int)PCM_LOCK_MODE_X);

	cluster_node_id = 1;
	UT_EXPECT_EREPORT(cluster_pcm_lock_release(tag));
	UT_ASSERT_EQ((int)cluster_pcm_lock_query(tag), (int)PCM_LOCK_MODE_X);
	UT_EXPECT_EREPORT(cluster_pcm_lock_downgrade(tag, PCM_LOCK_MODE_S, true));
	UT_ASSERT_EQ((int)cluster_pcm_lock_query(tag), (int)PCM_LOCK_MODE_X);

	cluster_node_id = 0;
	cluster_pcm_lock_downgrade(tag, PCM_LOCK_MODE_S, true);
	UT_ASSERT_EQ((int)cluster_pcm_lock_query(tag), (int)PCM_LOCK_MODE_S);
	cluster_pcm_lock_release(tag);
	UT_ASSERT_EQ((int)cluster_pcm_lock_query(tag), (int)PCM_LOCK_MODE_N);
}


UT_TEST(test_pcm_real_upgrade_requires_single_s_holder)
{
	BufferTag tag = make_tag(3);

	reset_fake_pcm_runtime(4);

	cluster_node_id = 0;
	cluster_pcm_lock_acquire(tag, PCM_LOCK_MODE_S);
	cluster_node_id = 1;
	cluster_pcm_lock_acquire(tag, PCM_LOCK_MODE_S);

	cluster_node_id = 0;
	UT_EXPECT_EREPORT(cluster_pcm_lock_upgrade(tag));
	UT_ASSERT_EQ((int)cluster_pcm_lock_query(tag), (int)PCM_LOCK_MODE_S);

	cluster_node_id = 1;
	cluster_pcm_lock_release(tag);
	cluster_node_id = 0;
	cluster_pcm_lock_upgrade(tag);
	UT_ASSERT_EQ((int)cluster_pcm_lock_query(tag), (int)PCM_LOCK_MODE_X);
}


UT_TEST(test_pcm_real_summary_counts_live_entries)
{
	BufferTag tag_s = make_tag(4);
	BufferTag tag_x = make_tag(5);
	int n_count, s_count, x_count, pi_total, convert_q;

	reset_fake_pcm_runtime(4);

	cluster_node_id = 0;
	cluster_pcm_lock_acquire(tag_s, PCM_LOCK_MODE_S);
	cluster_pcm_lock_acquire(tag_x, PCM_LOCK_MODE_X);

	cluster_pcm_grd_get_summary(&n_count, &s_count, &x_count, &pi_total, &convert_q);
	UT_ASSERT_EQ(n_count, 0);
	UT_ASSERT_EQ(s_count, 1);
	UT_ASSERT_EQ(x_count, 1);
	UT_ASSERT_EQ(pi_total, 0);
	UT_ASSERT_EQ(convert_q, 0);

	cluster_pcm_lock_downgrade(tag_x, PCM_LOCK_MODE_N, true);
	cluster_pcm_grd_get_summary(&n_count, &s_count, &x_count, &pi_total, &convert_q);
	UT_ASSERT_EQ(n_count, 1);
	UT_ASSERT_EQ(s_count, 1);
	UT_ASSERT_EQ(x_count, 0);
	UT_ASSERT_EQ(pi_total, 1);
}

UT_TEST(test_pcm_grd_entry_abi_includes_authority_generation)
{
	reset_fake_pcm_runtime(4);
	UT_ASSERT_EQ(fake_pcm_entrysize, 272);
}

UT_TEST(test_pcm_grd_convert_queue_placeholder_remains_null)
{
	BufferTag tag = make_tag(64);
	int n_count, s_count, x_count, pi_total, convert_q;

	reset_fake_pcm_runtime(4);
	cluster_pcm_lock_acquire(tag, PCM_LOCK_MODE_S);
	cluster_pcm_grd_get_summary(&n_count, &s_count, &x_count, &pi_total, &convert_q);
	UT_ASSERT_EQ(convert_q, 0);
}


UT_TEST(test_pcm_real_wait_event_call_sites_are_exercised)
{
	BufferTag tag = make_tag(6);

	reset_fake_pcm_runtime(4);
	UT_ASSERT_EQ((int)fake_init_wait_event_seen, (int)WAIT_EVENT_PCM_GRD_INIT);

	cluster_node_id = 0;
	cluster_pcm_lock_acquire(tag, PCM_LOCK_MODE_S);
	UT_ASSERT_EQ((int)fake_lwlock_wait_event_seen, (int)WAIT_EVENT_PCM_TRANSITION_APPLY);
	UT_ASSERT_EQ((int)ut_wait_event_info_storage, 0);
}


/* ============================================================
 * PGRAC: spec-2.31 D1 v0.4 — PCM API hardening (PCM-H1..H4).
 * ============================================================ */
UT_TEST(test_pcm_H1_same_node_s_refcount_increments)
{
	BufferTag tag = make_tag(10);

	reset_fake_pcm_runtime(4);

	cluster_node_id = 0;
	cluster_pcm_lock_acquire(tag, PCM_LOCK_MODE_S);
	UT_ASSERT_EQ((int)cluster_pcm_lock_query(tag), (int)PCM_LOCK_MODE_S);
	/* N→S transition counter incremented once */
	UT_ASSERT_EQ((int)cluster_pcm_get_trans_n_to_s_count(), 1);

	/* Second S acquire by same node — refcount bumps, no N→S transition. */
	cluster_pcm_lock_acquire(tag, PCM_LOCK_MODE_S);
	UT_ASSERT_EQ((int)cluster_pcm_lock_query(tag), (int)PCM_LOCK_MODE_S);
	UT_ASSERT_EQ((int)cluster_pcm_get_trans_n_to_s_count(), 1);

	/* First release: state still S (refcount drops from 2 to 1). */
	cluster_pcm_lock_release(tag);
	UT_ASSERT_EQ((int)cluster_pcm_lock_query(tag), (int)PCM_LOCK_MODE_S);
	UT_ASSERT_EQ((int)cluster_pcm_get_trans_s_to_n_release_count(), 0);
}


UT_TEST(test_pcm_H2_last_s_release_transitions_to_n)
{
	BufferTag tag = make_tag(11);

	reset_fake_pcm_runtime(4);

	cluster_node_id = 0;
	cluster_pcm_lock_acquire(tag, PCM_LOCK_MODE_S);
	cluster_pcm_lock_acquire(tag, PCM_LOCK_MODE_S);

	/* First release: state remains S. */
	cluster_pcm_lock_release(tag);
	UT_ASSERT_EQ((int)cluster_pcm_lock_query(tag), (int)PCM_LOCK_MODE_S);

	/* Second release (refcount→0): state→N, broadcast fires. */
	cluster_pcm_lock_release(tag);
	UT_ASSERT_EQ((int)cluster_pcm_lock_query(tag), (int)PCM_LOCK_MODE_N);
	UT_ASSERT_EQ((int)cluster_pcm_get_trans_s_to_n_release_count(), 1);
	UT_ASSERT((fake_cv_broadcast_count) >= (1));
}

/*
 * T400-P0-21 residual — a completed BufferDesc-aware S grant can be forced
 * through the holder-publication BARRIER_CLOSED restart before bufmgr commits
 * its local ownership mirror.  The restart is one physical cache-residency
 * attempt, not a second logical tag-only holder.  After content unlock
 * preserves that residency, the one real eviction release must therefore
 * remove the node's S bit completely.
 *
 * This test intentionally executes the master-side consequence of the actual
 * bufmgr restart branch.  test_granted_pending_holder_barrier_releases_* in
 * test_cluster_pcm_own.c locks the branch itself to the release-before-rearm
 * ordering.
 */
UT_TEST(test_pcm_buffer_s_barrier_restart_single_eviction_closes_residency)
{
	BufferTag tag = make_tag(12);
	BufferDesc buf;
	bool retry_denied = false;

	reset_fake_pcm_runtime(4);
	memset(&buf, 0, sizeof(buf));
	buf.tag = tag;
	buf.pcm_state = (uint8)PCM_STATE_N;

	/* First completed S grant, before holder-ledger publication. */
	UT_ASSERT(cluster_pcm_lock_acquire_buffer(&buf, PCM_LOCK_MODE_S, &retry_denied));
	UT_ASSERT(!retry_denied);
	UT_ASSERT_EQ((int)cluster_pcm_lock_query(tag), (int)PCM_LOCK_MODE_S);

	/*
	 * The BARRIER_CLOSED restart first releases this completed attempt, then
	 * re-enters the BufferDesc-aware acquire after dropping its exact
	 * GRANT_PENDING token.  It must not leave two master declarations owed by
	 * one resident BufferDesc.
	 */
	cluster_pcm_lock_release_buffer_for_eviction(&buf, PCM_LOCK_MODE_S);
	retry_denied = false;
	UT_ASSERT(cluster_pcm_lock_acquire_buffer(&buf, PCM_LOCK_MODE_S, &retry_denied));
	UT_ASSERT(!retry_denied);

	/* HC112 keeps S at content unlock; physical eviction releases it once. */
	cluster_pcm_lock_unlock_content_buffer(&buf, PCM_LOCK_MODE_S);
	cluster_pcm_lock_release_buffer_for_eviction(&buf, PCM_LOCK_MODE_S);
	UT_ASSERT_EQ((int)cluster_pcm_lock_query(tag), (int)PCM_LOCK_MODE_N);
}

/*
 * Negative leg for the fix above: the public tag-only API retains historical
 * logical nesting.  Two independent logical acquires still owe two releases;
 * making all same-node S re-acquires idempotent would under-release this case.
 */
UT_TEST(test_pcm_tag_only_nested_s_still_requires_two_releases)
{
	BufferTag tag = make_tag(13);

	reset_fake_pcm_runtime(4);
	cluster_pcm_lock_acquire(tag, PCM_LOCK_MODE_S);
	cluster_pcm_lock_acquire(tag, PCM_LOCK_MODE_S);

	cluster_pcm_lock_release(tag);
	UT_ASSERT_EQ((int)cluster_pcm_lock_query(tag), (int)PCM_LOCK_MODE_S);
	cluster_pcm_lock_release(tag);
	UT_ASSERT_EQ((int)cluster_pcm_lock_query(tag), (int)PCM_LOCK_MODE_N);
}


UT_TEST(test_pcm_H2b_same_node_s_residency_upgrades_to_x)
{
	BufferTag tag = make_tag(14);

	reset_fake_pcm_runtime(4);

	cluster_node_id = 0;
	cluster_pcm_lock_acquire(tag, PCM_LOCK_MODE_S);
	UT_ASSERT_EQ((int)cluster_pcm_lock_query(tag), (int)PCM_LOCK_MODE_S);

	/*
	 * spec-2.35 HC111/HC112 keeps S as cache residency after content-lock
	 * unlock.  A later local X acquire by the same node must upgrade the
	 * residency bit instead of waiting on its own preserved S holder.
	 */
	cluster_pcm_lock_acquire(tag, PCM_LOCK_MODE_X);
	UT_ASSERT_EQ((int)cluster_pcm_lock_query(tag), (int)PCM_LOCK_MODE_X);
	UT_ASSERT_EQ((int)cluster_pcm_get_trans_s_to_x_upgrade_count(), 1);
	UT_ASSERT_EQ(fake_cv_sleep_count, 0);
}


UT_TEST(test_pcm_H3_incompatible_x_waits_and_wakes)
{
	BufferTag tag = make_tag(12);

	reset_fake_pcm_runtime(4);

	/* Node 0 holds X. */
	cluster_node_id = 0;
	cluster_pcm_lock_acquire(tag, PCM_LOCK_MODE_X);
	UT_ASSERT_EQ((int)cluster_pcm_lock_query(tag), (int)PCM_LOCK_MODE_X);

	/* Arm stub:  on first Sleep, simulate node-0 releasing X.  Then the
	 * acquire loop sees state=N and proceeds to acquire X for node 1. */
	fake_cv_wake_release.tag = tag;
	fake_cv_wake_release.holder_node = 0;
	fake_cv_wake_release.armed = true;

	cluster_node_id = 1;
	cluster_pcm_lock_acquire(tag, PCM_LOCK_MODE_X);

	UT_ASSERT_EQ((int)cluster_pcm_lock_query(tag), (int)PCM_LOCK_MODE_X);
	UT_ASSERT((fake_cv_sleep_count) >= (1));
	UT_ASSERT_EQ((int)fake_cv_sleep_wait_event, (int)WAIT_EVENT_PCM_COMPATIBLE_STATE_WAIT);
	UT_ASSERT((fake_cv_prepare_count) >= (1));
	UT_ASSERT((fake_cv_cancel_count) >= (1));
}


UT_TEST(test_pcm_H4_release_broadcasts_only_on_state_change)
{
	BufferTag tag = make_tag(13);

	reset_fake_pcm_runtime(4);

	cluster_node_id = 0;
	cluster_pcm_lock_acquire(tag, PCM_LOCK_MODE_X);

	/* X→N release: broadcast fires (state changed to N). */
	cluster_pcm_lock_release(tag);
	UT_ASSERT_EQ((int)cluster_pcm_lock_query(tag), (int)PCM_LOCK_MODE_N);
	UT_ASSERT_EQ(fake_cv_broadcast_count, 1);

	/* S acquire + release (single holder): broadcast fires again. */
	cluster_pcm_lock_acquire(tag, PCM_LOCK_MODE_S);
	cluster_pcm_lock_release(tag);
	UT_ASSERT_EQ((int)cluster_pcm_lock_query(tag), (int)PCM_LOCK_MODE_N);
	UT_ASSERT_EQ(fake_cv_broadcast_count, 2);

	/* Two S acquires + one release: refcount 2→1, no state change, no broadcast. */
	cluster_pcm_lock_acquire(tag, PCM_LOCK_MODE_S);
	cluster_pcm_lock_acquire(tag, PCM_LOCK_MODE_S);
	cluster_pcm_lock_release(tag);
	UT_ASSERT_EQ((int)cluster_pcm_lock_query(tag), (int)PCM_LOCK_MODE_S);
	UT_ASSERT_EQ(fake_cv_broadcast_count, 2);

	/* Second release: refcount 1→0, state→N, broadcast fires. */
	cluster_pcm_lock_release(tag);
	UT_ASSERT_EQ((int)cluster_pcm_lock_query(tag), (int)PCM_LOCK_MODE_N);
	UT_ASSERT_EQ(fake_cv_broadcast_count, 3);
}


/* ============================================================
 * spec-4.7a D2/D3/D4 — block coherence gate decision logic (direct unit
 * coverage of the master-side X-contention gate, since the non-injection
 * e2e is blocked by the deferred concurrent-relation data plane — see
 * t/252 + spec-4.7a §4.1).
 * ============================================================ */

UT_TEST(test_pcm_d2_mode_covers_truth_table)
{
	/* X covers {S,X}; S covers {S}; N covers nothing (hold-until-revoked gate). */
	UT_ASSERT(cluster_pcm_mode_covers(PCM_LOCK_MODE_X, PCM_LOCK_MODE_S));
	UT_ASSERT(cluster_pcm_mode_covers(PCM_LOCK_MODE_X, PCM_LOCK_MODE_X));
	UT_ASSERT(cluster_pcm_mode_covers(PCM_LOCK_MODE_S, PCM_LOCK_MODE_S));
	UT_ASSERT(!cluster_pcm_mode_covers(PCM_LOCK_MODE_S, PCM_LOCK_MODE_X));
	UT_ASSERT(!cluster_pcm_mode_covers(PCM_LOCK_MODE_N, PCM_LOCK_MODE_S));
	UT_ASSERT(!cluster_pcm_mode_covers(PCM_LOCK_MODE_N, PCM_LOCK_MODE_X));
}

UT_TEST(test_pcm_d3_requester_is_holder_strict)
{
	BufferTag tag = make_tag(40);

	reset_fake_pcm_runtime(4);
	fake_cssd_dead_node = -1;

	cluster_node_id = 2;
	cluster_pcm_lock_acquire(tag, PCM_LOCK_MODE_X); /* node 2 holds X */

	/* x_holder==sender covers N→S and N→X (idempotent re-grant, D3). */
	UT_ASSERT(cluster_pcm_master_requester_is_holder(tag, 2, PCM_TRANS_N_TO_S));
	UT_ASSERT(cluster_pcm_master_requester_is_holder(tag, 2, PCM_TRANS_N_TO_X));
	/* S→X never self-regrants (real writer path → invalidate-then-grant). */
	UT_ASSERT(!cluster_pcm_master_requester_is_holder(tag, 2, PCM_TRANS_S_TO_X_UPGRADE));
	/* A non-holder is never a holder (fail-closed). */
	UT_ASSERT(!cluster_pcm_master_requester_is_holder(tag, 1, PCM_TRANS_N_TO_S));
	/* Missing entry → false (Rule 8.A fail-closed). */
	UT_ASSERT(!cluster_pcm_master_requester_is_holder(make_tag(41), 2, PCM_TRANS_N_TO_S));
}

UT_TEST(test_pcm_d4_other_live_holder_gate)
{
	BufferTag xtag = make_tag(42);
	BufferTag stag = make_tag(43);
	BufferTag selftag = make_tag(44);

	reset_fake_pcm_runtime(4);
	fake_cssd_dead_node = -1;

	/* node 2 holds X. */
	cluster_node_id = 2;
	cluster_pcm_lock_acquire(xtag, PCM_LOCK_MODE_X);

	/* Another live node (2) holds X → a different sender is BLOCKED (D4). */
	UT_ASSERT(cluster_pcm_master_other_live_holder_exists(xtag, 1));
	/* The holder itself is not an "other" holder → not blocked (self path). */
	UT_ASSERT(!cluster_pcm_master_other_live_holder_exists(xtag, 2));
	/* Missing entry → no holder → not blocked. */
	UT_ASSERT(!cluster_pcm_master_other_live_holder_exists(make_tag(99), 1));

	/* A DEAD holder is NOT counted — that is the warm-recovery path, not D4. */
	fake_cssd_dead_node = 2;
	UT_ASSERT(!cluster_pcm_master_other_live_holder_exists(xtag, 1));
	fake_cssd_dead_node = -1;

	/* node 1 and node 3 both hold S on stag. */
	cluster_node_id = 1;
	cluster_pcm_lock_acquire(stag, PCM_LOCK_MODE_S);
	cluster_node_id = 3;
	cluster_pcm_lock_acquire(stag, PCM_LOCK_MODE_S);
	UT_ASSERT(cluster_pcm_master_other_live_holder_exists(stag, 1)); /* sees node 3 */
	UT_ASSERT(cluster_pcm_master_other_live_holder_exists(stag, 3)); /* sees node 1 */
	UT_ASSERT(cluster_pcm_master_other_live_holder_exists(stag, 0)); /* non-holder sees both */

	/* Sole S holder → no OTHER holder → not blocked (self can upgrade). */
	cluster_node_id = 1;
	cluster_pcm_lock_acquire(selftag, PCM_LOCK_MODE_S);
	UT_ASSERT(!cluster_pcm_master_other_live_holder_exists(selftag, 1));
}

/*
 * spec-4.7a B (HG7 local-path completion) — the acquire-side bounded fail-closed.
 * When the local master path meets an incompatible LIVE remote holder and
 * hold-until-revoked is on, it must ereport (FEATURE_NOT_SUPPORTED) rather than
 * hang on wait_cv (the writer transfer that would revoke the holder is deferred).
 * This is the acquire-path mirror of the D4 master-dispatch gate above; together
 * they cover both round-trip paths HG7 promises "no hang" for.
 */
UT_TEST(test_pcm_b_local_master_remote_x_holder_fail_closed)
{
	BufferTag tag = make_tag(45);
	bool save = cluster_gcs_block_local_cache;

	reset_fake_pcm_runtime(4);
	fake_cssd_dead_node = -1;

	/* node 2 holds X — the conflicting remote LIVE holder. */
	cluster_node_id = 2;
	cluster_pcm_lock_acquire(tag, PCM_LOCK_MODE_X);

	/* node 1 (local master) wants X with hold-until-revoked on → fail-closed. */
	cluster_node_id = 1;
	cluster_gcs_block_local_cache = true;
	UT_EXPECT_EREPORT(cluster_pcm_lock_acquire(tag, PCM_LOCK_MODE_X));
	cluster_gcs_block_local_cache = save;

	/* The holder is untouched — no illegal transition applied on the fail-closed
	 * path (node 2 still records X). */
	UT_ASSERT(cluster_pcm_master_other_live_holder_exists(tag, 1));

	/* A DEAD conflicting holder is NOT fail-closed — that block belongs to the
	 * warm-recovery path; the acquire falls through to the legitimate wait.  We
	 * assert only the holder-liveness scoping of the gate here (the wait itself
	 * is covered by H3); do not call acquire (it would block on wait_cv). */
	fake_cssd_dead_node = 2;
	UT_ASSERT(!cluster_pcm_master_other_live_holder_exists(tag, 1));
	fake_cssd_dead_node = -1;
}

/*
 * spec-4.7 D1 — RECOVERING gate fail-closed.  When a block resource is
 * RECOVERING, cluster_pcm_lock_acquire_buffer must fail-closed 53R9L after the
 * bounded wait — never route to the dead master nor serve stale local state.
 * With cluster.gcs_block_recovery_wait_ms = 0 the gate fail-closes immediately
 * (deterministic;  the ereport precedes any sleep / CHECK_FOR_INTERRUPTS).
 * This proves the gate logic that lives in cluster_pcm_lock.o;  the phase
 * predicate itself (master DEAD → RECOVERING) is e2e-deferred (measure-first,
 * spec-4.7 D0 Impl note v0.1) and unit-proven with the master-rebuild logic in
 * spec-4.7 D3 (test_cluster_gcs_recovery).
 */
UT_TEST(test_pcm_d1_recovering_gate_fail_closed)
{
	BufferDesc buf;
	bool retry_denied = false;

	reset_fake_pcm_runtime(2);
	buf.tag = make_tag(77);

	fake_block_phase = GCS_BLOCK_RECOVERING;
	cluster_gcs_block_recovery_wait_ms = 0; /* immediate fail-closed, no sleep */
	UT_EXPECT_EREPORT(cluster_pcm_lock_acquire_buffer(&buf, PCM_LOCK_MODE_S, &retry_denied));
	fake_block_phase = GCS_BLOCK_NORMAL;
	cluster_gcs_block_recovery_wait_ms = 200;
}

/*
 * spec-4.7 D2 — master rebuild from one survivor re-declare.  Proves the
 * rebuild records the declared holder (X authoritative) and the monotone-max
 * PI watermark.  The block-protocol e2e is deferred (measure-first, D0 Impl
 * note v0.1);  this is the L239 unit-proof of the 8.A-relevant master-view
 * reconstruction (D3 adds the not-double-X conflict invariant).
 */
UT_TEST(test_pcm_d2_rebuild_from_redeclare)
{
	BufferTag tagx = make_tag(88);
	BufferTag tags = make_tag(89);

	reset_fake_pcm_runtime(4);
	fake_cssd_dead_node = -1;

	/* node 2 re-declares X on tagx with page_lsn 0x5000 + page_scn 0x5500
	 * (spec-2.41 D3 dual carrier). */
	cluster_gcs_block_master_rebuild_from_redeclare(tagx, (uint8)PCM_STATE_X, (XLogRecPtr)0x5000,
													(SCN)0x5500, 2, 7);
	/* The rebuilt master view records node 2 as the (live) X holder. */
	UT_ASSERT(cluster_pcm_master_other_live_holder_exists(tagx, 3));
	UT_ASSERT(!cluster_pcm_master_other_live_holder_exists(tagx, 2));
	UT_ASSERT_EQ((uint64)cluster_pcm_lock_pi_watermark_lsn_query(tagx), (uint64)0x5000);
	/* spec-2.41 D3 — the SCN watermark is advanced from page_scn (orthogonal). */
	UT_ASSERT_EQ((uint64)cluster_pcm_lock_pi_watermark_scn_query(tagx), (uint64)0x5500);

	/* node 1 re-declares S on tags with higher lsn+scn — both watermarks = max. */
	cluster_gcs_block_master_rebuild_from_redeclare(tags, (uint8)PCM_STATE_S, (XLogRecPtr)0x9000,
													(SCN)0x9900, 1, 7);
	UT_ASSERT(cluster_pcm_master_other_live_holder_exists(tags, 0));
	UT_ASSERT_EQ((uint64)cluster_pcm_lock_pi_watermark_lsn_query(tags), (uint64)0x9000);
	UT_ASSERT_EQ((uint64)cluster_pcm_lock_pi_watermark_scn_query(tags), (uint64)0x9900);

	/* A stale lower lsn+scn re-declare must NOT regress either watermark. */
	cluster_gcs_block_master_rebuild_from_redeclare(tags, (uint8)PCM_STATE_S, (XLogRecPtr)0x100,
													(SCN)0x150, 3, 7);
	UT_ASSERT_EQ((uint64)cluster_pcm_lock_pi_watermark_lsn_query(tags), (uint64)0x9000);
	UT_ASSERT_EQ((uint64)cluster_pcm_lock_pi_watermark_scn_query(tags), (uint64)0x9900);
}

/*
 * spec-4.7 D3 — not-double-X invariant (规则 8.A).  Two distinct nodes
 * declaring X on the SAME block (pre-crash single-X violated) must NEVER
 * reconstruct two X holders.  The first X declarer wins;  the conflicting
 * second is rejected (the rebuilt view keeps node 2, never node 3), so the
 * recovery path can never produce a cross-node double grant.
 */
UT_TEST(test_pcm_d3_not_double_x)
{
	BufferTag tag = make_tag(91);

	reset_fake_pcm_runtime(4);
	fake_cssd_dead_node = -1;

	cluster_gcs_block_master_rebuild_from_redeclare(tag, (uint8)PCM_STATE_X, (XLogRecPtr)0x4000,
													(SCN)0x4400, 2, 7);
	/* Conflicting X from a DIFFERENT node — must be rejected. */
	cluster_gcs_block_master_rebuild_from_redeclare(tag, (uint8)PCM_STATE_X, (XLogRecPtr)0x4000,
													(SCN)0x4400, 3, 7);

	/* x_holder stays node 2 (not 3):  node 2 self-excluded → false;  any other
	 * sender sees node 2 as the live X holder.  Had the conflicting node-3 X
	 * been applied, other_live_holder_exists(tag, 2) would be TRUE. */
	UT_ASSERT(!cluster_pcm_master_other_live_holder_exists(tag, 2));
	UT_ASSERT(cluster_pcm_master_other_live_holder_exists(tag, 3));

	/* Same node re-declaring X is idempotent (not a conflict).  spec-2.41 D3:
	 * carries page_scn alongside page_lsn (value irrelevant to this invariant). */
	UT_ASSERT(cluster_gcs_block_master_rebuild_from_redeclare(
		tag, (uint8)PCM_STATE_X, (XLogRecPtr)0x4000, (SCN)0x4400, 2, 7));
	UT_ASSERT(!cluster_pcm_master_other_live_holder_exists(tag, 2));
	UT_ASSERT(cluster_pcm_master_other_live_holder_exists(tag, 3));

	/*
	 * code-review P1 — X-vs-S contradiction (both directions) must fail-closed
	 * (return false), not silently keep/overwrite.
	 */
	{
		BufferTag tagxs = make_tag(92);
		BufferTag tagsx = make_tag(93);

		/* X-held then S from a DIFFERENT node → reject (was: silently dropped,
		 * returned true). */
		UT_ASSERT(cluster_gcs_block_master_rebuild_from_redeclare(
			tagxs, (uint8)PCM_STATE_X, (XLogRecPtr)0x10, (SCN)0x20, 2, 7));
		UT_ASSERT(!cluster_gcs_block_master_rebuild_from_redeclare(
			tagxs, (uint8)PCM_STATE_S, (XLogRecPtr)0x10, (SCN)0x20, 1, 7));
		/* still X by node 2 (S not merged). */
		UT_ASSERT(cluster_pcm_master_other_live_holder_exists(tagxs, 3));
		UT_ASSERT(!cluster_pcm_master_other_live_holder_exists(tagxs, 2));

		/* S-held by node 1 then X from a DIFFERENT node → reject (X-over-live-S
		 * = never reconstruct a double grant; was: silently overwrote S). */
		UT_ASSERT(cluster_gcs_block_master_rebuild_from_redeclare(
			tagsx, (uint8)PCM_STATE_S, (XLogRecPtr)0x10, (SCN)0x20, 1, 7));
		UT_ASSERT(!cluster_gcs_block_master_rebuild_from_redeclare(
			tagsx, (uint8)PCM_STATE_X, (XLogRecPtr)0x10, (SCN)0x20, 2, 7));
	}
}

/*
 * S3 forensics step 1a — SCN-watermark advance provenance ring.
 *	Every feed records {source, sender, request_id, epoch, old->new,
 *	advanced}; the latest record per tag is queryable.  The key semantic:
 *	a LATE / stale feed is recorded with advanced=false and the watermark
 *	unchanged — the branch-3 (watermark false-positive) discriminator a
 *	53R93 emit site attaches to its errdetail / LOG line.
 */
UT_TEST(test_pcm_wm_prov_table_keeps_last_advance)
{
	BufferTag tag = make_tag(95);
	BufferTag other = make_tag(96);
	ClusterPcmWmProv prov;

	reset_fake_pcm_runtime(4);
	fake_cssd_dead_node = -1;

	/* No feed yet — a definite NONE (no insert has ever been dropped). */
	UT_ASSERT(!cluster_pcm_lock_pi_watermark_prov_query(tag, &prov));
	UT_ASSERT_EQ((long)prov.source, (long)CLUSTER_PCM_WM_SRC_NONE);
	UT_ASSERT(!prov.table_full);

	/* Redeclare feed (real inline writer path): advancing feed recorded. */
	cluster_gcs_block_master_rebuild_from_redeclare(tag, (uint8)PCM_STATE_S, (XLogRecPtr)0x1000,
													(SCN)0x2000, 2, 7);
	UT_ASSERT(cluster_pcm_lock_pi_watermark_prov_query(tag, &prov));
	UT_ASSERT_EQ((long)prov.source, (long)CLUSTER_PCM_WM_SRC_REDECLARE);
	UT_ASSERT_EQ((long)prov.sender_node, 2L);
	UT_ASSERT_EQ((long)prov.epoch, 7L);
	UT_ASSERT_EQ((long)prov.new_scn, 0x2000L);

	/* ACK feed advances further: the tag's single slot is updated in place
	 * with the new advance's full wire identity. */
	cluster_pcm_lock_pi_watermark_scn_advance(tag, (SCN)0x3000, CLUSTER_PCM_WM_SRC_ACK_SLOT, 3,
											  4242, 9);
	UT_ASSERT(cluster_pcm_lock_pi_watermark_prov_query(tag, &prov));
	UT_ASSERT_EQ((long)prov.source, (long)CLUSTER_PCM_WM_SRC_ACK_SLOT);
	UT_ASSERT_EQ((long)prov.sender_node, 3L);
	UT_ASSERT_EQ((long)prov.request_id, 4242L);
	UT_ASSERT_EQ((long)prov.epoch, 9L);
	UT_ASSERT_EQ((long)prov.old_scn, 0x2000L);
	UT_ASSERT_EQ((long)prov.new_scn, 0x3000L);
	UT_ASSERT_EQ((uint64)cluster_pcm_lock_pi_watermark_scn_query(tag), (uint64)0x3000);

	/* A LATE / stale feed must NOT enter the table (step 1b): the query
	 * still returns the ACK_SLOT advance that produced the CURRENT
	 * watermark, and its new_scn equals that watermark. */
	cluster_pcm_lock_pi_watermark_scn_advance(tag, (SCN)0x100, CLUSTER_PCM_WM_SRC_ACK_SLOTLESS, 1,
											  4243, 5);
	UT_ASSERT(cluster_pcm_lock_pi_watermark_prov_query(tag, &prov));
	UT_ASSERT_EQ((long)prov.source, (long)CLUSTER_PCM_WM_SRC_ACK_SLOT);
	UT_ASSERT_EQ((long)prov.sender_node, 3L);
	UT_ASSERT_EQ((long)prov.request_id, 4242L);
	UT_ASSERT_EQ((long)prov.new_scn, 0x3000L);
	UT_ASSERT_EQ((uint64)cluster_pcm_lock_pi_watermark_scn_query(tag), (uint64)0x3000);
	UT_ASSERT_EQ((uint64)prov.new_scn, (uint64)cluster_pcm_lock_pi_watermark_scn_query(tag));

	/* A second tag gets its own slot (open addressing, insert-once). */
	cluster_gcs_block_master_rebuild_from_redeclare(other, (uint8)PCM_STATE_S, (XLogRecPtr)0x1000,
													(SCN)0x5000, 1, 7);
	UT_ASSERT(cluster_pcm_lock_pi_watermark_prov_query(other, &prov));
	UT_ASSERT_EQ((long)prov.source, (long)CLUSTER_PCM_WM_SRC_REDECLARE);
	UT_ASSERT_EQ((long)prov.new_scn, 0x5000L);
	UT_ASSERT(cluster_pcm_lock_pi_watermark_prov_query(tag, &prov));
	UT_ASSERT_EQ((long)prov.new_scn, 0x3000L);
}

UT_TEST(test_pcm_acquire_buffer_local_s_nonholder_registers_s_then_upgrades)
{
	BufferTag tag = make_tag(96);
	BufferDesc buf;
	bool save = cluster_gcs_block_local_cache;
	bool retry_denied = false;

	reset_fake_pcm_runtime(4);
	fake_cssd_dead_node = -1;
	cluster_gcs_block_local_cache = true;
	fake_local_x_upgrade_result = true;

	cluster_node_id = 2;
	cluster_pcm_lock_acquire(tag, PCM_LOCK_MODE_S);
	UT_ASSERT_EQ((int)cluster_pcm_lock_query(tag), (int)PCM_LOCK_MODE_S);

	memset(&buf, 0, sizeof(buf));
	buf.tag = tag;
	buf.pcm_state = (uint8)PCM_STATE_N;
	cluster_node_id = 0;
	UT_ASSERT(cluster_pcm_lock_acquire_buffer(&buf, PCM_LOCK_MODE_X, &retry_denied));
	UT_ASSERT(!retry_denied);
	UT_ASSERT_EQ((int)cluster_pcm_lock_query(tag), (int)PCM_LOCK_MODE_X);
	UT_ASSERT(!cluster_pcm_master_other_live_holder_exists(tag, 0));
	UT_ASSERT(cluster_pcm_master_requester_is_holder(tag, 0, PCM_TRANS_N_TO_X));

	cluster_gcs_block_local_cache = save;
}

/*
 * P0-26 sibling race: the local-master buffer-aware X path observes shared S
 * with no local S bit, then bootstraps a local S declaration before upgrade.
 * A queue handoff may replace that S authority with remote X in between.  The
 * nested acquire must preserve the BufferDesc-aware exact transfer route,
 * never escape through the tag-only legacy terminal.
 */
UT_TEST(test_pcm_acquire_buffer_s_bootstrap_revalidates_remote_x)
{
	BufferTag tag = make_tag(970);
	BufferDesc buf;
	PcmAuthoritySnapshot after;
	bool retry_denied = false;
	bool acquired = false;
	bool escaped_error = false;

	reset_fake_pcm_runtime(4);
	cluster_gcs_block_local_cache = true;
	cluster_node_id = 1;
	cluster_pcm_lock_acquire(tag, PCM_LOCK_MODE_S);

	memset(&buf, 0, sizeof(buf));
	buf.tag = tag;
	buf.pcm_state = (uint8)PCM_STATE_N;
	cluster_node_id = 0;
	fake_local_x_transfer_count = 0;
	fake_local_x_transfer_holder = -1;
	fake_acquire_entry_handoff_tag = tag;
	fake_acquire_entry_handoff_source = 1;
	fake_acquire_entry_handoff_target = 2;
	fake_acquire_entry_handoff_release = PCM_TRANS_S_TO_N_RELEASE;
	cluster_injection_armed_count = 1;
	fake_acquire_entry_handoff_armed = true;

	if (sigsetjmp(ut_ereport_jump, 1) == 0) {
		ut_ereport_jump_armed = true;
		acquired = cluster_pcm_lock_acquire_buffer(&buf, PCM_LOCK_MODE_X, &retry_denied);
		ut_ereport_jump_armed = false;
	} else {
		ut_ereport_jump_armed = false;
		escaped_error = true;
	}

	UT_ASSERT(!escaped_error);
	UT_ASSERT(acquired);
	UT_ASSERT(!retry_denied);
	UT_ASSERT_EQ(fake_local_x_transfer_count, 1);
	UT_ASSERT_EQ(fake_local_x_transfer_holder, 2);
	UT_ASSERT(cluster_pcm_lock_authority_snapshot(tag, &after));
	UT_ASSERT_EQ((int)after.state, (int)PCM_STATE_X);
	UT_ASSERT_EQ(after.x_holder_node, 2);
	UT_ASSERT_EQ(memcmp(&fake_local_x_transfer_expected, &after, sizeof(after)), 0);
}

/* P0-26 third entry: the ordinary buffer-aware S path also has an optimistic
 * remote-X precheck.  A local-X -> remote-X queue handoff between that check
 * and the entry-lock acquire must retain the BufferDesc-aware read-image
 * route; it must not fall through the tag-only legacy terminal. */
UT_TEST(test_pcm_acquire_buffer_s_revalidates_remote_x_after_precheck)
{
	BufferTag tag = make_tag(971);
	BufferDesc buf;
	PcmAuthoritySnapshot after;
	bool retry_denied = false;
	bool acquired = true;
	bool escaped_error = false;

	reset_fake_pcm_runtime(4);
	cluster_gcs_block_local_cache = true;
	cluster_node_id = 0;
	cluster_pcm_lock_acquire(tag, PCM_LOCK_MODE_X);

	memset(&buf, 0, sizeof(buf));
	buf.tag = tag;
	fake_acquire_entry_handoff_tag = tag;
	fake_acquire_entry_handoff_source = 0;
	fake_acquire_entry_handoff_target = 1;
	cluster_injection_armed_count = 1;
	fake_acquire_entry_handoff_armed = true;

	if (sigsetjmp(ut_ereport_jump, 1) == 0) {
		ut_ereport_jump_armed = true;
		acquired = cluster_pcm_lock_acquire_buffer(&buf, PCM_LOCK_MODE_S, &retry_denied);
		ut_ereport_jump_armed = false;
	} else {
		ut_ereport_jump_armed = false;
		escaped_error = true;
	}

	UT_ASSERT(!escaped_error);
	UT_ASSERT(!acquired); /* one-shot READ_IMAGE is intentionally non-durable */
	UT_ASSERT(!retry_denied);
	UT_ASSERT_EQ(fake_local_read_image_count, 1);
	UT_ASSERT_EQ(fake_local_read_image_holder, 1);
	UT_ASSERT(cluster_pcm_lock_authority_snapshot(tag, &after));
	UT_ASSERT_EQ((int)after.state, (int)PCM_STATE_X);
	UT_ASSERT_EQ(after.x_holder_node, 1);
	UT_ASSERT_EQ(memcmp(&fake_local_read_image_expected, &after, sizeof(after)), 0);
}

/* P0-26: the buffer-aware local-master X path used to inspect state/holder,
 * then call the tag-only acquire without carrying an authoritative token.
 * Commit a queue-style local-X -> remote-X handoff at the existing acquire
 * entry injection point, exactly after the optimistic precheck and before the
 * tag-only entry lock.  The buffer-aware caller must redirect to the existing
 * safe X-transfer path; the old code escapes through the legacy
 * "cross-node block write transfer not supported" ERROR instead.
 */
UT_TEST(test_pcm_acquire_buffer_revalidates_remote_x_after_precheck)
{
	BufferTag tag = make_tag(97);
	BufferDesc buf;
	PcmAuthoritySnapshot after;
	bool retry_denied = false;
	bool acquired = false;
	bool escaped_error = false;

	reset_fake_pcm_runtime(4);
	cluster_gcs_block_local_cache = true;
	cluster_node_id = 0;
	cluster_pcm_lock_acquire(tag, PCM_LOCK_MODE_X);

	memset(&buf, 0, sizeof(buf));
	buf.tag = tag;
	fake_local_x_transfer_count = 0;
	fake_local_x_transfer_holder = -1;
	fake_acquire_entry_handoff_tag = tag;
	fake_acquire_entry_handoff_source = 0;
	fake_acquire_entry_handoff_target = 1;
	cluster_injection_armed_count = 1;
	fake_acquire_entry_handoff_armed = true;

	if (sigsetjmp(ut_ereport_jump, 1) == 0) {
		ut_ereport_jump_armed = true;
		acquired = cluster_pcm_lock_acquire_buffer(&buf, PCM_LOCK_MODE_X, &retry_denied);
		ut_ereport_jump_armed = false;
	} else {
		ut_ereport_jump_armed = false;
		escaped_error = true;
	}

	UT_ASSERT(!escaped_error);
	UT_ASSERT(acquired);
	UT_ASSERT(!retry_denied);
	UT_ASSERT_EQ(fake_local_x_transfer_count, 1);
	UT_ASSERT_EQ(fake_local_x_transfer_holder, 1);
	UT_ASSERT(cluster_pcm_lock_authority_snapshot(tag, &after));
	UT_ASSERT_EQ((int)after.state, (int)PCM_STATE_X);
	UT_ASSERT_EQ(after.x_holder_node, 1);
	UT_ASSERT_EQ((long)after.s_holders_bitmap, 0L);
	UT_ASSERT_EQ(memcmp(&fake_local_x_transfer_expected, &after, sizeof(after)), 0);
}

UT_TEST(test_pcm_acquire_buffer_routes_unchanged_remote_x_with_exact_authority)
{
	BufferTag tag = make_tag(98);
	BufferDesc buf;
	PcmAuthoritySnapshot before;
	PcmAuthoritySnapshot after;
	bool retry_denied = false;

	reset_fake_pcm_runtime(4);
	cluster_gcs_block_local_cache = true;
	cluster_node_id = 0;
	cluster_pcm_lock_acquire(tag, PCM_LOCK_MODE_X);
	UT_ASSERT_EQ((int)cluster_pcm_lock_apply_gcs_transition(tag, PCM_TRANS_X_TO_N_RELEASE, 0), 1);
	UT_ASSERT_EQ((int)cluster_pcm_lock_apply_gcs_transition(tag, PCM_TRANS_N_TO_X, 1), 1);
	UT_ASSERT(cluster_pcm_lock_authority_snapshot(tag, &before));

	memset(&buf, 0, sizeof(buf));
	buf.tag = tag;
	fake_local_x_transfer_count = 0;
	fake_local_x_transfer_holder = -1;

	UT_ASSERT(cluster_pcm_lock_acquire_buffer(&buf, PCM_LOCK_MODE_X, &retry_denied));
	UT_ASSERT(!retry_denied);
	UT_ASSERT_EQ(fake_local_x_transfer_count, 1);
	UT_ASSERT_EQ(fake_local_x_transfer_holder, 1);
	UT_ASSERT_EQ(memcmp(&fake_local_x_transfer_expected, &before, sizeof(before)), 0);
	UT_ASSERT(cluster_pcm_lock_authority_snapshot(tag, &after));
	UT_ASSERT_EQ(memcmp(&after, &before, sizeof(before)), 0);
}

UT_TEST(test_pcm_x_transfer_commit_is_exact_and_late_reply_safe)
{
	BufferTag tag = make_tag(99);
	PcmAuthoritySnapshot expected;
	PcmAuthoritySnapshot stale;
	PcmAuthoritySnapshot after;
	PcmAuthoritySnapshot committed;

	reset_fake_pcm_runtime(4);
	cluster_node_id = 0;
	cluster_pcm_lock_acquire(tag, PCM_LOCK_MODE_X);
	UT_ASSERT_EQ((int)cluster_pcm_lock_apply_gcs_transition(tag, PCM_TRANS_X_TO_N_RELEASE, 0), 1);
	UT_ASSERT_EQ((int)cluster_pcm_lock_apply_gcs_transition(tag, PCM_TRANS_N_TO_X, 1), 1);
	UT_ASSERT(cluster_pcm_lock_authority_snapshot(tag, &expected));
	UT_ASSERT(cluster_pcm_lock_authority_matches(tag, &expected));

	stale = expected;
	stale.transition_count++;
	UT_ASSERT_EQ(cluster_pcm_lock_master_take_x_after_transfer(tag, &stale, (XLogRecPtr)0x1234,
															   InvalidScn, 1, 44, 900, 12),
				 PCM_X_TRANSFER_COMMIT_STALE);
	stale = expected;
	stale.master_holder.request_id++;
	UT_ASSERT_EQ(cluster_pcm_lock_master_take_x_after_transfer(tag, &stale, (XLogRecPtr)0x1234,
															   InvalidScn, 1, 44, 900, 12),
				 PCM_X_TRANSFER_COMMIT_STALE);
	stale = expected;
	stale.master_holder.cluster_epoch++;
	UT_ASSERT_EQ(cluster_pcm_lock_master_take_x_after_transfer(tag, &stale, (XLogRecPtr)0x1234,
															   InvalidScn, 1, 44, 900, 12),
				 PCM_X_TRANSFER_COMMIT_STALE);
	UT_ASSERT(cluster_pcm_lock_authority_snapshot(tag, &after));
	UT_ASSERT_EQ(memcmp(&after, &expected, sizeof(expected)), 0);

	UT_ASSERT_EQ(cluster_pcm_lock_master_take_x_after_transfer(tag, &expected, (XLogRecPtr)0x1234,
															   InvalidScn, 1, 44, 900, 12),
				 PCM_X_TRANSFER_COMMIT_OK);
	UT_ASSERT(cluster_pcm_lock_authority_snapshot(tag, &committed));
	UT_ASSERT_EQ((int)committed.state, (int)PCM_STATE_X);
	UT_ASSERT_EQ(committed.x_holder_node, 0);
	UT_ASSERT_EQ((long)committed.s_holders_bitmap, 0L);
	UT_ASSERT_EQ(committed.pending_x_requester_node, -1);
	UT_ASSERT_EQ((long)committed.master_holder.node_id, 0L);
	UT_ASSERT_EQ((long)committed.master_holder.procno, 44L);
	UT_ASSERT_EQ((uint64)committed.master_holder.cluster_epoch, (uint64)12);
	UT_ASSERT_EQ((uint64)committed.master_holder.request_id, (uint64)900);
	UT_ASSERT_EQ((uint64)committed.transition_count, (uint64)expected.transition_count + 1);
	UT_ASSERT_EQ(committed.authority_generation, expected.authority_generation + 1);

	/* A duplicate/late reply carries the displaced remote-X token. */
	UT_ASSERT_EQ(cluster_pcm_lock_master_take_x_after_transfer(tag, &expected, (XLogRecPtr)0x1234,
															   InvalidScn, 1, 44, 900, 12),
				 PCM_X_TRANSFER_COMMIT_STALE);
	UT_ASSERT(cluster_pcm_lock_authority_snapshot(tag, &after));
	UT_ASSERT_EQ(memcmp(&after, &committed, sizeof(committed)), 0);
}


UT_TEST(test_pcm_dead_node_cleanup_drops_holder_records)
{
	BufferTag stag = make_tag(94);
	BufferTag xtag = make_tag(95);
	uint32 s_bitmap;

	reset_fake_pcm_runtime(4);
	fake_cssd_dead_node = -1;

	/* Dead node 2 was the first S holder, so master_holder points at it. */
	cluster_node_id = 2;
	cluster_pcm_lock_acquire(stag, PCM_LOCK_MODE_S);
	cluster_node_id = 1;
	cluster_pcm_lock_acquire(stag, PCM_LOCK_MODE_S);
	UT_ASSERT_EQ(cluster_pcm_master_holder_node_by_tag(stag), 2);
	UT_ASSERT(!cluster_pcm_lock_clean_leave_verify_no_leftover(2));

	cluster_node_id = 2;
	cluster_pcm_lock_acquire(xtag, PCM_LOCK_MODE_X);
	UT_ASSERT_EQ((int)cluster_pcm_lock_query(xtag), (int)PCM_LOCK_MODE_X);

	UT_ASSERT_EQ((uint64)cluster_pcm_lock_cleanup_on_node_dead(2), (uint64)2);

	s_bitmap = cluster_pcm_lock_query_s_holders_bitmap(stag);
	UT_ASSERT_EQ((int)(s_bitmap & (1u << 2)), 0);
	UT_ASSERT((s_bitmap & (1u << 1)) != 0);
	UT_ASSERT_EQ((int)cluster_pcm_lock_query(stag), (int)PCM_LOCK_MODE_S);
	UT_ASSERT_EQ(cluster_pcm_master_holder_node_by_tag(stag), 1);

	UT_ASSERT_EQ((int)cluster_pcm_lock_query(xtag), (int)PCM_LOCK_MODE_N);
	UT_ASSERT(cluster_pcm_lock_clean_leave_verify_no_leftover(2));
	UT_ASSERT((fake_cv_broadcast_count) >= (1));

	/* Idempotent: a repeated dead-sweep pass has nothing left to clean. */
	UT_ASSERT_EQ((uint64)cluster_pcm_lock_cleanup_on_node_dead(2), (uint64)0);

	/* spec-4.6a D12 (r2-P1-3) pending-X form: a dead requester's parked X
	 * intent is cleared by the companion HC124 sweep the same dead-sweep hook
	 * drives, and the sweep is idempotent too. */
	cluster_pcm_lock_set_pending_x(stag, 2, 1234);
	UT_ASSERT_EQ(cluster_pcm_lock_query_pending_x_requester(stag), 2);
	UT_ASSERT_EQ((uint64)cluster_pcm_lock_clear_pending_x_for_node(2), (uint64)1);
	UT_ASSERT_EQ(cluster_pcm_lock_query_pending_x_requester(stag), -1);
	UT_ASSERT_EQ((uint64)cluster_pcm_lock_clear_pending_x_for_node(2), (uint64)0);

	/* GCS-race round-2 additional hardening: identity-safe compare-and-
	 * clear.  A mismatched identity must NOT wipe another requester's
	 * pending-X (the starvation guard a newer writer relies on); the
	 * matching identity clears exactly once. */
	cluster_pcm_lock_set_pending_x(stag, 2, 1234);
	UT_ASSERT_EQ(cluster_pcm_lock_clear_pending_x_if(stag, 3), false);
	UT_ASSERT_EQ(cluster_pcm_lock_query_pending_x_requester(stag), 2);
	UT_ASSERT_EQ(cluster_pcm_lock_clear_pending_x_if(stag, 2), true);
	UT_ASSERT_EQ(cluster_pcm_lock_query_pending_x_requester(stag), -1);
	UT_ASSERT_EQ(cluster_pcm_lock_clear_pending_x_if(stag, 2), false);
}

static PcmXGrdHandoffToken
make_pcm_x_grd_handoff_token(BufferTag tag, const PcmAuthoritySnapshot *authority,
							 int32 source_node, int32 requester_node, uint32 requester_procno,
							 uint64 request_id)
{
	PcmXGrdHandoffToken token;

	memset(&token, 0, sizeof(token));
	token.tag = tag;
	token.authority = *authority;
	token.cluster_epoch = 17;
	token.request_id = request_id;
	token.ticket_id = request_id;
	token.grant_generation = 23;
	UT_ASSERT(cluster_pcm_x_image_id_encode(0, 29, &token.image_id));
	token.source_own_generation = 31;
	token.page_scn = 0x4000;
	token.page_lsn = 0x5000;
	token.requester_node = requester_node;
	token.source_node = source_node;
	token.requester_procno = requester_procno;
	token.page_checksum = 37;
	return token;
}

static char *
read_gcs_block_source(void)
{
	FILE *file;
	char *source;
	long length;

	file = fopen(GCS_BLOCK_SOURCE_PATH, "rb");
	UT_ASSERT_NOT_NULL(file);
	if (file == NULL)
		return NULL;
	UT_ASSERT_EQ(fseek(file, 0, SEEK_END), 0);
	length = ftell(file);
	UT_ASSERT(length > 0);
	UT_ASSERT_EQ(fseek(file, 0, SEEK_SET), 0);
	source = malloc((size_t)length + 1);
	UT_ASSERT_NOT_NULL(source);
	if (source == NULL) {
		fclose(file);
		return NULL;
	}
	UT_ASSERT_EQ(fread(source, 1, (size_t)length, file), (size_t)length);
	source[length] = '\0';
	fclose(file);
	return source;
}

UT_TEST(test_pcm_authority_snapshot_is_one_entry_lock_view)
{
	BufferTag tag = make_tag(97);
	PcmAuthoritySnapshot snapshot;

	reset_fake_pcm_runtime(4);
	cluster_node_id = 1;
	cluster_pcm_lock_acquire(tag, PCM_LOCK_MODE_S);
	cluster_node_id = 2;
	cluster_pcm_lock_acquire(tag, PCM_LOCK_MODE_S);
	cluster_pcm_lock_set_pending_x(tag, 3, 0x1234);

	memset(&snapshot, 0x7f, sizeof(snapshot));
	UT_ASSERT(cluster_pcm_lock_authority_snapshot(tag, &snapshot));
	UT_ASSERT_EQ((int)snapshot.state, (int)PCM_STATE_S);
	UT_ASSERT_EQ(snapshot.x_holder_node, -1);
	UT_ASSERT_EQ(snapshot.s_holders_bitmap, (uint32)((1u << 1) | (1u << 2)));
	UT_ASSERT_EQ(snapshot.master_holder.node_id, (uint32)1);
	UT_ASSERT_EQ(snapshot.pending_x_requester_node, 3);
	UT_ASSERT_EQ(snapshot.pending_x_since_lsn, (uint64)0x1234);
	UT_ASSERT(snapshot.transition_count > 0);
}

UT_TEST(test_pcm_queue_pending_x_reservation_never_overwrites_another_node)
{
	BufferTag tag = make_tag(101);
	PcmAuthoritySnapshot snapshot;

	reset_fake_pcm_runtime(4);
	cluster_node_id = 1;
	/* Both producers lazily create the canonical N authority for a cold tag. */
	UT_ASSERT_EQ(cluster_pcm_lock_try_reserve_pending_x(tag, 2, 0x1111), PCM_PENDING_X_RESERVE_OK);
	UT_ASSERT(cluster_pcm_lock_authority_snapshot(tag, &snapshot));
	UT_ASSERT_EQ((int)snapshot.state, (int)PCM_STATE_N);
	UT_ASSERT(cluster_pcm_lock_queue_pending_x_exact(tag, 2, 0x1111));
	/* A delayed same-node legacy clear cannot erase a queue-kind claim. */
	UT_ASSERT(!cluster_pcm_lock_clear_pending_x_if(tag, 2));
	UT_ASSERT(cluster_pcm_lock_queue_pending_x_exact(tag, 2, 0x1111));
	UT_ASSERT(cluster_pcm_lock_clear_queue_pending_x_exact(tag, 2, 0x1111));

	cluster_pcm_lock_acquire(tag, PCM_LOCK_MODE_S);

	UT_ASSERT_EQ(cluster_pcm_lock_try_reserve_pending_x(tag, 2, 0x1111), PCM_PENDING_X_RESERVE_OK);
	UT_ASSERT(cluster_pcm_lock_authority_snapshot(tag, &snapshot));
	UT_ASSERT_EQ(snapshot.pending_x_requester_node, 2);
	UT_ASSERT(cluster_pcm_lock_queue_pending_x_exact(tag, 2, 0x1111));

	/* Legacy producers obey the same idle-only rule, including same-node. */
	UT_ASSERT_EQ(cluster_pcm_lock_set_pending_x(tag, 3, 0x1212), PCM_PENDING_X_RESERVE_OCCUPIED);
	UT_ASSERT_EQ(cluster_pcm_lock_set_pending_x(tag, 2, 0x1313), PCM_PENDING_X_RESERVE_OCCUPIED);
	UT_ASSERT(cluster_pcm_lock_authority_snapshot(tag, &snapshot));
	UT_ASSERT_EQ(snapshot.pending_x_requester_node, 2);
	UT_ASSERT(cluster_pcm_lock_queue_pending_x_exact(tag, 2, 0x1111));

	/* A different queue head cannot overwrite the live barrier. */
	UT_ASSERT_EQ(cluster_pcm_lock_try_reserve_pending_x(tag, 3, 0x2222),
				 PCM_PENDING_X_RESERVE_OCCUPIED);
	UT_ASSERT(cluster_pcm_lock_authority_snapshot(tag, &snapshot));
	UT_ASSERT_EQ(snapshot.pending_x_requester_node, 2);
	UT_ASSERT(cluster_pcm_lock_queue_pending_x_exact(tag, 2, 0x1111));

	/* A same-node legacy round is not ticket-exact replay proof. */
	UT_ASSERT_EQ(cluster_pcm_lock_try_reserve_pending_x(tag, 2, 0x3333),
				 PCM_PENDING_X_RESERVE_OCCUPIED);
	UT_ASSERT(cluster_pcm_lock_authority_snapshot(tag, &snapshot));
	UT_ASSERT_EQ(snapshot.pending_x_requester_node, 2);
	UT_ASSERT(cluster_pcm_lock_queue_pending_x_exact(tag, 2, 0x1111));

	UT_ASSERT(cluster_pcm_lock_clear_queue_pending_x_exact(tag, 2, 0x1111));
	UT_ASSERT_EQ(cluster_pcm_lock_try_reserve_pending_x(tag, 3, 0x4444), PCM_PENDING_X_RESERVE_OK);
	UT_ASSERT(cluster_pcm_lock_authority_snapshot(tag, &snapshot));
	UT_ASSERT_EQ(snapshot.pending_x_requester_node, 3);
	UT_ASSERT(cluster_pcm_lock_queue_pending_x_exact(tag, 3, 0x4444));
	/* A replay of the old release cannot erase the successor cookie. */
	UT_ASSERT(!cluster_pcm_lock_clear_queue_pending_x_exact(tag, 2, 0x1111));
	UT_ASSERT(cluster_pcm_lock_queue_pending_x_exact(tag, 3, 0x4444));
	UT_ASSERT(cluster_pcm_lock_clear_queue_pending_x_exact(tag, 3, 0x4444));

	tag = make_tag(102);
	UT_ASSERT_EQ(cluster_pcm_lock_set_pending_x(tag, 2, 0x5555), PCM_PENDING_X_RESERVE_OK);
	UT_ASSERT(cluster_pcm_lock_authority_snapshot(tag, &snapshot));
	UT_ASSERT_EQ((int)snapshot.state, (int)PCM_STATE_N);
	UT_ASSERT(cluster_pcm_lock_clear_pending_x_if(tag, 2));

	reset_fake_pcm_runtime(1);
	UT_ASSERT_EQ(cluster_pcm_lock_set_pending_x(make_tag(103), 2, 0x6666),
				 PCM_PENDING_X_RESERVE_OK);
	UT_ASSERT_EQ(cluster_pcm_lock_try_reserve_pending_x(make_tag(104), 3, 0x7777),
				 PCM_PENDING_X_RESERVE_NO_CAPACITY);
}

/* P0-25: pending-X is an admission barrier, not only an advisory wire gate.
 * The final decision and the S-holder bitmap mutation share entry_lock, so an
 * N->S request that raced past an earlier handler preflight cannot publish a
 * new holder after the queue writer claimed the tag.  An already-recorded S
 * holder may re-enter without changing the authority bytes. */
UT_TEST(test_pcm_pending_x_blocks_new_remote_s_holder_atomically)
{
	BufferTag tag = make_tag(110);
	PcmAuthoritySnapshot before;
	PcmAuthoritySnapshot after;

	reset_fake_pcm_runtime(4);
	UT_ASSERT_EQ(cluster_pcm_lock_try_reserve_pending_x(tag, 3, 9010), PCM_PENDING_X_RESERVE_OK);
	UT_ASSERT(cluster_pcm_lock_authority_snapshot(tag, &before));
	UT_ASSERT_EQ((int)before.state, (int)PCM_STATE_N);
	UT_ASSERT_EQ(cluster_pcm_lock_apply_gcs_transition_result(tag, PCM_TRANS_N_TO_S, 1),
				 PCM_GCS_TRANSITION_PENDING_X);
	UT_ASSERT(cluster_pcm_lock_authority_snapshot(tag, &after));
	UT_ASSERT_EQ((int)after.state, (int)PCM_STATE_N);
	UT_ASSERT_EQ(after.s_holders_bitmap, (uint32)0);
	UT_ASSERT_EQ(after.transition_count, before.transition_count);

	UT_ASSERT(cluster_pcm_lock_clear_queue_pending_x_exact(tag, 3, 9010));
	UT_ASSERT(cluster_pcm_lock_apply_gcs_transition(tag, PCM_TRANS_N_TO_S, 1));
	UT_ASSERT_EQ(cluster_pcm_lock_query_s_holders_bitmap(tag), (uint32)(1u << 1));

	UT_ASSERT_EQ(cluster_pcm_lock_try_reserve_pending_x(tag, 3, 9011), PCM_PENDING_X_RESERVE_OK);
	UT_ASSERT(cluster_pcm_lock_authority_snapshot(tag, &before));
	UT_ASSERT(cluster_pcm_lock_apply_gcs_transition(tag, PCM_TRANS_N_TO_S, 1));
	UT_ASSERT_EQ(cluster_pcm_lock_apply_gcs_transition_result(tag, PCM_TRANS_N_TO_S, 2),
				 PCM_GCS_TRANSITION_PENDING_X);
	UT_ASSERT(cluster_pcm_lock_authority_snapshot(tag, &after));
	UT_ASSERT_EQ(after.s_holders_bitmap, before.s_holders_bitmap);
	UT_ASSERT_EQ(after.transition_count, before.transition_count);

	UT_ASSERT(cluster_pcm_lock_clear_queue_pending_x_exact(tag, 3, 9011));
	UT_ASSERT(cluster_pcm_lock_apply_gcs_transition(tag, PCM_TRANS_N_TO_S, 2));
	UT_ASSERT_EQ(cluster_pcm_lock_query_s_holders_bitmap(tag), (uint32)((1u << 1) | (1u << 2)));
}

/* P0-25 local-master mirror: an existing local S holder re-enters immediately,
 * while a different local node waits until the queue cookie is cleared.  The
 * CV callback deterministically performs that clear in this single-threaded
 * harness, proving both no pre-clear bitmap publication and post-clear liveness. */
UT_TEST(test_pcm_pending_x_blocks_new_local_s_holder_until_clear)
{
	BufferTag tag = make_tag(111);

	reset_fake_pcm_runtime(4);
	cluster_node_id = 0;
	cluster_pcm_lock_acquire(tag, PCM_LOCK_MODE_S);
	UT_ASSERT_EQ(cluster_pcm_lock_try_reserve_pending_x(tag, 3, 9012), PCM_PENDING_X_RESERVE_OK);

	cluster_pcm_lock_acquire(tag, PCM_LOCK_MODE_S);
	UT_ASSERT_EQ(fake_cv_sleep_count, 0);
	UT_ASSERT_EQ(cluster_pcm_lock_query_s_holders_bitmap(tag), (uint32)(1u << 0));

	fake_cv_wake_pending_x_clear.tag = tag;
	fake_cv_wake_pending_x_clear.requester_node = 3;
	fake_cv_wake_pending_x_clear.ticket_id = 9012;
	fake_cv_wake_pending_x_clear.armed = true;
	cluster_node_id = 1;
	cluster_pcm_lock_acquire(tag, PCM_LOCK_MODE_S);
	UT_ASSERT_EQ(fake_cv_sleep_count, 1);
	UT_ASSERT_EQ((int)fake_cv_sleep_wait_event, (int)WAIT_EVENT_PCM_COMPATIBLE_STATE_WAIT);
	UT_ASSERT_EQ(cluster_pcm_lock_query_pending_x_requester(tag), -1);
	UT_ASSERT_EQ(cluster_pcm_lock_query_s_holders_bitmap(tag), (uint32)((1u << 0) | (1u << 1)));

	/* A reader woken by FINAL may observe the newly granted X and sleep again.
	 * The later holder X->S downgrade must wake it when S becomes compatible. */
	tag = make_tag(112);
	reset_fake_pcm_runtime(4);
	cluster_node_id = 0;
	cluster_pcm_lock_acquire(tag, PCM_LOCK_MODE_X);
	UT_ASSERT(cluster_pcm_lock_apply_gcs_transition(tag, PCM_TRANS_X_TO_S_DOWNGRADE, 0));
	UT_ASSERT_EQ(fake_cv_broadcast_count, 1);
}

UT_TEST(test_pcm_queue_handoff_x_exact_rejects_authority_drift)
{
	BufferTag tag = make_tag(98);
	PcmAuthoritySnapshot before;
	PcmAuthoritySnapshot after;
	PcmXGrdHandoffToken token;

	reset_fake_pcm_runtime(4);
	cluster_node_id = 1;
	cluster_pcm_lock_acquire(tag, PCM_LOCK_MODE_S);
	UT_ASSERT_EQ(cluster_pcm_lock_try_reserve_pending_x(tag, 3, 9001), PCM_PENDING_X_RESERVE_OK);
	UT_ASSERT(cluster_pcm_lock_authority_snapshot(tag, &before));
	token = make_pcm_x_grd_handoff_token(tag, &before, 1, 3, 41, 9001);

	/* A holder release remains legal under pending-X and still invalidates the
	 * optimistic authority token; a new S-holder admission is now prohibited. */
	cluster_pcm_lock_release(tag);
	UT_ASSERT_EQ(cluster_pcm_lock_queue_handoff_x_exact(&token), PCM_X_GRD_HANDOFF_STALE);
	UT_ASSERT(cluster_pcm_lock_authority_snapshot(tag, &after));
	UT_ASSERT_EQ((int)after.state, (int)PCM_STATE_N);
	UT_ASSERT_EQ(after.s_holders_bitmap, (uint32)0);
}

UT_TEST(test_pcm_queue_handoff_x_exact_rejects_residual_s_holder)
{
	BufferTag tag = make_tag(99);
	PcmAuthoritySnapshot before;
	PcmXGrdHandoffToken token;

	reset_fake_pcm_runtime(4);
	cluster_node_id = 1;
	cluster_pcm_lock_acquire(tag, PCM_LOCK_MODE_S);
	cluster_node_id = 2;
	cluster_pcm_lock_acquire(tag, PCM_LOCK_MODE_S);
	UT_ASSERT_EQ(cluster_pcm_lock_try_reserve_pending_x(tag, 3, 9002), PCM_PENDING_X_RESERVE_OK);
	UT_ASSERT(cluster_pcm_lock_authority_snapshot(tag, &before));
	token = make_pcm_x_grd_handoff_token(tag, &before, 1, 3, 42, 9002);

	UT_ASSERT_EQ(cluster_pcm_lock_queue_handoff_x_exact(&token), PCM_X_GRD_HANDOFF_BAD_STATE);
	UT_ASSERT_EQ((int)cluster_pcm_lock_query(tag), (int)PCM_STATE_S);
}

UT_TEST(test_pcm_queue_handoff_x_exact_commits_full_identity_and_replays)
{
	BufferTag tag = make_tag(100);
	PcmAuthoritySnapshot before;
	PcmAuthoritySnapshot after;
	PcmXGrdHandoffToken token;

	reset_fake_pcm_runtime(4);
	cluster_node_id = 1;
	cluster_pcm_lock_acquire(tag, PCM_LOCK_MODE_S);
	UT_ASSERT_EQ(cluster_pcm_lock_try_reserve_pending_x(tag, 3, 9003), PCM_PENDING_X_RESERVE_OK);
	UT_ASSERT(cluster_pcm_lock_authority_snapshot(tag, &before));
	token = make_pcm_x_grd_handoff_token(tag, &before, 1, 3, 43, 9003);
	token.cluster_epoch = 0;
	token.source_own_generation = 0;
	{
		PcmXGrdHandoffToken wrong_ticket = token;

		wrong_ticket.ticket_id++;
		UT_ASSERT_EQ(cluster_pcm_lock_queue_handoff_x_exact(&wrong_ticket),
					 PCM_X_GRD_HANDOFF_BAD_STATE);
		UT_ASSERT_EQ((int)cluster_pcm_lock_query(tag), (int)PCM_STATE_S);
	}

	UT_ASSERT_EQ(cluster_pcm_lock_queue_handoff_x_exact(&token), PCM_X_GRD_HANDOFF_OK);
	UT_ASSERT(cluster_pcm_lock_authority_snapshot(tag, &after));
	UT_ASSERT_EQ((int)after.state, (int)PCM_STATE_X);
	UT_ASSERT_EQ(after.x_holder_node, 3);
	UT_ASSERT_EQ(after.s_holders_bitmap, (uint32)0);
	UT_ASSERT_EQ(after.pending_x_requester_node, -1);
	UT_ASSERT_EQ(after.master_holder.node_id, (uint32)3);
	UT_ASSERT_EQ(after.master_holder.procno, (uint32)43);
	UT_ASSERT_EQ(after.master_holder.cluster_epoch, (uint64)0);
	UT_ASSERT_EQ(after.master_holder.request_id, (uint64)9003);
	UT_ASSERT_EQ(after.transition_count, before.transition_count + 1);
	UT_ASSERT_EQ(after.authority_generation, before.authority_generation + 1);
	/* FINAL preserves page_lsn in the immutable A-record only.  It must not
	 * promote one node's WAL position into the cross-node GRD version floor. */
	UT_ASSERT_EQ((uint64)cluster_pcm_lock_pi_watermark_lsn_query(tag), (uint64)InvalidXLogRecPtr);
	UT_ASSERT_EQ((uint64)cluster_pcm_lock_pi_watermark_scn_query(tag), (uint64)0x4000);

	/* SCN-only authority is a legal shape.  Exact replay must not depend on a
	 * same-stream LSN floor that the GRD cannot establish across nodes. */
	cluster_pcm_lock_pi_watermark_retire_for_tag(tag);
	cluster_pcm_lock_pi_watermark_scn_advance(tag, token.page_scn, CLUSTER_PCM_WM_SRC_REDECLARE, 1,
											  7000, 17);
	UT_ASSERT_EQ((uint64)cluster_pcm_lock_pi_watermark_lsn_query(tag), (uint64)InvalidXLogRecPtr);
	UT_ASSERT_EQ(cluster_pcm_lock_queue_handoff_x_exact(&token), PCM_X_GRD_HANDOFF_DUPLICATE);
	UT_ASSERT(cluster_pcm_lock_authority_snapshot(tag, &before));
	UT_ASSERT_EQ(before.authority_generation, after.authority_generation);
}

UT_TEST(test_pcm_queue_handoff_x_exact_accepts_global_n_with_real_image)
{
	BufferTag tag = make_tag(105);
	PcmAuthoritySnapshot before;
	PcmAuthoritySnapshot after;
	PcmXGrdHandoffToken token;

	reset_fake_pcm_runtime(4);
	UT_ASSERT_EQ(cluster_pcm_lock_try_reserve_pending_x(tag, 3, 9004), PCM_PENDING_X_RESERVE_OK);
	cluster_pcm_lock_pi_watermark_lsn_advance(tag, (XLogRecPtr)0x4000);
	cluster_pcm_lock_pi_watermark_scn_advance(tag, (SCN)0x3000, CLUSTER_PCM_WM_SRC_REDECLARE, 1,
											  7001, 17);
	UT_ASSERT(cluster_pcm_lock_authority_snapshot(tag, &before));
	UT_ASSERT_EQ((int)before.state, (int)PCM_STATE_N);
	UT_ASSERT_EQ(before.master_holder.node_id, UINT32_MAX);
	token = make_pcm_x_grd_handoff_token(tag, &before, 3, 3, 44, 9004);

	UT_ASSERT(token.image_id != 0);
	{
		PcmXGrdHandoffToken malformed = token;

		malformed.source_node = 2;
		UT_ASSERT_EQ(cluster_pcm_lock_queue_handoff_x_exact(&malformed),
					 PCM_X_GRD_HANDOFF_BAD_STATE);
		malformed = token;
		malformed.ticket_id++;
		UT_ASSERT_EQ(cluster_pcm_lock_queue_handoff_x_exact(&malformed),
					 PCM_X_GRD_HANDOFF_BAD_STATE);
		malformed = token;
		malformed.image_id = 29;
		UT_ASSERT_EQ(cluster_pcm_lock_queue_handoff_x_exact(&malformed), PCM_X_GRD_HANDOFF_INVALID);
	}
	UT_ASSERT_EQ(cluster_pcm_lock_queue_handoff_x_exact(&token), PCM_X_GRD_HANDOFF_OK);
	UT_ASSERT(cluster_pcm_lock_authority_snapshot(tag, &after));
	UT_ASSERT_EQ((int)after.state, (int)PCM_STATE_X);
	UT_ASSERT_EQ(after.x_holder_node, 3);
	UT_ASSERT_EQ(after.s_holders_bitmap, (uint32)0);
	UT_ASSERT_EQ(after.pending_x_requester_node, -1);
	UT_ASSERT_EQ(after.master_holder.node_id, (uint32)3);
	UT_ASSERT_EQ(after.master_holder.procno, (uint32)44);
	UT_ASSERT_EQ(after.transition_count, before.transition_count + 1);
	UT_ASSERT_EQ((uint64)cluster_pcm_lock_pi_watermark_lsn_query(tag), (uint64)0x4000);
}

UT_TEST(test_pcm_queue_handoff_x_exact_accepts_ordered_self_x)
{
	BufferTag tag = make_tag(106);
	PcmAuthoritySnapshot before;
	PcmAuthoritySnapshot after;
	PcmXGrdHandoffToken token;

	reset_fake_pcm_runtime(4);
	cluster_node_id = 2;
	cluster_pcm_lock_acquire(tag, PCM_LOCK_MODE_X);
	UT_ASSERT_EQ(cluster_pcm_lock_try_reserve_pending_x(tag, 2, 9005), PCM_PENDING_X_RESERVE_OK);
	UT_ASSERT(cluster_pcm_lock_authority_snapshot(tag, &before));
	UT_ASSERT_EQ((int)before.state, (int)PCM_STATE_X);
	UT_ASSERT_EQ(before.x_holder_node, 2);
	token = make_pcm_x_grd_handoff_token(tag, &before, 2, 2, 45, 9005);

	UT_ASSERT(token.image_id != 0);
	UT_ASSERT_EQ(cluster_pcm_lock_queue_handoff_x_exact(&token), PCM_X_GRD_HANDOFF_OK);
	UT_ASSERT(cluster_pcm_lock_authority_snapshot(tag, &after));
	UT_ASSERT_EQ((int)after.state, (int)PCM_STATE_X);
	UT_ASSERT_EQ(after.x_holder_node, 2);
	UT_ASSERT_EQ(after.pending_x_requester_node, -1);
	UT_ASSERT_EQ(after.master_holder.procno, (uint32)45);
	UT_ASSERT_EQ(after.transition_count, before.transition_count + 1);
}

UT_TEST(test_pcm_queue_handoff_x_exact_uses_scn_not_cross_stream_lsn)
{
	BufferTag tag = make_tag(107);
	PcmAuthoritySnapshot before;
	PcmAuthoritySnapshot after;
	PcmXGrdHandoffToken token;

	reset_fake_pcm_runtime(4);
	cluster_node_id = 3;
	cluster_pcm_lock_acquire(tag, PCM_LOCK_MODE_X);
	UT_ASSERT_EQ(cluster_pcm_lock_try_reserve_pending_x(tag, 3, 9006), PCM_PENDING_X_RESERVE_OK);
	UT_ASSERT(cluster_pcm_lock_authority_snapshot(tag, &before));
	token = make_pcm_x_grd_handoff_token(tag, &before, 3, 3, 46, 9006);

	/* Per-node WAL streams make page_lsn incomparable across holders.  A
	 * numerically larger historical LSN must not reject an image whose Lamport
	 * page SCN is newer; page_lsn remains A-record evidence, not GRD version
	 * authority. */
	cluster_pcm_lock_pi_watermark_lsn_advance(tag, (XLogRecPtr)(token.page_lsn + 1));
	cluster_pcm_lock_pi_watermark_scn_advance(tag, (SCN)(token.page_scn - 1),
											  CLUSTER_PCM_WM_SRC_REDECLARE, 1, 7002, 17);
	UT_ASSERT_EQ(cluster_pcm_lock_queue_handoff_x_exact(&token), PCM_X_GRD_HANDOFF_OK);
	UT_ASSERT(cluster_pcm_lock_authority_snapshot(tag, &after));
	UT_ASSERT_EQ((int)after.state, (int)PCM_STATE_X);
	UT_ASSERT_EQ(after.pending_x_requester_node, -1);
	UT_ASSERT_EQ((uint64)cluster_pcm_lock_pi_watermark_lsn_query(tag),
				 (uint64)(token.page_lsn + 1));
	UT_ASSERT_EQ((uint64)cluster_pcm_lock_pi_watermark_scn_query(tag), (uint64)token.page_scn);

	tag = make_tag(108);
	reset_fake_pcm_runtime(4);
	cluster_node_id = 3;
	cluster_pcm_lock_acquire(tag, PCM_LOCK_MODE_X);
	UT_ASSERT_EQ(cluster_pcm_lock_try_reserve_pending_x(tag, 3, 9007), PCM_PENDING_X_RESERVE_OK);
	UT_ASSERT(cluster_pcm_lock_authority_snapshot(tag, &before));
	token = make_pcm_x_grd_handoff_token(tag, &before, 3, 3, 47, 9007);
	cluster_pcm_lock_pi_watermark_scn_advance(tag, (SCN)(token.page_scn + 1),
											  CLUSTER_PCM_WM_SRC_REDECLARE, 1, 7003, 17);
	token.page_scn = (SCN)0;
	UT_ASSERT_EQ(cluster_pcm_lock_queue_handoff_x_exact(&token), PCM_X_GRD_HANDOFF_BAD_STATE);
	UT_ASSERT(cluster_pcm_lock_authority_snapshot(tag, &after));
	UT_ASSERT_EQ((int)after.state, (int)PCM_STATE_X);
	UT_ASSERT_EQ(after.pending_x_requester_node, 3);

	tag = make_tag(109);
	reset_fake_pcm_runtime(4);
	cluster_node_id = 3;
	cluster_pcm_lock_acquire(tag, PCM_LOCK_MODE_X);
	UT_ASSERT_EQ(cluster_pcm_lock_try_reserve_pending_x(tag, 3, 9008), PCM_PENDING_X_RESERVE_OK);
	UT_ASSERT(cluster_pcm_lock_authority_snapshot(tag, &before));
	token = make_pcm_x_grd_handoff_token(tag, &before, 3, 3, 48, 9008);
	cluster_pcm_lock_pi_watermark_scn_advance(tag, (SCN)(token.page_scn + 1),
											  CLUSTER_PCM_WM_SRC_REDECLARE, 1, 7004, 17);
	UT_ASSERT_EQ(cluster_pcm_lock_queue_handoff_x_exact(&token), PCM_X_GRD_HANDOFF_BAD_STATE);
	UT_ASSERT(cluster_pcm_lock_authority_snapshot(tag, &after));
	UT_ASSERT_EQ((int)after.state, (int)PCM_STATE_X);
	UT_ASSERT_EQ(after.pending_x_requester_node, 3);
}

/*
 * P0-20: reproduce the real ordering that failed t/400 at 14:23 without a
 * scheduler race.  A and B first hold S.  A is the pre-acked transfer source;
 * the production-equivalent effects of B's exact slotless INVALIDATE_ACK then
 * remove B and advance the monotone watermark to W.  A subsequently presents
 * an older materialized image S.
 *
 * The final GRD handoff gate remains the non-negotiable last defence, but the
 * protocol must classify this as a recoverable stale source before type 50 is
 * accepted and PREPARE_GRANT is emitted.  This test deliberately combines the
 * real GRD authority/watermark operations with a bounded source contract over
 * the production ACK and IMAGE_READY handlers.  It does not call the static
 * ACK handler directly; separate gate/provenance tests did not expose this
 * inter-handler gap.
 */
UT_TEST(test_pcm_x_slotless_ack_floor_fences_stale_source_before_prepare)
{
	BufferTag tag = make_tag(111);
	PcmAuthoritySnapshot authority;
	PcmAuthoritySnapshot after;
	PcmXGrdHandoffToken stale_handoff;
	ClusterPcmWmProv provenance;
	const SCN source_scn = (SCN)0x4000;
	const SCN watermark_scn = (SCN)0x5000;
	char *source;
	const char *ack_handler;
	const char *ack_end;
	const char *ack_match;
	const char *holder_remove;
	const char *watermark_advance;
	const char *bitmap_replace;
	const char *drive;
	const char *ready_handler;
	const char *ready_end;
	const char *floor_query;
	const char *floor_verdict;
	const char *image_ready;
	const char *prepare;
	bool source_floor_gate;

	reset_fake_pcm_runtime(4);
	cluster_node_id = 0;
	cluster_pcm_lock_acquire(tag, PCM_LOCK_MODE_S);
	cluster_node_id = 1;
	cluster_pcm_lock_acquire(tag, PCM_LOCK_MODE_S);
	UT_ASSERT_EQ(cluster_pcm_lock_try_reserve_pending_x(tag, 3, 9011), PCM_PENDING_X_RESERVE_OK);

	/* Source A (node 0) is pre-acked by the transfer driver.  Apply the two
	 * production-equivalent state effects of non-source B's ACK (node 1), then
	 * use the bounded source contract below to prove their real handler order. */
	UT_ASSERT(cluster_pcm_lock_apply_gcs_transition(tag, PCM_TRANS_S_TO_N_INVALIDATE, 1));
	cluster_pcm_lock_pi_watermark_scn_advance(tag, watermark_scn, CLUSTER_PCM_WM_SRC_ACK_SLOTLESS,
											  1, 9011, 17);
	UT_ASSERT(cluster_pcm_lock_pi_watermark_prov_query(tag, &provenance));
	UT_ASSERT_EQ((int)provenance.source, (int)CLUSTER_PCM_WM_SRC_ACK_SLOTLESS);
	UT_ASSERT_EQ(provenance.sender_node, 1);
	UT_ASSERT_EQ(provenance.request_id, UINT64_C(9011));
	UT_ASSERT_EQ((uint64)provenance.new_scn, (uint64)watermark_scn);
	UT_ASSERT_EQ(cluster_pcm_lock_query_s_holders_bitmap(tag), UINT32_C(1) << 0);
	UT_ASSERT(cluster_pcm_lock_queue_pending_x_exact(tag, 3, 9011));
	UT_ASSERT_EQ((int)gcs_block_lost_write_verdict(watermark_scn, source_scn),
				 (int)GCS_LOST_WRITE_FAIL_STALE);

	/* The existing last line of defence must still refuse S < W and retain
	 * the pending-X authority for an exact retry/re-source path. */
	UT_ASSERT(cluster_pcm_lock_authority_snapshot(tag, &authority));
	stale_handoff = make_pcm_x_grd_handoff_token(tag, &authority, 0, 3, 52, 9011);
	stale_handoff.page_scn = source_scn;
	UT_ASSERT_EQ(cluster_pcm_lock_queue_handoff_x_exact(&stale_handoff),
				 PCM_X_GRD_HANDOFF_BAD_STATE);
	UT_ASSERT(cluster_pcm_lock_authority_snapshot(tag, &after));
	UT_ASSERT_EQ((int)after.state, (int)PCM_STATE_S);
	UT_ASSERT_EQ(after.s_holders_bitmap, UINT32_C(1) << 0);
	UT_ASSERT_EQ(after.pending_x_requester_node, 3);
	UT_ASSERT_EQ((uint64)cluster_pcm_lock_pi_watermark_scn_query(tag), (uint64)watermark_scn);

	/* Pin the production ordering: B's exact ACK must publish W before the
	 * bitmap can drive type 49.  IMAGE_READY must then consume that current
	 * floor and classify S<W before the queue engine can arm PREPARE_GRANT. */
	source = read_gcs_block_source();
	if (source == NULL)
		return;
	ack_handler = strstr(source, "\ncluster_gcs_handle_block_invalidate_ack_envelope(");
	ack_end = ack_handler != NULL ? strstr(ack_handler, "\n/* PGRAC: spec-7.2 flip") : NULL;
	ack_match = ack_handler != NULL
					? strstr(ack_handler, "gcs_block_pcm_x_queue_invalidate_ack_match(")
					: NULL;
	holder_remove
		= ack_match != NULL ? strstr(ack_match, "cluster_pcm_lock_apply_gcs_transition(") : NULL;
	watermark_advance = holder_remove != NULL
							? strstr(holder_remove, "cluster_pcm_lock_pi_watermark_scn_advance(")
							: NULL;
	bitmap_replace
		= watermark_advance != NULL
			  ? strstr(watermark_advance, "cluster_pcm_x_master_drive_bitmap_replace_exact(")
			  : NULL;
	drive = bitmap_replace != NULL ? strstr(bitmap_replace, "gcs_block_pcm_x_master_drive_tag(")
								   : NULL;
	UT_ASSERT_NOT_NULL(ack_handler);
	UT_ASSERT_NOT_NULL(ack_end);
	UT_ASSERT_NOT_NULL(ack_match);
	UT_ASSERT_NOT_NULL(holder_remove);
	UT_ASSERT_NOT_NULL(watermark_advance);
	UT_ASSERT_NOT_NULL(bitmap_replace);
	UT_ASSERT_NOT_NULL(drive);
	if (ack_handler != NULL && ack_end != NULL && ack_match != NULL && holder_remove != NULL
		&& watermark_advance != NULL && bitmap_replace != NULL && drive != NULL)
		UT_ASSERT(ack_handler < ack_match && ack_match < holder_remove
				  && holder_remove < watermark_advance && watermark_advance < bitmap_replace
				  && bitmap_replace < drive && drive < ack_end);

	ready_handler = strstr(source, "\ncluster_gcs_handle_pcm_x_image_ready_envelope(");
	ready_end = ready_handler != NULL
					? strstr(ready_handler, "\ncluster_gcs_handle_pcm_x_prepare_grant_envelope(")
					: NULL;
	floor_query = ready_handler != NULL
					  ? strstr(ready_handler, "cluster_pcm_lock_pi_watermark_scn_query(")
					  : NULL;
	floor_verdict
		= floor_query != NULL ? strstr(floor_query, "gcs_block_lost_write_verdict(") : NULL;
	image_ready = ready_handler != NULL
					  ? strstr(ready_handler, "cluster_pcm_x_master_image_ready_exact(")
					  : NULL;
	prepare = image_ready != NULL ? strstr(image_ready, "PGRAC_IC_MSG_PCM_X_PREPARE_GRANT") : NULL;
	UT_ASSERT_NOT_NULL(ready_handler);
	UT_ASSERT_NOT_NULL(ready_end);
	UT_ASSERT_NOT_NULL(image_ready);
	UT_ASSERT_NOT_NULL(prepare);
	source_floor_gate = ready_handler != NULL && ready_end != NULL && floor_query != NULL
						&& floor_verdict != NULL && image_ready != NULL && prepare != NULL
						&& ready_handler < floor_query && floor_query < floor_verdict
						&& floor_verdict < image_ready && image_ready < prepare
						&& prepare < ready_end;
	UT_ASSERT(source_floor_gate);
	free(source);
}

/* spec-5.2a D2 (U1): clean-page X-transfer arm is one-shot.  arm(true) sets
 * the backend-local flag; consume() reads-and-clears it (the acquire path
 * calls consume() once so the eligibility can never leak into a SUBSEQUENT
 * (heap) buffer access — inv ①/⑤, R3).  is_armed() is a non-destructive
 * peek. */
UT_TEST(test_clean_page_xfer_arm_is_one_shot)
{
	/* Default disarmed. */
	cluster_pcm_clean_page_xfer_arm(false);
	UT_ASSERT_EQ(cluster_pcm_clean_page_xfer_is_armed() ? 1 : 0, 0);
	/* consume() on a disarmed flag returns false and stays disarmed. */
	UT_ASSERT_EQ(cluster_pcm_clean_page_xfer_consume() ? 1 : 0, 0);

	/* arm → peek true (non-destructive) → peek still true. */
	cluster_pcm_clean_page_xfer_arm(true);
	UT_ASSERT_EQ(cluster_pcm_clean_page_xfer_is_armed() ? 1 : 0, 1);
	UT_ASSERT_EQ(cluster_pcm_clean_page_xfer_is_armed() ? 1 : 0, 1);

	/* consume → returns true ONCE → flag now cleared (single-shot). */
	UT_ASSERT_EQ(cluster_pcm_clean_page_xfer_consume() ? 1 : 0, 1);
	UT_ASSERT_EQ(cluster_pcm_clean_page_xfer_is_armed() ? 1 : 0, 0);
	/* A second consume sees no leak. */
	UT_ASSERT_EQ(cluster_pcm_clean_page_xfer_consume() ? 1 : 0, 0);

	/* Idempotent re-arm/disarm. */
	cluster_pcm_clean_page_xfer_arm(true);
	cluster_pcm_clean_page_xfer_arm(false);
	UT_ASSERT_EQ(cluster_pcm_clean_page_xfer_is_armed() ? 1 : 0, 0);
}

UT_TEST(test_authority_generation_covers_pending_and_holder_tuple_changes)
{
	BufferTag tag = make_tag(122);
	PcmAuthoritySnapshot first;
	PcmAuthoritySnapshot second;

	reset_fake_pcm_runtime(4);
	UT_ASSERT_EQ(cluster_pcm_lock_try_reserve_pending_x(tag, 3, 9101),
				 PCM_PENDING_X_RESERVE_OK);
	UT_ASSERT(cluster_pcm_lock_authority_snapshot(tag, &first));
	UT_ASSERT(first.authority_generation > 0);
	UT_ASSERT(cluster_pcm_lock_clear_queue_pending_x_exact(tag, 3, 9101));
	UT_ASSERT(cluster_pcm_lock_authority_snapshot(tag, &second));
	UT_ASSERT_EQ(second.authority_generation, first.authority_generation + 1);

	cluster_node_id = 1;
	cluster_pcm_lock_acquire(tag, PCM_LOCK_MODE_S);
	UT_ASSERT(cluster_pcm_lock_authority_snapshot(tag, &first));
	cluster_pcm_lock_acquire(tag, PCM_LOCK_MODE_S);
	UT_ASSERT(cluster_pcm_lock_authority_snapshot(tag, &second));
	UT_ASSERT_EQ(second.authority_generation, first.authority_generation);

	cluster_node_id = 2;
	cluster_pcm_lock_acquire(tag, PCM_LOCK_MODE_S);
	UT_ASSERT(cluster_pcm_lock_authority_snapshot(tag, &second));
	UT_ASSERT_EQ(second.authority_generation, first.authority_generation + 1);
	first = second;
	cluster_pcm_lock_release(tag);
	UT_ASSERT(cluster_pcm_lock_authority_snapshot(tag, &second));
	UT_ASSERT_EQ(second.authority_generation, first.authority_generation + 1);
}

static void
make_empty_authority_entry(BufferTag tag, uint64 ticket_id)
{
	UT_ASSERT_EQ(cluster_pcm_lock_try_reserve_pending_x(tag, 31, ticket_id),
				 PCM_PENDING_X_RESERVE_OK);
	UT_ASSERT(cluster_pcm_lock_clear_queue_pending_x_exact(tag, 31, ticket_id));
}

static void
assert_gcs_transition_advances_generation(BufferTag tag, PcmLockTransition transition,
										   int holder_node)
{
	PcmAuthoritySnapshot before;
	PcmAuthoritySnapshot after;

	UT_ASSERT(cluster_pcm_lock_authority_snapshot(tag, &before));
	UT_ASSERT_EQ(cluster_pcm_lock_apply_gcs_transition_result(tag, transition, holder_node),
				 PCM_GCS_TRANSITION_APPLIED);
	UT_ASSERT(cluster_pcm_lock_authority_snapshot(tag, &after));
	UT_ASSERT_EQ(after.authority_generation, before.authority_generation + 1);
}

UT_TEST(test_authority_generation_covers_all_eight_live_transitions_and_aba)
{
	BufferTag tags[8];
	PcmAuthoritySnapshot old_x;
	PcmAuthoritySnapshot final_x;
	int i;

	reset_fake_pcm_runtime(8);
	for (i = 0; i < 8; i++) {
		tags[i] = make_tag((uint32)(124 + i));
		make_empty_authority_entry(tags[i], (uint64)(9200 + i));
	}

	assert_gcs_transition_advances_generation(tags[0], PCM_TRANS_N_TO_S, 1);
	assert_gcs_transition_advances_generation(tags[1], PCM_TRANS_N_TO_X, 1);

	UT_ASSERT(cluster_pcm_lock_apply_gcs_transition(tags[2], PCM_TRANS_N_TO_S, 1));
	assert_gcs_transition_advances_generation(tags[2], PCM_TRANS_S_TO_X_UPGRADE, 1);

	UT_ASSERT(cluster_pcm_lock_apply_gcs_transition(tags[3], PCM_TRANS_N_TO_X, 1));
	assert_gcs_transition_advances_generation(tags[3], PCM_TRANS_X_TO_S_DOWNGRADE, 1);

	UT_ASSERT(cluster_pcm_lock_apply_gcs_transition(tags[4], PCM_TRANS_N_TO_X, 1));
	assert_gcs_transition_advances_generation(tags[4], PCM_TRANS_X_TO_N_DOWNGRADE, 1);

	UT_ASSERT(cluster_pcm_lock_apply_gcs_transition(tags[5], PCM_TRANS_N_TO_X, 1));
	assert_gcs_transition_advances_generation(tags[5], PCM_TRANS_X_TO_N_RELEASE, 1);

	UT_ASSERT(cluster_pcm_lock_apply_gcs_transition(tags[6], PCM_TRANS_N_TO_S, 1));
	assert_gcs_transition_advances_generation(tags[6], PCM_TRANS_S_TO_N_INVALIDATE, 1);

	UT_ASSERT(cluster_pcm_lock_apply_gcs_transition(tags[7], PCM_TRANS_N_TO_S, 1));
	assert_gcs_transition_advances_generation(tags[7], PCM_TRANS_S_TO_N_RELEASE, 1);

	/* Same visible X holder tuple after X0->N->X0 is still a new authority
	 * episode; the full generation makes the old optimistic token stale. */
	UT_ASSERT(cluster_pcm_lock_apply_gcs_transition(tags[0], PCM_TRANS_S_TO_N_RELEASE, 1));
	UT_ASSERT(cluster_pcm_lock_apply_gcs_transition(tags[0], PCM_TRANS_N_TO_X, 0));
	UT_ASSERT(cluster_pcm_lock_authority_snapshot(tags[0], &old_x));
	UT_ASSERT(cluster_pcm_lock_apply_gcs_transition(tags[0], PCM_TRANS_X_TO_N_RELEASE, 0));
	UT_ASSERT(cluster_pcm_lock_apply_gcs_transition(tags[0], PCM_TRANS_N_TO_X, 0));
	UT_ASSERT(cluster_pcm_lock_authority_snapshot(tags[0], &final_x));
	UT_ASSERT_EQ(final_x.authority_generation, old_x.authority_generation + 2);
	UT_ASSERT(!cluster_pcm_lock_authority_matches(tags[0], &old_x));
}

UT_TEST(test_authority_generation_covers_redeclare_dead_leave_and_grant)
{
	BufferTag tag_x = make_tag(132);
	BufferTag tag_s = make_tag(133);
	BufferTag tag_leave = make_tag(134);
	BufferTag tag_grant = make_tag(135);
	PcmAuthoritySnapshot before;
	PcmAuthoritySnapshot after;
	PcmAuthoritySnapshot replay;

	reset_fake_pcm_runtime(8);
	fake_cssd_dead_node = -1;

	UT_ASSERT(cluster_gcs_block_master_rebuild_from_redeclare(
		tag_x, (uint8)PCM_STATE_X, InvalidXLogRecPtr, InvalidScn, 2, 17));
	UT_ASSERT(cluster_pcm_lock_authority_snapshot(tag_x, &before));
	UT_ASSERT(cluster_gcs_block_master_rebuild_from_redeclare(
		tag_x, (uint8)PCM_STATE_X, InvalidXLogRecPtr, InvalidScn, 2, 17));
	UT_ASSERT(cluster_pcm_lock_authority_snapshot(tag_x, &after));
	UT_ASSERT_EQ(after.authority_generation, before.authority_generation);
	UT_ASSERT(!cluster_gcs_block_master_rebuild_from_redeclare(
		tag_x, (uint8)PCM_STATE_X, InvalidXLogRecPtr, InvalidScn, 3, 17));
	UT_ASSERT(cluster_pcm_lock_authority_snapshot(tag_x, &replay));
	UT_ASSERT_EQ(replay.authority_generation, after.authority_generation);

	UT_ASSERT(cluster_gcs_block_master_rebuild_from_redeclare(
		tag_s, (uint8)PCM_STATE_S, InvalidXLogRecPtr, InvalidScn, 1, 17));
	UT_ASSERT(cluster_pcm_lock_authority_snapshot(tag_s, &before));
	UT_ASSERT(cluster_gcs_block_master_rebuild_from_redeclare(
		tag_s, (uint8)PCM_STATE_S, InvalidXLogRecPtr, InvalidScn, 2, 17));
	UT_ASSERT(cluster_pcm_lock_authority_snapshot(tag_s, &after));
	UT_ASSERT_EQ(after.authority_generation, before.authority_generation + 1);
	before = after;
	(void)cluster_pcm_lock_cleanup_on_node_dead(1);
	UT_ASSERT(cluster_pcm_lock_authority_snapshot(tag_s, &after));
	UT_ASSERT_EQ(after.authority_generation, before.authority_generation + 1);
	before = after;
	(void)cluster_pcm_lock_cleanup_on_node_dead(1);
	UT_ASSERT(cluster_pcm_lock_authority_snapshot(tag_s, &after));
	UT_ASSERT_EQ(after.authority_generation, before.authority_generation);

	cluster_node_id = 1;
	cluster_pcm_lock_acquire(tag_leave, PCM_LOCK_MODE_S);
	cluster_node_id = 2;
	cluster_pcm_lock_acquire(tag_leave, PCM_LOCK_MODE_S);
	UT_ASSERT(cluster_pcm_lock_authority_snapshot(tag_leave, &before));
	cluster_node_id = 1;
	(void)cluster_pcm_lock_clean_leave_release_all_self(18);
	UT_ASSERT(cluster_pcm_lock_authority_snapshot(tag_leave, &after));
	UT_ASSERT_EQ(after.authority_generation, before.authority_generation + 1);
	UT_ASSERT_EQ(after.master_holder.node_id, (uint32)2);
	before = after;
	(void)cluster_pcm_lock_clean_leave_release_all_self(18);
	UT_ASSERT(cluster_pcm_lock_authority_snapshot(tag_leave, &after));
	UT_ASSERT_EQ(after.authority_generation, before.authority_generation);

	make_empty_authority_entry(tag_grant, 9300);
	UT_ASSERT(cluster_pcm_lock_authority_snapshot(tag_grant, &before));
	cluster_pcm_lock_master_grant_x_to(tag_grant, 3, InvalidXLogRecPtr, InvalidScn, 700, 19);
	UT_ASSERT(cluster_pcm_lock_authority_snapshot(tag_grant, &after));
	UT_ASSERT_EQ(after.authority_generation, before.authority_generation + 1);
	before = after;
	cluster_pcm_lock_master_grant_x_to(tag_grant, 3, InvalidXLogRecPtr, InvalidScn, 700, 19);
	UT_ASSERT(cluster_pcm_lock_authority_snapshot(tag_grant, &after));
	UT_ASSERT_EQ(after.authority_generation, before.authority_generation);
}

UT_TEST(test_authority_generation_never_wraps)
{
	uint64 next = UINT64_C(0x1122334455667788);

	UT_ASSERT(PcmAuthorityGenerationNext(UINT64_C(1), &next));
	UT_ASSERT_EQ(next, UINT64_C(2));
	UT_ASSERT(PcmAuthorityGenerationNext(UINT64_MAX - 1, &next));
	UT_ASSERT_EQ(next, UINT64_MAX);

	next = UINT64_C(0x8877665544332211);
	UT_ASSERT(!PcmAuthorityGenerationNext(UINT64_MAX, &next));
	UT_ASSERT_EQ(next, UINT64_C(0x8877665544332211));
	UT_ASSERT(!PcmAuthorityGenerationNext(UINT64_C(0), &next));
	UT_ASSERT_EQ(next, UINT64_C(0x8877665544332211));
	UT_ASSERT(!PcmAuthorityGenerationNext(UINT64_C(1), NULL));
}

UT_TEST(test_authority_generation_max_rejects_real_mutation_before_core_write)
{
	BufferTag tag = make_tag(136);
	PcmAuthoritySnapshot baseline;
	PcmAuthoritySnapshot after;

	reset_fake_pcm_runtime(4);
	cluster_node_id = 1;
	cluster_pcm_lock_acquire(tag, PCM_LOCK_MODE_S);
	UT_ASSERT(cluster_pcm_lock_test_force_authority_generation(tag, UINT64_MAX));
	UT_ASSERT(cluster_pcm_lock_authority_snapshot(tag, &baseline));

	/* Every no-op stays legal at MAX: duplicate holder, duplicate re-declare,
	 * and mismatched clear neither increments nor PANICs. */
	UT_ASSERT_EQ(cluster_pcm_lock_apply_gcs_transition_result(tag, PCM_TRANS_N_TO_S, 1),
				 PCM_GCS_TRANSITION_APPLIED);
	UT_ASSERT(cluster_gcs_block_master_rebuild_from_redeclare(
		tag, (uint8)PCM_STATE_S, InvalidXLogRecPtr, InvalidScn, 1, 20));
	UT_ASSERT(!cluster_pcm_lock_clear_pending_x_if(tag, 3));
	UT_ASSERT(cluster_pcm_lock_authority_snapshot(tag, &after));
	UT_ASSERT_EQ(memcmp(&after, &baseline, sizeof(after)), 0);

	/* A new holder would change the bitmap.  The generation preflight PANICs
	 * before that first write; emulate PG's error cleanup for the unit lock
	 * stack, then prove the complete authority snapshot is byte-stable. */
	UT_EXPECT_EREPORT(
		(void)cluster_pcm_lock_apply_gcs_transition_result(tag, PCM_TRANS_N_TO_S, 2));
	fake_lwlock_depth = 0;
	fake_lwlock_held = NULL;
	fake_lwlock_mode = LW_EXCLUSIVE;
	memset(fake_lwlock_stack, 0, sizeof(fake_lwlock_stack));
	memset(fake_lwlock_mode_stack, 0, sizeof(fake_lwlock_mode_stack));
	UT_ASSERT(cluster_pcm_lock_authority_snapshot(tag, &after));
	UT_ASSERT_EQ(memcmp(&after, &baseline, sizeof(after)), 0);
}

UT_TEST(test_authority_and_watermark_snapshot_is_one_exact_entry_lock_token)
{
	BufferTag tag = make_tag(137);
	PcmAuthoritySnapshot authority;
	SCN watermark;

	reset_fake_pcm_runtime(4);
	cluster_node_id = 1;
	cluster_pcm_lock_acquire(tag, PCM_LOCK_MODE_X);
	cluster_pcm_lock_pi_watermark_scn_advance(tag, (SCN)101, CLUSTER_PCM_WM_SRC_REDECLARE, 1, 0,
											  22);
	UT_ASSERT(cluster_pcm_lock_authority_watermark_snapshot(tag, &authority, &watermark));
	UT_ASSERT_EQ((uint64)watermark, UINT64_C(101));
	UT_ASSERT(cluster_pcm_lock_authority_watermark_matches(tag, &authority, watermark));

	/* Watermark is outside the core generation by design, so the authority
	 * token alone remains equal while the combined marker token must stale. */
	cluster_pcm_lock_pi_watermark_scn_advance(tag, (SCN)202, CLUSTER_PCM_WM_SRC_REDECLARE, 1, 0,
											  22);
	UT_ASSERT(cluster_pcm_lock_authority_matches(tag, &authority));
	UT_ASSERT(!cluster_pcm_lock_authority_watermark_matches(tag, &authority, watermark));
}

UT_TEST(test_legacy_pending_x_rejects_zero_cookie_before_entry_mutation)
{
	BufferTag tag = make_tag(123);
	PcmAuthoritySnapshot snapshot;

	reset_fake_pcm_runtime(4);
	UT_ASSERT_EQ(cluster_pcm_lock_set_pending_x(tag, 2, UINT64_C(0)),
				 PCM_PENDING_X_RESERVE_INVALID);
	UT_ASSERT(!cluster_pcm_lock_authority_snapshot(tag, &snapshot));
}

UT_TEST(test_stale_x_commit_is_one_lock_exact_and_generation_single_step)
{
	BufferTag tag = make_tag(138);
	BufferTag drift_tag = make_tag(139);
	PcmStaleXCommitToken token;
	PcmAuthoritySnapshot after;
	PcmAuthoritySnapshot replay;

	reset_fake_pcm_runtime(4);
	cluster_node_id = 2;
	cluster_pcm_lock_acquire(tag, PCM_LOCK_MODE_X);
	cluster_pcm_lock_pi_watermark_scn_advance(
		tag, (SCN)401, CLUSTER_PCM_WM_SRC_REDECLARE, 2, UINT64_C(0xabc300), 7);
	memset(&token, 0, sizeof(token));
	token.tag = tag;
	token.request_id = UINT64_C(0xabc300);
	token.request_epoch = 7;
	token.final_page_scn = (SCN)401;
	token.durable_page_scn = (SCN)403;
	token.holder_node = 2;
	token.holder_proof = PCM_STALE_X_FULL_FENCE_PROOF;
	token.relation_generation = 9;
	UT_ASSERT(cluster_pcm_lock_authority_snapshot(tag, &token.authority));

	UT_ASSERT_EQ((int)cluster_pcm_lock_stale_x_commit_exact(&token, &after),
				 (int)PCM_STALE_X_COMMIT_OK);
	UT_ASSERT_EQ((int)after.state, (int)PCM_STATE_N);
	UT_ASSERT_EQ(after.x_holder_node, -1);
	UT_ASSERT_EQ((uint32)after.s_holders_bitmap, UINT32_C(0));
	UT_ASSERT_EQ(after.pending_x_requester_node, -1);
	UT_ASSERT_EQ((uint64)after.pending_x_since_lsn, UINT64_C(0));
	UT_ASSERT_EQ(after.master_holder.node_id, UINT32_MAX);
	UT_ASSERT_EQ(after.authority_generation, token.authority.authority_generation + 1);
	UT_ASSERT_EQ(after.transition_count, token.authority.transition_count + 1);
	UT_ASSERT_EQ((uint64)cluster_pcm_lock_pi_watermark_scn_query(tag), UINT64_C(403));

	/* Lost local completion may replay the same exact token.  It observes
	 * the one-step terminal tuple and cannot increment generation twice. */
	UT_ASSERT_EQ((int)cluster_pcm_lock_stale_x_commit_exact(&token, &replay),
				 (int)PCM_STALE_X_COMMIT_DUPLICATE);
	UT_ASSERT_EQ(memcmp(&replay, &after, sizeof(after)), 0);

	token.holder_proof = GCS_STALE_X_PROOF_REPORT_MASK;
	UT_ASSERT_EQ((int)cluster_pcm_lock_stale_x_commit_exact(&token, NULL),
				 (int)PCM_STALE_X_COMMIT_INVALID);
	token.holder_proof = PCM_STALE_X_FULL_FENCE_PROOF;

	/* A full authority ABA fence and an exact watermark are both required;
	 * neither mismatch is allowed to mutate the live X tuple. */
	cluster_pcm_lock_acquire(drift_tag, PCM_LOCK_MODE_X);
	cluster_pcm_lock_pi_watermark_scn_advance(
		drift_tag, (SCN)501, CLUSTER_PCM_WM_SRC_REDECLARE, 2, UINT64_C(0xabc301), 7);
	token.tag = drift_tag;
	token.request_id = UINT64_C(0xabc301);
	token.final_page_scn = (SCN)500;
	token.durable_page_scn = (SCN)503;
	UT_ASSERT(cluster_pcm_lock_authority_snapshot(drift_tag, &token.authority));
	UT_ASSERT_EQ((int)cluster_pcm_lock_stale_x_commit_exact(&token, NULL),
				 (int)PCM_STALE_X_COMMIT_WATERMARK_STALE);
	UT_ASSERT(cluster_pcm_lock_authority_snapshot(drift_tag, &after));
	UT_ASSERT_EQ((int)after.state, (int)PCM_STATE_X);

	token.final_page_scn = (SCN)501;
	token.authority.authority_generation--;
	UT_ASSERT_EQ((int)cluster_pcm_lock_stale_x_commit_exact(&token, NULL),
				 (int)PCM_STALE_X_COMMIT_STALE);
	UT_ASSERT(cluster_pcm_lock_authority_snapshot(drift_tag, &after));
	UT_ASSERT_EQ((int)after.state, (int)PCM_STATE_X);
}

UT_TEST(test_forward_cancel_local_release_is_epoch_and_authority_exact)
{
	BufferTag tag = make_tag(151);
	PcmAuthoritySnapshot pre_registration;
	PcmAuthoritySnapshot registered_authority;
	PcmAuthoritySnapshot post_registration_release;
	PcmAuthoritySnapshot old_authority;
	PcmAuthoritySnapshot new_authority;
	PcmAuthoritySnapshot after_reject;
	PcmAuthoritySnapshot after_apply;
	PcmAuthoritySnapshot after_idempotent;
	const uint64 barrier_epoch = UINT64_C(61);

	reset_fake_pcm_runtime(4);
	cluster_node_id = 0;
	fake_pcm_current_epoch = barrier_epoch;
	UT_ASSERT(cluster_pcm_lock_apply_gcs_transition(
		tag, PCM_TRANS_N_TO_S, 1));
	UT_ASSERT(cluster_pcm_lock_authority_snapshot(
		tag, &pre_registration));
	UT_ASSERT(cluster_pcm_lock_apply_gcs_transition(
		tag, PCM_TRANS_N_TO_S, cluster_node_id));
	UT_ASSERT(cluster_pcm_lock_authority_snapshot(
		tag, &registered_authority));
	UT_ASSERT(registered_authority.authority_generation
			  > pre_registration.authority_generation);

	/* The first exact-helper read sees E, then the deterministic second
	 * (entry-lock) read sees E+1.  The final lock-internal fence must reject
	 * without changing the complete authority. */
	fake_pcm_epoch_read_count = 0;
	fake_pcm_epoch_flip_on_read = 2;
	UT_ASSERT_EQ(
		(int)cluster_pcm_lock_apply_gcs_transition_exact_result(
			tag, PCM_TRANS_S_TO_N_RELEASE, cluster_node_id,
			barrier_epoch, &registered_authority),
		(int)PCM_GCS_TRANSITION_INCOMPATIBLE);
	fake_pcm_epoch_flip_on_read = 0;
	UT_ASSERT(cluster_pcm_lock_authority_snapshot(
		tag, &post_registration_release));
	UT_ASSERT(memcmp(&registered_authority,
					 &post_registration_release,
					 sizeof(registered_authority)) == 0);

	/* A forward marker predates the requester's legal S registration.
	 * The replay must use the complete current snapshot as its exact token;
	 * equating it to the marker's older generation would park forever. */
	UT_ASSERT_EQ(
		(int)cluster_pcm_lock_apply_gcs_transition_exact_result(
			tag, PCM_TRANS_S_TO_N_RELEASE, cluster_node_id,
			barrier_epoch, &registered_authority),
		(int)PCM_GCS_TRANSITION_APPLIED);
	UT_ASSERT(cluster_pcm_lock_authority_snapshot(
		tag, &post_registration_release));
	UT_ASSERT_EQ(
		post_registration_release.s_holders_bitmap,
		(uint32)(1u << 1));

	UT_ASSERT(cluster_pcm_lock_apply_gcs_transition(
		tag, PCM_TRANS_N_TO_S, cluster_node_id));
	UT_ASSERT(cluster_pcm_lock_authority_snapshot(tag, &old_authority));

	/* Epoch-only drift with the still-byte-exact authority is rejected.
	 * This isolates the lock-internal epoch fence from the token fence. */
	fake_pcm_current_epoch = barrier_epoch + 1;
	UT_ASSERT_EQ(
		(int)cluster_pcm_lock_apply_gcs_transition_exact_result(
			tag, PCM_TRANS_S_TO_N_RELEASE, cluster_node_id,
			barrier_epoch, &old_authority),
		(int)PCM_GCS_TRANSITION_INCOMPATIBLE);
	UT_ASSERT(cluster_pcm_lock_authority_snapshot(tag, &after_reject));
	UT_ASSERT(memcmp(&old_authority, &after_reject,
					 sizeof(old_authority)) == 0);

	/* Deterministic identity-check -> epoch flip -> same-tag new-S arm. */
	UT_ASSERT(cluster_pcm_lock_apply_gcs_transition(
		tag, PCM_TRANS_S_TO_N_RELEASE, cluster_node_id));
	UT_ASSERT(cluster_pcm_lock_apply_gcs_transition(
		tag, PCM_TRANS_N_TO_S, cluster_node_id));
	UT_ASSERT(cluster_pcm_lock_authority_snapshot(tag, &new_authority));
	UT_ASSERT(memcmp(&old_authority, &new_authority,
					 sizeof(old_authority)) != 0);

	UT_ASSERT_NOT_NULL(
		(void *)cluster_pcm_lock_apply_gcs_transition_exact_result);
	if (cluster_pcm_lock_apply_gcs_transition_exact_result == NULL)
		return;

	/* Both the old certificate epoch and its old complete authority token
	 * must be rejected with a byte-exact zero-mutation post-state. */
	UT_ASSERT_EQ(
		(int)cluster_pcm_lock_apply_gcs_transition_exact_result(
			tag, PCM_TRANS_S_TO_N_RELEASE, cluster_node_id,
			barrier_epoch, &old_authority),
		(int)PCM_GCS_TRANSITION_INCOMPATIBLE);
	UT_ASSERT(cluster_pcm_lock_authority_snapshot(tag, &after_reject));
	UT_ASSERT(memcmp(&new_authority, &after_reject,
					 sizeof(new_authority)) == 0);

	/* Even at the current epoch, stale authority is fail-closed. */
	UT_ASSERT_EQ(
		(int)cluster_pcm_lock_apply_gcs_transition_exact_result(
			tag, PCM_TRANS_S_TO_N_RELEASE, cluster_node_id,
			barrier_epoch + 1, &old_authority),
		(int)PCM_GCS_TRANSITION_INCOMPATIBLE);
	UT_ASSERT(cluster_pcm_lock_authority_snapshot(tag, &after_reject));
	UT_ASSERT(memcmp(&new_authority, &after_reject,
					 sizeof(new_authority)) == 0);

	/* Same epoch plus the complete current token applies exactly once. */
	UT_ASSERT_EQ(
		(int)cluster_pcm_lock_apply_gcs_transition_exact_result(
			tag, PCM_TRANS_S_TO_N_RELEASE, cluster_node_id,
			barrier_epoch + 1, &new_authority),
		(int)PCM_GCS_TRANSITION_APPLIED);
	UT_ASSERT(cluster_pcm_lock_authority_snapshot(tag, &after_apply));
	UT_ASSERT_EQ((int)after_apply.state, (int)PCM_STATE_S);
	UT_ASSERT_EQ(after_apply.s_holders_bitmap, (uint32)(1u << 1));
	UT_ASSERT(after_apply.authority_generation
			  > new_authority.authority_generation);

	/* Release-first/replay: requester bit already absent is an idempotent
	 * exact success and must not churn the remaining holder authority. */
	UT_ASSERT_EQ(
		(int)cluster_pcm_lock_apply_gcs_transition_exact_result(
			tag, PCM_TRANS_S_TO_N_RELEASE, cluster_node_id,
			barrier_epoch + 1, &after_apply),
		(int)PCM_GCS_TRANSITION_APPLIED);
	UT_ASSERT(cluster_pcm_lock_authority_snapshot(
		tag, &after_idempotent));
	UT_ASSERT(memcmp(&after_apply, &after_idempotent,
					 sizeof(after_apply)) == 0);
}

UT_TEST(test_exact_x_apply_owns_pending_clear_in_same_entry_lock)
{
	BufferTag tag = make_tag(157);
	PcmAuthoritySnapshot pending;
	PcmAuthoritySnapshot after_reject;
	PcmAuthoritySnapshot after_apply;
	PcmAuthoritySnapshot after_incompatible;
	PcmAuthoritySnapshot after_queue;
	PcmAuthoritySnapshot after_other;
	PcmAuthoritySnapshot max_minus_one;
	PcmAuthoritySnapshot after_exhaustion;
	PcmAuthoritySnapshot structural_max;
	PcmAuthoritySnapshot after_structural_exhaustion;
	const uint64 old_epoch = UINT64_C(81);
	const uint64 new_epoch = old_epoch + 1;
	const uint64 pending_cookie = UINT64_C(0x710081);
	int broadcasts_before;

	reset_fake_pcm_runtime(4);
	cluster_node_id = 0;
	fake_pcm_epoch_flip_on_read = 0;
	fake_pcm_current_epoch = new_epoch;
	UT_ASSERT_EQ(
		(int)cluster_pcm_lock_set_pending_x(
			tag, cluster_node_id, pending_cookie),
		(int)PCM_PENDING_X_RESERVE_OK);
	UT_ASSERT(cluster_pcm_lock_authority_snapshot(tag, &pending));
	UT_ASSERT_EQ(pending.pending_x_requester_node, cluster_node_id);
	UT_ASSERT_EQ(pending.pending_x_since_lsn, pending_cookie);

	/* An old request cannot clear a new-epoch same-requester cookie. */
	UT_ASSERT_EQ(
		(int)cluster_pcm_lock_apply_gcs_transition_exact_result(
			tag, PCM_TRANS_N_TO_X, cluster_node_id,
			old_epoch, NULL),
		(int)PCM_GCS_TRANSITION_INCOMPATIBLE);
	UT_ASSERT(cluster_pcm_lock_authority_snapshot(tag, &after_reject));
	UT_ASSERT(memcmp(&pending, &after_reject, sizeof(pending)) == 0);

	/* For a current request, X apply and its matching pending clear are one
	 * entry-lock transaction; no handler-side clear remains afterward. */
	UT_ASSERT_EQ(
		(int)cluster_pcm_lock_apply_gcs_transition_exact_result(
			tag, PCM_TRANS_N_TO_X, cluster_node_id,
			new_epoch, NULL),
		(int)PCM_GCS_TRANSITION_APPLIED);
	UT_ASSERT(cluster_pcm_lock_authority_snapshot(tag, &after_apply));
	UT_ASSERT_EQ((int)after_apply.state, (int)PCM_STATE_X);
	UT_ASSERT_EQ(after_apply.x_holder_node, cluster_node_id);
	UT_ASSERT_EQ(after_apply.pending_x_requester_node, -1);
	UT_ASSERT_EQ(after_apply.pending_x_since_lsn, UINT64_C(0));
	UT_ASSERT_EQ(fake_cv_broadcast_count, 1);

	/* Once the exact epoch/authority gate passed, structural X
	 * incompatibility still owns cleanup of this requester's legacy cookie
	 * before releasing the entry lock. */
	reset_fake_pcm_runtime(4);
	cluster_node_id = 0;
	fake_pcm_current_epoch = new_epoch;
	UT_ASSERT(cluster_pcm_lock_apply_gcs_transition(
		tag, PCM_TRANS_N_TO_S, 1));
	UT_ASSERT_EQ(
		(int)cluster_pcm_lock_set_pending_x(
			tag, cluster_node_id, pending_cookie),
		(int)PCM_PENDING_X_RESERVE_OK);
	broadcasts_before = fake_cv_broadcast_count;
	UT_ASSERT_EQ(
		(int)cluster_pcm_lock_apply_gcs_transition_exact_result(
			tag, PCM_TRANS_N_TO_X, cluster_node_id,
			new_epoch, NULL),
		(int)PCM_GCS_TRANSITION_INCOMPATIBLE);
	UT_ASSERT(cluster_pcm_lock_authority_snapshot(
		tag, &after_incompatible));
	UT_ASSERT_EQ(after_incompatible.pending_x_requester_node, -1);
	UT_ASSERT_EQ(after_incompatible.pending_x_since_lsn, UINT64_C(0));
	UT_ASSERT_EQ(fake_cv_broadcast_count, broadcasts_before + 1);

	/* A PCM-X queue ticket is not a legacy request cookie and is never
	 * consumed by this node-scoped compatibility seam. */
	reset_fake_pcm_runtime(4);
	cluster_node_id = 0;
	fake_pcm_current_epoch = new_epoch;
	UT_ASSERT_EQ(
		(int)cluster_pcm_lock_try_reserve_pending_x(
			tag, cluster_node_id, UINT64_C(17)),
		(int)PCM_PENDING_X_RESERVE_OK);
	broadcasts_before = fake_cv_broadcast_count;
	UT_ASSERT_EQ(
		(int)cluster_pcm_lock_apply_gcs_transition_exact_result(
			tag, PCM_TRANS_N_TO_X, cluster_node_id,
			new_epoch, NULL),
		(int)PCM_GCS_TRANSITION_APPLIED);
	UT_ASSERT(cluster_pcm_lock_authority_snapshot(tag, &after_queue));
	UT_ASSERT_EQ(after_queue.pending_x_requester_node, cluster_node_id);
	UT_ASSERT(
		(after_queue.pending_x_since_lsn
		 & PCM_PENDING_X_QUEUE_KIND) != 0);
	UT_ASSERT_EQ(fake_cv_broadcast_count, broadcasts_before);

	/* Another requester's legacy mark is likewise outside this transition's
	 * identity and remains byte-stable. */
	reset_fake_pcm_runtime(4);
	cluster_node_id = 0;
	fake_pcm_current_epoch = new_epoch;
	UT_ASSERT_EQ(
		(int)cluster_pcm_lock_set_pending_x(tag, 1, pending_cookie),
		(int)PCM_PENDING_X_RESERVE_OK);
	broadcasts_before = fake_cv_broadcast_count;
	UT_ASSERT_EQ(
		(int)cluster_pcm_lock_apply_gcs_transition_exact_result(
			tag, PCM_TRANS_N_TO_X, cluster_node_id,
			new_epoch, NULL),
		(int)PCM_GCS_TRANSITION_APPLIED);
	UT_ASSERT(cluster_pcm_lock_authority_snapshot(tag, &after_other));
	UT_ASSERT_EQ(after_other.pending_x_requester_node, 1);
	UT_ASSERT_EQ(after_other.pending_x_since_lsn, pending_cookie);
	UT_ASSERT_EQ(fake_cv_broadcast_count, broadcasts_before);

	/* Legacy nonexact callers retain their historical separation: transition
	 * apply does not consume a pending cookie or emit its clear wakeup. */
	reset_fake_pcm_runtime(4);
	cluster_node_id = 0;
	fake_pcm_current_epoch = new_epoch;
	UT_ASSERT_EQ(
		(int)cluster_pcm_lock_set_pending_x(
			tag, cluster_node_id, pending_cookie),
		(int)PCM_PENDING_X_RESERVE_OK);
	broadcasts_before = fake_cv_broadcast_count;
	UT_ASSERT_EQ(
		(int)cluster_pcm_lock_apply_gcs_transition_result(
			tag, PCM_TRANS_N_TO_X, cluster_node_id),
		(int)PCM_GCS_TRANSITION_APPLIED);
	UT_ASSERT(cluster_pcm_lock_authority_snapshot(tag, &after_other));
	UT_ASSERT_EQ(
		after_other.pending_x_requester_node, cluster_node_id);
	UT_ASSERT_EQ(after_other.pending_x_since_lsn, pending_cookie);
	UT_ASSERT_EQ(fake_cv_broadcast_count, broadcasts_before);

	/* APPLIED + matching legacy clear needs two generation commits.  At
	 * MAX-1 the exact seam must PANIC before the first state/cookie write,
	 * never after publishing X with the cookie still armed. */
	reset_fake_pcm_runtime(4);
	cluster_node_id = 0;
	fake_pcm_current_epoch = new_epoch;
	UT_ASSERT_EQ(
		(int)cluster_pcm_lock_set_pending_x(
			tag, cluster_node_id, pending_cookie),
		(int)PCM_PENDING_X_RESERVE_OK);
	UT_ASSERT(cluster_pcm_lock_test_force_authority_generation(
		tag, UINT64_MAX - 1));
	UT_ASSERT(cluster_pcm_lock_authority_snapshot(
		tag, &max_minus_one));
	UT_EXPECT_EREPORT(
		(void)cluster_pcm_lock_apply_gcs_transition_exact_result(
			tag, PCM_TRANS_N_TO_X, cluster_node_id,
			new_epoch, NULL));
	fake_lwlock_depth = 0;
	fake_lwlock_held = NULL;
	fake_lwlock_mode = LW_EXCLUSIVE;
	memset(fake_lwlock_stack, 0, sizeof(fake_lwlock_stack));
	memset(fake_lwlock_mode_stack, 0,
		   sizeof(fake_lwlock_mode_stack));
	UT_ASSERT(cluster_pcm_lock_authority_snapshot(
		tag, &after_exhaustion));
	UT_ASSERT(memcmp(&max_minus_one, &after_exhaustion,
					 sizeof(max_minus_one)) == 0);

	/* Structural INCOMPATIBLE performs only the matching legacy clear.  At
	 * MAX, that one-step cleanup must likewise PANIC before touching either
	 * pending field. */
	reset_fake_pcm_runtime(4);
	cluster_node_id = 0;
	fake_pcm_current_epoch = new_epoch;
	UT_ASSERT(cluster_pcm_lock_apply_gcs_transition(
		tag, PCM_TRANS_N_TO_S, 1));
	UT_ASSERT_EQ(
		(int)cluster_pcm_lock_set_pending_x(
			tag, cluster_node_id, pending_cookie),
		(int)PCM_PENDING_X_RESERVE_OK);
	UT_ASSERT(cluster_pcm_lock_test_force_authority_generation(
		tag, UINT64_MAX));
	UT_ASSERT(cluster_pcm_lock_authority_snapshot(
		tag, &structural_max));
	UT_EXPECT_EREPORT(
		(void)cluster_pcm_lock_apply_gcs_transition_exact_result(
			tag, PCM_TRANS_N_TO_X, cluster_node_id,
			new_epoch, NULL));
	fake_lwlock_depth = 0;
	fake_lwlock_held = NULL;
	fake_lwlock_mode = LW_EXCLUSIVE;
	memset(fake_lwlock_stack, 0, sizeof(fake_lwlock_stack));
	memset(fake_lwlock_mode_stack, 0,
		   sizeof(fake_lwlock_mode_stack));
	UT_ASSERT(cluster_pcm_lock_authority_snapshot(
		tag, &after_structural_exhaustion));
	UT_ASSERT(memcmp(&structural_max,
					 &after_structural_exhaustion,
					 sizeof(structural_max)) == 0);
}

int
main(void)
{
	UT_PLAN(72);
	UT_RUN(test_pcm_lock_mode_constant_aliases_match_pcm_state);
	UT_RUN(test_pcm_lock_transition_count_is_9);
	UT_RUN(test_pcm_lock_transition_enum_values_are_1_to_9);
	UT_RUN(test_pcm_grd_max_entries_default_is_minus_one);
	UT_RUN(test_pcm_buffer_desc_invariants_hold_at_stage_2_30);
	UT_RUN(test_pcm_lock_module_init_symbol_is_callable);
	UT_RUN(test_pcm_trans_1_n_to_s_validator_accepts);
	UT_RUN(test_pcm_trans_2_n_to_x_validator_accepts);
	UT_RUN(test_pcm_trans_3_s_to_x_upgrade_validator_accepts);
	UT_RUN(test_pcm_trans_4_x_to_s_downgrade_validator_accepts);
	UT_RUN(test_pcm_trans_5_x_to_n_downgrade_validator_accepts);
	UT_RUN(test_pcm_trans_6_x_to_n_release_validator_accepts);
	UT_RUN(test_pcm_trans_7_s_to_n_invalidate_validator_accepts);
	UT_RUN(test_pcm_trans_8_s_to_n_release_validator_accepts);
	UT_RUN(test_pcm_trans_9_cleanout_validator_reachable_but_apply_fail_closed);
	UT_RUN(test_pcm_illegal_transition_validator_rejects);
	UT_RUN(test_pcm_disable_path_counters_return_zero);
	UT_RUN(test_pcm_grd_entry_lifecycle_link_surface);
	UT_RUN(test_pcm_per_entry_lwlock_independence_link_surface);
	UT_RUN(test_pcm_pi_bitmap_atomic_accessor_linkable);
	UT_RUN(test_pcm_counter_observability_9_accessors_linkable);
	UT_RUN(test_pcm_real_shared_s_holders_release_independently);
	UT_RUN(test_pcm_real_x_release_and_downgrade_require_owner);
	UT_RUN(test_pcm_real_upgrade_requires_single_s_holder);
	UT_RUN(test_pcm_real_summary_counts_live_entries);
	UT_RUN(test_pcm_grd_entry_abi_includes_authority_generation);
	UT_RUN(test_pcm_grd_convert_queue_placeholder_remains_null);
	UT_RUN(test_pcm_real_wait_event_call_sites_are_exercised);
	UT_RUN(test_pcm_H1_same_node_s_refcount_increments);
	UT_RUN(test_pcm_H2_last_s_release_transitions_to_n);
	UT_RUN(test_pcm_buffer_s_barrier_restart_single_eviction_closes_residency);
	UT_RUN(test_pcm_tag_only_nested_s_still_requires_two_releases);
	UT_RUN(test_pcm_H2b_same_node_s_residency_upgrades_to_x);
	UT_RUN(test_pcm_H3_incompatible_x_waits_and_wakes);
	UT_RUN(test_pcm_H4_release_broadcasts_only_on_state_change);
	UT_RUN(test_pcm_d2_mode_covers_truth_table);
	UT_RUN(test_pcm_d3_requester_is_holder_strict);
	UT_RUN(test_pcm_d4_other_live_holder_gate);
	UT_RUN(test_pcm_b_local_master_remote_x_holder_fail_closed);
	UT_RUN(test_pcm_d1_recovering_gate_fail_closed);
	UT_RUN(test_pcm_d2_rebuild_from_redeclare);
	UT_RUN(test_pcm_d3_not_double_x);
	UT_RUN(test_pcm_wm_prov_table_keeps_last_advance);
	UT_RUN(test_pcm_acquire_buffer_local_s_nonholder_registers_s_then_upgrades);
	UT_RUN(test_pcm_acquire_buffer_s_bootstrap_revalidates_remote_x);
	UT_RUN(test_pcm_acquire_buffer_s_revalidates_remote_x_after_precheck);
	UT_RUN(test_pcm_acquire_buffer_revalidates_remote_x_after_precheck);
	UT_RUN(test_pcm_acquire_buffer_routes_unchanged_remote_x_with_exact_authority);
	UT_RUN(test_pcm_x_transfer_commit_is_exact_and_late_reply_safe);
	UT_RUN(test_pcm_dead_node_cleanup_drops_holder_records);
	UT_RUN(test_pcm_authority_snapshot_is_one_entry_lock_view);
	UT_RUN(test_pcm_queue_pending_x_reservation_never_overwrites_another_node);
	UT_RUN(test_pcm_pending_x_blocks_new_remote_s_holder_atomically);
	UT_RUN(test_pcm_pending_x_blocks_new_local_s_holder_until_clear);
	UT_RUN(test_pcm_queue_handoff_x_exact_rejects_authority_drift);
	UT_RUN(test_pcm_queue_handoff_x_exact_rejects_residual_s_holder);
	UT_RUN(test_pcm_queue_handoff_x_exact_commits_full_identity_and_replays);
	UT_RUN(test_pcm_queue_handoff_x_exact_accepts_global_n_with_real_image);
	UT_RUN(test_pcm_queue_handoff_x_exact_accepts_ordered_self_x);
	UT_RUN(test_pcm_queue_handoff_x_exact_uses_scn_not_cross_stream_lsn);
	UT_RUN(test_pcm_x_slotless_ack_floor_fences_stale_source_before_prepare);
	UT_RUN(test_clean_page_xfer_arm_is_one_shot);
	UT_RUN(test_authority_generation_covers_pending_and_holder_tuple_changes);
	UT_RUN(test_authority_generation_covers_all_eight_live_transitions_and_aba);
	UT_RUN(test_authority_generation_covers_redeclare_dead_leave_and_grant);
	UT_RUN(test_authority_generation_never_wraps);
	UT_RUN(test_authority_generation_max_rejects_real_mutation_before_core_write);
	UT_RUN(test_authority_and_watermark_snapshot_is_one_exact_entry_lock_token);
	UT_RUN(test_legacy_pending_x_rejects_zero_cookie_before_entry_mutation);
	UT_RUN(test_stale_x_commit_is_one_lock_exact_and_generation_single_step);
	UT_RUN(test_forward_cancel_local_release_is_epoch_and_authority_exact);
	UT_RUN(test_exact_x_apply_owns_pending_clear_in_same_entry_lock);
	UT_DONE();
	return ut_failed_count == 0 ? 0 : 1;
}
