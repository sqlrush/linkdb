/*-------------------------------------------------------------------------
 *
 * test_cluster_ges.c
 *	  Standalone unit tests for spec-2.13 GES protocol skeleton.
 *
 *	  T-ges-1 (5 tests, spec-2.13 D6):
 *	    a) cluster_ges_request_handler / cluster_ges_reply_handler symbol
 *	       linkable (UT_ASSERT_NOT_NULL fn ptr).
 *	    b) cluster_ges_request_defer_count / cluster_ges_reply_defer_count
 *	       accessors linkable + initial read returns 0.
 *	    c) **真行为 — request stub**:  pre = cluster_ges_request_defer_count()
 *	       → invoke cluster_ges_request_handler(envelope_sentinel, NULL) →
 *	       assert (1) handler 静默返回 (no ERROR/FATAL abort), (2)
 *	       cluster_ges_request_defer_count() == pre + 1, (3)
 *	       cluster_ges_reply_defer_count() == pre_reply (unchanged).
 *	    d) **真行为 — reply stub**:  symmetric with (c) but reply path;
 *	       assert reply_defer_count +1 + request_defer_count unchanged.
 *	    e) handler 跨 multiple invocations 真测 monotonic non-decrease +
 *	       counter accuracy (handler 调用 N 次 → counter 真递增 N).
 *
 *	  Stubs:
 *	    - ShmemInitStruct returns a union force-aligned buffer per L105
 *	      (Apple silicon tolerates misaligned atomic but strict-alignment
 *	      platform ARM Linux / SPARC SIGBUS without union force-align).
 *	    - cluster_shmem_register_region: no-op (region 注册不真测).
 *	    - elog / ereport: stubbed pass-through (DEBUG2 from handler
 *	      should be silent in test runner).
 *
 *	  Spec: spec-2.13 D6 + Q5.2 + Q9 (L105 union force-align).
 *	  Cross-spec lesson inheritance: L94 / L105 / L106 / L107.
 *
 *
 * Portions Copyright (c) 1996-2024, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2026, pgrac contributors
 *
 * Author: SqlRush <sqlrush@gmail.com>
 *
 * IDENTIFICATION
 *	  src/test/cluster_unit/test_cluster_ges.c
 *
 * NOTES
 *	  This is a pgrac-original file.  Standalone binary linking
 *	  cluster_ges.o only; all PG backend symbols stubbed locally.
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include <signal.h>
#include <stdlib.h> /* spec-5.8 D8 — malloc/free for the palloc/pfree stubs */
#include <string.h>

#include "access/transam.h" /* spec-5.8 D1c — InvalidTransactionId for the GetTopTransactionIdIfAny stub */
#include "cluster/cluster_ges.h"
#include "cluster/cluster_ges_dedup.h"
#include "cluster/cluster_ges_reply_wait.h"
#include "cluster/cluster_touched_peers.h" /* spec-5.14 D2 stamp stub */
#include "cluster/cluster_grd.h"
#include "cluster/cluster_grd_outbound.h"
#include "cluster/cluster_grd_work_queue.h"
#include "cluster/cluster_ic.h" /* spec-5.8 D8 — ClusterICSendResult for the send-envelope stub */
#include "cluster/cluster_ic_envelope.h"
#include "cluster/cluster_cssd.h"		   /* spec-5.7 Direction B stub — peer state */
#include "cluster/cluster_extend_gate.h"   /* spec-5.7 Direction B stub — sole-native */
#include "cluster/cluster_inject.h"		   /* S3 forensics step 1a stub prototypes */
#include "cluster/cluster_qvotec.h"
#include "cluster/cluster_sf_dep.h"
#include "cluster/cluster_xnode_profile.h" /* spec-5.59 D2 stub — profiling gate */
#include "port/atomics.h"

/* Drop PG's port.h printf -> pg_printf override; unit_test.h uses
 * stdlib printf and we don't link libpgport in this test binary. */
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


/* ============================================================
 * Stubs needed to link cluster_ges.o standalone.
 *
 *	ShmemInitStruct uses L105 union force-align pattern (mirror
 *	test_cluster_scn.c spec-2.11 P1.2 fix) — pg_atomic_uint64 must
 *	be 8-byte aligned on strict-alignment platforms.
 * ============================================================ */

bool IsUnderPostmaster = false;

/* S3 forensics step 1a stubs — cluster_ges.c now carries the
 * cluster-ges-master-work-queue-full injection point; this standalone
 * binary links no cluster_inject.o.  armed_count 0 keeps the macro's
 * fast-path gate closed; the helpers are link-only. */
int cluster_injection_armed_count = 0;

void
cluster_injection_run(const char *name pg_attribute_unused())
{}

bool
cluster_injection_should_skip(const char *name pg_attribute_unused())
{
	return false;
}

void
ExceptionalCondition(const char *conditionName pg_attribute_unused(),
					 const char *fileName pg_attribute_unused(),
					 int lineNumber pg_attribute_unused())
{
	abort();
}

bool
errstart(int e pg_attribute_unused(), const char *d pg_attribute_unused())
{
	return false;
}

bool
errstart_cold(int e pg_attribute_unused(), const char *d pg_attribute_unused())
{
	return false;
}

void
errfinish(const char *f pg_attribute_unused(), int l pg_attribute_unused(),
		  const char *fn pg_attribute_unused())
{}

int
errcode(int s pg_attribute_unused())
{
	return 0;
}

int
errmsg(const char *f pg_attribute_unused(), ...)
{
	return 0;
}

int
errmsg_internal(const char *f pg_attribute_unused(), ...)
{
	return 0;
}

int
errdetail(const char *f pg_attribute_unused(), ...)
{
	return 0;
}

int
errhint(const char *f pg_attribute_unused(), ...)
{
	return 0;
}

void
elog_start(const char *f pg_attribute_unused(), int l pg_attribute_unused(),
		   const char *fn pg_attribute_unused())
{}

void
elog_finish(int e pg_attribute_unused(), const char *f pg_attribute_unused(), ...)
{}

/*
 * spec-2.22 D6 — cluster_ges.c handler calls pgstat_report_wait_start/_end
 * for WAIT_EVENT_CLUSTER_LMD_PROBE.  The inline helpers reference
 * my_wait_event_info which lives in pgstat backend code.  Provide a
 * file-static fake so the standalone link resolves cleanly.
 */
#include "pgstat.h"
static uint32 ut_wait_event_info_storage = 0;
uint32 *my_wait_event_info = &ut_wait_event_info_storage;

/* cluster_lmd_graph_snapshot_copy stub — cluster_ges DEADLOCK_PROBE
 * handler calls into LMD graph;  test_cluster_ges binary links
 * cluster_ges.o standalone (no cluster_lmd_graph.o), so stub it. */
#include "cluster/cluster_lmd.h"

int
cluster_lmd_graph_snapshot_copy(ClusterLmdWaitEdge *out_buf pg_attribute_unused(),
								int max_edges pg_attribute_unused(), uint64 *out_gen_at_snapshot)
{
	if (out_gen_at_snapshot)
		*out_gen_at_snapshot = 0;
	return 0;
}
/* cluster_lmd_is_ready stub already provided below (line ~275). */

/*
 * spec-2.13 Q9 (L105 inherit):  ShmemInitStruct stub uses union
 * force-align to guarantee 8-byte alignment for pg_atomic_uint64
 * fields inside ClusterGesSharedState.
 */
void *
ShmemInitStruct(const char *name, Size size, bool *foundPtr)
{
	static union {
		uint64 force_align;
		char data[1024]; /* generous; cluster_ges_shmem_size() << 1KB */
	} ges_buf;
	static bool ges_initialized = false;

	if (name != NULL && strcmp(name, "pgrac cluster ges") == 0) {
		Assert(size <= sizeof(ges_buf.data)); /* catch shmem layout growth */
		*foundPtr = ges_initialized;
		ges_initialized = true;
		return ges_buf.data;
	}

	*foundPtr = true;
	return NULL;
}

void
cluster_shmem_register_region(const void *r pg_attribute_unused())
{}


/* ============================================================
 * spec-2.16 Step 6 L104 stubs — cross-module deps activated by
 *   Step 3 D6 handler (5-item validation + work_queue enqueue +
 *   REJECT_BUSY fallback).  Stubs default to "pass-through" so
 *   既有 T-ges-1 tests still PASS with handler 真激活 behavior.
 * ============================================================ */

int cluster_node_id = 0;
static bool stub_dedup_lifecycle_enabled = false;
static uint64 stub_dedup_lifecycle_enqueue_count = 0;
static GesDedupLifecyclePayload stub_dedup_lifecycle_last;
static uint32 stub_dedup_retire_pending = 0;
static uint64 stub_grd_retire_proc_count = 0;
static uint64 stub_lookup_superseded_boot = 0;
static uint64 stub_grd_retire_boot_count = 0;
static uint32 stub_grd_retire_boot_origin = 0;
static uint64 stub_grd_retire_boot_value = 0;
static uint64 stub_dedup_journal_register_count = 0;
static uint64 stub_dedup_journal_commit_count = 0;
static bool stub_dedup_journal_register_success = true;
static uint64 stub_dedup_record_identity_count = 0;
static ClusterGesDedupKey stub_dedup_record_identity_key;
static uint64 stub_dedup_record_identity_boot = 0;
static uint16 stub_dedup_record_identity_reply_len = 0;

bool
cluster_qvotec_in_quorum(void)
{
	return true; /* default in-quorum so validation step 4 passes */
}

uint64
cluster_qvotec_get_durable_self_incarnation(void)
{
	return stub_dedup_lifecycle_enabled ? UINT64CONST(101) : 0;
}

bool
cluster_qvotec_peer_boot_majority_exact(
	int32 peer pg_attribute_unused(), uint64 epoch pg_attribute_unused(),
	uint64 boot pg_attribute_unused(), uint64 *incarnation_out pg_attribute_unused())
{
	return stub_dedup_lifecycle_enabled;
}

bool
cluster_sf_peer_ges_dedup_done_capability_identity(
	int32 peer pg_attribute_unused(), uint32 *generation_out,
	uint64 *boot_out)
{
	if (generation_out)
		*generation_out = stub_dedup_lifecycle_enabled ? 7 : 0;
	if (boot_out)
		*boot_out = stub_dedup_lifecycle_enabled ? UINT64CONST(202) : 0;
	return stub_dedup_lifecycle_enabled;
}

/*
 * spec-5.7 Direction B link stubs — the REQUEST wait loop now consults CSSD
 * peer state + the liveness reclassify.  Default to "master alive / not
 * sole-native" so the dead-master native-safe abort is never taken in these
 * standalone GES tests (which exercise the steady-state grant/reject/retransmit
 * paths); the abort branch itself is covered by the extend-gate + TAP suites.
 */
ClusterCssdPeerState
cluster_cssd_get_peer_state(int32 peer_id pg_attribute_unused())
{
	return CLUSTER_CSSD_PEER_ALIVE;
}

bool
cluster_extend_liveness_is_sole_native(void)
{
	return false;
}

uint64
cluster_epoch_get_current(void)
{
	return 0; /* default epoch 0 — matches env_sentinel.epoch */
}

uint64
cluster_ges_reply_wait_next_request_id(void)
{
	static uint64 next_id = 1000;

	return ++next_id;
}

/* cluster_conf — return non-NULL so validation step 4 declared check
 * passes.  Signature must match cluster_conf.h (pulled in via
 * cluster_grd.h since spec-4.6). */
const ClusterNodeInfo *
cluster_conf_lookup_node(int32 node_id pg_attribute_unused())
{
	static ClusterNodeInfo dummy;
	return &dummy;
}

/* spec-4.6 D3/D4 stubs:  the drain path now reads the shard recovery
 * phase (master-side freeze gate) and handles GES_REDECLARE via the
 * insert-or-rebind entry API.  Standalone fixture:  NORMAL phase +
 * always-OK rebind. */
uint32
cluster_grd_shard_for_resource(const ClusterResId *resid pg_attribute_unused())
{
	return 0;
}

ClusterGrdShardPhase
cluster_grd_shard_phase(uint32 shard_id pg_attribute_unused())
{
	return GRD_SHARD_NORMAL;
}

/* spec-5.8 D1c stub:  cluster_ges.c REQUEST/CONVERT send fills the wire
 * waiter_xid from the backend's xid; the standalone fixture has no xact. */
TransactionId
GetTopTransactionIdIfAny(void)
{
	return InvalidTransactionId;
}

/* spec-5.9 D3 stub:  the GES wait loops consume a per-proc cancel token; the
 * token match logic is covered by test_cluster_cancel_token.  Here the fixture
 * never installs one, so consume always reports "no cancel". */
bool
cluster_cancel_token_consume(void)
{
	return false;
}

/* spec-4.6 P0#3 stub:  REDECLARE_DONE handler records peer barrier
 * completion.  Standalone fixture has no recovery shmem; no-op. */
void
cluster_grd_recovery_mark_peer_done(int32 node pg_attribute_unused(),
									uint64 epoch pg_attribute_unused(),
									uint64 event_id pg_attribute_unused())
{}

ClusterGrdEntryResult
cluster_grd_entry_rebind_or_insert_holder(const ClusterResId *resid pg_attribute_unused(),
										  const struct ClusterGrdHolderId *nh pg_attribute_unused(),
										  int32 src pg_attribute_unused(),
										  int lockmode pg_attribute_unused())
{
	return CLUSTER_GRD_ENTRY_OK;
}

ClusterGrdEntryResult
cluster_grd_entry_rebind_or_insert_holder_meta(
	const ClusterResId *resid pg_attribute_unused(),
	const struct ClusterGrdHolderId *nh pg_attribute_unused(),
	int32 src pg_attribute_unused(), int lockmode pg_attribute_unused(),
	uint64 boot pg_attribute_unused())
{
	return CLUSTER_GRD_ENTRY_OK;
}

static int32 stub_remote_master = -1;

int32
cluster_grd_lookup_master(const struct ClusterResId *resid pg_attribute_unused())
{
	return stub_remote_master >= 0 ? stub_remote_master : cluster_node_id;
}

void
cluster_grd_resid_encode(const LOCKTAG *src, struct ClusterResId *dst)
{
	memset(dst, 0, sizeof(*dst));
	if (src != NULL) {
		dst->type = src->locktag_type;
		dst->lockmethodid = src->locktag_lockmethodid;
	}
}

/* GRD inc helpers — stub bump local counters (test verifies via accessor) */
static uint64 stub_work_queue_full = 0;
static uint64 stub_inbound_validation_fail = 0;
static uint64 stub_cleanup_deferred = 0;
static uint64 stub_reply_deferred = 0;
static uint64 stub_reply_dropped = 0;
static uint64 stub_work_queue_enqueue_count = 0;
static uint64 stub_lmon_reply_enqueue_count = 0;
static uint64 stub_bast_received = 0;
static uint64 stub_lmd_cancel_enqueue_count = 0;
static uint32 stub_lmd_cancel_last_source = 0;
static uint64 stub_bast_ack = 0;
static uint64 stub_deadlock_probe_drop = 0;
static uint64 stub_backend_request_enqueue_count = 0;
static GesRequestPayload stub_backend_request_last;
static uint64 stub_backend_request_fail_on_call = 0;
static uint64 stub_cleanup_release_enqueue_count = 0;
static GesRequestPayload stub_cleanup_release_last;
static uint64 stub_cancel_wait_enqueue_count = 0;
static uint32 stub_cancel_wait_last_dest = 0;
static GesCancelWaitPayload stub_cancel_wait_last;
static int stub_release_drain_calls = 0;
static bool stub_release_drain_emit_once = false;
static ClusterGrdGrantIdentity stub_release_drain_grant;

void
cluster_grd_inc_ges_work_queue_full(void)
{
	stub_work_queue_full++;
}

/* spec-2.18 Sprint A Step 3 D9 L104 stub:  cluster_lms_wake_drain
 * broadcasts CV after successful work_queue enqueue. */
void cluster_lms_wake_drain(void);
void
cluster_lms_wake_drain(void)
{}

/*
 * spec-5.1c D1 stubs — cluster_ges_send_bast_targeted's local-delivery branch
 * references these backend primitives.  This test never drives a live local
 * holder (it does not call send_bast_targeted), so the stubs are inert and
 * link-only: ProcGlobal is never dereferenced, and SendProcSignal /
 * cluster_grd_bast_local_deliver_ok are never invoked.
 */
void *ProcGlobal = NULL;

int SendProcSignal(int pid, int reason, int backendid);
int
SendProcSignal(int pid pg_attribute_unused(), int reason pg_attribute_unused(),
			   int backendid pg_attribute_unused())
{
	return -1;
}

bool
cluster_grd_bast_local_deliver_ok(uint32 procno pg_attribute_unused(),
								  int proc_count pg_attribute_unused(),
								  uint64 holder_epoch pg_attribute_unused(),
								  uint64 current_epoch pg_attribute_unused(),
								  int target_pid pg_attribute_unused(),
								  int target_backendid pg_attribute_unused())
{
	return false;
}

/* spec-2.19 Sprint A Step 3 D8 L104 stubs:  cluster_lmd_is_ready (HC4
 * exact predicate) + cluster_lmd_submit_wait_edge (HC3 producer wake +
 * HC6 skeleton "no graph maintenance").  Called from cluster_ges.c
 * GES_REQ_OPCODE_DEADLOCK_PROBE handler. */
bool cluster_lmd_is_ready(void);
bool
cluster_lmd_is_ready(void)
{
	/* Standalone unit test:LMD shmem not attached, predicate false. */
	return false;
}
void cluster_lmd_submit_wait_edge(void);
void
cluster_lmd_submit_wait_edge(void)
{}
void
cluster_grd_inc_ges_inbound_validation_fail(void)
{
	stub_inbound_validation_fail++;
}
void
cluster_grd_inc_ges_cleanup_deferred(void)
{
	stub_cleanup_deferred++;
}
void
cluster_grd_inc_ges_reply_deferred(void)
{
	stub_reply_deferred++;
}
void
cluster_grd_inc_ges_reply_dropped(void)
{
	stub_reply_dropped++;
}
void
cluster_grd_inc_bast_received(void)
{
	stub_bast_received++;
}
void
cluster_grd_inc_bast_ack(void)
{
	stub_bast_ack++;
}
void
cluster_grd_inc_deadlock_probe_drop(void)
{
	stub_deadlock_probe_drop++;
}

/* work_queue + outbound enqueue stubs — accept always (no overflow path tested
 * at unit layer; TAP exercises overflow with real shmem). */
bool
cluster_grd_work_queue_enqueue(uint32 src pg_attribute_unused(),
							   const void *p pg_attribute_unused(), uint16 l pg_attribute_unused())
{
	stub_work_queue_enqueue_count++;
	return true;
}

bool
cluster_grd_work_queue_enqueue_identity(
	uint32 src pg_attribute_unused(), uint64 boot pg_attribute_unused(),
	const void *p pg_attribute_unused(), uint16 l pg_attribute_unused())
{
	stub_work_queue_enqueue_count++;
	return true;
}

bool
cluster_grd_work_queue_dequeue(ClusterGrdWorkItem *out pg_attribute_unused())
{
	return false;
}

void
cluster_grd_outbound_enqueue_lmon_reply(uint32 d pg_attribute_unused(),
										const void *p pg_attribute_unused(),
										uint16 l pg_attribute_unused())
{
	stub_lmon_reply_enqueue_count++;
}

void
cluster_grd_outbound_enqueue_ges_dedup_lifecycle(
	uint8 mt pg_attribute_unused(), uint32 d pg_attribute_unused(),
	const void *p, uint16 l,
	uint32 gen pg_attribute_unused())
{
	stub_dedup_lifecycle_enqueue_count++;
	if (p != NULL && l == sizeof(GesDedupLifecyclePayload))
		memcpy(&stub_dedup_lifecycle_last, p,
			   sizeof(stub_dedup_lifecycle_last));
}
/* spec-5.16 orphan-grant auto-release uses the cleanup-release producer. */
void
cluster_grd_outbound_enqueue_cleanup_release(uint32 d pg_attribute_unused(),
											 const void *p, uint16 l)
{
	stub_cleanup_release_enqueue_count++;
	if (p != NULL && l == sizeof(GesRequestPayload))
		memcpy(&stub_cleanup_release_last, p,
			   sizeof(stub_cleanup_release_last));
}

/* spec-2.23 D14 R13 stub audit — new symbol surface introduced by
 * Steps 1-9 needs file-local stubs so cluster_ges.o links standalone
 * in this test binary.  All stubs are minimal inert bodies; behavior
 * coverage lives in TAP 108/109 where real backend wiring runs. */

bool
cluster_grd_outbound_enqueue_backend_request(uint32 d pg_attribute_unused(), const void *p,
											 uint16 l)
{
	stub_backend_request_enqueue_count++;
	if (p != NULL && l == sizeof(GesRequestPayload))
		memcpy(&stub_backend_request_last, p, sizeof(stub_backend_request_last));
	if (stub_backend_request_fail_on_call != 0
		&& stub_backend_request_enqueue_count
			   == stub_backend_request_fail_on_call)
		return false;
	return true;
}

bool
cluster_grd_outbound_enqueue_backend_request_capability(
	uint32 d, const void *p, uint16 l,
	uint32 required_capability pg_attribute_unused(),
	uint32 capability_generation pg_attribute_unused())
{
	return cluster_grd_outbound_enqueue_backend_request(d, p, l);
}

/* spec-5.8 D8 — REPORT send-back deps newly referenced by cluster_ges.o.  The
 * standalone harness does not exercise the deadlock probe path (TAP 109 / the
 * 2-node TAP do); these only resolve at link time. */
int cluster_lmd_max_wait_edges = 1024;

/* spec-5.8 D8 — victim cancel flag referenced by the GES wait loops. */
volatile sig_atomic_t cluster_ges_cancel_pending = false;

ClusterICSendResult
cluster_ic_send_envelope(uint8 mt pg_attribute_unused(), int32 dest pg_attribute_unused(),
						 const void *p pg_attribute_unused(), uint32 len pg_attribute_unused())
{
	return CLUSTER_IC_SEND_DONE;
}

void *
palloc(Size sz)
{
	return malloc(sz);
}

void
pfree(void *p)
{
	free(p);
}

/* spec-2.24 D14 stub audit. */
void
cluster_grd_outbound_enqueue_lmd_cancel(uint32 d, const void *p, uint16 l)
{
	if (p != NULL && l == sizeof(GesCancelWaitPayload)
		&& ((const GesCancelWaitPayload *)p)->opcode == GES_REQ_OPCODE_CANCEL_WAIT) {
		stub_cancel_wait_enqueue_count++;
		stub_cancel_wait_last_dest = d;
		memcpy(&stub_cancel_wait_last, p, sizeof(stub_cancel_wait_last));
	}
}
bool
cluster_lmd_cancel_queue_enqueue(uint32 s, const void *p pg_attribute_unused(),
								 uint16 l pg_attribute_unused())
{
	stub_lmd_cancel_enqueue_count++;
	stub_lmd_cancel_last_source = s;
	return true;
}
void
cluster_lmd_cross_node_cancel_queue_full_count_inc(uint64 d pg_attribute_unused())
{}
void
cluster_lmd_cross_node_cancel_received_count_inc(uint64 d pg_attribute_unused())
{}
void
cluster_lmd_cross_node_victim_cancel_sent_count_inc(uint64 d pg_attribute_unused())
{}

void
cluster_grd_inc_bast_sent(void)
{}

/* spec-5.1c D1 — local BAST delivery bumps stale_drop on a guard miss. */
void
cluster_grd_inc_bast_stale_drop(void)
{}

/* spec-2.25 D14 R10 stub audit — native-lock probe 3 NEW symbols.
 * Real behavior tested in test_cluster_native_lock_probe.c (Step 11
 * forward-link).  Here we only need link-surface satisfaction +
 * call counters for T-ges-14..16 dispatch assertions.
 *
 * NOTE:  cannot #include cluster_native_lock_probe.h here because it
 * pulls in cluster_grd.h which conflicts with the local opaque stubs
 * for cluster_grd_entry_enqueue_or_grant et al.  Forward-declare the
 * probe enum + use struct ClusterGrdHolderId opaque (typedef'd in
 * cluster_grd.h but compatible with opaque struct here per C tag/
 * typedef rules — function pointer compares correctly at link time
 * because all callers see the same struct tag). */
static int stub_native_probe_local_calls;
static int stub_native_probe_recv_calls;
static int stub_native_probe_outbound_calls;
static uint32 stub_native_probe_outbound_last_dest;
static uint16 stub_native_probe_outbound_last_len;

/* Forward decls matching real prototypes in
 * src/include/cluster/cluster_native_lock_probe.h. */
typedef enum ClusterNativeLockProbeReply {
	CLUSTER_NATIVE_LOCK_PROBE_CLEAR_LOCAL = 0,
} ClusterNativeLockProbeReplyLocal;

int
cluster_native_lock_probe_local(const void *locktag pg_attribute_unused(),
								int lockmode pg_attribute_unused(),
								const struct ClusterGrdHolderId *eh pg_attribute_unused())
{
	stub_native_probe_local_calls++;
	return CLUSTER_NATIVE_LOCK_PROBE_CLEAR_LOCAL;
}

void
cluster_lms_native_probe_recv_reply(uint64 probe_id pg_attribute_unused(),
									int32 sender pg_attribute_unused(),
									int status pg_attribute_unused())
{
	stub_native_probe_recv_calls++;
}

/* spec-2.27 D2 / D7 R10 stub audit. */
int cluster_ges_retransmit_max_attempts = 5;
int cluster_ges_dedup_max_entries = 8192;
static uint64 stub_shard_master_generation = 0;

uint64
cluster_lms_get_shard_master_generation(void)
{
	return stub_shard_master_generation;
}

void
cluster_lms_inc_priority_starvation_observed(void)
{}

ClusterGesDedupLookupStatus
cluster_ges_dedup_lookup_or_register(const ClusterGesDedupKey *key pg_attribute_unused(),
									 uint8 *reply_out pg_attribute_unused(),
									 uint16 reply_buf_len pg_attribute_unused(),
									 uint16 *reply_len_out pg_attribute_unused())
{
	if (reply_len_out)
		*reply_len_out = 0;
	return 0; /* CLUSTER_GES_DEDUP_MISS_REGISTERED */
}

ClusterGesDedupLookupStatus
cluster_ges_dedup_lookup_or_register_identity(
	const ClusterGesDedupKey *key pg_attribute_unused(),
	uint64 boot pg_attribute_unused(), uint8 *reply_out pg_attribute_unused(),
	uint16 reply_buf_len pg_attribute_unused(),
	uint16 *reply_len_out pg_attribute_unused())
{
	if (reply_len_out)
		*reply_len_out = 0;
	return CLUSTER_GES_DEDUP_MISS_REGISTERED;
}

ClusterGesDedupLookupStatus
cluster_ges_dedup_lookup_or_register_identity_ex(
	const ClusterGesDedupKey *key pg_attribute_unused(),
	uint64 boot pg_attribute_unused(), uint64 *superseded_boot_out,
	uint8 *reply_out pg_attribute_unused(),
	uint16 reply_buf_len pg_attribute_unused(),
	uint16 *reply_len_out)
{
	if (superseded_boot_out)
		*superseded_boot_out = stub_lookup_superseded_boot;
	if (reply_len_out)
		*reply_len_out = 0;
	return CLUSTER_GES_DEDUP_MISS_REGISTERED;
}

bool
cluster_ges_dedup_request_is_retired(
	const ClusterGesDedupKey *key pg_attribute_unused(),
	uint64 boot pg_attribute_unused())
{
	return false;
}

void
cluster_ges_dedup_record_reply(const ClusterGesDedupKey *key pg_attribute_unused(),
							   const uint8 *reply pg_attribute_unused(),
							   uint16 reply_len pg_attribute_unused())
{}

void
cluster_ges_dedup_record_reply_identity(
	const ClusterGesDedupKey *key, uint64 boot,
	const uint8 *reply pg_attribute_unused(), uint16 reply_len)
{
	stub_dedup_record_identity_count++;
	if (key != NULL)
		stub_dedup_record_identity_key = *key;
	stub_dedup_record_identity_boot = boot;
	stub_dedup_record_identity_reply_len = reply_len;
}

ClusterGesDedupRetireResult
cluster_ges_dedup_retire_exact(
	const ClusterGesDedupKey *key pg_attribute_unused(),
	uint64 boot pg_attribute_unused())
{
	return CLUSTER_GES_DEDUP_RETIRE_ALREADY_ABSENT;
}

uint32
cluster_ges_dedup_retire_origin_proc_up_to(
	uint32 origin pg_attribute_unused(), uint32 procno pg_attribute_unused(),
	uint64 hwm pg_attribute_unused(), uint64 boot pg_attribute_unused(),
	uint32 *pending_out, bool *applied_out)
{
	if (pending_out)
		*pending_out = stub_dedup_retire_pending;
	if (applied_out)
		*applied_out = true;
	return 0;
}

bool
cluster_ges_dedup_journal_register(
	uint32 dest pg_attribute_unused(),
	const GesDedupLifecyclePayload *done pg_attribute_unused())
{
	stub_dedup_journal_register_count++;
	return stub_dedup_journal_register_success;
}

bool
cluster_ges_dedup_journal_commit(
	uint32 dest pg_attribute_unused(),
	const GesDedupLifecyclePayload *done pg_attribute_unused())
{
	stub_dedup_journal_commit_count++;
	return true;
}

bool
cluster_ges_dedup_journal_cancel(
	uint32 dest pg_attribute_unused(),
	const GesDedupLifecyclePayload *done pg_attribute_unused())
{
	return true;
}

bool
cluster_ges_dedup_journal_claim_due(
	TimestampTz now pg_attribute_unused(), uint32 *dest pg_attribute_unused(),
	GesDedupLifecyclePayload *done pg_attribute_unused())
{
	return false;
}

bool
cluster_ges_dedup_journal_ack(
	uint32 source pg_attribute_unused(),
	const GesDedupLifecyclePayload *ack pg_attribute_unused())
{
	return true;
}

uint32
cluster_ges_dedup_journal_drop_target_boot_mismatch(
	uint32 dest pg_attribute_unused(), uint64 boot pg_attribute_unused())
{
	return 0;
}

uint32
cluster_ges_dedup_journal_reap_dead_backend(void)
{
	return 0;
}

bool
cluster_lms_native_probe_required(const struct ClusterResId *resid pg_attribute_unused(),
								  int lockmode pg_attribute_unused())
{
	return false;
}

bool
cluster_lms_native_probe_schedule_grant_identity(
	const struct ClusterResId *resid pg_attribute_unused(),
	int lockmode pg_attribute_unused(),
	const struct ClusterGrdHolderId *requester pg_attribute_unused(),
	int32 source_node_id pg_attribute_unused(),
	uint32 request_opcode pg_attribute_unused(),
	uint64 shard_master_generation pg_attribute_unused(),
	int convert_current_mode pg_attribute_unused(),
	ClusterGrdWaiterMeta waiter_meta pg_attribute_unused())
{
	return false;
}

bool
cluster_lms_native_probe_schedule_grant(
	const struct ClusterResId *resid pg_attribute_unused(), int lockmode pg_attribute_unused(),
	const struct ClusterGrdHolderId *requester pg_attribute_unused(),
	int32 source_node_id pg_attribute_unused(), uint32 request_opcode pg_attribute_unused(),
	uint64 shard_master_generation pg_attribute_unused(),
	int convert_current_mode pg_attribute_unused())
{
	return false;
}

/* spec-5.3 — stubs for the convert wrappers + sync native probe (cluster_ges.c
 * references them; this harness exercises the dispatch/dedup paths, not the
 * convert decision, which is covered by test_cluster_grd). */
bool
cluster_lms_native_probe_wait_clear(
	const struct ClusterResId *resid pg_attribute_unused(), int lockmode pg_attribute_unused(),
	const struct ClusterGrdHolderId *requester pg_attribute_unused(),
	int timeout_ms pg_attribute_unused())
{
	return true;
}

ClusterGrdConvertResult
cluster_grd_convert_or_enqueue(
	const struct ClusterResId *resid pg_attribute_unused(), int32 node_id pg_attribute_unused(),
	uint32 procno pg_attribute_unused(), uint64 cluster_epoch pg_attribute_unused(),
	int current_mode pg_attribute_unused(), int requested_mode pg_attribute_unused(),
	uint64 convert_request_id pg_attribute_unused(), int32 source_node_id pg_attribute_unused(),
	uint64 shard_master_generation pg_attribute_unused(),
	ClusterGrdConflictHolder *conflict_holders_out pg_attribute_unused(),
	int *n_conflict_out pg_attribute_unused())
{
	return CLUSTER_GRD_CONVERT_NOT_READY;
}

/* spec-5.8 D1c/D1e — waiter-metadata variant stub. */
ClusterGrdConvertResult
cluster_grd_convert_or_enqueue_meta(
	const struct ClusterResId *resid pg_attribute_unused(), int32 node_id pg_attribute_unused(),
	uint32 procno pg_attribute_unused(), uint64 cluster_epoch pg_attribute_unused(),
	int current_mode pg_attribute_unused(), int requested_mode pg_attribute_unused(),
	uint64 convert_request_id pg_attribute_unused(), int32 source_node_id pg_attribute_unused(),
	uint64 shard_master_generation pg_attribute_unused(),
	ClusterGrdWaiterMeta meta pg_attribute_unused(),
	ClusterGrdConflictHolder *conflict_holders_out pg_attribute_unused(),
	int *n_conflict_out pg_attribute_unused())
{
	return CLUSTER_GRD_CONVERT_NOT_READY;
}

int
cluster_grd_release_and_drain(const struct ClusterResId *resid pg_attribute_unused(),
							  const struct ClusterGrdHolderId *holder pg_attribute_unused(),
							  ClusterGrdGrantIdentity *granted_out,
							  int max_out pg_attribute_unused())
{
	stub_release_drain_calls++;
	if (stub_release_drain_emit_once) {
		stub_release_drain_emit_once = false;
		*granted_out = stub_release_drain_grant;
		return 1;
	}
	return 0;
}

void
cluster_grd_retire_origin_proc_up_to(
	uint32 origin_node_id pg_attribute_unused(),
	uint64 origin_boot_incarnation pg_attribute_unused(),
	uint32 holder_procno pg_attribute_unused(),
	uint64 request_id_hwm pg_attribute_unused(),
	ClusterGrdRetireGrantCallback grant_cb pg_attribute_unused(),
	void *grant_cb_arg pg_attribute_unused(),
	ClusterGrdRetireStats *stats_out)
{
	stub_grd_retire_proc_count++;
	if (stats_out)
		memset(stats_out, 0, sizeof(*stats_out));
}

void
cluster_grd_retire_origin_boot(
	uint32 origin_node_id pg_attribute_unused(),
	uint64 origin_boot_incarnation pg_attribute_unused(),
	ClusterGrdRetireGrantCallback grant_cb pg_attribute_unused(),
	void *grant_cb_arg pg_attribute_unused(),
	ClusterGrdRetireStats *stats_out)
{
	stub_grd_retire_boot_count++;
	stub_grd_retire_boot_origin = origin_node_id;
	stub_grd_retire_boot_value = origin_boot_incarnation;
	if (stats_out)
		memset(stats_out, 0, sizeof(*stats_out));
}

ClusterGrdEntryResult
cluster_grd_rollback_convert(const struct ClusterResId *resid pg_attribute_unused(),
							 int32 node_id pg_attribute_unused(),
							 uint32 procno pg_attribute_unused(),
							 int upgraded_mode pg_attribute_unused(),
							 int old_mode pg_attribute_unused(),
							 uint64 old_request_id pg_attribute_unused(),
							 uint64 convert_request_id pg_attribute_unused())
{
	return CLUSTER_GRD_ENTRY_NOT_FOUND;
}

int cluster_ges_convert_timeout_ms = 30000;

void
cluster_grd_outbound_enqueue_lms_native_probe(uint32 dest, const void *p pg_attribute_unused(),
											  uint16 len)
{
	stub_native_probe_outbound_calls++;
	stub_native_probe_outbound_last_dest = dest;
	stub_native_probe_outbound_last_len = len;
}

/* cluster_ges_reply_wait API stubs (spec-2.23 D1). */
static bool stub_reply_wait_insert_enabled = false;
static GesReplyWaitEntry stub_reply_wait_entry;

GesReplyWaitEntry *
cluster_ges_reply_wait_insert(const GesReplyWaitKey *k pg_attribute_unused(),
							  TimestampTz deadline pg_attribute_unused())
{
	if (!stub_reply_wait_insert_enabled)
		return NULL;
	memset(&stub_reply_wait_entry, 0, sizeof(stub_reply_wait_entry));
	return &stub_reply_wait_entry;
}
GesReplyWaitEntry *
cluster_ges_reply_wait_lookup(const GesReplyWaitKey *k pg_attribute_unused())
{
	return NULL;
}
void
cluster_ges_reply_wait_wake(GesReplyWaitEntry *e pg_attribute_unused(),
							uint32 opcode pg_attribute_unused(),
							uint32 reason pg_attribute_unused())
{}
void
cluster_ges_reply_wait_delete(const GesReplyWaitKey *k pg_attribute_unused())
{}
/* spec-5.16 orphan-grant stubs (deliver / mark_abandoned). */
GesReplyDeliverResult
cluster_ges_reply_wait_deliver(const GesReplyWaitKey *k pg_attribute_unused(),
							   uint32 opcode pg_attribute_unused(),
							   uint32 reason pg_attribute_unused())
{
	return GES_REPLY_DELIVER_NO_WAITER;
}
bool
cluster_ges_reply_wait_mark_abandoned(const GesReplyWaitKey *k pg_attribute_unused(),
									  TimestampTz tombstone_deadline pg_attribute_unused())
{
	return false;
}
void
cluster_ges_inc_release_ack(void)
{}
void
cluster_ges_inc_reply_late_drop(void)
{}

/* cluster_lmd probe collector receive (spec-2.23 D8). */
struct GesDeadlockReportHeader;
bool
cluster_lmd_probe_collect_receive(const struct GesDeadlockReportHeader *r pg_attribute_unused(),
								  Size len pg_attribute_unused())
{
	return false;
}

/* cluster_grd GRD-owned waiter API (spec-2.23 D6). */
struct ClusterGrdConflictHolder;
struct ClusterGrdWaiterIdentity;
ClusterGrdGrantAction
cluster_grd_entry_enqueue_or_grant(const struct ClusterResId *r pg_attribute_unused(),
								   const struct ClusterGrdHolderId *h pg_attribute_unused(),
								   int32 src pg_attribute_unused(),
								   uint64 req_id pg_attribute_unused(),
								   uint64 shard_master_generation pg_attribute_unused(),
								   uint32 op pg_attribute_unused(), int mode pg_attribute_unused(),
								   struct ClusterGrdConflictHolder *out pg_attribute_unused(),
								   int *nout pg_attribute_unused())
{
	if (nout != NULL)
		*nout = 0;
	return CLUSTER_GRD_GRANT_NOW;
}
/* spec-5.5 D5 — conditional (NOWAIT) variant stub (mirror: grant, no conflict). */
ClusterGrdGrantAction
cluster_grd_entry_grant_conditional(const struct ClusterResId *r pg_attribute_unused(),
									const struct ClusterGrdHolderId *h pg_attribute_unused(),
									int32 src pg_attribute_unused(),
									uint64 req_id pg_attribute_unused(),
									uint64 shard_master_generation pg_attribute_unused(),
									uint32 op pg_attribute_unused(), int mode pg_attribute_unused(),
									struct ClusterGrdConflictHolder *out pg_attribute_unused(),
									int *nout pg_attribute_unused())
{
	if (nout != NULL)
		*nout = 0;
	return CLUSTER_GRD_GRANT_NOW;
}
/* spec-5.8 D1c/D1e — waiter-metadata variant stubs. */
ClusterGrdGrantAction
cluster_grd_entry_enqueue_or_grant_meta(
	const struct ClusterResId *r pg_attribute_unused(),
	const struct ClusterGrdHolderId *h pg_attribute_unused(), int32 src pg_attribute_unused(),
	uint64 req_id pg_attribute_unused(), ClusterGrdWaiterMeta meta pg_attribute_unused(),
	uint64 shard_master_generation pg_attribute_unused(), uint32 op pg_attribute_unused(),
	int mode pg_attribute_unused(), struct ClusterGrdConflictHolder *out pg_attribute_unused(),
	int *nout pg_attribute_unused())
{
	if (nout != NULL)
		*nout = 0;
	return CLUSTER_GRD_GRANT_NOW;
}
ClusterGrdGrantAction
cluster_grd_entry_grant_conditional_meta(
	const struct ClusterResId *r pg_attribute_unused(),
	const struct ClusterGrdHolderId *h pg_attribute_unused(), int32 src pg_attribute_unused(),
	uint64 req_id pg_attribute_unused(), ClusterGrdWaiterMeta meta pg_attribute_unused(),
	uint64 shard_master_generation pg_attribute_unused(), uint32 op pg_attribute_unused(),
	int mode pg_attribute_unused(), struct ClusterGrdConflictHolder *out pg_attribute_unused(),
	int *nout pg_attribute_unused())
{
	if (nout != NULL)
		*nout = 0;
	return CLUSTER_GRD_GRANT_NOW;
}
int
cluster_grd_entry_release_and_pop_compatible_waiter(
	const struct ClusterResId *r pg_attribute_unused(),
	const struct ClusterGrdHolderId *h pg_attribute_unused(),
	struct ClusterGrdWaiterIdentity *out pg_attribute_unused(), int max_out pg_attribute_unused())
{
	return 0;
}

ClusterGrdEntryResult
cluster_grd_release_holder_by_id(const struct ClusterResId *r pg_attribute_unused(),
								 const struct ClusterGrdHolderId *h pg_attribute_unused())
{
	return CLUSTER_GRD_ENTRY_OK;
}

ClusterGrdEntryResult
cluster_grd_cancel_waiter_by_id(const struct ClusterResId *r pg_attribute_unused(),
								const struct ClusterGrdHolderId *h pg_attribute_unused())
{
	return CLUSTER_GRD_ENTRY_NOT_FOUND;
}

/* spec-5.9 D4 stubs — the CANCEL_WAIT handler dequeues via these; the real
 * dequeue is covered by test_cluster_grd / the 2-node TAP. */
static ClusterGrdEntryResult stub_cancel_wait_result
	= CLUSTER_GRD_ENTRY_NOT_FOUND;
static ClusterGrdWaiterIdentity stub_cancel_wait_removed;
static uint64 stub_cancel_request_identity_calls;
static uint64 stub_cancel_convert_identity_calls;

ClusterGrdEntryResult
cluster_grd_cancel_waiter_by_id_seq(const struct ClusterResId *r pg_attribute_unused(),
									const struct ClusterGrdHolderId *h pg_attribute_unused(),
									uint64 ws pg_attribute_unused())
{
	return CLUSTER_GRD_ENTRY_NOT_FOUND;
}

ClusterGrdEntryResult
cluster_grd_cancel_waiter_by_id_seq_identity(
	const struct ClusterResId *r pg_attribute_unused(),
	const struct ClusterGrdHolderId *h pg_attribute_unused(),
	uint64 ws pg_attribute_unused(),
	struct ClusterGrdWaiterIdentity *removed_out)
{
	stub_cancel_request_identity_calls++;
	if (removed_out != NULL)
		*removed_out = stub_cancel_wait_removed;
	return stub_cancel_wait_result;
}

ClusterGrdEntryResult
cluster_grd_cancel_convert_by_id(const struct ClusterResId *r pg_attribute_unused(),
								 const struct ClusterGrdHolderId *h pg_attribute_unused(),
								 uint64 ws pg_attribute_unused())
{
	return CLUSTER_GRD_ENTRY_NOT_FOUND;
}

ClusterGrdEntryResult
cluster_grd_cancel_convert_by_id_identity(
	const struct ClusterResId *r pg_attribute_unused(),
	const struct ClusterGrdHolderId *h pg_attribute_unused(),
	uint64 ws pg_attribute_unused(),
	struct ClusterGrdWaiterIdentity *removed_out)
{
	stub_cancel_convert_identity_calls++;
	if (removed_out != NULL)
		*removed_out = stub_cancel_wait_removed;
	return stub_cancel_wait_result;
}

void
cluster_lmd_cancel_wait_stale_rejected_count_inc(uint64 d pg_attribute_unused())
{}

void
cluster_lmd_cancel_ack_received_count_inc(uint64 d pg_attribute_unused())
{}

/* GUC + PG runtime stubs. */
int cluster_ges_request_timeout_ms = 60000;

/* spec-5.59 D2 stubs: cluster_ges.o now carries GUC-gated profiling probes
 * (cluster_xnode_profile.h); the unit harness links neither cluster_guc.o
 * nor cluster_xnode_profile.o, so define the two gate symbols inertly
 * (probes early-return on enabled=false / Ctl=NULL). */
bool cluster_xnode_profile_enabled = false;
ClusterXnodeProfileShared *ClusterXnodeProfileCtl = NULL;

/* CHECK_FOR_INTERRUPTS() in the local-master wait loop. */
volatile sig_atomic_t InterruptPending = false;

void
ProcessInterrupts(void)
{}

/* PG_TRY/PG_CATCH machinery referenced by the local-master conflict wait loop.
 * The wait path is never exercised by these tests (no cross-node enqueue), so
 * these only need to satisfy the standalone link. */
sigjmp_buf *PG_exception_stack = NULL;
ErrorContextCallback *error_context_stack = NULL;
static bool stub_cv_throws_error = false;

void
pg_re_throw(void)
{
	if (stub_cv_throws_error && PG_exception_stack != NULL)
		siglongjmp(*PG_exception_stack, 1);
	abort();
}

bool
DoLockModesConflict(int a pg_attribute_unused(), int b pg_attribute_unused())
{
	return false;
}

static bool stub_clock_advances = false;
static TimestampTz stub_now = 0;
static bool stub_cv_timed_sleep_returns = true;

TimestampTz
GetCurrentTimestamp(void)
{
	if (stub_clock_advances)
		stub_now += 1000;
	return stub_now;
}

void *MyProc;

#include "storage/condition_variable.h"
void
ConditionVariablePrepareToSleep(ConditionVariable *cv pg_attribute_unused())
{}
bool
ConditionVariableCancelSleep(void)
{
	return false;
}
bool
ConditionVariableTimedSleep(ConditionVariable *cv pg_attribute_unused(),
							long timeout pg_attribute_unused(),
							uint32 wait_event pg_attribute_unused())
{
	if (stub_cv_throws_error && PG_exception_stack != NULL)
		siglongjmp(*PG_exception_stack, 1);
	return stub_cv_timed_sleep_returns;
}


/* ============================================================
 * T-ges-1 a/b/c/d/e (spec-2.13 D6 Q5.2).
 * ============================================================ */

UT_TEST(test_ges_request_handler_linkable)
{
	UT_ASSERT_NOT_NULL((void *)cluster_ges_request_handler);
}

UT_TEST(test_ges_reply_handler_linkable)
{
	UT_ASSERT_NOT_NULL((void *)cluster_ges_reply_handler);
}

UT_TEST(test_ges_accessors_linkable_and_initial_zero)
{
	cluster_ges_shmem_init();

	UT_ASSERT_NOT_NULL((void *)cluster_ges_request_defer_count);
	UT_ASSERT_NOT_NULL((void *)cluster_ges_reply_defer_count);

	UT_ASSERT_EQ(cluster_ges_request_defer_count(), (uint64)0);
	UT_ASSERT_EQ(cluster_ges_reply_defer_count(), (uint64)0);
}

UT_TEST(test_ges_request_handler_real_behavior)
{
	ClusterICEnvelope env_sentinel;
	uint64 pre_request;
	uint64 pre_reply;

	cluster_ges_shmem_init();

	memset(&env_sentinel, 0, sizeof(env_sentinel));
	env_sentinel.source_node_id = 7; /* sentinel non-zero peer id */

	pre_request = cluster_ges_request_defer_count();
	pre_reply = cluster_ges_reply_defer_count();

	/* Invoke stub — must return silently without ERROR/FATAL. */
	cluster_ges_request_handler(&env_sentinel, NULL);

	/* (1) handler returned (would not get here on FATAL abort).
	 * (2) request counter +1.
	 * (3) reply counter unchanged. */
	UT_ASSERT_EQ(cluster_ges_request_defer_count(), pre_request + 1);
	UT_ASSERT_EQ(cluster_ges_reply_defer_count(), pre_reply);
}

UT_TEST(test_ges_reply_handler_real_behavior)
{
	ClusterICEnvelope env_sentinel;
	uint64 pre_request;
	uint64 pre_reply;

	cluster_ges_shmem_init();

	memset(&env_sentinel, 0, sizeof(env_sentinel));
	env_sentinel.source_node_id = 11;

	pre_request = cluster_ges_request_defer_count();
	pre_reply = cluster_ges_reply_defer_count();

	cluster_ges_reply_handler(&env_sentinel, NULL);

	UT_ASSERT_EQ(cluster_ges_reply_defer_count(), pre_reply + 1);
	UT_ASSERT_EQ(cluster_ges_request_defer_count(), pre_request);
}

UT_TEST(test_ges_handler_counter_monotonic_n_invocations)
{
	ClusterICEnvelope env_sentinel;
	uint64 pre_request;
	uint64 pre_reply;
	const int N = 7;
	int i;

	cluster_ges_shmem_init();

	memset(&env_sentinel, 0, sizeof(env_sentinel));
	env_sentinel.source_node_id = 3;

	pre_request = cluster_ges_request_defer_count();
	pre_reply = cluster_ges_reply_defer_count();

	for (i = 0; i < N; i++)
		cluster_ges_request_handler(&env_sentinel, NULL);

	for (i = 0; i < N; i++)
		cluster_ges_reply_handler(&env_sentinel, NULL);

	UT_ASSERT_EQ(cluster_ges_request_defer_count(), pre_request + (uint64)N);
	UT_ASSERT_EQ(cluster_ges_reply_defer_count(), pre_reply + (uint64)N);
}

UT_TEST(test_ges_request_valid_payload_enqueues_work)
{
	ClusterICEnvelope env;
	GesRequestPayload req;
	uint64 pre_fail = stub_inbound_validation_fail;
	uint64 pre_enqueue = stub_work_queue_enqueue_count;

	cluster_ges_shmem_init();
	cluster_node_id = 0;
	memset(&env, 0, sizeof(env));
	env.source_node_id = 1;
	env.epoch = 0;
	env.payload_length = sizeof(GesRequestPayload);

	memset(&req, 0, sizeof(req));
	req.opcode = GES_REQ_OPCODE_REQUEST;
	req.holder_node_id = 1;

	cluster_ges_request_handler(&env, &req);

	UT_ASSERT_EQ(stub_inbound_validation_fail, pre_fail);
	UT_ASSERT_EQ(stub_work_queue_enqueue_count, pre_enqueue + 1);
}

UT_TEST(test_ges_reply_valid_payload_echoes_local_holder)
{
	ClusterICEnvelope env;
	GesReplyPayload rep;
	uint64 pre_fail = stub_inbound_validation_fail;
	uint64 pre_reply = cluster_ges_reply_defer_count();

	cluster_ges_shmem_init();
	cluster_node_id = 0;
	memset(&env, 0, sizeof(env));
	env.source_node_id = 1;
	env.epoch = 0;

	memset(&rep, 0, sizeof(rep));
	rep.opcode = GES_REPLY_OPCODE_REJECT;
	rep.reject_reason = GES_REJECT_REASON_LOCK_CONFLICT;
	rep.holder_node_id = 0;

	cluster_ges_reply_handler(&env, &rep);

	UT_ASSERT_EQ(stub_inbound_validation_fail, pre_fail);
	UT_ASSERT_EQ(cluster_ges_reply_defer_count(), pre_reply + 1);
}

UT_TEST(test_ges_duplicate_grant_without_waiter_does_not_release_live_holder)
{
	ClusterICEnvelope env;
	GesReplyPayload rep;
	uint64 pre_cleanup = stub_cleanup_release_enqueue_count;

	cluster_ges_shmem_init();
	cluster_node_id = 0;
	memset(&env, 0, sizeof(env));
	env.source_node_id = 1;
	env.epoch = 0;
	env.payload_length = sizeof(rep);
	memset(&rep, 0, sizeof(rep));
	rep.opcode = GES_REPLY_OPCODE_GRANT;
	rep.reply_for_opcode = GES_REQ_OPCODE_REQUEST;
	rep.holder_node_id = 0;
	rep.holder_procno = 17;
	rep.holder_request_id_lo = 99;

	/* Fixture deliver() returns NO_WAITER: this is indistinguishable from
	 * a retransmitted GRANT after the successful waiter was deleted. */
	cluster_ges_reply_handler(&env, &rep);
	UT_ASSERT_EQ(stub_cleanup_release_enqueue_count, pre_cleanup);
}

UT_TEST(test_ges_hwm_ack_waits_for_inflight_and_grd_retirement)
{
	ClusterICEnvelope env;
	GesDedupLifecyclePayload hwm;

	cluster_ges_shmem_init();
	cluster_node_id = 0;
	stub_dedup_lifecycle_enabled = true;
	memset(&env, 0, sizeof(env));
	env.source_node_id = 1;
	env.epoch = 0;
	env.payload_length = sizeof(hwm);
	memset(&hwm, 0, sizeof(hwm));
	hwm.version = GES_DEDUP_LIFECYCLE_VERSION;
	hwm.kind = GES_DEDUP_LIFECYCLE_PROC_EXIT_HWM;
	hwm.origin_node_id = 1;
	hwm.holder_procno = 17;
	hwm.request_id = 55;
	hwm.origin_boot_incarnation = UINT64CONST(202);
	hwm.target_boot_incarnation = UINT64CONST(101);
	hwm.link_generation = 7;

	stub_dedup_lifecycle_enqueue_count = 0;
	stub_grd_retire_proc_count = 0;
	stub_dedup_retire_pending = 1;
	cluster_ges_dedup_done_handler(&env, &hwm);
	UT_ASSERT_EQ(stub_grd_retire_proc_count, (uint64)1);
	UT_ASSERT_EQ(stub_dedup_lifecycle_enqueue_count, (uint64)0);

	stub_dedup_retire_pending = 0;
	cluster_ges_dedup_done_handler(&env, &hwm);
	UT_ASSERT_EQ(stub_grd_retire_proc_count, (uint64)2);
	UT_ASSERT_EQ(stub_dedup_lifecycle_enqueue_count, (uint64)1);
	UT_ASSERT_EQ(stub_dedup_lifecycle_last.kind,
				 (uint8)GES_DEDUP_LIFECYCLE_ACK);
	UT_ASSERT_EQ(stub_dedup_lifecycle_last.status,
				 (uint8)GES_DEDUP_ACK_HWM_APPLIED);

	stub_dedup_lifecycle_enabled = false;
}

UT_TEST(test_ges_authenticated_boot_switch_retires_old_grd_boot)
{
	ClusterICEnvelope env;
	GesRequestPayload req;

	cluster_ges_shmem_init();
	cluster_node_id = 0;
	stub_dedup_lifecycle_enabled = true;
	stub_lookup_superseded_boot = UINT64CONST(303);
	stub_grd_retire_boot_count = 0;
	stub_work_queue_enqueue_count = 0;
	memset(&env, 0, sizeof(env));
	env.source_node_id = 1;
	env.epoch = 0;
	env.payload_length = sizeof(req);
	memset(&req, 0, sizeof(req));
	req.opcode = GES_REQ_OPCODE_REQUEST;
	req.holder_node_id = 1;
	req.holder_procno = 17;
	req.holder_request_id_lo = 1;
	req.shard_master_generation_lo = 1;

	cluster_ges_request_handler(&env, &req);

	UT_ASSERT_EQ(stub_grd_retire_boot_count, (uint64)1);
	UT_ASSERT_EQ(stub_grd_retire_boot_origin, (uint32)1);
	UT_ASSERT_EQ(stub_grd_retire_boot_value, UINT64CONST(303));
	UT_ASSERT_EQ(stub_work_queue_enqueue_count, (uint64)1);

	stub_lookup_superseded_boot = 0;
	stub_dedup_lifecycle_enabled = false;
}

/*
 * P0#10 crash/recovery regression: a queued REQUEST retains its authenticated
 * origin boot.  If that node reconnects under a newer boot before the waiter
 * is promoted, the master must undo the promoted stale holder instead of
 * sending a GRANT that no post-restart reply-wait HTAB can own.  A grant for
 * the currently authenticated boot must still be delivered normally.
 */
UT_TEST(test_ges_drain_drops_grant_from_superseded_origin_boot)
{
	ClusterResId resid;
	ClusterGrdHolderId releasing;
	uint64 pre_reply;

	cluster_ges_shmem_init();
	cluster_node_id = 0;
	stub_dedup_lifecycle_enabled = true;
	memset(&resid, 0xA5, sizeof(resid));
	memset(&releasing, 0, sizeof(releasing));
	memset(&stub_release_drain_grant, 0,
		   sizeof(stub_release_drain_grant));
	stub_release_drain_grant.source_node_id = 1;
	stub_release_drain_grant.holder.node_id = 1;
	stub_release_drain_grant.holder.procno = 17;
	stub_release_drain_grant.holder.cluster_epoch = 0;
	stub_release_drain_grant.holder.request_id
		= UINT64CONST(0x1122334455667788);
	stub_release_drain_grant.request_opcode = GES_REQ_OPCODE_REQUEST;
	stub_release_drain_grant.origin_boot_incarnation = UINT64CONST(201);

	pre_reply = stub_lmon_reply_enqueue_count;
	stub_release_drain_calls = 0;
	stub_release_drain_emit_once = true;
	cluster_ges_release_and_drain_local(&resid, &releasing);

	/* First drain promotes stale waiter; recursive exact release drains again. */
	UT_ASSERT_EQ(stub_release_drain_calls, 2);
	UT_ASSERT_EQ(stub_lmon_reply_enqueue_count, pre_reply);

	/* The current authenticated boot (fixture value 202) remains deliverable. */
	stub_release_drain_grant.origin_boot_incarnation = UINT64CONST(202);
	stub_release_drain_calls = 0;
	stub_release_drain_emit_once = true;
	cluster_ges_release_and_drain_local(&resid, &releasing);
	UT_ASSERT_EQ(stub_release_drain_calls, 1);
	UT_ASSERT_EQ(stub_lmon_reply_enqueue_count, pre_reply + 1);

	stub_dedup_lifecycle_enabled = false;
}

UT_TEST(test_ges_lmon_drain_work_queue_symbol_linkable)
{
	UT_ASSERT_NOT_NULL((void *)cluster_ges_lmon_drain_work_queue);
}


UT_DEFINE_GLOBALS();

/* spec-5.14 D2: link-only stub.  This test exercises the GES enqueue logic,
 * not the touched_peers bitmap (covered by test_cluster_touched_peers + TAP). */
bool
cluster_touched_peers_stamp(int32 node_id pg_attribute_unused(),
							ClusterTouchKind kind pg_attribute_unused())
{
	return false;
}


/* spec-2.17 Step 2 — NEW T-ges-3 opcode + payload size invariant. */
static void
test_ges_opcode_enum_spec_2_17_extension(void)
{
	/* spec-2.17 Q5 v0.6:  7 opcode 全集 — BAST=4 / BAST_ACK=5 /
	 * DEADLOCK_PROBE=6 / CANCEL_PENDING=7. */
	UT_ASSERT_EQ((int)GES_REQ_OPCODE_REQUEST, 1);
	UT_ASSERT_EQ((int)GES_REQ_OPCODE_CONVERT, 2);
	UT_ASSERT_EQ((int)GES_REQ_OPCODE_RELEASE, 3);
	UT_ASSERT_EQ((int)GES_REQ_OPCODE_BAST, 4);
	UT_ASSERT_EQ((int)GES_REQ_OPCODE_BAST_ACK, 5);
	UT_ASSERT_EQ((int)GES_REQ_OPCODE_DEADLOCK_PROBE, 6);
	UT_ASSERT_EQ((int)GES_REQ_OPCODE_CANCEL_PENDING, 7);
}

static void
test_ges_bast_opcode_validates_as_target_local(void)
{
	ClusterICEnvelope env;
	GesRequestPayload req;
	uint64 pre_fail = stub_inbound_validation_fail;
	uint64 pre_bast = stub_bast_received;
	uint64 pre_enqueue = stub_work_queue_enqueue_count;

	cluster_ges_shmem_init();
	cluster_node_id = 0;
	memset(&env, 0, sizeof(env));
	env.source_node_id = 1; /* master */
	env.epoch = 0;
	env.payload_length = sizeof(GesRequestPayload);

	memset(&req, 0, sizeof(req));
	req.opcode = GES_REQ_OPCODE_BAST;
	req.holder_node_id = 0; /* local target, not envelope source */

	cluster_ges_request_handler(&env, &req);

	UT_ASSERT_EQ(stub_inbound_validation_fail, pre_fail);
	UT_ASSERT_EQ(stub_bast_received, pre_bast + 1);
	UT_ASSERT_EQ(stub_work_queue_enqueue_count, pre_enqueue);
}

static void
test_ges_cancel_pending_opcode_validates_as_target_local(void)
{
	ClusterICEnvelope env;
	GesRequestPayload req;
	uint64 pre_fail = stub_inbound_validation_fail;
	uint64 pre_cancel = stub_lmd_cancel_enqueue_count;

	cluster_ges_shmem_init();
	cluster_node_id = 0;
	memset(&env, 0, sizeof(env));
	env.source_node_id = 1; /* coordinator */
	env.epoch = 0;
	env.payload_length = sizeof(GesRequestPayload);

	memset(&req, 0, sizeof(req));
	req.opcode = GES_REQ_OPCODE_CANCEL_PENDING;
	req.holder_node_id = 0; /* local victim target, not envelope source */

	cluster_ges_request_handler(&env, &req);

	UT_ASSERT_EQ(stub_inbound_validation_fail, pre_fail);
	UT_ASSERT_EQ(stub_lmd_cancel_enqueue_count, pre_cancel + 1);
	UT_ASSERT_EQ(stub_lmd_cancel_last_source, (uint32)1);
}

static void
test_ges_bast_ack_opcode_validates_as_source_holder(void)
{
	ClusterICEnvelope env;
	GesRequestPayload req;
	uint64 pre_fail = stub_inbound_validation_fail;
	uint64 pre_ack = stub_bast_ack;
	uint64 pre_enqueue = stub_work_queue_enqueue_count;

	cluster_ges_shmem_init();
	cluster_node_id = 0;
	memset(&env, 0, sizeof(env));
	env.source_node_id = 1; /* holder */
	env.epoch = 0;
	env.payload_length = sizeof(GesRequestPayload);

	memset(&req, 0, sizeof(req));
	req.opcode = GES_REQ_OPCODE_BAST_ACK;
	req.holder_node_id = 1; /* holder must match envelope source */

	cluster_ges_request_handler(&env, &req);

	UT_ASSERT_EQ(stub_inbound_validation_fail, pre_fail);
	UT_ASSERT_EQ(stub_bast_ack, pre_ack + 1);
	UT_ASSERT_EQ(stub_work_queue_enqueue_count, pre_enqueue);
}

/* ============================================================
 * spec-2.25 T-ges-14..16 — NATIVE_LOCK_PROBE opcode dispatch tests.
 *
 *	T-ges-14:  opcode enum values 9 / 10 + opcode_max = 10 boundary.
 *	T-ges-15:  request handler dispatches to local probe + outbound reply
 *	           (HC33 source/target/sender validation prefix passes).
 *	T-ges-16:  reply handler routes status to LMS collector (recv_reply
 *	           counter increments;  HC36 stale drop deferred to TAP).
 * ============================================================ */

UT_TEST(test_ges_native_lock_probe_opcode_enum_extension)
{
	/* spec-2.25 D6:  opcode 9 + 10 ABI lock. */
	UT_ASSERT_EQ((int)GES_REQ_OPCODE_NATIVE_LOCK_PROBE, 9);
	UT_ASSERT_EQ((int)GES_REQ_OPCODE_NATIVE_LOCK_PROBE_REPLY, 10);
	/* Payload size lock.  spec-5.3 grew the probe REQUEST to 40B (+requester
	 * node/procno identity, so the peer can exclude the original requester's own
	 * sub-SUEX holds from the conflict scan);  the REPLY stays 32B. */
	UT_ASSERT_EQ((int)sizeof(GesNativeLockProbePayload), 40);
	UT_ASSERT_EQ((int)sizeof(GesNativeLockProbeReplyPayload), 32);
}

UT_TEST(test_ges_native_lock_probe_request_dispatch)
{
	/* spec-2.25 D5:  request handler probes local + emits reply. */
	GesNativeLockProbePayload probe;
	ClusterICEnvelope env;
	int pre_local = stub_native_probe_local_calls;
	int pre_outbound = stub_native_probe_outbound_calls;

	memset(&env, 0, sizeof(env));
	env.source_node_id = 7;
	env.epoch = 0;
	env.payload_length = sizeof(probe);

	memset(&probe, 0, sizeof(probe));
	probe.opcode = GES_REQ_OPCODE_NATIVE_LOCK_PROBE;
	probe.lockmode = 5;
	probe.probe_id = 0xDEADBEEFCAFEBABEull;

	cluster_ges_handle_native_lock_probe_request(&env, &probe);

	UT_ASSERT_EQ(stub_native_probe_local_calls, pre_local + 1);
	UT_ASSERT_EQ(stub_native_probe_outbound_calls, pre_outbound + 1);
	UT_ASSERT_EQ(stub_native_probe_outbound_last_dest, 7u); /* echo source */
	UT_ASSERT_EQ((int)stub_native_probe_outbound_last_len, 32);
}

UT_TEST(test_ges_native_lock_probe_reply_dispatch)
{
	/* spec-2.25 D5:  reply handler routes to LMS collector. */
	GesNativeLockProbeReplyPayload reply;
	ClusterICEnvelope env;
	int pre_recv = stub_native_probe_recv_calls;

	memset(&env, 0, sizeof(env));
	env.source_node_id = 3;
	env.epoch = 0;
	env.payload_length = sizeof(reply);

	memset(&reply, 0, sizeof(reply));
	reply.opcode = GES_REQ_OPCODE_NATIVE_LOCK_PROBE_REPLY;
	reply.status = 0; /* CLUSTER_NATIVE_LOCK_PROBE_CLEAR — value 0 */
	reply.probe_id = 0xAAAAAAAA00000001ull;
	reply.sender_node_id = 3; /* HC33 dual-source: == env.source_node_id */

	cluster_ges_handle_native_lock_probe_reply(&env, &reply);

	UT_ASSERT_EQ(stub_native_probe_recv_calls, pre_recv + 1);
}

UT_TEST(test_cancel_wait_success_terminalizes_exact_receiver_dedup)
{
	ClusterICEnvelope env;
	GesCancelWaitPayload cancel;
	uint64 request_calls;
	uint64 convert_calls;
	uint64 validation_fail;

	cluster_ges_shmem_init();
	cluster_node_id = 0;
	memset(&env, 0, sizeof(env));
	env.source_node_id = 9; /* coordinator need not host the waiter */
	env.payload_length = sizeof(cancel);
	memset(&cancel, 0, sizeof(cancel));
	cancel.opcode = GES_REQ_OPCODE_CANCEL_WAIT;
	cancel.kind = GES_CANCEL_WAIT_KIND_REQUEST;
	cancel.waiter_node_id = 4;
	cancel.waiter_procno = 17;
	cancel.waiter_cluster_epoch = UINT64CONST(23);
	cancel.waiter_request_id = UINT64CONST(99);
	cancel.wait_seq = UINT64CONST(7);
	memset(&stub_cancel_wait_removed, 0,
		   sizeof(stub_cancel_wait_removed));
	stub_cancel_wait_removed.holder.node_id = 4;
	stub_cancel_wait_removed.holder.procno = 17;
	stub_cancel_wait_removed.holder.cluster_epoch = UINT64CONST(23);
	stub_cancel_wait_removed.holder.request_id = UINT64CONST(99);
	stub_cancel_wait_removed.source_node_id = 4;
	stub_cancel_wait_removed.request_id = UINT64CONST(99);
	stub_cancel_wait_removed.shard_master_generation = UINT64CONST(77);
	stub_cancel_wait_removed.origin_boot_incarnation = UINT64CONST(101);
	stub_cancel_wait_removed.request_opcode = GES_REQ_OPCODE_REQUEST;
	stub_cancel_wait_result = CLUSTER_GRD_ENTRY_OK;
	stub_cancel_request_identity_calls = 0;
	stub_cancel_convert_identity_calls = 0;
	stub_dedup_record_identity_count = 0;
	memset(&stub_dedup_record_identity_key, 0,
		   sizeof(stub_dedup_record_identity_key));

	cluster_ges_request_handler(&env, &cancel);

	UT_ASSERT_EQ(stub_cancel_request_identity_calls, UINT64CONST(1));
	UT_ASSERT_EQ(stub_cancel_convert_identity_calls, UINT64CONST(0));
	UT_ASSERT_EQ(stub_dedup_record_identity_count, UINT64CONST(1));
	UT_ASSERT_EQ(stub_dedup_record_identity_key.origin_node_id,
				 (uint32)4);
	UT_ASSERT_EQ(stub_dedup_record_identity_key.opcode,
				 (uint32)GES_REQ_OPCODE_REQUEST);
	UT_ASSERT_EQ(stub_dedup_record_identity_key.request_id,
				 UINT64CONST(99));
	UT_ASSERT_EQ(stub_dedup_record_identity_key.cluster_epoch,
				 UINT64CONST(23));
	UT_ASSERT_EQ(
		stub_dedup_record_identity_key.shard_master_generation,
		UINT64CONST(77));
	UT_ASSERT_EQ(stub_dedup_record_identity_key.holder_procno,
				 (uint32)17);
	UT_ASSERT_EQ(stub_dedup_record_identity_boot, UINT64CONST(101));
	UT_ASSERT_EQ(stub_dedup_record_identity_reply_len, (uint16)0);

	/* A stale/missed cancellation may have raced a real GRANT.  It must
	 * never terminalize the dedup row for that live holder. */
	stub_cancel_wait_result = CLUSTER_GRD_ENTRY_NOT_FOUND;
	stub_dedup_record_identity_count = 0;
	cluster_ges_request_handler(&env, &cancel);
	UT_ASSERT_EQ(stub_dedup_record_identity_count, UINT64CONST(0));

	/* CONVERT success terminalizes only the exact convert cache key. */
	cancel.kind = GES_CANCEL_WAIT_KIND_CONVERT;
	stub_cancel_wait_removed.request_opcode = GES_REQ_OPCODE_CONVERT;
	stub_cancel_wait_removed.shard_master_generation = UINT64CONST(78);
	stub_cancel_wait_removed.origin_boot_incarnation = UINT64CONST(102);
	stub_cancel_wait_result = CLUSTER_GRD_ENTRY_OK;
	stub_dedup_record_identity_count = 0;
	request_calls = stub_cancel_request_identity_calls;
	convert_calls = stub_cancel_convert_identity_calls;
	cluster_ges_request_handler(&env, &cancel);
	UT_ASSERT_EQ(stub_cancel_request_identity_calls, request_calls);
	UT_ASSERT_EQ(stub_cancel_convert_identity_calls, convert_calls + 1);
	UT_ASSERT_EQ(stub_dedup_record_identity_count, UINT64CONST(1));
	UT_ASSERT_EQ(stub_dedup_record_identity_key.opcode,
				 (uint32)GES_REQ_OPCODE_CONVERT);
	UT_ASSERT_EQ(
		stub_dedup_record_identity_key.shard_master_generation,
		UINT64CONST(78));
	UT_ASSERT_EQ(stub_dedup_record_identity_boot, UINT64CONST(102));
	UT_ASSERT_EQ(stub_dedup_record_identity_reply_len, (uint16)0);

	/* NOT_FOUND may leave a poisoned output buffer in a buggy/mock primitive;
	 * the handler must not publish anything unless the return code is OK. */
	stub_cancel_wait_result = CLUSTER_GRD_ENTRY_NOT_FOUND;
	stub_cancel_wait_removed.request_opcode = GES_REQ_OPCODE_REQUEST;
	stub_dedup_record_identity_count = 0;
	cluster_ges_request_handler(&env, &cancel);
	UT_ASSERT_EQ(stub_dedup_record_identity_count, UINT64CONST(0));

	/* Unknown wire kinds fail validation before either GRD queue mutates. */
	cancel.kind = UINT8_MAX;
	request_calls = stub_cancel_request_identity_calls;
	convert_calls = stub_cancel_convert_identity_calls;
	validation_fail = stub_inbound_validation_fail;
	cluster_ges_request_handler(&env, &cancel);
	UT_ASSERT_EQ(stub_inbound_validation_fail, validation_fail + 1);
	UT_ASSERT_EQ(stub_cancel_request_identity_calls, request_calls);
	UT_ASSERT_EQ(stub_cancel_convert_identity_calls, convert_calls);
	UT_ASSERT_EQ(stub_dedup_record_identity_count, UINT64CONST(0));

	stub_cancel_wait_result = CLUSTER_GRD_ENTRY_NOT_FOUND;
	memset(&stub_cancel_wait_removed, 0,
		   sizeof(stub_cancel_wait_removed));
}

/*
 * P0#3 regression: a finite remote REQUEST timeout must remove the exact
 * master-side waiter.  The deadlock-victim path already sends CANCEL_WAIT;
 * the ordinary timeout path historically left the waiter + WFG edge behind.
 */
UT_TEST(test_ges_request_timeout_sends_wait_seq_exact_cancel_wait)
{
	ClusterResId resid;
	ClusterGrdHolderId holder;
	uint32 result;

	memset(&resid, 0x5A, sizeof(resid));
	memset(&holder, 0, sizeof(holder));
	holder.node_id = 0;
	holder.procno = 17;
	holder.cluster_epoch = 0;
	holder.request_id = UINT64CONST(0x1122334455667788);

	stub_remote_master = 7;
	stub_reply_wait_insert_enabled = true;
	stub_clock_advances = true;
	stub_now = 0;
	stub_backend_request_enqueue_count = 0;
	stub_cancel_wait_enqueue_count = 0;
	memset(&stub_backend_request_last, 0, sizeof(stub_backend_request_last));
	memset(&stub_cancel_wait_last, 0, sizeof(stub_cancel_wait_last));

	result = cluster_ges_send_request_and_wait(&resid, AccessExclusiveLock, &holder,
											   holder.request_id, 1, 0);

	UT_ASSERT_EQ(result, (uint32)GES_REJECT_REASON_TIMEOUT);
	UT_ASSERT_EQ(stub_backend_request_enqueue_count, (uint64)1);
	UT_ASSERT_EQ(stub_cancel_wait_enqueue_count, (uint64)1);
	UT_ASSERT_EQ(stub_cancel_wait_last_dest, (uint32)7);
	UT_ASSERT_EQ(stub_cancel_wait_last.opcode, (uint32)GES_REQ_OPCODE_CANCEL_WAIT);
	UT_ASSERT_EQ(stub_cancel_wait_last.kind, (uint32)GES_CANCEL_WAIT_KIND_REQUEST);
	UT_ASSERT_EQ(stub_cancel_wait_last.waiter_node_id, holder.node_id);
	UT_ASSERT_EQ(stub_cancel_wait_last.waiter_procno, holder.procno);
	UT_ASSERT_EQ(stub_cancel_wait_last.waiter_cluster_epoch, holder.cluster_epoch);
	UT_ASSERT_EQ(stub_cancel_wait_last.waiter_request_id, holder.request_id);
	UT_ASSERT_EQ(stub_cancel_wait_last.wait_seq, stub_backend_request_last.wait_seq);
	UT_ASSERT(memcmp(stub_cancel_wait_last.resid, &resid, sizeof(resid)) == 0);

	stub_remote_master = -1;
	stub_reply_wait_insert_enabled = false;
	stub_clock_advances = false;
	stub_now = 0;
}

UT_TEST(test_ges_release_timeout_stages_dedup_bypass_cleanup)
{
	ClusterResId resid;
	ClusterGrdHolderId holder;
	uint32 result;
	int saved_timeout = cluster_ges_request_timeout_ms;

	memset(&resid, 0x6B, sizeof(resid));
	memset(&holder, 0, sizeof(holder));
	holder.node_id = 0;
	holder.procno = 17;
	holder.cluster_epoch = 0;
	holder.request_id = UINT64CONST(0x2233445566778899);

	stub_remote_master = 7;
	stub_reply_wait_insert_enabled = true;
	stub_clock_advances = true;
	stub_now = 0;
	stub_backend_request_enqueue_count = 0;
	stub_cleanup_release_enqueue_count = 0;
	memset(&stub_cleanup_release_last, 0,
		   sizeof(stub_cleanup_release_last));
	cluster_ges_request_timeout_ms = 1;

	result = cluster_ges_send_release_and_wait(
		&resid, &holder, holder.request_id);

	UT_ASSERT_EQ(result, (uint32)GES_REJECT_REASON_TIMEOUT);
	UT_ASSERT_EQ(stub_backend_request_enqueue_count, (uint64)1);
	UT_ASSERT_EQ(stub_cleanup_release_enqueue_count, (uint64)1);
	UT_ASSERT_EQ(stub_cleanup_release_last.opcode,
				 (uint32)GES_REQ_OPCODE_RELEASE);
	UT_ASSERT_EQ(stub_cleanup_release_last.holder_request_id_lo,
				 (uint32)(holder.request_id & UINT64CONST(0xffffffff)));
	UT_ASSERT_EQ(stub_cleanup_release_last.holder_request_id_hi,
				 (uint32)(holder.request_id >> 32));

	cluster_ges_request_timeout_ms = saved_timeout;
	stub_remote_master = -1;
	stub_reply_wait_insert_enabled = false;
	stub_clock_advances = false;
	stub_now = 0;
}

static void
assert_lifecycle_timeout_result(uint32 result, uint32 expected_opcode)
{
	UT_ASSERT_EQ(result, (uint32)GES_REJECT_REASON_TIMEOUT);
	UT_ASSERT_EQ(stub_dedup_journal_register_count, (uint64)1);
	UT_ASSERT_EQ(stub_dedup_journal_commit_count, (uint64)1);
	UT_ASSERT_EQ(stub_dedup_lifecycle_enqueue_count, (uint64)1);
	UT_ASSERT_EQ(stub_dedup_lifecycle_last.kind,
				 (uint8)GES_DEDUP_LIFECYCLE_EXACT_DONE);
	UT_ASSERT_EQ(stub_dedup_lifecycle_last.opcode, expected_opcode);
	UT_ASSERT_EQ(stub_dedup_lifecycle_last.origin_boot_incarnation,
				 UINT64CONST(101));
	UT_ASSERT_EQ(stub_dedup_lifecycle_last.target_boot_incarnation,
				 UINT64CONST(202));
}

static void
reset_lifecycle_timeout_case(void)
{
	stub_now = 0;
	stub_dedup_journal_register_count = 0;
	stub_dedup_journal_commit_count = 0;
	stub_dedup_lifecycle_enqueue_count = 0;
	memset(&stub_dedup_lifecycle_last, 0,
		   sizeof(stub_dedup_lifecycle_last));
}

UT_TEST(test_five_cached_opcodes_terminalize_lifecycle_on_timeout)
{
	ClusterResId resid;
	ClusterGrdHolderId holder;
	uint64 request_id = UINT64CONST(0x3344556677889900);
	int saved_timeout = cluster_ges_request_timeout_ms;

	memset(&resid, 0x7C, sizeof(resid));
	memset(&holder, 0, sizeof(holder));
	holder.node_id = 0;
	holder.procno = 17;
	holder.cluster_epoch = 9;
	holder.request_id = request_id;

	stub_remote_master = 7;
	stub_reply_wait_insert_enabled = true;
	stub_clock_advances = true;
	stub_dedup_lifecycle_enabled = true;
	stub_shard_master_generation = 77;
	cluster_ges_request_timeout_ms = 1;

	reset_lifecycle_timeout_case();
	assert_lifecycle_timeout_result(
		cluster_ges_send_request_and_wait(
			&resid, AccessExclusiveLock, &holder, request_id, 1, 0),
		GES_REQ_OPCODE_REQUEST);

	reset_lifecycle_timeout_case();
	assert_lifecycle_timeout_result(
		cluster_ges_send_request_nowait_and_wait(
			&resid, AccessExclusiveLock, &holder, request_id + 1, 1, 0),
		GES_REQ_OPCODE_REQUEST_NOWAIT);

	reset_lifecycle_timeout_case();
	assert_lifecycle_timeout_result(
		cluster_ges_send_redeclare_and_wait(
			&resid, AccessExclusiveLock, &holder, request_id + 2),
		GES_REQ_OPCODE_REDECLARE);

	reset_lifecycle_timeout_case();
	assert_lifecycle_timeout_result(
		cluster_ges_send_release_and_wait(
			&resid, &holder, request_id),
		GES_REQ_OPCODE_RELEASE);

	reset_lifecycle_timeout_case();
	assert_lifecycle_timeout_result(
		cluster_ges_send_convert_and_wait(
			&resid, AccessExclusiveLock, ShareLock, &holder,
			request_id, request_id + 3, 1),
		GES_REQ_OPCODE_CONVERT);

	cluster_ges_request_timeout_ms = saved_timeout;
	stub_remote_master = -1;
	stub_reply_wait_insert_enabled = false;
	stub_clock_advances = false;
	stub_dedup_lifecycle_enabled = false;
	stub_shard_master_generation = 0;
	stub_now = 0;
}

/*
 * S3-P0-10 grant-wins cancellation: the CONVERT holder carries R_new, but
 * rollback must restore the pre-convert holder's distinct R_old.  Force the
 * real query-cancel PG_CATCH boundary and inspect the actual opcode-14 payload.
 */
UT_TEST(test_convert_cancel_rollback_restores_distinct_old_request_id)
{
	ClusterResId resid;
	ClusterGrdHolderId holder;
	const uint64 old_request_id = UINT64CONST(0x1111222233334444);
	const uint64 convert_request_id = UINT64CONST(0xaaaabbbbccccdddd);
	uint64 rollback_request_id;
	volatile bool caught = false;

	memset(&resid, 0xA7, sizeof(resid));
	memset(&holder, 0, sizeof(holder));
	holder.node_id = 0;
	holder.procno = 17;
	holder.cluster_epoch = 9;
	holder.request_id = convert_request_id;

	stub_remote_master = 7;
	stub_reply_wait_insert_enabled = true;
	stub_clock_advances = false;
	stub_dedup_lifecycle_enabled = false;
	stub_backend_request_enqueue_count = 0;
	stub_cancel_wait_enqueue_count = 0;
	stub_cv_throws_error = true;

	PG_TRY();
	{
		(void)cluster_ges_send_convert_and_wait(
			&resid, AccessExclusiveLock, ShareLock, &holder,
			old_request_id, convert_request_id, 1000);
	}
	PG_CATCH();
	{
		caught = true;
	}
	PG_END_TRY();

	stub_cv_throws_error = false;
	rollback_request_id
		= ((uint64)stub_backend_request_last.holder_request_id_lo)
		  | (((uint64)stub_backend_request_last.holder_request_id_hi) << 32);
	UT_ASSERT(caught);
	UT_ASSERT_EQ(stub_backend_request_enqueue_count, (uint64)2);
	UT_ASSERT_EQ(stub_cancel_wait_enqueue_count, (uint64)1);
	UT_ASSERT_EQ(stub_backend_request_last.opcode,
				 (uint32)GES_REQ_OPCODE_CONVERT_ROLLBACK);
	UT_ASSERT_EQ(stub_backend_request_last.wait_seq, convert_request_id);
	/* Old code fails here with actual=R_new (0xaaaabbbbccccdddd),
	 * expected=R_old (0x1111222233334444). */
	UT_ASSERT_EQ(rollback_request_id, old_request_id);
	UT_ASSERT_NE(rollback_request_id, convert_request_id);

	stub_remote_master = -1;
	stub_reply_wait_insert_enabled = false;
	stub_backend_request_enqueue_count = 0;
	stub_cancel_wait_enqueue_count = 0;
}

UT_TEST(test_convert_zero_old_request_id_fails_before_send)
{
	ClusterResId resid;
	ClusterGrdHolderId holder;
	const uint64 convert_request_id = UINT64CONST(0x123456789abcdef0);
	uint32 result;

	memset(&resid, 0x31, sizeof(resid));
	memset(&holder, 0, sizeof(holder));
	holder.node_id = 0;
	holder.procno = 18;
	holder.cluster_epoch = 10;
	holder.request_id = convert_request_id;

	stub_remote_master = 7;
	stub_reply_wait_insert_enabled = true;
	stub_clock_advances = true;
	stub_now = 0;
	stub_dedup_lifecycle_enabled = false;
	stub_backend_request_enqueue_count = 0;
	stub_cancel_wait_enqueue_count = 0;

	result = cluster_ges_send_convert_and_wait(
		&resid, AccessExclusiveLock, ShareLock, &holder,
		0, convert_request_id, 1);

	UT_ASSERT_EQ(result, (uint32)GES_REJECT_REASON_ILLEGAL_CONVERT);
	UT_ASSERT_EQ(stub_backend_request_enqueue_count, (uint64)0);
	UT_ASSERT_EQ(stub_cancel_wait_enqueue_count, (uint64)0);

	stub_remote_master = -1;
	stub_reply_wait_insert_enabled = false;
	stub_clock_advances = false;
	stub_now = 0;
}

UT_TEST(test_convert_equal_old_and_new_request_id_fails_before_send)
{
	ClusterResId resid;
	ClusterGrdHolderId holder;
	const uint64 request_id = UINT64CONST(0x23456789abcdef01);
	uint32 result;

	memset(&resid, 0x32, sizeof(resid));
	memset(&holder, 0, sizeof(holder));
	holder.node_id = 0;
	holder.procno = 19;
	holder.cluster_epoch = 11;
	holder.request_id = request_id;

	stub_remote_master = 7;
	stub_reply_wait_insert_enabled = true;
	stub_clock_advances = true;
	stub_now = 0;
	stub_dedup_lifecycle_enabled = false;
	stub_backend_request_enqueue_count = 0;
	stub_cancel_wait_enqueue_count = 0;

	result = cluster_ges_send_convert_and_wait(
		&resid, AccessExclusiveLock, ShareLock, &holder,
		request_id, request_id, 1);

	UT_ASSERT_EQ(result, (uint32)GES_REJECT_REASON_ILLEGAL_CONVERT);
	UT_ASSERT_EQ(stub_backend_request_enqueue_count, (uint64)0);
	UT_ASSERT_EQ(stub_cancel_wait_enqueue_count, (uint64)0);

	stub_remote_master = -1;
	stub_reply_wait_insert_enabled = false;
	stub_clock_advances = false;
	stub_now = 0;
}

UT_TEST(test_request_retransmit_ring_full_cancels_wait_before_done)
{
	ClusterResId resid;
	ClusterGrdHolderId holder;
	uint64 request_id = UINT64CONST(0x4455667788990011);
	uint32 result;

	memset(&resid, 0x8D, sizeof(resid));
	memset(&holder, 0, sizeof(holder));
	holder.node_id = 0;
	holder.procno = 17;
	holder.cluster_epoch = 9;
	holder.request_id = request_id;

	stub_remote_master = 7;
	stub_reply_wait_insert_enabled = true;
	stub_clock_advances = true;
	stub_cv_timed_sleep_returns = false;
	stub_dedup_lifecycle_enabled = true;
	stub_shard_master_generation = 77;
	stub_now = 0;
	stub_backend_request_enqueue_count = 0;
	stub_backend_request_fail_on_call = 2;
	stub_cancel_wait_enqueue_count = 0;
	reset_lifecycle_timeout_case();

	result = cluster_ges_send_request_and_wait(
		&resid, AccessExclusiveLock, &holder, request_id, 100, 0);

	UT_ASSERT_EQ(result, (uint32)GES_REJECT_REASON_WORK_QUEUE_FULL);
	UT_ASSERT_EQ(stub_backend_request_enqueue_count, (uint64)2);
	UT_ASSERT_EQ(stub_cancel_wait_enqueue_count, (uint64)1);
	UT_ASSERT_EQ(stub_dedup_journal_commit_count, (uint64)1);
	UT_ASSERT_EQ(stub_dedup_lifecycle_enqueue_count, (uint64)1);
	UT_ASSERT_EQ(stub_dedup_lifecycle_last.opcode,
				 (uint32)GES_REQ_OPCODE_REQUEST);

	stub_remote_master = -1;
	stub_reply_wait_insert_enabled = false;
	stub_clock_advances = false;
	stub_cv_timed_sleep_returns = true;
	stub_dedup_lifecycle_enabled = false;
	stub_shard_master_generation = 0;
	stub_backend_request_fail_on_call = 0;
	stub_now = 0;
}

UT_TEST(test_journal_full_refuses_first_request_before_ring)
{
	ClusterResId resid;
	ClusterGrdHolderId holder;
	uint64 request_id = UINT64CONST(0x5566778899001122);
	uint32 result;

	memset(&resid, 0x9E, sizeof(resid));
	memset(&holder, 0, sizeof(holder));
	holder.node_id = 0;
	holder.procno = 17;
	holder.cluster_epoch = 9;
	holder.request_id = request_id;

	stub_remote_master = 7;
	stub_reply_wait_insert_enabled = true;
	stub_dedup_lifecycle_enabled = true;
	stub_shard_master_generation = 77;
	stub_dedup_journal_register_success = false;
	stub_backend_request_enqueue_count = 0;
	reset_lifecycle_timeout_case();

	result = cluster_ges_send_request_and_wait(
		&resid, AccessExclusiveLock, &holder, request_id, 100, 0);

	UT_ASSERT_EQ(result, (uint32)GES_REJECT_REASON_WORK_QUEUE_FULL);
	UT_ASSERT_EQ(stub_dedup_journal_register_count, (uint64)1);
	UT_ASSERT_EQ(stub_backend_request_enqueue_count, (uint64)0);
	UT_ASSERT_EQ(stub_dedup_journal_commit_count, (uint64)0);
	UT_ASSERT_EQ(stub_dedup_lifecycle_enqueue_count, (uint64)0);

	stub_remote_master = -1;
	stub_reply_wait_insert_enabled = false;
	stub_dedup_lifecycle_enabled = false;
	stub_shard_master_generation = 0;
	stub_dedup_journal_register_success = true;
}

int
main(int argc pg_attribute_unused(), char *argv[] pg_attribute_unused())
{
	UT_PLAN(29);

	UT_RUN(test_ges_request_handler_linkable);
	UT_RUN(test_ges_reply_handler_linkable);
	UT_RUN(test_ges_accessors_linkable_and_initial_zero);
	UT_RUN(test_ges_request_handler_real_behavior);
	UT_RUN(test_ges_reply_handler_real_behavior);
	UT_RUN(test_ges_handler_counter_monotonic_n_invocations);
	UT_RUN(test_ges_request_valid_payload_enqueues_work);
	UT_RUN(test_ges_reply_valid_payload_echoes_local_holder);
	UT_RUN(test_ges_duplicate_grant_without_waiter_does_not_release_live_holder);
	UT_RUN(test_ges_hwm_ack_waits_for_inflight_and_grd_retirement);
	UT_RUN(test_ges_authenticated_boot_switch_retires_old_grd_boot);
	UT_RUN(test_ges_drain_drops_grant_from_superseded_origin_boot);
	UT_RUN(test_ges_lmon_drain_work_queue_symbol_linkable);
	UT_RUN(test_ges_opcode_enum_spec_2_17_extension);
	UT_RUN(test_ges_bast_opcode_validates_as_target_local);
	UT_RUN(test_ges_cancel_pending_opcode_validates_as_target_local);
	UT_RUN(test_ges_bast_ack_opcode_validates_as_source_holder);
	UT_RUN(test_ges_native_lock_probe_opcode_enum_extension);
	UT_RUN(test_ges_native_lock_probe_request_dispatch);
	UT_RUN(test_ges_native_lock_probe_reply_dispatch);
	UT_RUN(test_cancel_wait_success_terminalizes_exact_receiver_dedup);
	UT_RUN(test_ges_request_timeout_sends_wait_seq_exact_cancel_wait);
	UT_RUN(test_ges_release_timeout_stages_dedup_bypass_cleanup);
	UT_RUN(test_five_cached_opcodes_terminalize_lifecycle_on_timeout);
	UT_RUN(test_convert_cancel_rollback_restores_distinct_old_request_id);
	UT_RUN(test_convert_zero_old_request_id_fails_before_send);
	UT_RUN(test_convert_equal_old_and_new_request_id_fails_before_send);
	UT_RUN(test_request_retransmit_ring_full_cancels_wait_before_done);
	UT_RUN(test_journal_full_refuses_first_request_before_ring);

	UT_DONE();
	return ut_failed_count == 0 ? 0 : 1;
}
