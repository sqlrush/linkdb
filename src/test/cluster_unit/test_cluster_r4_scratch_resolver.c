/*-------------------------------------------------------------------------
 *
 * test_cluster_r4_scratch_resolver.c
 *	  Unit37 receipt for the real exact-ref visibility resolver.
 *
 * The test links a function-sectioned cluster_visibility_resolve.c and drives
 * its peer-origin, still-bound ITL-ref path into an exact-key memo hit.  Only
 * the backend-local memo is a fixture boundary.  Every overlay, wire, native
 * CLOG and durable-recovery alternative is trapped and must remain unused.
 *
 *-------------------------------------------------------------------------
 */
#define USE_CLUSTER_UNIT 1

#include "postgres.h"

#include "access/clog.h"
#include "access/transam.h"
#include "access/xlog.h"
#include "cluster/cluster_cr.h"
#include "cluster/cluster_epoch.h"
#include "cluster/cluster_guc.h"
#include "cluster/cluster_recovery_merge.h"
#include "cluster/cluster_remote_xact.h"
#include "cluster/cluster_runtime_visibility.h"
#include "cluster/cluster_subtrans.h"
#include "cluster/cluster_touched_peers.h"
#include "cluster/cluster_tt_durable.h"
#include "cluster/cluster_tt_status.h"
#include "cluster/cluster_undo_verdict.h"
#include "cluster/cluster_visibility_resolve.h"
#include "cluster/cluster_xid_authority.h"
#include "cluster/cluster_xid_stripe.h"
#include "cluster/cluster_xnode_lever.h"
#include "cluster/cluster_xnode_profile.h"
#include "storage/lwlock.h"

#include "unit_test.h"

UT_DEFINE_GLOBALS();

#define UT_SELF_NODE 3
#define UT_PEER_NODE 11
#define UT_UNDO_SEGMENT UINT16_C(0x1234)
#define UT_TT_SLOT UINT32_C(0x89ABCDEF)
#define UT_CLUSTER_EPOCH UINT32_C(0x10203040)
#define UT_RAW_XID ((TransactionId)UINT32_C(0x24681357))
#define UT_ANCHOR_LSN ((XLogRecPtr)UINT64_C(0x0102030405060708))
#define UT_READ_SCN ((SCN)UINT64_C(0x0011223344556677))
#define UT_COMMIT_SCN ((SCN)UINT64_C(0x0000000200000700))
#define UT_NATIVE_XID ((TransactionId)UINT32_C(798))
#define UT_NATIVE_HW UINT64_C(816)
#define UT_NEXT_FULL_XID UINT64_C(4195136)

/* Globals read by the retained real-resolver call graph. */
int cluster_node_id = UT_SELF_NODE;
int cluster_dg_role = CLUSTER_DG_ROLE_PRIMARY;
int cluster_subtrans_max_chain_depth = 32;
bool cluster_enable_adg = false;
bool cluster_crossnode_runtime_visibility = false;
bool cluster_crossnode_write_write = false;
bool cluster_cf_terminal_authority = false;
bool cluster_xnode_profile_enabled = false;
ClusterXnodeProfileShared *ClusterXnodeProfileCtl = NULL;
static LWLockPadded ut_main_lwlocks[45];
static VariableCacheData ut_variable_cache;
LWLockPadded *MainLWLockArray = ut_main_lwlocks;
VariableCache ShmemVariableCache = &ut_variable_cache;

typedef struct ResolverReceiptCalls {
	int memo_probe;
	int resolve_note;
	int peer_stamp;
	int overlay;
	int wire;
	int clog;
	int durable;
	int prehistory_probe;
	int prehistory_lock;
	int prehistory_unlock;
	int prehistory_status;
	int prehistory_note;
	int truncation_lock;
	int truncation_unlock;
} ResolverReceiptCalls;

static ResolverReceiptCalls ut_calls;
static ClusterTTStatusKey ut_seen_key;
static ClusterTTStatus ut_memo_status;
static SCN ut_memo_scn;
static int32 ut_stamped_node;
static ClusterTouchKind ut_stamped_kind;
static bool ut_memo_saw_resolve_scope;
static bool ut_memo_hit;
static bool ut_native_prehistory_armed;

static ClusterUndoTTSlotRef
ut_exact_peer_ref(void)
{
	ClusterUndoTTSlotRef ref;

	memset(&ref, 0, sizeof(ref));
	ref.origin_node_id = UT_PEER_NODE;
	ref.undo_segment_id = UT_UNDO_SEGMENT;
	ref.tt_slot_id = UT_TT_SLOT;
	ref.cluster_epoch = UT_CLUSTER_EPOCH;
	ref.local_xid = UT_RAW_XID;
	ref.cached_commit_scn = InvalidScn;
	ref.has_cached_status = false;
	return ref;
}

static void
ut_reset(ClusterTTStatus status, SCN scn)
{
	memset(&ut_calls, 0, sizeof(ut_calls));
	memset(&ut_seen_key, 0xA5, sizeof(ut_seen_key));
	ut_memo_status = status;
	ut_memo_scn = scn;
	ut_stamped_node = -1;
	ut_stamped_kind = CLUSTER_TOUCH_KIND_COUNT;
	ut_memo_saw_resolve_scope = false;

	cluster_node_id = UT_SELF_NODE;
	cluster_dg_role = CLUSTER_DG_ROLE_PRIMARY;
	cluster_enable_adg = false;
	cluster_crossnode_runtime_visibility = false;
	cluster_crossnode_write_write = false;
	cluster_cf_terminal_authority = false;
	cluster_xnode_profile_enabled = false;
	ClusterXnodeProfileCtl = NULL;
	ut_memo_hit = true;
	ut_native_prehistory_armed = false;
	memset(&ut_variable_cache, 0, sizeof(ut_variable_cache));
	ut_variable_cache.oldestClogXid = FirstNormalTransactionId;
}

/* Expected boundaries on the memo-hit path. */
bool
cluster_touched_peers_stamp(int32 node_id, ClusterTouchKind kind)
{
	ut_calls.peer_stamp++;
	ut_stamped_node = node_id;
	ut_stamped_kind = kind;
	return true;
}

void
cluster_lever_c_note_resolve(void)
{
	ut_calls.resolve_note++;
}

bool
cluster_vis_memo_probe(const ClusterTTStatusKey *key, uint8 *status_out, SCN *scn_out)
{
	ut_calls.memo_probe++;
	ut_seen_key = *key;
	ut_memo_saw_resolve_scope = cluster_vis_resolve_in_flight();
	if (!ut_memo_hit)
		return false;
	*status_out = (uint8)ut_memo_status;
	*scn_out = ut_memo_scn;
	return true;
}

/* Overlay/source fallbacks: none may execute after an exact memo hit. */
ClusterSemanticAdmissionResult
cluster_tt_status_source_dispatch(ClusterTTStatusSourceOp op pg_attribute_unused(),
								  const ClusterTTStatusSourceRequest *request pg_attribute_unused(),
								  ClusterTTStatusSourceResult *result pg_attribute_unused())
{
	ut_calls.overlay++;
	return CLUSTER_SEMANTIC_ADMISSION_CLOSED;
}

ClusterTTStatusResult
cluster_subtrans_lookup_parent(const ClusterTTStatusResult *child_result pg_attribute_unused(),
							   int depth_remaining pg_attribute_unused())
{
	ClusterTTStatusResult result;

	ut_calls.overlay++;
	memset(&result, 0, sizeof(result));
	result.status = CLUSTER_TT_STATUS_UNKNOWN;
	return result;
}

void
cluster_vis_memo_install(const ClusterTTStatusKey *key pg_attribute_unused(),
						 uint8 status pg_attribute_unused(), SCN commit_scn pg_attribute_unused())
{
	ut_calls.overlay++;
}

void
cluster_lever_c_note_tt_lookup(bool stamp_cached_present pg_attribute_unused(),
							   bool stamp_contradicted pg_attribute_unused())
{
	ut_calls.overlay++;
}

void
cluster_vis_bump_overlay_refresh_count(void)
{
	ut_calls.overlay++;
}

/* Runtime/wire fallbacks, including the synthetic UNKNOWN widening leg. */
bool
cluster_runtime_visibility_try_resolve_remote(int origin_node pg_attribute_unused(),
											  uint32 undo_segment_id pg_attribute_unused(),
											  TransactionId raw_xid pg_attribute_unused(),
											  SCN read_scn pg_attribute_unused(),
											  bool authoritative pg_attribute_unused(),
											  bool *out_committed, SCN *out_commit_scn,
											  bool *out_commit_scn_is_bound)
{
	ut_calls.wire++;
	if (out_committed != NULL)
		*out_committed = false;
	if (out_commit_scn != NULL)
		*out_commit_scn = InvalidScn;
	if (out_commit_scn_is_bound != NULL)
		*out_commit_scn_is_bound = false;
	return false;
}

ClusterUndoVerdictResult
cluster_undo_verdict_resolve(int origin_node pg_attribute_unused(),
							 uint32 undo_segment_id pg_attribute_unused(),
							 TransactionId raw_xid pg_attribute_unused(),
							 uint32 expected_tt_slot_id pg_attribute_unused(),
							 SCN read_scn pg_attribute_unused(),
							 bool authoritative pg_attribute_unused())
{
	ClusterUndoVerdictResult result;

	ut_calls.wire++;
	memset(&result, 0, sizeof(result));
	result.kind = CLUSTER_UNDO_VERDICT_UNKNOWN_FAIL_CLOSED;
	result.commit_scn = InvalidScn;
	return result;
}

int
cluster_xid_origin_slot(TransactionId xid pg_attribute_unused())
{
	ut_calls.wire++;
	return -1;
}

bool
cluster_vis_from_undo_verdict(ClusterUndoVerdictResult verdict pg_attribute_unused(),
							  ClusterVisResolve *out pg_attribute_unused())
{
	ut_calls.wire++;
	return false;
}

ClusterVisFreshRefOriginDecision
cluster_vis_freshref_origin_decision(int derived_slot pg_attribute_unused(),
									 int32 ref_origin pg_attribute_unused())
{
	ut_calls.wire++;
	return CLUSTER_VIS_FRESHREF_ORIGIN_STALE;
}

void
cluster_vis_freshref_verdict_note_resolved(void)
{
	ut_calls.wire++;
}

void
cluster_vis_freshref_verdict_note_failclosed(void)
{
	ut_calls.wire++;
}

void
cluster_rtvis_note_underivable_failclosed(void)
{
	ut_calls.wire++;
}

void
cluster_vis_bump_vis_variant_unknown_failclosed_count(void)
{
	ut_calls.wire++;
}

/* Native/ADG CLOG alternatives: the peer exact-ref leg must avoid all. */
bool
RecoveryInProgress(void)
{
	ut_calls.clog++;
	return false;
}

bool
TransactionIdDidCommit(TransactionId xid pg_attribute_unused())
{
	ut_calls.clog++;
	return false;
}

XidStatus
TransactionIdGetStatus(TransactionId xid pg_attribute_unused(), XLogRecPtr *lsn)
{
	ut_calls.clog++;
	ut_calls.prehistory_status++;
	if (lsn != NULL)
		*lsn = InvalidXLogRecPtr;
	return ut_native_prehistory_armed ? TRANSACTION_STATUS_COMMITTED
									  : TRANSACTION_STATUS_IN_PROGRESS;
}

bool
TransactionIdPrecedes(TransactionId id1 pg_attribute_unused(),
					  TransactionId id2 pg_attribute_unused())
{
	ut_calls.clog++;
	return false;
}

FullTransactionId
ReadNextFullTransactionId(void)
{
	ut_calls.clog++;
	return FullTransactionIdFromU64(UT_NEXT_FULL_XID);
}

uint64
cluster_cr_native_prehistory_covered_hw(void)
{
	ut_calls.prehistory_probe++;
	return ut_native_prehistory_armed ? UT_NATIVE_HW : 0;
}

void
cluster_cr_native_prehistory_reader_lock(void)
{
	ut_calls.clog++;
	ut_calls.prehistory_lock++;
}

void
cluster_cr_native_prehistory_reader_unlock(void)
{
	ut_calls.clog++;
	ut_calls.prehistory_unlock++;
}

bool
cluster_xid_native_prehistory_provable_full(uint64 next_full_xid pg_attribute_unused(),
											uint64 covered_hw_full pg_attribute_unused(),
											TransactionId xid pg_attribute_unused())
{
	ut_calls.clog++;
	return ut_native_prehistory_armed && next_full_xid == UT_NEXT_FULL_XID
		   && covered_hw_full == UT_NATIVE_HW && xid == UT_NATIVE_XID;
}

bool
cluster_xid_provably_foreign(TransactionId xid pg_attribute_unused())
{
	ut_calls.clog++;
	return false;
}

void
cluster_rtvis_note_native_prehistory_local(void)
{
	ut_calls.clog++;
	ut_calls.prehistory_note++;
}

bool
LWLockAcquire(LWLock *lock pg_attribute_unused(), LWLockMode mode pg_attribute_unused())
{
	ut_calls.clog++;
	ut_calls.truncation_lock++;
	return true;
}

void
LWLockRelease(LWLock *lock pg_attribute_unused())
{
	ut_calls.clog++;
	ut_calls.truncation_unlock++;
}

/* Materialized/durable authority alternatives: also forbidden here. */
bool
cluster_merged_instance_is_materialized(int origin_node pg_attribute_unused())
{
	ut_calls.durable++;
	return false;
}

uint64
cluster_merged_instance_recovered_through(int origin_node pg_attribute_unused())
{
	ut_calls.durable++;
	return 0;
}

bool
cluster_tt_recovery_remote_authority_covers(uint64 recovered_through pg_attribute_unused(),
											uint64 anchor_lsn pg_attribute_unused())
{
	ut_calls.durable++;
	return false;
}

uint64
cluster_epoch_get_current(void)
{
	ut_calls.durable++;
	return 0;
}

ClusterRemoteXactOutcome
cluster_remote_outcome_terminal_authorized(int origin_node pg_attribute_unused(),
										   TransactionId xid pg_attribute_unused(),
										   uint64 observed_epoch pg_attribute_unused(),
										   uint64 current_epoch pg_attribute_unused(),
										   bool retention_required pg_attribute_unused(),
										   bool retention_proven pg_attribute_unused(),
										   SCN *out_scn pg_attribute_unused())
{
	ut_calls.durable++;
	return CLUSTER_REMOTE_XACT_INDOUBT;
}

ClusterRemoteXactOutcome
cluster_remote_outcome_durable_checked(int origin_node pg_attribute_unused(),
									   TransactionId xid pg_attribute_unused(),
									   SCN *out_scn pg_attribute_unused())
{
	ut_calls.durable++;
	return CLUSTER_REMOTE_XACT_INDOUBT;
}

void
cluster_tt_recovery_count_remote_active_failclosed(void)
{
	ut_calls.durable++;
}

void
ExceptionalCondition(const char *condition_name, const char *file_name, int line_number)
{
	fprintf(stderr, "unexpected Assert: %s at %s:%d\n", condition_name, file_name, line_number);
	abort();
}

static void
ut_assert_exact_key(void)
{
	UT_ASSERT_EQ(ut_seen_key.origin_node_id, UT_PEER_NODE);
	UT_ASSERT_EQ(ut_seen_key.undo_segment_id, UT_UNDO_SEGMENT);
	UT_ASSERT_EQ(ut_seen_key.tt_slot_id, UT_TT_SLOT);
	UT_ASSERT_EQ(ut_seen_key.cluster_epoch, UT_CLUSTER_EPOCH);
	UT_ASSERT_EQ(ut_seen_key.local_xid, UT_RAW_XID);
	UT_ASSERT_EQ(ut_seen_key._reserved, 0);
	UT_ASSERT_EQ(ut_seen_key._reserved2, 0);
}

static void
ut_assert_no_fallback(void)
{
	UT_ASSERT_EQ(ut_calls.overlay, 0);
	UT_ASSERT_EQ(ut_calls.wire, 0);
	UT_ASSERT_EQ(ut_calls.clog, 0);
	UT_ASSERT_EQ(ut_calls.durable, 0);
}

static void
ut_run_memo_hit(ClusterTTStatus status, SCN memo_scn)
{
	ClusterUndoTTSlotRef ref = ut_exact_peer_ref();
	ClusterVisResolve out;

	ut_reset(status, memo_scn);
	memset(&out, 0xA5, sizeof(out));

	UT_ASSERT(!cluster_vis_resolve_in_flight());
	cluster_visibility_resolve_from_ref_scn(UT_RAW_XID, &ref, UT_ANCHOR_LSN, UT_READ_SCN, &out);
	UT_ASSERT(!cluster_vis_resolve_in_flight());

	UT_ASSERT_EQ(ut_calls.peer_stamp, 1);
	UT_ASSERT_EQ(ut_stamped_node, UT_PEER_NODE);
	UT_ASSERT_EQ(ut_stamped_kind, CLUSTER_TOUCH_VISIBILITY);
	UT_ASSERT_EQ(ut_calls.resolve_note, 1);
	UT_ASSERT_EQ(ut_calls.memo_probe, 1);
	UT_ASSERT(ut_memo_saw_resolve_scope);
	ut_assert_exact_key();

	UT_ASSERT_EQ(out.evidence, CLUSTER_VIS_EVIDENCE_REMOTE);
	UT_ASSERT_EQ(out.status, status);
	UT_ASSERT_EQ(out.commit_scn, memo_scn);
	UT_ASSERT(!out.commit_scn_is_bound);
	UT_ASSERT(memcmp(&out.ref, &ref, sizeof(ref)) == 0);
	ut_assert_no_fallback();
}

UT_TEST(test_peer_exact_memo_hit_propagates_committed)
{
	ut_run_memo_hit(CLUSTER_TT_STATUS_COMMITTED, UT_COMMIT_SCN);
}

UT_TEST(test_peer_exact_memo_hit_propagates_aborted)
{
	ut_run_memo_hit(CLUSTER_TT_STATUS_ABORTED, InvalidScn);
}

/*
 * The production memo accepts terminal values only.  This synthetic hit pins
 * the resolver's UNKNOWN propagation and, critically, proves that disabling
 * runtime visibility prevents UNKNOWN from re-entering the fresh-ref wire leg.
 */
UT_TEST(test_peer_exact_synthetic_unknown_hit_stays_failclosed_without_wire)
{
	ut_run_memo_hit(CLUSTER_TT_STATUS_UNKNOWN, InvalidScn);
}

/*
 * Formal 4x1x3 at 0c507acf first failed on native seed xid 798: a peer
 * acquired a still-bound (fresh) ITL ref, skipped the recycled-ref-only
 * native-prehistory gate, then missed TT authority that cannot exist for an
 * enabled=off seed transaction.  Verified prehistory is the local terminal
 * authority for every provably native xid regardless of ref freshness.
 */
UT_TEST(test_peer_fresh_native_ref_uses_covered_prehistory_before_overlay)
{
	ClusterUndoTTSlotRef ref = ut_exact_peer_ref();
	ClusterVisResolve out;

	ref.local_xid = UT_NATIVE_XID;
	ut_reset(CLUSTER_TT_STATUS_UNKNOWN, InvalidScn);
	ut_memo_hit = false;
	ut_native_prehistory_armed = true;
	cluster_crossnode_runtime_visibility = true;
	memset(&out, 0xA5, sizeof(out));

	cluster_visibility_resolve_from_ref_scn(UT_NATIVE_XID, &ref, UT_ANCHOR_LSN, UT_READ_SCN, &out);

	UT_ASSERT_EQ(out.evidence, CLUSTER_VIS_EVIDENCE_REMOTE);
	UT_ASSERT_EQ(out.status, CLUSTER_TT_STATUS_COMMITTED);
	UT_ASSERT_EQ(out.commit_scn, (SCN)1);
	UT_ASSERT(out.commit_scn_is_bound);
	UT_ASSERT_EQ(ut_calls.peer_stamp, 1);
	UT_ASSERT(ut_calls.prehistory_probe >= 2);
	UT_ASSERT_EQ(ut_calls.prehistory_lock, 1);
	UT_ASSERT_EQ(ut_calls.prehistory_unlock, 1);
	UT_ASSERT_EQ(ut_calls.prehistory_status, 1);
	UT_ASSERT_EQ(ut_calls.prehistory_note, 1);
	UT_ASSERT_EQ(ut_calls.truncation_lock, 1);
	UT_ASSERT_EQ(ut_calls.truncation_unlock, 1);
	UT_ASSERT_EQ(ut_calls.overlay, 0);
	UT_ASSERT_EQ(ut_calls.wire, 0);
}

int
main(void)
{
	UT_PLAN(4);
	UT_RUN(test_peer_exact_memo_hit_propagates_committed);
	UT_RUN(test_peer_exact_memo_hit_propagates_aborted);
	UT_RUN(test_peer_exact_synthetic_unknown_hit_stays_failclosed_without_wire);
	UT_RUN(test_peer_fresh_native_ref_uses_covered_prehistory_before_overlay);
	UT_DONE();
	return ut_failed_count == 0 ? 0 : 1;
}
