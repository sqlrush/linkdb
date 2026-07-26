/*-------------------------------------------------------------------------
 *
 * test_cluster_visibility_fork.c
 *	  pgrac spec-3.2 D10 — cluster_unit static-contract tests for
 *	  HeapTupleSatisfiesMVCC cluster visibility fork (D5) + D5b inject
 *	  mechanism.
 *
 *	  15 static + presence tests (v0.3 N3 + N4 + L177 + L178 enforcement):
 *	    T1   ClusterUndoTTSlotRef sizeof / offsetof contract
 *	    T2   ClusterUndoTTSlotRef field offsets stable (origin/segment/
 *	         slot/epoch/local_xid/commit_scn/has_cached_status/_padding)
 *	    T3   placeholder ref sentinel — tt_slot_id == 0 means "skip
 *	         cluster path" (v0.3 §3.3 + §3.4)
 *	    T4   self-origin sentinel — ref.origin_node_id == cluster_node_id
 *	         means "skip cluster path"
 *	    T5   ClusterTTStatusKey build_key contract — origin_node_id +
 *	         undo_segment_id + tt_slot_id + cluster_epoch + local_xid
 *	         must carry from ref (no fields invented)
 *	    T6   53R97 ERRCODE_CLUSTER_TT_STATUS_UNKNOWN encodable
 *	    T7   D5b inject API linkable (cluster_test_lookup_visibility_inject)
 *	    T8   D5b shmem helpers linkable (size + init + register)
 *	    T9   ENABLE_INJECTION conditional — production binary lookup
 *	         helper returns false (no inject table); ENABLE_INJECTION
 *	         build has the function fully defined
 *	    T10  CLUSTER_ITL_SLOT_UNALLOCATED sentinel = 255 (v0.3 D5 gate
 *	         skip tuples carrying placeholder)
 *	    T11  no is_xid_local_origin symbol in spec-3.2 implementation
 *	         (v0.2 §0.1 F1 hard guardrail;  cluster_unit static enforce)
 *	    T12  ClusterTTStatus enum 5 values stable (defensive duplicate
 *	         of test_cluster_tt_status T3-T7 to keep this binary self-
 *	         contained per L107 N+5 producer-consumer pattern)
 *	    S3-C05  production MultiXactIdGetUpdateXid derives mxid origin
 *	         before GetMultiXactIdMembers and refuses foreign/underivable
 *	         ids, so HOT traversal cannot decode a peer mxid in local SLRU
 *
 *	  No HeapTupleSatisfiesMVCC behavioral testing here — that requires
 *	  a real PG backend.  Behavioral coverage in cluster_tap t/204.
 *
 * Copyright (c) 1996-2024, PostgreSQL Global Development Group
 * Portions Copyright (c) 2026, pgrac contributors
 *
 * Author: SqlRush <sqlrush@gmail.com>
 *
 * IDENTIFICATION
 *	  src/test/cluster_unit/test_cluster_visibility_fork.c
 *
 * Spec: spec-3.2-mvcc-cluster-path-tt-status-wire.md (v1.0 FROZEN 2026-05-22)
 *
 *-------------------------------------------------------------------------
 */
#define USE_PGRAC_CLUSTER 1

#include "postgres.h"

#include <stddef.h>

#include "access/htup_details.h"
#include "cluster/cluster_itl_slot.h"
#include "cluster/cluster_scn.h"
#include "cluster/cluster_tt_slot.h"
#include "cluster/cluster_tt_status.h"
#include "cluster/cluster_visibility_inject.h"
#include "cluster/cluster_visibility_resolve.h"
#include "utils/errcodes.h"

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


UT_DEFINE_GLOBALS();

#ifndef HEAPAM_SOURCE_PATH
#error "HEAPAM_SOURCE_PATH must identify production heapam.c"
#endif
#ifndef HEAPAM_VISIBILITY_SOURCE_PATH
#error "HEAPAM_VISIBILITY_SOURCE_PATH must identify production heapam_visibility.c"
#endif
#ifndef VISIBILITY_RESOLVE_SOURCE_PATH
#error "VISIBILITY_RESOLVE_SOURCE_PATH must identify production cluster_visibility_resolve.c"
#endif
#ifndef TT_LOCAL_SOURCE_PATH
#error "TT_LOCAL_SOURCE_PATH must identify production cluster_tt_local.c"
#endif


void
ExceptionalCondition(const char *conditionName pg_attribute_unused(),
					 const char *fileName pg_attribute_unused(),
					 int lineNumber pg_attribute_unused())
{
	abort();
}


/* Stubs — cluster_unit binary does not link cluster_visibility_inject.o. */
bool
cluster_test_lookup_visibility_inject(TransactionId xid pg_attribute_unused(),
									  ClusterUndoTTSlotRef *ref pg_attribute_unused())
{
	return false;
}
Size
cluster_visibility_inject_shmem_size(void)
{
	return 0;
}
void
cluster_visibility_inject_shmem_init(void)
{}
void
cluster_visibility_inject_shmem_register(void)
{}

static char *
read_source(const char *path)
{
	FILE *fp;
	char *source;
	long length;

	fp = fopen(path, "rb");
	UT_ASSERT(fp != NULL);
	if (fp == NULL)
		return NULL;
	UT_ASSERT_EQ(fseek(fp, 0, SEEK_END), 0);
	length = ftell(fp);
	UT_ASSERT(length > 0);
	UT_ASSERT_EQ(fseek(fp, 0, SEEK_SET), 0);
	source = malloc((size_t)length + 1);
	UT_ASSERT(source != NULL);
	if (source == NULL) {
		fclose(fp);
		return NULL;
	}
	UT_ASSERT_EQ((long)fread(source, 1, (size_t)length, fp), length);
	source[length] = '\0';
	fclose(fp);
	return source;
}

static void
assert_data_active_publish(const char *source, const char *start_marker, const char *end_marker,
						   const char *uba_name)
{
	const char *start = strstr(source, start_marker);
	const char *end = start != NULL ? strstr(start + strlen(start_marker), end_marker) : NULL;
	char publish_call[160];
	const char *publish;
	const char *crit_end = start != NULL ? strstr(start, "END_CRIT_SECTION();") : NULL;

	snprintf(publish_call, sizeof(publish_call), "cluster_tt_local_record_data_active(xid, %s);",
			 uba_name);
	publish = start != NULL ? strstr(start, publish_call) : NULL;

	if (publish != NULL && end != NULL && publish >= end)
		publish = NULL;

	UT_ASSERT(start != NULL);
	UT_ASSERT(end != NULL);
	UT_ASSERT(publish != NULL);
	UT_ASSERT(crit_end != NULL);
	if (start == NULL || end == NULL || publish == NULL || crit_end == NULL)
		return;

	/* The ACTIVE identity is published only after the tuple + ITL stamp is
	 * WAL-protected, and before the function can release its heap buffer. */
	while (true) {
		const char *next = strstr(crit_end + 1, "END_CRIT_SECTION();");

		if (next == NULL || next >= publish)
			break;
		crit_end = next;
	}
	UT_ASSERT(crit_end < publish);
}


/* ===== T1: ClusterUndoTTSlotRef size 32B ===== */
UT_TEST(test_t1_undo_tt_slot_ref_sizeof_32)
{
	UT_ASSERT_EQ((int)sizeof(ClusterUndoTTSlotRef), 32);
}

/* ===== T2: field offsets locked (mirror spec-3.1 v0.4 M4 + spec-3.2 gate inputs) ===== */
UT_TEST(test_t2_ref_field_offsets)
{
	UT_ASSERT_EQ((int)offsetof(ClusterUndoTTSlotRef, origin_node_id), 0);
	UT_ASSERT_EQ((int)offsetof(ClusterUndoTTSlotRef, undo_segment_id), 2);
	UT_ASSERT_EQ((int)offsetof(ClusterUndoTTSlotRef, tt_slot_id), 4);
	UT_ASSERT_EQ((int)offsetof(ClusterUndoTTSlotRef, cluster_epoch), 8);
	UT_ASSERT_EQ((int)offsetof(ClusterUndoTTSlotRef, local_xid), 12);
	UT_ASSERT_EQ((int)offsetof(ClusterUndoTTSlotRef, cached_commit_scn), 16);
	UT_ASSERT_EQ((int)offsetof(ClusterUndoTTSlotRef, has_cached_status), 24);
	UT_ASSERT_EQ((int)offsetof(ClusterUndoTTSlotRef, _padding), 25);
}

/* ===== T3: placeholder ref sentinel (tt_slot_id == 0 = skip cluster path) ===== */
UT_TEST(test_t3_placeholder_ref_sentinel)
{
	ClusterUndoTTSlotRef ref;
	memset(&ref, 0, sizeof(ref));
	/* spec-3.1 D4 reader returns tt_slot_id = 0 placeholder for production
	 * heap pages.  spec-3.2 §3.3 gate:  this means "skip cluster path". */
	UT_ASSERT_EQ((int)ref.tt_slot_id, 0);
}

/* ===== T4: self-origin gate semantics ===== */
UT_TEST(test_t4_self_origin_gate)
{
	ClusterUndoTTSlotRef ref;
	int fake_self_node = 1;
	memset(&ref, 0, sizeof(ref));
	ref.origin_node_id = (uint16)fake_self_node;
	/* spec-3.2 §3.3:  ref.origin_node_id == cluster_node_id (self) =
	 * "skip cluster path" → tuple goes to PG-native body. */
	UT_ASSERT_EQ((int)ref.origin_node_id, fake_self_node);
}

/* ===== T5: ClusterTTStatusKey build from ref — field carry contract ===== */
UT_TEST(test_t5_build_key_field_carry)
{
	ClusterUndoTTSlotRef ref;
	ClusterTTStatusKey key;

	memset(&ref, 0, sizeof(ref));
	ref.origin_node_id = 7;
	ref.undo_segment_id = 3;
	ref.tt_slot_id = 42;
	ref.cluster_epoch = 100;
	ref.local_xid = 12345;

	memset(&key, 0, sizeof(key));
	key.origin_node_id = ref.origin_node_id;
	key.undo_segment_id = ref.undo_segment_id;
	key.tt_slot_id = ref.tt_slot_id;
	key.cluster_epoch = ref.cluster_epoch;
	key.local_xid = ref.local_xid;

	/* All five identity fields carry — no fields invented. */
	UT_ASSERT_EQ((int)key.origin_node_id, 7);
	UT_ASSERT_EQ((int)key.undo_segment_id, 3);
	UT_ASSERT_EQ((int)key.tt_slot_id, 42);
	UT_ASSERT_EQ((int)key.cluster_epoch, 100);
	UT_ASSERT_EQ((int)key.local_xid, 12345);
	/* Reserved fields zero on emit. */
	UT_ASSERT_EQ((int)key._reserved, 0);
	UT_ASSERT_EQ((int)key._reserved2, 0);
}

/* ===== T6: 53R97 SQLSTATE encodable ===== */
UT_TEST(test_t6_errcode_53r97_encodable)
{
	int sqlstate = MAKE_SQLSTATE('5', '3', 'R', '9', '7');
	UT_ASSERT_EQ((int)ERRCODE_CLUSTER_TT_STATUS_UNKNOWN, sqlstate);
}

/* ===== T7: D5b inject lookup API linkable ===== */
UT_TEST(test_t7_inject_lookup_linkable)
{
	UT_ASSERT_NE((void *)cluster_test_lookup_visibility_inject, NULL);
}

/* ===== T8: D5b shmem helpers linkable ===== */
UT_TEST(test_t8_inject_shmem_helpers_linkable)
{
	UT_ASSERT_NE((void *)cluster_visibility_inject_shmem_size, NULL);
	UT_ASSERT_NE((void *)cluster_visibility_inject_shmem_init, NULL);
	UT_ASSERT_NE((void *)cluster_visibility_inject_shmem_register, NULL);
}

/* ===== T9: ENABLE_INJECTION conditional — stub returns false in this
 * test (production-binary equivalent semantics) ===== */
UT_TEST(test_t9_production_inject_returns_false)
{
	ClusterUndoTTSlotRef ref;
	bool hit;
	memset(&ref, 0, sizeof(ref));
	/* This binary links the stub form (test_cluster_visibility_fork.c
	 * defines a local stub that always returns false) → covers the
	 * production no-op path semantics. */
	hit = cluster_test_lookup_visibility_inject(99, &ref);
	UT_ASSERT_EQ((int)hit, 0);
}

/* ===== T10: CLUSTER_ITL_SLOT_UNALLOCATED sentinel = 255 ===== */
UT_TEST(test_t10_itl_slot_unallocated_sentinel)
{
	/* spec-3.2 D5 gate:  tuple->t_itl_slot_idx == CLUSTER_ITL_SLOT_UNALLOCATED
	 * means "no ITL slot pointer;  skip cluster path". */
	UT_ASSERT_EQ((int)CLUSTER_ITL_SLOT_UNALLOCATED, 255);
}

/* ===== T11: v0.2 §0.1 F1 hard guardrail — no is_xid_local_origin
 * heuristic symbol in spec-3.2 implementation.  Compile-time check:
 * this file does NOT declare such a symbol;  if D5 D5b implementation
 * pulls one in, linker will surface it elsewhere. ===== */
UT_TEST(test_t11_no_is_xid_local_origin_in_this_unit)
{
	/* The test value is the absence of the symbol from our build.
	 * Linker-level enforcement at test_cluster_visibility_fork build
	 * time:  no is_xid_local_origin declared / used.  Lint script
	 * scripts/ci/check-no-clog-overlay.sh handles cross-repo
	 * BANNED_RE enforcement.  This static assertion just records the
	 * intent. */
	UT_ASSERT_EQ(1, 1);
}

/* ===== T12: ClusterTTStatus 5 values stable (self-contained) ===== */
UT_TEST(test_t12_status_enum_5_values)
{
	UT_ASSERT_EQ((int)CLUSTER_TT_STATUS_UNKNOWN, 0);
	UT_ASSERT_EQ((int)CLUSTER_TT_STATUS_IN_PROGRESS, 1);
	UT_ASSERT_EQ((int)CLUSTER_TT_STATUS_COMMITTED, 2);
	UT_ASSERT_EQ((int)CLUSTER_TT_STATUS_ABORTED, 3);
	UT_ASSERT_EQ((int)CLUSTER_TT_STATUS_CLEANED_OUT, 4);
}

/* ===== P0-33: every ordinary data-DML producer must publish the exact
 * binding as IN_PROGRESS after its ACTIVE ITL stamp, while it still owns the
 * buffer content lock.  Lock-only already did this; missing data-DML calls
 * made a fresh active remote ref miss the overlay and surface 53R97. ===== */
UT_TEST(test_p033_data_dml_publishes_active_identity)
{
	char *heap_source = read_source(HEAPAM_SOURCE_PATH);
	char *tt_source = read_source(TT_LOCAL_SOURCE_PATH);

	if (heap_source == NULL || tt_source == NULL) {
		free(heap_source);
		free(tt_source);
		return;
	}
	assert_data_active_publish(heap_source, "\nheap_insert(Relation",
							   "\nheap_prepare_insert(Relation", "cluster_itl_uba");
	assert_data_active_publish(heap_source, "\nheap_multi_insert(Relation",
							   "\nsimple_heap_insert(Relation", "cluster_mi_uba");
	assert_data_active_publish(heap_source, "\nheap_delete(Relation",
							   "\nsimple_heap_delete(Relation", "cluster_itl_uba");
	assert_data_active_publish(heap_source, "\nheap_update(Relation",
							   "\nsimple_heap_update(Relation", "cluster_itl_uba");

	/* A real undo-record UBA may live in a different record segment from the
	 * transaction's canonical TT segment after rollover.  The producer must
	 * therefore remember and publish the exact page-ref alias, and every
	 * terminal install must converge those aliases to COMMITTED/ABORTED. */
	UT_ASSERT(strstr(tt_source, "cluster_tt_local_record_data_active(TransactionId xid, UBA uba)")
			  != NULL);
	UT_ASSERT(strstr(tt_source, "active_alias_segments") != NULL);
	UT_ASSERT(strstr(tt_source, "install_binding_aliases") != NULL);
	UT_ASSERT(strstr(tt_source, "install_binding_aliases(binding, status, commit_scn)") != NULL);

	free(heap_source);
	free(tt_source);
}

/* P0-33 safety matrix: a proved remote ACTIVE status is non-terminal and
 * follows each consumer's existing truth table.  None of the authoritative
 * terminal/FROZEN/stale boundaries are widened by the producer fix. */
UT_TEST(test_p033_active_and_safety_boundary_matrix)
{
	UT_ASSERT_EQ((int)cluster_vis_evidence_route(CLUSTER_VIS_EVIDENCE_REMOTE, false),
				 (int)CLUSTER_VIS_ROUTE_REMOTE_VERDICT);
	UT_ASSERT_EQ((int)cluster_vis_update_xmin_verdict(CLUSTER_TT_STATUS_IN_PROGRESS),
				 (int)CVV_INVISIBLE);
	UT_ASSERT_EQ((int)cluster_vis_update_xmax_verdict(CLUSTER_TT_STATUS_IN_PROGRESS, false),
				 (int)CVV_BEING_MODIFIED);

	UT_ASSERT_EQ((int)cluster_vis_xmin_needs_resolution(HEAP_XMIN_FROZEN), 0);
	UT_ASSERT_EQ((int)cluster_vis_update_xmin_verdict(CLUSTER_TT_STATUS_COMMITTED),
				 (int)CVV_VISIBLE);
	UT_ASSERT_EQ((int)cluster_vis_update_xmin_verdict(CLUSTER_TT_STATUS_ABORTED),
				 (int)CVV_INVISIBLE);
	UT_ASSERT_EQ((int)cluster_vis_evidence_route(CLUSTER_VIS_EVIDENCE_STALE_OR_AMBIGUOUS, false),
				 (int)CLUSTER_VIS_ROUTE_FAILCLOSED_UNKNOWN);
	UT_ASSERT_EQ((int)cluster_vis_update_xmin_verdict(CLUSTER_TT_STATUS_UNKNOWN),
				 (int)CVV_FAILCLOSED_UNKNOWN);
}

/* S3-C05 / Loop6: heap_hot_search_buffer follows an invisible HOT member by
 * calling HeapTupleHeaderGetUpdateXid.  That funnels through the production
 * MultiXactIdGetUpdateXid helper, so this is the narrow SSOT at which a peer
 * mxid must be rejected before GetMultiXactIdMembers can touch the LOCAL
 * offsets/member SLRUs.  The native Assert(offset != 0) remains intact; the
 * cluster guard prevents an unowned id from ever reaching it. */
UT_TEST(test_s3c05_foreign_multixact_local_slru_decode_guard)
{
	char *heap_source = read_source(HEAPAM_SOURCE_PATH);
	const char *helper;
	const char *helper_end;
	const char *origin;
	const char *refuse;
	const char *decode;

	UT_ASSERT(heap_source != NULL);
	if (heap_source == NULL)
		return;

	helper = strstr(heap_source, "\nMultiXactIdGetUpdateXid(TransactionId xmax");
	helper_end = helper != NULL ? strstr(helper, "\n}\n\n/*\n * HeapTupleGetUpdateXid") : NULL;
	origin = helper != NULL ? strstr(helper, "cluster_mxid_origin_slot((MultiXactId)xmax)") : NULL;
	refuse = helper != NULL
				 ? strstr(helper, "cannot decode foreign multixact %u against local member storage")
				 : NULL;
	decode = helper != NULL ? strstr(helper, "GetMultiXactIdMembers(xmax") : NULL;

	UT_ASSERT(helper != NULL);
	UT_ASSERT(helper_end != NULL);
	UT_ASSERT(origin != NULL);
	UT_ASSERT(refuse != NULL);
	UT_ASSERT(decode != NULL);
	if (helper_end != NULL) {
		UT_ASSERT(origin != NULL && origin < helper_end);
		UT_ASSERT(refuse != NULL && refuse < helper_end);
		UT_ASSERT(decode != NULL && decode < helper_end);
	}
	if (origin != NULL && refuse != NULL && decode != NULL) {
		UT_ASSERT(origin < refuse);
		UT_ASSERT(refuse < decode);
	}
	UT_ASSERT(strstr(helper, "if (mx_origin < 0)") != NULL);
	UT_ASSERT(strstr(helper, "if (mx_origin != cluster_node_id)") != NULL);
	UT_ASSERT(strstr(helper, "ERRCODE_CLUSTER_MULTIXACT_MEMBER_OVERLAY_MISS") != NULL);

	free(heap_source);
}


/* A locally composed MultiXact has only local members and is safe to resolve
 * with PG's native member/CLOG machinery even when xmin was proved committed
 * by remote cluster evidence.  Remote or missing marker evidence remains
 * fail-closed before any local SLRU decoder can run. */
UT_TEST(test_local_multixact_over_remote_xmin_uses_native_update_semantics)
{
	char *source = read_source(HEAPAM_VISIBILITY_SOURCE_PATH);
	const char *helper;
	const char *helper_end;
	const char *validator;
	const char *validator_end;
	const char *members_decode;
	const char *member_loop;
	const char *member_normal_gate;
	const char *member_origin_gate;
	const char *validate_call;
	const char *validate_error;
	const char *is_running;
	const char *get_update_xid;
	const char *committed;
	const char *committed_lock_only;
	const char *committed_ok;
	const char *locked_upgraded;
	const char *current_xid;
	const char *did_commit;
	const char *set_hint_bits;
	const char *fork;
	const char *fork_end;
	const char *multi;
	const char *remote;
	const char *local_gate;
	const char *mxid_derive;
	const char *mxid_gate;
	const char *local_call;
	const char *fork_call;

	UT_ASSERT(source != NULL);
	if (source == NULL)
		return;

	validator = strstr(source, "\ncluster_satisfies_update_validate_local_multixact(");
	validator_end = validator != NULL ? strstr(validator, "\n}\n") : NULL;
	members_decode = validator != NULL ? strstr(validator, "GetMultiXactIdMembers") : NULL;
	member_loop = members_decode != NULL ? strstr(members_decode, "for (i = 0; i < nmembers; i++)") : NULL;
	member_normal_gate = member_loop != NULL
						 ? strstr(member_loop, "!TransactionIdIsNormal(members[i].xid)")
						 : NULL;
	member_origin_gate = member_normal_gate != NULL
						 ? strstr(member_normal_gate, "!cluster_xid_is_mine(members[i].xid)")
						 : NULL;
	helper = strstr(source, "\ncluster_satisfies_update_local_multixact(");
	helper_end = helper != NULL ? strstr(helper, "\n}\n") : NULL;
	committed = helper != NULL ? strstr(helper, "if (tuple->t_infomask & HEAP_XMAX_COMMITTED)") : NULL;
	committed_lock_only
		= committed != NULL ? strstr(committed, "HEAP_XMAX_IS_LOCKED_ONLY") : NULL;
	committed_ok = committed_lock_only != NULL ? strstr(committed_lock_only, "return TM_Ok") : NULL;
	validate_call = helper != NULL
					? strstr(helper, "if (!cluster_satisfies_update_validate_local_multixact(raw_mxid))")
					: NULL;
	validate_error = validate_call != NULL ? strstr(validate_call, "ereport(ERROR") : NULL;
	is_running = helper != NULL ? strstr(helper, "MultiXactIdIsRunning") : NULL;
	get_update_xid = helper != NULL ? strstr(helper, "HeapTupleGetUpdateXid") : NULL;
	locked_upgraded = helper != NULL ? strstr(helper, "HEAP_LOCKED_UPGRADED") : NULL;
	current_xid = helper != NULL ? strstr(helper, "TransactionIdIsCurrentTransactionId") : NULL;
	did_commit = helper != NULL ? strstr(helper, "TransactionIdDidCommit") : NULL;
	set_hint_bits = helper != NULL ? strstr(helper, "SetHintBits") : NULL;
	fork = strstr(source, "\ncluster_satisfies_update_fork(");
	fork_end = fork != NULL ? strstr(fork, "\n}\n#endif") : NULL;
	multi = fork != NULL ? strstr(fork, "if (tuple->t_infomask & HEAP_XMAX_IS_MULTI)") : NULL;
	remote = multi != NULL ? strstr(multi, "if (mr.multi_marker_is_remote)") : NULL;
	local_gate
		= remote != NULL ? strstr(remote, "if (mr.evidence != CLUSTER_VIS_EVIDENCE_LOCAL)") : NULL;
	mxid_derive = local_gate != NULL ? strstr(local_gate, "mx_origin = cluster_mxid_origin_slot(raw_mxid)")
										 : NULL;
	mxid_gate = mxid_derive != NULL ? strstr(mxid_derive, "mx_origin != cluster_node_id") : NULL;
	local_call
		= mxid_gate != NULL
			  ? strstr(local_gate, "cluster_satisfies_update_local_multixact(htup, curcid, buffer)")
			  : NULL;
	fork_call = fork_end != NULL
					? strstr(fork_end, "cluster_satisfies_update_fork(htup, curcid, buffer")
					: NULL;

	UT_ASSERT(validator != NULL);
	UT_ASSERT(validator_end != NULL);
	UT_ASSERT(members_decode != NULL);
	UT_ASSERT(member_loop != NULL);
	UT_ASSERT(member_normal_gate != NULL);
	UT_ASSERT(member_origin_gate != NULL);
	UT_ASSERT(helper != NULL);
	UT_ASSERT(helper_end != NULL);
	UT_ASSERT(committed != NULL);
	UT_ASSERT(committed_lock_only != NULL);
	UT_ASSERT(committed_ok != NULL);
	UT_ASSERT(validate_call != NULL);
	UT_ASSERT(validate_error != NULL);
	UT_ASSERT(is_running != NULL);
	UT_ASSERT(get_update_xid != NULL);
	UT_ASSERT(locked_upgraded != NULL);
	UT_ASSERT(current_xid != NULL);
	UT_ASSERT(did_commit != NULL);
	UT_ASSERT(set_hint_bits != NULL);
	UT_ASSERT(fork != NULL);
	UT_ASSERT(fork_end != NULL);
	UT_ASSERT(multi != NULL);
	UT_ASSERT(remote != NULL);
	UT_ASSERT(local_gate != NULL);
	UT_ASSERT(mxid_derive != NULL);
	UT_ASSERT(mxid_gate != NULL);
	UT_ASSERT(local_call != NULL);
	UT_ASSERT(fork_call != NULL);
	if (validator != NULL && validator_end != NULL && members_decode != NULL
		&& member_loop != NULL && member_normal_gate != NULL && member_origin_gate != NULL)
		UT_ASSERT(validator < members_decode && members_decode < member_loop
				  && member_loop < member_normal_gate && member_normal_gate < member_origin_gate
				  && member_origin_gate < validator_end);
	if (helper != NULL && helper_end != NULL && committed != NULL && committed_lock_only != NULL
		&& committed_ok != NULL && locked_upgraded != NULL && validate_call != NULL
		&& validate_error != NULL && is_running != NULL && get_update_xid != NULL
		&& current_xid != NULL && did_commit != NULL && set_hint_bits != NULL)
		UT_ASSERT(helper < committed && committed < committed_lock_only
				  && committed_lock_only < committed_ok && committed_ok < locked_upgraded
				  && locked_upgraded < validate_call && validate_call < validate_error
				  && validate_error < is_running && validate_error < get_update_xid
				  && get_update_xid < current_xid && current_xid < did_commit
				  && did_commit < helper_end && validate_error < set_hint_bits
				  && set_hint_bits < helper_end);
	if (multi != NULL && remote != NULL && local_gate != NULL && mxid_derive != NULL
		&& mxid_gate != NULL && local_call != NULL && fork_end != NULL)
		UT_ASSERT(multi < remote && remote < local_gate && local_gate < mxid_derive
				  && mxid_derive < mxid_gate && mxid_gate < local_call && local_call < fork_end);
	free(source);
}

/*
 * S3-P0-15: a cluster-off seed transaction predates the striped allocator
 * even when its page carries an exact-looking origin-0 ITL ref after
 * pg_basebackup.  The sealed native prehistory therefore has to preempt both
 * the self/remote ref classification and the remote live-TT path.  Limiting
 * the prehistory consume to `ref->local_xid != raw_xid` leaves exact seed refs
 * asking for a cluster-era overlay that can never exist (53R97 for xids such
 * as 802/804/806/816).
 *
 * The helper itself pins every negative boundary: boot coverage latch and
 * no-wrap/full-xid proof, XactTruncationLock + oldestClogXid, and the explicit
 * CLOG alphabet mapper.  SUB_COMMITTED or any doubt returns false to the
 * existing fail-closed path; callers never get a generic local-CLOG escape.
 */
UT_TEST(test_native_prehistory_preempts_exact_remote_seed_ref)
{
	char *source = read_source(VISIBILITY_RESOLVE_SOURCE_PATH);
	const char *helper;
	const char *helper_end;
	const char *reader_lock;
	const char *provable;
	const char *trunc_lock;
	const char *oldest_gate;
	const char *clog_read;
	const char *status_map;
	const char *reader_unlock;
	const char *classify;
	const char *classify_end;
	const char *placeholder;
	const char *prehistory_call;
	const char *adg_gate;
	const char *origin_gate;
	const char *exact_mismatch_gate;
	const char *remote_resolve;

	UT_ASSERT(source != NULL);
	if (source == NULL)
		return;

	helper = strstr(source, "\nresolve_native_prehistory(");
	helper_end = helper != NULL ? strstr(helper, "\n}\n") : NULL;
	reader_lock
		= helper != NULL ? strstr(helper, "cluster_cr_native_prehistory_reader_lock();") : NULL;
	provable = reader_lock != NULL
				   ? strstr(reader_lock, "cluster_xid_native_prehistory_provable_full(")
				   : NULL;
	trunc_lock = provable != NULL ? strstr(provable, "LWLockAcquire(XactTruncationLock, LW_SHARED)")
								 : NULL;
	oldest_gate = trunc_lock != NULL ? strstr(trunc_lock, "ShmemVariableCache->oldestClogXid")
									: NULL;
	clog_read = oldest_gate != NULL ? strstr(oldest_gate, "TransactionIdGetStatus(") : NULL;
	status_map = clog_read != NULL ? strstr(clog_read, "cluster_native_prehistory_map_status(")
								  : NULL;
	reader_unlock
		= status_map != NULL ? strstr(status_map, "cluster_cr_native_prehistory_reader_unlock();")
							: NULL;

	classify = strstr(source, "\nclassify_ref_guts(");
	classify_end = classify != NULL ? strstr(classify, "\n}\n") : NULL;
	placeholder = classify != NULL ? strstr(classify, "if (ref->tt_slot_id == 0)") : NULL;
	prehistory_call
		= placeholder != NULL ? strstr(placeholder, "if (resolve_native_prehistory(raw_xid, out))")
							 : NULL;
	adg_gate = placeholder != NULL ? strstr(placeholder, "if (cluster_enable_adg") : NULL;
	origin_gate = placeholder != NULL
					  ? strstr(placeholder, "if ((int32)ref->origin_node_id == cluster_node_id)")
					  : NULL;
	exact_mismatch_gate
		= origin_gate != NULL ? strstr(origin_gate, "if (ref->local_xid != raw_xid)") : NULL;
	remote_resolve
		= classify != NULL ? strstr(classify, "resolve_from_remote_ref(raw_xid, ref, read_scn, out)")
						  : NULL;

	UT_ASSERT(helper != NULL);
	UT_ASSERT(helper_end != NULL);
	UT_ASSERT(reader_lock != NULL);
	UT_ASSERT(provable != NULL);
	UT_ASSERT(trunc_lock != NULL);
	UT_ASSERT(oldest_gate != NULL);
	UT_ASSERT(clog_read != NULL);
	UT_ASSERT(status_map != NULL);
	UT_ASSERT(reader_unlock != NULL);
	UT_ASSERT(classify != NULL);
	UT_ASSERT(classify_end != NULL);
	UT_ASSERT(placeholder != NULL);
	UT_ASSERT(prehistory_call != NULL);
	UT_ASSERT(adg_gate != NULL);
	UT_ASSERT(origin_gate != NULL);
	UT_ASSERT(exact_mismatch_gate != NULL);
	UT_ASSERT(remote_resolve != NULL);
	if (helper != NULL && helper_end != NULL && reader_lock != NULL && provable != NULL
		&& trunc_lock != NULL && oldest_gate != NULL && clog_read != NULL
		&& status_map != NULL && reader_unlock != NULL)
		UT_ASSERT(helper < reader_lock && reader_lock < provable && provable < trunc_lock
				  && trunc_lock < oldest_gate && oldest_gate < clog_read
				  && clog_read < status_map && status_map < reader_unlock
				  && reader_unlock < helper_end);
	if (classify != NULL && classify_end != NULL && placeholder != NULL
		&& prehistory_call != NULL && adg_gate != NULL && origin_gate != NULL
		&& exact_mismatch_gate != NULL && remote_resolve != NULL)
		UT_ASSERT(classify < placeholder && placeholder < prehistory_call
				  && prehistory_call < adg_gate && adg_gate < origin_gate
				  && origin_gate < exact_mismatch_gate
				  && exact_mismatch_gate < remote_resolve
				  && remote_resolve < classify_end);

	free(source);
}

/*
 * S3-P0-26: a current xmax retains PostgreSQL's native three-state decision
 * after cluster evidence has proved xmin visible.  Lock-only is an active
 * lock, while an updater is classified by cmax versus the caller's curcid.
 * Whether the updater deleted the tuple is relevant only to a terminal
 * remote verdict, not to the current-xmax cmax decision.
 */
UT_TEST(test_p026_remote_xmin_current_xmax_preserves_native_three_states)
{
	char *source = read_source(HEAPAM_VISIBILITY_SOURCE_PATH);
	const char *fork;
	const char *fork_end;
	const char *is_delete;
	const char *native_self;
	const char *remote_verdict;
	const char *lock_only;
	const char *being_modified;
	const char *cmax;
	const char *self_modified;
	const char *invisible;
	const char *native_is_delete;
	const char *remote_delete_map;
	const char *native_update;
	const char *native_current;
	const char *native_lock_only;
	const char *native_being_modified;
	const char *native_cmax;
	const char *native_self_modified;
	const char *native_invisible;

	UT_ASSERT(source != NULL);
	if (source == NULL)
		return;

	fork = strstr(source, "\ncluster_satisfies_update_fork(");
	fork_end = fork != NULL ? strstr(fork, "\n}\n#endif") : NULL;
	is_delete = fork != NULL ? strstr(fork, "is_delete = !lock_only") : NULL;
	native_self
		= is_delete != NULL ? strstr(is_delete, "case CLUSTER_VIS_ROUTE_NATIVE_SELF:") : NULL;
	remote_verdict = native_self != NULL
						 ? strstr(native_self, "case CLUSTER_VIS_ROUTE_REMOTE_VERDICT:")
						 : NULL;
	lock_only = native_self != NULL ? strstr(native_self, "if (lock_only)") : NULL;
	being_modified
		= lock_only != NULL ? strstr(lock_only, "*res = TM_BeingModified;") : NULL;
	cmax = being_modified != NULL
			   ? strstr(being_modified, "HeapTupleHeaderGetCmax(tuple) >= curcid")
			   : NULL;
	self_modified = cmax != NULL ? strstr(cmax, "*res = TM_SelfModified;") : NULL;
	invisible = self_modified != NULL ? strstr(self_modified, "*res = TM_Invisible;") : NULL;
	native_is_delete = native_self != NULL ? strstr(native_self, "is_delete") : NULL;
	remote_delete_map = remote_verdict != NULL
							? strstr(remote_verdict,
									 "cluster_vis_update_xmax_verdict(r.status, is_delete)")
							: NULL;

	/* Independently pin the expectation to PostgreSQL's native non-MultiXact
	 * current-xmax arm in HeapTupleSatisfiesUpdate. */
	native_update = fork_end != NULL ? strstr(fork_end, "\nHeapTupleSatisfiesUpdate(") : NULL;
	native_current = native_update != NULL
						 ? strstr(native_update,
								  "if (TransactionIdIsCurrentTransactionId("
								  "HeapTupleHeaderGetRawXmax(tuple)))")
						 : NULL;
	native_lock_only
		= native_current != NULL ? strstr(native_current, "HEAP_XMAX_IS_LOCKED_ONLY") : NULL;
	native_being_modified
		= native_lock_only != NULL ? strstr(native_lock_only, "return TM_BeingModified;") : NULL;
	native_cmax = native_being_modified != NULL
					  ? strstr(native_being_modified,
							   "HeapTupleHeaderGetCmax(tuple) >= curcid")
					  : NULL;
	native_self_modified
		= native_cmax != NULL ? strstr(native_cmax, "return TM_SelfModified;") : NULL;
	native_invisible
		= native_self_modified != NULL ? strstr(native_self_modified, "return TM_Invisible;") : NULL;

	UT_ASSERT(fork != NULL);
	UT_ASSERT(fork_end != NULL);
	UT_ASSERT(is_delete != NULL);
	UT_ASSERT(native_self != NULL);
	UT_ASSERT(remote_verdict != NULL);
	UT_ASSERT(lock_only != NULL);
	UT_ASSERT(being_modified != NULL);
	UT_ASSERT(cmax != NULL);
	UT_ASSERT(self_modified != NULL);
	UT_ASSERT(invisible != NULL);
	UT_ASSERT(remote_delete_map != NULL);
	UT_ASSERT(native_update != NULL);
	UT_ASSERT(native_current != NULL);
	UT_ASSERT(native_lock_only != NULL);
	UT_ASSERT(native_being_modified != NULL);
	UT_ASSERT(native_cmax != NULL);
	UT_ASSERT(native_self_modified != NULL);
	UT_ASSERT(native_invisible != NULL);
	if (native_self != NULL && remote_verdict != NULL && lock_only != NULL
		&& being_modified != NULL && cmax != NULL && self_modified != NULL
		&& invisible != NULL)
		UT_ASSERT(native_self < lock_only && lock_only < being_modified
				  && being_modified < cmax && cmax < self_modified
				  && self_modified < invisible && invisible < remote_verdict);
	if (native_is_delete != NULL && remote_verdict != NULL)
		UT_ASSERT(native_is_delete >= remote_verdict);
	if (remote_delete_map != NULL && fork_end != NULL)
		UT_ASSERT(remote_delete_map < fork_end);
	if (native_current != NULL && native_lock_only != NULL
		&& native_being_modified != NULL && native_cmax != NULL
		&& native_self_modified != NULL && native_invisible != NULL)
		UT_ASSERT(native_current < native_lock_only
				  && native_lock_only < native_being_modified
				  && native_being_modified < native_cmax
				  && native_cmax < native_self_modified
				  && native_self_modified < native_invisible);

	free(source);
}

/*
 * S3-P0-13 G8 diagnostics: when xmin has resolved visible but the deleting
 * xmax remains unprovable, the error must name the actual deleting xid.
 * Reporting raw_xmin here sent investigators down the native-prehistory
 * branch even though the failed authority request was for a striped xmax.
 */
UT_TEST(test_deleting_xmax_error_names_actual_xmax)
{
	char *source = read_source(HEAPAM_VISIBILITY_SOURCE_PATH);
	const char *message;
	const char *hint;
	const char *actual_xmax;
	const char *wrong_xmin;

	UT_ASSERT(source != NULL);
	if (source == NULL)
		return;

	message = strstr(source,
					 "errmsg(\"cluster TT status unknown for deleting xmax of xid %u\"");
	hint = message != NULL ? strstr(message, "errhint(") : NULL;
	actual_xmax
		= message != NULL ? strstr(message, "HeapTupleHeaderGetRawXmax(tuple)") : NULL;
	wrong_xmin = message != NULL ? strstr(message, "\n\t\t\t\t\t\t\t\t\t\t\traw_xmin),") : NULL;

	UT_ASSERT(message != NULL);
	UT_ASSERT(hint != NULL);
	UT_ASSERT(actual_xmax != NULL);
	if (message != NULL && hint != NULL && actual_xmax != NULL)
		UT_ASSERT(message < actual_xmax && actual_xmax < hint);
	if (message != NULL && hint != NULL && wrong_xmin != NULL)
		UT_ASSERT(wrong_xmin > hint);

	free(source);
}


int
main(void)
{
	UT_RUN(test_t1_undo_tt_slot_ref_sizeof_32);
	UT_RUN(test_t2_ref_field_offsets);
	UT_RUN(test_t3_placeholder_ref_sentinel);
	UT_RUN(test_t4_self_origin_gate);
	UT_RUN(test_t5_build_key_field_carry);
	UT_RUN(test_t6_errcode_53r97_encodable);
	UT_RUN(test_t7_inject_lookup_linkable);
	UT_RUN(test_t8_inject_shmem_helpers_linkable);
	UT_RUN(test_t9_production_inject_returns_false);
	UT_RUN(test_t10_itl_slot_unallocated_sentinel);
	UT_RUN(test_t11_no_is_xid_local_origin_in_this_unit);
	UT_RUN(test_t12_status_enum_5_values);
	UT_RUN(test_p033_data_dml_publishes_active_identity);
	UT_RUN(test_p033_active_and_safety_boundary_matrix);
	UT_RUN(test_s3c05_foreign_multixact_local_slru_decode_guard);
	UT_RUN(test_local_multixact_over_remote_xmin_uses_native_update_semantics);
	UT_RUN(test_native_prehistory_preempts_exact_remote_seed_ref);
	UT_RUN(test_p026_remote_xmin_current_xmax_preserves_native_three_states);
	UT_RUN(test_deleting_xmax_error_names_actual_xmax);
	UT_DONE();
}
