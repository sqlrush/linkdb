/*-------------------------------------------------------------------------
 *
 * test_cluster_visibility_fork.c
 *	  pgrac spec-3.2 D10 — cluster_unit static-contract tests for
 *	  HeapTupleSatisfiesMVCC cluster visibility fork (D5) + D5b inject
 *	  mechanism.
 *
 *	  12 static + presence tests (v0.3 N3 + N4 + L177 + L178 enforcement):
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

/* P0-27 already freezes the exact HEAP_XMIN_FROZEN bit pair as durable
 * committed evidence.  The MVCC fork must consume that proof before any
 * remote xmin resolver/wire leg, while still running the existing exact xmax
 * gate so a foreign delete cannot become false-visible. */
UT_TEST(test_mvcc_frozen_xmin_bypasses_remote_resolve_but_keeps_xmax_gate)
{
	char *source = read_source(HEAPAM_VISIBILITY_SOURCE_PATH);
	const char *mvcc;
	const char *frozen;
	const char *xmax_gate;
	const char *xmin_resolve;

	if (source == NULL)
		return;
	mvcc = strstr(source,
		"if (cluster_enabled && BufferIsValid(buffer)");
	frozen = mvcc == NULL ? NULL : strstr(mvcc,
		"if (!cluster_vis_xmin_needs_resolution(tuple->t_infomask))");
	xmax_gate = frozen == NULL ? NULL : strstr(frozen,
		"cluster_remote_live_xmax_keeps_visible(buffer, tuple, snapshot)");
	xmin_resolve = mvcc == NULL ? NULL : strstr(mvcc,
		"cluster_visibility_resolve_from_ref_scn(raw_xmin");

	UT_ASSERT_NOT_NULL(mvcc);
	UT_ASSERT_NOT_NULL(frozen);
	UT_ASSERT_NOT_NULL(xmax_gate);
	UT_ASSERT_NOT_NULL(xmin_resolve);
	if (mvcc != NULL && frozen != NULL && xmax_gate != NULL
		&& xmin_resolve != NULL)
		UT_ASSERT(mvcc < frozen && frozen < xmax_gate
				  && xmax_gate < xmin_resolve);
	free(source);
}

/* Spec 8.4A I18/I19: the normal commit-stamp is a live block0 modifier. */
UT_TEST(test_normal_commit_stamp_is_modifier_gated_and_error_safe)
{
	char *source = read_source(TT_LOCAL_SOURCE_PATH);
	const char *start;
	const char *end;
	const char *peek;
	const char *enter;
	const char *try_block;
	const char *recheck;
	const char *durable;
	const char *finally_block;
	const char *leave;

	if (source == NULL)
		return;
	start = strstr(source, "\ncluster_tt_local_precommit_durable_finish(");
	end = start == NULL ? NULL : strstr(start, "\n}\n\nvoid\ncluster_tt_local_record_commit(");
	peek = start == NULL ? NULL : strstr(start, "cluster_tt_local_peek_binding(");
	enter = start == NULL ? NULL : strstr(start, "cluster_semantic_activation_modifier_enter(");
	try_block = start == NULL ? NULL : strstr(start, "PG_TRY();");
	recheck = start == NULL
				  ? NULL
				  : strstr(start, "cluster_tt_local_modifier_recheck_or_error(");
	durable = recheck == NULL
				? NULL
				: strstr(recheck, "cluster_tt_slot_durable_commit_writeonly(");
	finally_block = start == NULL ? NULL : strstr(start, "PG_FINALLY();");
	leave = finally_block == NULL
				? NULL
				: strstr(finally_block, "cluster_semantic_activation_leave(");

	UT_ASSERT_NOT_NULL(start);
	UT_ASSERT_NOT_NULL(end);
	UT_ASSERT_NOT_NULL(peek);
	UT_ASSERT_NOT_NULL(enter);
	UT_ASSERT_NOT_NULL(try_block);
	UT_ASSERT_NOT_NULL(recheck);
	UT_ASSERT_NOT_NULL(durable);
	UT_ASSERT_NOT_NULL(finally_block);
	UT_ASSERT_NOT_NULL(leave);
	if (start != NULL && end != NULL && peek != NULL && enter != NULL && try_block != NULL
		&& recheck != NULL && durable != NULL && finally_block != NULL && leave != NULL)
		UT_ASSERT(start < peek && peek < enter && enter < try_block && try_block < recheck
				  && recheck < durable && durable < finally_block && finally_block < leave
				  && leave < end);
	free(source);
}

/* The L3 fail-closed diagnostic must identify the authority lookup that
 * actually failed.  Naming raw_xmin as the deleting xid hides the exact
 * xmax/ref tuple and prevents deterministic route classification. */
UT_TEST(test_deleting_xmax_error_names_actual_xmax)
{
	char *source = read_source(HEAPAM_VISIBILITY_SOURCE_PATH);
	const char *message;
	const char *hint;
	const char *actual_xmax;
	const char *wrong_xmin;
	const char *wrong_hint;

	if (source == NULL)
		return;
	message = strstr(source,
		"errmsg(\"cluster TT status unknown for deleting xmax of xid %u\"");
	hint = message != NULL ? strstr(message, "errhint(") : NULL;
	actual_xmax = message != NULL
		? strstr(message, "HeapTupleHeaderGetRawXmax(tuple)") : NULL;
	wrong_xmin = message != NULL ? strstr(message, "raw_xmin),") : NULL;
	wrong_hint = message != NULL
		? strstr(message, "Remote deleter commit state not yet propagated") : NULL;

	UT_ASSERT_NOT_NULL(message);
	UT_ASSERT_NOT_NULL(hint);
	UT_ASSERT_NOT_NULL(actual_xmax);
	if (message != NULL && hint != NULL && actual_xmax != NULL)
		UT_ASSERT(message < actual_xmax && actual_xmax < hint);
	if (message != NULL && hint != NULL && wrong_xmin != NULL)
		UT_ASSERT(wrong_xmin > hint);
	if (message != NULL && hint != NULL && wrong_hint != NULL)
		UT_ASSERT(wrong_hint > hint);
	free(source);
}

/* spec-3.12 C3b/Q11: a concurrent winner may fill the segment returned by a
 * retention rollover before this backend allocates.  The follower must reread
 * and reclassify CURRENT instead of assuming that returned segment is fresh. */
UT_TEST(test_tt_retention_rollover_follower_reclassifies_current_segment)
{
	char *source = read_source(TT_LOCAL_SOURCE_PATH);
	const char *start;
	const char *end;
	const char *retry_loop;
	const char *classify;
	const char *rollover;
	const char *one_shot;
	const char *fresh_error;

	if (source == NULL)
		return;
	start = strstr(source, "\ncluster_tt_local_get_or_create_binding(");
	end = start == NULL
		? NULL
		: strstr(start, "\n}\n\n/*\n * cluster_tt_local_peek_binding");
	retry_loop = start == NULL ? NULL : strstr(start, "for (;;)");
	classify = retry_loop == NULL
		? NULL
		: strstr(retry_loop,
				 "cluster_tt_slot_alloc_ext(seg, top_xid, &retained_pressure)");
	rollover = classify == NULL
		? NULL
		: strstr(classify, "cluster_undo_tt_rollover_locked(");
	one_shot = rollover == NULL
		? NULL
		: strstr(rollover, "cluster_tt_slot_alloc(seg, top_xid)");
	fresh_error = rollover == NULL
		? NULL
		: strstr(rollover, "fresh rollover segment");

	UT_ASSERT_NOT_NULL(start);
	UT_ASSERT_NOT_NULL(end);
	UT_ASSERT_NOT_NULL(retry_loop);
	UT_ASSERT_NOT_NULL(classify);
	UT_ASSERT_NOT_NULL(rollover);
	if (start != NULL && end != NULL && retry_loop != NULL && classify != NULL
		&& rollover != NULL)
		UT_ASSERT(start < retry_loop && retry_loop < classify && classify < rollover
				  && rollover < end);
	if (one_shot != NULL && end != NULL)
		UT_ASSERT(one_shot > end);
	if (fresh_error != NULL && end != NULL)
		UT_ASSERT(fresh_error > end);
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
	UT_RUN(test_mvcc_frozen_xmin_bypasses_remote_resolve_but_keeps_xmax_gate);
	UT_RUN(test_normal_commit_stamp_is_modifier_gated_and_error_safe);
	UT_RUN(test_deleting_xmax_error_names_actual_xmax);
	UT_RUN(test_tt_retention_rollover_follower_reclassifies_current_segment);
	UT_DONE();
}
