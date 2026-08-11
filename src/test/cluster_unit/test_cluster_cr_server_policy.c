/*-------------------------------------------------------------------------
 *
 * test_cluster_cr_server_policy.c
 *	  Standalone unit tests for the spec-6.12b CR-server split policy
 *	  (cluster_cr_server_split_classify): FULL / PARTIAL / DENY over
 *	  write_scn-DESC chain-origin sequences, including the malformed-input
 *	  fail-closed and the empty-chain FULL.
 *
 * Portions Copyright (c) 1996-2024, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2026, pgrac contributors
 *
 * Author: SqlRush <sqlrush@gmail.com>
 *
 * IDENTIFICATION
 *	  src/test/cluster_unit/test_cluster_cr_server_policy.c
 *
 * NOTES
 *	  This is a pgrac-original file.  Links cluster_cr_server_policy.o
 *	  only; the policy is pure (no shmem / locks / elog).
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "cluster/cluster_cr_server.h"

#include "unit_test.h"

UT_DEFINE_GLOBALS();

/*
 * TT-P013-RULE25-B RED seam.  The implementation belongs in the pure
 * CR-server policy object after the RED is accepted; keeping the prototypes
 * test-local makes the current product fail at link time without changing a
 * product header.
 *
 * Only a positive origin-live proof may produce IN_PROGRESS.  An explicit
 * origin CLOG abort is positive ABORTED authority, while "not committed" by
 * itself remains UNKNOWN_FAIL_CLOSED.
 */
extern ClusterUndoVerdictKind
cluster_cr_server_resolved_scn_verdict(bool clog_did_commit, bool clog_did_abort,
									   bool xid_is_in_progress);
extern bool cluster_cr_server_live_binding_exact(bool xid_is_mine,
												 uint32 expected_segment_id,
												 uint32 expected_tt_slot_id,
												 uint16 matched_segment,
												 uint16 matched_slot,
												 bool xid_is_in_progress,
												 bool durable_binding_stable);

static char *
read_cr_server_source(void)
{
	const char *source_file = __FILE__;
	const char *source_suffix = "/src/test/cluster_unit/test_cluster_cr_server_policy.c";
	const char *suffix_at = strstr(source_file, source_suffix);
	char path[MAXPGPATH];
	FILE *file;
	long length;
	char *source;

	if (suffix_at != NULL)
		snprintf(path, sizeof(path), "%.*s/src/backend/cluster/cluster_cr_server.c",
				 (int)(suffix_at - source_file), source_file);
	else
		snprintf(path, sizeof(path), "../../../src/backend/cluster/cluster_cr_server.c");

	file = fopen(path, "rb");
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

/* Assert hook stub so the cassert libpgport links standalone. */
void
ExceptionalCondition(const char *conditionName pg_attribute_unused(),
					 const char *fileName pg_attribute_unused(),
					 int lineNumber pg_attribute_unused())
{
	abort();
}

UT_TEST(test_split_empty_is_full_prefix_zero)
{
	int prefix = -1;

	UT_ASSERT_EQ((int)cluster_cr_server_split_classify(NULL, 0, 0, &prefix),
				 (int)CLUSTER_CR_SPLIT_FULL);
	UT_ASSERT_EQ(prefix, 0);
}

UT_TEST(test_split_all_self_is_full)
{
	int32 origins[3] = { 0, 0, 0 };
	int prefix = -1;

	UT_ASSERT_EQ((int)cluster_cr_server_split_classify(origins, 3, 0, &prefix),
				 (int)CLUSTER_CR_SPLIT_FULL);
	UT_ASSERT_EQ(prefix, 3);
}

UT_TEST(test_split_self_prefix_foreign_suffix_is_partial)
{
	int32 origins[4] = { 0, 0, 1, 1 };
	int prefix = -1;

	UT_ASSERT_EQ((int)cluster_cr_server_split_classify(origins, 4, 0, &prefix),
				 (int)CLUSTER_CR_SPLIT_PARTIAL);
	UT_ASSERT_EQ(prefix, 2);
}

UT_TEST(test_split_all_foreign_is_partial_prefix_zero)
{
	/* Serving nothing is still a legal one-hop reply: the shipped current
	 * copy lets the requester do the whole peel (equivalent to a plain
	 * read image + local construction). */
	int32 origins[2] = { 1, 1 };
	int prefix = -1;

	UT_ASSERT_EQ((int)cluster_cr_server_split_classify(origins, 2, 0, &prefix),
				 (int)CLUSTER_CR_SPLIT_PARTIAL);
	UT_ASSERT_EQ(prefix, 0);
}

UT_TEST(test_split_interleave_is_deny)
{
	int32 self_after_foreign[3] = { 0, 1, 0 };
	int32 foreign_then_self[2] = { 1, 0 };
	int prefix = 77;

	UT_ASSERT_EQ((int)cluster_cr_server_split_classify(self_after_foreign, 3, 0, &prefix),
				 (int)CLUSTER_CR_SPLIT_DENY);
	UT_ASSERT_EQ((int)cluster_cr_server_split_classify(foreign_then_self, 2, 0, NULL),
				 (int)CLUSTER_CR_SPLIT_DENY);
}

UT_TEST(test_split_third_party_suffix_stays_partial)
{
	/* A >=3-node foreign suffix mixing OTHER nodes is still a clean
	 * self-prefix cut here; the REQUESTER's continue-run hits its own
	 * class-(3) walk backstop for the third-party chains (53R9G). */
	int32 origins[3] = { 0, 1, 2 };
	int prefix = -1;

	UT_ASSERT_EQ((int)cluster_cr_server_split_classify(origins, 3, 0, &prefix),
				 (int)CLUSTER_CR_SPLIT_PARTIAL);
	UT_ASSERT_EQ(prefix, 1);
}

UT_TEST(test_split_malformed_is_deny)
{
	UT_ASSERT_EQ((int)cluster_cr_server_split_classify(NULL, 2, 0, NULL),
				 (int)CLUSTER_CR_SPLIT_DENY);
	UT_ASSERT_EQ((int)cluster_cr_server_split_classify(NULL, -1, 0, NULL),
				 (int)CLUSTER_CR_SPLIT_DENY);
}

/*
 * spec-7.1 D1 serve: the INVALID_SCN verdict decision.  An explicit CLOG abort
 * upgrades to a positive ABORTED (the aborted-unstamped delayed-cleanout
 * window); everything else -- committed-but-unstamped, in-flight, 2PC, crashed
 * -- must stay REFUSE (8.A: never fabricate a commit_scn / positive answer).
 */
UT_TEST(test_invalid_scn_aborted_is_positive)
{
	UT_ASSERT_EQ((int)cluster_cr_server_invalid_scn_verdict(true),
				 (int)CLUSTER_CR_INVALID_SCN_ABORTED);
}

UT_TEST(test_invalid_scn_not_aborted_refuses)
{
	/* The 8.A tooth: a NON-abort (committed-unstamped / in-flight / in-doubt)
	 * must NOT upgrade -- it stays fail-closed on the refuse leg. */
	UT_ASSERT_EQ((int)cluster_cr_server_invalid_scn_verdict(false),
				 (int)CLUSTER_CR_INVALID_SCN_REFUSE);
}

/*
 * TT-P013-RULE25-B: split the current LMS_OWN_XID_REFUSE_OTHER bucket at the
 * exact RESOLVED_SCN positive-proof branches.  The origin's own ProcArray is
 * live authority, CLOG is terminal authority, and an unproved non-commit
 * remains UNKNOWN.
 */
UT_TEST(test_resolved_scn_live_xid_is_in_progress_not_other)
{
	UT_ASSERT_EQ((int)cluster_cr_server_resolved_scn_verdict(false, false, true),
				 (int)CLUSTER_UNDO_VERDICT_IN_PROGRESS);
}

UT_TEST(test_resolved_scn_terminal_and_unknown_boundaries)
{
	UT_ASSERT_EQ((int)cluster_cr_server_resolved_scn_verdict(true, false, false),
				 (int)CLUSTER_UNDO_VERDICT_COMMITTED_EXACT);
	UT_ASSERT_EQ((int)cluster_cr_server_resolved_scn_verdict(true, true, true),
				 (int)CLUSTER_UNDO_VERDICT_COMMITTED_EXACT);
	UT_ASSERT_EQ((int)cluster_cr_server_resolved_scn_verdict(false, false, false),
				 (int)CLUSTER_UNDO_VERDICT_UNKNOWN_FAIL_CLOSED);
}

UT_TEST(test_resolved_scn_live_requires_every_exact_binding_gate)
{
	/* matched_slot is allocator-internal 0-based; wire slot id is 1-based. */
	UT_ASSERT(cluster_cr_server_live_binding_exact(true, 17, 4, 17, 3, true, true));
	UT_ASSERT(!cluster_cr_server_live_binding_exact(false, 17, 4, 17, 3, true, true));
	UT_ASSERT(!cluster_cr_server_live_binding_exact(true, 18, 4, 17, 3, true, true));
	UT_ASSERT(!cluster_cr_server_live_binding_exact(true, 17, 5, 17, 3, true, true));
	UT_ASSERT(!cluster_cr_server_live_binding_exact(true, 17, 0, 17, 3, true, true));
	UT_ASSERT(!cluster_cr_server_live_binding_exact(true, 17, 49, 17, 3, true, true));
	UT_ASSERT(!cluster_cr_server_live_binding_exact(true, 17, 4, 17, 3, false, true));
	UT_ASSERT(!cluster_cr_server_live_binding_exact(true, 17, 4, 17, 3, true, false));
}

/*
 * The durable RESOLVED_SCN stamp precedes the CLOG terminal record.  Once
 * the exact origin binding has an explicit abort, ABORTED must win before
 * the unproved UNKNOWN leg; crash-lost/in-doubt remains fail-closed.
 */
UT_TEST(test_resolved_scn_explicit_abort_after_stamp_is_positive)
{
	char *source = read_cr_server_source();
	const char *resolved
		= source != NULL ? strstr(source, "case CLUSTER_TT_DURABLE_RESOLVED_SCN:") : NULL;
	const char *resolved_end
		= resolved != NULL ? strstr(resolved, "if (cluster_cr_accept_resolved_scn(scn))") : NULL;
	const char *exact_gate
		= resolved != NULL ? strstr(resolved, "if (exact_binding)") : NULL;
	const char *did_abort
		= exact_gate != NULL ? strstr(exact_gate, "TransactionIdDidAbort(xid)") : NULL;
	const char *abort_verdict
		= did_abort != NULL ? strstr(did_abort, "CLUSTER_UNDO_VERDICT_ABORTED") : NULL;
	const char *unknown
		= abort_verdict != NULL
			  ? strstr(abort_verdict, "CLUSTER_UNDO_VERDICT_UNKNOWN_FAIL_CLOSED")
			  : NULL;

	UT_ASSERT_EQ((int)cluster_cr_server_resolved_scn_verdict(false, true, false),
				 (int)CLUSTER_UNDO_VERDICT_ABORTED);
	UT_ASSERT_EQ((int)cluster_cr_server_resolved_scn_verdict(false, false, false),
				 (int)CLUSTER_UNDO_VERDICT_UNKNOWN_FAIL_CLOSED);

	UT_ASSERT_NOT_NULL(resolved);
	UT_ASSERT_NOT_NULL(resolved_end);
	UT_ASSERT_NOT_NULL(exact_gate);
	UT_ASSERT_NOT_NULL(did_abort);
	UT_ASSERT_NOT_NULL(abort_verdict);
	UT_ASSERT_NOT_NULL(unknown);
	if (resolved_end != NULL) {
		UT_ASSERT(exact_gate != NULL && exact_gate < resolved_end);
		UT_ASSERT(did_abort != NULL && did_abort < resolved_end);
		UT_ASSERT(abort_verdict != NULL && abort_verdict < resolved_end);
		UT_ASSERT(unknown != NULL && unknown < resolved_end);
	}
	free(source);
}

/*
 * D11 R19: UNDO_MULTI_VERDICT remains park-only.  Freeze the defensive inline
 * entry separately: an explicit kind case must jump over both the serve call
 * and inline-serve accounting to the pre-set DENIED one-reply path.
 */
UT_TEST(test_undo_multi_verdict_inline_entry_is_denied_without_serve)
{
	char *source = read_cr_server_source();
	const char *function
		= source != NULL ? strstr(source, "\ncluster_gcs_block_forward_serve_inline(") : NULL;
	const char *function_end = function != NULL ? strstr(function, "\n}\n\n#endif") : NULL;
	const char *kind_switch = function != NULL ? strstr(function, "\tswitch (kind) {") : NULL;
	const char *multi_case
		= kind_switch != NULL
			  ? strstr(kind_switch, "case CLUSTER_LMS_SLOT_KIND_UNDO_MULTI_VERDICT:")
			  : NULL;
	const char *deny_jump
		= multi_case != NULL ? strstr(multi_case, "goto inline_deny_no_serve;") : NULL;
	const char *serve = kind_switch != NULL ? strstr(kind_switch, "cr_serve_slot(&slot);") : NULL;
	const char *inline_note
		= serve != NULL ? strstr(serve, "cluster_lms_obs_note_inline_serve(") : NULL;
	const char *deny_label
		= inline_note != NULL ? strstr(inline_note, "\ninline_deny_no_serve:") : NULL;
	const char *reply
		= deny_label != NULL ? strstr(deny_label, "cr_build_and_send_reply(&slot);") : NULL;
	const char *direct_note
		= reply != NULL ? strstr(reply, "cluster_lms_obs_note_direct_reply();") : NULL;

	UT_ASSERT_NOT_NULL(function);
	UT_ASSERT_NOT_NULL(function_end);
	UT_ASSERT_NOT_NULL(kind_switch);
	UT_ASSERT_NOT_NULL(multi_case);
	UT_ASSERT_NOT_NULL(deny_jump);
	UT_ASSERT_NOT_NULL(serve);
	UT_ASSERT_NOT_NULL(inline_note);
	UT_ASSERT_NOT_NULL(deny_label);
	UT_ASSERT_NOT_NULL(reply);
	UT_ASSERT_NOT_NULL(direct_note);
	if (function != NULL && function_end != NULL && kind_switch != NULL && multi_case != NULL
		&& deny_jump != NULL && serve != NULL && inline_note != NULL && deny_label != NULL
		&& reply != NULL && direct_note != NULL)
		UT_ASSERT(function < kind_switch && kind_switch < multi_case && multi_case < deny_jump
				  && deny_jump < serve && serve < inline_note && inline_note < deny_label
				  && deny_label < reply && reply < direct_note && direct_note < function_end);
	free(source);
}

int
main(void)
{
	UT_PLAN(14);
	UT_RUN(test_split_empty_is_full_prefix_zero);
	UT_RUN(test_split_all_self_is_full);
	UT_RUN(test_split_self_prefix_foreign_suffix_is_partial);
	UT_RUN(test_split_all_foreign_is_partial_prefix_zero);
	UT_RUN(test_split_interleave_is_deny);
	UT_RUN(test_split_third_party_suffix_stays_partial);
	UT_RUN(test_split_malformed_is_deny);
	UT_RUN(test_invalid_scn_aborted_is_positive);
	UT_RUN(test_invalid_scn_not_aborted_refuses);
	UT_RUN(test_resolved_scn_live_xid_is_in_progress_not_other);
	UT_RUN(test_resolved_scn_terminal_and_unknown_boundaries);
	UT_RUN(test_resolved_scn_live_requires_every_exact_binding_gate);
	UT_RUN(test_resolved_scn_explicit_abort_after_stamp_is_positive);
	UT_RUN(test_undo_multi_verdict_inline_entry_is_denied_without_serve);
	UT_DONE();
	return ut_failed_count == 0 ? 0 : 1;
}
