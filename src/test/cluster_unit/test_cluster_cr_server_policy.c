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
 *	  only; the policy is pure (no shmem / locks / elog).  It also reads
 *	  cluster_cr_server.c to pin fail-closed error-observation ordering.
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include <signal.h>
#include <sys/wait.h>
#include <unistd.h>

#include "cluster/cluster_cr_server.h"
#include "cluster/cluster_tt_slot.h"
#include "cluster/cluster_tt_status.h"

#include "unit_test.h"

UT_DEFINE_GLOBALS();

/*
 * S3-P0-13 RED seam.  The implementation belongs in the pure CR-server
 * policy object after the RED is accepted; keeping the prototype test-local
 * makes the old product fail at link time without changing a product header.
 *
 * Only a positive origin-live proof may produce IN_PROGRESS.  "Not
 * committed" by itself remains UNKNOWN_FAIL_CLOSED.
 */
extern ClusterUndoVerdictKind
cluster_cr_server_resolved_scn_verdict(bool clog_did_commit, bool clog_did_abort,
									   bool xid_is_in_progress);
extern ClusterUndoVerdictKind
cluster_cr_server_resolved_scn_resampled_verdict(bool clog_did_commit_before,
												 bool clog_did_abort_before,
												 bool xid_is_in_progress,
												 bool clog_did_commit_after,
												 bool clog_did_abort_after);
extern bool cluster_cr_server_live_binding_exact(bool xid_is_mine,
												 uint32 expected_segment_id,
												 uint32 expected_tt_slot_id,
												 uint16 matched_segment,
												 uint16 matched_slot,
												 bool xid_is_in_progress,
												 bool durable_binding_stable);

/*
 * S3-P0-03 r57 RED seam.  Keep the wished-for pure admission API test-local
 * until product work is authorized.  Weak linkage lets the old product run
 * the existing 27-test baseline plus the source-linked RED; this matrix then
 * reports the missing policy as its own single RED instead of stopping at
 * link time.
 */
extern bool cluster_cr_server_live_overlay_admissible(
	bool allow_live, const ClusterTTStatusKey *key,
	uint16 self_node_id, uint32 expected_segment_id,
	uint32 expected_tt_slot_id, uint64 authority_epoch,
	TransactionId xid, bool lookup_found,
	const ClusterTTStatusResult *result);

/*
 * S3-P0-13 r39 observation seam.  Keep the wished-for API test-local until
 * the old product has failed this RED at link time.  The product header
 * defines the marker together with the real enums during GREEN.
 */
#ifndef CLUSTER_CR_SERVER_DIAGNOSTIC_DEFINED
typedef enum ClusterCrServerExactDiagnostic {
	CLUSTER_CR_SERVER_EXACT_OK = 0,
	CLUSTER_CR_SERVER_EXACT_NOT_AUTHORITATIVE,
	CLUSTER_CR_SERVER_EXACT_NOT_MINE,
	CLUSTER_CR_SERVER_EXACT_EXPECTED_SEGMENT_INVALID,
	CLUSTER_CR_SERVER_EXACT_EXPECTED_SLOT_INVALID,
	CLUSTER_CR_SERVER_EXACT_SEGMENT_MISMATCH,
	CLUSTER_CR_SERVER_EXACT_SLOT_MISMATCH
} ClusterCrServerExactDiagnostic;

typedef enum ClusterCrServerConfirmDiagnostic {
	CLUSTER_CR_SERVER_CONFIRM_STABLE = 0,
	CLUSTER_CR_SERVER_CONFIRM_RESOLVE_KIND,
	CLUSTER_CR_SERVER_CONFIRM_SEGMENT_MISMATCH,
	CLUSTER_CR_SERVER_CONFIRM_SLOT_MISMATCH,
	CLUSTER_CR_SERVER_CONFIRM_WRAP_MISMATCH,
	CLUSTER_CR_SERVER_CONFIRM_SCN_MISMATCH
} ClusterCrServerConfirmDiagnostic;

extern ClusterCrServerExactDiagnostic
cluster_cr_server_exact_diagnostic(bool allow_live, bool xid_is_mine,
								   uint32 expected_segment_id,
								   uint32 expected_tt_slot_id,
								   uint16 matched_segment,
								   uint16 matched_slot);
extern ClusterCrServerConfirmDiagnostic
cluster_cr_server_confirm_diagnostic(bool confirm_resolved_scn,
									 uint16 matched_segment,
									 uint16 matched_slot,
									 uint16 matched_wrap,
									 SCN matched_scn,
									 uint16 confirm_segment,
									 uint16 confirm_slot,
									 uint16 confirm_wrap,
									 SCN confirm_scn);
#endif

static char *
read_cr_server_source(void)
{
	FILE *file;
	long length;
	char *source;

	file = fopen(CLUSTER_CR_SERVER_SOURCE_PATH, "rb");
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

static const char *
find_before(const char *start, const char *end, const char *needle)
{
	const char *match;

	if (start == NULL || end == NULL)
		return NULL;
	match = strstr(start, needle);
	return match != NULL && match < end ? match : NULL;
}

/*
 * Source-linked identity contracts must match a whole trimmed source line.
 * A plain strstr("field = value") would also accept "field = value + 1",
 * turning the exact-key mutation into a false green.
 */
static const char *
find_exact_line_before(const char *start, const char *end,
					   const char *expected)
{
	const char *cursor = start;
	size_t expected_len;

	if (start == NULL || end == NULL || expected == NULL)
		return NULL;
	expected_len = strlen(expected);
	while (cursor < end) {
		const char *match = strstr(cursor, expected);
		const char *line_start;
		const char *line_end;

		if (match == NULL || match >= end)
			return NULL;
		line_start = match;
		while (line_start > start && line_start[-1] != '\n')
			line_start--;
		while (line_start < match
			   && (*line_start == ' ' || *line_start == '\t'))
			line_start++;
		line_end = match + expected_len;
		while (line_end < end && (*line_end == ' ' || *line_end == '\t'))
			line_end++;
		if (line_start == match
			&& (line_end == end || *line_end == '\n' || *line_end == '\r'))
			return match;
		cursor = match + 1;
	}
	return NULL;
}

static int
count_occurrences(const char *source, const char *needle)
{
	int count = 0;
	size_t needle_len = strlen(needle);
	const char *cursor = source;

	if (source == NULL || needle_len == 0)
		return 0;
	while ((cursor = strstr(cursor, needle)) != NULL) {
		count++;
		cursor += needle_len;
	}
	return count;
}

/* Assert hook stub so the cassert libpgport links standalone. */
void
ExceptionalCondition(const char *conditionName pg_attribute_unused(),
					 const char *fileName pg_attribute_unused(),
					 int lineNumber pg_attribute_unused())
{
	abort();
}

/*
 * Old-product fallback for the wished-for P0-03 pure policy.  A future strong
 * definition in cluster_cr_server_policy.o overrides this weak test stub.
 * Returning false preserves fail-closed behavior and makes only the positive
 * matrix row RED; every safety-negative row remains green on old code.
 */
__attribute__((weak)) bool
cluster_cr_server_live_overlay_admissible(
	bool allow_live pg_attribute_unused(),
	const ClusterTTStatusKey *key pg_attribute_unused(),
	uint16 self_node_id pg_attribute_unused(),
	uint32 expected_segment_id pg_attribute_unused(),
	uint32 expected_tt_slot_id pg_attribute_unused(),
	uint64 authority_epoch pg_attribute_unused(),
	TransactionId xid pg_attribute_unused(),
	bool lookup_found pg_attribute_unused(),
	const ClusterTTStatusResult *result pg_attribute_unused())
{
	return false;
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
 * S3-P0-13: split the current LMS_OWN_XID_REFUSE_OTHER bucket at the exact
 * RESOLVED_SCN + CLOG-IN_PROGRESS branch.  The origin's own ProcArray is the
 * positive live authority: it yields only IN_PROGRESS.  Commit remains
 * CLOG-authoritative, and an unproved non-commit remains UNKNOWN.
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
				 (int)CLUSTER_UNDO_VERDICT_UNKNOWN_FAIL_CLOSED);
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
 * r39 counter attribution must be mutually exclusive.  These literals pin
 * the precedence that makes one authoritative request land in exactly one
 * exact-gate bucket even when several inputs are malformed.
 */
UT_TEST(test_exact_diagnostic_is_one_mutually_exclusive_reason)
{
	UT_ASSERT_EQ((int)cluster_cr_server_exact_diagnostic(true, true, 17, 4, 17, 3),
				 (int)CLUSTER_CR_SERVER_EXACT_OK);
	UT_ASSERT_EQ((int)cluster_cr_server_exact_diagnostic(false, false, 0, 0, 18, 9),
				 (int)CLUSTER_CR_SERVER_EXACT_NOT_AUTHORITATIVE);
	UT_ASSERT_EQ((int)cluster_cr_server_exact_diagnostic(true, false, 0, 0, 18, 9),
				 (int)CLUSTER_CR_SERVER_EXACT_NOT_MINE);
	UT_ASSERT_EQ((int)cluster_cr_server_exact_diagnostic(true, true, 0, 0, 18, 9),
				 (int)CLUSTER_CR_SERVER_EXACT_EXPECTED_SEGMENT_INVALID);
	UT_ASSERT_EQ(
		(int)cluster_cr_server_exact_diagnostic(true, true, UINT16_MAX + 1U, 0, 18, 9),
		(int)CLUSTER_CR_SERVER_EXACT_EXPECTED_SEGMENT_INVALID);
	UT_ASSERT_EQ((int)cluster_cr_server_exact_diagnostic(true, true, 17, 0, 18, 9),
				 (int)CLUSTER_CR_SERVER_EXACT_EXPECTED_SLOT_INVALID);
	UT_ASSERT_EQ((int)cluster_cr_server_exact_diagnostic(
					 true, true, 17, 49, 18, 9),
				 (int)CLUSTER_CR_SERVER_EXACT_EXPECTED_SLOT_INVALID);
	UT_ASSERT_EQ((int)cluster_cr_server_exact_diagnostic(true, true, 17, 4, 18, 9),
				 (int)CLUSTER_CR_SERVER_EXACT_SEGMENT_MISMATCH);
	UT_ASSERT_EQ((int)cluster_cr_server_exact_diagnostic(true, true, 17, 4, 17, 9),
				 (int)CLUSTER_CR_SERVER_EXACT_SLOT_MISMATCH);
}

UT_TEST(test_confirm_diagnostic_is_one_mutually_exclusive_reason)
{
	UT_ASSERT_EQ((int)cluster_cr_server_confirm_diagnostic(
					 true, 17, 3, 9, 101, 17, 3, 9, 101),
				 (int)CLUSTER_CR_SERVER_CONFIRM_STABLE);
	UT_ASSERT_EQ((int)cluster_cr_server_confirm_diagnostic(
					 false, 17, 3, 9, 101, 18, 4, 10, 102),
				 (int)CLUSTER_CR_SERVER_CONFIRM_RESOLVE_KIND);
	UT_ASSERT_EQ((int)cluster_cr_server_confirm_diagnostic(
					 true, 17, 3, 9, 101, 18, 4, 10, 102),
				 (int)CLUSTER_CR_SERVER_CONFIRM_SEGMENT_MISMATCH);
	UT_ASSERT_EQ((int)cluster_cr_server_confirm_diagnostic(
					 true, 17, 3, 9, 101, 17, 4, 10, 102),
				 (int)CLUSTER_CR_SERVER_CONFIRM_SLOT_MISMATCH);
	UT_ASSERT_EQ((int)cluster_cr_server_confirm_diagnostic(
					 true, 17, 3, 9, 101, 17, 3, 10, 102),
				 (int)CLUSTER_CR_SERVER_CONFIRM_WRAP_MISMATCH);
	UT_ASSERT_EQ((int)cluster_cr_server_confirm_diagnostic(
					 true, 17, 3, 9, 101, 17, 3, 9, 102),
				 (int)CLUSTER_CR_SERVER_CONFIRM_SCN_MISMATCH);
}

UT_TEST(test_other_refusal_detail_roster_is_closed_and_invalid_fails_safe)
{
	int i;

	UT_ASSERT_EQ((int)CLUSTER_CR_SERVER_OTHER_DETAIL_COUNT, 13);
	for (i = 0; i < (int)CLUSTER_CR_SERVER_OTHER_DETAIL_COUNT; i++)
		UT_ASSERT_EQ(
			(int)cluster_cr_server_other_refusal_detail_normalize(
				(ClusterCrServerOtherRefusalDetail)i),
			i);

#ifdef USE_ASSERT_CHECKING
	{
		pid_t child;
		int status = 0;

		fflush(NULL);
		child = fork();
		UT_ASSERT(child >= 0);
		if (child == 0) {
			(void)cluster_cr_server_other_refusal_detail_normalize(
				(ClusterCrServerOtherRefusalDetail)
					CLUSTER_CR_SERVER_OTHER_DETAIL_COUNT);
			_exit(0);
		}
		if (child > 0) {
			UT_ASSERT_EQ(waitpid(child, &status, 0), child);
			UT_ASSERT(WIFSIGNALED(status));
			if (WIFSIGNALED(status))
				UT_ASSERT_EQ(WTERMSIG(status), SIGABRT);
		}
	}
#else
	UT_ASSERT_EQ(
		(int)cluster_cr_server_other_refusal_detail_normalize(
			(ClusterCrServerOtherRefusalDetail)-1),
		(int)CLUSTER_CR_SERVER_OTHER_RESIDUAL);
	UT_ASSERT_EQ(
		(int)cluster_cr_server_other_refusal_detail_normalize(
			(ClusterCrServerOtherRefusalDetail)
				CLUSTER_CR_SERVER_OTHER_DETAIL_COUNT),
		(int)CLUSTER_CR_SERVER_OTHER_RESIDUAL);
#endif
}

UT_TEST(test_srv_other_producers_share_one_detail_then_aggregate_wrapper)
{
	char *source = read_cr_server_source();
	const char *wrapper
		= source != NULL ? strstr(source, "\nlms_note_other_refusal_detail(") : NULL;
	const char *wrapper_end = wrapper != NULL ? strstr(wrapper, "\n}") : NULL;
	const char *detail_bump
		= find_before(wrapper, wrapper_end,
					  "cluster_cr_server_other_refusal_detail_bump(detail);");
	const char *aggregate_bump
		= find_before(wrapper, wrapper_end, "cluster_vis53r97_note_srv_other();");
	const char *invalid_xid
		= source != NULL ? strstr(source, "if (!TransactionIdIsNormal(xid))") : NULL;
	const char *invalid_xid_end
		= invalid_xid != NULL ? strstr(invalid_xid, "return false;") : NULL;
	const char *invalid_xid_note
		= find_before(invalid_xid, invalid_xid_end, "lms_note_other_refusal_detail(");
	const char *not_mine = invalid_xid_end != NULL
							   ? strstr(invalid_xid_end,
										"if (!slot->undo_authoritative "
										"&& !cluster_xid_is_mine(xid))")
							   : NULL;
	const char *not_mine_end
		= not_mine != NULL ? strstr(not_mine, "return false;") : NULL;
	const char *not_mine_note
		= find_before(not_mine, not_mine_end, "lms_note_other_refusal_detail(");
	const char *other_case
		= not_mine_end != NULL ? strstr(not_mine_end, "case LMS_OWN_XID_REFUSE_OTHER:")
							  : NULL;
	const char *other_case_end
		= other_case != NULL ? strstr(other_case, "return false;") : NULL;
	const char *other_case_note
		= find_before(other_case, other_case_end, "lms_note_other_refusal_detail(");

	UT_ASSERT_NOT_NULL(source);
	UT_ASSERT_EQ(count_occurrences(source, "cluster_vis53r97_note_srv_other();"), 1);
	UT_ASSERT_EQ(
		count_occurrences(source, "cluster_cr_server_other_refusal_detail_bump(detail);"),
		1);
	UT_ASSERT_EQ(count_occurrences(source, "lms_note_other_refusal_detail("), 4);
	UT_ASSERT_NOT_NULL(wrapper);
	UT_ASSERT_NOT_NULL(wrapper_end);
	UT_ASSERT_NOT_NULL(detail_bump);
	UT_ASSERT_NOT_NULL(aggregate_bump);
	if (detail_bump != NULL && aggregate_bump != NULL)
		UT_ASSERT(detail_bump < aggregate_bump);
	UT_ASSERT_NOT_NULL(invalid_xid_note);
	UT_ASSERT_NOT_NULL(not_mine_note);
	UT_ASSERT_NOT_NULL(other_case_note);
	free(source);
}

UT_TEST(test_resolved_scn_runtime_wires_mutually_exclusive_diagnostics)
{
	char *source = read_cr_server_source();
	const char *resolved
		= source != NULL ? strstr(source, "case CLUSTER_TT_DURABLE_RESOLVED_SCN:") : NULL;
	const char *resolved_end
		= resolved != NULL ? strstr(resolved, "if (cluster_cr_accept_resolved_scn(scn))") : NULL;
	const char *exact
		= find_before(resolved, resolved_end, "cluster_cr_server_exact_diagnostic(");
	const char *confirm
		= find_before(exact, resolved_end, "cluster_cr_server_confirm_diagnostic(");
	const char *detail
		= source != NULL ? strstr(source, "lms_note_other_refusal_detail(") : NULL;
	const char *other
		= detail != NULL ? strstr(detail, "cluster_vis53r97_note_srv_other();") : NULL;

	UT_ASSERT_NOT_NULL(resolved);
	UT_ASSERT_NOT_NULL(resolved_end);
	UT_ASSERT_NOT_NULL(exact);
	UT_ASSERT_NOT_NULL(confirm);
	UT_ASSERT_NOT_NULL(detail);
	UT_ASSERT_NOT_NULL(other);
	if (resolved != NULL && resolved_end != NULL && exact != NULL && confirm != NULL)
		UT_ASSERT(resolved < exact && exact < confirm && confirm < resolved_end);
	free(source);
}

/*
 * S3-P0-13 RED-3: terminal publication can race the origin's two independent
 * samples.  The old order is:
 *
 *   DidAbort(xid) == false
 *       -> owner publishes ABORTED in CLOG and exits ProcArray
 *   TransactionIdIsInProgress(xid) == false
 *
 * Treating those two false samples as one coherent UNKNOWN observation leaks
 * LMS_OWN_XID_REFUSE_OTHER to the fresh authoritative requester (53R97).
 * Re-sampling positive CLOG terminal authority after a non-live ProcArray
 * result closes the gap without widening an in-doubt all-false outcome.
 */
UT_TEST(test_resolved_scn_terminal_resample_closes_abort_publication_gap)
{
	UT_ASSERT_EQ((int)cluster_cr_server_resolved_scn_resampled_verdict(
					 false, false, false, false, true),
				 (int)CLUSTER_UNDO_VERDICT_ABORTED);
	UT_ASSERT_EQ((int)cluster_cr_server_resolved_scn_resampled_verdict(
					 false, false, false, true, false),
				 (int)CLUSTER_UNDO_VERDICT_COMMITTED_EXACT);
}

UT_TEST(test_resolved_scn_terminal_resample_preserves_live_and_unknown_boundaries)
{
	UT_ASSERT_EQ((int)cluster_cr_server_resolved_scn_resampled_verdict(
					 false, false, true, false, false),
				 (int)CLUSTER_UNDO_VERDICT_IN_PROGRESS);
	UT_ASSERT_EQ((int)cluster_cr_server_resolved_scn_resampled_verdict(
					 false, false, false, false, false),
				 (int)CLUSTER_UNDO_VERDICT_UNKNOWN_FAIL_CLOSED);
	UT_ASSERT_EQ((int)cluster_cr_server_resolved_scn_resampled_verdict(
					 true, false, false, false, false),
				 (int)CLUSTER_UNDO_VERDICT_COMMITTED_EXACT);
	UT_ASSERT_EQ((int)cluster_cr_server_resolved_scn_resampled_verdict(
					 true, true, false, false, false),
				 (int)CLUSTER_UNDO_VERDICT_UNKNOWN_FAIL_CLOSED);
	UT_ASSERT_EQ((int)cluster_cr_server_resolved_scn_resampled_verdict(
					 false, false, false, true, true),
				 (int)CLUSTER_UNDO_VERDICT_UNKNOWN_FAIL_CLOSED);
}

UT_TEST(test_terminal_resample_requires_authoritative_exact_non_live_gap)
{
	UT_ASSERT(cluster_cr_server_terminal_resample_allowed(
		true, true, false, false, false));
	UT_ASSERT(!cluster_cr_server_terminal_resample_allowed(
		false, true, false, false, false));
	UT_ASSERT(!cluster_cr_server_terminal_resample_allowed(
		true, false, false, false, false));
	UT_ASSERT(!cluster_cr_server_terminal_resample_allowed(
		true, true, true, false, false));
	UT_ASSERT(!cluster_cr_server_terminal_resample_allowed(
		true, true, false, true, false));
	UT_ASSERT(!cluster_cr_server_terminal_resample_allowed(
		true, true, false, false, true));
}

UT_TEST(test_resolved_scn_runtime_resamples_terminal_after_non_live)
{
	char *source = read_cr_server_source();
	const char *resolved
		= source != NULL ? strstr(source, "case CLUSTER_TT_DURABLE_RESOLVED_SCN:") : NULL;
	const char *resolved_end
		= resolved != NULL ? strstr(resolved, "if (cluster_cr_accept_resolved_scn(scn))") : NULL;
	const char *authoritative_gate
		= find_before(resolved, resolved_end,
					  "if (!did_commit_before) {");
	const char *exact_gate
		= find_before(authoritative_gate, resolved_end, "if (exact_binding)");
	const char *pre_abort
		= find_before(exact_gate, resolved_end,
					  "did_abort_before = TransactionIdDidAbort(xid);");
	const char *pre_abort_guard
		= find_before(pre_abort, resolved_end, "if (!did_abort_before)");
	const char *procarray
		= find_before(pre_abort_guard, resolved_end,
					  "xid_is_in_progress = TransactionIdIsInProgress(xid);");
	const char *resample_gate
		= find_before(procarray, resolved_end,
					  "if (cluster_cr_server_terminal_resample_allowed(");
	const char *gate_allow
		= find_before(resample_gate, resolved_end, "allow_live");
	const char *gate_exact
		= find_before(gate_allow, resolved_end, "exact_binding");
	const char *gate_commit
		= find_before(gate_exact, resolved_end, "did_commit_before");
	const char *gate_abort
		= find_before(gate_commit, resolved_end, "did_abort_before");
	const char *gate_live
		= find_before(gate_abort, resolved_end, "xid_is_in_progress");
	const char *post_commit
		= find_before(gate_live, resolved_end,
					  "did_commit_after = TransactionIdDidCommit(xid);");
	const char *post_commit_guard
		= find_before(post_commit, resolved_end, "if (!did_commit_after)");
	const char *post_abort
		= find_before(post_commit_guard, resolved_end,
					  "did_abort_after = TransactionIdDidAbort(xid);");
	const char *resampled
		= find_before(post_abort, resolved_end,
					  "cluster_cr_server_resolved_scn_resampled_verdict(");
	const char *arg_commit_before
		= find_before(resampled, resolved_end, "did_commit_before");
	const char *arg_abort_before
		= find_before(arg_commit_before, resolved_end, "did_abort_before");
	const char *arg_live
		= find_before(arg_abort_before, resolved_end, "exact_live");
	const char *arg_commit_after
		= find_before(arg_live, resolved_end, "did_commit_after");
	const char *arg_abort_after
		= find_before(arg_commit_after, resolved_end, "did_abort_after");

	UT_ASSERT_NOT_NULL(resolved);
	UT_ASSERT_NOT_NULL(resolved_end);
	UT_ASSERT_NOT_NULL(authoritative_gate);
	UT_ASSERT_NOT_NULL(exact_gate);
	UT_ASSERT_NOT_NULL(pre_abort);
	UT_ASSERT_NOT_NULL(pre_abort_guard);
	UT_ASSERT_NOT_NULL(procarray);
	UT_ASSERT_NOT_NULL(resample_gate);
	UT_ASSERT_NOT_NULL(gate_allow);
	UT_ASSERT_NOT_NULL(gate_exact);
	UT_ASSERT_NOT_NULL(gate_commit);
	UT_ASSERT_NOT_NULL(gate_abort);
	UT_ASSERT_NOT_NULL(gate_live);
	UT_ASSERT_NOT_NULL(post_commit);
	UT_ASSERT_NOT_NULL(post_commit_guard);
	UT_ASSERT_NOT_NULL(post_abort);
	UT_ASSERT_NOT_NULL(resampled);
	UT_ASSERT_NOT_NULL(arg_commit_before);
	UT_ASSERT_NOT_NULL(arg_abort_before);
	UT_ASSERT_NOT_NULL(arg_live);
	UT_ASSERT_NOT_NULL(arg_commit_after);
	UT_ASSERT_NOT_NULL(arg_abort_after);
	if (resolved != NULL && resolved_end != NULL && authoritative_gate != NULL
		&& exact_gate != NULL && pre_abort != NULL && pre_abort_guard != NULL
		&& procarray != NULL && resample_gate != NULL && gate_allow != NULL
		&& gate_exact != NULL && gate_commit != NULL && gate_abort != NULL
		&& gate_live != NULL && post_commit != NULL && post_commit_guard != NULL
		&& post_abort != NULL && resampled != NULL && arg_commit_before != NULL
		&& arg_abort_before != NULL && arg_live != NULL && arg_commit_after != NULL
		&& arg_abort_after != NULL) {
		UT_ASSERT(resolved < authoritative_gate
				  && authoritative_gate < exact_gate
				  && exact_gate < pre_abort && pre_abort < pre_abort_guard
				  && pre_abort_guard < procarray && procarray < resample_gate
				  && resample_gate < gate_allow && gate_allow < gate_exact
				  && gate_exact < gate_commit && gate_commit < gate_abort
				  && gate_abort < gate_live && gate_live < post_commit
				  && post_commit < post_commit_guard
				  && post_commit_guard < post_abort && post_abort < resampled
				  && resampled < arg_commit_before
				  && arg_commit_before < arg_abort_before
				  && arg_abort_before < arg_live && arg_live < arg_commit_after
				  && arg_commit_after < arg_abort_after
				  && arg_abort_after < resolved_end);
	}
	free(source);
}

UT_TEST(test_terminal_resample_is_counter_only_and_branch_closed)
{
	char *source = read_cr_server_source();
	const char *resolved
		= source != NULL ? strstr(source, "case CLUSTER_TT_DURABLE_RESOLVED_SCN:") : NULL;
	const char *resolved_end
		= resolved != NULL ? strstr(resolved, "if (cluster_cr_accept_resolved_scn(scn))") : NULL;
	const char *resample_gate
		= find_before(resolved, resolved_end,
					  "if (cluster_cr_server_terminal_resample_allowed(");
	const char *post_commit
		= find_before(resample_gate, resolved_end,
					  "did_commit_after = TransactionIdDidCommit(xid);");
	const char *post_commit_guard
		= find_before(post_commit, resolved_end, "if (!did_commit_after) {");
	const char *post_abort
		= find_before(post_commit_guard, resolved_end,
					  "did_abort_after = TransactionIdDidAbort(xid);");
	const char *abort_counter
		= find_before(post_abort, resolved_end,
					  "CLUSTER_CR_SERVER_STAT_TERMINAL_RESAMPLE_ABORT");
	const char *unknown_counter
		= find_before(abort_counter, resolved_end,
					  "CLUSTER_CR_SERVER_STAT_TERMINAL_RESAMPLE_UNKNOWN");
	const char *commit_counter
		= find_before(unknown_counter, resolved_end,
					  "CLUSTER_CR_SERVER_STAT_TERMINAL_RESAMPLE_COMMIT");
	const char *unknown
		= resolved != NULL
			  ? strstr(resolved, "case CLUSTER_UNDO_VERDICT_UNKNOWN_FAIL_CLOSED:")
			  : NULL;
	const char *refuse
		= unknown != NULL ? strstr(unknown, "return LMS_OWN_XID_REFUSE_OTHER;") : NULL;
	const char *direct_log
		= find_before(unknown, refuse, "ereport(LOG,");

	UT_ASSERT_NOT_NULL(resolved);
	UT_ASSERT_NOT_NULL(resolved_end);
	UT_ASSERT_NOT_NULL(resample_gate);
	UT_ASSERT_NOT_NULL(post_commit);
	UT_ASSERT_NOT_NULL(post_commit_guard);
	UT_ASSERT_NOT_NULL(post_abort);
	UT_ASSERT_NOT_NULL(abort_counter);
	UT_ASSERT_NOT_NULL(unknown_counter);
	UT_ASSERT_NOT_NULL(commit_counter);
	UT_ASSERT_NOT_NULL(unknown);
	UT_ASSERT_NOT_NULL(refuse);
	UT_ASSERT(direct_log == NULL);
	if (resolved != NULL && resolved_end != NULL && resample_gate != NULL
		&& post_commit != NULL && post_commit_guard != NULL && post_abort != NULL
		&& abort_counter != NULL && unknown_counter != NULL && commit_counter != NULL)
		UT_ASSERT(resolved < resample_gate && resample_gate < post_commit
				  && post_commit < post_commit_guard
				  && post_commit_guard < post_abort && post_abort < abort_counter
				  && abort_counter < unknown_counter && unknown_counter < commit_counter
				  && commit_counter < resolved_end);
	free(source);
}

/*
 * S3-P0-13 RED-2: a durable TT RESOLVED_SCN stamp precedes the CLOG terminal
 * record.  If the transaction later explicitly aborts, it has left ProcArray
 * but is not ambiguous: origin CLOG is positive ABORTED authority.  The r23
 * two-input policy sees only !commit && !live and returns UNKNOWN/OTHER,
 * which leaks to the requester as recycled-slot 53R97.
 *
 * The product must sample DidAbort in the RESOLVED_SCN arm and choose ABORTED
 * before the unproved OTHER leg.  The neighbouring live leg stays kind4, and
 * !commit + !abort + !live stays fail-closed for crash-lost/in-doubt cases.
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
		= did_abort != NULL
			  ? strstr(did_abort, "CLUSTER_UNDO_VERDICT_ABORTED")
			  : NULL;
	const char *unknown
		= abort_verdict != NULL
			  ? strstr(abort_verdict, "CLUSTER_UNDO_VERDICT_UNKNOWN_FAIL_CLOSED")
			  : NULL;

	/* r23's exact missing classification: explicit abort collapses into the
	 * same false/false bucket as crash-lost and returns OTHER/UNKNOWN. */
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

UT_TEST(test_undo_serve_error_preserves_exact_refusal_context)
{
	char *source = read_cr_server_source();
	const char *serve;
	const char *undo;
	const char *catch;
	const char *fail_closed;
	const char *switch_context;
	const char *copy;
	const char *flush;
	const char *log;
	const char *free_error;
	const char *deny;
	const char *construction;

	if (source == NULL)
		return;
	serve = strstr(source, "\ncr_serve_slot(ClusterLmsCrSlot *slot)");
	undo = serve != NULL ? strstr(serve, "CLUSTER_LMS_SLOT_KIND_UNDO_FETCH") : NULL;
	catch = undo != NULL ? strstr(undo, "PG_CATCH();") : NULL;
	construction = serve != NULL ? strstr(serve, "/* spec-6.12b CR construction. */") : NULL;
	fail_closed = find_before(catch, construction, "served = false;");
	switch_context = find_before(fail_closed, construction,
							 "MemoryContextSwitchTo(TopMemoryContext);");
	copy = find_before(catch, construction, "edata = CopyErrorData();");
	flush = find_before(copy, construction, "FlushErrorState();");
	log = find_before(flush, construction,
					  "cluster undo serve caught error; keeping fail-closed DENIED");
	free_error = find_before(log, construction, "FreeErrorData(edata);");
	deny = find_before(free_error, construction, "cluster_cr_server_stat_bump(denied_stat);");

	UT_ASSERT_NOT_NULL(serve);
	UT_ASSERT_NOT_NULL(undo);
	UT_ASSERT_NOT_NULL(catch);
	UT_ASSERT_NOT_NULL(fail_closed);
	UT_ASSERT_NOT_NULL(switch_context);
	UT_ASSERT_NOT_NULL(copy);
	UT_ASSERT_NOT_NULL(flush);
	UT_ASSERT_NOT_NULL(log);
	UT_ASSERT_NOT_NULL(free_error);
	UT_ASSERT_NOT_NULL(deny);
	UT_ASSERT_NOT_NULL(construction);
	UT_ASSERT_NOT_NULL(find_before(catch, construction, "slot->req_kind"));
	UT_ASSERT_NOT_NULL(find_before(catch, construction, "slot->undo_xid"));
	UT_ASSERT_NOT_NULL(find_before(catch, construction, "slot->undo_segment_id"));
	UT_ASSERT_NOT_NULL(find_before(catch, construction, "slot->undo_owner"));
	UT_ASSERT_NOT_NULL(find_before(catch, construction, "slot->epoch"));
	UT_ASSERT_NOT_NULL(find_before(catch, construction, "slot->requester_node"));
	UT_ASSERT_NOT_NULL(find_before(catch, construction, "slot->requester_backend"));
	UT_ASSERT_NOT_NULL(find_before(catch, construction, "slot->request_id"));
	UT_ASSERT_NOT_NULL(find_before(catch, construction, "unpack_sql_state(edata->sqlerrcode)"));
	UT_ASSERT_NOT_NULL(find_before(catch, construction, "edata->message"));
	if (serve != NULL && undo != NULL && catch != NULL && fail_closed != NULL
		&& switch_context != NULL && copy != NULL && flush != NULL && log != NULL
		&& free_error != NULL && deny != NULL && construction != NULL) {
		UT_ASSERT(serve < undo && undo < catch && catch < fail_closed
				  && fail_closed < switch_context && switch_context < copy && copy < flush
				  && flush < log && log < free_error && free_error < deny && deny < construction);
		UT_ASSERT(strstr(catch, "PG_RE_THROW();") == NULL
				  || strstr(catch, "PG_RE_THROW();") >= construction);
	}
	free(source);
}

UT_TEST(test_undo_submit_slots_busy_has_distinct_exact_signature)
{
	char *source = read_cr_server_source();
	const char *helper;
	const char *helper_end;
	const char *fetch;
	const char *fetch_end;
	const char *fetch_state;
	const char *fetch_busy;
	const char *fetch_return;
	const char *verdict;
	const char *verdict_end;
	const char *verdict_state;
	const char *verdict_busy;
	const char *verdict_return;
	const char *multi;
	const char *multi_end;
	const char *multi_state;
	const char *multi_busy;
	const char *multi_return;

	if (source == NULL)
		return;
	helper = strstr(source, "\ncr_log_undo_submit_slots_busy(");
	helper_end = helper != NULL ? strstr(helper, "\n}\n") : NULL;
	UT_ASSERT_NOT_NULL(helper);
	UT_ASSERT_NOT_NULL(helper_end);
	UT_ASSERT_NOT_NULL(find_before(helper, helper_end, "cluster undo submit refused: all slots busy"));
	UT_ASSERT_NOT_NULL(find_before(helper, helper_end, "req_kind"));
	UT_ASSERT_NOT_NULL(find_before(helper, helper_end, "xid"));
	UT_ASSERT_NOT_NULL(find_before(helper, helper_end, "segment"));
	UT_ASSERT_NOT_NULL(find_before(helper, helper_end, "fwd->epoch"));
	UT_ASSERT_NOT_NULL(find_before(helper, helper_end, "requester_node"));
	UT_ASSERT_NOT_NULL(find_before(helper, helper_end, "requester_backend"));
	UT_ASSERT_NOT_NULL(find_before(helper, helper_end, "request_id"));
	UT_ASSERT_NOT_NULL(find_before(helper, helper_end, "slot_states"));

	fetch = strstr(source, "\ncluster_lms_undo_fetch_submit(");
	verdict = strstr(source, "\ncluster_lms_undo_verdict_submit(");
	multi = strstr(source, "\ncluster_lms_undo_multi_verdict_submit(");
	fetch_end = verdict;
	verdict_end = multi;
	multi_end = multi != NULL ? strstr(multi, "\nlms_undo_fetch_serve(") : NULL;
	fetch_state = find_before(fetch, fetch_end, "slot_states[i] = expected;");
	fetch_busy = find_before(fetch_state, fetch_end, "cr_log_undo_submit_slots_busy(");
	fetch_return = find_before(fetch_busy, fetch_end, "return false; /* all slots busy");
	UT_ASSERT_NOT_NULL(fetch);
	UT_ASSERT_NOT_NULL(fetch_end);
	UT_ASSERT_NOT_NULL(fetch_state);
	UT_ASSERT_NOT_NULL(fetch_busy);
	UT_ASSERT_NOT_NULL(fetch_return);
	UT_ASSERT_NOT_NULL(find_before(fetch_busy, fetch_return, "CLUSTER_LMS_SLOT_KIND_UNDO_FETCH"));

	verdict_state = find_before(verdict, verdict_end, "slot_states[i] = expected;");
	verdict_busy = find_before(verdict_state, verdict_end, "cr_log_undo_submit_slots_busy(");
	verdict_return = find_before(verdict_busy, verdict_end, "return false; /* all slots busy");
	UT_ASSERT_NOT_NULL(verdict);
	UT_ASSERT_NOT_NULL(verdict_end);
	UT_ASSERT_NOT_NULL(verdict_state);
	UT_ASSERT_NOT_NULL(verdict_busy);
	UT_ASSERT_NOT_NULL(verdict_return);
	UT_ASSERT_NOT_NULL(
		find_before(verdict_busy, verdict_return, "CLUSTER_LMS_SLOT_KIND_UNDO_VERDICT"));

	multi_state = find_before(multi, multi_end, "slot_states[i] = expected;");
	multi_busy = find_before(multi_state, multi_end, "cr_log_undo_submit_slots_busy(");
	multi_return = find_before(multi_busy, multi_end, "return false; /* all slots busy");
	UT_ASSERT_NOT_NULL(multi);
	UT_ASSERT_NOT_NULL(multi_end);
	UT_ASSERT_NOT_NULL(multi_state);
	UT_ASSERT_NOT_NULL(multi_busy);
	UT_ASSERT_NOT_NULL(multi_return);
	UT_ASSERT_NOT_NULL(
		find_before(multi_busy, multi_return, "CLUSTER_LMS_SLOT_KIND_UNDO_MULTI_VERDICT"));

	if (helper != NULL && helper_end != NULL)
		UT_ASSERT(helper < helper_end);
	if (fetch != NULL && fetch_state != NULL && fetch_busy != NULL && fetch_return != NULL)
		UT_ASSERT(fetch < fetch_state && fetch_state < fetch_busy && fetch_busy < fetch_return);
	if (verdict != NULL && verdict_state != NULL && verdict_busy != NULL && verdict_return != NULL)
		UT_ASSERT(verdict < verdict_state && verdict_state < verdict_busy
				  && verdict_busy < verdict_return);
	if (multi != NULL && multi_state != NULL && multi_busy != NULL && multi_return != NULL)
		UT_ASSERT(multi < multi_state && multi_state < multi_busy && multi_busy < multi_return);
	free(source);
}

UT_TEST(test_current_mx_describe_rechecks_fence_before_direct_origin_reply)
{
	char *source = read_cr_server_source();
	const char *serve;
	const char *enumerate;
	const char *end_try;
	const char *fence_recheck;
	const char *deny;
	const char *direct_origin;
	const char *send;

	if (source == NULL)
		return;
	serve = strstr(source, "\ncluster_gcs_current_mx_describe_serve_inline(");
	enumerate = serve != NULL ? strstr(serve, "GetMultiXactIdMembers(") : NULL;
	end_try = enumerate != NULL ? strstr(enumerate, "PG_END_TRY();") : NULL;
	fence_recheck = end_try != NULL
						? strstr(end_try,
								 "(cluster_write_fence_enforcing() && "
								 "!cluster_write_fence_allowed())")
						: NULL;
	deny = fence_recheck != NULL ? strstr(fence_recheck, "if (!served)") : NULL;
	direct_origin = deny != NULL
						? strstr(deny,
								 "GcsBlockReplyHeaderSetForwardingMasterNode("
								 "outer, GCS_BLOCK_REPLY_NO_FORWARDING_MASTER)")
						: NULL;
	send = direct_origin != NULL ? strstr(direct_origin, "cluster_ic_send_envelope(") : NULL;

	UT_ASSERT_NOT_NULL(serve);
	UT_ASSERT_NOT_NULL(enumerate);
	UT_ASSERT_NOT_NULL(end_try);
	UT_ASSERT_NOT_NULL(fence_recheck);
	UT_ASSERT_NOT_NULL(deny);
	UT_ASSERT_NOT_NULL(direct_origin);
	UT_ASSERT_NOT_NULL(send);
	if (serve != NULL && enumerate != NULL && end_try != NULL && fence_recheck != NULL
		&& deny != NULL && direct_origin != NULL && send != NULL)
		UT_ASSERT(serve < enumerate && enumerate < end_try && end_try < fence_recheck
				  && fence_recheck < deny && deny < direct_origin && direct_origin < send);
	free(source);
}

UT_TEST(test_current_mx_member_proof_rechecks_fence_before_direct_origin_reply)
{
	char *source = read_cr_server_source();
	const char *serve;
	const char *validate;
	const char *own_xid_lookup;
	const char *candidate_verdict;
	const char *candidate_exact_fallback;
	const char *resolve;
	const char *fence_recheck;
	const char *body_clear;
	const char *direct_origin;
	const char *send;

	if (source == NULL)
		return;
	serve = strstr(source, "\ncluster_gcs_current_mx_member_proof_serve_inline(");
	validate = serve != NULL
				   ? strstr(serve, "cluster_multixact_current_wire_validate_proof_forward(")
				   : NULL;
	own_xid_lookup
		= validate != NULL ? strstr(validate, "cluster_tt_status_lookup_current_own_xid(") : NULL;
	candidate_verdict
		= validate != NULL
			  ? strstr(validate, "cluster_multixact_current_updater_candidate_verdict(")
			  : NULL;
	candidate_exact_fallback
		= candidate_verdict != NULL
			  ? strstr(candidate_verdict, "cluster_tt_status_lookup_exact(")
			  : NULL;
	resolve = own_xid_lookup != NULL
				  ? strstr(own_xid_lookup,
						   "cluster_multixact_current_resolve_origin_member_proof(")
				  : NULL;
	fence_recheck = resolve != NULL
						? strstr(resolve, "cluster_epoch_get_current() != request.prefix.epoch")
						: NULL;
	body_clear = fence_recheck != NULL ? strstr(fence_recheck, "memset(&page->body") : NULL;
	direct_origin = body_clear != NULL
						? strstr(body_clear,
								 "GcsBlockReplyHeaderSetForwardingMasterNode("
								 "outer, GCS_BLOCK_REPLY_NO_FORWARDING_MASTER)")
						: NULL;
	send = direct_origin != NULL ? strstr(direct_origin, "cluster_ic_send_envelope(") : NULL;

	UT_ASSERT_NOT_NULL(serve);
	UT_ASSERT_NOT_NULL(validate);
	UT_ASSERT_NOT_NULL(own_xid_lookup);
	UT_ASSERT_NOT_NULL(candidate_verdict);
	UT_ASSERT_NOT_NULL(resolve);
	UT_ASSERT_NOT_NULL(fence_recheck);
	UT_ASSERT_NOT_NULL(body_clear);
	UT_ASSERT_NOT_NULL(direct_origin);
	UT_ASSERT_NOT_NULL(send);
	if (serve != NULL && validate != NULL && own_xid_lookup != NULL && resolve != NULL
		&& fence_recheck != NULL && body_clear != NULL && direct_origin != NULL && send != NULL)
		UT_ASSERT(serve < validate && validate < own_xid_lookup && own_xid_lookup < resolve
				  && resolve < fence_recheck && fence_recheck < body_clear
				  && body_clear < direct_origin && direct_origin < send);
	if (candidate_verdict != NULL && fence_recheck != NULL)
		UT_ASSERT(candidate_exact_fallback == NULL || candidate_exact_fallback > fence_recheck);
	free(source);
}

/*
 * S3-P0-03 (r57): a data writer publishes an exact IN_PROGRESS overlay for
 * every page-ref segment alias before the page can be transferred.  The
 * origin verdict server nevertheless starts with the canonical durable-TT
 * scan.  While the xact is still live, a page-ref alias can therefore fail
 * the canonical segment equality check and become UNKNOWN until the later
 * terminal CLOG state is visible.
 *
 * Pin the production ordering needed to close that timing window: an
 * authoritative request must probe the exact page-ref overlay key first and
 * accept only an authoritative IN_PROGRESS result.  The canonical durable
 * scan remains the terminal/ABA authority fallback.  This is deliberately a
 * source-linked RED: no sleep, retry-budget increase, or UNKNOWN swallowing
 * can satisfy it.
 */
UT_TEST(test_p003_live_page_ref_alias_probes_exact_overlay_before_durable_scan)
{
	char *source = read_cr_server_source();
	const char *resolve;
	const char *resolve_end;
	const char *authority_epoch_arg;
	const char *serve;
	const char *serve_end;
	const char *authority_sample;
	const char *resolve_call;
	const char *authority_pass;
	const char *durable_scan;
	const char *exact_lookup;
	const char *key_origin;
	const char *key_segment;
	const char *key_slot;
	const char *key_epoch;
	const char *key_xid;
	const char *admit;
	const char *wire_in_progress;
	const char *early_return;
	const char *live_return;
	const char *next_return;

	if (source == NULL)
		return;

	resolve = strstr(source, "\nlms_resolve_own_xid_verdict(");
	resolve_end = resolve != NULL ? strstr(resolve, "\n}\n\n/*\n * lms_undo_verdict_serve") : NULL;
	authority_epoch_arg
		= find_before(resolve, resolve_end, "uint64 authority_epoch");
	serve = resolve_end != NULL ? strstr(resolve_end, "\nlms_undo_verdict_serve(") : NULL;
	serve_end = serve != NULL
					? strstr(serve, "\n}\n\n/*\n * lms_undo_authority_verdict_serve")
					: NULL;
	resolve_call
		= find_before(serve, serve_end, "lms_resolve_own_xid_verdict(");
	authority_sample
		= find_exact_line_before(
			serve, resolve_call,
			"slot->undo_auth.origin_epoch = cluster_epoch_get_current();");
	authority_pass
		= find_exact_line_before(resolve_call, serve_end,
								 "slot->undo_auth.origin_epoch,");
	durable_scan
		= find_before(resolve, resolve_end, "cluster_tt_slot_durable_resolve_by_xid(");
	exact_lookup
		= find_before(resolve, durable_scan, "cluster_tt_status_lookup_exact(&live_key, &live_result)");
	key_origin = find_exact_line_before(
		resolve, exact_lookup,
		"live_key.origin_node_id = (uint16)cluster_node_id;");
	key_segment
		= find_exact_line_before(resolve, exact_lookup,
								 "live_key.undo_segment_id = expected_segment_id;");
	key_slot = find_exact_line_before(resolve, exact_lookup,
									  "live_key.tt_slot_id = expected_tt_slot_id;");
	key_epoch = find_exact_line_before(
		resolve, exact_lookup,
		"live_key.cluster_epoch = (uint32)authority_epoch;");
	key_xid = find_exact_line_before(resolve, exact_lookup,
									 "live_key.local_xid = xid;");
	admit = find_before(exact_lookup, durable_scan,
						"cluster_cr_server_live_overlay_admissible(");
	wire_in_progress
		= find_exact_line_before(
			admit, durable_scan,
			"*out_verdict = (uint8)CLUSTER_GCS_UNDO_VERDICT_IN_PROGRESS;");
	early_return
		= find_before(exact_lookup, wire_in_progress, "return ");
	live_return
		= find_exact_line_before(wire_in_progress, durable_scan,
								 "return LMS_OWN_XID_PROVEN_IN_PROGRESS;");
	next_return = live_return != NULL
					  ? find_before(live_return + strlen("return "),
									durable_scan, "return ")
					  : NULL;

	UT_ASSERT_NOT_NULL(resolve);
	UT_ASSERT_NOT_NULL(resolve_end);
	UT_ASSERT_NOT_NULL(authority_epoch_arg);
	UT_ASSERT_NOT_NULL(serve);
	UT_ASSERT_NOT_NULL(serve_end);
	UT_ASSERT_NOT_NULL(authority_sample);
	UT_ASSERT_NOT_NULL(resolve_call);
	UT_ASSERT_NOT_NULL(authority_pass);
	UT_ASSERT_NOT_NULL(durable_scan);
	UT_ASSERT_NOT_NULL(exact_lookup);
	UT_ASSERT_NOT_NULL(key_origin);
	UT_ASSERT_NOT_NULL(key_segment);
	UT_ASSERT_NOT_NULL(key_slot);
	UT_ASSERT_NOT_NULL(key_epoch);
	UT_ASSERT_NOT_NULL(key_xid);
	UT_ASSERT_NOT_NULL(admit);
	UT_ASSERT_NOT_NULL(wire_in_progress);
	UT_ASSERT_NULL(early_return);
	UT_ASSERT_NOT_NULL(live_return);
	UT_ASSERT_NULL(next_return);
	if (resolve != NULL && resolve_end != NULL && authority_epoch_arg != NULL
		&& durable_scan != NULL
		&& exact_lookup != NULL && key_origin != NULL && key_segment != NULL
		&& key_slot != NULL && key_epoch != NULL && key_xid != NULL
		&& admit != NULL
		&& wire_in_progress != NULL && early_return == NULL
		&& live_return != NULL
		&& next_return == NULL)
		UT_ASSERT(resolve < authority_epoch_arg && authority_epoch_arg < key_origin
				  && key_origin < key_segment
				  && key_segment < key_slot && key_slot < key_epoch
				  && key_epoch < key_xid && key_xid < exact_lookup
				  && exact_lookup < admit && admit < wire_in_progress
				  && wire_in_progress < live_return && live_return < durable_scan
				  && durable_scan < resolve_end);
	if (serve != NULL && serve_end != NULL && authority_sample != NULL
		&& resolve_call != NULL && authority_pass != NULL)
		UT_ASSERT(serve < authority_sample && authority_sample < resolve_call
				  && resolve_call < authority_pass && authority_pass < serve_end);

	free(source);
}

static ClusterTTStatusKey
p003_live_key(uint16 origin, uint32 segment, uint32 slot,
			  uint32 epoch, TransactionId xid)
{
	ClusterTTStatusKey key;

	memset(&key, 0, sizeof(key));
	key.origin_node_id = origin;
	key.undo_segment_id = segment;
	key.tt_slot_id = slot;
	key.cluster_epoch = epoch;
	key.local_xid = xid;
	return key;
}

static ClusterTTStatusResult
p003_live_result(bool authoritative, ClusterTTStatus status, uint32 epoch)
{
	ClusterTTStatusResult result;

	memset(&result, 0, sizeof(result));
	result.status = status;
	result.commit_scn = InvalidScn;
	result.status_epoch = epoch;
	result.authoritative = authoritative;
	return result;
}

/*
 * The exact-overlay widening is live-only and identity-total.  Every key
 * namespace dimension, lookup freshness bit, and result authority bit must
 * agree.  Terminal states continue through durable TT + CLOG; UNKNOWN and
 * malformed live results stay fail-closed.  The same-slot/new-xid case is the
 * explicit ABA negative.
 */
UT_TEST(test_p003_live_overlay_admission_exact_identity_and_safety_matrix)
{
	const uint16 self = 2;
	const uint32 segment = 601;
	const uint32 slot = 17;
	const uint32 epoch = 9;
	const TransactionId xid = 4224802;
	ClusterTTStatusKey key = p003_live_key(self, segment, slot, epoch, xid);
	ClusterTTStatusResult result
		= p003_live_result(true, CLUSTER_TT_STATUS_IN_PROGRESS, epoch);

	UT_ASSERT_NOT_NULL(cluster_cr_server_live_overlay_admissible);
	if (cluster_cr_server_live_overlay_admissible == NULL)
		return;

#define P003_ADMIT(k_, found_, r_) \
	cluster_cr_server_live_overlay_admissible(true, &(k_), self, segment, slot, epoch, xid, \
											  (found_), &(r_))

	/* Sole positive: current exact authoritative IN_PROGRESS. */
	UT_ASSERT(P003_ADMIT(key, true, result));

	/* Request/key namespace must match in every dimension. */
	key.origin_node_id = self + 1;
	UT_ASSERT(!P003_ADMIT(key, true, result));
	key = p003_live_key(self, segment + 1, slot, epoch, xid);
	UT_ASSERT(!P003_ADMIT(key, true, result));
	key = p003_live_key(self, segment, slot + 1, epoch, xid);
	UT_ASSERT(!P003_ADMIT(key, true, result));
	key = p003_live_key(self, segment, slot, epoch - 1, xid);
	UT_ASSERT(!P003_ADMIT(key, true, result));
	key = p003_live_key(self, segment, slot, epoch + 1, xid);
	UT_ASSERT(!P003_ADMIT(key, true, result));
	key = p003_live_key(self, segment, slot, epoch, xid + 4);
	UT_ASSERT(!P003_ADMIT(key, true, result)); /* same slot, newer xid: ABA */
	key = p003_live_key(self, segment, slot, epoch, xid);

	/* The wire/reconfig epoch is 64-bit; the exact overlay key is 32-bit. */
	UT_ASSERT(!cluster_cr_server_live_overlay_admissible(
		true, &key, self, segment, slot, (uint64)UINT32_MAX + 1,
		xid, true, &result));

	/* Request carriers must be representable physical identities. */
	UT_ASSERT(!cluster_cr_server_live_overlay_admissible(
		true, &key, self, 0, slot, epoch, xid, true, &result));
	UT_ASSERT(!cluster_cr_server_live_overlay_admissible(
		true, &key, self, (uint32)UINT16_MAX + 1, slot, epoch, xid,
		true, &result));
	UT_ASSERT(!cluster_cr_server_live_overlay_admissible(
		true, &key, self, segment, 0, epoch, xid, true, &result));
	UT_ASSERT(!cluster_cr_server_live_overlay_admissible(
		true, &key, self, segment, TT_SLOTS_PER_SEGMENT + 1,
		epoch, xid, true, &result));
	key = p003_live_key(self, segment, slot, epoch, InvalidTransactionId);
	UT_ASSERT(!cluster_cr_server_live_overlay_admissible(
		true, &key, self, segment, slot, epoch, InvalidTransactionId,
		true, &result));
	key = p003_live_key(self, segment, slot, epoch, xid);

	/* Lookup/result authority and freshness are mandatory. */
	UT_ASSERT(!P003_ADMIT(key, false, result));
	result.authoritative = false;
	UT_ASSERT(!P003_ADMIT(key, true, result));
	result = p003_live_result(true, CLUSTER_TT_STATUS_IN_PROGRESS, epoch - 1);
	UT_ASSERT(!P003_ADMIT(key, true, result));
	result = p003_live_result(true, CLUSTER_TT_STATUS_IN_PROGRESS, epoch + 1);
	UT_ASSERT(!P003_ADMIT(key, true, result));

	/* No terminal/unknown widening: those retain the durable+CLOG authority. */
	result = p003_live_result(true, CLUSTER_TT_STATUS_UNKNOWN, epoch);
	UT_ASSERT(!P003_ADMIT(key, true, result));
	result = p003_live_result(true, CLUSTER_TT_STATUS_COMMITTED, epoch);
	result.commit_scn = 9001;
	UT_ASSERT(!P003_ADMIT(key, true, result));
	result = p003_live_result(true, CLUSTER_TT_STATUS_ABORTED, epoch);
	UT_ASSERT(!P003_ADMIT(key, true, result));
	result = p003_live_result(true, CLUSTER_TT_STATUS_CLEANED_OUT, epoch);
	UT_ASSERT(!P003_ADMIT(key, true, result));
	result = p003_live_result(true, CLUSTER_TT_STATUS_SUBCOMMITTED, epoch);
	UT_ASSERT(!P003_ADMIT(key, true, result));

	/* IN_PROGRESS never carries terminal SCN or parent identity. */
	result = p003_live_result(true, CLUSTER_TT_STATUS_IN_PROGRESS, epoch);
	result.commit_scn = 9001;
	UT_ASSERT(!P003_ADMIT(key, true, result));
	result = p003_live_result(true, CLUSTER_TT_STATUS_IN_PROGRESS, epoch);
	result.has_parent_key = true;
	UT_ASSERT(!P003_ADMIT(key, true, result));

	/* Wrong mode and malformed pointers are fail-closed. */
	result = p003_live_result(true, CLUSTER_TT_STATUS_IN_PROGRESS, epoch);
	UT_ASSERT(!cluster_cr_server_live_overlay_admissible(
		false, &key, self, segment, slot, epoch, xid, true, &result));
	UT_ASSERT(!cluster_cr_server_live_overlay_admissible(
		true, NULL, self, segment, slot, epoch, xid, true, &result));
	UT_ASSERT(!cluster_cr_server_live_overlay_admissible(
		true, &key, self, segment, slot, epoch, xid, true, NULL));

#undef P003_ADMIT
}

int
main(void)
{
	UT_PLAN(29);
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
	UT_RUN(test_exact_diagnostic_is_one_mutually_exclusive_reason);
	UT_RUN(test_confirm_diagnostic_is_one_mutually_exclusive_reason);
	UT_RUN(test_other_refusal_detail_roster_is_closed_and_invalid_fails_safe);
	UT_RUN(test_srv_other_producers_share_one_detail_then_aggregate_wrapper);
	UT_RUN(test_resolved_scn_runtime_wires_mutually_exclusive_diagnostics);
	UT_RUN(test_resolved_scn_terminal_resample_closes_abort_publication_gap);
	UT_RUN(test_resolved_scn_terminal_resample_preserves_live_and_unknown_boundaries);
	UT_RUN(test_terminal_resample_requires_authoritative_exact_non_live_gap);
	UT_RUN(test_resolved_scn_runtime_resamples_terminal_after_non_live);
	UT_RUN(test_terminal_resample_is_counter_only_and_branch_closed);
	UT_RUN(test_resolved_scn_explicit_abort_after_stamp_is_positive);
	UT_RUN(test_undo_serve_error_preserves_exact_refusal_context);
	UT_RUN(test_undo_submit_slots_busy_has_distinct_exact_signature);
	UT_RUN(test_current_mx_describe_rechecks_fence_before_direct_origin_reply);
	UT_RUN(test_current_mx_member_proof_rechecks_fence_before_direct_origin_reply);
	UT_RUN(test_p003_live_page_ref_alias_probes_exact_overlay_before_durable_scan);
	UT_RUN(test_p003_live_overlay_admission_exact_identity_and_safety_matrix);
	UT_DONE();
	return ut_failed_count == 0 ? 0 : 1;
}
