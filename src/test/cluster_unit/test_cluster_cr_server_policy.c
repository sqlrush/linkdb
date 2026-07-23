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

#include "cluster/cluster_cr_server.h"

#include "unit_test.h"

UT_DEFINE_GLOBALS();

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

int
main(void)
{
	UT_PLAN(11);
	UT_RUN(test_split_empty_is_full_prefix_zero);
	UT_RUN(test_split_all_self_is_full);
	UT_RUN(test_split_self_prefix_foreign_suffix_is_partial);
	UT_RUN(test_split_all_foreign_is_partial_prefix_zero);
	UT_RUN(test_split_interleave_is_deny);
	UT_RUN(test_split_third_party_suffix_stays_partial);
	UT_RUN(test_split_malformed_is_deny);
	UT_RUN(test_invalid_scn_aborted_is_positive);
	UT_RUN(test_invalid_scn_not_aborted_refuses);
	UT_RUN(test_undo_serve_error_preserves_exact_refusal_context);
	UT_RUN(test_undo_submit_slots_busy_has_distinct_exact_signature);
	UT_DONE();
	return ut_failed_count == 0 ? 0 : 1;
}
