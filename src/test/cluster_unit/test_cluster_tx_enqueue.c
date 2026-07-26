/*-------------------------------------------------------------------------
 *
 * test_cluster_tx_enqueue.c
 *	  Closed-set and production-caller gates for cluster TX waits.
 *
 * Portions Copyright (c) 1996-2024, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2026, pgrac contributors
 *
 * Author: SqlRush <sqlrush@gmail.com>
 *
 * IDENTIFICATION
 *	  src/test/cluster_unit/test_cluster_tx_enqueue.c
 *
 * NOTES
 *	  This is a pgrac-original file.
 *	  Spec: spec-3.6b-multixact-current-dml.md
 *
 *-------------------------------------------------------------------------
 */
#define USE_PGRAC_CLUSTER 1

#include "postgres.h"

#include "cluster/cluster_tx_enqueue.h"

#undef printf
#undef fprintf
#undef snprintf

#include "unit_test.h"


UT_DEFINE_GLOBALS();

#define HEAPAM_SOURCE_PATH "../../../src/backend/access/heap/heapam.c"
#define TX_ENQUEUE_SOURCE_PATH "../../../src/backend/cluster/cluster_tx_enqueue.c"
#define TT_HINT_SOURCE_PATH "../../../src/backend/cluster/cluster_tt_status_hint.c"
#define TT_LOCAL_SOURCE_PATH "../../../src/backend/cluster/cluster_tt_local.c"
#define HOT_TAP_SOURCE_PATH "../cluster_tap/t/407_cluster_current_mx_hot.pl"


void
ExceptionalCondition(const char *conditionName pg_attribute_unused(),
					 const char *fileName pg_attribute_unused(),
					 int lineNumber pg_attribute_unused())
{
	abort();
}


StaticAssertDecl(CLUSTER_TXW_RESOLVED == 0, "TX wait RESOLVED value changed");
StaticAssertDecl(CLUSTER_TXW_TIMEOUT == 1, "TX wait TIMEOUT value changed");
StaticAssertDecl(CLUSTER_TXW_DEAD_HOLDER == 2, "TX wait DEAD_HOLDER value changed");
StaticAssertDecl(CLUSTER_TXW_RETRY == 3, "TX wait RETRY value changed");
StaticAssertDecl(CLUSTER_TXW_DEADLOCK == 4, "TX wait DEADLOCK must be append-only value 4");


static char *
read_source(const char *path)
{
	FILE *file;
	long length;
	char *source;

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


static int
count_occurrences(const char *source, const char *needle)
{
	int count = 0;
	size_t needle_len = strlen(needle);
	const char *cursor = source;

	while ((cursor = strstr(cursor, needle)) != NULL) {
		count++;
		cursor += needle_len;
	}
	return count;
}


static const char *
matching_close_brace(const char *open_brace)
{
	const char *cursor;
	int depth = 0;

	UT_ASSERT_NOT_NULL(open_brace);
	if (open_brace == NULL)
		return NULL;

	for (cursor = open_brace; *cursor != '\0'; cursor++) {
		if (*cursor == '{')
			depth++;
		else if (*cursor == '}' && --depth == 0)
			return cursor;
	}

	UT_ASSERT(false);
	return NULL;
}


static const char *
find_before(const char *start, const char *needle, const char *end)
{
	const char *found;

	if (start == NULL || end == NULL)
		return NULL;
	found = strstr(start, needle);
	return found != NULL && found < end ? found : NULL;
}


static void
assert_exhaustive_result_switch(const char *call)
{
	const char *switch_pos;
	const char *open_brace;
	const char *close_brace;
	const char *default_pos;
	const char *error_pos;
	static const char *const cases[] = {
		"case CLUSTER_TXW_RESOLVED:", "case CLUSTER_TXW_TIMEOUT:",	"case CLUSTER_TXW_DEAD_HOLDER:",
		"case CLUSTER_TXW_RETRY:",	  "case CLUSTER_TXW_DEADLOCK:",
	};
	int i;

	UT_ASSERT_NOT_NULL(call);
	if (call == NULL)
		return;

	switch_pos = strstr(call, "switch (txw)");
	UT_ASSERT_NOT_NULL(switch_pos);
	if (switch_pos == NULL)
		return;
	open_brace = strchr(switch_pos, '{');
	close_brace = matching_close_brace(open_brace);
	UT_ASSERT_NOT_NULL(open_brace);
	UT_ASSERT_NOT_NULL(close_brace);
	if (open_brace == NULL || close_brace == NULL)
		return;

	for (i = 0; i < lengthof(cases); i++) {
		const char *case_pos = find_before(switch_pos, cases[i], close_brace);

		UT_ASSERT_NOT_NULL(case_pos);
	}
	default_pos = find_before(switch_pos, "default:", close_brace);
	error_pos = find_before(default_pos, "ereport(ERROR", close_brace);
	UT_ASSERT_NOT_NULL(default_pos);
	UT_ASSERT_NOT_NULL(error_pos);
}


UT_TEST(test_all_production_callers_use_the_five_way_closed_set)
{
	char *source = read_source(HEAPAM_SOURCE_PATH);
	const char *first;
	const char *second;
	const char *third;

	if (source == NULL)
		return;

	UT_ASSERT_EQ(count_occurrences(source, "cluster_tx_enqueue_wait("), 2);
	UT_ASSERT_EQ(count_occurrences(source,
								   "cluster_tx_enqueue_wait_current_mx("),
				 1);
	first = strstr(source, "cluster_tx_enqueue_wait(");
	second = first != NULL ? strstr(first + 1, "cluster_tx_enqueue_wait(") : NULL;
	third = strstr(source, "cluster_tx_enqueue_wait_current_mx(");
	assert_exhaustive_result_switch(first);
	assert_exhaustive_result_switch(second);
	assert_exhaustive_result_switch(third);
	free(source);
}


UT_TEST(test_wait_loop_consumes_a_matching_deadlock_token)
{
	char *source = read_source(TX_ENQUEUE_SOURCE_PATH);

	if (source == NULL)
		return;

	UT_ASSERT_STR_CONTAINS(source, "cluster_cancel_token_consume");
	UT_ASSERT_STR_CONTAINS(source, "CLUSTER_TXW_DEADLOCK");
	UT_ASSERT_STR_CONTAINS(source, "cluster_lmd_wait_state_clear");
	UT_ASSERT_STR_CONTAINS(source, "cluster_lmd_cancel_wait_edge_real");
	free(source);
}

UT_TEST(test_current_mx_wakeup_counts_the_matching_setlatch_event)
{
	char *source = read_source(TX_ENQUEUE_SOURCE_PATH);
	const char *slot_flag;
	const char *current_api;
	const char *wake;
	const char *current_branch;
	const char *counter;
	const char *set_latch;

	if (source == NULL)
		return;

	slot_flag = strstr(source, "bool current_mx_wait;");
	current_api = strstr(source, "\ncluster_tx_enqueue_wait_current_mx(");
	wake = strstr(source, "\ncluster_txw_wake_waiters(");
	current_branch
		= wake != NULL ? strstr(wake, "if (ClusterTxw->slots[i].current_mx_wait)")
					   : NULL;
	counter = current_branch != NULL
				  ? strstr(current_branch,
						   "CMX_STAT_WAKEUP")
				  : NULL;
	set_latch = counter != NULL ? strstr(counter, "SetLatch(") : NULL;

	UT_ASSERT_NOT_NULL(slot_flag);
	UT_ASSERT_NOT_NULL(current_api);
	UT_ASSERT_NOT_NULL(wake);
	UT_ASSERT_NOT_NULL(current_branch);
	UT_ASSERT_NOT_NULL(counter);
	UT_ASSERT_NOT_NULL(set_latch);
	if (current_branch != NULL && counter != NULL && set_latch != NULL)
		UT_ASSERT(current_branch < counter && counter < set_latch);
	free(source);
}


UT_TEST(test_current_mx_heap_wait_restart_is_shared_and_rechecks_hot)
{
	char *source = read_source(HEAPAM_SOURCE_PATH);
	const char *helper;
	const char *helper_end;
	const char *unlock;
	const char *wait_call;
	const char *switch_pos;
	const char *relock;
	const char *restart;
	const char *hot;
	const char *typed_wait;
	const char *typed_call;
	const char *restart_flag;
	const char *ordinary_wait;
	const char *ordinary_call;
	const char *latest_tid;
	const char *latest_recheck;
	const char *latest_visibility;
	const char *latest_authority;

	if (source == NULL)
		return;

	helper = strstr(source, "\ncluster_current_mx_wait_member_restart(");
	helper_end = helper != NULL
					 ? strstr(
						   helper,
						   "\nstatic bool\ncluster_current_mx_memo_lookup(")
					 : NULL;
	unlock = helper != NULL
				 ? strstr(helper, "LockBuffer(buffer, BUFFER_LOCK_UNLOCK);")
				 : NULL;
	wait_call = helper != NULL
					? strstr(helper,
							 "cluster_tx_enqueue_wait_current_mx(")
					: NULL;
	switch_pos = wait_call != NULL ? strstr(wait_call, "switch (txw)") : NULL;
	relock = switch_pos != NULL
				 ? strstr(switch_pos,
						  "LockBuffer(buffer, BUFFER_LOCK_EXCLUSIVE);")
				 : NULL;
	restart = relock != NULL
				  ? strstr(relock, "cluster_current_mx_operation_restart(operation);")
				  : NULL;

	UT_ASSERT_EQ(count_occurrences(
					 source, "cluster_current_mx_wait_member_restart("),
				 3);
	UT_ASSERT_EQ(count_occurrences(
					 source, "cluster_tx_enqueue_wait_current_mx("),
				 1);
	UT_ASSERT_NOT_NULL(helper);
	UT_ASSERT_NOT_NULL(helper_end);
	UT_ASSERT_NOT_NULL(unlock);
	UT_ASSERT_NOT_NULL(wait_call);
	UT_ASSERT_NOT_NULL(switch_pos);
	UT_ASSERT_NOT_NULL(relock);
	UT_ASSERT_NOT_NULL(restart);
	if (helper != NULL && helper_end != NULL && unlock != NULL
		&& wait_call != NULL && switch_pos != NULL && relock != NULL
		&& restart != NULL)
		UT_ASSERT(helper < unlock && unlock < wait_call
				  && wait_call < switch_pos && switch_pos < relock
				  && relock < restart && restart < helper_end);
	assert_exhaustive_result_switch(wait_call);

	hot = strstr(source, "\ncluster_current_mx_hot_updater_for_chain(");
	typed_call = hot != NULL
					 ? strstr(
						   hot,
						   "cluster_multixact_current_updater_proof_outcome(")
					 : NULL;
	typed_wait = typed_call != NULL
					 ? strstr(typed_call, "case CCMUPO_WAIT_MEMBER:")
					 : NULL;
	restart_flag = typed_wait != NULL
					   ? strstr(typed_wait, "*restart = true;")
					   : NULL;
	UT_ASSERT_NOT_NULL(hot);
	UT_ASSERT_NOT_NULL(typed_call);
	UT_ASSERT_NOT_NULL(typed_wait);
	UT_ASSERT_NOT_NULL(restart_flag);
	if (typed_wait != NULL && helper != NULL && restart_flag != NULL)
	{
		const char *hot_helper_call = strstr(
			typed_wait, "cluster_current_mx_wait_member_restart(");

		UT_ASSERT_NOT_NULL(hot_helper_call);
		UT_ASSERT(hot_helper_call == NULL
				  || (typed_wait < hot_helper_call
					  && hot_helper_call < restart_flag));
	}
	UT_ASSERT(strstr(source,
					 "CMX_RESOLVE_UNKNOWN") == NULL);

	ordinary_wait = strstr(source, "case CMDL_WAIT_MEMBER:");
	ordinary_call = ordinary_wait != NULL
						? strstr(
							  ordinary_wait,
							  "cluster_current_mx_wait_member_restart(")
						: NULL;
	UT_ASSERT_NOT_NULL(ordinary_wait);
	UT_ASSERT_NOT_NULL(ordinary_call);
	UT_ASSERT_STR_CONTAINS(source, "Assert(restart);");
	UT_ASSERT_STR_CONTAINS(source, "goto cluster_hot_search_restart;");
	UT_ASSERT_STR_CONTAINS(source, "goto cluster_latest_tid_recheck;");

	/*
	 * WHERE CURRENT OF must enter the current-MX authority bridge before its
	 * generic MVCC visibility probe.  The legacy visibility path can prove
	 * only terminal updater members, so probing it first rejects the exact
	 * ACTIVE case before the typed WAIT branch is reachable.
	 */
	latest_tid = strstr(source, "\nheap_get_latest_tid(");
	latest_recheck = latest_tid != NULL
					   ? strstr(latest_tid, "cluster_latest_tid_recheck:")
					   : NULL;
	latest_visibility = latest_recheck != NULL
						  ? strstr(
								latest_recheck,
								"valid = HeapTupleSatisfiesVisibility(")
						  : NULL;
	latest_authority = latest_recheck != NULL
						 ? strstr(
							   latest_recheck,
							   "if (cluster_authoritative_current_multi")
						 : NULL;
	UT_ASSERT_NOT_NULL(latest_tid);
	UT_ASSERT_NOT_NULL(latest_recheck);
	UT_ASSERT_NOT_NULL(latest_visibility);
	UT_ASSERT_NOT_NULL(latest_authority);
	UT_ASSERT(latest_authority == NULL || latest_visibility == NULL
			  || latest_authority < latest_visibility);
	free(source);
}


UT_TEST(test_current_mx_unknown_wait_result_fails_closed_before_relock)
{
	char *source = read_source(HEAPAM_SOURCE_PATH);
	const char *helper;
	const char *helper_end;
	const char *switch_pos;
	const char *open_brace;
	const char *close_brace;
	const char *default_pos;
	const char *finish;
	const char *error;
	const char *relock;

	if (source == NULL)
		return;

	helper = strstr(source, "\ncluster_current_mx_wait_member_restart(");
	helper_end = helper != NULL
					 ? strstr(
						   helper,
						   "\nstatic bool\ncluster_current_mx_memo_lookup(")
					 : NULL;
	switch_pos = helper != NULL ? strstr(helper, "switch (txw)") : NULL;
	open_brace = switch_pos != NULL ? strchr(switch_pos, '{') : NULL;
	close_brace = matching_close_brace(open_brace);
	default_pos = find_before(switch_pos, "default:", close_brace);
	finish = find_before(
		default_pos,
		"cluster_current_mx_operation_finish(operation);",
		close_brace);
	error = find_before(finish, "ereport(ERROR", close_brace);
	relock = close_brace != NULL
				 ? strstr(
					   close_brace,
					   "LockBuffer(buffer, BUFFER_LOCK_EXCLUSIVE);")
				 : NULL;

	UT_ASSERT_NOT_NULL(helper);
	UT_ASSERT_NOT_NULL(helper_end);
	UT_ASSERT_NOT_NULL(switch_pos);
	UT_ASSERT_NOT_NULL(open_brace);
	UT_ASSERT_NOT_NULL(close_brace);
	UT_ASSERT_NOT_NULL(default_pos);
	UT_ASSERT_NOT_NULL(finish);
	UT_ASSERT_NOT_NULL(error);
	UT_ASSERT_NOT_NULL(relock);
	if (helper != NULL && helper_end != NULL && switch_pos != NULL
		&& open_brace != NULL && close_brace != NULL && default_pos != NULL
		&& finish != NULL && error != NULL && relock != NULL)
		UT_ASSERT(helper < switch_pos && switch_pos < open_brace
				  && open_brace < default_pos && default_pos < finish
				  && finish < error && error < close_brace
				  && close_brace < relock && relock < helper_end);
	free(source);
}


UT_TEST(test_latest_tid_consumes_authenticated_root_before_lock_downgrade)
{
	char *source = read_source(HEAPAM_SOURCE_PATH);
	const char *latest_tid;
	const char *latest_end;
	const char *authority_call;
	const char *failure_goto;
	const char *failure_end;
	const char *visibility;
	const char *conflict;
	const char *authority_follow;
	const char *capture;
	const char *unlock;
	const char *share;
	const char *assign;
	const char *follow_goto;
	const char *stale_tuple_use;

	if (source == NULL)
		return;

	latest_tid = strstr(source, "\nheap_get_latest_tid(");
	latest_end = latest_tid != NULL
					 ? strstr(latest_tid, "\nUpdateXmaxHintBits(")
					 : NULL;
	authority_call = latest_tid != NULL
						 ? strstr(
							   latest_tid,
							   "cluster_current_mx_hot_updater_for_chain(")
						 : NULL;
	failure_goto = authority_call != NULL
					   ? strstr(
							 authority_call,
							 "goto cluster_latest_tid_recheck;")
					   : NULL;
	failure_end = failure_goto != NULL ? strchr(failure_goto, '}') : NULL;
	visibility = failure_end != NULL
					 ? strstr(
						   failure_end,
						   "valid = HeapTupleSatisfiesVisibility(")
					 : NULL;
	conflict = visibility != NULL
				   ? strstr(
						 visibility,
						 "HeapCheckForSerializableConflictOut(")
				   : NULL;
	authority_follow = conflict != NULL
						   ? strstr(
								 conflict,
								 "if (cluster_authoritative_current_multi)")
						   : NULL;
	capture = authority_follow != NULL
				  ? strstr(
						authority_follow,
						"ItemPointerCopy(&tp.t_data->t_ctid,")
				  : NULL;
	unlock = capture != NULL
				 ? strstr(
					   capture,
					   "LockBuffer(buffer, BUFFER_LOCK_UNLOCK);")
				 : NULL;
	share = unlock != NULL
				? strstr(
					  unlock,
					  "LockBuffer(buffer, BUFFER_LOCK_SHARE);")
				: NULL;
	assign = share != NULL
				 ? strstr(share, "ctid = cluster_authoritative_next_ctid;")
				 : NULL;
	follow_goto = assign != NULL
					  ? strstr(assign, "goto cluster_latest_tid_recheck;")
					  : NULL;
	stale_tuple_use = find_before(unlock, "tp.t_data", follow_goto);

	UT_ASSERT_NOT_NULL(latest_tid);
	UT_ASSERT_NOT_NULL(latest_end);
	UT_ASSERT_NOT_NULL(authority_call);
	UT_ASSERT_NOT_NULL(failure_goto);
	UT_ASSERT_NOT_NULL(failure_end);
	UT_ASSERT_NOT_NULL(visibility);
	UT_ASSERT_NOT_NULL(conflict);
	UT_ASSERT_NOT_NULL(authority_follow);
	UT_ASSERT_NOT_NULL(capture);
	UT_ASSERT_NOT_NULL(unlock);
	UT_ASSERT_NOT_NULL(share);
	UT_ASSERT_NOT_NULL(assign);
	UT_ASSERT_NOT_NULL(follow_goto);
	if (latest_tid != NULL && latest_end != NULL && authority_call != NULL
		&& failure_goto != NULL && failure_end != NULL && visibility != NULL
		&& conflict != NULL && authority_follow != NULL && capture != NULL
		&& unlock != NULL && share != NULL && assign != NULL
		&& follow_goto != NULL)
	{
		const char *unguarded_unlock = find_before(
			failure_end,
			"LockBuffer(buffer, BUFFER_LOCK_UNLOCK);",
			visibility);

		UT_ASSERT(latest_tid < authority_call
				  && authority_call < failure_goto
				  && failure_goto < failure_end
				  && failure_end < visibility && visibility < conflict
				  && conflict < authority_follow
				  && authority_follow < capture && capture < unlock
				  && unlock < share && share < assign
				  && assign < follow_goto && follow_goto < latest_end);
		UT_ASSERT_NULL(unguarded_unlock);
		UT_ASSERT_NULL(stale_tuple_use);
	}
	free(source);
}


UT_TEST(test_current_mx_local_resolution_is_orthogonal_to_matching_wakeup)
{
	char *heap_source = read_source(HEAPAM_SOURCE_PATH);
	char *tx_source = read_source(TX_ENQUEUE_SOURCE_PATH);
	char *hint_source = read_source(TT_HINT_SOURCE_PATH);
	char *local_source = read_source(TT_LOCAL_SOURCE_PATH);
	char *tap_source = read_source(HOT_TAP_SOURCE_PATH);

	if (heap_source == NULL || tx_source == NULL || hint_source == NULL
		|| local_source == NULL || tap_source == NULL)
		goto done;

	/*
	 * The current-MX wakeup metric counts matching SetLatch signal events,
	 * not terminal wait outcomes.  A same-node holder becomes visible to the
	 * wait loop's exact TT recheck without passing through the inbound
	 * remote-hint waker, so RESOLVED may legitimately pair with zero wakeups.
	 */
	UT_ASSERT_EQ(count_occurrences(
					 tx_source,
					 "cluster_multixact_current_stats_bump(CMX_STAT_WAKEUP)"),
				 1);
	UT_ASSERT_EQ(count_occurrences(
					 hint_source, "cluster_txw_wake_waiters(key);"),
				 1);
	UT_ASSERT_EQ(count_occurrences(
					 local_source, "cluster_txw_wake_waiters("),
				 0);
	UT_ASSERT_STR_CONTAINS(
		local_source,
		"cluster_tt_status_hint_emit(key, status, commit_scn);");

	/*
	 * hot_proof_hit_count is per successful MATCH consumer, not per outer
	 * operation.  This deterministic WHERE CURRENT OF fixture consumes one
	 * chain-helper MATCH and two ordinary-updated MATCHes after restart.
	 */
	UT_ASSERT_EQ(count_occurrences(
					 heap_source,
					 "cluster_multixact_current_stats_bump("
					 "CMX_STAT_HOT_PROOF_HIT)"),
				 3);
	UT_ASSERT_STR_CONTAINS(
		tap_source,
		"state_int($node1, 'wait_resolved_count')\n"
		"\t\t\t  == $before{wait_resolved_count} + 1");
	UT_ASSERT_STR_CONTAINS(
		tap_source,
		"state_int($node1, 'restart_bucket_1_count')\n"
		"\t\t\t  == $before{restart_bucket_1_count} + 1");
	UT_ASSERT_STR_CONTAINS(
		tap_source,
		"state_int($node1, 'hot_proof_hit_count')\n"
		"\t\t\t  == $before{hot_proof_hit_count} + 3");
	UT_ASSERT_STR_CONTAINS(
		tap_source,
		"RED-HOT matching SetLatch wakeup signal delta=");
	UT_ASSERT_NULL(strstr(
		tap_source,
		"state_int($node1, 'wakeup_count') == $before{wakeup_count} + 1"));

done:
	free(heap_source);
	free(tx_source);
	free(hint_source);
	free(local_source);
	free(tap_source);
}


int
main(void)
{
	UT_PLAN(7);
	UT_RUN(test_all_production_callers_use_the_five_way_closed_set);
	UT_RUN(test_wait_loop_consumes_a_matching_deadlock_token);
	UT_RUN(test_current_mx_wakeup_counts_the_matching_setlatch_event);
	UT_RUN(test_current_mx_heap_wait_restart_is_shared_and_rechecks_hot);
	UT_RUN(test_current_mx_unknown_wait_result_fails_closed_before_relock);
	UT_RUN(test_latest_tid_consumes_authenticated_root_before_lock_downgrade);
	UT_RUN(test_current_mx_local_resolution_is_orthogonal_to_matching_wakeup);
	UT_DONE();
	return ut_failed_count == 0 ? 0 : 1;
}
