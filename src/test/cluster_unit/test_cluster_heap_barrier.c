/*-------------------------------------------------------------------------
 *
 * test_cluster_heap_barrier.c
 *	  Source-executable contract tests for typed heap barrier propagation.
 *
 *
 * Portions Copyright (c) 1996-2024, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2026, pgrac contributors
 *
 * Author: SqlRush <sqlrush@gmail.com>
 *
 * IDENTIFICATION
 *	  src/test/cluster_unit/test_cluster_heap_barrier.c
 *
 * NOTES
 *	  This is a pgrac-original standalone test.  It pins the production
 *	  TableAM, heap, and nbtree ownership choreography without replacing the
 *	  backend-only visibility and buffer machinery with a fake implementation.
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#undef printf

#include "unit_test.h"


UT_DEFINE_GLOBALS();


#ifndef TABLEAM_HEADER_PATH
#error "TABLEAM_HEADER_PATH must identify production tableam.h"
#endif
#ifndef TABLEAM_SOURCE_PATH
#error "TABLEAM_SOURCE_PATH must identify production tableam.c"
#endif
#ifndef HEAPAM_HANDLER_SOURCE_PATH
#error "HEAPAM_HANDLER_SOURCE_PATH must identify production heapam_handler.c"
#endif
#ifndef NBTINSERT_SOURCE_PATH
#error "NBTINSERT_SOURCE_PATH must identify production nbtinsert.c"
#endif

void
ExceptionalCondition(const char *condition_name pg_attribute_unused(),
					 const char *file_name pg_attribute_unused(),
					 int line_number pg_attribute_unused())
{
	abort();
}

static char *
read_source(const char *path)
{
	FILE	   *file;
	char	   *source;
	long		length;

	file = fopen(path, "rb");
	UT_ASSERT_NOT_NULL(file);
	if (file == NULL)
		return NULL;
	UT_ASSERT_EQ(fseek(file, 0, SEEK_END), 0);
	length = ftell(file);
	UT_ASSERT(length > 0);
	UT_ASSERT_EQ(fseek(file, 0, SEEK_SET), 0);
	source = malloc((size_t) length + 1);
	UT_ASSERT_NOT_NULL(source);
	if (source == NULL)
	{
		fclose(file);
		return NULL;
	}
	UT_ASSERT_EQ((long) fread(source, 1, (size_t) length, file), length);
	source[length] = '\0';
	fclose(file);
	return source;
}

static const char *
find_function_end(const char *function)
{
	return function != NULL ? strstr(function, "\n}\n") : NULL;
}

static int
count_occurrences(const char *source, const char *needle)
{
	int			count = 0;

	while (source != NULL && (source = strstr(source, needle)) != NULL)
	{
		count++;
		source += strlen(needle);
	}
	return count;
}

UT_TEST(test_tableam_has_three_disjoint_results_and_tail_callback)
{
	char	   *header = read_source(TABLEAM_HEADER_PATH);
	const char *not_found;
	const char *found;
	const char *barrier;
	const char *callback;
	const char *struct_end;

	UT_ASSERT_NOT_NULL(header);
	not_found = header != NULL ? strstr(header, "TABLE_INDEX_FETCH_NOT_FOUND = 0") : NULL;
	found = not_found != NULL ? strstr(not_found, "TABLE_INDEX_FETCH_FOUND") : NULL;
	barrier = found != NULL ? strstr(found, "TABLE_INDEX_FETCH_BARRIER_CLOSED") : NULL;
	callback = header != NULL ? strstr(header, "(*index_fetch_tuple_barrier_aware)") : NULL;
	struct_end = header != NULL ? strstr(header, "} TableAmRoutine;") : NULL;
	UT_ASSERT_NOT_NULL(not_found);
	UT_ASSERT(found != NULL && not_found < found);
	UT_ASSERT(barrier != NULL && found < barrier);
	UT_ASSERT(callback != NULL && callback < struct_end);
	UT_ASSERT(count_occurrences(header, "TABLE_INDEX_FETCH_BARRIER_CLOSED") >= 1);
	free(header);
}

UT_TEST(test_typed_helper_has_fallback_and_exact_cleanup)
{
	char	   *source = read_source(TABLEAM_SOURCE_PATH);
	const char *helper;
	const char *helper_end;

	UT_ASSERT_NOT_NULL(source);
	helper = source != NULL ? strstr(source,
								 "\ntable_index_fetch_tuple_check_barrier_aware(") : NULL;
	helper_end = find_function_end(helper);
	UT_ASSERT_NOT_NULL(helper);
	UT_ASSERT_NOT_NULL(helper_end);
	if (helper != NULL && helper_end != NULL)
	{
		const char *slot = strstr(helper, "table_slot_create(rel, NULL)");
		const char *begin = strstr(helper, "table_index_fetch_begin(rel)");
		const char *typed = strstr(helper, "index_fetch_tuple_barrier_aware");
		const char *fallback = strstr(helper, "table_index_fetch_tuple(");
		const char *end = strstr(helper, "table_index_fetch_end(scan)");
		const char *drop = strstr(helper, "ExecDropSingleTupleTableSlot(slot)");

		UT_ASSERT(slot != NULL && begin != NULL && slot < begin);
		UT_ASSERT(typed != NULL && typed < helper_end);
		UT_ASSERT(fallback != NULL && typed < fallback && fallback < helper_end);
		UT_ASSERT(end != NULL && fallback < end && end < helper_end);
		UT_ASSERT(drop != NULL && end < drop && drop < helper_end);
	}
	free(source);
}

UT_TEST(test_existing_bool_helper_stays_independent)
{
	char	   *source = read_source(TABLEAM_SOURCE_PATH);
	const char *legacy;
	const char *legacy_end;

	UT_ASSERT_NOT_NULL(source);
	legacy = source != NULL ? strstr(source, "\ntable_index_fetch_tuple_check(Relation rel,") : NULL;
	legacy_end = find_function_end(legacy);
	UT_ASSERT_NOT_NULL(legacy);
	UT_ASSERT_NOT_NULL(legacy_end);
	if (legacy != NULL && legacy_end != NULL)
	{
		const char *old_call = strstr(legacy, "found = table_index_fetch_tuple(");
		const char *typed_call = strstr(legacy,
									"table_index_fetch_tuple_check_barrier_aware(");

		UT_ASSERT(old_call != NULL && old_call < legacy_end);
		UT_ASSERT(typed_call == NULL || typed_call > legacy_end);
	}
	free(source);
}

UT_TEST(test_heap_barrier_returns_before_visibility_or_tid_mutation)
{
	char	   *source = read_source(HEAPAM_HANDLER_SOURCE_PATH);
	const char *common;
	const char *common_end;
	const char *share_lock;
	const char *barrier_return;
	const char *hot_search;

	UT_ASSERT_NOT_NULL(source);
	common = source != NULL ? strstr(source, "\nheapam_index_fetch_tuple_internal(") : NULL;
	common_end = find_function_end(common);
	UT_ASSERT_NOT_NULL(common);
	UT_ASSERT_NOT_NULL(common_end);
	share_lock = common != NULL ? strstr(common, "ClusterLockBufferShareBarrierAware(") : NULL;
	barrier_return = share_lock != NULL ? strstr(share_lock,
											  "return TABLE_INDEX_FETCH_BARRIER_CLOSED;") : NULL;
	hot_search = common != NULL ? strstr(common, "heap_hot_search_buffer(") : NULL;
	UT_ASSERT(share_lock != NULL && share_lock < common_end);
	UT_ASSERT(barrier_return != NULL && barrier_return < hot_search);
	UT_ASSERT(hot_search != NULL && hot_search < common_end);
	if (share_lock != NULL && barrier_return != NULL)
	{
		const char *call_again = strstr(share_lock, "*call_again = false;");
		const char *all_dead = strstr(share_lock, "*all_dead = false;");

		UT_ASSERT(call_again != NULL && call_again < barrier_return);
		UT_ASSERT(all_dead != NULL && all_dead < barrier_return);
	}
	UT_ASSERT_NOT_NULL(strstr(source,
							 ".index_fetch_tuple_barrier_aware = heapam_index_fetch_tuple_barrier_aware"));
	free(source);
}

UT_TEST(test_btree_propagates_both_heap_refusals_without_bool_aliasing)
{
	char	   *source = read_source(NBTINSERT_SOURCE_PATH);
	const char *check_unique;
	const char *check_unique_end;

	UT_ASSERT_NOT_NULL(source);
	check_unique = source != NULL ? strstr(source, "\n_bt_check_unique(Relation rel,") : NULL;
	check_unique_end = find_function_end(check_unique);
	UT_ASSERT_NOT_NULL(check_unique);
	UT_ASSERT_NOT_NULL(check_unique_end);
	if (check_unique != NULL && check_unique_end != NULL)
	{
		UT_ASSERT(count_occurrences(check_unique,
									"table_index_fetch_tuple_check_barrier_aware(") >= 2);
		UT_ASSERT(count_occurrences(check_unique,
									"TABLE_INDEX_FETCH_BARRIER_CLOSED") >= 2);
		UT_ASSERT_NOT_NULL(strstr(check_unique, "*barrier_tid = htid;"));
		UT_ASSERT_NOT_NULL(strstr(check_unique, "*barrier_closed = true;"));
		UT_ASSERT_NOT_NULL(strstr(check_unique, "insertstate->bounds_valid = false;"));
		UT_ASSERT_NOT_NULL(strstr(check_unique, "_bt_relbuf(rel, nbuf);"));
	}
	free(source);
}

UT_TEST(test_btree_owner_unwinds_warms_and_researches)
{
	char	   *source = read_source(NBTINSERT_SOURCE_PATH);
	const char *doinsert;
	const char *doinsert_end;
	const char *barrier_branch;
	const char *leaf_release;
	const char *stack_free;
	const char *warm;
	const char *interrupt;
	const char *research;

	UT_ASSERT_NOT_NULL(source);
	doinsert = source != NULL ? strstr(source, "\n_bt_doinsert(Relation rel,") : NULL;
	doinsert_end = find_function_end(doinsert);
	barrier_branch = doinsert != NULL ? strstr(doinsert, "if (barrier_closed)") : NULL;
	leaf_release = barrier_branch != NULL ? strstr(barrier_branch,
											 "_bt_relbuf(rel, insertstate.buf);") : NULL;
	stack_free = leaf_release != NULL ? strstr(leaf_release, "_bt_freestack(stack);") : NULL;
	warm = stack_free != NULL ? strstr(stack_free,
									"table_index_fetch_tuple_check_barrier_aware(") : NULL;
	interrupt = warm != NULL ? strstr(warm, "CHECK_FOR_INTERRUPTS();") : NULL;
	research = interrupt != NULL ? strstr(interrupt, "goto search;") : NULL;
	UT_ASSERT_NOT_NULL(doinsert);
	UT_ASSERT_NOT_NULL(doinsert_end);
	UT_ASSERT(barrier_branch != NULL && barrier_branch < doinsert_end);
	UT_ASSERT(leaf_release != NULL && leaf_release < doinsert_end);
	UT_ASSERT(stack_free != NULL && leaf_release < stack_free && stack_free < doinsert_end);
	UT_ASSERT(warm != NULL && stack_free < warm && warm < doinsert_end);
	UT_ASSERT(interrupt != NULL && warm < interrupt && interrupt < doinsert_end);
	UT_ASSERT(research != NULL && interrupt < research && research < doinsert_end);
	if (warm != NULL)
		UT_ASSERT_NOT_NULL(strstr(warm, "SnapshotAny"));
	free(source);
}

int
main(void)
{
	UT_PLAN(6);
	UT_RUN(test_tableam_has_three_disjoint_results_and_tail_callback);
	UT_RUN(test_typed_helper_has_fallback_and_exact_cleanup);
	UT_RUN(test_existing_bool_helper_stays_independent);
	UT_RUN(test_heap_barrier_returns_before_visibility_or_tid_mutation);
	UT_RUN(test_btree_propagates_both_heap_refusals_without_bool_aliasing);
	UT_RUN(test_btree_owner_unwinds_warms_and_researches);
	UT_DONE();
	return ut_failed_count == 0 ? 0 : 1;
}
