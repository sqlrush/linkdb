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


static void
assert_exhaustive_result_switch(const char *call)
{
	const char *window_end;
	const char *switch_pos;
	static const char *const cases[] = {
		"case CLUSTER_TXW_RESOLVED:", "case CLUSTER_TXW_TIMEOUT:",	"case CLUSTER_TXW_DEAD_HOLDER:",
		"case CLUSTER_TXW_RETRY:",	  "case CLUSTER_TXW_DEADLOCK:",
	};
	int i;

	UT_ASSERT_NOT_NULL(call);
	if (call == NULL)
		return;

	window_end = call + Min(strlen(call), (size_t)3000);
	switch_pos = strstr(call, "switch (txw)");
	UT_ASSERT_NOT_NULL(switch_pos);
	if (switch_pos == NULL || switch_pos >= window_end)
		return;

	for (i = 0; i < lengthof(cases); i++) {
		const char *case_pos = strstr(switch_pos, cases[i]);

		UT_ASSERT_NOT_NULL(case_pos);
		UT_ASSERT(case_pos == NULL || case_pos < window_end);
	}
}


UT_TEST(test_all_production_callers_use_the_five_way_closed_set)
{
	char *source = read_source(HEAPAM_SOURCE_PATH);
	const char *first;
	const char *second;

	if (source == NULL)
		return;

	UT_ASSERT_EQ(count_occurrences(source, "cluster_tx_enqueue_wait("), 2);
	first = strstr(source, "cluster_tx_enqueue_wait(");
	second = first != NULL ? strstr(first + 1, "cluster_tx_enqueue_wait(") : NULL;
	assert_exhaustive_result_switch(first);
	assert_exhaustive_result_switch(second);
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


int
main(void)
{
	UT_PLAN(2);
	UT_RUN(test_all_production_callers_use_the_five_way_closed_set);
	UT_RUN(test_wait_loop_consumes_a_matching_deadlock_token);
	UT_DONE();
	return ut_failed_count == 0 ? 0 : 1;
}
