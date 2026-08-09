/*-------------------------------------------------------------------------
 *
 * test_cluster_jit_source_contract.c
 *
 *	Stage 8 JIT source-contract gate.  This is deliberately a source census:
 *	it freezes the approved ABI and sole-owner shapes before the corresponding
 *	production modules exist.  It does not implement or mirror redo, repair,
 *	reconciliation, or retry decisions.
 *
 *-------------------------------------------------------------------------
 */

#include <dirent.h>
#include <limits.h>
#include <stdbool.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>

#include "unit_test.h"

UT_DEFINE_GLOBALS();

#define SOURCE_ROOT "src"

static char *
read_source_file(const char *path)
{
	FILE	   *file;
	long		length;
	char	   *contents;

	file = fopen(path, "rb");
	if (file == NULL)
		return NULL;
	if (fseek(file, 0, SEEK_END) != 0 || (length = ftell(file)) < 0 ||
		fseek(file, 0, SEEK_SET) != 0) {
		fclose(file);
		return NULL;
	}
	contents = malloc((size_t) length + 1);
	if (contents == NULL) {
		fclose(file);
		return NULL;
	}
	if (fread(contents, 1, (size_t) length, file) != (size_t) length) {
		free(contents);
		fclose(file);
		return NULL;
	}
	contents[length] = '\0';
	fclose(file);
	return contents;
}

static int
count_literal(const char *contents, const char *literal)
{
	const char *cursor = contents;
	int		count = 0;
	size_t		literal_length = strlen(literal);

	while ((cursor = strstr(cursor, literal)) != NULL) {
		count++;
		cursor += literal_length;
	}
	return count;
}

static int
count_literal_in_file(const char *path, const char *literal)
{
	char	   *contents = read_source_file(path);
	int		count = 0;

	if (contents != NULL) {
		count = count_literal(contents, literal);
		free(contents);
	}
	return count;
}

static int
source_file_exists(const char *path)
{
	char	   *contents = read_source_file(path);

	if (contents == NULL)
		return 0;
	free(contents);
	return 1;
}

static int
count_lines_with(const char *path, const char *first, const char *second)
{
	char	   *contents = read_source_file(path);
	char	   *line;
	int		count = 0;

	if (contents == NULL)
		return 0;
	line = contents;
	while (line != NULL) {
		char	   *next = strchr(line, '\n');

		if (next != NULL)
			*next++ = '\0';
		if (strstr(line, first) != NULL && strstr(line, second) != NULL)
			count++;
		line = next;
	}
	free(contents);
	return count;
}

static bool
has_suffix(const char *name, const char *suffix)
{
	size_t		name_length = strlen(name);
	size_t		suffix_length = strlen(suffix);

	return name_length >= suffix_length &&
		strcmp(name + name_length - suffix_length, suffix) == 0;
}

static int
count_literal_in_tree(const char *path, const char *literal, const char *suffix)
{
	DIR		   *directory;
	struct dirent *entry;
	int		count = 0;

	directory = opendir(path);
	if (directory == NULL)
		return 0;
	while ((entry = readdir(directory)) != NULL) {
		char		child[PATH_MAX];
		struct stat status;

		if (strcmp(entry->d_name, ".") == 0 || strcmp(entry->d_name, "..") == 0)
			continue;
		if (snprintf(child, sizeof(child), "%s/%s", path, entry->d_name) >=
			(int) sizeof(child))
			continue;
		if (stat(child, &status) != 0)
			continue;
		if (S_ISDIR(status.st_mode))
			count += count_literal_in_tree(child, literal, suffix);
		else if (S_ISREG(status.st_mode) && has_suffix(entry->d_name, suffix))
			count += count_literal_in_file(child, literal);
	}
	closedir(directory);
	return count;
}

static void
expect_count(const char *contract, int actual, int expected)
{
	if (actual != expected) {
		printf("# %s: expected %d, found %d\n", contract, expected, actual);
		ut_current_failed = 1;
	}
}

UT_TEST(test_jit_pageversion_24_byte_abi)
{
	expect_count("JIT_PAGEVERSION_24B_ABI struct",
			 count_literal_in_file(SOURCE_ROOT "/include/access/xlogrecord.h",
						   "typedef struct RfPageVersionV1"), 1);
	expect_count("JIT_PAGEVERSION_24B_ABI incarnation",
			 count_lines_with(SOURCE_ROOT "/include/access/xlogrecord.h",
						  "segment_incarnation[16]", "uint8"), 1);
	expect_count("JIT_PAGEVERSION_24B_ABI token",
			 count_lines_with(SOURCE_ROOT "/include/access/xlogrecord.h",
						  "mutation_token", "uint64"), 1);
	expect_count("JIT_PAGEVERSION_24B_ABI static_assert",
			 count_literal_in_file(SOURCE_ROOT "/include/access/xlogrecord.h",
						   "sizeof(RfPageVersionV1) == 24"), 1);
}

UT_TEST(test_jit_id251_uses_48_byte_entries_and_1600_byte_maximum)
{
	const char *header = SOURCE_ROOT "/include/access/xlogrecord.h";

	expect_count("JIT_ID251_BLOCK_ID",
			 count_lines_with(header, "XLR_BLOCK_ID_PAGE_VERSION_EDGE", "251"), 1);
	expect_count("JIT_ID251_ENTRY_SIZE_48",
			 count_lines_with(header, "XLR_PAGE_VERSION_EDGE_ENTRY_SIZE", "48"), 1);
	expect_count("JIT_ID251_MAX_SIZE_1600",
			 count_lines_with(header, "XLR_PAGE_VERSION_EDGE_MAX_SIZE", "1600"), 1);
}

UT_TEST(test_jit_scalar_generation_references_are_absent)
{
	expect_count("JIT_NO_SCALAR_GENERATION_TOKEN",
			 count_literal_in_tree(SOURCE_ROOT "/backend", "ClusterFailureGenerationToken", ".c"), 0);
	expect_count("JIT_NO_SCALAR_GENERATION_FIELD",
			 count_literal_in_tree(SOURCE_ROOT "/backend", "failure_generation", ".c"), 0);
	expect_count("JIT_NO_SCALAR_GENERATION_TOKEN_HEADER",
			 count_literal_in_tree(SOURCE_ROOT "/include", "ClusterFailureGenerationToken", ".h"), 0);
	expect_count("JIT_NO_SCALAR_GENERATION_FIELD_HEADER",
			 count_literal_in_tree(SOURCE_ROOT "/include", "failure_generation", ".h"), 0);
}

UT_TEST(test_jit_generated_route_manifest_has_one_owner)
{
	expect_count("JIT_ROUTE_MANIFEST_DEF_OWNER",
			 source_file_exists(SOURCE_ROOT "/include/cluster/cluster_rf_route_manifest.def"), 1);
	expect_count("JIT_ROUTE_MANIFEST_IMPLEMENTATION_OWNER",
			 source_file_exists(SOURCE_ROOT "/backend/cluster/cluster_rf_route.c"), 1);
	expect_count("JIT_ROUTE_MANIFEST_SINGLE_GENERATED_INCLUDE",
			 count_lines_with(SOURCE_ROOT "/backend/cluster/cluster_rf_route.c",
						 "#include", "cluster_rf_route_manifest.def"), 1);
}

UT_TEST(test_jit_bufmgr_has_one_local_repair_constructor_caller)
{
	expect_count("JIT_LOCAL_REPAIR_OWNER_FILE",
			 source_file_exists(SOURCE_ROOT "/backend/cluster/cluster_page_local_repair.c"), 1);
	expect_count("JIT_LOCAL_REPAIR_BUFMGR_CONSTRUCTOR_CALLER",
			 count_literal_in_file(SOURCE_ROOT "/backend/storage/buffer/bufmgr.c",
						   "rf_page_local_repair_guard_begin("), 1);
	expect_count("JIT_LOCAL_REPAIR_SINGLE_PRODUCTION_CALL_EDGE",
			 count_literal_in_tree(SOURCE_ROOT "/backend",
							   "rf_page_local_repair_guard_begin(", ".c"), 2);
}

UT_TEST(test_jit_failed_owner_has_one_classification_only_reconcile_entry)
{
	expect_count("JIT_FAILED_OWNER_RECONCILE_OWNER_FILE",
			 source_file_exists(SOURCE_ROOT "/backend/cluster/cluster_space_reconcile.c"), 1);
	expect_count("JIT_FAILED_OWNER_RECONCILE_SINGLE_CALL_EDGE",
			 count_literal_in_tree(SOURCE_ROOT "/backend",
							   "cluster_space_reconcile_reserved_orphan_v1(", ".c"), 2);
}

UT_TEST(test_jit_cold_retry_has_one_poll_constant_and_formed_startup_caller)
{
	const char *header = SOURCE_ROOT "/include/cluster/cluster_recovery_cold_retry.h";

	expect_count("JIT_COLD_RETRY_OWNER_FILE",
			 source_file_exists(SOURCE_ROOT "/backend/cluster/cluster_recovery_cold_retry.c"), 1);
	expect_count("JIT_COLD_RETRY_POLL_100MS",
			 count_lines_with(header, "CLUSTER_COLD_RETRY_POLL_MS", "INT32_C(100)"), 1);
	expect_count("JIT_COLD_RETRY_FORMED_STARTUP_CALLER",
			 count_lines_with(SOURCE_ROOT "/backend/access/transam/xlogrecovery.c",
						 "#include", "cluster_recovery_cold_retry.h"), 1);
	expect_count("JIT_COLD_RETRY_SINGLE_CALLER_INCLUDE",
			 count_literal_in_tree(SOURCE_ROOT "/backend",
							   "cluster_recovery_cold_retry.h", ".c"), 1);
}

int
main(void)
{
	UT_PLAN(7);
	UT_RUN(test_jit_pageversion_24_byte_abi);
	UT_RUN(test_jit_id251_uses_48_byte_entries_and_1600_byte_maximum);
	UT_RUN(test_jit_scalar_generation_references_are_absent);
	UT_RUN(test_jit_generated_route_manifest_has_one_owner);
	UT_RUN(test_jit_bufmgr_has_one_local_repair_constructor_caller);
	UT_RUN(test_jit_failed_owner_has_one_classification_only_reconcile_entry);
	UT_RUN(test_jit_cold_retry_has_one_poll_constant_and_formed_startup_caller);
	UT_DONE();
	return ut_failed_count == 0 ? 0 : 1;
}
