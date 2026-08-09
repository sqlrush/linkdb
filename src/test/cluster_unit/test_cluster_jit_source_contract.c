/* Stage 8 JIT source-contract RED: structural source census only. */
#include <dirent.h>
#include <limits.h>
#include <stdbool.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>

#include "unit_test.h"

#ifndef JIT_SOURCE_ROOT
#error "JIT_SOURCE_ROOT must be an absolute configured source root"
#endif

UT_DEFINE_GLOBALS();

static const char source_root[] = JIT_SOURCE_ROOT;

static char *
read_file(const char *path)
{
	FILE *f = fopen(path, "rb");
	long n;
	char *s;

	if (f == NULL || fseek(f, 0, SEEK_END) != 0 || (n = ftell(f)) < 0 ||
		fseek(f, 0, SEEK_SET) != 0) {
		if (f != NULL)
			fclose(f);
		return NULL;
	}
	s = malloc((size_t) n + 1);
	if (s == NULL || fread(s, 1, (size_t) n, f) != (size_t) n) {
		free(s);
		fclose(f);
		return NULL;
	}
	s[n] = '\0';
	fclose(f);
	return s;
}

static bool
root_path(char *dst, size_t n, const char *relative)
{
	return snprintf(dst, n, "%s/%s", source_root, relative) < (int) n;
}

static bool
valid_root(void)
{
	char required[PATH_MAX];
	struct stat st;

	return source_root[0] == '/' && stat(source_root, &st) == 0 && S_ISDIR(st.st_mode) &&
		root_path(required, sizeof(required), "include/access/xlogrecord.h") &&
		read_file(required) != NULL;
}

/* Replace comments and literals with spaces while retaining punctuation/newlines. */
static char *
code_only(const char *raw)
{
	char *out = strdup(raw);
	size_t i;

	if (out == NULL)
		return NULL;
	for (i = 0; raw[i] != '\0'; i++) {
		if (raw[i] == '/' && raw[i + 1] == '/') {
			for (; raw[i] != '\0' && raw[i] != '\n'; i++) out[i] = ' ';
			if (raw[i] == '\0') break;
		}
		if (raw[i] == '/' && raw[i + 1] == '*') {
			out[i] = ' ';
			i++;
			out[i] = ' ';
			for (; raw[i] != '\0' && !(raw[i] == '*' && raw[i + 1] == '/'); i++)
				if (raw[i] != '\n') out[i] = ' ';
			if (raw[i] == '*') {
				out[i] = ' ';
				i++;
				out[i] = ' ';
			}
		}
		if (raw[i] == '\'' || raw[i] == '"') {
			char quote = raw[i];
			out[i] = ' ';
			for (i++; raw[i] != '\0' && raw[i] != quote; i++) {
				if (raw[i] == '\\' && raw[i + 1] != '\0') { out[i++] = ' '; }
				out[i] = raw[i] == '\n' ? '\n' : ' ';
			}
			if (raw[i] == quote) out[i] = ' ';
		}
	}
	return out;
}

static bool
ident_at(const char *s, const char *at, const char *name)
{
	size_t n = strlen(name);
	return strncmp(at, name, n) == 0 &&
		(at == s || !((at[-1] == '_') || (at[-1] >= 'A' && at[-1] <= 'Z') ||
				(at[-1] >= 'a' && at[-1] <= 'z') || (at[-1] >= '0' && at[-1] <= '9'))) &&
		!((at[n] == '_') || (at[n] >= 'A' && at[n] <= 'Z') ||
		  (at[n] >= 'a' && at[n] <= 'z') || (at[n] >= '0' && at[n] <= '9'));
}

static const char *
skip_space(const char *p)
{
	while (*p == ' ' || *p == '\t' || *p == '\n' || *p == '\r') p++;
	return p;
}

static const char *
matching(const char *p, char open, char close)
{
	int depth = 0;
	for (; *p != '\0'; p++) {
		if (*p == open) depth++;
		else if (*p == close && --depth == 0) return p;
	}
	return NULL;
}

static int
count_ident(const char *s, const char *name)
{
	const char *p;
	int count = 0;
	for (p = s; (p = strstr(p, name)) != NULL; p++) if (ident_at(s, p, name)) count++;
	return count;
}

static int
count_define(const char *code, const char *name, const char *value)
{
	const char *p = code;
	int count = 0;
	while ((p = strchr(p, '#')) != NULL) {
		const char *line = p;
		p++;
		if ((line == code || line[-1] == '\n') && strncmp(skip_space(p), "define", 6) == 0) {
			const char *q = skip_space(skip_space(p) + 6);
			if (ident_at(code, q, name)) {
				q = skip_space(q + strlen(name));
				if (strncmp(q, value, strlen(value)) == 0) count++;
			}
		}
	}
	return count;
}

static const char *
function_body(const char *code, const char *name)
{
	const char *p;
	for (p = code; (p = strstr(p, name)) != NULL; p++) {
		const char *open;
		const char *close;
		if (!ident_at(code, p, name) || *(open = skip_space(p + strlen(name))) != '(') continue;
		close = matching(open, '(', ')');
		if (close != NULL && *(close = skip_space(close + 1)) == '{') return close;
	}
	return NULL;
}

static int
count_real_calls(const char *code, const char *name)
{
	const char *p;
	int count = 0;
	for (p = code; (p = strstr(p, name)) != NULL; p++) {
		const char *open;
		const char *close;
		if (!ident_at(code, p, name) || *(open = skip_space(p + strlen(name))) != '(') continue;
		close = matching(open, '(', ')');
		if (close != NULL && *skip_space(close + 1) != '{') count++;
	}
	return count;
}

static int
count_definitions(const char *code, const char *name)
{
	const char *p = code;
	int count = 0;
	while ((p = strstr(p, name)) != NULL) {
		const char *body = function_body(p, name);
		if (body != NULL) {
			count++;
			p = body + 1;
		} else
			p++;
	}
	return count;
}

static int
count_tree_symbol(const char *path, const char *name, bool definitions)
{
	DIR *dir = opendir(path);
	struct dirent *entry;
	int count = 0;
	if (dir == NULL) return 0;
	while ((entry = readdir(dir)) != NULL) {
		char child[PATH_MAX];
		struct stat st;
		char *raw;
		char *code;
		if (entry->d_name[0] == '.') continue;
		if (snprintf(child, sizeof(child), "%s/%s", path, entry->d_name) >= (int) sizeof(child) || stat(child, &st) != 0) continue;
		if (S_ISDIR(st.st_mode)) count += count_tree_symbol(child, name, definitions);
		else if (S_ISREG(st.st_mode) && strstr(entry->d_name, ".c") != NULL && (raw = read_file(child)) != NULL) {
			code = code_only(raw); free(raw);
			count += definitions ? count_definitions(code, name) : count_real_calls(code, name);
			free(code);
		}
	}
	closedir(dir);
	return count;
}

static void
expect(const char *name, int actual, int expected)
{
	if (actual != expected) {
		printf("# %s: expected %d, found %d\n", name, expected, actual);
		ut_current_failed = 1;
	}
}

static char *
source_code(const char *relative)
{
	char path[PATH_MAX];
	char *raw;
	char *code;
	if (!root_path(path, sizeof(path), relative) || (raw = read_file(path)) == NULL) return NULL;
	code = code_only(raw);
	free(raw);
	return code;
}

UT_TEST(test_census_ignores_comments_and_literals)
{
	char *code = code_only("/* rf_page_local_repair_guard_begin() */\n\"ORPHAN_BLOCKED\"\nrf_page_local_repair_guard_begin();");
	expect("JIT_CENSUS_COMMENT_STRING_AWARE", count_real_calls(code, "rf_page_local_repair_guard_begin"), 1);
	free(code);
}

UT_TEST(test_jit_pageversion_24_byte_abi_and_id251_macros)
{
	char *code = source_code("include/access/xlogrecord.h");
	const char *body = code ? strstr(code, "typedef struct RfPageVersionV1") : NULL;

	expect("JIT_PAGEVERSION_24B_NAMED_STRUCT", body != NULL, 1);
	expect("JIT_PAGEVERSION_24B_INCARNATION_FIELD", body && strstr(body, "uint8 segment_incarnation[16]") != NULL, 1);
	expect("JIT_PAGEVERSION_24B_TOKEN_FIELD", body && strstr(body, "uint64 mutation_token") != NULL, 1);
	expect("JIT_PAGEVERSION_24B_STATIC_ASSERT", code && count_ident(code, "RfPageVersionV1") >= 2 && strstr(code, "== 24") != NULL, 1);
	expect("JIT_ID251_BLOCK_ID", code ? count_define(code, "XLR_BLOCK_ID_PAGE_VERSION_EDGE", "251") : 0, 1);
	expect("JIT_ID251_ENTRY_SIZE_48", code ? count_define(code, "XLR_PAGE_VERSION_EDGE_ENTRY_SIZE", "48") : 0, 1);
	expect("JIT_ID251_MAX_SIZE_1600", code ? count_define(code, "XLR_PAGE_VERSION_EDGE_MAX_SIZE", "1600") : 0, 1);
	free(code);
}

UT_TEST(test_jit_named_owners_and_real_call_sites)
{
	char *route = source_code("backend/cluster/cluster_rf_route.c");
	char *bufmgr = source_code("backend/storage/buffer/bufmgr.c");
	char *cold = source_code("backend/access/transam/xlogrecovery.c");
	char *reconcile = source_code("backend/cluster/cluster_space_reconcile.c");

	expect("JIT_ROUTE_SINGLE_GENERATED_OWNER", route && count_ident(route, "cluster_rf_route_manifest") == 1, 1);
	expect("JIT_LOCAL_REPAIR_BUFMGR_SINGLE_REAL_CALL", bufmgr ? count_real_calls(bufmgr, "rf_page_local_repair_guard_begin") : 0, 1);
	expect("JIT_FAILED_OWNER_RECONCILE_SINGLE_DEFINITION", reconcile && function_body(reconcile, "cluster_space_reconcile_reserved_orphan_v1") != NULL, 1);
	expect("JIT_COLD_RETRY_FORMED_STARTUP_REAL_CALL", cold ? count_real_calls(cold, "cluster_cold_retry_run") : 0, 1);
	expect("JIT_LOCAL_REPAIR_NO_SECOND_CALLER", count_tree_symbol(source_root, "rf_page_local_repair_guard_begin", false), 1);
	expect("JIT_FAILED_OWNER_NO_SECOND_OWNER", count_tree_symbol(source_root, "cluster_space_reconcile_reserved_orphan_v1", true), 1);
	expect("JIT_COLD_RETRY_NO_SECOND_CALLER", count_tree_symbol(source_root, "cluster_cold_retry_run", false), 1);
	free(route); free(bufmgr); free(cold); free(reconcile);
}

UT_TEST(test_jit_scalar_generation_is_absent)
{
	char *header = source_code("include/access/xlogrecord.h");
	expect("JIT_NO_SCALAR_GENERATION_HEADER", header ? count_ident(header, "ClusterFailureGenerationToken") + count_ident(header, "failure_generation") : 1, 0);
	free(header);
}

UT_TEST(test_jit_j3_reconcile_is_classification_only)
{
	char *code = source_code("backend/cluster/cluster_space_reconcile.c");
	const char *body = code ? function_body(code, "cluster_space_reconcile_reserved_orphan_v1") : NULL;
	const char *forbidden[] = {"smgropen", "smgrread", "smgrwrite", "smgrimmedsync", "smgrcreate", "PageInit", "cluster_space_allocate_materialized", "reuse", "release", "materialized_hwm"};
	int i;

	expect("JIT_J3_CLASSIFICATION_OWNER_BODY", body != NULL, 1);
	expect("JIT_J3_ORPHAN_BLOCKED_ONLY", body && count_ident(body, "ORPHAN_BLOCKED") > 0, 1);
	for (i = 0; i < (int) (sizeof(forbidden) / sizeof(forbidden[0])); i++)
		expect(forbidden[i], body ? count_ident(body, forbidden[i]) : 0, 0);
	free(code);
}

int
main(void)
{
	if (!valid_root()) {
		fprintf(stderr, "Bail out! JIT_SOURCE_ROOT must be an absolute readable PostgreSQL source root: %s\n", source_root);
		return 2;
	}
	UT_PLAN(5);
	UT_RUN(test_census_ignores_comments_and_literals);
	UT_RUN(test_jit_pageversion_24_byte_abi_and_id251_macros);
	UT_RUN(test_jit_named_owners_and_real_call_sites);
	UT_RUN(test_jit_scalar_generation_is_absent);
	UT_RUN(test_jit_j3_reconcile_is_classification_only);
	UT_DONE();
	return ut_failed_count == 0 ? 0 : 1;
}
