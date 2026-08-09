/* Stage 8 JIT source-contract RED: fail-closed structural source census. */
#include <dirent.h>
#include <errno.h>
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

typedef struct Scope { const char *begin; const char *end; } Scope;
static const char source_root[] = JIT_SOURCE_ROOT;
static bool census_failed;
static char census_detail[PATH_MAX + 128];

static void
census_fail(const char *what, const char *path)
{
	if (!census_failed)
		snprintf(census_detail, sizeof(census_detail), "%s: %s", what, path ? path : "(null)");
	census_failed = true;
}

static bool
join_path(char *dst, size_t cap, const char *base, const char *name)
{
	int n = snprintf(dst, cap, "%s/%s", base, name);
	if (n < 0 || n >= (int) cap) { census_fail("path truncated", base); return false; }
	return true;
}

static char *
read_required(const char *path)
{
	FILE *f = fopen(path, "rb");
	long n;
	char *s;
	if (f == NULL) { census_fail("open failed", path); return NULL; }
	if (fseek(f, 0, SEEK_END) != 0 || (n = ftell(f)) < 0 || fseek(f, 0, SEEK_SET) != 0) {
		fclose(f); census_fail("seek failed", path); return NULL;
	}
	s = malloc((size_t) n + 1);
	if (s == NULL) { fclose(f); census_fail("allocation failed", path); return NULL; }
	{
		size_t got = fread(s, 1, (size_t) n, f);
		int close_rc = fclose(f);
		if (got != (size_t) n || close_rc != 0) {
			free(s); census_fail("read failed", path); return NULL;
		}
	}
	s[n] = '\0';
	return s;
}

/* Preserve offsets/newlines; erase comments and string/character literals. */
static char *
code_only(const char *raw, const char *label)
{
	char *out = malloc(strlen(raw) + 1);
	size_t i = 0;
	if (out == NULL) { census_fail("allocation failed", label); return NULL; }
	strcpy(out, raw);
	while (raw[i] != '\0') {
		if (raw[i] == '/' && raw[i + 1] == '/') {
			while (raw[i] && raw[i] != '\n') out[i++] = ' ';
		} else if (raw[i] == '/' && raw[i + 1] == '*') {
			out[i++] = ' '; out[i++] = ' ';
			while (raw[i] && !(raw[i] == '*' && raw[i + 1] == '/')) {
				if (raw[i] != '\n') out[i] = ' ';
				i++;
			}
			if (!raw[i]) { free(out); census_fail("unterminated comment", label); return NULL; }
			out[i++] = ' '; out[i++] = ' ';
		} else if (raw[i] == '"' || raw[i] == '\'') {
			char quote = raw[i]; out[i++] = ' ';
			while (raw[i] && raw[i] != quote) {
				if (raw[i] == '\\' && raw[i + 1]) { out[i++] = ' '; out[i++] = ' '; }
				else { if (raw[i] != '\n') out[i] = ' '; i++; }
			}
			if (!raw[i]) { free(out); census_fail("unterminated literal", label); return NULL; }
			out[i++] = ' ';
		} else i++;
	}
	return out;
}

/* Preserve literals for #include parsing, but erase every comment. */
static char *
comments_only(const char *raw, const char *label)
{
	char *out = malloc(strlen(raw) + 1);
	size_t i = 0;
	if (out == NULL) { census_fail("allocation failed", label); return NULL; }
	strcpy(out, raw);
	while (raw[i] != '\0') {
		if (raw[i] == '/' && raw[i + 1] == '/') {
			while (raw[i] && raw[i] != '\n') out[i++] = ' ';
		} else if (raw[i] == '/' && raw[i + 1] == '*') {
			out[i++] = ' '; out[i++] = ' ';
			while (raw[i] && !(raw[i] == '*' && raw[i + 1] == '/')) {
				if (raw[i] != '\n') out[i] = ' ';
				i++;
			}
			if (!raw[i]) { free(out); census_fail("unterminated comment", label); return NULL; }
			out[i++] = ' '; out[i++] = ' ';
		} else if (raw[i] == '"' || raw[i] == '\'') {
			char quote = raw[i++];
			while (raw[i] && raw[i] != quote) {
				if (raw[i] == '\\' && raw[i + 1]) i += 2;
				else i++;
			}
			if (!raw[i]) { free(out); census_fail("unterminated literal", label); return NULL; }
			i++;
		} else i++;
	}
	return out;
}

static bool isid(int c) { return c == '_' || (c >= 'A' && c <= 'Z') || (c >= 'a' && c <= 'z') || (c >= '0' && c <= '9'); }
static const char *ws(const char *p, const char *end) { while (p < end && (*p == ' ' || *p == '\t' || *p == '\r' || *p == '\n')) p++; return p; }
static bool
token(const char *base, const char *p, const char *end, const char *want)
{
	size_t n = strlen(want);
	return p + n <= end && strncmp(p, want, n) == 0 &&
		(!isid((unsigned char) want[0]) || p == base || !isid((unsigned char) p[-1])) &&
		(!isid((unsigned char) want[n - 1]) || p + n == end || !isid((unsigned char) p[n]));
}

static const char *
find_token(const char *base, const char *p, const char *end, const char *name)
{
	while (p < end && (p = strstr(p, name)) != NULL && p < end) {
		if (token(base, p, end, name)) return p;
		p++;
	}
	return NULL;
}

static const char *
match_pair(const char *p, const char *end, char open, char close)
{
	int depth = 0;
	for (; p < end; p++) {
		if (*p == open) depth++;
		else if (*p == close && --depth == 0) return p;
	}
	return NULL;
}

static bool
preprocessor_line(const char *base, const char *p)
{
	const char *line = p;
	while (line > base && line[-1] != '\n') line--;
	while (line < p && (*line == ' ' || *line == '\t' || *line == '\r')) line++;
	return line < p && *line == '#';
}

static bool
top_level_at(const char *base, const char *p)
{
	int depth = 0;
	for (; base < p; base++) {
		if (*base == '{') depth++;
		else if (*base == '}') depth--;
		if (depth < 0) return false;
	}
	return depth == 0;
}

static bool
named_struct(const char *code, const char *name, Scope *body)
{
	const char *end = code + strlen(code), *p = code;
	while ((p = find_token(code, p, end, "typedef")) != NULL) {
		p = ws(p + 7, end); if (!token(code, p, end, "struct")) { p++; continue; }
		p = ws(p + 6, end); if (!token(code, p, end, name)) { p++; continue; }
		p = ws(p + strlen(name), end); if (*p != '{') { p++; continue; }
		body->begin = p + 1; body->end = match_pair(p, end, '{', '}');
		if (body->end == NULL) { census_fail("unmatched struct brace", name); return false; }
		p = ws(body->end + 1, end);
		if (!token(code, p, end, name) || *ws(p + strlen(name), end) != ';') return false;
		return true;
	}
	return false;
}

static int scope_tokens(Scope s, const char *name) { int n = 0; const char *p = s.begin; while ((p = find_token(s.begin, p, s.end, name)) != NULL) { n++; p += strlen(name); } return n; }

static bool
scope_sequence(Scope s, const char **items, int nitems)
{
	const char *start = s.begin;
	while ((start = find_token(s.begin, start, s.end, items[0])) != NULL) {
		const char *p = start; int i;
		for (i = 0; i < nitems; i++) {
			p = ws(p, s.end);
			if (!token(s.begin, p, s.end, items[i])) break;
			p += strlen(items[i]);
		}
		if (i == nitems) return true;
		start++;
	}
	return false;
}

static bool
function_scope(const char *code, const char *name, Scope *body)
{
	const char *end = code + strlen(code), *p = code;
	while ((p = find_token(code, p, end, name)) != NULL) {
		const char *q = ws(p + strlen(name), end), *close;
		if (!top_level_at(code, p) || preprocessor_line(code, p) || *q != '(' ||
			(close = match_pair(q, end, '(', ')')) == NULL) { p++; continue; }
		q = ws(close + 1, end);
		if (*q != '{') { p++; continue; }
		body->begin = q + 1; body->end = match_pair(q, end, '{', '}');
		if (body->end == NULL) census_fail("unmatched function brace", name);
		return body->end != NULL;
	}
	return false;
}

static int
function_definitions(const char *code, const char *name)
{
	const char *end = code + strlen(code), *p = code;
	int n = 0;
	while ((p = find_token(code, p, end, name)) != NULL) {
		const char *q = ws(p + strlen(name), end), *close, *body_end;
		if (!top_level_at(code, p) || preprocessor_line(code, p) || *q != '(' ||
			(close = match_pair(q, end, '(', ')')) == NULL || *(q = ws(close + 1, end)) != '{') {
			p++;
			continue;
		}
		body_end = match_pair(q, end, '{', '}');
		if (body_end == NULL) { census_fail("unmatched function brace", name); return n; }
		n++;
		p = body_end + 1;
	}
	return n;
}

static bool
declaration_before_call(Scope s, const char *name_at, const char *close)
{
	const char *p = name_at, *start, *after = ws(close + 1, s.end);
	if (after >= s.end || *after != ';') return false;
	while (p > s.begin && p[-1] != ';' && p[-1] != '{' && p[-1] != '}' && p[-1] != ':') p--;
	start = ws(p, name_at);
	if (start == name_at) return false;
	for (p = start; p < name_at; p++) {
		if (*p == '=' || *p == '(' || *p == ')' || *p == '.' || *p == '!' ||
			*p == '&' || *p == '|' || *p == '+' || *p == '-' || *p == '?' || *p == ',')
			return false;
		if (!(isid((unsigned char) *p) || *p == '*' || *p == ' ' || *p == '\t' || *p == '\r' || *p == '\n'))
			return false;
	}
	return true;
}

static int
calls_in(Scope s, const char *name)
{
	const char *p = s.begin; int n = 0;
	while ((p = find_token(s.begin, p, s.end, name)) != NULL) {
		const char *q = ws(p + strlen(name), s.end), *close;
		if (*q == '(' && (close = match_pair(q, s.end, '(', ')')) != NULL &&
			!preprocessor_line(s.begin, p) && !declaration_before_call(s, p, close)) n++;
		p += strlen(name);
	}
	return n;
}

static int
active_include_count(const char *raw, const char *name, const char *label)
{
	char *text = comments_only(raw, label);
	const char *line;
	int n = 0;
	if (text == NULL) return 0;
	for (line = text; *line != '\0';) {
		const char *line_end = strchr(line, '\n'), *p, *operand, *close, *base;
		if (line_end == NULL) line_end = text + strlen(text);
		p = line; while (p < line_end && (*p == ' ' || *p == '\t' || *p == '\r')) p++;
		if (p < line_end && *p++ == '#') {
			while (p < line_end && (*p == ' ' || *p == '\t' || *p == '\r')) p++;
			if (token(text, p, line_end, "include")) {
				p += 7; while (p < line_end && (*p == ' ' || *p == '\t' || *p == '\r')) p++;
				if (p < line_end && (*p == '"' || *p == '<')) {
					char terminator = *p == '"' ? '"' : '>';
					operand = ++p; close = memchr(operand, terminator, (size_t)(line_end - operand));
					if (close != NULL) {
						base = close;
						while (base > operand && base[-1] != '/') base--;
						if ((size_t)(close - base) == strlen(name) && !strncmp(base, name, strlen(name))) n++;
					}
				}
			}
		}
		line = *line_end == '\n' ? line_end + 1 : line_end;
	}
	free(text);
	return n;
}

static int
exact_numeric_define(const char *code, const char *name, const char *number)
{
	const char *p = code, *end = code + strlen(code); int n = 0;
	while ((p = strchr(p, '#')) != NULL) {
		const char *line_end = strchr(p, '\n'); const char *q;
		if (line_end == NULL) line_end = end;
		q = ws(p + 1, line_end);
		if (token(code, q, line_end, "define")) {
			q = ws(q + 6, line_end);
			if (token(code, q, line_end, name)) {
				q = ws(q + strlen(name), line_end);
				if (token(code, q, line_end, number) && ws(q + strlen(number), line_end) == line_end) n++;
			}
		}
		p = line_end;
	}
	return n;
}

static int
exact_cold_define(const char *code)
{
	const char *p = code, *end = code + strlen(code);
	int n = 0;
	while ((p = strchr(p, '#')) != NULL) {
		const char *line_end = strchr(p, '\n'), *q;
		if (line_end == NULL) line_end = end;
		q = ws(p + 1, line_end);
		if (token(code, q, line_end, "define")) {
			q = ws(q + 6, line_end);
			if (token(code, q, line_end, "CLUSTER_COLD_RETRY_POLL_MS")) {
				q = ws(q + strlen("CLUSTER_COLD_RETRY_POLL_MS"), line_end);
				if (token(code, q, line_end, "INT32_C") && *(q = ws(q + 7, line_end)) == '(') {
					q = ws(q + 1, line_end);
					if (token(code, q, line_end, "100") && *(q = ws(q + 3, line_end)) == ')' &&
						ws(q + 1, line_end) == line_end) n++;
				}
			}
		}
		p = line_end;
	}
	return n;
}

static char *
load_code(const char *relative, char **raw_out)
{
	char path[PATH_MAX]; char *raw, *code;
	if (!join_path(path, sizeof(path), source_root, relative)) return NULL;
	raw = read_required(path); if (raw == NULL) return NULL;
	code = code_only(raw, path); if (code == NULL) { free(raw); return NULL; }
	if (raw_out) *raw_out = raw; else free(raw);
	return code;
}

/* A not-yet-created contract owner is a named semantic RED, not census I/O loss. */
static char *
load_planned_code(const char *relative)
{
	char path[PATH_MAX]; struct stat st;
	if (!join_path(path, sizeof(path), source_root, relative)) return NULL;
	if (stat(path, &st) != 0) {
		if (errno == ENOENT) return NULL;
		census_fail("stat failed", path); return NULL;
	}
	if (!S_ISREG(st.st_mode)) { census_fail("planned owner is not regular", path); return NULL; }
	return load_code(relative, NULL);
}

typedef enum TreeMode { TREE_IDENT, TREE_DEFINITION, TREE_CALL, TREE_INCLUDE } TreeMode;
static int
tree_count(const char *path, const char *name, const char *suffix, TreeMode mode)
{
	DIR *dir = opendir(path); struct dirent *e; int total = 0;
	if (dir == NULL) { census_fail("opendir failed", path); return 0; }
	for (;;) {
		char child[PATH_MAX]; struct stat st;
		errno = 0;
		e = readdir(dir);
		if (e == NULL) {
			if (errno != 0) census_fail("readdir failed", path);
			break;
		}
		if (!strcmp(e->d_name, ".") || !strcmp(e->d_name, "..")) continue;
		if (!join_path(child, sizeof(child), path, e->d_name)) break;
		if (stat(child, &st) != 0) { census_fail("stat failed", child); break; }
		if (S_ISDIR(st.st_mode)) total += tree_count(child, name, suffix, mode);
		else if (S_ISREG(st.st_mode) && strlen(e->d_name) >= strlen(suffix) && !strcmp(e->d_name + strlen(e->d_name) - strlen(suffix), suffix)) {
			char *raw = read_required(child), *code; Scope body;
			if (raw == NULL) break;
			if (mode == TREE_INCLUDE) {
				total += active_include_count(raw, name, child);
				free(raw);
				if (census_failed) break;
				continue;
			}
			code = code_only(raw, child); free(raw); if (code == NULL) break;
			if (mode == TREE_IDENT) { Scope all = {code, code + strlen(code)}; total += scope_tokens(all, name); }
			else if (mode == TREE_DEFINITION) total += function_definitions(code, name);
			else { body.begin = code; body.end = code + strlen(code); total += calls_in(body, name) - function_definitions(code, name); }
			free(code);
		}
		if (census_failed) break;
	}
	if (closedir(dir) != 0) census_fail("closedir failed", path);
	return total;
}

static int
tree_relative_count(const char *relative, const char *name, const char *suffix, TreeMode mode)
{
	char path[PATH_MAX];
	if (!join_path(path, sizeof(path), source_root, relative)) return 0;
	return tree_count(path, name, suffix, mode);
}

static int
production_c_count(const char *name, TreeMode mode)
{
	static const char *const roots[] = {
		"backend", "bin", "common", "fe_utils", "interfaces", "pl", "port", "timezone"
	};
	int total = 0;
	for (size_t i = 0; i < sizeof(roots) / sizeof(roots[0]); i++) {
		total += tree_relative_count(roots[i], name, ".c", mode);
		if (census_failed) break;
	}
	return total;
}

static bool
identifier_has_prefix(const char *id, size_t n, const char *prefix)
{
	size_t want = strlen(prefix);
	return n >= want && !strncmp(id, prefix, want) && (n == want || id[want] == '_');
}

static bool
identifier_starts(const char *id, size_t n, const char *prefix)
{
	size_t want = strlen(prefix);
	return n >= want && !strncmp(id, prefix, want);
}

static bool
identifier_contains(const char *id, size_t n, const char *part)
{
	size_t want = strlen(part), i;
	if (n < want) return false;
	for (i = 0; i + want <= n; i++)
		if (!strncmp(id + i, part, want)) return true;
	return false;
}

static bool
assignment_after(const char *p, const char *end)
{
	if (p >= end) return false;
	if (*p == '=') return p + 1 == end || p[1] != '=';
	if (p + 1 >= end) return false;
	return ((strchr("+-*/%&|^", *p) != NULL && p[1] == '=') ||
		(p[0] == '+' && p[1] == '+') || (p[0] == '-' && p[1] == '-'));
}

static bool
prefix_update_before(Scope body, const char *id)
{
	const char *p = id, *start;
	while (p > body.begin && p[-1] != ';' && p[-1] != '{' && p[-1] != '}') p--;
	start = p;
	for (p = start; p + 1 < id; p++)
		if ((p[0] == '+' && p[1] == '+') || (p[0] == '-' && p[1] == '-')) return true;
	return false;
}

static int
forbidden_j3_effects(Scope body)
{
	static const char *const exact[] = {
		"RelationGetSmgr", "RelationCloseSmgr", "PageInit", "cluster_hw_apply_hwm",
		"cluster_space_truncate_recreate",
		"cluster_space_tombstone_for_drop"
	};
	const char *p = body.begin;
	int bad = 0;
	while (p < body.end) {
		const char *id, *after, *before;
		size_t n;
		if (!((*p == '_') || (*p >= 'A' && *p <= 'Z') || (*p >= 'a' && *p <= 'z'))) { p++; continue; }
		id = p++; while (p < body.end && isid((unsigned char) *p)) p++;
		n = (size_t)(p - id);
		for (size_t i = 0; i < sizeof(exact) / sizeof(exact[0]); i++)
			if (n == strlen(exact[i]) && !strncmp(id, exact[i], n)) bad++;
		if (identifier_starts(id, n, "smgropen") ||
			identifier_starts(id, n, "smgrcreate") ||
			identifier_starts(id, n, "smgrread") ||
			identifier_starts(id, n, "smgrwrite") ||
			identifier_starts(id, n, "smgrextend") ||
			identifier_starts(id, n, "smgrzeroextend") ||
			identifier_starts(id, n, "smgrnblocks") ||
			identifier_starts(id, n, "smgrtruncate") ||
			identifier_starts(id, n, "smgrimmedsync") ||
			identifier_starts(id, n, "smgrsync") ||
			identifier_starts(id, n, "smgrdosync") ||
			identifier_starts(id, n, "smgrrelease") ||
			identifier_starts(id, n, "smgrprefetch") ||
			identifier_starts(id, n, "mdopen") ||
			identifier_starts(id, n, "mdcreate") ||
			identifier_starts(id, n, "mdread") ||
			identifier_starts(id, n, "mdwrite") ||
			identifier_starts(id, n, "mdextend") ||
			identifier_starts(id, n, "mdzeroextend") ||
			identifier_starts(id, n, "mdnblocks") ||
			identifier_starts(id, n, "mdtruncate") ||
			identifier_starts(id, n, "mdimmedsync") ||
			identifier_starts(id, n, "mdsync") ||
			identifier_starts(id, n, "mdprefetch") ||
			identifier_has_prefix(id, n, "cluster_smgr_open") ||
			identifier_has_prefix(id, n, "cluster_smgr_create") ||
			identifier_has_prefix(id, n, "cluster_smgr_read") ||
			identifier_has_prefix(id, n, "cluster_smgr_write") ||
			identifier_has_prefix(id, n, "cluster_smgr_extend") ||
			identifier_has_prefix(id, n, "cluster_smgr_zeroextend") ||
			identifier_has_prefix(id, n, "cluster_smgr_truncate") ||
			identifier_has_prefix(id, n, "cluster_smgr_sync") ||
			identifier_has_prefix(id, n, "cluster_space_allocate_materialized") ||
			identifier_has_prefix(id, n, "cluster_space_materialize") ||
			identifier_has_prefix(id, n, "cluster_space_reuse") ||
			identifier_has_prefix(id, n, "cluster_space_release_extent") ||
			identifier_has_prefix(id, n, "cluster_space_physical_guard") ||
			identifier_has_prefix(id, n, "cluster_space_advance_materialized_hwm") ||
			identifier_has_prefix(id, n, "cluster_space_set_materialized_hwm")) bad++;
		if (identifier_contains(id, n, "materialized_hwm")) {
			bool member;
			before = id;
			while (before > body.begin && (before[-1] == ' ' || before[-1] == '\t' || before[-1] == '\r' || before[-1] == '\n')) before--;
			member = before > body.begin && before[-1] == '.';
			if (before - body.begin >= 2 && before[-2] == '-' && before[-1] == '>') member = true;
			after = ws(p, body.end);
			if (member && (assignment_after(after, body.end) || prefix_update_before(body, id))) bad++;
		}
	}
	return bad;
}

static void expect(const char *name, int got, int want) { if (got != want) { printf("# %s: expected %d, found %d\n", name, want, got); ut_current_failed = 1; } }

UT_TEST(test_checker_mutations)
{
	const char *raw = "/* #include \"cluster_rf_route_manifest.def\" */\n#include <cluster/cluster_rf_route_manifest.def>\n#include \"cluster_rf_route_manifest.def.extra\"\n#define X 2510\n\"f()\"\nvoid g(void){extern void f(void); f();}\nvoid d(void){} void d(void){}\n";
	const char *j3raw = "void h(void){x=s.materialized_hwm; smgrreadv(); cluster_smgr_read_target(); cluster_space_materialize_reserved_v1(); s.materialized_hwm++; cluster_space_release_extent();}\n";
	char *code = code_only(raw, "fixture"), *j3 = code_only(j3raw, "j3 fixture"); Scope g, h, all;
	expect("CHECKER_REJECTS_NUMERIC_PREFIX", exact_numeric_define(code, "X", "251"), 0);
	expect("CHECKER_IGNORES_COMMENT_LITERAL", function_scope(code, "g", &g) ? calls_in(g, "f") : 0, 1);
	expect("CHECKER_EXACT_ACTIVE_INCLUDE", active_include_count(raw, "cluster_rf_route_manifest.def", "fixture"), 1);
	expect("CHECKER_COUNTS_SAME_FILE_DEFINITIONS", function_definitions(code, "d"), 2);
	all.begin = code; all.end = code + strlen(code);
	expect("CHECKER_EXCLUDES_PROTOTYPE", calls_in(all, "f"), 1);
	expect("CHECKER_REJECTS_COLD_PREFIX", exact_cold_define("#define CLUSTER_COLD_RETRY_POLL_MS INT32_C(1000)\n"), 0);
	expect("CHECKER_ACCEPTS_EXACT_COLD", exact_cold_define("#define CLUSTER_COLD_RETRY_POLL_MS INT32_C( 100 )\n"), 1);
	expect("CHECKER_J3_READ_ALLOWED_MUTATIONS_CAUGHT", function_scope(j3, "h", &h) ? forbidden_j3_effects(h) : 0, 5);
	free(code); free(j3);
}

UT_TEST(test_pageversion_and_id251)
{
	char *code = load_code("include/access/xlogrecord.h", NULL); Scope s = {0}, all = {0}; int exact_assert = 0;
	const char *field1[] = {"uint8", "segment_incarnation", "[", "16", "]", ";"};
	const char *field2[] = {"uint64", "mutation_token", ";"};
	const char *assertion[] = {"_Static_assert", "(", "sizeof", "(", "RfPageVersionV1", ")", "==", "24", ",", ")", ";"};
	if (code) {
		all.begin = code; all.end = code + strlen(code);
		exact_assert = scope_sequence(all, assertion, 11);
	}
	expect("JIT_PAGEVERSION_NAMED_MATCHED_STRUCT", code && named_struct(code, "RfPageVersionV1", &s), 1);
	expect("JIT_PAGEVERSION_EXACT_FIELDS", s.begin && scope_sequence(s, field1, 6) && scope_tokens(s, "segment_incarnation") == 1 && scope_sequence(s, field2, 3) && scope_tokens(s, "mutation_token") == 1, 1);
	expect("JIT_PAGEVERSION_EXACT_STATIC_ASSERT", exact_assert, 1);
	expect("JIT_ID251_EXACT_48", code ? exact_numeric_define(code, "XLR_PAGE_VERSION_EDGE_ENTRY_SIZE", "48") : 0, 1);
	expect("JIT_ID251_EXACT_1600", code ? exact_numeric_define(code, "XLR_PAGE_VERSION_EDGE_MAX_SIZE", "1600") : 0, 1);
	free(code);
}

UT_TEST(test_global_owners_and_scalar_zero)
{
	char *buf = load_code("backend/storage/buffer/bufmgr.c", NULL); char *xlog = load_code("backend/access/transam/xlogrecovery.c", NULL); char *cold = load_planned_code("include/cluster/cluster_recovery_cold_retry.h"); Scope caller;
	expect("JIT_ROUTE_SOLE_GENERATED_INCLUDE", production_c_count("cluster_rf_route_manifest.def", TREE_INCLUDE), 1);
	expect("JIT_LOCAL_REPAIR_READBUFFER_CALL", buf && function_scope(buf, "ReadBuffer_common", &caller) ? calls_in(caller, "rf_page_local_repair_guard_begin") : 0, 1);
	expect("JIT_LOCAL_REPAIR_SINGLE_DEFINITION", tree_relative_count("backend", "rf_page_local_repair_guard_begin", ".c", TREE_DEFINITION), 1);
	expect("JIT_LOCAL_REPAIR_SOLE_CALLER", tree_relative_count("backend", "rf_page_local_repair_guard_begin", ".c", TREE_CALL), 1);
	expect("JIT_COLD_RETRY_STARTUP_CALL", xlog && function_scope(xlog, "StartupXLOG", &caller) ? calls_in(caller, "cluster_cold_retry_run") : 0, 1);
	expect("JIT_COLD_RETRY_SINGLE_DEFINITION", tree_relative_count("backend", "cluster_cold_retry_run", ".c", TREE_DEFINITION), 1);
	expect("JIT_COLD_RETRY_SOLE_CALLER", tree_relative_count("backend", "cluster_cold_retry_run", ".c", TREE_CALL), 1);
	expect("JIT_COLD_RETRY_EXACT_100MS", cold ? exact_cold_define(cold) : 0, 1);
	expect("JIT_COLD_RETRY_SINGLE_POLL_CONSTANT", tree_relative_count("backend", "CLUSTER_COLD_RETRY_POLL_MS", ".c", TREE_IDENT) + tree_relative_count("include", "CLUSTER_COLD_RETRY_POLL_MS", ".h", TREE_IDENT), 1);
	expect("JIT_SCALAR_GENERATION_ZERO", tree_relative_count("backend", "ClusterFailureGenerationToken", ".c", TREE_IDENT) + tree_relative_count("backend", "ClusterFailureGenerationToken", ".h", TREE_IDENT) + tree_relative_count("include", "ClusterFailureGenerationToken", ".c", TREE_IDENT) + tree_relative_count("include", "ClusterFailureGenerationToken", ".h", TREE_IDENT) + tree_relative_count("backend", "failure_generation", ".c", TREE_IDENT) + tree_relative_count("backend", "failure_generation", ".h", TREE_IDENT) + tree_relative_count("include", "failure_generation", ".c", TREE_IDENT) + tree_relative_count("include", "failure_generation", ".h", TREE_IDENT), 0);
	free(buf); free(xlog); free(cold);
}

UT_TEST(test_j3_classification_only)
{
	char *code = load_planned_code("backend/cluster/cluster_space_reconcile.c"); Scope body = {0};
	expect("JIT_J3_SINGLE_DEFINITION", code && function_scope(code, "cluster_space_reconcile_reserved_orphan_v1", &body), 1);
	expect("JIT_J3_NO_SECOND_DEFINITION", tree_relative_count("backend", "cluster_space_reconcile_reserved_orphan_v1", ".c", TREE_DEFINITION), 1);
	expect("JIT_J3_ORPHAN_BLOCKED_RESULT", body.begin && scope_tokens(body, "CLUSTER_SPACE_RESERVED_ACTION_MARK_ORPHAN_BLOCKED") > 0, 1);
	expect("JIT_J3_ZERO_TARGET_OR_REUSE_EFFECTS", code ? forbidden_j3_effects(body) : 0, 0);
	free(code);
}

int
main(void)
{
	struct stat st; char required[PATH_MAX]; char *probe;
	if (source_root[0] != '/' || stat(source_root, &st) != 0 || !S_ISDIR(st.st_mode) || !join_path(required, sizeof(required), source_root, "include/access/xlogrecord.h") || (probe = read_required(required)) == NULL) {
		fprintf(stderr, "Bail out! invalid JIT_SOURCE_ROOT: %s (%s)\n", source_root, census_detail); return 2;
	}
	free(probe);
	UT_PLAN(4); UT_RUN(test_checker_mutations); UT_RUN(test_pageversion_and_id251); UT_RUN(test_global_owners_and_scalar_zero); UT_RUN(test_j3_classification_only); UT_DONE();
	if (census_failed) { fprintf(stderr, "Bail out! incomplete source census: %s\n", census_detail); return 2; }
	return ut_failed_count ? 1 : 0;
}
