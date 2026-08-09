/*-------------------------------------------------------------------------
 *
 * test_cluster_jit_abi_contract.c
 *    Compiler-native capability observations for cluster JIT ABIs.
 *
 * IDENTIFICATION
 *    src/test/cluster_unit/test_cluster_jit_abi_contract.c
 *
 * Author: SqlRush <sqlrush@gmail.com>
 *
 * Portions Copyright (c) 1996-2024, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2026, pgrac contributors
 *
 * NOTES
 *    This test includes production declarations.  Missing capability
 *    declarations become executable false observations; compiler or include
 *    failures are classified by check_cluster_jit_proof.py, never here.
 *
 *-------------------------------------------------------------------------
 */

#include "postgres_fe.h"

#include <stddef.h>
#include <stdio.h>

/*
 * The proof driver enables this narrow instrumentation pass.  It observes
 * StaticAssertDecl invocations emitted by production headers through compiler
 * diagnostics; it does not read or tokenize those headers.
 */
#ifdef JIT_PROBE_STATIC_ASSERTS
#define JIT_PRAGMA_INNER(value) _Pragma(#value)
#define JIT_PRAGMA(value) JIT_PRAGMA_INNER(value)
#undef StaticAssertDecl
#define StaticAssertDecl(condition, errmessage) \
	_Static_assert(condition, errmessage); \
	JIT_PRAGMA(message("JIT_STATIC_ASSERT_DECL:" errmessage))
#endif

#include "access/xlogrecord.h"

#ifndef __has_include
#define __has_include(header) 0
#endif

#if __has_include("cluster/cluster_rf_route.h")
#include "cluster/cluster_rf_route.h"
#endif

#if __has_include("cluster/cluster_recovery_cold_retry.h")
#include "cluster/cluster_recovery_cold_retry.h"
#endif

/* Direct compiler bindings use the PostgreSQL portability spelling. */
#ifdef JIT_PROBE_STATIC_ASSERTS
#undef StaticAssertDecl
#define StaticAssertDecl(condition, errmessage) _Static_assert(condition, errmessage)
#endif

#if defined(PGRAC_JIT_PAGE_VERSION_EDGE_ABI_V1)
StaticAssertDecl(sizeof(RfPageVersionV1) == 24,
				 "JIT-A-T2-PAGEVERSION-SIZE-24");
StaticAssertDecl(offsetof(RfPageVersionV1, mutation_token) == 16,
				 "JIT-A-T2-PAGEVERSION-TOKEN-OFFSET-16");
StaticAssertDecl(XLR_BLOCK_ID_PAGE_VERSION_EDGE == 251,
				 "JIT-A-T2-ID251-VALUE-251");
StaticAssertDecl(XLR_PAGE_VERSION_EDGE_FORMAT_V1 == 1,
				 "JIT-A-T2-ID251-FORMAT-1");
StaticAssertDecl(XLR_PAGE_VERSION_EDGE_HEADER_SIZE == 16,
				 "JIT-A-T2-ID251-HEADER-16");
StaticAssertDecl(XLR_PAGE_VERSION_EDGE_ENTRY_SIZE == 48,
				 "JIT-A-T2-ID251-ENTRY-48");
StaticAssertDecl(XLR_PAGE_VERSION_EDGE_MAX_ENTRIES == 33,
				 "JIT-A-T2-ID251-MAX-33");
StaticAssertDecl(XLR_PAGE_VERSION_EDGE_MAX_SIZE == 1600,
				 "JIT-A-T2-ID251-MAX-1600");
#define JIT_OBS_PAGE_VERSION 1
#define JIT_OBS_ID251 1
#else
#define JIT_OBS_PAGE_VERSION 0
#define JIT_OBS_ID251 0
#endif

#if defined(PGRAC_JIT_SCALAR_GENERATION_REMOVED_V1)
#define JIT_OBS_SCALAR_GENERATION_ABSENT 1
#else
#define JIT_OBS_SCALAR_GENERATION_ABSENT 0
#endif

#if defined(PGRAC_JIT_RF_ROUTE_ABI_V1)
StaticAssertDecl(sizeof(RfOpcodeRouteV1) == 8,
				 "JIT-A-T2-ROUTE-ABI-8");
#define JIT_OBS_ROUTE_ABI 1
#else
#define JIT_OBS_ROUTE_ABI 0
#endif

#if defined(PGRAC_JIT_COLD_RETRY_ABI_V1)
StaticAssertDecl(CLUSTER_COLD_RETRY_POLL_MS == INT32_C(100),
				 "JIT-A-T2-COLD-POLL-100");
#define JIT_OBS_COLD_POLL 1
#else
#define JIT_OBS_COLD_POLL 0
#endif

static int
observe(const char *proof_id, int affirmative, const char *expected)
{
	printf("JIT_OBSERVATION\t%s\t%s\t%d\t%s\n",
		   proof_id, affirmative ? "PASS" : "FAIL", affirmative, expected);
	return affirmative ? 0 : 1;
}

int
main(void)
{
	int			failures = 0;

	puts("JIT_CONTROL\tHEADER-COMPILE-LINK-RUN\tPASS");
	failures += observe("JIT-A-T2-PAGEVERSION-TYPE",
					JIT_OBS_PAGE_VERSION, "declared");
	failures += observe("JIT-A-T2-PAGEVERSION-SIZE-24",
					JIT_OBS_PAGE_VERSION, "24");
	failures += observe("JIT-A-T2-PAGEVERSION-TOKEN-OFFSET-16",
					JIT_OBS_PAGE_VERSION, "16");
	failures += observe("JIT-A-T2-ID251-VALUE-251",
					JIT_OBS_ID251, "251");
	failures += observe("JIT-A-T2-ID251-HEADER-16",
					JIT_OBS_ID251, "16");
	failures += observe("JIT-A-T2-ID251-ENTRY-48",
					JIT_OBS_ID251, "48");
	failures += observe("JIT-A-T2-ID251-MAX-33-1600",
					JIT_OBS_ID251, "33/1600");
	failures += observe("JIT-A-T2-SCALAR-GENERATION-ABSENT",
					JIT_OBS_SCALAR_GENERATION_ABSENT, "absent");
	failures += observe("JIT-A-T2-ROUTE-ABI-8",
					JIT_OBS_ROUTE_ABI, "8");
	failures += observe("JIT-A-T2-COLD-POLL-100",
					JIT_OBS_COLD_POLL, "100");

	return failures == 0 ? 0 : 1;
}
