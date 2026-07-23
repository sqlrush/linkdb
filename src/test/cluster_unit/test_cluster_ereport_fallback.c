/*-------------------------------------------------------------------------
 *
 * test_cluster_ereport_fallback.c
 *	  Link-only ereport fallbacks for standalone cluster unit tests.
 *
 * Linux/aarch64's server libpgport CRC32C runtime chooser references four
 * backend ereport symbols.  This source is compiled once per symbol and the
 * resulting objects are placed in a test-only archive.  The linker therefore
 * extracts only symbols a standalone test does not already define, preserving
 * every test's local strong stubs.
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"

#if defined(CLUSTER_UNIT_STUB_ERRSTART)

bool
errstart(int elevel, const char *domain pg_attribute_unused())
{
	if (elevel >= ERROR)
		abort();
	return false;
}

#elif defined(CLUSTER_UNIT_STUB_ERRSTART_COLD)

bool
errstart_cold(int elevel, const char *domain pg_attribute_unused())
{
	if (elevel >= ERROR)
		abort();
	return false;
}

#elif defined(CLUSTER_UNIT_STUB_ERRMSG_INTERNAL)

int
errmsg_internal(const char *fmt pg_attribute_unused(), ...)
{
	return 0;
}

#elif defined(CLUSTER_UNIT_STUB_ERRFINISH)

void
errfinish(const char *filename pg_attribute_unused(), int lineno pg_attribute_unused(),
		  const char *funcname pg_attribute_unused())
{}

#else
#error "compile with exactly one CLUSTER_UNIT_STUB_* selector"
#endif
