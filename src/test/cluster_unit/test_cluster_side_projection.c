/*-------------------------------------------------------------------------
 *
 * test_cluster_side_projection.c
 *    RF-SIDE D-SIDE-04 focused unit tests: the derived-projection
 *    judgements for CLOG / MULTIXACT / COMMIT_TS.
 *
 *    RED mapping (spec §5.1 U-SIDE-08/09/10 + §2.4):
 *      - verified requires canonical producer + coverage + integrity
 *        (U-SIDE-08: a local bit never overrides TT/redo);
 *      - a miss/UNKNOWN projection lookup FAILS CLOSED — never read the
 *        local ProcArray/CLOG to guess;
 *      - rebuildable: CLOG rebuilds from canonical truth (no redo
 *        retention needed); MULTIXACT/COMMIT_TS need the source redo
 *        retained (U-SIDE-09 missing-not-empty; U-SIDE-10 unknown
 *        timestamp never flips UNKNOWN to COMMITTED);
 *      - every rebuild needs the verified canonical producer.
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "cluster/cluster_side_projection.h"

#include "unit_test.h"

UT_DEFINE_GLOBALS();

void
ExceptionalCondition(const char *conditionName pg_attribute_unused(),
					 const char *fileName pg_attribute_unused(),
					 int lineNumber pg_attribute_unused())
{
	abort();
}

#include <stdio.h>

static ClusterSideProjectionVerifyInput
ut_full_verify(void)
{
	ClusterSideProjectionVerifyInput in;

	memset(&in, 0, sizeof(in));
	in.canonical_truth_ok = true;
	in.coverage_ok = true;
	in.integrity_ok = true;
	return in;
}

UT_TEST(test_projection_verified_conjunction)
{
	ClusterSideProjectionVerifyInput in = ut_full_verify();
	ClusterSideProjectionKind kinds[] = {
		CLUSTER_SIDE_PROJECTION_CLOG,
		CLUSTER_SIDE_PROJECTION_MULTIXACT,
		CLUSTER_SIDE_PROJECTION_COMMIT_TS
	};
	int			i;

	for (i = 0; i < (int) lengthof(kinds); i++) {
		UT_ASSERT(cluster_side_projection_verified(kinds[i], &in));
		in.canonical_truth_ok = false;
		UT_ASSERT(!cluster_side_projection_verified(kinds[i], &in));
		in = ut_full_verify();
		in.coverage_ok = false;
		UT_ASSERT(!cluster_side_projection_verified(kinds[i], &in));
		in = ut_full_verify();
		in.integrity_ok = false;
		UT_ASSERT(!cluster_side_projection_verified(kinds[i], &in));
		in = ut_full_verify();
	}
	UT_ASSERT(!cluster_side_projection_verified(CLUSTER_SIDE_PROJECTION_CLOG, NULL));
	/* Out-of-range kind fails closed. */
	UT_ASSERT(!cluster_side_projection_verified((ClusterSideProjectionKind) 99,
												&in));
}

UT_TEST(test_projection_lookup_fail_closed)
{
	/* A miss/UNKNOWN projection NEVER answers from a local guess
	 * (U-SIDE-08: local bit cannot override TT/redo truth). */
	UT_ASSERT_EQ((int) cluster_side_projection_lookup(false),
				 (int) CLUSTER_SIDE_PROJECTION_LOOKUP_FAIL_CLOSED);
	UT_ASSERT_EQ((int) cluster_side_projection_lookup(true),
				 (int) CLUSTER_SIDE_PROJECTION_LOOKUP_OK);
}

UT_TEST(test_projection_rebuildable_per_kind)
{
	/* CLOG rebuilds from the canonical transaction truth: no redo
	 * retention required. */
	UT_ASSERT(cluster_side_projection_rebuildable(CLUSTER_SIDE_PROJECTION_CLOG,
												  false, true));

	/* MULTIXACT / COMMIT_TS rebuild from the retained redo: the source
	 * must still be retained (U-SIDE-09/10).  L9: a COMMIT_TS projection
	 * loss or unknown timestamp never changes the commit truth — the
	 * rebuild needs the source redo retained and the lookup fails
	 * closed until it is. */
	UT_ASSERT(cluster_side_projection_rebuildable(CLUSTER_SIDE_PROJECTION_MULTIXACT,
												  true, true));
	UT_ASSERT(!cluster_side_projection_rebuildable(CLUSTER_SIDE_PROJECTION_MULTIXACT,
												   false, true));
	UT_ASSERT(cluster_side_projection_rebuildable(CLUSTER_SIDE_PROJECTION_COMMIT_TS,
												  true, true));
	UT_ASSERT(!cluster_side_projection_rebuildable(CLUSTER_SIDE_PROJECTION_COMMIT_TS,
												   false, true));

	/* Every rebuild needs the verified canonical producer — without it,
	 * the rebuild is a guess and the resource stays BLOCKED. */
	UT_ASSERT(!cluster_side_projection_rebuildable(CLUSTER_SIDE_PROJECTION_CLOG,
												   false, false));
	UT_ASSERT(!cluster_side_projection_rebuildable(CLUSTER_SIDE_PROJECTION_MULTIXACT,
												   true, false));
	/* Out-of-range kind fails closed. */
	UT_ASSERT(!cluster_side_projection_rebuildable((ClusterSideProjectionKind) 99,
												   true, true));
}

int
main(void)
{
	UT_PLAN(3);

	UT_RUN(test_projection_verified_conjunction);
	UT_RUN(test_projection_lookup_fail_closed);
	UT_RUN(test_projection_rebuildable_per_kind);

	UT_DONE();
	return ut_failed_count == 0 ? 0 : 1;
}
