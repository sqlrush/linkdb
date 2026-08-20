/*-------------------------------------------------------------------------
 *
 * test_cluster_page_handoff.c
 *    RF-PAGE PGDEL-07 focused unit tests: the §7.4 FND-10 per-resource
 *    handoff judgement and the PL-12 retention denial.
 *
 *    RED mapping (spec §10.2 / §7.4 / §6.4):
 *      - ready requires the complete typed page proof AND the RF-SIDE
 *        proof AND the pinned retention interval AND zero consumers;
 *      - PL-12: a retire request before the PAGE proof exists is denied;
 *      - retaining redo alone is never the correctness closure (§7.4);
 *      - the judgement is per-resource: no global barrier state exists
 *        in this API (§6.4).
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "cluster/cluster_page_handoff.h"

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

static ClusterPageProof ut_proof;

static ClusterPageHandoffInput
ut_full_input(void)
{
	ClusterPageHandoffInput in;

	memset(&in, 0, sizeof(in));
	in.proof = &ut_proof;
	in.side_proof_ok = true;
	in.retention_pinned = true;
	in.consumers_zero = true;
	return in;
}

UT_TEST(test_fnd10_conjunction)
{
	ClusterPageHandoffInput in;

	/* The complete proof + SIDE proof + pinned interval + zero consumers
	 * -> ready. */
	memset(&ut_proof, 0, sizeof(ut_proof));
	ut_proof.contributor_coverage = true;
	ut_proof.durability_barrier_ok = true;
	ut_proof.post_read_ok = true;
	ut_proof.authority_revalidated = true;
	in = ut_full_input();
	UT_ASSERT(cluster_page_handoff_ready(&in));

	/* Every proof element is required. */
	ut_proof.contributor_coverage = false;
	UT_ASSERT(!cluster_page_handoff_ready(&in));
	ut_proof.contributor_coverage = true;
	ut_proof.durability_barrier_ok = false;
	UT_ASSERT(!cluster_page_handoff_ready(&in));
	ut_proof.durability_barrier_ok = true;
	ut_proof.post_read_ok = false;
	UT_ASSERT(!cluster_page_handoff_ready(&in));
	ut_proof.post_read_ok = true;
	ut_proof.authority_revalidated = false;
	UT_ASSERT(!cluster_page_handoff_ready(&in));
	ut_proof.authority_revalidated = true;

	/* SIDE proof / retention / consumers each required. */
	in = ut_full_input();
	in.side_proof_ok = false;
	UT_ASSERT(!cluster_page_handoff_ready(&in));
	in = ut_full_input();
	in.retention_pinned = false;
	UT_ASSERT(!cluster_page_handoff_ready(&in));
	in = ut_full_input();
	in.consumers_zero = false;
	UT_ASSERT(!cluster_page_handoff_ready(&in));

	/* §7.4: retaining the redo alone is never the closure. */
	in = ut_full_input();
	in.proof = NULL;
	UT_ASSERT(!cluster_page_handoff_ready(&in));
	UT_ASSERT(!cluster_page_handoff_ready(NULL));
}

UT_TEST(test_pl12_retention_denied_before_proof)
{
	ClusterPageHandoffInput in;

	memset(&ut_proof, 0, sizeof(ut_proof)); /* no page proof at all */
	in = ut_full_input();

	/* A retire request before the PAGE proof exists is DENIED (fail-
	 * closed); not-ready is a retry, never authority to remove the
	 * retained interval. */
	UT_ASSERT(cluster_page_handoff_retention_denied(&in));

	/* The deny is the default for anything short of a complete handoff. */
	ut_proof.contributor_coverage = true;
	ut_proof.durability_barrier_ok = true;
	ut_proof.post_read_ok = true;
	ut_proof.authority_revalidated = false; /* proof incomplete */
	UT_ASSERT(cluster_page_handoff_retention_denied(&in));
	ut_proof.authority_revalidated = true;
	UT_ASSERT(!cluster_page_handoff_retention_denied(&in)); /* now ready */
}

UT_TEST(test_handoff_is_per_resource)
{
	ClusterPageHandoffInput in;
	ClusterPageProof other;

	/* §6.4: one block's readiness never proves another block, relation,
	 * thread or instance readiness.  Two handoff judgements are
	 * independent — one ready input and one identical-but-incomplete
	 * input coexist without any shared barrier state. */
	memset(&ut_proof, 0, sizeof(ut_proof));
	ut_proof.contributor_coverage = true;
	ut_proof.durability_barrier_ok = true;
	ut_proof.post_read_ok = true;
	ut_proof.authority_revalidated = true;
	in = ut_full_input();
	UT_ASSERT(cluster_page_handoff_ready(&in));

	memset(&other, 0, sizeof(other)); /* incomplete proof for block B */
	in.proof = &other;
	UT_ASSERT(!cluster_page_handoff_ready(&in));
	UT_ASSERT(cluster_page_handoff_retention_denied(&in));

	/* The complete block A judgement is unaffected by block B's
	 * incompleteness (no global barrier). */
	in.proof = &ut_proof;
	UT_ASSERT(cluster_page_handoff_ready(&in));
}

int
main(void)
{
	UT_PLAN(3);

	UT_RUN(test_fnd10_conjunction);
	UT_RUN(test_pl12_retention_denied_before_proof);
	UT_RUN(test_handoff_is_per_resource);

	UT_DONE();
	return ut_failed_count == 0 ? 0 : 1;
}
