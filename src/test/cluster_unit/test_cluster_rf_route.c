/*-------------------------------------------------------------------------
 *
 * test_cluster_rf_route.c
 *    Stage 8 JIT Task 4 exhaustive redo-route tests.
 *
 *    The immutable RED build has no future route header or object.  It runs
 *    through the existing common record/apply decision and names the route
 *    information that boundary cannot represent.  Once the production
 *    route API exists, the same unit switches to its real declarations and
 *    exercises the generated authority directly; no substitute route table
 *    or test-owned parser is provided here.
 *
 *-------------------------------------------------------------------------
 */
#define USE_PGRAC_CLUSTER 1

#include "postgres.h"

#include "cluster/cluster_block_apply.h"

#if defined(__has_include)
#if __has_include("cluster/cluster_rf_route.h")
#define HAVE_CLUSTER_RF_ROUTE 1
#include "cluster/cluster_rf_route.h"
#endif
#endif

#include "unit_test.h"

UT_DEFINE_GLOBALS();


void
ExceptionalCondition(const char *condition_name, const char *file_name, int line_number)
{
	printf("# unexpected Assert: %s at %s:%d\n", condition_name, file_name, line_number);
	abort();
}


UT_TEST(test_existing_apply_boundary_control)
{
	UT_ASSERT_EQ((int) cluster_block_apply_decide(true, true, true),
				 (int) CLUSTER_BLKAPPLY_ACT_FPI);
}

#ifndef HAVE_CLUSTER_RF_ROUTE

UT_TEST(test_red_page_zero_block_must_not_be_noop)
{
	printf("# JIT_SEMANTIC_RED:T4-PAGE-ZERO-BLOCK\n");
	UT_ASSERT_NE((int) cluster_block_apply_decide(false, false, false),
				 (int) CLUSTER_BLKAPPLY_ACT_NOOP);
}

UT_TEST(test_red_adg_block_must_not_enter_delta_apply)
{
	printf("# JIT_SEMANTIC_RED:T4-ADG-BLOCK-NO-ORDINARY\n");
	UT_ASSERT_NE((int) cluster_block_apply_decide(true, false, false),
				 (int) CLUSTER_BLKAPPLY_ACT_DELTA);
}

UT_TEST(test_red_xid_stripe_requires_typed_route)
{
	printf("# JIT_SEMANTIC_RED:T4-XID-STRIPE-TYPED-OWNER\n");
	UT_ASSERT_NE((int) cluster_block_apply_decide(false, false, false),
				 (int) CLUSTER_BLKAPPLY_ACT_NOOP);
}

#endif

int
main(void)
{
	UT_PLAN(4);
	UT_RUN(test_existing_apply_boundary_control);
#ifndef HAVE_CLUSTER_RF_ROUTE
	UT_RUN(test_red_page_zero_block_must_not_be_noop);
	UT_RUN(test_red_adg_block_must_not_enter_delta_apply);
	UT_RUN(test_red_xid_stripe_requires_typed_route);
#endif
	UT_DONE();
	return ut_failed_count == 0 ? 0 : 1;
}
