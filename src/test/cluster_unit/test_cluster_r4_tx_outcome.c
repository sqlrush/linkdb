/*-------------------------------------------------------------------------
 *
 * test_cluster_r4_tx_outcome.c
 *    Stage 8 R4 closed outcome/proof compatibility table.
 *
 * Portions Copyright (c) 1996-2024, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2026, pgrac contributors
 *
 * IDENTIFICATION
 *    src/test/cluster_unit/test_cluster_r4_tx_outcome.c
 *
 * NOTES
 *    This is a pgrac-original file.
 *
 *-------------------------------------------------------------------------
 */
#define USE_PGRAC_CLUSTER 1

#include "postgres.h"

#include "cluster/cluster_tx_resolve.h"

#undef printf

#include "unit_test.h"

UT_DEFINE_GLOBALS();

static const bool expected[5][8] = {
	[CLUSTER_TX_UNKNOWN] = {
		[CLUSTER_TX_PROOF_NONE] = true,
		[CLUSTER_TX_PROOF_RECYCLED_BELOW_HORIZON] = true,
	},
	[CLUSTER_TX_IN_PROGRESS] = {
		[CLUSTER_TX_PROOF_ORIGIN_DURABLE_TT_CLOG] = true,
		[CLUSTER_TX_PROOF_ORIGIN_SUBTRANS_TOP] = true,
		[CLUSTER_TX_PROOF_ORIGIN_MULTIXACT] = true,
	},
	[CLUSTER_TX_PREPARED] = {
		[CLUSTER_TX_PROOF_ORIGIN_SUBTRANS_TOP] = true,
		[CLUSTER_TX_PROOF_ORIGIN_TWOPHASE] = true,
		[CLUSTER_TX_PROOF_ORIGIN_MULTIXACT] = true,
	},
	[CLUSTER_TX_COMMITTED] = {
		[CLUSTER_TX_PROOF_ITL_CLEANOUT] = true,
		[CLUSTER_TX_PROOF_ORIGIN_DURABLE_TT_CLOG] = true,
		[CLUSTER_TX_PROOF_ORIGIN_SUBTRANS_TOP] = true,
		[CLUSTER_TX_PROOF_ORIGIN_MULTIXACT] = true,
		[CLUSTER_TX_PROOF_RECOVERY_MATERIALIZED] = true,
	},
	[CLUSTER_TX_ABORTED] = {
		[CLUSTER_TX_PROOF_ITL_CLEANOUT] = true,
		[CLUSTER_TX_PROOF_ORIGIN_DURABLE_TT_CLOG] = true,
		[CLUSTER_TX_PROOF_ORIGIN_SUBTRANS_TOP] = true,
		[CLUSTER_TX_PROOF_ORIGIN_MULTIXACT] = true,
		[CLUSTER_TX_PROOF_RECOVERY_MATERIALIZED] = true,
	},
};

static void
run_pair(unsigned int pair)
{
	ClusterTxOutcome outcome = (ClusterTxOutcome)(pair / 8);
	ClusterTxProofKind proof = (ClusterTxProofKind)(pair % 8);

	UT_ASSERT_EQ(cluster_tx_outcome_proof_is_valid(outcome, proof), expected[outcome][proof]);
}

#define DEFINE_PAIR_TEST(n) \
	UT_TEST(test_outcome_proof_pair_##n) { run_pair(n); }

#define RUN_PAIR_TEST(n) UT_RUN(test_outcome_proof_pair_##n)

DEFINE_PAIR_TEST(0)
DEFINE_PAIR_TEST(1)
DEFINE_PAIR_TEST(2)
DEFINE_PAIR_TEST(3)
DEFINE_PAIR_TEST(4)
DEFINE_PAIR_TEST(5)
DEFINE_PAIR_TEST(6)
DEFINE_PAIR_TEST(7)
DEFINE_PAIR_TEST(8)
DEFINE_PAIR_TEST(9)
DEFINE_PAIR_TEST(10)
DEFINE_PAIR_TEST(11)
DEFINE_PAIR_TEST(12)
DEFINE_PAIR_TEST(13)
DEFINE_PAIR_TEST(14)
DEFINE_PAIR_TEST(15)
DEFINE_PAIR_TEST(16)
DEFINE_PAIR_TEST(17)
DEFINE_PAIR_TEST(18)
DEFINE_PAIR_TEST(19)
DEFINE_PAIR_TEST(20)
DEFINE_PAIR_TEST(21)
DEFINE_PAIR_TEST(22)
DEFINE_PAIR_TEST(23)
DEFINE_PAIR_TEST(24)
DEFINE_PAIR_TEST(25)
DEFINE_PAIR_TEST(26)
DEFINE_PAIR_TEST(27)
DEFINE_PAIR_TEST(28)
DEFINE_PAIR_TEST(29)
DEFINE_PAIR_TEST(30)
DEFINE_PAIR_TEST(31)
DEFINE_PAIR_TEST(32)
DEFINE_PAIR_TEST(33)
DEFINE_PAIR_TEST(34)
DEFINE_PAIR_TEST(35)
DEFINE_PAIR_TEST(36)
DEFINE_PAIR_TEST(37)
DEFINE_PAIR_TEST(38)
DEFINE_PAIR_TEST(39)

UT_TEST(test_out_of_domain_values_fail_closed)
{
	UT_ASSERT(!cluster_tx_outcome_proof_is_valid((ClusterTxOutcome)-1, CLUSTER_TX_PROOF_NONE));
	UT_ASSERT(!cluster_tx_outcome_proof_is_valid((ClusterTxOutcome)5, CLUSTER_TX_PROOF_NONE));
	UT_ASSERT(!cluster_tx_outcome_proof_is_valid(CLUSTER_TX_UNKNOWN, (ClusterTxProofKind)-1));
	UT_ASSERT(!cluster_tx_outcome_proof_is_valid(CLUSTER_TX_UNKNOWN, (ClusterTxProofKind)8));
}

int
main(void)
{
	UT_PLAN(41);
	RUN_PAIR_TEST(0);
	RUN_PAIR_TEST(1);
	RUN_PAIR_TEST(2);
	RUN_PAIR_TEST(3);
	RUN_PAIR_TEST(4);
	RUN_PAIR_TEST(5);
	RUN_PAIR_TEST(6);
	RUN_PAIR_TEST(7);
	RUN_PAIR_TEST(8);
	RUN_PAIR_TEST(9);
	RUN_PAIR_TEST(10);
	RUN_PAIR_TEST(11);
	RUN_PAIR_TEST(12);
	RUN_PAIR_TEST(13);
	RUN_PAIR_TEST(14);
	RUN_PAIR_TEST(15);
	RUN_PAIR_TEST(16);
	RUN_PAIR_TEST(17);
	RUN_PAIR_TEST(18);
	RUN_PAIR_TEST(19);
	RUN_PAIR_TEST(20);
	RUN_PAIR_TEST(21);
	RUN_PAIR_TEST(22);
	RUN_PAIR_TEST(23);
	RUN_PAIR_TEST(24);
	RUN_PAIR_TEST(25);
	RUN_PAIR_TEST(26);
	RUN_PAIR_TEST(27);
	RUN_PAIR_TEST(28);
	RUN_PAIR_TEST(29);
	RUN_PAIR_TEST(30);
	RUN_PAIR_TEST(31);
	RUN_PAIR_TEST(32);
	RUN_PAIR_TEST(33);
	RUN_PAIR_TEST(34);
	RUN_PAIR_TEST(35);
	RUN_PAIR_TEST(36);
	RUN_PAIR_TEST(37);
	RUN_PAIR_TEST(38);
	RUN_PAIR_TEST(39);
	UT_RUN(test_out_of_domain_values_fail_closed);
	UT_DONE();
	return ut_failed_count == 0 ? 0 : 1;
}
