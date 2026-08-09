/*-------------------------------------------------------------------------
 *
 * test_cluster_r4_route_policy.c
 *	  Stage 8 R4 canonical current-holder route policy.
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "cluster/cluster_gcs_block.h"

#undef printf

#include "unit_test.h"

UT_DEFINE_GLOBALS();

static PcmAuthoritySnapshot
route_snapshot(PcmState state)
{
	PcmAuthoritySnapshot authority;

	memset(&authority, 0, sizeof(authority));
	authority.master_holder.node_id = UINT32_MAX;
	authority.transition_count = 7;
	authority.state = state;
	authority.x_holder_node = -1;
	authority.pending_x_requester_node = -1;
	return authority;
}

static ClusterCrBuildReason
classify(const PcmAuthoritySnapshot *authority, uint64 epoch, uint64 generation,
		 int32 *holder_out)
{
	return cluster_r4_route_policy_classify(authority, epoch, generation, holder_out);
}

#define DEFINE_REASON_TEST(test_name, setup_code, expected_reason, expected_holder)                \
	UT_TEST(test_name)                                                                             \
	{                                                                                              \
		PcmAuthoritySnapshot authority = route_snapshot(PCM_STATE_N);                               \
		PcmAuthoritySnapshot *authority_ptr = &authority;                                           \
		uint64 epoch = 9;                                                                           \
		uint64 generation = (epoch << 32) | 3;                                                      \
		int32 holder = -1;                                                                          \
		setup_code;                                                                                  \
		UT_ASSERT_EQ(classify(authority_ptr, epoch, generation, &holder), (expected_reason));        \
		UT_ASSERT_EQ(holder, (expected_holder));                                                     \
	}

DEFINE_REASON_TEST(test_01_null_authority_is_protocol, authority_ptr = NULL,
			   CLUSTER_CR_BUILD_PROTOCOL, -1)

UT_TEST(test_02_null_output_is_protocol)
{
	PcmAuthoritySnapshot authority = route_snapshot(PCM_STATE_N);

	UT_ASSERT_EQ(classify(&authority, 9, (UINT64_C(9) << 32) | 3, NULL),
				 CLUSTER_CR_BUILD_PROTOCOL);
}

DEFINE_REASON_TEST(test_03_canonical_n_has_no_holder, (void)0, CLUSTER_CR_BUILD_NO_HOLDER, -1)
DEFINE_REASON_TEST(test_04_n_with_x_is_ambiguous, authority.x_holder_node = 1,
			   CLUSTER_CR_BUILD_HOLDER_AMBIGUOUS, -1)
DEFINE_REASON_TEST(test_05_n_with_s_is_ambiguous, authority.s_holders_bitmap = 1,
			   CLUSTER_CR_BUILD_HOLDER_AMBIGUOUS, -1)
DEFINE_REASON_TEST(test_06_n_with_master_is_ambiguous, authority.master_holder.node_id = 1,
			   CLUSTER_CR_BUILD_HOLDER_AMBIGUOUS, -1)
DEFINE_REASON_TEST(test_07_x_node_zero_is_selected,
			   authority.state = PCM_STATE_X; authority.x_holder_node = 0;
			   authority.master_holder.node_id = 0,
			   CLUSTER_CR_BUILD_NONE, 0)
DEFINE_REASON_TEST(test_08_x_node_31_is_selected,
			   authority.state = PCM_STATE_X; authority.x_holder_node = 31;
			   authority.master_holder.node_id = 31,
			   CLUSTER_CR_BUILD_NONE, 31)
DEFINE_REASON_TEST(test_09_x_without_holder_is_ambiguous, authority.state = PCM_STATE_X,
			   CLUSTER_CR_BUILD_HOLDER_AMBIGUOUS, -1)
DEFINE_REASON_TEST(test_10_x_negative_holder_is_ambiguous,
			   authority.state = PCM_STATE_X; authority.x_holder_node = -2,
			   CLUSTER_CR_BUILD_HOLDER_AMBIGUOUS, -1)
DEFINE_REASON_TEST(test_11_x_out_of_range_is_ambiguous,
			   authority.state = PCM_STATE_X; authority.x_holder_node = 32;
			   authority.master_holder.node_id = 32,
			   CLUSTER_CR_BUILD_HOLDER_AMBIGUOUS, -1)
DEFINE_REASON_TEST(test_12_x_master_mismatch_is_ambiguous,
			   authority.state = PCM_STATE_X; authority.x_holder_node = 4;
			   authority.master_holder.node_id = 5,
			   CLUSTER_CR_BUILD_HOLDER_AMBIGUOUS, -1)
DEFINE_REASON_TEST(test_13_x_with_s_bitmap_is_ambiguous,
			   authority.state = PCM_STATE_X; authority.x_holder_node = 4;
			   authority.master_holder.node_id = 4; authority.s_holders_bitmap = 1,
			   CLUSTER_CR_BUILD_HOLDER_AMBIGUOUS, -1)
DEFINE_REASON_TEST(test_14_s_node_zero_is_selected,
			   authority.state = PCM_STATE_S; authority.master_holder.node_id = 0;
			   authority.s_holders_bitmap = 1,
			   CLUSTER_CR_BUILD_NONE, 0)
DEFINE_REASON_TEST(test_15_s_node_31_is_selected,
			   authority.state = PCM_STATE_S; authority.master_holder.node_id = 31;
			   authority.s_holders_bitmap = UINT32_C(1) << 31,
			   CLUSTER_CR_BUILD_NONE, 31)
DEFINE_REASON_TEST(test_16_s_multiple_selects_canonical,
			   authority.state = PCM_STATE_S; authority.master_holder.node_id = 3;
			   authority.s_holders_bitmap = (UINT32_C(1) << 2) | (UINT32_C(1) << 3),
			   CLUSTER_CR_BUILD_NONE, 3)
DEFINE_REASON_TEST(test_17_s_zero_bitmap_is_ambiguous,
			   authority.state = PCM_STATE_S; authority.master_holder.node_id = 3,
			   CLUSTER_CR_BUILD_HOLDER_AMBIGUOUS, -1)
DEFINE_REASON_TEST(test_18_s_with_x_is_ambiguous,
			   authority.state = PCM_STATE_S; authority.master_holder.node_id = 3;
			   authority.s_holders_bitmap = UINT32_C(1) << 3; authority.x_holder_node = 3,
			   CLUSTER_CR_BUILD_HOLDER_AMBIGUOUS, -1)
DEFINE_REASON_TEST(test_19_s_without_master_is_ambiguous,
			   authority.state = PCM_STATE_S; authority.s_holders_bitmap = 1,
			   CLUSTER_CR_BUILD_HOLDER_AMBIGUOUS, -1)
DEFINE_REASON_TEST(test_20_s_out_of_range_master_is_ambiguous,
			   authority.state = PCM_STATE_S; authority.master_holder.node_id = 33;
			   authority.s_holders_bitmap = 1,
			   CLUSTER_CR_BUILD_HOLDER_AMBIGUOUS, -1)
DEFINE_REASON_TEST(test_21_s_missing_canonical_bit_is_ambiguous,
			   authority.state = PCM_STATE_S; authority.master_holder.node_id = 3;
			   authority.s_holders_bitmap = UINT32_C(1) << 2,
			   CLUSTER_CR_BUILD_HOLDER_AMBIGUOUS, -1)
DEFINE_REASON_TEST(test_22_unknown_state_is_ambiguous, authority.state = (PcmState)99,
			   CLUSTER_CR_BUILD_HOLDER_AMBIGUOUS, -1)
DEFINE_REASON_TEST(test_23_reserved_zero_is_required, authority.reserved[0] = 1,
			   CLUSTER_CR_BUILD_HOLDER_AMBIGUOUS, -1)
DEFINE_REASON_TEST(test_24_reserved_one_is_required, authority.reserved[1] = 1,
			   CLUSTER_CR_BUILD_HOLDER_AMBIGUOUS, -1)
DEFINE_REASON_TEST(test_25_pending_destructive_convert_is_recovering,
			   authority.pending_x_requester_node = 4,
			   CLUSTER_CR_BUILD_RECOVERING, -1)
DEFINE_REASON_TEST(test_26_zero_master_generation_is_rejected,
			   authority.state = PCM_STATE_S; authority.master_holder.node_id = 0;
			   authority.s_holders_bitmap = 1; generation = 0,
			   CLUSTER_CR_BUILD_GENERATION_MISMATCH, -1)
DEFINE_REASON_TEST(test_27_zero_restart_half_is_rejected,
			   authority.state = PCM_STATE_S; authority.master_holder.node_id = 0;
			   authority.s_holders_bitmap = 1; generation = epoch << 32,
			   CLUSTER_CR_BUILD_GENERATION_MISMATCH, -1)
DEFINE_REASON_TEST(test_28_wrong_epoch_half_is_rejected,
			   authority.state = PCM_STATE_S; authority.master_holder.node_id = 0;
			   authority.s_holders_bitmap = 1; generation = ((epoch + 1) << 32) | 3,
			   CLUSTER_CR_BUILD_GENERATION_MISMATCH, -1)
DEFINE_REASON_TEST(test_29_zero_transition_count_is_rejected,
			   authority.state = PCM_STATE_S; authority.master_holder.node_id = 0;
			   authority.s_holders_bitmap = 1; authority.transition_count = 0,
			   CLUSTER_CR_BUILD_GENERATION_MISMATCH, -1)
DEFINE_REASON_TEST(test_30_exhausted_transition_count_is_rejected,
			   authority.state = PCM_STATE_S; authority.master_holder.node_id = 0;
			   authority.s_holders_bitmap = 1; authority.transition_count = UINT64_MAX,
			   CLUSTER_CR_BUILD_GENERATION_MISMATCH, -1)

static ClusterR4CrRouteProof
route_proof(void)
{
	ClusterR4CrRouteProof proof;

	memset(&proof, 0, sizeof(proof));
	proof.formation_epoch = 9;
	proof.master_authority_generation = (UINT64_C(9) << 32) | 3;
	proof.master_resource_transition_count = 7;
	proof.expected_page_scn = (SCN)11;
	proof.selected_holder_node = 4;
	return proof;
}

UT_TEST(test_31_exact_duplicate_route_matches)
{
	ClusterR4CrRouteProof proof = route_proof();

	UT_ASSERT(cluster_r4_route_proof_matches(&proof, 9, (UINT64_C(9) << 32) | 3, 4, 7,
										   (SCN)11));
}

UT_TEST(test_32_transition_drift_closes_duplicate)
{
	ClusterR4CrRouteProof proof = route_proof();

	UT_ASSERT(!cluster_r4_route_proof_matches(&proof, 9, (UINT64_C(9) << 32) | 3, 4, 8,
											(SCN)11));
}

static const ClusterCrBuildResult reason_result[18] = {
	[CLUSTER_CR_BUILD_NONE] = CLUSTER_CR_BUILD_FULL,
	[CLUSTER_CR_BUILD_TARGET_DISABLED] = CLUSTER_CR_BUILD_RETRYABLE,
	[CLUSTER_CR_BUILD_RF_DEFERRED] = CLUSTER_CR_BUILD_RETRYABLE,
	[CLUSTER_CR_BUILD_WRONG_MASTER] = CLUSTER_CR_BUILD_RETRYABLE,
	[CLUSTER_CR_BUILD_NO_HOLDER] = CLUSTER_CR_BUILD_RETRYABLE,
	[CLUSTER_CR_BUILD_HOLDER_AMBIGUOUS] = CLUSTER_CR_BUILD_RETRYABLE,
	[CLUSTER_CR_BUILD_HOLDER_MOVED] = CLUSTER_CR_BUILD_RETRYABLE,
	[CLUSTER_CR_BUILD_RECOVERING] = CLUSTER_CR_BUILD_RETRYABLE,
	[CLUSTER_CR_BUILD_GENERATION_MISMATCH] = CLUSTER_CR_BUILD_RETRYABLE,
	[CLUSTER_CR_BUILD_CAPACITY] = CLUSTER_CR_BUILD_RETRYABLE,
	[CLUSTER_CR_BUILD_BAD_LOCATOR] = CLUSTER_CR_BUILD_FAIL_CLOSED,
	[CLUSTER_CR_BUILD_BAD_UNDO] = CLUSTER_CR_BUILD_FAIL_CLOSED,
	[CLUSTER_CR_BUILD_CHAIN_LIMIT] = CLUSTER_CR_BUILD_FAIL_CLOSED,
	[CLUSTER_CR_BUILD_SNAPSHOT_TOO_OLD] = CLUSTER_CR_BUILD_FAIL_CLOSED,
	[CLUSTER_CR_BUILD_EPOCH_MISMATCH] = CLUSTER_CR_BUILD_RETRYABLE,
	[CLUSTER_CR_BUILD_CANCELLED] = CLUSTER_CR_BUILD_FAIL_CLOSED,
	[CLUSTER_CR_BUILD_IO_ERROR] = CLUSTER_CR_BUILD_FAIL_CLOSED,
	[CLUSTER_CR_BUILD_PROTOCOL] = CLUSTER_CR_BUILD_FAIL_CLOSED,
};

static void
run_reason_polarity(int reason)
{
	UT_ASSERT_EQ(cluster_cr_build_result_for_reason((ClusterCrBuildReason)reason),
				 reason_result[reason]);
}

#define DEFINE_POLARITY_TEST(n) \
	UT_TEST(test_reason_polarity_##n) { run_reason_polarity(n); }

DEFINE_POLARITY_TEST(0)
DEFINE_POLARITY_TEST(1)
DEFINE_POLARITY_TEST(2)
DEFINE_POLARITY_TEST(3)
DEFINE_POLARITY_TEST(4)
DEFINE_POLARITY_TEST(5)
DEFINE_POLARITY_TEST(6)
DEFINE_POLARITY_TEST(7)
DEFINE_POLARITY_TEST(8)
DEFINE_POLARITY_TEST(9)
DEFINE_POLARITY_TEST(10)
DEFINE_POLARITY_TEST(11)
DEFINE_POLARITY_TEST(12)
DEFINE_POLARITY_TEST(13)
DEFINE_POLARITY_TEST(14)
DEFINE_POLARITY_TEST(15)
DEFINE_POLARITY_TEST(16)
DEFINE_POLARITY_TEST(17)

UT_TEST(test_unknown_reason_fails_closed)
{
	UT_ASSERT_EQ(cluster_cr_build_result_for_reason((ClusterCrBuildReason)-1),
				 CLUSTER_CR_BUILD_FAIL_CLOSED);
	UT_ASSERT_EQ(cluster_cr_build_result_for_reason((ClusterCrBuildReason)18),
				 CLUSTER_CR_BUILD_FAIL_CLOSED);
}

int
main(void)
{
	UT_PLAN(51);
	UT_RUN(test_01_null_authority_is_protocol);
	UT_RUN(test_02_null_output_is_protocol);
	UT_RUN(test_03_canonical_n_has_no_holder);
	UT_RUN(test_04_n_with_x_is_ambiguous);
	UT_RUN(test_05_n_with_s_is_ambiguous);
	UT_RUN(test_06_n_with_master_is_ambiguous);
	UT_RUN(test_07_x_node_zero_is_selected);
	UT_RUN(test_08_x_node_31_is_selected);
	UT_RUN(test_09_x_without_holder_is_ambiguous);
	UT_RUN(test_10_x_negative_holder_is_ambiguous);
	UT_RUN(test_11_x_out_of_range_is_ambiguous);
	UT_RUN(test_12_x_master_mismatch_is_ambiguous);
	UT_RUN(test_13_x_with_s_bitmap_is_ambiguous);
	UT_RUN(test_14_s_node_zero_is_selected);
	UT_RUN(test_15_s_node_31_is_selected);
	UT_RUN(test_16_s_multiple_selects_canonical);
	UT_RUN(test_17_s_zero_bitmap_is_ambiguous);
	UT_RUN(test_18_s_with_x_is_ambiguous);
	UT_RUN(test_19_s_without_master_is_ambiguous);
	UT_RUN(test_20_s_out_of_range_master_is_ambiguous);
	UT_RUN(test_21_s_missing_canonical_bit_is_ambiguous);
	UT_RUN(test_22_unknown_state_is_ambiguous);
	UT_RUN(test_23_reserved_zero_is_required);
	UT_RUN(test_24_reserved_one_is_required);
	UT_RUN(test_25_pending_destructive_convert_is_recovering);
	UT_RUN(test_26_zero_master_generation_is_rejected);
	UT_RUN(test_27_zero_restart_half_is_rejected);
	UT_RUN(test_28_wrong_epoch_half_is_rejected);
	UT_RUN(test_29_zero_transition_count_is_rejected);
	UT_RUN(test_30_exhausted_transition_count_is_rejected);
	UT_RUN(test_31_exact_duplicate_route_matches);
	UT_RUN(test_32_transition_drift_closes_duplicate);
	UT_RUN(test_reason_polarity_0);
	UT_RUN(test_reason_polarity_1);
	UT_RUN(test_reason_polarity_2);
	UT_RUN(test_reason_polarity_3);
	UT_RUN(test_reason_polarity_4);
	UT_RUN(test_reason_polarity_5);
	UT_RUN(test_reason_polarity_6);
	UT_RUN(test_reason_polarity_7);
	UT_RUN(test_reason_polarity_8);
	UT_RUN(test_reason_polarity_9);
	UT_RUN(test_reason_polarity_10);
	UT_RUN(test_reason_polarity_11);
	UT_RUN(test_reason_polarity_12);
	UT_RUN(test_reason_polarity_13);
	UT_RUN(test_reason_polarity_14);
	UT_RUN(test_reason_polarity_15);
	UT_RUN(test_reason_polarity_16);
	UT_RUN(test_reason_polarity_17);
	UT_RUN(test_unknown_reason_fails_closed);
	UT_DONE();
	return ut_failed_count == 0 ? 0 : 1;
}
