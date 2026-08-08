/*-------------------------------------------------------------------------
 *
 * test_cluster_active_itl_transfer.c
 *    Executable policy tests for active-ITL current-block transfer.
 *
 * Portions Copyright (c) 1996-2024, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2026, pgrac contributors
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "cluster/cluster_gcs_block.h"
#include "cluster/cluster_itl_touch.h"
#include "cluster/cluster_pcm_x_bufmgr.h"

#undef printf

#include "unit_test.h"

UT_DEFINE_GLOBALS();

void
ExceptionalCondition(const char *condition_name pg_attribute_unused(),
					 const char *file_name pg_attribute_unused(),
					 int line_number pg_attribute_unused())
{
	abort();
}

static ClusterItlTerminalProof
valid_proof(void)
{
	ClusterItlTerminalProof proof;

	MemSet(&proof, 0, sizeof(proof));
	proof.xid = 700;
	proof.buffer_id = 8;
	proof.own_generation = 41;
	proof.acquisition_epoch = 17;
	proof.slot_wrap = 3;
	proof.slot_class = ITL_FLAG_ACTIVE;
	proof.valid = true;
	return proof;
}

UT_TEST(u1_tracked_null_claim_fails_closed)
{
	UT_ASSERT_EQ(cluster_pcm_x_writer_null_route(true), CLUSTER_PCM_X_WRITER_FAIL_CLOSED);
}

UT_TEST(u2_nontracked_null_claim_is_legacy_safe)
{
	UT_ASSERT_EQ(cluster_pcm_x_writer_null_route(false), CLUSTER_PCM_X_WRITER_LEGACY_SAFE);
}

UT_TEST(u3_plain_read_uses_read_image)
{
	UT_ASSERT(cluster_gcs_block_must_preserve_x(true, false, false));
}

UT_TEST(u4_active_data_x_transfer_preserves_x)
{
	UT_ASSERT(cluster_gcs_block_must_preserve_x(false, true, true));
}

UT_TEST(u5_active_lock_only_x_transfer_preserves_x)
{
	UT_ASSERT(cluster_gcs_block_must_preserve_x(false, true, true));
}

UT_TEST(u6_terminal_x_transfer_may_revoke)
{
	UT_ASSERT(!cluster_gcs_block_must_preserve_x(false, true, false));
}

UT_TEST(u7_nontransfer_active_image_does_not_force_containment)
{
	UT_ASSERT(!cluster_gcs_block_must_preserve_x(false, false, true));
}

UT_TEST(u8_source_prepare_counts_first_stored_active_x)
{
	UT_ASSERT(cluster_gcs_block_count_active_source_prepare(true, true, true));
}

UT_TEST(u9_source_prepare_duplicate_does_not_count)
{
	UT_ASSERT(!cluster_gcs_block_count_active_source_prepare(false, true, true));
}

UT_TEST(u10_unstored_source_prepare_does_not_count)
{
	UT_ASSERT(!cluster_gcs_block_count_active_source_prepare(false, true, true));
}

UT_TEST(u11_non_x_source_prepare_does_not_count)
{
	UT_ASSERT(!cluster_gcs_block_count_active_source_prepare(true, false, true));
}

UT_TEST(u12_terminal_source_prepare_does_not_count)
{
	UT_ASSERT(!cluster_gcs_block_count_active_source_prepare(true, true, false));
}

UT_TEST(u13_exact_owner_proof_matches)
{
	ClusterItlTerminalProof proof = valid_proof();

	UT_ASSERT(cluster_itl_terminal_proof_owner_exact(&proof, 41, 17, true, 0, 0));
}

UT_TEST(u14_missing_owner_proof_is_rejected)
{
	ClusterItlTerminalProof proof = valid_proof();

	proof.valid = false;
	UT_ASSERT(!cluster_itl_terminal_proof_owner_exact(&proof, 41, 17, true, 0, 0));
}

UT_TEST(u15_later_x_generation_is_rejected)
{
	ClusterItlTerminalProof proof = valid_proof();

	UT_ASSERT(!cluster_itl_terminal_proof_owner_exact(&proof, 42, 17, true, 0, 0));
}

UT_TEST(u16_scope_change_is_rejected)
{
	ClusterItlTerminalProof proof = valid_proof();

	UT_ASSERT(!cluster_itl_terminal_proof_owner_exact(&proof, 41, 18, true, 0, 0));
}

UT_TEST(u17_non_x_owner_is_rejected)
{
	ClusterItlTerminalProof proof = valid_proof();

	UT_ASSERT(!cluster_itl_terminal_proof_owner_exact(&proof, 41, 17, false, 0, 0));
}

UT_TEST(u18_busy_owner_is_rejected)
{
	ClusterItlTerminalProof proof = valid_proof();

	UT_ASSERT(!cluster_itl_terminal_proof_owner_exact(&proof, 41, 17, true, 1, 0));
	UT_ASSERT(!cluster_itl_terminal_proof_owner_exact(&proof, 41, 17, true, 0, 99));
}

UT_TEST(u19_exact_slot_proof_matches)
{
	ClusterItlTerminalProof proof = valid_proof();

	UT_ASSERT(cluster_itl_terminal_proof_slot_exact(&proof, 700, 3, ITL_FLAG_ACTIVE));
}

UT_TEST(u20_slot_aba_or_class_change_is_rejected)
{
	ClusterItlTerminalProof proof = valid_proof();

	UT_ASSERT(!cluster_itl_terminal_proof_slot_exact(&proof, 701, 3, ITL_FLAG_ACTIVE));
	UT_ASSERT(!cluster_itl_terminal_proof_slot_exact(&proof, 700, 4, ITL_FLAG_ACTIVE));
	UT_ASSERT(!cluster_itl_terminal_proof_slot_exact(&proof, 700, 3,
											 ITL_FLAG_LOCK_ONLY_ACTIVE));
}

int
main(void)
{
	UT_RUN(u1_tracked_null_claim_fails_closed);
	UT_RUN(u2_nontracked_null_claim_is_legacy_safe);
	UT_RUN(u3_plain_read_uses_read_image);
	UT_RUN(u4_active_data_x_transfer_preserves_x);
	UT_RUN(u5_active_lock_only_x_transfer_preserves_x);
	UT_RUN(u6_terminal_x_transfer_may_revoke);
	UT_RUN(u7_nontransfer_active_image_does_not_force_containment);
	UT_RUN(u8_source_prepare_counts_first_stored_active_x);
	UT_RUN(u9_source_prepare_duplicate_does_not_count);
	UT_RUN(u10_unstored_source_prepare_does_not_count);
	UT_RUN(u11_non_x_source_prepare_does_not_count);
	UT_RUN(u12_terminal_source_prepare_does_not_count);
	UT_RUN(u13_exact_owner_proof_matches);
	UT_RUN(u14_missing_owner_proof_is_rejected);
	UT_RUN(u15_later_x_generation_is_rejected);
	UT_RUN(u16_scope_change_is_rejected);
	UT_RUN(u17_non_x_owner_is_rejected);
	UT_RUN(u18_busy_owner_is_rejected);
	UT_RUN(u19_exact_slot_proof_matches);
	UT_RUN(u20_slot_aba_or_class_change_is_rejected);
	UT_DONE();
}
