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

#include "access/generic_xlog.h"
#include "cluster/cluster_gcs_block.h"
#include "cluster/cluster_itl_touch.h"
#include "cluster/cluster_pcm_x_bufmgr.h"
#include "cluster/cluster_xnode_lever.h"
#include "storage/bufmgr.h"
#include "utils/memutils.h"

#undef printf

#include "unit_test.h"

UT_DEFINE_GLOBALS();

/*
 * Real cluster_itl_touch.o harness.  Only backend services below the
 * registration list and terminal-finish boundary are replaced: one aligned
 * shared-buffer page, holder-authority projection, memory allocation and the
 * no-fetch/GenericXLog terminal-stamp boundary.  The registration, dedupe,
 * proof invalidation and finish traversal remain the production object.
 */
static PGAlignedBlock test_buffer_block;
static bool test_capture_authority;
static uint64 test_own_generation;
static uint64 test_acquisition_epoch;
static uint8 test_pcm_state;
static uint32 test_own_flags;
static uint64 test_writer_activation_token;
static int test_node_count;
static bool test_recovery_merge_active;
static BufferTag test_live_tag;
static int test_stamp_lock_calls;
static bool test_stamp_lock_saw_valid_proof;
static int test_generic_register_calls;
static int test_generic_abort_calls;
static int test_generic_finish_calls;
static int test_stamp_unlock_calls;
static int test_stamp_skip_calls;
static int test_generic_state_storage;

int NBuffers = 1;
int NLocBuffer = 0;
char *BufferBlocks = test_buffer_block.data;
Block *LocalBufferBlockPointers = NULL;
MemoryContext CurrentMemoryContext = (MemoryContext)1;
MemoryContext TopTransactionContext = (MemoryContext)1;
bool cluster_enabled = true;
int cluster_node_id = 0;

void
ExceptionalCondition(const char *condition_name pg_attribute_unused(),
					 const char *file_name pg_attribute_unused(),
					 int line_number pg_attribute_unused())
{
	abort();
}

void *
palloc(Size size)
{
	void *memory = malloc(size);

	if (memory == NULL)
		abort();
	return memory;
}

void *
repalloc(void *pointer, Size size)
{
	void *memory = realloc(pointer, size);

	if (memory == NULL)
		abort();
	return memory;
}

bool
cluster_bufmgr_terminal_stamp_authority(Buffer buffer, const BufferTag *expected_tag,
										uint64 *own_generation,
										uint64 *acquisition_epoch,
										uint8 *pcm_state)
{
	UT_ASSERT_EQ(buffer, 1);
	if (!test_capture_authority || expected_tag == NULL
		|| !BufferTagsEqual(expected_tag, &test_live_tag)
		|| !cluster_itl_terminal_stamp_authority_admissible(
			cluster_enabled, cluster_node_id, test_node_count,
			test_recovery_merge_active, test_pcm_state, test_own_flags,
			test_writer_activation_token))
		return false;
	*own_generation = test_own_generation;
	*acquisition_epoch = test_acquisition_epoch;
	*pcm_state = test_pcm_state;
	return true;
}

Buffer
cluster_bufmgr_lock_resident_for_exact_itl_stamp(const ClusterItlTouchRecord *record,
											 ClusterItlStampSkipReason *out_reason)
{
	test_stamp_lock_calls++;
	test_stamp_lock_saw_valid_proof = record->proof.valid;
	if (!record->proof.valid
		|| !BufferTagsEqual(&test_live_tag,
							 &(BufferTag){ .spcOid = record->key.rloc.spcOid,
										  .dbOid = record->key.rloc.dbOid,
										  .relNumber = record->key.rloc.relNumber,
										  .forkNum = record->key.forknum,
										  .blockNum = record->key.block })
		|| !cluster_itl_terminal_stamp_authority_admissible(
			cluster_enabled, cluster_node_id, test_node_count,
			test_recovery_merge_active, test_pcm_state, test_own_flags,
			test_writer_activation_token)
		|| !cluster_itl_terminal_proof_owner_exact(
			&record->proof, test_own_generation, test_acquisition_epoch,
			test_pcm_state, true, test_own_flags, test_writer_activation_token)) {
		*out_reason = CLUSTER_ITL_STAMP_SKIP_INVALID_PROOF;
		return InvalidBuffer;
	}
	*out_reason = CLUSTER_ITL_STAMP_SKIP_NONE;
	return 1;
}

void
cluster_bufmgr_unlock_resident_stamp(Buffer buffer)
{
	UT_ASSERT_EQ(buffer, 1);
	test_stamp_unlock_calls++;
}

void
cluster_lever_g_note_stamp_skipped(void)
{
	test_stamp_skip_calls++;
}

GenericXLogState *
GenericXLogStartLogged(bool is_logged pg_attribute_unused())
{
	return (GenericXLogState *)&test_generic_state_storage;
}

Page
GenericXLogRegisterBuffer(GenericXLogState *state, Buffer buffer, int flags pg_attribute_unused())
{
	UT_ASSERT(state == (GenericXLogState *)&test_generic_state_storage);
	UT_ASSERT_EQ(buffer, 1);
	test_generic_register_calls++;
	return BufferGetPage(buffer);
}

XLogRecPtr
GenericXLogFinish(GenericXLogState *state)
{
	UT_ASSERT(state == (GenericXLogState *)&test_generic_state_storage);
	test_generic_finish_calls++;
	return InvalidXLogRecPtr;
}

void
GenericXLogAbort(GenericXLogState *state)
{
	UT_ASSERT(state == (GenericXLogState *)&test_generic_state_storage);
	test_generic_abort_calls++;
}

static ClusterItlSlotData *
reset_registration_fixture(TransactionId xid)
{
	Page page = (Page)test_buffer_block.data;
	PageHeader page_header = (PageHeader)page;
	ClusterItlSlotData *slot;

	cluster_itl_touch_reset_at_end_xact();
	MemSet(&test_buffer_block, 0, sizeof(test_buffer_block));
	page_header->pd_flags = PD_HAS_ITL;
	page_header->pd_special = BLCKSZ - CLUSTER_ITL_SPECIAL_SIZE;
	PageSetPageSizeAndVersion(page, BLCKSZ, PG_PAGE_LAYOUT_VERSION);
	slot = &ClusterPageGetItlSlots(page)[0];
	slot->xid = xid;
	slot->wrap = 9;
	slot->flags = ITL_FLAG_ACTIVE;

	test_capture_authority = false;
	test_own_generation = 41;
	test_acquisition_epoch = 17;
	test_pcm_state = PCM_STATE_X;
	test_own_flags = 0;
	test_writer_activation_token = 0;
	test_node_count = 2;
	test_recovery_merge_active = false;
	test_live_tag.spcOid = 11;
	test_live_tag.dbOid = 22;
	test_live_tag.relNumber = 33;
	test_live_tag.forkNum = MAIN_FORKNUM;
	test_live_tag.blockNum = 7;
	test_stamp_lock_calls = 0;
	test_stamp_lock_saw_valid_proof = false;
	test_generic_register_calls = 0;
	test_generic_abort_calls = 0;
	test_generic_finish_calls = 0;
	test_stamp_unlock_calls = 0;
	test_stamp_skip_calls = 0;
	return slot;
}

static ClusterItlTouchHandle
registration_handle(void)
{
	ClusterItlTouchHandle handle;

	MemSet(&handle, 0, sizeof(handle));
	handle.rloc.spcOid = 11;
	handle.rloc.dbOid = 22;
	handle.rloc.relNumber = 33;
	handle.block = 7;
	handle.forknum = MAIN_FORKNUM;
	handle.slot_idx = 0;
	return handle;
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
	proof.pcm_state = PCM_STATE_X;
	proof.valid = true;
	return proof;
}

UT_TEST(u13_exact_owner_proof_matches)
{
	ClusterItlTerminalProof proof = valid_proof();

	UT_ASSERT(cluster_itl_terminal_proof_owner_exact(
		&proof, 41, 17, PCM_STATE_X, true, 0, 0));
}

UT_TEST(u14_missing_owner_proof_is_rejected)
{
	ClusterItlTerminalProof proof = valid_proof();

	proof.valid = false;
	UT_ASSERT(!cluster_itl_terminal_proof_owner_exact(
		&proof, 41, 17, PCM_STATE_X, true, 0, 0));
}

UT_TEST(u15_later_x_generation_is_rejected)
{
	ClusterItlTerminalProof proof = valid_proof();

	UT_ASSERT(!cluster_itl_terminal_proof_owner_exact(
		&proof, 42, 17, PCM_STATE_X, true, 0, 0));
}

UT_TEST(u16_scope_change_is_rejected)
{
	ClusterItlTerminalProof proof = valid_proof();

	UT_ASSERT(!cluster_itl_terminal_proof_owner_exact(
		&proof, 41, 18, PCM_STATE_X, true, 0, 0));
}

UT_TEST(u17_non_x_owner_is_rejected)
{
	ClusterItlTerminalProof proof = valid_proof();

	UT_ASSERT(!cluster_itl_terminal_proof_owner_exact(
		&proof, 41, 17, PCM_STATE_N, true, 0, 0));
}

UT_TEST(u18_busy_owner_is_rejected)
{
	ClusterItlTerminalProof proof = valid_proof();

	UT_ASSERT(!cluster_itl_terminal_proof_owner_exact(
		&proof, 41, 17, PCM_STATE_X, true, 1, 0));
	UT_ASSERT(!cluster_itl_terminal_proof_owner_exact(
		&proof, 41, 17, PCM_STATE_X, true, 0, 99));
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

UT_TEST(u21_first_failed_capture_does_not_append)
{
	ClusterItlTouchHandle handle = registration_handle();
	TransactionId xid = 700;

	(void)reset_registration_fixture(xid);
	cluster_itl_touch_register_exact(&handle, 1, xid);
	UT_ASSERT_EQ(cluster_itl_touch_count(), 0);
}

UT_TEST(u22_failed_recapture_invalidates_one_existing_record)
{
	ClusterItlTouchHandle handle = registration_handle();
	TransactionId xid = 700;

	(void)reset_registration_fixture(xid);
	test_capture_authority = true;
	cluster_itl_touch_register_exact(&handle, 1, xid);
	UT_ASSERT_EQ(cluster_itl_touch_count(), 1);

	test_capture_authority = false;
	cluster_itl_touch_register_exact(&handle, 1, xid);
	UT_ASSERT_EQ(cluster_itl_touch_count(), 1);

	cluster_itl_xact_abort_finish(xid);
	UT_ASSERT_EQ(test_stamp_lock_calls, 1);
	UT_ASSERT(!test_stamp_lock_saw_valid_proof);
	UT_ASSERT_EQ(cluster_itl_touch_count(), 0);
}

UT_TEST(u23_invalidated_record_performs_no_terminal_page_stamp)
{
	ClusterItlTouchHandle handle = registration_handle();
	TransactionId xid = 700;
	ClusterItlSlotData *slot = reset_registration_fixture(xid);

	test_capture_authority = true;
	cluster_itl_touch_register_exact(&handle, 1, xid);
	test_capture_authority = false;
	cluster_itl_touch_register_exact(&handle, 1, xid);
	cluster_itl_xact_precommit_finish(xid, 99);

	UT_ASSERT_EQ(test_generic_register_calls, 0);
	UT_ASSERT_EQ(test_generic_finish_calls, 0);
	UT_ASSERT_EQ(test_generic_abort_calls, 1);
	UT_ASSERT_EQ(test_stamp_unlock_calls, 0);
	UT_ASSERT_EQ(test_stamp_skip_calls, 1);
	UT_ASSERT_EQ(slot->flags, ITL_FLAG_ACTIVE);
}

UT_TEST(u24_known_single_node_pcm_n_stamps_terminal_slot)
{
	ClusterItlTouchHandle handle = registration_handle();
	TransactionId xid = 700;
	ClusterItlSlotData *slot = reset_registration_fixture(xid);

	test_capture_authority = true;
	test_node_count = 1;
	test_pcm_state = PCM_STATE_N;
	cluster_itl_touch_register_exact(&handle, 1, xid);
	UT_ASSERT_EQ(cluster_itl_touch_count(), 1);

	cluster_itl_xact_precommit_finish(xid, 99);
	UT_ASSERT_EQ(slot->flags, ITL_FLAG_COMMITTED);
	UT_ASSERT_EQ(slot->commit_scn, 99);
	UT_ASSERT_EQ(test_generic_register_calls, 1);
	UT_ASSERT_EQ(test_generic_finish_calls, 1);
	UT_ASSERT_EQ(test_stamp_unlock_calls, 1);
	UT_ASSERT_EQ(test_stamp_skip_calls, 0);
}

UT_TEST(u25_peer_pcm_n_is_refused_before_registration)
{
	ClusterItlTouchHandle handle = registration_handle();
	TransactionId xid = 700;

	(void)reset_registration_fixture(xid);
	test_capture_authority = true;
	test_node_count = 2;
	test_pcm_state = PCM_STATE_N;
	cluster_itl_touch_register_exact(&handle, 1, xid);
	UT_ASSERT_EQ(cluster_itl_touch_count(), 0);
}

UT_TEST(u26_unknown_topology_pcm_n_is_refused)
{
	ClusterItlTouchHandle handle = registration_handle();
	TransactionId xid = 700;

	(void)reset_registration_fixture(xid);
	test_capture_authority = true;
	test_node_count = 0;
	test_pcm_state = PCM_STATE_N;
	cluster_itl_touch_register_exact(&handle, 1, xid);
	UT_ASSERT_EQ(cluster_itl_touch_count(), 0);
}

UT_TEST(u27_recovery_merge_pcm_n_is_refused)
{
	ClusterItlTouchHandle handle = registration_handle();
	TransactionId xid = 700;

	(void)reset_registration_fixture(xid);
	test_capture_authority = true;
	test_node_count = 1;
	test_pcm_state = PCM_STATE_N;
	test_recovery_merge_active = true;
	cluster_itl_touch_register_exact(&handle, 1, xid);
	UT_ASSERT_EQ(cluster_itl_touch_count(), 0);
}

UT_TEST(u28_tag_drift_preserves_active_slot)
{
	ClusterItlTouchHandle handle = registration_handle();
	TransactionId xid = 700;
	ClusterItlSlotData *slot = reset_registration_fixture(xid);

	test_capture_authority = true;
	test_node_count = 1;
	test_pcm_state = PCM_STATE_N;
	cluster_itl_touch_register_exact(&handle, 1, xid);
	test_live_tag.blockNum++;
	cluster_itl_xact_precommit_finish(xid, 99);

	UT_ASSERT_EQ(slot->flags, ITL_FLAG_ACTIVE);
	UT_ASSERT_EQ(test_generic_register_calls, 0);
	UT_ASSERT_EQ(test_stamp_skip_calls, 1);
}

UT_TEST(u29_generation_drift_preserves_active_slot)
{
	ClusterItlTouchHandle handle = registration_handle();
	TransactionId xid = 700;
	ClusterItlSlotData *slot = reset_registration_fixture(xid);

	test_capture_authority = true;
	test_node_count = 1;
	test_pcm_state = PCM_STATE_N;
	cluster_itl_touch_register_exact(&handle, 1, xid);
	test_own_generation++;
	cluster_itl_xact_precommit_finish(xid, 99);

	UT_ASSERT_EQ(slot->flags, ITL_FLAG_ACTIVE);
	UT_ASSERT_EQ(test_generic_register_calls, 0);
	UT_ASSERT_EQ(test_stamp_skip_calls, 1);
}

UT_TEST(u30_slot_drift_preserves_active_slot)
{
	ClusterItlTouchHandle handle = registration_handle();
	TransactionId xid = 700;
	ClusterItlSlotData *slot = reset_registration_fixture(xid);

	test_capture_authority = true;
	test_node_count = 1;
	test_pcm_state = PCM_STATE_N;
	cluster_itl_touch_register_exact(&handle, 1, xid);
	slot->wrap++;
	cluster_itl_xact_precommit_finish(xid, 99);

	UT_ASSERT_EQ(slot->flags, ITL_FLAG_ACTIVE);
	UT_ASSERT_EQ(test_generic_register_calls, 0);
	UT_ASSERT_EQ(test_stamp_unlock_calls, 1);
	UT_ASSERT_EQ(test_stamp_skip_calls, 1);
}

UT_TEST(u31_peer_pcm_x_contract_still_stamps)
{
	ClusterItlTouchHandle handle = registration_handle();
	TransactionId xid = 700;
	ClusterItlSlotData *slot = reset_registration_fixture(xid);

	test_capture_authority = true;
	cluster_itl_touch_register_exact(&handle, 1, xid);
	cluster_itl_xact_precommit_finish(xid, 99);

	UT_ASSERT_EQ(slot->flags, ITL_FLAG_COMMITTED);
	UT_ASSERT_EQ(test_generic_register_calls, 1);
	UT_ASSERT_EQ(test_stamp_skip_calls, 0);
}

UT_TEST(u32_known_single_node_pcm_x_is_refused)
{
	ClusterItlTouchHandle handle = registration_handle();
	TransactionId xid = 700;

	(void)reset_registration_fixture(xid);
	test_capture_authority = true;
	test_node_count = 1;
	test_pcm_state = PCM_STATE_X;
	cluster_itl_touch_register_exact(&handle, 1, xid);
	UT_ASSERT_EQ(cluster_itl_touch_count(), 0);
}

UT_TEST(u33_busy_pcm_n_tuple_is_refused)
{
	ClusterItlTouchHandle handle = registration_handle();
	TransactionId xid = 700;

	(void)reset_registration_fixture(xid);
	test_capture_authority = true;
	test_node_count = 1;
	test_pcm_state = PCM_STATE_N;
	test_own_flags = 1;
	cluster_itl_touch_register_exact(&handle, 1, xid);
	UT_ASSERT_EQ(cluster_itl_touch_count(), 0);

	test_own_flags = 0;
	test_writer_activation_token = 9;
	cluster_itl_touch_register_exact(&handle, 1, xid);
	UT_ASSERT_EQ(cluster_itl_touch_count(), 0);
}

UT_TEST(u34_epoch_drift_preserves_active_slot)
{
	ClusterItlTouchHandle handle = registration_handle();
	TransactionId xid = 700;
	ClusterItlSlotData *slot = reset_registration_fixture(xid);

	test_capture_authority = true;
	test_node_count = 1;
	test_pcm_state = PCM_STATE_N;
	cluster_itl_touch_register_exact(&handle, 1, xid);
	test_acquisition_epoch++;
	cluster_itl_xact_precommit_finish(xid, 99);

	UT_ASSERT_EQ(slot->flags, ITL_FLAG_ACTIVE);
	UT_ASSERT_EQ(test_generic_register_calls, 0);
	UT_ASSERT_EQ(test_stamp_skip_calls, 1);
}

int
main(void)
{
	UT_PLAN(22);
	UT_RUN(u13_exact_owner_proof_matches);
	UT_RUN(u14_missing_owner_proof_is_rejected);
	UT_RUN(u15_later_x_generation_is_rejected);
	UT_RUN(u16_scope_change_is_rejected);
	UT_RUN(u17_non_x_owner_is_rejected);
	UT_RUN(u18_busy_owner_is_rejected);
	UT_RUN(u19_exact_slot_proof_matches);
	UT_RUN(u20_slot_aba_or_class_change_is_rejected);
	UT_RUN(u21_first_failed_capture_does_not_append);
	UT_RUN(u22_failed_recapture_invalidates_one_existing_record);
	UT_RUN(u23_invalidated_record_performs_no_terminal_page_stamp);
	UT_RUN(u24_known_single_node_pcm_n_stamps_terminal_slot);
	UT_RUN(u25_peer_pcm_n_is_refused_before_registration);
	UT_RUN(u26_unknown_topology_pcm_n_is_refused);
	UT_RUN(u27_recovery_merge_pcm_n_is_refused);
	UT_RUN(u28_tag_drift_preserves_active_slot);
	UT_RUN(u29_generation_drift_preserves_active_slot);
	UT_RUN(u30_slot_drift_preserves_active_slot);
	UT_RUN(u31_peer_pcm_x_contract_still_stamps);
	UT_RUN(u32_known_single_node_pcm_x_is_refused);
	UT_RUN(u33_busy_pcm_n_tuple_is_refused);
	UT_RUN(u34_epoch_drift_preserves_active_slot);
	UT_DONE();
}
